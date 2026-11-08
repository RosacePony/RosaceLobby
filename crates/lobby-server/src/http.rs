//! HTTP JSON API.
//!
//! | method | path | auth | |
//! |---|---|---|---|
//! | GET | `/health` | - | liveness + counters |
//! | GET | `/v1/info` | - | UDP ports, timings |
//! | GET | `/v1/lobbies?game=..&version=..` | - | list |
//! | POST | `/v1/lobbies` | - | register, returns id + token |
//! | POST | `/v1/lobbies/{id}/heartbeat` | Bearer token | keep alive, update players |
//! | DELETE | `/v1/lobbies/{id}` | Bearer token | unregister |

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Instant;

use axum::extract::{ConnectInfo, DefaultBodyLimit, Path, Query, Request, State as AxState};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use lobby_proto::{ErrorResponse, HealthResponse, HeartbeatRequest, ListResponse, LobbyInfo, RegisterRequest, RegisterResponse, ServerInfo};
use serde::Deserialize;

use crate::{Lobby, State, random_hex};

type Shared = Arc<State>;

/// An API error: status + JSON `{ "error": ... }`.
pub struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(ErrorResponse { error: self.1 })).into_response()
    }
}

fn err(code: StatusCode, msg: impl Into<String>) -> ApiError {
    ApiError(code, msg.into())
}

pub fn router(state: Shared) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/info", get(info))
        .route("/v1/lobbies", get(list).post(register))
        .route("/v1/lobbies/{id}/heartbeat", post(heartbeat))
        .route("/v1/lobbies/{id}", axum::routing::delete(unregister))
        .layer(middleware::from_fn_with_state(state.clone(), rate_limit))
        .layer(DefaultBodyLimit::max(4096))
        .with_state(state)
}

/// The caller's IP (first `X-Forwarded-For` hop when `trust_proxy`).
fn client_ip(state: &State, headers: &HeaderMap, peer: SocketAddr) -> IpAddr {
    if state.config.trust_proxy
        && let Some(ip) = headers
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(',').next())
            .and_then(|v| v.trim().parse::<IpAddr>().ok())
    {
        return ip;
    }
    peer.ip()
}

async fn rate_limit(AxState(state): AxState<Shared>, ConnectInfo(peer): ConnectInfo<SocketAddr>, req: Request, next: Next) -> Response {
    let ip = client_ip(&state, req.headers(), peer);
    if req.uri().path() != "/health" && !state.http_limit.allow(ip, Instant::now()) {
        tracing::warn!(%ip, path = req.uri().path(), "rate limited");
        return err(StatusCode::TOO_MANY_REQUESTS, "too many requests").into_response();
    }
    next.run(req).await
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers.get("authorization")?.to_str().ok()?.strip_prefix("Bearer ").map(str::trim)
}

async fn health(AxState(state): AxState<Shared>) -> Json<HealthResponse> {
    let t = state.tables();
    Json(HealthResponse {
        status: "ok".into(),
        lobbies: t.lobbies.len(),
        sessions: t.sessions.len(),
        relayed_sessions: t.sessions.values().filter(|s| s.ready()).count(),
    })
}

async fn info(AxState(state): AxState<Shared>) -> Json<ServerInfo> {
    Json(ServerInfo {
        server_version: env!("CARGO_PKG_VERSION").into(),
        protocol_version: lobby_proto::PROTOCOL_VERSION,
        rendezvous_port: state.rendezvous_port,
        relay_port: state.relay_port,
        heartbeat_secs: state.config.heartbeat_secs,
        expire_secs: state.config.expire_secs,
    })
}

#[derive(Deserialize)]
struct ListQuery {
    game: String,
    version: Option<String>,
}

async fn list(AxState(state): AxState<Shared>, Query(q): Query<ListQuery>) -> Result<Json<ListResponse>, ApiError> {
    if !lobby_proto::valid_game_id(&q.game) {
        return Err(err(StatusCode::BAD_REQUEST, "invalid game id"));
    }
    let now = Instant::now();
    let t = state.tables();
    let mut lobbies: Vec<LobbyInfo> = t
        .lobbies
        .values()
        .filter(|l| l.info.game == q.game && q.version.as_ref().is_none_or(|v| &l.info.version == v))
        .map(|l| LobbyInfo { age_secs: now.duration_since(l.created).as_secs(), ..l.info.clone() })
        .collect();
    // Fullest first (players attract players), then oldest.
    lobbies.sort_by(|a, b| b.players.cmp(&a.players).then(b.age_secs.cmp(&a.age_secs)).then(a.id.cmp(&b.id)));
    lobbies.truncate(500);
    Ok(Json(ListResponse { lobbies, rendezvous_port: state.rendezvous_port, relay_port: state.relay_port }))
}

async fn register(
    AxState(state): AxState<Shared>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<RegisterRequest>,
) -> Result<Json<RegisterResponse>, ApiError> {
    let ip = client_ip(&state, &headers, peer);
    let now = Instant::now();
    if !state.register_limit.allow(ip, now) {
        return Err(err(StatusCode::TOO_MANY_REQUESTS, "too many registrations"));
    }
    req.validate().map_err(|e| err(StatusCode::BAD_REQUEST, e))?;
    let mut t = state.tables();
    if t.lobbies.len() >= state.config.max_lobbies {
        return Err(err(StatusCode::SERVICE_UNAVAILABLE, "lobby server full"));
    }
    if t.lobbies.values().filter(|l| l.owner_ip == ip).count() >= state.config.max_lobbies_per_ip {
        return Err(err(StatusCode::TOO_MANY_REQUESTS, "too many lobbies from this address"));
    }
    let id = random_hex(8);
    let token = random_hex(16);
    let info = LobbyInfo {
        id: id.clone(),
        game: req.game,
        version: req.version,
        name: req.name.trim().to_string(),
        mode: req.mode,
        map: req.map,
        players: req.players,
        max_players: req.max_players,
        has_password: req.has_password,
        region: req.region,
        age_secs: 0,
    };
    tracing::info!(lobby = %id, %ip, game = %info.game, version = %info.version, name = %info.name, "lobby registered");
    t.lobbies.insert(
        id.clone(),
        Lobby { info, token: token.clone(), owner_ip: ip, created: now, last_seen: now, local_addrs: req.local_addrs, game_ep: None, control_ep: None },
    );
    Ok(Json(RegisterResponse {
        id,
        token,
        heartbeat_secs: state.config.heartbeat_secs,
        expire_secs: state.config.expire_secs,
        rendezvous_port: state.rendezvous_port,
        relay_port: state.relay_port,
    }))
}

/// Checks the bearer token of lobby `id` (constant-time compare).
fn authorize<'a>(t: &'a mut crate::Tables, id: &str, headers: &HeaderMap) -> Result<&'a mut Lobby, ApiError> {
    let token = bearer(headers).ok_or_else(|| err(StatusCode::UNAUTHORIZED, "missing bearer token"))?;
    let lobby = t.lobbies.get_mut(id).ok_or_else(|| err(StatusCode::NOT_FOUND, "no such lobby"))?;
    if !ct_eq(lobby.token.as_bytes(), token.as_bytes()) {
        return Err(err(StatusCode::FORBIDDEN, "bad token"));
    }
    Ok(lobby)
}

pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

async fn heartbeat(AxState(state): AxState<Shared>, Path(id): Path<String>, headers: HeaderMap, Json(hb): Json<HeartbeatRequest>) -> Result<StatusCode, ApiError> {
    hb.validate().map_err(|e| err(StatusCode::BAD_REQUEST, e))?;
    let mut t = state.tables();
    let lobby = authorize(&mut t, &id, &headers)?;
    lobby.last_seen = Instant::now();
    lobby.info.players = hb.players.min(lobby.info.max_players);
    if let Some(m) = hb.map {
        lobby.info.map = m;
    }
    if let Some(m) = hb.mode {
        lobby.info.mode = m;
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn unregister(AxState(state): AxState<Shared>, Path(id): Path<String>, headers: HeaderMap) -> Result<StatusCode, ApiError> {
    let mut t = state.tables();
    authorize(&mut t, &id, &headers)?;
    t.lobbies.remove(&id);
    tracing::info!(lobby = %id, "lobby closed by host");
    Ok(StatusCode::NO_CONTENT)
}
