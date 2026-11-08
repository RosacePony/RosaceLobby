//! Rosace lobby server.
//!
//! - HTTP JSON API ([`http`]): hosts register a lobby and heartbeat it, players list them.
//! - UDP rendezvous ([`udp::run_rendezvous`]): tells a host and a joining client each other's
//!   public endpoints so both can punch a hole through their NATs.
//! - UDP relay ([`udp::run_relay`]): when punching fails, forwards datagrams between the two
//!   endpoints of a session unchanged (the game's packets pass through as-is).
//!
//! Everything shares one [`State`] behind a mutex (held briefly, never across an await).

pub mod http;
pub mod limits;
pub mod udp;

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use clap::Parser;
use lobby_proto::{LobbyInfo, Role};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::watch;

use crate::limits::{Bandwidth, RateLimiter};

/// Server settings: every flag also reads an environment variable.
#[derive(Clone, Debug, Parser)]
#[command(name = "lobby-server", version, about = "Rosace lobby + NAT traversal + relay server")]
pub struct Config {
    /// HTTP API listen address.
    #[arg(long, env = "LOBBY_HTTP_ADDR", default_value = "0.0.0.0:8080")]
    pub http_addr: SocketAddr,
    /// UDP rendezvous (hole punching) listen address.
    #[arg(long, env = "LOBBY_RENDEZVOUS_ADDR", default_value = "0.0.0.0:7777")]
    pub rendezvous_addr: SocketAddr,
    /// UDP relay listen address.
    #[arg(long, env = "LOBBY_RELAY_ADDR", default_value = "0.0.0.0:7778")]
    pub relay_addr: SocketAddr,
    /// Rendezvous port announced to clients (if a port mapping changes it). Default: the bound port.
    #[arg(long, env = "LOBBY_PUBLIC_RENDEZVOUS_PORT")]
    pub public_rendezvous_port: Option<u16>,
    /// Relay port announced to clients. Default: the bound port.
    #[arg(long, env = "LOBBY_PUBLIC_RELAY_PORT")]
    pub public_relay_port: Option<u16>,
    /// Hosts heartbeat this often (seconds).
    #[arg(long, env = "LOBBY_HEARTBEAT_SECS", default_value_t = 5)]
    pub heartbeat_secs: u64,
    /// A lobby without a heartbeat for this long disappears (seconds).
    #[arg(long, env = "LOBBY_EXPIRE_SECS", default_value_t = 20)]
    pub expire_secs: u64,
    /// Lobbies in total.
    #[arg(long, env = "LOBBY_MAX_LOBBIES", default_value_t = 2000)]
    pub max_lobbies: usize,
    /// Lobbies registered from one IP.
    #[arg(long, env = "LOBBY_MAX_LOBBIES_PER_IP", default_value_t = 5)]
    pub max_lobbies_per_ip: usize,
    /// HTTP requests per minute and IP (all endpoints).
    #[arg(long, env = "LOBBY_RATE_PER_MIN", default_value_t = 120)]
    pub rate_per_min: u32,
    /// Registrations per minute and IP.
    #[arg(long, env = "LOBBY_REGISTER_PER_MIN", default_value_t = 10)]
    pub register_per_min: u32,
    /// UDP control packets per second and IP (rendezvous + relay binds).
    #[arg(long, env = "LOBBY_UDP_PER_SEC", default_value_t = 50)]
    pub udp_per_sec: u32,
    /// Sessions (joins being set up, punched or relayed) in total.
    #[arg(long, env = "RELAY_MAX_SESSIONS", default_value_t = 512)]
    pub max_sessions: usize,
    /// Sessions started from one client IP.
    #[arg(long, env = "RELAY_MAX_SESSIONS_PER_IP", default_value_t = 8)]
    pub max_sessions_per_ip: usize,
    /// Relay bandwidth cap per session (bytes per second, both directions together).
    #[arg(long, env = "RELAY_BYTES_PER_SEC", default_value_t = 1_000_000)]
    pub relay_bytes_per_sec: u64,
    /// A session with no traffic for this long is closed (seconds).
    #[arg(long, env = "RELAY_IDLE_SECS", default_value_t = 30)]
    pub relay_idle_secs: u64,
    /// Use the first `X-Forwarded-For` address as the client IP (only behind a trusted reverse proxy).
    #[arg(long, env = "LOBBY_TRUST_PROXY", default_value_t = false)]
    pub trust_proxy: bool,
}

impl Config {
    /// Loopback, ephemeral ports: for tests.
    pub fn local_test() -> Config {
        let mut c = Config::parse_from(["lobby-server"]);
        c.http_addr = "127.0.0.1:0".parse().unwrap();
        c.rendezvous_addr = "127.0.0.1:0".parse().unwrap();
        c.relay_addr = "127.0.0.1:0".parse().unwrap();
        c
    }
}

/// A registered lobby.
#[derive(Debug)]
pub struct Lobby {
    pub info: LobbyInfo,
    pub token: String,
    pub owner_ip: IpAddr,
    pub created: Instant,
    pub last_seen: Instant,
    pub local_addrs: Vec<SocketAddr>,
    /// Public endpoint of the host's game socket (from `HostBeat`).
    pub game_ep: Option<SocketAddr>,
    /// Public endpoint of the host's control socket (from `HostControl`).
    pub control_ep: Option<SocketAddr>,
}

/// One join attempt: punched, relayed, or still being set up.
#[derive(Debug)]
pub struct Session {
    pub lobby: String,
    pub key: String,
    pub client_ip: IpAddr,
    pub nonce: u64,
    pub join_ep: SocketAddr,
    pub host: Option<SocketAddr>,
    pub client: Option<SocketAddr>,
    pub last_activity: Instant,
    pub bandwidth: Bandwidth,
    pub relayed_bytes: u64,
    pub dropped_bytes: u64,
}

impl Session {
    pub fn endpoint(&self, role: Role) -> Option<SocketAddr> {
        match role {
            Role::Host => self.host,
            Role::Client => self.client,
        }
    }
    pub fn ready(&self) -> bool {
        self.host.is_some() && self.client.is_some()
    }
}

#[derive(Debug, Default)]
pub struct Tables {
    pub lobbies: HashMap<String, Lobby>,
    pub sessions: HashMap<u64, Session>,
    /// Relay endpoint -> session.
    pub relay_index: HashMap<SocketAddr, u64>,
}

impl Tables {
    pub fn remove_session(&mut self, id: u64) {
        if let Some(s) = self.sessions.remove(&id) {
            for ep in [s.host, s.client].into_iter().flatten() {
                if self.relay_index.get(&ep) == Some(&id) {
                    self.relay_index.remove(&ep);
                }
            }
        }
    }
}

pub struct State {
    pub config: Config,
    pub tables: Mutex<Tables>,
    pub http_limit: RateLimiter,
    pub register_limit: RateLimiter,
    pub udp_limit: RateLimiter,
    pub rendezvous_port: u16,
    pub relay_port: u16,
}

impl State {
    pub fn tables(&self) -> std::sync::MutexGuard<'_, Tables> {
        self.tables.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Drops expired lobbies, idle sessions and stale rate-limit windows.
    pub fn sweep(&self, now: Instant) {
        let expire = Duration::from_secs(self.config.expire_secs);
        let idle = Duration::from_secs(self.config.relay_idle_secs);
        let mut t = self.tables();
        let before = t.lobbies.len();
        t.lobbies.retain(|id, l| {
            let keep = now.duration_since(l.last_seen) <= expire;
            if !keep {
                tracing::info!(lobby = %id, name = %l.info.name, "lobby expired");
            }
            keep
        });
        let dead: Vec<u64> = t.sessions.iter().filter(|(_, s)| now.duration_since(s.last_activity) > idle).map(|(id, _)| *id).collect();
        for id in &dead {
            t.remove_session(*id);
        }
        if before != t.lobbies.len() || !dead.is_empty() {
            tracing::debug!(lobbies = t.lobbies.len(), sessions = t.sessions.len(), "sweep");
        }
        drop(t);
        self.http_limit.sweep(now);
        self.register_limit.sweep(now);
        self.udp_limit.sweep(now);
    }
}

/// Random lowercase hex string of `bytes` random bytes.
pub fn random_hex(bytes: usize) -> String {
    let mut b = vec![0u8; bytes];
    getrandom::fill(&mut b).expect("os random");
    b.iter().map(|x| format!("{x:02x}")).collect()
}

pub fn random_u64() -> u64 {
    let mut b = [0u8; 8];
    getrandom::fill(&mut b).expect("os random");
    u64::from_le_bytes(b)
}

/// A started server (all tasks stop when it is dropped or [`Running::shutdown`] is called).
pub struct Running {
    pub http_addr: SocketAddr,
    pub rendezvous_addr: SocketAddr,
    pub relay_addr: SocketAddr,
    pub state: Arc<State>,
    stop: watch::Sender<bool>,
}

impl Running {
    pub fn http_url(&self) -> String {
        format!("http://{}", self.http_addr)
    }
    pub fn shutdown(&self) {
        let _ = self.stop.send(true);
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

/// Binds every socket and spawns the HTTP, rendezvous, relay and sweeper tasks.
pub async fn start(config: Config) -> std::io::Result<Running> {
    let http = TcpListener::bind(config.http_addr).await?;
    let rendezvous = Arc::new(UdpSocket::bind(config.rendezvous_addr).await?);
    let relay = Arc::new(UdpSocket::bind(config.relay_addr).await?);
    let (http_addr, rendezvous_addr, relay_addr) = (http.local_addr()?, rendezvous.local_addr()?, relay.local_addr()?);
    let state = Arc::new(State {
        rendezvous_port: config.public_rendezvous_port.unwrap_or(rendezvous_addr.port()),
        relay_port: config.public_relay_port.unwrap_or(relay_addr.port()),
        http_limit: RateLimiter::new(config.rate_per_min, Duration::from_secs(60)),
        register_limit: RateLimiter::new(config.register_per_min, Duration::from_secs(60)),
        udp_limit: RateLimiter::new(config.udp_per_sec, Duration::from_secs(1)),
        tables: Mutex::new(Tables::default()),
        config,
    });
    let (stop, stop_rx) = watch::channel(false);

    let app = http::router(state.clone());
    let mut rx = stop_rx.clone();
    tokio::spawn(async move {
        let serve = axum::serve(http, app.into_make_service_with_connect_info::<SocketAddr>()).with_graceful_shutdown(async move {
            let _ = rx.wait_for(|s| *s).await;
        });
        if let Err(e) = serve.await {
            tracing::error!("http server: {e}");
        }
    });
    spawn_until_stop(stop_rx.clone(), udp::run_rendezvous(state.clone(), rendezvous.clone()));
    spawn_until_stop(stop_rx.clone(), udp::run_relay(state.clone(), relay, rendezvous));
    let sweeper = state.clone();
    spawn_until_stop(stop_rx, async move {
        let mut tick = tokio::time::interval(Duration::from_millis(500));
        loop {
            tick.tick().await;
            sweeper.sweep(Instant::now());
        }
    });
    tracing::info!(%http_addr, %rendezvous_addr, %relay_addr, "lobby server started");
    Ok(Running { http_addr, rendezvous_addr, relay_addr, state, stop })
}

fn spawn_until_stop(mut stop: watch::Receiver<bool>, task: impl std::future::Future<Output = ()> + Send + 'static) {
    tokio::spawn(async move {
        tokio::select! {
            _ = task => {}
            _ = stop.wait_for(|s| *s) => {}
        }
    });
}
