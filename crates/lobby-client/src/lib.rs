//! Blocking client for the Rosace lobby server, usable from a game loop without an async
//! runtime (HTTP via `ureq`, UDP via `std::net::UdpSocket`; long calls belong on a thread).
//!
//! - [`LobbyClient`]: register / heartbeat / unregister / list over HTTP.
//! - [`HostAgent`]: runs on the host next to the game server. It registers the lobby,
//!   heartbeats it, registers the game server's OWN UDP socket with the rendezvous service
//!   (through a `try_clone` of it, send-only), punches towards joining clients from that socket
//!   and opens a local relay leg per relayed client.
//! - [`connect`]: joins a lobby: rendezvous, hole punching, relay fallback. Returns a UDP socket
//!   plus the address the game client must send to (the host's public endpoint when punched,
//!   the relay when relayed): hand both to the game's transport unchanged.

mod connect;
mod host;
mod net;

use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

pub use connect::{ConnectOptions, Connection, Route, connect};
pub use host::{HostAgent, HostOptions, HostStatus};
pub use lobby_proto as proto;
pub use lobby_proto::{HeartbeatRequest, LobbyInfo, RegisterRequest, RegisterResponse, ServerInfo};
pub use net::{disable_udp_connreset, lan_ip};

/// Everything that can go wrong talking to the lobby service.
#[derive(Debug)]
pub enum Error {
    /// Transport failure (DNS, refused, timeout...).
    Http(String),
    /// The server answered with an error status.
    Status(u16, String),
    Io(std::io::Error),
    /// No answer in time (rendezvous / relay).
    Timeout(String),
    /// The rendezvous or relay refused (lobby gone, server busy...).
    Refused(String),
    Invalid(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Http(e) => write!(f, "lobby server unreachable: {e}"),
            Error::Status(code, e) => write!(f, "lobby server error {code}: {e}"),
            Error::Io(e) => write!(f, "network error: {e}"),
            Error::Timeout(e) => write!(f, "timed out: {e}"),
            Error::Refused(e) => write!(f, "refused: {e}"),
            Error::Invalid(e) => write!(f, "invalid: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<ureq::Error> for Error {
    fn from(e: ureq::Error) -> Self {
        Error::Http(e.to_string())
    }
}

/// HTTP side of the lobby service.
#[derive(Clone)]
pub struct LobbyClient {
    base: String,
    agent: ureq::Agent,
}

impl std::fmt::Debug for LobbyClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LobbyClient").field("base", &self.base).finish()
    }
}

impl LobbyClient {
    /// `base_url` like `http://162.19.66.231:8080` (a trailing `/` is fine).
    pub fn new(base_url: &str) -> Self {
        let agent: ureq::Agent = ureq::Agent::config_builder().timeout_global(Some(Duration::from_secs(5))).http_status_as_error(false).build().into();
        Self { base: base_url.trim().trim_end_matches('/').to_string(), agent }
    }

    pub fn base_url(&self) -> &str {
        &self.base
    }

    /// Host name (or IP) of the base URL.
    pub fn host(&self) -> Result<String, Error> {
        url_host(&self.base).ok_or_else(|| Error::Invalid(format!("lobby url {}", self.base)))
    }

    /// The server's UDP address on `port` (same host as the HTTP API, IPv4 preferred).
    pub fn udp_addr(&self, port: u16) -> Result<SocketAddr, Error> {
        let host = self.host()?;
        let addrs: Vec<SocketAddr> = (host.as_str(), port).to_socket_addrs()?.collect();
        addrs.iter().find(|a| a.is_ipv4()).or(addrs.first()).copied().ok_or_else(|| Error::Invalid(format!("{host}: no address")))
    }

    fn read<T: serde::de::DeserializeOwned>(mut resp: ureq::http::Response<ureq::Body>) -> Result<T, Error> {
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            let body = resp.body_mut().read_to_string().unwrap_or_default();
            let msg = serde_json::from_str::<lobby_proto::ErrorResponse>(&body).map(|e| e.error).unwrap_or(body);
            return Err(Error::Status(status, msg));
        }
        resp.body_mut().read_json::<T>().map_err(|e| Error::Http(e.to_string()))
    }

    fn check(mut resp: ureq::http::Response<ureq::Body>) -> Result<(), Error> {
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            let body = resp.body_mut().read_to_string().unwrap_or_default();
            let msg = serde_json::from_str::<lobby_proto::ErrorResponse>(&body).map(|e| e.error).unwrap_or(body);
            return Err(Error::Status(status, msg));
        }
        Ok(())
    }

    pub fn info(&self) -> Result<ServerInfo, Error> {
        Self::read(self.agent.get(format!("{}/v1/info", self.base)).call()?)
    }

    pub fn health(&self) -> Result<lobby_proto::HealthResponse, Error> {
        Self::read(self.agent.get(format!("{}/health", self.base)).call()?)
    }

    pub fn register(&self, req: &RegisterRequest) -> Result<RegisterResponse, Error> {
        req.validate().map_err(Error::Invalid)?;
        Self::read(self.agent.post(format!("{}/v1/lobbies", self.base)).send_json(req)?)
    }

    pub fn heartbeat(&self, id: &str, token: &str, hb: &HeartbeatRequest) -> Result<(), Error> {
        Self::check(self.agent.post(format!("{}/v1/lobbies/{id}/heartbeat", self.base)).header("Authorization", format!("Bearer {token}")).send_json(hb)?)
    }

    pub fn unregister(&self, id: &str, token: &str) -> Result<(), Error> {
        Self::check(self.agent.delete(format!("{}/v1/lobbies/{id}", self.base)).header("Authorization", format!("Bearer {token}")).call()?)
    }

    /// Lobbies of `game`, only those with exactly `version` when given.
    pub fn list(&self, game: &str, version: Option<&str>) -> Result<Vec<LobbyInfo>, Error> {
        let mut req = self.agent.get(format!("{}/v1/lobbies", self.base)).query("game", game);
        if let Some(v) = version {
            req = req.query("version", v);
        }
        Ok(Self::read::<lobby_proto::ListResponse>(req.call()?)?.lobbies)
    }
}

/// `http://host:port/path` -> `host` (brackets of IPv6 literals removed).
pub fn url_host(url: &str) -> Option<String> {
    let rest = url.split_once("://").map(|(_, r)| r).unwrap_or(url);
    let authority = rest.split(['/', '?', '#']).next()?;
    let authority = authority.rsplit_once('@').map(|(_, h)| h).unwrap_or(authority);
    let host = if let Some(v6) = authority.strip_prefix('[') {
        v6.split(']').next()?
    } else {
        authority.split(':').next()?
    };
    (!host.is_empty()).then(|| host.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_hosts() {
        assert_eq!(url_host("http://162.19.66.231:8080").as_deref(), Some("162.19.66.231"));
        assert_eq!(url_host("https://lobby.example.com/").as_deref(), Some("lobby.example.com"));
        assert_eq!(url_host("http://[::1]:8080/x").as_deref(), Some("::1"));
        assert_eq!(url_host("localhost:8080").as_deref(), Some("localhost"));
        assert_eq!(url_host("http://").as_deref(), None);
        assert_eq!(LobbyClient::new("http://127.0.0.1:8080/").base_url(), "http://127.0.0.1:8080");
    }
}
