//! Wire types shared by the Rosace lobby server and its clients.
//!
//! Two transports:
//! - an HTTP JSON API (`/v1/...`) for registering, heartbeating and listing lobbies;
//! - small UDP datagrams ([`Packet`]) for the rendezvous service (NAT hole punching) and the
//!   relay control plane. Every control datagram starts with [`MAGIC`] + [`PROTOCOL_VERSION`];
//!   anything else that reaches the relay from a bound endpoint is game data and is forwarded
//!   unchanged.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

/// HTTP API prefix.
pub const API_PREFIX: &str = "/v1";
/// First bytes of every UDP control datagram.
pub const MAGIC: [u8; 4] = *b"RLBY";
/// Bumped on any incompatible change of [`Packet`].
pub const PROTOCOL_VERSION: u8 = 1;
/// Largest control datagram we ever produce or accept.
pub const MAX_PACKET: usize = 1200;

// ------------------------------------------------------------------ limits

pub const MAX_GAME_LEN: usize = 32;
pub const MAX_VERSION_LEN: usize = 32;
pub const MAX_NAME_LEN: usize = 48;
pub const MAX_FIELD_LEN: usize = 32;
pub const MAX_REGION_LEN: usize = 16;
pub const MAX_PLAYERS_LIMIT: u32 = 1024;
pub const MAX_LOCAL_ADDRS: usize = 8;

// ------------------------------------------------------------------ HTTP types

/// One public game as listed by `GET /v1/lobbies`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LobbyInfo {
    pub id: String,
    /// Game id, e.g. `claws_and_magic` (one server lists many games).
    pub game: String,
    /// Game build version; clients only join an exact match.
    pub version: String,
    pub name: String,
    pub mode: String,
    pub map: String,
    pub players: u32,
    pub max_players: u32,
    #[serde(default)]
    pub has_password: bool,
    #[serde(default)]
    pub region: Option<String>,
    /// Seconds since the lobby was registered.
    #[serde(default)]
    pub age_secs: u64,
}

/// `POST /v1/lobbies`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterRequest {
    pub game: String,
    pub version: String,
    pub name: String,
    pub mode: String,
    pub map: String,
    pub players: u32,
    pub max_players: u32,
    #[serde(default)]
    pub has_password: bool,
    #[serde(default)]
    pub region: Option<String>,
    /// LAN addresses of the host's game socket (players behind the same NAT try these first).
    #[serde(default)]
    pub local_addrs: Vec<SocketAddr>,
}

/// Reply to a registration: keep `token` secret, it authorises heartbeats, deletion and the
/// host's UDP registration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterResponse {
    pub id: String,
    pub token: String,
    pub heartbeat_secs: u64,
    pub expire_secs: u64,
    pub rendezvous_port: u16,
    pub relay_port: u16,
}

/// `POST /v1/lobbies/{id}/heartbeat` (token in `Authorization: Bearer <token>`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub players: u32,
    #[serde(default)]
    pub map: Option<String>,
    #[serde(default)]
    pub mode: Option<String>,
}

/// `GET /v1/lobbies?game=..&version=..`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListResponse {
    pub lobbies: Vec<LobbyInfo>,
    pub rendezvous_port: u16,
    pub relay_port: u16,
}

/// `GET /v1/info`: what a client needs before joining.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerInfo {
    pub server_version: String,
    pub protocol_version: u8,
    pub rendezvous_port: u16,
    pub relay_port: u16,
    pub heartbeat_secs: u64,
    pub expire_secs: u64,
}

/// `GET /health`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub lobbies: usize,
    pub sessions: usize,
    pub relayed_sessions: usize,
}

/// Body of every non-2xx reply.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}

fn check_text(field: &str, s: &str, max: usize, allow_empty: bool) -> Result<(), String> {
    if !allow_empty && s.trim().is_empty() {
        return Err(format!("{field} is empty"));
    }
    if s.chars().count() > max {
        return Err(format!("{field} is longer than {max} characters"));
    }
    if s.chars().any(|c| c.is_control()) {
        return Err(format!("{field} contains control characters"));
    }
    Ok(())
}

/// Game ids are lowercase ASCII identifiers (`[a-z0-9_-]`).
pub fn valid_game_id(s: &str) -> bool {
    !s.is_empty() && s.len() <= MAX_GAME_LEN && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
}

impl RegisterRequest {
    /// Same checks on both ends (the client fails fast, the server enforces).
    pub fn validate(&self) -> Result<(), String> {
        if !valid_game_id(&self.game) {
            return Err(format!("game must match [a-z0-9_-]{{1,{MAX_GAME_LEN}}}"));
        }
        check_text("version", &self.version, MAX_VERSION_LEN, false)?;
        check_text("name", &self.name, MAX_NAME_LEN, false)?;
        check_text("mode", &self.mode, MAX_FIELD_LEN, true)?;
        check_text("map", &self.map, MAX_FIELD_LEN, true)?;
        if let Some(r) = &self.region {
            check_text("region", r, MAX_REGION_LEN, true)?;
        }
        if self.max_players == 0 || self.max_players > MAX_PLAYERS_LIMIT {
            return Err(format!("max_players must be 1..={MAX_PLAYERS_LIMIT}"));
        }
        if self.players > self.max_players {
            return Err("players exceeds max_players".into());
        }
        if self.local_addrs.len() > MAX_LOCAL_ADDRS {
            return Err(format!("at most {MAX_LOCAL_ADDRS} local_addrs"));
        }
        Ok(())
    }
}

impl HeartbeatRequest {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(m) = &self.map {
            check_text("map", m, MAX_FIELD_LEN, true)?;
        }
        if let Some(m) = &self.mode {
            check_text("mode", m, MAX_FIELD_LEN, true)?;
        }
        if self.players > MAX_PLAYERS_LIMIT {
            return Err("players too large".into());
        }
        Ok(())
    }
}

// ------------------------------------------------------------------ UDP packets

/// Which side of a relayed session an endpoint is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Host,
    Client,
}

/// UDP control datagrams (rendezvous port, relay port and peer-to-peer punches).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "t")]
pub enum Packet {
    // ---- host -> rendezvous
    /// From the host's control socket (every few seconds): where to send join notices.
    HostControl { lobby: String, token: String },
    /// From the host's GAME socket (the one the game server reads): records its public
    /// endpoint and keeps the NAT mapping open. Never answered (the game would read it).
    HostBeat { lobby: String, token: String },
    // ---- rendezvous -> host control socket
    HostControlAck { public: SocketAddr },
    /// A client wants to join: punch towards `client` from the game socket.
    PeerJoin { session: u64, client: Vec<SocketAddr> },
    /// The client saw our punch: stop punching.
    PunchDone { session: u64 },
    /// The client fell back to the relay: open a relay leg for `session`.
    RelayRequest { session: u64, key: String, relay_port: u16 },
    // ---- client <-> rendezvous
    Join { lobby: String, nonce: u64, #[serde(default)] locals: Vec<SocketAddr> },
    JoinInfo { nonce: u64, session: u64, key: String, host: Vec<SocketAddr>, you: SocketAddr, relay_port: u16 },
    JoinError { nonce: u64, reason: String },
    /// Client -> rendezvous: punching worked.
    PunchOk { session: u64, key: String },
    // ---- peer to peer
    Punch { session: u64 },
    // ---- relay port
    RelayBind { session: u64, key: String, role: Role },
    /// `ready`: both sides are bound, data flows.
    RelayBound { session: u64, ready: bool },
    RelayError { session: u64, reason: String },
}

/// True if `b` looks like one of our control datagrams (any version).
pub fn is_control(b: &[u8]) -> bool {
    b.len() > MAGIC.len() && b[..MAGIC.len()] == MAGIC
}

impl Packet {
    pub fn encode(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(64);
        v.extend_from_slice(&MAGIC);
        v.push(PROTOCOL_VERSION);
        serde_json::to_writer(&mut v, self).expect("packet serialises");
        v
    }

    /// None for non-control datagrams, other protocol versions or malformed packets.
    pub fn decode(b: &[u8]) -> Option<Packet> {
        if !is_control(b) || b.len() > MAX_PACKET || b[MAGIC.len()] != PROTOCOL_VERSION {
            return None;
        }
        serde_json::from_slice(&b[MAGIC.len() + 1..]).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req() -> RegisterRequest {
        RegisterRequest {
            game: "claws_and_magic".into(),
            version: "0.2.7".into(),
            name: "Rosace - Horde".into(),
            mode: "Horde".into(),
            map: "Fortress".into(),
            players: 1,
            max_players: 128,
            has_password: false,
            region: None,
            local_addrs: vec!["192.168.1.10:5000".parse().unwrap()],
        }
    }

    #[test]
    fn packets_round_trip() {
        let all = [
            Packet::HostControl { lobby: "a".into(), token: "t".into() },
            Packet::HostBeat { lobby: "a".into(), token: "t".into() },
            Packet::HostControlAck { public: "1.2.3.4:5".parse().unwrap() },
            Packet::PeerJoin { session: 7, client: vec!["1.2.3.4:5".parse().unwrap()] },
            Packet::PunchDone { session: 7 },
            Packet::RelayRequest { session: 7, key: "k".into(), relay_port: 7778 },
            Packet::Join { lobby: "a".into(), nonce: 9, locals: vec![] },
            Packet::JoinInfo { nonce: 9, session: 7, key: "k".into(), host: vec![], you: "1.2.3.4:5".parse().unwrap(), relay_port: 7778 },
            Packet::JoinError { nonce: 9, reason: "x".into() },
            Packet::PunchOk { session: 7, key: "k".into() },
            Packet::Punch { session: 7 },
            Packet::RelayBind { session: 7, key: "k".into(), role: Role::Client },
            Packet::RelayBound { session: 7, ready: true },
            Packet::RelayError { session: 7, reason: "x".into() },
        ];
        for p in all {
            let b = p.encode();
            assert!(is_control(&b));
            assert!(b.len() < MAX_PACKET);
            assert_eq!(Packet::decode(&b), Some(p));
        }
    }

    #[test]
    fn foreign_datagrams_are_not_control() {
        assert_eq!(Packet::decode(b"\x01\x02\x03 netcode payload"), None);
        assert!(!is_control(b"RLB"));
        let mut b = Packet::Punch { session: 1 }.encode();
        b[4] = PROTOCOL_VERSION + 1;
        assert_eq!(Packet::decode(&b), None, "other versions are ignored");
    }

    #[test]
    fn register_validation() {
        assert!(req().validate().is_ok());
        let mut r = req();
        r.name = "x".repeat(MAX_NAME_LEN + 1);
        assert!(r.validate().is_err());
        let mut r = req();
        r.game = "Claws And Magic".into();
        assert!(r.validate().is_err());
        let mut r = req();
        r.players = 200;
        assert!(r.validate().is_err());
        let mut r = req();
        r.name = "bad\nname".into();
        assert!(r.validate().is_err());
        let mut r = req();
        r.name = "   ".into();
        assert!(r.validate().is_err());
        let mut r = req();
        r.local_addrs = vec!["1.1.1.1:1".parse().unwrap(); MAX_LOCAL_ADDRS + 1];
        assert!(r.validate().is_err());
    }
}
