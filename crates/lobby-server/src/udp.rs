//! UDP rendezvous (hole punching) and relay.
//!
//! Rendezvous (one port): the host registers two sockets with its lobby token: a CONTROL
//! socket (`HostControl`, receives join notices) and its GAME socket (`HostBeat`, never
//! answered because the game itself reads that socket). A client sends `Join`; the server
//! answers `JoinInfo` (host game endpoint + LAN candidates, session id + key) and sends
//! `PeerJoin` (client endpoints) to the host's control socket. Both sides then send `Punch`
//! datagrams to each other for a few seconds.
//!
//! Relay (another port): both ends of a session send `RelayBind { session, key, role }` from
//! the socket they will use; afterwards every non-control datagram from one bound endpoint is
//! forwarded unchanged to the other. Limits: max sessions (total and per client IP), a
//! per-session bandwidth cap, an idle timeout (see [`crate::State::sweep`]).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use lobby_proto::{MAX_LOCAL_ADDRS, Packet, Role, is_control};
use tokio::net::UdpSocket;

use crate::limits::Bandwidth;
use crate::{Session, State, http::ct_eq, random_hex, random_u64};

type Out = Vec<(Packet, SocketAddr)>;

async fn send_all(sock: &UdpSocket, out: Out) {
    for (p, to) in out {
        if let Err(e) = sock.send_to(&p.encode(), to).await {
            tracing::debug!(%to, "send failed: {e}");
        }
    }
}

/// Receives on the rendezvous port forever.
pub async fn run_rendezvous(state: Arc<State>, sock: Arc<UdpSocket>) {
    let mut buf = vec![0u8; 2048];
    loop {
        let (len, from) = match sock.recv_from(&mut buf).await {
            Ok(v) => v,
            // Windows reports ICMP "port unreachable" from an earlier send as a recv error.
            Err(e) => {
                tracing::trace!("rendezvous recv: {e}");
                continue;
            }
        };
        if !state.udp_limit.allow(from.ip(), Instant::now()) {
            continue;
        }
        let Some(packet) = Packet::decode(&buf[..len]) else { continue };
        let out = handle_rendezvous(&state, packet, from, Instant::now());
        send_all(&sock, out).await;
    }
}

/// Pure-ish rendezvous logic (state in, datagrams out): unit-testable without sockets.
pub fn handle_rendezvous(state: &State, packet: Packet, from: SocketAddr, now: Instant) -> Out {
    let mut out = Out::new();
    let mut t = state.tables();
    match packet {
        Packet::HostControl { lobby, token } => {
            if let Some(l) = t.lobbies.get_mut(&lobby)
                && ct_eq(l.token.as_bytes(), token.as_bytes())
            {
                if l.control_ep != Some(from) {
                    tracing::info!(%lobby, control = %from, "host control endpoint");
                }
                l.control_ep = Some(from);
                out.push((Packet::HostControlAck { public: from }, from));
            }
        }
        Packet::HostBeat { lobby, token } => {
            if let Some(l) = t.lobbies.get_mut(&lobby)
                && ct_eq(l.token.as_bytes(), token.as_bytes())
            {
                if l.game_ep != Some(from) {
                    tracing::info!(%lobby, game = %from, "host game endpoint");
                }
                l.game_ep = Some(from);
            }
        }
        Packet::Join { lobby, nonce, mut locals } => {
            locals.truncate(MAX_LOCAL_ADDRS);
            // A retransmitted Join (same endpoint + nonce) gets the same session back.
            let existing = t.sessions.iter().find(|(_, s)| s.join_ep == from && s.nonce == nonce && s.lobby == lobby).map(|(id, s)| (*id, s.key.clone()));
            let Some(l) = t.lobbies.get(&lobby) else {
                out.push((Packet::JoinError { nonce, reason: "no such lobby".into() }, from));
                return out;
            };
            let (Some(game_ep), Some(control_ep)) = (l.game_ep, l.control_ep) else {
                out.push((Packet::JoinError { nonce, reason: "host not reachable yet, retry".into() }, from));
                return out;
            };
            let mut host = vec![game_ep];
            host.extend(l.local_addrs.iter().copied().filter(|a| *a != game_ep));
            // Same public IP (same LAN): LAN addresses first, hairpin NAT often fails.
            if game_ep.ip() == from.ip() && host.len() > 1 {
                host.rotate_left(1);
            }
            let (session, key) = match existing {
                Some(s) => s,
                None => {
                    if t.sessions.len() >= state.config.max_sessions {
                        out.push((Packet::JoinError { nonce, reason: "server busy".into() }, from));
                        return out;
                    }
                    if t.sessions.values().filter(|s| s.client_ip == from.ip()).count() >= state.config.max_sessions_per_ip {
                        out.push((Packet::JoinError { nonce, reason: "too many sessions from this address".into() }, from));
                        return out;
                    }
                    let id = random_u64();
                    let key = random_hex(16);
                    t.sessions.insert(
                        id,
                        Session {
                            lobby: lobby.clone(),
                            key: key.clone(),
                            client_ip: from.ip(),
                            nonce,
                            join_ep: from,
                            host: None,
                            client: None,
                            last_activity: now,
                            bandwidth: Bandwidth::new(state.config.relay_bytes_per_sec, now),
                            relayed_bytes: 0,
                            dropped_bytes: 0,
                        },
                    );
                    tracing::info!(%lobby, session = id, client = %from, "join");
                    (id, key)
                }
            };
            let mut client = vec![from];
            client.extend(locals.into_iter().filter(|a| *a != from));
            out.push((Packet::JoinInfo { nonce, session, key, host, you: from, relay_port: state.relay_port }, from));
            out.push((Packet::PeerJoin { session, client }, control_ep));
        }
        Packet::PunchOk { session, key } => {
            if let Some(s) = t.sessions.get_mut(&session)
                && ct_eq(s.key.as_bytes(), key.as_bytes())
            {
                s.last_activity = now;
                let lobby = s.lobby.clone();
                tracing::info!(session, "punch succeeded");
                if let Some(ep) = t.lobbies.get(&lobby).and_then(|l| l.control_ep) {
                    out.push((Packet::PunchDone { session }, ep));
                }
            }
        }
        _ => {}
    }
    out
}

/// What the relay does with one datagram.
#[derive(Debug, PartialEq)]
pub enum RelayAction {
    /// Forward the datagram to this endpoint.
    Forward(SocketAddr),
    /// Control replies: (packet, to, via the rendezvous socket?).
    Control(Vec<(Packet, SocketAddr, bool)>),
    Drop,
}

/// Receives on the relay port forever. `rendezvous` sends relay requests to hosts' control
/// sockets (their NAT mapping points at the rendezvous port).
pub async fn run_relay(state: Arc<State>, sock: Arc<UdpSocket>, rendezvous: Arc<UdpSocket>) {
    let mut buf = vec![0u8; 2048];
    loop {
        let (len, from) = match sock.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(e) => {
                tracing::trace!("relay recv: {e}");
                continue;
            }
        };
        match relay_datagram(&state, &buf[..len], from, Instant::now()) {
            RelayAction::Forward(to) => {
                let _ = sock.send_to(&buf[..len], to).await;
            }
            RelayAction::Control(out) => {
                for (p, to, via_rendezvous) in out {
                    let s = if via_rendezvous { &rendezvous } else { &sock };
                    let _ = s.send_to(&p.encode(), to).await;
                }
            }
            RelayAction::Drop => {}
        }
    }
}

pub fn relay_datagram(state: &State, data: &[u8], from: SocketAddr, now: Instant) -> RelayAction {
    if is_control(data) {
        if !state.udp_limit.allow(from.ip(), now) {
            return RelayAction::Drop;
        }
        let Some(Packet::RelayBind { session, key, role }) = Packet::decode(data) else { return RelayAction::Drop };
        let mut t = state.tables();
        let Some(s) = t.sessions.get_mut(&session) else {
            return RelayAction::Control(vec![(Packet::RelayError { session, reason: "unknown session".into() }, from, false)]);
        };
        if !ct_eq(s.key.as_bytes(), key.as_bytes()) {
            return RelayAction::Control(vec![(Packet::RelayError { session, reason: "bad key".into() }, from, false)]);
        }
        s.last_activity = now;
        let old = match role {
            Role::Host => s.host.replace(from),
            Role::Client => s.client.replace(from),
        };
        let ready = s.ready();
        let other = s.endpoint(match role {
            Role::Host => Role::Client,
            Role::Client => Role::Host,
        });
        let lobby = s.lobby.clone();
        let host_missing = role == Role::Client && s.host.is_none();
        let key = s.key.clone();
        if let Some(old) = old
            && old != from
        {
            t.relay_index.remove(&old);
        }
        if old != Some(from) {
            tracing::info!(session, ?role, endpoint = %from, ready, "relay bind");
        }
        t.relay_index.insert(from, session);
        let mut out = vec![(Packet::RelayBound { session, ready }, from, false)];
        if ready && let Some(other) = other {
            out.push((Packet::RelayBound { session, ready }, other, false));
        }
        if host_missing {
            // Ask the host (through its control socket) to open its relay leg.
            match t.lobbies.get(&lobby).and_then(|l| l.control_ep) {
                Some(ep) => out.push((Packet::RelayRequest { session, key, relay_port: state.relay_port }, ep, true)),
                None => out.push((Packet::RelayError { session, reason: "host gone".into() }, from, false)),
            }
        }
        return RelayAction::Control(out);
    }
    let mut t = state.tables();
    let Some(id) = t.relay_index.get(&from).copied() else { return RelayAction::Drop };
    let Some(s) = t.sessions.get_mut(&id) else { return RelayAction::Drop };
    let to = if s.host == Some(from) { s.client } else { s.host };
    let Some(to) = to else { return RelayAction::Drop };
    if !s.bandwidth.take(data.len(), now) {
        s.dropped_bytes += data.len() as u64;
        return RelayAction::Drop;
    }
    s.last_activity = now;
    s.relayed_bytes += data.len() as u64;
    RelayAction::Forward(to)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Config, Lobby, Tables};
    use crate::limits::RateLimiter;
    use lobby_proto::LobbyInfo;
    use std::sync::Mutex;
    use std::time::Duration;

    fn state(cfg: impl FnOnce(&mut Config)) -> State {
        let mut c = Config::local_test();
        cfg(&mut c);
        State {
            http_limit: RateLimiter::new(1000, Duration::from_secs(60)),
            register_limit: RateLimiter::new(1000, Duration::from_secs(60)),
            udp_limit: RateLimiter::new(1000, Duration::from_secs(1)),
            tables: Mutex::new(Tables::default()),
            rendezvous_port: 7777,
            relay_port: 7778,
            config: c,
        }
    }

    fn add_lobby(s: &State) {
        let now = Instant::now();
        s.tables().lobbies.insert(
            "L".into(),
            Lobby {
                info: LobbyInfo {
                    id: "L".into(),
                    game: "g".into(),
                    version: "1".into(),
                    name: "n".into(),
                    mode: "m".into(),
                    map: "x".into(),
                    players: 1,
                    max_players: 4,
                    has_password: false,
                    region: None,
                    age_secs: 0,
                },
                token: "T".into(),
                owner_ip: "1.1.1.1".parse().unwrap(),
                created: now,
                last_seen: now,
                local_addrs: vec!["192.168.0.5:5000".parse().unwrap()],
                game_ep: None,
                control_ep: None,
            },
        );
    }

    fn a(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    /// Host registers, a client joins: returns (session, key).
    fn join(s: &State, client: SocketAddr) -> (u64, String) {
        let now = Instant::now();
        handle_rendezvous(s, Packet::HostControl { lobby: "L".into(), token: "T".into() }, a("1.1.1.1:6000"), now);
        handle_rendezvous(s, Packet::HostBeat { lobby: "L".into(), token: "T".into() }, a("1.1.1.1:5000"), now);
        let out = handle_rendezvous(s, Packet::Join { lobby: "L".into(), nonce: 1, locals: vec![] }, client, now);
        match &out[0].0 {
            Packet::JoinInfo { session, key, host, you, .. } => {
                assert_eq!(host[0], a("1.1.1.1:5000"));
                assert_eq!(*you, client);
                assert_eq!(out[1], (Packet::PeerJoin { session: *session, client: vec![client] }, a("1.1.1.1:6000")));
                (*session, key.clone())
            }
            p => panic!("unexpected {p:?}"),
        }
    }

    #[test]
    fn join_before_host_is_reachable_fails() {
        let s = state(|_| {});
        add_lobby(&s);
        let out = handle_rendezvous(&s, Packet::Join { lobby: "L".into(), nonce: 1, locals: vec![] }, a("2.2.2.2:1"), Instant::now());
        assert!(matches!(out[0].0, Packet::JoinError { .. }));
        let out = handle_rendezvous(&s, Packet::HostControl { lobby: "L".into(), token: "wrong".into() }, a("1.1.1.1:6000"), Instant::now());
        assert!(out.is_empty(), "bad token ignored");
    }

    #[test]
    fn join_is_idempotent_and_capped() {
        let s = state(|c| c.max_sessions_per_ip = 2);
        add_lobby(&s);
        let (id, _) = join(&s, a("2.2.2.2:1"));
        let (again, _) = join(&s, a("2.2.2.2:1"));
        assert_eq!(id, again, "retransmitted join reuses the session");
        let out = handle_rendezvous(&s, Packet::Join { lobby: "L".into(), nonce: 2, locals: vec![] }, a("2.2.2.2:2"), Instant::now());
        assert!(matches!(out[0].0, Packet::JoinInfo { .. }));
        let out = handle_rendezvous(&s, Packet::Join { lobby: "L".into(), nonce: 3, locals: vec![] }, a("2.2.2.2:3"), Instant::now());
        assert!(matches!(&out[0].0, Packet::JoinError { reason, .. } if reason.contains("too many")));
    }

    #[test]
    fn same_public_ip_tries_lan_first() {
        let s = state(|_| {});
        add_lobby(&s);
        let now = Instant::now();
        handle_rendezvous(&s, Packet::HostControl { lobby: "L".into(), token: "T".into() }, a("1.1.1.1:6000"), now);
        handle_rendezvous(&s, Packet::HostBeat { lobby: "L".into(), token: "T".into() }, a("1.1.1.1:5000"), now);
        let out = handle_rendezvous(&s, Packet::Join { lobby: "L".into(), nonce: 1, locals: vec![] }, a("1.1.1.1:9999"), now);
        let Packet::JoinInfo { host, .. } = &out[0].0 else { panic!() };
        assert_eq!(host, &vec![a("192.168.0.5:5000"), a("1.1.1.1:5000")]);
    }

    #[test]
    fn relay_binds_forwards_and_caps_bandwidth() {
        let s = state(|c| c.relay_bytes_per_sec = 1000);
        add_lobby(&s);
        let (id, key) = join(&s, a("2.2.2.2:1"));
        let now = Instant::now();
        let client = a("2.2.2.2:1");
        let host_leg = a("1.1.1.1:7000");
        // Data before binding is dropped.
        assert_eq!(relay_datagram(&s, b"x", client, now), RelayAction::Drop);
        // Bad key.
        let bad = Packet::RelayBind { session: id, key: "nope".into(), role: Role::Client }.encode();
        assert!(matches!(relay_datagram(&s, &bad, client, now), RelayAction::Control(v) if matches!(v[0].0, Packet::RelayError { .. })));
        // Client binds first: the host is asked (via rendezvous) to open its leg.
        let bind = Packet::RelayBind { session: id, key: key.clone(), role: Role::Client }.encode();
        let RelayAction::Control(out) = relay_datagram(&s, &bind, client, now) else { panic!() };
        assert_eq!(out[0], (Packet::RelayBound { session: id, ready: false }, client, false));
        assert!(matches!(&out[1], (Packet::RelayRequest { session, .. }, to, true) if *session == id && *to == a("1.1.1.1:6000")));
        let bind = Packet::RelayBind { session: id, key, role: Role::Host }.encode();
        let RelayAction::Control(out) = relay_datagram(&s, &bind, host_leg, now) else { panic!() };
        assert_eq!(out.len(), 2, "both sides told the relay is ready");
        assert!(out.iter().all(|(p, _, _)| *p == Packet::RelayBound { session: id, ready: true }));
        assert_eq!(relay_datagram(&s, &[1; 400], client, now), RelayAction::Forward(host_leg));
        assert_eq!(relay_datagram(&s, &[2; 400], host_leg, now), RelayAction::Forward(client));
        assert_eq!(relay_datagram(&s, &[3; 400], host_leg, now), RelayAction::Drop, "over 1000 B/s");
        assert_eq!(relay_datagram(&s, &[3; 400], host_leg, now + Duration::from_secs(1)), RelayAction::Forward(client));
        assert_eq!(relay_datagram(&s, b"x", a("9.9.9.9:9"), now), RelayAction::Drop, "strangers are ignored");
    }

    #[test]
    fn idle_sessions_and_silent_lobbies_expire() {
        let s = state(|c| {
            c.relay_idle_secs = 5;
            c.expire_secs = 20;
        });
        add_lobby(&s);
        let (id, key) = join(&s, a("2.2.2.2:1"));
        let bind = Packet::RelayBind { session: id, key, role: Role::Client }.encode();
        relay_datagram(&s, &bind, a("2.2.2.2:1"), Instant::now());
        let later = Instant::now() + Duration::from_secs(6);
        s.sweep(later);
        assert!(s.tables().sessions.is_empty());
        assert!(s.tables().relay_index.is_empty());
        assert_eq!(s.tables().lobbies.len(), 1);
        s.sweep(Instant::now() + Duration::from_secs(21));
        assert!(s.tables().lobbies.is_empty());
    }
}
