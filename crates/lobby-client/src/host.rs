//! Host side: lobby registration + heartbeat, rendezvous registration of the game socket,
//! punching towards joining clients, relay legs.
//!
//! Socket design (the game server keeps sole READ ownership of its UDP socket):
//! - GAME socket `G` (the game server's, e.g. renet's): this agent only SENDS through a
//!   `try_clone` of it: `HostBeat` to the rendezvous every 2 s (records G's public endpoint
//!   and keeps its NAT mapping open) and `Punch` packets to joining clients. Nothing is ever
//!   sent TO G by the lobby service, so the game never has to parse foreign packets except
//!   the clients' own short-lived punches, which a netcode transport drops as invalid.
//! - CONTROL socket `C` (owned by the agent): `HostControl` registration; receives
//!   `PeerJoin` (punch this client), `PunchDone` and `RelayRequest`.
//! - One RELAY LEG socket per relayed client: binds to the relay with the session key, then
//!   forwards relay -> `127.0.0.1:<game port>` and back. The game sees each relayed client
//!   as a distinct local address, so many relayed clients can share one relay.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use lobby_proto::{HeartbeatRequest, Packet, RegisterRequest, Role};

use crate::net::{disable_udp_connreset, lan_ip, recv, send};
use crate::{Error, LobbyClient};

/// How long the host punches towards a new client.
const PUNCH_FOR: Duration = Duration::from_secs(5);
const PUNCH_EVERY: Duration = Duration::from_millis(100);
const BEAT_EVERY: Duration = Duration::from_secs(2);
const LEG_IDLE: Duration = Duration::from_secs(45);

#[derive(Clone, Debug)]
pub struct HostOptions {
    /// Punch towards joining clients (false = every client ends up relayed; for tests).
    pub punch: bool,
}

impl Default for HostOptions {
    fn default() -> Self {
        Self { punch: true }
    }
}

/// What the agent is doing (for the UI / logs).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HostStatus {
    pub lobby_id: Option<String>,
    /// Public endpoint of the control socket as seen by the rendezvous server.
    pub control_public: Option<SocketAddr>,
    pub last_error: Option<String>,
    pub joins: u32,
    pub punched: u32,
    pub relay_legs: u32,
}

#[derive(Clone, Debug)]
struct Creds {
    id: String,
    token: String,
    rendezvous: SocketAddr,
    heartbeat: Duration,
}

struct Shared {
    stop: AtomicBool,
    players: AtomicU32,
    status: Mutex<HostStatus>,
    creds: Mutex<Option<Creds>>,
}

impl Shared {
    fn stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }
    fn status(&self) -> std::sync::MutexGuard<'_, HostStatus> {
        self.status.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn creds(&self) -> Option<Creds> {
        self.creds.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
    fn set_creds(&self, c: Option<Creds>) {
        *self.creds.lock().unwrap_or_else(|e| e.into_inner()) = c;
    }
}

/// Keeps a public lobby alive while the game server runs. Dropping it (or [`HostAgent::stop`])
/// stops every thread and unregisters the lobby.
pub struct HostAgent {
    shared: Arc<Shared>,
    threads: Vec<thread::JoinHandle<()>>,
}

impl HostAgent {
    /// Starts the agent threads; returns at once (registration happens in the background).
    /// `game_socket` is the game server's own UDP socket (cloned, send-only).
    pub fn start(client: LobbyClient, mut req: RegisterRequest, game_socket: &UdpSocket, opts: HostOptions) -> Result<HostAgent, Error> {
        req.validate().map_err(Error::Invalid)?;
        let game_tx = game_socket.try_clone()?;
        let game_local = game_socket.local_addr()?;
        let game_loopback = SocketAddr::new(if game_local.ip().is_unspecified() { IpAddr::V4(Ipv4Addr::LOCALHOST) } else { game_local.ip() }, game_local.port());
        if req.local_addrs.is_empty()
            && let Some(ip) = lan_ip()
        {
            req.local_addrs.push(SocketAddr::new(ip, game_local.port()));
        }
        let shared = Arc::new(Shared {
            stop: AtomicBool::new(false),
            players: AtomicU32::new(req.players),
            status: Mutex::new(HostStatus::default()),
            creds: Mutex::new(None),
        });
        let control = UdpSocket::bind("0.0.0.0:0")?;
        disable_udp_connreset(&control);
        control.set_read_timeout(Some(Duration::from_millis(20)))?;
        let s1 = shared.clone();
        let http = thread::Builder::new().name("lobby-http".into()).spawn(move || http_loop(client, req, &s1))?;
        let s2 = shared.clone();
        let udp = thread::Builder::new().name("lobby-udp".into()).spawn(move || udp_loop(control, game_tx, game_loopback, opts, s2))?;
        Ok(HostAgent { shared, threads: vec![http, udp] })
    }

    /// Players in the game (host included), sent with the next heartbeat.
    pub fn set_players(&self, n: u32) {
        self.shared.players.store(n, Ordering::Relaxed);
    }

    pub fn status(&self) -> HostStatus {
        self.shared.status().clone()
    }

    /// Stops and waits for the lobby to be unregistered (a few seconds at most).
    pub fn stop(mut self) {
        self.shared.stop.store(true, Ordering::Relaxed);
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

impl Drop for HostAgent {
    fn drop(&mut self) {
        // Threads finish on their own (the HTTP thread unregisters the lobby first).
        self.shared.stop.store(true, Ordering::Relaxed);
    }
}

/// Sleeps up to `d`, waking early on stop. Returns false if stopped.
fn nap(shared: &Shared, d: Duration) -> bool {
    let end = Instant::now() + d;
    while Instant::now() < end {
        if shared.stopped() {
            return false;
        }
        thread::sleep(Duration::from_millis(50).min(end.saturating_duration_since(Instant::now())));
    }
    !shared.stopped()
}

fn http_loop(client: LobbyClient, req: RegisterRequest, shared: &Shared) {
    let mut retry = Duration::from_secs(2);
    while !shared.stopped() {
        let Some(c) = shared.creds() else {
            req_register(&client, &req, shared, &mut retry);
            continue;
        };
        if !nap(shared, c.heartbeat) {
            break;
        }
        let hb = HeartbeatRequest { players: shared.players.load(Ordering::Relaxed), map: None, mode: None };
        match client.heartbeat(&c.id, &c.token, &hb) {
            Ok(()) => shared.status().last_error = None,
            Err(Error::Status(code @ (403 | 404), e)) => {
                // Expired (e.g. the network dropped for 20 s): register again.
                shared.status().last_error = Some(format!("heartbeat {code}: {e}"));
                shared.set_creds(None);
            }
            Err(e) => shared.status().last_error = Some(e.to_string()),
        }
    }
    if let Some(c) = shared.creds() {
        let _ = client.unregister(&c.id, &c.token);
    }
}

fn req_register(client: &LobbyClient, req: &RegisterRequest, shared: &Shared, retry: &mut Duration) {
    let mut req = req.clone();
    req.players = shared.players.load(Ordering::Relaxed).min(req.max_players);
    let r = client.register(&req).and_then(|r| Ok((client.udp_addr(r.rendezvous_port)?, r)));
    match r {
        Ok((rendezvous, r)) => {
            *retry = Duration::from_secs(2);
            {
                let mut st = shared.status();
                st.lobby_id = Some(r.id.clone());
                st.last_error = None;
            }
            shared.set_creds(Some(Creds { id: r.id, token: r.token, rendezvous, heartbeat: Duration::from_secs(r.heartbeat_secs.clamp(1, 60)) }));
        }
        Err(e) => {
            shared.status().last_error = Some(e.to_string());
            nap(shared, *retry);
            *retry = (*retry * 2).min(Duration::from_secs(30));
        }
    }
}

struct PunchJob {
    targets: Vec<SocketAddr>,
    until: Instant,
}

fn udp_loop(control: UdpSocket, game_tx: UdpSocket, game_loopback: SocketAddr, opts: HostOptions, shared: Arc<Shared>) {
    let mut buf = [0u8; 2048];
    let mut last_beat: Option<Instant> = None;
    let mut acked_for: Option<String> = None;
    let mut punches: HashMap<u64, PunchJob> = HashMap::new();
    let mut last_punch = Instant::now();
    let mut legs: HashSet<u64> = HashSet::new();
    let mut seen_joins: HashSet<u64> = HashSet::new();
    while !shared.stopped() {
        let Some(c) = shared.creds() else {
            nap(&shared, Duration::from_millis(100));
            continue;
        };
        let acked = acked_for.as_deref() == Some(c.id.as_str());
        // Fast until the control socket is acknowledged, then every 2 s.
        let every = if acked { BEAT_EVERY } else { Duration::from_millis(250) };
        if last_beat.is_none_or(|t| t.elapsed() >= every) {
            send(&control, &Packet::HostControl { lobby: c.id.clone(), token: c.token.clone() }, c.rendezvous);
            send(&game_tx, &Packet::HostBeat { lobby: c.id.clone(), token: c.token.clone() }, c.rendezvous);
            last_beat = Some(Instant::now());
        }
        if let Some((data, from)) = recv(&control, &mut buf)
            && from.ip() == c.rendezvous.ip()
            && let Some(p) = Packet::decode(data)
        {
            match p {
                Packet::HostControlAck { public } => {
                    acked_for = Some(c.id.clone());
                    shared.status().control_public = Some(public);
                }
                Packet::PeerJoin { session, client } => {
                    if seen_joins.insert(session) {
                        shared.status().joins += 1;
                    }
                    if opts.punch && !punches.contains_key(&session) {
                        punches.insert(session, PunchJob { targets: client, until: Instant::now() + PUNCH_FOR });
                    }
                }
                Packet::PunchDone { session } => {
                    if punches.remove(&session).is_some() {
                        shared.status().punched += 1;
                    }
                }
                Packet::RelayRequest { session, key, relay_port } => {
                    punches.remove(&session);
                    if legs.insert(session) {
                        shared.status().relay_legs += 1;
                        let relay = SocketAddr::new(c.rendezvous.ip(), relay_port);
                        let s = shared.clone();
                        let _ = thread::Builder::new().name(format!("lobby-leg-{session:x}")).spawn(move || relay_leg(session, key, relay, game_loopback, &s));
                    }
                }
                _ => {}
            }
        }
        if last_punch.elapsed() >= PUNCH_EVERY {
            last_punch = Instant::now();
            punches.retain(|_, j| j.until > last_punch);
            for (session, job) in &punches {
                for t in &job.targets {
                    send(&game_tx, &Packet::Punch { session: *session }, *t);
                }
            }
        }
    }
}

/// Forwards one relayed client: relay <-> the game's socket on loopback.
fn relay_leg(session: u64, key: String, relay: SocketAddr, game: SocketAddr, shared: &Shared) {
    let Ok(sock) = UdpSocket::bind("0.0.0.0:0") else { return };
    disable_udp_connreset(&sock);
    let _ = sock.set_read_timeout(Some(Duration::from_millis(100)));
    let bind = Packet::RelayBind { session, key, role: Role::Host };
    let mut buf = [0u8; 2048];
    let mut bound = false;
    let mut last_bind = Instant::now() - Duration::from_secs(10);
    let mut last_traffic = Instant::now();
    while !shared.stopped() && last_traffic.elapsed() < LEG_IDLE {
        // Re-bind until acknowledged, then every 10 s (keeps our NAT mapping to the relay).
        if last_bind.elapsed() >= if bound { Duration::from_secs(10) } else { Duration::from_millis(300) } {
            send(&sock, &bind, relay);
            last_bind = Instant::now();
        }
        let Ok((n, from)) = sock.recv_from(&mut buf) else { continue };
        let data = &buf[..n];
        if from == relay {
            if lobby_proto::is_control(data) {
                match Packet::decode(data) {
                    Some(Packet::RelayBound { .. }) => bound = true,
                    Some(Packet::RelayError { .. }) => break,
                    _ => {}
                }
            } else {
                last_traffic = Instant::now();
                let _ = sock.send_to(data, game);
            }
        } else if from == game {
            last_traffic = Instant::now();
            let _ = sock.send_to(data, relay);
        }
    }
}
