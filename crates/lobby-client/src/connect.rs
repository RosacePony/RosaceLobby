//! Client side: rendezvous, hole punching, relay fallback.

use std::net::{SocketAddr, UdpSocket};
use std::time::{Duration, Instant};

use lobby_proto::{Packet, Role};

use crate::net::{disable_udp_connreset, drain, lan_ip, recv, send};
use crate::{Error, LobbyClient};

#[derive(Clone, Debug)]
pub struct ConnectOptions {
    /// Skip punching and go straight to the relay.
    pub force_relay: bool,
    /// Give up punching after this long and fall back to the relay.
    pub punch_timeout: Duration,
    /// Rendezvous answer timeout.
    pub join_timeout: Duration,
    /// Relay setup timeout.
    pub relay_timeout: Duration,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self { force_relay: false, punch_timeout: Duration::from_secs(3), join_timeout: Duration::from_secs(5), relay_timeout: Duration::from_secs(6) }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    /// Direct path through both NATs.
    Punched,
    /// Through the lobby server's relay.
    Relayed,
}

/// A ready path to the host: give `socket` and `server_addr` to the game's transport.
#[derive(Debug)]
pub struct Connection {
    pub socket: UdpSocket,
    pub server_addr: SocketAddr,
    pub route: Route,
    pub session: u64,
}

/// Joins `lobby_id`: asks the rendezvous for the host's endpoints, punches for
/// `punch_timeout`, else binds the socket to the relay. Blocking (seconds): call it from a
/// thread in a game loop.
pub fn connect(client: &LobbyClient, lobby_id: &str, opts: &ConnectOptions) -> Result<Connection, Error> {
    let info = client.info()?;
    let rendezvous = client.udp_addr(info.rendezvous_port)?;
    let sock = UdpSocket::bind("0.0.0.0:0")?;
    disable_udp_connreset(&sock);
    sock.set_read_timeout(Some(Duration::from_millis(20)))?;
    let locals: Vec<SocketAddr> = lan_ip().map(|ip| SocketAddr::new(ip, sock.local_addr().map(|a| a.port()).unwrap_or(0))).into_iter().collect();
    let nonce = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(1) ^ u64::from(std::process::id());
    let mut buf = [0u8; 2048];

    // 1. Rendezvous.
    let join = Packet::Join { lobby: lobby_id.to_string(), nonce, locals };
    let start = Instant::now();
    let mut last_sent = start - Duration::from_secs(1);
    let mut last_error = None;
    let (session, key, hosts, relay_port) = loop {
        if start.elapsed() > opts.join_timeout {
            return Err(match last_error {
                Some(e) => Error::Refused(e),
                None => Error::Timeout("no answer from the rendezvous server (UDP blocked?)".into()),
            });
        }
        if last_sent.elapsed() >= Duration::from_millis(500) {
            send(&sock, &join, rendezvous);
            last_sent = Instant::now();
        }
        if let Some((data, from)) = recv(&sock, &mut buf)
            && from == rendezvous
        {
            match Packet::decode(data) {
                Some(Packet::JoinInfo { nonce: n, session, key, host, relay_port, .. }) if n == nonce => break (session, key, host, relay_port),
                // "Host not reachable yet" is retried until the timeout.
                Some(Packet::JoinError { nonce: n, reason }) if n == nonce => {
                    if reason.contains("no such lobby") {
                        return Err(Error::Refused(reason));
                    }
                    last_error = Some(reason);
                }
                _ => {}
            }
        }
    };

    // 2. Hole punching: the host punches from its game socket at the same time.
    if !opts.force_relay && !hosts.is_empty() {
        let start = Instant::now();
        let mut last = start - Duration::from_secs(1);
        while start.elapsed() < opts.punch_timeout {
            if last.elapsed() >= Duration::from_millis(100) {
                for h in &hosts {
                    send(&sock, &Packet::Punch { session }, *h);
                }
                last = Instant::now();
            }
            if let Some((data, from)) = recv(&sock, &mut buf)
                && let Some(Packet::Punch { session: s }) = Packet::decode(data)
                && s == session
            {
                // Tell the host (via the rendezvous) to stop punching.
                for _ in 0..3 {
                    send(&sock, &Packet::PunchOk { session, key: key.clone() }, rendezvous);
                }
                return finish(sock, from, Route::Punched, session);
            }
        }
    }

    // 3. Relay.
    let relay = SocketAddr::new(rendezvous.ip(), relay_port);
    let bind = Packet::RelayBind { session, key, role: Role::Client };
    let start = Instant::now();
    let mut last = start - Duration::from_secs(1);
    while start.elapsed() < opts.relay_timeout {
        if last.elapsed() >= Duration::from_millis(300) {
            send(&sock, &bind, relay);
            last = Instant::now();
        }
        if let Some((data, from)) = recv(&sock, &mut buf)
            && from == relay
        {
            match Packet::decode(data) {
                Some(Packet::RelayBound { session: s, ready: true }) if s == session => return finish(sock, relay, Route::Relayed, session),
                Some(Packet::RelayError { reason, .. }) => return Err(Error::Refused(format!("relay: {reason}"))),
                _ => {}
            }
        }
    }
    Err(Error::Timeout("relay did not connect the host".into()))
}

fn finish(sock: UdpSocket, server_addr: SocketAddr, route: Route, session: u64) -> Result<Connection, Error> {
    drain(&sock);
    sock.set_read_timeout(None)?;
    Ok(Connection { socket: sock, server_addr, route, session })
}
