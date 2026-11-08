//! In-process end-to-end tests: the real server on loopback ports, the real client library.

use std::net::{SocketAddr, UdpSocket};
use std::thread;
use std::time::{Duration, Instant};

use lobby_client::{ConnectOptions, HostAgent, HostOptions, LobbyClient, Route, connect};
use lobby_proto::{HeartbeatRequest, Packet, RegisterRequest, Role};
use lobby_server::{Config, Running, start};

fn req(name: &str) -> RegisterRequest {
    RegisterRequest {
        game: "claws_and_magic".into(),
        version: "0.2.7".into(),
        name: name.into(),
        mode: "Horde co-op".into(),
        map: "Fortress".into(),
        players: 1,
        max_players: 128,
        has_password: false,
        region: None,
        local_addrs: vec![],
    }
}

async fn server(cfg: impl FnOnce(&mut Config)) -> Running {
    let mut c = Config::local_test();
    cfg(&mut c);
    start(c).await.expect("server starts")
}

/// Runs blocking client code off the async runtime.
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(f).await.expect("blocking task")
}

/// A stand-in for the game server: echoes every non-control datagram (like renet, it drops
/// lobby control packets such as stray punches).
fn echo_game_socket() -> (UdpSocket, thread::JoinHandle<()>) {
    let s = UdpSocket::bind("127.0.0.1:0").unwrap();
    let reader = s.try_clone().unwrap();
    reader.set_read_timeout(Some(Duration::from_millis(100))).unwrap();
    let h = thread::spawn(move || {
        let mut buf = [0u8; 2048];
        let end = Instant::now() + Duration::from_secs(30);
        while Instant::now() < end {
            if let Ok((n, from)) = reader.recv_from(&mut buf) {
                if lobby_proto::is_control(&buf[..n]) {
                    continue;
                }
                if &buf[..n] == b"quit" {
                    break;
                }
                let mut reply = b"echo:".to_vec();
                reply.extend_from_slice(&buf[..n]);
                reply.extend_from_slice(format!(" from {from}").as_bytes());
                let _ = reader.send_to(&reply, from);
            }
        }
    });
    (s, h)
}

/// Sends `msg` through a connection and waits for the echo.
fn roundtrip(sock: &UdpSocket, to: SocketAddr, msg: &str) -> String {
    sock.set_read_timeout(Some(Duration::from_millis(200))).unwrap();
    let mut buf = [0u8; 2048];
    for _ in 0..20 {
        sock.send_to(msg.as_bytes(), to).unwrap();
        if let Ok((n, from)) = sock.recv_from(&mut buf) {
            assert_eq!(from, to, "replies come from the server address");
            let s = String::from_utf8_lossy(&buf[..n]).to_string();
            if lobby_proto::is_control(&buf[..n]) {
                continue;
            }
            return s;
        }
    }
    panic!("no echo for {msg}");
}

fn wait_registered(agent: &HostAgent) -> String {
    let end = Instant::now() + Duration::from_secs(5);
    while Instant::now() < end {
        let st = agent.status();
        if let (Some(id), Some(_)) = (st.lobby_id, st.control_public) {
            // Give the game-socket HostBeat a moment too.
            thread::sleep(Duration::from_millis(300));
            return id;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("host never registered: {:?}", agent.status());
}

#[tokio::test(flavor = "multi_thread")]
async fn register_list_heartbeat_expire_delete() {
    let srv = server(|c| c.expire_secs = 1).await;
    let url = srv.http_url();
    blocking(move || {
        let c = LobbyClient::new(&url);
        assert_eq!(c.health().unwrap().status, "ok");
        let r = c.register(&req("Rosace - Horde")).unwrap();
        assert_eq!(r.heartbeat_secs, 5);
        let list = c.list("claws_and_magic", Some("0.2.7")).unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].name, "Rosace - Horde");
        assert_eq!(list[0].max_players, 128);
        assert!(c.list("claws_and_magic", Some("0.2.6")).unwrap().is_empty(), "version filter");
        assert!(c.list("other_game", None).unwrap().is_empty(), "game filter");
        c.heartbeat(&r.id, &r.token, &HeartbeatRequest { players: 3, ..Default::default() }).unwrap();
        assert_eq!(c.list("claws_and_magic", None).unwrap()[0].players, 3);
        // Bad token.
        assert!(matches!(c.heartbeat(&r.id, "nope", &HeartbeatRequest::default()), Err(lobby_client::Error::Status(403, _))));
        assert!(matches!(c.unregister(&r.id, "nope"), Err(lobby_client::Error::Status(403, _))));
        // Silence -> expired.
        thread::sleep(Duration::from_millis(2600));
        assert!(c.list("claws_and_magic", None).unwrap().is_empty(), "expired after silence");
        assert!(matches!(c.heartbeat(&r.id, &r.token, &HeartbeatRequest::default()), Err(lobby_client::Error::Status(404, _))));
        // Delete.
        let r = c.register(&req("Second")).unwrap();
        c.unregister(&r.id, &r.token).unwrap();
        assert!(c.list("claws_and_magic", None).unwrap().is_empty());
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn validation_and_rate_limits() {
    let srv = server(|c| {
        c.register_per_min = 2;
        c.rate_per_min = 8;
    })
    .await;
    let url = srv.http_url();
    blocking(move || {
        let c = LobbyClient::new(&url);
        // Server-side validation (raw request: the client library validates first).
        let agent: ureq::Agent = ureq::Agent::config_builder().http_status_as_error(false).build().into();
        let mut bad = req("x");
        bad.name = "n".repeat(200);
        let resp = agent.post(format!("{url}/v1/lobbies")).send_json(&bad).unwrap();
        assert_eq!(resp.status().as_u16(), 400);
        assert!(matches!(c.register(&bad), Err(lobby_client::Error::Invalid(_))));
        assert!(matches!(c.list("Bad Game", None), Err(lobby_client::Error::Status(400, _))));
        c.register(&req("a")).unwrap();
        assert!(matches!(c.register(&req("c")), Err(lobby_client::Error::Status(429, _))), "2 registrations per minute");
        // 8 requests per minute in total: the rest are refused, /health stays open.
        let mut limited = false;
        for _ in 0..8 {
            if let Err(lobby_client::Error::Status(429, _)) = c.list("claws_and_magic", None) {
                limited = true;
            }
        }
        assert!(limited);
        assert_eq!(c.health().unwrap().status, "ok");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rendezvous_exchanges_public_endpoints() {
    let srv = server(|_| {}).await;
    let (url, rdv) = (srv.http_url(), srv.rendezvous_addr);
    blocking(move || {
        let c = LobbyClient::new(&url);
        let r = c.register(&req("Raw")).unwrap();
        let control = UdpSocket::bind("127.0.0.1:0").unwrap();
        let game = UdpSocket::bind("127.0.0.1:0").unwrap();
        let client = UdpSocket::bind("127.0.0.1:0").unwrap();
        for s in [&control, &game, &client] {
            s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        }
        let mut buf = [0u8; 2048];
        control.send_to(&Packet::HostControl { lobby: r.id.clone(), token: r.token.clone() }.encode(), rdv).unwrap();
        let (n, _) = control.recv_from(&mut buf).unwrap();
        assert_eq!(Packet::decode(&buf[..n]), Some(Packet::HostControlAck { public: control.local_addr().unwrap() }));
        game.send_to(&Packet::HostBeat { lobby: r.id.clone(), token: r.token.clone() }.encode(), rdv).unwrap();
        thread::sleep(Duration::from_millis(100));
        client.send_to(&Packet::Join { lobby: r.id.clone(), nonce: 42, locals: vec![] }.encode(), rdv).unwrap();
        let (n, _) = client.recv_from(&mut buf).unwrap();
        let Some(Packet::JoinInfo { nonce, session, host, you, relay_port, .. }) = Packet::decode(&buf[..n]) else { panic!("no JoinInfo") };
        assert_eq!(nonce, 42);
        assert_eq!(host, vec![game.local_addr().unwrap()], "client learns the host GAME socket endpoint");
        assert_eq!(you, client.local_addr().unwrap());
        assert_eq!(relay_port, srv_relay_port(&url));
        let (n, _) = control.recv_from(&mut buf).unwrap();
        assert_eq!(Packet::decode(&buf[..n]), Some(Packet::PeerJoin { session, client: vec![client.local_addr().unwrap()] }), "host learns the client endpoint");
        // Unknown lobby.
        client.send_to(&Packet::Join { lobby: "nope".into(), nonce: 1, locals: vec![] }.encode(), rdv).unwrap();
        let (n, _) = client.recv_from(&mut buf).unwrap();
        assert!(matches!(Packet::decode(&buf[..n]), Some(Packet::JoinError { .. })));
    })
    .await;
}

fn srv_relay_port(url: &str) -> u16 {
    LobbyClient::new(url).info().unwrap().relay_port
}

#[tokio::test(flavor = "multi_thread")]
async fn relay_forwards_between_two_udp_sockets() {
    let srv = server(|_| {}).await;
    let (url, rdv, relay) = (srv.http_url(), srv.rendezvous_addr, srv.relay_addr);
    blocking(move || {
        let c = LobbyClient::new(&url);
        let r = c.register(&req("Relay")).unwrap();
        let control = UdpSocket::bind("127.0.0.1:0").unwrap();
        let game = UdpSocket::bind("127.0.0.1:0").unwrap();
        let client = UdpSocket::bind("127.0.0.1:0").unwrap();
        let leg = UdpSocket::bind("127.0.0.1:0").unwrap();
        for s in [&control, &game, &client, &leg] {
            s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        }
        let mut buf = [0u8; 2048];
        control.send_to(&Packet::HostControl { lobby: r.id.clone(), token: r.token.clone() }.encode(), rdv).unwrap();
        control.recv_from(&mut buf).unwrap();
        game.send_to(&Packet::HostBeat { lobby: r.id.clone(), token: r.token.clone() }.encode(), rdv).unwrap();
        thread::sleep(Duration::from_millis(100));
        client.send_to(&Packet::Join { lobby: r.id.clone(), nonce: 1, locals: vec![] }.encode(), rdv).unwrap();
        let (n, _) = client.recv_from(&mut buf).unwrap();
        let Some(Packet::JoinInfo { session, key, .. }) = Packet::decode(&buf[..n]) else { panic!() };
        control.recv_from(&mut buf).unwrap(); // PeerJoin
        // Client binds: not ready yet, the host control socket gets a RelayRequest.
        client.send_to(&Packet::RelayBind { session, key: key.clone(), role: Role::Client }.encode(), relay).unwrap();
        let (n, _) = client.recv_from(&mut buf).unwrap();
        assert_eq!(Packet::decode(&buf[..n]), Some(Packet::RelayBound { session, ready: false }));
        let (n, from) = control.recv_from(&mut buf).unwrap();
        assert_eq!(from, rdv, "relay requests come through the rendezvous port");
        assert!(matches!(Packet::decode(&buf[..n]), Some(Packet::RelayRequest { session: s, .. }) if s == session));
        leg.send_to(&Packet::RelayBind { session, key, role: Role::Host }.encode(), relay).unwrap();
        let (n, _) = leg.recv_from(&mut buf).unwrap();
        assert_eq!(Packet::decode(&buf[..n]), Some(Packet::RelayBound { session, ready: true }));
        let (n, _) = client.recv_from(&mut buf).unwrap();
        assert_eq!(Packet::decode(&buf[..n]), Some(Packet::RelayBound { session, ready: true }));
        // Raw datagrams flow unchanged both ways.
        let payload: Vec<u8> = (0..=255u8).collect();
        client.send_to(&payload, relay).unwrap();
        let (n, from) = leg.recv_from(&mut buf).unwrap();
        assert_eq!((&buf[..n], from), (&payload[..], relay));
        leg.send_to(b"\x07netcode bytes", relay).unwrap();
        let (n, from) = client.recv_from(&mut buf).unwrap();
        assert_eq!((&buf[..n], from), (&b"\x07netcode bytes"[..], relay));
        // A stranger cannot inject.
        let stranger = UdpSocket::bind("127.0.0.1:0").unwrap();
        stranger.send_to(b"spoof", relay).unwrap();
        leg.set_read_timeout(Some(Duration::from_millis(300))).unwrap();
        assert!(leg.recv_from(&mut buf).is_err());
    })
    .await;
    let h = srv.state.tables().sessions.values().map(|s| s.relayed_bytes).sum::<u64>();
    assert!(h > 256);
}

#[tokio::test(flavor = "multi_thread")]
async fn host_agent_and_connect_punch_on_loopback() {
    let srv = server(|_| {}).await;
    let url = srv.http_url();
    let (route, echo) = blocking(move || {
        let (game, echo) = echo_game_socket();
        let agent = HostAgent::start(LobbyClient::new(&url), req("Punch me"), &game, HostOptions::default()).unwrap();
        let id = wait_registered(&agent);
        let conn = connect(&LobbyClient::new(&url), &id, &ConnectOptions::default()).unwrap();
        assert_eq!(conn.server_addr, game.local_addr().unwrap(), "punched straight to the host's game socket");
        let reply = roundtrip(&conn.socket, conn.server_addr, "hello");
        thread::sleep(Duration::from_millis(300));
        assert_eq!(agent.status().punched, 1, "host stopped punching after PunchOk");
        agent.stop();
        game.send_to(b"quit", game.local_addr().unwrap()).unwrap();
        echo.join().unwrap();
        (conn.route, reply)
    })
    .await;
    assert_eq!(route, Route::Punched);
    assert!(echo.starts_with("echo:hello"), "{echo}");
    // The agent unregistered on stop.
    assert!(srv.state.tables().lobbies.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn punch_failure_falls_back_to_relay_and_forced_relay_works() {
    let srv = server(|_| {}).await;
    let url = srv.http_url();
    let results = blocking(move || {
        let (game, echo) = echo_game_socket();
        // The host never punches: the client's punch attempt times out (as behind a symmetric NAT).
        let agent = HostAgent::start(LobbyClient::new(&url), req("No punch"), &game, HostOptions { punch: false }).unwrap();
        let id = wait_registered(&agent);
        let lc = LobbyClient::new(&url);
        let t0 = Instant::now();
        let a = connect(&lc, &id, &ConnectOptions { punch_timeout: Duration::from_millis(800), ..Default::default() }).unwrap();
        let fallback_time = t0.elapsed();
        // Second client, forced relay: two relayed clients must stay distinct for the game.
        let b = connect(&lc, &id, &ConnectOptions { force_relay: true, ..Default::default() }).unwrap();
        let ra = roundtrip(&a.socket, a.server_addr, "from-a");
        let rb = roundtrip(&b.socket, b.server_addr, "from-b");
        let legs = agent.status().relay_legs;
        agent.stop();
        game.send_to(b"quit", game.local_addr().unwrap()).unwrap();
        echo.join().unwrap();
        (a.route, b.route, ra, rb, legs, fallback_time)
    })
    .await;
    let (ra, rb, ea, eb, legs, t) = results;
    assert_eq!((ra, rb), (Route::Relayed, Route::Relayed));
    assert!(t >= Duration::from_millis(800), "tried punching first ({t:?})");
    assert!(ea.starts_with("echo:from-a"), "{ea}");
    assert!(eb.starts_with("echo:from-b"), "{eb}");
    // The game saw two different (loopback leg) addresses.
    let addr = |s: &str| s.rsplit(' ').next().unwrap().to_string();
    assert_ne!(addr(&ea), addr(&eb));
    assert_eq!(legs, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn relay_session_limits() {
    let srv = server(|c| c.max_sessions = 1).await;
    let url = srv.http_url();
    blocking(move || {
        let (game, _echo) = echo_game_socket();
        let agent = HostAgent::start(LobbyClient::new(&url), req("Full"), &game, HostOptions { punch: false }).unwrap();
        let id = wait_registered(&agent);
        let lc = LobbyClient::new(&url);
        let opts = ConnectOptions { force_relay: true, ..Default::default() };
        connect(&lc, &id, &opts).unwrap();
        let err = connect(&lc, &id, &ConnectOptions { join_timeout: Duration::from_secs(1), ..opts }).unwrap_err();
        assert!(err.to_string().contains("busy"), "{err}");
        agent.stop();
    })
    .await;
}
