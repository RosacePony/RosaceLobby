# RosaceLobby

A small, reusable lobby service for UDP games: a public game list, NAT traversal (UDP hole
punching) and a relay fallback, in one Rust binary. Used by *Claws & Magic*; any game can use it
with its own `game` id.

```
crates/
  lobby-proto    shared wire types (serde): HTTP JSON bodies + UDP packets, versioned
  lobby-server   axum + tokio server: HTTP API, UDP rendezvous, UDP relay
  lobby-client   blocking client library (ureq + std::net::UdpSocket, no async runtime):
                 list/register/heartbeat, HostAgent (host side), connect() (punch, else relay)
```

## Ports

| port | proto | what |
|---|---|---|
| 8080 | TCP | HTTP JSON API (can sit behind a reverse proxy / HTTPS) |
| 7777 | UDP | rendezvous: endpoint exchange for hole punching |
| 7778 | UDP | relay: forwards datagrams when punching fails |

## How a join works

```
 host game (renet server)            lobby server                     joining player
 game socket G :5000   control C     HTTP 8080 / UDP 7777 / 7778       socket K
 -----------------------------------------------------------------------------------------
 HostAgent: POST /v1/lobbies  ---->  id + token
            heartbeat every 5 s ---> (expires after 20 s of silence)
 C: HostControl{id,token} ------->   remembers C's public endpoint, ack
 G: HostBeat{id,token} (2 s) ---->   remembers G's PUBLIC endpoint (never answered)
                                     GET /v1/lobbies?game=..&version=..  <---- browse
                                     Join{lobby} (from K) <------------------ K
 C <-- PeerJoin{session, K's public endpoint + LAN addr}
                                     JoinInfo{session,key, G's public + LAN addrs} ----> K
 G: Punch -> K (every 100 ms, 5 s)                      K: Punch -> G (every 100 ms, 3 s)
   -- either side's packets open its own NAT; when K receives a Punch from G: --------
                                     PunchOk (from K) <------------------- K
 C <-- PunchDone (host stops punching)
   K is handed to the game's client transport with server address = G's public endpoint.

 Punching failed for 3 s (symmetric NAT, strict firewall) or the client forces the relay:
                                     RelayBind{session,key,Client} on 7778 <-- K
 C <-- RelayRequest{session,key} (sent from the rendezvous port: C's NAT mapping points there)
 leg L (new socket on the host): RelayBind{..,Host} -> 7778;  both bound -> RelayBound{ready}
   K -> relay -> L -> 127.0.0.1:5000 (G)   and back: G -> L -> relay -> K
   K is handed to the game's client transport with server address = the relay.
```

### Why the host needs its game socket AND a control socket

The game server (e.g. renet's `NetcodeServerTransport`) owns its UDP socket and reads every
datagram that arrives on it. The punch must come from that same socket, because the NAT mapping
the client punches towards is the one of that socket. So:

- `HostAgent` gets a `try_clone` of the game socket and only **sends** through it (`HostBeat`
  to the rendezvous, `Punch` to clients). The lobby service never sends anything to the game
  socket, so the game never has to parse foreign packets, apart from the client's short-lived
  `Punch` datagrams, which a netcode transport drops as invalid packets (the host also stops
  punching as soon as the client reports success).
- Everything the host must **receive** from the service (join notices, relay requests) goes to a
  separate control socket owned by the agent.
- A relayed client arrives through a **relay leg**: one local socket per relayed client that
  forwards relay <-> `127.0.0.1:<game port>`. The game server therefore sees every relayed
  client as a distinct address (`127.0.0.1:<leg port>`), even though they all come from the
  same relay. The relay itself is transparent: it forwards raw datagrams unchanged once both
  endpoints have bound with the session key, so the game's own packets (netcode encryption,
  sequence numbers) pass through as-is.

On the joining side nothing is proxied: `connect()` returns the very socket that punched (or
bound to the relay) plus the address to talk to; the game client uses both directly.

On Windows the library turns off `SIO_UDP_CONNRESET` on these sockets: otherwise an ICMP "port
unreachable" caused by a punch would surface as an error on the next `recv_from` of the game
transport.

## HTTP API (`/v1`)

| method | path | auth | body / reply |
|---|---|---|---|
| GET | `/health` | - | `{status, lobbies, sessions, relayed_sessions}` |
| GET | `/v1/info` | - | `{server_version, protocol_version, rendezvous_port, relay_port, heartbeat_secs, expire_secs}` |
| GET | `/v1/lobbies?game=ID&version=V` | - | `{lobbies: [LobbyInfo], rendezvous_port, relay_port}` |
| POST | `/v1/lobbies` | - | `RegisterRequest` -> `{id, token, heartbeat_secs, expire_secs, rendezvous_port, relay_port}` |
| POST | `/v1/lobbies/{id}/heartbeat` | `Authorization: Bearer <token>` | `{players, map?, mode?}` -> 204 (404 when expired: register again) |
| DELETE | `/v1/lobbies/{id}` | `Authorization: Bearer <token>` | 204 |

`LobbyInfo`: `id, game, version, name, mode, map, players, max_players, has_password, region,
age_secs`. Validation: game id `[a-z0-9_-]{1,32}`, name <= 48 chars, mode/map <= 32, version
<= 32, no control characters, `1 <= max_players <= 1024`, at most 8 LAN addresses. Errors are
`{ "error": "..." }` with 400/401/403/404/429/503.

UDP packets are `RLBY` + protocol version byte + JSON (see `lobby-proto`), so they can never be
mistaken for game data and old clients are ignored cleanly after a protocol bump.

## Limits (all configurable)

Flags or environment variables (`lobby-server --help`):

| env | default | |
|---|---|---|
| `LOBBY_HTTP_ADDR` | `0.0.0.0:8080` | HTTP listen |
| `LOBBY_RENDEZVOUS_ADDR` | `0.0.0.0:7777` | rendezvous listen |
| `LOBBY_RELAY_ADDR` | `0.0.0.0:7778` | relay listen |
| `LOBBY_PUBLIC_RENDEZVOUS_PORT` / `LOBBY_PUBLIC_RELAY_PORT` | bound port | ports announced to clients if a mapping changes them |
| `LOBBY_HEARTBEAT_SECS` / `LOBBY_EXPIRE_SECS` | 5 / 20 | heartbeat period, expiry |
| `LOBBY_MAX_LOBBIES` / `LOBBY_MAX_LOBBIES_PER_IP` | 2000 / 5 | |
| `LOBBY_RATE_PER_MIN` | 120 | HTTP requests per minute per IP (`/health` exempt) |
| `LOBBY_REGISTER_PER_MIN` | 10 | registrations per minute per IP |
| `LOBBY_UDP_PER_SEC` | 50 | UDP control packets per second per IP |
| `RELAY_MAX_SESSIONS` / `RELAY_MAX_SESSIONS_PER_IP` | 512 / 8 | join sessions (punched or relayed) |
| `RELAY_BYTES_PER_SEC` | 1000000 | relay bandwidth cap per session (token bucket) |
| `RELAY_IDLE_SECS` | 30 | sessions without traffic are closed |
| `LOBBY_TRUST_PROXY` | false | take the client IP from `X-Forwarded-For` (reverse proxy only) |
| `LOBBY_LOG` | `info` | tracing filter, e.g. `info,lobby_server=debug` |

## Deploy with Portainer

The image is built by GitHub Actions (`.github/workflows/docker.yml`) and pushed to
`ghcr.io/rosacepony/rosace-lobby:latest` on every push to `main` (plus `X.Y.Z` on `vX.Y.Z` tags).
After the first build, make the package public (GitHub > your profile > Packages >
rosace-lobby > Package settings > Change visibility), or add a ghcr.io registry with a token
in Portainer.

1. Portainer > **Stacks** > **Add stack** > name `rosace-lobby` > **Web editor**: paste
   [`docker-compose.portainer.yml`](docker-compose.portainer.yml) as-is (defaults are sensible;
   every setting is commented). **Deploy the stack**.
2. Open the ports on the VPS (AlmaLinux, firewalld):
   ```sh
   sudo firewall-cmd --permanent --add-port=8080/tcp
   sudo firewall-cmd --permanent --add-port=7777/udp
   sudo firewall-cmd --permanent --add-port=7778/udp
   sudo firewall-cmd --reload
   sudo firewall-cmd --list-ports
   ```
   If the **OVH network firewall** is enabled for the IP (OVH Control Panel > Bare Metal Cloud /
   VPS > IP > `...` > Network firewall), add "accept" rules for TCP 8080 and UDP 7777-7778
   before its final "deny" rule (or keep it disabled).
3. Check from your PC: `curl http://<server-ip>:8080/health` ->
   `{"status":"ok","lobbies":0,...}`. The container's own health check uses the same endpoint
   (`lobby-server healthcheck`; the image has no shell or curl).
4. Point the game at it (Claws & Magic: `lobby_url` in `%APPDATA%/ClawsAndMagic/settings.json`,
   or `--lobby-url http://<server-ip>:8080`; the built-in default is `http://162.19.66.231:8080`).
5. Updates: push to `main`, wait for the workflow, then Portainer > Stacks > rosace-lobby >
   **Pull and redeploy** (or enable the stack's GitOps/webhook redeploy).

**UDP source addresses must be preserved**: hole punching tells each side the *real* public
IP:port the server saw. Docker's normal published ports (iptables DNAT) keep it. If the
`userland-proxy` ends up handling UDP (rootless Docker, `"iptables": false` in daemon.json,
some Docker Desktop setups), every datagram appears to come from the Docker gateway: listing
works but punching and relaying break. Fix: use the commented `network_mode: host` variant at
the bottom of the Portainer compose file (no `ports:` section; firewall rules unchanged).
Logs: Portainer > Containers > rosace-lobby > Logs (`lobby registered`, `join`,
`punch succeeded`, `relay bind`, `lobby expired`).

## Other deployments

- **Docker Compose with a local build**: `docker compose up -d --build` (`docker-compose.yml`).
- **Plain Docker**: `docker build -t rosace-lobby . && docker run -d --restart unless-stopped -p 8080:8080 -p 7777:7777/udp -p 7778:7778/udp rosace-lobby`.
- **systemd**: `cargo build --release -p lobby-server`, then follow the header of
  [`deploy/rosace-lobby.service`](deploy/rosace-lobby.service).
- **Behind a reverse proxy (HTTPS)**: proxy only the HTTP API (8080) and set
  `LOBBY_TRUST_PROXY=true`; UDP 7777/7778 must still reach the server directly, on the same
  host name/IP as the API (clients derive the UDP address from the lobby URL's host).

## Using the client library

```rust
use lobby_client::{LobbyClient, HostAgent, HostOptions, RegisterRequest, connect, ConnectOptions};

// Host: create the game's UDP socket yourself, start the agent, then give the socket to the game.
let socket = std::net::UdpSocket::bind("0.0.0.0:5000")?;
lobby_client::disable_udp_connreset(&socket);
let agent = HostAgent::start(LobbyClient::new("http://lobby.example:8080"), request, &socket, HostOptions::default())?;
// ... game server reads `socket`; call agent.set_players(n) now and then; drop(agent) unlists.

// Client (blocking for up to ~10 s: run it on a thread).
let lobbies = LobbyClient::new(url).list("my_game", Some("1.2.3"))?;
let conn = connect(&LobbyClient::new(url), &lobbies[0].id, &ConnectOptions::default())?;
// conn.socket + conn.server_addr go straight to the game's client transport; conn.route says
// Punched or Relayed.
```

## Development

```sh
cargo test --workspace                  # unit + in-process end-to-end tests (loopback)
cargo clippy --workspace --all-targets
cargo run -p lobby-server -- --http-addr 127.0.0.1:8080   # local server for game tests
```

The end-to-end tests run the real server on loopback ports and the real client library:
register/list/heartbeat/expire/delete, validation and rate limits, raw rendezvous endpoint
exchange, relay forwarding between two UDP sockets (and stranger injection), a host agent +
`connect` punching to an echo "game socket", punch failure (host never punches) falling back to
the relay, forced relay with two relayed clients seen as distinct addresses, and the session cap.

Not done yet: IPv6, password-protected lobbies (the flag is listed but not enforced: the game
would check it), TURN-style per-session relay ports, metrics endpoint.
