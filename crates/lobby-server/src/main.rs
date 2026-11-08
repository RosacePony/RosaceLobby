//! `lobby-server` binary. Settings come from flags or environment variables (`--help`).
//! `lobby-server healthcheck [url]` probes `/health` and exits 0/1 (Docker HEALTHCHECK without curl).

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use clap::Parser;
use lobby_server::{Config, start};
use tracing_subscriber::EnvFilter;

fn healthcheck(addr: &str) -> bool {
    let Ok(mut s) = TcpStream::connect_timeout(&match addr.parse() {
        Ok(a) => a,
        Err(_) => return false,
    }, Duration::from_secs(3)) else {
        return false;
    };
    let _ = s.set_read_timeout(Some(Duration::from_secs(3)));
    if s.write_all(format!("GET /health HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes()).is_err() {
        return false;
    }
    let mut reply = String::new();
    let _ = s.read_to_string(&mut reply);
    reply.starts_with("HTTP/1.1 200") && reply.contains("\"ok\"")
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("healthcheck") {
        let port = std::env::var("LOBBY_HTTP_ADDR").ok().and_then(|a| a.rsplit(':').next().map(str::to_string)).unwrap_or_else(|| "8080".into());
        let addr = args.get(2).cloned().unwrap_or_else(|| format!("127.0.0.1:{port}"));
        std::process::exit(if healthcheck(&addr) { 0 } else { 1 });
    }
    tracing_subscriber::fmt().with_env_filter(EnvFilter::try_from_env("LOBBY_LOG").unwrap_or_else(|_| EnvFilter::new("info"))).init();
    let config = Config::parse();
    tracing::info!(?config, "starting");
    let running = match start(config).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("could not start: {e}");
            std::process::exit(1);
        }
    };
    println!("lobby-server listening: http {} rendezvous udp {} relay udp {}", running.http_addr, running.rendezvous_addr, running.relay_addr);
    shutdown_signal().await;
    tracing::info!("shutting down");
    running.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = term => {}
    }
}
