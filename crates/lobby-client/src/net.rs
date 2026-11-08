//! Small UDP helpers.

use std::io::ErrorKind;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::time::{Duration, Instant};

use lobby_proto::Packet;

/// This machine's LAN address (no packet sent: connecting a UDP socket only picks a route).
pub fn lan_ip() -> Option<IpAddr> {
    let s = UdpSocket::bind("0.0.0.0:0").ok()?;
    s.connect("8.8.8.8:80").ok()?;
    s.local_addr().ok().map(|a| a.ip()).filter(|ip| !ip.is_unspecified())
}

/// Windows reports an ICMP "port unreachable" caused by an earlier `send_to` as an error on
/// the NEXT `recv_from` (WSAECONNRESET). Punching sends to addresses that may be closed, and
/// game transports may treat that error as fatal: switch the behaviour off. No-op elsewhere.
pub fn disable_udp_connreset(socket: &UdpSocket) {
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawSocket;
        #[link(name = "ws2_32")]
        unsafe extern "system" {
            fn WSAIoctl(s: usize, code: u32, inb: *const u32, inl: u32, outb: *mut u8, outl: u32, ret: *mut u32, ov: *mut u8, cr: *mut u8) -> i32;
        }
        const SIO_UDP_CONNRESET: u32 = 0x9800_000C;
        let off: u32 = 0;
        let mut ret: u32 = 0;
        // SAFETY: valid socket handle, 4-byte input buffer, no output, no overlapped I/O.
        unsafe {
            WSAIoctl(
                socket.as_raw_socket() as usize,
                SIO_UDP_CONNRESET,
                &off,
                4,
                std::ptr::null_mut(),
                0,
                &mut ret,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
        }
    }
    #[cfg(not(windows))]
    let _ = socket;
}

pub(crate) fn send(sock: &UdpSocket, p: &Packet, to: SocketAddr) {
    let _ = sock.send_to(&p.encode(), to);
}

/// One datagram, None on timeout (read timeout of the socket) or transient errors.
pub(crate) fn recv<'a>(sock: &UdpSocket, buf: &'a mut [u8]) -> Option<(&'a [u8], SocketAddr)> {
    match sock.recv_from(buf) {
        Ok((n, from)) => Some((&buf[..n], from)),
        Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::ConnectionReset | ErrorKind::Interrupted) => None,
        Err(_) => None,
    }
}

/// Reads and discards everything queued (stray punches) without blocking.
pub(crate) fn drain(sock: &UdpSocket) {
    let _ = sock.set_nonblocking(true);
    let mut buf = [0u8; 2048];
    let start = Instant::now();
    while start.elapsed() < Duration::from_millis(50) {
        match sock.recv_from(&mut buf) {
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::ConnectionReset => {}
            Err(_) => break,
        }
    }
    let _ = sock.set_nonblocking(false);
}
