//! Rate limiting (fixed windows per IP) and the relay bandwidth cap.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// At most `limit` events per `window` and IP.
pub struct RateLimiter {
    limit: u32,
    window: Duration,
    hits: Mutex<HashMap<IpAddr, (Instant, u32)>>,
}

impl RateLimiter {
    pub fn new(limit: u32, window: Duration) -> Self {
        Self { limit, window, hits: Mutex::new(HashMap::new()) }
    }

    /// Counts one event; false when `ip` is over its limit for the current window.
    pub fn allow(&self, ip: IpAddr, now: Instant) -> bool {
        let mut hits = self.hits.lock().unwrap_or_else(|e| e.into_inner());
        let e = hits.entry(ip).or_insert((now, 0));
        if now.duration_since(e.0) >= self.window {
            *e = (now, 0);
        }
        e.1 += 1;
        e.1 <= self.limit
    }

    /// Forgets windows that are over.
    pub fn sweep(&self, now: Instant) {
        let mut hits = self.hits.lock().unwrap_or_else(|e| e.into_inner());
        hits.retain(|_, (start, _)| now.duration_since(*start) < self.window);
    }

    pub fn tracked(&self) -> usize {
        self.hits.lock().map(|h| h.len()).unwrap_or(0)
    }
}

/// Token bucket: `rate` bytes per second, bursts up to one second's worth.
#[derive(Debug, Clone)]
pub struct Bandwidth {
    rate: f64,
    tokens: f64,
    last: Instant,
}

impl Bandwidth {
    pub fn new(bytes_per_sec: u64, now: Instant) -> Self {
        Self { rate: bytes_per_sec as f64, tokens: bytes_per_sec as f64, last: now }
    }

    /// Takes `bytes` if available.
    pub fn take(&mut self, bytes: usize, now: Instant) -> bool {
        let dt = now.duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + dt * self.rate).min(self.rate);
        if self.tokens >= bytes as f64 {
            self.tokens -= bytes as f64;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limiter_windows() {
        let r = RateLimiter::new(3, Duration::from_secs(60));
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        let other: IpAddr = "10.0.0.2".parse().unwrap();
        let t0 = Instant::now();
        assert!((0..3).all(|_| r.allow(ip, t0)));
        assert!(!r.allow(ip, t0), "4th request in the window is refused");
        assert!(r.allow(other, t0), "limits are per IP");
        assert!(r.allow(ip, t0 + Duration::from_secs(61)), "new window");
        r.sweep(t0 + Duration::from_secs(200));
        assert_eq!(r.tracked(), 0);
    }

    #[test]
    fn bandwidth_caps_and_refills() {
        let t0 = Instant::now();
        let mut b = Bandwidth::new(1000, t0);
        assert!(b.take(600, t0));
        assert!(!b.take(600, t0), "over the burst");
        assert!(b.take(400, t0));
        assert!(b.take(500, t0 + Duration::from_millis(500)), "refilled half a second");
        assert!(!b.take(2000, t0 + Duration::from_secs(10)), "never more than one second of burst");
    }
}
