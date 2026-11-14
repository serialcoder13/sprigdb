//! systemd's readiness and watchdog protocol (the VM units are
//! `Type=notify` with `WatchdogSec`).
//!
//! Under systemd, `NOTIFY_SOCKET` names a datagram socket. The process
//! sends `READY=1` once the role reports ready, and `WATCHDOG=1` every half
//! `WATCHDOG_USEC` while the process watchdog ticks: a stalled runtime
//! stops the pings and systemd restarts the unit (for the agent, after its
//! own watchdog has fenced the node). Outside systemd nothing is sent.

use std::time::Duration;

use sprig_common::telemetry::{Health, ProcessState};

/// Sends `msg` to `NOTIFY_SOCKET`; `false` without one or on error.
fn send(socket: &str, msg: &str) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::net::UnixDatagram;
        let Ok(sock) = UnixDatagram::unbound() else {
            return false;
        };
        if let Some(name) = socket.strip_prefix('@') {
            #[cfg(target_os = "linux")]
            {
                use std::os::linux::net::SocketAddrExt;
                return std::os::unix::net::SocketAddr::from_abstract_name(name.as_bytes())
                    .and_then(|a| sock.send_to_addr(msg.as_bytes(), &a))
                    .is_ok();
            }
            #[cfg(not(target_os = "linux"))]
            {
                let _ = name;
                return false;
            }
        }
        sock.send_to(msg.as_bytes(), socket).is_ok()
    }
    #[cfg(not(unix))]
    {
        let _ = (socket, msg);
        false
    }
}

/// The ping interval: half of `WATCHDOG_USEC`, if set.
fn watchdog_interval(usec: Option<&str>) -> Option<Duration> {
    let us: u64 = usec?.trim().parse().ok()?;
    (us > 0).then(|| Duration::from_micros(us / 2))
}

/// Starts the notifier when the process runs under systemd.
pub(crate) fn spawn(health: Health) {
    let Ok(socket) = std::env::var("NOTIFY_SOCKET") else {
        return;
    };
    let every = watchdog_interval(std::env::var("WATCHDOG_USEC").ok().as_deref());
    tokio::spawn(async move {
        let (mut ready, mut stopping) = (false, false);
        let mut tick = tokio::time::interval(every.map_or(Duration::from_millis(200), |w| {
            w.min(Duration::from_millis(200))
        }));
        let mut last_ping = tokio::time::Instant::now();
        loop {
            tick.tick().await;
            match health.state() {
                ProcessState::Ready if !ready => {
                    ready = send(&socket, "READY=1");
                }
                ProcessState::Draining if !stopping => {
                    // Pings go on while the role drains.
                    stopping = send(&socket, "STOPPING=1");
                }
                _ => {}
            }
            if let Some(w) = every
                && last_ping.elapsed() >= w
                && health.is_live()
            {
                let _ = send(&socket, "WATCHDOG=1");
                last_ping = tokio::time::Instant::now();
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn half_the_watchdog() {
        assert_eq!(
            watchdog_interval(Some("3000000")),
            Some(Duration::from_millis(1500))
        );
        assert_eq!(watchdog_interval(Some("0")), None);
        assert_eq!(watchdog_interval(None), None);
    }

    #[cfg(unix)]
    #[test]
    fn sends_datagrams() {
        use std::os::unix::net::UnixDatagram;
        let dir = std::env::temp_dir().join(format!("sprig-sdn-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("notify");
        let _ = std::fs::remove_file(&path);
        let rx = UnixDatagram::bind(&path).expect("bind");
        assert!(send(path.to_str().expect("utf8"), "READY=1"));
        let mut buf = [0u8; 32];
        let n = rx.recv(&mut buf).expect("recv");
        assert_eq!(&buf[..n], b"READY=1");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
