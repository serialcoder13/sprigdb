//! Sprig 2PC's relay: the writer node's agent relays every gateway connection of a
//! `sprig_2pc` tenant to the local MariaDB, and runs
//! [`sprig_txn::fence::RelayFence`] over them.
//!
//! The wire, per connection:
//!
//! 1. The gateway sends one line, `SPRIGRELAY 1 <tenant id>\n` (at most
//!    64 bytes), then speaks MySQL as to the server.
//! 2. The relay asks the fence: a fenced tenant (or a relay still in its
//!    start-up recovery) gets a MySQL `ERR` packet 39448
//!    `ER_SPRIG_HELD_BRANCH_FENCED` in place of the server's greeting.
//! 3. Otherwise it connects to the local server, reads the greeting (never
//!    encrypted) for the connection id, registers it, and from then on
//!    copies bytes both ways unchanged, TLS included.
//! 4. The client side ends: a holder's backend side stays open (the server
//!    keeps the branch and its locks) until the fence ends it; any other
//!    connection closes. The backend side ends: the fence hears of it.
//!
//! [`RelayRegistry`] is the fence's [`RelayLinks`]: it ends a connection
//! with `KILL CONNECTION` as `sprig_ctl` and waits until
//! `information_schema.PROCESSLIST` no longer lists it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use mysql_async::prelude::Queryable;
use sprig_common::CancellationToken;
use sprig_common::ids::TenantId;
use sprig_txn::fence::{Admission, ClientGone, END_WAIT, FENCED_ERRNO, RelayFence, RelayLinks};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// The preamble's first word.
pub const PREAMBLE: &str = "SPRIGRELAY";
/// The preamble's longest line.
pub const PREAMBLE_MAX: usize = 64;
/// How long a client may take to send its preamble, and the relay to
/// reach the server.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// The preamble a gateway sends for `tenant`.
#[must_use]
pub fn preamble(tenant: TenantId) -> Vec<u8> {
    format!("{PREAMBLE} 1 {}\n", tenant.0).into_bytes()
}

/// Parses a preamble line (without its newline).
#[must_use]
pub fn parse_preamble(line: &[u8]) -> Option<TenantId> {
    let s = std::str::from_utf8(line).ok()?;
    let mut w = s.split(' ');
    if w.next()? != PREAMBLE || w.next()? != "1" {
        return None;
    }
    let t: u64 = w.next()?.parse().ok()?;
    if w.next().is_some() {
        return None;
    }
    Some(TenantId(t))
}

/// A MySQL `ERR` packet with sequence id 0 (sent in place of a greeting).
#[must_use]
pub fn err_packet(errno: u16, sqlstate: &[u8; 5], msg: &str) -> Vec<u8> {
    let mut body = vec![0xff];
    body.extend_from_slice(&errno.to_le_bytes());
    body.push(b'#');
    body.extend_from_slice(sqlstate);
    body.extend_from_slice(msg.as_bytes());
    let n = u32::try_from(body.len()).unwrap_or(0).to_le_bytes();
    let mut p = vec![n[0], n[1], n[2], 0];
    p.extend_from_slice(&body);
    p
}

/// The connection id in a server greeting's payload (protocol 10:
/// version byte, NUL-terminated server version, 4-byte id).
#[must_use]
pub fn greeting_thread(payload: &[u8]) -> Option<u64> {
    if payload.first() != Some(&10) {
        return None;
    }
    let nul = payload[1..].iter().position(|&b| b == 0)? + 1;
    let id = payload.get(nul + 1..nul + 5)?;
    Some(u64::from(u32::from_le_bytes(id.try_into().ok()?)))
}

/// How a relayed connection's copying ended.
enum End {
    Stopped,
    Client,
    Backend,
}

struct Link {
    stop: CancellationToken,
    backend_open: Arc<AtomicBool>,
}

/// The relay's connections, as the fence sees them.
pub struct RelayRegistry {
    links: Mutex<HashMap<u64, Link>>,
    ctl: mysql_async::Pool,
}

impl std::fmt::Debug for RelayRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelayRegistry").finish_non_exhaustive()
    }
}

impl RelayRegistry {
    /// A registry ending connections through `ctl` (`sprig_ctl` on the
    /// local server).
    #[must_use]
    pub fn new(ctl: mysql_async::Pool) -> Self {
        Self {
            links: Mutex::new(HashMap::new()),
            ctl,
        }
    }

    fn links(&self) -> std::sync::MutexGuard<'_, HashMap<u64, Link>> {
        self.links.lock().unwrap_or_else(PoisonError::into_inner)
    }

    async fn listed(&self, thread: u64) -> Option<bool> {
        let mut c = self.ctl.get_conn().await.ok()?;
        let n: Option<u64> = c
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE ID = ?",
                (thread,),
            )
            .await
            .ok()?;
        Some(n.unwrap_or(0) > 0)
    }
}

#[async_trait::async_trait]
impl RelayLinks for RelayRegistry {
    async fn end(&self, thread: u64) -> bool {
        if let Ok(mut c) = self.ctl.get_conn().await
            && let Err(e) = c.query_drop(format!("KILL CONNECTION {thread}")).await
        {
            tracing::debug!(thread, error = %e, "relay: KILL CONNECTION");
        }
        if let Some(l) = self.links().remove(&thread) {
            l.stop.cancel();
        }
        let until = tokio::time::Instant::now() + END_WAIT;
        loop {
            if self.listed(thread).await == Some(false) {
                return true;
            }
            if tokio::time::Instant::now() >= until {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn backend_alive(&self, thread: u64) -> bool {
        self.links()
            .get(&thread)
            .is_some_and(|l| l.backend_open.load(Ordering::SeqCst))
    }
}

/// One node's relay.
pub struct Relay {
    fence: Arc<RelayFence<RelayRegistry>>,
    registry: Arc<RelayRegistry>,
    backend: String,
}

impl std::fmt::Debug for Relay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Relay")
            .field("backend", &self.backend)
            .finish_non_exhaustive()
    }
}

impl Relay {
    /// A relay to the server at `backend` (`host:port`, the node's own
    /// address, so the tenant accounts' host rule admits it), fenced by
    /// `fence` over `registry`.
    #[must_use]
    pub fn new(
        fence: Arc<RelayFence<RelayRegistry>>,
        registry: Arc<RelayRegistry>,
        backend: String,
    ) -> Self {
        Self {
            fence,
            registry,
            backend,
        }
    }

    /// The fence (the control calls: prepare, finished, abandon).
    #[must_use]
    pub fn fence(&self) -> &Arc<RelayFence<RelayRegistry>> {
        &self.fence
    }

    /// Recovers the node, then relays connections from `listener` and
    /// runs the fence until `stop`.
    pub async fn serve(self: Arc<Self>, listener: TcpListener, stop: CancellationToken) {
        loop {
            match self.fence.start().await {
                Ok(_) => break,
                Err(e) => {
                    tracing::warn!(error = %e, "relay: start-up recovery failed; retrying");
                    tokio::select! {
                        () = stop.cancelled() => return,
                        () = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                }
            }
        }
        let fence = Arc::clone(&self.fence);
        let fstop = stop.clone();
        let fence_task = tokio::spawn(async move { fence.run(fstop).await });
        loop {
            tokio::select! {
                () = stop.cancelled() => break,
                r = listener.accept() => match r {
                    Ok((sock, _)) => {
                        let me = Arc::clone(&self);
                        let stop = stop.clone();
                        tokio::spawn(async move { me.relay_one(sock, stop).await });
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "relay: accept failed");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                },
            }
        }
        let _ = fence_task.await;
    }

    async fn read_preamble(sock: &mut TcpStream) -> Option<TenantId> {
        let mut line = Vec::with_capacity(PREAMBLE_MAX);
        let mut b = [0u8; 1];
        loop {
            if sock.read_exact(&mut b).await.is_err() {
                return None;
            }
            if b[0] == b'\n' {
                return parse_preamble(&line);
            }
            line.push(b[0]);
            if line.len() > PREAMBLE_MAX {
                return None;
            }
        }
    }

    async fn refuse(sock: &mut TcpStream, msg: &str) {
        let _ = sock
            .write_all(&err_packet(FENCED_ERRNO, b"08S01", msg))
            .await;
        let _ = sock.shutdown().await;
    }

    async fn relay_one(self: Arc<Self>, mut client: TcpStream, stop: CancellationToken) {
        let _ = client.set_nodelay(true);
        let Ok(Some(tenant)) =
            tokio::time::timeout(HANDSHAKE_TIMEOUT, Self::read_preamble(&mut client)).await
        else {
            return;
        };
        if self.fence.admit(tenant) == Admission::Fenced {
            Self::refuse(
                &mut client,
                "held branches are being recovered on this node; retry",
            )
            .await;
            return;
        }
        let Ok(Ok(mut backend)) =
            tokio::time::timeout(HANDSHAKE_TIMEOUT, TcpStream::connect(&self.backend)).await
        else {
            let _ = client.shutdown().await;
            return;
        };
        let _ = backend.set_nodelay(true);
        let mut head = [0u8; 4];
        if backend.read_exact(&mut head).await.is_err() {
            return;
        }
        let len = usize::from(head[0]) | usize::from(head[1]) << 8 | usize::from(head[2]) << 16;
        let mut payload = vec![0u8; len];
        if backend.read_exact(&mut payload).await.is_err() {
            return;
        }
        let Some(thread) = greeting_thread(&payload) else {
            // The server refused us (an ERR greeting): pass it on.
            let _ = client.write_all(&head).await;
            let _ = client.write_all(&payload).await;
            return;
        };
        if !self.fence.opened(tenant, thread) {
            Self::refuse(&mut client, "the tenant was fenced; retry").await;
            return;
        }
        let link_stop = stop.child_token();
        let backend_open = Arc::new(AtomicBool::new(true));
        self.registry.links().insert(
            thread,
            Link {
                stop: link_stop.clone(),
                backend_open: Arc::clone(&backend_open),
            },
        );
        if client.write_all(&head).await.is_err() || client.write_all(&payload).await.is_err() {
            self.client_gone(tenant, thread, backend, &link_stop).await;
            return;
        }
        let (mut cr, mut cw) = client.into_split();
        let (mut br, mut bw) = backend.into_split();
        let ended = {
            let up = async { tokio::io::copy(&mut cr, &mut bw).await };
            let down = async { tokio::io::copy(&mut br, &mut cw).await };
            tokio::pin!(up, down);
            tokio::select! {
                () = link_stop.cancelled() => End::Stopped,
                _ = &mut up => End::Client,
                _ = &mut down => End::Backend,
            }
        };
        match ended {
            End::Client => {
                drop(cr);
                drop(cw);
                if let Ok(b) = br.reunite(bw) {
                    self.client_gone(tenant, thread, b, &link_stop).await;
                }
                return;
            }
            End::Backend => {
                backend_open.store(false, Ordering::SeqCst);
                self.fence.backend_gone(tenant, thread);
            }
            End::Stopped => {}
        }
        self.registry.links().remove(&thread);
    }

    /// The client went away: close, or keep a holder's backend open until
    /// the fence ends it.
    async fn client_gone(
        &self,
        tenant: TenantId,
        thread: u64,
        mut backend: TcpStream,
        stop: &CancellationToken,
    ) {
        match self.fence.client_gone(tenant, thread) {
            ClientGone::Close => {
                self.registry.links().remove(&thread);
                let _ = backend.shutdown().await;
            }
            ClientGone::Keep => {
                // Read and drop whatever the server still sends; the
                // connection ends when the fence ends it (or the server).
                let mut sink = [0u8; 4096];
                loop {
                    tokio::select! {
                        () = stop.cancelled() => break,
                        r = backend.read(&mut sink) => match r {
                            Ok(0) | Err(_) => {
                                if let Some(l) = self.registry.links().get(&thread) {
                                    l.backend_open.store(false, Ordering::SeqCst);
                                }
                                self.fence.backend_gone(tenant, thread);
                                break;
                            }
                            Ok(_) => {}
                        },
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preambles_round_trip_and_reject_junk() {
        let p = preamble(TenantId(26_306_000_001));
        assert_eq!(p.last(), Some(&b'\n'));
        assert_eq!(
            parse_preamble(&p[..p.len() - 1]),
            Some(TenantId(26_306_000_001))
        );
        assert_eq!(parse_preamble(b"SPRIGRELAY 2 7"), None);
        assert_eq!(parse_preamble(b"SPRIGRELAY 1 x"), None);
        assert_eq!(parse_preamble(b"SPRIGRELAY 1 7 8"), None);
        assert_eq!(parse_preamble(b"GET / HTTP/1.1"), None);
    }

    #[test]
    fn greeting_carries_the_connection_id() {
        let mut g = vec![10];
        g.extend_from_slice(b"11.8.3-MariaDB\0");
        g.extend_from_slice(&4242u32.to_le_bytes());
        g.extend_from_slice(b"salt....\0");
        assert_eq!(greeting_thread(&g), Some(4242));
        assert_eq!(greeting_thread(&[0xff, 1, 2]), None);
    }

    #[test]
    fn the_refusal_is_a_mysql_err_packet() {
        let p = err_packet(FENCED_ERRNO, b"08S01", "fenced");
        assert_eq!(usize::from(p[0]), p.len() - 4);
        assert_eq!(p[3], 0);
        assert_eq!(p[4], 0xff);
        assert_eq!(u16::from_le_bytes([p[5], p[6]]), 39448);
        assert_eq!(&p[7..13], b"#08S01");
    }
}
