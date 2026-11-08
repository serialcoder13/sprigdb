//! Client-disconnect detection (chapter 04, "Accept and protocol
//! detection": "On client disconnect mid-statement the task sends `KILL
//! QUERY` and discards the backend connection").
//!
//! Between statements a front end is reading from the client, so a hang-up
//! shows as end of file. During a statement nothing reads the socket; these
//! functions watch it for the peer's FIN or reset without consuming bytes,
//! so a pipelined next command stays in the socket for the front end.

use std::future::Future;
use std::time::Duration;

use tokio::io::Interest;
use tokio::net::TcpStream;

use super::SessionHandle;
use crate::frontend::MaybeTls;

/// How often a socket holding unread bytes is re-checked: its readiness
/// stays set until the bytes are read, so it can't simply be awaited.
const PENDING_RECHECK: Duration = Duration::from_millis(100);

/// The TCP socket under a possibly-TLS client stream.
#[must_use]
pub fn tcp_of(stream: &MaybeTls<TcpStream>) -> &TcpStream {
    match stream {
        MaybeTls::Plain(s) => s,
        MaybeTls::Tls(s) => s.get_ref().0,
    }
}

/// Resolves when the peer has closed or reset `sock`. Reads nothing.
///
/// The read side's "closed" readiness (`EPOLLRDHUP` on Linux, `EV_EOF` on
/// the BSDs and macOS) reports a FIN even when unread bytes precede it. A
/// client that half-closes its write side and still waits for the answer
/// looks the same as one that left; MySQL clients don't do that.
pub async fn peer_closed(sock: &TcpStream) {
    let mut probe = [0u8; 1];
    loop {
        match sock.ready(Interest::READABLE).await {
            Ok(r) if r.is_read_closed() => return,
            Ok(_) => {}
            Err(_) => return,
        }
        // Readable: end of file, an error, or bytes the client sent ahead.
        // `peek` returns at once here, or waits again if readiness was
        // spurious.
        match sock.peek(&mut probe).await {
            Ok(0) | Err(_) => return,
            Ok(_) => tokio::time::sleep(PENDING_RECHECK).await,
        }
    }
}

/// [`peer_closed`] for a front end's client stream. With TLS, the FIN after
/// the client's `close_notify` is what counts.
pub async fn client_gone(stream: &MaybeTls<TcpStream>) {
    peer_closed(tcp_of(stream)).await;
}

/// How a watched statement ended.
#[derive(Debug)]
pub enum StatementEnd<T> {
    /// It finished; here is its result.
    Done(T),
    /// The client disconnected first: send `KILL QUERY` for the backend
    /// connection, discard that connection, and end the session.
    ClientGone,
    /// `KILL QUERY` or `KILL` arrived first: send `KILL QUERY` for the
    /// backend connection and answer the client (MySQL 1317), or close it
    /// after `KILL`.
    Killed,
}

/// Runs `statement` until it finishes, the client disconnects, or the
/// session is killed, whichever is first. The statement's future is dropped
/// in the other two cases.
///
/// The front end must not need the client stream mutably while this runs:
/// it awaits the backend's answer here, then relays it (a relay that fails
/// to write has found the disconnect itself).
pub async fn watch_statement<T>(
    session: &SessionHandle,
    stream: &MaybeTls<TcpStream>,
    statement: impl Future<Output = T>,
) -> StatementEnd<T> {
    tokio::select! {
        biased;
        r = statement => StatementEnd::Done(r),
        () = session.flags().query_killed() => StatementEnd::Killed,
        () = client_gone(stream) => StatementEnd::ClientGone,
    }
}
