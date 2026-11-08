//! Client-disconnect detection while a statement runs (chapter 04: "On
//! client disconnect mid-statement the task sends `KILL QUERY` and discards
//! the backend connection").
//!
//! The command loop owns the client stream inside a `Framed`, which the
//! running statement writes through, so it can't also lend the stream to
//! [`peer_closed`]. A [`ClientWatch`] holds a second descriptor for the same
//! socket (`dup`), registered with the reactor on its own; watching it
//! reads nothing, so pipelined commands stay queued for the loop.

use tokio::net::TcpStream;

use crate::frontend::MaybeTls;
use crate::session::{peer_closed, tcp_of};

/// Watches a client socket for the peer's FIN or reset.
#[derive(Debug)]
pub(crate) struct ClientWatch {
    dup: Option<TcpStream>,
}

impl ClientWatch {
    /// A watch on `stream`'s socket. If the descriptor can't be duplicated
    /// the watch never fires (a write to the client still finds the
    /// disconnect).
    pub(crate) fn new(stream: &MaybeTls<TcpStream>) -> Self {
        Self {
            dup: dup(tcp_of(stream)),
        }
    }

    /// Resolves when the client has closed or reset the connection.
    pub(crate) async fn gone(&self) {
        match &self.dup {
            Some(s) => peer_closed(s).await,
            None => std::future::pending().await,
        }
    }
}

#[cfg(unix)]
fn dup(sock: &TcpStream) -> Option<TcpStream> {
    use std::os::fd::AsFd as _;
    let fd = sock.as_fd().try_clone_to_owned().ok()?;
    let std = std::net::TcpStream::from(fd);
    std.set_nonblocking(true).ok()?;
    TcpStream::from_std(std).ok()
}

#[cfg(not(unix))]
fn dup(_sock: &TcpStream) -> Option<TcpStream> {
    None
}
