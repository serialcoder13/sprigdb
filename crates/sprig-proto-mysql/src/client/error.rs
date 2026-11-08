//! [`ClientError`]: why a backend connection or command failed.

use crate::error::ErrPacket;
use crate::proto_error::ProtoError;

/// A failure of the backend client.
///
/// A server's ERR packet in the command phase is not a failure of the
/// connection: the raw packet API returns it as a packet, and only the typed
/// helpers turn it into [`ClientError::Server`]. Every other variant except
/// [`ClientError::Busy`] leaves the connection unusable
/// ([`MysqlClient::is_broken`](super::MysqlClient::is_broken)).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClientError {
    /// The TCP connection couldn't be opened.
    #[error("connecting to {addr}: {source}")]
    Connect {
        /// `host:port`.
        addr: String,
        /// The socket error.
        #[source]
        source: std::io::Error,
    },
    /// The transport failed after connecting.
    #[error("i/o: {0}")]
    Io(#[from] std::io::Error),
    /// A packet couldn't be framed or parsed.
    #[error("protocol: {0}")]
    Proto(ProtoError),
    /// TLS setup or the TLS handshake failed.
    #[error("TLS: {0}")]
    Tls(String),
    /// Authentication can't proceed: an unsupported plugin, or a step that
    /// needs TLS.
    #[error("authentication: {0}")]
    Auth(String),
    /// The server answered with an ERR packet.
    #[error("server error {} ({}): {}", .0.code, String::from_utf8_lossy(&.0.sqlstate), .0.message)]
    Server(ErrPacket),
    /// A deadline passed.
    #[error("timed out: {0}")]
    Timeout(&'static str),
    /// The server sent a packet that doesn't fit the protocol state
    /// (chapter 05: 39012 if the client awaits a response).
    #[error("unexpected packet: {0}")]
    Unexpected(String),
    /// The server closed the connection.
    #[error("the server closed the connection")]
    Closed,
    /// The client was used out of order, such as a command while a
    /// response is still being read. The connection is unharmed.
    #[error("client misuse: {0}")]
    Busy(&'static str),
}

impl From<ProtoError> for ClientError {
    fn from(e: ProtoError) -> Self {
        match e {
            ProtoError::Io(io) => ClientError::Io(io),
            other => ClientError::Proto(other),
        }
    }
}

impl ClientError {
    /// The server's ERR packet, if this is one.
    #[must_use]
    pub fn server_error(&self) -> Option<&ErrPacket> {
        match self {
            ClientError::Server(e) => Some(e),
            _ => None,
        }
    }

    /// Whether the connection to the server is lost or unusable: anything
    /// but a server ERR or a misuse.
    #[must_use]
    pub fn is_connection_lost(&self) -> bool {
        !matches!(self, ClientError::Server(_) | ClientError::Busy(_))
    }
}

/// The result type of the backend client.
pub type ClientResult<T> = Result<T, ClientError>;
