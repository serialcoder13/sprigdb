//! The client's transport: plaintext TCP, TLS over TCP, or any other
//! stream (tests).

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// A bidirectional byte stream the client can run over.
pub trait Io: AsyncRead + AsyncWrite + Send + Sync + Unpin {}

impl<T: AsyncRead + AsyncWrite + Send + Sync + Unpin> Io for T {}

/// The transport under the packet codec.
pub enum ClientStream {
    /// Plaintext TCP.
    Plain(TcpStream),
    /// TLS over TCP.
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
    /// Another stream, such as an in-memory pipe in tests; never upgraded
    /// to TLS.
    Other(Box<dyn Io>),
}

impl std::fmt::Debug for ClientStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ClientStream::Plain(_) => "ClientStream::Plain",
            ClientStream::Tls(_) => "ClientStream::Tls",
            ClientStream::Other(_) => "ClientStream::Other",
        })
    }
}

impl AsyncRead for ClientStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ClientStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            ClientStream::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
            ClientStream::Other(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ClientStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            ClientStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            ClientStream::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
            ClientStream::Other(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ClientStream::Plain(s) => Pin::new(s).poll_flush(cx),
            ClientStream::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
            ClientStream::Other(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ClientStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            ClientStream::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
            ClientStream::Other(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Negotiated TLS parameters of a backend connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientTlsInfo {
    /// `1.2` or `1.3`.
    pub version: &'static str,
    /// The IANA cipher suite name.
    pub cipher: &'static str,
}

impl ClientStream {
    /// The TLS parameters, or `None` for plaintext.
    #[must_use]
    pub fn tls_info(&self) -> Option<ClientTlsInfo> {
        let ClientStream::Tls(s) = self else {
            return None;
        };
        let conn = s.get_ref().1;
        Some(ClientTlsInfo {
            version: match conn.protocol_version() {
                Some(rustls::ProtocolVersion::TLSv1_3) => "1.3",
                Some(rustls::ProtocolVersion::TLSv1_2) => "1.2",
                _ => "unknown",
            },
            cipher: conn
                .negotiated_cipher_suite()
                .and_then(|c| c.suite().as_str())
                .unwrap_or("unknown"),
        })
    }
}
