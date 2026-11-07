//! The boundary between the listener manager and the protocol front ends
//! (chapter 04, "Interfaces").
//!
//! The listener manager accepts a socket, applies the PROXY header and the
//! listener's TLS mode, and hands the front end a [`ClientConn`]. Front ends
//! (MySQL and MariaDB in chapter 05, MongoDB in 30, Redis in 40, HTTP in 37)
//! implement [`Frontend`].

use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use sprig_common::time::Clock;
use sprig_common::{CancellationToken, Result, SprigError};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;

use crate::listener::{ListenerSpec, TlsMode};

/// A client protocol, one per listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Protocol {
    /// MySQL 8.4 persona, 3306.
    MySql84,
    /// MariaDB persona, 3307.
    MariaDb,
    /// MongoDB, 27017 (R5).
    Mongo,
    /// RESP2 and RESP3, 6379 (R6).
    Redis,
    /// REST and gRPC, 8080 and 8443.
    Http,
    /// Bolt for Neo4j drivers, 7687 (R8).
    Bolt,
    /// Kafka protocol, 9092 (R9).
    Kafka,
}

impl Protocol {
    /// Whether the client sends the first bytes. MySQL and MariaDB clients
    /// wait for the server's greeting; every other protocol speaks first, so
    /// its listener can detect a TLS `ClientHello` from the first byte.
    #[must_use]
    pub const fn client_speaks_first(self) -> bool {
        !matches!(self, Protocol::MySql84 | Protocol::MariaDb)
    }

    /// The log and metric name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Protocol::MySql84 => "mysql",
            Protocol::MariaDb => "mariadb",
            Protocol::Mongo => "mongo",
            Protocol::Redis => "redis",
            Protocol::Http => "http",
            Protocol::Bolt => "bolt",
            Protocol::Kafka => "kafka",
        }
    }
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What every front end shares. Chapter 04 lists the full set (settings,
/// `MetaStore`, catalogs, auth, logic, executor, backends, limits,
/// sessions); fields arrive with the build steps that implement them.
#[non_exhaustive]
pub struct GatewayContext {
    /// The clock every component reads time through.
    pub clock: Arc<dyn Clock>,
}

impl GatewayContext {
    /// A context with only a clock.
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self { clock }
    }
}

impl fmt::Debug for GatewayContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatewayContext").finish_non_exhaustive()
    }
}

/// A protocol front end.
#[async_trait]
pub trait Frontend: Send + Sync + 'static {
    /// The protocol it serves.
    fn protocol(&self) -> Protocol;

    /// Serves one client connection until it ends: handshake, login,
    /// command loop. Returning closes the connection.
    ///
    /// # Errors
    /// Any error that ended the connection; the listener logs it.
    async fn serve(&self, conn: ClientConn, ctx: Arc<GatewayContext>) -> Result<()>;
}

/// TLS the front end negotiates inside its own protocol, after the
/// greeting. Only the MySQL family does this (`CLIENT_SSL`, chapter 05);
/// every other listener has finished TLS before the front end runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InProtocolTls {
    /// Don't offer TLS: the listener is `off`, or TLS was already handled
    /// by first-byte detection.
    NotOffered,
    /// Offer TLS; plaintext is accepted too (`optional`).
    Allowed,
    /// Offer TLS and refuse a plaintext login (`required`; MySQL 3159).
    Required,
}

/// A stream that is plaintext or server-side TLS.
pub enum MaybeTls<S> {
    /// Plaintext.
    Plain(S),
    /// TLS, after a completed handshake.
    Tls(Box<tokio_rustls::server::TlsStream<S>>),
}

impl<S> fmt::Debug for MaybeTls<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            MaybeTls::Plain(_) => "MaybeTls::Plain",
            MaybeTls::Tls(_) => "MaybeTls::Tls",
        })
    }
}

impl<S> MaybeTls<S> {
    /// The negotiated TLS parameters, or `None` for plaintext.
    #[must_use]
    pub fn tls_info(&self) -> Option<TlsInfo> {
        let MaybeTls::Tls(s) = self else { return None };
        let conn = s.get_ref().1;
        Some(TlsInfo {
            version: match conn.protocol_version() {
                Some(rustls::ProtocolVersion::TLSv1_3) => "1.3",
                Some(rustls::ProtocolVersion::TLSv1_2) => "1.2",
                _ => "unknown",
            },
            cipher: conn
                .negotiated_cipher_suite()
                .and_then(|c| c.suite().as_str())
                .unwrap_or("unknown"),
            sni: conn.server_name().map(str::to_owned),
        })
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for MaybeTls<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTls::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for MaybeTls<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTls::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_flush(cx),
            MaybeTls::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTls::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Negotiated TLS parameters of a connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsInfo {
    /// `1.2` or `1.3`.
    pub version: &'static str,
    /// The IANA cipher suite name.
    pub cipher: &'static str,
    /// The server name the client asked for, if any.
    pub sni: Option<String>,
}

/// An accepted client connection, handed to a [`Frontend`].
pub struct ClientConn {
    /// The client stream: TLS already established for client-speaks-first
    /// protocols when the client asked for it.
    pub stream: MaybeTls<TcpStream>,
    /// The client address, after the PROXY header.
    pub peer: SocketAddr,
    /// The listener as it was when the connection was accepted.
    pub listener: Arc<ListenerSpec>,
    /// The listener generation at accept.
    pub generation: u64,
    /// TLS the front end should offer in its own protocol (MySQL family).
    pub in_protocol_tls: InProtocolTls,
    /// Cancelled when the listener retires this connection: the listener
    /// moved, was disabled or tightened, and `listener.drain_seconds` passed;
    /// or the gateway's drain timed out. The listener then drops the front
    /// end's future, which closes the connection. Closing at a safe point
    /// with 39004 first is build step 8, with sessions.
    pub retired: CancellationToken,
    pub(crate) acceptor: Option<TlsAcceptor>,
}

impl fmt::Debug for ClientConn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientConn")
            .field("stream", &self.stream)
            .field("peer", &self.peer)
            .field("listener", &self.listener.name)
            .field("generation", &self.generation)
            .field("in_protocol_tls", &self.in_protocol_tls)
            .finish_non_exhaustive()
    }
}

impl ClientConn {
    /// The negotiated TLS parameters, or `None` for plaintext.
    #[must_use]
    pub fn tls_info(&self) -> Option<TlsInfo> {
        self.stream.tls_info()
    }

    /// Upgrades a plaintext connection to TLS with the listener's
    /// certificate, for protocols that negotiate TLS in-protocol (the MySQL
    /// family's `SSLRequest`). The caller has already consumed the request.
    ///
    /// # Errors
    /// `Internal` if TLS is not offered on this connection or the stream is
    /// already TLS; the handshake's error if it fails. The connection is
    /// unusable after an error.
    pub async fn start_tls(self) -> Result<ClientConn> {
        let ClientConn {
            stream,
            peer,
            listener,
            generation,
            in_protocol_tls,
            retired,
            acceptor,
        } = self;
        let (
            MaybeTls::Plain(tcp),
            Some(acceptor),
            InProtocolTls::Allowed | InProtocolTls::Required,
        ) = (stream, acceptor.clone(), in_protocol_tls)
        else {
            return Err(SprigError::internal(
                "start_tls on a connection without in-protocol TLS",
            ));
        };
        let tls = acceptor.accept(tcp).await.map_err(|e| {
            crate::listener::count_handshake(listener.name, None, "error");
            SprigError::internal("TLS handshake failed").with_source(e)
        })?;
        let stream = MaybeTls::Tls(Box::new(tls));
        crate::listener::count_handshake(listener.name, stream.tls_info().as_ref(), "ok");
        Ok(ClientConn {
            stream,
            peer,
            listener,
            generation,
            in_protocol_tls: InProtocolTls::NotOffered,
            retired,
            acceptor: None,
        })
    }
}

impl InProtocolTls {
    /// The in-protocol TLS offer for a listener in `mode`.
    #[must_use]
    pub const fn for_mode(mode: TlsMode) -> Self {
        match mode {
            TlsMode::Off => InProtocolTls::NotOffered,
            TlsMode::Optional => InProtocolTls::Allowed,
            TlsMode::Required => InProtocolTls::Required,
        }
    }
}
