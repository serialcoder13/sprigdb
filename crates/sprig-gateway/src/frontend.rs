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
use std::sync::atomic::{AtomicU8, Ordering};
use std::task::{Context, Poll};

use async_trait::async_trait;
use sprig_common::time::Clock;
use sprig_common::{CancellationToken, Result, SprigError};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;

use crate::listener::{ListenerSpec, TlsMode, TlsVersion};
use crate::session::SessionTable;

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
    /// AMQP 0-9-1 and 1.0, 5672 and 5671 (R9).
    Amqp,
}

impl Protocol {
    /// Whether the client sends the first bytes. MySQL and MariaDB clients
    /// wait for the server's greeting; every other protocol speaks first, so
    /// its listener can detect a TLS `ClientHello` from the first byte.
    #[must_use]
    pub const fn client_speaks_first(self) -> bool {
        !matches!(self, Protocol::MySql84 | Protocol::MariaDb)
    }

    /// Whether the front end closes its sessions at a safe point (after the
    /// current statement or transaction) when its listener is made stricter.
    /// The listener manager then waits twice `listener.drain_seconds` before
    /// forcing the older generation's connections closed, so the safe-point
    /// close at `drain_seconds` gets its chance first (chapter 04, "Live
    /// listener changes").
    #[must_use]
    pub const fn closes_at_safe_points(self) -> bool {
        matches!(self, Protocol::MySql84 | Protocol::MariaDb | Protocol::Http)
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
            Protocol::Amqp => "amqp",
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
    /// Every open session: connection ids, `SHOW PROCESSLIST`, `KILL`.
    pub sessions: Arc<SessionTable>,
    /// Backend pools, routing and read-your-writes (chapter 06); `None`
    /// in contexts built without them (tests of the listener layer).
    pub backends: Option<Arc<crate::backend::Backends>>,
    /// The other gateways of the DC, through the peer service (chapter 04
    /// step 10): cross-gateway `KILL`. `None` without registration.
    pub peers: Option<Arc<crate::peer::Peers>>,
    /// Connection, admission and memory limits (chapter 04 step 7); `None`
    /// for no limits (tests of the listener layer).
    pub limits: Option<Arc<crate::limits::Limiter>>,
}

impl GatewayContext {
    /// A context with a clock and an empty session table on slot 0.
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        let sessions = Arc::new(SessionTable::new(clock.clone()));
        Self::with_sessions(clock, sessions)
    }

    /// A context with a clock and a given session table.
    #[must_use]
    pub fn with_sessions(clock: Arc<dyn Clock>, sessions: Arc<SessionTable>) -> Self {
        Self {
            clock,
            sessions,
            backends: None,
            peers: None,
            limits: None,
        }
    }

    /// The same context with the gateway's limits.
    #[must_use]
    pub fn with_limits(mut self, limits: Arc<crate::limits::Limiter>) -> Self {
        self.limits = Some(limits);
        self
    }

    /// The same context with the peer service's clients.
    #[must_use]
    pub fn with_peers(mut self, peers: Arc<crate::peer::Peers>) -> Self {
        self.peers = Some(peers);
        self
    }

    /// The same context with the gateway's backends.
    #[must_use]
    pub fn with_backends(mut self, backends: Arc<crate::backend::Backends>) -> Self {
        self.backends = Some(backends);
        self
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
            // rustls hands back peer certificates only after verifying them
            // against the listener's client verifier; listeners don't ask
            // for client certificates yet, so this stays false for now.
            client_cert: conn.peer_certificates().is_some_and(|c| !c.is_empty()),
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
    /// The client presented a certificate the listener verified (chapter
    /// 14's `REQUIRE X509`).
    pub client_cert: bool,
}

/// What a connection runs over now: plaintext or a TLS version. Set by the
/// listener after its TLS handshake and by [`ClientConn::start_tls`] after
/// an in-protocol one; read when a listener is made stricter, to keep the
/// connections that already meet the new spec (chapter 04, "Live listener
/// changes").
#[derive(Debug, Default)]
pub struct ConnTransport(AtomicU8);

impl ConnTransport {
    const PLAIN: u8 = 0;
    const TLS_1_2: u8 = 2;
    const TLS_1_3: u8 = 3;

    /// Records the negotiated TLS parameters (`None`: plaintext). A
    /// version rustls doesn't name counts as 1.2, the lower one.
    pub fn set(&self, tls: Option<&TlsInfo>) {
        let v = match tls {
            None => Self::PLAIN,
            Some(i) if i.version == "1.3" => Self::TLS_1_3,
            Some(_) => Self::TLS_1_2,
        };
        self.0.store(v, Ordering::Release);
    }

    /// The TLS version, or `None` for plaintext.
    #[must_use]
    pub fn tls(&self) -> Option<TlsVersion> {
        match self.0.load(Ordering::Acquire) {
            Self::PLAIN => None,
            Self::TLS_1_3 => Some(TlsVersion::V1_3),
            _ => Some(TlsVersion::V1_2),
        }
    }
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
    /// Cancelled when the listener retires this connection by force: the
    /// listener moved, was disabled or tightened so that this connection no
    /// longer meets it, and twice `listener.drain_seconds` passed (once for
    /// front ends without safe points); or the gateway's drain timed out.
    /// The listener then drops the front end's future, which closes the
    /// connection.
    pub retired: CancellationToken,
    /// Cancelled when the connection should close at its next safe point:
    /// at `listener.drain_seconds` after a listener change that this
    /// connection no longer meets, or as soon as the gateway drains. Front
    /// ends that can end gracefully (HTTP: `GOAWAY`, or `connection: close`
    /// on the next response) watch it; `retired` follows as the forced
    /// close.
    pub close_soon: CancellationToken,
    /// What the connection runs over now; [`ClientConn::start_tls`] updates
    /// it.
    pub transport: Arc<ConnTransport>,
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
            close_soon,
            transport,
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
        let info = stream.tls_info();
        crate::listener::count_handshake(listener.name, info.as_ref(), "ok");
        transport.set(info.as_ref());
        Ok(ClientConn {
            stream,
            peer,
            listener,
            generation,
            in_protocol_tls: InProtocolTls::NotOffered,
            retired,
            close_soon,
            transport,
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
