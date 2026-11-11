//! HTTPS for the controller's Admin API (chapter 37, "Admin API over TLS";
//! hardening round 2): the server side the controller serves `admin_addr`
//! with, and the client side gateways and `sprigctl` call it with.
//!
//! - [`server_config`]: a certificate chain and key in PEM files, and
//!   optionally a CA whose client certificates are then required (mutual
//!   TLS).
//! - [`TlsListener`]: an [`axum::serve::Listener`] that accepts TCP and
//!   finishes each TLS handshake in its own task (a slow client never holds
//!   up the others), within [`HANDSHAKE_TIMEOUT`].
//! - [`client_config`] and [`send`]: one HTTP/1.1 request over a fresh TLS
//!   connection, verifying the server against a CA file and optionally
//!   presenting a client certificate. Admin calls are rare enough that a
//!   connection per call costs nothing that matters.
//!
//! Plain HTTP stays the default everywhere; this is used only when a
//! controller has `[controller] admin_tls_cert` set.

use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::sign::CertifiedKey;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::{Result, SprigCode, SprigError};

pub use rustls::{ClientConfig, ServerConfig};

/// The header in which a gateway relaying an Admin API call passes the
/// address its client connected from (ST47). The controller believes it
/// only with [`RELAY_AUTH_HEADER`].
pub const CLIENT_ADDR_HEADER: &str = "x-sprig-client-addr";

/// The header in which a relaying gateway presents its service identity's
/// secret (`[gateway] controller_token_file`), vouching for
/// [`CLIENT_ADDR_HEADER`].
pub const RELAY_AUTH_HEADER: &str = "x-sprig-relay-auth";

/// How long a client has to finish its TLS handshake.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

fn config_err(msg: String) -> SprigError {
    SprigError::new(SprigCode::ConfigInvalid, msg)
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn read(path: &Path, what: &str) -> Result<Vec<u8>> {
    std::fs::read(path).map_err(|e| config_err(format!("{what} {}: {e}", path.display())))
}

/// The certificates in a PEM file; at least one.
///
/// # Errors
/// 39998 if the file can't be read, doesn't parse, or holds none.
pub fn certs(path: &Path, what: &str) -> Result<Vec<CertificateDer<'static>>> {
    let pem = read(path, what)?;
    let certs = CertificateDer::pem_slice_iter(&pem)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| config_err(format!("{what} {}: {e}", path.display())))?;
    if certs.is_empty() {
        return Err(config_err(format!(
            "{what} {} holds no certificate",
            path.display()
        )));
    }
    Ok(certs)
}

fn key(path: &Path, what: &str) -> Result<PrivateKeyDer<'static>> {
    crate::secret_file::check(path, what)?;
    let pem = read(path, what)?;
    PrivateKeyDer::from_pem_slice(&pem)
        .map_err(|e| config_err(format!("{what} {}: {e}", path.display())))
}

fn roots(ca: &Path, what: &str) -> Result<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    for c in certs(ca, what)? {
        roots
            .add(c)
            .map_err(|e| config_err(format!("{what} {}: {e}", ca.display())))?;
    }
    Ok(roots)
}

/// A server configuration: `cert` (the chain, leaf first) and `key`;
/// with `client_ca`, every client must present a certificate it signed.
/// HTTP/1.1 only (ALPN `http/1.1`), TLS 1.2 and 1.3. The certificate and
/// key are re-read when either file changes ([`ReloadingCert`]), so a
/// rotated certificate is served without a restart.
///
/// # Errors
/// 39998 for a file that can't be read or parsed, or a key that doesn't
/// match the certificate.
pub fn server_config(
    cert: &Path,
    key_file: &Path,
    client_ca: Option<&Path>,
) -> Result<Arc<rustls::ServerConfig>> {
    server_config_named(cert, key_file, client_ca, ADMIN_NAMES)
}

/// The configuration keys error messages name: certificate, key, client CA.
pub type KeyNames = [&'static str; 3];

/// The Admin API's keys (chapter 37, "Admin API over TLS").
pub const ADMIN_NAMES: KeyNames = [
    "[controller] admin_tls_cert",
    "[controller] admin_tls_key",
    "[controller] admin_tls_client_ca",
];

/// The internal services' keys (`[controller] internal_tls`: the
/// process identity in `[tls]`).
pub const INTERNAL_NAMES: KeyNames = ["[tls] cert", "[tls] key", "[tls] ca"];

/// [`server_config`] with the configuration keys `names` in its errors.
///
/// # Errors
/// As [`server_config`].
pub fn server_config_named(
    cert: &Path,
    key_file: &Path,
    client_ca: Option<&Path>,
    names: KeyNames,
) -> Result<Arc<rustls::ServerConfig>> {
    let resolver = Arc::new(ReloadingCert::new(cert, key_file, names)?);
    let builder = rustls::ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(|e| config_err(format!("TLS: {e}")))?;
    let builder = match client_ca {
        None => builder.with_no_client_auth(),
        Some(ca) => {
            let v = rustls::server::WebPkiClientVerifier::builder_with_provider(
                Arc::new(roots(ca, names[2])?),
                provider(),
            )
            .build()
            .map_err(|e| config_err(format!("{}: {e}", names[2])))?;
            builder.with_client_cert_verifier(v)
        }
    };
    let mut cfg = builder.with_cert_resolver(resolver);
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(cfg))
}

/// How often a [`ReloadingCert`] looks at its files' modification times.
pub const RELOAD_CHECK: Duration = Duration::from_secs(2);

/// A server certificate read from two PEM files and read again when
/// either file's modification time changes (looked at, at most every
/// [`RELOAD_CHECK`], on a handshake). A pair that doesn't load (a key
/// written before its certificate, say) keeps the previous one served and
/// logs a warning; the next change loads again. `sprig-vm.sh rotate-certs`
/// writes the key first and then the certificate, each by rename.
pub struct ReloadingCert {
    cert: PathBuf,
    key: PathBuf,
    names: KeyNames,
    state: Mutex<CertState>,
}

struct CertState {
    checked: Instant,
    stamp: (Option<SystemTime>, Option<SystemTime>),
    current: Arc<CertifiedKey>,
}

impl std::fmt::Debug for ReloadingCert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReloadingCert")
            .field("cert", &self.cert)
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

fn modified(p: &Path) -> Option<SystemTime> {
    std::fs::metadata(p).and_then(|m| m.modified()).ok()
}

impl ReloadingCert {
    /// Reads the pair once.
    ///
    /// # Errors
    /// 39998 for a file that can't be read or parsed, or a key that
    /// doesn't match the certificate.
    pub fn new(cert: &Path, key_file: &Path, names: KeyNames) -> Result<Self> {
        let stamp = (modified(cert), modified(key_file));
        let current = Arc::new(certified(cert, key_file, names)?);
        Ok(Self {
            cert: cert.to_path_buf(),
            key: key_file.to_path_buf(),
            names,
            state: Mutex::new(CertState {
                checked: Instant::now(),
                stamp,
                current,
            }),
        })
    }

    /// The pair served now, read again first if the files changed.
    #[must_use]
    pub fn current(&self) -> Arc<CertifiedKey> {
        let mut st = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if st.checked.elapsed() >= RELOAD_CHECK {
            st.checked = Instant::now();
            let stamp = (modified(&self.cert), modified(&self.key));
            if stamp != st.stamp {
                st.stamp = stamp;
                match certified(&self.cert, &self.key, self.names) {
                    Ok(ck) => {
                        st.current = Arc::new(ck);
                        tracing::info!(cert = %self.cert.display(), "server certificate re-read");
                    }
                    Err(e) => tracing::warn!(
                        cert = %self.cert.display(),
                        error = %e,
                        "server certificate changed but doesn't load; still serving the previous one"
                    ),
                }
            }
        }
        Arc::clone(&st.current)
    }
}

impl rustls::server::ResolvesServerCert for ReloadingCert {
    fn resolve(&self, _hello: rustls::server::ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.current())
    }
}

fn certified(cert: &Path, key_file: &Path, names: KeyNames) -> Result<CertifiedKey> {
    let chain = certs(cert, names[0])?;
    let key = key(key_file, names[1])?;
    CertifiedKey::from_der(chain, key, &provider())
        .map_err(|e| config_err(format!("{} and {}: {e}", names[0], names[1])))
}

/// A client configuration verifying servers against the CA in `ca`, and
/// presenting `client` (certificate chain, key) when given.
///
/// # Errors
/// 39998 for a file that can't be read or parsed.
pub fn client_config(
    ca: &Path,
    client: Option<(&Path, &Path)>,
) -> Result<Arc<rustls::ClientConfig>> {
    let builder = rustls::ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(|e| config_err(format!("Admin API TLS: {e}")))?
        .with_root_certificates(roots(ca, "the Admin API's CA file")?);
    let mut cfg = match client {
        None => builder.with_no_client_auth(),
        Some((c, k)) => builder
            .with_client_auth_cert(
                certs(c, "the client certificate")?,
                key(k, "the client key")?,
            )
            .map_err(|e| config_err(format!("the client certificate: {e}")))?,
    };
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(cfg))
}

/// Sends `req` (an `https://host:port/...` URI) over a new TLS connection
/// and returns the response head with its streaming body.
///
/// # Errors
/// A message naming what failed: the URI, the connection, the handshake
/// (an unknown CA, a name the certificate doesn't cover) or the exchange.
pub async fn send(
    config: &Arc<rustls::ClientConfig>,
    req: Request<Full<Bytes>>,
) -> std::result::Result<Response<Incoming>, String> {
    let uri = req.uri().clone();
    let host = uri
        .host()
        .ok_or_else(|| format!("{uri}: no host"))?
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_owned();
    let port = uri.port_u16().unwrap_or(443);
    let tcp = TcpStream::connect((host.as_str(), port))
        .await
        .map_err(|e| format!("{host}:{port}: {e}"))?;
    let _ = tcp.set_nodelay(true);
    let name = ServerName::try_from(host.clone()).map_err(|e| format!("{host}: {e}"))?;
    let tls = tokio_rustls::TlsConnector::from(Arc::clone(config))
        .connect(name, tcp)
        .await
        .map_err(|e| format!("TLS with {host}:{port}: {e}"))?;
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tls))
        .await
        .map_err(|e| format!("{host}:{port}: {e}"))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    // HTTP/1.1 wants the host header; the request line carries the path.
    let (mut parts, body) = req.into_parts();
    if !parts.headers.contains_key(hyper::header::HOST)
        && let Ok(v) = hyper::header::HeaderValue::from_str(&format!("{host}:{port}"))
    {
        parts.headers.insert(hyper::header::HOST, v);
    }
    if let Some(pq) = uri.path_and_query()
        && let Ok(u) = pq.as_str().parse()
    {
        parts.uri = u;
    }
    sender
        .send_request(Request::from_parts(parts, body))
        .await
        .map_err(|e| format!("{host}:{port}: {e}"))
}

/// A TLS listener for [`axum::serve`] (see the module notes). Wrap it in
/// `axum::serve::ListenerExt::tap_io` to get the client's address as
/// `ConnectInfo<SocketAddr>`.
#[derive(Debug)]
pub struct TlsListener {
    rx: mpsc::Receiver<(tokio_rustls::server::TlsStream<TcpStream>, SocketAddr)>,
    local: SocketAddr,
}

impl TlsListener {
    /// Serves TLS with `config` on `listener`; the accept loop ends when
    /// this value is dropped.
    ///
    /// # Errors
    /// Never in practice (the listener's own address).
    pub fn new(listener: TcpListener, config: Arc<rustls::ServerConfig>) -> Result<Self> {
        let local = listener
            .local_addr()
            .map_err(|e| SprigError::internal("TLS listener address").with_source(e))?;
        let (tx, rx) = mpsc::channel(64);
        let acceptor = tokio_rustls::TlsAcceptor::from(config);
        tokio::spawn(async move {
            loop {
                let (tcp, peer) = tokio::select! {
                    () = tx.closed() => return,
                    r = listener.accept() => match r {
                        Ok(x) => x,
                        Err(e) => {
                            tracing::debug!(error = %e, "Admin API accept failed");
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                    },
                };
                let _ = tcp.set_nodelay(true);
                let (acceptor, tx) = (acceptor.clone(), tx.clone());
                tokio::spawn(async move {
                    match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp)).await {
                        Ok(Ok(s)) => {
                            let _ = tx.send((s, peer)).await;
                        }
                        Ok(Err(e)) => {
                            tracing::debug!(%peer, error = %e, "Admin API TLS handshake failed");
                        }
                        Err(_) => tracing::debug!(%peer, "Admin API TLS handshake timed out"),
                    }
                });
            }
        });
        Ok(Self { rx, local })
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = tokio_rustls::server::TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.rx.recv().await {
            Some(x) => x,
            // The accept loop ended (it never does while this lives).
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        Ok(self.local)
    }
}

// ---------------------------------------------------------------------------
// The internal services' client side (hardening round 3)
// ---------------------------------------------------------------------------

static INTERNAL_CLIENT: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();

/// Installs this process's client identity for `https://` internal calls
/// (permits, peer pings, observations, the relay; chapter 25): it verifies
/// controllers against `ca` and presents `cert` and `key` (the process
/// identity in `[tls]`). The first call wins; later ones are ignored.
///
/// # Errors
/// 39998 for a file that can't be read or parsed.
pub fn install_internal_client(ca: &Path, cert: &Path, key_file: &Path) -> Result<()> {
    let cfg = client_config(ca, Some((cert, key_file)))?;
    let _ = INTERNAL_CLIENT.set(cfg);
    Ok(())
}

/// The client identity [`install_internal_client`] installed, if any.
#[must_use]
pub fn internal_client() -> Option<Arc<rustls::ClientConfig>> {
    INTERNAL_CLIENT.get().cloned()
}

/// A connector for hyper's pooled client that speaks plain TCP to
/// `http://` addresses and TLS (with [`internal_client`]'s identity) to
/// `https://` ones, so one client serves controllers before and after
/// `[controller] internal_tls` is switched on.
#[derive(Debug, Clone)]
pub struct InternalConnector {
    http: hyper_util::client::legacy::connect::HttpConnector,
}

impl InternalConnector {
    /// A connector whose TCP connects time out after `timeout`.
    #[must_use]
    pub fn new(timeout: Duration) -> Self {
        let mut c = hyper_util::client::legacy::connect::HttpConnector::new();
        c.set_connect_timeout(Some(timeout));
        c.set_nodelay(true);
        c.enforce_http(false);
        Self { http: c }
    }
}

/// A connection [`InternalConnector`] made.
#[derive(Debug)]
pub enum MaybeTls {
    /// `http://`.
    Plain(TokioIo<TcpStream>),
    /// `https://`.
    Tls(Box<TokioIo<tokio_rustls::client::TlsStream<TcpStream>>>),
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

impl tower_service::Service<hyper::Uri> for InternalConnector {
    type Response = MaybeTls;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = std::result::Result<MaybeTls, BoxError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), BoxError>> {
        tower_service::Service::<hyper::Uri>::poll_ready(&mut self.http, cx).map_err(Into::into)
    }

    fn call(&mut self, uri: hyper::Uri) -> Self::Future {
        let mut http = self.http.clone();
        Box::pin(async move {
            let https = uri.scheme_str() == Some("https");
            let tcp = tower_service::Service::call(&mut http, uri.clone()).await?;
            if !https {
                return Ok(MaybeTls::Plain(tcp));
            }
            let cfg = internal_client().ok_or_else(|| {
                format!(
                    "{uri}: an https:// internal address needs this process's [tls] (ca, cert, key)"
                )
            })?;
            let host = uri
                .host()
                .unwrap_or_default()
                .trim_start_matches('[')
                .trim_end_matches(']')
                .to_owned();
            let name = ServerName::try_from(host.clone()).map_err(|e| format!("{host}: {e}"))?;
            let tls = tokio_rustls::TlsConnector::from(cfg)
                .connect(name, tcp.into_inner())
                .await
                .map_err(|e| format!("TLS with {host}: {e}"))?;
            Ok(MaybeTls::Tls(Box::new(TokioIo::new(tls))))
        })
    }
}

impl hyper_util::client::legacy::connect::Connection for MaybeTls {
    fn connected(&self) -> hyper_util::client::legacy::connect::Connected {
        match self {
            Self::Plain(s) => s.connected(),
            Self::Tls(_) => hyper_util::client::legacy::connect::Connected::new(),
        }
    }
}

impl hyper::rt::Read for MaybeTls {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl hyper::rt::Write for MaybeTls {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

#[cfg(test)]
mod tests;
