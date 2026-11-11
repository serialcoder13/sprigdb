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

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
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
/// HTTP/1.1 only (ALPN `http/1.1`), TLS 1.2 and 1.3.
///
/// # Errors
/// 39998 for a file that can't be read or parsed, or a key that doesn't
/// match the certificate.
pub fn server_config(
    cert: &Path,
    key_file: &Path,
    client_ca: Option<&Path>,
) -> Result<Arc<rustls::ServerConfig>> {
    let chain = certs(cert, "[controller] admin_tls_cert")?;
    let key = key(key_file, "[controller] admin_tls_key")?;
    let builder = rustls::ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(|e| config_err(format!("Admin API TLS: {e}")))?;
    let builder = match client_ca {
        None => builder.with_no_client_auth(),
        Some(ca) => {
            let v = rustls::server::WebPkiClientVerifier::builder_with_provider(
                Arc::new(roots(ca, "[controller] admin_tls_client_ca")?),
                provider(),
            )
            .build()
            .map_err(|e| config_err(format!("[controller] admin_tls_client_ca: {e}")))?;
            builder.with_client_cert_verifier(v)
        }
    };
    let mut cfg = builder.with_single_cert(chain, key).map_err(|e| {
        config_err(format!(
            "[controller] admin_tls_cert and admin_tls_key: {e}"
        ))
    })?;
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(cfg))
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

#[cfg(test)]
mod tests;
