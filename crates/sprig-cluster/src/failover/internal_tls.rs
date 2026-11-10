//! Mutual TLS for the agents' internal service (chapter 25, "As built in
//! R2 (r2final)"; decision RC2): the `node_rebuild` backup stream and the
//! worker's rebuild calls.
//!
//! Every process with a `[tls]` section (the internal CA, its own chain
//! and key, chapter 27's certificates) serves and calls the agents'
//! internal service over TLS 1.2+ with client certificates: the server
//! requires a certificate the internal CA signed, and the client checks
//! the server's against the same CA and the address it dialled (an IP SAN
//! for an IP, a DNS SAN for a name). The one-time token stays: TLS keeps
//! the stream private and proves both ends are SprigDB processes; the
//! token still says which target may pull which source's backup, once.
//!
//! Without `[tls]` the service is plain HTTP, for development only; the
//! agent warns at start, and the deployment runbook requires `[tls]`.

use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use sprig_common::{Result, SprigCode, SprigError};
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// How long a TLS handshake may take before the connection is dropped.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// The internal service's TLS: a server and a client configuration from
/// one identity and one CA.
#[derive(Clone)]
pub struct InternalTls {
    server: Arc<rustls::ServerConfig>,
    client: Arc<rustls::ClientConfig>,
}

impl std::fmt::Debug for InternalTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InternalTls")
    }
}

fn bad(what: &str, e: impl std::fmt::Display) -> SprigError {
    SprigError::new(SprigCode::ConfigInvalid, format!("[tls] {what}: {e}"))
}

impl InternalTls {
    /// From `[tls]`'s files.
    ///
    /// # Errors
    /// 39998 for an unreadable or invalid file.
    pub fn from_config(cfg: &sprig_common::config::TlsConfig) -> Result<Self> {
        let read =
            |p: &std::path::Path| std::fs::read(p).map_err(|e| bad(&p.display().to_string(), e));
        Self::from_pems(&read(&cfg.ca)?, &read(&cfg.cert)?, &read(&cfg.key)?)
    }

    /// From PEM text: the CA bundle, this process's chain and its key.
    ///
    /// # Errors
    /// 39998 for invalid PEM, an empty CA or a key that doesn't fit.
    pub fn from_pems(ca: &[u8], cert: &[u8], key: &[u8]) -> Result<Self> {
        let mut roots = rustls::RootCertStore::empty();
        for c in CertificateDer::pem_slice_iter(ca) {
            roots
                .add(c.map_err(|e| bad("ca", e))?)
                .map_err(|e| bad("ca", e))?;
        }
        if roots.is_empty() {
            return Err(bad("ca", "no certificate"));
        }
        let roots = Arc::new(roots);
        let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(cert)
            .collect::<std::result::Result<_, _>>()
            .map_err(|e| bad("cert", e))?;
        let key = PrivateKeyDer::from_pem_slice(key).map_err(|e| bad("key", e))?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::clone(&roots),
            Arc::clone(&provider),
        )
        .build()
        .map_err(|e| bad("ca", e))?;
        let mut server = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .map_err(|e| bad("versions", e))?
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain.clone(), key.clone_key())
            .map_err(|e| bad("cert", e))?;
        server.alpn_protocols = vec![b"http/1.1".to_vec()];
        let mut client = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| bad("versions", e))?
            .with_root_certificates(roots)
            .with_client_auth_cert(chain, key)
            .map_err(|e| bad("cert", e))?;
        client.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Self {
            server: Arc::new(server),
            client: Arc::new(client),
        })
    }

    /// The server side, for an accept loop.
    #[must_use]
    pub fn acceptor(&self) -> TlsAcceptor {
        TlsAcceptor::from(Arc::clone(&self.server))
    }

    /// Connects to `host:port` (as an `https://` URL names it) and
    /// completes the handshake, presenting this process's certificate and
    /// checking the server's against the CA and `host`.
    ///
    /// # Errors
    /// Internal for an unreachable server, a bad name or a failed
    /// handshake (an untrusted or mismatched certificate).
    pub async fn connect(
        &self,
        host: &str,
        port: u16,
    ) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
        let name = ServerName::try_from(
            host.trim_start_matches('[')
                .trim_end_matches(']')
                .to_owned(),
        )
        .map_err(|e| SprigError::internal(format!("{host}: {e}")))?;
        let tcp = tokio::time::timeout(
            HANDSHAKE_TIMEOUT,
            TcpStream::connect((host.trim_start_matches('[').trim_end_matches(']'), port)),
        )
        .await
        .map_err(|_| SprigError::internal(format!("{host}:{port}: connect timed out")))?
        .map_err(|e| SprigError::internal(format!("{host}:{port}: {e}")))?;
        let _ = tcp.set_nodelay(true);
        tokio::time::timeout(
            HANDSHAKE_TIMEOUT,
            TlsConnector::from(Arc::clone(&self.client)).connect(name, tcp),
        )
        .await
        .map_err(|_| SprigError::internal(format!("{host}:{port}: TLS handshake timed out")))?
        .map_err(|e| SprigError::internal(format!("{host}:{port}: TLS: {e}")))
    }
}

/// `scheme`, `host` and `port` of an internal URL (`http://h:p` or
/// `https://h:p`, an optional path ignored).
///
/// # Errors
/// Internal for anything else.
pub fn split_url(url: &str) -> Result<(bool, String, u16)> {
    let (tls, rest) = if let Some(r) = url.strip_prefix("https://") {
        (true, r)
    } else if let Some(r) = url.strip_prefix("http://") {
        (false, r)
    } else {
        return Err(SprigError::internal(format!("{url}: not an http(s) URL")));
    };
    let hostport = rest.split('/').next().unwrap_or(rest);
    let (host, port) = hostport
        .rsplit_once(':')
        .ok_or_else(|| SprigError::internal(format!("{url}: no port")))?;
    let port = port
        .parse()
        .map_err(|e| SprigError::internal(format!("{url}: port: {e}")))?;
    Ok((tls, host.to_owned(), port))
}

#[cfg(test)]
mod tests;
