//! Connection options of the backend client and its TLS setup.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::WebPkiSupportedAlgorithms;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use zeroize::Zeroizing;

use super::error::{ClientError, ClientResult};
use crate::packets::Caps;

/// Whether the connection uses TLS.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TlsMode {
    /// Plaintext only.
    Disabled,
    /// TLS when the server offers it, else plaintext.
    Preferred,
    /// TLS or fail. Backend connections always use this.
    #[default]
    Required,
}

/// The TLS client certificate: PEM files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientCert {
    /// Certificate chain, leaf first.
    pub cert_file: PathBuf,
    /// The private key (PKCS#8, PKCS#1 or SEC1).
    pub key_file: PathBuf,
}

/// How to build the rustls configuration ([`TlsOptions::build`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TlsOptions {
    /// CA bundle to verify the server against; Mozilla's roots without it.
    pub ca_file: Option<PathBuf>,
    /// Encrypt without verifying the server's certificate: development
    /// only (MariaDB's self-generated certificates).
    pub skip_verify: bool,
    /// The client certificate, for accounts with `REQUIRE X509` or
    /// `REQUIRE SUBJECT ... ISSUER ...`.
    pub client_cert: Option<ClientCert>,
}

impl TlsOptions {
    /// Builds a rustls client configuration: TLS 1.2 and 1.3 with the ring
    /// provider.
    ///
    /// # Errors
    /// [`ClientError::Tls`] if a file can't be read or parsed.
    pub fn build(&self) -> ClientResult<Arc<rustls::ClientConfig>> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
            .map_err(|e| ClientError::Tls(e.to_string()))?;
        let builder = if self.skip_verify {
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerify(
                    provider.signature_verification_algorithms,
                )))
        } else {
            let mut roots = rustls::RootCertStore::empty();
            if let Some(ca) = &self.ca_file {
                let pem = std::fs::read(ca)
                    .map_err(|e| ClientError::Tls(format!("reading {}: {e}", ca.display())))?;
                for cert in CertificateDer::pem_slice_iter(&pem) {
                    let cert =
                        cert.map_err(|e| ClientError::Tls(format!("{}: {e}", ca.display())))?;
                    roots
                        .add(cert)
                        .map_err(|e| ClientError::Tls(format!("{}: {e}", ca.display())))?;
                }
            } else {
                roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            }
            builder.with_root_certificates(roots)
        };
        let config = match &self.client_cert {
            None => builder.with_no_client_auth(),
            Some(cc) => {
                let read = |p: &PathBuf| {
                    std::fs::read(p)
                        .map_err(|e| ClientError::Tls(format!("reading {}: {e}", p.display())))
                };
                let cert_pem = read(&cc.cert_file)?;
                let key_pem = Zeroizing::new(read(&cc.key_file)?);
                let chain = CertificateDer::pem_slice_iter(&cert_pem)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| ClientError::Tls(format!("{}: {e}", cc.cert_file.display())))?;
                let key = PrivateKeyDer::from_pem_slice(&key_pem)
                    .map_err(|e| ClientError::Tls(format!("{}: {e}", cc.key_file.display())))?;
                builder
                    .with_client_auth_cert(chain, key)
                    .map_err(|e| ClientError::Tls(e.to_string()))?
            }
        };
        Ok(Arc::new(config))
    }
}

/// Accepts any server certificate but still checks the handshake
/// signatures, so the session keys belong to whoever holds the presented
/// key.
#[derive(Debug)]
struct NoVerify(WebPkiSupportedAlgorithms);

impl ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_schemes()
    }
}

/// Options for one backend connection.
#[derive(Clone)]
#[allow(clippy::struct_excessive_bools)] // independent protocol options
pub struct ClientConfig {
    /// The account.
    pub user: String,
    /// Its password; empty for certificate-only accounts.
    pub password: Zeroizing<String>,
    /// Default schema at login (`CONNECT_WITH_DB`).
    pub database: Option<String>,
    /// TLS policy.
    pub tls: TlsMode,
    /// The rustls configuration; built from [`TlsOptions::default`] when
    /// TLS is used and this is `None`.
    pub tls_config: Option<Arc<rustls::ClientConfig>>,
    /// Name to verify the certificate against and send as SNI; the host
    /// part of the address when `None`.
    pub server_name: Option<String>,
    /// Collation id in the handshake response.
    pub collation: u8,
    /// Largest packet accepted from the server.
    pub max_packet: usize,
    /// Negotiate `CLIENT_FOUND_ROWS` (the session's client did).
    pub found_rows: bool,
    /// Negotiate `CLIENT_LOCAL_FILES`; off unless asked.
    pub local_files: bool,
    /// Negotiate `CLIENT_MULTI_STATEMENTS` (the gateway's sync prefixes
    /// need it).
    pub multi_statements: bool,
    /// zlib compression after login, if the server offers it.
    pub compress: bool,
    /// Extra capabilities to ask for, such as MariaDB's
    /// `MARIADB_CLIENT_EXTENDED_METADATA`; masked by what the server offers.
    pub extra_caps: Caps,
    /// Connection attributes.
    pub attrs: Vec<(String, String)>,
    /// Deadline for TCP, TLS and authentication together.
    pub connect_timeout: Duration,
}

impl std::fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientConfig")
            .field("user", &self.user)
            .field("database", &self.database)
            .field("tls", &self.tls)
            .field("server_name", &self.server_name)
            .field("collation", &self.collation)
            .field("found_rows", &self.found_rows)
            .field("local_files", &self.local_files)
            .field("multi_statements", &self.multi_statements)
            .field("compress", &self.compress)
            .field("extra_caps", &self.extra_caps)
            .finish_non_exhaustive()
    }
}

impl ClientConfig {
    /// Options for `user` with `password`: TLS required, multi-statements
    /// on, collation 45 (`utf8mb4_general_ci`), 1 GiB packets, a 2 s
    /// connect deadline and `_client_name=sprigdb`.
    #[must_use]
    pub fn new(user: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            user: user.into(),
            password: Zeroizing::new(password.into()),
            database: None,
            tls: TlsMode::Required,
            tls_config: None,
            server_name: None,
            collation: 45,
            max_packet: 1 << 30,
            found_rows: false,
            local_files: false,
            multi_statements: true,
            compress: false,
            extra_caps: Caps::empty(),
            attrs: vec![
                ("_client_name".to_owned(), "sprigdb".to_owned()),
                (
                    "_client_version".to_owned(),
                    env!("CARGO_PKG_VERSION").to_owned(),
                ),
            ],
            connect_timeout: Duration::from_secs(2),
        }
    }
}
