//! TLS to the binlog source (chapter 10, "The binlog client"; chapter 38,
//! "Binlog sources").
//!
//! The reader upgrades its connection the way every MySQL client does:
//! after the server's greeting it sends an SSL request (the handshake
//! response's first 32 bytes with `CLIENT_SSL`), runs the TLS handshake on
//! the same socket, and sends the real handshake response inside it. The
//! rustls set-up matches the backend client's (`sprig-proto-mysql`'s
//! `TlsOptions`): TLS 1.2 and 1.3 with the ring provider, the cluster's CA
//! when the record names one and Mozilla's roots otherwise, and a
//! development-only mode that encrypts without checking the certificate.
//!
//! Verification is the default: [`BinlogTls::verify`] checks the chain
//! against the CA and the name against the certificate (the host, or
//! [`BinlogTls::server_name`]).

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::WebPkiSupportedAlgorithms;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use sprig_common::{Result, SprigCode, SprigError};

/// How the reader secures its connection to the source.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct BinlogTls {
    /// A CA bundle file (PEM) to verify the server against.
    pub ca_file: Option<PathBuf>,
    /// CA certificates (PEM) held in memory, added to [`Self::ca_file`]'s.
    pub ca_pem: Option<Vec<u8>>,
    /// Encrypt without verifying the server's certificate: development
    /// only (MariaDB's self-generated certificates).
    pub skip_verify: bool,
    /// The name to verify the certificate against and send as SNI; the
    /// source's host when `None`.
    pub server_name: Option<String>,
}

impl fmt::Debug for BinlogTls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BinlogTls")
            .field("ca_file", &self.ca_file)
            .field("ca_pem", &self.ca_pem.as_ref().map(Vec::len))
            .field("skip_verify", &self.skip_verify)
            .field("server_name", &self.server_name)
            .finish()
    }
}

/// A TLS failure: the handshake, a certificate, or a CA file. Not
/// retryable as a disconnection is: the fix is in the configuration.
pub(crate) fn tls_error(msg: impl fmt::Display) -> SprigError {
    SprigError::new(SprigCode::BinlogUnsupported, format!("TLS: {msg}"))
}

impl BinlogTls {
    /// Verify against `ca_file` (Mozilla's roots when `None`).
    #[must_use]
    pub fn verify(ca_file: Option<PathBuf>) -> Self {
        Self {
            ca_file,
            ..Self::default()
        }
    }

    /// Encrypt without verifying (development).
    #[must_use]
    pub fn skip_verify() -> Self {
        Self {
            skip_verify: true,
            ..Self::default()
        }
    }

    /// The name the certificate must carry when connecting to `host`.
    ///
    /// # Errors
    /// `ER_SPRIG_BINLOG_UNSUPPORTED` if it isn't a DNS name or an address.
    pub(crate) fn name_for(&self, host: &str) -> Result<ServerName<'static>> {
        let name = self.server_name.as_deref().unwrap_or(host);
        let name = name
            .strip_prefix('[')
            .and_then(|n| n.strip_suffix(']'))
            .unwrap_or(name);
        ServerName::try_from(name.to_owned())
            .map_err(|e| tls_error(format!("server name {name:?}: {e}")))
    }

    /// The rustls client configuration.
    ///
    /// # Errors
    /// `ER_SPRIG_BINLOG_UNSUPPORTED` if a CA can't be read or parsed.
    pub fn client_config(&self) -> Result<Arc<rustls::ClientConfig>> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
            .map_err(tls_error)?;
        let builder = if self.skip_verify {
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerify(
                    provider.signature_verification_algorithms,
                )))
        } else {
            let mut roots = rustls::RootCertStore::empty();
            let mut add = |pem: &[u8], what: &str| -> Result<()> {
                let mut n = 0;
                for cert in CertificateDer::pem_slice_iter(pem) {
                    let cert = cert.map_err(|e| tls_error(format!("{what}: {e}")))?;
                    roots
                        .add(cert)
                        .map_err(|e| tls_error(format!("{what}: {e}")))?;
                    n += 1;
                }
                if n == 0 {
                    return Err(tls_error(format!("{what}: no PEM certificate")));
                }
                Ok(())
            };
            if let Some(ca) = &self.ca_file {
                let pem = std::fs::read(ca)
                    .map_err(|e| tls_error(format!("reading {}: {e}", ca.display())))?;
                add(&pem, &ca.display().to_string())?;
            }
            if let Some(pem) = &self.ca_pem {
                add(pem, "the CA certificates")?;
            }
            if self.ca_file.is_none() && self.ca_pem.is_none() {
                roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            }
            builder.with_root_certificates(roots)
        };
        Ok(Arc::new(builder.with_no_client_auth()))
    }
}

/// Accepts any server certificate but still checks the handshake
/// signatures, so the session keys belong to whoever holds the presented
/// key (as the backend client's `skip_verify`).
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
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configs_build_and_names_parse() {
        assert!(BinlogTls::skip_verify().client_config().is_ok());
        assert!(BinlogTls::verify(None).client_config().is_ok());
        let missing = BinlogTls::verify(Some("/nonexistent/ca.pem".into()));
        let e = missing.client_config().unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::BinlogUnsupported));
        assert!(e.message.starts_with("TLS: reading"));
        let junk = BinlogTls {
            ca_pem: Some(b"not a certificate".to_vec()),
            ..BinlogTls::default()
        };
        assert!(junk.client_config().is_err());
        let t = BinlogTls::default();
        assert!(matches!(
            t.name_for("127.0.0.1"),
            Ok(ServerName::IpAddress(_))
        ));
        assert!(matches!(t.name_for("[::1]"), Ok(ServerName::IpAddress(_))));
        assert!(matches!(
            t.name_for("db-0.db-internal.ns.svc"),
            Ok(ServerName::DnsName(_))
        ));
        let named = BinlogTls {
            server_name: Some("mariadb.example".into()),
            ..BinlogTls::default()
        };
        assert!(matches!(
            named.name_for("10.0.0.1"),
            Ok(ServerName::DnsName(_))
        ));
        assert!(!format!("{named:?}").contains("BEGIN"));
    }
}
