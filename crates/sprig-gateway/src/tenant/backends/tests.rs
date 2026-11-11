//! `[per_user]`'s TLS options (harden1's `ca_file`; lead's request in
//! hardening round 2): what the backend pool builds its rustls
//! configuration from, without a `GatewayLocator`.

use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::ServerName;
use sprig_common::ids::{ClusterId, TenantId, UserId};
use sprig_proto_mysql::client::TlsOptions;
use sprig_provision::credentials::{Credential, TlsMode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::PerUser;
use crate::testutil::{TempDir, TestCa};

fn per_user(dir: &Path, extra: &str) -> sprig_common::Result<PerUser> {
    std::fs::write(
        dir.join("backends.toml"),
        "[backend_secret]\npassword = \"0123456789abcdef0123456789abcdef\"\n",
    )
    .unwrap();
    let text = format!("secret = \"file:backends.toml#backend_secret\"\n{extra}");
    let v: toml::Value = toml::from_str::<toml::Table>(&text).unwrap().into();
    PerUser::from_toml(&v, dir)
}

fn credential(p: &PerUser) -> Credential {
    p.target(
        ClusterId::new("dc1-main").unwrap(),
        TenantId(7),
        UserId(3),
    )
    .credential()
    .unwrap()
}

/// The options the backend pool builds for `c` (`backend::Backends::tls_for`).
fn options(c: &Credential) -> TlsOptions {
    TlsOptions {
        ca_file: c.ca_file.clone(),
        skip_verify: c.tls == TlsMode::SkipVerify,
        client_cert: None,
    }
}

/// One TLS handshake against a server presenting `cert_pem`/`key_pem` for
/// `localhost`, with `config` on the client side.
async fn handshake(
    config: Arc<rustls::ClientConfig>,
    cert_pem: &str,
    key_pem: &str,
) -> std::io::Result<()> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    let chain = vec![CertificateDer::from_pem_slice(cert_pem.as_bytes()).unwrap()];
    let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes()).unwrap();
    let server = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(chain, key)
    .unwrap();
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
    let srv = tokio::spawn(async move {
        let (s, _) = l.accept().await.unwrap();
        if let Ok(mut t) = acceptor.accept(s).await {
            let mut b = [0u8; 4];
            let _ = t.read_exact(&mut b).await;
            let _ = t.write_all(b"pong").await;
        }
    });
    let tcp = tokio::net::TcpStream::connect(addr).await?;
    let mut t = tokio_rustls::TlsConnector::from(config)
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await?;
    t.write_all(b"ping").await?;
    let mut b = [0u8; 4];
    t.read_exact(&mut b).await?;
    srv.abort();
    assert_eq!(&b, b"pong");
    Ok(())
}

#[test]
fn ca_file_reaches_the_credential_resolved_against_the_file() {
    let d = TempDir::new();
    // Relative: next to the backends file.
    let p = per_user(d.path(), "tls = \"verify\"\nca_file = \"tls/ca.pem\"\n").unwrap();
    assert_eq!(p.tls, TlsMode::Verify);
    assert_eq!(p.ca_file, Some(d.path().join("tls/ca.pem")));
    let c = credential(&p);
    assert_eq!(c.user, "t7_u3");
    assert_eq!(c.ca_file, Some(d.path().join("tls/ca.pem")));
    assert_eq!(c.tls, TlsMode::Verify);

    // Absolute: as written. Unset: the built-in roots (None).
    let p = per_user(d.path(), "ca_file = \"/run/sprig/tls/ca.pem\"\n").unwrap();
    assert_eq!(credential(&p).ca_file, Some("/run/sprig/tls/ca.pem".into()));
    assert_eq!(p.tls, TlsMode::Verify, "verify is the default");
    let p = per_user(d.path(), "").unwrap();
    assert_eq!(credential(&p).ca_file, None);

    // An unknown key is refused (deny_unknown_fields), so a typo such as
    // `ca` doesn't silently fall back to the public roots.
    let e = per_user(d.path(), "ca = \"/x.pem\"\n").unwrap_err();
    assert!(e.to_string().contains("[per_user]"), "{e}");
}

#[test]
fn a_missing_ca_file_fails_when_the_tls_options_are_built() {
    let d = TempDir::new();
    let p = per_user(d.path(), "ca_file = \"nope.pem\"\n").unwrap();
    let e = options(&credential(&p)).build().unwrap_err();
    assert!(e.to_string().contains("nope.pem"), "{e}");

    // A file that holds no certificate is passed on as it is (the root
    // store is then empty); it never falls back to the public roots.
    std::fs::write(d.path().join("empty.pem"), "").unwrap();
    let p = per_user(d.path(), "ca_file = \"empty.pem\"\n").unwrap();
    let o = options(&credential(&p));
    assert_eq!(o.ca_file, Some(d.path().join("empty.pem")));
}

#[test]
fn skip_verify_ignores_the_ca_file() {
    let d = TempDir::new();
    let p = per_user(d.path(), "tls = \"skip_verify\"\nca_file = \"nope.pem\"\n").unwrap();
    let o = options(&credential(&p));
    assert!(o.skip_verify);
    // The missing CA isn't read: skip_verify doesn't use it.
    o.build().unwrap();
}

#[tokio::test]
async fn verify_trusts_the_private_ca_and_nothing_else() {
    let d = TempDir::new();
    let ca = TestCa::new();
    std::fs::write(
        d.path().join("ca.pem"),
        pem_of(&ca.der),
    )
    .unwrap();
    let leaf = ca.leaf("mariadb-1", (2099, 1, 1));
    let p = per_user(d.path(), "ca_file = \"ca.pem\"\n").unwrap();
    let config = options(&credential(&p)).build().unwrap();
    handshake(Arc::clone(&config), &leaf.cert_pem, &leaf.key_pem)
        .await
        .unwrap();

    // A server certificate from another CA is refused.
    let other = TestCa::new().leaf("mariadb-1", (2099, 1, 1));
    let e = handshake(config, &other.cert_pem, &other.key_pem)
        .await
        .unwrap_err();
    assert!(e.to_string().to_lowercase().contains("certificate"), "{e}");

    // Without ca_file the public roots don't know the private CA either.
    let p = per_user(d.path(), "").unwrap();
    let config = options(&credential(&p)).build().unwrap();
    assert!(
        handshake(config, &leaf.cert_pem, &leaf.key_pem)
            .await
            .is_err()
    );
}

fn pem_of(der: &rustls::pki_types::CertificateDer<'_>) -> String {
    use std::fmt::Write as _;
    let b64 = base64_encode(der.as_ref());
    let mut out = String::from("-----BEGIN CERTIFICATE-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        let _ = writeln!(out, "{}", std::str::from_utf8(chunk).unwrap());
    }
    out.push_str("-----END CERTIFICATE-----\n");
    out
}

fn base64_encode(b: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(b)
}
