//! The SMTP client against in-process servers: plain, STARTTLS, implicit
//! TLS with a client certificate, and the refusals (no STARTTLS offered, a
//! wrong CA, data injected after the STARTTLS answer, a missing client
//! certificate).

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

use std::sync::Arc;

use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair, SanType};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use super::*;

struct Ca {
    issuer: Issuer<'static, KeyPair>,
    pem: String,
}

impl Ca {
    fn new(cn: &str) -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.distinguished_name.push(DnType::CommonName, cn);
        let key = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        Self {
            pem: cert.pem(),
            issuer: Issuer::new(params, key),
        }
    }

    /// A leaf for `localhost` and 127.0.0.1: (chain, key) as PEM.
    fn leaf(&self, cn: &str) -> (String, String) {
        let mut params = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
        params
            .subject_alt_names
            .push(SanType::IpAddress("127.0.0.1".parse().unwrap()));
        params.distinguished_name.push(DnType::CommonName, cn);
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        (cert.pem(), key.serialize_pem())
    }
}

fn pem_certs(pem: &str) -> Vec<rustls::pki_types::CertificateDer<'static>> {
    rustls_pemfile::certs(&mut std::io::BufReader::new(pem.as_bytes()))
        .collect::<std::result::Result<_, _>>()
        .unwrap()
}

fn pem_key(pem: &str) -> rustls::pki_types::PrivateKeyDer<'static> {
    rustls_pemfile::private_key(&mut std::io::BufReader::new(pem.as_bytes()))
        .unwrap()
        .unwrap()
}

/// A server config for `ca`'s leaf; with `clients`, client certificates of
/// that CA are required.
fn server_tls(ca: &Ca, clients: Option<&Ca>) -> tokio_rustls::TlsAcceptor {
    let (cert, key) = ca.leaf("smtp");
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let b = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .unwrap();
    let b = match clients {
        Some(c) => {
            let mut roots = rustls::RootCertStore::empty();
            for x in pem_certs(&c.pem) {
                roots.add(x).unwrap();
            }
            let v = rustls::server::WebPkiClientVerifier::builder_with_provider(
                Arc::new(roots),
                provider,
            )
            .build()
            .unwrap();
            b.with_client_cert_verifier(v)
        }
        None => b.with_no_client_auth(),
    };
    let cfg = b.with_single_cert(pem_certs(&cert), pem_key(&key)).unwrap();
    tokio_rustls::TlsAcceptor::from(Arc::new(cfg))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Plain,
    StartTls,
    /// STARTTLS answered with extra bytes after `220` (an injection).
    StartTlsInjected,
    /// No STARTTLS in the EHLO answer.
    NoStartTls,
    Implicit,
}

/// Runs the SMTP dialogue on `r`/`w` until QUIT; returns what it read.
async fn dialogue<S>(s: S, mut tls: Option<tokio_rustls::TlsAcceptor>, mode: Mode) -> String
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let mut r = BufReader::new(s);
    r.get_mut().write_all(b"220 hi\r\n").await.unwrap();
    let mut got = String::new();
    let mut in_data = false;
    loop {
        let mut line = String::new();
        if r.read_line(&mut line).await.unwrap_or(0) == 0 {
            return got;
        }
        got.push_str(&line);
        if in_data {
            if line == ".\r\n" {
                in_data = false;
                r.get_mut().write_all(b"250 queued\r\n").await.unwrap();
            }
            continue;
        }
        if line.starts_with("STARTTLS") {
            if mode == Mode::StartTlsInjected {
                r.get_mut()
                    .write_all(b"220 go\r\n250 injected\r\n")
                    .await
                    .unwrap();
                return got;
            }
            r.get_mut().write_all(b"220 go\r\n").await.unwrap();
            let acceptor = tls.take().unwrap();
            let inner = r.into_inner();
            // The type changes after the upgrade: finish on a boxed stream.
            let t = acceptor.accept(inner).await.unwrap();
            let rest = Box::pin(dialogue_tls(t)).await;
            return got + "--tls--\r\n" + &rest;
        }
        let reply: &[u8] = if line.starts_with("EHLO") {
            match mode {
                Mode::StartTls | Mode::StartTlsInjected => b"250-hi\r\n250 STARTTLS\r\n",
                _ => b"250-hi\r\n250 AUTH PLAIN\r\n",
            }
        } else if line.starts_with("AUTH") {
            b"235 ok\r\n"
        } else if line.starts_with("DATA") {
            in_data = true;
            b"354 go\r\n"
        } else if line.starts_with("QUIT") {
            r.get_mut().write_all(b"221 bye\r\n").await.unwrap();
            return got;
        } else {
            b"250 ok\r\n"
        };
        r.get_mut().write_all(reply).await.unwrap();
    }
}

/// The dialogue after STARTTLS (no greeting).
async fn dialogue_tls<S>(s: S) -> String
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let mut r = BufReader::new(s);
    let mut got = String::new();
    let mut in_data = false;
    loop {
        let mut line = String::new();
        if r.read_line(&mut line).await.unwrap_or(0) == 0 {
            return got;
        }
        got.push_str(&line);
        if in_data {
            if line == ".\r\n" {
                in_data = false;
                r.get_mut().write_all(b"250 queued\r\n").await.unwrap();
            }
            continue;
        }
        let reply: &[u8] = if line.starts_with("EHLO") {
            b"250-hi\r\n250 AUTH PLAIN\r\n"
        } else if line.starts_with("AUTH") {
            b"235 ok\r\n"
        } else if line.starts_with("DATA") {
            in_data = true;
            b"354 go\r\n"
        } else if line.starts_with("QUIT") {
            r.get_mut().write_all(b"221 bye\r\n").await.unwrap();
            return got;
        } else {
            b"250 ok\r\n"
        };
        r.get_mut().write_all(reply).await.unwrap();
    }
}

/// One-connection server; returns its port and what it read.
async fn server(
    mode: Mode,
    tls: Option<tokio_rustls::TlsAcceptor>,
) -> (u16, tokio::task::JoinHandle<String>) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let h = tokio::spawn(async move {
        let (s, _) = l.accept().await.unwrap();
        if mode == Mode::Implicit {
            match tls.unwrap().accept(s).await {
                Ok(t) => dialogue(t, None, mode).await,
                Err(e) => format!("handshake failed: {e}"),
            }
        } else {
            dialogue(s, tls, mode).await
        }
    });
    (port, h)
}

fn mail() -> Mail {
    Mail {
        to: vec!["ana@x".into(), "bo@x".into()],
        subject: "s".into(),
        body: "b".into(),
    }
}

const DATE: &str = "Tue, 10 Nov 2026 14:03:11 +0000";

fn cfg(url: &str, auth: Option<(&str, &str)>, tls: TlsFiles) -> SmtpConfig {
    SmtpConfig::parse(
        url,
        "sprig@x",
        auth.map(|(u, p)| (u.to_owned(), p.to_owned())),
        tls,
    )
    .unwrap()
    .unwrap()
}

#[test]
fn urls_and_rendering() {
    let none = TlsFiles::default;
    assert_eq!(SmtpConfig::parse("", "a@b", None, none()).unwrap(), None);
    let c = cfg("smtp://127.0.0.1:2525", None, none());
    assert_eq!(
        (c.addr(), c.security),
        ("127.0.0.1:2525".into(), Security::Plain)
    );
    assert_eq!(cfg("smtp://mail", None, none()).addr(), "mail:25");
    let s = cfg("smtps://mail.example", None, none());
    assert_eq!(
        (s.addr(), s.security),
        ("mail.example:465".into(), Security::Implicit)
    );
    let s = cfg("smtp+starttls://mail.example/", None, none());
    assert_eq!(
        (s.addr(), s.security),
        ("mail.example:587".into(), Security::StartTls)
    );
    assert_eq!(cfg("smtps://[::1]:4650", None, none()).addr(), "[::1]:4650");
    assert!(SmtpConfig::parse("http://mail", "a@b", None, none()).is_err());
    assert!(SmtpConfig::parse("smtp://", "a@b", None, none()).is_err());
    assert!(SmtpConfig::parse("smtp://u@mail", "a@b", None, none()).is_err());
    assert!(SmtpConfig::parse("smtp://mail:x", "a@b", None, none()).is_err());
    // A password in the clear only to loopback.
    let auth = || Some(("u".to_owned(), "p".to_owned()));
    assert!(SmtpConfig::parse("smtp://mail.example", "a@b", auth(), none()).is_err());
    assert!(SmtpConfig::parse("smtp://127.0.0.1:2525", "a@b", auth(), none()).is_ok());
    assert!(SmtpConfig::parse("smtp://localhost", "a@b", auth(), none()).is_ok());
    assert!(SmtpConfig::parse("smtp+starttls://mail.example", "a@b", auth(), none()).is_ok());
    // A certificate without its key.
    let half = TlsFiles {
        client_cert_pem: Some("x".into()),
        ..TlsFiles::default()
    };
    assert!(SmtpConfig::parse("smtps://mail", "a@b", None, half).is_err());
    // The password never shows in Debug.
    let d = format!("{:?}", cfg("smtps://mail", Some(("u", "secret")), none()));
    assert!(!d.contains("secret"), "{d}");
    let m = Mail {
        to: vec!["x@y".into()],
        subject: "hi\r\nBcc: evil".into(),
        body: "line\n.dot".into(),
    };
    let r = render("s@d", &m, DATE, "1@sprig");
    assert!(r.contains("Subject: hiBcc: evil\r\n"));
    assert!(r.contains("\r\n..dot\r\n.\r\n"));
    assert_eq!(base64(b"\0u\0p"), "AHUAcA==");
    assert!(offers(
        "250-hi\r\n250-STARTTLS\r\n250 AUTH PLAIN LOGIN\r\n",
        "starttls"
    ));
    assert!(offers("250-hi\r\n250 AUTH PLAIN\r\n", "AUTH"));
    assert!(!offers("250-hi\r\n250 8BITMIME\r\n", "STARTTLS"));
}

#[tokio::test]
async fn plain_conversation() {
    let (port, h) = server(Mode::Plain, None).await;
    let c = cfg(
        &format!("smtp://127.0.0.1:{port}"),
        None,
        TlsFiles::default(),
    );
    send(&c, &mail(), DATE, "1@x").await.unwrap();
    let got = h.await.unwrap();
    assert!(got.contains("RCPT TO:<ana@x>\r\nRCPT TO:<bo@x>"));
    assert!(got.contains("Subject: s\r\n"));
}

#[tokio::test]
async fn starttls_upgrades_then_authenticates() {
    let ca = Ca::new("test ca");
    let (port, h) = server(Mode::StartTls, Some(server_tls(&ca, None))).await;
    let tls = TlsFiles {
        ca_pem: Some(ca.pem.clone()),
        ..TlsFiles::default()
    };
    let c = cfg(
        &format!("smtp+starttls://localhost:{port}"),
        Some(("ana", "pw")),
        tls,
    );
    send(&c, &mail(), DATE, "1@x").await.unwrap();
    let got = h.await.unwrap();
    let (before, after) = got.split_once("--tls--").unwrap();
    // Nothing secret before the upgrade; EHLO again, AUTH and the mail after.
    assert!(
        !before.contains("AUTH") && !before.contains("MAIL FROM"),
        "{before}"
    );
    assert!(after.starts_with("\r\nEHLO sprigdb"), "{after}");
    assert!(after.contains("AUTH PLAIN AGFuYQBwdw==\r\n"), "{after}");
    assert!(after.contains("RCPT TO:<bo@x>"), "{after}");
}

#[tokio::test]
async fn starttls_is_required_when_asked_for() {
    let (port, _h) = server(Mode::NoStartTls, None).await;
    let c = cfg(
        &format!("smtp+starttls://localhost:{port}"),
        None,
        TlsFiles::default(),
    );
    let e = send(&c, &mail(), DATE, "1@x").await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::NotifyFailed));
    assert!(e.message.contains("doesn't offer STARTTLS"), "{e}");
}

#[tokio::test]
async fn data_after_the_starttls_answer_is_refused() {
    let ca = Ca::new("test ca");
    let (port, _h) = server(Mode::StartTlsInjected, Some(server_tls(&ca, None))).await;
    let tls = TlsFiles {
        ca_pem: Some(ca.pem.clone()),
        ..TlsFiles::default()
    };
    let c = cfg(&format!("smtp+starttls://localhost:{port}"), None, tls);
    let e = send(&c, &mail(), DATE, "1@x").await.unwrap_err();
    assert!(e.message.contains("after its STARTTLS answer"), "{e}");
}

#[tokio::test]
async fn implicit_tls_checks_the_server_and_offers_a_client_certificate() {
    let server_ca = Ca::new("server ca");
    let client_ca = Ca::new("client ca");
    let (cert, key) = client_ca.leaf("sprig controller");
    // 1. Right CA, client certificate: delivered.
    let (port, h) = server(
        Mode::Implicit,
        Some(server_tls(&server_ca, Some(&client_ca))),
    )
    .await;
    let good = TlsFiles {
        ca_pem: Some(server_ca.pem.clone()),
        client_cert_pem: Some(cert.clone()),
        client_key_pem: Some(key.clone()),
    };
    let c = cfg(
        &format!("smtps://127.0.0.1:{port}"),
        Some(("u", "p")),
        good.clone(),
    );
    send(&c, &mail(), DATE, "1@x").await.unwrap();
    assert!(h.await.unwrap().contains("RCPT TO:<ana@x>"));
    // 2. No client certificate: the server refuses the handshake.
    let (port, h) = server(
        Mode::Implicit,
        Some(server_tls(&server_ca, Some(&client_ca))),
    )
    .await;
    let no_cert = TlsFiles {
        ca_pem: Some(server_ca.pem.clone()),
        ..TlsFiles::default()
    };
    let c = cfg(&format!("smtps://127.0.0.1:{port}"), None, no_cert);
    assert!(send(&c, &mail(), DATE, "1@x").await.is_err());
    assert!(h.await.unwrap().starts_with("handshake failed"));
    // 3. A server certificate from another CA: refused by the client.
    let other = Ca::new("other ca");
    let (port, _h) = server(Mode::Implicit, Some(server_tls(&other, None))).await;
    let c = cfg(&format!("smtps://127.0.0.1:{port}"), None, good);
    let e = send(&c, &mail(), DATE, "1@x").await.unwrap_err();
    assert!(e.message.contains("TLS with"), "{e}");
    // 4. The Mozilla roots don't know a private CA either.
    let (port, _h) = server(Mode::Implicit, Some(server_tls(&server_ca, None))).await;
    let c = cfg(
        &format!("smtps://127.0.0.1:{port}"),
        None,
        TlsFiles::default(),
    );
    assert!(send(&c, &mail(), DATE, "1@x").await.is_err());
}
