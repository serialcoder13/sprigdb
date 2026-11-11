//! Approval email over TLS to a real SMTP server (Mailpit), chapter 52.
//!
//! ```sh
//! SPRIG_SMTP_LIVE_URL=smtp+starttls://localhost:24025 \
//! SPRIG_SMTP_LIVE_CA=target/agent-approvals2/pki/ca.pem \
//! SPRIG_SMTP_LIVE_API=http://127.0.0.1:24026 \
//! cargo test -p sprig-approvals --test smtp_live -- --nocapture
//! ```
//!
//! Sends one message with `AUTH PLAIN` and finds it through Mailpit's
//! API; the same server refuses the message when the CA is wrong. Skips
//! without `SPRIG_SMTP_LIVE_URL`.

#![allow(clippy::unwrap_used, clippy::print_stdout)]

use sprig_approvals::smtp::{Mail, SmtpConfig, TlsFiles, send};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// `GET path` on Mailpit's API (plain HTTP on loopback): the body.
async fn api(base: &str, path: &str) -> String {
    let hp = base.trim_start_matches("http://").trim_end_matches('/');
    let mut s = tokio::net::TcpStream::connect(hp).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nhost: {hp}\r\nconnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).await.unwrap();
    let mut out = Vec::new();
    s.read_to_end(&mut out).await.unwrap();
    String::from_utf8_lossy(&out).into_owned()
}

#[tokio::test]
async fn approval_mail_over_tls_reaches_mailpit() {
    let Ok(url) = std::env::var("SPRIG_SMTP_LIVE_URL") else {
        println!("skipped: SPRIG_SMTP_LIVE_URL is not set");
        return;
    };
    let ca = std::fs::read_to_string(std::env::var("SPRIG_SMTP_LIVE_CA").unwrap()).unwrap();
    let api_base = std::env::var("SPRIG_SMTP_LIVE_API").unwrap();
    let subject = format!(
        "[SprigDB] index.create new: shop.orders {}",
        std::process::id()
    );
    let mail = Mail {
        to: vec!["ana@acme.example".into()],
        subject: subject.clone(),
        body: "index (customer_id) saves about 840 ms/h\n.dot line\n".into(),
    };
    let good = SmtpConfig::parse(
        &url,
        "sprigdb@approvals2.example",
        Some(("approvals2".into(), "pw".into())),
        TlsFiles {
            ca_pem: Some(ca),
            ..TlsFiles::default()
        },
    )
    .unwrap()
    .unwrap();
    let t0 = std::time::Instant::now();
    send(
        &good,
        &mail,
        "Tue, 10 Nov 2026 14:03:11 +0000",
        "live.1@sprigdb",
    )
    .await
    .unwrap();
    println!("{url}: sent in {:?}", t0.elapsed());
    let mut found = false;
    for _ in 0..20 {
        let list = api(&api_base, "/api/v1/messages?limit=50").await;
        if list.contains(&subject) {
            found = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    assert!(found, "the message isn't in Mailpit");
    // The Mozilla roots don't know the test CA: refused, nothing sent.
    let other = SmtpConfig::parse(&url, "sprigdb@x", None, TlsFiles::default())
        .unwrap()
        .unwrap();
    let e = send(
        &other,
        &mail,
        "Tue, 10 Nov 2026 14:03:11 +0000",
        "live.2@sprigdb",
    )
    .await
    .unwrap_err();
    println!("{url}: without the CA: {e}");
    assert!(e.message.contains("TLS"), "{e}");
}
