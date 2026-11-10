//! The internal service's mutual TLS: only certificates of the internal CA
//! get in, and the client checks the server's name.

#![allow(clippy::unwrap_used)]

use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair, SanType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

    /// A leaf for 127.0.0.1 and `name`: (chain, key).
    fn leaf(&self, cn: &str, name: &str) -> (String, String) {
        let mut params = CertificateParams::new(vec![name.to_owned()]).unwrap();
        params
            .subject_alt_names
            .push(SanType::IpAddress("127.0.0.1".parse().unwrap()));
        params.distinguished_name.push(DnType::CommonName, cn);
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    fn tls(&self, cn: &str, name: &str) -> InternalTls {
        let (c, k) = self.leaf(cn, name);
        InternalTls::from_pems(self.pem.as_bytes(), c.as_bytes(), k.as_bytes()).unwrap()
    }
}

/// An echo server over mutual TLS; returns its port.
async fn echo(tls: InternalTls) -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let acceptor = tls.acceptor();
    tokio::spawn(async move {
        loop {
            let Ok((s, _)) = l.accept().await else { return };
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                if let Ok(mut t) = acceptor.accept(s).await {
                    let mut b = [0u8; 5];
                    if t.read_exact(&mut b).await.is_ok() {
                        let _ = t.write_all(&b).await;
                        let _ = t.shutdown().await;
                    }
                }
            });
        }
    });
    port
}

async fn round_trip(c: &InternalTls, host: &str, port: u16) -> Result<Vec<u8>> {
    let mut s = c.connect(host, port).await?;
    s.write_all(b"hello")
        .await
        .map_err(|e| SprigError::internal(e.to_string()))?;
    let mut out = Vec::new();
    s.read_to_end(&mut out)
        .await
        .map_err(|e| SprigError::internal(e.to_string()))?;
    Ok(out)
}

#[tokio::test]
async fn agents_of_one_ca_talk_and_nothing_else_does() {
    let ca = Ca::new("SprigDB internal CA");
    let port = echo(ca.tls("agent-src", "agent-src.sprig")).await;
    // Another agent of the CA, dialling the address in the server's SANs.
    let target = ca.tls("agent-dst", "agent-dst.sprig");
    assert_eq!(round_trip(&target, "127.0.0.1", port).await.unwrap(), b"hello");
    // A client certificate from another CA: the server refuses it (the
    // client sees the stream end without the echo, or an error).
    let rogue = Ca::new("someone else");
    let stranger = {
        let (c, k) = rogue.leaf("stranger", "stranger");
        // It trusts our CA, so only the server's check can stop it.
        InternalTls::from_pems(ca.pem.as_bytes(), c.as_bytes(), k.as_bytes()).unwrap()
    };
    let r = round_trip(&stranger, "127.0.0.1", port).await;
    assert!(r.map_or(true, |b| b.is_empty()), "a foreign client got an answer");
    // A server whose certificate doesn't name the address dialled.
    assert!(
        round_trip(&target, "localhost", port).await.is_err(),
        "the server's name wasn't checked"
    );
    // A server from another CA isn't trusted.
    let fake = echo(rogue.tls("fake", "fake")).await;
    assert!(round_trip(&target, "127.0.0.1", fake).await.is_err());
}

#[test]
fn urls_split_into_scheme_host_and_port() {
    assert_eq!(
        split_url("https://10.0.0.7:7401").unwrap(),
        (true, "10.0.0.7".into(), 7401)
    );
    assert_eq!(
        split_url("http://agent-1:7401/internal/v1/backup").unwrap(),
        (false, "agent-1".into(), 7401)
    );
    assert!(split_url("ftp://x:1").is_err());
    assert!(split_url("https://x").is_err());
}

#[test]
fn bad_files_are_config_errors() {
    let e = InternalTls::from_pems(b"", b"", b"").unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ConfigInvalid));
}
