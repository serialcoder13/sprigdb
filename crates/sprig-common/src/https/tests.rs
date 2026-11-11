use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::Router;
use axum::extract::ConnectInfo;
use axum::routing::get;
use axum::serve::ListenerExt;
use http_body_util::BodyExt;
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};

use super::*;

struct Dir(PathBuf);

impl Dir {
    fn new() -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "sprig-common-https-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
    fn write(&self, name: &str, text: &str) -> PathBuf {
        let p = self.0.join(name);
        std::fs::write(&p, text).unwrap();
        p
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Ca {
    issuer: Issuer<'static, KeyPair>,
    pem: String,
}

impl Ca {
    fn new(name: &str) -> Self {
        let mut p = CertificateParams::new(Vec::<String>::new()).unwrap();
        p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        p.distinguished_name.push(DnType::CommonName, name);
        let key = KeyPair::generate().unwrap();
        let cert = p.self_signed(&key).unwrap();
        Self {
            pem: cert.pem(),
            issuer: Issuer::new(p, key),
        }
    }
    /// (certificate PEM, key PEM) for `names`.
    fn leaf(&self, names: &[&str]) -> (String, String) {
        let p = CertificateParams::new(names.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>())
            .unwrap();
        let key = KeyPair::generate().unwrap();
        let cert = p.signed_by(&key, &self.issuer).unwrap();
        (cert.pem(), key.serialize_pem())
    }
}

/// Serves `GET /peer` (the client's address) over TLS; returns the URL base.
async fn server(cfg: Arc<rustls::ServerConfig>) -> String {
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let l = TlsListener::new(tcp, cfg).unwrap();
    let addr = axum::serve::Listener::local_addr(&l).unwrap();
    let app = Router::new().route(
        "/peer",
        get(|ConnectInfo(p): ConnectInfo<SocketAddr>| async move { p.ip().to_string() }),
    );
    tokio::spawn(async move {
        axum::serve(
            l.tap_io(|_| {}),
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    format!("https://localhost:{}", addr.port())
}

async fn get_peer(
    cfg: &Arc<rustls::ClientConfig>,
    base: &str,
) -> std::result::Result<String, String> {
    let req = Request::get(format!("{base}/peer"))
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = send(cfg, req).await?;
    assert_eq!(resp.status(), 200);
    let b = resp.into_body().collect().await.unwrap().to_bytes();
    Ok(String::from_utf8_lossy(&b).into_owned())
}

fn files(d: &Dir, ca: &Ca, prefix: &str, names: &[&str]) -> (PathBuf, PathBuf) {
    let (c, k) = ca.leaf(names);
    (
        d.write(&format!("{prefix}.pem"), &c),
        d.write(&format!("{prefix}-key.pem"), &k),
    )
}

#[tokio::test]
async fn https_with_the_deployment_ca() {
    let d = Dir::new();
    let ca = Ca::new("deployment CA");
    let ca_file = d.write("ca.pem", &ca.pem);
    let (cert, key) = files(&d, &ca, "controller", &["localhost"]);
    let base = server(server_config(&cert, &key, None).unwrap()).await;

    let client = client_config(&ca_file, None).unwrap();
    assert_eq!(get_peer(&client, &base).await.unwrap(), "127.0.0.1");

    // Another CA's client doesn't trust the server.
    let other = Ca::new("other CA");
    let other_file = d.write("other.pem", &other.pem);
    let e = get_peer(&client_config(&other_file, None).unwrap(), &base)
        .await
        .unwrap_err();
    assert!(e.contains("TLS"), "{e}");

    // A name the certificate doesn't cover.
    let e = get_peer(&client, &base.replace("localhost", "127.0.0.1"))
        .await
        .unwrap_err();
    assert!(e.contains("TLS"), "{e}");
}

#[tokio::test]
async fn a_client_ca_requires_a_client_certificate() {
    let d = Dir::new();
    let ca = Ca::new("deployment CA");
    let ca_file = d.write("ca.pem", &ca.pem);
    let (cert, key) = files(&d, &ca, "controller", &["localhost"]);
    let base = server(server_config(&cert, &key, Some(&ca_file)).unwrap()).await;

    // No client certificate: refused (the error shows on the first read
    // under TLS 1.3, so either step may fail).
    assert!(
        get_peer(&client_config(&ca_file, None).unwrap(), &base)
            .await
            .is_err()
    );

    // One from the deployment CA: accepted.
    let (cc, ck) = files(&d, &ca, "client", &["sprig-client"]);
    let ok = client_config(&ca_file, Some((&cc, &ck))).unwrap();
    assert_eq!(get_peer(&ok, &base).await.unwrap(), "127.0.0.1");

    // One from another CA: refused.
    let other = Ca::new("other CA");
    let (oc, ok_) = files(&d, &other, "stranger", &["sprig-client"]);
    let bad = client_config(&ca_file, Some((&oc, &ok_))).unwrap();
    assert!(get_peer(&bad, &base).await.is_err());
}

#[tokio::test]
async fn a_stalled_handshake_holds_up_no_one() {
    let d = Dir::new();
    let ca = Ca::new("deployment CA");
    let ca_file = d.write("ca.pem", &ca.pem);
    let (cert, key) = files(&d, &ca, "controller", &["localhost"]);
    let base = server(server_config(&cert, &key, None).unwrap()).await;
    let port: u16 = base.rsplit(':').next().unwrap().parse().unwrap();
    // Connects and says nothing.
    let _idle = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let client = client_config(&ca_file, None).unwrap();
    let r = tokio::time::timeout(Duration::from_secs(5), get_peer(&client, &base)).await;
    assert_eq!(r.unwrap().unwrap(), "127.0.0.1");
}

#[test]
fn bad_files_name_themselves() {
    let d = Dir::new();
    let missing = d.0.join("missing.pem");
    let e = server_config(&missing, &missing, None).unwrap_err();
    assert!(e.to_string().contains("admin_tls_cert"), "{e}");
    let empty = d.write("empty.pem", "");
    let e = client_config(&empty, None).unwrap_err();
    assert!(e.to_string().contains("holds no certificate"), "{e}");
    let ca = Ca::new("x");
    let (c, _) = files(&d, &ca, "a", &["localhost"]);
    let (_, k2) = files(&d, &ca, "b", &["localhost"]);
    let e = server_config(&c, &k2, None).unwrap_err();
    assert!(e.to_string().contains("admin_tls_key"), "{e}");
}
