//! Test helpers: certificates from rcgen, a temporary directory, a line
//! echo front end, and TLS client plumbing.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};
use rustls::pki_types::{CertificateDer, ServerName};
use sprig_common::Result;
use sprig_common::time::SystemClock;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::frontend::{ClientConn, Frontend, GatewayContext, InProtocolTls, Protocol};
use crate::listener::{Frontends, ListenerManager, ListenerSpec, Resolution};

/// A test certificate authority.
pub(crate) struct TestCa {
    issuer: Issuer<'static, KeyPair>,
    pub(crate) der: CertificateDer<'static>,
}

/// A leaf certificate and its key, PEM, plus the certificate's DER.
pub(crate) struct Leaf {
    pub(crate) cert_pem: String,
    pub(crate) key_pem: String,
    pub(crate) der: CertificateDer<'static>,
}

impl TestCa {
    pub(crate) fn new() -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, "SprigDB test CA");
        let key = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        Self {
            der: cert.der().clone(),
            issuer: Issuer::new(params, key),
        }
    }

    /// A `localhost` leaf valid until `not_after` (year, month, day).
    pub(crate) fn leaf(&self, cn: &str, not_after: (i32, u8, u8)) -> Leaf {
        let mut params = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
        params.distinguished_name.push(DnType::CommonName, cn);
        params.not_after = rcgen::date_time_ymd(not_after.0, not_after.1, not_after.2);
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        Leaf {
            cert_pem: cert.pem(),
            key_pem: key.serialize_pem(),
            der: cert.der().clone(),
        }
    }
}

/// A directory removed on drop.
pub(crate) struct TempDir(PathBuf);

impl TempDir {
    pub(crate) fn new() -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "sprig-gateway-test-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.0
    }

    /// Writes `leaf` to `cert.pem` and `key.pem`; returns their paths.
    pub(crate) fn write_leaf(&self, leaf: &Leaf) -> (PathBuf, PathBuf) {
        let (c, k) = (self.0.join("cert.pem"), self.0.join("key.pem"));
        std::fs::write(&c, &leaf.cert_pem).unwrap();
        std::fs::write(&k, &leaf.key_pem).unwrap();
        (c, k)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A line echo front end. Server-speaks-first protocols get a greeting
/// line naming the in-protocol TLS offer, and may send `starttls`.
/// Every other line comes back as `<line> peer=<addr> tls=<version|none>
/// gen=<generation>`.
pub(crate) struct Echo(pub(crate) Protocol);

#[async_trait]
impl Frontend for Echo {
    fn protocol(&self) -> Protocol {
        self.0
    }

    async fn serve(&self, mut conn: ClientConn, _ctx: Arc<GatewayContext>) -> Result<()> {
        if !self.0.client_speaks_first() {
            let offer = match conn.in_protocol_tls {
                InProtocolTls::NotOffered => "none",
                InProtocolTls::Allowed => "allowed",
                InProtocolTls::Required => "required",
            };
            let _ = conn
                .stream
                .write_all(format!("hello tls={offer}\n").as_bytes())
                .await;
        }
        while let Some(line) = read_line(&mut conn.stream).await {
            if line == "starttls" {
                conn = conn.start_tls().await?;
                continue;
            }
            let tls = conn.tls_info().map_or("none", |i| i.version);
            let reply = format!(
                "{line} peer={} tls={tls} gen={}\n",
                conn.peer, conn.generation
            );
            if conn.stream.write_all(reply.as_bytes()).await.is_err() {
                break;
            }
        }
        Ok(())
    }
}

/// Reads one `\n`-terminated line, without the newline; `None` at EOF or
/// on error.
pub(crate) async fn read_line(s: &mut (impl AsyncRead + Unpin)) -> Option<String> {
    let mut line = Vec::new();
    loop {
        match s.read_u8().await {
            Ok(b'\n') => return String::from_utf8(line).ok(),
            Ok(b) => line.push(b),
            Err(_) => return None,
        }
    }
}

/// Sends `line` and reads the reply, within 5 s.
pub(crate) async fn roundtrip(
    s: &mut (impl AsyncRead + AsyncWrite + Unpin),
    line: &str,
) -> Option<String> {
    tokio::time::timeout(Duration::from_secs(5), async {
        s.write_all(format!("{line}\n").as_bytes()).await.ok()?;
        read_line(s).await
    })
    .await
    .ok()
    .flatten()
}

/// A manager with echo front ends for every protocol.
pub(crate) fn manager() -> ListenerManager {
    let fes = [
        Protocol::MySql84,
        Protocol::MariaDb,
        Protocol::Mongo,
        Protocol::Redis,
        Protocol::Http,
    ]
    .into_iter()
    .fold(Frontends::new(), |f, p| f.with(Arc::new(Echo(p))));
    ListenerManager::new(fes, Arc::new(GatewayContext::new(Arc::new(SystemClock))))
}

/// A resolution of `specs` with `drain`.
pub(crate) fn resolution(specs: Vec<ListenerSpec>, drain: Duration) -> Resolution {
    Resolution {
        specs,
        rejected: Vec::new(),
        drain,
    }
}

/// A loopback spec on an ephemeral port.
pub(crate) fn spec(name: &'static str, protocol: Protocol) -> ListenerSpec {
    ListenerSpec::new(name, protocol, "127.0.0.1:0".parse().unwrap())
}

/// A TLS client trusting `ca`.
pub(crate) fn connector(ca: &CertificateDer<'static>) -> TlsConnector {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(ca.clone()).unwrap();
    let cfg = rustls::ClientConfig::builder_with_provider(crate::tls::provider())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    TlsConnector::from(Arc::new(cfg))
}

/// Connects with TLS to `addr` as `localhost`.
pub(crate) async fn tls_connect(
    addr: SocketAddr,
    ca: &CertificateDer<'static>,
) -> std::io::Result<TlsStream<TcpStream>> {
    let tcp = TcpStream::connect(addr).await?;
    tls_over(tcp, ca).await
}

/// Runs a client TLS handshake over `tcp`.
pub(crate) async fn tls_over(
    tcp: TcpStream,
    ca: &CertificateDer<'static>,
) -> std::io::Result<TlsStream<TcpStream>> {
    let name = ServerName::try_from("localhost").unwrap();
    tokio::time::timeout(Duration::from_secs(5), connector(ca).connect(name, tcp))
        .await
        .map_err(|_| std::io::Error::other("TLS handshake timed out"))?
}

/// The leaf certificate the server presented.
pub(crate) fn server_cert(s: &TlsStream<TcpStream>) -> CertificateDer<'static> {
    s.get_ref().1.peer_certificates().unwrap()[0].clone()
}

/// Whether the server has closed `s`: a read returns EOF or an error
/// within 5 s.
pub(crate) async fn closed_by_server(s: &mut (impl AsyncRead + Unpin)) -> bool {
    let mut b = [0u8; 64];
    matches!(
        tokio::time::timeout(Duration::from_secs(5), s.read(&mut b)).await,
        Ok(Ok(0) | Err(_))
    )
}
