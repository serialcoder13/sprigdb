//! Listener manager tests: TLS per mode, first-byte detection, PROXY,
//! re-binding with drain, certificate rotation, and settings resolution.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use sprig_common::ids::DcId;
use sprig_common::time::SystemClock;
use sprig_settings::{Level, MemSettings, Registry, SettingCtx, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::conn::starts_tls;
use super::*;
use crate::frontend::Protocol;
use crate::testutil::{
    Echo, TempDir, TestCa, closed_by_server, manager, read_line, resolution, roundtrip,
    server_cert, spec, tls_connect, tls_over,
};
use crate::tls::{CertStore, PollOutcome};

/// Long enough that nothing drains during a test.
const NO_DRAIN: Duration = Duration::from_secs(600);
const FAR: (i32, u8, u8) = (2045, 1, 1);

async fn addr(mgr: &ListenerManager, name: &str) -> SocketAddr {
    mgr.local_addr(name).await.expect("listener is bound")
}

/// A spec with TLS `mode` and a fresh `default` bundle signed by `ca`.
fn tls_spec(
    name: &'static str,
    protocol: Protocol,
    mode: TlsMode,
    ca: &TestCa,
    dir: &TempDir,
) -> ListenerSpec {
    let (c, k) = dir.write_leaf(&ca.leaf("gateway", FAR));
    let bundle = CertBundle::load("default", c, k, Arc::new(SystemClock)).unwrap();
    ListenerSpec {
        tls: mode,
        cert: Some(bundle),
        ..spec(name, protocol)
    }
}

/// A port nothing listens on right now.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[test]
fn first_byte_detection() {
    assert!(starts_tls(0x16));
    for b in [b'G', b'P', b'*', 0x00, 0x03, 0x60, b'\r'] {
        assert!(!starts_tls(b), "{b:#x}");
    }
}

#[test]
fn stricter_changes() {
    let base = spec("redis", Protocol::Redis);
    let with = |f: fn(&mut ListenerSpec)| {
        let mut s = base.clone();
        f(&mut s);
        s
    };
    assert!(!base.stricter_than(&base));
    assert!(with(|s| s.tls = TlsMode::Required).stricter_than(&base));
    assert!(!base.stricter_than(&with(|s| s.tls = TlsMode::Optional)));
    assert!(with(|s| s.min_tls = TlsVersion::V1_3).stricter_than(&base));
    assert!(with(|s| s.enabled = false).stricter_than(&base));
    assert!(with(|s| s.bind = "127.0.0.1:1".parse().unwrap()).stricter_than(&base));
    assert!(!with(|s| s.handshake_timeout = Duration::from_secs(1)).stricter_than(&base));
}

#[test]
fn private_addresses() {
    for a in [
        "10.1.2.3",
        "172.16.0.1",
        "192.168.1.1",
        "127.0.0.1",
        "169.254.1.1",
        "::1",
        "fd00::1",
        "fe80::1",
        "::ffff:10.0.0.1",
    ] {
        assert!(is_private(a.parse().unwrap()), "{a}");
    }
    for a in ["0.0.0.0", "8.8.8.8", "::", "2001:db8::1"] {
        assert!(!is_private(a.parse().unwrap()), "{a}");
    }
}

#[tokio::test]
async fn off_serves_plaintext_and_does_not_speak_tls() {
    let mgr = manager();
    let r = mgr
        .apply(resolution(vec![spec("redis", Protocol::Redis)], NO_DRAIN))
        .await;
    assert_eq!(r.applied, ["redis"]);
    let a = addr(&mgr, "redis").await;

    let mut c = TcpStream::connect(a).await.unwrap();
    let reply = roundtrip(&mut c, "ping").await.unwrap();
    assert!(reply.starts_with("ping peer=127.0.0.1:"), "{reply}");
    assert!(reply.ends_with("tls=none gen=1"), "{reply}");

    let ca = TestCa::new();
    assert!(tls_connect(a, &ca.der).await.is_err());
    mgr.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn required_serves_tls_and_refuses_plaintext() {
    let (ca, dir) = (TestCa::new(), TempDir::new());
    let mgr = manager();
    let s = tls_spec("redis", Protocol::Redis, TlsMode::Required, &ca, &dir);
    mgr.apply(resolution(vec![s], NO_DRAIN)).await;
    let a = addr(&mgr, "redis").await;

    let mut t = tls_connect(a, &ca.der).await.unwrap();
    let reply = roundtrip(&mut t, "ping").await.unwrap();
    assert!(reply.ends_with("tls=1.3 gen=1"), "{reply}");

    let mut p = TcpStream::connect(a).await.unwrap();
    p.write_all(b"ping\n").await.unwrap();
    assert!(closed_by_server(&mut p).await);
    mgr.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn required_http_answers_plaintext_with_400() {
    let (ca, dir) = (TestCa::new(), TempDir::new());
    let mgr = manager();
    let s = tls_spec("https", Protocol::Http, TlsMode::Required, &ca, &dir);
    mgr.apply(resolution(vec![s], NO_DRAIN)).await;
    let mut p = TcpStream::connect(addr(&mgr, "https").await).await.unwrap();
    p.write_all(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
        .await
        .unwrap();
    let mut body = String::new();
    tokio::time::timeout(Duration::from_secs(5), p.read_to_string(&mut body))
        .await
        .unwrap()
        .unwrap();
    assert!(body.starts_with("HTTP/1.1 400 "), "{body}");
    mgr.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn optional_detects_tls_from_the_first_byte() {
    let (ca, dir) = (TestCa::new(), TempDir::new());
    let mgr = manager();
    let s = tls_spec("mongo", Protocol::Mongo, TlsMode::Optional, &ca, &dir);
    mgr.apply(resolution(vec![s], NO_DRAIN)).await;
    let a = addr(&mgr, "mongo").await;

    let mut p = TcpStream::connect(a).await.unwrap();
    assert!(
        roundtrip(&mut p, "plain")
            .await
            .unwrap()
            .ends_with("tls=none gen=1")
    );
    let mut t = tls_connect(a, &ca.der).await.unwrap();
    assert!(
        roundtrip(&mut t, "secure")
            .await
            .unwrap()
            .ends_with("tls=1.3 gen=1")
    );
    mgr.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn min_version_1_3_refuses_a_1_2_client() {
    let (ca, dir) = (TestCa::new(), TempDir::new());
    let mgr = manager();
    let s = ListenerSpec {
        min_tls: TlsVersion::V1_3,
        ..tls_spec("redis", Protocol::Redis, TlsMode::Required, &ca, &dir)
    };
    mgr.apply(resolution(vec![s], NO_DRAIN)).await;
    let mut roots = rustls::RootCertStore::empty();
    roots.add(ca.der.clone()).unwrap();
    let cfg = rustls::ClientConfig::builder_with_provider(crate::tls::provider())
        .with_protocol_versions(&[&rustls::version::TLS12])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let tcp = TcpStream::connect(addr(&mgr, "redis").await).await.unwrap();
    let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let r = tokio_rustls::TlsConnector::from(Arc::new(cfg))
        .connect(name, tcp)
        .await;
    assert!(r.is_err());
    mgr.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn mysql_family_gets_an_in_protocol_tls_flag() {
    let (ca, dir) = (TestCa::new(), TempDir::new());
    let mgr = manager();
    let off = spec("mysql", Protocol::MySql84);
    let optional = tls_spec("mariadb", Protocol::MariaDb, TlsMode::Optional, &ca, &dir);
    mgr.apply(resolution(vec![off, optional], NO_DRAIN)).await;

    let mut c = TcpStream::connect(addr(&mgr, "mysql").await).await.unwrap();
    assert_eq!(read_line(&mut c).await.unwrap(), "hello tls=none");
    let mut c = TcpStream::connect(addr(&mgr, "mariadb").await)
        .await
        .unwrap();
    assert_eq!(read_line(&mut c).await.unwrap(), "hello tls=allowed");
    assert!(
        roundtrip(&mut c, "plain")
            .await
            .unwrap()
            .ends_with("tls=none gen=1")
    );

    let required = tls_spec("mysql", Protocol::MySql84, TlsMode::Required, &ca, &dir);
    mgr.apply(resolution(vec![required], NO_DRAIN)).await;
    let mut c = TcpStream::connect(addr(&mgr, "mysql").await).await.unwrap();
    assert_eq!(read_line(&mut c).await.unwrap(), "hello tls=required");
    c.write_all(b"starttls\n").await.unwrap();
    let mut t = tls_over(c, &ca.der).await.unwrap();
    assert!(
        roundtrip(&mut t, "upgraded")
            .await
            .unwrap()
            .ends_with("tls=1.3 gen=2")
    );
    mgr.shutdown(Duration::ZERO).await;
}

fn proxied(mode: ProxyMode, trusted: &str, protocol: Protocol) -> ListenerSpec {
    ListenerSpec {
        proxy: ProxyPolicy {
            mode,
            trusted: ProxyPolicy::parse_cidrs(trusted).unwrap(),
        },
        handshake_timeout: Duration::from_secs(2),
        ..spec(protocol.as_str(), protocol)
    }
}

fn v2_header() -> Vec<u8> {
    let mut h = b"\r\n\r\n\0\r\nQUIT\n".to_vec();
    h.extend([
        0x21, 0x11, 0, 12, 192, 0, 2, 9, 198, 51, 100, 2, 0x1f, 0x90, 0x18, 0xeb,
    ]);
    h
}

#[tokio::test]
async fn proxy_required_from_trusted_peers() {
    let mgr = manager();
    mgr.apply(resolution(
        vec![proxied(ProxyMode::Required, "127.0.0.0/8", Protocol::Redis)],
        NO_DRAIN,
    ))
    .await;
    let a = addr(&mgr, "redis").await;

    let mut c = TcpStream::connect(a).await.unwrap();
    c.write_all(b"PROXY TCP4 192.0.2.1 198.51.100.2 51000 6379\r\n")
        .await
        .unwrap();
    let reply = roundtrip(&mut c, "v1").await.unwrap();
    assert!(reply.starts_with("v1 peer=192.0.2.1:51000 "), "{reply}");

    let mut c = TcpStream::connect(a).await.unwrap();
    c.write_all(&v2_header()).await.unwrap();
    let reply = roundtrip(&mut c, "v2").await.unwrap();
    assert!(reply.starts_with("v2 peer=192.0.2.9:8080 "), "{reply}");

    let mut c = TcpStream::connect(a).await.unwrap();
    c.write_all(b"no header here\n").await.unwrap();
    assert!(closed_by_server(&mut c).await);
    mgr.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn proxy_header_from_an_untrusted_peer_is_left_alone() {
    let mgr = manager();
    mgr.apply(resolution(
        vec![proxied(ProxyMode::Required, "10.0.0.0/8", Protocol::Redis)],
        NO_DRAIN,
    ))
    .await;
    let mut c = TcpStream::connect(addr(&mgr, "redis").await).await.unwrap();
    let reply = roundtrip(&mut c, "PROXY TCP4 192.0.2.1 198.51.100.2 1 2")
        .await
        .unwrap();
    assert!(
        reply.starts_with("PROXY TCP4 192.0.2.1 198.51.100.2 1 2 peer=127.0.0.1:"),
        "{reply}"
    );
    mgr.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn proxy_optional_on_both_kinds_of_listener() {
    let mgr = manager();
    let specs = vec![
        proxied(ProxyMode::Optional, "127.0.0.1", Protocol::Redis),
        proxied(ProxyMode::Optional, "127.0.0.1", Protocol::MySql84),
    ];
    mgr.apply(resolution(specs, NO_DRAIN)).await;

    // Client speaks first: a header is used if present.
    let r = addr(&mgr, "redis").await;
    let mut c = TcpStream::connect(r).await.unwrap();
    assert!(
        roundtrip(&mut c, "direct")
            .await
            .unwrap()
            .starts_with("direct peer=127.0.0.1:")
    );
    let mut c = TcpStream::connect(r).await.unwrap();
    c.write_all(&v2_header()).await.unwrap();
    assert!(
        roundtrip(&mut c, "via")
            .await
            .unwrap()
            .starts_with("via peer=192.0.2.9:8080 ")
    );
    // HTTP methods that start with P are not mistaken for a header.
    let mut c = TcpStream::connect(r).await.unwrap();
    assert!(
        roundtrip(&mut c, "POST /")
            .await
            .unwrap()
            .starts_with("POST / peer=127.0.0.1:")
    );

    // Server speaks first: the greeting follows a short wait for a header.
    let s = addr(&mgr, "mysql").await;
    let mut c = TcpStream::connect(s).await.unwrap();
    assert_eq!(read_line(&mut c).await.unwrap(), "hello tls=none");
    assert!(
        roundtrip(&mut c, "direct")
            .await
            .unwrap()
            .starts_with("direct peer=127.0.0.1:")
    );
    let mut c = TcpStream::connect(s).await.unwrap();
    c.write_all(b"PROXY TCP6 2001:db8::7 2001:db8::1 4000 3306\r\n")
        .await
        .unwrap();
    assert_eq!(read_line(&mut c).await.unwrap(), "hello tls=none");
    assert!(
        roundtrip(&mut c, "via")
            .await
            .unwrap()
            .starts_with("via peer=[2001:db8::7]:4000 ")
    );
    mgr.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn rebinding_moves_the_port_and_drains_old_connections() {
    let mgr = manager();
    let drain = Duration::from_millis(800);
    mgr.apply(resolution(vec![spec("redis", Protocol::Redis)], drain))
        .await;
    let old = addr(&mgr, "redis").await;
    let mut c1 = TcpStream::connect(old).await.unwrap();
    assert!(
        roundtrip(&mut c1, "before")
            .await
            .unwrap()
            .ends_with("gen=1")
    );

    let moved = ListenerSpec {
        bind: SocketAddr::from(([127, 0, 0, 1], free_port())),
        ..spec("redis", Protocol::Redis)
    };
    let r = mgr.apply(resolution(vec![moved.clone()], drain)).await;
    assert_eq!(r.applied, ["redis"]);
    assert_eq!(addr(&mgr, "redis").await, moved.bind);
    assert_eq!(mgr.generation("redis").await, 2);

    // The old socket is closed; the old connection is still served.
    assert!(TcpStream::connect(old).await.is_err());
    assert!(
        roundtrip(&mut c1, "during")
            .await
            .unwrap()
            .ends_with("gen=1")
    );
    let mut c2 = TcpStream::connect(moved.bind).await.unwrap();
    assert!(roundtrip(&mut c2, "new").await.unwrap().ends_with("gen=2"));

    // After listener.drain_seconds the old generation is closed.
    assert!(closed_by_server(&mut c1).await);
    assert!(
        roundtrip(&mut c2, "still")
            .await
            .unwrap()
            .ends_with("gen=2")
    );

    // Re-applying the same spec changes nothing.
    let r = mgr.apply(resolution(vec![moved], drain)).await;
    assert!(r.applied.is_empty() && r.rejected.is_empty());
    assert_eq!(mgr.generation("redis").await, 2);
    mgr.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn looser_changes_keep_connections_and_stricter_ones_drain_every_older_generation() {
    let (ca, dir) = (TestCa::new(), TempDir::new());
    let mgr = manager();
    let drain = Duration::from_millis(300);
    let optional = tls_spec("mongo", Protocol::Mongo, TlsMode::Optional, &ca, &dir);
    mgr.apply(resolution(vec![optional.clone()], drain)).await;
    let a = addr(&mgr, "mongo").await;
    let mut c1 = TcpStream::connect(a).await.unwrap();
    assert!(roundtrip(&mut c1, "one").await.unwrap().ends_with("gen=1"));

    // Looser: off. The socket stays; c1 is not drained.
    let off = ListenerSpec {
        tls: TlsMode::Off,
        ..optional.clone()
    };
    mgr.apply(resolution(vec![off], drain)).await;
    assert_eq!(addr(&mgr, "mongo").await, a);
    tokio::time::sleep(drain * 2).await;
    assert!(roundtrip(&mut c1, "two").await.unwrap().ends_with("gen=1"));

    // Stricter than both older generations: required. c1 closes after the drain.
    let required = ListenerSpec {
        tls: TlsMode::Required,
        ..optional
    };
    mgr.apply(resolution(vec![required], drain)).await;
    assert!(
        roundtrip(&mut c1, "three")
            .await
            .unwrap()
            .ends_with("gen=1")
    );
    assert!(closed_by_server(&mut c1).await);
    let mut t = tls_connect(a, &ca.der).await.unwrap();
    assert!(
        roundtrip(&mut t, "four")
            .await
            .unwrap()
            .ends_with("tls=1.3 gen=3")
    );
    mgr.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn a_failed_bind_keeps_the_old_listener() {
    let mgr = manager();
    mgr.apply(resolution(vec![spec("redis", Protocol::Redis)], NO_DRAIN))
        .await;
    let a = addr(&mgr, "redis").await;
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let busy = ListenerSpec {
        bind: taken.local_addr().unwrap(),
        ..spec("redis", Protocol::Redis)
    };
    let r = mgr.apply(resolution(vec![busy], NO_DRAIN)).await;
    assert!(r.applied.is_empty());
    assert_eq!(r.rejected.len(), 1);
    assert!(
        r.rejected[0].reason.contains("can't bind"),
        "{:?}",
        r.rejected
    );
    assert_eq!(addr(&mgr, "redis").await, a);
    assert_eq!(mgr.generation("redis").await, 1);
    let mut c = TcpStream::connect(a).await.unwrap();
    assert!(roundtrip(&mut c, "ok").await.is_some());
    mgr.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn disabling_closes_the_socket_and_drains() {
    let mgr = manager();
    let drain = Duration::from_millis(200);
    mgr.apply(resolution(vec![spec("redis", Protocol::Redis)], drain))
        .await;
    let a = addr(&mgr, "redis").await;
    let mut c = TcpStream::connect(a).await.unwrap();
    assert!(roundtrip(&mut c, "hi").await.is_some());
    let off = ListenerSpec {
        enabled: false,
        ..spec("redis", Protocol::Redis)
    };
    assert_eq!(
        mgr.apply(resolution(vec![off], drain)).await.applied,
        ["redis"]
    );
    assert!(mgr.local_addr("redis").await.is_none());
    assert!(TcpStream::connect(a).await.is_err());
    assert!(closed_by_server(&mut c).await);
    mgr.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn missing_front_end_or_certificate_is_rejected() {
    let mgr = ListenerManager::new(
        Frontends::new().with(Arc::new(Echo(Protocol::Redis))),
        Arc::new(crate::frontend::GatewayContext::new(Arc::new(SystemClock))),
    );
    let no_cert = ListenerSpec {
        tls: TlsMode::Required,
        ..spec("redis", Protocol::Redis)
    };
    let r = mgr
        .apply(resolution(
            vec![spec("bolt", Protocol::Bolt), no_cert],
            NO_DRAIN,
        ))
        .await;
    assert!(r.applied.is_empty());
    let reasons: Vec<_> = r
        .rejected
        .iter()
        .map(|r| (r.name, r.reason.as_str()))
        .collect();
    assert_eq!(
        reasons,
        [
            ("bolt", "no bolt front end in this build yet"),
            ("redis", "TLS mode needs a certificate bundle")
        ]
    );
    mgr.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn certificate_rotation_reaches_new_handshakes_only() {
    let (ca, dir) = (TestCa::new(), TempDir::new());
    let certs = CertStore::new(Arc::new(SystemClock));
    let first = ca.leaf("first", FAR);
    let (c, k) = dir.write_leaf(&first);
    let bundle = certs.bundle("default", &c, &k).unwrap();
    let mgr = manager();
    let s = ListenerSpec {
        tls: TlsMode::Required,
        cert: Some(bundle),
        ..spec("redis", Protocol::Redis)
    };
    mgr.apply(resolution(vec![s], NO_DRAIN)).await;
    let a = addr(&mgr, "redis").await;
    let mut old = tls_connect(a, &ca.der).await.unwrap();
    assert_eq!(server_cert(&old), first.der);

    let second = ca.leaf("second", FAR);
    dir.write_leaf(&second);
    assert!(matches!(certs.poll()[0].1, PollOutcome::Pending));
    assert!(matches!(certs.poll()[0].1, PollOutcome::Rotated));
    let new = tls_connect(a, &ca.der).await.unwrap();
    assert_eq!(server_cert(&new), second.der);
    assert_eq!(
        mgr.generation("redis").await,
        1,
        "rotation bumps no generation"
    );

    // The old connection keeps its session and its certificate.
    assert!(
        roundtrip(&mut old, "still here")
            .await
            .unwrap()
            .ends_with("tls=1.3 gen=1")
    );
    assert_eq!(server_cert(&old), first.der);

    // A bad pair (the new certificate with the old key) is refused.
    std::fs::write(&c, ca.leaf("third", FAR).cert_pem).unwrap();
    certs.poll();
    assert!(matches!(certs.poll()[0].1, PollOutcome::Rejected(_)));
    let after = tls_connect(a, &ca.der).await.unwrap();
    assert_eq!(server_cert(&after), second.der);
    mgr.shutdown(Duration::ZERO).await;
}

#[tokio::test]
async fn shutdown_closes_listeners_then_connections() {
    let mgr = manager();
    mgr.apply(resolution(vec![spec("redis", Protocol::Redis)], NO_DRAIN))
        .await;
    let a = addr(&mgr, "redis").await;
    let mut c = TcpStream::connect(a).await.unwrap();
    assert!(roundtrip(&mut c, "hi").await.is_some());
    mgr.shutdown(Duration::from_millis(100)).await;
    assert!(TcpStream::connect(a).await.is_err());
    assert!(closed_by_server(&mut c).await);
}

#[test]
fn the_registry_accepts_every_gateway_key() {
    let reg = Registry::global().unwrap();
    for keys in &LISTENERS {
        for key in [keys.bind.key, keys.enabled.key, keys.tls.key, keys.cert.key] {
            assert!(reg.get(key).is_some(), "{key}");
        }
    }
    assert!(reg.get("listener.drain_seconds").is_some());
    assert!(reg.get("tls.bundles.default.cert_path").is_some());
}

#[test]
fn specs_resolve_from_settings() {
    let (ca, dir) = (TestCa::new(), TempDir::new());
    let (c, k) = dir.write_leaf(&ca.leaf("gateway", FAR));
    let reg = Registry::global().unwrap();
    let mut settings = MemSettings::new(reg);
    let dc: DcId = "mum1".parse().unwrap();
    let ctx = SettingCtx::process(&dc);
    let certs = CertStore::new(Arc::new(SystemClock));

    // Defaults: the R0 listeners that need TLS have no certificate yet.
    let res = resolve(&settings, &ctx, &certs);
    let names = |v: &[ListenerSpec]| v.iter().map(|s| s.name).collect::<Vec<_>>();
    assert_eq!(
        names(&res.specs),
        ["http", "mongo", "redis", "bolt", "kafka"]
    );
    let rejected: Vec<_> = res.rejected.iter().map(|r| r.name).collect();
    assert_eq!(rejected, ["mysql", "mariadb", "https"]);
    assert_eq!(res.drain, Duration::from_secs(600));
    let http = &res.specs[0];
    assert_eq!(http.bind, "0.0.0.0:8080".parse().unwrap());
    assert_eq!((http.enabled, http.tls), (true, TlsMode::Off));
    assert!(!res.specs[2].enabled);

    let mut set = |key: &str, v: Value| settings.set_value(Level::Global, &ctx, key, &v).unwrap();
    set(
        "tls.bundles.default.cert_path",
        Value::Str(c.display().to_string()),
    );
    set(
        "tls.bundles.default.key_path",
        Value::Str(k.display().to_string()),
    );
    set("tls.min_version", "1.3".into());
    set("listener.redis.enabled", Value::Bool(true));
    set("listener.redis.bind", "127.0.0.1:16379".into());
    set("listener.redis.tls", "optional".into());
    set("listener.mongo.bind", "nonsense".into());
    set("gateway.proxy_protocol", "required".into());
    set(
        "gateway.proxy_trusted_cidrs",
        "10.0.0.0/8, 192.168.0.0/16".into(),
    );
    set("listener.drain_seconds", Value::Int(5));
    let res = resolve(&settings, &ctx, &certs);
    assert_eq!(
        names(&res.specs),
        [
            "mysql", "mariadb", "http", "https", "redis", "bolt", "kafka"
        ]
    );
    assert_eq!(res.rejected.len(), 1);
    assert_eq!(res.rejected[0].name, "mongo");
    assert_eq!(res.drain, Duration::from_secs(5));
    let redis = res.specs.iter().find(|s| s.name == "redis").unwrap();
    assert_eq!(redis.bind, "127.0.0.1:16379".parse().unwrap());
    assert_eq!(
        (redis.enabled, redis.tls, redis.min_tls),
        (true, TlsMode::Optional, TlsVersion::V1_3)
    );
    assert_eq!(redis.proxy.mode, ProxyMode::Required);
    assert_eq!(redis.proxy.trusted.len(), 2);
    let mysql = &res.specs[0];
    assert!(Arc::ptr_eq(
        mysql.cert.as_ref().unwrap(),
        redis.cert.as_ref().unwrap()
    ));

    // A bad CIDR list leaves every listener as it is.
    settings
        .set_value(
            Level::Global,
            &ctx,
            "gateway.proxy_trusted_cidrs",
            &"10.0.0.0/99".into(),
        )
        .unwrap();
    let res = resolve(&settings, &ctx, &certs);
    assert!(res.specs.is_empty());
    assert_eq!(res.rejected.len(), LISTENERS.len());
}
