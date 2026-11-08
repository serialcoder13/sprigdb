//! End-to-end tests: the MySQL and MariaDB front ends on ephemeral ports,
//! behind the real listener manager, with an accounts file and the canned
//! executor, driven by `mysql_async` (both personas, every TLS mode,
//! wrong passwords, the decoy, compression, `KILL`, change user, reset)
//! and by a raw protocol client for the commands `mysql_async` doesn't
//! send.

// Scenarios read top to bottom.
#![allow(clippy::too_many_lines, clippy::many_single_char_names)]

use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{SinkExt as _, StreamExt as _};
use mysql_async::prelude::*;
use mysql_async::{ChangeUserOpts, Conn, OptsBuilder, SslOpts};
use sprig_auth::verifier::{CachingSha2Verifier, NativeVerifier, native_scramble};
use sprig_auth::{FastCache, FileAccounts};
use sprig_common::time::SystemClock;
use sprig_proto_mysql::command::com;
use sprig_proto_mysql::packets::{HandshakeResponse, HandshakeV10, OkPacket, SslRequest};
use sprig_proto_mysql::{Caps, ErrPacket, Packet, PacketCodec, Persona};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use super::listener_hooks::{ListenerHooks, Strictness};
use super::login::LoginDeps;
use super::settings::{FrontendSettings, SettingsSource};
use super::{MySqlConfig, MySqlFrontend};
use crate::frontend::{GatewayContext, Protocol};
use crate::listener::{Frontends, ListenerManager, ListenerSpec, TlsMode};
use crate::pipeline::canned::CannedExecutor;
use crate::testutil::{TempDir, TestCa, resolution, spec};
use crate::tls::CertStore;

const PW: &str = "sprig-test-pw";
const OTHER_PW: &str = "other-pw";

fn hex(b: &[u8]) -> String {
    b.iter().fold(String::from("0x"), |mut s, x| {
        let _ = write!(s, "{x:02X}");
        s
    })
}

/// The accounts file: `app` (both verifiers), `other`, and `nat` (native
/// only).
fn accounts_toml() -> String {
    let sha2 = |pw: &str| {
        hex(&CachingSha2Verifier::create(pw.as_bytes(), 1000)
            .unwrap()
            .to_bytes())
    };
    let native = NativeVerifier::create(PW.as_bytes()).to_string();
    let mut s = String::new();
    for (user, pw, with_sha2, with_native) in [
        ("app", PW, true, true),
        ("other", OTHER_PW, true, false),
        ("nat", PW, false, true),
    ] {
        let _ = writeln!(s, "[[account]]\nuser = \"{user}\"");
        if with_sha2 {
            let _ = writeln!(s, "caching_sha2_password = \"{}\"", sha2(pw));
        }
        if with_native {
            let _ = writeln!(s, "mysql_native_password = \"{native}\"");
        }
        s.push_str(
            "cluster = \"dc-a-c1\"\nbackend_user = \"root\"\nbackend_credentials = \"env:SPRIG_TEST_BACKEND\"\n\n",
        );
    }
    s
}

struct Gateway {
    mgr: ListenerManager,
    ctx: Arc<GatewayContext>,
    hooks: ListenerHooks,
    mysql: SocketAddr,
    mariadb: SocketAddr,
    specs: Vec<ListenerSpec>,
    _dir: TempDir,
}

impl Gateway {
    async fn start(tls: TlsMode) -> Self {
        let dir = TempDir::new();
        let path = dir.path().join("accounts.toml");
        std::fs::write(&path, accounts_toml()).unwrap();
        let accounts = FileAccounts::open(&path).unwrap();
        let clock = Arc::new(SystemClock);
        let login = Arc::new(LoginDeps::new(
            sprig_auth::LoginSource::File(accounts),
            Arc::new(FastCache::new(
                clock.clone(),
                sprig_auth::fast_cache::DEFAULT_TTL,
            )),
        ));
        let hooks = ListenerHooks::new();
        let fe = |persona| {
            Arc::new(MySqlFrontend::new(MySqlConfig {
                persona,
                settings: SettingsSource::Fixed(FrontendSettings::default()),
                login: Arc::clone(&login),
                executor: Arc::new(CannedExecutor),
                hooks: hooks.clone(),
                hostname: "gw-test".to_owned(),
            }))
        };
        let ctx = Arc::new(GatewayContext::new(clock.clone()));
        let mgr = ListenerManager::new(
            Frontends::new()
                .with(fe(Persona::MySql84))
                .with(fe(Persona::MariaDb)),
            Arc::clone(&ctx),
        );
        let ca = TestCa::new();
        let (cert, key) = dir.write_leaf(&ca.leaf("gw", (2045, 1, 1)));
        let certs = CertStore::new(clock);
        let bundle = certs.bundle("default", &cert, &key).unwrap();
        let specs: Vec<ListenerSpec> =
            [("mysql", Protocol::MySql84), ("mariadb", Protocol::MariaDb)]
                .into_iter()
                .map(|(name, p)| {
                    let mut s = spec(name, p);
                    s.tls = tls;
                    s.cert = (tls != TlsMode::Off).then(|| bundle.clone());
                    s
                })
                .collect();
        let report = mgr
            .apply(resolution(specs.clone(), Duration::from_secs(60)))
            .await;
        assert!(report.rejected.is_empty(), "{:?}", report.rejected);
        let mysql = mgr.local_addr("mysql").await.unwrap();
        let mariadb = mgr.local_addr("mariadb").await.unwrap();
        Self {
            mgr,
            ctx,
            hooks,
            mysql,
            mariadb,
            specs,
            _dir: dir,
        }
    }

    fn addr(&self, persona: Persona) -> SocketAddr {
        match persona {
            Persona::MySql84 => self.mysql,
            Persona::MariaDb => self.mariadb,
        }
    }

    async fn stop(self) {
        self.mgr.shutdown(Duration::from_secs(1)).await;
    }
}

fn opts(addr: SocketAddr, user: &str, pw: &str, tls: bool) -> OptsBuilder {
    let o = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(addr.port())
        .user(Some(user))
        .pass(Some(pw))
        .prefer_socket(false);
    if tls {
        o.ssl_opts(
            SslOpts::default()
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true),
        )
    } else {
        o
    }
}

async fn connect(o: OptsBuilder) -> mysql_async::Result<Conn> {
    tokio::time::timeout(Duration::from_secs(10), Conn::new(o))
        .await
        .expect("connect timed out")
}

fn server_code(e: &mysql_async::Error) -> Option<u16> {
    match e {
        mysql_async::Error::Server(s) => Some(s.code),
        _ => None,
    }
}

#[tokio::test]
async fn both_personas_over_tls() {
    let gw = Gateway::start(TlsMode::Required).await;
    for persona in [Persona::MySql84, Persona::MariaDb] {
        let mut c = connect(opts(gw.addr(persona), "app", PW, true))
            .await
            .unwrap_or_else(|e| panic!("{persona:?}: {e}"));
        assert_ne!(c.id(), 0);
        let one: Option<i64> = c.query_first("SELECT 1").await.unwrap();
        assert_eq!(one, Some(1));
        let comment: Option<String> = c
            .query_first("select @@version_comment limit 1")
            .await
            .unwrap();
        assert_eq!(
            comment.as_deref(),
            Some(concat!("SprigDB ", env!("CARGO_PKG_VERSION")))
        );
        let version: Option<String> = c.query_first("SELECT VERSION()").await.unwrap();
        assert_eq!(version.as_deref(), Some(persona.default_version()));
        let (v, server) = (version.unwrap(), c.server_version());
        assert_eq!(v.contains("MariaDB"), persona == Persona::MariaDb);
        assert_eq!(server.0, if persona == Persona::MySql84 { 8 } else { 11 });
        let hp: Option<(String, u64)> = c
            .query_first("SELECT @@hostname, @@max_allowed_packet")
            .await
            .unwrap();
        assert_eq!(hp, Some(("gw-test".to_owned(), 64 << 20)));
        // Prepared statements through the executor's boundary.
        let echoed: Option<String> = c.exec_first("SELECT ?", (42,)).await.unwrap();
        assert_eq!(echoed.as_deref(), Some("42"));
        let echoed: Option<String> = c.exec_first("SELECT ?", ("hi",)).await.unwrap();
        assert_eq!(echoed.as_deref(), Some("hi"));
        let seven: Option<i64> = c.exec_first("SELECT 7", ()).await.unwrap();
        assert_eq!(seven, Some(7));
        // Multi-statements: two resultsets.
        let mut r = c.query_iter("SELECT 1; SELECT 2").await.unwrap();
        let a: Vec<i64> = r.collect().await.unwrap();
        let b: Vec<i64> = r.collect().await.unwrap();
        assert_eq!((a, b), (vec![1], vec![2]));
        drop(r);
        // An executor error is an ERR; the session goes on.
        let e = c.query_drop("SELECT nonsense FROM t").await.unwrap_err();
        assert_eq!(server_code(&e), Some(1064));
        c.ping().await.unwrap();
        assert!(c.reset().await.unwrap());
        let one: Option<i64> = c.query_first("SELECT 1").await.unwrap();
        assert_eq!(one, Some(1));
        c.disconnect().await.unwrap();
    }
    gw.stop().await;
}

#[tokio::test]
async fn required_listener_refuses_plaintext_with_3159() {
    let gw = Gateway::start(TlsMode::Required).await;
    for persona in [Persona::MySql84, Persona::MariaDb] {
        let e = connect(opts(gw.addr(persona), "app", PW, false))
            .await
            .unwrap_err();
        assert_eq!(server_code(&e), Some(3159), "{e}");
    }
    gw.stop().await;
}

#[tokio::test]
async fn off_listener_offers_no_tls_and_needs_tls_for_a_full_login() {
    let gw = Gateway::start(TlsMode::Off).await;
    // A client that requires TLS fails on its side.
    let e = connect(opts(gw.mysql, "app", PW, true)).await.unwrap_err();
    assert_eq!(server_code(&e), None, "{e}");
    // A cold caching_sha2 login over plaintext needs the RSA path (D4).
    let e = connect(opts(gw.mysql, "app", PW, false)).await.unwrap_err();
    assert_eq!(server_code(&e), Some(1045), "{e}");
    assert!(e.to_string().contains("connect with TLS"), "{e}");
    // mysql_native_password works in plaintext (switch from the greeting's
    // plugin).
    for persona in [Persona::MySql84, Persona::MariaDb] {
        let mut c = connect(opts(gw.addr(persona), "nat", PW, false))
            .await
            .unwrap_or_else(|e| panic!("{persona:?}: {e}"));
        let one: Option<i64> = c.query_first("SELECT 1").await.unwrap();
        assert_eq!(one, Some(1));
        c.disconnect().await.unwrap();
    }
    gw.stop().await;
}

#[tokio::test]
async fn optional_listener_fast_path_after_a_tls_login() {
    let gw = Gateway::start(TlsMode::Optional).await;
    // Plaintext first: the fast cache is cold.
    let e = connect(opts(gw.mysql, "app", PW, false)).await.unwrap_err();
    assert_eq!(server_code(&e), Some(1045));
    // A TLS login takes the full path and fills the fast cache ...
    connect(opts(gw.mysql, "app", PW, true))
        .await
        .unwrap()
        .disconnect()
        .await
        .unwrap();
    // ... so plaintext now takes the fast path, on both personas.
    for persona in [Persona::MySql84, Persona::MariaDb] {
        let mut c = connect(opts(gw.addr(persona), "app", PW, false))
            .await
            .unwrap_or_else(|e| panic!("{persona:?}: {e}"));
        let u: Option<String> = c.query_first("SELECT CURRENT_USER()").await.unwrap();
        assert_eq!(u.as_deref(), Some("app@%"));
        c.disconnect().await.unwrap();
    }
    // A wrong password misses the fast path and fails the full one.
    let e = connect(opts(gw.mysql, "app", "nope", true))
        .await
        .unwrap_err();
    assert_eq!(server_code(&e), Some(1045));
    gw.stop().await;
}

#[tokio::test]
async fn wrong_password_and_unknown_user_look_alike() {
    let gw = Gateway::start(TlsMode::Required).await;
    let t = Instant::now();
    let wrong = connect(opts(gw.mysql, "app", "nope", true))
        .await
        .unwrap_err();
    let t_wrong = t.elapsed();
    let t = Instant::now();
    let ghost = connect(opts(gw.mysql, "ghost", "nope", true))
        .await
        .unwrap_err();
    let t_ghost = t.elapsed();
    assert_eq!(server_code(&wrong), Some(1045));
    assert_eq!(server_code(&ghost), Some(1045));
    let (mysql_async::Error::Server(w), mysql_async::Error::Server(g)) = (&wrong, &ghost) else {
        unreachable!()
    };
    assert_eq!(w.message.replace("'app'", "'ghost'"), g.message);
    assert_eq!(w.state, g.state);
    // Both did a full exchange with a hash; neither is instant.
    tracing::debug!(?t_wrong, ?t_ghost, "login failure times");
    gw.stop().await;
}

#[tokio::test]
async fn compression_zlib() {
    let gw = Gateway::start(TlsMode::Optional).await;
    for (persona, tls) in [
        (Persona::MySql84, true),
        (Persona::MariaDb, true),
        (Persona::MySql84, false),
    ] {
        let user = if tls { "app" } else { "nat" };
        let mut c = connect(
            opts(gw.addr(persona), user, PW, tls).compression(mysql_async::Compression::default()),
        )
        .await
        .unwrap_or_else(|e| panic!("{persona:?} tls={tls}: {e}"));
        let big: Option<String> = c.query_first("SELECT REPEAT('x', 300000)").await.unwrap();
        assert_eq!(big.map(|s| s.len()), Some(300_000));
        let one: Option<i64> = c.exec_first("SELECT ?", (1,)).await.unwrap();
        assert_eq!(one, Some(1));
        c.disconnect().await.unwrap();
    }
    gw.stop().await;
}

#[tokio::test]
async fn kill_query_and_connection() {
    let gw = Gateway::start(TlsMode::Required).await;
    let mut a = connect(opts(gw.mysql, "app", PW, true)).await.unwrap();
    let mut b = connect(opts(gw.mariadb, "app", PW, true)).await.unwrap();
    let a_id = a.id();

    // KILL QUERY: the sleeping statement fails with 1317; the session stays.
    let sleeper = tokio::spawn(async move {
        let t = Instant::now();
        let r = a.query_drop("SELECT SLEEP(30)").await;
        (a, r, t.elapsed())
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let rows: Vec<(u32, String, String, String)> = b
        .query_map(
            "SHOW PROCESSLIST",
            |(id, user, _host, _db, cmd, _time, _state, info): (
                u32,
                String,
                String,
                Option<String>,
                String,
                u64,
                String,
                Option<String>,
            )| { (id, user, cmd, info.unwrap_or_default()) },
        )
        .await
        .unwrap();
    assert!(
        rows.iter()
            .any(|r| r.0 == a_id && r.1 == "app" && r.2 == "Query" && r.3.contains("SLEEP")),
        "{rows:?}"
    );
    b.query_drop(format!("KILL QUERY {a_id}")).await.unwrap();
    let (mut a, r, took) = sleeper.await.unwrap();
    assert_eq!(server_code(&r.unwrap_err()), Some(1317));
    assert!(took < Duration::from_secs(10));
    let one: Option<i64> = a.query_first("SELECT 1").await.unwrap();
    assert_eq!(one, Some(1));

    // Another user can't kill it; unknown ids are 1094.
    let mut o = connect(opts(gw.mysql, "other", OTHER_PW, true))
        .await
        .unwrap();
    let e = o.query_drop(format!("KILL {a_id}")).await.unwrap_err();
    assert_eq!(server_code(&e), Some(1095));
    let e = o.query_drop("KILL 4000000").await.unwrap_err();
    assert_eq!(server_code(&e), Some(1094));

    // KILL CONNECTION: a's connection closes.
    b.query_drop(format!("KILL CONNECTION {a_id}"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(a.query_drop("SELECT 1").await.is_err());
    assert!(
        gw.ctx
            .sessions
            .get(sprig_common::ids::ConnId(a_id))
            .is_none()
    );
    b.disconnect().await.unwrap();
    o.disconnect().await.unwrap();
    gw.stop().await;
}

#[tokio::test]
async fn change_user_and_default_database() {
    let gw = Gateway::start(TlsMode::Required).await;
    let mut c = connect(opts(gw.mysql, "app", PW, true).db_name(Some("shop")))
        .await
        .unwrap();
    let db: Option<String> = c.query_first("SELECT DATABASE()").await.unwrap();
    assert_eq!(db.as_deref(), Some("shop"));
    c.change_user(
        ChangeUserOpts::default()
            .with_user(Some("other".to_owned()))
            .with_pass(Some(OTHER_PW.to_owned())),
    )
    .await
    .unwrap();
    let u: Option<String> = c.query_first("SELECT CURRENT_USER()").await.unwrap();
    assert_eq!(u.as_deref(), Some("other@%"));
    // A failed change user ends the connection.
    let e = c
        .change_user(
            ChangeUserOpts::default()
                .with_user(Some("app".to_owned()))
                .with_pass(Some("nope".to_owned())),
        )
        .await
        .unwrap_err();
    assert_eq!(server_code(&e), Some(1045));
    // Unknown database at login: the executor's 1049.
    let e = connect(opts(gw.mysql, "app", PW, true).db_name(Some("missing")))
        .await
        .unwrap_err();
    assert_eq!(server_code(&e), Some(1049));
    gw.stop().await;
}

#[tokio::test]
async fn safe_point_close_after_a_stricter_listener() {
    let gw = Gateway::start(TlsMode::Optional).await;
    let mut idle = connect(opts(gw.mysql, "nat", PW, false)).await.unwrap();
    let mut tls = connect(opts(gw.mysql, "app", PW, true)).await.unwrap();
    let mut trx = connect(opts(gw.mysql, "nat", PW, false)).await.unwrap();
    trx.query_drop("BEGIN").await.unwrap();
    let mut other = connect(opts(gw.mariadb, "nat", PW, false)).await.unwrap();

    // mysql moves from optional to required.
    let mut stricter = gw.specs[0].clone();
    stricter.tls = TlsMode::Required;
    assert_eq!(
        gw.hooks
            .listener_changed("mysql", Strictness::of(&stricter), Duration::ZERO),
        3,
        "every session accepted under the optional spec is scheduled"
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    let e = idle.query_drop("SELECT 1").await.unwrap_err();
    assert_eq!(server_code(&e), Some(39004), "{e}");
    let e = tls.query_drop("SELECT 1").await.unwrap_err();
    assert_eq!(server_code(&e), Some(39004));
    // Inside a transaction: served until it ends.
    trx.query_drop("SELECT 1").await.unwrap();
    trx.query_drop("COMMIT").await.unwrap();
    let e = trx.query_drop("SELECT 1").await.unwrap_err();
    assert_eq!(server_code(&e), Some(39004));
    // Another listener's sessions are untouched.
    other.query_drop("SELECT 1").await.unwrap();
    other.disconnect().await.unwrap();
    gw.stop().await;
}

// ----- raw protocol ---------------------------------------------------------

type Raw = Framed<TcpStream, PacketCodec>;

async fn next(raw: &mut Raw) -> Bytes {
    tokio::time::timeout(Duration::from_secs(5), raw.next())
        .await
        .expect("reply in time")
        .expect("open")
        .expect("valid packet")
        .payload
}

fn client_caps() -> Caps {
    Caps::server_default(Persona::MySql84)
        - Caps::COMPRESS
        - Caps::ZSTD_COMPRESSION_ALGORITHM
        - Caps::QUERY_ATTRIBUTES
}

/// Logs in as `nat` with `mysql_native_password` over plaintext.
async fn raw_login(addr: SocketAddr) -> (Raw, HandshakeV10) {
    let mut raw = Framed::new(
        TcpStream::connect(addr).await.unwrap(),
        PacketCodec::default(),
    );
    let hs = HandshakeV10::decode(&next(&mut raw).await).unwrap();
    let resp = HandshakeResponse {
        caps: client_caps(),
        max_packet: 1 << 24,
        collation: 255,
        user: b"nat".to_vec(),
        auth_response: native_scramble(PW.as_bytes(), &hs.auth_data).into(),
        plugin: Some("mysql_native_password".to_owned()),
        ..HandshakeResponse::default()
    };
    raw.send(Packet::new(1, resp.encode())).await.unwrap();
    let ok = next(&mut raw).await;
    assert_eq!(ok[0], 0, "login OK: {ok:?}");
    (raw, hs)
}

async fn command(raw: &mut Raw, payload: &[u8]) -> Bytes {
    raw.codec_mut().reset_seq();
    raw.send(Bytes::copy_from_slice(payload)).await.unwrap();
    next(raw).await
}

fn err_code(p: &[u8]) -> Option<u16> {
    ErrPacket::decode(p).ok().map(|e| e.code)
}

#[tokio::test]
async fn raw_commands() {
    let gw = Gateway::start(TlsMode::Off).await;
    let (mut raw, hs) = raw_login(gw.mysql).await;
    assert!(!hs.caps.contains(Caps::SSL));
    assert_eq!(hs.collation, 255);
    assert_eq!(hs.auth_plugin, "caching_sha2_password");
    assert_eq!(hs.auth_data.len(), 20);

    assert_eq!(command(&mut raw, &[com::PING]).await[0], 0);
    let stats = command(&mut raw, &[com::STATISTICS]).await;
    assert!(
        String::from_utf8_lossy(&stats).starts_with("Uptime: "),
        "{stats:?}"
    );
    assert_eq!(
        err_code(&command(&mut raw, &[com::SHUTDOWN]).await),
        Some(1227)
    );
    assert_eq!(
        err_code(&command(&mut raw, &[com::BINLOG_DUMP, 0, 0]).await),
        Some(1227)
    );
    assert_eq!(err_code(&command(&mut raw, &[0x20]).await), Some(1047));
    // COM_SET_OPTION answers with the EOF form (OK 0xFE under DEPRECATE_EOF).
    assert_eq!(command(&mut raw, &[com::SET_OPTION, 1, 0]).await[0], 0xFE);
    // A malformed command gets 1835 and the session goes on.
    assert_eq!(
        err_code(&command(&mut raw, &[com::STMT_CLOSE, 1]).await),
        Some(1835)
    );
    // Multi-statements now off: the canned executor refuses a batch.
    let mut q = vec![com::QUERY];
    q.extend_from_slice(b"SELECT 1; SELECT 2");
    assert_eq!(err_code(&command(&mut raw, &q).await), Some(1064));
    // COM_INIT_DB: OK with the schema in session tracking.
    let mut init = vec![com::INIT_DB];
    init.extend_from_slice(b"shop");
    let ok = OkPacket::decode(&command(&mut raw, &init).await, client_caps()).unwrap();
    assert_eq!(
        ok.session_changes().unwrap(),
        [sprig_proto_mysql::packets::SessionChange::Schema(
            Bytes::from_static(b"shop")
        )]
    );
    // COM_PROCESS_INFO: a resultset of 8 columns.
    assert_eq!(command(&mut raw, &[com::PROCESS_INFO]).await[..], [8]);
    // Unknown statement ids.
    let mut exec = vec![com::STMT_EXECUTE];
    exec.extend_from_slice(&[9, 0, 0, 0, 0, 1, 0, 0, 0]);
    let mut raw2 = raw_login(gw.mysql).await.0;
    assert_eq!(err_code(&command(&mut raw2, &exec).await), Some(1243));
    // A statement that the executor leaves unanswered never happens with
    // the canned executor; COM_QUIT closes.
    raw2.codec_mut().reset_seq();
    raw2.send(Bytes::from_static(&[com::QUIT])).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), raw2.next())
            .await
            .unwrap()
            .is_none()
    );
    gw.stop().await;
}

#[tokio::test]
async fn raw_handshake_refusals() {
    // An SSLRequest on an `off` listener: 1043.
    let gw = Gateway::start(TlsMode::Off).await;
    let mut raw = Framed::new(
        TcpStream::connect(gw.mysql).await.unwrap(),
        PacketCodec::default(),
    );
    let _ = next(&mut raw).await;
    let ssl = SslRequest {
        caps: client_caps(),
        max_packet: 0,
        collation: 255,
    };
    raw.send(Packet::new(1, ssl.encode())).await.unwrap();
    assert_eq!(err_code(&next(&mut raw).await), Some(1043));

    // A client without PLUGIN_AUTH: 1251.
    let mut raw = Framed::new(
        TcpStream::connect(gw.mariadb).await.unwrap(),
        PacketCodec::default(),
    );
    let hs = HandshakeV10::decode(&next(&mut raw).await).unwrap();
    assert!(
        !hs.caps.contains(Caps::CLIENT_MYSQL),
        "MariaDB greeting clears bit 0"
    );
    assert!(hs.caps.contains(Caps::MARIADB_CLIENT_EXTENDED_METADATA));
    assert_eq!(hs.collation, 45);
    let resp = HandshakeResponse {
        caps: client_caps() - Caps::PLUGIN_AUTH,
        max_packet: 1 << 24,
        collation: 45,
        user: b"nat".to_vec(),
        ..HandshakeResponse::default()
    };
    raw.send(Packet::new(1, resp.encode())).await.unwrap();
    assert_eq!(err_code(&next(&mut raw).await), Some(1251));
    gw.stop().await;
}

// ----- real command-line clients ---------------------------------------------

/// Runs a client command line; returns (success, stdout + stderr).
async fn run_cli(args: Vec<String>) -> (bool, String) {
    let out = tokio::task::spawn_blocking(move || {
        std::process::Command::new(&args[0])
            .args(&args[1..])
            .output()
    })
    .await
    .unwrap();
    match out {
        Ok(o) => (
            o.status.success(),
            format!(
                "{}{}",
                String::from_utf8_lossy(&o.stdout),
                String::from_utf8_lossy(&o.stderr)
            ),
        ),
        Err(e) => (false, format!("can't run {e}")),
    }
}

/// The `mysql` client on the host (if installed) and the `mariadb` client
/// from a throwaway `mariadb:11.8` container (if Docker is available)
/// against both personas. Ignored by default: it needs those tools.
/// `cargo test -p sprig-gateway --lib mysql::tests::cli_clients -- --ignored --nocapture`
#[tokio::test]
#[ignore = "needs the mysql client and/or Docker"]
async fn cli_clients() {
    let mut gw = Gateway::start(TlsMode::Optional).await;
    // Docker containers reach the host through host.docker.internal, so
    // these listeners bind every interface.
    for s in &mut gw.specs {
        s.bind = "0.0.0.0:0".parse().unwrap();
    }
    let report = gw
        .mgr
        .apply(resolution(gw.specs.clone(), Duration::from_secs(60)))
        .await;
    assert!(report.rejected.is_empty());
    let (my, maria) = (
        gw.mgr.local_addr("mysql").await.unwrap().port(),
        gw.mgr.local_addr("mariadb").await.unwrap().port(),
    );
    let sql = "SELECT 1; SELECT @@version_comment; SELECT VERSION(); SELECT CURRENT_USER(); SHOW PROCESSLIST";
    let host_mysql = |port: u16, extra: &[&str]| {
        let mut v: Vec<String> = [
            "mysql",
            "-h127.0.0.1",
            &format!("-P{port}"),
            "-uapp",
            &format!("-p{PW}"),
            "--ssl-mode=REQUIRED",
        ]
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
        v.extend(extra.iter().map(|s| (*s).to_owned()));
        v.extend(["-e".to_owned(), sql.to_owned()]);
        v
    };
    for (name, args) in [
        ("mysql 3306 persona, TLS", host_mysql(my, &[])),
        ("mysql 3307 persona, TLS", host_mysql(maria, &[])),
        (
            "mysql, zstd",
            host_mysql(
                my,
                &[
                    "--compression-algorithms=zstd",
                    "--zstd-compression-level=5",
                ],
            ),
        ),
        (
            "mysql, zlib",
            host_mysql(my, &["--compression-algorithms=zlib"]),
        ),
        (
            "mysql, cleartext plugin over TLS",
            host_mysql(
                my,
                &[
                    "--enable-cleartext-plugin",
                    "--default-auth=mysql_clear_password",
                ],
            ),
        ),
    ] {
        let (ok, out) = run_cli(args).await;
        eprintln!("=== {name}: ok={ok}\n{out}");
        assert!(ok || out.starts_with("can't run"), "{name}: {out}");
    }
    // Plaintext after the TLS logins filled the fast cache.
    let (ok, out) = run_cli(vec![
        "mysql".into(),
        "-h127.0.0.1".into(),
        format!("-P{my}"),
        "-uapp".into(),
        format!("-p{PW}"),
        "--ssl-mode=DISABLED".into(),
        "-e".into(),
        "SELECT 1".into(),
    ])
    .await;
    eprintln!("=== mysql plaintext fast path: ok={ok}\n{out}");
    for (port, persona) in [(maria, "3307"), (my, "3306")] {
        let (ok, out) = run_cli(vec![
            "docker".into(),
            "run".into(),
            "--rm".into(),
            "mariadb:11.8".into(),
            "mariadb".into(),
            "-hhost.docker.internal".into(),
            format!("-P{port}"),
            "-uapp".into(),
            format!("-p{PW}"),
            "--ssl".into(),
            "--ssl-verify-server-cert=0".into(),
            "-e".into(),
            sql.into(),
        ])
        .await;
        eprintln!("=== mariadb client (docker) against {persona}: ok={ok}\n{out}");
        let (ok, out) = run_cli(vec![
            "docker".into(),
            "run".into(),
            "--rm".into(),
            "mariadb:11.8".into(),
            "mariadb".into(),
            "-hhost.docker.internal".into(),
            format!("-P{port}"),
            "-unat".into(),
            format!("-p{PW}"),
            "--skip-ssl".into(),
            "--compress".into(),
            "-e".into(),
            "SELECT 1; SELECT @@version".into(),
        ])
        .await;
        eprintln!(
            "=== mariadb client (docker) native+compress, plaintext against {persona}: ok={ok}\n{out}"
        );
    }
    gw.stop().await;
}
