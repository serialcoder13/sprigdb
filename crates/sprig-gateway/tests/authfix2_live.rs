//! Live checks of account and authorization features, against a running
//! private deployment:
//!
//! - `set_global_tenant_defaults`: `SYSTEM_VARIABLES_ADMIN` makes `SET
//!   GLOBAL` / `PERSIST` of the allowed variables the tenant's defaults
//!   for new sessions (MySQL, MariaDB and HTTP), refuses the rest (1227),
//!   and leaves other tenants alone.
//! - `set_any_definer`: `DEFINER=` another user, for holders, becomes that
//!   user (views, procedures, functions, triggers, events); `SQL SECURITY
//!   DEFINER` runs with the definer's grants and row-level security
//!   policies; non-holders and unknown accounts get 1227.
//! - `client_certificates_everywhere`: a CA made now; `REQUIRE X509`,
//!   `SUBJECT` and `ISSUER` on MySQL, HTTPS (Basic), Kafka, AMQP (PLAIN
//!   and `EXTERNAL`) and Bolt; `SHOW CREATE USER` on both personas; the CA
//!   file replaced while the listeners run.
//! - `account_changes_are_audited`: `ALTER USER ... REQUIRE`, `ATTRIBUTE`
//!   and `COMMENT` emit `auth.account_changed` naming the change, without
//!   secrets (the gateway's log).
//! - `sink_pauses_when_its_owner_loses_select`: 39229 on `REVOKE`, and the
//!   sink resumes on `GRANT`.
//!
//! ```sh
//! export AF2_MYSQL=33306 AF2_MARIADB=33307 AF2_HTTP=33080 AF2_HTTPS=33443
//! export AF2_KAFKA=33092 AF2_AMQPS=33671 AF2_BOLT=33687
//! export AF2_SET=<dir>/setting.sh   # <cmd> <key> <json>: a dc setting
//! export AF2_LOG=<the gateway's log file>  AF2_HOOK_PORT=33881
//! # sink.destination_allowlist must include 127.0.0.1:$AF2_HOOK_PORT
//! cargo test -p sprig-gateway --test authfix2_live -- --nocapture --test-threads 1
//! ```
//!
//! Tenants `acme` and `globex`, each with `app` (password `sprig-dev-app`)
//! holding everything in its tenant (gateway-dev.sh). Skipped without
//! `AF2_MYSQL`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stderr,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::many_single_char_names,
    clippy::option_option,
    missing_docs
)]

use std::sync::Arc;
use std::time::Duration;

use mysql_async::prelude::Queryable;
use mysql_async::{ClientIdentity, Conn, OptsBuilder, SslOpts};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const PW: &str = "sprig-dev-app";
const UPW: &str = "Af2-pass-1";
const H: &str = "127.0.0.1";

fn var(v: &str) -> Option<String> {
    std::env::var(v).ok().filter(|s| !s.is_empty())
}

fn port(v: &str) -> u16 {
    var(v).unwrap_or_else(|| panic!("{v}")).parse().unwrap()
}

fn on() -> bool {
    if var("AF2_MYSQL").is_none() {
        eprintln!("AF2_MYSQL not set: skipped");
        return false;
    }
    true
}

/// A dc-scope setting through `AF2_SET`.
fn set(key: &str, json: &str) {
    let out = std::process::Command::new(var("AF2_SET").expect("AF2_SET"))
        .args([key, json])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

struct Checks {
    n: usize,
    failed: Vec<String>,
}

impl Checks {
    fn new() -> Self {
        Self {
            n: 0,
            failed: Vec::new(),
        }
    }

    fn ok(&mut self, what: &str, cond: bool, detail: impl FnOnce() -> String) {
        self.n += 1;
        if cond {
            eprintln!("PASS {what}");
        } else {
            let d = detail();
            eprintln!("FAIL {what}: {d}");
            self.failed.push(format!("{what}: {d}"));
        }
    }

    fn done(self, name: &str) {
        eprintln!("{name}: {} checks, {} failed", self.n, self.failed.len());
        assert!(self.failed.is_empty(), "{:#?}", self.failed);
    }
}

/// A MySQL-protocol session on `port` (TLS, any server certificate), with
/// a client certificate when given.
async fn my(port: u16, user: &str, pass: &str, cert: Option<&Pem>) -> mysql_async::Result<Conn> {
    let mut ssl = SslOpts::default().with_danger_accept_invalid_certs(true);
    if let Some(c) = cert {
        ssl = ssl.with_client_identity(Some(ClientIdentity::new(
            c.chain.as_bytes().to_vec().into(),
            c.key.as_bytes().to_vec().into(),
        )));
    }
    let o = OptsBuilder::default()
        .ip_or_hostname(H)
        .tcp_port(port)
        .user(Some(user))
        .pass(Some(pass))
        .ssl_opts(Some(ssl))
        .prefer_socket(false);
    Conn::new(o).await
}

fn errno<T>(r: &mysql_async::Result<T>) -> Option<u16> {
    match r {
        Ok(_) => None,
        Err(mysql_async::Error::Server(s)) => Some(s.code),
        Err(_) => Some(0),
    }
}

fn msg<T>(r: &mysql_async::Result<T>) -> String {
    match r {
        Ok(_) => "ok".into(),
        Err(e) => e.to_string(),
    }
}

/// The first column of the first row, or the error as text.
async fn one(c: &mut Conn, q: &str) -> Result<String, String> {
    one_r(c, q).await.map_err(|e| e.to_string())
}

async fn one_r(c: &mut Conn, q: &str) -> mysql_async::Result<String> {
    let v: Option<mysql_async::Value> = c.query_first(q).await?;
    Ok(match v {
        Some(mysql_async::Value::Bytes(b)) => String::from_utf8_lossy(&b).into_owned(),
        Some(mysql_async::Value::Int(i)) => i.to_string(),
        Some(mysql_async::Value::UInt(i)) => i.to_string(),
        Some(mysql_async::Value::NULL) | None => "NULL".into(),
        Some(other) => format!("{other:?}"),
    })
}

/// Waits up to `secs` for `f` to give `true`.
async fn settle<F, Fut>(secs: u64, f: F) -> bool
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..secs * 4 {
        if f().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    false
}

// ---------------------------------------------------------------------
// 1. SET GLOBAL as tenant defaults.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_global_tenant_defaults() {
    if !on() {
        return;
    }
    let (p, pm) = (port("AF2_MYSQL"), port("AF2_MARIADB"));
    let mut ck = Checks::new();
    let mut app = my(p, "acme.app", PW, None).await.unwrap();
    let reset = "SET GLOBAL sql_mode = DEFAULT, time_zone = DEFAULT, transaction_isolation = DEFAULT, \
                 max_execution_time = DEFAULT, wait_timeout = DEFAULT";
    app.query_drop(reset).await.unwrap();
    for q in [
        "DROP USER IF EXISTS af2_plain, af2_sv".to_owned(),
        format!("CREATE USER af2_plain IDENTIFIED BY '{UPW}'"),
        format!("CREATE USER af2_sv IDENTIFIED BY '{UPW}'"),
        "GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO af2_sv".to_owned(),
    ] {
        app.query_drop(&q).await.unwrap();
    }
    let mut plain = my(p, "acme.af2_plain", UPW, None).await.unwrap();
    let r = plain.query_drop("SET GLOBAL time_zone = '+01:00'").await;
    ck.ok(
        "no SYSTEM_VARIABLES_ADMIN: 1227 naming it",
        errno(&r) == Some(1227) && msg(&r).contains("SYSTEM_VARIABLES_ADMIN"),
        || msg(&r),
    );
    let r = plain.query_drop("SET PERSIST wait_timeout = 10").await;
    ck.ok("PERSIST without it: 1227", errno(&r) == Some(1227), || {
        msg(&r)
    });
    let mut sv = my(p, "acme.af2_sv", UPW, None).await.unwrap();
    let r = sv.query_drop("SET GLOBAL max_connections = 10").await;
    ck.ok(
        "outside the allow-list: 1227 even for a holder",
        errno(&r) == Some(1227) && msg(&r).contains("every tenant"),
        || msg(&r),
    );
    let r = sv.query_drop("SET GLOBAL sql_log_bin = 0").await;
    ck.ok("sql_log_bin: 1227", errno(&r) == Some(1227), || msg(&r));
    for (q, code) in [
        ("SET GLOBAL time_zone = 'Mars/Base'", 1298),
        ("SET GLOBAL sql_mode = 'NO_SUCH_MODE'", 1231),
        ("SET GLOBAL transaction_isolation = 'SOMETIMES'", 1231),
        ("SET GLOBAL wait_timeout = 'x'", 1232),
    ] {
        let r = sv.query_drop(q).await;
        ck.ok(&format!("{q}: {code}"), errno(&r) == Some(code), || msg(&r));
    }
    let before = one(&mut sv, "SELECT @@session.time_zone").await.unwrap();
    for q in [
        "SET GLOBAL sql_mode = 'ANSI_QUOTES,STRICT_TRANS_TABLES'",
        "SET PERSIST time_zone = '+03:00'",
        "SET GLOBAL TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "SET PERSIST_ONLY max_execution_time = 1234",
        "SET @@global.wait_timeout = 77",
    ] {
        let r = sv.query_drop(q).await;
        ck.ok(&format!("holder: {q}"), r.is_ok(), || msg(&r));
    }
    let now = one(&mut sv, "SELECT @@session.time_zone").await.unwrap();
    ck.ok(
        "the setting session keeps its own value",
        now == before,
        || format!("{before} -> {now}"),
    );
    let g = one(&mut sv, "SELECT @@global.time_zone").await;
    ck.ok(
        "@@global reads the tenant's value",
        g.as_deref() == Ok("+03:00"),
        || format!("{g:?}"),
    );
    // New sessions on 3306.
    let mut n = my(p, "acme.af2_plain", UPW, None).await.unwrap();
    for (q, want) in [
        ("SELECT @@sql_mode", "ANSI_QUOTES,STRICT_TRANS_TABLES"),
        ("SELECT @@time_zone", "+03:00"),
        ("SELECT @@transaction_isolation", "READ-COMMITTED"),
        ("SELECT @@max_execution_time", "1234"),
        ("SELECT @@wait_timeout", "77"),
    ] {
        let v = one(&mut n, q).await;
        ck.ok(
            &format!("new 3306 session: {q}"),
            v.as_deref() == Ok(want),
            || format!("{v:?}"),
        );
    }
    let v = one(
        &mut n,
        "SELECT NOW() = CONVERT_TZ(UTC_TIMESTAMP(), '+00:00', '+03:00')",
    )
    .await;
    ck.ok(
        "the time zone applies to NOW()",
        v.as_deref() == Ok("1"),
        || format!("{v:?}"),
    );
    // 3307.
    let mut m = my(pm, "acme.af2_plain", UPW, None).await.unwrap();
    let v = one(&mut m, "SELECT @@time_zone").await;
    ck.ok(
        "new 3307 session: time_zone",
        v.as_deref() == Ok("+03:00"),
        || format!("{v:?}"),
    );
    let v = one(&mut m, "SELECT @@sql_mode").await;
    ck.ok(
        "new 3307 session: sql_mode",
        v.as_deref()
            .is_ok_and(|s| s.contains("ANSI_QUOTES") && s.contains("STRICT_TRANS_TABLES")),
        || format!("{v:?}"),
    );
    let v = one(&mut m, "SELECT @@transaction_isolation").await;
    ck.ok(
        "new 3307 session: transaction_isolation",
        v.as_deref() == Ok("READ-COMMITTED"),
        || format!("{v:?}"),
    );
    let mut msv = my(pm, "acme.af2_sv", UPW, None).await.unwrap();
    let r = msv.query_drop("SET GLOBAL max_execution_time = 5").await;
    ck.ok(
        "3307: max_execution_time is MySQL's (1193)",
        errno(&r) == Some(1193),
        || msg(&r),
    );
    // HTTP (a Basic session).
    if let Some(hp) = var("AF2_HTTP") {
        let body = http_sql(
            hp.parse().unwrap(),
            None,
            "acme.af2_plain",
            UPW,
            "SELECT @@time_zone AS tz",
        )
        .await;
        ck.ok("HTTP session: time_zone", body.contains("+03:00"), || {
            body.clone()
        });
    }
    // Another tenant isn't touched.
    let mut gx = my(p, "globex.app", PW, None).await.unwrap();
    let v = one(&mut gx, "SELECT @@time_zone").await;
    ck.ok(
        "globex keeps the server's time_zone",
        v.as_deref() != Ok("+03:00"),
        || format!("{v:?}"),
    );
    // An expression, and session assignments in the same statement.
    let r = sv
        .query_drop("SET GLOBAL time_zone = CONCAT('+0', '4:00'), SESSION wait_timeout = 30")
        .await;
    ck.ok("an expression and a SESSION part", r.is_ok(), || msg(&r));
    let v = one(&mut sv, "SELECT @@session.wait_timeout").await;
    ck.ok("the SESSION part ran", v.as_deref() == Ok("30"), || {
        format!("{v:?}")
    });
    let mut n = my(p, "acme.af2_plain", UPW, None).await.unwrap();
    let v = one(&mut n, "SELECT @@time_zone").await;
    ck.ok(
        "the expression's value",
        v.as_deref() == Ok("+04:00"),
        || format!("{v:?}"),
    );
    // DEFAULT removes the tenant's value.
    sv.query_drop(reset).await.unwrap();
    let mut n = my(p, "acme.af2_plain", UPW, None).await.unwrap();
    let v = one(&mut n, "SELECT @@time_zone").await;
    ck.ok(
        "DEFAULT: the server's value again",
        v.as_deref() != Ok("+04:00"),
        || format!("{v:?}"),
    );
    let v = one(&mut n, "SELECT @@global.time_zone").await;
    ck.ok(
        "DEFAULT: @@global is the server's",
        v.as_deref() != Ok("+04:00"),
        || format!("{v:?}"),
    );
    app.query_drop("DROP USER IF EXISTS af2_plain, af2_sv")
        .await
        .unwrap();
    ck.done("set_global_tenant_defaults");
}

// ---------------------------------------------------------------------
// 2. SET_ANY_DEFINER.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_any_definer() {
    if !on() {
        return;
    }
    let p = port("AF2_MYSQL");
    let mut ck = Checks::new();
    let mut app = my(p, "acme.app", PW, None).await.unwrap();
    for q in [
        "DROP DATABASE IF EXISTS af2def".to_owned(),
        "DROP USER IF EXISTS af2_bob, af2_eve, af2_cal, af2_run".to_owned(),
        "CREATE DATABASE af2def".to_owned(),
        "CREATE TABLE af2def.docs (id INT PRIMARY KEY, owner VARCHAR(64) NOT NULL)".to_owned(),
        "INSERT INTO af2def.docs VALUES (1, 'af2_bob@%'), (2, 'af2_bob@%'), (3, 'app@%')"
            .to_owned(),
        format!("CREATE USER af2_bob IDENTIFIED BY '{UPW}'"),
        format!("CREATE USER af2_eve IDENTIFIED BY '{UPW}'"),
        format!("CREATE USER af2_cal IDENTIFIED BY '{UPW}'"),
        format!("CREATE USER af2_run IDENTIFIED BY '{UPW}'"),
        "GRANT SELECT, EXECUTE ON af2def.* TO af2_bob".to_owned(),
        "GRANT ALL ON af2def.* TO af2_eve".to_owned(),
        "GRANT ALL ON af2def.* TO af2_cal".to_owned(),
        "GRANT SET_ANY_DEFINER ON *.* TO af2_cal".to_owned(),
        "GRANT EXECUTE ON af2def.* TO af2_run".to_owned(),
    ] {
        let r = app.query_drop(&q).await;
        ck.ok(&format!("setup: {q}"), r.is_ok(), || msg(&r));
    }
    let mut eve = my(p, "acme.af2_eve", UPW, None).await.unwrap();
    let r = eve
        .query_drop("CREATE DEFINER=`af2_bob`@`%` VIEW af2def.v0 AS SELECT id FROM af2def.docs")
        .await;
    ck.ok(
        "without SET_ANY_DEFINER: 1227",
        errno(&r) == Some(1227) && msg(&r).contains("SET_ANY_DEFINER"),
        || msg(&r),
    );
    let mut cal = my(p, "acme.af2_cal", UPW, None).await.unwrap();
    let r = cal
        .query_drop("CREATE DEFINER='nobody'@'%' PROCEDURE af2def.p0() SELECT 1")
        .await;
    ck.ok(
        "an account the tenant doesn't have: 1227 (ALLOW_NONEXISTENT_DEFINER)",
        errno(&r) == Some(1227) && msg(&r).contains("ALLOW_NONEXISTENT_DEFINER"),
        || msg(&r),
    );
    for q in [
        "CREATE DEFINER=`af2_bob`@`%` VIEW af2def.v1 AS SELECT id FROM af2def.docs",
        "CREATE DEFINER='af2_bob'@'%' PROCEDURE af2def.cnt() SQL SECURITY DEFINER \
         SELECT COUNT(*) FROM af2def.docs",
        "CREATE DEFINER=af2_bob FUNCTION af2def.total() RETURNS INT READS SQL DATA \
         SQL SECURITY DEFINER RETURN (SELECT COUNT(*) FROM af2def.docs)",
        "CREATE DEFINER=`af2_bob`@`%` TRIGGER af2def.tr BEFORE INSERT ON af2def.docs \
         FOR EACH ROW SET NEW.owner = NEW.owner",
        "CREATE DEFINER=`af2_bob`@`%` EVENT af2def.ev ON SCHEDULE EVERY 1 DAY DISABLE DO SELECT 1",
        "CREATE DEFINER=CURRENT_USER PROCEDURE af2def.mine() SELECT CURRENT_USER()",
    ] {
        let r = cal.query_drop(q).await;
        ck.ok(&format!("holder: {q}"), r.is_ok(), || msg(&r));
    }
    for (q, want) in [
        (
            "SELECT DEFINER FROM information_schema.VIEWS WHERE TABLE_SCHEMA = 'af2def' AND TABLE_NAME = 'v1'",
            "af2_bob@%",
        ),
        (
            "SELECT DEFINER FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = 'af2def' AND ROUTINE_NAME = 'cnt'",
            "af2_bob@%",
        ),
        (
            "SELECT DEFINER FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = 'af2def' AND ROUTINE_NAME = 'total'",
            "af2_bob@%",
        ),
        (
            "SELECT DEFINER FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = 'af2def' AND TRIGGER_NAME = 'tr'",
            "af2_bob@%",
        ),
        (
            "SELECT DEFINER FROM information_schema.EVENTS WHERE EVENT_SCHEMA = 'af2def' AND EVENT_NAME = 'ev'",
            "af2_bob@%",
        ),
        (
            "SELECT DEFINER FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = 'af2def' AND ROUTINE_NAME = 'mine'",
            "af2_cal@%",
        ),
    ] {
        let v = one(&mut app, q).await;
        ck.ok(
            &format!("definer: {want} ({q})"),
            v.as_deref() == Ok(want),
            || format!("{v:?}"),
        );
    }
    let v: mysql_async::Result<Option<mysql_async::Row>> =
        app.query_first("SHOW CREATE VIEW af2def.v1").await;
    let text = v
        .as_ref()
        .ok()
        .and_then(|r| r.as_ref())
        .and_then(|r| r.get::<String, usize>(1))
        .unwrap_or_default();
    ck.ok(
        "SHOW CREATE VIEW names the definer",
        text.contains("DEFINER=`af2_bob`@`%`"),
        || format!("{v:?}"),
    );
    // SQL SECURITY DEFINER runs with bob's grants: af2_run holds EXECUTE
    // only.
    let mut run = my(p, "acme.af2_run", UPW, None).await.unwrap();
    let r = run.query_drop("SELECT COUNT(*) FROM af2def.docs").await;
    ck.ok(
        "the caller can't read the table itself",
        errno(&r) == Some(1142),
        || msg(&r),
    );
    let v = one(&mut run, "CALL af2def.cnt()").await;
    ck.ok(
        "SQL SECURITY DEFINER: bob's SELECT reads",
        v.as_deref() == Ok("3"),
        || format!("{v:?}"),
    );
    // Row-level security: bob's policy applies inside his routines.
    for q in [
        "CREATE POLICY own ON af2def.docs USING (owner = CURRENT_USER())",
        "ALTER TABLE af2def.docs ENABLE ROW LEVEL SECURITY",
    ] {
        let r = app.query_drop(q).await;
        ck.ok(&format!("setup: {q}"), r.is_ok(), || msg(&r));
    }
    let ok = settle(10, || async {
        let Ok(mut c) = my(p, "acme.af2_run", UPW, None).await else {
            return false;
        };
        one(&mut c, "CALL af2def.cnt()").await.as_deref() == Ok("2")
    })
    .await;
    ck.ok(
        "row-level security: the definer's (bob's) policy, 2 rows",
        ok,
        String::new,
    );
    let mut run = my(p, "acme.af2_run", UPW, None).await.unwrap();
    let v = one(&mut run, "SELECT af2def.total()").await;
    ck.ok(
        "a definer function: bob's rows",
        v.as_deref() == Ok("2"),
        || format!("{v:?}"),
    );
    // Bob's grants, not the creator's: without his SELECT it fails.
    app.query_drop("ALTER TABLE af2def.docs DISABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    app.query_drop("REVOKE SELECT ON af2def.* FROM af2_bob")
        .await
        .unwrap();
    let failed = settle(10, || async {
        let Ok(mut c) = my(p, "acme.af2_run", UPW, None).await else {
            return false;
        };
        c.query_drop("CALL af2def.cnt()").await.is_err()
    })
    .await;
    ck.ok(
        "the definer's SELECT revoked: the call fails",
        failed,
        String::new,
    );
    app.query_drop("DROP DATABASE IF EXISTS af2def")
        .await
        .unwrap();
    app.query_drop("DROP USER IF EXISTS af2_bob, af2_eve, af2_cal, af2_run")
        .await
        .unwrap();
    ck.done("set_any_definer");
}

// ---------------------------------------------------------------------
// 3. Client certificates.

struct Pki {
    issuer: Issuer<'static, KeyPair>,
    pem: String,
}

/// A client chain and key, PEM.
struct Pem {
    chain: String,
    key: String,
}

fn ca(cn: &str) -> Pki {
    let mut p = CertificateParams::new(Vec::<String>::new()).unwrap();
    p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    p.distinguished_name = rcgen::DistinguishedName::new();
    p.distinguished_name.push(DnType::CommonName, cn);
    let key = KeyPair::generate().unwrap();
    let cert = p.self_signed(&key).unwrap();
    Pki {
        pem: cert.pem(),
        issuer: Issuer::new(p, key),
    }
}

/// `/O=Authfix2/CN=<cn>` from `ca`.
fn leaf(ca: &Pki, cn: &str) -> Pem {
    let mut p = CertificateParams::new(Vec::<String>::new()).unwrap();
    p.distinguished_name = rcgen::DistinguishedName::new();
    p.distinguished_name
        .push(DnType::OrganizationName, "Authfix2");
    p.distinguished_name.push(DnType::CommonName, cn);
    p.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let key = KeyPair::generate().unwrap();
    let cert = p.signed_by(&key, &ca.issuer).unwrap();
    Pem {
        chain: cert.pem(),
        key: key.serialize_pem(),
    }
}

#[derive(Debug)]
struct AnyServer;

impl rustls::client::danger::ServerCertVerifier for AnyServer {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

type Tls = tokio_rustls::client::TlsStream<TcpStream>;

/// TLS to `port`, presenting `cert` when given.
async fn tls(port: u16, cert: Option<&Pem>, alpn: &[&[u8]]) -> std::io::Result<Tls> {
    use rustls::pki_types::pem::PemObject;
    let b = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(AnyServer));
    let mut cfg = match cert {
        Some(c) => {
            let chain: Vec<CertificateDer<'static>> =
                CertificateDer::pem_slice_iter(c.chain.as_bytes())
                    .collect::<Result<_, _>>()
                    .unwrap();
            let key = PrivateKeyDer::from_pem_slice(c.key.as_bytes()).unwrap();
            b.with_client_auth_cert(chain, key).unwrap()
        }
        None => b.with_no_client_auth(),
    };
    cfg.alpn_protocols = alpn.iter().map(|a| a.to_vec()).collect();
    let tcp = TcpStream::connect((H, port)).await?;
    tokio_rustls::TlsConnector::from(Arc::new(cfg))
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await
}

/// `POST /v1/databases/-/sql:execute` with Basic over TLS (`https` set
/// to the client certificate) or plain HTTP; the status line and body.
async fn http_sql(
    port: u16,
    https: Option<Option<&Pem>>,
    user: &str,
    pass: &str,
    sql: &str,
) -> String {
    use base64::Engine as _;
    let body = serde_json::json!({ "sql": sql }).to_string();
    let auth = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"));
    let req = format!(
        "POST /v1/databases/-/sql:execute HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Basic {auth}\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let mut out = Vec::new();
    let r = async {
        if let Some(cert) = https {
            let mut s = tls(port, cert, &[b"http/1.1"]).await?;
            s.write_all(req.as_bytes()).await?;
            s.read_to_end(&mut out).await
        } else {
            let mut s = TcpStream::connect((H, port)).await?;
            s.write_all(req.as_bytes()).await?;
            s.read_to_end(&mut out).await
        }
    };
    match tokio::time::timeout(Duration::from_secs(10), r).await {
        Ok(Ok(_)) => String::from_utf8_lossy(&out).into_owned(),
        Ok(Err(e)) => format!("io error: {e}"),
        Err(_) => "timeout".into(),
    }
}

/// A Kafka SASL PLAIN login over TLS: `Ok` or the refusal.
async fn kafka_login(port: u16, cert: Option<&Pem>, user: &str, pass: &str) -> Result<(), String> {
    let s = tls(port, cert, &[])
        .await
        .map_err(|e| format!("tls: {e}"))?;
    let mut c = sprig_proto_kafka::client::Client::new(s, "af2");
    tokio::time::timeout(Duration::from_secs(10), c.sasl_plain(user, pass))
        .await
        .map_err(|_| "timeout".to_owned())?
        .map_err(|e| format!("{e:?}"))
}

/// A Bolt basic login over TLS.
async fn bolt_login(port: u16, cert: Option<&Pem>, user: &str, pass: &str) -> Result<(), String> {
    let s = tls(port, cert, &[])
        .await
        .map_err(|e| format!("tls: {e}"))?;
    let mut c = sprig_proto_bolt::client::BoltClient::handshake(s, None)
        .await
        .map_err(|e| format!("{e:?}"))?;
    tokio::time::timeout(Duration::from_secs(10), c.login(user, pass))
        .await
        .map_err(|_| "timeout".to_owned())?
        .map_err(|e| format!("{e:?}"))
}

/// An AMQP 0-9-1 login over TLS with `mechanism` (`PLAIN` with user and
/// password, or `EXTERNAL`): the mechanisms the server offered, and the
/// result.
async fn amqp_login(
    port: u16,
    cert: Option<&Pem>,
    mechanism: &str,
    user: &str,
    pass: &str,
) -> (String, Result<(), String>) {
    use sprig_proto_amqp::v091::client::{Client, default_capabilities, plain};
    let s = match tls(port, cert, &[]).await {
        Ok(s) => s,
        Err(e) => return (String::new(), Err(format!("tls: {e}"))),
    };
    let response = if mechanism == "PLAIN" {
        plain(user, pass)
    } else {
        bytes::Bytes::new()
    };
    let r = tokio::time::timeout(
        Duration::from_secs(10),
        Client::connect(s, mechanism, response, "/", default_capabilities(), None),
    )
    .await;
    match r {
        Ok(Ok(c)) => (c.mechanisms.clone(), Ok(())),
        Ok(Err(e)) => (String::new(), Err(e.to_string())),
        Err(_) => (String::new(), Err("timeout".into())),
    }
}

/// The mechanisms an AMQP server offers (the `connection.start`).
async fn amqp_offered(port: u16, cert: Option<&Pem>) -> String {
    use sprig_proto_amqp::v091::client::Client;
    use sprig_proto_amqp::v091::method::Method;
    let Ok(s) = tls(port, cert, &[]).await else {
        return "tls failed".into();
    };
    let Ok(mut c) = Client::raw(s).await else {
        return "no client".into();
    };
    match c.next_method(0).await.map(|m| m.method) {
        Ok(Method::ConnectionStart(st)) => String::from_utf8_lossy(&st.mechanisms).into_owned(),
        other => format!("{other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_certificates_everywhere() {
    if !on() {
        return;
    }
    let (p, pm) = (port("AF2_MYSQL"), port("AF2_MARIADB"));
    let (https, kafka, amqps, bolt) = (
        port("AF2_HTTPS"),
        port("AF2_KAFKA"),
        port("AF2_AMQPS"),
        port("AF2_BOLT"),
    );
    let mut ck = Checks::new();
    let dir = std::env::temp_dir().join(format!("af2-ca-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let one_ca = ca("Authfix2 CA one");
    let two_ca = ca("Authfix2 CA two");
    let ca_path = dir.join("client-ca.pem");
    std::fs::write(&ca_path, &one_ca.pem).unwrap();
    // `acme.af2_x` and `af2_x` (the AMQP EXTERNAL user names come from the
    // common name: `acme.<user>`).
    let alice = leaf(&one_ca, "acme.af2_subj");
    let bob = leaf(&one_ca, "acme.af2_x509");
    let rogue = leaf(&two_ca, "acme.af2_subj");

    let mut app = my(p, "acme.app", PW, None).await.unwrap();
    for q in [
        // AMQP's vhost `/` is database `mq` (amqp.default_vhost_database).
        "CREATE DATABASE IF NOT EXISTS mq".to_owned(),
        "DROP USER IF EXISTS af2_x509, af2_subj, af2_bad, af2_pw".to_owned(),
        format!("CREATE USER af2_x509 IDENTIFIED BY '{UPW}' REQUIRE X509"),
        format!(
            "CREATE USER af2_subj IDENTIFIED BY '{UPW}' REQUIRE SUBJECT '/O=Authfix2/CN=acme.af2_subj' \
             AND ISSUER '/CN=Authfix2 CA one'"
        ),
        format!(
            "CREATE USER af2_bad IDENTIFIED BY '{UPW}' REQUIRE SUBJECT '/O=Authfix2/CN=someone'"
        ),
        format!("CREATE USER af2_pw IDENTIFIED BY '{UPW}'"),
        "GRANT SELECT ON *.* TO af2_x509, af2_subj, af2_bad, af2_pw".to_owned(),
    ] {
        let r = app.query_drop(&q).await;
        ck.ok(&format!("setup: {q}"), r.is_ok(), || msg(&r));
    }
    // SHOW CREATE USER, both personas.
    let v = one(&mut app, "SHOW CREATE USER af2_subj").await;
    ck.ok(
        "SHOW CREATE USER (3306): REQUIRE SUBJECT ... ISSUER, no hash",
        v.as_deref().is_ok_and(|s| {
            s.starts_with("CREATE USER `af2_subj`@`%` IDENTIFIED WITH 'caching_sha2_password' REQUIRE SUBJECT '/O=Authfix2/CN=acme.af2_subj' ISSUER '/CN=Authfix2 CA one' PASSWORD EXPIRE DEFAULT")
                && !s.contains(" AS ")
        }),
        || format!("{v:?}"),
    );
    let v = one(&mut app, "SHOW CREATE USER af2_x509").await;
    ck.ok(
        "SHOW CREATE USER (3306): REQUIRE X509",
        v.as_deref().is_ok_and(|s| s.contains(" REQUIRE X509 ")),
        || format!("{v:?}"),
    );
    let mut am = my(pm, "acme.app", PW, None).await.unwrap();
    let v = one(&mut am, "SHOW CREATE USER af2_subj").await;
    ck.ok(
        "SHOW CREATE USER (3307): MariaDB's form",
        v.as_deref() == Ok(
            "CREATE USER `af2_subj`@`%` REQUIRE ISSUER '/CN=Authfix2 CA one' SUBJECT '/O=Authfix2/CN=acme.af2_subj'",
        ),
        || format!("{v:?}"),
    );
    let mut pwc = my(p, "acme.af2_pw", UPW, None).await.unwrap();
    let r = one_r(&mut pwc, "SHOW CREATE USER af2_subj").await;
    ck.ok(
        "another's SHOW CREATE USER without CREATE USER: 1142",
        errno(&r) == Some(1142),
        || msg(&r),
    );
    let r = one_r(&mut pwc, "SHOW CREATE USER CURRENT_USER()").await;
    ck.ok("one's own: allowed", r.is_ok(), || msg(&r));
    let r = one_r(&mut app, "SHOW CREATE USER nobody").await;
    ck.ok("an unknown account: 1396", errno(&r) == Some(1396), || {
        msg(&r)
    });

    // The listeners ask for certificates.
    set(
        "tls.bundles.default.client_ca_path",
        &format!("\"{}\"", ca_path.display()),
    );
    for l in ["mysql", "https", "kafka", "amqps", "bolt"] {
        set(&format!("listener.{l}.client_cert"), "\"optional\"");
    }
    let ready = settle(15, || async {
        my(p, "acme.af2_x509", UPW, Some(&bob)).await.is_ok()
            && kafka_login(kafka, Some(&bob), "acme.af2_x509", UPW)
                .await
                .is_ok()
    })
    .await;
    ck.ok("listeners applied", ready, String::new);

    // MySQL (already covered by wireup; the CA reload below uses it).
    let r = my(p, "acme.af2_subj", UPW, Some(&alice)).await;
    ck.ok("MySQL: SUBJECT/ISSUER match", r.is_ok(), || msg(&r));

    // Kafka.
    let r = kafka_login(kafka, Some(&alice), "acme.af2_subj", UPW).await;
    ck.ok("Kafka: SUBJECT/ISSUER match logs in", r.is_ok(), || {
        format!("{r:?}")
    });
    let r = kafka_login(kafka, Some(&bob), "acme.af2_subj", UPW).await;
    ck.ok("Kafka: another subject is refused", r.is_err(), || {
        format!("{r:?}")
    });
    let r = kafka_login(kafka, Some(&alice), "acme.af2_bad", UPW).await;
    ck.ok(
        "Kafka: REQUIRE SUBJECT not matching is refused",
        r.is_err(),
        || format!("{r:?}"),
    );
    let r = kafka_login(kafka, None, "acme.af2_x509", UPW).await;
    ck.ok(
        "Kafka: X509 without a certificate is refused",
        r.is_err(),
        || format!("{r:?}"),
    );
    let r = kafka_login(kafka, None, "acme.af2_pw", UPW).await;
    ck.ok(
        "Kafka: a password user without a certificate",
        r.is_ok(),
        || format!("{r:?}"),
    );

    // Bolt.
    let r = bolt_login(bolt, Some(&alice), "acme.af2_subj", UPW).await;
    ck.ok("Bolt: SUBJECT/ISSUER match logs in", r.is_ok(), || {
        format!("{r:?}")
    });
    let r = bolt_login(bolt, Some(&bob), "acme.af2_x509", UPW).await;
    ck.ok("Bolt: X509 with a certificate logs in", r.is_ok(), || {
        format!("{r:?}")
    });
    let r = bolt_login(bolt, None, "acme.af2_x509", UPW).await;
    ck.ok(
        "Bolt: X509 without a certificate is refused",
        r.is_err(),
        || format!("{r:?}"),
    );
    let r = bolt_login(bolt, Some(&bob), "acme.af2_subj", UPW).await;
    ck.ok("Bolt: another subject is refused", r.is_err(), || {
        format!("{r:?}")
    });

    // HTTPS with Basic.
    let b = http_sql(
        https,
        Some(Some(&alice)),
        "acme.af2_subj",
        UPW,
        "SELECT 1 AS one",
    )
    .await;
    ck.ok(
        "HTTPS Basic: SUBJECT/ISSUER match",
        b.starts_with("HTTP/1.1 200"),
        || b.chars().take(300).collect(),
    );
    let b = http_sql(https, Some(None), "acme.af2_x509", UPW, "SELECT 1 AS one").await;
    ck.ok(
        "HTTPS Basic: X509 without a certificate is 401",
        b.starts_with("HTTP/1.1 401"),
        || b.chars().take(300).collect(),
    );
    let b = http_sql(
        https,
        Some(Some(&bob)),
        "acme.af2_x509",
        UPW,
        "SELECT 1 AS one",
    )
    .await;
    ck.ok(
        "HTTPS Basic: X509 with one",
        b.starts_with("HTTP/1.1 200"),
        || b.chars().take(300).collect(),
    );
    let b = http_sql(
        https,
        Some(Some(&alice)),
        "acme.af2_bad",
        UPW,
        "SELECT 1 AS one",
    )
    .await;
    ck.ok(
        "HTTPS Basic: subject mismatch is 401",
        b.starts_with("HTTP/1.1 401"),
        || b.chars().take(300).collect(),
    );

    // AMQP: PLAIN with the certificate's names, and EXTERNAL.
    let offered = amqp_offered(amqps, None).await;
    ck.ok(
        "AMQP: no EXTERNAL without a certificate",
        !offered.contains("EXTERNAL"),
        || offered.clone(),
    );
    let offered = amqp_offered(amqps, Some(&alice)).await;
    ck.ok(
        "AMQP: EXTERNAL offered with a certificate",
        offered.contains("EXTERNAL"),
        || offered.clone(),
    );
    let (_, r) = amqp_login(amqps, Some(&alice), "PLAIN", "acme.af2_subj", UPW).await;
    ck.ok("AMQP PLAIN: SUBJECT/ISSUER match", r.is_ok(), || {
        format!("{r:?}")
    });
    let (_, r) = amqp_login(amqps, Some(&bob), "PLAIN", "acme.af2_subj", UPW).await;
    ck.ok(
        "AMQP PLAIN: another subject is refused (403)",
        r.as_ref().is_err_and(|e| e.contains("403")),
        || format!("{r:?}"),
    );
    let (_, r) = amqp_login(amqps, Some(&alice), "EXTERNAL", "", "").await;
    ck.ok(
        "AMQP EXTERNAL: CN acme.af2_subj logs in with no password",
        r.is_ok(),
        || format!("{r:?}"),
    );
    let (_, r) = amqp_login(amqps, Some(&bob), "EXTERNAL", "", "").await;
    ck.ok(
        "AMQP EXTERNAL: an X509 account (CN acme.af2_x509)",
        r.is_ok(),
        || format!("{r:?}"),
    );
    let pw_cert = leaf(&one_ca, "acme.af2_pw");
    let (_, r) = amqp_login(amqps, Some(&pw_cert), "EXTERNAL", "", "").await;
    ck.ok(
        "AMQP EXTERNAL: an account without REQUIRE X509/SUBJECT is refused (403)",
        r.as_ref().is_err_and(|e| e.contains("403")),
        || format!("{r:?}"),
    );
    let bad_cert = leaf(&one_ca, "acme.af2_bad");
    let (_, r) = amqp_login(amqps, Some(&bad_cert), "EXTERNAL", "", "").await;
    ck.ok(
        "AMQP EXTERNAL: the account's SUBJECT still checked (403)",
        r.as_ref().is_err_and(|e| e.contains("403")),
        || format!("{r:?}"),
    );

    // The CA file changes while the listeners run: certificates of the new
    // CA are accepted within a few seconds, the old CA's refused.
    let rogue_x = leaf(&two_ca, "acme.af2_x509");
    let r = my(p, "acme.af2_x509", UPW, Some(&rogue_x)).await;
    ck.ok(
        "before: a certificate of CA two fails the handshake",
        r.is_err(),
        || msg(&r),
    );
    std::fs::write(&ca_path, &two_ca.pem).unwrap();
    let swapped = settle(15, || async {
        my(p, "acme.af2_x509", UPW, Some(&rogue_x)).await.is_ok()
    })
    .await;
    ck.ok(
        "after the file changed: CA two's certificate logs in",
        swapped,
        String::new,
    );
    let r = my(p, "acme.af2_x509", UPW, Some(&bob)).await;
    ck.ok("after: CA one's certificate fails", r.is_err(), || msg(&r));
    let r = kafka_login(kafka, Some(&rogue_x), "acme.af2_x509", UPW).await;
    ck.ok("after: Kafka takes CA two too", r.is_ok(), || {
        format!("{r:?}")
    });
    let r = my(p, "acme.af2_subj", UPW, Some(&rogue)).await;
    ck.ok(
        "after: CA two's alice fails ISSUER '/CN=Authfix2 CA one'",
        errno(&r) == Some(1045),
        || msg(&r),
    );

    for l in ["mysql", "https", "kafka", "amqps", "bolt"] {
        set(&format!("listener.{l}.client_cert"), "\"off\"");
    }
    app.query_drop("DROP USER IF EXISTS af2_x509, af2_subj, af2_bad, af2_pw")
        .await
        .unwrap();
    app.query_drop("DROP DATABASE IF EXISTS mq").await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    ck.done("client_certificates_everywhere");
}

// ---------------------------------------------------------------------
// 4. Audit of account changes.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn account_changes_are_audited() {
    if !on() {
        return;
    }
    let Some(log) = var("AF2_LOG") else {
        eprintln!("AF2_LOG not set: skipped");
        return;
    };
    let p = port("AF2_MYSQL");
    let mut ck = Checks::new();
    let mut app = my(p, "acme.app", PW, None).await.unwrap();
    app.query_drop("DROP USER IF EXISTS af2_aud").await.unwrap();
    app.query_drop(format!("CREATE USER af2_aud IDENTIFIED BY '{UPW}'"))
        .await
        .unwrap();
    let start = std::fs::metadata(&log).map(|m| m.len()).unwrap_or(0);
    for q in [
        "ALTER USER af2_aud REQUIRE SUBJECT '/O=Audit/CN=x' AND ISSUER '/CN=Audit CA'",
        "ALTER USER af2_aud REQUIRE X509",
        "ALTER USER af2_aud ATTRIBUTE '{\"org\": \"secret-org-value\", \"gone\": null}'",
        "ALTER USER af2_aud COMMENT 'private comment text'",
        "ALTER USER af2_aud IDENTIFIED BY 'Af2-new-secret-9' REQUIRE NONE",
    ] {
        let r = app.query_drop(q).await;
        ck.ok(q, r.is_ok(), || msg(&r));
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let all = std::fs::read(&log).unwrap();
    let text = String::from_utf8_lossy(&all[usize::try_from(start).unwrap()..]).into_owned();
    let events: Vec<&str> = text
        .lines()
        .filter(|l| l.contains("auth.account_changed") && l.contains("af2_aud"))
        .collect();
    let has = |needle: &str| events.iter().any(|l| l.contains(needle));
    ck.ok(
        "REQUIRE SUBJECT/ISSUER audited",
        has("REQUIRE SUBJECT '/O=Audit/CN=x' AND ISSUER '/CN=Audit CA'"),
        || events.join("\n"),
    );
    ck.ok("REQUIRE X509 audited", has("REQUIRE X509"), || {
        events.join("\n")
    });
    ck.ok("REQUIRE NONE audited", has("REQUIRE NONE"), || {
        events.join("\n")
    });
    ck.ok(
        "ATTRIBUTE audited with its keys",
        has("ATTRIBUTE set: org; removed: gone"),
        || events.join("\n"),
    );
    ck.ok("COMMENT audited", has("COMMENT set: comment"), || {
        events.join("\n")
    });
    ck.ok(
        "no secrets: values, comment text and passwords absent",
        !text.contains("secret-org-value")
            && !text.contains("private comment text")
            && !text.contains("Af2-new-secret-9"),
        || "a secret is in the log".into(),
    );
    app.query_drop("DROP USER IF EXISTS af2_aud").await.unwrap();
    ck.done("account_changes_are_audited");
}

// ---------------------------------------------------------------------
// 5. 39229: a sink pauses when its owner loses SELECT.

async fn sink_state(c: &mut Conn, db: &str, sink: &str) -> String {
    let rows: mysql_async::Result<Vec<mysql_async::Row>> =
        c.query(format!("SHOW SINKS FROM {db}")).await;
    match rows {
        Ok(rows) => rows
            .into_iter()
            .find_map(|r| {
                let cols: Vec<String> = r
                    .unwrap()
                    .into_iter()
                    .map(|v| match v {
                        mysql_async::Value::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
                        other => format!("{other:?}"),
                    })
                    .collect();
                cols.get(1)
                    .is_some_and(|n| n == sink)
                    .then(|| cols.join(" | "))
            })
            .unwrap_or_else(|| "no such sink".into()),
        Err(e) => e.to_string(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sink_pauses_when_its_owner_loses_select() {
    if !on() {
        return;
    }
    let Some(hook) = var("AF2_HOOK_PORT") else {
        eprintln!("AF2_HOOK_PORT not set: skipped");
        return;
    };
    let p = port("AF2_MYSQL");
    let mut ck = Checks::new();
    // A webhook receiver that answers 200 to everything.
    let l = tokio::net::TcpListener::bind((H, hook.parse::<u16>().unwrap()))
        .await
        .unwrap();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = l.accept().await {
            tokio::spawn(async move {
                let mut buf = vec![0u8; 65536];
                let _ = s.read(&mut buf).await;
                let _ = s
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await;
            });
        }
    });
    let mut app = my(p, "acme.app", PW, None).await.unwrap();
    for q in [
        "DROP DATABASE IF EXISTS af2sink".to_owned(),
        "DROP USER IF EXISTS af2_own".to_owned(),
        "CREATE DATABASE af2sink".to_owned(),
        "CREATE TABLE af2sink.orders (id INT PRIMARY KEY, v INT)".to_owned(),
        "CREATE STREAM af2sink.ev PARTITIONS 1".to_owned(),
        format!("CREATE USER af2_own IDENTIFIED BY '{UPW}'"),
        "GRANT SPRIG_SINK_ADMIN ON *.* TO af2_own".to_owned(),
        "GRANT SELECT, INSERT ON af2sink.* TO af2_own".to_owned(),
    ] {
        let r = app.query_drop(&q).await;
        ck.ok(&format!("setup: {q}"), r.is_ok(), || msg(&r));
    }
    let mut own = my(p, "acme.af2_own", UPW, None).await.unwrap();
    own.query_drop("USE af2sink").await.unwrap();
    for q in [
        format!("CREATE CONNECTION hook TYPE WEBHOOK WITH (url = 'http://127.0.0.1:{hook}/in')"),
        "CREATE SINK s1 FROM STREAM ev TO WEBHOOK CONNECTION hook".to_owned(),
    ] {
        let r = own.query_drop(&q).await;
        ck.ok(&format!("owner: {q}"), r.is_ok(), || msg(&r));
    }
    let running = settle(30, || async {
        let Ok(mut c) = my(p, "acme.app", PW, None).await else {
            return false;
        };
        sink_state(&mut c, "af2sink", "s1")
            .await
            .contains("running")
    })
    .await;
    ck.ok("the sink runs", running, String::new);
    // The owner loses SELECT: the grant change pauses it with 39229.
    app.query_drop("REVOKE SELECT ON af2sink.* FROM af2_own")
        .await
        .unwrap();
    let mut st = String::new();
    let paused = settle(15, || async {
        let Ok(mut c) = my(p, "acme.app", PW, None).await else {
            return false;
        };
        let s = sink_state(&mut c, "af2sink", "s1").await;
        s.contains("paused") && s.contains("39229")
    })
    .await;
    if !paused {
        st = sink_state(&mut app, "af2sink", "s1").await;
    }
    ck.ok("REVOKE SELECT: paused with 39229", paused, || st.clone());
    // And back.
    app.query_drop("GRANT SELECT ON af2sink.* TO af2_own")
        .await
        .unwrap();
    let back = settle(20, || async {
        let Ok(mut c) = my(p, "acme.app", PW, None).await else {
            return false;
        };
        sink_state(&mut c, "af2sink", "s1")
            .await
            .contains("running")
    })
    .await;
    ck.ok("GRANT SELECT again: running", back, || {
        "still paused".into()
    });
    // A locked owner pauses it too.
    app.query_drop("ALTER USER af2_own ACCOUNT LOCK")
        .await
        .unwrap();
    let locked = settle(15, || async {
        let Ok(mut c) = my(p, "acme.app", PW, None).await else {
            return false;
        };
        let s = sink_state(&mut c, "af2sink", "s1").await;
        s.contains("39229") && s.contains("locked")
    })
    .await;
    ck.ok("ACCOUNT LOCK: paused with 39229", locked, String::new);
    app.query_drop("ALTER USER af2_own ACCOUNT UNLOCK")
        .await
        .unwrap();
    let back = settle(20, || async {
        let Ok(mut c) = my(p, "acme.app", PW, None).await else {
            return false;
        };
        sink_state(&mut c, "af2sink", "s1")
            .await
            .contains("running")
    })
    .await;
    ck.ok("ACCOUNT UNLOCK: running again", back, String::new);
    let _ = app.query_drop("DROP DATABASE IF EXISTS af2sink").await;
    let _ = app.query_drop("DROP USER IF EXISTS af2_own").await;
    ck.done("sink_pauses_when_its_owner_loses_select");
}
