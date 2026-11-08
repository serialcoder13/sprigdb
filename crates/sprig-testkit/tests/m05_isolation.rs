//! The SQL isolation suite of milestone M0.5 (chapter 48: "the isolation
//! suite passes; a revoked privilege is refused within seconds on every
//! gateway"; chapter 29 build step 6, chapter 46 step 4).
//!
//! Two tenants on one cluster get the same databases (`m05_shop`,
//! `m05_crm`) with a marker value each, and users with different grants:
//!
//! | User | Grants |
//! |---|---|
//! | `m05_reader` | `SELECT ON m05_shop.*`; role `m05_r` (`SELECT ON m05_crm.*`), not a default |
//! | `m05_writer` | `SELECT, INSERT ON m05_shop.orders` |
//! | `m05_col` | `SELECT (id), INSERT ON m05_shop.orders` |
//! | `m05_none` | nothing |
//! | `m05_roleu` | role `m05_r` as a default role |
//!
//! Each user runs a script of statements on both personas; every attempt to
//! cross a grant must fail with the error number MySQL 8.4 (MySQL persona)
//! or MariaDB 11.8 (MariaDB persona) gives for it (1142, 1143, 1044, 1045,
//! 1227, 1049). Then tenant A's administrator tries tenant B's physical
//! schema names every way the suite knows (1049, and B's marker never
//! appears), and, with `SPRIG_TEST_M05_BACKEND`, MariaDB's grant tables are
//! audited: every backend account of a tenant is granted only on that
//! tenant's schemas, never globally.
//!
//! **Propagation.** With `SPRIG_TEST_M05_SECOND` (a second gateway's
//! MariaDB-persona `host:port`), a session on the second gateway reads a
//! table, the administrator revokes the grant on the first, and the
//! session polls until refused: the time is printed and must be under 5 s.
//! The same for a database-level grant while the session is `USE`d into
//! the database.
//!
//! ```sh
//! SPRIG_TEST_M05_A=mysql://authz-a.app:sprig-authz-app@127.0.0.1:14307 \
//! SPRIG_TEST_M05_B=mysql://authz-b.app:sprig-authz-app@127.0.0.1:14307 \
//! SPRIG_TEST_M05_MYSQL_PORT=14306 \
//! SPRIG_TEST_M05_SECOND=127.0.0.1:14317 \
//! SPRIG_TEST_M05_BACKEND=mysql://root:sprig-dev-root@127.0.0.1:13301 \
//!   cargo test -p sprig-testkit --test m05_isolation -- --ignored --nocapture
//! ```
//!
//! `SPRIG_TEST_M05_REPORT` names a file for one JSON line per check.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines
)]

use std::fmt::Write as _;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Row, SslOpts, Value};
use sprig_testkit::{require_env, skip};

const A: &str = "SPRIG_TEST_M05_A";
const B: &str = "SPRIG_TEST_M05_B";
const MYSQL_PORT: &str = "SPRIG_TEST_M05_MYSQL_PORT";
const SECOND: &str = "SPRIG_TEST_M05_SECOND";
const BACKEND: &str = "SPRIG_TEST_M05_BACKEND";
const REPORT: &str = "SPRIG_TEST_M05_REPORT";
const PW: &str = "m05-pw-1";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Persona {
    MySql,
    MariaDb,
}

/// What a statement must do.
#[derive(Clone, Copy, Debug)]
enum Expect {
    Ok,
    /// This error number on both personas.
    Err(u16),
    /// MySQL persona, MariaDB persona.
    Persona(u16, u16),
}

impl Expect {
    fn code(self, p: Persona) -> Option<u16> {
        match self {
            Expect::Ok => None,
            Expect::Err(c) => Some(c),
            Expect::Persona(m, d) => Some(if p == Persona::MySql { m } else { d }),
        }
    }
}

fn opts(url: &str, host_port: Option<&str>, user: Option<(&str, &str)>) -> Opts {
    let base = Opts::from_url(url).expect("a mysql:// URL");
    let mut b = OptsBuilder::from_opts(base.clone())
        .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true))
        .prefer_socket(false);
    if let Some(hp) = host_port {
        let (h, p) = hp.rsplit_once(':').unwrap();
        b = b.ip_or_hostname(h.to_owned()).tcp_port(p.parse().unwrap());
    }
    if let Some((u, pw)) = user {
        b = b.user(Some(u.to_owned())).pass(Some(pw.to_owned()));
    }
    Opts::from(b)
}

/// `acme.app` -> `acme.`.
fn tenant_prefix(url: &str) -> String {
    let u = Opts::from_url(url).unwrap();
    let user = u.user().unwrap().to_owned();
    let (t, _) = user.split_once('.').expect("a qualified login: <tenant>.<user>");
    format!("{t}.")
}

fn port_of(url: &str) -> u16 {
    Opts::from_url(url).unwrap().tcp_port()
}

fn host_of(url: &str) -> String {
    Opts::from_url(url).unwrap().ip_or_hostname().to_owned()
}

fn cell(v: &Value) -> String {
    match v {
        Value::NULL => "NULL".into(),
        Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        other => format!("{other:?}"),
    }
}

/// Runs `sql`; the error number (or `None`) and the rows as text.
async fn run(c: &mut Conn, sql: &str) -> (Option<u16>, String, String) {
    match c.query::<Row, _>(sql).await {
        Ok(rows) => {
            let mut t = String::new();
            for r in rows {
                let cells: Vec<String> = r.unwrap().iter().map(cell).collect();
                let _ = writeln!(t, "{}", cells.join("|"));
            }
            (None, t, String::new())
        }
        Err(mysql_async::Error::Server(s)) => (Some(s.code), String::new(), s.message),
        Err(e) => (Some(0), String::new(), e.to_string()),
    }
}

async fn admin(c: &mut Conn, sql: &str) {
    if let (Some(code), _, m) = run(c, sql).await {
        panic!("setup statement failed ({code} {m}): {sql}");
    }
}

/// Builds one tenant's objects and users (idempotent: drops first).
async fn setup(c: &mut Conn, marker: &str) {
    for s in [
        "DROP USER IF EXISTS m05_reader, m05_writer, m05_col, m05_none, m05_roleu, m05_prop",
        "DROP ROLE IF EXISTS m05_r",
        "DROP DATABASE IF EXISTS m05_shop",
        "DROP DATABASE IF EXISTS m05_crm",
        "CREATE DATABASE m05_shop",
        "CREATE DATABASE m05_crm",
        "CREATE TABLE m05_shop.orders (id INT PRIMARY KEY, total INT, note VARCHAR(40))",
        "CREATE TABLE m05_shop.items (id INT PRIMARY KEY, price INT)",
        "CREATE TABLE m05_crm.leads (id INT PRIMARY KEY, note VARCHAR(40))",
    ] {
        admin(c, s).await;
    }
    admin(
        c,
        &format!("INSERT INTO m05_shop.orders VALUES (1, 10, '{marker}')"),
    )
    .await;
    admin(c, &format!("INSERT INTO m05_crm.leads VALUES (1, '{marker}')")).await;
    for s in [
        format!("CREATE USER m05_reader IDENTIFIED BY '{PW}'"),
        format!("CREATE USER m05_writer IDENTIFIED BY '{PW}'"),
        format!("CREATE USER m05_col IDENTIFIED BY '{PW}'"),
        format!("CREATE USER m05_none IDENTIFIED BY '{PW}'"),
        format!("CREATE USER m05_roleu IDENTIFIED BY '{PW}'"),
        "GRANT SELECT ON m05_shop.* TO m05_reader".into(),
        "GRANT SELECT, INSERT ON m05_shop.orders TO m05_writer".into(),
        "GRANT SELECT (id), INSERT ON m05_shop.orders TO m05_col".into(),
        "CREATE ROLE m05_r".into(),
        "GRANT SELECT ON m05_crm.* TO m05_r".into(),
        "GRANT m05_r TO m05_reader, m05_roleu".into(),
        "SET DEFAULT ROLE m05_r FOR m05_roleu".into(),
    ] {
        admin(c, &s).await;
    }
}

async fn teardown(c: &mut Conn) {
    for s in [
        "DROP USER IF EXISTS m05_reader, m05_writer, m05_col, m05_none, m05_roleu, m05_prop",
        "DROP ROLE IF EXISTS m05_r",
        "DROP DATABASE IF EXISTS m05_shop",
        "DROP DATABASE IF EXISTS m05_crm",
    ] {
        let _ = run(c, s).await;
    }
}

/// Each user's script: statement and expectation, run in order on one
/// connection per persona.
fn scripts() -> Vec<(&'static str, Vec<(&'static str, Expect)>)> {
    use Expect::{Err, Ok, Persona as P};
    vec![
        (
            "m05_reader",
            vec![
                ("SELECT * FROM m05_shop.orders", Ok),
                ("SELECT * FROM m05_shop.items", Ok),
                ("INSERT INTO m05_shop.orders VALUES (9, 9, 'x')", Err(1142)),
                ("UPDATE m05_shop.orders SET total = 0", Err(1142)),
                ("DELETE FROM m05_shop.orders", Err(1142)),
                ("SELECT * FROM m05_crm.leads", Err(1142)),
                ("USE m05_crm", Err(1044)),
                ("USE m05_shop", Ok),
                ("SELECT COUNT(*) FROM orders", Ok),
                ("CREATE TABLE m05_shop.x (a INT)", Err(1142)),
                ("DROP TABLE m05_shop.items", Err(1142)),
                ("ALTER TABLE m05_shop.items ADD COLUMN y INT", Err(1142)),
                ("CREATE INDEX ix ON m05_shop.items (price)", Err(1142)),
                ("DROP DATABASE m05_shop", Err(1044)),
                ("CREATE DATABASE m05_other", Err(1044)),
                ("CREATE USER m05_evil IDENTIFIED BY 'x'", Err(1227)),
                ("DROP USER m05_none", Err(1227)),
                ("ALTER USER m05_none IDENTIFIED BY 'x'", Err(1227)),
                ("GRANT SELECT ON m05_crm.* TO m05_reader", Err(1044)),
                ("GRANT ALL ON *.* TO m05_reader", Err(1045)),
                ("GRANT SELECT ON m05_shop.orders TO m05_none", Err(1142)),
                ("GRANT m05_r TO m05_none", Err(1227)),
                ("SHOW GRANTS FOR m05_writer", P(1142, 1044)),
                ("SHOW GRANTS", Ok),
                ("ALTER TABLE m05_shop.orders SPRIG SET (reads.max_replica_lag_ms = 500)", Err(1142)),
                ("ALTER DATABASE m05_shop SPRIG SET (reads.max_replica_lag_ms = 500)", Err(1044)),
                ("ALTER TENANT SPRIG SET (reads.max_replica_lag_ms = 500)", Err(1227)),
                ("SET ROLE m05_r", Ok),
                ("SELECT * FROM m05_crm.leads", Ok),
                ("SET ROLE NONE", Ok),
                ("SELECT * FROM m05_crm.leads", Err(1142)),
                ("SELECT * FROM mysql.user", P(1142, 1142)),
            ],
        ),
        (
            "m05_writer",
            vec![
                ("INSERT INTO m05_shop.orders VALUES (2, 20, 'w')", Ok),
                ("SELECT * FROM m05_shop.orders", Ok),
                ("DELETE FROM m05_shop.orders WHERE id = 2", Err(1142)),
                ("UPDATE m05_shop.orders SET total = 1", Err(1142)),
                ("SELECT * FROM m05_shop.items", Err(1142)),
                ("INSERT INTO m05_shop.items VALUES (1, 1)", Err(1142)),
                (
                    "INSERT INTO m05_shop.orders SELECT id + 100, price, 'x' FROM m05_shop.items",
                    Err(1142),
                ),
                ("USE m05_shop", Ok),
                ("USE m05_crm", Err(1044)),
            ],
        ),
        (
            "m05_col",
            vec![
                ("SELECT id FROM m05_shop.orders", Ok),
                ("SELECT total FROM m05_shop.orders", Err(1143)),
                ("SELECT * FROM m05_shop.orders", Err(1142)),
                ("UPDATE m05_shop.orders SET total = 1", Err(1142)),
                ("SELECT * FROM m05_shop.items", Err(1142)),
            ],
        ),
        (
            "m05_none",
            vec![
                ("SELECT * FROM m05_shop.orders", Err(1142)),
                ("SELECT * FROM m05_crm.leads", Err(1142)),
                ("USE m05_shop", Err(1044)),
                ("INSERT INTO m05_crm.leads VALUES (5, 'x')", Err(1142)),
                ("CREATE TABLE m05_shop.y (a INT)", Err(1142)),
                ("SHOW GRANTS", Ok),
                ("SET ROLE m05_r", P(3530, 1959)),
            ],
        ),
        (
            "m05_roleu",
            vec![
                ("SELECT * FROM m05_crm.leads", Ok),
                ("SELECT * FROM m05_shop.orders", Err(1142)),
                ("SET ROLE NONE", Ok),
                ("SELECT * FROM m05_crm.leads", Err(1142)),
                ("SET ROLE DEFAULT", Ok),
                ("SELECT * FROM m05_crm.leads", Ok),
            ],
        ),
    ]
}

struct Report(Option<std::fs::File>);

impl Report {
    fn line(&mut self, v: &serde_json::Value) {
        if let Some(f) = &mut self.0 {
            use std::io::Write as _;
            let _ = writeln!(f, "{v}");
        }
    }
}

/// The schema of `table` holding `marker`, found through the backend.
async fn schema_with(backend: &mut Conn, table: &str, marker: &str) -> Option<String> {
    let schemas: Vec<String> = backend
        .exec(
            "SELECT TABLE_SCHEMA FROM information_schema.TABLES \
             WHERE TABLE_NAME = ? AND TABLE_SCHEMA LIKE 't%\\_d%'",
            (table,),
        )
        .await
        .unwrap();
    for s in schemas {
        let found: Option<u64> = backend
            .exec_first(
                format!("SELECT COUNT(*) FROM `{s}`.`{table}` WHERE note = ?"),
                (marker,),
            )
            .await
            .unwrap_or(None);
        if found.unwrap_or(0) > 0 {
            return Some(s);
        }
    }
    None
}

/// Polls `sql` on `c` until it fails with `code`; the time it took.
async fn until_refused(c: &mut Conn, sql: &str, code: u16, limit: Duration) -> Option<Duration> {
    let start = Instant::now();
    while start.elapsed() < limit {
        if let (Some(got), _, _) = run(c, sql).await
            && got == code
        {
            return Some(start.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    None
}

#[tokio::test]
#[ignore = "needs two tenants' admin endpoints: SPRIG_TEST_M05_A and SPRIG_TEST_M05_B"]
async fn m05_isolation_suite() {
    let a_url = require_env!(A);
    let b_url = require_env!(B);
    let mysql_port = skip::var(MYSQL_PORT);
    let mut report = Report(
        skip::var(REPORT).map(|p| std::fs::File::create(p).expect("report file")),
    );
    let tenants = [("A", a_url.clone()), ("B", b_url.clone())];
    let markers = ["m05-marker-tenant-a-77", "m05-marker-tenant-b-88"];
    for ((_, url), marker) in tenants.iter().zip(markers) {
        let mut c = Conn::new(opts(url, None, None)).await.expect("admin login");
        setup(&mut c, marker).await;
        c.disconnect().await.unwrap();
    }
    let mut failures = Vec::new();
    let mut checks = 0usize;
    let host = host_of(&a_url);
    let mut endpoints = vec![(Persona::MariaDb, format!("{host}:{}", port_of(&a_url)))];
    if let Some(p) = &mysql_port {
        endpoints.push((Persona::MySql, format!("{host}:{p}")));
    }
    for ((tname, url), marker) in tenants.iter().zip(markers) {
        let other = markers.iter().find(|m| **m != marker).unwrap();
        let prefix = tenant_prefix(url);
        for (persona, hp) in &endpoints {
            for (user, script) in scripts() {
                let login = format!("{prefix}{user}");
                let mut c = Conn::new(opts(url, Some(hp), Some((&login, PW))))
                    .await
                    .unwrap_or_else(|e| panic!("login {login} on {hp}: {e}"));
                for (sql, want) in script {
                    checks += 1;
                    let (got, text, msg) = run(&mut c, sql).await;
                    let expected = want.code(*persona);
                    let ok = got == expected && !text.contains(other) && !msg.contains(other);
                    report.line(&serde_json::json!({
                        "tenant": tname, "persona": format!("{persona:?}"), "user": user,
                        "sql": sql, "expected": expected, "got": got, "message": msg, "pass": ok,
                    }));
                    if !ok {
                        failures.push(format!(
                            "{tname} {persona:?} {user}: {sql}: expected {expected:?}, got {got:?} {msg}"
                        ));
                    }
                }
                let _ = c.disconnect().await;
            }
            // Wrong password.
            checks += 1;
            let bad = Conn::new(opts(url, Some(hp), Some((&format!("{prefix}m05_reader"), "nope"))))
                .await;
            let code = match bad {
                Err(mysql_async::Error::Server(s)) => Some(s.code),
                Ok(_) => None,
                Err(_) => Some(0),
            };
            if code != Some(1045) {
                failures.push(format!("{tname} {persona:?}: wrong password gave {code:?}"));
            }
        }
    }

    // Across tenants: A's administrator against B's physical names.
    if let Some(burl) = skip::var(BACKEND) {
        let mut backend = Conn::new(opts(&burl, None, None)).await.expect("backend root");
        let b_schema = schema_with(&mut backend, "orders", markers[1]).await.expect("B's schema");
        let a_schema = schema_with(&mut backend, "orders", markers[0]).await.expect("A's schema");
        let mut a = Conn::new(opts(&a_url, None, None)).await.unwrap();
        let upper = b_schema.to_uppercase();
        let probes = [
            format!("SELECT * FROM {b_schema}.orders"),
            format!("SELECT * FROM `{b_schema}`.`orders`"),
            format!("SELECT * FROM {upper}.orders"),
            format!("USE {b_schema}"),
            format!("GRANT SELECT ON {b_schema}.* TO m05_reader"),
            format!("GRANT SELECT ON `{b_schema}`.`orders` TO m05_reader"),
            format!("INSERT INTO {b_schema}.orders VALUES (50, 1, 'x')"),
            format!("SELECT * FROM m05_shop.orders UNION SELECT * FROM {b_schema}.orders"),
            format!("CREATE VIEW m05_shop.v_b AS SELECT * FROM {b_schema}.orders"),
        ];
        for p in &probes {
            checks += 1;
            let (got, text, msg) = run(&mut a, p).await;
            let ok = got == Some(1049) && !text.contains(markers[1]) && !msg.contains(&b_schema);
            report.line(&serde_json::json!({"probe": p, "got": got, "message": msg, "pass": ok}));
            if !ok {
                failures.push(format!("cross-tenant probe {p}: got {got:?} {msg}"));
            }
        }
        // Reads that list things must not show B.
        for p in [
            "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES",
            "SELECT * FROM information_schema.SCHEMA_PRIVILEGES",
            "SELECT * FROM information_schema.TABLE_PRIVILEGES",
            "SHOW DATABASES",
        ] {
            checks += 1;
            let (_, text, _) = run(&mut a, p).await;
            if text.contains(&b_schema) || text.contains(markers[1]) {
                failures.push(format!("{p} shows tenant B"));
            }
        }
        let _ = a.disconnect().await;

        // MariaDB's grant tables: each tenant's accounts reach only its own
        // schemas, and hold no global privilege.
        for schema in [&a_schema, &b_schema] {
            let tid = schema
                .strip_prefix('t')
                .and_then(|s| s.split('_').next())
                .unwrap()
                .to_owned();
            let like = format!("t{tid}\\_%");
            let own = format!("t{tid}\\_");
            let own_plain = format!("t{tid}_");
            let dbs: Vec<(String, String)> = backend
                .exec("SELECT User, Db FROM mysql.db WHERE User LIKE ?", (&like,))
                .await
                .unwrap();
            let tables: Vec<(String, String)> = backend
                .exec(
                    "SELECT User, Db FROM mysql.tables_priv WHERE User LIKE ? \
                     UNION ALL SELECT User, Db FROM mysql.columns_priv WHERE User LIKE ? \
                     UNION ALL SELECT User, Db FROM mysql.procs_priv WHERE User LIKE ?",
                    (&like, &like, &like),
                )
                .await
                .unwrap();
            let global: Vec<String> = backend
                .exec(
                    "SELECT User FROM mysql.global_priv WHERE User LIKE ? \
                     AND JSON_VALUE(Priv, '$.access') <> '0'",
                    (&like,),
                )
                .await
                .unwrap();
            checks += 3;
            for (u, d) in &dbs {
                if !d.starts_with(&own) || d.contains("sprig") {
                    failures.push(format!("backend account {u} granted on database {d}"));
                }
            }
            for (u, d) in &tables {
                if !d.starts_with(&own_plain) || d.ends_with("_sprig") {
                    failures.push(format!("backend account {u} granted on table in {d}"));
                }
            }
            for u in &global {
                failures.push(format!("backend account {u} holds a global privilege"));
            }
            println!(
                "backend audit t{tid}: {} database grants, {} table/column/routine grants, {} global",
                dbs.len(),
                tables.len(),
                global.len()
            );
        }
        let _ = backend.disconnect().await;
    }

    // Propagation to a second gateway.
    if let Some(second) = skip::var(SECOND) {
        let prefix = tenant_prefix(&a_url);
        let mut adm = Conn::new(opts(&a_url, None, None)).await.unwrap();
        admin(&mut adm, &format!("CREATE USER m05_prop IDENTIFIED BY '{PW}'")).await;
        let login = format!("{prefix}m05_prop");
        for (level, grant, revoke, probe, code) in [
            (
                "table",
                "GRANT SELECT ON m05_shop.orders TO m05_prop",
                "REVOKE SELECT ON m05_shop.orders FROM m05_prop",
                "SELECT COUNT(*) FROM m05_shop.orders",
                1142u16,
            ),
            (
                "database",
                "GRANT SELECT ON m05_shop.* TO m05_prop",
                "REVOKE SELECT ON m05_shop.* FROM m05_prop",
                "SELECT COUNT(*) FROM orders",
                1142u16,
            ),
        ] {
            let t0 = Instant::now();
            admin(&mut adm, grant).await;
            let mut s = Conn::new(opts(&a_url, Some(&second), Some((&login, PW))))
                .await
                .unwrap();
            // The grant reaching the second gateway.
            let mut granted_after = None;
            while t0.elapsed() < Duration::from_secs(10) {
                if level == "database" {
                    let _ = run(&mut s, "USE m05_shop").await;
                }
                if run(&mut s, probe).await.0.is_none() {
                    granted_after = Some(t0.elapsed());
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            let granted_after = granted_after
                .unwrap_or_else(|| panic!("{level} grant never reached the second gateway"));
            for _ in 0..20 {
                assert!(run(&mut s, probe).await.0.is_none());
            }
            let t1 = Instant::now();
            admin(&mut adm, revoke).await;
            let revoke_returned = t1.elapsed();
            let refused = until_refused(&mut s, probe, code, Duration::from_secs(10)).await;
            let total = t1.elapsed();
            checks += 1;
            let took = refused.map(|_| total);
            println!(
                "propagation ({level}): grant seen on the second gateway after {granted_after:?}; \
                 REVOKE returned after {revoke_returned:?}; refused on the second gateway \
                 {took:?} after the REVOKE was sent"
            );
            report.line(&serde_json::json!({
                "propagation": level,
                "grant_ms": granted_after.as_secs_f64() * 1000.0,
                "revoke_returned_ms": revoke_returned.as_secs_f64() * 1000.0,
                "refused_ms": took.map(|d| d.as_secs_f64() * 1000.0),
            }));
            match took {
                Some(d) if d < Duration::from_secs(5) => {}
                other => failures.push(format!("{level} revoke on the second gateway: {other:?}")),
            }
            let _ = s.disconnect().await;
        }
        let _ = adm.disconnect().await;
    }

    for (_, url) in &tenants {
        let mut c = Conn::new(opts(url, None, None)).await.unwrap();
        teardown(&mut c).await;
        let _ = c.disconnect().await;
    }
    println!("m05 isolation: {checks} checks, {} failures", failures.len());
    for f in &failures {
        println!("FAIL {f}");
    }
    assert!(failures.is_empty(), "{} failures", failures.len());
}
