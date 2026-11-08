//! The gateway-level leak suite of milestone M0.3 (chapter 48: "no
//! physical name appears in any result, error or `SHOW` output in the leak
//! suite"; chapter 07, "Testing" and build step 10).
//!
//! Two tenants run the same corpus through one endpoint: result sets,
//! column metadata, errors (unknown table, duplicate key, FK violations,
//! the other tenant's names, physical names typed by hand), warnings,
//! `SHOW` and `SHOW CREATE`, `information_schema`, `DATABASE()`,
//! `PROCESSLIST`, `EXPLAIN` and prepared statements. Every connection runs
//! through a small TCP proxy inside the test, and every byte the server
//! sends (handshake, OK and error packets, column definitions, rows,
//! session-state changes) is scanned with chapter 07's detector
//! (`sprig_sql::names::physical_name_leaks`, ported below). The leak
//! suite therefore needs plaintext: the dev gateway's listeners accept it
//! (TLS `optional`).
//!
//! It also checks isolation on the wire: neither tenant ever receives a byte
//! of the other's secret marker, sees the other's sessions in the process
//! list, or kills the other's session.
//!
//! ```sh
//! # Through the gateway (milestone M0.3's dev tenants):
//! SPRIG_TEST_M03_A=mysql://acme.app:sprig-dev-app@127.0.0.1:13307 \
//! SPRIG_TEST_M03_B=mysql://globex.app:sprig-dev-app@127.0.0.1:13307 \
//!   cargo test -p sprig-testkit --test m03_leaks -- --ignored --nocapture
//! # Baseline: direct MariaDB, two users with grants on their own prefix
//! # (deploy/compose/m03/run.sh creates them):
//! SPRIG_TEST_M03_BASELINE=1 \
//! SPRIG_TEST_M03_A=mysql://m03_acme:sprig-dev-app@127.0.0.1:13301 \
//! SPRIG_TEST_M03_B=mysql://m03_globex:sprig-dev-app@127.0.0.1:13301 \
//!   cargo test -p sprig-testkit --test m03_leaks -- --ignored --nocapture
//! ```
//!
//! In baseline mode database names get the prefix `<tag>_` (the user name
//! with every non-alphanumeric character as `_`) and the leak verdict is
//! n/a: no physical names exist. Instead the test asserts that the
//! detector caught the physical-looking probe names MariaDB echoes in its
//! errors, which proves the byte capture end to end.
//!
//! `SPRIG_TEST_M03_BACKEND` (a direct root URL of the tenants' cluster)
//! makes the probes the cluster's real physical schema names rather than
//! made-up ones. `SPRIG_TEST_M03_REPORT` names a file for one JSON line per
//! check (`deploy/compose/m03/run.sh` collects them).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines
)]

use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Row, Value};
use sprig_testkit::{require_env, skip};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const A: &str = "SPRIG_TEST_M03_A";
const B: &str = "SPRIG_TEST_M03_B";
const BASELINE: &str = "SPRIG_TEST_M03_BASELINE";
const BACKEND: &str = "SPRIG_TEST_M03_BACKEND";
const REPORT: &str = "SPRIG_TEST_M03_REPORT";

// --- the detector (chapter 07) ---------------------------------------------------

/// `sprig_sql::names::physical_name_leaks`, ported byte for byte (the
/// testkit doesn't depend on `sprig-sql`): every `t<digits>_` not preceded
/// by a letter or digit, up to the end of the identifier.
fn physical_name_leaks(bytes: &[u8]) -> Vec<String> {
    fn is_ident_byte(c: u8) -> bool {
        c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80
    }
    let mut found = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let starts = bytes[i] == b't'
            && (i == 0 || !bytes[i - 1].is_ascii_alphanumeric())
            && bytes.get(i + 1).is_some_and(u8::is_ascii_digit);
        if starts {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if bytes.get(j) == Some(&b'_') {
                let mut k = j;
                while k < bytes.len() && is_ident_byte(bytes[k]) {
                    k += 1;
                }
                found.push(String::from_utf8_lossy(&bytes[i..k]).into_owned());
                i = k;
                continue;
            }
        }
        i += 1;
    }
    found
}

#[test]
fn detector_matches_chapter_07() {
    assert_eq!(
        physical_name_leaks(b"Tables_in_t42_d1_g1_s1"),
        ["t42_d1_g1_s1"]
    );
    assert_eq!(
        physical_name_leaks(b"`t7_sprig`.`x` and 't12_d3_ref'"),
        ["t7_sprig", "t12_d3_ref"]
    );
    assert!(physical_name_leaks(b"shop.t1 tx_1 st1_x t_1 at42_d1").is_empty());
}

// --- a proxy that keeps every byte the server sends ------------------------------

/// Server-to-client bytes, per proxied connection, since the last `take`.
#[derive(Clone, Default)]
struct Wire(Arc<Mutex<Vec<Vec<u8>>>>);

impl Wire {
    fn take(&self) -> Vec<Vec<u8>> {
        let mut g = self.0.lock().unwrap();
        g.iter_mut().map(std::mem::take).collect()
    }
}

/// Listens on a loopback port and forwards each connection to `upstream`,
/// recording what the server sends. Bytes are recorded before they are
/// forwarded, so a reply the driver has read is always in the record.
async fn start_proxy(upstream: String) -> (u16, Wire) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let wire = Wire::default();
    let w = wire.clone();
    tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            let up = upstream.clone();
            let w = w.clone();
            tokio::spawn(async move {
                let Ok(server) = TcpStream::connect(&up).await else {
                    return;
                };
                let idx = {
                    let mut g = w.0.lock().unwrap();
                    g.push(Vec::new());
                    g.len() - 1
                };
                let (mut cr, mut cw) = client.into_split();
                let (mut sr, mut sw) = server.into_split();
                let c2s = tokio::spawn(async move {
                    let _ = tokio::io::copy(&mut cr, &mut sw).await;
                    let _ = sw.shutdown().await;
                });
                let mut buf = vec![0u8; 64 * 1024];
                loop {
                    let n = match sr.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };
                    w.0.lock().unwrap()[idx].extend_from_slice(&buf[..n]);
                    if cw.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
                let _ = cw.shutdown().await;
                c2s.abort();
            });
        }
    });
    (port, wire)
}

// --- tenants -------------------------------------------------------------------

fn tag_of(user: &str) -> String {
    user.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

struct Tenant {
    tag: String,
    /// Database prefix: `<tag>_` in baseline mode, empty through the gateway.
    prefix: String,
    opts: Opts,
    proxy_port: u16,
    wire: Wire,
    conn: Conn,
    /// A value only this tenant ever stores.
    secret: String,
}

impl Tenant {
    async fn open(url: &str, baseline: bool, nonce: &str) -> Self {
        let opts = Opts::from_url(url).unwrap_or_else(|e| panic!("{url}: {e}"));
        let user = opts.user().expect("the URL names a user").to_owned();
        let tag = tag_of(&user);
        let upstream = format!("{}:{}", opts.ip_or_hostname(), opts.tcp_port());
        let (proxy_port, wire) = start_proxy(upstream).await;
        let conn = Self::connect_to(&opts, proxy_port).await;
        Tenant {
            prefix: if baseline {
                format!("{tag}_")
            } else {
                String::new()
            },
            secret: format!("{tag}-secret-{nonce}"),
            tag,
            opts,
            proxy_port,
            wire,
            conn,
        }
    }

    async fn connect_to(opts: &Opts, port: u16) -> Conn {
        let b = OptsBuilder::from_opts(opts.clone())
            .ip_or_hostname("127.0.0.1")
            .tcp_port(port)
            .prefer_socket(false)
            .db_name(None::<String>);
        Conn::new(b)
            .await
            .unwrap_or_else(|e| panic!("connect through the proxy to {opts:?}: {e}"))
    }

    /// Another session of the same tenant, through the same proxy.
    async fn session(&self) -> Conn {
        Self::connect_to(&self.opts, self.proxy_port).await
    }

    fn db(&self) -> String {
        format!("{}m03_leaks", self.prefix)
    }

    /// A database only this tenant has, even through the gateway.
    fn vault(&self) -> String {
        format!("{}_vault_{}", self.db(), self.tag)
    }
}

/// What one statement returned.
#[derive(Default)]
struct Outcome {
    errno: Option<u16>,
    message: String,
    /// Every row (and warning) as text, `|`-separated.
    text: String,
    rows: usize,
    columns: usize,
    leaks: Vec<String>,
}

fn cell(v: &Value) -> String {
    match v {
        Value::NULL => "NULL".to_owned(),
        Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        other => other.as_sql(true),
    }
}

/// Runs `sql` (text protocol), reads every result set and, if the server
/// reports warnings, `SHOW WARNINGS`. Column definitions are checked field
/// by field, and every byte the server sent meanwhile is scanned.
async fn run(conn: &mut Conn, wire: &Wire, sql: &str) -> Outcome {
    wire.take();
    let mut out = Outcome::default();
    let mut col_leaks = Vec::new();
    let res: mysql_async::Result<()> = async {
        let mut qr = conn.query_iter(sql).await?;
        loop {
            if let Some(cols) = qr.columns() {
                out.columns += cols.len();
                for c in cols.iter() {
                    for f in [
                        c.schema_ref(),
                        c.table_ref(),
                        c.org_table_ref(),
                        c.name_ref(),
                        c.org_name_ref(),
                    ] {
                        col_leaks.extend(physical_name_leaks(f));
                    }
                }
            }
            let set: Vec<Row> = qr.collect().await?;
            for r in set {
                out.rows += 1;
                let cells: Vec<String> = r.unwrap().iter().map(cell).collect();
                let _ = writeln!(out.text, "{}", cells.join("|"));
            }
            if qr.is_empty() {
                break;
            }
        }
        Ok(())
    }
    .await;
    if let Err(e) = res {
        if let mysql_async::Error::Server(s) = &e {
            out.errno = Some(s.code);
            out.message.clone_from(&s.message);
        } else {
            out.errno = Some(0);
            out.message = e.to_string();
        }
    }
    if conn.get_warnings() > 0
        && let Ok(rows) = conn.query::<Row, _>("SHOW WARNINGS").await
    {
        for r in rows {
            let cells: Vec<String> = r.unwrap().iter().map(cell).collect();
            let _ = writeln!(out.text, "warning: {}", cells.join("|"));
        }
    }
    for chunk in wire.take() {
        out.leaks.extend(physical_name_leaks(&chunk));
    }
    out.leaks.extend(col_leaks);
    out.leaks.sort();
    out.leaks.dedup();
    out
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Expect {
    Ok,
    Err,
    /// Either; only the bytes are checked.
    Any,
}

/// Each tenant's objects: same logical names through the gateway.
const SETUP: &[&str] = &[
    "DROP DATABASE IF EXISTS {db}",
    "DROP DATABASE IF EXISTS {vault}",
    "CREATE DATABASE {db}",
    "CREATE TABLE {db}.parent (id INT PRIMARY KEY, name VARCHAR(60) NOT NULL, \
     UNIQUE KEY uq_parent_name (name)) ENGINE = InnoDB",
    "CREATE TABLE {db}.child (id INT AUTO_INCREMENT PRIMARY KEY, parent_id INT NOT NULL, \
     note VARCHAR(80), KEY ix_child_note (note), \
     CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) REFERENCES {db}.parent (id)) ENGINE = InnoDB",
    "CREATE TABLE {db}.audit (id INT AUTO_INCREMENT PRIMARY KEY, msg VARCHAR(80)) ENGINE = InnoDB",
    "CREATE TRIGGER {db}.trg_child_ai AFTER INSERT ON {db}.child FOR EACH ROW \
     INSERT INTO {db}.audit (msg) VALUES (CONCAT('child ', NEW.id))",
    "CREATE VIEW {db}.v_child AS SELECT c.id, c.note, p.name FROM {db}.child c \
     JOIN {db}.parent p ON p.id = c.parent_id",
    "CREATE PROCEDURE {db}.p_count(OUT n INT) BEGIN \
     SELECT COUNT(*) INTO n FROM {db}.child; SELECT n AS counted, DATABASE() AS db; END",
    "CREATE FUNCTION {db}.f_double(x INT) RETURNS INT DETERMINISTIC RETURN x * 2",
    "CREATE TABLE {db}.gone (x INT)",
    "CREATE VIEW {db}.v_broken AS SELECT x FROM {db}.gone",
    "DROP TABLE {db}.gone",
    "INSERT INTO {db}.parent VALUES (1, '{tag}-one'), (2, '{tag}-two'), (3, '{secret}')",
    "INSERT INTO {db}.child (parent_id, note) VALUES (1, '{tag}-a'), (2, '{tag}-b')",
    "CREATE DATABASE {vault}",
    "CREATE TABLE {vault}.secrets (id INT PRIMARY KEY, v VARCHAR(80))",
    "INSERT INTO {vault}.secrets VALUES (1, '{secret}')",
];

/// The corpus: label, statement, expectation.
const CORPUS: &[(&str, &str, Expect)] = &[
    // Session and results
    ("use", "USE {db}", Expect::Ok),
    ("database()", "SELECT DATABASE(), SCHEMA()", Expect::Ok),
    ("select unqualified", "SELECT * FROM child", Expect::Ok),
    (
        "select join",
        "SELECT c.id, c.note, p.name FROM {db}.child c JOIN {db}.parent p ON p.id = c.parent_id",
        Expect::Ok,
    ),
    ("select view", "SELECT * FROM {db}.v_child", Expect::Ok),
    ("function", "SELECT {db}.f_double(21)", Expect::Ok),
    ("call", "CALL {db}.p_count(@n)", Expect::Ok),
    ("out param", "SELECT @n", Expect::Ok),
    ("current user", "SELECT CURRENT_USER(), USER()", Expect::Ok),
    // SHOW
    ("show databases", "SHOW DATABASES", Expect::Ok),
    ("show tables", "SHOW TABLES", Expect::Ok),
    ("show full tables", "SHOW FULL TABLES FROM {db}", Expect::Ok),
    (
        "show table status",
        "SHOW TABLE STATUS FROM {db}",
        Expect::Ok,
    ),
    ("show columns", "SHOW COLUMNS FROM {db}.child", Expect::Ok),
    (
        "show full columns",
        "SHOW FULL COLUMNS FROM child",
        Expect::Ok,
    ),
    ("show index", "SHOW INDEX FROM {db}.child", Expect::Ok),
    (
        "show create table",
        "SHOW CREATE TABLE {db}.child",
        Expect::Ok,
    ),
    (
        "show create view",
        "SHOW CREATE VIEW {db}.v_child",
        Expect::Ok,
    ),
    (
        "show create procedure",
        "SHOW CREATE PROCEDURE {db}.p_count",
        Expect::Ok,
    ),
    (
        "show create function",
        "SHOW CREATE FUNCTION {db}.f_double",
        Expect::Ok,
    ),
    (
        "show create trigger",
        "SHOW CREATE TRIGGER {db}.trg_child_ai",
        Expect::Ok,
    ),
    (
        "show create database",
        "SHOW CREATE DATABASE {db}",
        Expect::Ok,
    ),
    (
        "show procedure status",
        "SHOW PROCEDURE STATUS WHERE Db = '{db}'",
        Expect::Ok,
    ),
    (
        "show function status",
        "SHOW FUNCTION STATUS WHERE Db = '{db}'",
        Expect::Ok,
    ),
    ("show triggers", "SHOW TRIGGERS FROM {db}", Expect::Ok),
    (
        "show open tables",
        "SHOW OPEN TABLES FROM {db}",
        Expect::Any,
    ),
    ("show grants", "SHOW GRANTS", Expect::Any),
    // information_schema
    (
        "is schemata",
        "SELECT * FROM information_schema.SCHEMATA",
        Expect::Ok,
    ),
    (
        "is tables (all)",
        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES",
        Expect::Ok,
    ),
    (
        "is tables",
        "SELECT * FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{db}'",
        Expect::Ok,
    ),
    (
        "is columns",
        "SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '{db}'",
        Expect::Ok,
    ),
    (
        "is statistics",
        "SELECT * FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '{db}'",
        Expect::Ok,
    ),
    (
        "is key_column_usage",
        "SELECT * FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '{db}'",
        Expect::Ok,
    ),
    (
        "is referential_constraints",
        "SELECT * FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = '{db}'",
        Expect::Ok,
    ),
    (
        "is table_constraints",
        "SELECT * FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = '{db}'",
        Expect::Ok,
    ),
    (
        "is views",
        "SELECT * FROM information_schema.VIEWS WHERE TABLE_SCHEMA = '{db}'",
        Expect::Ok,
    ),
    (
        "is routines",
        "SELECT * FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = '{db}'",
        Expect::Ok,
    ),
    (
        "is parameters",
        "SELECT * FROM information_schema.PARAMETERS WHERE SPECIFIC_SCHEMA = '{db}'",
        Expect::Ok,
    ),
    (
        "is triggers",
        "SELECT * FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = '{db}'",
        Expect::Ok,
    ),
    (
        "is partitions",
        "SELECT * FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA = '{db}'",
        Expect::Ok,
    ),
    (
        "is schema_privileges",
        "SELECT * FROM information_schema.SCHEMA_PRIVILEGES",
        Expect::Any,
    ),
    // EXPLAIN (and the rewritten statement MariaDB and MySQL print as a note)
    (
        "explain",
        "EXPLAIN SELECT c.note, p.name FROM {db}.child c JOIN {db}.parent p ON p.id = c.parent_id WHERE c.id > 0",
        Expect::Ok,
    ),
    (
        "explain extended",
        "EXPLAIN EXTENDED SELECT c.note, p.name FROM {db}.child c JOIN {db}.parent p ON p.id = c.parent_id",
        Expect::Any,
    ),
    (
        "explain json",
        "EXPLAIN FORMAT=JSON SELECT c.note, p.name FROM {db}.child c JOIN {db}.parent p ON p.id = c.parent_id WHERE p.name > ''",
        Expect::Ok,
    ),
    // Warnings
    (
        "warning: insert ignore",
        "INSERT IGNORE INTO {db}.parent VALUES (1, 'dup')",
        Expect::Ok,
    ),
    (
        "warning: drop if exists",
        "DROP TABLE IF EXISTS {db}.nope",
        Expect::Ok,
    ),
    (
        "warning: create if not exists",
        "CREATE TABLE IF NOT EXISTS {db}.parent (id INT)",
        Expect::Ok,
    ),
    (
        "warning: drop view if exists",
        "DROP VIEW IF EXISTS {db}.nope_view",
        Expect::Ok,
    ),
    // Errors caused on purpose
    ("err unknown table", "SELECT * FROM {db}.nope", Expect::Err),
    (
        "err unknown column",
        "SELECT nope FROM {db}.child",
        Expect::Err,
    ),
    (
        "err qualified column",
        "SELECT {db}.child.nope FROM {db}.child",
        Expect::Err,
    ),
    (
        "err duplicate key (primary)",
        "INSERT INTO {db}.parent VALUES (1, 'dup')",
        Expect::Err,
    ),
    (
        "err duplicate key (unique)",
        "INSERT INTO {db}.parent VALUES (9, '{tag}-one')",
        Expect::Err,
    ),
    (
        "err fk insert",
        "INSERT INTO {db}.child (parent_id, note) VALUES (999, 'orphan')",
        Expect::Err,
    ),
    (
        "err fk delete",
        "DELETE FROM {db}.parent WHERE id = 1",
        Expect::Err,
    ),
    (
        "err fk update",
        "UPDATE {db}.parent SET id = 50 WHERE id = 1",
        Expect::Err,
    ),
    (
        "err table exists",
        "CREATE TABLE {db}.parent (id INT)",
        Expect::Err,
    ),
    ("err drop unknown", "DROP TABLE {db}.nope", Expect::Err),
    ("err unknown procedure", "CALL {db}.nope()", Expect::Err),
    ("err unknown function", "SELECT {db}.nope_fn()", Expect::Err),
    ("err unknown database", "USE {db}_missing", Expect::Err),
    (
        "err show create unknown",
        "SHOW CREATE TABLE {db}.nope",
        Expect::Err,
    ),
    (
        "err broken view",
        "SELECT * FROM {db}.v_broken",
        Expect::Err,
    ),
    ("err syntax", "SELEC * FROM {db}.child", Expect::Err),
    (
        "err syntax later",
        "SELECT * FROM {db}.child WHERE id = = 1",
        Expect::Err,
    ),
    (
        "err ambiguous",
        "SELECT id FROM {db}.child JOIN {db}.parent ON 1 = 1",
        Expect::Err,
    ),
    // SprigDB's settings view is literal rows: never stored in a definition
    // (chapter 07 rule 9: 39051 through the gateway; MariaDB has no such
    // table, so baseline fails too). Reading it is fine (an error in
    // baseline). A procedure body isn't checked here: plain MariaDB would
    // store it unchecked.
    (
        "settings view",
        "SELECT SETTING_NAME, VALUE FROM information_schema.SPRIG_SETTINGS LIMIT 3",
        Expect::Any,
    ),
    (
        "err settings view in a view",
        "CREATE VIEW {db}.v_settings AS SELECT * FROM information_schema.SPRIG_SETTINGS",
        Expect::Err,
    ),
    (
        "err settings view in create select",
        "CREATE TABLE {db}.t_settings AS SELECT * FROM information_schema.SPRIG_SETTINGS",
        Expect::Err,
    ),
    // The other tenant's names (the same logical names through the gateway:
    // only its private database differs)
    ("other: use vault", "USE {other_vault}", Expect::Err),
    (
        "other: select vault",
        "SELECT * FROM {other_vault}.secrets",
        Expect::Err,
    ),
    (
        "other: show tables vault",
        "SHOW TABLES FROM {other_vault}",
        Expect::Err,
    ),
    (
        "other: select its db",
        "SELECT name FROM {other_db}.parent",
        Expect::Any,
    ),
];

/// One finding per statement, for the printed table.
struct Line {
    tenant: String,
    label: String,
    expect: Expect,
    errno: Option<u16>,
    rows: usize,
    leaks: Vec<String>,
    foreign_secret: bool,
    problem: Option<String>,
}

async fn run_line(
    t: &mut Tenant,
    other_secret: &str,
    label: &str,
    sql: &str,
    expect: Expect,
    lines: &mut Vec<Line>,
) -> Outcome {
    let out = run(&mut t.conn, &t.wire, sql).await;
    let problem = match (expect, out.errno) {
        (Expect::Ok, Some(e)) => Some(format!("failed with {e}: {}", out.message)),
        (Expect::Err, None) => Some(format!("succeeded ({} rows)", out.rows)),
        _ => None,
    };
    lines.push(Line {
        tenant: t.tag.clone(),
        label: label.to_owned(),
        expect,
        errno: out.errno,
        rows: out.rows,
        leaks: out.leaks.clone(),
        foreign_secret: out.text.contains(other_secret) || out.message.contains(other_secret),
        problem,
    });
    out
}

/// One JSON line for run.sh's matrix.
fn report(file: Option<&str>, tenant: &str, check: &str, status: &str, detail: &str) {
    let line = serde_json::json!({
        "suite": "leaks", "tenant": tenant, "check": check, "status": status,
        "detail": detail.chars().take(400).collect::<String>(), "ms": 0,
    });
    println!("{line}");
    if let Some(f) = file {
        use std::io::Write;
        let mut fh = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(f)
            .unwrap_or_else(|e| panic!("{f}: {e}"));
        writeln!(fh, "{line}").unwrap();
    }
}

/// Physical schema names to type by hand: the cluster's real ones when a
/// backend URL is given, else made-up ones of the right shape.
async fn probes() -> Vec<String> {
    if let Some(url) = skip::var(BACKEND) {
        let mut c = Conn::from_url(url.as_str())
            .await
            .unwrap_or_else(|e| panic!("{BACKEND}: {e}"));
        let names: Vec<String> = c
            .query(
                "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME \
                 REGEXP '^t[0-9]+_(d[0-9]+_(g[0-9]+_s[0-9]+|ref)|sprig)$' ORDER BY 1 LIMIT 12",
            )
            .await
            .unwrap();
        c.disconnect().await.ok();
        if !names.is_empty() {
            return names;
        }
    }
    ["t1_d1_g1_s1", "t2_d1_g1_s1", "t1_sprig", "t2_sprig"]
        .map(str::to_owned)
        .to_vec()
}

#[tokio::test]
#[ignore = "needs two tenants' endpoints: SPRIG_TEST_M03_A and SPRIG_TEST_M03_B"]
async fn leak_suite() {
    let url_a = require_env!(A);
    let url_b = require_env!(B);
    let baseline = skip::flag(BASELINE);
    let report_file = skip::var(REPORT);
    let rf = report_file.as_deref();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .to_string();
    let mut tenants = [
        Tenant::open(&url_a, baseline, &nonce).await,
        Tenant::open(&url_b, baseline, &nonce).await,
    ];
    let probes = probes().await;
    let mut skipped_in_baseline = Vec::new();
    let mut lines = Vec::new();
    let started = Instant::now();

    // Setup, both tenants (scanned like everything else).
    let mut setup_failures = Vec::new();
    for i in 0..2 {
        let (t, o) = pair(&mut tenants, i);
        let other_secret = o.secret.clone();
        let (o_db, o_vault) = (o.db(), o.vault());
        for s in SETUP
            .iter()
            .filter(|s| !(baseline && needs_super_in_baseline(s)))
        {
            let sql = fill(s, t, &o_db, &o_vault);
            let out = run_line(t, &other_secret, "setup", &sql, Expect::Ok, &mut lines).await;
            if let Some(e) = out.errno {
                setup_failures.push(format!("{}: {sql}: {e} {}", t.tag, out.message));
            }
        }
    }
    assert!(
        setup_failures.is_empty(),
        "setup failed:\n{}",
        setup_failures.join("\n")
    );

    // The corpus, then the physical probes, tenant by tenant.
    let mut own_rows = Vec::new();
    for i in 0..2 {
        let (t, o) = pair(&mut tenants, i);
        let other_secret = o.secret.clone();
        let (o_db, o_vault) = (o.db(), o.vault());
        for (label, s, expect) in CORPUS {
            if baseline && needs_super_in_baseline(s) {
                skipped_in_baseline.push(*label);
                continue;
            }
            let sql = fill(s, t, &o_db, &o_vault);
            run_line(t, &other_secret, label, &sql, *expect, &mut lines).await;
        }
        // Prepared statements: the prepare response carries column definitions.
        t.wire.take();
        let db = t.db();
        let prep = async {
            let st = t
                .conn
                .prep(format!("SELECT id, note FROM {db}.child WHERE id >= ?"))
                .await?;
            let rows: Vec<Row> = t.conn.exec(&st, (0,)).await?;
            t.conn.close(st).await?;
            mysql_async::Result::Ok(rows.len())
        }
        .await;
        let prep_err = t
            .conn
            .prep(format!("SELECT * FROM {db}.nope WHERE id = ?"))
            .await
            .err();
        let mut leaks: Vec<String> = t
            .wire
            .take()
            .iter()
            .flat_map(|c| physical_name_leaks(c))
            .collect();
        leaks.dedup();
        lines.push(Line {
            tenant: t.tag.clone(),
            label: "prepared select + prepare error".into(),
            expect: Expect::Ok,
            errno: None,
            rows: prep.as_ref().copied().unwrap_or(0),
            leaks,
            foreign_secret: false,
            problem: match (&prep, &prep_err) {
                (Err(e), _) => Some(format!("prepared select failed: {e}")),
                (_, None) => Some("preparing a statement on an unknown table succeeded".into()),
                _ => None,
            },
        });
        for p in &probes {
            for s in [
                format!("USE {p}"),
                format!("SELECT * FROM {p}.parent"),
                format!("SHOW TABLES FROM {p}"),
                format!("SELECT * FROM `{p}`.`secrets`"),
            ] {
                run_line(t, &other_secret, "probe", &s, Expect::Any, &mut lines).await;
            }
        }
        // Own rows only.
        let out = run(
            &mut t.conn,
            &t.wire,
            &format!("SELECT name FROM {db}.parent ORDER BY id"),
        )
        .await;
        own_rows.push((t.tag.clone(), out.text, t.secret.clone(), other_secret));
    }

    // PROCESSLIST while both tenants have a statement running, and KILL
    // across tenants.
    let mut sleepers = Vec::new();
    for t in &tenants {
        let mut c = t.session().await;
        let sql = format!(
            "SELECT /* m03-sleeper-{} */ SLEEP(3), COUNT(*) FROM {}.child",
            t.tag,
            t.db()
        );
        sleepers.push(tokio::spawn(async move {
            let r: mysql_async::Result<Vec<Row>> = c.query(sql).await;
            c.disconnect().await.ok();
            r.map(|_| ())
        }));
    }
    tokio::time::sleep(Duration::from_millis(700)).await;
    let mut processlist = Vec::new();
    for i in 0..2 {
        let (t, o) = pair(&mut tenants, i);
        let other_secret = o.secret.clone();
        let other_mark = format!("m03-sleeper-{}", o.tag);
        let own_mark = format!("m03-sleeper-{}", t.tag);
        let mut seen_other = Vec::new();
        let mut seen_own = false;
        for (label, s) in [
            ("show processlist", "SHOW PROCESSLIST"),
            ("show full processlist", "SHOW FULL PROCESSLIST"),
            (
                "is processlist",
                "SELECT * FROM information_schema.PROCESSLIST",
            ),
        ] {
            let out = run_line(t, &other_secret, label, s, Expect::Ok, &mut lines).await;
            if out.text.contains(&other_mark) {
                seen_other.push(label);
            }
            seen_own |= out.text.contains(&own_mark);
        }
        processlist.push((t.tag.clone(), seen_other, seen_own));
    }
    for s in sleepers {
        s.await.unwrap().expect("sleeper statement");
    }
    // Tenant B tries to kill tenant A's session, and the other way round.
    let mut kills = Vec::new();
    for i in 0..2 {
        let (t, o) = pair(&mut tenants, i);
        let victim_id: Option<u64> = o.conn.query_first("SELECT CONNECTION_ID()").await.unwrap();
        let victim_id = victim_id.unwrap();
        let other_secret = o.secret.clone();
        let out = run_line(
            t,
            &other_secret,
            "kill other tenant",
            &format!("KILL {victim_id}"),
            Expect::Err,
            &mut lines,
        )
        .await;
        let alive: mysql_async::Result<Option<u8>> = o.conn.query_first("SELECT 1").await;
        kills.push((t.tag.clone(), o.tag.clone(), out.errno, alive.is_ok()));
    }

    // Clean up.
    for t in &mut tenants {
        let (db, vault) = (t.db(), t.vault());
        let _ = t
            .conn
            .query_drop(format!(
                "DROP DATABASE IF EXISTS {db}; DROP DATABASE IF EXISTS {vault}"
            ))
            .await;
    }

    // --- verdicts ---------------------------------------------------------------
    println!(
        "\n{:<12} {:<34} {:<5} {:>6} {:>5}  leaks / problem",
        "tenant", "statement", "want", "errno", "rows"
    );
    for l in &lines {
        if l.label == "setup" && l.leaks.is_empty() && l.problem.is_none() {
            continue;
        }
        println!(
            "{:<12} {:<34} {:<5} {:>6} {:>5}  {}{}{}",
            l.tenant.chars().take(12).collect::<String>(),
            l.label,
            format!("{:?}", l.expect).to_lowercase(),
            l.errno.map_or("-".to_owned(), |e| e.to_string()),
            l.rows,
            if l.leaks.is_empty() {
                String::new()
            } else {
                format!("LEAKS {:?} ", l.leaks)
            },
            if l.foreign_secret {
                "OTHER TENANT'S SECRET "
            } else {
                ""
            },
            l.problem.as_deref().unwrap_or(""),
        );
    }
    println!(
        "{} statements in {:.1}s; probes: {probes:?}",
        lines.len(),
        started.elapsed().as_secs_f64()
    );
    skipped_in_baseline.dedup();
    if !skipped_in_baseline.is_empty() {
        println!("skipped in baseline (MariaDB 1419 without SUPER): {skipped_in_baseline:?}");
    }

    let mut failed = Vec::new();
    for t in &tenants {
        let mine: Vec<&Line> = lines.iter().filter(|l| l.tenant == t.tag).collect();
        let n = mine.len();
        // Leaks.
        let leaky: Vec<String> = mine
            .iter()
            .filter(|l| !l.leaks.is_empty())
            .map(|l| format!("{}: {:?}", l.label, l.leaks))
            .collect();
        let probe_hits = mine
            .iter()
            .filter(|l| l.label == "probe" && !l.leaks.is_empty())
            .count();
        if baseline {
            // The probes' names come back in MariaDB's errors: the detector
            // must see them, or the capture is broken.
            let non_probe = leaky.iter().filter(|l| !l.starts_with("probe")).count();
            let detail = format!(
                "n/a in baseline; detector self-check: caught the echoed probe names in {probe_hits} replies, \
                 {non_probe} other statements matched"
            );
            if probe_hits == 0 {
                report(
                    rf,
                    &t.tag,
                    "leaks",
                    "fail",
                    &format!("{detail} (want > 0: the byte capture isn't working)"),
                );
                failed.push(format!("{}: detector self-check found nothing", t.tag));
            } else {
                report(rf, &t.tag, "leaks", "n/a", &detail);
            }
        } else if leaky.is_empty() {
            report(
                rf,
                &t.tag,
                "leaks",
                "pass",
                &format!("{n} statements, every server byte scanned: no physical name"),
            );
        } else {
            report(rf, &t.tag, "leaks", "fail", &leaky.join("; "));
            failed.push(format!("{}: leaks: {}", t.tag, leaky.join("; ")));
        }
        // Expected successes and errors.
        let wrong: Vec<String> = mine
            .iter()
            .filter_map(|l| l.problem.as_ref().map(|p| format!("{}: {p}", l.label)))
            .collect();
        let errnos: Vec<String> = mine
            .iter()
            .filter(|l| l.expect == Expect::Err)
            .filter_map(|l| l.errno.map(|e| e.to_string()))
            .collect();
        if wrong.is_empty() {
            report(
                rf,
                &t.tag,
                "outcomes",
                "pass",
                &format!(
                    "every statement succeeded or failed as intended; errors: {}",
                    errnos.join(" ")
                ),
            );
        } else {
            report(rf, &t.tag, "outcomes", "fail", &wrong.join("; "));
            failed.push(format!("{}: {}", t.tag, wrong.join("; ")));
        }
        // The other tenant's secret never reaches this tenant.
        let foreign: Vec<&str> = mine
            .iter()
            .filter(|l| l.foreign_secret)
            .map(|l| l.label.as_str())
            .collect();
        if foreign.is_empty() {
            report(
                rf,
                &t.tag,
                "other_secret",
                "pass",
                &format!("the other tenant's secret is in none of {n} replies"),
            );
        } else {
            report(rf, &t.tag, "other_secret", "fail", &foreign.join(", "));
            failed.push(format!("{}: saw the other's secret in {foreign:?}", t.tag));
        }
    }
    for (tag, text, secret, other_secret) in &own_rows {
        let ok = text.contains(secret) && !text.contains(other_secret.as_str());
        report(
            rf,
            tag,
            "own_rows",
            if ok { "pass" } else { "fail" },
            &format!(
                "SELECT from its m03_leaks.parent: {}",
                text.replace('\n', " ")
            ),
        );
        if !ok {
            failed.push(format!("{tag}: own rows: {text}"));
        }
    }
    for (tag, seen, own) in &processlist {
        if seen.is_empty() {
            // Its own running statement is the positive control: when it
            // shows, the Info column's text was scanned as well.
            report(
                rf,
                tag,
                "processlist",
                "pass",
                &format!(
                    "the other tenant's running statement isn't in SHOW [FULL] PROCESSLIST or \
                     information_schema.PROCESSLIST; its own {}",
                    if *own {
                        "is (Info scanned)"
                    } else {
                        "isn't shown either"
                    }
                ),
            );
        } else {
            report(rf, tag, "processlist", "fail", &format!("seen in {seen:?}"));
            failed.push(format!("{tag}: sees the other's session in {seen:?}"));
        }
    }
    for (tag, victim, errno, alive) in &kills {
        let ok = errno.is_some() && *alive;
        report(
            rf,
            tag,
            "kill_other",
            if ok { "pass" } else { "fail" },
            &format!(
                "KILL of {victim}'s session: errno {errno:?}; {victim}'s session {}",
                if *alive { "still works" } else { "is gone" }
            ),
        );
        if !ok {
            failed.push(format!(
                "{tag}: KILL of {victim}'s session: {errno:?}, alive {alive}"
            ));
        }
    }
    assert!(failed.is_empty(), "M0.3 leak suite:\n{}", failed.join("\n"));
}

/// Statements MariaDB refuses to a user without SUPER while binary logging
/// is on (1419): stored functions and triggers. Through the gateway the
/// backend account creates them; in baseline mode, whose stand-in tenants
/// are plain users of the Compose single node, they are left out rather
/// than changing `log_bin_trust_function_creators` on a shared server.
fn needs_super_in_baseline(sql: &str) -> bool {
    sql.contains("trg_child_ai") || sql.contains("f_double")
}

/// The tenant at `i` mutably and the other one.
fn pair(t: &mut [Tenant; 2], i: usize) -> (&mut Tenant, &mut Tenant) {
    let [a, b] = t;
    if i == 0 { (a, b) } else { (b, a) }
}

/// Fills `{db}`, `{vault}`, `{tag}`, `{secret}`, `{other_db}` and `{other_vault}`.
fn fill(sql: &str, t: &Tenant, other_db: &str, other_vault: &str) -> String {
    sql.replace("{db}", &t.db())
        .replace("{vault}", &t.vault())
        .replace("{tag}", &t.tag)
        .replace("{secret}", &t.secret)
        .replace("{other_db}", other_db)
        .replace("{other_vault}", other_vault)
}
