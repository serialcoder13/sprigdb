//! Milestone M0.4's scenario suite (chapter 48, "M0.4"; chapter 13 build
//! step 14), through the real gateway and the real controller, on one
//! reference topology per run.
//!
//! The tenant's statements go through the gateway as `<tenant>.app`; chapter
//! 12 records them in the tenant's catalog; the controller's `auto_index`
//! loop evaluates, validates (replica, Galera RSU or estimate-and-canary,
//! by the cluster), builds, observes and drops. The test only generates
//! load, changes settings with `SPRIG SET`, and reads the catalog's
//! `auto_index*` tables and the gateway's `SHOW INDEX` to see what happened.
//!
//! Scenarios, scaled down (tables of 30,000 to 100,000 rows; Galera tables
//! are filled 25,000 rows per transaction):
//!
//! 1. **Worked example 1** (`orders`): `customer_id = ? AND status IN (...)
//!    ORDER BY created_at DESC LIMIT ?` with a user index on `created_at`
//!    and status updates: an index starting with `customer_id` is proposed,
//!    validated and built.
//! 2. **Worked example 2** (`events`): F1 and F2 get one merged index
//!    `(account_id, kind, created_at)`; F4 (`LOWER(source) = ?`) gets no
//!    candidate, and its skipped row says `function:LOWER`.
//! 3. **Worked example 3, the `ORDER BY ... LIMIT` trap** (`t`): `(c)` for
//!    a range on `c` sends `d = 1 ORDER BY c LIMIT 10` through the whole
//!    index: rejected for the regression; the covering retry `(c, d)` is
//!    accepted and built.
//! 4. **A user-created equivalent retires ours** (`v`): after `(a)` is
//!    built, the user creates `(a, b)`: ours is retired as `superseded`.
//! 5. **Unused, user and migrated indexes** (`u`): `(a)` is built, then its
//!    query stops; on the test clock 8 days later the indexes are active,
//!    40 days later the unused one is retired as `unused`, and 42 days
//!    later dropped. The user index and the migrated one (an
//!    `index_origins` row, as a dump replay records it in R4) are never
//!    touched.
//!
//! Worked example 4 is a MongoDB collection (R5) and is not in M0.4.
//!
//! The controller must run with `SPRIGDB_CONTROLLER_AUTOINDEX_TEST_CLOCK_FILE`
//! naming the file in `SPRIG_TEST_M04_CLOCK_FILE` (the auto-index loop's
//! clock runs ahead by the seconds it holds), and the gateway with the
//! workload settings of `deploy/compose/m04/run.sh` (1-minute buckets, every
//! fingerprint watched). `deploy/compose/m04/run.sh` does all of it.
//!
//! ```sh
//! SPRIG_TEST_M04_GATEWAY=127.0.0.1:13307 SPRIG_TEST_M04_TENANT=acme:1 \
//! SPRIG_TEST_M04_CATALOG=mysql://root:sprig-dev-root@127.0.0.1:13301 \
//! SPRIG_TEST_M04_CLOCK_FILE=$PWD/deploy/compose/.gateway-dev/autoindex-clock \
//! SPRIG_TEST_M04_TOPOLOGY=single \
//!   cargo test -p sprig-testkit --test m04_scenarios -- --ignored --nocapture
//! ```
//!
//! `SPRIG_TEST_M04_REPORT` names a file for the JSON summary;
//! `SPRIG_TEST_M04_ROWS` scales the large tables (100,000);
//! `SPRIG_TEST_M04_KEEP=1` keeps the database.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::many_single_char_names,
    clippy::struct_excessive_bools
)]

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Row, SslOpts};
use serde_json::{Value as Json, json};
use sprig_testkit::{skip, unique_ident};

const PASSWORD: &str = "sprig-dev-app";
const DAY: u64 = 86_400;

struct Cfg {
    gateway: String,
    tenant: String,
    tenant_id: u64,
    catalog: String,
    clock_file: String,
    topology: String,
    rows: u64,
}

fn cfg() -> Option<Cfg> {
    let get = |k: &str| {
        let v = skip::var(k);
        if v.is_none() {
            skip::report_missing(k, "m04_scenarios");
        }
        v
    };
    let gateway = get("SPRIG_TEST_M04_GATEWAY")?;
    let tenant = get("SPRIG_TEST_M04_TENANT")?;
    let catalog = get("SPRIG_TEST_M04_CATALOG")?;
    let clock_file = get("SPRIG_TEST_M04_CLOCK_FILE")?;
    let (name, id) = tenant
        .split_once(':')
        .expect("SPRIG_TEST_M04_TENANT is name:id");
    Some(Cfg {
        gateway,
        tenant: name.to_owned(),
        tenant_id: id.parse().unwrap(),
        catalog,
        clock_file,
        topology: skip::var("SPRIG_TEST_M04_TOPOLOGY").unwrap_or_else(|| "unknown".into()),
        rows: skip::var("SPRIG_TEST_M04_ROWS").map_or(100_000, |r| r.parse().unwrap()),
    })
}

async fn gw(c: &Cfg, db: Option<&str>) -> Conn {
    let (host, port) = c.gateway.rsplit_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{}.app", c.tenant)))
        .pass(Some(PASSWORD))
        .db_name(db)
        .prefer_socket(false)
        .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true));
    Conn::new(opts).await.expect("gateway login")
}

async fn catalog(c: &Cfg) -> Conn {
    Conn::new(Opts::from_url(&c.catalog).unwrap())
        .await
        .unwrap()
}

fn set_clock(c: &Cfg, secs: u64) {
    std::fs::write(&c.clock_file, format!("{secs}\n")).unwrap();
    println!(
        "[clock] auto_index clock +{} days",
        secs as f64 / DAY as f64
    );
}

/// One automatic index row, as the catalog holds it.
#[derive(Debug, Clone)]
struct AutoRow {
    name: String,
    table: String,
    definition: String,
    state: String,
    drop_reason: Option<String>,
    validation_id: Option<u64>,
    created_at: f64,
    state_changed_at: f64,
}

async fn auto_rows(c: &Cfg, db: &str) -> Vec<AutoRow> {
    let s = format!("t{}_sprig", c.tenant_id);
    let mut q = catalog(c).await;
    let rows: Vec<Row> = q
        .exec(
            format!(
                "SELECT a.name, o.name, CAST(a.definition AS CHAR), a.state, a.drop_reason, \
                 a.validation_id, UNIX_TIMESTAMP(a.created_at), UNIX_TIMESTAMP(a.state_changed_at) \
                 FROM `{s}`.auto_indexes a JOIN `{s}`.objects o ON o.object_id = a.object_id \
                 JOIN `{s}`.`databases` d ON d.db_id = o.db_id WHERE d.name = ? \
                 ORDER BY a.index_id"
            ),
            (db,),
        )
        .await
        .unwrap();
    let _ = q.disconnect().await;
    rows.into_iter()
        .map(|mut r| AutoRow {
            name: r.take(0).unwrap(),
            table: r.take(1).unwrap(),
            definition: r.take(2).unwrap(),
            state: r.take(3).unwrap(),
            drop_reason: r.take(4).unwrap(),
            validation_id: r.take(5).unwrap(),
            created_at: r.take::<String, _>(6).unwrap().parse().unwrap(),
            state_changed_at: r.take::<String, _>(7).unwrap().parse().unwrap(),
        })
        .collect()
}

/// The parts of a stored definition, as column names.
fn parts(def: &str) -> Vec<String> {
    let v: Json = serde_json::from_str(def).unwrap();
    v["parts"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|p| p["column"].as_str().unwrap_or_default().to_owned())
                .collect()
        })
        .unwrap_or_default()
}

async fn validation(c: &Cfg, id: u64) -> Json {
    let s = format!("t{}_sprig", c.tenant_id);
    let mut q = catalog(c).await;
    let r: Option<Row> = q
        .exec_first(
            format!(
                "SELECT method, node, verdict, IFNULL(reason, ''), statements, noisy, \
                 target_p95_before, target_p95_after, worst_regression, \
                 TIMESTAMPDIFF(MICROSECOND, started_at, finished_at) / 1000 \
                 FROM `{s}`.auto_index_validations WHERE validation_id = ?"
            ),
            (id,),
        )
        .await
        .unwrap();
    let _ = q.disconnect().await;
    let Some(mut r) = r else { return Json::Null };
    json!({
        "validation_id": id,
        "method": r.take::<String, _>(0).unwrap(),
        "node": r.take::<String, _>(1).unwrap(),
        "verdict": r.take::<String, _>(2).unwrap(),
        "reason": r.take::<String, _>(3).unwrap(),
        "statements": r.take::<u64, _>(4).unwrap(),
        "noisy": r.take::<u64, _>(5).unwrap(),
        "p95_before_ms": r.take::<Option<f64>, _>(6).unwrap(),
        "p95_after_ms": r.take::<Option<f64>, _>(7).unwrap(),
        "worst_regression_pct": r.take::<Option<f64>, _>(8).unwrap(),
        "elapsed_ms": r.take::<Option<String>, _>(9).unwrap(),
    })
}

/// Validations of every candidate on `table`, oldest first.
async fn validations_on(c: &Cfg, db: &str, table: &str) -> Vec<Json> {
    let s = format!("t{}_sprig", c.tenant_id);
    let mut q = catalog(c).await;
    let ids: Vec<u64> = q
        .exec(
            format!(
                "SELECT v.validation_id FROM `{s}`.auto_index_validations v \
                 JOIN `{s}`.auto_index_candidates k ON k.candidate_id = v.candidate_id \
                 JOIN `{s}`.objects o ON o.object_id = k.object_id \
                 JOIN `{s}`.`databases` d ON d.db_id = o.db_id \
                 WHERE d.name = ? AND o.name = ? ORDER BY v.validation_id"
            ),
            (db, table),
        )
        .await
        .unwrap();
    let _ = q.disconnect().await;
    let mut out = Vec::new();
    for id in ids {
        let mut v = validation(c, id).await;
        let s2 = format!("t{}_sprig", c.tenant_id);
        let mut q = catalog(c).await;
        let def: Option<String> = q
            .exec_first(
                format!(
                    "SELECT CAST(k.definition AS CHAR) FROM `{s2}`.auto_index_candidates k \
                     JOIN `{s2}`.auto_index_validations v ON v.candidate_id = k.candidate_id \
                     WHERE v.validation_id = ?"
                ),
                (id,),
            )
            .await
            .unwrap();
        let _ = q.disconnect().await;
        v["parts"] = json!(def.as_deref().map(parts).unwrap_or_default());
        out.push(v);
    }
    out
}

/// Skipped candidate rows of `table` (no key parts), with their reasons.
async fn skips_on(c: &Cfg, db: &str, table: &str) -> Vec<(String, String)> {
    let s = format!("t{}_sprig", c.tenant_id);
    let mut q = catalog(c).await;
    let rows: Vec<(String, String)> = q
        .exec(
            format!(
                "SELECT CAST(k.fingerprints AS CHAR), IFNULL(k.reason, '') \
                 FROM `{s}`.auto_index_candidates k JOIN `{s}`.objects o ON o.object_id = k.object_id \
                 JOIN `{s}`.`databases` d ON d.db_id = o.db_id \
                 WHERE d.name = ? AND o.name = ? AND k.def_hash = 0"
            ),
            (db, table),
        )
        .await
        .unwrap();
    let _ = q.disconnect().await;
    rows
}

/// Index names on `table` as the gateway shows them, with or without
/// automatic ones.
async fn show_index(c: &Cfg, db: &str, table: &str, show_auto: bool) -> Vec<String> {
    let mut g = gw(c, Some(db)).await;
    g.query_drop(format!(
        "SET @sprig_show_auto_indexes = {}",
        u8::from(show_auto)
    ))
    .await
    .unwrap();
    let rows: Vec<Row> = g.query(format!("SHOW INDEX FROM {table}")).await.unwrap();
    let _ = g.disconnect().await;
    let mut names: Vec<String> = rows
        .into_iter()
        .map(|r| r.get::<String, _>("Key_name").unwrap())
        .collect();
    names.dedup();
    names
}

/// The shard schema of logical database `db` (R0: one shard per
/// database), from the catalog: `t<tenant>_d<db>_...`.
async fn physical(c: &Cfg, db: &str) -> String {
    let s = format!("t{}_sprig", c.tenant_id);
    let mut q = catalog(c).await;
    let id: u64 = q
        .exec_first(
            format!("SELECT db_id FROM `{s}`.`databases` WHERE name = ? AND state = 'active'"),
            (db,),
        )
        .await
        .unwrap()
        .expect("the database in the catalog");
    let schema: String = q
        .exec_first(
            "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE ?",
            (format!("t{}\\_d{id}\\_%", c.tenant_id),),
        )
        .await
        .unwrap()
        .expect("the shard schema");
    let _ = q.disconnect().await;
    schema
}

/// Fills a table on the cluster directly, not through the gateway: the
/// rows are the table's existing data, not part of the workload the
/// trigger window sees (a bulk load in the window would count as an hour's
/// writes in the cost model). 25,000 rows per transaction keeps Galera
/// write sets small.
async fn fill(c: &Cfg, schema: &str, table: &str, rows: u64, select: &str) {
    let mut q = catalog(c).await;
    q.query_drop(format!("USE `{schema}`")).await.unwrap();
    let mut lo = 1;
    while lo <= rows {
        let hi = (lo + 24_999).min(rows);
        q.query_drop(format!(
            "INSERT INTO {table} {select} FROM seq_{lo}_to_{hi}"
        ))
        .await
        .unwrap_or_else(|e| panic!("fill {table} on {}: {e}", c.topology));
        lo = hi + 1;
    }
    q.query_drop(format!(
        "ANALYZE TABLE {}",
        table.split(' ').next().unwrap()
    ))
    .await
    .unwrap();
    let _ = q.disconnect().await;
}

/// Runs a statement, retrying 39071 (`ER_SPRIG_CATALOG_CONFLICT`, SQLSTATE
/// 40001: "catalog row busy; retry"), which DDL can meet on Galera while
/// the controller writes the same catalog.
async fn ddl(g: &mut Conn, sql: &str) {
    for attempt in 1..=10 {
        match g.query_drop(sql).await {
            Ok(()) => return,
            Err(mysql_async::Error::Server(e)) if e.code == 39071 && attempt < 10 => {
                println!(
                    "[retry] 39071 on attempt {attempt}: {}",
                    &sql[..sql.len().min(60)]
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => panic!("{sql}: {e}"),
        }
    }
}

/// A small deterministic generator.
struct Gen(u64);

impl Gen {
    fn next(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n.max(1)
    }
}

/// The load of one round: each statement as text, prepared ones with
/// their parameters.
struct Load {
    ex1: bool,
    ex2: bool,
    trap: bool,
    trap_d_runs: u32,
    u_query: bool,
    u_other: bool,
    v_query: bool,
}

/// Statements Galera aborted (1213) or that waited too long for a lock
/// (1205) while a total-order index build held the table: counted, not
/// fatal (Galera clients retry these, as for any DDL there).
static ABORTED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

trait Tolerate {
    fn tolerate(self);
}

impl<T> Tolerate for Result<T, mysql_async::Error> {
    fn tolerate(self) {
        match self {
            Ok(_) => {}
            Err(mysql_async::Error::Server(e)) if matches!(e.code, 1213 | 1205) => {
                ABORTED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                println!("[aborted] {} {}", e.code, e.message);
            }
            Err(e) => panic!("load statement: {e}"),
        }
    }
}

async fn round(
    g: &mut Conn,
    rng: &mut Gen,
    l: &mut Load,
    n: u64,
    first: &mut BTreeMap<&'static str, Instant>,
) {
    let mut mark = |k: &'static str| {
        first.entry(k).or_insert_with(Instant::now);
    };
    if l.ex1 {
        mark("ex1");
        for _ in 0..4 {
            let cust = rng.next(250);
            let r: Result<Vec<Row>, _> = g
                .exec(
                    "SELECT o.id, o.total, o.created_at FROM orders o WHERE o.customer_id = ? \
                     AND o.status IN ('open', 'hold') ORDER BY o.created_at DESC LIMIT 20",
                    (cust,),
                )
                .await;
            r.tolerate();
        }
        // Writes: inserts and status updates (worked example 1's write mix).
        g.exec_drop(
            "INSERT INTO orders (customer_id, status, created_at, total, note) \
             VALUES (?, 'open', NOW(), ?, 'n')",
            (rng.next(250), rng.next(10_000)),
        )
        .await
        .tolerate();
        for _ in 0..3 {
            g.exec_drop(
                "UPDATE orders SET status = 'paid' WHERE id = ?",
                (1 + rng.next(n),),
            )
            .await
            .tolerate();
        }
    }
    if l.ex2 {
        mark("ex2");
        for _ in 0..3 {
            let (a, k) = (rng.next(200), rng.next(5));
            g.query_drop(format!(
                "SELECT id, created_at FROM events WHERE account_id = {a} AND kind = {k} \
                 AND created_at >= '2026-01-01 00:00:00' + INTERVAL {} MINUTE \
                 ORDER BY created_at LIMIT 20",
                rng.next(n / 2)
            ))
            .await
            .tolerate();
        }
        let (a, k) = (rng.next(200), rng.next(5));
        g.query_drop(format!(
            "SELECT COUNT(*) FROM events WHERE account_id = {a} AND kind = {k}"
        ))
        .await
        .tolerate();
        for _ in 0..2 {
            g.query_drop(format!(
                "SELECT id, kind FROM events WHERE account_id = {} ORDER BY id DESC LIMIT 20",
                rng.next(200)
            ))
            .await
            .tolerate();
        }
        g.query_drop(format!(
            "SELECT id FROM events WHERE LOWER(source) = 'src{}'",
            rng.next(50)
        ))
        .await
        .tolerate();
        g.query_drop(format!(
            "INSERT INTO events (account_id, kind, created_at, source, payload) \
             VALUES ({}, {}, NOW(), 'Src1', 'p')",
            rng.next(200),
            rng.next(5)
        ))
        .await
        .tolerate();
    }
    if l.trap {
        mark("trap");
        for _ in 0..4 {
            let lo = rng.next(n.saturating_sub(200));
            g.query_drop(format!(
                "SELECT id, pad FROM t WHERE c BETWEEN {lo} AND {}",
                lo + 100
            ))
            .await
            .tolerate();
        }
        if l.trap_d_runs < 12 && rng.next(3) == 0 {
            l.trap_d_runs += 1;
            g.query_drop("SELECT id FROM t WHERE d = 1 ORDER BY c LIMIT 10")
                .await
                .tolerate();
        }
    }
    if l.u_query {
        mark("u");
        for _ in 0..3 {
            g.query_drop(format!("SELECT id, b FROM u WHERE a = {}", rng.next(3000)))
                .await
                .tolerate();
        }
    }
    if l.u_other {
        g.query_drop(format!(
            "SELECT pad FROM u WHERE id = {}",
            1 + rng.next(30_000)
        ))
        .await
        .tolerate();
        g.query_drop(format!(
            "SELECT COUNT(*) FROM u WHERE b = {}",
            rng.next(100)
        ))
        .await
        .tolerate();
    }
    if l.v_query {
        mark("v");
        for _ in 0..3 {
            g.query_drop(format!("SELECT id, b FROM v WHERE a = {}", rng.next(3000)))
                .await
                .tolerate();
        }
    }
}

fn built(r: &AutoRow) -> bool {
    matches!(r.state.as_str(), "observing" | "active")
}

#[tokio::test]
#[ignore = "needs the dev gateway and controller (deploy/compose/m04/run.sh)"]
async fn chapter_13_scenarios_on_one_topology() {
    let Some(c) = cfg() else { return };
    let db = unique_ident("m04");
    let n = c.rows;
    let small = (n * 3 / 10).max(10_000);
    println!(
        "[setup] topology {} tenant {} db {db}, {n} rows",
        c.topology, c.tenant
    );
    let mut g = gw(&c, None).await;
    // Tenant settings: a 1-hour window evaluated every minute, a low p95
    // bar for the scaled-down tables, no build pause, room for the day's
    // indexes.
    ddl(
        &mut g,
        "SPRIG SET (auto_index.window_hours = 1, auto_index.eval_interval_minutes = 1, \
         auto_index.p95_threshold_ms = 5, auto_index.max_new_per_day = 50, \
         auto_index.rollout_pause_minutes = 0, auto_index.cooldown_hours = 1)",
    )
    .await;
    ddl(&mut g, &format!("CREATE DATABASE {db}")).await;
    g.query_drop(format!("USE {db}")).await.unwrap();
    for ddl_sql in [
        "CREATE TABLE orders (id BIGINT PRIMARY KEY AUTO_INCREMENT, customer_id INT NOT NULL, \
         status VARCHAR(12) NOT NULL, created_at DATETIME NOT NULL, total DECIMAL(10,2) NOT NULL, \
         note VARCHAR(100) NOT NULL, KEY idx_created (created_at)) ENGINE=InnoDB",
        "CREATE TABLE events (id BIGINT PRIMARY KEY AUTO_INCREMENT, account_id INT NOT NULL, \
         kind TINYINT NOT NULL, created_at DATETIME NOT NULL, \
         source VARCHAR(32) COLLATE utf8mb4_uca1400_ai_ci NOT NULL, payload VARCHAR(100) NOT NULL) \
         ENGINE=InnoDB",
        "CREATE TABLE t (id INT PRIMARY KEY, a INT NOT NULL, b TINYINT NOT NULL, c INT NOT NULL, \
         d TINYINT NOT NULL, pad VARCHAR(200) NOT NULL) ENGINE=InnoDB",
        "CREATE TABLE u (id INT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL, \
         pad VARCHAR(100) NOT NULL) ENGINE=InnoDB",
        "CREATE TABLE v (id INT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL, \
         pad VARCHAR(100) NOT NULL) ENGINE=InnoDB",
    ] {
        ddl(&mut g, ddl_sql).await;
    }
    let t0 = Instant::now();
    let schema = physical(&c, &db).await;
    fill(
        &c,
        &schema,
        "orders (customer_id, status, created_at, total, note)",
        n,
        "SELECT seq % 250, CASE WHEN seq % 100 < 5 THEN 'open' WHEN seq % 100 = 5 THEN 'hold' \
         ELSE 'paid' END, '2026-01-01' + INTERVAL seq MINUTE, (seq % 997) / 10, REPEAT('n', 40)",
    )
    .await;
    fill(
        &c,
        &schema,
        "events (account_id, kind, created_at, source, payload)",
        n,
        "SELECT seq % 200, seq % 5, '2026-01-01' + INTERVAL seq MINUTE, CONCAT('Src', seq % 50), \
         REPEAT('p', 40)",
    )
    .await;
    fill(
        &c,
        &schema,
        "t",
        n,
        &format!(
            "SELECT seq, (seq * 7919) % {n}, seq % 3, seq, IF(seq > {}, 1, 0), REPEAT('x', 150)",
            n - n / 100
        ),
    )
    .await;
    fill(
        &c,
        &schema,
        "u",
        small,
        "SELECT seq, seq % 3000, seq % 100, REPEAT('u', 60)",
    )
    .await;
    fill(
        &c,
        &schema,
        "v",
        small,
        "SELECT seq, seq % 3000, seq % 100, REPEAT('v', 60)",
    )
    .await;
    // A user index and a migrated one on u; the migrated origin is what a
    // dump replay records (chapter 13 "Index origins"; the import path is
    // R4), written here as that path will.
    ddl(&mut g, "CREATE INDEX idx_u_b ON u (b)").await;
    ddl(&mut g, "CREATE INDEX ix_u_mig ON u (pad(16))").await;
    {
        let s = format!("t{}_sprig", c.tenant_id);
        let mut q = catalog(&c).await;
        q.exec_drop(
            format!(
                "INSERT INTO `{s}`.index_origins (object_id, index_name, origin, source, recorded_at) \
                 SELECT o.object_id, 'ix_u_mig', 'migrated', 'mysqldump', NOW(6) FROM `{s}`.objects o \
                 JOIN `{s}`.`databases` d ON d.db_id = o.db_id WHERE d.name = ? AND o.name = 'u'"
            ),
            (db.as_str(),),
        )
        .await
        .unwrap();
        let _ = q.disconnect().await;
    }
    // The trap's ORDER BY statement must not qualify by itself: no top-N
    // trigger on t, and it runs fewer than min_runs times.
    ddl(&mut g, "ALTER TABLE t SPRIG SET (auto_index.top_n = 0)").await;
    println!("[setup] filled in {:.1} s", t0.elapsed().as_secs_f64());

    // ---- Phase 1: load until every expected index is built --------------
    let mut rng = Gen(0x9e37_79b9_7f4a_7c15);
    let mut load = Load {
        ex1: true,
        ex2: true,
        trap: true,
        trap_d_runs: 0,
        u_query: true,
        u_other: true,
        v_query: true,
    };
    let mut first: BTreeMap<&'static str, Instant> = BTreeMap::new();
    let mut done: BTreeMap<&'static str, (f64, AutoRow)> = BTreeMap::new();
    let deadline = Instant::now() + Duration::from_secs(25 * 60);
    let mut last_poll = Instant::now();
    let want = |table: &str, rows: &[AutoRow]| -> Option<AutoRow> {
        rows.iter()
            .find(|r| {
                r.table == table
                    && built(r)
                    && match table {
                        "orders" => {
                            parts(&r.definition).first().map(String::as_str) == Some("customer_id")
                        }
                        "events" => {
                            parts(&r.definition).starts_with(&["account_id".into(), "kind".into()])
                        }
                        "t" => parts(&r.definition) == ["c", "d"],
                        _ => parts(&r.definition).first().map(String::as_str) == Some("a"),
                    }
            })
            .cloned()
    };
    while done.len() < 5 && Instant::now() < deadline {
        round(&mut g, &mut rng, &mut load, n, &mut first).await;
        if last_poll.elapsed() >= Duration::from_secs(5) {
            last_poll = Instant::now();
            let rows = auto_rows(&c, &db).await;
            for (k, table) in [
                ("ex1", "orders"),
                ("ex2", "events"),
                ("trap", "t"),
                ("u", "u"),
                ("v", "v"),
            ] {
                if done.contains_key(k) {
                    continue;
                }
                if let Some(r) = want(table, &rows) {
                    let s = first[k].elapsed().as_secs_f64();
                    println!(
                        "[built] {k}: {} {:?} after {s:.0} s",
                        r.name,
                        parts(&r.definition)
                    );
                    done.insert(k, (s, r));
                    if k == "u" {
                        // The query that wanted it stops; u's other
                        // statements go on.
                        load.u_query = false;
                    }
                }
            }
            let states: Vec<String> = rows
                .iter()
                .map(|r| format!("{}:{}:{}", r.table, r.name, r.state))
                .collect();
            println!(
                "[poll {:>4.0} s] {}",
                t0.elapsed().as_secs_f64(),
                states.join(" ")
            );
        }
    }
    let rows = auto_rows(&c, &db).await;
    let mut report = json!({
        "topology": c.topology,
        "tenant": c.tenant,
        "rows": n,
        "examples": {},
    });
    for (k, table) in [
        ("ex1", "orders"),
        ("ex2", "events"),
        ("trap", "t"),
        ("u", "u"),
        ("v", "v"),
    ] {
        let vals = validations_on(&c, &db, table).await;
        report["examples"][k] = json!({
            "table": table,
            "built": done.get(k).map(|(s, r)| json!({
                "index": r.name, "parts": parts(&r.definition), "state": r.state,
                "seconds_from_first_query": s.round(),
            })),
            "rows": rows.iter().filter(|r| r.table == table).map(|r| json!({
                "index": r.name, "parts": parts(&r.definition), "state": r.state,
                "drop_reason": r.drop_reason, "validation_id": r.validation_id,
            })).collect::<Vec<_>>(),
            "validations": vals,
        });
    }
    let skips = skips_on(&c, &db, "events").await;
    report["examples"]["ex2"]["skipped"] = json!(skips);
    println!("{}", serde_json::to_string_pretty(&report).unwrap());

    // Done-when 1: each worked example built within one trigger window
    // (1 hour here).
    for k in ["ex1", "ex2", "trap", "u", "v"] {
        let (s, _) = done
            .get(k)
            .unwrap_or_else(|| panic!("{k}: no index built on {}: {rows:#?}", c.topology));
        assert!(*s < 3600.0, "{k}: {s} s");
    }
    // Example 2: F4 is explained, never indexed.
    assert!(
        skips.iter().any(|(_, r)| r == "function:LOWER"),
        "no function:LOWER skip on events: {skips:?}"
    );
    // Done-when 2: the trap's (c) was rejected for a regression, and its
    // covering retry accepted.
    let tv = validations_on(&c, &db, "t").await;
    assert!(
        tv.iter().any(|v| v["parts"] == json!(["c"])
            && v["verdict"] == "rejected"
            && v["reason"]
                .as_str()
                .unwrap_or_default()
                .starts_with("regression")),
        "trap not rejected: {tv:#?}"
    );
    assert!(
        tv.iter()
            .any(|v| v["parts"] == json!(["c", "d"]) && v["verdict"] == "accepted"),
        "no accepted covering retry: {tv:#?}"
    );
    assert!(
        !rows
            .iter()
            .any(|r| r.table == "t" && parts(&r.definition) == ["c"] && built(r)),
        "the trap index was built"
    );
    // Hidden from metadata, visible with the session flag.
    let ex1 = &done["ex1"].1;
    assert!(
        !show_index(&c, &db, "orders", false)
            .await
            .contains(&ex1.name)
    );
    assert!(
        show_index(&c, &db, "orders", true)
            .await
            .contains(&ex1.name)
    );

    // ---- Phase 2: a user index equal to or extending ours retires it ----
    load.v_query = false;
    let v_auto = done["v"].1.name.clone();
    ddl(&mut g, "CREATE INDEX idx_user_ab ON v (a, b)").await;
    let t2 = Instant::now();
    let mut superseded = None;
    while t2.elapsed() < Duration::from_secs(300) {
        round(&mut g, &mut rng, &mut load, n, &mut first).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        if let Some(r) = auto_rows(&c, &db)
            .await
            .into_iter()
            .find(|r| r.name == v_auto)
            && matches!(r.state.as_str(), "retiring" | "dropping" | "dropped")
        {
            println!(
                "[superseded] {} -> {} after {:.0} s",
                r.name,
                r.state,
                t2.elapsed().as_secs_f64()
            );
            superseded = Some((t2.elapsed().as_secs_f64(), r));
            break;
        }
    }
    let (sup_s, sup) = superseded.expect("ours wasn't retired by the user's index");
    assert_eq!(sup.drop_reason.as_deref(), Some("superseded"));
    report["supersede"] = json!({"index": sup.name, "state": sup.state, "seconds": sup_s.round()});

    // ---- Phase 3: 8 and 40 days later: unused dropped, others kept -------
    // Light traffic on u that never uses (a): primary-key reads and the
    // user index.
    load.ex1 = false;
    load.ex2 = false;
    load.trap = false;
    let pass = |secs: u64| async move {
        // A pass of the loop runs at least every minute.
        tokio::time::sleep(Duration::from_secs(secs)).await;
    };
    let u_auto = done["u"].1.name.clone();
    let mut timeline = Vec::new();
    for (day, wait) in [(8, 90), (40, 90), (42, 90)] {
        set_clock(&c, day * DAY);
        let t3 = Instant::now();
        while t3.elapsed() < Duration::from_secs(wait) {
            round(&mut g, &mut rng, &mut load, n, &mut first).await;
            pass(1).await;
        }
        let rows = auto_rows(&c, &db).await;
        let states: BTreeMap<String, String> = rows
            .iter()
            .map(|r| {
                (
                    format!("{}.{}", r.table, r.name),
                    format!(
                        "{}{}",
                        r.state,
                        r.drop_reason
                            .as_ref()
                            .map(|d| format!(" ({d})"))
                            .unwrap_or_default()
                    ),
                )
            })
            .collect();
        println!("[day {day}] {states:?}");
        timeline.push(json!({"day": day, "states": states}));
    }
    set_clock(&c, 0);
    report["unused"] = json!(timeline);
    let rows = auto_rows(&c, &db).await;
    let u_row = rows.iter().find(|r| r.name == u_auto).unwrap();
    assert_eq!(u_row.state, "dropped", "{u_row:?}");
    assert_eq!(u_row.drop_reason.as_deref(), Some("unused"));
    let u_ix = show_index(&c, &db, "u", true).await;
    assert!(!u_ix.contains(&u_auto), "{u_ix:?}");
    assert!(
        u_ix.contains(&"idx_u_b".to_owned()),
        "user index gone: {u_ix:?}"
    );
    assert!(
        u_ix.contains(&"ix_u_mig".to_owned()),
        "migrated index gone: {u_ix:?}"
    );
    let v_ix = show_index(&c, &db, "v", true).await;
    assert!(
        !v_ix.contains(&v_auto) && v_ix.contains(&"idx_user_ab".to_owned()),
        "{v_ix:?}"
    );
    // Every observed index became active on day 8 (the timeline).
    report["seconds_total"] = json!(t0.elapsed().as_secs());
    report["load_statements_aborted"] = json!(ABORTED.load(std::sync::atomic::Ordering::Relaxed));
    report["created_at_spread_s"] = json!(
        rows.iter()
            .map(|r| r.state_changed_at - r.created_at)
            .fold(0.0, f64::max)
    );

    if let Some(p) = skip::var("SPRIG_TEST_M04_REPORT") {
        std::fs::write(&p, serde_json::to_string_pretty(&report).unwrap()).unwrap();
    }
    if skip::var("SPRIG_TEST_M04_KEEP").as_deref() != Some("1") {
        ddl(&mut g, &format!("DROP DATABASE {db}")).await;
    }
    ddl(
        &mut g,
        "SPRIG RESET (auto_index.window_hours, auto_index.eval_interval_minutes, \
         auto_index.p95_threshold_ms, auto_index.max_new_per_day, auto_index.rollout_pause_minutes, \
         auto_index.cooldown_hours)",
    )
    .await;
    let _ = g.disconnect().await;
}
