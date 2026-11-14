//! The automatic-indexing scenario suite, through the real gateway and the
//! real controller, on one reference topology per run.
//!
//! The tenant's statements go through the gateway as `<tenant>.app`; workload
//! capture records them in the tenant's catalog; the controller's `auto_index`
//! loop evaluates, validates (replica, Galera RSU or estimate-and-canary,
//! by the cluster), builds, observes and drops. The test only generates
//! load, changes settings with `SPRIG SET`, and reads the catalog's
//! `auto_index*` tables and the gateway's `SHOW INDEX` to see what happened.
//!
//! Scenarios, scaled down (tables of 30,000 to 100,000 rows; Galera tables
//! are filled 25,000 rows per transaction):
//!
//! 1. **Orders** (`orders`): `customer_id = ? AND status IN (...)
//!    ORDER BY created_at DESC LIMIT ?` with a user index on `created_at`
//!    and status updates: an index starting with `customer_id` is proposed,
//!    validated and built.
//! 2. **Events** (`events`): the range query and the count on `account_id`
//!    and `kind` get one merged index `(account_id, kind, created_at)`;
//!    `LOWER(source) = ?` gets no candidate, and its skipped row says `function:LOWER`.
//! 3. **The `ORDER BY ... LIMIT` trap** (`t`): `(c)` for
//!    a range on `c` sends `d = 1 ORDER BY c LIMIT 10` through the whole
//!    index: rejected for the regression; the covering retry `(c, d)` is
//!    accepted and built.
//! 4. **A user-created equivalent retires ours** (`v`): after `(a)` is
//!    built, the user creates `(a, b)`: ours is retired as `superseded`.
//! 5. **Unused, user and migrated indexes** (`u`): `(a)` is built, then its
//!    query stops; after 400 reads that don't use it (judged by reads,
//!    never days) it is ignored, and after 400 more dropped as `unused`.
//!    The user index and the migrated one (an `index_origins` row, as a
//!    dump replay records it) are untouched: `retire_user_indexes` is
//!    `off` on `u` here.
//!
//! [`usage_based_removal_on_one_topology`] is the usage-based removal scenario:
//! automatic and user indexes removed by reads, `recommend`, the primary
//! key, `UNIQUE` and foreign-key indexes never touched, and
//! `sprigctl index restore`.
//!
//! MongoDB collections are not covered here.
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

#[allow(dead_code)] // the clock no longer decides removal; reads do
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

/// The shard schema of logical database `db` (one shard per
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
        // Writes: inserts and status updates (the orders write mix).
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
    // dump replay records, written here as the import path will.
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

    // Check 1: each scenario's index built within one trigger window
    // (1 hour here).
    for k in ["ex1", "ex2", "trap", "u", "v"] {
        let (s, _) = done
            .get(k)
            .unwrap_or_else(|| panic!("{k}: no index built on {}: {rows:#?}", c.topology));
        assert!(*s < 3600.0, "{k}: {s} s");
    }
    // Events: `LOWER(source)` is explained, never indexed.
    assert!(
        skips.iter().any(|(_, r)| r == "function:LOWER"),
        "no function:LOWER skip on events: {skips:?}"
    );
    // Check 2: the trap's (c) was rejected for a regression, and its
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

    // ---- Phase 3: unused dropped by reads, others kept -------------------
    // Light traffic on u that never uses (a): primary-key reads and the
    // user index. 400 reads judge it (database scope), 400 more while it
    // is IGNORED drop it; no clock is moved.
    load.ex1 = false;
    load.ex2 = false;
    load.trap = false;
    ddl(
        &mut g,
        &format!("ALTER DATABASE {db} SPRIG SET (auto_index.usage_sample_reads = 400)"),
    )
    .await;
    // The user and migrated indexes of u are left alone here (their own
    // removal is the usage-based removal scenario's): retire_user_indexes off on u.
    ddl(
        &mut g,
        "ALTER TABLE u SPRIG SET (auto_index.retire_user_indexes = 'off', \
         auto_index.maintenance_cost_ms = 10)",
    )
    .await;
    let u_auto = done["u"].1.name.clone();
    let mut timeline = Vec::new();
    let t3 = Instant::now();
    let mut last = String::new();
    while t3.elapsed() < Duration::from_secs(15 * 60) {
        round(&mut g, &mut rng, &mut load, n, &mut first).await;
        // Writes to a: an index on a table nobody writes is kept, and the
        // query (a) served is remembered until they cost more.
        g.query_drop(format!(
            "UPDATE u SET a = {} WHERE id = {}",
            rng.next(3000),
            1 + rng.next(small)
        ))
        .await
        .tolerate();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let rows = auto_rows(&c, &db).await;
        let Some(r) = rows.iter().find(|r| r.name == u_auto) else {
            continue;
        };
        let state = format!(
            "{}{}",
            r.state,
            r.drop_reason
                .as_ref()
                .map(|d| format!(" ({d})"))
                .unwrap_or_default()
        );
        if state != last {
            println!("[{:.0} s] u.{u_auto}: {state}", t3.elapsed().as_secs_f64());
            timeline.push(json!({"seconds": t3.elapsed().as_secs(), "state": state}));
            last = state;
        }
        if r.state == "dropped" {
            break;
        }
    }
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

// ---- Index removal judged by usage, never by days -----------------------

/// An `index_usage` row: a user or migrated index under usage-based removal.
#[derive(Debug, Clone)]
struct UsageRowT {
    table: String,
    name: String,
    state: String,
    drop_reason: Option<String>,
    observed: Json,
}

async fn usage_rows(c: &Cfg, db: &str) -> Vec<UsageRowT> {
    let s = format!("t{}_sprig", c.tenant_id);
    let mut q = catalog(c).await;
    let rows: Vec<Row> = q
        .exec(
            format!(
                "SELECT o.name, u.index_name, u.state, u.drop_reason, CAST(u.observed AS CHAR) \
                 FROM `{s}`.index_usage u JOIN `{s}`.objects o ON o.object_id = u.object_id \
                 JOIN `{s}`.`databases` d ON d.db_id = o.db_id WHERE d.name = ? \
                 ORDER BY o.name, u.index_name"
            ),
            (db,),
        )
        .await
        .unwrap_or_default();
    let _ = q.disconnect().await;
    rows.into_iter()
        .map(|mut r| UsageRowT {
            table: r.take(0).unwrap(),
            name: r.take(1).unwrap(),
            state: r.take(2).unwrap(),
            drop_reason: r.take(3).unwrap(),
            observed: serde_json::from_str(&r.take::<String, _>(4).unwrap()).unwrap_or(Json::Null),
        })
        .collect()
}

/// `observed` of an automatic index row.
async fn auto_observed(c: &Cfg, name: &str) -> Json {
    let s = format!("t{}_sprig", c.tenant_id);
    let mut q = catalog(c).await;
    let v: Option<Option<String>> = q
        .exec_first(
            format!("SELECT CAST(observed AS CHAR) FROM `{s}`.auto_indexes WHERE name = ?"),
            (name,),
        )
        .await
        .unwrap();
    let _ = q.disconnect().await;
    v.flatten()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or(Json::Null)
}

/// Index name → IGNORED, on the shard (the catalog node is the writer).
async fn shard_indexes(c: &Cfg, schema: &str, table: &str) -> BTreeMap<String, bool> {
    let mut q = catalog(c).await;
    let rows: Vec<(String, String)> = q
        .exec(
            "SELECT DISTINCT INDEX_NAME, IGNORED FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (schema, table),
        )
        .await
        .unwrap();
    let _ = q.disconnect().await;
    rows.into_iter().map(|(n, i)| (n, i == "YES")).collect()
}

fn sprigctl(c: &Cfg, args: &[&str]) -> (bool, String) {
    let bin = skip::var("SPRIG_TEST_M04_SPRIGCTL").unwrap_or_else(|| "sprigctl".into());
    let out = std::process::Command::new(bin)
        .args(args)
        .env("SPRIGCTL_TENANT", &c.tenant)
        .output()
        .expect("sprigctl runs");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    println!("[sprigctl {}] {}", args.join(" "), text.trim());
    (out.status.success(), text)
}

/// Usage-based removal, live: an automatic index and a user index that serve no
/// reads are made `IGNORED`, watched for `usage_sample_reads` reads and
/// dropped; a used user index, the primary key, a `UNIQUE` index and a
/// foreign key's index are never touched; `retire_user_indexes =
/// recommend` only recommends; an unused index on a table nobody writes
/// (`r`) is kept; `sprigctl index restore` brings a retiring
/// index back and recreates a dropped one. Once `(a)`'s query stops, its
/// remembered savings protect it (the periodic-job rule) until writes to
/// `a`, at 10 ms per entry change on `u`, cost more. No clock is moved: every
/// decision is by reads. `usage_sample_reads` is 400 at database scope
/// and 200 on table `x` (object scope wins); `retire_user_indexes` is `on`
/// at database scope and `recommend` on table `w` (the dev stack may hold
/// `recommend` at global scope, for other tenants' sake).
#[tokio::test]
#[ignore = "needs the dev gateway and a controller with usage-based index removal (deploy/compose/m04/run.sh o1)"]
async fn usage_based_removal_on_one_topology() {
    let Some(c) = cfg() else { return };
    let db = unique_ident("o1");
    println!(
        "[setup] topology {} tenant {} db {db}",
        c.topology, c.tenant
    );
    let mut g = gw(&c, None).await;
    ddl(
        &mut g,
        "SPRIG SET (auto_index.window_hours = 1, auto_index.eval_interval_minutes = 1, \
         auto_index.p95_threshold_ms = 5, auto_index.max_new_per_day = 50, \
         auto_index.rollout_pause_minutes = 0, auto_index.cooldown_hours = 1)",
    )
    .await;
    ddl(&mut g, &format!("CREATE DATABASE {db}")).await;
    g.query_drop(format!("USE {db}")).await.unwrap();
    for sql in [
        "CREATE TABLE p (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL) ENGINE=InnoDB",
        "CREATE TABLE u (id INT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL, c INT NOT NULL, \
         p_id INT NOT NULL, pad VARCHAR(100) NOT NULL, UNIQUE KEY uq_u_c (c), KEY ix_u_p (p_id), \
         KEY idx_u_b (b), KEY idx_u_old (pad(10)), \
         CONSTRAINT fk_u_p FOREIGN KEY (p_id) REFERENCES p (id)) ENGINE=InnoDB",
        "CREATE TABLE w (id INT PRIMARY KEY, b INT NOT NULL, pad VARCHAR(100) NOT NULL, \
         KEY idx_w_old (b)) ENGINE=InnoDB",
        "CREATE TABLE x (id INT PRIMARY KEY, b INT NOT NULL, pad VARCHAR(100) NOT NULL, \
         KEY idx_x_old (b)) ENGINE=InnoDB",
        "CREATE TABLE r (id INT PRIMARY KEY, b INT NOT NULL, pad VARCHAR(100) NOT NULL, \
         KEY idx_r_old (b)) ENGINE=InnoDB",
    ] {
        ddl(&mut g, sql).await;
    }
    ddl(
        &mut g,
        &format!(
            "ALTER DATABASE {db} SPRIG SET (auto_index.usage_sample_reads = 400, \
             auto_index.retire_user_indexes = 'on')"
        ),
    )
    .await;
    ddl(
        &mut g,
        "ALTER TABLE w SPRIG SET (auto_index.retire_user_indexes = 'recommend')",
    )
    .await;
    ddl(
        &mut g,
        "ALTER TABLE x SPRIG SET (auto_index.usage_sample_reads = 200)",
    )
    .await;
    let schema = physical(&c, &db).await;
    fill(&c, &schema, "p", 3000, "SELECT seq, CONCAT('p', seq)").await;
    fill(
        &c,
        &schema,
        "u",
        30_000,
        "SELECT seq, seq % 3000, seq % 100, seq, 1 + seq % 3000, REPEAT('u', 60)",
    )
    .await;
    for t in ["w", "x", "r"] {
        fill(
            &c,
            &schema,
            t,
            10_000,
            "SELECT seq, seq % 100, REPEAT('w', 60)",
        )
        .await;
    }
    let t0 = Instant::now();
    let mut rng = Gen(0x0123_4567_89ab_cdef);
    let mut a_query = true;
    let mut timeline: Vec<Json> = Vec::new();
    let mut seen: BTreeMap<String, String> = BTreeMap::new();
    let mut note = |key: String, state: String, extra: Json, t: f64| {
        if seen.get(&key) != Some(&state) {
            println!("[{t:>4.0} s] {key}: {state} {extra}");
            timeline.push(json!({"t_s": t.round(), "index": key, "state": state, "detail": extra}));
            seen.insert(key, state);
        }
    };
    let mut u_auto: Option<String> = None;
    let mut restored_x = false;
    let mut last_poll = Instant::now();
    let deadline = Instant::now() + Duration::from_secs(25 * 60);
    let mut writes = 0u64;
    let mut reads = 0u64;
    loop {
        assert!(
            Instant::now() < deadline,
            "usage-based removal scenario timed out (see the log)"
        );
        // Load: u's reads by b (idx_u_b) and by id; by a until its index
        // exists and has served a while; writes; w and x by id.
        for _ in 0..4 {
            if a_query {
                g.query_drop(format!("SELECT id, b FROM u WHERE a = {}", rng.next(3000)))
                    .await
                    .tolerate();
            }
            g.query_drop(format!(
                "SELECT COUNT(*) FROM u WHERE b = {}",
                rng.next(100)
            ))
            .await
            .tolerate();
            g.query_drop(format!(
                "SELECT pad FROM u WHERE id = {}",
                1 + rng.next(30_000)
            ))
            .await
            .tolerate();
            g.query_drop(format!(
                "SELECT pad FROM w WHERE id = {}",
                1 + rng.next(10_000)
            ))
            .await
            .tolerate();
            g.query_drop(format!(
                "SELECT pad FROM x WHERE id = {}",
                1 + rng.next(10_000)
            ))
            .await
            .tolerate();
            g.query_drop(format!(
                "SELECT pad FROM r WHERE id = {}",
                1 + rng.next(10_000)
            ))
            .await
            .tolerate();
            reads += 5 + u64::from(a_query);
        }
        // w and x are written (their unused indexes cost something); r,
        // read-only, never is: its unused index is kept.
        for t in ["w", "x"] {
            g.query_drop(format!(
                "UPDATE {t} SET b = {} WHERE id = {}",
                rng.next(100),
                1 + rng.next(10_000)
            ))
            .await
            .tolerate();
            writes += 1;
        }
        g.query_drop(format!(
            "UPDATE u SET pad = REPEAT('v', 60) WHERE id = {}",
            1 + rng.next(30_000)
        ))
        .await
        .tolerate();
        writes += 1;
        if !a_query {
            // Writes to `a` once its query stopped: (a) now only costs.
            g.query_drop(format!(
                "UPDATE u SET a = {} WHERE id = {}",
                rng.next(3000),
                1 + rng.next(30_000)
            ))
            .await
            .tolerate();
            writes += 1;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        if last_poll.elapsed() < Duration::from_secs(5) {
            continue;
        }
        last_poll = Instant::now();
        let t = t0.elapsed().as_secs_f64();
        let autos = auto_rows(&c, &db).await;
        if u_auto.is_none()
            && let Some(r) = autos.iter().find(|r| {
                r.table == "u"
                    && built(r)
                    && parts(&r.definition).first().map(String::as_str) == Some("a")
            })
        {
            u_auto = Some(r.name.clone());
        }
        for r in autos.iter().filter(|r| r.table == "u") {
            let o = auto_observed(&c, &r.name).await;
            note(
                format!("u.{} (auto)", r.name),
                format!(
                    "{}{}",
                    r.state,
                    r.drop_reason
                        .as_ref()
                        .map(|d| format!(" ({d})"))
                        .unwrap_or_default()
                ),
                json!({"usage": o["usage"]["last"], "retire_reads": o["retire"]["reads"]}),
                t,
            );
        }
        let users = usage_rows(&c, &db).await;
        for r in &users {
            note(
                format!("{}.{} (user)", r.table, r.name),
                format!(
                    "{}{}",
                    r.state,
                    r.drop_reason
                        .as_ref()
                        .map(|d| format!(" ({d})"))
                        .unwrap_or_default()
                ),
                json!({"usage": r.observed["usage"]["last"], "retire_reads": r.observed["retire"]["reads"]}),
                t,
            );
        }
        // Once (a) has served for a minute, its query stops.
        if a_query
            && let Some(name) = &u_auto
            && autos.iter().any(|r| &r.name == name && built(r))
            && auto_observed(&c, name).await["usage"]["verdict"] == "keep"
        {
            println!(
                "[{t:>4.0} s] (a) kept by usage; its query stops now, writes to a begin, \
                 and an entry change costs 10 ms on u"
            );
            a_query = false;
            // The query (a) served is remembered (a periodic job, as far as
            // the rule knows) and still in the workload store, so its
            // savings count until the writes cost more.
            ddl(
                &mut g,
                "ALTER TABLE u SPRIG SET (auto_index.maintenance_cost_ms = 10)",
            )
            .await;
        }
        // x's unused index is retiring: restore it (x keeps being read).
        let state_of = |t: &str, n: &str| {
            users
                .iter()
                .find(|r| r.table == t && r.name == n)
                .map(|r| r.state.clone())
        };
        if !restored_x && state_of("x", "idx_x_old").as_deref() == Some("retiring") {
            assert_eq!(
                shard_indexes(&c, &schema, "x").await.get("idx_x_old"),
                Some(&true)
            );
            let (ok, out) = sprigctl(&c, &["index", "restore", &format!("{db}.x.idx_x_old")]);
            assert!(ok, "restore x: {out}");
            assert_eq!(
                shard_indexes(&c, &schema, "x").await.get("idx_x_old"),
                Some(&false)
            );
            restored_x = true;
        }
        let u_done = u_auto
            .as_ref()
            .is_some_and(|n| autos.iter().any(|r| &r.name == n && r.state == "dropped"))
            && state_of("u", "idx_u_old").as_deref() == Some("dropped");
        let r_kept = users.iter().any(|r| {
            r.table == "r"
                && r.name == "idx_r_old"
                && r.state == "serving"
                && r.observed["usage"]["verdict"] == "keep"
        });
        if u_done
            && restored_x
            && r_kept
            && state_of("w", "idx_w_old").as_deref() == Some("recommended")
        {
            break;
        }
    }
    let t_end = t0.elapsed().as_secs_f64();
    println!("[{t_end:>4.0} s] {reads} reads, {writes} writes through the gateway");

    // Restore of a dropped user index: recreated from its definition, pinned.
    let (ok, out) = sprigctl(&c, &["index", "restore", &format!("{db}.u.idx_u_old")]);
    assert!(ok, "restore u: {out}");
    let (_, listing) = sprigctl(&c, &["index", "list", "--db", &db, "--all"]);

    // Keep loading a little: pinned ones stay.
    for _ in 0..300 {
        g.query_drop(format!(
            "SELECT pad FROM x WHERE id = {}",
            1 + rng.next(10_000)
        ))
        .await
        .tolerate();
        g.query_drop(format!(
            "SELECT pad FROM u WHERE id = {}",
            1 + rng.next(30_000)
        ))
        .await
        .tolerate();
    }
    tokio::time::sleep(Duration::from_secs(70)).await;

    let users = usage_rows(&c, &db).await;
    let st = |t: &str, n: &str| {
        users
            .iter()
            .find(|r| r.table == t && r.name == n)
            .map(|r| r.state.clone())
    };
    let u_ix = shard_indexes(&c, &schema, "u").await;
    let x_ix = shard_indexes(&c, &schema, "x").await;
    let w_ix = shard_indexes(&c, &schema, "w").await;
    let r_ix = shard_indexes(&c, &schema, "r").await;
    let autos = auto_rows(&c, &db).await;
    let u_auto = u_auto.expect("(a) built on u");
    let a_row = autos.iter().find(|r| r.name == u_auto).unwrap();
    let report = json!({
        "topology": c.topology,
        "tenant": c.tenant,
        "seconds": t_end.round(),
        "reads": reads,
        "writes": writes,
        "timeline": timeline,
        "final": {
            "auto": {"index": u_auto, "state": a_row.state, "drop_reason": a_row.drop_reason,
                     "observed": auto_observed(&c, &u_auto).await},
            "user": users.iter().map(|r| json!({"table": r.table, "index": r.name,
                     "state": r.state, "drop_reason": r.drop_reason})).collect::<Vec<_>>(),
            "u_indexes_ignored": u_ix, "w_indexes_ignored": w_ix, "x_indexes_ignored": x_ix,
            "r_indexes_ignored": r_ix,
        },
        "sprigctl_index_list": listing,
    });
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
    if let Some(p) = skip::var("SPRIG_TEST_M04_REPORT") {
        std::fs::write(&p, serde_json::to_string_pretty(&report).unwrap()).unwrap();
    }

    // The automatic index: ignored, watched by reads, dropped as unused.
    assert_eq!(a_row.state, "dropped");
    assert_eq!(a_row.drop_reason.as_deref(), Some("unused"));
    assert!(!u_ix.contains_key(&u_auto));
    assert!(
        timeline.iter().any(
            |e| e["index"] == format!("u.{u_auto} (auto)") && e["state"] == "retiring (unused)"
        ),
        "{timeline:#?}"
    );
    // The used user index stays visible; the protected ones never get a
    // row and are never ignored.
    assert_eq!(st("u", "idx_u_b").as_deref(), Some("serving"));
    for p in ["PRIMARY", "uq_u_c", "ix_u_p", "idx_u_b"] {
        assert_eq!(u_ix.get(p), Some(&false), "u.{p}: {u_ix:?}");
        if p != "idx_u_b" {
            assert!(st("u", p).is_none(), "u.{p} has a row");
        }
    }
    // The unused user index: retiring, then dropped; restore recreated it.
    assert!(
        timeline
            .iter()
            .any(|e| e["index"] == "u.idx_u_old (user)" && e["state"] == "dropped (unused)"),
        "{timeline:#?}"
    );
    assert_eq!(st("u", "idx_u_old").as_deref(), Some("pinned"));
    assert_eq!(u_ix.get("idx_u_old"), Some(&false));
    // recommend: only recommended, still visible.
    assert_eq!(st("w", "idx_w_old").as_deref(), Some("recommended"));
    assert_eq!(w_ix.get("idx_w_old"), Some(&false));
    // A table nobody writes: its unused index is kept (0 saved against 0
    // write cost), judged and reported with a ratio of 0.
    assert_eq!(st("r", "idx_r_old").as_deref(), Some("serving"));
    assert_eq!(r_ix.get("idx_r_old"), Some(&false));
    let r_usage = &users
        .iter()
        .find(|r| r.table == "r" && r.name == "idx_r_old")
        .unwrap()
        .observed["usage"];
    assert_eq!(r_usage["verdict"], "keep", "{r_usage}");
    assert_eq!(r_usage["last"]["served"], 0, "{r_usage}");
    // Restored while retiring: visible and pinned, through more reads.
    assert_eq!(st("x", "idx_x_old").as_deref(), Some("pinned"));
    assert_eq!(x_ix.get("idx_x_old"), Some(&false));

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
