//! bugfix-31 (decisions/bugfix-interim.md): stored code keeps working
//! through the advisor's regroup and the splits, merges and moves that
//! follow.
//!
//! - Database `sc` has a procedure, a function, a view, a trigger and an
//!   event over `la` (the trigger writes `la_log`), a procedure running
//!   dynamic SQL from a string literal over `dyn_t`, and tables `cust` and
//!   `ord` that no code names (`cust` has a trigger that touches only its
//!   own row). Database `dz` has a procedure whose dynamic SQL is built at
//!   run time, so it may read any of `dz`'s tables.
//! - The tenant's administrator and a user granted only the objects run
//!   every one of them from two clients the whole time while the sharding
//!   loop regroups the tenant's tables, then `cust`'s group is split, its
//!   halves merged and the merged shard moved to another cluster.
//! - Before the fix every table moved, and every statement through stored
//!   code failed with 1146 for both users. After it, the tables stored code
//!   names stay in their database's default schema (the run's evidence
//!   says why), `cust` and `ord` are sharded, and nothing fails.
//!
//! Runs against a deployment `deploy/compose/gateway-dev.sh up` started
//! (its own etcd and tenant-id range), with a second cluster to move to:
//!
//! ```sh
//! SPRIG_TEST_STORED_CODE=<gateway mysql port>,<admin port> \
//! SPRIG_TEST_STORED_CODE_TENANT=<a new tenant name> \
//! SPRIG_TEST_STORED_CODE_MOVE_TO=dc-a-c2 \
//! SPRIG_TEST_STORED_CODE_NODES=13301,13311 \
//! SPRIG_TEST_STORED_CODE_SPRIGCTL=<path to sprigctl> \
//!   cargo test -p sprig-controller --test live_stored_code -- --nocapture
//! ```
//!
//! `..._NODES` are the writers of the clusters the tenant uses (read as
//! `root`, password `SPRIG_TEST_ROOT_PASSWORD`, default `sprig-dev-root`).
//! The dev clusters run with `event_scheduler` off, so the event is
//! checked by where its tables are, not by running it. The test deletes
//! the tenant at the end.

#![allow(
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::many_single_char_names,
    missing_docs
)]

use std::collections::BTreeMap;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{OptsBuilder, Pool, PoolConstraints, PoolOpts, SslOpts};
use serde_json::Value;

const PW: &str = "sprig-stored-code-pw";

struct Env {
    mysql: u16,
    admin: u16,
    tenant: String,
    move_to: String,
    nodes: Vec<u16>,
    sprigctl: String,
}

fn env() -> Option<Env> {
    let v = std::env::var("SPRIG_TEST_STORED_CODE").ok()?;
    let (m, a) = v.split_once(',')?;
    Some(Env {
        mysql: m.trim().parse().ok()?,
        admin: a.trim().parse().ok()?,
        tenant: std::env::var("SPRIG_TEST_STORED_CODE_TENANT")
            .unwrap_or_else(|_| "storedcode".to_owned()),
        move_to: std::env::var("SPRIG_TEST_STORED_CODE_MOVE_TO")
            .unwrap_or_else(|_| "dc-a-c2".to_owned()),
        nodes: std::env::var("SPRIG_TEST_STORED_CODE_NODES")
            .unwrap_or_else(|_| "13301,13311".to_owned())
            .split(',')
            .filter_map(|p| p.trim().parse().ok())
            .collect(),
        sprigctl: std::env::var("SPRIG_TEST_STORED_CODE_SPRIGCTL").ok()?,
    })
}

fn ctl(e: &Env, args: &[&str]) -> Result<Value, String> {
    let out = Command::new(&e.sprigctl)
        .args([
            "--endpoint",
            &format!("http://127.0.0.1:{}", e.admin),
            "-o",
            "json",
        ])
        .args(args)
        .env("SPRIGCTL_ACTOR", "live_stored_code")
        .output()
        .map_err(|x| x.to_string())?;
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    if !out.status.success() {
        return Err(format!(
            "sprigctl {args:?}: {text}{}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
}

async fn ctl_retry(e: &Env, args: &[&str]) -> Value {
    let t0 = Instant::now();
    loop {
        match ctl(e, args) {
            Ok(v) => return v,
            Err(x) if t0.elapsed() < Duration::from_secs(60) => {
                eprintln!("retrying: {x}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(x) => panic!("{x}"),
        }
    }
}

fn pool(e: &Env, user: &str, db: Option<&str>) -> Pool {
    Pool::new(
        OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(e.mysql)
            .user(Some(format!("{}.{user}", e.tenant)))
            .pass(Some(PW))
            .ssl_opts(
                SslOpts::default()
                    .with_danger_accept_invalid_certs(true)
                    .with_danger_skip_domain_validation(true),
            )
            .db_name(db)
            .pool_opts(
                PoolOpts::new()
                    .with_constraints(PoolConstraints::new(0, 2).expect("constraints"))
                    .with_inactive_connection_ttl(Duration::from_secs(60)),
            ),
    )
}

fn root_pool(port: u16) -> Pool {
    let pw =
        std::env::var("SPRIG_TEST_ROOT_PASSWORD").unwrap_or_else(|_| "sprig-dev-root".to_owned());
    Pool::new(
        OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(port)
            .user(Some("root"))
            .pass(Some(pw))
            .pool_opts(
                PoolOpts::new()
                    .with_constraints(PoolConstraints::new(0, 1).expect("constraints"))
                    .with_inactive_connection_ttl(Duration::from_secs(60)),
            ),
    )
}

fn tenant_id(v: &Value) -> Value {
    match v {
        Value::Object(m) => m.get("tenant_id").cloned().unwrap_or_else(|| {
            m.values()
                .map(tenant_id)
                .find(|x| !x.is_null())
                .unwrap_or(Value::Null)
        }),
        _ => Value::Null,
    }
}

/// The `cust` group's id and `(shard, cluster)` list.
fn shards(e: &Env) -> Option<(u64, Vec<(u64, String)>)> {
    let l = ctl(e, &["shard", "list", &e.tenant]).ok()?;
    let g = l
        .as_array()?
        .iter()
        .find(|g| g["name"].as_str().is_some_and(|n| n.contains("cust")))?;
    Some((
        g["group"].as_u64()?,
        g["shards"]
            .as_array()?
            .iter()
            .filter_map(|s| {
                Some((
                    s["shard"].as_u64()?,
                    s["cluster"].as_str().unwrap_or_default().to_owned(),
                ))
            })
            .collect(),
    ))
}

async fn wait_shards(
    e: &Env,
    what: &str,
    done: impl Fn(&[(u64, String)]) -> bool,
) -> (u64, Vec<(u64, String)>) {
    let t0 = Instant::now();
    loop {
        if let Some((g, s)) = shards(e)
            && done(&s)
        {
            eprintln!("{what}: group {g} shards {s:?} after {:?}", t0.elapsed());
            return (g, s);
        }
        assert!(
            t0.elapsed() < Duration::from_secs(1800),
            "{what} never happened: {:?}",
            ctl(e, &["shard", "list", &e.tenant])
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn code(x: &mysql_async::Error) -> String {
    match x {
        mysql_async::Error::Server(s) => s.code.to_string(),
        other => format!("{other}"),
    }
}

#[derive(Default)]
struct Tally {
    ok: u64,
    errors: BTreeMap<String, u64>,
    first: BTreeMap<String, String>,
}

fn show(t: &Arc<std::sync::Mutex<Tally>>, what: &str) {
    let t = t.lock().expect("lock");
    eprintln!(
        "  {what}: {} rounds ok, errors {:?} {:?}",
        t.ok, t.errors, t.first
    );
}

/// Every schema of `tenant` holding `table`, on every node.
async fn holders(e: &Env, tenant: u64, table: &str) -> Vec<(u16, String)> {
    let mut out = Vec::new();
    for &port in &e.nodes {
        let p = root_pool(port);
        if let Ok(mut c) = p.get_conn().await {
            let rows: Vec<String> = c
                .exec(
                    "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_SCHEMA \
                     LIKE ? AND TABLE_NAME = ?",
                    (format!("t{tenant}\\_d%"), table),
                )
                .await
                .unwrap_or_default();
            out.extend(rows.into_iter().map(|s| (port, s)));
        }
        p.disconnect().await.ok();
    }
    out.sort();
    out
}

/// `(schema, trigger)` of `tenant`'s triggers on `table`, on every node.
async fn triggers_on(e: &Env, tenant: u64, table: &str) -> Vec<(u16, String, String)> {
    let mut out = Vec::new();
    for &port in &e.nodes {
        let p = root_pool(port);
        if let Ok(mut c) = p.get_conn().await {
            let rows: Vec<(String, String)> = c
                .exec(
                    "SELECT TRIGGER_SCHEMA, TRIGGER_NAME FROM information_schema.TRIGGERS \
                     WHERE TRIGGER_SCHEMA LIKE ? AND EVENT_OBJECT_TABLE = ?",
                    (format!("t{tenant}\\_d%"), table),
                )
                .await
                .unwrap_or_default();
            out.extend(rows.into_iter().map(|(s, n)| (port, s, n)));
        }
        p.disconnect().await.ok();
    }
    out.sort();
    out
}

/// The schema of `tenant`'s stored routine or event `name`.
async fn code_schema(e: &Env, tenant: u64, name: &str) -> Vec<(u16, String)> {
    let mut out = Vec::new();
    for &port in &e.nodes {
        let p = root_pool(port);
        if let Ok(mut c) = p.get_conn().await {
            let like = format!("t{tenant}\\_d%");
            let rows: Vec<String> = c
                .exec(
                    "SELECT ROUTINE_SCHEMA FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA \
                     LIKE ? AND ROUTINE_NAME = ? UNION SELECT EVENT_SCHEMA FROM \
                     information_schema.EVENTS WHERE EVENT_SCHEMA LIKE ? AND EVENT_NAME = ? \
                     UNION SELECT TABLE_SCHEMA FROM information_schema.VIEWS WHERE TABLE_SCHEMA \
                     LIKE ? AND TABLE_NAME = ?",
                    (&like, name, &like, name, &like, name),
                )
                .await
                .unwrap_or_default();
            out.extend(rows.into_iter().map(|s| (port, s)));
        }
        p.disconnect().await.ok();
    }
    out.sort();
    out
}

/// One round of a client: the plain tables, then every piece of stored
/// code.
async fn round(c: &mut mysql_async::Conn, id: u64) -> Result<(), mysql_async::Error> {
    c.exec_drop("INSERT INTO cust (id, v) VALUES (?, ?)", (id, "w"))
        .await?;
    // cust's own trigger fills vu on whatever shard the row lands.
    let vu: Option<Option<String>> = c
        .exec_first("SELECT vu FROM cust WHERE id = ?", (id,))
        .await?;
    if vu != Some(Some("W".to_owned())) {
        return Err(mysql_async::Error::Other(
            format!("cust trigger: vu {vu:?} for {id}").into(),
        ));
    }
    c.exec_drop(
        "INSERT INTO ord (id, cust_id, v) VALUES (?, ?, 'o')",
        (id * 10, id),
    )
    .await?;
    // la's trigger writes la_log.
    c.exec_drop("INSERT INTO la (k, v) VALUES (?, 'x')", (id,))
        .await?;
    let _: Option<u64> = c.query_first("CALL pcount()").await?;
    let n: Option<i64> = c.query_first("SELECT fcount()").await?;
    if n.unwrap_or(0) == 0 {
        return Err(mysql_async::Error::Other(
            "fcount() saw no la_log row: the trigger didn't write".into(),
        ));
    }
    let _: Option<u64> = c.query_first("SELECT COUNT(*) FROM vla").await?;
    let _: Option<u64> = c.query_first("CALL pdyn()").await?;
    let _: Option<u64> = c.query_first("CALL dz.popaque()").await?;
    Ok(())
}

#[tokio::test]
async fn stored_code_survives_regroups_splits_merges_and_moves() {
    let Some(e) = env() else {
        eprintln!("skipped: set SPRIG_TEST_STORED_CODE and SPRIG_TEST_STORED_CODE_SPRIGCTL");
        return;
    };
    let created = ctl_retry(&e, &["tenant", "create", &e.tenant, "--cluster", "dc-a-c1"]).await;
    let tid = tenant_id(&created).as_u64().unwrap_or(0);
    eprintln!("tenant {} created: id {tid}", e.tenant);
    ctl_retry(
        &e,
        &[
            "tenant",
            "user",
            "add",
            &e.tenant,
            "app",
            "--password",
            PW,
            "--if-not-exists",
        ],
    )
    .await;
    let t0 = Instant::now();
    loop {
        let pl = pool(&e, "app", None);
        let r = async {
            let mut c = pl.get_conn().await?;
            for q in [
                "CREATE DATABASE IF NOT EXISTS sc",
                "CREATE DATABASE IF NOT EXISTS dz",
                "USE sc",
                "CREATE TABLE IF NOT EXISTS la (k BIGINT PRIMARY KEY, v TEXT NOT NULL) ENGINE=InnoDB",
                "CREATE TABLE IF NOT EXISTS la_log (id BIGINT AUTO_INCREMENT PRIMARY KEY, k BIGINT, \
                 n BIGINT) ENGINE=InnoDB",
                "CREATE TABLE IF NOT EXISTS cust (id BIGINT PRIMARY KEY, v TEXT NOT NULL, vu TEXT) \
                 ENGINE=InnoDB",
                "CREATE TABLE IF NOT EXISTS ord (id BIGINT PRIMARY KEY, cust_id BIGINT NOT NULL, \
                 v TEXT, FOREIGN KEY (cust_id) REFERENCES cust(id)) ENGINE=InnoDB",
                "CREATE TABLE IF NOT EXISTS dyn_t (k BIGINT PRIMARY KEY) ENGINE=InnoDB",
                "CREATE TABLE IF NOT EXISTS dz.dx (k BIGINT PRIMARY KEY) ENGINE=InnoDB",
                "CREATE TABLE IF NOT EXISTS dz.dy (k BIGINT PRIMARY KEY) ENGINE=InnoDB",
                "INSERT IGNORE INTO la (k, v) VALUES (1, 'a')",
                "INSERT IGNORE INTO cust (id, v, vu) VALUES (1, 'a', 'A'), (2, 'b', 'B')",
                "INSERT IGNORE INTO ord (id, cust_id, v) VALUES (10, 1, 'x')",
                "INSERT IGNORE INTO dyn_t VALUES (1)",
                "INSERT IGNORE INTO dz.dx VALUES (1)",
                "DROP PROCEDURE IF EXISTS pcount",
                "CREATE PROCEDURE pcount() SELECT COUNT(*) FROM la",
                "DROP FUNCTION IF EXISTS fcount",
                "CREATE FUNCTION fcount() RETURNS BIGINT READS SQL DATA \
                 RETURN (SELECT COUNT(*) FROM la_log)",
                "CREATE OR REPLACE VIEW vla AS SELECT k, v FROM la",
                "DROP TRIGGER IF EXISTS tr_la",
                "CREATE TRIGGER tr_la AFTER INSERT ON la FOR EACH ROW \
                 INSERT INTO la_log (k, n) VALUES (NEW.k, 1)",
                "DROP TRIGGER IF EXISTS tr_cust",
                "CREATE TRIGGER tr_cust BEFORE INSERT ON cust FOR EACH ROW SET NEW.vu = UPPER(NEW.v)",
                "DROP EVENT IF EXISTS ev_la",
                "CREATE EVENT ev_la ON SCHEDULE EVERY 1 HOUR DISABLE \
                 DO INSERT INTO la_log (k, n) SELECT 0, COUNT(*) FROM la",
                "DROP PROCEDURE IF EXISTS pdyn",
                "CREATE PROCEDURE pdyn() BEGIN PREPARE s FROM 'SELECT COUNT(*) FROM dyn_t'; \
                 EXECUTE s; DEALLOCATE PREPARE s; END",
                "DROP PROCEDURE IF EXISTS dz.popaque",
                "CREATE PROCEDURE dz.popaque() BEGIN SET @q = CONCAT('SELECT COUNT(*) FROM ', \
                 'dx'); PREPARE s FROM @q; EXECUTE s; DEALLOCATE PREPARE s; END",
                "CREATE USER IF NOT EXISTS 'tbl'@'%' IDENTIFIED BY 'sprig-stored-code-pw'",
                "GRANT SELECT, INSERT ON sc.cust TO 'tbl'@'%'",
                "GRANT SELECT, INSERT ON sc.ord TO 'tbl'@'%'",
                "GRANT SELECT, INSERT ON sc.la TO 'tbl'@'%'",
                "GRANT SELECT ON sc.vla TO 'tbl'@'%'",
                "GRANT EXECUTE ON PROCEDURE sc.pcount TO 'tbl'@'%'",
                "GRANT EXECUTE ON FUNCTION sc.fcount TO 'tbl'@'%'",
                "GRANT EXECUTE ON PROCEDURE sc.pdyn TO 'tbl'@'%'",
                "GRANT EXECUTE ON PROCEDURE dz.popaque TO 'tbl'@'%'",
            ] {
                if let Err(x) = c.query_drop(q).await {
                    return Err(mysql_async::Error::Other(format!("{q}: {x}").into()));
                }
            }
            drop(c);
            Ok::<_, mysql_async::Error>(())
        }
        .await;
        pl.disconnect().await.ok();
        match r {
            Ok(()) => break,
            Err(x) if t0.elapsed() < Duration::from_secs(60) => {
                eprintln!("schema: {x}; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(x) => panic!("schema: {x}"),
        }
    }

    // Both users' clients, throughout.
    let stop = Arc::new(AtomicBool::new(false));
    let next_id = Arc::new(AtomicU64::new(1_000));
    let mut tallies = Vec::new();
    let mut clients = Vec::new();
    for user in ["app", "tbl"] {
        let tally = Arc::new(std::sync::Mutex::new(Tally::default()));
        tallies.push((user, tally.clone()));
        let pl = pool(&e, user, Some("sc"));
        let (stop, next_id) = (stop.clone(), next_id.clone());
        clients.push(tokio::spawn(async move {
            let mut i = 0u64;
            while !stop.load(Ordering::SeqCst) {
                i += 1;
                let id = next_id.fetch_add(1, Ordering::SeqCst);
                let r = async {
                    let mut c = pl.get_conn().await?;
                    round(&mut c, id).await?;
                    if i % 5 == 0 {
                        c.disconnect().await?;
                    }
                    Ok::<_, mysql_async::Error>(())
                }
                .await;
                {
                    let mut t = tally.lock().expect("lock");
                    match r {
                        Ok(()) => t.ok += 1,
                        Err(x) => {
                            let k = code(&x);
                            *t.errors.entry(k.clone()).or_default() += 1;
                            t.first.entry(k).or_insert_with(|| x.to_string());
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            pl.disconnect().await.ok();
        }));
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    for (u, t) in &tallies {
        show(t, &format!("{u} before the regroup"));
    }

    for (k, v) in [
        ("advisor.mode", "off"),
        ("advisor.min_ndv_per_shard", "0"),
        ("sharding.min_shards", "2"),
    ] {
        ctl_retry(
            &e,
            &[
                "settings",
                "set",
                k,
                v,
                "--scope",
                &format!("tenant:{}", e.tenant),
                "--reason",
                "live_stored_code",
                "--yes",
            ],
        )
        .await;
    }
    let mut steps: Vec<String> = Vec::new();
    let (gid, sh) = wait_shards(&e, "regroup", |s| s.len() >= 2).await;
    steps.push(format!("regroup: shards {sh:?}"));
    for (u, t) in &tallies {
        show(t, &format!("{u} after the regroup"));
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    let g = gid.to_string();
    let last = sh.last().expect("a shard").0;
    ctl_retry(&e, &["shard", "split", &e.tenant, &g, &last.to_string()]).await;
    let before: Vec<u64> = sh.iter().map(|s| s.0).collect();
    let (_, sh) = wait_shards(&e, "split", |s| {
        s.len() == before.len() + 1 && !s.iter().any(|x| x.0 == last)
    })
    .await;
    steps.push(format!("split {last}: shards {sh:?}"));
    for (u, t) in &tallies {
        show(t, &format!("{u} after the split"));
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    let kids: Vec<u64> = sh
        .iter()
        .map(|s| s.0)
        .filter(|s| !before.contains(s))
        .collect();
    assert_eq!(kids.len(), 2, "split children: {sh:?}");
    ctl_retry(
        &e,
        &[
            "shard",
            "merge",
            &e.tenant,
            &g,
            &kids[0].to_string(),
            &kids[1].to_string(),
        ],
    )
    .await;
    let n = sh.len();
    let (_, sh) = wait_shards(&e, "merge", |s| {
        s.len() == n - 1 && !s.iter().any(|x| kids.contains(&x.0))
    })
    .await;
    steps.push(format!("merge {kids:?}: shards {sh:?}"));
    for (u, t) in &tallies {
        show(t, &format!("{u} after the merge"));
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    let merged = sh
        .iter()
        .map(|s| s.0)
        .find(|s| !before.contains(s))
        .expect("the merged shard");
    ctl_retry(
        &e,
        &[
            "shard",
            "move",
            &e.tenant,
            &g,
            &merged.to_string(),
            "--to",
            &e.move_to,
        ],
    )
    .await;
    let to = e.move_to.clone();
    let (_, sh) = wait_shards(&e, "move", |s| {
        s.iter().any(|x| x.1 == to) && !s.iter().any(|x| x.0 == merged && x.1 != to)
    })
    .await;
    steps.push(format!("move {merged} to {to}: shards {sh:?}"));
    for (u, t) in &tallies {
        show(t, &format!("{u} after the move"));
    }
    tokio::time::sleep(Duration::from_secs(8)).await;
    stop.store(true, Ordering::SeqCst);
    for c in clients {
        let _ = c.await;
    }
    let mut failed: Vec<String> = Vec::new();
    for (u, t) in &tallies {
        let t = t.lock().expect("lock");
        eprintln!(
            "{u}: {} rounds ok, errors {:?}, first of each {:?}",
            t.ok, t.errors, t.first
        );
        // 1205: the gateway's fence wait running out while data moved
        // (drivers retry it). Anything else is a failure.
        for (k, v) in &t.errors {
            if k != "1205" {
                failed.push(format!("{u}: {v} x {k}: {}", t.first[k]));
            }
        }
        if t.ok == 0 {
            failed.push(format!("{u}: no round succeeded"));
        }
    }
    eprintln!("steps: {steps:?}");

    // Where things are: the code's tables beside it; cust and ord sharded,
    // cust's trigger on every shard of cust.
    if tid > 0 {
        let home = code_schema(&e, tid, "pcount").await;
        eprintln!("pcount in {home:?}");
        for t in ["la", "la_log", "dyn_t"] {
            let h = holders(&e, tid, t).await;
            eprintln!("{t} in {h:?}");
            if h != home {
                failed.push(format!("{t} in {h:?}, pcount in {home:?}"));
            }
        }
        for (obj, tables) in [("fcount", "la_log"), ("vla", "la"), ("ev_la", "la")] {
            let at = code_schema(&e, tid, obj).await;
            let h = holders(&e, tid, tables).await;
            if at != h {
                failed.push(format!("{obj} in {at:?}, {tables} in {h:?}"));
            }
        }
        let dz = code_schema(&e, tid, "popaque").await;
        for t in ["dx", "dy"] {
            let h = holders(&e, tid, t).await;
            eprintln!("dz.{t} in {h:?}");
            if h != dz {
                failed.push(format!("dz.{t} in {h:?}, popaque in {dz:?}"));
            }
        }
        let cust = holders(&e, tid, "cust").await;
        let trg = triggers_on(&e, tid, "cust").await;
        eprintln!("cust in {cust:?}; its triggers {trg:?}");
        if cust.iter().any(|h| home.contains(h)) || cust.len() < 2 {
            failed.push(format!("cust wasn't sharded: {cust:?}"));
        }
        for (port, s) in &cust {
            if !trg
                .iter()
                .any(|(p, ts, n)| p == port && ts == s && n == "tr_cust")
            {
                failed.push(format!("no tr_cust on {s} at {port}"));
            }
        }
    }
    // The run's evidence says why each table stayed.
    match ctl(&e, &["advisor", "show", &e.tenant]) {
        Ok(v) => {
            let text = v.to_string();
            for t in ["la", "la_log", "dyn_t", "dx", "dy"] {
                let line = format!("{t}: stays unsharded because stored code");
                if !text.contains(&line) {
                    failed.push(format!("the advisor's evidence has no \"{line}\""));
                }
            }
            eprintln!("advisor evidence: {}", v["evidence"]["notes"]);
        }
        Err(x) => failed.push(format!("advisor show: {x}")),
    }

    // Cleanup: the tenant this test created, by name.
    {
        let pl = pool(&e, "app", None);
        if let Ok(mut c) = pl.get_conn().await {
            let _ = c.query_drop("DROP DATABASE IF EXISTS sc").await;
            let _ = c.query_drop("DROP DATABASE IF EXISTS dz").await;
        }
        pl.disconnect().await.ok();
    }
    if std::env::var("SPRIG_TEST_STORED_CODE_KEEP").is_err()
        && let Err(x) = ctl(&e, &["tenant", "delete", &e.tenant, "--yes"])
    {
        eprintln!("tenant delete: {x}");
    }
    assert!(failed.is_empty(), "{failed:#?}");
}
