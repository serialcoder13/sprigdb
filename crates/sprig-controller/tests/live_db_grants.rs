//! bugfix-19 and bugfix-20 (decisions/bugfix-interim.md): backend grants
//! follow every schema of a database, and gathered and analytic reads
//! work for a user who may only `SELECT`.
//!
//! - A user with only `db.*` privileges reads and writes the database's
//!   tables while the sharding loop regroups them into a group of their
//!   own, and while that group's shards are split, merged and moved to
//!   another cluster. Before the fix, the user's backend account held
//!   `db.*` on the database's first schema only, so every statement on a
//!   regrouped table failed with 1142 (`*.*` users were fine).
//! - A user without a grant on the database still gets 1142 there.
//! - A user with only `SELECT` runs a gathered read (a window function
//!   over the sharded table) and an analytic (DataFusion) read. Before,
//!   both failed with 39420: the gateway's temporary copies need
//!   `CREATE TEMPORARY TABLES` on the backend account. The user still
//!   can't create a temporary table of its own.
//!
//! Runs against a deployment `deploy/compose/gateway-dev.sh up` started
//! (its own etcd and tenant-id range), with a second cluster to move to:
//!
//! ```sh
//! SPRIG_TEST_DB_GRANTS=<gateway mysql port>,<admin port> \
//! SPRIG_TEST_DB_GRANTS_TENANT=<a new tenant name> \
//! SPRIG_TEST_DB_GRANTS_MOVE_TO=dc-a-c2 \
//!   cargo test -p sprig-controller --test live_db_grants -- --nocapture
//! ```
//!
//! It deletes the tenant at the end.

#![allow(
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::many_single_char_names,
    missing_docs
)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{OptsBuilder, Pool, PoolConstraints, PoolOpts, SslOpts};
use serde_json::Value;

const PW: &str = "sprig-db-grants-pw";

struct Env {
    mysql: u16,
    admin: u16,
    tenant: String,
    move_to: String,
    sprigctl: PathBuf,
}

fn env() -> Option<Env> {
    let v = std::env::var("SPRIG_TEST_DB_GRANTS").ok()?;
    let (m, a) = v.split_once(',')?;
    let target = std::env::var("CARGO_TARGET_DIR").map_or_else(
        |_| PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../target")),
        PathBuf::from,
    );
    let sprigctl = target.join("debug").join("sprigctl");
    if !sprigctl.exists() {
        eprintln!("skipped: build sprigctl first ({})", sprigctl.display());
        return None;
    }
    Some(Env {
        mysql: m.trim().parse().ok()?,
        admin: a.trim().parse().ok()?,
        tenant: std::env::var("SPRIG_TEST_DB_GRANTS_TENANT")
            .unwrap_or_else(|_| "dbgrants".to_owned()),
        move_to: std::env::var("SPRIG_TEST_DB_GRANTS_MOVE_TO")
            .unwrap_or_else(|_| "dc-a-c2".to_owned()),
        sprigctl,
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
        .env("SPRIGCTL_ACTOR", "live_db_grants")
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

fn pool(e: &Env, user: &str, pw: &str, db: Option<&str>) -> Pool {
    Pool::new(
        OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(e.mysql)
            .user(Some(format!("{}.{user}", e.tenant)))
            .pass(Some(pw))
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

/// The first `tenant_id` anywhere in `v`.
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

/// Waits until `done` holds for the group's shards.
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
        let limit = std::env::var("SPRIG_TEST_DB_GRANTS_WAIT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1800);
        assert!(
            t0.elapsed() < Duration::from_secs(limit),
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

/// What the `db.*` user's clients saw.
#[derive(Default)]
struct Tally {
    ok: u64,
    errors: BTreeMap<String, u64>,
    first: BTreeMap<String, String>,
}

#[tokio::test]
async fn database_grants_follow_regroups_splits_merges_and_moves() {
    let Some(e) = env() else {
        eprintln!("skipped: set SPRIG_TEST_DB_GRANTS=<gateway mysql port>,<admin port>");
        return;
    };
    let created = ctl_retry(&e, &["tenant", "create", &e.tenant, "--cluster", "dc-a-c1"]).await;
    eprintln!("tenant {} created: id {}", e.tenant, tenant_id(&created));
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
    // The schema and the three users, before the settings that shard it.
    let t0 = Instant::now();
    loop {
        let pl = pool(&e, "app", PW, None);
        let r = async {
            let mut c = pl.get_conn().await?;
            for q in [
                "CREATE DATABASE IF NOT EXISTS gr",
                "CREATE DATABASE IF NOT EXISTS other",
                "CREATE TABLE IF NOT EXISTS gr.la (k INT PRIMARY KEY, v TEXT NOT NULL) ENGINE=InnoDB",
                "CREATE TABLE IF NOT EXISTS gr.cust (id BIGINT PRIMARY KEY, v TEXT NOT NULL) ENGINE=InnoDB",
                "CREATE TABLE IF NOT EXISTS gr.ord (id BIGINT PRIMARY KEY, cust_id BIGINT NOT NULL, v TEXT, \
                 FOREIGN KEY (cust_id) REFERENCES cust(id)) ENGINE=InnoDB",
                "CREATE TABLE IF NOT EXISTS other.o (k INT PRIMARY KEY) ENGINE=InnoDB",
                "INSERT IGNORE INTO gr.la (k, v) VALUES (100, '0')",
                "INSERT IGNORE INTO gr.cust (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c')",
                "INSERT IGNORE INTO gr.ord (id, cust_id, v) VALUES (10, 1, 'x'), (20, 2, 'y')",
                "CREATE USER IF NOT EXISTS 'dbonly'@'%' IDENTIFIED BY 'sprig-db-grants-pw'",
                "GRANT SELECT, INSERT, UPDATE, DELETE ON gr.* TO 'dbonly'@'%'",
                "CREATE USER IF NOT EXISTS 'reader'@'%' IDENTIFIED BY 'sprig-db-grants-pw'",
                "GRANT SELECT ON gr.* TO 'reader'@'%'",
                "CREATE USER IF NOT EXISTS 'nogrant'@'%' IDENTIFIED BY 'sprig-db-grants-pw'",
                "GRANT SELECT ON other.* TO 'nogrant'@'%'",
            ] {
                c.query_drop(q).await?;
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

    // The db.* user's clients: writes and reads of all three tables,
    // throughout, each on a pool that sometimes opens fresh connections.
    let stop = Arc::new(AtomicBool::new(false));
    let tally = Arc::new(std::sync::Mutex::new(Tally::default()));
    let next_id = Arc::new(AtomicU64::new(1_000));
    let mut clients = Vec::new();
    for n in 0..2u64 {
        let pl = pool(&e, "dbonly", PW, Some("gr"));
        let (stop, tally, next_id) = (stop.clone(), tally.clone(), next_id.clone());
        clients.push(tokio::spawn(async move {
            let mut i = 0u64;
            while !stop.load(Ordering::SeqCst) {
                i += 1;
                let id = next_id.fetch_add(1, Ordering::SeqCst);
                let r = async {
                    let mut c = pl.get_conn().await?;
                    c.exec_drop("INSERT INTO cust (id, v) VALUES (?, ?)", (id, "w"))
                        .await?;
                    c.exec_drop(
                        "INSERT INTO ord (id, cust_id, v) VALUES (?, ?, 'o')",
                        (id * 10, id),
                    )
                    .await?;
                    let got: Option<String> = c
                        .exec_first("SELECT v FROM cust WHERE id = ?", (id,))
                        .await?;
                    if got.as_deref() != Some("w") {
                        return Err(mysql_async::Error::Other(
                            format!("read back {got:?} for {id}").into(),
                        ));
                    }
                    c.query_drop(format!("UPDATE cust SET v = 'u' WHERE id = {id}"))
                        .await?;
                    c.query_drop("UPDATE la SET v = CONCAT(v, '') WHERE k = 100")
                        .await?;
                    let _: Option<u64> = c
                        .query_first("SELECT COUNT(*) FROM cust c JOIN ord o ON o.cust_id = c.id")
                        .await?;
                    if i % 5 == n {
                        // A fresh connection now and then.
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

    // The loop's first design: the group gets its own schemas.
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
                "live_db_grants",
                "--yes",
            ],
        )
        .await;
    }
    let mut steps: Vec<String> = Vec::new();
    let (gid, sh) = wait_shards(&e, "regroup", |s| s.len() >= 2).await;
    steps.push(format!("regroup: shards {sh:?}"));
    {
        let t = tally.lock().expect("lock");
        eprintln!(
            "  db.* user so far: {} rounds ok, errors {:?} {:?}",
            t.ok, t.errors, t.first
        );
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    let g = gid.to_string();
    // A split of the last shard.
    let last = sh.last().expect("a shard").0;
    ctl_retry(&e, &["shard", "split", &e.tenant, &g, &last.to_string()]).await;
    let before: Vec<u64> = sh.iter().map(|s| s.0).collect();
    let (_, sh) = wait_shards(&e, "split", |s| {
        s.len() == before.len() + 1 && !s.iter().any(|x| x.0 == last)
    })
    .await;
    steps.push(format!("split {last}: shards {sh:?}"));
    {
        let t = tally.lock().expect("lock");
        eprintln!(
            "  db.* user so far: {} rounds ok, errors {:?} {:?}",
            t.ok, t.errors, t.first
        );
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    // The two children merged again.
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
    {
        let t = tally.lock().expect("lock");
        eprintln!(
            "  db.* user so far: {} rounds ok, errors {:?} {:?}",
            t.ok, t.errors, t.first
        );
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    // A move of the merged shard to the other cluster.
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
    {
        let t = tally.lock().expect("lock");
        eprintln!(
            "  db.* user so far: {} rounds ok, errors {:?} {:?}",
            t.ok, t.errors, t.first
        );
    }
    tokio::time::sleep(Duration::from_secs(8)).await;
    stop.store(true, Ordering::SeqCst);
    for c in clients {
        let _ = c.await;
    }
    let mut failed: Vec<String> = Vec::new();
    {
        let t = tally.lock().expect("lock");
        eprintln!(
            "db.* user: {} rounds ok, errors {:?}, first of each {:?}",
            t.ok, t.errors, t.first
        );
        // 1205: the gateway's fence wait running out while the data kept
        // moving (H3); drivers retry it. Anything else is a failure.
        for (k, v) in &t.errors {
            if k != "1205" {
                failed.push(format!("db.* user: {v} x {k}: {}", t.first[k]));
            }
        }
        if t.ok == 0 {
            failed.push("db.* user: no round succeeded".into());
        }
    }
    eprintln!("steps: {steps:?}");

    // Bug 2: gathered and analytic reads for a SELECT-only user.
    let rd = pool(&e, "reader", PW, Some("gr"));
    let mut c = rd.get_conn().await.expect("reader login");
    let total: Option<u64> = c
        .query_first("SELECT COUNT(*) FROM cust")
        .await
        .expect("reader count");
    for (what, q) in [
        (
            "gathered window",
            "SELECT MAX(rn) FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM cust) x",
        ),
        (
            "analytic window",
            "SELECT /*+ SPRIG_ANALYTICS */ MAX(rn) FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM cust) x",
        ),
    ] {
        match c.query_first::<u64, _>(q).await {
            Ok(got) if got == total => eprintln!("{what}: {got:?}"),
            other => failed.push(format!("reader {what}: {other:?} (COUNT(*) {total:?})")),
        }
    }
    match c.query_drop("CREATE TEMPORARY TABLE mine (a INT)").await {
        Err(x) if ["1044", "1142"].contains(&code(&x).as_str()) => {
            eprintln!("reader's own temporary table refused: {x}");
        }
        other => failed.push(format!("reader CREATE TEMPORARY TABLE: {other:?}")),
    }
    drop(c);
    rd.disconnect().await.ok();

    // A user without a grant on gr still can't read or write it.
    let ng = pool(&e, "nogrant", PW, None);
    let mut c = ng.get_conn().await.expect("nogrant login");
    for q in [
        "SELECT COUNT(*) FROM gr.cust",
        "SELECT v FROM gr.cust WHERE id = 1",
        "INSERT INTO gr.cust (id, v) VALUES (5, 'n')",
        "SELECT k FROM gr.la",
    ] {
        match c.query_drop(q).await {
            Err(x) if code(&x) == "1142" => {}
            other => failed.push(format!("nogrant {q}: {other:?} (wanted 1142)")),
        }
    }
    match c.query_drop("SELECT k FROM other.o").await {
        Ok(()) => {}
        other => failed.push(format!("nogrant on its own database: {other:?}")),
    }
    drop(c);
    ng.disconnect().await.ok();

    // Cleanup: the tenant this test created, by name.
    {
        let pl = pool(&e, "app", PW, None);
        if let Ok(mut c) = pl.get_conn().await {
            let _ = c.query_drop("DROP DATABASE IF EXISTS gr").await;
            let _ = c.query_drop("DROP DATABASE IF EXISTS other").await;
        }
        pl.disconnect().await.ok();
    }
    if std::env::var("SPRIG_TEST_DB_GRANTS_KEEP").is_err()
        && let Err(x) = ctl(&e, &["tenant", "delete", &e.tenant, "--yes"])
    {
        eprintln!("tenant delete: {x}");
    }
    assert!(failed.is_empty(), "{failed:#?}");
}
