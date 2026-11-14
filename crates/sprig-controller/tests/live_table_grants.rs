//! Table, column and routine grants follow a table through regroups,
//! splits, merges and moves.
//!
//! - A user whose only privileges are on single tables, columns and a
//!   procedure of `gr` (`GRANT ... ON gr.cust`, `GRANT SELECT (k, v),
//!   UPDATE (v) ON gr.la`, `GRANT EXECUTE ON PROCEDURE gr.pcount`) reads
//!   and writes them from two clients the whole time while the sharding
//!   loop regroups `cust` into a group of its own, and while that group's
//!   shards are split, merged and moved to another cluster. Before the fix
//!   the user's backend account held each table grant on the database's
//!   first schema only, so every statement on `cust` failed with 1142 once
//!   it was regrouped.
//! - A user granted only `ord` still gets 1142 on `cust`, and a user
//!   without a grant on `gr` gets 1142 everywhere in it.
//! - MariaDB's own grant rows match the table's schemas: on every cluster,
//!   each schema holding `cust` has the user's row and no row names a
//!   schema without it; a `REVOKE` reaches every one of them.
//!
//! Runs against a deployment `deploy/compose/gateway-dev.sh up` started
//! (its own etcd and tenant-id range), with a second cluster to move to:
//!
//! ```sh
//! SPRIG_TEST_TABLE_GRANTS=<gateway mysql port>,<admin port> \
//! SPRIG_TEST_TABLE_GRANTS_TENANT=<a new tenant name> \
//! SPRIG_TEST_TABLE_GRANTS_MOVE_TO=dc-a-c2 \
//! SPRIG_TEST_TABLE_GRANTS_NODES=13301,13311 \
//!   cargo test -p sprig-controller --test live_table_grants -- --nocapture
//! ```
//!
//! `..._NODES` are the writers of the clusters the tenant uses (read as
//! `root`, password `SPRIG_TEST_ROOT_PASSWORD`, default `sprig-dev-root`).
//! It deletes the tenant at the end.

#![allow(
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::many_single_char_names,
    missing_docs
)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{OptsBuilder, Pool, PoolConstraints, PoolOpts, SslOpts};
use serde_json::Value;

const PW: &str = "sprig-table-grants-pw";

struct Env {
    mysql: u16,
    admin: u16,
    tenant: String,
    move_to: String,
    nodes: Vec<u16>,
    sprigctl: PathBuf,
}

fn env() -> Option<Env> {
    let v = std::env::var("SPRIG_TEST_TABLE_GRANTS").ok()?;
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
        tenant: std::env::var("SPRIG_TEST_TABLE_GRANTS_TENANT")
            .unwrap_or_else(|_| "tablegrants".to_owned()),
        move_to: std::env::var("SPRIG_TEST_TABLE_GRANTS_MOVE_TO")
            .unwrap_or_else(|_| "dc-a-c2".to_owned()),
        nodes: std::env::var("SPRIG_TEST_TABLE_GRANTS_NODES")
            .unwrap_or_else(|_| "13301,13311".to_owned())
            .split(',')
            .filter_map(|p| p.trim().parse().ok())
            .collect(),
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
        .env("SPRIGCTL_ACTOR", "live_table_grants")
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
        let limit = std::env::var("SPRIG_TEST_TABLE_GRANTS_WAIT_SECS")
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

/// What the table user's clients saw.
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

/// One node's view of a table: its port, the schemas holding the table,
/// and the grant rows ((account, schema) → privileges) on it.
type NodeRows = (u16, BTreeSet<String>, BTreeMap<(String, String), String>);

/// On every node: the schemas of `tenant` holding table `table`, and the
/// grant rows (schema → privileges) of `tenant`'s accounts on it.
async fn grant_rows(e: &Env, tenant: u64, table: &str) -> Vec<NodeRows> {
    let mut out = Vec::new();
    for &port in &e.nodes {
        let p = root_pool(port);
        let r = async {
            let mut c = p.get_conn().await?;
            let like = format!("t{tenant}\\_d%");
            let holders: Vec<String> = c
                .exec(
                    "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_SCHEMA LIKE ? \
                     AND TABLE_NAME = ?",
                    (&like, table),
                )
                .await?;
            let rows: Vec<(String, String, String)> = c
                .exec(
                    "SELECT User, Db, Table_priv FROM mysql.tables_priv WHERE User LIKE ? \
                     AND Table_name = ?",
                    (format!("t{tenant}\\_u%"), table),
                )
                .await?;
            Ok::<_, mysql_async::Error>((
                holders.into_iter().collect::<BTreeSet<_>>(),
                rows.into_iter()
                    .map(|(u, d, p)| ((u, d), p))
                    .collect::<BTreeMap<_, _>>(),
            ))
        }
        .await;
        p.disconnect().await.ok();
        match r {
            Ok((h, g)) => out.push((port, h, g)),
            Err(x) => eprintln!("node {port}: {x}"),
        }
    }
    out
}

#[tokio::test]
async fn table_grants_follow_regroups_splits_merges_and_moves() {
    let Some(e) = env() else {
        eprintln!("skipped: set SPRIG_TEST_TABLE_GRANTS=<gateway mysql port>,<admin port>");
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
        let pl = pool(&e, "app", PW, None);
        let r = async {
            let mut c = pl.get_conn().await?;
            for q in [
                "CREATE DATABASE IF NOT EXISTS gr",
                "CREATE DATABASE IF NOT EXISTS other",
                "CREATE TABLE IF NOT EXISTS gr.la (k INT PRIMARY KEY, v TEXT NOT NULL, secret TEXT) ENGINE=InnoDB",
                "CREATE TABLE IF NOT EXISTS gr.cust (id BIGINT PRIMARY KEY, v TEXT NOT NULL) ENGINE=InnoDB",
                "CREATE TABLE IF NOT EXISTS gr.ord (id BIGINT PRIMARY KEY, cust_id BIGINT NOT NULL, v TEXT, \
                 FOREIGN KEY (cust_id) REFERENCES cust(id)) ENGINE=InnoDB",
                "CREATE TABLE IF NOT EXISTS other.o (k INT PRIMARY KEY) ENGINE=InnoDB",
                "INSERT IGNORE INTO gr.la (k, v, secret) VALUES (100, '0', 's')",
                "INSERT IGNORE INTO gr.cust (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c')",
                "INSERT IGNORE INTO gr.ord (id, cust_id, v) VALUES (10, 1, 'x'), (20, 2, 'y')",
                "DROP PROCEDURE IF EXISTS gr.pcount",
                // The routine names no table: a body naming a table the
                // advisor regroups fails with 1146 for every user once the
                // table leaves the default schema.
                "CREATE PROCEDURE gr.pcount() SELECT 41 + 1",
                "CREATE USER IF NOT EXISTS 'tbl'@'%' IDENTIFIED BY 'sprig-table-grants-pw'",
                "GRANT SELECT, INSERT, UPDATE ON gr.cust TO 'tbl'@'%'",
                "GRANT SELECT, INSERT ON gr.ord TO 'tbl'@'%'",
                "GRANT SELECT (k, v), UPDATE (v) ON gr.la TO 'tbl'@'%'",
                "GRANT EXECUTE ON PROCEDURE gr.pcount TO 'tbl'@'%'",
                "CREATE USER IF NOT EXISTS 'ordonly'@'%' IDENTIFIED BY 'sprig-table-grants-pw'",
                "GRANT SELECT ON gr.ord TO 'ordonly'@'%'",
                "CREATE USER IF NOT EXISTS 'nogrant'@'%' IDENTIFIED BY 'sprig-table-grants-pw'",
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

    // The table user's clients, throughout, each on a pool that sometimes
    // opens fresh connections.
    let stop = Arc::new(AtomicBool::new(false));
    let tally = Arc::new(std::sync::Mutex::new(Tally::default()));
    let next_id = Arc::new(AtomicU64::new(1_000));
    let mut clients = Vec::new();
    for n in 0..2u64 {
        let pl = pool(&e, "tbl", PW, Some("gr"));
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
                    // Column grants.
                    c.query_drop("UPDATE la SET v = CONCAT(v, '') WHERE k = 100")
                        .await?;
                    let _: Option<(i64, String)> = c.query_first("SELECT k, v FROM la").await?;
                    let _: Option<u64> = c
                        .query_first("SELECT COUNT(*) FROM cust c JOIN ord o ON o.cust_id = c.id")
                        .await?;
                    // The routine grant.
                    let _: Option<u64> = c.query_first("CALL pcount()").await?;
                    if i % 5 == n {
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
                "live_table_grants",
                "--yes",
            ],
        )
        .await;
    }
    let mut steps: Vec<String> = Vec::new();
    let (gid, sh) = wait_shards(&e, "regroup", |s| s.len() >= 2).await;
    steps.push(format!("regroup: shards {sh:?}"));
    show(&tally, "after the regroup");
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
    show(&tally, "after the split");
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
    show(&tally, "after the merge");
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
    show(&tally, "after the move");
    tokio::time::sleep(Duration::from_secs(8)).await;
    stop.store(true, Ordering::SeqCst);
    for c in clients {
        let _ = c.await;
    }
    let mut failed: Vec<String> = Vec::new();
    {
        let t = tally.lock().expect("lock");
        eprintln!(
            "table user: {} rounds ok, errors {:?}, first of each {:?}",
            t.ok, t.errors, t.first
        );
        // 1205: the gateway's fence wait running out while the data kept
        // moving; drivers retry it. Anything else is a failure.
        for (k, v) in &t.errors {
            if k != "1205" {
                failed.push(format!("table user: {v} x {k}: {}", t.first[k]));
            }
        }
        if t.ok == 0 {
            failed.push("table user: no round succeeded".into());
        }
    }
    eprintln!("steps: {steps:?}");

    // MariaDB's rows for `cust` match the schemas holding it, on every
    // cluster: one row per holding schema, none elsewhere.
    let check_rows = |rows: &[NodeRows], want_insert: bool, failed: &mut Vec<String>| {
        let mut holders_total = 0;
        for (port, holders, grants) in rows {
            holders_total += holders.len();
            let users: BTreeSet<&String> = grants.keys().map(|(u, _)| u).collect();
            eprintln!("node {port}: cust in {holders:?}; rows {grants:?}");
            for ((u, db), privs) in grants {
                if !holders.contains(db) {
                    failed.push(format!(
                        "node {port}: {u} keeps a row on {db}, which has no cust"
                    ));
                }
                if privs.contains("Insert") != want_insert {
                    failed.push(format!(
                        "node {port}: {u} on {db}: {privs} (INSERT wanted: {want_insert})"
                    ));
                }
            }
            if users.len() != 1 {
                failed.push(format!(
                    "node {port}: accounts with cust rows: {users:?} (one wanted)"
                ));
            }
            for h in holders {
                if !grants.keys().any(|(_, d)| d == h) {
                    failed.push(format!("node {port}: no row on {h}, which holds cust"));
                }
            }
        }
        if holders_total == 0 {
            failed.push("no node holds cust".into());
        }
    };
    if tid > 0 {
        let rows = grant_rows(&e, tid, "cust").await;
        check_rows(&rows, true, &mut failed);
    }

    // A user granted only `ord`, and one without any grant on `gr`.
    for (user, queries) in [
        (
            "ordonly",
            vec![
                "SELECT COUNT(*) FROM gr.cust",
                "SELECT v FROM gr.cust WHERE id = 1",
                "INSERT INTO gr.cust (id, v) VALUES (5, 'n')",
                "SELECT secret FROM gr.la",
            ],
        ),
        (
            "nogrant",
            vec![
                "SELECT COUNT(*) FROM gr.cust",
                "SELECT v FROM gr.cust WHERE id = 1",
                "INSERT INTO gr.cust (id, v) VALUES (5, 'n')",
                "SELECT k FROM gr.la",
                "SELECT id FROM gr.ord",
            ],
        ),
    ] {
        let p = pool(&e, user, PW, None);
        let mut c = p.get_conn().await.expect("login");
        for q in queries {
            match c.query_drop(q).await {
                Err(x) if ["1142", "1143"].contains(&code(&x).as_str()) => {}
                other => failed.push(format!("{user} {q}: {other:?} (wanted 1142)")),
            }
        }
        if user == "ordonly"
            && let Err(x) = c.query_drop("SELECT id FROM gr.ord").await
        {
            failed.push(format!("ordonly on ord: {x}"));
        }
        drop(c);
        p.disconnect().await.ok();
    }
    // The table user's column grant doesn't reach other columns.
    {
        let p = pool(&e, "tbl", PW, Some("gr"));
        let mut c = p.get_conn().await.expect("tbl login");
        match c.query_drop("SELECT secret FROM la").await {
            Err(x) if ["1142", "1143"].contains(&code(&x).as_str()) => {}
            other => failed.push(format!("tbl SELECT secret: {other:?} (wanted 1143)")),
        }
        drop(c);
        p.disconnect().await.ok();
    }

    // REVOKE reaches every schema holding cust.
    {
        let p = pool(&e, "app", PW, None);
        let mut c = p.get_conn().await.expect("app login");
        c.query_drop("REVOKE INSERT ON gr.cust FROM 'tbl'@'%'")
            .await
            .expect("revoke");
        drop(c);
        p.disconnect().await.ok();
        // Every gateway session's next statement reconciles; the issuing
        // one already did on its own cluster. One statement on each.
        let p = pool(&e, "tbl", PW, Some("gr"));
        if let Ok(mut c) = p.get_conn().await {
            let _ = c.query_drop("SELECT COUNT(*) FROM cust").await;
            match c
                .query_drop("INSERT INTO cust (id, v) VALUES (7, 'r')")
                .await
            {
                Err(x) if code(&x) == "1142" => {}
                other => failed.push(format!("tbl INSERT after REVOKE: {other:?}")),
            }
        }
        p.disconnect().await.ok();
        tokio::time::sleep(Duration::from_secs(2)).await;
        if tid > 0 {
            let rows = grant_rows(&e, tid, "cust").await;
            check_rows(&rows, false, &mut failed);
        }
    }

    // Cleanup: the tenant this test created, by name.
    {
        let pl = pool(&e, "app", PW, None);
        if let Ok(mut c) = pl.get_conn().await {
            let _ = c.query_drop("DROP DATABASE IF EXISTS gr").await;
            let _ = c.query_drop("DROP DATABASE IF EXISTS other").await;
        }
        pl.disconnect().await.ok();
    }
    if std::env::var("SPRIG_TEST_TABLE_GRANTS_KEEP").is_err()
        && let Err(x) = ctl(&e, &["tenant", "delete", &e.tenant, "--yes"])
    {
        eprintln!("tenant delete: {x}");
    }
    assert!(failed.is_empty(), "{failed:#?}");
}
