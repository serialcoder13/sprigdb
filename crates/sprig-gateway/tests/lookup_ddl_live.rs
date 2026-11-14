//! Global lookup indexes through the gateway (chapter 19's DDL path,
//! chapter 21's `ADD UNIQUE` on a sharded table), live on a private
//! deployment:
//!
//! 1. `customers (id PK, email, name)` with 20,000 rows, pinned
//!    `SHARD KEY (id)`, made a group by the advisor and split into 4 shards.
//!    A point read by `email` goes to every shard (the gateway's
//!    `sprig_router_fanout_shards` histogram: 4 per read).
//! 2. `ALTER TABLE customers ADD UNIQUE KEY uk_email (email)` while 4
//!    writers insert, change emails and delete. Afterwards the same reads go
//!    to 1 shard each, `SPRIG SHOW LOOKUP INDEXES` and
//!    `information_schema.SPRIG_LOOKUP_INDEXES` show it `public`, and every
//!    row has exactly its lookup row (checked on the lookup shards).
//! 3. Uniqueness across shards: a duplicate insert and an update to a taken
//!    email fail with 1062; an update to a new email moves the row's
//!    lookup row; a delete releases it.
//! 4. A split of one shard while point reads run: reads stay right and
//!    narrow.
//! 5. `kill -9` of the gateway while a rebuilt lookup back-fills (drop,
//!    40,000 more rows, `ADD UNIQUE` again): the controller takes the
//!    stopped build over after `BUILD_LEASE` and makes it public; the rows
//!    and lookup rows match again.
//!
//! ```sh
//! source target/agent-lookupddl/env.sh   # a private deployment (gateway-dev.sh up)
//! export SPRIG_TEST_LKD_GATEWAY=127.0.0.1:46306 SPRIG_TEST_LKD_METRICS=127.0.0.1:46190
//! export SPRIG_TEST_LKD_TENANT=acme SPRIG_TEST_LKD_TENANT_ID=<id>
//! export SPRIG_TEST_LKD_CLUSTERS=127.0.0.1:13301,127.0.0.1:13311   # root, every cluster
//! export SPRIG_TEST_LKD_SPRIGCTL=target/agent-lookupddl/debug/sprigctl
//! export SPRIG_TEST_LKD_GW_PIDFILE=target/agent-lookupddl/gwdev/gateway.pid
//! export SPRIG_TEST_LKD_GW_RESTART='deploy/compose/gateway-dev.sh restart'
//! cargo test -p sprig-gateway --test lookup_ddl_live -- --nocapture --test-threads 1
//! ```
//!
//! Without `SPRIG_TEST_LKD_GATEWAY` the test says what it needs and passes.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    missing_docs
)]

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read as _, Write as _};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Pool, PoolConstraints, PoolOpts, Row};

const DB: &str = "lookupddl_db";

struct Env {
    gateway: String,
    metrics: String,
    tenant: String,
    tenant_id: u64,
    clusters: Vec<String>,
    sprigctl: String,
    pidfile: String,
    restart: String,
}

fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

impl Env {
    fn load() -> Option<Self> {
        Some(Self {
            gateway: var("SPRIG_TEST_LKD_GATEWAY")?,
            metrics: var("SPRIG_TEST_LKD_METRICS")?,
            tenant: var("SPRIG_TEST_LKD_TENANT").unwrap_or_else(|| "acme".into()),
            tenant_id: var("SPRIG_TEST_LKD_TENANT_ID")?.parse().ok()?,
            clusters: var("SPRIG_TEST_LKD_CLUSTERS")?
                .split(',')
                .map(|s| s.trim().to_owned())
                .collect(),
            sprigctl: var("SPRIG_TEST_LKD_SPRIGCTL")?,
            pidfile: var("SPRIG_TEST_LKD_GW_PIDFILE")?,
            restart: var("SPRIG_TEST_LKD_GW_RESTART")?,
        })
    }

    fn app(&self) -> Pool {
        let (h, p) = self.gateway.split_once(':').unwrap();
        let opts = OptsBuilder::default()
            .ip_or_hostname(h)
            .tcp_port(p.parse().unwrap())
            .user(Some(format!("{}.app", self.tenant)))
            .pass(Some("sprig-dev-app"))
            .db_name(Some(DB))
            .pool_opts(
                PoolOpts::default()
                    .with_constraints(PoolConstraints::new(0, 16).unwrap())
                    .with_inactive_connection_ttl(Duration::from_secs(60)),
            );
        Pool::new(opts)
    }

    async fn root(&self, i: usize) -> Conn {
        let (h, p) = self.clusters[i].split_once(':').unwrap();
        Conn::new(
            OptsBuilder::default()
                .ip_or_hostname(h)
                .tcp_port(p.parse().unwrap())
                .user(Some("root"))
                .pass(Some("sprig-dev-root")),
        )
        .await
        .unwrap()
    }

    fn sprigctl(&self, args: &[&str]) -> String {
        let out = std::process::Command::new(&self.sprigctl)
            .args(args)
            .output()
            .expect("sprigctl");
        let text = String::from_utf8_lossy(&out.stdout).into_owned();
        assert!(
            out.status.success(),
            "sprigctl {args:?}: {text}{}",
            String::from_utf8_lossy(&out.stderr)
        );
        text
    }
}

/// `sprig_router_fanout_shards` (sum, count) from the gateway's metrics.
fn fanout(addr: &str) -> (f64, f64) {
    let mut s = std::net::TcpStream::connect(addr).expect("metrics");
    write!(
        s,
        "GET /metrics HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut body = String::new();
    s.read_to_string(&mut body).unwrap();
    let (mut sum, mut count) = (0.0, 0.0);
    for line in body.lines() {
        let v = |l: &str| {
            l.rsplit(' ')
                .next()
                .and_then(|x| x.parse::<f64>().ok())
                .unwrap_or(0.0)
        };
        if line.starts_with("sprig_router_fanout_shards_sum") {
            sum += v(line);
        } else if line.starts_with("sprig_router_fanout_shards_count") {
            count += v(line);
        }
    }
    (sum, count)
}

/// Shards per point read by email over `n` reads.
async fn shards_per_read(e: &Env, c: &mut Conn, ids: &[i64]) -> f64 {
    let (s0, n0) = fanout(&e.metrics);
    for id in ids {
        let r: Option<i64> = c
            .exec_first(
                "SELECT id FROM customers WHERE email = ?",
                (format!("person{id}@x.org"),),
            )
            .await
            .unwrap();
        assert_eq!(r, Some(*id), "point read of person{id}");
    }
    let (s1, n1) = fanout(&e.metrics);
    (s1 - s0) / (n1 - n0).max(1.0)
}

async fn errno_of<T>(r: mysql_async::Result<T>) -> Option<u16> {
    match r {
        Err(mysql_async::Error::Server(s)) => Some(s.code),
        _ => None,
    }
}

/// Every lookup row of `uk_email`: email (lowercase) → owner ids.
async fn lookup_rows(e: &Env) -> BTreeMap<String, Vec<i64>> {
    let mut out: BTreeMap<String, Vec<i64>> = BTreeMap::new();
    for i in 0..e.clusters.len() {
        let mut r = e.root(i).await;
        let schemas: Vec<String> = r
            .exec(
                "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME = \
                 '__sprig_lk_customers__uk_email' AND TABLE_SCHEMA LIKE ?",
                (format!("t{}\\_d%", e.tenant_id),),
            )
            .await
            .unwrap();
        for s in schemas {
            let rows: Vec<(String, i64)> = r
                .query(format!(
                    "SELECT email, owner_id FROM `{s}`.`__sprig_lk_customers__uk_email`"
                ))
                .await
                .unwrap();
            for (em, id) in rows {
                out.entry(em.to_lowercase()).or_default().push(id);
            }
        }
        r.disconnect().await.unwrap();
    }
    out
}

/// Every row's (email, id) through the gateway; the uniqueness check.
async fn owners(c: &mut Conn) -> BTreeMap<String, i64> {
    let rows: Vec<(Option<String>, i64)> =
        c.query("SELECT email, id FROM customers").await.unwrap();
    let mut out = BTreeMap::new();
    for (em, id) in rows {
        let Some(em) = em else { continue };
        let prev = out.insert(em.to_lowercase(), id);
        assert!(prev.is_none(), "email {em} held by {id} and {prev:?}");
    }
    out
}

/// Every owner has its lookup row; returns orphans (lookup rows whose
/// owner doesn't hold the value).
async fn check_consistent(e: &Env, c: &mut Conn, label: &str) -> usize {
    let own = owners(c).await;
    let lk = lookup_rows(e).await;
    for (em, id) in &own {
        let held = lk.get(em).cloned().unwrap_or_default();
        assert!(
            held.contains(id),
            "{label}: row {id} ({em}) has no lookup row (lookup holds {held:?})"
        );
    }
    let orphans = lk
        .iter()
        .filter(|(em, ids)| ids.iter().any(|i| own.get(*em) != Some(i)))
        .count();
    println!(
        "{label}: {} rows, {} lookup rows, {orphans} orphans",
        own.len(),
        lk.values().map(Vec::len).sum::<usize>()
    );
    orphans
}

async fn state_of(c: &mut Conn) -> Option<String> {
    let rows: Vec<Row> = c
        .query("SPRIG SHOW LOOKUP INDEXES FROM customers")
        .await
        .unwrap_or_default();
    rows.first().and_then(|r| r.get::<String, _>(6))
}

/// Writers changing `customers` until `stop`: inserts of new ids, email
/// changes and deletes. Returns (statements, errors).
fn writers(
    pool: &Pool,
    stop: &Arc<AtomicBool>,
    next: &Arc<AtomicI64>,
    n: usize,
) -> Vec<tokio::task::JoinHandle<(u64, u64)>> {
    (0..n)
        .map(|w| {
            let pool = pool.clone();
            let stop = Arc::clone(stop);
            let next = Arc::clone(next);
            tokio::spawn(async move {
                let (mut ok, mut err) = (0u64, 0u64);
                let mut k = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    k += 1;
                    let Ok(mut c) = pool.get_conn().await else {
                        err += 1;
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    };
                    let id = next.fetch_add(1, Ordering::Relaxed);
                    let r = match k % 3 {
                        0 => {
                            c.exec_drop(
                                "INSERT INTO customers (id, email, name) VALUES (?, ?, 'w')",
                                (id, format!("person{id}@x.org")),
                            )
                            .await
                        }
                        1 => {
                            let victim = 1 + (id * 7919 + w as i64) % 20_000;
                            c.exec_drop(
                                "UPDATE customers SET email = ? WHERE id = ?",
                                (format!("moved{id}@x.org"), victim),
                            )
                            .await
                        }
                        _ => {
                            let victim = 1 + (id * 104_729 + w as i64) % 20_000;
                            c.exec_drop("DELETE FROM customers WHERE id = ?", (victim,))
                                .await
                        }
                    };
                    if r.is_ok() {
                        ok += 1;
                    } else {
                        err += 1;
                    }
                }
                (ok, err)
            })
        })
        .collect()
}

async fn stop_writers(
    stop: &AtomicBool,
    ws: Vec<tokio::task::JoinHandle<(u64, u64)>>,
) -> (u64, u64) {
    stop.store(true, Ordering::Relaxed);
    let mut t = (0, 0);
    for w in ws {
        let (a, b) = w.await.unwrap();
        t.0 += a;
        t.1 += b;
    }
    t
}

/// The ids still holding their seeded email.
async fn seeded_ids(c: &mut Conn, n: usize) -> Vec<i64> {
    c.query(format!(
        "SELECT id FROM customers WHERE email LIKE 'person%' AND id <= 20000 ORDER BY id LIMIT {n}"
    ))
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn lookup_ddl_live() {
    let Some(e) = Env::load() else {
        eprintln!(
            "skipped: set SPRIG_TEST_LKD_GATEWAY, _METRICS, _TENANT_ID, _CLUSTERS, _SPRIGCTL, \
             _GW_PIDFILE and _GW_RESTART (module docs)"
        );
        return;
    };
    let t0 = Instant::now();
    // ---- 1. a table in 4 shards ------------------------------------------
    {
        let (h, p) = e.gateway.split_once(':').unwrap();
        let mut c = Conn::new(
            OptsBuilder::default()
                .ip_or_hostname(h)
                .tcp_port(p.parse().unwrap())
                .user(Some(format!("{}.app", e.tenant)))
                .pass(Some("sprig-dev-app")),
        )
        .await
        .unwrap();
        c.query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
            .await
            .unwrap();
        c.query_drop(format!("CREATE DATABASE {DB}")).await.unwrap();
        c.disconnect().await.unwrap();
    }
    let pool = e.app();
    let mut c = pool.get_conn().await.unwrap();
    c.query_drop(
        "CREATE TABLE customers (id BIGINT PRIMARY KEY, email VARCHAR(100) COLLATE \
         utf8mb4_uca1400_ai_ci, name VARCHAR(40))",
    )
    .await
    .unwrap();
    for b in 0..20 {
        let vals: Vec<String> = (b * 1000 + 1..=b * 1000 + 1000)
            .map(|i| format!("({i}, 'person{i}@x.org', 'n{i}')"))
            .collect();
        c.query_drop(format!(
            "INSERT INTO customers (id, email, name) VALUES {}",
            vals.join(", ")
        ))
        .await
        .unwrap();
    }
    c.query_drop("SPRIG PIN customers SHARD KEY (id)")
        .await
        .unwrap();
    e.sprigctl(&["advisor", "run", &e.tenant, "-o", "json", "--yes"]);
    let mut cat = e.root(0).await;
    let run: Option<u64> = cat
        .query_first(format!(
            "SELECT MAX(run_id) FROM t{}_sprig.advisor_runs",
            e.tenant_id
        ))
        .await
        .unwrap();
    e.sprigctl(&[
        "advisor",
        "apply",
        &e.tenant,
        &run.unwrap().to_string(),
        "-o",
        "json",
        "--yes",
    ]);
    let group_of = |cat: &mut Conn| {
        let q = format!(
            "SELECT m.group_id FROM t{0}_sprig.group_members m JOIN t{0}_sprig.objects o \
             ON o.object_id = m.object_id WHERE o.name = 'customers' AND o.state = 'active'",
            e.tenant_id
        );
        let cat: &mut Conn = cat;
        async move { cat.query_first::<u32, _>(q).await.unwrap() }
    };
    let started = Instant::now();
    let group = loop {
        if let Some(g) = group_of(&mut cat).await {
            break g;
        }
        assert!(
            started.elapsed() < Duration::from_secs(300),
            "never grouped"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    let shard_count = |n: usize| {
        let e = &e;
        async move {
            let started = Instant::now();
            loop {
                let out = e.sprigctl(&["shard", "list", &e.tenant, "-o", "json"]);
                let v: serde_json::Value = serde_json::from_str(&out).unwrap_or_default();
                let shards: Vec<u64> = v
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|g| g["group"].as_u64() == Some(u64::from(group)))
                    .flat_map(|g| g["shards"].as_array().cloned().unwrap_or_default())
                    .filter_map(|s| s["shard"].as_u64())
                    .collect();
                if shards.len() >= n {
                    return shards;
                }
                assert!(
                    started.elapsed() < Duration::from_secs(600),
                    "group {group} never reached {n} shards: {out}"
                );
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    };
    let first = shard_count(1).await;
    e.sprigctl(&[
        "shard",
        "split",
        &e.tenant,
        &group.to_string(),
        &first[0].to_string(),
        "--ways",
        "4",
        "--yes",
    ]);
    let four = shard_count(4).await;
    // The router has the 4-shard map once a scatter fans out to 4.
    let ids = seeded_ids(&mut c, 50).await;
    let started = Instant::now();
    let before = loop {
        let f = shards_per_read(&e, &mut c, &ids).await;
        if f >= 3.99 {
            break f;
        }
        assert!(started.elapsed() < Duration::from_secs(120), "fan-out {f}");
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    println!(
        "[{:?}] group {group}, shards {four:?}; a point read by email visits {before:.2} shards",
        t0.elapsed()
    );

    // ---- 2. ADD UNIQUE under writes --------------------------------------
    let stop = Arc::new(AtomicBool::new(false));
    let next = Arc::new(AtomicI64::new(100_001));
    let ws = writers(&pool, &stop, &next, 4);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let ddl_t = Instant::now();
    c.query_drop("ALTER TABLE customers ADD UNIQUE KEY uk_email (email)")
        .await
        .unwrap();
    let ddl_s = ddl_t.elapsed();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (ok, err) = stop_writers(&stop, ws).await;
    println!(
        "[{:?}] ADD UNIQUE took {ddl_s:?} under {ok} writes ({err} failed)",
        t0.elapsed()
    );
    assert_eq!(state_of(&mut c).await.as_deref(), Some("public"));
    let view: Vec<(String, String, String)> = c
        .query(
            "SELECT TABLE_NAME, INDEX_NAME, STATE FROM information_schema.SPRIG_LOOKUP_INDEXES \
             WHERE TABLE_SCHEMA = 'lookupddl_db'",
        )
        .await
        .unwrap();
    assert_eq!(
        view,
        vec![("customers".into(), "uk_email".into(), "public".into())]
    );
    let shown: Vec<Row> = c.query("SHOW INDEX FROM customers").await.unwrap();
    assert!(
        shown
            .iter()
            .any(|r| r.get::<String, _>(2).as_deref() == Some("uk_email")),
        "SHOW INDEX lists uk_email"
    );
    check_consistent(&e, &mut c, "after ADD UNIQUE under writes").await;
    let ids = seeded_ids(&mut c, 200).await;
    let after = shards_per_read(&e, &mut c, &ids).await;
    println!("a point read by email now visits {after:.2} shards (was {before:.2})");
    assert!(after <= 1.01, "{after}");

    // ---- 3. uniqueness across shards -------------------------------------
    let a = ids[0];
    let b = *ids
        .iter()
        .find(|i| **i % 4 != a % 4 && **i != a)
        .unwrap_or(&ids[1]);
    let dup = c
        .exec_drop(
            "INSERT INTO customers (id, email, name) VALUES (?, ?, 'dup')",
            (900_001, format!("PERSON{a}@X.ORG")),
        )
        .await;
    assert_eq!(errno_of(dup).await, Some(1062), "a duplicate insert");
    let dup = c
        .exec_drop(
            "UPDATE customers SET email = ? WHERE id = ?",
            (format!("person{a}@x.org"), b),
        )
        .await;
    assert_eq!(
        errno_of(dup).await,
        Some(1062),
        "an update to a taken email"
    );
    c.exec_drop(
        "UPDATE customers SET email = ? WHERE id = ?",
        ("renamed@x.org", b),
    )
    .await
    .unwrap();
    let r: Option<i64> = c
        .query_first("SELECT id FROM customers WHERE email = 'RENAMED@x.org'")
        .await
        .unwrap();
    assert_eq!(r, Some(b));
    let r: Option<i64> = c
        .exec_first(
            "SELECT id FROM customers WHERE email = ?",
            (format!("person{b}@x.org"),),
        )
        .await
        .unwrap();
    assert_eq!(r, None, "the old email finds nothing");
    // The freed email can be taken by another row now.
    c.exec_drop(
        "INSERT INTO customers (id, email, name) VALUES (?, ?, 'reuse')",
        (900_002, format!("person{b}@x.org")),
    )
    .await
    .unwrap();
    c.exec_drop("DELETE FROM customers WHERE id = ?", (900_002,))
        .await
        .unwrap();
    c.exec_drop(
        "INSERT INTO customers (id, email, name) VALUES (?, ?, 'again')",
        (900_003, format!("person{b}@x.org")),
    )
    .await
    .unwrap();
    // Multi-row: one statement with two equal emails.
    let dup = c
        .query_drop(
            "INSERT INTO customers (id, email) VALUES (900004, 'same@x.org'), (900005, 'SAME@x.org')",
        )
        .await;
    assert_eq!(
        errno_of(dup).await,
        Some(1062),
        "two equal emails in one insert"
    );
    check_consistent(&e, &mut c, "after the uniqueness checks").await;
    println!("[{:?}] uniqueness across shards holds", t0.elapsed());

    // ---- 4. a split while point reads run --------------------------------
    let stop = Arc::new(AtomicBool::new(false));
    let reads = AtomicU64::new(0);
    let reader = {
        let pool = pool.clone();
        let stop = Arc::clone(&stop);
        let ids = ids.clone();
        tokio::spawn(async move {
            let mut c = pool.get_conn().await.unwrap();
            let mut n = 0u64;
            let mut bad = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                for id in ids.iter().skip(1).take(20) {
                    let r: mysql_async::Result<Option<i64>> = c
                        .exec_first(
                            "SELECT id FROM customers WHERE email = ?",
                            (format!("person{id}@x.org"),),
                        )
                        .await;
                    match r {
                        Ok(Some(x)) if x == *id => n += 1,
                        Ok(other) => bad.push((id, format!("{other:?}"))),
                        Err(_) => {} // a held range's wait, retried next round
                    }
                }
            }
            (n, bad)
        })
    };
    let shard_to_split = four[1];
    e.sprigctl(&[
        "shard",
        "split",
        &e.tenant,
        &group.to_string(),
        &shard_to_split.to_string(),
        "--yes",
    ]);
    let five = shard_count(5).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    stop.store(true, Ordering::Relaxed);
    let (n, bad) = reader.await.unwrap();
    reads.fetch_add(n, Ordering::Relaxed);
    assert!(
        bad.is_empty(),
        "wrong point reads during the split: {bad:?}"
    );
    let f = shards_per_read(&e, &mut c, &ids[1..100]).await;
    assert!(f <= 1.01, "after the split: {f}");
    check_consistent(&e, &mut c, "after the split").await;
    println!(
        "[{:?}] split shard {shard_to_split}: shards {five:?}; {n} point reads right during it, {f:.2} shards per read after",
        t0.elapsed()
    );

    // ---- 5. kill -9 during the back-fill ---------------------------------
    c.query_drop("ALTER TABLE customers DROP INDEX uk_email")
        .await
        .unwrap();
    assert_eq!(state_of(&mut c).await, None, "the lookup is gone");
    for b in 0..40 {
        let vals: Vec<String> = (200_001 + b * 1000..200_001 + b * 1000 + 1000)
            .map(|i| format!("({i}, 'bulk{i}@x.org', 'b')"))
            .collect();
        c.query_drop(format!(
            "INSERT INTO customers (id, email, name) VALUES {}",
            vals.join(", ")
        ))
        .await
        .unwrap();
    }
    let pid: i32 = std::fs::read_to_string(&e.pidfile)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let ddl = {
        let pool = pool.clone();
        tokio::spawn(async move {
            let mut c = pool.get_conn().await.unwrap();
            c.query_drop("ALTER TABLE customers ADD UNIQUE KEY uk_email (email)")
                .await
        })
    };
    // Kill as soon as the back-fill runs.
    let mut cat = e.root(0).await;
    let started = Instant::now();
    loop {
        let st: Option<String> = cat
            .query_first(format!(
                "SELECT state FROM t{}_sprig.lookup_indexes WHERE index_name = 'uk_email'",
                e.tenant_id
            ))
            .await
            .unwrap();
        if st.as_deref() == Some("backfilling") {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(120),
            "no back-fill: {st:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let killed_at = Instant::now();
    let st = std::process::Command::new("kill")
        .args(["-9", &pid.to_string()])
        .status()
        .unwrap();
    assert!(st.success());
    assert!(ddl.await.unwrap().is_err(), "the client lost its gateway");
    println!(
        "[{:?}] killed the gateway (pid {pid}) during the back-fill",
        t0.elapsed()
    );
    let st = std::process::Command::new("sh")
        .args(["-c", &e.restart])
        .status()
        .unwrap();
    assert!(st.success(), "restart");
    drop(pool);
    let pool = e.app();
    let started = Instant::now();
    let mut c = loop {
        if let Ok(c) = pool.get_conn().await {
            break c;
        }
        assert!(started.elapsed() < Duration::from_secs(120));
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    // Writes go on while the controller takes the build over.
    let stop = Arc::new(AtomicBool::new(false));
    let next = Arc::new(AtomicI64::new(300_001));
    let ws = writers(&pool, &stop, &next, 2);
    let started = Instant::now();
    loop {
        let st = state_of(&mut c).await;
        if st.as_deref() == Some("public") {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(300),
            "the stopped build was never taken over: {st:?}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let resumed = killed_at.elapsed();
    let (ok, err) = stop_writers(&stop, ws).await;
    println!(
        "[{:?}] the controller finished the build {resumed:?} after the kill ({ok} writes meanwhile, {err} failed)",
        t0.elapsed()
    );
    check_consistent(&e, &mut c, "after kill -9 and the take-over").await;
    let ids2: Vec<i64> = c
        .query("SELECT id FROM customers WHERE email LIKE 'bulk%' ORDER BY id LIMIT 100")
        .await
        .unwrap();
    for id in &ids2 {
        let r: Option<i64> = c
            .exec_first(
                "SELECT id FROM customers WHERE email = ?",
                (format!("bulk{id}@x.org"),),
            )
            .await
            .unwrap();
        assert_eq!(r, Some(*id));
    }
    // A rebuild removes the orphans the kill and the failed writes left.
    c.query_drop("SPRIG REBUILD LOOKUP INDEX uk_email ON customers")
        .await
        .unwrap();
    let orphans = check_consistent(&e, &mut c, "after SPRIG REBUILD").await;
    assert_eq!(orphans, 0);
    let names: BTreeSet<String> = lookup_rows(&e).await.into_keys().collect();
    assert_eq!(names.len(), owners(&mut c).await.len());
    println!(
        "[{:?}] done; {} reads during the split",
        t0.elapsed(),
        reads.load(Ordering::Relaxed)
    );
    c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
}
