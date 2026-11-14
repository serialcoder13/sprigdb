//! Live: the balancer evens out a deliberately skewed fleet without
//! oscillating.
//!
//! Needs the test bed of `deploy/compose/balancer/run.sh up`: a deployment
//! of its own over the three Compose clusters, ten tenants `bl-01` …
//! `bl-10`, eight of them on `dc-bl-c1`, and a controller (balancer in
//! `recommend`), a worker and a gateway (18306 and 18307). Run with
//! `SPRIG_TEST_BALANCER_LIVE=1` and `SPRIGCTL` naming the sprigctl binary.
//!
//! 1. Each tenant gets a table of 15,000 to 60,000 rows (about 4 to 16 MB),
//!    and a writer that upserts about four rows a second through the
//!    gateway's MariaDB persona (18307) for the whole
//!    run (each write retried with the same values until it succeeds, so
//!    the expected contents are exact).
//! 2. `recommend` for a minute: decisions are written, no job is created.
//! 3. `auto`, with short cooldowns: the balancer moves tenants until the
//!    fleet is even; then five more minutes of planning runs must move
//!    nothing, and no tenant may return to a cluster it left.
//! 4. Every tenant's rows, read through the gateway and on the cluster
//!    that holds it, equal what the writer wrote.
//!
//! With `SPRIG_TEST_BALANCER_PIN=<tenant>`, that tenant is pinned
//! (`sprigctl tenant pin`) before `auto`: it must not move, a decision must
//! say it was kept by its pin, and at the end `sprigctl shard move` moves
//! it to the emptiest cluster (an operator decision).
//!
//! It prints the fleet before and after, every move with its decision, the
//! rejected moves' reasons and `explain placement` / `explain decision`.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::print_stdout,
    clippy::many_single_char_names
)]

use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{OptsBuilder, Pool, PoolConstraints, PoolOpts};
use serde_json::Value;
use tokio::sync::Mutex;

const PW: &str = "sprig-dev-app";
const DC: &str = "dc-bl";

fn tenants() -> Vec<String> {
    (1..=10).map(|i| format!("bl-{i:02}")).collect()
}

fn ctl(args: &[&str]) -> String {
    let bin = std::env::var("SPRIGCTL").expect("SPRIGCTL names the sprigctl binary");
    let out = Command::new(bin)
        .args(["--endpoint", "http://127.0.0.1:18480"])
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "sprigctl {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

fn ctl_json(args: &[&str]) -> Value {
    let mut a = vec!["-o", "json"];
    a.extend_from_slice(args);
    serde_json::from_str(&ctl(&a)).unwrap()
}

fn set(key: &str, value: &str) {
    ctl(&[
        "settings",
        "set",
        key,
        value,
        "--scope",
        &format!("dc:{DC}"),
        "--reason",
        "live_balance",
    ]);
}

fn pool(port: u16, user: &str, pass: &str) -> Pool {
    let opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some(user))
        .pass(Some(pass))
        .prefer_socket(false)
        .ssl_opts(Some(
            mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true),
        ))
        .pool_opts(
            PoolOpts::default()
                .with_constraints(PoolConstraints::new(0, 2).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(120)),
        );
    Pool::new(opts)
}

/// The cluster's writer port as the root account sees it.
fn writer_port(cluster: &str) -> u16 {
    match cluster {
        "dc-bl-c1" => 13301,
        "dc-bl-c2" => 13311,
        "dc-bl-c3" => 13321,
        o => panic!("unknown cluster {o}"),
    }
}

/// `cluster -> full` and each tenant's cluster, from `explain placement`.
fn fleet() -> (BTreeMap<String, f64>, BTreeMap<String, String>, Value) {
    let mut home = BTreeMap::new();
    let mut full = BTreeMap::new();
    let mut first = Value::Null;
    for t in tenants() {
        let e = ctl_json(&["explain", "placement", &t]);
        for c in e["clusters"].as_array().unwrap() {
            full.insert(
                c["cluster"].as_str().unwrap().to_owned(),
                c["full"].as_f64().unwrap(),
            );
        }
        home.insert(t, e["shards"][0]["cluster"].as_str().unwrap().to_owned());
        if first.is_null() {
            first = e;
        }
    }
    (full, home, first)
}

fn print_fleet(label: &str, full: &BTreeMap<String, f64>, home: &BTreeMap<String, String>) {
    println!("\n== fleet {label}");
    for (c, f) in full {
        let on: Vec<&str> = home
            .iter()
            .filter(|(_, h)| *h == c)
            .map(|(t, _)| t.as_str())
            .collect();
        println!(
            "  {c}: full {:.4} ({:.2} points), {} tenants {:?}",
            f,
            f * 100.0,
            on.len(),
            on
        );
    }
    let max = full.values().copied().fold(f64::MIN, f64::max);
    let min = full.values().copied().fold(f64::MAX, f64::min);
    println!("  spread {:.2} points", (max - min) * 100.0);
}

fn jobs() -> Vec<Value> {
    ctl_json(&["job", "list"])["jobs"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

/// One tenant's writer and what it wrote.
struct Writer {
    tenant: String,
    expected: Mutex<BTreeMap<u64, (u64, String)>>,
    writes: AtomicU64,
    retries: AtomicU64,
    max_ms: AtomicU64,
}

async fn write_loop(w: Arc<Writer>, pool: Pool, stop: Arc<AtomicBool>, seed: u64) {
    let mut next_id = seed + 1;
    let mut n: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        n += 1;
        let (id, v) = if n % 5 < 3 {
            next_id += 1;
            (next_id - 1, 1)
        } else {
            let g = w.expected.lock().await;
            let id = 1 + (n * 7919) % (next_id - 1);
            (id, g.get(&id).map_or(1, |(v, _)| v + 1))
        };
        let pad = format!("{}-{v}-{}", w.tenant, "x".repeat(120));
        let started = Instant::now();
        loop {
            let r = async {
                let mut c = pool.get_conn().await?;
                c.exec_drop(
                    "INSERT INTO bl_app.items (id, v, pad) VALUES (?, ?, ?) \
                     ON DUPLICATE KEY UPDATE v = VALUES(v), pad = VALUES(pad)",
                    (id, v, pad.as_str()),
                )
                .await
            }
            .await;
            match r {
                Ok(()) => break,
                Err(e) => {
                    w.retries.fetch_add(1, Ordering::Relaxed);
                    if w.retries.load(Ordering::Relaxed) % 20 == 1 {
                        println!("  [{}] write retried: {e}", w.tenant);
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
        let ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        w.max_ms.fetch_max(ms, Ordering::Relaxed);
        w.expected.lock().await.insert(id, (v, pad));
        w.writes.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Count and a digest of every row, as the database returns them.
async fn digest(pool: &Pool, schema: &str) -> (u64, String, BTreeMap<u64, (u64, String)>) {
    let mut c = pool.get_conn().await.unwrap();
    let rows: Vec<(u64, u64, String)> = c
        .query(format!("SELECT id, v, pad FROM {schema}.items ORDER BY id"))
        .await
        .unwrap();
    let sum: Option<(u64, String)> = c
        .query_first(format!(
            "SELECT COUNT(*), CAST(SUM(CRC32(CONCAT_WS(':', id, v, pad))) AS CHAR) \
             FROM {schema}.items"
        ))
        .await
        .unwrap();
    let (n, s) = sum.unwrap();
    (
        n,
        s,
        rows.into_iter().map(|(i, v, p)| (i, (v, p))).collect(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn skewed_fleet_evens_out_without_oscillating() {
    if std::env::var("SPRIG_TEST_BALANCER_LIVE").is_err() {
        eprintln!(
            "skipped: run deploy/compose/balancer/run.sh up, then set SPRIG_TEST_BALANCER_LIVE=1"
        );
        return;
    }
    let ids: BTreeMap<String, u64> = tenants()
        .into_iter()
        .map(|t| {
            let id = ctl_json(&["tenant", "show", &t])["tenant_id"]
                .as_u64()
                .unwrap();
            (t, id)
        })
        .collect();

    // 1. Tables and writers.
    let stop = Arc::new(AtomicBool::new(false));
    let mut writers = Vec::new();
    let mut pools = BTreeMap::new();
    for (i, t) in tenants().iter().enumerate() {
        let p = pool(18307, &format!("{t}.app"), PW);
        // A cluster restarted by another test may take a few seconds to be
        // usable again.
        let mut c = {
            let mut tries = 0;
            loop {
                match p.get_conn().await {
                    Ok(c) => break c,
                    Err(e) if tries < 30 => {
                        tries += 1;
                        println!("  [{t}] waiting for the gateway: {e}");
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                    Err(e) => panic!("{t}: {e}"),
                }
            }
        };
        c.query_drop("CREATE DATABASE IF NOT EXISTS bl_app")
            .await
            .unwrap();
        c.query_drop(
            "CREATE TABLE IF NOT EXISTS bl_app.items (id BIGINT UNSIGNED PRIMARY KEY, \
             v BIGINT UNSIGNED NOT NULL, pad VARCHAR(255) NOT NULL) ENGINE=InnoDB",
        )
        .await
        .unwrap();
        c.query_drop("DELETE FROM bl_app.items").await.unwrap();
        let seed = 15_000 + 5_000 * i as u64;
        let w = Arc::new(Writer {
            tenant: t.clone(),
            expected: Mutex::new(BTreeMap::new()),
            writes: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            max_ms: AtomicU64::new(0),
        });
        {
            let mut g = w.expected.lock().await;
            for chunk in (1..=seed).collect::<Vec<_>>().chunks(500) {
                let mut sql = String::from("INSERT INTO bl_app.items (id, v, pad) VALUES ");
                let vals: Vec<String> = chunk
                    .iter()
                    .map(|id| {
                        let pad = format!("{t}-1-{}", "x".repeat(120));
                        g.insert(*id, (1, pad.clone()));
                        format!("({id}, 1, '{pad}')")
                    })
                    .collect();
                sql.push_str(&vals.join(","));
                c.query_drop(sql).await.unwrap();
            }
        }
        let _ = c.query_drop("ANALYZE TABLE bl_app.items").await;
        drop(c);
        tokio::spawn(write_loop(
            Arc::clone(&w),
            p.clone(),
            Arc::clone(&stop),
            seed,
        ));
        writers.push(w);
        pools.insert(t.clone(), p);
    }
    // Persistent statistics on the clusters (the balancer's storage scan).
    for port in [13301u16, 13311, 13321] {
        let root = pool(port, "root", "sprig-dev-root");
        let mut c = root.get_conn().await.unwrap();
        for id in ids.values() {
            let tables: Vec<(String, String)> = c
                .exec(
                    "SELECT database_name, table_name FROM mysql.innodb_table_stats \
                     WHERE database_name LIKE ?",
                    (format!("t{id}\\_%"),),
                )
                .await
                .unwrap();
            for (d, t) in tables {
                let _ = c.query_drop(format!("ANALYZE TABLE `{d}`.`{t}`")).await;
            }
        }
        drop(c);
        root.disconnect().await.unwrap();
    }

    // Short cooldowns and a planning run every 10 s.
    for (k, v) in [
        ("balancer.interval_seconds", "10"),
        ("balancer.sustain_minutes", "0"),
        ("balancer.spread_pct", "1"),
        ("balancer.cluster_cooldown_minutes", "1"),
        ("balancer.shard_cooldown_hours", "0"),
        ("balancer.window_minutes", "5"),
    ] {
        set(k, v);
    }

    // 2. Recommend only.
    set("balancer.mode", "recommend");
    tokio::time::sleep(Duration::from_secs(60)).await;
    let (full0, home0, e0) = fleet();
    print_fleet("before (balancer.mode recommend)", &full0, &home0);
    println!(
        "\n== explain placement bl-01 (recommend)\n{}",
        ctl(&["explain", "placement", "bl-01"])
    );
    let _ = e0;
    assert!(jobs().is_empty(), "recommend created jobs: {:?}", jobs());

    // An operator pin on the crowded cluster: that tenant must stay put.
    let pinned = std::env::var("SPRIG_TEST_BALANCER_PIN").ok();
    if let Some(p) = &pinned {
        println!(
            "\n== sprigctl tenant pin {p}\n{}",
            ctl(&[
                "tenant",
                "pin",
                p,
                "--reason",
                "live_balance: stays on the crowded cluster"
            ])
        );
    }

    // 3. Auto.
    set("balancer.mode", "auto");
    let started = Instant::now();
    let mut last_change = Instant::now();
    let mut seen: BTreeMap<String, String> = BTreeMap::new();
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        let js = jobs();
        for j in &js {
            let id = j["id"].as_str().unwrap_or_default().to_owned();
            let st = j["state"].as_str().unwrap_or_default().to_owned();
            if seen.get(&id) != Some(&st) {
                println!(
                    "  [{:>4}s] job {} tenant {} {} -> {}: {}",
                    started.elapsed().as_secs(),
                    &id[..8.min(id.len())],
                    j["tenant"],
                    j["source"].as_str().unwrap_or("?"),
                    j["target"].as_str().unwrap_or("?"),
                    st
                );
                seen.insert(id, st);
                last_change = Instant::now();
            }
        }
        let active = js.iter().any(|j| {
            !matches!(
                j["state"].as_str(),
                Some("done" | "rolled_back" | "retaining" | "cleaning" | "post_cutover")
            )
        });
        if !active && last_change.elapsed() > Duration::from_secs(120) {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(1500),
            "the fleet didn't settle in 25 minutes"
        );
    }
    let settled = jobs().len();
    if let Some(p) = &pinned {
        let pid = ids[p];
        assert!(
            jobs().iter().all(|j| j["tenant"].as_u64() != Some(pid)),
            "the pinned tenant {p} moved"
        );
    }
    let (full1, home1, _) = fleet();
    print_fleet(
        &format!("after {} s of auto", started.elapsed().as_secs()),
        &full1,
        &home1,
    );
    println!("\n== five more minutes of planning runs (30 runs)");
    tokio::time::sleep(Duration::from_secs(300)).await;
    let js = jobs();
    let (full2, home2, _) = fleet();
    print_fleet("five minutes later", &full2, &home2);
    assert_eq!(js.len(), settled, "moves after the fleet settled");

    // Every move, and no move back.
    let mut by_tenant: BTreeMap<u64, Vec<&Value>> = BTreeMap::new();
    for j in &js {
        by_tenant
            .entry(j["tenant"].as_u64().unwrap())
            .or_default()
            .push(j);
    }
    let name_of: BTreeMap<u64, String> = ids.iter().map(|(t, i)| (*i, t.clone())).collect();
    println!("\n== moves ({})", js.len());
    for (tid, list) in &mut by_tenant {
        list.sort_by_key(|j| j["created_ms"].as_u64());
        let mut left: BTreeSet<String> = BTreeSet::new();
        for j in list.iter() {
            let from = j["source"].as_str().unwrap().to_owned();
            let to = j["target"].as_str().unwrap().to_owned();
            assert!(!left.contains(&to), "tenant {tid} moved back to {to}");
            left.insert(from.clone());
            let t = &name_of[tid];
            let d = j["decision_id"].as_u64().unwrap();
            let row = ctl_json(&["explain", "decision", &d.to_string(), "--tenant", t]);
            println!(
                "- {t} {from} -> {to}, job {}, decision {d} ({} / {}): {}",
                j["state"].as_str().unwrap(),
                row["mode"].as_str().unwrap_or("?"),
                row["outcome"].as_str().unwrap_or("?"),
                row["summary"].as_str().unwrap_or("?")
            );
        }
    }
    let spread = |f: &BTreeMap<String, f64>| {
        f.values().copied().fold(f64::MIN, f64::max) - f.values().copied().fold(f64::MAX, f64::min)
    };
    assert!(
        spread(&full2) < spread(&full0) / 2.0,
        "spread {} -> {}",
        spread(&full0),
        spread(&full2)
    );

    // An operator move (of the pinned tenant: only operators move pinned
    // tenants) is a decision too.
    let mut home3 = home2.clone();
    if let Some(p) = &pinned {
        let to = full2
            .iter()
            .filter(|(c, _)| **c != home2[p])
            .min_by(|a, b| a.1.total_cmp(b.1))
            .map(|(c, _)| c.clone())
            .unwrap();
        println!(
            "\n== sprigctl shard move {p} --to {to} --wait\n{}",
            ctl(&["shard", "move", p, "--to", &to, "--wait"])
        );
        let pid = ids[p];
        let job = jobs()
            .into_iter()
            .find(|j| j["tenant"].as_u64() == Some(pid))
            .unwrap();
        println!("  job: {job}");
        home3 = fleet().1;
        assert_eq!(home3[p], to, "{p} not on {to} after the operator move");
    }

    // 4. Stop writing and compare.
    stop.store(true, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(2)).await;
    println!("\n== data (writer: writes, retries, slowest write)");
    for w in &writers {
        let t = &w.tenant;
        let id = ids[t];
        let expected = w.expected.lock().await.clone();
        let (n, sum, rows) = digest(&pools[t], "bl_app").await;
        assert_eq!(rows, expected, "{t}: rows through the gateway differ");
        let home = &home3[t];
        let root = pool(writer_port(home), "root", "sprig-dev-root");
        // bl_app's shard schema, t<id>_d<db>_g<g>_s<s>.
        let schema: String = root
            .get_conn()
            .await
            .unwrap()
            .exec_first(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE ?",
                (format!("t{id}\\_d%"),),
            )
            .await
            .unwrap()
            .unwrap();
        let (bn, bsum, _) = digest(&root, &format!("`{schema}`")).await;
        root.disconnect().await.unwrap();
        assert_eq!((bn, &bsum), (n, &sum), "{t}: backend and gateway differ");
        println!(
            "  {t} on {home} ({schema}): {n} rows, crc32 sum {sum} (gateway = backend = expected); \
             {} writes, {} retries, slowest {} ms",
            w.writes.load(Ordering::Relaxed),
            w.retries.load(Ordering::Relaxed),
            w.max_ms.load(Ordering::Relaxed)
        );
    }

    // Rejected moves and explain output.
    println!("\n== decisions other than the moves");
    let mut pin_reason = false;
    for (t, id) in &ids {
        let home = &home3[t];
        let root = pool(writer_port(home), "root", "sprig-dev-root");
        let mut c = root.get_conn().await.unwrap();
        let rows: Vec<(u64, String, String, String, String)> = c
            .query(format!(
                "SELECT decision_id, mode, outcome, reason_code, summary FROM t{id}_sprig.decisions \
                 WHERE kind LIKE 'shard.%' ORDER BY decision_id"
            ))
            .await
            .unwrap();
        for (d, mode, outcome, reason, summary) in rows {
            if outcome == "succeeded" {
                continue;
            }
            if pinned.as_deref() == Some(t.as_str()) && summary.contains("pinned by operator") {
                pin_reason = true;
            }
            println!("- {t} #{d} {mode}/{outcome} {reason}: {summary}");
        }
        drop(c);
        root.disconnect().await.unwrap();
    }
    assert!(
        pinned.is_none() || pin_reason,
        "no decision says the pinned tenant was kept by its pin"
    );
    if let Some((t, list)) = by_tenant.iter().next() {
        let d = list[0]["decision_id"].as_u64().unwrap().to_string();
        println!(
            "\n== sprigctl explain decision {d} --tenant {}\n{}",
            name_of[t],
            ctl(&["explain", "decision", &d, "--tenant", &name_of[t]])
        );
    }
    println!(
        "\n== sprigctl explain placement bl-01\n{}",
        ctl(&["explain", "placement", "bl-01"])
    );
    for p in pools.into_values() {
        p.disconnect().await.unwrap();
    }
}
