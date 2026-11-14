//! A tenant moves between two simulated data centers under load.
//!
//! ```sh
//! cargo build -p sprigdb -p sprigctl
//! deploy/compose/dc/run.sh up      # mum1 and fra1 with netem, processes, tenant dc-move
//! SPRIG_TEST_DC_MOVE=1 cargo test -p sprig-controller --test live_dc_move -- --nocapture
//! ```
//!
//! Writers in both DCs (connection attribute `sprig_dc`) run logged
//! transactions through their own DC's gateway: the tenant's home is the
//! other DC for half of them (remote homes). `sprigctl tenant move dc-move
//! --to <the other DC>` moves the tenant; run the test again to move it
//! back. (The cluster it left keeps the fenced source for
//! `mover.source_retain_hours`, so a move back picks another cluster:
//! the bed has two in mum1.) Probe
//! sessions opened before each move check what a move promises:
//! an idle session with a variable, a default database and a prepared
//! statement follows transparently; one holding a temporary table gets
//! 39502 (or 39033) and must reconnect. Afterwards every acknowledged
//! write is on the new home exactly once, a filler table's checksum is
//! unchanged, every write error was a clean retryable one (1213, 1205),
//! and each move's write pause (the mover's hold) is within the 5 s budget.
//! The test prints the pause, the time from start to commit, the longest
//! gap in acknowledged writes, and statement round trips to a local and a
//! remote home.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::many_single_char_names
)]

use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{OptsBuilder, Pool, PoolConstraints, PoolOpts};
use sprig_common::CancellationToken;

const TENANT: &str = "dc-move";
const BUDGET_MS: u64 = 5000;

fn enabled() -> bool {
    std::env::var("SPRIG_TEST_DC_MOVE").is_ok_and(|v| v == "1")
}

/// MariaDB persona of each DC's gateway.
fn port(dc: &str) -> u16 {
    if dc == "mum1" { 22307 } else { 22317 }
}

fn admin(dc: &str) -> String {
    format!(
        "http://127.0.0.1:{}",
        if dc == "mum1" { 22326 } else { 22327 }
    )
}

/// The dev gateways' self-signed certificate: TLS lets the gateway use
/// `caching_sha2_password`'s full authentication.
fn tls() -> mysql_async::SslOpts {
    mysql_async::SslOpts::default()
        .with_danger_accept_invalid_certs(true)
        .with_danger_skip_domain_validation(true)
}

fn pool(dc: &str, max: usize) -> Pool {
    Pool::new(
        OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(port(dc))
            .user(Some(format!("{TENANT}.app")))
            .pass(Some("sprig-dev-app"))
            .ssl_opts(tls())
            .db_name(Some("dc_log"))
            .connect_attribute("sprig_dc", dc)
            .pool_opts(
                PoolOpts::new()
                    .with_constraints(PoolConstraints::new(0, max).unwrap())
                    .with_inactive_connection_ttl(Duration::from_secs(60)),
            ),
    )
}

fn sprigctl(dc: &str, args: &[&str]) -> serde_json::Value {
    let target = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".into());
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    let bin = format!("{root}/{target}/debug/sprigctl");
    let bin = if std::path::Path::new(&target).is_absolute() {
        format!("{target}/debug/sprigctl")
    } else {
        bin
    };
    let out = Command::new(bin)
        .args(["--endpoint", &admin(dc), "-o", "json"])
        .args(args)
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "sprigctl {args:?} failed: {}{}",
        text,
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_str(&text).unwrap_or(serde_json::Value::Null)
}

#[derive(Default)]
struct Log {
    /// Acknowledged state per key (`None`: deleted).
    rows: BTreeMap<i64, Option<i64>>,
    /// Keys whose last write's outcome is unknown, with the states it may
    /// have left.
    uncertain: BTreeMap<i64, Vec<Option<i64>>>,
    events: BTreeSet<(u32, u64)>,
    events_uncertain: BTreeSet<(u32, u64)>,
    /// Acknowledgement times, for the longest gap.
    acks: Vec<Instant>,
    max_latency: Duration,
    errors: BTreeMap<String, u64>,
}

fn code(e: &mysql_async::Error) -> String {
    match e {
        mysql_async::Error::Server(s) => s.code.to_string(),
        other => format!(
            "io: {}",
            other.to_string().chars().take(60).collect::<String>()
        ),
    }
}

async fn writer(
    pool: Pool,
    me: u32,
    log: Arc<Mutex<Log>>,
    stop: CancellationToken,
    ops: Arc<AtomicU64>,
) {
    let mut seq = 0u64;
    let mut n = 0i64;
    while !stop.is_cancelled() {
        tokio::time::sleep(Duration::from_millis(10)).await;
        n += 1;
        let key = i64::from(me) * 10_000 + n % 300;
        let delete = n % 7 == 0;
        seq += 1;
        let started = Instant::now();
        let r: Result<(), mysql_async::Error> = async {
            let mut c = pool.get_conn().await?;
            c.query_drop("BEGIN").await?;
            if delete {
                c.exec_drop("DELETE FROM kv WHERE id = ?", (key,)).await?;
            } else {
                c.exec_drop(
                    "INSERT INTO kv (id, k) VALUES (?, ?) ON DUPLICATE KEY UPDATE k = VALUES(k)",
                    (key, n),
                )
                .await?;
            }
            c.exec_drop("INSERT INTO events (writer, seq) VALUES (?, ?)", (me, seq))
                .await?;
            c.query_drop("COMMIT").await
        }
        .await;
        let mut l = log.lock().unwrap();
        match r {
            Ok(()) => {
                l.rows.insert(key, (!delete).then_some(n));
                l.uncertain.remove(&key);
                l.events.insert((me, seq));
                l.acks.push(Instant::now());
                l.max_latency = l.max_latency.max(started.elapsed());
                ops.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                *l.errors.entry(code(&e)).or_default() += 1;
                // Only a lost connection (or a lost COMMIT reply) leaves the
                // outcome unknown.
                if !matches!(e, mysql_async::Error::Server(_)) {
                    let prev = l.rows.get(&key).copied().flatten();
                    l.uncertain
                        .entry(key)
                        .or_default()
                        .extend([prev, (!delete).then_some(n)]);
                    l.events_uncertain.insert((me, seq));
                }
            }
        }
    }
}

/// Median of `n` round trips of a statement that reaches the home's
/// MariaDB, from `dc`'s gateway.
async fn round_trip(dc: &str) -> Duration {
    let p = pool(dc, 1);
    let mut c = p.get_conn().await.unwrap();
    let mut v = Vec::new();
    for _ in 0..15 {
        let t = Instant::now();
        let _: Option<i64> = c
            .query_first("SELECT COUNT(*) FROM filler WHERE id = 1")
            .await
            .unwrap();
        v.push(t.elapsed());
    }
    drop(c);
    p.disconnect().await.unwrap();
    v.sort();
    v[v.len() / 2]
}

async fn checksum(dc: &str) -> (i64, i64) {
    let p = pool(dc, 1);
    let mut c = p.get_conn().await.unwrap();
    let r: (i64, i64) = c
        .query_first(
            "SELECT COUNT(*), CAST(BIT_XOR(CRC32(CONCAT_WS('|', id, a, b))) AS SIGNED) FROM filler",
        )
        .await
        .unwrap()
        .unwrap();
    drop(c);
    p.disconnect().await.unwrap();
    r
}

struct Probes {
    follower: mysql_async::Conn,
    stmt: mysql_async::Statement,
    temp: mysql_async::Conn,
}

async fn open_probes(dc: &str) -> Probes {
    let p = pool(dc, 2);
    let mut follower = p.get_conn().await.unwrap();
    follower.query_drop("SET @probe = 41").await.unwrap();
    let stmt = follower.prep("SELECT @probe + ?").await.unwrap();
    let mut temp = p.get_conn().await.unwrap();
    temp.query_drop("CREATE TEMPORARY TABLE scratch (a INT)")
        .await
        .unwrap();
    temp.query_drop("INSERT INTO scratch VALUES (1)")
        .await
        .unwrap();
    Probes {
        follower,
        stmt,
        temp,
    }
}

async fn check_probes(mut p: Probes, to: &str) -> String {
    let v: Option<i64> = p.follower.exec_first(&p.stmt, (1,)).await.unwrap();
    assert_eq!(
        v,
        Some(42),
        "the idle session's variable and prepared statement followed"
    );
    let db: Option<String> = p.follower.query_first("SELECT DATABASE()").await.unwrap();
    assert_eq!(db.as_deref(), Some("dc_log"));
    let temp: Result<Option<i64>, _> = p.temp.query_first("SELECT a FROM scratch").await;
    match temp {
        Ok(v) => format!("temporary-table session still works ({v:?}): no DC change for it"),
        Err(e) => {
            let c = code(&e);
            assert!(
                c == "39502" || c == "39033" || c.starts_with("io"),
                "a session with a temporary table gets 39502 (or loses its connection), got {c}: {e}"
            );
            format!("temporary-table session after the move to {to}: {c}")
        }
    }
}

async fn run_move(from: &str, to: &str) -> (u64, u64, Duration, Duration) {
    let log = Arc::new(Mutex::new(Log::default()));
    let stop = CancellationToken::new();
    let ops = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();
    let pools: Vec<(String, Pool)> = ["mum1", "fra1"]
        .iter()
        .map(|d| ((*d).to_owned(), pool(d, 4)))
        .collect();
    let mut me = if from == "mum1" { 0 } else { 100 };
    for (_dc, p) in &pools {
        for _ in 0..3 {
            me += 1;
            tasks.push(tokio::spawn(writer(
                p.clone(),
                me,
                log.clone(),
                stop.clone(),
                ops.clone(),
            )));
        }
    }
    let probes = open_probes(from).await;
    let probes_remote = open_probes(to).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    println!(
        "{from} -> {to}: {} writes acknowledged before the move",
        ops.load(Ordering::Relaxed)
    );
    let t = Instant::now();
    let v = sprigctl(
        from,
        &[
            "tenant",
            "move",
            TENANT,
            "--to",
            to,
            "--wait",
            "--max-pause-ms",
            &BUDGET_MS.to_string(),
        ],
    );
    let wall = t.elapsed();
    assert_eq!(v["home_dc"], to, "the move committed: {v}");
    let pause = v["pause_ms"].as_u64().unwrap_or(0);
    let duration = v["duration_ms"].as_u64().unwrap_or(0);
    let move_started = t;
    // Keep writing after the commit, through both gateways.
    tokio::time::sleep(Duration::from_secs(8)).await;
    stop.cancel();
    for t in tasks {
        t.await.unwrap();
    }
    println!("{}", check_probes(probes, to).await);
    println!(
        "{} (session opened on the new home's gateway)",
        check_probes(probes_remote, to).await
    );
    for (_, p) in pools {
        p.disconnect().await.unwrap();
    }
    let l = Arc::try_unwrap(log).ok().unwrap().into_inner().unwrap();
    // Every error was a clean, retryable one.
    for (c, n) in &l.errors {
        assert!(
            c == "1213" || c == "1205",
            "unexpected write error {c} ({n} times); all: {:?}",
            l.errors
        );
    }
    // The longest gap between acknowledged writes, around the move.
    let mut acks: Vec<Instant> = l
        .acks
        .iter()
        .copied()
        .filter(|a| *a >= move_started)
        .collect();
    acks.sort();
    let gap = acks
        .windows(2)
        .map(|w| w[1] - w[0])
        .max()
        .unwrap_or_default();
    // Every acknowledged write is on the new home, exactly once.
    let p = pool(to, 1);
    let mut c = p.get_conn().await.unwrap();
    let rows: Vec<(i64, i64)> = c.query("SELECT id, k FROM kv").await.unwrap();
    let have: BTreeMap<i64, i64> = rows.into_iter().collect();
    let mut checked = 0;
    for (key, want) in &l.rows {
        if let Some(options) = l.uncertain.get(key) {
            assert!(
                options.contains(&have.get(key).copied()),
                "key {key}: {:?} not among {options:?}",
                have.get(key)
            );
            continue;
        }
        assert_eq!(have.get(key).copied(), *want, "key {key}");
        checked += 1;
    }
    let events: Vec<(u32, u64)> = c.query("SELECT writer, seq FROM events").await.unwrap();
    let got: BTreeSet<(u32, u64)> = events.iter().copied().collect();
    assert_eq!(got.len(), events.len(), "no event twice");
    for e in &l.events {
        assert!(got.contains(e), "acknowledged event {e:?} lost");
    }
    // This round's writers: base + 1 ..= base + 6.
    let base = if from == "mum1" { 0 } else { 100 };
    for e in got
        .difference(&l.events)
        .filter(|e| e.0 > base && e.0 <= base + 6)
    {
        assert!(
            l.events_uncertain.contains(e),
            "event {e:?} was never acknowledged nor uncertain"
        );
    }
    drop(c);
    p.disconnect().await.unwrap();
    println!(
        "{from} -> {to}: pause (mover hold) {pause} ms, start to commit {duration} ms, sprigctl --wait {} ms, \
         longest gap between acknowledged writes {} ms, longest write {} ms; {} writes acknowledged, {} keys and {} events checked, errors {:?}",
        wall.as_millis(),
        gap.as_millis(),
        l.max_latency.as_millis(),
        l.events.len(),
        checked,
        l.events.len(),
        l.errors
    );
    assert!(
        pause > 0 && pause <= BUDGET_MS,
        "the pause {pause} ms is within {BUDGET_MS} ms"
    );
    (pause, duration, gap, l.max_latency)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn tenant_moves_between_data_centers_under_load() {
    if !enabled() {
        eprintln!("skipped: set SPRIG_TEST_DC_MOVE=1 with deploy/compose/dc/run.sh up");
        return;
    }
    let t = sprigctl("mum1", &["tenant", "show", TENANT]);
    let home = t["home_dc"].as_str().unwrap().to_owned();
    let away = if home == "mum1" { "fra1" } else { "mum1" };
    // Schema and a filler table, through the home's gateway.
    {
        let setup = Pool::new(
            OptsBuilder::default()
                .ip_or_hostname("127.0.0.1")
                .tcp_port(port(&home))
                .user(Some(format!("{TENANT}.app")))
                .pass(Some("sprig-dev-app"))
                .ssl_opts(tls()),
        );
        let mut c = setup.get_conn().await.unwrap();
        c.query_drop("CREATE DATABASE IF NOT EXISTS dc_log")
            .await
            .unwrap();
        c.query_drop("USE dc_log").await.unwrap();
        for t in ["kv", "events", "filler"] {
            c.query_drop(format!("DROP TABLE IF EXISTS {t}"))
                .await
                .unwrap();
        }
        c.query_drop("CREATE TABLE kv (id BIGINT PRIMARY KEY, k BIGINT NOT NULL)")
            .await
            .unwrap();
        c.query_drop("CREATE TABLE events (writer INT, seq BIGINT, at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6), PRIMARY KEY (writer, seq))").await.unwrap();
        c.query_drop("CREATE TABLE filler (id INT PRIMARY KEY, a VARCHAR(64), b DOUBLE)")
            .await
            .unwrap();
        for chunk in 0..20 {
            let values: Vec<String> = (0..1000)
                .map(|i| {
                    let id = chunk * 1000 + i;
                    format!(
                        "({id}, 'row-{id}-{}', {})",
                        id * 7 % 13,
                        f64::from(id) / 3.0
                    )
                })
                .collect();
            c.query_drop(format!("INSERT INTO filler VALUES {}", values.join(",")))
                .await
                .unwrap();
        }
        drop(c);
        setup.disconnect().await.unwrap();
    }
    let before = checksum(&home).await;
    println!(
        "statement round trip with the home in {home}: from {home}'s gateway {} ms, from {away}'s {} ms",
        round_trip(&home).await.as_millis(),
        round_trip(away).await.as_millis()
    );
    let first = run_move(&home, away).await;
    assert_eq!(
        checksum(away).await,
        before,
        "filler unchanged after {home} -> {away}"
    );
    // The traffic window on the new home: both client DCs show up once the
    // gateways have reported (every 10 s).
    tokio::time::sleep(Duration::from_secs(12)).await;
    let tr = sprigctl(away, &["tenant", "traffic", TENANT]);
    println!("traffic on {away} after the move: {}", tr["clients"]);
    println!(
        "summary: {home}->{away} pause {} ms, commit after {} ms, longest gap between acknowledged writes {} ms, longest write {} ms",
        first.0,
        first.1,
        first.2.as_millis(),
        first.3.as_millis()
    );
}
