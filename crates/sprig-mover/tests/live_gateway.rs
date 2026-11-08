//! A tenant moves around every topology pair under sysbench load, through
//! a gateway that honours the cutover's hold and follows the move
//! (milestone M0.6, done-when 1 and 3).
//!
//! ```sh
//! deploy/compose/gateway-dev.sh up          # etcd, clusters, controller
//! deploy/compose/mover/run.sh up            # gateway on 17306/17307, tenant moversb
//! docker run --rm --entrypoint sysbench sprigdb-sysbench:dev oltp_read_write ... prepare
//! SPRIG_TEST_MOVER_GATEWAY=1 cargo test -p sprig-mover --test live_gateway -- --nocapture
//! ```
//!
//! While `sysbench oltp_read_write` runs through the MySQL persona (17306),
//! logged writers use the MariaDB persona (17307) on `mlog.kv` and
//! `mlog.events`, and an in-process worker moves the tenant c1 → c2 → c3
//! → c1 → c3 → c2 → c1. Afterwards: every acknowledged logged write is on
//! the final cluster exactly once, every sysbench table still has its row
//! count (sysbench's deletes are paired with inserts), and each move's
//! hold and the longest write latency seen during it are printed.

#![allow(clippy::unwrap_used, clippy::too_many_lines, clippy::similar_names)]

use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{OptsBuilder, Pool};
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::CancellationToken;
use sprig_common::ids::{ClusterId, DcId, TenantId};
use sprig_common::time::SystemClock;
use sprig_meta::{EtcdConfig, EtcdStore, MetaStore, Tier};
use sprig_mover::mysql::MysqlEnvFactory;
use sprig_mover::{
    JobRuntime, JobSpec, JobState, JobStore, MetaMover, Mover, MoverSettings, WorkerOpts,
};
use sprig_shard::{Placement, TenantMaps};

const SB: &str = "sprigdb-dev-mover-sb";

#[derive(Default)]
struct Log {
    rows: BTreeMap<i64, Option<i64>>,
    uncertain: BTreeMap<i64, Vec<Option<i64>>>,
    events: Vec<(u32, u64)>,
    events_uncertain: Vec<(u32, u64)>,
    max_latency: Duration,
    errors: u64,
}

fn gateway_pool() -> Pool {
    Pool::new(
        OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(17307)
            .user(Some("moversb.app"))
            .pass(Some("sprig-dev-app"))
            .db_name(Some("mlog")),
    )
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
        tokio::time::sleep(Duration::from_millis(5)).await;
        n += 1;
        let key = i64::from(me) * 10_000 + n % 500;
        let delete = n % 5 == 0;
        seq += 1;
        let started = Instant::now();
        loop {
            if stop.is_cancelled() {
                return;
            }
            let r: Result<(), mysql_async::Error> = async {
                let mut c = pool.get_conn().await?;
                c.query_drop("BEGIN").await?;
                if delete {
                    c.exec_drop("DELETE FROM kv WHERE id = ?", (key,)).await?;
                } else {
                    c.exec_drop(
                        "INSERT INTO kv (id, k, c) VALUES (?, ?, 'x') ON DUPLICATE KEY UPDATE k = VALUES(k)",
                        (key, n),
                    )
                    .await?;
                }
                c.exec_drop("INSERT INTO events (writer, seq) VALUES (?, ?)", (me, seq))
                    .await?;
                c.query_drop("COMMIT").await
            }
            .await;
            let ok = {
                let mut l = log.lock().unwrap();
                match r {
                    Ok(()) => {
                        l.rows.insert(key, (!delete).then_some(n));
                        l.uncertain.remove(&key);
                        l.events.push((me, seq));
                        l.max_latency = l.max_latency.max(started.elapsed());
                        ops.fetch_add(1, Ordering::Relaxed);
                        true
                    }
                    Err(e) => {
                        l.errors += 1;
                        // Only a lost connection leaves the outcome unknown.
                        if !matches!(e, mysql_async::Error::Server(_)) {
                            let prev = l.rows.get(&key).copied().flatten();
                            l.uncertain
                                .entry(key)
                                .or_default()
                                .extend([prev, (!delete).then_some(n)]);
                            l.events_uncertain.push((me, seq));
                            seq += 1;
                        }
                        false
                    }
                }
            };
            if ok {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

fn docker(args: &[&str]) -> String {
    let o = Command::new("docker").args(args).output().unwrap();
    String::from_utf8_lossy(&o.stdout).into_owned() + &String::from_utf8_lossy(&o.stderr)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tenant_moves_under_sysbench_through_the_gateway() {
    if std::env::var("SPRIG_TEST_MOVER_GATEWAY").is_err() {
        eprintln!("skipped: set SPRIG_TEST_MOVER_GATEWAY=1 after deploy/compose/mover/run.sh up");
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "sprig_mover=info,warn".into()),
        )
        .with_test_writer()
        .try_init();
    let root = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../deploy/compose/mover/.state/tenant_id"
    );
    let tenant = TenantId(
        std::fs::read_to_string(root)
            .unwrap()
            .trim()
            .parse()
            .unwrap(),
    );
    let cfg = EtcdConfig::new(vec!["http://127.0.0.1:2379".into()]);
    let dc: Arc<dyn MetaStore> = Arc::new(
        EtcdStore::connect(Tier::Dc(DcId::new("dc-a").unwrap()), &cfg)
            .await
            .unwrap(),
    );
    let global: Arc<dyn MetaStore> =
        Arc::new(EtcdStore::connect(Tier::Global, &cfg).await.unwrap());
    let maps = TenantMaps::new(dc.clone());
    let start = Placement::of(&maps.load(tenant).await.unwrap().0).unwrap();

    let settings = MoverSettings {
        copy_chunk_rows: 1000,
        ready_sustain: Duration::from_secs(3),
        cutover_retry: Duration::from_secs(2),
        source_retain: Duration::from_secs(3),
        ..MoverSettings::default()
    };
    let registry = ClusterRegistry::new(global, dc.clone()).unwrap();
    let factory = Arc::new(MysqlEnvFactory::new(registry, maps.clone(), settings));
    let store = JobStore::new(dc.clone(), Arc::new(SystemClock));
    let mut opts = WorkerOpts::new("mover-gw-test");
    opts.scan_interval = Duration::from_millis(200);
    let stop_worker = CancellationToken::new();
    let worker =
        tokio::spawn(JobRuntime::new(store.clone(), factory, opts).run(stop_worker.clone()));
    let mover = MetaMover::new(store);

    // Load: sysbench through the MySQL persona, logged writers through the
    // MariaDB persona.
    let secs = std::env::var("SPRIG_TEST_MOVER_SB_SECONDS").unwrap_or_else(|_| "900".into());
    let _ = docker(&["rm", "-f", SB]);
    let out = docker(&[
        "run",
        "-d",
        "--name",
        SB,
        "--memory",
        "256m",
        "--entrypoint",
        "sysbench",
        "sprigdb-sysbench:dev",
        "oltp_read_write",
        "--db-driver=mysql",
        "--mysql-host=host.docker.internal",
        "--mysql-port=17306",
        "--mysql-user=moversb.app",
        "--mysql-password=sprig-dev-app",
        "--mysql-db=sbtest",
        "--mysql-ssl=off",
        "--tables=4",
        "--table-size=5000",
        "--threads=4",
        &format!("--time={secs}"),
        "--report-interval=1",
        "--db-ps-mode=disable",
        "--mysql-ignore-errors=1213,1146,1142,1044,1205,2013,1927,39110",
        "run",
    ]);
    assert!(!out.contains("Error"), "sysbench didn't start: {out}");
    let log = Arc::new(Mutex::new(Log::default()));
    let stop = CancellationToken::new();
    let ops = Arc::new(AtomicU64::new(0));
    let pool = gateway_pool();
    let writers: Vec<_> = (0..4)
        .map(|i| {
            tokio::spawn(writer(
                pool.clone(),
                i,
                log.clone(),
                stop.clone(),
                ops.clone(),
            ))
        })
        .collect();
    tokio::time::sleep(Duration::from_secs(5)).await;

    let cycle = [
        "dc-a-c1", "dc-a-c2", "dc-a-c3", "dc-a-c1", "dc-a-c3", "dc-a-c2", "dc-a-c1",
    ];
    let first = cycle
        .iter()
        .position(|c| *c == start.cluster.as_str())
        .unwrap_or(0);
    let route: Vec<&str> = cycle[first..]
        .iter()
        .chain(&cycle[1..=first])
        .copied()
        .collect();
    let pairs: usize = std::env::var("SPRIG_TEST_MOVER_PAIRS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(6);
    let mut report = Vec::new();
    for pair in route.windows(2).take(pairs) {
        let (from, to) = (pair[0], pair[1]);
        log.lock().unwrap().max_latency = Duration::ZERO;
        let ops0 = ops.load(Ordering::Relaxed);
        let t0 = Instant::now();
        let id = mover
            .submit(JobSpec::move_tenant(
                tenant,
                ClusterId::new(from).unwrap(),
                ClusterId::new(to).unwrap(),
            ))
            .await
            .unwrap();
        let st = loop {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let Ok(st) = mover.status(&id).await else {
                continue; // an etcd hiccup
            };
            if st.state.is_terminal() || st.state == JobState::Paused {
                break st;
            }
            assert!(
                t0.elapsed() < Duration::from_secs(900),
                "{from} → {to} stuck: {st:?}"
            );
        };
        assert_eq!(st.state, JobState::Done, "{from} → {to}: {st:?}");
        let lat = log.lock().unwrap().max_latency;
        let line = format!(
            "{from} → {to}: {:.1} s, copied {} rows, applied {} changes, holds {:?} ms, \
             {} attempt(s), verify mismatches {}, logged writes {}, longest logged write {} ms",
            t0.elapsed().as_secs_f64(),
            st.progress.rows_copied,
            st.progress.rows_applied,
            st.progress.holds_ms,
            st.progress.cutover_attempts,
            st.progress.verify_mismatches,
            ops.load(Ordering::Relaxed) - ops0,
            lat.as_millis()
        );
        eprintln!("{line}");
        report.push(line);
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    stop.cancel();
    for w in writers {
        w.await.unwrap();
    }
    let _ = docker(&["stop", "-t", "2", SB]);
    let sb = docker(&["logs", SB]);
    let _ = docker(&["rm", "-f", SB]);
    stop_worker.cancel();
    let _ = worker.await;

    // Checks, through the gateway.
    let l = std::mem::take(&mut *log.lock().unwrap());
    let mut c = pool.get_conn().await.unwrap();
    let rows: Vec<(i64, i64)> = c.query("SELECT id, k FROM kv").await.unwrap();
    let got: BTreeMap<i64, i64> = rows.into_iter().collect();
    let mut bad = Vec::new();
    for (key, want) in &l.rows {
        let have = got.get(key).copied();
        if have != *want && !l.uncertain.get(key).is_some_and(|a| a.contains(&have)) {
            bad.push(format!("kv {key}: {have:?}, logged {want:?}"));
        }
    }
    let ev: Vec<(u32, u64)> = c.query("SELECT writer, seq FROM events").await.unwrap();
    let mut counts: BTreeMap<(u32, u64), u32> = BTreeMap::new();
    for e in ev {
        *counts.entry(e).or_default() += 1;
    }
    for e in &l.events {
        if counts.get(e) != Some(&1) {
            bad.push(format!("event {e:?} x{:?}", counts.get(e)));
        }
    }
    let known: BTreeSet<_> = l.events.iter().chain(&l.events_uncertain).collect();
    bad.extend(
        counts
            .keys()
            .filter(|e| !known.contains(e))
            .map(|e| format!("event {e:?} never written")),
    );
    for t in 1..=4 {
        let n: Option<u64> = c
            .query_first(format!("SELECT COUNT(*) FROM sbtest.sbtest{t}"))
            .await
            .unwrap();
        if n != Some(5000) {
            bad.push(format!("sbtest{t} has {n:?} rows, not 5000"));
        }
    }
    let end = Placement::of(&maps.load(tenant).await.unwrap().0).unwrap();
    eprintln!(
        "logged: {} acknowledged, {} uncertain, {} errors retried; final {} epoch {}",
        l.events.len(),
        l.events_uncertain.len(),
        l.errors,
        end.cluster,
        end.epoch
    );
    let tail: Vec<&str> = sb.lines().rev().take(40).collect();
    eprintln!(
        "sysbench (end):\n{}",
        tail.into_iter().rev().collect::<Vec<_>>().join("\n")
    );
    // Per-second reports: the lowest throughput second and how many
    // seconds had no transactions at all.
    let tps: Vec<f64> = sb
        .lines()
        .filter_map(|l| l.split("tps: ").nth(1))
        .filter_map(|r| r.split_whitespace().next()?.parse().ok())
        .collect();
    let zero = tps.iter().filter(|t| **t == 0.0).count();
    eprintln!(
        "sysbench: {} one-second reports, {zero} with zero tps, min {:?}",
        tps.len(),
        tps.iter()
            .copied()
            .fold(None, |m: Option<f64>, t| Some(m.map_or(t, |m| m.min(t))))
    );
    std::fs::write(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../deploy/compose/mover/.state/report.txt"
        ),
        report.join("\n") + "\n" + &sb,
    )
    .unwrap();
    assert!(bad.is_empty(), "{bad:#?}");
}
