//! Live: many stream instances on one cluster share one binlog dump (R9,
//! r9feed; decision 127 item 1). Against a private deployment (a gateway,
//! one worker, the controller, one MariaDB node).
//!
//! ```text
//! SPRIG_SF_GW=127.0.0.1:32306                  # the gateway's MySQL port
//! SPRIG_SF_TENANTS=r9fa,r9fb,r9fc,r9fd         # tenants with user app
//! SPRIG_SF_STREAMS=52                          # streams per tenant
//! SPRIG_SF_TXNS=8                              # transactions per stream
//! SPRIG_SF_NODE=mysql://root:pw@127.0.0.1:32301  # the node, for PROCESSLIST
//! SPRIG_SF_WORKER_PIDFILE=<state>/worker.pid   # optional: kill -9 mid-run
//! SPRIG_SF_WORKER_START='<command starting the worker again>'
//! cargo test -p sprig-streams --test live_shared_feed -- --ignored --nocapture
//! ```
//!
//! Each tenant gets `SPRIG_SF_STREAMS` event streams of one partition in
//! database `sf` (default 4 × 52 = 208 instances, each with its own
//! sequencer and checkpoint). The test:
//!
//! 1. publishes a probe into every stream and waits until each has it (all
//!    sequencers run), then counts `Binlog Dump` threads on the node:
//!    `SPRIG_SF_EXPECT_DUMPS` (1) is the most allowed;
//! 2. runs `SPRIG_SF_PUBLISHERS` sessions of transactions, each
//!
//!    ```text
//!    BEGIN
//!    SELECT n FROM ctr WHERE k = <stream> FOR UPDATE   -- commits on one
//!    UPDATE ctr SET n = n + 1 WHERE k = <stream>        -- stream serialize:
//!    INSERT INTO ord VALUES (<stream>, <id>, n + 1)     -- n is commit order
//!    INSERT INTO s<stream> (key, value) VALUES ('<id>', '{"n": n + 1}')
//!    COMMIT (every 7th: ROLLBACK)
//!    ```
//!
//!    and, halfway, `kill -9`s the worker and starts it again (when the
//!    pid file and start command are given), drops one probe-only stream
//!    per tenant and creates a new one (instances removed and added live);
//! 3. waits for every stream to hold its committed records, and checks per
//!    stream: offsets without gaps, every committed id once, no rolled-back
//!    id, `n` rising (commit order), and the dump count again.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    missing_docs
)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts};

const DB: &str = "sf";

fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

fn num(k: &str, d: u64) -> u64 {
    var(k).and_then(|v| v.parse().ok()).unwrap_or(d)
}

fn pool(gw: &str, tenant: &str, max: usize) -> Pool {
    let (host, port) = gw.split_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{tenant}.app")))
        .pass(Some(var("SPRIG_SF_PASSWORD").unwrap_or_else(|| "sprig-dev-app".into())))
        .db_name(Some(DB))
        .ssl_opts(Some(
            mysql_async::SslOpts::default()
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true),
        ))
        .pool_opts(
            PoolOpts::new()
                .with_constraints(PoolConstraints::new(0, max).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(120)),
        );
    Pool::new(opts)
}

async fn conn(p: &Pool) -> Conn {
    for _ in 0..240 {
        if let Ok(c) = p.get_conn().await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("gateway unreachable");
}

/// `Binlog Dump` threads on the node.
async fn dumps(node: &str) -> u64 {
    let mut c = Conn::new(Opts::from_url(node).unwrap()).await.unwrap();
    let n: Option<u64> = c
        .query_first(
            "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE COMMAND LIKE 'Binlog Dump%'",
        )
        .await
        .unwrap();
    let _ = c.disconnect().await;
    n.unwrap_or(0)
}

async fn count(c: &mut Conn, stream: &str) -> u64 {
    c.query_first(format!("SELECT COUNT(*) FROM `{stream}`"))
        .await
        .unwrap()
        .unwrap_or(0)
}

async fn read_all(c: &mut Conn, stream: &str) -> Vec<(u64, String, String)> {
    let mut out = Vec::new();
    let mut from = 0u64;
    loop {
        let rows: Vec<(u64, Option<String>, Option<String>)> = c
            .query(format!(
                "SELECT offset_no, CAST(key AS CHAR), CAST(value AS CHAR) FROM `{stream}` \
                 WHERE partition_no = 0 AND offset_no >= {from} ORDER BY offset_no LIMIT 2000"
            ))
            .await
            .unwrap();
        if rows.is_empty() {
            break;
        }
        from = rows.last().unwrap().0 + 1;
        out.extend(
            rows.into_iter()
                .map(|(o, k, v)| (o, k.unwrap_or_default(), v.unwrap_or_default())),
        );
    }
    out
}

/// Creates stream `s` with one partition, or keeps it.
async fn create_stream(c: &mut Conn, s: &str) {
    for attempt in 0..5 {
        match c
            .query_drop(format!("CREATE STREAM IF NOT EXISTS {s} PARTITIONS 1"))
            .await
        {
            Ok(()) => return,
            Err(e) if attempt < 4 => {
                println!("CREATE STREAM {s}: {e}; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => panic!("CREATE STREAM {s}: {e}"),
        }
    }
}

/// Waits until every `(tenant, stream)` holds at least its count.
async fn wait_counts(pools: &[Pool], want: &BTreeMap<(usize, String), u64>, limit: Duration) {
    let t = Instant::now();
    let mut left: BTreeMap<(usize, String), u64> = want.clone();
    while !left.is_empty() {
        let mut still = BTreeMap::new();
        for ((ti, s), n) in &left {
            let mut c = conn(&pools[*ti]).await;
            let have = count(&mut c, s).await;
            if have < *n {
                still.insert((*ti, s.clone()), *n);
            }
        }
        left = still;
        if left.is_empty() {
            break;
        }
        assert!(
            t.elapsed() < limit,
            "{} streams still behind after {:?}: {:?}",
            left.len(),
            limit,
            left.iter().take(5).collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "live: needs a private deployment (see the module docs)"]
async fn hundreds_of_stream_instances_share_one_binlog_dump() {
    let gw = var("SPRIG_SF_GW").expect("SPRIG_SF_GW");
    let node = var("SPRIG_SF_NODE").expect("SPRIG_SF_NODE");
    let tenants: Vec<String> = var("SPRIG_SF_TENANTS")
        .expect("SPRIG_SF_TENANTS")
        .split(',')
        .map(str::to_owned)
        .collect();
    let per = num("SPRIG_SF_STREAMS", 52) as usize;
    let txns = num("SPRIG_SF_TXNS", 8);
    let publishers = num("SPRIG_SF_PUBLISHERS", 16) as usize;
    let expect_dumps = num("SPRIG_SF_EXPECT_DUMPS", 1);
    let pools: Vec<Pool> = tenants
        .iter()
        .map(|t| pool(&gw, t, publishers + 2))
        .collect();
    let before = dumps(&node).await;
    println!(
        "{} tenants x {per} streams = {} instances; dumps on the node before: {before}",
        tenants.len(),
        tenants.len() * per
    );

    // 1. Streams and their tables; a probe in each.
    let t0 = Instant::now();
    let setups: Vec<_> = tenants
        .iter()
        .enumerate()
        .map(|(ti, tenant)| {
            let p = pools[ti].clone();
            let tenant = tenant.clone();
            tokio::spawn(async move {
                let mut c = conn(&p).await;
                for s in 0..per {
                    let _ = c.query_drop(format!("DROP STREAM IF EXISTS s{s}")).await;
                }
                let _ = c.query_drop("DROP STREAM IF EXISTS sx").await;
                c.query_drop("DROP TABLE IF EXISTS ctr").await.unwrap();
                c.query_drop("DROP TABLE IF EXISTS ord").await.unwrap();
                c.query_drop("CREATE TABLE ctr (k INT PRIMARY KEY, n BIGINT NOT NULL)")
                    .await
                    .unwrap();
                c.query_drop(
                    "CREATE TABLE ord (k INT NOT NULL, id BIGINT NOT NULL, n BIGINT NOT NULL, \
                     PRIMARY KEY (k, id))",
                )
                .await
                .unwrap();
                let values: Vec<String> = (0..=per).map(|k| format!("({k}, 0)")).collect();
                c.query_drop(format!("INSERT INTO ctr VALUES {}", values.join(",")))
                    .await
                    .unwrap();
                for s in 0..per {
                    create_stream(&mut c, &format!("s{s}")).await;
                }
                for s in 0..per {
                    c.query_drop(format!(
                        "INSERT INTO s{s} (key, value) VALUES ('probe', 'p')"
                    ))
                    .await
                    .unwrap();
                }
                println!("{tenant}: {per} streams made");
            })
        })
        .collect();
    for s in setups {
        s.await.unwrap();
    }
    println!("streams made in {:.1} s", t0.elapsed().as_secs_f64());
    let mut probe = BTreeMap::new();
    for ti in 0..tenants.len() {
        for s in 0..per {
            probe.insert((ti, format!("s{s}")), 1);
        }
    }
    let t1 = Instant::now();
    wait_counts(&pools, &probe, Duration::from_secs(600)).await;
    println!(
        "every sequencer ran its probe {:.1} s later",
        t1.elapsed().as_secs_f64()
    );
    let d1 = dumps(&node).await;
    println!("Binlog Dump threads with every instance running: {d1}");
    assert!(
        d1 <= expect_dumps,
        "{d1} binlog dumps for {} stream instances (expected at most {expect_dumps})",
        tenants.len() * per
    );

    // 2. Transactions, a worker kill, streams dropped and made live.
    let next_id = Arc::new(AtomicU64::new(1));
    let total = txns * (tenants.len() * per) as u64;
    let done = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let slots: Vec<(usize, usize)> = (0..tenants.len())
        .flat_map(|t| (0..per).map(move |s| (t, s)))
        .collect();
    let slots = Arc::new(slots);
    let t2 = Instant::now();
    let tasks: Vec<_> = (0..publishers)
        .map(|w| {
            let pools = pools.clone();
            let slots = Arc::clone(&slots);
            let next_id = Arc::clone(&next_id);
            let done = Arc::clone(&done);
            let errors = Arc::clone(&errors);
            tokio::spawn(async move {
                let mut i = w;
                loop {
                    let n = done.fetch_add(1, Ordering::SeqCst);
                    if n >= total {
                        break;
                    }
                    let (ti, s) = slots[i % slots.len()];
                    i += publishers;
                    let id = next_id.fetch_add(1, Ordering::SeqCst);
                    let commit = id % 7 != 0;
                    let r: Result<(), mysql_async::Error> = async {
                        let mut c = pools[ti].get_conn().await?;
                        c.query_drop("START TRANSACTION").await?;
                        let cur: Option<u64> = c
                            .query_first(format!("SELECT n FROM ctr WHERE k = {s} FOR UPDATE"))
                            .await?;
                        let n = cur.unwrap_or(0) + 1;
                        c.query_drop(format!("UPDATE ctr SET n = {n} WHERE k = {s}"))
                            .await?;
                        c.query_drop(format!("INSERT INTO ord VALUES ({s}, {id}, {n})"))
                            .await?;
                        c.query_drop(format!(
                            "INSERT INTO s{s} (key, value) VALUES ('{id}', '{{\"n\":{n},\"id\":{id}}}')"
                        ))
                        .await?;
                        c.query_drop(if commit { "COMMIT" } else { "ROLLBACK" })
                            .await?;
                        Ok(())
                    }
                    .await;
                    if let Err(e) = r {
                        // A lost COMMIT answer counts by `ord` later.
                        if errors.fetch_add(1, Ordering::SeqCst) < 5 {
                            println!("txn {id}: {e}");
                        }
                    }
                }
            })
        })
        .collect();
    // Halfway: kill the worker, drop and make streams.
    let mut killed = false;
    let mut changed = false;
    while !tasks.iter().all(tokio::task::JoinHandle::is_finished) {
        let n = done.load(Ordering::SeqCst);
        if !killed && n >= total / 2 {
            killed = true;
            if let (Some(pidfile), Some(start)) =
                (var("SPRIG_SF_WORKER_PIDFILE"), var("SPRIG_SF_WORKER_START"))
            {
                let pid = std::fs::read_to_string(&pidfile).unwrap().trim().to_owned();
                println!("kill -9 worker {pid} at {n} of {total} transactions");
                std::process::Command::new("kill")
                    .args(["-9", &pid])
                    .status()
                    .unwrap();
                tokio::time::sleep(Duration::from_secs(2)).await;
                let ok = std::process::Command::new("sh")
                    .args(["-c", &start])
                    .status()
                    .unwrap();
                assert!(ok.success(), "worker start failed");
                println!("worker started again");
            }
        }
        if !changed && n >= total * 3 / 5 {
            changed = true;
            // One probe-only stream per tenant is dropped and a new one
            // made, with records, while the others run.
            for p in &pools {
                let mut c = conn(p).await;
                c.query_drop(format!("DROP STREAM s{}", per - 1))
                    .await
                    .unwrap();
                create_stream(&mut c, "sx").await;
                for k in 0..5 {
                    c.query_drop(format!("INSERT INTO sx (key, value) VALUES ('x{k}', 'x')"))
                        .await
                        .unwrap();
                }
            }
            println!("dropped s{} and made sx in every tenant", per - 1);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    for t in tasks {
        t.await.unwrap();
    }
    let wrote = t2.elapsed().as_secs_f64();
    println!(
        "{total} transactions in {wrote:.1} s ({} errors)",
        errors.load(Ordering::SeqCst)
    );

    // 3. Every stream's records.
    let mut want: BTreeMap<(usize, String), u64> = BTreeMap::new();
    let mut truth: BTreeMap<(usize, usize), Vec<(u64, u64)>> = BTreeMap::new();
    for (ti, p) in pools.iter().enumerate() {
        let mut c = conn(p).await;
        let rows: Vec<(u64, u64, u64)> = c.query("SELECT k, id, n FROM ord").await.unwrap();
        for (k, id, n) in rows {
            truth.entry((ti, k as usize)).or_default().push((n, id));
        }
        for s in 0..per - 1 {
            let n = truth.get(&(ti, s)).map_or(0, Vec::len) as u64;
            want.insert((ti, format!("s{s}")), n + 1);
        }
        want.insert((ti, "sx".into()), 5);
    }
    let t3 = Instant::now();
    wait_counts(&pools, &want, Duration::from_secs(900)).await;
    println!(
        "sequencing caught up {:.1} s after the writers ended",
        t3.elapsed().as_secs_f64()
    );
    tokio::time::sleep(Duration::from_secs(3)).await;
    let mut checked = 0usize;
    let mut records = 0usize;
    let mut inversions = 0usize;
    for (ti, p) in pools.iter().enumerate() {
        let mut c = conn(p).await;
        for s in 0..per - 1 {
            let name = format!("s{s}");
            let recs = read_all(&mut c, &name).await;
            let offsets: Vec<u64> = recs.iter().map(|r| r.0).collect();
            assert_eq!(
                offsets,
                (0..recs.len() as u64).collect::<Vec<_>>(),
                "{}.{name}: offsets",
                tenants[ti]
            );
            let mut ids = BTreeSet::new();
            let mut last_n = 0u64;
            for (_, k, v) in recs.iter().filter(|r| r.1 != "probe") {
                let j: serde_json::Value = serde_json::from_str(v).unwrap();
                let id = j["id"].as_u64().unwrap();
                let n = j["n"].as_u64().unwrap();
                assert_eq!(k, &id.to_string());
                assert!(ids.insert(id), "{}.{name}: id {id} twice", tenants[ti]);
                assert_ne!(id % 7, 0, "{}.{name}: rolled-back id {id}", tenants[ti]);
                if n <= last_n {
                    inversions += 1;
                }
                last_n = n;
                records += 1;
            }
            let mut exp = truth.remove(&(ti, s)).unwrap_or_default();
            exp.sort_unstable();
            let exp_ids: BTreeSet<u64> = exp.iter().map(|x| x.1).collect();
            assert_eq!(
                ids,
                exp_ids,
                "{}.{name}: records differ from the committed transactions",
                tenants[ti]
            );
            checked += 1;
        }
        let sx = read_all(&mut c, "sx").await;
        assert_eq!(sx.len(), 5, "{}.sx: {sx:?}", tenants[ti]);
        checked += 1;
    }
    let d2 = dumps(&node).await;
    println!(
        "checked {checked} streams, {records} records: every committed record once, \
         {inversions} commit-order inversions; Binlog Dump threads now: {d2}"
    );
    assert_eq!(inversions, 0, "commit-order inversions");
    assert!(d2 <= expect_dumps, "{d2} binlog dumps at the end");
}
