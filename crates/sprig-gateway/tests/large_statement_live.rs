//! Very large statements don't stall the gateway: a 64 MiB text statement, the 64 MiB row it wrote read
//! back, and a 16 MiB prepared-statement parameter go through a running
//! gateway while other sessions keep running small statements; their
//! latency is measured before and during the large ones.
//!
//! Needs a gateway on a cluster whose `max_allowed_packet` is at least
//! 64 MiB (a private one: never the shared development clusters) and a
//! login whose default database may be written:
//!
//! ```sh
//! export SPRIG_TEST_LARGE_GATEWAY=mysql://gwhard.app:sprig-dev-app@127.0.0.1:41306/gwbig
//! cargo test -p sprig-gateway --test large_statement_live -- --nocapture --test-threads 1
//! ```
//!
//! `SPRIG_TEST_LARGE_MB` (64) and `SPRIG_TEST_LARGE_PARAM_MB` (16) change
//! the sizes; `SPRIG_TEST_LARGE_EXTRA` (2) more sessions each run a 16 MiB
//! `SELECT` three times meanwhile, so a gateway with few worker threads
//! has all of them busy; `SPRIG_TEST_LARGE_STALL_MS` (1000) is the most a
//! small statement may take while they run. With
//! `SPRIG_TEST_LARGE_DIRECT` (a `mysql://` URL of the same table straight
//! on the cluster) as many sessions run the small statement on MariaDB
//! directly, which tells a gateway stall from a server one.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    missing_docs
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, SslOpts};
use sprig_testkit::skip;

const VAR: &str = "SPRIG_TEST_LARGE_GATEWAY";
const MIB: usize = 1 << 20;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn opts(url: &str) -> Opts {
    let base = Opts::from_url(url).expect("SPRIG_TEST_LARGE_GATEWAY is a mysql:// URL");
    OptsBuilder::from_opts(base)
        .ssl_opts(Some(
            SslOpts::default().with_danger_accept_invalid_certs(true),
        ))
        .max_allowed_packet(Some(256 * MIB))
        .wait_timeout(None)
        .into()
}

/// Latencies (start offset, duration) of one small-statement session.
type Samples = Vec<(Duration, Duration)>;

async fn small_loop(url: String, stop: Arc<AtomicBool>, t0: Instant) -> Samples {
    let mut c = Conn::new(opts(&url)).await.expect("connect");
    let mut out = Vec::new();
    while !stop.load(Ordering::Relaxed) {
        let s = Instant::now();
        let v: Option<i64> = c
            .query_first("SELECT v FROM small WHERE id = 1")
            .await
            .expect("small statement");
        assert_eq!(v, Some(1));
        out.push((s - t0, s.elapsed()));
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    c.disconnect().await.ok();
    out
}

struct Stats {
    n: usize,
    p50: Duration,
    p99: Duration,
    max: Duration,
}

fn stats(mut d: Vec<Duration>) -> Stats {
    d.sort();
    let at = |q: f64| {
        d.get(((d.len() as f64 - 1.0) * q).round() as usize)
            .copied()
            .unwrap_or_default()
    };
    Stats {
        n: d.len(),
        p50: at(0.50),
        p99: at(0.99),
        max: d.last().copied().unwrap_or_default(),
    }
}

fn show(label: &str, s: &Stats) {
    println!(
        "{label}: {} statements, p50 {:.2} ms, p99 {:.2} ms, max {:.2} ms",
        s.n,
        s.p50.as_secs_f64() * 1e3,
        s.p99.as_secs_f64() * 1e3,
        s.max.as_secs_f64() * 1e3
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_statements_dont_stall_other_sessions() {
    let Some(url) = skip::var(VAR) else {
        skip::report_missing(VAR, "large_statements_dont_stall_other_sessions");
        return;
    };
    let big_mb = env_usize("SPRIG_TEST_LARGE_MB", 64);
    let param_mb = env_usize("SPRIG_TEST_LARGE_PARAM_MB", 16);
    let bound = Duration::from_millis(env_usize("SPRIG_TEST_LARGE_STALL_MS", 1000) as u64);

    let mut big = Conn::new(opts(&url)).await.expect("connect");
    let limit: Option<u64> = big
        .query_first("SELECT @@max_allowed_packet")
        .await
        .unwrap();
    println!("@@max_allowed_packet through the gateway: {limit:?}");
    big.query_drop("CREATE TABLE IF NOT EXISTS big (id BIGINT PRIMARY KEY, b LONGBLOB)")
        .await
        .unwrap();
    big.query_drop("CREATE TABLE IF NOT EXISTS small (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    big.query_drop("INSERT IGNORE INTO small VALUES (1, 1)")
        .await
        .unwrap();
    big.query_drop("DELETE FROM big").await.unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();
    // The small sessions run on a runtime of their own: this process's
    // work on the large statements (building them, TLS) must not delay
    // them and pass for the gateway's stall.
    let small_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let loops: Vec<_> = (0..8)
        .map(|_| small_rt.spawn(small_loop(url.clone(), Arc::clone(&stop), t0)))
        .collect();
    let direct_url = std::env::var("SPRIG_TEST_LARGE_DIRECT").ok();
    let direct: Vec<_> = direct_url
        .iter()
        .flat_map(|u| (0..8).map(move |_| u.clone()))
        .map(|u| small_rt.spawn(small_loop(u, Arc::clone(&stop), t0)))
        .collect();

    // Quiet phase.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let quiet_end = t0.elapsed();

    // Extra sessions with 16 MiB statements of their own.
    let extra = env_usize("SPRIG_TEST_LARGE_EXTRA", 2);
    let extras: Vec<_> = (0..extra)
        .map(|_| {
            let url = url.clone();
            tokio::spawn(async move {
                let mut c = Conn::new(opts(&url)).await.expect("connect");
                let mut sql = String::with_capacity(16 * MIB + 32);
                sql.push_str("SELECT LENGTH('");
                sql.extend(std::iter::repeat_n('x', 16 * MIB - 64));
                sql.push_str("')");
                let mut took = Vec::new();
                for _ in 0..3 {
                    let s = Instant::now();
                    let n: Option<i64> = c.query_first(&sql).await.expect("a 16 MiB SELECT");
                    assert_eq!(n, Some((16 * MIB - 64) as i64));
                    took.push(s.elapsed());
                }
                c.disconnect().await.ok();
                took
            })
        })
        .collect();

    // A text statement of `big_mb` MiB less 1 KiB.
    let head = "INSERT INTO big VALUES (1, '";
    let tail = "')";
    let fill = big_mb * MIB - 1024 - head.len() - tail.len();
    let mut sql = String::with_capacity(big_mb * MIB);
    sql.push_str(head);
    sql.extend(std::iter::repeat_n('a', fill));
    sql.push_str(tail);
    let s = Instant::now();
    big.query_drop(&sql)
        .await
        .expect("the large text statement");
    let text_took = s.elapsed();
    drop(sql);

    // The row back: a `big_mb` MiB value.
    let s = Instant::now();
    println!("row read starts at +{:.2} s", t0.elapsed().as_secs_f64());
    let row: Option<(i64, Vec<u8>)> = big
        .query_first("SELECT id, b FROM big WHERE id = 1")
        .await
        .expect("the large row");
    let read_took = s.elapsed();
    let (id, b) = row.expect("the row");
    assert_eq!(id, 1);
    assert_eq!(b.len(), fill);
    drop(b);

    // A prepared statement with a `param_mb` MiB parameter.
    let param = vec![b'p'; param_mb * MIB];
    let s = Instant::now();
    println!(
        "parameter insert starts at +{:.2} s",
        t0.elapsed().as_secs_f64()
    );
    big.exec_drop("INSERT INTO big VALUES (?, ?)", (2_i64, param))
        .await
        .expect("the large parameter");
    let param_took = s.elapsed();
    let len: Option<i64> = big
        .query_first("SELECT LENGTH(b) FROM big WHERE id = 2")
        .await
        .unwrap();
    assert_eq!(len, Some((param_mb * MIB) as i64));

    for (i, e) in extras.into_iter().enumerate() {
        let took = e.await.expect("extra session");
        println!(
            "extra session {i}: three 16 MiB SELECTs took {:?}",
            took.iter()
                .map(|d| format!("{:.2} s", d.as_secs_f64()))
                .collect::<Vec<_>>()
        );
    }

    // A short tail so the samples cover the end.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let busy_end = t0.elapsed();
    stop.store(true, Ordering::Relaxed);
    let mut all = Vec::new();
    for l in loops {
        all.extend(l.await.expect("small loop"));
    }
    let mut on_server = Vec::new();
    for l in direct {
        on_server.extend(l.await.expect("direct loop"));
    }
    small_rt.shutdown_background();
    big.query_drop("DELETE FROM big").await.unwrap();
    big.disconnect().await.ok();

    // A new session still logs in: the gateway is alive.
    let mut fresh = Conn::new(opts(&url)).await.expect("the gateway is alive");
    let one: Option<i64> = fresh.query_first("SELECT 1").await.unwrap();
    assert_eq!(one, Some(1));
    fresh.disconnect().await.ok();

    println!(
        "large text statement ({big_mb} MiB): {:.2} s; its row read back: {:.2} s; \
         {param_mb} MiB parameter: {:.2} s",
        text_took.as_secs_f64(),
        read_took.as_secs_f64(),
        param_took.as_secs_f64()
    );
    let quiet = stats(
        all.iter()
            .filter(|(at, _)| *at < quiet_end)
            .map(|x| x.1)
            .collect(),
    );
    let busy = stats(
        all.iter()
            .filter(|(at, _)| *at >= quiet_end && *at < busy_end)
            .map(|x| x.1)
            .collect(),
    );
    let mut slow: Vec<_> = all.iter().filter(|(at, _)| *at >= quiet_end).collect();
    slow.sort_by_key(|x| std::cmp::Reverse(x.1));
    for (at, d) in slow.iter().take(5) {
        println!(
            "slow small statement at +{:.2} s: {:.0} ms",
            at.as_secs_f64(),
            d.as_secs_f64() * 1e3
        );
    }
    if !on_server.is_empty() {
        let mut slow: Vec<_> = on_server
            .iter()
            .filter(|(at, _)| *at >= quiet_end)
            .collect();
        slow.sort_by_key(|x| std::cmp::Reverse(x.1));
        for (at, d) in slow.iter().take(3) {
            println!(
                "slow direct statement at +{:.2} s: {:.0} ms",
                at.as_secs_f64(),
                d.as_secs_f64() * 1e3
            );
        }
        show(
            "the same statement straight on MariaDB during the large ones",
            &stats(
                on_server
                    .iter()
                    .filter(|(at, _)| *at >= quiet_end && *at < busy_end)
                    .map(|x| x.1)
                    .collect(),
            ),
        );
    }
    show("small statements before (8 sessions)", &quiet);
    show("small statements during the large ones", &busy);
    assert!(
        busy.max < bound,
        "a small statement took {:?} while the large ones ran (bound {bound:?})",
        busy.max
    );
}
