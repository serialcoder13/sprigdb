//! Throughput of the hottest commands (chapter 40, "Latency budget"; R6
//! round 3, decision R23): `GET`, `SET`, `INCR`, `HSET`, `LPUSH`, `LPOP`
//! and `ZADD`, each from N clients sending one command at a time, against
//! a SprigDB gateway and a reference Redis.
//!
//! - `SPRIG_BENCH_TARGETS`: `name=url` pairs, comma-separated, such as
//!   `redis=redis://127.0.0.1:21381,sprig=redis://acme.app:pw@127.0.0.1:21380`.
//! - `SPRIG_BENCH_CLIENTS`: client counts, comma-separated (`8,64`).
//! - `SPRIG_BENCH_SECONDS`: seconds per command and count (5).
//! - `SPRIG_BENCH_COMMANDS`: which commands (all seven).
//!
//! Skipped when `SPRIG_BENCH_TARGETS` is unset. Each run writes keys under
//! `bench:` and deletes them at the end. Prints one line per target,
//! command and count: commands per second, median and 99th percentile.

#![allow(
    clippy::unwrap_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::doc_markdown
)]

use std::time::{Duration, Instant};

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

async fn conn(url: &str) -> redis::aio::MultiplexedConnection {
    redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

/// The command client `c` sends as its `i`th.
fn command(name: &str, c: usize, i: u64) -> redis::Cmd {
    let mut q = redis::cmd(name);
    match name {
        "GET" => {
            q.arg(format!("bench:s:{}", i % 1000));
        }
        "SET" => {
            q.arg(format!("bench:s:{}", (c as u64 * 7919 + i) % 1000))
                .arg("value-0123456789");
        }
        "INCR" => {
            q.arg(format!("bench:n:{}", (c as u64 * 7919 + i) % 1000));
        }
        "HSET" => {
            q.arg(format!("bench:h:{}", c % 100))
                .arg(format!("f{}", i % 50))
                .arg("v");
        }
        "LPUSH" => {
            q.arg(format!("bench:l:{c}")).arg("item-0123456789");
        }
        "LPOP" => {
            q.arg(format!("bench:l:{c}"));
        }
        _ => {
            q.arg(format!("bench:z:{}", c % 100))
                .arg(i % 1000)
                .arg(format!("m{}", i % 200));
        }
    }
    q
}

/// Runs `name` from `clients` clients for `secs`: (ops/s, p50 ms, p99 ms).
async fn measure(url: &str, name: &str, clients: usize, secs: u64) -> (f64, f64, f64) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    let start = Instant::now();
    let mut tasks = Vec::new();
    for c in 0..clients {
        let url = url.to_owned();
        let name = name.to_owned();
        tasks.push(tokio::spawn(async move {
            let mut k = conn(&url).await;
            let mut lat = Vec::new();
            let mut i = 0u64;
            while Instant::now() < deadline {
                let t = Instant::now();
                let r: redis::RedisResult<redis::Value> = command(&name, c, i).query_async(&mut k).await;
                if let Err(e) = r {
                    eprintln!("{name}: {e}");
                }
                lat.push(t.elapsed());
                i += 1;
            }
            lat
        }));
    }
    let mut all = Vec::new();
    for t in tasks {
        all.extend(t.await.unwrap());
    }
    let wall = start.elapsed().as_secs_f64();
    all.sort_unstable();
    let pick = |q: f64| {
        all.get(((all.len() as f64 - 1.0) * q) as usize)
            .map_or(0.0, |d| d.as_secs_f64() * 1000.0)
    };
    (all.len() as f64 / wall, pick(0.5), pick(0.99))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn throughput() {
    let Some(targets) = var("SPRIG_BENCH_TARGETS") else {
        eprintln!("SPRIG_BENCH_TARGETS not set: skipped");
        return;
    };
    let clients: Vec<usize> = var("SPRIG_BENCH_CLIENTS")
        .unwrap_or_else(|| "8,64".into())
        .split(',')
        .filter_map(|x| x.parse().ok())
        .collect();
    let secs: u64 = var("SPRIG_BENCH_SECONDS")
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let cmds = var("SPRIG_BENCH_COMMANDS")
        .unwrap_or_else(|| "SET,GET,INCR,HSET,LPUSH,LPOP,ZADD".into());
    for t in targets.split(',') {
        let Some((label, url)) = t.split_once('=') else {
            continue;
        };
        for n in &clients {
            for name in cmds.split(',') {
                let (ops, p50, p99) = measure(url, name, *n, secs).await;
                println!(
                    "bench {label} {name} clients={n} ops_per_s={ops:.0} p50_ms={p50:.2} p99_ms={p99:.2}"
                );
            }
        }
        let mut k = conn(url).await;
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg("bench:*")
            .query_async(&mut k)
            .await
            .unwrap_or_default();
        for chunk in keys.chunks(500) {
            let _: redis::RedisResult<i64> = redis::cmd("DEL").arg(chunk).query_async(&mut k).await;
        }
    }
}
