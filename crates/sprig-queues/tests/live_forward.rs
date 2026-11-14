//! Live: the feed-driven outbox forwarder (publishing inside a
//! database transaction) against a running deployment
//! (gateway with AMQP on, a worker, the controller).
//!
//! ```text
//! SPRIG_FWD_MYSQL=mysql://acme.app:sprig-dev-app@127.0.0.1:28306/mq \
//! SPRIG_FWD_AMQP=amqp://acme.app:sprig-dev-app@127.0.0.1:28682/%2f \
//! SPRIG_FWD_TXNS=2000 SPRIG_FWD_PUBLISHERS=16 SPRIG_FWD_LABEL=feed \
//! cargo test -p sprig-queues --test live_forward -- --nocapture
//! ```
//!
//! `SPRIG_FWD_PUBLISHERS` sessions run `SPRIG_FWD_TXNS` transactions in all:
//!
//! ```text
//! BEGIN
//! CALL sprig.mq_publish('', q, '<id>', NULL)  -- the outbox row: its id is
//!                                              -- taken now (insert order)
//! SELECT SLEEP(0..15 ms)
//! SELECT n FROM ctr WHERE k = 1 FOR UPDATE    -- commits serialize on this
//! UPDATE ctr SET n = n + 1 WHERE k = 1         -- row: n is the commit order
//! INSERT INTO ord (id, n) VALUES (<id>, n + 1)
//! COMMIT
//! ```
//!
//! An AMQP consumer acknowledges every message. The test checks that every
//! committed transaction's message (the ids in `ord`) arrived exactly once,
//! counts commit-order inversions (a message whose `n` is below one
//! received before it), and prints commit-to-delivery latency. With
//! `SPRIG_FWD_EXPECT_ORDER=1` an inversion fails the test. Publishers that
//! lose their connection (a `kill -9` of a gateway) reconnect; a
//! transaction whose `COMMIT` answer was lost counts by `ord`.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    missing_docs
)]

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use lapin::options::{BasicAckOptions, BasicConsumeOptions, BasicQosOptions, QueueDeclareOptions};
use lapin::types::FieldTable;
use lapin::{Connection, ConnectionProperties};
use mysql_async::prelude::Queryable;

fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

fn pct(v: &mut [f64], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(f64::total_cmp);
    let i = ((v.len() as f64 - 1.0) * p).round() as usize;
    v[i]
}

async fn conn(url: &str) -> mysql_async::Conn {
    for _ in 0..240 {
        if let Ok(c) = mysql_async::Conn::new(mysql_async::Opts::from_url(url).unwrap()).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("can't connect to {url}");
}

/// One publisher's transactions; `next` hands out ids; `publish` is the
/// publish statement with `{id}` for the message's body.
async fn publisher(
    url: String,
    publish: String,
    next: Arc<Mutex<u64>>,
    total: u64,
    commits: Arc<Mutex<HashMap<u64, i64>>>,
    seed: u64,
) {
    let mut c = conn(&url).await;
    let mut rng = seed;
    loop {
        let id = {
            let mut n = next.lock().unwrap();
            if *n >= total {
                return;
            }
            *n += 1;
            *n
        };
        // xorshift: the sleep that makes insert order differ from commit order.
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let sleep_ms = rng % 16;
        let r: std::result::Result<(), mysql_async::Error> = async {
            c.query_drop("BEGIN").await?;
            c.query_drop(publish.replace("{id}", &id.to_string()))
                .await?;
            c.query_drop(format!("SELECT SLEEP({})", sleep_ms as f64 / 1000.0))
                .await?;
            let n: Option<u64> = c
                .query_first("SELECT n FROM ctr WHERE k = 1 FOR UPDATE")
                .await?;
            c.query_drop("UPDATE ctr SET n = n + 1 WHERE k = 1").await?;
            c.query_drop(format!(
                "INSERT INTO ord (id, n) VALUES ({id}, {})",
                n.unwrap_or(0) + 1
            ))
            .await?;
            c.query_drop("COMMIT").await?;
            Ok(())
        }
        .await;
        match r {
            Ok(()) => {
                commits.lock().unwrap().insert(id, now_us());
            }
            Err(e) => {
                eprintln!("publisher {seed}: txn {id} failed ({e}); reconnecting");
                let _ = c.query_drop("ROLLBACK").await;
                c = conn(&url).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn transactional_publishes_arrive_once_in_commit_order() {
    let (Some(url), Some(amqp)) = (var("SPRIG_FWD_MYSQL"), var("SPRIG_FWD_AMQP")) else {
        return;
    };
    let label = var("SPRIG_FWD_LABEL").unwrap_or_else(|| "run".into());
    let q = format!("fwd_{label}_{}", now_us());
    let publish = format!("CALL sprig.mq_publish('', '{q}', '{{id}}', NULL)");
    run(&url, &amqp, &q, true, &publish, &label).await;
}

/// Cross-tenant: an ancestor's transactions publish into a
/// member's inherited queue through the ancestor's `__sprig_xo`, whose rows
/// the ancestor's forwarder moves into the member's `__sprig_qo`, whose
/// rows the member's forwarder enqueues. `SPRIG_FWD_XO_MYSQL` (the
/// ancestor, database `mq`), `SPRIG_FWD_XO_AMQP` (the member, vhost `mq`),
/// `SPRIG_FWD_XO_PUBLISH` (the publish with `{id}`), `SPRIG_FWD_XO_QUEUE`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cross_tenant_publishes_arrive_once_in_commit_order() {
    let (Some(url), Some(amqp), Some(publish), Some(q)) = (
        var("SPRIG_FWD_XO_MYSQL"),
        var("SPRIG_FWD_XO_AMQP"),
        var("SPRIG_FWD_XO_PUBLISH"),
        var("SPRIG_FWD_XO_QUEUE"),
    ) else {
        return;
    };
    let label = var("SPRIG_FWD_LABEL").unwrap_or_else(|| "xo".into());
    run(&url, &amqp, &q, false, &publish, &label).await;
}

async fn run(url: &str, amqp: &str, q: &str, declare: bool, publish: &str, label: &str) {
    let url = url.to_owned();
    let total: u64 = var("SPRIG_FWD_TXNS").map_or(1000, |v| v.parse().unwrap());
    let pubs: u64 = var("SPRIG_FWD_PUBLISHERS").map_or(16, |v| v.parse().unwrap());
    let expect_order = var("SPRIG_FWD_EXPECT_ORDER").is_some();
    let mut c = conn(&url).await;
    c.query_drop("CREATE TABLE IF NOT EXISTS ctr (k INT PRIMARY KEY, n BIGINT NOT NULL)")
        .await
        .unwrap();
    c.query_drop(
        "CREATE TABLE IF NOT EXISTS ord (id BIGINT PRIMARY KEY, n BIGINT NOT NULL, run VARCHAR(64) NULL)",
    )
    .await
    .unwrap();
    c.query_drop("DELETE FROM ord").await.unwrap();
    c.query_drop("REPLACE INTO ctr VALUES (1, 0)")
        .await
        .unwrap();
    // The consumer: reconnects when its gateway goes (a kill -9); a
    // message whose acknowledgement was lost comes back `redelivered`.
    let first_conn = Connection::connect(amqp, ConnectionProperties::default())
        .await
        .unwrap();
    first_conn
        .create_channel()
        .await
        .unwrap()
        .queue_declare(
            q.into(),
            QueueDeclareOptions {
                durable: true,
                passive: !declare,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        first_conn.close(0, "declared".into()),
    )
    .await;
    let received: Arc<Mutex<Vec<(u64, i64, bool)>>> = Arc::new(Mutex::new(Vec::new()));
    let rec = Arc::clone(&received);
    let (amqp_url, qname) = (amqp.to_owned(), q.to_owned());
    let consume = tokio::spawn(async move {
        loop {
            let Ok(ac) = Connection::connect(&amqp_url, ConnectionProperties::default()).await
            else {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            };
            let r: std::result::Result<(), lapin::Error> = async {
                let ch = ac.create_channel().await?;
                ch.basic_qos(500, BasicQosOptions::default()).await?;
                let mut consumer = ch
                    .basic_consume(
                        qname.as_str().into(),
                        "fwd-test".into(),
                        BasicConsumeOptions::default(),
                        FieldTable::default(),
                    )
                    .await?;
                while let Some(d) = consumer.next().await {
                    let d = d?;
                    let at = now_us();
                    let id: u64 = String::from_utf8_lossy(&d.data).trim().parse().unwrap_or(0);
                    rec.lock().unwrap().push((id, at, d.redelivered));
                    d.ack(BasicAckOptions::default()).await?;
                }
                Ok(())
            }
            .await;
            eprintln!("consumer: connection ended ({r:?}); reconnecting");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });
    let next = Arc::new(Mutex::new(0u64));
    let commits: Arc<Mutex<HashMap<u64, i64>>> = Arc::new(Mutex::new(HashMap::new()));
    let started = Instant::now();
    let mut tasks = Vec::new();
    for p in 0..pubs {
        tasks.push(tokio::spawn(publisher(
            url.clone(),
            publish.to_owned(),
            Arc::clone(&next),
            total,
            Arc::clone(&commits),
            0x9e37_79b9_7f4a_7c15 ^ (p + 1),
        )));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let publish_s = started.elapsed().as_secs_f64();
    // What committed: ord.
    let mut c = conn(&url).await;
    let ord: Vec<(u64, u64)> = c.query("SELECT id, n FROM ord").await.unwrap();
    let n_of: HashMap<u64, u64> = ord.iter().copied().collect();
    let want: HashSet<u64> = n_of.keys().copied().collect();
    // Wait for every committed message (fallback included), at most 60 s.
    let t0 = Instant::now();
    loop {
        let got: HashSet<u64> = received.lock().unwrap().iter().map(|r| r.0).collect();
        if t0.elapsed().as_secs().is_multiple_of(10) && t0.elapsed().subsec_millis() < 100 {
            eprintln!("waiting: {} of {} received", got.len(), want.len());
        }
        if want.is_subset(&got) || t0.elapsed() > Duration::from_secs(60) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // A moment more for late duplicates.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let rec3 = received.lock().unwrap().clone();
    let redelivered = rec3.iter().filter(|r| r.2).count();
    // A duplicate is a second fresh (not `redelivered`) delivery: a
    // message enqueued twice. Redeliveries after a lost acknowledgement
    // are the consumer's at-least-once, not the forwarder's.
    let mut fresh: HashMap<u64, u32> = HashMap::new();
    for (id, _, re) in &rec3 {
        if !*re {
            *fresh.entry(*id).or_default() += 1;
        }
    }
    let rec: Vec<(u64, i64)> = rec3.iter().map(|r| (r.0, r.1)).collect();
    let mut seen: HashMap<u64, u32> = HashMap::new();
    for (id, _) in &rec {
        *seen.entry(*id).or_default() += 1;
    }
    let dups: Vec<u64> = fresh
        .iter()
        .filter(|(_, n)| **n > 1)
        .map(|(i, _)| *i)
        .collect();
    let missing: Vec<u64> = want
        .iter()
        .filter(|i| !seen.contains_key(i))
        .copied()
        .collect();
    let extra: Vec<u64> = seen.keys().filter(|i| !want.contains(i)).copied().collect();
    // Commit-order inversions: a message whose n is below the highest n
    // received before it (first deliveries only).
    let mut inversions = 0u64;
    let mut max_n = 0u64;
    let mut first: HashSet<u64> = HashSet::new();
    for (id, _) in &rec {
        if !first.insert(*id) {
            continue;
        }
        let Some(n) = n_of.get(id) else { continue };
        if *n < max_n {
            inversions += 1;
        }
        max_n = max_n.max(*n);
    }
    let commits = commits.lock().unwrap().clone();
    let mut lat: Vec<f64> = rec
        .iter()
        .filter_map(|(id, at)| commits.get(id).map(|c| (*at - *c) as f64 / 1000.0))
        .collect();
    let (p50, p90, p99, max) = (
        pct(&mut lat, 0.5),
        pct(&mut lat, 0.9),
        pct(&mut lat, 0.99),
        pct(&mut lat, 1.0),
    );
    eprintln!(
        "[{label}] committed {} of {total} in {publish_s:.1} s with {pubs} publishers; received {} ({} distinct, {redelivered} redelivered); duplicates {}; missing {}; not committed but received {}; commit-order inversions {inversions}; commit→delivery ms p50 {p50:.1} p90 {p90:.1} p99 {p99:.1} max {max:.1}",
        want.len(),
        rec.len(),
        seen.len(),
        dups.len(),
        missing.len(),
        extra.len(),
    );
    consume.abort();
    assert!(dups.is_empty(), "duplicates: {dups:?}");
    assert!(missing.is_empty(), "missing: {missing:?}");
    assert!(extra.is_empty(), "received but never committed: {extra:?}");
    if expect_order {
        assert_eq!(inversions, 0, "commit order broken");
    }
}
