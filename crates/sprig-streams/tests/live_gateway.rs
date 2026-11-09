//! Chapter 45's core live, through a gateway (R9): streams created with
//! SQL, publishing inside transactions (some rolled back), a change topic
//! on the same table, records read back in order with `SELECT ... FROM
//! <stream>`, while a script moves the tenant between clusters and kills
//! the worker running the sequencer (`run.sh` of r9streams' notes drives
//! both). And the throughput of one partition and of several.
//!
//! ```text
//! SPRIG_LIVE_GW=mysql://acme.app:sprig-dev-app@127.0.0.1:38306 \
//!   cargo test -p sprig-streams --test live_gateway -- --ignored --nocapture
//! ```
#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::type_complexity,
    missing_docs
)]

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts};

const DB: &str = "r9streams_shop";

fn url() -> String {
    std::env::var("SPRIG_LIVE_GW")
        .unwrap_or_else(|_| "mysql://acme.app:sprig-dev-app@127.0.0.1:38306".into())
}

async fn conn(db: Option<&str>) -> Conn {
    let u = match db {
        Some(d) => format!("{}/{d}", url()),
        None => url(),
    };
    for _ in 0..60 {
        if let Ok(c) = Conn::new(Opts::from_url(&u).unwrap()).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("gateway unreachable");
}

fn env_u64(k: &str, d: u64) -> u64 {
    std::env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(d)
}

/// What one writer did.
#[derive(Debug, Default)]
struct Wrote {
    /// Keys whose create (order row + event) committed.
    created: BTreeSet<u64>,
    /// Keys whose create's commit answer was lost (decided later from the
    /// orders table).
    ambiguous: BTreeSet<u64>,
    /// Keys whose pay (update + event) committed.
    paid: BTreeSet<u64>,
    paid_ambiguous: BTreeSet<u64>,
    /// Keys only ever written in rolled-back transactions.
    rolled_back: BTreeSet<u64>,
    errors: u64,
}

async fn txn(c: &mut Conn, stmts: &[String], commit: bool) -> Result<(), (bool, String)> {
    c.query_drop("START TRANSACTION")
        .await
        .map_err(|e| (false, e.to_string()))?;
    for s in stmts {
        if let Err(e) = c.query_drop(s.as_str()).await {
            let _ = c.query_drop("ROLLBACK").await;
            return Err((false, e.to_string()));
        }
    }
    let end = if commit { "COMMIT" } else { "ROLLBACK" };
    c.query_drop(end).await.map_err(|e| (commit, e.to_string()))
}

async fn writer(w: u64, n: u64) -> Wrote {
    let mut c = conn(Some(DB)).await;
    let mut out = Wrote::default();
    for i in 0..n {
        let key = w * 1_000_000 + i;
        let rb = i % 5 == 4;
        let create = vec![
            format!("INSERT INTO orders (order_id, status, amount) VALUES ({key}, 'open', 49.90)"),
            format!(
                "INSERT INTO order_events (key, value, headers) VALUES ({key}, \
                 JSON_OBJECT('type','order.created','order_id',{key},'rb',{rb}), \
                 JSON_OBJECT('source','live','w',{w}))"
            ),
        ];
        match txn(&mut c, &create, !rb).await {
            Ok(()) if rb => {
                out.rolled_back.insert(key);
                continue;
            }
            Ok(()) => {
                out.created.insert(key);
            }
            Err((ambiguous, e)) => {
                out.errors += 1;
                if ambiguous {
                    out.ambiguous.insert(key);
                } else if rb {
                    out.rolled_back.insert(key);
                }
                eprintln!("writer {w} key {key} create: {e}");
                c = conn(Some(DB)).await;
                continue;
            }
        }
        let pay = vec![
            format!("UPDATE orders SET status = 'paid' WHERE order_id = {key}"),
            format!(
                "INSERT INTO order_events (key, value) VALUES ({key}, \
                 JSON_OBJECT('type','order.paid','order_id',{key}))"
            ),
        ];
        match txn(&mut c, &pay, true).await {
            Ok(()) => {
                out.paid.insert(key);
            }
            Err((ambiguous, e)) => {
                out.errors += 1;
                if ambiguous {
                    out.paid_ambiguous.insert(key);
                }
                eprintln!("writer {w} key {key} pay: {e}");
                c = conn(Some(DB)).await;
            }
        }
    }
    out
}

/// Every record of a stream, by partition, in offset order:
/// (offset, key, value).
async fn read_all(c: &mut Conn, stream: &str) -> BTreeMap<u32, Vec<(u64, String, String)>> {
    let mut out: BTreeMap<u32, Vec<(u64, String, String)>> = BTreeMap::new();
    let parts: Vec<u32> = c
        .query(format!("SELECT DISTINCT partition_no FROM `{stream}`"))
        .await
        .unwrap();
    for p in parts {
        let mut from = 0u64;
        loop {
            let rows: Vec<(u64, Option<String>, Option<String>)> = c
                .query(format!(
                    "SELECT offset_no, CAST(key AS CHAR), CAST(value AS CHAR) FROM `{stream}` \
                     WHERE partition_no = {p} AND offset_no >= {from} ORDER BY offset_no LIMIT 2000"
                ))
                .await
                .unwrap();
            if rows.is_empty() {
                break;
            }
            from = rows.last().unwrap().0 + 1;
            out.entry(p).or_default().extend(
                rows.into_iter()
                    .map(|(o, k, v)| (o, k.unwrap_or_default(), v.unwrap_or_default())),
            );
        }
    }
    out
}

async fn count(c: &mut Conn, stream: &str) -> u64 {
    c.query_first(format!("SELECT COUNT(*) FROM `{stream}`"))
        .await
        .unwrap()
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "live: needs r9streams' private deployment"]
async fn publish_change_topic_and_read_back_across_a_move() {
    let writers = env_u64("SPRIG_LIVE_WRITERS", 4);
    let per = env_u64("SPRIG_LIVE_PER_WRITER", 400);
    {
        let mut c = conn(None).await;
        c.query_drop(format!("CREATE DATABASE IF NOT EXISTS {DB}"))
            .await
            .unwrap();
        c.query_drop(format!("USE {DB}")).await.unwrap();
        let _ = c.query_drop("DROP STREAM IF EXISTS order_events").await;
        let _ = c
            .query_drop("ALTER TABLE orders SPRIG SET (stream.change_topic = off)")
            .await;
        c.query_drop("DROP TABLE IF EXISTS orders").await.unwrap();
        c.query_drop(
            "CREATE TABLE orders (order_id BIGINT PRIMARY KEY, status VARCHAR(16) NOT NULL, \
             amount DECIMAL(10,2) NOT NULL)",
        )
        .await
        .unwrap();
        c.query_drop(
            "CREATE STREAM order_events KEY (order_id) ATTACHED TO GROUP OF orders \
             PARTITIONS 4 RETENTION '7 days'",
        )
        .await
        .unwrap();
        c.query_drop("ALTER TABLE orders SPRIG SET (stream.change_topic = on)")
            .await
            .unwrap();
        let shown: Vec<(String, String, String)> = c
            .query("SHOW STREAMS")
            .await
            .unwrap()
            .into_iter()
            .map(|r: mysql_async::Row| {
                (
                    r.get::<String, _>(0).unwrap(),
                    r.get::<String, _>(1).unwrap(),
                    r.get::<String, _>(2).unwrap(),
                )
            })
            .collect();
        println!("streams: {shown:?}");
        // Refusals.
        for (sql, code) in [
            ("UPDATE order_events SET value = 1", 39941),
            ("DELETE FROM order_events", 39941),
            (
                "INSERT INTO `orders.changes` (key, value) VALUES (1, 2)",
                39941,
            ),
            ("ALTER STREAM order_events PARTITIONS 2", 39943),
            ("CREATE STREAM order_events", 39942),
            ("DROP STREAM nope", 39940),
        ] {
            match c.query_drop(sql).await {
                Err(mysql_async::Error::Server(e)) => {
                    assert_eq!(e.code, code, "{sql}: {}", e.message);
                }
                other => panic!("{sql}: {other:?}"),
            }
        }
    }
    println!("writers: {writers} x {per} orders (every 5th rolled back)");
    let t = Instant::now();
    let tasks: Vec<_> = (1..=writers)
        .map(|w| tokio::spawn(writer(w, per)))
        .collect();
    let mut all = Wrote::default();
    for t in tasks {
        let w = t.await.unwrap();
        all.created.extend(w.created);
        all.ambiguous.extend(w.ambiguous);
        all.paid.extend(w.paid);
        all.paid_ambiguous.extend(w.paid_ambiguous);
        all.rolled_back.extend(w.rolled_back);
        all.errors += w.errors;
    }
    let wrote_s = t.elapsed().as_secs_f64();
    let mut c = conn(Some(DB)).await;
    // The truth: committed orders.
    let orders: Vec<(u64, String)> = c
        .query("SELECT order_id, status FROM orders")
        .await
        .unwrap();
    let orders: BTreeMap<u64, String> = orders.into_iter().collect();
    for k in &all.created {
        assert!(orders.contains_key(k), "acknowledged order {k} is missing");
    }
    for k in &all.rolled_back {
        assert!(!orders.contains_key(k), "rolled-back order {k} exists");
    }
    let expected_events: u64 = orders
        .values()
        .map(|s| if s == "paid" { 2 } else { 1 })
        .sum();
    println!(
        "wrote {} orders ({} paid, {} rolled back, {} errors, {} ambiguous) in {wrote_s:.1} s; expecting {expected_events} events",
        orders.len(),
        orders.values().filter(|s| *s == "paid").count(),
        all.rolled_back.len(),
        all.errors,
        all.ambiguous.len() + all.paid_ambiguous.len()
    );
    // Wait for the sequencers.
    let t = Instant::now();
    loop {
        let n = count(&mut c, "order_events").await;
        let m = count(&mut c, "orders.changes").await;
        if n >= expected_events && m >= expected_events {
            break;
        }
        assert!(
            t.elapsed() < Duration::from_secs(300),
            "sequencing stalled at {n} events, {m} changes of {expected_events}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    println!(
        "sequencing caught up {:.1} s after the writers ended",
        t.elapsed().as_secs_f64()
    );
    tokio::time::sleep(Duration::from_secs(3)).await;
    // Event stream: per key, created then paid, on one partition; no
    // rolled-back event; offsets without gaps; nothing twice.
    let events = read_all(&mut c, "order_events").await;
    let mut per_key: BTreeMap<u64, Vec<(u32, String)>> = BTreeMap::new();
    let mut total = 0u64;
    for (p, recs) in &events {
        let offsets: Vec<u64> = recs.iter().map(|r| r.0).collect();
        assert_eq!(
            offsets,
            (0..recs.len() as u64).collect::<Vec<_>>(),
            "partition {p}"
        );
        for (_, k, v) in recs {
            let j: serde_json::Value = serde_json::from_str(v).unwrap();
            assert_ne!(
                j["rb"],
                serde_json::json!(true),
                "a rolled-back event was delivered: {v}"
            );
            per_key
                .entry(k.parse().unwrap())
                .or_default()
                .push((*p, j["type"].as_str().unwrap().to_owned()));
            total += 1;
        }
    }
    assert_eq!(total, expected_events, "events in the log");
    for (k, status) in &orders {
        let evs = per_key
            .get(k)
            .unwrap_or_else(|| panic!("order {k} has no event"));
        let types: Vec<&str> = evs.iter().map(|e| e.1.as_str()).collect();
        let want: &[&str] = if status == "paid" {
            &["order.created", "order.paid"]
        } else {
            &["order.created"]
        };
        assert_eq!(types, want, "order {k}");
        assert!(
            evs.iter().all(|e| e.0 == evs[0].0),
            "order {k} on one partition"
        );
    }
    assert_eq!(per_key.len(), orders.len(), "no event without its order");
    // Change topic: per key c then u, with before and after.
    let changes = read_all(&mut c, "orders.changes").await;
    let mut ops: BTreeMap<u64, Vec<String>> = BTreeMap::new();
    let mut ctotal = 0u64;
    for (p, recs) in &changes {
        let offsets: Vec<u64> = recs.iter().map(|r| r.0).collect();
        assert_eq!(
            offsets,
            (0..recs.len() as u64).collect::<Vec<_>>(),
            "changes partition {p}"
        );
        for (_, _, v) in recs {
            let j: serde_json::Value = serde_json::from_str(v).unwrap();
            let op = j["op"].as_str().unwrap().to_owned();
            if op == "ddl" || op == "t" {
                continue;
            }
            let row = if op == "d" { &j["before"] } else { &j["after"] };
            let k = row["order_id"].as_u64().unwrap();
            if op == "u" {
                assert_eq!(j["before"]["status"], "open");
                assert_eq!(j["after"]["status"], "paid");
            }
            assert_eq!(j["source"]["object"], "orders");
            ops.entry(k).or_default().push(op);
            ctotal += 1;
        }
    }
    assert_eq!(ctotal, expected_events, "changes in the topic");
    for (k, status) in &orders {
        let want: Vec<&str> = if status == "paid" {
            vec!["c", "u"]
        } else {
            vec!["c"]
        };
        assert_eq!(
            ops.get(k)
                .map(|v| v.iter().map(String::as_str).collect::<Vec<_>>()),
            Some(want),
            "order {k}"
        );
    }
    let leader_epochs: Vec<(u32, u64)> = c
        .query("SELECT partition_no, COUNT(*) FROM order_events GROUP BY partition_no")
        .await
        .unwrap();
    println!(
        "checked: {total} events and {ctotal} changes, every order's in commit order, none rolled back; per partition {leader_epochs:?}"
    );
}

/// Records per second through one partition and through several: bulk
/// publishes (500 records per statement, autocommit), then the time until
/// the log holds them all.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "live: needs r9streams' private deployment"]
async fn throughput_per_partition() {
    let records = env_u64("SPRIG_LIVE_RECORDS", 50_000);
    let mut c = conn(None).await;
    c.query_drop(format!("CREATE DATABASE IF NOT EXISTS {DB}"))
        .await
        .unwrap();
    c.query_drop(format!("USE {DB}")).await.unwrap();
    for (name, parts, keyed) in [("tp_one", 1u32, false), ("tp_four", 4, true)] {
        let _ = c.query_drop(format!("DROP STREAM IF EXISTS {name}")).await;
        c.query_drop(format!("CREATE STREAM {name} PARTITIONS {parts}"))
            .await
            .unwrap();
        // Let the controller start the sequencer.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let value = format!(
            "'{{\"type\":\"order.paid\",\"amount\":49.90,\"note\":\"{}\"}}'",
            "x".repeat(150)
        );
        let t = Instant::now();
        let writers = if keyed { 4 } else { 1 };
        let per = records / writers;
        let tasks: Vec<_> = (0..writers)
            .map(|w| {
                let value = value.clone();
                tokio::spawn(async move {
                    let mut c = conn(Some(DB)).await;
                    let mut i = 0;
                    while i < per {
                        let rows: Vec<String> = (i..(i + 500).min(per))
                            .map(|j| format!("('{w}-{j}', {value})"))
                            .collect();
                        c.query_drop(format!(
                            "INSERT INTO {name} (key, value) VALUES {}",
                            rows.join(",")
                        ))
                        .await
                        .unwrap();
                        i += 500;
                    }
                })
            })
            .collect();
        for t in tasks {
            t.await.unwrap();
        }
        let published = t.elapsed().as_secs_f64();
        let total = per * writers;
        loop {
            let n = count(&mut c, name).await;
            if n >= total {
                break;
            }
            assert!(t.elapsed() < Duration::from_secs(600), "stalled at {n}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let all = t.elapsed().as_secs_f64();
        let per_part: Vec<(u32, u64)> = c
            .query(format!(
                "SELECT partition_no, COUNT(*) FROM {name} GROUP BY partition_no"
            ))
            .await
            .unwrap();
        println!(
            "{name}: {total} records, {parts} partitions; published in {published:.1} s ({:.0}/s), \
             all sequenced after {all:.1} s: {:.0} records/s, {:.0} per partition; {per_part:?}",
            total as f64 / published,
            total as f64 / all,
            total as f64 / all / f64::from(parts)
        );
        // Latency: one record, commit to readable.
        let mut lat = Vec::new();
        for i in 0..20 {
            let before = count(&mut c, name).await;
            let t = Instant::now();
            c.query_drop(format!(
                "INSERT INTO {name} (key, value) VALUES ('lat-{i}', 'x')"
            ))
            .await
            .unwrap();
            while count(&mut c, name).await <= before {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            lat.push(t.elapsed().as_millis());
        }
        lat.sort_unstable();
        println!(
            "{name}: publish to readable, 20 records: median {} ms, max {} ms",
            lat[10], lat[19]
        );
        c.query_drop(format!("DROP STREAM {name}")).await.unwrap();
    }
}
