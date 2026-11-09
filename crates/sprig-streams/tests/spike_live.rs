//! Chapter 45 build step 1: the stream-log spike (`decisions/spike-stream-log.md`).
//!
//! Appends and range reads on the stream log's table, partitioned by `day`
//! as chapter 45 lays it out, on the dev replication cluster (primary with
//! two semi-synchronous replicas, binlog on), in the schema
//! `r9streams_spike`, which the run drops at the end.
//!
//! ```text
//! SPRIG_SPIKE_URL=mysql://root:sprig-dev-root@127.0.0.1:13311 \
//! SPRIG_SPIKE_REPLICA=mysql://root:sprig-dev-root@127.0.0.1:13312 \
//!   cargo test -p sprig-streams --test spike_live -- --ignored --nocapture
//! ```
#![allow(clippy::unwrap_used, clippy::cast_precision_loss, missing_docs)]

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts};

const SCHEMA: &str = "r9streams_spike";

async fn conn(url: &str) -> Conn {
    Conn::new(Opts::from_url(&format!("{url}/{SCHEMA}")).unwrap())
        .await
        .unwrap()
}

fn log_ddl(name: &str, partitioned: bool) -> String {
    let (pk, uk, parts) = if partitioned {
        (
            "PRIMARY KEY (partition_no, offset_no, day)",
            "UNIQUE KEY uk_source (partition_no, source_event, day)",
            " PARTITION BY RANGE COLUMNS (day) (\
             PARTITION p20261107 VALUES LESS THAN ('2026-11-08'), \
             PARTITION p20261108 VALUES LESS THAN ('2026-11-09'), \
             PARTITION p20261109 VALUES LESS THAN ('2026-11-10'), \
             PARTITION p20261110 VALUES LESS THAN ('2026-11-11'), \
             PARTITION pmax VALUES LESS THAN (MAXVALUE))",
        )
    } else {
        (
            "PRIMARY KEY (partition_no, offset_no)",
            "UNIQUE KEY uk_source (partition_no, source_event)",
            "",
        )
    };
    format!(
        "CREATE TABLE {name} (
  partition_no INT UNSIGNED NOT NULL, offset_no BIGINT UNSIGNED NOT NULL,
  day DATE NOT NULL, ts DATETIME(3) NOT NULL, k VARBINARY(4096) NULL, v LONGBLOB NULL,
  headers JSON NULL, source_event VARBINARY(64) NOT NULL, leader_epoch INT UNSIGNED NOT NULL,
  {pk}, {uk}, KEY k_compact (partition_no, k(255), offset_no)
) ENGINE=InnoDB{parts}"
    )
}

/// One sequencer-like batch: lock the checkpoint, insert `n` records of
/// partition `p` from `next`, move the checkpoint, commit.
async fn append(c: &mut Conn, table: &str, p: u32, next: u64, n: u64, day: &str) {
    let mut tx = c
        .start_transaction(mysql_async::TxOpts::default())
        .await
        .unwrap();
    let _: Option<(u64, u32)> = tx
        .exec_first(
            "SELECT next_offset, leader_epoch FROM ckpt WHERE stream_id = 1 AND partition_no = ? FOR UPDATE",
            (p,),
        )
        .await
        .unwrap();
    let mut sql = format!(
        "INSERT INTO {table} (partition_no, offset_no, day, ts, k, v, headers, source_event, leader_epoch) VALUES "
    );
    for i in 0..n {
        let o = next + i;
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!(
            "({p},{o},'{day}','{day} 10:00:00.000','order-{}',\
             '{{\"type\":\"order.paid\",\"order_id\":{o},\"amount\":49.90,\"customer\":\"c-{}\",\"items\":[1,2,3],\"note\":\"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\"}}',\
             '{{\"source\":\"checkout\"}}','e{p}-{o}',3)",
            o % 1000,
            o % 97
        ));
    }
    tx.query_drop(sql).await.unwrap();
    tx.exec_drop(
        "UPDATE ckpt SET next_offset = ?, resume_token = 'tok' WHERE stream_id = 1 AND partition_no = ?",
        (next + n, p),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

async fn reset(c: &mut Conn, table: &str, partitioned: bool, parts: u32) {
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .await
        .unwrap();
    c.query_drop(log_ddl(table, partitioned)).await.unwrap();
    c.query_drop("DELETE FROM ckpt").await.unwrap();
    for p in 0..parts {
        c.exec_drop(
            "INSERT INTO ckpt VALUES (1, ?, 0, 0, 1, '')",
            (p,),
        )
        .await
        .unwrap();
    }
}

/// `writers` tasks, one partition each, for `secs`; records per second.
async fn run_appends(url: &str, table: &str, writers: u32, batch: u64, secs: u64) -> (f64, u64) {
    let mut tasks = Vec::new();
    for p in 0..writers {
        let url = url.to_owned();
        let table = table.to_owned();
        tasks.push(tokio::spawn(async move {
            let mut c = conn(&url).await;
            let start = Instant::now();
            let mut next = 0;
            let mut worst = Duration::ZERO;
            while start.elapsed() < Duration::from_secs(secs) {
                let t = Instant::now();
                append(&mut c, &table, p, next, batch, "2026-11-09").await;
                worst = worst.max(t.elapsed());
                next += batch;
            }
            (next, start.elapsed(), worst)
        }));
    }
    let mut total = 0;
    let mut worst = Duration::ZERO;
    let mut elapsed = Duration::ZERO;
    for t in tasks {
        let (n, e, w) = t.await.unwrap();
        total += n;
        worst = worst.max(w);
        elapsed = elapsed.max(e);
    }
    (
        total as f64 / elapsed.as_secs_f64(),
        u64::try_from(worst.as_millis()).unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "spike: needs the dev replication cluster and a heavy-lock slot"]
#[allow(clippy::too_many_lines)]
async fn stream_log_spike() {
    let url = std::env::var("SPRIG_SPIKE_URL")
        .unwrap_or_else(|_| "mysql://root:sprig-dev-root@127.0.0.1:13311".into());
    let replica = std::env::var("SPRIG_SPIKE_REPLICA")
        .unwrap_or_else(|_| "mysql://root:sprig-dev-root@127.0.0.1:13312".into());
    let secs: u64 = std::env::var("SPRIG_SPIKE_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    {
        let mut c = Conn::new(Opts::from_url(&url).unwrap()).await.unwrap();
        c.query_drop(format!("DROP DATABASE IF EXISTS {SCHEMA}"))
            .await
            .unwrap();
        c.query_drop(format!("CREATE DATABASE {SCHEMA}"))
            .await
            .unwrap();
    }
    let mut c = conn(&url).await;
    c.query_drop(
        "CREATE TABLE ckpt (stream_id BIGINT UNSIGNED NOT NULL, partition_no INT UNSIGNED NOT NULL,
         next_offset BIGINT UNSIGNED NOT NULL, log_start BIGINT UNSIGNED NOT NULL,
         leader_epoch INT UNSIGNED NOT NULL, resume_token VARBINARY(512) NOT NULL,
         PRIMARY KEY (stream_id, partition_no)) ENGINE=InnoDB",
    )
    .await
    .unwrap();

    let phases = std::env::var("SPRIG_SPIKE_PHASES").unwrap_or_else(|_| "append,lag,read,drop".into());
    println!("== appends (records/s, worst batch ms), {secs} s each");
    for partitioned in [true, false].into_iter().filter(|_| phases.contains("append")) {
        for (writers, batch) in [(1, 1), (1, 10), (1, 100), (1, 500), (4, 100), (8, 100)] {
            reset(&mut c, "sl", partitioned, writers).await;
            let (rps, worst) = run_appends(&url, "sl", writers, batch, secs).await;
            println!(
                "partitioned={partitioned} writers={writers} batch={batch}: {rps:.0} records/s total, {:.0} per partition, worst {worst} ms",
                rps / f64::from(writers)
            );
        }
    }

    // Replica lag right after a burst.
    reset(&mut c, "sl", true, 1).await;
    let (rps, _) = run_appends(&url, "sl", 1, 500, secs).await;
    let (count, last): (u64, Option<u64>) = c
        .query_first("SELECT COUNT(*), MAX(offset_no) FROM sl")
        .await
        .unwrap()
        .unwrap();
    println!("burst wrote {count} records, last offset {last:?}");
    let last = last.unwrap_or(0);
    let mut r = Conn::new(Opts::from_url(&format!("{replica}/{SCHEMA}")).unwrap())
        .await
        .unwrap();
    let t = Instant::now();
    loop {
        let got: Option<u64> = r
            .query_first("SELECT MAX(offset_no) FROM sl")
            .await
            .unwrap_or(None);
        if got == Some(last) {
            break;
        }
        assert!(t.elapsed() < Duration::from_secs(120), "replica never caught up");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    println!(
        "replica caught up {} ms after a {rps:.0} records/s burst ended",
        t.elapsed().as_millis()
    );

    // Range reads across day partitions.
    reset(&mut c, "sl", true, 1).await;
    let per_day = 50_000u64;
    let mut next = 0;
    for day in ["2026-11-07", "2026-11-08", "2026-11-09"] {
        while next < per_day * (1 + ["2026-11-07", "2026-11-08", "2026-11-09"].iter().position(|d| *d == day).unwrap() as u64) {
            append(&mut c, "sl", 0, next, 1000, day).await;
            next += 1000;
        }
    }
    c.query_drop("ANALYZE TABLE sl").await.unwrap();
    let plan: Vec<mysql_async::Row> = c
        .query(
            "EXPLAIN PARTITIONS SELECT partition_no, offset_no, ts, k, v, headers FROM sl \
             WHERE partition_no = 0 AND offset_no >= 49900 ORDER BY offset_no LIMIT 500",
        )
        .await
        .unwrap();
    for row in &plan {
        let cols: Vec<String> = (0..row.len())
            .map(|i| {
                row.get_opt::<Option<String>, usize>(i)
                    .and_then(Result::ok)
                    .flatten()
                    .unwrap_or_else(|| "NULL".into())
            })
            .collect();
        println!("explain: {}", cols.join(" | "));
    }
    let t = Instant::now();
    let mut from = 0u64;
    let mut read = 0u64;
    let mut bytes = 0u64;
    loop {
        let rows: Vec<(u32, u64, String, Vec<u8>, Vec<u8>, Option<String>)> = c
            .exec(
                "SELECT partition_no, offset_no, CAST(ts AS CHAR), k, v, CAST(headers AS CHAR) FROM sl \
                 WHERE partition_no = 0 AND offset_no >= ? ORDER BY offset_no LIMIT 500",
                (from,),
            )
            .await
            .unwrap();
        if rows.is_empty() {
            break;
        }
        for r in &rows {
            assert_eq!(r.1, from, "offsets read in order without gaps");
            from += 1;
            bytes += (r.3.len() + r.4.len()) as u64;
        }
        read += rows.len() as u64;
    }
    let e = t.elapsed().as_secs_f64();
    println!(
        "range reads: {read} records over 3 day partitions in {e:.2} s: {:.0} records/s, {:.1} MB/s, batches of 500",
        read as f64 / e,
        bytes as f64 / e / 1e6
    );
    // A read in the middle of the oldest day: index dive cost.
    let t = Instant::now();
    for i in 0..200u64 {
        let _: Vec<(u64,)> = c
            .exec(
                "SELECT offset_no FROM sl WHERE partition_no = 0 AND offset_no >= ? ORDER BY offset_no LIMIT 100",
                ((i * 731) % next,),
            )
            .await
            .unwrap();
    }
    println!(
        "point fetch of 100 records at a random offset: {:.2} ms each",
        t.elapsed().as_secs_f64() * 1000.0 / 200.0
    );

    // DROP PARTITION under an append load.
    let url2 = url.clone();
    c.exec_drop("INSERT INTO ckpt VALUES (1, 1, 0, 0, 1, '') ON DUPLICATE KEY UPDATE next_offset = 0", ())
        .await
        .unwrap();
    let load = tokio::spawn(async move {
        let mut c = conn(&url2).await;
        let start = Instant::now();
        let mut n = 0;
        let mut worst = Duration::ZERO;
        while start.elapsed() < Duration::from_secs(6) {
            let t = Instant::now();
            append(&mut c, "sl", 1, n, 100, "2026-11-09").await;
            worst = worst.max(t.elapsed());
            n += 100;
        }
        (n, worst)
    });
    tokio::time::sleep(Duration::from_secs(2)).await;
    let t = Instant::now();
    c.query_drop("ALTER TABLE sl DROP PARTITION p20261107")
        .await
        .unwrap();
    let drop_ms = t.elapsed().as_millis();
    let t = Instant::now();
    c.query_drop(
        "ALTER TABLE sl REORGANIZE PARTITION pmax INTO (PARTITION p20261111 VALUES LESS THAN ('2026-11-12'), PARTITION pmax VALUES LESS THAN (MAXVALUE))",
    )
    .await
    .unwrap();
    let add_ms = t.elapsed().as_millis();
    let (n, worst) = load.await.unwrap();
    println!(
        "DROP PARTITION of a 50,000-record day under load: {drop_ms} ms; REORGANIZE pmax (empty) to add a day: {add_ms} ms; \
         the writer appended {n} records meanwhile, worst batch {} ms",
        worst.as_millis()
    );
    let start: Option<u64> = c
        .query_first("SELECT MIN(offset_no) FROM sl WHERE partition_no = 0")
        .await
        .unwrap();
    println!("log start of partition 0 after the drop: {start:?}");

    drop(c);
    let mut c = Conn::new(Opts::from_url(&url).unwrap()).await.unwrap();
    c.query_drop(format!("DROP DATABASE {SCHEMA}")).await.unwrap();
}
