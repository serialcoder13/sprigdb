//! Stream behaviour on scratch servers: heartbeat timing (chapter 10's open
//! question: `@master_heartbeat_period` is in nanoseconds), optional
//! `COM_REGISTER_SLAVE`, silence detection, and a throughput measurement.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::similar_names
)]

mod common;

use std::time::{Duration, Instant};

use common::{drop_schema, gtid_pos, opts, schema_name, server_id, source, sql, urls};
use mysql_async::prelude::Queryable;
use sprig_binlog::{BinlogStream, Event, StreamOptions};
use sprig_common::SprigCode;

/// With a 300 ms period on an idle server, heartbeats arrive about every
/// 300 ms: the period is taken in nanoseconds.
#[tokio::test]
async fn heartbeats_arrive_at_the_requested_period() {
    let Some(us) = urls(common::SCRATCH, module_path!()) else {
        return;
    };
    for url in us {
        let mut c = sql(&url).await;
        let now = gtid_pos(&mut c).await;
        let mut o = StreamOptions::new(server_id());
        o.heartbeat = Duration::from_millis(300);
        let mut s = BinlogStream::connect(&source(&url), &now, &o)
            .await
            .unwrap();
        let mut beats = Vec::new();
        let start = Instant::now();
        while beats.len() < 4 && start.elapsed() < Duration::from_secs(5) {
            if matches!(s.next().await.unwrap(), Event::Heartbeat) {
                beats.push(Instant::now());
            }
        }
        assert_eq!(beats.len(), 4, "{url}: heartbeats");
        for w in beats.windows(2) {
            let gap = w[1] - w[0];
            assert!(
                gap >= Duration::from_millis(200) && gap <= Duration::from_millis(600),
                "{url}: heartbeat gap {gap:?}"
            );
        }
    }
}

/// `COM_REGISTER_SLAVE` is optional: a registered stream shows in `SHOW
/// SLAVE HOSTS`; an unregistered one works without it (every other test).
#[tokio::test]
async fn registration_is_optional_and_visible() {
    let Some(us) = urls(common::SCRATCH, module_path!()) else {
        return;
    };
    for url in us {
        let mut c = sql(&url).await;
        let now = gtid_pos(&mut c).await;
        let mut o = StreamOptions::new(server_id());
        o.register_slave = true;
        let id = o.server_id;
        let _s = BinlogStream::connect(&source(&url), &now, &o)
            .await
            .unwrap();
        let hosts: Vec<mysql_async::Row> = c.query("SHOW SLAVE HOSTS").await.unwrap();
        let ids: Vec<u32> = hosts
            .iter()
            .filter_map(|r| r.get::<u32, _>("Server_id"))
            .collect();
        assert!(ids.contains(&id), "{url}: {ids:?} lacks {id}");
    }
}

/// `SPRIG_TEST_BINLOG_PAUSE=<container>=<url>`: pauses that throwaway
/// container while a stream with 200 ms heartbeats reads it; the stream
/// ends with the retryable `ER_SPRIG_BINLOG_DISCONNECTED` after about three
/// periods.
#[tokio::test]
async fn silence_ends_the_stream() {
    let Some(spec) = std::env::var("SPRIG_TEST_BINLOG_PAUSE").ok() else {
        let _ = urls("SPRIG_TEST_BINLOG_PAUSE", module_path!());
        return;
    };
    let (container, url) = spec.split_once('=').unwrap();
    let mut c = sql(url).await;
    let now = gtid_pos(&mut c).await;
    let mut o = StreamOptions::new(server_id());
    o.heartbeat = Duration::from_millis(200);
    let mut s = BinlogStream::connect(&source(url), &now, &o).await.unwrap();
    let docker = |cmd: &str| {
        let ok = std::process::Command::new("docker")
            .args([cmd, container])
            .stdout(std::process::Stdio::null())
            .status()
            .unwrap()
            .success();
        assert!(ok, "docker {cmd} {container}");
    };
    docker("pause");
    let start = Instant::now();
    let err = loop {
        match s.next().await {
            Ok(_) => {}
            Err(e) => break e,
        }
    };
    let waited = start.elapsed();
    docker("unpause");
    assert_eq!(err.code(), Some(SprigCode::BinlogDisconnected), "{err}");
    assert!(err.retryable);
    assert!(waited < Duration::from_secs(3), "{waited:?}");
    eprintln!("silence detected after {waited:?}");
}

/// Throughput: 200,000 sysbench-shaped rows, read back through the stream.
/// Run with `--ignored --nocapture`, in release mode, for numbers.
#[tokio::test]
#[ignore = "measurement, not a check"]
async fn throughput() {
    let Some(us) = urls(common::SCRATCH, module_path!()) else {
        return;
    };
    for url in us {
        let schema = schema_name();
        let mut c = sql(&url).await;
        c.query_drop(format!("CREATE DATABASE {schema}"))
            .await
            .unwrap();
        c.query_drop(format!(
            "CREATE TABLE {schema}.sbtest (id INT PRIMARY KEY, k INT, c CHAR(120), pad CHAR(60))"
        ))
        .await
        .unwrap();
        let from = gtid_pos(&mut c).await;
        for batch in 0..200 {
            let vals: Vec<String> = (0..1000)
                .map(|i| {
                    let id = batch * 1000 + i;
                    format!(
                        "({id}, {}, REPEAT('c', 119), REPEAT('p', 59))",
                        id * 7 % 1000
                    )
                })
                .collect();
            c.query_drop(format!(
                "INSERT INTO {schema}.sbtest VALUES {}",
                vals.join(",")
            ))
            .await
            .unwrap();
        }
        let to = gtid_pos(&mut c).await;
        let start = Instant::now();
        let mut s = BinlogStream::connect(&source(&url), &from, &opts(&schema))
            .await
            .unwrap();
        let mut rows = 0usize;
        while !s.position().reached(&to) {
            if let Event::Rows { rows: r, .. } = s.next().await.unwrap() {
                rows += r.len();
            }
        }
        let secs = start.elapsed().as_secs_f64();
        assert_eq!(rows, 200_000);
        #[allow(clippy::cast_precision_loss)]
        let rate = rows as f64 / secs;
        eprintln!("{url}: {rows} rows in {secs:.2} s = {rate:.0} rows/s");
        drop_schema(&url, &schema).await;
    }
}
