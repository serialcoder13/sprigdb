#![allow(clippy::unwrap_used, clippy::float_cmp)]

use super::*;

fn cid() -> ClusterId {
    ClusterId::new("mum1-c1").unwrap()
}

fn report(gw: &str, at_ms: u64, busy_us: u64, rows: u64) -> LoadReport {
    LoadReport {
        gateway: gw.to_owned(),
        at_ms,
        shards: vec![ShardLoad {
            shard: ShardKey::new(1, 1, 1),
            cluster: cid(),
            statements: 0,
            busy_us,
            rows_written: rows,
            rows_read: 0,
            connections: 3,
        }],
    }
}

#[test]
fn rates_sum_over_gateways_and_average_over_the_window() {
    let mut c = LoadCollector::new(Duration::from_secs(1800));
    let k = ShardKey::new(1, 1, 1);
    // Two gateways, each adding 100 ms of statement time per second.
    for i in 0..=360u64 {
        let t = i * 10_000;
        c.ingest(&report("gw-a", t, i * 1_000_000, i * 50));
        c.ingest(&report("gw-b", t, i * 1_000_000, i * 50));
    }
    let l = c.shard_load(&k);
    // 200 ms/s steady state, reached by the EWMA after 2 windows.
    assert!((l.busy_ms_per_sec - 200.0).abs() < 200.0 * 0.15, "{l:?}");
    assert!((l.write_rows_per_sec - 10.0).abs() < 10.0 * 0.15, "{l:?}");
    assert_eq!(l.connections, 6.0);
    assert_eq!(c.shards().len(), 1);
    assert_eq!(c.shards()[0].1, cid());
    // Coverage: both reported within two periods.
    assert_eq!(c.coverage(3_600_000, Duration::from_secs(10), 2), 1.0);
    assert_eq!(c.coverage(3_600_000, Duration::from_secs(10), 4), 0.5);
    assert_eq!(c.coverage(9_000_000, Duration::from_secs(10), 2), 0.0);
}

#[test]
fn a_restarted_gateway_gives_no_negative_rate() {
    let mut c = LoadCollector::new(Duration::from_secs(60));
    let k = ShardKey::new(1, 1, 1);
    c.ingest(&report("gw", 0, 5_000_000, 10));
    c.ingest(&report("gw", 10_000, 6_000_000, 20));
    let before = c.shard_load(&k).busy_ms_per_sec;
    assert!(before > 0.0);
    c.ingest(&report("gw", 20_000, 10, 0));
    assert!(c.shard_load(&k).busy_ms_per_sec < before);
    assert!(c.shard_load(&k).busy_ms_per_sec >= 0.0);
    c.forget_before(30_000);
    assert_eq!(c.shard_load(&k), LoadVector::default());
}

#[test]
fn accumulator_reports_cumulative_counters() {
    let a = LoadAccumulator::new();
    let k = ShardKey::new(1, 1, 1);
    a.record(k, &cid(), Duration::from_millis(3), 1, 0);
    a.record(k, &cid(), Duration::from_millis(2), 0, 7);
    a.connections(k, 4);
    let r = a.report("gw", 1);
    assert_eq!(r.shards.len(), 1);
    let s = &r.shards[0];
    assert_eq!((s.statements, s.busy_us, s.rows_written, s.rows_read, s.connections), (2, 5000, 1, 7, 4));
}

/// Live: `SPRIG_TEST_MARIADB_SINGLE`; creates and drops
/// `placement_scan_<pid>`.
#[tokio::test]
async fn live_storage_scan_reads_innodb_table_stats() {
    let Ok(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE") else {
        eprintln!("skipped: set SPRIG_TEST_MARIADB_SINGLE to run it");
        return;
    };
    let pool = mysql_async::Pool::new(mysql_async::Opts::from_url(&url).unwrap());
    let mut conn = pool.get_conn().await.unwrap();
    let schema = format!("placement_scan_{}", std::process::id());
    conn.query_drop(format!("DROP DATABASE IF EXISTS {schema}")).await.unwrap();
    conn.query_drop(format!("CREATE DATABASE {schema}")).await.unwrap();
    for t in ["a", "b"] {
        conn.query_drop(format!(
            "CREATE TABLE {schema}.{t} (id INT PRIMARY KEY, v VARCHAR(200), KEY kv (v)) ENGINE=InnoDB"
        ))
        .await
        .unwrap();
        conn.query_drop(format!(
            "INSERT INTO {schema}.{t} SELECT seq, REPEAT('x', 150) FROM seq_1_to_2000"
        ))
        .await
        .unwrap();
        conn.query_drop(format!("ANALYZE TABLE {schema}.{t}")).await.unwrap();
    }
    let page = page_size(&mut conn).await.unwrap();
    let s = scan_schema(&mut conn, &schema, page).await.unwrap();
    conn.query_drop(format!("DROP DATABASE {schema}")).await.unwrap();
    assert_eq!(s.tables, 2);
    // 4,000 rows of about 160 bytes, in two indexes each: well over 1 MB.
    assert!(s.bytes > 1_000_000, "{s:?}");
    assert_eq!(s.bytes % page, 0);
    let none = scan_schema(&mut conn, &schema, page).await.unwrap();
    assert_eq!(none, SchemaSize::default());
    drop(conn);
    pool.disconnect().await.unwrap();
}
