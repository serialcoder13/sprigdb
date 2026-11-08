//! The latency gate of milestone M0.2 (chapter 48): "the gateway adds no
//! more than 0.5 ms at p50 inside a data center", against direct MariaDB
//! (chapter 46, "Performance").
//!
//! Measures three workloads on one connection per endpoint, sequentially,
//! interleaving target and baseline in batches so drift hits both alike:
//!
//! - `point_select`: a prepared `SELECT id, v FROM t WHERE id = ?`
//!   (`COM_STMT_EXECUTE`);
//! - `point_select_text`: the same as a plain query (`COM_QUERY`);
//! - `insert`: a prepared single-row autocommit `INSERT`.
//!
//! It prints p50, p99 and max per workload and endpoint and fails when a
//! workload's p50 delta (target minus baseline) exceeds
//! `SPRIG_TEST_LATENCY_MAX_P50_DELTA_MS` (0.5), or its p99 delta exceeds
//! `SPRIG_TEST_LATENCY_MAX_P99_DELTA_MS` when that is set. Ignored by
//! default; run it with the target (the gateway) and a baseline:
//!
//! ```sh
//! SPRIG_TEST_LATENCY_TARGET=mysql://root:sprig-dev-root@127.0.0.1:3307 \
//! SPRIG_TEST_LATENCY_BASELINE=mysql://root:sprig-dev-root@127.0.0.1:13301 \
//!   cargo test -p sprig-testkit --test latency_gate -- --ignored --nocapture
//! ```
//!
//! The baseline defaults to `SPRIG_TEST_MARIADB_SINGLE`. Tables go in the
//! URL's database, or `sprig_latency` (created if missing), and are dropped
//! afterwards. `SPRIG_TEST_LATENCY_SAMPLES` sets the samples per workload
//! and endpoint (2000). Pass `?require_ssl=true&verify_ca=false` in both URLs
//! to measure over TLS.

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Statement};
use sprig_testkit::connectors::latency::{Summary, delta_ms, ms, summarize};
use sprig_testkit::connectors::{
    LATENCY_BASELINE, LATENCY_MAX_P50_DELTA_MS, LATENCY_MAX_P99_DELTA_MS, LATENCY_SAMPLES,
    LATENCY_TARGET,
};
use sprig_testkit::{require_env, skip, unique_ident};

const ROWS: u32 = 1000;
const BATCH: usize = 100;

#[derive(Clone, Copy, Debug)]
enum Workload {
    PointSelect,
    PointSelectText,
    Insert,
}

impl Workload {
    const ALL: [Workload; 3] = [Self::PointSelect, Self::PointSelectText, Self::Insert];

    fn name(self) -> &'static str {
        match self {
            Self::PointSelect => "point_select",
            Self::PointSelectText => "point_select_text",
            Self::Insert => "insert",
        }
    }
}

struct Endpoint {
    label: &'static str,
    conn: Conn,
    points: String,
    inserts: String,
    select: Statement,
    insert: Statement,
}

impl Endpoint {
    async fn open(label: &'static str, url: &str, suffix: &str) -> Self {
        let opts = Opts::from_url(url).unwrap_or_else(|e| panic!("{label} URL: {e}"));
        let has_db = opts.db_name().is_some();
        let mut conn = Conn::new(OptsBuilder::from_opts(opts).prefer_socket(false))
            .await
            .unwrap_or_else(|e| panic!("connecting to the {label}: {e}"));
        if !has_db {
            conn.query_drop("CREATE DATABASE IF NOT EXISTS sprig_latency")
                .await
                .unwrap();
            conn.query_drop("USE sprig_latency").await.unwrap();
        }
        let points = format!("lat_pt_{suffix}");
        let inserts = format!("lat_ins_{suffix}");
        conn.query_drop(format!(
            "CREATE TABLE {points} (id INT PRIMARY KEY, v VARCHAR(32) NOT NULL)"
        ))
        .await
        .unwrap();
        let values: Vec<String> = (1..=ROWS).map(|i| format!("({i}, 'value-{i}')")).collect();
        conn.query_drop(format!("INSERT INTO {points} VALUES {}", values.join(", ")))
            .await
            .unwrap();
        conn.query_drop(format!(
            "CREATE TABLE {inserts} (id BIGINT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(32) NOT NULL)"
        ))
        .await
        .unwrap();
        let select = conn
            .prep(format!("SELECT id, v FROM {points} WHERE id = ?"))
            .await
            .unwrap();
        let insert = conn
            .prep(format!("INSERT INTO {inserts} (v) VALUES (?)"))
            .await
            .unwrap();
        Self {
            label,
            conn,
            points,
            inserts,
            select,
            insert,
        }
    }

    /// One operation, timed.
    async fn op(&mut self, w: Workload, i: u32) -> Duration {
        let key = i.wrapping_mul(7919) % ROWS + 1;
        let t0 = Instant::now();
        match w {
            Workload::PointSelect => {
                let row: Option<(u32, String)> =
                    self.conn.exec_first(&self.select, (key,)).await.unwrap();
                assert!(row.is_some(), "{}: no row {key}", self.label);
            }
            Workload::PointSelectText => {
                let row: Option<(u32, String)> = self
                    .conn
                    .query_first(format!(
                        "SELECT id, v FROM {} WHERE id = {key}",
                        self.points
                    ))
                    .await
                    .unwrap();
                assert!(row.is_some(), "{}: no row {key}", self.label);
            }
            Workload::Insert => {
                self.conn.exec_drop(&self.insert, ("x",)).await.unwrap();
            }
        }
        t0.elapsed()
    }

    async fn close(mut self) {
        let drop = format!("DROP TABLE IF EXISTS {}, {}", self.points, self.inserts);
        if let Err(e) = self.conn.query_drop(drop).await {
            eprintln!("{}: dropping the tables: {e}", self.label);
        }
        let _ = self.conn.disconnect().await;
    }
}

fn env_f64(name: &str) -> Option<f64> {
    skip::var(name).map(|v| v.parse().unwrap_or_else(|e| panic!("{name}={v}: {e}")))
}

#[tokio::test]
#[ignore = "measures a live endpoint; set SPRIG_TEST_LATENCY_TARGET"]
async fn gateway_adds_at_most_half_a_millisecond_at_p50() {
    let target_url = require_env!(LATENCY_TARGET);
    let baseline_url = skip::var(LATENCY_BASELINE)
        .or_else(|| skip::var(skip::MARIADB_SINGLE).map(|v| skip::urls(&v)[0].clone()));
    let Some(baseline_url) = baseline_url else {
        skip::report_missing(LATENCY_BASELINE, module_path!());
        return;
    };
    let samples: usize = skip::var(LATENCY_SAMPLES).map_or(2000, |v| v.parse().unwrap());
    let max_p50 = env_f64(LATENCY_MAX_P50_DELTA_MS).unwrap_or(0.5);
    let max_p99 = env_f64(LATENCY_MAX_P99_DELTA_MS);
    let rounds = samples.div_ceil(BATCH);

    let suffix = unique_ident("g");
    let mut eps = [
        // Distinct tables, so target and baseline may be the same server.
        Endpoint::open("target", &target_url, &format!("{suffix}_t")).await,
        Endpoint::open("baseline", &baseline_url, &format!("{suffix}_b")).await,
    ];

    let mut report = Vec::new();
    for w in Workload::ALL {
        // Warm up: statement caches, buffer pool, connection buffers.
        for ep in &mut eps {
            for i in 0..200 {
                ep.op(w, i).await;
            }
        }
        let mut times: [Vec<Duration>; 2] = [Vec::new(), Vec::new()];
        let mut i = 0u32;
        for round in 0..rounds {
            // Alternate which endpoint goes first.
            for k in [round % 2, 1 - round % 2] {
                for _ in 0..BATCH {
                    i += 1;
                    times[k].push(eps[k].op(w, i).await);
                }
            }
        }
        let [t, b] = times;
        let (mut t, mut b) = (t, b);
        report.push((w, summarize(&mut t), summarize(&mut b)));
    }
    for ep in eps {
        ep.close().await;
    }

    println!("latency gate: target {target_url} vs baseline {baseline_url}");
    println!(
        "{:<18} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
        "workload", "tgt p50", "base p50", "Δ p50", "tgt p99", "base p99", "Δ p99"
    );
    let mut failures = Vec::new();
    for (w, t, b) in &report {
        let (d50, d99) = (delta_ms(t.p50, b.p50), delta_ms(t.p99, b.p99));
        println!(
            "{:<18} {:>9.3} {:>9.3} {:>+9.3} {:>9.3} {:>9.3} {:>+9.3}   ms",
            w.name(),
            ms(t.p50),
            ms(b.p50),
            d50,
            ms(t.p99),
            ms(b.p99),
            d99
        );
        if d50 > max_p50 {
            failures.push(format!("{}: p50 +{d50:.3} ms > {max_p50} ms", w.name()));
        }
        if let Some(max) = max_p99
            && d99 > max
        {
            failures.push(format!("{}: p99 +{d99:.3} ms > {max} ms", w.name()));
        }
    }
    for (w, t, b) in &report {
        let (t, b): (&Summary, &Summary) = (t, b);
        println!("  {:<18} target   {t}\n  {:<18} baseline {b}", w.name(), "");
    }
    assert!(
        failures.is_empty(),
        "latency gate failed: {}",
        failures.join("; ")
    );
}
