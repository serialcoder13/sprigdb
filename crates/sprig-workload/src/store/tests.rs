//! The store against the Compose MariaDB (11.8).
//!
//! `SPRIG_TEST_MARIADB_SINGLE` (for example
//! `mysql://root:sprig-dev-root@127.0.0.1:13301`) names the server; without
//! it the live cases are skipped with a message. Each case works in its own
//! schema `__sprig_test_workload_<pid>_<n>`, dropped afterwards.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts};
use ring::rand::SystemRandom;
use sprig_common::ids::{DbId, TenantId};
use sprig_common::names::quote;
use sprig_proto_mysql::packets::Persona;

use super::*;
use crate::fingerprint::{SqlCtx, SqlFingerprinter};
use crate::keyring::MasterKeyring;
use crate::recorder::{ExecSummary, FlushRow, Recorder, RecorderConfig};
use crate::samples::{CaptureMeta, SampleContext, SamplePayload, seal};
use crate::shape::NoResolver;
use crate::window::SampledLiterals;

const MARIADB_ENV: &str = "SPRIG_TEST_MARIADB_SINGLE";

async fn with_schema<F, Fut>(case: &str, body: F)
where
    F: FnOnce(Conn, String) -> Fut,
    Fut: Future<Output = ()>,
{
    static N: AtomicU64 = AtomicU64::new(0);
    let Ok(url) = std::env::var(MARIADB_ENV) else {
        eprintln!("skipping {case}: {MARIADB_ENV} is not set");
        return;
    };
    let schema = format!(
        "__sprig_test_workload_{}_{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    );
    let opts = Opts::from_url(&url).unwrap();
    let mut admin = Conn::new(opts.clone()).await.unwrap();
    admin
        .query_drop(format!("CREATE DATABASE {}", quote(&schema)))
        .await
        .unwrap();
    let mut conn = Conn::new(opts).await.unwrap();
    conn.query_drop(format!("USE {}", quote(&schema)))
        .await
        .unwrap();
    crate::flush::migrate(&mut conn, &schema, "test")
        .await
        .unwrap();
    body(conn, schema.clone()).await;
    admin
        .query_drop(format!("DROP DATABASE {}", quote(&schema)))
        .await
        .unwrap();
}

fn hour() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    now - now % 3600
}

fn row(fp: FingerprintId, bucket: u64, part: u16, runs: u64, us: u64) -> FlushRow {
    let mut stats = Stats::default();
    for _ in 0..runs {
        stats.exec_count += 1;
        stats.total_us += us;
        stats.max_us = stats.max_us.max(us);
        stats.latency_hist.record(us);
        stats.rows_returned += 2;
    }
    stats.examined_samples = 1;
    stats.rows_examined = 5000;
    stats.returned_sampled = 2;
    stats.scan_runs = 1;
    stats.scan_sessions.insert(bucket ^ u64::from(part));
    FlushRow {
        tenant: TenantId(1),
        fingerprint: fp,
        bucket_start: bucket,
        part,
        stats,
        last_seen: UNIX_EPOCH + Duration::from_secs(bucket + 10),
    }
}

fn new_fp(sql: &str) -> (crate::fingerprint::Fingerprint, NewFingerprint) {
    let f = SqlFingerprinter::default();
    let ctx = SqlCtx::new(Persona::MariaDb).current_db("shop", DbId(1));
    let fp = f.sql(TenantId(1), sql.as_bytes(), &ctx, &NoResolver);
    let clock = std::sync::Arc::new(sprig_common::time::SystemClock);
    let r = Recorder::new(RecorderConfig::default(), clock);
    r.record(TenantId(1), &fp, &ExecSummary::default());
    let n = r.drain_new().pop().unwrap();
    (fp, n)
}

async fn span(t: &WorkloadTables, c: &mut Conn, from: u64, to: u64) -> Vec<FpWindowStats> {
    t.window(
        c,
        TenantId(1),
        UNIX_EPOCH + Duration::from_secs(from),
        UNIX_EPOCH + Duration::from_secs(to),
    )
    .await
    .unwrap()
}

async fn count(conn: &mut Conn, schema: &str, table: &str) -> u64 {
    conn.query_first(format!("SELECT COUNT(*) FROM {}.{table}", quote(schema)))
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn flushes_are_idempotent_and_windows_merge_gateways_and_buckets() {
    with_schema("window", |mut conn, schema| async move {
        let t = WorkloadTables::new(&schema);
        let (fp, n) = new_fp("SELECT * FROM orders WHERE customer_id = 42");
        assert_eq!(
            t.insert_fingerprints(&mut conn, std::slice::from_ref(&n))
                .await
                .unwrap(),
            1
        );
        assert_eq!(t.insert_fingerprints(&mut conn, &[n]).await.unwrap(), 0);
        let h = hour();
        // Two gateways in this hour, one in the last; gateway 1 flushes
        // twice (cumulative values: the second replaces the first).
        let g1 = [row(fp.id, h, 0, 3, 1000)];
        t.replace_stats(&mut conn, 1, &g1).await.unwrap();
        t.replace_stats(&mut conn, 1, &[row(fp.id, h, 0, 5, 1000)])
            .await
            .unwrap();
        t.replace_stats(&mut conn, 2, &[row(fp.id, h, 0, 4, 200_000)])
            .await
            .unwrap();
        t.replace_stats(&mut conn, 1, &[row(fp.id, h - 3600, 0, 1, 1000)])
            .await
            .unwrap();
        let from = UNIX_EPOCH + Duration::from_secs(h - 3600);
        let to = UNIX_EPOCH + Duration::from_secs(h + 3600);
        let w = t.window(&mut conn, TenantId(1), from, to).await.unwrap();
        assert_eq!(w.len(), 1);
        let w = &w[0];
        assert_eq!(w.id, fp.id);
        assert_eq!(&*w.text, &*fp.text);
        assert_eq!(w.runs(), 10);
        assert_eq!(w.stats.max_us, 200_000);
        assert!(w.p95_ms() >= 200.0, "{}", w.p95_ms());
        assert_eq!(w.stats.examined_samples, 3);
        assert_eq!(w.stats.scan_runs, 3);
        assert_eq!(w.stats.scan_sessions.len(), 2, "union of session sets");
        // Only this hour.
        let w = t
            .window(
                &mut conn,
                TenantId(1),
                UNIX_EPOCH + Duration::from_secs(h),
                to,
            )
            .await
            .unwrap();
        assert_eq!(w[0].runs(), 9);
        // last_seen never moves back (first_seen was now).
        let fr = t.fingerprints(&mut conn, &[fp.id]).await.unwrap();
        assert!(fr[0].last_seen >= UNIX_EPOCH + Duration::from_secs(h + 10));
        assert!(fr[0].last_seen >= fr[0].first_seen);
        assert_eq!(fr[0].stmt_type, "SELECT");
    })
    .await;
}

#[tokio::test]
async fn rollup_merges_closed_buckets_once_and_keeps_windows_equal() {
    with_schema("rollup", |mut conn, schema| async move {
        let t = WorkloadTables::new(&schema);
        let (fp, n) = new_fp("SELECT * FROM orders WHERE id = 1");
        t.insert_fingerprints(&mut conn, &[n]).await.unwrap();
        let h = hour();
        let old = h - 2 * 3600;
        for (g, part) in [(1, 0), (1, 1), (2, 0)] {
            t.replace_stats(&mut conn, g, &[row(fp.id, old, part, 2, 1000)])
                .await
                .unwrap();
        }
        // The open bucket isn't touched.
        t.replace_stats(&mut conn, 1, &[row(fp.id, h, 0, 1, 1000)])
            .await
            .unwrap();
        let before = span(&t, &mut conn, old, h + 3600).await;
        let rep = t
            .rollup(&mut conn, h + 60, &Retention::default())
            .await
            .unwrap();
        assert_eq!((rep.merged, rep.merged_rows), (1, 3));
        assert_eq!(count(&mut conn, &schema, "workload_stats").await, 2);
        let after = span(&t, &mut conn, old, h + 3600).await;
        assert_eq!(before[0].stats, after[0].stats);
        // A late partial row of the merged bucket merges into the same row.
        t.replace_stats(&mut conn, 3, &[row(fp.id, old, 0, 1, 1000)])
            .await
            .unwrap();
        let rep = t
            .rollup(&mut conn, h + 60, &Retention::default())
            .await
            .unwrap();
        assert_eq!((rep.merged, rep.merged_rows), (1, 2));
        let again = t
            .rollup(&mut conn, h + 60, &Retention::default())
            .await
            .unwrap();
        assert_eq!(again.merged, 0, "idempotent");
        assert_eq!(
            span(&t, &mut conn, old, h + 3600).await[0].runs(),
            before[0].runs() + 1
        );
    })
    .await;
}

#[tokio::test]
async fn old_hours_become_day_rows_and_retention_deletes() {
    with_schema("days", |mut conn, schema| async move {
        let t = WorkloadTables::new(&schema);
        let (fp, n) = new_fp("SELECT * FROM orders WHERE id = 2");
        t.insert_fingerprints(&mut conn, &[n]).await.unwrap();
        let h = hour();
        let day = (h - 10 * 86_400) - (h - 10 * 86_400) % 86_400;
        for i in 0..5 {
            t.replace_stats(&mut conn, 1, &[row(fp.id, day + i * 3600, 0, 2, 1000)])
                .await
                .unwrap();
        }
        // Past retention.
        t.replace_stats(&mut conn, 1, &[row(fp.id, h - 40 * 86_400, 0, 2, 1000)])
            .await
            .unwrap();
        let rep = t.rollup(&mut conn, h, &Retention::default()).await.unwrap();
        assert_eq!(rep.merged, 6, "partials of gateway 1 first");
        assert_eq!(rep.days, 6);
        let w = t
            .window(
                &mut conn,
                TenantId(1),
                UNIX_EPOCH + Duration::from_secs(day),
                UNIX_EPOCH + Duration::from_secs(day + 86_400),
            )
            .await
            .unwrap();
        assert_eq!(w[0].runs(), 10);
        let parts: Vec<u16> = conn
            .query(format!(
                "SELECT part FROM {}.workload_stats ORDER BY bucket_start",
                quote(&schema)
            ))
            .await
            .unwrap();
        assert_eq!(parts, vec![DAY_PART, DAY_PART]);
        // Rerun: day rows stay as they are.
        let rep = t.rollup(&mut conn, h, &Retention::default()).await.unwrap();
        assert_eq!((rep.merged, rep.days), (0, 0));
        let rep = t.retain(&mut conn, h, &Retention::default()).await.unwrap();
        assert_eq!(rep.expired_stats, 1);
        assert_eq!(rep.expired_fingerprints, 0, "seen recently");
        let rep = t
            .retain(&mut conn, h + 60 * 86_400, &Retention::default())
            .await
            .unwrap();
        assert_eq!((rep.expired_stats, rep.expired_fingerprints), (1, 1));
    })
    .await;
}

#[tokio::test]
async fn samples_are_stored_sealed_capped_and_opened() {
    with_schema("samples", |mut conn, schema| async move {
        let t = WorkloadTables::new(&schema);
        let keys = MasterKeyring::new([(1, [3u8; 32])]).unwrap();
        let rng = SystemRandom::new();
        let (fp, _) = new_fp("SELECT * FROM orders WHERE customer_id = 'secret-value'");
        let sql = b"SELECT * FROM orders WHERE customer_id = 'secret-value'";
        let payload = SamplePayload {
            literals: SampledLiterals::from_execution(sql, &fp, &[]),
            context: SampleContext {
                database: Some("shop".into()),
                db_id: DbId(1),
                ..SampleContext::default()
            },
        };
        let now = SystemTime::now();
        let mut rows = Vec::new();
        for i in 0..60u64 {
            let mut s = seal(
                &keys,
                &rng,
                TenantId(1),
                fp.id,
                &payload,
                CaptureMeta {
                    captured_at: now - Duration::from_secs(i),
                    latency_us: 1000,
                    rows_examined: Some(10),
                    rows_returned: 1,
                    expires_at: now + Duration::from_secs(if i == 59 { 0 } else { 7 * 86_400 }),
                },
            )
            .unwrap();
            s.sample_id = i + 1;
            rows.push(s);
        }
        t.replace_samples(&mut conn, &rows).await.unwrap();
        // Stored sealed: the value isn't in the table.
        let hits: u64 = conn
            .query_first(format!(
                "SELECT COUNT(*) FROM {}.workload_samples WHERE INSTR(payload, 'secret') > 0",
                quote(&schema)
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hits, 0);
        let got = t.samples(&mut conn, fp.id, 5).await.unwrap();
        assert_eq!(got.len(), 5);
        assert_eq!(got[0].sample_id, 1, "newest first");
        let (a, b) = (&got[0], &rows[0]);
        assert_eq!(
            (
                a.nonce,
                &a.payload,
                a.key_version,
                a.rows_examined,
                a.latency_us
            ),
            (
                b.nonce,
                &b.payload,
                b.key_version,
                b.rows_examined,
                b.latency_us
            )
        );
        let skew = |x: SystemTime, y: SystemTime| {
            x.duration_since(y)
                .or_else(|_| y.duration_since(x))
                .unwrap()
        };
        assert!(skew(a.captured_at, b.captured_at) < Duration::from_micros(1));
        assert!(skew(a.expires_at, b.expires_at) < Duration::from_secs(1));
        let opened = t
            .open_samples(&mut conn, &keys, TenantId(1), fp.id, 2)
            .await
            .unwrap();
        assert_eq!(opened.len(), 2);
        assert_eq!(opened[0].payload, payload);
        // Another tenant's keys open nothing.
        let none = t
            .open_samples(&mut conn, &keys, TenantId(2), fp.id, 2)
            .await
            .unwrap();
        assert!(none.is_empty());
        tokio::time::sleep(Duration::from_secs(1)).await;
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let rep = t
            .retain(&mut conn, secs, &Retention::default())
            .await
            .unwrap();
        assert_eq!(
            rep.expired_samples,
            1 + 9,
            "one expired, then the cap of 50"
        );
        assert_eq!(count(&mut conn, &schema, "workload_samples").await, 50);
    })
    .await;
}

#[tokio::test]
async fn maintain_migrates_rolls_up_and_retains_through_a_locator() {
    with_schema("maintain", |mut conn, schema| async move {
        let t = WorkloadTables::new(&schema);
        let (fp, n) = new_fp("SELECT * FROM orders WHERE id = 3");
        t.insert_fingerprints(&mut conn, &[n]).await.unwrap();
        let h = hour();
        for g in [1, 2] {
            t.replace_stats(&mut conn, g, &[row(fp.id, h - 7200, 0, 2, 1000)])
                .await
                .unwrap();
        }
        let url = std::env::var(MARIADB_ENV).unwrap();
        let pool = mysql_async::Pool::new(url.as_str());
        let locator = sprig_catalog::StaticLocator::new().with(
            TenantId(1),
            sprig_catalog::CatalogEndpoint {
                pool: pool.clone(),
                store: sprig_catalog::Store::new(schema.clone()),
            },
        );
        let rep = maintain(
            &locator,
            TenantId(1),
            UNIX_EPOCH + Duration::from_secs(h + 60),
            &Retention::default(),
            "test",
        )
        .await
        .unwrap();
        assert_eq!((rep.merged, rep.merged_rows), (1, 2));
        let store = CatalogWorkloadStore::new(locator.shared());
        let w = store
            .window(
                TenantId(1),
                UNIX_EPOCH + Duration::from_secs(h - 7200),
                UNIX_EPOCH + Duration::from_secs(h),
            )
            .await
            .unwrap();
        assert_eq!(w[0].runs(), 4);
        // A tenant whose catalog has no workload tables reads as empty.
        conn.query_drop(format!("CREATE DATABASE {}", quote(&format!("{schema}_e"))))
            .await
            .unwrap();
        let none = sprig_catalog::StaticLocator::new().with(
            TenantId(2),
            sprig_catalog::CatalogEndpoint {
                pool: pool.clone(),
                store: sprig_catalog::Store::new(format!("{schema}_e")),
            },
        );
        let empty = CatalogWorkloadStore::new(none.shared())
            .window(TenantId(2), UNIX_EPOCH, UNIX_EPOCH + Duration::from_secs(h))
            .await
            .unwrap();
        assert!(empty.is_empty());
        conn.query_drop(format!("DROP DATABASE {}", quote(&format!("{schema}_e"))))
            .await
            .unwrap();
        drop(conn);
        pool.disconnect().await.unwrap();
    })
    .await;
}

/// A fingerprint row recorded with an older shape (shape
/// version 1, `json_path` without `func` and `cmp`) is rewritten with this
/// build's shape the next time a gateway records the statement; a row of
/// the current or a newer version is left alone.
#[tokio::test]
async fn an_old_shape_version_row_is_rewritten() {
    with_schema("x34", |mut conn, schema| async move {
        let t = WorkloadTables::new(&schema);
        let (fp, n) = new_fp("SELECT k FROM db0 WHERE JSON_VALUE(v, '$.email') = 'a@b.c'");
        assert!(!fp.shape.all_unsargable(), "{}", fp.shape.to_json());
        // The row as a build before shape version 2 wrote it.
        let mut old: serde_json::Value = serde_json::from_str(&fp.shape.to_json()).unwrap();
        old["v"] = 1.into();
        for p in old["predicates"].as_array_mut().unwrap() {
            let p = p.as_object_mut().unwrap();
            assert_eq!(p["op"], "json_path");
            p.remove("func");
            p.remove("cmp");
        }
        t.insert_fingerprints(&mut conn, std::slice::from_ref(&n))
            .await
            .unwrap();
        let fps = format!("{}.workload_fingerprints", quote(&schema));
        conn.exec_drop(
            format!("UPDATE {fps} SET shape = ?, shape_version = 1, flags = 'static'"),
            (old.to_string(),),
        )
        .await
        .unwrap();
        let before = t.fingerprints(&mut conn, &[fp.id]).await.unwrap();
        assert!(
            before[0].shape.all_unsargable(),
            "the old row: no field column"
        );
        // The statement is seen again: the row is rewritten (2 affected).
        assert_eq!(
            t.insert_fingerprints(&mut conn, std::slice::from_ref(&n))
                .await
                .unwrap(),
            2
        );
        let after = t.fingerprints(&mut conn, &[fp.id]).await.unwrap();
        assert_eq!(*after[0].shape, *fp.shape);
        assert!(after[0].shape.predicates[0].op.is_field_sargable());
        assert_eq!(after[0].first_seen, before[0].first_seen);
        assert_eq!(after[0].flags, "static", "flags are kept");
        let v: u16 = conn
            .query_first(format!("SELECT shape_version FROM {fps}"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(v, SHAPE_VERSION);
        // Same version: nothing changes.
        assert_eq!(
            t.insert_fingerprints(&mut conn, std::slice::from_ref(&n))
                .await
                .unwrap(),
            0
        );
        // A newer build's row isn't taken back.
        conn.query_drop(format!(
            "UPDATE {fps} SET shape = '{{}}', shape_version = {}",
            SHAPE_VERSION + 1
        ))
        .await
        .unwrap();
        assert_eq!(t.insert_fingerprints(&mut conn, &[n]).await.unwrap(), 0);
        let s: String = conn
            .query_first(format!("SELECT shape FROM {fps}"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(s, "{}");
    })
    .await;
}
