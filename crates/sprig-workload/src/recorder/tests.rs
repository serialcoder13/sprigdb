//! Recorder tests: counting, buckets, caps, eviction, probes.

#![allow(clippy::many_single_char_names)]

use std::sync::Arc;
use std::time::Duration;

use sprig_common::ids::{DbId, TenantId};
use sprig_common::time::ManualClock;
use sprig_proto_mysql::packets::Persona;

use super::*;
use crate::fingerprint::{SqlCtx, SqlFingerprinter};
use crate::shape::NoResolver;

fn fp(f: &SqlFingerprinter, tenant: u64, sql: &str) -> Fingerprint {
    let ctx = SqlCtx::new(Persona::MariaDb).current_db("shop", DbId(1));
    f.sql(TenantId(tenant), sql.as_bytes(), &ctx, &NoResolver)
}

fn ex(us: u64) -> ExecSummary {
    ExecSummary {
        latency_us: us,
        rows_returned: 1,
        shards: 1,
        clusters: 1,
        ..ExecSummary::default()
    }
}

fn setup(cfg: RecorderConfig) -> (Recorder, Arc<ManualClock>, SqlFingerprinter) {
    let clock = Arc::new(ManualClock::new());
    (
        Recorder::new(cfg, clock.clone()),
        clock,
        SqlFingerprinter::default(),
    )
}

#[test]
fn counts_and_percentiles() {
    let (r, _c, f) = setup(RecorderConfig::default());
    let a = fp(&f, 1, "SELECT * FROM t WHERE id = 1");
    let a2 = fp(&f, 1, "select * from t where id = 99");
    assert_eq!(a.id, a2.id);
    for us in 1..=100u64 {
        r.record(
            TenantId(1),
            if us % 2 == 0 { &a } else { &a2 },
            &ex(us * 100),
        );
    }
    let s = r.stats(TenantId(1), a.id).unwrap();
    assert_eq!(s.exec_count, 100);
    assert_eq!(s.total_us, 505_000);
    assert_eq!(s.max_us, 10_000);
    let (p50, p95, p99) = s.latency_hist.p50_p95_p99().unwrap();
    assert!((5_000..=6_000).contains(&p50), "{p50}");
    assert!((9_500..=11_300).contains(&p95), "{p95}");
    assert!(p99 >= 9_900);
    let new = r.drain_new();
    assert_eq!(new.len(), 1);
    assert_eq!(&*new[0].meta.text, "SELECT * FROM shop.t WHERE id = ?");
    assert!(r.drain_new().is_empty());
    assert_eq!(r.metrics().fingerprints_active, 1);
}

#[test]
fn buckets_roll_into_the_closed_outbox() {
    let (r, c, f) = setup(RecorderConfig::default());
    let a = fp(&f, 1, "SELECT 1");
    r.record(TenantId(1), &a, &ex(50));
    r.record(TenantId(1), &a, &ex(70));
    let snap = r.snapshot();
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].stats.exec_count, 2);
    assert!(r.drain_closed().is_empty());
    c.advance(Duration::from_secs(3600));
    r.record(TenantId(1), &a, &ex(90));
    let closed = r.drain_closed();
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0].stats.exec_count, 2);
    assert_eq!(closed[0].bucket_start + 3600, r.snapshot()[0].bucket_start);
    // A snapshot after the bucket ends closes it too.
    c.advance(Duration::from_secs(3600));
    assert!(r.snapshot().is_empty());
    assert_eq!(r.drain_closed()[0].stats.exec_count, 1);
}

#[test]
fn tenant_fingerprint_cap_counts_under_other() {
    let cfg = RecorderConfig {
        max_fingerprints_per_tenant: 3,
        ..RecorderConfig::default()
    };
    let (r, _c, f) = setup(cfg);
    for i in 0..10 {
        let sql = format!("SELECT c{i} FROM t");
        r.record(TenantId(7), &fp(&f, 7, &sql), &ex(10));
    }
    let other = Reserved::Other.id(FpKind::Sql);
    assert_eq!(r.stats(TenantId(7), other).unwrap().exec_count, 7);
    assert_eq!(r.metrics().other_total, 7);
    // Another tenant is unaffected.
    r.record(TenantId(8), &fp(&f, 8, "SELECT x FROM t"), &ex(10));
    assert!(r.stats(TenantId(8), other).is_none());
    let new = r.drain_new();
    assert_eq!(new.iter().filter(|n| n.meta.flags.other).count(), 1);
}

#[test]
fn memory_cap_evicts_coldest_and_starts_new_parts() {
    let cfg = RecorderConfig {
        max_memory_bytes: 64 << 10,
        max_tenant_memory_pct: 100,
        ..RecorderConfig::default()
    };
    let (r, c, f) = setup(cfg);
    // Start at the beginning of a bucket, so the whole run (about 440 s)
    // stays in one bucket whatever the real time is: the test failed when
    // the run crossed a bucket's end.
    let bucket_ms = u64::from(RecorderConfig::default().bucket_minutes) * 60_000;
    let now_ms = u64::try_from(
        Clock::wall(&*c)
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    c.advance(Duration::from_millis(bucket_ms - now_ms % bucket_ms));
    let first = fp(&f, 1, "SELECT first FROM t");
    r.record(TenantId(1), &first, &ex(10));
    for i in 0..400 {
        c.advance(Duration::from_millis(1100));
        let sql = format!("SELECT c{i} FROM t{i}");
        r.record(TenantId(1), &fp(&f, 1, &sql), &ex(10));
    }
    let m = r.metrics();
    assert!(m.evicted_total > 0);
    assert!(m.memory_bytes <= 64 << 10, "{m:?}");
    // The coldest (first) entry was evicted into the outbox ...
    assert!(r.stats(TenantId(1), first.id).is_none());
    let closed = r.drain_closed();
    let bucket = closed.first().map(|x| x.bucket_start);
    if let Some(row) = closed.iter().find(|x| x.fingerprint == first.id) {
        assert_eq!(row.part, 0);
        assert_eq!(Some(row.bucket_start), bucket);
    }
    // ... and starts part 1 when it returns in the same bucket.
    r.record(TenantId(1), &first, &ex(10));
    let snap = r.snapshot();
    let back = snap.iter().find(|x| x.fingerprint == first.id).unwrap();
    assert_eq!(back.part, 1);
    if let Some(b) = bucket {
        assert_eq!(back.bucket_start, b);
    }
}

#[test]
fn a_noisy_tenant_cannot_evict_the_others() {
    let cfg = RecorderConfig {
        max_memory_bytes: 256 << 10,
        max_tenant_memory_pct: 25,
        ..RecorderConfig::default()
    };
    let (r, c, f) = setup(cfg);
    let quiet: Vec<Fingerprint> = (0..5)
        .map(|i| fp(&f, 2, &format!("SELECT q{i} FROM t")))
        .collect();
    for q in &quiet {
        r.record(TenantId(2), q, &ex(10));
    }
    for i in 0..2000 {
        c.advance(Duration::from_millis(10));
        r.record(
            TenantId(1),
            &fp(&f, 1, &format!("SELECT n{i} FROM t")),
            &ex(10),
        );
    }
    assert!(
        r.tenant_bytes(TenantId(1)) <= (256 << 10) / 4,
        "{}",
        r.tenant_bytes(TenantId(1))
    );
    for q in &quiet {
        assert!(r.stats(TenantId(2), q.id).is_some());
    }
}

#[test]
fn scan_runs_and_sessions() {
    let (r, _c, f) = setup(RecorderConfig::default());
    let a = fp(&f, 1, "SELECT * FROM big WHERE note LIKE '%x%'");
    for s in 0..12u64 {
        let e = ExecSummary {
            latency_us: 2_000_000,
            rows_returned: 10,
            examined: Some(2_000_000),
            session: s % 10,
            ..ExecSummary::default()
        };
        r.record(TenantId(1), &a, &e);
    }
    // Returning 5% of what it examined is not a scan run.
    let e = ExecSummary {
        latency_us: 10,
        rows_returned: 100_000,
        examined: Some(2_000_000),
        session: 99,
        ..ExecSummary::default()
    };
    r.record(TenantId(1), &a, &e);
    let s = r.stats(TenantId(1), a.id).unwrap();
    assert_eq!(s.scan_runs, 12);
    assert_eq!(s.scan_sessions.len(), 8);
    assert_eq!(s.examined_samples, 13);
    assert_eq!(
        SessionSet::decode(&s.scan_sessions.encode()),
        s.scan_sessions
    );
}

#[test]
fn plan_probes_watched_fingerprints() {
    let (r, _c, f) = setup(RecorderConfig::default());
    let fast = fp(&f, 1, "SELECT 1 FROM t WHERE a = 1");
    let slow = fp(&f, 1, "SELECT 2 FROM t WHERE b = 1");
    assert_eq!(r.plan(TenantId(1), &fast), ProbePlan::default());
    for _ in 0..20 {
        r.record(TenantId(1), &fast, &ex(100));
        r.record(TenantId(1), &slow, &ex(200_000));
    }
    assert!(!r.plan(TenantId(1), &fast).examined);
    // p95 200 ms > 50 ms, mean >= 100 ms: every execution is probed.
    for _ in 0..10 {
        assert!(r.plan(TenantId(1), &slow).examined);
    }
    // Watched by request.
    r.watch(TenantId(1), fast.id, true);
    assert!(
        r.plan(TenantId(1), &fast).examined,
        "first probe of the bucket"
    );
}

#[test]
fn watched_fingerprints_get_their_first_samples_of_a_bucket() {
    let (r, c, f) = setup(RecorderConfig {
        sample_rate: 0.0,
        ..RecorderConfig::default()
    });
    let slow = fp(&f, 1, "SELECT 2 FROM t WHERE b = 1");
    r.record(TenantId(1), &slow, &ex(200_000));
    let asked = (0..10)
        .filter(|_| r.plan(TenantId(1), &slow).capture_params)
        .count();
    assert_eq!(asked, 3, "the floor, then the rate (0)");
    // A new bucket starts a new floor.
    c.advance(std::time::Duration::from_secs(3600));
    r.record(TenantId(1), &slow, &ex(200_000));
    let asked = (0..10)
        .filter(|_| r.plan(TenantId(1), &slow).capture_params)
        .count();
    assert_eq!(asked, 3);
}

#[test]
fn concurrent_records_all_count() {
    let (r, _c, f) = setup(RecorderConfig::default());
    let r = Arc::new(r);
    let a = Arc::new(fp(&f, 1, "SELECT * FROM t WHERE id = 1"));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let (r, a) = (Arc::clone(&r), Arc::clone(&a));
            std::thread::spawn(move || {
                for i in 0..10_000u64 {
                    r.record(TenantId(1), &a, &ex(i % 1000));
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert_eq!(r.stats(TenantId(1), a.id).unwrap().exec_count, 80_000);
    assert_eq!(r.drain_new().len(), 1);
}

#[test]
fn forget_tenant_closes_its_entries() {
    let (r, _c, f) = setup(RecorderConfig::default());
    r.record(TenantId(3), &fp(&f, 3, "SELECT 1"), &ex(10));
    r.forget_tenant(TenantId(3));
    assert_eq!(r.metrics().fingerprints_active, 0);
    assert_eq!(r.drain_closed().len(), 1);
}

#[test]
fn digests_outlive_buckets_and_reset_per_tenant() {
    // R4 round 5: the totals `performance_schema`'s digest summary shows.
    let (r, c, f) = setup(RecorderConfig::default());
    let a = fp(&f, 1, "SELECT * FROM t WHERE id = 1");
    let b = fp(&f, 2, "SELECT * FROM t WHERE id = 1");
    r.record(TenantId(1), &a, &ex(50));
    c.advance(Duration::from_secs(3600));
    r.record(TenantId(1), &a, &ex(30));
    let mut failed = ex(90);
    failed.error = true;
    r.record(TenantId(1), &a, &failed);
    r.record(TenantId(2), &b, &ex(10));
    // The bucket started again; the digest didn't.
    assert_eq!(r.stats(TenantId(1), a.id).unwrap().exec_count, 2);
    let d = r.digests(TenantId(1));
    assert_eq!(d.len(), 1);
    let g = &d[0].digest;
    assert_eq!(
        (
            g.exec_count,
            g.error_count,
            g.total_us,
            g.min_us,
            g.max_us,
            g.rows_returned
        ),
        (3, 1, 170, 30, 90, 3)
    );
    assert_eq!(&*d[0].text, "SELECT * FROM shop.t WHERE id = ?");
    assert!(g.first_seen.unwrap() < g.last_seen.unwrap());
    r.reset_digests(TenantId(1));
    assert!(r.digests(TenantId(1)).is_empty());
    assert_eq!(r.digests(TenantId(2)).len(), 1);
    r.record(TenantId(1), &a, &ex(40));
    assert_eq!(r.digests(TenantId(1))[0].digest.exec_count, 1);
    assert_eq!(r.stats(TenantId(1), a.id).unwrap().exec_count, 3);
}
