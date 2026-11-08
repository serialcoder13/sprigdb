#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use sprig_common::ids::{DbId, TenantId};
use sprig_common::time::ManualClock;
use sprig_proto_mysql::packets::Persona;

use super::*;
use crate::fingerprint::{Fingerprint, SqlCtx, SqlFingerprinter};
use crate::recorder::{ExecSummary, RecorderConfig};
use crate::shape::NoResolver;

fn fp(f: &SqlFingerprinter, t: u64, sql: &str) -> Fingerprint {
    let ctx = SqlCtx::new(Persona::MariaDb).current_db("shop", DbId(1));
    f.sql(TenantId(t), sql.as_bytes(), &ctx, &NoResolver)
}

fn ex(us: u64) -> ExecSummary {
    ExecSummary {
        latency_us: us,
        rows_returned: 1,
        ..ExecSummary::default()
    }
}

struct Rig {
    rec: Arc<Recorder>,
    clock: Arc<ManualClock>,
    target: Arc<MemoryTarget>,
    flusher: Flusher,
    f: SqlFingerprinter,
}

fn rig() -> Rig {
    let clock = Arc::new(ManualClock::new());
    // Start just after a bucket boundary.
    let now = clock
        .wall()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    clock.advance(Duration::from_secs(3600 - now % 3600 + 5));
    let rec = Arc::new(Recorder::new(RecorderConfig::default(), clock.clone()));
    let target = Arc::new(MemoryTarget::default());
    let flusher = Flusher::new(
        Arc::clone(&rec),
        Arc::new(SampleOutbox::new(3, 1)),
        target.clone(),
        7,
        clock.clone(),
        Arc::new(FlushSettings::default),
    );
    Rig {
        rec,
        clock,
        target,
        flusher,
        f: SqlFingerprinter::default(),
    }
}

#[tokio::test]
async fn a_flush_writes_new_fingerprints_once_and_cumulative_stats() {
    let r = rig();
    let a = fp(&r.f, 1, "SELECT * FROM t WHERE id = 1");
    let b = fp(&r.f, 2, "SELECT * FROM u WHERE id = 1");
    for _ in 0..3 {
        r.rec.record(TenantId(1), &a, &ex(100));
    }
    r.rec.record(TenantId(2), &b, &ex(100));
    let rep = r.flusher.flush().await;
    assert_eq!((rep.tenants, rep.fingerprints, rep.stats), (2, 2, 2));
    r.rec.record(TenantId(1), &a, &ex(100));
    let rep = r.flusher.flush().await;
    // No new fingerprint; tenant 1's cumulative row again (4 runs).
    assert_eq!((rep.fingerprints, rep.stats), (0, 2));
    let batches = r.target.batches.lock().unwrap();
    let last = batches
        .iter()
        .rev()
        .find(|(t, ..)| *t == TenantId(1))
        .unwrap();
    assert_eq!(last.1, 7, "under this gateway's id");
    assert_eq!(last.2.stats[0].stats.exec_count, 4);
}

#[tokio::test]
async fn failed_tenants_keep_their_rows_and_others_go_on() {
    let r = rig();
    let a = fp(&r.f, 1, "SELECT * FROM t WHERE id = 1");
    let b = fp(&r.f, 2, "SELECT * FROM u WHERE id = 1");
    r.rec.record(TenantId(1), &a, &ex(100));
    r.rec.record(TenantId(2), &b, &ex(100));
    r.target.failing.lock().unwrap().insert(TenantId(1));
    let rep = r.flusher.flush().await;
    assert_eq!((rep.tenants, rep.failed), (1, 1));
    // The bucket ends while tenant 1's catalog is down: its closed row is
    // kept, then written once the catalog is back.
    r.clock.advance(Duration::from_secs(3600));
    let rep = r.flusher.flush().await;
    assert_eq!(rep.failed, 1);
    r.target.failing.lock().unwrap().clear();
    let rep = r.flusher.flush().await;
    assert_eq!(rep.failed, 0);
    let batches = r.target.batches.lock().unwrap();
    let t1: Vec<_> = batches.iter().filter(|(t, ..)| *t == TenantId(1)).collect();
    assert_eq!(t1.len(), 1);
    assert_eq!(t1[0].2.fingerprints.len(), 1, "the first sighting survived");
    assert_eq!(t1[0].2.stats.len(), 1, "the closed bucket survived");
    assert_eq!(t1[0].2.stats[0].stats.exec_count, 1);
}

#[tokio::test]
async fn closed_rows_past_the_rollup_delay_are_dropped_not_double_counted() {
    let r = rig();
    let a = fp(&r.f, 1, "SELECT * FROM t WHERE id = 1");
    r.rec.record(TenantId(1), &a, &ex(100));
    r.target.failing.lock().unwrap().insert(TenantId(1));
    r.flusher.flush().await;
    // Down for the whole next bucket and the rollup delay.
    r.clock.advance(Duration::from_secs(3600 + 11 * 60));
    let rep = r.flusher.flush().await;
    assert_eq!(rep.late_dropped, 1);
    r.target.failing.lock().unwrap().clear();
    let rep = r.flusher.flush().await;
    assert_eq!((rep.stats, rep.fingerprints), (0, 1));
}

#[tokio::test]
async fn the_next_flush_comes_at_the_period_or_the_bucket_end() {
    let r = rig();
    // 5 s into the bucket: the period (900 s) comes first.
    assert_eq!(
        r.flusher.next_delay(Duration::ZERO),
        Duration::from_secs(900)
    );
    r.clock.advance(Duration::from_secs(3000));
    assert_eq!(
        r.flusher.next_delay(Duration::from_secs(30)),
        Duration::from_secs(3600 - 3005 + 30)
    );
}

#[tokio::test]
async fn run_flushes_once_more_at_shutdown() {
    let r = rig();
    let a = fp(&r.f, 1, "SELECT 1");
    r.rec.record(TenantId(1), &a, &ex(100));
    let flusher = Arc::new(r.flusher);
    let stop = CancellationToken::new();
    stop.cancel();
    Arc::clone(&flusher).run(stop).await;
    assert_eq!(r.target.batches.lock().unwrap().len(), 1);
}
