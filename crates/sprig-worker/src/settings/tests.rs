//! The worker's settings follow etcd: a dc-scope change reaches what jobs
//! resolve within chapter 23's 2 s.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_common::ids::DcId;
use sprig_common::CancellationToken;
use sprig_meta::{MemStore, MetaStore, Op, Record, Tier, Txn};
use sprig_mover::MoverSettings;
use sprig_settings::etcd::SettingRecord;
use sprig_settings::{SettingCtx, Value};

use super::WorkerSettings;

/// Chapter 23: "a change reaches every gateway within 2 s"; workers too.
const DELAY: Duration = Duration::from_secs(2);

async fn put(store: &dyn MetaStore, rel: &str, v: i64) {
    let rec = Record::new(SettingRecord {
        value: Some(Value::Int(v)),
        set_by: "op:test".into(),
        ..SettingRecord::default()
    });
    store
        .txn(Txn {
            when: Vec::new(),
            then: vec![Op::Put {
                key: store.tier().key(rel).unwrap(),
                value: rec.encode().unwrap(),
                lease: None,
            }],
            otherwise: Vec::new(),
        })
        .await
        .unwrap();
}

async fn within<T: PartialEq + std::fmt::Debug>(want: &T, f: impl Fn() -> T) -> Duration {
    let t0 = Instant::now();
    loop {
        if f() == *want {
            return t0.elapsed();
        }
        assert!(t0.elapsed() < DELAY, "not applied within {DELAY:?}: {:?}", f());
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn dc_scope_changes_reach_job_settings_within_two_seconds() {
    let dc = DcId::new("mum1").unwrap();
    let mem = MemStore::new(Tier::Dc(dc.clone()));
    let global: Arc<dyn MetaStore> = Arc::new(mem.with_tier(Tier::Global));
    let dc_store: Arc<dyn MetaStore> = Arc::new(mem);
    let stop = CancellationToken::new();
    let s = WorkerSettings::start(Arc::clone(&global), Arc::clone(&dc_store), dc.clone(), &stop)
        .await
        .unwrap()
        .unwrap();
    let ctx = || {
        let snap = s.snapshot();
        MoverSettings::resolve(&snap, &SettingCtx::process(&dc))
    };
    assert_eq!(ctx().source_retain, Duration::from_secs(24 * 3600));

    // The bug: `mover.source_retain_hours = 0` at dc scope.
    put(&*dc_store, "settings/mover.source_retain_hours", 0).await;
    let took = within(&Duration::ZERO, || ctx().source_retain).await;
    eprintln!("dc-scope mover.source_retain_hours reached the worker in {took:?}");

    // Global under dc: the dc value still wins; a later global change of
    // another key arrives too.
    put(&*global, "settings/mover.copy_chunk_rows", 77).await;
    within(&77, || ctx().copy_chunk_rows).await;
    assert_eq!(ctx().source_retain, Duration::ZERO);

    // Trigger settings, re-read at every failure through `RunOpts::live`.
    let opts = s.run_opts();
    assert_eq!(opts.max_attempts, 10);
    put(&*global, "settings/logic.trigger_max_attempts", 3).await;
    let took = within(&3, || opts.current().max_attempts).await;
    eprintln!("global logic.trigger_max_attempts reached a running trigger in {took:?}");

    // kv.* for the sweeper's next pass.
    put(&*dc_store, "settings/kv.purge_interval_seconds", 2).await;
    within(&2, || {
        s.get(&sprig_controller::kv_sweep_loop::PURGE_INTERVAL_SECONDS)
    })
    .await;
    stop.cancel();
}

#[tokio::test]
async fn defaults_without_mirrors() {
    let s = WorkerSettings::defaults(DcId::new("mum1").unwrap()).unwrap();
    assert!(s.live().is_none());
    assert_eq!(s.trigger_budget(), Duration::from_secs(30));
    assert_eq!(s.job_budget(), Duration::from_secs(300));
    assert_eq!(s.run_opts().lanes, 8);
}
