//! The worker's settings follow etcd: a dc-scope change reaches what jobs
//! resolve within chapter 23's 2 s.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_common::CancellationToken;
use sprig_common::ids::DcId;
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
        assert!(
            t0.elapsed() < DELAY,
            "not applied within {DELAY:?}: {:?}",
            f()
        );
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
    let s = WorkerSettings::start(
        Arc::clone(&global),
        Arc::clone(&dc_store),
        dc.clone(),
        &stop,
    )
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

/// Decision RF3 (r2last): a tenant's own catalog rows win over the etcd
/// scopes for its jobs: trigger options and the mover's cutover hold.
#[tokio::test]
async fn tenant_rows_win_for_that_tenants_jobs() {
    use sprig_catalog::{SettingRow, SettingScope};
    use sprig_common::ids::TenantId;
    use sprig_controller::tenant_scopes::{TenantScopes, build_rows};

    let dc = DcId::new("mum1").unwrap();
    let mem = MemStore::new(Tier::Dc(dc.clone()));
    let global: Arc<dyn MetaStore> = Arc::new(mem.with_tier(Tier::Global));
    let dc_store: Arc<dyn MetaStore> = Arc::new(mem);
    let stop = CancellationToken::new();
    let scopes = TenantScopes::new(
        Arc::clone(&global),
        Arc::clone(&dc_store),
        Arc::new(sprig_controller::catalogs::StaticPools::new()),
    )
    .unwrap();
    let s = WorkerSettings::start(
        Arc::clone(&global),
        Arc::clone(&dc_store),
        dc.clone(),
        &stop,
    )
    .await
    .unwrap()
    .unwrap()
    .with_tenant_scopes(Arc::clone(&scopes));
    put(&*global, "settings/logic.trigger_max_attempts", 3).await;
    put(&*dc_store, "settings/mover.cutover_max_hold_ms", 4000).await;
    within(&3, || s.run_opts().max_attempts).await;
    let t = TenantId(77);
    let row = |key: &str, v: serde_json::Value| SettingRow {
        scope: SettingScope::Tenant,
        key: key.into(),
        value: Some(v),
        bounds: None,
        set_by: "op:test".into(),
        set_at: std::time::SystemTime::UNIX_EPOCH,
        version: 2,
    };
    scopes.seed(
        t,
        2,
        build_rows(
            sprig_settings::Registry::global().unwrap(),
            t,
            2,
            vec![
                row("logic.trigger_max_attempts", serde_json::json!(7)),
                row("mover.cutover_max_hold_ms", serde_json::json!(900)),
            ],
        ),
    );
    // The tenant's trigger: its own value; any other tenant: the global.
    assert_eq!(s.run_opts_for(t).max_attempts, 7);
    assert_eq!(s.run_opts_for(TenantId(78)).max_attempts, 3);
    // The mover: the tenant's value over the dc's.
    let snap = s.snapshot();
    let rows = scopes.cached(t);
    let ctx = SettingCtx::process(&dc).for_tenant(t, &[]);
    assert_eq!(
        MoverSettings::resolve_with(&snap, rows.as_deref(), &ctx).cutover_max_hold,
        Duration::from_millis(900)
    );
    assert_eq!(
        MoverSettings::resolve(&snap, &ctx).cutover_max_hold,
        Duration::from_millis(4000)
    );
    stop.cancel();
}
