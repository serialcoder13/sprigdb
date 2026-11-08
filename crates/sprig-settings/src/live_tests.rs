//! Tests of the etcd scopes: leader writes with their checks, the mirrors,
//! watches and callbacks. Every case runs against `MemStore` (paused time)
//! and, when `SPRIG_TEST_ETCD` names an etcd, against `EtcdStore`, as
//! `sprig-meta`'s conformance suite does. On etcd the "global" tier is a
//! private DC tier standing in for `/sprig/g/`, so cases never touch the
//! shared global keys, and every key is deleted afterwards.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::borrow::Cow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures::FutureExt;
use futures::future::BoxFuture;
use serde_json::Value as Json;
use sprig_common::ids::ClusterId;
use sprig_common::time::{Clock, SystemClock};
use sprig_common::{SprigCode, SprigError};
use sprig_meta::conformance::{ETCD_ENV, delete_prefix, unique_tier};
use sprig_meta::elect::{Election, Elector, LeaderRecord, Leadership};
use sprig_meta::session::{SessionOpts, TokioClock};
use sprig_meta::{Cmp, EtcdConfig, EtcdStore, MemStore, MetaStore, Op, Txn};

use crate::*;

const WAIT: Duration = Duration::from_secs(10);

static CACHE_MB: Setting<u32> = Setting {
    key: "test.cache_mb",
    default: 2000,
    scopes: Scopes::OPERATIONAL,
    hard: HardBounds::range(1, 100_000),
    apply: Apply::Live,
    operator_only: true,
    owner: 23,
    doc: "A cluster, dc and global key",
};
static MODE: Setting<Cow<'static, str>> = Setting {
    key: "test.mode",
    default: Cow::Borrowed("on"),
    scopes: Scopes::OPERATIONAL,
    hard: HardBounds::one_of(&["on", "off", "recommend"]),
    apply: Apply::Live,
    operator_only: true,
    owner: 23,
    doc: "A text key",
};
static PORT: Setting<u32> = Setting {
    key: "test.port",
    default: 3306,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(1, 65_535),
    apply: Apply::Rebind,
    operator_only: true,
    owner: 23,
    doc: "A dc and global key",
};
static DATA_ONLY: Setting<u32> = Setting {
    key: "test.data_only",
    default: 1,
    scopes: Scopes::DATA,
    hard: HardBounds::NONE,
    apply: Apply::Live,
    operator_only: false,
    owner: 23,
    doc: "A data-chain key: global, tenant, database, object",
};

fn registry() -> &'static Registry {
    static R: OnceLock<Registry> = OnceLock::new();
    R.get_or_init(|| {
        Registry::new([&CACHE_MB as &dyn AnySetting, &MODE, &PORT, &DATA_ONLY]).unwrap()
    })
}

/// What a case gets.
struct Env {
    stores: SettingsStores,
    clock: Arc<dyn Clock>,
}

impl Env {
    fn writer(&self) -> SettingsWriter {
        SettingsWriter::new(registry(), self.stores.clone(), self.clock.clone())
    }

    async fn lead(&self, store: &Arc<dyn MetaStore>) -> Leadership {
        let e = Election::new(
            Arc::clone(store),
            self.clock.clone(),
            SessionOpts::new(Duration::from_secs(5)),
        );
        let me = LeaderRecord {
            holder: "test".to_owned(),
            dc: None,
            build: "test".to_owned(),
        };
        e.campaign(&store.tier().key("leader").unwrap(), &me)
            .await
            .unwrap()
    }

    fn dc(&self) -> Scope {
        Scope::Dc(self.stores.dc_id().clone())
    }

    fn cluster(&self, n: u32) -> ClusterId {
        ClusterId::new(format!("{}-c{n}", self.stores.dc_id())).unwrap()
    }
}

type Case = fn(Env) -> BoxFuture<'static, ()>;

macro_rules! cases {
    ($($name:ident),* $(,)?) => {
        mod mem {
            $(#[test] fn $name() { super::run_mem(|e| Box::pin(super::$name(e))); })*
        }
        mod etcd {
            $(#[test] fn $name() { super::run_etcd(|e| Box::pin(super::$name(e))); })*
        }
    };
}

cases! {
    writes_are_checked_and_recorded,
    mirrors_follow_writes_and_notify,
    tightened_bounds_warn_and_clamp,
    a_deposed_leader_cannot_write,
    guarded_writes_and_bound_patches,
}

fn run_mem(case: Case) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    rt.block_on(async {
        let clock: Arc<dyn Clock> = Arc::new(TokioClock::new());
        let dc = MemStore::with_clock(unique_tier(), clock.clone());
        let global = dc.with_tier(unique_tier());
        let stores = SettingsStores::new(Arc::new(global), Arc::new(dc)).unwrap();
        case(Env { stores, clock }).await;
    });
}

fn run_etcd(case: Case) {
    let Ok(endpoints) = std::env::var(ETCD_ENV) else {
        eprintln!("skipping etcd settings case: set {ETCD_ENV} to run it");
        return;
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let cfg = EtcdConfig::new(endpoints.split(',').map(str::to_owned).collect());
        let dc = EtcdStore::connect(unique_tier(), &cfg).await.unwrap();
        let global = dc.with_tier(unique_tier());
        let roots = [dc.tier().root(), global.tier().root()];
        let cleanup = dc.clone();
        let stores = SettingsStores::new(Arc::new(global), Arc::new(dc)).unwrap();
        let env = Env {
            stores,
            clock: Arc::new(SystemClock),
        };
        let outcome = std::panic::AssertUnwindSafe(case(env)).catch_unwind().await;
        for root in &roots {
            delete_prefix(&cleanup, root).await;
        }
        if let Err(p) = outcome {
            std::panic::resume_unwind(p);
        }
    });
}

fn code(e: &SprigError) -> Option<SprigCode> {
    e.code()
}

async fn until(mut f: impl FnMut() -> bool) {
    tokio::time::timeout(WAIT, async {
        while !f() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("condition in time");
}

// ---- cases ----------------------------------------------------------------

async fn writes_are_checked_and_recorded(env: Env) {
    let w = env.writer();
    let gl = env.lead(&env.stores.global).await;
    let dl = env.lead(&env.stores.dc).await;
    let dc = env.dc();
    let set = |v: i64| SettingChange::Set(Value::Int(v));

    w.set(
        &dl,
        &dc,
        "test.cache_mb",
        set(3000),
        "op:kiran",
        "larger caches",
    )
    .await
    .unwrap();
    let (rec, _) = w.read(&dc, "test.cache_mb").await.unwrap().unwrap();
    assert_eq!(rec.schema_version(), 1);
    assert_eq!(rec.body.value, Some(Value::Int(3000)));
    assert_eq!(rec.body.set_by, "op:kiran");
    assert_eq!(rec.body.reason, "larger caches");
    assert!(rec.body.set_at.ends_with('Z'));

    // The stored JSON follows chapter 23's record layout.
    let key = env.stores.record_key(&dc, "test.cache_mb").unwrap();
    assert!(key.as_str().ends_with("/settings/test.cache_mb"));
    let raw = env
        .stores
        .dc
        .get(&key, sprig_meta::Consistency::Linearizable)
        .await
        .unwrap()
        .unwrap();
    let json: Json = serde_json::from_slice(&raw.value).unwrap();
    assert_eq!(json["v"], 1);
    assert_eq!(json["value"], 3000);

    let err = |r: sprig_common::Result<Vec<Warning>>| code(&r.unwrap_err());
    assert_eq!(
        err(w.set(&dl, &dc, "test.nope", set(1), "op", "").await),
        Some(SprigCode::SettingUnknown)
    );
    assert_eq!(
        err(w
            .set(&dl, &dc, "sprig.catalog_version", set(1), "op", "")
            .await),
        Some(SprigCode::SettingUnknown)
    );
    assert_eq!(
        err(w.set(&dl, &dc, "test.data_only", set(1), "op", "").await),
        Some(SprigCode::SettingScopeNotAllowed)
    );
    let cl = Scope::Cluster(env.cluster(1));
    assert_eq!(
        err(w.set(&dl, &cl, "test.port", set(1), "op", "").await),
        Some(SprigCode::SettingScopeNotAllowed)
    );
    assert_eq!(
        err(w
            .set(
                &dl,
                &dc,
                "test.cache_mb",
                SettingChange::Set("big".into()),
                "op",
                ""
            )
            .await),
        Some(SprigCode::SettingBadValue)
    );
    assert_eq!(
        err(w.set(&dl, &dc, "test.cache_mb", set(0), "op", "").await),
        Some(SprigCode::SettingOutOfBounds),
        "hard bounds apply"
    );
    assert_eq!(
        err(w
            .set(
                &dl,
                &dc,
                "test.mode",
                SettingChange::Set("maybe".into()),
                "op",
                ""
            )
            .await),
        Some(SprigCode::SettingOutOfBounds),
        "a value outside the hard allowed list"
    );
    // The wrong leader, another DC's scope, a cluster of another DC and a
    // tenant scope are bugs in the caller.
    assert_eq!(
        err(w
            .set(&dl, &Scope::Global, "test.cache_mb", set(1), "op", "")
            .await),
        Some(SprigCode::Internal)
    );
    let other_dc = Scope::Dc(sprig_common::ids::DcId::new("zz9").unwrap());
    assert_eq!(
        err(w
            .set(&dl, &other_dc, "test.cache_mb", set(1), "op", "")
            .await),
        Some(SprigCode::Internal)
    );
    let other_cluster = Scope::Cluster(ClusterId::new("zz9-c1").unwrap());
    assert_eq!(
        err(w
            .set(&dl, &other_cluster, "test.cache_mb", set(1), "op", "")
            .await),
        Some(SprigCode::Internal)
    );
    let tenant = Scope::Tenant(sprig_common::ids::TenantId(7));
    assert_eq!(
        err(w
            .set(&gl, &tenant, "test.data_only", set(1), "op", "")
            .await),
        Some(SprigCode::Internal)
    );

    // Fields a newer build added are kept.
    let mut obj = json.as_object().unwrap().clone();
    obj.insert("added_later".to_owned(), Json::from(true));
    env.stores
        .dc
        .txn(Txn {
            then: vec![Op::Put {
                key: key.clone(),
                value: serde_json::to_vec(&obj).unwrap(),
                lease: None,
            }],
            ..Txn::default()
        })
        .await
        .unwrap();
    w.set(&dl, &dc, "test.cache_mb", set(3500), "op:kiran", "")
        .await
        .unwrap();
    let (rec, _) = w.read(&dc, "test.cache_mb").await.unwrap().unwrap();
    assert_eq!(rec.body.value, Some(Value::Int(3500)));
    let raw = env
        .stores
        .dc
        .get(&key, sprig_meta::Consistency::Linearizable)
        .await
        .unwrap()
        .unwrap();
    let json: Json = serde_json::from_slice(&raw.value).unwrap();
    assert_eq!(json["added_later"], true);

    // Bounds-only records, and an empty record is deleted.
    w.set(
        &dl,
        &dc,
        "test.cache_mb",
        SettingChange::SetBounds(Bounds::max(9000.0)),
        "op",
        "",
    )
    .await
    .unwrap();
    w.set(&dl, &dc, "test.cache_mb", SettingChange::Reset, "op", "")
        .await
        .unwrap();
    let (rec, _) = w.read(&dc, "test.cache_mb").await.unwrap().unwrap();
    assert_eq!(rec.body.value, None);
    assert_eq!(rec.body.bounds, Some(Bounds::max(9000.0)));
    w.set(
        &dl,
        &dc,
        "test.cache_mb",
        SettingChange::ResetBounds,
        "op",
        "",
    )
    .await
    .unwrap();
    assert!(w.read(&dc, "test.cache_mb").await.unwrap().is_none());
    // Resetting what isn't set is fine.
    w.set(&dl, &dc, "test.cache_mb", SettingChange::Reset, "op", "")
        .await
        .unwrap();

    // Global writes go through the global leader.
    w.set(&gl, &Scope::Global, "test.port", set(3310), "op", "")
        .await
        .unwrap();
    assert!(
        env.stores
            .record_key(&Scope::Global, "test.port")
            .unwrap()
            .starts_with(&env.stores.global.tier().root())
    );
    gl.resign().await.unwrap();
    dl.resign().await.unwrap();
}

async fn mirrors_follow_writes_and_notify(env: Env) {
    let w = env.writer();
    let gl = env.lead(&env.stores.global).await;
    let dl = env.lead(&env.stores.dc).await;
    let dc_id = env.stores.dc_id().clone();
    let dc = env.dc();
    let c1 = env.cluster(1);
    let set = |v: i64| SettingChange::Set(Value::Int(v));

    // A row written before the mirrors start is in the first snapshot.
    w.set(&gl, &Scope::Global, "test.port", set(3310), "op", "")
        .await
        .unwrap();
    let live = LiveSettings::start(registry(), env.stores.clone()).unwrap();
    tokio::time::timeout(WAIT, live.ready()).await.unwrap();
    let ctx = SettingCtx::process(&dc_id);
    let on_c1 = SettingCtx {
        cluster: Some(&c1),
        ..ctx
    };
    assert_eq!(live.get(&PORT, &ctx), 3310);
    assert_eq!(live.get(&CACHE_MB, &ctx), 2000);

    let mut cache = live.watch(&CACHE_MB);
    let mut mode = live.subscribe(&["test.mode"]);
    let all_test = live.subscribe(&["test.*"]);
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&calls);
    let dc_for_cb = dc_id.clone();
    let reg = live.on_change("test", &["test.cache_mb"], move |s| {
        seen.fetch_add(1, Ordering::SeqCst);
        if s.get(&CACHE_MB, &SettingCtx::process(&dc_for_cb)) == 13 {
            Err("13 can't be applied".to_owned())
        } else {
            Ok(())
        }
    });

    w.set(&dl, &dc, "test.cache_mb", set(3000), "op", "")
        .await
        .unwrap();
    tokio::time::timeout(WAIT, cache.changed())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(live.get(&CACHE_MB, &ctx), 3000);
    let r = live.resolve("test.cache_mb", &ctx).unwrap();
    assert_eq!(r.source, ScopeKind::Dc);
    assert!(all_test.has_changed().unwrap());
    assert!(
        !mode.has_changed().unwrap(),
        "other keys' watches stay quiet"
    );
    until(|| calls.load(Ordering::SeqCst) == 1).await;

    // Nearest scope wins: global under dc, cluster over dc for work on it.
    w.set(&gl, &Scope::Global, "test.cache_mb", set(4000), "op", "")
        .await
        .unwrap();
    w.set(
        &dl,
        &Scope::Cluster(c1.clone()),
        "test.cache_mb",
        set(5000),
        "op",
        "",
    )
    .await
    .unwrap();
    until(|| live.get(&CACHE_MB, &on_c1) == 5000).await;
    assert_eq!(live.get(&CACHE_MB, &ctx), 3000);
    w.set(&dl, &dc, "test.cache_mb", SettingChange::Reset, "op", "")
        .await
        .unwrap();
    until(|| live.get(&CACHE_MB, &ctx) == 4000).await;
    assert_eq!(live.get(&CACHE_MB, &on_c1), 5000);

    w.set(
        &dl,
        &dc,
        "test.mode",
        SettingChange::Set("off".into()),
        "op",
        "",
    )
    .await
    .unwrap();
    tokio::time::timeout(WAIT, mode.changed())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(live.get(&MODE, &ctx), "off");

    // A callback that fails keeps running for later changes.
    let before = calls.load(Ordering::SeqCst);
    w.set(&dl, &dc, "test.cache_mb", set(13), "op", "")
        .await
        .unwrap();
    until(|| calls.load(Ordering::SeqCst) == before + 1).await;
    w.set(&dl, &dc, "test.cache_mb", set(14), "op", "")
        .await
        .unwrap();
    until(|| calls.load(Ordering::SeqCst) == before + 2).await;

    // Dropping the registration stops the callback.
    drop(reg);
    w.set(&dl, &dc, "test.cache_mb", set(15), "op", "")
        .await
        .unwrap();
    until(|| live.get(&CACHE_MB, &ctx) == 15).await;
    assert_eq!(calls.load(Ordering::SeqCst), before + 2);

    // Records the registry doesn't know (internal keys) are skipped.
    env.stores
        .global
        .txn(Txn {
            then: vec![Op::Put {
                key: env
                    .stores
                    .record_key(&Scope::Global, "system.deployment_id")
                    .unwrap(),
                value: br#"{"v":1,"value":"dep-1","bounds":{"locked":true}}"#.to_vec(),
                lease: None,
            }],
            ..Txn::default()
        })
        .await
        .unwrap();
    w.set(&gl, &Scope::Global, "test.port", set(3311), "op", "")
        .await
        .unwrap();
    until(|| live.get(&PORT, &ctx) == 3311).await;
    gl.resign().await.unwrap();
    dl.resign().await.unwrap();
}

async fn tightened_bounds_warn_and_clamp(env: Env) {
    let w = env.writer();
    let gl = env.lead(&env.stores.global).await;
    let dl = env.lead(&env.stores.dc).await;
    let dc_id = env.stores.dc_id().clone();
    let dc = env.dc();
    let c2 = env.cluster(2);
    let set = |v: i64| SettingChange::Set(Value::Int(v));
    let live = LiveSettings::start(registry(), env.stores.clone()).unwrap();
    tokio::time::timeout(WAIT, live.ready()).await.unwrap();
    let ctx = SettingCtx::process(&dc_id);

    w.set(&dl, &dc, "test.cache_mb", set(100), "op", "")
        .await
        .unwrap();
    w.set(
        &dl,
        &Scope::Cluster(c2.clone()),
        "test.cache_mb",
        set(40),
        "op",
        "",
    )
    .await
    .unwrap();
    w.set(
        &dl,
        &Scope::Cluster(env.cluster(3)),
        "test.cache_mb",
        set(80),
        "op",
        "",
    )
    .await
    .unwrap();
    let warnings = w
        .set(
            &gl,
            &Scope::Global,
            "test.cache_mb",
            SettingChange::SetBounds(Bounds::max(50.0)),
            "op",
            "tighter",
        )
        .await
        .unwrap();
    let mut flagged: Vec<Scope> = warnings.iter().map(|w| w.scope.clone()).collect();
    flagged.sort();
    assert_eq!(flagged, vec![dc.clone(), Scope::Cluster(env.cluster(3))]);
    assert!(warnings[0].message.contains("clamped"));

    // Clamped at read time, flagged in the resolution.
    until(|| live.resolve("test.cache_mb", &ctx).unwrap().value == Value::Int(50)).await;
    let r = live.resolve("test.cache_mb", &ctx).unwrap();
    assert!(r.clamped);
    assert_eq!(r.source, ScopeKind::Dc);

    // New writes are checked against the tighter bounds.
    let e = w.set(&dl, &dc, "test.cache_mb", set(60), "op", "").await;
    assert_eq!(code(&e.unwrap_err()), Some(SprigCode::SettingOutOfBounds));
    let e = w
        .set(
            &dl,
            &Scope::Cluster(c2.clone()),
            "test.cache_mb",
            set(60),
            "op",
            "",
        )
        .await;
    assert_eq!(code(&e.unwrap_err()), Some(SprigCode::SettingOutOfBounds));
    // dc bounds also limit clusters.
    w.set(
        &dl,
        &dc,
        "test.cache_mb",
        SettingChange::SetBounds(Bounds::max(30.0)),
        "op",
        "",
    )
    .await
    .unwrap();
    let e = w
        .set(
            &dl,
            &Scope::Cluster(c2.clone()),
            "test.cache_mb",
            set(35),
            "op",
            "",
        )
        .await;
    assert_eq!(code(&e.unwrap_err()), Some(SprigCode::SettingOutOfBounds));

    // Locking refuses lower values and ignores the stored ones.
    let warnings = w
        .set(
            &gl,
            &Scope::Global,
            "test.cache_mb",
            SettingChange::SetBounds(Bounds::locked()),
            "op",
            "",
        )
        .await
        .unwrap();
    assert!(warnings.iter().any(|w| w.message.contains("locked")));
    let e = w.set(&dl, &dc, "test.cache_mb", set(20), "op", "").await;
    assert_eq!(code(&e.unwrap_err()), Some(SprigCode::SettingLocked));
    until(|| live.get(&CACHE_MB, &ctx) == 2000).await;

    // Loosening restores the stored value.
    w.set(
        &gl,
        &Scope::Global,
        "test.cache_mb",
        SettingChange::ResetBounds,
        "op",
        "",
    )
    .await
    .unwrap();
    until(|| live.get(&CACHE_MB, &ctx) == 100).await;
    gl.resign().await.unwrap();
    dl.resign().await.unwrap();
}

async fn a_deposed_leader_cannot_write(env: Env) {
    let w = env.writer();
    let dl = env.lead(&env.stores.dc).await;
    let dc = env.dc();
    dl.resign().await.unwrap();
    let e = w
        .set(
            &dl,
            &dc,
            "test.cache_mb",
            SettingChange::Set(Value::Int(10)),
            "op",
            "",
        )
        .await
        .unwrap_err();
    assert!(!e.retryable);
    assert!(w.read(&dc, "test.cache_mb").await.unwrap().is_none());

    // A new leader's token is checked: the old one's writes fail.
    let next = env.lead(&env.stores.dc).await;
    w.set(
        &next,
        &dc,
        "test.cache_mb",
        SettingChange::Set(Value::Int(11)),
        "op",
        "",
    )
    .await
    .unwrap();
    assert!(
        w.set(
            &dl,
            &dc,
            "test.cache_mb",
            SettingChange::Set(Value::Int(12)),
            "op",
            ""
        )
        .await
        .is_err()
    );
    let (rec, _) = w.read(&dc, "test.cache_mb").await.unwrap().unwrap();
    assert_eq!(rec.body.value, Some(Value::Int(11)));
    next.resign().await.unwrap();
}

/// The Admin API's path: writes under the serving leader's fencing compare
/// (the DC leader's, also for `global` in single-etcd mode), bounds
/// patched inside the guarded write, outcomes with the records before and
/// after, and a guard that no longer holds.
async fn guarded_writes_and_bound_patches(env: Env) {
    let w = env.writer();
    let dl = env.lead(&env.stores.dc).await;
    let guard = vec![Cmp::CreateRev(dl.key().clone(), dl.token)];
    let dc = env.dc();
    let key = "test.cache_mb";
    let set = |v: i64| SettingEdit::Change(SettingChange::Set(Value::Int(v)));
    let patch = |p: BoundsPatch| SettingEdit::PatchBounds(p);

    let o = w
        .set_guarded(&guard, &Scope::Global, key, set(3000), "op:kiran", "r1")
        .await
        .unwrap();
    assert!(o.changed);
    assert_eq!(o.previous, None);
    assert_eq!(o.current.as_ref().unwrap().value, Some(Value::Int(3000)));
    w.set_guarded(&guard, &dc, key, set(4000), "op:kiran", "r2")
        .await
        .unwrap();
    // A maximum on global keeps the value and warns about the dc value.
    let o = w
        .set_guarded(
            &guard,
            &Scope::Global,
            key,
            patch(BoundsPatch {
                max: Some(Some(3500.0)),
                ..BoundsPatch::default()
            }),
            "op:kiran",
            "cap",
        )
        .await
        .unwrap();
    let cur = o.current.unwrap();
    assert_eq!(cur.value, Some(Value::Int(3000)));
    assert_eq!(cur.bounds.as_ref().unwrap().max, Some(3500.0));
    assert_eq!(o.warnings.len(), 1, "{:?}", o.warnings);
    assert_eq!(o.warnings[0].scope, dc);
    // Lock keeps the maximum.
    let o = w
        .set_guarded(
            &guard,
            &Scope::Global,
            key,
            patch(BoundsPatch {
                locked: Some(true),
                ..BoundsPatch::default()
            }),
            "op:kiran",
            "freeze",
        )
        .await
        .unwrap();
    let b = o.current.unwrap().bounds.unwrap();
    assert!(b.locked);
    assert_eq!(b.max, Some(3500.0));
    assert!(o.warnings[0].message.contains("locked"), "{:?}", o.warnings);
    let e = w
        .set_guarded(&guard, &dc, key, set(100), "op", "")
        .await
        .unwrap_err();
    assert_eq!(code(&e), Some(SprigCode::SettingLocked));
    // Unlock and clear the maximum: nothing left of the bounds.
    let o = w
        .set_guarded(
            &guard,
            &Scope::Global,
            key,
            patch(BoundsPatch {
                locked: Some(false),
                max: Some(None),
                ..BoundsPatch::default()
            }),
            "op:kiran",
            "thaw",
        )
        .await
        .unwrap();
    assert_eq!(o.current.unwrap().bounds, None);
    // Out of bounds, wrong type, unknown, wrong scope.
    w.set_guarded(
        &guard,
        &Scope::Global,
        key,
        patch(BoundsPatch {
            max: Some(Some(5000.0)),
            ..BoundsPatch::default()
        }),
        "op",
        "",
    )
    .await
    .unwrap();
    let err = |r: sprig_common::Result<RecordOutcome>| code(&r.unwrap_err());
    assert_eq!(
        err(w.set_guarded(&guard, &dc, key, set(6000), "op", "").await),
        Some(SprigCode::SettingOutOfBounds)
    );
    assert_eq!(
        err(w
            .set_guarded(
                &guard,
                &dc,
                key,
                SettingEdit::Change(SettingChange::Set("big".into())),
                "op",
                ""
            )
            .await),
        Some(SprigCode::SettingBadValue)
    );
    assert_eq!(
        err(w
            .set_guarded(&guard, &dc, "test.nope", set(1), "op", "")
            .await),
        Some(SprigCode::SettingUnknown)
    );
    assert_eq!(
        err(w
            .set_guarded(&guard, &dc, "test.data_only", set(1), "op", "")
            .await),
        Some(SprigCode::SettingScopeNotAllowed)
    );
    // A reset of nothing changes nothing.
    let o = w
        .set_guarded(
            &guard,
            &Scope::Cluster(env.cluster(4)),
            key,
            SettingEdit::Change(SettingChange::Reset),
            "op",
            "",
        )
        .await
        .unwrap();
    assert!(!o.changed);
    // A guard that no longer holds: not the leader.
    let stale = vec![Cmp::CreateRev(dl.key().clone(), dl.token + 1_000_000)];
    assert_eq!(
        err(w.set_guarded(&stale, &dc, key, set(10), "op", "").await),
        Some(SprigCode::NotLeader)
    );
    let (rec, _) = w.read(&dc, key).await.unwrap().unwrap();
    assert_eq!(rec.body.value, Some(Value::Int(4000)));
}
