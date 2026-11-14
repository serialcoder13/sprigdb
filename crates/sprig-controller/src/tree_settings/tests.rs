//! The `tree_settings` loop over the in-memory catalog and a `MemStore`
//! directory: the `a → b → c` example (if b changes later and c didn't
//! override, c gets it), bounds
//! set by an ancestor, moves, gaps, unsupported modes, which directory
//! events count, and the loop end to end: a change at an ancestor reaches
//! a descendant within seconds.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

use std::time::Instant;

use sprig_catalog::cache::mem::MemSource;
use sprig_catalog::cache::{Loaded, TenantCatalog};
use sprig_catalog::store::SettingScope;
use sprig_catalog::{
    AllowAll, DirectoryAllocator, FixedEtcd, LogDecisions, Principal, SetRequest, Target,
    TenantSettingsWriter,
};
use sprig_common::ids::UserId;
use sprig_common::time::SystemClock;
use sprig_meta::elect::LeaderRecord;
use sprig_meta::{KeyValue, MemStore, MetaStore, Tier};
use sprig_settings::{
    AnySetting, Apply, Bounds, HardBounds, MemSettings, Registry, Resolved, ScopeKind, Scopes,
    Setting, SettingChange, Value,
};

use super::*;
use crate::runtime::{Loop, Runtime, Source, Stores};
use crate::tree::NewTenant;

static LOCK_MINUTES: Setting<i64> = Setting {
    key: "auth.password_lock_minutes",
    default: 0,
    scopes: Scopes::TREE,
    hard: HardBounds::range(-1, 1_000_000),
    apply: Apply::Live,
    operator_only: false,
    owner: 14,
    doc: "Temporary lock length; -1 = until unlocked",
};
static ATTEMPTS: Setting<u32> = Setting {
    key: "auth.failed_login_attempts",
    default: 0,
    scopes: Scopes::TREE,
    hard: HardBounds::range(0, 32_767),
    apply: Apply::Live,
    operator_only: false,
    owner: 14,
    doc: "Failed logins before a lock; 0 = never",
};
static MFA: Setting<bool> = Setting {
    key: "auth.mfa_required",
    default: false,
    scopes: Scopes::TREE,
    hard: HardBounds::NONE,
    apply: Apply::Live,
    operator_only: false,
    owner: 14,
    doc: "Require a second factor",
};

fn registry() -> &'static Registry {
    static R: std::sync::OnceLock<Registry> = std::sync::OnceLock::new();
    R.get_or_init(|| {
        Registry::new([
            &LOCK_MINUTES as &dyn AnySetting,
            &ATTEMPTS,
            &MFA,
            &ANCESTRY_MODE,
        ])
        .unwrap()
    })
}

fn dc() -> DcId {
    DcId::new("mum1").unwrap()
}

struct Env {
    store: MemStore,
    dir: Directory,
    sync: Arc<TreeSync<MemSource>>,
    writer: TenantSettingsWriter<MemSource>,
    etcd: MemSettings<'static>,
}

impl Env {
    fn new() -> Self {
        let store = MemStore::new(Tier::Dc(dc()));
        let global: Arc<dyn MetaStore> = Arc::new(store.with_tier(Tier::Global));
        let dir = Directory::new(Arc::clone(&global));
        let v = Versioner::new(
            MemSource::new(),
            Arc::new(DirectoryAllocator::new(Arc::clone(&global))),
        );
        let sync = Arc::new(TreeSync::new(v.clone(), dir.clone(), Arc::new(SystemClock)));
        let writer = TenantSettingsWriter::new(
            v,
            registry(),
            Arc::new(FixedEtcd {
                rows: Arc::new(MemSettings::new(registry())),
                dc: dc(),
            }),
            Arc::new(AllowAll),
            Arc::new(LogDecisions),
            Arc::new(SystemClock),
        );
        Self {
            store,
            dir,
            sync,
            writer,
            etcd: MemSettings::new(registry()),
        }
    }

    fn mem(&self) -> &MemSource {
        self.sync.versioner().backend()
    }

    /// Creates `name` under `parent`, active, with an empty catalog.
    async fn tenant(&self, name: &str, parent: Option<TenantId>) -> TenantId {
        let e = self
            .dir
            .create(
                &NewTenant {
                    name: name.to_owned(),
                    parent,
                    home_dc: dc(),
                    placement_hint: None,
                },
                64,
                std::time::SystemTime::now(),
            )
            .await
            .unwrap();
        let id = e.body().tenant_id;
        self.mem().bootstrap(id);
        self.dir
            .update(id, |e| {
                e.state = TenantState::Active;
                Ok(true)
            })
            .await
            .unwrap();
        id
    }

    async fn set(&self, t: TenantId, key: &str, change: SettingChange) {
        self.writer
            .set(&SetRequest {
                tenant: t,
                target: Target::Tenant,
                key: key.to_owned(),
                change,
                principal: Principal::Operator("test".to_owned()),
                reason: "test".to_owned(),
            })
            .await
            .unwrap();
    }

    async fn sync(&self, t: TenantId) -> SyncOutcome {
        self.sync.sync(t).await.unwrap().unwrap()
    }

    /// What `t` resolves for `key` from its own catalog snapshot.
    fn resolve(&self, t: TenantId, key: &str) -> Resolved {
        let tables = self.mem().tables(t).unwrap();
        let cat = TenantCatalog::build(registry(), t, Loaded::core(tables.snapshot(None)));
        cat.settings
            .resolve(&self.etcd, key, &cat.setting_ctx(&dc()))
            .unwrap()
    }
}

fn int(i: i64) -> SettingChange {
    SettingChange::Set(Value::Int(i))
}

#[tokio::test]
async fn a_b_c_nearest_level_wins_also_for_later_changes() {
    let env = Env::new();
    let a = env.tenant("aa", None).await;
    let b = env.tenant("bb", Some(a)).await;
    let c = env.tenant("cc", Some(b)).await;
    let (lm, fa, mfa) = (LOCK_MINUTES.key, ATTEMPTS.key, MFA.key);
    env.set(a, lm, int(30)).await;
    env.set(a, fa, int(5)).await;
    env.set(b, lm, int(10)).await;
    env.set(c, fa, int(3)).await;
    for t in [a, b, c] {
        assert!(matches!(env.sync(t).await, SyncOutcome::Copied { .. }));
        assert_eq!(
            env.sync(t).await,
            SyncOutcome::Current,
            "a second pass copies nothing"
        );
    }
    // c sees a overlaid by b overlaid by c.
    let r = env.resolve(c, lm);
    assert_eq!(
        (r.value, r.source),
        (Value::Int(10), ScopeKind::Ancestor(b))
    );
    let r = env.resolve(c, fa);
    assert_eq!((r.value, r.source), (Value::Int(3), ScopeKind::Tenant));
    let r = env.resolve(b, fa);
    assert_eq!((r.value, r.source), (Value::Int(5), ScopeKind::Ancestor(a)));
    assert_eq!(env.resolve(a, lm).source, ScopeKind::Tenant);
    let cat = env.mem().tables(c).unwrap();
    assert!(
        cat.settings
            .contains_key(&(SettingScope::Ancestor(a), lm.to_owned()))
    );

    // b changes later: what c didn't override follows b; what c overrode
    // stays c's. Until the loop runs, c still reads b's previous rows.
    env.set(b, lm, int(20)).await;
    env.set(b, fa, int(4)).await;
    env.set(b, mfa, SettingChange::Set(Value::Bool(true))).await;
    assert_eq!(env.resolve(c, lm).value, Value::Int(10));
    let SyncOutcome::Copied { report, .. } = env.sync(c).await else {
        panic!("b's change must be copied");
    };
    assert_eq!(
        report.copied,
        vec![b],
        "only the ancestor that changed is read"
    );
    assert_eq!(env.resolve(c, lm).value, Value::Int(20));
    assert_eq!(env.resolve(c, fa).value, Value::Int(3));
    assert_eq!(env.resolve(c, mfa).value, Value::Bool(true));

    // a's bounds bind every descendant: c reads b's 20 clamped to 15.
    env.set(a, lm, SettingChange::SetBounds(Bounds::max(15.0)))
        .await;
    env.sync(b).await;
    env.sync(c).await;
    let r = env.resolve(c, lm);
    assert_eq!(
        (r.value, r.source, r.clamped),
        (Value::Int(15), ScopeKind::Ancestor(b), true)
    );
    // A scope's own bounds limit only narrower scopes: with b's value
    // gone, c reads a's 30 unclamped.
    env.set(b, lm, SettingChange::Reset).await;
    env.sync(c).await;
    let r = env.resolve(c, lm);
    assert_eq!(
        (r.value, r.source, r.clamped),
        (Value::Int(30), ScopeKind::Ancestor(a), false)
    );
    // c's own value obeys a's bound at read time.
    env.writer
        .set(&SetRequest {
            tenant: c,
            target: Target::Tenant,
            key: lm.to_owned(),
            change: int(40),
            principal: Principal::User(UserId(1)),
            reason: String::new(),
        })
        .await
        .unwrap_err();
}

#[tokio::test]
async fn a_moved_subtree_drops_its_old_ancestors_rows() {
    let env = Env::new();
    let a = env.tenant("aa", None).await;
    let b = env.tenant("bb", Some(a)).await;
    let c = env.tenant("cc", Some(b)).await;
    env.set(a, LOCK_MINUTES.key, int(30)).await;
    env.set(b, LOCK_MINUTES.key, int(10)).await;
    env.sync(c).await;
    assert_eq!(env.resolve(c, LOCK_MINUTES.key).value, Value::Int(10));
    // c moves under a (parent links are the truth; the path follows).
    env.dir
        .update(c, |e| {
            e.parent_tenant_id = a.0;
            e.path = format!("/{a}/{c}/");
            e.depth = 1;
            Ok(true)
        })
        .await
        .unwrap();
    let SyncOutcome::Copied { report, .. } = env.sync(c).await else {
        panic!("the move must be applied");
    };
    assert_eq!(report.removed, vec![b]);
    let r = env.resolve(c, LOCK_MINUTES.key);
    assert_eq!(
        (r.value, r.source),
        (Value::Int(30), ScopeKind::Ancestor(a))
    );
    let rows = env.mem().tables(c).unwrap();
    let old = &rows.settings[&(SettingScope::Ancestor(b), LOCK_MINUTES.key.to_owned())];
    assert!(old.is_empty(), "the old ancestor's row is a tombstone");
}

#[tokio::test]
async fn a_gap_at_an_ancestor_is_settled_before_copying() {
    let env = Env::new();
    let a = env.tenant("aa", None).await;
    let b = env.tenant("bb", Some(a)).await;
    env.set(a, LOCK_MINUTES.key, int(30)).await;
    env.sync(b).await;
    // A tenant-scope change at a allocated its version and crashed before
    // committing: the directory's settings_version is ahead of a's catalog.
    let committed = env.mem().tables(a).unwrap().versions.catalog;
    let gap = env
        .sync
        .versioner()
        .allocator()
        .allocate(a, VersionKind::Catalog, committed, true)
        .await
        .unwrap();
    assert!(matches!(env.sync(b).await, SyncOutcome::Copied { .. }));
    assert_eq!(
        env.mem().tables(a).unwrap().versions.catalog,
        gap,
        "settled"
    );
    let vs = env.mem().tables(b).unwrap().settings
        [&(SettingScope::Tenant, ANCESTOR_VERSIONS_KEY.to_owned())]
        .value
        .clone()
        .unwrap();
    assert_eq!(vs[a.0.to_string()], gap);
}

#[tokio::test]
async fn unsupported_ancestry_modes_block_instead_of_copying() {
    let env = Env::new();
    let a = env.tenant("aa", None).await;
    let b = env.tenant("bb", Some(a)).await;
    env.set(
        a,
        ANCESTRY_MODE.key,
        SettingChange::Set(Value::from("shared")),
    )
    .await;
    assert_eq!(
        env.sync(b).await,
        SyncOutcome::Blocked("tenant.ancestry_mode = shared is not implemented yet")
    );
    // copy (and auto) proceed.
    env.set(
        a,
        ANCESTRY_MODE.key,
        SettingChange::Set(Value::from("copy")),
    )
    .await;
    assert!(matches!(env.sync(b).await, SyncOutcome::Copied { .. }));
}

fn event(store: &MemStore, dir: &Directory, e: &StoredEntry) -> MetaEvent {
    let key = dir.key(e.body().tenant_id).unwrap();
    let _ = store;
    MetaEvent {
        source: Source::Global,
        key: key.clone(),
        kv: Some(KeyValue {
            key,
            value: e.record.encode().unwrap(),
            create_revision: 1,
            mod_revision: e.mod_revision,
            version: 1,
            lease: None,
        }),
        mod_revision: e.mod_revision,
    }
}

#[tokio::test]
async fn only_settings_path_and_parent_changes_fan_out() {
    let env = Env::new();
    let a = env.tenant("aa", None).await;
    let b = env.tenant("bb", Some(a)).await;
    let c = env.tenant("cc", Some(b)).await;
    let other = env.tenant("other", None).await;
    let lp = TreeSettings::new(Arc::clone(&env.sync), dc(), None).unwrap();
    lp.ready().await;
    let rev = env.dir.get(other).await.unwrap().unwrap().mod_revision;
    lp.mirror.wait_for(rev).await;
    let eb = env.dir.get(b).await.unwrap().unwrap();
    let ev = event(&env.store, &env.dir, &eb);
    let mut keys = lp.keys_for(&ev);
    keys.sort();
    assert_eq!(keys, vec![b, c], "first sight counts as a change");
    assert!(lp.keys_for(&ev).is_empty());
    // A catalog change moves catalog_version only.
    let mut moved = eb.clone();
    moved.record.body.catalog_version += 5;
    assert!(lp.keys_for(&event(&env.store, &env.dir, &moved)).is_empty());
    moved.record.body.settings_version += 1;
    let mut keys = lp.keys_for(&event(&env.store, &env.dir, &moved));
    keys.sort();
    assert_eq!(keys, vec![b, c]);
    // Tenants homed elsewhere, deleted or failed are not ours.
    let mut elsewhere = env.dir.get(other).await.unwrap().unwrap();
    elsewhere.record.body.home_dc = DcId::new("ams1").unwrap();
    assert!(
        lp.keys_for(&event(&env.store, &env.dir, &elsewhere))
            .is_empty()
    );
    let mut deleted = MetaEvent {
        kv: None,
        ..event(&env.store, &env.dir, &eb)
    };
    assert!(lp.keys_for(&deleted).is_empty());
    deleted.key = env.dir.prefix().unwrap();
    assert!(lp.keys_for(&deleted).is_empty());
    assert_eq!(lp.workers(), 16);
    let (stores, clock) = (
        Stores {
            dc: Arc::new(env.store.clone()),
            global: Arc::clone(env.dir.store()),
        },
        Arc::new(SystemClock) as Arc<dyn Clock>,
    );
    let (cx, _stop) = crate::runtime::tests::cx_for(
        &stores,
        &clock,
        None,
        crate::settings::ControlSettings::default(),
    );
    let mut all = lp.all_keys(&cx).await.unwrap();
    all.sort();
    assert_eq!(all, vec![a, b, c, other]);
}

/// The loop end to end, on a leader-elected runtime: a change at `a`
/// reaches `c`'s catalog within seconds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_loop_copies_an_ancestors_change_within_seconds() {
    let env = Env::new();
    let a = env.tenant("aa", None).await;
    let b = env.tenant("bb", Some(a)).await;
    let c = env.tenant("cc", Some(b)).await;
    let stores = Stores {
        dc: Arc::new(env.store.clone()),
        global: Arc::clone(env.dir.store()),
    };
    let lp = TreeSettings::new(Arc::clone(&env.sync), dc(), None).unwrap();
    let rt = Runtime::new(
        stores,
        Arc::new(SystemClock),
        LeaderRecord {
            holder: "test".into(),
            dc: Some(dc().to_string()),
            build: "test".into(),
        },
    )
    .with_loop(Loop::new(lp));
    let stop = sprig_common::CancellationToken::new();
    let run = tokio::spawn(rt.run(stop.clone()));
    // The first resync brings every catalog up to date.
    let wait = |what: &'static str, f: Box<dyn Fn() -> bool + Send>| async move {
        let t0 = Instant::now();
        while !f() {
            assert!(t0.elapsed() < Duration::from_secs(10), "timed out: {what}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        t0.elapsed()
    };
    let mem = env.sync.versioner().backend();
    let has_ancestry = |t: TenantId| {
        mem.tables(t).is_some_and(|tb| {
            tb.settings
                .contains_key(&(SettingScope::Tenant, ANCESTOR_VERSIONS_KEY.to_owned()))
        })
    };
    let first = {
        let mem2 = Arc::clone(&env.sync);
        wait(
            "first sync",
            Box::new(move || {
                let m = mem2.versioner().backend();
                [a, b, c].iter().all(|t| {
                    m.tables(*t).is_some_and(|tb| {
                        tb.settings
                            .contains_key(&(SettingScope::Tenant, ANCESTOR_VERSIONS_KEY.to_owned()))
                    })
                })
            }),
        )
        .await
    };
    assert!(has_ancestry(c));
    println!("first sync of a, b and c took {first:?}");
    env.set(a, LOCK_MINUTES.key, int(45)).await;
    let took = {
        let s = Arc::clone(&env.sync);
        wait(
            "a's change reaches c",
            Box::new(move || {
                s.versioner().backend().tables(c).is_some_and(|tb| {
                    tb.settings
                        .get(&(SettingScope::Ancestor(a), LOCK_MINUTES.key.to_owned()))
                        .is_some_and(|r| r.value == Some(serde_json::json!(45)))
                })
            }),
        )
        .await
    };
    println!("a's change reached c in {took:?}");
    assert!(took < Duration::from_secs(5));
    assert_eq!(env.resolve(c, LOCK_MINUTES.key).value, Value::Int(45));
    stop.cancel();
    run.await.unwrap();
}
