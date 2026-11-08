//! Setting writes (chapter 08 step 7) over the in-memory catalog and a
//! `MemStore` directory: each check and its error, where bounds come from,
//! the row a write leaves, warnings and decisions, and the M0.3 "done
//! when" at the library level: an object-scope change takes effect within
//! seconds, and a value outside its bounds is refused.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::borrow::Cow;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::json;
use sprig_common::ErrorKind;
use sprig_common::ids::{DbId, DcId, GroupId, ObjectId, TenantId, UserId};
use sprig_common::time::SystemClock;
use sprig_meta::{Consistency, MemStore, MetaStore, Op, Tier, Txn};
use sprig_settings::{
    AnySetting, Apply, Bounds, HardBounds, Level, MemSettings, OwnedCtx, Registry, Scopes, Setting,
    SettingChange, SettingCtx, Settings, SettingsStores, Value,
};

use super::*;
use crate::ancestry::{ANCESTOR_VERSIONS_KEY, AncestorRows, apply as apply_ancestry};
use crate::cache::mem::MemSource;
use crate::cache::{
    CacheConfig, CatalogSource, DirectoryWatch, Freshness, Loaded, Node, TenantCache,
    TenantCatalog, TenantRef,
};
use crate::store::{
    DatabaseRow, DbState, Namespace, ObjectFlags, ObjectKind, ObjectRow, ObjectState,
};
use crate::version::DirectoryAllocator;

static MAX_PER_TABLE: Setting<u32> = Setting {
    key: "auto_index.max_per_table",
    default: 5,
    scopes: Scopes::ALL,
    hard: HardBounds::range(1, 64),
    apply: Apply::Live,
    operator_only: false,
    owner: 13,
    doc: "Automatic indexes per table",
};
static MODE: Setting<Cow<'static, str>> = Setting {
    key: "auto_index.mode",
    default: Cow::Borrowed("recommend"),
    scopes: Scopes::ALL,
    hard: HardBounds::one_of(&["off", "recommend", "on"]),
    apply: Apply::Live,
    operator_only: false,
    owner: 13,
    doc: "off, recommend or on",
};
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
static MAX_DATABASES: Setting<u64> = Setting {
    key: "catalog.max_databases",
    default: 1000,
    scopes: Scopes(0b10_0100),
    hard: HardBounds::range(1, 1_000_000),
    apply: Apply::Live,
    operator_only: true,
    owner: 8,
    doc: "Databases per tenant",
};

/// A key in the process-wide registry of this test binary, for the
/// `Settings` (whose registry is the global one) in the "done when" test.
static OBJECT_LIMIT: Setting<u32> = Setting {
    key: "test_catalog.object_limit",
    default: 10,
    scopes: Scopes::ALL,
    hard: HardBounds::range(1, 1000),
    apply: Apply::Live,
    operator_only: false,
    owner: 8,
    doc: "A test key at every scope",
};
sprig_settings::register!(OBJECT_LIMIT);

fn registry() -> &'static Registry {
    static R: OnceLock<Registry> = OnceLock::new();
    R.get_or_init(|| {
        Registry::new([
            &MAX_PER_TABLE as &dyn AnySetting,
            &MODE,
            &LOCK_MINUTES,
            &MAX_DATABASES,
        ])
        .unwrap()
    })
}

const T: TenantId = TenantId(42);
const PARENT: TenantId = TenantId(7);
const SHOP: DbId = DbId(1);
const LOGS: DbId = DbId(2);
const ORDERS: ObjectId = ObjectId(11);
const EVENTS: ObjectId = ObjectId(12);
const DROPPED: ObjectId = ObjectId(13);
const SECRET: ObjectId = ObjectId(14);

fn at() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_762_560_000)
}

fn db(id: DbId, name: &str, v: u64) -> DatabaseRow {
    DatabaseRow {
        db_id: id,
        name: name.to_owned(),
        name_key: name.to_owned(),
        default_charset: "utf8mb4".to_owned(),
        default_collation: "utf8mb4_bin".to_owned(),
        default_group_id: GroupId(0),
        state: DbState::Active,
        options: json!({}),
        created_at: at(),
        created_by: None,
        changed_version: v,
    }
}

fn obj(id: ObjectId, d: DbId, name: &str, state: ObjectState, v: u64) -> ObjectRow {
    ObjectRow {
        object_id: id,
        db_id: d,
        namespace: Namespace::Relation,
        name: name.to_owned(),
        name_key: name.to_owned(),
        kind: ObjectKind::Table,
        parent_id: None,
        state,
        schema_version: 1,
        flags: ObjectFlags::empty(),
        pending_job_id: None,
        created_at: at(),
        changed_version: v,
    }
}

/// Collects decisions.
#[derive(Default)]
struct Decisions(Mutex<Vec<SettingDecision>>);

#[async_trait::async_trait]
impl DecisionSink for Decisions {
    async fn record(&self, d: &SettingDecision) -> sprig_common::Result<()> {
        self.0.lock().unwrap().push(d.clone());
        Ok(())
    }
}

/// Records what it was asked; refuses `AlterObject(SECRET)`.
#[derive(Default)]
struct Auth(Mutex<Vec<Privilege>>);

#[async_trait::async_trait]
impl SettingAuthorizer for Auth {
    async fn authorize(
        &self,
        _: TenantId,
        _: &Principal,
        need: Privilege,
    ) -> sprig_common::Result<()> {
        self.0.lock().unwrap().push(need);
        if need == Privilege::AlterObject(SECRET) {
            return Err(SprigError::standard(1142, *b"42000", "ALTER command denied"));
        }
        Ok(())
    }
}

struct Env {
    global: Arc<MemStore>,
    writer: TenantSettingsWriter<MemSource>,
    decisions: Arc<Decisions>,
    auth: Arc<Auth>,
}

fn dc() -> DcId {
    DcId::new("mum1").unwrap()
}

/// etcd rows: global bounds max 8 on `max_per_table`, dc locks
/// `password_lock_minutes`.
fn etcd_rows(registry: &'static Registry) -> MemSettings<'static> {
    let mut m = MemSettings::new(registry);
    let d = dc();
    let ctx = SettingCtx::process(&d);
    if registry.get(MAX_PER_TABLE.key).is_some() {
        m.set_bounds(Level::Global, &ctx, MAX_PER_TABLE.key, Bounds::max(8.0))
            .unwrap();
        m.set_bounds(Level::Dc, &ctx, LOCK_MINUTES.key, Bounds::locked())
            .unwrap();
    }
    m
}

async fn seed_directory(global: &dyn MetaStore, t: TenantId) {
    let key = global.tier().key(&format!("tenants/{t}")).unwrap();
    global
        .txn(Txn {
            then: vec![Op::Put {
                key,
                value: serde_json::to_vec(&json!({"v": 1, "tenant_id": t.0, "name": "t",
                    "state": "active", "catalog_version": 0, "auth_version": 0}))
                .unwrap(),
                lease: None,
            }],
            ..Txn::default()
        })
        .await
        .unwrap();
}

impl Env {
    async fn new() -> Self {
        Self::with_registry(registry(), Arc::new(etcd_rows(registry()))).await
    }

    async fn with_registry(registry: &'static Registry, etcd: Arc<MemSettings<'static>>) -> Self {
        let global = Arc::new(MemStore::new(Tier::Global));
        seed_directory(&*global, T).await;
        let v = Versioner::new(
            MemSource::new(),
            Arc::new(DirectoryAllocator::new(global.clone())),
        );
        v.backend().bootstrap(T);
        // Two databases, three objects (one dropped).
        let mut c = v.change(T, VersionKind::Catalog).await.unwrap();
        let ver = c.version();
        let tx = c.tx();
        tx.put_database(db(SHOP, "shop", ver));
        tx.put_database(db(LOGS, "logs", ver));
        tx.put_object(obj(ORDERS, SHOP, "orders", ObjectState::Active, ver));
        tx.put_object(obj(EVENTS, LOGS, "events", ObjectState::Active, ver));
        tx.put_object(obj(DROPPED, SHOP, "gone", ObjectState::Dropped, ver));
        tx.put_object(obj(SECRET, LOGS, "secret", ObjectState::Active, ver));
        c.commit().await.unwrap();
        let decisions = Arc::new(Decisions::default());
        let auth = Arc::new(Auth::default());
        let writer = TenantSettingsWriter::new(
            v,
            registry,
            Arc::new(FixedEtcd { rows: etcd, dc: dc() }),
            auth.clone(),
            decisions.clone(),
            Arc::new(SystemClock),
        );
        Self {
            global,
            writer,
            decisions,
            auth,
        }
    }

    fn mem(&self) -> &MemSource {
        self.writer.versioner().backend()
    }

    async fn set(&self, target: Target, key: &str, change: SettingChange) -> sprig_common::Result<SetOutcome> {
        self.writer
            .set(&SetRequest {
                tenant: T,
                target,
                key: key.to_owned(),
                change,
                principal: Principal::User(UserId(7)),
                reason: "test".to_owned(),
            })
            .await
    }

    async fn directory(&self) -> serde_json::Value {
        let key = self.global.tier().key(&format!("tenants/{T}")).unwrap();
        let kv = self
            .global
            .get(&key, Consistency::Linearizable)
            .await
            .unwrap()
            .unwrap();
        serde_json::from_slice(&kv.value).unwrap()
    }

    fn row(&self, scope: SettingScope, key: &str) -> Option<SettingRow> {
        self.mem()
            .tables(T)
            .unwrap()
            .settings
            .get(&(scope, key.to_owned()))
            .cloned()
    }

    /// Copies `rows` as `PARENT`'s, with the ancestry, as the tree loop does.
    async fn parent_rows(&self, rows: Vec<SettingRow>) {
        let v = self.writer.versioner();
        let mut c = v.change(T, VersionKind::Catalog).await.unwrap();
        let ver = c.version();
        let want = [TenantRef {
            id: PARENT,
            name: "parent".to_owned(),
        }];
        let fresh = [AncestorRows {
            tenant: PARENT,
            settings_version: 1,
            rows,
        }];
        apply_ancestry(c.tx(), T, ver, at(), &want, &fresh)
            .await
            .unwrap();
        c.commit().await.unwrap();
    }
}

fn code(e: &SprigError) -> u16 {
    e.protocol().mysql_errno
}

fn int(i: i64) -> SettingChange {
    SettingChange::Set(Value::Int(i))
}

fn parent_row(key: &str, value: Option<serde_json::Value>, bounds: Option<Bounds>) -> SettingRow {
    SettingRow {
        scope: SettingScope::Tenant,
        key: key.to_owned(),
        value,
        bounds,
        set_by: "user:1".to_owned(),
        set_at: at(),
        version: 1,
    }
}

#[tokio::test]
async fn keys_scopes_principals_and_types_are_checked_first() {
    let env = Env::new().await;
    let mp = MAX_PER_TABLE.key;
    for (target, key, change, want) in [
        (Target::Tenant, "nope.key", int(1), 39081),
        (Target::Tenant, "sprig.catalog_version", int(1), 39081),
        (Target::Object(ORDERS), LOCK_MINUTES.key, int(5), 39082),
        (Target::Database(SHOP), MAX_DATABASES.key, int(5), 39082),
        (Target::Tenant, MAX_DATABASES.key, int(5), 1227),
        (Target::Tenant, mp, SettingChange::Set(Value::from("x")), 39084),
        (
            Target::Tenant,
            mp,
            SettingChange::SetBounds(Bounds::allowed([Value::from("x")])),
            39084,
        ),
        (
            Target::Tenant,
            mp,
            SettingChange::SetBounds(Bounds {
                min: Some(f64::NAN),
                ..Bounds::default()
            }),
            39084,
        ),
    ] {
        let e = env.set(target, key, change.clone()).await.unwrap_err();
        assert_eq!(code(&e), want, "{target} {key} {change:?}: {e}");
    }
    // An operator may set an operator-only key.
    let ok = env
        .writer
        .set(&SetRequest {
            tenant: T,
            target: Target::Tenant,
            key: MAX_DATABASES.key.to_owned(),
            change: int(50),
            principal: Principal::Operator("kiran".to_owned()),
            reason: "bigger tenant".to_owned(),
        })
        .await
        .unwrap();
    assert!(ok.version.is_some());
    assert_eq!(
        env.row(SettingScope::Tenant, MAX_DATABASES.key)
            .unwrap()
            .set_by,
        "op:kiran"
    );
    // Nothing above allocated a version but the operator's write.
    assert_eq!(env.directory().await["catalog_version"], 2);
}

#[tokio::test]
async fn bounds_of_every_wider_scope_apply() {
    let env = Env::new().await;
    let mp = MAX_PER_TABLE.key;
    // Hard bounds 1-64; global max 8 (etcd).
    assert_eq!(code(&env.set(Target::Tenant, mp, int(65)).await.unwrap_err()), 39080);
    let e = env.set(Target::Tenant, mp, int(9)).await.unwrap_err();
    assert_eq!(code(&e), 39080);
    assert!(e.message.contains("max 8"), "{e}");
    env.set(Target::Tenant, mp, int(8)).await.unwrap();
    // dc locks password_lock_minutes: values and bounds are refused, a
    // reset is not.
    let lm = LOCK_MINUTES.key;
    assert_eq!(code(&env.set(Target::Tenant, lm, int(30)).await.unwrap_err()), 39083);
    assert_eq!(
        code(
            &env.set(Target::Tenant, lm, SettingChange::SetBounds(Bounds::max(5.0)))
                .await
                .unwrap_err()
        ),
        39083
    );
    assert!(env.set(Target::Tenant, lm, SettingChange::Reset).await.unwrap().version.is_none());

    // The parent's bound reaches the tenant and everything below it.
    env.parent_rows(vec![parent_row(mp, Some(json!(3)), Some(Bounds::max(6.0)))])
        .await;
    assert_eq!(code(&env.set(Target::Tenant, mp, int(7)).await.unwrap_err()), 39080);
    assert_eq!(code(&env.set(Target::Object(ORDERS), mp, int(7)).await.unwrap_err()), 39080);
    env.set(Target::Object(ORDERS), mp, int(6)).await.unwrap();

    // The tenant's own bound limits its databases and objects, not itself.
    env.set(Target::Tenant, mp, SettingChange::SetBounds(Bounds::max(4.0)))
        .await
        .unwrap();
    env.set(Target::Tenant, mp, int(6)).await.unwrap();
    assert_eq!(code(&env.set(Target::Database(SHOP), mp, int(5)).await.unwrap_err()), 39080);
    assert_eq!(code(&env.set(Target::Object(EVENTS), mp, int(5)).await.unwrap_err()), 39080);
    env.set(Target::Object(EVENTS), mp, int(4)).await.unwrap();

    // A database's allowed list binds its objects only.
    let mode = MODE.key;
    env.set(
        Target::Database(SHOP),
        mode,
        SettingChange::SetBounds(Bounds::allowed([Value::from("on"), Value::from("recommend")])),
    )
    .await
    .unwrap();
    let e = env
        .set(Target::Object(ORDERS), mode, SettingChange::Set(Value::from("off")))
        .await
        .unwrap_err();
    assert_eq!(code(&e), 39080);
    assert!(e.message.contains("allowed ('recommend', 'on')"), "{e}");
    env.set(Target::Object(ORDERS), mode, SettingChange::Set(Value::from("on")))
        .await
        .unwrap();
    // The privilege check refuses SECRET before any bound is read.
    let e = env
        .set(Target::Object(SECRET), mode, SettingChange::Set(Value::from("off")))
        .await
        .unwrap_err();
    assert_eq!(code(&e), 1142);
    // Locked at the database: its objects can't set the key.
    env.set(Target::Database(LOGS), mode, SettingChange::SetBounds(Bounds::locked()))
        .await
        .unwrap();
    let e = env
        .set(Target::Object(EVENTS), mode, SettingChange::Set(Value::from("on")))
        .await
        .unwrap_err();
    assert_eq!(code(&e), 39083);
    assert!(env.auth.0.lock().unwrap().contains(&Privilege::AlterObject(SECRET)));
}

#[tokio::test]
async fn refused_writes_allocate_nothing_and_targets_must_exist() {
    let env = Env::new().await;
    let mp = MAX_PER_TABLE.key;
    let before = env.directory().await;
    let catalog_before = env.mem().tables(T).unwrap().versions.catalog;
    for (target, want) in [
        (Target::Database(DbId(99)), 1049),
        (Target::Object(ObjectId(99)), 1146),
        (Target::Object(DROPPED), 1146),
    ] {
        let e = env.set(target, mp, int(3)).await.unwrap_err();
        assert_eq!(code(&e), want, "{target}");
        assert!(matches!(e.kind, ErrorKind::Standard { .. }));
    }
    assert_eq!(code(&env.set(Target::Tenant, mp, int(9)).await.unwrap_err()), 39080);
    assert_eq!(env.directory().await, before, "no version allocated: no gap");
    assert_eq!(env.mem().tables(T).unwrap().versions.catalog, catalog_before);
    // Nothing to reset: no version either.
    let r = env.set(Target::Object(ORDERS), mp, SettingChange::Reset).await.unwrap();
    assert_eq!(r.version, None);
    assert_eq!(env.directory().await, before);
}

#[tokio::test]
async fn the_row_holds_a_value_and_bounds_and_tenant_writes_move_settings_version() {
    let env = Env::new().await;
    let mp = MAX_PER_TABLE.key;
    let r = env.set(Target::Object(ORDERS), mp, int(3)).await.unwrap();
    let v1 = r.version.unwrap();
    assert_eq!(r.previous, None);
    let dir = env.directory().await;
    assert_eq!(dir["catalog_version"], v1);
    assert!(dir.get("settings_version").is_none_or(|s| s == 0), "{dir}");

    let r = env.set(Target::Tenant, mp, int(6)).await.unwrap();
    let v2 = r.version.unwrap();
    assert_eq!(env.directory().await["settings_version"], v2);
    env.set(Target::Tenant, mp, SettingChange::SetBounds(Bounds::max(7.0)))
        .await
        .unwrap();
    let row = env.row(SettingScope::Tenant, mp).unwrap();
    assert_eq!((row.value, row.bounds), (Some(json!(6)), Some(Bounds::max(7.0))));
    assert_eq!(row.set_by, "user:7");
    // RESET keeps the bounds, RESET BOUNDS the value; both leave a tombstone.
    let r = env.set(Target::Tenant, mp, SettingChange::Reset).await.unwrap();
    assert_eq!(r.previous.unwrap().value, Some(Value::Int(6)));
    assert_eq!(r.current.unwrap().bounds, Some(Bounds::max(7.0)));
    let r = env.set(Target::Tenant, mp, SettingChange::ResetBounds).await.unwrap();
    assert_eq!(r.current, None);
    let row = env.row(SettingScope::Tenant, mp).unwrap();
    assert!(row.is_empty());
    assert_eq!(row.version, r.version.unwrap());
    assert_eq!(env.directory().await["settings_version"], r.version.unwrap());
}

#[tokio::test]
async fn tightened_bounds_warn_and_record_a_decision() {
    let env = Env::new().await;
    let mp = MAX_PER_TABLE.key;
    env.set(Target::Object(ORDERS), mp, int(7)).await.unwrap();
    env.set(Target::Database(LOGS), mp, int(6)).await.unwrap();
    env.set(Target::Object(EVENTS), mp, int(2)).await.unwrap();
    let r = env
        .set(Target::Tenant, mp, SettingChange::SetBounds(Bounds::max(5.0)))
        .await
        .unwrap();
    assert_eq!(r.summary.as_deref(), Some("2 values will be clamped"));
    assert_eq!(r.warnings.len(), 2);
    assert!(
        r.warnings
            .iter()
            .any(|w| w.scope == sprig_settings::Scope::Object(T, ORDERS) && w.value == Value::Int(7)),
        "{:?}",
        r.warnings
    );
    assert!(r.warnings[0].message.contains("read as"), "{:?}", r.warnings);
    {
        let d = env.decisions.0.lock().unwrap();
        assert_eq!(d.len(), 1);
        assert_eq!((d[0].clamped, d[0].ignored), (2, 0));
        assert_eq!(d[0].reason_code, "bounds_tightened");
        assert_eq!(d[0].target, Target::Tenant);
    }
    // A database bound only counts its own objects.
    let r = env
        .set(Target::Database(SHOP), mp, SettingChange::SetBounds(Bounds::locked()))
        .await
        .unwrap();
    assert_eq!(r.summary.as_deref(), Some("1 value will be ignored"));
    // A value write never warns.
    let r = env.set(Target::Tenant, mp, int(3)).await.unwrap();
    assert!(r.warnings.is_empty() && r.summary.is_none());
    assert_eq!(env.decisions.0.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn privileges_follow_the_chapter() {
    let env = Env::new().await;
    let mp = MAX_PER_TABLE.key;
    env.set(Target::Object(ORDERS), mp, int(3)).await.unwrap();
    env.set(Target::Database(SHOP), mp, int(3)).await.unwrap();
    env.set(Target::Tenant, mp, int(3)).await.unwrap();
    env.set(Target::Object(ORDERS), mp, SettingChange::SetBounds(Bounds::max(9.0)))
        .await
        .unwrap();
    assert_eq!(
        *env.auth.0.lock().unwrap(),
        vec![
            Privilege::AlterObject(ORDERS),
            Privilege::AlterDatabase(SHOP),
            Privilege::SettingsAdmin,
            Privilege::SettingsAdmin,
        ]
    );
    // AllowAll allows.
    assert!(
        AllowAll
            .authorize(T, &Principal::System, Privilege::SettingsAdmin)
            .await
            .is_ok()
    );
    assert_eq!(Principal::System.set_by(), "system");
    assert!(!Principal::Operator("x".into()).is_tenant());
}

/// The cache's source: the writer's own in-memory catalog.
struct Via(Versioner<MemSource>);

#[async_trait::async_trait]
impl CatalogSource for Via {
    async fn load(
        &self,
        tenant: TenantId,
        prev: Option<&TenantCatalog>,
        node: Node,
    ) -> sprig_common::Result<Loaded> {
        self.0.backend().load(tenant, prev, node).await
    }
}

/// M0.3 "done when", at the library level: "a setting changed at object
/// scope takes effect within seconds, and a value outside its bounds is
/// refused". A gateway-like reader holds a catalog cache invalidated by
/// the directory and a `ResolvedWatch` on the object's key; a write at
/// object scope wakes it with the new value; a value outside the tenant's
/// bounds is refused and changes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn done_when_object_scope_change_applies_within_seconds() {
    let global_registry = Registry::global().unwrap();
    assert!(global_registry.get(OBJECT_LIMIT.key).is_some());
    let env = Env::with_registry(global_registry, Arc::new(MemSettings::new(global_registry))).await;
    let key = OBJECT_LIMIT.key;
    env.set(Target::Tenant, key, SettingChange::SetBounds(Bounds::max(50.0)))
        .await
        .unwrap();

    // The reader: etcd mirrors (empty), Settings, a cache fed by the
    // directory watch, notifying Settings of every load.
    let dc_store = MemStore::new(Tier::Dc(dc()));
    let live = sprig_settings::LiveSettings::start(
        global_registry,
        SettingsStores::new(Arc::new(dc_store.with_tier(Tier::Global)), Arc::new(dc_store)).unwrap(),
    )
    .unwrap();
    live.ready().await;
    let settings = Settings::new(live);
    let feed = DirectoryWatch::start(env.global.clone()).unwrap();
    feed.ready().await;
    let cache = TenantCache::start(
        global_registry,
        Arc::new(Via(env.writer.versioner().clone())),
        Arc::new(feed),
        Some(Arc::new(settings.clone())),
        CacheConfig::default(),
    );
    let _pin = cache.pin(T);
    let cat = cache.get(T, Freshness::Latest).await.unwrap();
    let ctx = OwnedCtx {
        db: Some(SHOP),
        object: Some(ORDERS),
        ..OwnedCtx::process(dc())
    };
    let mut w = settings
        .watch_resolved(key, ctx, Some(Arc::clone(&cat.settings)))
        .unwrap();
    assert_eq!(w.current().value, Value::Int(10));

    let started = Instant::now();
    env.set(Target::Object(ORDERS), key, int(25)).await.unwrap();
    let r = tokio::time::timeout(Duration::from_secs(5), w.changed())
        .await
        .expect("the object-scope value arrives within seconds")
        .unwrap();
    assert_eq!(r.value, Value::Int(25));
    assert_eq!(r.source, sprig_settings::ScopeKind::Object);
    println!("object-scope change applied in {:?}", started.elapsed());

    // Outside the tenant's bounds: refused, nothing changes.
    let e = env.set(Target::Object(ORDERS), key, int(51)).await.unwrap_err();
    assert_eq!(code(&e), 39080);
    assert!(
        tokio::time::timeout(Duration::from_millis(300), w.changed())
            .await
            .is_err(),
        "a refused write changes nothing"
    );
    let now = cache.get(T, Freshness::Latest).await.unwrap();
    let res = settings
        .resolve(key, &now.object_ctx(&dc(), ORDERS), Some(&now.settings))
        .unwrap();
    assert_eq!(res.value, Value::Int(25));
    let _ = ANCESTOR_VERSIONS_KEY;
}
