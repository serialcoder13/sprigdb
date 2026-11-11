//! Cache tests over [`MemSource`] (changes through the real `Versioner`)
//! and a [`DirectoryWatch`] on a `MemStore` global tier: incremental loads
//! equal full loads (property test), single flight, freshness rules,
//! invalidation, replica lag and gaps, failures, the memory bound,
//! name misses, the TTL backstop, settings notification, concurrency and
//! scale.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::similar_names,
    clippy::many_single_char_names,
    clippy::needless_pass_by_value,
    clippy::items_after_statements
)]

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use proptest::prelude::*;
use serde_json::json;
use sprig_common::SprigCode;
use sprig_common::ids::{DbId, DcId, GroupId, ObjectId, TenantId};
use sprig_meta::conformance::unique_tier;
use sprig_meta::{MemStore, MetaStore, Op, Tier, Txn};
use sprig_settings::{
    AnySetting, Apply, Bounds, HardBounds, LiveSettings, OwnedCtx, Registry, ScopeKind, Scopes,
    Setting, Settings, SettingsStores, TenantNotify, TenantScope, TenantSettings, Value,
};
use tokio::sync::broadcast;

use super::mem::{MemSource, MemSourceTx};
use super::*;
use crate::store::{
    DatabaseRow, DbState, Namespace, ObjectFlags, ObjectKind, ObjectRow, ObjectState, SettingRow,
    SettingScope,
};
use crate::version::{DirectoryAllocator, VersionKind, Versioner};

pub(crate) static ALPHA: Setting<u32> = Setting {
    key: "test.alpha",
    default: 1,
    scopes: Scopes::ALL,
    hard: HardBounds::range(0, 1_000_000),
    apply: Apply::Live,
    operator_only: false,
    owner: 8,
    doc: "A test key at every scope",
};
static MODE: Setting<Cow<'static, str>> = Setting {
    key: "test.mode",
    default: Cow::Borrowed("a"),
    scopes: Scopes::DATA,
    hard: HardBounds::one_of(&["a", "b", "c"]),
    apply: Apply::Live,
    operator_only: false,
    owner: 8,
    doc: "A test text key",
};

pub(crate) fn registry() -> &'static Registry {
    static R: OnceLock<Registry> = OnceLock::new();
    R.get_or_init(|| Registry::new([&ALPHA as &dyn AnySetting, &MODE]).unwrap())
}

pub(crate) const T: TenantId = TenantId(77);

fn at() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_762_560_000)
}

pub(crate) fn db(id: u32, name: &str, v: u64, state: DbState) -> DatabaseRow {
    DatabaseRow {
        db_id: DbId(id),
        name: name.to_owned(),
        name_key: name.to_lowercase(),
        default_charset: "utf8mb4".to_owned(),
        default_collation: "utf8mb4_bin".to_owned(),
        default_group_id: GroupId(0),
        state,
        options: json!({}),
        created_at: at(),
        created_by: None,
        changed_version: v,
    }
}

pub(crate) fn obj(id: u64, d: u32, name: &str, v: u64, state: ObjectState) -> ObjectRow {
    ObjectRow {
        object_id: ObjectId(id),
        db_id: DbId(d),
        namespace: Namespace::Relation,
        name: name.to_owned(),
        name_key: name.to_lowercase(),
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

pub(crate) fn setting(
    scope: SettingScope,
    key: &str,
    value: Option<serde_json::Value>,
    bounds: Option<Bounds>,
    v: u64,
) -> SettingRow {
    SettingRow {
        scope,
        key: key.to_owned(),
        value,
        bounds,
        set_by: "user:1".to_owned(),
        set_at: at(),
        version: v,
    }
}

/// The cache's source: the versioner's own backend.
struct ViaVersioner(Versioner<MemSource>);

#[async_trait::async_trait]
impl CatalogSource for ViaVersioner {
    async fn load(
        &self,
        tenant: TenantId,
        prev: Option<&TenantCatalog>,
        node: Node,
    ) -> sprig_common::Result<Loaded> {
        self.0.backend().load(tenant, prev, node).await
    }

    async fn committed(
        &self,
        tenant: TenantId,
        node: Node,
    ) -> sprig_common::Result<Option<(u64, u64)>> {
        self.0.backend().committed(tenant, node).await
    }
}

struct Env {
    global: Arc<MemStore>,
    v: Versioner<MemSource>,
    feed: DirectoryWatch,
}

impl Env {
    async fn new() -> Self {
        let global = Arc::new(MemStore::new(Tier::Global));
        let v = Versioner::new(
            MemSource::new(),
            Arc::new(DirectoryAllocator::new(global.clone())),
        );
        let feed = DirectoryWatch::start(global.clone()).unwrap();
        feed.ready().await;
        let env = Self { global, v, feed };
        env.tenant(T).await;
        env
    }

    async fn tenant(&self, t: TenantId) {
        self.v.backend().bootstrap(t);
        let key = self.global.tier().key(&format!("tenants/{t}")).unwrap();
        let body = json!({"v": 1, "tenant_id": t.0, "name": "t", "state": "active",
                          "catalog_version": 0, "auth_version": 0});
        self.global
            .txn(Txn {
                then: vec![Op::Put {
                    key,
                    value: serde_json::to_vec(&body).unwrap(),
                    lease: None,
                }],
                ..Txn::default()
            })
            .await
            .unwrap();
    }

    fn mem(&self) -> &MemSource {
        self.v.backend()
    }

    fn cache(&self, config: CacheConfig, notify: Option<Arc<dyn TenantNotify>>) -> TenantCache {
        TenantCache::start(
            registry(),
            Arc::new(ViaVersioner(self.v.clone())),
            Arc::new(self.feed.clone()),
            notify,
            config,
        )
    }

    async fn commit(&self, t: TenantId, f: impl FnOnce(&mut MemSourceTx, u64)) -> u64 {
        let mut c = self.v.change(t, VersionKind::Catalog).await.unwrap();
        let ver = c.version();
        f(c.tx(), ver);
        c.commit().await.unwrap()
    }

    /// Waits until the directory watch has delivered version `v` of `t`.
    async fn seen(&self, t: TenantId, v: u64) {
        eventually("directory event", Duration::from_secs(2), || {
            self.feed.cached(t).is_none_or(|d| d.catalog_version >= v)
        })
        .await;
    }

    /// Allocates a version and drops the change: a gap.
    async fn gap(&self, t: TenantId) {
        let c = self.v.change(t, VersionKind::Catalog).await.unwrap();
        drop(c);
    }
}

fn fast() -> CacheConfig {
    CacheConfig {
        version_wait: Duration::from_millis(100),
        load_timeout: Duration::from_secs(5),
        ..CacheConfig::default()
    }
}

async fn eventually(what: &str, limit: Duration, mut f: impl FnMut() -> bool) {
    let start = Instant::now();
    while !f() {
        assert!(start.elapsed() < limit, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

// ---- incremental loads equal full loads ----

#[derive(Debug, Clone)]
pub(crate) enum OpSpec {
    CreateDb(u8),
    DropDb(u8),
    CreateObj(u8, u8),
    RenameObj(u8, u8),
    DropObj(u8),
    Set(u8, u8, Option<u32>, Option<u32>),
    Reset(u8, u8, bool, bool),
    Unknown(u8, bool),
    Invalid(u8),
    Ancestry(u8),
    Batch(u8),
    Gap,
    Purge(u8),
    Load,
}

pub(crate) fn op_spec() -> impl Strategy<Value = OpSpec> {
    prop_oneof![
        2 => (0_u8..4).prop_map(OpSpec::CreateDb),
        1 => (0_u8..4).prop_map(OpSpec::DropDb),
        4 => (0_u8..4, 0_u8..6).prop_map(|(d, n)| OpSpec::CreateObj(d, n)),
        2 => (0_u8..8, 0_u8..6).prop_map(|(o, n)| OpSpec::RenameObj(o, n)),
        1 => (0_u8..8).prop_map(OpSpec::DropObj),
        4 => (0_u8..12, 0_u8..2, prop::option::of(0_u32..50), prop::option::of(0_u32..50))
            .prop_map(|(s, k, v, b)| OpSpec::Set(s, k, v, b)),
        2 => (0_u8..12, 0_u8..2, any::<bool>(), any::<bool>()).prop_map(|(s, k, v, b)| OpSpec::Reset(s, k, v, b)),
        1 => (0_u8..12, any::<bool>()).prop_map(|(s, r)| OpSpec::Unknown(s, r)),
        1 => (0_u8..12).prop_map(OpSpec::Invalid),
        1 => (0_u8..3).prop_map(OpSpec::Ancestry),
        1 => (2_u8..5).prop_map(OpSpec::Batch),
        1 => Just(OpSpec::Gap),
        1 => (0_u8..4).prop_map(OpSpec::Purge),
        4 => Just(OpSpec::Load),
    ]
}

/// What exists, so generated changes are ones the write path would make:
/// unique live names, no writes to dropped scopes.
#[derive(Default)]
pub(crate) struct Model {
    dbs: BTreeMap<u32, (String, bool)>,
    objs: BTreeMap<u64, (u32, String, bool)>,
    next_obj: u64,
}

impl Model {
    fn live_dbs(&self) -> Vec<u32> {
        self.dbs
            .iter()
            .filter(|(_, (_, l))| *l)
            .map(|(&d, _)| d)
            .collect()
    }
    fn live_objs(&self) -> Vec<u64> {
        self.objs
            .iter()
            .filter(|(_, (_, _, l))| *l)
            .map(|(&o, _)| o)
            .collect()
    }
    fn name_free(&self, d: u32, name: &str) -> bool {
        !self
            .objs
            .values()
            .any(|(od, n, l)| *l && *od == d && n == name)
    }
    /// A settings scope for selector `s`: tenant, an ancestor, a live
    /// database or a live object.
    fn scope(&self, s: u8) -> Option<SettingScope> {
        match s % 4 {
            0 => Some(SettingScope::Tenant),
            1 => Some(SettingScope::Ancestor(TenantId(1 + u64::from(s % 2)))),
            2 => {
                let l = self.live_dbs();
                (!l.is_empty()).then(|| SettingScope::Database(DbId(l[s as usize % l.len()])))
            }
            _ => {
                let l = self.live_objs();
                (!l.is_empty()).then(|| SettingScope::Object(ObjectId(l[s as usize % l.len()])))
            }
        }
    }
}

const KEYS: [&str; 2] = ["test.alpha", "test.mode"];

fn value_for(key: &str, v: u32) -> serde_json::Value {
    if key == "test.mode" {
        json!(["a", "b", "c", "zz"][v as usize % 4])
    } else {
        json!(v)
    }
}

/// One row write of a generated change; `bool`: the row is new (an
/// insert rather than an update, for the MariaDB store).
#[derive(Debug, Clone)]
pub(crate) enum W {
    Db(DatabaseRow, bool),
    Obj(ObjectRow, bool),
    Set(SettingRow),
    Reset(SettingScope, &'static str, bool, bool, u64),
}

fn apply_mem(tx: &mut MemSourceTx, ws: Vec<W>) {
    for w in ws {
        match w {
            W::Db(r, _) => tx.put_database(r),
            W::Obj(r, _) => tx.put_object(r),
            W::Set(r) => tx.put_setting(r),
            W::Reset(s, k, v, b, ver) => tx.reset_setting(s, k, v, b, ver),
        }
    }
}

/// Stages the writes of one generated op; `false` if it does nothing.
fn stage(m: &mut Model, tx: &mut MemSourceTx, ver: u64, op: &OpSpec) -> bool {
    let ws = writes(m, ver, op);
    let any = !ws.is_empty();
    apply_mem(tx, ws);
    any
}

/// The writes of one generated op (none if it does nothing), updating the
/// model.
pub(crate) fn writes(m: &mut Model, ver: u64, op: &OpSpec) -> Vec<W> {
    let mut out = Vec::new();
    match *op {
        OpSpec::CreateDb(d) => {
            let id = u32::from(d) + 1;
            if m.dbs.get(&id).is_some_and(|(_, l)| *l) || m.dbs.contains_key(&id) {
                return Vec::new();
            }
            let name = format!("Db{id}");
            out.push(W::Db(db(id, &name, ver, DbState::Active), true));
            m.dbs.insert(id, (name, true));
        }
        OpSpec::DropDb(d) => {
            let id = u32::from(d) + 1;
            let Some((name, true)) = m.dbs.get(&id).cloned() else {
                return Vec::new();
            };
            out.push(W::Db(db(id, &name, ver, DbState::Dropped), false));
            m.dbs.insert(id, (name, false));
            for (&o, (od, n, l)) in &mut m.objs {
                if *od == id && *l {
                    out.push(W::Obj(obj(o, id, n, ver, ObjectState::Dropped), false));
                    *l = false;
                }
            }
        }
        OpSpec::CreateObj(d, n) => {
            let live = m.live_dbs();
            if live.is_empty() {
                return Vec::new();
            }
            let d = live[d as usize % live.len()];
            let name = format!("T{n}");
            if !m.name_free(d, &name) {
                return Vec::new();
            }
            m.next_obj += 1;
            let id = m.next_obj;
            out.push(W::Obj(obj(id, d, &name, ver, ObjectState::Active), true));
            m.objs.insert(id, (d, name, true));
        }
        OpSpec::RenameObj(o, n) => {
            let live = m.live_objs();
            if live.is_empty() {
                return Vec::new();
            }
            let id = live[o as usize % live.len()];
            let d = m.objs[&id].0;
            let name = format!("T{n}");
            if !m.name_free(d, &name) {
                return Vec::new();
            }
            out.push(W::Obj(obj(id, d, &name, ver, ObjectState::Active), false));
            m.objs.insert(id, (d, name, true));
        }
        OpSpec::DropObj(o) => {
            let live = m.live_objs();
            if live.is_empty() {
                return Vec::new();
            }
            let id = live[o as usize % live.len()];
            let (d, name, _) = m.objs[&id].clone();
            out.push(W::Obj(obj(id, d, &name, ver, ObjectState::Dropped), false));
            m.objs.insert(id, (d, name, false));
        }
        OpSpec::Set(s, k, v, b) => {
            let Some(scope) = m.scope(s) else {
                return Vec::new();
            };
            if v.is_none() && b.is_none() {
                return Vec::new();
            }
            let key = KEYS[k as usize];
            let bounds = b.map(|b| {
                if key == "test.mode" {
                    Bounds::allowed([Value::from("a"), Value::from(["b", "c"][b as usize % 2])])
                } else {
                    Bounds::max(f64::from(b))
                }
            });
            out.push(W::Set(setting(
                scope,
                key,
                v.map(|v| value_for(key, v)),
                bounds,
                ver,
            )));
        }
        OpSpec::Reset(s, k, value, bounds) => {
            let Some(scope) = m.scope(s) else {
                return Vec::new();
            };
            out.push(W::Reset(
                scope,
                KEYS[k as usize],
                value || !bounds,
                bounds,
                ver,
            ));
        }
        OpSpec::Unknown(s, reset) => {
            let Some(scope) = m.scope(s) else {
                return Vec::new();
            };
            if reset {
                out.push(W::Reset(scope, "future.key", true, true, ver));
            } else {
                out.push(W::Set(setting(
                    scope,
                    "future.key",
                    Some(json!(3)),
                    None,
                    ver,
                )));
            }
        }
        OpSpec::Invalid(s) => {
            let Some(scope) = m.scope(s) else {
                return Vec::new();
            };
            out.push(W::Set(setting(
                scope,
                "test.alpha",
                Some(json!({"not": "scalar"})),
                None,
                ver,
            )));
        }
        OpSpec::Ancestry(n) => {
            let ids: Vec<serde_json::Value> = (1..=u64::from(n))
                .map(|i| json!({"id": i, "name": format!("a{i}")}))
                .collect();
            out.push(W::Set(setting(
                SettingScope::Tenant,
                "sprig.ancestry",
                Some(json!(ids)),
                None,
                ver,
            )));
        }
        OpSpec::Batch(_) | OpSpec::Gap | OpSpec::Purge(_) | OpSpec::Load => {}
    }
    out
}

fn run_sequence(ops: Vec<OpSpec>) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let env = Env::new().await;
        let mem = env.mem();
        let mut m = Model::default();
        let mut cached: Option<TenantCatalog> = None;
        let check = |cached: &mut Option<TenantCatalog>| {
            let loaded = mem_load(mem, cached.as_ref());
            let inc = match cached.as_ref() {
                Some(c) => c.apply(loaded),
                None => TenantCatalog::build(registry(), T, loaded),
            };
            let full = TenantCatalog::build(registry(), T, mem_load(mem, None));
            assert_eq!(inc, full, "incremental and full loads differ");
            assert_eq!(inc.bytes(), full.bytes());
            *cached = Some(inc);
        };
        let mut i = 0;
        while i < ops.len() {
            match &ops[i] {
                OpSpec::Load => check(&mut cached),
                OpSpec::Gap => env.gap(T).await,
                OpSpec::Purge(back) => {
                    let committed = mem.tables(T).unwrap().versions.catalog;
                    mem.purge(T, committed.saturating_sub(u64::from(*back)))
                        .await
                        .unwrap();
                }
                OpSpec::Batch(n) => {
                    // Several ops in one change.
                    let n = usize::from(*n);
                    let batch: Vec<OpSpec> = ops[i + 1..ops.len().min(i + 1 + n)].to_vec();
                    i += batch.len();
                    let mut c = env.v.change(T, VersionKind::Catalog).await.unwrap();
                    let ver = c.version();
                    for op in &batch {
                        stage(&mut m, c.tx(), ver, op);
                    }
                    c.commit().await.unwrap();
                }
                op => {
                    let mut c = env.v.change(T, VersionKind::Catalog).await.unwrap();
                    let ver = c.version();
                    if stage(&mut m, c.tx(), ver, op) {
                        c.commit().await.unwrap();
                    } else {
                        c.rollback().await.unwrap();
                    }
                }
            }
            i += 1;
        }
        check(&mut cached);
    });
}

/// A synchronous load from the mem source's committed tables.
fn mem_load(mem: &MemSource, prev: Option<&TenantCatalog>) -> Loaded {
    Loaded::core(mem.tables(T).unwrap().snapshot(prev.map(|p| p.version)))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn incremental_loads_equal_full_loads(ops in prop::collection::vec(op_spec(), 1..60)) {
        run_sequence(ops);
    }
}

#[test]
fn a_purge_past_the_cached_version_forces_a_full_load() {
    run_sequence(vec![
        OpSpec::CreateDb(0),
        OpSpec::CreateObj(0, 1),
        OpSpec::Set(3, 0, Some(5), None),
        OpSpec::Load,
        OpSpec::DropObj(0),
        OpSpec::Reset(0, 0, true, true),
        OpSpec::Purge(0),
        OpSpec::Load,
    ]);
}

#[tokio::test]
async fn snapshots_index_names_settings_and_ancestry() {
    let env = Env::new().await;
    env.commit(T, |tx, v| {
        tx.put_database(db(1, "Shop", v, DbState::Active));
        tx.put_object(obj(10, 1, "Orders", v, ObjectState::Active));
        tx.put_setting(setting(
            SettingScope::Object(ObjectId(10)),
            "test.alpha",
            Some(json!(7)),
            None,
            v,
        ));
        tx.put_setting(setting(
            SettingScope::Database(DbId(1)),
            "test.alpha",
            None,
            Some(Bounds::max(5.0)),
            v,
        ));
        tx.put_setting(setting(
            SettingScope::Tenant,
            "sprig.ancestry",
            Some(json!([{"id": 3, "name": "root"}, 4])),
            None,
            v,
        ));
        tx.put_setting(setting(
            SettingScope::Ancestor(TenantId(4)),
            "test.alpha",
            Some(json!(9)),
            None,
            v,
        ));
    })
    .await;
    let cat = TenantCatalog::build(registry(), T, mem_load(env.mem(), None));
    assert_eq!(cat.database("shop").unwrap().db_id, DbId(1));
    let o = cat.object(DbId(1), Namespace::Relation, "orders").unwrap();
    assert_eq!(o.object_id, ObjectId(10));
    assert_eq!(cat.objects.names(DbId(1), Namespace::Relation).count(), 1);
    assert_eq!(
        cat.ancestry.iter().map(|r| r.id.0).collect::<Vec<_>>(),
        vec![3, 4]
    );
    assert_eq!(cat.ancestry[0].name, "root");
    assert_eq!(cat.settings.ancestry(), &[TenantId(3), TenantId(4)]);
    // Resolution over the snapshot: the object's 7 is clamped by its
    // database's max 5; the tenant itself inherits the parent's 9.
    let etcd = sprig_settings::MemSettings::new(registry());
    let dc = DcId::new("mum1").unwrap();
    let r = cat
        .settings
        .resolve(&etcd, "test.alpha", &cat.object_ctx(&dc, ObjectId(10)))
        .unwrap();
    assert_eq!(
        (r.value, r.source, r.clamped),
        (Value::Int(5), ScopeKind::Object, true)
    );
    let r = cat
        .settings
        .resolve(&etcd, "test.alpha", &cat.setting_ctx(&dc))
        .unwrap();
    assert_eq!(
        (r.value, r.source),
        (Value::Int(9), ScopeKind::Ancestor(TenantId(4)))
    );
    assert_eq!(cat.clamped(&etcd, &dc).len(), 1);
}

// ---- the cache ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_cold_reads_share_one_load() {
    let env = Env::new().await;
    env.commit(T, |tx, v| tx.put_database(db(1, "a", v, DbState::Active)))
        .await;
    env.mem().set_delay(Duration::from_millis(100));
    let cache = env.cache(fast(), None);
    let mut tasks = Vec::new();
    for _ in 0..20 {
        let c = cache.clone();
        tasks.push(tokio::spawn(async move {
            c.get(T, Freshness::Latest).await.unwrap()
        }));
    }
    let mut got = Vec::new();
    for t in tasks {
        got.push(t.await.unwrap());
    }
    assert!(got.iter().all(|c| Arc::ptr_eq(c, &got[0])));
    assert_eq!(got[0].version, 1);
    assert_eq!(env.mem().loads(), (1, 0));
}

#[tokio::test]
async fn reads_never_wait_behind_a_load_unless_they_need_its_version() {
    let env = Env::new().await;
    let v1 = env
        .commit(T, |tx, v| tx.put_database(db(1, "a", v, DbState::Active)))
        .await;
    let cache = env.cache(fast(), None);
    let c1 = cache.get(T, Freshness::Latest).await.unwrap();
    assert_eq!(c1.version, v1);

    env.mem().set_delay(Duration::from_millis(300));
    let v2 = env
        .commit(T, |tx, v| tx.put_database(db(2, "b", v, DbState::Active)))
        .await;
    // Wait for the directory event.
    eventually("directory event", Duration::from_secs(2), || {
        env.feed.cached(T).is_some_and(|d| d.catalog_version == v2)
    })
    .await;

    // A statement that needs nothing newer gets v1 at once, and starts a
    // background load.
    let t0 = Instant::now();
    let c = cache.get(T, Freshness::ANY).await.unwrap();
    assert_eq!(c.version, v1);
    assert!(t0.elapsed() < Duration::from_millis(100));
    // Its own change (read your own DDL) makes it wait for v2.
    let c = cache
        .get(T, Freshness::Cached { min_version: v2 })
        .await
        .unwrap();
    assert!(c.version >= v2);
    assert!(c.database("b").is_some());
    // c1 is untouched: snapshots are immutable.
    assert!(c1.database("b").is_none());
}

#[tokio::test]
async fn pinned_tenants_reload_on_directory_events_others_on_next_use() {
    let env = Env::new().await;
    let other = TenantId(78);
    env.tenant(other).await;
    let cache = env.cache(fast(), None);
    let _pin = cache.pin(T);
    cache.get(T, Freshness::Latest).await.unwrap();
    cache.get(other, Freshness::Latest).await.unwrap();

    let t0 = Instant::now();
    let v = env
        .commit(T, |tx, v| tx.put_database(db(1, "a", v, DbState::Active)))
        .await;
    let w = env
        .commit(other, |tx, v| {
            tx.put_database(db(1, "a", v, DbState::Active));
        })
        .await;
    eventually("pinned reload", Duration::from_secs(2), || {
        cache.cached(T).is_some_and(|c| c.version == v)
    })
    .await;
    let latency = t0.elapsed();
    eprintln!("invalidation latency (MemStore): {latency:?}");
    // The unpinned tenant waits for its next use.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(cache.cached(other).unwrap().version, 0);
    assert!(cache.version_lag() > Duration::ZERO);
    env.seen(other, w).await;
    assert_eq!(
        cache.get(other, Freshness::Latest).await.unwrap().version,
        w
    );
    assert_eq!(cache.version_lag(), Duration::ZERO);
}

#[tokio::test]
async fn a_lagging_replica_hands_over_to_the_writer() {
    let env = Env::new().await;
    let cache = env.cache(fast(), None);
    cache.get(T, Freshness::Latest).await.unwrap();
    env.mem().freeze_replica(T, true).unwrap();
    let v = env
        .commit(T, |tx, v| tx.put_database(db(1, "a", v, DbState::Active)))
        .await;
    env.seen(T, v).await;
    let t0 = Instant::now();
    let c = cache.get(T, Freshness::Latest).await.unwrap();
    assert_eq!(c.version, v);
    assert!(
        t0.elapsed() >= Duration::from_millis(90),
        "{:?}",
        t0.elapsed()
    );
    let (replica, writer) = env.mem().loads();
    assert!(replica >= 2 && writer >= 1, "{replica} {writer}");
}

/// Decision PF4: the directory moves before the catalog commits, so a
/// reader asking for the new version polls the committed version with a
/// small read and loads in full once, when it is there.
#[tokio::test]
async fn a_version_not_yet_committed_is_polled_and_loaded_once() {
    let env = Env::new().await;
    let cache = env.cache(fast(), None);
    cache.get(T, Freshness::Latest).await.unwrap();
    let (r0, w0) = env.mem().loads();
    // Allocated (the directory moves), committed 40 ms later.
    let mut c = env.v.change(T, VersionKind::Catalog).await.unwrap();
    let v = c.version();
    c.tx().put_database(db(1, "a", v, DbState::Active));
    env.seen(T, v).await;
    let commit = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(40)).await;
        c.commit().await.unwrap()
    });
    let got = cache
        .get(T, Freshness::Cached { min_version: v })
        .await
        .unwrap();
    assert_eq!(got.version, v);
    assert_eq!(commit.await.unwrap(), v);
    let (r1, w1) = env.mem().loads();
    assert!(
        (r1 - r0) + (w1 - w0) <= 2,
        "full loads while waiting: replica {} writer {}",
        r1 - r0,
        w1 - w0
    );
    assert!(env.mem().version_reads() >= 2, "the wait polled");
}

#[tokio::test]
async fn a_gap_is_accepted_after_the_version_wait() {
    let env = Env::new().await;
    let v1 = env
        .commit(T, |tx, v| tx.put_database(db(1, "a", v, DbState::Active)))
        .await;
    env.gap(T).await;
    env.seen(T, v1 + 1).await;
    let cache = env.cache(fast(), None);
    let t0 = Instant::now();
    let c = cache.get(T, Freshness::Latest).await.unwrap();
    assert_eq!(c.version, v1);
    assert!(t0.elapsed() >= Duration::from_millis(90));
    // The gap counts as reached: the next read doesn't wait again.
    let t0 = Instant::now();
    cache.get(T, Freshness::Latest).await.unwrap();
    assert!(t0.elapsed() < Duration::from_millis(50));
    // The next change commits above the gap and loads normally.
    let v3 = env
        .commit(T, |tx, v| tx.put_database(db(2, "b", v, DbState::Active)))
        .await;
    assert_eq!(v3, v1 + 2);
    env.seen(T, v3).await;
    assert_eq!(cache.get(T, Freshness::Latest).await.unwrap().version, v3);
}

#[tokio::test]
async fn failed_loads_keep_serving_the_cached_snapshot() {
    let env = Env::new().await;
    let cache = env.cache(fast(), None);
    let v1 = env
        .commit(T, |tx, v| tx.put_database(db(1, "a", v, DbState::Active)))
        .await;
    cache.get(T, Freshness::Latest).await.unwrap();
    env.mem().set_failing(true);
    let v2 = env
        .commit(T, |tx, v| tx.put_database(db(2, "b", v, DbState::Active)))
        .await;
    assert_eq!(cache.get(T, Freshness::ANY).await.unwrap().version, v1);
    assert_eq!(cache.get(T, Freshness::Latest).await.unwrap().version, v1);
    let e = cache
        .get(T, Freshness::Cached { min_version: v2 })
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::CatalogUnavailable));
    let cold = TenantId(79);
    env.tenant(cold).await;
    let e = cache.get(cold, Freshness::ANY).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::CatalogUnavailable));
    env.mem().set_failing(false);
    env.seen(T, v2).await;
    assert_eq!(cache.get(T, Freshness::Latest).await.unwrap().version, v2);
    // A tenant with no directory entry.
    let e = cache.get(TenantId(5), Freshness::Latest).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::TenantNotReady));
}

#[derive(Default)]
struct Recorder {
    loaded: Mutex<Vec<(TenantId, u64, BTreeSet<&'static str>)>>,
    evicted: Mutex<Vec<TenantId>>,
}

impl TenantNotify for Recorder {
    fn tenant_loaded(&self, s: &Arc<TenantSettings>, changed: &BTreeSet<&'static str>) {
        self.loaded
            .lock()
            .unwrap()
            .push((s.tenant(), s.version(), changed.clone()));
    }
    fn tenant_evicted(&self, t: TenantId) {
        self.evicted.lock().unwrap().push(t);
    }
}

#[tokio::test]
async fn the_memory_bound_evicts_idle_tenants_least_recently_used_first() {
    let env = Env::new().await;
    let tenants: Vec<TenantId> = (100..106).map(TenantId).collect();
    for &t in &tenants {
        env.tenant(t).await;
        env.commit(t, |tx, v| {
            for i in 0..20 {
                tx.put_object(obj(i + 1, 1, &format!("t{i}"), v, ObjectState::Active));
            }
        })
        .await;
    }
    let rec = Arc::new(Recorder::default());
    let cfg = CacheConfig {
        max_tenants: 3,
        ..fast()
    };
    let cache = env.cache(cfg, Some(rec.clone()));
    let _pin = cache.pin(tenants[0]);
    for &t in &tenants {
        cache.get(t, Freshness::ANY).await.unwrap();
    }
    // Tenants 0 (pinned) and the two most recent stay.
    assert_eq!(cache.len(), 3);
    assert!(cache.cached(tenants[0]).is_some());
    assert!(cache.cached(tenants[4]).is_some());
    assert!(cache.cached(tenants[5]).is_some());
    assert_eq!(rec.evicted.lock().unwrap().len(), 3);
    // Evicted tenants are no longer followed in the directory.
    assert_eq!(env.feed.followed(), 3);
    // The byte count matches the snapshots held.
    let held: usize = [0, 4, 5]
        .iter()
        .map(|&i| cache.cached(tenants[i]).unwrap().bytes())
        .sum();
    assert_eq!(cache.bytes(), held);

    // A byte limit below two snapshots: the idle tenants go, then even a
    // pinned one; the tenant just used stays.
    let one = cache.cached(tenants[5]).unwrap().bytes();
    cache.set_config(CacheConfig {
        max_tenants: 3,
        max_bytes: one + one / 2,
        ..fast()
    });
    assert_eq!(cache.len(), 1);
    assert!(cache.bytes() <= one + one / 2);
    // A snapshot held by a statement survives eviction.
    let held = cache.get(tenants[1], Freshness::ANY).await.unwrap();
    assert!(cache.evict(tenants[1]));
    assert!(!cache.evict(tenants[1]));
    assert_eq!(held.objects.len(), 20);
}

#[tokio::test]
async fn a_name_miss_reloads_from_the_writer_at_most_once_a_second() {
    let env = Env::new().await;
    let cache = env.cache(fast(), None);
    cache.get(T, Freshness::Latest).await.unwrap();
    env.mem().freeze_replica(T, true).unwrap();
    env.commit(T, |tx, v| {
        tx.put_object(obj(1, 1, "new", v, ObjectState::Active));
    })
    .await;
    let c = cache.reload_after_miss(T).await.unwrap().unwrap();
    assert!(c.object(DbId(1), Namespace::Relation, "new").is_some());
    assert_eq!(env.mem().loads().1, 1);
    assert!(cache.reload_after_miss(T).await.unwrap().is_none());
    assert_eq!(env.mem().loads().1, 1);
}

/// A feed whose watch never fires: only `refresh` sees the directory.
struct SilentFeed {
    truth: DirectoryWatch,
    seen: Mutex<HashMap<TenantId, crate::version::DirectoryVersions>>,
    refreshes: AtomicUsize,
    tx: broadcast::Sender<TenantId>,
}

#[async_trait::async_trait]
impl DirectoryFeed for SilentFeed {
    async fn versions(
        &self,
        t: TenantId,
    ) -> sprig_common::Result<crate::version::DirectoryVersions> {
        if let Some(v) = self.cached(t) {
            return Ok(v);
        }
        self.refresh(t).await
    }
    fn cached(&self, t: TenantId) -> Option<crate::version::DirectoryVersions> {
        self.seen.lock().unwrap().get(&t).copied()
    }
    async fn refresh(
        &self,
        t: TenantId,
    ) -> sprig_common::Result<crate::version::DirectoryVersions> {
        self.refreshes.fetch_add(1, Ordering::SeqCst);
        let v = self.truth.refresh(t).await?;
        self.seen.lock().unwrap().insert(t, v);
        Ok(v)
    }
    fn forget(&self, t: TenantId) {
        self.seen.lock().unwrap().remove(&t);
    }
    fn changes(&self) -> broadcast::Receiver<TenantId> {
        self.tx.subscribe()
    }
}

#[tokio::test]
async fn the_ttl_backstop_rereads_the_directory() {
    let env = Env::new().await;
    let feed = Arc::new(SilentFeed {
        truth: env.feed.clone(),
        seen: Mutex::new(HashMap::new()),
        refreshes: AtomicUsize::new(0),
        tx: broadcast::channel(4).0,
    });
    let cfg = CacheConfig {
        ttl: Duration::from_millis(100),
        ..fast()
    };
    let cache = TenantCache::start(
        registry(),
        Arc::new(ViaVersioner(env.v.clone())),
        feed.clone(),
        None,
        cfg,
    );
    cache.get(T, Freshness::Latest).await.unwrap();
    let v = env
        .commit(T, |tx, v| tx.put_database(db(1, "a", v, DbState::Active)))
        .await;
    // Within the TTL, the missed event goes unnoticed.
    assert_eq!(cache.get(T, Freshness::ANY).await.unwrap().version, 0);
    tokio::time::sleep(Duration::from_millis(120)).await;
    // Past it, the next use re-reads the entry and loads in the background.
    assert_eq!(cache.get(T, Freshness::ANY).await.unwrap().version, 0);
    eventually("backstop reload", Duration::from_secs(2), || {
        cache.cached(T).is_some_and(|c| c.version == v)
    })
    .await;
    assert!(feed.refreshes.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn loads_report_changed_setting_keys_and_drive_resolved_watches() {
    let env = Env::new().await;
    // Settings over live (empty) etcd mirrors.
    let dc_store = Arc::new(MemStore::new(unique_tier()));
    let g = Arc::new(dc_store.with_tier(unique_tier()));
    let stores = SettingsStores::new(g, dc_store).unwrap();
    let live = LiveSettings::start(registry(), stores.clone()).unwrap();
    live.ready().await;
    let settings = Settings::new(live);
    let rec = Arc::new(Recorder::default());
    struct Both(Arc<Recorder>, Settings);
    impl TenantNotify for Both {
        fn tenant_loaded(&self, s: &Arc<TenantSettings>, c: &BTreeSet<&'static str>) {
            self.0.tenant_loaded(s, c);
            self.1.tenant_loaded(s, c);
        }
        fn tenant_evicted(&self, t: TenantId) {
            self.0.tenant_evicted(t);
            self.1.tenant_evicted(t);
        }
    }
    let cache = env.cache(fast(), Some(Arc::new(Both(rec.clone(), settings.clone()))));
    let _pin = cache.pin(T);
    env.commit(T, |tx, v| {
        tx.put_setting(setting(
            SettingScope::Tenant,
            "test.alpha",
            Some(json!(4)),
            None,
            v,
        ));
    })
    .await;
    let cat = cache.get(T, Freshness::Latest).await.unwrap();
    let dc = stores.dc_id().clone();
    assert_eq!(
        settings.get(&ALPHA, &cat.setting_ctx(&dc), Some(&cat.settings)),
        4
    );

    let any = settings.watch(&ALPHA);
    let mut w = settings
        .watch_resolved(
            "test.alpha",
            OwnedCtx::from_ctx(&cat.setting_ctx(&dc)),
            Some(Arc::clone(&cat.settings)),
        )
        .unwrap();
    // A change to another key doesn't wake the resolved watch.
    env.commit(T, |tx, v| {
        tx.put_setting(setting(
            SettingScope::Tenant,
            "test.mode",
            Some(json!("b")),
            None,
            v,
        ));
    })
    .await;
    // A change to the key does, without any read: the pinned tenant
    // reloads on the directory event.
    env.commit(T, |tx, v| {
        tx.put_setting(setting(
            SettingScope::Tenant,
            "test.alpha",
            Some(json!(6)),
            None,
            v,
        ));
    })
    .await;
    let r = tokio::time::timeout(Duration::from_secs(3), w.changed())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.value, Value::Int(6));
    assert!(any.has_changed().unwrap());
    let loaded = rec.loaded.lock().unwrap().clone();
    // The first load reports every key; later ones the keys that changed
    // (two quick commits may arrive in one reload).
    assert_eq!(loaded[0].2, BTreeSet::from(["test.alpha"]));
    let later: BTreeSet<&str> = loaded[1..]
        .iter()
        .flat_map(|(_, _, k)| k.iter().copied())
        .collect();
    assert_eq!(later, BTreeSet::from(["test.alpha", "test.mode"]));
    assert!(loaded.last().unwrap().2.contains("test.alpha"));
    assert!(loaded.iter().all(|(t, _, _)| *t == T));
    // A resolved value under a tenant bound added later is clamped, and the
    // object scope reads through the snapshot.
    let ts = TenantScope::Tenant;
    let cat = cache.get(T, Freshness::Latest).await.unwrap();
    assert_eq!(
        cat.settings.entry(ts, "test.alpha").unwrap().value,
        Some(Value::Int(6))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn readers_never_see_a_torn_snapshot() {
    // Every change v writes object o<v> and sets test.alpha = v at tenant
    // scope, in one commit. In any snapshot at version V: V objects, and
    // test.alpha = V. Readers also never see versions go backwards.
    let env = Arc::new(Env::new().await);
    let cache = env.cache(fast(), None);
    let _pin = cache.pin(T);
    cache.get(T, Freshness::Latest).await.unwrap();
    const CHANGES: u64 = 150;
    let writer = {
        let env = Arc::clone(&env);
        tokio::spawn(async move {
            for _ in 0..CHANGES {
                env.commit(T, |tx, v| {
                    tx.put_object(obj(v, 1, &format!("o{v}"), v, ObjectState::Active));
                    tx.put_setting(setting(
                        SettingScope::Tenant,
                        "test.alpha",
                        Some(json!(v)),
                        None,
                        v,
                    ));
                })
                .await;
            }
        })
    };
    let mut readers = Vec::new();
    for r in 0..8 {
        let cache = cache.clone();
        readers.push(tokio::spawn(async move {
            let mut last = 0;
            let mut seen = 0_u64;
            loop {
                let fresh = if r % 2 == 0 {
                    Freshness::ANY
                } else {
                    Freshness::Latest
                };
                let c = cache.get(T, fresh).await.unwrap();
                assert!(
                    c.version >= last,
                    "version went back: {} after {last}",
                    c.version
                );
                last = c.version;
                assert_eq!(c.objects.len() as u64, c.version);
                let alpha = c
                    .settings
                    .entry(TenantScope::Tenant, "test.alpha")
                    .map(|e| e.value.clone());
                if c.version > 0 {
                    assert_eq!(alpha, Some(Some(Value::Int(c.version.cast_signed()))));
                }
                seen += 1;
                if c.version == CHANGES {
                    return seen;
                }
                tokio::task::yield_now().await;
            }
        }));
    }
    writer.await.unwrap();
    for r in readers {
        let seen = tokio::time::timeout(Duration::from_secs(20), r)
            .await
            .unwrap()
            .unwrap();
        assert!(seen > 0);
    }
}

#[test]
fn extension_states_are_typed_and_replaced_per_module() {
    #[derive(Debug)]
    struct Users(usize);
    impl ExtState for Users {
        fn bytes(&self) -> usize {
            self.0 * 100
        }
    }
    #[derive(Debug)]
    struct Other;
    impl ExtState for Other {}
    let s =
        ExtensionStates::default().with(vec![("auth", Arc::new(Users(3)) as Arc<dyn ExtState>)]);
    assert_eq!(s.get::<Users>("auth").unwrap().0, 3);
    assert!(s.get::<Other>("auth").is_none());
    assert_eq!(s.bytes(), 300);
    let s2 = s.with(vec![("auth", Arc::new(Users(4)) as Arc<dyn ExtState>)]);
    assert_eq!(s2.get::<Users>("auth").unwrap().0, 4);
    assert_eq!(s.get::<Users>("auth").unwrap().0, 3);
    assert_eq!(s2.modules().collect::<Vec<_>>(), vec!["auth"]);
}

#[tokio::test]
async fn scale_100k_objects_load_within_budget() {
    let env = Env::new().await;
    let n = 100_000_u64;
    env.commit(T, |tx, v| {
        tx.put_database(db(1, "big", v, DbState::Active));
        for i in 1..=n {
            tx.put_object(obj(i, 1, &format!("table_{i}"), v, ObjectState::Active));
        }
    })
    .await;
    let t0 = Instant::now();
    let full = TenantCatalog::build(registry(), T, mem_load(env.mem(), None));
    let build = t0.elapsed();
    env.commit(T, |tx, v| {
        tx.put_object(obj(7, 1, "renamed", v, ObjectState::Active));
    })
    .await;
    let loaded = mem_load(env.mem(), Some(&full));
    let t0 = Instant::now();
    let next = full.apply(loaded);
    let inc = t0.elapsed();
    eprintln!(
        "100k objects: full build {build:?}, {} MB estimated; one incremental change {inc:?}",
        full.bytes() >> 20
    );
    assert!(full.bytes() < 64 << 20, "{} bytes", full.bytes());
    assert!(
        next.object(DbId(1), Namespace::Relation, "renamed")
            .is_some()
    );
    assert!(
        next.object(DbId(1), Namespace::Relation, "table_7")
            .is_none()
    );
    // The change copied one shard of the id index.
    assert_eq!(next.objects.by_id.unshared(&full.objects.by_id), 1);
    if cfg!(not(debug_assertions)) {
        assert!(build < Duration::from_secs(2));
        assert!(inc < Duration::from_millis(20));
    }
}
