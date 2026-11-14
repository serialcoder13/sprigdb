//! Six-scope resolution over a tenant snapshot and the etcd snapshot:
//! precedence, bounds and clamping tables, the layered example (global,
//! tenant, table), layering equivalence, memoization, the
//! incremental builder, clamp reports and watches.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::type_complexity,
    clippy::useless_vec
)]

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use proptest::prelude::*;
use sprig_common::ids::{ClusterId, DbId, DcId, ObjectId, TenantId};
use sprig_meta::conformance::unique_tier;
use sprig_meta::{MemStore, MetaStore, Op, Record, Txn};

use crate::*;

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
static WILDCARD: Setting<bool> = Setting {
    key: "doc.wildcard_index",
    default: false,
    scopes: Scopes::DATA,
    hard: HardBounds::NONE,
    apply: Apply::Live,
    operator_only: false,
    owner: 31,
    doc: "Wildcard indexes on collections",
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

fn registry() -> &'static Registry {
    static R: std::sync::OnceLock<Registry> = std::sync::OnceLock::new();
    R.get_or_init(|| {
        Registry::new([
            &MAX_PER_TABLE as &dyn AnySetting,
            &MODE,
            &WILDCARD,
            &LOCK_MINUTES,
        ])
        .unwrap()
    })
}

const ACME: TenantId = TenantId(42);
const DB: DbId = DbId(1);
const ORDERS: ObjectId = ObjectId(105);

fn dc() -> DcId {
    DcId::new("mum1").unwrap()
}

fn c1() -> ClusterId {
    ClusterId::new("mum1-c1").unwrap()
}

fn val(v: i64) -> Entry {
    Entry {
        value: Some(Value::Int(v)),
        bounds: None,
    }
}

fn bounds(b: Bounds) -> Entry {
    Entry {
        value: None,
        bounds: Some(b),
    }
}

fn both(v: i64, b: Bounds) -> Entry {
    Entry {
        value: Some(Value::Int(v)),
        bounds: Some(b),
    }
}

fn min(m: f64) -> Bounds {
    Bounds {
        min: Some(m),
        ..Bounds::default()
    }
}

/// Where a test row lives.
#[derive(Debug, Clone, Copy)]
enum At {
    Global,
    Dc,
    Cluster,
    Ancestor(u64),
    Tenant,
    Database,
    Object,
}

/// The etcd rows and the tenant rows of one case.
fn layers(
    key: &str,
    rows: &[(At, Entry)],
    ancestry: &[TenantId],
) -> (MemSettings<'static>, TenantSettings) {
    let mut etcd = MemSettings::new(registry());
    let mut t = TenantSettings::builder(registry(), ACME, 7);
    t.set_ancestry(ancestry.to_vec());
    for (at, e) in rows {
        let e = e.clone();
        match at {
            At::Global => etcd.insert_row(Scope::Global, key, e),
            At::Dc => etcd.insert_row(Scope::Dc(dc()), key, e),
            At::Cluster => etcd.insert_row(Scope::Cluster(c1()), key, e),
            At::Ancestor(a) => t.put(TenantScope::Ancestor(TenantId(*a)), key, e),
            At::Tenant => t.put(TenantScope::Tenant, key, e),
            At::Database => t.put(TenantScope::Database(DB), key, e),
            At::Object => t.put(TenantScope::Object(ORDERS), key, e),
        }
    }
    (etcd, t.build())
}

/// A context with the scopes up to `depth`: 0 process, 1 + cluster,
/// 2 + tenant, 3 + database, 4 + object.
fn ctx<'a>(dc: &'a DcId, cluster: &'a ClusterId, depth: u8) -> SettingCtx<'a> {
    let mut c = SettingCtx::process(dc);
    if depth >= 1 {
        c = c.with_cluster(cluster);
    }
    if depth >= 2 {
        c.tenant = Some(ACME);
    }
    if depth >= 3 {
        c = c.with_db(DB);
    }
    if depth >= 4 {
        c = c.with_object(ORDERS);
    }
    c
}

fn resolve(key: &str, rows: &[(At, Entry)], depth: u8) -> Resolved {
    let (etcd, t) = layers(key, rows, &[]);
    let (d, c) = (dc(), c1());
    let cx = ctx(&d, &c, depth);
    let tenant = (depth >= 2).then_some(&t);
    resolve_layered(&etcd, tenant, key, &cx).unwrap()
}

#[test]
fn chapter_08_worked_example() {
    // Global bounds max 8; tenant acme sets 6 with bounds max 5; table
    // orders sets 7. Each step of the walk, as (rows so far, ctx
    // depth, value, bounds max, source, clamped).
    let key = MAX_PER_TABLE.key;
    let global = (At::Global, bounds(Bounds::max(8.0)));
    let tenant = (At::Tenant, both(6, Bounds::max(5.0)));
    let object = (At::Object, val(7));
    let table: Vec<(Vec<(At, Entry)>, u8, i64, f64, ScopeKind, bool)> = vec![
        (vec![], 4, 5, 64.0, ScopeKind::Default, false),
        (vec![global.clone()], 4, 5, 8.0, ScopeKind::Default, false),
        (
            vec![global.clone(), tenant.clone()],
            2,
            6,
            5.0,
            ScopeKind::Tenant,
            false,
        ),
        (
            vec![global, tenant, object],
            4,
            5,
            5.0,
            ScopeKind::Object,
            true,
        ),
    ];
    for (i, (rows, depth, v, max, source, clamped)) in table.into_iter().enumerate() {
        let r = resolve(key, &rows, depth);
        assert_eq!(r.value, Value::Int(v), "row {i}");
        assert_eq!(r.bounds.max, Some(max), "row {i}");
        assert_eq!(r.bounds.min, Some(1.0), "row {i}");
        assert_eq!(r.source, source, "row {i}");
        assert_eq!(r.clamped, clamped, "row {i}");
    }
}

#[test]
fn nearest_scope_wins() {
    let key = MAX_PER_TABLE.key;
    let all = vec![
        (At::Global, val(10)),
        (At::Dc, val(11)),
        (At::Cluster, val(12)),
        (At::Tenant, val(13)),
        (At::Database, val(14)),
        (At::Object, val(15)),
    ];
    // (ctx depth, rows dropped from the narrow end, value, source)
    let table = [
        (0, 0, 11, ScopeKind::Dc),
        (1, 0, 12, ScopeKind::Cluster),
        (2, 0, 13, ScopeKind::Tenant),
        (3, 0, 14, ScopeKind::Database),
        (4, 0, 15, ScopeKind::Object),
        (4, 1, 14, ScopeKind::Database),
        (4, 2, 13, ScopeKind::Tenant),
        (4, 3, 12, ScopeKind::Cluster),
        (4, 4, 11, ScopeKind::Dc),
        (4, 5, 10, ScopeKind::Global),
        (4, 6, 5, ScopeKind::Default),
        // Without a cluster in the context, cluster rows don't apply.
        (0, 3, 11, ScopeKind::Dc),
    ];
    for (depth, drop, v, source) in table {
        let rows = &all[..all.len() - drop];
        let r = resolve(key, rows, depth);
        assert_eq!(
            (r.value, r.source),
            (Value::Int(v), source),
            "depth {depth}, drop {drop}"
        );
        assert!(!r.clamped);
    }
}

#[test]
fn wider_bounds_clamp_lock_and_filter() {
    let n = MAX_PER_TABLE.key;
    let m = MODE.key;
    let s = |x: &str| Entry {
        value: Some(Value::from(x)),
        bounds: None,
    };
    // (key, rows, value, source, clamped, ignored)
    let table: Vec<(
        &str,
        Vec<(At, Entry)>,
        Value,
        ScopeKind,
        bool,
        Vec<(ScopeKind, Ignored)>,
    )> = vec![
        // Hard bounds clamp a stored value that predates them.
        (
            n,
            vec![(At::Object, val(500))],
            Value::Int(64),
            ScopeKind::Object,
            true,
            vec![],
        ),
        // A dc minimum raises the tenant's value.
        (
            n,
            vec![(At::Dc, bounds(min(3.0))), (At::Tenant, val(2))],
            Value::Int(3),
            ScopeKind::Tenant,
            true,
            vec![],
        ),
        // A scope's own bounds limit only narrower scopes.
        (
            n,
            vec![(At::Tenant, both(9, Bounds::max(4.0)))],
            Value::Int(9),
            ScopeKind::Tenant,
            false,
            vec![],
        ),
        // Bounds from several scopes intersect.
        (
            n,
            vec![
                (At::Global, bounds(Bounds::max(20.0))),
                (At::Cluster, bounds(Bounds::max(10.0))),
                (At::Tenant, bounds(min(12.0))),
                (At::Object, val(30)),
            ],
            Value::Int(10),
            ScopeKind::Object,
            true,
            vec![],
        ),
        // A lock at dc ignores every narrower value; the dc's own value
        // stands.
        (
            n,
            vec![
                (At::Dc, both(7, Bounds::locked())),
                (At::Tenant, val(3)),
                (At::Object, val(4)),
            ],
            Value::Int(7),
            ScopeKind::Dc,
            false,
            vec![
                (ScopeKind::Tenant, Ignored::Locked),
                (ScopeKind::Object, Ignored::Locked),
            ],
        ),
        // An allowed list at global ignores a value outside it; the next
        // wider value wins.
        (
            m,
            vec![
                (
                    At::Global,
                    Entry {
                        value: None,
                        bounds: Some(Bounds::allowed([
                            Value::from("off"),
                            Value::from("recommend"),
                        ])),
                    },
                ),
                (At::Tenant, s("off")),
                (At::Object, s("on")),
            ],
            Value::from("off"),
            ScopeKind::Tenant,
            false,
            vec![(ScopeKind::Object, Ignored::NotAllowed)],
        ),
        // A value of the wrong type (a newer binary's) is ignored.
        (
            m,
            vec![(At::Tenant, val(3))],
            Value::from("recommend"),
            ScopeKind::Default,
            false,
            vec![(ScopeKind::Tenant, Ignored::WrongType)],
        ),
        // The default is clamped by bounds tightened above it.
        (
            n,
            vec![(At::Tenant, bounds(Bounds::max(2.0)))],
            Value::Int(2),
            ScopeKind::Default,
            true,
            vec![],
        ),
    ];
    for (i, (key, rows, v, source, clamped, ignored)) in table.into_iter().enumerate() {
        let r = resolve(key, &rows, 4);
        assert_eq!(r.value, v, "case {i}");
        assert_eq!(r.source, source, "case {i}");
        assert_eq!(r.clamped, clamped, "case {i}");
        assert_eq!(r.ignored, ignored, "case {i}");
    }
}

#[test]
fn keys_resolve_only_at_accepted_scopes() {
    // doc.wildcard_index accepts object, database, tenant and global: dc
    // and cluster rows are skipped.
    let key = WILDCARD.key;
    let t = |b: bool| Entry {
        value: Some(Value::Bool(b)),
        bounds: None,
    };
    let r = resolve(
        key,
        &[
            (At::Global, t(true)),
            (At::Dc, t(false)),
            (At::Cluster, t(false)),
        ],
        4,
    );
    assert_eq!((r.value, r.source), (Value::Bool(true), ScopeKind::Global));
}

#[test]
fn ancestors_sit_between_the_tenant_and_the_cluster() {
    // The tenant tree a → b → c, from c's catalog: a (root) and b (parent)
    // rows are ancestor copies. Nearest level wins; every ancestor's bounds
    // apply.
    let key = LOCK_MINUTES.key;
    let (a, b) = (TenantId(1), TenantId(2));
    let rows = vec![
        (At::Global, val(5)),
        (At::Ancestor(1), both(30, Bounds::max(60.0))),
        (At::Ancestor(2), val(90)),
    ];
    let (etcd, ts) = layers(key, &rows, &[a, b]);
    let d = dc();
    let r = ts.resolve(&etcd, key, &ts.ctx(&d)).unwrap();
    assert_eq!(r.value, Value::Int(60));
    assert_eq!(r.source, ScopeKind::Ancestor(b));
    assert!(r.clamped);
    // Without the ancestry (step 6 not run yet), ancestor rows don't apply.
    let (etcd, ts) = layers(key, &rows, &[]);
    let r = ts.resolve(&etcd, key, &ts.ctx(&d)).unwrap();
    assert_eq!((r.value, r.source), (Value::Int(5), ScopeKind::Global));
}

#[test]
fn a_context_for_another_tenant_is_refused() {
    let (etcd, ts) = layers(MAX_PER_TABLE.key, &[], &[]);
    let d = dc();
    let other = SettingCtx::process(&d).for_tenant(TenantId(9), &[]);
    let e = ts.resolve(&etcd, MAX_PER_TABLE.key, &other).unwrap_err();
    assert_eq!(e.code(), Some(sprig_common::SprigCode::Internal));
    let e = ts.resolve(&etcd, "no.such_key", &ts.ctx(&d)).unwrap_err();
    assert_eq!(e.code(), Some(sprig_common::SprigCode::SettingUnknown));
}

#[test]
fn results_are_memoized_per_etcd_snapshot() {
    let key = MAX_PER_TABLE.key;
    let (mut etcd, ts) = layers(key, &[(At::Tenant, val(9))], &[]);
    let (d, c) = (dc(), c1());
    let tctx = ts.ctx(&d);
    let a = ts.resolve(&etcd, key, &tctx).unwrap();
    let b = ts.resolve(&etcd, key, &tctx).unwrap();
    assert_eq!(a, b);
    assert_eq!(ts.memoized(), 1);
    // Another context is another entry.
    ts.resolve(&etcd, key, &tctx.with_cluster(&c)).unwrap();
    ts.resolve(&etcd, key, &tctx.with_db(DB)).unwrap();
    assert_eq!(ts.memoized(), 3);
    // An etcd change gets a new stamp: the memo is not served for it.
    let stamp = etcd.stamp();
    let base = SettingCtx::process(&d);
    etcd.set_bounds(Level::Global, &base, key, Bounds::max(4.0))
        .unwrap();
    assert_ne!(etcd.stamp(), stamp);
    let r = ts.resolve(&etcd, key, &tctx).unwrap();
    assert_eq!((r.value, r.clamped), (Value::Int(4), true));
    // The replaced entry doesn't grow the memo.
    assert_eq!(ts.memoized(), 3);
    // The memo stays bounded.
    for o in 0..(super::memo::CAP as u64 + 10) {
        ts.resolve(&etcd, key, &tctx.with_object(ObjectId(o)))
            .unwrap();
    }
    assert!(ts.memoized() <= super::memo::CAP);
}

#[test]
fn the_builder_shares_untouched_scopes_and_applies_resets() {
    let key = MAX_PER_TABLE.key;
    let mut b = TenantSettings::builder(registry(), ACME, 3);
    b.put(TenantScope::Tenant, key, val(9));
    b.put(TenantScope::Database(DB), key, val(8));
    b.put(
        TenantScope::Object(ORDERS),
        MODE.key,
        Entry {
            value: Some(Value::from("on")),
            bounds: None,
        },
    );
    b.put(TenantScope::Tenant, "future.key", val(1));
    b.put(TenantScope::Tenant, "sprig.catalog_version", val(3));
    let v3 = b.build();
    assert_eq!(v3.len(), 3);
    assert_eq!(v3.ignored_rows(), 1);
    assert!(v3.bytes() > 0);

    // v4: a reset at database scope (an empty row), the unknown key reset,
    // the object dropped.
    let mut e = v3.edit(4);
    e.put(TenantScope::Database(DB), key, Entry::default());
    e.put(TenantScope::Tenant, "future.key", Entry::default());
    e.remove_scope(TenantScope::Object(ORDERS));
    let v4 = e.build();
    assert_eq!(v4.len(), 1);
    assert_eq!(v4.ignored_rows(), 0);
    assert_eq!(v4.version(), 4);
    assert!(v4.entry(TenantScope::Database(DB), key).is_none());
    assert_eq!(v4.entry(TenantScope::Tenant, key), Some(&val(9)));
    assert_eq!(
        v4.changed_keys(&v3),
        BTreeSet::from([MAX_PER_TABLE.key, MODE.key])
    );
    assert!(v4.changed_keys(&v4).is_empty());

    // A full build of the same rows is equal.
    let mut f = TenantSettings::builder(registry(), ACME, 4);
    f.put(TenantScope::Tenant, key, val(9));
    assert_eq!(f.build(), v4);

    // An unreadable value replaces the row and is counted.
    let mut e = v4.edit(5);
    e.put_invalid(TenantScope::Tenant, key);
    let v5 = e.build();
    assert!(v5.is_empty());
    assert_eq!(v5.ignored_rows(), 1);
    // ... until a readable value comes back.
    let mut e = v5.edit(6);
    e.put(TenantScope::Tenant, key, val(2));
    let v6 = e.build();
    assert_eq!((v6.len(), v6.ignored_rows()), (1, 0));
}

#[test]
fn clamped_values_are_reported_with_warnings() {
    let n = MAX_PER_TABLE.key;
    let rows = vec![
        (At::Global, bounds(Bounds::max(8.0))),
        (At::Tenant, both(6, Bounds::max(5.0))),
        (At::Database, val(3)),
        (At::Object, val(7)),
        (At::Ancestor(1), val(40)),
    ];
    let (mut etcd, ts) = layers(n, &rows, &[TenantId(1)]);
    let d = dc();
    let db_of = |o: ObjectId| (o == ORDERS).then_some(DB);
    let out = ts.clamped(&etcd, &d, &db_of);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].scope, TenantScope::Object(ORDERS));
    assert_eq!(out[0].effective, Some(Value::Int(5)));
    assert_eq!(clamp_summary(&out).unwrap(), "1 value will be clamped");
    let w = out[0].warning(ACME);
    assert_eq!(w.scope, Scope::Object(ACME, ORDERS));
    assert!(w.message.contains("read as 5"), "{}", w.message);

    // Locking at dc ignores every tenant value: three warnings.
    let base = SettingCtx::process(&d);
    etcd.set_bounds(Level::Dc, &base, n, Bounds::locked())
        .unwrap();
    let out = ts.clamped(&etcd, &d, &db_of);
    assert_eq!(out.len(), 3);
    assert!(out.iter().all(|c| c.why == Some(Ignored::Locked)));
    assert_eq!(clamp_summary(&out).unwrap(), "3 values will be ignored");
    assert!(clamp_summary(&[]).is_none());
}

/// Random rows at every scope, split over the two layers, resolve as they
/// do in one store.
fn arb_entry() -> impl Strategy<Value = Option<Entry>> {
    let value = prop_oneof![Just(None), (-5_i64..80).prop_map(|v| Some(Value::Int(v)))];
    let bound = prop_oneof![
        3 => Just(None),
        1 => (0.0_f64..70.0).prop_map(|m| Some(Bounds::max(m.floor()))),
        1 => (0.0_f64..70.0).prop_map(|m| Some(min(m.floor()))),
        1 => Just(Some(Bounds::locked())),
        1 => Just(Some(Bounds::allowed([Value::Int(3), Value::Int(7), Value::Int(40)]))),
    ];
    prop_oneof![
        1 => Just(None),
        3 => (value, bound).prop_map(|(value, bounds)| {
            let e = Entry { value, bounds };
            (e != Entry::default()).then_some(e)
        }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn layering_matches_a_single_store(
        rows in prop::collection::vec(arb_entry(), 8),
        depth in 0_u8..5,
    ) {
        let key = MAX_PER_TABLE.key;
        let at = [
            At::Global, At::Dc, At::Cluster, At::Ancestor(1), At::Ancestor(2),
            At::Tenant, At::Database, At::Object,
        ];
        let present: Vec<(At, Entry)> = at
            .iter()
            .zip(rows)
            .filter_map(|(a, e)| e.map(|e| (*a, e)))
            .collect();
        let ancestry = [TenantId(1), TenantId(2)];
        let (etcd, ts) = layers(key, &present, &ancestry);
        // The same rows in one MemSettings, ancestors as their tenant rows.
        let mut one = MemSettings::new(registry());
        for (a, e) in &present {
            let scope = match a {
                At::Global => Scope::Global,
                At::Dc => Scope::Dc(dc()),
                At::Cluster => Scope::Cluster(c1()),
                At::Ancestor(t) => Scope::Tenant(TenantId(*t)),
                At::Tenant => Scope::Tenant(ACME),
                At::Database => Scope::Database(ACME, DB),
                At::Object => Scope::Object(ACME, ORDERS),
            };
            one.insert_row(scope, key, e.clone());
        }
        let (d, c) = (dc(), c1());
        let mut cx = ctx(&d, &c, depth);
        if depth >= 2 {
            cx.ancestry = &ancestry;
        }
        let tenant = (depth >= 2).then_some(&ts);
        let layered = resolve_layered(&etcd, tenant, key, &cx).unwrap();
        prop_assert_eq!(&layered, &one.resolve(key, &cx).unwrap());
        if let Some(ts) = tenant {
            // Memoized resolution agrees, twice.
            prop_assert_eq!(&layered, &ts.resolve(&etcd, key, &cx).unwrap());
            prop_assert_eq!(&layered, &ts.resolve(&etcd, key, &cx).unwrap());
        }
        // The result obeys the bounds of every scope wider than its source
        // (a scope's own bounds limit only narrower scopes).
        let included = |a: &At| match a {
            At::Global | At::Dc => true,
            At::Cluster => depth >= 1,
            At::Ancestor(_) | At::Tenant => depth >= 2,
            At::Database => depth >= 3,
            At::Object => depth >= 4,
        };
        let rank = |a: &At| at.iter().position(|b| format!("{b:?}") == format!("{a:?}")).unwrap();
        let source_rank = match layered.source {
            ScopeKind::Default => at.len(),
            ScopeKind::Global => 0,
            ScopeKind::Dc => 1,
            ScopeKind::Cluster => 2,
            ScopeKind::Ancestor(t) => 2 + usize::try_from(t.0).unwrap(),
            ScopeKind::Tenant => 5,
            ScopeKind::Database => 6,
            ScopeKind::Object => 7,
        };
        let mut wider: Bounds = MAX_PER_TABLE.hard.into();
        for (a, e) in &present {
            if included(a) && rank(a) < source_rank && let Some(b) = &e.bounds {
                wider = wider.intersect(b);
            }
        }
        let sane = wider.min.zip(wider.max).is_none_or(|(l, h)| l.ceil() <= h.floor());
        if sane && layered.source != ScopeKind::Default {
            prop_assert_eq!(wider.clamp(&layered.value), layered.value.clone());
            prop_assert!(!wider.locked);
        }
    }
}

// ---- Settings over live mirrors ----

async fn put_record(
    store: &Arc<MemStore>,
    stores: &SettingsStores,
    scope: &Scope,
    key: &str,
    e: Entry,
) {
    let rec = Record::new(SettingRecord {
        value: e.value,
        bounds: e.bounds,
        set_by: "op:test".to_owned(),
        set_at: "2026-11-08T00:00:00Z".to_owned(),
        reason: String::new(),
    });
    store
        .txn(Txn {
            then: vec![Op::Put {
                key: stores.record_key(scope, key).unwrap(),
                value: rec.encode().unwrap(),
                lease: None,
            }],
            ..Txn::default()
        })
        .await
        .unwrap();
}

async fn live() -> (Settings, Arc<MemStore>, SettingsStores) {
    let dc_store = Arc::new(MemStore::new(unique_tier()));
    let global = Arc::new(dc_store.with_tier(unique_tier()));
    let stores = SettingsStores::new(global.clone(), dc_store).unwrap();
    let live = LiveSettings::start(registry(), stores.clone()).unwrap();
    live.ready().await;
    (Settings::new(live), global, stores)
}

async fn settle() {
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
}

#[tokio::test]
async fn settings_resolve_watch_and_follow_tenant_snapshots() {
    let (settings, global, stores) = live().await;
    let key = MAX_PER_TABLE.key;
    let d = stores.dc_id().clone();
    let v1 = {
        let mut b = TenantSettings::builder(registry(), ACME, 1);
        b.put(TenantScope::Tenant, key, val(9));
        Arc::new(b.build())
    };
    let tctx = v1.ctx(&d);
    assert_eq!(settings.get(&MAX_PER_TABLE, &tctx, Some(&v1)), 9);
    // Process-level: the default, memoized.
    let p = SettingCtx::process(&d);
    assert_eq!(settings.get(&MAX_PER_TABLE, &p, None), 5);
    assert_eq!(settings.get(&MAX_PER_TABLE, &p, None), 5);

    let mut any = settings.watch(&MAX_PER_TABLE);
    let mut w = settings
        .watch_resolved(key, OwnedCtx::from_ctx(&tctx), Some(Arc::clone(&v1)))
        .unwrap();
    assert_eq!(w.current().value, Value::Int(9));

    // A tenant reload that changes another key: the key watch stays
    // quiet; the resolved watch doesn't wake.
    let v2 = {
        let mut e = v1.edit(2);
        e.put(
            TenantScope::Tenant,
            MODE.key,
            Entry {
                value: Some(Value::from("on")),
                bounds: None,
            },
        );
        Arc::new(e.build())
    };
    settings.tenant_loaded(&v2, &v2.changed_keys(&v1));
    assert!(!any.has_changed().unwrap());

    // An etcd bound tightening the key: both wake, the value is clamped.
    put_record(
        &global,
        &stores,
        &Scope::Global,
        key,
        bounds(Bounds::max(4.0)),
    )
    .await;
    let r = tokio::time::timeout(Duration::from_secs(5), w.changed())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((r.value.clone(), r.clamped), (Value::Int(4), true));
    assert!(any.has_changed().unwrap());
    any.borrow_and_update();
    assert_eq!(settings.get(&MAX_PER_TABLE, &tctx, Some(&v2)), 4);

    // The tenant lowers its value under the bound: the watch sees it.
    let v3 = {
        let mut e = v2.edit(3);
        e.put(TenantScope::Tenant, key, val(2));
        Arc::new(e.build())
    };
    settings.tenant_loaded(&v3, &v3.changed_keys(&v2));
    assert!(any.has_changed().unwrap());
    let r = tokio::time::timeout(Duration::from_secs(5), w.changed())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((r.value, r.clamped), (Value::Int(2), false));

    // An older snapshot arriving late doesn't move the watch back.
    settings.tenant_loaded(&v2, &BTreeSet::from([key]));
    settle().await;
    let quiet = tokio::time::timeout(Duration::from_millis(100), w.changed()).await;
    assert!(quiet.is_err(), "{quiet:?}");
    assert_eq!(w.current().value, Value::Int(2));

    // Eviction keeps a watched tenant; an unwatched one is forgotten.
    settings.tenant_evicted(ACME);
    drop(w);
    settings.tenant_evicted(ACME);

    // Clamp reports read the current etcd snapshot.
    let out = settings.clamped(&v2, &d, &|_| None);
    assert_eq!(out.len(), 1);
}
