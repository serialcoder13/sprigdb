//! Explanations: chapter 08's worked example step by step, locks, allowed
//! lists, scopes not accepted, a clamped default, and a property: the
//! steps always agree with resolution.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::borrow::Cow;
use std::sync::OnceLock;

use proptest::prelude::*;
use sprig_common::ids::{ClusterId, DbId, DcId, ObjectId, TenantId};

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

fn registry() -> &'static Registry {
    static R: OnceLock<Registry> = OnceLock::new();
    R.get_or_init(|| Registry::new([&MAX_PER_TABLE as &dyn AnySetting, &MODE, &WILDCARD]).unwrap())
}

const ACME: TenantId = TenantId(42);
const ROOT: TenantId = TenantId(7);
const DB: DbId = DbId(1);
const ORDERS: ObjectId = ObjectId(105);

fn dc() -> DcId {
    DcId::new("mum1").unwrap()
}

fn entry(value: Option<Value>, bounds: Option<Bounds>) -> Entry {
    Entry { value, bounds }
}

fn kinds(e: &Explanation) -> Vec<(ScopeKind, StepOutcome)> {
    e.steps.iter().map(|s| (s.kind, s.outcome)).collect()
}

/// Chapter 08's example: global bounds max 8; tenant 6 with bounds max
/// 5; table `orders` 7 → 5, clamped, source object.
#[test]
fn the_chapter_example_step_by_step() {
    let d = dc();
    let mut etcd = MemSettings::new(registry());
    etcd.set_bounds(
        Level::Global,
        &SettingCtx::process(&d),
        MAX_PER_TABLE.key,
        Bounds::max(8.0),
    )
    .unwrap();
    let mut b = TenantSettings::builder(registry(), ACME, 3);
    b.put(
        TenantScope::Tenant,
        MAX_PER_TABLE.key,
        entry(Some(Value::Int(6)), Some(Bounds::max(5.0))),
    );
    b.put(
        TenantScope::Object(ORDERS),
        MAX_PER_TABLE.key,
        entry(Some(Value::Int(7)), None),
    );
    let ts = b.build();
    let ctx = ts.ctx(&d).with_db(DB).with_object(ORDERS);
    let e = explain_layered(&etcd, Some(&ts), MAX_PER_TABLE.key, &ctx).unwrap();
    assert_eq!(
        e.resolved,
        resolve_layered(&etcd, Some(&ts), MAX_PER_TABLE.key, &ctx).unwrap()
    );
    assert_eq!(e.resolved.value, Value::Int(5));
    assert_eq!(e.resolved.source, ScopeKind::Object);
    assert!(e.resolved.clamped);
    assert_eq!(
        kinds(&e),
        vec![
            (ScopeKind::Default, StepOutcome::Overridden),
            (ScopeKind::Global, StepOutcome::BoundsOnly),
            (ScopeKind::Dc, StepOutcome::Unset),
            (ScopeKind::Tenant, StepOutcome::Overridden),
            (ScopeKind::Database, StepOutcome::Unset),
            (ScopeKind::Object, StepOutcome::Effective),
        ]
    );
    let hard = |max: f64| Bounds {
        min: Some(1.0),
        max: Some(max),
        ..Bounds::default()
    };
    assert_eq!(e.steps[1].bounds_in_force, hard(64.0));
    assert_eq!(e.steps[3].bounds_in_force, hard(8.0));
    assert_eq!(e.steps[5].bounds_in_force, hard(5.0));
    assert_eq!(e.steps[5].clamped_to, Some(Value::Int(5)));
    assert_eq!(e.steps[3].clamped_to, None);
    assert_eq!(e.steps[1].scope, Some(Scope::Global));
    assert_eq!(e.steps[0].scope, None);
}

#[test]
fn locks_allowed_lists_and_scopes_not_accepted() {
    let d = dc();
    let c = ClusterId::new("mum1-c1").unwrap();
    let mut etcd = MemSettings::new(registry());
    let pctx = SettingCtx::process(&d).with_cluster(&c);
    etcd.set_bounds(
        Level::Global,
        &pctx,
        MODE.key,
        Bounds::allowed([Value::from("off"), Value::from("recommend")]),
    )
    .unwrap();
    etcd.set_value(Level::Dc, &pctx, MODE.key, &Value::from("off"))
        .unwrap();
    etcd.set_bounds(Level::Cluster, &pctx, MODE.key, Bounds::locked())
        .unwrap();
    let mut b = TenantSettings::builder(registry(), ACME, 3);
    b.set_ancestry(vec![ROOT]);
    b.put(
        TenantScope::Ancestor(ROOT),
        MODE.key,
        entry(Some(Value::from("on")), None),
    );
    b.put(
        TenantScope::Tenant,
        MODE.key,
        entry(Some(Value::from("recommend")), None),
    );
    let ts = b.build();
    let ctx = ts.ctx(&d).with_cluster(&c);
    let e = explain_layered(&etcd, Some(&ts), MODE.key, &ctx).unwrap();
    assert_eq!(
        e.resolved,
        resolve_layered(&etcd, Some(&ts), MODE.key, &ctx).unwrap()
    );
    assert_eq!(e.resolved.value, Value::from("off"));
    assert_eq!(e.resolved.source, ScopeKind::Dc);
    assert_eq!(
        kinds(&e),
        vec![
            (ScopeKind::Default, StepOutcome::Overridden),
            (ScopeKind::Global, StepOutcome::BoundsOnly),
            (ScopeKind::Dc, StepOutcome::Effective),
            (ScopeKind::Cluster, StepOutcome::BoundsOnly),
            (
                ScopeKind::Ancestor(ROOT),
                StepOutcome::Ignored(Ignored::Locked)
            ),
            (ScopeKind::Tenant, StepOutcome::Ignored(Ignored::Locked)),
        ]
    );
    // Without the lock: the ancestor's "on" isn't allowed; the tenant's
    // "recommend" wins.
    etcd.reset_bounds(Level::Cluster, &pctx, MODE.key).unwrap();
    let e = explain_layered(&etcd, Some(&ts), MODE.key, &ctx).unwrap();
    assert_eq!(e.resolved.source, ScopeKind::Tenant);
    assert_eq!(
        e.steps[4].outcome,
        StepOutcome::Ignored(Ignored::NotAllowed)
    );
    assert_eq!(e.steps[5].outcome, StepOutcome::Effective);
    // A key that doesn't accept the cluster scope.
    let e = explain_layered(&etcd, Some(&ts), WILDCARD.key, &ctx).unwrap();
    assert_eq!(e.steps[2].kind, ScopeKind::Dc);
    assert_eq!(e.steps[2].outcome, StepOutcome::NotAccepted);
    assert_eq!(e.steps[3].outcome, StepOutcome::NotAccepted);
    assert_eq!(e.steps[0].outcome, StepOutcome::Effective);
}

#[test]
fn a_default_outside_tightened_bounds_is_clamped() {
    let d = dc();
    let mut etcd = MemSettings::new(registry());
    etcd.set_bounds(
        Level::Global,
        &SettingCtx::process(&d),
        MAX_PER_TABLE.key,
        Bounds::max(3.0),
    )
    .unwrap();
    let e = explain_layered(&etcd, None, MAX_PER_TABLE.key, &SettingCtx::process(&d)).unwrap();
    assert_eq!(e.resolved.value, Value::Int(3));
    assert!(e.resolved.clamped);
    assert_eq!(e.steps[0].outcome, StepOutcome::Effective);
    assert_eq!(e.steps[0].clamped_to, Some(Value::Int(3)));
    assert!(
        explain_layered(&etcd, None, "no.such_key", &SettingCtx::process(&d)).is_err(),
        "39081"
    );
}

/// One optional row per scope of the full chain.
fn arb_entry() -> impl Strategy<Value = Option<Entry>> {
    let value = prop::option::of(0i64..20).prop_map(|v| v.map(Value::Int));
    let bounds = prop::option::of(
        (
            prop::option::of(0f64..10.0),
            prop::option::of(5f64..20.0),
            any::<bool>(),
        )
            .prop_map(|(min, max, locked)| Bounds {
                min: min.map(f64::floor),
                max: max.map(f64::floor),
                allowed: None,
                locked,
            }),
    );
    prop::option::of((value, bounds).prop_map(|(value, bounds)| Entry { value, bounds }))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Whatever the rows, the explanation's result is resolution's, the
    /// step marked effective is the source, and the last step's bounds in
    /// force plus its own bounds give the final bounds.
    #[test]
    fn steps_agree_with_resolution(rows in prop::collection::vec(arb_entry(), 7)) {
        let d = dc();
        let c = ClusterId::new("mum1-c1").unwrap();
        let mut etcd = MemSettings::new(registry());
        let pctx = SettingCtx::process(&d).with_cluster(&c);
        let key = MAX_PER_TABLE.key;
        let levels = [Level::Global, Level::Dc, Level::Cluster];
        for (level, row) in levels.iter().zip(&rows) {
            if let Some(e) = row {
                if let Some(v) = &e.value { etcd.set_value(*level, &pctx, key, v).ok(); }
                if let Some(b) = &e.bounds { etcd.set_bounds(*level, &pctx, key, b.clone()).ok(); }
            }
        }
        let mut b = TenantSettings::builder(registry(), ACME, 1);
        b.set_ancestry(vec![ROOT]);
        let tscopes = [
            TenantScope::Ancestor(ROOT),
            TenantScope::Tenant,
            TenantScope::Database(DB),
            TenantScope::Object(ORDERS),
        ];
        for (s, row) in tscopes.iter().zip(&rows[3..]) {
            if let Some(e) = row { b.put(*s, key, e.clone()); }
        }
        let ts = b.build();
        let ctx = ts.ctx(&d).with_cluster(&c).with_db(DB).with_object(ORDERS);
        let e = explain_layered(&etcd, Some(&ts), key, &ctx).unwrap();
        let r = resolve_layered(&etcd, Some(&ts), key, &ctx).unwrap();
        prop_assert_eq!(&e.resolved, &r);
        let effective: Vec<_> = e
            .steps
            .iter()
            .filter(|s| s.outcome == StepOutcome::Effective)
            .collect();
        prop_assert_eq!(effective.len(), 1);
        prop_assert_eq!(effective[0].kind, r.source);
        let last = e.steps.last().unwrap();
        let fin = match last.entry.as_ref().and_then(|x| x.bounds.as_ref()) {
            Some(b) if last.accepted && !last.bounds_in_force.locked => last.bounds_in_force.intersect(b),
            _ => last.bounds_in_force.clone(),
        };
        prop_assert_eq!(fin, r.bounds);
    }
}
