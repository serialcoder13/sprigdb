#![allow(clippy::unwrap_used, clippy::float_cmp)]

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use sprig_common::ids::{ClusterId, DcId, TenantId};
use sprig_settings::{Bounds, Level, Registry};

use super::*;
use crate::autoindex::model::{AutoIndexId, Role};
use crate::autoindex::store::{Definition, Model, StoredKind};

const TENANT: TenantId = TenantId(42);
const DB: DbId = DbId(1);
const OBJ: ObjectId = ObjectId(1201);

fn dc() -> DcId {
    DcId::new("mum1").unwrap()
}

fn cluster() -> ClusterId {
    ClusterId::new("mum1-c7").unwrap()
}

fn mem() -> MemSettings<'static> {
    MemSettings::new(Registry::global().unwrap())
}

#[test]
fn defaults_resolve_to_kirans_values() {
    let s = mem();
    let dc = dc();
    let ctx = SettingCtx::process(&dc)
        .for_tenant(TENANT, &[])
        .with_db(DB)
        .with_object(OBJ);
    let p = ObjectPolicy::resolve(&s, &ctx).unwrap();
    assert_eq!(p.mode, Mode::On);
    assert_eq!(p.trigger.min_runs, 50);
    assert_eq!(p.trigger.window_hours, 24);
    assert_eq!(p.trigger.p95_threshold_ms, 100.0);
    assert_eq!(p.trigger.top_n, 10);
    assert_eq!(p.trigger.min_examined_per_returned, 100.0);
    assert_eq!(p.budget.max_per_table, 5);
    assert_eq!(p.budget.max_storage_pct, 20.0);
    assert_eq!(p.observe.min_improvement_pct, 30.0);
    assert_eq!(p.observe.max_regression_pct, 10.0);
    assert_eq!(p.observe.unused_drop_days, 30);
    assert_eq!(p.cost.build_rate_bytes_per_sec, 20e6);
    assert_eq!(p.generation.max_key_bytes, 512);
    assert!(p.used.values().all(|u| u.source == ScopeKind::Default));
    assert_eq!(
        p.evidence()["auto_index.max_per_table"],
        serde_json::json!({"value": 5, "source": "default", "clamped": false})
    );
    // Without a replica the margin applies.
    assert_eq!(p.cost_for(None, false).estimate_margin, Some(1.5));
    assert_eq!(p.cost_for(Some(50e6), true).build_rate_bytes_per_sec, 50e6);
}

#[test]
fn chapter_08_example_tightened_bounds_clamp_the_object() {
    // Global bounds max 8; tenant 6 with bounds max 5; table 7 → 5,
    // clamped, source object.
    let mut s = mem();
    let dc = dc();
    let ctx = SettingCtx::process(&dc)
        .for_tenant(TENANT, &[])
        .with_db(DB)
        .with_object(OBJ);
    let key = "auto_index.max_per_table";
    s.set_bounds(Level::Global, &ctx, key, Bounds::max(8.0))
        .unwrap();
    s.set_value(Level::Tenant, &ctx, key, &Value::Int(6))
        .unwrap();
    s.set_value(Level::Object, &ctx, key, &Value::Int(7))
        .unwrap();
    // Tightening is never refused, even with a narrower value above it.
    s.set_bounds(Level::Tenant, &ctx, key, Bounds::max(5.0))
        .unwrap();
    let p = ObjectPolicy::resolve(&s, &ctx).unwrap();
    assert_eq!(p.budget.max_per_table, 5);
    let u = &p.used[key];
    assert_eq!(u.source, ScopeKind::Object);
    assert!(u.clamped);
    // The tenant itself keeps 6: a scope's bounds limit narrower ones only.
    let tctx = SettingCtx::process(&dc).for_tenant(TENANT, &[]);
    assert_eq!(
        ObjectPolicy::resolve(&s, &tctx)
            .unwrap()
            .budget
            .max_per_table,
        6
    );
}

#[test]
fn mode_off_on_one_table_while_the_tenant_is_on() {
    let mut s = mem();
    let dc = dc();
    let ctx = SettingCtx::process(&dc)
        .for_tenant(TENANT, &[])
        .with_db(DB)
        .with_object(OBJ);
    s.set_value(Level::Tenant, &ctx, "auto_index.mode", &Value::from("on"))
        .unwrap();
    s.set_value(Level::Object, &ctx, "auto_index.mode", &Value::from("off"))
        .unwrap();
    assert_eq!(ObjectPolicy::resolve(&s, &ctx).unwrap().mode, Mode::Off);
    let other = ctx.with_object(ObjectId(9));
    assert_eq!(ObjectPolicy::resolve(&s, &other).unwrap().mode, Mode::On);
    // ALLOWED at the tenant: an object's 'off' is ignored.
    s.set_bounds(
        Level::Tenant,
        &ctx,
        "auto_index.mode",
        Bounds::allowed([Value::from("on"), Value::from("recommend")]),
    )
    .unwrap();
    assert_eq!(ObjectPolicy::resolve(&s, &ctx).unwrap().mode, Mode::On);
    // Bad text never parses into a mode.
    assert!(
        s.set_value(
            Level::Object,
            &ctx,
            "auto_index.mode",
            &Value::from("maybe")
        )
        .is_err()
    );
}

#[test]
fn scan_keys_refuse_cluster_scope() {
    let mut s = mem();
    let dc = dc();
    let cl = cluster();
    let ctx = SettingCtx::process(&dc).with_cluster(&cl);
    let e = s
        .set_value(
            Level::Cluster,
            &ctx,
            "auto_index.scan_min_runs",
            &Value::Int(9),
        )
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::SettingScopeNotAllowed));
    // scan_min_sessions is bounded at 8 by the registry.
    let tctx = SettingCtx::process(&dc).for_tenant(TENANT, &[]);
    assert!(
        s.set_value(
            Level::Tenant,
            &tctx,
            "auto_index.scan_min_sessions",
            &Value::Int(9)
        )
        .is_err()
    );
}

#[test]
fn shared_limits_only_tighten_at_data_scopes() {
    let mut s = mem();
    let dc = dc();
    let cl = cluster();
    let cctx = SettingCtx::process(&dc).with_cluster(&cl);
    let octx = SettingCtx::process(&dc)
        .with_cluster(&cl)
        .for_tenant(TENANT, &[])
        .with_db(DB)
        .with_object(OBJ);
    let lag = "auto_index.abort_replica_lag_seconds";
    s.set_value(
        Level::Cluster,
        &cctx,
        "auto_index.max_builds_per_cluster",
        &Value::Int(2),
    )
    .unwrap();
    s.set_value(
        Level::Cluster,
        &cctx,
        "auto_index.build_window",
        &Value::from("01:00-05:00"),
    )
    .unwrap();
    // A tenant asking for more lag and more builds gets the cluster's.
    s.set_value(Level::Tenant, &octx, lag, &Value::Int(600))
        .unwrap();
    s.set_value(
        Level::Tenant,
        &octx,
        "auto_index.max_builds_per_cluster",
        &Value::Int(4),
    )
    .unwrap();
    let l = SharedLimits::resolve(&s, &cctx, &octx).unwrap();
    assert_eq!(l.abort_replica_lag_seconds, 300);
    assert_eq!(l.max_builds_per_cluster, 2);
    assert_eq!(
        l.build_window,
        BuildWindow::Fixed {
            start: 60,
            end: 300
        }
    );
    // Tighter values at data scopes apply.
    s.set_value(Level::Object, &octx, lag, &Value::Int(60))
        .unwrap();
    s.set_value(
        Level::Database,
        &octx,
        "auto_index.build_window",
        &Value::from("03:00-07:00"),
    )
    .unwrap();
    let l = SharedLimits::resolve(&s, &cctx, &octx).unwrap();
    assert_eq!(l.abort_replica_lag_seconds, 60);
    assert_eq!(
        l.build_window,
        BuildWindow::Fixed {
            start: 180,
            end: 300
        }
    );
}

#[test]
fn build_windows() {
    assert_eq!(BuildWindow::parse("auto"), Some(BuildWindow::Auto));
    assert_eq!(
        BuildWindow::parse("22:30-02:00"),
        Some(BuildWindow::Fixed {
            start: 1350,
            end: 120
        })
    );
    assert_eq!(BuildWindow::parse("25:00-01:00"), None);
    let w = BuildWindow::parse("22:30-02:00").unwrap();
    assert!(w.contains(23 * 60) && w.contains(60) && !w.contains(3 * 60));
    // Overlap across midnight.
    let c = BuildWindow::parse("23:00-03:00").unwrap();
    assert_eq!(
        c.tighten(w),
        BuildWindow::Fixed {
            start: 1380,
            end: 120
        }
    );
    // No overlap: the cluster's window stands.
    let d = BuildWindow::parse("10:00-11:00").unwrap();
    assert_eq!(c.tighten(d), c);
    assert_eq!(BuildWindow::Auto.tighten(d), d);
    assert_eq!(c.tighten(BuildWindow::Auto), c);
}

#[test]
fn day_caps_apply_at_each_level_they_are_set() {
    let mut s = mem();
    let dc = dc();
    let tctx = SettingCtx::process(&dc).for_tenant(TENANT, &[]);
    let key = "auto_index.max_new_per_day";
    s.set_value(Level::Database, &tctx.with_db(DB), key, &Value::Int(2))
        .unwrap();
    s.set_value(
        Level::Object,
        &tctx.with_db(DbId(2)).with_object(ObjectId(7)),
        key,
        &Value::Int(1),
    )
    .unwrap();
    let caps = DayCaps::resolve(
        &s,
        &tctx,
        &[(DB, OBJ), (DB, ObjectId(8)), (DbId(2), ObjectId(7))],
    )
    .unwrap();
    assert_eq!(caps.tenant, 5);
    assert_eq!(caps.databases, BTreeMap::from([(DB, 2)]));
    assert_eq!(caps.objects, BTreeMap::from([(ObjectId(7), 1)]));
    // Two new in database 1: a third there waits; database 2 still has room.
    let created = [(DB, OBJ), (DB, ObjectId(8))];
    assert!(!caps.admits(&created, DB, OBJ));
    assert!(caps.admits(&created, DbId(2), ObjectId(9)));
    // Object 7's own cap.
    assert!(!caps.admits(&[(DbId(2), ObjectId(7))], DbId(2), ObjectId(7)));
    // The tenant's 5.
    let five = [(DbId(3), ObjectId(1)); 5];
    assert!(!caps.admits(&five, DbId(4), ObjectId(2)));
}

#[test]
fn routes() {
    assert_eq!(route(Mode::Off, false, true, true), Route::Skip);
    assert_eq!(
        route(Mode::On, false, true, false),
        Route::ValidateThenRollOut
    );
    assert_eq!(
        route(Mode::Recommend, false, true, true),
        Route::ValidateThenRecommend
    );
    assert_eq!(route(Mode::Recommend, false, true, false), Route::Recommend);
    assert_eq!(route(Mode::Recommend, false, false, true), Route::Recommend);
    // The fast trigger on a huge table only recommends.
    assert_eq!(
        route(Mode::On, true, true, true),
        Route::ValidateThenRecommend
    );
    assert_eq!(
        Route::Recommend.first_state(),
        Some(IndexState::Recommended)
    );
}

fn dropped(def: &IndexDef, reason: DropReason, at: SystemTime) -> AutoIndexRow {
    AutoIndexRow {
        index_id: AutoIndexId(1),
        object: def.object,
        name: def.name(),
        model: Model::Sql,
        kind: StoredKind::BTree,
        definition: Definition::of(def),
        state: IndexState::Dropped,
        prior_state: None,
        candidate_id: 1,
        validation_id: None,
        decision_id: None,
        est_benefit_ms_h: 1.0,
        est_write_ms_h: 1.0,
        est_bytes: 1,
        actual_bytes: None,
        shards: BTreeMap::new(),
        baseline: BTreeMap::new(),
        observed: None,
        last_used_at: None,
        drop_reason: Some(reason),
        created_at: at,
        state_changed_at: at,
    }
}

#[test]
fn dampers_block_harmful_recreation_and_hold_cooldowns() {
    let now = SystemTime::now();
    let day = Duration::from_secs(86_400);
    let d = Dampers {
        recreate_block: 14 * day,
        cooldown: day,
    };
    let def = IndexDef::btree(OBJ, &["customer_id", "created_at"]);
    let fp = Fingerprint(7);
    // Dropped for no gain 3 days ago: blocked.
    let rows = [dropped(&def, DropReason::NoGain, now - 3 * day)];
    assert_eq!(
        d.check(&def, &[fp], &rows, &[], now),
        Damped::RecreateBlocked(DropReason::NoGain)
    );
    assert_eq!(
        d.check(&def, &[fp], &rows, &[], now).reason().unwrap(),
        "recreate_blocked:no_gain"
    );
    // 15 days ago: allowed, but counted as churn within 30 days.
    let rows = [dropped(&def, DropReason::Regression, now - 15 * day)];
    assert_eq!(d.check(&def, &[fp], &rows, &[], now), Damped::Allow);
    assert!(record_churn(&def, &rows, now));
    // Unused drops don't block.
    let rows = [dropped(&def, DropReason::Unused, now - day)];
    assert_eq!(d.check(&def, &[fp], &rows, &[], now), Damped::Allow);
    // Another definition isn't blocked.
    let other = IndexDef::btree(OBJ, &["customer_id"]);
    let rows = [dropped(&def, DropReason::NoGain, now - day)];
    assert_eq!(d.check(&other, &[fp], &rows, &[], now), Damped::Allow);
    assert!(!record_churn(&other, &rows, now));
    // A candidate for the fingerprint decided 2 hours ago: cooldown.
    let decided = CandidateRow {
        candidate_id: 1,
        run_id: 1,
        object: OBJ,
        def_hash: other.def_hash(),
        kind: StoredKind::BTree,
        definition: Definition::of(&other),
        covering: false,
        fingerprints: vec![(fp, Role::Primary)],
        status: crate::autoindex::store::CandidateStatus::Rejected,
        reason: None,
        benefit_ms_h: None,
        write_ms_h: None,
        est_bytes: None,
        build_seconds: None,
        evidence: crate::autoindex::model::Evidence::default(),
        created_at: now - Duration::from_secs(7200),
    };
    assert_eq!(
        d.check(&other, &[fp], &[], std::slice::from_ref(&decided), now),
        Damped::Cooldown
    );
    assert_eq!(
        d.check(&other, &[Fingerprint(8)], &[], &[decided], now),
        Damped::Allow
    );
}

/// A simulated month: a definition dropped as harmful is never proposed
/// again inside the block, so it is created at most once per
/// `recreate_block_days + cooldown` (chapter 13's stability test, in
/// miniature).
#[test]
fn simulated_month_never_flaps() {
    let day = Duration::from_secs(86_400);
    let d = Dampers {
        recreate_block: 14 * day,
        cooldown: day,
    };
    let def = IndexDef::btree(OBJ, &["a"]);
    let start = SystemTime::UNIX_EPOCH + 1000 * day;
    let mut dropped_rows: Vec<AutoIndexRow> = Vec::new();
    let mut creations = 0;
    let mut live_until: Option<SystemTime> = None;
    for h in 0..(30 * 24) {
        let now = start + Duration::from_secs(3600 * h);
        if let Some(until) = live_until {
            if now >= until {
                // Observation drops it as no_gain after 7 days.
                dropped_rows.push(dropped(&def, DropReason::NoGain, now));
                live_until = None;
            }
            continue;
        }
        if d.check(&def, &[Fingerprint(1)], &dropped_rows, &[], now) == Damped::Allow {
            creations += 1;
            live_until = Some(now + 7 * day);
        }
    }
    // Day 0 and day 21 (7 observing + 14 blocked): twice in 30 days.
    assert_eq!(creations, 2);
}

/// Decision E2 (confirmed): `auto_index.min_storage_budget_mb` defaults to
/// 64, is set by tenants at tenant, database and object scope (not
/// operator-only) and by operators at global, dc and cluster scope, and
/// resolves per object: a table's value overrides its tenant's.
#[test]
fn storage_floor_resolves_per_object_and_a_table_overrides_its_tenant() {
    let key = "auto_index.min_storage_budget_mb";
    let reg = Registry::global().unwrap();
    let k = reg.get(key).unwrap();
    assert!(!k.operator_only());
    for sc in [
        sprig_settings::ScopeKind::Object,
        sprig_settings::ScopeKind::Database,
        sprig_settings::ScopeKind::Tenant,
        sprig_settings::ScopeKind::Cluster,
        sprig_settings::ScopeKind::Dc,
        sprig_settings::ScopeKind::Global,
    ] {
        assert!(k.scopes().contains(sc), "{sc:?}");
    }
    let mb = 1024.0 * 1024.0;
    let mut s = mem();
    let dc = dc();
    let ctx = SettingCtx::process(&dc)
        .for_tenant(TENANT, &[])
        .with_db(DB)
        .with_object(OBJ);
    assert_eq!(
        ObjectPolicy::resolve(&s, &ctx)
            .unwrap()
            .budget
            .min_storage_bytes,
        64.0 * mb
    );
    s.set_value(Level::Global, &ctx, key, &Value::Int(32))
        .unwrap();
    s.set_value(Level::Tenant, &ctx, key, &Value::Int(128))
        .unwrap();
    s.set_value(Level::Database, &ctx, key, &Value::Int(96))
        .unwrap();
    s.set_value(Level::Object, &ctx, key, &Value::Int(8))
        .unwrap();
    assert_eq!(
        ObjectPolicy::resolve(&s, &ctx)
            .unwrap()
            .budget
            .min_storage_bytes,
        8.0 * mb
    );
    // A sibling table in the same database gets the database's value; one
    // in another database the tenant's.
    let sibling = ctx.with_object(ObjectId(9));
    assert_eq!(
        ObjectPolicy::resolve(&s, &sibling)
            .unwrap()
            .budget
            .min_storage_bytes,
        96.0 * mb
    );
    let elsewhere = ctx.with_db(DbId(2)).with_object(ObjectId(10));
    assert_eq!(
        ObjectPolicy::resolve(&s, &elsewhere)
            .unwrap()
            .budget
            .min_storage_bytes,
        128.0 * mb
    );
}
