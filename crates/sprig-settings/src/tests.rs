//! Resolution and write-check tests, including chapter 08's worked example
//! and chapter 14's `a → b → c` tenant tree.

use std::borrow::Cow;

use sprig_common::SprigCode;
use sprig_common::ids::{ClusterId, DbId, DcId, ObjectId, TenantId};

use crate::*;

static FAILED_LOGINS: Setting<u32> = Setting {
    key: "auth.failed_login_attempts",
    default: 0,
    scopes: Scopes::TREE,
    hard: HardBounds::range(0, 1000),
    apply: Apply::Live,
    operator_only: false,
    owner: 14,
    doc: "Failures before a temporary lock (0: off)",
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
static LIFETIME_DAYS: Setting<u32> = Setting {
    key: "auth.default_password_lifetime_days",
    default: 0,
    scopes: Scopes::TREE,
    hard: HardBounds::range(0, 36_500),
    apply: Apply::Live,
    operator_only: false,
    owner: 14,
    doc: "Password lifetime when an account sets none (0: never expires)",
};
static MFA_REQUIRED: Setting<Cow<'static, str>> = Setting {
    key: "auth.mfa_required",
    default: Cow::Borrowed("off"),
    scopes: Scopes::TREE,
    hard: HardBounds::one_of(&["off", "admins", "all"]),
    apply: Apply::NewSessions,
    operator_only: false,
    owner: 14,
    doc: "off, admins or all",
};
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
static POOL_SIZE: Setting<u32> = Setting {
    key: "gateway.test_pool_size",
    default: 8,
    scopes: Scopes::OPERATIONAL,
    hard: HardBounds::range(1, 1024),
    apply: Apply::Live,
    operator_only: true,
    owner: 6,
    doc: "A process-level key, for scope tests",
};

// One key in the process-wide registry, to check `register!`.
crate::register!(MAX_PER_TABLE);

fn registry() -> Registry {
    Registry::new([
        &FAILED_LOGINS as &dyn AnySetting,
        &LOCK_MINUTES,
        &LIFETIME_DAYS,
        &MFA_REQUIRED,
        &MAX_PER_TABLE,
        &POOL_SIZE,
    ])
    .unwrap()
}

fn code(r: sprig_common::Result<()>) -> Option<SprigCode> {
    r.err().and_then(|e| e.code())
}

#[test]
fn chapter_08_example() {
    // Global bounds max 8; tenant acme sets 6 with bounds max 5; table
    // orders sets 7. The table resolves to 5, clamped, source object.
    let reg = registry();
    let mut s = MemSettings::new(&reg);
    let dc = DcId::new("mum1").unwrap();
    let acme = TenantId(42);
    let base = SettingCtx::process(&dc).for_tenant(acme, &[]);
    let orders = SettingCtx {
        db: Some(DbId(1)),
        object: Some(ObjectId(105)),
        ..base
    };
    let key = MAX_PER_TABLE.key;

    assert_eq!(s.resolve(key, &orders).unwrap().value, Value::Int(5));
    s.set_bounds(Level::Global, &base, key, Bounds::max(8.0))
        .unwrap();
    s.set_value(Level::Tenant, &base, key, &Value::Int(6))
        .unwrap();
    s.set_bounds(Level::Tenant, &base, key, Bounds::max(5.0))
        .unwrap();
    // Writes that would be refused today can be in the store from before a
    // bound tightened; write the object's 7 before the tenant bound exists.
    s.reset_bounds(Level::Tenant, &base, key).unwrap();
    s.set_value(Level::Object, &orders, key, &Value::Int(7))
        .unwrap();
    s.set_bounds(Level::Tenant, &base, key, Bounds::max(5.0))
        .unwrap();

    let r = s.resolve(key, &orders).unwrap();
    assert_eq!(r.value, Value::Int(5));
    assert_eq!(r.source, ScopeKind::Object);
    assert!(r.clamped);
    assert_eq!(r.bounds.max, Some(5.0));
    assert_eq!(r.bounds.min, Some(1.0));

    // The tenant itself sees its own 6: its own bounds limit only narrower
    // scopes.
    let t = s.resolve(key, &base).unwrap();
    assert_eq!(
        (t.value, t.source, t.clamped),
        (Value::Int(6), ScopeKind::Tenant, false)
    );

    // A new write at the object scope is checked against max 5.
    assert_eq!(
        code(s.set_value(Level::Object, &orders, key, &Value::Int(7))),
        Some(SprigCode::SettingOutOfBounds)
    );
}

/// Chapter 14's worked example: `a` (the platform) → `b` (a client) → `c`
/// (its sub-client).
#[test]
#[allow(clippy::many_single_char_names)] // the names of the book's example
fn ancestor_chain_a_b_c() {
    let reg = registry();
    let mut s = MemSettings::new(&reg);
    let dc = DcId::new("mum1").unwrap();
    let (a, b, c) = (TenantId(1), TenantId(2), TenantId(3));
    let ab = [a, b];
    let ctx_a = SettingCtx::process(&dc).for_tenant(a, &[]);
    let ctx_b = SettingCtx::process(&dc).for_tenant(b, &ab[..1]);
    let ctx_c = SettingCtx::process(&dc).for_tenant(c, &ab);
    let t = Level::Tenant;

    s.set_value(t, &ctx_a, FAILED_LOGINS.key, &Value::Int(5))
        .unwrap();
    s.set_bounds(t, &ctx_a, FAILED_LOGINS.key, Bounds::max(10.0))
        .unwrap();
    s.set_value(t, &ctx_c, FAILED_LOGINS.key, &Value::Int(3))
        .unwrap();
    s.set_value(t, &ctx_a, LOCK_MINUTES.key, &Value::Int(15))
        .unwrap();
    s.set_value(t, &ctx_b, LOCK_MINUTES.key, &Value::Int(30))
        .unwrap();
    s.set_value(t, &ctx_b, LIFETIME_DAYS.key, &Value::Int(90))
        .unwrap();
    s.set_value(t, &ctx_a, MFA_REQUIRED.key, &"admins".into())
        .unwrap();
    let mfa = Bounds::allowed(["admins".into(), "all".into()]);
    s.set_bounds(t, &ctx_a, MFA_REQUIRED.key, mfa).unwrap();

    let at_c = |s: &MemSettings<'_>, key| {
        let r = s.resolve(key, &ctx_c).unwrap();
        (r.value, r.source)
    };
    assert_eq!(
        at_c(&s, FAILED_LOGINS.key),
        (Value::Int(3), ScopeKind::Tenant)
    );
    assert_eq!(
        at_c(&s, LOCK_MINUTES.key),
        (Value::Int(30), ScopeKind::Ancestor(b))
    );
    assert_eq!(
        at_c(&s, LIFETIME_DAYS.key),
        (Value::Int(90), ScopeKind::Ancestor(b))
    );
    assert_eq!(
        at_c(&s, MFA_REQUIRED.key),
        ("admins".into(), ScopeKind::Ancestor(a))
    );
    assert_eq!(s.get(&MFA_REQUIRED, &ctx_c), "admins");
    assert_eq!(s.get(&LOCK_MINUTES, &ctx_c), 30);

    // 1. b sets the lifetime to 60; c never set it, so c gets b's change.
    s.set_value(t, &ctx_b, LIFETIME_DAYS.key, &Value::Int(60))
        .unwrap();
    assert_eq!(
        at_c(&s, LIFETIME_DAYS.key),
        (Value::Int(60), ScopeKind::Ancestor(b))
    );

    // 2. b sets failed_login_attempts to 8; c keeps 3, its own is nearer.
    s.set_value(t, &ctx_b, FAILED_LOGINS.key, &Value::Int(8))
        .unwrap();
    assert_eq!(
        at_c(&s, FAILED_LOGINS.key),
        (Value::Int(3), ScopeKind::Tenant)
    );

    // 3. b tries 12: refused with 39080, outside a's max 10.
    assert_eq!(
        code(s.set_value(t, &ctx_b, FAILED_LOGINS.key, &Value::Int(12))),
        Some(SprigCode::SettingOutOfBounds)
    );

    // 4. c tries mfa_required = off: refused with 39080, outside a's list.
    assert_eq!(
        code(s.set_value(t, &ctx_c, MFA_REQUIRED.key, &"off".into())),
        Some(SprigCode::SettingOutOfBounds)
    );

    // 5. a tightens the bound to max 2: c's 3 is clamped to 2 at read time
    //    and flagged; a's own 5 is untouched. Loosening restores 3.
    s.set_bounds(t, &ctx_a, FAILED_LOGINS.key, Bounds::max(2.0))
        .unwrap();
    let r = s.resolve(FAILED_LOGINS.key, &ctx_c).unwrap();
    assert_eq!(
        (r.value, r.source, r.clamped),
        (Value::Int(2), ScopeKind::Tenant, true)
    );
    assert_eq!(
        s.resolve(FAILED_LOGINS.key, &ctx_a).unwrap().value,
        Value::Int(5)
    );
    s.set_bounds(t, &ctx_a, FAILED_LOGINS.key, Bounds::max(10.0))
        .unwrap();
    let r = s.resolve(FAILED_LOGINS.key, &ctx_c).unwrap();
    assert_eq!((r.value, r.clamped), (Value::Int(3), false));
}

#[test]
fn lock_blocks_writes_and_ignores_older_values() {
    let reg = registry();
    let mut s = MemSettings::new(&reg);
    let dc = DcId::new("mum1").unwrap();
    let ctx = SettingCtx::process(&dc).for_tenant(TenantId(7), &[]);
    let key = MAX_PER_TABLE.key;

    s.set_value(Level::Tenant, &ctx, key, &Value::Int(9))
        .unwrap();
    s.set_value(Level::Dc, &ctx, key, &Value::Int(4)).unwrap();
    s.set_bounds(Level::Dc, &ctx, key, Bounds::locked())
        .unwrap();

    let r = s.resolve(key, &ctx).unwrap();
    assert_eq!((r.value, r.source), (Value::Int(4), ScopeKind::Dc));
    assert_eq!(r.ignored, vec![(ScopeKind::Tenant, Ignored::Locked)]);
    assert_eq!(
        code(s.set_value(Level::Tenant, &ctx, key, &Value::Int(3))),
        Some(SprigCode::SettingLocked)
    );
    assert_eq!(
        code(s.set_bounds(Level::Tenant, &ctx, key, Bounds::max(3.0))),
        Some(SprigCode::SettingLocked)
    );
}

#[test]
fn values_outside_a_later_allowed_list_are_ignored() {
    let reg = registry();
    let mut s = MemSettings::new(&reg);
    let dc = DcId::new("mum1").unwrap();
    let ctx = SettingCtx::process(&dc).for_tenant(TenantId(7), &[]);
    let key = MFA_REQUIRED.key;

    s.set_value(Level::Tenant, &ctx, key, &"off".into())
        .unwrap();
    s.set_bounds(Level::Global, &ctx, key, Bounds::allowed(["all".into()]))
        .unwrap();
    let r = s.resolve(key, &ctx).unwrap();
    assert_eq!((r.value, r.source), ("off".into(), ScopeKind::Default));
    assert_eq!(r.ignored, vec![(ScopeKind::Tenant, Ignored::NotAllowed)]);
}

#[test]
fn write_checks_report_the_right_codes() {
    let reg = registry();
    let mut s = MemSettings::new(&reg);
    let dc = DcId::new("mum1").unwrap();
    let cluster = ClusterId::new("mum1-c7").unwrap();
    let process = SettingCtx::process(&dc);
    let tenant = process.for_tenant(TenantId(7), &[]);
    let db = SettingCtx {
        db: Some(DbId(1)),
        ..tenant
    };

    let unknown = s.set_value(Level::Global, &process, "nope.key", &Value::Int(1));
    assert_eq!(code(unknown), Some(SprigCode::SettingUnknown));
    assert_eq!(
        s.resolve("sprig.catalog_version", &process)
            .unwrap_err()
            .code(),
        Some(SprigCode::SettingUnknown)
    );
    // auth.* keys don't accept the database scope.
    let wrong_scope = s.set_value(Level::Database, &db, FAILED_LOGINS.key, &Value::Int(1));
    assert_eq!(code(wrong_scope), Some(SprigCode::SettingScopeNotAllowed));
    // A cluster write needs a cluster in the context.
    let no_cluster = s.set_value(Level::Cluster, &process, POOL_SIZE.key, &Value::Int(4));
    assert_eq!(code(no_cluster), Some(SprigCode::SettingScopeNotAllowed));
    let on_cluster = SettingCtx {
        cluster: Some(&cluster),
        ..process
    };
    s.set_value(Level::Cluster, &on_cluster, POOL_SIZE.key, &Value::Int(4))
        .unwrap();
    assert_eq!(s.get(&POOL_SIZE, &on_cluster), 4);
    assert_eq!(s.get(&POOL_SIZE, &process), 8);

    let bad_type = s.set_value(Level::Global, &process, POOL_SIZE.key, &"big".into());
    assert_eq!(code(bad_type), Some(SprigCode::SettingBadValue));
    let negative = s.set_value(Level::Global, &process, POOL_SIZE.key, &Value::Int(-1));
    assert_eq!(code(negative), Some(SprigCode::SettingBadValue));
    let hard = s.set_value(Level::Global, &process, POOL_SIZE.key, &Value::Int(0));
    assert_eq!(code(hard), Some(SprigCode::SettingOutOfBounds));
    let not_in_set = s.set_value(
        Level::Global,
        &process,
        MFA_REQUIRED.key,
        &"sometimes".into(),
    );
    assert_eq!(code(not_in_set), Some(SprigCode::SettingOutOfBounds));
}

#[test]
fn reset_removes_empty_rows() {
    let reg = registry();
    let mut s = MemSettings::new(&reg);
    let dc = DcId::new("mum1").unwrap();
    let ctx = SettingCtx::process(&dc);
    s.set_value(Level::Dc, &ctx, POOL_SIZE.key, &Value::Int(16))
        .unwrap();
    assert_eq!(s.get(&POOL_SIZE, &ctx), 16);
    s.reset_value(Level::Dc, &ctx, POOL_SIZE.key).unwrap();
    assert_eq!(
        s.resolve(POOL_SIZE.key, &ctx).unwrap().source,
        ScopeKind::Default
    );
}

#[test]
fn bounds_intersect() {
    let a = Bounds {
        min: Some(1.0),
        max: Some(10.0),
        allowed: Some(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
        locked: false,
    };
    let b = Bounds {
        min: Some(2.0),
        max: None,
        allowed: Some(vec![Value::Int(2), Value::Int(3), Value::Int(4)]),
        locked: true,
    };
    let i = a.intersect(&b);
    assert_eq!(i.min, Some(2.0));
    assert_eq!(i.max, Some(10.0));
    assert_eq!(i.allowed, Some(vec![Value::Int(2), Value::Int(3)]));
    assert!(i.locked);
    assert_eq!(Bounds::max(2.5).clamp(&Value::Int(3)), Value::Int(2));
    assert_eq!(
        Bounds::max(2.5).clamp(&Value::Float(3.0)),
        Value::Float(2.5)
    );
}

#[test]
fn registry_rejects_bad_declarations() {
    static NO_DOC: Setting<u32> = Setting {
        doc: " ",
        key: "x.no_doc",
        ..const_base()
    };
    static BAD_KEY: Setting<u32> = Setting {
        key: "NoDots",
        ..const_base()
    };
    static INTERNAL: Setting<u32> = Setting {
        key: "sprig.next_db_id",
        ..const_base()
    };
    static OUT_OF_HARD: Setting<u32> = Setting {
        key: "x.out_of_hard",
        default: 0,
        ..const_base()
    };
    for bad in [&NO_DOC, &BAD_KEY, &INTERNAL, &OUT_OF_HARD] {
        assert!(
            Registry::new([bad as &dyn AnySetting]).is_err(),
            "{}",
            bad.key
        );
    }
    assert!(Registry::new([&POOL_SIZE as &dyn AnySetting, &POOL_SIZE]).is_err());
}

const fn const_base() -> Setting<u32> {
    Setting {
        key: "x.base",
        default: 1,
        scopes: Scopes::ALL,
        hard: HardBounds::range(1, 2),
        apply: Apply::Live,
        operator_only: false,
        owner: 3,
        doc: "base",
    }
}

#[test]
fn global_registry_collects_registered_keys() {
    let reg = Registry::global().unwrap();
    let def = reg.get(MAX_PER_TABLE.key).unwrap();
    assert_eq!(def.default_value(), Value::Int(5));
    assert_eq!(def.owner(), 13);
    assert_eq!(
        serde_json::to_string(&Bounds::max(5.0)).unwrap(),
        r#"{"max":5.0,"locked":false}"#
    );
}

static NOT_YET: Setting<u32> = Setting {
    key: "gateway.test_not_yet_built",
    default: 1,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(0, 10),
    apply: Apply::Live,
    operator_only: true,
    owner: 3,
    doc: "A key registered ahead of its code, for the marks test",
};

/// bugfix-89's round (smallfix): a key registered ahead of its code stays
/// registered (a stored value and `SET` are still checked) but listings
/// leave it out; a key read only at start is shown as `restart`.
#[test]
fn marked_keys_are_hidden_or_restart_only() {
    let reg = Registry::new([
        &NOT_YET as &dyn AnySetting,
        &POOL_SIZE,
        &MAX_PER_TABLE,
        &MFA_REQUIRED,
    ])
    .unwrap()
    .with_marks(&[(NOT_YET.key, Mark::Unbuilt), (POOL_SIZE.key, Mark::Restart)]);
    assert!(reg.get(NOT_YET.key).is_some());
    assert!(reg.is_unbuilt(NOT_YET.key));
    let listed: Vec<&str> = reg.iter().map(|d| d.key()).collect();
    assert_eq!(listed, [MFA_REQUIRED.key, MAX_PER_TABLE.key, POOL_SIZE.key]);
    assert_eq!(reg.iter_all().count(), 4);
    assert_eq!(reg.apply_name(&POOL_SIZE), "restart");
    assert_eq!(reg.apply_name(&MAX_PER_TABLE), "live");
    assert_eq!(reg.apply_name(&MFA_REQUIRED), "new_sessions");
    // The production table names registered keys only, each once.
    let mut seen = std::collections::BTreeSet::new();
    for (k, _, why) in crate::registry::MARKED {
        assert!(seen.insert(*k), "{k} marked twice");
        assert!(!why.is_empty(), "{k}");
    }
    assert_eq!(
        crate::registry::mark_of("redis.sharded_pubsub_hubs"),
        Some(Mark::Restart)
    );
    // A value can still be stored for it.
    let mut s = MemSettings::new(&reg);
    let dc = DcId::new("mum1").unwrap();
    let ctx = SettingCtx::process(&dc);
    s.set_value(Level::Global, &ctx, NOT_YET.key, &Value::Int(3))
        .unwrap();
    assert_eq!(s.resolve(NOT_YET.key, &ctx).unwrap().value, Value::Int(3));
}

#[test]
fn default_is_clamped_by_tightened_bounds() {
    let reg = registry();
    let mut s = MemSettings::new(&reg);
    let dc = DcId::new("mum1").unwrap();
    let ctx = SettingCtx::process(&dc).for_tenant(TenantId(7), &[]);
    let key = MAX_PER_TABLE.key;

    // No scope sets a value, but the operator caps the setting below its default (5).
    s.set_bounds(Level::Global, &ctx, key, Bounds::max(3.0))
        .unwrap();
    let r = s.resolve(key, &ctx).unwrap();
    assert_eq!((r.value, r.source), (Value::Int(3), ScopeKind::Default));
    assert!(r.clamped);
}
