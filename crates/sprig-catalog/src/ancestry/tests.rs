//! The catalog side of the ancestry copy (chapter 08 step 6), over the
//! in-memory catalog: what [`apply`] writes, clears and leaves alone, and
//! that caches load the result incrementally exactly as in full.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::borrow::Cow;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::json;
use sprig_common::ids::{DcId, TenantId};
use sprig_meta::{MemStore, MetaStore, Op, Tier, Txn};
use sprig_settings::{
    AnySetting, Apply, Bounds, HardBounds, MemSettings, Registry, ScopeKind, Scopes, Setting, Value,
};

use super::*;
use crate::cache::mem::MemSource;
use crate::cache::{Loaded, TenantCatalog};
use crate::store::{SettingRow, SettingScope};
use crate::version::{DirectoryAllocator, VersionKind, Versioner};

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

fn registry() -> &'static Registry {
    static R: std::sync::OnceLock<Registry> = std::sync::OnceLock::new();
    R.get_or_init(|| Registry::new([&LOCK_MINUTES as &dyn AnySetting, &MODE]).unwrap())
}

const D: TenantId = TenantId(30);
const A: TenantId = TenantId(10);
const B: TenantId = TenantId(20);

fn r(id: TenantId, name: &str) -> TenantRef {
    TenantRef {
        id,
        name: name.to_owned(),
    }
}

fn at() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_762_560_000)
}

fn tenant_row(key: &str, value: Option<serde_json::Value>, bounds: Option<Bounds>) -> SettingRow {
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

async fn env() -> Versioner<MemSource> {
    let global = Arc::new(MemStore::new(Tier::Global));
    let key = global.tier().key(&format!("tenants/{D}")).unwrap();
    global
        .txn(Txn {
            then: vec![Op::Put {
                key,
                value: serde_json::to_vec(&json!({"v": 1, "tenant_id": D.0})).unwrap(),
                lease: None,
            }],
            ..Txn::default()
        })
        .await
        .unwrap();
    let v = Versioner::new(MemSource::new(), Arc::new(DirectoryAllocator::new(global)));
    v.backend().bootstrap(D);
    v
}

async fn run(
    v: &Versioner<MemSource>,
    want: &[TenantRef],
    fresh: &[AncestorRows],
) -> (u64, AncestryReport) {
    let mut c = v.change(D, VersionKind::Catalog).await.unwrap();
    let ver = c.version();
    let rep = apply(c.tx(), D, ver, at(), want, fresh).await.unwrap();
    c.commit().await.unwrap();
    (ver, rep)
}

fn row_of(v: &Versioner<MemSource>, scope: SettingScope, key: &str) -> Option<SettingRow> {
    v.backend()
        .tables(D)
        .unwrap()
        .settings
        .get(&(scope, key.to_owned()))
        .cloned()
}

#[tokio::test]
async fn a_root_gets_an_empty_ancestry_once() {
    let v = env().await;
    let (ver, rep) = run(&v, &[], &[]).await;
    assert!(rep.keys_written && rep.changed());
    let a = row_of(&v, SettingScope::Tenant, ANCESTRY_KEY).unwrap();
    assert_eq!(a.value, Some(json!([])));
    assert_eq!(a.version, ver);
    let vs = row_of(&v, SettingScope::Tenant, ANCESTOR_VERSIONS_KEY).unwrap();
    assert_eq!(vs.value, Some(json!({})));
    // Nothing to do the second time.
    let (_, rep) = run(&v, &[], &[]).await;
    assert!(!rep.changed(), "{rep:?}");
}

#[tokio::test]
async fn copies_replace_changed_rows_and_clear_removed_ones() {
    let v = env().await;
    let want = [r(A, "a"), r(B, "b")];
    let (v1, rep) = run(
        &v,
        &want,
        &[
            AncestorRows {
                tenant: A,
                settings_version: 3,
                rows: vec![
                    tenant_row("auth.password_lock_minutes", Some(json!(30)), None),
                    tenant_row("auto_index.mode", None, Some(Bounds::allowed([Value::from("off"), Value::from("on")]))),
                    // Internal and empty rows are never copied.
                    tenant_row("sprig.catalog_version", Some(json!("9")), None),
                    tenant_row("auto_index.max_per_table", None, None),
                ],
            },
            AncestorRows {
                tenant: B,
                settings_version: 4,
                rows: vec![tenant_row("auth.password_lock_minutes", Some(json!(10)), None)],
            },
        ],
    )
    .await;
    assert_eq!((rep.rows_written, rep.rows_cleared), (3, 0));
    assert_eq!(rep.copied, vec![A, B]);
    let a_lock = row_of(&v, SettingScope::Ancestor(A), "auth.password_lock_minutes").unwrap();
    assert_eq!((a_lock.value, a_lock.version), (Some(json!(30)), v1));
    assert!(row_of(&v, SettingScope::Ancestor(A), "sprig.catalog_version").is_none());
    assert_eq!(
        row_of(&v, SettingScope::Tenant, ANCESTRY_KEY).unwrap().value,
        Some(json!([{"id": 10, "name": "a"}, {"id": 20, "name": "b"}]))
    );
    assert_eq!(
        row_of(&v, SettingScope::Tenant, ANCESTOR_VERSIONS_KEY)
            .unwrap()
            .value,
        Some(json!({"10": 3, "20": 4}))
    );

    // a changes: the lock row is the same, the mode bound goes, a new key
    // arrives. Only what moved is written.
    let (v2, rep) = run(
        &v,
        &want,
        &[AncestorRows {
            tenant: A,
            settings_version: 7,
            rows: vec![
                tenant_row("auth.password_lock_minutes", Some(json!(30)), None),
                tenant_row("auto_index.mode", Some(json!("on")), None),
            ],
        }],
    )
    .await;
    assert_eq!((rep.rows_written, rep.rows_cleared), (1, 0));
    let a_lock = row_of(&v, SettingScope::Ancestor(A), "auth.password_lock_minutes").unwrap();
    assert_eq!(a_lock.version, v1, "an unchanged copy is left alone");
    let mode = row_of(&v, SettingScope::Ancestor(A), "auto_index.mode").unwrap();
    assert_eq!((mode.value, mode.bounds, mode.version), (Some(json!("on")), None, v2));
    assert_eq!(
        row_of(&v, SettingScope::Tenant, ANCESTOR_VERSIONS_KEY)
            .unwrap()
            .value,
        Some(json!({"10": 7, "20": 4}))
    );

    // a drops the lock row.
    let (v3, rep) = run(
        &v,
        &want,
        &[AncestorRows {
            tenant: A,
            settings_version: 8,
            rows: vec![tenant_row("auto_index.mode", Some(json!("on")), None)],
        }],
    )
    .await;
    assert_eq!((rep.rows_written, rep.rows_cleared), (0, 1));
    let gone = row_of(&v, SettingScope::Ancestor(A), "auth.password_lock_minutes").unwrap();
    assert!(gone.is_empty(), "cleared rows stay as tombstones");
    assert_eq!(gone.version, v3);

    // d moves out from under a: a's rows are cleared, b's stay.
    let (_, rep) = run(&v, &[r(B, "b")], &[]).await;
    assert_eq!(rep.removed, vec![A]);
    assert_eq!(rep.rows_cleared, 1);
    assert!(
        row_of(&v, SettingScope::Ancestor(A), "auto_index.mode")
            .unwrap()
            .is_empty()
    );
    assert!(
        !row_of(&v, SettingScope::Ancestor(B), "auth.password_lock_minutes")
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        row_of(&v, SettingScope::Tenant, ANCESTOR_VERSIONS_KEY)
            .unwrap()
            .value,
        Some(json!({"20": 4}))
    );
}

#[tokio::test]
async fn rows_of_tenants_outside_the_ancestry_are_refused() {
    let v = env().await;
    let mut c = v.change(D, VersionKind::Catalog).await.unwrap();
    let ver = c.version();
    let fresh = [AncestorRows {
        tenant: B,
        settings_version: 1,
        rows: Vec::new(),
    }];
    let e = apply(c.tx(), D, ver, at(), &[r(A, "a")], &fresh)
        .await
        .unwrap_err();
    assert!(e.message.contains("isn't an ancestor"), "{e}");
    let e = apply(c.tx(), D, ver, at(), &[r(D, "self")], &[])
        .await
        .unwrap_err();
    assert!(e.message.contains("its own ancestors"), "{e}");
}

#[test]
fn staleness_follows_the_directory_versions() {
    let s = AncestryState::from_values(
        D,
        Some(&json!([{"id": 10, "name": "a"}, 20])),
        Some(&json!({"10": 5, "20": 2, "junk": "x"})),
    );
    assert_eq!(s.ids(), vec![A, B]);
    let want = |sa, sb| vec![(r(A, "a"), sa), (r(B, ""), sb)];
    assert!(s.is_current(&want(5, 2)));
    assert!(s.is_current(&want(4, 1)), "older directory versions are covered");
    assert_eq!(s.stale(&want(6, 2)), vec![A]);
    assert!(!s.is_current(&want(6, 2)));
    // A renamed ancestor isn't the same ancestry.
    assert!(!s.is_current(&[(r(A, "a2"), 5), (r(B, ""), 2)]));
    // A new ancestor has no copy yet.
    let three = [(r(A, "a"), 5), (r(B, ""), 2), (r(TenantId(25), "x"), 0)];
    assert_eq!(s.stale(&three), vec![TenantId(25)]);
    let none = AncestryState::from_values(D, None, None);
    assert!(!none.is_current(&[]), "a catalog the loop never reached");
    assert!(none.stale(&[]).is_empty());
}

#[tokio::test]
async fn caches_load_the_copy_incrementally_as_in_full() {
    let v = env().await;
    let dc = DcId::new("mum1").unwrap();
    let etcd = MemSettings::new(registry());
    let load = |since: Option<u64>| Loaded::core(v.backend().tables(D).unwrap().snapshot(since));
    let mut cached = TenantCatalog::build(registry(), D, load(None));
    let steps: Vec<(Vec<TenantRef>, Vec<AncestorRows>)> = vec![
        (
            vec![r(A, "a"), r(B, "b")],
            vec![
                AncestorRows {
                    tenant: A,
                    settings_version: 1,
                    rows: vec![tenant_row("auth.password_lock_minutes", Some(json!(30)), None)],
                },
                AncestorRows {
                    tenant: B,
                    settings_version: 1,
                    rows: vec![tenant_row("auth.password_lock_minutes", Some(json!(10)), None)],
                },
            ],
        ),
        (
            vec![r(A, "a"), r(B, "b")],
            vec![AncestorRows {
                tenant: B,
                settings_version: 2,
                rows: Vec::new(),
            }],
        ),
        (vec![r(B, "b")], Vec::new()),
    ];
    let expect = [(10, ScopeKind::Ancestor(B)), (30, ScopeKind::Ancestor(A)), (0, ScopeKind::Default)];
    for ((want, fresh), (value, source)) in steps.iter().zip(expect) {
        run(&v, want, fresh).await;
        let next = cached.apply(load(Some(cached.version)));
        let full = TenantCatalog::build(registry(), D, load(None));
        assert_eq!(next, full);
        let res = next
            .settings
            .resolve(&etcd, "auth.password_lock_minutes", &next.setting_ctx(&dc))
            .unwrap();
        assert_eq!((res.value, res.source), (Value::Int(value), source));
        assert_eq!(
            next.ancestry.iter().map(|r| r.id).collect::<Vec<_>>(),
            want.iter().map(|r| r.id).collect::<Vec<_>>()
        );
        cached = next;
    }
}
