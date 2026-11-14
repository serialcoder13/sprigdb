//! The settings service over in-memory stores: one `MemStore` etcd in
//! single-etcd mode (the global tier is a view of the DC tier), the
//! in-memory catalog, and a directory with a parent and a child tenant.
//! Every refusal (39080-39084, 1049, 1146, 39072, 39504, 39792), writes at
//! all six scopes, bounds patches, locks, provenance through the chain
//! (ancestor copies included) and a tenant's rows. The live case over
//! etcd and MariaDB is `live_tests`.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

use std::borrow::Cow;
use std::sync::{Arc, OnceLock};
use std::time::SystemTime;

use sprig_catalog::cache::mem::MemSource;
use sprig_catalog::store::{
    DatabaseRow, DbState, Namespace, ObjectFlags, ObjectKind, ObjectRow, ObjectState,
};
use sprig_catalog::{DirectoryAllocator, VersionKind, Versioner};
use sprig_common::ids::{DbId, GroupId, ObjectId};
use sprig_common::time::SystemClock;
use sprig_meta::{MemStore, MetaStore, Tier};
use sprig_settings::{AnySetting, Apply, HardBounds, Scopes, Setting, SettingsStores};

use super::*;
use crate::tree::NewTenant;
use crate::tree_settings::TreeSync;

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
static CACHE_MB: Setting<u32> = Setting {
    key: "catalog.cache_max_mb",
    default: 512,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(16, 65_536),
    apply: Apply::Live,
    operator_only: true,
    owner: 8,
    doc: "Cache memory per gateway",
};

fn registry() -> &'static Registry {
    static R: OnceLock<Registry> = OnceLock::new();
    R.get_or_init(|| {
        Registry::new([
            &MAX_PER_TABLE as &dyn AnySetting,
            &MODE,
            &WILDCARD,
            &CACHE_MB,
            &sprig_auth::authz::settings::AUTHORIZER,
        ])
        .unwrap()
    })
}

const SHOP: DbId = DbId(1);
const ORDERS: ObjectId = ObjectId(11);

fn dc() -> DcId {
    DcId::new("mum1").unwrap()
}

fn db_row(v: u64) -> DatabaseRow {
    DatabaseRow {
        db_id: SHOP,
        name: "shop".into(),
        name_key: "shop".into(),
        default_charset: "utf8mb4".into(),
        default_collation: "utf8mb4_bin".into(),
        default_group_id: GroupId(0),
        state: DbState::Active,
        options: serde_json::json!({}),
        created_at: SystemTime::now(),
        created_by: None,
        changed_version: v,
    }
}

fn table_row(v: u64) -> ObjectRow {
    ObjectRow {
        object_id: ORDERS,
        db_id: SHOP,
        namespace: Namespace::Relation,
        name: "orders".into(),
        name_key: "orders".into(),
        kind: ObjectKind::Table,
        parent_id: None,
        state: ObjectState::Active,
        schema_version: 1,
        flags: ObjectFlags::empty(),
        pending_job_id: None,
        created_at: SystemTime::now(),
        changed_version: v,
    }
}

struct Env {
    svc: SettingsService<MemSource>,
    live: LiveSettings,
    tree: TreeSync<MemSource>,
    guard: Vec<Cmp>,
    acme: TenantId,
    pune: TenantId,
}

/// A DC leader key with a known create revision, standing in for the
/// controller's leadership.
async fn leader_guard(store: &dyn MetaStore) -> Vec<Cmp> {
    let key = store.tier().key("leader").unwrap();
    let r = store
        .txn(sprig_meta::Txn {
            then: vec![sprig_meta::Op::Put {
                key: key.clone(),
                value: b"{}".to_vec(),
                lease: None,
            }],
            ..sprig_meta::Txn::default()
        })
        .await
        .unwrap();
    vec![Cmp::CreateRev(key, r.revision)]
}

impl Env {
    async fn new() -> Self {
        let dc_store = MemStore::new(Tier::Dc(dc()));
        let global = dc_store.with_tier(Tier::Global);
        let (dcs, gs): (Arc<dyn MetaStore>, Arc<dyn MetaStore>) =
            (Arc::new(dc_store), Arc::new(global));
        let live = LiveSettings::start(
            registry(),
            SettingsStores::new(Arc::clone(&gs), Arc::clone(&dcs)).unwrap(),
        )
        .unwrap();
        live.ready().await;
        let directory = Directory::new(Arc::clone(&gs));
        let mut ids = Vec::new();
        for (name, parent) in [("acme", None), ("pune", Some(0usize))] {
            let e = directory
                .create(
                    &NewTenant {
                        name: name.into(),
                        parent: parent.map(|i: usize| ids[i]),
                        home_dc: dc(),
                        placement_hint: None,
                    },
                    64,
                    SystemTime::now(),
                )
                .await
                .unwrap();
            let id = e.body().tenant_id;
            directory
                .update(id, |b| {
                    b.state = TenantState::Active;
                    Ok(true)
                })
                .await
                .unwrap();
            ids.push(id);
        }
        let v = Versioner::new(
            MemSource::new(),
            Arc::new(DirectoryAllocator::new(Arc::clone(&gs))),
        );
        for &t in &ids {
            v.backend().bootstrap(t);
            let mut c = v.change(t, VersionKind::Catalog).await.unwrap();
            let ver = c.version();
            c.tx().put_database(db_row(ver));
            c.tx().put_object(table_row(ver));
            c.commit().await.unwrap();
        }
        let guard = leader_guard(&*dcs).await;
        let tree = TreeSync::new(v.clone(), directory.clone(), Arc::new(SystemClock));
        Self {
            svc: SettingsService::new(live.clone(), v, directory, Arc::new(SystemClock)),
            live,
            tree,
            guard,
            acme: ids[0],
            pune: ids[1],
        }
    }

    async fn write(&self, key: &str, scope: &str, op: SettingOp) -> Result<SettingWriteResponse> {
        self.svc
            .write(
                key,
                &SettingWrite {
                    scope: scope.parse().unwrap(),
                    op,
                    actor: "kiran".into(),
                    reason: "test".into(),
                },
                self.guard.clone(),
            )
            .await
    }

    async fn set(&self, key: &str, scope: &str, text: &str) -> Result<SettingWriteResponse> {
        self.write(
            key,
            scope,
            SettingOp::Set {
                value: None,
                value_text: Some(text.into()),
            },
        )
        .await
    }

    async fn bounds(
        &self,
        key: &str,
        scope: &str,
        min: Option<f64>,
        max: Option<f64>,
    ) -> Result<SettingWriteResponse> {
        self.write(
            key,
            scope,
            SettingOp::Bounds {
                min,
                max,
                allowed: None,
                allowed_text: None,
                clear: Vec::new(),
            },
        )
        .await
    }

    async fn get(&self, key: &str, q: SettingContextQuery) -> SettingExplanation {
        self.svc.explain(key, &q).await.unwrap()
    }

    /// Waits until the mirror shows `key` at `scope` as `want`.
    async fn mirrored(&self, key: &str, scope: Scope, want: Option<Value>) {
        for _ in 0..200 {
            let got = self
                .live
                .snapshot()
                .entry(&scope, key)
                .and_then(|e| e.value.clone());
            if got == want {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!("{key} at {scope:?} never became {want:?}");
    }
}

fn code(e: &SprigError) -> u16 {
    e.protocol().mysql_errno
}

fn ctx(tenant: &str, db: Option<&str>, object: Option<&str>) -> SettingContextQuery {
    SettingContextQuery {
        tenant: Some(tenant.into()),
        db: db.map(Into::into),
        object: object.map(Into::into),
        ..SettingContextQuery::default()
    }
}

#[test]
fn scope_syntax_round_trips() {
    for s in [
        "global",
        "dc",
        "dc:mum1",
        "cluster:mum1-c7",
        "tenant:acme",
        "db:acme.shop",
        "object:acme.shop.orders",
        "object:acme.shop.a.b",
    ] {
        let r: ScopeRef = s.parse().unwrap();
        assert_eq!(r.to_string(), s);
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(serde_json::from_str::<ScopeRef>(&json).unwrap(), r);
    }
    assert_eq!(
        "database:acme.shop"
            .parse::<ScopeRef>()
            .unwrap()
            .to_string(),
        "db:acme.shop"
    );
    for bad in [
        "",
        "galaxy",
        "global:x",
        "cluster",
        "tenant:",
        "db:acme",
        "object:acme.shop",
        "db:.x",
    ] {
        assert!(bad.parse::<ScopeRef>().is_err(), "{bad}");
    }
    assert!(ScopeRef::Cluster(ClusterId::new("mum1-c1").unwrap()).is_etcd());
    assert_eq!(ScopeRef::Tenant("a".into()).tenant(), Some("a"));
}

#[test]
fn keys_list_describe_and_parse() {
    let all = list_keys(registry(), &ListSettingKeysQuery::default()).unwrap();
    assert_eq!(all.keys.len(), 5);
    let q = |prefix: Option<&str>, op: Option<bool>, scope: Option<&str>| ListSettingKeysQuery {
        prefix: prefix.map(Into::into),
        operator_only: op,
        scope: scope.map(Into::into),
    };
    let ai = list_keys(registry(), &q(Some("auto_index."), None, None)).unwrap();
    assert_eq!(
        ai.keys.iter().map(|k| k.key.as_str()).collect::<Vec<_>>(),
        ["auto_index.max_per_table", "auto_index.mode"]
    );
    let ops = list_keys(registry(), &q(None, Some(true), None)).unwrap();
    assert_eq!(ops.keys.len(), 1);
    assert_eq!(ops.keys[0].key, "catalog.cache_max_mb");
    let cl = list_keys(registry(), &q(None, None, Some("cluster"))).unwrap();
    assert_eq!(
        cl.keys.len(),
        2,
        "WILDCARD, CACHE_MB and auth.authorizer don't accept cluster"
    );
    assert_eq!(
        code(&list_keys(registry(), &q(None, None, Some("planet"))).unwrap_err()),
        39792
    );
    let v = key_view(&MODE);
    assert_eq!(v.kind, "string");
    assert_eq!(
        v.scopes,
        ["global", "dc", "cluster", "tenant", "database", "object"]
    );
    assert_eq!(v.hard_bounds.allowed.as_ref().unwrap().len(), 3);
    assert_eq!(v.apply, "live");
    assert!(v.tenant_settable);
    assert!(!key_view(&CACHE_MB).tenant_settable);
    assert_eq!(
        key_view(&WILDCARD).scopes,
        ["global", "tenant", "database", "object"]
    );

    assert_eq!(parse_text(&MAX_PER_TABLE, " 12 ").unwrap(), Value::Int(12));
    assert_eq!(parse_text(&WILDCARD, "ON").unwrap(), Value::Bool(true));
    assert_eq!(parse_text(&MODE, "on").unwrap(), Value::from("on"));
    for (def, bad) in [
        (&MAX_PER_TABLE as &dyn AnySetting, "many"),
        (&MAX_PER_TABLE, "-1"),
        (&MAX_PER_TABLE, "1.5"),
        (&WILDCARD, "maybe"),
    ] {
        assert_eq!(code(&parse_text(def, bad).unwrap_err()), 39084, "{bad}");
    }
}

#[tokio::test]
async fn every_refusal() {
    let env = Env::new().await;
    let k = MAX_PER_TABLE.key;
    // 39081 unknown, also internal keys.
    assert_eq!(
        code(&env.set("no.such_key", "global", "1").await.unwrap_err()),
        39081
    );
    assert_eq!(
        code(
            &env.set("sprig.catalog_version", "tenant:acme", "1")
                .await
                .unwrap_err()
        ),
        39081
    );
    assert_eq!(
        code(
            &env.svc
                .explain("no.such_key", &SettingContextQuery::default())
                .await
                .unwrap_err()
        ),
        39081
    );
    // 39082 scope not accepted: before anything is read.
    assert_eq!(
        code(
            &env.set(WILDCARD.key, "cluster:mum1-c1", "on")
                .await
                .unwrap_err()
        ),
        39082
    );
    assert_eq!(
        code(
            &env.set(CACHE_MB.key, "tenant:nobody", "64")
                .await
                .unwrap_err()
        ),
        39082
    );
    // 39084 wrong type, as text and as typed JSON.
    assert_eq!(
        code(&env.set(k, "global", "lots").await.unwrap_err()),
        39084
    );
    let typed = env
        .write(
            k,
            "tenant:acme",
            SettingOp::Set {
                value: Some(Value::from("8")),
                value_text: None,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(code(&typed), 39084);
    assert_eq!(
        code(&env.set(k, "dc", "100").await.unwrap_err()),
        39080,
        "hard bounds 1-64"
    );
    // 39080 outside a global bound, at dc and in a tenant.
    env.bounds(k, "global", None, Some(8.0)).await.unwrap();
    for _ in 0..200 {
        if env.live.snapshot().entry(&Scope::Global, k).is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert_eq!(code(&env.set(k, "dc", "9").await.unwrap_err()), 39080);
    let e = env.set(k, "tenant:acme", "9").await.unwrap_err();
    assert_eq!(code(&e), 39080, "{e}");
    assert!(e.message.contains("max 8"), "{e}");
    env.set(k, "tenant:acme", "8").await.unwrap();
    // 39083 locked at global: dc and tenant values refused.
    env.write(MODE.key, "global", SettingOp::Lock { locked: true })
        .await
        .unwrap();
    for _ in 0..200 {
        if env
            .live
            .snapshot()
            .entry(&Scope::Global, MODE.key)
            .is_some()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert_eq!(
        code(&env.set(MODE.key, "dc", "on").await.unwrap_err()),
        39083
    );
    assert_eq!(
        code(
            &env.set(MODE.key, "object:acme.shop.orders", "on")
                .await
                .unwrap_err()
        ),
        39083
    );
    // Not allowed by the hard list: 39080.
    assert_eq!(
        code(&env.set(MODE.key, "global", "sometimes").await.unwrap_err()),
        39080
    );
    // Unknown tenant, database, object; another DC; a cluster elsewhere.
    assert_eq!(
        code(&env.set(k, "tenant:nobody", "3").await.unwrap_err()),
        39072
    );
    assert_eq!(
        code(&env.set(k, "db:acme.nope", "3").await.unwrap_err()),
        1049
    );
    assert_eq!(
        code(&env.set(k, "object:acme.shop.nope", "3").await.unwrap_err()),
        1146
    );
    assert_eq!(code(&env.set(k, "dc:fra1", "3").await.unwrap_err()), 39792);
    assert_eq!(
        code(&env.set(k, "cluster:fra1-c1", "3").await.unwrap_err()),
        39792
    );
    let other_dc = SettingContextQuery {
        dc: Some("fra1".into()),
        ..SettingContextQuery::default()
    };
    assert_eq!(
        code(&env.svc.explain(k, &other_dc).await.unwrap_err()),
        39792
    );
    assert_eq!(
        code(
            &env.svc
                .explain(k, &ctx("acme", None, Some("orders")))
                .await
                .unwrap_err()
        ),
        39792
    );
    // Malformed requests.
    let none = env
        .write(
            k,
            "global",
            SettingOp::Set {
                value: None,
                value_text: None,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(code(&none), 39792);
    assert_eq!(
        code(
            &env.bounds(k, "global", Some(9.0), Some(3.0))
                .await
                .unwrap_err()
        ),
        39792
    );
    assert_eq!(
        code(
            &env.bounds(MODE.key, "global", Some(1.0), None)
                .await
                .unwrap_err()
        ),
        39084
    );
    let nothing = env
        .write(
            k,
            "global",
            SettingOp::Bounds {
                min: None,
                max: None,
                allowed: None,
                allowed_text: None,
                clear: Vec::new(),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(code(&nothing), 39792);
    // 39504: an etcd write under a guard that no longer holds.
    let mut stale = env.guard.clone();
    if let Some(Cmp::CreateRev(_, rev)) = stale.first_mut() {
        *rev += 1000;
    }
    let e = env
        .svc
        .write(
            k,
            &SettingWrite {
                scope: ScopeRef::Dc(None),
                op: SettingOp::Set {
                    value: Some(Value::Int(3)),
                    value_text: None,
                },
                actor: String::new(),
                reason: String::new(),
            },
            stale,
        )
        .await
        .unwrap_err();
    assert_eq!(code(&e), 39504);
}

#[tokio::test]
async fn writes_resolve_with_provenance() {
    let env = Env::new().await;
    let k = MAX_PER_TABLE.key;
    // global: value and bounds, kept apart by patches.
    let r = env.set(k, "global", "6").await.unwrap();
    assert!(r.changed);
    assert_eq!(r.set_by, "op:kiran");
    assert_eq!(r.current.as_ref().unwrap().value, Some(Value::Int(6)));
    assert_eq!(r.current.as_ref().unwrap().reason.as_deref(), Some("test"));
    let r = env
        .bounds(k, "global", Some(2.0), Some(10.0))
        .await
        .unwrap();
    let cur = r.current.unwrap();
    assert_eq!(cur.value, Some(Value::Int(6)));
    assert_eq!(cur.bounds.as_ref().unwrap().max, Some(10.0));
    let r = env.bounds(k, "global", None, Some(9.0)).await.unwrap();
    assert_eq!(
        r.current.unwrap().bounds.unwrap().min,
        Some(2.0),
        "min kept"
    );
    env.mirrored(k, Scope::Global, Some(Value::Int(6))).await;

    // No context: dc and global.
    let e = env.get(k, SettingContextQuery::default()).await;
    assert_eq!(e.value, Value::Int(6));
    assert_eq!(e.source, "global");
    assert_eq!(e.context.dc, dc());
    let g = e.chain.iter().find(|s| s.scope == "global").unwrap();
    assert_eq!(g.set_by.as_deref(), Some("op:kiran"));
    assert_eq!(g.reason.as_deref(), Some("test"));
    assert!(g.set_at.as_deref().unwrap().ends_with('Z'));
    assert_eq!(e.chain[0].scope, "default");
    assert_eq!(e.chain[0].outcome, "overridden");

    // A tenant value, a database value, an object value: the nearest wins.
    env.set(k, "tenant:acme", "7").await.unwrap();
    env.set(k, "db:acme.shop", "8").await.unwrap();
    let r = env.set(k, "object:acme.shop.orders", "4").await.unwrap();
    assert!(r.version.is_some());
    assert_eq!(r.set_by, "op:kiran");
    let e = env.get(k, ctx("acme", Some("shop"), Some("orders"))).await;
    assert_eq!(e.value, Value::Int(4));
    assert_eq!(e.source, "object");
    assert_eq!(e.source_target.as_deref(), Some("acme.shop.orders"));
    assert_eq!(e.context.object.as_deref(), Some("orders"));
    assert!(e.catalog_version.is_some());
    let obj = e.chain.last().unwrap();
    assert_eq!(obj.set_by.as_deref(), Some("op:kiran"));
    assert!(obj.version.is_some());
    assert_eq!(obj.bounds_in_force.max, Some(9.0));
    let scopes: Vec<(&str, &str)> = e
        .chain
        .iter()
        .map(|s| (s.scope.as_str(), s.outcome.as_str()))
        .collect();
    assert_eq!(
        scopes,
        [
            ("default", "overridden"),
            ("global", "overridden"),
            ("dc", "unset"),
            ("tenant", "overridden"),
            ("database", "overridden"),
            ("object", "effective"),
        ]
    );
    // `db.object` in the object parameter alone.
    let e2 = env.get(k, ctx("acme", None, Some("shop.orders"))).await;
    assert_eq!(e2.value, Value::Int(4));
    // The database's own context.
    assert_eq!(
        env.get(k, ctx("acme", Some("shop"), None)).await.value,
        Value::Int(8)
    );

    // Tightening the tenant's bounds warns about the narrower values.
    let r = env.bounds(k, "tenant:acme", None, Some(5.0)).await.unwrap();
    assert_eq!(r.warnings.len(), 1, "{:?}", r.warnings);
    assert_eq!(r.warnings[0].scope, "db:acme.shop");
    assert!(
        r.warnings[0].message.contains("at db:acme.shop")
            && r.warnings[0].message.ends_with("read as 5"),
        "{}",
        r.warnings[0].message
    );
    assert!(r.summary.is_some());
    assert_eq!(
        r.current.as_ref().unwrap().value,
        Some(Value::Int(7)),
        "value kept"
    );
    let e = env.get(k, ctx("acme", Some("shop"), None)).await;
    assert_eq!(e.value, Value::Int(5));
    assert!(e.clamped);
    assert_eq!(e.source, "database");

    // Reset: the value, the bounds, nothing left.
    let r = env
        .write(
            k,
            "db:acme.shop",
            SettingOp::Reset {
                value: true,
                bounds: false,
            },
        )
        .await
        .unwrap();
    assert!(r.changed);
    assert!(r.current.is_none());
    let again = env
        .write(
            k,
            "db:acme.shop",
            SettingOp::Reset {
                value: true,
                bounds: false,
            },
        )
        .await
        .unwrap();
    assert!(!again.changed);
    let r = env
        .write(
            k,
            "tenant:acme",
            SettingOp::Reset {
                value: true,
                bounds: true,
            },
        )
        .await
        .unwrap();
    assert!(r.changed && r.current.is_none());
    let r = env
        .write(
            k,
            "global",
            SettingOp::Reset {
                value: false,
                bounds: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(r.current.unwrap().bounds, None);

    // Allowed lists as text; lock and unlock keep them.
    let r = env
        .write(
            MODE.key,
            "tenant:acme",
            SettingOp::Bounds {
                min: None,
                max: None,
                allowed: None,
                allowed_text: Some(vec!["off".into(), "recommend".into()]),
                clear: Vec::new(),
            },
        )
        .await
        .unwrap();
    assert_eq!(r.current.unwrap().bounds.unwrap().allowed.unwrap().len(), 2);
    assert_eq!(
        code(&env.set(MODE.key, "db:acme.shop", "on").await.unwrap_err()),
        39080
    );
    env.write(MODE.key, "tenant:acme", SettingOp::Lock { locked: true })
        .await
        .unwrap();
    assert_eq!(
        code(&env.set(MODE.key, "db:acme.shop", "off").await.unwrap_err()),
        39083
    );
    let r = env
        .write(MODE.key, "tenant:acme", SettingOp::Lock { locked: false })
        .await
        .unwrap();
    let b = r.current.unwrap().bounds.unwrap();
    assert!(!b.locked);
    assert_eq!(b.allowed.unwrap().len(), 2);
    env.set(MODE.key, "db:acme.shop", "off").await.unwrap();

    // A dc value and a cluster value, in a cluster context.
    env.set(k, "dc", "3").await.unwrap();
    env.set(k, "cluster:mum1-c1", "2").await.unwrap();
    env.mirrored(
        k,
        Scope::Cluster(ClusterId::new("mum1-c1").unwrap()),
        Some(Value::Int(2)),
    )
    .await;
    let e = env
        .get(
            k,
            SettingContextQuery {
                cluster: Some("mum1-c1".into()),
                ..SettingContextQuery::default()
            },
        )
        .await;
    assert_eq!(
        (e.value.clone(), e.source.as_str()),
        (Value::Int(2), "cluster")
    );
    assert_eq!(e.source_target.as_deref(), Some("mum1-c1"));
}

#[tokio::test]
async fn ancestors_and_a_tenants_rows() {
    let env = Env::new().await;
    let k = MAX_PER_TABLE.key;
    env.set(k, "tenant:acme", "9").await.unwrap();
    env.bounds(k, "tenant:acme", None, Some(12.0))
        .await
        .unwrap();
    // Before the tree loop copies acme's rows, pune doesn't see them.
    assert_eq!(env.get(k, ctx("pune", None, None)).await.source, "default");
    env.tree.sync(env.pune).await.unwrap();
    let e = env.get(k, ctx("pune", Some("shop"), Some("orders"))).await;
    assert_eq!(e.value, Value::Int(9));
    assert_eq!(e.source, "ancestor");
    assert_eq!(e.source_target.as_deref(), Some("acme"));
    // A value above the ancestor's copied bound is refused.
    let r = env
        .set(k, "object:pune.shop.orders", "20")
        .await
        .unwrap_err();
    assert_eq!(code(&r), 39080);
    env.set(k, "object:pune.shop.orders", "11").await.unwrap();

    let rows = env.svc.tenant_settings("pune").await.unwrap();
    assert_eq!(rows.tenant_id, env.pune);
    let got: Vec<(&str, &str)> = rows
        .rows
        .iter()
        .map(|r| (r.scope.as_str(), r.target.as_str()))
        .collect();
    assert_eq!(got, [("object", "pune.shop.orders"), ("ancestor", "acme")]);
    assert_eq!(rows.rows[0].set_by, "op:kiran");
    let acme = env.svc.tenant_settings("acme").await.unwrap();
    assert_eq!(acme.rows.len(), 1);
    assert_eq!(acme.rows[0].value, Some(Value::Int(9)));
    assert_eq!(acme.rows[0].bounds.as_ref().unwrap().max, Some(12.0));
    assert_eq!(acme.tenant_id, env.acme);
    assert_eq!(
        code(&env.svc.tenant_settings("nobody").await.unwrap_err()),
        39072
    );
}

/// The authorizer engine-switch guard on the Admin API: `auth.authorizer` is
/// refused with 39368 at every scope while a live gateway doesn't list the
/// engine, and accepted once none is left that doesn't.
#[tokio::test]
async fn authorizer_needs_every_gateway_to_know_the_engine() {
    let env = Env::new().await;
    let k = sprig_auth::authz::settings::AUTHORIZER.key;
    let gateways = env.live.stores().dc.tier().key("gateways/gw-old").unwrap();
    let put = sprig_meta::Txn {
        then: vec![sprig_meta::Op::Put {
            key: gateways.clone(),
            value: br#"{"v":1,"gateway_id":"gw-old","slot":1,"authorizers":[]}"#.to_vec(),
            lease: None,
        }],
        ..sprig_meta::Txn::default()
    };
    env.live.stores().dc.txn(put).await.unwrap();
    for scope in ["tenant:acme", "dc:mum1", "global"] {
        let e = env.set(k, scope, "mysql_grants").await.unwrap_err();
        assert_eq!(code(&e), 39368, "{scope}: {e}");
        assert!(e.message.contains("gw-old (dc mum1)"), "{e}");
    }
    // A reset isn't checked; an engine this build doesn't know is the
    // bounds' to refuse.
    env.write(
        k,
        "tenant:acme",
        SettingOp::Reset {
            value: true,
            bounds: false,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        code(&env.set(k, "dc:mum1", "supabase").await.unwrap_err()),
        39080
    );
    let del = sprig_meta::Txn {
        then: vec![sprig_meta::Op::Delete(gateways)],
        ..sprig_meta::Txn::default()
    };
    env.live.stores().dc.txn(del).await.unwrap();
    env.set(k, "tenant:acme", "mysql_grants").await.unwrap();
    env.set(k, "dc:mum1", "mysql_grants").await.unwrap();
}
