//! The settings service against the Compose etcd and MariaDB single
//! (`SPRIG_TEST_ETCD`, for example `http://127.0.0.1:2379`, and
//! `SPRIG_TEST_MARIADB_SINGLE`, for example
//! `mysql://root:<password>@127.0.0.1:13301`); skipped without both. The
//! DC and global tiers are private tiers of that etcd and the tenant's
//! catalog is the schema `__sprig_test_settings_<pid>_<tag>_<id>`; all
//! are removed afterwards, also when the case fails.
//!
//! Covers: a global bound refuses a tenant value outside it (39080); a dc
//! value written through the service reaches another process's
//! `LiveSettings` within seconds; an object-scope value resolves with
//! provenance `object`; the tenant's rows.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::FutureExt;
use mysql_async::prelude::Queryable;
use sprig_catalog::store::{
    DatabaseRow, DbState, Namespace, ObjectFlags, ObjectKind, ObjectRow, ObjectState,
};
use sprig_catalog::{DirectoryAllocator, MariaBackend, VersionKind, Versioner};
use sprig_common::ids::{DbId, GroupId, ObjectId};
use sprig_common::time::SystemClock;
use sprig_meta::conformance::{ETCD_ENV, delete_prefix, unique_tier};
use sprig_meta::{EtcdConfig, EtcdStore, MetaStore, Op, Txn};
use sprig_settings::{HardBounds, Scopes, Setting, SettingsStores};

use super::*;
use crate::admin::types::CreateTenantRequest;
use crate::catalogs::{CatalogNaming, ShardMapLocator, StaticPools};
use crate::tenants::{NoHooks, TenantService};
use crate::tree_settings::TreeSync;

const MARIADB_ENV: &str = "SPRIG_TEST_MARIADB_SINGLE";

static OBJECT_LIMIT: Setting<u32> = Setting {
    key: "test_settings.object_limit",
    default: 10,
    scopes: Scopes::ALL,
    hard: HardBounds::range(1, 1000),
    apply: Apply::Live,
    operator_only: false,
    owner: 8,
    doc: "A test key at every scope",
};
sprig_settings::register!(OBJECT_LIMIT);

fn tag() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    format!(
        "{:x}{:x}",
        (nanos ^ std::process::id()) & 0xff_ffff,
        N.fetch_add(1, Ordering::Relaxed)
    )
}

fn code(e: &SprigError) -> u16 {
    e.protocol().mysql_errno
}

struct Env {
    dc_store: Arc<dyn MetaStore>,
    global: Arc<dyn MetaStore>,
    url: String,
    tag: String,
}

impl Env {
    async fn cleanup(&self) {
        if let Ok(mut c) =
            mysql_async::Conn::new(mysql_async::Opts::from_url(&self.url).unwrap()).await
        {
            let like = format!(
                "__sprig\\_test\\_settings\\_{}\\_{}\\_%",
                std::process::id(),
                self.tag
            );
            let schemas: Vec<String> = c
                .exec(
                    "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE ?",
                    (like,),
                )
                .await
                .unwrap_or_default();
            for s in schemas {
                let _ = c.query_drop(format!("DROP DATABASE IF EXISTS `{s}`")).await;
            }
            let _ = c.disconnect().await;
        }
        delete_prefix(&*self.dc_store, &self.dc_store.tier().root()).await;
        delete_prefix(&*self.global, &self.global.tier().root()).await;
    }
}

async fn with_env<F, Fut>(body: F)
where
    F: FnOnce(Arc<Env>) -> Fut,
    Fut: Future<Output = ()>,
{
    let (Some(etcd), Some(url)) = (
        std::env::var(ETCD_ENV)
            .ok()
            .filter(|v| !v.trim().is_empty()),
        std::env::var(MARIADB_ENV)
            .ok()
            .filter(|v| !v.trim().is_empty()),
    ) else {
        eprintln!("skipped the settings live case: set {ETCD_ENV} and {MARIADB_ENV} to run it");
        return;
    };
    let cfg = EtcdConfig::new(vec![etcd]);
    let env = Arc::new(Env {
        dc_store: Arc::new(EtcdStore::connect(unique_tier(), &cfg).await.unwrap()),
        global: Arc::new(EtcdStore::connect(unique_tier(), &cfg).await.unwrap()),
        url,
        tag: tag(),
    });
    let r = AssertUnwindSafe(body(Arc::clone(&env)))
        .catch_unwind()
        .await;
    env.cleanup().await;
    if let Err(p) = r {
        std::panic::resume_unwind(p);
    }
}

async fn until(what: &str, mut f: impl FnMut() -> bool) -> Duration {
    let t0 = Instant::now();
    while !f() {
        assert!(t0.elapsed() < Duration::from_secs(5), "{what} within 5 s");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    t0.elapsed()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_settings_through_the_service() {
    with_env(|env| async move {
        let registry = Registry::global().unwrap();
        let prefix = format!("__sprig_test_settings_{}_{}_", std::process::id(), env.tag);
        let naming: CatalogNaming = Arc::new(move |t| format!("{prefix}{t}"));
        let pools = StaticPools::new()
            .with_url(ClusterId::new("lt-c1").unwrap(), &env.url)
            .unwrap();
        let locator = ShardMapLocator::new(Arc::clone(&env.dc_store), Arc::new(pools), naming);
        let v = Versioner::new(
            MariaBackend::new(Arc::new(locator.clone())),
            Arc::new(DirectoryAllocator::new(Arc::clone(&env.global))),
        );
        let directory = Directory::new(Arc::clone(&env.global));
        let tree = Arc::new(TreeSync::new(v.clone(), directory.clone(), Arc::new(SystemClock)));
        let tenants = TenantService::new(
            Arc::clone(&env.dc_store),
            locator,
            tree,
            Arc::new(NoHooks),
            Arc::new(SystemClock),
            None,
        )
        .unwrap();
        let name = format!("test-{}-acme", env.tag);
        let t = tenants
            .create(
                &CreateTenantRequest {
                    name: name.clone(),
                    parent: None,
                    cluster: None,
                },
                &[],
            )
            .await
            .unwrap();
        let id = t.tenant.tenant_id;
        // A database and a table in the catalog.
        let mut ch = v.change(id, VersionKind::Catalog).await.unwrap();
        let ver = ch.version();
        {
            let (s, q) = ch.tx().parts();
            s.insert_database(
                q,
                &DatabaseRow {
                    db_id: DbId(1),
                    name: "shop".into(),
                    name_key: "shop".into(),
                    default_charset: "utf8mb4".into(),
                    default_collation: "utf8mb4_bin".into(),
                    default_group_id: GroupId(0),
                    state: DbState::Active,
                    options: serde_json::json!({}),
                    created_at: SystemTime::now(),
                    created_by: None,
                    changed_version: ver,
                },
            )
            .await
            .unwrap();
            s.insert_object(
                q,
                &ObjectRow {
                    object_id: ObjectId(1),
                    db_id: DbId(1),
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
                    changed_version: ver,
                },
            )
            .await
            .unwrap();
        }
        ch.commit().await.unwrap();

        // This controller's mirror, and another process's.
        let stores = SettingsStores::new(Arc::clone(&env.global), Arc::clone(&env.dc_store)).unwrap();
        let live = LiveSettings::start(registry, stores.clone()).unwrap();
        let other = LiveSettings::start(registry, stores).unwrap();
        live.ready().await;
        other.ready().await;
        let svc = SettingsService::new(live.clone(), v, directory, Arc::new(SystemClock));
        // The DC leader's fencing compare.
        let leader = env.dc_store.tier().key("leader").unwrap();
        let r = env
            .dc_store
            .txn(Txn {
                then: vec![Op::Put {
                    key: leader.clone(),
                    value: b"{}".to_vec(),
                    lease: None,
                }],
                ..Txn::default()
            })
            .await
            .unwrap();
        let guard = vec![Cmp::CreateRev(leader, r.revision)];
        let k = OBJECT_LIMIT.key;
        let write = |scope: String, op: SettingOp| {
            let (svc, guard) = (&svc, guard.clone());
            async move {
                svc.write(
                    k,
                    &SettingWrite {
                        scope: scope.parse().unwrap(),
                        op,
                        actor: "kiran".into(),
                        reason: "live test".into(),
                    },
                    guard,
                )
                .await
            }
        };
        let set = |v: &str| SettingOp::Set {
            value: None,
            value_text: Some(v.into()),
        };

        // 1. A global bound; a tenant value outside it is refused.
        write(
            "global".into(),
            SettingOp::Bounds {
                min: None,
                max: Some(50.0),
                allowed: None,
                allowed_text: None,
                clear: Vec::new(),
            },
        )
        .await
        .unwrap();
        until("the global bound in the mirror", || {
            live.snapshot().entry(&Scope::Global, k).is_some()
        })
        .await;
        for scope in [format!("tenant:{name}"), format!("object:{name}.shop.orders")] {
            let e = write(scope.clone(), set("51")).await.unwrap_err();
            assert_eq!(code(&e), 39080, "{scope}: {e}");
            assert!(e.message.contains("max 50"), "{e}");
        }

        // 2. A dc value reaches another process's mirror within seconds.
        let dc = live.stores().dc_id().clone();
        let t0 = Instant::now();
        write("dc".into(), set("7")).await.unwrap();
        let took = until("the dc value in another process", || {
            other.get(&OBJECT_LIMIT, &SettingCtx::process(&dc)) == 7
        })
        .await;
        println!(
            "live: dc value visible to another LiveSettings {:?} after the write began ({took:?} after it returned)",
            t0.elapsed()
        );

        // 3. An object-scope value resolves with provenance "object".
        let r = write(format!("object:{name}.shop.orders"), set("25")).await.unwrap();
        assert!(r.version.is_some());
        let e = svc
            .explain(
                k,
                &SettingContextQuery {
                    tenant: Some(name.clone()),
                    db: Some("shop".into()),
                    object: Some("orders".into()),
                    ..SettingContextQuery::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(e.value, Value::Int(25));
        assert_eq!(e.source, "object");
        assert_eq!(e.source_target, Some(format!("{name}.shop.orders")));
        let obj = e.chain.last().unwrap();
        assert_eq!(obj.bounds_in_force.max, Some(50.0));
        assert_eq!(obj.set_by.as_deref(), Some("op:kiran"));
        let dcs = e.chain.iter().find(|s| s.scope == "dc").unwrap();
        assert_eq!((dcs.value.clone(), dcs.outcome.as_str()), (Some(Value::Int(7)), "overridden"));
        let rows = svc.tenant_settings(&name).await.unwrap();
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0].target, format!("{name}.shop.orders"));

        // Resets leave nothing behind.
        write("dc".into(), SettingOp::Reset { value: true, bounds: false }).await.unwrap();
        let r = write("global".into(), SettingOp::Reset { value: true, bounds: true }).await.unwrap();
        assert!(r.changed && r.current.is_none());
    })
    .await;
}
