//! The catalog reconciler against the Compose single node: a test
//! tenant's catalog in `__sprig_test_ctlrec_*`, its physical schemas
//! `t<tenant>_d..` (tenant ids from 9,100,000,000), crashes at each DDL
//! window, outside changes, then passes that repair them. Everything the
//! case created is dropped afterwards. Skipped without
//! `SPRIG_TEST_MARIADB_SINGLE`.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::FutureExt;
use mysql_async::prelude::Queryable;
use mysql_async::{Opts, Pool};
use sprig_catalog::ddl::{
    DdlEngine, DdlFailpoint, DdlOpts, DdlRequest, DdlStatement, DdlStep, MariaExecutor, ObjectName,
};
use sprig_catalog::migrate::{MigrateOpts, Migrator};
use sprig_catalog::{
    CatalogEndpoint, DirectoryAllocator, MariaBackend, StaticLocator, Store, Versioner,
};
use sprig_common::ids::{GroupId, ShardId};
use sprig_common::time::SystemClock;
use sprig_meta::{MemStore, MetaStore, Op, Tier, Txn};

use super::*;

const MARIADB_ENV: &str = "SPRIG_TEST_MARIADB_SINGLE";

fn unique() -> (TenantId, String) {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let pid = u64::from(std::process::id());
    (
        TenantId(9_100_000_000 + pid * 1000 + n),
        format!("__sprig_test_ctlrec_{pid}_{n}"),
    )
}

async fn cleanup(pool: &Pool, tenant: TenantId, catalog: &str) {
    let mut c = pool.get_conn().await.unwrap();
    let names: Vec<String> = c
        .query(format!("SHOW DATABASES LIKE 't{tenant}\\_%'"))
        .await
        .unwrap();
    for n in names {
        c.query_drop(format!("DROP DATABASE IF EXISTS `{n}`"))
            .await
            .unwrap();
    }
    c.query_drop(format!("DROP DATABASE IF EXISTS `{catalog}`"))
        .await
        .unwrap();
}

fn crash(step: DdlStep) -> DdlFailpoint {
    Arc::new(move |_, s| {
        if s == step {
            Err(SprigError::internal("injected crash"))
        } else {
            Ok(())
        }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_reconciler_repairs_crashes_and_outside_changes() {
    let Some(url) = std::env::var(MARIADB_ENV)
        .ok()
        .filter(|v| !v.trim().is_empty())
    else {
        eprintln!("skipped live_reconciler_repairs_crashes_and_outside_changes: set {MARIADB_ENV}");
        return;
    };
    let pool = Pool::new(Opts::from_url(&url).unwrap());
    let (tenant, catalog) = unique();
    let body = async {
        let mut c = pool.get_conn().await.unwrap();
        Migrator::new(&[])
            .unwrap()
            .run(&mut c, &catalog, &MigrateOpts::default())
            .await
            .unwrap();
        drop(c);
        let global: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Global));
        let key = Tier::Global.key(&format!("tenants/{tenant}")).unwrap();
        let entry = serde_json::json!({"v": 1, "tenant_id": tenant.0, "name": "live", "state": "active",
            "home_dc": "mum1", "catalog_version": 0, "auth_version": 0});
        global
            .txn(Txn {
                then: vec![Op::Put {
                    key,
                    value: serde_json::to_vec(&entry).unwrap(),
                    lease: None,
                }],
                ..Txn::default()
            })
            .await
            .unwrap();
        let locator = StaticLocator::new()
            .with(
                tenant,
                CatalogEndpoint {
                    pool: pool.clone(),
                    store: Store::new(&catalog),
                },
            )
            .shared();
        let v = Versioner::new(
            MariaBackend::new(locator),
            Arc::new(DirectoryAllocator::new(Arc::clone(&global))),
        );
        let engine = DdlEngine::new(v, Arc::new(SystemClock)).with_opts(DdlOpts {
            ddl_wait: Duration::from_millis(200),
            poll: Duration::from_millis(20),
            ..DdlOpts::default()
        });
        let exec = Arc::new(MariaExecutor::new(pool.clone()));
        let req = |statement: DdlStatement, sql: &'static str| DdlRequest {
            tenant,
            statement,
            sql,
            logical_sql: sql,
            current_db: Some("billing"),
            user: None,
            lower_case_table_names: false,
        };
        let table = |name: &str| DdlStatement::CreateObject {
            kind: ObjectKind::Table,
            name: ObjectName::bare(name),
            if_not_exists: false,
            or_replace: false,
            parent: None,
        };
        let run =
            async |statement, sql| engine.execute(&req(statement, sql), &*exec).await.unwrap();
        run(
            DdlStatement::CreateDatabase {
                name: "billing".to_owned(),
                if_not_exists: false,
                charset: None,
                collation: None,
            },
            "CREATE DATABASE billing",
        )
        .await;
        run(table("orders"), "CREATE TABLE orders (id INT PRIMARY KEY)").await;
        run(table("old"), "CREATE TABLE old (id INT)").await;
        let s = SchemaName::shard(tenant, DbId(1), GroupId(1), ShardId(1));

        // Three crashes: after CREATE TABLE ran, before ALTER ran, before
        // DROP TABLE ran.
        assert!(
            engine
                .clone()
                .with_failpoint(crash(DdlStep::Applied))
                .execute(&req(table("items"), "CREATE TABLE items (id INT)"), &*exec)
                .await
                .is_err()
        );
        assert!(
            engine
                .clone()
                .with_failpoint(crash(DdlStep::Prepared))
                .execute(
                    &req(
                        DdlStatement::AlterObject {
                            kind: ObjectKind::Table,
                            name: ObjectName::bare("orders"),
                            if_exists: false,
                            rename_to: None,
                        },
                        "ALTER TABLE orders ADD COLUMN c INT"
                    ),
                    &*exec
                )
                .await
                .is_err()
        );
        assert!(
            engine
                .clone()
                .with_failpoint(crash(DdlStep::Prepared))
                .execute(
                    &req(
                        DdlStatement::DropObjects {
                            kind: ObjectKind::Table,
                            names: vec![ObjectName::bare("old")],
                            if_exists: false
                        },
                        "DROP TABLE old"
                    ),
                    &*exec
                )
                .await
                .is_err()
        );

        // Outside changes: a table in the shard schema, an orphan schema.
        let orphan = SchemaName::shard(tenant, DbId(99), GroupId(9), ShardId(9));
        let mut c = pool.get_conn().await.unwrap();
        c.query_drop(format!("CREATE TABLE `{s}`.legacy (id INT)"))
            .await
            .unwrap();
        c.query_drop(format!("CREATE DATABASE `{orphan}`"))
            .await
            .unwrap();
        drop(c);

        let dir = Directory::new(Arc::clone(&global));
        let cfg = ReconcileConfig {
            interval: Duration::from_secs(300),
            stuck: Duration::ZERO,
            orphan_retention: Duration::ZERO,
            tombstone_retention: Duration::from_secs(3600),
        };
        let rec = CatalogReconciler::new(
            engine.clone(),
            dir,
            Arc::new(FixedExecutor(Arc::clone(&exec) as Arc<dyn DdlExecutor>)),
            Arc::new(LogDecisions),
            DcId::new("mum1").unwrap(),
            None,
            Arc::new(SystemClock),
        )
        .with_config(cfg);
        let r = rec.pass(tenant).await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        let mut labels: Vec<&str> = r.resolved.iter().map(|(_, r)| r.label()).collect();
        labels.sort_unstable();
        assert_eq!(labels, ["finished", "rolled_back", "rolled_forward"]);
        assert_eq!(r.adopted, 1, "legacy");
        assert_eq!(r.orphans, [orphan]);
        assert!(r.objects_missing.is_empty() && r.schemas_missing.is_empty());

        let snap = engine
            .versioner()
            .backend()
            .full_snapshot(tenant)
            .await
            .unwrap();
        let live = |name: &str| {
            snap.objects
                .iter()
                .find(|o| o.name == name && o.state != ObjectState::Dropped)
                .cloned()
        };
        assert_eq!(live("items").unwrap().state, ObjectState::Active);
        assert_eq!(live("orders").unwrap().schema_version, 1);
        assert_eq!(live("orders").unwrap().pending_job_id, None);
        assert!(live("old").is_none());
        assert!(
            exec.show_create(&s, ObjectKind::Table, "old")
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(live("legacy").unwrap().state, ObjectState::Active);
        assert!(DdlIntent::all(&snap.settings).is_empty());

        // The second pass drops the (empty) orphan; nothing else to do.
        let r = rec.pass(tenant).await.unwrap();
        assert_eq!(r.orphans_dropped, [orphan]);
        assert!(r.resolved.is_empty() && r.adopted == 0);
        assert!(exec.schema(&orphan).await.unwrap().is_none());

        // A dropped database whose schema came back is dropped again.
        run(
            DdlStatement::DropDatabase {
                name: "billing".to_owned(),
                if_exists: false,
            },
            "DROP DATABASE billing",
        )
        .await;
        let mut c = pool.get_conn().await.unwrap();
        c.query_drop(format!("CREATE DATABASE `{s}`"))
            .await
            .unwrap();
        drop(c);
        let r = rec.pass(tenant).await.unwrap();
        assert_eq!(r.dropped_schemas, [s]);
        assert!(exec.schema(&s).await.unwrap().is_none());
    };
    let r = AssertUnwindSafe(body).catch_unwind().await;
    cleanup(&pool, tenant, &catalog).await;
    pool.disconnect().await.unwrap();
    if let Err(p) = r {
        std::panic::resume_unwind(p);
    }
}

/// One pass of this build's reconciler over one tenant of a running
/// deployment (`SPRIG_TEST_RECONCILE_TENANT`, etcd at `SPRIG_TEST_ETCD`,
/// DC `SPRIG_TEST_DC`): how a tenant that an older build left `deleting`
/// (it skipped deleting tenants, so a stuck job and the databases behind
/// it stayed for ever) is released before the deployment's controller runs
/// this build. The controller's `tenant_bootstrap` loop then finishes the
/// deletion itself.
#[tokio::test]
#[ignore = "acts on a running deployment"]
async fn pass_one_live_tenant() {
    let Some(tenant) = std::env::var("SPRIG_TEST_RECONCILE_TENANT")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(TenantId)
    else {
        eprintln!("set SPRIG_TEST_RECONCILE_TENANT");
        return;
    };
    let meta = sprig_common::config::MetaConfig {
        dc_seeds: vec![
            std::env::var("SPRIG_TEST_ETCD").unwrap_or_else(|_| "http://127.0.0.1:2379".into()),
        ],
        global_seeds: Vec::new(),
        deployment_id: "sprigdb-dev".into(),
    };
    let dc = DcId::new(std::env::var("SPRIG_TEST_DC").unwrap_or_else(|_| "dc-a".into())).unwrap();
    let tiers =
        sprig_meta::etcd::connect_tiers(dc.clone(), &meta, None, Duration::from_secs(5), true)
            .await
            .unwrap();
    let dcs: Arc<dyn MetaStore> = Arc::new(tiers.dc);
    let global: Arc<dyn MetaStore> = Arc::new(tiers.global.unwrap());
    let registry =
        sprig_cluster::registry::ClusterRegistry::new(Arc::clone(&global), Arc::clone(&dcs))
            .unwrap();
    let pools: Arc<dyn crate::catalogs::ClusterPools> =
        Arc::new(crate::catalogs::RegistryPools::new(registry, None).budget("reconcile_test", 4));
    let locator = crate::catalogs::ShardMapLocator::new(
        Arc::clone(&dcs),
        pools,
        crate::catalogs::default_naming(),
    );
    let v = Versioner::new(
        MariaBackend::new(Arc::new(locator.clone())),
        Arc::new(DirectoryAllocator::new(Arc::clone(&global))),
    );
    let rec = CatalogReconciler::new(
        DdlEngine::new(v, Arc::new(SystemClock)),
        Directory::new(global),
        Arc::new(PoolExecutors::new(locator)),
        Arc::new(LogDecisions),
        dc,
        None,
        Arc::new(SystemClock),
    );
    let r = rec.pass(tenant).await.unwrap();
    eprintln!(
        "tenant {tenant}: resolved {:?}, databases dropped {:?}, errors {:?}",
        r.resolved, r.deleted_databases, r.errors
    );
    assert!(r.errors.is_empty());
}
