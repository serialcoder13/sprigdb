//! The catalog reconciler over the in-memory catalog, the in-memory
//! executor and a `MemStore` directory: stuck jobs (young, busy, stuck),
//! missing schemas and objects, outside objects, orphans, schemas of
//! dropped databases, purging by age, clamped values, and the loop's
//! keys and timing.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::time::{Duration, SystemTime};

use sprig_catalog::DirectoryAllocator;
use sprig_catalog::cache::mem::MemSource;
use sprig_catalog::ddl::mem::MemExecutor;
use sprig_catalog::ddl::{
    DdlEngine, DdlFailpoint, DdlOpts, DdlRequest, DdlStatement, DdlStep, ObjectName, PhysicalObject,
};
use sprig_catalog::store::{SettingRow, SettingScope};
use sprig_catalog::{VersionKind, Versioner};
use sprig_common::ids::{GroupId, ShardId};
use sprig_common::time::SystemClock;
use sprig_meta::{MemStore, MetaStore, Tier};
use std::sync::Arc;

use super::*;
use crate::tree::NewTenant;

fn dc() -> DcId {
    DcId::new("mum1").unwrap()
}

struct Env {
    dir: Directory,
    v: Versioner<MemSource>,
    exec: Arc<MemExecutor>,
    tenant: TenantId,
}

impl Env {
    async fn new() -> Self {
        let store = MemStore::new(Tier::Dc(dc()));
        let global: Arc<dyn MetaStore> = Arc::new(store.with_tier(Tier::Global));
        let dir = Directory::new(Arc::clone(&global));
        let v = Versioner::new(MemSource::new(), Arc::new(DirectoryAllocator::new(global)));
        let e = dir
            .create(
                &NewTenant {
                    name: "acme".to_owned(),
                    parent: None,
                    home_dc: dc(),
                    placement_hint: None,
                },
                64,
                SystemTime::now(),
            )
            .await
            .unwrap();
        let tenant = e.body().tenant_id;
        v.backend().bootstrap(tenant);
        dir.update(tenant, |e| {
            e.state = TenantState::Active;
            Ok(true)
        })
        .await
        .unwrap();
        Self {
            dir,
            v,
            exec: Arc::new(MemExecutor::new()),
            tenant,
        }
    }

    fn engine(&self) -> DdlEngine<MemSource> {
        DdlEngine::new(self.v.clone(), Arc::new(SystemClock)).with_opts(DdlOpts {
            ddl_wait: Duration::from_millis(100),
            poll: Duration::from_millis(10),
            ..DdlOpts::default()
        })
    }

    fn reconciler(&self, cfg: ReconcileConfig) -> CatalogReconciler<MemSource> {
        CatalogReconciler::new(
            self.engine(),
            self.dir.clone(),
            Arc::new(FixedExecutor(Arc::clone(&self.exec) as Arc<dyn DdlExecutor>)),
            Arc::new(LogDecisions),
            dc(),
            None,
            Arc::new(SystemClock),
        )
        .with_config(cfg)
    }

    fn req<'a>(&self, statement: DdlStatement, sql: &'a str) -> DdlRequest<'a> {
        DdlRequest {
            tenant: self.tenant,
            statement,
            sql,
            logical_sql: sql,
            current_db: Some("billing"),
            user: None,
            lower_case_table_names: false,
        }
    }

    async fn run(&self, statement: DdlStatement, sql: &str) {
        self.engine()
            .execute(&self.req(statement, sql), &*self.exec)
            .await
            .unwrap();
    }

    async fn crash(&self, step: DdlStep, statement: DdlStatement, sql: &str) {
        let f: DdlFailpoint = Arc::new(move |_, s| {
            if s == step {
                Err(SprigError::internal("injected crash"))
            } else {
                Ok(())
            }
        });
        assert!(
            self.engine()
                .with_failpoint(f)
                .execute(&self.req(statement, sql), &*self.exec)
                .await
                .is_err()
        );
    }

    fn schema(&self, db: u32) -> SchemaName {
        SchemaName::shard(self.tenant, DbId(db), GroupId(db), ShardId(db))
    }

    fn snapshot(&self) -> Snapshot {
        self.v.backend().tables(self.tenant).unwrap().snapshot(None)
    }

    fn live(&self, name: &str) -> Option<sprig_catalog::store::ObjectRow> {
        self.snapshot()
            .objects
            .into_iter()
            .find(|o| o.name == name && o.state != ObjectState::Dropped)
    }
}

fn create_db(name: &str) -> DdlStatement {
    DdlStatement::CreateDatabase {
        name: name.to_owned(),
        if_not_exists: false,
        charset: None,
        collation: None,
    }
}

fn create_table(name: &str) -> DdlStatement {
    DdlStatement::CreateObject {
        kind: ObjectKind::Table,
        name: ObjectName::bare(name),
        if_not_exists: false,
        or_replace: false,
        parent: None,
    }
}

fn alter(name: &str) -> DdlStatement {
    DdlStatement::AlterObject {
        kind: ObjectKind::Table,
        name: ObjectName::bare(name),
        if_exists: false,
        rename_to: None,
    }
}

fn now_cfg() -> ReconcileConfig {
    ReconcileConfig {
        interval: Duration::from_secs(300),
        stuck: Duration::ZERO,
        orphan_retention: Duration::ZERO,
        tombstone_retention: Duration::from_secs(24 * 3600),
    }
}

async fn billing() -> Env {
    let env = Env::new().await;
    env.run(create_db("billing"), "CREATE DATABASE billing")
        .await;
    env.run(create_table("orders"), "CREATE TABLE orders (id INT)")
        .await;
    env
}

#[tokio::test]
async fn stuck_jobs_are_finished_or_rolled_back_once_old_enough() {
    let env = billing().await;
    env.crash(
        DdlStep::Applied,
        create_table("items"),
        "CREATE TABLE items (id INT)",
    )
    .await;
    env.crash(
        DdlStep::Prepared,
        alter("orders"),
        "ALTER TABLE orders ADD c INT",
    )
    .await;

    // Too young: left alone.
    let young = ReconcileConfig {
        stuck: Duration::from_secs(3600),
        ..now_cfg()
    };
    let r = env.reconciler(young).pass(env.tenant).await.unwrap();
    assert_eq!(r.jobs_waiting, 2);
    assert!(r.resolved.is_empty());
    // A job's schema busy with DDL: left alone too, whatever its age.
    env.exec.set_busy(env.schema(1), true);
    let r = env.reconciler(now_cfg()).pass(env.tenant).await.unwrap();
    assert_eq!(r.jobs_waiting, 2);
    // Neither held database was compared, so nothing was adopted.
    assert_eq!(r.adopted, 0);
    env.exec.set_busy(env.schema(1), false);

    let r = env.reconciler(now_cfg()).pass(env.tenant).await.unwrap();
    let mut labels: Vec<&str> = r.resolved.iter().map(|(_, r)| r.label()).collect();
    labels.sort_unstable();
    assert_eq!(labels, ["finished", "rolled_back"]);
    assert_eq!(env.live("items").unwrap().state, ObjectState::Active);
    assert_eq!(env.live("orders").unwrap().pending_job_id, None);
    assert_eq!(r.adopted, 0, "items finished, not adopted");
    assert!(r.errors.is_empty(), "{:?}", r.errors);
    assert!(
        env.snapshot()
            .settings
            .iter()
            .all(|s| !s.key.starts_with("sprig.ddl."))
    );
}

#[tokio::test]
async fn missing_and_outside_objects() {
    let env = billing().await;
    let s = env.schema(1);
    // Dropped outside SprigDB: an alert, the row stays.
    env.exec.drop_object(&s, ObjectKind::Table, "orders");
    // Created outside SprigDB: adopted, internal objects ignored.
    for name in ["legacy", "__sprig_dtx"] {
        env.exec.create_object(
            &s,
            PhysicalObject {
                kind: ObjectKind::Table,
                name: name.to_owned(),
                parent: None,
            },
        );
    }
    let r = env.reconciler(now_cfg()).pass(env.tenant).await.unwrap();
    assert_eq!(r.objects_missing, ["billing.orders"]);
    assert_eq!(r.adopted, 1);
    assert!(env.live("orders").is_some());
    assert_eq!(env.live("legacy").unwrap().state, ObjectState::Active);
    assert!(env.live("__sprig_dtx").is_none());

    // A database whose schema is gone: an alert.
    env.run(create_db("crm"), "CREATE DATABASE crm").await;
    let crm = env.schema(2);
    env.exec
        .execute(&sprig_catalog::ddl::drop_schema(crm))
        .await
        .unwrap();
    let r = env.reconciler(now_cfg()).pass(env.tenant).await.unwrap();
    assert_eq!(r.schemas_missing, [crm]);
    assert_eq!(r.adopted, 0, "adopted once only");
}

#[tokio::test]
async fn orphans_are_quarantined_then_dropped_when_empty() {
    let env = billing().await;
    let empty = SchemaName::shard(env.tenant, DbId(77), GroupId(7), ShardId(7));
    let full = SchemaName::shard(env.tenant, DbId(78), GroupId(8), ShardId(8));
    env.exec.create_schema(empty);
    env.exec.create_schema(full);
    env.exec.create_object(
        &full,
        PhysicalObject {
            kind: ObjectKind::Table,
            name: "data".to_owned(),
            parent: None,
        },
    );
    // Another tenant's schema is never ours.
    let other = SchemaName::shard(TenantId(env.tenant.0 + 1), DbId(1), GroupId(1), ShardId(1));
    env.exec.create_schema(other);

    let week = ReconcileConfig {
        orphan_retention: Duration::from_secs(7 * 86_400),
        ..now_cfg()
    };
    let rec = env.reconciler(week);
    let r = rec.pass(env.tenant).await.unwrap();
    assert_eq!(r.orphans, [empty, full]);
    assert!(r.orphans_dropped.is_empty(), "quarantined first");
    // Past the quarantine (retention zero): the empty one goes.
    let rec = env.reconciler(now_cfg());
    rec.pass(env.tenant).await.unwrap();
    let r = rec.pass(env.tenant).await.unwrap();
    assert_eq!(r.orphans_dropped, [empty]);
    assert!(!env.exec.has_schema(&empty));
    assert!(env.exec.has_schema(&full));
    assert!(env.exec.has_schema(&other));
}

#[tokio::test]
async fn a_dropped_databases_schema_is_dropped() {
    let env = billing().await;
    env.run(
        DdlStatement::DropDatabase {
            name: "billing".to_owned(),
            if_exists: false,
        },
        "DROP DATABASE billing",
    )
    .await;
    // Recreated behind SprigDB's back (or a drop that never finished).
    let s = env.schema(1);
    env.exec.create_schema(s);
    let r = env.reconciler(now_cfg()).pass(env.tenant).await.unwrap();
    assert_eq!(r.dropped_schemas, [s]);
    assert!(
        r.orphans.is_empty(),
        "a tombstone's schema is not an orphan"
    );
    assert!(!env.exec.has_schema(&s));
}

#[tokio::test]
async fn tombstones_are_purged_after_the_retention() {
    let env = billing().await;
    env.run(
        DdlStatement::DropObjects {
            kind: ObjectKind::Table,
            names: vec![ObjectName::bare("orders")],
            if_exists: false,
        },
        "DROP TABLE orders",
    )
    .await;
    let dropped = env.snapshot().objects.len();
    assert_eq!(dropped, 1);
    let rec = env.reconciler(now_cfg());
    // Committed just now: kept.
    let r = rec.pass(env.tenant).await.unwrap();
    assert!(r.purged.is_none());
    assert_eq!(env.snapshot().objects.len(), 1);
    // The version row was last written 25 hours ago: purged.
    env.v
        .backend()
        .set_version_time(
            env.tenant,
            SystemTime::now() - Duration::from_secs(25 * 3600),
        )
        .unwrap();
    let r = rec.pass(env.tenant).await.unwrap();
    let (up_to, report) = r.purged.unwrap();
    assert_eq!(up_to, env.snapshot().versions.catalog);
    assert_eq!(report.objects, 1);
    assert!(env.snapshot().objects.is_empty());
    assert_eq!(env.snapshot().versions.purged, up_to);
    // Nothing new: no second purge.
    assert!(rec.pass(env.tenant).await.unwrap().purged.is_none());
}

#[tokio::test]
async fn clamped_values_are_counted() {
    let env = billing().await;
    // A tenant value above the key's hard maximum: read clamped.
    let mut c = env
        .v
        .change(env.tenant, VersionKind::Catalog)
        .await
        .unwrap();
    let v = c.version();
    c.tx().put_setting(SettingRow {
        scope: SettingScope::Tenant,
        key: "catalog.ddl_wait_ms".to_owned(),
        value: Some(serde_json::json!(99_000_000)),
        bounds: None,
        set_by: "system".to_owned(),
        set_at: SystemTime::now(),
        version: v,
    });
    c.commit().await.unwrap();
    let r = env.reconciler(now_cfg()).pass(env.tenant).await.unwrap();
    assert_eq!(r.clamped.get("tenant"), Some(&1));
}

#[tokio::test]
async fn the_loop_lists_active_tenants_here_and_paces_itself() {
    let env = billing().await;
    let rec = env.reconciler(now_cfg());
    let store = MemStore::new(Tier::Dc(dc()));
    let cx = LoopCx::new(
        Arc::new(store.clone()),
        Arc::new(store.with_tier(Tier::Global)),
        None,
        Arc::new(SystemClock),
        sprig_common::CancellationToken::new(),
        tokio::sync::watch::channel(Arc::new(crate::settings::ControlSettings::default())).1,
    );
    assert_eq!(rec.all_keys(&cx).await.unwrap(), [env.tenant]);
    assert_eq!(
        rec.reconcile(&cx, &env.tenant).await.unwrap(),
        Outcome::RequeueAfter(Duration::from_secs(300))
    );
    // Asked again at once: waits out the rest of the interval.
    match rec.reconcile(&cx, &env.tenant).await.unwrap() {
        Outcome::RequeueAfter(d) => {
            assert!(d <= Duration::from_secs(300) && d > Duration::from_secs(290));
        }
        other => panic!("{other:?}"),
    }
    // A tenant that isn't active isn't reconciled.
    env.dir
        .update(env.tenant, |e| {
            e.state = TenantState::Deleting;
            Ok(true)
        })
        .await
        .unwrap();
    assert!(rec.all_keys(&cx).await.unwrap().is_empty());
}
