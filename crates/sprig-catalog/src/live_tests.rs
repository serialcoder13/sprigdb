//! Live tests against the Compose stack.
//!
//! - `SPRIG_TEST_MARIADB_SINGLE` (for example
//!   `mysql://root:sprig-dev-root@127.0.0.1:13301`) names the MariaDB; without
//!   it every case is skipped with a message.
//! - `SPRIG_TEST_ETCD` (for example `http://127.0.0.1:2379`) names an etcd
//!   for the global tier; without it the cases use `MemStore`. On etcd the
//!   "global" tier is a private DC tier standing in for `/sprig/g/`, as in
//!   `sprig-settings`, so cases never touch shared keys, and its keys are
//!   deleted afterwards.
//!
//! Each case works in its own schema `__sprig_test_catalog_<pid>_<n>`,
//! dropped afterwards, also when the case fails.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::many_single_char_names,
    clippy::similar_names
)]

use std::collections::BTreeMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::FutureExt;
use mysql_async::prelude::Queryable;
use mysql_async::{Opts, Pool};
use serde_json::json;
use sprig_common::ids::{DbId, GroupId, JobId, ObjectId, TenantId, UserId};
use sprig_common::{ErrorKind, SprigCode, SprigError};
use sprig_meta::conformance::{ETCD_ENV, delete_prefix, unique_tier};
use sprig_meta::elect::{Election, Elector, LeaderRecord};
use sprig_meta::session::SessionOpts;
use sprig_meta::{Consistency, EtcdConfig, EtcdStore, MemStore, MetaStore, Op, Tier, Txn};
use sprig_settings::Bounds;

use crate::migrate::{
    self, CORE, Ceiling, MigrateOpts, Migration, Migrator, Module, ModuleFormat, Phase,
};
use crate::store::*;
use crate::version::*;

const MARIADB_ENV: &str = "SPRIG_TEST_MARIADB_SINGLE";

pub(crate) fn maria_url() -> Option<String> {
    std::env::var(MARIADB_ENV)
        .ok()
        .filter(|v| !v.trim().is_empty())
}

pub(crate) fn unique_schema() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    format!(
        "__sprig_test_catalog_{}_{:x}_{}",
        std::process::id(),
        nanos,
        N.fetch_add(1, Ordering::Relaxed)
    )
}

/// A tenant id no other case uses.
pub(crate) fn unique_tenant() -> TenantId {
    static N: AtomicU64 = AtomicU64::new(0);
    TenantId(
        9_000_000_000 + u64::from(std::process::id()) * 1000 + N.fetch_add(1, Ordering::Relaxed),
    )
}

/// Runs `body` with a pool and a fresh schema name; drops the schema
/// afterwards, also on failure. Skips without `SPRIG_TEST_MARIADB_SINGLE`.
pub(crate) async fn with_schema<F, Fut>(case: &str, body: F)
where
    F: FnOnce(Pool, String) -> Fut,
    Fut: Future<Output = ()>,
{
    let Some(url) = maria_url() else {
        eprintln!("skipped {case}: set {MARIADB_ENV} to run it");
        return;
    };
    let pool = Pool::new(Opts::from_url(&url).unwrap());
    let schema = unique_schema();
    let r = AssertUnwindSafe(body(pool.clone(), schema.clone()))
        .catch_unwind()
        .await;
    let mut conn = pool.get_conn().await.unwrap();
    conn.query_drop(format!("DROP DATABASE IF EXISTS `{schema}`"))
        .await
        .unwrap();
    drop(conn);
    pool.disconnect().await.unwrap();
    if let Err(p) = r {
        std::panic::resume_unwind(p);
    }
}

pub(crate) async fn migrate(pool: &Pool, schema: &str) {
    let mut conn = pool.get_conn().await.unwrap();
    Migrator::new(&[])
        .unwrap()
        .run(&mut conn, schema, &MigrateOpts::default())
        .await
        .unwrap();
}

/// Runs `body` with the global tier (see [`global_store`]) and deletes
/// its keys afterwards, also when the body fails.
pub(crate) async fn with_global<F, Fut>(body: F)
where
    F: FnOnce(Arc<dyn MetaStore>) -> Fut,
    Fut: Future<Output = ()>,
{
    let g = global_store().await;
    let r = AssertUnwindSafe(body(Arc::clone(&g))).catch_unwind().await;
    delete_prefix(&*g, &g.tier().root()).await;
    if let Err(p) = r {
        std::panic::resume_unwind(p);
    }
}

/// The global tier: etcd when configured (a private tier), else memory.
pub(crate) async fn global_store() -> Arc<dyn MetaStore> {
    match std::env::var(ETCD_ENV) {
        Ok(url) if !url.trim().is_empty() => Arc::new(
            EtcdStore::connect(unique_tier(), &EtcdConfig::new(vec![url]))
                .await
                .unwrap(),
        ),
        _ => Arc::new(MemStore::new(Tier::Global)),
    }
}

pub(crate) async fn seed_directory(store: &dyn MetaStore, tenant: TenantId) {
    let key = store.tier().key(&format!("tenants/{tenant}")).unwrap();
    let body = json!({"v": 1, "tenant_id": tenant.0, "name": "live", "state": "active",
                      "catalog_version": 0, "auth_version": 0});
    store
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

async fn directory_versions(store: &dyn MetaStore, tenant: TenantId) -> DirectoryVersions {
    let key = store.tier().key(&format!("tenants/{tenant}")).unwrap();
    let kv = store
        .get(&key, Consistency::Linearizable)
        .await
        .unwrap()
        .unwrap();
    serde_json::from_slice(&kv.value).unwrap()
}

pub(crate) fn catalog_for(
    pool: &Pool,
    schema: &str,
    tenant: TenantId,
    store: Arc<dyn MetaStore>,
) -> MariaCatalog {
    let locator = StaticLocator::new()
        .with(
            tenant,
            CatalogEndpoint {
                pool: pool.clone(),
                store: Store::new(schema),
            },
        )
        .shared();
    Versioner::new(
        MariaBackend::new(locator),
        Arc::new(DirectoryAllocator::new(store)),
    )
}

fn at(micros: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_micros(1_762_560_000_000_000 + micros)
}

fn setting(key: &str, value: serde_json::Value, version: u64) -> SettingRow {
    SettingRow {
        scope: SettingScope::Tenant,
        key: key.to_owned(),
        value: Some(value),
        bounds: None,
        set_by: "user:7".to_owned(),
        set_at: at(1),
        version,
    }
}

fn object(id: u64, name: &str, version: u64) -> ObjectRow {
    ObjectRow {
        object_id: ObjectId(id),
        db_id: DbId(1),
        namespace: Namespace::Relation,
        name: name.to_owned(),
        name_key: name.to_lowercase(),
        kind: ObjectKind::Table,
        parent_id: None,
        state: ObjectState::Active,
        schema_version: 1,
        flags: ObjectFlags::empty(),
        pending_job_id: None,
        created_at: at(2),
        changed_version: version,
    }
}

fn database(id: u32, name: &str, version: u64) -> DatabaseRow {
    DatabaseRow {
        db_id: DbId(id),
        name: name.to_owned(),
        name_key: name.to_lowercase(),
        default_charset: "utf8mb4".to_owned(),
        default_collation: "utf8mb4_uca1400_ai_ci".to_owned(),
        default_group_id: GroupId(0),
        state: DbState::Active,
        options: json!({}),
        created_at: at(3),
        created_by: Some(UserId(7)),
        changed_version: version,
    }
}

fn is_dup(e: &SprigError) -> bool {
    matches!(e.kind, ErrorKind::Backend { errno: 1062, .. })
}

// ---- migrations ----

static EXT: &[Migration] = &[
    Migration {
        format: 1,
        phase: Phase::Expand,
        name: "ext_items",
        statements: &[
            "CREATE TABLE IF NOT EXISTS ext_items (id INT PRIMARY KEY, a INT) ENGINE=InnoDB",
        ],
    },
    Migration {
        format: 2,
        phase: Phase::Expand,
        name: "ext_items.b",
        statements: &["ALTER TABLE ext_items ADD COLUMN IF NOT EXISTS b INT"],
    },
    Migration {
        format: 3,
        phase: Phase::Contract,
        name: "drop ext_items.a",
        statements: &["ALTER TABLE ext_items DROP COLUMN IF EXISTS a"],
    },
];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_migrations_are_idempotent_concurrent_and_recorded() {
    with_schema("live_migrations", |pool, schema| async move {
        // Four processes migrate the same new catalog at once.
        let mut runs = Vec::new();
        for i in 0..4 {
            let pool = pool.clone();
            let schema = schema.clone();
            runs.push(tokio::spawn(async move {
                let mut conn = pool.get_conn().await.unwrap();
                let opts = MigrateOpts {
                    applied_by: format!("test:{i}"),
                    ..MigrateOpts::default()
                };
                Migrator::new(&[])
                    .unwrap()
                    .run(&mut conn, &schema, &opts)
                    .await
                    .unwrap()
            }));
        }
        let mut applied = 0;
        for r in runs {
            let report = r.await.unwrap();
            applied += report.applied.len();
            assert_eq!(
                report.state[CORE],
                ModuleFormat {
                    format: 1,
                    compat: 1
                }
            );
        }
        assert_eq!(applied, 1, "exactly one run applied the core migration");

        let mut conn = pool.get_conn().await.unwrap();
        let again = Migrator::new(&[])
            .unwrap()
            .run(&mut conn, &schema, &MigrateOpts::default())
            .await
            .unwrap();
        assert!(again.applied.is_empty());

        // The tables, utf8mb4_bin throughout.
        let tables: Vec<(String, String)> = conn
            .exec(
                "SELECT TABLE_NAME, TABLE_COLLATION FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME",
                (&schema,),
            )
            .await
            .unwrap();
        let names: Vec<&str> = tables.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(
            names,
            ["databases", "objects", "schema_versions", "settings"]
        );
        assert!(tables.iter().all(|(_, c)| c == "utf8mb4_bin"), "{tables:?}");

        let state = migrate::read_state(&mut conn, &schema).await.unwrap();
        assert_eq!(
            state[CORE],
            ModuleFormat {
                format: 1,
                compat: 1
            }
        );
        let hist = migrate::read_history(&mut conn, &schema, CORE)
            .await
            .unwrap();
        assert_eq!(hist.history.len(), 1);
        assert!(hist.history[0].by.starts_with("test:"));
        let store = Store::new(&schema);
        assert_eq!(
            store.versions(&mut conn).await.unwrap(),
            Versions::default()
        );

        // Another process holds the lock: a short wait gives up with 39071.
        let mut holder = pool.get_conn().await.unwrap();
        let lock = format!("sprig_migrate.{schema}");
        let got: Option<i64> = holder
            .exec_first("SELECT GET_LOCK(?, 0)", (&lock,))
            .await
            .unwrap();
        assert_eq!(got, Some(1));
        let opts = MigrateOpts {
            lock_timeout: Duration::from_secs(1),
            ..MigrateOpts::default()
        };
        // R9 (r9scale): a run with nothing to apply reads the format and
        // returns without the lock.
        let r = Migrator::new(&[])
            .unwrap()
            .run(&mut conn, &schema, &opts)
            .await
            .unwrap();
        assert!(r.applied.is_empty() && r.blocked.is_empty(), "{r:?}");
        assert_eq!(r.state[CORE].format, 1);
        let pending = Module {
            name: "test_ext",
            migrations: EXT,
        };
        let e = Migrator::new(&[pending])
            .unwrap()
            .run(&mut conn, &schema, &opts)
            .await
            .unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::CatalogConflict));
        holder
            .exec_drop("DO RELEASE_LOCK(?)", (&lock,))
            .await
            .unwrap();

        // An extension module: expands run by default, the contract waits
        // for its ceiling.
        let ext = Module {
            name: "test_ext",
            migrations: EXT,
        };
        let m = Migrator::new(&[ext]).unwrap();
        let r = m
            .run(&mut conn, &schema, &MigrateOpts::default())
            .await
            .unwrap();
        assert_eq!(
            r.applied.iter().map(|a| a.format).collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(r.blocked.len(), 1);
        assert_eq!(r.blocked[0].phase, Phase::Contract);
        assert_eq!(
            r.state["test_ext"],
            ModuleFormat {
                format: 2,
                compat: 1
            }
        );
        // A binary that reads only format 1 still works after expands.
        let older = Module {
            name: "test_ext",
            migrations: &EXT[..1],
        };
        Migrator::new(&[older])
            .unwrap()
            .run(&mut conn, &schema, &MigrateOpts::default())
            .await
            .unwrap();
        let opts = MigrateOpts {
            ceilings: BTreeMap::from([("test_ext".to_owned(), Ceiling::NONE)]),
            ..MigrateOpts::default()
        };
        let r = m.run(&mut conn, &schema, &opts).await.unwrap();
        assert_eq!(r.applied.len(), 1);
        assert_eq!(
            r.state["test_ext"],
            ModuleFormat {
                format: 3,
                compat: 3
            }
        );
        let cols: Vec<String> = conn
            .exec(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? \
                 AND TABLE_NAME = 'ext_items' ORDER BY ORDINAL_POSITION",
                (&schema,),
            )
            .await
            .unwrap();
        assert_eq!(cols, ["id", "b"]);
        // After the contract, the older binary is refused with 39073.
        let e = Migrator::new(&[older])
            .unwrap()
            .run(&mut conn, &schema, &MigrateOpts::default())
            .await
            .unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::CatalogFormatTooNew));
        let hist = migrate::read_history(&mut conn, &schema, "test_ext")
            .await
            .unwrap();
        assert_eq!(hist.history.len(), 3);
    })
    .await;
}

#[tokio::test]
async fn live_a_crash_between_statements_and_record_reruns_safely() {
    with_schema("live_crash_migration", |pool, schema| async move {
        let mut conn = pool.get_conn().await.unwrap();
        // The core migration's statements ran, then the process died
        // before recording it.
        conn.query_drop(format!(
            "CREATE DATABASE `{schema}` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
        ))
        .await
        .unwrap();
        conn.query_drop(format!("USE `{schema}`")).await.unwrap();
        for stmt in migrate::CORE_MIGRATIONS[0].statements {
            conn.query_drop(*stmt).await.unwrap();
        }
        conn.query_drop("UPDATE settings SET value = '5' WHERE setting_key = 'sprig.next_db_id'")
            .await
            .unwrap();
        let r = Migrator::new(&[])
            .unwrap()
            .run(&mut conn, &schema, &MigrateOpts::default())
            .await
            .unwrap();
        assert_eq!(r.applied.len(), 1);
        // Re-running kept the existing internal keys.
        let store = Store::new(&schema);
        assert_eq!(store.next_db_id(&mut conn).await.unwrap(), DbId(5));
    })
    .await;
}

/// Chapter 10, "Cutover" (found in R9's live move): a migration run
/// against a catalog whose tables a rename fence moved away must not
/// recreate them empty; it waits (39070) until the catalog is unfenced.
#[tokio::test]
async fn live_a_fenced_catalog_is_not_migrated() {
    with_schema("live_fenced_catalog", |pool, schema| async move {
        migrate(&pool, &schema).await;
        let mut conn = pool.get_conn().await.unwrap();
        let tables: Vec<String> = conn
            .exec(
                "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? \
                 ORDER BY TABLE_NAME",
                (&schema,),
            )
            .await
            .unwrap();
        assert!(tables.iter().any(|t| t == "databases"), "{tables:?}");
        // The rename fence's shape: `__sprig_f_<16 hex>` per table.
        let pairs: Vec<(String, String)> = tables
            .iter()
            .enumerate()
            .map(|(i, t)| (t.clone(), format!("__sprig_f_{:016x}", 0xfe00 + i)))
            .collect();
        let rename = |back: bool| {
            let parts: Vec<String> = pairs
                .iter()
                .map(|(a, b)| {
                    let (x, y) = if back { (b, a) } else { (a, b) };
                    format!("`{schema}`.`{x}` TO `{schema}`.`{y}`")
                })
                .collect();
            format!("RENAME TABLE {}", parts.join(", "))
        };
        conn.query_drop(rename(false)).await.unwrap();
        assert_eq!(
            migrate::fenced_tables(&mut conn, &schema)
                .await
                .unwrap()
                .len(),
            pairs.len()
        );
        let e = Migrator::new(&[])
            .unwrap()
            .run(&mut conn, &schema, &MigrateOpts::default())
            .await
            .unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::CatalogUnavailable), "{e}");
        assert!(e.retryable, "{e}");
        assert!(e.to_string().contains("fenced"), "{e}");
        let now: Vec<String> = conn
            .exec(
                "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? \
                 AND TABLE_NAME = 'databases'",
                (&schema,),
            )
            .await
            .unwrap();
        assert!(now.is_empty(), "nothing recreated: {now:?}");
        // Unfenced: the catalog is as it was and needs nothing.
        conn.query_drop(rename(true)).await.unwrap();
        let r = Migrator::new(&[])
            .unwrap()
            .run(&mut conn, &schema, &MigrateOpts::default())
            .await
            .unwrap();
        assert!(r.applied.is_empty(), "{:?}", r.applied);
    })
    .await;
}

// ---- store ----

#[tokio::test]
async fn live_live_names_allow_many_tombstones() {
    // Chapter 08, open question 4: a unique key on a PERSISTENT generated
    // column that is NULL for many rows.
    with_schema("live_names", |pool, schema| async move {
        migrate(&pool, &schema).await;
        let s = Store::new(&schema);
        let mut c = pool.get_conn().await.unwrap();
        for id in 1..=3 {
            s.insert_object(&mut c, &object(id, "Orders", 1))
                .await
                .unwrap();
            if id < 3 {
                assert!(
                    s.set_object_state(&mut c, ObjectId(id), ObjectState::Dropped, 2)
                        .await
                        .unwrap()
                );
            }
        }
        let e = s
            .insert_object(&mut c, &object(4, "orders", 3))
            .await
            .unwrap_err();
        assert!(is_dup(&e), "{e:?}");
        // Another namespace or database may reuse the name.
        let mut f = object(5, "orders", 3);
        f.kind = ObjectKind::Function;
        f.namespace = Namespace::Function;
        s.insert_object(&mut c, &f).await.unwrap();
        let mut other_db = object(6, "orders", 3);
        other_db.db_id = DbId(2);
        s.insert_object(&mut c, &other_db).await.unwrap();
        assert_eq!(
            s.object_by_name(&mut c, DbId(1), Namespace::Relation, "orders")
                .await
                .unwrap()
                .unwrap()
                .object_id,
            ObjectId(3)
        );
        for id in 1..=3 {
            s.insert_database(&mut c, &database(id, "Shop", 1))
                .await
                .unwrap();
            if id < 3 {
                s.set_database_state(&mut c, DbId(id), DbState::Dropped, 2)
                    .await
                    .unwrap();
            }
        }
        let e = s
            .insert_database(&mut c, &database(4, "shop", 1))
            .await
            .unwrap_err();
        assert!(is_dup(&e));
        // Names compare exactly (utf8mb4_bin): "SHOP" is another key.
        let mut upper = database(5, "SHOP", 1);
        upper.name_key = "SHOP".to_owned();
        s.insert_database(&mut c, &upper).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn live_store_round_trips_counters_tombstones_and_snapshots() {
    with_schema("live_store", |pool, schema| async move {
        migrate(&pool, &schema).await;
        let s = Store::new(&schema);
        let mut c = pool.get_conn().await.unwrap();

        // Counters: never reused, ranges reserved atomically.
        assert_eq!(s.next_db_id(&mut c).await.unwrap(), DbId(1));
        assert_eq!(s.next_db_id(&mut c).await.unwrap(), DbId(2));
        assert_eq!(s.next_ids(&mut c, Counter::Object, 10).await.unwrap(), 1);
        assert_eq!(s.next_object_id(&mut c).await.unwrap(), ObjectId(11));
        let e = s
            .next_ids(&mut c, Counter::Other("user"), 1)
            .await
            .unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::TenantNotReady));

        // databases
        let db = database(1, "Shop", 1);
        s.insert_database(&mut c, &db).await.unwrap();
        assert_eq!(s.database(&mut c, DbId(1)).await.unwrap().unwrap(), db);
        assert_eq!(
            s.database_by_name(&mut c, "shop").await.unwrap().unwrap(),
            db
        );
        let mut db2 = db.clone();
        db2.options = json!({"comment": "x"});
        db2.changed_version = 2;
        assert!(s.update_database(&mut c, &db2).await.unwrap());
        assert!(
            s.update_database(&mut c, &db2).await.unwrap(),
            "unchanged still exists"
        );
        assert!(
            !s.update_database(&mut c, &database(9, "none", 2))
                .await
                .unwrap()
        );
        assert_eq!(s.database(&mut c, DbId(1)).await.unwrap().unwrap(), db2);

        // objects, with every optional column set
        let mut o = object(1, "Orders", 2);
        o.flags = ObjectFlags::GIPK_ADDED | ObjectFlags::RLS;
        o.pending_job_id = Some(JobId::new_v7());
        o.parent_id = Some(ObjectId(77));
        o.state = ObjectState::Creating;
        s.insert_object(&mut c, &o).await.unwrap();
        assert_eq!(s.object(&mut c, ObjectId(1)).await.unwrap().unwrap(), o);
        let mut bad = object(2, "f", 2);
        bad.kind = ObjectKind::LogicFunction;
        assert_eq!(
            s.insert_object(&mut c, &bad).await.unwrap_err().code(),
            Some(SprigCode::Internal)
        );
        o.name = "orders_v2".to_owned();
        o.name_key = "orders_v2".to_owned();
        o.pending_job_id = None;
        o.state = ObjectState::Active;
        o.changed_version = 3;
        assert!(s.update_object(&mut c, &o).await.unwrap());
        assert_eq!(
            s.object_by_name(&mut c, DbId(1), Namespace::Relation, "orders_v2")
                .await
                .unwrap()
                .unwrap(),
            o
        );

        // settings: values, bounds, resets leave tombstones
        let mut row = setting("auto_index.max_per_table", json!(6), 3);
        row.bounds = Some(Bounds::max(5.0));
        s.put_setting(&mut c, &row).await.unwrap();
        let got = s
            .setting(&mut c, SettingScope::Tenant, &row.key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got, row);
        assert_eq!(
            got.entry().unwrap().value,
            Some(sprig_settings::Value::Int(6))
        );
        let obj_row = SettingRow {
            scope: SettingScope::Object(ObjectId(1)),
            ..setting("auto_index.mode", json!("recommend"), 3)
        };
        s.put_setting(&mut c, &obj_row).await.unwrap();
        assert!(
            s.reset_setting(
                &mut c,
                SettingScope::Tenant,
                &row.key,
                true,
                false,
                "user:7",
                at(9),
                4
            )
            .await
            .unwrap()
        );
        let half = s
            .setting(&mut c, SettingScope::Tenant, &row.key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (half.value, half.bounds.is_some(), half.version),
            (None, true, 4)
        );
        s.reset_setting(
            &mut c,
            SettingScope::Tenant,
            &row.key,
            false,
            true,
            "user:7",
            at(9),
            5,
        )
        .await
        .unwrap();
        let empty = s
            .setting(&mut c, SettingScope::Tenant, &row.key)
            .await
            .unwrap()
            .unwrap();
        assert!(empty.is_empty());
        assert!(
            !s.reset_setting(
                &mut c,
                SettingScope::Tenant,
                "nope",
                true,
                true,
                "user:7",
                at(9),
                5
            )
            .await
            .unwrap()
        );
        let since: Vec<String> = s
            .settings_since(&mut c, Some(4))
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.key)
            .collect();
        assert_eq!(since, ["auto_index.max_per_table"]);
        assert_eq!(
            s.settings_of(&mut c, SettingScope::Object(ObjectId(1)))
                .await
                .unwrap()
                .len(),
            1
        );
        // Internal keys.
        s.put_internal(
            &mut c,
            "sprig.ancestry",
            &json!([{"id": 1, "name": "root"}]),
            0,
            at(1),
        )
        .await
        .unwrap();
        assert_eq!(
            s.internal(&mut c, "sprig.ancestry").await.unwrap(),
            Some(json!([{"id": 1, "name": "root"}]))
        );
        assert!(
            s.put_internal(&mut c, "auto_index.mode", &json!(1), 0, at(1))
                .await
                .is_err()
        );

        // schema_versions
        for v in 1..=2 {
            s.insert_schema_version(
                &mut c,
                &SchemaVersionRow {
                    object_id: ObjectId(1),
                    version: v,
                    ddl: format!("CREATE TABLE orders (id INT) -- {v}"),
                    create_sql: (v == 2).then(|| "CREATE TABLE `orders` (`id` int)".to_owned()),
                    create_sql_hash: (v == 2).then_some([7_u8; 32]),
                    ddl_job_id: Some(JobId::new_v7()),
                    applied_by: Some(UserId(7)),
                    applied_at: at(10 + v),
                    changed_version: v + 1,
                },
            )
            .await
            .unwrap();
        }
        assert_eq!(
            s.schema_versions(&mut c, ObjectId(1)).await.unwrap().len(),
            2
        );
        let latest = s
            .latest_schema_version(&mut c, ObjectId(1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!((latest.version, latest.create_sql_hash), (2, Some([7; 32])));

        // Snapshots: incremental by version.
        let snap = s.snapshot(&pool, Some(2)).await.unwrap();
        assert_eq!(snap.since, Some(2));
        assert!(snap.databases.is_empty(), "db changed at 2 is not after 2");
        assert_eq!(snap.objects.len(), 1);
        let full = s.snapshot(&pool, None).await.unwrap();
        assert_eq!(full.databases.len(), 1);
        assert!(full.settings.iter().any(|r| r.key == "sprig.next_db_id"));
    })
    .await;
}

// ---- change() ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_changes_are_monotonic_atomic_and_survive_crashes() {
    with_schema("live_change", |pool, schema| async move {
        migrate(&pool, &schema).await;
        with_global(|g| async move {
            let tenant = unique_tenant();
            seed_directory(&*g, tenant).await;
            let cat = catalog_for(&pool, &schema, tenant, Arc::clone(&g));

            // One change: a database and a tenant setting.
            let mut c = cat
                .change_with(
                    tenant,
                    VersionKind::Catalog,
                    ChangeOpts {
                        tenant_settings: true,
                    },
                )
                .await
                .unwrap();
            let v = c.version();
            assert_eq!(v, 1);
            {
                let (s, q) = c.tx().parts();
                s.insert_database(q, &database(1, "shop", v)).await.unwrap();
                s.put_setting(q, &setting("auto_index.mode", json!("on"), v))
                    .await
                    .unwrap();
            }
            assert_eq!(c.commit().await.unwrap(), 1);
            let d = directory_versions(&*g, tenant).await;
            assert_eq!((d.catalog_version, d.settings_version), (1, 1));

            // Concurrent writers, two rows per change, and a reader that must
            // never see half a change or a version going backwards.
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let reader = {
                let cat = cat.clone();
                let stop = Arc::clone(&stop);
                tokio::spawn(async move {
                    let mut last = 0;
                    let mut reads = 0;
                    while !stop.load(Ordering::Relaxed) {
                        let snap = cat.snapshot(tenant, None).await.unwrap();
                        assert!(snap.versions.catalog >= last);
                        last = snap.versions.catalog;
                        let mut per: BTreeMap<u64, usize> = BTreeMap::new();
                        for r in snap.settings.iter().filter(|r| r.key.starts_with("test.")) {
                            assert!(r.version <= snap.versions.catalog);
                            *per.entry(r.version).or_default() += 1;
                        }
                        assert!(per.values().all(|&n| n == 2), "half a change: {per:?}");
                        reads += 1;
                    }
                    reads
                })
            };
            let mut writers = Vec::new();
            for w in 0_u64..8 {
                let cat = cat.clone();
                writers.push(tokio::spawn(async move {
                    let mut mine = Vec::new();
                    for i in 0..5 {
                        let mut c = cat.change(tenant, VersionKind::Catalog).await.unwrap();
                        let v = c.version();
                        {
                            let (s, q) = c.tx().parts();
                            s.put_setting(q, &setting(&format!("test.w{w}.{i}.a"), json!(v), v))
                                .await
                                .unwrap();
                            tokio::time::sleep(Duration::from_millis(w % 3)).await;
                            s.put_setting(q, &setting(&format!("test.w{w}.{i}.b"), json!(v), v))
                                .await
                                .unwrap();
                        }
                        mine.push(c.commit().await.unwrap());
                    }
                    mine
                }));
            }
            let mut all = Vec::new();
            for w in writers {
                let mine = w.await.unwrap();
                assert!(mine.windows(2).all(|p| p[0] < p[1]));
                all.extend(mine);
            }
            stop.store(true, Ordering::Relaxed);
            assert!(reader.await.unwrap() > 0);
            all.sort_unstable();
            assert_eq!(all, (2..=41).collect::<Vec<u64>>(), "no gaps, no reuse");
            let snap = cat.snapshot(tenant, Some(1)).await.unwrap();
            assert_eq!(snap.versions.catalog, 41);
            assert_eq!(directory_versions(&*g, tenant).await.catalog_version, 41);

            // Crash after allocation: a gap, nothing visible, the lock freed.
            let crashing = cat.clone().with_failpoint(Arc::new(|_, _, s| {
                if s == Step::Allocated {
                    Err(SprigError::internal("injected crash"))
                } else {
                    Ok(())
                }
            }));
            assert!(crashing.change(tenant, VersionKind::Catalog).await.is_err());
            // Crash after the rows are written, before COMMIT.
            let crashing = cat.clone().with_failpoint(Arc::new(|_, _, s| {
                if s == Step::BeforeCommit {
                    Err(SprigError::internal("injected crash"))
                } else {
                    Ok(())
                }
            }));
            let mut c = crashing.change(tenant, VersionKind::Catalog).await.unwrap();
            assert_eq!(c.version(), 43);
            {
                let (s, q) = c.tx().parts();
                s.put_setting(q, &setting("test.half", json!(43), 43))
                    .await
                    .unwrap();
            }
            assert!(c.commit().await.is_err());
            let snap = cat.snapshot(tenant, Some(41)).await.unwrap();
            assert_eq!(snap.versions.catalog, 41);
            assert!(snap.settings.is_empty(), "{:?}", snap.settings);
            assert_eq!(directory_versions(&*g, tenant).await.catalog_version, 43);
            // Settle closes the gap at once.
            assert_eq!(cat.settle(tenant, VersionKind::Catalog).await.unwrap(), 43);
            assert_eq!(
                cat.snapshot(tenant, Some(41))
                    .await
                    .unwrap()
                    .versions
                    .catalog,
                43
            );
            // A crash after COMMIT: committed, though the caller saw an error.
            let crashing = cat.clone().with_failpoint(Arc::new(|_, _, s| {
                if s == Step::Committed {
                    Err(SprigError::internal("reply lost"))
                } else {
                    Ok(())
                }
            }));
            let mut c = crashing.change(tenant, VersionKind::Auth).await.unwrap();
            {
                let (s, q) = c.tx().parts();
                s.put_setting(q, &setting("test.auth", json!(1), 1))
                    .await
                    .unwrap();
            }
            assert!(c.commit().await.is_err());
            let snap = cat.snapshot(tenant, Some(43)).await.unwrap();
            assert_eq!(snap.versions.auth, 1);

            // A held version row: a second change gives up after the lock wait
            // with 39071; auth changes don't wait for catalog changes.
            let short = {
                let locator = StaticLocator::new()
                    .with(
                        tenant,
                        CatalogEndpoint {
                            pool: pool.clone(),
                            store: Store::new(&schema),
                        },
                    )
                    .shared();
                Versioner::new(
                    MariaBackend::new(locator).with_opts(MariaOpts {
                        lock_wait: Duration::from_secs(1),
                        statement_timeout: Duration::from_secs(5),
                    }),
                    Arc::new(DirectoryAllocator::new(Arc::clone(&g))),
                )
            };
            let held = cat.change(tenant, VersionKind::Catalog).await.unwrap();
            let e = short
                .change(tenant, VersionKind::Catalog)
                .await
                .unwrap_err();
            assert_eq!(e.code(), Some(SprigCode::CatalogConflict), "{e:?}");
            assert!(e.retryable);
            let a = short.change(tenant, VersionKind::Auth).await.unwrap();
            assert_eq!(a.commit().await.unwrap(), 2);
            held.rollback().await.unwrap();

            // Purge: tombstones and empty rows go; caches older than the purge
            // reload in full.
            let mut c = cat.change(tenant, VersionKind::Catalog).await.unwrap();
            let v = c.version();
            {
                let (s, q) = c.tx().parts();
                s.set_database_state(q, DbId(1), DbState::Dropped, v)
                    .await
                    .unwrap();
                s.reset_setting(
                    q,
                    SettingScope::Tenant,
                    "auto_index.mode",
                    true,
                    true,
                    "user:7",
                    at(5),
                    v,
                )
                .await
                .unwrap();
            }
            c.commit().await.unwrap();
            let r = cat.purge(tenant, u64::MAX).await.unwrap();
            assert_eq!((r.databases, r.settings), (1, 1));
            let snap = cat.snapshot(tenant, Some(3)).await.unwrap();
            assert_eq!(
                snap.since, None,
                "older than sprig.purged_version: a full load"
            );
            assert_eq!(snap.versions.purged, v);
            assert!(snap.databases.is_empty());
            assert_eq!(cat.snapshot(tenant, Some(v)).await.unwrap().since, Some(v));
        })
        .await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_a_deposed_leader_cannot_allocate_or_commit() {
    with_schema("live_leader", |pool, schema| async move {
        migrate(&pool, &schema).await;
        with_global(|g| async move {
            let tenant = unique_tenant();
            seed_directory(&*g, tenant).await;
            let election = Election::new(
                Arc::clone(&g),
                Arc::new(sprig_common::time::SystemClock),
                SessionOpts::new(Duration::from_secs(5)),
            );
            let key = g.tier().key("leader").unwrap();
            let me = LeaderRecord {
                holder: "ctl-1".to_owned(),
                dc: None,
                build: "test".to_owned(),
            };
            let leadership = election.campaign(&key, &me).await.unwrap();
            let locator = StaticLocator::new()
                .with(
                    tenant,
                    CatalogEndpoint {
                        pool: pool.clone(),
                        store: Store::new(&schema),
                    },
                )
                .shared();
            let cat = Versioner::new(
                MariaBackend::new(locator),
                Arc::new(DirectoryAllocator::leader(leadership.clone())),
            );
            assert_eq!(
                cat.change(tenant, VersionKind::Catalog)
                    .await
                    .unwrap()
                    .commit()
                    .await
                    .unwrap(),
                1
            );
            let mut open = cat.change(tenant, VersionKind::Catalog).await.unwrap();
            {
                let (s, q) = open.tx().parts();
                s.put_setting(q, &setting("test.deposed", json!(2), 2))
                    .await
                    .unwrap();
            }
            // Another controller takes over.
            g.txn(Txn {
                then: vec![Op::Delete(key.clone())],
                ..Txn::default()
            })
            .await
            .unwrap();
            let other = Election::new(
                Arc::clone(&g),
                Arc::new(sprig_common::time::SystemClock),
                SessionOpts::new(Duration::from_secs(5)),
            );
            let new_leader = other
                .campaign(
                    &key,
                    &LeaderRecord {
                        holder: "ctl-2".to_owned(),
                        ..me.clone()
                    },
                )
                .await
                .unwrap();
            let e = cat.change(tenant, VersionKind::Auth).await.unwrap_err();
            assert_eq!(e.code(), Some(SprigCode::NotLeader));
            let e = open.commit().await.unwrap_err();
            assert_eq!(e.code(), Some(SprigCode::NotLeader));
            let snap = cat.snapshot(tenant, Some(1)).await.unwrap();
            assert_eq!(snap.versions.catalog, 1);
            assert!(snap.settings.is_empty());
            let d = directory_versions(&*g, tenant).await;
            assert_eq!((d.catalog_version, d.auth_version), (2, 0));
            new_leader.resign().await.unwrap();
            let _ = leadership.resign().await;
        })
        .await;
    })
    .await;
}
