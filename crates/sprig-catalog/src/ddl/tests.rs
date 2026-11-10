//! Unit tests of the DDL path over the in-memory catalog (`MemSource`), a
//! `MemStore` directory and the in-memory executor: every statement kind,
//! name keys, `IF [NOT] EXISTS`, refused statements, conflicts, and a
//! crash at every window followed by the reconciler's repair.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use sprig_common::ids::{DbId, GroupId, ShardId, TenantId, UserId};
use sprig_common::names::SchemaName;
use sprig_common::time::SystemClock;
use sprig_common::{ErrorKind, SprigCode, SprigError};
use sprig_meta::{MemStore, MetaStore, Op, Tier, Txn};

use super::mem::{Fault, MemExecutor};
use super::*;
use crate::cache::mem::{MemSource, MemTables};
use crate::store::ObjectState;
use crate::version::{DirectoryAllocator, Failpoint, Step};

const T: TenantId = TenantId(42);

async fn seed(store: &dyn MetaStore, tenant: TenantId) {
    let key = Tier::Global.key(&format!("tenants/{tenant}")).unwrap();
    let body = json!({"v": 1, "tenant_id": tenant.0, "name": "acme", "state": "active",
                      "home_dc": "mum1", "catalog_version": 0, "auth_version": 0});
    let r = store
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
    assert!(r.succeeded);
}

struct Env {
    v: Versioner<MemSource>,
    exec: MemExecutor,
}

impl Env {
    async fn new() -> Self {
        let g = Arc::new(MemStore::new(Tier::Global));
        seed(&*g, T).await;
        let mem = MemSource::new();
        mem.bootstrap(T);
        let v = Versioner::new(mem, Arc::new(DirectoryAllocator::new(g)));
        Self {
            v,
            exec: MemExecutor::new(),
        }
    }

    fn engine(&self) -> DdlEngine<MemSource> {
        DdlEngine::new(self.v.clone(), Arc::new(SystemClock)).with_opts(DdlOpts {
            ddl_wait: Duration::from_millis(200),
            poll: Duration::from_millis(10),
            ..DdlOpts::default()
        })
    }

    fn crashing(&self, step: DdlStep) -> DdlEngine<MemSource> {
        let f: DdlFailpoint = Arc::new(move |_, s| {
            if s == step {
                Err(SprigError::internal(format!("injected crash at {s:?}")))
            } else {
                Ok(())
            }
        });
        self.engine().with_failpoint(f)
    }

    fn crashing_commit(&self) -> DdlEngine<MemSource> {
        let f: Failpoint = Arc::new(|_, _, s| {
            if s == Step::BeforeCommit {
                Err(SprigError::internal(
                    "injected crash before the versioned commit",
                ))
            } else {
                Ok(())
            }
        });
        DdlEngine::new(self.v.clone().with_failpoint(f), Arc::new(SystemClock))
    }

    fn tables(&self) -> MemTables {
        self.v.backend().tables(T).unwrap()
    }

    async fn run(&self, stmt: DdlStatement, sql: &str) -> Result<DdlOutcome> {
        self.engine().execute(&req(stmt, sql), &self.exec).await
    }

    /// Repairs every intent left behind, as the reconciler would.
    async fn repair(&self) -> Vec<Resolution> {
        let snap = self.v.backend().full_snapshot(T).await.unwrap();
        let mut out = Vec::new();
        for i in DdlIntent::all(&snap.settings) {
            out.push(
                self.engine()
                    .resolve(T, &i, &snap, &self.exec)
                    .await
                    .unwrap(),
            );
        }
        out
    }

    fn intents(&self) -> usize {
        DdlIntent::all(&self.tables().settings.into_values().collect::<Vec<_>>()).len()
    }

    fn object(&self, name: &str) -> Option<ObjectRow> {
        self.tables()
            .objects
            .into_values()
            .find(|o| o.name == name && o.state != ObjectState::Dropped)
    }
}

fn req(statement: DdlStatement, sql: &str) -> DdlRequest<'_> {
    DdlRequest {
        tenant: T,
        statement,
        sql,
        logical_sql: sql,
        current_db: Some("billing"),
        user: Some(UserId(7)),
        lower_case_table_names: false,
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

fn create(kind: ObjectKind, name: &str) -> DdlStatement {
    DdlStatement::CreateObject {
        kind,
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

fn drop_tables(names: &[&str], if_exists: bool) -> DdlStatement {
    DdlStatement::DropObjects {
        kind: ObjectKind::Table,
        names: names.iter().map(|n| ObjectName::bare(n)).collect(),
        if_exists,
    }
}

fn rename(pairs: &[(&str, &str)]) -> DdlStatement {
    DdlStatement::RenameTables {
        pairs: pairs
            .iter()
            .map(|(a, b)| (ObjectName::bare(a), ObjectName::bare(b)))
            .collect(),
        if_exists: false,
    }
}

fn shard(db: u32, group: u32, s: u32) -> SchemaName {
    SchemaName::shard(T, DbId(db), GroupId(group), ShardId(s))
}

fn errno_of(e: &SprigError) -> u16 {
    errno(e).unwrap()
}

async fn billing_with_orders() -> Env {
    let env = Env::new().await;
    env.run(create_db("billing"), "CREATE DATABASE billing")
        .await
        .unwrap();
    env.run(
        create(ObjectKind::Table, "orders"),
        "CREATE TABLE orders (id INT)",
    )
    .await
    .unwrap();
    env
}

#[tokio::test]
async fn the_whole_life_of_a_database_and_a_table() {
    let env = Env::new().await;
    let o = env
        .run(create_db("billing"), "CREATE DATABASE billing")
        .await
        .unwrap();
    assert_eq!(o.affected_rows, 1);
    let v1 = o.version.unwrap();
    let s = shard(1, 1, 1);
    assert_eq!(s.to_string(), "t42_d1_g1_s1");
    assert!(env.exec.has_schema(&s));
    let t = env.tables();
    let db = &t.databases[&DbId(1)];
    assert_eq!(db.state, DbState::Active);
    assert_eq!(db.default_group_id, GroupId(1));
    assert_eq!(shard_schemas(T, db), vec![s]);
    assert_eq!(db.default_charset, "utf8mb4");
    assert_eq!(db.changed_version, v1);
    assert_eq!(db.created_by, Some(UserId(7)));

    // A table: creating, then active at schema version 1.
    let o = env
        .run(
            create(ObjectKind::Table, "orders"),
            "CREATE TABLE orders (id INT)",
        )
        .await
        .unwrap();
    let v2 = o.version.unwrap();
    assert!(v2 > v1);
    let orders = env.object("orders").unwrap();
    assert_eq!(orders.state, ObjectState::Active);
    assert_eq!(orders.schema_version, 1);
    assert_eq!(orders.pending_job_id, None);
    assert_eq!(orders.changed_version, v2);
    let sv = &env.tables().schema_versions[&(orders.object_id, 1)];
    assert_eq!(sv.ddl, "CREATE TABLE orders (id INT)");
    assert_eq!(
        sv.create_sql.as_deref(),
        Some("CREATE TABLE `orders` /* rev 1 */")
    );
    assert!(sv.create_sql_hash.is_some());
    assert_eq!(sv.ddl_job_id, o.job);
    assert_eq!(sv.applied_by, Some(UserId(7)));
    assert_eq!(env.exec.names(&s, ObjectKind::Table), ["orders"]);
    assert!(
        env.exec.has_object(&s, ObjectKind::Table, "__sprig_dtx"),
        "CREATE DATABASE made chapter 18's decision table"
    );
    assert!(
        env.object("__sprig_dtx").is_none(),
        "internal: no catalog row"
    );

    // ALTER: schema version 2.
    let v3 = env
        .run(alter("orders"), "ALTER TABLE orders ADD c INT")
        .await
        .unwrap()
        .version
        .unwrap();
    let orders = env.object("orders").unwrap();
    assert_eq!(orders.schema_version, 2);
    assert_eq!(orders.changed_version, v3);
    assert_eq!(
        env.tables().schema_versions[&(orders.object_id, 2)]
            .create_sql
            .as_deref(),
        Some("CREATE TABLE `orders` /* rev 2 */")
    );

    // RENAME: same id, new name, version 3.
    env.run(
        rename(&[("orders", "orders_v2")]),
        "RENAME TABLE orders TO orders_v2",
    )
    .await
    .unwrap();
    assert!(env.object("orders").is_none());
    let renamed = env.object("orders_v2").unwrap();
    assert_eq!(renamed.object_id, orders.object_id);
    assert_eq!(renamed.schema_version, 3);
    assert_eq!(env.exec.names(&s, ObjectKind::Table), ["orders_v2"]);

    // DROP: a tombstone that frees the name.
    let v5 = env
        .run(drop_tables(&["orders_v2"], false), "DROP TABLE orders_v2")
        .await
        .unwrap()
        .version
        .unwrap();
    let t = env.tables();
    let tomb = &t.objects[&orders.object_id];
    assert_eq!(tomb.state, ObjectState::Dropped);
    assert_eq!(tomb.changed_version, v5);
    assert!(env.exec.names(&s, ObjectKind::Table).is_empty());
    env.run(
        create(ObjectKind::Table, "orders_v2"),
        "CREATE TABLE orders_v2 (id INT)",
    )
    .await
    .unwrap();
    assert_ne!(env.object("orders_v2").unwrap().object_id, orders.object_id);

    // DROP DATABASE: tombstones for the database and its objects.
    let o = env
        .run(
            DdlStatement::DropDatabase {
                name: "billing".to_owned(),
                if_exists: false,
            },
            "DROP DATABASE billing",
        )
        .await
        .unwrap();
    assert_eq!(o.affected_rows, 1);
    let t = env.tables();
    assert_eq!(t.databases[&DbId(1)].state, DbState::Dropped);
    assert!(t.objects.values().all(|o| o.state == ObjectState::Dropped));
    assert!(!env.exec.has_schema(&s));
    // The name is free; ids are never reused.
    env.run(create_db("billing"), "CREATE DATABASE billing")
        .await
        .unwrap();
    assert!(env.exec.has_schema(&shard(2, 2, 2)));
    assert_eq!(env.intents(), 0);
    // The catalog version only moved on the versioned changes.
    assert_eq!(env.tables().versions.catalog, o.version.unwrap() + 1);
}

#[tokio::test]
async fn name_keys_follow_lower_case_table_names() {
    assert_eq!(database_key("Billing", true), "billing");
    assert_eq!(database_key("Billing", false), "Billing");
    assert_eq!(object_key(ObjectKind::Table, "Orders", true), "orders");
    assert_eq!(object_key(ObjectKind::Table, "Orders", false), "Orders");
    assert_eq!(object_key(ObjectKind::Procedure, "DoIt", false), "doit");
    assert_eq!(object_key(ObjectKind::Event, "Nightly", false), "nightly");
    assert_eq!(object_key(ObjectKind::Trigger, "Trg", true), "Trg");

    let env = Env::new().await;
    let mut r = req(create_db("Billing"), "CREATE DATABASE Billing");
    r.lower_case_table_names = true;
    env.engine().execute(&r, &env.exec).await.unwrap();
    let db = env.tables().databases[&DbId(1)].clone();
    assert_eq!(
        (db.name.as_str(), db.name_key.as_str()),
        ("Billing", "billing")
    );
    let mut r = req(create_db("BILLING"), "CREATE DATABASE BILLING");
    r.lower_case_table_names = true;
    let e = env.engine().execute(&r, &env.exec).await.unwrap_err();
    assert_eq!(errno_of(&e), 1007);
    // Case-sensitive tenants keep both.
    env.run(create_db("BILLING"), "CREATE DATABASE BILLING")
        .await
        .unwrap();

    // Routines compare in lower case whatever the setting.
    let mut r = req(
        create(ObjectKind::Procedure, "DoIt"),
        "CREATE PROCEDURE DoIt() BEGIN END",
    );
    r.current_db = Some("BILLING");
    env.engine().execute(&r, &env.exec).await.unwrap();
    let mut r = req(
        create(ObjectKind::Procedure, "DOIT"),
        "CREATE PROCEDURE DOIT() BEGIN END",
    );
    r.current_db = Some("BILLING");
    let e = env.engine().execute(&r, &env.exec).await.unwrap_err();
    assert_eq!(errno_of(&e), 1304);
}

#[tokio::test]
async fn existence_errors_and_notes_read_like_mariadb() {
    let env = billing_with_orders().await;
    let before = env.tables().versions.catalog;

    let e = env
        .run(create_db("billing"), "CREATE DATABASE billing")
        .await
        .unwrap_err();
    assert_eq!(
        (errno_of(&e), e.message.as_str()),
        (1007, "Can't create database 'billing'; database exists")
    );
    let o = env
        .run(
            DdlStatement::CreateDatabase {
                name: "billing".to_owned(),
                if_not_exists: true,
                charset: None,
                collation: None,
            },
            "CREATE DATABASE IF NOT EXISTS billing",
        )
        .await
        .unwrap();
    assert_eq!(o.notes[0].code, 1007);
    assert_eq!(o.version, None);
    let e = env
        .run(
            DdlStatement::DropDatabase {
                name: "nope".to_owned(),
                if_exists: false,
            },
            "DROP DATABASE nope",
        )
        .await
        .unwrap_err();
    assert_eq!(errno_of(&e), 1008);

    let e = env
        .run(
            create(ObjectKind::Table, "orders"),
            "CREATE TABLE orders (id INT)",
        )
        .await
        .unwrap_err();
    assert_eq!(
        (errno_of(&e), e.message.as_str()),
        (1050, "Table 'orders' already exists")
    );
    let o = env
        .run(
            DdlStatement::CreateObject {
                kind: ObjectKind::Table,
                name: ObjectName::bare("orders"),
                if_not_exists: true,
                or_replace: false,
                parent: None,
            },
            "CREATE TABLE IF NOT EXISTS orders (id INT)",
        )
        .await
        .unwrap();
    assert_eq!(o.notes[0].code, 1050);

    let e = env
        .run(drop_tables(&["nope"], false), "DROP TABLE nope")
        .await
        .unwrap_err();
    assert_eq!(
        (errno_of(&e), e.message.as_str()),
        (1051, "Unknown table 'billing.nope'")
    );
    let o = env
        .run(drop_tables(&["nope"], true), "DROP TABLE IF EXISTS nope")
        .await
        .unwrap();
    assert_eq!(o.notes[0].code, 1051);
    let e = env
        .run(alter("nope"), "ALTER TABLE nope ADD c INT")
        .await
        .unwrap_err();
    assert_eq!(
        (errno_of(&e), e.message.as_str()),
        (1146, "Table 'billing.nope' doesn't exist")
    );
    let e = env
        .run(rename(&[("nope", "x")]), "RENAME TABLE nope TO x")
        .await
        .unwrap_err();
    assert_eq!(errno_of(&e), 1146);
    let e = env
        .run(
            DdlStatement::CreateObject {
                kind: ObjectKind::Table,
                name: ObjectName::qualified("crm", "t"),
                if_not_exists: false,
                or_replace: false,
                parent: None,
            },
            "CREATE TABLE crm.t (id INT)",
        )
        .await
        .unwrap_err();
    assert_eq!(
        (errno_of(&e), e.message.as_str()),
        (1049, "Unknown database 'crm'")
    );
    let mut r = req(create(ObjectKind::Table, "t"), "CREATE TABLE t (id INT)");
    r.current_db = None;
    let e = env.engine().execute(&r, &env.exec).await.unwrap_err();
    assert_eq!(errno_of(&e), 1046);

    // Reserved names.
    let e = env
        .run(create_db("t42_x"), "CREATE DATABASE t42_x")
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ReservedName));
    let e = env
        .run(create_db("mysql"), "CREATE DATABASE mysql")
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ReservedName));
    let e = env
        .run(
            create(ObjectKind::Table, "__sprig_x"),
            "CREATE TABLE __sprig_x (id INT)",
        )
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ReservedName));

    // None of it changed the catalog.
    assert_eq!(env.tables().versions.catalog, before);
    assert_eq!(env.intents(), 0);
}

#[tokio::test]
async fn a_refused_statement_rolls_back_without_a_version() {
    let env = billing_with_orders().await;
    let before = env.tables();

    // ALTER refused by MariaDB: the mark is cleared, nothing versioned.
    let e = env
        .run(alter("orders"), "ALTER TABLE orders BROKEN")
        .await
        .unwrap_err();
    assert_eq!(errno_of(&e), 1064);
    assert!(matches!(e.kind, ErrorKind::Backend { .. }));
    let after = env.tables();
    assert_eq!(after.versions.catalog, before.versions.catalog);
    assert_eq!(env.object("orders").unwrap().pending_job_id, None);
    assert_eq!(env.intents(), 0);

    // CREATE refused: the creating row is gone; its message names the
    // logical database.
    env.exec.inject(Fault::Refuse(SprigError::backend(
        1005,
        *b"HY000",
        "Can't create table `t42_d1_g1_s1`.`items` (errno: 150)",
    )));
    let e = env
        .run(
            create(ObjectKind::Table, "items"),
            "CREATE TABLE items (id INT)",
        )
        .await
        .unwrap_err();
    assert_eq!(
        e.message,
        "Can't create table `billing`.`items` (errno: 150)"
    );
    assert!(env.object("items").is_none());
    assert_eq!(env.tables().objects.len(), before.objects.len());
    assert_eq!(env.tables().versions.catalog, before.versions.catalog);

    // CREATE DATABASE refused: the row is gone, the id is not reused.
    env.exec.inject(Fault::Refuse(SprigError::backend(
        1044,
        *b"42000",
        "Access denied",
    )));
    let e = env
        .run(create_db("crm"), "CREATE DATABASE crm")
        .await
        .unwrap_err();
    assert_eq!(errno_of(&e), 1044);
    assert!(env.tables().databases.values().all(|d| d.name != "crm"));
    env.run(create_db("crm"), "CREATE DATABASE crm")
        .await
        .unwrap();
    assert!(env.exec.has_schema(&shard(3, 3, 3)));
    assert_eq!(env.intents(), 0);

    // CREATE DATABASE whose schema is made but whose second statement (the
    // transactions' decision table) is refused: the new schema goes too
    // (r2close; r2kind's leftover left such schemas behind).
    env.exec.inject(Fault::Pass);
    env.exec.inject(Fault::Refuse(SprigError::backend(
        1142,
        *b"42000",
        "CREATE command denied",
    )));
    let e = env
        .run(create_db("sales"), "CREATE DATABASE sales")
        .await
        .unwrap_err();
    assert_eq!(errno_of(&e), 1142);
    assert!(env.tables().databases.values().all(|d| d.name != "sales"));
    let made = env
        .exec
        .log()
        .iter()
        .rev()
        .find(|st| matches!(st.effect, super::exec::Effect::CreateSchema))
        .map(|st| st.schema)
        .unwrap();
    assert!(
        !env.exec.has_schema(&made),
        "the refused database's new schema {made} is dropped"
    );
    assert!(
        env.exec
            .log()
            .last()
            .unwrap()
            .sql
            .starts_with("DROP DATABASE IF EXISTS")
    );
    assert_eq!(env.intents(), 0);
}

#[tokio::test]
async fn a_second_ddl_on_a_held_object_waits_then_fails_with_39071() {
    let env = billing_with_orders().await;
    // A crashed ALTER holds `orders`.
    let e = env
        .crashing(DdlStep::Prepared)
        .execute(
            &req(alter("orders"), "ALTER TABLE orders ADD c INT"),
            &env.exec,
        )
        .await
        .unwrap_err();
    assert!(e.message.contains("injected crash"));
    assert!(env.object("orders").unwrap().pending_job_id.is_some());
    for (stmt, sql) in [
        (alter("orders"), "ALTER TABLE orders ADD d INT"),
        (drop_tables(&["orders"], false), "DROP TABLE orders"),
        (rename(&[("orders", "o2")]), "RENAME TABLE orders TO o2"),
        (
            DdlStatement::Truncate {
                name: ObjectName::bare("orders"),
            },
            "TRUNCATE TABLE orders",
        ),
        (
            DdlStatement::DropDatabase {
                name: "billing".to_owned(),
                if_exists: false,
            },
            "DROP DATABASE billing",
        ),
    ] {
        let started = std::time::Instant::now();
        let e = env.run(stmt, sql).await.unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::CatalogConflict), "{sql}: {e}");
        assert!(e.retryable);
        assert!(
            started.elapsed() >= Duration::from_millis(150),
            "{sql} waited"
        );
    }
    // Other objects are free.
    env.run(
        create(ObjectKind::Table, "items"),
        "CREATE TABLE items (id INT)",
    )
    .await
    .unwrap();

    // A waiting DDL goes ahead once the holder finishes.
    let env2 = billing_with_orders().await;
    let held = env2
        .crashing(DdlStep::Applied)
        .execute(
            &req(alter("orders"), "ALTER TABLE orders ADD c INT"),
            &env2.exec,
        )
        .await;
    assert!(held.is_err());
    let waiter = {
        let engine = env2.engine().with_opts(DdlOpts {
            ddl_wait: Duration::from_secs(5),
            poll: Duration::from_millis(10),
            ..DdlOpts::default()
        });
        let exec = &env2.exec;
        async move {
            engine
                .execute(&req(alter("orders"), "ALTER TABLE orders ADD d INT"), exec)
                .await
        }
    };
    let repair = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        env2.repair().await
    };
    let (w, r) = tokio::join!(waiter, repair);
    assert_eq!(r[0].label(), "finished");
    w.unwrap();
    assert_eq!(env2.object("orders").unwrap().schema_version, 3);
}

#[tokio::test]
async fn concurrent_creates_of_one_name_admit_exactly_one() {
    let env = Env::new().await;
    env.run(create_db("billing"), "CREATE DATABASE billing")
        .await
        .unwrap();
    let mut tasks = Vec::new();
    let exec = &env.exec;
    for _ in 0..8 {
        let engine = env.engine();
        tasks.push(async move {
            engine
                .execute(
                    &req(
                        create(ObjectKind::Table, "orders"),
                        "CREATE TABLE orders (id INT)",
                    ),
                    exec,
                )
                .await
        });
    }
    let results = futures::future::join_all(tasks).await;
    let ok = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(ok, 1, "{results:?}");
    for r in results.iter().filter_map(|r| r.as_ref().err()) {
        assert!(
            errno(r) == Some(1050) || r.code() == Some(SprigCode::CatalogConflict),
            "{r}"
        );
    }
    let live: Vec<_> = env
        .tables()
        .objects
        .into_values()
        .filter(|o| o.name == "orders")
        .collect();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].state, ObjectState::Active);
}

/// One statement crashed at `step`, then repaired: what the catalog and
/// the cluster look like.
struct Crash {
    env: Env,
    resolutions: Vec<Resolution>,
}

async fn crash(
    setup: &[(DdlStatement, &str)],
    stmt: DdlStatement,
    sql: &str,
    step: Option<DdlStep>,
) -> Crash {
    let env = Env::new().await;
    for (s, q) in setup {
        env.run(s.clone(), q).await.unwrap();
    }
    let engine = match step {
        Some(s) => env.crashing(s),
        None => env.crashing_commit(),
    };
    assert!(engine.execute(&req(stmt, sql), &env.exec).await.is_err());
    let resolutions = env.repair().await;
    assert_eq!(env.intents(), 0, "every intent resolved");
    assert!(
        env.tables()
            .objects
            .values()
            .all(|o| o.pending_job_id.is_none() && o.state != ObjectState::Creating),
        "no marks left"
    );
    assert!(
        env.tables()
            .databases
            .values()
            .all(|d| d.state != DbState::Creating)
    );
    Crash { env, resolutions }
}

fn labels(c: &Crash) -> Vec<&'static str> {
    c.resolutions.iter().map(Resolution::label).collect()
}

#[tokio::test]
async fn a_crash_at_every_window_converges() {
    let db = [(create_db("billing"), "CREATE DATABASE billing")];
    let with_orders = [
        (create_db("billing"), "CREATE DATABASE billing"),
        (
            create(ObjectKind::Table, "orders"),
            "CREATE TABLE orders (id INT)",
        ),
    ];
    let s = shard(1, 1, 1);

    // CREATE DATABASE.
    let c = crash(
        &[],
        create_db("billing"),
        "CREATE DATABASE billing",
        Some(DdlStep::Planned),
    )
    .await;
    assert!(labels(&c).is_empty());
    assert!(c.env.tables().databases.is_empty());
    let c = crash(
        &[],
        create_db("billing"),
        "CREATE DATABASE billing",
        Some(DdlStep::Prepared),
    )
    .await;
    assert_eq!(labels(&c), ["rolled_back"]);
    assert!(c.env.tables().databases.is_empty());
    assert!(!c.env.exec.has_schema(&s));
    for step in [Some(DdlStep::Applied), None] {
        let c = crash(&[], create_db("billing"), "CREATE DATABASE billing", step).await;
        assert_eq!(labels(&c), ["finished"]);
        assert_eq!(c.env.tables().databases[&DbId(1)].state, DbState::Active);
        assert!(c.env.exec.has_schema(&s));
    }

    // CREATE TABLE.
    let c = crash(
        &db,
        create(ObjectKind::Table, "orders"),
        "CREATE TABLE orders (id INT)",
        Some(DdlStep::Planned),
    )
    .await;
    assert!(labels(&c).is_empty());
    assert!(c.env.object("orders").is_none());
    let c = crash(
        &db,
        create(ObjectKind::Table, "orders"),
        "CREATE TABLE orders (id INT)",
        Some(DdlStep::Prepared),
    )
    .await;
    assert_eq!(labels(&c), ["rolled_back"]);
    assert!(c.env.object("orders").is_none());
    assert!(c.env.tables().objects.is_empty());
    for step in [Some(DdlStep::Applied), None] {
        let c = crash(
            &db,
            create(ObjectKind::Table, "orders"),
            "CREATE TABLE orders (id INT)",
            step,
        )
        .await;
        assert_eq!(labels(&c), ["finished"]);
        let o = c.env.object("orders").unwrap();
        assert_eq!((o.state, o.schema_version), (ObjectState::Active, 1));
        let sv = &c.env.tables().schema_versions[&(o.object_id, 1)];
        assert_eq!(
            sv.ddl, "CREATE TABLE orders (id INT)",
            "the intent kept the statement"
        );
    }

    // ALTER TABLE.
    let c = crash(
        &with_orders,
        alter("orders"),
        "ALTER TABLE orders ADD c INT",
        Some(DdlStep::Prepared),
    )
    .await;
    assert_eq!(labels(&c), ["rolled_back"]);
    assert_eq!(c.env.object("orders").unwrap().schema_version, 1);
    for step in [Some(DdlStep::Applied), None] {
        let c = crash(
            &with_orders,
            alter("orders"),
            "ALTER TABLE orders ADD c INT",
            step,
        )
        .await;
        assert_eq!(labels(&c), ["finished"]);
        assert_eq!(c.env.object("orders").unwrap().schema_version, 2);
    }

    // RENAME TABLE.
    let c = crash(
        &with_orders,
        rename(&[("orders", "o2")]),
        "RENAME TABLE orders TO o2",
        Some(DdlStep::Prepared),
    )
    .await;
    assert_eq!(labels(&c), ["rolled_back"]);
    assert!(c.env.object("orders").is_some());
    for step in [Some(DdlStep::Applied), None] {
        let c = crash(
            &with_orders,
            rename(&[("orders", "o2")]),
            "RENAME TABLE orders TO o2",
            step,
        )
        .await;
        assert_eq!(labels(&c), ["finished"]);
        assert!(c.env.object("orders").is_none());
        assert_eq!(c.env.object("o2").unwrap().schema_version, 2);
    }

    // DROP TABLE rolls forward from either side.
    for step in [Some(DdlStep::Prepared), Some(DdlStep::Applied), None] {
        let c = crash(
            &with_orders,
            drop_tables(&["orders"], false),
            "DROP TABLE orders",
            step,
        )
        .await;
        assert_eq!(labels(&c), ["rolled_forward"]);
        assert!(c.env.object("orders").is_none());
        assert!(c.env.exec.names(&s, ObjectKind::Table).is_empty());
    }

    // DROP DATABASE too.
    let drop_db = DdlStatement::DropDatabase {
        name: "billing".to_owned(),
        if_exists: false,
    };
    for step in [Some(DdlStep::Prepared), Some(DdlStep::Applied), None] {
        let c = crash(&with_orders, drop_db.clone(), "DROP DATABASE billing", step).await;
        assert_eq!(labels(&c), ["rolled_forward"]);
        assert_eq!(c.env.tables().databases[&DbId(1)].state, DbState::Dropped);
        assert!(!c.env.exec.has_schema(&s));
    }
}

#[tokio::test]
async fn a_lost_connection_leaves_the_job_for_the_reconciler() {
    // Lost after the statement took effect: finished.
    let env = billing_with_orders().await;
    env.exec.inject(Fault::LostAfter);
    let e = env
        .run(alter("orders"), "ALTER TABLE orders ADD c INT")
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::OutcomeUnknown));
    assert!(env.object("orders").unwrap().pending_job_id.is_some());
    assert_eq!(env.intents(), 1);
    let r = env.repair().await;
    assert_eq!(r[0].label(), "finished");
    assert_eq!(env.object("orders").unwrap().schema_version, 2);

    // Lost before it reached the server: rolled back.
    let env = billing_with_orders().await;
    env.exec.inject(Fault::LostBefore);
    assert!(
        env.run(
            create(ObjectKind::Table, "items"),
            "CREATE TABLE items (id INT)"
        )
        .await
        .is_err()
    );
    assert_eq!(env.object("items").unwrap().state, ObjectState::Creating);
    let r = env.repair().await;
    assert_eq!(r[0].label(), "rolled_back");
    assert!(env.object("items").is_none());
}

#[tokio::test]
async fn phase_3_retries_its_read_before_leaving_the_job() {
    let quick = |env: &Env| {
        env.engine().with_opts(DdlOpts {
            ddl_wait: Duration::from_millis(200),
            poll: Duration::from_millis(10),
            finish_read_attempts: 3,
            finish_read_backoff: Duration::from_millis(5),
        })
    };
    // Two failed reads, then one that works: published at once, nothing
    // left for the reconciler.
    let env = billing_with_orders().await;
    env.exec.fail_reads(2);
    let o = quick(&env)
        .execute(
            &req(alter("orders"), "ALTER TABLE orders ADD c INT"),
            &env.exec,
        )
        .await
        .unwrap();
    assert!(o.version.is_some());
    assert_eq!(env.intents(), 0);
    let orders = env.object("orders").unwrap();
    assert!(orders.pending_job_id.is_none());
    assert_eq!(orders.schema_version, 2);

    // Every attempt fails: the error comes back and the job stays for the
    // reconciler, which finishes it.
    let env = billing_with_orders().await;
    env.exec.fail_reads(3);
    let e = quick(&env)
        .execute(
            &req(alter("orders"), "ALTER TABLE orders ADD c INT"),
            &env.exec,
        )
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::OutcomeUnknown));
    assert!(env.object("orders").unwrap().pending_job_id.is_some());
    assert_eq!(env.intents(), 1);
    let r = env.repair().await;
    assert_eq!(r[0].label(), "finished");
    assert_eq!(env.object("orders").unwrap().schema_version, 2);

    // The default: four reads over about 3.5 s.
    let d = DdlOpts::default();
    assert_eq!(d.finish_read_attempts, 4);
    assert_eq!(d.finish_read_backoff, Duration::from_millis(500));
}

#[tokio::test]
async fn renames_swap_and_chain_like_mariadb() {
    let env = billing_with_orders().await;
    env.run(
        create(ObjectKind::Table, "items"),
        "CREATE TABLE items (id INT)",
    )
    .await
    .unwrap();
    let orders = env.object("orders").unwrap().object_id;
    let items = env.object("items").unwrap().object_id;
    env.run(
        rename(&[("orders", "tmp"), ("items", "orders"), ("tmp", "items")]),
        "RENAME TABLE orders TO tmp, items TO orders, tmp TO items",
    )
    .await
    .unwrap();
    assert_eq!(env.object("orders").unwrap().object_id, items);
    assert_eq!(env.object("items").unwrap().object_id, orders);
    assert!(env.object("tmp").is_none());
    let e = env
        .run(
            rename(&[("orders", "items")]),
            "RENAME TABLE orders TO items",
        )
        .await
        .unwrap_err();
    assert_eq!(errno_of(&e), 1050);

    // Across databases.
    env.run(create_db("crm"), "CREATE DATABASE crm")
        .await
        .unwrap();
    env.run(
        DdlStatement::RenameTables {
            pairs: vec![(
                ObjectName::bare("orders"),
                ObjectName::qualified("crm", "orders"),
            )],
            if_exists: false,
        },
        "RENAME TABLE orders TO crm.orders",
    )
    .await
    .unwrap();
    let moved = env.object("orders").unwrap();
    assert_eq!(moved.db_id, DbId(2));
    assert_eq!(
        env.exec.names(&shard(2, 2, 2), ObjectKind::Table),
        ["orders"]
    );
}

#[tokio::test]
async fn if_exists_alters_and_renames_note_a_missing_target() {
    let env = billing_with_orders().await;
    // ALTER TABLE IF EXISTS of a missing table: a note, nothing changes.
    let o = env
        .run(
            DdlStatement::AlterObject {
                kind: ObjectKind::Table,
                name: ObjectName::bare("nope"),
                if_exists: true,
                rename_to: None,
            },
            "ALTER TABLE IF EXISTS nope ADD c INT",
        )
        .await
        .unwrap();
    assert_eq!(o.version, None);
    assert_eq!(o.notes.len(), 1);
    assert_eq!(o.notes[0].code, 1146);
    assert_eq!(o.notes[0].message, "Table 'billing.nope' doesn't exist");
    // Without IF EXISTS it is MariaDB's error.
    let e = env
        .run(alter("nope"), "ALTER TABLE nope ADD c INT")
        .await
        .unwrap_err();
    assert_eq!(errno_of(&e), 1146);

    // RENAME TABLE IF EXISTS skips a missing source with a note.
    let orders = env.object("orders").unwrap().object_id;
    let o = env
        .run(
            DdlStatement::RenameTables {
                pairs: vec![
                    (ObjectName::bare("nope"), ObjectName::bare("nope2")),
                    (ObjectName::bare("orders"), ObjectName::bare("orders2")),
                ],
                if_exists: true,
            },
            "RENAME TABLE IF EXISTS nope TO nope2, orders TO orders2",
        )
        .await
        .unwrap();
    assert!(o.version.is_some());
    assert_eq!(o.notes.len(), 1);
    assert_eq!(env.object("orders2").unwrap().object_id, orders);
    // Every source missing: notes only.
    let o = env
        .run(
            DdlStatement::RenameTables {
                pairs: vec![(ObjectName::bare("nope"), ObjectName::bare("nope2"))],
                if_exists: true,
            },
            "RENAME TABLE IF EXISTS nope TO nope2",
        )
        .await
        .unwrap();
    assert_eq!((o.version, o.notes.len()), (None, 1));
}

#[tokio::test]
async fn an_alter_that_also_renames_moves_the_object() {
    let env = billing_with_orders().await;
    let orders = env.object("orders").unwrap();
    let o = env
        .run(
            DdlStatement::AlterObject {
                kind: ObjectKind::Table,
                name: ObjectName::bare("orders"),
                if_exists: false,
                rename_to: Some(ObjectName::bare("orders_v2")),
            },
            "ALTER TABLE orders ADD c INT, RENAME TO orders_v2",
        )
        .await
        .unwrap();
    assert!(o.version.is_some());
    let moved = env.object("orders_v2").unwrap();
    assert_eq!(moved.object_id, orders.object_id);
    assert_eq!(moved.schema_version, orders.schema_version + 1);
    assert!(env.object("orders").is_none());
    assert_eq!(env.intents(), 0);
    assert_eq!(
        env.exec.names(&shard(1, 1, 1), ObjectKind::Table),
        ["orders_v2"]
    );
    // Onto a taken name: 1050, nothing marked.
    env.run(
        create(ObjectKind::Table, "items"),
        "CREATE TABLE items (id INT)",
    )
    .await
    .unwrap();
    let e = env
        .run(
            DdlStatement::AlterObject {
                kind: ObjectKind::Table,
                name: ObjectName::bare("orders_v2"),
                if_exists: false,
                rename_to: Some(ObjectName::bare("items")),
            },
            "ALTER TABLE orders_v2 ADD d INT, RENAME TO items",
        )
        .await
        .unwrap_err();
    assert_eq!(errno_of(&e), 1050);
    assert!(env.object("orders_v2").unwrap().pending_job_id.is_none());
}

#[tokio::test]
async fn triggers_hang_off_their_tables_and_go_with_them() {
    let env = billing_with_orders().await;
    env.run(
        DdlStatement::CreateObject {
            kind: ObjectKind::Trigger,
            name: ObjectName::bare("trg"),
            if_not_exists: false,
            or_replace: false,
            parent: Some(ObjectName::bare("orders")),
        },
        "CREATE TRIGGER trg BEFORE INSERT ON orders",
    )
    .await
    .unwrap();
    let trg = env.object("trg").unwrap();
    assert_eq!(trg.parent_id, Some(env.object("orders").unwrap().object_id));
    assert_eq!(trg.kind, ObjectKind::Trigger);
    env.run(drop_tables(&["orders"], false), "DROP TABLE orders")
        .await
        .unwrap();
    assert!(
        env.object("trg").is_none(),
        "the trigger is a tombstone too"
    );

    // A trigger on a missing table.
    let e = env
        .run(
            DdlStatement::CreateObject {
                kind: ObjectKind::Trigger,
                name: ObjectName::bare("trg2"),
                if_not_exists: false,
                or_replace: false,
                parent: Some(ObjectName::bare("orders")),
            },
            "CREATE TRIGGER trg2 BEFORE INSERT ON orders",
        )
        .await
        .unwrap_err();
    assert_eq!(errno_of(&e), 1146);
}

#[tokio::test]
async fn or_replace_views_truncate_and_alter_database() {
    let env = billing_with_orders().await;
    env.run(create(ObjectKind::View, "v"), "CREATE VIEW v AS SELECT 1")
        .await
        .unwrap();
    let id = env.object("v").unwrap().object_id;
    env.run(
        DdlStatement::CreateObject {
            kind: ObjectKind::View,
            name: ObjectName::bare("v"),
            if_not_exists: false,
            or_replace: true,
            parent: None,
        },
        "CREATE OR REPLACE VIEW v AS SELECT 2",
    )
    .await
    .unwrap();
    let v = env.object("v").unwrap();
    assert_eq!((v.object_id, v.schema_version), (id, 2));

    // TRUNCATE changes nothing in the catalog.
    let before = env.tables().versions.catalog;
    let o = env
        .run(
            DdlStatement::Truncate {
                name: ObjectName::bare("orders"),
            },
            "TRUNCATE TABLE orders",
        )
        .await
        .unwrap();
    assert_eq!(o.version, None);
    assert_eq!(env.tables().versions.catalog, before);

    // ALTER DATABASE records the new defaults.
    let o = env
        .run(
            DdlStatement::AlterDatabase {
                name: "billing".to_owned(),
                charset: Some("latin1".to_owned()),
                collation: None,
            },
            "ALTER DATABASE billing CHARACTER SET latin1",
        )
        .await
        .unwrap();
    assert!(o.version.is_some());
    assert_eq!(env.tables().databases[&DbId(1)].default_charset, "latin1");
    let e = env
        .run(
            DdlStatement::AlterDatabase {
                name: "billing".to_owned(),
                charset: Some("latin1; DROP".to_owned()),
                collation: None,
            },
            "ALTER DATABASE billing CHARACTER SET x",
        )
        .await
        .unwrap_err();
    assert_eq!(errno_of(&e), 1115);
}

#[tokio::test]
async fn objects_created_outside_are_adopted() {
    let env = billing_with_orders().await;
    let s = shard(1, 1, 1);
    env.exec.create_object(
        &s,
        PhysicalObject {
            kind: ObjectKind::Table,
            name: "legacy".to_owned(),
            parent: None,
        },
    );
    env.exec.create_object(
        &s,
        PhysicalObject {
            kind: ObjectKind::Table,
            name: "__sprig_dtx".to_owned(),
            parent: None,
        },
    );
    let db = env.tables().databases[&DbId(1)].clone();
    let found = env.exec.objects(&s).await.unwrap();
    let (v, n) = env
        .engine()
        .adopt(T, &db, &found, &env.exec, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 1, "only legacy: orders has a row, __sprig_ is internal");
    let legacy = env.object("legacy").unwrap();
    assert_eq!(
        (legacy.state, legacy.changed_version),
        (ObjectState::Active, v)
    );
    assert!(
        env.tables().schema_versions[&(legacy.object_id, 1)]
            .ddl
            .starts_with("-- adopted")
    );
    assert!(env.object("__sprig_dtx").is_none());
    // Nothing more to adopt.
    let found = env.exec.objects(&s).await.unwrap();
    assert!(
        env.engine()
            .adopt(T, &db, &found, &env.exec, false)
            .await
            .unwrap()
            .is_none()
    );
}

#[test]
fn physical_names_become_logical() {
    let names = |id: DbId| (id == DbId(1)).then(|| "billing".to_owned());
    assert_eq!(
        logical_text(
            "CREATE TABLE `x` (FOREIGN KEY (a) REFERENCES `t42_d1_g1_s1`.`p` (id))",
            T,
            &names
        ),
        "CREATE TABLE `x` (FOREIGN KEY (a) REFERENCES `billing`.`p` (id))"
    );
    assert_eq!(
        logical_text("Table 't42_d1_g1_s1.x' doesn't exist", T, &names),
        "Table 'billing.x' doesn't exist"
    );
    // Other tenants, unknown databases and the catalog stay as they are.
    assert_eq!(
        logical_text("`t43_d1_g1_s1` `t42_d9_g1_s1` `t42_sprig`", T, &names),
        "`t43_d1_g1_s1` `t42_d9_g1_s1` `t42_sprig`"
    );
}

#[test]
fn intents_round_trip_as_json() {
    let i = DdlIntent {
        job: JobId(uuid::Uuid::now_v7()),
        started_ms: 1_700_000_000_000,
        ddl: "RENAME TABLE a TO b".to_owned(),
        user: Some(UserId(7)),
        shards: None,
        op: IntentOp::Rename {
            moves: vec![Move {
                object: ObjectId(3),
                db: DbId(1),
                name: "b".to_owned(),
                name_key: "b".to_owned(),
            }],
        },
    };
    let row = i.row(5, SystemTime::UNIX_EPOCH).unwrap();
    assert_eq!(row.key, intent_key(i.job));
    assert_eq!(row.value.as_ref().unwrap()["op"], "rename");
    assert_eq!(DdlIntent::from_row(&row).unwrap(), i);
    assert_eq!(i.objects(), vec![ObjectId(3)]);
    assert_eq!(
        drop_sql(
            &shard(1, 1, 1),
            ObjectKind::Table,
            &["a".to_owned(), "b".to_owned()]
        ),
        "DROP TABLE IF EXISTS `t42_d1_g1_s1`.`a`, `t42_d1_g1_s1`.`b`"
    );
}
