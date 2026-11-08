//! Live tests of the DDL path on the Compose single node: a catalog in
//! `__sprig_test_catalog_*` for a test tenant (ids from 9,000,000,000), its
//! physical schemas `t<tenant>_d..`, created, changed and dropped through
//! [`DdlEngine`] and [`MariaExecutor`]; crashes and repairs; conflicts.
//! Every case drops the tenant's schemas afterwards. Skipped without
//! `SPRIG_TEST_MARIADB_SINGLE`.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use mysql_async::Pool;
use mysql_async::prelude::Queryable;
use sprig_common::ids::{TenantId, UserId};
use sprig_common::time::SystemClock;
use sprig_common::{SprigCode, SprigError};

use super::*;
use crate::live_tests::{
    catalog_for, migrate, seed_directory, unique_tenant, with_global, with_schema,
};
use crate::version::MariaBackend;

struct Live {
    tenant: TenantId,
    pool: Pool,
    v: Versioner<MariaBackend>,
    exec: MariaExecutor,
}

impl Live {
    fn engine(&self) -> DdlEngine<MariaBackend> {
        DdlEngine::new(self.v.clone(), Arc::new(SystemClock)).with_opts(DdlOpts {
            ddl_wait: Duration::from_millis(300),
            poll: Duration::from_millis(20),
            ..DdlOpts::default()
        })
    }

    fn req<'a>(&self, statement: DdlStatement, sql: &'a str) -> DdlRequest<'a> {
        DdlRequest {
            tenant: self.tenant,
            statement,
            sql,
            logical_sql: sql,
            current_db: Some("billing"),
            user: Some(UserId(1)),
            lower_case_table_names: false,
        }
    }

    async fn run(&self, statement: DdlStatement, sql: &str) -> Result<DdlOutcome> {
        self.engine()
            .execute(&self.req(statement, sql), &self.exec)
            .await
    }

    async fn snapshot(&self) -> crate::store::Snapshot {
        self.v.backend().full_snapshot(self.tenant).await.unwrap()
    }

    async fn object(&self, name: &str) -> Option<ObjectRow> {
        self.snapshot()
            .await
            .objects
            .into_iter()
            .find(|o| o.name == name && o.state != ObjectState::Dropped)
    }

    async fn db(&self, name: &str) -> DatabaseRow {
        self.snapshot()
            .await
            .databases
            .into_iter()
            .find(|d| d.name == name && d.state != DbState::Dropped)
            .unwrap()
    }

    async fn schema_of(&self, name: &str) -> SchemaName {
        shard_schemas(self.tenant, &self.db(name).await)[0]
    }

    async fn repair(&self) -> Vec<Resolution> {
        let snap = self.snapshot().await;
        let mut out = Vec::new();
        for i in DdlIntent::all(&snap.settings) {
            out.push(
                self.engine()
                    .resolve(self.tenant, &i, &snap, &self.exec)
                    .await
                    .unwrap(),
            );
        }
        out
    }
}

async fn drop_tenant_schemas(pool: &Pool, tenant: TenantId) {
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
}

async fn with_tenant<F, Fut>(case: &str, body: F)
where
    F: FnOnce(Live) -> Fut,
    Fut: Future<Output = ()>,
{
    with_schema(case, |pool, schema| async move {
        with_global(|g| async move {
            migrate(&pool, &schema).await;
            let tenant = unique_tenant();
            seed_directory(&*g, tenant).await;
            let live = Live {
                tenant,
                pool: pool.clone(),
                v: catalog_for(&pool, &schema, tenant, g),
                exec: MariaExecutor::new(pool.clone()),
            };
            let r = AssertUnwindSafe(body(live)).catch_unwind().await;
            drop_tenant_schemas(&pool, tenant).await;
            if let Err(p) = r {
                std::panic::resume_unwind(p);
            }
        })
        .await;
    })
    .await;
}

fn create_db(name: &str) -> DdlStatement {
    DdlStatement::CreateDatabase {
        name: name.to_owned(),
        if_not_exists: false,
        charset: None,
        collation: None,
    }
}

fn create(kind: ObjectKind, name: ObjectName) -> DdlStatement {
    DdlStatement::CreateObject {
        kind,
        name,
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

async fn columns(pool: &Pool, schema: &SchemaName, table: &str) -> Vec<String> {
    let mut c = pool.get_conn().await.unwrap();
    c.exec(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? \
         AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
        (schema.to_string(), table),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn live_create_alter_rename_drop() {
    with_tenant("live_ddl_lifecycle", |l| async move {
        let o = l
            .run(create_db("billing"), "CREATE DATABASE billing")
            .await
            .unwrap();
        assert_eq!(o.affected_rows, 1);
        let db = l.db("billing").await;
        assert_eq!(db.state, DbState::Active);
        assert!(!db.default_charset.is_empty() && !db.default_collation.is_empty());
        let s = l.schema_of("billing").await;
        assert_eq!(s.to_string(), format!("t{}_d1_g1_s1", l.tenant));
        assert!(l.exec.schema(&s).await.unwrap().is_some());

        // A table, unqualified: the executor runs it in the shard schema.
        l.run(
            create(ObjectKind::Table, ObjectName::bare("orders")),
            "CREATE TABLE orders (id INT PRIMARY KEY, total DECIMAL(10,2))",
        )
        .await
        .unwrap();
        let orders = l.object("orders").await.unwrap();
        assert_eq!(
            (orders.state, orders.schema_version),
            (ObjectState::Active, 1)
        );
        assert_eq!(columns(&l.pool, &s, "orders").await, ["id", "total"]);

        // ALTER and CREATE INDEX are alters of the table.
        l.run(
            alter("orders"),
            "ALTER TABLE orders ADD COLUMN placed DATETIME",
        )
        .await
        .unwrap();
        l.run(alter("orders"), "CREATE INDEX ix_placed ON orders (placed)")
            .await
            .unwrap();
        let orders = l.object("orders").await.unwrap();
        assert_eq!(orders.schema_version, 3);
        let mut c = l.pool.get_conn().await.unwrap();
        let store =
            l.v.backend()
                .locator()
                .writer(l.tenant)
                .await
                .unwrap()
                .store;
        let sv = store
            .latest_schema_version(&mut c, orders.object_id)
            .await
            .unwrap()
            .unwrap();
        drop(c);
        let text = sv.create_sql.unwrap();
        assert!(text.contains("`placed` datetime"), "{text}");
        assert!(text.contains("KEY `ix_placed`"), "{text}");
        assert_eq!(sv.ddl, "CREATE INDEX ix_placed ON orders (placed)");

        // A second database, and a foreign key across: create_sql names
        // the logical database.
        l.run(create_db("crm"), "CREATE DATABASE crm")
            .await
            .unwrap();
        let crm = l.schema_of("crm").await;
        l.run(
            create(ObjectKind::Table, ObjectName::qualified("crm", "customers")),
            &format!("CREATE TABLE `{crm}`.customers (id INT PRIMARY KEY)"),
        )
        .await
        .unwrap();
        l.run(
            create(ObjectKind::Table, ObjectName::bare("invoices")),
            &format!(
                "CREATE TABLE invoices (id INT PRIMARY KEY, cid INT, \
                 FOREIGN KEY (cid) REFERENCES `{crm}`.customers (id))"
            ),
        )
        .await
        .unwrap();
        let inv = l.object("invoices").await.unwrap();
        let mut c = l.pool.get_conn().await.unwrap();
        let sv = store
            .latest_schema_version(&mut c, inv.object_id)
            .await
            .unwrap()
            .unwrap();
        drop(c);
        let text = sv.create_sql.unwrap();
        assert!(text.contains("REFERENCES `crm`.`customers`"), "{text}");
        assert!(!text.contains(&crm.to_string()), "{text}");

        // A view, a trigger, a procedure.
        l.run(
            create(ObjectKind::View, ObjectName::bare("big")),
            "CREATE VIEW big AS SELECT id FROM orders WHERE total > 100",
        )
        .await
        .unwrap();
        l.run(
            DdlStatement::CreateObject {
                kind: ObjectKind::Trigger,
                name: ObjectName::bare("trg"),
                if_not_exists: false,
                or_replace: false,
                parent: Some(ObjectName::bare("orders")),
            },
            "CREATE TRIGGER trg BEFORE INSERT ON orders FOR EACH ROW SET NEW.total = NEW.total",
        )
        .await
        .unwrap();
        l.run(
            create(ObjectKind::Procedure, ObjectName::bare("Recalc")),
            "CREATE PROCEDURE Recalc() SELECT 1",
        )
        .await
        .unwrap();
        assert_eq!(
            l.object("trg").await.unwrap().parent_id,
            Some(orders.object_id)
        );
        assert_eq!(l.object("Recalc").await.unwrap().name_key, "recalc");

        // A backend error comes back with logical names.
        let e = l
            .run(
                create(ObjectKind::View, ObjectName::bare("broken")),
                "CREATE VIEW broken AS SELECT * FROM nope",
            )
            .await
            .unwrap_err();
        assert_eq!(errno(&e), Some(1146));
        assert_eq!(e.message, "Table 'billing.nope' doesn't exist");
        assert!(l.object("broken").await.is_none());

        // RENAME keeps the id and the trigger.
        l.run(
            DdlStatement::RenameTables {
                pairs: vec![(ObjectName::bare("orders"), ObjectName::bare("orders_old"))],
                if_exists: false,
            },
            "RENAME TABLE orders TO orders_old",
        )
        .await
        .unwrap();
        let renamed = l.object("orders_old").await.unwrap();
        assert_eq!(renamed.object_id, orders.object_id);
        assert!(
            l.exec
                .show_create(&s, ObjectKind::Table, "orders_old")
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            l.exec
                .show_create(&s, ObjectKind::Table, "orders")
                .await
                .unwrap()
                .is_none()
        );

        // TRUNCATE: no version.
        let o = l
            .run(
                DdlStatement::Truncate {
                    name: ObjectName::bare("orders_old"),
                },
                "TRUNCATE TABLE orders_old",
            )
            .await
            .unwrap();
        assert_eq!(o.version, None);

        // DROP TABLE takes its trigger along.
        l.run(
            DdlStatement::DropObjects {
                kind: ObjectKind::Table,
                names: vec![ObjectName::bare("orders_old")],
                if_exists: false,
            },
            "DROP TABLE orders_old",
        )
        .await
        .unwrap();
        assert!(l.object("orders_old").await.is_none());
        assert!(l.object("trg").await.is_none());
        let o = l
            .run(
                DdlStatement::DropObjects {
                    kind: ObjectKind::Table,
                    names: vec![ObjectName::bare("orders_old")],
                    if_exists: true,
                },
                "DROP TABLE IF EXISTS orders_old",
            )
            .await
            .unwrap();
        assert_eq!(o.notes[0].code, 1051);

        // DROP DATABASE: every physical schema goes, tombstones stay.
        let o = l
            .run(
                DdlStatement::DropDatabase {
                    name: "billing".to_owned(),
                    if_exists: false,
                },
                "DROP DATABASE billing",
            )
            .await
            .unwrap();
        assert_eq!(o.affected_rows, 1, "invoices");
        assert!(l.exec.schema(&s).await.unwrap().is_none());
        let snap = l.snapshot().await;
        assert!(
            snap.objects
                .iter()
                .filter(|o| o.db_id == DbId(1))
                .all(|o| o.state == ObjectState::Dropped)
        );
        assert_eq!(DdlIntent::all(&snap.settings).len(), 0);
        // Physical schemas left: crm only.
        assert_eq!(l.exec.tenant_schemas(l.tenant).await.unwrap(), vec![crm]);
    })
    .await;
}

#[tokio::test]
async fn live_refusal_conflict_and_crash_repair() {
    with_tenant("live_ddl_crash", |l| async move {
        l.run(create_db("billing"), "CREATE DATABASE billing")
            .await
            .unwrap();
        let s = l.schema_of("billing").await;
        l.run(
            create(ObjectKind::Table, ObjectName::bare("orders")),
            "CREATE TABLE orders (id INT PRIMARY KEY)",
        )
        .await
        .unwrap();
        let version = l.snapshot().await.versions.catalog;

        // Refused: rolled back, no version.
        let e = l
            .run(alter("orders"), "ALTER TABLE orders ADD COLUMN id INT")
            .await
            .unwrap_err();
        assert_eq!(errno(&e), Some(1060));
        assert_eq!(l.snapshot().await.versions.catalog, version);
        assert_eq!(l.object("orders").await.unwrap().pending_job_id, None);

        // A crash after the ALTER ran: the next ALTER waits, then 39071.
        let crash = |step: DdlStep| -> DdlFailpoint {
            Arc::new(move |_, s| {
                if s == step {
                    Err(SprigError::internal("injected crash"))
                } else {
                    Ok(())
                }
            })
        };
        let e = l
            .engine()
            .with_failpoint(crash(DdlStep::Applied))
            .execute(
                &l.req(alter("orders"), "ALTER TABLE orders ADD COLUMN a INT"),
                &l.exec,
            )
            .await
            .unwrap_err();
        assert!(e.message.contains("injected"));
        let e = l
            .run(alter("orders"), "ALTER TABLE orders ADD COLUMN b INT")
            .await
            .unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::CatalogConflict));
        assert_eq!(columns(&l.pool, &s, "orders").await, ["id", "a"]);

        // A crash before CREATE TABLE ran, and one after.
        assert!(
            l.engine()
                .with_failpoint(crash(DdlStep::Prepared))
                .execute(
                    &l.req(
                        create(ObjectKind::Table, ObjectName::bare("never")),
                        "CREATE TABLE never (id INT)"
                    ),
                    &l.exec
                )
                .await
                .is_err()
        );
        assert!(
            l.engine()
                .with_failpoint(crash(DdlStep::Applied))
                .execute(
                    &l.req(
                        create(ObjectKind::Table, ObjectName::bare("items")),
                        "CREATE TABLE items (id INT)"
                    ),
                    &l.exec
                )
                .await
                .is_err()
        );
        assert_eq!(
            l.object("items").await.unwrap().state,
            ObjectState::Creating
        );

        // The reconciler's repair: the ALTER and `items` finish, `never`
        // rolls back.
        let mut labels: Vec<&str> = l.repair().await.iter().map(Resolution::label).collect();
        labels.sort_unstable();
        assert_eq!(labels, ["finished", "finished", "rolled_back"]);
        assert_eq!(l.object("orders").await.unwrap().schema_version, 2);
        assert_eq!(l.object("items").await.unwrap().state, ObjectState::Active);
        assert!(l.object("never").await.is_none());
        l.run(alter("orders"), "ALTER TABLE orders ADD COLUMN b INT")
            .await
            .unwrap();

        // A crash before DROP TABLE ran rolls forward.
        assert!(
            l.engine()
                .with_failpoint(crash(DdlStep::Prepared))
                .execute(
                    &l.req(
                        DdlStatement::DropObjects {
                            kind: ObjectKind::Table,
                            names: vec![ObjectName::bare("items")],
                            if_exists: false,
                        },
                        "DROP TABLE items"
                    ),
                    &l.exec
                )
                .await
                .is_err()
        );
        assert!(
            l.exec
                .show_create(&s, ObjectKind::Table, "items")
                .await
                .unwrap()
                .is_some()
        );
        let r = l.repair().await;
        assert_eq!(r[0].label(), "rolled_forward");
        assert!(
            l.exec
                .show_create(&s, ObjectKind::Table, "items")
                .await
                .unwrap()
                .is_none()
        );
        assert!(l.object("items").await.is_none());

        // A crash after CREATE DATABASE ran finishes it.
        assert!(
            l.engine()
                .with_failpoint(crash(DdlStep::Applied))
                .execute(&l.req(create_db("crm"), "CREATE DATABASE crm"), &l.exec)
                .await
                .is_err()
        );
        let r = l.repair().await;
        assert_eq!(r[0].label(), "finished");
        assert_eq!(l.db("crm").await.state, DbState::Active);
    })
    .await;
}

#[tokio::test]
async fn live_concurrent_ddl_on_one_table() {
    with_tenant("live_ddl_concurrent", |l| async move {
        l.run(create_db("billing"), "CREATE DATABASE billing")
            .await
            .unwrap();
        l.run(
            create(ObjectKind::Table, ObjectName::bare("orders")),
            "CREATE TABLE orders (id INT PRIMARY KEY)",
        )
        .await
        .unwrap();
        // Eight ALTERs at once, each with its own column; a long wait so
        // they queue rather than fail.
        let engine = l.engine().with_opts(DdlOpts {
            ddl_wait: Duration::from_secs(30),
            poll: Duration::from_millis(10),
            ..DdlOpts::default()
        });
        let sqls: Vec<String> = (0..8)
            .map(|i| format!("ALTER TABLE orders ADD COLUMN c{i} INT"))
            .collect();
        let tasks = sqls.iter().map(|sql| {
            let engine = engine.clone();
            let r = l.req(alter("orders"), sql);
            let exec = &l.exec;
            async move { engine.execute(&r, exec).await }
        });
        let results = futures::future::join_all(tasks).await;
        assert!(results.iter().all(Result::is_ok), "{results:?}");
        assert_eq!(l.object("orders").await.unwrap().schema_version, 9);
        let s = l.schema_of("billing").await;
        assert_eq!(columns(&l.pool, &s, "orders").await.len(), 9);

        // With no wait, a held table fails at once with 39071.
        let held = l
            .engine()
            .with_failpoint(Arc::new(|_, s| {
                if s == DdlStep::Applied {
                    Err(SprigError::internal("injected crash"))
                } else {
                    Ok(())
                }
            }))
            .execute(
                &l.req(alter("orders"), "ALTER TABLE orders ADD COLUMN z INT"),
                &l.exec,
            )
            .await;
        assert!(held.is_err());
        let e = l
            .engine()
            .with_opts(DdlOpts {
                ddl_wait: Duration::ZERO,
                poll: Duration::from_millis(10),
                ..DdlOpts::default()
            })
            .execute(
                &l.req(alter("orders"), "ALTER TABLE orders ADD COLUMN y INT"),
                &l.exec,
            )
            .await
            .unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::CatalogConflict));
        assert_eq!(l.repair().await[0].label(), "finished");
    })
    .await;
}
