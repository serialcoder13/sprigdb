//! Live tests of schema changes across shards (chapter 21, R1) on the
//! Compose clusters: a hand-made four-shard table group, shards 1 and 2 on
//! the single node (also the catalog's cluster), shards 3 and 4 on the
//! replication primary, changed through [`DdlEngine`] and a
//! [`RoutedExecutor`]:
//!
//! - `ADD COLUMN`, `CREATE INDEX`, a generated column and its index, a
//!   `CHECK` constraint, `CREATE PROCEDURE` and `CREATE TRIGGER` (with the
//!   creating user's backend account as definer, checked on every shard);
//! - a `CHECK` that one shard's rows violate: refused there, undone on the
//!   shards already changed, the catalog unchanged;
//! - crashes in the middle of a job, repaired by the reconciler's
//!   `resolve`;
//! - a sequence of changes under a light write load (4 writers), with no
//!   client errors.
//!
//! Tenant `3_130_600_001` (r1ddl's range, brief rule 8b); everything it
//! creates is dropped by exact name. Skipped without
//! `SPRIG_TEST_MARIADB_SINGLE` and `SPRIG_TEST_MARIADB_REPLICATION`.

#![allow(
    clippy::many_single_char_names,
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts};
use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId, TenantId, UserId};
use sprig_common::names::SchemaName;
use sprig_common::time::SystemClock;

use super::*;
use crate::live_tests::{catalog_for, migrate, seed_directory, with_global, with_schema};
use crate::store::ObjectState;
use crate::version::MariaBackend;

const TENANT: TenantId = TenantId(3_130_600_001);
const DEFINER_USER: &str = "t3130600001_u1";

fn env(n: &str) -> Option<String> {
    std::env::var(n).ok().filter(|v| !v.trim().is_empty())
}

fn pool(url: &str) -> Pool {
    let opts = OptsBuilder::from_opts(Opts::from_url(url).unwrap()).pool_opts(
        PoolOpts::new()
            .with_constraints(PoolConstraints::new(0, 16).unwrap())
            .with_inactive_connection_ttl(Duration::from_secs(30)),
    );
    Pool::new(opts)
}

fn shard(n: u32) -> SchemaName {
    SchemaName::shard(TENANT, DbId(1), GroupId(2), ShardId(n))
}

fn template() -> SchemaName {
    SchemaName::shard(TENANT, DbId(1), GroupId(1), ShardId(1))
}

struct Live {
    v: Versioner<MariaBackend>,
    exec: RoutedExecutor,
    single: Pool,
    primary: Pool,
}

impl Live {
    fn engine(&self) -> DdlEngine<MariaBackend> {
        DdlEngine::new(self.v.clone(), Arc::new(SystemClock)).with_opts(DdlOpts {
            ddl_wait: Duration::from_millis(500),
            poll: Duration::from_millis(20),
            ..DdlOpts::default()
        })
    }

    fn crashing(&self, step: DdlStep) -> DdlEngine<MariaBackend> {
        self.engine().with_failpoint(Arc::new(move |_, s| {
            if s == step {
                Err(sprig_common::SprigError::internal(format!(
                    "injected crash at {s:?}"
                )))
            } else {
                Ok(())
            }
        }))
    }

    fn pool_of(&self, s: &SchemaName) -> &Pool {
        if matches!(
            s,
            SchemaName::Shard {
                shard: ShardId(3 | 4),
                group: GroupId(2),
                ..
            }
        ) {
            &self.primary
        } else {
            &self.single
        }
    }

    async fn run(&self, stmt: DdlStatement, sql: &str) -> Result<DdlOutcome> {
        self.engine().execute(&req(stmt, sql), &self.exec).await
    }

    async fn q<T: mysql_async::prelude::FromRow + Send + 'static>(
        &self,
        s: &SchemaName,
        sql: &str,
    ) -> Vec<T> {
        let mut c = self.pool_of(s).get_conn().await.unwrap();
        c.query(sql).await.unwrap()
    }

    /// Column names of `orders` on shard `s`.
    async fn columns(&self, s: &SchemaName) -> Vec<String> {
        self.q(
            s,
            &format!(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '{s}' \
                 AND TABLE_NAME = 'orders' ORDER BY ORDINAL_POSITION"
            ),
        )
        .await
    }

    async fn show(&self, s: &SchemaName) -> String {
        shards::comparable(
            &self
                .exec
                .show_create(s, ObjectKind::Table, "orders")
                .await
                .unwrap()
                .unwrap(),
        )
    }

    /// Every shard has the same `orders`; returns its text.
    async fn same_everywhere(&self) -> String {
        let first = self.show(&shard(1)).await;
        for n in 2..=4 {
            assert_eq!(self.show(&shard(n)).await, first, "shard {n} differs");
        }
        first
    }

    async fn catalog_def(&self, name: &str) -> Option<String> {
        let snap = self.v.backend().full_snapshot(TENANT).await.unwrap();
        let o = snap
            .objects
            .iter()
            .find(|o| o.name == name && o.state == ObjectState::Active)?;
        assert!(o.pending_job_id.is_none());
        let mut locked = self.v.lock(TENANT, VersionKind::Catalog).await.unwrap();
        let r = locked
            .tx()
            .latest_schema_version(o.object_id)
            .await
            .unwrap();
        locked.release().await.unwrap();
        r.and_then(|r| r.create_sql)
    }

    async fn intents(&self) -> Vec<DdlIntent> {
        DdlIntent::all(
            &self
                .v
                .backend()
                .full_snapshot(TENANT)
                .await
                .unwrap()
                .settings,
        )
    }

    async fn repair(&self) -> Vec<Resolution> {
        let snap = self.v.backend().full_snapshot(TENANT).await.unwrap();
        let mut out = Vec::new();
        for i in DdlIntent::all(&snap.settings) {
            out.push(
                self.engine()
                    .resolve(TENANT, &i, &snap, &self.exec)
                    .await
                    .unwrap(),
            );
        }
        out
    }
}

fn req(statement: DdlStatement, sql: &str) -> DdlRequest<'_> {
    DdlRequest {
        tenant: TENANT,
        statement,
        sql,
        logical_sql: sql,
        current_db: Some("r1ddl_shop"),
        user: Some(UserId(1)),
        lower_case_table_names: false,
    }
}

fn alter() -> DdlStatement {
    DdlStatement::AlterObject {
        kind: ObjectKind::Table,
        name: ObjectName::bare("orders"),
        if_exists: false,
        rename_to: None,
    }
}

fn create(kind: ObjectKind, name: &str, parent: Option<&str>) -> DdlStatement {
    DdlStatement::CreateObject {
        kind,
        name: ObjectName::bare(name),
        if_not_exists: false,
        or_replace: false,
        parent: parent.map(ObjectName::bare),
    }
}

/// `{s}` in `sql` is the template schema, backquoted.
fn t(sql: &str) -> String {
    sql.replace("{s}", &format!("`{}`", template()))
}

async fn cleanup(single: &Pool, primary: &Pool) {
    for p in [single, primary] {
        let mut c = p.get_conn().await.unwrap();
        for n in 1..=4 {
            c.query_drop(format!("DROP DATABASE IF EXISTS `{}`", shard(n)))
                .await
                .unwrap();
        }
        c.query_drop(format!("DROP DATABASE IF EXISTS `{}`", template()))
            .await
            .unwrap();
        c.query_drop(format!("DROP USER IF EXISTS `{DEFINER_USER}`@`%`"))
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn live_schema_changes_across_four_shards() {
    let (Some(_), Some(repl)) = (
        env("SPRIG_TEST_MARIADB_SINGLE"),
        env("SPRIG_TEST_MARIADB_REPLICATION"),
    ) else {
        eprintln!("skipped: needs SPRIG_TEST_MARIADB_SINGLE and SPRIG_TEST_MARIADB_REPLICATION");
        return;
    };
    let single_url = env("SPRIG_TEST_MARIADB_SINGLE").unwrap();
    let primary_url = repl.split(',').next().unwrap().trim().to_owned();
    with_schema("live_shard_ddl", |cat_pool, cat_schema| async move {
        with_global(|g| async move {
            migrate(&cat_pool, &cat_schema).await;
            seed_directory(&*g, TENANT).await;
            let single = pool(&single_url);
            let primary = pool(&primary_url);
            cleanup(&single, &primary).await;
            let ms = MariaExecutor::new(single.clone());
            let mp = MariaExecutor::new(primary.clone());
            let c_single = ClusterId::new("dev-single").unwrap();
            let c_primary = ClusterId::new("dev-primary").unwrap();
            let group: Vec<SchemaName> = (1..=4).map(shard).collect();
            let exec = RoutedExecutor::new(ms.clone())
                .route(shard(1), c_single.clone(), ms.clone())
                .route(shard(2), c_single.clone(), ms.clone())
                .route(shard(3), c_primary.clone(), mp.clone())
                .route(shard(4), c_primary.clone(), mp.clone())
                .layout(Some("orders"), group.clone())
                .layout(None, group.clone());
            let l = Live {
                v: catalog_for(&cat_pool, &cat_schema, TENANT, g),
                exec,
                single: single.clone(),
                primary: primary.clone(),
            };
            let r = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(body(&l))).await;
            cleanup(&single, &primary).await;
            single.disconnect().await.unwrap();
            primary.disconnect().await.unwrap();
            if let Err(p) = r {
                std::panic::resume_unwind(p);
            }
        })
        .await;
    })
    .await;
}

async fn body(l: &Live) {
    // The database (template schema on the catalog's cluster) and the
    // group's four shard schemas, as a split would leave them.
    l.run(
        DdlStatement::CreateDatabase {
            name: "r1ddl_shop".into(),
            if_not_exists: false,
            charset: None,
            collation: None,
        },
        "CREATE DATABASE r1ddl_shop",
    )
    .await
    .unwrap();
    for n in 1..=4 {
        let s = shard(n);
        let mut c = l.pool_of(&s).get_conn().await.unwrap();
        c.query_drop(format!("CREATE DATABASE `{s}`"))
            .await
            .unwrap();
    }
    for p in [&l.single, &l.primary] {
        let mut c = p.get_conn().await.unwrap();
        c.query_drop(format!(
            "CREATE USER IF NOT EXISTS `{DEFINER_USER}`@`%` IDENTIFIED BY 'r1ddl-unused'"
        ))
        .await
        .unwrap();
        c.query_drop(format!(
            "GRANT SELECT, INSERT, UPDATE, EXECUTE, TRIGGER ON `t3130600001\\_d%`.* TO `{DEFINER_USER}`@`%`"
        ))
        .await
        .unwrap();
    }

    // CREATE TABLE fans out to the group.
    let t0 = Instant::now();
    l.run(
        create(ObjectKind::Table, "orders", None),
        &t("CREATE TABLE {s}.`orders` (id BIGINT NOT NULL PRIMARY KEY, customer_id BIGINT NOT NULL, \
            qty INT NOT NULL, price DECIMAL(10,2) NOT NULL DEFAULT 0)"),
    )
    .await
    .unwrap();
    eprintln!("CREATE TABLE on 4 shards: {:?}", t0.elapsed());
    for n in 1..=4 {
        assert_eq!(
            l.columns(&shard(n)).await,
            ["id", "customer_id", "qty", "price"]
        );
    }
    // Rows: 2,000 per shard.
    for n in 1..=4u32 {
        let s = shard(n);
        let mut c = l.pool_of(&s).get_conn().await.unwrap();
        let base = u64::from(n) * 1_000_000;
        for chunk in 0..4u64 {
            let vals: Vec<String> = (0..500u64)
                .map(|i| {
                    let id = base + chunk * 500 + i;
                    format!("({id}, {}, {}, {}.50)", id % 97, 1 + id % 50, id % 300)
                })
                .collect();
            c.query_drop(format!(
                "INSERT INTO `{s}`.orders VALUES {}",
                vals.join(",")
            ))
            .await
            .unwrap();
        }
    }

    // ADD COLUMN.
    let t0 = Instant::now();
    let o = l
        .run(
            alter(),
            &t("ALTER TABLE {s}.`orders` ADD COLUMN note VARCHAR(200) NULL"),
        )
        .await
        .unwrap();
    eprintln!("ADD COLUMN on 4 shards: {:?}", t0.elapsed());
    assert!(o.version.is_some());
    let def = l.same_everywhere().await;
    assert!(def.contains("`note` varchar(200)"), "{def}");
    assert!(l.catalog_def("orders").await.unwrap().contains("`note`"));

    // ADD INDEX, as CREATE INDEX.
    let t0 = Instant::now();
    l.run(
        alter(),
        &t("CREATE INDEX by_customer ON {s}.`orders` (customer_id)"),
    )
    .await
    .unwrap();
    eprintln!("CREATE INDEX on 4 shards: {:?}", t0.elapsed());
    assert!(l.same_everywhere().await.contains("KEY `by_customer`"));

    // A generated column, then its index (two statements: MariaDB refuses
    // both in one online ALTER).
    let t0 = Instant::now();
    l.run(
        alter(),
        &t("ALTER TABLE {s}.`orders` ADD COLUMN total DECIMAL(12,2) AS (qty * price) VIRTUAL"),
    )
    .await
    .unwrap();
    l.run(
        alter(),
        &t("ALTER TABLE {s}.`orders` ADD INDEX by_total (total)"),
    )
    .await
    .unwrap();
    eprintln!("generated column + index on 4 shards: {:?}", t0.elapsed());
    let def = l.same_everywhere().await;
    assert!(
        def.contains("GENERATED ALWAYS AS") && def.contains("KEY `by_total`"),
        "{def}"
    );
    let n: Vec<u64> = l
        .q(
            &shard(3),
            &format!(
                "SELECT COUNT(*) FROM `{}`.orders WHERE total >= 0",
                shard(3)
            ),
        )
        .await;
    assert_eq!(n, [2000]);

    // A CHECK every shard's rows satisfy.
    l.run(
        alter(),
        &t("ALTER TABLE {s}.`orders` ADD CONSTRAINT qty_pos CHECK (qty > 0)"),
    )
    .await
    .unwrap();
    assert!(
        l.same_everywhere()
            .await
            .contains("CONSTRAINT `qty_pos` CHECK")
    );

    // A CHECK shard 3's rows violate: refused there, undone on 1 and 2.
    {
        let s = shard(3);
        let mut c = l.pool_of(&s).get_conn().await.unwrap();
        c.query_drop(format!(
            "INSERT INTO `{s}`.orders (id, customer_id, qty, price) VALUES (9, 1, 5000, 1)"
        ))
        .await
        .unwrap();
    }
    let before = l.same_everywhere().await;
    let cat_before = l.catalog_def("orders").await;
    let e = l
        .run(
            alter(),
            &t("ALTER TABLE {s}.`orders` ADD CONSTRAINT qty_small CHECK (qty < 1000)"),
        )
        .await
        .unwrap_err();
    eprintln!("refused CHECK: {e}");
    assert_eq!(errno(&e), Some(4025), "{e}");
    assert_eq!(l.same_everywhere().await, before);
    assert_eq!(l.catalog_def("orders").await, cat_before);
    assert!(l.intents().await.is_empty());

    // Stored code: a procedure and a trigger on every shard, defined by the
    // creating user's backend account.
    l.run(
        create(ObjectKind::Procedure, "order_count", None),
        &format!(
            "CREATE DEFINER=`{DEFINER_USER}`@`%` PROCEDURE order_count() SQL SECURITY DEFINER \
             SELECT COUNT(*) FROM orders"
        ),
    )
    .await
    .unwrap();
    l.run(
        create(ObjectKind::Trigger, "orders_bi", Some("orders")),
        &format!(
            "CREATE DEFINER=`{DEFINER_USER}`@`%` TRIGGER orders_bi BEFORE INSERT ON orders \
             FOR EACH ROW SET NEW.note = COALESCE(NEW.note, 'via trigger')"
        ),
    )
    .await
    .unwrap();
    for n in 1..=4 {
        let s = shard(n);
        let r: Vec<String> = l
            .q(
                &s,
                &format!(
                    "SELECT DEFINER FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = '{s}' \
                     AND ROUTINE_NAME = 'order_count' UNION ALL SELECT DEFINER FROM \
                     information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = '{s}' AND \
                     TRIGGER_NAME = 'orders_bi'"
                ),
            )
            .await;
        assert_eq!(
            r,
            [format!("{DEFINER_USER}@%"), format!("{DEFINER_USER}@%")],
            "shard {n}"
        );
        let mut c = l.pool_of(&s).get_conn().await.unwrap();
        c.query_drop(format!(
            "INSERT INTO `{s}`.orders (id, customer_id, qty, price) VALUES (8, 1, 1, 1)"
        ))
        .await
        .unwrap();
        let note: Option<String> = c
            .query_first(format!("SELECT note FROM `{s}`.orders WHERE id = 8"))
            .await
            .unwrap();
        assert_eq!(note.as_deref(), Some("via trigger"));
    }

    // A crash after two shards: the reconciler undoes them.
    let e = l
        .crashing(DdlStep::Shard(2))
        .execute(
            &req(
                alter(),
                &t("ALTER TABLE {s}.`orders` ADD COLUMN crash1 INT NULL"),
            ),
            &l.exec,
        )
        .await
        .unwrap_err();
    assert!(e.message.contains("injected crash"));
    assert_eq!(
        l.columns(&shard(1))
            .await
            .iter()
            .filter(|c| *c == "crash1")
            .count(),
        1
    );
    assert_eq!(
        l.columns(&shard(3))
            .await
            .iter()
            .filter(|c| *c == "crash1")
            .count(),
        0
    );
    let r = l.repair().await;
    assert_eq!(r, [Resolution::RolledBack]);
    assert!(!l.same_everywhere().await.contains("crash1"));
    // A crash after every shard but before publishing: finished.
    let _ = l
        .crashing(DdlStep::Applied)
        .execute(
            &req(
                alter(),
                &t("ALTER TABLE {s}.`orders` ADD COLUMN crash2 INT NULL"),
            ),
            &l.exec,
        )
        .await
        .unwrap_err();
    let r = l.repair().await;
    assert!(
        matches!(r.as_slice(), [Resolution::Finished { .. }]),
        "{r:?}"
    );
    assert!(l.same_everywhere().await.contains("`crash2`"));
    assert!(l.catalog_def("orders").await.unwrap().contains("`crash2`"));

    // Schema changes under a light write load: 4 writers, each inserting
    // and updating on every shard through pooled connections.
    let stop = Arc::new(AtomicBool::new(false));
    let ok = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let mut writers = Vec::new();
    for w in 0..4u64 {
        let pools = [l.single.clone(), l.primary.clone()];
        let (stop, ok, errors) = (Arc::clone(&stop), Arc::clone(&ok), Arc::clone(&errors));
        writers.push(tokio::spawn(async move {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let n = (i % 4) as u32 + 1;
                let s = shard(n);
                let p = &pools[usize::from(n >= 3)];
                let id = 50_000_000 + w * 1_000_000 + i;
                let r = async {
                    let mut c = p.get_conn().await?;
                    c.query_drop(format!(
                        "INSERT INTO `{s}`.orders (id, customer_id, qty, price) VALUES ({id}, {w}, 3, 2.5)"
                    ))
                    .await?;
                    c.query_drop(format!(
                        "UPDATE `{s}`.orders SET qty = qty + 1 WHERE id = {id}"
                    ))
                    .await
                }
                .await;
                match r {
                    Ok(()) => ok.fetch_add(1, Ordering::Relaxed),
                    Err(e) => {
                        eprintln!("writer {w}: {e}");
                        errors.fetch_add(1, Ordering::Relaxed)
                    }
                };
                i += 1;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }));
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let t0 = Instant::now();
    for sql in [
        "ALTER TABLE {s}.`orders` ADD COLUMN under_load INT NULL DEFAULT 7",
        "CREATE INDEX by_qty ON {s}.`orders` (qty)",
        "ALTER TABLE {s}.`orders` ADD COLUMN q2 INT AS (qty * 2) VIRTUAL",
        "ALTER TABLE {s}.`orders` ADD INDEX by_q2 (q2)",
        "DROP INDEX by_qty ON {s}.`orders`",
        "ALTER TABLE {s}.`orders` MODIFY COLUMN note VARCHAR(300) NULL",
        "ALTER TABLE {s}.`orders` ADD CONSTRAINT price_ok CHECK (price >= 0)",
    ] {
        let t1 = Instant::now();
        l.run(alter(), &t(sql)).await.unwrap();
        eprintln!("under load: {sql}: {:?}", t1.elapsed());
    }
    let elapsed = t0.elapsed();
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.await.unwrap();
    }
    let (ok, errors) = (ok.load(Ordering::Relaxed), errors.load(Ordering::Relaxed));
    eprintln!("light load: 7 changes in {elapsed:?}; {ok} writes OK, {errors} errors");
    assert_eq!(errors, 0);
    assert!(ok > 100);
    let def = l.same_everywhere().await;
    assert!(def.contains("`under_load`") && def.contains("`by_q2`") && !def.contains("`by_qty`"));
    assert!(def.contains("`note` varchar(300)"));

    // DROP TABLE goes everywhere (its trigger with it).
    l.run(
        DdlStatement::DropObjects {
            kind: ObjectKind::Table,
            names: vec![ObjectName::bare("orders")],
            if_exists: false,
        },
        &t("DROP TABLE {s}.`orders`"),
    )
    .await
    .unwrap();
    for n in 1..=4 {
        assert!(l.columns(&shard(n)).await.is_empty());
    }
    assert!(l.intents().await.is_empty());
}
