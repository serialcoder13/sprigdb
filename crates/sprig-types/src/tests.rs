//! The entry point over a scripted runner: the statements each plan sends
//! (chapter 32, "Testing": golden tests for writes, DDL and metadata).

use std::sync::Arc;

use sprig_proto_mysql::ColumnDef;
use sprig_proto_mysql::resultset::column_type;

use super::*;
use crate::exec::fake::Fake;
use crate::model::fixtures::assets;
use crate::rewrite::tests::One;

fn ctx(l: &One) -> Ctx<'_> {
    Ctx {
        lookup: l,
        lex: LexOptions::mariadb(),
        inheritance_only: false,
        limits: Limits::default(),
    }
}

fn state() -> TypesState {
    let db = assets();
    let types: Vec<TypeDef> = db.iter().map(|t| (**t).clone()).collect();
    let h: Vec<HierarchyDef> = db.hierarchies().map(|h| (**h).clone()).collect();
    TypesState::new(types, h)
}

fn rows(cols: &[(&str, u8)], data: &[&[Option<&str>]]) -> Rows {
    Rows {
        columns: cols
            .iter()
            .map(|(n, t)| ColumnDef {
                name: bytes::Bytes::copy_from_slice(n.as_bytes()),
                col_type: *t,
                ..ColumnDef::default()
            })
            .collect(),
        rows: data
            .iter()
            .map(|r| r.iter().map(|v| v.map(|s| s.as_bytes().to_vec())).collect())
            .collect(),
    }
}

async fn run(sql: &str, fake: &mut Fake) -> Result<Handled> {
    let l = One(Arc::new(assets()));
    let st = state();
    handle(sql, Some(&st), &ctx(&l), fake).await
}

#[tokio::test]
async fn plain_statements_are_not_typed() {
    let mut f = Fake::default();
    for sql in [
        "SELECT * FROM plain",
        "INSERT INTO asset (id, name) VALUES (1, 'a')",
        "UPDATE asset SET name = 'x'",
        "SELECT name FROM asset",
        "SET @x = 1",
        "SELECT _sprig_hpath FROM org_unit",
    ] {
        assert!(
            matches!(run(sql, &mut f).await.unwrap(), Handled::NotTyped),
            "{sql}"
        );
    }
    assert!(f.log.is_empty());
    let none = TypesState::unmigrated();
    assert!(!maybe_typed(b"SELECT 1", Some(&none)));
    assert!(maybe_typed(
        b"create table t (a int) inherits (p)",
        Some(&none)
    ));
}

/// Names that merely start with `sprig_` (the connector matrix's dump
/// databases `sprig_dump_*`) are not `SPRIG_*` calls: only the real
/// function names, in call position, are (r4fix).
#[tokio::test]
async fn sprig_prefixed_names_are_not_functions() {
    let mut f = Fake::default();
    for sql in [
        "CREATE DATABASE sprig_dump_1",
        "SELECT * FROM sprig_dump_1.plain",
        "SELECT /*!40001 SQL_NO_CACHE */ * FROM `sprig_dump_1`.`sprig_t`",
        "INSERT INTO sprig_dump_1.sprig_t (sprig_depth) VALUES (1)",
        "UPDATE sprig_t SET sprig_path = 'x'",
        "DELETE FROM sprig_dump_1.sprig_t",
        "SELECT sprig_depth, `sprig_subtree` FROM sprig_dump_1.sprig_t",
        "SELECT sprig_dump_1.sprig_depth(1)",
        "SELECT 'SPRIG_DEPTH(org_unit)'",
    ] {
        assert!(
            matches!(run(sql, &mut f).await.unwrap(), Handled::NotTyped),
            "{sql}"
        );
    }
    assert!(f.log.is_empty(), "{:?}", f.log);
    let lex = LexOptions::mariadb();
    for sql in [
        "SELECT SPRIG_DEPTH(org_unit) FROM org_unit",
        "SELECT * FROM t WHERE sprig_subtree (t, 1)",
        "SELECT SPRIG_ORIGIN() FROM v",
        "select sprig_ancestors(t, 1), Sprig_Descendants(t, 2), sprig_path(t)",
    ] {
        assert!(grammar::calls_sprig_function(sql.as_bytes(), &lex), "{sql}");
    }
    for sql in [
        "SELECT * FROM sprig_dump_1.t",
        "SELECT sprig_depth FROM t",
        "SELECT sprig_dump_1.sprig_depth(1)",
        "SELECT sprig_other(1)",
        "SELECT `SPRIG_DEPTH`(t)",
    ] {
        assert!(
            !grammar::calls_sprig_function(sql.as_bytes(), &lex),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn inserts_write_each_level() {
    let mut f = Fake::default();
    let h = run(
        "INSERT INTO truck (id, name, vin, payload_kg) VALUES (7, 'T', '1FT', 10.5), (8, 'U', '2FT', 3)",
        &mut f,
    )
    .await
    .unwrap();
    let Handled::Ok(d) = h else { panic!("{h:?}") };
    assert_eq!(d.affected_rows, 2);
    assert_eq!(d.info, "Records: 2  Duplicates: 0  Warnings: 0");
    // The plan's own transaction runs at READ COMMITTED (decision T17).
    assert_eq!(f.plan_isolation, 1);
    assert_eq!(
        f.log,
        [
            "BEGIN",
            "INSERT INTO `asset` (`id`, `name`, `_type`, `_type_path`) VALUES (7, 'T', 'truck', \
             '/asset/vehicle/truck/'), (8, 'U', 'truck', '/asset/vehicle/truck/')",
            "INSERT INTO `vehicle` (`id`, `vin`) VALUES (7, '1FT'), (8, '2FT')",
            "INSERT INTO `truck` (`id`, `payload_kg`) VALUES (7, 10.5), (8, 3)",
            "COMMIT",
        ]
    );
    // Inside a client transaction: a savepoint.
    let mut f = Fake {
        in_txn: true,
        ..Fake::default()
    };
    run("INSERT INTO vehicle VALUES (9, 'V', 4, 'VIN')", &mut f)
        .await
        .unwrap();
    let log = numbered(&f.log);
    assert_eq!(log[0], "SAVEPOINT sprig_types_N");
    assert_eq!(
        f.log[1],
        "INSERT INTO `asset` (`id`, `name`, `site_id`, `_type`, `_type_path`) VALUES (9, 'V', 4, \
         'vehicle', '/asset/vehicle/')"
    );
    assert_eq!(log.last().unwrap(), "RELEASE SAVEPOINT sprig_types_N");
    assert!(
        !f.log
            .iter()
            .any(|l| l.contains("innodb_snapshot_isolation"))
    );
}

/// bugfix-54 (T17's leftover): inside a client's transaction on a session
/// with MariaDB's snapshot-isolation check on, the plan runs with the check
/// off and turns it on again, whether the plan succeeds or fails. A 1020
/// would roll back the client's whole transaction, savepoint included.
#[tokio::test]
async fn nested_plans_turn_snapshot_isolation_off() {
    let mut f = Fake {
        in_txn: true,
        snapshot_isolation: true,
        ..Fake::default()
    };
    run("INSERT INTO vehicle VALUES (9, 'V', 4, 'VIN')", &mut f)
        .await
        .unwrap();
    assert_eq!(f.log[0], "SET SESSION innodb_snapshot_isolation = OFF");
    let log = numbered(&f.log);
    assert_eq!(log[1], "SAVEPOINT sprig_types_N");
    assert_eq!(log[log.len() - 2], "RELEASE SAVEPOINT sprig_types_N");
    assert_eq!(
        f.log.last().unwrap(),
        "SET SESSION innodb_snapshot_isolation = ON"
    );
    // A failing plan: rolled back to the savepoint, and the check is on
    // again.
    let mut f = Fake {
        in_txn: true,
        snapshot_isolation: true,
        ..Fake::default()
    };
    let tx = crate::exec::Txn::begin(&mut f).await.unwrap();
    let e = tx
        .end::<()>(
            &mut f,
            Err(sprig_common::SprigError::standard(
                1062,
                *b"23000",
                "Duplicate entry",
            )),
        )
        .await
        .unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 1062);
    assert_eq!(
        numbered(&f.log),
        [
            "SET SESSION innodb_snapshot_isolation = OFF",
            "SAVEPOINT sprig_types_N",
            "ROLLBACK TO SAVEPOINT sprig_types_N",
            "SET SESSION innodb_snapshot_isolation = ON",
        ]
    );
    // Autocommit plans don't touch it (they run at READ COMMITTED).
    let mut f = Fake {
        snapshot_isolation: true,
        ..Fake::default()
    };
    run("INSERT INTO vehicle VALUES (9, 'V', 4, 'VIN')", &mut f)
        .await
        .unwrap();
    assert!(
        !f.log
            .iter()
            .any(|l| l.contains("innodb_snapshot_isolation"))
    );
}

/// The log with each bracket's savepoint number as `N`.
fn numbered(log: &[String]) -> Vec<String> {
    log.iter()
        .map(|l| match l.find("sprig_types_") {
            Some(i) => {
                let head = &l[..i + "sprig_types_".len()];
                let rest = l[head.len()..].trim_start_matches(|c: char| c.is_ascii_digit());
                format!("{head}N{rest}")
            }
            None => l.clone(),
        })
        .collect()
}

/// RL66: a plan inside another plan's bracket, both inside the client's
/// transaction, uses its own savepoint, so the inner `RELEASE` leaves the
/// outer savepoint alone (it failed with 1305 when both had one name).
#[tokio::test]
async fn nested_brackets_have_their_own_savepoints() {
    let mut f = Fake {
        in_txn: true,
        ..Fake::default()
    };
    let outer = crate::exec::Txn::begin(&mut f).await.unwrap();
    let inner = crate::exec::Txn::begin(&mut f).await.unwrap();
    inner.end(&mut f, Ok(())).await.unwrap();
    outer.end(&mut f, Ok(())).await.unwrap();
    let name = |l: &str| l.rsplit(' ').next().unwrap_or_default().to_owned();
    assert_eq!(f.log.len(), 4);
    assert!(f.log[0].starts_with("SAVEPOINT sprig_types_"));
    assert!(f.log[1].starts_with("SAVEPOINT sprig_types_"));
    assert_ne!(name(&f.log[0]), name(&f.log[1]));
    assert_eq!(f.log[2], format!("RELEASE SAVEPOINT {}", name(&f.log[1])));
    assert_eq!(f.log[3], format!("RELEASE SAVEPOINT {}", name(&f.log[0])));
}

#[tokio::test]
async fn insert_refusals() {
    let mut f = Fake::default();
    for (sql, code) in [
        ("INSERT INTO truck (id, _type) VALUES (1, 'x')", 39705),
        ("INSERT INTO truck (id, nope) VALUES (1, 2)", 1054),
        ("INSERT INTO truck (id, name) VALUES (1)", 1136),
        (
            "INSERT INTO truck (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = 'b'",
            39710,
        ),
        (
            "INSERT INTO truck (id, name) SELECT id, name FROM asset",
            39710,
        ),
        ("INSERT INTO truck (id, name) VALUES (RAND(), 'a')", 39710),
    ] {
        let e = run(sql, &mut f).await.unwrap_err();
        assert_eq!(e.protocol().mysql_errno, code, "{sql}: {e}");
    }
    assert!(f.log.is_empty(), "{:?}", f.log);
}

#[tokio::test]
async fn updates_on_one_table_forward_and_on_several_count() {
    let mut f = Fake::default();
    let Handled::Forward { sql, .. } = run("UPDATE vehicle SET vin = 'X' WHERE vin = 'Y'", &mut f)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        sql,
        "UPDATE `vehicle` AS `vehicle` SET `vehicle`.`vin` = 'X' WHERE `vehicle`.vin = 'Y'"
    );
    let Handled::Forward { sql, .. } = run(
        "UPDATE vehicle SET name = 'n' WHERE site_id = 3 LIMIT 5",
        &mut f,
    )
    .await
    .unwrap() else {
        panic!()
    };
    assert_eq!(
        sql,
        "UPDATE `asset` AS `vehicle` SET `vehicle`.`name` = 'n' WHERE (`vehicle`.site_id = 3) AND \
         (`vehicle`.`_type_path` >= '/asset/vehicle/' AND `vehicle`.`_type_path` < \
         '/asset/vehicle0') LIMIT 5"
    );
    let mut f = Fake::default();
    f.answers.push_back(rows(
        &[("id", column_type::LONGLONG), ("u", column_type::LONGLONG)],
        &[&[Some("7"), Some("0")], &[Some("8"), Some("1")]],
    ));
    let Handled::Ok(d) = run(
        "UPDATE truck SET name = 'n', payload_kg = 1 WHERE vin LIKE '1%'",
        &mut f,
    )
    .await
    .unwrap() else {
        panic!()
    };
    assert_eq!(d.affected_rows, 1);
    assert_eq!(d.info, "Rows matched: 2  Changed: 1  Warnings: 0");
    assert_eq!(
        f.log[1],
        "SELECT `truck__r`.`id`, (('n') <=> `truck__r`.`name` AND (1) <=> `truck__x2`.`payload_kg`) \
         FROM (`asset` AS `truck__r` JOIN `vehicle` AS `truck__x1` ON `truck__x1`.`id` = \
         `truck__r`.`id` JOIN `truck` AS `truck__x2` ON `truck__x2`.`id` = `truck__r`.`id`) WHERE \
         `truck__x1`.vin LIKE '1%' FOR UPDATE"
    );
    assert!(
        f.log[2].ends_with("WHERE `truck__r`.`id` IN (7, 8)"),
        "{}",
        f.log[2]
    );
    let e = run("UPDATE truck SET id = 3", &mut f).await.unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39706);
    let e = run("UPDATE vehicle SET _type_path = 'x'", &mut f)
        .await
        .unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39705);
}

#[tokio::test]
async fn deletes_go_deepest_first() {
    let mut f = Fake::default();
    f.answers.push_back(rows(
        &[
            ("id", column_type::LONGLONG),
            ("p", column_type::VAR_STRING),
        ],
        &[
            &[Some("7"), Some("/asset/vehicle/truck/")],
            &[Some("9"), Some("/asset/vehicle/")],
        ],
    ));
    let Handled::Ok(d) = run("DELETE FROM vehicle WHERE site_id = 3", &mut f)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(d.affected_rows, 2);
    assert_eq!(
        &f.log[1..],
        [
            "SELECT `vehicle`.`id`, `vehicle`.`_type_path` FROM `asset` AS `vehicle` WHERE \
             (`vehicle`.site_id = 3) AND (`vehicle`.`_type_path` >= '/asset/vehicle/' AND \
             `vehicle`.`_type_path` < '/asset/vehicle0') FOR UPDATE",
            "DELETE FROM `truck` WHERE `id` IN (7)",
            "DELETE FROM `vehicle` WHERE `id` IN (7, 9)",
            "DELETE FROM `asset` WHERE `id` IN (7, 9)",
            "COMMIT",
        ]
    );
}

#[tokio::test]
async fn hierarchy_writes_fill_paths() {
    let mut f = Fake::default();
    let Handled::Ok(_) = run(
        "INSERT INTO org_unit (id, parent_id) VALUES (42, 7)",
        &mut f,
    )
    .await
    .unwrap() else {
        panic!()
    };
    assert_eq!(
        f.log[1],
        "INSERT INTO org_unit (id, parent_id) VALUES (42, 7)"
    );
    assert!(
        f.log[2].starts_with(
            "UPDATE `org_unit` SET _sprig_hpath = CONCAT('/', CAST(`id` AS CHAR), '/')"
        ),
        "{}",
        f.log[2]
    );
    assert!(
        f.log[3].contains("JOIN `org_unit` AS `p` ON `c`.`parent_id` = `p`.`id`"),
        "{}",
        f.log[3]
    );
    let e = run("UPDATE org_unit SET id = 3 WHERE id = 1", &mut f)
        .await
        .unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39706);
    // A move under its own descendant.
    let mut f = Fake::default();
    f.answers.push_back(rows(
        &[("id", 3), ("p", 3), ("n", 3)],
        &[&[Some("7"), Some("1"), Some("42")]],
    ));
    f.answers.push_back(rows(
        &[("p", 253), ("d", 3), ("rk", 3)],
        &[&[Some("/1/7/"), Some("1"), Some("1")]],
    ));
    f.answers.push_back(rows(
        &[("p", 253), ("d", 3), ("rk", 3)],
        &[&[Some("/1/7/42/"), Some("2"), Some("1")]],
    ));
    let e = run("UPDATE org_unit SET parent_id = 42 WHERE id = 7", &mut f)
        .await
        .unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39711, "{e}");
    assert_eq!(f.log.last().unwrap(), "ROLLBACK");
}

/// Makes `car` resolvable after its CREATE runs.
struct After<'a>(&'a mut Fake);
#[async_trait::async_trait]
impl Runner for After<'_> {
    async fn query(&mut self, sql: &str) -> Result<Rows> {
        self.0.query(sql).await
    }
    async fn exec(&mut self, sql: &str) -> Result<Done> {
        if sql.starts_with("CREATE TABLE car") {
            self.0.ids.push(("car".into(), 5));
            self.0.ids.push(("vehicle".into(), 2));
        }
        self.0.exec(sql).await
    }
    fn in_transaction(&self) -> bool {
        false
    }
    fn found_rows(&self) -> bool {
        false
    }
    async fn object_id(
        &mut self,
        db: Option<&str>,
        t: &str,
    ) -> Result<Option<(DbId, sprig_common::ids::ObjectId)>> {
        self.0.object_id(db, t).await
    }
    async fn commit_types(&mut self, c: &TypesChange) -> Result<()> {
        self.0.commit_types(c).await
    }
}

#[tokio::test]
async fn create_inherits_builds_the_extension_table() {
    let mut f = Fake {
        ids: vec![("asset".into(), 1), ("car".into(), 5)],
        ..Fake::default()
    };
    // `car` doesn't exist yet: the first object_id call must miss.
    f.ids.retain(|(n, _)| n != "car");
    let l = One(Arc::new(assets()));
    let st = state();
    let c = ctx(&l);
    let sql = "CREATE TABLE car (doors INT NOT NULL, KEY (doors)) ENGINE=InnoDB INHERITS (vehicle)";
    let mut a = After(&mut f);
    let Handled::Ok(_) = handle(sql, Some(&st), &c, &mut a).await.unwrap() else {
        panic!()
    };
    assert_eq!(
        f.log,
        [
            "CREATE TABLE car (`id` bigint(20) NOT NULL, doors INT NOT NULL, KEY (doors), PRIMARY KEY \
          (`id`), CONSTRAINT `_sprig_fk_car` FOREIGN KEY (`id`) REFERENCES `vehicle` (`id`)) \
          ENGINE=InnoDB"
        ]
    );
    let change = &f.committed[0];
    let car = &change.upsert_types[0];
    assert_eq!(car.path, "/asset/vehicle/car/");
    assert_eq!(car.depth, 2);
    assert_eq!(car.parent, Some(sprig_common::ids::ObjectId(2)));
    assert_eq!(car.own_fields.len(), 1);
    // Clashes and two parents.
    let e = run(
        "CREATE TABLE bike (vin INT) INHERITS (vehicle)",
        &mut Fake::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39702);
    let e = run(
        "CREATE TABLE bike (x INT) INHERITS (vehicle, building)",
        &mut Fake::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39701);
}

#[tokio::test]
async fn drops_need_cascade_and_go_deepest_first() {
    let e = run("DROP TABLE vehicle", &mut Fake::default())
        .await
        .unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39707);
    let mut f = Fake::default();
    run("DROP TABLE vehicle CASCADE", &mut f).await.unwrap();
    // The rows stay, as the root's (T8, the lead's choice of 12 November).
    assert_eq!(
        f.log,
        [
            "DROP TABLE IF EXISTS `truck`",
            "DROP TABLE IF EXISTS `vehicle`",
            "UPDATE `asset` SET `_type` = 'asset', `_type_path` = '/asset/' WHERE `_type_path` \
             >= '/asset/vehicle/' AND `_type_path` < '/asset/vehicle0' LIMIT 1000",
        ]
    );
    assert_eq!(f.committed.len(), 2);
    assert_eq!(f.committed[1].delete_types.len(), 2);
    // A leaf under a subtype: its rows become the parent's, keeping their
    // rows in the parent's table.
    let mut f = Fake::default();
    run("DROP TABLE truck", &mut f).await.unwrap();
    assert_eq!(
        &f.log[1..],
        [
            "UPDATE `asset` SET `_type` = 'vehicle', `_type_path` = '/asset/vehicle/' WHERE \
             `_type_path` >= '/asset/vehicle/truck/' AND `_type_path` < '/asset/vehicle/truck0' \
             LIMIT 1000",
        ]
    );
}

#[tokio::test]
async fn metadata_shows_logical_tables() {
    let l = One(Arc::new(assets()));
    let c = ctx(&l);
    let db = assets();
    let vehicle = db.get("vehicle", false).unwrap();
    let physical = "CREATE TABLE `vehicle` (\n  `id` bigint(20) NOT NULL,\n  `vin` char(17) NOT NULL,\n  \
                    PRIMARY KEY (`id`),\n  UNIQUE KEY `vin` (`vin`),\n  CONSTRAINT `_sprig_fk_vehicle` \
                    FOREIGN KEY (`id`) REFERENCES `asset` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
    assert_eq!(
        show::logical_create(physical, vehicle, Some("asset"), &c),
        "CREATE TABLE `vehicle` (\n  `vin` char(17) NOT NULL,\n  UNIQUE KEY `vin` (`vin`)\n) \
         INHERITS (`asset`) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    );
    let asset = db.get("asset", false).unwrap();
    let physical = "CREATE TABLE `asset` (\n  `id` bigint(20) NOT NULL,\n  `_type` varchar(64) NOT NULL \
                    DEFAULT 'asset' INVISIBLE,\n  `_type_path` varchar(255) NOT NULL DEFAULT '/asset/' \
                    INVISIBLE,\n  PRIMARY KEY (`id`),\n  KEY `_sprig_type_path` (`_type_path`,`id`)\n) \
                    ENGINE=InnoDB";
    assert_eq!(
        show::logical_create(physical, asset, None, &c),
        "CREATE TABLE `asset` (\n  `id` bigint(20) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB"
    );
    // The logical text reads back as the same type.
    let Ok(grammar::Stmt::CreateTable(ct)) = grammar::classify(
        show::logical_create(
            "CREATE TABLE `vehicle` (\n  `id` bigint(20) NOT NULL,\n  `vin` char(17),\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB",
            vehicle,
            Some("asset"),
            &c,
        )
        .as_bytes(),
        &LexOptions::mariadb(),
    ) else {
        panic!()
    };
    assert_eq!(ct.inherits[0].name, "asset");
}

// Discriminator columns (inheritance plan step 8, Kiran's answer Q2 of 12
// November 2026): `vehicle` typed by its own `vtype` column.

mod discriminator {
    use super::*;
    use crate::model::Storage;
    use crate::model::fleet::fleet;

    async fn run_on(storage: Storage, sql: &str, fake: &mut Fake) -> Result<Handled> {
        let db = fleet(storage);
        let st = TypesState::new(db.iter().map(|t| (**t).clone()).collect(), Vec::new());
        let l = One(Arc::new(db));
        handle(sql, Some(&st), &ctx(&l), fake).await
    }

    fn forwarded(h: Handled) -> String {
        match h {
            Handled::Forward { sql, .. } => sql,
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn single_table_reads_use_the_root_table() {
        let mut f = Fake::default();
        let s = Storage::SingleTable;
        assert_eq!(
            forwarded(run_on(s, "SELECT * FROM truck", &mut f).await.unwrap()),
            "SELECT `truck`.`id`, `truck`.`name`, `truck`.`vtype`, `truck`.`payload_kg` FROM \
             `vehicle` AS `truck` WHERE (`truck`.`_type_path` >= '/vehicle/truck/' AND \
             `truck`.`_type_path` < '/vehicle/truck0')"
        );
        assert_eq!(
            forwarded(
                run_on(s, "SELECT cap FROM smalltruck WHERE id = 3", &mut f)
                    .await
                    .unwrap()
            ),
            "SELECT `smalltruck`.cap FROM `vehicle` AS `smalltruck` WHERE (`smalltruck`.id = 3) \
             AND (`smalltruck`.`_type_path` >= '/vehicle/truck/smalltruck/' AND \
             `smalltruck`.`_type_path` < '/vehicle/truck/smalltruck0')"
        );
        // ONLY on the root keeps its table's every column (what clients
        // and dump tools know), filtered on the type.
        assert_eq!(
            forwarded(
                run_on(s, "SELECT * FROM ONLY vehicle", &mut f)
                    .await
                    .unwrap()
            ),
            "SELECT `vehicle`.* FROM `vehicle` AS `vehicle` WHERE `vehicle`.`_type` = 'vehicle'"
        );
        // A polymorphic read of the root is the plain table.
        assert!(matches!(
            run_on(s, "SELECT * FROM vehicle", &mut f).await.unwrap(),
            Handled::NotTyped
        ));
        assert!(f.log.is_empty());
    }

    /// Dumps of a single-table hierarchy carry every row through the root:
    /// restored, the discriminator types each row again, in whatever order
    /// the subtypes (and their adopted columns) come back.
    #[tokio::test]
    async fn single_table_dumps_go_through_the_root() {
        let mut f = Fake::default();
        let s = Storage::SingleTable;
        assert!(matches!(
            run_on(
                s,
                "SELECT /*!40001 SQL_NO_CACHE */ * FROM `vehicle`",
                &mut f
            )
            .await
            .unwrap(),
            Handled::NotTyped
        ));
        let sql = forwarded(
            run_on(s, "SELECT /*!40001 SQL_NO_CACHE */ * FROM `truck`", &mut f)
                .await
                .unwrap(),
        );
        assert!(
            sql.ends_with("FROM `vehicle` AS `truck` WHERE 1 = 0"),
            "{sql}"
        );
        // Joined storage keeps one row per type (ONLY).
        let sql = forwarded(
            run_on(
                Storage::Joined,
                "SELECT /*!40001 SQL_NO_CACHE */ * FROM `vehicle`",
                &mut f,
            )
            .await
            .unwrap(),
        );
        assert!(sql.contains("`_type` = 'vehicle'"), "{sql}");
        // Single-table storage without a discriminator is refused.
        let e = run(
            "CREATE TABLE plain_root (id INT PRIMARY KEY) WITH (SPRIG_STORAGE = 'single_table')",
            &mut Fake::default(),
        )
        .await
        .unwrap_err();
        assert_eq!(e.protocol().mysql_errno, 39710, "{e}");
    }

    #[tokio::test]
    async fn inserts_set_or_check_the_value() {
        let mut f = Fake::default();
        let s = Storage::SingleTable;
        assert_eq!(
            forwarded(
                run_on(
                    s,
                    "INSERT INTO truck (id, name, payload_kg) VALUES (5, 't', 10.5)",
                    &mut f
                )
                .await
                .unwrap()
            ),
            "INSERT INTO `vehicle` (`id`, `name`, `payload_kg`, `vtype`) VALUES (5, 't', 10.5, \
             'TRUCK')"
        );
        assert_eq!(
            forwarded(
                run_on(s, "INSERT INTO truck VALUES (6, 'u', 'TRUCK', 3)", &mut f)
                    .await
                    .unwrap()
            ),
            "INSERT INTO `vehicle` (`id`, `name`, `vtype`, `payload_kg`) VALUES (6, 'u', 'TRUCK', 3)"
        );
        for bad in [
            "INSERT INTO truck (id, vtype) VALUES (7, 'CAR')",
            "INSERT INTO truck (id, vtype) VALUES (7, 'SMALLTRUCK')",
            "INSERT INTO truck (id, vtype) VALUES (7, CONCAT('TR', 'UCK'))",
        ] {
            let e = run_on(s, bad, &mut f).await.unwrap_err();
            assert_eq!(e.protocol().mysql_errno, 39717, "{bad}: {e}");
        }
        // Any value goes into the root, declared or not.
        assert!(matches!(
            run_on(
                s,
                "INSERT INTO vehicle (id, vtype) VALUES (8, 'MINITRUCK')",
                &mut f
            )
            .await
            .unwrap(),
            Handled::NotTyped
        ));
        // Joined storage writes the value instead of _type and _type_path.
        let mut f = Fake::default();
        run_on(
            Storage::Joined,
            "INSERT INTO smalltruck (id, name, payload_kg, cap) VALUES (9, 's', 1, 2)",
            &mut f,
        )
        .await
        .unwrap();
        assert_eq!(
            f.log,
            [
                "BEGIN",
                "INSERT INTO `vehicle` (`id`, `name`, `vtype`) VALUES (9, 's', 'SMALLTRUCK')",
                "INSERT INTO `truck` (`id`, `payload_kg`) VALUES (9, 1)",
                "INSERT INTO `smalltruck` (`id`, `cap`) VALUES (9, 2)",
                "COMMIT",
            ]
        );
    }

    #[tokio::test]
    async fn joined_root_inserts_and_updates_move_extension_rows() {
        let j = Storage::Joined;
        let mut f = Fake::default();
        // Columns: id, _type_path, then one "has a row" flag per extension
        // table, shallowest first: truck, car, smalltruck.
        let sync_cols = [
            ("id", column_type::LONGLONG),
            ("p", 253),
            ("e0", column_type::LONGLONG),
            ("e1", column_type::LONGLONG),
            ("e2", column_type::LONGLONG),
        ];
        f.answers.push_back(rows(
            &sync_cols,
            &[
                &[
                    Some("9"),
                    Some("/vehicle/truck/"),
                    Some("0"),
                    Some("0"),
                    Some("0"),
                ],
                &[
                    Some("10"),
                    Some("/vehicle/"),
                    Some("0"),
                    Some("0"),
                    Some("0"),
                ],
            ],
        ));
        let h = run_on(
            j,
            "INSERT INTO vehicle (id, name, vtype) VALUES (9, 'x', 'TRUCK'), (10, 'y', 'MINITRUCK')",
            &mut f,
        )
        .await
        .unwrap();
        let Handled::Ok(d) = h else { panic!("{h:?}") };
        assert_eq!(d.affected_rows, 2);
        assert_eq!(f.log[0], "BEGIN");
        assert_eq!(
            f.log[1],
            "INSERT INTO vehicle (id, name, vtype) VALUES (9, 'x', 'TRUCK'), (10, 'y', 'MINITRUCK')"
        );
        assert!(f.log[2].starts_with(
            "SELECT r.`id`, r.`_type_path`, e0.`id` IS NOT NULL, e1.`id` IS NOT NULL, e2.`id` IS \
             NOT NULL FROM `vehicle` AS r LEFT JOIN `truck` AS e0"
        ), "{}", f.log[2]);
        assert_eq!(
            f.log[3..],
            ["INSERT INTO `truck` (`id`) VALUES (9)", "COMMIT"]
        );

        // A truck becomes a car: its truck row goes, a car row comes.
        let mut f = Fake::default();
        f.answers.push_back(rows(
            &[("id", column_type::LONGLONG), ("u", column_type::LONGLONG)],
            &[&[Some("9"), Some("0")]],
        ));
        f.answers.push_back(rows(
            &sync_cols,
            &[&[
                Some("9"),
                Some("/vehicle/car/"),
                Some("1"),
                Some("0"),
                Some("0"),
            ]],
        ));
        let h = run_on(j, "UPDATE truck SET vtype = 'CAR' WHERE id = 9", &mut f)
            .await
            .unwrap();
        let Handled::Ok(d) = h else { panic!("{h:?}") };
        assert_eq!(d.affected_rows, 1);
        assert_eq!(
            f.log[1],
            "SELECT `truck`.`id`, (('CAR') <=> `truck`.`vtype`) FROM `vehicle` AS `truck` WHERE \
             (`truck`.id = 9) AND (`truck`.`_type_path` >= '/vehicle/truck/' AND \
             `truck`.`_type_path` < '/vehicle/truck0') FOR UPDATE"
        );
        assert_eq!(
            f.log[2],
            "UPDATE `vehicle` AS `truck` SET `truck`.`vtype` = 'CAR' WHERE `truck`.`id` IN (9)"
        );
        assert_eq!(
            f.log[4..],
            [
                "DELETE FROM `truck` WHERE `id` IN (9)",
                "INSERT INTO `car` (`id`) VALUES (9)",
                "COMMIT"
            ]
        );
        // In single-table storage the same update is one statement.
        let mut f = Fake::default();
        assert_eq!(
            forwarded(
                run_on(
                    Storage::SingleTable,
                    "UPDATE truck SET vtype = 'CAR' WHERE id = 9",
                    &mut f
                )
                .await
                .unwrap()
            ),
            "UPDATE `vehicle` AS `truck` SET `truck`.`vtype` = 'CAR' WHERE (`truck`.id = 9) AND \
             (`truck`.`_type_path` >= '/vehicle/truck/' AND `truck`.`_type_path` < \
             '/vehicle/truck0')"
        );
        // Single-table deletes touch only the root's table.
        let mut f = Fake::default();
        f.answers.push_back(rows(
            &[("id", column_type::LONGLONG), ("p", 253)],
            &[&[Some("9"), Some("/vehicle/truck/")]],
        ));
        run_on(
            Storage::SingleTable,
            "DELETE FROM truck WHERE id = 9",
            &mut f,
        )
        .await
        .unwrap();
        assert_eq!(
            f.log[2..],
            ["DELETE FROM `vehicle` WHERE `id` IN (9)", "COMMIT"]
        );
    }

    #[test]
    fn show_create_gives_the_where_and_the_roots_options() {
        let db = fleet(Storage::SingleTable);
        let l = One(Arc::new(fleet(Storage::SingleTable)));
        let c = ctx(&l);
        let root_text = "CREATE TABLE `vehicle` (\n  `id` bigint(20) NOT NULL,\n  `name` varchar(50) \
                         DEFAULT NULL,\n  `vtype` varchar(31) NOT NULL,\n  `payload_kg` decimal(10,2) \
                         DEFAULT NULL,\n  `cap` int(11) DEFAULT NULL,\n  `seats` int(11) DEFAULT NULL,\n  \
                         `_type` varchar(64) GENERATED ALWAYS AS (case `vtype` when 'TRUCK' then 'truck' \
                         else 'vehicle' end) VIRTUAL INVISIBLE,\n  PRIMARY KEY (`id`),\n  KEY \
                         `_sprig_type_path` (`_type_path`,`id`)\n) ENGINE=InnoDB";
        let stub = "CREATE TABLE `truck` (\n  `id` bigint(20) NOT NULL,\n  PRIMARY KEY (`id`)\n) \
                    ENGINE=InnoDB";
        let truck = db.get("truck", false).unwrap();
        let shown = show::logical_create_from(
            stub,
            Some(root_text),
            truck,
            Some("vehicle"),
            Some("vtype"),
            &c,
        );
        assert_eq!(
            shown,
            "CREATE TABLE `truck` (\n  `payload_kg` decimal(10,2) DEFAULT NULL\n) INHERITS \
             (`vehicle`) WHERE `vtype` = 'TRUCK' ENGINE=InnoDB"
        );
        let Ok(grammar::Stmt::CreateTable(ct)) =
            grammar::classify(shown.as_bytes(), &LexOptions::mariadb())
        else {
            panic!()
        };
        assert_eq!(
            ct.disc_where,
            Some(("vtype".into(), crate::model::DiscValue::Str("TRUCK".into())))
        );
        let vehicle = db.get("vehicle", false).unwrap();
        let shown = show::logical_create_from(root_text, None, vehicle, None, None, &c);
        assert!(
            shown.ends_with(
                ") ENGINE=InnoDB WITH (SPRIG_STORAGE = 'single_table', SPRIG_DISCRIMINATOR = \
                 'vtype')"
            ),
            "{shown}"
        );
        assert!(
            !shown.contains("_type") && shown.contains("`seats`"),
            "{shown}"
        );
    }

    #[tokio::test]
    #[allow(clippy::items_after_statements)] // the runner sits by its use
    async fn a_later_subtype_adopts_rows_and_columns() {
        // vehicle (single-table, vtype) gets `minitruck (cap2 INT)` under
        // truck: the stub table, the column, then the type columns again
        // with the new value mapped.
        let mut f = Fake {
            ids: vec![("vehicle".into(), 21), ("truck".into(), 22)],
            ..Fake::default()
        };
        f.answers.push_back(rows(
            &[("Table", 253), ("Create Table", 253)],
            &[&[
                Some("vehicle"),
                Some(
                    "CREATE TABLE `vehicle` (\n  `id` bigint(20) NOT NULL,\n  `vtype` varchar(31) \
                     NOT NULL,\n  `wheels` int(11) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) \
                     ENGINE=InnoDB",
                ),
            ]],
        ));
        f.answers
            .push_back(rows(&[("c", 253)], &[&[Some("utf8mb4")]]));
        struct Mini<'a>(&'a mut Fake);
        #[async_trait::async_trait]
        impl Runner for Mini<'_> {
            async fn query(&mut self, sql: &str) -> Result<Rows> {
                self.0.query(sql).await
            }
            async fn exec(&mut self, sql: &str) -> Result<Done> {
                if sql.starts_with("CREATE TABLE minitruck") {
                    self.0.ids.push(("minitruck".into(), 25));
                }
                self.0.exec(sql).await
            }
            fn in_transaction(&self) -> bool {
                false
            }
            fn found_rows(&self) -> bool {
                false
            }
            async fn object_id(
                &mut self,
                db: Option<&str>,
                t: &str,
            ) -> Result<Option<(DbId, sprig_common::ids::ObjectId)>> {
                self.0.object_id(db, t).await
            }
            async fn commit_types(&mut self, c: &TypesChange) -> Result<()> {
                self.0.commit_types(c).await
            }
        }
        let db = fleet(Storage::SingleTable);
        // The root owns `wheels` (a Hibernate SINGLE_TABLE column).
        let mut types: Vec<TypeDef> = db.iter().map(|t| (**t).clone()).collect();
        for t in &mut types {
            if t.name == "vehicle" {
                t.own_fields.push(crate::model::fixtures::field(
                    "wheels", "vehicle", "int(11)",
                ));
            }
        }
        let st = TypesState::new(types.clone(), Vec::new());
        let l = One(Arc::new(crate::model::DbTypes::new(types, Vec::new())));
        let c = ctx(&l);
        let sql =
            "CREATE TABLE minitruck (wheels INT, cap2 INT) INHERITS (truck) WHERE vtype = 'MINI'";
        let mut m = Mini(&mut f);
        handle(sql, Some(&st), &c, &mut m).await.unwrap();
        assert_eq!(f.log[0], "SHOW CREATE TABLE `vehicle`");
        assert_eq!(
            f.log[1],
            "CREATE TABLE minitruck (`id` bigint(20) NOT NULL, PRIMARY KEY (`id`))"
        );
        assert_eq!(f.log[2], "ALTER TABLE `vehicle` ADD COLUMN cap2 INT");
        assert!(
            f.log[3].starts_with("SELECT CHARSET(x.`vtype`)"),
            "{}",
            f.log[3]
        );
        assert_eq!(
            f.log[4],
            "ALTER TABLE `vehicle` DROP INDEX IF EXISTS `_sprig_type_path`"
        );
        assert!(
            f.log[5].contains(
                "AS (CASE (`vtype` COLLATE utf8mb4_bin) WHEN 'TRUCK' THEN 'truck' WHEN \
                 'SMALLTRUCK' THEN 'smalltruck' WHEN 'CAR' THEN 'car' WHEN 'MINI' THEN \
                 'minitruck' ELSE 'vehicle' END) VIRTUAL INVISIBLE"
            ) || f.log[5].contains("WHEN 'MINI' THEN 'minitruck'"),
            "{}",
            f.log[5]
        );
        assert!(f.log[5].contains("WHEN 'MINI' THEN '/vehicle/truck/minitruck/'"));
        assert_eq!(
            f.log[6],
            "ALTER TABLE `vehicle` ADD INDEX IF NOT EXISTS `_sprig_type_path` (`_type_path`, `id`)"
        );
        let change = &f.committed[0];
        let mini = change
            .upsert_types
            .iter()
            .find(|t| t.name == "minitruck")
            .unwrap();
        assert_eq!(
            mini.disc_value,
            Some(crate::model::DiscValue::Str("MINI".into()))
        );
        let own: Vec<_> = mini
            .own_fields
            .iter()
            .map(|f| (f.name.clone(), f.physical_table.clone()))
            .collect();
        assert_eq!(
            own,
            [
                ("wheels".to_owned(), "vehicle".to_owned()),
                ("cap2".to_owned(), "vehicle".to_owned())
            ]
        );
        // The root no longer owns the adopted column.
        let root_fields = change
            .fields
            .iter()
            .find(|(i, _)| i.0 == 21)
            .map(|(_, f)| f.iter().map(|f| f.name.clone()).collect::<Vec<_>>())
            .unwrap();
        assert!(
            !root_fields.contains(&"wheels".to_owned()),
            "{root_fields:?}"
        );
        // A value another subtype has, or no value at all, is refused.
        for (bad, code) in [
            (
                "CREATE TABLE m2 (x INT) INHERITS (truck) WHERE vtype = 'CAR'",
                39717,
            ),
            ("CREATE TABLE m3 (x INT) INHERITS (truck)", 39710),
            (
                "CREATE TABLE m4 (x INT) INHERITS (truck) WHERE kind = 'X'",
                39710,
            ),
        ] {
            let mut f = Fake::default();
            f.answers.push_back(rows(
                &[("Table", 253), ("Create Table", 253)],
                &[&[
                    Some("vehicle"),
                    Some("CREATE TABLE `vehicle` (\n  `id` bigint(20) NOT NULL,\n  PRIMARY KEY (`id`)\n)"),
                ]],
            ));
            let e = run_on(Storage::SingleTable, bad, &mut f).await.unwrap_err();
            assert_eq!(e.protocol().mysql_errno, code, "{bad}: {e}");
        }
    }
}
