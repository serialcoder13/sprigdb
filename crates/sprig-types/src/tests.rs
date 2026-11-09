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
    assert_eq!(f.log[0], "SAVEPOINT sprig_types");
    assert_eq!(
        f.log[1],
        "INSERT INTO `asset` (`id`, `name`, `site_id`, `_type`, `_type_path`) VALUES (9, 'V', 4, \
         'vehicle', '/asset/vehicle/')"
    );
    assert_eq!(f.log.last().unwrap(), "RELEASE SAVEPOINT sprig_types");
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
    assert_eq!(
        f.log,
        [
            "DROP TABLE IF EXISTS `truck`",
            "DROP TABLE IF EXISTS `vehicle`",
            "SELECT `id` FROM `asset` WHERE `_type_path` >= '/asset/vehicle/' AND `_type_path` < \
             '/asset/vehicle0' LIMIT 1000",
        ]
    );
    assert_eq!(f.committed.len(), 2);
    assert_eq!(f.committed[1].delete_types.len(), 2);
    // A leaf under a subtype: its rows leave the parent's table too.
    let mut f = Fake::default();
    f.answers
        .push_back(rows(&[("id", column_type::LONGLONG)], &[&[Some("7")]]));
    run("DROP TABLE truck", &mut f).await.unwrap();
    assert_eq!(
        &f.log[1..4],
        [
            "SELECT `id` FROM `asset` WHERE `_type_path` >= '/asset/vehicle/truck/' AND \
             `_type_path` < '/asset/vehicle/truck0' LIMIT 1000",
            "DELETE FROM `vehicle` WHERE `id` IN (7)",
            "DELETE FROM `asset` WHERE `id` IN (7)",
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
