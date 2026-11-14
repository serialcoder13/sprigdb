//! Automatic indexes are invisible to schema tools and
//! reserved to SprigDB, and the 64-index rule.

#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use sprig_common::ids::TenantId;
use sprig_proto_mysql::ColumnDef;
use sprig_proto_mysql::packets::Persona;

use super::*;
use crate::names::result::TextRow;
use crate::names::show::ShowKind;
use crate::names::{
    Action, ColumnRegistry, MemoryNameMap, OutputCtx, PlanCtx, ResultMapper, RewriteCtx, plan,
};

fn opts() -> LexOptions {
    LexOptions::mariadb()
}

fn ch(sql: &str) -> IndexChanges {
    index_changes(sql.as_bytes(), &opts()).unwrap_or_else(|| panic!("no changes for {sql}"))
}

fn names(v: &[Option<String>]) -> Vec<&str> {
    v.iter().map(|n| n.as_deref().unwrap_or("-")).collect()
}

const HIDDEN: Visibility = Visibility {
    auto_indexes: false,
    internal: false,
    gipk: false,
};
const SHOWN: Visibility = Visibility {
    auto_indexes: true,
    internal: false,
    gipk: false,
};

#[test]
fn index_names_of_every_ddl_shape() {
    let c = ch("CREATE UNIQUE INDEX IF NOT EXISTS `Idx_A` ON t (a)");
    assert_eq!(names(&c.created), ["Idx_A"]);
    let c = ch("create or replace index k on t (a)");
    assert_eq!(names(&c.created), ["k"]);
    let c = ch("DROP INDEX IF EXISTS sprig_auto_00000001 ON t");
    assert_eq!(c.dropped, ["sprig_auto_00000001"]);
    let c = ch(
        "ALTER TABLE shop.orders ADD INDEX (a), ADD KEY k2 (b), ADD UNIQUE KEY IF NOT EXISTS u (c), \
         ADD FULLTEXT ft (d), ADD CONSTRAINT cu UNIQUE (e), ADD PRIMARY KEY (id), \
         ADD CONSTRAINT fk FOREIGN KEY fki (x) REFERENCES p (id), ADD COLUMN `key` int, \
         ADD CONSTRAINT ck CHECK (a > 0), DROP INDEX old, DROP KEY IF EXISTS old2, DROP PRIMARY KEY, \
         RENAME INDEX r1 TO r2, ALTER INDEX v IGNORED",
    );
    assert_eq!(names(&c.created), ["-", "k2", "u", "ft", "cu", "PRIMARY"]);
    assert_eq!(c.constraints, ["cu", "fki", "fk"]);
    assert_eq!(c.dropped, ["old", "old2", "PRIMARY"]);
    assert_eq!(c.renamed, [("r1".to_owned(), "r2".to_owned())]);
    assert_eq!(c.altered, ["v"]);
    assert_eq!(c.net_added(), 3);
    // mysqldump's shape, executable comments included.
    let c = ch(
        "CREATE TABLE `orders` (\n  `id` int(11) NOT NULL,\n  `key` int,\n  PRIMARY KEY (`id`),\n  \
         KEY `by_c` (`customer_id`) /*!80000 INVISIBLE */,\n  UNIQUE KEY `u` (`id`,`key`),\n  \
         CONSTRAINT `fk_c` FOREIGN KEY (`customer_id`) REFERENCES `customers` (`id`)\n) ENGINE=InnoDB \
         /*!40101 DEFAULT CHARSET=utf8mb4 */",
    );
    assert_eq!(names(&c.created), ["PRIMARY", "by_c", "u"]);
    assert_eq!(c.constraints, ["fk_c"]);
    // Not index DDL.
    for sql in [
        "SELECT 1",
        "CREATE VIEW v AS SELECT 1",
        "DROP TABLE t",
        "ALTER DATABASE d CHARACTER SET utf8mb4",
    ] {
        assert!(index_changes(sql.as_bytes(), &opts()).is_none(), "{sql}");
    }
    assert!(ch("ALTER TABLE t ADD COLUMN c int, ENGINE=InnoDB").is_empty());
    assert!(ch("CREATE TABLE t2 LIKE t").is_empty());
}

#[test]
fn reserved_index_names_fail_with_39050() {
    for sql in [
        "CREATE INDEX sprig_auto_5be0c2a1 ON orders (customer_id)",
        "CREATE INDEX SPRIG_AUTO_x ON orders (customer_id)",
        "ALTER TABLE orders ADD KEY `sprig_auto_5be0c2a1` (customer_id)",
        "ALTER TABLE orders ADD CONSTRAINT sprig_auto_u UNIQUE (a)",
        "ALTER TABLE orders RENAME INDEX sprig_auto_5be0c2a1 TO mine",
        "ALTER TABLE orders RENAME INDEX mine TO sprig_auto_5be0c2a1",
        "ALTER TABLE orders ALTER INDEX sprig_auto_5be0c2a1 IGNORED",
        "CREATE TABLE t (a int, KEY sprig_auto_1 (a))",
    ] {
        let e = check_reserved(&ch(sql), SHOWN).unwrap_err();
        assert!(matches!(e, NameError::Reserved(_)), "{sql}");
        assert_eq!(e.errno(), 39050);
    }
    // Dropping one: only while shown.
    let drop = ch("ALTER TABLE orders DROP INDEX sprig_auto_5be0c2a1");
    assert!(check_reserved(&drop, HIDDEN).is_err());
    assert!(check_reserved(&drop, SHOWN).is_ok());
    let internal = Visibility {
        internal: true,
        ..HIDDEN
    };
    assert!(check_reserved(&drop, internal).is_ok());
    // Ordinary names, including ones that merely look alike.
    for sql in [
        "CREATE INDEX sprig_autos ON t (a)",
        "CREATE INDEX my_sprig_auto_1 ON t (a)",
        "ALTER TABLE t ADD INDEX (sprig_auto_col)",
    ] {
        assert!(check_reserved(&ch(sql), HIDDEN).is_ok(), "{sql}");
    }
}

fn acme() -> MemoryNameMap {
    MemoryNameMap::new(TenantId(42)).with_db(1, "shop", 1, 1)
}

fn plan_with(sql: &str, vis: Visibility, internal: bool) -> Result<crate::names::Plan, NameError> {
    let map = acme();
    let reg = ColumnRegistry::mariadb_11_8();
    let mut rctx = RewriteCtx::new(&map, Persona::MariaDb).current_db(Some("shop"));
    rctx.internal = internal;
    plan(sql.as_bytes(), &PlanCtx::new(rctx, &reg).vis(vis))
}

#[test]
fn plan_refuses_reserved_index_ddl() {
    let e = plan_with(
        "CREATE INDEX sprig_auto_5be0c2a1 ON orders (customer_id)",
        HIDDEN,
        false,
    )
    .unwrap_err();
    assert_eq!(e.errno(), 39050);
    assert_eq!(e.to_string(), "'sprig_auto_5be0c2a1' is a reserved name");
    let e = plan_with(
        "ALTER TABLE shop.orders DROP INDEX sprig_auto_5be0c2a1",
        HIDDEN,
        false,
    )
    .unwrap_err();
    assert_eq!(e.errno(), 39050);
    // Shown: the drop goes to MariaDB, rewritten.
    let p = plan_with(
        "ALTER TABLE shop.orders DROP INDEX sprig_auto_5be0c2a1",
        SHOWN,
        false,
    )
    .unwrap();
    match &p.statements[0].action {
        Action::Forward { sql, .. } => assert_eq!(
            String::from_utf8_lossy(sql),
            "ALTER TABLE `t42_d1_g1_s1`.orders DROP INDEX sprig_auto_5be0c2a1"
        ),
        a => panic!("{a:?}"),
    }
    // SprigDB's own sessions build them.
    assert!(
        plan_with(
            "ALTER TABLE shop.orders ADD INDEX IF NOT EXISTS sprig_auto_5be0c2a1 (customer_id)",
            HIDDEN,
            true,
        )
        .is_ok()
    );
    // Ordinary index DDL is untouched.
    assert!(
        plan_with(
            "CREATE INDEX by_customer ON orders (customer_id)",
            HIDDEN,
            false
        )
        .is_ok()
    );
}

#[test]
fn the_session_flag() {
    let f = |s: &str| session_flag(s.as_bytes(), &opts());
    for (sql, v) in [
        ("SET @sprig_show_auto_indexes = 1", true),
        ("set @SPRIG_SHOW_AUTO_INDEXES:=0", false),
        ("SET @`sprig_show_auto_indexes` = ON", true),
        ("SET @'sprig_show_auto_indexes' = 'off'", false),
        ("SET @sprig_show_auto_indexes = TRUE;", true),
        ("/* tool */ SET @sprig_show_auto_indexes = '1'", true),
    ] {
        assert_eq!(f(sql).unwrap().unwrap(), v, "{sql}");
    }
    for sql in [
        "SET @a = 1",
        "SET @sprig_show_internal = 1",
        "SELECT @sprig_show_auto_indexes",
        "SET sql_mode = ''",
    ] {
        assert!(f(sql).is_none(), "{sql}");
    }
    for sql in [
        "SET @a = 1, @sprig_show_auto_indexes = 1",
        "SET @sprig_show_auto_indexes = 2",
        "SET @sprig_show_auto_indexes = @x",
    ] {
        assert_eq!(f(sql).unwrap().unwrap_err().errno(), 39051, "{sql}");
    }
}

fn auto(name: &str, value: Option<f64>) -> ExistingIndex {
    ExistingIndex {
        name: name.to_owned(),
        value,
    }
}

#[test]
fn the_64_index_rule() {
    let mut existing: Vec<ExistingIndex> = (0..61).map(|i| auto(&format!("u{i}"), None)).collect();
    existing.push(auto("sprig_auto_0000000a", Some(900.0)));
    existing.push(auto("sprig_auto_0000000b", Some(120.0)));
    assert!(make_room(&existing, 1).is_empty(), "64 fits");
    assert_eq!(make_room(&existing, 2), ["sprig_auto_0000000b"]);
    // An index no catalog row explains goes first.
    existing.push(auto("sprig_auto_0000000c", None));
    assert_eq!(make_room(&existing, 1), ["sprig_auto_0000000c"]);
    assert_eq!(
        make_room(&existing, 3),
        [
            "sprig_auto_0000000c",
            "sprig_auto_0000000b",
            "sprig_auto_0000000a"
        ]
    );
    // Not enough automatic indexes to make room: drop nothing, MariaDB
    // refuses the statement with 1069.
    assert!(make_room(&existing, 4).is_empty());
    // Ties by name.
    let tied = vec![
        auto("sprig_auto_00000002", Some(1.0)),
        auto("sprig_auto_00000001", Some(1.0)),
    ];
    let mut full: Vec<ExistingIndex> = (0..62).map(|i| auto(&format!("u{i}"), None)).collect();
    full.extend(tied);
    assert_eq!(make_room(&full, 1), ["sprig_auto_00000001"]);
    assert_eq!(ch("CREATE INDEX k ON t (a)").net_added(), 1);
    assert_eq!(
        ch("ALTER TABLE t DROP INDEX a, ADD INDEX b (x), ADD INDEX c (y)").net_added(),
        1
    );
}

// ---- hiding, end to end through the result mapper ----

fn def(name: &str) -> ColumnDef {
    ColumnDef {
        schema: Bytes::new(),
        table: Bytes::new(),
        org_table: Bytes::new(),
        name: Bytes::from(name.to_owned()),
        org_name: Bytes::new(),
        ext_type: None,
        charset: 224,
        length: 256,
        col_type: 253,
        flags: 0,
        decimals: 0,
    }
}

fn row(vals: &[&str]) -> TextRow {
    vals.iter()
        .map(|v| Some(Bytes::from((*v).to_owned())))
        .collect()
}

fn mapped(kind: ShowKind, vis: Visibility, cols: &[&str], rows: &[&[&str]]) -> Vec<Vec<String>> {
    let m = acme();
    let ctx = OutputCtx::new(&m);
    let mut r = ResultMapper::show(kind, vis);
    let mut defs: Vec<ColumnDef> = cols.iter().map(|c| def(c)).collect();
    r.column_defs(&mut defs, &ctx);
    let mut out = Vec::new();
    for v in rows {
        r.row(row(v), &ctx, &mut out);
    }
    r.finish(&ctx, &mut out);
    out.iter()
        .map(|r| {
            r.iter()
                .map(|v| String::from_utf8_lossy(v.as_deref().unwrap_or_default()).into_owned())
                .collect()
        })
        .collect()
}

#[test]
fn show_index_hides_automatic_indexes_unless_shown() {
    let rows: &[&[&str]] = &[
        &["orders", "PRIMARY", "id"],
        &["orders", "sprig_auto_5be0c2a1", "customer_id"],
        &["orders", "sprig_auto_5be0c2a1", "created_at"],
        &["orders", "by_status", "status"],
    ];
    let cols = ["Table", "Key_name", "Column_name"];
    let hidden = mapped(ShowKind::Index, HIDDEN, &cols, rows);
    assert_eq!(
        hidden.iter().map(|r| r[1].as_str()).collect::<Vec<_>>(),
        ["PRIMARY", "by_status"]
    );
    let shown = mapped(ShowKind::Index, SHOWN, &cols, rows);
    assert_eq!(shown.len(), 4);
}

const ORDERS: &str = "CREATE TABLE `orders` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `customer_id` bigint(20) NOT NULL,
  `status` varchar(16) NOT NULL,
  `created_at` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `by_status` (`status`),
  KEY `sprig_auto_5be0c2a1` (`customer_id`,`created_at`),
  KEY `SPRIG_AUTO_0000beef` (`created_at`) IGNORED
) ENGINE=InnoDB AUTO_INCREMENT=101 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

#[test]
fn show_create_table_and_dumps_see_no_drift() {
    // What mysqldump, Flyway, Hibernate's validator and every ORM read.
    let out = crate::names::filter_create_table(ORDERS, &HIDDEN);
    let want = "CREATE TABLE `orders` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `customer_id` bigint(20) NOT NULL,
  `status` varchar(16) NOT NULL,
  `created_at` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `by_status` (`status`)
) ENGINE=InnoDB AUTO_INCREMENT=101 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
    assert_eq!(out, want);
    // The same text with the automatic indexes gone is a fixed point: a
    // second dump of a restored table is identical.
    assert_eq!(crate::names::filter_create_table(&out, &HIDDEN), want);
    assert_eq!(crate::names::filter_create_table(ORDERS, &SHOWN), ORDERS);
    // Through the SHOW CREATE TABLE mapper (the value is column 2).
    let rows: &[&[&str]] = &[&["orders", ORDERS]];
    let hidden = mapped(
        ShowKind::CreateTable,
        HIDDEN,
        &["Table", "Create Table"],
        rows,
    );
    assert_eq!(hidden[0][1], want);
    let shown = mapped(
        ShowKind::CreateTable,
        SHOWN,
        &["Table", "Create Table"],
        rows,
    );
    assert_eq!(shown[0][1], ORDERS);
}

#[test]
fn create_table_like_never_copies_automatic_indexes() {
    let sql = crate::names::create_like_sql("CREATE TABLE `t42_d1_g1_s1`.`orders_copy` ", ORDERS)
        .unwrap();
    assert!(!sql.to_ascii_lowercase().contains("sprig_auto_"), "{sql}");
    assert!(sql.contains("KEY `by_status` (`status`)"));
    assert!(!sql.contains("AUTO_INCREMENT=101"));
    // The planner turns LIKE into CreateLike whatever the session sees.
    for vis in [HIDDEN, SHOWN] {
        let p = plan_with("CREATE TABLE orders_copy LIKE orders", vis, false).unwrap();
        assert!(matches!(p.statements[0].action, Action::CreateLike { .. }));
    }
}

#[test]
fn statistics_hides_automatic_indexes_unless_shown() {
    let sql = |vis| match &plan_with(
        "SELECT INDEX_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = 'shop'",
        vis,
        false,
    )
    .unwrap()
    .statements[0]
        .action
    {
        Action::Forward { sql, .. } => String::from_utf8_lossy(sql).into_owned(),
        a => panic!("{a:?}"),
    };
    assert!(sql(HIDDEN).contains("LEFT(`INDEX_NAME`, 11) <> 'sprig_auto_'"));
    assert!(!sql(SHOWN).contains("sprig_auto_"));
}
