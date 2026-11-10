//! Metadata of grouped tables (r6three's S1): every path lists a table the
//! advisor moved into a group's schema, once, with its logical names.

use sprig_common::ids::{DbId, GroupId, ShardId, TenantId};
use sprig_proto_mysql::packets::Persona;

use crate::names::map::SchemaName;
use crate::names::{
    AccountInfo, Action, ColumnRegistry, GroupedTables, MemoryNameMap, PlanCtx, RewriteCtx,
    WithGroups, plan,
};

fn base() -> MemoryNameMap {
    MemoryNameMap::new(TenantId(42))
        .with_db(1, "billing", 1, 1)
        .with_db(2, "crm", 2, 2)
}

/// `orders` and `order_items` in group 3 (shards 2 and 5), `countries` a
/// reference table; `invoices` stays in `billing`'s own schema.
fn groups() -> GroupedTables {
    let t = TenantId(42);
    let mut g = GroupedTables::default();
    let g3 = SchemaName::shard(t, DbId(1), GroupId(3), ShardId(2));
    g.add(DbId(1), "orders", g3);
    g.add(DbId(1), "order_items", g3);
    g.add(DbId(1), "countries", SchemaName::reference(t, DbId(1)));
    g
}

fn sql_on(text: &str, persona: Persona, grouped: bool) -> String {
    let m = base();
    let g = groups();
    let w = WithGroups {
        base: &m,
        groups: &g,
    };
    let map: &dyn crate::names::NameMap = if grouped { &w } else { &m };
    let reg = ColumnRegistry::mariadb_11_8();
    let rctx = RewriteCtx::new(map, persona)
        .current_db(Some("billing"))
        .account(AccountInfo {
            user: "alice",
            account_host: "%",
            client_host: "203.0.113.7",
        });
    let p = plan(text.as_bytes(), &PlanCtx::new(rctx, &reg)).unwrap();
    match p.statements.into_iter().next().unwrap().action {
        Action::Forward { sql, .. } => String::from_utf8(sql).unwrap(),
        a => panic!("{a:?}"),
    }
}

fn sql(text: &str) -> String {
    sql_on(text, Persona::MariaDb, true)
}

const SCHEMAS: [&str; 3] = ["'t42_d1_g1_s1'", "'t42_d1_g3_s2'", "'t42_d1_ref'"];

fn reads_every_group(s: &str) {
    for schema in SCHEMAS {
        assert!(s.contains(schema), "{schema} missing from {s}");
    }
    assert_eq!(s.matches("UNION ALL").count(), 2, "{s}");
}

#[test]
fn show_tables_lists_every_group() {
    let s = sql("SHOW TABLES");
    assert!(s.starts_with("SELECT * FROM (SELECT `TABLE_NAME` AS `Tables_in_billing` FROM ("));
    reads_every_group(&s);
    assert!(s.ends_with("ORDER BY CAST(`Tables_in_billing` AS BINARY)"), "{s}");
    // FROM, LIKE (case-sensitive, as MariaDB's) and FULL.
    let s = sql("SHOW FULL TABLES FROM billing LIKE 'o%'");
    reads_every_group(&s);
    assert!(s.contains("AS `Table_type`"), "{s}");
    assert!(s.contains("`TABLE_NAME` LIKE BINARY 'o%'"), "{s}");
    // WHERE stays over the result's column, as the client wrote it.
    let s = sql("SHOW TABLES WHERE `Tables_in_billing` <> 'x';");
    assert!(s.contains("AS `t` WHERE `Tables_in_billing` <> 'x' ORDER BY"), "{s}");
}

#[test]
fn table_status_and_triggers_list_every_group() {
    let s = sql("SHOW TABLE STATUS LIKE 'orders'");
    reads_every_group(&s);
    assert!(s.contains("`TEMPORARY` AS `Temporary`"), "{s}");
    let s = sql_on("SHOW TABLE STATUS", Persona::MySql84, true);
    assert!(s.contains("`TABLE_COMMENT` AS `Comment`"), "{s}");
    assert!(!s.contains("Max_index_length"), "MySQL's 18 columns: {s}");
    let s = sql("SHOW TRIGGERS FROM billing LIKE 'orders'");
    assert!(s.contains("`EVENT_OBJECT_SCHEMA` = 't42_d1_g3_s2'"), "{s}");
    assert!(s.contains("`EVENT_OBJECT_TABLE` LIKE BINARY 'orders'"), "{s}");
    assert!(s.starts_with("SELECT `Trigger`, `Event`, `Table`, `Statement`"), "{s}");
}

#[test]
fn other_databases_and_maps_keep_show() {
    assert_eq!(sql("SHOW TABLES FROM crm"), "SHOW TABLES FROM `t42_d2_g2_s2`");
    assert_eq!(sql_on("SHOW TABLES", Persona::MariaDb, false), "SHOW TABLES");
    assert_eq!(sql("SHOW EVENTS"), "SHOW EVENTS");
}

#[test]
fn one_table_reads_its_groups_schema() {
    for (text, want) in [
        ("SHOW CREATE TABLE orders", "SHOW CREATE TABLE `t42_d1_g3_s2`.orders"),
        ("SHOW CREATE TABLE billing.Orders", "SHOW CREATE TABLE `t42_d1_g3_s2`.Orders"),
        ("SHOW CREATE TABLE countries", "SHOW CREATE TABLE `t42_d1_ref`.countries"),
        ("SHOW CREATE TABLE invoices", "SHOW CREATE TABLE invoices"),
        ("SHOW CREATE TABLE crm.orders", "SHOW CREATE TABLE `t42_d2_g2_s2`.orders"),
        ("SHOW FULL COLUMNS FROM orders", "SHOW FULL COLUMNS FROM `t42_d1_g3_s2`.orders"),
        (
            "SHOW COLUMNS FROM orders FROM billing",
            "SHOW COLUMNS FROM orders FROM `t42_d1_g3_s2`",
        ),
        ("SHOW INDEX FROM billing.order_items", "SHOW INDEX FROM `t42_d1_g3_s2`.order_items"),
        ("DESCRIBE orders", "DESCRIBE `t42_d1_g3_s2`.orders"),
        ("DESC billing.orders id", "DESC `t42_d1_g3_s2`.orders id"),
    ] {
        assert_eq!(sql(text), want, "{text}");
    }
    // 3306 reads MySQL-shaped COLUMNS from the group's schema.
    let s = sql_on("SHOW COLUMNS FROM orders", Persona::MySql84, true);
    assert!(s.contains("'t42_d1_g3_s2'"), "{s}");
    assert!(!s.contains("'t42_d1_g1_s1'"), "{s}");
}

#[test]
fn information_schema_reads_every_group() {
    for table in ["TABLES", "COLUMNS", "STATISTICS", "KEY_COLUMN_USAGE", "TABLE_CONSTRAINTS"] {
        let s = sql(&format!(
            "SELECT * FROM information_schema.{table} WHERE TABLE_SCHEMA = 'billing'"
        ));
        reads_every_group(&s);
        assert!(!s.contains("= 't42_d2_"), "{table}: {s}");
    }
    // A foreign key between grouped tables names its schema logically.
    let s = sql("SELECT REFERENCED_TABLE_SCHEMA FROM information_schema.KEY_COLUMN_USAGE");
    assert!(
        s.contains("WHEN 't42_d1_g3_s2' THEN CONVERT('billing'"),
        "{s}"
    );
    let s = sql_on(
        "SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'billing'",
        Persona::MySql84,
        true,
    );
    reads_every_group(&s);
}
