//! The leak suite (chapter 07 build step 10, started here): every
//! captured MariaDB output and message goes through the plan's mapper for
//! the statement a client of tenant 9999 would have sent, and no byte the
//! client sees may hold a physical name. Local answers are checked too.

#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use common::fixture::Captured;
use common::leaks::{assert_result_clean, check_messages};
use sprig_common::ids::TenantId;
use sprig_proto_mysql::packets::Persona;
use sprig_sql::names::{
    Action, ColumnRegistry, MemoryNameMap, OutputCtx, PlanCtx, ProcessRow, RewriteCtx,
    SessionSource, plan,
};

fn tenant() -> MemoryNameMap {
    MemoryNameMap::new(TenantId(9999))
        .with_db(1, "billing", 1, 1)
        .with_db(2, "crm", 2, 2)
}

/// Client statement → the fixture holding MariaDB's answer to its rewrite.
const CORPUS: &[(&str, &str)] = &[
    ("SHOW TABLES", "tables"),
    ("SHOW FULL TABLES FROM billing", "full_tables"),
    ("SHOW TABLES LIKE '%o%'", "tables_like"),
    (
        "SHOW TABLES FROM billing WHERE Tables_in_billing LIKE '%s%'",
        "tables_where",
    ),
    ("SHOW TABLE STATUS FROM billing", "table_status"),
    ("SHOW COLUMNS FROM orders", "columns_orders"),
    (
        "SHOW FULL COLUMNS FROM billing.customers",
        "full_columns_customers",
    ),
    ("SHOW COLUMNS FROM gipk", "columns_gipk"),
    ("DESCRIBE orders", "describe_orders"),
    ("SHOW INDEX FROM orders", "index_orders"),
    ("SHOW KEYS FROM customers", "keys_customers"),
    ("SHOW INDEX FROM gipk", "index_gipk"),
    ("SHOW CREATE TABLE orders", "create_table_orders"),
    ("SHOW CREATE TABLE customers", "create_table_customers"),
    ("SHOW CREATE TABLE gipk", "create_table_gipk"),
    ("SHOW CREATE TABLE parts", "create_table_parts"),
    ("SHOW CREATE TABLE v_orders", "create_table_view"),
    ("SHOW CREATE VIEW billing.v_orders", "create_view"),
    ("SHOW CREATE PROCEDURE p_count", "create_procedure"),
    ("SHOW CREATE FUNCTION f_double", "create_function"),
    ("SHOW CREATE TRIGGER trg_orders", "create_trigger"),
    ("SHOW CREATE EVENT ev_purge", "create_event"),
    ("SHOW CREATE DATABASE billing", "create_database"),
    ("SHOW TRIGGERS FROM billing", "triggers"),
    ("SHOW EVENTS FROM billing", "events"),
    ("SHOW PROCEDURE STATUS", "procedure_status"),
    ("SHOW FUNCTION STATUS", "function_status"),
    ("SHOW OPEN TABLES FROM billing", "open_tables"),
    ("SHOW WARNINGS", "warnings"),
    ("CHECK TABLE orders, crm.accounts", "check_table"),
];

struct Sessions;

impl SessionSource for Sessions {
    fn processes(&self) -> Vec<ProcessRow> {
        vec![ProcessRow {
            id: 1,
            user: "alice".into(),
            host: "203.0.113.7:1".into(),
            db: Some("billing".into()),
            command: "Query".into(),
            info: Some("SHOW PROCESSLIST".into()),
            ..ProcessRow::default()
        }]
    }
}

#[test]
fn no_physical_name_reaches_the_client() {
    let map = tenant();
    let registry = ColumnRegistry::mariadb_11_8();
    let octx = OutputCtx::new(&map);
    for persona in [Persona::MariaDb, Persona::MySql84] {
        let rctx = RewriteCtx::new(&map, persona).current_db(Some("billing"));
        let pctx = PlanCtx::new(rctx, &registry).sessions(&Sessions);
        for (sql, fixture) in CORPUS {
            let p = plan(sql.as_bytes(), &pctx).unwrap_or_else(|e| panic!("{sql}: {e}"));
            let Action::Forward { mapper, .. } = &p.single().unwrap().action else {
                panic!("{sql}: not forwarded");
            };
            let cap = Captured::load(fixture);
            let mut defs = cap.columns.clone();
            let mut m = mapper.clone();
            let rows = m.map_all(&mut defs, cap.rows, &octx);
            assert_result_clean(sql, &defs, &rows);
        }
        for sql in [
            "SHOW DATABASES",
            "SHOW SCHEMAS LIKE 'b%'",
            "SHOW FULL PROCESSLIST",
        ] {
            let p = plan(sql.as_bytes(), &pctx).unwrap();
            let Action::Local(a) = &p.single().unwrap().action else {
                panic!("{sql}: not local");
            };
            assert_result_clean(sql, &a.columns, &a.rows);
        }
    }
    let msgs = check_messages(&octx);
    assert!(msgs.len() >= 7);
    assert_eq!(msgs[0], "Table 'billing.nope' doesn't exist");
    assert_eq!(msgs[1], "Unknown column 'billing.orders.nope' in 'SELECT'");
    assert!(msgs[2].starts_with(
        "Cannot add or update a child row: a foreign key constraint fails (`billing`.`orders`,"
    ));
    assert_eq!(msgs[4], "Table '?.missing' doesn't exist");
    assert_eq!(msgs[5], "PROCEDURE billing.nope does not exist");
}
