//! Chapter 07 steps 4, 5, 7 and 8 against a real MariaDB: the `SHOW`
//! family through the result mappers, the `information_schema` inline
//! views, error messages, and the leak check on every byte a client would
//! see. Skipped unless the Compose single node is named:
//!
//! ```sh
//! export SPRIG_TEST_MARIADB_SINGLE=mysql://root:sprig-dev-root@127.0.0.1:13301
//! cargo test -p sprig-sql --test names_live -- --nocapture
//! # refresh the captured fixtures under tests/fixtures/names/:
//! SPRIG_GOLDEN_CAPTURE=1 cargo test -p sprig-sql --test names_live
//! ```
//!
//! The test creates the `t9999_*` schemas of `tests/fixtures/names/setup.sql`
//! and drops them at the end, also on failure.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    missing_docs
)]

mod common;

use std::collections::BTreeSet;

use common::fixture::{Captured, row_strings};
use sprig_common::ids::TenantId;
use sprig_proto_mysql::client::{ClientConfig, MysqlClient, TlsMode, TlsOptions};
use sprig_proto_mysql::packets::Persona;
use sprig_sql::names::{
    Action, ColumnRegistry, MemoryNameMap, OutputCtx, PlanCtx, ProcessRow, RewriteCtx,
    SessionSource, TextRow, create_like_sql, map_message, physical_name_leaks, plan,
};

const ENV: &str = "SPRIG_TEST_MARIADB_SINGLE";

fn tenant() -> MemoryNameMap {
    MemoryNameMap::new(TenantId(9999))
        .with_db(1, "billing", 1, 1)
        .with_db(2, "crm", 2, 2)
}

async fn connect(url: &str, db: Option<&str>) -> MysqlClient {
    let rest = url.strip_prefix("mysql://").expect("mysql:// URL");
    let (cred, addr) = rest.rsplit_once('@').expect("user@host");
    let (user, pass) = cred.split_once(':').unwrap_or((cred, ""));
    let mut cfg = ClientConfig::new(user, pass);
    cfg.tls = TlsMode::Preferred;
    cfg.tls_config = Some(
        TlsOptions {
            skip_verify: true,
            ..TlsOptions::default()
        }
        .build()
        .unwrap(),
    );
    cfg.database = db.map(str::to_owned);
    MysqlClient::connect(addr.trim_end_matches('/'), &cfg)
        .await
        .unwrap()
}

async fn run_script(c: &mut MysqlClient, file: &str) {
    let text = std::fs::read_to_string(common::fixture::dir().join(file)).unwrap();
    for line in text.lines() {
        let l = line.trim();
        if l.is_empty() || l.starts_with("--") {
            continue;
        }
        c.query_drop(l.trim_end_matches(';'))
            .await
            .unwrap_or_else(|e| panic!("{l}: {e}"));
    }
}

async fn fetch(c: &mut MysqlClient, sql: &str) -> Captured {
    let rs = c
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .find(|r| !r.columns.is_empty())
        .unwrap_or_else(|| panic!("{sql}: no resultset"));
    Captured {
        sql: sql.to_owned(),
        rows: rs.text_rows().unwrap(),
        columns: rs.columns,
    }
}

/// The SHOW outputs the offline tests replay, as MariaDB answers the
/// rewritten statements. Rows of the whole server are kept only where the
/// fixture filters them to `t9999`.
const CAPTURES: &[(&str, &str)] = &[
    ("databases", "SHOW DATABASES LIKE 'information\\_schema'"),
    ("tables", "SHOW TABLES"),
    ("full_tables", "SHOW FULL TABLES FROM `t9999_d1_g1_s1`"),
    ("tables_like", "SHOW TABLES LIKE '%o%'"),
    (
        "tables_where",
        "SHOW TABLES FROM `t9999_d1_g1_s1` WHERE `Tables_in_t9999_d1_g1_s1` LIKE '%s%'",
    ),
    ("table_status", "SHOW TABLE STATUS FROM `t9999_d1_g1_s1`"),
    ("columns_orders", "SHOW COLUMNS FROM orders"),
    (
        "full_columns_customers",
        "SHOW FULL COLUMNS FROM `t9999_d1_g1_s1`.customers",
    ),
    ("columns_gipk", "SHOW COLUMNS FROM gipk"),
    ("describe_orders", "DESCRIBE orders"),
    ("index_orders", "SHOW INDEX FROM orders"),
    ("keys_customers", "SHOW KEYS FROM customers"),
    ("index_gipk", "SHOW INDEX FROM gipk"),
    ("create_table_orders", "SHOW CREATE TABLE orders"),
    ("create_table_customers", "SHOW CREATE TABLE customers"),
    ("create_table_gipk", "SHOW CREATE TABLE gipk"),
    ("create_table_parts", "SHOW CREATE TABLE parts"),
    ("create_table_view", "SHOW CREATE TABLE v_orders"),
    ("create_view", "SHOW CREATE VIEW `t9999_d1_g1_s1`.v_orders"),
    ("create_procedure", "SHOW CREATE PROCEDURE p_count"),
    ("create_function", "SHOW CREATE FUNCTION f_double"),
    ("create_trigger", "SHOW CREATE TRIGGER trg_orders"),
    ("create_event", "SHOW CREATE EVENT ev_purge"),
    ("create_database", "SHOW CREATE DATABASE `t9999_d1_g1_s1`"),
    ("triggers", "SHOW TRIGGERS FROM `t9999_d1_g1_s1`"),
    ("events", "SHOW EVENTS FROM `t9999_d1_g1_s1`"),
    (
        "procedure_status",
        "SHOW PROCEDURE STATUS WHERE Db LIKE 't9999\\_%'",
    ),
    (
        "function_status",
        "SHOW FUNCTION STATUS WHERE Db LIKE 't9999\\_%'",
    ),
    ("open_tables", "SHOW OPEN TABLES FROM `t9999_d1_g1_s1`"),
    ("processlist", "SHOW PROCESSLIST"),
    (
        "check_table",
        "CHECK TABLE orders, `t9999_d2_g2_s2`.accounts",
    ),
];

/// Statements whose errors and warnings the message tests replay.
const ERRORS: &[&str] = &[
    "SELECT * FROM `t9999_d1_g1_s1`.nope",
    "SELECT `t9999_d1_g1_s1`.orders.nope FROM `t9999_d1_g1_s1`.orders",
    "INSERT INTO `t9999_d1_g1_s1`.orders (id, customer_id) VALUES (1, 42)",
    "DROP TABLE `t9999_d1_g1_s1`.nope",
    "SELECT * FROM `t9999_d7_g7_s7`.missing",
    "CALL `t9999_d1_g1_s1`.nope()",
    "INSERT INTO `t9999_d1_g1_s1`.__sprig_lk_orders__email VALUES ('a'), ('a')",
];

async fn capture(url: &str) {
    let mut c = connect(url, Some("t9999_d1_g1_s1")).await;
    // Open the tables so SHOW OPEN TABLES has rows.
    c.query_drop("SELECT COUNT(*) FROM orders JOIN customers JOIN v_orders")
        .await
        .unwrap();
    for (name, sql) in CAPTURES {
        let mut cap = fetch(&mut c, sql).await;
        if *name == "processlist" {
            cap.rows.clear();
        }
        cap.save(name);
    }
    let mut errors = Vec::new();
    for sql in ERRORS {
        let e = c.query(sql).await.expect_err(sql);
        let e = e.server_error().expect("server error").clone();
        errors.push(serde_json::json!({ "sql": sql, "errno": e.code, "message": e.message }));
    }
    // A warning with a physical name.
    c.query_drop("DROP TABLE IF EXISTS `t9999_d1_g1_s1`.nope")
        .await
        .unwrap();
    fetch(&mut c, "SHOW WARNINGS").await.save("warnings");
    let path = common::fixture::dir().join("errors.json");
    std::fs::write(path, serde_json::to_string_pretty(&errors).unwrap() + "\n").unwrap();
    c.quit().await;
}

struct Sessions;

impl SessionSource for Sessions {
    fn processes(&self) -> Vec<ProcessRow> {
        vec![ProcessRow {
            id: 7,
            user: "alice".into(),
            host: "203.0.113.7:50000".into(),
            db: Some("billing".into()),
            command: "Query".into(),
            time: 0,
            time_ms: 1,
            state: None,
            info: Some("SELECT * FROM information_schema.PROCESSLIST".into()),
        }]
    }
}

/// What the client would see for `sql` in a session of tenant 9999 in
/// `billing`: column names and rows, after plan, MariaDB and the mapper.
async fn client_view(
    c: &mut MysqlClient,
    map: &MemoryNameMap,
    persona: Persona,
    sql: &str,
) -> Result<(Vec<String>, Vec<TextRow>), String> {
    let registry = ColumnRegistry::mariadb_11_8();
    let rctx = RewriteCtx::new(map, persona).current_db(Some("billing"));
    let pctx = PlanCtx::new(rctx, &registry).sessions(&Sessions);
    let p = plan(sql.as_bytes(), &pctx).map_err(|e| format!("ERR {} {e}", e.errno()))?;
    let s = p.single().expect("one statement");
    let octx = OutputCtx::new(map);
    match &s.action {
        Action::Local(a) => Ok((
            a.columns
                .iter()
                .map(|c| String::from_utf8_lossy(&c.name).into_owned())
                .collect(),
            a.rows.clone(),
        )),
        Action::Forward { sql: fwd, mapper } => {
            let fwd = String::from_utf8(fwd.clone()).unwrap();
            let rs = match c.query(&fwd).await {
                Ok(rs) => rs,
                Err(e) => {
                    let e = e.server_error().expect("server error").clone();
                    return Err(format!("ERR {} {}", e.code, map_message(&e.message, &octx)));
                }
            };
            let rs = rs
                .into_iter()
                .find(|r| !r.columns.is_empty())
                .unwrap_or_default();
            let mut defs = rs.columns.clone();
            let mut m = mapper.clone();
            let rows = m.map_all(&mut defs, rs.text_rows().unwrap(), &octx);
            for d in &defs {
                for f in [&d.schema, &d.table, &d.org_table, &d.name, &d.org_name] {
                    assert!(physical_name_leaks(f).is_empty(), "{sql}: column def {d:?}");
                }
            }
            Ok((
                defs.iter()
                    .map(|c| String::from_utf8_lossy(&c.name).into_owned())
                    .collect(),
                rows,
            ))
        }
        Action::CreateLike { .. } => Err("CREATE LIKE".into()),
    }
}

fn assert_no_leaks(sql: &str, rows: &[TextRow]) {
    for r in rows {
        for v in r.iter().flatten() {
            let leaks = physical_name_leaks(v);
            assert!(
                leaks.is_empty(),
                "{sql}: leaked {leaks:?} in {:?}",
                String::from_utf8_lossy(v)
            );
        }
    }
}

fn column(names: &[String], rows: &[TextRow], name: &str) -> Vec<String> {
    let i = names
        .iter()
        .position(|n| n.eq_ignore_ascii_case(name))
        .unwrap_or_else(|| panic!("no column {name} in {names:?}"));
    rows.iter().map(|r| row_strings(r)[i].clone()).collect()
}

fn set(v: &[String]) -> BTreeSet<String> {
    v.iter().cloned().collect()
}

async fn show_end_to_end(c: &mut MysqlClient) {
    let map = tenant();
    let mdb = Persona::MariaDb;
    let view = async |c: &mut MysqlClient, sql: &str| {
        let (names, rows) = client_view(c, &map, mdb, sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_no_leaks(sql, &rows);
        (names, rows)
    };

    let (n, r) = view(c, "SHOW DATABASES").await;
    assert_eq!(n, ["Database"]);
    assert_eq!(
        column(&n, &r, "Database"),
        ["billing", "crm", "information_schema"]
    );
    let (n, r) = view(c, "SHOW DATABASES WHERE `Database` LIKE '%i%'").await;
    assert_eq!(n, ["Database"]);
    assert_eq!(
        column(&n, &r, "Database"),
        ["billing", "information_schema"]
    );

    let (n, r) = view(c, "SHOW FULL TABLES").await;
    assert_eq!(n[0], "Tables_in_billing");
    assert_eq!(
        set(&column(&n, &r, "Tables_in_billing")),
        set(&["customers", "gipk", "orders", "parts", "v_orders"].map(String::from))
    );
    let (n, r) = view(c, "SHOW TABLES FROM crm WHERE Tables_in_crm LIKE 'a%'").await;
    assert_eq!(n, ["Tables_in_crm"]);
    assert_eq!(column(&n, &r, "Tables_in_crm"), ["accounts"]);
    let (n, _) = view(c, "SHOW TABLES LIKE 'o%'").await;
    assert_eq!(n, ["Tables_in_billing (o%)"]);

    let (n, r) = view(c, "SHOW TABLE STATUS").await;
    assert!(
        !column(&n, &r, "Name")
            .iter()
            .any(|t| t.starts_with("__sprig"))
    );

    let (n, r) = view(c, "SHOW FULL COLUMNS FROM billing.orders").await;
    assert_eq!(
        column(&n, &r, "Field"),
        ["id", "customer_id", "account_id", "total", "created_at"]
    );
    let (n, r) = view(c, "DESCRIBE customers").await;
    assert_eq!(column(&n, &r, "Field"), ["id", "name", "email"]);
    let (n, r) = view(c, "SHOW COLUMNS FROM gipk").await;
    assert_eq!(column(&n, &r, "Field"), ["v"]);
    let (n, r) = client_view(c, &map, Persona::MySql84, "SHOW COLUMNS FROM gipk")
        .await
        .unwrap();
    assert_eq!(column(&n, &r, "Field"), ["my_row_id", "v"]);

    // idx_mixed has a hidden column, so it goes; it also backs fk_acct,
    // so MariaDB made no fk_acct index.
    let (n, r) = view(c, "SHOW INDEX FROM orders").await;
    assert_eq!(column(&n, &r, "Key_name"), ["PRIMARY"]);
    let (n, r) = view(c, "SHOW KEYS FROM customers").await;
    assert_eq!(
        set(&column(&n, &r, "Key_name")),
        set(&["PRIMARY", "email"].map(String::from))
    );

    let (n, r) = view(c, "SHOW CREATE TABLE orders").await;
    let text = &column(&n, &r, "Create Table")[0];
    assert!(text.contains("REFERENCES `crm`.`accounts`"), "{text}");
    for hidden in [
        "_sprig_",
        "sprig_auto_",
        "idx_status",
        "idx_mixed",
        "chk_status",
    ] {
        assert!(!text.contains(hidden), "{hidden} in {text}");
    }
    assert!(
        text.contains("CONSTRAINT `chk_total` CHECK (`total` >= 0)\n)"),
        "{text}"
    );

    let (n, r) = view(c, "SHOW CREATE VIEW v_orders").await;
    let text = &column(&n, &r, "Create View")[0];
    assert!(
        text.contains("`billing`.`orders`") && text.contains("`crm`.`accounts`"),
        "{text}"
    );
    let (n, r) = view(c, "SHOW CREATE TRIGGER trg_orders").await;
    assert!(column(&n, &r, "SQL Original Statement")[0].contains("`crm`.accounts"));
    let (n, r) = view(c, "SHOW CREATE EVENT ev_purge").await;
    assert!(column(&n, &r, "Create Event")[0].contains("`billing`.orders"));
    let (n, r) = view(c, "SHOW CREATE PROCEDURE p_count").await;
    assert!(column(&n, &r, "Create Procedure")[0].contains("`billing`.orders"));
    let (n, r) = view(c, "SHOW CREATE DATABASE billing").await;
    assert_eq!(column(&n, &r, "Database"), ["billing"]);
    assert!(column(&n, &r, "Create Database")[0].starts_with("CREATE DATABASE `billing`"));

    let (n, r) = view(c, "SHOW TRIGGERS").await;
    assert_eq!(column(&n, &r, "Trigger"), ["trg_orders"]);
    let (n, r) = view(c, "SHOW EVENTS").await;
    assert_eq!(column(&n, &r, "Name"), ["ev_purge"]);
    assert_eq!(column(&n, &r, "Db"), ["billing"]);
    let (n, r) = view(c, "SHOW PROCEDURE STATUS").await;
    assert_eq!(column(&n, &r, "Name"), ["p_count"]);
    assert_eq!(column(&n, &r, "Db"), ["billing"]);
    let (n, r) = view(c, "SHOW FUNCTION STATUS").await;
    assert_eq!(column(&n, &r, "Name"), ["f_double"]);
    let (n, r) = view(c, "SHOW OPEN TABLES").await;
    // Without FROM, MariaDB lists the whole server's open tables; only the
    // tenant's stay.
    assert!(
        column(&n, &r, "Database")
            .iter()
            .all(|d| d == "billing" || d == "crm")
    );

    let (n, r) = view(c, "SHOW PROCESSLIST").await;
    assert_eq!(column(&n, &r, "db"), ["billing"]);

    let (n, r) = view(c, "CHECK TABLE orders, crm.accounts").await;
    assert_eq!(column(&n, &r, "Table"), ["billing.orders", "crm.accounts"]);

    let e = client_view(c, &map, mdb, "SHOW MASTER STATUS")
        .await
        .unwrap_err();
    assert!(e.starts_with("ERR 1227"), "{e}");

    // Messages.
    for (sql, want) in [
        (
            "SELECT * FROM billing.nope",
            "ERR 1146 Table 'billing.nope' doesn't exist",
        ),
        (
            "INSERT INTO billing.orders (id, customer_id) VALUES (1, 42)",
            "`billing`.`orders`",
        ),
    ] {
        let e = client_view(c, &map, mdb, sql).await.unwrap_err();
        assert!(e.contains(want), "{sql}: {e}");
        assert!(physical_name_leaks(e.as_bytes()).is_empty(), "{e}");
    }
}

async fn create_like_end_to_end(c: &mut MysqlClient) {
    let map = tenant();
    let registry = ColumnRegistry::mariadb_11_8();
    let rctx = RewriteCtx::new(&map, Persona::MariaDb).current_db(Some("billing"));
    let pctx = PlanCtx::new(rctx, &registry);
    let p = plan(b"CREATE TABLE orders_copy LIKE billing.orders", &pctx).unwrap();
    let Action::CreateLike { show_sql, head } = &p.single().unwrap().action else {
        panic!("not a CREATE LIKE plan");
    };
    let show = String::from_utf8(show_sql.clone()).unwrap();
    assert_eq!(show, "SHOW CREATE TABLE `t9999_d1_g1_s1`.orders");
    let cap = fetch(c, &show).await;
    let text = String::from_utf8(cap.rows[0][1].clone().unwrap().to_vec()).unwrap();
    let sql = create_like_sql(head, &text).unwrap();
    c.query_drop(&sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let copy = fetch(c, "SHOW CREATE TABLE orders_copy").await;
    let copy = String::from_utf8(copy.rows[0][1].clone().unwrap().to_vec()).unwrap();
    assert!(
        !copy.contains("_sprig_") && !copy.contains("sprig_auto_"),
        "{copy}"
    );
    assert!(!copy.contains("FOREIGN KEY"), "{copy}");
    assert!(copy.contains("`chk_total`"), "{copy}");
    c.query_drop("DROP TABLE orders_copy").await.unwrap();
}

async fn information_schema_end_to_end(c: &mut MysqlClient) {
    let map = tenant();
    let mdb = Persona::MariaDb;
    let view = async |c: &mut MysqlClient, sql: &str| {
        let (names, rows) = client_view(c, &map, mdb, sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_no_leaks(sql, &rows);
        (names, rows)
    };
    let registry = ColumnRegistry::mariadb_11_8();

    // SELECT * keeps the backend's shape; only the tenant's objects.
    let (n, r) = view(c, "SELECT * FROM information_schema.TABLES").await;
    assert_eq!(n.len(), registry.columns("TABLES").unwrap().len());
    assert_eq!(
        set(&column(&n, &r, "TABLE_SCHEMA")),
        set(&["billing", "crm"].map(String::from))
    );
    let names = set(&column(&n, &r, "TABLE_NAME"));
    assert!(names.contains("orders") && names.contains("accounts"));
    assert!(
        !names
            .iter()
            .any(|t| t.starts_with("__sprig") || t == "secret" || t == "databases")
    );

    // Pushdown: one branch; the outer predicate still applies.
    let (n, r) = view(
        c,
        "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'crm' ORDER BY 1",
    )
    .await;
    assert_eq!(column(&n, &r, "TABLE_NAME"), ["accounts"]);
    let (n, r) = view(
        c,
        "SELECT t.TABLE_NAME FROM information_schema.TABLES AS t \
         WHERE t.TABLE_SCHEMA IN ('billing', 'nope') AND t.TABLE_NAME = 'orders'",
    )
    .await;
    assert_eq!(column(&n, &r, "TABLE_NAME"), ["orders"]);
    let (n, r) = view(
        c,
        "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() \
         AND TABLE_TYPE = 'VIEW'",
    )
    .await;
    assert_eq!(column(&n, &r, "TABLE_NAME"), ["v_orders"]);
    let (_, r) = view(
        c,
        "SELECT * FROM information_schema.TABLES WHERE TABLE_SCHEMA = 't9999_d7_g7_s7'",
    )
    .await;
    assert!(r.is_empty());

    let (n, r) = view(
        c,
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = 'billing' AND TABLE_NAME = 'orders' ORDER BY ORDINAL_POSITION",
    )
    .await;
    assert_eq!(
        column(&n, &r, "COLUMN_NAME"),
        ["id", "customer_id", "account_id", "total", "created_at"]
    );
    let (n, r) = view(
        c,
        "SELECT DISTINCT INDEX_NAME FROM information_schema.STATISTICS \
         WHERE TABLE_NAME = 'orders' ORDER BY 1",
    )
    .await;
    assert_eq!(column(&n, &r, "INDEX_NAME"), ["PRIMARY"]);
    let (n, r) = view(
        c,
        "SELECT CONSTRAINT_NAME, REFERENCED_TABLE_SCHEMA FROM information_schema.KEY_COLUMN_USAGE \
         WHERE TABLE_SCHEMA = 'billing' AND TABLE_NAME = 'orders' ORDER BY 1",
    )
    .await;
    assert_eq!(
        column(&n, &r, "CONSTRAINT_NAME"),
        ["fk_acct", "fk_cust", "PRIMARY"]
    );
    assert_eq!(
        column(&n, &r, "REFERENCED_TABLE_SCHEMA"),
        ["crm", "billing", "NULL"]
    );
    let (n, r) = view(
        c,
        "SELECT CONSTRAINT_NAME, UNIQUE_CONSTRAINT_SCHEMA FROM information_schema.REFERENTIAL_CONSTRAINTS ORDER BY 1",
    )
    .await;
    assert_eq!(
        column(&n, &r, "UNIQUE_CONSTRAINT_SCHEMA"),
        ["crm", "billing"]
    );
    let (n, r) = view(
        c,
        "SELECT CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS \
         WHERE TABLE_NAME = 'orders' ORDER BY 1",
    )
    .await;
    assert_eq!(
        column(&n, &r, "CONSTRAINT_NAME"),
        ["chk_total", "fk_acct", "fk_cust", "PRIMARY"]
    );
    let (n, r) = view(
        c,
        "SELECT CONSTRAINT_NAME FROM information_schema.CHECK_CONSTRAINTS",
    )
    .await;
    assert_eq!(column(&n, &r, "CONSTRAINT_NAME"), ["chk_total"]);
    let (n, r) = view(
        c,
        "SELECT TABLE_NAME, VIEW_DEFINITION, DEFINER FROM information_schema.VIEWS",
    )
    .await;
    assert_eq!(column(&n, &r, "TABLE_NAME"), ["v_orders"]);
    assert!(column(&n, &r, "VIEW_DEFINITION")[0].contains("`crm`.`accounts`"));
    let (n, r) = view(
        c,
        "SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_DEFINITION FROM information_schema.ROUTINES ORDER BY 2",
    )
    .await;
    assert_eq!(column(&n, &r, "ROUTINE_NAME"), ["f_double", "p_count"]);
    assert!(column(&n, &r, "ROUTINE_DEFINITION")[1].contains("`billing`.orders"));
    let (n, r) = view(
        c,
        "SELECT SPECIFIC_NAME FROM information_schema.PARAMETERS ORDER BY 1",
    )
    .await;
    assert!(
        column(&n, &r, "SPECIFIC_NAME")
            .iter()
            .all(|s| s == "f_double" || s == "p_count")
    );
    let (n, r) = view(
        c,
        "SELECT TRIGGER_SCHEMA, TRIGGER_NAME, EVENT_OBJECT_SCHEMA, ACTION_STATEMENT FROM information_schema.TRIGGERS",
    )
    .await;
    assert_eq!(column(&n, &r, "TRIGGER_NAME"), ["trg_orders"]);
    assert_eq!(column(&n, &r, "EVENT_OBJECT_SCHEMA"), ["billing"]);
    let (n, r) = view(
        c,
        "SELECT EVENT_SCHEMA, EVENT_NAME FROM information_schema.EVENTS",
    )
    .await;
    assert_eq!(column(&n, &r, "EVENT_NAME"), ["ev_purge"]);
    let (n, r) = view(
        c,
        "SELECT DISTINCT TABLE_NAME FROM information_schema.PARTITIONS WHERE PARTITION_NAME IS NOT NULL",
    )
    .await;
    assert_eq!(column(&n, &r, "TABLE_NAME"), ["parts"]);

    // SCHEMATA from the name map; PROCESSLIST from the sessions.
    let (n, r) = view(
        c,
        "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA ORDER BY 1",
    )
    .await;
    assert_eq!(
        column(&n, &r, "SCHEMA_NAME"),
        ["billing", "crm", "information_schema"]
    );
    let (n, r) = view(
        c,
        "SELECT * FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = 'billing'",
    )
    .await;
    assert_eq!(n.len(), registry.columns("SCHEMATA").unwrap().len());
    assert_eq!(column(&n, &r, "DEFAULT_CHARACTER_SET_NAME"), ["utf8mb4"]);
    let (n, r) = view(c, "SELECT ID, USER, DB FROM information_schema.PROCESSLIST").await;
    assert_eq!(column(&n, &r, "DB"), ["billing"]);

    // Joins and subqueries over several views, three-part column names.
    let (n, r) = view(
        c,
        "SELECT t.TABLE_NAME, COUNT(*) AS n FROM information_schema.TABLES t \
         JOIN information_schema.COLUMNS c ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME \
         WHERE t.TABLE_SCHEMA = 'billing' AND t.TABLE_TYPE = 'BASE TABLE' GROUP BY t.TABLE_NAME ORDER BY 1",
    )
    .await;
    assert_eq!(
        row_strings(&r[2]),
        ["orders", "5"],
        "{n:?} {:?}",
        r.iter().map(row_strings).collect::<Vec<_>>()
    );
    let (n, r) = view(
        c,
        "SELECT information_schema.TABLES.TABLE_NAME FROM information_schema.TABLES \
         WHERE information_schema.TABLES.TABLE_SCHEMA = 'crm'",
    )
    .await;
    assert_eq!(column(&n, &r, "TABLE_NAME"), ["accounts"]);
    let (n, r) = view(
        c,
        "SELECT COUNT(*) AS n FROM orders WHERE EXISTS (SELECT 1 FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = 'billing' AND TABLE_NAME = 'orders' AND COLUMN_NAME = '_sprig_p_status')",
    )
    .await;
    assert_eq!(column(&n, &r, "n"), ["0"]);

    // Pass-through and refused tables.
    let (_, r) = view(c, "SELECT COUNT(*) FROM information_schema.COLLATIONS").await;
    assert_eq!(r.len(), 1);
    let e = client_view(c, &map, mdb, "SELECT * FROM information_schema.INNODB_TRX")
        .await
        .unwrap_err();
    assert!(e.starts_with("ERR 1142"), "{e}");
    let e = client_view(
        c,
        &map,
        mdb,
        "SELECT * FROM information_schema.SPRIG_SETTINGS",
    )
    .await
    .unwrap_err();
    assert!(e.starts_with("ERR 39051"), "{e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn names_live() {
    let Ok(url) = std::env::var(ENV) else {
        eprintln!("skipped: {ENV} is not set");
        return;
    };
    let mut admin = connect(&url, None).await;
    run_script(&mut admin, "setup.sql").await;
    let u = url.clone();
    let body = tokio::spawn(async move {
        if std::env::var("SPRIG_GOLDEN_CAPTURE").is_ok() {
            capture(&u).await;
        }
        let mut c = connect(&u, Some("t9999_d1_g1_s1")).await;
        show_end_to_end(&mut c).await;
        create_like_end_to_end(&mut c).await;
        information_schema_end_to_end(&mut c).await;
        c.quit().await;
    })
    .await;
    run_script(&mut admin, "teardown.sql").await;
    admin.quit().await;
    if let Err(e) = body {
        std::panic::resume_unwind(e.into_panic());
    }
}
