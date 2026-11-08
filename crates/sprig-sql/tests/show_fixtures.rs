//! The `SHOW` family's result side (chapter 07 build step 4) on captured
//! MariaDB 11.8 output: each fixture under `tests/fixtures/names/show/` is
//! what MariaDB answered to the rewritten statement for tenant 9999
//! (`billing` = `t9999_d1_g1_s1`, `crm` = `t9999_d2_g2_s2`); the mapper
//! must produce what a client of the tenant sees. `names_live.rs`
//! refreshes the fixtures.

#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use common::fixture::{Captured, row_strings};
use common::leaks::assert_clean;
use sprig_common::ids::TenantId;
use sprig_common::names::AccountName;
use sprig_proto_mysql::packets::Persona;
use sprig_sql::names::{
    AccountResolver, MemoryNameMap, OutputCtx, ResultMapper, ShowKind, TextRow, Visibility,
};

fn tenant() -> MemoryNameMap {
    MemoryNameMap::new(TenantId(9999))
        .with_db(1, "billing", 1, 1)
        .with_db(2, "crm", 2, 2)
}

struct Mapped {
    names: Vec<String>,
    rows: Vec<Vec<String>>,
    hidden: u64,
    foreign: u64,
}

fn map_with(fixture: &str, mut mapper: ResultMapper, map: &MemoryNameMap) -> Mapped {
    let cap = Captured::load(fixture);
    let ctx = OutputCtx::new(map);
    let mut defs = cap.columns.clone();
    let rows: Vec<TextRow> = mapper.map_all(&mut defs, cap.rows.clone(), &ctx);
    let names: Vec<String> = defs
        .iter()
        .map(|d| String::from_utf8_lossy(&d.name).into_owned())
        .collect();
    for d in &defs {
        for f in [&d.schema, &d.table, &d.org_table, &d.name, &d.org_name] {
            assert_clean(fixture, f);
        }
    }
    for r in &rows {
        for v in r.iter().flatten() {
            assert_clean(fixture, v);
        }
    }
    Mapped {
        names,
        rows: rows.iter().map(row_strings).collect(),
        hidden: mapper.hidden_rows(),
        foreign: mapper.foreign_names(),
    }
}

fn show(fixture: &str, kind: ShowKind, persona: Persona) -> Mapped {
    map_with(
        fixture,
        ResultMapper::show(kind, Visibility::for_persona(persona)),
        &tenant(),
    )
}

fn col(m: &Mapped, name: &str) -> Vec<String> {
    let i = m
        .names
        .iter()
        .position(|n| n == name)
        .unwrap_or_else(|| panic!("no {name} in {:?}", m.names));
    m.rows.iter().map(|r| r[i].clone()).collect()
}

const MDB: Persona = Persona::MariaDb;

#[test]
fn tables() {
    let m = show("tables", ShowKind::Tables, MDB);
    assert_eq!(m.names, ["Tables_in_billing"]);
    assert_eq!(
        col(&m, "Tables_in_billing"),
        ["customers", "gipk", "orders", "parts", "v_orders"]
    );
    assert_eq!(m.hidden, 2);
    let m = show("full_tables", ShowKind::Tables, MDB);
    assert_eq!(m.names, ["Tables_in_billing", "Table_type"]);
    assert_eq!(col(&m, "Table_type").last().unwrap(), "VIEW");
    let m = show("tables_like", ShowKind::Tables, MDB);
    assert_eq!(m.names, ["Tables_in_billing (%o%)"]);
    assert_eq!(
        col(&m, "Tables_in_billing (%o%)"),
        ["customers", "orders", "v_orders"]
    );
    let m = show("tables_where", ShowKind::Tables, MDB);
    assert!(
        !col(&m, "Tables_in_billing")
            .iter()
            .any(|t| t.starts_with("__sprig"))
    );
}

#[test]
fn table_status_and_open_tables() {
    let m = show("table_status", ShowKind::TableStatus, MDB);
    assert_eq!(
        col(&m, "Name"),
        ["customers", "gipk", "orders", "parts", "v_orders"]
    );
    let m = show("open_tables", ShowKind::OpenTables, MDB);
    assert!(col(&m, "Database").iter().all(|d| d == "billing"));
    assert!(!col(&m, "Table").iter().any(|t| t.starts_with("__sprig")));
}

#[test]
fn columns_and_describe() {
    for f in ["columns_orders", "describe_orders"] {
        let m = show(f, ShowKind::Columns, MDB);
        assert_eq!(
            col(&m, "Field"),
            ["id", "customer_id", "account_id", "total", "created_at"],
            "{f}"
        );
    }
    let m = show("full_columns_customers", ShowKind::Columns, MDB);
    assert_eq!(col(&m, "Field"), ["id", "name", "email"]);
    assert_eq!(m.names.len(), 9);
    // The GIPK follows the persona's variable.
    assert_eq!(
        col(&show("columns_gipk", ShowKind::Columns, MDB), "Field"),
        ["v"]
    );
    assert_eq!(
        col(
            &show("columns_gipk", ShowKind::Columns, Persona::MySql84),
            "Field"
        ),
        ["my_row_id", "v"]
    );
    // @sprig_show_internal shows everything.
    let all = Visibility {
        internal: true,
        ..Visibility::none()
    };
    let m = map_with(
        "columns_orders",
        ResultMapper::show(ShowKind::Columns, all),
        &tenant(),
    );
    assert_eq!(col(&m, "Field").last().unwrap(), "_sprig_p_status");
}

#[test]
fn index() {
    let m = show("index_orders", ShowKind::Index, MDB);
    assert_eq!(col(&m, "Key_name"), ["PRIMARY"]);
    assert_eq!(m.hidden, 5);
    let m = show("keys_customers", ShowKind::Index, MDB);
    assert_eq!(col(&m, "Key_name"), ["PRIMARY", "email"]);
    let m = show("index_gipk", ShowKind::Index, MDB);
    assert!(m.rows.is_empty());
    assert_eq!(
        col(
            &show("index_gipk", ShowKind::Index, Persona::MySql84),
            "Key_name"
        ),
        ["PRIMARY"]
    );
    let auto = Visibility {
        auto_indexes: true,
        ..Visibility::for_persona(MDB)
    };
    let m = map_with(
        "index_orders",
        ResultMapper::show(ShowKind::Index, auto),
        &tenant(),
    );
    assert_eq!(
        col(&m, "Key_name"),
        ["PRIMARY", "sprig_auto_3fa91c0e", "sprig_auto_3fa91c0e"]
    );
}

#[test]
fn create_table_line_filter() {
    let m = show("create_table_orders", ShowKind::CreateTable, MDB);
    assert_eq!(
        col(&m, "Create Table")[0],
        "CREATE TABLE `orders` (
  `id` int(11) NOT NULL,
  `customer_id` int(11) NOT NULL,
  `account_id` int(11) DEFAULT NULL,
  `total` decimal(10,2) DEFAULT 0.00,
  `created_at` datetime DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_acct` FOREIGN KEY (`account_id`) REFERENCES `crm`.`accounts` (`id`),
  CONSTRAINT `fk_cust` FOREIGN KEY (`customer_id`) REFERENCES `customers` (`id`),
  CONSTRAINT `chk_total` CHECK (`total` >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci"
    );
    let m = show("create_table_customers", ShowKind::CreateTable, MDB);
    let text = &col(&m, "Create Table")[0];
    assert!(
        !text.contains("_sprig_rk") && !text.contains("idx_rk"),
        "{text}"
    );
    assert!(text.contains("  UNIQUE KEY `email` (`email`)\n)"), "{text}");
    let m = show("create_table_gipk", ShowKind::CreateTable, MDB);
    assert_eq!(
        col(&m, "Create Table")[0],
        "CREATE TABLE `gipk` (\n  `v` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci"
    );
    let m = show("create_table_parts", ShowKind::CreateTable, MDB);
    let cap = Captured::load("create_table_parts");
    assert_eq!(
        col(&m, "Create Table")[0],
        String::from_utf8_lossy(cap.rows[0][1].as_ref().unwrap())
    );
    // SHOW CREATE TABLE of a view answers like SHOW CREATE VIEW.
    let m = show("create_table_view", ShowKind::CreateTable, MDB);
    assert!(col(&m, "Create View")[0].contains("`billing`.`orders`"));
}

#[test]
fn create_view_trigger_event_routine_database() {
    let m = show("create_view", ShowKind::CreateView, MDB);
    let v = &col(&m, "Create View")[0];
    assert!(
        v.contains("from (`billing`.`orders` `o` join `crm`.`accounts` `a`"),
        "{v}"
    );
    let m = show("create_trigger", ShowKind::CreateTrigger, MDB);
    let t = &col(&m, "SQL Original Statement")[0];
    assert!(
        t.contains("TRIGGER `billing`.trg_orders BEFORE INSERT ON `billing`.orders"),
        "{t}"
    );
    assert!(t.contains("FROM `crm`.accounts"), "{t}");
    let m = show("create_event", ShowKind::CreateEvent, MDB);
    assert!(col(&m, "Create Event")[0].contains("DELETE FROM `billing`.orders"));
    let m = show("create_procedure", ShowKind::CreateProcedure, MDB);
    assert!(col(&m, "Create Procedure")[0].contains("FROM `billing`.orders WHERE"));
    let m = show("create_function", ShowKind::CreateFunction, MDB);
    assert!(col(&m, "Create Function")[0].contains("RETURN x * 2"));
    let m = show("create_database", ShowKind::CreateDatabase, MDB);
    assert_eq!(col(&m, "Database"), ["billing"]);
    assert!(col(&m, "Create Database")[0].starts_with("CREATE DATABASE `billing` /*!40100"));
}

#[test]
fn triggers_events_routines() {
    let m = show("triggers", ShowKind::Triggers, MDB);
    assert_eq!(col(&m, "Trigger"), ["trg_orders"]);
    assert!(col(&m, "Statement")[0].contains("`crm`.accounts"));
    let m = show("events", ShowKind::Events, MDB);
    assert_eq!(col(&m, "Name"), ["ev_purge"]);
    assert_eq!(col(&m, "Db"), ["billing"]);
    let m = show("procedure_status", ShowKind::ProcedureStatus, MDB);
    // __sprig_internal is hidden; p_secret is in a schema the tenant's map
    // doesn't know.
    assert_eq!(col(&m, "Name"), ["p_count"]);
    assert_eq!(col(&m, "Db"), ["billing"]);
    assert_eq!(m.hidden, 2);
    let m = show("function_status", ShowKind::FunctionStatus, MDB);
    assert_eq!(col(&m, "Name"), ["f_double"]);
}

#[test]
fn warnings_and_check_table() {
    let m = show("warnings", ShowKind::Warnings, MDB);
    assert_eq!(col(&m, "Message"), ["Unknown table 'billing.nope'"]);
    let m = map_with("check_table", ResultMapper::scrub(), &tenant());
    assert_eq!(col(&m, "Table"), ["billing.orders", "crm.accounts"]);
}

struct Accounts;

impl AccountResolver for Accounts {
    fn account_name(&self, a: &AccountName) -> Option<String> {
        (a.to_string() == "t9999_u7").then(|| "alice".to_owned())
    }
}

#[test]
fn definers_map_through_the_resolver() {
    let map = tenant();
    let ctx = OutputCtx::new(&map).accounts(&Accounts);
    let mut cap = Captured::load("create_view");
    let text = String::from_utf8_lossy(cap.rows[0][1].as_ref().unwrap())
        .replace("`root`@`%`", "`t9999_u7`@`10.0.0.%`");
    cap.rows[0][1] = Some(text.into());
    let mut m = ResultMapper::show(ShowKind::CreateView, Visibility::for_persona(MDB));
    let mut defs = cap.columns.clone();
    let rows = m.map_all(&mut defs, cap.rows, &ctx);
    let v = row_strings(&rows[0])[1].clone();
    assert!(v.contains("DEFINER=`alice`@`10.0.0.%`"), "{v}");
}

#[test]
fn another_tenant_sees_only_question_marks() {
    // Tenant 9998 must never see tenant 9999's names, whatever reaches it.
    let other = MemoryNameMap::new(TenantId(9998)).with_db(1, "billing", 1, 1);
    for (f, kind) in [
        ("create_view", ShowKind::CreateView),
        ("create_table_orders", ShowKind::CreateTable),
        ("warnings", ShowKind::Warnings),
        ("triggers", ShowKind::Triggers),
    ] {
        let m = map_with(
            f,
            ResultMapper::show(kind, Visibility::for_persona(MDB)),
            &other,
        );
        assert!(m.foreign > 0, "{f}: no foreign names counted");
    }
    let m = map_with(
        "procedure_status",
        ResultMapper::show(ShowKind::ProcedureStatus, Visibility::for_persona(MDB)),
        &other,
    );
    assert!(m.rows.is_empty());
}

#[test]
fn every_fixture_is_clean_under_scrub() {
    // The fallback mapper alone keeps every captured output clean.
    let dir = common::fixture::dir().join("show");
    let mut n = 0;
    for e in std::fs::read_dir(dir).unwrap() {
        let name = e.unwrap().file_name().to_string_lossy().into_owned();
        let Some(stem) = name.strip_suffix(".json") else {
            continue;
        };
        map_with(stem, ResultMapper::scrub(), &tenant());
        n += 1;
    }
    assert!(n >= 30, "only {n} fixtures");
}
