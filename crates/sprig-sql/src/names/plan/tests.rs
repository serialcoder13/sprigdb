use sprig_common::ids::TenantId;
use sprig_proto_mysql::packets::Persona;

use super::*;
use crate::names::show::ProcessRow;
use crate::names::{AccountInfo, EditKind, MemoryNameMap};

fn acme() -> MemoryNameMap {
    MemoryNameMap::new(TenantId(42))
        .with_db(1, "billing", 1, 1)
        .with_db(2, "crm", 2, 2)
}

struct Sessions;

impl SessionSource for Sessions {
    fn processes(&self) -> Vec<ProcessRow> {
        vec![ProcessRow {
            id: 1,
            user: "alice".into(),
            info: Some("x".repeat(150)),
            ..ProcessRow::default()
        }]
    }
}

fn plan_on(sql: &str, persona: Persona) -> Result<Plan, NameError> {
    let map = acme();
    let reg = ColumnRegistry::mariadb_11_8();
    let rctx = RewriteCtx::new(&map, persona)
        .current_db(Some("billing"))
        .account(AccountInfo {
            user: "alice",
            account_host: "%",
            client_host: "203.0.113.7",
        });
    plan(
        sql.as_bytes(),
        &PlanCtx::new(rctx, &reg).sessions(&Sessions),
    )
}

fn one(sql: &str) -> StmtPlan {
    plan_on(sql, Persona::MariaDb).unwrap().statements.remove(0)
}

fn fwd(sql: &str) -> (String, ResultMapper) {
    match one(sql).action {
        Action::Forward { sql, mapper } => (String::from_utf8(sql).unwrap(), mapper),
        a => panic!("{a:?}"),
    }
}

fn local(sql: &str) -> LocalAnswer {
    match one(sql).action {
        Action::Local(a) => a,
        a => panic!("{a:?}"),
    }
}

fn cells(a: &LocalAnswer) -> Vec<Vec<Option<String>>> {
    a.rows
        .iter()
        .map(|r| {
            r.iter()
                .map(|v| v.as_ref().map(|b| String::from_utf8_lossy(b).into_owned()))
                .collect()
        })
        .collect()
}

#[test]
fn show_databases_is_local() {
    let a = local("SHOW DATABASES");
    assert_eq!(&a.columns[0].name[..], b"Database");
    assert_eq!(
        cells(&a),
        [
            [Some("billing".into())],
            [Some("crm".into())],
            [Some("information_schema".into())]
        ]
    );
    let a = local("show schemas like 'c%'");
    assert_eq!(&a.columns[0].name[..], b"Database (c%)");
    assert_eq!(cells(&a), [[Some("crm".into())]]);
    let a = match plan_on("SHOW DATABASES", Persona::MySql84)
        .unwrap()
        .statements
        .remove(0)
        .action
    {
        Action::Local(a) => a,
        a => panic!("{a:?}"),
    };
    assert_eq!(a.rows.len(), 4, "performance_schema on 3306");
    let (sql, m) = fwd("SHOW DATABASES WHERE `Database` <> 'crm'");
    assert_eq!(
        sql,
        "SELECT `Database` FROM (SELECT 'billing' AS `Database` UNION ALL SELECT 'crm' AS `Database` UNION ALL SELECT 'information_schema' AS `Database`) AS `SCHEMATA` WHERE `Database` <> 'crm' ORDER BY `Database`"
    );
    assert!(m.needs_rows());
}

#[test]
fn show_tables_where_names_the_physical_column() {
    let (sql, m) =
        fwd("SHOW TABLES WHERE Tables_in_billing LIKE 'o%' OR `TABLES_IN_BILLING` = 'x'");
    assert_eq!(
        sql,
        "SHOW TABLES WHERE `Tables_in_t42_d1_g1_s1` LIKE 'o%' OR `Tables_in_t42_d1_g1_s1` = 'x'"
    );
    assert_eq!(m.show_kind(), Some(ShowKind::Tables));
    let (sql, _) =
        fwd("SHOW FULL TABLES FROM crm WHERE Tables_in_crm = 'a' AND Table_type = 'VIEW'");
    assert_eq!(
        sql,
        "SHOW FULL TABLES FROM `t42_d2_g2_s2` WHERE `Tables_in_t42_d2_g2_s2` = 'a' AND Table_type = 'VIEW'"
    );
}

#[test]
fn show_kinds_get_their_mappers() {
    for (q, k) in [
        ("SHOW TABLE STATUS", ShowKind::TableStatus),
        ("SHOW COLUMNS FROM orders", ShowKind::Columns),
        ("SHOW INDEX FROM crm.accounts", ShowKind::Index),
        ("SHOW CREATE TABLE orders", ShowKind::CreateTable),
        ("SHOW CREATE VIEW v", ShowKind::CreateView),
        ("SHOW CREATE PROCEDURE p", ShowKind::CreateProcedure),
        ("SHOW CREATE FUNCTION crm.f", ShowKind::CreateFunction),
        ("SHOW CREATE TRIGGER t", ShowKind::CreateTrigger),
        ("SHOW CREATE EVENT e", ShowKind::CreateEvent),
        ("SHOW CREATE DATABASE crm", ShowKind::CreateDatabase),
        ("SHOW TRIGGERS", ShowKind::Triggers),
        ("SHOW EVENTS FROM crm", ShowKind::Events),
        ("SHOW PROCEDURE STATUS", ShowKind::ProcedureStatus),
        ("SHOW FUNCTION STATUS", ShowKind::FunctionStatus),
        ("SHOW OPEN TABLES", ShowKind::OpenTables),
        ("SHOW WARNINGS", ShowKind::Warnings),
        ("SHOW ERRORS", ShowKind::Errors),
        ("DESCRIBE orders", ShowKind::Columns),
    ] {
        let (_, m) = fwd(q);
        assert_eq!(m.show_kind(), Some(k), "{q}");
    }
    let s = one("SHOW CREATE PROCEDURE crm.p");
    let show = s.show.unwrap();
    assert_eq!(show.object.as_deref(), Some("p"));
    assert_eq!(show.schema.as_deref(), Some("t42_d2_g2_s2"));
    for q in [
        "SHOW VARIABLES LIKE 'x'",
        "SHOW GRANTS",
        "SHOW COLLATION",
        "CHECK TABLE orders",
        "EXPLAIN SELECT 1",
    ] {
        let (_, m) = fwd(q);
        assert!(m.needs_rows() && m.show_kind().is_none(), "{q}");
    }
    let (_, m) = fwd("SELECT 1");
    assert!(!m.needs_rows());
}

#[test]
fn refused_and_local_shows() {
    let e = plan_on("SHOW BINARY LOGS", Persona::MariaDb).unwrap_err();
    assert_eq!((e.errno(), &e.sqlstate()), (1227, b"42000"));
    let a = local("SHOW PROCESSLIST");
    assert_eq!(a.columns.len(), 9);
    assert_eq!(cells(&a)[0][7].as_ref().unwrap().len(), 100);
    let a = local("SHOW FULL PROCESSLIST");
    assert_eq!(cells(&a)[0][7].as_ref().unwrap().len(), 150);
    let a = match plan_on("SHOW PROCESSLIST", Persona::MySql84)
        .unwrap()
        .statements
        .remove(0)
        .action
    {
        Action::Local(a) => a,
        a => panic!("{a:?}"),
    };
    assert_eq!(a.columns.len(), 8);
}

#[test]
fn create_table_like() {
    match one("CREATE TABLE IF NOT EXISTS crm.c2 LIKE billing.orders").action {
        Action::CreateLike { show_sql, head } => {
            assert_eq!(show_sql, b"SHOW CREATE TABLE `t42_d1_g1_s1`.orders");
            assert_eq!(head, "CREATE TABLE IF NOT EXISTS `t42_d2_g2_s2`.c2");
        }
        a => panic!("{a:?}"),
    }
    match one("create temporary table c2 (like orders)").action {
        Action::CreateLike { show_sql, head } => {
            assert_eq!(show_sql, b"SHOW CREATE TABLE orders");
            assert_eq!(head, "create temporary table c2");
        }
        a => panic!("{a:?}"),
    }
    assert!(matches!(
        one("CREATE TABLE c2 (id INT)").action,
        Action::Forward { .. }
    ));
    assert!(matches!(
        one("CREATE TABLE c2 SELECT * FROM t WHERE a LIKE 'x'").action,
        Action::Forward { .. }
    ));
}

#[test]
fn statements_split_after_rewrite_and_track_use() {
    let p = plan_on(
        "SELECT DATABASE(); USE crm; SHOW TABLES WHERE Tables_in_crm = 'a'; SELECT * FROM information_schema.TABLES",
        Persona::MariaDb,
    )
    .unwrap();
    assert_eq!(p.statements.len(), 4);
    let sqls: Vec<String> = p
        .statements
        .iter()
        .map(|s| match &s.action {
            Action::Forward { sql, .. } => String::from_utf8(sql.clone()).unwrap(),
            a => panic!("{a:?}"),
        })
        .collect();
    assert_eq!(sqls[0], "SELECT 'billing'");
    assert_eq!(sqls[1], "USE `t42_d2_g2_s2`");
    assert_eq!(sqls[2], "SHOW TABLES WHERE `Tables_in_t42_d2_g2_s2` = 'a'");
    assert!(sqls[3].starts_with("SELECT * FROM (SELECT `TABLE_CATALOG`"));
    assert!(
        p.forward_sql()
            .unwrap()
            .starts_with(b"SELECT 'billing';USE `t42_d2_g2_s2`;SHOW")
    );
    assert!(p.single().is_none());
    let p = plan_on("SHOW DATABASES; SELECT 1", Persona::MariaDb).unwrap();
    assert!(p.forward_sql().is_none());
}

#[test]
fn offsets_follow_edits() {
    let e = |start: usize, end: usize, rep: &str| Edit {
        range: start..end,
        kind: EditKind::Database,
        replacement: rep.into(),
    };
    let edits = [e(2, 4, "abcdef"), e(10, 12, "")];
    assert_eq!(map_offset(&edits, 0), 0);
    assert_eq!(map_offset(&edits, 2), 2);
    assert_eq!(map_offset(&edits, 4), 8);
    assert_eq!(map_offset(&edits, 12), 14);
    assert_eq!(map_offset(&edits, 20), 22);
}

#[test]
fn information_schema_in_other_statements() {
    let (sql, m) = fwd("SELECT COUNT(*) FROM information_schema.COLLATIONS");
    assert_eq!(sql, "SELECT COUNT(*) FROM information_schema.COLLATIONS");
    assert!(
        m.needs_rows(),
        "pass-through tables still have account columns scrubbed"
    );
    let e = plan_on(
        "PREPARE s FROM 'SELECT * FROM information_schema.TABLES'",
        Persona::MariaDb,
    )
    .unwrap_err();
    assert_eq!(e.errno(), 39051);
    assert!(
        plan_on(
            "PREPARE s FROM 'SELECT * FROM information_schema.ENGINES'",
            Persona::MariaDb
        )
        .is_ok()
    );
    let e = plan_on(
        "CREATE VIEW v AS SELECT * FROM information_schema.TABLES",
        Persona::MariaDb,
    )
    .unwrap_err();
    assert_eq!(e.errno(), 39051);
    let (sql, _) = fwd(
        "INSERT INTO t SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE()",
    );
    assert!(
        sql.starts_with("INSERT INTO t SELECT TABLE_NAME FROM (SELECT"),
        "{sql}"
    );
    assert!(
        sql.ends_with(") AS `TABLES` WHERE TABLE_SCHEMA = 'billing'"),
        "{sql}"
    );
    // DATABASE() was substituted first, so the pushdown sees 'billing'.
    assert_eq!(sql.matches("UNION ALL").count(), 0);
    let p = plan_on(
        "USE information_schema; SELECT * FROM TABLES",
        Persona::MariaDb,
    )
    .unwrap();
    let Action::Forward { sql, .. } = &p.statements[1].action else {
        panic!()
    };
    assert!(String::from_utf8_lossy(sql).contains(") AS `TABLES`"));
}
