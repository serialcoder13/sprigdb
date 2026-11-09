use sprig_common::ids::{DbId, GroupId, ShardId, TenantId};
use sprig_proto_mysql::packets::Persona;

use super::*;
use crate::names::map::SchemaName;
use crate::names::{LogicalDb, MemoryNameMap};

fn acme() -> MemoryNameMap {
    let mut m = MemoryNameMap::new(TenantId(42))
        .with_db(1, "billing", 1, 1)
        .with_db(2, "crm", 2, 2);
    let t = TenantId(42);
    m.insert(LogicalDb {
        id: DbId(3),
        name: "o'k".into(),
        shards: vec![
            SchemaName::shard(t, DbId(3), GroupId(3), ShardId(5)),
            SchemaName::shard(t, DbId(3), GroupId(3), ShardId(4)),
        ],
        reference: Some(SchemaName::reference(t, DbId(3))),
    });
    m
}

struct One;

impl SessionSource for One {
    fn processes(&self) -> Vec<ProcessRow> {
        vec![ProcessRow {
            id: 9,
            user: "alice".into(),
            host: "h:1".into(),
            db: None,
            command: "Sleep".into(),
            ..ProcessRow::default()
        }]
    }
}

fn ctx_with<'a>(
    map: &'a MemoryNameMap,
    reg: &'a ColumnRegistry,
    f: impl FnOnce(&mut InfoSchemaCtx<'a>),
) -> InfoSchemaCtx<'a> {
    let mut c = InfoSchemaCtx {
        map,
        lex: LexOptions::mariadb(),
        vis: Visibility::for_persona(Persona::MariaDb),
        registry: reg,
        sessions: Some(&One),
        visible: None,
        in_information_schema: false,
        user: Some(("alice", "203.0.113.7")),
        stored_names: false,
    };
    f(&mut c);
    c
}

fn run_with(
    sql: &str,
    kind: StmtKind,
    f: impl FnOnce(&mut InfoSchemaCtx<'_>),
) -> Result<Option<String>, NameError> {
    let map = acme();
    let reg = ColumnRegistry::mariadb_11_8();
    let ctx = ctx_with(&map, &reg, f);
    rewrite_information_schema(sql.as_bytes(), kind, &ctx)
        .map(|o| o.map(|r| String::from_utf8(r.sql).unwrap()))
}

fn run(sql: &str) -> String {
    run_with(sql, StmtKind::Select, |_| {}).unwrap().unwrap()
}

/// The `WHERE <restriction> = '<schema>'` branches of a rewrite.
fn branches_of(sql: &str, restrict: &str) -> Vec<String> {
    let needle = format!("WHERE `{restrict}` = '");
    sql.match_indices(&needle)
        .map(|(i, _)| {
            let rest = &sql[i + needle.len()..];
            rest[..rest.find('\'').unwrap()].to_owned()
        })
        .collect()
}

#[test]
fn one_branch_per_representative_schema() {
    let sql = run("SELECT * FROM information_schema.TABLES");
    // o'k has two shards in one group: its lowest shard represents it.
    assert_eq!(
        branches_of(&sql, "TABLE_SCHEMA"),
        ["t42_d1_g1_s1", "t42_d2_g2_s2", "t42_d3_g3_s4"]
    );
    assert_eq!(sql.matches(" UNION ALL ").count(), 2);
    assert!(sql.ends_with(") AS `TABLES`"), "{sql}");
    assert!(
        sql.contains("CONVERT('o''k' USING utf8mb3) COLLATE utf8mb3_general_ci AS `TABLE_SCHEMA`"),
        "{sql}"
    );
    assert!(!sql.contains(" IN ("), "never an IN list on the schema");
    assert!(!sql.contains("LIKE"), "never LIKE on the schema");
}

#[test]
fn pushdown_picks_branches_and_copies_table_names() {
    let sql = run(
        "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'BILLING' AND TABLE_NAME = 'orders'",
    );
    assert_eq!(branches_of(&sql, "TABLE_SCHEMA"), ["t42_d1_g1_s1"]);
    assert!(
        sql.contains("AND `TABLE_NAME` = 'orders') AS `TABLES` WHERE TABLE_SCHEMA = 'BILLING'"),
        "{sql}"
    );
    let sql = run(
        "SELECT 1 FROM information_schema.COLUMNS c WHERE c.TABLE_SCHEMA IN ('crm', 'o''k', 'nope') AND c.TABLE_NAME IN ('a', 'b')",
    );
    assert_eq!(
        branches_of(&sql, "TABLE_SCHEMA"),
        ["t42_d2_g2_s2", "t42_d3_g3_s4"]
    );
    assert!(
        sql.contains("AND `TABLE_NAME` IN ('a', 'b')) c WHERE"),
        "{sql}"
    );
    // Two conjuncts on the schema intersect.
    let sql = run(
        "SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA IN ('crm','billing') AND 'billing' = TABLE_SCHEMA",
    );
    assert_eq!(branches_of(&sql, "TABLE_SCHEMA"), ["t42_d1_g1_s1"]);
    // A column that always equals the restriction column pushes too.
    let sql = run(
        "SELECT 1 FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = 'crm' AND EVENT_OBJECT_TABLE = 't'",
    );
    assert_eq!(branches_of(&sql, "EVENT_OBJECT_SCHEMA"), ["t42_d2_g2_s2"]);
    assert!(sql.contains("AND `EVENT_OBJECT_TABLE` = 't')"), "{sql}");
}

#[test]
fn no_pushdown_when_unsafe() {
    for q in [
        "SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'crm' OR TABLE_NAME = 'x'",
        "SELECT 1 FROM information_schema.TABLES t WHERE x.TABLE_SCHEMA = 'crm'",
        "SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA LIKE 'crm'",
        "SELECT 1 FROM information_schema.TABLES WHERE (SELECT 1 FROM d.t WHERE TABLE_SCHEMA = 'crm')",
        "SELECT * FROM information_schema.TABLES a, information_schema.TABLES b WHERE a.TABLE_SCHEMA = 'crm'",
    ] {
        let sql = run(q);
        assert_eq!(branches_of(&sql, "TABLE_SCHEMA").len() % 3, 0, "{q}: {sql}");
    }
    // BETWEEN's AND is not a conjunct boundary.
    let sql = run(
        "SELECT 1 FROM information_schema.TABLES WHERE TABLE_ROWS BETWEEN 1 AND 2 AND TABLE_SCHEMA = 'crm'",
    );
    assert_eq!(branches_of(&sql, "TABLE_SCHEMA"), ["t42_d2_g2_s2"]);
}

#[test]
fn nothing_matches_opens_nothing() {
    let sql = run("SELECT * FROM information_schema.TABLES WHERE TABLE_SCHEMA = 't42_d1_g1_s1'");
    assert_eq!(branches_of(&sql, "TABLE_SCHEMA"), ["__sprig_none"]);
}

#[test]
fn hiding_follows_visibility() {
    let sql = run("SELECT * FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = 'crm'");
    assert!(sql.contains("LEFT(`TABLE_NAME`, 8) <> '__sprig_'"));
    assert!(sql.contains("LEFT(`INDEX_NAME`, 11) <> 'sprig_auto_'"));
    assert!(sql.contains("NOT IN (SELECT `TABLE_NAME`, `INDEX_NAME` FROM information_schema.STATISTICS WHERE `TABLE_SCHEMA` = 't42_d2_g2_s2' AND (LEFT(`COLUMN_NAME`, 7) = '_sprig_' OR `COLUMN_NAME` = 'my_row_id'))"), "{sql}");
    let sql = run_with(
        "SELECT * FROM information_schema.STATISTICS",
        StmtKind::Select,
        |c| {
            c.vis = Visibility {
                auto_indexes: true,
                internal: false,
                gipk: true,
            };
        },
    )
    .unwrap()
    .unwrap();
    assert!(!sql.contains("sprig_auto_") && !sql.contains("my_row_id"));
    assert!(sql.contains("'_sprig_'"));
    let sql = run_with(
        "SELECT * FROM information_schema.COLUMNS",
        StmtKind::Select,
        |c| {
            c.vis = Visibility {
                auto_indexes: false,
                internal: true,
                gipk: false,
            };
        },
    )
    .unwrap()
    .unwrap();
    assert!(!sql.contains("_sprig_"), "{sql}");
    let sql = run("SELECT * FROM information_schema.KEY_COLUMN_USAGE");
    assert!(sql.contains(
        "(`REFERENCED_TABLE_NAME` IS NULL OR LEFT(`REFERENCED_TABLE_NAME`, 8) <> '__sprig_')"
    ));
    let sql = run("SELECT * FROM information_schema.TABLE_CONSTRAINTS");
    assert!(sql.contains(
        "information_schema.CHECK_CONSTRAINTS WHERE `CONSTRAINT_SCHEMA` = 't42_d1_g1_s1' AND INSTR"
    ));
    let sql = run("SELECT * FROM information_schema.CHECK_CONSTRAINTS");
    assert!(sql.contains("INSTR(`CHECK_CLAUSE`, '`_sprig_') = 0"));
}

#[test]
fn schema_and_definition_columns_are_mapped() {
    let sql =
        run("SELECT * FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = 'billing'");
    assert!(sql.contains(
        "CONVERT('billing' USING utf8mb3) COLLATE utf8mb3_general_ci AS `CONSTRAINT_SCHEMA`"
    ));
    assert!(sql.contains("CASE `REFERENCED_TABLE_SCHEMA` WHEN 't42_d1_g1_s1' THEN CONVERT('billing' USING utf8mb3) COLLATE utf8mb3_general_ci WHEN 't42_d2_g2_s2' THEN CONVERT('crm' USING utf8mb3) COLLATE utf8mb3_general_ci WHEN 't42_d3_g3_s5' THEN CONVERT('o''k' USING utf8mb3) COLLATE utf8mb3_general_ci WHEN 't42_d3_g3_s4' THEN CONVERT('o''k' USING utf8mb3) COLLATE utf8mb3_general_ci WHEN 't42_d3_ref' THEN CONVERT('o''k' USING utf8mb3) COLLATE utf8mb3_general_ci ELSE NULL END AS `REFERENCED_TABLE_SCHEMA`"), "{sql}");
    let sql =
        run("SELECT VIEW_DEFINITION FROM information_schema.VIEWS WHERE TABLE_SCHEMA = 'billing'");
    assert!(sql.contains("REPLACE(REPLACE(`VIEW_DEFINITION`, '`t42_d1_g1_s1`', '`billing`'), '`t42_d2_g2_s2`', '`crm`')"), "{sql}");
}

#[test]
fn literals_follow_sql_mode() {
    let sql = run_with(
        "SELECT * FROM information_schema.TABLES",
        StmtKind::Select,
        |c| {
            c.lex = LexOptions::mariadb()
                .no_backslash_escapes(true)
                .ansi_quotes(true);
        },
    )
    .unwrap()
    .unwrap();
    assert!(
        sql.contains("CONVERT('o''k' USING utf8mb3) COLLATE utf8mb3_general_ci AS `TABLE_SCHEMA`")
    );
    let map = MemoryNameMap::new(TenantId(42)).with_db(1, "a\\b", 1, 1);
    let reg = ColumnRegistry::mariadb_11_8();
    let ctx = ctx_with(&map, &reg, |_| {});
    let r = rewrite_information_schema(
        b"SELECT * FROM information_schema.TABLES",
        StmtKind::Select,
        &ctx,
    )
    .unwrap()
    .unwrap();
    assert!(
        String::from_utf8(r.sql)
            .unwrap()
            .contains("'a\\\\b' USING utf8mb3) COLLATE utf8mb3_general_ci AS")
    );
}

#[test]
fn aliases_and_three_part_names() {
    let sql = run("SELECT t.TABLE_NAME FROM information_schema.tables AS t");
    assert!(sql.contains(") AS t"), "{sql}");
    assert!(!sql.contains("AS `TABLES`"));
    let sql = run(
        "SELECT information_schema.TABLES.TABLE_NAME FROM information_schema.TABLES WHERE information_schema.TABLES.TABLE_SCHEMA = 'crm'",
    );
    assert!(
        sql.starts_with("SELECT TABLES.TABLE_NAME FROM (SELECT"),
        "{sql}"
    );
    assert!(
        sql.ends_with(") AS `TABLES` WHERE TABLES.TABLE_SCHEMA = 'crm'"),
        "{sql}"
    );
    // The qualified pushdown in that form still applies.
    assert_eq!(branches_of(&sql, "TABLE_SCHEMA"), ["t42_d2_g2_s2"]);
    let sql = run("SELECT * FROM `information_schema`.`TABLES` `x` JOIN d.t ON 1");
    assert!(sql.contains(") `x` JOIN d.t ON 1"), "{sql}");
}

#[test]
fn unqualified_tables_inside_information_schema() {
    let r = run_with(
        "SELECT * FROM TABLES t, COLUMNS WHERE 1",
        StmtKind::Select,
        |c| {
            c.in_information_schema = true;
        },
    )
    .unwrap()
    .unwrap();
    assert!(r.contains(") t, (SELECT"), "{r}");
    assert!(r.contains(") AS `COLUMNS` WHERE 1"), "{r}");
    // A CTE name is left to MariaDB.
    let r = run_with(
        "WITH x AS (SELECT 1) SELECT * FROM x",
        StmtKind::Select,
        |c| {
            c.in_information_schema = true;
        },
    )
    .unwrap();
    assert!(r.is_none());
    let e = run_with("SELECT * FROM INNODB_TRX", StmtKind::Select, |c| {
        c.in_information_schema = true;
    })
    .unwrap_err();
    assert_eq!(e.errno(), 1142);
}

#[test]
fn classes() {
    assert!(
        run_with(
            "SELECT * FROM information_schema.COLLATIONS",
            StmtKind::Select,
            |_| {}
        )
        .unwrap()
        .is_none()
    );
    let e = run_with(
        "SELECT * FROM information_schema.INNODB_TRX",
        StmtKind::Select,
        |_| {},
    )
    .unwrap_err();
    assert_eq!(e.errno(), 1142);
    assert_eq!(
        e.to_string(),
        "SELECT command denied to user 'alice'@'203.0.113.7' for table `information_schema`.`INNODB_TRX`"
    );
    let e = run_with(
        "SELECT * FROM information_schema.SPRIG_SETTINGS",
        StmtKind::Select,
        |_| {},
    )
    .unwrap_err();
    assert_eq!(e.errno(), 39051);
    let e = run_with(
        "SELECT * FROM information_schema.GLOBAL_STATUS.x",
        StmtKind::Select,
        |_| {},
    )
    .unwrap_err();
    assert_eq!(e.errno(), 1142);
    // Mapped tables inside a definition fail closed.
    let e = run_with(
        "CREATE VIEW v AS SELECT * FROM information_schema.TABLES",
        StmtKind::Ddl,
        |_| {},
    )
    .unwrap_err();
    assert_eq!(e.errno(), 39051);
    // SHOW and DESCRIBE of the views themselves pass.
    assert!(
        run_with(
            "SHOW COLUMNS FROM information_schema.TABLES",
            StmtKind::Show,
            |_| {}
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(classify_table("schemata"), TableClass::Schemata);
}

#[test]
fn registry() {
    let reg = ColumnRegistry::mariadb_11_8();
    assert_eq!(reg.columns("tables").unwrap().len(), 23);
    let r = ColumnRegistry::from_rows([("T".into(), "A".into()), ("t".into(), "B".into())]);
    assert_eq!(r.columns("T").unwrap(), ["A", "B"]);
    assert!(ColumnRegistry::load_sql().contains("TABLE_SCHEMA = 'information_schema'"));
    let map = acme();
    let empty = ColumnRegistry::default();
    let ctx = ctx_with(&map, &empty, |_| {});
    let e = rewrite_information_schema(
        b"SELECT * FROM information_schema.TABLES",
        StmtKind::Select,
        &ctx,
    )
    .unwrap_err();
    assert_eq!(e.errno(), 39051);
}

#[test]
fn schemata_and_processlist() {
    let sql = run("SELECT * FROM information_schema.SCHEMATA");
    assert!(
        sql.contains("CONVERT('information_schema' USING utf8mb3) COLLATE utf8mb3_general_ci AS `SCHEMA_NAME`"),
        "{sql}"
    );
    assert_eq!(
        branches_of(&sql, "SCHEMA_NAME"),
        ["t42_d1_g1_s1", "t42_d2_g2_s2", "t42_d3_g3_s5"]
    );
    let sql = run("SELECT * FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = 'crm'");
    assert_eq!(branches_of(&sql, "SCHEMA_NAME"), ["t42_d2_g2_s2"]);
    assert!(!sql.contains("'information_schema' AS"));
    let sql = run_with(
        "SELECT * FROM information_schema.SCHEMATA",
        StmtKind::Select,
        |c| {
            c.lex = LexOptions::mysql84();
        },
    )
    .unwrap()
    .unwrap();
    assert!(sql.contains(
        "CONVERT('performance_schema' USING utf8mb3) COLLATE utf8mb3_general_ci AS `SCHEMA_NAME`"
    ));
    let sql = run_with(
        "SELECT * FROM information_schema.SCHEMATA",
        StmtKind::Select,
        |c| {
            c.visible = Some(&|d: &LogicalDb| d.name == "crm");
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(branches_of(&sql, "SCHEMA_NAME"), ["t42_d2_g2_s2"]);
    let sql = run("SELECT USER FROM information_schema.PROCESSLIST");
    assert!(sql.contains("SELECT 9 AS `ID`, 'alice' AS `USER`, 'h:1' AS `HOST`, NULL AS `DB`, 'Sleep' AS `COMMAND`"), "{sql}");
    let e = run_with(
        "SELECT * FROM information_schema.PROCESSLIST",
        StmtKind::Select,
        |c| c.sessions = None,
    )
    .unwrap_err();
    assert_eq!(e.errno(), 39051);
}
