use sprig_common::ids::TenantId;

use super::*;
use crate::lex::{LexOptions, lex};
use crate::names::hide::Visibility;
use crate::names::infoschema::{ColumnRegistry, InfoSchemaCtx, rewrite_information_schema};
use crate::names::rewrite::StmtKind;
use crate::names::show::{ShowKind, classify};
use crate::names::{MemoryNameMap, NameError};

fn lit(s: &str) -> String {
    crate::names::rewrite::sql_string(s, false)
}

fn acme() -> MemoryNameMap {
    MemoryNameMap::new(TenantId(42)).with_db(1, "shop", 1, 1)
}

fn rewrite(sql: &str, persona: Persona) -> Result<Option<String>, NameError> {
    let map = acme();
    let reg = ColumnRegistry::mariadb_11_8();
    let ctx = InfoSchemaCtx {
        map: &map,
        lex: LexOptions::for_persona(persona),
        vis: Visibility::for_persona(persona),
        registry: &reg,
        sessions: None,
        visible: None,
        in_information_schema: false,
        user: Some(("alice", "203.0.113.7")),
        stored_names: false,
    };
    rewrite_information_schema(sql.as_bytes(), StmtKind::Select, &ctx)
        .map(|o| o.map(|r| String::from_utf8(r.sql).unwrap()))
}

/// The select list of a rewrite's first branch, as `AS` aliases or bare
/// column names, in order.
fn branch_columns(sql: &str) -> Vec<String> {
    let start = sql.find("(SELECT ").unwrap() + 8;
    let end = start + sql[start..].find(" FROM information_schema.").unwrap();
    let list = &sql[start..end];
    // Split on top-level commas.
    let mut out = Vec::new();
    let (mut depth, mut quote, mut from) = (0i32, false, 0usize);
    let b = list.as_bytes();
    for (i, &c) in b.iter().enumerate() {
        match c {
            b'\'' => quote = !quote,
            b'(' if !quote => depth += 1,
            b')' if !quote => depth -= 1,
            b',' if !quote && depth == 0 => {
                out.push(list[from..i].trim().to_owned());
                from = i + 1;
            }
            _ => {}
        }
    }
    out.push(list[from..].trim().to_owned());
    out.into_iter()
        .map(|item| {
            let name = item.rsplit(" AS ").next().unwrap_or(&item).to_owned();
            name.trim_matches('`').to_owned()
        })
        .collect()
}

#[test]
fn columns_take_mysqls_shape_on_3306() {
    let sql = rewrite(
        "SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'shop'",
        Persona::MySql84,
    )
    .unwrap()
    .unwrap();
    assert_eq!(branch_columns(&sql), mysql_column_names("COLUMNS").unwrap());
    assert!(sql.contains("LEFT JOIN (SELECT `TABLE_NAME` AS `_sprig_jt`"));
    assert!(sql.contains("WHERE `CONSTRAINT_SCHEMA` = 't42_d1_g1_s1'"));
    assert!(sql.contains("IF((`DATA_TYPE` = 'longtext' AND `_sprig_jc` IS NOT NULL), 'json', `DATA_TYPE`) AS `DATA_TYPE`"));
    // MariaDB's own columns are gone.
    assert!(!sql.contains("IS_SYSTEM_TIME_PERIOD_START"));
    // The schema is still the logical name.
    assert!(
        sql.contains("CONVERT('shop' USING utf8mb3) COLLATE utf8mb3_general_ci AS `TABLE_SCHEMA`")
    );
    // 3307 keeps MariaDB's shape and no join.
    let sql = rewrite("SELECT * FROM information_schema.COLUMNS", Persona::MariaDb)
        .unwrap()
        .unwrap();
    assert!(sql.contains("`IS_SYSTEM_TIME_PERIOD_START`"));
    assert!(!sql.contains("_sprig_jc"));
}

#[test]
fn other_mysql_shapes() {
    let sql = rewrite(
        "SELECT * FROM information_schema.STATISTICS",
        Persona::MySql84,
    )
    .unwrap()
    .unwrap();
    let cols = branch_columns(&sql);
    assert_eq!(cols[cols.len() - 2..], ["IS_VISIBLE", "EXPRESSION"]);
    // Hiding still reads MariaDB's columns in the branch's WHERE.
    assert!(sql.contains("LEFT(`INDEX_NAME`, 11) <> 'sprig_auto_'"));

    let sql = rewrite(
        "SELECT * FROM information_schema.CHECK_CONSTRAINTS",
        Persona::MySql84,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        branch_columns(&sql),
        [
            "CONSTRAINT_CATALOG",
            "CONSTRAINT_SCHEMA",
            "CONSTRAINT_NAME",
            "CHECK_CLAUSE"
        ]
    );
    assert!(sql.contains("AND NOT (`LEVEL` = 'Column' AND `CHECK_CLAUSE` = CONCAT('json_valid(`'"));

    let sql = rewrite(
        "SELECT * FROM information_schema.TABLE_CONSTRAINTS",
        Persona::MySql84,
    )
    .unwrap()
    .unwrap();
    assert_eq!(branch_columns(&sql).last().unwrap(), "ENFORCED");
    assert!(
        sql.contains(
            "AND NOT (`CONSTRAINT_TYPE` = 'CHECK' AND (`TABLE_NAME`, `CONSTRAINT_NAME`) IN"
        )
    );

    let sql = rewrite("SELECT * FROM information_schema.TABLES", Persona::MySql84)
        .unwrap()
        .unwrap();
    let cols = branch_columns(&sql);
    assert_eq!(cols.len(), 21);
    assert!(!cols.contains(&"TEMPORARY".to_owned()));

    let sql = rewrite(
        "SELECT * FROM information_schema.SCHEMATA",
        Persona::MySql84,
    )
    .unwrap()
    .unwrap();
    assert!(sql.contains("'NO' AS `DEFAULT_ENCRYPTION`"));
    assert!(!sql.contains("SCHEMA_COMMENT"));
}

#[test]
fn keywords_carry_reserved_on_3306() {
    let my = rewrite(
        "SELECT WORD FROM INFORMATION_SCHEMA.KEYWORDS WHERE RESERVED=1 ORDER BY WORD",
        Persona::MySql84,
    )
    .unwrap()
    .unwrap();
    assert!(my.starts_with(
        "SELECT WORD FROM (SELECT `WORD`, `RESERVED` FROM JSON_TABLE('[[\"ACCESSIBLE\",1],"
    ));
    assert!(my.ends_with(") AS `KEYWORDS` WHERE RESERVED=1 ORDER BY WORD"));
    // 3307 relays MariaDB's own table, as MariaDB 11.8 has it.
    assert_eq!(
        rewrite(
            "SELECT k.word FROM information_schema.keywords k",
            Persona::MariaDb,
        )
        .unwrap(),
        None
    );
    assert_eq!(keywords::MYSQL84.len(), 734);
    assert_eq!(keywords::MYSQL84.iter().filter(|(_, r)| *r).count(), 262);
}

#[test]
fn mysql_only_tables_on_3306() {
    let sql = rewrite(
        "SELECT COLUMN_NAME FROM information_schema.COLUMN_STATISTICS WHERE SCHEMA_NAME = 'shop'",
        Persona::MySql84,
    )
    .unwrap()
    .unwrap();
    assert!(sql.contains("FROM DUAL WHERE 0) AS `COLUMN_STATISTICS`"));
    // On 3307 it is a server-wide table MariaDB doesn't have: 1142 as before.
    let e = rewrite(
        "SELECT * FROM information_schema.COLUMN_STATISTICS",
        Persona::MariaDb,
    )
    .unwrap_err();
    assert_eq!(e.errno(), 1142);
    let sql = rewrite(
        "SELECT ID FROM information_schema.COLLATIONS WHERE COLLATION_NAME = 'utf8mb4_0900_ai_ci'",
        Persona::MySql84,
    )
    .unwrap()
    .unwrap();
    assert!(sql.contains("AS `PAD_ATTRIBUTE` FROM information_schema.COLLATIONS"));
    // Relayed unchanged on 3307.
    assert_eq!(
        rewrite(
            "SELECT * FROM information_schema.COLLATIONS",
            Persona::MariaDb
        )
        .unwrap(),
        None
    );
    // Three-part column references lose their schema with the view.
    let sql = rewrite(
        "SELECT information_schema.KEYWORDS.WORD FROM information_schema.KEYWORDS",
        Persona::MySql84,
    )
    .unwrap()
    .unwrap();
    assert!(sql.starts_with("SELECT KEYWORDS.WORD FROM (SELECT"));
}

#[test]
fn show_columns_becomes_a_query() {
    let text = b"SHOW FULL COLUMNS FROM `t42_d1_g1_s1`.`j` LIKE 'k%'";
    let toks = lex(text, &LexOptions::mysql84()).unwrap();
    let info = classify(text, &toks, false).unwrap();
    assert_eq!(info.kind, ShowKind::Columns);
    let sql = show_columns_rewrite(&info, text, None, &acme(), &lit).unwrap();
    let sql = String::from_utf8(sql).unwrap();
    assert!(sql.starts_with("SELECT `Field`, `Type`, `Collation`, `Null`, `Key`, `Default`, `Extra`, `Privileges`, `Comment` FROM (SELECT `COLUMN_NAME` AS `Field`"));
    assert!(sql.contains("WHERE `TABLE_SCHEMA` = 't42_d1_g1_s1' AND `TABLE_NAME` = 'j' AND (SELECT 1 FROM `t42_d1_g1_s1`.`j` LIMIT 0) IS NULL"));
    assert!(sql.ends_with(") AS `COLUMNS` WHERE `Field` LIKE 'k%' ORDER BY `_sprig_pos`"));

    // Unqualified: the current database's schema; WHERE is kept.
    let text = b"SHOW COLUMNS FROM j WHERE `Type` = 'json';";
    let toks = lex(text, &LexOptions::mysql84()).unwrap();
    let info = classify(text, &toks, false).unwrap();
    let sql = show_columns_rewrite(&info, text, Some("shop"), &acme(), &lit).unwrap();
    let sql = String::from_utf8(sql).unwrap();
    assert!(sql.starts_with("SELECT `Field`, `Type`, `Null`, `Key`, `Default`, `Extra` FROM"));
    assert!(sql.ends_with(") AS `COLUMNS` WHERE `Type` = 'json' ORDER BY `_sprig_pos`"));
    // No database: MariaDB answers (1046).
    assert!(show_columns_rewrite(&info, text, None, &acme(), &lit).is_none());
}

#[test]
fn describe_targets() {
    let t = |s: &str| {
        let toks = lex(s.as_bytes(), &LexOptions::mysql84()).unwrap();
        describe_target(s.as_bytes(), &toks, false)
    };
    assert_eq!(
        t("DESCRIBE j"),
        Some(DescribeTarget {
            schema: None,
            table: "j".into(),
            like: None
        })
    );
    assert_eq!(
        t("desc `t42_d1_g1_s1`.`j` 'k%';"),
        Some(DescribeTarget {
            schema: Some("t42_d1_g1_s1".into()),
            table: "j".into(),
            like: Some("k%".into())
        })
    );
    assert_eq!(t("EXPLAIN j id").unwrap().like.as_deref(), Some("id"));
    assert_eq!(t("EXPLAIN SELECT 1"), None);
    assert_eq!(t("EXPLAIN FORMAT=JSON SELECT 1"), None);
    assert_eq!(t("DESCRIBE j k extra"), None);
}

/// MariaDB 11.8's `SHOW CREATE TABLE` of the table of
/// `tests/compat_live.rs`, captured on 8 November 2026.
const MARIADB_J: &str = "CREATE TABLE `j` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `j` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`j`)),
  `k` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL CHECK (json_valid(`k`)),
  `s` varchar(10) DEFAULT 'a''b',
  `f` tinyint(1) DEFAULT NULL,
  `z` int(10) unsigned zerofill DEFAULT NULL,
  `u` bigint(20) unsigned NOT NULL,
  `d` datetime DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `d2` datetime(3) DEFAULT current_timestamp(3),
  `y` year(4) DEFAULT NULL,
  `c` varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_uca1400_ai_ci DEFAULT NULL,
  `l` longtext DEFAULT NULL,
  `h` int(11) DEFAULT NULL INVISIBLE,
  PRIMARY KEY (`id`),
  KEY `ix` (`s`) IGNORED,
  KEY `iy` (`u`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

#[test]
fn create_table_in_mysqls_spelling() {
    let out = mysql_create_table(MARIADB_J);
    assert_eq!(
        out,
        "CREATE TABLE `j` (
  `id` int NOT NULL AUTO_INCREMENT,
  `j` json DEFAULT NULL,
  `k` json NOT NULL,
  `s` varchar(10) DEFAULT 'a''b',
  `f` tinyint(1) DEFAULT NULL,
  `z` int(10) unsigned zerofill DEFAULT NULL,
  `u` bigint unsigned NOT NULL,
  `d` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `d2` datetime(3) DEFAULT CURRENT_TIMESTAMP(3),
  `y` year DEFAULT NULL,
  `c` varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `l` longtext DEFAULT NULL,
  `h` int DEFAULT NULL /*!80023 INVISIBLE */,
  PRIMARY KEY (`id`),
  KEY `ix` (`s`) /*!80000 INVISIBLE */,
  KEY `iy` (`u`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
    );
    // Views and anything else pass.
    let v = "CREATE ALGORITHM=UNDEFINED VIEW `v` AS select 1";
    assert_eq!(mysql_create_table(v), v);
}

#[test]
fn the_view_keeps_the_clients_spelling_of_the_table() {
    // SQLAlchemy's reflection query: the derived table must answer to
    // `columns`, as MariaDB compares table aliases case-sensitively.
    let sql = rewrite(
        "SELECT information_schema.columns.table_schema FROM information_schema.columns \
         WHERE information_schema.columns.table_schema = 'shop'",
        Persona::MySql84,
    )
    .unwrap()
    .unwrap();
    assert!(sql.starts_with("SELECT columns.table_schema FROM (SELECT"));
    assert!(sql.ends_with(") AS `columns` WHERE columns.table_schema = 'shop'"));
}

#[test]
fn show_engines_is_mysqls_list() {
    assert!(is_show_engines(b"SHOW ENGINES"));
    assert!(is_show_engines(b"show storage engines;"));
    assert!(!is_show_engines(b"SHOW ENGINE INNODB STATUS"));
    let lit = |v: &str| format!("'{v}'");
    let sql = String::from_utf8(show_engines_sql(&lit)).unwrap();
    assert!(sql.contains("'MRG_MYISAM' AS `e`"), "{sql}");
    assert!(!sql.contains("SEQUENCE") && !sql.contains("Aria"), "{sql}");
    assert!(sql.ends_with("ORDER BY m.`n`"), "{sql}");
}

#[test]
fn show_collation_reads_mysqls_collations() {
    let lit = |v: &str| format!("'{v}'");
    let s = |q: &str| show_collation_sql(q.as_bytes(), &lit).map(|v| String::from_utf8(v).unwrap());
    let all = s("SHOW COLLATION").unwrap();
    assert!(all.contains("`PAD_ATTRIBUTE` AS `Pad_attribute`"), "{all}");
    assert!(all.ends_with("ORDER BY `Collation`"), "{all}");
    assert!(
        s("show collation like 'utf8mb4%';")
            .unwrap()
            .contains(" WHERE `Collation` LIKE 'utf8mb4%' ORDER BY")
    );
    assert!(
        s("SHOW COLLATION WHERE Charset = 'latin1'")
            .unwrap()
            .contains(" WHERE Charset = 'latin1' ORDER BY")
    );
    assert!(s("SHOW COLLATIONS").is_none());
    assert!(s("SHOW CHARACTER SET").is_none());
}
