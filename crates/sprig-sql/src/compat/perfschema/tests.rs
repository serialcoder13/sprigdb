use super::*;
use sprig_proto_mysql::packets::Persona;

fn vars() -> PersonaVars {
    PersonaVars::new(Persona::MySql84, "8.4.8-SprigDB")
        .with_session(Some("STRICT_TRANS_TABLES"), Some("0.5"))
}

fn session<'a>(
    v: &'a PersonaVars,
    attrs: &'a [(String, String)],
    db: Option<&'a str>,
) -> PsSession<'a> {
    static STATUS: &[(&str, String)] = &[];
    PsSession {
        vars: v,
        owned_vars: &[],
        status: STATUS,
        uptime: 42,
        connection_id: 7,
        connect_attrs: attrs,
        current_db: db,
    }
}

fn rw(sql: &str, db: Option<&str>) -> Option<Result<String, WireError>> {
    let v = vars();
    let attrs = vec![
        ("_client_name".to_owned(), "libmysql".to_owned()),
        ("program_name".to_owned(), "mysql".to_owned()),
    ];
    let s = session(&v, &attrs, db);
    rewrite(sql.as_bytes(), &LexOptions::mysql84(), &s)
        .map(|r| r.map(|b| String::from_utf8(b).unwrap()))
}

fn ok(sql: &str) -> String {
    rw(sql, None).expect("rewritten").expect("no error")
}

#[test]
fn leaves_other_statements_alone() {
    assert!(rw("SELECT 1", None).is_none());
    assert!(rw("SELECT a FROM sysx.t", None).is_none());
    assert!(rw("SELECT thread_id FROM t", None).is_none());
    assert!(rw("SELECT 'performance_schema.threads'", None).is_none());
}

#[test]
fn every_catalog_object_has_columns() {
    assert_eq!(catalog::OBJECTS.len(), 215);
    for (s, t, cols) in catalog::OBJECTS {
        assert!(!cols.is_empty(), "{s}.{t}");
        for c in cols.split(';') {
            assert!(c.contains(':'), "{s}.{t}: {c}");
        }
    }
}

#[test]
fn session_variables_carry_the_persona() {
    let sql = ok(
        "SELECT VARIABLE_VALUE FROM performance_schema.session_variables WHERE VARIABLE_NAME = 'version'",
    );
    assert!(
        sql.starts_with("SELECT VARIABLE_VALUE FROM (SELECT LOWER(VARIABLE_NAME) AS VARIABLE_NAME"),
        "{sql}"
    );
    assert!(sql.contains("FROM information_schema.SESSION_VARIABLES WHERE LOWER(VARIABLE_NAME) NOT IN ('version', "), "{sql}");
    assert!(
        sql.contains("UNION ALL SELECT 'version', '8.4.8-SprigDB'"),
        "{sql}"
    );
    assert!(
        sql.contains("UNION ALL SELECT 'sql_mode', 'STRICT_TRANS_TABLES'"),
        "{sql}"
    );
    assert!(
        sql.contains("UNION ALL SELECT 'max_execution_time', '500'"),
        "{sql}"
    );
    assert!(
        sql.contains("UNION ALL SELECT 'performance_schema', 'ON'"),
        "{sql}"
    );
    assert!(
        sql.contains("WHEN 'utf8mb4_uca1400_ai_ci' THEN 'utf8mb4_0900_ai_ci'"),
        "{sql}"
    );
    assert!(
        sql.ends_with(") AS `session_variables` WHERE VARIABLE_NAME = 'version'"),
        "{sql}"
    );
    // Global: MySQL's default sql_mode.
    let g = ok("SELECT * FROM performance_schema.global_variables g");
    assert!(
        g.contains("UNION ALL SELECT 'sql_mode', 'ONLY_FULL_GROUP_BY,"),
        "{g}"
    );
    assert!(g.ends_with(") g"), "{g}");
}

#[test]
fn status_names_in_mysqls_case() {
    let v = vars();
    let status = [
        ("Com_select", "3".to_owned()),
        ("Questions", "9".to_owned()),
    ];
    let s = PsSession {
        status: &status,
        ..session(&v, &[], None)
    };
    let sql = String::from_utf8(
        rewrite(
            b"SELECT * FROM performance_schema.session_status",
            &LexOptions::mysql84(),
            &s,
        )
        .unwrap()
        .unwrap(),
    )
    .unwrap();
    assert!(
        sql.contains("CONCAT(UPPER(LEFT(VARIABLE_NAME, 1)), LOWER(SUBSTR(VARIABLE_NAME, 2)))"),
        "{sql}"
    );
    assert!(
        sql.contains(
            "<> 'COM_' AND UPPER(VARIABLE_NAME) NOT IN ('QUESTIONS', 'THREADS_CONNECTED')"
        ),
        "{sql}"
    );
    assert!(
        sql.contains("UNION ALL SELECT 'Questions', '9' UNION ALL SELECT 'Threads_connected'"),
        "{sql}"
    );
    assert!(!sql.contains("'Com_select'"), "{sql}");
    let g = ok("SELECT * FROM performance_schema.global_status");
    assert!(g.contains("SELECT 'Uptime', '42'"), "{g}");
    assert!(g.contains("SELECT 'Threads_connected', (SELECT CAST(COUNT(*) AS CHAR) FROM information_schema.PROCESSLIST)"), "{g}");
}

#[test]
fn threads_come_from_the_tenants_processlist() {
    let sql = ok(
        "SELECT THREAD_ID FROM performance_schema.threads WHERE PROCESSLIST_ID = CONNECTION_ID()",
    );
    assert!(
        sql.contains("FROM information_schema.PROCESSLIST) AS `threads` WHERE"),
        "{sql}"
    );
    let sql = ok(
        "SELECT t.thread_id, performance_schema.threads.name FROM `performance_schema`.`threads` t",
    );
    assert!(!sql.contains("performance_schema.threads.name"), "{sql}");
    assert!(sql.contains(", `threads`.name FROM (SELECT"), "{sql}");
    assert!(sql.ends_with("information_schema.PROCESSLIST) t"), "{sql}");
}

#[test]
fn connect_attrs_are_the_sessions() {
    let sql = ok(
        "SELECT ATTR_NAME, ATTR_VALUE FROM performance_schema.session_account_connect_attrs WHERE PROCESSLIST_ID = 7",
    );
    assert!(sql.contains("SELECT CAST(7 AS UNSIGNED) AS PROCESSLIST_ID, '_client_name' AS ATTR_NAME, 'libmysql' AS ATTR_VALUE, 0 AS ORDINAL_POSITION UNION ALL SELECT CAST(7 AS UNSIGNED) AS PROCESSLIST_ID, 'program_name'"), "{sql}");
}

#[test]
fn other_tables_are_empty_with_mysqls_columns() {
    let sql = ok("SELECT * FROM performance_schema.events_statements_summary_by_digest");
    assert!(sql.starts_with("SELECT * FROM (SELECT CAST(NULL AS CHAR(64)) AS `SCHEMA_NAME`, CAST(NULL AS CHAR(64)) AS `DIGEST`"), "{sql}");
    assert!(
        sql.ends_with(" LIMIT 0) AS `events_statements_summary_by_digest`"),
        "{sql}"
    );
    let sql = ok("SELECT * FROM sys.`x$schema_index_statistics`");
    assert!(
        sql.contains("LIMIT 0) AS `x$schema_index_statistics`"),
        "{sql}"
    );
}

#[test]
fn unknown_tables_fail_as_on_mysql() {
    let e = rw("SELECT * FROM performance_schema.nosuch", None)
        .unwrap()
        .unwrap_err();
    assert_eq!(e.code, 1146);
    assert_eq!(e.message, "Table 'performance_schema.nosuch' doesn't exist");
    let e = rw("SELECT * FROM sys.nosuch", None).unwrap().unwrap_err();
    assert_eq!(e.message, "Table 'sys.nosuch' doesn't exist");
}

#[test]
fn sys_views() {
    let sql = ok("SELECT * FROM sys.version");
    assert_eq!(
        sql,
        "SELECT * FROM (SELECT '2.1.3' AS sys_version, '8.4.8' AS mysql_version) AS `version`"
    );
    let sql = ok("SELECT conn_id, program_name FROM sys.session");
    assert!(
        sql.contains("CASE ID WHEN 7 THEN 'mysql' END AS program_name"),
        "{sql}"
    );
    assert!(
        sql.contains("FROM information_schema.PROCESSLIST WHERE COMMAND <> 'Daemon') AS `session`"),
        "{sql}"
    );
    let sql = ok("SELECT * FROM sys.schema_object_overview");
    assert!(
        sql.contains("FROM information_schema.STATISTICS GROUP BY TABLE_SCHEMA, INDEX_TYPE"),
        "{sql}"
    );
}

#[test]
fn unqualified_names_in_the_current_schema() {
    let sql = rw(
        "SELECT * FROM threads JOIN session_variables v ON 1, events_waits_current",
        Some("performance_schema"),
    )
    .unwrap()
    .unwrap();
    assert!(
        sql.starts_with("SELECT * FROM (SELECT CAST(ID AS UNSIGNED) AS THREAD_ID"),
        "{sql}"
    );
    assert!(
        sql.contains(") AS `threads` JOIN (SELECT LOWER(VARIABLE_NAME)"),
        "{sql}"
    );
    assert!(sql.contains(") v ON 1, (SELECT"), "{sql}");
    assert!(sql.ends_with("LIMIT 0) AS `events_waits_current`"), "{sql}");
    // A column named like a table isn't one; an unknown table fails.
    let sql = rw(
        "SELECT threads FROM session_status",
        Some("performance_schema"),
    )
    .unwrap()
    .unwrap();
    assert!(
        sql.starts_with("SELECT threads FROM (SELECT CONCAT("),
        "{sql}"
    );
    let e = rw("SELECT a FROM t", Some("performance_schema"))
        .unwrap()
        .unwrap_err();
    assert_eq!(e.message, "Table 'performance_schema.t' doesn't exist");
}

#[test]
fn functions() {
    let sql = ok("SELECT FORMAT_BYTES(1024) AS b");
    assert!(sql.starts_with("SELECT (CASE WHEN (1024) IS NULL THEN NULL WHEN ABS((1024)) >= 1152921504606846976 THEN"), "{sql}");
    assert!(sql.ends_with("' bytes') END) AS b"), "{sql}");
    // A whole item keeps the call's text as its name.
    let sql = ok("SELECT FORMAT_BYTES(1), sys.format_time(2) FROM t");
    assert!(sql.contains(" END) AS `FORMAT_BYTES(1)`, (CASE"), "{sql}");
    assert!(
        sql.ends_with(" END) AS `sys.format_time(2)` FROM t"),
        "{sql}"
    );
    let sql = ok("SELECT CONCAT(FORMAT_BYTES(1), 'x')");
    assert!(!sql.contains("AS `"), "{sql}");
    let sql = ok("SELECT sys.format_bytes(size) FROM t");
    assert!(
        sql.contains(
            " WHEN ABS((size)) >= 1024 THEN CONCAT(CAST((size) / 1024 AS DECIMAL(65,2)), ' KiB')"
        ),
        "{sql}"
    );
    let sql = ok("SELECT FORMAT_PICO_TIME(t) FROM x");
    assert!(sql.contains("' min')"), "{sql}");
    let sql = ok("SELECT sys.format_time(t) FROM x");
    assert!(
        sql.contains("CAST(CAST((t) AS DOUBLE) / 604800000000000000 AS DECIMAL(65,2)))), ' w')"),
        "{sql}"
    );
    assert_eq!(
        ok("SELECT PS_CURRENT_THREAD_ID() AS t"),
        "SELECT CAST(CONNECTION_ID() AS UNSIGNED) AS t"
    );
    assert_eq!(
        ok("SELECT sys.version_major(), sys.version_minor()"),
        "SELECT 8 AS `sys.version_major()`, 4 AS `sys.version_minor()`"
    );
    assert_eq!(
        ok("SELECT sys.quote_identifier('a`b') q"),
        "SELECT CONCAT('`', REPLACE(('a`b'), '`', '``'), '`') q"
    );
    // Nested: the inner call too.
    let sql = ok("SELECT FORMAT_BYTES(FORMAT_BYTES(1)) AS n");
    assert!(!sql.to_ascii_uppercase().contains("FORMAT_BYTES"), "{sql}");
    // A parameter is used once: left alone.
    assert!(rw("SELECT FORMAT_BYTES(?)", None).is_none());
}
