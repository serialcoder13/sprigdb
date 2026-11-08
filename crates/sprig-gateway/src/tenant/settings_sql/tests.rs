//! `information_schema.SPRIG_SETTINGS` inlining: where it may go (chapter
//! 07 rule 9) and the 39052 message.

#![allow(clippy::unwrap_used)]

use sprig_sql::lex::LexOptions;

use super::*;

const VIEW: &str = "(SELECT 1 AS `VALUE`)";

fn inline(sql: &str, lex: LexOptions) -> Option<Result<String>> {
    inline_view(sql.as_bytes(), &lex, || Ok(VIEW.to_owned()))
        .map(|r| r.map(|b| String::from_utf8(b).unwrap()))
}

fn code(r: Option<Result<String>>) -> u16 {
    r.unwrap().unwrap_err().protocol().mysql_errno
}

#[test]
fn statements_that_run_now_get_the_rows() {
    for lex in [LexOptions::mariadb(), LexOptions::mysql84()] {
        for (sql, want) in [
            (
                "SELECT * FROM information_schema.SPRIG_SETTINGS",
                "SELECT * FROM (SELECT 1 AS `VALUE`) AS `SPRIG_SETTINGS`",
            ),
            (
                "select VALUE from INFORMATION_SCHEMA.`sprig_settings` s where 1",
                "select VALUE from (SELECT 1 AS `VALUE`) s where 1",
            ),
            (
                "SET @v = (SELECT VALUE FROM information_schema.SPRIG_SETTINGS LIMIT 1)",
                "SET @v = (SELECT VALUE FROM (SELECT 1 AS `VALUE`) AS `SPRIG_SETTINGS` LIMIT 1)",
            ),
            (
                "INSERT INTO d.t SELECT VALUE FROM information_schema.SPRIG_SETTINGS",
                "INSERT INTO d.t SELECT VALUE FROM (SELECT 1 AS `VALUE`) AS `SPRIG_SETTINGS`",
            ),
            (
                "SELECT 1; WITH x AS (SELECT * FROM information_schema.SPRIG_SETTINGS) SELECT * FROM x;",
                "SELECT 1; WITH x AS (SELECT * FROM (SELECT 1 AS `VALUE`) AS `SPRIG_SETTINGS`) SELECT * FROM x;",
            ),
        ] {
            assert_eq!(inline(sql, lex).unwrap().unwrap(), want, "{sql}");
        }
        // No reference: nothing to do, whatever the statement.
        assert!(inline("CREATE VIEW v AS SELECT 1", lex).is_none());
        // Inside a string (a PREPARE text): not inlined here; `plan`
        // refuses it.
        assert!(
            inline(
                "PREPARE s FROM 'SELECT * FROM information_schema.SPRIG_SETTINGS'",
                lex
            )
            .is_none()
        );
    }
}

/// Rule 9: the rows would be stored in the object's definition.
#[test]
fn stored_definitions_and_compound_statements_fail_with_39051() {
    for lex in [LexOptions::mariadb(), LexOptions::mysql84()] {
        for sql in [
            "CREATE VIEW v AS SELECT * FROM information_schema.SPRIG_SETTINGS",
            "create or replace view v as select VALUE from information_schema.sprig_settings",
            "ALTER VIEW v AS SELECT * FROM information_schema.SPRIG_SETTINGS",
            "CREATE TABLE t AS SELECT * FROM information_schema.SPRIG_SETTINGS",
            "CREATE PROCEDURE p() SELECT * FROM information_schema.SPRIG_SETTINGS",
            "CREATE PROCEDURE p() BEGIN SELECT 1; SELECT * FROM information_schema.SPRIG_SETTINGS; END",
            "CREATE FUNCTION f() RETURNS INT RETURN (SELECT COUNT(*) FROM information_schema.SPRIG_SETTINGS)",
            "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW SET @n = (SELECT COUNT(*) FROM information_schema.SPRIG_SETTINGS)",
            "CREATE EVENT e ON SCHEDULE EVERY 1 DAY DO SELECT * FROM information_schema.SPRIG_SETTINGS",
            "BEGIN NOT ATOMIC SELECT * FROM information_schema.SPRIG_SETTINGS; END",
            "lbl: BEGIN NOT ATOMIC SELECT * FROM information_schema.SPRIG_SETTINGS; END",
            "IF 1 THEN SELECT * FROM information_schema.SPRIG_SETTINGS; END IF",
            "SET STATEMENT max_statement_time = 1 FOR CREATE VIEW v AS SELECT * FROM information_schema.SPRIG_SETTINGS",
            "SELECT 1; CREATE VIEW v AS SELECT * FROM information_schema.SPRIG_SETTINGS",
            "/*!50001 CREATE VIEW v AS SELECT * FROM information_schema.SPRIG_SETTINGS */",
        ] {
            assert_eq!(code(inline(sql, lex)), 39051, "{sql}");
        }
    }
}

/// The view is never built for a refused statement.
#[test]
fn a_refused_statement_never_builds_the_rows() {
    let r = inline_view(
        b"CREATE VIEW v AS SELECT * FROM information_schema.SPRIG_SETTINGS",
        &LexOptions::mariadb(),
        || panic!("the view was built"),
    );
    assert!(r.unwrap().is_err());
}

#[test]
fn the_too_large_message_reads_as_one_sentence() {
    let m = too_large().message;
    assert!(!m.contains("  "), "{m:?}");
    assert!(m.ends_with("rows; use SHOW SPRIG SETTINGS ... FOR TABLE or FOR DATABASE"), "{m:?}");
}
