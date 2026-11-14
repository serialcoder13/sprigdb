use sprig_common::ids::TenantId;
use sprig_proto_mysql::packets::Persona;

use super::*;
use crate::names::MemoryNameMap;

#[test]
fn edits_apply_in_order() {
    let e = |r: Range<usize>, s: &str| Edit {
        range: r,
        kind: EditKind::Database,
        replacement: s.to_owned(),
    };
    assert_eq!(
        apply_edits(b"abcdef", &[e(0..1, "X"), e(3..5, "")]),
        b"Xbcf"
    );
    assert_eq!(apply_edits(b"abc", &[]), b"abc");
    assert_eq!(apply_edits(b"abc", &[e(3..3, "!")]), b"abc!");
}

#[test]
fn deep_nesting_fails_closed_without_overflowing() {
    let m = MemoryNameMap::new(TenantId(1)).with_db(1, "d", 1, 1);
    let ctx = RewriteCtx::new(&m, Persona::MySql84);
    let deep = format!("SELECT {}1{}", "(SELECT ".repeat(5000), ")".repeat(5000));
    assert_eq!(rewrite(deep.as_bytes(), &ctx).unwrap_err().errno(), 39051);
    let deep = format!("SELECT * FROM {}d.t{}", "(".repeat(5000), ")".repeat(5000));
    assert_eq!(rewrite(deep.as_bytes(), &ctx).unwrap_err().errno(), 39051);
    let deep = format!("BEGIN NOT ATOMIC {} END", "BEGIN ".repeat(5000));
    assert_eq!(rewrite(deep.as_bytes(), &ctx).unwrap_err().errno(), 39051);
    // Deep but bounded is fine.
    let ok = format!(
        "SELECT {}1{} FROM d.t",
        "(SELECT ".repeat(40),
        ")".repeat(40)
    );
    assert!(rewrite(ok.as_bytes(), &ctx).is_ok());
}

#[test]
fn deep_statements_never_overflow_the_ast_path() {
    let m = MemoryNameMap::new(TenantId(1)).with_db(1, "d", 1, 1);
    let ctx = RewriteCtx::new(&m, Persona::MySql84);
    let cases = [
        format!(
            "SELECT {}1{} FROM d.t",
            "(SELECT ".repeat(12),
            ")".repeat(12)
        ),
        format!("SELECT {}1{} FROM d.t", "(".repeat(150), ")".repeat(150)),
        format!("SELECT {}x FROM d.t", "NOT ".repeat(150)),
        format!("SELECT {}x FROM d.t", "- ".repeat(150)),
        format!(
            "SELECT {}1{} FROM d.t",
            "CASE WHEN a THEN ".repeat(150),
            " END".repeat(150)
        ),
        format!(
            "SELECT * FROM d.t WHERE {}",
            vec!["a = 1"; 2000].join(" AND ")
        ),
        format!(
            "SELECT * FROM d.t WHERE {}",
            vec!["a = 1"; 98].join(" AND ")
        ),
        format!("SELECT * FROM {}", vec!["d.t"; 500].join(" JOIN ")),
    ];
    for c in cases {
        let _ = rewrite(c.as_bytes(), &ctx);
    }
}

#[test]
fn statement_ranges_keep_the_executable_comments_they_open() {
    let m = MemoryNameMap::new(TenantId(1)).with_db(1, "d", 1, 1);
    for persona in [Persona::MySql84, Persona::MariaDb] {
        let ctx = RewriteCtx::new(&m, persona);
        let sql = b"START TRANSACTION /*!40100 WITH CONSISTENT SNAPSHOT */; SELECT 1";
        let r = rewrite(sql, &ctx).unwrap();
        let first = &r.statements[0];
        assert_eq!(
            &sql[first.range.clone()],
            b"START TRANSACTION /*!40100 WITH CONSISTENT SNAPSHOT */"
        );
        // A statement wholly inside a comment keeps the comment.
        let sql = b"/*!40101 SET NAMES utf8mb4 */";
        let r = rewrite(sql, &ctx).unwrap();
        assert_eq!(&sql[r.statements[0].range.clone()], &sql[..]);
        // Dump tools' triggers and views: several comments, one statement.
        let sql = b"/*!50003 CREATE*/ /*!50017 DEFINER=`app`@`%`*/ /*!50003 TRIGGER tr BEFORE INSERT ON t FOR EACH ROW SET NEW.a = 1 */";
        let r = rewrite(sql, &ctx).unwrap();
        assert_eq!(r.statements.len(), 1);
        assert_eq!(&sql[r.statements[0].range.clone()], &sql[..]);
        // Two statements in one comment keep their own ranges.
        let sql = b"/*!40101 SET @a = 1; SET @b = 2 */";
        let r = rewrite(sql, &ctx).unwrap();
        assert_eq!(&sql[r.statements[0].range.clone()], b"SET @a = 1");
    }
}

/// Account functions and `DATABASE()` in a table's
/// definition would run inside MariaDB as the backend account; no lookup
/// can replace them there (no subqueries in defaults), so they fail.
#[test]
fn account_functions_in_table_definitions_fail() {
    let m = MemoryNameMap::new(TenantId(1)).with_db(1, "d", 1, 1);
    let ctx = RewriteCtx::new(&m, Persona::MySql84).current_db(Some("d"));
    for sql in [
        "CREATE TABLE t (u VARCHAR(80) DEFAULT (CURRENT_USER()))",
        "CREATE TABLE t (u VARCHAR(80) DEFAULT (current_user))",
        "CREATE TABLE d.t (u VARCHAR(80) AS (USER()) VIRTUAL)",
        "ALTER TABLE t ADD COLUMN s VARCHAR(80) DEFAULT (DATABASE())",
        "CREATE TABLE t (u VARCHAR(80), CHECK (u <> SESSION_USER()))",
    ] {
        assert_eq!(
            rewrite(sql.as_bytes(), &ctx).unwrap_err().errno(),
            39051,
            "{sql}"
        );
    }
    for sql in [
        "CREATE TABLE t (u VARCHAR(80) DEFAULT 'x', ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE t AS SELECT CURRENT_USER() AS u, DATABASE() AS d",
        "INSERT INTO t VALUES (CURRENT_USER())",
    ] {
        assert!(rewrite(sql.as_bytes(), &ctx).is_ok(), "{sql}");
    }
}

#[test]
fn only_the_rls_subtree_view_of_sprig_sys_passes() {
    // Row-level security: the rewrite's subtree subquery stays as written;
    // every other `__sprig_sys` name is still unknown.
    let m = MemoryNameMap::new(TenantId(1)).with_db(1, "d", 1, 1);
    let ctx = RewriteCtx::new(&m, Persona::MySql84);
    let sql = "SELECT a FROM d.t WHERE (`t`.org IN (SELECT `descendant` FROM `__sprig_sys`.`__sprig_subtree_v`))";
    let out = rewrite(sql.as_bytes(), &ctx).unwrap();
    let text = String::from_utf8(apply_edits(sql.as_bytes(), &out.edits)).unwrap();
    assert!(
        text.ends_with("FROM `__sprig_sys`.`__sprig_subtree_v`))") && !text.contains("FROM d.t"),
        "{text}"
    );
    for bad in [
        "SELECT * FROM __sprig_sys.__sprig_names_v",
        "SELECT * FROM __sprig_sys.__sprig_subtree",
        "SELECT * FROM d.__sprig_subtree_v",
    ] {
        assert!(rewrite(bad.as_bytes(), &ctx).is_err(), "{bad}");
    }
}
