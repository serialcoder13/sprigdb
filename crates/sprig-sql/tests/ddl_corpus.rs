//! Table-driven DDL classification per statement form and persona (the
//! gateway side of catalog DDL): every statement runs through
//! the walker, `Engine::Auto` and, where sqlparser-rs models it,
//! `Engine::Ast`; all three must give the same [`DdlTarget`].
//!
//! Targets are rendered compactly: `~billing.t` is an unqualified name
//! resolved to the session's database (`billing`), `crm.t` a qualified
//! one, `~?.t` an unqualified name with no current database. `none` is no
//! target, `nested` DDL the gateway can't route through the catalog.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

mod common;

use std::fmt::Write as _;

use common::{PERSONAS, acme, both, ctx};
use sprig_proto_mysql::packets::Persona;
use sprig_sql::lex::LexOptions;
use sprig_sql::names::{
    AlterOp, DdlName, DdlTarget, Engine, MemoryNameMap, NameError, RewriteCtx, Rewritten,
    StatementInfo, rewrite,
};

#[derive(Clone, Copy)]
enum On {
    Both,
    MySql,
    MariaDb,
}

fn name(n: &DdlName) -> String {
    let db = n.db.as_deref().unwrap_or("?");
    if n.explicit_db {
        format!("{db}.{}", n.name)
    } else {
        format!("~{db}.{}", n.name)
    }
}

fn flag(out: &mut String, on: bool, word: &str) {
    if on {
        out.push(' ');
        out.push_str(word);
    }
}

fn db_opts(
    out: &mut String,
    charset: Option<&String>,
    collation: Option<&String>,
    other: &[String],
) {
    if let Some(c) = charset {
        let _ = write!(out, " charset={c}");
    }
    if let Some(c) = collation {
        let _ = write!(out, " collate={c}");
    }
    if !other.is_empty() {
        let _ = write!(out, " other=[{}]", other.join(","));
    }
}

fn target(t: &DdlTarget) -> String {
    let mut s = String::new();
    match t {
        DdlTarget::CreateDatabase {
            name,
            if_not_exists,
            or_replace,
            charset,
            collation,
            other_options,
        } => {
            let _ = write!(s, "CreateDatabase {name}");
            flag(&mut s, *if_not_exists, "ine");
            flag(&mut s, *or_replace, "replace");
            db_opts(&mut s, charset.as_ref(), collation.as_ref(), other_options);
        }
        DdlTarget::AlterDatabase {
            name,
            explicit,
            charset,
            collation,
            other_options,
        } => {
            let _ = write!(
                s,
                "AlterDatabase {}{}",
                if *explicit { "" } else { "~" },
                name.as_deref().unwrap_or("?")
            );
            db_opts(&mut s, charset.as_ref(), collation.as_ref(), other_options);
        }
        DdlTarget::DropDatabase { name, if_exists } => {
            let _ = write!(s, "DropDatabase {name}");
            flag(&mut s, *if_exists, "ie");
        }
        DdlTarget::CreateObject {
            kind,
            name: n,
            if_not_exists,
            or_replace,
            temporary,
            parent,
            like,
            as_select,
        } => {
            let _ = write!(s, "Create {kind} {}", name(n));
            flag(&mut s, *if_not_exists, "ine");
            flag(&mut s, *or_replace, "replace");
            flag(&mut s, *temporary, "temp");
            if let Some(p) = parent {
                let _ = write!(s, " parent={}", name(p));
            }
            if let Some(l) = like {
                let _ = write!(s, " like={}", name(l));
            }
            flag(&mut s, *as_select, "select");
        }
        DdlTarget::AlterObject {
            kind,
            name: n,
            if_exists,
            op,
        } => {
            let _ = write!(s, "Alter {kind} {}", name(n));
            flag(&mut s, *if_exists, "ie");
            match op {
                AlterOp::Change { rename_to } => {
                    if let Some(r) = rename_to {
                        let _ = write!(s, " rename={}", name(r));
                    }
                }
                AlterOp::CreateIndex {
                    name,
                    if_not_exists,
                    or_replace,
                } => {
                    let _ = write!(s, " +index({})", name.as_deref().unwrap_or(""));
                    flag(&mut s, *if_not_exists, "ine");
                    flag(&mut s, *or_replace, "replace");
                }
                AlterOp::DropIndex { name, if_exists } => {
                    let _ = write!(s, " -index({name})");
                    flag(&mut s, *if_exists, "ie");
                }
            }
        }
        DdlTarget::DropObjects {
            kind,
            names,
            if_exists,
            temporary,
        } => {
            let ns: Vec<String> = names.iter().map(name).collect();
            let _ = write!(s, "Drop {kind} [{}]", ns.join(", "));
            flag(&mut s, *if_exists, "ie");
            flag(&mut s, *temporary, "temp");
        }
        DdlTarget::RenameTables { pairs, if_exists } => {
            let ps: Vec<String> = pairs
                .iter()
                .map(|(a, b)| format!("{}>{}", name(a), name(b)))
                .collect();
            let _ = write!(s, "Rename [{}]", ps.join(", "));
            flag(&mut s, *if_exists, "ie");
        }
        DdlTarget::Truncate { name: n } => {
            let _ = write!(s, "Truncate {}", name(n));
        }
    }
    s
}

fn statement(st: &StatementInfo) -> String {
    match (&st.ddl, st.nested_ddl) {
        (Some(t), _) => target(t),
        (None, true) => "nested".to_owned(),
        (None, false) => "none".to_owned(),
    }
}

fn render(r: &Result<Rewritten, NameError>) -> String {
    match r {
        Ok(r) => r
            .statements
            .iter()
            .map(statement)
            .collect::<Vec<_>>()
            .join(" ; "),
        Err(e) => format!("ERR {}", e.errno()),
    }
}

/// Classifies `sql` with every engine and checks they agree.
fn classify(map: &MemoryNameMap, c: RewriteCtx<'_>, sql: &str) -> String {
    let lexer = rewrite(sql.as_bytes(), &c.engine(Engine::Lexer));
    let auto = rewrite(sql.as_bytes(), &c.engine(Engine::Auto));
    let got = render(&lexer);
    assert_eq!(got, render(&auto), "Auto differs on {sql:?}");
    if let Ok(a) = &auto {
        assert!(
            a.statements.iter().all(|s| s.engines_agree),
            "the AST and the walker disagree on {sql:?}"
        );
    }
    // The AST engine alone, where sqlparser-rs models the statement.
    let ast = rewrite(sql.as_bytes(), &c.engine(Engine::Ast));
    if ast.is_ok() {
        assert_eq!(got, render(&ast), "Ast differs on {sql:?}");
    }
    // The text rewrite agrees too (only with the shared fixture's context).
    if c.current_db == Some("billing") {
        let _ = both(map, c.lex, sql);
    }
    got
}

fn run(cases: &[(On, &str, &str)]) {
    let map = acme();
    let mut failures = Vec::new();
    for &(on, sql, want) in cases {
        for p in PERSONAS {
            let applies = match on {
                On::Both => true,
                On::MySql => p == Persona::MySql84,
                On::MariaDb => p == Persona::MariaDb,
            };
            if !applies {
                continue;
            }
            let c = ctx(&map, LexOptions::for_persona(p), Engine::Lexer);
            let got = classify(&map, c, sql);
            if got != want {
                failures.push(format!("[{p:?}] {sql}\n   want: {want}\n    got: {got}"));
            }
        }
    }
    assert!(failures.is_empty(), "\n{}\n", failures.join("\n"));
}

#[test]
fn databases() {
    use On::Both as X;
    run(&[
        (X, "CREATE DATABASE sales", "CreateDatabase sales"),
        (
            X,
            "CREATE SCHEMA IF NOT EXISTS sales CHARACTER SET = utf8mb4 COLLATE utf8mb4_bin",
            "CreateDatabase sales ine charset=utf8mb4 collate=utf8mb4_bin",
        ),
        (
            X,
            "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `sales` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */",
            "CreateDatabase sales ine charset=utf8mb4 collate=utf8mb4_0900_ai_ci",
        ),
        (
            X,
            "CREATE OR REPLACE DATABASE sales CHARSET latin1 COMMENT 'x'",
            "CreateDatabase sales replace charset=latin1 other=[COMMENT]",
        ),
        (
            X,
            "CREATE DATABASE sales DEFAULT CHARSET='UTF8MB4' DEFAULT ENCRYPTION = 'Y'",
            "CreateDatabase sales charset=utf8mb4 other=[ENCRYPTION]",
        ),
        (
            X,
            "ALTER DATABASE crm CHARACTER SET latin1",
            "AlterDatabase crm charset=latin1",
        ),
        (
            X,
            "ALTER SCHEMA DEFAULT COLLATE = utf8mb4_bin",
            "AlterDatabase ~billing collate=utf8mb4_bin",
        ),
        (
            X,
            "ALTER DATABASE `crm` CHAR SET utf8mb4 READ ONLY = 1",
            "AlterDatabase crm charset=utf8mb4 other=[READ]",
        ),
        // A setting write, not DDL: no target. Its dotted key is a position
        // the walker doesn't read yet.
        (
            X,
            "ALTER DATABASE crm SPRIG SET (doc.wildcard_index = 'on')",
            "ERR 39051",
        ),
        (X, "ALTER DATABASE crm SPRIG SET (x = 'on')", "none"),
        (X, "DROP DATABASE crm", "DropDatabase crm"),
        (X, "DROP SCHEMA IF EXISTS nope", "DropDatabase nope ie"),
        (
            X,
            "DROP DATABASE IF EXISTS `my.db`",
            "DropDatabase my.db ie",
        ),
        (X, "DROP DATABASE nope", "ERR 1049"),
        (X, "CREATE DATABASE t42_sprig", "ERR 39050"),
    ]);
}

#[test]
fn tables() {
    use On::Both as X;
    run(&[
        (X, "CREATE TABLE t (id INT)", "Create TABLE ~billing.t"),
        (
            X,
            "CREATE TABLE IF NOT EXISTS crm.`Order s` (id INT)",
            "Create TABLE crm.Order s ine",
        ),
        (
            X,
            "CREATE TEMPORARY TABLE tmp (id INT)",
            "Create TABLE ~billing.tmp temp",
        ),
        (
            X,
            "CREATE TEMPORARY TABLE IF NOT EXISTS crm.tmp SELECT * FROM crm.t",
            "Create TABLE crm.tmp ine temp select",
        ),
        (
            X,
            "CREATE TABLE crm.t2 LIKE t",
            "Create TABLE crm.t2 like=~billing.t",
        ),
        (
            X,
            "CREATE TABLE t2 (LIKE crm.t)",
            "Create TABLE ~billing.t2 like=crm.t",
        ),
        (
            X,
            "CREATE TABLE t2 AS SELECT * FROM crm.t",
            "Create TABLE ~billing.t2 select",
        ),
        (
            X,
            "CREATE TABLE t2 (a INT) IGNORE SELECT a FROM t",
            "Create TABLE ~billing.t2 select",
        ),
        (
            X,
            "CREATE TABLE t2 (a INT) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
            "Create TABLE ~billing.t2",
        ),
        (
            X,
            "CREATE OR REPLACE TABLE crm.t (id INT)",
            "Create TABLE crm.t replace",
        ),
        (
            X,
            "CREATE TABLE /*!32312 IF NOT EXISTS*/ `t` (id INT) ENGINE=InnoDB",
            "Create TABLE ~billing.t ine",
        ),
        (
            X,
            "CREATE TABLE `a``b`.`x``y` (id INT)",
            "Create TABLE a`b.x`y",
        ),
        (
            X,
            "CREATE TABLE Ünïcødé.t (a INT REFERENCES crm.p (id))",
            "Create TABLE Ünïcødé.t",
        ),
        (X, "CREATE TABLE nope.t (a INT)", "ERR 1049"),
        (X, "CREATE TABLE __sprig_x (a INT)", "ERR 39050"),
        (
            X,
            "ALTER TABLE t ADD COLUMN c INT",
            "Alter TABLE ~billing.t",
        ),
        (
            X,
            "ALTER TABLE IF EXISTS crm.t ADD c INT",
            "Alter TABLE crm.t ie",
        ),
        (
            X,
            "ALTER TABLE t RENAME TO crm.u",
            "Rename [~billing.t>crm.u]",
        ),
        (
            X,
            "ALTER TABLE crm.t RENAME AS u",
            "Rename [crm.t>~billing.u]",
        ),
        (
            X,
            "ALTER TABLE t ADD c INT, RENAME TO u",
            "Alter TABLE ~billing.t rename=~billing.u",
        ),
        (
            X,
            "ALTER TABLE t RENAME COLUMN a TO b",
            "Alter TABLE ~billing.t",
        ),
        (
            X,
            "ALTER TABLE t RENAME INDEX a TO b",
            "Alter TABLE ~billing.t",
        ),
        (X, "ALTER ONLINE TABLE crm.t FORCE", "Alter TABLE crm.t"),
        (
            X,
            "ALTER TABLE t SPRIG SET (auto_index.mode = 'recommend')",
            "none",
        ),
        (X, "DROP TABLE t", "Drop TABLE [~billing.t]"),
        (
            X,
            "DROP TABLE IF EXISTS a, crm.b, `my.db`.c",
            "Drop TABLE [~billing.a, crm.b, my.db.c] ie",
        ),
        (
            X,
            "DROP TEMPORARY TABLE IF EXISTS tmp",
            "Drop TABLE [~billing.tmp] ie temp",
        ),
        (
            X,
            "DROP TABLES a, b RESTRICT",
            "Drop TABLE [~billing.a, ~billing.b]",
        ),
        (X, "DROP TABLE Mixed.T", "Drop TABLE [Mixed.T]"),
        (X, "RENAME TABLE a TO b", "Rename [~billing.a>~billing.b]"),
        (
            X,
            "RENAME TABLE a TO crm.a, crm.b TO billing.b",
            "Rename [~billing.a>crm.a, crm.b>billing.b]",
        ),
        (
            X,
            "RENAME TABLES IF EXISTS crm.a NOWAIT TO crm.b",
            "Rename [crm.a>crm.b] ie",
        ),
        (X, "TRUNCATE t", "Truncate ~billing.t"),
        (X, "TRUNCATE TABLE crm.t", "Truncate crm.t"),
        (X, "TRUNCATE TABLE t WAIT 5", "Truncate ~billing.t"),
    ]);
}

#[test]
fn indexes() {
    use On::Both as X;
    run(&[
        (
            X,
            "CREATE UNIQUE INDEX i ON t (a)",
            "Alter TABLE ~billing.t +index(i)",
        ),
        (
            X,
            "CREATE INDEX i ON crm.t (a, b)",
            "Alter TABLE crm.t +index(i)",
        ),
        (
            X,
            "CREATE OR REPLACE INDEX IF NOT EXISTS i USING BTREE ON crm.t (a)",
            "Alter TABLE crm.t +index(i) ine replace",
        ),
        (X, "DROP INDEX i ON crm.t", "Alter TABLE crm.t -index(i)"),
        (
            X,
            "DROP INDEX IF EXISTS `PRIMARY` ON t",
            "Alter TABLE ~billing.t -index(PRIMARY) ie",
        ),
        (
            X,
            "DROP INDEX i ON t ALGORITHM = INPLACE LOCK = NONE",
            "Alter TABLE ~billing.t -index(i)",
        ),
    ]);
}

#[test]
fn views_routines_triggers_events_sequences() {
    use On::Both as X;
    run(&[
        (X, "CREATE VIEW v AS SELECT 1", "Create VIEW ~billing.v"),
        (
            X,
            "CREATE OR REPLACE ALGORITHM=MERGE DEFINER=`root`@`%` SQL SECURITY INVOKER VIEW crm.v AS SELECT * FROM t",
            "Create VIEW crm.v replace",
        ),
        (
            X,
            "/*!50001 CREATE ALGORITHM=UNDEFINED */ /*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */ /*!50001 VIEW `v` AS select 1 AS `1` */",
            "Create VIEW ~billing.v",
        ),
        (
            X,
            "CREATE VIEW IF NOT EXISTS crm.v (a) AS SELECT a FROM crm.t",
            "Create VIEW crm.v ine",
        ),
        (X, "ALTER VIEW v AS SELECT 2", "Alter VIEW ~billing.v"),
        (
            X,
            "ALTER DEFINER=CURRENT_USER() VIEW crm.v AS SELECT 1",
            "Alter VIEW crm.v",
        ),
        (
            X,
            "DROP VIEW IF EXISTS v1, crm.v2 CASCADE",
            "Drop VIEW [~billing.v1, crm.v2] ie",
        ),
        (
            X,
            "CREATE DEFINER=CURRENT_USER PROCEDURE crm.p() BEGIN SELECT 1; END",
            "Create PROCEDURE crm.p",
        ),
        (
            X,
            "CREATE PROCEDURE p() BEGIN DROP TABLE t; CREATE TABLE u (a INT); END",
            "Create PROCEDURE ~billing.p",
        ),
        (
            X,
            "CREATE FUNCTION IF NOT EXISTS f(x INT) RETURNS INT DETERMINISTIC RETURN x + 1",
            "Create FUNCTION ~billing.f ine",
        ),
        (
            X,
            "CREATE OR REPLACE DEFINER='admin'@'%' FUNCTION crm.f() RETURNS INT RETURN 1",
            "Create FUNCTION crm.f replace",
        ),
        (X, "CREATE FUNCTION f RETURNS STRING SONAME 'x.so'", "none"),
        (
            X,
            "ALTER PROCEDURE p COMMENT 'x'",
            "Alter PROCEDURE ~billing.p",
        ),
        (
            X,
            "ALTER FUNCTION crm.f SQL SECURITY INVOKER",
            "Alter FUNCTION crm.f",
        ),
        (
            X,
            "DROP PROCEDURE IF EXISTS crm.p",
            "Drop PROCEDURE [crm.p] ie",
        ),
        (X, "DROP FUNCTION f", "Drop FUNCTION [~billing.f]"),
        (
            X,
            "CREATE DEFINER='admin'@'%' TRIGGER crm.trg BEFORE INSERT ON crm.orders FOR EACH ROW SET NEW.a = 1",
            "Create TRIGGER crm.trg parent=crm.orders",
        ),
        (
            X,
            "CREATE TRIGGER IF NOT EXISTS trg AFTER UPDATE ON orders FOR EACH ROW FOLLOWS other INSERT INTO log VALUES (1)",
            "Create TRIGGER ~billing.trg ine parent=~billing.orders",
        ),
        (
            X,
            "CREATE OR REPLACE TRIGGER trg BEFORE DELETE ON t FOR EACH ROW BEGIN DELETE FROM crm.u; END",
            "Create TRIGGER ~billing.trg replace parent=~billing.t",
        ),
        (
            X,
            "DROP TRIGGER IF EXISTS crm.trg",
            "Drop TRIGGER [crm.trg] ie",
        ),
        (
            X,
            "CREATE EVENT IF NOT EXISTS e ON SCHEDULE EVERY 1 DAY DO DELETE FROM t",
            "Create EVENT ~billing.e ine",
        ),
        (
            X,
            "ALTER EVENT e ON SCHEDULE EVERY 2 DAY RENAME TO crm.e2",
            "Alter EVENT ~billing.e rename=crm.e2",
        ),
        (X, "ALTER EVENT crm.e DISABLE", "Alter EVENT crm.e"),
        (X, "DROP EVENT e", "Drop EVENT [~billing.e]"),
        (
            X,
            "CREATE SEQUENCE IF NOT EXISTS crm.s START WITH 1 INCREMENT BY 1",
            "Create SEQUENCE crm.s ine",
        ),
        (
            X,
            "CREATE TEMPORARY SEQUENCE s",
            "Create SEQUENCE ~billing.s temp",
        ),
        (
            X,
            "ALTER SEQUENCE IF EXISTS s RESTART WITH 5",
            "Alter SEQUENCE ~billing.s ie",
        ),
        (
            X,
            "DROP SEQUENCE IF EXISTS s1, crm.s2",
            "Drop SEQUENCE [~billing.s1, crm.s2] ie",
        ),
    ]);
}

#[test]
fn not_catalog_ddl() {
    use On::Both as X;
    run(&[
        (X, "SELECT 1", "none"),
        (X, "INSERT INTO crm.t VALUES (1)", "none"),
        (X, "CREATE USER bob IDENTIFIED BY 'x'", "none"),
        (X, "DROP USER bob", "none"),
        (X, "RENAME USER a TO b", "none"),
        (X, "SHOW CREATE TABLE crm.t", "none"),
        (X, "BEGIN NOT ATOMIC CREATE TABLE t (a INT); END", "nested"),
        (
            X,
            "BEGIN NOT ATOMIC IF 1 THEN DROP TABLE crm.t; END IF; END",
            "nested",
        ),
        (X, "PREPARE s FROM 'DROP TABLE t'", "nested"),
        (
            X,
            "EXECUTE IMMEDIATE 'CREATE TABLE crm.t (a INT)'",
            "nested",
        ),
        (X, "PREPARE s FROM 'SELECT 1'", "none"),
    ]);
}

#[test]
fn multi_statement_texts_follow_use() {
    use On::Both as X;
    run(&[
        (
            X,
            "USE crm; CREATE TABLE t (a INT); DROP TABLE billing.u, v",
            "none ; Create TABLE ~crm.t ; Drop TABLE [billing.u, ~crm.v]",
        ),
        (
            X,
            "CREATE TABLE a (x INT); RENAME TABLE a TO b; TRUNCATE b",
            "Create TABLE ~billing.a ; Rename [~billing.a>~billing.b] ; Truncate ~billing.b",
        ),
    ]);
}

#[test]
fn personas_and_executable_comments() {
    run(&[
        (
            On::MariaDb,
            "CREATE /*M!100100 OR REPLACE */ TABLE t (a INT)",
            "Create TABLE ~billing.t replace",
        ),
        (
            On::MySql,
            "CREATE /*M!100100 OR REPLACE */ TABLE t (a INT)",
            "Create TABLE ~billing.t",
        ),
        (
            On::Both,
            "DROP TABLE /*!40000 IF EXISTS */ crm.t /* trailing */",
            "Drop TABLE [crm.t] ie",
        ),
        (
            On::Both,
            "CREATE /*!50003 DEFINER=`root`@`localhost`*/ /*!50003 TRIGGER `trg` BEFORE INSERT ON `crm`.`t` FOR EACH ROW SET NEW.a = 1 */",
            "Create TRIGGER ~billing.trg parent=crm.t",
        ),
    ]);
}

#[test]
fn ansi_quotes_and_no_current_database() {
    let map = acme();
    for p in PERSONAS {
        let lex = LexOptions::for_persona(p).ansi_quotes(true);
        let c = ctx(&map, lex, Engine::Lexer);
        assert_eq!(
            classify(&map, c, "CREATE TABLE \"crm\".\"q t\" (a INT)"),
            "Create TABLE crm.q t"
        );
        assert_eq!(
            classify(&map, c, "DROP TABLE IF EXISTS \"a\", crm.\"b\""),
            "Drop TABLE [~billing.a, crm.b] ie"
        );
        let c = ctx(&map, LexOptions::for_persona(p), Engine::Lexer).current_db(None);
        assert_eq!(
            classify(&map, c, "CREATE TABLE t (a INT)"),
            "Create TABLE ~?.t"
        );
        assert_eq!(
            classify(&map, c, "ALTER DATABASE CHARACTER SET latin1"),
            "AlterDatabase ~? charset=latin1"
        );
        let r = rewrite(b"ALTER DATABASE CHARACTER SET latin1", &c).unwrap();
        let e = r.statements[0]
            .ddl
            .as_ref()
            .unwrap()
            .to_catalog()
            .unwrap_err();
        assert_eq!(e.errno(), 1046);
    }
}

#[test]
fn conversion_to_the_catalog() {
    use sprig_catalog::ddl::{DdlStatement, ObjectName};
    use sprig_catalog::store::ObjectKind;

    let map = acme();
    let c = ctx(&map, LexOptions::mariadb(), Engine::Lexer);
    let convert = |sql: &str| {
        let r = rewrite(sql.as_bytes(), &c).unwrap();
        r.statements[0].ddl.as_ref().unwrap().to_catalog()
    };
    assert_eq!(
        convert("CREATE TRIGGER trg BEFORE INSERT ON crm.t FOR EACH ROW SET NEW.a = 1").unwrap(),
        Some(DdlStatement::CreateObject {
            kind: ObjectKind::Trigger,
            name: ObjectName::qualified("billing", "trg"),
            if_not_exists: false,
            or_replace: false,
            parent: Some(ObjectName::qualified("crm", "t")),
        })
    );
    assert_eq!(
        convert("DROP INDEX i ON t").unwrap(),
        Some(DdlStatement::AlterObject {
            kind: ObjectKind::Table,
            name: ObjectName::qualified("billing", "t"),
            if_exists: false,
            rename_to: None,
        })
    );
    assert_eq!(
        convert("ALTER TABLE crm.a RENAME TO crm.b").unwrap(),
        Some(DdlStatement::RenameTables {
            pairs: vec![(
                ObjectName::qualified("crm", "a"),
                ObjectName::qualified("crm", "b")
            )],
            if_exists: false,
        })
    );
    assert_eq!(convert("CREATE TEMPORARY TABLE x (a INT)").unwrap(), None);
    assert_eq!(
        convert("ALTER TABLE t ADD c INT, RENAME TO u").unwrap(),
        Some(DdlStatement::AlterObject {
            kind: ObjectKind::Table,
            name: ObjectName::qualified("billing", "t"),
            if_exists: false,
            rename_to: Some(ObjectName::qualified("billing", "u")),
        })
    );
    assert_eq!(
        convert("ALTER TABLE IF EXISTS t ADD c INT").unwrap(),
        Some(DdlStatement::AlterObject {
            kind: ObjectKind::Table,
            name: ObjectName::qualified("billing", "t"),
            if_exists: true,
            rename_to: None,
        })
    );
    assert_eq!(
        convert("CREATE DATABASE d COMMENT 'x'")
            .unwrap_err()
            .errno(),
        39051
    );
}
