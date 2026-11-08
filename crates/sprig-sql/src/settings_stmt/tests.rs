#![allow(clippy::unwrap_used)]

use super::*;

fn p(sql: &str) -> SettingsStmt {
    parse(sql.as_bytes(), &LexOptions::mysql84())
        .unwrap_or_else(|| panic!("not a settings statement: {sql}"))
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

fn err(sql: &str) -> u16 {
    parse(sql.as_bytes(), &LexOptions::mariadb())
        .unwrap()
        .unwrap_err()
        .errno()
}

fn bare(s: &str) -> Literal {
    Literal::Bare(s.to_owned())
}

fn text(s: &str) -> Literal {
    Literal::Text(s.to_owned())
}

#[test]
fn chapter_08_examples() {
    assert_eq!(
        p(
            "ALTER TABLE orders SPRIG SET (auto_index.mode = 'recommend', auto_index.max_per_table = 8)"
        ),
        SettingsStmt::Alter {
            target: SettingsTarget::Table {
                db: None,
                name: "orders".into()
            },
            op: SettingsOp::Set(vec![
                ("auto_index.mode".into(), text("recommend")),
                ("auto_index.max_per_table".into(), bare("8")),
            ]),
        }
    );
    assert_eq!(
        p("ALTER TABLE orders SPRIG RESET (auto_index.mode);"),
        SettingsStmt::Alter {
            target: SettingsTarget::Table {
                db: None,
                name: "orders".into()
            },
            op: SettingsOp::Reset(vec!["auto_index.mode".into()]),
        }
    );
    assert_eq!(
        p("alter database shop sprig set (doc.wildcard_index = 'on')"),
        SettingsStmt::Alter {
            target: SettingsTarget::Database(Some("shop".into())),
            op: SettingsOp::Set(vec![("doc.wildcard_index".into(), text("on"))]),
        }
    );
    let SettingsStmt::Alter {
        target: SettingsTarget::Tenant(None),
        op: SettingsOp::SetBounds(b),
    } = p(
        "ALTER TENANT SPRIG SET BOUNDS (auto_index.max_per_table MAX 6,
                               auto_index.mode ALLOWED ('on', 'recommend'))",
    )
    else {
        panic!("bounds");
    };
    assert_eq!(b[0].0, "auto_index.max_per_table");
    assert_eq!(b[0].1.max, Some(6.0));
    assert_eq!(b[1].1.allowed, Some(vec![text("on"), text("recommend")]));
    let SettingsStmt::Alter {
        target: SettingsTarget::Database(Some(db)),
        op: SettingsOp::SetBounds(b),
    } = p("ALTER DATABASE shop SPRIG SET BOUNDS (doc.wildcard_index LOCKED)")
    else {
        panic!("locked");
    };
    assert_eq!(db, "shop");
    assert_eq!(b[0].1.locked, Some(true));
    assert_eq!(
        p("ALTER TENANT SPRIG RESET BOUNDS (auto_index.max_per_table)"),
        SettingsStmt::Alter {
            target: SettingsTarget::Tenant(None),
            op: SettingsOp::ResetBounds(vec!["auto_index.max_per_table".into()]),
        }
    );
    assert_eq!(
        p("ALTER TENANT pune SPRIG SET (auth.password_lock_minutes = 30)"),
        SettingsStmt::Alter {
            target: SettingsTarget::Tenant(Some("pune".into())),
            op: SettingsOp::Set(vec![("auth.password_lock_minutes".into(), bare("30"))]),
        }
    );
    assert_eq!(
        p("SHOW SPRIG SETTINGS LIKE 'auto_index.%' FOR TABLE orders"),
        SettingsStmt::Show {
            like: Some("auto_index.%".into()),
            target: SettingsTarget::Table {
                db: None,
                name: "orders".into()
            },
        }
    );
}

#[test]
fn short_forms_and_targets() {
    assert_eq!(
        p("SPRIG SET reads.max_replica_lag_ms = 500"),
        SettingsStmt::Alter {
            target: SettingsTarget::Tenant(None),
            op: SettingsOp::Set(vec![("reads.max_replica_lag_ms".into(), bare("500"))]),
        }
    );
    assert_eq!(
        p("SPRIG RESET reads.max_replica_lag_ms, sql.local_infile"),
        SettingsStmt::Alter {
            target: SettingsTarget::Tenant(None),
            op: SettingsOp::Reset(vec![
                "reads.max_replica_lag_ms".into(),
                "sql.local_infile".into()
            ]),
        }
    );
    let SettingsStmt::Alter {
        op: SettingsOp::SetBounds(b),
        ..
    } = p("SPRIG SET BOUNDS reads.max_replica_lag_ms MIN -1 MAX 2.5 UNLOCKED")
    else {
        panic!("short bounds");
    };
    assert_eq!(
        (b[0].1.min, b[0].1.max, b[0].1.locked),
        (Some(-1.0), Some(2.5), Some(false))
    );
    assert_eq!(
        p("ALTER TABLE `billing`.`orders` SPRIG SET (sql.local_infile = OFF)"),
        SettingsStmt::Alter {
            target: SettingsTarget::Table {
                db: Some("billing".into()),
                name: "orders".into()
            },
            op: SettingsOp::Set(vec![("sql.local_infile".into(), bare("OFF"))]),
        }
    );
    assert_eq!(
        p("ALTER DATABASE SPRIG SET (sql.local_infile = 0)"),
        SettingsStmt::Alter {
            target: SettingsTarget::Database(None),
            op: SettingsOp::Set(vec![("sql.local_infile".into(), bare("0"))]),
        }
    );
    assert_eq!(
        p("SHOW SPRIG SETTINGS"),
        SettingsStmt::Show {
            like: None,
            target: SettingsTarget::Tenant(None)
        }
    );
    assert_eq!(
        p("SHOW SPRIG SETTINGS FOR DATABASE"),
        SettingsStmt::Show {
            like: None,
            target: SettingsTarget::Database(None)
        }
    );
    assert_eq!(
        p("SHOW SPRIG SETTINGS FOR TENANT acme"),
        SettingsStmt::Show {
            like: None,
            target: SettingsTarget::Tenant(Some("acme".into()))
        }
    );
}

#[test]
fn other_statements_are_not_settings() {
    for sql in [
        "ALTER TABLE orders ADD c INT",
        "SET autocommit = 0",
        "SHOW TABLES",
        "ALTER DATABASE shop CHARACTER SET utf8mb4",
        "SELECT 'SPRIG SET'",
        "ALTER TABLE t COMMENT 'SPRIG'",
    ] {
        assert!(
            parse(sql.as_bytes(), &LexOptions::mysql84()).is_none(),
            "{sql}"
        );
    }
}

#[test]
fn errors() {
    assert_eq!(err("SPRIG SET"), 1064);
    assert_eq!(err("SPRIG SET k ="), 1064);
    assert_eq!(
        err("ALTER TABLE t SPRIG SET k = 1"),
        1064,
        "ALTER needs a list"
    );
    assert_eq!(err("ALTER TABLE t SPRIG SET BOUNDS (k)"), 1064);
    assert_eq!(err("SPRIG SET k = 1 junk"), 1064);
    assert_eq!(err("ALTER FUNCTION f SPRIG SET (k = 1)"), 39051);
}

#[test]
fn like_patterns() {
    assert!(like_matches("auto\\_index.%", "auto_index.mode"));
    assert!(!like_matches("auto\\_index.%", "autoXindex.mode"));
    assert!(like_matches("READS.%", "reads.routing"));
    assert!(like_matches("sql._ocal_infile", "sql.local_infile"));
    assert!(!like_matches("sql", "sql.local_infile"));
}
