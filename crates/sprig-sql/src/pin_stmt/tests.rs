#![allow(clippy::unwrap_used)]

use super::*;

fn p(sql: &str) -> PinStmt {
    parse(sql.as_bytes(), &LexOptions::mysql84())
        .unwrap_or_else(|| panic!("not a pin statement: {sql}"))
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

fn err(sql: &str) -> u16 {
    parse(sql.as_bytes(), &LexOptions::mariadb())
        .unwrap()
        .unwrap_err()
        .errno()
}

fn t(name: &str) -> PinTable {
    PinTable {
        db: None,
        name: name.into(),
    }
}

#[test]
fn chapter_16_examples() {
    assert_eq!(
        p("SPRIG PIN orders SHARD KEY (customer_id);"),
        PinStmt::Pin {
            table: t("orders"),
            spec: PinSpec::ShardKey(vec!["customer_id".into()]),
        }
    );
    assert_eq!(
        p("SPRIG PIN events COLOCATE WITH customers ON (customer_id);"),
        PinStmt::Pin {
            table: t("events"),
            spec: PinSpec::Colocate {
                parent: t("customers"),
                columns: vec!["customer_id".into()],
            },
        }
    );
    assert_eq!(
        p("SPRIG PIN countries REFERENCE;"),
        PinStmt::Pin {
            table: t("countries"),
            spec: PinSpec::Reference,
        }
    );
    assert_eq!(
        p("SPRIG PIN audit_log UNSHARDED;"),
        PinStmt::Pin {
            table: t("audit_log"),
            spec: PinSpec::Unsharded,
        }
    );
    assert_eq!(
        p("SPRIG UNPIN orders;"),
        PinStmt::Unpin { table: t("orders") }
    );
    assert_eq!(p("SPRIG SHOW ADVICE;"), PinStmt::ShowAdvice);
    assert_eq!(p("SPRIG SHOW PINS"), PinStmt::ShowPins);
}

#[test]
fn qualified_quoted_and_any_case() {
    assert_eq!(
        p("sprig pin `shop`.`Order Lines` shard key (`order_id`, line_no)"),
        PinStmt::Pin {
            table: PinTable {
                db: Some("shop".into()),
                name: "Order Lines".into(),
            },
            spec: PinSpec::ShardKey(vec!["order_id".into(), "line_no".into()]),
        }
    );
    assert_eq!(
        p("Sprig Pin shop.events /* c */ Colocate With crm.customers On (customer_id)"),
        PinStmt::Pin {
            table: PinTable {
                db: Some("shop".into()),
                name: "events".into(),
            },
            spec: PinSpec::Colocate {
                parent: PinTable {
                    db: Some("crm".into()),
                    name: "customers".into(),
                },
                columns: vec!["customer_id".into()],
            },
        }
    );
    assert_eq!(
        p("SPRIG UNPIN shop.orders"),
        PinStmt::Unpin {
            table: PinTable {
                db: Some("shop".into()),
                name: "orders".into(),
            }
        }
    );
}

#[test]
fn other_statements_are_not_pins() {
    for sql in [
        "SPRIG SET (advisor.mode = 'off')",
        "SPRIG RESET advisor.mode",
        "SHOW SPRIG SETTINGS",
        "SELECT 1",
        "PIN orders",
        "",
    ] {
        assert!(
            parse(sql.as_bytes(), &LexOptions::mariadb()).is_none(),
            "{sql}"
        );
    }
}

#[test]
fn syntax_errors_are_1064() {
    for sql in [
        "SPRIG PIN orders",
        "SPRIG PIN orders SHARD KEY customer_id",
        "SPRIG PIN orders SHARD KEY ()",
        "SPRIG PIN orders SHARD KEY (a,)",
        "SPRIG PIN orders SHARDED",
        "SPRIG PIN events COLOCATE WITH customers",
        "SPRIG PIN events COLOCATE customers ON (customer_id)",
        "SPRIG PIN events COLOCATE WITH customers ON customer_id",
        "SPRIG PIN countries REFERENCE extra",
        "SPRIG PIN 'orders' UNSHARDED",
        "SPRIG UNPIN",
        "SPRIG UNPIN orders; SELECT 1",
        "SPRIG SHOW",
        "SPRIG SHOW ADVICES",
        "SPRIG SHOW ADVICE FOR orders",
    ] {
        assert_eq!(err(sql), 1064, "{sql}");
    }
}

#[test]
fn settings_statements_leave_pins_alone() {
    // `SPRIG ...` used to be the settings parser's alone: pins and `SPRIG
    // SHOW` are not settings statements any more.
    for sql in [
        "SPRIG PIN orders UNSHARDED",
        "SPRIG UNPIN orders",
        "SPRIG SHOW ADVICE",
    ] {
        assert!(
            crate::settings_stmt::parse(sql.as_bytes(), &LexOptions::mariadb()).is_none(),
            "{sql}"
        );
    }
}
