//! `SPRIG SHOW ADVICE` rows from a stored design, as chapter 16's
//! advisor writes it (`Design::to_json`).

#![allow(clippy::unwrap_used)]

use serde_json::json;

use super::*;

fn design() -> serde_json::Value {
    json!({
        "groups": [{
            "name": "customers",
            "key": {"parts": [{"class": "numeric"}]},
            "members": [
                {"object": 11, "table": "customers", "routing": {"kind": "columns", "columns": ["id"]}, "pin": null},
                {"object": 12, "table": "orders", "routing": {"kind": "columns", "columns": ["customer_id"]}, "pin": "shard_key"},
                {"object": 13, "table": "order_items", "routing": {"kind": "parent", "parent": 12, "fk": "fk_items_order"}, "pin": null},
                {"object": 15, "table": "events", "routing": {"kind": "columns", "columns": ["customer_id"]}, "pin": "colocate"},
            ],
        }, {
            "name": "tags",
            "key": {"parts": [{"class": "string", "collation": "utf8mb4_bin"}, {"class": "numeric"}]},
            "members": [
                {"object": 17, "table": "tags", "routing": {"kind": "columns", "columns": ["name", "id"]}, "pin": null},
            ],
        }],
        "unsharded": [
            {"object": 16, "table": "countries", "pin": "reference"},
            {"object": 18, "table": "secrets", "pin": null},
        ],
    })
}

fn run() -> AdviceRun {
    AdviceRun {
        run_id: 17,
        cause: "first_split".into(),
        mode: "on".into(),
        status: "applied".into(),
        verdict: Some("apply".into()),
        best: Some(0.902_4),
        runner_up: Some(0.855),
        confidence: Some(1.0),
        units: 48_210,
        decision: Some(8812),
        created_at: "2026-11-09 10:00:00".into(),
        design: Some(design()),
    }
}

/// The shop in database `shop`; `secrets` (18) is hidden from the session.
fn see(o: u64) -> Option<(String, String)> {
    let t = match o {
        11 => "customers",
        12 => "orders",
        13 => "order_items",
        15 => "events",
        16 => "countries",
        17 => "tags",
        _ => return None,
    };
    Some(("shop".into(), t.into()))
}

#[allow(clippy::ref_option)] // cells are `Option<Bytes>`
fn s(v: &Option<Bytes>) -> Option<&str> {
    v.as_deref().map(|b| std::str::from_utf8(b).unwrap())
}

#[test]
fn one_row_per_member_then_unsharded_tables() {
    let rows = advice_rows(&run(), &see);
    let cols = advice_columns(33);
    assert_eq!(cols.len(), 19);
    assert!(rows.iter().all(|r| r.len() == cols.len()));
    // The run's columns on every row.
    for r in &rows {
        assert_eq!(s(&r[0]), Some("17"));
        assert_eq!(s(&r[4]), Some("apply"));
        assert_eq!(s(&r[5]), Some("0.902"));
        assert_eq!(s(&r[6]), Some("0.855"));
        assert_eq!(s(&r[7]), Some("1.00"));
        assert_eq!(s(&r[9]), Some("8812"));
    }
    let tail: Vec<Vec<Option<&str>>> = rows
        .iter()
        .map(|r| r[11..].iter().map(s).collect())
        .collect();
    let n = None;
    assert_eq!(
        tail,
        vec![
            vec![
                Some("customers"),
                Some("numeric"),
                Some("shop"),
                Some("customers"),
                Some("columns"),
                Some("id"),
                n,
                n
            ],
            vec![
                Some("customers"),
                Some("numeric"),
                Some("shop"),
                Some("orders"),
                Some("columns"),
                Some("customer_id"),
                n,
                Some("shard_key")
            ],
            vec![
                Some("customers"),
                Some("numeric"),
                Some("shop"),
                Some("order_items"),
                Some("parent"),
                n,
                Some("orders"),
                n
            ],
            vec![
                Some("customers"),
                Some("numeric"),
                Some("shop"),
                Some("events"),
                Some("columns"),
                Some("customer_id"),
                n,
                Some("colocate")
            ],
            vec![
                Some("tags"),
                Some("string(utf8mb4_bin), numeric"),
                Some("shop"),
                Some("tags"),
                Some("columns"),
                Some("name, id"),
                n,
                n
            ],
            vec![
                n,
                n,
                Some("shop"),
                Some("countries"),
                Some("unsharded"),
                n,
                n,
                Some("reference")
            ],
        ]
    );
}

#[test]
fn hidden_tables_and_physical_names_never_show() {
    let rows = advice_rows(&run(), &see);
    let all: String = rows
        .iter()
        .flatten()
        .filter_map(|v| s(v).map(str::to_owned))
        .collect::<Vec<_>>()
        .join("|");
    assert!(!all.contains("secrets"), "{all}");
    // No schema name of the t<tenant>_d<db>_g<group>_s<shard> form.
    assert!(!all.contains("_sprig") && !all.contains("_s1"), "{all}");
    // A hidden root hides the group's name, not its visible members.
    let only_orders = |o: u64| (o == 12).then(|| ("shop".to_owned(), "orders".to_owned()));
    let rows = advice_rows(&run(), &only_orders);
    assert_eq!(rows.len(), 1);
    assert_eq!(s(&rows[0][11]), None);
    assert_eq!(s(&rows[0][14]), Some("orders"));
}

#[test]
fn a_run_without_a_design_is_one_row() {
    let r = AdviceRun {
        verdict: None,
        best: None,
        runner_up: None,
        confidence: None,
        decision: None,
        design: None,
        status: "failed".into(),
        ..run()
    };
    let rows = advice_rows(&r, &see);
    assert_eq!(rows.len(), 1);
    assert_eq!(s(&rows[0][3]), Some("failed"));
    assert!(rows[0][4..8].iter().all(Option::is_none));
    assert!(rows[0][11..].iter().all(Option::is_none));
}
