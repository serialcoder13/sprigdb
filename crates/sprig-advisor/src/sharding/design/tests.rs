#![allow(clippy::unwrap_used, clippy::float_cmp)]

use super::*;
use sprig_shard::pins::{Pin, PinKind};

const CUSTOMERS: Obj = 1;
const ORDERS: Obj = 2;
const ITEMS: Obj = 3;
const PRODUCTS: Obj = 4;
const EVENTS: Obj = 5;
const COUNTRIES: Obj = 6;

fn col(n: &str, class: Option<KeyClass>, ndv: f64) -> Column {
    Column {
        name: n.into(),
        class,
        ndv: Some(ndv),
        top_share: Some(0.01),
    }
}

fn table(o: Obj, name: &str, cols: Vec<Column>, gb: f64) -> Table {
    Table {
        object: o,
        db: 1,
        name: name.into(),
        pk: vec![cols[0].name.clone()],
        columns: cols,
        uniques: Vec::new(),
        size_bytes: gb * 1e9,
    }
}

fn fk(name: &str, child: Obj, c: &str, parent: Obj, p: &str) -> ForeignKey {
    ForeignKey {
        name: name.into(),
        child,
        child_cols: vec![c.into()],
        parent,
        parent_cols: vec![p.into()],
    }
}

fn touch(o: Obj, eq: &[&str]) -> Touch {
    Touch {
        object: o,
        eq: eq.iter().map(|s| (*s).to_owned()).collect(),
        ..Touch::default()
    }
}

fn insert(o: Obj, cols: &[&str]) -> Touch {
    Touch {
        object: o,
        insert: cols.iter().map(|s| (*s).to_owned()).collect(),
        write: true,
        ..Touch::default()
    }
}

fn join(a: Obj, x: &str, b: Obj, y: &str) -> JoinEdge {
    JoinEdge {
        a: (a, x.into()),
        b: (b, y.into()),
    }
}

fn stmt(fp: u64, weight: f64, touches: Vec<Touch>, joins: Vec<JoinEdge>) -> Stmt {
    Stmt {
        weight,
        fingerprint: fp,
        touches,
        joins,
    }
}

/// Chapter 16's shop, in SQL only.
fn shop() -> Input {
    let n = Some(KeyClass::Numeric);
    Input {
        tables: vec![
            table(
                CUSTOMERS,
                "customers",
                vec![col("id", n, 1e6), col("email", None, 1e6)],
                2.0,
            ),
            table(
                ORDERS,
                "orders",
                vec![
                    col("id", n, 1e7),
                    col("customer_id", n, 1e6),
                    col("created_at", None, 1e7),
                ],
                30.0,
            ),
            table(
                ITEMS,
                "order_items",
                vec![
                    col("id", n, 3e7),
                    col("order_id", n, 1e7),
                    col("product_id", n, 1e5),
                ],
                60.0,
            ),
            table(
                PRODUCTS,
                "products",
                vec![col("id", n, 1e5), col("stock", n, 100.0)],
                1.0,
            ),
            table(
                EVENTS,
                "events",
                vec![col("id", n, 1e9), col("customer_id", n, 1e6)],
                180.0,
            ),
            table(
                COUNTRIES,
                "countries",
                vec![col("code", None, 200.0)],
                0.001,
            ),
        ],
        fks: vec![
            fk("fk_orders_customer", ORDERS, "customer_id", CUSTOMERS, "id"),
            fk("fk_items_order", ITEMS, "order_id", ORDERS, "id"),
            fk("fk_items_product", ITEMS, "product_id", PRODUCTS, "id"),
        ],
        stmts: vec![
            stmt(1, 2.0e6, vec![touch(CUSTOMERS, &["id"])], vec![]),
            stmt(2, 1.5e6, vec![touch(ORDERS, &["customer_id"])], vec![]),
            stmt(
                3,
                1.2e6,
                vec![touch(ORDERS, &["id"]), touch(ITEMS, &[])],
                vec![join(ITEMS, "order_id", ORDERS, "id")],
            ),
            stmt(
                4,
                0.3e6,
                vec![
                    insert(ORDERS, &["id", "customer_id"]),
                    insert(ITEMS, &["id", "order_id", "product_id"]),
                ],
                vec![],
            ),
            stmt(5, 3.0e6, vec![touch(PRODUCTS, &["id"])], vec![]),
            stmt(
                7,
                5.0e6,
                vec![insert(EVENTS, &["id", "customer_id"])],
                vec![],
            ),
            stmt(8, 0.9e6, vec![touch(EVENTS, &["customer_id"])], vec![]),
            stmt(
                9,
                0.2e6,
                vec![touch(CUSTOMERS, &["id"]), touch(EVENTS, &[])],
                vec![join(CUSTOMERS, "id", EVENTS, "customer_id")],
            ),
        ],
        units: vec![Unit {
            weight: 0.3e6,
            writes: vec![ORDERS, ITEMS],
        }],
        split_bytes: 32.0 * 1_073_741_824.0,
        pins: Vec::new(),
    }
}

fn members(d: &Design, root: Obj) -> Vec<(Obj, Routing)> {
    d.groups
        .iter()
        .find(|g| g.members[0].object == root)
        .unwrap()
        .members
        .iter()
        .map(|m| (m.object, m.routing.clone()))
        .collect()
}

#[test]
fn the_structural_seed_follows_foreign_key_trees() {
    let s = AdvisorSettings::default();
    let (d, ev) = structural(&shop(), &s);
    assert_eq!(
        members(&d, CUSTOMERS),
        vec![
            (
                CUSTOMERS,
                Routing::Columns {
                    columns: vec!["id".into()]
                }
            ),
            (
                ORDERS,
                Routing::Columns {
                    columns: vec!["customer_id".into()]
                }
            ),
            (
                ITEMS,
                Routing::Parent {
                    parent: ORDERS,
                    fk: "fk_items_order".into()
                }
            ),
        ]
    );
    assert_eq!(members(&d, PRODUCTS).len(), 1);
    assert_eq!(members(&d, EVENTS).len(), 1);
    // countries' key is a string without a supported collation here.
    assert_eq!(d.unsharded, vec![COUNTRIES]);
    assert!(ev.iter().any(|e| e.contains("fk_items_product")), "{ev:?}");
}

#[test]
fn the_search_attaches_events_to_customers() {
    let s = AdvisorSettings::default();
    let r = search(&shop(), &s);
    assert_eq!(
        r.path,
        vec![Move::Attach {
            object: EVENTS,
            group_root: CUSTOMERS,
            column: "customer_id".into()
        }]
    );
    assert!(members(&r.best, CUSTOMERS).contains(&(
        EVENTS,
        Routing::Columns {
            columns: vec!["customer_id".into()]
        }
    )));
    let (seed, _) = structural(&shop(), &s);
    let seed_score = score(&shop(), &seed, &s);
    assert!(
        r.score.total > seed_score.total + 0.01,
        "{:?} vs {:?}",
        r.score,
        seed_score
    );
    let c = confidence(&shop(), &r.best, r.alternatives.first().map(|(d, _)| d), &s);
    assert!(c >= 0.99, "confidence {c}");
}

#[test]
fn shuffled_input_gives_the_same_design() {
    let s = AdvisorSettings::default();
    let a = search(&shop(), &s);
    let mut x = shop();
    x.tables.reverse();
    x.fks.reverse();
    x.stmts.reverse();
    for st in &mut x.stmts {
        st.touches.reverse();
    }
    let b = search(&x, &s);
    assert_eq!(a.best, b.best);
    assert_eq!(
        a.best.to_json(&shop()).to_string(),
        b.best.to_json(&x).to_string()
    );
}

#[test]
fn scores_lie_in_range_and_scatter_never_helps() {
    let s = AdvisorSettings::default();
    let input = shop();
    let unsharded = Design {
        groups: Vec::new(),
        unsharded: input.tables.iter().map(|t| t.object).collect(),
    };
    assert_eq!(score(&input, &unsharded, &s).total, 1.0);
    let r = search(&input, &s);
    assert!((0.0..=1.0).contains(&r.score.total));
    // A scatter-only statement never raises a score.
    let mut more = input.clone();
    more.stmts
        .push(stmt(99, 1e6, vec![touch(ORDERS, &["created_at"])], vec![]));
    assert!(score(&more, &r.best, &s).total <= r.score.total);
}

#[test]
fn key_filters_reject_low_cardinality_and_skew() {
    let s = AdvisorSettings::default();
    let mut input = shop();
    // products keyed by its id with only 150 values: not enough for 2 shards.
    input.tables[3].columns[0].ndv = Some(150.0);
    let (d, ev) = structural(&input, &s);
    assert!(d.unsharded.contains(&PRODUCTS));
    assert!(ev.iter().any(|e| e.contains("distinct values")), "{ev:?}");
    input.tables[3].columns[0].ndv = Some(1e5);
    input.tables[3].columns[0].top_share = Some(0.4);
    let (d, _) = structural(&input, &s);
    assert!(d.unsharded.contains(&PRODUCTS));
}

#[test]
fn verdicts_follow_the_guardrails() {
    use std::time::Duration;
    let s = AdvisorSettings::default();
    let enough = Evidence {
        observed: Duration::from_secs(25 * 3600),
        units: 20_000,
        confidence: 0.95,
        waited: Duration::ZERO,
    };
    assert_eq!(decide(Cause::FirstSplit, &enough, &s), Verdict::Apply);
    let thin = Evidence {
        units: 10,
        ..enough
    };
    assert_eq!(decide(Cause::FirstSplit, &thin, &s), Verdict::Wait);
    let waited = Evidence {
        waited: Duration::from_secs(73 * 3600),
        ..thin
    };
    assert_eq!(
        decide(Cause::FirstSplit, &waited, &s),
        Verdict::ApplyLowConfidence
    );
    let rec = AdvisorSettings {
        mode: AdvisorMode::Recommend,
        ..s
    };
    assert_eq!(decide(Cause::FirstSplit, &enough, &rec), Verdict::Recommend);
    let off = AdvisorSettings {
        mode: AdvisorMode::Off,
        ..s
    };
    assert_eq!(decide(Cause::FirstSplit, &thin, &off), Verdict::Apply);
}

// Pins (chapter 16 step 8).

fn pin(o: Obj, kind: PinKind) -> Pin {
    Pin {
        object: sprig_common::ids::ObjectId(o),
        db: sprig_common::ids::DbId(1),
        kind,
    }
}

fn shard_key(cols: &[&str]) -> PinKind {
    PinKind::ShardKey {
        columns: cols.iter().map(|c| (*c).to_owned()).collect(),
    }
}

fn colocate(parent: Obj, cols: &[&str]) -> PinKind {
    PinKind::Colocate {
        parent: sprig_common::ids::ObjectId(parent),
        columns: cols.iter().map(|c| (*c).to_owned()).collect(),
    }
}

fn cols(c: &[&str]) -> Routing {
    Routing::Columns {
        columns: c.iter().map(|x| (*x).to_owned()).collect(),
    }
}

fn group_of(d: &Design, o: Obj) -> Option<Obj> {
    d.groups
        .iter()
        .find(|g| g.members.iter().any(|m| m.object == o))
        .map(|g| g.members[0].object)
}

/// Every move of the search's path names its table.
fn moved(r: &SearchResult) -> Vec<Obj> {
    r.path
        .iter()
        .map(|m| match m {
            Move::Attach { object, .. } | Move::Detach { object } => *object,
        })
        .collect()
}

#[test]
fn pinned_unsharded_and_reference_tables_stay_out_of_groups() {
    let s = AdvisorSettings::default();
    let mut input = shop();
    input.pins = vec![
        pin(EVENTS, PinKind::Unsharded),
        pin(PRODUCTS, PinKind::Reference),
    ];
    let r = search(&input, &s);
    assert!(r.best.unsharded.contains(&EVENTS));
    assert!(r.best.unsharded.contains(&PRODUCTS));
    assert_eq!(group_of(&r.best, EVENTS), None);
    assert_eq!(group_of(&r.best, PRODUCTS), None);
    assert!(!moved(&r).contains(&EVENTS));
    // The customers tree is as before; order_items' products key is no
    // cross-group edge any more (products is out of the groups).
    assert_eq!(group_of(&r.best, ITEMS), Some(CUSTOMERS));
    assert!(r.evidence.iter().any(|e| e == "events: pinned UNSHARDED"));
    assert!(
        r.evidence
            .iter()
            .any(|e| e.starts_with("products: pinned REFERENCE; unsharded in R1")),
        "{:?}",
        r.evidence
    );
    assert!(!r.evidence.iter().any(|e| e.contains("fk_items_product")));
    // The JSON says which entries are pinned.
    let j = r.best.to_json(&input);
    let un = j["unsharded"].as_array().unwrap();
    assert!(
        un.iter()
            .any(|u| u["table"] == "events" && u["pin"] == "unsharded")
    );
    assert!(
        un.iter()
            .any(|u| u["table"] == "products" && u["pin"] == "reference")
    );
}

#[test]
fn a_shard_key_pin_routes_by_its_columns() {
    let s = AdvisorSettings::default();
    // The book's example matches the seed: orders stays a column member of
    // the customers group on customer_id.
    let mut input = shop();
    input.pins = vec![pin(ORDERS, shard_key(&["customer_id"]))];
    let (d, _) = structural(&input, &s);
    assert!(members(&d, CUSTOMERS).contains(&(ORDERS, cols(&["customer_id"]))));
    // Keyed by its own id: a group of its own, and order_items follows it
    // as a column member on order_id.
    input.pins = vec![pin(ORDERS, shard_key(&["id"]))];
    let (d, ev) = structural(&input, &s);
    assert_eq!(
        members(&d, ORDERS),
        vec![(ORDERS, cols(&["id"])), (ITEMS, cols(&["order_id"]))]
    );
    assert_eq!(members(&d, CUSTOMERS), vec![(CUSTOMERS, cols(&["id"]))]);
    assert!(
        ev.iter()
            .any(|e| e == "orders: pinned SHARD KEY (id): a group of its own"),
        "{ev:?}"
    );
    let r = search(&input, &s);
    assert_eq!(group_of(&r.best, ORDERS), Some(ORDERS));
    assert!(!moved(&r).contains(&ORDERS));
    // A pinned key passes no NDV filter, only the type check.
    let mut input = shop();
    input.tables[3].columns[0].ndv = Some(150.0);
    assert!(structural(&input, &s).0.unsharded.contains(&PRODUCTS));
    input.pins = vec![pin(PRODUCTS, shard_key(&["id"]))];
    assert_eq!(
        group_of(&structural(&input, &s).0, PRODUCTS),
        Some(PRODUCTS)
    );
}

#[test]
fn a_colocate_pin_places_a_table_without_a_foreign_key() {
    let s = AdvisorSettings::default();
    let mut input = shop();
    input.pins = vec![pin(EVENTS, colocate(CUSTOMERS, &["customer_id"]))];
    let (d, _) = structural(&input, &s);
    assert!(members(&d, CUSTOMERS).contains(&(EVENTS, cols(&["customer_id"]))));
    // The search has nothing left to move it for.
    let r = search(&input, &s);
    assert!(!moved(&r).contains(&EVENTS));
    assert_eq!(group_of(&r.best, EVENTS), Some(CUSTOMERS));
    // Through orders, which is in the customers group on customer_id.
    input.pins = vec![pin(EVENTS, colocate(ORDERS, &["customer_id"]))];
    let (d, _) = structural(&input, &s);
    assert!(members(&d, CUSTOMERS).contains(&(EVENTS, cols(&["customer_id"]))));
    // In place of a foreign-key parent: order_items with products.
    input.pins = vec![pin(ITEMS, colocate(PRODUCTS, &["product_id"]))];
    let (d, ev) = structural(&input, &s);
    assert_eq!(
        members(&d, PRODUCTS),
        vec![(PRODUCTS, cols(&["id"])), (ITEMS, cols(&["product_id"]))]
    );
    assert!(
        ev.iter()
            .any(|e| e.contains("cross-group foreign key fk_items_order")),
        "{ev:?}"
    );
    // A parent that stays unsharded can't take it: the pin is reported.
    input.pins = vec![pin(EVENTS, colocate(COUNTRIES, &["customer_id"]))];
    let (d, ev) = structural(&input, &s);
    assert!(d.unsharded.contains(&EVENTS));
    assert!(ev.iter().any(|e| e.contains("not honoured")), "{ev:?}");
}

#[test]
fn the_search_never_moves_a_pinned_table() {
    let s = AdvisorSettings::default();
    // Unpinned, the search attaches events to customers (above). Pinned
    // to its own key, events keeps its group.
    let mut input = shop();
    input.pins = vec![pin(EVENTS, shard_key(&["id"]))];
    let r = search(&input, &s);
    assert!(!moved(&r).contains(&EVENTS));
    assert_eq!(members(&r.best, EVENTS), vec![(EVENTS, cols(&["id"]))]);
    for (d, _) in &r.alternatives {
        assert_eq!(group_of(d, EVENTS), Some(EVENTS));
    }
    // Pinned with its seed place, orders is never detached.
    input.pins = vec![pin(ORDERS, shard_key(&["customer_id"]))];
    let r = search(&input, &s);
    assert!(!moved(&r).contains(&ORDERS));
    for (d, _) in std::iter::once(&(r.best.clone(), r.score)).chain(&r.alternatives) {
        assert_eq!(group_of(d, ORDERS), Some(CUSTOMERS));
    }
    // Pins in any order give the same design.
    input.pins = vec![
        pin(EVENTS, colocate(CUSTOMERS, &["customer_id"])),
        pin(COUNTRIES, PinKind::Reference),
        pin(ORDERS, shard_key(&["id"])),
    ];
    let a = search(&input, &s);
    input.pins.reverse();
    let b = search(&input, &s);
    assert_eq!(a.best, b.best);
}
