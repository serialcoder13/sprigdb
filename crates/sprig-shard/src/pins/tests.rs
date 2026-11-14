#![allow(clippy::unwrap_used)]

use super::*;

// An example shop: 1 customers, 2 orders, 3 order_items, 4 events,
// 5 countries, 6 audit_log.
const CUSTOMERS: ObjectId = ObjectId(1);
const ORDERS: ObjectId = ObjectId(2);
const ITEMS: ObjectId = ObjectId(3);
const EVENTS: ObjectId = ObjectId(4);
const COUNTRIES: ObjectId = ObjectId(5);
const AUDIT: ObjectId = ObjectId(6);

fn name(o: ObjectId) -> String {
    let n = match o.0 {
        1 => "customers",
        2 => "orders",
        3 => "order_items",
        4 => "events",
        5 => "countries",
        6 => "audit_log",
        _ => "?",
    };
    format!("shop.{n}")
}

fn bin() -> KeyClass {
    column_class("varchar", Some("utf8mb4_bin")).unwrap()
}

fn columns() -> BTreeMap<ObjectId, Columns> {
    let num = Some(KeyClass::Numeric);
    let mut m = BTreeMap::new();
    m.insert(
        CUSTOMERS,
        vec![("id".into(), num), ("email".into(), Some(bin()))],
    );
    m.insert(
        ORDERS,
        vec![
            ("id".into(), num),
            ("customer_id".into(), num),
            ("note".into(), None),
        ],
    );
    m.insert(ITEMS, vec![("order_id".into(), num), ("line".into(), num)]);
    m.insert(
        EVENTS,
        vec![
            ("id".into(), num),
            ("customer_id".into(), num),
            ("email".into(), Some(bin())),
        ],
    );
    m.insert(COUNTRIES, vec![("code".into(), Some(bin()))]);
    m.insert(AUDIT, vec![("id".into(), num)]);
    m
}

fn pin(o: ObjectId, kind: PinKind) -> Pin {
    Pin {
        object: o,
        db: DbId(1),
        kind,
    }
}

fn key(cols: &[&str]) -> PinKind {
    PinKind::ShardKey {
        columns: cols.iter().map(|c| (*c).to_owned()).collect(),
    }
}

fn colo(parent: ObjectId, cols: &[&str]) -> PinKind {
    PinKind::Colocate {
        parent,
        columns: cols.iter().map(|c| (*c).to_owned()).collect(),
    }
}

fn check(pins: &[Pin], placed: &[Placed], new: &Pin) -> Result<()> {
    let cols = columns();
    check_pin(
        new,
        &Facts {
            pins,
            columns: &cols,
            placed,
            name: &name,
        },
    )
}

fn code(r: Result<()>) -> u16 {
    r.unwrap_err().protocol().mysql_errno
}

/// A group keyed by customer id with `customers`, `orders` and
/// `order_items`, in `shards` shards.
fn customers_group(shards: usize) -> Vec<Placed> {
    let k = KeySpec::new(vec![KeyClass::Numeric]).unwrap();
    let m = |o, routing| Placed {
        object: o,
        group: GroupId(3),
        routing,
        key: Some(k.clone()),
        shards,
    };
    vec![
        m(CUSTOMERS, MemberRouting::Columns(vec!["id".into()])),
        m(ORDERS, MemberRouting::Columns(vec!["customer_id".into()])),
        m(
            ITEMS,
            MemberRouting::Parent {
                parent: ORDERS,
                fk: "fk_items_order".into(),
            },
        ),
    ]
}

#[test]
fn chapter_16_examples_are_accepted() {
    let mut pins = Vec::new();
    for p in [
        pin(ORDERS, key(&["customer_id"])),
        pin(EVENTS, colo(CUSTOMERS, &["customer_id"])),
        pin(COUNTRIES, PinKind::Reference),
        pin(AUDIT, PinKind::Unsharded),
    ] {
        check(&pins, &[], &p).unwrap();
        pins.push(p);
    }
    // Before any design: nothing is placed, so nothing needs a re-key.
    assert_eq!(
        code(check(&pins, &[], &pin(CUSTOMERS, key(&["email"])))),
        39410
    );
}

#[test]
fn columns_must_exist_once_and_be_key_types() {
    assert_eq!(code(check(&[], &[], &pin(ORDERS, key(&["nope"])))), 1072);
    assert_eq!(
        code(check(&[], &[], &pin(ORDERS, key(&["id", "id"])))),
        39410
    );
    assert_eq!(code(check(&[], &[], &pin(ORDERS, key(&["note"])))), 39410);
    assert_eq!(
        code(check(&[], &[], &pin(EVENTS, colo(CUSTOMERS, &["nope"])))),
        1072
    );
}

#[test]
fn colocate_rules() {
    // Itself.
    assert_eq!(
        code(check(&[], &[], &pin(EVENTS, colo(EVENTS, &["id"])))),
        39410
    );
    // With a table pinned out of sharded groups, either order.
    for kind in [PinKind::Unsharded, PinKind::Reference] {
        let pins = [pin(CUSTOMERS, kind.clone())];
        assert_eq!(
            code(check(
                &pins,
                &[],
                &pin(EVENTS, colo(CUSTOMERS, &["customer_id"]))
            )),
            39410
        );
        let pins = [pin(EVENTS, colo(CUSTOMERS, &["customer_id"]))];
        assert_eq!(code(check(&pins, &[], &pin(CUSTOMERS, kind))), 39410);
    }
    // A cycle.
    let pins = [pin(EVENTS, colo(ORDERS, &["id"]))];
    assert_eq!(
        code(check(&pins, &[], &pin(ORDERS, colo(EVENTS, &["id"])))),
        39410
    );
    // Classes against the root's SHARD KEY pin, through a chain.
    let pins = [pin(CUSTOMERS, key(&["id"]))];
    check(&pins, &[], &pin(EVENTS, colo(CUSTOMERS, &["customer_id"]))).unwrap();
    assert_eq!(
        code(check(&pins, &[], &pin(EVENTS, colo(CUSTOMERS, &["email"])))),
        39410
    );
    assert_eq!(
        code(check(
            &pins,
            &[],
            &pin(EVENTS, colo(CUSTOMERS, &["customer_id", "id"]))
        )),
        39410
    );
    let chain = [
        pin(CUSTOMERS, key(&["id"])),
        pin(ORDERS, colo(CUSTOMERS, &["customer_id"])),
    ];
    check(&chain, &[], &pin(EVENTS, colo(ORDERS, &["customer_id"]))).unwrap();
    assert_eq!(
        code(check(&chain, &[], &pin(EVENTS, colo(ORDERS, &["email"])))),
        39410
    );
    // A new SHARD KEY on the root must match the tables co-located with it.
    let deps = [pin(EVENTS, colo(CUSTOMERS, &["customer_id"]))];
    check(&deps, &[], &pin(CUSTOMERS, key(&["id"]))).unwrap();
    assert_eq!(
        code(check(&deps, &[], &pin(CUSTOMERS, key(&["email"])))),
        39410
    );
    // Without a root key yet, the classes are the advisor's to settle.
    check(&[], &[], &pin(EVENTS, colo(CUSTOMERS, &["email"]))).unwrap();
    // A placed root: its group's key.
    let placed = customers_group(1);
    assert_eq!(
        code(check(
            &[],
            &placed,
            &pin(EVENTS, colo(CUSTOMERS, &["email"]))
        )),
        39410
    );
    check(
        &[],
        &placed,
        &pin(EVENTS, colo(CUSTOMERS, &["customer_id"])),
    )
    .unwrap();
}

#[test]
fn split_groups_take_only_pins_that_match() {
    let split = customers_group(4);
    // As it is: fine.
    check(&[], &split, &pin(ORDERS, key(&["customer_id"]))).unwrap();
    check(&[], &split, &pin(CUSTOMERS, key(&["id"]))).unwrap();
    check(&[], &split, &pin(ORDERS, colo(CUSTOMERS, &["customer_id"]))).unwrap();
    // Another key, another group or out of the group: a re-key.
    assert_eq!(code(check(&[], &split, &pin(ORDERS, key(&["id"])))), 39411);
    assert_eq!(
        code(check(&[], &split, &pin(ITEMS, key(&["order_id"])))),
        39411
    );
    assert_eq!(
        code(check(&[], &split, &pin(ORDERS, PinKind::Unsharded))),
        39411
    );
    assert_eq!(
        code(check(&[], &split, &pin(ORDERS, PinKind::Reference))),
        39411
    );
    // One shard: still the first design's to change.
    let one = customers_group(1);
    check(&[], &one, &pin(ORDERS, key(&["id"]))).unwrap();
    check(&[], &one, &pin(ORDERS, PinKind::Unsharded)).unwrap();
    // A table outside the split group may join it later (adoption).
    check(&[], &split, &pin(EVENTS, colo(CUSTOMERS, &["customer_id"]))).unwrap();
}

#[test]
fn placement_matching() {
    let g = customers_group(2);
    assert!(matches_placement(&pin(ORDERS, key(&["customer_id"])), &g));
    assert!(matches_placement(
        &pin(ORDERS, colo(CUSTOMERS, &["customer_id"])),
        &g
    ));
    assert!(!matches_placement(&pin(ORDERS, key(&["id"])), &g));
    assert!(!matches_placement(&pin(ITEMS, key(&["order_id"])), &g));
    assert!(!matches_placement(&pin(EVENTS, key(&["id"])), &g));
    assert!(!matches_placement(&pin(ORDERS, PinKind::Unsharded), &g));
}

#[test]
fn column_classes() {
    assert_eq!(column_class("BIGINT", None), Some(KeyClass::Numeric));
    assert_eq!(column_class("varchar", Some("utf8mb4_bin")), Some(bin()));
    assert_eq!(column_class("varchar", None), None);
    assert_eq!(column_class("text", Some("utf8mb4_bin")), None);
    assert_eq!(class_name(KeyClass::Numeric), "numeric");
    assert_eq!(class_name(bin()), "string(utf8mb4_bin)");
}

#[test]
fn an_older_broken_pin_blocks_only_its_own_chain() {
    // events' pin no longer matches the customers group's key (say the
    // column type changed after it was set).
    let pins = [pin(EVENTS, colo(CUSTOMERS, &["email"]))];
    let placed = customers_group(1);
    check(&pins, &placed, &pin(AUDIT, PinKind::Unsharded)).unwrap();
    check(&pins, &placed, &pin(ORDERS, key(&["customer_id"]))).unwrap();
    assert_eq!(
        code(check(&pins, &placed, &pin(CUSTOMERS, key(&["id"])))),
        39410
    );
}
