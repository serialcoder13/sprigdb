#![allow(
    clippy::unwrap_used,
    clippy::many_single_char_names,
    clippy::format_collect,
    clippy::too_many_lines
)]

use super::*;

fn member(o: u64, g: u32, off: bool) -> GroupMember {
    GroupMember {
        object: ObjectId(o),
        group: GroupId(g),
        db: DbId(1),
        routing: MemberRouting::Columns(vec!["k".into()]),
        rk_state: RkState::Ready,
        off_class_seen: off,
    }
}

fn group(parts: Vec<KeyClass>) -> TableGroup {
    TableGroup {
        id: GroupId(3),
        kind: GroupKind::Sharded,
        name: "g".into(),
        key: Some(KeySpec::new(parts).unwrap()),
        state: GroupState::Active,
        min_shards: None,
    }
}

fn cmp(c: CmpType) -> Comparison {
    Comparison {
        cmp: c,
        part_a: Some(0),
        part_b: Some(0),
        sql_rules: true,
    }
}

#[test]
fn colocation_rules() {
    let g = group(vec![KeyClass::Numeric]);
    let (a, b) = (member(1, 3, false), member(2, 3, false));
    assert!(colocation_safe(&cmp(CmpType::Integer), &g, &a, &b));
    assert!(colocation_safe(&cmp(CmpType::Decimal), &g, &a, &b));
    assert!(!colocation_safe(&cmp(CmpType::Double), &g, &a, &b));
    // Another group, another part, no part.
    assert!(!colocation_safe(
        &cmp(CmpType::Integer),
        &g,
        &a,
        &member(2, 4, false)
    ));
    let mut c = cmp(CmpType::Integer);
    c.part_b = Some(1);
    assert!(!colocation_safe(&c, &g, &a, &b));
    c.part_b = None;
    assert!(!colocation_safe(&c, &g, &a, &b));
    // Off-class values break SQL co-location, not MongoDB's.
    let off = member(2, 3, true);
    assert!(!colocation_safe(&cmp(CmpType::Integer), &g, &a, &off));
    let mut mongo = cmp(CmpType::Integer);
    mongo.sql_rules = false;
    assert!(colocation_safe(&mongo, &g, &a, &off));
    // Strings: same collation, or a finer _bin of the same charset.
    let ci = CollationId(2304); // utf8mb4_uca1400_ai_ci, PAD SPACE
    let gs = group(vec![KeyClass::String { collation: ci }]);
    assert!(colocation_safe(&cmp(CmpType::String(ci)), &gs, &a, &b));
    assert!(colocation_safe(
        &cmp(CmpType::String(CollationId(46))),
        &gs,
        &a,
        &b
    ));
    assert!(colocation_safe(
        &cmp(CmpType::String(CollationId(1070))),
        &gs,
        &a,
        &b
    ));
    assert!(!colocation_safe(
        &cmp(CmpType::String(CollationId(224))),
        &gs,
        &a,
        &b
    ));
    assert!(!colocation_safe(
        &cmp(CmpType::String(CollationId(47))),
        &gs,
        &a,
        &b
    ));
    // A PAD SPACE _bin isn't finer than a NO PAD group.
    let np = group(vec![KeyClass::String {
        collation: CollationId(2308),
    }]);
    assert!(!colocation_safe(
        &cmp(CmpType::String(CollationId(46))),
        &np,
        &a,
        &b
    ));
}

#[test]
fn module_is_well_formed() {
    assert_eq!(SHARDING_MODULE.name, "sharding");
    for (i, m) in SHARDING_MODULE.migrations.iter().enumerate() {
        assert_eq!(m.format as usize, i + 1);
        assert_eq!(m.phase, Phase::Expand);
        for s in m.statements {
            assert!(s.starts_with("CREATE TABLE IF NOT EXISTS"), "{s}");
        }
    }
    sprig_catalog::Migrator::new(&[SHARDING_MODULE]).unwrap();
}
