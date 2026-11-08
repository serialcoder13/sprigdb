#![allow(clippy::unwrap_used)]

use super::*;

#[test]
fn module_formats_are_numbered_without_gaps() {
    assert_eq!(AUTOINDEX_MODULE.name, "autoindex");
    for (i, m) in AUTOINDEX_MODULE.migrations.iter().enumerate() {
        assert_eq!(m.format as usize, i + 1);
        for s in m.statements {
            assert!(
                s.starts_with("CREATE TABLE IF NOT EXISTS") || s.contains("ON DUPLICATE KEY"),
                "not idempotent: {s}"
            );
        }
    }
    sprig_catalog::Migrator::new(&[AUTOINDEX_MODULE]).unwrap();
}

/// The column names of a `CREATE TABLE` statement, in order.
fn columns(ddl: &str) -> Vec<&str> {
    ddl.lines()
        .skip(1)
        .map(str::trim)
        .filter(|l| {
            !l.is_empty()
                && !l.starts_with(')')
                && !l.starts_with("PRIMARY")
                && !l.starts_with("UNIQUE")
                && !l.starts_with("KEY")
                && !l.starts_with('\'')
        })
        .filter_map(|l| l.split_whitespace().next())
        .collect()
}

#[test]
fn tables_have_appendix_c_columns() {
    assert_eq!(
        columns(DDL_AUTO_INDEXES),
        [
            "index_id",
            "object_id",
            "name",
            "model",
            "kind",
            "definition",
            "state",
            "prior_state",
            "candidate_id",
            "validation_id",
            "decision_id",
            "est_benefit_ms_h",
            "est_write_ms_h",
            "est_bytes",
            "actual_bytes",
            "shards",
            "baseline",
            "observed",
            "last_used_at",
            "drop_reason",
            "created_at",
            "state_changed_at",
        ]
    );
    assert_eq!(
        columns(DDL_CANDIDATES),
        [
            "candidate_id",
            "run_id",
            "object_id",
            "def_hash",
            "kind",
            "definition",
            "covering",
            "fingerprints",
            "status",
            "reason",
            "benefit_ms_h",
            "write_ms_h",
            "est_bytes",
            "build_seconds",
            "evidence",
            "created_at",
        ]
    );
    assert_eq!(columns(DDL_VALIDATIONS).len(), 19);
}

#[test]
fn every_enum_value_is_in_its_ddl() {
    for s in IndexState::ALL {
        assert!(DDL_AUTO_INDEXES.contains(&format!("'{s}'")), "{s}");
    }
    for k in StoredKind::ALL {
        assert!(DDL_AUTO_INDEXES.contains(&format!("'{k}'")), "{k}");
        assert!(k.as_str().len() <= 16 || k.is_advice(), "{k}");
    }
    for m in Model::ALL {
        assert!(DDL_AUTO_INDEXES.contains(&format!("'{m}'")), "{m}");
    }
    for s in CandidateStatus::ALL {
        assert!(DDL_CANDIDATES.contains(&format!("'{s}'")), "{s}");
    }
    for m in ValidationMethod::ALL {
        assert!(DDL_VALIDATIONS.contains(&format!("'{m}'")), "{m}");
    }
    for v in Verdict::ALL {
        assert!(DDL_VALIDATIONS.contains(&format!("'{v}'")), "{v}");
    }
    for d in DropReason::ALL {
        assert!(d.as_str().len() <= 48);
        assert_eq!(DropReason::parse(d.as_str()).unwrap(), *d);
    }
}

#[test]
fn state_machine_follows_chapter_13() {
    use IndexState as S;
    // The main path.
    for (a, b) in [
        (S::Validating, S::RollingOut),
        (S::Validating, S::Recommended),
        (S::Validating, S::Rejected),
        (S::Recommended, S::RollingOut),
        (S::Recommended, S::Dropped),
        (S::RollingOut, S::Observing),
        (S::RollingOut, S::Failed),
        (S::Observing, S::Active),
        (S::Observing, S::Retiring),
        (S::Active, S::Retiring),
        (S::Retiring, S::Active),
        (S::Retiring, S::Dropping),
        (S::Active, S::Dropping),
        (S::Dropping, S::Dropped),
        // A failed build's leftovers are cleaned up.
        (S::Failed, S::Dropping),
    ] {
        assert!(a.can_move_to(b), "{a} -> {b}");
    }
    // Some edges that don't exist.
    for (a, b) in [
        (S::Validating, S::Active),
        (S::Active, S::Observing),
        (S::Dropped, S::Active),
        (S::Rejected, S::RollingOut),
        (S::Retiring, S::Observing),
        (S::Frozen, S::Dropped),
        (S::Failed, S::Active),
        (S::Failed, S::RollingOut),
        (S::Rejected, S::Dropping),
    ] {
        assert!(!a.can_move_to(b), "{a} -> {b}");
    }
    // Freeze from anywhere but frozen.
    for s in S::ALL {
        assert_eq!(s.can_move_to(S::Frozen), *s != S::Frozen, "{s}");
    }
    assert!(S::Active.counts_for_budget());
    assert!(!S::Validating.counts_for_budget());
    assert!(!S::Dropped.counts_for_budget());
}

#[test]
fn origins_derive_from_names_unless_recorded() {
    assert_eq!(origin_of("PRIMARY", None), Origin::System);
    assert_eq!(origin_of("sprig_auto_5be0c2a1", None), Origin::Auto);
    assert_eq!(origin_of("idx_created", None), Origin::User);
    assert_eq!(
        origin_of("idx_created", Some(Origin::Migrated)),
        Origin::Migrated
    );
    // A name that only looks automatic is a user index.
    assert_eq!(origin_of("sprig_auto_XYZ", None), Origin::User);
}

#[test]
fn drop_reasons_that_block_recreation() {
    let blocking: Vec<_> = DropReason::ALL
        .iter()
        .filter(|r| r.blocks_recreate())
        .collect();
    assert_eq!(
        blocking,
        [
            &DropReason::NoGain,
            &DropReason::Regression,
            &DropReason::WriteCost,
            &DropReason::Operator,
            &DropReason::Dismissed
        ]
    );
}

#[test]
fn definition_json_shape() {
    let def = IndexDef {
        object: ObjectId(5),
        kind: IndexKind::BTree,
        parts: vec![KeyPart::col("a"), KeyPart::col("b").desc()],
        options: IndexOptions::default(),
    };
    let j = serde_json::to_value(Definition::of(&def)).unwrap();
    assert_eq!(
        j,
        serde_json::json!({"parts": [{"column": "a"}, {"column": "b", "desc": true}]})
    );
    let back: Definition = serde_json::from_value(j).unwrap();
    assert_eq!(back.on(ObjectId(5), IndexKind::BTree), def);
}

#[test]
fn truncation_keeps_char_boundaries() {
    assert_eq!(truncate("abc", 2), "ab");
    assert_eq!(truncate("aé", 2), "a");
    assert_eq!(truncate("abc", 9), "abc");
}
