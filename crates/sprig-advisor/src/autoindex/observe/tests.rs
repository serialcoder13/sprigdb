//! Unit tests of observation: the watch's rules and supersession.

#![allow(clippy::unwrap_used, clippy::float_cmp)]

use std::collections::BTreeMap;

use sprig_common::ids::{Fingerprint, ObjectId};

use super::*;
use crate::autoindex::model::{IndexDef, KeyPart, Origin};
use crate::autoindex::rollout::node::NodeIndex;
use crate::autoindex::store::{Baseline, DropReason};

fn rules() -> ObserveRules {
    ObserveRules {
        min_improvement_pct: 30.0,
        max_regression_pct: 10.0,
        observe_days: 7,
        recreate_block_days: 14,
        usage_sample_reads: 100,
        min_usage_ratio: 0.05,
        retire_user_indexes: crate::autoindex::policy::Mode::On,
        maintenance_cost_ms: 0.05,
    }
}

fn b(p95: f64, mean: f64) -> Baseline {
    Baseline {
        runs: 1000,
        p95_ms: p95,
        mean_ms: mean,
    }
}

fn l(runs: u64, p95: f64, mean: f64, uses: Option<bool>, write: bool) -> FpLatency {
    FpLatency {
        runs,
        p95_ms: p95,
        mean_ms: mean,
        uses_index: uses,
        write,
        affected_per_run: 1.0,
        updated: None,
    }
}

const T: Fingerprint = Fingerprint(0x9c41_d2e0_7b3a_5f18);
const R: Fingerprint = Fingerprint(0x51aa_02c4_e9d0_f337);
const W: Fingerprint = Fingerprint(0x1111_2222_3333_4444);

fn base() -> BTreeMap<String, Baseline> {
    [(T, b(400.0, 260.0)), (R, b(100.0, 80.0)), (W, b(2.0, 1.0))]
        .into_iter()
        .map(|(f, b)| (f.to_string(), b))
        .collect()
}

#[test]
fn a_good_index_is_kept_after_the_period() {
    let now: BTreeMap<_, _> = [(T, l(500, 12.0, 9.0, Some(true), false))].into();
    let (v, m) = judge_watch(&base(), &[T], &now, 24.0, &rules(), 5, false);
    assert_eq!(v, WatchVerdict::Continue);
    assert!(m.improvement_pct.unwrap() > 95.0);
    let (v, _) = judge_watch(&base(), &[T], &now, 24.0 * 7.0, &rules(), 5, true);
    assert_eq!(v, WatchVerdict::Activate);
}

#[test]
fn no_gain_regression_and_write_cost_retire() {
    // The targets barely improved.
    let now: BTreeMap<_, _> = [(T, l(500, 350.0, 240.0, Some(true), false))].into();
    assert_eq!(
        judge_watch(&base(), &[T], &now, 24.0, &rules(), 5, false).0,
        WatchVerdict::Retire(DropReason::NoGain)
    );
    // Another reader now uses it and got 44% slower (worked example 3).
    let now: BTreeMap<_, _> = [
        (T, l(500, 4.0, 3.0, Some(true), false)),
        (R, l(40, 144.0, 120.0, Some(true), false)),
    ]
    .into();
    let (v, m) = judge_watch(&base(), &[T], &now, 24.0, &rules(), 5, false);
    assert_eq!(v, WatchVerdict::Retire(DropReason::Regression));
    assert_eq!(m.worst_fp.as_deref(), Some(R.to_string().as_str()));
    // The same slowdown in a reader that doesn't use the index is noise.
    let now: BTreeMap<_, _> = [
        (T, l(500, 4.0, 3.0, Some(true), false)),
        (R, l(40, 144.0, 120.0, Some(false), false)),
    ]
    .into();
    assert_eq!(
        judge_watch(&base(), &[T], &now, 24.0, &rules(), 5, false).0,
        WatchVerdict::Continue
    );
    // Writes 3x slower and many of them; the target runs rarely.
    let now: BTreeMap<_, _> = [
        (T, l(10, 4.0, 3.0, Some(true), false)),
        (W, l(1_000_000, 6.0, 3.0, None, true)),
    ]
    .into();
    let (v, m) = judge_watch(&base(), &[T], &now, 24.0, &rules(), 5, false);
    assert_eq!(v, WatchVerdict::Retire(DropReason::WriteCost));
    assert!(m.write_cost_ms_h.unwrap() > m.benefit_ms_h.unwrap());
    // Too few runs to judge: keep watching.
    let now: BTreeMap<_, _> = [(T, l(3, 390.0, 250.0, Some(true), false))].into();
    assert_eq!(
        judge_watch(&base(), &[T], &now, 24.0, &rules(), 5, false).0,
        WatchVerdict::Continue
    );
}

fn ni(name: &str, cols: &[&str]) -> NodeIndex {
    NodeIndex {
        name: name.into(),
        parts: cols.iter().map(|c| KeyPart::col(c)).collect(),
        unique: false,
        ignored: false,
    }
}

#[test]
fn user_and_migrated_indexes_supersede_ours() {
    let def = IndexDef::btree(ObjectId(7), &["customer_id", "created_at"]);
    let mut ix: BTreeMap<String, NodeIndex> = BTreeMap::new();
    ix.insert(
        "PRIMARY".into(),
        ni("PRIMARY", &["customer_id", "created_at"]),
    );
    ix.insert(def.name(), ni(&def.name(), &["customer_id", "created_at"]));
    ix.insert("idx_c".into(), ni("idx_c", &["customer_id"]));
    let rec = BTreeMap::new();
    // Neither the primary key, our own index nor a shorter one supersede.
    assert!(superseding(&ix, &rec, &def).is_none());
    // A user index that extends ours does.
    ix.insert(
        "idx_cc".into(),
        ni("idx_cc", &["customer_id", "created_at", "total"]),
    );
    assert_eq!(superseding(&ix, &rec, &def).unwrap().name, "idx_cc");
    // Unless it is ignored.
    ix.get_mut("idx_cc").unwrap().ignored = true;
    assert!(superseding(&ix, &rec, &def).is_none());
    // A migrated one counts too.
    ix.insert("imp".into(), ni("imp", &["customer_id", "created_at"]));
    let rec: BTreeMap<String, Origin> = [("imp".to_owned(), Origin::Migrated)].into();
    assert_eq!(superseding(&ix, &rec, &def).unwrap().name, "imp");
    // Directions and prefixes must match or be wider.
    let mut d = KeyPart::col("customer_id");
    d.desc = true;
    assert!(!supersedes(&[d, KeyPart::col("created_at")], &def.parts));
    let mut short = KeyPart::col("customer_id");
    short.prefix = Some(8);
    let mut long = KeyPart::col("customer_id");
    long.prefix = Some(16);
    assert!(supersedes(&[long.clone()], &[short.clone()]));
    assert!(!supersedes(&[short], &[long]));
}

// ---- decision O1: usage, never days ----

use super::usage::{
    Before, ReadUse, Remembered, UsageRules, UsageVerdict, before_of, entry_changes, judge,
    regressed, remember, saved_per_run,
};
use super::user::{TableIndex, add_user_sql, clause_of, drop_user_sql, ignore_user_sql, protected};

const P: Fingerprint = Fingerprint(0x0bad_cafe_0000_0001);

fn ur() -> UsageRules {
    UsageRules {
        sample_reads: 100_000,
        min_usage_ratio: 0.05,
        maintenance_cost_ms: 0.05,
    }
}

fn read(fp: Fingerprint, runs: u64, uses: Option<bool>, saved: f64) -> ReadUse {
    ReadUse {
        fp,
        runs,
        uses,
        saved_ms_per_run: saved,
    }
}

#[test]
fn nothing_is_judged_before_enough_reads() {
    let reads = [read(T, 99_999, Some(false), 0.0)];
    let (v, m) = judge(&reads, 1e6, &BTreeMap::new(), None, &ur());
    assert_eq!(v, UsageVerdict::NotYet);
    assert_eq!(m.reads, 99_999);
}

#[test]
fn the_ratio_rule() {
    // 5% exactly is kept; 4.99% with no value is removed.
    let reads = [
        read(T, 5_000, Some(true), 0.0),
        read(R, 95_000, Some(false), 0.0),
    ];
    let (v, m) = judge(&reads, 100.0, &BTreeMap::new(), None, &ur());
    assert_eq!(v, UsageVerdict::Keep);
    assert!((m.ratio - 0.05).abs() < 1e-12);
    let reads = [
        read(T, 4_990, Some(true), 0.0),
        read(R, 95_010, Some(false), 0.0),
    ];
    let (v, m) = judge(&reads, 100.0, &BTreeMap::new(), None, &ur());
    assert_eq!(v, UsageVerdict::Remove);
    assert_eq!(m.served, 4_990);
    // Never used at all is the extreme case, even with no writes.
    let reads = [read(R, 100_000, Some(false), 0.0)];
    assert_eq!(
        judge(&reads, 0.0, &BTreeMap::new(), None, &ur()).0,
        UsageVerdict::Remove
    );
}

#[test]
fn the_value_check_compares_saved_time_with_write_cost() {
    // 1% of reads, each saving 2 ms: 2,000 ms saved.
    let reads = [
        read(T, 1_000, Some(true), 2.0),
        read(R, 99_000, Some(false), 0.0),
    ];
    // 30,000 entry changes × 0.05 ms = 1,500 ms: worth it, kept.
    let (v, m) = judge(&reads, 30_000.0, &BTreeMap::new(), None, &ur());
    assert_eq!(v, UsageVerdict::Keep);
    assert_eq!(m.saved_ms, 2_000.0);
    assert_eq!(m.write_cost_ms, 1_500.0);
    // 50,000 changes × 0.05 = 2,500 ms: costs more than it saves, removed.
    let (v, m) = judge(&reads, 50_000.0, &BTreeMap::new(), None, &ur());
    assert_eq!(v, UsageVerdict::Remove);
    assert!(m.saved_ms < m.write_cost_ms);
}

#[test]
fn unknown_plans_decide_nothing_when_they_could_matter() {
    // 10% of reads couldn't be explained: they might use it.
    let reads = [
        read(R, 90_000, Some(false), 0.0),
        read(T, 10_000, None, 0.0),
    ];
    assert_eq!(
        judge(&reads, 1e6, &BTreeMap::new(), None, &ur()).0,
        UsageVerdict::Unknown
    );
    // 1% unknown can't lift it to 5%: removed.
    let reads = [read(R, 99_000, Some(false), 0.0), read(T, 1_000, None, 0.0)];
    assert_eq!(
        judge(&reads, 1e6, &BTreeMap::new(), None, &ur()).0,
        UsageVerdict::Remove
    );
}

#[test]
fn a_periodic_job_keeps_its_index() {
    // Month-end: P ran 3 times in an earlier sample, saving 9 s each.
    let mut memory = BTreeMap::new();
    remember(
        &mut memory,
        &[
            read(P, 3, Some(true), 9_000.0),
            read(R, 99_997, Some(false), 0.0),
        ],
        None,
        1,
    );
    assert_eq!(
        memory[&P.to_string()],
        Remembered {
            saved_ms_per_run: 9_000.0,
            runs: 3,
            seen_ms: 1
        }
    );
    // This sample is all other reads, and writes cost 20,000 ms.
    let reads = [read(R, 100_000, Some(false), 0.0)];
    let alive: std::collections::BTreeSet<Fingerprint> = [P, R].into();
    let (v, m) = judge(&reads, 400_000.0, &memory, Some(&alive), &ur());
    assert_eq!(v, UsageVerdict::Keep);
    assert_eq!(m.periodic_saved_ms, 27_000.0);
    // Once the job is gone from the workload, it no longer protects it.
    let alive: std::collections::BTreeSet<Fingerprint> = [R].into();
    let (v, _) = judge(&reads, 400_000.0, &memory, Some(&alive), &ur());
    assert_eq!(v, UsageVerdict::Remove);
    remember(&mut memory, &reads, Some(&alive), 2);
    assert!(memory.is_empty());
    // Worth less than its writes: removed even while the job exists.
    let mut memory = BTreeMap::new();
    remember(&mut memory, &[read(P, 3, Some(true), 1.0)], None, 1);
    assert_eq!(
        judge(&reads, 400_000.0, &memory, None, &ur()).0,
        UsageVerdict::Remove
    );
}

#[test]
fn savings_and_write_cost() {
    // Measured: the baseline mean before an automatic index was built.
    assert_eq!(saved_per_run(0.5, Some(8.0), 20.0, 1e5), 7.5);
    assert_eq!(saved_per_run(9.0, Some(8.0), 20.0, 1e5), 0.0);
    // Estimated: time per row through the index × rows a full scan reads.
    assert_eq!(saved_per_run(1.0, None, 10.0, 1010.0), 100.0);
    // Inserts and deletes change every index; updates only those over a
    // column they set.
    let mut w = BTreeMap::new();
    w.insert(
        T,
        FpLatency {
            write: true,
            affected_per_run: 1.0,
            ..FpLatency::read(100, 1.0, 1.0)
        },
    );
    w.insert(
        R,
        FpLatency {
            write: true,
            affected_per_run: 2.0,
            updated: Some(vec!["status".into()]),
            ..FpLatency::read(50, 1.0, 1.0)
        },
    );
    w.insert(P, FpLatency::read(1_000_000, 1.0, 1.0));
    assert_eq!(entry_changes(&w, &["b".into()]), 100.0);
    assert_eq!(entry_changes(&w, &["status".into(), "b".into()]), 200.0);
    assert_eq!(super::usage::reads_in(&w), 1_000_000);
}

#[test]
fn a_served_query_slower_while_ignored_restores_the_index() {
    let lat_before: BTreeMap<_, _> = [(T, FpLatency::read(500, 2.0, 1.5))].into();
    let before: BTreeMap<String, Before> = before_of([T], &lat_before);
    // 2 ms → 40 ms: regressed.
    let now: BTreeMap<_, _> = [(T, FpLatency::read(50, 40.0, 30.0))].into();
    let r = regressed(&before, &now, 10.0, 5).unwrap();
    assert_eq!((r.before_ms, r.now_ms), (2.0, 40.0));
    // Too few runs yet, or a fraction of a millisecond: not a regression.
    let now: BTreeMap<_, _> = [(T, FpLatency::read(3, 40.0, 30.0))].into();
    assert!(regressed(&before, &now, 10.0, 5).is_none());
    let now: BTreeMap<_, _> = [(T, FpLatency::read(50, 2.4, 1.6))].into();
    assert!(regressed(&before, &now, 10.0, 5).is_none());
    // A fingerprint it never served isn't watched.
    let now: BTreeMap<_, _> = [(R, FpLatency::read(50, 400.0, 300.0))].into();
    assert!(regressed(&before, &now, 10.0, 5).is_none());
}

fn tix(name: &str, cols: &[&str], unique: bool, ty: &str) -> TableIndex {
    TableIndex {
        name: name.into(),
        parts: cols.iter().map(|c| KeyPart::col(c)).collect(),
        unique,
        index_type: ty.into(),
        ignored: false,
        clause: None,
    }
}

#[test]
fn primary_unique_and_foreign_key_indexes_are_never_removed() {
    let fks = vec![
        vec!["p_id".to_owned()],
        vec!["a".to_owned(), "b".to_owned()],
    ];
    assert_eq!(
        protected(&tix("PRIMARY", &["id"], true, "BTREE"), &fks),
        Some("primary_key")
    );
    assert_eq!(
        protected(&tix("uq_c", &["c"], true, "BTREE"), &fks),
        Some("unique")
    );
    assert_eq!(
        protected(&tix("ix_p", &["p_id"], false, "BTREE"), &fks),
        Some("foreign_key")
    );
    // Leading columns cover the key: needed by it.
    assert_eq!(
        protected(&tix("ix_p2", &["p_id", "x"], false, "BTREE"), &fks),
        Some("foreign_key")
    );
    assert_eq!(
        protected(&tix("ix_ab", &["a", "b", "c"], false, "BTREE"), &fks),
        Some("foreign_key")
    );
    // Not leading, or only part of a composite key: eligible.
    assert_eq!(
        protected(&tix("ix_xp", &["x", "p_id"], false, "BTREE"), &fks),
        None
    );
    assert_eq!(protected(&tix("ix_a", &["a"], false, "BTREE"), &fks), None);
    assert_eq!(
        protected(&tix("sprig_auto_1a2b3c4d", &["x"], false, "BTREE"), &fks),
        Some("automatic")
    );
    assert_eq!(
        protected(&tix("ft", &["x"], false, "FULLTEXT"), &fks),
        Some("not_btree")
    );
    assert_eq!(protected(&tix("idx_b", &["b"], false, "BTREE"), &[]), None);
    // The statements refuse the primary key and automatic indexes.
    let t = ShardTable {
        shard: "0".into(),
        schema: "s".into(),
        table: "u".into(),
    };
    assert!(drop_user_sql(&t, "PRIMARY").is_err());
    assert!(ignore_user_sql(&t, "sprig_auto_1a2b3c4d", true).is_err());
    assert_eq!(
        ignore_user_sql(&t, "idx_b", true).unwrap(),
        "ALTER TABLE `s`.`u` ALTER INDEX `idx_b` IGNORED"
    );
    assert_eq!(
        add_user_sql(&t, "idx_b", "KEY `idx_b` (`b`)").unwrap(),
        "ALTER TABLE `s`.`u` ADD KEY `idx_b` (`b`)"
    );
    assert!(add_user_sql(&t, "idx_b", "KEY `other` (`b`)").is_err());
}

#[test]
fn the_saved_definition_comes_from_show_create_table() {
    let create = "CREATE TABLE `u` (\n  `id` int(11) NOT NULL,\n  `b` int(11) NOT NULL,\n  \
                  `pad` varchar(100) NOT NULL,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `uq_b` (`b`),\n  \
                  KEY `idx_b` (`b`),\n  KEY `idx_old` (`pad`(10),`b` DESC) COMMENT 'old' IGNORED\n) \
                  ENGINE=InnoDB";
    assert_eq!(
        clause_of(create, "idx_b").as_deref(),
        Some("KEY `idx_b` (`b`)")
    );
    assert_eq!(
        clause_of(create, "idx_old").as_deref(),
        Some("KEY `idx_old` (`pad`(10),`b` DESC) COMMENT 'old'")
    );
    assert!(clause_of(create, "uq_b").is_none());
    assert!(clause_of(create, "missing").is_none());
}
