#![allow(
    clippy::unwrap_used,
    clippy::float_cmp,
    clippy::many_single_char_names,
    clippy::field_reassign_with_default
)]

use proptest::prelude::*;

use super::*;
use crate::autoindex::candidates::{Generator, merge};
use crate::autoindex::fixtures::{
    EVENTS, GB, INVOICES, ORDERS, events, events_fps, events_writes, fp_example1, invoices,
    invoices_fps, orders, orders_other_reads, orders_writes,
};
use crate::autoindex::model::{Evidence, IndexDef, KeyPart, PartSource};

fn near(got: f64, want: f64, tol: f64) -> bool {
    (got - want).abs() <= tol * want.abs().max(1.0)
}

fn example1_fps() -> Vec<FpInput> {
    let mut v = vec![fp_example1()];
    v.extend(orders_other_reads());
    v.extend(orders_writes());
    v
}

fn find<'a>(c: &'a [Candidate], cols: &[&str]) -> &'a Candidate {
    c.iter()
        .find(|c| c.def.columns().collect::<Vec<_>>() == cols)
        .unwrap()
}

#[test]
fn example_1_numbers() {
    let t = orders();
    let fps = example1_fps();
    let ctx = TableContext::new(ORDERS, &t, &fps, CostParams::default());
    // u_T 0.0079 ms/row; fixed 2.5 ms.
    assert!(near(ctx.u_t, 0.0079, 1e-9));
    let fixed = 260.0 - 32_600.0 * ctx.u_t;
    assert!(near(fixed, 2.46, 0.01));
    // Writes: 144,000 inserts and 216,000 status updates an hour.
    assert!(near(ctx.writes.inserts_h, 144_000.0, 1e-9));

    let f = &fps[0];
    let g = Generator::default();
    let cands = g.for_table(&ctx.reads[&f.id].1, &t);
    let a = find(&cands, &["customer_id", "status", "created_at"]);
    let b = find(&cands, &["customer_id", "created_at"]);
    // A' = A + total, covering (built here: see the candidate tests).
    let a_cov = Candidate {
        def: IndexDef {
            parts: [a.def.parts.clone(), vec![KeyPart::col("total")]].concat(),
            ..a.def.clone()
        },
        covering: true,
        serves: a.serves.clone(),
        evidence: Evidence {
            parts: vec![
                PartSource::Eq,
                PartSource::Eq,
                PartSource::Order,
                PartSource::Cover,
            ],
            ..Evidence::default()
        },
    };
    let m = Model;
    let ea = m.estimate(&a_cov, &ctx);
    let eb = m.estimate(b, &ctx);
    // A' (customer_id, status, created_at, total): e = 28, t̂ = 2.7 ms,
    // B 195,500 ms/h, Wc 18,000, S 3.0 GB, net/S 59,200 per GB.
    assert!(near(ea.per_fp[0].entries, 28.0, 0.01));
    assert!(
        near(ea.per_fp[0].t_hat_ms, 2.7, 0.01),
        "{}",
        ea.per_fp[0].t_hat_ms
    );
    assert!(
        near(ea.benefit_ms_h, 195_500.0, 0.001),
        "{}",
        ea.benefit_ms_h
    );
    assert!(near(ea.write_ms_h, 18_000.0, 1e-9));
    assert!(near(ea.bytes / GB, 3.0, 0.01), "{}", ea.bytes);
    assert!(near(ea.net() / (ea.bytes / GB), 59_200.0, 0.01));
    // B (customer_id, created_at): e = 286 × 3, t̂ = 9.3 ms, B 190,500 ms/h,
    // Wc 7,200 (inserts only), S 2.24 GB, net/S 81,800 per GB.
    assert!(near(eb.per_fp[0].entries, 286.0, 0.002));
    assert!(
        near(eb.per_fp[0].t_hat_ms, 9.3, 0.01),
        "{}",
        eb.per_fp[0].t_hat_ms
    );
    assert!(
        near(eb.benefit_ms_h, 190_500.0, 0.001),
        "{}",
        eb.benefit_ms_h
    );
    assert!(near(eb.write_ms_h, 7_200.0, 1e-9));
    assert!(near(eb.bytes / GB, 2.24, 0.001));
    assert!(near(eb.net() / (eb.bytes / GB), 81_800.0, 0.001));
    // Both pass every gate; the build takes about 2 minutes.
    assert!(ea.passes() && eb.passes());
    assert!(near(eb.build_seconds, 112.0, 0.01));

    // Budget: 20% of 31.5 GB = 6.3 GB, none yet → B.
    let mut pool: Vec<Pooled> = cands
        .iter()
        .map(|c| Pooled {
            candidate: c.clone(),
            estimate: m.estimate(c, &ctx),
            fast_only: false,
        })
        .filter(|p| p.estimate.passes())
        .collect();
    pool.push(Pooled {
        candidate: a_cov.clone(),
        estimate: ea.clone(),
        fast_only: false,
    });
    let s = select(pool, &ctx, &[], Budget::default());
    assert_eq!(s.chosen.len(), 1, "{:?}", s.chosen);
    assert_eq!(
        s.chosen[0].candidate.def.columns().collect::<Vec<_>>(),
        ["customer_id", "created_at"]
    );
    assert!(s.chosen[0].replaces.is_none());
    // The others add less than they cost once B exists.
    assert!(
        s.deferred
            .iter()
            .all(|(_, r)| r == "marginal_below_min_net" || r.starts_with("covered_by"))
    );
}

#[test]
fn example_2_merged_index_wins_and_f3_is_deferred() {
    let t = events();
    let mut fps: Vec<FpInput> = events_fps().to_vec();
    fps.extend(events_writes());
    let ctx = TableContext::new(EVENTS, &t, &fps, CostParams::default());
    let g = Generator::default();
    let mut cands = Vec::new();
    for f in &fps[..4] {
        cands.extend(
            g.generate_all(f, &|o| (o == EVENTS).then_some(&t))
                .candidates,
        );
    }
    let merged = merge(cands);
    let m = Model;
    let pool: Vec<Pooled> = merged
        .iter()
        .map(|c| Pooled {
            candidate: c.clone(),
            estimate: m.estimate(c, &ctx),
            fast_only: false,
        })
        .collect();
    // Wc = 800,000 inserts × 0.05 ms = 40,000 ms/h for either.
    for p in &pool {
        assert!(near(p.estimate.write_ms_h, 40_000.0, 1e-9));
    }
    let passing: Vec<Pooled> = pool.into_iter().filter(|p| p.estimate.passes()).collect();
    let s = select(passing, &ctx, &[], Budget::default());
    assert_eq!(s.chosen.len(), 1);
    let chosen = &s.chosen[0].candidate;
    assert_eq!(
        chosen.def.columns().collect::<Vec<_>>(),
        ["account_id", "kind", "created_at"]
    );
    // F3's own index: with the merged one F3 reads 2,000 rows (about
    // 16 ms); its own would save less than its 40,000 ms/h write cost.
    let (with_merged, _) = ctx.predict(&chosen.def.parts, Fingerprint(0xF3)).unwrap();
    assert!((10.0..20.0).contains(&with_merged), "{with_merged}");
    let f3 = find(&merged, &["account_id"]);
    let e3 = m.estimate(f3, &ctx);
    let gain = e3.per_fp[0].per_hour * (with_merged - e3.per_fp[0].t_hat_ms);
    assert!(gain < e3.write_ms_h, "gain {gain}");
    assert!(
        s.deferred
            .iter()
            .any(|(c, r)| c.def == f3.def && r == "marginal_below_min_net"),
        "{:?}",
        s.deferred
    );
}

#[test]
fn example_3_covering_variant_costs_72_mb_more() {
    let t = invoices();
    let fps = invoices_fps().to_vec();
    let ctx = TableContext::new(INVOICES, &t, &fps, CostParams::default());
    let c1 = IndexDef::btree(INVOICES, &["state", "due_date"]);
    let c2 = IndexDef::btree(INVOICES, &["state", "due_date", "amount"]);
    let extra = ctx.bytes(&c2.parts) - ctx.bytes(&c1.parts);
    assert!(near(extra, 72e6, 1e-9));
    // F6 with C1 reads the whole table in state order with lookups; with
    // C2 it reads only the index.
    let (t1, _) = ctx.predict(&c1.parts, Fingerprint(0xF6)).unwrap();
    let (t2, _) = ctx.predict(&c2.parts, Fingerprint(0xF6)).unwrap();
    assert!(t2 < t1);
    // F5 is predicted to drop well below its 180 ms either way.
    let (t5, _) = ctx.predict(&c1.parts, Fingerprint(0xF5)).unwrap();
    assert!(t5 < 0.7 * 180.0);
}

#[test]
fn gates() {
    let t = orders();
    let fps = example1_fps();
    let b = IndexDef::btree(ORDERS, &["customer_id", "created_at"]);
    let cand = Candidate {
        def: b,
        covering: false,
        serves: vec![(fp_example1().id, Role::Primary)],
        evidence: Evidence::default(),
    };
    let mut p = CostParams::default();
    // A slow build: 2.24 GB at 0.05 MB/s is 12 hours.
    p.build_rate_bytes_per_sec = 0.05e6;
    let e = Model.estimate(&cand, &TableContext::new(ORDERS, &t, &fps, p));
    assert!(e.failed.contains(&Gate::BuildTime));
    // Demanding a 99% improvement.
    let mut p = CostParams::default();
    p.min_improvement_pct = 99.0;
    let e = Model.estimate(&cand, &TableContext::new(ORDERS, &t, &fps, p));
    assert_eq!(e.failed, [Gate::Improvement(fp_example1().id)]);
    // Without a replica, the margin makes 30% into 45%: B still passes.
    let mut p = CostParams::default();
    p.estimate_margin = Some(1.5);
    assert!(
        Model
            .estimate(&cand, &TableContext::new(ORDERS, &t, &fps, p))
            .passes()
    );
    // Heavy writes: benefit below twice the write cost.
    let mut p = CostParams::default();
    p.maintenance_cost_ms = 20.0;
    let e = Model.estimate(&cand, &TableContext::new(ORDERS, &t, &fps, p));
    assert!(e.failed.contains(&Gate::BenefitRatio));
    assert!(e.failed.contains(&Gate::MinNet));
}

fn auto(name: &str, bytes: f64, value: f64) -> AutoOnTable {
    AutoOnTable {
        name: name.into(),
        info: IndexInfo {
            name: name.into(),
            parts: vec![KeyPart::col(name)],
            unique: false,
            origin: Origin::Auto,
            ignored: false,
            usable: true,
        },
        bytes,
        value_ms_h: value,
        active: true,
    }
}

fn example1_pool(ctx: &TableContext<'_>) -> Vec<Pooled> {
    let f = fp_example1();
    let cands = Generator::default().for_table(&ctx.reads[&f.id].1, ctx.stats);
    cands
        .into_iter()
        .map(|c| Pooled {
            estimate: Model.estimate(&c, ctx),
            candidate: c,
            fast_only: false,
        })
        .filter(|p| p.estimate.passes())
        .collect()
}

#[test]
fn replacement_needs_twice_the_value_per_byte() {
    let t = orders();
    let fps = example1_fps();
    let ctx = TableContext::new(ORDERS, &t, &fps, CostParams::default());
    let full: Vec<AutoOnTable> = (0..5)
        .map(|i| {
            auto(
                &format!("sprig_auto_0000000{i}"),
                0.5 * GB,
                50_000.0 + f64::from(i),
            )
        })
        .collect();
    // 5 indexes already, each worth 100,000 ms/h per GB: B (81,800 per
    // GB) isn't worth twice the weakest, so it waits.
    let s = select(example1_pool(&ctx), &ctx, &full, Budget::default());
    assert!(s.chosen.is_empty());
    assert!(s.deferred.iter().any(|(_, r)| r == "budget"));
    // A weak one (10,000 per GB) is replaced.
    let mut weak = full.clone();
    weak[2].value_ms_h = 5_000.0;
    let s = select(example1_pool(&ctx), &ctx, &weak, Budget::default());
    assert_eq!(s.chosen.len(), 1);
    assert_eq!(s.chosen[0].replaces.as_deref(), Some("sprig_auto_00000002"));
    // A fast-trigger-only candidate never displaces.
    let mut pool = example1_pool(&ctx);
    for p in &mut pool {
        p.fast_only = true;
    }
    let s = select(pool, &ctx, &weak, Budget::default());
    assert!(s.chosen.is_empty());
    assert!(
        s.deferred
            .iter()
            .all(|(_, r)| r == "fast_trigger_no_displace" || r.starts_with("covered"))
    );
}

#[test]
fn tightened_bounds_retire_one_and_add_nothing() {
    let t = orders();
    let fps = example1_fps();
    let ctx = TableContext::new(ORDERS, &t, &fps, CostParams::default());
    let three: Vec<AutoOnTable> = (0..3)
        .map(|i| {
            auto(
                &format!("sprig_auto_1000000{i}"),
                0.1 * GB,
                1000.0 * f64::from(i + 1),
            )
        })
        .collect();
    let s = select(
        example1_pool(&ctx),
        &ctx,
        &three,
        Budget {
            max_per_table: 1,
            max_storage_pct: 20.0,
            ..Budget::default()
        },
    );
    assert_eq!(
        s.retire,
        Some(("sprig_auto_10000000".to_owned(), DropReason::Budget))
    );
    assert!(s.chosen.is_empty());
}

#[test]
fn write_load_counts_updates_touching_the_index() {
    let fps = orders_writes();
    let w = WriteLoad::from_fps(ORDERS, &fps, 24.0);
    assert!(near(
        w.touching(["customer_id", "created_at"]),
        144_000.0,
        1e-9
    ));
    assert!(near(w.touching(["customer_id", "status"]), 360_000.0, 1e-9));
    assert_eq!(median(vec![]), 0.0);
    assert_eq!(median(vec![3.0, 1.0, 2.0]), 2.0);
    assert_eq!(median(vec![4.0, 1.0, 2.0, 3.0]), 2.5);
}

proptest! {
    /// More runs never lower the benefit; more writes never lower the
    /// write cost (chapter 13, "Testing").
    #[test]
    fn cost_is_monotone(runs_mul in 1u64..10, write_mul in 1u64..10) {
        let t = orders();
        let mut fps = example1_fps();
        let base = TableContext::new(ORDERS, &t, &fps, CostParams::default());
        let cand = Candidate {
            def: IndexDef::btree(ORDERS, &["customer_id", "status", "created_at"]),
            covering: false,
            serves: vec![(fps[0].id, Role::Primary)],
            evidence: Evidence::default(),
        };
        let e0 = Model.estimate(&cand, &base);
        fps[0].runs *= runs_mul;
        for w in fps.iter_mut().filter(|f| f.shape.writes.is_some()) {
            w.runs *= write_mul;
        }
        let more = TableContext::new(ORDERS, &t, &fps, CostParams::default());
        let e1 = Model.estimate(&cand, &more);
        prop_assert!(e1.benefit_ms_h + 1e-6 >= e0.benefit_ms_h);
        prop_assert!(e1.write_ms_h + 1e-6 >= e0.write_ms_h);
    }

    /// Selection never takes a table past max_per_table automatic indexes
    /// or max_storage_pct extra storage.
    #[test]
    fn selection_respects_budgets(
        existing in prop::collection::vec((1u32..40, 1u32..100_000), 0..7),
        max_per_table in 1u32..7,
        pct in 1u32..40,
        sizes in prop::collection::vec((1u32..60, 1u32..400_000), 1..10),
    ) {
        let mut t = orders();
        t.data_bytes = 10_000_000_000;
        t.index_bytes = 0;
        let fps = example1_fps();
        let ctx = TableContext::new(ORDERS, &t, &fps, CostParams::default());
        let auto: Vec<AutoOnTable> = existing
            .iter()
            .enumerate()
            .map(|(i, (gb10, v))| auto(&format!("sprig_auto_2{i:07}"), f64::from(*gb10) * 0.01 * GB, f64::from(*v)))
            .collect();
        let pool: Vec<Pooled> = sizes
            .iter()
            .enumerate()
            .map(|(i, (gb10, net))| {
                let cols: Vec<String> = (0..=i).map(|k| format!("c{k}")).collect();
                let refs: Vec<&str> = cols.iter().map(String::as_str).collect();
                let mut def = IndexDef::btree(ORDERS, &refs);
                def.parts.rotate_left(i % cols.len());
                Pooled {
                    candidate: Candidate {
                        def,
                        covering: false,
                        serves: vec![],
                        evidence: Evidence::default(),
                    },
                    estimate: Estimate {
                        benefit_ms_h: f64::from(*net),
                        write_ms_h: 0.0,
                        bytes: f64::from(*gb10) * 0.01 * GB,
                        build_seconds: 1.0,
                        per_fp: vec![FpEstimate {
                            fp: Fingerprint(i as u64),
                            role: Role::Primary,
                            per_hour: 1.0,
                            t_ms: f64::from(*net),
                            t_hat_ms: 0.0,
                            entries: 1.0,
                        }],
                        failed: vec![],
                    },
                    fast_only: false,
                }
            })
            .collect();
        // No floor: the property is about the percentage.
        let budget = Budget { max_per_table, max_storage_pct: f64::from(pct), min_storage_bytes: 0.0 };
        let s = select(pool, &ctx, &auto, budget);
        if !s.chosen.is_empty() {
            let replaced: BTreeSet<&str> = s.chosen.iter().filter_map(|c| c.replaces.as_deref()).collect();
            let kept: Vec<&AutoOnTable> = auto.iter().filter(|a| !replaced.contains(a.name.as_str())).collect();
            let count = kept.len() + s.chosen.len();
            let bytes: f64 = kept.iter().map(|a| a.bytes).sum::<f64>()
                + s.chosen.iter().map(|c| c.estimate.bytes).sum::<f64>();
            prop_assert!(count <= max_per_table as usize);
            prop_assert!(bytes <= f64::from(pct) / 100.0 * 1e10 + 1.0);
            prop_assert!(s.retire.is_none());
        }
        prop_assert!(s.chosen.iter().filter(|c| c.replaces.is_some()).count() <= 1);
    }
}

/// Decision E2: the storage budget is 20% of the table, never below the
/// floor (`auto_index.min_storage_budget_mb`, 64 MB).
#[test]
fn storage_budget_has_a_floor_for_small_tables() {
    let b = Budget::default();
    let mb = 1024.0 * 1024.0;
    // A 10 MB table: 2 MB by the percentage, 64 MB by the floor.
    assert!((b.cap_bytes(10.0 * mb) - 64.0 * mb).abs() < 1.0);
    // A 1 GB table: the percentage wins.
    assert!((b.cap_bytes(1024.0 * mb) - 0.2 * 1024.0 * mb).abs() < 1.0);
    // No floor: the strict percentage.
    let strict = Budget {
        min_storage_bytes: 0.0,
        ..b
    };
    assert!((strict.cap_bytes(10.0 * mb) - 2.0 * mb).abs() < 1.0);
}
