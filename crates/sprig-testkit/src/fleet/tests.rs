#![allow(
    clippy::unwrap_used,
    clippy::float_cmp,
    clippy::cast_precision_loss,
    clippy::too_many_lines
)]

use super::*;

fn assert_even_and_calm(r: &FleetReport) {
    assert!(
        r.violations.is_empty(),
        "seed {}: {:?}",
        r.seed,
        r.violations
    );
    assert_eq!(r.reverted, 0, "seed {}", r.seed);
    assert_eq!(r.returned_shards, 0, "seed {}", r.seed);
    assert!(r.spread_before > 0.25, "seed {}: not skewed: {r:?}", r.seed);
    assert!(r.spread_after <= 0.30, "seed {}: {r:?}", r.seed);
    assert!(r.max_full_after <= 0.80, "seed {}: {r:?}", r.seed);
}

#[test]
fn a_skewed_fleet_evens_out_without_oscillating() {
    for seed in 1..=6 {
        let cfg = FleetConfig {
            days: 6,
            ..FleetConfig::skewed(seed)
        };
        let r = run(&cfg);
        assert_even_and_calm(&r);
        assert!(r.moves > 0);
    }
}

#[test]
fn pinned_tenants_stay_put() {
    let cfg = FleetConfig {
        days: 4,
        pin_every: 5,
        ..FleetConfig::skewed(21)
    };
    let r = run(&cfg);
    assert_eq!(r.pinned, 48);
    assert_eq!(r.pinned_moved, 0, "{r:?}");
    assert!(r.moves > 0);
    assert!(r.violations.is_empty());
}

#[test]
fn modes_plan_alike_and_only_auto_moves() {
    let mut cfg = FleetConfig {
        days: 2,
        new_tenants_per_day: 0,
        ..FleetConfig::skewed(7)
    };
    let auto = run(&cfg);
    assert!(auto.moves > 0);
    for mode in [BalancerMode::Recommend, BalancerMode::DryRun] {
        cfg.mode = mode;
        let r = run(&cfg);
        assert_eq!(r.moves, 0);
        assert!(r.decisions > 0);
        assert_eq!(r.spread_before, auto.spread_before);
    }
}

#[test]
fn an_outage_and_recovery_cause_no_move_back() {
    let cfg = FleetConfig {
        days: 6,
        outage: Some((3, 1, 2)),
        ..FleetConfig::skewed(11)
    };
    let r = run(&cfg);
    assert!(r.violations.is_empty(), "{:?}", r.violations);
    assert_eq!(r.reverted, 0);
}

/// The M0.6 evidence: many seeds, 30 days each. Slow in debug builds; run
/// with `cargo test -p sprig-testkit --release fleet_report -- --ignored
/// --nocapture`. `SPRIG_FLEET_SEEDS` (default 200) and
/// `SPRIG_FLEET_AVOID_MOVE_BACK=0` to turn off the move-back rule.
#[test]
#[ignore = "long: run explicitly in release"]
fn fleet_report() {
    let seeds: u64 = std::env::var("SPRIG_FLEET_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);
    let avoid = std::env::var("SPRIG_FLEET_AVOID_MOVE_BACK").map_or(true, |v| v != "0");
    let scenario = std::env::var("SPRIG_FLEET_SCENARIO").unwrap_or_default();
    let base = FleetConfig {
        avoid_move_back: avoid,
        ..match scenario.as_str() {
            "big" => FleetConfig::big_clusters_small_tenants(0),
            "huge" => FleetConfig::huge_tenants(0),
            _ => FleetConfig::skewed(0),
        }
    };
    println!(
        "scenario: {}",
        if scenario.is_empty() {
            "skewed"
        } else {
            &scenario
        }
    );
    let started = std::time::Instant::now();
    let rs = run_many(&base, 1..seeds + 1);
    let n = rs.len() as f64;
    let mean = |f: &dyn Fn(&FleetReport) -> f64| rs.iter().map(f).sum::<f64>() / n;
    let max = |f: &dyn Fn(&FleetReport) -> f64| rs.iter().map(f).fold(f64::MIN, f64::max);
    let min = |f: &dyn Fn(&FleetReport) -> f64| rs.iter().map(f).fold(f64::MAX, f64::min);
    println!(
        "fleet simulator: {seeds} seeds x {} days, {} clusters, {} tenants (+{}/day), noise ±{:.0}%, skew {:.0}% on 2 clusters, avoid_move_back {avoid}, {:.1} s",
        base.days,
        base.clusters,
        base.tenants,
        base.new_tenants_per_day,
        base.noise * 100.0,
        base.skew * 100.0,
        started.elapsed().as_secs_f64()
    );
    println!(
        "spread before: mean {:.3} min {:.3} max {:.3}",
        mean(&|r| r.spread_before),
        min(&|r| r.spread_before),
        max(&|r| r.spread_before)
    );
    println!(
        "max full before: mean {:.3} max {:.3}",
        mean(&|r| r.max_full_before),
        max(&|r| r.max_full_before)
    );
    println!(
        "spread after: mean {:.3} min {:.3} max {:.3}",
        mean(&|r| r.spread_after),
        min(&|r| r.spread_after),
        max(&|r| r.spread_after)
    );
    println!(
        "max full after: mean {:.3} max {:.3}",
        mean(&|r| r.max_full_after),
        max(&|r| r.max_full_after)
    );
    println!(
        "spread, worst over the last day: max {:.3}",
        max(&|r| r.spread_last_day_max)
    );
    println!(
        "hours to spread <= 0.25: mean {:.1} max {:.1} (never: {})",
        mean(&|r| r.hours_to_even.unwrap_or(f64::NAN)).max(0.0),
        max(&|r| r.hours_to_even.unwrap_or(0.0)),
        rs.iter().filter(|r| r.hours_to_even.is_none()).count()
    );
    println!(
        "moves: mean {:.1} min {:.0} max {:.0}; failed mean {:.1}; last week mean {:.2} max {:.0}",
        mean(&|r| r.moves as f64),
        min(&|r| r.moves as f64),
        max(&|r| r.moves as f64),
        mean(&|r| r.moves_failed as f64),
        mean(&|r| r.moves_last_week as f64),
        max(&|r| r.moves_last_week as f64)
    );
    println!(
        "most moves of one shard: max {:.0}",
        max(&|r| r.max_moves_per_shard as f64)
    );
    println!(
        "reverted (back within 7 days): total {}",
        rs.iter().map(|r| r.reverted).sum::<u64>()
    );
    println!(
        "shards that ever returned to a cluster: total {}",
        rs.iter().map(|r| r.returned_shards).sum::<u64>()
    );
    println!(
        "constraint violations: total {}",
        rs.iter().map(|r| r.violations.len()).sum::<usize>()
    );
    println!(
        "placements: total {}, refused {}",
        rs.iter().map(|r| r.placements).sum::<u64>(),
        rs.iter().map(|r| r.placement_failures).sum::<u64>()
    );
    let mut by: BTreeMap<String, u64> = BTreeMap::new();
    for r in &rs {
        for (k, v) in &r.moves_by_reason {
            *by.entry(k.clone()).or_default() += v;
        }
    }
    println!("moves by reason: {by:?}");
    for r in &rs {
        assert!(
            r.violations.is_empty(),
            "seed {}: {:?}",
            r.seed,
            r.violations
        );
    }
    if avoid {
        assert_eq!(
            rs.iter()
                .map(|r| r.reverted + r.returned_shards)
                .sum::<u64>(),
            0
        );
    }
}
