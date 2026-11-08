#![allow(
    clippy::unwrap_used,
    clippy::float_cmp,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

use std::collections::BTreeSet;

use sprig_common::ids::DcId;

use super::*;
use crate::model::{ClusterState, ShardView};
use crate::settings::{GB, LeastBusyWeights};
use crate::testutil::{cid, dc, fleet, load, put};

fn planner() -> Planner {
    Planner::new(BalancerSettings::default())
}

fn req(t: u64, l: LoadVector) -> PlaceRequest {
    PlaceRequest {
        tenant: TenantId(t),
        units: vec![ShardKey::new(t, 1, 1)],
        predicted: l,
        reason: PlaceReason::NewTenant,
    }
}

#[test]
fn least_busy_matches_the_chapter_example() {
    let p = planner();
    let a = Fit {
        full_after: 0.6,
        storage_after: 0.3,
        busy_after: 0.6,
        connections_after: 0.2,
        pressure: 0.0,
    };
    let b = Fit {
        full_after: 0.5,
        storage_after: 0.5,
        busy_after: 0.3,
        connections_after: 0.5,
        pressure: 0.0,
    };
    assert!((p.least_busy(&a) - 0.60).abs() < 1e-9);
    assert!((p.least_busy(&b) - 0.58).abs() < 1e-9);
    // Pressure counts as CPU.
    let c = Fit { pressure: 0.9, ..a };
    assert!(p.least_busy(&c) < p.least_busy(&a));
}

#[test]
fn place_clear_winner_or_least_busy_then_lower_id() {
    let mut v = fleet(3);
    v.clusters.get_mut(&cid(1)).unwrap().background = load(50.0, 0.0);
    v.clusters.get_mut(&cid(2)).unwrap().background = load(10.0, 0.0);
    v.clusters.get_mut(&cid(3)).unwrap().background = load(40.0, 0.0);
    let d = planner().place(&v, &req(7, load(1.0, 0.0)), 0).unwrap();
    assert_eq!(d.cluster, cid(2));
    assert_eq!(d.how, PlaceHow::ClearWin);
    assert_eq!(d.alternatives.len(), 3);
    assert_eq!(d.alternatives[0].cluster, cid(2));

    // Near-equal scores: least busy decides; ties to the lower id.
    let v = fleet(3);
    let d = planner().place(&v, &req(7, load(1.0, 0.0)), 0).unwrap();
    assert_eq!(d.how, PlaceHow::LeastBusy);
    assert_eq!(d.cluster, cid(1));

    // Busy time differs but storage decides `full`: least busy weighs CPU.
    let mut v = fleet(2);
    v.clusters.get_mut(&cid(1)).unwrap().background = load(30.0, 600.0);
    v.clusters.get_mut(&cid(2)).unwrap().background = load(30.0, 100.0);
    v.clusters.get_mut(&cid(1)).unwrap().cap.busy_ms_per_sec = 10_000.0;
    v.clusters.get_mut(&cid(2)).unwrap().cap.busy_ms_per_sec = 10_000.0;
    let d = planner().place(&v, &req(7, load(1.0, 0.0)), 0).unwrap();
    assert_eq!(d.cluster, cid(2));
}

#[test]
fn rpo_zero_tenants_prefer_zero_loss_clusters() {
    let mut v = fleet(2);
    v.clusters.get_mut(&cid(2)).unwrap().caps.zero_loss = true;
    v.clusters.get_mut(&cid(2)).unwrap().background = load(10.0, 0.0);
    let mut t = crate::model::TenantView::new(TenantId(7));
    t.rpo_zero = true;
    v.tenants.insert(TenantId(7), t);
    let d = planner().place(&v, &req(7, load(1.0, 0.0)), 0).unwrap();
    assert_eq!(d.cluster, cid(2));
}

#[test]
fn placement_unavailable_names_the_blocker() {
    let mut v = fleet(2);
    for c in v.clusters.values_mut() {
        c.limits.max_schemas = 1;
    }
    put(&mut v, 1, 1, 1, load(0.0, 0.0));
    put(&mut v, 2, 1, 2, load(0.0, 0.0));
    let e = planner().place(&v, &req(7, load(0.0, 0.0)), 0).unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::PlacementUnavailable));
    assert!(
        e.message.contains("max_shard_schemas_per_cluster"),
        "{}",
        e.message
    );
    // Never names other tenants.
    assert!(!e.message.contains("t1"));
}

#[test]
fn a_thousand_new_tenants_spread_evenly_and_weights_apply_live() {
    let mut v = fleet(4);
    let p = planner();
    for t in 0..1000u64 {
        let d = p.place(&v, &req(10 + t, load(0.05, 0.5)), 0).unwrap();
        let k = ShardKey::new(10 + t, 1, 1);
        v.add_shard(ShardView::new(k, d.cluster, load(0.05, 0.5)));
    }
    let spare: Vec<f64> = v
        .clusters
        .keys()
        .map(|c| 1.0 - v.full(c).unwrap().value)
        .collect();
    let max = spare.iter().copied().fold(f64::MIN, f64::max);
    let min = spare.iter().copied().fold(f64::MAX, f64::min);
    assert!(max - min <= 0.05, "spare values {spare:?}");

    // A live weight change alters the next choice: c1 has more spare
    // storage, c2 more spare connections.
    let mut v = fleet(2);
    v.clusters.get_mut(&cid(1)).unwrap().background = LoadVector {
        storage_bytes: 10.0 * GB,
        connections: 30.0,
        ..LoadVector::default()
    };
    v.clusters.get_mut(&cid(2)).unwrap().background = LoadVector {
        storage_bytes: 30.0 * GB,
        connections: 10.0,
        ..LoadVector::default()
    };
    let mut s = BalancerSettings {
        clear_win_margin: 0.5,
        ..BalancerSettings::default()
    };
    s.least_busy = LeastBusyWeights::parse("storage=1,cpu=0,connections=0");
    assert_eq!(
        Planner::new(s.clone())
            .place(&v, &req(7, LoadVector::default()), 0)
            .unwrap()
            .cluster,
        cid(1)
    );
    s.least_busy = LeastBusyWeights::parse("storage=0,cpu=0,connections=1");
    assert_eq!(
        Planner::new(s)
            .place(&v, &req(7, LoadVector::default()), 0)
            .unwrap()
            .cluster,
        cid(2)
    );
}

/// c1 at 85% storage with 17 shards of 5 GB, c2 and c3 empty.
fn hot_fleet() -> FleetView {
    let mut v = fleet(3);
    for i in 0..17 {
        put(&mut v, 100 + u64::from(i), i, 1, load(5.0, 10.0));
    }
    v
}

#[test]
fn hot_cluster_moves_only_after_the_sustain_window() {
    let v = hot_fleet();
    let p = planner();
    let r = p.rebalance(&v, 0);
    assert_eq!(r.trigger, Trigger::None);
    assert!(r.moves.is_empty());
    assert_eq!(r.warming.len(), 1);
    let r = p.rebalance(&v, 29 * 60);
    assert!(r.moves.is_empty());
    let r = p.rebalance(&v, 30 * 60);
    assert_eq!(r.trigger, Trigger::Hot);
    assert_eq!(r.moves.len(), 1);
    let m = &r.moves[0];
    assert_eq!(m.from, cid(1));
    assert_eq!(m.reason, MoveReason::ClusterHot);
    assert!((m.gain - 0.05).abs() < 1e-9);
    // A cool-down break resets the window.
    let p = planner();
    p.rebalance(&v, 0);
    let mut cool = v.clone();
    cool.shards.retain(|k, _| k.tenant.0 < 105);
    p.rebalance(&cool, 10 * 60);
    assert!(p.rebalance(&v, 30 * 60).moves.is_empty());
}

#[test]
fn guardrails_block_moves() {
    let v = hot_fleet();
    let now = 3600;
    // Cluster cooldown.
    let mut w = v.clone();
    w.clusters.get_mut(&cid(1)).unwrap().last_move_off = Some(now - 60);
    let p = planner();
    p.rebalance(&w, 0);
    let r = p.rebalance(&w, now);
    assert!(r.moves.is_empty());
    assert!(r.notes.iter().any(|n| n.guardrail == "cluster_cooldown"));
    // Shard cooldowns, pins and locks on every shard.
    let mut w = v.clone();
    for (i, s) in w.shards.values_mut().enumerate() {
        match i % 3 {
            0 => s.cooldown_until = now + 1,
            1 => s.pinned = true,
            _ => s.locked = true,
        }
    }
    let p = planner();
    p.rebalance(&w, 0);
    let r = p.rebalance(&w, now);
    assert!(r.moves.is_empty());
    for g in ["shard_cooldown", "pinned", "locked"] {
        assert!(
            r.notes.iter().any(|n| n.guardrail == g),
            "{g}: {:?}",
            r.notes
        );
    }
    assert_eq!(r.stuck, vec![cid(1)]);
    // DC concurrency.
    let mut w = v.clone();
    let s = ShardKey::new(100, 1, 0);
    for _ in 0..4 {
        w.active.push(crate::model::ActiveMove {
            shards: vec![s],
            from: cid(2),
            to: cid(3),
            load: LoadVector::default(),
        });
    }
    let p = planner();
    p.rebalance(&w, 0);
    let r = p.rebalance(&w, now);
    assert!(r.moves.is_empty());
    assert!(r.notes.iter().any(|n| n.guardrail == "concurrency"));
    // Tenant moves per day.
    let mut w = v;
    for t in w.tenants.values_mut() {
        t.moves_last_day = 2;
    }
    let p = planner();
    p.rebalance(&w, 0);
    assert!(p.rebalance(&w, now).moves.is_empty());
}

#[test]
fn moves_never_heat_the_target_or_invert_the_pair() {
    // c1 at 90%, c2 at 68%: any 5 GB move pushes c2 past target_pct (70).
    let mut v = fleet(2);
    for i in 0..18 {
        put(&mut v, 100 + u64::from(i), i, 1, load(5.0, 0.0));
    }
    v.clusters.get_mut(&cid(2)).unwrap().background = load(68.0, 0.0);
    let p = planner();
    p.rebalance(&v, 0);
    let r = p.rebalance(&v, 3600);
    assert!(r.moves.is_empty());
    assert!(r.notes.iter().any(|n| n.guardrail == "target_pct"));
    // One big shard on c1 (85 GB) and c2 empty: moving it would leave c2
    // fuller than c1 by more than spread_pct.
    let mut v = fleet(2);
    put(&mut v, 1, 1, 1, load(85.0, 0.0));
    let p = planner();
    let mut w = v.clone();
    w.clusters.get_mut(&cid(2)).unwrap().limits.target = 1.0;
    p.rebalance(&w, 0);
    let r = p.rebalance(&w, 3600);
    assert!(r.moves.is_empty());
    assert!(
        r.notes.iter().any(|n| n.guardrail == "would_invert"),
        "{:?}",
        r.notes
    );
}

#[test]
fn spread_triggers_moves_off_the_fullest() {
    // 60% vs 20%: no cluster is hot, but the spread is 40 points.
    let mut v = fleet(2);
    for i in 0..12 {
        put(&mut v, 100 + u64::from(i), i, 1, load(5.0, 0.0));
    }
    v.clusters.get_mut(&cid(2)).unwrap().background = load(20.0, 0.0);
    let p = planner();
    assert!(p.rebalance(&v, 0).moves.is_empty());
    let r = p.rebalance(&v, 1800);
    assert_eq!(r.trigger, Trigger::Spread);
    assert_eq!(r.moves.len(), 1);
    assert_eq!(r.moves[0].reason, MoveReason::Spread);
    assert_eq!(r.moves[0].to, cid(2));
}

#[test]
fn best_value_is_gain_per_gb_copied() {
    // A 1 GB shard with high busy time beats a 20 GB shard that gains the
    // same on the busy dimension.
    let mut v = fleet(2);
    put(&mut v, 1, 1, 1, load(1.0, 450.0));
    put(&mut v, 2, 2, 1, load(20.0, 450.0));
    v.clusters.get_mut(&cid(1)).unwrap().background = load(0.0, 0.0);
    let p = planner();
    p.rebalance(&v, 0);
    let r = p.rebalance(&v, 1800);
    assert_eq!(r.moves.len(), 1, "{:?}", r.notes);
    assert_eq!(r.moves[0].shards[0], ShardKey::new(1, 1, 1));
}

#[test]
fn big_shards_become_approvals() {
    let mut v = fleet(2);
    put(&mut v, 1, 1, 1, load(30.0, 0.0));
    put(&mut v, 2, 2, 1, load(55.0, 0.0));
    for t in v.tenants.values_mut() {
        t.knobs.auto_size_limit_bytes = 10.0 * GB;
    }
    let p = planner();
    p.rebalance(&v, 0);
    let r = p.rebalance(&v, 1800);
    assert!(r.moves.is_empty());
    assert_eq!(r.approvals.len(), 1);
    assert_eq!(r.approvals[0].to, cid(2));
}

#[test]
fn residency_and_drain_moves_skip_cooldowns() {
    let mut v = fleet(3);
    let a = put(&mut v, 1, 1, 1, load(1.0, 0.0));
    let b = put(&mut v, 2, 2, 2, load(1.0, 0.0));
    v.clusters.get_mut(&cid(3)).unwrap().dc = DcId::new("fra1").unwrap();
    v.shards.get_mut(&a).unwrap().cooldown_until = u64::MAX;
    v.tenants.get_mut(&TenantId(1)).unwrap().allowed_dcs = Some(BTreeSet::from([dc("fra1")]));
    v.clusters.get_mut(&cid(2)).unwrap().state = ClusterState::Draining;
    let r = planner().rebalance(&v, 0);
    let mut got: Vec<(ShardKey, ClusterId, MoveReason)> = r
        .moves
        .iter()
        .map(|m| (m.shards[0], m.to.clone(), m.reason))
        .collect();
    got.sort();
    assert_eq!(
        got,
        vec![
            (a, cid(3), MoveReason::ResidencyChange),
            (b, cid(1), MoveReason::Drain),
        ]
    );
}

#[test]
fn no_move_back_within_seven_days_unless_disabled() {
    let mut v = hot_fleet();
    for s in v.shards.values_mut() {
        s.history = vec![crate::model::Visit {
            cluster: cid(2),
            left_at: 0,
        }];
    }
    // c3 is fuller than c2 so c2 would be the natural target.
    v.clusters.get_mut(&cid(3)).unwrap().background = load(30.0, 0.0);
    let p = planner();
    p.rebalance(&v, 0);
    let r = p.rebalance(&v, 3600);
    assert_eq!(r.moves[0].to, cid(3));
    assert!(r.notes.iter().any(|n| n.guardrail == "move_back"));
    let mut p = planner();
    p.avoid_move_back = false;
    p.rebalance(&v, 0);
    assert_eq!(p.rebalance(&v, 3600).moves[0].to, cid(2));
}

#[test]
fn plans_are_identical_whatever_the_mode() {
    let v = hot_fleet();
    let run = |mode: BalancerMode| {
        let p = Planner::new(BalancerSettings {
            mode,
            ..BalancerSettings::default()
        });
        p.rebalance(&v, 0);
        p.rebalance(&v, 3600).moves
    };
    let auto = run(BalancerMode::Auto);
    assert!(!auto.is_empty());
    assert_eq!(auto, run(BalancerMode::Recommend));
    assert_eq!(auto, run(BalancerMode::DryRun));
}
