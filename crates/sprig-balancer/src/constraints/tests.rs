#![allow(clippy::unwrap_used, clippy::float_cmp, clippy::many_single_char_names)]

use std::collections::BTreeSet;

use sprig_common::ids::GroupId;

use super::*;
use crate::model::{ClusterState, CoWrite};
use crate::testutil::{cid, dc, fleet, load, put};

fn unit(t: u64, l: LoadVector) -> Unit {
    Unit {
        tenant: TenantId(t),
        shards: vec![ShardKey::new(t, 1, 99)],
        load: l,
        from: None,
    }
}

fn blocker_of(f: &Feasibility, c: u32) -> Option<&Blocker> {
    f.blocked.iter().find(|(x, _)| *x == cid(c)).map(|(_, b)| b)
}

#[test]
fn every_hard_constraint_blocks_its_cluster() {
    let mut v = fleet(9);
    v.tenants
        .insert(TenantId(1), crate::model::TenantView::new(TenantId(1)));
    let t = v.tenants.get_mut(&TenantId(1)).unwrap();
    t.allowed_dcs = Some(BTreeSet::from([dc("mum1")]));
    t.needs = BTreeSet::from(["vector".to_owned()]);
    for c in v.clusters.values_mut() {
        c.caps.features.insert("vector".to_owned());
    }
    // 1. Residency: a replica outside the allowed DCs.
    v.clusters.get_mut(&cid(1)).unwrap().replica_dcs = vec![dc("fra1")];
    // 2. State.
    v.clusters.get_mut(&cid(2)).unwrap().state = ClusterState::Degraded;
    // 3. Capability.
    v.clusters.get_mut(&cid(3)).unwrap().caps.features.clear();
    // 4. Storage: 85 GB used, +10 GB passes 90%.
    v.clusters.get_mut(&cid(4)).unwrap().background = load(85.0, 0.0);
    // 6. Table cap.
    v.clusters.get_mut(&cid(5)).unwrap().limits.max_tables = 4;
    // 6. Table budget.
    v.clusters.get_mut(&cid(6)).unwrap().over_table_budget = true;
    // Stale probes.
    v.clusters.get_mut(&cid(7)).unwrap().probe_stale = true;
    // 7. Stability: recovered 10 minutes ago (rebalancing only).
    v.clusters.get_mut(&cid(8)).unwrap().recovered_at = Some(1000 - 600);

    let u = unit(1, load(10.0, 0.0));
    let f = feasible(&v, &u, Purpose::Place, 1000);
    assert_eq!(blocker_of(&f, 1), Some(&Blocker::Residency));
    assert!(matches!(blocker_of(&f, 2), Some(Blocker::State { .. })));
    assert!(matches!(
        blocker_of(&f, 3),
        Some(Blocker::Capability { .. })
    ));
    assert_eq!(blocker_of(&f, 4), Some(&Blocker::Storage));
    assert_eq!(blocker_of(&f, 5), Some(&Blocker::TableCap));
    assert_eq!(blocker_of(&f, 6), Some(&Blocker::TableBudget));
    assert_eq!(blocker_of(&f, 7), Some(&Blocker::StaleProbe));
    // Placement ignores the stability window; rebalancing doesn't.
    let ok: Vec<_> = f.ok.iter().map(|(c, _)| c.clone()).collect();
    assert_eq!(ok, vec![cid(8), cid(9)]);
    let mut moving = u.clone();
    moving.from = Some(cid(9));
    let f = feasible(&v, &moving, Purpose::Rebalance, 1000);
    assert!(matches!(blocker_of(&f, 8), Some(Blocker::Stability { .. })));
    assert_eq!(blocker_of(&f, 9), Some(&Blocker::Source));
    // Residency moves are exempt from the stability window.
    let f = feasible(&v, &moving, Purpose::Residency, 1000);
    assert!(f.ok.iter().any(|(c, _)| *c == cid(8)));
}

#[test]
fn moves_need_mover_flags_on_both_ends() {
    let mut v = fleet(3);
    let k = put(&mut v, 1, 1, 1, load(1.0, 0.0));
    v.clusters.get_mut(&cid(2)).unwrap().caps.mover_target = false;
    let u = unit_of(&v, k).unwrap();
    let f = feasible(&v, &u, Purpose::Rebalance, 0);
    assert!(
        matches!(blocker_of(&f, 2), Some(Blocker::Capability { missing }) if missing == "mover_target")
    );
    assert!(f.ok.iter().any(|(c, _)| *c == cid(3)));
    v.clusters.get_mut(&cid(1)).unwrap().caps.mover_source = false;
    let f = feasible(&v, &u, Purpose::Rebalance, 0);
    assert!(f.ok.is_empty());
    // A placement needs neither.
    let f = feasible(&v, &unit(1, load(1.0, 0.0)), Purpose::Place, 0);
    assert_eq!(f.ok.len(), 3);
}

#[test]
fn schema_cap_counts_schemas_and_tables() {
    // With the schema cap at 50, the 51st schema goes elsewhere.
    let mut v = fleet(2);
    for c in v.clusters.values_mut() {
        c.limits.max_schemas = 50;
    }
    for i in 0..50 {
        put(&mut v, 100 + i, 1, 1, load(0.0, 0.0));
    }
    let f = feasible(&v, &unit(500, load(0.0, 0.0)), Purpose::Place, 0);
    assert_eq!(blocker_of(&f, 1), Some(&Blocker::SchemaCap));
    assert_eq!(f.ok.len(), 1);
    // With the table cap at 250 and 5 tables per schema, the same.
    let mut v = fleet(2);
    for c in v.clusters.values_mut() {
        c.limits.max_tables = 250;
    }
    for i in 0..50 {
        put(&mut v, 100 + i, 1, 1, load(0.0, 0.0));
    }
    let f = feasible(&v, &unit(500, load(0.0, 0.0)), Purpose::Place, 0);
    assert_eq!(blocker_of(&f, 1), Some(&Blocker::TableCap));
    assert!(f.main_blocker().unwrap().is_schema_cap());
    // Operators may go past the caps.
    let mut u = unit(500, load(0.0, 0.0));
    u.from = Some(cid(2));
    assert!(
        feasible(&v, &u, Purpose::Operator, 0)
            .ok
            .iter()
            .any(|(c, _)| *c == cid(1))
    );
}

#[test]
fn excluded_target_is_skipped_until_the_time() {
    let mut v = fleet(2);
    let k = put(&mut v, 1, 1, 1, load(1.0, 0.0));
    v.shards.get_mut(&k).unwrap().excluded = vec![(cid(2), 500)];
    let u = unit_of(&v, k).unwrap();
    assert_eq!(
        blocker_of(&feasible(&v, &u, Purpose::Rebalance, 100), 2),
        Some(&Blocker::Excluded { until: 500 })
    );
    assert!(feasible(&v, &u, Purpose::Rebalance, 500).ok.len() == 1);
}

#[test]
fn co_writes_bind_groups_only_under_reject() {
    let mut v = fleet(2);
    let a = ShardKey::new(1, 1, 1);
    let b = ShardKey::new(1, 2, 2);
    let c = ShardKey::new(1, 3, 3);
    for k in [a, b, c] {
        v.add_shard(crate::model::ShardView::new(k, cid(1), load(1.0, 10.0)));
    }
    v.cowrites.push(CoWrite {
        tenant: TenantId(1),
        a: GroupId(1),
        b: GroupId(2),
        per_hour: 50.0,
    });
    v.cowrites.push(CoWrite {
        tenant: TenantId(1),
        a: GroupId(2),
        b: GroupId(3),
        per_hour: 0.5,
    });
    // Not under reject: a moves alone.
    assert_eq!(unit_of(&v, a).unwrap().shards, vec![a]);
    v.tenants.get_mut(&TenantId(1)).unwrap().non_xa_reject = true;
    let u = unit_of(&v, a).unwrap();
    assert_eq!(u.shards.len(), 2);
    assert!(u.shards.contains(&b));
    assert_eq!(u.load.storage_bytes, 2.0 * crate::settings::GB);
    // 0.5 per hour is under balancer.cowrite_min_per_hour (1).
    assert_eq!(
        bound_groups(&v, TenantId(1), GroupId(3)),
        BTreeSet::from([GroupId(3)])
    );
    // The split rate counts co-writes left on other clusters.
    let solo = Unit {
        tenant: TenantId(1),
        shards: vec![a],
        load: load(1.0, 0.0),
        from: Some(cid(1)),
    };
    assert_eq!(cowrite_split_rate(&v, &solo, &cid(2)), 50.0);
    assert_eq!(cowrite_split_rate(&v, &solo, &cid(1)), 0.0);
}
