#![allow(clippy::unwrap_used, clippy::panic)]

use super::*;
use crate::topology::{KeyClass, ShardLoc, even_ranges};

/// A graph's group: four shards over two clusters, keyed by `part`.
fn group(held: Option<u64>) -> Arc<Group> {
    let shards = even_ranges(4)
        .into_iter()
        .enumerate()
        .map(|(i, lo)| ShardLoc {
            shard: u32::try_from(i + 1).unwrap(),
            schema: format!("t1_d2_g30_s{}", i + 1),
            cluster: if i < 2 { "c1" } else { "c2" }.to_owned(),
            lo,
            epoch: 3,
            held_until_ms: (i == 2).then_some(held).flatten(),
        })
        .collect();
    Arc::new(Group {
        id: 30,
        db: 2,
        version: 9,
        key_class: KeyClass::Int,
        spec: sprig_shard::KeySpec::new(vec![sprig_shard::KeyClass::Numeric]).unwrap(),
        shards,
    })
}

fn spec(g: &Arc<Group>, op: HopOp, targets: Targets) -> RouteSpec {
    RouteSpec {
        group: Arc::clone(g),
        graph: 77,
        op,
        targets,
    }
}

#[test]
fn a_hop_by_key_reads_only_the_frontiers_shards_each_with_its_own_ids() {
    let g = group(None);
    let ids: Vec<u64> = (1..=40).collect();
    let r = plan_route(&spec(
        &g,
        HopOp::Expand {
            out: true,
            types: vec![4],
        },
        Targets::ByKey(ids.clone()),
    ));
    let ts = r.targets();
    assert!(!ts.is_empty() && ts.len() <= 4);
    let mut seen = Vec::new();
    for t in ts {
        let i = g.shards.iter().position(|s| s.shard == t.shard).unwrap();
        assert_eq!(t.cluster, g.shards[i].cluster);
        assert_eq!(t.at.schema, g.shards[i].schema);
        assert_eq!(t.at.group, 30);
        assert!(
            t.sql
                .contains(&format!("`{}`.`__sprig_ge_77`", g.shards[i].schema))
        );
        assert!(t.sql.contains("LEFT JOIN"));
        assert!(t.sql.ends_with(" AND e.type IN (4)"));
        // Each id in the statement of the shard its hash routes to.
        let list = t.sql.split("e.src IN (").nth(1).unwrap();
        let list = list.split(')').next().unwrap();
        for id in list.split(',') {
            let id: u64 = id.parse().unwrap();
            assert_eq!(g.route(node_hash(&g, id).unwrap()), i);
            seen.push(id);
        }
    }
    seen.sort_unstable();
    assert_eq!(seen, ids);
    // One id: a single-shard pass.
    let one = plan_route(&spec(&g, HopOp::Nodes, Targets::ByKey(vec![5])));
    assert!(matches!(one.plan, Plan::Pass(_)));
    assert!(
        one.line()
            .starts_with("GraphRoute nodes: pass over 1 statement(s) on s")
    );
}

#[test]
fn incoming_without_mirrors_scatters_and_edges_by_hash_follow_the_hash() {
    let g = group(None);
    let r = plan_route(&spec(
        &g,
        HopOp::Expand {
            out: false,
            types: vec![],
        },
        Targets::All(vec![1, 2]),
    ));
    assert!(
        matches!(&r.plan, Plan::Read(p) if p.kind == ReadKind::Scatter && p.targets.len() == 4)
    );
    assert!(r.targets().iter().all(|t| t.sql.contains("e.dst IN (1,2)")));
    let h = g.shards[3].lo + 5;
    let e = plan_route(&spec(
        &g,
        HopOp::EdgesWithSrc,
        Targets::ByHash(vec![(h, 900)]),
    ));
    let [t] = e.targets() else { panic!() };
    assert_eq!(t.shard, 4);
    assert!(t.sql.contains("e.id IN (900)"));
    // Large lists come in batches on the same shard.
    let many: Vec<(u64, u64)> = (0..2500).map(|i| (h, i)).collect();
    let e = plan_route(&spec(&g, HopOp::EdgesWithSrc, Targets::ByHash(many)));
    assert_eq!(e.targets().len(), 3);
}

#[test]
fn scans_write_values_as_hex_and_skip_shards_that_match_nothing() {
    let g = group(None);
    let r = plan_route(&spec(
        &g,
        HopOp::Scan(ScanOp {
            label: Some(3),
            props: vec![(
                "name".into(),
                vec![Lit::Str("O'Brien \\ x".into()), Lit::Int(-2)],
            )],
            limit: Some(11),
            ..ScanOp::default()
        }),
        Targets::All(vec![]),
    ));
    assert_eq!(r.targets().len(), 4);
    let t = &r.targets()[0].sql;
    assert!(t.contains("JOIN `t1_d2_g30_s1`.`__sprig_gl_77` l ON l.id = n.id AND l.label = 3"));
    assert!(
        t.contains("JSON_VALUE(n.props, '$.\"name\"') IN (_utf8mb4 X'4F27427269656E205C2078', -2)")
    );
    assert!(t.ends_with(" LIMIT 11"));
    assert!(!t.contains("n.id IN"));
    // An index with no value can match nothing: no statement at all.
    let none = plan_route(&spec(
        &g,
        HopOp::Scan(ScanOp {
            label: Some(3),
            index: Some(("_sprig_i1_0".into(), vec![])),
            ..ScanOp::default()
        }),
        Targets::All(vec![]),
    ));
    assert!(none.targets().is_empty());
    assert!(matches!(none.plan, Plan::Read(_)));
}

#[test]
fn a_reach_runs_on_one_shard_inside_its_range_and_reports_holds() {
    let g = group(Some(1_234));
    let r = plan_route(&spec(
        &g,
        HopOp::Reach {
            seeds: vec![(1, 1), (1, 2)],
            types: vec![],
            out: false,
            hops: 3,
        },
        Targets::Shard(2),
    ));
    let [t] = r.targets() else { panic!() };
    assert_eq!(t.shard, 3);
    assert!(t.sql.contains("`t1_d2_g30_s3`.`__sprig_gm_77`"));
    assert!(t.sql.contains(&format!(
        "AND r.rk >= {} AND r.rk < {}",
        g.shards[2].lo, g.shards[3].lo
    )));
    assert!(
        t.sql
            .starts_with("SET STATEMENT max_recursive_iterations = 4 FOR")
    );
    assert_eq!(r.held_until_ms, Some(1_234));
    // A route that doesn't touch the held shard reports no hold.
    let other = plan_route(&spec(&g, HopOp::Edges, Targets::Shard(0)));
    assert_eq!(other.held_until_ms, None);
    let lines = explain_lines(&g, true);
    assert!(lines[0].contains("group 30 (map version 9), 4 shard(s)"));
    assert!(lines[3].ends_with("(held)"));
    assert!(
        lines
            .iter()
            .any(|l| l.contains("when the mirrors are fresh"))
    );
}
