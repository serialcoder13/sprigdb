use std::collections::HashMap;
use std::time::{Duration, Instant};

use sprig_common::ids::{ClusterId, DcId, TenantId};

use super::galera::{WriterSetTracker, choose_writer, rendezvous_score};
use super::*;
use crate::fence::FenceScope;
use crate::health::NodeAssessment;
use crate::probe::RowImage;
use crate::record::{
    AdminState, Capacity, GaleraWriters, ProvisionerKind, SecretRefs, WriterWeight,
};
use crate::testutil::{facts_mut, galera, node, replication, single};

fn record(kind: TopologyKind, nodes: &[&str]) -> ClusterRecord {
    ClusterRecord {
        cluster_id: ClusterId::new("dc-a-c1").unwrap(),
        cluster_number: 1,
        name: "c1".into(),
        dc_id: DcId::new("dc-a").unwrap(),
        topology: kind,
        provisioner: ProvisionerKind::External,
        admin_state: AdminState::Active,
        nodes: nodes
            .iter()
            .enumerate()
            .map(|(i, n)| NodeSpec {
                node: node(n),
                addr: format!("db{}.internal:3306", i + 1),
                zone: None,
            })
            .collect(),
        secrets: SecretRefs {
            ctl: "env:CTL".into(),
            repl: "env:REPL".into(),
        },
        capacity: Capacity::default(),
        galera: None,
        declared: None,
        profile_version: 0,
        profile: None,
        health: None,
        tuning: None,
        gaps: Vec::new(),
        node_uuids: std::collections::BTreeMap::new(),
    }
}

fn connector() -> Arc<dyn NodeConnector> {
    Arc::new(StaticConnector::default())
}

/// A driver fed with one full pass of `probes`.
fn fed(kind: TopologyKind, probes: &[NodeProbe]) -> Arc<dyn TopologyDriver> {
    let names: Vec<String> = probes.iter().map(|p| p.node.to_string()).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    let d = for_topology(kind, connector(), ClusterSettings::default());
    d.observe(&record(kind, &names));
    let p = d.derive(probes, None);
    d.health(probes, &p);
    d
}

fn ww(n: &str, weight: f64) -> WriterWeight {
    WriterWeight {
        node: node(n),
        weight,
    }
}

#[test]
fn single_node_driver() {
    let d = fed(TopologyKind::SingleNode, &single());
    let w = d.writer(ShardId(17)).unwrap();
    assert_eq!(w.node.as_str(), "n1");
    assert_eq!(w.addr, "db1.internal:3306");
    let r = d.readers(ShardId(17));
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].ryw, RywMethod::WriterOnly);
    let src = d.binlog_source(SourcePref::Auto).unwrap();
    assert_eq!(src.node.as_str(), "n1");
    assert_eq!(src.feed_level, FeedLevel::Full);
    assert!(d.binlog_source(SourcePref::Replica).is_err());
    assert_eq!(d.validation_node(), None);
    assert!(d.failover().is_none());
}

#[test]
fn primary_replica_driver() {
    let d = fed(TopologyKind::PrimaryReplica, &replication());
    assert_eq!(d.writer(ShardId(1)).unwrap().node.as_str(), "n1");
    let r = d.readers(ShardId(1));
    let names: Vec<&str> = r.iter().map(|r| r.endpoint.node.as_str()).collect();
    assert_eq!(names.len(), 2);
    assert!(names.contains(&"n2") && names.contains(&"n3"));
    assert!(r.iter().all(|r| r.ryw == RywMethod::GtidWait));
    // Semi-sync AFTER_SYNC: zero loss, so Auto reads the writer.
    assert_eq!(
        d.binlog_source(SourcePref::Auto).unwrap().node.as_str(),
        "n1"
    );
    let rep = d.binlog_source(SourcePref::Replica).unwrap();
    assert!(rep.node.as_str() != "n1");
    let v = d.validation_node().unwrap();
    assert_eq!(v.node.as_str(), "n3");
    assert_eq!(v.method, ValidationMethod::ReplicaBuild);
}

#[test]
fn replicas_without_log_slave_updates_are_not_sources() {
    let mut probes = replication();
    for p in &mut probes[1..] {
        facts_mut(p).binlog.log_slave_updates = false;
    }
    let d = fed(TopologyKind::PrimaryReplica, &probes);
    assert!(d.binlog_source(SourcePref::Replica).is_err());
    assert_eq!(
        d.binlog_source(SourcePref::Auto).unwrap().node.as_str(),
        "n1"
    );
}

#[test]
fn minimal_row_image_everywhere_means_no_source() {
    let mut probes = replication();
    for p in &mut probes {
        facts_mut(p).binlog.row_image = Some(RowImage::Minimal);
    }
    let d = fed(TopologyKind::PrimaryReplica, &probes);
    let e = d.binlog_source(SourcePref::Auto).unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ClusterCapabilityRequired));
    let p = d.derive(&probes, None);
    assert!(!p.derived.feed_source && !p.derived.mover_source);
    assert_eq!(p.derived.feed_level, FeedLevel::Unsupported);
}

#[test]
fn no_writer_is_39100() {
    let mut probes = single();
    facts_mut(&mut probes[0]).server.read_only = Some(crate::probe::ReadOnly::On);
    let d = fed(TopologyKind::SingleNode, &probes);
    let e = d.writer(ShardId(1)).unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ClusterUnavailable));
    assert!(d.readers(ShardId(1)).is_empty());
}

#[test]
fn an_unreachable_writer_is_kept_not_dropped() {
    let d = fed(TopologyKind::PrimaryReplica, &replication());
    // Every probe fails: health is unknown; gateways keep the writer.
    let lost: Vec<NodeProbe> = ["n1", "n2", "n3"]
        .iter()
        .map(|n| crate::testutil::failed(n, None))
        .collect();
    let h = d.health(&lost, &CapabilityProfile::default());
    assert_eq!(h, ClusterHealth::Unknown);
    assert_eq!(d.writer(ShardId(1)).unwrap().node.as_str(), "n1");
}

#[test]
fn two_writable_primaries_keep_the_one_replicas_follow() {
    let mut probes = replication();
    // n3 stops replicating and becomes writable: a second primary.
    {
        let f = facts_mut(&mut probes[2]);
        f.channels.clear();
        f.server.read_only = Some(crate::probe::ReadOnly::Off);
        f.server.server_id = 999;
    }
    let writers = [node("n3"), node("n1")];
    assert_eq!(
        basic::choose_writer_for_tests(None, &writers, &probes),
        Some(node("n1"))
    );
    // The current writer stays while it is writable.
    assert_eq!(
        basic::choose_writer_for_tests(Some(&node("n3")), &writers, &probes),
        Some(node("n3"))
    );
    let d = BasicDriver::new(
        TopologyKind::PrimaryReplica,
        connector(),
        ClusterSettings::default(),
    );
    d.observe(&record(TopologyKind::PrimaryReplica, &["n1", "n2", "n3"]));
    let p = d.derive(&probes, None);
    assert_eq!(d.health(&probes, &p), ClusterHealth::Degraded);
    assert_eq!(d.writer(ShardId(1)).unwrap().node.as_str(), "n1");
}

#[test]
fn external_clusters_discover_declared_endpoints() {
    let mut rec = record(TopologyKind::External, &["n1"]);
    rec.declared = Some(Declared {
        writer_endpoint: Some("db1.internal:3306".into()), // already n1
        reader_endpoints: vec!["ro.internal:3306".into(), "ro2.internal:3306".into()],
        ..Declared::default()
    });
    let nodes = discover_nodes(&rec);
    let names: Vec<&str> = nodes.iter().map(|n| n.node.as_str()).collect();
    assert_eq!(names, ["n1", "reader-1", "reader-2"]);
    assert_eq!(nodes[2].addr, "ro2.internal:3306");
    // Other topologies list only their nodes.
    rec.topology = TopologyKind::PrimaryReplica;
    assert_eq!(discover_nodes(&rec).len(), 1);
}

#[test]
fn external_contradiction_degrades() {
    let mut rec = record(TopologyKind::External, &["n1", "n2"]);
    rec.declared = Some(Declared {
        // Declared writer is actually the replica n2.
        writer_endpoint: Some("db2.internal:3306".into()),
        ..Declared::default()
    });
    let probes = replication()[..2].to_vec();
    let d = BasicDriver::new(
        TopologyKind::External,
        connector(),
        ClusterSettings::default(),
    );
    d.observe(&rec);
    let p = d.derive(&probes, rec.declared.as_ref());
    assert_eq!(d.health(&probes, &p), ClusterHealth::Degraded);
    rec.declared = Some(Declared {
        writer_endpoint: Some("db1.internal:3306".into()),
        ..Declared::default()
    });
    d.observe(&rec);
    assert_eq!(d.health(&probes, &p), ClusterHealth::Healthy);
}

#[test]
fn ddl_methods() {
    let p = crate::profile::derive(DeriveInput {
        topology: TopologyKind::SingleNode,
        probes: &single(),
        declared: None,
        previous: None,
    });
    let plain = TableTraits::default();
    assert_eq!(
        ddl_method_for(false, Some(&p), DdlKind::AddIndex, &plain),
        DdlMethod::Online
    );
    // The fixture says spatial indexes don't build online.
    let spatial = TableTraits {
        spatial: true,
        ..plain
    };
    assert!(!p.features.online_spatial_index);
    assert_eq!(
        ddl_method_for(false, Some(&p), DdlKind::AddIndex, &spatial),
        DdlMethod::CopySwap
    );
    let ft = TableTraits {
        fulltext: true,
        ..plain
    };
    assert_eq!(
        ddl_method_for(false, Some(&p), DdlKind::AddIndex, &ft),
        DdlMethod::CopySwap
    );
    let vcol = TableTraits {
        virtual_column: true,
        ..plain
    };
    assert_eq!(
        ddl_method_for(false, Some(&p), DdlKind::AddIndex, &vcol),
        DdlMethod::Online
    );
    assert_eq!(
        ddl_method_for(false, Some(&p), DdlKind::Rebuild, &plain),
        DdlMethod::CopySwap
    );
    // No profile: nothing is assumed online.
    assert_eq!(
        ddl_method_for(false, None, DdlKind::AddIndex, &plain),
        DdlMethod::CopySwap
    );
    // Galera: metadata-only changes in TOI, the rest by copy-and-swap.
    let d = fed(TopologyKind::Galera, &galera());
    assert_eq!(d.ddl_method(DdlKind::DropIndex, &plain), DdlMethod::Toi);
    assert_eq!(
        d.ddl_method(DdlKind::ToggleIgnoredIndex, &plain),
        DdlMethod::Toi
    );
    assert_eq!(d.ddl_method(DdlKind::AddIndex, &plain), DdlMethod::CopySwap);
}

#[test]
fn fence_method_falls_back_to_rename() {
    let mut probes = single();
    if let Some(p) = facts_mut(&mut probes[0]).privileges.as_mut() {
        p.connection_admin = false;
    }
    let d = BasicDriver::new(
        TopologyKind::SingleNode,
        connector(),
        ClusterSettings::default(),
    );
    d.observe(&record(TopologyKind::SingleNode, &["n1"]));
    let p = d.derive(&probes, None);
    assert_eq!(p.derived.fence_methods, [FenceMethod::Rename]);
    d.health(&probes, &p);
    let req = FenceRequest {
        tenant: TenantId(42),
        schemas: vec!["t42_d1_g3_s17".into()],
        tables: None,
        scope: FenceScope::Writes,
        method: FenceMethod::RevokeKill,
        deadline: Instant::now() + Duration::from_secs(5),
    };
    assert_eq!(d.core.fence_method(&req).unwrap(), FenceMethod::Rename);
    assert_eq!(fence_shard_id(&req), ShardId(17));
}

#[tokio::test]
async fn cluster_self_fence_waits_for_r2() {
    let d = fed(TopologyKind::SingleNode, &single());
    let e = d.fence_cluster(FenceReason::LeaseLost).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ClusterCapabilityRequired));
}

// --- Galera writer choice -------------------------------------------------

#[test]
#[allow(clippy::float_cmp)] // the same inputs must give the same bits
fn rendezvous_is_deterministic_and_weighted() {
    let a = rendezvous_score(ShardId(1), &node("n1"), 1.0);
    assert_eq!(a, rendezvous_score(ShardId(1), &node("n1"), 1.0));
    assert!(a > 0.0);
    assert!(rendezvous_score(ShardId(1), &node("n1"), 2.0) > a);
    assert!(rendezvous_score(ShardId(1), &node("n1"), 0.0) == 0.0);
    // Weights move load proportionally.
    let set = [ww("n1", 1.0), ww("n2", 1.0), ww("n3", 2.0)];
    let mut counts: HashMap<String, u32> = HashMap::new();
    for s in 0..20_000 {
        *counts
            .entry(choose_writer(ShardId(s), &set).unwrap().node.to_string())
            .or_default() += 1;
    }
    let n3 = f64::from(counts["n3"]) / 20_000.0;
    assert!((0.45..0.55).contains(&n3), "{counts:?}");
    let n1 = f64::from(counts["n1"]) / 20_000.0;
    assert!((0.20..0.30).contains(&n1), "{counts:?}");
    // A zero weight never wins.
    let set = [ww("n1", 0.0), ww("n2", 1.0)];
    assert!((0..1000).all(|s| choose_writer(ShardId(s), &set).unwrap().node.as_str() == "n2"));
    assert!(choose_writer(ShardId(1), &[]).is_none());
}

#[test]
fn a_leaving_node_moves_only_its_shards() {
    let full = [ww("n1", 1.0), ww("n2", 1.0), ww("n3", 1.0)];
    let without_n2 = [ww("n1", 1.0), ww("n3", 1.0)];
    let mut moved = 0;
    for s in 0..5_000 {
        let before = &choose_writer(ShardId(s), &full).unwrap().node;
        let after = &choose_writer(ShardId(s), &without_n2).unwrap().node;
        if before.as_str() == "n2" {
            moved += 1;
            assert!(after.as_str() != "n2");
        } else {
            assert_eq!(before, after, "shard {s} moved off a staying node");
        }
    }
    assert!(moved > 1_000, "{moved}");
    // Order doesn't matter.
    let shuffled = [ww("n3", 1.0), ww("n1", 1.0), ww("n2", 1.0)];
    for s in 0..1_000 {
        assert_eq!(
            choose_writer(ShardId(s), &full).unwrap().node,
            choose_writer(ShardId(s), &shuffled).unwrap().node
        );
    }
}

fn report(health: ClusterHealth, ok: &[&str], all: &[&str]) -> HealthReport {
    HealthReport {
        health,
        nodes: all
            .iter()
            .map(|n| NodeAssessment {
                node: node(n),
                state: if ok.contains(n) {
                    NodeState::Ok
                } else {
                    NodeState::Unreachable
                },
                role: ObservedRole::Galera,
                lag_ms: None,
                failures: 0,
                reason: None,
            })
            .collect(),
        writers: ok.iter().map(|n| node(n)).collect(),
    }
}

#[test]
fn writer_set_leaves_at_once_and_rejoins_after_the_hold() {
    let all = ["n1", "n2", "n3"];
    let rejoin = Duration::from_secs(120);
    let t0 = Instant::now();
    let mut t = WriterSetTracker::new();
    let first = t
        .observe(
            None,
            &report(ClusterHealth::Healthy, &all, &all),
            t0,
            rejoin,
        )
        .expect("a new cluster gets every usable node");
    assert_eq!(first.writer_epoch, 1);
    assert_eq!(first.writers.len(), 3);
    assert!(
        t.observe(
            Some(&first),
            &report(ClusterHealth::Healthy, &all, &all),
            t0,
            rejoin
        )
        .is_none()
    );

    // n2 fails: it leaves at once.
    let r = report(ClusterHealth::Degraded, &["n1", "n3"], &all);
    let second = t
        .observe(Some(&first), &r, t0 + Duration::from_secs(5), rejoin)
        .unwrap();
    assert_eq!(second.writer_epoch, 2);
    let names: Vec<&str> = second.writers.iter().map(|w| w.node.as_str()).collect();
    assert_eq!(names, ["n1", "n3"]);

    // n2 returns: held until it has been usable for the rejoin period.
    let back = report(ClusterHealth::Healthy, &all, &all);
    let t1 = t0 + Duration::from_secs(10);
    assert!(t.observe(Some(&second), &back, t1, rejoin).is_none());
    assert!(
        t.observe(Some(&second), &back, t1 + Duration::from_secs(119), rejoin)
            .is_none()
    );
    // A flap restarts the hold.
    assert!(
        t.observe(Some(&second), &r, t1 + Duration::from_secs(119), rejoin)
            .is_none()
    );
    let t2 = t1 + Duration::from_secs(130);
    assert!(t.observe(Some(&second), &back, t2, rejoin).is_none());
    let third = t
        .observe(Some(&second), &back, t2 + rejoin, rejoin)
        .unwrap();
    assert_eq!(third.writer_epoch, 3);
    assert_eq!(third.writers.len(), 3);

    // A partitioned controller changes nothing.
    let unknown = report(ClusterHealth::Unknown, &[], &all);
    assert!(
        t.observe(Some(&third), &unknown, t2 + rejoin * 2, rejoin)
            .is_none()
    );
}

#[test]
fn writer_set_keeps_published_weights() {
    let all = ["n1", "n2"];
    let current = GaleraWriters {
        writer_epoch: 7,
        writers: vec![ww("n1", 3.0), ww("n2", 0.5)],
    };
    let mut t = WriterSetTracker::new();
    let r = report(ClusterHealth::Degraded, &["n1"], &all);
    let next = t
        .observe(Some(&current), &r, Instant::now(), Duration::ZERO)
        .unwrap();
    assert_eq!(next.writers, [ww("n1", 3.0)]);
    let back = report(ClusterHealth::Healthy, &all, &all);
    let again = t
        .observe(Some(&next), &back, Instant::now(), Duration::ZERO)
        .unwrap();
    assert_eq!(again.writers, [ww("n1", 3.0), ww("n2", 0.5)]);
}

#[test]
fn galera_driver() {
    let probes = galera();
    let d = GaleraDriver::new(connector(), ClusterSettings::default());
    let mut rec = record(TopologyKind::Galera, &["n1", "n2", "n3"]);
    d.observe(&rec);
    let p = d.derive(&probes, None);
    assert_eq!(d.health(&probes, &p), ClusterHealth::Healthy);

    // Before a set is published, every usable node is a writer.
    let mut seen = std::collections::HashSet::new();
    for s in 0..300 {
        seen.insert(d.writer(ShardId(s)).unwrap().node);
    }
    assert_eq!(seen.len(), 3);

    let set = d.plan_writer_set(Instant::now()).expect("first set");
    assert_eq!(set.writer_epoch, 1);
    rec.galera = Some(set.clone());
    d.observe(&rec);
    assert!(d.plan_writer_set(Instant::now()).is_none());
    for s in 0..50 {
        assert_eq!(
            d.writer(ShardId(s)).unwrap().node,
            choose_writer(ShardId(s), &set.writers).unwrap().node
        );
    }
    let readers = d.readers(ShardId(1));
    assert_eq!(readers.len(), 3);
    assert!(readers.iter().all(|r| r.ryw == RywMethod::WsrepSyncWait));
    let src = d.binlog_source(SourcePref::Auto).unwrap();
    assert_eq!(src.node.as_str(), "n1");
    assert!(src.switchable, "wsrep_gtid_mode with a shared domain");
    let v = d.validation_node().unwrap();
    assert_eq!(v.method, ValidationMethod::GaleraRsu);
    assert_eq!(v.node.as_str(), "n3");

    // A node this gateway sees fail is skipped even before a new set.
    let mut down = probes.clone();
    down[1] = crate::testutil::failed("n2", Some(1045));
    d.health(&down, &p);
    for s in 0..300 {
        assert!(d.writer(ShardId(s)).unwrap().node.as_str() != "n2");
    }
    assert_eq!(d.readers(ShardId(1)).len(), 2);
    // Two synced nodes are too few for an RSU build.
    assert!(d.validation_node().is_none());
}

/// The dev clusters' bug: a deep probe found XA working, the publisher
/// held the upgrade for its sustain period, and the light passes in
/// between carried the published `xa = false` forward, so the upgrade
/// never published. The driver now carries the last deep probe's results.
#[test]
fn deep_only_upgrades_sustain_across_light_passes() {
    use crate::profile::CapabilityPublisher;
    let kind = TopologyKind::SingleNode;
    let d = for_topology(kind, connector(), ClusterSettings::default());
    let mut rec = record(kind, &["n1"]);
    // Published before: XA didn't work then.
    let mut old = crate::profile::derive(DeriveInput {
        topology: kind,
        probes: &single(),
        declared: None,
        previous: None,
    });
    old.derived.xa = false;
    old.version = 4;
    rec.profile = Some(old.clone());
    d.observe(&rec);
    let sustain = Duration::from_secs(60);
    let mut pubr = CapabilityPublisher::new(Some(old), sustain);
    let t0 = Instant::now();
    // A deep pass: XA works, the upgrade is held.
    let deep = single();
    let c = d.derive(&deep, None);
    assert!(c.derived.xa);
    assert!(pubr.observe(c, t0).is_none());
    assert!(pubr.pending().contains(&"derived.xa"));
    // Light passes (no active probes) keep proposing it.
    let mut light = single();
    light[0].active = None;
    for s in [10, 30, 59] {
        let c = d.derive(&light, None);
        assert!(c.derived.xa, "carried from the deep pass");
        assert!(pubr.observe(c, t0 + Duration::from_secs(s)).is_none());
        assert!(pubr.pending().contains(&"derived.xa"));
    }
    let up = pubr
        .observe(d.derive(&light, None), t0 + sustain)
        .expect("published after the sustain period");
    assert!(up.profile.derived.xa);
    // A later deep pass that finds XA broken downgrades at once.
    let mut broken = single();
    if let Some(a) = broken[0].active.as_mut() {
        a.xa = false;
    }
    let down = pubr
        .observe(
            d.derive(&broken, None),
            t0 + sustain + Duration::from_secs(1),
        )
        .expect("downgrades publish at once");
    assert!(!down.profile.derived.xa);
    assert!(
        !d.derive(&light, None).derived.xa,
        "the new deep facts carry"
    );
}

#[test]
fn primary_replica_carries_xa_and_galera_never_has_it() {
    let d = fed(TopologyKind::PrimaryReplica, &replication());
    let mut light = replication();
    for p in &mut light {
        p.active = None;
    }
    assert!(d.derive(&light, None).derived.xa);
    // Even an active probe that claims XA on Galera doesn't make it XA.
    let mut g = galera();
    if let Some(a) = g.iter_mut().find_map(|p| p.active.as_mut()) {
        a.xa = true;
    } else {
        g[0].active = Some(crate::testutil::all_passed());
    }
    let d = fed(TopologyKind::Galera, &g);
    assert!(!d.derive(&g, None).derived.xa);
}
