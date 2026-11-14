use super::*;
use crate::probe::{HeartbeatReading, ReadOnly, ThreadState};
use crate::testutil::{facts_mut, failed, galera, replication, single};

fn observe(topology: TopologyKind, probes: &[NodeProbe]) -> HealthReport {
    HealthTracker::new().observe(topology, probes, &ClusterSettings::default())
}

fn state_of(r: &HealthReport, node: &str) -> NodeState {
    r.nodes
        .iter()
        .find(|n| n.node.as_str() == node)
        .map(|n| n.state)
        .unwrap()
}

#[test]
fn healthy_topologies() {
    let r = observe(TopologyKind::SingleNode, &single());
    assert_eq!(r.health, ClusterHealth::Healthy);
    assert_eq!(r.writers.len(), 1);

    let r = observe(TopologyKind::PrimaryReplica, &replication());
    assert_eq!(r.health, ClusterHealth::Healthy, "{r:?}");
    assert_eq!(r.writers, [NodeId::new("n1").unwrap()]);
    assert_eq!(r.nodes[1].role, ObservedRole::Replica);

    let r = observe(TopologyKind::Galera, &galera());
    assert_eq!(r.health, ClusterHealth::Healthy);
    assert_eq!(r.writers.len(), 3);
}

#[test]
fn unexpected_read_only_breaks_the_writer() {
    let mut probes = single();
    facts_mut(&mut probes[0]).server.read_only = Some(ReadOnly::NoLock);
    let r = observe(TopologyKind::SingleNode, &probes);
    assert_eq!(state_of(&r, "n1"), NodeState::Broken);
    assert_eq!(r.health, ClusterHealth::NoWriter);
}

#[test]
fn stopped_replication_breaks_the_replica() {
    let mut probes = replication();
    facts_mut(&mut probes[2]).channels[0].sql = ThreadState::No;
    let r = observe(TopologyKind::PrimaryReplica, &probes);
    assert_eq!(state_of(&r, "n3"), NodeState::Broken);
    assert_eq!(r.health, ClusterHealth::Degraded);
}

#[test]
fn lag_comes_from_heartbeats_first() {
    let mut probes = replication();
    {
        let f = facts_mut(&mut probes[1]);
        f.channels[0].seconds_behind = Some(0);
        f.heartbeats = Some(vec![HeartbeatReading {
            writer_node: "n1".into(),
            seq: 7,
            age_ms: 30_000,
        }]);
    }
    {
        let f = facts_mut(&mut probes[2]);
        f.heartbeats = None;
        f.channels[0].seconds_behind = Some(11);
    }
    let r = observe(TopologyKind::PrimaryReplica, &probes);
    assert_eq!(r.nodes[1].lag_ms, Some(30_000));
    assert_eq!(state_of(&r, "n2"), NodeState::Lagging);
    assert_eq!(r.nodes[2].lag_ms, Some(11_000));
    assert_eq!(state_of(&r, "n3"), NodeState::Lagging);
    assert_eq!(r.health, ClusterHealth::Degraded);

    // A heartbeat from a node that is not a writer is ignored.
    let mut probes = replication();
    let f = facts_mut(&mut probes[1]);
    f.channels[0].seconds_behind = Some(1);
    f.heartbeats = Some(vec![HeartbeatReading {
        writer_node: "old-primary".into(),
        seq: 1,
        age_ms: 999_999,
    }]);
    let r = observe(TopologyKind::PrimaryReplica, &probes);
    assert_eq!(r.nodes[1].lag_ms, Some(1000));
    assert_eq!(state_of(&r, "n2"), NodeState::Ok);
}

#[test]
fn a_partitioned_prober_says_unknown_never_no_writer() {
    let settings = ClusterSettings::default();
    let mut t = HealthTracker::new();
    assert_eq!(
        t.observe(TopologyKind::PrimaryReplica, &replication(), &settings)
            .health,
        ClusterHealth::Healthy
    );
    let mut cut = replication();
    cut[0] = failed("n1", None);
    for i in 1..=4 {
        let r = t.observe(TopologyKind::PrimaryReplica, &cut, &settings);
        assert_eq!(r.health, ClusterHealth::Unknown, "pass {i}");
        let expect = if i < settings.unhealthy_after_failures {
            NodeState::Ok
        } else {
            NodeState::Unreachable
        };
        assert_eq!(state_of(&r, "n1"), expect, "pass {i}");
        assert_eq!(r.nodes[0].failures, i);
    }
    // It answers again: the counter resets.
    let r = t.observe(TopologyKind::PrimaryReplica, &replication(), &settings);
    assert_eq!(r.health, ClusterHealth::Healthy);
    assert_eq!(r.nodes[0].failures, 0);

    let mut all = galera();
    for (i, p) in all.iter_mut().enumerate() {
        *p = failed(&format!("n{}", i + 1), None);
    }
    assert_eq!(
        observe(TopologyKind::Galera, &all).health,
        ClusterHealth::Unknown
    );
}

#[test]
fn failed_login_is_broken_at_once() {
    let r = observe(TopologyKind::SingleNode, &[failed("n1", Some(1045))]);
    assert_eq!(state_of(&r, "n1"), NodeState::Broken);
    assert_eq!(r.health, ClusterHealth::Unknown);
}

#[test]
fn two_writable_primaries_degrade() {
    let mut probes = replication();
    facts_mut(&mut probes[2]).channels.clear();
    let r = observe(TopologyKind::PrimaryReplica, &probes);
    assert_eq!(r.writers.len(), 2);
    assert_eq!(r.health, ClusterHealth::Degraded);
}

#[test]
fn galera_node_states() {
    let mut probes = galera();
    facts_mut(&mut probes[1])
        .galera
        .as_mut()
        .unwrap()
        .local_state = GaleraLocalState::DonorDesynced;
    facts_mut(&mut probes[2])
        .galera
        .as_mut()
        .unwrap()
        .local_state = GaleraLocalState::Joined;
    let r = observe(TopologyKind::Galera, &probes);
    assert_eq!(state_of(&r, "n2"), NodeState::Desynced);
    assert_eq!(state_of(&r, "n3"), NodeState::Joining);
    assert_eq!(r.writers, [NodeId::new("n1").unwrap()]);
    assert_eq!(r.health, ClusterHealth::Degraded);

    // Quorum lost everywhere: positively no writer.
    let mut probes = galera();
    for p in &mut probes {
        facts_mut(p).galera.as_mut().unwrap().cluster_status = "non-Primary".into();
    }
    let r = observe(TopologyKind::Galera, &probes);
    assert!(r.nodes.iter().all(|n| n.state == NodeState::Broken));
    assert_eq!(r.health, ClusterHealth::NoWriter);

    // Quorum lost on the nodes we see, one unreachable: can't tell.
    probes[0] = failed("n1", None);
    assert_eq!(
        observe(TopologyKind::Galera, &probes).health,
        ClusterHealth::Unknown
    );
}

#[test]
fn galera_without_a_state_uuid_is_degraded() {
    // A captured UUID that isn't zero: healthy.
    let mut probes = galera();
    for p in &mut probes {
        facts_mut(p).galera.as_mut().unwrap().state_uuid =
            Some("f01a6c83-8353-410e-bd55-31ce16ff1426".into());
    }
    let r = observe(TopologyKind::Galera, &probes);
    assert_eq!(r.health, ClusterHealth::Healthy);
    assert!(r.nodes.iter().all(|n| n.reason.is_none()));

    // All zeros: every node still writes, the cluster is degraded,
    // and each node says why.
    for p in &mut probes {
        facts_mut(p).galera.as_mut().unwrap().state_uuid =
            Some("00000000-0000-0000-0000-000000000000".into());
    }
    let r = observe(TopologyKind::Galera, &probes);
    assert_eq!(r.health, ClusterHealth::Degraded);
    assert_eq!(r.writers.len(), 3);
    assert!(r.nodes.iter().all(|n| n.state == NodeState::Ok));
    assert!(
        r.nodes
            .iter()
            .all(|n| n.reason.as_deref() == Some(LOST_IDENTITY))
    );

    // A node that is also donor keeps its own reason; the cluster stays degraded.
    facts_mut(&mut probes[1])
        .galera
        .as_mut()
        .unwrap()
        .local_state = GaleraLocalState::DonorDesynced;
    let r = observe(TopologyKind::Galera, &probes);
    assert_eq!(state_of(&r, "n2"), NodeState::Desynced);
    assert_eq!(r.health, ClusterHealth::Degraded);
}

#[test]
fn zero_uuid_spelling() {
    use crate::probe::zero_uuid;
    assert!(zero_uuid("00000000-0000-0000-0000-000000000000"));
    assert!(zero_uuid(" 00000000-0000-0000-0000-000000000000\n"));
    assert!(!zero_uuid("f01a6c83-8353-410e-bd55-31ce16ff1426"));
    assert!(!zero_uuid("00000000-0000-0000-0000-000000000001"));
    assert!(!zero_uuid(""));
    assert!(!zero_uuid("----"));
}

#[test]
fn display_matches_the_record() {
    for (h, s) in [
        (ClusterHealth::Healthy, "\"healthy\""),
        (ClusterHealth::NoWriter, "\"no_writer\""),
    ] {
        assert_eq!(serde_json::to_string(&h).unwrap(), s);
        assert_eq!(format!("\"{h}\""), s);
    }
}

#[test]
fn heartbeat_writer_counts() {
    let w = HeartbeatWriter::new(NodeId::new("n1").unwrap(), 41);
    assert_eq!(w.seq(), 41);
}
