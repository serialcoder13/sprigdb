//! Integration tests against the development Compose topologies
//! (`deploy/compose`). Each test is skipped unless its variable is set to a
//! comma-separated list of node URLs, the writer first:
//!
//! ```sh
//! export SPRIG_TEST_MARIADB_SINGLE=mysql://root:sprig-dev-root@127.0.0.1:13301
//! export SPRIG_TEST_MARIADB_REPLICATION=mysql://root:sprig-dev-root@127.0.0.1:13311,mysql://root:sprig-dev-root@127.0.0.1:13312,mysql://root:sprig-dev-root@127.0.0.1:13313
//! export SPRIG_TEST_MARIADB_GALERA=mysql://root:sprig-dev-root@127.0.0.1:13321,mysql://root:sprig-dev-root@127.0.0.1:13322,mysql://root:sprig-dev-root@127.0.0.1:13323
//! cargo test -p sprig-cluster --test compose -- --nocapture --test-threads=1
//! ```
//!
//! `SPRIG_CAPTURE_FIXTURES=<dir>` makes `capture_fixtures` write each node's
//! full capture as JSON, for the parser fixtures.

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_cluster::health::NodeState;
use sprig_cluster::probe::{ProbeDepth, Prober};
use sprig_cluster::profile::{
    CapabilityPublisher, DeriveInput, Direction, FenceMethod, LossClass, RywMethod,
    ValidationMethod, derive, registration_gaps,
};
use sprig_cluster::record::{
    AdminState, Capacity, NodeSpec, ProvisionerKind, SecretRefs, TopologyKind,
};
use sprig_cluster::registry::{ClusterRegistry, Registration};
use sprig_cluster::{
    ClusterHealth, ClusterSettings, HealthTracker, HeartbeatWriter, NodeEndpoint, ProbeSession,
};
use sprig_common::ids::{DcId, NodeId};
use sprig_meta::{MemStore, Tier};

fn endpoints(var: &str) -> Option<Vec<NodeEndpoint>> {
    let urls = std::env::var(var).ok().filter(|v| !v.trim().is_empty())?;
    Some(
        urls.split(',')
            .enumerate()
            .map(|(i, u)| {
                NodeEndpoint::from_url(NodeId::new(format!("n{}", i + 1)).unwrap(), u.trim())
                    .unwrap()
            })
            .collect(),
    )
}

macro_rules! topology_or_skip {
    ($var:literal) => {
        match endpoints($var) {
            Some(eps) => eps,
            None => {
                eprintln!("skipped: {} is not set", $var);
                return;
            }
        }
    };
}

fn prober() -> Prober {
    Prober::new(Duration::from_secs(10))
}

/// Deep probe, derive, gates; prints the profile for the report.
async fn deep_profile(
    topology: TopologyKind,
    eps: &[NodeEndpoint],
) -> (
    Vec<sprig_cluster::NodeProbe>,
    sprig_cluster::CapabilityProfile,
) {
    // Root stands in for sprig_repl, so the binlog dump probe runs too.
    let probes = prober()
        .probe_cluster_with_repl(topology, eps, Some(eps), ProbeDepth::Deep)
        .await;
    for p in &probes {
        assert!(p.result.is_ok(), "{}: {:?}", p.node, p.result);
    }
    let active = probes
        .iter()
        .find_map(|p| p.active.as_ref())
        .expect("no active probe ran");
    let dump = active.binlog_dump.as_ref().expect("the dump probe ran");
    assert!(dump.passed(), "{dump:?}");
    let profile = derive(DeriveInput {
        topology,
        probes: &probes,
        declared: None,
        previous: None,
    });
    let gaps = registration_gaps(&probes, &profile, 0);
    assert!(gaps.blocking.is_empty(), "{:?}", gaps.blocking);
    println!(
        "=== {topology} profile ===\n{}\nactive: {}",
        serde_json::to_string_pretty(&profile).unwrap(),
        serde_json::to_string(&probes.iter().find_map(|p| p.active.clone())).unwrap()
    );
    (probes, profile)
}

fn assert_common(p: &sprig_cluster::CapabilityProfile) {
    let v = p.server.version.expect("version");
    assert!(v.major >= 11, "{v}");
    assert_eq!(p.server.lower_case_table_names, 0);
    assert!(p.binlog.log_bin && p.binlog.gtid_strict_mode && p.binlog.in_files);
    assert!(p.derived.mover_source && p.derived.feed_source && p.derived.mover_target);
    assert_eq!(p.derived.feed_retention_secs, 604_800);
    assert!(p.features.json_schema_valid && p.features.json_table);
    assert!(p.features.vcol_index);
    assert!(p.derived.enforce_json_schema);
    assert_eq!(
        p.derived.fence_methods,
        [FenceMethod::RevokeKill, FenceMethod::Rename]
    );
    assert!(p.privileges.sql_log_bin_settable == Some(true));
}

#[tokio::test]
async fn single_node_profile_health_and_heartbeat() {
    let eps = topology_or_skip!("SPRIG_TEST_MARIADB_SINGLE");
    let (probes, p) = deep_profile(TopologyKind::SingleNode, &eps).await;
    assert_common(&p);
    assert!(p.derived.xa, "XA round trip");
    assert!(!p.derived.zero_loss_failover);
    assert_eq!(p.derived.expected_loss, LossClass::NoReplica);
    assert_eq!(p.derived.ryw, RywMethod::WriterOnly);
    assert_eq!(p.derived.validation, None);
    assert!(p.derived.online_index_build);

    let mut tracker = HealthTracker::new();
    let report = tracker.observe(
        TopologyKind::SingleNode,
        &probes,
        &ClusterSettings::default(),
    );
    assert_eq!(report.health, ClusterHealth::Healthy, "{report:?}");

    // Heartbeats: two beats, then a light probe sees the second.
    let mut s = ProbeSession::connect(&eps[0], Duration::from_secs(5))
        .await
        .unwrap();
    let mut hb = HeartbeatWriter::new(eps[0].node.clone(), 0);
    hb.beat(&mut s).await.unwrap();
    let seq = hb.beat(&mut s).await.unwrap();
    s.close().await;
    let light = prober().probe(&eps[0], ProbeDepth::Light).await;
    let beats = light.facts().unwrap().heartbeats.clone().unwrap();
    let mine = beats.iter().find(|h| h.writer_node == "n1").unwrap();
    assert!(mine.seq >= seq);
    assert!(mine.age_ms < 5_000, "{mine:?}");
}

#[tokio::test]
async fn replication_profile_semi_sync_downgrade_and_lag() {
    let eps = topology_or_skip!("SPRIG_TEST_MARIADB_REPLICATION");
    let topo = TopologyKind::PrimaryReplica;
    let (probes, p) = deep_profile(topo, &eps).await;
    assert_common(&p);
    assert!(p.derived.xa);
    assert!(p.semi_sync.enabled && p.semi_sync.active && p.semi_sync.clients >= 1);
    assert!(p.derived.zero_loss_failover, "semi-sync AFTER_SYNC");
    assert_eq!(p.derived.expected_loss, LossClass::Zero);
    assert_eq!(p.derived.ryw, RywMethod::GtidWait);
    assert_eq!(p.derived.validation, Some(ValidationMethod::ReplicaBuild));

    // Heartbeat on the primary; replicas see it with a small lag.
    let mut s = ProbeSession::connect(&eps[0], Duration::from_secs(5))
        .await
        .unwrap();
    HeartbeatWriter::new(eps[0].node.clone(), 0)
        .beat(&mut s)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let light = prober().probe_cluster(topo, &eps, ProbeDepth::Light).await;
    let report = HealthTracker::new().observe(topo, &light, &ClusterSettings::default());
    assert_eq!(report.health, ClusterHealth::Healthy, "{report:?}");
    assert_eq!(report.writers, [eps[0].node.clone()]);
    for n in &report.nodes[1..] {
        assert_eq!(n.state, NodeState::Ok);
        assert!(n.lag_ms.is_some_and(|l| l < 5_000), "{n:?}");
    }

    // Semi-sync off downgrades within one light probe; turning it back on
    // is an upgrade held for the sustain period.
    let mut publisher = CapabilityPublisher::new(Some(p.clone()), Duration::from_secs(60));
    s.exec("SET GLOBAL rpl_semi_sync_master_enabled = OFF")
        .await
        .unwrap();
    let off = prober().probe_cluster(topo, &eps, ProbeDepth::Light).await;
    s.exec("SET GLOBAL rpl_semi_sync_master_enabled = ON")
        .await
        .unwrap();
    let t0 = Instant::now();
    let pubd = publisher
        .observe(
            derive(DeriveInput {
                topology: topo,
                probes: &off,
                declared: None,
                previous: Some(&p),
            }),
            t0,
        )
        .expect("downgrade published at once");
    assert!(!pubd.profile.derived.zero_loss_failover);
    assert!(pubd.changes.iter().any(|c| c.field == "derived.zero_loss_failover"
        && c.direction == Direction::Downgrade));

    // Wait for the replicas to reconnect as semi-sync clients.
    let mut on = Vec::new();
    for _ in 0..50 {
        on = prober().probe_cluster(topo, &eps, ProbeDepth::Light).await;
        if on[0].facts().unwrap().semi_sync.master_clients >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let again = derive(DeriveInput {
        topology: topo,
        probes: &on,
        declared: None,
        previous: Some(&p),
    });
    assert!(again.derived.zero_loss_failover, "semi-sync back on");
    // Held: nothing is published before the sustain period.
    let early = publisher.observe(again.clone(), t0 + Duration::from_secs(1));
    assert!(early.is_none(), "{:?}", early.map(|e| e.changes));
    let up = publisher
        .observe(again, t0 + Duration::from_secs(61))
        .expect("upgrade after sustain");
    assert!(up.profile.derived.zero_loss_failover);
    s.close().await;
    let _ = probes;
}

#[tokio::test]
async fn galera_profile_and_health() {
    let eps = topology_or_skip!("SPRIG_TEST_MARIADB_GALERA");
    let topo = TopologyKind::Galera;
    let (probes, p) = deep_profile(topo, &eps).await;
    assert_common(&p);
    let g = p.galera.clone().expect("galera info");
    assert_eq!(g.size, 3);
    assert_eq!(g.synced, 3);
    assert!(g.gtid_mode && g.shared_gtid_domain);
    assert!(p.derived.zero_loss_failover, "all nodes synced");
    assert_eq!(p.derived.ryw, RywMethod::WsrepSyncWait);
    assert_eq!(p.derived.validation, Some(ValidationMethod::GaleraRsu));
    assert!(!p.derived.online_index_build);
    assert_eq!(p.privileges.osu_method_settable, Some(true));
    let report = HealthTracker::new().observe(topo, &probes, &ClusterSettings::default());
    assert_eq!(report.health, ClusterHealth::Healthy, "{report:?}");
    assert_eq!(report.writers.len(), 3);
}

#[tokio::test]
async fn registration_flow_on_the_single_node() {
    let eps = topology_or_skip!("SPRIG_TEST_MARIADB_SINGLE");
    let dc = DcId::new("dc-a").unwrap();
    let reg = ClusterRegistry::new(
        Arc::new(MemStore::new(Tier::Global)),
        Arc::new(MemStore::new(Tier::Dc(dc))),
    )
    .unwrap();
    let mut events = reg.subscribe();
    let stored = reg
        .register(Registration {
            name: "dc-a-single".into(),
            topology: TopologyKind::SingleNode,
            provisioner: ProvisionerKind::External,
            nodes: vec![NodeSpec {
                node: eps[0].node.clone(),
                addr: eps[0].addr(),
                zone: None,
            }],
            secrets: SecretRefs {
                ctl: "env:SPRIG_CTL".into(),
                repl: "env:SPRIG_REPL".into(),
            },
            capacity: Capacity::default(),
            declared: None,
        })
        .await
        .unwrap();
    let id = stored.body().cluster_id.clone();
    assert_eq!(id.as_str(), "dc-a-c1");

    let probes = prober()
        .probe_cluster(TopologyKind::SingleNode, &eps, ProbeDepth::Deep)
        .await;
    let profile = derive(DeriveInput {
        topology: TopologyKind::SingleNode,
        probes: &probes,
        declared: None,
        previous: None,
    });
    let gaps = registration_gaps(&probes, &profile, 0);
    let state = if gaps.blocking.is_empty() {
        AdminState::Active
    } else {
        AdminState::Rejected
    };
    let mut publisher = CapabilityPublisher::new(None, Duration::from_secs(60));
    let first = publisher.observe(profile, Instant::now()).unwrap();
    reg.publish_profile(&id, &first).await.unwrap();
    reg.set_admin_state(&id, state, gaps.advisories)
        .await
        .unwrap();
    let health = HealthTracker::new()
        .observe(
            TopologyKind::SingleNode,
            &probes,
            &ClusterSettings::default(),
        )
        .health;
    let rec = reg
        .publish_health(&id, health, "2026-11-07T00:00:00Z".into())
        .await
        .unwrap();
    assert_eq!(rec.body().admin_state, AdminState::Active);
    assert_eq!(rec.body().profile_version, 1);
    assert_eq!(
        rec.body().health.as_ref().unwrap().state,
        ClusterHealth::Healthy
    );
    let mut seen = Vec::new();
    while let Ok(e) = events.try_recv() {
        seen.push(e);
    }
    assert_eq!(seen.len(), 4, "{seen:?}");
}

/// Writes fixtures; run with `--ignored` and `SPRIG_CAPTURE_FIXTURES`.
#[tokio::test]
#[ignore = "writes fixture files"]
async fn capture_fixtures() {
    let Ok(dir) = std::env::var("SPRIG_CAPTURE_FIXTURES") else {
        return;
    };
    let Ok(spec) = std::env::var("SPRIG_CAPTURE_NODES") else {
        return;
    };
    // name=url pairs, comma-separated.
    for pair in spec.split(',') {
        let (name, url) = pair.split_once('=').unwrap();
        let ep = NodeEndpoint::from_url(NodeId::new("n1").unwrap(), url).unwrap();
        let raw = prober().capture(&ep, ProbeDepth::Full).await.unwrap();
        let path = format!("{dir}/{name}.json");
        std::fs::write(&path, serde_json::to_string_pretty(&raw).unwrap()).unwrap();
        println!("wrote {path}");
    }
}
