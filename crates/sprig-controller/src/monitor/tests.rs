//! The `registry` loop over `MemStore`, with nodes that refuse connections:
//! scheduling, settings per cluster, health publishing, the probing grace,
//! leader guards. Profiles from real servers are in sprigctl's Compose
//! test.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use sprig_cluster::health::ClusterHealth;
use sprig_cluster::probe::ProbeDepth;
use sprig_cluster::record::{
    AdminState, Capacity, NodeSpec, ProvisionerKind, SecretRefs, TopologyKind,
};
use sprig_cluster::registry::{ClusterRegistry, Registration};
use sprig_common::SprigCode;
use sprig_common::ids::{ClusterId, DcId, NodeId};
use sprig_common::time::SystemClock;
use sprig_meta::{Cmp, MemStore, MetaStore, Op, Tier, Txn};
use sprig_settings::{LiveSettings, Registry, SettingsStores};

use super::*;
use crate::runtime::Source;

pub(crate) struct Fixture {
    pub(crate) dc: MemStore,
    pub(crate) global: MemStore,
    pub(crate) live: LiveSettings,
    pub(crate) monitor: Arc<ClusterMonitor>,
}

pub(crate) async fn fixture(dc: &str) -> Fixture {
    let dcs = MemStore::new(Tier::Dc(DcId::new(dc).unwrap()));
    let global = dcs.with_tier(Tier::Global);
    let live = LiveSettings::start(
        Registry::global().unwrap(),
        SettingsStores::new(Arc::new(global.clone()), Arc::new(dcs.clone())).unwrap(),
    )
    .unwrap();
    live.ready().await;
    let registry = ClusterRegistry::new(Arc::new(global.clone()), Arc::new(dcs.clone())).unwrap();
    let monitor = Arc::new(ClusterMonitor::new(
        registry,
        live.clone(),
        Arc::new(SystemClock),
    ));
    Fixture {
        dc: dcs,
        global,
        live,
        monitor,
    }
}

/// A registration whose nodes refuse connections (port 1).
pub(crate) fn unreachable(name: &str, topology: TopologyKind) -> Registration {
    Registration {
        name: name.into(),
        topology,
        provisioner: ProvisionerKind::External,
        nodes: vec![NodeSpec {
            node: NodeId::new("n1").unwrap(),
            addr: "127.0.0.1:1".into(),
            zone: None,
        }],
        secrets: SecretRefs {
            ctl: "env:SPRIG_TEST_MONITOR_CTL".into(),
            repl: "env:SPRIG_TEST_MONITOR_REPL".into(),
        },
        capacity: Capacity::default(),
        declared: None,
    }
}

/// Writes a setting record straight into the store, as the DC leader's
/// settings writer would.
pub(crate) async fn put_setting(store: &MemStore, rel: &str, json: &str) {
    store
        .txn(Txn {
            when: vec![],
            then: vec![Op::Put {
                key: store.tier().key(rel).unwrap(),
                value: json.as_bytes().to_vec(),
                lease: None,
            }],
            otherwise: vec![],
        })
        .await
        .unwrap();
}

/// Becomes the "leader": a leader key and the guard on it.
pub(crate) async fn lead(store: &MemStore) -> Vec<Cmp> {
    let key = store.tier().key("leader").unwrap();
    let r = store
        .txn(Txn {
            when: vec![],
            then: vec![Op::Put {
                key: key.clone(),
                value: b"{}".to_vec(),
                lease: None,
            }],
            otherwise: vec![],
        })
        .await
        .unwrap();
    vec![Cmp::CreateRev(key, r.revision)]
}

async fn wait_for(what: &str, f: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !f() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

#[tokio::test]
async fn an_unreachable_cluster_is_unknown_and_follows_live_settings() {
    let f = fixture("mon-a").await;
    let guard = lead(&f.dc).await;
    let reg = f.monitor.registry().clone();
    let s = reg
        .register(unreachable("u1", TopologyKind::SingleNode))
        .await
        .unwrap();
    let id = s.body().cluster_id.clone();
    reg.set_admin_state(&id, AdminState::Active, vec![])
        .await
        .unwrap();

    let r = f
        .monitor
        .pass(&id, None, guard.clone())
        .await
        .unwrap()
        .unwrap();
    // No profile yet: the first pass goes deep.
    assert_eq!(r.depth, Some(ProbeDepth::Deep));
    assert_eq!(r.next, Duration::from_secs(5));
    let rec = r.cluster.unwrap();
    assert_eq!(
        rec.body().health.as_ref().unwrap().state,
        ClusterHealth::Unknown
    );
    assert!(
        rec.body().profile.is_some(),
        "an empty profile is published"
    );
    assert!(f.monitor.knows(&id));
    assert!(f.monitor.last_report(&id).await.is_some());

    // A dc value, then a cluster value that wins for this cluster only.
    put_setting(
        &f.dc,
        "settings/cluster.health_interval_seconds",
        r#"{"v":1,"value":7,"set_by":"test"}"#,
    )
    .await;
    wait_for("the dc value", || {
        f.monitor.settings_for(Some(&id)).health_interval == Duration::from_secs(7)
    })
    .await;
    put_setting(
        &f.dc,
        &format!("cluster-settings/{id}/cluster.health_interval_seconds"),
        r#"{"v":1,"value":2,"set_by":"test"}"#,
    )
    .await;
    wait_for("the cluster value", || {
        f.monitor.settings_for(Some(&id)).health_interval == Duration::from_secs(2)
    })
    .await;
    assert_eq!(
        f.monitor.settings_for(None).health_interval,
        Duration::from_secs(7)
    );
    // Within the interval a scheduled pass only waits, now for at most 2 s.
    let r = f
        .monitor
        .pass(&id, None, guard.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.depth, None);
    assert!(r.next <= Duration::from_secs(2), "{:?}", r.next);
    let r = f
        .monitor
        .pass(&id, Some(ProbeDepth::Light), guard)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.next, Duration::from_secs(2));
    drop(f.live);
}

#[tokio::test]
async fn probing_records_get_a_grace_period_and_guards_hold() {
    let f = fixture("mon-b").await;
    let guard = lead(&f.dc).await;
    let reg = f.monitor.registry().clone();
    let s = reg
        .register(unreachable("p1", TopologyKind::PrimaryReplica))
        .await
        .unwrap();
    let id = s.body().cluster_id.clone();
    let r = f
        .monitor
        .pass(&id, None, guard.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.depth, None, "a registration in flight owns the record");
    assert!(r.next > Duration::from_secs(25));
    // Forced (sprigctl cluster probe): gates run and reject it.
    let r = f
        .monitor
        .pass(&id, Some(ProbeDepth::Full), guard.clone())
        .await
        .unwrap()
        .unwrap();
    let rec = r.cluster.unwrap();
    assert_eq!(rec.body().admin_state, AdminState::Rejected);
    assert!(
        rec.body().gaps[0].starts_with("node n1: no sprig_ctl login"),
        "{:?}",
        rec.body().gaps
    );

    // A stale guard: 39504, nothing written.
    let stale = vec![Cmp::CreateRev(f.dc.tier().key("leader").unwrap(), 1)];
    let e = f.monitor.pass(&id, Some(ProbeDepth::Light), stale).await;
    let before = reg.get(&id).await.unwrap().unwrap().mod_revision;
    if let Err(e) = e {
        assert_eq!(e.code(), Some(SprigCode::NotLeader));
    }
    assert_eq!(reg.get(&id).await.unwrap().unwrap().mod_revision, before);

    // Gone: forgotten.
    let missing = ClusterId::new("mon-b-c99").unwrap();
    assert!(
        f.monitor
            .pass(&missing, None, guard)
            .await
            .unwrap()
            .is_none()
    );
    assert!(!f.monitor.knows(&missing));
    let _ = &f.global;
}

#[tokio::test]
async fn record_events_schedule_and_passes_debounce() {
    let f = fixture("mon-c").await;
    let l = RegistryLoop::new(Arc::clone(&f.monitor));
    let id = ClusterId::new("mon-c-c1").unwrap();
    let ev = |deleted: bool| MetaEvent {
        source: Source::Dc,
        key: f.dc.tier().key("clusters/mon-c-c1").unwrap(),
        kv: (!deleted).then(|| sprig_meta::KeyValue {
            key: f.dc.tier().key("clusters/mon-c-c1").unwrap(),
            value: Vec::new(),
            create_revision: 1,
            mod_revision: 1,
            version: 1,
            lease: None,
        }),
        mod_revision: 1,
    };
    assert_eq!(l.keys_for(&ev(false)), vec![id.clone()]);
    assert_eq!(l.keys_for(&ev(true)), vec![id]);
    assert_eq!(
        (l.name(), l.tier(), l.workers()),
        ("registry", LoopTier::DcLeader, 8)
    );

    let guard = lead(&f.dc).await;
    let reg = f.monitor.registry().clone();
    let s = reg
        .register(unreachable("d1", TopologyKind::SingleNode))
        .await
        .unwrap();
    let id = s.body().cluster_id.clone();
    reg.set_admin_state(&id, AdminState::Active, vec![])
        .await
        .unwrap();
    let first = f
        .monitor
        .pass(&id, None, guard.clone())
        .await
        .unwrap()
        .unwrap();
    assert!(first.depth.is_some());
    // Woken again at once (its own write): no probe, just the wait.
    let again = f
        .monitor
        .pass(&id, None, guard.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(again.depth, None);
    assert!(again.next <= Duration::from_secs(5) && again.next > Duration::from_secs(4));
    // A forced pass always probes.
    let forced = f
        .monitor
        .pass(&id, Some(ProbeDepth::Light), guard)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(forced.depth, Some(ProbeDepth::Light));
}
