use std::time::{Duration, Instant};

use sprig_meta::MemStore;

use super::*;
use crate::profile::{CapabilityPublisher, DeriveInput, derive};
use crate::record::{GaleraWriters, WriterWeight};
use crate::testutil::{facts_mut, node, replication};

struct Fixture {
    global: MemStore,
    dc: MemStore,
    reg: ClusterRegistry,
}

fn fixture(dc: &str) -> Fixture {
    let global = MemStore::new(Tier::Global);
    let dc = MemStore::new(Tier::Dc(DcId::new(dc).unwrap()));
    let reg = ClusterRegistry::new(Arc::new(global.clone()), Arc::new(dc.clone())).unwrap();
    Fixture { global, dc, reg }
}

fn registration(name: &str) -> Registration {
    Registration {
        name: name.into(),
        topology: TopologyKind::PrimaryReplica,
        provisioner: ProvisionerKind::External,
        nodes: vec![
            NodeSpec {
                node: node("n1"),
                addr: "db1:3306".into(),
                zone: Some("a".into()),
            },
            NodeSpec {
                node: node("n2"),
                addr: "db2:3306".into(),
                zone: Some("b".into()),
            },
        ],
        secrets: SecretRefs {
            ctl: "vault:kv/sprig/mum1/ctl".into(),
            repl: "vault:kv/sprig/mum1/repl".into(),
        },
        capacity: Capacity {
            memory_gb: Some(256),
            ..Capacity::default()
        },
        declared: None,
    }
}

#[test]
fn needs_a_global_and_a_dc_store() {
    let g = Arc::new(MemStore::new(Tier::Global));
    let d = Arc::new(MemStore::new(Tier::Dc(DcId::new("mum1").unwrap())));
    assert!(ClusterRegistry::new(g.clone(), g.clone()).is_err());
    assert!(ClusterRegistry::new(d.clone(), d.clone()).is_err());
    assert!(ClusterRegistry::new(g, d).is_ok());
}

#[tokio::test]
async fn ids_are_global_and_lead_back_to_the_dc() {
    let mum = fixture("mum1");
    // A second DC sharing the same global tier.
    let fra_dc = MemStore::new(Tier::Dc(DcId::new("fra1").unwrap()));
    let fra = ClusterRegistry::new(Arc::new(mum.global.clone()), Arc::new(fra_dc)).unwrap();

    let a = mum.reg.register(registration("mum1-g1")).await.unwrap();
    let b = fra.register(registration("fra1-g1")).await.unwrap();
    let c = mum.reg.register(registration("mum1-g2")).await.unwrap();
    assert_eq!(a.body().cluster_id.as_str(), "mum1-c1");
    assert_eq!(b.body().cluster_id.as_str(), "fra1-c2");
    assert_eq!(
        (c.body().cluster_number, c.body().cluster_id.as_str()),
        (3, "mum1-c3")
    );
    assert_eq!(a.body().admin_state, AdminState::Probing);

    let e = mum.reg.lookup(2).await.unwrap().unwrap();
    assert_eq!((e.dc_id.as_str(), e.name.as_str()), ("fra1", "fra1-g1"));
    assert!(mum.reg.lookup(4).await.unwrap().is_none());

    let got = mum.reg.get(&a.body().cluster_id).await.unwrap().unwrap();
    assert_eq!(got.body(), a.body());
    assert_eq!(mum.reg.list().await.unwrap().len(), 2);
    assert_eq!(
        mum.dc
            .get(
                &Tier::Dc(DcId::new("mum1").unwrap())
                    .key("clusters/mum1-c1")
                    .unwrap(),
                Consistency::Linearizable
            )
            .await
            .unwrap()
            .map(|kv| kv.version),
        Some(1)
    );
}

#[tokio::test]
async fn allocation_skips_numbers_taken_meanwhile() {
    let f = fixture("mum1");
    // Someone else took number 1 and 2 directly.
    for n in [1u32, 2] {
        let entry = Record::new(ClusterIdEntry {
            dc_id: DcId::new("fra1").unwrap(),
            name: format!("x{n}"),
            cluster_id: ClusterId::new(format!("fra1-c{n}")).unwrap(),
        });
        f.global
            .txn(Txn {
                when: vec![],
                then: vec![Op::Put {
                    key: ClusterRegistry::id_key(n).unwrap(),
                    value: entry.encode().unwrap(),
                    lease: None,
                }],
                otherwise: vec![],
            })
            .await
            .unwrap();
    }
    let (n, id) = f.reg.allocate_id("mum1-g1").await.unwrap();
    assert_eq!((n, id.as_str()), (3, "mum1-c3"));
}

#[tokio::test]
async fn names_are_unique_and_nodes_required() {
    let f = fixture("mum1");
    f.reg.register(registration("g1")).await.unwrap();
    assert!(f.reg.register(registration("g1")).await.is_err());
    let mut empty = registration("g2");
    empty.nodes.clear();
    assert!(f.reg.register(empty).await.is_err());
}

#[tokio::test]
async fn admin_state_follows_the_state_machine() {
    let f = fixture("mum1");
    let id = f
        .reg
        .register(registration("g1"))
        .await
        .unwrap()
        .body()
        .cluster_id
        .clone();
    let r = f
        .reg
        .set_admin_state(
            &id,
            AdminState::Rejected,
            vec!["MariaDB 11.4.5 is below 11.8.0".into()],
        )
        .await
        .unwrap();
    assert_eq!(r.body().gaps.len(), 1);
    assert!(
        f.reg
            .set_admin_state(&id, AdminState::Draining, vec![])
            .await
            .is_err()
    );
    let r = f
        .reg
        .set_admin_state(&id, AdminState::Active, vec![])
        .await
        .unwrap();
    assert!(r.body().gaps.is_empty());
    for s in [
        AdminState::Draining,
        AdminState::Drained,
        AdminState::Retired,
    ] {
        f.reg.set_admin_state(&id, s, vec![]).await.unwrap();
    }
    // A retired name can be registered again.
    assert!(f.reg.register(registration("g1")).await.is_ok());
}

#[tokio::test]
async fn updates_are_compare_and_set() {
    let f = fixture("mum1");
    let id = f
        .reg
        .register(registration("g1"))
        .await
        .unwrap()
        .body()
        .cluster_id
        .clone();
    // Concurrent writers both land: the loser re-reads and retries.
    let (a, b) = tokio::join!(
        f.reg.update(&id, |r| {
            r.capacity.storage_gb = Some(4000);
            Ok(true)
        }),
        f.reg.update(&id, |r| {
            r.galera = Some(GaleraWriters {
                writer_epoch: 4,
                writers: vec![WriterWeight {
                    node: node("n1"),
                    weight: 1.0,
                }],
            });
            Ok(true)
        }),
    );
    a.unwrap();
    b.unwrap();
    let r = f.reg.get(&id).await.unwrap().unwrap();
    assert_eq!(r.body().capacity.storage_gb, Some(4000));
    assert_eq!(r.body().galera.as_ref().unwrap().writer_epoch, 4);
    // A no-op leaves the revision alone.
    let same = f.reg.update(&id, |_| Ok(false)).await.unwrap();
    assert_eq!(same.mod_revision, r.mod_revision);
    // The id never changes.
    assert!(
        f.reg
            .update(&id, |r| {
                r.cluster_id = ClusterId::new("mum1-c99").unwrap();
                Ok(true)
            })
            .await
            .is_err()
    );
    assert!(
        f.reg
            .update(&ClusterId::new("mum1-c42").unwrap(), |_| Ok(true))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn publishing_emits_events() {
    let f = fixture("mum1");
    let mut rx = f.reg.subscribe();
    let id = f
        .reg
        .register(registration("g1"))
        .await
        .unwrap()
        .body()
        .cluster_id
        .clone();

    let good = derive(DeriveInput {
        topology: TopologyKind::PrimaryReplica,
        probes: &replication(),
        declared: None,
        previous: None,
    });
    let mut fallen = replication();
    facts_mut(&mut fallen[0]).semi_sync.master_active = false;
    let bad = derive(DeriveInput {
        topology: TopologyKind::PrimaryReplica,
        probes: &fallen,
        declared: None,
        previous: Some(&good),
    });
    let mut publisher = CapabilityPublisher::new(None, Duration::from_secs(60));
    let t0 = Instant::now();
    f.reg
        .publish_profile(&id, &publisher.observe(good, t0).unwrap())
        .await
        .unwrap();
    let r = f
        .reg
        .publish_profile(&id, &publisher.observe(bad, t0).unwrap())
        .await
        .unwrap();
    assert_eq!(r.body().profile_version, 2);
    assert_eq!(r.body().profile.as_ref().unwrap().version, 2);

    f.reg
        .publish_health(&id, ClusterHealth::Healthy, "2026-11-07T00:00:00Z".into())
        .await
        .unwrap();
    // Same state: no write, no event; `since` is kept.
    let r = f
        .reg
        .publish_health(&id, ClusterHealth::Healthy, "2026-11-07T00:00:05Z".into())
        .await
        .unwrap();
    assert_eq!(
        r.body().health.as_ref().unwrap().since,
        "2026-11-07T00:00:00Z"
    );

    let mut events = Vec::new();
    while let Ok(e) = rx.try_recv() {
        events.push(e);
    }
    assert!(matches!(events[0], ClusterEvent::Registered { .. }));
    assert!(matches!(
        events[1],
        ClusterEvent::CapabilityChanged { version: 1, .. }
    ));
    let ClusterEvent::CapabilityChanged {
        version: 2,
        changes,
        ..
    } = &events[2]
    else {
        panic!("{events:?}");
    };
    assert!(
        changes
            .iter()
            .any(|c| c.field == "derived.zero_loss_failover")
    );
    assert!(matches!(
        events[3],
        ClusterEvent::HealthChanged {
            from: None,
            to: ClusterHealth::Healthy,
            ..
        }
    ));
    assert_eq!(events.len(), 4, "{events:?}");
}

#[tokio::test]
async fn watch_follows_records() {
    let f = fixture("mum1");
    let mut w = f.reg.watch(0).await.unwrap();
    let id = f
        .reg
        .register(registration("g1"))
        .await
        .unwrap()
        .body()
        .cluster_id
        .clone();
    f.reg
        .set_admin_state(&id, AdminState::Active, vec![])
        .await
        .unwrap();
    let RegistryChange::Put(first) = w.next().await.unwrap().unwrap() else {
        panic!()
    };
    assert_eq!(first.body().admin_state, AdminState::Probing);
    let RegistryChange::Put(second) = w.next().await.unwrap().unwrap() else {
        panic!()
    };
    assert_eq!(
        events_between(Some(first.body()), second.body()),
        [ClusterEvent::AdminStateChanged {
            cluster: id.clone(),
            from: AdminState::Probing,
            to: AdminState::Active,
        }]
    );
    f.dc.txn(Txn {
        when: vec![],
        then: vec![Op::Delete(f.reg.record_key(&id).unwrap())],
        otherwise: vec![],
    })
    .await
    .unwrap();
    let RegistryChange::Deleted { cluster, .. } = w.next().await.unwrap().unwrap() else {
        panic!()
    };
    assert_eq!(cluster, id);
}

#[test]
fn events_between_records() {
    let base = ClusterRecord {
        cluster_id: ClusterId::new("mum1-c7").unwrap(),
        cluster_number: 7,
        name: "g".into(),
        dc_id: DcId::new("mum1").unwrap(),
        topology: TopologyKind::Galera,
        provisioner: ProvisionerKind::External,
        admin_state: AdminState::Active,
        nodes: vec![],
        secrets: SecretRefs {
            ctl: "c".into(),
            repl: "r".into(),
        },
        capacity: Capacity::default(),
        galera: None,
        declared: None,
        profile_version: 0,
        profile: None,
        health: None,
        tuning: None,
        gaps: vec![],
        node_identities: std::collections::BTreeMap::new(),
    };
    assert!(matches!(
        events_between(None, &base)[..],
        [ClusterEvent::Registered { .. }]
    ));
    assert!(events_between(Some(&base), &base).is_empty());
    let mut next = base.clone();
    next.galera = Some(GaleraWriters {
        writer_epoch: 5,
        writers: vec![],
    });
    next.tuning = Some(crate::record::TuningState::default());
    let ev = events_between(Some(&base), &next);
    assert!(matches!(
        ev[0],
        ClusterEvent::WriterChanged {
            writer_epoch: 5,
            ..
        }
    ));
    assert!(matches!(ev[1], ClusterEvent::TuningChanged { .. }));
}

#[tokio::test]
async fn etcd_unreachable_is_39500() {
    let f = fixture("mum1");
    f.global.set_unavailable(true);
    let e = f.reg.register(registration("g1")).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::MetaUnavailable));
}

#[tokio::test]
async fn guarded_writes_fail_once_the_guard_breaks() {
    let f = fixture("mum1");
    let leader = f.dc.tier().key("leader").unwrap();
    let put = |v: &str| Op::Put {
        key: leader.clone(),
        value: v.as_bytes().to_vec(),
        lease: None,
    };
    let r =
        f.dc.txn(Txn {
            when: vec![],
            then: vec![put("a")],
            otherwise: vec![],
        })
        .await
        .unwrap();
    let guarded = f
        .reg
        .clone()
        .with_guard(vec![Cmp::CreateRev(leader.clone(), r.revision)]);
    let a = guarded.register(registration("mum1-g1")).await.unwrap();
    let id = a.body().cluster_id.clone();
    guarded
        .set_admin_state(&id, AdminState::Active, vec![])
        .await
        .unwrap();
    assert_eq!(
        guarded
            .register(registration("mum1-g1"))
            .await
            .unwrap_err()
            .code(),
        Some(SprigCode::ClusterExists)
    );
    // A new leader: the old token no longer holds.
    f.dc.txn(Txn {
        when: vec![],
        then: vec![Op::Delete(leader.clone())],
        otherwise: vec![],
    })
    .await
    .unwrap();
    f.dc.txn(Txn {
        when: vec![],
        then: vec![put("b")],
        otherwise: vec![],
    })
    .await
    .unwrap();
    let e = guarded
        .set_admin_state(&id, AdminState::Draining, vec![])
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::NotLeader));
    let e = guarded.register(registration("mum1-g2")).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::NotLeader));
    // Unguarded writes still work.
    f.reg
        .set_admin_state(&id, AdminState::Draining, vec![])
        .await
        .unwrap();
}

#[tokio::test]
async fn reads_reach_another_dcs_records() {
    // Single-etcd mode: both DC tiers in one store.
    let global = MemStore::new(Tier::Global);
    let a = MemStore::new(Tier::Dc(DcId::new("dc-a").unwrap()));
    let b = a.with_tier(Tier::Dc(DcId::new("dc-b").unwrap()));
    let reg_a = ClusterRegistry::new(Arc::new(global.clone()), Arc::new(a)).unwrap();
    let reg_b = ClusterRegistry::new(Arc::new(global), Arc::new(b)).unwrap();
    let stored = reg_b.register(registration("b1")).await.unwrap();
    let id = stored.body().cluster_id.clone();
    assert_eq!(dc_of(&id).unwrap().as_str(), "dc-b");
    let seen = reg_a.get(&id).await.unwrap().unwrap();
    assert_eq!(seen.body().cluster_id, id);
    // A cluster of this DC that doesn't exist is still None.
    assert!(
        reg_a
            .get(&ClusterId::new("dc-a-c9").unwrap())
            .await
            .unwrap()
            .is_none()
    );
    assert!(dc_of(&ClusterId::new("plain").unwrap()).is_none());
    assert!(dc_of(&ClusterId::new("dc-a-cx").unwrap()).is_none());
}
