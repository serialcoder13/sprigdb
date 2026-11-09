//! Which node controllers write to after a cluster failover (the
//! published writer first, as the gateway's W1 reads it).

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use sprig_cluster::failover::writer::{WriterLease, writer_lease_key};
use sprig_common::ids::NodeId;
use sprig_meta::conformance::unique_tier;
use sprig_meta::{MemStore, Op, Txn};

use super::*;
use crate::failover::cluster::{ClusterFailoverRecord, record_key};

/// `db1` registered first (the writer at registration), `db2` and `db3`
/// its replicas.
fn repl() -> ClusterRecord {
    serde_json::from_value(serde_json::json!({
        "cluster_id": "dc-a-c2", "cluster_number": 2, "name": "repl", "dc_id": "dc-a",
        "topology": "primary_replica", "provisioner": "external", "admin_state": "active",
        "nodes": [{"node": "db1", "addr": "127.0.0.1:13311"},
                  {"node": "db2", "addr": "127.0.0.1:13312"},
                  {"node": "db3", "addr": "127.0.0.1:13313"}],
        "secrets": {"ctl": "x", "repl": "y"},
        "capacity": {"storage_gb": 1, "busy_ms_per_sec": 1, "connections": 1},
        "profile_version": 0
    }))
    .unwrap()
}

fn node(n: &str) -> NodeId {
    NodeId::new(n).unwrap()
}

fn lease(n: &str, agent: &str, epoch: u64) -> WriterLease {
    WriterLease {
        node_id: n.into(),
        agent: agent.into(),
        writer_epoch: epoch,
    }
}

fn recorded(w: &str, epoch: u64) -> ClusterFailoverRecord {
    ClusterFailoverRecord {
        cluster_id: "dc-a-c2".into(),
        state: "healthy".into(),
        writer: Some(w.into()),
        writer_epoch: epoch,
        ..ClusterFailoverRecord::default()
    }
}

#[test]
fn after_a_failover_the_published_writer_wins() {
    let rec = repl();
    // db1 failed over to db2 (epoch 2). The driver still says db1 (its
    // next pass hasn't run), or nothing (it sees no writer): either way
    // the old primary, now read-only, must not be chosen.
    let p = published(
        Some(&lease("db2", "agent-db2", 2)),
        Some(&recorded("db2", 2)),
    );
    assert_eq!(p, Published::Node(node("db2"), 2));
    assert_eq!(
        writer_of(&rec, &p, Some(node("db1"))).unwrap().node,
        node("db2")
    );
    assert_eq!(writer_of(&rec, &p, None).unwrap().node, node("db2"));
    // The failover record alone (the lease expired with its agent).
    let p = published(None, Some(&recorded("db3", 3)));
    assert_eq!(writer_of(&rec, &p, None).unwrap().node, node("db3"));
}

#[test]
fn nobody_writes_while_the_writer_is_in_transit() {
    let rec = repl();
    for p in [
        published(Some(&lease("db2", "controller:ctl-1", 2)), None),
        published(
            Some(&lease("db1", "agent-db1", 1)),
            Some(&recorded("db2", 2)),
        ),
    ] {
        assert!(matches!(p, Published::InTransit(_)), "{p:?}");
        let e = writer_of(&rec, &p, Some(node("db1"))).unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::ClusterUnavailable));
    }
}

#[test]
fn without_a_failover_the_driver_then_the_registration_decide() {
    let rec = repl();
    assert_eq!(published(None, None), Published::Nothing);
    assert_eq!(
        writer_of(&rec, &Published::Nothing, Some(node("db3")))
            .unwrap()
            .node,
        node("db3")
    );
    assert_eq!(
        writer_of(&rec, &Published::Nothing, None).unwrap().node,
        node("db1")
    );
    // A lease naming a node the record doesn't have is ignored.
    let p = Published::Node(node("db9"), 4);
    assert_eq!(writer_of(&rec, &p, None).unwrap().node, node("db1"));
}

#[tokio::test]
async fn the_published_writer_is_read_from_the_dc_tier() {
    let dc: Arc<dyn MetaStore> = Arc::new(MemStore::new(unique_tier()));
    let c = ClusterId::new("dc-a-c2").unwrap();
    assert_eq!(read_published(&*dc, &c).await.unwrap(), Published::Nothing);
    let put = |key, value| Op::Put {
        key,
        value,
        lease: None,
    };
    dc.txn(Txn {
        when: Vec::new(),
        then: vec![
            put(
                writer_lease_key(dc.tier(), &c).unwrap(),
                Record::new(lease("db2", "agent-db2", 2)).encode().unwrap(),
            ),
            put(
                record_key(dc.tier(), &c).unwrap(),
                Record::new(recorded("db2", 2)).encode().unwrap(),
            ),
        ],
        otherwise: Vec::new(),
    })
    .await
    .unwrap();
    assert_eq!(
        read_published(&*dc, &c).await.unwrap(),
        Published::Node(node("db2"), 2)
    );
}
