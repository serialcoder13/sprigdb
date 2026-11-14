//! Which node controllers write to after a cluster failover (the
//! published writer first, as the gateway reads it).

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

/// While a mover's cutover holds the catalog's shard map, the controller
/// runs no catalog migration (the cutover may rename the catalog's tables
/// away; found in a live move).
#[tokio::test]
async fn catalog_migrations_wait_while_a_cutover_holds_the_catalog() {
    let dc = MemStore::new(unique_tier());
    let pools: Arc<dyn ClusterPools> = Arc::new(StaticPools::new());
    let loc = ShardMapLocator::new(Arc::new(dc.clone()), pools, default_naming());
    let t = TenantId(7);
    // No map, then a serving map: nothing holds it.
    not_held(&loc, t).await.unwrap();
    let key = ShardMap::catalog_key(dc.tier(), t).unwrap();
    let mut map = ShardMap::catalog(t, ClusterId::new("cat-hold-c1").unwrap(), 1);
    let put = |m: &ShardMap| Txn {
        when: vec![],
        then: vec![Op::Put {
            key: key.clone(),
            value: Record::new(m.clone()).encode().unwrap(),
            lease: None,
        }],
        otherwise: vec![],
    };
    dc.txn(put(&map)).await.unwrap();
    not_held(&loc, t).await.unwrap();
    assert_eq!(loc.held_by(t).await.unwrap(), None);
    // Held by a cutover.
    map.ranges[0].state = RangeState::Held;
    map.ranges[0].job_id = Some("job-42".into());
    map.ranges[0].hold_until_ms = Some(1);
    dc.txn(put(&map)).await.unwrap();
    assert_eq!(loc.held_by(t).await.unwrap().as_deref(), Some("job-42"));
    let e = not_held(&loc, t).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::CatalogUnavailable));
    assert!(e.retryable && e.to_string().contains("job-42"), "{e}");
}

/// A sequence seen in a live move: a cutover
/// rename-fenced the tenant's catalog, catalog migrations then ran on it,
/// and the unfence found "both t…_sprig.databases and its fenced name
/// exist". Now the migration refuses the fenced catalog, so the unfence
/// works; and a recreation the old migrator made (empty tables plus the
/// internal keys) makes the unfence fail loudly instead of dropping rows.
/// Live on the Compose single node; skipped without
/// `SPRIG_TEST_MARIADB_SINGLE`. Its catalog is `t<id>_sprig` with an id
/// from 9,200,000,000 + pid × 1000, dropped by exact name afterwards.
#[tokio::test]
async fn live_migrations_skip_a_fenced_catalog_and_the_unfence_restores_it() {
    use futures::FutureExt;
    use mysql_async::prelude::Queryable;
    use sprig_catalog::StaticLocator;
    use sprig_catalog::migrate::{CORE_MIGRATIONS, MigrateOpts, Migrator};
    use sprig_cluster::fence::{
        FenceMethod, FenceOptions, FenceRequest, FenceScope, fence, unfence,
    };
    use sprig_cluster::sql::NodeEndpoint;

    let Some(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE")
        .ok()
        .filter(|v| !v.trim().is_empty())
    else {
        eprintln!("skipped: set SPRIG_TEST_MARIADB_SINGLE");
        return;
    };
    let tenant = TenantId(9_200_000_000 + u64::from(std::process::id()) * 1000);
    let catalog = format!("t{tenant}_sprig");
    let pool = Pool::new(mysql_async::Opts::from_url(&url).unwrap());
    let ep = NodeEndpoint::from_url(NodeId::new("n1").unwrap(), &url).unwrap();
    let body = async {
        let mut conn = pool.get_conn().await.unwrap();
        let exists: Option<i64> = conn
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?",
                (&catalog,),
            )
            .await
            .unwrap();
        assert_eq!(exists, Some(0), "{catalog} must not exist before the test");
        Migrator::new(&[])
            .unwrap()
            .run(&mut conn, &catalog, &MigrateOpts::default())
            .await
            .unwrap();
        let opts = FenceOptions::default();
        let req = FenceRequest {
            tenant,
            schemas: vec![catalog.clone()],
            tables: None,
            scope: FenceScope::Writes,
            method: FenceMethod::Rename,
            deadline: std::time::Instant::now() + Duration::from_secs(30),
        };
        let databases = format!(
            "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{catalog}' \
             AND TABLE_NAME = 'databases'"
        );

        // 1. The cutover fences the catalog; a migration waits.
        let receipt = fence(&req, &ep, &[], &opts).await.unwrap();
        assert!(receipt.renamed.iter().any(|r| r.from == "databases"));
        let loc = StaticLocator::new();
        let e = migrate_catalog(
            &loc,
            tenant,
            &mut conn,
            &catalog,
            &Migrator::new(&[]).unwrap(),
        )
        .await
        .unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::CatalogUnavailable), "{e}");
        let n: Option<i64> = conn.query_first(&databases).await.unwrap();
        assert_eq!(n, Some(0), "no empty catalog tables were created");
        // 2. The abort's unfence brings the catalog back intact.
        unfence(&receipt, &ep, &opts).await.unwrap();
        let n: Option<i64> = conn.query_first(&databases).await.unwrap();
        assert_eq!(n, Some(1));
        let r = Migrator::new(&[])
            .unwrap()
            .run(&mut conn, &catalog, &MigrateOpts::default())
            .await
            .unwrap();
        assert!(r.applied.is_empty(), "{:?}", r.applied);

        // 3. The old bug's recreation: empty tables and the internal keys.
        let receipt = fence(&req, &ep, &[], &opts).await.unwrap();
        conn.query_drop(format!("USE `{catalog}`")).await.unwrap();
        for stmt in CORE_MIGRATIONS[0].statements {
            conn.query_drop(*stmt).await.unwrap();
        }
        let e = unfence(&receipt, &ep, &opts).await.unwrap_err();
        assert!(e.to_string().contains("has rows"), "{e}");
        // The operator drops the stray copies by hand; the unfence then
        // renames the real tables back.
        for t in ["databases", "objects", "settings", "schema_versions"] {
            conn.query_drop(format!("DROP TABLE `{catalog}`.`{t}`"))
                .await
                .unwrap();
        }
        unfence(&receipt, &ep, &opts).await.unwrap();
        let n: Option<i64> = conn.query_first(&databases).await.unwrap();
        assert_eq!(n, Some(1));
        drop(conn);
    };
    let r = std::panic::AssertUnwindSafe(body).catch_unwind().await;
    let mut c = pool.get_conn().await.unwrap();
    c.query_drop(format!("DROP DATABASE IF EXISTS `{catalog}`"))
        .await
        .unwrap();
    drop(c);
    pool.disconnect().await.unwrap();
    if let Err(p) = r {
        std::panic::resume_unwind(p);
    }
}
