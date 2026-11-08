#![allow(clippy::unwrap_used, clippy::float_cmp, clippy::too_many_lines)]

use super::*;

fn record(admin: &str, health: &str) -> ClusterRecord {
    serde_json::from_value(serde_json::json!({
        "cluster_id": "mum1-c7", "cluster_number": 7, "name": "g1", "dc_id": "mum1",
        "topology": "single_node", "provisioner": "external", "admin_state": admin,
        "nodes": [], "secrets": {"ctl": "x", "repl": "y"},
        "capacity": {"storage_gb": 4000, "busy_ms_per_sec": 16000, "connections": 3000},
        "profile_version": 0,
        "health": {"state": health, "since": "2026-10-31T09:12:03Z"},
        "gaps": ["tuning: over table budget"]
    }))
    .unwrap()
}

#[test]
fn registry_records_become_placement_views() {
    let c = cluster_view(&record("active", "healthy"), ClusterLimits::default());
    assert_eq!(c.state, ClusterState::Placeable);
    assert_eq!(c.cap.storage_bytes, 4000.0 * GB);
    assert_eq!(c.cap.busy_ms_per_sec, 16000.0);
    assert!(c.over_table_budget);
    // No profile yet: the mover can't use it.
    assert!(!c.caps.mover_target);
    assert_eq!(
        cluster_view(&record("active", "degraded"), ClusterLimits::default()).state,
        ClusterState::Degraded
    );
    assert_eq!(
        cluster_view(&record("draining", "healthy"), ClusterLimits::default()).state,
        ClusterState::Draining
    );
    assert_eq!(
        cluster_view(&record("probing", "healthy"), ClusterLimits::default()).state,
        ClusterState::Unavailable
    );
}

/// Live: `SPRIG_TEST_MARIADB_SINGLE`. Two registered clusters (both the
/// Compose single node), three tenants' catalogs mapped to the first, a
/// planning run, and `explain placement`. Schemas `t917000N_placement`
/// are dropped afterwards.
#[tokio::test]
async fn live_run_builds_the_view_and_explains() {
    use sprig_meta::{MemStore, Op, Tier, Txn};

    let Ok(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE") else {
        eprintln!("skipped: set SPRIG_TEST_MARIADB_SINGLE to run it");
        return;
    };
    let dc = DcId::new("mum1").unwrap();
    let global: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Global));
    let dcs: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(dc.clone())));
    let c1 = ClusterId::new("mum1-c1").unwrap();
    let c2 = ClusterId::new("mum1-c2").unwrap();
    let mut ops = Vec::new();
    for (n, c) in [(1, &c1), (2, &c2)] {
        let mut r = record("active", "healthy");
        r.cluster_id = c.clone();
        r.cluster_number = n;
        r.gaps.clear();
        ops.push(Op::Put {
            key: dcs.tier().key(&format!("clusters/{c}")).unwrap(),
            value: Record::new(r).encode().unwrap(),
            lease: None,
        });
    }
    let pool = mysql_async::Pool::new(mysql_async::Opts::from_url(&url).unwrap());
    let mut conn = pool.get_conn().await.unwrap();
    let tenants = [9_170_001u64, 9_170_002, 9_170_003];
    for t in tenants {
        let s = format!("t{t}_placement");
        conn.query_drop(format!("DROP DATABASE IF EXISTS {s}"))
            .await
            .unwrap();
        conn.query_drop(format!("CREATE DATABASE {s}"))
            .await
            .unwrap();
        conn.query_drop(format!(
            "CREATE TABLE {s}.a (id INT PRIMARY KEY, v VARCHAR(100)) ENGINE=InnoDB"
        ))
        .await
        .unwrap();
        conn.query_drop(format!("ANALYZE TABLE {s}.a"))
            .await
            .unwrap();
        ops.push(Op::Put {
            key: dcs.tier().key(&format!("shardmaps/{t}/0")).unwrap(),
            value: Record::new(ShardMap::catalog(TenantId(t), c1.clone(), 1))
                .encode()
                .unwrap(),
            lease: None,
        });
    }
    dcs.txn(Txn {
        when: Vec::new(),
        then: ops,
        otherwise: Vec::new(),
    })
    .await
    .unwrap();
    let pools = crate::catalogs::StaticPools::new()
        .with_url(c1.clone(), &url)
        .unwrap()
        .with_url(c2.clone(), &url)
        .unwrap();
    let pools: Arc<dyn ClusterPools> = Arc::new(pools);
    let locator = crate::catalogs::ShardMapLocator::new(
        Arc::clone(&dcs),
        Arc::clone(&pools),
        crate::catalogs::default_naming(),
    );
    let b = BalancerLoop::new(
        dc,
        ClusterRegistry::new(Arc::clone(&global), Arc::clone(&dcs)).unwrap(),
        Arc::clone(&dcs),
        pools,
        Arc::new(locator),
        Directory::new(global),
        None,
        Arc::new(sprig_common::time::SystemClock),
        None,
        "ctl-test".to_owned(),
    );
    let r = async {
        let report = b.run_once().await.unwrap();
        assert_eq!(report.fullness.len(), 2);
        let view = b.view().await.unwrap();
        assert_eq!(view.shards.len(), 3);
        let s = &view.shards[&ShardKey::new(9_170_001, 0, 0)];
        assert_eq!(s.cluster, c1);
        assert_eq!(s.load.tables, 1.0);
        assert_eq!(s.load.schemas, 1.0);
        assert!(s.load.storage_bytes > 0.0);
        let e = b.explain_placement("9170002").await.unwrap();
        assert_eq!(e.shards.len(), 1);
        assert!(e.clusters[0].holds_tenant);
        assert!(!e.reasoning.is_empty());
    };
    r.await;
    for t in tenants {
        conn.query_drop(format!("DROP DATABASE t{t}_placement"))
            .await
            .unwrap();
    }
}
