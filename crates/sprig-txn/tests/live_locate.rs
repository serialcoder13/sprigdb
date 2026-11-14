//! Live test of the resolver finding anchors through shard maps:
//! `MapLocator` over in-memory maps, `MyAdmin` on the dev single node.
//!
//! ```sh
//! SPRIG_TEST_MARIADB_SINGLE=mysql://root:sprig-dev-root@127.0.0.1:13301 \
//! cargo test -p sprig-txn --test live_locate -- --nocapture
//! ```
//!
//! It creates exactly two schemas of test tenant 3930650003 and drops them
//! by name at the start and the end.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use mysql_async::prelude::Queryable;
use mysql_async::{Opts, Pool};
use sprig_common::ids::{ClusterId, DbId, DcId, GroupId, ShardId, TenantId};
use sprig_meta::{MemStore, MetaStore, Tier};
use sprig_shard::{ShardMap, ShardMapStore, TenantMaps};
use sprig_txn::decision::create_table_sql;
use sprig_txn::mysql::{MyAdmin, MyCluster, MyConn};
use sprig_txn::xid::bqual;
use sprig_txn::{
    AnchorLocator, BranchConn, ClusterAdmin, ClusterCaps, Ctl, Decision, DecisionRow, DtidGen,
    Gtrid, MapLocator, Resolver, ResolverConfig, Xid,
};

const T: TenantId = TenantId(3_930_650_003);
/// A database's default group (no map of its own).
const DEFAULT: &str = "t3930650003_d1_g1_s4";
/// Shard 2 of sharded group 9.
const SHARDED: &str = "t3930650003_d1_g9_s2";

fn pool(url: &str) -> Pool {
    Pool::new(
        mysql_async::OptsBuilder::from_opts(Opts::from_url(url).unwrap()).pool_opts(
            mysql_async::PoolOpts::default()
                .with_constraints(mysql_async::PoolConstraints::new(0, 2).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(30)),
        ),
    )
}

async fn drop_schemas(p: &Pool) {
    let mut c = p.get_conn().await.unwrap();
    for s in [DEFAULT, SHARDED] {
        c.query_drop(format!("DROP DATABASE IF EXISTS `{s}`"))
            .await
            .unwrap();
    }
}

/// Prepares a branch inserting `k` into `schema`.t, then disconnects, as
/// a crashed gateway would.
async fn prepared(url: &str, x: &Xid, schema: &str, k: i64) {
    let mut c = MyConn::connect(Opts::from_url(url).unwrap()).await.unwrap();
    c.control(&[Ctl::XaStart(x.clone())]).await.unwrap();
    c.exec(&format!("INSERT INTO `{schema}`.t VALUES ({k})"))
        .await
        .unwrap();
    c.control(&[Ctl::XaEnd(x.clone()), Ctl::XaPrepare(x.clone())])
        .await
        .unwrap();
    drop(c);
}

#[tokio::test]
async fn the_resolver_finds_anchors_through_the_shard_maps() {
    let Ok(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE") else {
        eprintln!("skipped: set SPRIG_TEST_MARIADB_SINGLE to run it");
        return;
    };
    let url = url.split(',').next().unwrap().trim().to_owned();
    let p = pool(&url);
    drop_schemas(&p).await;
    {
        let mut c = p.get_conn().await.unwrap();
        for s in [DEFAULT, SHARDED] {
            c.query_drop(format!("CREATE DATABASE `{s}`"))
                .await
                .unwrap();
            c.query_drop(create_table_sql(s)).await.unwrap();
            c.query_drop(format!("CREATE TABLE `{s}`.t (k BIGINT PRIMARY KEY)"))
                .await
                .unwrap();
        }
    }
    let cluster = ClusterId::new("dev-single").unwrap();
    // Maps: the tenant map on the single node; group 9 in two ranges.
    let store: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(DcId::new("mum1").unwrap())));
    let maps = ShardMapStore::new(Arc::clone(&store));
    let c2 = std::slice::from_ref(&cluster);
    maps.create(&ShardMap::presplit(T, GroupId(0), 0, 1, c2).unwrap())
        .await
        .unwrap();
    maps.create(&ShardMap::presplit(T, GroupId(9), 5, 2, c2).unwrap())
        .await
        .unwrap();
    let opts = Opts::from_url(&url).unwrap();
    let mut clusters = BTreeMap::new();
    clusters.insert(
        cluster.clone(),
        MyCluster {
            opts: opts.clone(),
            admin_opts: opts,
            caps: ClusterCaps { xa: true },
        },
    );
    let admin = Arc::new(MyAdmin::new(&clusters));
    let locator = Arc::new(MapLocator::new(
        maps,
        TenantMaps::new(Arc::clone(&store)),
        Arc::clone(&admin) as Arc<dyn sprig_txn::GroupSchemas>,
    ));
    let default_home = sprig_txn::AnchorRef {
        db: DbId(1),
        group: GroupId(1),
        hash_point: 0,
    };
    let sharded_home = sprig_txn::AnchorRef {
        db: DbId(1),
        group: GroupId(9),
        hash_point: 1 << 63,
    };
    assert_eq!(
        locator.locate(T, default_home).await.unwrap(),
        (cluster.clone(), DEFAULT.to_owned())
    );
    assert_eq!(
        locator.locate(T, sharded_home).await.unwrap(),
        (cluster.clone(), SHARDED.to_owned())
    );

    // Two crashed transactions: one with nothing recorded (home in the
    // default group), one whose commit row is in the sharded group.
    let dtids = DtidGen::new(0x3930_6500);
    let xid = |home, shard| Xid {
        gtrid: Gtrid {
            tenant: T,
            dtid: dtids.next(),
            start_ms: 0,
            home,
        },
        bqual: bqual(&cluster, ShardId(shard)),
    };
    let lost = xid(default_home, 4);
    let won = xid(sharded_home, 2);
    prepared(&url, &lost, DEFAULT, 1).await;
    prepared(&url, &won, SHARDED, 2).await;
    admin
        .insert_decision(
            &cluster,
            SHARDED,
            &DecisionRow::new(won.gtrid.dtid, Decision::Commit, "r1integrate_txn"),
        )
        .await
        .unwrap();
    let r = Resolver::new(
        admin.clone(),
        locator,
        ResolverConfig {
            min_age: Duration::from_secs(1),
            ..ResolverConfig::default()
        },
    );
    let first = r.scan(&cluster).await.unwrap();
    assert!(first.young >= 2, "{first:?}");
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let t0 = std::time::Instant::now();
    let rep = r.scan(&cluster).await.unwrap();
    eprintln!("scan after min_age: {rep:?} in {:?}", t0.elapsed());
    let mine = admin.xa_recover(&cluster).await.unwrap();
    assert!(
        !mine.iter().any(|x| x.gtrid.tenant == T),
        "still prepared: {mine:?}"
    );
    let mut c = p.get_conn().await.unwrap();
    let in_default: Vec<i64> = c
        .query(format!("SELECT k FROM `{DEFAULT}`.t"))
        .await
        .unwrap();
    let in_sharded: Vec<i64> = c
        .query(format!("SELECT k FROM `{SHARDED}`.t"))
        .await
        .unwrap();
    assert_eq!((in_default, in_sharded), (vec![], vec![2]));
    let abort = admin
        .read_decision(&cluster, DEFAULT, lost.gtrid.dtid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (abort.outcome, abort.coordinator.as_str()),
        (Decision::Abort, "resolver")
    );
    drop(c);
    drop_schemas(&p).await;
    p.disconnect().await.unwrap();
    // The resolver and the locator hold the admin too.
    drop(r);
    drop(admin);
}
