//! Regression: a shard schema the mover makes on another cluster has the
//! distributed transactions' decision table, so the first coordinated
//! write anchored there works.
//!
//! Needs the Compose stack's clusters (no etcd, no worker: the test drives
//! the job's source and target directly, as the mover's preparing step
//! does):
//!
//! ```sh
//! SPRIG_TEST_MOVER_LIVE=1 cargo test -p sprig-mover --test live_dtx -- --nocapture
//! ```
//!
//! A shard schema `t<id>_d1_g1_s1` with a table and `__sprig_dtx` is made
//! on `dc-a-c1` (13301); the target on `dc-a-c2` (13311) prepares the job
//! (schemas, tables, object statements); then a coordinated write runs on
//! the new schema as a distributed transaction's anchor branch does: an XA branch that
//! writes a row and its decision row, prepared and committed. Before the
//! fix the decision insert failed with 1146.
//!
//! The tenant id is 1,000,000,000 above every `t<id>_` schema on the three
//! clusters at the start, checked free on each; the test drops only its own
//! two schemas, by exact name.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::sync::Arc;
use std::time::Duration;

use mysql_async::prelude::Queryable;
use mysql_async::{Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts};
use sprig_binlog::BinlogSource;
use sprig_cluster::{NodeEndpoint, TopologyKind};
use sprig_common::ids::{ClusterId, JobId, NodeId, TenantId};
use sprig_mover::mysql::{ClusterAccess, MysqlSource, MysqlTarget};
use sprig_mover::{SourceIo, TargetIo};

const ROOT_PW: &str = "sprig-dev-root";
const URLS: [&str; 3] = ["127.0.0.1:13301", "127.0.0.1:13311", "127.0.0.1:13321"];

fn opts(addr: &str) -> Opts {
    Opts::from_url(&format!("mysql://root:{ROOT_PW}@{addr}")).unwrap()
}

fn pool(addr: &str) -> Pool {
    Pool::new(
        OptsBuilder::from_opts(opts(addr)).pool_opts(
            PoolOpts::new()
                .with_constraints(PoolConstraints::new(0, 2).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(60)),
        ),
    )
}

fn access(cluster: &str, addr: &str) -> ClusterAccess {
    let node = NodeId::new(format!("{cluster}-n1")).unwrap();
    let ep = NodeEndpoint {
        node,
        opts: opts(addr),
    };
    ClusterAccess {
        cluster: ClusterId::new(cluster).unwrap(),
        topology: TopologyKind::SingleNode,
        writer: ep.clone(),
        nodes: vec![ep],
        repl: BinlogSource {
            host: "127.0.0.1".into(),
            port: addr.rsplit(':').next().unwrap().parse().unwrap(),
            user: "root".into(),
            password: ROOT_PW.into(),
            tls: None,
        },
        domain: None,
    }
}

/// The highest `t<id>_` tenant id on any of the three clusters.
async fn highest_tenant(pools: &[Pool]) -> u64 {
    let mut top = 0;
    for p in pools {
        let mut c = p.get_conn().await.unwrap();
        let names: Vec<String> = c
            .query(
                "SELECT schema_name FROM information_schema.schemata \
                 WHERE schema_name REGEXP '^t[0-9]+_'",
            )
            .await
            .unwrap();
        for n in names {
            let id: u64 = n[1..].split('_').next().unwrap().parse().unwrap_or(0);
            top = top.max(id);
        }
    }
    top
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_moved_shard_schema_takes_coordinated_writes() {
    if std::env::var("SPRIG_TEST_MOVER_LIVE").is_err() {
        eprintln!("skipped: set SPRIG_TEST_MOVER_LIVE=1 with the Compose stack up");
        return;
    }
    let pools: Vec<Pool> = URLS.iter().map(|u| pool(u)).collect();
    // Rule 8f: above every id present and clear of the range of the agent
    // holding the highest one.
    let tenant =
        TenantId(highest_tenant(&pools).await + 1_000_000_000 + u64::from(std::process::id() % 97));
    let data = format!("t{tenant}_d1_g1_s1");
    for p in &pools {
        let mut c = p.get_conn().await.unwrap();
        let n: Option<u64> = c
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name LIKE ?",
                (format!("t{tenant}\\_%"),),
            )
            .await
            .unwrap();
        assert_eq!(n, Some(0), "tenant {tenant} is taken");
    }
    eprintln!("tenant {tenant}: {data} on dc-a-c1, prepared on dc-a-c2");

    let mut src = pools[0].get_conn().await.unwrap();
    src.query_drop(format!("CREATE DATABASE `{data}`"))
        .await
        .unwrap();
    src.query_drop(sprig_catalog::ddl::dtx_table_sql(&data))
        .await
        .unwrap();
    src.query_drop(format!(
        "CREATE TABLE `{data}`.items (id INT PRIMARY KEY, v VARCHAR(40) NOT NULL)"
    ))
    .await
    .unwrap();
    src.query_drop(format!(
        "INSERT INTO `{data}`.items VALUES (1, 'a'), (2, 'b')"
    ))
    .await
    .unwrap();
    drop(src);

    let body = async {
        let job = JobId::new_v7();
        let source = Arc::new(MysqlSource::new(
            access("dc-a-c1", URLS[0]),
            job,
            tenant,
            vec![data.clone()],
            None,
            4_242_001,
            false,
        ));
        let target = MysqlTarget::new(access("dc-a-c2", URLS[1]), job, None, Arc::clone(&source));
        let tables = source.tables().await.unwrap();
        assert!(
            tables.iter().all(|t| t.name != "__sprig_dtx"),
            "the decision table's rows aren't copied"
        );
        let objects = source.objects().await.unwrap();
        target.prepare(&tables, &objects).await.unwrap();

        // The coordinated write a distributed transaction's anchor branch
        // makes.
        let mut t = pools[1].get_conn().await.unwrap();
        let xid = format!("'bugfix1-{tenant}'");
        t.query_drop(format!("XA START {xid}")).await.unwrap();
        t.query_drop(format!("INSERT INTO `{data}`.items VALUES (3, 'c')"))
            .await
            .unwrap();
        let decision = t
            .query_drop(format!(
                "INSERT INTO `{data}`.`__sprig_dtx` (dtid, outcome, decider, coordinator, participants) \
                 VALUES (UNHEX(MD5('bugfix1')), 'commit', NULL, 'bugfix1', NULL)"
            ))
            .await;
        t.query_drop(format!("XA END {xid}")).await.unwrap();
        if let Err(e) = decision {
            t.query_drop(format!("XA ROLLBACK {xid}")).await.unwrap();
            panic!("the decision row on the new shard schema: {e}");
        }
        t.query_drop(format!("XA PREPARE {xid}")).await.unwrap();
        t.query_drop(format!("XA COMMIT {xid}")).await.unwrap();
        let n: Option<u64> = t
            .query_first(format!(
                "SELECT COUNT(*) FROM `{data}`.`__sprig_dtx` WHERE coordinator = 'bugfix1'"
            ))
            .await
            .unwrap();
        assert_eq!(n, Some(1));
        target.forget(job).await.unwrap();
    };
    let r = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(body)).await;
    // Cleanup by exact name: the test's own schema on both clusters.
    for p in &pools[..2] {
        let mut c = p.get_conn().await.unwrap();
        c.query_drop(format!("DROP DATABASE IF EXISTS `{data}`"))
            .await
            .unwrap();
    }
    for p in pools {
        p.disconnect().await.unwrap();
    }
    if let Err(e) = r {
        std::panic::resume_unwind(e);
    }
}
