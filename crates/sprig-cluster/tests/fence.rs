//! Fences and drivers against the development Compose topologies
//! (`deploy/compose`). Each test is skipped unless its `SPRIG_TEST_MARIADB_*`
//! variable (the names of `sprig_testkit::skip`) lists the node URLs, writer
//! first:
//!
//! ```sh
//! export SPRIG_TEST_MARIADB_SINGLE=mysql://root:sprig-dev-root@127.0.0.1:13301
//! export SPRIG_TEST_MARIADB_REPLICATION=mysql://root:sprig-dev-root@127.0.0.1:13311,...
//! export SPRIG_TEST_MARIADB_GALERA=mysql://root:sprig-dev-root@127.0.0.1:13321,...
//! cargo test -p sprig-cluster --test fence -- --test-threads=1
//! ```
//!
//! The servers are shared: every test makes its own tenant with a random id
//! (`t<id>_...` schemas and accounts, as fences only touch those), its own
//! control account where it needs one, and drops all of it afterwards. No
//! global variable is changed.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::many_single_char_names,
    missing_docs
)]

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::FutureExt;
use mysql_async::prelude::Queryable;
use sprig_cluster::driver::{
    BasicDriver, GaleraDriver, NodeConnector, SourcePref, StaticConnector, TopologyDriver,
};
use sprig_cluster::fence::{
    FenceMethod, FenceOptions, FenceRequest, FenceScope, fence, fenced_name, unfence,
};
use sprig_cluster::probe::feed::{self, FeedLevel};
use sprig_cluster::probe::{ProbeDepth, Prober};
use sprig_cluster::profile::RywMethod;
use sprig_cluster::record::{
    AdminState, Capacity, ClusterRecord, NodeSpec, ProvisionerKind, SecretRefs, TopologyKind,
};
use sprig_cluster::sql::literal;
use sprig_cluster::{ClusterHealth, ClusterSettings, NodeEndpoint, ProbeSession};
use sprig_common::SprigCode;
use sprig_common::ids::{ClusterId, DcId, JobId, NodeId, ShardId, TenantId};

/// The `SPRIG_TEST_*` variables of `sprig_testkit::skip`, inlined so these
/// tests don't depend on the harness crate.
mod skip {
    pub(crate) const MARIADB_SINGLE: &str = "SPRIG_TEST_MARIADB_SINGLE";
    pub(crate) const MARIADB_REPLICATION: &str = "SPRIG_TEST_MARIADB_REPLICATION";
    pub(crate) const MARIADB_GALERA: &str = "SPRIG_TEST_MARIADB_GALERA";

    pub(crate) fn var(name: &str) -> Option<String> {
        std::env::var(name)
            .ok()
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty())
    }

    pub(crate) fn urls(value: &str) -> Vec<String> {
        value
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect()
    }

    pub(crate) fn report_missing(name: &str, test: &str) {
        assert!(
            var("SPRIG_TEST_STRICT").is_none_or(|v| v == "0"),
            "{test}: {name} is not set (SPRIG_TEST_STRICT)"
        );
        eprintln!("skipped {test}: set {name} to run it");
    }
}

/// A unique SQL identifier: `prefix_<random hex>`.
fn unique_ident(prefix: &str) -> String {
    #[allow(clippy::cast_possible_truncation)]
    let r = JobId::new_v7().0.as_u128() as u64;
    format!("{prefix}_{:012x}", r & 0xffff_ffff_ffff)
}

const PASSWORD: &str = "fence-test-pw-1";

fn endpoints(var: &str) -> Option<Vec<NodeEndpoint>> {
    let urls = skip::var(var)?;
    Some(
        skip::urls(&urls)
            .iter()
            .enumerate()
            .map(|(i, u)| {
                NodeEndpoint::from_url(NodeId::new(format!("n{}", i + 1)).unwrap(), u).unwrap()
            })
            .collect(),
    )
}

macro_rules! topology_or_skip {
    ($var:expr) => {
        match endpoints($var) {
            Some(eps) => eps,
            None => {
                skip::report_missing($var, module_path!());
                return;
            }
        }
    };
}

/// A random tenant id far above anything real, so names can't collide.
fn random_tenant() -> TenantId {
    #[allow(clippy::cast_possible_truncation)]
    let r = JobId::new_v7().0.as_u128() as u64;
    TenantId(9_000_000_000 + r % 900_000_000)
}

async fn admin(ep: &NodeEndpoint) -> ProbeSession {
    ProbeSession::connect(ep, Duration::from_secs(10))
        .await
        .unwrap()
}

async fn exec(ep: &NodeEndpoint, sql: &str) {
    let mut s = admin(ep).await;
    s.exec(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    s.close().await;
}

async fn scalar(ep: &NodeEndpoint, sql: &str) -> Option<String> {
    let mut s = admin(ep).await;
    let v = s.scalar(sql).await.unwrap();
    s.close().await;
    v
}

async fn grants(ep: &NodeEndpoint, grantee: &str) -> Vec<String> {
    let mut s = admin(ep).await;
    let mut v: Vec<String> = s
        .rows(&format!("SHOW GRANTS FOR {grantee}"))
        .await
        .unwrap()
        .into_iter()
        .filter_map(|r| r.into_iter().next().and_then(|(_, v)| v))
        .collect();
    s.close().await;
    v.sort();
    v
}

/// A tenant with one shard schema, a user and a role on the cluster.
struct Tenant {
    id: TenantId,
    schema: String,
    user: String,
    role: String,
}

impl Tenant {
    fn new() -> Self {
        let id = random_tenant();
        Self {
            id,
            schema: format!("t{id}_d1_g1_s5"),
            user: format!("t{id}_u1"),
            role: format!("t{id}_r1"),
        }
    }

    fn user_grantee(&self) -> String {
        format!("{}@'%'", literal(&self.user))
    }

    async fn create(&self, writer: &NodeEndpoint) {
        let s = &self.schema;
        for sql in [
            format!("CREATE DATABASE `{s}`"),
            format!("CREATE TABLE `{s}`.x (id INT PRIMARY KEY, v INT) ENGINE=InnoDB"),
            format!("CREATE TABLE `{s}`.y (id INT PRIMARY KEY) ENGINE=InnoDB"),
            format!("INSERT INTO `{s}`.x VALUES (1, 1)"),
            format!(
                "CREATE USER {} IDENTIFIED BY {}",
                self.user_grantee(),
                literal(PASSWORD)
            ),
            format!(
                "GRANT SELECT, INSERT, UPDATE, DELETE ON `{s}`.* TO {}",
                self.user_grantee()
            ),
            format!("CREATE ROLE {}", literal(&self.role)),
            // Stored escaped in mysql.db, as a GRANT on a literal name can be.
            format!(
                "GRANT INSERT, DELETE ON `{}`.* TO {}",
                s.replace('_', "\\_"),
                literal(&self.role)
            ),
        ] {
            exec(writer, &sql).await;
        }
    }

    async fn drop(&self, writer: &NodeEndpoint) {
        let mut s = admin(writer).await;
        for sql in [
            format!("DROP DATABASE IF EXISTS `{}`", self.schema),
            format!("DROP USER IF EXISTS {}", self.user_grantee()),
            format!("DROP ROLE IF EXISTS {}", literal(&self.role)),
        ] {
            if let Err(e) = s.exec(&sql).await {
                eprintln!("cleanup: {sql}: {e}");
            }
        }
        s.close().await;
    }

    /// A tenant connection to `ep`, with the shard schema as its database.
    async fn connect(&self, ep: &NodeEndpoint, with_db: bool) -> mysql_async::Conn {
        let db = if with_db { self.schema.as_str() } else { "" };
        let url = format!("mysql://{}:{PASSWORD}@{}/{db}", self.user, ep.addr());
        // Users replicate to replicas asynchronously.
        let mut last = None;
        for _ in 0..50 {
            match mysql_async::Conn::new(mysql_async::Opts::from_url(&url).unwrap()).await {
                Ok(c) => return c,
                Err(e) => last = Some(e),
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("tenant login to {}: {last:?}", ep.addr());
    }

    fn request(&self, method: FenceMethod, scope: FenceScope) -> FenceRequest {
        FenceRequest {
            tenant: self.id,
            schemas: vec![self.schema.clone()],
            tables: None,
            scope,
            method,
            deadline: Instant::now() + Duration::from_secs(30),
        }
    }
}

/// Runs `body`, then `cleanup` even if `body` panicked.
async fn guarded<F, C>(body: F, cleanup: C)
where
    F: Future<Output = ()>,
    C: Future<Output = ()>,
{
    let r = AssertUnwindSafe(body).catch_unwind().await;
    cleanup.await;
    if let Err(p) = r {
        std::panic::resume_unwind(p);
    }
}

fn record(kind: TopologyKind, eps: &[NodeEndpoint]) -> ClusterRecord {
    ClusterRecord {
        cluster_id: ClusterId::new("dc-a-c9").unwrap(),
        cluster_number: 9,
        name: "fence-test".into(),
        dc_id: DcId::new("dc-a").unwrap(),
        topology: kind,
        provisioner: ProvisionerKind::External,
        admin_state: AdminState::Active,
        nodes: eps
            .iter()
            .map(|e| NodeSpec {
                node: e.node.clone(),
                addr: e.addr(),
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
        node_identities: std::collections::BTreeMap::new(),
    }
}

/// A driver fed with one full probe pass.
async fn driver(kind: TopologyKind, eps: &[NodeEndpoint]) -> Arc<dyn TopologyDriver> {
    let connector: Arc<dyn NodeConnector> = Arc::new(StaticConnector::new(eps.iter().cloned()));
    let d: Arc<dyn TopologyDriver> = match kind {
        TopologyKind::Galera => Arc::new(GaleraDriver::new(connector, ClusterSettings::default())),
        k => Arc::new(BasicDriver::new(k, connector, ClusterSettings::default())),
    };
    let rec = record(kind, eps);
    let nodes = d.discover(&rec).await.unwrap();
    assert_eq!(nodes.len(), eps.len());
    d.observe(&rec);
    let probes = Prober::new(Duration::from_secs(10))
        .probe_cluster(kind, eps, ProbeDepth::Full)
        .await;
    for p in &probes {
        assert!(p.result.is_ok(), "{}: {:?}", p.node, p.result);
    }
    let profile = d.derive(&probes, None);
    let h = d.health(&probes, &profile);
    assert_eq!(h, ClusterHealth::Healthy);
    d
}

async fn insert(c: &mut mysql_async::Conn, schema: &str, id: u32) -> Result<(), u16> {
    c.query_drop(format!("INSERT INTO `{schema}`.x VALUES ({id}, {id})"))
        .await
        .map_err(|e| match e {
            mysql_async::Error::Server(s) => s.code,
            _ => 0,
        })
}

#[tokio::test]
async fn revoke_kill_stops_an_open_transaction_and_unfence_restores_grants() {
    let eps = topology_or_skip!(skip::MARIADB_SINGLE);
    let t = Tenant::new();
    let w = eps[0].clone();
    t.create(&w).await;
    guarded(
        async {
            let d = driver(TopologyKind::SingleNode, &eps).await;
            let user_before = grants(&w, &t.user_grantee()).await;
            let role_before = grants(&w, &literal(&t.role)).await;

            // An open transaction in the schema, and an idle session elsewhere.
            let mut writing = t.connect(&w, true).await;
            writing.query_drop("BEGIN").await.unwrap();
            insert(&mut writing, &t.schema, 2).await.unwrap();
            let mut idle = t.connect(&w, false).await;
            idle.query_drop("SELECT 1").await.unwrap();

            let started = Instant::now();
            let receipt = d
                .fence_shard(&t.request(FenceMethod::RevokeKill, FenceScope::Writes))
                .await
                .unwrap();
            println!(
                "fenced in {} ms: {}",
                started.elapsed().as_millis(),
                serde_json::to_string(&receipt).unwrap()
            );
            assert_eq!(receipt.method, FenceMethod::RevokeKill);
            assert!(!receipt.writer_pos.0.is_empty());
            let user = receipt.revoked.iter().find(|g| g.user == t.user).unwrap();
            assert_eq!(user.privileges, ["INSERT", "UPDATE", "DELETE"]);
            let role = receipt.revoked.iter().find(|g| g.user == t.role).unwrap();
            assert_eq!(role.privileges, ["INSERT", "DELETE"]);
            let killed: Vec<u64> = receipt.killed.iter().map(|(_, id)| *id).collect();
            assert!(killed.contains(&u64::from(writing.id())), "{killed:?}");
            assert!(!killed.contains(&u64::from(idle.id())), "{killed:?}");

            // The open transaction is gone and its write never commits.
            assert!(writing.query_drop("COMMIT").await.is_err());
            let n = scalar(&w, &format!("SELECT COUNT(*) FROM `{}`.x", t.schema)).await;
            assert_eq!(n.as_deref(), Some("1"));
            // Surviving and new sessions can read but not write.
            assert_eq!(insert(&mut idle, &t.schema, 3).await, Err(1142));
            let mut fresh = t.connect(&w, true).await;
            assert_eq!(insert(&mut fresh, &t.schema, 3).await, Err(1142));
            let v: Option<u32> = fresh
                .query_first("SELECT v FROM x WHERE id = 1")
                .await
                .unwrap();
            assert_eq!(v, Some(1));
            drop(fresh);

            // Unfence restores exactly the grants there were.
            d.unfence_shard(&receipt).await.unwrap();
            assert_eq!(grants(&w, &t.user_grantee()).await, user_before);
            assert_eq!(grants(&w, &literal(&t.role)).await, role_before);
            let mut again = t.connect(&w, true).await;
            insert(&mut again, &t.schema, 4).await.unwrap();
            // Replaying a receipt twice is harmless.
            d.unfence_shard(&receipt).await.unwrap();
            assert_eq!(grants(&w, &t.user_grantee()).await, user_before);
        },
        t.drop(&w),
    )
    .await;
}

#[tokio::test]
async fn rename_waits_for_lock_holders_then_fences_and_unfences() {
    let eps = topology_or_skip!(skip::MARIADB_SINGLE);
    let t = Tenant::new();
    let w = eps[0].clone();
    t.create(&w).await;
    guarded(
        async {
            let opts = FenceOptions::default();
            let req = t.request(FenceMethod::Rename, FenceScope::Writes);

            // An open transaction holds a metadata lock: the rename gives
            // up after cluster.fence_lock_wait_seconds and changes nothing.
            let mut holder = t.connect(&w, true).await;
            holder.query_drop("BEGIN").await.unwrap();
            insert(&mut holder, &t.schema, 2).await.unwrap();
            let started = Instant::now();
            let e = fence(&req, &w, &[], &opts).await.unwrap_err();
            assert_eq!(e.code(), Some(SprigCode::ClusterFenceFailed), "{e}");
            assert!(started.elapsed() < Duration::from_secs(5));
            let tables = scalar(
                &w,
                &format!(
                    "SELECT GROUP_CONCAT(TABLE_NAME ORDER BY TABLE_NAME) \
                     FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{}'",
                    t.schema
                ),
            )
            .await;
            assert_eq!(tables.as_deref(), Some("x,y"));
            holder.query_drop("COMMIT").await.unwrap();

            // Now it goes through, atomically, for every table.
            let receipt = fence(&req, &w, &[], &opts).await.unwrap();
            assert_eq!(receipt.renamed.len(), 2);
            assert!(receipt.revoked.is_empty() && receipt.killed.is_empty());
            assert!(
                receipt.writer_pos.0.split('-').count() == 3,
                "the rename's own GTID: {}",
                receipt.writer_pos
            );
            assert_eq!(insert(&mut holder, &t.schema, 3).await, Err(1146));
            let fenced = scalar(
                &w,
                &format!(
                    "SELECT COUNT(*) FROM information_schema.TABLES \
                     WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}'",
                    t.schema,
                    fenced_name("x")
                ),
            )
            .await;
            assert_eq!(fenced.as_deref(), Some("1"));
            // Fencing again finds nothing left to rename.
            let twice = fence(&req, &w, &[], &opts).await.unwrap();
            assert!(twice.renamed.is_empty());

            unfence(&receipt, &w, &opts).await.unwrap();
            insert(&mut holder, &t.schema, 3).await.unwrap();
            unfence(&receipt, &w, &opts).await.unwrap(); // idempotent

            // Only some tables.
            let only_y = FenceRequest {
                tables: Some(vec!["y".into()]),
                ..t.request(FenceMethod::Rename, FenceScope::Writes)
            };
            let r = fence(&only_y, &w, &[], &opts).await.unwrap();
            assert_eq!(r.renamed.len(), 1);
            assert_eq!(r.renamed[0].from, "y");
            insert(&mut holder, &t.schema, 4).await.unwrap();
            unfence(&r, &w, &opts).await.unwrap();
        },
        t.drop(&w),
    )
    .await;
}

/// During a cutover something recreated tables under their old names
/// while a rename fence held (catalog migrations did, on a fenced
/// catalog). The unfence drops a recreation only if it is empty and was
/// created after the fence; otherwise it fails and drops nothing.
#[tokio::test]
async fn unfence_drops_only_empty_recreations_made_after_the_fence() {
    let eps = topology_or_skip!(skip::MARIADB_SINGLE);
    let t = Tenant::new();
    let w = eps[0].clone();
    t.create(&w).await;
    let sc = t.schema.clone();
    guarded(
        async {
            let opts = FenceOptions::default();
            let req = t.request(FenceMethod::Rename, FenceScope::Writes);
            let count = |q: String| {
                let w = w.clone();
                async move { scalar(&w, &q).await }
            };
            let rows_in_x = format!("SELECT COUNT(*) FROM `{sc}`.x");

            // Empty recreations: dropped, and the real tables come back.
            let receipt = fence(&req, &w, &[], &opts).await.unwrap();
            assert!(receipt.fenced_at_unix.is_some());
            exec(
                &w,
                &format!("CREATE TABLE `{sc}`.x (id INT PRIMARY KEY, v INT)"),
            )
            .await;
            exec(&w, &format!("CREATE TABLE `{sc}`.y (id INT PRIMARY KEY)")).await;
            unfence(&receipt, &w, &opts).await.unwrap();
            assert_eq!(count(rows_in_x.clone()).await.as_deref(), Some("1"));
            let fenced_left = format!(
                "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{sc}' \
                 AND TABLE_NAME LIKE '\\_\\_sprig\\_f\\_%'"
            );
            assert_eq!(count(fenced_left.clone()).await.as_deref(), Some("0"));

            // A recreation with rows: nothing dropped, the unfence fails.
            let receipt = fence(&req, &w, &[], &opts).await.unwrap();
            exec(
                &w,
                &format!("CREATE TABLE `{sc}`.x (id INT PRIMARY KEY, v INT)"),
            )
            .await;
            exec(&w, &format!("INSERT INTO `{sc}`.x VALUES (7, 7)")).await;
            let e = unfence(&receipt, &w, &opts).await.unwrap_err();
            assert_eq!(e.code(), Some(SprigCode::ClusterFenceFailed), "{e}");
            assert!(e.to_string().contains("has rows"), "{e}");
            assert!(e.to_string().contains("its fenced name exist"), "{e}");
            assert_eq!(count(fenced_left.clone()).await.as_deref(), Some("2"));
            assert_eq!(
                count(rows_in_x.clone()).await.as_deref(),
                Some("1"),
                "the stray"
            );
            // A receipt without the fence time proves nothing either.
            exec(&w, &format!("DELETE FROM `{sc}`.x")).await;
            let old = sprig_cluster::FenceReceipt {
                fenced_at_unix: None,
                ..receipt.clone()
            };
            let e = unfence(&old, &w, &opts).await.unwrap_err();
            assert!(e.to_string().contains("no fence time"), "{e}");
            // With the fence time, the now empty stray goes.
            unfence(&receipt, &w, &opts).await.unwrap();
            assert_eq!(count(rows_in_x).await.as_deref(), Some("1"));
            assert_eq!(count(fenced_left).await.as_deref(), Some("0"));
        },
        t.drop(&w),
    )
    .await;
}

#[tokio::test]
async fn without_connection_admin_only_rename_remains() {
    let eps = topology_or_skip!(skip::MARIADB_SINGLE);
    let w = eps[0].clone();
    let t = Tenant::new();
    let ctl = unique_ident("fence_ctl");
    let ctl_grantee = format!("{}@'%'", literal(&ctl));
    t.create(&w).await;
    exec(
        &w,
        &format!(
            "CREATE USER {ctl_grantee} IDENTIFIED BY {}",
            literal(PASSWORD)
        ),
    )
    .await;
    guarded(
        async {
            for sql in [
                format!("GRANT ALL ON `t%`.* TO {ctl_grantee} WITH GRANT OPTION"),
                // sprig_ctl's global privileges, except CONNECTION ADMIN.
                format!(
                    "GRANT CREATE USER, PROCESS, SLAVE MONITOR, BINLOG MONITOR ON *.* \
                     TO {ctl_grantee}"
                ),
            ] {
                exec(&w, &sql).await;
            }
            let ctl_ep = NodeEndpoint::from_url(
                w.node.clone(),
                &format!("mysql://{ctl}:{PASSWORD}@{}", w.addr()),
            )
            .unwrap();
            let d = driver(TopologyKind::SingleNode, std::slice::from_ref(&ctl_ep)).await;
            let p = d.derive(
                &Prober::new(Duration::from_secs(10))
                    .probe_cluster(
                        TopologyKind::SingleNode,
                        std::slice::from_ref(&ctl_ep),
                        ProbeDepth::Full,
                    )
                    .await,
                None,
            );
            assert_eq!(p.derived.fence_methods, [FenceMethod::Rename]);

            // Asked for RevokeKill, the driver renames instead.
            let receipt = d
                .fence_shard(&t.request(FenceMethod::RevokeKill, FenceScope::Writes))
                .await
                .unwrap();
            assert_eq!(receipt.method, FenceMethod::Rename);
            assert_eq!(receipt.renamed.len(), 2);
            d.unfence_shard(&receipt).await.unwrap();

            exec(
                &w,
                &format!("GRANT CONNECTION ADMIN ON *.* TO {ctl_grantee}"),
            )
            .await;
            let p = d.derive(
                &Prober::new(Duration::from_secs(10))
                    .probe_cluster(
                        TopologyKind::SingleNode,
                        std::slice::from_ref(&ctl_ep),
                        ProbeDepth::Full,
                    )
                    .await,
                None,
            );
            assert_eq!(
                p.derived.fence_methods,
                [FenceMethod::RevokeKill, FenceMethod::Rename]
            );
        },
        async {
            exec(&w, &format!("DROP USER IF EXISTS {ctl_grantee}")).await;
            t.drop(&w).await;
        },
    )
    .await;
}

#[tokio::test]
async fn replication_all_scope_kills_replica_readers() {
    let eps = topology_or_skip!(skip::MARIADB_REPLICATION);
    let t = Tenant::new();
    let w = eps[0].clone();
    t.create(&w).await;
    guarded(
        async {
            let d = driver(TopologyKind::PrimaryReplica, &eps).await;
            assert_eq!(d.writer(ShardId(5)).unwrap().node, w.node);
            let readers = d.readers(ShardId(5));
            assert_eq!(readers.len(), eps.len() - 1);
            assert!(readers.iter().all(|r| r.ryw == RywMethod::GtidWait));
            assert_eq!(d.binlog_source(SourcePref::Auto).unwrap().node, w.node);
            assert!(d.binlog_source(SourcePref::Replica).unwrap().node != w.node);
            assert!(d.validation_node().is_some());

            // A reader on a replica, in the schema.
            let r = eps[1].clone();
            let mut reader = t.connect(&r, true).await;
            let n: Option<u32> = reader.query_first("SELECT COUNT(*) FROM x").await.unwrap();
            assert_eq!(n, Some(1));

            let receipt = d
                .fence_shard(&t.request(FenceMethod::RevokeKill, FenceScope::All))
                .await
                .unwrap();
            assert!(
                receipt
                    .killed
                    .contains(&(r.node.clone(), u64::from(reader.id()))),
                "{:?}",
                receipt.killed
            );
            let user = receipt.revoked.iter().find(|g| g.user == t.user).unwrap();
            assert_eq!(user.privileges, ["INSERT", "UPDATE", "DELETE", "SELECT"]);
            assert!(reader.query_drop("SELECT 1").await.is_err());

            // The revoke replicated: the replica refuses the tenant too.
            let mut denied = false;
            for _ in 0..50 {
                if grants(&r, &t.user_grantee())
                    .await
                    .iter()
                    .all(|g| !g.contains(&t.schema))
                {
                    denied = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert!(denied, "revoke reached the replica");

            d.unfence_shard(&receipt).await.unwrap();
            let mut back = t.connect(&r, true).await;
            let n: Option<u32> = back.query_first("SELECT COUNT(*) FROM x").await.unwrap();
            assert_eq!(n, Some(1));
        },
        t.drop(&w),
    )
    .await;
}

#[tokio::test]
async fn galera_fence_kills_on_every_node() {
    let eps = topology_or_skip!(skip::MARIADB_GALERA);
    let t = Tenant::new();
    let w = eps[0].clone();
    t.create(&w).await;
    guarded(
        async {
            let d = driver(TopologyKind::Galera, &eps).await;
            let shard_writer = d.writer(ShardId(5)).unwrap().node;
            assert_eq!(d.readers(ShardId(5)).len(), eps.len());
            let src = d.binlog_source(SourcePref::Auto).unwrap();
            assert!(src.switchable && src.feed_level == FeedLevel::Full);
            assert!(d.validation_node().is_some());

            // An open transaction on a node other than the shard's writer.
            let other = eps.iter().find(|e| e.node != shard_writer).unwrap().clone();
            let mut writing = t.connect(&other, true).await;
            writing.query_drop("BEGIN").await.unwrap();
            insert(&mut writing, &t.schema, 2).await.unwrap();

            let receipt = d
                .fence_shard(&t.request(FenceMethod::RevokeKill, FenceScope::Writes))
                .await
                .unwrap();
            assert_eq!(receipt.writer, shard_writer);
            assert!(
                receipt
                    .killed
                    .contains(&(other.node.clone(), u64::from(writing.id()))),
                "{:?}",
                receipt.killed
            );
            assert!(writing.query_drop("COMMIT").await.is_err());
            let mut fresh = t.connect(&other, true).await;
            assert_eq!(insert(&mut fresh, &t.schema, 3).await, Err(1142));
            d.unfence_shard(&receipt).await.unwrap();
            let mut again = t.connect(&other, true).await;
            insert(&mut again, &t.schema, 3).await.unwrap();
        },
        t.drop(&w),
    )
    .await;
}

#[tokio::test]
async fn feed_requirements_hold_on_every_topology() {
    let mut seen = 0;
    for (var, kind) in [
        (skip::MARIADB_SINGLE, TopologyKind::SingleNode),
        (skip::MARIADB_REPLICATION, TopologyKind::PrimaryReplica),
        (skip::MARIADB_GALERA, TopologyKind::Galera),
    ] {
        let Some(eps) = endpoints(var) else { continue };
        seen += 1;
        let probes = Prober::new(Duration::from_secs(10))
            .probe_cluster(kind, &eps, ProbeDepth::Full)
            .await;
        for p in &probes {
            let f = p.facts().expect("probe answered");
            assert_eq!(f.binlog.annotate_row_events, Some(true), "{}", p.node);
            let c = feed::check(f, feed::DEFAULT_RETENTION_HOURS);
            assert_eq!(c.level, FeedLevel::Full, "{kind} {}: {:?}", p.node, c.gaps);
        }
        assert!(feed::report(&probes, feed::DEFAULT_RETENTION_HOURS).is_empty());
        let d = driver(kind, &eps).await;
        let profile = d.derive(&probes, None);
        assert!(profile.derived.feed_source);
        assert_eq!(profile.derived.feed_level, FeedLevel::Full);
    }
    if seen == 0 {
        skip::report_missing(skip::MARIADB_SINGLE, module_path!());
    }
}
