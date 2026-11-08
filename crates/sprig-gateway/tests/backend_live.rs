//! Backend pools, state replay, pins, read-your-writes, writer selection
//! and failure paths against the Compose stack (chapter 06). Each test is
//! skipped unless its variables are set:
//!
//! ```sh
//! export SPRIG_TEST_MARIADB_SINGLE=mysql://root:sprig-dev-root@127.0.0.1:13301
//! export SPRIG_TEST_MARIADB_REPLICATION=mysql://root:sprig-dev-root@127.0.0.1:13311,mysql://root:sprig-dev-root@127.0.0.1:13312,mysql://root:sprig-dev-root@127.0.0.1:13313
//! export SPRIG_TEST_MARIADB_GALERA=mysql://root:sprig-dev-root@127.0.0.1:13321,mysql://root:sprig-dev-root@127.0.0.1:13322,mysql://root:sprig-dev-root@127.0.0.1:13323
//! export SPRIG_TEST_TOXIPROXY=http://127.0.0.1:18474
//! export SPRIG_TEST_DOCKER=1     # throwaway primary + replica for the lag test
//! cargo test -p sprig-gateway --test backend_live -- --nocapture
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    missing_docs
)]

use std::sync::Arc;
use std::time::Duration;

use sprig_auth::accounts::BackendTarget;
use sprig_cluster::record::TopologyKind;
use sprig_common::ids::{ClusterId, DcId, NodeId, ShardId};
use sprig_common::{SprigCode, SprigError};
use sprig_gateway::backend::settings::BackendSettings;
use sprig_gateway::backend::{
    BackendSession, Backends, ClusterView, FixedSettings, NodeHealth, NodeRef, NodeRole, RouteKind,
    SessionOpts, StaticClusters, StmtInfo, StmtOutcome,
};
use sprig_proto_mysql::client::{ClientConfig, MysqlClient, PacketKind, TlsMode, TlsOptions};
use sprig_proto_mysql::{ErrPacket, Persona};
use sprig_testkit::faults::compose::OwnContainer;
use sprig_testkit::faults::toxiproxy::{Toxic, Toxiproxy};
use sprig_testkit::util::TempDir;
use sprig_testkit::{skip, unique_ident};

struct Url {
    user: String,
    pass: String,
    addr: String,
}

fn urls(var: &str) -> Option<Vec<Url>> {
    let v = skip::var(var)?;
    Some(
        skip::urls(&v)
            .into_iter()
            .map(|u| {
                let rest = u.strip_prefix("mysql://").unwrap();
                let (cred, addr) = rest.rsplit_once('@').unwrap();
                let (user, pass) = cred.split_once(':').unwrap_or((cred, ""));
                Url {
                    user: user.into(),
                    pass: pass.into(),
                    addr: addr.trim_end_matches('/').into(),
                }
            })
            .collect(),
    )
}

macro_rules! need {
    ($var:expr, $name:literal) => {
        match urls($var) {
            Some(u) => u,
            None => {
                skip::report_missing($var, $name);
                return;
            }
        }
    };
}

fn fast_settings() -> BackendSettings {
    BackendSettings {
        health_interval: Duration::from_millis(200),
        health_failures_to_down: 2,
        position_poll: Duration::from_millis(50),
        gtid_wait_timeout: Duration::from_millis(2000),
        max_replica_lag: Duration::from_secs(10),
        ..BackendSettings::default()
    }
}

struct Rig {
    backends: Arc<Backends>,
    settings: Arc<FixedSettings>,
    target: BackendTarget,
    view: ClusterView,
    _dir: TempDir,
}

/// Backends over one cluster of `addrs`, logging in as `user`.
fn rig(
    topology: TopologyKind,
    user: &str,
    pass: &str,
    addrs: &[String],
    s: BackendSettings,
) -> Rig {
    let cluster = ClusterId::new(sprig_testkit::unique_name("bt")).unwrap();
    let nodes = addrs
        .iter()
        .enumerate()
        .map(|(i, a)| NodeRef {
            node: NodeId::new(format!("n{}", i + 1)).unwrap(),
            addr: a.clone(),
        })
        .collect();
    let view = ClusterView::new(cluster.clone(), topology, nodes);
    let clusters = StaticClusters::new();
    clusters.insert(view.clone());
    let settings = FixedSettings::new(s);
    let backends = Backends::new(DcId::new("dc-a").unwrap(), clusters, settings.clone());
    backends.start();
    let dir = TempDir::new("sprig-bt").unwrap();
    let cred = dir.join("cred.toml");
    std::fs::write(
        &cred,
        format!("user = \"{user}\"\npassword = \"{pass}\"\ntls = \"skip_verify\"\n"),
    )
    .unwrap();
    let target = BackendTarget {
        cluster,
        user: user.into(),
        credentials: format!("file:{}", cred.display()).parse().unwrap(),
    };
    Rig {
        backends,
        settings,
        target,
        view,
        _dir: dir,
    }
}

impl Rig {
    fn session(&self, persona: Persona) -> BackendSession {
        self.backends.session(
            self.target.clone(),
            SessionOpts {
                persona,
                ..SessionOpts::default()
            },
        )
    }

    fn node(&self, i: usize) -> sprig_gateway::backend::NodeKey {
        self.view.key(&self.view.nodes[i].node)
    }

    async fn wait_for(&self, what: &str, timeout: Duration, f: impl Fn(&Rig) -> bool) {
        let end = tokio::time::Instant::now() + timeout;
        while !f(self) {
            assert!(
                tokio::time::Instant::now() < end,
                "timed out waiting for {what}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn wait_roles(&self, roles: &[NodeRole]) {
        self.wait_for("node roles", Duration::from_secs(20), |r| {
            roles
                .iter()
                .enumerate()
                .all(|(i, role)| r.backends.nodes().status(&r.node(i)).role() == *role)
        })
        .await;
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        self.backends.shutdown();
    }
}

#[derive(Debug)]
struct Res {
    rows: Vec<Vec<Option<String>>>,
    out: StmtOutcome,
}

impl Res {
    fn cell(&self, r: usize, c: usize) -> Option<&str> {
        self.rows[r][c].as_deref()
    }
    fn err(&self) -> Option<&ErrPacket> {
        self.out.error.as_ref()
    }
}

async fn run_as(bs: &mut BackendSession, info: &StmtInfo, sql: &str) -> Result<Res, SprigError> {
    bs.query(info, sql.as_bytes()).await?;
    let mut ncols = 0;
    let mut rows = Vec::new();
    while let Some(p) = bs.next().await? {
        match p.kind {
            PacketKind::ColumnCount(n) => ncols = usize::try_from(n).unwrap(),
            PacketKind::Row => rows.push(
                sprig_proto_mysql::resultset::decode_text_row(&p.payload, ncols)
                    .unwrap()
                    .into_iter()
                    .map(|v| v.map(|b| String::from_utf8_lossy(b).into_owned()))
                    .collect(),
            ),
            _ => {}
        }
    }
    let out = bs.finish().await?;
    Ok(Res { rows, out })
}

async fn run(bs: &mut BackendSession, sql: &str) -> Result<Res, SprigError> {
    run_as(bs, &StmtInfo::classify(sql.as_bytes()), sql).await
}

/// Runs `sql` and fails the test on a server error.
async fn ok(bs: &mut BackendSession, sql: &str) -> Res {
    let r = run(bs, sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert!(r.err().is_none(), "{sql}: {:?}", r.err());
    r
}

fn code(e: &SprigError) -> u16 {
    e.protocol().mysql_errno
}

/// A side connection, for KILL and checks behind the gateway's back.
async fn side(u: &Url) -> MysqlClient {
    let mut c = ClientConfig::new(u.user.clone(), u.pass.clone());
    c.tls = TlsMode::Required;
    c.tls_config = Some(
        TlsOptions {
            skip_verify: true,
            ..TlsOptions::default()
        }
        .build()
        .unwrap(),
    );
    c.connect_timeout = Duration::from_secs(10);
    MysqlClient::connect(&u.addr, &c).await.unwrap()
}

#[tokio::test]
async fn pool_reuse_replays_session_state() {
    let u = need!(skip::MARIADB_SINGLE, "pool_reuse_replays_session_state");
    let s = BackendSettings {
        max_conns_per_account_node: 1,
        ..fast_settings()
    };
    let rig = rig(
        TopologyKind::SingleNode,
        &u[0].user,
        &u[0].pass,
        &[u[0].addr.clone()],
        s,
    );
    let db = unique_ident("sprig_bt");
    let mut a = rig.session(Persona::MariaDb);
    ok(&mut a, &format!("CREATE DATABASE {db}")).await;
    ok(&mut a, &format!("USE {db}")).await;
    assert_eq!(a.vars().schema.as_deref(), Some(db.as_str()));
    ok(
        &mut a,
        "CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY, v INT)",
    )
    .await;
    ok(
        &mut a,
        "SET time_zone = '+03:00', SESSION sql_safe_updates = 1",
    )
    .await;
    ok(&mut a, "SET @a = 41, @s = 'it''s'").await;
    let ins = ok(&mut a, "INSERT INTO t (v) VALUES (1), (2)").await;
    assert_eq!(ins.out.last_insert_id, 1);
    ok(&mut a, "SET NAMES latin1").await;
    assert!(!a.is_pinned());
    let conn_a = ok(&mut a, "SELECT CONNECTION_ID()")
        .await
        .cell(0, 0)
        .unwrap()
        .to_owned();

    // Another session (logged in with a schema) on the same, only
    // connection sees none of a's state.
    let mut b = rig.backends.session(
        rig.target.clone(),
        SessionOpts {
            persona: Persona::MySql84,
            schema: Some(db.clone()),
            ..SessionOpts::default()
        },
    );
    let r = ok(
        &mut b,
        "SELECT @@time_zone, DATABASE(), @a, LAST_INSERT_ID(), @@sql_mode, @@character_set_client, \
         @@sql_safe_updates, CONNECTION_ID()",
    )
    .await;
    assert_eq!(
        r.cell(0, 7),
        Some(conn_a.as_str()),
        "same backend connection"
    );
    assert_eq!(r.cell(0, 0), Some("SYSTEM"));
    assert_eq!(r.cell(0, 1), Some(db.as_str()));
    assert_eq!(r.cell(0, 2), None);
    assert_eq!(r.cell(0, 3), Some("0"));
    assert!(
        r.cell(0, 4).unwrap().contains("ONLY_FULL_GROUP_BY"),
        "MySQL persona baseline"
    );
    assert_eq!(r.cell(0, 5), Some("utf8mb4"));
    assert_eq!(r.cell(0, 6), Some("0"));
    // b assigns a user variable from an expression: captured before release.
    ok(&mut b, "SELECT @c := 7 * 6").await;
    assert_eq!(b.vars().user_vars.len(), 1);

    // a gets its state back on the shared connection.
    let r = ok(
        &mut a,
        "SELECT @@time_zone, DATABASE(), @a, @s, LAST_INSERT_ID(), @@character_set_client, \
         @@sql_safe_updates, @c, CONNECTION_ID()",
    )
    .await;
    assert_eq!(r.cell(0, 8), Some(conn_a.as_str()));
    assert_eq!(r.cell(0, 0), Some("+03:00"));
    assert_eq!(r.cell(0, 1), Some(db.as_str()));
    assert_eq!(r.cell(0, 2), Some("41"));
    assert_eq!(r.cell(0, 3), Some("it's"));
    assert_eq!(r.cell(0, 4), Some("1"));
    assert_eq!(r.cell(0, 5), Some("latin1"));
    assert_eq!(r.cell(0, 6), Some("1"));
    assert_eq!(r.cell(0, 7), None, "b's @c doesn't leak");
    // And b keeps its own @c.
    let r = ok(&mut b, "SELECT @c, @@time_zone").await;
    assert_eq!(r.cell(0, 0), Some("42"));
    assert_eq!(r.cell(0, 1), Some("SYSTEM"));
    // Warnings and FOUND_ROWS are captured before release.
    ok(&mut a, "SET NAMES utf8mb4").await;
    let w = ok(&mut a, "SELECT CAST('x1' AS INT)").await;
    assert!(w.out.warnings > 0);
    assert!(!a.captured().warnings.is_empty());
    ok(&mut a, "SELECT SQL_CALC_FOUND_ROWS * FROM t LIMIT 1").await;
    assert_eq!(a.captured().found_rows, 2);
    // A session without a schema never runs on a connection that has one.
    let mut c = rig.session(Persona::MariaDb);
    let r = ok(&mut c, "SELECT DATABASE()").await;
    assert_eq!(r.cell(0, 0), None);
    ok(&mut a, &format!("DROP DATABASE {db}")).await;
    a.close().await;
    b.close().await;
    c.close().await;
}

#[tokio::test]
async fn pins_hold_one_connection_until_released() {
    let u = need!(
        skip::MARIADB_SINGLE,
        "pins_hold_one_connection_until_released"
    );
    let s = BackendSettings {
        max_conns_per_account_node: 1,
        acquire_timeout: Duration::from_millis(400),
        ..fast_settings()
    };
    let rig = rig(
        TopologyKind::SingleNode,
        &u[0].user,
        &u[0].pass,
        &[u[0].addr.clone()],
        s,
    );
    let db = unique_ident("sprig_bt");
    let mut a = rig.session(Persona::MariaDb);
    let mut b = rig.session(Persona::MariaDb);
    ok(&mut a, &format!("CREATE DATABASE {db}")).await;
    ok(&mut a, &format!("USE {db}")).await;
    ok(&mut a, "CREATE TABLE t (v INT)").await;

    let expect_starved = |r: Result<Res, SprigError>| {
        let e = r.expect_err("the only connection is pinned");
        assert_eq!(e.code(), Some(SprigCode::BackendPoolTimeout), "{e}");
    };

    // Temporary table.
    let r = ok(&mut a, "CREATE TEMPORARY TABLE tmp (x INT)").await;
    assert!(r.out.pinned);
    let id1 = ok(&mut a, "SELECT CONNECTION_ID()")
        .await
        .cell(0, 0)
        .unwrap()
        .to_owned();
    expect_starved(run(&mut b, "SELECT 1").await);
    ok(&mut a, "INSERT INTO tmp VALUES (1)").await;
    let r = ok(&mut a, "SELECT COUNT(*), CONNECTION_ID() FROM tmp").await;
    assert_eq!(r.cell(0, 0), Some("1"));
    assert_eq!(r.cell(0, 1), Some(id1.as_str()));
    ok(&mut a, "DROP TEMPORARY TABLE tmp").await;
    assert!(!a.is_pinned());
    ok(&mut b, "SELECT 1").await;

    // Transaction.
    ok(&mut a, "BEGIN").await;
    assert!(a.is_pinned());
    ok(&mut a, "INSERT INTO t VALUES (1)").await;
    expect_starved(run(&mut b, "SELECT 1").await);
    ok(&mut a, "COMMIT").await;
    assert!(!a.is_pinned());
    assert!(!a.causal().is_empty(), "the commit's GTID is tracked");

    // autocommit = 0: the first statement opens a transaction.
    ok(&mut a, "SET autocommit = 0").await;
    assert!(!a.is_pinned());
    ok(&mut a, "SELECT * FROM t").await;
    assert!(a.is_pinned());
    ok(&mut a, "COMMIT").await;
    ok(&mut a, "SET autocommit = 1").await;
    assert!(!a.is_pinned());

    // GET_LOCK.
    ok(&mut a, "SELECT GET_LOCK('sprig_bt_lock', 1)").await;
    assert!(a.is_pinned());
    expect_starved(run(&mut b, "SELECT 1").await);
    ok(&mut a, "SELECT RELEASE_LOCK('sprig_bt_lock')").await;
    assert!(!a.is_pinned());

    // Untracked state pins for the rest of the session.
    ok(&mut a, "SET timestamp = 1000").await;
    assert!(a.is_pinned());
    let r = ok(&mut a, "SELECT UNIX_TIMESTAMP()").await;
    assert_eq!(r.cell(0, 0), Some("1000"));
    a.close().await;
    // Closing released the connection (discarded).
    ok(&mut b, &format!("DROP DATABASE {db}")).await;
    b.close().await;
}

#[tokio::test]
async fn writer_selection_per_topology() {
    if let Some(u) = urls(skip::MARIADB_SINGLE) {
        let rig = rig(
            TopologyKind::SingleNode,
            &u[0].user,
            &u[0].pass,
            &[u[0].addr.clone()],
            fast_settings(),
        );
        let w = rig
            .backends
            .writer(&rig.target.cluster, ShardId(0))
            .await
            .unwrap();
        assert_eq!(w.node.as_str(), "n1");
    }
    if let Some(u) = urls(skip::MARIADB_REPLICATION) {
        // Replicas first in the record: the checks must still find the primary.
        let addrs = vec![u[1].addr.clone(), u[0].addr.clone(), u[2].addr.clone()];
        let rig = rig(
            TopologyKind::PrimaryReplica,
            &u[0].user,
            &u[0].pass,
            &addrs,
            fast_settings(),
        );
        let mut s = rig.session(Persona::MariaDb);
        ok(&mut s, "SELECT 1").await; // starts the monitor
        rig.wait_roles(&[NodeRole::Replica, NodeRole::Writer, NodeRole::Replica])
            .await;
        let w = rig
            .backends
            .writer(&rig.target.cluster, ShardId(0))
            .await
            .unwrap();
        assert_eq!(w.addr, u[0].addr, "the primary");
        let r = run_as(&mut s, &StmtInfo::write(), "SELECT @@server_id")
            .await
            .unwrap();
        assert_eq!(r.out.node.node.as_str(), "n2", "writes go to the primary");
        s.close().await;
    }
    if let Some(u) = urls(skip::MARIADB_GALERA) {
        let addrs: Vec<String> = u.iter().map(|x| x.addr.clone()).collect();
        let rig = rig(
            TopologyKind::Galera,
            &u[0].user,
            &u[0].pass,
            &addrs,
            fast_settings(),
        );
        let mut s = rig.session(Persona::MariaDb);
        ok(&mut s, "SELECT 1").await;
        rig.wait_roles(&[NodeRole::Galera, NodeRole::Galera, NodeRole::Galera])
            .await;
        let mut writers = std::collections::HashSet::new();
        for shard in 0..20 {
            let w = rig
                .backends
                .writer(&rig.target.cluster, ShardId(shard))
                .await
                .unwrap();
            let expect = sprig_cluster::driver::galera::choose_writer(
                ShardId(shard),
                &rig.view.galera_writers,
            )
            .unwrap();
            assert_eq!(w.node, expect.node, "rendezvous");
            writers.insert(w.node);
        }
        assert!(writers.len() > 1, "shards spread over Galera nodes");
        s.close().await;
    }
}

#[tokio::test]
async fn read_your_writes_on_primary_and_replicas() {
    let u = need!(
        skip::MARIADB_REPLICATION,
        "read_your_writes_on_primary_and_replicas"
    );
    let addrs: Vec<String> = u.iter().map(|x| x.addr.clone()).collect();
    let rig = rig(
        TopologyKind::PrimaryReplica,
        &u[0].user,
        &u[0].pass,
        &addrs,
        fast_settings(),
    );
    let db = unique_ident("sprig_bt");
    let mut a = rig.session(Persona::MariaDb);
    ok(&mut a, &format!("CREATE DATABASE {db}")).await;
    ok(&mut a, &format!("CREATE TABLE {db}.t (v INT)")).await;
    rig.wait_roles(&[NodeRole::Writer, NodeRole::Replica, NodeRole::Replica])
        .await;
    let mut b = rig.session(Persona::MariaDb);
    let mut replica_reads = 0;
    for i in 1..=40 {
        ok(&mut a, &format!("INSERT INTO {db}.t VALUES ({i})")).await;
        let r = ok(&mut a, &format!("SELECT MAX(v) FROM {db}.t")).await;
        assert_eq!(
            r.cell(0, 0),
            Some(i.to_string().as_str()),
            "read {i} missed the write"
        );
        if matches!(r.out.route, RouteKind::Replica | RouteKind::ReplicaWait) {
            replica_reads += 1;
        }
        // Another session of the same tenant through this gateway too.
        let r = ok(&mut b, &format!("SELECT MAX(v) FROM {db}.t")).await;
        assert_eq!(
            r.cell(0, 0),
            Some(i.to_string().as_str()),
            "watermark read {i}"
        );
    }
    eprintln!("replica reads: {replica_reads}/40");
    assert!(replica_reads > 20, "reads left the writer: {replica_reads}");
    ok(&mut a, &format!("DROP DATABASE {db}")).await;
    a.close().await;
    b.close().await;
}

/// With `autocommit = 0` a plain read opens a transaction, so it runs on
/// the writer and pins there (chapter 06, "Pins"); before the fix it went
/// to a replica, opened the transaction there, and the gateway discarded
/// that connection and pinned the session as untracked for good.
#[tokio::test]
async fn autocommit_off_reads_stay_on_the_writer() {
    let u = need!(
        skip::MARIADB_REPLICATION,
        "autocommit_off_reads_stay_on_the_writer"
    );
    let addrs: Vec<String> = u.iter().map(|x| x.addr.clone()).collect();
    let rig = rig(
        TopologyKind::PrimaryReplica,
        &u[0].user,
        &u[0].pass,
        &addrs,
        fast_settings(),
    );
    let db = unique_ident("sprig_bt");
    let mut a = rig.session(Persona::MariaDb);
    ok(&mut a, &format!("CREATE DATABASE {db}")).await;
    ok(&mut a, &format!("CREATE TABLE {db}.t (v INT)")).await;
    rig.wait_roles(&[NodeRole::Writer, NodeRole::Replica, NodeRole::Replica])
        .await;
    // Plain reads leave the writer while autocommit is on.
    let mut replica = false;
    for _ in 0..20 {
        let r = ok(&mut a, &format!("SELECT COUNT(*) FROM {db}.t")).await;
        replica |= matches!(r.out.route, RouteKind::Replica | RouteKind::ReplicaWait);
    }
    assert!(replica, "no read left the writer with autocommit on");

    ok(&mut a, "SET autocommit = 0").await;
    let r = ok(&mut a, &format!("SELECT COUNT(*) FROM {db}.t")).await;
    assert_eq!(
        r.out.route,
        RouteKind::Writer,
        "first read of the implicit transaction"
    );
    assert!(r.out.pinned, "the implicit transaction pins the connection");
    let writer = r.out.node.clone();
    ok(&mut a, &format!("INSERT INTO {db}.t VALUES (1)")).await;
    let r = ok(&mut a, &format!("SELECT COUNT(*) FROM {db}.t")).await;
    assert_eq!(r.cell(0, 0), Some("1"));
    assert_eq!(
        r.out.node, writer,
        "the transaction stays on one connection"
    );
    ok(&mut a, "COMMIT").await;
    // Still autocommit = 0: the next read opens the next transaction there.
    let r = ok(&mut a, &format!("SELECT COUNT(*) FROM {db}.t")).await;
    assert_eq!(r.out.route, RouteKind::Writer);
    ok(&mut a, "ROLLBACK").await;

    ok(&mut a, "SET autocommit = 1").await;
    let mut replica = false;
    for _ in 0..20 {
        let r = ok(&mut a, &format!("SELECT COUNT(*) FROM {db}.t")).await;
        assert!(!r.out.pinned, "nothing pins once autocommit is back on");
        replica |= matches!(r.out.route, RouteKind::Replica | RouteKind::ReplicaWait);
    }
    assert!(replica, "reads leave the writer again with autocommit on");
    ok(&mut a, &format!("DROP DATABASE {db}")).await;
    a.close().await;
}

#[tokio::test]
async fn read_your_writes_on_galera() {
    let u = need!(skip::MARIADB_GALERA, "read_your_writes_on_galera");
    let addrs: Vec<String> = u.iter().map(|x| x.addr.clone()).collect();
    let rig = rig(
        TopologyKind::Galera,
        &u[0].user,
        &u[0].pass,
        &addrs,
        fast_settings(),
    );
    let db = unique_ident("sprig_bt");
    let mut a = rig.session(Persona::MariaDb);
    ok(&mut a, &format!("CREATE DATABASE {db}")).await;
    ok(
        &mut a,
        &format!("CREATE TABLE {db}.t (id INT PRIMARY KEY AUTO_INCREMENT, v INT)"),
    )
    .await;
    rig.wait_roles(&[NodeRole::Galera, NodeRole::Galera, NodeRole::Galera])
        .await;
    let mut sync_waits = 0;
    let mut nodes = std::collections::HashSet::new();
    let mut b = rig.session(Persona::MariaDb);
    for i in 1..=30 {
        ok(&mut a, &format!("INSERT INTO {db}.t (v) VALUES ({i})")).await;
        let r = ok(
            &mut b,
            &format!("SELECT MAX(v), @@wsrep_sync_wait FROM {db}.t"),
        )
        .await;
        assert_eq!(
            r.cell(0, 0),
            Some(i.to_string().as_str()),
            "read {i} missed the write"
        );
        if r.out.route == RouteKind::GaleraSyncWait {
            sync_waits += 1;
            assert_eq!(r.cell(0, 1), Some("1"));
        }
        nodes.insert(r.out.node.node.clone());
    }
    eprintln!("sync waits: {sync_waits}/30 over {} nodes", nodes.len());
    assert!(sync_waits > 0, "reads off the writer use wsrep_sync_wait");
    ok(&mut a, &format!("DROP DATABASE {db}")).await;
    a.close().await;
    b.close().await;
}

async fn kill(u: &Url, thread: &str) {
    let mut c = side(u).await;
    let _ = c.query_drop(&format!("KILL CONNECTION {thread}")).await;
    c.quit().await;
}

#[tokio::test]
async fn failures_mid_statement() {
    let u = need!(skip::MARIADB_SINGLE, "failures_mid_statement");
    let s = BackendSettings {
        max_conns_per_account_node: 1,
        ..fast_settings()
    };
    let rig = rig(
        TopologyKind::SingleNode,
        &u[0].user,
        &u[0].pass,
        &[u[0].addr.clone()],
        s,
    );
    let db = unique_ident("sprig_bt");
    let mut a = rig.session(Persona::MariaDb);
    ok(&mut a, &format!("CREATE DATABASE {db}")).await;
    ok(&mut a, &format!("USE {db}")).await;
    ok(&mut a, "CREATE TABLE t (v INT)").await;
    ok(&mut a, "INSERT INTO t VALUES (1)").await;

    // A plain read whose connection is killed before any result: retried.
    let id = ok(&mut a, "SELECT CONNECTION_ID()")
        .await
        .cell(0, 0)
        .unwrap()
        .to_owned();
    let killer = {
        let u0 = Url {
            user: u[0].user.clone(),
            pass: u[0].pass.clone(),
            addr: u[0].addr.clone(),
        };
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            kill(&u0, &id).await;
        })
    };
    let r = run_as(&mut a, &StmtInfo::read(), "SELECT SLEEP(2), v FROM t")
        .await
        .unwrap();
    killer.await.unwrap();
    assert!(r.err().is_none(), "retried: {:?}", r.err());
    assert_eq!(r.cell(0, 1), Some("1"));

    // An autocommit write killed mid-statement: the server's error or 39032.
    let id = ok(&mut a, "SELECT CONNECTION_ID()")
        .await
        .cell(0, 0)
        .unwrap()
        .to_owned();
    let killer = {
        let u0 = Url {
            user: u[0].user.clone(),
            pass: u[0].pass.clone(),
            addr: u[0].addr.clone(),
        };
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            kill(&u0, &id).await;
        })
    };
    match run(&mut a, "UPDATE t SET v = v + SLEEP(2)").await {
        Err(e) => assert_eq!(e.code(), Some(SprigCode::OutcomeUnknown), "{e}"),
        Ok(r) => assert!(r.err().is_some_and(|e| e.code == 1927), "{r:?}"),
    }
    killer.await.unwrap();
    ok(&mut a, "SELECT 1").await;

    // Lost inside a transaction: 1213, rolled back, unpinned.
    ok(&mut a, "BEGIN").await;
    ok(&mut a, "INSERT INTO t VALUES (2)").await;
    let id = ok(&mut a, "SELECT CONNECTION_ID()")
        .await
        .cell(0, 0)
        .unwrap()
        .to_owned();
    kill(&u[0], &id).await;
    let e = run(&mut a, "INSERT INTO t VALUES (3)").await.unwrap_err();
    assert_eq!(code(&e), 1213, "{e}");
    assert!(e.retryable);
    assert!(!a.is_pinned());
    let r = ok(&mut a, "SELECT COUNT(*) FROM t").await;
    assert_eq!(r.cell(0, 0), Some("1"), "the transaction rolled back");

    // Lost while holding a temporary table: 39033.
    ok(&mut a, "CREATE TEMPORARY TABLE tmp (x INT)").await;
    let id = ok(&mut a, "SELECT CONNECTION_ID()")
        .await
        .cell(0, 0)
        .unwrap()
        .to_owned();
    kill(&u[0], &id).await;
    let e = run(&mut a, "SELECT * FROM tmp").await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::SessionStateLost), "{e}");
    assert!(!a.is_pinned());
    ok(&mut a, &format!("DROP DATABASE {db}")).await;
    a.close().await;
}

fn toxiproxy() -> Option<Toxiproxy> {
    skip::var(skip::TOXIPROXY).map(|u| Toxiproxy::new(&u).unwrap())
}

#[tokio::test]
async fn network_cut_mid_result() {
    let u = need!(skip::MARIADB_SINGLE, "network_cut_mid_result");
    let Some(tp) = toxiproxy() else {
        skip::report_missing(skip::TOXIPROXY, "network_cut_mid_result");
        return;
    };
    let upstream = skip::var(skip::TOXIPROXY_UPSTREAM).unwrap_or_else(|| "single:3306".into());
    let proxy = tp.own_proxy("bt-cut", &upstream).await.unwrap();
    let rig = rig(
        TopologyKind::SingleNode,
        &u[0].user,
        &u[0].pass,
        &[proxy.host_addr().to_owned()],
        fast_settings(),
    );
    let mut a = rig.session(Persona::MariaDb);
    ok(&mut a, "SELECT 1").await;
    a.query(
        &StmtInfo::read(),
        b"SELECT seq, REPEAT('x', 200) FROM mysql.seq_1_to_5000000",
    )
    .await
    .unwrap();
    let mut rows = 0;
    let mut err = None;
    loop {
        match a.next().await {
            Ok(Some(p)) => {
                if p.kind == PacketKind::Row {
                    rows += 1;
                    if rows == 1000 {
                        proxy.api().set_enabled(proxy.name(), false).await.unwrap();
                    }
                }
            }
            Ok(None) => break,
            Err(e) => {
                err = Some(e);
                break;
            }
        }
    }
    let e = err.expect("the cut ends the result with an error");
    assert_eq!(
        e.code(),
        Some(SprigCode::BackendLost),
        "{e} after {rows} rows"
    );
    let _ = a.finish().await;
    // The node goes Down; back Up once the link returns.
    rig.wait_for("node down", Duration::from_secs(10), |r| {
        r.backends.nodes().status(&r.node(0)).health == NodeHealth::Down
    })
    .await;
    let e = run(&mut a, "SELECT 1").await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::BackendUnavailable), "{e}");
    proxy.api().set_enabled(proxy.name(), true).await.unwrap();
    rig.wait_for("node up", Duration::from_secs(10), |r| {
        r.backends.nodes().status(&r.node(0)).health == NodeHealth::Up
    })
    .await;
    ok(&mut a, "SELECT 1").await;
    a.close().await;
}

#[tokio::test]
async fn replica_down_reads_fail_over_to_the_writer() {
    let u = need!(
        skip::MARIADB_REPLICATION,
        "replica_down_reads_fail_over_to_the_writer"
    );
    let Some(tp) = toxiproxy() else {
        skip::report_missing(
            skip::TOXIPROXY,
            "replica_down_reads_fail_over_to_the_writer",
        );
        return;
    };
    let proxy = tp.own_proxy("bt-replica", "replica-1:3306").await.unwrap();
    let addrs = vec![u[0].addr.clone(), proxy.host_addr().to_owned()];
    let rig = rig(
        TopologyKind::PrimaryReplica,
        &u[0].user,
        &u[0].pass,
        &addrs,
        fast_settings(),
    );
    let mut a = rig.session(Persona::MariaDb);
    ok(&mut a, "SELECT 1").await;
    rig.wait_roles(&[NodeRole::Writer, NodeRole::Replica]).await;
    rig.wait_for("replica caught up", Duration::from_secs(10), |r| {
        r.backends.nodes().status(&r.node(1)).lag == Some(Duration::ZERO)
    })
    .await;
    let r = ok(&mut a, "SELECT @@server_id").await;
    assert_eq!(r.out.route, RouteKind::Replica);
    assert_eq!(r.out.node.node.as_str(), "n2");

    proxy.api().set_enabled(proxy.name(), false).await.unwrap();
    // A read straight away may still pick the replica: it fails over.
    let r = ok(&mut a, "SELECT @@server_id").await;
    assert_eq!(r.out.node.node.as_str(), "n1", "failed over to the writer");
    rig.wait_for("replica down", Duration::from_secs(10), |r| {
        r.backends.nodes().status(&r.node(1)).health == NodeHealth::Down
    })
    .await;
    for _ in 0..5 {
        let r = ok(&mut a, "SELECT @@server_id").await;
        assert_eq!(r.out.route, RouteKind::Writer);
    }
    proxy.api().set_enabled(proxy.name(), true).await.unwrap();
    rig.wait_for("replica up", Duration::from_secs(10), |r| {
        let s = r.backends.nodes().status(&r.node(1));
        s.health == NodeHealth::Up && s.lag == Some(Duration::ZERO)
    })
    .await;
    let r = ok(&mut a, "SELECT @@server_id").await;
    assert_eq!(r.out.route, RouteKind::Replica);
    a.close().await;
}

const OWN_PW: &str = "sprig-sim-root";

fn own_mariadb(prefix: &str, server_id: u32) -> (OwnContainer, String) {
    let cnf = sprig_testkit::compose::compose_dir()
        .join("mariadb")
        .join("binlog.cnf");
    let mount = format!("{}:/etc/mysql/conf.d/10-binlog.cnf:ro", cnf.display());
    let env = format!("MARIADB_ROOT_PASSWORD={OWN_PW}");
    let sid = format!("--server-id={server_id}");
    let c = OwnContainer::run(
        prefix,
        &[
            "--network",
            "sprigdb-dev_dc-a",
            "-e",
            &env,
            "-p",
            "127.0.0.1::3306",
            "-v",
            &mount,
            "mariadb:11.8",
            &sid,
            "--gtid-domain-id=9",
        ],
    )
    .unwrap();
    let addr = c.port(3306).unwrap();
    (c, addr)
}

async fn connect_retry(addr: &str) -> MysqlClient {
    let u = Url {
        user: "root".into(),
        pass: OWN_PW.into(),
        addr: addr.into(),
    };
    let end = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let mut c = ClientConfig::new(u.user.clone(), u.pass.clone());
        c.tls = TlsMode::Required;
        c.tls_config = Some(
            TlsOptions {
                skip_verify: true,
                ..TlsOptions::default()
            }
            .build()
            .unwrap(),
        );
        c.connect_timeout = Duration::from_secs(5);
        match MysqlClient::connect(addr, &c).await {
            Ok(c) => return c,
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < end,
                    "{addr} never came up: {e}"
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

#[tokio::test]
async fn read_your_writes_with_a_lagging_replica() {
    if !skip::flag(skip::DOCKER) {
        skip::report_missing(skip::DOCKER, "read_your_writes_with_a_lagging_replica");
        return;
    }
    let Some(tp) = toxiproxy() else {
        skip::report_missing(skip::TOXIPROXY, "read_your_writes_with_a_lagging_replica");
        return;
    };
    let (primary, paddr) = own_mariadb("bt-primary", 901);
    let (replica, raddr) = own_mariadb("bt-replica", 902);
    let mut p = connect_retry(&paddr).await;
    let mut r = connect_retry(&raddr).await;
    // Replication runs through toxiproxy, so latency on that link makes the
    // replica lag for real.
    let link = tp
        .own_proxy("bt-repl", &format!("{}:3306", primary.name()))
        .await
        .unwrap();
    let inner_port = link.host_addr().rsplit(':').next().unwrap().to_owned();
    r.query_drop(&format!(
        "CHANGE MASTER TO MASTER_HOST='toxiproxy', MASTER_PORT={inner_port}, MASTER_USER='root', \
         MASTER_PASSWORD='{OWN_PW}', MASTER_USE_GTID=slave_pos; START SLAVE"
    ))
    .await
    .unwrap();
    p.quit().await;
    r.quit().await;

    let rig = rig(
        TopologyKind::PrimaryReplica,
        "root",
        OWN_PW,
        &[paddr.clone(), raddr.clone()],
        fast_settings(),
    );
    let db = unique_ident("sprig_bt");
    let mut a = rig.session(Persona::MariaDb);
    ok(&mut a, &format!("CREATE DATABASE {db}")).await;
    ok(&mut a, &format!("CREATE TABLE {db}.t (v INT)")).await;
    rig.wait_roles(&[NodeRole::Writer, NodeRole::Replica]).await;
    rig.wait_for("replica caught up", Duration::from_secs(30), |r| {
        r.backends.nodes().status(&r.node(1)).lag == Some(Duration::ZERO)
    })
    .await;
    let r = ok(&mut a, &format!("SELECT COUNT(*) FROM {db}.t")).await;
    assert!(
        matches!(r.out.route, RouteKind::Replica | RouteKind::ReplicaWait),
        "{:?}",
        r.out.route
    );

    // Freeze the position poller (its estimate stays "caught up"), then
    // slow replication down to 1.5 s.
    let mut s = fast_settings();
    s.position_poll = Duration::from_secs(600);
    s.gtid_wait_timeout = Duration::from_millis(100);
    rig.settings.set(s.clone());
    tokio::time::sleep(Duration::from_millis(300)).await;
    link.api()
        .add_toxic(link.name(), &Toxic::latency(1500, 0))
        .await
        .unwrap();

    // The 100 ms wait times out (1242) and the read reruns on the writer.
    ok(&mut a, &format!("INSERT INTO {db}.t VALUES (1)")).await;
    let r = ok(&mut a, &format!("SELECT MAX(v) FROM {db}.t")).await;
    assert_eq!(r.cell(0, 0), Some("1"), "read missed the write");
    assert_eq!(
        r.out.route,
        RouteKind::Writer,
        "wait timed out, reran on the writer"
    );

    // A 5 s wait succeeds on the replica.
    s.gtid_wait_timeout = Duration::from_secs(5);
    rig.settings.set(s);
    ok(&mut a, &format!("INSERT INTO {db}.t VALUES (2)")).await;
    let started = std::time::Instant::now();
    let r = ok(&mut a, &format!("SELECT MAX(v) FROM {db}.t")).await;
    assert_eq!(r.cell(0, 0), Some("2"), "read missed the write");
    assert_eq!(r.out.route, RouteKind::ReplicaWait);
    assert!(
        started.elapsed() >= Duration::from_millis(500),
        "it waited for the replica"
    );
    a.close().await;
    drop(link);
    drop(replica);
    drop(primary);
}

#[tokio::test]
async fn lowered_limits_converge() {
    let u = need!(skip::MARIADB_SINGLE, "lowered_limits_converge");
    let rig = rig(
        TopologyKind::SingleNode,
        &u[0].user,
        &u[0].pass,
        &[u[0].addr.clone()],
        fast_settings(),
    );
    let mut sessions: Vec<BackendSession> = (0..4).map(|_| rig.session(Persona::MariaDb)).collect();
    for s in &mut sessions {
        ok(s, "BEGIN").await; // pinned: four connections at once
    }
    for s in &mut sessions {
        ok(s, "COMMIT").await;
    }
    let key = sprig_gateway::backend::PoolKey {
        cluster: rig.target.cluster.clone(),
        node: rig.view.nodes[0].node.clone(),
        account: rig.target.user.clone(),
        found_rows: false,
        mariadb_caps: 0,
    };
    assert_eq!(rig.backends.pools().stats(&key).idle, 4);
    rig.settings.set(BackendSettings {
        max_conns_per_node: 1,
        acquire_timeout: Duration::from_millis(300),
        ..fast_settings()
    });
    rig.wait_for("idle excess closed", Duration::from_secs(5), |r| {
        r.backends.pools().stats(&key).open <= 1
    })
    .await;
    // Under the new limit: one pinned session starves the others with 39031.
    ok(&mut sessions[0], "BEGIN").await;
    let e = run(&mut sessions[1], "SELECT 1").await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::BackendPoolTimeout), "{e}");
    ok(&mut sessions[0], "COMMIT").await;
    ok(&mut sessions[1], "SELECT 1").await;
    for mut s in sessions {
        s.close().await;
    }
}

/// MariaDB binds a prepared statement's unqualified names to the default
/// schema at prepare time. Two sessions in different schemas that prepare
/// the same text on the same pooled connection must each get their own
/// schema's table (the statement cache keys by schema and text); before the
/// fix the second session got the first one's statement.
#[tokio::test]
async fn prepared_statements_follow_the_session_schema() {
    let u = need!(
        skip::MARIADB_SINGLE,
        "prepared_statements_follow_the_session_schema"
    );
    let rig = rig(
        TopologyKind::SingleNode,
        &u[0].user,
        &u[0].pass,
        &[u[0].addr.clone()],
        BackendSettings {
            max_conns_per_node: 1, // every statement uses the same connection
            ..fast_settings()
        },
    );
    let (a, b) = (unique_ident("sprig_bt_a"), unique_ident("sprig_bt_b"));
    let mut s = rig.session(Persona::MariaDb);
    for db in [&a, &b] {
        ok(&mut s, &format!("CREATE DATABASE {db}")).await;
        ok(&mut s, &format!("CREATE TABLE {db}.t (v INT)")).await;
    }
    // One session switching schemas stands in for sessions that log in
    // with different databases (a session without one never gets a
    // connection that has one).
    let sql = b"SELECT v FROM t";
    let info = StmtInfo::classify(sql);
    for db in [&a, &b, &a] {
        ok(&mut s, &format!("USE {db}")).await;
        let st = s.prepare(&info, sql).await.unwrap();
        assert_eq!(
            String::from_utf8_lossy(&st.columns[0].schema),
            db.as_str(),
            "the statement was prepared under another schema"
        );
    }
    for db in [&a, &b] {
        ok(&mut s, &format!("DROP DATABASE {db}")).await;
    }
    s.close().await;
}
