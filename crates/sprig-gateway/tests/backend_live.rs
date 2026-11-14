//! Backend pools, state replay, pins, read-your-writes, writer selection
//! and failure paths against the Compose stack. Each test is
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
use sprig_gateway::backend::settings::{BackendSettings, CausalScope, GaleraUnconfirmed};
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

/// A commit on a connection with a fixed `timestamp` (Sprig 2PC's held
/// branches fix their clock, and so may clients): MariaDB 11.8 reports
/// `timestamp` in place of `last_gtid` after the commit, so the gateway
/// reads the GTID itself. Without that the session's causal position
/// didn't move and another session's next read went to a replica that
/// hadn't applied the commit (a read-your-writes bug the differential
/// suite found under Sprig 2PC).
#[tokio::test]
async fn read_your_writes_after_a_commit_under_a_fixed_timestamp() {
    let u = need!(
        skip::MARIADB_REPLICATION,
        "read_your_writes_after_a_commit_under_a_fixed_timestamp"
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
    // The schema through a connection of its own: MariaDB drops
    // `last_gtid` only on a connection whose first tracked change is the
    // fixed clock, so the session's connection must not commit first.
    let mut s = side(&u[0]).await;
    s.query_drop(&format!("CREATE DATABASE {db}"))
        .await
        .unwrap();
    s.query_drop(&format!("CREATE TABLE {db}.t (v INT)"))
        .await
        .unwrap();
    // A read starts the node monitor; it commits nothing.
    let mut b = rig.session(Persona::MariaDb);
    ok(&mut b, "SELECT 1").await;
    rig.wait_roles(&[NodeRole::Writer, NodeRole::Replica, NodeRole::Replica])
        .await;
    let mut a = rig.session(Persona::MariaDb);
    let mut replica_reads = 0;
    for i in 1..=30 {
        let before = a.causal().clone();
        ok(&mut a, "SET timestamp = 1794242775.483399").await;
        ok(&mut a, "START TRANSACTION").await;
        ok(&mut a, &format!("INSERT INTO {db}.t VALUES ({i})")).await;
        ok(&mut a, "COMMIT").await;
        assert!(
            !before.covers(a.causal()),
            "commit {i}: the causal position didn't move ({before} → {})",
            a.causal()
        );
        ok(&mut a, "SET timestamp = DEFAULT").await;
        // An autocommit write under a fixed clock too.
        ok(&mut a, "SET timestamp = 1794242775.483399").await;
        let before = a.causal().clone();
        ok(&mut a, &format!("INSERT INTO {db}.t VALUES ({})", 1000 + i)).await;
        assert!(
            !before.covers(a.causal()),
            "autocommit write {i}: the causal position didn't move"
        );
        ok(&mut a, "SET timestamp = DEFAULT").await;
        // Another session of the same account, through this gateway.
        let r = ok(&mut b, &format!("SELECT COUNT(*) FROM {db}.t")).await;
        assert_eq!(
            r.cell(0, 0),
            Some((2 * i).to_string().as_str()),
            "read {i} missed a commit"
        );
        if matches!(r.out.route, RouteKind::Replica | RouteKind::ReplicaWait) {
            replica_reads += 1;
        }
    }
    eprintln!("replica reads: {replica_reads}/30");
    a.close().await;
    b.close().await;
    s.query_drop(&format!("DROP DATABASE {db}")).await.unwrap();
}

/// With `autocommit = 0` a plain read opens a transaction, so it runs on
/// the writer and pins there; before the fix it went
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
        if i % 2 == 0 {
            // Give the position poll time to see the other nodes apply it;
            // otherwise the read may leave the writer with the sync check
            // alone.
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
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
        // The check runs before the client's statement, which keeps 0.
        assert_eq!(r.cell(0, 1), Some("0"), "read {i}");
        if matches!(
            r.out.route,
            RouteKind::GaleraSyncWait | RouteKind::GaleraUnconfirmed
        ) {
            sync_waits += 1;
        }
        nodes.insert(r.out.node.node.clone());
    }
    eprintln!("sync waits: {sync_waits}/30 over {} nodes", nodes.len());
    assert!(
        sync_waits > 0,
        "reads leave the writer once others have the write"
    );
    ok(&mut a, &format!("DROP DATABASE {db}")).await;
    a.close().await;
    b.close().await;
}

/// Galera read routing under load: four sessions, 1,000 write-then-read
/// rounds each, in four shapes (autocommit insert, update, explicit
/// transaction, delete and insert), with and without a pause before the
/// read, once per `reads.galera_unconfirmed` mode. No read misses its
/// write; with `check` most reads leave the writer.
#[tokio::test]
async fn read_your_writes_on_galera_stress() {
    let u = need!(skip::MARIADB_GALERA, "read_your_writes_on_galera_stress");
    let addrs: Vec<String> = u.iter().map(|x| x.addr.clone()).collect();
    let mut on_writer = Vec::new();
    for mode in [GaleraUnconfirmed::Writer, GaleraUnconfirmed::Check] {
        let rig = rig(
            TopologyKind::Galera,
            &u[0].user,
            &u[0].pass,
            &addrs,
            BackendSettings {
                position_poll: Duration::from_millis(25),
                galera_unconfirmed: mode,
                ..fast_settings()
            },
        );
        let (misses, routes, slowest, timeouts) = galera_stress_round(&rig, 1000).await;
        let writer = routes.get("Writer").copied().unwrap_or(0);
        eprintln!(
            "{mode:?}: 4 x 1000 reads: {misses} misses, {writer} on the writer, \
             {timeouts} took the check timeout or longer, slowest {slowest:?}; routes {routes:?}"
        );
        assert_eq!(misses, 0, "{mode:?}: reads missed their writes");
        assert!(
            slowest < Duration::from_secs(1),
            "{mode:?}: a read took {slowest:?}"
        );
        on_writer.push(writer);
    }
    assert!(
        on_writer[1] * 2 < on_writer[0],
        "check mode moves reads off the writer: {on_writer:?}"
    );
}

/// Four sessions of write-then-read rounds: the misses, the routes, the
/// slowest read and how many reads took the check timeout or longer.
async fn galera_stress_round(
    rig: &Rig,
    rounds: u32,
) -> (u32, std::collections::BTreeMap<String, u32>, Duration, u32) {
    let check_timeout = rig.settings.get().galera_sync_wait_timeout;
    let db = unique_ident("sprig_bt");
    let mut a = rig.session(Persona::MariaDb);
    ok(&mut a, &format!("CREATE DATABASE {db}")).await;
    ok(
        &mut a,
        &format!("CREATE TABLE {db}.c (w INT PRIMARY KEY, v INT NOT NULL)"),
    )
    .await;
    rig.wait_roles(&[NodeRole::Galera, NodeRole::Galera, NodeRole::Galera])
        .await;
    let mut workers = Vec::new();
    for w in 0..4u32 {
        let mut s = rig.session(Persona::MariaDb);
        let db = db.clone();
        workers.push(tokio::spawn(async move {
            ok(&mut s, &format!("INSERT INTO {db}.c VALUES ({w}, 0)")).await;
            let mut routes = std::collections::BTreeMap::<String, u32>::new();
            let mut misses = 0u32;
            let mut slowest = Duration::ZERO;
            let mut timeouts = 0u32;
            let mut seed = u64::from(w) * 7919 + 1;
            for i in 1..=rounds {
                match i % 4 {
                    0 => {
                        ok(&mut s, &format!("UPDATE {db}.c SET v = {i} WHERE w = {w}")).await;
                    }
                    1 => {
                        ok(&mut s, "BEGIN").await;
                        ok(&mut s, &format!("UPDATE {db}.c SET v = {i} WHERE w = {w}")).await;
                        ok(&mut s, "COMMIT").await;
                    }
                    2 => {
                        ok(&mut s, &format!("DELETE FROM {db}.c WHERE w = {w}")).await;
                        ok(&mut s, &format!("INSERT INTO {db}.c VALUES ({w}, {i})")).await;
                    }
                    _ => {
                        ok(
                            &mut s,
                            &format!("INSERT INTO {db}.c VALUES ({w}, {i}) ON DUPLICATE KEY UPDATE v = {i}"),
                        )
                        .await;
                    }
                }
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                if seed >> 62 == 0 {
                    // A quarter of the reads wait 0-40 ms: the poll may
                    // then show the write on other nodes.
                    tokio::time::sleep(Duration::from_millis((seed >> 32) % 41)).await;
                }
                let started = tokio::time::Instant::now();
                let r = ok(&mut s, &format!("SELECT v FROM {db}.c WHERE w = {w}")).await;
                let took = started.elapsed();
                slowest = slowest.max(took);
                timeouts += u32::from(took >= check_timeout);
                if r.cell(0, 0) != Some(i.to_string().as_str()) {
                    misses += 1;
                    eprintln!("worker {w} round {i}: read {:?} on {:?}", r.cell(0, 0), r.out.route);
                }
                *routes.entry(format!("{:?}", r.out.route)).or_default() += 1;
            }
            s.close().await;
            (misses, routes, slowest, timeouts)
        }));
    }
    let mut misses = 0;
    let mut slowest = Duration::ZERO;
    let mut timeouts = 0;
    let mut routes = std::collections::BTreeMap::<String, u32>::new();
    for h in workers {
        let (m, r, sl, t) = h.await.unwrap();
        misses += m;
        slowest = slowest.max(sl);
        timeouts += t;
        for (k, n) in r {
            *routes.entry(k).or_default() += n;
        }
    }
    ok(&mut a, &format!("DROP DATABASE {db}")).await;
    a.close().await;
    (misses, routes, slowest, timeouts)
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

/// Blocks apply on a Galera node: `FLUSH TABLES WITH READ LOCK` on a side
/// connection holds the node's applier until [`ApplyBlock::release`] (or
/// until the connection drops, if the test panics first).
struct ApplyBlock {
    conn: Option<MysqlClient>,
}

impl ApplyBlock {
    async fn on(u: &Url) -> Self {
        let mut c = side(u).await;
        c.query_drop("FLUSH TABLES WITH READ LOCK").await.unwrap();
        Self { conn: Some(c) }
    }

    async fn release(&mut self) {
        if let Some(mut c) = self.conn.take() {
            c.query_drop("UNLOCK TABLES").await.unwrap();
            c.quit().await;
        }
    }
}

/// Waits until a Galera node is Synced again with an empty receive queue.
async fn wait_synced(u: &Url) {
    let end = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let mut c = side(u).await;
        let rs = c
            .query(
                "SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS \
                 WHERE VARIABLE_NAME = 'wsrep_local_state'",
            )
            .await
            .unwrap();
        c.quit().await;
        let state = rs
            .iter()
            .find(|r| !r.columns.is_empty())
            .and_then(|r| r.text_rows().ok())
            .and_then(|rows| rows.into_iter().next())
            .and_then(|row| row.into_iter().next().flatten())
            .map(|b| String::from_utf8_lossy(&b).into_owned());
        if state.as_deref() == Some("4") {
            return;
        }
        assert!(
            tokio::time::Instant::now() < end,
            "{} not Synced: {state:?}",
            u.addr
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// A Galera node that is behind never
/// stalls a read that must see a write. Apply is blocked on one node while
/// health checks are too slow to notice (as between two checks in
/// production), so only the polled positions and the bounded check protect
/// the reads: each takes well under a second and is served by a node that
/// has the write, or by the writer. Until the node is marked behind, a read
/// may try it and lose at most `reads.galera_sync_wait_timeout_ms` before
/// rerunning on the writer; once it is marked behind it is never tried.
/// Once the node catches up, reads return to it.
#[tokio::test]
async fn a_lagging_galera_node_never_stalls_reads() {
    let u = need!(
        skip::MARIADB_GALERA,
        "a_lagging_galera_node_never_stalls_reads"
    );
    let addrs: Vec<String> = u.iter().map(|x| x.addr.clone()).collect();
    let rig = rig(
        TopologyKind::Galera,
        &u[0].user,
        &u[0].pass,
        &addrs,
        BackendSettings {
            // Health checks see the blocked node as Synced and ready for
            // the whole test: the first check runs at once, the next after
            // a minute.
            health_interval: Duration::from_secs(60),
            ..fast_settings()
        },
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
    let writer = rig
        .backends
        .writer(&rig.target.cluster, ShardId(0))
        .await
        .unwrap();
    let lag_i = rig
        .view
        .nodes
        .iter()
        .position(|n| n.node != writer.node)
        .unwrap();
    let lagging = rig.node(lag_i);
    eprintln!("writer {}, blocking apply on {lagging}", writer.node);

    let check_timeout = rig.settings.get().galera_sync_wait_timeout;
    let mut block = ApplyBlock::on(&u[lag_i]).await;
    let mut slowest = Duration::ZERO;
    let mut check_timeouts = 0u32;
    let mut served = std::collections::HashMap::<String, u32>::new();
    let result = Box::pin(async {
        for i in 1..=40 {
            ok(&mut a, &format!("INSERT INTO {db}.t (v) VALUES ({i})")).await;
            if i % 4 == 0 {
                // Let the position poll see the other nodes catch up.
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            // A fresh session each time.
            let mut b = rig.session(Persona::MariaDb);
            let started = tokio::time::Instant::now();
            let r = run(&mut b, &format!("SELECT MAX(v) FROM {db}.t")).await;
            let took = started.elapsed();
            b.close().await;
            let r = r.unwrap_or_else(|e| panic!("read {i}: {e} after {took:?}"));
            slowest = slowest.max(took);
            if took >= check_timeout {
                check_timeouts += 1;
            }
            assert!(r.err().is_none(), "read {i}: {:?} after {took:?}", r.err());
            assert_eq!(r.cell(0, 0), Some(i.to_string().as_str()), "read {i}");
            assert!(took < Duration::from_secs(1), "read {i} took {took:?}");
            assert_ne!(r.out.node, lagging, "read {i} went to the blocked node");
            *served
                .entry(format!("{}/{:?}", r.out.node.node, r.out.route))
                .or_default() += 1;
        }
        // Far behind (reads.max_galera_lag_ms, 1 s): the node is marked
        // and gets no reads at all, even ones with no write to see.
        rig.wait_for(
            "the blocked node to be behind",
            Duration::from_secs(10),
            |r| r.backends.nodes().status(&lagging).behind,
        )
        .await;
        // Reads that must see a write (possibly with no node but the writer
        // showing it) never try the node behind, so no
        // check times out.
        for i in 41..=60 {
            ok(&mut a, &format!("INSERT INTO {db}.t (v) VALUES ({i})")).await;
            let mut b = rig.session(Persona::MariaDb);
            let started = tokio::time::Instant::now();
            let r = ok(&mut b, &format!("SELECT MAX(v) FROM {db}.t")).await;
            let took = started.elapsed();
            b.close().await;
            assert_eq!(r.cell(0, 0), Some(i.to_string().as_str()), "read {i}");
            assert!(took < check_timeout, "read {i} took {took:?}");
            assert_ne!(r.out.node, lagging, "read {i} went to the node behind");
            *served
                .entry(format!("behind:{}/{:?}", r.out.node.node, r.out.route))
                .or_default() += 1;
        }
        rig.settings.set(BackendSettings {
            causal_scope: CausalScope::Session,
            ..rig.settings.get().as_ref().clone()
        });
        for i in 0..20 {
            let mut b = rig.session(Persona::MariaDb);
            let started = tokio::time::Instant::now();
            let r = ok(&mut b, &format!("SELECT COUNT(*) FROM {db}.t")).await;
            b.close().await;
            assert!(started.elapsed() < Duration::from_secs(1), "plain read {i}");
            assert_ne!(
                r.out.node, lagging,
                "plain read {i} went to the node behind"
            );
        }
        rig.settings.set(BackendSettings {
            causal_scope: CausalScope::TenantGateway,
            ..rig.settings.get().as_ref().clone()
        });
    })
    .await;
    block.release().await;
    wait_synced(&u[lag_i]).await;
    let () = result;
    eprintln!(
        "blocked: slowest read {slowest:?}, {check_timeouts} check timeouts; served by {served:?}"
    );
    rig.wait_for("the node to catch up", Duration::from_secs(10), |r| {
        !r.backends.nodes().status(&lagging).behind
    })
    .await;

    // Caught up: reads that must see a write go to that node again.
    let mut back = false;
    for i in 61..=120 {
        ok(&mut a, &format!("INSERT INTO {db}.t (v) VALUES ({i})")).await;
        tokio::time::sleep(Duration::from_millis(120)).await;
        let mut b = rig.session(Persona::MariaDb);
        let r = ok(&mut b, &format!("SELECT MAX(v) FROM {db}.t")).await;
        b.close().await;
        assert_eq!(r.cell(0, 0), Some(i.to_string().as_str()), "read {i}");
        if r.out.node == lagging {
            back = true;
            break;
        }
    }
    assert!(back, "reads never returned to {lagging} after it caught up");
    ok(&mut a, &format!("DROP DATABASE {db}")).await;
    a.close().await;
}

/// Follow-up 1: the session reports the capabilities its backend
/// connection negotiated, not the ones `SessionOpts` asked for.
#[tokio::test]
async fn backend_caps_are_the_connections_own() {
    use sprig_proto_mysql::Caps;
    let u = need!(skip::MARIADB_SINGLE, "backend_caps_are_the_connections_own");
    let rig = rig(
        TopologyKind::SingleNode,
        &u[0].user,
        &u[0].pass,
        &[u[0].addr.clone()],
        fast_settings(),
    );
    let ext = Caps::MARIADB_CLIENT_EXTENDED_METADATA;
    for asked in [ext, Caps::empty()] {
        let mut s = rig.backends.session(
            rig.target.clone(),
            SessionOpts {
                mariadb_caps: asked,
                ..SessionOpts::default()
            },
        );
        assert_eq!(s.backend_caps(), None, "no connection yet");
        s.query(&StmtInfo::read(), b"SELECT 1").await.unwrap();
        let caps = s.backend_caps().expect("the statement's connection");
        while s.next().await.unwrap().is_some() {}
        s.finish().await.unwrap();
        // MariaDB 11.8 grants extended metadata when asked, and only then.
        assert_eq!(caps.contains(ext), asked.contains(ext), "{asked:?}");
        assert!(caps.contains(Caps::PROTOCOL_41));
        assert_eq!(s.backend_caps(), None, "released between statements");
        ok(&mut s, "SELECT GET_LOCK('sprig_bt_caps', 1)").await;
        assert_eq!(
            s.backend_caps().map(|c| c.contains(ext)),
            Some(asked.contains(ext)),
            "the held connection's"
        );
        ok(&mut s, "SELECT RELEASE_LOCK('sprig_bt_caps')").await;
        s.close().await;
    }
}

fn reader_route(r: RouteKind) -> bool {
    matches!(
        r,
        RouteKind::Replica
            | RouteKind::ReplicaWait
            | RouteKind::Galera
            | RouteKind::GaleraSyncWait
            | RouteKind::GaleraUnconfirmed
    )
}

/// Follow-up 2: plain reads on replicas and Galera nodes never leave state
/// on the reader ("unexpected session state on a reader"). Covers the
/// wait-or-fail prefix (including timeouts), user variables, schema and
/// variable syncs, prepared reads, and `SET STATEMENT ... FOR SELECT`,
/// whose temporary values MariaDB's tracking reports as if they stuck.
#[tokio::test]
async fn reads_leave_no_state_on_readers() {
    let mut ran = false;
    for (var, topology) in [
        (skip::MARIADB_REPLICATION, TopologyKind::PrimaryReplica),
        (skip::MARIADB_GALERA, TopologyKind::Galera),
    ] {
        let Some(u) = urls(var) else { continue };
        ran = true;
        let addrs: Vec<String> = u.iter().map(|x| x.addr.clone()).collect();
        let rig = rig(
            topology,
            &u[0].user,
            &u[0].pass,
            &addrs,
            BackendSettings {
                // Short replica waits: some time out and rerun on the writer.
                gtid_wait_timeout: Duration::from_millis(1),
                ..fast_settings()
            },
        );
        let db = unique_ident("sprig_bt");
        let mut a = rig.session(Persona::MySql84);
        ok(&mut a, &format!("CREATE DATABASE {db}")).await;
        ok(
            &mut a,
            &format!("CREATE TABLE {db}.t (id INT PRIMARY KEY, v INT)"),
        )
        .await;
        let roles = if topology == TopologyKind::Galera {
            [NodeRole::Galera; 3]
        } else {
            [NodeRole::Writer, NodeRole::Replica, NodeRole::Replica]
        };
        rig.wait_roles(&roles).await;
        ok(&mut a, &format!("USE {db}")).await;
        ok(&mut a, "SET NAMES latin1").await;
        ok(&mut a, "SET time_zone = '+05:30', sql_select_limit = 1000").await;
        ok(&mut a, "SET @k = 3").await;
        let mut on_readers = 0;
        for i in 0..60 {
            if i % 3 == 0 {
                ok(&mut a, &format!("REPLACE INTO t VALUES ({}, {i})", i % 7)).await;
            }
            if i % 5 == 0 {
                tokio::time::sleep(Duration::from_millis(80)).await;
            }
            if i % 6 == 4 {
                // A state change right after SET STATEMENT carries its revert.
                ok(&mut a, &format!("SET @k = {}", i % 7)).await;
                assert!(
                    !a.is_pinned(),
                    "{topology:?} SET @k after SET STATEMENT: {:?}",
                    a.pins()
                );
            }
            let sql = match i % 6 {
                0 => "SELECT COUNT(*) FROM t".to_owned(),
                1 => "SELECT v FROM t WHERE id = @k".to_owned(),
                2 => "SET STATEMENT max_statement_time = 5 FOR SELECT COUNT(*) FROM t".to_owned(),
                3 => "SET STATEMENT innodb_lock_wait_timeout = 2 FOR SELECT * FROM t".to_owned(),
                4 => "SELECT @@time_zone, @@character_set_client, DATABASE()".to_owned(),
                _ => "SELECT SQL_CALC_FOUND_ROWS * FROM t LIMIT 1".to_owned(),
            };
            let r = ok(&mut a, &sql).await;
            if reader_route(r.out.route) {
                on_readers += 1;
            }
            assert!(
                !(reader_route(r.out.route) && r.out.pinned),
                "{topology:?} {sql}: state on a reader ({:?})",
                r.out.route
            );
            assert!(!a.is_pinned(), "{topology:?} {sql}: pinned {:?}", a.pins());

            // Prepared too.
            let info = StmtInfo::classify(b"SELECT v FROM t WHERE id = 1");
            a.execute(&info, b"SELECT v FROM t WHERE id = 1", &mut |st| {
                // COM_STMT_EXECUTE, no cursor, one iteration, no parameters.
                let mut p = vec![0x17];
                p.extend_from_slice(&st.id.to_le_bytes());
                p.push(0);
                p.extend_from_slice(&1u32.to_le_bytes());
                Ok(bytes::Bytes::from(p))
            })
            .await
            .unwrap();
            while a.next().await.unwrap().is_some() {}
            let o = a.finish().await.unwrap();
            assert!(
                !(reader_route(o.route) && o.pinned),
                "{topology:?} prepared read"
            );
        }
        // On the writer too: the revert reported with the next change
        // (before the fix, a lifelong Untracked pin).
        for _ in 0..3 {
            ok(
                &mut a,
                "SET STATEMENT innodb_lock_wait_timeout = 3 FOR SELECT * FROM t FOR UPDATE",
            )
            .await;
            ok(&mut a, "SET @z = 1").await;
            assert!(!a.is_pinned(), "{topology:?}: {:?}", a.pins());
        }
        eprintln!("{topology:?}: {on_readers}/60 text reads on readers");
        assert!(on_readers > 0, "{topology:?}: no read left the writer");
        // SET STATEMENT left nothing behind (MariaDB reports the revert of
        // an allowed variable later: its true value, the baseline's).
        assert!(
            a.vars()
                .vars
                .get("max_statement_time")
                .is_none_or(|v| v == "0.000000"),
            "{:?}",
            a.vars().vars.get("max_statement_time")
        );
        assert!(!a.vars().vars.contains_key("innodb_lock_wait_timeout"));
        let r = run_as(&mut a, &StmtInfo::write(), "SELECT @@max_statement_time")
            .await
            .unwrap();
        assert_eq!(r.cell(0, 0), Some("0.000000"));
        // The replayed state is intact.
        let r = ok(
            &mut a,
            "SELECT @@time_zone, @@character_set_client, @@sql_select_limit",
        )
        .await;
        assert_eq!(
            r.rows[0],
            [
                Some("+05:30".into()),
                Some("latin1".into()),
                Some("1000".into())
            ]
        );
        ok(&mut a, &format!("DROP DATABASE {db}")).await;
        a.close().await;
    }
    if !ran {
        skip::report_missing(skip::MARIADB_REPLICATION, "reads_leave_no_state_on_readers");
    }
}

/// Follow-up 3: a client's `session_track_*`, timeout or `wsrep_sync_wait`
/// settings never stick to the backend connection. The gateway keeps the
/// client's values (to answer) and its own tracking: commits, schema and
/// variables are still seen, on this connection and the next session's.
#[tokio::test]
async fn clients_cannot_change_the_gateways_session_tracking() {
    let u = need!(
        skip::MARIADB_SINGLE,
        "clients_cannot_change_the_gateways_session_tracking"
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
    let db = unique_ident("sprig_bt");
    let mut s = rig.session(Persona::MariaDb);
    ok(
        &mut s,
        "SET SESSION session_track_system_variables = 'autocommit', session_track_schema = OFF, \
         session_track_state_change = OFF, wait_timeout = 5, net_read_timeout = 7",
    )
    .await;
    assert!(!s.is_pinned(), "{:?}", s.pins());
    let g = &s.vars().gateway_vars;
    assert_eq!(g["session_track_system_variables"], "autocommit");
    // As MariaDB itself answers @@session_track_schema.
    assert_eq!(g["session_track_schema"], "0");
    assert_eq!(g["session_track_state_change"], "0");
    assert_eq!(g["wait_timeout"], "5");
    assert_eq!(g["net_read_timeout"], "7");
    // The backend connection kept the gateway's values.
    let r = ok(
        &mut s,
        "SELECT @@session.session_track_system_variables, @@session.session_track_schema, \
         @@session.session_track_state_change, @@session.wait_timeout, \
         @@session.net_read_timeout = @@global.net_read_timeout",
    )
    .await;
    assert_eq!(r.cell(0, 0), Some("*"));
    assert_eq!(r.cell(0, 1), Some("1"));
    assert_eq!(r.cell(0, 2), Some("1"));
    assert!(r.cell(0, 3).unwrap().parse::<u64>().unwrap() >= 600);
    assert_eq!(r.cell(0, 4), Some("1"));
    // Tracking still works: variables, schema, commits.
    ok(&mut s, "SET time_zone = '+02:00'").await;
    assert_eq!(s.vars().vars["time_zone"], "+02:00");
    ok(&mut s, &format!("CREATE DATABASE {db}")).await;
    ok(&mut s, &format!("USE {db}")).await;
    assert_eq!(s.vars().schema.as_deref(), Some(db.as_str()));
    ok(&mut s, "CREATE TABLE t (v INT)").await;
    ok(&mut s, "INSERT INTO t VALUES (1)").await;
    assert!(!s.causal().is_empty(), "the commit's GTID was tracked");
    // Tracking off for part of one statement: the change it hid is resynced.
    ok(
        &mut s,
        "SET session_track_system_variables = '', time_zone = '+03:00'",
    )
    .await;
    assert_eq!(s.vars().vars["time_zone"], "+03:00");
    assert_eq!(s.vars().gateway_vars["session_track_system_variables"], "");
    // MariaDB Connector/J's start-up statement.
    ok(
        &mut s,
        "set autocommit=1, session_track_schema=1, session_track_system_variables = \
         CONCAT(@@global.session_track_system_variables, ',auto_increment_increment')",
    )
    .await;
    assert!(!s.is_pinned());
    assert!(
        s.vars().gateway_vars["session_track_system_variables"]
            .contains("auto_increment_increment")
    );
    // The next session on the same backend connection sees none of it.
    let mut t = rig.session(Persona::MariaDb);
    let r = ok(
        &mut t,
        "SELECT @@session.session_track_system_variables, @@session.wait_timeout >= 600, @@time_zone",
    )
    .await;
    assert_eq!(r.cell(0, 0), Some("*"));
    assert_eq!(r.cell(0, 1), Some("1"));
    assert_ne!(r.cell(0, 2), Some("+03:00"));
    assert!(t.vars().gateway_vars.is_empty());
    t.close().await;
    // A failed SET changes nothing and records nothing.
    let r = run(&mut s, "SET session_track_schema = 'bogus'")
        .await
        .unwrap();
    assert!(r.err().is_some());
    assert_eq!(s.vars().gateway_vars["session_track_schema"], "1");
    ok(&mut s, &format!("DROP DATABASE {db}")).await;
    s.close().await;

    // Galera: a client asking for wsrep_sync_wait gets causal reads from
    // the shard's writer instead; the connection keeps 0.
    if let Some(u) = urls(skip::MARIADB_GALERA) {
        let addrs: Vec<String> = u.iter().map(|x| x.addr.clone()).collect();
        let g = crate::rig(
            TopologyKind::Galera,
            &u[0].user,
            &u[0].pass,
            &addrs,
            fast_settings(),
        );
        let mut s = g.session(Persona::MariaDb);
        ok(&mut s, "SELECT 1").await;
        g.wait_roles(&[NodeRole::Galera, NodeRole::Galera, NodeRole::Galera])
            .await;
        ok(&mut s, "SET SESSION wsrep_sync_wait = 1").await;
        assert_eq!(s.vars().gateway_vars["wsrep_sync_wait"], "1");
        for _ in 0..10 {
            let r = ok(&mut s, "SELECT @@session.wsrep_sync_wait").await;
            assert_eq!(r.out.route, RouteKind::Writer);
            assert_eq!(r.cell(0, 0), Some("0"));
        }
        ok(&mut s, "SET SESSION wsrep_sync_wait = 0").await;
        let mut left = false;
        for _ in 0..20 {
            left |= reader_route(ok(&mut s, "SELECT 1").await.out.route);
        }
        assert!(left, "plain reads leave the writer again");
        s.close().await;
    }
}

/// Workload probes: `Handler_read_*` deltas on the
/// statement's own connection, less the reading's own cost, give the rows
/// a scan, a range read and a point lookup examined; pinned sessions are
/// never probed, so their diagnostics survive.
#[tokio::test]
async fn probes_measure_rows_examined_on_the_same_connection() {
    let u = need!(
        skip::MARIADB_SINGLE,
        "probes_measure_rows_examined_on_the_same_connection"
    );
    let rig = rig(
        TopologyKind::SingleNode,
        &u[0].user,
        &u[0].pass,
        &[u[0].addr.clone()],
        fast_settings(),
    );
    let db = unique_ident("sprig_bt");
    let mut a = rig.session(Persona::MariaDb);
    ok(&mut a, &format!("CREATE DATABASE {db}")).await;
    ok(&mut a, &format!("USE {db}")).await;
    ok(
        &mut a,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT NOT NULL, w INT NOT NULL, KEY by_v (v))",
    )
    .await;
    ok(
        &mut a,
        "INSERT INTO t WITH RECURSIVE s (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s \
         WHERE n < 1000) SELECT n, n % 100, n FROM s",
    )
    .await;

    let probed = |r: &Res| r.out.examined.expect("a probe ran");
    // Not asked: no probe.
    let r = ok(&mut a, "SELECT COUNT(*) FROM t WHERE w > 0").await;
    assert_eq!(r.out.examined, None);
    // A full scan, twice (the first probe of a connection calibrates).
    for _ in 0..2 {
        a.probe_next_statement();
        let r = ok(&mut a, "SELECT COUNT(*) FROM t WHERE w > 0").await;
        assert_eq!(r.cell(0, 0), Some("1000"));
        let x = probed(&r);
        assert!((1000..=1002).contains(&x), "full scan examined {x}");
    }
    // A range read on by_v: 20 rows.
    a.probe_next_statement();
    let r = ok(&mut a, "SELECT id FROM t WHERE v BETWEEN 1 AND 2").await;
    assert_eq!(r.rows.len(), 20);
    let x = probed(&r);
    assert!((20..=22).contains(&x), "range read examined {x}");
    // A point lookup.
    a.probe_next_statement();
    let r = ok(&mut a, "SELECT w FROM t WHERE id = 5").await;
    assert_eq!(probed(&r), 1);
    // A write: rows read to find what to change.
    a.probe_next_statement();
    let r = ok(&mut a, "UPDATE t SET w = w + 1 WHERE v = 3").await;
    assert_eq!(r.out.affected_rows, 10);
    let x = probed(&r);
    assert!((10..=12).contains(&x), "update examined {x}");

    // In a transaction the session is pinned: no probe, and ROW_COUNT()
    // still answers for the client's statement.
    ok(&mut a, "BEGIN").await;
    a.probe_next_statement();
    let r = ok(&mut a, "UPDATE t SET w = w + 1 WHERE v = 4").await;
    assert_eq!(r.out.examined, None);
    let rc = ok(&mut a, "SELECT ROW_COUNT()").await;
    assert_eq!(rc.cell(0, 0), Some("10"));
    ok(&mut a, "COMMIT").await;

    ok(&mut a, &format!("DROP DATABASE {db}")).await;
}
