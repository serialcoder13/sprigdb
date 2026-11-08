//! The binlog dump probe (chapter 09, "Binlog dump") against the Compose
//! topologies: every node of each, as root standing in for `sprig_repl`;
//! an account without `REPLICATION SLAVE` on the single node (the feed is
//! `unsupported`, with the grant to run); and, on a throwaway server named
//! by `SPRIG_TEST_BINLOG_SCRATCH` (whose globals tests may change), a
//! compressed binlog.
//!
//! ```sh
//! export SPRIG_TEST_MARIADB_SINGLE=mysql://root:sprig-dev-root@127.0.0.1:13301
//! export SPRIG_TEST_MARIADB_REPLICATION=mysql://root:sprig-dev-root@127.0.0.1:13311,...
//! export SPRIG_TEST_MARIADB_GALERA=mysql://root:sprig-dev-root@127.0.0.1:13321,...
//! cargo test -p sprig-cluster --test dump_probe
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::time::Duration;

use sprig_cluster::probe::dump::{self, DumpFailureKind};
use sprig_cluster::probe::feed::{self, FeedLevel};
use sprig_cluster::probe::{ProbeDepth, Prober};
use sprig_cluster::profile::{DeriveInput, derive};
use sprig_cluster::record::TopologyKind;
use sprig_cluster::{NodeEndpoint, ProbeSession};
use sprig_common::ids::NodeId;

const TIMEOUT: Duration = Duration::from_secs(2);

fn endpoints(var: &str) -> Option<Vec<NodeEndpoint>> {
    let urls = std::env::var(var).ok().filter(|v| !v.trim().is_empty())?;
    Some(
        urls.split(',')
            .enumerate()
            .map(|(i, u)| {
                NodeEndpoint::from_url(NodeId::new(format!("n{}", i + 1)).unwrap(), u.trim())
                    .unwrap()
            })
            .collect(),
    )
}

macro_rules! topology_or_skip {
    ($var:literal) => {
        match endpoints($var) {
            Some(eps) => eps,
            None => {
                eprintln!("skipped: {} is not set", $var);
                return;
            }
        }
    };
}

async fn gtid_binlog_pos(ep: &NodeEndpoint) -> String {
    let mut s = ProbeSession::connect(ep, TIMEOUT).await.unwrap();
    let pos = s.scalar("SELECT @@gtid_binlog_pos").await.unwrap();
    s.close().await;
    pos.unwrap_or_default()
}

/// Every node's dump passes at its current position, within the probe
/// timeout: format description, the events after it, the end of the dump.
async fn every_node_reads(eps: &[NodeEndpoint]) {
    for ep in eps {
        let from = gtid_binlog_pos(ep).await;
        let d = dump::run(ep, &from, TIMEOUT).await;
        println!("{}: {d:?}", ep.node);
        assert!(d.passed(), "{}: {d:?}", ep.node);
        let fd = d
            .events
            .iter()
            .position(|e| e == "format_description")
            .expect("a format description");
        assert!(d.events.len() > fd + 1, "{}: {:?}", ep.node, d.events);
        assert_eq!(d.checksums, Some(true));
        assert!(
            d.server_version
                .as_deref()
                .is_some_and(|v| v.contains("MariaDB"))
        );
        assert!(d.took_ms < 2000);
    }
}

#[tokio::test]
async fn single_node() {
    let eps = topology_or_skip!("SPRIG_TEST_MARIADB_SINGLE");
    every_node_reads(&eps).await;
}

#[tokio::test]
async fn primary_and_replicas() {
    let eps = topology_or_skip!("SPRIG_TEST_MARIADB_REPLICATION");
    every_node_reads(&eps).await;
}

#[tokio::test]
async fn galera_nodes() {
    let eps = topology_or_skip!("SPRIG_TEST_MARIADB_GALERA");
    every_node_reads(&eps).await;
}

/// An account without `REPLICATION SLAVE` logs in but can't dump: 1227,
/// the feed and the mover are `unsupported`, and the report names the
/// grant.
#[tokio::test]
async fn an_account_without_replication_slave_makes_the_feed_unsupported() {
    let eps = topology_or_skip!("SPRIG_TEST_MARIADB_SINGLE");
    let root = &eps[0];
    let tag = std::process::id();
    let user = format!("sprig_norepl_{tag:x}");
    let pw = format!("pw-{tag:x}");
    let mut s = ProbeSession::connect(root, TIMEOUT).await.unwrap();
    s.exec(&format!("DROP USER IF EXISTS '{user}'@'%'"))
        .await
        .unwrap();
    // BINLOG MONITOR alone: it may read positions, not stream events.
    s.exec(&format!(
        "CREATE USER '{user}'@'%' IDENTIFIED VIA mysql_native_password USING PASSWORD('{pw}')"
    ))
    .await
    .unwrap();
    s.exec(&format!("GRANT BINLOG MONITOR ON *.* TO '{user}'@'%'"))
        .await
        .unwrap();
    let url = format!(
        "mysql://{user}:{pw}@{}:{}",
        root.opts.ip_or_hostname(),
        root.opts.tcp_port()
    );
    let weak = NodeEndpoint::from_url(root.node.clone(), &url).unwrap();

    let direct = dump::run(&weak, &gtid_binlog_pos(root).await, TIMEOUT).await;
    let probes = Prober::new(Duration::from_secs(10))
        .probe_cluster_with_repl(
            TopologyKind::SingleNode,
            &eps,
            Some(std::slice::from_ref(&weak)),
            ProbeDepth::Deep,
        )
        .await;
    s.exec(&format!("DROP USER IF EXISTS '{user}'@'%'"))
        .await
        .unwrap();
    s.close().await;

    let f = direct.failure.as_ref().expect("the dump fails");
    assert_eq!(
        (f.kind, f.errno),
        (DumpFailureKind::Privilege, Some(1227)),
        "{f:?}"
    );
    assert!(direct.blocks_feed());
    assert_eq!(
        direct.advisory().unwrap(),
        format!(
            "the change feed can't read the binlog as sprig_repl ({}): \
             GRANT REPLICATION SLAVE, BINLOG MONITOR ON *.* TO sprig_repl",
            f.detail
        )
    );

    let profile = derive(DeriveInput {
        topology: TopologyKind::SingleNode,
        probes: &probes,
        declared: None,
        previous: None,
    });
    assert_eq!(profile.derived.feed_level, FeedLevel::Unsupported);
    assert!(!profile.derived.feed_source && !profile.derived.mover_source);
    let lines = feed::report(&probes, feed::DEFAULT_RETENTION_HOURS);
    assert!(
        lines
            .iter()
            .any(|l| l.starts_with("node n1: binlog dump is error 1227")
                && l.ends_with("GRANT REPLICATION SLAVE, BINLOG MONITOR ON *.* TO sprig_repl")),
        "{lines:#?}"
    );
}

/// `log_bin_compress = ON` on a throwaway server: the probe, started just
/// before a write, decodes the compressed events and passes, on a server
/// with `encrypt_binlog = ON` too (the server decrypts the events; the
/// reader skips the `START_ENCRYPTION` event it sends ahead of them).
#[tokio::test]
async fn compressed_binlog_on_a_scratch_server() {
    let eps = topology_or_skip!("SPRIG_TEST_BINLOG_SCRATCH");
    for ep in &eps {
        let mut s = ProbeSession::connect(ep, TIMEOUT).await.unwrap();
        let encrypted = s
            .scalar("SELECT @@global.encrypt_binlog")
            .await
            .ok()
            .flatten()
            .is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("ON"));
        s.exec("SET GLOBAL log_bin_compress = ON").await.unwrap();
        s.exec("SET GLOBAL log_bin_compress_min_len = 10")
            .await
            .unwrap();
        s.exec("CREATE DATABASE IF NOT EXISTS sprig_dump_probe_t")
            .await
            .unwrap();
        s.exec("CREATE TABLE IF NOT EXISTS sprig_dump_probe_t.t (id INT PRIMARY KEY, v TEXT)")
            .await
            .unwrap();
        let from = gtid_binlog_pos(ep).await;
        // A fresh session sees the new setting.
        let mut w = ProbeSession::connect(ep, TIMEOUT).await.unwrap();
        w.exec(&format!(
            "INSERT INTO sprig_dump_probe_t.t VALUES ({}, REPEAT('compress me ', 50))",
            std::process::id()
        ))
        .await
        .unwrap();
        w.close().await;
        // Started before the write, the probe reads its compressed events.
        let d = dump::run(ep, &from, TIMEOUT).await;
        s.exec("DROP DATABASE sprig_dump_probe_t").await.unwrap();
        s.exec("SET GLOBAL log_bin_compress = OFF").await.unwrap();
        s.close().await;
        println!("{}: encrypt_binlog = {encrypted}: {d:?}", ep.node);
        assert!(d.passed(), "{d:?}");
        assert!(
            d.events.iter().any(|e| e == "write_rows_compressed"),
            "{d:?}"
        );
        assert_eq!(d.events.last().map(String::as_str), Some("dump_end"));
    }
}
