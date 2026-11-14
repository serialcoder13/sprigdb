//! The per-cluster monitor: health checks every `backend.health_interval_ms`
//! and position polling every `reads.replica_position_poll_ms`, on one side
//! connection per node, all nodes in parallel, each check with a 1 s timeout.
//!
//! The checks log in as the cluster's `sprig_gw` monitoring account
//! (`SLAVE MONITOR` only), or, while it can't log in, as the first login's
//! backend account ([`Backends::monitor_connection`]); a connection opened
//! as the latter is replaced once `sprig_gw` may be tried again. Each
//! poll also reads the node's `max_allowed_packet`; the smallest of a
//! round is the cluster's packet limit.
//!
//! Every node's position is polled, Galera nodes included: a read that must
//! see a write goes only to a node whose position covers it. After each
//! round the monitor estimates every node's lag and marks the nodes that
//! are behind (no reads), with `sprig_backend_node_behind` and the
//! `backend.node_behind` and `backend.node_caught_up` events.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_cluster::record::TopologyKind;
use sprig_common::ids::{ClusterId, NodeId};
use sprig_proto_mysql::client::MysqlClient;

use super::Backends;
use super::cluster::{ClusterView, NodeKey, NodeRef};
use super::health::{NodeRole, Observation};
use crate::reads::GtidSet;

const CHECK_TIMEOUT: Duration = Duration::from_secs(1);

const POLL_SQL: &str = "SELECT @@read_only, @@gtid_slave_pos, @@gtid_binlog_pos, \
    (SELECT COUNT(*) FROM information_schema.SLAVE_STATUS), @@global.max_allowed_packet";
const POLL_SQL_PLAIN: &str =
    "SELECT @@read_only, @@gtid_slave_pos, @@gtid_binlog_pos, 0, @@global.max_allowed_packet";
/// Galera: the GTIDs the node has applied are in `@@gtid_binlog_pos`;
/// with `wsrep_gtid_mode` on, the cluster's writes share one domain,
/// `wsrep_gtid_domain_id`, with the same sequence numbers on every node.
const POLL_SQL_GALERA: &str = "SELECT @@read_only, @@gtid_slave_pos, @@gtid_binlog_pos, 0, \
    @@global.max_allowed_packet, @@wsrep_gtid_mode, @@wsrep_gtid_domain_id";
const GALERA_SQL: &str =
    "SHOW GLOBAL STATUS WHERE Variable_name IN ('wsrep_local_state', 'wsrep_ready')";

/// A node's role from one poll: a writer only when it is writable and
/// known to replicate from nobody. A replica can be writable
/// (`read_only = 0`), so a node whose replication status couldn't be read
/// is never taken for a writer (it is `Unknown`, unless `read_only`).
fn role_of(topology: TopologyKind, read_only: bool, channels: Option<u64>) -> NodeRole {
    match (topology, channels) {
        (TopologyKind::Galera, _) => NodeRole::Galera,
        (_, Some(n)) if n > 0 => NodeRole::Replica,
        _ if read_only => NodeRole::Replica,
        (_, Some(_)) => NodeRole::Writer,
        (_, None) => NodeRole::Unknown,
    }
}

struct Polled {
    obs: Observation,
    slave: Option<GtidSet>,
    binlog: Option<GtidSet>,
    /// Galera with `wsrep_gtid_mode` on: `wsrep_gtid_domain_id`.
    galera_domain: Option<u32>,
    /// `@@global.max_allowed_packet`.
    max_packet: Option<usize>,
}

fn text(v: Option<&Option<bytes::Bytes>>) -> String {
    v.and_then(Option::as_ref)
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default()
}

async fn poll(c: &mut MysqlClient, topology: TopologyKind, health: bool) -> Result<Polled, String> {
    let first = if topology == TopologyKind::Galera {
        POLL_SQL_GALERA
    } else {
        POLL_SQL
    };
    // Without the replication status (an account that may not read
    // `SLAVE_STATUS`, an older server) the node's channels are unknown.
    let mut channels_known = true;
    let row = match c.query_first(first).await {
        Ok(r) => r,
        Err(e) if e.server_error().is_some() => {
            channels_known = false;
            c.query_first(POLL_SQL_PLAIN)
                .await
                .map_err(|e| e.to_string())?
        }
        Err(e) => return Err(e.to_string()),
    }
    .ok_or("no row")?;
    let read_only = text(row.first()) == "1";
    let slave = GtidSet::parse(&text(row.get(1))).ok();
    let binlog = GtidSet::parse(&text(row.get(2))).ok();
    let channels: u64 = text(row.get(3)).parse().unwrap_or(0);
    let max_packet = text(row.get(4)).parse().ok();
    let galera_domain = (text(row.get(5)) == "1")
        .then(|| text(row.get(6)).parse().ok())
        .flatten();
    let mut obs = Observation {
        role: role_of(topology, read_only, channels_known.then_some(channels)),
        read_only,
        galera_usable: None,
    };
    if health && topology == TopologyKind::Galera {
        let rs = c.query(GALERA_SQL).await.map_err(|e| e.to_string())?;
        let mut state = String::new();
        let mut ready = String::new();
        for r in rs.iter().filter(|r| !r.columns.is_empty()) {
            for row in r.text_rows().map_err(|e| e.to_string())? {
                match text(row.first()).to_ascii_lowercase().as_str() {
                    "wsrep_local_state" => state = text(row.get(1)),
                    "wsrep_ready" => ready = text(row.get(1)),
                    _ => {}
                }
            }
        }
        obs.galera_usable = Some(state == "4" && ready.eq_ignore_ascii_case("ON"));
    }
    Ok(Polled {
        obs,
        slave,
        binlog,
        galera_domain,
        max_packet,
    })
}

/// A monitor connection and whether it is `sprig_gw`'s.
type MonitorConn = (MysqlClient, bool);

async fn check(
    b: &Backends,
    view: &ClusterView,
    node: &NodeRef,
    conn: Option<MonitorConn>,
    health: bool,
) -> (Option<MonitorConn>, Result<Polled, String>) {
    let fut = async {
        let mut c = match conn {
            // A fallback connection goes once sprig_gw may be tried again.
            Some((c, gw)) if c.is_idle() && (gw || !b.gw_usable(&view.id)) => (c, gw),
            _ => b
                .monitor_connection(view, node, CHECK_TIMEOUT)
                .await
                .map_err(|e| e.message)?,
        };
        let r = poll(&mut c.0, view.topology, health).await;
        Ok::<_, String>((c, r))
    };
    match tokio::time::timeout(CHECK_TIMEOUT, fut).await {
        Ok(Ok((c, Ok(p)))) => (Some(c), Ok(p)),
        Ok(Ok((_, Err(e))) | Err(e)) => (None, Err(e)),
        Err(_) => (None, Err("check timed out".to_owned())),
    }
}

/// Publishes a node's lag and whether it is behind (more than `max_lag`;
/// an unknown lag is not behind), with an event when that changes.
fn note_lag(b: &Backends, key: &NodeKey, max_lag: Duration) {
    let s = b.nodes.status(key);
    let (cluster, node) = (key.cluster.to_string(), key.node.to_string());
    if let Some(lag) = s.lag {
        metrics::gauge!("sprig_backend_replica_lag_seconds", "cluster" => cluster.clone(), "node" => node.clone())
            .set(lag.as_secs_f64());
    }
    let behind = s.lag.is_some_and(|l| l > max_lag);
    metrics::gauge!("sprig_backend_node_behind", "cluster" => cluster, "node" => node)
        .set(if behind { 1.0 } else { 0.0 });
    if let Some(now) = b.nodes.set_behind(key, behind) {
        if now {
            tracing::warn!(event = "backend.node_behind", node = %key, lag = ?s.lag, "backend node behind: no reads until it catches up");
        } else {
            tracing::info!(event = "backend.node_caught_up", node = %key, "backend node caught up");
        }
    }
}

pub(super) async fn run(b: Arc<Backends>, cluster: ClusterId) {
    let mut conns: HashMap<NodeId, MonitorConn> = HashMap::new();
    let mut last_health: HashMap<NodeId, Instant> = HashMap::new();
    loop {
        let settings = b.settings(&cluster);
        if let Ok(view) = b.view(&cluster).await {
            let due: Vec<bool> = view
                .nodes
                .iter()
                .map(|n| {
                    last_health
                        .get(&n.node)
                        .is_none_or(|t| t.elapsed() >= settings.health_interval)
                })
                .collect();
            let checks = view.nodes.iter().zip(&due).map(|(n, &health)| {
                let c = conns.remove(&n.node);
                let (b, view) = (&b, &view);
                async move { (n, health, check(b, view, n, c, health).await) }
            });
            let galera = view.topology == TopologyKind::Galera;
            let mut galera_round: Vec<(NodeKey, Option<GtidSet>)> = Vec::new();
            let mut galera_domain = None;
            let mut max_packet: Option<usize> = None;
            for (n, health, (conn, result)) in futures::future::join_all(checks).await {
                let key = view.key(&n.node);
                if health {
                    last_health.insert(n.node.clone(), Instant::now());
                }
                match result {
                    Ok(p) => {
                        if let Some(m) = p.max_packet {
                            max_packet = Some(max_packet.map_or(m, |x| x.min(m)));
                        }
                        if galera {
                            galera_domain = galera_domain.or(p.galera_domain);
                            galera_round.push((key.clone(), p.binlog));
                        } else {
                            let is_writer = p.obs.role == NodeRole::Writer;
                            b.nodes.record_positions(&key, p.slave, p.binlog, is_writer);
                        }
                        if health && let Some(h) = b.nodes.record_pass(&key, p.obs) {
                            b.on_health_change(&key, h);
                        }
                        if let Some(c) = conn {
                            conns.insert(n.node.clone(), c);
                        }
                    }
                    Err(e) => {
                        b.nodes.clear_positions(&key);
                        if health {
                            tracing::debug!(node = %key, error = %e, "backend health check failed");
                            if let Some(h) = b
                                .nodes
                                .record_failure(&key, settings.health_failures_to_down)
                            {
                                b.on_health_change(&key, h);
                            }
                        }
                    }
                }
            }
            if galera {
                b.nodes
                    .record_galera_positions(&cluster, &galera_round, galera_domain);
            }
            if max_packet.is_some() {
                b.record_packet_limit(&cluster, max_packet);
            }
            let max_lag = if galera {
                settings.max_galera_lag
            } else {
                settings.max_replica_lag
            };
            for n in &view.nodes {
                note_lag(&b, &view.key(&n.node), max_lag);
            }
        }
        tokio::select! {
            () = b.shutdown.cancelled() => return,
            () = tokio::time::sleep(settings.position_poll) => {}
        }
    }
}

#[cfg(test)]
mod role_tests {
    use super::*;

    #[test]
    fn a_node_is_a_writer_only_when_known_to_replicate_from_nobody() {
        let p = TopologyKind::PrimaryReplica;
        assert_eq!(role_of(p, false, Some(0)), NodeRole::Writer);
        assert_eq!(role_of(p, false, Some(1)), NodeRole::Replica);
        assert_eq!(role_of(p, true, Some(0)), NodeRole::Replica);
        assert_eq!(role_of(p, true, None), NodeRole::Replica);
        // The replication status couldn't be read (the plain poll): a
        // writable replica must not look like a writer.
        assert_eq!(role_of(p, false, None), NodeRole::Unknown);
        assert_eq!(role_of(TopologyKind::Galera, false, None), NodeRole::Galera);
    }
}
