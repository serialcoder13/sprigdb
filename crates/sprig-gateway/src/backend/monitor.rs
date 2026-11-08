//! The per-cluster monitor: health checks every `backend.health_interval_ms`
//! and position polling every `reads.replica_position_poll_ms` (chapter
//! 06, "Health checks" and "Read routing"), on one side connection per
//! node, all nodes in parallel, each check with a 1 s timeout.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_cluster::record::TopologyKind;
use sprig_common::ids::{ClusterId, NodeId};
use sprig_proto_mysql::client::MysqlClient;

use super::Backends;
use super::cluster::{ClusterView, NodeRef};
use super::health::{NodeRole, Observation};
use crate::reads::GtidSet;

const CHECK_TIMEOUT: Duration = Duration::from_secs(1);

const POLL_SQL: &str = "SELECT @@read_only, @@gtid_slave_pos, @@gtid_binlog_pos, \
    (SELECT COUNT(*) FROM information_schema.SLAVE_STATUS)";
const POLL_SQL_PLAIN: &str = "SELECT @@read_only, @@gtid_slave_pos, @@gtid_binlog_pos, 0";
const GALERA_SQL: &str =
    "SHOW GLOBAL STATUS WHERE Variable_name IN ('wsrep_local_state', 'wsrep_ready')";

struct Polled {
    obs: Observation,
    slave: Option<GtidSet>,
    binlog: Option<GtidSet>,
}

fn text(v: Option<&Option<bytes::Bytes>>) -> String {
    v.and_then(Option::as_ref)
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default()
}

async fn poll(c: &mut MysqlClient, topology: TopologyKind, health: bool) -> Result<Polled, String> {
    let row = match c.query_first(POLL_SQL).await {
        Ok(r) => r,
        Err(e) if e.server_error().is_some() => c
            .query_first(POLL_SQL_PLAIN)
            .await
            .map_err(|e| e.to_string())?,
        Err(e) => return Err(e.to_string()),
    }
    .ok_or("no row")?;
    let read_only = text(row.first()) == "1";
    let slave = GtidSet::parse(&text(row.get(1))).ok();
    let binlog = GtidSet::parse(&text(row.get(2))).ok();
    let channels: u64 = text(row.get(3)).parse().unwrap_or(0);
    let mut obs = Observation {
        role: match topology {
            TopologyKind::Galera => NodeRole::Galera,
            _ if channels > 0 || read_only => NodeRole::Replica,
            _ => NodeRole::Writer,
        },
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
    Ok(Polled { obs, slave, binlog })
}

async fn check(
    b: &Backends,
    view: &ClusterView,
    node: &NodeRef,
    conn: Option<MysqlClient>,
    health: bool,
) -> (Option<MysqlClient>, Result<Polled, String>) {
    let Some(target) = b.monitor_target(&view.id) else {
        return (None, Err("no monitor account".to_owned()));
    };
    let fut = async {
        let mut c = match conn {
            Some(c) if c.is_idle() => c,
            _ => b
                .side_connection(node, &target, CHECK_TIMEOUT)
                .await
                .map_err(|e| e.message)?,
        };
        let r = poll(&mut c, view.topology, health).await;
        Ok::<_, String>((c, r))
    };
    match tokio::time::timeout(CHECK_TIMEOUT, fut).await {
        Ok(Ok((c, Ok(p)))) => (Some(c), Ok(p)),
        Ok(Ok((_, Err(e))) | Err(e)) => (None, Err(e)),
        Err(_) => (None, Err("check timed out".to_owned())),
    }
}

pub(super) async fn run(b: Arc<Backends>, cluster: ClusterId) {
    let mut conns: HashMap<NodeId, MysqlClient> = HashMap::new();
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
            for (n, health, (conn, result)) in futures::future::join_all(checks).await {
                let key = view.key(&n.node);
                if health {
                    last_health.insert(n.node.clone(), Instant::now());
                }
                match result {
                    Ok(p) => {
                        let is_writer = p.obs.role == NodeRole::Writer;
                        b.nodes.record_positions(&key, p.slave, p.binlog, is_writer);
                        if let Some(lag) = b.nodes.status(&key).lag {
                            metrics::gauge!("sprig_backend_replica_lag_seconds", "cluster" => cluster.to_string(), "node" => n.node.to_string())
                                .set(lag.as_secs_f64());
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
        }
        tokio::select! {
            () = b.shutdown.cancelled() => return,
            () = tokio::time::sleep(settings.position_poll) => {}
        }
    }
}
