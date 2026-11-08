//! The probe suite (chapter 09, "Probes"; build steps 2 and 8).
//!
//! Probes run as `sprig_ctl` with `SHOW GLOBAL VARIABLES|STATUS WHERE
//! Variable_name IN (...)`, so a missing variable gives no row, not an
//! error. A [`ProbeDepth::Light`] probe runs every
//! `cluster.health_interval_seconds`; [`ProbeDepth::Full`] adds binlog
//! files, the horizon, grants and the clock; [`ProbeDepth::Deep`] adds the
//! active probes ([`active`]) on one writable node.

pub mod active;
pub mod feed;
pub mod grants;
pub mod parse;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sprig_common::ids::NodeId;
use sprig_common::time::{Clock, SystemClock};

pub use active::{ActiveProbes, FeatureFlags};
pub use feed::{FeedCheck, FeedGap, FeedLevel};
pub use grants::{Grant, GrantTarget, PrivilegeInfo};
pub use parse::{
    BinlogFacts, BinlogFormat, GaleraLocalState, GaleraStatus, HeartbeatReading, NodeFacts,
    RawCapture, ReadOnly, ReplicaChannel, RoutineLimits, RowImage, RowMetadata, SemiSyncFacts,
    ServerFacts, ServerVersion, TableCacheInfo, ThreadState, WaitPoint,
};

use crate::record::TopologyKind;
use crate::sql::{NodeEndpoint, ProbeError, ProbeSession, TextRow, literal};
use crate::sys;

/// Global variables every probe reads.
pub const VARIABLES: &[&str] = &[
    "version",
    "server_id",
    "lower_case_table_names",
    "default_storage_engine",
    "read_only",
    "performance_schema",
    "gtid_domain_id",
    "gtid_strict_mode",
    "gtid_binlog_pos",
    "gtid_current_pos",
    "log_bin",
    "log_slave_updates",
    "binlog_format",
    "binlog_row_image",
    "binlog_row_metadata",
    "binlog_expire_logs_seconds",
    "binlog_checksum",
    "binlog_storage_engine",
    "binlog_alter_two_phase",
    "binlog_annotate_row_events",
    "replicate_annotate_row_events",
    "legacy_xa_rollback_at_disconnect",
    "wsrep_on",
    "wsrep_gtid_mode",
    "wsrep_gtid_domain_id",
    "wsrep_osu_method",
    "wsrep_sync_wait",
    "rpl_semi_sync_master_enabled",
    "rpl_semi_sync_master_wait_point",
    "rpl_semi_sync_master_timeout",
    "rpl_semi_sync_slave_enabled",
    "table_open_cache",
    "table_definition_cache",
    "innodb_open_files",
    "open_files_limit",
    "max_connections",
    "stored_program_cache",
    "max_sp_recursion_depth",
    "max_statement_time",
];

/// Global status counters every probe reads.
pub const STATUS: &[&str] = &[
    "rpl_semi_sync_master_status",
    "rpl_semi_sync_master_clients",
    "rpl_semi_sync_slave_status",
    "wsrep_ready",
    "wsrep_connected",
    "wsrep_cluster_status",
    "wsrep_local_state",
    "wsrep_cluster_size",
    "wsrep_cluster_conf_id",
    "wsrep_flow_control_paused",
    "opened_tables",
    "open_tables",
    "table_open_cache_hits",
    "table_open_cache_misses",
    "memory_used",
    "threads_running",
];

/// How much a probe reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeDepth {
    /// Variables, status, replication channels, heartbeats.
    Light,
    /// Plus binlog files and horizon, grants and clock skew.
    Full,
    /// Plus the active probes on one writable node.
    Deep,
}

/// One node's probe result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeProbe {
    /// The node.
    pub node: NodeId,
    /// What was asked for.
    pub depth: ProbeDepth,
    /// When it finished, Unix milliseconds.
    pub at_unix_ms: u64,
    /// How long it took.
    pub took_ms: u64,
    /// The facts, or why the node couldn't be probed.
    pub result: Result<NodeFacts, ProbeError>,
    /// Active probes, on the one node they ran on.
    pub active: Option<ActiveProbes>,
}

impl NodeProbe {
    /// The facts, if the probe succeeded.
    #[must_use]
    pub fn facts(&self) -> Option<&NodeFacts> {
        self.result.as_ref().ok()
    }
}

/// Runs probes against nodes.
#[derive(Clone)]
pub struct Prober {
    timeout: Duration,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for Prober {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Prober")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl Prober {
    /// A prober whose round trips each time out after `timeout`
    /// (`cluster.probe_timeout_ms`).
    #[must_use]
    pub fn new(timeout: Duration) -> Self {
        Self::with_clock(timeout, Arc::new(SystemClock))
    }

    /// The same, reading wall time from `clock`.
    #[must_use]
    pub fn with_clock(timeout: Duration, clock: Arc<dyn Clock>) -> Self {
        Self { timeout, clock }
    }

    /// The round-trip timeout.
    #[must_use]
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Reads one node.
    ///
    /// # Errors
    /// The node couldn't be reached or a required statement failed.
    pub async fn capture(
        &self,
        ep: &NodeEndpoint,
        depth: ProbeDepth,
    ) -> Result<RawCapture, ProbeError> {
        let mut s = ProbeSession::connect(ep, self.timeout).await?;
        let r = self.capture_on(&mut s, depth).await;
        s.close().await;
        r
    }

    async fn capture_on(
        &self,
        s: &mut ProbeSession,
        depth: ProbeDepth,
    ) -> Result<RawCapture, ProbeError> {
        let vars = s
            .rows(&format!(
                "SHOW GLOBAL VARIABLES WHERE Variable_name IN ({})",
                in_list(VARIABLES)
            ))
            .await?;
        let status = s
            .rows(&format!(
                "SHOW GLOBAL STATUS WHERE Variable_name IN ({})",
                in_list(STATUS)
            ))
            .await?;
        let channels = s.rows("SHOW ALL SLAVES STATUS").await?;
        let mut raw = RawCapture {
            variables: RawCapture::name_value_map(&vars),
            status: RawCapture::name_value_map(&status),
            channels: RawCapture::channel_maps(channels),
            heartbeats: heartbeats(s).await?,
            ..RawCapture::default()
        };
        if depth >= ProbeDepth::Full {
            raw.binary_logs = binary_logs(s).await?;
            if let Some(oldest) = raw.binary_logs.as_ref().and_then(|l| l.first()) {
                raw.horizon = refused_as_none(
                    s.scalar(&format!(
                        "SELECT BINLOG_GTID_POS({}, 4)",
                        literal(&oldest.0)
                    ))
                    .await,
                )?
                .flatten();
            }
            raw.grants = refused_as_none(s.rows("SHOW GRANTS").await)?.map(|rows| {
                rows.into_iter()
                    .filter_map(|r| r.into_iter().next().and_then(|(_, v)| v))
                    .map(|l| parse::redact_grant(&l))
                    .collect()
            });
            raw.clock_skew_ms = self.clock_skew(s).await?;
        }
        Ok(raw)
    }

    async fn clock_skew(&self, s: &mut ProbeSession) -> Result<Option<i64>, ProbeError> {
        let before = unix_us(self.clock.wall());
        let server = s.scalar("SELECT UNIX_TIMESTAMP(NOW(6))").await?;
        let after = unix_us(self.clock.wall());
        let Some(server) = server.and_then(|v| decimal_to_us(&v)) else {
            return Ok(None);
        };
        Ok(Some((server - i64::midpoint(before, after)) / 1000))
    }

    /// Probes one node and parses the result; never fails.
    pub async fn probe(&self, ep: &NodeEndpoint, depth: ProbeDepth) -> NodeProbe {
        let start = self.clock.now();
        let result = self
            .capture(ep, depth)
            .await
            .map(|raw| NodeFacts::parse(&raw));
        if let Err(e) = &result {
            tracing::debug!(node = %ep.node, addr = %ep.addr(), error = %e, "probe failed");
        }
        NodeProbe {
            node: ep.node.clone(),
            depth,
            at_unix_ms: u64::try_from(unix_us(self.clock.wall()) / 1000).unwrap_or(0),
            took_ms: u64::try_from(self.clock.now().duration_since(start).as_millis())
                .unwrap_or(u64::MAX),
            result,
            active: None,
        }
    }

    /// Probes every node concurrently. With [`ProbeDepth::Deep`], the active
    /// probes then run on the first writable node (the first `Synced` node
    /// on Galera) and are attached to its result.
    pub async fn probe_cluster(
        &self,
        topology: TopologyKind,
        nodes: &[NodeEndpoint],
        depth: ProbeDepth,
    ) -> Vec<NodeProbe> {
        let base = depth.min(ProbeDepth::Full);
        let mut probes =
            futures::future::join_all(nodes.iter().map(|ep| self.probe(ep, base))).await;
        for p in &mut probes {
            p.depth = depth;
        }
        if depth == ProbeDepth::Deep
            && let Some(i) = active_target(topology, &probes)
        {
            let galera = probes[i].facts().is_some_and(|f| f.galera.is_some());
            match active::run(&nodes[i], self.timeout, galera).await {
                Ok(a) => probes[i].active = Some(a),
                Err(e) => {
                    tracing::warn!(node = %nodes[i].node, error = %e, "active probes failed");
                }
            }
        }
        probes
    }

    /// Creates `__sprig_sys` and its tables on `ep`.
    ///
    /// # Errors
    /// The node couldn't be reached or the DDL was refused.
    pub async fn ensure_sys_schema(&self, ep: &NodeEndpoint) -> Result<(), ProbeError> {
        let mut s = ProbeSession::connect(ep, self.timeout).await?;
        let r = sys::ensure(&mut s).await;
        s.close().await;
        r
    }
}

/// The node active probes run on: a writable node that isn't a replica; on
/// Galera, the first usable node.
#[must_use]
pub fn active_target(topology: TopologyKind, probes: &[NodeProbe]) -> Option<usize> {
    probes.iter().position(|p| {
        p.facts().is_some_and(|f| match (&f.galera, topology) {
            (Some(g), _) => g.usable(),
            (None, TopologyKind::Galera) => false,
            (None, _) => !f.is_replica() && f.writable(),
        })
    })
}

async fn heartbeats(s: &mut ProbeSession) -> Result<Option<Vec<(String, u64, i64)>>, ProbeError> {
    let rows = refused_as_none(
        s.rows(&format!(
            "SELECT writer_node, seq, TIMESTAMPDIFF(MICROSECOND, written_at, UTC_TIMESTAMP(6)) \
             FROM {}",
            sys::heartbeat_table()
        ))
        .await,
    )?;
    Ok(rows.map(|rows| {
        rows.into_iter()
            .filter_map(|r| {
                let mut it = r.into_iter().map(|(_, v)| v);
                let w = it.next()??;
                let seq = it.next()??.parse().ok()?;
                let age = it.next()??.parse().ok()?;
                Some((w, seq, age))
            })
            .collect()
    }))
}

async fn binary_logs(s: &mut ProbeSession) -> Result<Option<Vec<(String, u64)>>, ProbeError> {
    match s.rows("SHOW BINARY LOGS").await {
        Ok(rows) => Ok(Some(
            rows.into_iter()
                .filter_map(|r: TextRow| {
                    let mut it = r.into_iter().map(|(_, v)| v);
                    let name = it.next()??;
                    let size = it
                        .next()
                        .flatten()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0);
                    Some((name, size))
                })
                .collect(),
        )),
        // 1381: binary logging is off.
        Err(e) if e.errno == Some(1381) => Ok(Some(Vec::new())),
        Err(e) if e.is_server_error() => Ok(None),
        Err(e) => Err(e),
    }
}

fn refused_as_none<T>(r: Result<T, ProbeError>) -> Result<Option<T>, ProbeError> {
    match r {
        Ok(v) => Ok(Some(v)),
        Err(e) if e.is_server_error() => Ok(None),
        Err(e) => Err(e),
    }
}

fn in_list(names: &[&str]) -> String {
    names
        .iter()
        .map(|n| literal(n))
        .collect::<Vec<_>>()
        .join(", ")
}

fn unix_us(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_micros()).unwrap_or(i64::MAX),
        Err(e) => -i64::try_from(e.duration().as_micros()).unwrap_or(i64::MAX),
    }
}

/// `1762519022.252233` to microseconds.
fn decimal_to_us(s: &str) -> Option<i64> {
    let (int, frac) = s.split_once('.').unwrap_or((s, ""));
    let int: i64 = int.parse().ok()?;
    let mut frac: String = frac.chars().take(6).collect();
    while frac.len() < 6 {
        frac.push('0');
    }
    Some(int.checked_mul(1_000_000)? + frac.parse::<i64>().ok()?)
}

#[cfg(test)]
mod tests;
