//! Galera RSU validation (chapter 13 build step 8; "Validation harness",
//! Galera, desynced node).
//!
//! ```text
//! 1. checks: the cluster is Primary with at least min_cluster_size nodes,
//!    the node and every peer are Synced (one node out at a time), the
//!    index isn't there; note the node's global wsrep_OSU_method and
//!    wsrep_desync, its GTID position and the table's signature
//! 2. mark the node validating, writer weight 0
//! 3. snapshot the statistics rows
//! 4. on the node: SET SESSION wsrep_OSU_method = 'RSU', sql_log_bin = 0;
//!      ALTER TABLE ... ADD INDEX IF NOT EXISTS x (...)
//!    SET SESSION wsrep_OSU_method = 'TOI'
//!    watched: the cluster must stay Primary with its size and every peer
//!    Synced, or the build is killed and the validation aborted
//! 5. wait for the node to report Synced again, at most
//!    max(resync_floor, 2 × build time); else abort
//! 6. ANALYZE LOCAL (not replicated), replay; the node must stay Synced
//! 7. the table's signature changed (user DDL): abort
//! 8. always: drop the same way (RSU, then wait for Synced), restore the
//!    statistics, then prove the node is as it was: no index here or on
//!    any peer, Synced, the cluster size unchanged, global settings
//!    unchanged, no GTID from this node
//! 9. unmark, weight restored
//! ```
//!
//! Under RSU the node applies the DDL locally while write sets queue, so it
//! lags and serves nothing until it resyncs. An extra index on one node is
//! harmless for row-based write sets. The session variables die with the
//! session; the validator still sets `wsrep_OSU_method` back to `TOI`
//! after each statement, as the chapter does, and checks the globals.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_cluster::sql::{NodeEndpoint, ProbeError};
use sprig_common::Result;

use super::node::{
    NodeSql, NodeWatch, StatsSnapshot, add_index_sql, analyze_sql, drop_index_sql, own_new_gtids,
    run_watched,
};
use super::replica::drop_with_retries;
use super::{
    Method, NodeMarker, ValidationPlan, ValidationReport, Validator, abort, ms_since, now_ms,
    replay_on, report,
};
use crate::autoindex::replay::{ReplayGuard, VerdictKind};

const STATUS: [&str; 4] = [
    "wsrep_local_state_comment",
    "wsrep_cluster_size",
    "wsrep_cluster_status",
    "wsrep_ready",
];

/// Validates on one Galera node in rolling schema upgrade mode.
pub struct RsuValidator {
    node: NodeEndpoint,
    peers: Vec<NodeEndpoint>,
    marker: Arc<dyn NodeMarker>,
    probe_timeout: Duration,
}

impl std::fmt::Debug for RsuValidator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RsuValidator")
            .field("node", &self.node)
            .field("peers", &self.peers.len())
            .finish_non_exhaustive()
    }
}

impl RsuValidator {
    /// A validator for `node` (the driver's `validation_node()`); `peers`
    /// are the cluster's other nodes, checked before and during the
    /// validation and afterwards.
    #[must_use]
    pub fn new(node: NodeEndpoint, peers: Vec<NodeEndpoint>, marker: Arc<dyn NodeMarker>) -> Self {
        Self {
            node,
            peers,
            marker,
            probe_timeout: Duration::from_secs(10),
        }
    }
}

/// One node's Galera status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WsrepState {
    pub(crate) local: String,
    pub(crate) size: u32,
    pub(crate) status: String,
    pub(crate) ready: bool,
}

pub(crate) fn wsrep_state(m: &BTreeMap<String, String>) -> WsrepState {
    WsrepState {
        local: m
            .get("wsrep_local_state_comment")
            .cloned()
            .unwrap_or_default(),
        size: m
            .get("wsrep_cluster_size")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0),
        status: m.get("wsrep_cluster_status").cloned().unwrap_or_default(),
        ready: m
            .get("wsrep_ready")
            .is_some_and(|v| v.eq_ignore_ascii_case("ON")),
    }
}

/// Why the cluster isn't healthy enough, if it isn't. `need_synced`:
/// the node itself must be `Synced` (not while its RSU statement runs).
pub(crate) fn degraded(s: &WsrepState, min_size: u32, need_synced: bool) -> Option<String> {
    if !s.status.eq_ignore_ascii_case("Primary") {
        return Some(format!("cluster status {}", s.status));
    }
    if s.size < min_size {
        return Some(format!("cluster size {} < {min_size}", s.size));
    }
    if need_synced && (!s.local.eq_ignore_ascii_case("Synced") || !s.ready) {
        return Some(format!("node {} (ready {})", s.local, s.ready));
    }
    None
}

/// Watches the validation node and its peers.
struct GaleraWatch {
    mon: NodeSql,
    peers: Vec<(String, NodeSql)>,
    min_size: u32,
    /// The node itself must be Synced (off while its RSU statement runs).
    need_synced: bool,
}

impl GaleraWatch {
    async fn local(&mut self) -> std::result::Result<WsrepState, String> {
        self.mon
            .status(&STATUS)
            .await
            .map(|m| wsrep_state(&m))
            .map_err(|e| format!("node unreachable: {e}"))
    }
}

#[async_trait::async_trait]
impl NodeWatch for GaleraWatch {
    async fn check(&mut self) -> std::result::Result<(), String> {
        let s = self.local().await?;
        if let Some(why) = degraded(&s, self.min_size, self.need_synced) {
            return Err(why);
        }
        for (name, p) in &mut self.peers {
            let m = p
                .status(&STATUS)
                .await
                .map_err(|e| format!("peer {name} unreachable: {e}"))?;
            if let Some(why) = degraded(&wsrep_state(&m), self.min_size, true) {
                return Err(format!("peer {name}: {why}"));
            }
        }
        Ok(())
    }
    async fn kill_query(&mut self, id: u32) -> std::result::Result<(), ProbeError> {
        self.mon.exec(&format!("KILL QUERY {id}")).await
    }
}

#[async_trait::async_trait]
impl ReplayGuard for GaleraWatch {
    async fn check(&mut self) -> std::result::Result<(), String> {
        NodeWatch::check(self).await
    }
}

/// A session for RSU statements: binary log off, short lock waits.
async fn rsu_session(
    ep: &NodeEndpoint,
    timeout: Duration,
    lock_wait: Duration,
) -> std::result::Result<NodeSql, ProbeError> {
    super::replica::quiet_session(ep, timeout, lock_wait).await
}

/// Runs one statement in RSU mode, watched, then switches back to TOI and
/// waits for `Synced`. Returns the statement's time, the resync time, and
/// an abort reason.
async fn rsu_statement(
    ddl: &mut NodeSql,
    watch: &mut GaleraWatch,
    sql: &str,
    plan: &ValidationPlan,
) -> (
    std::result::Result<(), ProbeError>,
    u64,
    Option<u64>,
    Option<String>,
) {
    let set = &plan.settings;
    if let Err(e) = ddl.exec("SET SESSION wsrep_OSU_method = 'RSU'").await {
        return (Err(e), 0, None, None);
    }
    watch.need_synced = false;
    let started = Instant::now();
    let (res, killed) = run_watched(ddl, watch, sql, set.poll).await;
    let took = started.elapsed();
    let back = ddl.exec("SET SESSION wsrep_OSU_method = 'TOI'").await;
    let res = res.and(back);
    // Wait for Synced: max(floor, 2 × the statement's time).
    let deadline = set.resync_floor.max(took * 2);
    let waited = Instant::now();
    let mut synced = None;
    let mut last = String::new();
    while waited.elapsed() <= deadline {
        match watch.local().await {
            Ok(s) if s.local.eq_ignore_ascii_case("Synced") && s.ready => {
                synced = Some(ms_since(waited));
                break;
            }
            Ok(s) => last = s.local,
            Err(e) => last = e,
        }
        tokio::time::sleep(set.poll.min(Duration::from_millis(200))).await;
    }
    watch.need_synced = true;
    let abort = killed.or_else(|| {
        synced.is_none().then(|| {
            format!(
                "not Synced within {} ms after RSU (state {last})",
                deadline.as_millis()
            )
        })
    });
    (
        res,
        u64::try_from(took.as_millis()).unwrap_or(u64::MAX),
        synced,
        abort,
    )
}

#[async_trait::async_trait]
impl Validator for RsuValidator {
    #[allow(clippy::too_many_lines)]
    async fn validate(&self, plan: &ValidationPlan) -> Result<ValidationReport> {
        plan.check()?;
        let set = &plan.settings;
        let mut r = report(plan, Method::GaleraRsu, &self.node.node);
        let name = plan.index_name();
        let t = &plan.target;

        // 1. Checks.
        let mon = match NodeSql::connect(&self.node, self.probe_timeout).await {
            Ok(m) => m,
            Err(e) => {
                abort(&mut r, format!("node unreachable: {e}"));
                r.finished_at_ms = now_ms();
                return Ok(r);
            }
        };
        let mut peers = Vec::new();
        for p in &self.peers {
            match NodeSql::connect(p, self.probe_timeout).await {
                Ok(s) => peers.push((p.node.to_string(), s)),
                Err(e) => {
                    abort(&mut r, format!("peer {} unreachable: {e}", p.node));
                    r.finished_at_ms = now_ms();
                    return Ok(r);
                }
            }
        }
        let mut watch = GaleraWatch {
            mon,
            peers,
            min_size: set.min_cluster_size,
            need_synced: true,
        };
        let pre = async {
            NodeWatch::check(&mut watch).await?;
            let size = watch.local().await?.size;
            let existing = watch
                .mon
                .index_columns(t, &name)
                .await
                .map_err(|e| e.to_string())?;
            if !existing.is_empty() {
                return Err(format!("index_exists: {name} is already on the node"));
            }
            let globals = watch
                .mon
                .scalar(
                    "SELECT CONCAT_WS(',', @@global.wsrep_OSU_method, @@global.wsrep_desync, \
                     @@global.wsrep_on, @@global.wsrep_mode)",
                )
                .await
                .map_err(|e| e.to_string())?
                .unwrap_or_default();
            let (server_id, gtid) = watch.mon.gtid().await.map_err(|e| e.to_string())?;
            let sig = watch
                .mon
                .table_signature(t, &name)
                .await
                .map_err(|e| e.to_string())?;
            if sig.starts_with('|') {
                return Err(format!(
                    "table {}.{} not found on the node",
                    t.schema, t.table
                ));
            }
            Ok((size, globals, server_id, gtid, sig))
        }
        .await;
        let (size_before, globals_before, server_id, gtid_before, sig_before) = match pre {
            Ok(x) => x,
            Err(why) => {
                abort(&mut r, why);
                close(watch).await;
                r.finished_at_ms = now_ms();
                return Ok(r);
            }
        };
        // Never fewer nodes than the cluster had when it started.
        watch.min_size = watch.min_size.max(size_before);

        // 2. Out of service.
        if let Err(e) = self.marker.mark_validating(&self.node.node).await {
            abort(&mut r, format!("could not mark the node validating: {e}"));
            close(watch).await;
            r.finished_at_ms = now_ms();
            return Ok(r);
        }

        // 3. Statistics. With wsrep_mode REPLICATE_ARIA the statistics
        // tables would replicate: then no ANALYZE at all.
        let columns: Vec<String> = plan.candidate.def.columns().map(str::to_owned).collect();
        let aria_replicated = globals_before
            .to_ascii_uppercase()
            .contains("REPLICATE_ARIA");
        let stats = if set.analyze_after_build && !aria_replicated {
            match StatsSnapshot::take(&mut watch.mon, t, &columns, &name).await {
                Ok(s) => Some(s),
                Err(e) => {
                    r.node_check
                        .ok(format!("statistics not snapshotted ({e}); ANALYZE skipped"));
                    None
                }
            }
        } else {
            None
        };

        // 4-5. Build in RSU, resync.
        let ddl_timeout = set.max_build + Duration::from_secs(60);
        let mut ddl = rsu_session(&self.node, ddl_timeout, set.ddl_lock_wait).await;
        let mut built = false;
        match &mut ddl {
            Err(e) => abort(&mut r, format!("DDL session: {e}")),
            Ok(d) => {
                let sql = add_index_sql(t, &name, &plan.candidate.def.ddl_columns(), false);
                let (res, took, synced, why) = rsu_statement(d, &mut watch, &sql, plan).await;
                r.build_ms = Some(took);
                r.resync_ms = synced;
                match (res, why) {
                    (_, Some(why)) => abort(&mut r, format!("build: {why}")),
                    (Err(e), None) => abort(&mut r, format!("build failed: {e}")),
                    (Ok(()), None) => built = true,
                }
                // 6. Local statistics; ANALYZE LOCAL is not replicated.
                if built && stats.is_some() {
                    let started = Instant::now();
                    let (res, killed) =
                        run_watched(d, &mut watch, &analyze_sql(t, &name, &columns), set.poll)
                            .await;
                    match (res, killed) {
                        (Ok(()), None) => r.analyze_ms = Some(ms_since(started)),
                        (_, Some(why)) => abort(&mut r, format!("analyze stopped: {why}")),
                        (Err(e), None) => {
                            r.node_check
                                .ok(format!("ANALYZE failed ({e}); replaying without"));
                        }
                    }
                    built = r.verdict.kind != VerdictKind::Aborted;
                }
            }
        }

        if built {
            replay_on(&self.node, plan, &mut watch, &mut r).await;
            // 7. User DDL wins.
            match watch.mon.table_signature(t, &name).await {
                Ok(sig) if sig != sig_before => {
                    abort(&mut r, "user_ddl: the table changed during validation");
                }
                Ok(_) => {}
                Err(e) => abort(&mut r, format!("node unreachable after replay: {e}")),
            }
        }

        // 8. Clean up and prove it.
        let keep = built && plan.keep_if_accepted && r.verdict.kind == VerdictKind::Accepted;
        let mut ddl = match ddl {
            Ok(d) => Some(d),
            Err(_) => rsu_session(&self.node, ddl_timeout, set.ddl_lock_wait)
                .await
                .ok(),
        };
        if keep {
            r.index_kept = true;
            r.node_check.ok(format!("{name} kept for the rollout"));
        } else if let Some(d) = &mut ddl {
            let started = Instant::now();
            let drop_sql = drop_index_sql(t, &name);
            // The drop goes through RSU too; a failed watch doesn't stop it.
            let res = match d.exec("SET SESSION wsrep_OSU_method = 'RSU'").await {
                Ok(()) => {
                    let res = drop_with_retries(d, &drop_sql).await;
                    let back = d.exec("SET SESSION wsrep_OSU_method = 'TOI'").await;
                    res.and(back)
                }
                Err(e) => Err(e),
            };
            if let Err(e) = res {
                r.node_check.bad(format!("drop of {name} failed: {e}"));
            }
            // Wait for Synced again.
            let deadline = set
                .resync_floor
                .max(Duration::from_millis(ms_since(started)) * 2);
            let waited = Instant::now();
            let mut synced = false;
            while waited.elapsed() <= deadline {
                if watch
                    .local()
                    .await
                    .is_ok_and(|s| s.local.eq_ignore_ascii_case("Synced"))
                {
                    synced = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            r.drop_ms = Some(ms_since(started));
            if !synced {
                r.node_check.bad("node not Synced after the drop");
            }
            if let Some(s) = &stats {
                match s.restore(d).await {
                    Ok(()) => match StatsSnapshot::take(&mut watch.mon, t, &columns, &name).await {
                        Ok(now) if &now == s => {
                            r.node_check
                                .ok(format!("statistics rows restored ({} rows)", s.rows()));
                        }
                        Ok(_) => r.node_check.bad("statistics rows differ after restore"),
                        Err(e) => r.node_check.bad(format!("statistics not re-read: {e}")),
                    },
                    Err(e) => r.node_check.bad(format!("statistics not restored: {e}")),
                }
            }
        } else {
            r.node_check.bad("no session to drop the index with");
        }
        if let Some(d) = ddl {
            d.s.close().await;
        }
        verify(
            &mut watch,
            t,
            &name,
            keep,
            size_before,
            &globals_before,
            server_id,
            &gtid_before,
            &mut r,
        )
        .await;

        // 9. Back in service.
        if let Err(e) = self.marker.unmark(&self.node.node).await {
            r.node_check.bad(format!("could not unmark the node: {e}"));
        }
        close(watch).await;
        r.finished_at_ms = now_ms();
        tracing::info!(
            candidate = r.candidate_id,
            node = %r.node,
            verdict = r.verdict.kind.as_str(),
            reason = %r.verdict.reason,
            restored = r.node_check.restored,
            "RSU validation finished"
        );
        Ok(r)
    }
}

#[allow(clippy::too_many_arguments)]
async fn verify(
    watch: &mut GaleraWatch,
    t: &super::IndexTarget,
    name: &str,
    keep: bool,
    size_before: u32,
    globals_before: &str,
    server_id: u32,
    gtid_before: &str,
    r: &mut ValidationReport,
) {
    if !keep {
        match watch.mon.index_columns(t, name).await {
            Ok(c) if c.is_empty() => r.node_check.ok(format!("{name} absent")),
            Ok(_) => r.node_check.bad(format!("{name} still on the node")),
            Err(e) => r.node_check.bad(format!("index not re-checked: {e}")),
        }
    }
    for (p, s) in &mut watch.peers {
        match s.index_columns(t, name).await {
            Ok(c) if c.is_empty() => {}
            Ok(_) => r.node_check.bad(format!("{name} reached peer {p}")),
            Err(e) => r.node_check.bad(format!("peer {p} not re-checked: {e}")),
        }
    }
    match watch.local().await {
        Ok(s) if s.local.eq_ignore_ascii_case("Synced") && s.size >= size_before => {
            r.node_check.ok(format!("Synced, cluster size {}", s.size));
        }
        Ok(s) => r.node_check.bad(format!(
            "node {}, cluster size {} (was {size_before})",
            s.local, s.size
        )),
        Err(e) => r.node_check.bad(e),
    }
    match watch
        .mon
        .scalar(
            "SELECT CONCAT_WS(',', @@global.wsrep_OSU_method, @@global.wsrep_desync, \
             @@global.wsrep_on, @@global.wsrep_mode)",
        )
        .await
    {
        Ok(Some(g)) if g == globals_before => {
            r.node_check.ok(format!("globals unchanged ({g})"));
        }
        Ok(g) => r
            .node_check
            .bad(format!("globals changed: {globals_before} -> {g:?}")),
        Err(e) => r.node_check.bad(format!("globals not re-read: {e}")),
    }
    match watch.mon.gtid().await {
        Ok((_, after)) => {
            let errant = own_new_gtids(server_id, gtid_before, &after);
            if errant.is_empty() {
                r.node_check
                    .ok(format!("no GTID from server {server_id} (now {after})"));
            } else {
                r.node_check.bad(format!(
                    "GTIDs written by the validation node: {}",
                    errant.join(",")
                ));
            }
        }
        Err(e) => r.node_check.bad(format!("GTIDs not re-read: {e}")),
    }
}

async fn close(w: GaleraWatch) {
    w.mon.s.close().await;
    for (_, p) in w.peers {
        p.s.close().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(local: &str, size: &str, status: &str, ready: &str) -> BTreeMap<String, String> {
        [
            ("wsrep_local_state_comment", local),
            ("wsrep_cluster_size", size),
            ("wsrep_cluster_status", status),
            ("wsrep_ready", ready),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
    }

    #[test]
    fn health_rules() {
        let ok = wsrep_state(&m("Synced", "3", "Primary", "ON"));
        assert_eq!(degraded(&ok, 3, true), None);
        let desynced = wsrep_state(&m("Donor/Desynced", "3", "Primary", "ON"));
        assert_eq!(degraded(&desynced, 3, false), None);
        assert!(degraded(&desynced, 3, true).is_some());
        let small = wsrep_state(&m("Synced", "2", "Primary", "ON"));
        assert!(
            degraded(&small, 3, true)
                .unwrap_or_default()
                .contains("size 2")
        );
        let np = wsrep_state(&m("Synced", "3", "non-Primary", "OFF"));
        assert!(
            degraded(&np, 3, false)
                .unwrap_or_default()
                .contains("non-Primary")
        );
        let not_ready = wsrep_state(&m("Synced", "3", "Primary", "OFF"));
        assert!(degraded(&not_ready, 3, true).is_some());
    }
}
