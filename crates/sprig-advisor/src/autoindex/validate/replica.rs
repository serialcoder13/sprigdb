//! Replica-build validation (chapter 13 build step 7; "Validation
//! harness", replica build), as the month-1 spike measured it
//! (`decisions/spike-replica-index.md`).
//!
//! ```text
//! 1. checks: the node replicates (IO and SQL threads running, lag under
//!    auto_index.abort_replica_lag_seconds), the index isn't there yet;
//!    note @@server_id, @@gtid_binlog_pos and the table's signature
//! 2. mark the node validating (reads stop going there)
//! 3. snapshot the statistics rows ANALYZE will touch
//! 4. on the node, binlog off:
//!      ALTER TABLE ... ADD INDEX IF NOT EXISTS x (...), ALGORITHM=INPLACE, LOCK=NONE
//!      ANALYZE LOCAL TABLE ... PERSISTENT FOR COLUMNS (...) INDEXES (x)
//!    while a second session watches replication; a stopped thread or
//!    lag over the limit kills the build (KILL QUERY) and aborts
//! 5. replay, the same watch before every statement
//! 6. the table's signature changed (user DDL): abort
//! 7. always: DROP INDEX IF EXISTS x and restore the statistics rows,
//!    binlog off (unless accepted and kept), then prove the node is as it
//!    was: no index, no GTID from this server, statistics rows identical,
//!    both replication threads running
//! 8. unmark
//! ```
//!
//! Every statement that changes the node runs on a session with
//! `sql_log_bin = 0`, so nothing reaches the binary log, downstream
//! replicas or the mover, and the node's GTID position keeps following
//! the primary. The account needs `BINLOG ADMIN` and `READ_ONLY ADMIN`
//! (chapter 09), plus `SELECT`, `INSERT` and `DELETE` on
//! `mysql.table_stats`, `column_stats` and `index_stats` to put the
//! statistics back.

use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_cluster::sql::{NodeEndpoint, ProbeError};
use sprig_common::Result;

use super::node::{
    NodeSql, NodeWatch, StatsSnapshot, add_def_sql, analyze_sql, col, drop_index_sql,
    own_new_gtids, run_watched,
};
use super::{
    Method, NodeMarker, ValidationPlan, ValidationReport, Validator, abort, ms_since, now_ms,
    replay_on, report,
};
use crate::autoindex::replay::{ReplayGuard, VerdictKind};

/// Validates on one replica.
pub struct ReplicaValidator {
    node: NodeEndpoint,
    marker: Arc<dyn NodeMarker>,
    probe_timeout: Duration,
}

impl std::fmt::Debug for ReplicaValidator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicaValidator")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

impl ReplicaValidator {
    /// A validator for the replica `node` (from the driver's
    /// `validation_node()` and the `sprig_ctl` connector).
    #[must_use]
    pub fn new(node: NodeEndpoint, marker: Arc<dyn NodeMarker>) -> Self {
        Self {
            node,
            marker,
            probe_timeout: Duration::from_secs(10),
        }
    }
}

/// Replication as `SHOW SLAVE STATUS` shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplicaState {
    io: String,
    sql: String,
    lag: Option<u64>,
    sql_errno: String,
}

pub(crate) async fn replica_state(
    n: &mut NodeSql,
) -> std::result::Result<Option<ReplicaState>, ProbeError> {
    let rows = n.rows("SHOW SLAVE STATUS").await?;
    Ok(rows.first().map(|r| ReplicaState {
        io: col(r, "Slave_IO_Running").unwrap_or_default().to_owned(),
        sql: col(r, "Slave_SQL_Running").unwrap_or_default().to_owned(),
        lag: col(r, "Seconds_Behind_Master").and_then(|v| v.parse().ok()),
        sql_errno: col(r, "Last_SQL_Errno").unwrap_or("0").to_owned(),
    }))
}

/// Why a replica isn't fit to validate on, if it isn't.
pub(crate) fn unfit(state: Option<&ReplicaState>, max_lag: Duration) -> Option<String> {
    let Some(s) = state else {
        return Some("not a replica (no replication configured; promoted?)".into());
    };
    if !s.sql.eq_ignore_ascii_case("Yes") {
        return Some(format!(
            "replica SQL thread not running ({}, errno {})",
            s.sql, s.sql_errno
        ));
    }
    if !s.io.eq_ignore_ascii_case("Yes") {
        return Some(format!("replica IO thread not running ({})", s.io));
    }
    match s.lag {
        None => Some("replica lag unknown".into()),
        Some(l) if l > max_lag.as_secs() => Some(format!("replica lagging {l} s")),
        Some(_) => None,
    }
}

/// Watches replication on the validation node.
struct ReplicaWatch {
    mon: NodeSql,
    max_lag: Duration,
}

#[async_trait::async_trait]
impl NodeWatch for ReplicaWatch {
    async fn check(&mut self) -> std::result::Result<(), String> {
        match replica_state(&mut self.mon).await {
            Ok(s) => unfit(s.as_ref(), self.max_lag).map_or(Ok(()), Err),
            Err(e) => Err(format!("node unreachable: {e}")),
        }
    }
    async fn kill_query(&mut self, id: u32) -> std::result::Result<(), ProbeError> {
        self.mon.exec(&format!("KILL QUERY {id}")).await
    }
}

#[async_trait::async_trait]
impl ReplayGuard for ReplicaWatch {
    async fn check(&mut self) -> std::result::Result<(), String> {
        NodeWatch::check(self).await
    }
}

/// Opens a session with the binary log off and short lock waits.
pub(crate) async fn quiet_session(
    ep: &NodeEndpoint,
    timeout: Duration,
    lock_wait: Duration,
) -> std::result::Result<NodeSql, ProbeError> {
    let mut s = NodeSql::connect(ep, timeout).await?;
    s.exec("SET SESSION sql_log_bin = 0").await?;
    s.exec(&format!(
        "SET SESSION lock_wait_timeout = {}",
        lock_wait.as_secs().max(1)
    ))
    .await?;
    Ok(s)
}

/// Drops the index with retries on lock waits (5 tries, 2 s apart).
pub(crate) async fn drop_with_retries(
    ddl: &mut NodeSql,
    sql: &str,
) -> std::result::Result<(), ProbeError> {
    let mut last = None;
    for attempt in 0..5 {
        match ddl.exec(sql).await {
            Ok(()) => return Ok(()),
            // 1205: lock wait timeout; the next try may get the lock.
            Err(e) if e.errno == Some(1205) => {
                tracing::warn!(attempt, "lock wait dropping the validation index; retrying");
                last = Some(e);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => return Err(e),
        }
    }
    Err(last.unwrap_or_else(|| ProbeError {
        errno: None,
        message: "drop failed".into(),
        timeout: false,
    }))
}

#[async_trait::async_trait]
impl Validator for ReplicaValidator {
    #[allow(clippy::too_many_lines)]
    async fn validate(&self, plan: &ValidationPlan) -> Result<ValidationReport> {
        plan.check()?;
        let set = &plan.settings;
        let mut r = report(plan, Method::Replica, &self.node.node);
        let name = plan.index_name();
        let t = &plan.target;

        // 1. Checks, before anything changes.
        let mut mon = match NodeSql::connect(&self.node, self.probe_timeout).await {
            Ok(m) => m,
            Err(e) => {
                abort(&mut r, format!("node unreachable: {e}"));
                r.finished_at_ms = now_ms();
                return Ok(r);
            }
        };
        let pre = async {
            let state = replica_state(&mut mon).await.map_err(|e| e.to_string())?;
            if let Some(why) = unfit(state.as_ref(), set.abort_replica_lag) {
                return Err(why);
            }
            let existing = mon
                .index_columns(t, &name)
                .await
                .map_err(|e| e.to_string())?;
            if !existing.is_empty() {
                // Kept from an earlier accepted validation, or a stray the
                // reconciler will handle: never touch it here.
                return Err(format!("index_exists: {name} is already on the node"));
            }
            let (server_id, gtid) = mon.gtid().await.map_err(|e| e.to_string())?;
            let sig = mon
                .table_signature(t, &name)
                .await
                .map_err(|e| e.to_string())?;
            if sig.starts_with('|') {
                return Err(format!(
                    "table {}.{} not found on the node",
                    t.schema, t.table
                ));
            }
            Ok((server_id, gtid, sig))
        }
        .await;
        let (server_id, gtid_before, sig_before) = match pre {
            Ok(x) => x,
            Err(why) => {
                mon.s.close().await;
                abort(&mut r, why);
                r.finished_at_ms = now_ms();
                return Ok(r);
            }
        };

        // 2. Out of service.
        if let Err(e) = self.marker.mark_validating(&self.node.node).await {
            mon.s.close().await;
            abort(&mut r, format!("could not mark the node validating: {e}"));
            r.finished_at_ms = now_ms();
            return Ok(r);
        }

        let columns: Vec<String> = plan.candidate.def.columns().map(str::to_owned).collect();
        // 3. Statistics as they were.
        let stats = if set.analyze_after_build {
            match StatsSnapshot::take(&mut mon, t, &columns, &name).await {
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

        let mut watch = ReplicaWatch {
            mon,
            max_lag: set.abort_replica_lag,
        };
        let ddl_timeout = set.max_build + Duration::from_secs(60);
        let mut ddl = quiet_session(&self.node, ddl_timeout, set.ddl_lock_wait).await;

        // 4. Build, watched.
        let mut built = false;
        match &mut ddl {
            Err(e) => abort(&mut r, format!("DDL session: {e}")),
            Ok(ddl) => {
                let sql = add_def_sql(t, &name, &plan.candidate.def, true);
                let started = Instant::now();
                let (res, killed) = run_watched(ddl, &mut watch, &sql, set.poll).await;
                r.build_ms = Some(ms_since(started));
                match (res, killed) {
                    (_, Some(why)) => abort(&mut r, format!("build stopped: {why}")),
                    (Err(e), None) if e.errno == Some(1205) => {
                        abort(&mut r, "build: lock wait timeout");
                    }
                    (Err(e), None) => abort(&mut r, format!("build failed: {e}")),
                    (Ok(()), None) => built = true,
                }
                if built && stats.is_some() {
                    let started = Instant::now();
                    let (res, killed) =
                        run_watched(ddl, &mut watch, &analyze_sql(t, &name, &columns), set.poll)
                            .await;
                    match (res, killed) {
                        (Ok(()), None) => r.analyze_ms = Some(ms_since(started)),
                        (_, Some(why)) => {
                            abort(&mut r, format!("analyze stopped: {why}"));
                            built = false;
                        }
                        (Err(e), None) => {
                            r.node_check
                                .ok(format!("ANALYZE failed ({e}); replaying without"));
                        }
                    }
                }
            }
        }

        // 5. Replay, under the same watch.
        if built {
            replay_on(&self.node, plan, &mut watch, &mut r).await;
            // 6. User DDL during the validation wins.
            match watch.mon.table_signature(t, &name).await {
                Ok(sig) if sig != sig_before => {
                    abort(&mut r, "user_ddl: the table changed during validation");
                }
                Ok(_) => {}
                Err(e) => abort(&mut r, format!("node unreachable after replay: {e}")),
            }
        }

        // 7. Clean up and prove it.
        let keep = built && plan.keep_if_accepted && r.verdict.kind == VerdictKind::Accepted;
        cleanup(
            &self.node,
            plan,
            &name,
            keep,
            ddl.ok(),
            &mut watch.mon,
            stats.as_ref(),
            (server_id, &gtid_before),
            &mut r,
        )
        .await;

        // 8. Back in service.
        if let Err(e) = self.marker.unmark(&self.node.node).await {
            r.node_check.bad(format!("could not unmark the node: {e}"));
        }
        watch.mon.s.close().await;
        r.finished_at_ms = now_ms();
        tracing::info!(
            candidate = r.candidate_id,
            node = %r.node,
            verdict = r.verdict.kind.as_str(),
            reason = %r.verdict.reason,
            restored = r.node_check.restored,
            "replica validation finished"
        );
        Ok(r)
    }
}

#[allow(clippy::too_many_arguments)]
async fn cleanup(
    ep: &NodeEndpoint,
    plan: &ValidationPlan,
    name: &str,
    keep: bool,
    ddl: Option<NodeSql>,
    mon: &mut NodeSql,
    stats: Option<&StatsSnapshot>,
    (server_id, gtid_before): (u32, &str),
    r: &mut ValidationReport,
) {
    let set = &plan.settings;
    let t = &plan.target;
    // A killed or broken DDL session is replaced.
    let mut ddl = match ddl {
        Some(d) => d,
        None => match quiet_session(ep, set.max_build, set.ddl_lock_wait).await {
            Ok(d) => d,
            Err(e) => {
                r.node_check
                    .bad(format!("no session to clean up with: {e}"));
                return;
            }
        },
    };
    if keep {
        r.index_kept = true;
        r.node_check.ok(format!("{name} kept for the rollout"));
    } else {
        let started = Instant::now();
        let mut res = drop_with_retries(&mut ddl, &drop_index_sql(t, name)).await;
        if res.as_ref().is_err_and(|e| !e.is_server_error()) {
            // The session broke; one more try on a fresh one.
            match quiet_session(ep, set.max_build, set.ddl_lock_wait).await {
                Ok(d) => {
                    ddl = d;
                    res = drop_with_retries(&mut ddl, &drop_index_sql(t, name)).await;
                }
                Err(e) => res = Err(e),
            }
        }
        r.drop_ms = Some(ms_since(started));
        if let Err(e) = res {
            r.node_check.bad(format!("drop of {name} failed: {e}"));
        }
        if let Some(s) = stats {
            match s.restore(&mut ddl).await {
                Ok(()) => {
                    match StatsSnapshot::take(
                        mon,
                        t,
                        &plan
                            .candidate
                            .def
                            .columns()
                            .map(str::to_owned)
                            .collect::<Vec<_>>(),
                        name,
                    )
                    .await
                    {
                        Ok(now) if &now == s => r
                            .node_check
                            .ok(format!("statistics rows restored ({} rows)", s.rows())),
                        Ok(_) => r.node_check.bad("statistics rows differ after restore"),
                        Err(e) => r.node_check.bad(format!("statistics not re-read: {e}")),
                    }
                }
                Err(e) => r.node_check.bad(format!("statistics not restored: {e}")),
            }
        }
        match mon.index_columns(t, name).await {
            Ok(c) if c.is_empty() => r.node_check.ok(format!("{name} absent")),
            Ok(_) => r.node_check.bad(format!("{name} still on the node")),
            Err(e) => r.node_check.bad(format!("index not re-checked: {e}")),
        }
    }
    ddl.s.close().await;
    match mon.gtid().await {
        Ok((_, after)) => {
            let errant = own_new_gtids(server_id, gtid_before, &after);
            if errant.is_empty() {
                r.node_check
                    .ok(format!("no GTID from server {server_id} (now {after})"));
            } else {
                r.node_check.bad(format!(
                    "errant GTIDs from this replica: {}",
                    errant.join(",")
                ));
            }
        }
        Err(e) => r.node_check.bad(format!("GTIDs not re-read: {e}")),
    }
    match replica_state(mon).await {
        Ok(Some(s)) if s.io.eq_ignore_ascii_case("Yes") && s.sql.eq_ignore_ascii_case("Yes") => {
            r.node_check.ok(format!(
                "replication running, lag {} s",
                s.lag.map_or("?".into(), |l| l.to_string())
            ));
        }
        Ok(s) => r.node_check.bad(format!("replication not running: {s:?}")),
        Err(e) => r.node_check.bad(format!("replication not re-checked: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st(io: &str, sql: &str, lag: Option<u64>) -> ReplicaState {
        ReplicaState {
            io: io.into(),
            sql: sql.into(),
            lag,
            sql_errno: "0".into(),
        }
    }

    #[test]
    fn fitness() {
        let max = Duration::from_secs(300);
        assert_eq!(unfit(Some(&st("Yes", "Yes", Some(0))), max), None);
        assert_eq!(unfit(Some(&st("Yes", "Yes", Some(300))), max), None);
        assert!(
            unfit(Some(&st("Yes", "Yes", Some(301))), max)
                .unwrap_or_default()
                .contains("lagging 301")
        );
        assert!(
            unfit(Some(&st("Yes", "No", None)), max)
                .unwrap_or_default()
                .contains("SQL thread")
        );
        assert!(
            unfit(Some(&st("Connecting", "Yes", Some(0))), max)
                .unwrap_or_default()
                .contains("IO thread")
        );
        assert!(
            unfit(Some(&st("Yes", "Yes", None)), max)
                .unwrap_or_default()
                .contains("unknown")
        );
        assert!(
            unfit(None, max)
                .unwrap_or_default()
                .contains("not a replica")
        );
    }
}
