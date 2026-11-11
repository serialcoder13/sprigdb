//! Online builds on single nodes and primaries with replicas (chapter 13,
//! "Rollout", `Online`).
//!
//! ```text
//! 1. checks: the writer is writable; every replica replicates within the lag
//!    limit; no user DDL on the table; the index isn't there already (else
//!    done: an earlier attempt, or the replica validation kept it)
//! 2. up to lock_retries times:
//!      SET SESSION lock_wait_timeout = ddl_lock_wait
//!      ALTER TABLE s.t ADD INDEX IF NOT EXISTS x (...), ALGORITHM=INPLACE, LOCK=NONE
//!      watched every poll (node::judge_threads, replicas, read_only); killed on
//!      user DDL, lag, degradation, or a client queue behind a build that hasn't
//!      started
//!      1205 or a queue kill: user DDL holding the lock wins (abort); else wait
//!      lock_retry_pause and try again
//! 3. replicas: wait until every replica has the index, at most
//!    max(catch_up_floor, 2 × build); record the lag seen
//! ```
//!
//! The binary log stays on: the `ALTER` replicates like any statement. With
//! `binlog_alter_two_phase` the replicas start at once, otherwise after the
//! primary finishes, lagging by the build time.

use std::time::{Duration, Instant};

use sprig_cluster::sql::{NodeEndpoint, ProbeError};

use super::node::{BuildCheck, Node, Stop, judge_threads, ms, watched};
use super::{
    AbortReason, BuildOutcome, BuildReport, ClusterView, Nodes, RolloutSettings, ShardTable,
};
use crate::autoindex::model::IndexDef;

const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// What the online build watches.
struct Watch {
    mon: Node,
    replicas: Vec<Node>,
    table: ShardTable,
    ours: u64,
    started: bool,
    queue_kill_after: Duration,
    max_lag: Duration,
    max_lag_seen: Option<u64>,
}

impl Watch {
    async fn replicas_ok(&mut self) -> Option<(Stop, String)> {
        for r in &mut self.replicas {
            match r.replication().await {
                Ok(Some(rep)) => {
                    if let Some(l) = rep.lag {
                        self.max_lag_seen = Some(self.max_lag_seen.unwrap_or(0).max(l));
                    }
                    if !rep.running {
                        return Some((
                            Stop::Degraded,
                            format!("replica {}: {}", r.name, rep.problem.unwrap_or_default()),
                        ));
                    }
                    if rep.lag.is_some_and(|l| l > self.max_lag.as_secs()) {
                        return Some((
                            Stop::ReplicaLag,
                            format!(
                                "replica {} lags {} s (limit {} s)",
                                r.name,
                                rep.lag.unwrap_or(0),
                                self.max_lag.as_secs()
                            ),
                        ));
                    }
                }
                Ok(None) => {
                    return Some((
                        Stop::Degraded,
                        format!("{} is not a replica any more", r.name),
                    ));
                }
                Err(e) => return Some((Stop::Degraded, format!("replica {}: {e}", r.name))),
            }
        }
        None
    }
}

#[async_trait::async_trait]
impl BuildCheck for Watch {
    async fn check(&mut self) -> Option<(Stop, String)> {
        match self.mon.threads().await {
            Ok(th) => {
                if let Some(s) = judge_threads(
                    &th,
                    self.ours,
                    &self.table,
                    &mut self.started,
                    self.queue_kill_after,
                ) {
                    return Some(s);
                }
            }
            Err(e) => return Some((Stop::Degraded, format!("writer unreachable: {e}"))),
        }
        match self.mon.read_only().await {
            Ok(true) => return Some((Stop::Degraded, "the writer turned read-only".into())),
            Ok(false) => {}
            Err(e) => return Some((Stop::Degraded, format!("writer unreachable: {e}"))),
        }
        self.replicas_ok().await
    }
}

fn stop_reason(s: Stop) -> AbortReason {
    match s {
        Stop::UserDdl => AbortReason::UserDdl,
        Stop::ReplicaLag => AbortReason::ReplicaLag,
        Stop::Degraded | Stop::LockQueue => AbortReason::Degraded,
    }
}

async fn close(w: Watch) {
    w.mon.close().await;
    for r in w.replicas {
        r.close().await;
    }
}

/// Builds `def` on one shard of a single node or a primary with replicas.
#[allow(clippy::too_many_lines)] // one build, step after step
pub async fn build(
    view: &ClusterView,
    t: &ShardTable,
    def: &IndexDef,
    s: &RolloutSettings,
) -> BuildOutcome {
    let (writer, replica_eps): (&NodeEndpoint, &[NodeEndpoint]) = match &view.nodes {
        Nodes::Single { writer } => (writer, &[]),
        Nodes::PrimaryReplica { writer, replicas } => (writer, replicas.as_slice()),
        Nodes::Galera { .. } => {
            return BuildOutcome::Failed {
                error: "online builds don't run on Galera".into(),
            };
        }
    };
    let name = def.name();
    let mut report = BuildReport {
        shard: t.shard.clone(),
        method: "online".into(),
        already_present: false,
        build_ms: 0,
        lock_retries: 0,
        replicas_ms: None,
        max_replica_lag_s: None,
        nodes: Vec::new(),
    };

    // 1. Checks.
    let mon = match Node::connect(writer, PROBE_TIMEOUT).await {
        Ok(m) => m,
        Err(e) => return aborted(AbortReason::Degraded, format!("writer unreachable: {e}")),
    };
    let mut replicas = Vec::new();
    for ep in replica_eps {
        match Node::connect(ep, PROBE_TIMEOUT).await {
            Ok(n) => replicas.push(n),
            Err(e) => {
                mon.close().await;
                for r in replicas {
                    r.close().await;
                }
                return aborted(
                    AbortReason::Degraded,
                    format!("replica {} unreachable: {e}", ep.node),
                );
            }
        }
    }
    let mut w = Watch {
        mon,
        replicas,
        table: t.clone(),
        ours: 0,
        started: false,
        queue_kill_after: s.queue_kill_after,
        max_lag: s.abort_replica_lag,
        max_lag_seen: None,
    };
    let pre = async {
        if !w
            .mon
            .table_exists(t)
            .await
            .map_err(|e| (AbortReason::Degraded, format!("writer: {e}")))?
        {
            return Err((
                AbortReason::Degraded,
                format!("table {}.{} not found", t.schema, t.table),
            ));
        }
        let have = w
            .mon
            .indexes(t)
            .await
            .map_err(|e| (AbortReason::Degraded, format!("writer: {e}")))?;
        if let Some(i) = have.get(&name) {
            if i.parts == def.parts {
                return Ok(true);
            }
            return Err((
                AbortReason::Degraded,
                format!("{name} exists with another definition (drift)"),
            ));
        }
        if let Some((stop, why)) = w.check().await {
            return Err((stop_reason(stop), why));
        }
        Ok(false)
    }
    .await;
    let present = match pre {
        Ok(p) => p,
        Err(e) => {
            let (reason, detail) = e;
            close(w).await;
            if detail.contains("drift") {
                return BuildOutcome::Failed { error: detail };
            }
            return aborted(reason, detail);
        }
    };
    if present {
        report.already_present = true;
        let r = catch_up(&mut w, t, &name, Duration::ZERO, s, &mut report).await;
        close(w).await;
        return r.unwrap_or(BuildOutcome::Built(report));
    }

    // 2. A field-column definition's columns first (R6 round 6): instant,
    // replicated; MariaDB refuses them online in the index's statement.
    if let Some(sql) = crate::autoindex::kv::add_fields_sql(&t.schema, &t.table, def)
        && let Err(e) = w.mon.exec(&sql).await
    {
        close(w).await;
        return BuildOutcome::Failed {
            error: format!("field columns: {e}"),
        };
    }

    // 3. Build, with lock-safe retries.
    let sql = format!(
        "ALTER TABLE {}.{} {}, ALGORITHM=INPLACE, LOCK=NONE",
        sprig_common::names::quote(&t.schema),
        sprig_common::names::quote(&t.table),
        super::add_index_clause(def)
    );
    let mut tries = 0;
    let built_in = loop {
        tries += 1;
        let mut ddl = match Node::connect(writer, s.max_build + Duration::from_secs(60)).await {
            Ok(d) => d,
            Err(e) => {
                close(w).await;
                return aborted(AbortReason::Degraded, format!("DDL session: {e}"));
            }
        };
        if let Err(e) = ddl
            .exec(&format!(
                "SET SESSION lock_wait_timeout = {}",
                s.ddl_lock_wait.as_secs().max(1)
            ))
            .await
        {
            ddl.close().await;
            close(w).await;
            return BuildOutcome::Failed {
                error: format!("session: {e}"),
            };
        }
        w.ours = u64::from(ddl.id());
        w.started = false;
        let (res, stopped, took) = watched(&mut ddl, writer, &sql, s.poll, true, &mut w).await;
        ddl.close().await;
        let lock_wait = match (&res, &stopped) {
            (Ok(()), _) => break took,
            (_, Some((Stop::LockQueue, _))) => true,
            (_, Some((stop, why))) => {
                let (stop, why) = (*stop, why.clone());
                close(w).await;
                return aborted(stop_reason(stop), why);
            }
            (Err(e), None) => is_lock_wait(e),
        };
        if !lock_wait {
            let e = res.err().map(|e| e.to_string()).unwrap_or_default();
            close(w).await;
            if e.contains("1799") || e.to_ascii_lowercase().contains("online_alter_log") {
                return BuildOutcome::OnlineLogTooBig;
            }
            return BuildOutcome::Failed { error: e };
        }
        // A lock wait: user DDL holding the lock wins.
        report.lock_retries += 1;
        if let Some((Stop::UserDdl, why)) = w.check().await {
            close(w).await;
            return aborted(AbortReason::UserDdl, why);
        }
        if tries >= s.lock_retries {
            close(w).await;
            return BuildOutcome::LockTimeout { tries };
        }
        tracing::info!(index = %name, tries, "index build waited for its lock; retrying");
        tokio::time::sleep(s.lock_retry_pause).await;
    };
    report.build_ms = ms(built_in);
    report.nodes.push(super::NodeBuild {
        node: writer.node.to_string(),
        build_ms: report.build_ms,
        resync_ms: None,
        skipped: false,
    });

    // 3. Replicas.
    let r = catch_up(&mut w, t, &name, built_in, s, &mut report).await;
    close(w).await;
    r.unwrap_or(BuildOutcome::Built(report))
}

fn aborted(reason: AbortReason, detail: String) -> BuildOutcome {
    BuildOutcome::Aborted { reason, detail }
}

/// 1205 (`lock_wait_timeout`), or the build killed while waiting.
fn is_lock_wait(e: &ProbeError) -> bool {
    e.errno == Some(1205)
}

/// Waits until every replica has the index. `None` when done (the report
/// filled in); an outcome when replication broke meanwhile. A replica
/// still without it at the deadline is reported, not failed: it is
/// replaying the build.
async fn catch_up(
    w: &mut Watch,
    t: &ShardTable,
    name: &str,
    build: Duration,
    s: &RolloutSettings,
    report: &mut BuildReport,
) -> Option<BuildOutcome> {
    if w.replicas.is_empty() {
        return None;
    }
    let deadline = s.catch_up_floor.max(build * 2);
    let started = Instant::now();
    loop {
        let mut missing = 0;
        for r in &mut w.replicas {
            match r.indexes(t).await {
                Ok(m) if m.contains_key(name) => {}
                Ok(_) => missing += 1,
                Err(e) => {
                    return Some(aborted(
                        AbortReason::Degraded,
                        format!("replica {} unreachable: {e}", r.name),
                    ));
                }
            }
            if let Ok(Some(rep)) = r.replication().await
                && let Some(l) = rep.lag
            {
                w.max_lag_seen = Some(w.max_lag_seen.unwrap_or(0).max(l));
            }
        }
        report.max_replica_lag_s = w.max_lag_seen;
        if missing == 0 {
            report.replicas_ms = Some(ms(started.elapsed()));
            return None;
        }
        if started.elapsed() > deadline {
            tracing::warn!(index = name, missing, "replicas still building the index");
            return None;
        }
        // Replication that stops meanwhile is degradation; lag is expected.
        for r in &mut w.replicas {
            if let Ok(Some(rep)) = r.replication().await
                && !rep.running
            {
                return Some(aborted(
                    AbortReason::Degraded,
                    format!("replica {}: {}", r.name, rep.problem.unwrap_or_default()),
                ));
            }
        }
        tokio::time::sleep(s.poll).await;
    }
}
