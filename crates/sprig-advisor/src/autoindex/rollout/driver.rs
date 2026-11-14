//! The rollout state machine over the store: one step per call, every
//! change recorded in the tenant catalog before and after the build, so a
//! new leader resumes where the old one stopped (after a cluster failover,
//! progress is re-read from the shards).
//!
//! `rolling_out` rows move to `observing` when every shard is built and to
//! `failed` after `max_failures`; nothing else changes their state here.
//! Per-shard states go `pending → building → built` in
//! `auto_indexes.shards`; attempts, failures and the next attempt time go
//! to `observed.rollout` ([`Progress`]).

use std::time::{Duration, SystemTime};

use mysql_async::prelude::Queryable;
use sprig_common::Result;
use sprig_common::ids::TenantId;

use super::node::{Node, is_sprig_build, ms};
use super::{
    AbortReason, BuildMethod, BuildOutcome, BuildReport, BuildSlots, ClusterView, CopySwapHook,
    CopySwapRequest, CopySwapStatus, Progress, RolloutSettings, ShardLock, ShardLocks, ShardTable,
    add_index_clause, choose_method, galera, online, unix_ms, window_gate,
};
use crate::autoindex::policy::BuildWindow;
use crate::autoindex::store::{
    AutoIndexRow, AutoIndexStore, DecisionLog, IndexDecision, IndexState, ShardBuild, StateChange,
};
use crate::autoindex::validate::NodeMarker;

/// Everything one rollout step needs besides the row.
pub struct RolloutCx<'a> {
    /// The tenant.
    pub tenant: TenantId,
    /// The cluster holding the object's shards (one for now).
    pub view: &'a ClusterView,
    /// The object's shards, in rollout order (the validation or canary
    /// shard first, then by benefit).
    pub shards: &'a [ShardTable],
    /// Data plus indexes of the table, bytes (largest shard).
    pub table_bytes: u64,
    /// Settings.
    pub settings: &'a RolloutSettings,
    /// `build_window = auto`: the least busy window, when known.
    pub auto_window: Option<BuildWindow>,
    /// Slots.
    pub slots: &'a dyn BuildSlots,
    /// Builds the caller knows of that hold slots elsewhere (the
    /// scheduler's build jobs on the cluster).
    pub external_builds: u32,
    /// The mover's shard locks.
    pub locks: &'a dyn ShardLocks,
    /// The mover's copy-and-swap; `None` until it exists.
    pub mover: Option<&'a dyn CopySwapHook>,
    /// Takes Galera nodes out of service during rolling RSU builds.
    pub marker: &'a dyn NodeMarker,
    /// Decisions.
    pub decisions: &'a dyn DecisionLog,
    /// Now.
    pub now: SystemTime,
}

/// What one step did.
#[derive(Debug, Clone, PartialEq)]
pub enum Step {
    /// The row isn't `rolling_out` (any more).
    NotRollingOut,
    /// Nothing ran: a gate is closed or the row waits for its next try.
    Wait {
        /// Why.
        reason: String,
        /// Look again after this long.
        retry_after: Duration,
    },
    /// A shard was built; more remain.
    ShardBuilt {
        /// The build.
        report: BuildReport,
        /// The next shard after this long (`rollout_pause`).
        next_after: Duration,
    },
    /// Every shard is built: the row is `observing`.
    Observing {
        /// The last build, when this step ran one.
        report: Option<BuildReport>,
    },
    /// The build didn't go through; tried again later.
    Retry {
        /// What happened.
        outcome: BuildOutcome,
        /// When.
        retry_after: Duration,
    },
    /// The rollout failed for good: the row is `failed`.
    Failed {
        /// The last error.
        reason: String,
    },
}

/// Time until the window opens again after the current opening: for a
/// fixed window, its next start (at least a minute away); otherwise an
/// hour. Used after a build ran out of lock retries ("the next window").
#[must_use]
pub fn next_window(window: BuildWindow, auto: Option<BuildWindow>, now: SystemTime) -> Duration {
    let w = match (window, auto) {
        (BuildWindow::Auto, Some(a)) => a,
        (w, _) => w,
    };
    match w {
        BuildWindow::Fixed { start, end } if start != end => {
            let secs = now
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs());
            let minute = (secs / 60) % 1440;
            let ahead = (u64::from(start) + 1440 - minute) % 1440;
            let ahead = if ahead == 0 { 1440 } else { ahead };
            Duration::from_secs(ahead * 60 - secs % 60)
        }
        _ => Duration::from_secs(3600),
    }
}

/// Advances one `rolling_out` index by at most one shard build.
///
/// # Errors
/// Catalog errors; nothing on the shards is left half-recorded (a
/// `building` shard is re-checked on the next step).
#[allow(clippy::too_many_lines, clippy::many_single_char_names)] // the state machine, edge by edge
pub async fn advance<Q: Queryable + Send>(
    store: &AutoIndexStore,
    q: &mut Q,
    cx: &RolloutCx<'_>,
    row: &AutoIndexRow,
) -> Result<Step> {
    if row.state != IndexState::RollingOut {
        return Ok(Step::NotRollingOut);
    }
    let Some(def) = row.def() else {
        return fail(
            store,
            q,
            cx,
            row,
            Progress::of(row.observed.as_ref()),
            "advice is never built",
        )
        .await;
    };
    let s = cx.settings;
    let now_ms = unix_ms(cx.now);
    let mut p = Progress::of(row.observed.as_ref());
    if p.not_before_ms > now_ms {
        return Ok(Step::Wait {
            reason: p
                .waiting
                .clone()
                .unwrap_or_else(|| "waiting for the next try".into()),
            retry_after: Duration::from_millis(p.not_before_ms - now_ms),
        });
    }
    let pending = cx
        .shards
        .iter()
        .find(|t| row.shards.get(&t.shard) != Some(&ShardBuild::Built));
    let Some(t) = pending else {
        return finish(store, q, cx, row, p, None).await;
    };
    let wait = |reason: String, after: Duration| Step::Wait {
        reason,
        retry_after: after,
    };

    // Gates.
    let method = choose_method(cx.view, cx.table_bytes, &s.galera, cx.mover.is_some());
    if let BuildMethod::Recommend(why) = &method {
        return Ok(wait(format!("no safe build method: {why}"), s.retry_after));
    }
    if let Some(why) = &cx.view.unhealthy {
        return Ok(wait(format!("cluster unhealthy: {why}"), s.retry_after));
    }
    if let Err(opens_in) = window_gate(s.build_window, cx.auto_window, cx.now) {
        return Ok(wait("outside the build window".into(), opens_in));
    }
    match cx.locks.state(cx.tenant, &t.shard).await {
        Ok(ShardLock::Free) => {}
        Ok(ShardLock::Moving(why)) => {
            return Ok(wait(
                format!("shard {} is moving: {why}", t.shard),
                s.retry_after,
            ));
        }
        Err(e) => return Ok(wait(format!("shard lock unreadable: {e}"), s.retry_after)),
    }
    // No build while a distributed schema change holds the table.
    match schema_change_on(store, q, row.object).await {
        Ok(None) => {}
        Ok(Some(job)) => {
            return Ok(wait(
                format!("a schema change (job {job}) holds the table"),
                s.poll.max(Duration::from_secs(30)),
            ));
        }
        Err(e) => return Ok(wait(format!("catalog unreadable: {e}"), s.retry_after)),
    }
    let running = match running_builds(cx.view).await {
        Ok(n) => n,
        Err(e) => return Ok(wait(format!("writer unreachable: {e}"), s.retry_after)),
    };
    let Some(slot) = cx.slots.try_acquire(
        &cx.view.cluster,
        s.max_builds_per_cluster,
        running + cx.external_builds,
    ) else {
        return Ok(wait(
            format!(
                "no build slot on {} ({} of {} in use)",
                cx.view.cluster,
                running + cx.external_builds,
                s.max_builds_per_cluster
            ),
            s.poll.max(Duration::from_secs(30)),
        ));
    };

    // Build.
    store
        .set_shard(q, row.index_id, &t.shard, ShardBuild::Building)
        .await?;
    p.attempts += 1;
    p.waiting = None;
    tracing::info!(
        tenant = %cx.tenant, index = %row.name, shard = %t.shard, method = method.label(),
        "building an automatic index"
    );
    let outcome = match &method {
        BuildMethod::Online => online::build(cx.view, t, &def, s).await,
        BuildMethod::GaleraRsu => galera::build_rsu(cx.view, t, &def, s, cx.marker).await,
        BuildMethod::GaleraToi => galera::build_toi(cx.view, t, &def, s).await,
        BuildMethod::CopySwap => copy_swap(cx, t, &def).await,
        BuildMethod::Recommend(_) => unreachable_recommend(),
    };
    drop(slot);

    let pending_state = |o: &BuildOutcome| match o {
        BuildOutcome::Built(_) => ShardBuild::Built,
        _ => ShardBuild::Pending,
    };
    store
        .set_shard(q, row.index_id, &t.shard, pending_state(&outcome))
        .await?;
    match outcome {
        BuildOutcome::Built(report) => {
            metrics::histogram!("sprig_advisor_autoindex_build_seconds", "method" => method.label())
                .record(Duration::from_millis(report.build_ms).as_secs_f64());
            p.builds.push(report.clone());
            p.last_error = None;
            let more = cx.shards.iter().any(|x| {
                x.shard != t.shard && row.shards.get(&x.shard) != Some(&ShardBuild::Built)
            });
            if more {
                p.not_before_ms = unix_ms(cx.now + s.rollout_pause);
                save(store, q, row, &p).await?;
                return Ok(Step::ShardBuilt {
                    report,
                    next_after: s.rollout_pause,
                });
            }
            finish(store, q, cx, row, p, Some(report)).await
        }
        BuildOutcome::Aborted { reason, ref detail } => {
            let after = match reason {
                AbortReason::UserDdl => s.user_ddl_cooldown,
                _ => s.retry_after,
            };
            p.last_error = Some(format!("{}: {detail}", reason.as_str()));
            p.waiting = Some(format!("aborted ({})", reason.as_str()));
            p.not_before_ms = unix_ms(cx.now + after);
            save(store, q, row, &p).await?;
            if reason == AbortReason::UserDdl {
                log(
                    cx,
                    row,
                    "index.defer",
                    "deferred",
                    "user_ddl",
                    format!(
                        "user DDL on {}.{} won; the build of {} is retried after {} s",
                        t.schema,
                        t.table,
                        row.name,
                        after.as_secs()
                    ),
                )
                .await;
            }
            Ok(Step::Retry {
                outcome,
                retry_after: after,
            })
        }
        BuildOutcome::LockTimeout { tries } => {
            let after = next_window(s.build_window, cx.auto_window, cx.now);
            p.lock_timeouts += 1;
            p.last_error = Some(format!("lock wait timeout after {tries} tries"));
            p.waiting = Some("lock waits; the next window".into());
            p.not_before_ms = unix_ms(cx.now + after);
            save(store, q, row, &p).await?;
            Ok(Step::Retry {
                outcome,
                retry_after: after,
            })
        }
        BuildOutcome::OnlineLogTooBig | BuildOutcome::Failed { .. } => {
            let error = if let BuildOutcome::Failed { error } = &outcome {
                error.clone()
            } else {
                p.online_log_failures += 1;
                "innodb_online_alter_log_max_size exceeded".to_owned()
            };
            p.failures += 1;
            p.last_error = Some(error.clone());
            if p.failures >= s.max_failures {
                return fail(store, q, cx, row, p, &error).await;
            }
            let after = if matches!(outcome, BuildOutcome::OnlineLogTooBig) {
                next_window(s.build_window, cx.auto_window, cx.now)
            } else {
                s.retry_after
            };
            p.waiting = Some(format!("failed {} time(s)", p.failures));
            p.not_before_ms = unix_ms(cx.now + after);
            save(store, q, row, &p).await?;
            Ok(Step::Retry {
                outcome,
                retry_after: after,
            })
        }
    }
}

fn unreachable_recommend() -> BuildOutcome {
    BuildOutcome::Failed {
        error: "no build method".into(),
    }
}

/// Sprig builds running on the cluster's writer (any controller's).
/// The DDL job holding `object` (its `objects.pending_job_id`), if any: builds wait until the schema change has published.
async fn schema_change_on<Q: Queryable + Send>(
    store: &AutoIndexStore,
    q: &mut Q,
    object: sprig_common::ids::ObjectId,
) -> std::result::Result<Option<String>, String> {
    let sql = format!(
        "SELECT LOWER(HEX(pending_job_id)) FROM {}.objects WHERE object_id = ?",
        sprig_common::names::quote(store.schema())
    );
    match q.exec_first::<Option<String>, _, _>(sql, (object.0,)).await {
        Ok(r) => Ok(r.flatten()),
        // A catalog without `objects` (a test catalog of this module alone)
        // has no DDL jobs.
        Err(mysql_async::Error::Server(e)) if e.code == 1146 => Ok(None),
        Err(e) => Err(e.to_string()),
    }
}

async fn running_builds(view: &ClusterView) -> std::result::Result<u32, String> {
    let Some(w) = view.nodes.writer() else {
        return Err("no writer".into());
    };
    let mut n = Node::connect(w, Duration::from_secs(10))
        .await
        .map_err(|e| e.to_string())?;
    let th = n.threads().await.map_err(|e| e.to_string());
    n.close().await;
    Ok(u32::try_from(
        th?.iter()
            .filter(|t| t.info.as_deref().is_some_and(is_sprig_build))
            .count(),
    )
    .unwrap_or(u32::MAX))
}

async fn copy_swap(
    cx: &RolloutCx<'_>,
    t: &ShardTable,
    def: &crate::autoindex::model::IndexDef,
) -> BuildOutcome {
    let Some(m) = cx.mover else {
        return BuildOutcome::Failed {
            error: "copy-and-swap without a mover".into(),
        };
    };
    let req = CopySwapRequest {
        tenant: cx.tenant,
        cluster: cx.view.cluster.clone(),
        table: t.clone(),
        // The shadow is offline: a field column and its index together.
        ddl: match crate::autoindex::kv::add_fields_clause(def) {
            Some(cols) => format!("{cols}, {}", add_index_clause(def)),
            None => add_index_clause(def),
        },
    };
    let started = std::time::Instant::now();
    match m.copy_swap(&req).await {
        Ok(CopySwapStatus::Done) => BuildOutcome::Built(BuildReport {
            shard: t.shard.clone(),
            method: "copy_swap".into(),
            already_present: false,
            build_ms: ms(started.elapsed()),
            lock_retries: 0,
            replicas_ms: None,
            max_replica_lag_s: None,
            nodes: Vec::new(),
        }),
        Ok(CopySwapStatus::Running) => BuildOutcome::Aborted {
            reason: AbortReason::Moving,
            detail: "copy-and-swap running".into(),
        },
        Ok(CopySwapStatus::Failed(e)) => BuildOutcome::Failed { error: e },
        Err(e) => BuildOutcome::Aborted {
            reason: AbortReason::Degraded,
            detail: format!("mover: {e}"),
        },
    }
}

async fn save<Q: Queryable + Send>(
    store: &AutoIndexStore,
    q: &mut Q,
    row: &AutoIndexRow,
    p: &Progress,
) -> Result<()> {
    store
        .set_observed(q, row.index_id, &p.into_observed(row.observed.as_ref()))
        .await?;
    Ok(())
}

async fn finish<Q: Queryable + Send>(
    store: &AutoIndexStore,
    q: &mut Q,
    cx: &RolloutCx<'_>,
    row: &AutoIndexRow,
    mut p: Progress,
    report: Option<BuildReport>,
) -> Result<Step> {
    p.built_at_ms = Some(unix_ms(cx.now));
    p.not_before_ms = 0;
    p.waiting = None;
    save(store, q, row, &p).await?;
    let moved = store
        .set_state(
            q,
            row.index_id,
            &StateChange {
                from: IndexState::RollingOut,
                to: IndexState::Observing,
                at: cx.now,
                drop_reason: None,
            },
        )
        .await?;
    if !moved {
        return Ok(Step::NotRollingOut);
    }
    log(
        cx,
        row,
        "index.create",
        "succeeded",
        "built",
        format!(
            "{} built on {} shard(s); observed for the watch period",
            row.name,
            cx.shards.len()
        ),
    )
    .await;
    Ok(Step::Observing { report })
}

async fn fail<Q: Queryable + Send>(
    store: &AutoIndexStore,
    q: &mut Q,
    cx: &RolloutCx<'_>,
    row: &AutoIndexRow,
    mut p: Progress,
    error: &str,
) -> Result<Step> {
    p.last_error = Some(error.to_owned());
    p.waiting = None;
    save(store, q, row, &p).await?;
    store
        .set_state(
            q,
            row.index_id,
            &StateChange {
                from: IndexState::RollingOut,
                to: IndexState::Failed,
                at: cx.now,
                drop_reason: None,
            },
        )
        .await?;
    log(
        cx,
        row,
        "index.create",
        "failed",
        "rollout_failed",
        format!("{} failed to build: {error}", row.name),
    )
    .await;
    Ok(Step::Failed {
        reason: error.to_owned(),
    })
}

async fn log(
    cx: &RolloutCx<'_>,
    row: &AutoIndexRow,
    kind: &'static str,
    outcome: &'static str,
    reason: &str,
    summary: String,
) {
    let d = IndexDecision {
        tenant: cx.tenant,
        kind,
        subject_type: "index",
        subject_id: format!("{}/{}", row.object.0, row.name),
        mode: "applied",
        outcome,
        reason_code: reason.to_owned(),
        summary,
        evidence: serde_json::json!({
            "cluster": cx.view.cluster.to_string(),
            "shards": cx.shards.len(),
            "table_bytes": cx.table_bytes,
        }),
        alternatives: None,
        at: cx.now,
    };
    if let Err(e) = cx.decisions.record(&d).await {
        tracing::warn!(error = %e, "decision not recorded");
    }
}
