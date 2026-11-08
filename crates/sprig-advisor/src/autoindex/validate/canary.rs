//! Estimate-and-canary validation (chapter 13 build step 9), for single
//! nodes and external clusters with no usable replica, and whenever
//! parameter samples are off.
//!
//! ```text
//! 1. every target fingerprint's predicted improvement (cost model) must be
//!    at least min_improvement_pct × estimate_margin (45% by default)
//! 2. EXPLAIN FORMAT=JSON each target's sampled statements on the node: the
//!    plan must read at least explain_rows_tolerance × the rows the model
//!    assumed (else the benefit is overstated)
//! 3. canary: build the index for real on the canary shard (R0: the
//!    tenant's only shard), online, with the binary log on
//! 4. replay the samples A/B on the canary right away (IGNORE INDEX gives
//!    the before plan); a rejection drops the index at once
//! 5. otherwise the index stays, observing for auto_index.canary_hours;
//!    the observer then calls `judge` with the target fingerprints' p95
//!    before and since (and, from R1, the unbuilt shards as a control)
//! ```
//!
//! Step 4 is this module's addition *(choice)*: the chapter's canary only
//! observes, which takes a day and lets a harmful index serve traffic in
//! the meantime. Replaying right after the build catches the plain cases
//! (an index the optimizer ignores, a statement it makes slower) in
//! seconds. It runs on the canary shard's writer, read-only and bounded by
//! `max_statement_time`, and its A side is the statement the workload
//! already runs there. `canary_replay = false` turns it off.

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sprig_cluster::sql::{NodeEndpoint, ProbeError};
use sprig_common::Result;
use sprig_common::ids::Fingerprint;

use super::node::{NodeSql, NodeWatch, add_index_sql, drop_index_sql, run_watched};
use super::{
    CanaryState, Method, ValidationPlan, ValidationReport, Validator, abort, ms_since, now_ms,
    replay_on, report,
};
use crate::autoindex::replay::{
    self, NoGuard, ReplayConn, StatementRole, Thresholds, VerdictKind, parse_plan, select_form,
    verdict::median,
};

/// Validates by estimates and a canary build.
pub struct CanaryValidator {
    node: NodeEndpoint,
    online_build: bool,
    probe_timeout: Duration,
}

impl std::fmt::Debug for CanaryValidator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CanaryValidator")
            .field("node", &self.node)
            .field("online_build", &self.online_build)
            .finish_non_exhaustive()
    }
}

impl CanaryValidator {
    /// A validator building the canary on `node`, the canary shard's
    /// writer. `online_build` is the profile's `derived.online_index_build`;
    /// without it the canary build is left to the rollout's copy-and-swap.
    #[must_use]
    pub fn new(node: NodeEndpoint, online_build: bool) -> Self {
        Self {
            node,
            online_build,
            probe_timeout: Duration::from_secs(10),
        }
    }

    /// Drops a canary that [`judge`] rejected (binary log on: the drop is
    /// the real one).
    ///
    /// # Errors
    /// The drop failed.
    pub async fn drop_canary(&self, plan: &ValidationPlan) -> Result<()> {
        let mut s = NodeSql::connect(&self.node, plan.settings.max_build).await?;
        s.exec(&format!(
            "SET SESSION lock_wait_timeout = {}",
            plan.settings.ddl_lock_wait.as_secs().max(1)
        ))
        .await?;
        let res = super::replica::drop_with_retries(
            &mut s,
            &drop_index_sql(&plan.target, &plan.index_name()),
        )
        .await;
        s.s.close().await;
        Ok(res?)
    }
}

/// The node must stay reachable while the canary builds.
struct Reachable {
    mon: NodeSql,
}

#[async_trait::async_trait]
impl NodeWatch for Reachable {
    async fn check(&mut self) -> std::result::Result<(), String> {
        self.mon
            .scalar("SELECT 1")
            .await
            .map(|_| ())
            .map_err(|e| format!("node unreachable: {e}"))
    }
    async fn kill_query(&mut self, id: u32) -> std::result::Result<(), ProbeError> {
        self.mon.exec(&format!("KILL QUERY {id}")).await
    }
}

/// Step 1: the cost model's predictions against the stricter bar.
pub(crate) fn check_predictions(plan: &ValidationPlan) -> std::result::Result<(), String> {
    let t = &plan.settings.replay.thresholds;
    let bar = t.min_improvement_pct * plan.settings.estimate_margin;
    for fp in plan.candidate.primaries() {
        let Some(p) = plan.predictions.iter().find(|p| p.fingerprint == fp) else {
            return Err(format!("no_prediction: {fp}"));
        };
        let got = p.improvement_pct();
        if got < bar {
            return Err(format!(
                "prediction_below_margin: {fp} predicts {got:.1}% < {bar:.0}%"
            ));
        }
    }
    Ok(())
}

/// Step 2: `EXPLAIN` each target's samples; the median row estimate per
/// fingerprint must reach `tolerance ×` the rows the model assumed.
async fn check_explain(
    conn: &mut dyn ReplayConn,
    plan: &ValidationPlan,
    notes: &mut Vec<String>,
) -> std::result::Result<(), String> {
    let s = &plan.settings;
    let mut cur_db: Option<String> = None;
    for fp in plan.candidate.primaries() {
        let Some(pred) = plan.predictions.iter().find(|p| p.fingerprint == fp) else {
            continue;
        };
        let mut rows = Vec::new();
        for st in plan
            .statements
            .iter()
            .filter(|st| st.fingerprint == fp && st.role == StatementRole::Target)
            .take(s.replay.samples_per_fingerprint)
        {
            let Ok(form) = select_form(&st.sql, &plan.target.pk) else {
                continue;
            };
            if st.context.database.is_some() && st.context.database != cur_db {
                let db = st.context.database.as_deref().unwrap_or_default();
                if conn
                    .exec(&format!("USE {}", sprig_common::names::quote(db)))
                    .await
                    .is_err()
                {
                    continue;
                }
                cur_db.clone_from(&st.context.database);
            }
            if let Ok(Some(j)) = conn
                .first_value(&format!("EXPLAIN FORMAT=JSON {}", form.sql))
                .await
                && let Ok(p) = parse_plan(&j)
            {
                rows.push(p.rows_estimate());
            }
        }
        if rows.is_empty() {
            notes.push(format!("{fp}: no sample to EXPLAIN; estimates only"));
            continue;
        }
        let m = median(&rows);
        let need = s.explain_rows_tolerance * pred.rows_assumed;
        notes.push(format!(
            "{fp}: EXPLAIN reads {m:.0} rows, the model assumed {:.0}",
            pred.rows_assumed
        ));
        if m < need {
            return Err(format!(
                "explain_disagrees: {fp} plan reads {m:.0} rows, model assumed {:.0}",
                pred.rows_assumed
            ));
        }
    }
    Ok(())
}

#[async_trait::async_trait]
impl Validator for CanaryValidator {
    #[allow(clippy::too_many_lines)]
    async fn validate(&self, plan: &ValidationPlan) -> Result<ValidationReport> {
        plan.check()?;
        let set = &plan.settings;
        let mut r = report(plan, Method::EstimateCanary, &self.node.node);
        let name = plan.index_name();
        let t = &plan.target;
        let finish = |mut r: ValidationReport| {
            r.finished_at_ms = now_ms();
            tracing::info!(
                candidate = r.candidate_id,
                node = %r.node,
                verdict = r.verdict.kind.as_str(),
                reason = %r.verdict.reason,
                "estimate-and-canary validation finished"
            );
            Ok(r)
        };

        // 1. Predictions.
        if let Err(why) = check_predictions(plan) {
            r.verdict.kind = VerdictKind::Rejected;
            r.verdict.reason = why;
            return finish(r);
        }

        // 2. EXPLAIN, on a read-only session.
        let mut conn = match sprig_cluster::ProbeSession::connect(
            &self.node,
            set.replay.timeout + Duration::from_secs(30),
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                abort(&mut r, format!("node unreachable: {e}"));
                return finish(r);
            }
        };
        let mut notes = Vec::new();
        let explained = match replay::prepare_session(&mut conn, &set.replay).await {
            Ok(()) => check_explain(&mut conn, plan, &mut notes).await,
            Err(e) => Err(format!("session: {e}")),
        };
        conn.close().await;
        for n in notes {
            r.node_check.ok(n);
        }
        if let Err(why) = explained {
            if why.starts_with("session") {
                abort(&mut r, why);
            } else {
                r.verdict.kind = VerdictKind::Rejected;
                r.verdict.reason = why;
            }
            return finish(r);
        }
        if !self.online_build {
            r.verdict.kind = VerdictKind::Inconclusive;
            r.verdict.reason =
                "canary_needs_rollout: no online build here; the rollout builds the canary".into();
            return finish(r);
        }

        // 3. The canary build, for real.
        let mut mon = match NodeSql::connect(&self.node, self.probe_timeout).await {
            Ok(m) => m,
            Err(e) => {
                abort(&mut r, format!("node unreachable: {e}"));
                return finish(r);
            }
        };
        match mon.index_columns(t, &name).await {
            Ok(c) if c.is_empty() => {}
            Ok(_) => {
                abort(&mut r, format!("index_exists: {name} is already built"));
                mon.s.close().await;
                return finish(r);
            }
            Err(e) => {
                abort(&mut r, format!("node unreachable: {e}"));
                mon.s.close().await;
                return finish(r);
            }
        }
        let mut watch = Reachable { mon };
        let mut ddl =
            match NodeSql::connect(&self.node, set.max_build + Duration::from_secs(60)).await {
                Ok(d) => d,
                Err(e) => {
                    abort(&mut r, format!("DDL session: {e}"));
                    watch.mon.s.close().await;
                    return finish(r);
                }
            };
        let lock = ddl
            .exec(&format!(
                "SET SESSION lock_wait_timeout = {}",
                set.ddl_lock_wait.as_secs().max(1)
            ))
            .await;
        let started = Instant::now();
        let (res, killed) = match lock {
            Ok(()) => {
                let sql = add_index_sql(t, &name, &plan.candidate.def.ddl_columns(), true);
                run_watched(&mut ddl, &mut watch, &sql, set.poll).await
            }
            Err(e) => (Err(e), None),
        };
        r.build_ms = Some(ms_since(started));
        let built = match (res, killed) {
            (_, Some(why)) => {
                abort(&mut r, format!("build stopped: {why}"));
                false
            }
            (Err(e), None) if e.errno == Some(1205) => {
                abort(&mut r, "build: lock wait timeout");
                false
            }
            (Err(e), None) => {
                abort(&mut r, format!("build failed: {e}"));
                false
            }
            (Ok(()), None) => true,
        };

        // 4. Replay on the canary.
        let has_samples = plan
            .statements
            .iter()
            .any(|s| s.role == StatementRole::Target);
        if built && set.canary_replay && has_samples {
            replay_on(&self.node, plan, &mut NoGuard, &mut r).await;
        } else if built {
            r.verdict.kind = VerdictKind::Accepted;
            r.verdict.reason = "estimates passed; canary observing".into();
        }

        // 5. Keep and observe, or drop.
        let keep = built
            && matches!(
                r.verdict.kind,
                VerdictKind::Accepted | VerdictKind::Inconclusive
            );
        if keep {
            r.index_kept = true;
            let until = now_ms() + u64::from(set.canary_hours) * 3_600_000;
            r.canary = Some(CanaryState {
                observe_until_ms: until,
            });
            r.node_check.ok(format!(
                "{name} built as the canary; observing {} h",
                set.canary_hours
            ));
        } else {
            let started = Instant::now();
            match super::replica::drop_with_retries(&mut ddl, &drop_index_sql(t, &name)).await {
                Ok(()) => r.node_check.ok(format!("{name} dropped")),
                Err(e) => r.node_check.bad(format!("drop of {name} failed: {e}")),
            }
            r.drop_ms = Some(ms_since(started));
            match watch.mon.index_columns(t, &name).await {
                Ok(c) if c.is_empty() => r.node_check.ok(format!("{name} absent")),
                Ok(_) => r.node_check.bad(format!("{name} still on the node")),
                Err(e) => r.node_check.bad(format!("index not re-checked: {e}")),
            }
        }
        ddl.s.close().await;
        watch.mon.s.close().await;
        finish(r)
    }
}

/// One fingerprint over the canary period, from the workload store.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct CanaryObservation {
    /// The fingerprint.
    pub fingerprint: Fingerprint,
    /// A target of the candidate, or another fingerprint on the table.
    pub role: StatementRole,
    /// p95 before the build (the baseline), ms.
    pub baseline_p95_ms: f64,
    /// p95 on the canary since the build, ms.
    pub observed_p95_ms: f64,
    /// Runs on the canary since the build.
    pub runs: u64,
    /// R1: the same fingerprint's p95 on unbuilt shards, before and since,
    /// to take out drift common to all shards.
    pub control: Option<(f64, f64)>,
}

/// The verdict on a canary after `auto_index.canary_hours`: every target's
/// p95 improved by `min_improvement_pct` against its baseline (scaled by
/// the control group's drift when there is one), and no other fingerprint
/// got slower than `1 + max_regression_pct` times plus the noise floor.
/// Fewer than `min_runs` runs of a target: inconclusive.
#[must_use]
pub fn judge(obs: &[CanaryObservation], t: &Thresholds, min_runs: u64) -> (VerdictKind, String) {
    let expected = |o: &CanaryObservation| match o.control {
        Some((cb, co)) if cb > 0.0 && co > 0.0 => o.baseline_p95_ms * co / cb,
        _ => o.baseline_p95_ms,
    };
    for o in obs {
        let e = expected(o);
        if o.role == StatementRole::Regression
            && o.observed_p95_ms > (1.0 + t.max_regression_pct / 100.0) * e + t.noise_floor_ms
        {
            return (
                VerdictKind::Rejected,
                format!(
                    "regression: {} p95 {e:.1} -> {:.1} ms on the canary",
                    o.fingerprint, o.observed_p95_ms
                ),
            );
        }
    }
    let targets: Vec<&CanaryObservation> = obs
        .iter()
        .filter(|o| o.role == StatementRole::Target)
        .collect();
    if targets.is_empty() {
        return (VerdictKind::Inconclusive, "no target observed".into());
    }
    for o in &targets {
        if o.runs < min_runs {
            return (
                VerdictKind::Inconclusive,
                format!(
                    "{}: {} runs on the canary, need {min_runs}",
                    o.fingerprint, o.runs
                ),
            );
        }
        let e = expected(o);
        let impr = if e > 0.0 {
            100.0 * (1.0 - o.observed_p95_ms / e)
        } else {
            0.0
        };
        if impr < t.min_improvement_pct {
            return (
                VerdictKind::Rejected,
                format!(
                    "no_gain: {} p95 {e:.1} -> {:.1} ms ({impr:.1}%)",
                    o.fingerprint, o.observed_p95_ms
                ),
            );
        }
    }
    (VerdictKind::Accepted, "canary improved every target".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn o(fp: u64, role: StatementRole, b: f64, a: f64) -> CanaryObservation {
        CanaryObservation {
            fingerprint: Fingerprint(fp),
            role,
            baseline_p95_ms: b,
            observed_p95_ms: a,
            runs: 100,
            control: None,
        }
    }

    #[test]
    fn canary_judgement() {
        use StatementRole::{Regression, Target};
        let t = Thresholds::default();
        assert_eq!(
            judge(
                &[o(1, Target, 400.0, 12.0), o(2, Regression, 50.0, 52.0)],
                &t,
                50
            )
            .0,
            VerdictKind::Accepted
        );
        let (k, why) = judge(&[o(1, Target, 400.0, 300.0)], &t, 50);
        assert_eq!(k, VerdictKind::Rejected);
        assert!(why.starts_with("no_gain"), "{why}");
        let (k, why) = judge(
            &[o(1, Target, 400.0, 12.0), o(2, Regression, 50.0, 70.0)],
            &t,
            50,
        );
        assert_eq!(k, VerdictKind::Rejected);
        assert!(why.starts_with("regression"), "{why}");
        let mut few = o(1, Target, 400.0, 12.0);
        few.runs = 3;
        assert_eq!(judge(&[few], &t, 50).0, VerdictKind::Inconclusive);
        assert_eq!(judge(&[], &t, 50).0, VerdictKind::Inconclusive);
        // A control group that got 2x slower too: the target's 400 → 500
        // is really a 37.5% gain against an expected 800.
        let mut c = o(1, Target, 400.0, 500.0);
        c.control = Some((100.0, 200.0));
        assert_eq!(judge(&[c], &t, 50).0, VerdictKind::Accepted);
    }
}
