//! The `auto_index` loop's validation step (chapter 13 build steps 7-9):
//! [`SampleValidation`] turns a `validating` row into a validation plan
//! and runs the cluster's validator.
//!
//! ```text
//! validate(row):
//!   observed.pre_validated (a covering retry accepted)   → accepted
//!   candidate  := its auto_index_candidates row (definition, serves, evidence)
//!   inputs     := observed.plan (predictions, columns read, regression
//!                 fingerprints, the table in physical names), written by
//!                 the evaluator
//!   statements := up to replay_samples samples of each target and regression
//!                 fingerprint, opened with the workload keys, bound
//!                 (SampleValue::Raw) and rewritten to physical names
//!   method     := method_for(derived.validation, any target sample)
//!                 replica:          the first replica
//!                 galera_rsu:       the first node that isn't the writer
//!                 estimate_canary:  the writer (R0: the tenant's only shard)
//!   reports    := validate_with_covering_retry
//!   each report → an auto_index_validations row
//!   accepted                       → Accepted (the loop rolls it out)
//!   rejected, covering retry accepted → the variant gets its candidate row,
//!                 its validation row and an auto_indexes row in `validating`
//!                 marked pre_validated; this row → Rejected
//!   rejected                       → Rejected
//!   inconclusive, aborted          → Pending (asked again next pass), except
//!                                    a canary kept for observation → Accepted
//! ```
//!
//! **Node marking (R0).** Chapter 06's read routing has no "validating"
//! mark yet and a Galera writer weight is set by hand, so the controller's
//! [`LogMarker`] only logs. That is safe in R0: a replica or Galera node
//! under validation carries an extra index only on that node, with the
//! binary log off, and gateways send R0 sessions to the cluster's writer,
//! which is never chosen as the validation node.
//!
//! **Canary.** On a single node the canary is the tenant's only shard, so a
//! kept canary *is* the rollout: the row is accepted (the validation row
//! records `accepted`, its reason says the canary is kept) and the observer
//! watches it for `observe_days`, which covers chapter 13's
//! `canary_hours` observation in R0.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sprig_advisor::autoindex::evaluate::{CatalogStore, EvalStore, PlanInputs};
use sprig_advisor::autoindex::model::{AutoIndexId, Candidate, Role};
use sprig_advisor::autoindex::replay::{StatementRole, VerdictKind};
use sprig_advisor::autoindex::rollout::Nodes;
use sprig_advisor::autoindex::store::{
    AutoIndexRow, AutoIndexStore, CandidateRow, CandidateStatus, Definition, IndexState,
    StoredKind, ValidationMethod, ValidationRow, Verdict,
};
use sprig_advisor::autoindex::validate::canary::CanaryValidator;
use sprig_advisor::autoindex::validate::galera::RsuValidator;
use sprig_advisor::autoindex::validate::replica::ReplicaValidator;
use sprig_advisor::autoindex::validate::{
    Method, NodeMarker, ValidationPlan, ValidationReport, Validator, covering_variant, method_for,
    validate_with_covering_retry,
};
use sprig_catalog::CatalogLocator;
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::ids::{Fingerprint, NodeId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_workload::keyring::TenantKeyring;

use crate::autoindex_loop::{ObjectEnv, Validated, ValidationHook};
use crate::autoindex_workload::{name_map, statements, tenant_catalog};

/// A [`NodeMarker`] that only logs (R0; module docs).
#[derive(Debug, Clone, Copy, Default)]
pub struct LogMarker;

#[async_trait::async_trait]
impl NodeMarker for LogMarker {
    async fn mark_validating(&self, node: &NodeId) -> Result<()> {
        tracing::info!(%node, "auto_index: node validating (R0: not taken out of routing)");
        Ok(())
    }
    async fn unmark(&self, node: &NodeId) -> Result<()> {
        tracing::info!(%node, "auto_index: node validated");
        Ok(())
    }
}

/// The loop's [`ValidationHook`] (module docs).
pub struct SampleValidation {
    locator: Arc<dyn CatalogLocator>,
    registry: ClusterRegistry,
    keyring: Option<Arc<dyn TenantKeyring>>,
    marker: Arc<dyn NodeMarker>,
}

impl std::fmt::Debug for SampleValidation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SampleValidation")
            .field("keyring", &self.keyring.is_some())
            .finish_non_exhaustive()
    }
}

impl SampleValidation {
    /// Validation reading catalogs through `locator`, cluster profiles from
    /// `registry`, opening samples with `keyring` (none: estimates and a
    /// canary only).
    #[must_use]
    pub fn new(
        locator: Arc<dyn CatalogLocator>,
        registry: ClusterRegistry,
        keyring: Option<Arc<dyn TenantKeyring>>,
        marker: Arc<dyn NodeMarker>,
    ) -> Self {
        Self {
            locator,
            registry,
            keyring,
            marker,
        }
    }
}

fn ms_time(ms: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms)
}

/// The shard number of a shard schema, 0 when it isn't one.
fn shard_of(schema: &str) -> u64 {
    match schema.parse::<SchemaName>() {
        Ok(SchemaName::Shard { shard, .. }) => u64::from(shard.0),
        _ => 0,
    }
}

const fn method_col(m: Method) -> ValidationMethod {
    match m {
        Method::Replica => ValidationMethod::Replica,
        Method::GaleraRsu => ValidationMethod::GaleraRsu,
        Method::EstimateCanary => ValidationMethod::EstimateCanary,
    }
}

const fn verdict_col(k: VerdictKind) -> Verdict {
    match k {
        VerdictKind::Accepted => Verdict::Accepted,
        VerdictKind::Rejected => Verdict::Rejected,
        VerdictKind::Inconclusive => Verdict::Inconclusive,
        VerdictKind::Aborted => Verdict::Aborted,
    }
}

/// A kept canary counts as accepted (module docs).
fn kept_canary(r: &ValidationReport) -> bool {
    r.method == Method::EstimateCanary && r.index_kept && r.canary.is_some()
}

/// The `auto_index_validations` row of report `r`.
fn validation_row(
    r: &ValidationReport,
    candidate_id: u64,
    cluster: &str,
    shard_id: u64,
) -> ValidationRow {
    let accepted = r.verdict.kind == VerdictKind::Accepted || kept_canary(r);
    let reason = if kept_canary(r) && r.verdict.kind != VerdictKind::Accepted {
        format!("canary kept for observation; {}", r.verdict.reason)
    } else {
        r.verdict.reason.clone()
    };
    #[allow(clippy::cast_possible_truncation)]
    ValidationRow {
        validation_id: 0,
        candidate_id,
        method: method_col(r.method),
        cluster_id: cluster.to_owned(),
        node: r.node.clone(),
        shard_id,
        started_at: ms_time(r.started_at_ms),
        finished_at: Some(ms_time(r.finished_at_ms.max(r.started_at_ms))),
        build_seconds: r
            .build_seconds()
            .map(|s| u32::try_from(s).unwrap_or(u32::MAX)),
        statements: r.verdict.statements as u64,
        noisy: r.verdict.noisy as u64,
        target_p95_before: r.verdict.target_p95_before,
        target_p95_after: r.verdict.target_p95_after,
        worst_regression: Some(r.verdict.worst_regression_pct),
        worst_fp: r.verdict.worst_fp,
        recall_at_k: None,
        verdict: if accepted {
            Verdict::Accepted
        } else {
            verdict_col(r.verdict.kind)
        },
        reason: Some(reason).filter(|s| !s.is_empty()),
        details: r.details_json(),
    }
}

/// The validator for `method` on `o`'s cluster.
fn validator(
    method: Method,
    o: &ObjectEnv,
    online_build: bool,
    marker: &Arc<dyn NodeMarker>,
) -> Result<Box<dyn Validator>> {
    let no_node = || SprigError::new(SprigCode::ClusterUnavailable, "no node to validate on");
    Ok(match (method, &o.view.nodes) {
        (Method::Replica, Nodes::PrimaryReplica { writer, replicas }) => Box::new(
            ReplicaValidator::new(
                replicas.first().ok_or_else(no_node)?.clone(),
                Arc::clone(marker),
            )
            // bugfix-61: a cut-off validation's leftover is dropped first.
            .with_writer(writer.clone()),
        ),
        (Method::GaleraRsu, Nodes::Galera { nodes, writer }) => {
            let node = nodes
                .iter()
                .find(|n| &n.node != writer)
                .ok_or_else(no_node)?;
            let peers = nodes
                .iter()
                .filter(|n| n.node != node.node)
                .cloned()
                .collect();
            let mut v = RsuValidator::new(node.clone(), peers, Arc::clone(marker));
            // bugfix-61: a cut-off validation's leftover is dropped first.
            if let Some(w) = nodes.iter().find(|n| &n.node == writer) {
                v = v.with_writer(w.clone());
            }
            Box::new(v)
        }
        _ => Box::new(CanaryValidator::new(
            o.view.nodes.writer().ok_or_else(no_node)?.clone(),
            online_build,
        )),
    })
}

#[async_trait::async_trait]
impl ValidationHook for SampleValidation {
    #[allow(clippy::too_many_lines)] // the flow in the module docs, in one place
    async fn validate(
        &self,
        tenant: TenantId,
        row: &AutoIndexRow,
        object: &ObjectEnv,
    ) -> Result<Validated> {
        // A covering variant accepted by its parent's retry.
        if let Some(id) = row
            .observed
            .as_ref()
            .and_then(|o| o.get("pre_validated"))
            .and_then(serde_json::Value::as_u64)
        {
            return Ok(Validated::Accepted {
                validation_id: Some(id),
            });
        }
        // Advice is never built: the loop stops it at `recommended`.
        let Some(def) = row.def() else {
            return Ok(Validated::Accepted {
                validation_id: None,
            });
        };
        let Some(inputs) = PlanInputs::of(row.observed.as_ref()) else {
            return Ok(Validated::Rejected {
                reason: "no_plan: the row predates validation plans".into(),
            });
        };
        let Some(target) = inputs.target.clone() else {
            return Ok(Validated::Rejected {
                reason: "no_plan: no table".into(),
            });
        };
        let ep = self.locator.writer(tenant).await?;
        let store = AutoIndexStore::of(&ep.store);
        // One short catalog connection at a time: the catalog snapshot and
        // the samples below take their own, and the validation itself can
        // run for minutes (the R0 catalog-connection deadlock).
        let cand_row = {
            let mut q = crate::catalogs::acquire(&ep.pool, "auto_index validation").await?;
            store.candidate(&mut q, row.candidate_id).await?
        };
        let Some(cand_row) = cand_row else {
            return Ok(Validated::Rejected {
                reason: format!("no candidate {}", row.candidate_id),
            });
        };
        let candidate = Candidate {
            def,
            covering: cand_row.covering,
            serves: cand_row.fingerprints.clone(),
            evidence: cand_row.evidence.clone(),
        };
        let settings = object.validation;

        // Samples: targets (primary), then merged and regression
        // fingerprints, each once.
        let mut fps: Vec<(Fingerprint, StatementRole)> = Vec::new();
        for (f, r) in &candidate.serves {
            let role = if *r == Role::Primary {
                StatementRole::Target
            } else {
                StatementRole::Regression
            };
            if !fps.iter().any(|(g, _)| g == f) {
                fps.push((*f, role));
            }
        }
        for f in inputs.regression() {
            if !fps.iter().any(|(g, _)| *g == f) {
                fps.push((f, StatementRole::Regression));
            }
        }
        let stmts = match &self.keyring {
            Some(k) => {
                let cat = tenant_catalog(&*self.locator, tenant).await?;
                let map = name_map(&cat);
                statements(
                    &*self.locator,
                    &**k,
                    &map,
                    tenant,
                    &fps,
                    settings.replay.samples_per_fingerprint,
                    Some(&candidate.def),
                )
                .await?
            }
            None => Vec::new(),
        };
        let samples = stmts.iter().any(|s| s.role == StatementRole::Target);
        let profile = self
            .registry
            .get(&object.view.cluster)
            .await?
            .and_then(|c| c.body().profile.clone());
        let derived = profile.as_ref().map(|p| p.derived.clone());
        let method = method_for(derived.as_ref().and_then(|d| d.validation), samples);
        let online = derived.as_ref().is_none_or(|d| d.online_index_build)
            || matches!(object.view.nodes, Nodes::Single { .. });
        let v = validator(method, object, online, &self.marker)?;
        let plan = ValidationPlan {
            candidate_id: row.candidate_id,
            candidate,
            target: target.clone(),
            statements: stmts,
            predictions: inputs.predictions.clone(),
            columns_read: inputs.columns_by_fp(),
            keep_if_accepted: false,
            settings,
        };
        tracing::info!(%tenant, index = %row.name, method = method.as_str(),
            statements = plan.statements.len(), "auto_index: validating");
        let reports = validate_with_covering_retry(&*v, &plan).await?;
        let cluster = object.view.cluster.to_string();
        let shard = shard_of(&target.schema);
        let first = &reports[0];
        let mut q = crate::catalogs::acquire(&ep.pool, "auto_index validation").await?;
        let vid = store
            .insert_validation(
                &mut q,
                &validation_row(first, row.candidate_id, &cluster, shard),
            )
            .await?;
        tracing::info!(%tenant, index = %row.name, verdict = first.verdict.kind.as_str(),
            reason = %first.verdict.reason, p95_before = ?first.verdict.target_p95_before,
            p95_after = ?first.verdict.target_p95_after,
            worst_regression_pct = first.verdict.worst_regression_pct,
            elapsed_ms = first.elapsed_ms(), "auto_index: validated");
        if first.verdict.kind == VerdictKind::Accepted || kept_canary(first) {
            store.set_validation(&mut q, row.index_id, vid).await?;
            drop(q);
            return Ok(Validated::Accepted {
                validation_id: Some(vid),
            });
        }
        if first.verdict.kind != VerdictKind::Rejected {
            drop(q);
            return Ok(Validated::Pending);
        }
        let mut reason = format!("validation: {}", first.verdict.reason);
        if let Some(retry) = reports.get(1) {
            // The variant validate_with_covering_retry built, again.
            let variant = first
                .verdict
                .covering_hint
                .as_ref()
                .and_then(|h| covering_variant(&plan, h.fingerprint))
                .filter(|c| c.def.name() == retry.index)
                .ok_or_else(|| SprigError::internal("a covering retry without its candidate"))?;
            let now = SystemTime::now();
            let crow = CandidateRow {
                candidate_id: 0,
                def_hash: variant.def.def_hash(),
                kind: StoredKind::from(variant.def.kind),
                definition: Definition::of(&variant.def),
                covering: true,
                status: if retry.verdict.kind == VerdictKind::Accepted {
                    CandidateStatus::Selected
                } else {
                    CandidateStatus::Rejected
                },
                reason: Some(format!("covering_retry:{}", row.name)),
                evidence: variant.evidence.clone(),
                created_at: now,
                ..cand_row.clone()
            };
            let cid = store.insert_candidate(&mut q, &crow).await?;
            let vid2 = store
                .insert_validation(&mut q, &validation_row(retry, cid, &cluster, shard))
                .await?;
            // `put_index` below takes its own connection.
            drop(q);
            tracing::info!(%tenant, index = %retry.index, verdict = retry.verdict.kind.as_str(),
                reason = %retry.verdict.reason, "auto_index: covering retry validated");
            if retry.verdict.kind == VerdictKind::Accepted {
                let mut observed = serde_json::json!({
                    "plan": inputs,
                    "pre_validated": vid2,
                    "covering_retry_of": row.name,
                });
                if let Some(r) = row.observed.as_ref().and_then(|o| o.get("route")) {
                    observed["route"] = r.clone();
                }
                let new_row = AutoIndexRow {
                    index_id: AutoIndexId(0),
                    name: variant.def.name(),
                    kind: StoredKind::from(variant.def.kind),
                    definition: Definition::of(&variant.def),
                    state: IndexState::Validating,
                    prior_state: None,
                    candidate_id: cid,
                    validation_id: Some(vid2),
                    decision_id: None,
                    shards: std::collections::BTreeMap::new(),
                    observed: Some(observed),
                    last_used_at: None,
                    drop_reason: None,
                    created_at: now,
                    state_changed_at: now,
                    ..row.clone()
                };
                let cs = CatalogStore::new(ep.pool.clone(), ep.store.schema());
                match cs.put_index(&new_row).await? {
                    Some(id) => {
                        reason = format!(
                            "{reason}; covering retry {} accepted (row {})",
                            new_row.name, id.0
                        );
                    }
                    None => {
                        reason = format!("{reason}; covering retry {} exists", new_row.name);
                    }
                }
            } else {
                reason = format!(
                    "{reason}; covering retry rejected: {}",
                    retry.verdict.reason
                );
            }
        } else {
            drop(q);
        }
        Ok(Validated::Rejected { reason })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_numbers_come_from_shard_schemas() {
        assert_eq!(shard_of("t7_d1_g1_s3"), 3);
        assert_eq!(shard_of("t7_sprig"), 0);
        assert_eq!(shard_of("nonsense"), 0);
    }
}
