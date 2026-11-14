//! Operator placement calls (manual moves and pins):
//! pinning and unpinning a tenant, and the decision rows of operator
//! moves and pins. Any controller answers them: pins are DC-tier records
//! (`sprig_balancer::pins`).
//!
//! ```text
//! POST   /admin/v1/tenants/{name}/pin   {"cluster": "dc-a-c2", "reason": "...", "by": "alice"}
//! DELETE /admin/v1/tenants/{name}/pin
//! ```
//!
//! A pin names the cluster the tenant is on, or the target of a mover job
//! already moving it there (`sprigctl shard move --to <c> --pin` submits
//! the move, then pins). An operator's move of a pinned tenant takes the
//! pin with it: the pin is re-pointed at the target when the job is
//! submitted, and the balancer's run puts it back where the tenant is if
//! the move rolls back (`sprig_balancer::pins` module docs). Operator
//! decisions of a move carry its job id.

use std::sync::Arc;
use std::time::UNIX_EPOCH;

use serde::{Deserialize, Serialize};
use serde_json::json;
use sprig_balancer::decisions::{CatalogDecisionLog, DecisionLog, Mode, NewDecision, Outcome};
use sprig_balancer::pins::{PinStore, TenantPin};
use sprig_common::Result;
use sprig_common::ids::{ClusterId, JobId, TenantId};
use sprig_mover::{JobFilter, JobStatus, JobStore, MetaMover, Mover};
use sprig_shard::{Placement, TenantMaps};

use super::{AdminService, invalid};
use crate::catalogs::{ClusterPools, RegistryPools, ShardMapLocator, default_naming};

/// `POST /admin/v1/tenants/{name}/pin`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinRequest {
    /// The cluster; the tenant's current cluster when absent.
    #[serde(default)]
    pub cluster: Option<String>,
    /// Why, for the decision log and `explain placement`.
    #[serde(default)]
    pub reason: Option<String>,
    /// The operator; `operator` when absent.
    #[serde(default)]
    pub by: Option<String>,
}

/// `DELETE /admin/v1/tenants/{name}/pin`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnpinResponse {
    /// The tenant.
    pub tenant: String,
    /// The pin it had, if any.
    pub was: Option<TenantPin>,
}

fn now_s(svc: &AdminService) -> u64 {
    svc.clock
        .wall()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

impl AdminService {
    fn pins(&self) -> PinStore {
        PinStore::new(self.stores.dc.clone())
    }

    /// The balancer's decision log (its pools); a log of its own only for
    /// a service without the balancer (tests), whose pools last one call.
    fn decision_log(&self) -> Arc<dyn DecisionLog> {
        if let Some(p) = &self.placement {
            return p.decisions();
        }
        let pools: Arc<dyn ClusterPools> = Arc::new(RegistryPools::new(
            self.monitor.registry().clone(),
            Some(Arc::clone(&self.monitor)),
        ));
        Arc::new(CatalogDecisionLog::new(Arc::new(ShardMapLocator::new(
            self.stores.dc.clone(),
            pools,
            default_naming(),
        ))))
    }

    /// Writes an operator decision; a failure is logged, never returned:
    /// the operator's action already happened.
    #[allow(clippy::too_many_arguments)]
    async fn operator_decision(
        &self,
        tenant: TenantId,
        kind: &str,
        reason_code: &str,
        summary: String,
        evidence: serde_json::Value,
        actor: &str,
        job: Option<JobId>,
    ) {
        let d = NewDecision {
            decided_at_us: i64::try_from(now_s(self)).unwrap_or(0) * 1_000_000,
            component: "balancer".to_owned(),
            kind: kind.to_owned(),
            subject_type: "shard".to_owned(),
            subject_id: format!("t{tenant}/g0/s0"),
            mode: Mode::Operator,
            outcome: Outcome::Succeeded,
            reason_code: reason_code.to_owned(),
            summary,
            evidence,
            alternatives: None,
            job_id: job.map(JobId::to_bytes),
            parent_id: None,
            actor: actor.to_owned(),
            dc_id: self.dc().to_string(),
        };
        if let Err(e) = self.decision_log().record(tenant, &d).await {
            tracing::warn!(%tenant, kind, error = %e, "operator decision not written");
        }
    }

    /// Records an operator's `sprigctl shard move` as a `shard.move`
    /// decision with mode `operator` (operator moves skip the balancer's
    /// rules, the 7-day move-back rule included).
    pub(crate) async fn record_operator_move(&self, st: &JobStatus) {
        let (Some(from), Some(to)) = (&st.source, &st.target) else {
            return;
        };
        self.operator_decision(
            st.tenant,
            "shard.move",
            "operator",
            format!(
                "an operator moved t{}/g0/s0 from {from} to {to} (job {})",
                st.tenant, st.id
            ),
            json!({"from": from.to_string(), "to": to.to_string(), "job": st.id.to_string()}),
            "operator",
            Some(st.id),
        )
        .await;
        self.pin_follows_move(st, to).await;
    }

    /// A pinned tenant's operator move re-points its pin at the target
    /// (module docs). Best effort, like the decision: the move runs.
    async fn pin_follows_move(&self, st: &JobStatus, to: &ClusterId) {
        let Some(old) = self.pin_of(st.tenant).await else {
            return;
        };
        if old.cluster == *to {
            return;
        }
        let pin = TenantPin {
            cluster: to.clone(),
            ..old.clone()
        };
        if let Err(e) = self.pins().pin(&pin).await {
            tracing::warn!(tenant = %st.tenant, error = %e, "the pin didn't follow the move");
            return;
        }
        self.operator_decision(
            st.tenant,
            "shard.pin",
            "pin_follows_move",
            format!(
                "the pin of t{} moved from {} to {} with the operator's move (job {}); it \
                 returns to {} if the move rolls back",
                st.tenant, old.cluster, to, st.id, old.cluster
            ),
            json!({"from": old.cluster.to_string(), "to": to.to_string(),
                "job": st.id.to_string(), "pinned_by": old.pinned_by}),
            "operator",
            Some(st.id),
        )
        .await;
    }

    /// Pins a tenant (module docs).
    ///
    /// # Errors
    /// 39792 for a cluster the tenant is neither on nor moving to; the
    /// tenant lookup's and the stores' errors.
    pub async fn pin_tenant_cluster(&self, name: &str, req: PinRequest) -> Result<TenantPin> {
        let tenant = self.get_tenant(name).await?.tenant_id;
        let (map, _) = TenantMaps::new(self.stores.dc.clone()).load(tenant).await?;
        let current = Placement::of(&map)?.cluster;
        let cluster = match req.cluster.as_deref() {
            None => current.clone(),
            Some(c) => ClusterId::new(c).map_err(|e| invalid(e.to_string()))?,
        };
        if cluster != current {
            let jobs = MetaMover::new(JobStore::new(self.stores.dc.clone(), self.clock.clone()))
                .list(&JobFilter {
                    tenant: Some(tenant),
                    active_only: true,
                })
                .await?;
            if !jobs.iter().any(|j| j.target.as_ref() == Some(&cluster)) {
                return Err(invalid(format!(
                    "tenant {name} is on {current}, not {cluster}: move it first with \
                     `sprigctl shard move {name} --to {cluster} --pin`"
                )));
            }
        }
        let by = req.by.unwrap_or_else(|| "operator".to_owned());
        let pin = TenantPin {
            tenant_id: tenant,
            cluster: cluster.clone(),
            pinned_by: by.clone(),
            pinned_at: now_s(self),
            reason: req.reason.unwrap_or_default(),
        };
        self.pins().pin(&pin).await?;
        self.operator_decision(
            tenant,
            "shard.pin",
            "operator",
            format!("tenant {name} {}", pin.describe()),
            json!({"cluster": cluster.to_string(), "reason": pin.reason}),
            &by,
            None,
        )
        .await;
        Ok(pin)
    }

    /// Unpins a tenant.
    ///
    /// # Errors
    /// The tenant lookup's and the store's errors.
    pub async fn unpin_tenant_cluster(&self, name: &str) -> Result<UnpinResponse> {
        let tenant = self.get_tenant(name).await?.tenant_id;
        let was = self.pins().unpin(tenant).await?;
        if let Some(p) = &was {
            self.operator_decision(
                tenant,
                "shard.unpin",
                "operator",
                format!(
                    "tenant {name} unpinned from {}; the balancer may move it again",
                    p.cluster
                ),
                json!({"cluster": p.cluster.to_string()}),
                "operator",
                None,
            )
            .await;
        }
        Ok(UnpinResponse {
            tenant: name.to_owned(),
            was,
        })
    }

    /// A tenant's pin, for `explain placement`.
    pub(crate) async fn pin_of(&self, tenant: TenantId) -> Option<TenantPin> {
        self.pins().get(tenant).await.ok().flatten()
    }
}
