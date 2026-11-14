//! The real [`MoveExecutor`]: the mover ([`sprig_mover::Mover`]).
//!
//! A move takes a tenant's whole placement unit (its catalog and every
//! database's shard schema, kept on one cluster), so an order becomes
//! [`JobSpec::move_tenant`] with the decision id. Job states map to move
//! statuses: committed states (`post_cutover` onwards) are `Succeeded`,
//! because the shard map already points at the target; `rolled_back` is
//! `Failed` (or `Cancelled` when cancellation was asked); `planned` is
//! `Queued`; everything else is `Running`.

use std::sync::Arc;

use sprig_common::ids::JobId;
use sprig_common::{Result, SprigError};
use sprig_mover::{JobSpec, JobState, JobStatus, Mover};

use crate::schedule::{MoveExecutor, MoveOrder, MoveStatus, MoveTicket};

/// Runs moves through the mover.
pub struct MoverExecutor {
    mover: Arc<dyn Mover>,
}

impl std::fmt::Debug for MoverExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MoverExecutor").finish_non_exhaustive()
    }
}

impl MoverExecutor {
    /// An executor over `mover`.
    #[must_use]
    pub fn new(mover: Arc<dyn Mover>) -> Self {
        Self { mover }
    }
}

/// The job spec for an order.
#[must_use]
pub fn spec_of(order: &MoveOrder) -> JobSpec {
    let mut spec = JobSpec::move_tenant(order.tenant, order.from.clone(), order.to.clone())
        .with_decision(order.decision_id);
    spec.requested_by = Some("balancer".to_owned());
    spec
}

/// The move status of a job.
#[must_use]
pub fn status_of(st: &JobStatus) -> MoveStatus {
    if st.state.is_committed() {
        return MoveStatus::Succeeded;
    }
    match st.state {
        JobState::RolledBack if st.cancel_requested => MoveStatus::Cancelled,
        JobState::RolledBack => MoveStatus::Failed {
            reason: st.error.clone().unwrap_or_else(|| "rolled back".to_owned()),
        },
        JobState::Planned => MoveStatus::Queued,
        _ => MoveStatus::Running,
    }
}

fn job_id(t: &MoveTicket) -> Result<JobId> {
    t.0.parse()
        .map_err(|e| SprigError::internal(format!("bad mover job id {:?}: {e}", t.0)))
}

#[async_trait::async_trait]
impl MoveExecutor for MoverExecutor {
    async fn submit(&self, order: &MoveOrder) -> Result<MoveTicket> {
        let id = self.mover.submit(spec_of(order)).await?;
        Ok(MoveTicket(id.to_string()))
    }

    async fn status(&self, ticket: &MoveTicket) -> Result<MoveStatus> {
        let st = self.mover.status(&job_id(ticket)?).await?;
        Ok(status_of(&st))
    }

    async fn cancel(&self, ticket: &MoveTicket, reason: &str) -> Result<()> {
        self.mover.cancel(&job_id(ticket)?, reason).await
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_common::ids::{ClusterId, TenantId};

    use super::*;
    use crate::model::ShardKey;
    use crate::plan::MoveReason;

    fn st(state: JobState, cancel: bool) -> JobStatus {
        JobStatus {
            id: JobId::new_v7(),
            kind: sprig_mover::JobKind::MoveShard,
            tenant: TenantId(42),
            state,
            source: None,
            target: None,
            progress: sprig_mover::api::Progress::default(),
            error: Some("verify failed".to_owned()),
            cancel_requested: cancel,
            decision_id: Some(7),
            epoch: None,
            created_ms: 0,
            updated_ms: 0,
        }
    }

    #[test]
    fn job_states_map_to_move_statuses() {
        assert_eq!(status_of(&st(JobState::Planned, false)), MoveStatus::Queued);
        assert_eq!(
            status_of(&st(JobState::Copying, false)),
            MoveStatus::Running
        );
        assert_eq!(
            status_of(&st(JobState::CuttingOver, false)),
            MoveStatus::Running
        );
        for s in [
            JobState::PostCutover,
            JobState::Retaining,
            JobState::Cleaning,
            JobState::Done,
        ] {
            assert_eq!(status_of(&st(s, false)), MoveStatus::Succeeded);
        }
        assert_eq!(
            status_of(&st(JobState::RolledBack, false)),
            MoveStatus::Failed {
                reason: "verify failed".to_owned()
            }
        );
        assert_eq!(
            status_of(&st(JobState::RolledBack, true)),
            MoveStatus::Cancelled
        );
    }

    #[test]
    fn orders_become_tenant_moves_with_the_decision() {
        let o = MoveOrder {
            tenant: TenantId(42),
            shards: vec![ShardKey::new(42, 0, 0)],
            from: ClusterId::new("mum1-c1").unwrap(),
            to: ClusterId::new("mum1-c2").unwrap(),
            reason: MoveReason::Spread,
            decision_id: 99,
            size_bytes: 1.0,
        };
        let s = spec_of(&o);
        assert_eq!(s.decision_id, Some(99));
        assert_eq!(s.tenant, TenantId(42));
        assert_eq!(s.sources[0].cluster, o.from);
        assert_eq!(s.targets[0].cluster, o.to);
        assert_eq!(s.requested_by.as_deref(), Some("balancer"));
    }
}
