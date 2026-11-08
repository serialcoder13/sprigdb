//! Deciding by insert race (chapter 18): the one rule the coordinator and
//! the resolver share.
//!
//! ```text
//! decide_by_race(schema S, dtid):
//!   INSERT INTO S.__sprig_dtx (dtid, 'abort', me)
//!     success         → abort
//!     duplicate key   → read the row: commit or abort → it
//!                       pending → decide at row.decider; settle the pending row
//!     lock wait, other → unknown; retry on the next round
//! ```
//!
//! `InnoDB` keeps one row per primary key, and an insert that meets an
//! uncommitted row with the same key waits for that transaction to end, so
//! exactly one outcome is ever recorded per dtid.

use sprig_common::ids::{ClusterId, TenantId};

use crate::backend::{AnchorLocator, ClusterAdmin, DbError, errno};
use crate::decision::{Decision, DecisionRow};
use crate::xid::Dtid;

/// What a race found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Race {
    /// The recorded outcome: [`Decision::Commit`] or [`Decision::Abort`].
    Decided(Decision),
    /// Nothing could be recorded or read this round.
    Unknown(DbError),
}

/// Where to race: a cluster and an anchor schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaceAt<'a> {
    /// The cluster.
    pub cluster: &'a ClusterId,
    /// The anchor schema.
    pub schema: &'a str,
}

/// Races an `abort` row for `dtid` at `at`. A `pending` row is followed to
/// its decider through `locator` (resolver only: the coordinator always
/// races where the last resource's own row is) and, when `settle`, updated
/// to the outcome.
pub async fn decide_by_race(
    admin: &dyn ClusterAdmin,
    locator: Option<&dyn AnchorLocator>,
    tenant: TenantId,
    at: RaceAt<'_>,
    dtid: Dtid,
    me: &str,
    settle: bool,
) -> Race {
    race_inner(admin, locator, tenant, at, dtid, me, settle, 0).await
}

#[allow(clippy::too_many_arguments)]
fn race_inner<'a>(
    admin: &'a dyn ClusterAdmin,
    locator: Option<&'a dyn AnchorLocator>,
    tenant: TenantId,
    at: RaceAt<'a>,
    dtid: Dtid,
    me: &'a str,
    settle: bool,
    depth: u8,
) -> futures::future::BoxFuture<'a, Race> {
    Box::pin(async move {
        let row = DecisionRow::new(dtid, Decision::Abort, me);
        match admin.insert_decision(at.cluster, at.schema, &row).await {
            Ok(()) => return Race::Decided(Decision::Abort),
            Err(e) if e.errno == errno::DUP_ENTRY => {}
            Err(e) => return Race::Unknown(e),
        }
        let found = match admin.read_decision(at.cluster, at.schema, dtid).await {
            Ok(Some(r)) => r,
            Ok(None) => return Race::Unknown(DbError::new(0, "decision row vanished")),
            Err(e) => return Race::Unknown(e),
        };
        match found.outcome {
            Decision::Commit | Decision::Abort => Race::Decided(found.outcome),
            Decision::Pending => {
                let (Some(loc), Some(decider)) = (locator, found.decider) else {
                    return Race::Unknown(DbError::new(0, "pending row without a decider"));
                };
                if depth > 2 {
                    return Race::Unknown(DbError::new(0, "decider chain too long"));
                }
                let (c, s) = match loc.locate(tenant, decider).await {
                    Ok(x) => x,
                    Err(e) => return Race::Unknown(DbError::new(0, e.to_string())),
                };
                let r = race_inner(
                    admin,
                    locator,
                    tenant,
                    RaceAt {
                        cluster: &c,
                        schema: &s,
                    },
                    dtid,
                    me,
                    settle,
                    depth + 1,
                )
                .await;
                if settle && let Race::Decided(o) = &r {
                    // Best effort: a later pass settles it if this fails.
                    let _ = admin.settle_pending(at.cluster, at.schema, dtid, *o).await;
                }
                r
            }
        }
    })
}
