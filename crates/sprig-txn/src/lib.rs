//! `sprig-txn`: distributed transactions, the decision log and the
//! resolver.
//!
//! Layer: 2 execution. Design: [chapter 18](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/18-transactions.md) (Distributed transactions) of the SprigDB low-level design.
//!
//! - [`coordinator`]: [`TxnCoordinator`], one per session: branches per
//!   cluster, the write rule and `txn.non_xa_policy`, savepoints, and
//!   commit as local, XA two-phase with a last resource, or best effort.
//! - [`race`]: deciding by insert race on `__sprig_dtx`, shared by the
//!   coordinator and the resolver.
//! - [`resolver`]: finishes in-doubt branches after crashes, drains a
//!   tenant's prepared branches, deletes decision rows.
//! - [`backend`]: the seams to MariaDB; [`mysql`] implements them on
//!   `mysql_async`, the gateway on its own connections, and the
//!   simulation (`tests/sim_txn.rs`) on the MariaDB model.
//! - [`lookup`]: `DistTxn`, chapter 19's lookup sessions (Pre, Main,
//!   Post) on the coordinator.
//! - [`xid`], [`decision`], [`settings`]: ids and xids, decision rows,
//!   `txn.*` keys.
//!
//! Not yet (later build steps): Sprig 2PC (R3), client XA rewriting,
//! eager consistent-snapshot branches, SERIALIZABLE autocommit reads,
//! waiting for mover holds before prepare, and the gateway wiring (with
//! chapter 17's router).

pub mod backend;
pub mod coordinator;
pub mod decision;
pub mod lookup;
pub mod mysql;
pub mod race;
pub mod resolver;
pub mod settings;
pub mod xid;

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};

pub use backend::{
    AnchorLocator, BranchConn, ClusterAdmin, ClusterCaps, Ctl, DbError, DbResult, TxnBackend,
};
pub use coordinator::{
    BeginOptions, Branch, BranchKind, CommitOutcome, Intent, Point, ShardRef, StmtMark,
    TxnCoordinator, TxnHooks, TxnMode,
};
pub use decision::{Decision, DecisionRow};
pub use resolver::{DrainReport, Resolver, ScanReport, SweepReport};
pub use settings::{NonXaPolicy, ResolverConfig, TxnConfig};
pub use xid::{AnchorRef, Dtid, DtidGen, Gtrid, Xid};

use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::{SprigCode, SprigError};

/// An [`AnchorLocator`] over a fixed table, for tests and tools until
/// chapter 15's group maps place anchors.
#[derive(Debug, Default)]
pub struct StaticLocator {
    map: Mutex<BTreeMap<(TenantId, AnchorRef), (ClusterId, String)>>,
}

impl StaticLocator {
    /// An empty table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Places `anchor` of `tenant` in `schema` on `cluster`.
    pub fn insert(&self, tenant: TenantId, anchor: AnchorRef, cluster: ClusterId, schema: &str) {
        self.map
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert((tenant, anchor), (cluster, schema.to_owned()));
    }
}

#[async_trait::async_trait]
impl AnchorLocator for StaticLocator {
    async fn locate(
        &self,
        tenant: TenantId,
        anchor: AnchorRef,
    ) -> sprig_common::Result<(ClusterId, String)> {
        self.map
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(tenant, anchor))
            .cloned()
            .ok_or_else(|| {
                SprigError::new(
                    SprigCode::ClusterNotFound,
                    format!("no shard holds anchor {anchor} of tenant {tenant}"),
                )
            })
    }
}
