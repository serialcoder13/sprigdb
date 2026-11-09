//! The Admin API's calls for in-doubt distributed transactions (chapter
//! 18, "Observability"; `sprigctl txn list|show|resolve`), over
//! `sprig_txn::indoubt`.
//!
//! ```text
//! GET  /admin/v1/txns[?tenant=<name>]          prepared branches by transaction
//! GET  /admin/v1/txns/{dtid}                   one transaction
//! POST /admin/v1/txns/{dtid}:resolve           finish it now, by its decision
//! ```
//!
//! The calls act on this data center's clusters (`active`, `draining`,
//! `drained`), as `sprig_ctl`, on a pool budget of their own
//! (`txn_admin`, one connection per cluster). Listing reads decision rows
//! and never races; resolving decides by the insert race at the home, as
//! the resolver does: an operator can't choose the outcome.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sprig_common::ids::{ClusterId, TenantId, TxnId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_shard::{ShardMapStore, TenantMaps};
use sprig_txn::indoubt::{self, DecisionSeen, InDoubt};
use sprig_txn::mysql::MyAdmin;
use sprig_txn::{AnchorLocator, ClusterAdmin, MapLocator, Resolver, ResolverConfig, Xid};

use super::AdminService;
use crate::catalogs::{ClusterPools, RegistryPools};
use crate::txn_resolver::{CtlPools, scanned};

/// Connections per cluster for these calls.
const POOL_MAX: usize = 1;

/// One prepared branch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchView {
    /// The cluster holding it.
    pub cluster: String,
    /// Its branch qualifier (`c<cluster>.s<shard>`).
    pub bqual: String,
}

/// One in-doubt transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxnView {
    /// The dtid, 32 hex digits.
    pub dtid: String,
    /// The tenant id.
    pub tenant: u64,
    /// When the transaction started (ms since the Unix epoch).
    pub started_ms: u64,
    /// The home anchor (`d<db>.g<group>@<hash point>`).
    pub home: String,
    /// Where the home's decision rows are now (`cluster/schema`).
    pub home_at: Option<String>,
    /// `commit`, `abort`, `pending`, `none` (nothing recorded: a resolve
    /// rolls it back) or `unknown` (the home can't be read now).
    pub decision: String,
    /// Who recorded the decision (a gateway id, `resolver`, ...).
    pub decided_by: Option<String>,
    /// The decision row's age in seconds.
    pub decision_age_seconds: Option<u64>,
    /// Why the decision is unknown.
    pub note: Option<String>,
    /// The prepared branches.
    pub branches: Vec<BranchView>,
}

/// `GET /admin/v1/txns`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxnList {
    /// In-doubt transactions, oldest first.
    pub transactions: Vec<TxnView>,
    /// Clusters that couldn't be read, with the error.
    pub unreachable: Vec<String>,
}

/// `POST /admin/v1/txns/{dtid}:resolve`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolveView {
    /// The dtid.
    pub dtid: String,
    /// Branches committed.
    pub committed: Vec<BranchView>,
    /// Branches rolled back.
    pub rolled_back: Vec<BranchView>,
    /// Branches left prepared (a live connection owns them, or the
    /// decision couldn't be made): retry later.
    pub unresolved: Vec<BranchView>,
    /// Clusters that couldn't be read.
    pub unreachable: Vec<String>,
}

fn branch(c: &ClusterId, x: &Xid) -> BranchView {
    BranchView {
        cluster: c.to_string(),
        bqual: x.bqual.clone(),
    }
}

fn view(t: &InDoubt) -> TxnView {
    let (decided_by, age, note) = match &t.decision {
        DecisionSeen::Recorded(r) => (Some(r.coordinator.clone()), r.age_seconds, None),
        DecisionSeen::None => (None, None, None),
        DecisionSeen::Unknown(e) => (None, None, Some(e.clone())),
    };
    TxnView {
        dtid: t.gtrid.dtid.to_string(),
        tenant: t.gtrid.tenant.0,
        started_ms: t.gtrid.start_ms,
        home: t.gtrid.home.to_string(),
        home_at: t.home_at.as_ref().map(|(c, s)| format!("{c}/{s}")),
        decision: t.decision.as_str().to_owned(),
        decided_by,
        decision_age_seconds: age,
        note,
        branches: t.branches.iter().map(|(c, x)| branch(c, x)).collect(),
    }
}

/// The admin, locator and resolver these calls use.
pub struct TxnAdmin {
    admin: Arc<dyn ClusterAdmin>,
    locator: Arc<dyn AnchorLocator>,
    resolver: Resolver,
}

impl std::fmt::Debug for TxnAdmin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnAdmin").finish_non_exhaustive()
    }
}

impl TxnAdmin {
    /// Over `admin` and `locator`; rows the resolve races in name
    /// `operator` as their coordinator.
    #[must_use]
    pub fn new(admin: Arc<dyn ClusterAdmin>, locator: Arc<dyn AnchorLocator>) -> Self {
        let cfg = ResolverConfig {
            id: "operator".to_owned(),
            ..ResolverConfig::default()
        };
        Self {
            resolver: Resolver::new(Arc::clone(&admin), Arc::clone(&locator), cfg),
            admin,
            locator,
        }
    }

    /// In-doubt transactions on `clusters` (of `tenant` only, when given).
    pub async fn list(&self, clusters: &[ClusterId], tenant: Option<TenantId>) -> TxnList {
        let l = indoubt::list(&*self.admin, &*self.locator, clusters, tenant).await;
        TxnList {
            transactions: l.txns.iter().map(view).collect(),
            unreachable: l
                .unreachable
                .iter()
                .map(|(c, e)| format!("{c}: {e}"))
                .collect(),
        }
    }

    /// One in-doubt transaction.
    ///
    /// # Errors
    /// 39447 when none of its branches is prepared on `clusters`.
    pub async fn show(&self, clusters: &[ClusterId], dtid: TxnId) -> Result<TxnView> {
        indoubt::show(&*self.admin, &*self.locator, clusters, dtid)
            .await
            .map(|t| view(&t))
    }

    /// Finishes one transaction by its decision.
    ///
    /// # Errors
    /// 39447 when none of its branches is prepared on `clusters`.
    pub async fn resolve(&self, clusters: &[ClusterId], dtid: TxnId) -> Result<ResolveView> {
        let r = self.resolver.resolve(clusters, dtid).await?;
        let list = |v: &[(ClusterId, Xid)]| v.iter().map(|(c, x)| branch(c, x)).collect();
        Ok(ResolveView {
            dtid: dtid.to_string(),
            committed: list(&r.committed),
            rolled_back: list(&r.rolled_back),
            unresolved: list(&r.unresolved),
            unreachable: r
                .unreachable
                .iter()
                .map(|(c, e)| format!("{c}: {e}"))
                .collect(),
        })
    }
}

/// A dtid from its text form.
///
/// # Errors
/// 39792 for anything but 32 hex digits.
pub fn parse_dtid(s: &str) -> Result<TxnId> {
    s.parse::<TxnId>().map_err(|_| {
        SprigError::new(
            SprigCode::AdminRequestInvalid,
            format!("{s:?} is not a dtid (32 hex digits, as `sprigctl txn list` prints it)"),
        )
    })
}

impl AdminService {
    /// The transaction calls' admin, built on first use over this
    /// controller's registry and the DC tier's shard maps.
    fn txn(&self) -> &Arc<TxnAdmin> {
        self.txn.get_or_init(|| {
            let pools: Arc<dyn ClusterPools> = Arc::new(
                RegistryPools::new(
                    self.monitor.registry().clone(),
                    Some(Arc::clone(&self.monitor)),
                )
                .budget("txn_admin", POOL_MAX),
            );
            let admin = Arc::new(MyAdmin::over(Arc::new(CtlPools(pools))));
            let locator = MapLocator::new(
                ShardMapStore::new(Arc::clone(&self.stores.dc)),
                TenantMaps::new(Arc::clone(&self.stores.dc)),
                Arc::clone(&admin) as Arc<dyn sprig_txn::GroupSchemas>,
            );
            Arc::new(TxnAdmin::new(admin, Arc::new(locator)))
        })
    }

    /// The DC's clusters that may hold tenant data.
    async fn txn_clusters(&self) -> Result<Vec<ClusterId>> {
        Ok(self
            .monitor
            .registry()
            .list()
            .await?
            .iter()
            .map(sprig_cluster::registry::StoredCluster::body)
            .filter(|c| scanned(c.admin_state))
            .map(|c| c.cluster_id.clone())
            .collect())
    }

    /// `sprigctl txn list [--tenant <name>]`.
    ///
    /// # Errors
    /// The tenant lookup's and the registry's errors.
    pub async fn list_txns(&self, tenant: Option<&str>) -> Result<TxnList> {
        let t = match tenant {
            Some(name) => Some(self.get_tenant(name).await?.tenant_id),
            None => None,
        };
        let clusters = self.txn_clusters().await?;
        Ok(self.txn().list(&clusters, t).await)
    }

    /// `sprigctl txn show <dtid>`.
    ///
    /// # Errors
    /// 39792 for a malformed dtid; 39447 when it isn't in doubt.
    pub async fn show_txn(&self, dtid: &str) -> Result<TxnView> {
        let d = parse_dtid(dtid)?;
        let clusters = self.txn_clusters().await?;
        self.txn().show(&clusters, d).await
    }

    /// `sprigctl txn resolve <dtid>`.
    ///
    /// # Errors
    /// 39792 for a malformed dtid; 39447 when it isn't in doubt.
    pub async fn resolve_txn(&self, dtid: &str) -> Result<ResolveView> {
        let d = parse_dtid(dtid)?;
        let clusters = self.txn_clusters().await?;
        self.txn().resolve(&clusters, d).await
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn dtids_parse_as_32_hex_digits() {
        let d = parse_dtid("0000000000000001000000000000002a").unwrap();
        assert_eq!(d.0, (1u128 << 64) | 0x2a);
        let e = parse_dtid("xyz").unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::AdminRequestInvalid));
    }

    #[tokio::test]
    async fn an_empty_dc_lists_nothing_and_shows_39447() {
        let t = TxnAdmin::new(
            Arc::new(NoBranches),
            Arc::new(sprig_txn::StaticLocator::new()),
        );
        let l = t.list(&[], None).await;
        assert!(l.transactions.is_empty() && l.unreachable.is_empty());
        let e = t.show(&[], TxnId(1)).await.unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::TxnNotInDoubt));
        assert_eq!(e.code().map(SprigCode::number), Some(39447));
    }

    struct NoBranches;

    #[async_trait::async_trait]
    impl ClusterAdmin for NoBranches {
        async fn xa_recover(&self, _: &ClusterId) -> sprig_txn::DbResult<Vec<Xid>> {
            Ok(Vec::new())
        }
        async fn xa_finish(&self, _: &ClusterId, _: &Xid, _: bool) -> sprig_txn::DbResult<()> {
            Ok(())
        }
        async fn kill(&self, _: &ClusterId, _: u64) -> sprig_txn::DbResult<()> {
            Ok(())
        }
        async fn insert_decision(
            &self,
            _: &ClusterId,
            _: &str,
            _: &sprig_txn::DecisionRow,
        ) -> sprig_txn::DbResult<()> {
            Ok(())
        }
        async fn read_decision(
            &self,
            _: &ClusterId,
            _: &str,
            _: sprig_txn::Dtid,
        ) -> sprig_txn::DbResult<Option<sprig_txn::DecisionRow>> {
            Ok(None)
        }
        async fn settle_pending(
            &self,
            _: &ClusterId,
            _: &str,
            _: sprig_txn::Dtid,
            _: sprig_txn::Decision,
        ) -> sprig_txn::DbResult<()> {
            Ok(())
        }
        async fn decision_rows(
            &self,
            _: &ClusterId,
        ) -> sprig_txn::DbResult<Vec<(String, sprig_txn::DecisionRow)>> {
            Ok(Vec::new())
        }
        async fn delete_decision(
            &self,
            _: &ClusterId,
            _: &str,
            _: sprig_txn::Dtid,
        ) -> sprig_txn::DbResult<()> {
            Ok(())
        }
    }
}
