//! In-doubt transactions for operators (chapter 18, "Observability":
//! `sprigctl txn list`, `show` and `resolve`).
//!
//! A transaction is in doubt while any of its XA branches is prepared on
//! a cluster: `XA RECOVER` lists it there. [`list`] gathers the prepared
//! coordinator branches of the given clusters by transaction and reads
//! (never races) each one's decision row at its home anchor. [`show`] does
//! the same for one transaction. [`Resolver::resolve`] finishes one
//! transaction now, whatever its age, the way the resolver would: the
//! decision comes from the insert race at the home (a commit row commits,
//! anything else aborts), never from the operator. There is no forced
//! commit or rollback: an operator who guessed could split a transaction.
//!
//! ```text
//! list(clusters, tenant?):
//!   for each cluster C: XA RECOVER on C (unreachable: noted, skipped)
//!   group the coordinator xids by dtid (one branch per cluster)
//!   for each: locate the home anchor; read its decision row there
//! resolve(clusters, dtid):
//!   for each branch of dtid in XA RECOVER: decide_by_race at the home,
//!     then XA COMMIT or XA ROLLBACK (1397 while still listed: a live
//!     connection owns it; reported as unresolved)
//! ```

use std::collections::BTreeMap;
use std::fmt::Write as _;

use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};

use crate::backend::{AnchorLocator, ClusterAdmin};
use crate::decision::DecisionRow;
use crate::resolver::Resolver;
use crate::xid::{Dtid, Gtrid, Xid};

/// What is known of an in-doubt transaction's decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecisionSeen {
    /// The row at the home anchor (`commit`, `abort` or `pending`).
    Recorded(DecisionRow),
    /// No row yet: the resolver would race an abort.
    None,
    /// The home couldn't be located or read now.
    Unknown(String),
}

impl DecisionSeen {
    /// `commit`, `abort`, `pending`, `none` or `unknown`.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Recorded(r) => r.outcome.as_str(),
            Self::None => "none",
            Self::Unknown(_) => "unknown",
        }
    }
}

/// One in-doubt transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InDoubt {
    /// Its global id (tenant, dtid, start time, home anchor).
    pub gtrid: Gtrid,
    /// Its prepared branches, by cluster.
    pub branches: Vec<(ClusterId, Xid)>,
    /// Where its decision lives now (cluster, schema), when located.
    pub home_at: Option<(ClusterId, String)>,
    /// Its decision.
    pub decision: DecisionSeen,
}

/// What [`list`] found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InDoubtList {
    /// The transactions, oldest start first.
    pub txns: Vec<InDoubt>,
    /// Clusters whose `XA RECOVER` failed, with the error.
    pub unreachable: Vec<(ClusterId, String)>,
}

/// Prepared coordinator branches of `clusters` (of `tenant` only, when
/// given), by transaction, with each decision as recorded now.
pub async fn list(
    admin: &dyn ClusterAdmin,
    locator: &dyn AnchorLocator,
    clusters: &[ClusterId],
    tenant: Option<TenantId>,
) -> InDoubtList {
    let mut out = InDoubtList::default();
    let mut by: BTreeMap<Dtid, (Gtrid, Vec<(ClusterId, Xid)>)> = BTreeMap::new();
    for c in clusters {
        match admin.xa_recover(c).await {
            Ok(xids) => {
                for x in xids {
                    if tenant.is_some_and(|t| t != x.gtrid.tenant) {
                        continue;
                    }
                    by.entry(x.gtrid.dtid)
                        .or_insert_with(|| (x.gtrid, Vec::new()))
                        .1
                        .push((c.clone(), x));
                }
            }
            Err(e) => out.unreachable.push((c.clone(), e.to_string())),
        }
    }
    for (_, (gtrid, branches)) in by {
        let (home_at, decision) = read_decision(admin, locator, &gtrid).await;
        out.txns.push(InDoubt {
            gtrid,
            branches,
            home_at,
            decision,
        });
    }
    out.txns
        .sort_by_key(|t| (t.gtrid.start_ms, t.gtrid.dtid.to_bytes()));
    out
}

/// One in-doubt transaction.
///
/// # Errors
/// 39447 when no branch of `dtid` is prepared on `clusters` (the
/// unreachable ones are named).
pub async fn show(
    admin: &dyn ClusterAdmin,
    locator: &dyn AnchorLocator,
    clusters: &[ClusterId],
    dtid: Dtid,
) -> Result<InDoubt> {
    let l = list(admin, locator, clusters, None).await;
    l.txns
        .into_iter()
        .find(|t| t.gtrid.dtid == dtid)
        .ok_or_else(|| not_in_doubt(dtid, &l.unreachable))
}

fn not_in_doubt(dtid: Dtid, unreachable: &[(ClusterId, String)]) -> SprigError {
    let mut m =
        format!("no branch of transaction {dtid} is prepared on this data center's clusters");
    if !unreachable.is_empty() {
        let names: Vec<String> = unreachable.iter().map(|(c, _)| c.to_string()).collect();
        let _ = write!(m, " (not reachable: {})", names.join(", "));
    }
    SprigError::new(SprigCode::TxnNotInDoubt, m)
}

/// The decision row at `g`'s home, read without racing.
async fn read_decision(
    admin: &dyn ClusterAdmin,
    locator: &dyn AnchorLocator,
    g: &Gtrid,
) -> (Option<(ClusterId, String)>, DecisionSeen) {
    let (cluster, schema) = match locator.locate(g.tenant, g.home).await {
        Ok(at) => at,
        Err(e) => return (None, DecisionSeen::Unknown(e.to_string())),
    };
    let seen = match admin.read_decision(&cluster, &schema, g.dtid).await {
        Ok(Some(r)) => DecisionSeen::Recorded(r),
        Ok(None) => DecisionSeen::None,
        Err(e) => DecisionSeen::Unknown(e.to_string()),
    };
    (Some((cluster, schema)), seen)
}

/// What [`Resolver::resolve`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Resolved {
    /// Branches committed.
    pub committed: Vec<(ClusterId, Xid)>,
    /// Branches rolled back.
    pub rolled_back: Vec<(ClusterId, Xid)>,
    /// Branches left prepared: the decision couldn't be made or read, or
    /// a live connection owns the branch (retry later).
    pub unresolved: Vec<(ClusterId, Xid)>,
    /// Clusters whose `XA RECOVER` failed.
    pub unreachable: Vec<(ClusterId, String)>,
}

impl Resolver {
    /// Finishes every prepared branch of `dtid` on `clusters` now, by the
    /// transaction's decision (the insert race at its home decides abort
    /// when nothing is recorded), as a scan would once the branch is old
    /// enough.
    ///
    /// # Errors
    /// 39447 when no branch of `dtid` is prepared on `clusters`.
    pub async fn resolve(&self, clusters: &[ClusterId], dtid: Dtid) -> Result<Resolved> {
        let mut out = Resolved::default();
        let mut found = false;
        for c in clusters {
            let xids = match self.admin().xa_recover(c).await {
                Ok(l) => l,
                Err(e) => {
                    out.unreachable.push((c.clone(), e.to_string()));
                    continue;
                }
            };
            for x in xids.into_iter().filter(|x| x.gtrid.dtid == dtid) {
                found = true;
                match self.finish_branch(c, &x).await {
                    Some(true) => out.committed.push((c.clone(), x)),
                    Some(false) => out.rolled_back.push((c.clone(), x)),
                    None => out.unresolved.push((c.clone(), x)),
                }
            }
        }
        if !found {
            return Err(not_in_doubt(dtid, &out.unreachable));
        }
        tracing::info!(
            %dtid,
            committed = out.committed.len(),
            rolled_back = out.rolled_back.len(),
            unresolved = out.unresolved.len(),
            "operator resolved an in-doubt transaction"
        );
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::sync::{Arc, Mutex};

    use sprig_common::ids::{DbId, GroupId, ShardId};

    use super::*;
    use crate::StaticLocator;
    use crate::backend::{DbError, DbResult, errno};
    use crate::decision::Decision;
    use crate::settings::ResolverConfig;
    use crate::xid::{AnchorRef, DtidGen, bqual};

    const T: TenantId = TenantId(7);

    #[derive(Default)]
    struct Fake {
        prepared: Mutex<BTreeMap<ClusterId, Vec<Xid>>>,
        rows: Mutex<Vec<DecisionRow>>,
        down: Vec<ClusterId>,
        owned: Vec<Xid>,
    }

    #[async_trait::async_trait]
    impl ClusterAdmin for Fake {
        async fn xa_recover(&self, c: &ClusterId) -> DbResult<Vec<Xid>> {
            if self.down.contains(c) {
                return Err(DbError::lost("down"));
            }
            Ok(self
                .prepared
                .lock()
                .unwrap()
                .get(c)
                .cloned()
                .unwrap_or_default())
        }
        async fn xa_finish(&self, c: &ClusterId, x: &Xid, _commit: bool) -> DbResult<()> {
            if self.owned.contains(x) {
                return Err(DbError::new(errno::XAER_NOTA, "owned"));
            }
            if let Some(l) = self.prepared.lock().unwrap().get_mut(c) {
                l.retain(|y| y != x);
            }
            Ok(())
        }
        async fn kill(&self, _: &ClusterId, _: u64) -> DbResult<()> {
            Ok(())
        }
        async fn insert_decision(&self, _: &ClusterId, _: &str, row: &DecisionRow) -> DbResult<()> {
            let mut rows = self.rows.lock().unwrap();
            if rows.iter().any(|r| r.dtid == row.dtid) {
                return Err(DbError::new(errno::DUP_ENTRY, "dup"));
            }
            rows.push(row.clone());
            Ok(())
        }
        async fn read_decision(
            &self,
            _: &ClusterId,
            _: &str,
            d: Dtid,
        ) -> DbResult<Option<DecisionRow>> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .find(|r| r.dtid == d)
                .cloned())
        }
        async fn settle_pending(
            &self,
            _: &ClusterId,
            _: &str,
            _: Dtid,
            _: Decision,
        ) -> DbResult<()> {
            Ok(())
        }
        async fn decision_rows(&self, _: &ClusterId) -> DbResult<Vec<(String, DecisionRow)>> {
            Ok(Vec::new())
        }
        async fn delete_decision(&self, _: &ClusterId, _: &str, _: Dtid) -> DbResult<()> {
            Ok(())
        }
    }

    fn c(n: &str) -> ClusterId {
        ClusterId::new(n).unwrap()
    }

    fn home() -> AnchorRef {
        AnchorRef {
            db: DbId(1),
            group: GroupId(3),
            hash_point: 0,
        }
    }

    fn xid(g: Gtrid, on: &ClusterId) -> Xid {
        Xid {
            gtrid: g,
            bqual: bqual(on, ShardId(1)),
        }
    }

    fn gtrid(ids: &DtidGen, tenant: TenantId, start_ms: u64) -> Gtrid {
        Gtrid {
            tenant,
            dtid: ids.next(),
            start_ms,
            home: home(),
        }
    }

    /// Two transactions over two clusters: one with a commit row, one
    /// with none; a third tenant's branch; one cluster down.
    fn world() -> (Arc<Fake>, Arc<StaticLocator>, Gtrid, Gtrid) {
        let ids = DtidGen::new(1);
        let (a, b) = (c("dc-a-c1"), c("dc-a-c2"));
        let committed = gtrid(&ids, T, 200);
        let open = gtrid(&ids, T, 100);
        let other = gtrid(&ids, TenantId(8), 50);
        let mut prepared = BTreeMap::new();
        prepared.insert(
            a.clone(),
            vec![xid(committed, &a), xid(open, &a), xid(other, &a)],
        );
        prepared.insert(b.clone(), vec![xid(committed, &b)]);
        let fake = Fake {
            prepared: Mutex::new(prepared),
            rows: Mutex::new(vec![DecisionRow::new(
                committed.dtid,
                Decision::Commit,
                "gw1",
            )]),
            down: vec![c("dc-a-c3")],
            owned: Vec::new(),
        };
        let loc = StaticLocator::new();
        loc.insert(T, home(), a, "t7_d1_g3_s1");
        (Arc::new(fake), Arc::new(loc), committed, open)
    }

    fn clusters() -> Vec<ClusterId> {
        vec![c("dc-a-c1"), c("dc-a-c2"), c("dc-a-c3")]
    }

    #[tokio::test]
    async fn list_groups_branches_and_reads_decisions_without_racing() {
        let (fake, loc, committed, open) = world();
        let l = list(&*fake, &*loc, &clusters(), Some(T)).await;
        assert_eq!(l.unreachable.len(), 1);
        assert_eq!(l.txns.len(), 2, "{l:?}");
        // Oldest first.
        assert_eq!(l.txns[0].gtrid.dtid, open.dtid);
        assert_eq!(l.txns[0].decision, DecisionSeen::None);
        assert_eq!(l.txns[1].gtrid.dtid, committed.dtid);
        assert_eq!(l.txns[1].branches.len(), 2);
        assert_eq!(l.txns[1].decision.as_str(), "commit");
        // Reading didn't record anything.
        assert_eq!(fake.rows.lock().unwrap().len(), 1);
        // Without a tenant filter the other tenant's branch shows (its
        // home isn't known to the locator).
        let all = list(&*fake, &*loc, &clusters(), None).await;
        assert_eq!(all.txns.len(), 3);
        assert_eq!(all.txns[0].decision.as_str(), "unknown");
    }

    #[tokio::test]
    async fn resolve_follows_the_decision_and_races_abort_when_none() {
        let (fake, loc, committed, open) = world();
        let r = Resolver::new(fake.clone(), loc.clone(), ResolverConfig::default());
        let done = r.resolve(&clusters(), committed.dtid).await.unwrap();
        assert_eq!(done.committed.len(), 2);
        assert!(done.rolled_back.is_empty() && done.unresolved.is_empty());
        let done = r.resolve(&clusters(), open.dtid).await.unwrap();
        assert_eq!(done.rolled_back.len(), 1);
        // The race recorded the abort at the home.
        assert!(
            fake.rows
                .lock()
                .unwrap()
                .iter()
                .any(|row| row.dtid == open.dtid && row.outcome == Decision::Abort)
        );
        // Nothing left of either: 39447.
        let e = r.resolve(&clusters(), open.dtid).await.unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::TxnNotInDoubt));
        assert!(e.to_string().contains("dc-a-c3"), "{e}");
        let e = show(&*fake, &*loc, &clusters(), committed.dtid)
            .await
            .unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::TxnNotInDoubt));
    }

    #[tokio::test]
    async fn a_branch_a_live_connection_owns_stays_unresolved() {
        let (fake, loc, committed, _) = world();
        let a = c("dc-a-c1");
        let owned = xid(committed, &a);
        let fake = Arc::new(Fake {
            prepared: Mutex::new(fake.prepared.lock().unwrap().clone()),
            rows: Mutex::new(fake.rows.lock().unwrap().clone()),
            down: Vec::new(),
            owned: vec![owned.clone()],
        });
        let r = Resolver::new(fake.clone(), loc, ResolverConfig::default());
        let done = r.resolve(&clusters(), committed.dtid).await.unwrap();
        assert_eq!(done.committed.len(), 1);
        assert_eq!(done.unresolved, vec![(a, owned)]);
    }
}
