//! The resolver (chapter 18, "Resolver"): finishes in-doubt branches after
//! crashes, drains prepared branches before a mover fence, and deletes
//! decision rows that are no longer needed.
//!
//! ```text
//! every txn.resolver_interval_seconds, for each cluster C:
//!   for each coordinator xid in XA RECOVER on C:
//!     remember when it was first seen; younger than txn.resolver_min_age_seconds: skip
//!     o = decide_by_race(home anchor of the gtrid, dtid)
//!     XA COMMIT or XA ROLLBACK it; 1397 while still listed: a live connection
//!       owns it, retry on the next scan
//!   for each held branch with redo rows on C older than min_age (Sprig 2PC, R3):
//!     o = decide_by_race(home anchor of its gtrid, dtid)
//!     commit: replay its statements and delete the rows, in one transaction
//!     abort:  delete the rows
//!   sweep: delete commit and pending rows whose prepared participants are
//!     all finished (no XA branch listed, no redo row left), and abort rows
//!     past the retention period
//! ```
//!
//! Age is measured from when the resolver first saw the branch prepared,
//! so a restarted resolver waits a full `min_age` again: safe, only slower.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::Duration;

use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use tokio::time::Instant;

use crate::backend::{AnchorLocator, ClusterAdmin, errno};
use crate::decision::{Decision, ParticipantKind};
use crate::race::{Race, RaceAt, decide_by_race};
use crate::settings::ResolverConfig;
use crate::xid::{Gtrid, Xid};

/// What one scan of a cluster did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanReport {
    /// Coordinator xids listed by `XA RECOVER`.
    pub listed: usize,
    /// Younger than `txn.resolver_min_age_seconds`: the coordinator's job.
    pub young: usize,
    /// Committed by this scan.
    pub committed: usize,
    /// Rolled back by this scan.
    pub rolled_back: usize,
    /// Old enough but not finished (decision unknown, or a live
    /// connection owns the branch).
    pub unresolved: usize,
    /// Age of the oldest listed branch, since first seen.
    pub oldest: Duration,
}

/// What one pass over a cluster's redo rows did (Sprig 2PC, R3).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RedoReport {
    /// Held branches with redo rows.
    pub listed: usize,
    /// Younger than `txn.resolver_min_age_seconds`: the coordinator's job.
    pub young: usize,
    /// Committed branches replayed.
    pub replayed: usize,
    /// Rows left by a holder that finished meanwhile.
    pub gone: usize,
    /// Aborted branches whose rows were deleted.
    pub dropped: usize,
    /// Replays refused because a statement changed a different number
    /// of rows (the rows stay; an operator decides).
    pub hazards: usize,
    /// Not decided yet.
    pub unresolved: usize,
}

/// What a drain did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DrainReport {
    /// Branches committed.
    pub committed: usize,
    /// Branches rolled back.
    pub rolled_back: usize,
    /// Branches still prepared at the deadline.
    pub remaining: usize,
}

/// What a sweep of decision rows did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// Commit rows deleted.
    pub commit_deleted: usize,
    /// Abort rows deleted after the retention period.
    pub abort_deleted: usize,
    /// Pending rows deleted (their participants finished).
    pub settled: usize,
}

/// One data center's resolver.
pub struct Resolver {
    admin: Arc<dyn ClusterAdmin>,
    locator: Arc<dyn AnchorLocator>,
    cfg: RwLock<ResolverConfig>,
    first_seen: Mutex<HashMap<(ClusterId, Xid), Instant>>,
}

impl std::fmt::Debug for Resolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Resolver")
            .field("cfg", &self.config())
            .finish_non_exhaustive()
    }
}

/// The tenant of a shard schema `t<tenant>_...`.
#[must_use]
pub fn tenant_of_schema(schema: &str) -> Option<TenantId> {
    let rest = schema.strip_prefix('t')?;
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok().map(TenantId)
}

impl Resolver {
    /// A resolver acting through `admin` (as `sprig_ctl`).
    pub fn new(
        admin: Arc<dyn ClusterAdmin>,
        locator: Arc<dyn AnchorLocator>,
        cfg: ResolverConfig,
    ) -> Self {
        Self {
            admin,
            locator,
            cfg: RwLock::new(cfg),
            first_seen: Mutex::new(HashMap::new()),
        }
    }

    /// The configuration now.
    #[must_use]
    pub fn config(&self) -> ResolverConfig {
        self.cfg
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Replaces the configuration (live settings changed). Branches keep
    /// their first-seen times.
    pub fn set_config(&self, cfg: ResolverConfig) {
        *self.cfg.write().unwrap_or_else(PoisonError::into_inner) = cfg;
    }

    fn seen(&self) -> std::sync::MutexGuard<'_, HashMap<(ClusterId, Xid), Instant>> {
        self.first_seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The transaction's outcome, deciding abort if nothing is recorded
    /// yet (the insert race at the gtrid's home anchor).
    ///
    /// # Errors
    /// 39100 when the decision can't be recorded or read now.
    pub async fn decide(&self, g: &Gtrid) -> Result<Decision> {
        let (cluster, schema) = self.locator.locate(g.tenant, g.home).await?;
        let me = self.config().id;
        match decide_by_race(
            &*self.admin,
            Some(&*self.locator),
            g.tenant,
            RaceAt {
                cluster: &cluster,
                schema: &schema,
            },
            g.dtid,
            &me,
            true,
        )
        .await
        {
            Race::Decided(d) => Ok(d),
            Race::Unknown(e) => Err(SprigError::new(
                SprigCode::ClusterUnavailable,
                format!("could not decide {}: {e}", g.dtid),
            )),
        }
    }

    /// The admin it acts through.
    pub(crate) fn admin(&self) -> &Arc<dyn ClusterAdmin> {
        &self.admin
    }

    /// [`Self::finish`], for `indoubt`'s operator resolve.
    pub(crate) async fn finish_branch(&self, cluster: &ClusterId, x: &Xid) -> Option<bool> {
        self.finish(cluster, x).await
    }

    /// Decides and finishes one branch. `Some(committed?)` once finished.
    async fn finish(&self, cluster: &ClusterId, x: &Xid) -> Option<bool> {
        let d = self.decide(&x.gtrid).await.ok()?;
        let commit = d == Decision::Commit;
        match self.admin.xa_finish(cluster, x, commit).await {
            Ok(()) => {}
            Err(e) if e.errno == errno::XAER_NOTA => {
                // Finished meanwhile (not listed), or owned by a live
                // connection (listed): retry on the next scan.
                let list = self.admin.xa_recover(cluster).await.ok()?;
                if list.contains(x) {
                    return None;
                }
            }
            Err(_) => return None,
        }
        metrics::counter!("sprig_txn_resolutions_total", "outcome" => d.as_str()).increment(1);
        if !commit {
            metrics::counter!("sprig_txn_rollbacks_total", "reason" => "resolver").increment(1);
        }
        tracing::info!(%cluster, xid = %x, outcome = %d, "resolver finished an in-doubt branch");
        Some(commit)
    }

    /// Scans one cluster's `XA RECOVER` and finishes branches older than
    /// `txn.resolver_min_age_seconds`.
    ///
    /// # Errors
    /// 39100 when `XA RECOVER` fails.
    pub async fn scan(&self, cluster: &ClusterId) -> Result<ScanReport> {
        let xids = self.admin.xa_recover(cluster).await.map_err(|e| {
            SprigError::new(
                SprigCode::ClusterUnavailable,
                format!("XA RECOVER on {cluster}: {e}"),
            )
        })?;
        let now = Instant::now();
        let ages: Vec<(Xid, Duration)> = {
            let mut seen = self.seen();
            seen.retain(|(c, x), _| c != cluster || xids.contains(x));
            xids.iter()
                .map(|x| {
                    let t = *seen.entry((cluster.clone(), x.clone())).or_insert(now);
                    (x.clone(), now.saturating_duration_since(t))
                })
                .collect()
        };
        let min_age = self.config().min_age;
        let mut r = ScanReport {
            listed: xids.len(),
            ..ScanReport::default()
        };
        for (x, age) in ages {
            r.oldest = r.oldest.max(age);
            if age < min_age {
                r.young += 1;
                continue;
            }
            match self.finish(cluster, &x).await {
                Some(true) => r.committed += 1,
                Some(false) => r.rolled_back += 1,
                None => r.unresolved += 1,
            }
        }
        let c = cluster.to_string();
        #[allow(clippy::cast_precision_loss)]
        {
            metrics::gauge!("sprig_txn_in_doubt_branches", "cluster" => c.clone())
                .set((r.listed - r.committed - r.rolled_back) as f64);
        }
        metrics::gauge!("sprig_txn_oldest_prepared_age_seconds", "cluster" => c)
            .set(r.oldest.as_secs_f64());
        Ok(r)
    }

    /// Decides one held branch and replays or drops its redo rows; a
    /// finished branch's rows are deleted (a replay's marker only after
    /// `txn.abort_record_retention_hours`).
    async fn finish_redo(
        &self,
        cluster: &ClusterId,
        b: &crate::redo::RedoBranch,
        r: &mut RedoReport,
    ) {
        match b.mark {
            Some(crate::redo::RedoMark::Done) => {
                if self
                    .admin
                    .delete_redo(cluster, &b.schema, b.dtid)
                    .await
                    .is_ok()
                {
                    r.gone += 1;
                }
                return;
            }
            Some(crate::redo::RedoMark::Replayed) => {
                if b.age_seconds >= self.config().abort_retention.as_secs()
                    && self
                        .admin
                        .delete_redo(cluster, &b.schema, b.dtid)
                        .await
                        .is_ok()
                {
                    r.gone += 1;
                }
                return;
            }
            None => {}
        }
        let Ok(d) = self.decide(&b.gtrid).await else {
            r.unresolved += 1;
            return;
        };
        match d {
            Decision::Commit => match self.admin.replay_redo(cluster, &b.schema, b.dtid).await {
                Ok(crate::redo::Replay::Applied) => {
                    r.replayed += 1;
                    metrics::counter!("sprig_txn_redo_replays_total", "result" => "applied")
                        .increment(1);
                    tracing::info!(%cluster, dtid = %b.dtid, "resolver replayed a held branch");
                }
                Ok(crate::redo::Replay::Gone) => r.gone += 1,
                Ok(crate::redo::Replay::Hazard(why)) => {
                    r.hazards += 1;
                    metrics::counter!("sprig_txn_redo_replays_total", "result" => "hazard")
                        .increment(1);
                    tracing::error!(%cluster, dtid = %b.dtid, %why,
                        "a committed held branch can't be replayed exactly: its rows changed \
                         since it prepared; the redo rows stay for an operator");
                }
                Err(_) => r.unresolved += 1,
            },
            Decision::Abort => {
                if self
                    .admin
                    .delete_redo(cluster, &b.schema, b.dtid)
                    .await
                    .is_ok()
                {
                    r.dropped += 1;
                } else {
                    r.unresolved += 1;
                }
            }
            Decision::Pending => r.unresolved += 1,
        }
    }

    /// Sprig 2PC (R3): replays committed held branches whose redo rows are
    /// older than `txn.resolver_min_age_seconds`, and drops aborted ones.
    ///
    /// # Errors
    /// 39100 when the rows can't be listed.
    pub async fn scan_redo(&self, cluster: &ClusterId) -> Result<RedoReport> {
        let held = self.admin.redo_branches(cluster).await.map_err(|e| {
            SprigError::new(
                SprigCode::ClusterUnavailable,
                format!("redo rows on {cluster}: {e}"),
            )
        })?;
        let min_age = self.config().min_age.as_secs();
        let mut r = RedoReport {
            listed: held.len(),
            ..RedoReport::default()
        };
        for b in &held {
            if b.mark.is_none() && b.age_seconds < min_age {
                r.young += 1;
                continue;
            }
            self.finish_redo(cluster, b, &mut r).await;
        }
        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!("sprig_txn_held_branches", "cluster" => cluster.to_string())
            .set((r.young + r.unresolved + r.hazards) as f64);
        Ok(r)
    }

    /// Finishes every prepared coordinator branch of `tenant` on `cluster`
    /// now, whatever its age (the mover before a fence, DDL before a
    /// step). Branches still prepared at `deadline` are counted in
    /// `remaining`; the caller aborts its cutover and retries later.
    ///
    /// # Errors
    /// 39100 when `XA RECOVER` fails.
    pub async fn drain_prepared(
        &self,
        cluster: &ClusterId,
        tenant: TenantId,
        deadline: Instant,
    ) -> Result<DrainReport> {
        let mut rep = DrainReport::default();
        loop {
            // Held branches (Sprig 2PC): replayed or dropped, whatever age.
            if let Ok(held) = self.admin.redo_branches(cluster).await {
                let mut r = RedoReport::default();
                for b in held.iter().filter(|b| b.gtrid.tenant == tenant) {
                    self.finish_redo(cluster, b, &mut r).await;
                }
                rep.committed += r.replayed;
                rep.rolled_back += r.dropped;
            }
            let xids: Vec<Xid> = self
                .admin
                .xa_recover(cluster)
                .await
                .map_err(|e| {
                    SprigError::new(
                        SprigCode::ClusterUnavailable,
                        format!("XA RECOVER on {cluster}: {e}"),
                    )
                })?
                .into_iter()
                .filter(|x| x.gtrid.tenant == tenant)
                .collect();
            if xids.is_empty() {
                return Ok(rep);
            }
            for x in &xids {
                match self.finish(cluster, x).await {
                    Some(true) => rep.committed += 1,
                    Some(false) => rep.rolled_back += 1,
                    None => {}
                }
            }
            if Instant::now() >= deadline {
                let left = self
                    .admin
                    .xa_recover(cluster)
                    .await
                    .map(|l| l.iter().filter(|x| x.gtrid.tenant == tenant).count())
                    .unwrap_or(xids.len());
                rep.remaining = left;
                return Ok(rep);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Deletes decision rows on `cluster` that are no longer needed:
    /// commit and pending rows older than `min_age` whose prepared
    /// participants are all finished, abort rows past their retention.
    ///
    /// # Errors
    /// 39100 when the rows can't be listed.
    pub async fn sweep(&self, cluster: &ClusterId) -> Result<SweepReport> {
        let rows = self.admin.decision_rows(cluster).await.map_err(|e| {
            SprigError::new(
                SprigCode::ClusterUnavailable,
                format!("decision rows on {cluster}: {e}"),
            )
        })?;
        let cfg = self.config();
        let min_age = cfg.min_age.as_secs();
        let mut recovered: BTreeMap<ClusterId, Option<Vec<Xid>>> = BTreeMap::new();
        let mut redo: BTreeMap<ClusterId, Option<Vec<crate::xid::Dtid>>> = BTreeMap::new();
        let mut rep = SweepReport::default();
        for (schema, row) in rows {
            let age = row.age_seconds.unwrap_or(0);
            match row.outcome {
                // A commit row, or a home's pending pointer: needed only
                // while a prepared participant may still race for the
                // decision. Pending rows are never raced here: once the
                // last resource's commit row is gone, a race would record
                // a false abort.
                Decision::Commit | Decision::Pending if age >= min_age => {
                    let mut finished = true;
                    for p in row
                        .participants
                        .iter()
                        .filter(|p| p.kind == ParticipantKind::Xa)
                    {
                        if !recovered.contains_key(&p.cluster) {
                            let l = self.admin.xa_recover(&p.cluster).await.ok();
                            recovered.insert(p.cluster.clone(), l);
                        }
                        match recovered.get(&p.cluster) {
                            Some(Some(l)) if !l.iter().any(|x| x.gtrid.dtid == row.dtid) => {}
                            _ => finished = false,
                        }
                    }
                    // A held participant's redo rows may still be replayed:
                    // the commit row must outlive them.
                    for p in row
                        .participants
                        .iter()
                        .filter(|p| p.kind == ParticipantKind::Held)
                    {
                        if !redo.contains_key(&p.cluster) {
                            let l = self.admin.redo_branches(&p.cluster).await.ok().map(|v| {
                                v.into_iter()
                                    .filter(|b| b.statements > 0)
                                    .map(|b| b.dtid)
                                    .collect()
                            });
                            redo.insert(p.cluster.clone(), l);
                        }
                        match redo.get(&p.cluster) {
                            Some(Some(l)) if !l.contains(&row.dtid) => {}
                            _ => finished = false,
                        }
                    }
                    if finished
                        && self
                            .admin
                            .delete_decision(cluster, &schema, row.dtid)
                            .await
                            .is_ok()
                    {
                        if row.outcome == Decision::Pending {
                            rep.settled += 1;
                        } else {
                            rep.commit_deleted += 1;
                        }
                    }
                }
                Decision::Abort if age >= cfg.abort_retention.as_secs() => {
                    if self
                        .admin
                        .delete_decision(cluster, &schema, row.dtid)
                        .await
                        .is_ok()
                    {
                        rep.abort_deleted += 1;
                    }
                }
                _ => {}
            }
        }
        Ok(rep)
    }

    /// Scans and sweeps `clusters()` every `txn.resolver_interval_seconds`
    /// until cancelled.
    pub async fn run(&self, clusters: impl Fn() -> Vec<ClusterId>, cancel: CancellationToken) {
        loop {
            for c in clusters() {
                if let Err(e) = self.scan(&c).await {
                    tracing::warn!(cluster = %c, error = %e, "resolver scan failed");
                }
                if let Err(e) = self.scan_redo(&c).await {
                    tracing::warn!(cluster = %c, error = %e, "resolver redo scan failed");
                }
                if let Err(e) = self.sweep(&c).await {
                    tracing::warn!(cluster = %c, error = %e, "resolver sweep failed");
                }
            }
            tokio::select! {
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(self.config().interval) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_names_carry_the_tenant() {
        assert_eq!(tenant_of_schema("t42_d1_g3_s5"), Some(TenantId(42)));
        assert_eq!(tenant_of_schema("app"), None);
    }
}
