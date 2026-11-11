//! The global failover decision (chapter 25, "The global failover
//! decision"), run by the global leader.
//!
//! ```text
//! dc_failover(X), global leader, when dc-leases/X is deleted:
//! 1. create failover record F (state suspect); dcs/X.state = suspect
//! 2. wait failover.clock_drift_margin_ms; X took its lease back -> close F
//! 3. sites = active DCs and witnesses, other than X, that hold valid DC leases. If the
//!    global voters span fewer than failover.min_sites sites, or fewer than
//!    failover.min_sites - 1 sites are available: F = manual_only; alert
//! 4. ObserveDc(X) to every site (observe.rs)
//! 5. need at least failover.min_sites - 1 "down" verdicts and no "alive"; else retry
//!    until failover.decision_timeout_seconds, then manual_only and alert
//! 6. dcs/X.state = failed; F.state = promoting
//! 7. for each tenant T homed in X, at most failover.max_parallel_promotions at a time:
//!    skip (with the reason in F) if T's failover.mode is manual, if no standby DC holds
//!    every unit, if the standby DC's reported lag > T's failover.rpo_seconds, or if a
//!    cluster holding T is listed unsafe; else promote(T, standby DC)
//! 8. F.state = done; skipped tenants stay fenced in X and wait for X or an operator
//! ```
//!
//! [`GlobalFailover::tick`] also moves a `failed` DC that holds a lease
//! again (or a `suspect` one whose record ended `manual_only`) to
//! `returning` (chapter 25, "A returning server or DC stays
//! fenced"); the returning DC's own leader does the rest
//! ([`super::recovery::ReturningDc`]).
//!
//! **When a missing lease counts.** The leader acts on a DC whose lease it
//! saw and then saw vanish, or whose lease has been missing for three
//! lease TTLs since this leader started (a new leader during an outage,
//! or a DC that never took its lease); a freshly started leader thus never
//! fails over a DC whose controllers simply haven't renewed yet.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, UNIX_EPOCH};

use futures::StreamExt;
use serde_json::Value;
use sprig_common::ids::{DcId, TenantId};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigError};
use sprig_meta::bootstrap::{DcRecord, rfc3339};
use sprig_meta::dclease::{PERMIT_STATES, dc_key, lease_key};
use sprig_meta::{Cmp, Consistency, KeyValue, ListOpts, MetaStore, Op, Record, Tier};

use sprig_shard::home::read_home;

use super::decision::{self, Tally, TenantCandidate, TenantPlan};
use super::observe::{ObserveTargets, targets_key};
use super::promote::{Guard, Promoted, Promotion, promote};
use super::record::{FailoverRecord, FailoverState, TenantOutcome, Verdict, Vote, failover_key};
use super::settings::FailoverSettings;

/// Asks a site to observe a DC (`Observer.ObserveDc`).
#[async_trait::async_trait]
pub trait ObserverClient: Send + Sync {
    /// `site`'s verdict on `x`.
    async fn observe(&self, site: &DcId, x: &DcId, failover_id: &str) -> Result<Verdict>;
}

/// What a decision run ended with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Nothing to do: the DC isn't active, or its lease is back.
    NotNeeded,
    /// The record ended in this state.
    Ended(FailoverRecord),
}

/// The global leader's failover driver.
pub struct GlobalFailover {
    store: Arc<dyn MetaStore>,
    clock: Arc<dyn Clock>,
    settings: Arc<dyn Fn(&DcId) -> FailoverSettings + Send + Sync>,
    observers: Arc<dyn ObserverClient>,
    promotion: Promotion,
    seen: Mutex<HashMap<DcId, Seen>>,
    started: Instant,
    /// Two tiers: positions the target DCs' `tenant_promote` loops
    /// reported, by (failover, tenant), until the record takes them.
    reported: Mutex<HashMap<(String, u64), BTreeMap<String, String>>>,
    /// Chapter 52: promotions held for approval, by (failover, tenant),
    /// with when the gate was last asked.
    held: Mutex<HashMap<(String, u64), Instant>>,
    /// Chapter 52's approvals; the process's installed ones when unset.
    approvals: std::sync::OnceLock<Arc<crate::approvals::ControllerApprovals>>,
}

/// How often a held promotion asks chapter 52's gate again.
pub const HOLD_RECHECK: Duration = Duration::from_secs(10);

impl std::fmt::Debug for GlobalFailover {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlobalFailover").finish_non_exhaustive()
    }
}

type BoxFut<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// What the leader saw of one DC's lease since it started.
#[derive(Debug, Default)]
struct Seen {
    held_once: bool,
    missing_since: Option<Instant>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn unix_ms(clock: &dyn Clock) -> u64 {
    clock
        .wall()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

impl GlobalFailover {
    /// A driver over the global tier `store` (reads), writing through
    /// `promotion.guard`.
    #[must_use]
    pub fn new(
        store: Arc<dyn MetaStore>,
        clock: Arc<dyn Clock>,
        settings: Arc<dyn Fn(&DcId) -> FailoverSettings + Send + Sync>,
        observers: Arc<dyn ObserverClient>,
        promotion: Promotion,
    ) -> Self {
        let started = clock.now();
        Self {
            store,
            clock,
            settings,
            observers,
            promotion,
            seen: Mutex::new(HashMap::new()),
            started,
            reported: Mutex::new(HashMap::new()),
            held: Mutex::new(HashMap::new()),
            approvals: std::sync::OnceLock::new(),
        }
    }

    /// Holds promotions through `a` (chapter 52) instead of the process's
    /// installed approvals.
    #[must_use]
    pub fn with_approvals(self, a: Arc<crate::approvals::ControllerApprovals>) -> Self {
        let _ = self.approvals.set(a);
        self
    }

    fn approvals_hub(&self) -> Option<&crate::approvals::ControllerApprovals> {
        self.approvals
            .get()
            .or_else(|| crate::approvals::installed())
            .map(Arc::as_ref)
    }

    /// `Global.PromotionDone` (two tiers): the target DC finished steps
    /// 2-4 of `tenant`'s promotion at `epoch`; step 5 (the entry becomes
    /// `active`, `auth_version + 1`) is written here, under the leader's
    /// token, and the record takes `applied` at its next pass. Returns
    /// `false` when the directory moved on.
    ///
    /// # Errors
    /// 39504 when leadership was lost; the store's errors.
    pub async fn promotion_done(
        &self,
        failover_id: &str,
        tenant: TenantId,
        to: &DcId,
        epoch: u64,
        applied: BTreeMap<String, String>,
    ) -> Result<bool> {
        let ok =
            super::promote::activate(self.guard(), &*self.store, tenant, to, epoch, None).await?;
        if ok {
            lock(&self.reported).insert((failover_id.to_owned(), tenant.0), applied);
            tracing::info!(%tenant, %to, epoch, failover = failover_id, "promotion finished in the target DC; tenant active");
        }
        Ok(ok)
    }

    fn guard(&self) -> &Guard {
        &self.promotion.guard
    }

    async fn dcs(&self) -> Result<Vec<(Record<DcRecord>, u64)>> {
        let page = self
            .store
            .list(&Tier::Global.key("dcs/")?, ListOpts::default())
            .await?;
        page.kvs
            .iter()
            .map(|kv| Ok((Record::<DcRecord>::decode(&kv.value)?, kv.mod_revision)))
            .collect()
    }

    async fn leases(&self) -> Result<BTreeSet<DcId>> {
        let page = self
            .store
            .list(&Tier::Global.key("dc-leases/")?, ListOpts::default())
            .await?;
        Ok(page
            .kvs
            .iter()
            .filter_map(|kv| {
                kv.key
                    .as_str()
                    .rsplit('/')
                    .next()
                    .and_then(|s| DcId::new(s).ok())
            })
            .collect())
    }

    async fn has_lease(&self, dc: &DcId) -> Result<bool> {
        Ok(self
            .store
            .get(&lease_key(dc)?, Consistency::Linearizable)
            .await?
            .is_some())
    }

    /// One pass: DCs whose lease vanished get a decision run (returned in
    /// order); failed DCs holding a lease again become `returning`.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn tick(&self) -> Result<Vec<Outcome>> {
        let now = self.clock.now();
        let dcs = self.dcs().await?;
        let leases = self.leases().await?;
        let open: BTreeSet<DcId> = self
            .open_records()
            .await?
            .into_iter()
            .map(|r| r.dc_id)
            .collect();
        let mut lost = Vec::new();
        for (rec, rev) in &dcs {
            let dc = &rec.body.dc_id;
            let held = leases.contains(dc);
            let ttl = (self.settings)(dc).dc_lease;
            let state = rec.body.state.as_str();
            // A failed DC, or one left suspect by a manual-only decision,
            // that holds a lease again returns through `returning`, so it
            // fences any tenant an operator moved meanwhile.
            if held && (state == "failed" || (state == "suspect" && !open.contains(dc))) {
                self.set_state(rec.clone(), *rev, "returning").await?;
                tracing::info!(%dc, was = state, "the DC holds a lease again: returning");
                continue;
            }
            let mut seen = lock(&self.seen);
            let s = seen.entry(dc.clone()).or_default();
            if held {
                s.held_once = true;
                s.missing_since = None;
                continue;
            }
            if rec.body.kind == "witness" || !matches!(state, "active" | "draining") {
                continue;
            }
            s.missing_since.get_or_insert(now);
            let long_enough = now.saturating_duration_since(self.started) >= ttl * 3;
            if s.held_once || long_enough {
                lost.push(dc.clone());
            }
        }
        let mut out = Vec::new();
        for dc in lost {
            out.push(self.dc_failover(&dc).await?);
        }
        // Resume any record a previous leader left open.
        for rec in self.open_records().await? {
            if !out
                .iter()
                .any(|o| matches!(o, Outcome::Ended(f) if f.failover_id == rec.failover_id))
            {
                out.push(self.resume(rec).await?);
            }
        }
        Ok(out)
    }

    async fn open_records(&self) -> Result<Vec<FailoverRecord>> {
        let page = self
            .store
            .list(&Tier::Global.key("failovers/")?, ListOpts::default())
            .await?;
        Ok(page
            .kvs
            .iter()
            .filter_map(|kv| Record::<FailoverRecord>::decode(&kv.value).ok())
            .map(|r| r.body)
            .filter(|r| {
                matches!(
                    r.state,
                    FailoverState::Suspect | FailoverState::Observing | FailoverState::Promoting
                )
            })
            .collect())
    }

    async fn set_state(&self, mut rec: Record<DcRecord>, rev: u64, state: &str) -> Result<bool> {
        let key = dc_key(&rec.body.dc_id)?;
        state.clone_into(&mut rec.body.state);
        self.guard()
            .txn(
                vec![Cmp::ModRev(key.clone(), rev)],
                vec![Op::Put {
                    key,
                    value: rec.encode()?,
                    lease: None,
                }],
            )
            .await
    }

    async fn write_record(&self, f: &FailoverRecord) -> Result<()> {
        let key = failover_key(&f.failover_id)?;
        let ok = self
            .guard()
            .txn(
                vec![],
                vec![Op::Put {
                    key,
                    value: Record::new(f.clone()).encode()?,
                    lease: None,
                }],
            )
            .await?;
        if ok {
            Ok(())
        } else {
            Err(SprigError::internal("failover record write refused"))
        }
    }

    async fn read_dc(&self, dc: &DcId) -> Result<Option<(Record<DcRecord>, u64)>> {
        match self
            .store
            .get(&dc_key(dc)?, Consistency::Linearizable)
            .await?
        {
            Some(kv) => Ok(Some((
                Record::<DcRecord>::decode(&kv.value)?,
                kv.mod_revision,
            ))),
            None => Ok(None),
        }
    }

    /// Steps 1-8 for DC `x`.
    ///
    /// # Errors
    /// The store's errors; 39504 when leadership was lost (a new leader
    /// resumes the record).
    pub async fn dc_failover(&self, x: &DcId) -> Result<Outcome> {
        // 1. The record and `suspect`, only while the lease is still gone.
        let Some((mut rec, rev)) = self.read_dc(x).await? else {
            return Ok(Outcome::NotNeeded);
        };
        if !matches!(rec.body.state.as_str(), "active" | "draining") {
            return Ok(Outcome::NotNeeded);
        }
        let id = format!("fo-{}-{x}", unix_ms(&*self.clock));
        let f = FailoverRecord {
            failover_id: id.clone(),
            dc_id: x.clone(),
            state: FailoverState::Suspect,
            lease_lost_at: rfc3339(self.clock.wall()),
            votes: Vec::new(),
            tenants: Vec::new(),
            reason: None,
        };
        "suspect".clone_into(&mut rec.body.state);
        let dkey = dc_key(x)?;
        let ok = self
            .guard()
            .txn(
                vec![
                    Cmp::ModRev(dkey.clone(), rev),
                    Cmp::Missing(lease_key(x)?),
                    Cmp::Missing(failover_key(&id)?),
                ],
                vec![
                    Op::Put {
                        key: failover_key(&id)?,
                        value: Record::new(f.clone()).encode()?,
                        lease: None,
                    },
                    Op::Put {
                        key: dkey,
                        value: rec.encode()?,
                        lease: None,
                    },
                ],
            )
            .await?;
        if !ok {
            return Ok(Outcome::NotNeeded);
        }
        tracing::warn!(dc = %x, failover = %id, "DcLeaseLost: DC lease gone; DC suspect");
        metrics::counter!("sprig_controller_dc_failovers_total", "result" => "started")
            .increment(1);
        self.resume(f).await
    }

    /// Continues an open record from its state.
    #[allow(clippy::too_many_lines, clippy::many_single_char_names)] // steps 2-8 in order
    ///
    /// # Errors
    /// As [`Self::dc_failover`].
    pub async fn resume(&self, mut f: FailoverRecord) -> Result<Outcome> {
        let x = f.dc_id.clone();
        let s = (self.settings)(&x);
        if f.state == FailoverState::Suspect {
            // 2. The drift margin.
            tokio::time::sleep(s.clock_drift_margin).await;
            if self.has_lease(&x).await? {
                return self
                    .close(f, "the DC took its lease back within the drift margin")
                    .await;
            }
            // 3. Sites.
            if !s.self_fence {
                return self
                    .manual_only(f, "failover.self_fence is false for the DC")
                    .await;
            }
            let sites = self.sites(&x).await?;
            let voters = self.voter_sites().await?;
            if let Err(why) = decision::sites_check(voters, sites.len(), s.min_sites) {
                return self.manual_only(f, &why).await;
            }
            f.state = FailoverState::Observing;
            self.write_record(&f).await?;
        }
        if f.state == FailoverState::Observing {
            // 4-5. Observe until enough sites agree.
            let deadline = self.clock.now() + s.decision_timeout;
            let verdicts = loop {
                let sites = self.sites(&x).await?;
                let verdicts = self.observe_all(&sites, &x, &f.failover_id, &s).await;
                f.votes = verdicts
                    .iter()
                    .filter(|v| v.observer_lease_valid)
                    .map(|v| Vote {
                        dc: v.observer_dc.clone(),
                        verdict: v.verdict,
                        unsafe_writers: v.unsafe_writers.clone(),
                    })
                    .collect();
                self.write_record(&f).await?;
                if self.has_lease(&x).await? {
                    return self
                        .close(f, "the DC took its lease back while observed")
                        .await;
                }
                match decision::tally(&verdicts, s.min_sites) {
                    Tally::FailOver => break verdicts,
                    Tally::Retry(why) => {
                        tracing::info!(dc = %x, why, "failover decision: not yet");
                        if why.contains("alive") {
                            tracing::warn!(dc = %x, "DcSplitView");
                        }
                    }
                }
                if self.clock.now() >= deadline {
                    return self
                        .manual_only(f, "no agreement before failover.decision_timeout_seconds")
                        .await;
                }
                tokio::time::sleep(s.observer_probe_interval).await;
            };
            // 6. Failed.
            if let Some((rec, rev)) = self.read_dc(&x).await?
                && rec.body.state == "suspect"
            {
                self.set_state(rec, rev, "failed").await?;
            }
            f.state = FailoverState::Promoting;
            let lags = decision::lags(&verdicts);
            let unsafe_w = decision::unsafe_writers(&verdicts);
            f.tenants = self.plan(&x, &s, &lags, &unsafe_w).await?;
            self.write_record(&f).await?;
            tracing::warn!(dc = %x, failover = %f.failover_id, "DC failed; promoting its tenants");
        }
        if f.state == FailoverState::Promoting {
            let before = f.clone();
            // Two tiers: committed promotions the target DC finished.
            for t in f.tenants.iter_mut().filter(|t| t.outcome == "committed") {
                let Some(to) = t.to.clone() else { continue };
                let Some(e) = read_home(&*self.store, TenantId(t.tenant_id)).await? else {
                    continue;
                };
                if e.home_dc != to || Some(e.epoch) != t.epoch {
                    t.outcome = "skipped".into();
                    t.reason = Some(format!(
                        "the directory moved on: home {} epoch {}",
                        e.home_dc, e.epoch
                    ));
                } else if e.state == "active" {
                    t.outcome = "promoted".into();
                    if let Some(a) =
                        lock(&self.reported).remove(&(f.failover_id.clone(), t.tenant_id))
                    {
                        t.applied = a;
                    }
                }
            }
            // 7. Promotions, bounded.
            let todo: Vec<usize> = f
                .tenants
                .iter()
                .enumerate()
                .filter(|(_, t)| t.outcome == "planned")
                .map(|(i, _)| i)
                .collect();
            // Chapter 52: under a global `failover.mode = approve` each
            // promotion waits for an approval, and an approved one goes
            // ahead only if the checks still allow it now.
            let (todo, tickets) = self.approvals(&mut f, &x, &s, todo).await?;
            let par = s.max_parallel_promotions.max(1) as usize;
            let mut jobs: Vec<BoxFut<'_, (usize, Result<Promoted>)>> = Vec::new();
            for &i in &todo {
                let t = f.tenants[i].clone();
                let x = x.clone();
                let id = f.failover_id.clone();
                let p = &self.promotion;
                jobs.push(Box::pin(async move {
                    let to = t.to.clone().unwrap_or_else(|| x.clone());
                    let r =
                        promote(p, &id, TenantId(t.tenant_id), &x, &to, t.epoch.unwrap_or(0)).await;
                    (i, r)
                }));
            }
            let results: Vec<(usize, Result<Promoted>)> = futures::stream::iter(jobs)
                .buffer_unordered(par)
                .collect()
                .await;
            for (i, r) in results {
                let ticket = tickets.get(&i);
                match &r {
                    Ok(Promoted::Done { .. }) => {
                        crate::approvals::dc_failover_done(
                            self.approvals_hub(),
                            ticket,
                            true,
                            "promoted",
                        )
                        .await;
                    }
                    Ok(Promoted::Committed { .. }) => {
                        crate::approvals::dc_failover_done(
                            self.approvals_hub(),
                            ticket,
                            true,
                            "committed; the target DC finishes the promotion",
                        )
                        .await;
                    }
                    Ok(Promoted::Skipped(why)) => {
                        crate::approvals::dc_failover_done(
                            self.approvals_hub(),
                            ticket,
                            false,
                            why,
                        )
                        .await;
                    }
                    Err(_) => {}
                }
                let t = &mut f.tenants[i];
                match r {
                    Ok(Promoted::Done { epoch, applied }) => {
                        t.outcome = "promoted".into();
                        t.epoch = Some(epoch);
                        t.applied = applied;
                        t.reason = None;
                    }
                    Ok(Promoted::Skipped(why)) => {
                        t.outcome = "skipped".into();
                        t.reason = Some(why);
                    }
                    Ok(Promoted::Committed { epoch }) => {
                        // Two tiers: the target DC's tenant_promote loop
                        // finishes it (r2next).
                        t.outcome = "committed".into();
                        t.epoch = Some(epoch);
                        t.reason = None;
                    }
                    Err(e) if e.code() == Some(sprig_common::SprigCode::NotLeader) => {
                        return Err(e);
                    }
                    Err(e) => {
                        // Left planned: the next pass retries it.
                        tracing::warn!(tenant = t.tenant_id, error = %e, "promotion failed; retrying later");
                    }
                }
            }
            if f.tenants
                .iter()
                .all(|t| t.outcome != "planned" && t.outcome != "committed")
            {
                f.state = FailoverState::Done;
                metrics::counter!("sprig_controller_dc_failovers_total", "result" => "done")
                    .increment(1);
            }
            // A pass that changed nothing (promotions held for approval)
            // writes nothing.
            if f != before {
                self.write_record(&f).await?;
            }
        }
        Ok(Outcome::Ended(f))
    }

    /// Chapter 52's gate over the planned promotions `todo`: those to run
    /// now, with the ticket of each one an approval released. A held
    /// tenant stays `planned` with the item in its reason, and asks again
    /// every [`HOLD_RECHECK`]. An approved one runs only if the lost DC's
    /// lease is still gone, the sites still agree it is down, and a fresh
    /// plan still promotes the tenant (its standby's lag within its RPO,
    /// its writers safe); otherwise the item fails, or the tenant is
    /// skipped when the DC came back.
    #[allow(clippy::many_single_char_names)] // f, x, s as in resume
    async fn approvals(
        &self,
        f: &mut FailoverRecord,
        x: &DcId,
        s: &FailoverSettings,
        todo: Vec<usize>,
    ) -> Result<(Vec<usize>, HashMap<usize, sprig_approvals::machine::Ticket>)> {
        use crate::approvals::{DcFailoverGate, dc_failover_done, dc_failover_gate};
        let mut go = Vec::new();
        let mut tickets = HashMap::new();
        let mut approved = Vec::new();
        for i in todo {
            let t = &f.tenants[i];
            let key = (f.failover_id.clone(), t.tenant_id);
            let now = self.clock.now();
            if lock(&self.held)
                .get(&key)
                .is_some_and(|at| now.saturating_duration_since(*at) < HOLD_RECHECK)
            {
                continue;
            }
            let to = t.to.clone().unwrap_or_else(|| x.clone());
            let gate = dc_failover_gate(
                self.approvals_hub(),
                x,
                TenantId(t.tenant_id),
                &to,
                &f.failover_id,
                t.lag_ms,
            )
            .await;
            match gate {
                DcFailoverGate::Go(None) => go.push(i),
                DcFailoverGate::Go(Some(ticket)) => {
                    lock(&self.held).remove(&key);
                    approved.push((i, ticket));
                }
                DcFailoverGate::Hold(why) => {
                    if f.tenants[i].reason.as_deref() != Some(why.as_str()) {
                        tracing::warn!(tenant = t.tenant_id, dc = %x, why, "promotion held (chapter 52)");
                    }
                    lock(&self.held).insert(key, now);
                    f.tenants[i].reason = Some(why);
                }
            }
        }
        if approved.is_empty() {
            return Ok((go, tickets));
        }
        if self.has_lease(x).await? {
            let why = format!("DC {x} took its lease back while the promotion was held");
            for (i, ticket) in approved {
                f.tenants[i].outcome = "skipped".into();
                f.tenants[i].reason = Some(why.clone());
                dc_failover_done(self.approvals_hub(), Some(&ticket), false, &why).await;
            }
            return Ok((go, tickets));
        }
        let sites = self.sites(x).await?;
        let verdicts = self.observe_all(&sites, x, &f.failover_id, s).await;
        if let Tally::Retry(why) = decision::tally(&verdicts, s.min_sites) {
            let why = format!("the failover checks no longer allow it: {why}");
            for (i, ticket) in approved {
                f.tenants[i].reason = Some(why.clone());
                dc_failover_done(self.approvals_hub(), Some(&ticket), false, &why).await;
            }
            return Ok((go, tickets));
        }
        let lags = decision::lags(&verdicts);
        let unsafe_w = decision::unsafe_writers(&verdicts);
        let fresh = self.plan(x, s, &lags, &unsafe_w).await?;
        for (i, ticket) in approved {
            let id = f.tenants[i].tenant_id;
            match fresh.iter().find(|o| o.tenant_id == id) {
                Some(o) if o.outcome == "planned" => {
                    let t = &mut f.tenants[i];
                    t.to.clone_from(&o.to);
                    t.lag_ms = o.lag_ms;
                    t.epoch = o.epoch;
                    t.reason = None;
                    tracing::warn!(tenant = id, dc = %x, to = ?t.to, "approved promotion: the checks still allow it");
                    go.push(i);
                    tickets.insert(i, ticket);
                }
                other => {
                    let why = other
                        .and_then(|o| o.reason.clone())
                        .unwrap_or_else(|| format!("the tenant is no longer homed in {x}"));
                    f.tenants[i].outcome = "skipped".into();
                    f.tenants[i].reason = Some(why.clone());
                    dc_failover_done(self.approvals_hub(), Some(&ticket), false, &why).await;
                }
            }
        }
        Ok((go, tickets))
    }

    async fn close(&self, mut f: FailoverRecord, why: &str) -> Result<Outcome> {
        if let Some((rec, rev)) = self.read_dc(&f.dc_id).await?
            && rec.body.state == "suspect"
        {
            self.set_state(rec, rev, "active").await?;
        }
        f.state = FailoverState::Closed;
        f.reason = Some(why.to_owned());
        self.write_record(&f).await?;
        metrics::counter!("sprig_controller_dc_failovers_total", "result" => "closed").increment(1);
        Ok(Outcome::Ended(f))
    }

    async fn manual_only(&self, mut f: FailoverRecord, why: &str) -> Result<Outcome> {
        tracing::warn!(dc = %f.dc_id, why, "FailoverManualOnly");
        f.state = FailoverState::ManualOnly;
        f.reason = Some(why.to_owned());
        self.write_record(&f).await?;
        metrics::counter!("sprig_controller_dc_failovers_total", "result" => "manual_only")
            .increment(1);
        Ok(Outcome::Ended(f))
    }

    /// Active DCs and witnesses other than `x` that hold DC leases.
    async fn sites(&self, x: &DcId) -> Result<Vec<DcId>> {
        let leases = self.leases().await?;
        Ok(self
            .dcs()
            .await?
            .into_iter()
            .map(|(r, _)| r.body)
            .filter(|d| {
                &d.dc_id != x
                    && PERMIT_STATES.contains(&d.state.as_str())
                    && leases.contains(&d.dc_id)
            })
            .map(|d| d.dc_id)
            .collect())
    }

    async fn voter_sites(&self) -> Result<usize> {
        Ok(self
            .dcs()
            .await?
            .iter()
            .filter(|(r, _)| r.body.global_voter)
            .count())
    }

    async fn observe_all(
        &self,
        sites: &[DcId],
        x: &DcId,
        id: &str,
        s: &FailoverSettings,
    ) -> Vec<Verdict> {
        let limit = s.observer_probe_interval * s.observer_probes + Duration::from_secs(5);
        let mut asks: Vec<BoxFut<'_, Option<Verdict>>> = Vec::new();
        for site in sites {
            let obs = Arc::clone(&self.observers);
            let (site, x, id) = (site.clone(), x.clone(), id.to_owned());
            asks.push(Box::pin(async move {
                match tokio::time::timeout(limit, obs.observe(&site, &x, &id)).await {
                    Ok(Ok(v)) => Some(v),
                    Ok(Err(e)) => {
                        tracing::info!(%site, error = %e, "observer did not answer");
                        None
                    }
                    Err(_) => None,
                }
            }));
        }
        futures::future::join_all(asks)
            .await
            .into_iter()
            .flatten()
            .collect()
    }

    /// Step 7's plan for every tenant homed in `x`.
    async fn plan(
        &self,
        x: &DcId,
        s: &FailoverSettings,
        lags: &BTreeMap<(DcId, u64), u32>,
        unsafe_w: &BTreeSet<String>,
    ) -> Result<Vec<TenantOutcome>> {
        let writers: Vec<String> = match self
            .store
            .get(&targets_key(x)?, Consistency::Linearizable)
            .await?
        {
            Some(kv) => Record::<ObserveTargets>::decode(&kv.value)?
                .body
                .writers
                .iter()
                .map(super::observe::WriterTarget::label)
                .collect(),
            None => Vec::new(),
        };
        let mut out = Vec::new();
        for c in self.tenants_in(x, s, &writers).await? {
            let mut o = TenantOutcome {
                tenant_id: c.tenant_id,
                outcome: "planned".into(),
                to: None,
                lag_ms: None,
                reason: None,
                epoch: Some(c.epoch),
                applied: BTreeMap::new(),
                errant: None,
            };
            match decision::plan_tenant(&c, lags, unsafe_w) {
                TenantPlan::Promote { to, lag_ms } => {
                    o.to = Some(to);
                    o.lag_ms = Some(lag_ms);
                }
                TenantPlan::Skip(why) => {
                    tracing::warn!(tenant = c.tenant_id, why, "tenant stays fenced");
                    o.outcome = "skipped".into();
                    o.reason = Some(why);
                }
            }
            out.push(o);
        }
        Ok(out)
    }

    /// Directory entries homed in `x`, with the tenant settings the
    /// directory copies (`standby_dcs`, `failover {mode, rpo_seconds}`).
    async fn tenants_in(
        &self,
        x: &DcId,
        s: &FailoverSettings,
        writers: &[String],
    ) -> Result<Vec<TenantCandidate>> {
        let prefix = Tier::Global.key("tenants/")?;
        let mut after = None;
        let mut out = Vec::new();
        loop {
            let page = self
                .store
                .list(
                    &prefix,
                    ListOpts {
                        after: after.clone(),
                        ..ListOpts::default()
                    },
                )
                .await?;
            for kv in &page.kvs {
                if let Some(c) = candidate(kv, x, s, writers) {
                    out.push(c);
                }
            }
            if !page.more {
                break;
            }
            after = page.kvs.last().map(|kv| kv.key.clone());
        }
        Ok(out)
    }
}

fn candidate(
    kv: &KeyValue,
    x: &DcId,
    s: &FailoverSettings,
    writers: &[String],
) -> Option<TenantCandidate> {
    let raw: serde_json::Map<String, Value> = serde_json::from_slice(&kv.value).ok()?;
    if raw.get("home_dc").and_then(Value::as_str) != Some(x.as_str()) {
        return None;
    }
    if !matches!(
        raw.get("state").and_then(Value::as_str),
        Some("active" | "migrating")
    ) {
        return None;
    }
    let tenant_id = kv.key.as_str().rsplit('/').next()?.parse().ok()?;
    let fo = raw.get("failover");
    let mode = fo.and_then(|f| f.get("mode")).and_then(Value::as_str);
    let rpo = fo
        .and_then(|f| f.get("rpo_seconds"))
        .and_then(Value::as_u64)
        .map_or(s.rpo, Duration::from_secs);
    let standby_dcs = raw
        .get("standby_dcs")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .filter_map(|d| DcId::new(d).ok())
                .collect()
        })
        .unwrap_or_default();
    Some(TenantCandidate {
        tenant_id,
        epoch: raw.get("epoch").and_then(Value::as_u64).unwrap_or(0),
        manual: s.manual || mode == Some("manual"),
        rpo,
        standby_dcs,
        writers: writers.to_vec(),
        pinned: raw.get("pinned_dc").and_then(Value::as_str) == Some(x.as_str()),
        allowed_dcs: sprig_shard::home::allowed_dcs(&raw),
    })
}

#[cfg(test)]
mod tests;
