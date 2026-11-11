//! DC leases (chapter 25, "DC leases, sub-leases and self-fencing";
//! chapter 23's `dc-leases/<dc_id>`).
//!
//! Every controller of DC X runs one [`DcLease`]:
//!
//! ```text
//! 1. the leader creates the etcd lease (TTL failover.dc_lease_seconds) and puts
//!    dc-leases/X = {lease_id, dc_epoch + 1}; dcs/X.dc_epoch + 1 in the same txn
//! 2. every controller sends keep-alives every TTL/4 and keeps its own deadline D,
//!    using chapter 23's rule with margin = failover.fence_lead_ms
//! 3. Permits.Renew: a controller with a valid D replies valid_ms = min(TTL/2, D - now)
//! ```
//!
//! `D - now` above is measured to `D - margin`, as for every session: a
//! holder's permit ends `fence_lead` before etcd could expire the lease.
//!
//! **Where it differs from the book.** The book lets X take a new DC lease
//! only while `dcs/X.state` is `active` or `joining`, and a returning DC
//! only after it fenced its moved tenants. Here a DC may take its lease in
//! any state but `retired` (a DC that lost its lease takes it back as soon
//! as it can, which is how the global leader learns it returned), and
//! **permits** are what the states gate: only `active` and `joining` get
//! valid permits (and `draining`, which still serves its tenants until
//! migration empties it). Permits are the only write authority, so the safety is
//! the same, and the global leader sees a returning DC without a separate
//! signal.
//!
//! Time is the injected [`Clock`]; waits use tokio's timer, so the
//! simulation drives it with paused time. [`DcLease::run`] is a plain
//! future: run it inside the process's own task so a paused process stops
//! renewing (no background task is spawned).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sprig_common::ids::DcId;
use sprig_common::time::Clock;
use sprig_common::{Result, SprigError};

use crate::bootstrap::DcRecord;
use crate::permit::Permit;
use crate::{Cmp, Consistency, Key, LeaseId, MetaStore, Op, Record, Tier, Txn, Versioned};

/// A failed round of [`DcLease::run`] is retried this soon.
pub const RETRY_AFTER: Duration = Duration::from_millis(250);

/// The shortest time a keep-alive is given, even right at the deadline.
pub const MIN_RENEW_BUDGET: Duration = Duration::from_millis(200);

/// DC states in which a DC may hold valid permits: `draining` still
/// serves the tenants that migration hasn't moved yet.
pub const PERMIT_STATES: [&str; 3] = ["active", "joining", "draining"];

/// `dc-leases/<dc>` in the global tier.
///
/// # Errors
/// Internal never in practice.
pub fn lease_key(dc: &DcId) -> Result<Key> {
    Tier::Global.key(&format!("dc-leases/{dc}"))
}

/// `dcs/<dc>` in the global tier.
///
/// # Errors
/// Internal never in practice.
pub fn dc_key(dc: &DcId) -> Result<Key> {
    Tier::Global.key(&format!("dcs/{dc}"))
}

/// The value of `dc-leases/<dc>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DcLeaseRecord {
    /// The etcd lease every controller of the DC renews.
    pub lease_id: i64,
    /// The DC's epoch under this lease.
    pub dc_epoch: u64,
    /// The controller that took it.
    #[serde(default)]
    pub holder: String,
}

impl Versioned for DcLeaseRecord {
    const V: u32 = 1;
}

/// How a DC lease is kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DcLeaseOpts {
    /// `failover.dc_lease_seconds`.
    pub ttl: Duration,
    /// `failover.fence_lead_ms`: permits end this long before the lease
    /// could expire.
    pub fence_lead: Duration,
    /// Keep-alive period, `ttl / 4`.
    pub period: Duration,
    /// `failover.self_fence`: false keeps permits valid whatever the lease
    /// says (and disables automatic DC failover for the DC).
    pub self_fence: bool,
}

impl DcLeaseOpts {
    /// `ttl` and `fence_lead`, keep-alives every `ttl / 4`, self-fencing on.
    #[must_use]
    pub fn new(ttl: Duration, fence_lead: Duration) -> Self {
        Self {
            ttl,
            fence_lead,
            period: ttl / 4,
            self_fence: true,
        }
    }
}

impl Default for DcLeaseOpts {
    /// The book's defaults: 10 s TTL, 3 s fence lead.
    fn default() -> Self {
        Self::new(Duration::from_secs(10), Duration::from_millis(3000))
    }
}

#[derive(Debug, Default)]
struct Inner {
    /// The lease this controller renews, with the DC epoch it carries.
    lease: Option<(LeaseId, u64)>,
    /// When etcd could expire the lease at the earliest, judged locally.
    deadline: Option<Instant>,
    /// `dcs/<dc>.state` as last read.
    record_state: String,
}

/// One controller's view of its DC's lease (see the module docs).
pub struct DcLease {
    dc: DcId,
    holder: String,
    store: Arc<dyn MetaStore>,
    clock: Arc<dyn Clock>,
    opts: DcLeaseOpts,
    state: Mutex<Inner>,
    ticks: AtomicU64,
    /// A round has finished without an error since this keeper started
    /// (hardening round 2): until then the controller's permit answer would
    /// be a refusal it hasn't checked.
    settled: std::sync::atomic::AtomicBool,
}

impl std::fmt::Debug for DcLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DcLease")
            .field("dc", &self.dc)
            .field("holder", &self.holder)
            .finish_non_exhaustive()
    }
}

impl DcLease {
    /// A lease keeper for controller `holder` of `dc`, over the global
    /// tier `store`.
    #[must_use]
    pub fn new(
        dc: DcId,
        holder: impl Into<String>,
        store: Arc<dyn MetaStore>,
        clock: Arc<dyn Clock>,
        opts: DcLeaseOpts,
    ) -> Self {
        Self {
            dc,
            holder: holder.into(),
            store,
            clock,
            opts,
            state: Mutex::new(Inner::default()),
            ticks: AtomicU64::new(0),
            settled: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// The DC.
    #[must_use]
    pub fn dc(&self) -> &DcId {
        &self.dc
    }

    /// The options.
    #[must_use]
    pub fn opts(&self) -> DcLeaseOpts {
        self.opts
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Rounds run so far (a watchdog compares it).
    #[must_use]
    pub fn ticks(&self) -> u64 {
        self.ticks.load(Ordering::SeqCst)
    }

    /// Whether a round has finished without an error since this keeper
    /// started. Until then [`Self::permit`] would refuse only because this
    /// controller hasn't read the lease yet, so the permit service answers
    /// "unavailable" instead and holders ask another controller (a
    /// restarted controller used to fence its DC's gateways for a moment;
    /// hardening round 2).
    #[must_use]
    pub fn settled(&self) -> bool {
        self.settled.load(Ordering::SeqCst)
    }

    /// Before this instant the DC lease certainly exists: the local
    /// deadline minus the fence lead. `None` without a lease.
    #[must_use]
    pub fn valid_until(&self) -> Option<Instant> {
        let s = self.lock();
        s.deadline.and_then(|d| d.checked_sub(self.opts.fence_lead))
    }

    /// Whether the DC lease is certainly alive now (see
    /// [`Self::valid_until`]); an observer's verdict counts only then.
    #[must_use]
    pub fn lease_valid(&self) -> bool {
        let now = self.clock.now();
        self.valid_until().is_some_and(|u| now < u)
    }

    /// The DC epoch of the lease held, if any.
    #[must_use]
    pub fn dc_epoch(&self) -> Option<u64> {
        self.lock().lease.map(|(_, e)| e)
    }

    /// `dcs/<dc>.state` as last read.
    #[must_use]
    pub fn dc_state(&self) -> String {
        self.lock().record_state.clone()
    }

    /// The permit this controller grants now (chapter 25 step 3).
    #[must_use]
    pub fn permit(&self) -> Permit {
        let now = self.clock.now();
        let s = self.lock();
        let epoch = s.lease.map_or(0, |(_, e)| e);
        let half = self.opts.ttl / 2;
        if !self.opts.self_fence {
            return Permit::valid(epoch, half);
        }
        if !PERMIT_STATES.contains(&s.record_state.as_str()) {
            return Permit::fenced(epoch);
        }
        let Some(until) = s.deadline.and_then(|d| d.checked_sub(self.opts.fence_lead)) else {
            return Permit::fenced(epoch);
        };
        if now >= until {
            return Permit::fenced(epoch);
        }
        Permit::valid(epoch, (until - now).min(half))
    }

    /// One round (see the module docs): renew the lease held; without one,
    /// adopt the DC's lease or, when `may_take` (this controller leads its
    /// DC), take a new one.
    ///
    /// # Errors
    /// The store's errors; the deadline is then left as it was, so the
    /// permits end on time.
    pub async fn tick(&self, may_take: bool) -> Result<()> {
        self.ticks.fetch_add(1, Ordering::SeqCst);
        let r = self.round(may_take).await;
        if r.is_ok() {
            self.settled.store(true, Ordering::SeqCst);
        }
        // A deadline that passed ends the lease locally whatever etcd says.
        let now = self.clock.now();
        let mut s = self.lock();
        if s.deadline.is_some_and(|d| now >= d) {
            tracing::warn!(dc = %self.dc, holder = %self.holder, "DC lease deadline passed");
            s.deadline = None;
            s.lease = None;
        }
        r
    }

    async fn round(&self, may_take: bool) -> Result<()> {
        // The lease first: its renewal never waits behind another read
        // (r2next: a slow linearizable read of the DC record used to hold
        // up the keep-alive until the permits lapsed).
        let held = self.lock().lease;
        let renewed = match held {
            Some((lease, _)) => Some(self.renew(lease).await),
            None => None,
        };
        // The DC record: its state gates permits. A failed read keeps the
        // last state; it can't extend anything.
        let dckey = dc_key(&self.dc)?;
        let rec = match self.store.get(&dckey, Consistency::Linearizable).await {
            Ok(Some(kv)) => Some((Record::<DcRecord>::decode(&kv.value)?, kv.mod_revision)),
            Ok(None) => None,
            Err(e) => return renewed.unwrap_or(Err(e)),
        };
        let state = rec
            .as_ref()
            .map(|(r, _)| r.body.state.clone())
            .unwrap_or_default();
        self.lock().record_state.clone_from(&state);
        if let Some(r) = renewed {
            return r;
        }
        // No lease: adopt the DC's, if there is one.
        let lkey = lease_key(&self.dc)?;
        if let Some(kv) = self.store.get(&lkey, Consistency::Linearizable).await? {
            let r = Record::<DcLeaseRecord>::decode(&kv.value)?;
            let lease = LeaseId(r.body.lease_id);
            self.lock().lease = Some((lease, r.body.dc_epoch));
            return self.renew(lease).await;
        }
        let Some((rec, rev)) = rec else {
            return Ok(());
        };
        if !may_take || state == "retired" {
            return Ok(());
        }
        self.take(rec, rev, &dckey, &lkey).await
    }

    /// How long a keep-alive sent now may take: until the permits would end
    /// (the deadline minus the fence lead), at least [`MIN_RENEW_BUDGET`];
    /// one period without a deadline yet.
    fn renew_budget(&self, now: Instant) -> Duration {
        let s = self.lock();
        match s.deadline.and_then(|d| d.checked_sub(self.opts.fence_lead)) {
            Some(until) => until.saturating_duration_since(now).max(MIN_RENEW_BUDGET),
            None => self.opts.period,
        }
    }

    async fn renew(&self, lease: LeaseId) -> Result<()> {
        let ts = self.clock.now();
        let budget = self.renew_budget(ts);
        let r = tokio::time::timeout(budget, self.store.keep_alive(lease)).await;
        match r {
            Ok(Ok(Some(granted))) => {
                let mut s = self.lock();
                let to = ts + granted;
                s.deadline = Some(s.deadline.map_or(to, |d| d.max(to)));
                Ok(())
            }
            Ok(Ok(None)) => {
                tracing::warn!(dc = %self.dc, holder = %self.holder, "DC lease ended");
                let mut s = self.lock();
                s.lease = None;
                s.deadline = None;
                Ok(())
            }
            Ok(Err(e)) => Err(e),
            Err(_) => Err(SprigError::new(
                sprig_common::SprigCode::MetaUnavailable,
                "DC lease keep-alive got no answer while the permits were valid",
            )),
        }
    }

    async fn take(
        &self,
        mut rec: Record<DcRecord>,
        rev: u64,
        dckey: &Key,
        lkey: &Key,
    ) -> Result<()> {
        let t0 = self.clock.now();
        let lease = self.store.grant_lease(self.opts.ttl).await?;
        let epoch = rec.body.dc_epoch + 1;
        rec.body.dc_epoch = epoch;
        let value = Record::new(DcLeaseRecord {
            lease_id: lease.0,
            dc_epoch: epoch,
            holder: self.holder.clone(),
        })
        .encode()?;
        let r = self
            .store
            .txn(Txn {
                when: vec![Cmp::Missing(lkey.clone()), Cmp::ModRev(dckey.clone(), rev)],
                then: vec![
                    Op::Put {
                        key: lkey.clone(),
                        value,
                        lease: Some(lease),
                    },
                    Op::Put {
                        key: dckey.clone(),
                        value: rec.encode()?,
                        lease: None,
                    },
                ],
                otherwise: Vec::new(),
            })
            .await;
        match r {
            Ok(r) if r.succeeded => {
                tracing::info!(dc = %self.dc, holder = %self.holder, dc_epoch = epoch, "took the DC lease");
                let mut s = self.lock();
                s.lease = Some((lease, epoch));
                s.deadline = Some(t0 + self.opts.ttl);
                Ok(())
            }
            Ok(_) => {
                let _ = self.store.revoke_lease(lease).await;
                Ok(())
            }
            Err(e) => {
                let _ = self.store.revoke_lease(lease).await;
                Err(e)
            }
        }
    }

    /// Renews every period until dropped; `leading` says whether this
    /// controller may take a new lease. A failed round is retried after
    /// [`RETRY_AFTER`], so one slow or lost keep-alive costs a quarter
    /// second, not a period (r2next).
    pub async fn run(&self, leading: impl Fn() -> bool) {
        loop {
            let started = tokio::time::Instant::now();
            let wait = match self.tick(leading()).await {
                Ok(()) => self.opts.period.saturating_sub(started.elapsed()),
                Err(e) => {
                    tracing::warn!(dc = %self.dc, error = %e, "DC lease round failed; retrying");
                    RETRY_AFTER
                }
            };
            tokio::time::sleep(wait).await;
        }
    }
}

#[cfg(test)]
mod tests;
