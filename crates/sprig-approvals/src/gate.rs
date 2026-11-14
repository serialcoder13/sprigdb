//! [`Approvals`]: the gate components call before acting, people's
//! actions, the timers, and the events every transition produces
//! (escalation and auto-apply).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sprig_catalog::CatalogLocator;
use sprig_common::ids::{DcId, TenantId};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{MetaStore, Tier};

use crate::access::{Caller, may_act};
use crate::catalog::CatalogRecStore;
use crate::etcd::EtcdRecStore;
use crate::machine::{self, Act, EventKind, TickChange, Ticket, Verdict};
use crate::model::{
    Home, Kind, Micros, Mode, Proposal, RecId, Recommendation, Subject, wall_micros,
};
use crate::policy::Policy;
use crate::store::{Filter, MemStore, RecStore, modify, not_found};

/// One transition, as hooks (decision log, notifications, metrics) see it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    /// What happened.
    pub kind: EventKind,
    /// The item after it.
    pub rec: Recommendation,
    /// Who did it.
    pub actor: String,
    /// When.
    pub at_us: Micros,
}

/// Receives every event. Hooks must not fail the transition: they log
/// their own errors.
#[async_trait::async_trait]
pub trait Hook: Send + Sync {
    /// One event.
    async fn event(&self, ev: &Event);
}

/// Finds the store of an item.
pub trait Stores: Send + Sync {
    /// A tenant's catalog store.
    fn tenant(&self, t: TenantId) -> Arc<dyn RecStore>;
    /// A DC tier's store, if this process reaches it.
    fn dc(&self, dc: &DcId) -> Option<Arc<dyn RecStore>>;
    /// The global tier's store, if this process reaches it.
    fn global(&self) -> Option<Arc<dyn RecStore>>;
}

/// Catalog stores through a locator, and the tiers this process holds.
pub struct LocatorStores {
    locator: Arc<dyn CatalogLocator>,
    dcs: BTreeMap<DcId, Arc<dyn RecStore>>,
    global: Option<Arc<dyn RecStore>>,
}

impl std::fmt::Debug for LocatorStores {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocatorStores").finish_non_exhaustive()
    }
}

impl LocatorStores {
    /// Catalogs through `locator`.
    #[must_use]
    pub fn new(locator: Arc<dyn CatalogLocator>) -> Self {
        Self {
            locator,
            dcs: BTreeMap::new(),
            global: None,
        }
    }

    /// Adds DC `dc`'s tier.
    #[must_use]
    pub fn with_dc(mut self, dc: DcId, store: Arc<dyn MetaStore>) -> Self {
        let s: Arc<dyn RecStore> = Arc::new(EtcdRecStore::new(store, Tier::Dc(dc.clone())));
        self.dcs.insert(dc, s);
        self
    }

    /// Adds the global tier.
    #[must_use]
    pub fn with_global(mut self, store: Arc<dyn MetaStore>) -> Self {
        self.global = Some(Arc::new(EtcdRecStore::new(store, Tier::Global)));
        self
    }
}

impl Stores for LocatorStores {
    fn tenant(&self, t: TenantId) -> Arc<dyn RecStore> {
        Arc::new(CatalogRecStore::new(self.locator.clone(), t))
    }
    fn dc(&self, dc: &DcId) -> Option<Arc<dyn RecStore>> {
        self.dcs.get(dc).cloned()
    }
    fn global(&self) -> Option<Arc<dyn RecStore>> {
        self.global.clone()
    }
}

/// Memory stores for tests and the simulation.
#[derive(Debug, Default)]
pub struct MemStores {
    tenants: Mutex<BTreeMap<TenantId, Arc<MemStore>>>,
    dcs: Mutex<BTreeMap<DcId, Arc<MemStore>>>,
    global: Mutex<Option<Arc<MemStore>>>,
}

impl MemStores {
    /// Empty.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The memory store of `t`.
    pub fn of(&self, t: TenantId) -> Arc<MemStore> {
        self.tenants
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(t)
            .or_insert_with(|| Arc::new(MemStore::tenant(t)))
            .clone()
    }
}

impl Stores for MemStores {
    fn tenant(&self, t: TenantId) -> Arc<dyn RecStore> {
        self.of(t)
    }
    fn dc(&self, dc: &DcId) -> Option<Arc<dyn RecStore>> {
        let d = dc.clone();
        Some(
            self.dcs
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(dc.clone())
                .or_insert_with(|| Arc::new(MemStore::new(move |n| RecId::Dc(d.clone(), n))))
                .clone(),
        )
    }
    fn global(&self) -> Option<Arc<dyn RecStore>> {
        Some(
            self.global
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get_or_insert_with(|| Arc::new(MemStore::new(RecId::Global)))
                .clone(),
        )
    }
}

/// Knobs a person's action needs from the item's policy.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ActKnobs {
    /// `approvals.ignore_once_days`.
    pub ignore_once_days: u32,
    /// `approvals.reraise_factor`.
    pub reraise_factor: f64,
}

impl Default for ActKnobs {
    fn default() -> Self {
        Self {
            ignore_once_days: crate::settings::IGNORE_ONCE_DAYS.default,
            reraise_factor: crate::settings::RERAISE_FACTOR.default,
        }
    }
}

/// The approvals hub of a process.
pub struct Approvals {
    stores: Arc<dyn Stores>,
    hooks: Vec<Arc<dyn Hook>>,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for Approvals {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Approvals")
            .field("hooks", &self.hooks.len())
            .finish_non_exhaustive()
    }
}

fn unreachable_store(what: &str) -> SprigError {
    SprigError::new(
        SprigCode::CatalogUnavailable,
        format!("this process doesn't reach the {what} that holds the recommendation"),
    )
}

impl Approvals {
    /// A hub over `stores`.
    #[must_use]
    pub fn new(stores: Arc<dyn Stores>, clock: Arc<dyn Clock>) -> Self {
        Self {
            stores,
            hooks: Vec::new(),
            clock,
        }
    }

    /// Adds a hook.
    #[must_use]
    pub fn with_hook(mut self, h: Arc<dyn Hook>) -> Self {
        self.hooks.push(h);
        self
    }

    /// Now, in microseconds.
    #[must_use]
    pub fn now(&self) -> Micros {
        wall_micros(self.clock.wall())
    }

    /// The stores.
    #[must_use]
    pub fn stores(&self) -> &Arc<dyn Stores> {
        &self.stores
    }

    /// The store an item of `kind` about `subject` lives in.
    ///
    /// # Errors
    /// 39070 when this process doesn't reach it; `Internal` for a subject
    /// without the tenant or DC its kind needs.
    pub fn store_of(&self, kind: Kind, subject: &Subject) -> Result<Arc<dyn RecStore>> {
        match kind.home() {
            Home::Tenant => subject
                .tenant
                .map(|t| self.stores.tenant(t))
                .ok_or_else(|| SprigError::internal(format!("{kind} needs a tenant"))),
            Home::Dc => {
                let dc = subject
                    .dc
                    .as_ref()
                    .ok_or_else(|| SprigError::internal(format!("{kind} needs a DC")))?;
                self.stores
                    .dc(dc)
                    .ok_or_else(|| unreachable_store("DC tier"))
            }
            Home::Global => self
                .stores
                .global()
                .ok_or_else(|| unreachable_store("global tier")),
        }
    }

    /// The store of item `id`.
    ///
    /// # Errors
    /// 39070 when this process doesn't reach it.
    pub fn store_for(&self, id: &RecId) -> Result<Arc<dyn RecStore>> {
        match id {
            RecId::Tenant(t, _) => Ok(self.stores.tenant(*t)),
            RecId::Dc(dc, _) => self
                .stores
                .dc(dc)
                .ok_or_else(|| unreachable_store("DC tier")),
            RecId::Global(_) => self
                .stores
                .global()
                .ok_or_else(|| unreachable_store("global tier")),
        }
    }

    async fn emit(&self, kind: EventKind, rec: &Recommendation, actor: &str) {
        crate::metrics::event(kind, rec);
        let ev = Event {
            kind,
            rec: rec.clone(),
            actor: actor.to_owned(),
            at_us: self.now(),
        };
        tracing::info!(target: "sprig_approvals", event = kind.name(), id = %rec.id,
            kind = %rec.kind, level = %rec.level, state = %rec.state, actor, "recommendation");
        for h in &self.hooks {
            h.event(&ev).await;
        }
    }

    /// The gate: records or refreshes `p` and
    /// says whether to act. `ancestry` is the tenant's, root first.
    ///
    /// # Errors
    /// The store's. Callers under `approve` treat an error as
    /// [`Verdict::Wait`]: never act on a failed check.
    pub async fn check(
        &self,
        p: &Proposal,
        policy: &Policy,
        ancestry: &[TenantId],
    ) -> Result<Verdict> {
        match policy.mode {
            Mode::Off => return Ok(Verdict::Off),
            Mode::On if !p.emergency => {
                return Ok(Verdict::Proceed(Ticket {
                    id: None,
                    kind: p.kind,
                }));
            }
            _ => {}
        }
        if policy.mode == Mode::On {
            // An emergency under `on`: act, as always.
            return Ok(Verdict::Proceed(Ticket {
                id: None,
                kind: p.kind,
            }));
        }
        let store = self.store_of(p.kind, &p.subject)?;
        let raised = store.raise(p, policy, ancestry, self.now()).await?;
        for s in &raised.superseded {
            self.emit(EventKind::Superseded, s, "gate").await;
        }
        if let Some(rec) = &raised.rec {
            if let Some(ev) = raised.event {
                self.emit(ev, rec, "gate").await;
            }
        } else if matches!(raised.verdict, Verdict::Suppressed(_)) {
            metrics::counter!("sprig_approvals_suppressed_total", "kind" => p.kind.as_str())
                .increment(1);
        }
        Ok(raised.verdict)
    }

    /// As [`check`](Self::check), but an error reads as "don't act" under
    /// `approve` and `recommend`, and as "act" under `on`.
    pub async fn check_or_hold(
        &self,
        p: &Proposal,
        policy: &Policy,
        ancestry: &[TenantId],
    ) -> Verdict {
        match self.check(p, policy, ancestry).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(kind = %p.kind, target = %p.target, error = %e,
                    "approvals: check failed; holding the decision");
                match policy.mode {
                    Mode::On => Verdict::Proceed(Ticket {
                        id: None,
                        kind: p.kind,
                    }),
                    _ => Verdict::Off,
                }
            }
        }
    }

    /// The component reports a finished action.
    ///
    /// # Errors
    /// The store's.
    pub async fn finished(&self, ticket: &Ticket, ok: bool, detail: &str) -> Result<()> {
        let Some(id) = &ticket.id else {
            return Ok(());
        };
        let store = self.store_for(id)?;
        let now = self.now();
        let (rec, ev) = modify(&*store, id, |r| Ok(machine::finish(r, ok, detail, now))).await?;
        if let Some(ev) = ev {
            self.emit(ev, &rec, "component").await;
        }
        Ok(())
    }

    /// One item.
    ///
    /// # Errors
    /// 39972 if missing; the store's.
    pub async fn get(&self, id: &RecId) -> Result<Recommendation> {
        self.store_for(id)?
            .get(id.number())
            .await?
            .ok_or_else(|| not_found(id))
    }

    /// A person's action, after `may_act`.
    ///
    /// # Errors
    /// 39972, 39973, 39974, 39976, 39977; the store's.
    pub async fn act(
        &self,
        id: &RecId,
        act: &Act,
        caller: &Caller,
        ancestry: &[TenantId],
        knobs: ActKnobs,
    ) -> Result<Recommendation> {
        let store = self.store_for(id)?;
        let now = self.now();
        let (rec, ev) = modify(&*store, id, |r| {
            if !may_act(r, caller, ancestry) {
                return Err(SprigError::new(
                    SprigCode::ApprovalDenied,
                    format!(
                        "{} may not act on recommendation {} (level {})",
                        caller.name, r.id, r.level
                    ),
                ));
            }
            machine::act(
                r,
                act,
                &caller.name,
                knobs.ignore_once_days,
                knobs.reraise_factor,
                now,
            )
            .map(Some)
        })
        .await?;
        if let Some(ev) = ev {
            if let (EventKind::Approved | EventKind::Ignored, Some(_)) = (ev, rec.decided_at_us) {
                crate::metrics::decided(&rec, now);
            }
            self.emit(ev, &rec, &caller.name).await;
        }
        Ok(rec)
    }

    /// Runs the timers of every due item in `store`
    /// (escalation and auto-apply). Returns the changes made.
    ///
    /// # Errors
    /// The store's.
    pub async fn tick(
        &self,
        store: &dyn RecStore,
        applying_timeout: Duration,
    ) -> Result<Vec<(RecId, TickChange)>> {
        let now = self.now();
        let mut out = Vec::new();
        for r in store.due(now, 500).await? {
            let id = r.id.clone();
            let res = modify(store, &id, |r| Ok(machine::tick(r, now, applying_timeout))).await;
            match res {
                Ok((rec, Some(change))) => {
                    self.emit(change.event(), &rec, "approvals").await;
                    out.push((id, change));
                }
                Ok((_, None)) => {}
                Err(e) => tracing::warn!(id = %id, error = %e, "approvals: tick failed"),
            }
        }
        Ok(out)
    }

    /// An applier found the item stale.
    ///
    /// # Errors
    /// The store's.
    pub async fn expire(&self, id: &RecId, why: &str) -> Result<()> {
        let store = self.store_for(id)?;
        let now = self.now();
        let (rec, ev) = modify(&*store, id, |r| Ok(machine::expire(r, why, now))).await?;
        if let Some(ev) = ev {
            self.emit(ev, &rec, "approvals").await;
        }
        Ok(())
    }

    /// The approvals loop starts an approved item through an applier;
    /// `None` when it isn't approved or its window hasn't opened.
    ///
    /// # Errors
    /// The store's.
    pub async fn start(&self, id: &RecId) -> Result<Option<Ticket>> {
        let store = self.store_for(id)?;
        let now = self.now();
        let (rec, started) = modify(&*store, id, |r| {
            Ok(machine::start_applying(r, now).then_some(()))
        })
        .await?;
        if started.is_none() {
            return Ok(None);
        }
        self.emit(EventKind::Applying, &rec, "approvals").await;
        Ok(Some(Ticket {
            id: Some(rec.id.clone()),
            kind: rec.kind,
        }))
    }

    /// Items of one store passing `f`.
    ///
    /// # Errors
    /// The store's.
    pub async fn list(&self, store: &dyn RecStore, f: &Filter) -> Result<Vec<Recommendation>> {
        store.list(f).await
    }
}
