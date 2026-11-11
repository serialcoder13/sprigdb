//! Chapter 52 in the controller: the approvals hub every deciding loop
//! calls before acting ([`ControllerApprovals`]), appliers for components
//! that never revisit a held decision, the decision-log hook, and the
//! `approvals` loop that runs timers, appliers and gauges.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::Duration;

use sprig_approvals::gate::{Approvals, Event, Hook};
use sprig_approvals::machine::{EventKind, Ticket};
use sprig_approvals::model::{Proposal, RecId, Recommendation, State, Subject};
use sprig_approvals::store::{Filter, RecStore};
use sprig_approvals::{Kind, Policy, Verdict, settings as keys};
use sprig_balancer::decisions::{DecisionLog, Mode, NewDecision, Outcome as DecOutcome};
use sprig_common::Result;
use sprig_common::ids::{ClusterId, DbId, DcId, ObjectId, TenantId};
use sprig_settings::{LiveSettings, Resolved, SettingCtx};

use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler, WatchSpec};
use crate::tenant_scopes::TenantScopes;
use crate::tree::Directory;

/// Applies items of a kind whose component parks held decisions and never
/// proposes them again (automatic indexing): the approvals loop calls it.
#[async_trait::async_trait]
pub trait Applier: Send + Sync {
    /// Whether the loop applies approved items (`false`: the component
    /// proposes them again and applies them itself; the applier only
    /// reports progress).
    fn pushes(&self) -> bool {
        true
    }
    /// `Some(why)` when the item no longer holds (it becomes `expired`).
    async fn revalidate(&self, rec: &Recommendation) -> Result<Option<String>>;
    /// Starts the change through the component's own machinery.
    async fn apply(&self, rec: &Recommendation) -> Result<()>;
    /// For an item in `applying`: `Some((ok, detail))` once the change is
    /// done or failed.
    async fn progress(&self, rec: &Recommendation) -> Result<Option<(bool, String)>>;
}

pub mod http;
pub mod index;
pub mod notify;
pub mod service;
pub mod webhook;

/// Proposes held decisions of a component that parks them (automatic
/// indexing's `recommended` rows): the approvals loop calls it per tenant
/// before its pass, so items are raised and refreshed like any gate call.
#[async_trait::async_trait]
pub trait Producer: Send + Sync {
    /// Raises or refreshes `tenant`'s items through `ctl.gate`.
    async fn sync(&self, ctl: &ControllerApprovals, tenant: TenantId) -> Result<()>;
}

/// The controller's approvals: the hub, policy resolution, ancestry and
/// appliers.
pub struct ControllerApprovals {
    hub: Arc<Approvals>,
    dc: DcId,
    directory: Directory,
    live: Option<LiveSettings>,
    scopes: Option<Arc<TenantScopes>>,
    appliers: RwLock<BTreeMap<Kind, Arc<dyn Applier>>>,
    producers: RwLock<Vec<Arc<dyn Producer>>>,
    locator: Option<Arc<dyn sprig_catalog::CatalogLocator>>,
}

impl fmt::Debug for ControllerApprovals {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControllerApprovals")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

impl ControllerApprovals {
    /// The controller of `dc`.
    #[must_use]
    pub fn new(
        hub: Arc<Approvals>,
        dc: DcId,
        directory: Directory,
        live: Option<LiveSettings>,
    ) -> Self {
        Self {
            hub,
            dc,
            directory,
            live,
            scopes: None,
            appliers: RwLock::new(BTreeMap::new()),
            producers: RwLock::new(Vec::new()),
            locator: None,
        }
    }

    /// Tenant, database and object scopes from the tenants' catalogs.
    #[must_use]
    pub fn with_tenant_scopes(mut self, s: Arc<TenantScopes>) -> Self {
        self.scopes = Some(s);
        self
    }

    /// Tenant catalogs, for email subscriptions.
    #[must_use]
    pub fn with_locator(mut self, l: Arc<dyn sprig_catalog::CatalogLocator>) -> Self {
        self.locator = Some(l);
        self
    }

    /// The catalog locator.
    ///
    /// # Panics
    /// Never in the controller, which always sets one; tests without one
    /// get a static locator that knows no tenant.
    #[must_use]
    pub fn locator(&self) -> Arc<dyn sprig_catalog::CatalogLocator> {
        self.locator
            .clone()
            .unwrap_or_else(|| Arc::new(sprig_catalog::StaticLocator::new()))
    }

    /// Registers the applier of `kind`.
    pub fn register(&self, kind: Kind, a: Arc<dyn Applier>) {
        self.appliers
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(kind, a);
    }

    /// Registers a producer.
    pub fn register_producer(&self, p: Arc<dyn Producer>) {
        self.producers
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .push(p);
    }

    /// Runs every producer for `tenant`; errors are logged.
    pub async fn produce(&self, tenant: TenantId) {
        let ps: Vec<Arc<dyn Producer>> = self
            .producers
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        for p in ps {
            if let Err(e) = p.sync(self, tenant).await {
                tracing::debug!(tenant = %tenant, error = %e, "approvals: producer");
            }
        }
    }

    fn applier(&self, kind: Kind) -> Option<Arc<dyn Applier>> {
        self.appliers
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&kind)
            .cloned()
    }

    /// The hub.
    #[must_use]
    pub fn hub(&self) -> &Arc<Approvals> {
        &self.hub
    }

    /// This controller's DC.
    #[must_use]
    pub fn dc(&self) -> &DcId {
        &self.dc
    }

    /// The directory.
    #[must_use]
    pub fn directory(&self) -> &Directory {
        &self.directory
    }

    /// Brings this controller's copy of `tenant`'s setting rows up to the
    /// directory's catalog version now, so the next resolution sees a change
    /// made a moment ago (approvals round 3: the vector gate).
    pub async fn refresh_tenant(&self, tenant: TenantId) {
        if let Some(s) = &self.scopes {
            let _ = s.get_fresh(tenant).await;
        }
    }

    /// Resolves `key` for a subject: object, database, tenant (with its
    /// ancestors' copies), cluster, DC, global.
    pub async fn resolve(
        &self,
        key: &str,
        tenant: Option<TenantId>,
        db: Option<DbId>,
        object: Option<ObjectId>,
        cluster: Option<&ClusterId>,
    ) -> Option<Resolved> {
        let live = self.live.as_ref()?;
        let snap = live.snapshot();
        let mut ctx = SettingCtx::process(&self.dc);
        if let Some(c) = cluster {
            ctx = ctx.with_cluster(c);
        }
        let rows = match (tenant, &self.scopes) {
            (Some(t), Some(s)) => s.get(t).await,
            _ => None,
        };
        if let Some(t) = tenant {
            ctx = ctx.for_tenant(t, &[]);
            if let Some(d) = db {
                ctx = ctx.with_db(d);
                if let Some(o) = object {
                    ctx = ctx.with_object(o);
                }
            }
        }
        match rows {
            Some(r) => r.resolve(&snap, key, &ctx).ok(),
            None => snap.resolve(key, &ctx).ok(),
        }
    }

    /// `kind`'s policy for a subject.
    pub async fn policy(
        &self,
        kind: Kind,
        tenant: Option<TenantId>,
        db: Option<DbId>,
        object: Option<ObjectId>,
        cluster: Option<&ClusterId>,
    ) -> Policy {
        let mut values: BTreeMap<&'static str, Resolved> = BTreeMap::new();
        for key in [
            kind.mode_key(),
            keys::START_LEVEL.key,
            keys::ESCALATE_AFTER.key,
            keys::AUTO_APPLY_AFTER.key,
            keys::AUTO_APPLY_FORBIDDEN.key,
            keys::ALLOW_EMERGENCY.key,
            keys::RERAISE_FACTOR.key,
            keys::IGNORE_ONCE_DAYS.key,
        ] {
            if let Some(r) = self.resolve(key, tenant, db, object, cluster).await {
                values.insert(key, r);
            }
        }
        let lookup = move |k: &str| values.get(k).cloned();
        let p = Policy::resolve(kind, &lookup);
        for w in &p.warnings {
            tracing::warn!(kind = %kind, "approvals policy: {w}");
        }
        p
    }

    /// `kind`'s policy with the mode the component resolved itself.
    pub async fn policy_with(
        &self,
        kind: Kind,
        mode_text: &str,
        source: sprig_settings::ScopeKind,
        tenant: Option<TenantId>,
        db: Option<DbId>,
        object: Option<ObjectId>,
    ) -> Policy {
        let mut values: BTreeMap<&'static str, Resolved> = BTreeMap::new();
        for key in [
            keys::START_LEVEL.key,
            keys::ESCALATE_AFTER.key,
            keys::AUTO_APPLY_AFTER.key,
            keys::AUTO_APPLY_FORBIDDEN.key,
            keys::ALLOW_EMERGENCY.key,
            keys::RERAISE_FACTOR.key,
            keys::IGNORE_ONCE_DAYS.key,
        ] {
            if let Some(r) = self.resolve(key, tenant, db, object, None).await {
                values.insert(key, r);
            }
        }
        let lookup = move |k: &str| values.get(k).cloned();
        Policy::resolve_with(kind, mode_text, source, &lookup)
    }

    /// The tenant's ancestors, root first.
    pub async fn ancestry(&self, tenant: TenantId) -> Vec<TenantId> {
        match self.directory.get(tenant).await {
            Ok(Some(e)) => ancestors_of(&e.body().path, tenant),
            _ => Vec::new(),
        }
    }

    /// A tenant subject with the tenant's name and home DC filled in.
    pub async fn subject(&self, tenant: TenantId) -> Subject {
        let mut s = Subject::tenant(tenant, self.dc.clone());
        if let Ok(Some(e)) = self.directory.get(tenant).await {
            s.tenant_name = Some(e.body().name.clone());
            s.dc = Some(e.body().home_dc.clone());
        }
        s
    }

    /// The gate: records or refreshes `p` and says whether to act. A failed
    /// check never lets an `approve` decision through.
    pub async fn gate(&self, p: &Proposal, policy: &Policy) -> Verdict {
        let ancestry = match p.subject.tenant {
            Some(t) => self.ancestry(t).await,
            None => Vec::new(),
        };
        self.hub.check_or_hold(p, policy, &ancestry).await
    }

    /// The component reports a finished action; errors are logged.
    pub async fn finished(&self, ticket: &Ticket, ok: bool, detail: &str) {
        if let Err(e) = self.hub.finished(ticket, ok, detail).await {
            tracing::warn!(error = %e, "approvals: could not record the result");
        }
    }

    /// One pass over `store`: timers, appliers, progress. Returns the
    /// waiting items for the gauges.
    async fn pass(&self, store: &dyn RecStore, timeout: Duration) -> Result<Vec<Recommendation>> {
        self.hub.tick(store, timeout).await?;
        let live = store.list(&Filter::default()).await?;
        for r in &live {
            let Some(a) = self.applier(r.kind) else {
                continue;
            };
            match r.state {
                State::Approved if a.pushes() => match a.revalidate(r).await {
                    Ok(Some(why)) => self.hub.expire(&r.id, &why).await?,
                    Ok(None) => {
                        if let Some(t) = self.hub.start(&r.id).await?
                            && let Err(e) = a.apply(r).await
                        {
                            self.finished(&t, false, &e.message).await;
                        }
                    }
                    Err(e) => tracing::warn!(id = %r.id, error = %e, "approvals: revalidate"),
                },
                State::Applying => {
                    if let Ok(Some((ok, detail))) = a.progress(r).await {
                        let t = Ticket {
                            id: Some(r.id.clone()),
                            kind: r.kind,
                        };
                        self.finished(&t, ok, &detail).await;
                    }
                }
                _ => {}
            }
        }
        Ok(live)
    }
}

/// Reports DC moves (`dc.move`) done: the decider starts approved moves
/// itself; the item is applied once the tenant's home is the target.
#[derive(Debug)]
pub struct DcMoveProgress {
    directory: Directory,
}

impl DcMoveProgress {
    /// Over the directory.
    #[must_use]
    pub fn new(directory: Directory) -> Self {
        Self { directory }
    }
}

#[async_trait::async_trait]
impl Applier for DcMoveProgress {
    fn pushes(&self) -> bool {
        false
    }
    async fn revalidate(&self, _rec: &Recommendation) -> Result<Option<String>> {
        Ok(None)
    }
    async fn apply(&self, _rec: &Recommendation) -> Result<()> {
        Ok(())
    }
    async fn progress(&self, rec: &Recommendation) -> Result<Option<(bool, String)>> {
        let (Some(t), Some(to)) = (
            rec.subject.tenant,
            rec.action
                .machine
                .get("to")
                .and_then(serde_json::Value::as_str),
        ) else {
            return Ok(Some((false, "the item names no tenant or target".into())));
        };
        Ok(match self.directory.get(t).await? {
            Some(e) if e.body().home_dc.as_str() == to => {
                Some((true, format!("the tenant's home is now {to}")))
            }
            Some(_) => None,
            None => Some((false, "the tenant is gone".into())),
        })
    }
}

static PROCESS: std::sync::OnceLock<Arc<ControllerApprovals>> = std::sync::OnceLock::new();

/// Makes `a` the process's approvals, for loops built far from the role's
/// wiring (failover).
pub fn install(a: Arc<ControllerApprovals>) {
    let _ = PROCESS.set(a);
}

/// The process's approvals, if the controller installed them.
#[must_use]
pub fn installed() -> Option<&'static Arc<ControllerApprovals>> {
    PROCESS.get()
}

/// Chapter 52's gate for a cluster writer failover the safety checks
/// allow: `None` to go ahead, `Some(why)` to hold. Held only when
/// `failover.mode = approve` comes from the global scope; an approval
/// lets the failover code proceed on a later pass only if its checks
/// still allow it then (it calls this again), never forces one.
pub async fn failover_gate(dc: &DcId, cluster: &ClusterId, writer: &str) -> Option<String> {
    use sprig_approvals::{Action, Mode, Proposal};
    let a = installed()?;
    let policy = a
        .policy(Kind::FailoverCluster, None, None, None, Some(cluster))
        .await;
    if policy.mode != Mode::Approve {
        return None;
    }
    let p = Proposal {
        kind: Kind::FailoverCluster,
        subject: Subject {
            dc: Some(dc.clone()),
            cluster: Some(cluster.clone()),
            ..Subject::default()
        },
        target: format!("cluster/{cluster}"),
        action: Action {
            text: format!("fail over cluster {cluster} (writer {writer} lost)"),
            machine: serde_json::json!({ "writer": writer }),
        },
        evidence: serde_json::json!({ "writer": writer }),
        score: 1.0,
        summary: format!("cluster {cluster}: writer {writer} lost; fail over"),
        emergency: false,
    };
    match a.gate(&p, &policy).await {
        Verdict::Proceed(t) => {
            if t.id.is_some() {
                a.finished(&t, true, "failover started").await;
            }
            None
        }
        Verdict::Wait(id) => Some(format!("failover held for approval as {id}")),
        Verdict::Suppressed(id) => Some(format!("failover ignored ({id})")),
        _ => Some("failover held: approvals unavailable".into()),
    }
}

/// What chapter 52's gate says about one tenant's promotion in a DC
/// failover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DcFailoverGate {
    /// Promote now; report the result with [`dc_failover_done`] when the
    /// ticket names an item.
    Go(Option<Ticket>),
    /// Held (the reason names the item); ask again on a later pass.
    Hold(String),
}

/// Chapter 52's gate for promoting `tenant` from the lost DC `x` to `to`
/// in DC failover `failover_id` (the global leader, chapter 25 step 7).
/// Held only when `failover.mode = approve` comes from the global scope;
/// the item lives in the global tier. An approval lets the failover code
/// go ahead only after it has checked again that the DC's lease is still
/// gone, the sites still agree, and the standby's lag is still within the
/// tenant's RPO: it can stop a failover, never force one.
pub async fn dc_failover_gate(
    a: Option<&ControllerApprovals>,
    x: &DcId,
    tenant: TenantId,
    to: &DcId,
    failover_id: &str,
    lag_ms: Option<u32>,
) -> DcFailoverGate {
    use sprig_approvals::{Action, Mode};
    let Some(a) = a else {
        return DcFailoverGate::Go(None);
    };
    let policy = a.policy(Kind::FailoverDc, None, None, None, None).await;
    if policy.mode != Mode::Approve {
        return DcFailoverGate::Go(None);
    }
    let mut subject = a.subject(tenant).await;
    subject.dc = Some(x.clone());
    let name = subject
        .tenant_name
        .clone()
        .unwrap_or_else(|| tenant.to_string());
    let p = Proposal {
        kind: Kind::FailoverDc,
        subject,
        target: format!("t{tenant}/dc/{x}"),
        action: Action {
            text: format!("promote tenant {name}'s standby in {to} (DC {x} lost)"),
            machine: serde_json::json!({ "from": x.as_str(), "to": to.as_str() }),
        },
        evidence: serde_json::json!({
            "failover": failover_id,
            "lost_dc": x.as_str(),
            "standby_dc": to.as_str(),
            "standby_lag_ms": lag_ms,
        }),
        score: 1.0,
        summary: format!("DC {x} is lost: fail tenant {name} over to its standby in {to}"),
        emergency: false,
    };
    match a.gate(&p, &policy).await {
        Verdict::Proceed(t) => DcFailoverGate::Go(Some(t)),
        Verdict::Wait(id) => DcFailoverGate::Hold(format!("held for approval as {id}")),
        Verdict::Suppressed(id) => DcFailoverGate::Hold(format!("ignored ({id}); fenced in {x}")),
        _ => DcFailoverGate::Hold("held: approvals unavailable".into()),
    }
}

/// Reports the promotion a [`DcFailoverGate::Go`] ticket let through.
pub async fn dc_failover_done(
    a: Option<&ControllerApprovals>,
    ticket: Option<&Ticket>,
    ok: bool,
    detail: &str,
) {
    if let (Some(t), Some(a)) = (ticket, a)
        && t.id.is_some()
    {
        a.finished(t, ok, detail).await;
    }
}

/// Ancestors (root first) from a directory path `/1/42/77/`, without
/// `tenant` itself.
#[must_use]
pub fn ancestors_of(path: &str, tenant: TenantId) -> Vec<TenantId> {
    path.split('/')
        .filter_map(|s| s.parse::<u64>().ok())
        .map(TenantId)
        .filter(|t| *t != tenant)
        .collect()
}

/// Writes every transition of a tenant's item to its decision log
/// (chapter 11; chapter 52, "Observability").
pub struct DecisionHook {
    log: Arc<dyn DecisionLog>,
    dc: DcId,
}

impl fmt::Debug for DecisionHook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DecisionHook").finish_non_exhaustive()
    }
}

impl DecisionHook {
    /// A hook writing to `log`.
    #[must_use]
    pub fn new(log: Arc<dyn DecisionLog>, dc: DcId) -> Self {
        Self { log, dc }
    }
}

#[async_trait::async_trait]
impl Hook for DecisionHook {
    async fn event(&self, ev: &Event) {
        let Some(tenant) = ev.rec.subject.tenant else {
            return;
        };
        if !matches!(ev.rec.id, RecId::Tenant(..)) {
            return;
        }
        let person = !matches!(
            ev.actor.as_str(),
            "gate" | "approvals" | "component" | "auto-apply"
        );
        let (mode, outcome) = match ev.kind {
            EventKind::New | EventKind::Reraised => (Mode::Recommended, DecOutcome::Proposed),
            EventKind::Applying | EventKind::Emergency => (Mode::Applied, DecOutcome::Running),
            EventKind::Applied => (Mode::Applied, DecOutcome::Succeeded),
            EventKind::Failed => (Mode::Applied, DecOutcome::Failed),
            EventKind::Ignored => (Mode::Operator, DecOutcome::Rejected),
            EventKind::Expired | EventKind::Superseded => {
                (Mode::Recommended, DecOutcome::Superseded)
            }
            _ if person => (Mode::Operator, DecOutcome::Succeeded),
            _ => (Mode::Recommended, DecOutcome::Scheduled),
        };
        let d = NewDecision {
            decided_at_us: ev.at_us,
            component: "approvals".into(),
            kind: ev.kind.decision_kind().into(),
            subject_type: "recommendation".into(),
            subject_id: ev.rec.id.to_string(),
            mode,
            outcome,
            reason_code: ev.kind.name().into(),
            summary: format!("{} {}: {}", ev.rec.kind, ev.rec.state, ev.rec.summary),
            evidence: serde_json::json!({
                "kind": ev.rec.kind.as_str(),
                "level": ev.rec.level.as_str(),
                "notify_level": ev.rec.notify_level.to_string(),
                "target": ev.rec.target,
                "action": ev.rec.action.text,
                "score": ev.rec.score,
            }),
            alternatives: None,
            job_id: None,
            parent_id: None,
            actor: if person {
                format!("op:{}", ev.actor)
            } else {
                ev.actor.clone()
            },
            dc_id: self.dc.to_string(),
        };
        if let Err(e) = self.log.record(tenant, &d).await {
            tracing::warn!(tenant = %tenant, error = %e, "approvals: decision log write failed");
        }
    }
}

/// What the `approvals` loop reconciles: a tenant homed here, or this
/// DC's tier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ApprovalKey {
    /// One tenant's catalog items.
    Tenant(TenantId),
    /// The DC tier's items.
    Dc,
}

impl fmt::Display for ApprovalKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApprovalKey::Tenant(t) => write!(f, "t{t}"),
            ApprovalKey::Dc => f.write_str("dc"),
        }
    }
}

/// The `approvals` loop (chapter 52, "Escalation and auto-apply"): on the
/// DC leader, every `approvals.tick_seconds`, per tenant homed here and
/// for the DC tier.
pub struct ApprovalsLoop {
    a: Arc<ControllerApprovals>,
    seen: Mutex<BTreeMap<ApprovalKey, Vec<Recommendation>>>,
    notifier: Option<Arc<notify::Notifier>>,
}

impl fmt::Debug for ApprovalsLoop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApprovalsLoop").finish_non_exhaustive()
    }
}

impl ApprovalsLoop {
    /// The loop over `a`.
    #[must_use]
    pub fn new(a: Arc<ControllerApprovals>) -> Self {
        Self {
            a,
            seen: Mutex::new(BTreeMap::new()),
            notifier: None,
        }
    }

    /// Sends the daily digests.
    #[must_use]
    pub fn with_notifier(mut self, n: Arc<notify::Notifier>) -> Self {
        self.notifier = Some(n);
        self
    }

    async fn setting_u32(
        &self,
        s: &'static sprig_settings::Setting<u32>,
        t: Option<TenantId>,
    ) -> u32 {
        self.a
            .resolve(s.key, t, None, None, None)
            .await
            .and_then(|r| r.value.as_f64())
            .map_or(s.default, |v| {
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let v = v.max(0.0) as u32;
                v
            })
    }
}

#[async_trait::async_trait]
impl Reconciler for ApprovalsLoop {
    type Key = ApprovalKey;

    fn name(&self) -> &'static str {
        "approvals"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn watches(&self) -> Vec<WatchSpec> {
        vec![WatchSpec::global("tenants/")]
    }

    fn keys_for(&self, ev: &MetaEvent) -> Vec<ApprovalKey> {
        self.a
            .directory
            .tenant_of(&ev.key)
            .into_iter()
            .map(ApprovalKey::Tenant)
            .collect()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<ApprovalKey>> {
        let mut keys: Vec<ApprovalKey> = self
            .a
            .directory
            .list()
            .await?
            .iter()
            .map(crate::tree::StoredEntry::body)
            .filter(|e| e.home_dc == self.a.dc && e.state == crate::tree::TenantState::Active)
            .map(|e| ApprovalKey::Tenant(e.tenant_id))
            .collect();
        keys.push(ApprovalKey::Dc);
        Ok(keys)
    }

    async fn reconcile(&self, _cx: &LoopCx, key: &ApprovalKey) -> Result<Outcome> {
        let tenant = match key {
            ApprovalKey::Tenant(t) => Some(*t),
            ApprovalKey::Dc => None,
        };
        let tick = Duration::from_secs(u64::from(
            self.setting_u32(&keys::TICK_SECONDS, None).await.max(1),
        ));
        if let Some(t) = tenant {
            match self.a.directory.get(t).await? {
                Some(e)
                    if e.body().home_dc == self.a.dc
                        && e.body().state == crate::tree::TenantState::Active => {}
                _ => {
                    self.seen
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .remove(key);
                    return Ok(Outcome::Done);
                }
            }
        }
        let store: Arc<dyn RecStore> = match tenant {
            Some(t) => self.a.hub.stores().tenant(t),
            None => match self.a.hub.stores().dc(&self.a.dc) {
                Some(s) => s,
                None => return Ok(Outcome::RequeueAfter(tick)),
            },
        };
        let timeout = Duration::from_secs(
            u64::from(
                self.setting_u32(&keys::APPLYING_TIMEOUT_HOURS, tenant)
                    .await,
            ) * 3600,
        );
        if let Some(t) = tenant {
            self.a.produce(t).await;
        }
        let live = self.a.pass(&*store, timeout).await?;
        let retention = self.setting_u32(&keys::RETENTION_DAYS, tenant).await;
        let before = self.a.hub.now() - i64::from(retention) * 86_400 * 1_000_000;
        if let Err(e) = store.purge(before).await {
            tracing::debug!(key = %key, error = %e, "approvals: purge");
        }
        if let Some(n) = &self.notifier {
            n.digest(tenant, &live, self.a.hub.now()).await;
        }
        let all: Vec<Recommendation> = {
            let mut g = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
            g.insert(key.clone(), live);
            g.values().flatten().cloned().collect()
        };
        sprig_approvals::metrics::waiting(&all, self.a.hub.now());
        Ok(Outcome::RequeueAfter(tick))
    }

    fn workers(&self) -> usize {
        4
    }
}

/// Timers, expiry and retention of the global tier's items (DC failover
/// holds), on the global leader.
pub struct GlobalApprovalsLoop {
    a: Arc<ControllerApprovals>,
}

impl fmt::Debug for GlobalApprovalsLoop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GlobalApprovalsLoop")
            .finish_non_exhaustive()
    }
}

impl GlobalApprovalsLoop {
    /// The loop over `a`.
    #[must_use]
    pub fn new(a: Arc<ControllerApprovals>) -> Self {
        Self { a }
    }
}

#[async_trait::async_trait]
impl Reconciler for GlobalApprovalsLoop {
    type Key = &'static str;

    fn name(&self) -> &'static str {
        "approvals_global"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::GlobalLeader
    }

    fn keys_for(&self, _ev: &MetaEvent) -> Vec<&'static str> {
        vec!["global"]
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<&'static str>> {
        Ok(vec!["global"])
    }

    async fn reconcile(&self, cx: &LoopCx, _key: &&'static str) -> Result<Outcome> {
        cx.leader()?;
        let num = |r: Option<Resolved>, d: u32| {
            r.and_then(|r| r.value.as_f64()).map_or(d, |v| {
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let v = v.max(0.0) as u32;
                v
            })
        };
        let tick = num(
            self.a
                .resolve(keys::TICK_SECONDS.key, None, None, None, None)
                .await,
            keys::TICK_SECONDS.default,
        );
        let tick = Duration::from_secs(u64::from(tick.max(1)));
        let Some(store) = self.a.hub.stores().global() else {
            return Ok(Outcome::RequeueAfter(tick));
        };
        let hours = num(
            self.a
                .resolve(keys::APPLYING_TIMEOUT_HOURS.key, None, None, None, None)
                .await,
            keys::APPLYING_TIMEOUT_HOURS.default,
        );
        self.a
            .pass(&*store, Duration::from_secs(u64::from(hours) * 3600))
            .await?;
        let days = num(
            self.a
                .resolve(keys::RETENTION_DAYS.key, None, None, None, None)
                .await,
            keys::RETENTION_DAYS.default,
        );
        let before = self.a.hub.now() - i64::from(days) * 86_400 * 1_000_000;
        if let Err(e) = store.purge(before).await {
            tracing::debug!(error = %e, "approvals: global purge");
        }
        Ok(Outcome::RequeueAfter(tick))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ancestors_come_from_the_path() {
        assert_eq!(
            ancestors_of("/1/42/77/", TenantId(77)),
            vec![TenantId(1), TenantId(42)]
        );
        assert_eq!(ancestors_of("/5/", TenantId(5)), Vec::<TenantId>::new());
    }
}
