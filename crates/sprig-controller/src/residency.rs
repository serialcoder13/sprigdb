//! Data residency (chapter 26; R2 items 4): a tenant's policy over DC
//! tags, resolved to its allowed DCs, enforced at every place the tenant's
//! data can move to another DC, and converged when it changes.
//!
//! ```text
//! allowed_dcs(T) = resolve(policy(T)) ∩ allowed_dcs(parent(T))   # root: own set
//!                  no policy on T or any ancestor -> unrestricted
//! set_policy(T, text, apply), POST /admin/v1/tenants/{name}:residency:
//! 1. parse, else 39506; 2. resolve through the tree, empty -> 39507 naming the
//!    ancestor; 3. warn about DCs admitted only through a missing tag; 4. plan T and
//!    every descendant; 5. a step with no way to comply -> 39507 (apply only);
//! 6. apply: the directory's residency block {policy, policy_hash, allowed_dcs,
//!    deny_remote_access, state, pending} on T, allowed_dcs on its descendants
//! residency loop (DC leader, every tenant homed in its DC, every 10 s while not
//! compliant; again on every directory or DC record change):
//!    re-resolve (DC tags change); drop standbys outside the set; start a move of a
//!    home outside the set (cause residency, migration.mode ignored); shards on
//!    clusters whose DCs leave the set move through the balancer's residency_change;
//!    state compliant | converging | violated, with what is pending
//! ```
//!
//! **Enforcement points (as built).** Every path that can put a tenant's
//! data in another DC checks the set from the directory entry it commits
//! against, so a policy changed concurrently stops it at its commit point:
//!
//! | Path | Check |
//! |---|---|
//! | DC move, start (`migrate`) | target in the set (39550) |
//! | DC move, commit (`sprig_shard::home::commit`) | target in the set, same transaction as the home change |
//! | Automatic DC failover (`failover::decision`) | standbys outside the set skipped |
//! | Promotion, commit point (`failover::promote`) | target in the set, same transaction |
//! | Manual failover, standbys (`failover::manual`) | target / standby DC in the set (39505) |
//! | Placement (`balancer_loop`) | the set in each tenant's view: no cluster with a DC outside it |
//! | New tenants (`tenants`) | a child's home must be in its parent's set (39507) |
//! | Logins (gateway) | outside the set: handed to an allowed DC (chapter 14), or 39302 |
//!
//! A missing or malformed `allowed_dcs` in a residency block allows
//! nothing (fail closed). The global tier being unreachable leaves every
//! reader with the last set it saw.

pub mod policy;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sprig_catalog::placement::{MapRole, ShardMap};
use sprig_cluster::record::{AdminState, ClusterRecord};
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::ids::{ClusterId, DcId, TenantId};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::bootstrap::DcRecord;
use sprig_meta::{Cmp, ListOpts, MetaStore, Op, Record, Tier, TierView, Txn};
use sprig_shard::home::{self, HomeEntry};

pub use policy::{Policy, resolve};

use crate::failover::promote::DcTiers;
use crate::migrate::{DcMoves, MoveTenantRequest};
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler, WatchSpec};

/// How often a tenant that isn't compliant is looked at again.
pub const RECHECK: Duration = Duration::from_secs(10);

/// 39507.
fn unsatisfiable(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::ResidencyUnsatisfiable, msg)
}

fn now_ms(clock: &dyn Clock) -> u64 {
    clock
        .wall()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn list_dcs(set: &BTreeSet<DcId>) -> String {
    if set.is_empty() {
        return "none".into();
    }
    set.iter().map(DcId::as_str).collect::<Vec<_>>().join(", ")
}

/// The directory entry's `residency` block (chapter 26, "Data structures
/// and storage").
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResidencyBlock {
    /// The tenant's own policy; empty: whatever the ancestors allow.
    #[serde(default)]
    pub policy: String,
    /// Hash of the policy text.
    #[serde(default)]
    pub policy_hash: String,
    /// The resolved set.
    #[serde(default)]
    pub allowed_dcs: Vec<DcId>,
    /// `residency.deny_remote_access`.
    #[serde(default)]
    pub deny_remote_access: bool,
    /// `compliant`, `converging` or `violated`.
    #[serde(default)]
    pub state: String,
    /// What is still to do (or why it can't be done).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending: Vec<String>,
    /// When the block last changed, Unix ms.
    #[serde(default)]
    pub updated_ms: u64,
}

impl ResidencyBlock {
    /// The block of a directory entry, if it has one.
    #[must_use]
    pub fn of(raw: &Map<String, Value>) -> Option<Self> {
        let v = raw.get("residency").filter(|v| !v.is_null())?;
        // A block that doesn't parse allows nothing (fail closed).
        Some(serde_json::from_value(v.clone()).unwrap_or_else(|_| ResidencyBlock {
            state: "violated".into(),
            pending: vec!["the residency block doesn't parse".into()],
            ..ResidencyBlock::default()
        }))
    }

    /// The allowed set.
    #[must_use]
    pub fn allowed(&self) -> BTreeSet<DcId> {
        self.allowed_dcs.iter().cloned().collect()
    }
}

/// The DCs a cluster's data is in: its own, and every replica's (chapter
/// 26, "The cluster-level replica rule"). Backup targets join when chapter
/// 47's backups exist.
#[must_use]
pub fn cluster_dcs(c: &ClusterRecord) -> BTreeSet<DcId> {
    let mut out = BTreeSet::from([c.dc_id.clone()]);
    if let Some(d) = &c.declared {
        out.extend(d.replica_dcs.iter().cloned());
    }
    out
}

/// `eligible(c, T) = cluster_dcs(c) ⊆ allowed_dcs(T)`; every cluster is
/// eligible for a tenant without residency.
#[must_use]
pub fn eligible(c: &ClusterRecord, allowed: Option<&BTreeSet<DcId>>) -> bool {
    allowed.is_none_or(|a| cluster_dcs(c).is_subset(a))
}

/// One tenant as tree resolution sees it.
#[derive(Debug, Clone)]
pub struct Node {
    /// The parent.
    pub parent: Option<TenantId>,
    /// The tenant's own policy; `None` or empty: none.
    pub policy: Option<Policy>,
}

/// `allowed_dcs` of every tenant in `nodes` (see the module docs): `None`
/// for a tenant without a policy on itself or any ancestor.
#[must_use]
pub fn resolve_tree(
    nodes: &BTreeMap<TenantId, Node>,
    registry: &[DcRecord],
) -> BTreeMap<TenantId, Option<BTreeSet<DcId>>> {
    fn go(
        t: TenantId,
        nodes: &BTreeMap<TenantId, Node>,
        registry: &[DcRecord],
        memo: &mut BTreeMap<TenantId, Option<BTreeSet<DcId>>>,
        depth: usize,
    ) -> Option<BTreeSet<DcId>> {
        if let Some(r) = memo.get(&t) {
            return r.clone();
        }
        let node = nodes.get(&t)?;
        let parent = match node.parent {
            // A cycle or an absurd depth: fail closed.
            Some(_) if depth > 64 => Some(BTreeSet::new()),
            Some(p) => go(p, nodes, registry, memo, depth + 1),
            None => None,
        };
        let own = node
            .policy
            .as_ref()
            .filter(|p| !p.is_empty())
            .map(|p| resolve(p, registry));
        let r = match (own, parent) {
            (None, None) => None,
            (None, Some(p)) => Some(p),
            (Some(o), None) => Some(o),
            (Some(o), Some(p)) => Some(o.intersection(&p).cloned().collect()),
        };
        memo.insert(t, r.clone());
        r
    }
    let mut memo = BTreeMap::new();
    for &t in nodes.keys() {
        go(t, nodes, registry, &mut memo, 0);
    }
    memo
}

/// A cluster holding a tenant, and the DCs its data is in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterFacts {
    /// The cluster.
    pub id: ClusterId,
    /// `cluster_dcs`.
    pub dcs: BTreeSet<DcId>,
}

/// What the plan needs about one tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Facts {
    /// The tenant.
    pub tenant: TenantId,
    /// Its name.
    pub name: String,
    /// The home DC.
    pub home_dc: DcId,
    /// The directory state.
    pub state: String,
    /// `pinned_dc` (`sprigctl tenant pin`).
    pub pinned_dc: Option<DcId>,
    /// `standby_dcs`.
    pub standby_dcs: Vec<DcId>,
    /// The target of a move in progress.
    pub migration_to: Option<DcId>,
    /// Clusters of the home DC holding it.
    pub clusters: Vec<ClusterFacts>,
    /// Active change-feed subscriptions, by the DC whose workers consume
    /// them: (DC, subscription name).
    pub feed_subs: Vec<(DcId, String)>,
}

impl Facts {
    /// The facts of a directory entry (clusters left empty).
    #[must_use]
    pub fn of(tenant: TenantId, e: &HomeEntry) -> Self {
        Self {
            tenant,
            name: e
                .raw
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            home_dc: e.home_dc.clone(),
            state: e.state.clone(),
            pinned_dc: e.pinned_dc.clone(),
            standby_dcs: standby_dcs(&e.raw),
            migration_to: e.migration_to(),
            clusters: Vec::new(),
            feed_subs: Vec::new(),
        }
    }
}

/// `standby_dcs` of an entry.
#[must_use]
pub fn standby_dcs(raw: &Map<String, Value>) -> Vec<DcId> {
    raw.get("standby_dcs")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .filter_map(|d| DcId::new(d).ok())
                .collect()
        })
        .unwrap_or_default()
}

/// One step of a compliance plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// Move the tenant's home (a `tenant_migration`, chapter 24).
    MoveHome {
        /// From.
        from: DcId,
        /// To.
        to: DcId,
    },
    /// Stop treating a DC as a standby; the operator removes the copy.
    DropStandby(DcId),
    /// Move the tenant off a cluster whose copies are outside the set.
    MoveOffCluster {
        /// The cluster.
        cluster: ClusterId,
        /// Its DCs outside the set.
        outside: BTreeSet<DcId>,
    },
    /// A move in progress whose target is outside the set: its commit is
    /// refused.
    StopMove(DcId),
    /// Pause a change-feed subscription consumed outside the set (chapter
    /// 38): no new consumer job starts there.
    PauseFeed {
        /// The DC whose workers consume it.
        dc: DcId,
        /// The subscription.
        name: String,
    },
}

impl fmt::Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Step::MoveHome { from, to } => write!(f, "move the home from {from} to {to}"),
            Step::DropStandby(d) => write!(
                f,
                "drop the standby in {d} (SprigDB stops failing over there; remove the copy's replication and data)"
            ),
            Step::MoveOffCluster { cluster, outside } => write!(
                f,
                "move off cluster {cluster}, which has copies in {}",
                list_dcs(outside)
            ),
            Step::StopMove(d) => write!(f, "the move in progress to {d} is refused at its commit"),
            Step::PauseFeed { dc, name } => write!(
                f,
                "pause change-feed subscription {name}, consumed in {dc}"
            ),
        }
    }
}

/// A compliance plan.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Plan {
    /// What to do.
    pub steps: Vec<Step>,
    /// Why some of it can't be done.
    pub blocking: Vec<String>,
}

impl Plan {
    /// The steps and blockers as text.
    #[must_use]
    pub fn lines(&self) -> Vec<String> {
        self.steps
            .iter()
            .map(ToString::to_string)
            .chain(self.blocking.iter().map(|b| format!("blocked: {b}")))
            .collect()
    }
}

/// Step 4 for one tenant: what it takes to comply with `allowed`.
/// `targets` are the allowed DCs that could take the home (active, with
/// an active eligible cluster), best first.
#[must_use]
pub fn plan(f: &Facts, allowed: &BTreeSet<DcId>, targets: &[DcId]) -> Plan {
    let mut p = Plan::default();
    if allowed.is_empty() {
        p.blocking
            .push("the residency allows no data center".into());
        return p;
    }
    if let Some(to) = &f.migration_to
        && !allowed.contains(to)
    {
        p.steps.push(Step::StopMove(to.clone()));
    }
    if !allowed.contains(&f.home_dc) {
        if let Some(pin) = &f.pinned_dc {
            p.blocking.push(format!(
                "the tenant is pinned to {pin}, outside the allowed DCs: sprigctl tenant unpin {} first",
                f.name
            ));
        } else if let Some(to) = targets.iter().find(|d| allowed.contains(d)) {
            p.steps.push(Step::MoveHome {
                from: f.home_dc.clone(),
                to: to.clone(),
            });
        } else {
            p.blocking.push(format!(
                "no allowed DC ({}) has an active cluster that keeps every copy inside the allowed DCs",
                list_dcs(allowed)
            ));
        }
    } else {
        for c in &f.clusters {
            let outside: BTreeSet<DcId> = c.dcs.difference(allowed).cloned().collect();
            if !outside.is_empty() {
                p.steps.push(Step::MoveOffCluster {
                    cluster: c.id.clone(),
                    outside,
                });
            }
        }
    }
    for s in &f.standby_dcs {
        if !allowed.contains(s) {
            p.steps.push(Step::DropStandby(s.clone()));
        }
    }
    for (dc, name) in &f.feed_subs {
        if !allowed.contains(dc) {
            p.steps.push(Step::PauseFeed {
                dc: dc.clone(),
                name: name.clone(),
            });
        }
    }
    p
}

/// `POST /admin/v1/tenants/{name}:residency`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResidencyRequest {
    /// The new policy; `None` keeps the current one; `""` removes it.
    #[serde(default)]
    pub policy: Option<String>,
    /// `residency.deny_remote_access`; `None` keeps it.
    #[serde(default)]
    pub deny_remote_access: Option<bool>,
    /// Write it; otherwise print the plan only.
    #[serde(default)]
    pub apply: bool,
}

/// One tenant's part of a plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantPlan {
    /// The tenant.
    pub tenant: String,
    /// Its id.
    pub tenant_id: TenantId,
    /// Its home.
    pub home_dc: DcId,
    /// Its allowed DCs under the plan; `None`: unrestricted.
    pub allowed_dcs: Option<Vec<DcId>>,
    /// The steps and blockers.
    pub plan: Vec<String>,
}

/// A tenant's residency as `sprigctl tenant residency` shows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResidencyView {
    /// The tenant.
    pub tenant: String,
    /// Its id.
    pub tenant_id: TenantId,
    /// Its own policy.
    pub policy: String,
    /// The allowed DCs; `None`: unrestricted.
    pub allowed_dcs: Option<Vec<DcId>>,
    /// `residency.deny_remote_access`.
    pub deny_remote_access: bool,
    /// `compliant`, `converging`, `violated`, or `none`.
    pub state: String,
    /// What is pending.
    #[serde(default)]
    pub pending: Vec<String>,
    /// The plan for the tenant and every descendant (a dry run, or what
    /// was applied).
    #[serde(default)]
    pub plans: Vec<TenantPlan>,
    /// Step 3's warnings.
    #[serde(default)]
    pub warnings: Vec<String>,
    /// Whether this call wrote the policy.
    #[serde(default)]
    pub applied: bool,
}

/// A directory entry with its id.
#[derive(Debug, Clone)]
struct Entry {
    id: TenantId,
    home: HomeEntry,
}

impl Entry {
    fn parent(&self) -> Option<TenantId> {
        self.home
            .raw
            .get("parent_tenant_id")
            .and_then(Value::as_u64)
            .filter(|&p| p != 0)
            .map(TenantId)
    }

    fn name(&self) -> String {
        self.home
            .raw
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    }

    fn under(&self, t: TenantId) -> bool {
        self.id != t
            && self
                .home
                .raw
                .get("path")
                .and_then(Value::as_str)
                .is_some_and(|p| p.contains(&format!("/{t}/")))
    }
}

async fn entries(global: &dyn MetaStore) -> Result<Vec<Entry>> {
    let prefix = Tier::Global.key("tenants/")?;
    let mut out = Vec::new();
    let mut opts = ListOpts::default();
    loop {
        let page = global.list(&prefix, opts.clone()).await?;
        for kv in &page.kvs {
            let Some(id) = kv
                .key
                .as_str()
                .rsplit('/')
                .next()
                .and_then(|s| s.parse::<u64>().ok())
            else {
                continue;
            };
            if let Ok(home) = HomeEntry::decode(&kv.value, kv.mod_revision) {
                out.push(Entry {
                    id: TenantId(id),
                    home,
                });
            }
        }
        if !page.more {
            break;
        }
        opts.at = Some(page.revision);
        opts.after = page.kvs.last().map(|kv| kv.key.clone());
    }
    Ok(out)
}

fn nodes_of(entries: &[Entry]) -> BTreeMap<TenantId, Node> {
    entries
        .iter()
        .map(|e| {
            let policy = ResidencyBlock::of(&e.home.raw)
                .map(|b| Policy::parse(&b.policy).unwrap_or_default());
            (
                e.id,
                Node {
                    parent: e.parent(),
                    policy,
                },
            )
        })
        .collect()
}

/// Residency's controller side: the Admin calls and the loop share it.
pub struct Residency {
    dc: DcId,
    global: Arc<dyn MetaStore>,
    tiers: DcTiers,
    clock: Arc<dyn Clock>,
    moves: Option<Arc<DcMoves>>,
    states: Mutex<HashMap<TenantId, &'static str>>,
    cache: Mutex<Option<(Instant, Arc<Snapshot>)>>,
}

impl std::fmt::Debug for Residency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Residency")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

/// The directory and DC registry, read once per pass.
#[derive(Debug)]
struct Snapshot {
    entries: Vec<Entry>,
    dcs: Vec<DcRecord>,
    allowed: BTreeMap<TenantId, Option<BTreeSet<DcId>>>,
}

impl Residency {
    /// Residency for the controllers of `dc`: `global` is the global
    /// tier, `tiers` reaches each DC's tier (for maps and clusters),
    /// `moves` starts DC moves (the loop only).
    #[must_use]
    pub fn new(
        dc: DcId,
        global: Arc<dyn MetaStore>,
        tiers: DcTiers,
        clock: Arc<dyn Clock>,
        moves: Option<Arc<DcMoves>>,
    ) -> Self {
        Self {
            dc,
            global,
            tiers,
            clock,
            moves,
            states: Mutex::new(HashMap::new()),
            cache: Mutex::new(None),
        }
    }

    /// Residency over one etcd holding every tier (tests, single-etcd
    /// mode).
    #[must_use]
    pub fn single_etcd(
        dc: DcId,
        store: &Arc<dyn MetaStore>,
        clock: Arc<dyn Clock>,
        moves: Option<Arc<DcMoves>>,
    ) -> Self {
        let s = Arc::clone(store);
        Self::new(
            dc,
            TierView::over(store, Tier::Global),
            Arc::new(move |d: &DcId| TierView::over(&s, Tier::Dc(d.clone()))),
            clock,
            moves,
        )
    }

    async fn snapshot(&self, fresh: bool) -> Result<Arc<Snapshot>> {
        if !fresh
            && let Some((at, s)) = &*self.cache.lock().unwrap_or_else(PoisonError::into_inner)
            && at.elapsed() < Duration::from_secs(1)
        {
            return Ok(Arc::clone(s));
        }
        let entries = entries(&*self.global).await?;
        let dcs = crate::dc::list(&*self.global).await?;
        let allowed = resolve_tree(&nodes_of(&entries), &dcs);
        let s = Arc::new(Snapshot {
            entries,
            dcs,
            allowed,
        });
        *self.cache.lock().unwrap_or_else(PoisonError::into_inner) =
            Some((Instant::now(), Arc::clone(&s)));
        Ok(s)
    }

    /// The clusters of `dc` that hold `tenant` (from its maps there), with
    /// their DCs.
    async fn clusters_of(&self, tenant: TenantId, dc: &DcId) -> Result<Vec<ClusterFacts>> {
        let tier = (self.tiers)(dc);
        let prefix = tier.tier().key(&format!("shardmaps/{tenant}/"))?;
        let page = tier.list(&prefix, ListOpts::default()).await?;
        let mut ids = BTreeSet::new();
        for kv in &page.kvs {
            if let Ok(m) = Record::<ShardMap>::decode(&kv.value)
                && m.body.role == MapRole::Home
            {
                ids.extend(m.body.ranges.iter().map(|r| r.cluster_id.clone()));
            }
        }
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let reg = ClusterRegistry::new(Arc::clone(&self.global), tier)?;
        let mut out = Vec::new();
        for id in ids {
            let dcs = match reg.get(&id).await? {
                Some(c) => cluster_dcs(c.body()),
                None => BTreeSet::from([dc.clone()]),
            };
            out.push(ClusterFacts { id, dcs });
        }
        Ok(out)
    }

    /// Allowed DCs that could take a home: active, full, with an active
    /// cluster whose copies all stay inside `allowed`; nearest to `from`
    /// first by the latency matrix, then by id.
    async fn targets(
        &self,
        dcs: &[DcRecord],
        allowed: &BTreeSet<DcId>,
        from: &DcId,
    ) -> Vec<DcId> {
        let matrix = crate::latency::Matrix::read(&*self.global)
            .await
            .unwrap_or_default();
        let mut out = Vec::new();
        for d in dcs {
            if d.kind != "full" || d.state != "active" || !allowed.contains(&d.dc_id) {
                continue;
            }
            let Ok(reg) = ClusterRegistry::new(Arc::clone(&self.global), (self.tiers)(&d.dc_id))
            else {
                continue;
            };
            let ok = reg.list().await.is_ok_and(|cs| {
                cs.iter().any(|c| {
                    c.body().admin_state == AdminState::Active && eligible(c.body(), Some(allowed))
                })
            });
            if ok {
                out.push(d.dc_id.clone());
            }
        }
        out.sort_by_key(|d| {
            (
                matrix
                    .rtt(from.as_str(), d.as_str())
                    .map_or(u64::MAX, |r| (r * 1000.0) as u64),
                d.clone(),
            )
        });
        out
    }

    async fn facts(&self, e: &Entry, dcs: &[DcRecord]) -> Result<Facts> {
        let mut f = Facts::of(e.id, &e.home);
        f.clusters = self.clusters_of(e.id, &e.home.home_dc).await?;
        // Every DC's feed subscriptions of the tenant (a tier out of reach
        // is skipped: its subscriptions can't be paused from here either).
        for d in dcs {
            let subs = sprig_feed::subs::Subscriptions::new((self.tiers)(&d.dc_id));
            if let Ok(list) = subs.list(e.id).await {
                f.feed_subs.extend(
                    list.into_iter()
                        .filter(|s| s.state == "active")
                        .map(|s| (d.dc_id.clone(), s.name)),
                );
            }
        }
        Ok(f)
    }

    /// `GET /admin/v1/tenants/{name}/residency`.
    ///
    /// # Errors
    /// 39072 for an unknown tenant; the stores' errors.
    pub async fn show(&self, name: &str) -> Result<ResidencyView> {
        let id = crate::tree::Directory::new(Arc::clone(&self.global))
            .require(name)
            .await?
            .body()
            .tenant_id;
        let snap = self.snapshot(true).await?;
        let e = snap
            .entries
            .iter()
            .find(|e| e.id == id)
            .ok_or_else(|| SprigError::new(SprigCode::TenantNotReady, "no directory entry"))?;
        let b = ResidencyBlock::of(&e.home.raw);
        Ok(ResidencyView {
            tenant: name.to_owned(),
            tenant_id: id,
            policy: b.as_ref().map(|b| b.policy.clone()).unwrap_or_default(),
            allowed_dcs: home::allowed_dcs(&e.home.raw).map(|a| a.into_iter().collect()),
            deny_remote_access: b.as_ref().is_some_and(|b| b.deny_remote_access),
            state: b
                .as_ref()
                .map_or_else(|| "none".to_owned(), |b| b.state.clone()),
            pending: b.map(|b| b.pending).unwrap_or_default(),
            plans: Vec::new(),
            warnings: Vec::new(),
            applied: false,
        })
    }

    /// `POST /admin/v1/tenants/{name}:residency` (see the module docs).
    ///
    /// # Errors
    /// 39072 unknown tenant; 39506 bad policy; 39507 when the policy
    /// allows no DC, or (with `apply`) some tenant of the subtree can't
    /// comply; the stores' errors.
    #[allow(clippy::too_many_lines)] // the six steps, in order
    pub async fn set(&self, name: &str, req: &ResidencyRequest) -> Result<ResidencyView> {
        let id = crate::tree::Directory::new(Arc::clone(&self.global))
            .require(name)
            .await?
            .body()
            .tenant_id;
        let snap = self.snapshot(true).await?;
        let me = snap
            .entries
            .iter()
            .find(|e| e.id == id)
            .ok_or_else(|| SprigError::new(SprigCode::TenantNotReady, "no directory entry"))?;
        let cur = ResidencyBlock::of(&me.home.raw);
        // 1.
        let text = req
            .policy
            .clone()
            .unwrap_or_else(|| cur.as_ref().map(|b| b.policy.clone()).unwrap_or_default());
        let p = Policy::parse(&text)?;
        let deny = req
            .deny_remote_access
            .unwrap_or_else(|| cur.as_ref().is_some_and(|b| b.deny_remote_access));
        // 2.
        let mut nodes = nodes_of(&snap.entries);
        if let Some(n) = nodes.get_mut(&id) {
            n.policy = Some(p.clone());
        }
        let allowed = resolve_tree(&nodes, &snap.dcs);
        let mine = allowed.get(&id).cloned().flatten();
        if let Some(a) = &mine
            && a.is_empty()
        {
            let own = resolve(&p, &snap.dcs);
            let mut msg = format!(
                "residency policy {p} allows no active data center for tenant {name} (it admits: {})",
                list_dcs(&own)
            );
            // Name the nearest ancestor whose set excludes the policy's.
            let mut up = me.parent();
            while let Some(a) = up {
                let Some(ae) = snap.entries.iter().find(|e| e.id == a) else {
                    break;
                };
                if let Some(Some(set)) = allowed.get(&a)
                    && own.intersection(set).next().is_none()
                {
                    msg.push_str(&format!(
                        "; ancestor {} allows only {}",
                        ae.name(),
                        list_dcs(set)
                    ));
                    break;
                }
                up = ae.parent();
            }
            return Err(unsatisfiable(msg));
        }
        // 3.
        let warnings: Vec<String> = p
            .admitted_without_tags(&snap.dcs)
            .into_iter()
            .map(|(d, tags)| format!("{d} is admitted although it has no {tags} tag"))
            .collect();
        // 4.
        let mut plans = Vec::new();
        let mut blocked = Vec::new();
        for e in snap.entries.iter().filter(|e| e.id == id || e.under(id)) {
            let a = allowed.get(&e.id).cloned().flatten();
            let lines = match &a {
                None => Vec::new(),
                Some(set) => {
                    let f = self.facts(e, &snap.dcs).await?;
                    let targets = self.targets(&snap.dcs, set, &e.home.home_dc).await;
                    let pl = plan(&f, set, &targets);
                    for b in &pl.blocking {
                        blocked.push(format!("{}: {b}", e.name()));
                    }
                    pl.lines()
                }
            };
            plans.push(TenantPlan {
                tenant: e.name(),
                tenant_id: e.id,
                home_dc: e.home.home_dc.clone(),
                allowed_dcs: a.map(|s| s.into_iter().collect()),
                plan: lines,
            });
        }
        let mut view = ResidencyView {
            tenant: name.to_owned(),
            tenant_id: id,
            policy: p.text().to_owned(),
            allowed_dcs: mine.clone().map(|s| s.into_iter().collect()),
            deny_remote_access: deny,
            state: cur
                .as_ref()
                .map_or_else(|| "none".to_owned(), |b| b.state.clone()),
            pending: Vec::new(),
            plans,
            warnings,
            applied: false,
        };
        if !req.apply {
            return Ok(view);
        }
        // 5.
        if !blocked.is_empty() {
            return Err(unsatisfiable(format!(
                "tenant {name}'s residency can't be applied:\n- {}",
                blocked.join("\n- ")
            )));
        }
        // 6. The tenant, then each descendant whose set changes.
        let at = now_ms(&*self.clock);
        for tp in &view.plans {
            let own = tp.tenant_id == id;
            let set = tp.allowed_dcs.clone();
            let lines = tp.plan.clone();
            let (text, hash) = (p.text().to_owned(), p.hash());
            home::update_home(&*self.global, tp.tenant_id, |e| {
                let prev = ResidencyBlock::of(&e.raw);
                let Some(set) = &set else {
                    // Unrestricted: the tenant's own block goes, and so does
                    // an inherited one (no policy of its own). With no set,
                    // `deny_remote_access` has nothing to deny.
                    if own || prev.as_ref().is_some_and(|b| b.policy.is_empty()) {
                        return Ok(e.raw.remove("residency").is_some());
                    }
                    return Ok(false);
                };
                let mut b = prev.unwrap_or_default();
                if own {
                    b.policy.clone_from(&text);
                    b.policy_hash.clone_from(&hash);
                    b.deny_remote_access = deny;
                }
                let changed = b.allowed_dcs != *set || own;
                b.allowed_dcs.clone_from(set);
                b.state = if lines.is_empty() {
                    "compliant".into()
                } else {
                    "converging".into()
                };
                b.pending.clone_from(&lines);
                b.updated_ms = at;
                if !changed {
                    return Ok(false);
                }
                e.raw.insert(
                    "residency".into(),
                    serde_json::to_value(&b).map_err(|x| SprigError::internal(x.to_string()))?,
                );
                Ok(true)
            })
            .await?;
        }
        *self.cache.lock().unwrap_or_else(PoisonError::into_inner) = None;
        tracing::info!(tenant = %name, policy = %p, allowed = ?view.allowed_dcs, "residency policy applied");
        metrics::counter!("sprig_controller_residency_changes_total").increment(1);
        view.applied = true;
        view.state = if view.plans.iter().any(|t| !t.plan.is_empty()) {
            "converging".into()
        } else {
            "compliant".into()
        };
        view.pending = view
            .plans
            .iter()
            .find(|t| t.tenant_id == id)
            .map(|t| t.plan.clone())
            .unwrap_or_default();
        Ok(view)
    }

    /// The loop's pass for one tenant homed here.
    ///
    /// # Errors
    /// The stores' errors.
    #[allow(clippy::too_many_lines)] // resolve, act, record
    pub async fn reconcile_tenant(&self, tenant: TenantId) -> Result<Outcome> {
        let snap = self.snapshot(false).await?;
        let Some(e) = snap.entries.iter().find(|e| e.id == tenant) else {
            self.forget(tenant);
            return Ok(Outcome::Done);
        };
        if e.home.home_dc != self.dc {
            self.forget(tenant);
            return Ok(Outcome::Done);
        }
        let block = ResidencyBlock::of(&e.home.raw);
        let allowed = snap.allowed.get(&tenant).cloned().flatten();
        let Some(set) = allowed else {
            // Unrestricted: drop a block inherited from an ancestor whose
            // policy is gone.
            if let Some(b) = &block
                && b.policy.is_empty()
            {
                home::update_home(&*self.global, tenant, |x| {
                    Ok(x.raw.remove("residency").is_some())
                })
                .await?;
                *self.cache.lock().unwrap_or_else(PoisonError::into_inner) = None;
            }
            self.forget(tenant);
            return Ok(Outcome::Done);
        };
        let f = self.facts(e, &snap.dcs).await?;
        let targets = self.targets(&snap.dcs, &set, &e.home.home_dc).await;
        let pl = plan(&f, &set, &targets);
        let mut pending = pl.lines();
        let mut failed = !pl.blocking.is_empty();
        // Act.
        for step in &pl.steps {
            match step {
                Step::DropStandby(d) => {
                    if let Err(x) = self.drop_standby(tenant, d).await {
                        tracing::warn!(%tenant, dc = %d, error = %x, "residency: standby not dropped");
                        failed = true;
                    } else {
                        metrics::counter!("sprig_controller_residency_violations_total", "point" => "standby")
                            .increment(1);
                        pending.retain(|l| *l != step.to_string());
                    }
                }
                Step::MoveHome { to, .. } => {
                    if f.state != "active" || f.migration_to.is_some() {
                        continue;
                    }
                    let Some(moves) = &self.moves else { continue };
                    let req = MoveTenantRequest {
                        to: to.to_string(),
                        ..MoveTenantRequest::default()
                    };
                    match moves.start_caused(tenant, &f.name, &req, "residency").await {
                        Ok(_) => {
                            metrics::counter!("sprig_controller_residency_violations_total", "point" => "home")
                                .increment(1);
                            tracing::info!(%tenant, %to, "residency: moving the home");
                        }
                        Err(x) => {
                            tracing::warn!(%tenant, %to, error = %x, "residency: move not started");
                            pending.push(format!("the move to {to} didn't start: {}", x.message));
                            failed = true;
                        }
                    }
                }
                Step::PauseFeed { dc, name } => {
                    let subs = sprig_feed::subs::Subscriptions::new((self.tiers)(dc));
                    match subs.set_state(tenant, name, "paused").await {
                        Ok(_) => {
                            metrics::counter!("sprig_controller_residency_violations_total", "point" => "feed")
                                .increment(1);
                            tracing::info!(%tenant, %dc, subscription = %name, "residency: feed subscription paused");
                            pending.retain(|l| *l != step.to_string());
                        }
                        Err(x) => {
                            tracing::warn!(%tenant, %dc, error = %x, "residency: feed subscription not paused");
                            failed = true;
                        }
                    }
                }
                // The balancer moves shards (its residency_change) and the
                // move's commit refuses a disallowed target.
                Step::MoveOffCluster { .. } | Step::StopMove(_) => {}
            }
        }
        let state: &'static str = if failed {
            "violated"
        } else if pending.is_empty() {
            "compliant"
        } else {
            "converging"
        };
        let set_v: Vec<DcId> = set.iter().cloned().collect();
        let at = now_ms(&*self.clock);
        let write = block.as_ref().is_none_or(|b| {
            b.allowed_dcs != set_v || b.state != state || b.pending != pending
        });
        if write {
            home::update_home(&*self.global, tenant, |x| {
                let mut b = ResidencyBlock::of(&x.raw).unwrap_or_default();
                if b.allowed_dcs == set_v && b.state == state && b.pending == pending {
                    return Ok(false);
                }
                b.allowed_dcs.clone_from(&set_v);
                b.state = state.to_owned();
                b.pending.clone_from(&pending);
                b.updated_ms = at;
                x.raw.insert(
                    "residency".into(),
                    serde_json::to_value(&b).map_err(|e| SprigError::internal(e.to_string()))?,
                );
                Ok(true)
            })
            .await?;
            *self.cache.lock().unwrap_or_else(PoisonError::into_inner) = None;
            if state == "violated" {
                tracing::error!(%tenant, ?pending, "ResidencyViolated: the tenant can't comply with its residency");
            }
        }
        self.record(tenant, state);
        Ok(if state == "compliant" {
            Outcome::Done
        } else {
            Outcome::RequeueAfter(RECHECK)
        })
    }

    /// Removes `dc` from `tenant`'s standbys: its standby maps there go
    /// (best effort: the tier may be out of reach), then `standby_dcs`.
    async fn drop_standby(&self, tenant: TenantId, dc: &DcId) -> Result<()> {
        let tier = (self.tiers)(dc);
        if let Ok(prefix) = tier.tier().key(&format!("shardmaps/{tenant}/"))
            && let Ok(page) = tier.list(&prefix, ListOpts::default()).await
        {
            for kv in page.kvs {
                if Record::<ShardMap>::decode(&kv.value).is_ok_and(|m| m.body.role == MapRole::Standby)
                {
                    let _ = tier
                        .txn(Txn {
                            when: vec![Cmp::ModRev(kv.key.clone(), kv.mod_revision)],
                            then: vec![Op::Delete(kv.key)],
                            otherwise: Vec::new(),
                        })
                        .await;
                }
            }
        }
        home::update_home(&*self.global, tenant, |e| {
            let list = standby_dcs(&e.raw);
            if !list.contains(dc) {
                return Ok(false);
            }
            let kept: Vec<&str> = list
                .iter()
                .filter(|d| *d != dc)
                .map(DcId::as_str)
                .collect();
            e.raw.insert("standby_dcs".into(), json!(kept));
            Ok(true)
        })
        .await?;
        tracing::info!(%tenant, %dc, "residency: standby dropped");
        Ok(())
    }

    fn record(&self, tenant: TenantId, state: &'static str) {
        let mut m = self.states.lock().unwrap_or_else(PoisonError::into_inner);
        m.insert(tenant, state);
        Self::publish(&m);
    }

    fn forget(&self, tenant: TenantId) {
        let mut m = self.states.lock().unwrap_or_else(PoisonError::into_inner);
        if m.remove(&tenant).is_some() {
            Self::publish(&m);
        }
    }

    #[allow(clippy::cast_precision_loss)] // tenant counts
    fn publish(m: &HashMap<TenantId, &'static str>) {
        for s in ["compliant", "converging", "violated"] {
            let n = m.values().filter(|v| **v == s).count();
            metrics::gauge!("sprig_controller_residency_tenants", "state" => s).set(n as f64);
        }
    }

    /// The allowed DCs of a tenant (`None`: unrestricted), from the
    /// directory as last read (at most a second old).
    ///
    /// # Errors
    /// The stores' errors.
    pub async fn allowed(&self, tenant: TenantId) -> Result<Option<BTreeSet<DcId>>> {
        let snap = self.snapshot(false).await?;
        Ok(snap
            .entries
            .iter()
            .find(|e| e.id == tenant)
            .and_then(|e| home::allowed_dcs(&e.home.raw)))
    }
}

/// Every tenant's allowed DCs as the directory holds them (tenants
/// without residency left out), for placement.
///
/// # Errors
/// The store's errors.
pub async fn allowed_map(global: &dyn MetaStore) -> Result<BTreeMap<TenantId, BTreeSet<DcId>>> {
    Ok(entries(global)
        .await?
        .into_iter()
        .filter_map(|e| home::allowed_dcs(&e.home.raw).map(|a| (e.id, a)))
        .collect())
}

/// The `residency` loop (DC leader): [`Residency::reconcile_tenant`] for
/// every tenant homed in the DC.
#[derive(Debug, Clone)]
pub struct ResidencyLoop(pub Arc<Residency>);

#[async_trait::async_trait]
impl Reconciler for ResidencyLoop {
    type Key = TenantId;

    fn name(&self) -> &'static str {
        "residency"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn watches(&self) -> Vec<WatchSpec> {
        vec![WatchSpec::global("tenants/"), WatchSpec::global("dcs/")]
    }

    fn keys_for(&self, ev: &MetaEvent) -> Vec<TenantId> {
        let dir = crate::tree::Directory::new(Arc::clone(&self.0.global));
        if let Some(t) = dir.tenant_of(&ev.key) {
            return vec![t];
        }
        // A DC record changed (tags, a new DC, a retirement): every tenant
        // homed here that has a residency is resolved again.
        self.0
            .states
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .copied()
            .collect()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<TenantId>> {
        let snap = self.0.snapshot(true).await?;
        Ok(snap
            .entries
            .iter()
            .filter(|e| e.home.home_dc == self.0.dc)
            .filter(|e| {
                ResidencyBlock::of(&e.home.raw).is_some()
                    || snap.allowed.get(&e.id).is_some_and(Option::is_some)
            })
            .map(|e| e.id)
            .collect())
    }

    async fn reconcile(&self, _cx: &LoopCx, key: &TenantId) -> Result<Outcome> {
        self.0.reconcile_tenant(*key).await
    }
}

/// Checks a new child's home against its parent's set (chapter 26: a
/// child's residency lies within its ancestors'). Returns the block the
/// child inherits, if the parent has one.
///
/// # Errors
/// 39507 when the parent's set excludes `home`.
pub fn child_block(
    parent_name: &str,
    parent_raw: &Map<String, Value>,
    home: &DcId,
    at_ms: u64,
) -> Result<Option<ResidencyBlock>> {
    let Some(set) = home::allowed_dcs(parent_raw) else {
        return Ok(None);
    };
    if !set.contains(home) {
        return Err(unsatisfiable(format!(
            "tenant {parent_name}'s residency allows {}, not {home}: create the tenant through a controller of an allowed DC",
            list_dcs(&set)
        )));
    }
    Ok(Some(ResidencyBlock {
        allowed_dcs: set.into_iter().collect(),
        state: "compliant".into(),
        updated_ms: at_ms,
        ..ResidencyBlock::default()
    }))
}

/// Writes `block` into entry `id` (compare-and-swap on its revision).
///
/// # Errors
/// The store's errors; 39070 when the entry kept changing.
pub async fn write_block(global: &dyn MetaStore, id: TenantId, block: &ResidencyBlock) -> Result<()> {
    let v = serde_json::to_value(block).map_err(|e| SprigError::internal(e.to_string()))?;
    home::update_home(global, id, |e| {
        e.raw.insert("residency".into(), v.clone());
        Ok(true)
    })
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
