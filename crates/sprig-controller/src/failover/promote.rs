//! Promotion and epochs.
//!
//! ```text
//! promote(T, Y), a tenant_promote job in Y's tier:
//! 1. global txn on tenants/T: if epoch == e and home_dc == X then home_dc = Y,
//!    epoch = e + 1, state = failing_over                     # the commit point
//! 2. stop T's standby jobs; record applied_source_gtid per source cluster in F
//! 3. Y's shard maps for T: role home, each range epoch + 1, tenant_epoch = e + 1
//! 4. grants reconciler: write grants on the promoted schemas; auth_version + 1
//! 5. state = active
//! ```
//!
//! Every step is idempotent and the commit point is recorded in the
//! directory, so a promotion interrupted after step 1 is finished by the
//! next call (the entry then shows `home_dc = Y`, `state = failing_over`).
//! Gateways see the new epoch and route to Y; one still routing to X meets
//! a fenced writer.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::{Value, json};
use sprig_catalog::placement::{MapRole, ShardMap};
use sprig_common::ids::{DcId, TenantId};
use sprig_common::{Result, SprigError};
use sprig_meta::{Cmp, Leadership, ListOpts, MetaStore, Op, Record, Tier, TierView, Txn};
use sprig_shard::home::{HomeEntry, directory_key, read_home};

/// The data-path side of a promotion, per deployment: the standby jobs,
/// the copies' write access.
#[async_trait::async_trait]
pub trait PromoteHooks: Send + Sync {
    /// Step 2: stops the tenant's standby jobs in `to` and returns
    /// `applied_source_gtid` per source cluster.
    async fn stop_standby(&self, tenant: TenantId, to: &DcId) -> Result<BTreeMap<String, String>>;
    /// Step 4: makes the tenant's copies in `to` writable (grants; for a
    /// standby that replicates, the replication stops and `read_only`
    /// clears).
    async fn make_writable(&self, tenant: TenantId, to: &DcId) -> Result<()>;
    /// Two tiers: `to`'s tier and nodes are out of this controller's reach,
    /// so the promotion stops at the commit point and `to`'s own
    /// `tenant_promote` loop does steps 2-4 and asks the global leader for
    /// step 5 ([`TwoTierHooks`]).
    fn finished_in(&self, _to: &DcId) -> bool {
        false
    }
}

/// Hooks for a controller in a two-tier deployment: promotions into its
/// own DC run here; into any other DC they stop at the commit point and
/// that DC's `tenant_promote` loop finishes them.
pub struct TwoTierHooks {
    /// The hooks for this DC.
    pub inner: Arc<dyn PromoteHooks>,
    /// This DC.
    pub me: DcId,
}

impl std::fmt::Debug for TwoTierHooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TwoTierHooks")
            .field("me", &self.me)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl PromoteHooks for TwoTierHooks {
    async fn stop_standby(&self, tenant: TenantId, to: &DcId) -> Result<BTreeMap<String, String>> {
        self.inner.stop_standby(tenant, to).await
    }
    async fn make_writable(&self, tenant: TenantId, to: &DcId) -> Result<()> {
        self.inner.make_writable(tenant, to).await
    }
    fn finished_in(&self, to: &DcId) -> bool {
        *to != self.me
    }
}

/// Hooks that do nothing (tests, and tenants whose standbys are only maps).
#[derive(Debug, Clone, Copy, Default)]
pub struct NoHooks;

#[async_trait::async_trait]
impl PromoteHooks for NoHooks {
    async fn stop_standby(&self, _t: TenantId, _to: &DcId) -> Result<BTreeMap<String, String>> {
        Ok(BTreeMap::new())
    }
    async fn make_writable(&self, _t: TenantId, _to: &DcId) -> Result<()> {
        Ok(())
    }
}

/// Global-tier writes, guarded by the global leader's token when there is
/// one.
#[derive(Clone)]
pub enum Guard {
    /// Writes commit only while this leadership holds.
    Leader(Leadership),
    /// Unguarded (single-etcd tests and tools).
    Store(Arc<dyn MetaStore>),
}

impl std::fmt::Debug for Guard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Guard::Leader(l) => write!(f, "Guard::Leader({})", l.token),
            Guard::Store(_) => f.write_str("Guard::Store"),
        }
    }
}

impl Guard {
    /// Runs `then` if `when` holds (and the token still holds).
    ///
    /// # Errors
    /// 39504 when leadership was lost; the store's errors.
    pub async fn txn(&self, when: Vec<Cmp>, then: Vec<Op>) -> Result<bool> {
        match self {
            Guard::Leader(l) => Ok(l.commit_if(when, then).await?.succeeded),
            Guard::Store(s) => Ok(s
                .txn(Txn {
                    when,
                    then,
                    otherwise: Vec::new(),
                })
                .await?
                .succeeded),
        }
    }
}

/// What [`promote`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Promoted {
    /// The tenant is homed in the target now, at this epoch.
    Done {
        /// The new epoch.
        epoch: u64,
        /// `applied_source_gtid` per source cluster.
        applied: BTreeMap<String, String>,
    },
    /// The directory changed under the decision; nothing was done.
    Skipped(String),
    /// Two tiers: committed (step 1); the target DC's `tenant_promote`
    /// loop does the rest.
    Committed {
        /// The new epoch.
        epoch: u64,
    },
}

/// Maps a DC to its tier's store.
pub type DcTiers = Arc<dyn Fn(&DcId) -> Arc<dyn MetaStore> + Send + Sync>;

/// Everything a promotion needs.
#[derive(Clone)]
pub struct Promotion {
    /// The global tier, for reads.
    pub global: Arc<dyn MetaStore>,
    /// Global writes.
    pub guard: Guard,
    /// A DC's tier (`TierView` over one etcd in single-etcd mode).
    pub dc_tier: DcTiers,
    /// The data-path hooks.
    pub hooks: Arc<dyn PromoteHooks>,
}

impl std::fmt::Debug for Promotion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Promotion")
            .field("guard", &self.guard)
            .finish_non_exhaustive()
    }
}

impl Promotion {
    /// Promotions in one etcd: every tier is a view of `store`.
    #[must_use]
    pub fn single_etcd(
        store: &Arc<dyn MetaStore>,
        guard: Guard,
        hooks: Arc<dyn PromoteHooks>,
    ) -> Self {
        let s = Arc::clone(store);
        Self {
            global: TierView::over(store, Tier::Global),
            guard,
            dc_tier: Arc::new(move |dc: &DcId| TierView::over(&s, Tier::Dc(dc.clone()))),
            hooks,
        }
    }
}

async fn put_entry(g: &Guard, e: &HomeEntry, tenant: TenantId) -> Result<bool> {
    let key = directory_key(tenant)?;
    g.txn(
        vec![Cmp::ModRev(key.clone(), e.rev)],
        vec![Op::Put {
            key,
            value: e.encode()?,
            lease: None,
        }],
    )
    .await
}

/// Steps 1-5 for `tenant`, from `from` (at directory epoch `epoch`) to
/// `to`, for failover `failover_id`.
///
/// # Errors
/// The store's errors and the hooks'; 39504 when leadership was lost. The
/// promotion is resumable: call again.
pub async fn promote(
    p: &Promotion,
    failover_id: &str,
    tenant: TenantId,
    from: &DcId,
    to: &DcId,
    epoch: u64,
) -> Result<Promoted> {
    // 1. The commit point (or a resume after it).
    let new_epoch = epoch + 1;
    let e = loop {
        let e = read_home(&*p.global, tenant).await?.ok_or_else(|| {
            SprigError::internal(format!("tenant {tenant} has no directory entry"))
        })?;
        if &e.home_dc == to && e.epoch == new_epoch {
            break e;
        }
        if &e.home_dc != from || e.epoch != epoch {
            return Ok(Promoted::Skipped(format!(
                "the directory changed: home {} epoch {}",
                e.home_dc, e.epoch
            )));
        }
        // Residency: the commit compares this entry's revision, so the
        // residency read here is the one in force at the commit point.
        if !e.residency_allows(to) {
            return Ok(Promoted::Skipped(format!(
                "residency doesn't allow {to} for the tenant"
            )));
        }
        let mut next = e.clone();
        next.raw.insert("home_dc".into(), json!(to.as_str()));
        next.raw.insert("epoch".into(), json!(new_epoch));
        next.raw.insert("state".into(), json!("failing_over"));
        next.raw.insert(
            "failover".into(),
            merge(
                e.raw.get("failover"),
                json!({"failover_id": failover_id, "from": from.as_str()}),
            ),
        );
        if put_entry(&p.guard, &next, tenant).await? {
            tracing::info!(%tenant, %from, %to, epoch = new_epoch, "tenant failover committed");
        }
    };
    if p.hooks.finished_in(to) {
        return Ok(Promoted::Committed { epoch: new_epoch });
    }
    // 2-4.
    let applied = promote_here(&*p.hooks, &*(p.dc_tier)(to), tenant, to, new_epoch).await?;
    // 5.
    if !activate(&p.guard, &*p.global, tenant, to, new_epoch, Some(e)).await? {
        return Ok(Promoted::Skipped(
            "the directory changed after the commit".into(),
        ));
    }
    Ok(Promoted::Done {
        epoch: new_epoch,
        applied,
    })
}

/// Steps 2-4 in the target DC: the standby stops (its positions are
/// returned for the record), its maps in `tier` become `home` at `epoch`,
/// the copies become writable. Idempotent.
///
/// # Errors
/// The store's errors and the hooks'.
pub async fn promote_here(
    hooks: &dyn PromoteHooks,
    tier: &dyn MetaStore,
    tenant: TenantId,
    to: &DcId,
    epoch: u64,
) -> Result<BTreeMap<String, String>> {
    // 2. Standby jobs stop; their positions go to the record.
    let applied = hooks.stop_standby(tenant, to).await?;
    // 3. The standby maps become home maps.
    promote_maps(tier, tenant, epoch).await?;
    // 4. Write access.
    hooks.make_writable(tenant, to).await?;
    Ok(applied)
}

/// Step 5: the entry becomes `active` with `auth_version + 1`, while it
/// still names `to` at `epoch`. Returns `false` when the directory moved
/// on. `seen` is an entry already read.
///
/// # Errors
/// The store's errors; 39504 when leadership was lost.
pub async fn activate(
    guard: &Guard,
    global: &dyn MetaStore,
    tenant: TenantId,
    to: &DcId,
    epoch: u64,
    seen: Option<HomeEntry>,
) -> Result<bool> {
    let mut e = match seen {
        Some(e) => e,
        None => read_home(global, tenant)
            .await?
            .ok_or_else(|| SprigError::internal("directory entry vanished"))?,
    };
    loop {
        if &e.home_dc != to || e.epoch != epoch {
            return Ok(false);
        }
        if e.state == "active" {
            return Ok(true);
        }
        let mut next = e.clone();
        let auth = e
            .raw
            .get("auth_version")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        next.raw.insert("auth_version".into(), json!(auth + 1));
        next.raw.insert("state".into(), json!("active"));
        if put_entry(guard, &next, tenant).await? {
            return Ok(true);
        }
        e = read_home(global, tenant)
            .await?
            .ok_or_else(|| SprigError::internal("directory entry vanished"))?;
    }
}

fn merge(old: Option<&Value>, add: Value) -> Value {
    let mut m = old.and_then(Value::as_object).cloned().unwrap_or_default();
    if let Value::Object(a) = add {
        m.extend(a);
    }
    Value::Object(m)
}

/// Step 3: every map of the tenant in this DC tier becomes `home` with
/// range epochs + 1 and `tenant_epoch = epoch`. Maps already at `epoch`
/// are left alone.
///
/// # Errors
/// The store's errors.
pub async fn promote_maps(dc: &dyn MetaStore, tenant: TenantId, epoch: u64) -> Result<usize> {
    let prefix = dc.tier().key(&format!("shardmaps/{tenant}/"))?;
    let page = dc.list(&prefix, ListOpts::default()).await?;
    let mut n = 0;
    for kv in page.kvs {
        let mut map = Record::<ShardMap>::decode(&kv.value)?;
        if map.body.role == MapRole::Home && map.body.tenant_epoch >= epoch {
            continue;
        }
        let m: &mut ShardMap = &mut map.body;
        m.role = MapRole::Home;
        m.tenant_epoch = epoch;
        m.map_version += 1;
        for r in &mut m.ranges {
            r.epoch += 1;
        }
        sprig_shard::map::validate(m)?;
        let r = dc
            .txn(Txn {
                when: vec![Cmp::ModRev(kv.key.clone(), kv.mod_revision)],
                then: vec![Op::Put {
                    key: kv.key.clone(),
                    value: map.encode()?,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        if r.succeeded {
            n += 1;
        }
    }
    Ok(n)
}

/// A returning DC's side (it stays fenced): every map of the tenant in this tier becomes `quarantined`.
///
/// # Errors
/// The store's errors.
pub async fn quarantine_maps(dc: &dyn MetaStore, tenant: TenantId) -> Result<usize> {
    let prefix = dc.tier().key(&format!("shardmaps/{tenant}/"))?;
    let page = dc.list(&prefix, ListOpts::default()).await?;
    let mut n = 0;
    for kv in page.kvs {
        let mut map = Record::<ShardMap>::decode(&kv.value)?;
        if map.body.role == MapRole::Quarantined {
            continue;
        }
        map.body.role = MapRole::Quarantined;
        map.body.map_version += 1;
        let r = dc
            .txn(Txn {
                when: vec![Cmp::ModRev(kv.key.clone(), kv.mod_revision)],
                then: vec![Op::Put {
                    key: kv.key.clone(),
                    value: map.encode()?,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        if r.succeeded {
            n += 1;
        }
    }
    Ok(n)
}
