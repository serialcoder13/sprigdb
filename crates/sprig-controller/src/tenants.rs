//! Tenant bootstrap and deletion (chapter 08, "Bootstrap, DDL and
//! migrations", build step 9; chapter 23, "The tenant tree").
//!
//! ```text
//! sprigctl tenant create acme --dc mum1 [--under platform] [--residency ...] [--admin admin]
//! global controller: validate name, parent and residency; allocate the id; create
//!   tenant-names/acme and tenants/<id> (state creating, versions 0) in one txn (23)
//! home DC controller, job tenant_bootstrap (idempotent, progress in sprig.bootstrap):
//!   pick a cluster (chapter 11, least busy without a hint); create shardmaps/<id>/0;
//!   CREATE DATABASE t<id>_sprig; run all migrations; insert internal keys;
//!   copy the ancestry and ancestors' settings (tree_settings reconcile);
//!   create the admin user (14) with backend grants (29); state creating → active
//! ```
//!
//! [`TenantService`] serves the Admin API's tenant calls and runs the
//! bootstrap; the [`TenantLifecycle`] loop (`tenant_bootstrap`, DC leader)
//! finishes what a crash or a lost request left behind: a tenant still
//! `creating` is bootstrapped again, and one past
//! `catalog.bootstrap_timeout_minutes` becomes `failed` with an alert
//! (chapter 08: "bootstrap past `catalog.bootstrap_timeout_minutes`
//! becomes failed"); a tenant `deleting` is cleaned up. Every step is idempotent:
//!
//! 1. **Placement.** The shard map `shardmaps/<id>/0` records the choice,
//!    written once with a `Missing` guard and the DC leader's token, so two
//!    runs agree. Interim placement until chapter 11's (M0.6): the
//!    `--cluster` asked for, else the active cluster of the DC holding the
//!    fewest catalogs, ties to the lower cluster number. The choice, its
//!    reason and the counts considered are logged as a `tenant.place`
//!    decision and kept in `sprig.bootstrap`.
//! 2. **Catalog.** Bootstrap creates `t<id>_sprig` with the tenant's
//!    owner mark as the schema's comment (`sprig tenant <id> <incarnation>`,
//!    in the same statement), then the migrator runs every module's
//!    migrations (`CREATE ... IF NOT EXISTS`, serialized by `GET_LOCK`). A
//!    schema that already exists is adopted only if it carries this
//!    tenant's mark (a re-run); any other is an earlier tenant's, and
//!    bootstrap refuses it: the catalog shard map is removed (so deleting
//!    the tenant never touches that schema) and the tenant becomes
//!    `failed` with an alert.
//! 3. **Ancestry.** The `tree_settings` reconcile (`TreeSync`) writes
//!    `sprig.ancestry`, `sprig.ancestor_versions` and the ancestors' rows,
//!    in one versioned change (`change()` doesn't check the tenant's state,
//!    so it works while `creating`).
//! 4. **Hooks** ([`BootstrapHooks`]): chapter 14's admin user and verifier
//!    tables and chapter 29's backend accounts and grants belong to other
//!    milestones; they plug in here.
//! 5. **Active.** The entry goes `creating` → `active` in a transaction
//!    guarded by its mod revision.
//!
//! Chapter 08 creates no logical database at bootstrap; the first one comes
//! from the tenant's own `CREATE DATABASE`.
//!
//! **Deletion** (chapter 23's `delete_tenant`): only a leaf that is
//! `active` or `failed`; the entry becomes `deleting` and the parent's
//! child count drops in one transaction. Cleanup ([`cleanup`]'s steps)
//! then ends the tenant's mover jobs, runs the hooks' `dropping`, waits
//! while the catalog lists live databases, removes the tenant's schemas,
//! accounts and name rows on every cluster of the DC by exact names
//! (`sprig_cluster::purge`), drops the catalog schema (left alone if it
//! carries another tenant's mark), deletes its finished job records and
//! its keys in the DC tier, and finally the entry and its name.
//!
//! **Ids are never reused** (chapter 23). Besides the directory's counter,
//! a new tenant's id is raised past every schema shaped like a tenant's
//! (`t<id>_sprig`, `t<id>_d...`) already on the DC's active clusters, so a
//! counter that was lost (a deployment rebuilt over clusters that kept an
//! earlier tenant's schemas) can't hand out an id whose schemas exist. The
//! owner mark is the backstop for a cluster that couldn't be read then.
//!
//! Where this differs from the book: chapter 08 runs bootstrap as a job
//! (`tenant_bootstrap`) through the scheduler; here the Admin API call runs
//! it inline and the loop of the same name is the crash backstop, so
//! `sprigctl tenant create` returns once the tenant is active. The
//! bootstrap deadline is measured from when this leader first saw the
//! tenant `creating` (the entry's `created_at` is for people only).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sprig_catalog::bootstrap::{
    BootstrapProgress, BootstrapStep, CatalogClaim, catalog_owner, claim_catalog, create_catalog,
    drop_catalog, live_databases, read_progress, tenant_schema_ids, write_progress,
};
use sprig_catalog::migrate::MigrateOpts;
use sprig_catalog::placement::ShardMap;
use sprig_catalog::settings::BOOTSTRAP_TIMEOUT_MINUTES;
use sprig_catalog::{CatalogEndpoint, Migrator, Module, SettingsBackend, SettingsTx, Store};
use sprig_common::ids::{ClusterId, DcId, JobId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Cmp, Consistency, ListOpts, MetaStore, Op, Record, Txn};
use sprig_settings::{LiveSettings, SettingCtx};

use crate::admin::types::{
    CreateTenantRequest, CreateTenantResponse, ListTenantsResponse, TenantView,
};
use crate::catalogs::ShardMapLocator;
use crate::monitor::leader_guard;
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler, WatchSpec};
use crate::settings::TENANT_MAX_DEPTH;
use crate::tree::{Directory, DirectoryEntry, NewTenant, StoredEntry, TenantState};
use crate::tree_settings::{SyncOutcome, TreeSync};

/// Other chapters' parts of bootstrap and deletion.
#[async_trait::async_trait]
pub trait BootstrapHooks: Send + Sync + 'static {
    /// Catalog modules migrated with the core module (chapter 14's
    /// verifier tables, chapter 29's privileges, ...).
    fn modules(&self) -> Vec<Module> {
        Vec::new()
    }

    /// After the catalog and the ancestry: chapter 14's admin user,
    /// chapter 29's backend accounts and grants. Must be idempotent.
    ///
    /// # Errors
    /// The hook's error; bootstrap retries.
    async fn catalog_ready(
        &self,
        _tenant: &DirectoryEntry,
        _catalog: &CatalogEndpoint,
    ) -> Result<()> {
        Ok(())
    }

    /// Before a deleted tenant's catalog is dropped (`None` if it never
    /// got one): chapter 29 removes backend accounts. Must be idempotent.
    ///
    /// # Errors
    /// The hook's error; cleanup retries.
    async fn dropping(
        &self,
        _tenant: &DirectoryEntry,
        _catalog: Option<&CatalogEndpoint>,
    ) -> Result<()> {
        Ok(())
    }
}

/// No hooks: what M0.3 has.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoHooks;

impl BootstrapHooks for NoHooks {}

/// The Admin API's tenant calls (`sprig.admin.v1.Tenants`).
#[async_trait::async_trait]
pub trait TenantAdmin: Send + Sync {
    /// Creates a tenant and bootstraps it; re-running for a tenant still
    /// `creating` continues its bootstrap. `guard` is the DC leader's.
    ///
    /// # Errors
    /// As [`TenantService::create`].
    async fn create(
        &self,
        req: CreateTenantRequest,
        guard: Vec<Cmp>,
    ) -> Result<CreateTenantResponse>;
    /// Every tenant.
    ///
    /// # Errors
    /// The store's errors.
    async fn list(&self) -> Result<ListTenantsResponse>;
    /// One tenant by name, with its ancestry and catalog cluster.
    ///
    /// # Errors
    /// 39072 for an unknown name.
    async fn show(&self, name: &str) -> Result<TenantView>;
    /// Deletes a leaf tenant: `deleting`, then cleanup.
    ///
    /// # Errors
    /// As [`TenantService::delete`].
    async fn delete(&self, name: &str, guard: Vec<Cmp>) -> Result<TenantView>;
}

/// What a bootstrap run ended with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bootstrapped {
    /// The entry after the run.
    pub entry: DirectoryEntry,
    /// The catalog's cluster.
    pub cluster: ClusterId,
    /// Why it went there.
    pub placement: String,
}

/// Bootstraps, shows and deletes tenants homed in this DC.
pub struct TenantService<B: SettingsBackend>
where
    B::Tx: SettingsTx,
{
    directory: Directory,
    dc_store: Arc<dyn MetaStore>,
    dc: DcId,
    locator: ShardMapLocator,
    tree: Arc<TreeSync<B>>,
    hooks: Arc<dyn BootstrapHooks>,
    migrator: Migrator,
    clock: Arc<dyn Clock>,
    live: Option<LiveSettings>,
}

impl<B: SettingsBackend> std::fmt::Debug for TenantService<B>
where
    B::Tx: SettingsTx,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantService")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

fn placement_unavailable(message: String) -> SprigError {
    SprigError::new(SprigCode::PlacementUnavailable, message)
}

/// The mark a tenant's catalog schema carries as its comment: the id and
/// this creation's incarnation (`created_at` for entries made before
/// incarnations existed).
fn owner_mark(e: &DirectoryEntry) -> String {
    let incarnation = if e.incarnation.is_empty() {
        &e.created_at
    } else {
        &e.incarnation
    };
    format!("sprig tenant {} {incarnation}", e.tenant_id)
}

impl<B: SettingsBackend> TenantService<B>
where
    B::Tx: SettingsTx,
{
    /// The service of DC `dc_store`'s tier: catalogs found with `locator`,
    /// ancestry copied by `tree` (whose directory is used), `hooks` for
    /// other chapters, `live` for `tenant.max_depth` and the bootstrap
    /// deadline (defaults without it).
    ///
    /// # Errors
    /// `Internal` if `dc_store` isn't a DC tier or a hook's module is
    /// invalid.
    pub fn new(
        dc_store: Arc<dyn MetaStore>,
        locator: ShardMapLocator,
        tree: Arc<TreeSync<B>>,
        hooks: Arc<dyn BootstrapHooks>,
        clock: Arc<dyn Clock>,
        live: Option<LiveSettings>,
    ) -> Result<Self> {
        let sprig_meta::Tier::Dc(dc) = dc_store.tier().clone() else {
            return Err(SprigError::internal(
                "tenant bootstrap needs a DC tier store",
            ));
        };
        let migrator = Migrator::new(&hooks.modules())?.with_clock(Arc::clone(&clock));
        Ok(Self {
            directory: tree.directory().clone(),
            dc_store,
            dc,
            locator,
            tree,
            hooks,
            migrator,
            clock,
            live,
        })
    }

    /// The directory.
    #[must_use]
    pub fn directory(&self) -> &Directory {
        &self.directory
    }

    /// The DC.
    #[must_use]
    pub fn dc(&self) -> &DcId {
        &self.dc
    }

    fn max_depth(&self) -> u32 {
        self.live.as_ref().map_or(TENANT_MAX_DEPTH.default, |l| {
            l.get(&TENANT_MAX_DEPTH, &SettingCtx::process(&self.dc))
        })
    }

    fn bootstrap_timeout(&self) -> Duration {
        let m = self
            .live
            .as_ref()
            .map_or(BOOTSTRAP_TIMEOUT_MINUTES.default, |l| {
                l.get(&BOOTSTRAP_TIMEOUT_MINUTES, &SettingCtx::process(&self.dc))
            });
        Duration::from_secs(m.saturating_mul(60))
    }

    /// Creates `req.name` in the directory (chapter 23) and bootstraps it
    /// here. A tenant of that name still `creating` under the same parent
    /// is bootstrapped again (a re-run after a crash); one already `active`
    /// is returned with `created: false`.
    ///
    /// # Errors
    /// 39792 for a bad name; 39360 if the name belongs to another tenant;
    /// 39072, 39303, 39508 or 39509 for the parent; 39507 when the
    /// parent's residency doesn't allow this DC; 39120 if no cluster can
    /// take the catalog; 39504 off the leader; catalog errors.
    pub async fn create(
        &self,
        req: &CreateTenantRequest,
        guard: &[Cmp],
    ) -> Result<CreateTenantResponse> {
        let parent = match &req.parent {
            Some(p) => Some(self.directory.require(p).await?),
            None => None,
        };
        let parent_id = parent.as_ref().map(|p| p.body().tenant_id);
        let (entry, created) = if let Some(e) = self.directory.by_name(&req.name).await? {
            let b = e.body();
            let same = b.parent() == parent_id && b.home_dc == self.dc;
            if !same || !matches!(b.state, TenantState::Creating | TenantState::Active) {
                return Err(SprigError::new(
                    SprigCode::TenantExists,
                    format!("a tenant named {:?} exists ({})", req.name, b.state),
                ));
            }
            (e, false)
        } else {
            if let Some(c) = &req.cluster
                && !self.locator.pools().placeable().await?.contains(c)
            {
                return Err(placement_unavailable(format!(
                    "cluster {c} is not an active cluster of dc {}",
                    self.dc
                )));
            }
            // Chapter 26: a child's residency lies within its parent's;
            // its home must be one of the parent's allowed DCs.
            let inherited = match &parent {
                Some(p) => match sprig_shard::home::read_home(
                    &**self.directory.store(),
                    p.body().tenant_id,
                )
                .await?
                {
                    Some(pe) => crate::residency::child_block(
                        &p.body().name,
                        &pe.raw,
                        &self.dc,
                        self.clock
                            .wall()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
                    )?,
                    None => None,
                },
                None => None,
            };
            self.reserve_ids_past_tenant_schemas().await?;
            let new = NewTenant {
                name: req.name.clone(),
                parent: parent_id,
                home_dc: self.dc.clone(),
                placement_hint: req.cluster.clone(),
            };
            let e = self
                .directory
                .create(&new, self.max_depth(), self.clock.wall())
                .await?;
            if let Some(b) = &inherited {
                crate::residency::write_block(&**self.directory.store(), e.body().tenant_id, b)
                    .await?;
            }
            (e, true)
        };
        let done = self.bootstrap(entry.body().tenant_id, guard).await?;
        let tenant = self.show_id(done.entry.tenant_id).await?;
        Ok(CreateTenantResponse {
            created,
            tenant,
            placement: Some(done.placement),
        })
    }

    /// Runs (or continues) `id`'s bootstrap; see the module docs.
    ///
    /// # Errors
    /// 39072 for a tenant that isn't `creating` or `active` here; 39120 if
    /// no cluster can take the catalog; 39504 off the leader; catalog
    /// errors (retryable ones are retried by the loop).
    pub async fn bootstrap(&self, id: TenantId, guard: &[Cmp]) -> Result<Bootstrapped> {
        let entry =
            self.directory.get(id).await?.ok_or_else(|| {
                SprigError::new(SprigCode::TenantNotReady, format!("no tenant {id}"))
            })?;
        let b = entry.body().clone();
        if b.home_dc != self.dc {
            return Err(SprigError::new(
                SprigCode::TenantNotReady,
                format!(
                    "tenant {} is homed in {}, not {}",
                    b.name, b.home_dc, self.dc
                ),
            ));
        }
        if !matches!(b.state, TenantState::Creating | TenantState::Active) {
            return Err(SprigError::new(
                SprigCode::TenantNotReady,
                format!("tenant {} is {}", b.name, b.state),
            ));
        }
        let (cluster, placement, considered) = self.place(&b, guard).await?;
        if b.state == TenantState::Active {
            return Ok(Bootstrapped {
                entry: b,
                cluster,
                placement,
            });
        }
        let pool = self.locator.pools().writer(&cluster).await?;
        let store = Store::new(self.locator.schema(id));
        let mark = owner_mark(&b);
        match claim_catalog(&pool, store.schema(), &mark).await? {
            CatalogClaim::Created | CatalogClaim::Ours => {}
            CatalogClaim::Foreign(found) => {
                return Err(self
                    .refuse_foreign_catalog(&b, &cluster, store.schema(), &found, guard)
                    .await);
            }
        }
        let opts = MigrateOpts {
            applied_by: format!("controller:{}", self.dc),
            ..MigrateOpts::default()
        };
        create_catalog(&pool, store.schema(), &self.migrator, &opts).await?;
        let progress = |step| {
            write_progress(
                &pool,
                &store,
                step,
                &cluster,
                &placement,
                &considered,
                self.clock.wall(),
            )
        };
        progress(BootstrapStep::Migrated).await?;
        match self.tree.sync_entry(&b).await? {
            SyncOutcome::Current | SyncOutcome::Copied { .. } => {}
            SyncOutcome::Behind(x) => {
                return Err(SprigError::new(
                    SprigCode::CatalogUnavailable,
                    format!("ancestor {x}'s catalog is behind its directory; try again"),
                ));
            }
            SyncOutcome::Blocked(why) => {
                return Err(SprigError::new(SprigCode::TenantNotReady, why));
            }
        }
        progress(BootstrapStep::Ancestry).await?;
        let ep = CatalogEndpoint {
            pool: pool.clone(),
            store: store.clone(),
        };
        self.hooks.catalog_ready(&b, &ep).await?;
        progress(BootstrapStep::Hooks).await?;
        let after = self
            .directory
            .update(id, |e| {
                if e.state != TenantState::Creating {
                    return Ok(false);
                }
                e.state = TenantState::Active;
                e.placement_hint = None;
                Ok(true)
            })
            .await?;
        progress(BootstrapStep::Done).await?;
        tracing::info!(tenant = %id, name = %b.name, %cluster, placement, "tenant bootstrapped");
        Ok(Bootstrapped {
            entry: after.body().clone(),
            cluster,
            placement,
        })
    }

    /// Raises the directory's id counter past every schema shaped like a
    /// tenant's on the DC's active clusters (module docs). A cluster that can't be read in
    /// 5 s is skipped with a warning: the owner mark still keeps its
    /// schemas from being adopted.
    ///
    /// # Errors
    /// The directory's errors.
    async fn reserve_ids_past_tenant_schemas(&self) -> Result<()> {
        // The naming around the id: `t` and `_sprig` in production.
        let probe = TenantId(u64::MAX);
        let name = self.locator.schema(probe);
        let Some((prefix, suffix)) = name.split_once(&probe.0.to_string()) else {
            return Ok(());
        };
        let mut highest = 0;
        for c in self.locator.pools().placeable().await? {
            let found = tokio::time::timeout(Duration::from_secs(5), async {
                let pool = self.locator.pools().writer(&c).await?;
                tenant_schema_ids(&pool, prefix, suffix).await
            })
            .await;
            match found {
                Ok(Ok(ids)) => highest = ids.into_iter().fold(highest, u64::max),
                Ok(Err(e)) => tracing::warn!(cluster = %c, error = %e,
                    "couldn't list tenant schemas before allocating a tenant id"),
                Err(_) => tracing::warn!(cluster = %c,
                    "listing tenant schemas before allocating a tenant id timed out"),
            }
        }
        if highest > 0 {
            self.directory.reserve_ids_above(highest).await?;
        }
        Ok(())
    }

    /// Bootstrap met an earlier tenant's catalog schema under this
    /// tenant's id: removes the catalog shard map (so cleanup never drops
    /// that schema), marks the tenant `failed` with an alert, and returns
    /// the error for the caller. The map goes first: a crash in between
    /// leaves a `creating` tenant, whose next run refuses again.
    async fn refuse_foreign_catalog(
        &self,
        b: &DirectoryEntry,
        cluster: &ClusterId,
        schema: &str,
        found: &str,
        guard: &[Cmp],
    ) -> SprigError {
        let why = format!(
            "the catalog schema {schema} of tenant {} already exists on cluster {cluster} and \
             isn't this tenant's (its mark is {found:?}): an earlier tenant with id {} left it. \
             Check and drop it, then delete this tenant and create it again (it gets a new id)",
            b.name, b.tenant_id
        );
        let unmap = async {
            let key = ShardMap::catalog_key(self.dc_store.tier(), b.tenant_id)?;
            if let Some(kv) = self.dc_store.get(&key, Consistency::Linearizable).await? {
                let mut when = vec![Cmp::ModRev(key.clone(), kv.mod_revision)];
                when.extend_from_slice(guard);
                let r = self
                    .dc_store
                    .txn(Txn {
                        when,
                        then: vec![Op::Delete(key)],
                        otherwise: Vec::new(),
                    })
                    .await?;
                if !r.succeeded {
                    return Err(SprigError::new(
                        SprigCode::NotLeader,
                        "this controller no longer leads the DC",
                    ));
                }
            }
            self.fail(b.tenant_id, &why).await
        };
        if let Err(e) = unmap.await {
            tracing::warn!(tenant = %b.tenant_id, error = %e,
                "couldn't mark a tenant failed after refusing a foreign catalog; the loop will retry");
        }
        tracing::error!(alert = "TenantCatalogNotOwned", tenant = %b.tenant_id, name = %b.name,
            %cluster, schema, mark = found, "tenant bootstrap refused an existing catalog schema");
        SprigError::new(SprigCode::TenantNotReady, why)
    }

    /// Step 1: the recorded placement, or a new one written once.
    async fn place(
        &self,
        b: &DirectoryEntry,
        guard: &[Cmp],
    ) -> Result<(ClusterId, String, Vec<(ClusterId, u64)>)> {
        let key = ShardMap::catalog_key(self.dc_store.tier(), b.tenant_id)?;
        if let Some(c) = self.locator.cluster_of(b.tenant_id).await? {
            let p = self.recorded_placement(b.tenant_id, &c).await;
            return Ok((
                c,
                p.as_ref()
                    .map_or_else(|| "recorded".to_owned(), |p| p.placement.clone()),
                p.map(|p| p.considered).unwrap_or_default(),
            ));
        }
        if b.state == TenantState::Active {
            return Err(SprigError::internal(format!(
                "active tenant {} has no catalog shard map in {}",
                b.name, self.dc
            )));
        }
        let placeable = self.locator.pools().placeable().await?;
        let counts = self.catalogs_per_cluster().await?;
        let considered: Vec<(ClusterId, u64)> = placeable
            .iter()
            .map(|c| (c.clone(), counts.get(c).copied().unwrap_or(0)))
            .collect();
        let (cluster, reason) = match &b.placement_hint {
            Some(h) if placeable.contains(h) => (h.clone(), "requested"),
            Some(h) => {
                return Err(placement_unavailable(format!(
                    "cluster {h} is not an active cluster of dc {}",
                    self.dc
                )));
            }
            None => {
                let best = considered
                    .iter()
                    .enumerate()
                    .min_by_key(|(i, (_, n))| (*n, *i))
                    .map(|(_, (c, _))| c.clone())
                    .ok_or_else(|| {
                        placement_unavailable(format!(
                            "dc {} has no active cluster for tenant {}'s catalog",
                            self.dc, b.name
                        ))
                    })?;
                (best, "fewest_tenants")
            }
        };
        let map = ShardMap::catalog(b.tenant_id, cluster.clone(), b.epoch);
        let mut when = vec![Cmp::Missing(key.clone())];
        when.extend_from_slice(guard);
        let r = self
            .dc_store
            .txn(Txn {
                when,
                then: vec![Op::Put {
                    key,
                    value: Record::new(map).encode()?,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        if !r.succeeded {
            // Another run placed it first, or this controller lost the lead.
            return match self.locator.cluster_of(b.tenant_id).await? {
                Some(c) => Ok((c, "recorded".to_owned(), Vec::new())),
                None => Err(SprigError::new(
                    SprigCode::NotLeader,
                    "this controller no longer leads the DC",
                )),
            };
        }
        tracing::info!(event = "decision", component = "controller", kind = "tenant.place",
            tenant = %b.tenant_id, name = %b.name, %cluster, reason, considered = ?considered,
            "catalog placed");
        Ok((cluster, reason.to_owned(), considered))
    }

    async fn recorded_placement(&self, id: TenantId, c: &ClusterId) -> Option<BootstrapProgress> {
        let pool = self.locator.pools().writer(c).await.ok()?;
        read_progress(&pool, &Store::new(self.locator.schema(id)))
            .await
            .ok()
            .flatten()
    }

    /// Catalog shard maps per cluster in this DC.
    async fn catalogs_per_cluster(&self) -> Result<HashMap<ClusterId, u64>> {
        let prefix = self.dc_store.tier().key("shardmaps/")?;
        let mut out: HashMap<ClusterId, u64> = HashMap::new();
        let mut opts = ListOpts::default();
        loop {
            let page = self.dc_store.list(&prefix, opts.clone()).await?;
            for kv in &page.kvs {
                if !kv.key.as_str().ends_with("/0") {
                    continue;
                }
                if let Ok(m) = Record::<ShardMap>::decode(&kv.value)
                    && let Some(c) = m.body.catalog_cluster()
                {
                    *out.entry(c.clone()).or_default() += 1;
                }
            }
            if !page.more {
                return Ok(out);
            }
            opts.at = Some(page.revision);
            opts.after = page.kvs.last().map(|kv| kv.key.clone());
        }
    }

    /// Marks a bootstrap that ran past its deadline `failed`.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn fail(&self, id: TenantId, why: &str) -> Result<()> {
        let after = self
            .directory
            .update(id, |e| {
                if e.state != TenantState::Creating {
                    return Ok(false);
                }
                e.state = TenantState::Failed;
                Ok(true)
            })
            .await?;
        if after.body().state == TenantState::Failed {
            tracing::error!(alert = "TenantBootstrapFailed", tenant = %id,
                name = %after.body().name, why, "tenant bootstrap failed");
        }
        Ok(())
    }

    /// Deletes `name`: a leaf tenant that is `active` or `failed` becomes
    /// `deleting`, then cleanup runs (here, and in the loop until it
    /// finishes).
    ///
    /// # Errors
    /// 39072 for an unknown tenant, or one that isn't `active` or
    /// `failed`; 39362 while it has children; the store's errors.
    pub async fn delete(&self, name: &str, guard: &[Cmp]) -> Result<TenantView> {
        let e = self.directory.require(name).await?;
        let id = e.body().tenant_id;
        if e.body().home_dc != self.dc {
            return Err(SprigError::new(
                SprigCode::TenantNotReady,
                format!(
                    "tenant {name} is homed in {}; delete it there",
                    e.body().home_dc
                ),
            ));
        }
        let marked = self.directory.mark_deleting(id).await?;
        let view = Self::view(&marked, None, Vec::new(), None);
        if let Err(err) = self.cleanup(id, guard).await {
            tracing::warn!(tenant = %id, error = %err, "tenant cleanup will be retried by the loop");
        }
        Ok(view)
    }

    /// Cleans up a `deleting` tenant; see the module docs and
    /// [`cleanup`]'s steps.
    ///
    /// # Errors
    /// 39072 while the catalog lists live databases or the tenant's mover
    /// jobs are still ending; 39504 when `guard` fails; the store's,
    /// catalog's and clusters' errors.
    pub async fn cleanup(&self, id: TenantId, guard: &[Cmp]) -> Result<()> {
        let Some(e) = self.directory.get(id).await? else {
            return Ok(());
        };
        if e.body().state != TenantState::Deleting {
            return Ok(());
        }
        // 1. Mover jobs end first: their copies and retained sources are
        //    dropped by the jobs themselves.
        let jobs = sprig_mover::JobStore::new(Arc::clone(&self.dc_store), Arc::clone(&self.clock));
        let settled = cleanup::settle_jobs(&jobs, id).await?;
        if !settled.open.is_empty() {
            return Err(SprigError::new(
                SprigCode::TenantNotReady,
                format!(
                    "waiting for {} mover jobs of the deleted tenant to end",
                    settled.open.len()
                ),
            ));
        }
        // 2. The catalog: hooks, the live-database guard, its database ids.
        let mark = owner_mark(e.body());
        let catalog_cluster = self.locator.cluster_of(id).await?;
        let mut catalog: Option<CatalogEndpoint> = None;
        let mut db_ids = std::collections::BTreeSet::new();
        match &catalog_cluster {
            Some(c) => {
                let ep = CatalogEndpoint {
                    pool: self.locator.pools().writer(c).await?,
                    store: Store::new(self.locator.schema(id)),
                };
                self.hooks.dropping(e.body(), Some(&ep)).await?;
                match catalog_owner(&ep.pool, ep.store.schema()).await? {
                    // Another tenant's mark: never this tenant's to drop.
                    Some(found) if !found.is_empty() && found != mark => {
                        tracing::warn!(tenant = %id, schema = ep.store.schema(), mark = found,
                            "the catalog schema carries another tenant's mark; left in place");
                    }
                    // Ours, unmarked (made before marks existed), or gone.
                    _ => {
                        let live = live_databases(&ep.pool, &ep.store).await?;
                        if live > 0 {
                            return Err(SprigError::new(
                                SprigCode::TenantNotReady,
                                format!("the tenant still has {live} databases; drop them first"),
                            ));
                        }
                        db_ids = cleanup::catalog_db_ids(&ep.pool, ep.store.schema()).await?;
                        catalog = Some(ep);
                    }
                }
            }
            None => self.hooks.dropping(e.body(), None).await?,
        }
        // 3. Every cluster of the DC, by exact names. Only under the real
        //    naming: a test's catalog naming says nothing about who owns
        //    `t<id>_` names on a shared server.
        if self.locator.schema(id) == SchemaName::catalog(id).to_string() {
            let finished: Vec<JobId> = settled.finished.iter().map(|(r, _)| r.job_id).collect();
            for c in self.locator.pools().sweepable().await? {
                let pool = self.locator.pools().writer(&c).await?;
                let mut conn = crate::catalogs::acquire(&pool, "tenant cleanup").await?;
                let known = sprig_cluster::purge::Known {
                    owner_mark: Some(mark.clone()),
                    drop_schemas: true,
                    db_ids: db_ids.clone(),
                    keep_catalog: catalog_cluster.as_ref() == Some(&c),
                    jobs: finished.clone(),
                };
                let p = sprig_cluster::purge::purge(&mut conn, id, &known).await?;
                drop(conn);
                if let Some(found) = &p.foreign {
                    tracing::warn!(alert = "TenantCleanupForeignCatalog", tenant = %id, cluster = %c, mark = found,
                        "another tenant with this id has a catalog here; its names were left alone");
                    continue;
                }
                for (schema, why) in &p.left {
                    tracing::warn!(tenant = %id, cluster = %c, schema, why, "a schema of the deleted tenant was left in place");
                }
                if !p.is_empty() {
                    tracing::info!(event = "tenant.cleanup", tenant = %id, cluster = %c,
                        schemas = ?p.drop_schemas,
                        accounts = ?p.drop_accounts.iter().map(|a| a.user.as_str()).collect::<Vec<_>>(),
                        names = p.delete_names.len(), jobs = p.jobs.len(),
                        "deleted tenant's objects removed");
                }
            }
        }
        // 4. The catalog itself.
        if let Some(ep) = &catalog {
            drop_catalog(&ep.pool, &ep.store).await?;
        }
        // 5. Finished job records, then the tenant's keys in the DC tier
        //    (the catalog's shard map among them).
        for (rec, rev) in &settled.finished {
            if !jobs.remove_finished(rec, *rev, guard).await? {
                return Err(SprigError::new(
                    SprigCode::NotLeader,
                    "a job record changed, or this controller no longer leads the DC",
                ));
            }
        }
        let n = cleanup::purge_meta(&*self.dc_store, id, guard).await?;
        tracing::info!(event = "tenant.cleanup", tenant = %id, keys = n,
            jobs = settled.finished.len(), "deleted tenant's records removed");
        // 6. The entry and its name.
        self.directory.remove(&e).await
    }

    fn view(
        e: &StoredEntry,
        parent: Option<String>,
        ancestry: Vec<String>,
        cluster: Option<ClusterId>,
    ) -> TenantView {
        let b = e.body();
        TenantView {
            tenant_id: b.tenant_id,
            name: b.name.clone(),
            state: b.state.as_str().to_owned(),
            home_dc: b.home_dc.clone(),
            parent_tenant_id: b.parent_tenant_id,
            parent,
            path: b.path.clone(),
            depth: b.depth,
            child_count: b.child_count,
            catalog_version: b.catalog_version,
            auth_version: b.auth_version,
            settings_version: b.settings_version,
            cluster,
            ancestry,
            created_at: b.created_at.clone(),
            mod_revision: e.mod_revision,
        }
    }

    async fn show_id(&self, id: TenantId) -> Result<TenantView> {
        let e =
            self.directory.get(id).await?.ok_or_else(|| {
                SprigError::new(SprigCode::TenantNotReady, format!("no tenant {id}"))
            })?;
        let ancestry: Vec<String> = self
            .directory
            .ancestry(e.body())
            .await?
            .into_iter()
            .map(|(r, _)| r.name)
            .collect();
        let cluster = if e.body().home_dc == self.dc {
            self.locator.cluster_of(id).await?
        } else {
            None
        };
        Ok(Self::view(&e, ancestry.last().cloned(), ancestry, cluster))
    }
}

#[async_trait::async_trait]
impl<B: SettingsBackend> TenantAdmin for TenantService<B>
where
    B::Tx: SettingsTx,
{
    async fn create(
        &self,
        req: CreateTenantRequest,
        guard: Vec<Cmp>,
    ) -> Result<CreateTenantResponse> {
        TenantService::create(self, &req, &guard).await
    }

    async fn list(&self) -> Result<ListTenantsResponse> {
        let entries = self.directory.list().await?;
        let names: BTreeMap<u64, String> = entries
            .iter()
            .map(|e| (e.body().tenant_id.0, e.body().name.clone()))
            .collect();
        Ok(ListTenantsResponse {
            tenants: entries
                .iter()
                .map(|e| {
                    let parent = names.get(&e.body().parent_tenant_id).cloned();
                    Self::view(e, parent, Vec::new(), None)
                })
                .collect(),
        })
    }

    async fn show(&self, name: &str) -> Result<TenantView> {
        let e = self.directory.require(name).await?;
        self.show_id(e.body().tenant_id).await
    }

    async fn delete(&self, name: &str, guard: Vec<Cmp>) -> Result<TenantView> {
        TenantService::delete(self, name, &guard).await
    }
}

/// The `tenant_bootstrap` loop: finishes or fails `creating` tenants and
/// cleans up `deleting` ones homed in this DC.
pub struct TenantLifecycle<B: SettingsBackend>
where
    B::Tx: SettingsTx,
{
    service: Arc<TenantService<B>>,
    first_seen: Mutex<HashMap<TenantId, Instant>>,
}

impl<B: SettingsBackend> std::fmt::Debug for TenantLifecycle<B>
where
    B::Tx: SettingsTx,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantLifecycle")
            .field("dc", self.service.dc())
            .finish_non_exhaustive()
    }
}

impl<B: SettingsBackend> TenantLifecycle<B>
where
    B::Tx: SettingsTx,
{
    /// The loop over `service`.
    #[must_use]
    pub fn new(service: Arc<TenantService<B>>) -> Self {
        Self {
            service,
            first_seen: Mutex::new(HashMap::new()),
        }
    }

    fn wants(&self, e: &DirectoryEntry) -> bool {
        e.home_dc == *self.service.dc()
            && matches!(e.state, TenantState::Creating | TenantState::Deleting)
    }

    fn first_seen(&self) -> std::sync::MutexGuard<'_, HashMap<TenantId, Instant>> {
        self.first_seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

#[async_trait::async_trait]
impl<B: SettingsBackend> Reconciler for TenantLifecycle<B>
where
    B::Tx: SettingsTx,
{
    type Key = TenantId;

    fn name(&self) -> &'static str {
        "tenant_bootstrap"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn watches(&self) -> Vec<WatchSpec> {
        vec![WatchSpec::global("tenants/")]
    }

    fn workers(&self) -> usize {
        4
    }

    fn keys_for(&self, ev: &MetaEvent) -> Vec<TenantId> {
        let Some(kv) = &ev.kv else {
            return Vec::new();
        };
        match StoredEntry::decode(kv) {
            Ok(e) if self.wants(e.body()) => vec![e.body().tenant_id],
            _ => Vec::new(),
        }
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<TenantId>> {
        Ok(self
            .service
            .directory()
            .list()
            .await?
            .iter()
            .map(StoredEntry::body)
            .filter(|e| self.wants(e))
            .map(|e| e.tenant_id)
            .collect())
    }

    async fn reconcile(&self, cx: &LoopCx, key: &TenantId) -> Result<Outcome> {
        let Some(e) = self.service.directory().get(*key).await? else {
            self.first_seen().remove(key);
            return Ok(Outcome::Done);
        };
        if !self.wants(e.body()) {
            self.first_seen().remove(key);
            return Ok(Outcome::Done);
        }
        let guard = leader_guard(cx)?;
        match e.body().state {
            TenantState::Creating => {
                let since = *self.first_seen().entry(*key).or_insert_with(Instant::now);
                if since.elapsed() > self.service.bootstrap_timeout() {
                    self.service
                        .fail(*key, "past catalog.bootstrap_timeout_minutes")
                        .await?;
                    self.first_seen().remove(key);
                    return Ok(Outcome::Done);
                }
                match self.service.bootstrap(*key, &guard).await {
                    Ok(_) => {
                        self.first_seen().remove(key);
                        Ok(Outcome::Done)
                    }
                    Err(err) if err.code() == Some(SprigCode::PlacementUnavailable) => {
                        tracing::warn!(tenant = %key, error = %err, "tenant bootstrap waits for a cluster");
                        Ok(Outcome::Blocked {
                            reason: "no cluster can take the catalog",
                        })
                    }
                    Err(err) => Err(err),
                }
            }
            TenantState::Deleting => match self.service.cleanup(*key, &guard).await {
                Ok(()) => Ok(Outcome::Done),
                Err(err) if err.code() == Some(SprigCode::TenantNotReady) => {
                    tracing::warn!(tenant = %key, error = %err, "tenant cleanup waits");
                    Ok(Outcome::Blocked {
                        reason: "the deleted tenant still has databases or open mover jobs",
                    })
                }
                Err(err) => Err(err),
            },
            _ => Ok(Outcome::Done),
        }
    }
}

mod cleanup;

#[cfg(test)]
mod tests;
