//! The queue engine's footing in the gateway (chapter 50, build steps
//! 2–10 and 18): [`GatewayMq`] is `sprig-queues`' [`MqCatalog`] over the
//! tenant catalog, the shard maps and chapter 29's grants, and
//! [`EtcdLeases`] its owner leases in this DC's etcd tier.
//!
//! - **Vhosts.** `/` is `amqp.default_vhost_database`, any other vhost the
//!   database of the same name; a database the tenant lacks is 39250.
//! - **The model** is the `mq` module of the tenant's catalog snapshot;
//!   declares are versioned catalog changes, and this gateway reads its
//!   own change at once.
//! - **Messages** live in the vhost database's first shard schema, on the
//!   tenant's cluster, written through the cluster's `sprig_ctl` pool.
//! - **Privileges** (step 18): `RabbitMQ`'s configure, write and read map to
//!   grants on the database or on the object's name (`CREATE`/`DROP`/
//!   `ALTER`; `INSERT`; `SELECT`, and `DELETE` for queues), or a rule in
//!   `mq_permissions`. A user with none of them on the vhost's database
//!   can't open it.
//! - **Leases**: `/sprig/dc/<dc_id>/queue-owners/<tenant_id>/<object_id>`,
//!   bound to one 10-second lease of this gateway kept alive every 3 s; a
//!   lost lease stops every owner of this gateway (their writes would be
//!   fenced anyway). Other gateways read the holder to relay to it
//!   ([`super::peer`]).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sprig_auth::authz::{Action, SessionGrants, Target};
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::ObjectKind;
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogLocator, Freshness, TenantCatalog};
use sprig_common::CancellationToken;
use sprig_common::ids::{DbId, TenantId};
use sprig_meta::{Cmp, Key, LeaseId, MetaStore, Op, OpResult, Txn};
use sprig_proto_amqp::auth::Principal;
use sprig_proto_amqp::error::{MqCode, MqError};
use sprig_proto_amqp::mem::Perm;
use sprig_queues::catalog::{MqState, MqStore};
use sprig_queues::engine::{Acquired, Leases, MqCatalog, TopoChange, VhostRef};
use sprig_queues::maria::MariaStore;
use sprig_queues::settings::{self as qs, QueueSettings};
use sprig_queues::store::MsgStore;
use sprig_queues::token::TokenKey;
use sprig_queues::topology::Topology;
use sprig_settings::{LiveSettings, SettingCtx};

use crate::tenant::TenantServices;

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn unavailable(e: impl std::fmt::Display) -> MqError {
    MqError::new(MqCode::OwnerMoved, format!("catalog unavailable: {e}"))
}

type GrantKey = (u64, u64, u64, u64);

/// `sprig-queues`' catalog side over the tenant services.
pub struct GatewayMq {
    svc: Arc<TenantServices>,
    live: LiveSettings,
    migrated: Mutex<HashSet<u64>>,
    keys: Mutex<HashMap<u64, Arc<TokenKey>>>,
    stores: tokio::sync::Mutex<HashMap<(u64, u32), Arc<dyn MsgStore>>>,
    /// The same stores as MariaDB stores (their confirm mode).
    maria: Mutex<HashMap<(u64, u32), Arc<MariaStore>>>,
    /// When each tenant's catalog was last refreshed for routing.
    refreshed: Mutex<HashMap<u64, std::time::Instant>>,
    grants: Mutex<HashMap<GrantKey, Arc<dyn SessionGrants>>>,
    /// Each cluster's feed-forwarder beat as last read (bugfix-42).
    beats: Mutex<HashMap<String, (std::time::Instant, bool)>>,
}

impl std::fmt::Debug for GatewayMq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayMq").finish_non_exhaustive()
    }
}

impl GatewayMq {
    /// The catalog side over `svc`.
    #[must_use]
    pub fn new(svc: Arc<TenantServices>, live: LiveSettings) -> Self {
        Self {
            svc,
            live,
            migrated: Mutex::new(HashSet::new()),
            keys: Mutex::new(HashMap::new()),
            stores: tokio::sync::Mutex::new(HashMap::new()),
            maria: Mutex::new(HashMap::new()),
            refreshed: Mutex::new(HashMap::new()),
            grants: Mutex::new(HashMap::new()),
            beats: Mutex::new(HashMap::new()),
        }
    }

    async fn catalog(&self, tenant: u64, fresh: Freshness) -> Result<Arc<TenantCatalog>, MqError> {
        self.svc
            .cache
            .get(TenantId(tenant), fresh)
            .await
            .map_err(unavailable)
    }

    fn state(cat: &TenantCatalog) -> MqState {
        cat.ext
            .get::<MqState>(sprig_queues::catalog::MODULE)
            .cloned()
            .unwrap_or_default()
    }

    /// The `mq` tables and the token key, once per tenant per process.
    async fn prepare(&self, tenant: u64) -> Result<(), MqError> {
        if lock(&self.migrated).contains(&tenant) {
            return Ok(());
        }
        let ep = self
            .svc
            .locator
            .writer(TenantId(tenant))
            .await
            .map_err(unavailable)?;
        let mut conn = ep.pool.get_conn().await.map_err(unavailable)?;
        let schema = ep.store.schema().to_owned();
        sprig_queues::catalog::migrate(&mut conn, &schema)
            .await
            .map_err(unavailable)?;
        let secret = MqStore::new(&schema)
            .token_secret(&mut conn)
            .await
            .map_err(unavailable)?;
        lock(&self.keys).insert(tenant, Arc::new(TokenKey::new(&secret)));
        lock(&self.migrated).insert(tenant);
        Ok(())
    }

    fn database(&self, vhost: &str) -> String {
        let def = self.live.get(
            &super::DEFAULT_VHOST_DATABASE,
            &SettingCtx::process(&self.svc.dc),
        );
        sprig_proto_amqp::mem::default_database(vhost, &def)
    }

    fn grants(&self, p: &Principal, cat: &TenantCatalog) -> Option<Arc<dyn SessionGrants>> {
        let user = p.user_id?;
        let key = (cat.tenant.0, user, cat.version, cat.auth_version);
        if let Some(g) = lock(&self.grants).get(&key) {
            return Some(Arc::clone(g));
        }
        let g = crate::tenant::authz::grants_of(
            &self.svc,
            cat,
            Some(sprig_common::ids::UserId(
                u32::try_from(user).unwrap_or(u32::MAX),
            )),
        );
        let mut m = lock(&self.grants);
        if m.len() > 4096 {
            m.clear();
        }
        m.insert(key, Arc::clone(&g));
        Some(g)
    }

    fn lctn(&self, cat: &TenantCatalog) -> bool {
        self.svc.settings.get(
            &sprig_catalog::ddl::LOWER_CASE_TABLE_NAMES,
            &cat.setting_ctx(&self.svc.dc),
            Some(&cat.settings),
        ) == 1
    }

    fn allows(
        &self,
        g: &dyn SessionGrants,
        cat: &TenantCatalog,
        db: &str,
        a: Action,
        name: Option<&str>,
    ) -> bool {
        if g.unrestricted() {
            return true;
        }
        let lctn = self.lctn(cat);
        let db_key = database_key(db, lctn);
        if g.allows(a, Target::Database { db_key: &db_key }) || g.allows(a, Target::Tenant) {
            return true;
        }
        name.is_some_and(|n| {
            let name_key = object_key(ObjectKind::Table, n, lctn);
            g.allows(
                a,
                Target::Named {
                    db_key: &db_key,
                    name_key: &name_key,
                },
            )
        })
    }
}

#[async_trait]
impl MqCatalog for GatewayMq {
    async fn open(&self, p: &Principal, vhost: &str) -> Result<VhostRef, MqError> {
        let db = self.database(vhost);
        let cat = self.catalog(p.tenant_id, Freshness::Latest).await?;
        let lctn = self.lctn(&cat);
        let Some(row) = cat.database(&database_key(&db, lctn)) else {
            return Err(MqError::not_found(format!(
                "vhost '{vhost}' not found: the tenant has no database '{db}' (amqp.default_vhost_database maps '/')"
            )));
        };
        let r = VhostRef {
            tenant: p.tenant_id,
            db_id: row.db_id.0,
            db: row.name.clone(),
            name: vhost.to_owned(),
        };
        // A user with no right on the database at all can't use its vhost.
        if let Some(g) = self.grants(p, &cat) {
            let any = [
                Action::Select,
                Action::Insert,
                Action::Delete,
                Action::Create,
            ]
            .into_iter()
            .any(|a| self.allows(&*g, &cat, &row.name, a, None))
                || g.unrestricted()
                || Self::has_rule(p, &cat, &r)
                // R9 (r9tests): a right on one of its queues only (an
                // inherited queue sink's sprig_inherited).
                || Self::state(&cat)
                    .topology(row.db_id.0, cat.version)
                    .queues
                    .keys()
                    .any(|q| {
                        g.allows(
                            Action::Select,
                            Target::Named {
                                db_key: &database_key(&row.name, lctn),
                                name_key: &object_key(ObjectKind::Table, q, lctn),
                            },
                        )
                    })
                || cat.objects.iter().any(|o| {
                    o.db_id == row.db_id
                        && g.allows(
                            Action::Select,
                            Target::Object {
                                db_key: &database_key(&row.name, lctn),
                                db: row.db_id,
                                object: o.object_id,
                            },
                        )
                });
            if !any {
                return Err(MqError::access_refused(format!(
                    "access to vhost '{vhost}' refused for user '{}'",
                    p.user
                )));
            }
        }
        self.prepare(p.tenant_id).await?;
        Ok(r)
    }

    fn topology(&self, v: &VhostRef) -> Arc<Topology> {
        // Routing never waits, but a change made through another gateway
        // (known from the directory watch) is loaded in the background,
        // at most every 100 ms per tenant.
        let due = {
            let mut r = lock(&self.refreshed);
            let now = std::time::Instant::now();
            match r.get(&v.tenant) {
                Some(t) if now.duration_since(*t) < Duration::from_millis(100) => false,
                _ => {
                    r.insert(v.tenant, now);
                    true
                }
            }
        };
        if due {
            let svc = Arc::clone(&self.svc);
            let t = TenantId(v.tenant);
            tokio::spawn(async move {
                let _ = svc.cache.get(t, Freshness::Latest).await;
            });
        }
        match self.svc.cache.cached(TenantId(v.tenant)) {
            Some(cat) => Self::state(&cat).topology(v.db_id, cat.version),
            None => Arc::new(Topology::new()),
        }
    }

    async fn refresh(&self, v: &VhostRef) -> Result<Arc<Topology>, MqError> {
        let cat = self.catalog(v.tenant, Freshness::Latest).await?;
        Ok(Self::state(&cat).topology(v.db_id, cat.version))
    }

    async fn change(&self, v: &VhostRef, c: TopoChange) -> Result<Arc<Topology>, MqError> {
        self.prepare(v.tenant).await?;
        let tenant = TenantId(v.tenant);
        let new_id = match &c {
            TopoChange::AddExchange(_) | TopoChange::AddQueue(_) => {
                let ep = self.svc.locator.writer(tenant).await.map_err(unavailable)?;
                let mut conn = ep.pool.get_conn().await.map_err(unavailable)?;
                ep.store
                    .next_object_id(&mut conn)
                    .await
                    .map_err(unavailable)?
                    .0
            }
            _ => 0,
        };
        let mut ch = self
            .svc
            .catalog
            .change(tenant, VersionKind::Catalog)
            .await
            .map_err(unavailable)?;
        let applied = {
            let (store, tx) = ch.tx().parts();
            MqStore::new(store.schema())
                .apply(tx, v.db_id, &c, new_id)
                .await
        };
        if let Err(e) = applied {
            let _ = ch.rollback().await;
            return Err(unavailable(e));
        }
        let version = ch.commit().await.map_err(unavailable)?;
        let cat = self
            .catalog(
                v.tenant,
                Freshness::Cached {
                    min_version: version,
                },
            )
            .await?;
        Ok(Self::state(&cat).topology(v.db_id, cat.version))
    }

    async fn store(&self, v: &VhostRef) -> Result<Arc<dyn MsgStore>, MqError> {
        {
            let mut stores = self.stores.lock().await;
            match stores.get(&(v.tenant, v.db_id)) {
                Some(s) if !s.stale() => {
                    // The setting may have changed since the store opened.
                    if let Some(m) = lock(&self.maria).get(&(v.tenant, v.db_id)) {
                        m.set_confirm_replica(self.settings(v).confirm_replica);
                    }
                    return Ok(Arc::clone(s));
                }
                Some(_) => {
                    // A shard move fenced the schema: open it again where
                    // the tenant's map points once the move is done.
                    tracing::info!(tenant = v.tenant, vhost = %v.name, "queue store moved; opening it again");
                    stores.remove(&(v.tenant, v.db_id));
                }
                None => {}
            }
        }
        self.wait_out_hold(TenantId(v.tenant)).await;
        let mut stores = self.stores.lock().await;
        if let Some(s) = stores.get(&(v.tenant, v.db_id))
            && !s.stale()
        {
            return Ok(Arc::clone(s));
        }
        let tenant = TenantId(v.tenant);
        let cat = self.catalog(v.tenant, Freshness::Latest).await?;
        let row = cat
            .databases
            .get(DbId(v.db_id))
            .ok_or_else(|| MqError::not_found(format!("vhost '{}'", v.name)))?;
        let schema = sprig_catalog::ddl::shard_schemas(tenant, row)
            .first()
            .map(ToString::to_string)
            .ok_or_else(|| MqError::not_found(format!("database '{}' has no shard", row.name)))?;
        let cluster = self
            .svc
            .locator
            .cluster(tenant)
            .await
            .map_err(unavailable)?;
        let pool = self
            .svc
            .locator
            .ctl_pool(&cluster)
            .await
            .map_err(unavailable)?;
        // `queue.confirm_mode = replica`: pools on the cluster's replicas
        // (none for one node or Galera, whose commit is the confirm).
        let replicas = match self.svc.locator.ctl_replica_opts(&cluster).await {
            Ok(opts) => opts
                .into_iter()
                .map(|o| {
                    mysql_async::Pool::new(
                        mysql_async::OptsBuilder::from_opts(o).pool_opts(
                            mysql_async::PoolOpts::new()
                                .with_constraints(
                                    mysql_async::PoolConstraints::new(0, 4).unwrap_or_default(),
                                )
                                .with_inactive_connection_ttl(Duration::from_secs(60)),
                        ),
                    )
                })
                .collect(),
            Err(e) => {
                tracing::debug!(%cluster, error = %e, "no replica pools for queue confirms");
                Vec::new()
            }
        };
        let s = Arc::new(MariaStore::new(pool, schema).with_replicas(replicas));
        s.set_confirm_replica(self.settings(v).confirm_replica);
        s.ensure().await.map_err(unavailable)?;
        // R9 (r9fwd): the tenant's feed-driven outbox forwarder.
        crate::outbox_feed::subscribe(&self.svc, tenant).await;
        lock(&self.maria).insert((v.tenant, v.db_id), Arc::clone(&s));
        let s: Arc<dyn MsgStore> = s;
        stores.insert((v.tenant, v.db_id), Arc::clone(&s));
        Ok(s)
    }

    fn token_key(&self, tenant: u64) -> Option<Arc<TokenKey>> {
        lock(&self.keys).get(&tenant).cloned()
    }

    fn allowed(&self, p: &Principal, v: &VhostRef, perm: Perm, kind: &str, name: &str) -> bool {
        let Some(cat) = self.svc.cache.cached(TenantId(v.tenant)) else {
            return false;
        };
        if Self::rule_allows(p, &cat, v, perm, name) {
            return true;
        }
        let Some(g) = self.grants(p, &cat) else {
            // No user id: an internal principal.
            return true;
        };
        let ok = |a: Action| self.allows(&*g, &cat, &v.db, a, Some(name));
        match perm {
            Perm::Configure => ok(Action::Create) || ok(Action::Drop) || ok(Action::Alter),
            Perm::Write => ok(Action::Insert),
            Perm::Read if kind == "queue" => ok(Action::Select) && ok(Action::Delete),
            Perm::Read => ok(Action::Select),
        }
    }

    fn settings(&self, v: &VhostRef) -> QueueSettings {
        let Some(cat) = self.svc.cache.cached(TenantId(v.tenant)) else {
            return QueueSettings::default();
        };
        let ctx = cat.setting_ctx(&self.svc.dc).with_db(DbId(v.db_id));
        let n = |k: &'static sprig_settings::Setting<u64>| {
            self.svc.settings.get(k, &ctx, Some(&cat.settings))
        };
        let t = |k: &'static sprig_settings::Setting<std::borrow::Cow<'static, str>>| {
            self.svc.settings.get(k, &ctx, Some(&cat.settings))
        };
        QueueSettings {
            batch_rows: usize::try_from(n(&qs::WRITE_BATCH_MAX_ROWS)).unwrap_or(500),
            cache_bytes: n(&qs::OWNER_CACHE_MB) << 20,
            confirm_replica: t(&qs::CONFIRM_MODE) == "replica",
            delivery_limit_default: i64::try_from(n(&qs::DELIVERY_LIMIT_DEFAULT)).unwrap_or(20),
            delay_max_ms: n(&qs::DELAY_MAX_MS),
            default_quorum: t(&qs::DEFAULT_TYPE) == "quorum",
            consumer_timeout_ms: n(&qs::CONSUMER_TIMEOUT_MS),
            publish_timeout_ms: n(&qs::PUBLISH_TIMEOUT_MS),
            quota_bytes_per_sec: n(&qs::QUOTA_PUBLISH_BYTES_PER_SEC),
            max_unacked: u32::try_from(n(&super::MAX_UNACKED_PER_CONSUMER)).unwrap_or(65_535),
            takeover_grace_ms: n(&qs::TAKEOVER_GRACE_MS),
            owner_move_after_ms: n(&qs::OWNER_MOVE_AFTER_SECONDS).saturating_mul(1000),
            owner_move_cooldown_ms: n(&qs::OWNER_MOVE_COOLDOWN_SECONDS).saturating_mul(1000),
            forward_feed: t(&qs::FORWARDER) == "feed",
            forward_fallback_ms: n(&qs::FORWARD_FALLBACK_MS),
        }
    }

    /// The tenant's cluster's `queue-forwarder-beats/` beat, in its home DC's
    /// tier, read at most once a second per cluster (bugfix-42).
    async fn feed_forwarder_runs(&self, v: &VhostRef) -> bool {
        const REREAD: Duration = Duration::from_secs(1);
        let tenant = TenantId(v.tenant);
        let Ok(cluster) = self.svc.locator.cluster(tenant).await else {
            return false;
        };
        let name = cluster.to_string();
        if let Some((at, runs)) = lock(&self.beats).get(&name)
            && at.elapsed() < REREAD
        {
            return *runs;
        }
        let home = self.svc.locator.home_of(tenant);
        let dc = self.svc.locator.dc_store();
        let store = if home == self.svc.dc {
            dc
        } else {
            sprig_meta::TierView::over(&dc, sprig_meta::Tier::Dc(home))
        };
        let now = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis()),
        )
        .unwrap_or(0);
        let runs = matches!(
            sprig_feed::beat::read(&store, &cluster).await,
            Ok(Some(b)) if b.fresh(now)
        );
        lock(&self.beats).insert(name, (std::time::Instant::now(), runs));
        runs
    }
}

impl GatewayMq {
    /// Waits while a mover's cutover holds the tenant's map (chapter 10),
    /// at most 10 s: a store opened on the cluster about to be fenced
    /// would write where the move no longer copies.
    async fn wait_out_hold(&self, tenant: TenantId) {
        let started = std::time::Instant::now();
        let mut changes = self.svc.locator.placement_changes_for(tenant);
        while started.elapsed() < Duration::from_secs(10) {
            let held = self
                .svc
                .locator
                .placement_followed(tenant)
                .await
                .and_then(|p| p.hold)
                .is_some_and(|h| {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
                    now < h.until_ms
                });
            if !held {
                return;
            }
            let _ = tokio::time::timeout(Duration::from_millis(200), changes.changed()).await;
        }
    }

    fn has_rule(p: &Principal, cat: &TenantCatalog, v: &VhostRef) -> bool {
        let Some(user) = p.user_id else { return false };
        Self::state(cat)
            .topology(v.db_id, cat.version)
            .permissions
            .iter()
            .any(|r| r.user_id == user)
    }

    fn rule_allows(
        p: &Principal,
        cat: &TenantCatalog,
        v: &VhostRef,
        perm: Perm,
        name: &str,
    ) -> bool {
        let Some(user) = p.user_id else { return false };
        Self::state(cat)
            .topology(v.db_id, cat.version)
            .permissions
            .iter()
            .filter(|r| r.user_id == user)
            .any(|r| match perm {
                Perm::Configure => r.configure.is_match(name),
                Perm::Write => r.write.is_match(name),
                Perm::Read => r.read.is_match(name),
            })
    }
}

// ----------------------------------------------------------------------
// Owner leases in etcd.
// ----------------------------------------------------------------------

/// The lease's time to live.
const LEASE_TTL: Duration = Duration::from_secs(10);
/// How often it is kept alive.
const KEEPALIVE: Duration = Duration::from_secs(3);

#[derive(Debug, Serialize, Deserialize)]
struct LeaseValue {
    v: u32,
    gateway_id: String,
}

#[derive(Default)]
struct Held {
    lease: Option<LeaseId>,
    lost: CancellationToken,
    keys: HashMap<(u64, u64), u64>,
}

/// Owner leases of this gateway in this DC's etcd tier.
pub struct EtcdLeases {
    store: Arc<dyn MetaStore>,
    me: String,
    held: tokio::sync::Mutex<Held>,
}

impl std::fmt::Debug for EtcdLeases {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EtcdLeases")
            .field("me", &self.me)
            .finish_non_exhaustive()
    }
}

impl EtcdLeases {
    /// Leases of gateway `me` in `store` (the DC tier).
    #[must_use]
    pub fn new(store: Arc<dyn MetaStore>, me: String) -> Self {
        Self {
            store,
            me,
            held: tokio::sync::Mutex::new(Held::default()),
        }
    }

    fn key(&self, tenant: u64, object: u64) -> Result<Key, MqError> {
        self.store
            .tier()
            .key(&format!("queue-owners/{tenant}/{object}"))
            .map_err(unavailable)
    }

    /// Keeps the lease alive until `stop`; a lost lease cancels every
    /// owner of this gateway. At `stop` the lease is revoked, so the next
    /// owners take over at once.
    pub async fn keep(self: Arc<Self>, stop: CancellationToken) {
        loop {
            tokio::select! {
                () = stop.cancelled() => break,
                () = tokio::time::sleep(KEEPALIVE) => {}
            }
            let lease = self.held.lock().await.lease;
            let Some(l) = lease else { continue };
            let alive = matches!(self.store.keep_alive(l).await, Ok(Some(_)));
            if !alive {
                let mut h = self.held.lock().await;
                if h.lease == Some(l) {
                    tracing::warn!(gateway = %self.me, "queue owner lease lost: owners stop");
                    h.lost.cancel();
                    *h = Held::default();
                }
            }
        }
        let mut h = self.held.lock().await;
        h.lost.cancel();
        if let Some(l) = h.lease.take() {
            let _ = self.store.revoke_lease(l).await;
        }
    }
}

#[async_trait]
impl Leases for EtcdLeases {
    fn me(&self) -> &str {
        &self.me
    }

    async fn acquire(&self, tenant: u64, object: u64) -> Result<Acquired, MqError> {
        let key = self.key(tenant, object)?;
        let mut h = self.held.lock().await;
        if h.keys.contains_key(&(tenant, object)) {
            return Ok(Acquired::Mine(h.lost.clone()));
        }
        let lease = if let Some(l) = h.lease {
            l
        } else {
            let l = self
                .store
                .grant_lease(LEASE_TTL)
                .await
                .map_err(unavailable)?;
            h.lease = Some(l);
            h.lost = CancellationToken::new();
            l
        };
        let value = serde_json::to_vec(&LeaseValue {
            v: 1,
            gateway_id: self.me.clone(),
        })
        .unwrap_or_default();
        let r = self
            .store
            .txn(Txn {
                when: vec![Cmp::Missing(key.clone())],
                then: vec![
                    Op::Put {
                        key: key.clone(),
                        value,
                        lease: Some(lease),
                    },
                    Op::Get(key.clone()),
                ],
                otherwise: vec![Op::Get(key.clone())],
            })
            .await
            .map_err(unavailable)?;
        let kv = r.results.iter().find_map(|o| match o {
            OpResult::Get(Some(kv)) => Some(kv.clone()),
            _ => None,
        });
        if r.succeeded {
            let rev = kv.map_or(r.revision, |k| k.create_revision);
            h.keys.insert((tenant, object), rev);
            return Ok(Acquired::Mine(h.lost.clone()));
        }
        let Some(kv) = kv else {
            return Ok(Acquired::Held("another gateway".into()));
        };
        let holder = serde_json::from_slice::<LeaseValue>(&kv.value)
            .map_or_else(|_| "another gateway".to_owned(), |v| v.gateway_id);
        if holder != self.me {
            return Ok(Acquired::Held(holder));
        }
        // An earlier process of this gateway (the id is stable across
        // restarts) left the key on a lease that hasn't expired yet: that
        // process is gone, so take the key over. The owner epoch in MariaDB
        // fences anything it might still write.
        let value = serde_json::to_vec(&LeaseValue {
            v: 1,
            gateway_id: self.me.clone(),
        })
        .unwrap_or_default();
        let r = self
            .store
            .txn(Txn {
                when: vec![Cmp::ModRev(key.clone(), kv.mod_revision)],
                then: vec![
                    Op::Put {
                        key: key.clone(),
                        value,
                        lease: Some(lease),
                    },
                    Op::Get(key.clone()),
                ],
                otherwise: vec![],
            })
            .await
            .map_err(unavailable)?;
        if !r.succeeded {
            return Ok(Acquired::Held(format!("{holder} (an earlier process)")));
        }
        let rev = r
            .results
            .iter()
            .find_map(|o| match o {
                OpResult::Get(Some(k)) => Some(k.create_revision),
                _ => None,
            })
            .unwrap_or(kv.create_revision);
        h.keys.insert((tenant, object), rev);
        Ok(Acquired::Mine(h.lost.clone()))
    }

    async fn holder(&self, tenant: u64, object: u64) -> Result<Option<String>, MqError> {
        let key = self.key(tenant, object)?;
        let kv = self
            .store
            .get(&key, sprig_meta::Consistency::Linearizable)
            .await
            .map_err(unavailable)?;
        Ok(kv.map(|kv| {
            serde_json::from_slice::<LeaseValue>(&kv.value)
                .map_or_else(|_| "another gateway".to_owned(), |v| v.gateway_id)
        }))
    }

    async fn release(&self, tenant: u64, object: u64) {
        let Ok(key) = self.key(tenant, object) else {
            return;
        };
        let mut h = self.held.lock().await;
        let Some(rev) = h.keys.remove(&(tenant, object)) else {
            return;
        };
        let _ = self
            .store
            .txn(Txn {
                when: vec![Cmp::CreateRev(key.clone(), rev)],
                then: vec![Op::Delete(key)],
                otherwise: vec![],
            })
            .await;
    }
}
