//! Graphs in the gateway (chapter 43, R8): finding, creating and dropping
//! native graphs through the tenant catalog, their storage on the shard,
//! and chapter 29's checks. Used by the Bolt front end ([`crate::bolt`])
//! and by `CREATE GRAPH` / `DROP GRAPH` in SQL ([`crate::exec`]).
//!
//! - **Catalog.** A graph is an `objects` row (kind `graph`, namespace
//!   `graph`) and a `graphs` row, written in one versioned change; labels
//!   and relationship types are `graph_node_sources` and
//!   `graph_edge_sources` rows, added on first use in their own change.
//! - **Storage** (round 3). A new graph is a sharded table group keyed by
//!   `part` (numeric), presplit into `sharding.min_shards` shards (database
//!   scope) on the tenant's cluster: a `table_groups` row (origin
//!   `structural`), four `group_members` rows (nodes on `part`; edges,
//!   labels and mirrors through their node), the shard map in etcd, and in
//!   each shard schema `t<t>_d<db>_g<group>_s<n>` the node, edge, label and
//!   mirror tables with `__sprig_dtx`. The id counter stays in the
//!   database's first shard schema. Splits, merges and moves of the group
//!   are the mover's (chapter 20), like any group's. Graphs from rounds 1
//!   and 2 stay in the first shard schema, one shard. Everything runs as the
//!   cluster's `sprig_ctl` account; statements run inside the Bolt
//!   transaction (`READ COMMITTED`) through [`txn`].
//! - **Privileges.** A graph is an object with the table actions: reading
//!   needs `SELECT`, `CREATE`/`MERGE` need `INSERT`, `SET`/`REMOVE`
//!   `UPDATE`, `DELETE` `DELETE` on the graph or its database;
//!   `CREATE GRAPH` needs `CREATE`, `DROP GRAPH` `DROP`. Without `SELECT`
//!   on the graph, reads may rest on grants of single labels and types
//!   (`GRANT READ ON GRAPH db.g NODES L`, decision G42): the statement
//!   then sees only those, through `sprig_graph::access`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::SystemTime;

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use sprig_auth::authz::{Action, SessionGrants, Target};
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::{Namespace, ObjectFlags, ObjectKind, ObjectRow, ObjectState};
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogLocator, Freshness, TenantCatalog};
use sprig_common::ids::{ClusterId, DbId, ObjectId, TenantId, UserId};
use sprig_common::ids::{DcId, GroupId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_graph::ast::{Entity, Schema};
use sprig_graph::catalog::{GraphCatalog, GraphDef, GraphState, MODULE, Tables, backend_error};
use sprig_graph::maria::{CounterIds, Dictionary, GraphShards, SchemaOps, ShardLoc};
use sprig_graph::schema::{plan_create, plan_drop};
use sprig_graph::store::{IndexDef, SchemaCounts};
use sprig_meta::{Tier, TierView};
use sprig_shard::{KeyClass, KeySpec, ShardMap, ShardMapCache, ShardMapStore, shard_schema};

use crate::tenant::TenantServices;

pub(crate) mod mirror;
pub(crate) mod txn;

/// Ids handed out per block of the counter table.
const ID_BLOCK: u64 = 256;
/// Connections of Bolt transactions per cluster and gateway.
pub const SESSION_POOL_MAX: usize = 64;

/// One resolved graph.
#[derive(Debug, Clone)]
pub struct GraphRef {
    /// The tenant.
    pub tenant: TenantId,
    /// The logical database.
    pub db_id: DbId,
    /// Its name.
    pub db: String,
    /// The graph's name.
    pub name: String,
    /// `objects.object_id`.
    pub graph_id: u64,
    /// The shard schema holding its tables.
    pub schema: String,
    /// The cluster.
    pub cluster: ClusterId,
    /// Labels and types at resolution.
    pub def: Arc<GraphDef>,
    /// Its shards at resolution.
    pub shards: Arc<GraphShards>,
    /// While the mover holds a range of the graph's map: the latest end of
    /// the holds (ms since the epoch).
    pub held_until: Option<u64>,
}

impl GraphRef {
    /// The graph's tables in the database's first shard schema (the id
    /// counter; every table of a graph from rounds 1 and 2).
    #[must_use]
    pub fn tables(&self) -> Tables {
        Tables::of(&self.schema, self.graph_id)
    }
}

/// The gateway's graph services.
pub struct GatewayGraphs {
    pub(crate) svc: Arc<TenantServices>,
    migrated: Mutex<HashSet<u64>>,
    counters: Mutex<HashMap<(u64, u64), Arc<CounterIds>>>,
    /// Labels and types this process made or looked up since the
    /// snapshot, by (tenant, graph): (labels, types).
    names: Mutex<HashMap<(u64, u64), Names>>,
    /// Pools for Bolt transactions, by cluster: apart from the cluster's
    /// `sprig_ctl` pool, which the catalog and the id counters use while a
    /// transaction holds its connection.
    sessions: Mutex<HashMap<ClusterId, mysql_async::Pool>>,
    /// Replica pools for Bolt reads in `mode: "r"`, by node address.
    replicas: Mutex<HashMap<String, mysql_async::Pool>>,
    /// Spreads reads over replicas.
    next_replica: std::sync::atomic::AtomicUsize,
    /// Bookmark keys by tenant (the tenant's `graph_keys` secret).
    keys: Mutex<HashMap<u64, Arc<ring::hmac::Key>>>,
    /// Shard map caches by home DC (graph groups' maps).
    maps: Mutex<HashMap<DcId, ShardMapCache>>,
    /// Shard schemas known to have `__sprig_dtx`: (cluster, schema).
    dtx_ready: Mutex<HashSet<(String, String)>>,
    /// Chapter 18's coordinator pieces for Bolt transactions.
    pub(crate) coord: txn::CoordServices,
    /// Round 5: asynchronous mirrors' mode changes and subscriptions.
    pub(crate) mirrors: mirror::MirrorState,
}

type Names = (BTreeMap<u32, String>, BTreeMap<u32, String>);

impl std::fmt::Debug for GatewayGraphs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayGraphs").finish_non_exhaustive()
    }
}

fn not_found(db: &str, name: &str) -> SprigError {
    SprigError::new(
        SprigCode::GraphNotFound,
        format!("Graph not found: {db}.{name}"),
    )
}

fn state_of(cat: &TenantCatalog) -> GraphState {
    cat.ext
        .get::<GraphState>(MODULE)
        .cloned()
        .unwrap_or_default()
}

impl GatewayGraphs {
    /// Over the tenant services.
    #[must_use]
    pub fn new(svc: Arc<TenantServices>) -> Self {
        Self {
            svc,
            migrated: Mutex::new(HashSet::new()),
            counters: Mutex::new(HashMap::new()),
            names: Mutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
            replicas: Mutex::new(HashMap::new()),
            next_replica: std::sync::atomic::AtomicUsize::new(0),
            keys: Mutex::new(HashMap::new()),
            maps: Mutex::new(HashMap::new()),
            dtx_ready: Mutex::new(HashSet::new()),
            coord: txn::CoordServices::new(),
            mirrors: mirror::MirrorState::default(),
        }
    }

    /// The shard map cache of `tenant`'s home DC (started at first use).
    ///
    /// # Errors
    /// `Internal` never in practice.
    pub fn map_cache(&self, tenant: TenantId) -> Result<ShardMapCache> {
        let home = self.svc.locator.home_of(tenant);
        let mut m = self.maps.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(c) = m.get(&home) {
            return Ok(c.clone());
        }
        let dc = self.svc.locator.dc_store();
        let store = if home == self.svc.dc {
            dc
        } else {
            TierView::over(&dc, Tier::Dc(home.clone()))
        };
        let c = ShardMapCache::spawn(store)?;
        m.insert(home, c.clone());
        Ok(c)
    }

    /// A linearizable map store over `tenant`'s home DC tier.
    fn map_store(&self, tenant: TenantId) -> ShardMapStore {
        let home = self.svc.locator.home_of(tenant);
        let dc = self.svc.locator.dc_store();
        if home == self.svc.dc {
            ShardMapStore::new(dc)
        } else {
            ShardMapStore::new(TierView::over(&dc, Tier::Dc(home)))
        }
    }

    /// Group `group`'s map: the watched copy, or etcd when the copy hasn't
    /// seen it yet (a graph just created).
    ///
    /// # Errors
    /// 39424-class map errors from the store.
    pub async fn shard_map(&self, tenant: TenantId, group: u32) -> Result<Arc<ShardMap>> {
        if let Ok(m) = self
            .map_cache(tenant)
            .and_then(|c| c.get(tenant, GroupId(group)))
        {
            return Ok(m);
        }
        let (m, _) = self.map_store(tenant).load(tenant, GroupId(group)).await?;
        Ok(Arc::new(m))
    }

    /// Chapter 18's decision table in every shard schema of `shards`
    /// (once per schema and process): a split or move creates its new
    /// shard schemas without it, and a coordinated commit anchored there
    /// needs it before its branches open.
    async fn ensure_dtx(&self, shards: &[ShardLoc]) -> Result<()> {
        for sh in shards {
            let k = (sh.cluster.clone(), sh.schema.clone());
            if self
                .dtx_ready
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .contains(&k)
            {
                continue;
            }
            let cluster = ClusterId::new(&sh.cluster)
                .map_err(|e| SprigError::internal(format!("cluster id {}: {e}", sh.cluster)))?;
            let pool = self.pool(&cluster).await?;
            let mut c = pool.get_conn().await.map_err(backend_error)?;
            // With the uniqueness claim table (decision G41), which graphs
            // made before round 4 lack.
            let r = match c
                .query_drop(sprig_catalog::ddl::dtx_table_sql(&sh.schema))
                .await
            {
                Ok(()) => c.query_drop(sh.tables.claims_ddl()).await,
                Err(e) => Err(e),
            };
            match r {
                Ok(()) => {
                    self.dtx_ready
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .insert(k);
                }
                // The schema isn't there (yet): the statement will fail on
                // its own and retry with a newer map.
                Err(e) => {
                    tracing::debug!(error = %e, schema = %sh.schema, "decision table not ensured");
                }
            }
        }
        Ok(())
    }

    /// The shards of graph `def` in database `db`, and the end of any hold.
    async fn layout(
        &self,
        tenant: TenantId,
        def: &GraphDef,
        default_schema: &str,
        cluster: &ClusterId,
    ) -> Result<(Arc<GraphShards>, Option<u64>)> {
        let Some(group) = def.group_id else {
            return Ok((
                Arc::new(GraphShards::single(
                    def.graph_id,
                    def.db_id,
                    default_schema,
                    cluster.as_str(),
                )),
                None,
            ));
        };
        let map = self.shard_map(tenant, group).await?;
        let mut held: Option<u64> = None;
        let shards: Vec<ShardLoc> = map
            .ranges
            .iter()
            .map(|r| {
                if r.state == sprig_shard::shardmap::RangeState::Held {
                    let u = r.hold_until_ms.unwrap_or(0);
                    held = Some(held.map_or(u, |h| h.max(u)));
                }
                let schema = shard_schema(tenant, DbId(def.db_id), GroupId(group), r.shard);
                ShardLoc {
                    shard: r.shard.0,
                    lo: r.lo,
                    epoch: r.epoch,
                    cluster: r.cluster.to_string(),
                    tables: Tables::of(&schema, def.graph_id),
                    schema,
                }
            })
            .collect();
        self.ensure_dtx(&shards).await?;
        Ok((
            Arc::new(GraphShards {
                graph: def.graph_id,
                db: def.db_id,
                group,
                version: map.version,
                shards,
                seq: Tables::of(default_schema, def.graph_id).seq,
                // Every grouped graph has mirror rows (decision G29); who
                // keeps them follows `graph.mirror` (round 5).
                mirrors: true,
                mirror_async: def.mirror_async,
                mirror_building: !def.mirrors,
            }),
            held,
        ))
    }

    /// The tenant's bookmark key: its catalog's `graph_keys` secret, the
    /// same on every gateway.
    ///
    /// # Errors
    /// The locator's; a backend error.
    pub async fn bookmark_key(&self, tenant: TenantId) -> Result<Arc<ring::hmac::Key>> {
        if let Some(k) = self
            .keys
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&tenant.0)
        {
            return Ok(Arc::clone(k));
        }
        self.prepare(tenant).await?;
        let ep = self.svc.locator.writer(tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(backend_error)?;
        let secret = GraphCatalog::new(ep.store.schema())
            .bookmark_secret(&mut conn)
            .await?;
        let k = Arc::new(sprig_graph::bookmark::key(&secret));
        self.keys
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(tenant.0, Arc::clone(&k));
        Ok(k)
    }

    /// One replica pool of `cluster` for a read, taking turns; `None` for
    /// clusters without replicas (one node, Galera, external).
    ///
    /// # Errors
    /// The locator's.
    pub async fn replica_pool(&self, cluster: &ClusterId) -> Result<Option<mysql_async::Pool>> {
        let opts = self.svc.locator.ctl_replica_opts(cluster).await?;
        if opts.is_empty() {
            return Ok(None);
        }
        let i = self
            .next_replica
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % opts.len();
        let o = opts[i].clone();
        let key = format!(
            "{}:{}:{}",
            o.ip_or_hostname(),
            o.tcp_port(),
            o.user().unwrap_or("")
        );
        let mut m = self.replicas.lock().unwrap_or_else(PoisonError::into_inner);
        let pool = m.entry(key).or_insert_with(|| {
            mysql_async::Pool::new(
                mysql_async::OptsBuilder::from_opts(o).pool_opts(
                    mysql_async::PoolOpts::new()
                        .with_constraints(
                            mysql_async::PoolConstraints::new(0, SESSION_POOL_MAX)
                                .unwrap_or_default(),
                        )
                        .with_inactive_connection_ttl(std::time::Duration::from_secs(60)),
                ),
            )
        });
        Ok(Some(pool.clone()))
    }

    /// The module's tables in the tenant catalog, once per process.
    async fn prepare(&self, tenant: TenantId) -> Result<()> {
        if self
            .migrated
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&tenant.0)
        {
            return Ok(());
        }
        let ep = self.svc.locator.writer(tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(backend_error)?;
        GraphCatalog::new(ep.store.schema())
            .migrate(&mut conn)
            .await?;
        self.migrated
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(tenant.0);
        Ok(())
    }

    /// The tenant's catalog snapshot.
    ///
    /// # Errors
    /// The cache's.
    pub async fn catalog(&self, tenant: TenantId, fresh: Freshness) -> Result<Arc<TenantCatalog>> {
        self.svc.cache.get(tenant, fresh).await
    }

    /// Whether the tenant's names are case-insensitive.
    #[must_use]
    pub fn lctn(&self, cat: &TenantCatalog) -> bool {
        self.svc.settings.get(
            &sprig_catalog::ddl::LOWER_CASE_TABLE_NAMES,
            &cat.setting_ctx(&self.svc.dc),
            Some(&cat.settings),
        ) == 1
    }

    /// The grants of `user` in `cat` (`None`: an internal principal).
    #[must_use]
    pub fn grants(
        &self,
        cat: &TenantCatalog,
        user: Option<UserId>,
    ) -> Option<Arc<dyn SessionGrants>> {
        user.map(|u| crate::tenant::authz::grants_of(&self.svc, cat, Some(u)))
    }

    /// Whether `g` allows `a` on graph `object` (or its database).
    #[must_use]
    pub fn allows(
        &self,
        g: Option<&dyn SessionGrants>,
        cat: &TenantCatalog,
        db: &str,
        db_id: DbId,
        object: Option<u64>,
        a: Action,
    ) -> bool {
        let Some(g) = g else { return true };
        if g.unrestricted() || g.allows(a, Target::Tenant) {
            return true;
        }
        let db_key = database_key(db, self.lctn(cat));
        if g.allows(a, Target::Database { db_key: &db_key }) {
            return true;
        }
        object.is_some_and(|o| {
            g.allows(
                a,
                Target::Object {
                    db_key: &db_key,
                    db: db_id,
                    object: ObjectId(o),
                },
            )
        })
    }

    /// The labels and types `g`'s grants on single elements of graph
    /// `graph` let it write with `a` (`INSERT` = `CREATE`, `DELETE`,
    /// `UPDATE` = `SET PROPERTY`; round 5): `None` when they name none.
    #[must_use]
    pub fn writable(
        g: &dyn SessionGrants,
        graph: u64,
        a: Action,
    ) -> Option<(HashSet<String>, HashSet<String>)> {
        let (mut labels, mut types) = (HashSet::new(), HashSet::new());
        for (rels, name, set) in g.graph_elements(ObjectId(graph)) {
            if !set.has(a) {
                continue;
            }
            if rels {
                types.insert(name);
            } else {
                labels.insert(name);
            }
        }
        (!labels.is_empty() || !types.is_empty()).then_some((labels, types))
    }

    /// What `g`'s grants on single labels and relationship types of graph
    /// `graph` let it read (decision G42); `None` when they give nothing.
    #[must_use]
    pub fn visible(g: &dyn SessionGrants, graph: u64) -> Option<sprig_graph::access::Visible> {
        let mut v = sprig_graph::access::Visible::default();
        for (rels, name, a) in g.graph_elements(ObjectId(graph)) {
            if !a.has(Action::Select) {
                continue;
            }
            if rels {
                v.types.insert(name);
            } else {
                v.labels.insert(name);
            }
        }
        (!v.labels.is_empty() || !v.types.is_empty()).then_some(v)
    }

    /// Resolves `db.name` in `cat`: `None` if there is no such graph.
    ///
    /// # Errors
    /// 39902 for an unknown database; locator errors.
    pub async fn resolve(
        &self,
        cat: &TenantCatalog,
        db: &str,
        name: &str,
    ) -> Result<Option<GraphRef>> {
        let lctn = self.lctn(cat);
        let Some(row) = cat.database(&database_key(db, lctn)) else {
            return Err(SprigError::new(
                SprigCode::GraphNotFound,
                format!(
                    "Database '{db}' does not exist: create it in SQL first (graphs live in a logical database)"
                ),
            ));
        };
        let Some(o) = cat
            .object(
                row.db_id,
                Namespace::Graph,
                &object_key(ObjectKind::Graph, name, lctn),
            )
            .filter(|o| o.state == ObjectState::Active && o.kind == ObjectKind::Graph)
        else {
            return Ok(None);
        };
        let Some(def) = state_of(cat).graphs.get(&o.object_id.0).cloned() else {
            return Ok(None);
        };
        let schema = sprig_catalog::ddl::shard_schemas(cat.tenant, row)
            .first()
            .map(ToString::to_string)
            .ok_or_else(|| not_found(db, name))?;
        let cluster = self.svc.locator.cluster(cat.tenant).await?;
        let (shards, held_until) = self.layout(cat.tenant, &def, &schema, &cluster).await?;
        Ok(Some(GraphRef {
            tenant: cat.tenant,
            db_id: row.db_id,
            db: row.name.clone(),
            name: o.name.clone(),
            graph_id: o.object_id.0,
            schema,
            cluster,
            def,
            shards,
            held_until,
        }))
    }

    /// `g` again with the shard map now (after a map change).
    ///
    /// # Errors
    /// The map store's.
    pub async fn refresh(&self, g: &GraphRef) -> Result<GraphRef> {
        let (shards, held_until) = self.layout(g.tenant, &g.def, &g.schema, &g.cluster).await?;
        Ok(GraphRef {
            shards,
            held_until,
            ..g.clone()
        })
    }

    /// The `sprig_ctl` pool of `cluster`.
    ///
    /// # Errors
    /// The locator's.
    pub async fn pool(&self, cluster: &ClusterId) -> Result<mysql_async::Pool> {
        self.svc.locator.ctl_pool(cluster).await
    }

    /// The pool Bolt transactions take connections from on `cluster`:
    /// the `sprig_ctl` account, up to [`SESSION_POOL_MAX`] connections.
    ///
    /// # Errors
    /// The locator's; a backend error.
    pub async fn session_pool(&self, cluster: &ClusterId) -> Result<mysql_async::Pool> {
        if let Some(p) = self
            .sessions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(cluster)
        {
            return Ok(p.clone());
        }
        let ctl = self.pool(cluster).await?;
        let opts = {
            let c = ctl.get_conn().await.map_err(backend_error)?;
            c.opts().clone()
        };
        let pool = mysql_async::Pool::new(
            mysql_async::OptsBuilder::from_opts(opts).pool_opts(
                mysql_async::PoolOpts::new()
                    .with_constraints(
                        mysql_async::PoolConstraints::new(0, SESSION_POOL_MAX).unwrap_or_default(),
                    )
                    .with_inactive_connection_ttl(std::time::Duration::from_secs(60)),
            ),
        );
        Ok(self
            .sessions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(cluster.clone())
            .or_insert(pool)
            .clone())
    }

    /// The id counters of `g`.
    ///
    /// # Errors
    /// The locator's.
    pub async fn counters(&self, g: &GraphRef) -> Result<Arc<CounterIds>> {
        if let Some(c) = self
            .counters
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(g.tenant.0, g.graph_id))
        {
            return Ok(Arc::clone(c));
        }
        let pool = self.pool(&g.cluster).await?;
        let c = Arc::new(CounterIds::new(pool, &g.tables(), ID_BLOCK));
        self.counters
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert((g.tenant.0, g.graph_id), Arc::clone(&c));
        Ok(c)
    }

    /// `CREATE GRAPH`: the group's id and the shard schemas with their
    /// tables, the shard map, then the catalog rows (object, graph, group,
    /// members) in one change. Returns the graph and the catalog version
    /// that has it.
    ///
    /// # Errors
    /// 39902 for an unknown database; 39908 for a name in use (unless
    /// `if_not_exists`); catalog, map and backend errors.
    #[allow(clippy::too_many_lines)] // ids, schemas, map, catalog, undo
    pub async fn create(
        &self,
        tenant: TenantId,
        db: &str,
        name: &str,
        if_not_exists: bool,
    ) -> Result<(GraphRef, u64)> {
        self.prepare(tenant).await?;
        let cat = self.catalog(tenant, Freshness::Latest).await?;
        if let Some(g) = self.resolve(&cat, db, name).await? {
            if if_not_exists {
                return Ok((g, cat.version));
            }
            return Err(SprigError::new(
                SprigCode::GraphDefinitionInvalid,
                format!("Graph '{db}.{name}' already exists"),
            ));
        }
        let lctn = self.lctn(&cat);
        let row = cat
            .database(&database_key(db, lctn))
            .cloned()
            .ok_or_else(|| not_found(db, name))?;
        let schema = sprig_catalog::ddl::shard_schemas(tenant, &row)
            .first()
            .map(ToString::to_string)
            .ok_or_else(|| not_found(db, name))?;
        let shards = self
            .svc
            .settings
            .get(
                &sprig_balancer::shards::settings::MIN_SHARDS,
                &cat.setting_ctx(&self.svc.dc).with_db(row.db_id),
                Some(&cat.settings),
            )
            .clamp(1, 256);
        let ep = self.svc.locator.writer(tenant).await?;
        let (object_id, members, group) = {
            let mut c = ep.pool.get_conn().await.map_err(backend_error)?;
            let o = ep.store.next_object_id(&mut c).await?;
            let mut m = [0u64; 4];
            for x in &mut m {
                *x = ep.store.next_object_id(&mut c).await?.0;
            }
            sprig_shard::groups::migrate(&mut c, ep.store.schema()).await?;
            let g = alloc_group_id(&mut c, ep.store.schema()).await?;
            (o, m, g)
        };
        let cluster = self.svc.locator.cluster(tenant).await?;
        let spec = KeySpec::new(vec![KeyClass::Numeric])?;
        let map = ShardMap::presplit(
            tenant,
            GroupId(group),
            spec.digest,
            shards,
            std::slice::from_ref(&cluster),
        )?;
        let schemas: Vec<String> = map
            .ranges
            .iter()
            .map(|r| shard_schema(tenant, row.db_id, GroupId(group), r.shard))
            .collect();
        let seq = Tables::of(&schema, object_id.0);
        let made = async {
            let pool = self.pool(&cluster).await?;
            let mut c = pool.get_conn().await.map_err(backend_error)?;
            let like: Option<(String, String)> = c
                .query_first(format!(
                    "SHOW CREATE DATABASE {}",
                    sprig_common::names::quote(&schema)
                ))
                .await
                .map_err(backend_error)?;
            for s in &schemas {
                let sql = like.as_ref().map_or_else(
                    || {
                        format!(
                            "CREATE DATABASE IF NOT EXISTS {}",
                            sprig_common::names::quote(s)
                        )
                    },
                    |(_, x)| {
                        x.replacen("CREATE DATABASE", "CREATE DATABASE IF NOT EXISTS", 1)
                            .replace(
                                &sprig_common::names::quote(&schema),
                                &sprig_common::names::quote(s),
                            )
                    },
                );
                c.query_drop(sql).await.map_err(backend_error)?;
                c.query_drop(sprig_catalog::ddl::dtx_table_sql(s))
                    .await
                    .map_err(backend_error)?;
                for d in Tables::of(s, object_id.0).shard_ddl() {
                    c.query_drop(d).await.map_err(backend_error)?;
                }
            }
            for d in seq.seq_ddl() {
                c.query_drop(d).await.map_err(backend_error)?;
            }
            self.map_store(tenant).create(&map).await?;
            Ok::<(), SprigError>(())
        }
        .await;
        if let Err(e) = made {
            self.drop_physical(tenant, &cluster, &schemas, &seq, Some(group))
                .await;
            return Err(e);
        }
        let mut ch = self
            .svc
            .catalog
            .change(tenant, VersionKind::Catalog)
            .await?;
        let version = ch.version();
        let r = {
            let (store, tx) = ch.tx().parts();
            let obj = ObjectRow {
                object_id,
                db_id: row.db_id,
                namespace: Namespace::Graph,
                name: name.to_owned(),
                name_key: object_key(ObjectKind::Graph, name, lctn),
                kind: ObjectKind::Graph,
                parent_id: None,
                state: ObjectState::Active,
                schema_version: 1,
                flags: ObjectFlags::default(),
                pending_job_id: None,
                created_at: SystemTime::now(),
                changed_version: version,
            };
            let cat_schema = store.schema().to_owned();
            match store.insert_object(tx, &obj).await {
                Ok(()) => match GraphCatalog::new(&cat_schema)
                    .insert_native_in(tx, object_id.0, row.db_id.0, Some(group))
                    .await
                {
                    Ok(()) => {
                        insert_group(
                            tx,
                            &cat_schema,
                            group,
                            shards,
                            &spec,
                            row.db_id,
                            object_id.0,
                            members,
                        )
                        .await
                    }
                    Err(e) => Err(e),
                },
                Err(e) => Err(e),
            }
        };
        if let Err(e) = r {
            let _ = ch.rollback().await;
            self.drop_physical(tenant, &cluster, &schemas, &seq, Some(group))
                .await;
            return Err(e);
        }
        let v = ch.commit().await?;
        let cat = self
            .catalog(tenant, Freshness::Cached { min_version: v })
            .await?;
        let g = self
            .resolve(&cat, db, name)
            .await?
            .ok_or_else(|| SprigError::internal("graph missing after create"))?;
        tracing::info!(
            tenant = tenant.0,
            db,
            graph = name,
            id = object_id.0,
            group,
            shards,
            "graph created"
        );
        Ok((g, v))
    }

    /// Drops a graph's storage: its group's shard schemas (each on its
    /// cluster, as `shards` lists them) and map, or its tables in one
    /// schema; then the id counter.
    async fn drop_physical(
        &self,
        tenant: TenantId,
        cluster: &ClusterId,
        schemas: &[String],
        seq: &Tables,
        group: Option<u32>,
    ) {
        self.drop_schemas(
            &schemas
                .iter()
                .map(|s| (cluster.clone(), s.clone()))
                .collect::<Vec<_>>(),
        )
        .await;
        self.drop_sql(cluster, &format!("DROP TABLE IF EXISTS {}", seq.seq))
            .await;
        if let Some(g) = group
            && let Err(e) = self.map_store(tenant).delete(tenant, GroupId(g)).await
            && e.code() != Some(SprigCode::ShardMapUnavailable)
        {
            tracing::warn!(error = %e, group = g, "deleting a graph's shard map failed");
        }
    }

    async fn drop_schemas(&self, at: &[(ClusterId, String)]) {
        for (c, s) in at {
            self.drop_sql(
                c,
                &format!("DROP DATABASE IF EXISTS {}", sprig_common::names::quote(s)),
            )
            .await;
        }
    }

    async fn drop_sql(&self, cluster: &ClusterId, sql: &str) {
        if let Ok(pool) = self.pool(cluster).await
            && let Ok(mut c) = pool.get_conn().await
            && let Err(e) = c.query_drop(sql).await
        {
            tracing::warn!(error = %e, sql, "dropping graph storage failed");
        }
    }

    /// `DROP GRAPH`: the catalog rows in one change (with the group and
    /// its members), then the storage.
    /// Returns the catalog version (0 when nothing was dropped).
    ///
    /// # Errors
    /// 39902 for an unknown graph (unless `if_exists`).
    pub async fn drop(
        &self,
        tenant: TenantId,
        db: &str,
        name: &str,
        if_exists: bool,
    ) -> Result<u64> {
        self.prepare(tenant).await?;
        let cat = self.catalog(tenant, Freshness::Latest).await?;
        let found = match self.resolve(&cat, db, name).await {
            Err(e) if if_exists && e.code() == Some(SprigCode::GraphNotFound) => None,
            r => r?,
        };
        let Some(g) = found else {
            if if_exists {
                return Ok(0);
            }
            return Err(not_found(db, name));
        };
        let group = g.def.group_id;
        let mut ch = self
            .svc
            .catalog
            .change(tenant, VersionKind::Catalog)
            .await?;
        let version = ch.version();
        let r = {
            let (store, tx) = ch.tx().parts();
            let cat_schema = store.schema().to_owned();
            match store
                .set_object_state(tx, ObjectId(g.graph_id), ObjectState::Dropped, version)
                .await
            {
                Ok(_) => match GraphCatalog::new(&cat_schema).delete(tx, g.graph_id).await {
                    Ok(()) => match group {
                        Some(gid) => delete_group(tx, &cat_schema, gid).await,
                        None => Ok(()),
                    },
                    Err(e) => Err(e),
                },
                Err(e) => Err(e),
            }
        };
        if let Err(e) = r {
            let _ = ch.rollback().await;
            return Err(e);
        }
        let v = ch.commit().await?;
        if group.is_some() {
            let at: Vec<(ClusterId, String)> = g
                .shards
                .shards
                .iter()
                .filter_map(|s| {
                    ClusterId::new(&s.cluster)
                        .ok()
                        .map(|c| (c, s.schema.clone()))
                })
                .collect();
            self.drop_schemas(&at).await;
            self.drop_physical(tenant, &g.cluster, &[], &g.tables(), group)
                .await;
        } else {
            self.drop_sql(&g.cluster, &g.tables().drop_ddl()).await;
        }
        self.counters
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&(tenant.0, g.graph_id));
        tracing::info!(tenant = tenant.0, db, graph = name, "graph dropped");
        Ok(v)
    }

    /// The table objects a mapped graph reads, by name: `(table, object)`.
    ///
    /// # Errors
    /// 39908 for a table that doesn't exist or is in a sharded group.
    pub fn mapped_sources(
        &self,
        cat: &TenantCatalog,
        db: DbId,
        def: &sprig_graph::mapped::MappedDef,
    ) -> Result<Vec<(String, u64)>> {
        let lctn = self.lctn(cat);
        let sharded: HashSet<u64> = cat
            .ext
            .get::<crate::exec::router::layout::ShardingState>(sprig_shard::groups::MODULE)
            .map(|st| {
                st.members
                    .iter()
                    .filter(|m| {
                        st.groups
                            .iter()
                            .any(|g| g.id == m.group && g.kind == "sharded")
                    })
                    .map(|m| m.object)
                    .collect()
            })
            .unwrap_or_default();
        let mut out: Vec<(String, u64)> = Vec::new();
        let tables = def
            .vertices
            .iter()
            .map(|v| &v.table)
            .chain(def.edges.iter().map(|e| &e.table));
        for t in tables {
            if out.iter().any(|(x, _)| x == t) {
                continue;
            }
            let key = object_key(ObjectKind::Table, t, lctn);
            let o = cat
                .object(db, Namespace::Relation, &key)
                .filter(|o| o.state == ObjectState::Active && o.kind == ObjectKind::Table)
                .ok_or_else(|| {
                    SprigError::new(
                        SprigCode::GraphDefinitionInvalid,
                        format!("Table '{t}' doesn't exist"),
                    )
                })?;
            if sharded.contains(&o.object_id.0) {
                return Err(SprigError::new(
                    SprigCode::GraphDefinitionInvalid,
                    format!(
                        "table '{t}' is sharded: mapped graphs over sharded tables aren't built yet"
                    ),
                ));
            }
            out.push((t.clone(), o.object_id.0));
        }
        Ok(out)
    }

    /// `CREATE PROPERTY GRAPH`: checks the tables and their columns (every
    /// column of a table without a property list; each expression runs once
    /// with `LIMIT 0`), then writes the `objects` and `graphs` rows (kind
    /// `mapped`, the definition as JSON) in one change. `can_read(object)`
    /// is chapter 29's `SELECT` check on each table. Returns the catalog
    /// version (that of the snapshot when `IF NOT EXISTS` found the graph).
    ///
    /// # Errors
    /// 39908 for a name in use, a missing table or column, a sharded table
    /// or a bad expression; 1142 for a table the user can't read.
    #[allow(clippy::too_many_lines)] // tables, columns, probe, catalog
    pub async fn create_mapped(
        &self,
        tenant: TenantId,
        db: &str,
        c: &sprig_graph::mapped::CreatePropertyGraph,
        can_read: &(dyn Fn(u64) -> bool + Send + Sync),
    ) -> Result<u64> {
        self.prepare(tenant).await?;
        let cat = self.catalog(tenant, Freshness::Latest).await?;
        if let Some(g) = self.resolve(&cat, db, &c.name).await? {
            if c.if_not_exists {
                return Ok(cat.version);
            }
            if !(c.or_replace && g.def.kind == "mapped") {
                return Err(SprigError::new(
                    SprigCode::GraphDefinitionInvalid,
                    format!("Graph '{db}.{}' already exists", c.name),
                ));
            }
            self.drop(tenant, db, &c.name, true).await?;
        }
        let cat = self.catalog(tenant, Freshness::Latest).await?;
        let lctn = self.lctn(&cat);
        let row = cat
            .database(&database_key(db, lctn))
            .cloned()
            .ok_or_else(|| not_found(db, &c.name))?;
        let schema = sprig_catalog::ddl::shard_schemas(tenant, &row)
            .first()
            .map(ToString::to_string)
            .ok_or_else(|| not_found(db, &c.name))?;
        let sources = self.mapped_sources(&cat, row.db_id, &c.def)?;
        for (t, o) in &sources {
            if !can_read(*o) {
                return Err(SprigError::standard(
                    1142,
                    *b"42000",
                    format!(
                        "SELECT command denied for table '{t}' (a source of graph '{}')",
                        c.name
                    ),
                ));
            }
        }
        let cluster = self.svc.locator.cluster(tenant).await?;
        let pool = self.pool(&cluster).await?;
        let mut conn = pool.get_conn().await.map_err(backend_error)?;
        let mut def = c.def.clone();
        let invalid = |m: String| SprigError::new(SprigCode::GraphDefinitionInvalid, m);
        let mut columns_of = HashMap::new();
        for (t, _) in &sources {
            let cols: Vec<String> = conn
                .exec(
                    sprig_graph::mapped::MappedStore::columns_sql(),
                    (schema.as_str(), t.as_str()),
                )
                .await
                .map_err(backend_error)?;
            let cols: Vec<String> = cols
                .into_iter()
                .filter(|x| !x.starts_with("_sprig"))
                .collect();
            columns_of.insert(t.clone(), cols);
        }
        let check_props =
            |table: &str, key: &str, props: &mut Option<Vec<(String, String)>>| -> Result<()> {
                let cols = &columns_of[table];
                if !cols.iter().any(|x| x.eq_ignore_ascii_case(key)) {
                    return Err(invalid(format!("table '{table}' has no column '{key}'")));
                }
                if props.is_none() {
                    *props = Some(cols.iter().map(|x| (x.clone(), x.clone())).collect());
                }
                Ok(())
            };
        for v in &mut def.vertices {
            check_props(&v.table, &v.key, &mut v.properties)?;
        }
        for e in &mut def.edges {
            check_props(&e.table, &e.key, &mut e.properties)?;
            for col in [&e.src.column, &e.dst.column] {
                if !columns_of[&e.table]
                    .iter()
                    .any(|x| x.eq_ignore_ascii_case(col))
                {
                    return Err(invalid(format!(
                        "table '{}' has no column '{col}'",
                        e.table
                    )));
                }
            }
        }
        // Each table's key and properties, once, with nothing read.
        let probes = def
            .vertices
            .iter()
            .map(|v| (&v.table, &v.key, v.properties.clone().unwrap_or_default()))
            .chain(
                def.edges
                    .iter()
                    .map(|e| (&e.table, &e.key, e.properties.clone().unwrap_or_default())),
            );
        for (t, key, props) in probes {
            let mut sql = format!("SELECT v.`{}`", key.replace('`', "``"));
            for (_, e) in &props {
                if e.chars().all(|c| c.is_alphanumeric() || c == '_') {
                    let _ = write!(sql, ", v.`{e}`");
                } else {
                    let _ = write!(sql, ", {e}");
                }
            }
            let _ = write!(
                sql,
                " FROM {}.{} v LIMIT 0",
                sprig_common::names::quote(&schema),
                sprig_common::names::quote(t)
            );
            conn.query_drop(&sql)
                .await
                .map_err(|e| invalid(format!("a property of table '{t}' doesn't evaluate: {e}")))?;
        }
        drop(conn);
        let text = serde_json::to_string(&def).map_err(|e| SprigError::internal(e.to_string()))?;
        let ep = self.svc.locator.writer(tenant).await?;
        let object_id = {
            let mut c = ep.pool.get_conn().await.map_err(backend_error)?;
            ep.store.next_object_id(&mut c).await?
        };
        let mut ch = self
            .svc
            .catalog
            .change(tenant, VersionKind::Catalog)
            .await?;
        let version = ch.version();
        let r = {
            let (store, tx) = ch.tx().parts();
            let obj = ObjectRow {
                object_id,
                db_id: row.db_id,
                namespace: Namespace::Graph,
                name: c.name.clone(),
                name_key: object_key(ObjectKind::Graph, &c.name, lctn),
                kind: ObjectKind::Graph,
                parent_id: None,
                state: ObjectState::Active,
                schema_version: 1,
                flags: ObjectFlags::default(),
                pending_job_id: None,
                created_at: SystemTime::now(),
                changed_version: version,
            };
            let cat_schema = store.schema().to_owned();
            match store.insert_object(tx, &obj).await {
                Ok(()) => {
                    GraphCatalog::new(&cat_schema)
                        .insert_mapped(tx, object_id.0, row.db_id.0, &text)
                        .await
                }
                Err(e) => Err(e),
            }
        };
        if let Err(e) = r {
            let _ = ch.rollback().await;
            return Err(e);
        }
        let v = ch.commit().await?;
        tracing::info!(tenant = tenant.0, db, graph = %c.name, id = object_id.0, "mapped graph created");
        Ok(v)
    }

    /// Runs a read-only Cypher statement on graph `db.name` for a SQL
    /// session (`GRAPH_TABLE`): `SELECT` on the graph (and, for a mapped
    /// graph, on each source table), the graph budgets, read-only
    /// transactions on the writers.
    ///
    /// # Errors
    /// 39902 for an unknown graph; 1142; the executor's errors.
    #[allow(clippy::too_many_lines)] // resolve, check, open, run, end
    pub async fn query_read(
        self: &Arc<Self>,
        cat: &TenantCatalog,
        grants: Option<&dyn SessionGrants>,
        user: &str,
        db: &str,
        name: &str,
        cypher: &str,
    ) -> Result<sprig_graph::Output> {
        let q = sprig_graph::parser::parse(cypher)?;
        let g = self
            .resolve(cat, db, name)
            .await?
            .ok_or_else(|| not_found(db, name))?;
        let denied = |what: &str| {
            SprigError::standard(
                1142,
                *b"42000",
                format!("SELECT command denied to user '{user}' for {what}"),
            )
        };
        if !self.allows(
            grants,
            cat,
            &g.db,
            g.db_id,
            Some(g.graph_id),
            Action::Select,
        ) {
            return Err(denied(&format!("graph '{}.{}'", g.db, g.name)));
        }
        let ctx = cat.setting_ctx(&self.svc.dc).with_db(g.db_id);
        let budgets = sprig_graph::settings::budgets(&|s| {
            self.svc.settings.get(s, &ctx, Some(&cat.settings))
        });
        let mut rc = txn::ReadConns::default();
        let opened: Result<()> = async {
            for c in rc.missing(&g.shards) {
                let cluster = ClusterId::new(&c)
                    .map_err(|e| SprigError::internal(format!("cluster id {c}: {e}")))?;
                let pool = self.session_pool(&cluster).await?;
                let mut conn = pool.get_conn().await.map_err(backend_error)?;
                conn.query_drop("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                    .await
                    .map_err(backend_error)?;
                conn.query_drop("START TRANSACTION READ ONLY")
                    .await
                    .map_err(backend_error)?;
                rc.conns.insert(c, conn);
            }
            Ok(())
        }
        .await;
        if let Err(e) = opened {
            rc.end().await;
            return Err(e);
        }
        let r = if g.def.kind == "mapped" {
            let def: sprig_graph::mapped::MappedDef =
                serde_json::from_str(g.def.definition.as_deref().unwrap_or("{}"))
                    .map_err(|e| SprigError::internal(format!("mapped graph definition: {e}")))?;
            let mut ok = true;
            for (t, o) in self.mapped_sources(cat, g.db_id, &def)? {
                if !self.allows(grants, cat, &g.db, g.db_id, Some(o), Action::Select) {
                    rc.end().await;
                    return Err(denied(&format!("table '{}.{t}'", g.db)));
                }
                ok &= true;
            }
            match rc.conns.get_mut(g.cluster.as_str()) {
                Some(conn) if ok => {
                    let mut store =
                        sprig_graph::mapped::MappedStore::new(conn, g.graph_id, &g.schema, def);
                    sprig_graph::exec::run(
                        &mut store,
                        &q,
                        &sprig_graph::eval::Params::new(),
                        &budgets,
                    )
                    .await
                }
                _ => Err(SprigError::internal("no connection for a mapped graph")),
            }
        } else {
            let (labels, types) = self.names_of(&g);
            let dict = self.dictionary(&g);
            let ids = self.counters(&g).await?;
            let mut store = sprig_graph::maria::MariaStore::new(
                &mut rc,
                Arc::clone(&g.shards),
                labels,
                types,
                dict,
                ids,
            )
            .with_schema(g.def.indexes.clone(), None);
            sprig_graph::exec::run(&mut store, &q, &sprig_graph::eval::Params::new(), &budgets)
                .await
        };
        rc.end().await;
        let out = r?;
        if out.qtype != "r" {
            return Err(SprigError::new(
                SprigCode::GraphUnsupported,
                "GRAPH_TABLE reads only",
            ));
        }
        Ok(out)
    }

    /// The labels and types of `g`: the snapshot's and those this process
    /// learned since.
    #[must_use]
    pub fn names_of(&self, g: &GraphRef) -> Names {
        let mut labels = g.def.labels.clone();
        let mut types = g.def.types.clone();
        if let Some((l, t)) = self
            .names
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(g.tenant.0, g.graph_id))
        {
            labels.extend(l.iter().map(|(k, v)| (*k, v.clone())));
            types.extend(t.iter().map(|(k, v)| (*k, v.clone())));
        }
        (labels, types)
    }

    fn learn(&self, tenant: TenantId, graph: u64, label: bool, id: u32, name: &str) {
        let mut m = self.names.lock().unwrap_or_else(PoisonError::into_inner);
        let e = m.entry((tenant.0, graph)).or_default();
        if label {
            e.0.insert(id, name.to_owned());
        } else {
            e.1.insert(id, name.to_owned());
        }
    }

    /// The label and type dictionary of `g`.
    #[must_use]
    pub fn dictionary(self: &Arc<Self>, g: &GraphRef) -> Arc<dyn Dictionary> {
        Arc::new(GwDict {
            graphs: Arc::clone(self),
            tenant: g.tenant,
            graph: g.graph_id,
        })
    }
}

/// Index and constraint commands of one graph.
struct GwSchema {
    graphs: Arc<GatewayGraphs>,
    g: GraphRef,
}

#[async_trait]
impl SchemaOps for GwSchema {
    async fn run(&self, cmd: &Schema) -> Result<SchemaCounts> {
        self.graphs.schema(&self.g, cmd).await
    }
}

fn neo_schema(code: &str, msg: impl std::fmt::Display) -> SprigError {
    SprigError::new(
        SprigCode::GraphDefinitionInvalid,
        format!("[Neo.ClientError.Schema.{code}] {msg}"),
    )
}

impl GatewayGraphs {
    /// How `g`'s index and constraint commands run.
    #[must_use]
    pub fn schema_ops(self: &Arc<Self>, g: &GraphRef) -> Arc<dyn SchemaOps> {
        Arc::new(GwSchema {
            graphs: Arc::clone(self),
            g: g.clone(),
        })
    }

    /// Runs `sql(tables)` on every shard of `g`, each on its cluster, as
    /// `sprig_ctl`; stops at the first error.
    async fn on_shards(&self, g: &GraphRef, sql: impl Fn(&Tables) -> Result<String>) -> Result<()> {
        for sh in &g.shards.shards {
            let cluster = ClusterId::new(&sh.cluster)
                .map_err(|e| SprigError::internal(format!("cluster id {}: {e}", sh.cluster)))?;
            let pool = self.pool(&cluster).await?;
            let mut c = pool.get_conn().await.map_err(backend_error)?;
            c.query_drop(sql(&sh.tables)?)
                .await
                .map_err(backend_error)?;
        }
        Ok(())
    }

    /// `g`'s indexes from the catalog now, with token names.
    async fn current_indexes(&self, g: &GraphRef) -> Result<Vec<IndexDef>> {
        let ep = self.svc.locator.writer(g.tenant).await?;
        let mut c = ep.pool.get_conn().await.map_err(backend_error)?;
        let rows = GraphCatalog::new(ep.store.schema())
            .indexes(&mut c, Some(g.graph_id))
            .await?;
        let (labels, types) = self.names_of(g);
        let mut out = Vec::with_capacity(rows.len());
        for (_, mut d) in rows {
            let names = if d.entity == Entity::Node {
                &labels
            } else {
                &types
            };
            if let Some(n) = names.get(&d.token_id) {
                d.token.clone_from(n);
            }
            out.push(d);
        }
        Ok(out)
    }

    /// `CREATE INDEX`, `CREATE CONSTRAINT`, `DROP INDEX`, `DROP
    /// CONSTRAINT` on graph `g`: catalog rows in versioned changes around
    /// the `ALTER TABLE` (see [`sprig_graph::schema`]).
    ///
    /// # Errors
    /// 39908 as [`plan_create`] and [`plan_drop`] say, or when existing
    /// data breaks a new constraint; catalog and backend errors.
    #[allow(clippy::too_many_lines)] // drop, or plan, record, alter, mark online
    pub async fn schema(self: &Arc<Self>, g: &GraphRef, cmd: &Schema) -> Result<SchemaCounts> {
        self.prepare(g.tenant).await?;
        let existing = self.current_indexes(g).await?;
        if let Schema::Drop { .. } = cmd {
            let Some(d) = plan_drop(&existing, cmd)? else {
                return Ok(vec![]);
            };
            // The catalog first, so no statement plans with the index once
            // its columns go.
            let mut ch = self
                .svc
                .catalog
                .change(g.tenant, VersionKind::Catalog)
                .await?;
            let r = {
                let (store, tx) = ch.tx().parts();
                GraphCatalog::new(store.schema())
                    .delete_index(tx, g.graph_id, d.id)
                    .await
            };
            if let Err(e) = r {
                let _ = ch.rollback().await;
                return Err(e);
            }
            ch.commit().await?;
            self.on_shards(g, |t| Ok(t.index_drop_ddl(&d))).await?;
            if d.unique && g.shards.group != 0 {
                self.on_shards(g, |t| {
                    Ok(format!("DELETE FROM {} WHERE idx = {}", t.claims, d.id))
                })
                .await?;
            }
            tracing::info!(tenant = g.tenant.0, graph = g.graph_id, index = %d.name, "graph index dropped");
            return Ok(vec![(
                if d.unique {
                    "constraints-removed"
                } else {
                    "indexes-removed"
                },
                1,
            )]);
        }
        let Some(mut d) = plan_create(&existing, cmd)? else {
            return Ok(vec![]);
        };
        d.token_id = self
            .dictionary(g)
            .create(d.entity == Entity::Node, &d.token)
            .await?;
        let mut ch = self
            .svc
            .catalog
            .change(g.tenant, VersionKind::Catalog)
            .await?;
        let r = {
            let (store, tx) = ch.tx().parts();
            GraphCatalog::new(store.schema())
                .insert_index(tx, g.graph_id, &d)
                .await
        };
        d.id = match r {
            Ok(id) => id,
            Err(e) => {
                let _ = ch.rollback().await;
                return Err(match e.kind {
                    sprig_common::ErrorKind::Backend { errno: 1062, .. } => neo_schema(
                        "IndexWithNameAlreadyExists",
                        format!("There already exists an index called '{}'.", d.name),
                    ),
                    _ => e,
                });
            }
        };
        ch.commit().await?;
        let mut altered = self.on_shards(g, |t| t.index_ddl(&d)).await;
        if altered.is_ok() && d.unique && g.shards.group != 0 {
            altered = self.backfill_claims(g, &d).await;
        }
        let mut ch = self
            .svc
            .catalog
            .change(g.tenant, VersionKind::Catalog)
            .await?;
        let r = {
            let (store, tx) = ch.tx().parts();
            let gc = GraphCatalog::new(store.schema());
            if altered.is_ok() {
                gc.index_online(tx, g.graph_id, d.id).await
            } else {
                gc.delete_index(tx, g.graph_id, d.id).await
            }
        };
        if let Err(e) = r {
            let _ = ch.rollback().await;
            return Err(e);
        }
        ch.commit().await?;
        if let Err(e) = altered {
            let _ = self.on_shards(g, |t| Ok(t.index_drop_ddl(&d))).await;
            if d.unique && g.shards.group != 0 {
                let _ = self
                    .on_shards(g, |t| {
                        Ok(format!("DELETE FROM {} WHERE idx = {}", t.claims, d.id))
                    })
                    .await;
            }
            return Err(match e.kind {
                sprig_common::ErrorKind::Backend { errno: 1062, .. } => neo_schema(
                    "ConstraintCreationFailed",
                    format!(
                        "Unable to create constraint {}: existing data has duplicate values of {}",
                        d.name,
                        sprig_graph::schema::describe(d.entity, &d.token, &d.props)
                    ),
                ),
                _ => e,
            });
        }
        tracing::info!(tenant = g.tenant.0, graph = g.graph_id, index = %d.name, "graph index created");
        Ok(vec![(
            if d.unique {
                "constraints-added"
            } else {
                "indexes-added"
            },
            1,
        )])
    }
}

impl GatewayGraphs {
    /// Claims every existing key of new constraint `d` (decision G41),
    /// after its `ALTER TABLE` and while writers already claim (the
    /// catalog says `building`): each shard's elements with the property
    /// columns set, in pages of 1,000 ids, as autocommit inserts on the
    /// claims' shards. A key held by another live element with the same
    /// key is a duplicate across shards (1062).
    ///
    /// # Errors
    /// 1062 naming the two elements; backend errors.
    #[allow(
        clippy::too_many_lines,
        clippy::many_single_char_names,
        clippy::items_after_statements
    )] // page, claim, check the holder
    async fn backfill_claims(&self, g: &GraphRef, d: &IndexDef) -> Result<()> {
        use sprig_graph::schema::{claim_key, claim_rk};
        let layout = &g.shards;
        let mut conns: HashMap<String, mysql_async::Conn> = HashMap::new();
        async fn conn<'a>(
            me: &GatewayGraphs,
            conns: &'a mut HashMap<String, mysql_async::Conn>,
            cluster: &str,
        ) -> Result<&'a mut mysql_async::Conn> {
            if !conns.contains_key(cluster) {
                let id = ClusterId::new(cluster)
                    .map_err(|e| SprigError::internal(format!("cluster id {cluster}: {e}")))?;
                let c = me
                    .pool(&id)
                    .await?
                    .get_conn()
                    .await
                    .map_err(backend_error)?;
                conns.insert(cluster.to_owned(), c);
            }
            conns
                .get_mut(cluster)
                .ok_or_else(|| SprigError::internal("claim backfill connection"))
        }
        let col = Tables::index_column(d, 0);
        let node = d.entity == Entity::Node;
        let mut claimed = 0u64;
        for sh in &layout.shards {
            let mut after = 0u64;
            loop {
                let sql = if node {
                    format!(
                        "SELECT id, _sprig_rk, labels, props FROM {} WHERE id > {after} \
                         AND `{col}` IS NOT NULL ORDER BY id LIMIT 1000",
                        sh.tables.nodes
                    )
                } else {
                    format!(
                        "SELECT id, _sprig_rk, CAST(type AS CHAR), props FROM {} WHERE id > {after} \
                         AND `{col}` IS NOT NULL ORDER BY id LIMIT 1000",
                        sh.tables.edges
                    )
                };
                let rows: Vec<(u64, u64, String, String)> = conn(self, &mut conns, &sh.cluster)
                    .await?
                    .query(sql)
                    .await
                    .map_err(backend_error)?;
                let Some(last) = rows.last() else { break };
                after = last.0;
                for (id, rk, tok, props) in rows {
                    let props: serde_json::Value =
                        serde_json::from_str(&props).unwrap_or(serde_json::Value::Null);
                    let has = if node {
                        serde_json::from_str::<Vec<u32>>(&tok)
                            .unwrap_or_default()
                            .contains(&d.token_id)
                    } else {
                        tok.parse::<u32>().ok() == Some(d.token_id)
                    };
                    let Some(key) = has.then(|| claim_key(d, &props)).flatten() else {
                        continue;
                    };
                    let h = claim_rk(d.id, &key);
                    let at = &layout.shards[layout.index_of(h)];
                    let c = conn(self, &mut conns, &at.cluster).await?;
                    c.exec_drop(
                        format!(
                            "INSERT INTO {} (idx, k, owner, owner_rk, _sprig_rk) VALUES (?, ?, ?, ?, ?) \
                             ON DUPLICATE KEY UPDATE owner = owner",
                            at.tables.claims
                        ),
                        (d.id, key.clone(), id, rk, h),
                    )
                    .await
                    .map_err(backend_error)?;
                    let holder: Option<(u64, u64)> = c
                        .exec_first(
                            format!(
                                "SELECT owner, owner_rk FROM {} WHERE idx = ? AND k = ?",
                                at.tables.claims
                            ),
                            (d.id, key.clone()),
                        )
                        .await
                        .map_err(backend_error)?;
                    claimed += 1;
                    let Some((o, ork)) = holder.filter(|(o, _)| *o != id) else {
                        continue;
                    };
                    // Another element holds the key: a duplicate if it is
                    // live with the same key, else an orphan to take over.
                    let os = &layout.shards[layout.index_of(ork)];
                    let other: Option<(String, String)> = if node {
                        conn(self, &mut conns, &os.cluster)
                            .await?
                            .query_first(format!(
                                "SELECT CAST(labels AS CHAR), props FROM {} WHERE id = {o}",
                                os.tables.nodes
                            ))
                            .await
                            .map_err(backend_error)?
                    } else {
                        conn(self, &mut conns, &os.cluster)
                            .await?
                            .query_first(format!(
                                "SELECT CAST(type AS CHAR), props FROM {} WHERE id = {o}",
                                os.tables.edges
                            ))
                            .await
                            .map_err(backend_error)?
                    };
                    let live = other.is_some_and(|(t, p)| {
                        let p: serde_json::Value =
                            serde_json::from_str(&p).unwrap_or(serde_json::Value::Null);
                        let has = if node {
                            serde_json::from_str::<Vec<u32>>(&t)
                                .unwrap_or_default()
                                .contains(&d.token_id)
                        } else {
                            t.parse::<u32>().ok() == Some(d.token_id)
                        };
                        has && claim_key(d, &p).as_deref() == Some(key.as_slice())
                    });
                    if live {
                        return Err(SprigError::backend(
                            1062,
                            *b"23000",
                            format!(
                                "{}({id}) and {}({o}) have the same key",
                                if node { "Node" } else { "Relationship" },
                                if node { "Node" } else { "Relationship" }
                            ),
                        ));
                    }
                    let c = conn(self, &mut conns, &at.cluster).await?;
                    c.exec_drop(
                        format!(
                            "UPDATE {} SET owner = ?, owner_rk = ? WHERE idx = ? AND k = ? AND owner = ?",
                            at.tables.claims
                        ),
                        (id, rk, d.id, key, o),
                    )
                    .await
                    .map_err(backend_error)?;
                }
            }
        }
        tracing::info!(tenant = g.tenant.0, graph = g.graph_id, index = %d.name, claimed, "uniqueness claims back-filled");
        Ok(())
    }
}

/// New labels and types as versioned catalog changes.
struct GwDict {
    graphs: Arc<GatewayGraphs>,
    tenant: TenantId,
    graph: u64,
}

#[async_trait]
impl Dictionary for GwDict {
    async fn create(&self, label: bool, name: &str) -> Result<u32> {
        // A busy catalog (concurrent DDL or other new labels) is retried.
        let mut tries = 0;
        loop {
            match self.create_once(label, name).await {
                Err(e)
                    if tries < 3
                        && matches!(
                            e.code(),
                            Some(SprigCode::CatalogConflict | SprigCode::CatalogUnavailable)
                        ) =>
                {
                    tries += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(50 << tries)).await;
                }
                r => return r,
            }
        }
    }
}

impl GwDict {
    async fn create_once(&self, label: bool, name: &str) -> Result<u32> {
        // Usually it exists already (another session or gateway made it):
        // no versioned change then.
        let ep = self.graphs.svc.locator.writer(self.tenant).await?;
        {
            let mut c = ep.pool.get_conn().await.map_err(backend_error)?;
            if let Some(id) = GraphCatalog::new(ep.store.schema())
                .find_source(&mut c, self.graph, label, name)
                .await?
            {
                self.graphs.learn(self.tenant, self.graph, label, id, name);
                return Ok(id);
            }
        }
        let mut ch = self
            .graphs
            .svc
            .catalog
            .change(self.tenant, VersionKind::Catalog)
            .await?;
        let r = {
            let (store, tx) = ch.tx().parts();
            GraphCatalog::new(store.schema())
                .add_source(tx, self.graph, label, name)
                .await
        };
        match r {
            Ok(id) => {
                ch.commit().await?;
                self.graphs.learn(self.tenant, self.graph, label, id, name);
                Ok(id)
            }
            Err(e) => {
                let _ = ch.rollback().await;
                Err(e)
            }
        }
    }
}

/// A new group id from the catalog's `sprig.next_group_id`, kept past every
/// group id in use (as chapter 16's apply does).
async fn alloc_group_id(c: &mut mysql_async::Conn, cat: &str) -> Result<u32> {
    let cat = cat.replace('`', "``");
    c.exec_drop(
        format!(
            "INSERT INTO `{cat}`.settings (scope_kind, scope_id, setting_key, value, bounds, \
             set_by, set_at, version) VALUES ('tenant', 0, 'sprig.next_group_id', '1', NULL, \
             'system', UTC_TIMESTAMP(6), 0) ON DUPLICATE KEY UPDATE setting_key = setting_key"
        ),
        (),
    )
    .await
    .map_err(backend_error)?;
    let floor: Option<u64> = c
        .query_first(format!(
            "SELECT GREATEST(COALESCE((SELECT MAX(default_group_id) FROM `{cat}`.`databases`), 0), \
             COALESCE((SELECT MAX(group_id) FROM `{cat}`.table_groups), 0)) + 1"
        ))
        .await
        .map_err(backend_error)?;
    c.exec_drop(
        format!(
            "UPDATE `{cat}`.settings SET value = LAST_INSERT_ID(GREATEST(CAST(value AS UNSIGNED), ?) + 1), \
             set_at = UTC_TIMESTAMP(6) WHERE scope_kind = 'tenant' AND scope_id = 0 \
             AND setting_key = 'sprig.next_group_id'"
        ),
        (floor.unwrap_or(1),),
    )
    .await
    .map_err(backend_error)?;
    let next: Option<u64> = c
        .query_first("SELECT LAST_INSERT_ID()")
        .await
        .map_err(backend_error)?;
    next.and_then(|n| n.checked_sub(1))
        .and_then(|n| u32::try_from(n).ok())
        .filter(|n| *n > 0)
        .ok_or_else(|| SprigError::internal("group ids exhausted"))
}

/// The graph's `table_groups` row (sharded, keyed by `part`, origin
/// `structural`) and its four members: nodes on `part`; edges, labels and
/// mirrors through their node (`src`, `id`, `dst`). Members carry object
/// ids of their own, with no `objects` row, so the SQL router never
/// resolves a name to them. A presplit group keeps its shard count as the
/// group's `min_shards`, so merges never undo the presplit.
#[allow(clippy::too_many_arguments)]
async fn insert_group<Q: Queryable + Send>(
    q: &mut Q,
    cat: &str,
    group: u32,
    shards: u32,
    spec: &KeySpec,
    db: DbId,
    graph: u64,
    members: [u64; 4],
) -> Result<()> {
    let cat = cat.replace('`', "``");
    q.exec_drop(
        format!(
            "INSERT INTO `{cat}`.table_groups (group_id, kind, name, key_spec, key_spec_digest, \
             state, min_shards, origin, created_at, updated_at) VALUES (?, 'sharded', ?, ?, ?, \
             'active', ?, 'structural', UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))"
        ),
        (
            group,
            format!("graph_{graph}"),
            spec.to_json().to_string(),
            spec.digest,
            (shards > 1).then_some(shards),
        ),
    )
    .await
    .map_err(backend_error)?;
    let [nodes, edges, labels, mirrors] = members;
    let rows: [Member<'_>; 4] = [
        (nodes, "columns", Some("[\"part\"]"), None, None),
        (edges, "parent", None, Some(nodes), Some("src")),
        (labels, "parent", None, Some(nodes), Some("id")),
        (mirrors, "parent", None, Some(nodes), Some("dst")),
    ];
    for (o, routing, cols, parent, fk) in rows {
        q.exec_drop(
            format!(
                "INSERT INTO `{cat}`.group_members (object_id, group_id, db_id, routing, key_columns, \
                 parent_object_id, parent_fk, rk_state, pinned, joined_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, 'ready', 0, UTC_TIMESTAMP(6))"
            ),
            (o, group, db.0, routing, cols, parent, fk),
        )
        .await
        .map_err(backend_error)?;
    }
    Ok(())
}

/// A member row: object, routing, key columns, parent, parent's column.
type Member<'a> = (u64, &'a str, Option<&'a str>, Option<u64>, Option<&'a str>);

/// Deletes a graph's group and its members.
async fn delete_group<Q: Queryable + Send>(q: &mut Q, cat: &str, group: u32) -> Result<()> {
    let cat = cat.replace('`', "``");
    for t in ["group_members", "table_groups"] {
        q.exec_drop(
            format!("DELETE FROM `{cat}`.{t} WHERE group_id = ?"),
            (group,),
        )
        .await
        .map_err(backend_error)?;
    }
    Ok(())
}
