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
//!   `CREATE GRAPH` needs `CREATE`, `DROP GRAPH` `DROP`. Per-label grants
//!   (`GRANT READ ON GRAPH ... NODES ...`) are not built.

use std::collections::{BTreeMap, HashMap, HashSet};
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
use sprig_common::{Result, SprigCode, SprigError};
use sprig_graph::ast::{Entity, Schema};
use sprig_graph::catalog::{GraphCatalog, GraphDef, GraphState, MODULE, Tables, backend_error};
use sprig_graph::maria::{CounterIds, Dictionary, GraphShards, SchemaOps, ShardLoc};
use sprig_graph::schema::{plan_create, plan_drop};
use sprig_graph::store::{IndexDef, SchemaCounts};
use sprig_common::ids::{DcId, GroupId, ShardId};
use sprig_meta::{Tier, TierView};
use sprig_shard::{KeyClass, KeySpec, ShardMap, ShardMapCache, ShardMapStore, shard_schema};

use crate::tenant::TenantServices;

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
    /// Chapter 18's coordinator pieces for Bolt transactions.
    pub(crate) coord: txn::CoordServices,
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
            coord: txn::CoordServices::new(),
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
        let shards = map
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
        Ok((
            Arc::new(GraphShards {
                graph: def.graph_id,
                db: def.db_id,
                group,
                version: map.version,
                shards,
                seq: Tables::of(default_schema, def.graph_id).seq,
                mirrors: def.mirrors,
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
        let shards = u32::try_from(self.svc.settings.get(
            &sprig_balancer::shards::settings::MIN_SHARDS,
            &cat.setting_ctx(&self.svc.dc).with_db(row.db_id),
            Some(&cat.settings),
        ))
        .unwrap_or(1)
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
                    || format!("CREATE DATABASE IF NOT EXISTS {}", sprig_common::names::quote(s)),
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
                        insert_group(tx, &cat_schema, group, &spec, row.db_id, object_id.0, members)
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
        self.drop_schemas(&schemas.iter().map(|s| (cluster.clone(), s.clone())).collect::<Vec<_>>())
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
            self.drop_sql(c, &format!("DROP DATABASE IF EXISTS {}", sprig_common::names::quote(s)))
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
                .filter_map(|s| ClusterId::new(&s.cluster).ok().map(|c| (c, s.schema.clone())))
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
    async fn on_shards(
        &self,
        g: &GraphRef,
        sql: impl Fn(&Tables) -> Result<String>,
    ) -> Result<()> {
        for sh in &g.shards.shards {
            let cluster = ClusterId::new(&sh.cluster)?;
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
        let altered = self.on_shards(g, |t| t.index_ddl(&d)).await;
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
/// resolves a name to them.
async fn insert_group<Q: Queryable + Send>(
    q: &mut Q,
    cat: &str,
    group: u32,
    spec: &KeySpec,
    db: DbId,
    graph: u64,
    members: [u64; 4],
) -> Result<()> {
    let cat = cat.replace('`', "``");
    q.exec_drop(
        format!(
            "INSERT INTO `{cat}`.table_groups (group_id, kind, name, key_spec, key_spec_digest, \
             state, origin, created_at, updated_at) VALUES (?, 'sharded', ?, ?, ?, 'active', \
             'structural', UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))"
        ),
        (
            group,
            format!("graph_{graph}"),
            spec.to_json().to_string(),
            spec.digest,
        ),
    )
    .await
    .map_err(backend_error)?;
    let [nodes, edges, labels, mirrors] = members;
    let rows: [(u64, &str, Option<&str>, Option<u64>, Option<&str>); 4] = [
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
