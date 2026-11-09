//! Graphs in the gateway (chapter 43, R8): finding, creating and dropping
//! native graphs through the tenant catalog, their storage on the shard,
//! and chapter 29's checks. Used by the Bolt front end ([`crate::bolt`])
//! and by `CREATE GRAPH` / `DROP GRAPH` in SQL ([`crate::exec`]).
//!
//! - **Catalog.** A graph is an `objects` row (kind `graph`, namespace
//!   `graph`) and a `graphs` row, written in one versioned change; labels
//!   and relationship types are `graph_node_sources` and
//!   `graph_edge_sources` rows, added on first use in their own change.
//! - **Storage.** The graph's tables are in its database's first shard
//!   schema, created and dropped as the cluster's `sprig_ctl` account.
//!   Statements run on a `sprig_ctl` connection of the tenant's cluster
//!   inside the Bolt transaction (`READ COMMITTED`).
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
use sprig_graph::catalog::{GraphCatalog, GraphDef, GraphState, MODULE, Tables, backend_error};
use sprig_graph::maria::{CounterIds, Dictionary};

use crate::tenant::TenantServices;

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
}

impl GraphRef {
    /// The graph's tables.
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
        }
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
        Ok(Some(GraphRef {
            tenant: cat.tenant,
            db_id: row.db_id,
            db: row.name.clone(),
            name: o.name.clone(),
            graph_id: o.object_id.0,
            schema,
            cluster,
            def,
        }))
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

    /// `CREATE GRAPH`: the physical tables, then the catalog rows in one
    /// change. Returns the graph and the catalog version that has it.
    ///
    /// # Errors
    /// 39902 for an unknown database; 39908 for a name in use (unless
    /// `if_not_exists`); catalog and backend errors.
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
        let ep = self.svc.locator.writer(tenant).await?;
        let object_id = {
            let mut c = ep.pool.get_conn().await.map_err(backend_error)?;
            ep.store.next_object_id(&mut c).await?
        };
        let cluster = self.svc.locator.cluster(tenant).await?;
        let tables = Tables::of(&schema, object_id.0);
        {
            let pool = self.pool(&cluster).await?;
            let mut c = pool.get_conn().await.map_err(backend_error)?;
            for s in tables.create_ddl() {
                c.query_drop(s).await.map_err(backend_error)?;
            }
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
            match store.insert_object(tx, &obj).await {
                Ok(()) => {
                    GraphCatalog::new(store.schema())
                        .insert_native(tx, object_id.0, row.db_id.0)
                        .await
                }
                Err(e) => Err(e),
            }
        };
        if let Err(e) = r {
            let _ = ch.rollback().await;
            self.drop_tables(&cluster, &tables).await;
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
            "graph created"
        );
        Ok((g, v))
    }

    async fn drop_tables(&self, cluster: &ClusterId, tables: &Tables) {
        if let Ok(pool) = self.pool(cluster).await
            && let Ok(mut c) = pool.get_conn().await
            && let Err(e) = c.query_drop(tables.drop_ddl()).await
        {
            tracing::warn!(error = %e, "dropping graph tables failed");
        }
    }

    /// `DROP GRAPH`: the catalog rows in one change, then the tables.
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
        let Some(g) = self.resolve(&cat, db, name).await? else {
            if if_exists {
                return Ok(0);
            }
            return Err(not_found(db, name));
        };
        let mut ch = self
            .svc
            .catalog
            .change(tenant, VersionKind::Catalog)
            .await?;
        let version = ch.version();
        let r = {
            let (store, tx) = ch.tx().parts();
            match store
                .set_object_state(tx, ObjectId(g.graph_id), ObjectState::Dropped, version)
                .await
            {
                Ok(_) => {
                    GraphCatalog::new(store.schema())
                        .delete(tx, g.graph_id)
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
        self.drop_tables(&g.cluster, &g.tables()).await;
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
