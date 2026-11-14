//! Docs search: the release's documentation, which the controller loads
//! into `__sprig_docs` on every cluster, searched by any signed-in user.
//!
//! - [`search`]: `POST /v1/docs:search`'s hybrid search: the top 40 by
//!   full-text and the top 40 by cosine distance to the query's
//!   embedding, fused by reciprocal rank with at most two hits per page.
//! - [`sql`]: the read-only virtual database `sprig_docs` in SQL sessions:
//!   `SELECT ... FROM sprig_docs.chunks ...` runs on the docs-reader
//!   account; anything else that names `sprig_docs` is refused.
//! - [`embed`]: the query embedding model, loaded on first use.
//! - [`http`]: the REST handler.
//!
//! **Reads.** Every read goes through `sprig_docs_r`, the docs-reader
//! account (`sprig_provision::docs_account`), which can `SELECT` from
//! `__sprig_docs` and nothing else; never through a tenant's account,
//! which has no grant there. The cluster is the caller's tenant's
//! catalog cluster when its pack is ready, else another cluster's copy.
//!
//! **Versions.** A search reads one release: the one asked for, else
//! this gateway's own when it is loaded, else the newest loaded.

pub mod embed;
pub mod http;
pub mod search;
pub mod sql;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Pool, PoolConstraints, PoolOpts};
use sprig_cluster::record::AdminState;
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::ids::{ClusterId, DcId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_docs::settings as keys;
use sprig_docs::table;
use sprig_settings::{LiveSettings, SettingCtx};

use crate::tenant::TenantServices;

/// Reader connections kept per cluster.
const POOL_MAX: usize = 4;
/// How long the list of a cluster's ready versions is reused.
const READY_TTL: Duration = Duration::from_secs(30);
/// The longest wait for a reader connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

static SERVICE: OnceLock<Arc<DocsService>> = OnceLock::new();

/// Makes `svc` the process's docs service (once; later calls are ignored).
pub fn install(svc: Arc<DocsService>) {
    let _ = SERVICE.set(svc);
}

/// The process's docs service, when the gateway has tenant logins.
#[must_use]
pub fn service() -> Option<Arc<DocsService>> {
    SERVICE.get().cloned()
}

/// "Docs search is unavailable": no cluster has the docs loaded, or
/// they're turned off.
#[must_use]
pub fn unavailable(why: impl Into<String>) -> SprigError {
    SprigError::new(
        SprigCode::DocsUnavailable,
        format!("docs search is unavailable: {}", why.into()),
    )
}

/// The docs: where they are and how to read them.
pub struct DocsService {
    tenants: Arc<TenantServices>,
    registry: ClusterRegistry,
    live: Option<(LiveSettings, DcId)>,
    /// Reader pools by cluster: the writer and credential they were made
    /// for, and when that was last checked.
    pools: Mutex<HashMap<ClusterId, (String, Pool, Instant)>>,
    ready: Mutex<HashMap<ClusterId, (Instant, Vec<String>)>>,
    /// The DC's clusters, by number, and when they were listed.
    clusters: Mutex<Option<(Instant, Vec<ClusterId>)>>,
    /// The query embedding model.
    pub model: embed::Model,
    /// Searches are keyword-only for want of the model (for the event on
    /// each change).
    degraded: AtomicBool,
}

impl std::fmt::Debug for DocsService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DocsService").finish_non_exhaustive()
    }
}

/// Where a read goes: a reader connection on a cluster with the version.
pub struct Target {
    /// The cluster.
    pub cluster: ClusterId,
    /// The release read.
    pub version: String,
    /// A `sprig_docs_r` connection.
    pub conn: Conn,
}

impl std::fmt::Debug for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Target")
            .field("cluster", &self.cluster)
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

impl DocsService {
    /// Over the tenant services' locator and stores, reading the `docs.*`
    /// settings from `live` (defaults without).
    ///
    /// # Errors
    /// The cluster registry's (a store that isn't a DC tier).
    pub fn new(tenants: Arc<TenantServices>, live: Option<(LiveSettings, DcId)>) -> Result<Self> {
        let registry =
            ClusterRegistry::new(Arc::clone(&tenants.global), tenants.locator.dc_store())?;
        Ok(Self {
            tenants,
            registry,
            live,
            pools: Mutex::new(HashMap::new()),
            ready: Mutex::new(HashMap::new()),
            clusters: Mutex::new(None),
            model: embed::Model::default(),
            degraded: AtomicBool::new(false),
        })
    }

    fn get<T: sprig_settings::SettingValue>(&self, s: &sprig_settings::Setting<T>) -> T {
        match &self.live {
            Some((l, dc)) => l.get(s, &SettingCtx::process(dc)),
            None => s.default.clone(),
        }
    }

    /// `docs.enabled`.
    #[must_use]
    pub fn enabled(&self) -> bool {
        self.get(&keys::ENABLED)
    }

    /// `docs.search_limit`.
    #[must_use]
    pub fn default_limit(&self) -> usize {
        usize::try_from(self.get(&keys::SEARCH_LIMIT)).unwrap_or(8)
    }

    /// `docs.embed_queries`.
    #[must_use]
    pub fn embed_queries(&self) -> bool {
        self.get(&keys::EMBED_QUERIES)
    }

    /// `docs.embed_threads`.
    #[must_use]
    pub fn embed_threads(&self) -> usize {
        usize::try_from(self.get(&keys::EMBED_THREADS)).unwrap_or(2)
    }

    /// `docs.model_dir`, else the default next to the binary.
    #[must_use]
    pub fn model_dir(&self) -> PathBuf {
        let set = self.get(&keys::MODEL_DIR);
        if set.trim().is_empty() {
            keys::next_to_binary(keys::DEFAULT_MODEL_DIR)
                .unwrap_or_else(|| PathBuf::from(keys::DEFAULT_MODEL_DIR))
        } else {
            PathBuf::from(set.trim())
        }
    }

    /// The query's embedding, or why there is none (embedding off, or the
    /// model unavailable: then `docs.search.degraded` is raised once per
    /// change and searches are keyword-only).
    pub async fn embed_query(&self, query: &str) -> Option<Vec<f32>> {
        if !self.embed_queries() {
            return None;
        }
        match self
            .model
            .embed(&self.model_dir(), self.embed_threads(), query)
            .await
        {
            Ok(v) => {
                if self.degraded.swap(false, Ordering::Relaxed) {
                    tracing::info!(
                        event = "docs.search.degraded",
                        degraded = false,
                        "docs search embeds queries again"
                    );
                }
                Some(v)
            }
            Err(why) => {
                metrics::counter!("sprig_docs_search_degraded_total").increment(1);
                if !self.degraded.swap(true, Ordering::Relaxed) {
                    tracing::warn!(event = "docs.search.degraded", degraded = true, error = %why,
                        "the docs embedding model is unavailable: docs search is keyword-only");
                }
                None
            }
        }
    }

    /// The `sprig_docs_r` pool of `cluster`'s writer; the writer and the
    /// credential are looked up again at most every [`READY_TTL`].
    async fn pool(&self, cluster: &ClusterId) -> Result<Pool> {
        if let Some((_, p, at)) = self
            .pools
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(cluster)
            && at.elapsed() < READY_TTL
        {
            return Ok(p.clone());
        }
        let stored = self.registry.get(cluster).await?.ok_or_else(|| {
            SprigError::new(SprigCode::ClusterNotFound, format!("no cluster {cluster}"))
        })?;
        let ctl_ref = stored.body().secrets.ctl.clone();
        let opts = self.tenants.locator.ctl_opts(cluster).await?;
        let fingerprint = format!("{}|{}|{ctl_ref}", opts.ip_or_hostname(), opts.tcp_port());
        if let Some((f, p, at)) = self
            .pools
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_mut(cluster)
            && *f == fingerprint
        {
            *at = Instant::now();
            return Ok(p.clone());
        }
        let cred = sprig_provision::docs_account::docs_credential(&ctl_ref)?;
        let constraints = PoolConstraints::new(0, POOL_MAX).unwrap_or_default();
        let pool = Pool::new(
            OptsBuilder::from_opts(opts)
                .user(Some(cred.user.clone()))
                .pass(Some(cred.password().to_owned()))
                .db_name(None::<String>)
                .pool_opts(
                    PoolOpts::new()
                        .with_constraints(constraints)
                        .with_inactive_connection_ttl(Duration::from_secs(60)),
                ),
        );
        let old = self
            .pools
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(cluster.clone(), (fingerprint, pool.clone(), Instant::now()));
        if let Some((_, old, _)) = old {
            tokio::spawn(async move {
                let _ = old.disconnect().await;
            });
        }
        Ok(pool)
    }

    async fn ready_versions(&self, cluster: &ClusterId, conn: &mut Conn) -> Result<Vec<String>> {
        if let Some((at, v)) = self
            .ready
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(cluster)
            && at.elapsed() < READY_TTL
        {
            return Ok(v.clone());
        }
        let v: Vec<String> = conn
            .query(format!(
                "SELECT version FROM {} WHERE state = 'ready' ORDER BY loaded_at DESC, version DESC",
                table::qualified(table::PACKS)
            ))
            .await
            .map_err(|e| {
                SprigError::new(SprigCode::BackendUnavailable, "docs: reading packs").with_source(e)
            })?;
        self.ready
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(cluster.clone(), (Instant::now(), v.clone()));
        Ok(v)
    }

    /// The clusters to try for `tenant`: its catalog cluster, then the
    /// DC's other clusters.
    async fn candidates(&self, tenant: Option<TenantId>) -> Vec<ClusterId> {
        let mut out = Vec::new();
        if let Some(t) = tenant {
            let home = match self.tenants.locator.cached_cluster(t) {
                Some(c) => Some(c),
                None => self.tenants.locator.cluster(t).await.ok(),
            };
            out.extend(home);
        }
        for c in self.clusters().await {
            if !out.contains(&c) {
                out.push(c);
            }
        }
        out
    }

    /// The DC's clusters that may hold data, by number (listed again at
    /// most every [`READY_TTL`]).
    async fn clusters(&self) -> Vec<ClusterId> {
        if let Some((at, v)) = &*self.clusters.lock().unwrap_or_else(PoisonError::into_inner)
            && at.elapsed() < READY_TTL
        {
            return v.clone();
        }
        let Ok(list) = self.registry.list().await else {
            return Vec::new();
        };
        let mut all: Vec<(u32, ClusterId)> = list
            .into_iter()
            .filter(|c| {
                !matches!(
                    c.body().admin_state,
                    AdminState::Rejected | AdminState::Retired
                )
            })
            .map(|c| (c.body().cluster_number, c.body().cluster_id.clone()))
            .collect();
        all.sort();
        let v: Vec<ClusterId> = all.into_iter().map(|(_, c)| c).collect();
        *self.clusters.lock().unwrap_or_else(PoisonError::into_inner) =
            Some((Instant::now(), v.clone()));
        v
    }

    /// A reader connection and the version to read for `tenant`: the
    /// first candidate cluster with the version loaded.
    ///
    /// # Errors
    /// 39989 when docs are off or no cluster has the version.
    pub async fn target(&self, tenant: Option<TenantId>, version: Option<&str>) -> Result<Target> {
        if !self.enabled() {
            return Err(unavailable("it is turned off (docs.enabled)"));
        }
        let mut last: Option<SprigError> = None;
        for cluster in self.candidates(tenant).await {
            let r = async {
                let pool = self.pool(&cluster).await?;
                let mut conn = tokio::time::timeout(CONNECT_TIMEOUT, pool.get_conn())
                    .await
                    .map_err(|_| {
                        SprigError::new(
                            SprigCode::BackendUnavailable,
                            format!("docs: cluster {cluster} didn't answer"),
                        )
                    })?
                    .map_err(|e| {
                        SprigError::new(
                            SprigCode::BackendUnavailable,
                            format!("docs: cluster {cluster}"),
                        )
                        .with_source(e)
                    })?;
                let ready = self.ready_versions(&cluster, &mut conn).await?;
                Ok::<_, SprigError>((conn, ready))
            }
            .await;
            let (conn, ready) = match r {
                Ok(x) => x,
                Err(e) => {
                    tracing::debug!(%cluster, error = %e, "docs: cluster skipped");
                    last = Some(e);
                    continue;
                }
            };
            let pick = match version {
                Some(v) => ready.iter().find(|r| r.as_str() == v).cloned(),
                None => ready
                    .iter()
                    .find(|r| r.as_str() == env!("CARGO_PKG_VERSION"))
                    .or_else(|| ready.first())
                    .cloned(),
            };
            if let Some(version) = pick {
                return Ok(Target {
                    cluster,
                    version,
                    conn,
                });
            }
        }
        Err(match (version, last) {
            (Some(v), _) => unavailable(format!("release {v} isn't loaded on any cluster")),
            (None, Some(e)) => unavailable(format!("no cluster has them loaded (last error: {e})")),
            (None, None) => unavailable("no cluster has them loaded yet"),
        })
    }
}
