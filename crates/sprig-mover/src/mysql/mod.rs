//! The production source, stream and target on MariaDB (chapter 10), and
//! the factory a worker builds them with from cluster records.
//!
//! Every connection is `sprig_ctl` on the cluster's writer, except the
//! binlog stream, which is `sprig_repl` on the same node: R0 reads chunks
//! and the binlog from the writer (on Galera, the first synced node), so
//! the snapshot a chunk reads is never older than the low watermark the
//! stream has applied.

pub mod source;
pub mod sql;
pub mod stream;
pub mod target;

use std::sync::Arc;
use std::time::Duration;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Pool, PoolConstraints, PoolOpts};
use sprig_binlog::{BinlogSource, GtidPos};
use sprig_cluster::driver::NodeConnector as _;
use sprig_cluster::registry::ClusterRegistry;
use sprig_cluster::{ClusterRecord, NodeEndpoint, TopologyKind};
use sprig_common::ids::{ClusterId, JobId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_provision::credentials::CredentialConnector;
use sprig_shard::TenantMaps;
use twox_hash::XxHash3_64;

use crate::api::JobKind;
use crate::io::JobEnv;
use crate::maps::TenantMapIo;
use crate::record::JobRecord;
use crate::settings::MoverSettings;
use crate::worker::EnvFactory;

pub use source::MysqlSource;
pub use stream::MysqlStream;
pub use target::MysqlTarget;

/// How long one connection attempt may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// A MariaDB error as a retryable backend error.
pub(crate) fn db_err(what: &str, e: &mysql_async::Error) -> SprigError {
    match e {
        mysql_async::Error::Server(s) => {
            let mut st = [b'H', b'Y', b'0', b'0', b'0'];
            if s.state.len() == 5 {
                st.copy_from_slice(s.state.as_bytes());
            }
            SprigError::backend(s.code, st, format!("{what}: {} ({})", s.message, s.code))
        }
        other => SprigError::new(SprigCode::BackendUnavailable, format!("{what}: {other}")),
    }
}

/// The server error number of `e`, if it is one.
pub(crate) fn errno(e: &mysql_async::Error) -> Option<u16> {
    match e {
        mysql_async::Error::Server(s) => Some(s.code),
        _ => None,
    }
}

/// One cluster as the mover reaches it.
#[derive(Clone)]
pub struct ClusterAccess {
    /// The cluster.
    pub cluster: ClusterId,
    /// Its topology.
    pub topology: TopologyKind,
    /// `sprig_ctl` on the writer.
    pub writer: NodeEndpoint,
    /// `sprig_ctl` on every node (fences kill sessions everywhere).
    pub nodes: Vec<NodeEndpoint>,
    /// `sprig_repl` on the writer.
    pub repl: BinlogSource,
    /// Galera: the cluster's `wsrep_gtid_domain_id`; positions keep only it.
    pub domain: Option<u32>,
}

impl std::fmt::Debug for ClusterAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterAccess")
            .field("cluster", &self.cluster)
            .field("writer", &self.writer)
            .finish_non_exhaustive()
    }
}

async fn connect(ep: &NodeEndpoint) -> Result<Conn> {
    tokio::time::timeout(CONNECT_TIMEOUT, Conn::new(ep.opts.clone()))
        .await
        .map_err(|_| {
            SprigError::new(
                SprigCode::BackendUnavailable,
                format!("connect to {}: timed out", ep.addr()),
            )
        })?
        .map_err(|e| db_err(&format!("connect to {}", ep.addr()), &e))
}

impl ClusterAccess {
    /// Finds `rec`'s writer: Galera's first synced node, the node that
    /// isn't a replica, or the single node.
    ///
    /// # Errors
    /// 39100 without a reachable writer; 39998 for credentials that don't
    /// resolve.
    pub async fn discover(rec: &ClusterRecord) -> Result<Self> {
        let ctl = CredentialConnector::ctl(&rec.secrets);
        let repl = CredentialConnector::repl(&rec.secrets);
        let mut nodes = Vec::new();
        let mut writer = None;
        let mut domain = None;
        for spec in &rec.nodes {
            let ep = ctl.endpoint(spec)?;
            nodes.push(ep.clone());
            if writer.is_some() {
                continue;
            }
            let Ok(mut c) = connect(&ep).await else {
                continue;
            };
            let probe: Option<(String, i64, u32)> = c
                .query_first("SELECT @@wsrep_on, @@read_only, @@wsrep_gtid_domain_id")
                .await
                .ok()
                .flatten();
            let replica_rows: Vec<mysql_async::Row> =
                c.query("SHOW ALL SLAVES STATUS").await.unwrap_or_default();
            let galera_ready = match rec.topology {
                TopologyKind::Galera => {
                    let st: Option<(String, String)> = c
                        .query_first("SHOW STATUS LIKE 'wsrep_local_state'")
                        .await
                        .ok()
                        .flatten();
                    st.is_some_and(|(_, v)| v == "4")
                }
                _ => true,
            };
            let _ = c.disconnect().await;
            let Some((wsrep, read_only, wsrep_domain)) = probe else {
                continue;
            };
            let wsrep = wsrep == "1" || wsrep.eq_ignore_ascii_case("ON");
            let ok = if wsrep {
                galera_ready
            } else {
                replica_rows.is_empty() && read_only == 0
            };
            if ok {
                if wsrep {
                    domain = Some(wsrep_domain);
                }
                writer = Some((spec.clone(), ep));
            }
        }
        let Some((spec, writer)) = writer else {
            return Err(SprigError::new(
                SprigCode::ClusterUnavailable,
                format!("cluster {} has no reachable writer", rec.cluster_id),
            ));
        };
        let r = repl.endpoint(&spec)?;
        Ok(Self {
            cluster: rec.cluster_id.clone(),
            topology: rec.topology,
            // The credential's TLS too: managed clusters' sprig_repl is
            // REQUIRE SSL (r2opfix).
            repl: r.binlog_source().await,
            writer,
            nodes,
            domain,
        })
    }

    /// A small `sprig_ctl` pool to the writer whose connections run
    /// `setup` first.
    #[must_use]
    pub fn pool(&self, setup: &[&str], max: usize) -> Pool {
        let constraints = PoolConstraints::new(0, max.max(1)).unwrap_or_default();
        Pool::new(
            OptsBuilder::from_opts(self.writer.opts.clone())
                .setup(setup.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>())
                .client_found_rows(true)
                .pool_opts(
                    PoolOpts::new()
                        .with_constraints(constraints)
                        .with_inactive_connection_ttl(Duration::from_secs(60)),
                ),
        )
    }

    /// Keeps only the cluster's domain of a Galera position.
    #[must_use]
    pub fn trim(&self, mut p: GtidPos) -> GtidPos {
        if let Some(d) = self.domain {
            p.retain_domains(|x| x == d);
        }
        p
    }
}

/// The binlog client server id of a job: `mover.server_id_base` plus a
/// hash of the job id below 10^6. The same job keeps its id across owners,
/// so a new owner's dump replaces an old owner's on the source.
#[must_use]
pub fn server_id(base: u32, job: JobId) -> u32 {
    let h = XxHash3_64::oneshot(job.0.as_bytes()) % 1_000_000;
    base.saturating_add(u32::try_from(h).unwrap_or(0))
}

/// The environment of a hash-range job (split, merge, shard move): one
/// source over every source shard's schemas, a target routing rows into
/// the new shard schemas, and the group's map in `store`.
///
/// # Errors
/// `Internal` for a spec that isn't a hash-range job.
pub fn group_env(
    src: ClusterAccess,
    tgt: ClusterAccess,
    rec: &JobRecord,
    route: crate::route::ShardRoute,
    store: Arc<dyn sprig_meta::MetaStore>,
    server_id_base: u32,
) -> Result<JobEnv> {
    group_env_with(src, tgt, rec, route, store, server_id_base, None)
}

/// [`group_env`], with chapter 18's resolver finishing the tenant's
/// prepared branches at cutover.
///
/// # Errors
/// `Internal` for a spec that isn't a hash-range job.
pub fn group_env_with(
    src: ClusterAccess,
    tgt: ClusterAccess,
    rec: &JobRecord,
    route: crate::route::ShardRoute,
    store: Arc<dyn sprig_meta::MetaStore>,
    server_id_base: u32,
    drain: Option<Arc<dyn crate::io::PreparedDrain>>,
) -> Result<JobEnv> {
    let schemas: Vec<String> = rec
        .spec
        .sources
        .iter()
        .flat_map(|s| s.schemas.iter().cloned())
        .collect();
    let source = Arc::new(
        MysqlSource::new(
            src,
            rec.job_id,
            rec.tenant_id,
            schemas,
            None,
            server_id(server_id_base, rec.job_id),
            false,
        )
        .with_drain(drain)
        .with_rekey(route.rekey.is_some()),
    );
    let target = Arc::new(
        MysqlTarget::new(tgt, rec.job_id, None, source.clone()).with_route(Arc::new(route)),
    );
    let maps = Arc::new(crate::groupmaps::GroupMapIo::new(store, &rec.spec)?);
    Ok(JobEnv {
        source,
        target,
        maps,
    })
}

/// Builds jobs' environments from the DC's cluster records.
pub struct MysqlEnvFactory {
    registry: ClusterRegistry,
    maps: TenantMaps,
    settings: MoverSettings,
    /// Node addresses replaced before discovery (`host:port` → `host:port`):
    /// fault tests route a cluster's writer through a proxy.
    addr_overrides: std::collections::HashMap<String, String>,
    /// The one etcd, for moves between DCs (chapter 24): set by
    /// [`Self::with_home_changes`].
    home_store: Option<Arc<dyn sprig_meta::MetaStore>>,
    /// The DC tier holding group shard maps, for hash-range jobs (chapter
    /// 20): set by [`Self::with_group_maps`].
    group_store: Option<Arc<dyn sprig_meta::MetaStore>>,
    /// Chapter 18's resolver, for cutovers: set by [`Self::with_drain`].
    drain: Option<Arc<dyn crate::io::PreparedDrain>>,
    /// The worker's live settings and DC: set by
    /// [`Self::with_live_settings`]; without them every job gets
    /// `settings`.
    live: Option<(sprig_settings::LiveSettings, sprig_common::ids::DcId)>,
}

impl std::fmt::Debug for MysqlEnvFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MysqlEnvFactory").finish_non_exhaustive()
    }
}

impl MysqlEnvFactory {
    /// Clusters from `registry`, maps in `maps` (the DC tier), and
    /// `settings` for every job.
    #[must_use]
    pub fn new(registry: ClusterRegistry, maps: TenantMaps, settings: MoverSettings) -> Self {
        Self {
            registry,
            maps,
            settings,
            addr_overrides: std::collections::HashMap::new(),
            home_store: None,
            group_store: None,
            drain: None,
            live: None,
        }
    }

    /// Resolves each job's `mover.*` settings from `live` (the worker's
    /// mirrors of the global, dc and cluster scopes) in `dc`, for the job's
    /// tenant and source cluster, every time they are asked for: at the
    /// claim, then every second while the job runs. The settings given to
    /// [`Self::new`] are then unused.
    #[must_use]
    pub fn with_live_settings(
        mut self,
        live: sprig_settings::LiveSettings,
        dc: sprig_common::ids::DcId,
    ) -> Self {
        self.live = Some((live, dc));
        self
    }

    /// Cutovers finish the tenant's prepared distributed-transaction
    /// branches on the source through `drain` (chapter 18's resolver)
    /// before and after the fence.
    #[must_use]
    pub fn with_drain(mut self, drain: Arc<dyn crate::io::PreparedDrain>) -> Self {
        self.drain = Some(drain);
        self
    }

    /// Runs hash-range jobs (split, merge, shard move, chapter 20) too:
    /// they cut over on group shard maps in `store`, the DC tier.
    #[must_use]
    pub fn with_group_maps(mut self, store: Arc<dyn sprig_meta::MetaStore>) -> Self {
        self.group_store = Some(store);
        self
    }

    /// Runs moves between DCs too (chapter 24): a job whose source and
    /// target clusters are in different DCs commits with a home change
    /// ([`crate::home::HomeMapIo`]) on `store`, any tier of the one etcd
    /// (single-etcd mode).
    #[must_use]
    pub fn with_home_changes(mut self, store: Arc<dyn sprig_meta::MetaStore>) -> Self {
        self.home_store = Some(store);
        self
    }

    /// Reaches the node at `from` (`host:port`, as registered) through `to`
    /// instead: fault tests put a proxy in between.
    #[must_use]
    pub fn with_addr_override(mut self, from: &str, to: &str) -> Self {
        self.addr_overrides.insert(from.to_owned(), to.to_owned());
        self
    }

    async fn access(&self, id: &ClusterId) -> Result<ClusterAccess> {
        let stored = self.registry.get(id).await?.ok_or_else(|| {
            SprigError::new(SprigCode::ClusterNotFound, format!("no cluster {id}"))
        })?;
        let mut rec = stored.body().clone();
        for n in &mut rec.nodes {
            if let Some(to) = self.addr_overrides.get(&n.addr) {
                n.addr.clone_from(to);
            }
        }
        ClusterAccess::discover(&rec).await
    }
}

#[async_trait::async_trait]
impl EnvFactory for MysqlEnvFactory {
    async fn env(&self, rec: &JobRecord) -> Result<JobEnv> {
        let s = &rec.spec.sources[0];
        let t = &rec.spec.targets[0];
        let src = self.access(&s.cluster).await?;
        let tgt = if t.cluster == s.cluster {
            src.clone()
        } else {
            self.access(&t.cluster).await?
        };
        let swap = (rec.kind == JobKind::CopySwapDdl)
            .then(|| t.ddl.clone())
            .flatten();
        if let Some(route) = crate::route::ShardRoute::from_spec(&rec.spec)? {
            let store = self.group_store.clone().ok_or_else(|| {
                SprigError::internal("this worker has no shard-map store for hash-range jobs")
            })?;
            return group_env_with(
                src,
                tgt,
                rec,
                route,
                store,
                self.settings(rec).server_id_base,
                self.drain.clone(),
            );
        }
        let source = Arc::new(
            MysqlSource::new(
                src,
                rec.job_id,
                rec.tenant_id,
                s.schemas.clone(),
                s.tables.clone(),
                server_id(self.settings(rec).server_id_base, rec.job_id),
                swap.is_some(),
            )
            .with_drain(self.drain.clone()),
        );
        let target = Arc::new(MysqlTarget::new(tgt, rec.job_id, swap, source.clone()));
        let from = sprig_cluster::registry::dc_of(&s.cluster);
        let to = sprig_cluster::registry::dc_of(&t.cluster);
        let maps: Arc<dyn crate::io::MapIo> = match (&self.home_store, from, to) {
            (Some(store), Some(from), Some(to)) if from != to => Arc::new(
                crate::home::HomeMapIo::new(store.clone(), rec.tenant_id, from, to),
            ),
            _ => Arc::new(TenantMapIo::new(self.maps.clone(), rec.tenant_id)),
        };
        Ok(JobEnv {
            source,
            target,
            maps,
        })
    }

    fn settings(&self, rec: &JobRecord) -> MoverSettings {
        let mut s = match &self.live {
            Some((live, dc)) => {
                let snap = live.snapshot();
                let ctx = sprig_settings::SettingCtx::process(dc).for_tenant(rec.tenant_id, &[]);
                let ctx = match rec.spec.source_cluster() {
                    Some(c) => ctx.with_cluster(c),
                    None => ctx,
                };
                MoverSettings::resolve(&snap, &ctx)
            }
            None => self.settings.clone(),
        };
        // A move between DCs brings its own budget (chapter 24).
        if let Some(ms) = rec.spec.max_hold_ms {
            s.cutover_max_hold = std::time::Duration::from_millis(ms);
        }
        if let Some(ms) = rec.spec.retain_ms {
            s.source_retain = std::time::Duration::from_millis(ms);
        }
        s
    }
}
