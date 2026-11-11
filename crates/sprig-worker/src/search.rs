//! The search role (chapter 39, "Placement of copies", "Snapshots, seeding
//! and rebuilds"; build steps 5, 6 and 12; R5 round 6): copies of search
//! indexes on this worker, the `SearchPartition` service gateways query,
//! and the `search_copy` job that keeps one copy.
//!
//! A worker has the role when `SPRIG_SEARCH_LISTEN` names its address
//! (`host:port`; 7401 in the design); it then offers slots of class
//! `search`, and only such workers get copies. Files live under
//! `SPRIG_SEARCH_DIR` (default `<temp>/sprig-search/<worker id>`),
//! snapshots under `SPRIG_SEARCH_SNAPSHOTS` when set.
//!
//! ```text
//! search_copy(tenant, index, n):        until cancelled, every second:
//!   the index's record gone → stop, delete the files, end (dropped)
//!   no copy of the record's generation running here:
//!     files on disk                         → resume from their position
//!     else a ready peer copy (CopySegments)  → its files, then resume
//!     else the newest snapshot              → its files, then resume
//!     else empty                            → built from MariaDB through a
//!                                             gateway (SearchSeed, as the definer)
//!   write search/copies/<tenant>/<index>/<n>: worker, address, state, applied
//!   the lowest ready copy, every search.snapshot_interval_minutes: a snapshot
//!   the copy stopped because its object was dropped → remove the records, end
//! cancelled (lost, moved, fewer copies) → stop; the record goes
//!
//! Query (gateway → here): the copy of that generation; wait ready, or until its
//!   applied positions reach the targets; run the ops; reply with the positions
//! CopySegments (peer → here): hard links of a ready copy's committed files,
//!   streamed in 1 MiB chunks
//! ```

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use mysql_async::Pool;
use serde_json::{Value, json};
use sprig_api::proto::internal::search_partition_client::SearchPartitionClient;
use sprig_api::proto::internal::search_partition_server::{
    SearchPartition, SearchPartitionServer,
};
use sprig_api::proto::internal::{
    CopySegmentsRequest, SearchPartitionQueryReply, SearchPartitionQueryRequest, SegmentChunk,
};
use sprig_catalog::CatalogLocator as _;
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::ids::{ClusterId, DcId, TenantId};
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use sprig_controller::catalogs::ShardMapLocator;
use sprig_controller::dispatch::JobClass;
use sprig_feed::names::{CatalogPools, CatalogResolver};
use sprig_feed::{Clusters, MetaPlacement};
use sprig_meta::{ListOpts, MetaStore, Record, Versioned};
use sprig_search::copy::{
    Copy, CopyHost, CopyKnobs, FeedParts, Phase, SeedReply, SeedRequest, Seeder, list_tree,
    verify_tree,
};
use sprig_search::def::IndexDef;
use sprig_search::placement::{self, CopyRecord, IndexRecord};
use sprig_search::rpc::{QueryReply, QueryRequest, WireError};
use sprig_search::schema::{Key, Record as SearchRecord};
use sprig_settings::LiveSettings;
use tokio::io::AsyncWriteExt as _;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::{Request, Response, Status};

use crate::msg_triggers::{Gateways, PeerPems};
use crate::runtime::{JobCtx, JobHandler};

/// Bytes per `CopySegments` chunk.
const CHUNK: usize = 1 << 20;

fn unavailable(msg: impl Into<String>) -> SprigError {
    let mut e = SprigError::new(SprigCode::SearchUnavailable, msg);
    e.retryable = true;
    e
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A worker's registration, as far as search reads it.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct WorkerReg {
    #[serde(default)]
    slots: BTreeMap<String, u32>,
}

impl Versioned for WorkerReg {
    const V: u32 = 1;
}

/// Catalog pools through the shard-map locator.
struct Catalogs(Arc<ShardMapLocator>);

#[async_trait]
impl CatalogPools for Catalogs {
    async fn pool(&self, tenant: TenantId) -> Result<Pool> {
        Ok(self.0.writer(tenant).await?.pool)
    }
}

/// What the role needs from the worker.
pub struct SearchDeps {
    /// This worker's registration id.
    pub worker_id: String,
    /// This DC.
    pub dc_id: DcId,
    /// The DC tier.
    pub dc: Arc<dyn MetaStore>,
    /// Clusters (cluster list).
    pub registry: ClusterRegistry,
    /// Clusters (binlog readers, positions).
    pub clusters: Arc<dyn Clusters>,
    /// Catalogs, for the feed's names.
    pub locator: Arc<ShardMapLocator>,
    /// Gateways (seeds).
    pub gateways: Arc<Gateways>,
    /// Mutual TLS (`[tls]`).
    pub tls: Option<PeerPems>,
    /// Live settings.
    pub live: Option<LiveSettings>,
}

/// The search role of this worker.
pub struct SearchRole {
    deps: SearchDeps,
    feed: FeedParts,
    listen: std::net::SocketAddr,
    addr: String,
    dir: PathBuf,
    snapshots: Option<PathBuf>,
    copies: Mutex<HashMap<(u64, u64, u32), Arc<Copy>>>,
    channels: Mutex<HashMap<String, Channel>>,
    stop: CancellationToken,
}

impl std::fmt::Debug for SearchRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SearchRole")
            .field("addr", &self.addr)
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

impl SearchRole {
    /// The role when `SPRIG_SEARCH_LISTEN` is set (`None` otherwise).
    ///
    /// # Errors
    /// 39998 for an address that doesn't parse.
    pub fn from_env(deps: SearchDeps, stop: CancellationToken) -> Result<Option<Arc<Self>>> {
        let Some(listen) = std::env::var("SPRIG_SEARCH_LISTEN")
            .ok()
            .filter(|s| !s.trim().is_empty())
        else {
            return Ok(None);
        };
        let listen: std::net::SocketAddr = listen.trim().parse().map_err(|e| {
            SprigError::new(
                SprigCode::ConfigInvalid,
                format!("SPRIG_SEARCH_LISTEN {listen}: {e}"),
            )
        })?;
        let addr = std::env::var("SPRIG_SEARCH_ADVERTISE")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| listen.to_string());
        let dir = std::env::var_os("SPRIG_SEARCH_DIR").map_or_else(
            || {
                std::env::temp_dir()
                    .join("sprig-search")
                    .join(deps.worker_id.replace(['/', ':'], "_"))
            },
            PathBuf::from,
        );
        let snapshots = std::env::var_os("SPRIG_SEARCH_SNAPSHOTS").map(PathBuf::from);
        let feed = FeedParts {
            clusters: Arc::clone(&deps.clusters),
            placement: Arc::new(MetaPlacement::new(Arc::clone(&deps.dc))),
            names: Arc::new(CatalogResolver::new(Arc::new(Catalogs(Arc::clone(
                &deps.locator,
            ))))),
        };
        Ok(Some(Arc::new(Self {
            deps,
            feed,
            listen,
            addr,
            dir,
            snapshots,
            copies: Mutex::new(HashMap::new()),
            channels: Mutex::new(HashMap::new()),
            stop,
        })))
    }

    /// Slots of class `search` this worker offers.
    #[must_use]
    pub fn slots() -> (JobClass, u32) {
        (JobClass::Search, sprig_controller::dispatch::SEARCH_SLOTS)
    }

    /// Serves `SearchPartition` until the role stops.
    ///
    /// # Errors
    /// 39998 for TLS files that don't load or an address that can't be
    /// bound.
    pub async fn serve(self: &Arc<Self>) -> Result<()> {
        let mut b = tonic::transport::Server::builder();
        if let Some(t) = &self.deps.tls {
            b = b
                .tls_config(
                    tonic::transport::ServerTlsConfig::new()
                        .identity(Identity::from_pem(&t.cert, &t.key))
                        .client_ca_root(Certificate::from_pem(&t.ca)),
                )
                .map_err(|e| {
                    SprigError::new(SprigCode::ConfigInvalid, format!("search TLS: {e}"))
                })?;
        }
        let svc = SearchPartitionServer::new(PartitionService(Arc::clone(self)))
            .max_decoding_message_size(64 << 20)
            .max_encoding_message_size(256 << 20);
        let listener = tokio::net::TcpListener::bind(self.listen).await.map_err(|e| {
            SprigError::new(
                SprigCode::ConfigInvalid,
                format!("can't bind the search service on {}: {e}", self.listen),
            )
        })?;
        tracing::info!(listen = %self.listen, addr = %self.addr, mtls = self.deps.tls.is_some(), dir = %self.dir.display(), "search role: SearchPartition listening");
        let stop = self.stop.clone();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        tokio::spawn(async move {
            if let Err(e) = b
                .add_service(svc)
                .serve_with_incoming_shutdown(incoming, stop.cancelled_owned())
                .await
            {
                tracing::warn!(error = %e, "search service stopped");
            }
        });
        Ok(())
    }

    fn knobs(&self) -> sprig_gateway_free::Knobs {
        sprig_gateway_free::Knobs::read(self.deps.live.as_ref(), &self.deps.dc_id)
    }

    fn copy_dir(&self, tenant: u64, index: u64, generation: u32, n: u32) -> PathBuf {
        self.dir
            .join(format!("t{tenant}"))
            .join(format!("i{index}"))
            .join(format!("g{generation}"))
            .join(format!("c{n}"))
    }

    fn client(&self, addr: &str) -> Result<SearchPartitionClient<Channel>> {
        let scheme = if self.deps.tls.is_some() {
            "https"
        } else {
            "http"
        };
        let uri = format!("{scheme}://{addr}");
        let mut channels = lock(&self.channels);
        if let Some(c) = channels.get(&uri) {
            return Ok(SearchPartitionClient::new(c.clone()));
        }
        let mut ep = Endpoint::from_shared(uri.clone())
            .map_err(|e| unavailable(format!("search peer {uri}: {e}")))?
            .connect_timeout(Duration::from_secs(2));
        if let Some(t) = &self.deps.tls {
            let host = addr.rsplit_once(':').map_or(addr, |(h, _)| h).to_owned();
            ep = ep
                .tls_config(
                    ClientTlsConfig::new()
                        .ca_certificate(Certificate::from_pem(&t.ca))
                        .identity(Identity::from_pem(&t.cert, &t.key))
                        .domain_name(host),
                )
                .map_err(|e| unavailable(format!("search peer TLS: {e}")))?;
        }
        let ch = ep.connect_lazy();
        channels.insert(uri, ch.clone());
        Ok(SearchPartitionClient::new(ch))
    }

    /// Search workers registered now.
    async fn live_workers(&self) -> Result<std::collections::HashSet<String>> {
        let prefix = self.deps.dc.tier().key("workers/")?;
        let page = self
            .deps
            .dc
            .list(
                &prefix,
                ListOpts {
                    limit: 10_000,
                    ..ListOpts::default()
                },
            )
            .await?;
        Ok(page
            .kvs
            .iter()
            .filter(|kv| {
                Record::<WorkerReg>::decode(&kv.value)
                    .is_ok_and(|r| r.body.slots.get("search").copied().unwrap_or(0) > 0)
            })
            .filter_map(|kv| kv.key.as_str().rsplit('/').next().map(str::to_owned))
            .collect())
    }

    /// Fills `dest` from a ready peer copy of the generation; whether it
    /// did.
    async fn from_peer(&self, rec: &IndexRecord, me: u32, dest: &Path) -> Result<bool> {
        let live = self.live_workers().await?;
        let copies = placement::list_copies(&self.deps.dc, rec.tenant, rec.index).await?;
        let (ready, _) = placement::query_order(&copies, rec.def.generation, &|w| live.contains(w));
        for peer in ready.iter().filter(|c| c.copy != me) {
            let incoming = dest.with_extension("incoming");
            let _ = tokio::fs::remove_dir_all(&incoming).await;
            match self.fetch(peer, rec, &incoming).await {
                Ok(()) if verify_tree(&incoming, rec.def.partitions)? => {
                    let _ = tokio::fs::remove_dir_all(dest).await;
                    tokio::fs::rename(&incoming, dest)
                        .await
                        .map_err(|e| SprigError::internal(format!("search peer files: {e}")))?;
                    tracing::info!(tenant = rec.tenant, index = rec.index, peer = peer.copy, worker = %peer.worker_id, "search copy seeded from a peer");
                    return Ok(true);
                }
                Ok(()) => {
                    tracing::warn!(peer = peer.copy, "a peer's search files didn't verify");
                }
                Err(e) => {
                    tracing::warn!(peer = peer.copy, error = %e, "search peer seeding failed");
                }
            }
            let _ = tokio::fs::remove_dir_all(&incoming).await;
        }
        Ok(false)
    }

    async fn fetch(&self, peer: &CopyRecord, rec: &IndexRecord, into: &Path) -> Result<()> {
        let mut c = self.client(&peer.addr)?;
        let mut stream = c
            .copy_segments(CopySegmentsRequest {
                tenant_id: rec.tenant,
                index_id: rec.index,
                generation: rec.def.generation,
            })
            .await
            .map_err(|s| unavailable(format!("CopySegments: {}", s.message())))?
            .into_inner();
        let io = |e: std::io::Error| SprigError::internal(format!("search peer files: {e}"));
        let mut file: Option<(String, tokio::fs::File)> = None;
        while let Some(chunk) = stream
            .message()
            .await
            .map_err(|s| unavailable(format!("CopySegments: {}", s.message())))?
        {
            let rel = Path::new(&chunk.path);
            if rel.is_absolute()
                || rel
                    .components()
                    .any(|c| matches!(c, std::path::Component::ParentDir))
            {
                return Err(SprigError::internal("a peer sent a path outside the copy"));
            }
            if file.as_ref().is_none_or(|(p, _)| *p != chunk.path) {
                let p = into.join(rel);
                if let Some(d) = p.parent() {
                    tokio::fs::create_dir_all(d).await.map_err(io)?;
                }
                file = Some((chunk.path.clone(), tokio::fs::File::create(&p).await.map_err(io)?));
            }
            if let Some((_, f)) = file.as_mut() {
                f.write_all(&chunk.data).await.map_err(io)?;
                if chunk.last {
                    f.flush().await.map_err(io)?;
                }
            }
        }
        metrics::counter!("sprig_search_seeds_total", "source" => "peer").increment(1);
        Ok(())
    }

    /// The snapshot directory of an index's generation.
    fn snap_root(&self, rec: &IndexRecord) -> Option<PathBuf> {
        self.snapshots.as_ref().map(|s| {
            s.join(format!("t{}", rec.tenant))
                .join(format!("i{}", rec.index))
                .join(format!("g{}", rec.def.generation))
        })
    }

    /// Fills `dest` from the newest snapshot; whether it did.
    fn from_snapshot(&self, rec: &IndexRecord, dest: &Path) -> Result<bool> {
        let Some(root) = self.snap_root(rec) else {
            return Ok(false);
        };
        let mut names: Vec<u64> = std::fs::read_dir(&root)
            .map(|rd| {
                rd.filter_map(|e| e.ok()?.file_name().to_str()?.parse().ok())
                    .collect()
            })
            .unwrap_or_default();
        names.sort_unstable();
        let io = |e: std::io::Error| SprigError::internal(format!("search snapshot: {e}"));
        for n in names.into_iter().rev() {
            let src = root.join(n.to_string());
            if !src.join("manifest.json").exists() {
                continue;
            }
            let incoming = dest.with_extension("incoming");
            let _ = std::fs::remove_dir_all(&incoming);
            for f in list_tree(&src.join("files")).map_err(io)? {
                let to = incoming.join(&f);
                if let Some(d) = to.parent() {
                    std::fs::create_dir_all(d).map_err(io)?;
                }
                std::fs::copy(src.join("files").join(&f), &to).map_err(io)?;
            }
            if verify_tree(&incoming, rec.def.partitions)? {
                let _ = std::fs::remove_dir_all(dest);
                std::fs::rename(&incoming, dest).map_err(io)?;
                metrics::counter!("sprig_search_seeds_total", "source" => "snapshot").increment(1);
                tracing::info!(tenant = rec.tenant, index = rec.index, snapshot = n, "search copy restored from a snapshot");
                return Ok(true);
            }
            let _ = std::fs::remove_dir_all(&incoming);
        }
        Ok(false)
    }

    /// Writes a snapshot of `copy` and keeps the newest `keep`.
    fn snapshot(&self, rec: &IndexRecord, copy: &Copy, keep: usize) -> Result<Option<u64>> {
        let Some(root) = self.snap_root(rec) else {
            return Ok(None);
        };
        let io = |e: std::io::Error| SprigError::internal(format!("search snapshot: {e}"));
        let at = now_ms();
        let tmp = self.dir.join(".snap").join(format!("{}-{}-{at}", rec.tenant, rec.index));
        copy.snapshot_into(&tmp)?;
        let part = root.join(format!("{at}.part"));
        let _ = std::fs::remove_dir_all(&part);
        let files = list_tree(&tmp).map_err(io)?;
        let mut bytes = 0u64;
        for f in &files {
            let to = part.join("files").join(f);
            if let Some(d) = to.parent() {
                std::fs::create_dir_all(d).map_err(io)?;
            }
            bytes += std::fs::copy(tmp.join(f), &to).map_err(io)?;
        }
        let _ = std::fs::remove_dir_all(&tmp);
        let manifest = json!({
            "tenant": rec.tenant, "index": rec.index, "generation": rec.def.generation,
            "at_ms": at, "bytes": bytes,
            "files": files.iter().map(|p| p.to_string_lossy().into_owned()).collect::<Vec<_>>(),
            "applied": copy.applied().text(),
        });
        std::fs::write(part.join("manifest.json"), manifest.to_string()).map_err(io)?;
        std::fs::rename(&part, root.join(at.to_string())).map_err(io)?;
        metrics::counter!("sprig_search_snapshot_bytes_total").increment(bytes);
        let mut names: Vec<u64> = std::fs::read_dir(&root)
            .map(|rd| {
                rd.filter_map(|e| e.ok()?.file_name().to_str()?.parse().ok())
                    .collect()
            })
            .unwrap_or_default();
        names.sort_unstable();
        while names.len() > keep.max(1) {
            let old = names.remove(0);
            let _ = std::fs::remove_dir_all(root.join(old.to_string()));
        }
        Ok(Some(at))
    }

    fn find(&self, tenant: u64, index: u64, generation: u32) -> Option<Arc<Copy>> {
        let m = lock(&self.copies);
        let mut v: Vec<&Arc<Copy>> = m
            .iter()
            .filter(|((t, i, _), c)| *t == tenant && *i == index && c.def.generation == generation)
            .map(|(_, c)| c)
            .collect();
        // A ready copy first.
        v.sort_by_key(|c| u8::from(c.phase() != Phase::Ready));
        v.first().map(|c| Arc::clone(c))
    }
}

/// `search.*` values the role reads (process scope, decision S8).
mod sprig_gateway_free {
    use std::time::Duration;

    use sprig_common::ids::DcId;
    use sprig_search::settings as s;
    use sprig_settings::{LiveSettings, SettingCtx};

    #[derive(Debug, Clone, Copy)]
    pub(super) struct Knobs {
        pub commit_interval: Duration,
        pub writer_bytes: usize,
        pub snapshot_every: Duration,
        pub snapshot_keep: usize,
    }

    impl Knobs {
        pub(super) fn read(live: Option<&LiveSettings>, dc: &DcId) -> Self {
            let cx = SettingCtx::process(dc);
            let get = |k: &sprig_settings::Setting<u64>| live.map_or(k.default, |l| l.get(k, &cx));
            Self {
                commit_interval: Duration::from_millis(get(&s::COMMIT_INTERVAL_MS)),
                writer_bytes: usize::try_from(get(&s::WRITER_MEMORY_MB))
                    .unwrap_or(32)
                    .saturating_mul(1 << 20),
                snapshot_every: Duration::from_secs(get(&s::SNAPSHOT_INTERVAL_MINUTES) * 60),
                snapshot_keep: usize::try_from(get(&s::SNAPSHOT_KEEP)).unwrap_or(3),
            }
        }
    }
}

/// The copy's host on this worker.
struct WorkerHost(Arc<SearchRole>);

struct GatewaySeeder {
    gateways: Arc<Gateways>,
    tenant: u64,
    index: u64,
    generation: u32,
}

async fn seed_call(
    gateways: &Gateways,
    req: &SeedRequest,
) -> Result<Vec<SearchRecord>> {
    let body = serde_json::to_string(req)
        .map_err(|e| SprigError::internal(format!("a seed request: {e}")))?;
    let reply = gateways.seed_page(req.tenant ^ req.index, body).await?;
    let r: SeedReply = serde_json::from_str(&reply)
        .map_err(|e| SprigError::internal(format!("a seed reply: {e}")))?;
    r.rows.map_err(|w| w.error())
}

#[async_trait]
impl Seeder for GatewaySeeder {
    async fn page(&mut self, after: Option<&Key>, limit: usize) -> Result<Vec<SearchRecord>> {
        seed_call(
            &self.gateways,
            &SeedRequest {
                tenant: self.tenant,
                index: self.index,
                generation: self.generation,
                after: after.cloned(),
                limit,
            },
        )
        .await
    }

    async fn close(self: Box<Self>) {}
}

#[async_trait]
impl CopyHost for WorkerHost {
    fn knobs(&self) -> CopyKnobs {
        let k = self.0.knobs();
        CopyKnobs {
            commit_interval: k.commit_interval,
            writer_bytes: k.writer_bytes,
        }
    }

    fn feed(&self) -> &FeedParts {
        &self.0.feed
    }

    async fn clusters(&self) -> Result<Vec<ClusterId>> {
        Ok(self
            .0
            .deps
            .registry
            .list()
            .await?
            .into_iter()
            .filter(|c| c.body().admin_state != sprig_cluster::record::AdminState::Retired)
            .map(|c| c.body().cluster_id.clone())
            .collect())
    }

    async fn seeder(&self, tenant: TenantId, def: &IndexDef) -> Result<Box<dyn Seeder>> {
        Ok(Box::new(GatewaySeeder {
            gateways: Arc::clone(&self.0.deps.gateways),
            tenant: tenant.0,
            index: def.index_id,
            generation: def.generation,
        }))
    }

    async fn object_gone(&self, tenant: TenantId, def: &IndexDef) -> bool {
        let r = seed_call(
            &self.0.deps.gateways,
            &SeedRequest {
                tenant: tenant.0,
                index: def.index_id,
                generation: def.generation,
                after: None,
                limit: 0,
            },
        )
        .await;
        matches!(r, Err(e) if e.code() == Some(SprigCode::SearchIndexNotFound))
    }
}

struct PartitionService(Arc<SearchRole>);

type ChunkStream =
    Pin<Box<dyn tokio_stream::Stream<Item = std::result::Result<SegmentChunk, Status>> + Send>>;

#[tonic::async_trait]
impl SearchPartition for PartitionService {
    async fn query(
        &self,
        request: Request<SearchPartitionQueryRequest>,
    ) -> std::result::Result<Response<SearchPartitionQueryReply>, Status> {
        let req: QueryRequest = serde_json::from_str(&request.get_ref().request_json)
            .map_err(|e| Status::invalid_argument(format!("a search query: {e}")))?;
        let started = Instant::now();
        let (answers, applied) = match self.0.find(req.tenant, req.index, req.generation) {
            None => (
                Err(WireError::of(&unavailable(format!(
                    "no copy of search index {} generation {} on worker {}",
                    req.index, req.generation, self.0.deps.worker_id
                )))),
                BTreeMap::new(),
            ),
            Some(copy) => {
                let timeout = Duration::from_millis(req.wait.timeout_ms);
                let targets: Vec<(ClusterId, sprig_binlog::GtidPos)> = req
                    .wait
                    .targets
                    .iter()
                    .filter_map(|(c, p)| Some((ClusterId::new(c).ok()?, p.parse().ok()?)))
                    .collect();
                let waited = if targets.is_empty() {
                    copy.wait_ready(timeout).await
                } else {
                    copy.wait_for(&targets, timeout).await
                };
                let applied = copy.applied().text();
                let r = match waited {
                    Err(e) => Err(e),
                    Ok(()) => {
                        let ix = Arc::clone(&copy.index);
                        let (q, ops, mx) = (req.query.clone(), req.ops.clone(), req.max_expansions);
                        tokio::task::spawn_blocking(move || {
                            sprig_search::rpc::execute(&ix, &q, &ops, mx)
                        })
                        .await
                        .map_err(|e| SprigError::internal(e.to_string()))
                        .and_then(|r| r)
                    }
                };
                (r.map_err(|e| WireError::of(&e)), applied)
            }
        };
        metrics::histogram!("sprig_search_query_duration_seconds", "phase" => "copy")
            .record(started.elapsed().as_secs_f64());
        let reply = serde_json::to_string(&QueryReply { answers, applied })
            .map_err(|e| Status::internal(format!("a search reply: {e}")))?;
        Ok(Response::new(SearchPartitionQueryReply { reply_json: reply }))
    }

    type CopySegmentsStream = ChunkStream;

    async fn copy_segments(
        &self,
        request: Request<CopySegmentsRequest>,
    ) -> std::result::Result<Response<Self::CopySegmentsStream>, Status> {
        let r = request.get_ref();
        let copy = self
            .0
            .find(r.tenant_id, r.index_id, r.generation)
            .filter(|c| c.phase() == Phase::Ready)
            .ok_or_else(|| Status::unavailable("no ready copy of that index here"))?;
        let tmp = self.0.dir.join(".snap").join(format!(
            "peer-{}-{}-{}",
            r.tenant_id,
            r.index_id,
            now_ms()
        ));
        let snap = {
            let (c, t) = (Arc::clone(&copy), tmp.clone());
            tokio::task::spawn_blocking(move || c.snapshot_into(&t))
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        };
        snap.map_err(|e| Status::unavailable(e.message))?;
        let files = list_tree(&tmp).map_err(|e| Status::internal(e.to_string()))?;
        let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<SegmentChunk, Status>>(4);
        tokio::spawn(async move {
            use tokio::io::AsyncReadExt as _;
            'files: for f in files {
                let path = f.to_string_lossy().into_owned();
                let Ok(mut file) = tokio::fs::File::open(tmp.join(&f)).await else {
                    let _ = tx.send(Err(Status::internal(format!("{path} vanished")))).await;
                    break;
                };
                let mut offset = 0u64;
                loop {
                    let mut buf = vec![0u8; CHUNK];
                    let n = match file.read(&mut buf).await {
                        Ok(n) => n,
                        Err(e) => {
                            let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                            break 'files;
                        }
                    };
                    buf.truncate(n);
                    let last = n < CHUNK;
                    let chunk = SegmentChunk {
                        path: path.clone(),
                        offset,
                        data: buf,
                        last,
                    };
                    offset += n as u64;
                    if tx.send(Ok(chunk)).await.is_err() {
                        break 'files;
                    }
                    if last {
                        break;
                    }
                }
            }
            let _ = tokio::fs::remove_dir_all(&tmp).await;
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }
}

/// `search_copy`: keeps one copy of an index on this worker.
#[derive(Debug, Clone)]
pub struct SearchCopyJob(pub Arc<SearchRole>);

struct Running {
    copy: Arc<Copy>,
    generation: u32,
    def: IndexDef,
    seeded_from: &'static str,
}

impl SearchCopyJob {
    async fn start(
        &self,
        rec: &IndexRecord,
        n: u32,
        cancel: &CancellationToken,
    ) -> Result<Running> {
        let role = &self.0;
        let dir = role.copy_dir(rec.tenant, rec.index, rec.def.generation, n);
        let has_files = dir.join("p0").join("meta.json").exists();
        let seeded_from = if has_files {
            "files"
        } else if role.from_peer(rec, n, &dir).await.unwrap_or_else(|e| {
            tracing::warn!(error = %e, "search peer seeding unavailable");
            false
        }) {
            "peer"
        } else if {
            let (r, rec2, d) = (Arc::clone(role), rec.clone(), dir.clone());
            tokio::task::spawn_blocking(move || r.from_snapshot(&rec2, &d))
                .await
                .map_err(|e| SprigError::internal(e.to_string()))?
                .unwrap_or_else(|e| {
                    tracing::warn!(error = %e, "search snapshot restore failed");
                    false
                })
        } {
            "snapshot"
        } else {
            "mariadb"
        };
        let host: Arc<dyn CopyHost> = Arc::new(WorkerHost(Arc::clone(role)));
        let copy = Copy::start(
            host,
            TenantId(rec.tenant),
            Arc::new(rec.def.clone()),
            &rec.analyzers,
            dir,
            cancel.child_token(),
        )?;
        lock(&role.copies).insert((rec.tenant, rec.index, n), Arc::clone(&copy));
        tracing::info!(tenant = rec.tenant, index = rec.index, copy = n, generation = rec.def.generation, from = seeded_from, "search copy started");
        Ok(Running {
            copy,
            generation: rec.def.generation,
            def: rec.def.clone(),
            seeded_from,
        })
    }

    fn remove_files(&self, tenant: u64, index: u64) {
        let d = self
            .0
            .dir
            .join(format!("t{tenant}"))
            .join(format!("i{index}"));
        if let Err(e) = std::fs::remove_dir_all(&d)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(dir = %d.display(), error = %e, "search copy files not removed");
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn serve(&self, cx: &JobCtx, tenant: u64, index: u64, n: u32) -> std::result::Result<Value, String> {
        let role = &self.0;
        let job_id = cx.job_id.to_string();
        let mut running: Option<Running> = None;
        let mut last_snapshot = Instant::now();
        let stop = |running: &mut Option<Running>| {
            if let Some(r) = running.take() {
                r.copy.stop();
                lock(&role.copies).remove(&(tenant, index, n));
            }
        };
        loop {
            let rec = match placement::get_index(&role.deps.dc, tenant, index).await {
                Ok(r) => r.map(placement::checked).transpose(),
                Err(e) => Err(e),
            };
            match rec {
                Ok(None) => {
                    // Dropped: its records went with it.
                    stop(&mut running);
                    self.remove_files(tenant, index);
                    return Ok(json!({"dropped": true}));
                }
                Ok(Some(rec)) => {
                    if running
                        .as_ref()
                        .is_some_and(|r| r.generation != rec.def.generation || r.def != rec.def)
                    {
                        stop(&mut running);
                    }
                    if running.is_none() {
                        match self.start(&rec, n, &cx.cancel).await {
                            Ok(r) => running = Some(r),
                            Err(e) => tracing::warn!(tenant, index, error = %e, "search copy didn't start"),
                        }
                    }
                    if let Some(r) = &running {
                        let phase = r.copy.phase();
                        if phase == Phase::Stopped && !cx.cancel.is_cancelled() {
                            // The object was dropped (the copy saw it).
                            stop(&mut running);
                            let _ = placement::drop_index(&role.deps.dc, tenant, index).await;
                            self.remove_files(tenant, index);
                            return Ok(json!({"dropped": true}));
                        }
                        let cr = CopyRecord {
                            tenant,
                            index,
                            copy: n,
                            generation: r.generation,
                            worker_id: role.deps.worker_id.clone(),
                            addr: role.addr.clone(),
                            state: phase.name().to_owned(),
                            applied: r.copy.applied().text(),
                            job_id: job_id.clone(),
                            updated_ms: now_ms(),
                            seeded_from: r.seeded_from.to_owned(),
                        };
                        if let Err(e) = placement::put_copy(&role.deps.dc, &cr).await {
                            tracing::warn!(error = %e, "search copy record not written");
                        }
                        // Snapshots: the lowest-numbered ready copy writes them.
                        let k = role.knobs();
                        if phase == Phase::Ready
                            && role.snapshots.is_some()
                            && last_snapshot.elapsed() >= k.snapshot_every
                        {
                            last_snapshot = Instant::now();
                            let lowest = placement::list_copies(&role.deps.dc, tenant, index)
                                .await
                                .unwrap_or_default()
                                .iter()
                                .filter(|c| c.state == "ready" && c.generation == r.generation)
                                .map(|c| c.copy)
                                .min();
                            if lowest == Some(n) {
                                let (ro, rc, c) = (Arc::clone(role), rec.clone(), Arc::clone(&r.copy));
                                let keep = k.snapshot_keep;
                                match tokio::task::spawn_blocking(move || ro.snapshot(&rc, &c, keep)).await {
                                    Ok(Ok(Some(at))) => tracing::info!(tenant, index, at, "search snapshot written"),
                                    Ok(Ok(None)) => {}
                                    Ok(Err(e)) => tracing::warn!(error = %e, "search snapshot failed"),
                                    Err(e) => tracing::warn!(error = %e, "search snapshot failed"),
                                }
                            }
                        }
                    }
                }
                Err(e) => tracing::warn!(error = %e, "search index record unreadable"),
            }
            tokio::select! {
                () = cx.cancel.cancelled() => {
                    stop(&mut running);
                    let _ = placement::drop_copy(&role.deps.dc, tenant, index, n, &job_id).await;
                    return Err("cancelled".into());
                }
                () = role.stop.cancelled() => {
                    stop(&mut running);
                    return Err("worker stopping".into());
                }
                () = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    }
}

#[async_trait]
impl JobHandler for SearchCopyJob {
    fn kind(&self) -> &'static str {
        placement::JOB_KIND
    }

    fn class(&self) -> JobClass {
        JobClass::Search
    }

    async fn run(&self, cx: &JobCtx) -> std::result::Result<Value, String> {
        let s = &cx.spec;
        let (Some(tenant), Some(index), Some(n)) = (
            s["tenant"].as_u64(),
            s["index"].as_u64(),
            s["copy"].as_u64().and_then(|n| u32::try_from(n).ok()),
        ) else {
            return Err(format!("bad search_copy spec: {s}"));
        };
        self.serve(cx, tenant, index, n).await
    }
}
