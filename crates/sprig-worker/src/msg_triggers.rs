//! Triggers on streams and queues (chapter 41, "Triggers on streams and
//! queues"; R9, r9triggers) on chapter 23's job runtime:
//!
//! - `logic_stream_trigger`: one per binding, long-running, in the
//!   `consumer` job class. It reads the stream's log and its consumer
//!   group's offsets (`__sprig_logic_<binding>`) as `sprig_ctl`, and runs
//!   [`sprig_logic::msgrun::run_stream`], each run in a gateway.
//! - `logic_queue_trigger`: one per binding; `logic.trigger_lanes` loops of
//!   [`sprig_logic::msgrun::run_queue`], each asking a gateway to run the
//!   queue's next message.
//!
//! Both report progress every second (their lease: a lost job stops),
//! pick up a new version within 5 s, and end, removing their index entry,
//! when the trigger's stream or queue (or the binding) is gone.
//!
//! **Gateways.** Runs go over the peer service (`sprig.internal.logic.v1`
//! on each gateway's `peer` port, mutual TLS with `[tls]`, plaintext on
//! loopback without it) to a ready gateway of this DC, the same one for a
//! binding while it answers (rendezvous on the binding), the next one when
//! it doesn't.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mysql_async::Pool;
use mysql_async::prelude::Queryable;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sprig_api::proto::logic_peer::RunTriggerRequest;
use sprig_api::proto::logic_peer::logic_peer_client::LogicPeerClient;
use sprig_catalog::CatalogLocator;
use sprig_common::ids::TenantId;
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_controller::catalogs::{ClusterPools, ShardMapLocator};
use sprig_controller::dispatch::JobClass;
use sprig_logic::LogicStore;
use sprig_logic::declare::SourceKind;
use sprig_logic::messages::{self, RecordIn, RunReply, RunRequest, SourceRow};
use sprig_logic::msgrun::{self, Gateway, MsgStats, StreamSide, Template};
use sprig_logic::triggers::RunOpts;
use sprig_meta::{ListOpts, MetaStore, Record, Versioned};
use sprig_streams::offsets;
use sprig_streams::read::{LogReader, ReadLimits};
use sprig_streams::{PartitionNo, StreamId, StreamState, StreamsStore};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::runtime::{JobCtx, JobHandler};

/// How long one run may take before the peer call gives up (the run's
/// own budget is `logic.trigger_budget_ms`; this is the transport's).
const CALL_TIMEOUT: Duration = Duration::from_secs(120);
/// How often a job looks for a new version or a dropped source.
const RELOAD_EVERY: Duration = Duration::from_secs(5);

/// The parts of a gateway's registration record a worker needs
/// (`sprig_gateway::registry::GatewayRecord`).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GwRecord {
    gateway_id: String,
    addr: String,
    #[serde(default)]
    listeners: BTreeMap<String, u16>,
    state: String,
}

impl Versioned for GwRecord {
    const V: u32 = 1;
}

/// The process identity for the peer service's mutual TLS.
#[derive(Clone)]
pub struct PeerPems {
    /// The internal CA.
    pub ca: Vec<u8>,
    /// This process's chain.
    pub cert: Vec<u8>,
    /// Its key.
    pub key: Vec<u8>,
}

impl PeerPems {
    /// Reads `[tls]`.
    ///
    /// # Errors
    /// 39998 for an unreadable file.
    pub fn from_config(cfg: &sprig_common::config::TlsConfig) -> Result<Self> {
        let read = |p: &std::path::Path| {
            std::fs::read(p).map_err(|e| {
                SprigError::new(
                    SprigCode::ConfigInvalid,
                    format!("[tls] {}: {e}", p.display()),
                )
            })
        };
        Ok(Self {
            ca: read(&cfg.ca)?,
            cert: read(&cfg.cert)?,
            key: read(&cfg.key)?,
        })
    }
}

/// Clients of this DC's gateways' `LogicPeer` service.
pub struct Gateways {
    dc: Arc<dyn MetaStore>,
    tls: Option<PeerPems>,
    channels: Mutex<HashMap<String, Channel>>,
    listed: Mutex<Option<(Instant, Vec<GwRecord>)>>,
}

impl std::fmt::Debug for Gateways {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gateways")
            .field("mtls", &self.tls.is_some())
            .finish_non_exhaustive()
    }
}

fn unreachable_gw(m: impl Into<String>) -> SprigError {
    let mut e = SprigError::new(SprigCode::ForwardFailed, m);
    e.retryable = true;
    e
}

impl Gateways {
    /// Gateways registered in `dc`, reached with `tls` (`None`: plaintext).
    #[must_use]
    pub fn new(dc: Arc<dyn MetaStore>, tls: Option<PeerPems>) -> Self {
        Self {
            dc,
            tls,
            channels: Mutex::new(HashMap::new()),
            listed: Mutex::new(None),
        }
    }

    /// Ready gateways with a peer port (the list is kept for 2 s).
    async fn ready(&self) -> Result<Vec<GwRecord>> {
        if let Some((at, v)) = self
            .listed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            && at.elapsed() < Duration::from_secs(2)
        {
            return Ok(v.clone());
        }
        let prefix = self.dc.tier().key("gateways/")?;
        let page = self
            .dc
            .list(
                &prefix,
                ListOpts {
                    limit: 10_000,
                    ..ListOpts::default()
                },
            )
            .await?;
        let v: Vec<GwRecord> = page
            .kvs
            .iter()
            .filter_map(|kv| Record::<GwRecord>::decode(&kv.value).ok())
            .map(|r| r.body)
            .filter(|g| g.state == "ready" && g.listeners.contains_key("peer"))
            .collect();
        *self.listed.lock().unwrap_or_else(PoisonError::into_inner) =
            Some((Instant::now(), v.clone()));
        Ok(v)
    }

    fn client(&self, g: &GwRecord) -> Result<LogicPeerClient<Channel>> {
        let port = g.listeners.get("peer").copied().unwrap_or(7403);
        let scheme = if self.tls.is_some() { "https" } else { "http" };
        let uri = format!("{scheme}://{}:{port}", g.addr);
        let mut channels = self.channels.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(c) = channels.get(&uri) {
            return Ok(LogicPeerClient::new(c.clone()));
        }
        let mut ep = Endpoint::from_shared(uri.clone())
            .map_err(|e| unreachable_gw(format!("peer address {uri}: {e}")))?
            .connect_timeout(Duration::from_secs(5))
            .timeout(CALL_TIMEOUT);
        if let Some(t) = &self.tls {
            let cfg = ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(&t.ca))
                .identity(Identity::from_pem(&t.cert, &t.key))
                .domain_name(g.addr.clone());
            ep = ep
                .tls_config(cfg)
                .map_err(|e| unreachable_gw(format!("peer TLS: {e}")))?;
        }
        let ch = ep.connect_lazy();
        channels.insert(uri, ch.clone());
        Ok(LogicPeerClient::new(ch))
    }

    /// One run on the first gateway, in rendezvous order for `seed`, that
    /// answers.
    ///
    /// # Errors
    /// 39788 (retryable) when none does.
    pub async fn run(&self, seed: u64, req: &RunRequest) -> Result<RunReply> {
        let mut gws = self.ready().await?;
        if gws.is_empty() {
            return Err(unreachable_gw(
                "no ready gateway with a peer port in this DC",
            ));
        }
        gws.sort_by_key(|g| std::cmp::Reverse(rendezvous(seed, &g.gateway_id)));
        let body = serde_json::to_string(req)
            .map_err(|e| SprigError::internal(format!("a trigger run: {e}")))?;
        let mut last = None;
        for g in &gws {
            let mut c = match self.client(g) {
                Ok(c) => c,
                Err(e) => {
                    last = Some(e);
                    continue;
                }
            };
            match c
                .run_trigger(RunTriggerRequest {
                    request_json: body.clone(),
                })
                .await
            {
                Ok(r) => {
                    return serde_json::from_str(&r.get_ref().reply_json)
                        .map_err(|e| SprigError::internal(format!("a trigger reply: {e}")));
                }
                Err(s) => {
                    last = Some(unreachable_gw(format!(
                        "gateway {}: {}",
                        g.gateway_id,
                        s.message()
                    )));
                    // The list may be stale.
                    *self.listed.lock().unwrap_or_else(PoisonError::into_inner) = None;
                }
            }
        }
        Err(last.unwrap_or_else(|| unreachable_gw("no gateway answered")))
    }
}

/// Highest-random-weight choice of a gateway for a binding.
fn rendezvous(seed: u64, id: &str) -> u64 {
    // FNV-1a over the seed and the id.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in seed.to_le_bytes().iter().chain(id.as_bytes()) {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

/// The runners' gateway: [`Gateways`] for one binding, and its run log.
struct GwClient {
    gws: Arc<Gateways>,
    seed: u64,
    log: RunLog,
}

/// Where a trigger's run log goes: `logic_log` in its tenant's catalog.
#[derive(Clone)]
struct RunLog {
    catalog: Pool,
    schema: String,
    module_id: u64,
    version: Arc<AtomicU64>,
}

impl RunLog {
    async fn write(&self, level: &str, message: &str) {
        let Ok(mut c) = self.catalog.get_conn().await else {
            return;
        };
        let _ = LogicStore::new(&self.schema)
            .log(
                &mut c,
                self.module_id,
                Some(self.version.load(Ordering::Relaxed)),
                level,
                "trigger",
                message,
            )
            .await;
    }
}

#[async_trait]
impl Gateway for GwClient {
    async fn run(&self, req: &RunRequest) -> Result<RunReply> {
        self.gws.run(self.seed, req).await
    }

    async fn note(&self, level: &str, message: &str) {
        self.log.write(level, message).await;
    }
}

/// What the message-trigger jobs share.
#[derive(Clone)]
pub struct MsgEnv {
    /// This DC's tier (the index).
    pub dc: Arc<dyn MetaStore>,
    /// Tenant catalogs.
    pub locator: Arc<ShardMapLocator>,
    /// `sprig_ctl` pools per cluster.
    pub pools: Arc<dyn ClusterPools>,
    /// The gateways runs go to.
    pub gateways: Arc<Gateways>,
    /// Retries, attempts, lanes (following the worker's live settings).
    pub opts: RunOpts,
    /// The worker's live settings: `logic.trigger_budget_ms` when a
    /// trigger job starts.
    pub settings: crate::settings::WorkerSettings,
}

impl std::fmt::Debug for MsgEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MsgEnv").finish_non_exhaustive()
    }
}

/// A job's binding as the catalog has it now.
struct Loaded {
    source: SourceRow,
    version: u64,
    catalog: Pool,
    schema: String,
}

fn spec_u64(spec: &Value, k: &str) -> std::result::Result<u64, String> {
    spec[k]
        .as_u64()
        .ok_or_else(|| format!("spec.{k} is missing"))
}

impl MsgEnv {
    /// The binding, or `None` when it (or its source row) is gone or
    /// paused.
    async fn load(&self, tenant: TenantId, binding: u64) -> Result<Option<Loaded>> {
        let ep = self.locator.writer(tenant).await?;
        let schema = ep.store.schema().to_owned();
        let mut c = ep.pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
        })?;
        let Some(source) = messages::source(&mut c, &schema, binding).await? else {
            return Ok(None);
        };
        let store = LogicStore::new(&schema);
        let b = store.binding_by_id(&mut c, binding).await?;
        if b.as_ref().is_none_or(|b| b.state != "active") {
            return Ok(None);
        }
        let Some(m) = store.module_by_id(&mut c, source.module_id).await? else {
            return Ok(None);
        };
        if m.state != "active" {
            return Ok(None);
        }
        let Some(version) = m.active_version else {
            return Ok(None);
        };
        // The source's database (a dropped database's streams and queues
        // are gone, even when a new one has its name).
        let db_state: Option<String> = c
            .exec_first(
                format!(
                    "SELECT state FROM {}.`databases` WHERE db_id = ?",
                    quote(&schema)
                ),
                (source.db_id,),
            )
            .await
            .map_err(|e| {
                SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
            })?;
        if db_state.as_deref() != Some("active") {
            return Ok(None);
        }
        // The source itself.
        let alive = match source.kind {
            SourceKind::Stream => StreamsStore::new(schema.clone())
                .get(&mut c, StreamId(source.object_id))
                .await?
                .is_some_and(|d| d.state == StreamState::Active),
            SourceKind::Queue => {
                let r: std::result::Result<Option<u64>, _> = c
                    .exec_first(
                        format!(
                            "SELECT object_id FROM {}.mq_queues WHERE object_id = ? AND db_id = ? \
                             AND state = 'active'",
                            quote(&schema)
                        ),
                        (source.object_id, source.db_id),
                    )
                    .await;
                matches!(r, Ok(Some(_)))
            }
        };
        if !alive {
            return Ok(None);
        }
        Ok(Some(Loaded {
            source,
            version,
            catalog: ep.pool.clone(),
            schema,
        }))
    }

    fn template(&self, tenant: TenantId, l: &Loaded) -> Template {
        let budget = self.settings.trigger_budget();
        Template {
            req: RunRequest {
                tenant: tenant.0,
                source: l.source.clone(),
                version: l.version,
                record: None,
                max_attempts: self.opts.current().max_attempts,
                lease_ms: 30_000.max(u64::try_from(budget.as_millis()).unwrap_or(30_000) + 5_000),
                budget_ms: u64::try_from(budget.as_millis()).unwrap_or(30_000),
            },
            version: Arc::new(AtomicU64::new(l.version)),
        }
    }

    async fn shard_pool(&self, tenant: TenantId) -> Result<Pool> {
        let cluster = self.locator.cluster_of(tenant).await?.ok_or_else(|| {
            let mut e = SprigError::new(
                SprigCode::TenantNotReady,
                format!("tenant {tenant} has no shard map here"),
            );
            e.retryable = true;
            e
        })?;
        self.pools.writer(&cluster).await
    }

    /// Supervises a running trigger: progress every second, a new version
    /// or a dropped source every [`RELOAD_EVERY`]. Returns `true` when the
    /// trigger is gone (its index entry is then removed).
    async fn supervise(
        &self,
        cx: &JobCtx,
        tenant: TenantId,
        binding: u64,
        t: &Template,
        stats: &MsgStats,
        stop: &sprig_common::CancellationToken,
    ) -> bool {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let mut last_reload = Instant::now();
        loop {
            tokio::select! {
                () = stop.cancelled() => return false,
                _ = tick.tick() => {}
            }
            let mut p = stats.json();
            p["binding"] = json!(binding);
            p["version"] = json!(t.version.load(Ordering::Relaxed));
            match cx.progress(p).await {
                Ok(true) => {}
                Ok(false) => {
                    stop.cancel();
                    return false;
                }
                Err(e) => tracing::warn!(error = %e, "trigger job progress not written"),
            }
            if last_reload.elapsed() >= RELOAD_EVERY {
                last_reload = Instant::now();
                match self.load(tenant, binding).await {
                    Ok(Some(l)) => {
                        let old = t.version.swap(l.version, Ordering::Relaxed);
                        if old != l.version {
                            tracing::info!(%tenant, binding, version = l.version, "trigger: new version");
                        }
                    }
                    Ok(None) => {
                        tracing::info!(%tenant, binding, "trigger's binding or source is gone; its job ends");
                        let _ = messages::delete_index(self.dc.as_ref(), tenant, binding).await;
                        stop.cancel();
                        return true;
                    }
                    Err(e) => tracing::debug!(error = %e, "trigger: catalog unreadable"),
                }
            }
        }
    }
}

/// A stream trigger's stream and offsets, as `sprig_ctl`.
struct MariaSide {
    catalog: Pool,
    schema: String,
    shard: Pool,
    log_schema: String,
    group: String,
    stream: StreamId,
    source: SourceRow,
    version: Arc<AtomicU64>,
}

fn db_err(e: mysql_async::Error) -> SprigError {
    sprig_streams::maria::map_err(e)
}

#[async_trait]
impl StreamSide for MariaSide {
    async fn positions(&self) -> Result<HashMap<u32, u64>> {
        let mut c = self.catalog.get_conn().await.map_err(db_err)?;
        Ok(
            offsets::positions(&mut c, &self.schema, &self.group, self.stream.0)
                .await?
                .into_iter()
                .collect(),
        )
    }

    async fn bounds(&self) -> Result<HashMap<u32, (u64, u64)>> {
        let mut c = self.shard.get_conn().await.map_err(db_err)?;
        let rows: Vec<(u32, u64, u64)> = c
            .exec(
                format!(
                    "SELECT partition_no, log_start, next_offset FROM {}.{} \
                     WHERE stream_id = ? AND partition_no <> ?",
                    quote(&self.log_schema),
                    quote(sprig_streams::model::CKPT_TABLE)
                ),
                (self.stream.0, sprig_streams::model::SHARD_ROW),
            )
            .await
            .map_err(db_err)?;
        Ok(rows.into_iter().map(|(p, s, n)| (p, (s, n))).collect())
    }

    async fn read(&self, p: u32, from: u64, max: u32) -> Result<Vec<RecordIn>> {
        let mut c = self.shard.get_conn().await.map_err(db_err)?;
        let r = LogReader {
            schema: self.log_schema.clone(),
        }
        .read(
            &mut c,
            self.stream,
            PartitionNo(p),
            sprig_streams::Offset(from),
            ReadLimits {
                max_records: max,
                max_bytes: 4 << 20,
            },
        )
        .await;
        let batch = match r {
            Ok(b) => b,
            // Below the log start: the runner moves past it.
            Err(e) if e.code() == Some(SprigCode::StreamOffsetOutOfRange) => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        Ok(batch
            .records
            .into_iter()
            .map(|r| RecordIn {
                partition: r.partition.0,
                offset: r.offset.0,
                ts_ms: r.ts_ms,
                key: r.key,
                value: r.value,
                headers: r.headers,
            })
            .collect())
    }

    async fn init(&self, p: u32, at: u64) -> Result<()> {
        let mut c = self.catalog.get_conn().await.map_err(db_err)?;
        offsets::init_positions(&mut c, &self.schema, &self.group, self.stream.0, &[(p, at)]).await
    }

    async fn advance(&self, p: u32, expect: u64, to: u64) -> Result<bool> {
        let mut c = self.catalog.get_conn().await.map_err(db_err)?;
        offsets::advance(
            &mut c,
            &self.schema,
            &self.group,
            self.stream.0,
            p,
            expect,
            to,
        )
        .await
    }

    async fn dead_letter(&self, r: &RecordIn, attempts: u32, last: &RunReply) -> Result<()> {
        let mut c = self.catalog.get_conn().await.map_err(db_err)?;
        let store = LogicStore::new(&self.schema);
        let version = self.version.load(Ordering::Relaxed);
        let event =
            messages::record_value(&format!("{}.{}", self.source.db_name, self.source.name), r);
        store
            .put_dead_letter(
                &mut c,
                self.source.binding_id,
                version,
                &format!("p{}:{}", r.partition, r.offset),
                b"",
                &event,
                attempts,
                last.code,
                &last.message,
            )
            .await?;
        let _ = store
            .log(
                &mut c,
                self.source.module_id,
                Some(version),
                "error",
                "trigger",
                &format!(
                    "stream {}.{} partition {} offset {}: dead letter after {attempts} attempts: {} {}",
                    self.source.db_name,
                    self.source.name,
                    r.partition,
                    r.offset,
                    last.code,
                    last.message
                ),
            )
            .await;
        Ok(())
    }
}

/// `logic_stream_trigger` (see the module docs).
#[derive(Debug, Clone)]
pub struct StreamTrigger(pub MsgEnv);

#[async_trait]
impl JobHandler for StreamTrigger {
    fn kind(&self) -> &'static str {
        messages::STREAM_JOB_KIND
    }

    fn class(&self) -> JobClass {
        JobClass::Consumer
    }

    async fn run(&self, cx: &JobCtx) -> std::result::Result<Value, String> {
        let env = &self.0;
        let tenant = TenantId(spec_u64(&cx.spec, "tenant")?);
        let binding = spec_u64(&cx.spec, "binding")?;
        let Some(l) = env.load(tenant, binding).await.map_err(|e| e.to_string())? else {
            let _ = messages::delete_index(env.dc.as_ref(), tenant, binding).await;
            return Ok(json!({"end": "gone", "binding": binding}));
        };
        let def = {
            let mut c = l.catalog.get_conn().await.map_err(|e| e.to_string())?;
            StreamsStore::new(l.schema.clone())
                .get(&mut c, StreamId(l.source.object_id))
                .await
                .map_err(|e| e.to_string())?
                .ok_or("the stream is gone")?
        };
        let log = def.log.first().ok_or("the stream has no log shard")?;
        let shard = env.shard_pool(tenant).await.map_err(|e| e.to_string())?;
        let t = env.template(tenant, &l);
        let side: Arc<dyn StreamSide> = Arc::new(MariaSide {
            catalog: l.catalog.clone(),
            schema: l.schema.clone(),
            shard,
            log_schema: log.slot.schema(tenant, def.db).to_string(),
            group: offsets::logic_group(binding),
            stream: def.id,
            source: l.source.clone(),
            version: Arc::clone(&t.version),
        });
        let log = RunLog {
            catalog: l.catalog.clone(),
            schema: l.schema.clone(),
            module_id: l.source.module_id,
            version: Arc::clone(&t.version),
        };
        log.write(
            "info",
            &format!(
                "trigger on stream {}.{} started (job {}, {} partitions)",
                l.source.db_name, l.source.name, cx.job_id, def.partitions
            ),
        )
        .await;
        let gw: Arc<dyn Gateway> = Arc::new(GwClient {
            gws: Arc::clone(&env.gateways),
            seed: binding,
            log,
        });
        let stats = Arc::new(MsgStats::default());
        let stop = cx.cancel.child_token();
        tracing::info!(%tenant, binding, stream = def.id.0, partitions = def.partitions, "trigger on a stream started");
        let runner = msgrun::run_stream(
            side,
            gw,
            t.clone(),
            env.opts.clone(),
            Arc::clone(&stats),
            def.partitions,
            Duration::from_millis(100),
            stop.clone(),
        );
        tokio::pin!(runner);
        let sup = env.supervise(cx, tenant, binding, &t, &stats, &stop);
        tokio::pin!(sup);
        let (r, gone) = tokio::select! {
            r = &mut runner => {
                stop.cancel();
                (r, false)
            }
            gone = &mut sup => {
                stop.cancel();
                (runner.await, gone)
            }
        };
        let mut out = stats.json();
        out["binding"] = json!(binding);
        if gone {
            out["end"] = json!("gone");
            return Ok(out);
        }
        r.map(|()| out).map_err(|e| e.to_string())
    }
}

/// `logic_queue_trigger` (see the module docs).
#[derive(Debug, Clone)]
pub struct QueueTrigger(pub MsgEnv);

#[async_trait]
impl JobHandler for QueueTrigger {
    fn kind(&self) -> &'static str {
        messages::QUEUE_JOB_KIND
    }

    fn class(&self) -> JobClass {
        JobClass::Consumer
    }

    async fn run(&self, cx: &JobCtx) -> std::result::Result<Value, String> {
        let env = &self.0;
        let tenant = TenantId(spec_u64(&cx.spec, "tenant")?);
        let binding = spec_u64(&cx.spec, "binding")?;
        let Some(l) = env.load(tenant, binding).await.map_err(|e| e.to_string())? else {
            let _ = messages::delete_index(env.dc.as_ref(), tenant, binding).await;
            return Ok(json!({"end": "gone", "binding": binding}));
        };
        let t = env.template(tenant, &l);
        let log = RunLog {
            catalog: l.catalog.clone(),
            schema: l.schema.clone(),
            module_id: l.source.module_id,
            version: Arc::clone(&t.version),
        };
        log.write(
            "info",
            &format!(
                "trigger on queue {} of vhost {} started (job {}, {} lanes)",
                l.source.name, l.source.db_name, cx.job_id, env.opts.lanes
            ),
        )
        .await;
        let gw: Arc<dyn Gateway> = Arc::new(GwClient {
            gws: Arc::clone(&env.gateways),
            seed: binding,
            log,
        });
        let stats = Arc::new(MsgStats::default());
        let stop = cx.cancel.child_token();
        tracing::info!(%tenant, binding, queue = %l.source.name, lanes = env.opts.lanes, "trigger on a queue started");
        let runner = msgrun::run_queue(
            gw,
            t.clone(),
            env.opts.clone(),
            Arc::clone(&stats),
            stop.clone(),
        );
        tokio::pin!(runner);
        let sup = env.supervise(cx, tenant, binding, &t, &stats, &stop);
        tokio::pin!(sup);
        let gone = tokio::select! {
            () = &mut runner => {
                stop.cancel();
                false
            }
            gone = &mut sup => {
                stop.cancel();
                runner.await;
                gone
            }
        };
        let mut out = stats.json();
        out["binding"] = json!(binding);
        if gone {
            out["end"] = json!("gone");
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rendezvous_is_stable_and_spreads() {
        let a = rendezvous(7, "gw-1");
        assert_eq!(a, rendezvous(7, "gw-1"));
        let firsts: std::collections::HashSet<&str> = (0..64u64)
            .map(|b| {
                ["gw-1", "gw-2", "gw-3"]
                    .into_iter()
                    .max_by_key(|g| rendezvous(b, g))
                    .unwrap_or("gw-1")
            })
            .collect();
        assert_eq!(firsts.len(), 3, "bindings spread over the gateways");
    }

    #[test]
    fn gateway_records_parse() {
        let raw = br#"{"v":1,"gateway_id":"g1","slot":3,"version":"0.1","addr":"127.0.0.1","listeners":{"peer":7403,"mysql":3306},"state":"ready","load":{"connections":0,"active":0}}"#;
        let r = Record::<GwRecord>::decode(raw).unwrap();
        assert_eq!(r.body.listeners.get("peer"), Some(&7403));
        assert_eq!(r.body.state, "ready");
    }
}
