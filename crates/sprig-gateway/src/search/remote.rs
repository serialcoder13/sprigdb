//! Copies on workers, from the gateway's side (chapter 39, "Placement of
//! copies", "Scatter-gather"; R5 round 6): the index's record in the DC
//! tier, the live search workers, and queries sent to a ready copy.
//!
//! ```text
//! query(index, ops, consistency):
//!   put the index's record once per definition (the controller places copies)
//!   latest: every DC cluster's @@gtid_binlog_pos now (the targets)
//!   ready copies of the generation on live workers, most recently reported first:
//!     Query; a retryable error or an unreachable worker → the next copy
//!   none ready: poll every 100 ms until one is, up to search.wait_timeout_ms
//!     → 39824 (copies seeding) or 39825 (no copy placed yet)
//! ```

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sprig_api::proto::internal::SearchPartitionQueryRequest;
use sprig_api::proto::internal::search_partition_client::SearchPartitionClient;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{ListOpts, MetaStore, Record, Versioned};
use sprig_search::analyze::CustomAnalyzer;
use sprig_search::def::IndexDef;
use sprig_search::placement::{self, CopyRecord, IndexRecord};
use sprig_search::rpc::{Answer, QueryReply, QueryRequest};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::peer::PeerTls;

fn unavailable(msg: impl Into<String>) -> SprigError {
    let mut e = SprigError::new(SprigCode::SearchUnavailable, msg);
    e.retryable = true;
    e
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

/// The gateway's view of copies on workers.
pub(crate) struct Remote {
    store: Arc<dyn MetaStore>,
    tls: Option<PeerTls>,
    channels: Mutex<HashMap<String, Channel>>,
    workers: Mutex<Option<(Instant, HashSet<String>)>>,
    written: Mutex<HashMap<(u64, u64), IndexRecord>>,
    copies: Mutex<HashMap<(u64, u64), (Instant, Vec<CopyRecord>)>>,
}

impl std::fmt::Debug for Remote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Remote")
            .field("mtls", &self.tls.is_some())
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Remote {
    pub(crate) fn new(store: Arc<dyn MetaStore>, tls: Option<PeerTls>) -> Self {
        Self {
            store,
            tls,
            channels: Mutex::new(HashMap::new()),
            workers: Mutex::new(None),
            written: Mutex::new(HashMap::new()),
            copies: Mutex::new(HashMap::new()),
        }
    }

    /// Workers of this DC with the search role (kept 2 s).
    ///
    /// # Errors
    /// The store's.
    pub(crate) async fn search_workers(&self) -> Result<HashSet<String>> {
        if let Some((at, w)) = lock(&self.workers).as_ref()
            && at.elapsed() < Duration::from_secs(2)
        {
            return Ok(w.clone());
        }
        let prefix = self.store.tier().key("workers/")?;
        let page = self
            .store
            .list(
                &prefix,
                ListOpts {
                    limit: 10_000,
                    ..ListOpts::default()
                },
            )
            .await?;
        let mut out = HashSet::new();
        for kv in &page.kvs {
            let Ok(r) = Record::<WorkerReg>::decode(&kv.value) else {
                continue;
            };
            if r.body.slots.get("search").copied().unwrap_or(0) > 0
                && let Some(id) = kv.key.as_str().rsplit('/').next()
            {
                out.insert(id.to_owned());
            }
        }
        *lock(&self.workers) = Some((Instant::now(), out.clone()));
        Ok(out)
    }

    /// Writes the index's record when this gateway hasn't written this
    /// definition yet.
    ///
    /// # Errors
    /// The store's.
    pub(crate) async fn ensure(
        &self,
        tenant: TenantId,
        def: &IndexDef,
        customs: &[CustomAnalyzer],
        copies: u32,
    ) -> Result<()> {
        let rec = IndexRecord {
            tenant: tenant.0,
            index: def.index_id,
            def: def.clone(),
            analyzers: customs.to_vec(),
            copies,
        };
        let k = (tenant.0, def.index_id);
        if lock(&self.written).get(&k) == Some(&rec) {
            return Ok(());
        }
        if placement::put_index(&self.store, &rec).await? {
            tracing::info!(tenant = %tenant, index = def.name, generation = def.generation, copies, "search index placed on workers");
        }
        lock(&self.written).insert(k, rec);
        Ok(())
    }

    /// Removes the index's records (a dropped index).
    pub(crate) async fn drop_index(&self, tenant: TenantId, index: u64) {
        lock(&self.written).remove(&(tenant.0, index));
        if let Err(e) = placement::drop_index(&self.store, tenant.0, index).await {
            tracing::warn!(tenant = %tenant, index, error = %e, "search index record not removed");
        }
    }

    /// The index's copies (kept 500 ms).
    ///
    /// # Errors
    /// The store's.
    pub(crate) async fn copies(&self, tenant: TenantId, index: u64, fresh: bool) -> Result<Vec<CopyRecord>> {
        let k = (tenant.0, index);
        if !fresh
            && let Some((at, v)) = lock(&self.copies).get(&k)
            && at.elapsed() < Duration::from_millis(500)
        {
            return Ok(v.clone());
        }
        let v = placement::list_copies(&self.store, tenant.0, index).await?;
        lock(&self.copies).insert(k, (Instant::now(), v.clone()));
        Ok(v)
    }

    fn client(&self, addr: &str) -> Result<SearchPartitionClient<Channel>> {
        let scheme = if self.tls.is_some() { "https" } else { "http" };
        let uri = format!("{scheme}://{addr}");
        let mut channels = lock(&self.channels);
        if let Some(c) = channels.get(&uri) {
            return Ok(SearchPartitionClient::new(c.clone()).max_decoding_message_size(256 << 20));
        }
        let mut ep = Endpoint::from_shared(uri.clone())
            .map_err(|e| unavailable(format!("search worker address {uri}: {e}")))?
            .connect_timeout(Duration::from_secs(2));
        if let Some(t) = &self.tls {
            let host = addr.rsplit_once(':').map_or(addr, |(h, _)| h).to_owned();
            let cfg = ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(&t.ca_pem))
                .identity(Identity::from_pem(&t.cert_pem, &t.key_pem))
                .domain_name(host);
            ep = ep
                .tls_config(cfg)
                .map_err(|e| unavailable(format!("search worker TLS: {e}")))?;
        }
        let ch = ep.connect_lazy();
        channels.insert(uri, ch.clone());
        Ok(SearchPartitionClient::new(ch).max_decoding_message_size(256 << 20))
    }

    async fn ask(&self, c: &CopyRecord, req: &str, timeout: Duration) -> Result<QueryReply> {
        let mut client = self.client(&c.addr)?;
        let mut r = tonic::Request::new(SearchPartitionQueryRequest {
            request_json: req.to_owned(),
        });
        r.set_timeout(timeout);
        let reply = client.query(r).await.map_err(|s| {
            unavailable(format!(
                "search copy {} on worker {}: {}",
                c.copy,
                c.worker_id,
                s.message()
            ))
        })?;
        serde_json::from_str(&reply.get_ref().reply_json)
            .map_err(|e| SprigError::internal(format!("a search copy's reply: {e}")))
    }

    /// Runs `req` on a ready copy, trying the others on retryable errors;
    /// waits for one up to `wait` when none is ready.
    ///
    /// # Errors
    /// 39824, 39825, and the query's own errors.
    pub(crate) async fn query(
        &self,
        tenant: TenantId,
        def: &IndexDef,
        req: &QueryRequest,
        wait: Duration,
        call_timeout: Duration,
    ) -> Result<(Vec<Answer>, String)> {
        let body = serde_json::to_string(req)
            .map_err(|e| SprigError::internal(format!("a search query: {e}")))?;
        let started = Instant::now();
        let mut last: Option<SprigError> = None;
        let mut fresh = false;
        loop {
            let live = self.search_workers().await?;
            let copies = self.copies(tenant, def.index_id, fresh).await?;
            let (ready, seeding) =
                placement::query_order(&copies, def.generation, &|w| live.contains(w));
            for c in &ready {
                let left = call_timeout.max(wait.saturating_sub(started.elapsed()));
                match self.ask(c, &body, left + Duration::from_millis(500)).await {
                    Ok(r) => match r.answers {
                        Ok(a) => {
                            metrics::counter!("sprig_search_remote_queries_total", "result" => "ok")
                                .increment(1);
                            return Ok((a, serde_json::to_string(&r.applied).unwrap_or_default()));
                        }
                        Err(w) if w.retryable => last = Some(w.error()),
                        Err(w) => return Err(w.error()),
                    },
                    Err(e) => last = Some(e),
                }
                metrics::counter!("sprig_search_remote_queries_total", "result" => "retried")
                    .increment(1);
            }
            if started.elapsed() >= wait {
                return Err(last.unwrap_or_else(|| {
                    if seeding.is_empty() && ready.is_empty() {
                        unavailable(format!(
                            "search index '{}' has no copy on a live worker yet",
                            def.name
                        ))
                    } else {
                        SprigError::new(
                            SprigCode::SearchIndexNotReady,
                            format!("search index '{}' is still being built", def.name),
                        )
                    }
                }));
            }
            fresh = true;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// `ready on w1, w2` / `seeding on w1` / `placing`, for `SHOW SEARCH
    /// INDEXES` and `$listSearchIndexes`.
    pub(crate) async fn state(&self, tenant: TenantId, def: &IndexDef) -> (bool, String) {
        let Ok(live) = self.search_workers().await else {
            return (false, "unknown".to_owned());
        };
        let Ok(copies) = self.copies(tenant, def.index_id, true).await else {
            return (false, "unknown".to_owned());
        };
        let (ready, seeding) = placement::query_order(&copies, def.generation, &|w| live.contains(w));
        let names = |v: &[CopyRecord]| {
            let mut w: Vec<&str> = v.iter().map(|c| c.worker_id.as_str()).collect();
            w.sort_unstable();
            w.join(", ")
        };
        if !ready.is_empty() {
            (true, format!("ready on {}", names(&ready)))
        } else if !seeding.is_empty() {
            (false, format!("seeding on {}", names(&seeding)))
        } else {
            (false, "placing".to_owned())
        }
    }
}
