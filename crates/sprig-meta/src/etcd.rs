//! `EtcdStore`, the [`MetaStore`] over the `etcd-client` crate (chapter 23,
//! build step 2).
//!
//! It maps the trait onto etcd's v3 API one to one:
//! - [`MetaStore::list`] is a range read pinned to one revision, paged with
//!   `limit` and a start key just after the previous page's last key;
//! - [`Cmp::Missing`] compares `version == 0`; [`Cmp::CreateRev`] and
//!   [`Cmp::ModRev`] also require `version > 0`, so a missing key never
//!   matches, as in [`MemStore`](crate::MemStore);
//! - a watch waits for etcd to confirm it was created before it returns, so
//!   a watch from 0 sees every write that follows the call;
//! - keep-alives reuse one stream per lease.
//!
//! Every unary call has a deadline (`meta.request_timeout_ms`, 2 s by
//! default). Transport failures, timeouts and "no leader" are 39500
//! `ER_SPRIG_META_UNAVAILABLE` (retryable); bad requests are internal
//! errors, as they are in `MemStore`.
//!
//! **Single-etcd mode** (R0): one etcd serves both tiers. [`EtcdStore::
//! with_tier`] gives a second view over the same connection, and
//! [`connect_tiers`] does this when the DC and global seeds are the same.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use etcd_client::{
    Certificate, Client, Compare, CompareOp, ConnectOptions, DeleteOptions, GetOptions, Identity,
    LeaseKeepAliveStream, LeaseKeeper, PutOptions, TlsOptions, TxnOp, TxnOpResponse, WatchOptions,
    WatchRequestSender, WatchResponse, WatchResponseStream,
};
use futures::StreamExt;
use sprig_common::config::{MetaConfig, TlsConfig};
use sprig_common::{Result, SprigCode, SprigError};

use crate::{
    Cmp, Consistency, Key, KeyValue, LeaseId, ListOpts, ListPage, MetaStore, Op, OpResult,
    Revision, Tier, Txn, TxnResult, WatchEvent, WatchStream,
};

/// Default deadline of one etcd request (`meta.request_timeout_ms`).
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_millis(2000);

/// How to reach one etcd cluster.
#[derive(Debug, Clone)]
pub struct EtcdConfig {
    /// Client URLs, `http://` or `https://`. With TLS they must be `https://`.
    pub endpoints: Vec<String>,
    /// Mutual TLS; `None` for plain text (development only).
    pub tls: Option<EtcdTls>,
    /// Deadline of each unary request.
    pub request_timeout: Duration,
    /// Deadline of establishing a connection.
    pub connect_timeout: Duration,
}

impl EtcdConfig {
    /// Plain-text endpoints with the default timeouts.
    #[must_use]
    pub fn new(endpoints: Vec<String>) -> Self {
        Self {
            endpoints,
            tls: None,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            connect_timeout: DEFAULT_REQUEST_TIMEOUT,
        }
    }

    /// The same endpoints over mutual TLS.
    #[must_use]
    pub fn with_tls(mut self, tls: EtcdTls) -> Self {
        self.tls = Some(tls);
        self
    }
}

/// Mutual TLS material: the CA that signed etcd's certificates, and this
/// process's certificate and key. etcd maps the certificate's common name to
/// its user (chapter 23, "Security notes").
#[derive(Clone)]
pub struct EtcdTls {
    /// The CA bundle, PEM.
    pub ca_pem: Vec<u8>,
    /// This process's certificate chain, PEM.
    pub cert_pem: Vec<u8>,
    /// This process's private key, PEM.
    pub key_pem: Vec<u8>,
    /// The name to verify in etcd's certificate; `None` uses the endpoint's
    /// host.
    pub domain: Option<String>,
}

impl std::fmt::Debug for EtcdTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print the key.
        f.debug_struct("EtcdTls")
            .field("domain", &self.domain)
            .finish_non_exhaustive()
    }
}

impl EtcdTls {
    /// Reads the files named by `[tls]` in `sprigdb.toml`.
    ///
    /// # Errors
    /// 39998 `ER_SPRIG_CONFIG_INVALID` if a file can't be read.
    pub fn from_files(tls: &TlsConfig) -> Result<Self> {
        Ok(Self {
            ca_pem: read_pem(&tls.ca)?,
            cert_pem: read_pem(&tls.cert)?,
            key_pem: read_pem(&tls.key)?,
            domain: None,
        })
    }

    pub(crate) fn options(&self) -> TlsOptions {
        let mut o = TlsOptions::new()
            .ca_certificate(Certificate::from_pem(&self.ca_pem))
            .identity(Identity::from_pem(&self.cert_pem, &self.key_pem));
        if let Some(d) = &self.domain {
            o = o.domain_name(d.clone());
        }
        o
    }
}

fn read_pem(path: &Path) -> Result<Vec<u8>> {
    std::fs::read(path).map_err(|e| {
        SprigError::new(
            SprigCode::ConfigInvalid,
            format!("cannot read {}: {e}", path.display()),
        )
    })
}

/// The stores of one process: its DC tier and, on controllers, the global
/// tier. In single-etcd mode both share one connection.
#[derive(Debug, Clone)]
pub struct TierStores {
    /// This DC's tier.
    pub dc: EtcdStore,
    /// The global tier, when `global_seeds` are configured.
    pub global: Option<EtcdStore>,
}

/// Connects to the tiers named by `[meta]`. When `global_seeds` is empty or
/// equal to `dc_seeds` (single-etcd mode), the global store reuses the DC
/// store's connection.
///
/// # Errors
/// 39998 `ER_SPRIG_CONFIG_INVALID` if no DC seed is configured; 39500
/// `ER_SPRIG_META_UNAVAILABLE` if no seed answers.
pub async fn connect_tiers(
    dc: sprig_common::ids::DcId,
    meta: &MetaConfig,
    tls: Option<EtcdTls>,
    request_timeout: Duration,
    want_global: bool,
) -> Result<TierStores> {
    if meta.dc_seeds.is_empty() {
        return Err(SprigError::new(
            SprigCode::ConfigInvalid,
            "[meta] dc_seeds is empty",
        ));
    }
    let cfg = |endpoints: &[String]| EtcdConfig {
        endpoints: endpoints.to_vec(),
        tls: tls.clone(),
        request_timeout,
        connect_timeout: request_timeout,
    };
    let dc_store = EtcdStore::connect(Tier::Dc(dc), &cfg(&meta.dc_seeds)).await?;
    let global = if !want_global {
        None
    } else if meta.global_seeds.is_empty() || same_set(&meta.global_seeds, &meta.dc_seeds) {
        Some(dc_store.with_tier(Tier::Global))
    } else {
        Some(EtcdStore::connect(Tier::Global, &cfg(&meta.global_seeds)).await?)
    };
    Ok(TierStores {
        dc: dc_store,
        global,
    })
}

fn same_set(a: &[String], b: &[String]) -> bool {
    let mut a: Vec<&str> = a.iter().map(|s| s.trim_end_matches('/')).collect();
    let mut b: Vec<&str> = b.iter().map(|s| s.trim_end_matches('/')).collect();
    a.sort_unstable();
    b.sort_unstable();
    a == b
}

/// An open keep-alive stream of one lease.
type Keeper = (LeaseKeeper, LeaseKeepAliveStream);
type KeeperSlot = Arc<tokio::sync::Mutex<Option<Keeper>>>;

/// State shared by every view of one connection.
#[derive(Default)]
struct Shared {
    /// One keep-alive stream per lease, opened on first use.
    keepers: Mutex<HashMap<LeaseId, KeeperSlot>>,
    /// TTLs etcd granted to leases this process created.
    granted: Mutex<HashMap<LeaseId, Duration>>,
    /// Request senders of live watches, for [`EtcdStore::request_progress`].
    watchers: Mutex<Vec<Weak<tokio::sync::Mutex<WatchRequestSender>>>>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A [`MetaStore`] on etcd.
#[derive(Clone)]
pub struct EtcdStore {
    tier: Tier,
    client: Client,
    timeout: Duration,
    shared: Arc<Shared>,
    /// How it connected: the seeds and TLS, for endpoint discovery.
    cfg: Arc<EtcdConfig>,
}

impl std::fmt::Debug for EtcdStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EtcdStore")
            .field("tier", &self.tier)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl EtcdStore {
    /// Connects and checks that etcd answers.
    ///
    /// # Errors
    /// 39998 `ER_SPRIG_CONFIG_INVALID` for no endpoints or TLS over
    /// `http://`; 39500 `ER_SPRIG_META_UNAVAILABLE` if no endpoint answers.
    pub async fn connect(tier: Tier, cfg: &EtcdConfig) -> Result<Self> {
        if cfg.endpoints.is_empty() {
            return Err(SprigError::new(
                SprigCode::ConfigInvalid,
                "no etcd endpoints",
            ));
        }
        let mut opts = ConnectOptions::new()
            .with_connect_timeout(cfg.connect_timeout)
            .with_keep_alive(Duration::from_secs(10), Duration::from_secs(5))
            .with_keep_alive_while_idle(true)
            .with_require_leader(true);
        if let Some(tls) = &cfg.tls {
            if let Some(plain) = cfg.endpoints.iter().find(|e| e.starts_with("http://")) {
                return Err(SprigError::new(
                    SprigCode::ConfigInvalid,
                    format!("etcd endpoint {plain} is http:// but TLS is configured"),
                ));
            }
            opts = opts.with_tls(tls.options());
        }
        let connect = Client::connect(&cfg.endpoints, Some(opts));
        let client = match tokio::time::timeout(cfg.connect_timeout, connect).await {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => return Err(map_err("connect", e)),
            Err(_) => return Err(timed_out("connect", cfg.connect_timeout)),
        };
        let store = Self {
            tier,
            client,
            timeout: cfg.request_timeout,
            shared: Arc::default(),
            cfg: Arc::new(cfg.clone()),
        };
        // The channel connects lazily; one read proves a seed answers.
        let root = store.tier.root();
        store
            .call(
                "connect",
                store.client.kv_client().get(
                    root.as_str(),
                    Some(GetOptions::new().with_count_only().with_serializable()),
                ),
            )
            .await?;
        Ok(store)
    }

    /// Another tier over the same connection (single-etcd mode).
    #[must_use]
    pub fn with_tier(&self, tier: Tier) -> Self {
        Self {
            tier,
            client: self.client.clone(),
            timeout: self.timeout,
            shared: Arc::clone(&self.shared),
            cfg: Arc::clone(&self.cfg),
        }
    }

    /// The client, for endpoint discovery.
    pub(crate) fn client(&self) -> &Client {
        &self.client
    }

    /// The configuration it connected with.
    pub(crate) fn config(&self) -> &EtcdConfig {
        &self.cfg
    }

    /// Compacts etcd's history at or below `revision`, as
    /// [`MemStore::compact`](crate::MemStore::compact) does. Compaction is
    /// cluster-wide, across both tiers in single-etcd mode. Compacting at
    /// or below an earlier compaction does nothing.
    ///
    /// # Errors
    /// 39500 if etcd is unreachable; internal for a future revision.
    pub async fn compact(&self, revision: Revision) -> Result<()> {
        let rev = to_i64(revision)?;
        match self
            .call("compact", self.client.kv_client().compact(rev, None))
            .await
        {
            Ok(_) => Ok(()),
            // "mvcc: required revision has been compacted"
            Err(e) if e.message.contains("compacted") => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Asks etcd to send a progress mark to every live watch of this
    /// connection, as [`MemStore::request_progress`](crate::MemStore::request_progress)
    /// does.
    ///
    /// # Errors
    /// 39500 if a request can't be sent.
    pub async fn request_progress(&self) -> Result<()> {
        let senders: Vec<_> = {
            let mut w = lock(&self.shared.watchers);
            w.retain(|s| s.strong_count() > 0);
            w.iter().filter_map(Weak::upgrade).collect()
        };
        for s in senders {
            s.lock()
                .await
                .request_progress()
                .await
                .map_err(|e| map_err("request_progress", e))?;
        }
        Ok(())
    }

    /// Runs one request with the store's deadline.
    async fn call<T>(
        &self,
        op: &'static str,
        f: impl Future<Output = std::result::Result<T, etcd_client::Error>>,
    ) -> Result<T> {
        match tokio::time::timeout(self.timeout, f).await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => Err(map_err(op, e)),
            Err(_) => Err(timed_out(op, self.timeout)),
        }
    }

    fn keeper_slot(&self, lease: LeaseId) -> KeeperSlot {
        Arc::clone(lock(&self.shared.keepers).entry(lease).or_default())
    }

    fn forget_lease(&self, lease: LeaseId) {
        lock(&self.shared.keepers).remove(&lease);
        lock(&self.shared.granted).remove(&lease);
    }

    /// One keep-alive round trip on the lease's stream, opening it first if
    /// needed. Returns etcd's TTL in seconds; 0 means the lease is gone.
    async fn keep_alive_once(
        &self,
        lease: LeaseId,
        slot: &mut Option<Keeper>,
    ) -> std::result::Result<i64, etcd_client::Error> {
        if let Some((keeper, stream)) = slot.as_mut() {
            keeper.keep_alive().await?;
            return match stream.message().await? {
                Some(r) => Ok(r.ttl()),
                None => Err(etcd_client::Error::LeaseKeepAliveError(
                    "keep-alive stream closed".to_owned(),
                )),
            };
        }
        // Opening the stream sends one keep-alive and consumes its answer.
        match self.client.lease_client().keep_alive(lease.0).await {
            Ok(k) => {
                *slot = Some(k);
            }
            Err(etcd_client::Error::LeaseKeepAliveError(m)) if m.contains("not found") => {
                return Ok(0);
            }
            Err(e) => return Err(e),
        }
        let known = lock(&self.shared.granted).get(&lease).copied();
        match known {
            Some(ttl) => Ok(i64::try_from(ttl.as_secs()).unwrap_or(i64::MAX)),
            // A lease another process granted: ask again for its TTL.
            None => Box::pin(self.keep_alive_once(lease, slot)).await,
        }
    }
}

/// The longest one keep-alive may take inside the store; callers bound it
/// by their lease's deadline.
pub const KEEP_ALIVE_BOUND: Duration = Duration::from_secs(10);

fn timed_out(op: &str, after: Duration) -> SprigError {
    SprigError::new(
        SprigCode::MetaUnavailable,
        format!("etcd {op} timed out after {} ms", after.as_millis()),
    )
}

/// Maps an etcd client error: unreachable or leaderless is 39500, a bad
/// request is internal.
fn map_err(op: &str, e: etcd_client::Error) -> SprigError {
    use etcd_client::Error as E;
    use tonic::Code;
    let unavailable = match &e {
        E::GRpcStatus(s) => match s.code() {
            Code::Unavailable
            | Code::DeadlineExceeded
            | Code::Cancelled
            | Code::Aborted
            | Code::ResourceExhausted
            | Code::Unknown => true,
            // Reads at a compacted revision, as MemStore reports them.
            Code::OutOfRange => s.message().contains("compacted"),
            _ => false,
        },
        E::TransportError(_)
        | E::IoError(_)
        | E::WatchError(_)
        | E::LeaseKeepAliveError(_)
        | E::EndpointError(_) => true,
        _ => false,
    };
    let message = match &e {
        E::GRpcStatus(s) => format!("etcd {op}: {}", s.message()),
        other => format!("etcd {op}: {other}"),
    };
    let code = if unavailable {
        SprigCode::MetaUnavailable
    } else {
        SprigCode::Internal
    };
    SprigError::new(code, message).with_source(e)
}

fn to_i64(r: u64) -> Result<i64> {
    i64::try_from(r).map_err(|_| SprigError::internal(format!("revision {r} out of range")))
}

fn to_u64(r: i64) -> u64 {
    u64::try_from(r).unwrap_or(0)
}

fn header_rev(h: Option<&etcd_client::ResponseHeader>) -> Revision {
    h.map_or(0, |h| to_u64(h.revision()))
}

/// The first key after every key that starts with `prefix`.
fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            return end;
        }
    }
    // An all-0xff prefix: to the end of the key space.
    vec![0]
}

fn key_from(raw: &[u8]) -> Result<Key> {
    String::from_utf8(raw.to_vec())
        .map(Key::from_stored)
        .map_err(|_| SprigError::internal("etcd returned a key that isn't UTF-8"))
}

fn kv_from(kv: &etcd_client::KeyValue) -> Result<KeyValue> {
    Ok(KeyValue {
        key: key_from(kv.key())?,
        value: kv.value().to_vec(),
        create_revision: to_u64(kv.create_revision()),
        mod_revision: to_u64(kv.mod_revision()),
        version: to_u64(kv.version()),
        lease: (kv.lease() != 0).then_some(LeaseId(kv.lease())),
    })
}

fn compares(when: &[Cmp]) -> Result<Vec<Compare>> {
    let mut out = Vec::with_capacity(when.len() * 2);
    for c in when {
        match c {
            Cmp::Missing(k) => out.push(Compare::version(k.as_str(), CompareOp::Equal, 0)),
            Cmp::CreateRev(k, r) => {
                out.push(Compare::version(k.as_str(), CompareOp::Greater, 0));
                out.push(Compare::create_revision(
                    k.as_str(),
                    CompareOp::Equal,
                    to_i64(*r)?,
                ));
            }
            Cmp::ModRev(k, r) => {
                out.push(Compare::version(k.as_str(), CompareOp::Greater, 0));
                out.push(Compare::mod_revision(
                    k.as_str(),
                    CompareOp::Equal,
                    to_i64(*r)?,
                ));
            }
        }
    }
    Ok(out)
}

fn txn_ops(ops: &[Op]) -> Vec<TxnOp> {
    ops.iter()
        .map(|op| match op {
            Op::Put { key, value, lease } => TxnOp::put(
                key.as_str(),
                value.clone(),
                lease.map(|l| PutOptions::new().with_lease(l.0)),
            ),
            Op::Delete(key) => TxnOp::delete(key.as_str(), Some(DeleteOptions::new())),
            Op::Get(key) => TxnOp::get(key.as_str(), Some(GetOptions::new())),
        })
        .collect()
}

fn op_result(r: &TxnOpResponse) -> Result<OpResult> {
    Ok(match r {
        TxnOpResponse::Put(_) => OpResult::Put,
        TxnOpResponse::Delete(d) => OpResult::Delete(d.deleted() > 0),
        TxnOpResponse::Get(g) => OpResult::Get(g.kvs().first().map(kv_from).transpose()?),
        TxnOpResponse::Txn(_) => {
            return Err(SprigError::internal("unexpected nested etcd transaction"));
        }
    })
}

/// Turns one watch response into events. `Ok(true)` means the watch ended.
fn watch_events(resp: &WatchResponse, out: &mut VecDeque<Result<WatchEvent>>) -> bool {
    if resp.canceled() {
        if resp.compact_revision() > 0 {
            out.push_back(Ok(WatchEvent::Compacted {
                compact_revision: to_u64(resp.compact_revision()),
            }));
        } else {
            out.push_back(Err(SprigError::new(
                SprigCode::MetaUnavailable,
                format!("etcd cancelled the watch: {}", resp.cancel_reason()),
            )));
        }
        return true;
    }
    if resp.events().is_empty() {
        if !resp.created() {
            out.push_back(Ok(WatchEvent::Progress(header_rev(resp.header()))));
        }
        return false;
    }
    for ev in resp.events() {
        let Some(kv) = ev.kv() else { continue };
        let item = match ev.event_type() {
            etcd_client::EventType::Put => kv_from(kv).map(WatchEvent::Put),
            etcd_client::EventType::Delete => key_from(kv.key()).map(|key| WatchEvent::Delete {
                key,
                mod_revision: to_u64(kv.mod_revision()),
            }),
        };
        let failed = item.is_err();
        out.push_back(item);
        if failed {
            return true;
        }
    }
    false
}

struct WatchState {
    responses: WatchResponseStream,
    /// Keeps the request side open; etcd ends the watch when it closes.
    _sender: Arc<tokio::sync::Mutex<WatchRequestSender>>,
    pending: VecDeque<Result<WatchEvent>>,
    ended: bool,
}

#[async_trait::async_trait]
impl MetaStore for EtcdStore {
    fn tier(&self) -> &Tier {
        &self.tier
    }

    async fn get(&self, key: &Key, c: Consistency) -> Result<Option<KeyValue>> {
        let mut opts = GetOptions::new();
        if c == Consistency::Serializable {
            opts = opts.with_serializable();
        }
        let resp = self
            .call("get", self.client.kv_client().get(key.as_str(), Some(opts)))
            .await?;
        resp.kvs().first().map(kv_from).transpose()
    }

    async fn list(&self, prefix: &Key, opts: ListOpts) -> Result<ListPage> {
        let start = match &opts.after {
            Some(k) => {
                let mut b = k.as_str().as_bytes().to_vec();
                b.push(0);
                b
            }
            None => prefix.as_str().as_bytes().to_vec(),
        };
        let limit = i64::try_from(opts.limit.max(1)).unwrap_or(i64::MAX);
        let mut get = GetOptions::new()
            .with_range(prefix_end(prefix.as_str().as_bytes()))
            .with_limit(limit);
        if let Some(at) = opts.at {
            get = get.with_revision(to_i64(at)?);
        }
        let resp = self
            .call("list", self.client.kv_client().get(start, Some(get)))
            .await?;
        let kvs = resp.kvs().iter().map(kv_from).collect::<Result<Vec<_>>>()?;
        Ok(ListPage {
            kvs,
            revision: opts.at.unwrap_or_else(|| header_rev(resp.header())),
            more: resp.more(),
        })
    }

    async fn txn(&self, txn: Txn) -> Result<TxnResult> {
        let req = etcd_client::Txn::new()
            .when(compares(&txn.when)?)
            .and_then(txn_ops(&txn.then))
            .or_else(txn_ops(&txn.otherwise));
        let resp = self.call("txn", self.client.kv_client().txn(req)).await?;
        let results = resp
            .op_responses()
            .iter()
            .map(op_result)
            .collect::<Result<Vec<_>>>()?;
        Ok(TxnResult {
            succeeded: resp.succeeded(),
            revision: header_rev(resp.header()),
            results,
        })
    }

    async fn watch(&self, prefix: &Key, from: Revision) -> Result<WatchStream> {
        let opts = WatchOptions::new()
            .with_range(prefix_end(prefix.as_str().as_bytes()))
            .with_start_revision(to_i64(from)?);
        let stream = self
            .call(
                "watch",
                self.client
                    .watch_client()
                    .watch(prefix.as_str(), Some(opts)),
            )
            .await?;
        let (sender, mut responses) = stream.split();
        // Wait until etcd has created the watcher, so that a watch from 0
        // sees every write after this call returns.
        let mut pending = VecDeque::new();
        let ended = loop {
            let first = self.call("watch", responses.message()).await?;
            let Some(resp) = first else {
                return Err(SprigError::new(
                    SprigCode::MetaUnavailable,
                    "etcd closed the watch before creating it",
                ));
            };
            let ended = watch_events(&resp, &mut pending);
            if resp.created() || resp.canceled() || ended {
                break ended;
            }
        };
        let sender = Arc::new(tokio::sync::Mutex::new(sender));
        lock(&self.shared.watchers).push(Arc::downgrade(&sender));
        let state = WatchState {
            responses,
            _sender: sender,
            pending,
            ended,
        };
        let s = futures::stream::unfold(state, |mut st| async move {
            loop {
                if let Some(item) = st.pending.pop_front() {
                    return Some((item, st));
                }
                if st.ended {
                    return None;
                }
                match st.responses.next().await {
                    Some(Ok(resp)) => st.ended = watch_events(&resp, &mut st.pending),
                    Some(Err(e)) => {
                        st.pending.push_back(Err(map_err("watch", e)));
                        st.ended = true;
                    }
                    None => return None,
                }
            }
        });
        Ok(Box::pin(s))
    }

    async fn grant_lease(&self, ttl: Duration) -> Result<LeaseId> {
        // etcd leases are whole seconds; round up, and etcd raises a TTL
        // below its minimum (about 1.5 election timeouts).
        let secs = ttl.as_secs() + u64::from(ttl.subsec_nanos() > 0);
        let secs = i64::try_from(secs.max(1)).unwrap_or(i64::MAX);
        let resp = self
            .call("grant_lease", self.client.lease_client().grant(secs, None))
            .await?;
        if !resp.error().is_empty() {
            return Err(SprigError::internal(format!(
                "etcd grant_lease: {}",
                resp.error()
            )));
        }
        let id = LeaseId(resp.id());
        let granted = Duration::from_secs(to_u64(resp.ttl()));
        lock(&self.shared.granted).insert(id, granted);
        Ok(id)
    }

    async fn keep_alive(&self, lease: LeaseId) -> Result<Option<Duration>> {
        let slot = self.keeper_slot(lease);
        let mut guard = slot.lock().await;
        // The stream is taken out while a keep-alive is in flight: if the
        // caller gives up (its own deadline, derived from the lease's TTL),
        // the stream is dropped with the future, so a late answer is never
        // read as the answer to a later keep-alive sent at a later time.
        let mut keeper = guard.take();
        // Keep-alives are bounded by their callers (sessions and DC leases
        // retry until their lease's own deadline); the per-request deadline
        // only stops a call that has lost its way (r2next: a slow etcd must
        // not end a lease that is still alive).
        let bound = self.timeout.max(KEEP_ALIVE_BOUND);
        let r = tokio::time::timeout(bound, self.keep_alive_once(lease, &mut keeper)).await;
        match r {
            Ok(Ok(ttl)) if ttl > 0 => {
                *guard = keeper;
                Ok(Some(Duration::from_secs(to_u64(ttl))))
            }
            Ok(Ok(_)) => {
                drop(guard);
                self.forget_lease(lease);
                Ok(None)
            }
            Ok(Err(e)) => Err(map_err("keep_alive", e)),
            Err(_) => Err(timed_out("keep_alive", bound)),
        }
    }

    async fn revoke_lease(&self, lease: LeaseId) -> Result<()> {
        let r = self
            .call("revoke_lease", self.client.lease_client().revoke(lease.0))
            .await;
        self.forget_lease(lease);
        match r {
            Ok(_) => Ok(()),
            // Revoking a lease that already ended is not an error.
            Err(e) if e.message.contains("not found") => Ok(()),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_end_increments_the_last_byte() {
        assert_eq!(prefix_end(b"/sprig/g/"), b"/sprig/g0".to_vec());
        assert_eq!(prefix_end(b"a\xff"), b"b".to_vec());
        assert_eq!(prefix_end(b"\xff\xff"), vec![0]);
    }

    #[test]
    fn seed_sets_compare_without_order_or_trailing_slash() {
        let a = vec!["http://a:2379".to_owned(), "http://b:2379/".to_owned()];
        let b = vec!["http://b:2379".to_owned(), "http://a:2379".to_owned()];
        assert!(same_set(&a, &b));
        assert!(!same_set(&a, &b[..1]));
    }

    #[tokio::test]
    async fn tls_over_http_is_a_config_error() {
        let tls = EtcdTls {
            ca_pem: Vec::new(),
            cert_pem: Vec::new(),
            key_pem: Vec::new(),
            domain: None,
        };
        let cfg = EtcdConfig::new(vec!["http://127.0.0.1:1".to_owned()]).with_tls(tls);
        let err = EtcdStore::connect(Tier::Global, &cfg).await.unwrap_err();
        assert_eq!(err.code(), Some(SprigCode::ConfigInvalid));
        let err = EtcdStore::connect(Tier::Global, &EtcdConfig::new(Vec::new()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Some(SprigCode::ConfigInvalid));
    }

    #[tokio::test]
    async fn missing_tls_files_are_a_config_error() {
        let tls = TlsConfig {
            ca: "/nonexistent/ca.pem".into(),
            cert: "/nonexistent/c.pem".into(),
            key: "/nonexistent/k.pem".into(),
        };
        let err = EtcdTls::from_files(&tls).unwrap_err();
        assert_eq!(err.code(), Some(SprigCode::ConfigInvalid));
    }
}
