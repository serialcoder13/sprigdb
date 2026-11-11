//! Permits over JSON/HTTP, and the renew loop every holder runs (chapter
//! 25, "DC leases, sub-leases and self-fencing" step 3; chapter 23,
//! "Internal services").
//!
//! **Where it differs from the book.** `Permits.Renew` is
//! `POST /internal/v1/permits/renew` with the book's messages as JSON on
//! each controller's `[controller] internal_addr`, until the mTLS gRPC
//! port 7400 exists. Holders find the controllers of their DC in the DC
//! tier's `controllers/<node>` registrations (`internal_addr`).
//!
//! **Arming.** A [`PermitKeeper`] enforces permits only once it has seen a
//! controller of its DC that serves them. Until then (a deployment whose
//! controllers predate R2) writes flow as before. Once armed it stays
//! armed for the life of the process: losing every controller then fences
//! the holder, as the book says.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Method, Request, header};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sprig_common::https::InternalConnector;
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};

use super::{Permit, PermitHolder, PermitRequest, PermitSource};
use crate::{ListOpts, MetaStore};

/// How often a keeper reads the DC's controller registrations again.
pub const DISCOVERY_PERIOD: Duration = Duration::from_secs(5);

/// JSON over HTTP/1.1, with pooled keep-alive connections: plain to
/// `http://` addresses, mutual TLS with this process's `[tls]` identity
/// to `https://` ones (`[controller] internal_tls`; hardening round 3).
#[derive(Debug, Clone)]
pub struct JsonHttp {
    http: Client<InternalConnector, Full<Bytes>>,
    timeout: Duration,
}

fn unavailable(msg: String) -> SprigError {
    SprigError::new(SprigCode::MetaUnavailable, msg)
}

impl JsonHttp {
    /// A client whose calls time out after `timeout`.
    #[must_use]
    pub fn new(timeout: Duration) -> Self {
        Self {
            http: Client::builder(TokioExecutor::new())
                .pool_idle_timeout(Duration::from_secs(30))
                .build(InternalConnector::new(timeout)),
            timeout,
        }
    }

    /// One call; `body` is sent as JSON when given.
    ///
    /// # Errors
    /// 39500 for a transport failure, a timeout or a non-2xx answer.
    pub async fn call<B: Serialize + Sync, T: DeserializeOwned>(
        &self,
        method: Method,
        url: &str,
        body: Option<&B>,
    ) -> Result<T> {
        let bytes = match body {
            Some(b) => serde_json::to_vec(b).map_err(|e| SprigError::internal(e.to_string()))?,
            None => Vec::new(),
        };
        let req = Request::builder()
            .method(method)
            .uri(url)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from(bytes)))
            .map_err(|e| SprigError::internal(format!("{url}: {e}")))?;
        let go = async {
            let resp = self
                .http
                .request(req)
                .await
                .map_err(|e| unavailable(format!("{url}: {e}")))?;
            let status = resp.status();
            let body = resp
                .into_body()
                .collect()
                .await
                .map_err(|e| unavailable(format!("{url}: {e}")))?
                .to_bytes();
            if !status.is_success() {
                return Err(unavailable(format!(
                    "{url}: HTTP {status}: {}",
                    String::from_utf8_lossy(&body)
                )));
            }
            serde_json::from_slice(&body).map_err(|e| unavailable(format!("{url}: {e}")))
        };
        tokio::time::timeout(self.timeout, go)
            .await
            .map_err(|_| unavailable(format!("{url}: no answer within {:?}", self.timeout)))?
    }
}

/// `Permits.Renew` on one controller (`http://host:port`).
#[derive(Debug, Clone)]
pub struct HttpPermitSource {
    base: String,
    http: JsonHttp,
}

impl HttpPermitSource {
    /// A source at `base` (`http://host:port`, or just `host:port`).
    #[must_use]
    pub fn new(base: &str, timeout: Duration) -> Self {
        Self::with_client(base, JsonHttp::new(timeout))
    }

    /// A source at `base` sharing `http`'s connections.
    #[must_use]
    pub fn with_client(base: &str, http: JsonHttp) -> Self {
        let base = base.trim_end_matches('/');
        let base = if base.contains("://") {
            base.to_owned()
        } else {
            format!("http://{base}")
        };
        Self { base, http }
    }
}

#[async_trait::async_trait]
impl PermitSource for HttpPermitSource {
    async fn renew(&self, req: &PermitRequest) -> Result<Permit> {
        self.http
            .call(
                Method::POST,
                &format!("{}/internal/v1/permits/renew", self.base),
                Some(req),
            )
            .await
    }
}

/// The internal addresses the DC's controllers registered
/// (`controllers/<node>` with an `internal_addr`), sorted: `host:port`,
/// or `https://host:port` for a controller that serves them over TLS
/// (`"internal_tls": true`).
///
/// # Errors
/// The store's errors.
pub async fn internal_addrs(dc: &dyn MetaStore) -> Result<Vec<String>> {
    let prefix = dc.tier().key("controllers/")?;
    let page = dc.list(&prefix, ListOpts::default()).await?;
    let mut out: Vec<String> = page
        .kvs
        .iter()
        .filter_map(|kv| {
            let v: serde_json::Value = serde_json::from_slice(&kv.value).ok()?;
            let a = v.get("internal_addr")?.as_str()?;
            let tls = v.get("internal_tls").and_then(serde_json::Value::as_bool) == Some(true);
            Some(if tls && !a.is_empty() && !a.contains("://") {
                format!("https://{a}")
            } else {
                a.to_owned()
            })
        })
        .filter(|a| !a.is_empty())
        .collect();
    out.sort();
    out.dedup();
    Ok(out)
}

/// A holder's permit, kept by a renew loop over the DC's controllers (see
/// the module docs).
pub struct PermitKeeper {
    holder: Arc<PermitHolder>,
    armed: AtomicBool,
    heartbeat: Arc<AtomicU64>,
    sources: Mutex<Vec<String>>,
}

impl std::fmt::Debug for PermitKeeper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PermitKeeper")
            .field("holder", &self.holder)
            .field("armed", &self.armed())
            .finish_non_exhaustive()
    }
}

impl PermitKeeper {
    /// A keeper of `holder`, not armed yet.
    #[must_use]
    pub fn new(holder: Arc<PermitHolder>) -> Self {
        Self {
            holder,
            armed: AtomicBool::new(false),
            heartbeat: Arc::new(AtomicU64::new(0)),
            sources: Mutex::new(Vec::new()),
        }
    }

    /// The holder.
    #[must_use]
    pub fn holder(&self) -> &Arc<PermitHolder> {
        &self.holder
    }

    /// Whether permits are enforced (a controller serving them was seen).
    #[must_use]
    pub fn armed(&self) -> bool {
        self.armed.load(Ordering::SeqCst)
    }

    /// Arms the keeper (tests, and roles told to enforce from the start).
    pub fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    /// Whether the holder may write now: not armed, or a valid permit.
    #[must_use]
    pub fn may_write(&self) -> bool {
        !self.armed() || self.holder.writable()
    }

    /// The controllers last discovered.
    #[must_use]
    pub fn sources(&self) -> Vec<String> {
        self.sources
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// A counter the runtime's heartbeat task raises every few
    /// milliseconds: what a [`super::Watchdog`] watches, so a slow renewal
    /// never looks like a stuck process.
    #[must_use]
    pub fn heartbeat(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.heartbeat)
    }

    /// Renews every `period` from the controllers registered in `dc`
    /// (read again every [`DISCOVERY_PERIOD`]; the last list is kept while
    /// the store is unreachable), until `stop`. Also raises the heartbeat
    /// every `period / 10`.
    pub async fn run(
        self: Arc<Self>,
        dc: Arc<dyn MetaStore>,
        period: Duration,
        stop: CancellationToken,
    ) {
        let beat = Arc::clone(&self.heartbeat);
        let beat_stop = stop.clone();
        let tick = (period / 10).max(Duration::from_millis(10));
        let heartbeat = tokio::spawn(async move {
            while !beat_stop.is_cancelled() {
                beat.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(tick).await;
            }
        });
        let http = JsonHttp::new(period.min(Duration::from_millis(800)));
        let mut cache: HashMap<String, Arc<dyn PermitSource>> = HashMap::new();
        let mut discovered: Option<Instant> = None;
        while !stop.is_cancelled() {
            if discovered.is_none_or(|t| t.elapsed() >= DISCOVERY_PERIOD) {
                match tokio::time::timeout(period, internal_addrs(&*dc)).await {
                    Ok(Ok(addrs)) => {
                        discovered = Some(Instant::now());
                        if !addrs.is_empty() {
                            if !self.armed.swap(true, Ordering::SeqCst) {
                                tracing::info!(controllers = ?addrs, role = %self.holder.req.role, "permits armed: writes need a valid DC permit from now on");
                            }
                            *self.sources.lock().unwrap_or_else(PoisonError::into_inner) = addrs;
                        }
                    }
                    Ok(Err(e)) => tracing::debug!(error = %e, "can't list the DC's controllers"),
                    Err(_) => tracing::debug!("listing the DC's controllers timed out"),
                }
            }
            let addrs = self.sources();
            let sources: Vec<Arc<dyn PermitSource>> = addrs
                .iter()
                .map(|a| {
                    Arc::clone(cache.entry(a.clone()).or_insert_with(|| {
                        Arc::new(HttpPermitSource::with_client(a, http.clone()))
                    }))
                })
                .collect();
            if !sources.is_empty() {
                self.holder.renew_once(&sources, period).await;
            }
            tokio::select! {
                () = stop.cancelled() => break,
                () = tokio::time::sleep(period) => {}
            }
        }
        heartbeat.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemStore, Op, Tier, Txn};
    use sprig_common::ids::DcId;
    use sprig_common::time::SystemClock;

    #[tokio::test]
    async fn a_controller_serving_tls_is_called_with_https() {
        let mem: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(DcId::new("fra1").unwrap())));
        for (node, v) in [
            ("c1", &br#"{"v":1,"admin_addr":"x","internal_addr":"10.0.0.11:7481","internal_tls":true}"#[..]),
            ("c2", &br#"{"v":1,"admin_addr":"x","internal_addr":"10.0.0.12:7481"}"#[..]),
            ("c3", &br#"{"v":1,"admin_addr":"x","internal_addr":"10.0.0.13:7481","internal_tls":false}"#[..]),
        ] {
            let key = mem.tier().key(&format!("controllers/{node}")).unwrap();
            mem.txn(Txn {
                when: vec![],
                then: vec![Op::Put {
                    key,
                    value: v.to_vec(),
                    lease: None,
                }],
                otherwise: vec![],
            })
            .await
            .unwrap();
        }
        assert_eq!(
            internal_addrs(&*mem).await.unwrap(),
            vec![
                "10.0.0.12:7481".to_owned(),
                "10.0.0.13:7481".to_owned(),
                "https://10.0.0.11:7481".to_owned(),
            ]
        );
        // A plain address gets http://; an https:// one keeps its scheme.
        let h = JsonHttp::new(Duration::from_millis(100));
        assert_eq!(
            HttpPermitSource::with_client("10.0.0.12:7481", h.clone()).base,
            "http://10.0.0.12:7481"
        );
        assert_eq!(
            HttpPermitSource::with_client("https://10.0.0.11:7481", h).base,
            "https://10.0.0.11:7481"
        );
    }

    #[tokio::test]
    async fn keeper_arms_on_a_registered_controller_and_fences_without_answers() {
        let mem: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(DcId::new("fra1").unwrap())));
        let holder = Arc::new(PermitHolder::new("gw-1", "gateway", Arc::new(SystemClock)));
        let k = Arc::new(PermitKeeper::new(Arc::clone(&holder)));
        assert!(k.may_write(), "not armed: writes flow as before R2");
        // A controller registers an address nobody answers on.
        let key = mem.tier().key("controllers/c1").unwrap();
        let v = br#"{"v":1,"admin_addr":"x","internal_addr":"127.0.0.1:9"}"#.to_vec();
        mem.txn(Txn {
            when: vec![],
            then: vec![Op::Put {
                key,
                value: v,
                lease: None,
            }],
            otherwise: vec![],
        })
        .await
        .unwrap();
        assert_eq!(
            internal_addrs(&*mem).await.unwrap(),
            vec!["127.0.0.1:9".to_owned()]
        );
        let stop = CancellationToken::new();
        let run = tokio::spawn(Arc::clone(&k).run(
            Arc::clone(&mem),
            Duration::from_millis(50),
            stop.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(k.armed());
        assert!(!k.may_write(), "armed without a permit: fenced");
        assert!(k.heartbeat().load(Ordering::SeqCst) > 0);
        stop.cancel();
        let _ = run.await;
    }
}
