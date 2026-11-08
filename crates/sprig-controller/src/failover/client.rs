//! Clients of the internal failover services ([`super::service`]): permits
//! for gateways, agents and workers; `ObserveDc` for the global leader;
//! the observer's probes of another DC.

use std::sync::Arc;
use std::time::Duration;

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Method, Request, header};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sprig_common::ids::DcId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::permit::{Permit, PermitRequest, PermitSource};
use sprig_meta::{Consistency, MetaStore, Record};

use super::global::ObserverClient;
use super::observe::{DcProbe, ObserveTargets, PeerPing, ProbeRound, mysql_handshake, targets_key};
use super::record::Verdict;
use super::service::ObserveRequest;

/// JSON over HTTP/1.1.
#[derive(Debug, Clone)]
pub struct JsonHttp {
    http: Client<HttpConnector, Full<Bytes>>,
    timeout: Duration,
}

fn unavailable(msg: String) -> SprigError {
    SprigError::new(SprigCode::MetaUnavailable, msg)
}

impl JsonHttp {
    /// A client whose calls time out after `timeout`.
    #[must_use]
    pub fn new(timeout: Duration) -> Self {
        let mut c = HttpConnector::new();
        c.set_connect_timeout(Some(timeout));
        c.set_nodelay(true);
        Self {
            http: Client::builder(TokioExecutor::new())
                .pool_idle_timeout(Duration::from_secs(30))
                .build(c),
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
                return Err(unavailable(format!("{url}: HTTP {status}")));
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
    /// A source at `base`.
    #[must_use]
    pub fn new(base: &str, timeout: Duration) -> Self {
        Self {
            base: base.trim_end_matches('/').to_owned(),
            http: JsonHttp::new(timeout),
        }
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

async fn targets(global: &dyn MetaStore, dc: &DcId) -> Result<ObserveTargets> {
    Ok(match global.get(&targets_key(dc)?, Consistency::Linearizable).await? {
        Some(kv) => Record::<ObserveTargets>::decode(&kv.value)?.body,
        None => ObserveTargets::default(),
    })
}

/// Asks a site's controllers (from `dc-observe/<site>`) for a verdict.
#[derive(Clone)]
pub struct HttpObserverClient {
    global: Arc<dyn MetaStore>,
    http: JsonHttp,
}

impl std::fmt::Debug for HttpObserverClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpObserverClient").finish_non_exhaustive()
    }
}

impl HttpObserverClient {
    /// A client reading targets from the global tier.
    #[must_use]
    pub fn new(global: Arc<dyn MetaStore>, timeout: Duration) -> Self {
        Self {
            global,
            http: JsonHttp::new(timeout),
        }
    }
}

#[async_trait::async_trait]
impl ObserverClient for HttpObserverClient {
    async fn observe(&self, site: &DcId, x: &DcId, failover_id: &str) -> Result<Verdict> {
        let t = targets(&*self.global, site).await?;
        let req = ObserveRequest {
            dc_id: x.clone(),
            failover_id: failover_id.to_owned(),
        };
        let mut last = unavailable(format!("{site} lists no controllers"));
        for c in &t.controllers {
            let url = format!("{}/internal/v1/observer/observe", c.trim_end_matches('/'));
            match self.http.call(Method::POST, &url, Some(&req)).await {
                Ok(v) => return Ok(v),
                Err(e) => last = e,
            }
        }
        Err(last)
    }
}

/// The observer's probes: `Peer.Ping` to the DC's controllers, a MySQL
/// handshake to its writers.
#[derive(Clone)]
pub struct HttpDcProbe {
    global: Arc<dyn MetaStore>,
    http: JsonHttp,
    timeout: Duration,
}

impl std::fmt::Debug for HttpDcProbe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpDcProbe").finish_non_exhaustive()
    }
}

impl HttpDcProbe {
    /// Probes with `timeout` per call, reading targets from the global tier.
    #[must_use]
    pub fn new(global: Arc<dyn MetaStore>, timeout: Duration) -> Self {
        Self {
            global,
            http: JsonHttp::new(timeout),
            timeout,
        }
    }
}

#[async_trait::async_trait]
impl DcProbe for HttpDcProbe {
    async fn probe(&self, dc: &DcId) -> ProbeRound {
        let Ok(t) = targets(&*self.global, dc).await else {
            return ProbeRound::default();
        };
        let pings = futures::future::join_all(t.controllers.iter().map(|c| {
            let url = format!("{}/internal/v1/peer/ping", c.trim_end_matches('/'));
            async move {
                self.http
                    .call::<(), PeerPing>(Method::GET, &url, None)
                    .await
                    .ok()
            }
        }))
        .await
        .into_iter()
        .flatten()
        .collect();
        let writers = futures::future::join_all(t.writers.iter().map(|w| async move {
            (w.clone(), mysql_handshake(&w.addr, self.timeout).await)
        }))
        .await;
        ProbeRound { pings, writers }
    }
}
