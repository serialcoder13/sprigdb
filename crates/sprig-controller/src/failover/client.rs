//! Clients of the internal failover services ([`super::service`]): permits
//! for gateways, agents and workers; `ObserveDc` for the global leader;
//! the observer's probes of another DC.

use std::sync::Arc;
use std::time::Duration;

use hyper::Method;
use sprig_common::ids::DcId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Consistency, MetaStore, Record};

use super::global::ObserverClient;
use super::observe::{DcProbe, ObserveTargets, PeerPing, ProbeRound, mysql_handshake, targets_key};
use super::record::Verdict;
use super::service::ObserveRequest;

// The JSON client and `Permits.Renew`'s client live in `sprig-meta`, so
// gateways and agents use them without depending on the controller.
pub use sprig_meta::permit::http::{HttpPermitSource, JsonHttp};

fn unavailable(msg: String) -> SprigError {
    SprigError::new(SprigCode::MetaUnavailable, msg)
}

async fn targets(global: &dyn MetaStore, dc: &DcId) -> Result<ObserveTargets> {
    Ok(
        match global
            .get(&targets_key(dc)?, Consistency::Linearizable)
            .await?
        {
            Some(kv) => Record::<ObserveTargets>::decode(&kv.value)?.body,
            None => ObserveTargets::default(),
        },
    )
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
        let writers = futures::future::join_all(
            t.writers
                .iter()
                .map(|w| async move { (w.clone(), mysql_handshake(&w.addr, self.timeout).await) }),
        )
        .await;
        ProbeRound { pings, writers }
    }
}
