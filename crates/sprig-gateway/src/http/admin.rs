//! The Admin API on the gateways (chapter 37 step 4).
//!
//! Every gateway serves `/admin/v1/` on 8080 and 8443, over REST and gRPC.
//! The controllers implement it: a gateway forwards each call to the DC
//! leader's controller, which answers writes (reads too, so a caller sees
//! one consistent leader). The controller's address comes from its
//! registration, `/sprig/dc/<dc>/controllers/<node>`, under the holder
//! named by `/sprig/dc/<dc>/leader`; with no leader, any registered
//! controller answers (its writes then fail with 39504).
//!
//! `Gateways` is the gateways' own service: `ListGateways` and `GetGateway`
//! read the registration records, and `DrainGateway` drains this gateway or
//! asks the named one through the peer service. These calls are authorized
//! by the controller: the caller's `authorization` must be accepted by
//! `GET /admin/v1/dcs` there.
//!
//! **Where the book differs.** Chapter 37 says this step retires the
//! controller's interim listener on 7480. It stays, as the controllers'
//! internal Admin port that gateways forward to (bind it to a private
//! address); clients use the gateways.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde::Deserialize;
use serde_json::json;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Consistency, ListOpts, MetaStore, Record, Versioned};

use crate::registry;

/// How long a forwarded admin call may take (tenant creation waits for the
/// bootstrap).
pub const ADMIN_TIMEOUT: Duration = Duration::from_secs(120);

/// `controllers/<node>`: where a controller serves its Admin port. Written
/// by the controller under its session (sprig-controller).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Deserialize)]
pub struct ControllerRecord {
    /// `host:port` of the controller's Admin API.
    pub admin_addr: String,
}

impl Versioned for ControllerRecord {
    const V: u32 = 1;
}

/// A reply to pass back: status and JSON body.
#[derive(Debug, Clone)]
pub struct Reply {
    /// HTTP status.
    pub status: u16,
    /// The body (JSON).
    pub body: Bytes,
}

/// Starts this gateway's drain; `false` if it was already draining.
pub type DrainFn = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// The gateway's side of the Admin API.
pub struct AdminApi {
    store: Arc<dyn MetaStore>,
    client: Client<HttpConnector, Full<Bytes>>,
    gateway_id: String,
    peers: Option<Arc<crate::peer::Peers>>,
    drain: DrainFn,
}

impl std::fmt::Debug for AdminApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminApi")
            .field("gateway_id", &self.gateway_id)
            .finish_non_exhaustive()
    }
}

fn unavailable(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::NotLeader, msg)
}

impl AdminApi {
    /// Forwards to the controllers registered in `store` (this DC's tier).
    #[must_use]
    pub fn new(
        store: Arc<dyn MetaStore>,
        gateway_id: String,
        peers: Option<Arc<crate::peer::Peers>>,
        drain: DrainFn,
    ) -> Self {
        let mut http = HttpConnector::new();
        http.set_connect_timeout(Some(Duration::from_secs(5)));
        Self {
            store,
            client: Client::builder(TokioExecutor::new()).build(http),
            gateway_id,
            peers,
            drain,
        }
    }

    /// The Admin address of the controller to ask.
    async fn controller(&self) -> Result<String> {
        let store = self.store.as_ref();
        let leader_key = store.tier().key("leader")?;
        if let Some(kv) = store.get(&leader_key, Consistency::Linearizable).await?
            && let Ok(l) = Record::<sprig_meta::LeaderRecord>::decode(&kv.value)
        {
            let key = store
                .tier()
                .key(&format!("controllers/{}", l.body.holder))?;
            if let Some(c) = store.get(&key, Consistency::Linearizable).await? {
                return Ok(Record::<ControllerRecord>::decode(&c.value)?
                    .body
                    .admin_addr);
            }
        }
        let prefix = store.tier().key("controllers/")?;
        let page = store.list(&prefix, ListOpts::default()).await?;
        for kv in &page.kvs {
            if let Ok(r) = Record::<ControllerRecord>::decode(&kv.value) {
                return Ok(r.body.admin_addr);
            }
        }
        Err(unavailable(
            "no controller of this DC is registered: the Admin API is unavailable",
        ))
    }

    /// Forwards one REST call (`path_and_query` under `/admin/v1/`) to the
    /// controller.
    ///
    /// # Errors
    /// 39504 when no controller is reachable.
    pub async fn forward(
        &self,
        method: &Method,
        path_and_query: &str,
        authorization: Option<&str>,
        body: Bytes,
    ) -> Result<Reply> {
        let addr = self.controller().await?;
        let mut b = Request::builder()
            .method(method)
            .uri(format!("http://{addr}{path_and_query}"))
            .header("content-type", "application/json")
            .header("x-sprig-forwarded", "1");
        if let Some(a) = authorization {
            b = b.header("authorization", a);
        }
        let req = b
            .body(Full::new(body))
            .map_err(|e| SprigError::internal("admin request").with_source(e))?;
        let resp = tokio::time::timeout(ADMIN_TIMEOUT, self.client.request(req))
            .await
            .map_err(|_| unavailable(format!("the controller at {addr} didn't answer in time")))?
            .map_err(|e| unavailable(format!("the controller at {addr} is unreachable: {e}")))?;
        let status = resp.status().as_u16();
        let body = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| unavailable(format!("the controller's reply broke off: {e}")))?
            .to_bytes();
        Ok(Reply { status, body })
    }

    /// Whether the controller accepts `authorization` for admin calls.
    async fn authorized(&self, authorization: Option<&str>) -> Result<()> {
        let r = self
            .forward(&Method::GET, "/admin/v1/dcs", authorization, Bytes::new())
            .await?;
        if r.status == StatusCode::UNAUTHORIZED.as_u16() {
            return Err(SprigError::new(
                SprigCode::AdminUnauthenticated,
                "the Admin API needs a valid credential",
            ));
        }
        Ok(())
    }

    /// `Gateways.ListGateways`, `GetGateway` and `DrainGateway`.
    ///
    /// # Errors
    /// 39793 without valid credentials; 39792 for an unknown gateway; 39788
    /// when the named gateway can't be reached.
    pub async fn gateways(
        &self,
        rpc: &str,
        name: Option<&str>,
        authorization: Option<&str>,
    ) -> Result<serde_json::Value> {
        self.authorized(authorization).await?;
        match (rpc, name) {
            ("ListGateways", _) => {
                let all = registry::list(self.store.as_ref()).await?;
                Ok(json!({ "gateways": all }))
            }
            ("GetGateway", Some(id)) => {
                let g = registry::get(self.store.as_ref(), id)
                    .await?
                    .ok_or_else(|| {
                        SprigError::new(SprigCode::AdminRequestInvalid, format!("no gateway {id}"))
                    })?;
                Ok(serde_json::to_value(g).unwrap_or_default())
            }
            ("DrainGateway", Some(id)) => {
                let started = if id == self.gateway_id {
                    (self.drain)("admin")
                } else {
                    let peers = self.peers.as_ref().ok_or_else(|| {
                        SprigError::new(
                            SprigCode::ForwardFailed,
                            "this gateway has no peer service to reach other gateways",
                        )
                    })?;
                    peers.drain(id, "admin").await?
                };
                Ok(json!({ "gateway_id": id, "started": started }))
            }
            _ => Err(SprigError::new(
                SprigCode::AdminRequestInvalid,
                format!("unknown Gateways call {rpc}"),
            )),
        }
    }
}
