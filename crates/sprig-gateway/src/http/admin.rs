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
    /// The Admin API serves HTTPS (chapter 37, "Admin API over TLS").
    #[serde(default)]
    pub admin_tls: bool,
}

/// The TLS configuration every [`AdminApi`] of this process uses for a
/// controller whose Admin API serves HTTPS: `[gateway] controller_ca_file`
/// (else `[tls] ca`), with `[tls] cert` and `key` as the client
/// certificate. Set once at start ([`set_controller_tls`]).
static CONTROLLER_TLS: std::sync::OnceLock<Arc<sprig_common::https::ClientConfig>> =
    std::sync::OnceLock::new();

/// Sets the process's controller TLS configuration (see
/// [`CONTROLLER_TLS`]); a no-op without a CA file.
///
/// # Errors
/// 39998 if the files can't be read or parsed.
pub fn set_controller_tls(
    ca: Option<&std::path::Path>,
    client: Option<(&std::path::Path, &std::path::Path)>,
) -> Result<()> {
    if let Some(ca) = ca {
        let c = sprig_common::https::client_config(ca, client)?;
        let _ = CONTROLLER_TLS.set(c);
    }
    Ok(())
}

/// The request a relayed admin call becomes: only the caller's
/// `authorization` is carried over (whatever else the client sent,
/// including its own `x-sprig-client-addr`, stays behind). With the
/// client's address and this gateway's service secret, both go along so
/// the controller can record the person's address (ST47).
///
/// # Errors
/// Internal for a URI that doesn't parse.
pub fn relay_request(
    url: &str,
    method: &Method,
    authorization: Option<&str>,
    client: Option<std::net::IpAddr>,
    relay_secret: Option<&str>,
    body: Bytes,
) -> Result<Request<Full<Bytes>>> {
    let mut b = Request::builder()
        .method(method)
        .uri(url)
        .header("content-type", "application/json")
        .header("x-sprig-forwarded", "1");
    if let Some(a) = authorization {
        b = b.header("authorization", a);
    }
    if let (Some(ip), Some(secret)) = (client, relay_secret) {
        b = b
            .header(sprig_common::https::CLIENT_ADDR_HEADER, ip.to_string())
            .header(sprig_common::https::RELAY_AUTH_HEADER, secret);
    }
    b.body(Full::new(body))
        .map_err(|e| SprigError::internal("admin request").with_source(e))
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
    /// `[gateway] controller_token_file`: the credential for calls this
    /// gateway makes on its own behalf (decision ST12).
    internal_token: Option<String>,
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
            internal_token: None,
        }
    }

    /// With the internal credential read from `path` (`[gateway]
    /// controller_token_file`).
    ///
    /// # Errors
    /// 39998 if the file can't be read or is empty.
    pub fn with_internal_token_file(mut self, path: Option<&std::path::Path>) -> Result<Self> {
        if let Some(p) = path {
            sprig_common::secret_file::check(p, "[gateway] controller_token_file")?;
            let t = std::fs::read_to_string(p).map_err(|e| {
                SprigError::new(
                    SprigCode::ConfigInvalid,
                    format!("[gateway] controller_token_file {}: {e}", p.display()),
                )
            })?;
            let t = t.trim().to_owned();
            if t.is_empty() {
                return Err(SprigError::new(
                    SprigCode::ConfigInvalid,
                    format!("[gateway] controller_token_file {} is empty", p.display()),
                ));
            }
            self.internal_token = Some(t);
        }
        Ok(self)
    }

    /// Forwards a call this gateway makes on its own behalf (Studio's
    /// views, after the gateway checked the caller), with the internal
    /// credential when one is configured.
    ///
    /// # Errors
    /// As [`AdminApi::forward`].
    pub async fn forward_internal(
        &self,
        method: &Method,
        path_and_query: &str,
        body: Bytes,
    ) -> Result<Reply> {
        let auth = self.internal_token.as_ref().map(|t| format!("Bearer {t}"));
        self.forward(method, path_and_query, auth.as_deref(), body)
            .await
    }

    /// The Admin address of the controller to ask, and whether it serves
    /// HTTPS.
    async fn controller(&self) -> Result<(String, bool)> {
        let store = self.store.as_ref();
        let leader_key = store.tier().key("leader")?;
        if let Some(kv) = store.get(&leader_key, Consistency::Linearizable).await?
            && let Ok(l) = Record::<sprig_meta::LeaderRecord>::decode(&kv.value)
        {
            let key = store
                .tier()
                .key(&format!("controllers/{}", l.body.holder))?;
            if let Some(c) = store.get(&key, Consistency::Linearizable).await? {
                let r = Record::<ControllerRecord>::decode(&c.value)?.body;
                return Ok((r.admin_addr, r.admin_tls));
            }
        }
        let prefix = store.tier().key("controllers/")?;
        let page = store.list(&prefix, ListOpts::default()).await?;
        for kv in &page.kvs {
            if let Ok(r) = Record::<ControllerRecord>::decode(&kv.value) {
                return Ok((r.body.admin_addr, r.body.admin_tls));
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
        self.forward_from(method, path_and_query, authorization, body, None)
            .await
    }

    /// As [`Self::forward`], for a call relayed for a client at `client`:
    /// with `[gateway] controller_token_file` configured, the controller
    /// records `client` as the caller's address (ST47).
    ///
    /// # Errors
    /// As [`Self::forward`].
    pub async fn forward_from(
        &self,
        method: &Method,
        path_and_query: &str,
        authorization: Option<&str>,
        body: Bytes,
        client: Option<std::net::IpAddr>,
    ) -> Result<Reply> {
        let (addr, https) = self.controller().await?;
        let scheme = if https { "https" } else { "http" };
        let req = relay_request(
            &format!("{scheme}://{addr}{path_and_query}"),
            method,
            authorization,
            client,
            self.internal_token.as_deref(),
            body,
        )?;
        let sent = async {
            if https {
                let tls = CONTROLLER_TLS.get().ok_or_else(|| {
                    unavailable(format!(
                        "the controller at {addr} serves HTTPS: set [gateway] controller_ca_file \
                         (or [tls] ca) to the CA that signed its certificate"
                    ))
                })?;
                sprig_common::https::send(tls, req).await.map_err(|e| {
                    unavailable(format!("the controller at {addr} is unreachable: {e}"))
                })
            } else {
                self.client.request(req).await.map_err(|e| {
                    unavailable(format!("the controller at {addr} is unreachable: {e}"))
                })
            }
        };
        let resp = tokio::time::timeout(ADMIN_TIMEOUT, sent)
            .await
            .map_err(|_| {
                unavailable(format!("the controller at {addr} didn't answer in time"))
            })??;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn header<'a>(r: &'a Request<Full<Bytes>>, n: &str) -> Option<&'a str> {
        r.headers().get(n).and_then(|v| v.to_str().ok())
    }

    #[test]
    fn a_relayed_call_carries_the_client_only_with_the_gateways_identity() {
        use sprig_common::https::{CLIENT_ADDR_HEADER, RELAY_AUTH_HEADER};
        let ip: std::net::IpAddr = "203.0.113.7".parse().unwrap();
        let r = relay_request(
            "https://ctl:7480/admin/v1/operators:login",
            &Method::POST,
            None,
            Some(ip),
            Some("gw-secret"),
            Bytes::from_static(b"{}"),
        )
        .unwrap();
        assert_eq!(r.uri().scheme_str(), Some("https"));
        assert_eq!(header(&r, CLIENT_ADDR_HEADER), Some("203.0.113.7"));
        assert_eq!(header(&r, RELAY_AUTH_HEADER), Some("gw-secret"));
        assert_eq!(header(&r, "authorization"), None);
        assert_eq!(header(&r, "x-sprig-forwarded"), Some("1"));

        // Without the gateway's service secret the address isn't sent:
        // the controller would ignore it anyway.
        let r = relay_request(
            "http://ctl:7480/admin/v1/clusters",
            &Method::GET,
            Some("Bearer sprigop_x"),
            Some(ip),
            None,
            Bytes::new(),
        )
        .unwrap();
        assert_eq!(header(&r, CLIENT_ADDR_HEADER), None);
        assert_eq!(header(&r, RELAY_AUTH_HEADER), None);
        assert_eq!(header(&r, "authorization"), Some("Bearer sprigop_x"));

        // The gateway's own calls name no client.
        let r = relay_request(
            "http://ctl:7480/admin/v1/dcs",
            &Method::GET,
            None,
            None,
            Some("gw-secret"),
            Bytes::new(),
        )
        .unwrap();
        assert_eq!(header(&r, CLIENT_ADDR_HEADER), None);
        assert_eq!(header(&r, RELAY_AUTH_HEADER), None);
        // Only these headers exist: nothing a client sent is copied.
        let names: Vec<&str> = r
            .headers()
            .keys()
            .map(hyper::header::HeaderName::as_str)
            .collect();
        assert_eq!(names, ["content-type", "x-sprig-forwarded"]);
    }

    #[test]
    fn the_registration_says_whether_the_admin_api_is_https() {
        let old: ControllerRecord =
            serde_json::from_str(r#"{"admin_addr":"ctl:7480","version":"x"}"#).unwrap();
        assert!(!old.admin_tls);
        let new: ControllerRecord =
            serde_json::from_str(r#"{"admin_addr":"ctl:7480","admin_tls":true}"#).unwrap();
        assert!(new.admin_tls);
    }
}
