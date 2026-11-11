//! The Admin API's guard (chapter 14, "Operators"): one layer in front of
//! every `/admin/v1/` route that authenticates the caller, checks their
//! role, DC and token scopes, records them as the actor, and audits every
//! change with their name (chapter 47's `admin.*`).
//!
//! ```text
//! credential                       before the switch     after the switch
//! sprigop_ / sprigsvc_ token       the token's owner     the same
//! a service_token_files secret     that service          the same
//! [controller] gateway_token_file  service "gateway"     the same
//! [controller] admin_token_file    bootstrap (platform)  39310, or break glass
//! (no admin token configured)      bootstrap, loopback   39310
//! [controller] dc_operator_token   DC operator of here   39310
//! ```
//!
//! A DC operator reaches only its DCs' controllers; there it reads
//! everything and changes clusters, mover and other jobs, in-doubt
//! transactions, recommendations (chapter 52 checks the level), its DCs'
//! drains, and settings at `dc` and `cluster` scope. A token's scopes
//! (`<area>:read|write`) narrow its owner's rights. Gateways' service
//! identities pass to the routes, which accept them only where Studio's
//! views call ([`crate::admin::http`]'s `authorize_gateway`).
//!
//! The guard hands the routes the caller in the `x-sprig-authn` header
//! (JSON of [`Principal`]), after removing any copy the request brought.
//!
//! **The client's address (ST47).** Every audit line the guard writes,
//! and the sign-in events, name the client's address. It is the TCP peer,
//! unless the request carries `x-sprig-client-addr` (the address a gateway
//! saw) together with `x-sprig-relay-auth` holding the secret of a
//! configured service identity (`gateway_token_file` or
//! `service_token_files`): only then is the forwarded address believed,
//! and the audit records `via` = that identity. Otherwise the header is
//! ignored. The guard removes both headers and hands the routes the result
//! in `x-sprig-client`.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, header};
use axum::middleware::Next;
use axum::response::Response;
use serde_json::Value;
use sprig_auth::operator as op;
use sprig_common::audit::{self, AuditStream};
use sprig_common::{Result, SprigCode, SprigError};

use super::{Operators, Principal, PrincipalKind, forbidden};
use crate::admin::http::{error_response, unauthenticated};

/// The header that carries the guard's [`Principal`] to the routes.
pub const AUTHN_HEADER: &str = "x-sprig-authn";

/// A relaying gateway's header: the address its client connected from.
pub const CLIENT_ADDR_HEADER: &str = sprig_common::https::CLIENT_ADDR_HEADER;

/// A relaying gateway's header: its service identity's secret, without
/// which [`CLIENT_ADDR_HEADER`] is ignored.
pub const RELAY_AUTH_HEADER: &str = sprig_common::https::RELAY_AUTH_HEADER;

/// The header that carries the client's address the guard established to
/// the routes (sign-in calls record it).
pub const CLIENT_HEADER: &str = "x-sprig-client";

/// Where a call came from (see the module notes).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClientAddr {
    /// The client's IP address: forwarded and vouched for, else the peer.
    pub addr: Option<String>,
    /// The service identity that vouched for a forwarded address.
    pub via: Option<String>,
    /// The TCP peer (the gateway, for a relayed call).
    pub peer: Option<String>,
}

/// The largest request body the guard reads.
const MAX_BODY: usize = 16 * 1024 * 1024;

/// The guard's configuration and directory.
pub struct Guard {
    ops: Arc<Operators>,
    admin_token: Option<String>,
    dc_token: Option<String>,
    services: Vec<(String, String)>,
    break_glass: bool,
}

impl std::fmt::Debug for Guard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Guard")
            .field(
                "services",
                &self.services.iter().map(|s| &s.0).collect::<Vec<_>>(),
            )
            .field("break_glass", &self.break_glass)
            .finish_non_exhaustive()
    }
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim)
}

/// Constant-time equality of a presented secret with a configured one.
fn same(got: &str, want: &str) -> bool {
    let (a, b) = (got.as_bytes(), want.as_bytes());
    let mut diff = u8::from(a.len() != b.len());
    for (i, x) in b.iter().enumerate() {
        diff |= x ^ a.get(i).copied().unwrap_or(0);
    }
    diff == 0
}

fn login_required() -> SprigError {
    SprigError::new(
        SprigCode::OperatorLoginRequired,
        "named operators are in use: log in (sprigctl login) and use your session or a token \
         you issued; the bootstrap token works only under [controller] admin_token_break_glass",
    )
}

impl Guard {
    /// A guard over `ops`: the bootstrap token, the legacy DC-operator
    /// token, configured service identities (name, secret) and break glass.
    #[must_use]
    pub fn new(
        ops: Arc<Operators>,
        admin_token: Option<String>,
        dc_token: Option<String>,
        services: Vec<(String, String)>,
        break_glass: bool,
    ) -> Self {
        Self {
            ops,
            admin_token,
            dc_token,
            services,
            break_glass,
        }
    }

    /// Where a request came from: the address in [`CLIENT_ADDR_HEADER`]
    /// when [`RELAY_AUTH_HEADER`] holds a configured service identity's
    /// secret and the address parses, else the TCP `peer`.
    #[must_use]
    pub fn client_addr(&self, headers: &HeaderMap, peer: Option<SocketAddr>) -> ClientAddr {
        let peer_ip = peer.map(|p| p.ip().to_string());
        let relay = headers
            .get(RELAY_AUTH_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|v| !v.is_empty());
        let vouched = relay.and_then(|r| {
            self.services
                .iter()
                .find(|(_, secret)| same(r, secret))
                .map(|(name, _)| name.clone())
        });
        let forwarded = headers
            .get(CLIENT_ADDR_HEADER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<IpAddr>().ok());
        match (vouched, forwarded) {
            (Some(via), Some(ip)) => ClientAddr {
                addr: Some(ip.to_string()),
                via: Some(via),
                peer: peer_ip,
            },
            _ => ClientAddr {
                addr: peer_ip.clone(),
                via: None,
                peer: peer_ip,
            },
        }
    }

    /// The directory.
    #[must_use]
    pub fn operators(&self) -> &Arc<Operators> {
        &self.ops
    }

    /// The caller of a request.
    ///
    /// # Errors
    /// 39793 for a wrong credential; 39310 for the bootstrap or legacy
    /// credentials (or none) after the switch; 39316 for an ended token.
    pub async fn authenticate(&self, headers: &HeaderMap) -> Result<Principal> {
        let presented = bearer(headers).filter(|b| !b.is_empty());
        if let Some(b) = presented
            && (b.starts_with(op::TOKEN_PREFIX) || b.starts_with(op::SERVICE_PREFIX))
        {
            return self.ops.authenticate(b).await;
        }
        if let Some(b) = presented
            && let Some((name, _)) = self.services.iter().find(|(_, s)| same(s, b))
        {
            return Ok(Principal::gateway_service(name));
        }
        let is_admin = matches!((presented, &self.admin_token), (Some(b), Some(t)) if same(b, t));
        let is_dc = matches!((presented, &self.dc_token), (Some(b), Some(t)) if same(b, t));
        if self.ops.switched().await? {
            if is_admin && self.break_glass {
                return Ok(Principal {
                    kind: PrincipalKind::BreakGlass,
                    name: "breakglass".into(),
                    ..Principal::bootstrap()
                });
            }
            if is_admin || is_dc || presented.is_none() {
                return Err(login_required());
            }
            return Err(unauthenticated());
        }
        if is_dc {
            return Ok(Principal {
                kind: PrincipalKind::LegacyDcOperator,
                name: "dc-token".into(),
                platform: false,
                dcs: vec![self.ops.dc().clone()],
                ..Principal::bootstrap()
            });
        }
        match &self.admin_token {
            None => Ok(Principal::bootstrap()),
            Some(_) if is_admin => Ok(Principal::bootstrap()),
            Some(_) => Err(unauthenticated()),
        }
    }

    /// Whether `p` may make the call; see the module notes.
    ///
    /// # Errors
    /// 39313 with the reason.
    pub fn authorize(
        &self,
        p: &Principal,
        method: &Method,
        path: &str,
        query: Option<&str>,
        body: Option<&Value>,
    ) -> Result<()> {
        let write = !matches!(*method, Method::GET | Method::HEAD);
        if self_service(p, method, path) {
            return Ok(());
        }
        match p.kind {
            PrincipalKind::Bootstrap | PrincipalKind::BreakGlass => return Ok(()),
            PrincipalKind::Service if p.is_gateway() => return Ok(()),
            _ => {}
        }
        let area = op::area_of(path);
        if !p.scopes.is_empty() && !op::scopes_cover(&p.scopes, area, write) {
            return Err(forbidden(format!(
                "this token's scopes ({}) don't allow {} on {area}",
                p.scopes.join(", "),
                if write { "changes" } else { "reads" }
            )));
        }
        if p.platform {
            return Ok(());
        }
        let dc = self.ops.dc();
        if !p.dcs.contains(dc) {
            return Err(forbidden(format!(
                "{} operates {}; this controller serves dc {dc}",
                p.audit_name(),
                p.dcs
                    .iter()
                    .map(|d| format!("dc {d}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        if !write {
            return Ok(());
        }
        let ok = match area {
            "clusters" | "mover" | "jobs" | "txns" | "recommendations" => true,
            "dcs" => path
                .strip_prefix("/admin/v1/dcs/")
                .and_then(|r| r.rsplit_once(':'))
                .is_some_and(|(d, m)| {
                    matches!(m, "drain" | "undrain") && p.dcs.iter().any(|x| x.as_str() == d)
                }),
            "settings" => {
                let scope = body
                    .and_then(|b| b.get("scope"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| query_param(query, "scope"));
                scope.is_some_and(|s| {
                    s == "dc"
                        || s.starts_with("cluster:")
                        || s.strip_prefix("dc:")
                            .is_some_and(|d| p.dcs.iter().any(|x| x.as_str() == d))
                })
            }
            _ => false,
        };
        if ok {
            Ok(())
        } else {
            Err(forbidden(format!(
                "a DC operator can't {method} {path}: platform operators only \
                 (DC operators change clusters, jobs, moves, in-doubt transactions, \
                 recommendations, their DCs' drains and dc or cluster settings)"
            )))
        }
    }
}

/// Calls everyone signed in may make about themselves; the service checks
/// ownership.
fn self_service(p: &Principal, method: &Method, path: &str) -> bool {
    if p.kind != PrincipalKind::Operator {
        return false;
    }
    match (method, path) {
        (&Method::GET, "/admin/v1/operators:whoami")
        | (&Method::POST, "/admin/v1/operators:logout")
        | (&Method::GET | &Method::POST, "/admin/v1/operator-tokens") => true,
        (&Method::POST, p2) if p2.starts_with("/admin/v1/operator-tokens/") => {
            p2.ends_with(":revoke")
        }
        (&Method::POST, p2) => p2 == format!("/admin/v1/operators/{}:setPassword", p.name),
        (&Method::GET, p2) => p2 == format!("/admin/v1/operators/{}", p.name),
        _ => false,
    }
}

fn query_param(query: Option<&str>, name: &str) -> Option<String> {
    query?.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == name).then(|| v.replace("%3A", ":").replace("%3a", ":"))
    })
}

/// Whether a route takes an `actor` the guard should set.
fn takes_actor(method: &Method, path: &str) -> bool {
    let area = op::area_of(path);
    match area {
        "settings" => *method != Method::GET,
        "recommendations" => {
            *method == Method::POST && !path.ends_with(":subscriptions") && !path.ends_with(":gate")
        }
        _ => *method == Method::POST && path.contains("/indexes/"),
    }
}

fn pct(s: &str) -> String {
    s.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                char::from(b).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

fn public(method: &Method, path: &str) -> bool {
    *method == Method::POST
        && matches!(
            path,
            "/admin/v1/operators:login" | "/admin/v1/operators:enrol" | "/admin/v1/traffic/reports"
        )
}

/// The layer: see the module notes.
pub async fn layer(State(g): State<Arc<Guard>>, req: Request, next: Next) -> Response {
    let (mut parts, body) = req.into_parts();
    parts.headers.remove(AUTHN_HEADER);
    let peer = parts
        .extensions
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0);
    let client = g.client_addr(&parts.headers, peer);
    parts.headers.remove(CLIENT_ADDR_HEADER);
    parts.headers.remove(RELAY_AUTH_HEADER);
    parts.headers.remove(CLIENT_HEADER);
    if let Some(a) = &client.addr
        && let Ok(v) = HeaderValue::from_str(a)
    {
        parts.headers.insert(CLIENT_HEADER, v);
    }
    let method = parts.method.clone();
    let path = parts.uri.path().to_owned();
    if !path.starts_with("/admin/v1/") || public(&method, &path) {
        return next.run(Request::from_parts(parts, body)).await;
    }
    let p = match g.authenticate(&parts.headers).await {
        Ok(p) => p,
        Err(e) => return error_response(&e),
    };
    let write = !matches!(method, Method::GET | Method::HEAD);
    let bytes = if write {
        match axum::body::to_bytes(body, MAX_BODY).await {
            Ok(b) => b,
            Err(e) => {
                return error_response(&SprigError::new(
                    SprigCode::AdminRequestInvalid,
                    format!("request body: {e}"),
                ));
            }
        }
    } else {
        Bytes::new()
    };
    let mut json: Option<Value> = if bytes.iter().all(u8::is_ascii_whitespace) {
        None
    } else {
        serde_json::from_slice(&bytes).ok()
    };
    if let Err(e) = g.authorize(&p, &method, &path, parts.uri.query(), json.as_ref()) {
        if write {
            record(
                &g,
                &p,
                &client,
                &method,
                &path,
                403,
                json.as_ref(),
                "admin.denied",
            );
        }
        return error_response(&e);
    }
    let mut bytes = bytes;
    if let Some(actor) = p.actor() {
        if let Some(Value::Object(o)) = &mut json
            && (o.contains_key("actor") || takes_actor(&method, &path))
        {
            o.insert("actor".into(), Value::String(actor.clone()));
            bytes = Bytes::from(serde_json::to_vec(&json).unwrap_or_default());
        }
        if method == Method::DELETE && op::area_of(&path) == "settings" {
            let mut q: Vec<String> = parts
                .uri
                .query()
                .unwrap_or("")
                .split('&')
                .filter(|kv| !kv.is_empty() && !kv.starts_with("actor="))
                .map(str::to_owned)
                .collect();
            q.push(format!("actor={}", pct(&actor)));
            if let Ok(u) = format!("{path}?{}", q.join("&")).parse() {
                parts.uri = u;
            }
        }
    }
    if let Ok(v) = HeaderValue::from_str(&serde_json::to_string(&p).unwrap_or_default()) {
        parts.headers.insert(AUTHN_HEADER, v);
    }
    parts.headers.remove(header::CONTENT_LENGTH);
    let resp = next
        .run(Request::from_parts(parts, Body::from(bytes)))
        .await;
    let status = resp.status().as_u16();
    if p.kind == PrincipalKind::BreakGlass {
        record(
            &g,
            &p,
            &client,
            &method,
            &path,
            status,
            json.as_ref(),
            "breakglass.admin_token",
        );
    }
    if write {
        let event = format!("admin.{}", op::area_of(&path).replace('-', "_"));
        record(
            &g,
            &p,
            &client,
            &method,
            &path,
            status,
            json.as_ref(),
            &event,
        );
    }
    resp
}

#[allow(clippy::too_many_arguments)] // one audit line's fields
fn record(
    g: &Guard,
    p: &Principal,
    client: &ClientAddr,
    method: &Method,
    path: &str,
    status: u16,
    body: Option<&Value>,
    event: &str,
) {
    let reason = body
        .and_then(|b| b.get("reason"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let fields = serde_json::json!({
        "actor": p.audit_name(),
        "actor_kind": p.kind,
        "key_id": p.key_id,
        "owner": p.owner,
        "method": method.as_str(),
        "path": path,
        "status": status,
        "reason": reason,
        "dc": g.ops.dc(),
        "client_addr": client.addr,
        "via": client.via,
    });
    tracing::info!(target: "sprig_audit", event, actor = %p.audit_name(), method = %method,
        path, status, reason, client_addr = client.addr.as_deref().unwrap_or(""),
        via = client.via.as_deref().unwrap_or(""), "admin call");
    audit::record(AuditStream::Platform, event, fields);
}
