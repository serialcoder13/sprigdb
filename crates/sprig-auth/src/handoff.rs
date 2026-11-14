//! Credential residency: handing a login to an allowed DC.
//!
//! Gateways outside a pinned tenant's allowed DCs never load its
//! credentials. They hand the login to a gateway in the nearest allowed DC
//! and relay until a result arrives; the target decides with its own
//! [`LoginService`] (credentials, policy order, counters, audit) and
//! returns the principal's fields only.
//!
//! ```text
//! route(tenant's allowed DCs, this DC, residency.deny_remote_access):
//!   no pin, or this DC allowed   → Local
//!   deny_remote_access           → Deny (39302 after the exchange)
//!   else                         → HandOff(allowed DCs, nearest first)
//! relay (RelayLogin, the source side):
//!   fast path: never (the scramble can't be checked without the cache, and
//!     the nonce is the source's), so the client is asked for its password
//!     in clear text over TLS (the full path; plaintext clients get the TLS
//!     hint, as for any full authentication without TLS)
//!   check_password → HandoffTarget::login(dc, start, password) for each DC in
//!     order until one answers; the answer is the verdict
//! serve (serve_handoff, the target side):
//!   LoginService::start(the request) → clear-text fast path or full check
//!   → decide → HandoffResult { principal fields | the refusal packet }
//! ```
//!
//! **Transport.** This runs over `GatewayPeer.HandoffLogin` on port 7403
//! with mutual TLS: the gateway's `PeerHandoff`
//! implements [`HandoffTarget`] over it, and [`LocalPeers`] (tests: DCs as
//! in-process services) in process. The messages ([`HandoffStart`],
//! [`PrincipalWire`]) are serde types carried as JSON in the gRPC
//! messages. A handoff costs one WAN round trip: the whole exchange
//! happens at the source, and only the decision crosses.
//!
//! **Wiring.** [`LoginService::begin`] (and
//! [`LoginService::begin_http`]) read the tenant's residency from the
//! directory *before* anything of its credentials is loaded, and pick the
//! flow: the local [`crate::MysqlLogin`], [`RelayLogin`], or [`DenyLogin`]
//! (39302 after the exchange). A residency block whose allowed set is
//! empty or unreadable allows no DC: such logins are refused with 39302
//! (fail closed). The target re-checks that it is an allowed DC before
//! deciding ([`serve_handoff`]), so a misrouted handoff never loads
//! verifiers outside the allowed DCs.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sprig_common::ids::DcId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::auth::{AuthPath, AuthVerdict, LoginFlow, Method, Outcome};
use sprig_proto_mysql::handshake::NONCE_LEN;
use sprig_proto_mysql::{ErrPacket, Persona};
use zeroize::Zeroizing;

use crate::accounts::BackendTarget;
use crate::audit::{self, AuditEvent};
use crate::mysql::{LoginRequest, LoginService, MysqlLogin, Principal, max_user_len};
use crate::store::AccountLimits;

/// Where a login is decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// Here.
    Local,
    /// In one of these DCs, nearest first.
    HandOff(Vec<DcId>),
    /// Nowhere: `residency.deny_remote_access` (39302).
    Deny,
}

/// The route of a login for a tenant pinned to `allowed` (nearest first;
/// `None` or empty: not pinned) arriving in `here`.
#[must_use]
pub fn route(allowed: Option<&[DcId]>, here: &DcId, deny_remote_access: bool) -> Route {
    match allowed {
        None | Some([]) => Route::Local,
        Some(a) if a.contains(here) => Route::Local,
        Some(_) if deny_remote_access => Route::Deny,
        Some(a) => Route::HandOff(a.to_vec()),
    }
}

/// The start of a handed-off login: the request's fields.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HandoffStart {
    /// `mysql` or `mariadb`.
    pub persona: String,
    /// The login name as sent.
    pub login_name: Vec<u8>,
    /// The TLS SNI.
    pub sni: Option<String>,
    /// The client address.
    pub client_ip: IpAddr,
    /// TLS at the source gateway.
    pub tls: bool,
    /// A verified client certificate at the source.
    pub client_cert: bool,
    /// Its subject and issuer (`REQUIRE SUBJECT`, `ISSUER`).
    /// Absent from a source that predates them, which then hands off as
    /// it did: such accounts are refused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_subject: Option<String>,
    /// See [`Self::client_subject`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_issuer: Option<String>,
    /// `CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS`.
    pub can_handle_expired_password: bool,
    /// The source DC, for the target's audit event.
    pub source_dc: DcId,
}

impl std::fmt::Debug for HandoffStart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandoffStart")
            .field("login_name", &String::from_utf8_lossy(&self.login_name))
            .field("client_ip", &self.client_ip)
            .field("source_dc", &self.source_dc)
            .finish_non_exhaustive()
    }
}

impl HandoffStart {
    fn request(&self) -> LoginRequest {
        LoginRequest {
            persona: if self.persona == "mariadb" {
                Persona::MariaDb
            } else {
                Persona::MySql84
            },
            login_name: self.login_name.clone(),
            sni: self.sni.clone(),
            client_ip: self.client_ip,
            tls: self.tls,
            client_cert: self.client_cert,
            client_dn: match (&self.client_subject, &self.client_issuer) {
                (Some(subject), Some(issuer)) => Some(crate::mysql::ClientDn {
                    subject: subject.clone(),
                    issuer: issuer.clone(),
                }),
                _ => None,
            },
            can_handle_expired_password: self.can_handle_expired_password,
        }
    }

    fn of(req: &LoginRequest, source_dc: DcId) -> Self {
        Self {
            persona: match req.persona {
                Persona::MySql84 => "mysql".into(),
                Persona::MariaDb => "mariadb".into(),
            },
            login_name: req.login_name.clone(),
            sni: req.sni.clone(),
            client_ip: req.client_ip,
            tls: req.tls,
            client_cert: req.client_cert,
            client_subject: req.client_dn.as_ref().map(|d| d.subject.clone()),
            client_issuer: req.client_dn.as_ref().map(|d| d.issuer.clone()),
            can_handle_expired_password: req.can_handle_expired_password,
            source_dc,
        }
    }
}

/// The target's decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandoffResult {
    /// Accepted: the principal's fields.
    Accepted(Box<Principal>),
    /// Refused with this packet.
    Refused(ErrPacket),
}

/// Reaches gateways of other DCs (the gateway's `GatewayPeer` service).
#[async_trait]
pub trait HandoffTarget: Send + Sync + 'static {
    /// Decides the login `start` with `password` in DC `dc`.
    ///
    /// # Errors
    /// The DC can't be reached; the source then tries the next.
    async fn login(
        &self,
        dc: &DcId,
        start: HandoffStart,
        password: Zeroizing<Vec<u8>>,
    ) -> Result<HandoffResult>;
}

/// The target side: decides a handed-off login with `svc`. The target
/// audits it (`auth.handoff`, plus the login's own event) and counts
/// failures. A target outside the tenant's allowed DCs refuses with 39301
/// without loading anything of its credentials.
pub async fn serve_handoff(
    svc: &Arc<LoginService>,
    start: HandoffStart,
    password: &[u8],
) -> HandoffResult {
    let req = start.request();
    if !matches!(svc.route_of(&req, false).await, Route::Local) {
        metrics::counter!("sprig_auth_handoffs_total", "outcome" => "misrouted").increment(1);
        return HandoffResult::Refused(ErrPacket::from_error(
            req.persona,
            &SprigError::new(
                SprigCode::AuthUnavailable,
                "this data center isn't allowed to decide the tenant's logins",
            ),
        ));
    }
    let mut login = svc.start(req).await;
    let (outcome, path) = if login.check_clear_fast(password) {
        (Outcome::Verified, AuthPath::Fast)
    } else if login
        .check_password(Zeroizing::new(password.to_vec()))
        .await
    {
        (Outcome::Verified, AuthPath::Full)
    } else {
        (Outcome::WrongCredentials, AuthPath::Full)
    };
    let verdict = login.decide(outcome, path).await;
    let mut e = AuditEvent::new(audit::events::HANDOFF, "mysql", svc.clock().wall());
    e.client_ip = Some(start.client_ip);
    e.user = Some(String::from_utf8_lossy(&start.login_name).into_owned());
    e.detail = Some(format!("from {}", start.source_dc));
    match verdict {
        AuthVerdict::Accept => match login.principal().cloned() {
            Some(p) => {
                e.tenant = p.tenant;
                e.tenant_name.clone_from(&p.tenant_name);
                e.user_id = p.user_id;
                svc.audit().emit(&e);
                HandoffResult::Accepted(Box::new(p))
            }
            None => HandoffResult::Refused(ErrPacket::from_error(
                Persona::MySql84,
                &SprigError::internal("accepted without a principal"),
            )),
        },
        AuthVerdict::Refuse(p) => {
            e.outcome = "refused";
            e.reason = login.refusal_reason();
            svc.audit().emit(&e);
            HandoffResult::Refused(p)
        }
    }
}

/// DCs as in-process login services: tests, and single-process setups.
#[derive(Default)]
pub struct LocalPeers {
    dcs: HashMap<DcId, Arc<LoginService>>,
}

impl std::fmt::Debug for LocalPeers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalPeers")
            .field("dcs", &self.dcs.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl LocalPeers {
    /// `dc`'s logins are decided by `svc`.
    #[must_use]
    pub fn with(mut self, dc: DcId, svc: Arc<LoginService>) -> Self {
        self.dcs.insert(dc, svc);
        self
    }
}

#[async_trait]
impl HandoffTarget for LocalPeers {
    async fn login(
        &self,
        dc: &DcId,
        start: HandoffStart,
        password: Zeroizing<Vec<u8>>,
    ) -> Result<HandoffResult> {
        let svc = self.dcs.get(dc).ok_or_else(|| {
            SprigError::new(SprigCode::AuthUnavailable, format!("no gateway in {dc}"))
        })?;
        Ok(serve_handoff(svc, start, &password).await)
    }
}

/// The source side: a [`LoginFlow`] that relays the decision to an allowed
/// DC (see the module docs).
pub struct RelayLogin {
    req: LoginRequest,
    here: DcId,
    dcs: Vec<DcId>,
    peers: Arc<dyn HandoffTarget>,
    result: Option<HandoffResult>,
    used_password: bool,
    rebackend: Option<Rebackend>,
}

/// The source gateway's own backend target for an accepted principal.
type Rebackend = Arc<dyn Fn(&Principal) -> Option<Result<BackendTarget>> + Send + Sync>;

impl std::fmt::Debug for RelayLogin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelayLogin")
            .field("here", &self.here)
            .field("dcs", &self.dcs)
            .field("result", &self.result)
            .finish_non_exhaustive()
    }
}

impl RelayLogin {
    /// Relays `req`, arriving in `here`, to `dcs` (nearest first) through
    /// `peers`.
    #[must_use]
    pub fn new(
        req: LoginRequest,
        here: DcId,
        dcs: Vec<DcId>,
        peers: Arc<dyn HandoffTarget>,
    ) -> Self {
        Self {
            req,
            here,
            dcs,
            peers,
            result: None,
            used_password: false,
            rebackend: None,
        }
    }

    /// The same relay giving accepted principals this gateway's own
    /// backend target (its account and credential), never the deciding
    /// gateway's.
    #[must_use]
    pub fn with_rebackend(mut self, f: Rebackend) -> Self {
        self.rebackend = Some(f);
        self
    }

    /// An accepted principal with this gateway's backend target; a refusal
    /// (39301) when none can be had.
    fn localize(&self, r: HandoffResult) -> HandoffResult {
        let HandoffResult::Accepted(mut p) = r else {
            return r;
        };
        match self.rebackend.as_ref().and_then(|f| f(&p)) {
            Some(Ok(b)) => p.backend = b,
            Some(Err(e)) => {
                tracing::warn!(error = %e, "no backend for a handed-off login");
                return HandoffResult::Refused(ErrPacket::from_error(self.req.persona, &e));
            }
            None if p.backend.credentials.to_string() == UNRESOLVED => {
                return HandoffResult::Refused(ErrPacket::from_error(
                    self.req.persona,
                    &SprigError::new(
                        SprigCode::AuthUnavailable,
                        "no backend account for a handed-off login",
                    ),
                ));
            }
            None => {}
        }
        HandoffResult::Accepted(p)
    }

    /// The principal, once accepted.
    #[must_use]
    pub fn principal(&self) -> Option<&Principal> {
        match &self.result {
            Some(HandoffResult::Accepted(p)) => Some(p),
            _ => None,
        }
    }
}

#[async_trait]
impl LoginFlow for RelayLogin {
    fn method(&self, client_plugin: Option<&str>) -> Method {
        if client_plugin == Some(Method::ClearText.plugin_name()) {
            Method::ClearText
        } else {
            Method::CachingSha2
        }
    }

    fn check_fast(&mut self, _nonce: &[u8; NONCE_LEN], scramble: &[u8]) -> bool {
        self.used_password |= !scramble.is_empty();
        false
    }

    fn check_native(&mut self, _nonce: &[u8; NONCE_LEN], scramble: &[u8]) -> bool {
        self.used_password |= !scramble.is_empty();
        false
    }

    async fn check_password(&mut self, password: Zeroizing<Vec<u8>>) -> bool {
        self.used_password |= !password.is_empty();
        let start = HandoffStart::of(&self.req, self.here.clone());
        for dc in &self.dcs {
            match self.peers.login(dc, start.clone(), password.clone()).await {
                Ok(r) => {
                    let r = self.localize(r);
                    metrics::counter!("sprig_auth_handoffs_total", "outcome" => "decided")
                        .increment(1);
                    let ok = matches!(r, HandoffResult::Accepted(_));
                    self.result = Some(r);
                    return ok;
                }
                Err(e) => {
                    metrics::counter!("sprig_auth_handoffs_total", "outcome" => "unreachable")
                        .increment(1);
                    tracing::warn!(%dc, error = %e, "login handoff target unreachable; trying the next");
                }
            }
        }
        false
    }

    async fn decide(&mut self, outcome: Outcome, _path: AuthPath) -> AuthVerdict {
        let persona = self.req.persona;
        match (&self.result, outcome) {
            (Some(HandoffResult::Accepted(_)), Outcome::Verified) => AuthVerdict::Accept,
            (Some(HandoffResult::Refused(p)), _) => AuthVerdict::Refuse(p.clone()),
            (None, Outcome::WrongCredentials | Outcome::Verified)
                if self.used_password && !self.dcs.is_empty() =>
            {
                // Every allowed DC was unreachable.
                AuthVerdict::Refuse(ErrPacket::from_error(
                    persona,
                    &SprigError::new(
                        SprigCode::AuthUnavailable,
                        "authentication unavailable; retry",
                    ),
                ))
            }
            _ => {
                let name = String::from_utf8_lossy(&self.req.login_name).into_owned();
                AuthVerdict::Refuse(ErrPacket::access_denied(
                    persona,
                    &name,
                    &self.req.client_ip.to_string(),
                    self.used_password,
                ))
            }
        }
    }
}

/// This gateway's side of the hand-off: its DC and how it reaches the
/// gateways of other DCs.
pub struct HandoffConfig {
    /// This gateway's DC (`[meta] dc_id`), never the client's hint.
    pub here: DcId,
    /// The other DCs' gateways.
    pub peers: Arc<dyn HandoffTarget>,
}

impl std::fmt::Debug for HandoffConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandoffConfig")
            .field("here", &self.here)
            .finish_non_exhaustive()
    }
}

impl LoginService {
    /// Where the login `req` is decided (see the module docs): `Local`
    /// without a hand-off configuration, for the accounts file, for a
    /// login that names no tenant, or when the directory can't be read
    /// (the local login then refuses with 39301 itself).
    pub async fn route_of(&self, req: &LoginRequest, http: bool) -> Route {
        let (Some(h), crate::mysql::LoginSource::Catalog(c)) = (self.handoff(), self.source())
        else {
            return Route::Local;
        };
        let max_user = if http {
            crate::admin::MAX_NAME
        } else {
            max_user_len(req.persona)
        };
        match c.residency_of(req, max_user).await {
            Ok(Some(r)) => {
                if r.allowed_dcs.is_empty() {
                    // A block without a readable set allows nowhere.
                    Route::Deny
                } else {
                    route(Some(&r.allowed_dcs), &h.here, r.deny_remote_access)
                }
            }
            Ok(None) => Route::Local,
            Err(e) => {
                tracing::warn!(error = %e, "residency unreadable; the login decides locally (39301 if its catalog can't load either)");
                Route::Local
            }
        }
    }

    /// Starts one MySQL login, routed by the tenant's residency: decided
    /// here, relayed to an allowed DC, or refused.
    pub async fn begin(self: &Arc<Self>, req: LoginRequest) -> ResidentLogin {
        self.begin_as(req, false).await
    }

    /// [`Self::begin`] for an HTTP login.
    pub async fn begin_http(self: &Arc<Self>, req: LoginRequest) -> ResidentLogin {
        self.begin_as(req, true).await
    }

    async fn begin_as(self: &Arc<Self>, req: LoginRequest, http: bool) -> ResidentLogin {
        match self.route_of(&req, http).await {
            Route::Local => ResidentLogin::Local(Box::new(if http {
                self.start_http(req).await
            } else {
                self.start(req).await
            })),
            Route::Deny => {
                metrics::counter!("sprig_auth_handoffs_total", "outcome" => "denied").increment(1);
                ResidentLogin::Deny(DenyLogin {
                    persona: req.persona,
                })
            }
            Route::HandOff(dcs) => {
                let (here, peers) = match self.handoff() {
                    Some(h) => (h.here.clone(), Arc::clone(&h.peers)),
                    None => return ResidentLogin::Local(Box::new(self.start(req).await)),
                };
                let mut relay = RelayLogin::new(req, here, dcs, peers);
                if let crate::mysql::LoginSource::Catalog(c) = self.source() {
                    let c = Arc::clone(c);
                    relay = relay.with_rebackend(Arc::new(move |p: &Principal| c.local_backend(p)));
                }
                ResidentLogin::Relay(Box::new(relay))
            }
        }
    }
}

/// A login refused because the tenant's residency denies remote access
/// (39302), after the same exchange as any login.
#[derive(Debug)]
pub struct DenyLogin {
    persona: Persona,
}

#[async_trait]
impl LoginFlow for DenyLogin {
    fn method(&self, client_plugin: Option<&str>) -> Method {
        if client_plugin == Some(Method::ClearText.plugin_name()) {
            Method::ClearText
        } else {
            Method::CachingSha2
        }
    }
    fn check_fast(&mut self, _nonce: &[u8; NONCE_LEN], _scramble: &[u8]) -> bool {
        false
    }
    fn check_native(&mut self, _nonce: &[u8; NONCE_LEN], _scramble: &[u8]) -> bool {
        false
    }
    async fn check_password(&mut self, _password: Zeroizing<Vec<u8>>) -> bool {
        false
    }
    async fn decide(&mut self, _outcome: Outcome, _path: AuthPath) -> AuthVerdict {
        AuthVerdict::Refuse(remote_access_denied(self.persona))
    }
}

/// A login as residency routed it ([`LoginService::begin`]).
#[derive(Debug)]
pub enum ResidentLogin {
    /// Decided here.
    Local(Box<MysqlLogin>),
    /// Relayed to an allowed DC.
    Relay(Box<RelayLogin>),
    /// Refused: 39302.
    Deny(DenyLogin),
}

impl ResidentLogin {
    /// Who logged in, once accepted.
    #[must_use]
    pub fn principal(&self) -> Option<&Principal> {
        match self {
            Self::Local(l) => l.principal(),
            Self::Relay(r) => r.principal(),
            Self::Deny(_) => None,
        }
    }

    /// The local login, if decided here (its file account, its refusal
    /// reason).
    #[must_use]
    pub fn local(&self) -> Option<&MysqlLogin> {
        match self {
            Self::Local(l) => Some(l.as_ref()),
            _ => None,
        }
    }

    /// The fast path for a clear-text password (HTTP Basic); never for a
    /// relayed or denied login.
    pub fn check_clear_fast(&mut self, password: &[u8]) -> bool {
        match self {
            Self::Local(l) => l.check_clear_fast(password),
            _ => false,
        }
    }
}

#[async_trait]
impl LoginFlow for ResidentLogin {
    fn method(&self, client_plugin: Option<&str>) -> Method {
        match self {
            Self::Local(l) => l.method(client_plugin),
            Self::Relay(r) => r.method(client_plugin),
            Self::Deny(d) => d.method(client_plugin),
        }
    }
    fn check_fast(&mut self, nonce: &[u8; NONCE_LEN], scramble: &[u8]) -> bool {
        match self {
            Self::Local(l) => l.check_fast(nonce, scramble),
            Self::Relay(r) => r.check_fast(nonce, scramble),
            Self::Deny(d) => d.check_fast(nonce, scramble),
        }
    }
    fn check_native(&mut self, nonce: &[u8; NONCE_LEN], scramble: &[u8]) -> bool {
        match self {
            Self::Local(l) => l.check_native(nonce, scramble),
            Self::Relay(r) => r.check_native(nonce, scramble),
            Self::Deny(d) => d.check_native(nonce, scramble),
        }
    }
    async fn check_password(&mut self, password: Zeroizing<Vec<u8>>) -> bool {
        match self {
            Self::Local(l) => l.check_password(password).await,
            Self::Relay(r) => r.check_password(password).await,
            Self::Deny(d) => d.check_password(password).await,
        }
    }
    async fn decide(&mut self, outcome: Outcome, path: AuthPath) -> AuthVerdict {
        match self {
            Self::Local(l) => l.decide(outcome, path).await,
            Self::Relay(r) => r.decide(outcome, path).await,
            Self::Deny(d) => d.decide(outcome, path).await,
        }
    }
}

/// What a principal's backend credential crosses as when it isn't a
/// `file:` or `env:` reference (the derived per-user backend passwords):
/// the source gateway derives its own.
const UNRESOLVED: &str = "env:SPRIG_HANDOFF_UNRESOLVED";

/// A [`Principal`] on the wire (`GatewayPeer.HandoffLogin`'s reply): the
/// principal's fields only. The backend credential crosses as its
/// reference, never the secret, and the source replaces the target with
/// its own ([`RelayLogin::with_rebackend`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipalWire {
    /// The tenant.
    pub tenant: Option<u64>,
    /// Its name.
    pub tenant_name: Option<String>,
    /// The user's id.
    pub user_id: Option<u32>,
    /// The user.
    pub user: String,
    /// The account's host pattern.
    pub host: String,
    /// The client address.
    pub client_ip: IpAddr,
    /// Sandbox mode.
    pub must_change_password: bool,
    /// `MAX_QUERIES_PER_HOUR`, `MAX_UPDATES_PER_HOUR`,
    /// `MAX_CONNECTIONS_PER_HOUR`, `MAX_USER_CONNECTIONS`.
    pub limits: [u32; 4],
    /// The auth version.
    pub auth_version: u64,
    /// The backend cluster.
    pub backend_cluster: String,
    /// The backend account.
    pub backend_user: String,
    /// The backend credential's reference.
    pub backend_credentials: String,
}

impl PrincipalWire {
    /// The wire form of `p`.
    #[must_use]
    pub fn of(p: &Principal) -> Self {
        Self {
            tenant: p.tenant.map(|t| t.0),
            tenant_name: p.tenant_name.clone(),
            user_id: p.user_id.map(|u| u.0),
            user: p.user.clone(),
            host: p.host.clone(),
            client_ip: p.client_ip,
            must_change_password: p.must_change_password,
            limits: [
                p.limits.max_queries_per_hour,
                p.limits.max_updates_per_hour,
                p.limits.max_connections_per_hour,
                p.limits.max_user_connections,
            ],
            auth_version: p.auth_version,
            backend_cluster: p.backend.cluster.to_string(),
            backend_user: p.backend.user.clone(),
            backend_credentials: match &p.backend.credentials.source {
                sprig_provision::credentials::SecretSource::Resolved(_) => UNRESOLVED.to_owned(),
                _ => p.backend.credentials.to_string(),
            },
        }
    }

    /// The principal.
    ///
    /// # Errors
    /// Internal for a malformed cluster id or credential reference.
    pub fn principal(&self) -> Result<Principal> {
        Ok(Principal {
            tenant: self.tenant.map(sprig_common::ids::TenantId),
            tenant_name: self.tenant_name.clone(),
            user_id: self.user_id.map(sprig_common::ids::UserId),
            user: self.user.clone(),
            host: self.host.clone(),
            client_ip: self.client_ip,
            must_change_password: self.must_change_password,
            limits: AccountLimits {
                max_queries_per_hour: self.limits[0],
                max_updates_per_hour: self.limits[1],
                max_connections_per_hour: self.limits[2],
                max_user_connections: self.limits[3],
            },
            auth_version: self.auth_version,
            backend: BackendTarget {
                cluster: sprig_common::ids::ClusterId::new(&self.backend_cluster)
                    .map_err(|e| SprigError::internal(format!("handoff principal: {e}")))?,
                user: self.backend_user.clone(),
                credentials: self.backend_credentials.parse().map_err(|e: SprigError| {
                    SprigError::internal(format!("handoff principal: {}", e.message))
                })?,
            },
        })
    }
}

/// 39302: the tenant's credentials stay in its allowed DCs and remote
/// access is denied.
#[must_use]
pub fn remote_access_denied(persona: Persona) -> ErrPacket {
    ErrPacket::from_error(
        persona,
        &SprigError::new(
            SprigCode::RemoteAccessDenied,
            "this tenant can't be reached from this data center",
        ),
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::mysql::FixedPolicy;
    use crate::mysql::tests::world;

    fn dc(s: &str) -> DcId {
        DcId::new(s).unwrap()
    }

    #[test]
    fn routes() {
        let (fra, ams, nyc) = (dc("fra1"), dc("ams1"), dc("nyc1"));
        let pin = [fra.clone(), ams.clone()];
        assert_eq!(route(None, &nyc, false), Route::Local);
        assert_eq!(route(Some(&[]), &nyc, true), Route::Local);
        assert_eq!(route(Some(&pin), &ams, true), Route::Local);
        assert_eq!(route(Some(&pin), &nyc, true), Route::Deny);
        assert_eq!(route(Some(&pin), &nyc, false), Route::HandOff(pin.to_vec()));
        assert_eq!(remote_access_denied(Persona::MySql84).code, 39302);
    }

    #[test]
    fn a_handoff_keeps_the_certificate_names() {
        let mut req = crate::mysql::tests::req("acme.alice");
        req.client_cert = true;
        req.client_dn = Some(crate::mysql::ClientDn {
            subject: "/O=Acme/CN=alice".into(),
            issuer: "/CN=Acme CA".into(),
        });
        let start = HandoffStart::of(&req, dc("nyc1"));
        let wire = serde_json::to_string(&start).unwrap();
        let back: HandoffStart = serde_json::from_str(&wire).unwrap();
        assert_eq!(back.request(), req);
        // A source from before names were handed off sends none: the target reads
        // none, and a SUBJECT/ISSUER account is refused there.
        let old = wire
            .replace(",\"client_subject\":\"/O=Acme/CN=alice\"", "")
            .replace(",\"client_issuer\":\"/CN=Acme CA\"", "");
        assert!(!old.contains("client_subject"), "{old}");
        let back: HandoffStart = serde_json::from_str(&old).unwrap();
        assert_eq!(back.request().client_dn, None);
        assert!(back.request().client_cert);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_relayed_login_is_decided_in_the_allowed_dc() {
        // The allowed DC (fra1) has the tenant; the source (nyc1) doesn't.
        let w = world(FixedPolicy::default());
        let peers: Arc<dyn HandoffTarget> =
            Arc::new(LocalPeers::default().with(dc("fra1"), Arc::clone(&w.svc)));
        let relay = |login: &str| {
            RelayLogin::new(
                crate::mysql::tests::req(login),
                dc("nyc1"),
                vec![dc("down1"), dc("fra1")],
                Arc::clone(&peers),
            )
        };
        let mut ok = relay("acme.alice");
        assert!(
            !ok.check_fast(&[0; NONCE_LEN], &[1; 32]),
            "never the fast path"
        );
        assert!(ok.check_password(Zeroizing::new(b"pw".to_vec())).await);
        assert_eq!(
            ok.decide(Outcome::Verified, AuthPath::Full).await,
            AuthVerdict::Accept
        );
        assert_eq!(ok.principal().unwrap().user, "alice");
        let mut bad = relay("acme.alice");
        assert!(!bad.check_password(Zeroizing::new(b"no".to_vec())).await);
        let v = bad.decide(Outcome::WrongCredentials, AuthPath::Full).await;
        assert!(matches!(v, AuthVerdict::Refuse(ref p) if p.code == 1045));
        // The target audited and decided.
        let names = w.audit.names();
        assert!(names.contains(&"auth.handoff"), "{names:?}");
        // Every target down: 39301.
        let mut lost = RelayLogin::new(
            crate::mysql::tests::req("acme.alice"),
            dc("nyc1"),
            vec![dc("down1")],
            Arc::clone(&peers),
        );
        assert!(!lost.check_password(Zeroizing::new(b"pw".to_vec())).await);
        let v = lost.decide(Outcome::WrongCredentials, AuthPath::Full).await;
        assert!(matches!(v, AuthVerdict::Refuse(ref p) if p.code == 39301));
    }

    #[allow(clippy::unnecessary_wraps)] // reads as set_residency's argument
    fn pinned(dcs: &[&str], deny: bool) -> Option<crate::account::TenantResidency> {
        Some(crate::account::TenantResidency {
            allowed_dcs: dcs.iter().map(|d| dc(d)).collect(),
            deny_remote_access: deny,
        })
    }

    /// The residency wiring: `begin` reads the residency first and picks the
    /// flow; the source never loads the tenant's catalog; the target
    /// refuses a handoff it isn't allowed to decide.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn begin_routes_by_residency_and_the_target_checks_it() {
        // fra1: an allowed DC, deciding with its own catalog.
        let fra = crate::mysql::tests::world_at(
            FixedPolicy::default(),
            Some(Arc::new(HandoffConfig {
                here: dc("fra1"),
                peers: Arc::new(LocalPeers::default()),
            })),
        );
        fra.tenants.set_residency("acme", pinned(&["fra1"], false));
        let peers: Arc<dyn HandoffTarget> =
            Arc::new(LocalPeers::default().with(dc("fra1"), Arc::clone(&fra.svc)));
        // nyc1: outside; its catalog cache must never be asked for acme.
        let nyc = crate::mysql::tests::world_at(
            FixedPolicy::default(),
            Some(Arc::new(HandoffConfig {
                here: dc("nyc1"),
                peers: Arc::clone(&peers),
            })),
        );
        nyc.tenants.set_residency("acme", pinned(&["fra1"], false));
        nyc.catalogs.map.lock().unwrap().clear();
        let req = crate::mysql::tests::req;
        // Relayed and accepted, with the principal from fra1.
        let mut l = nyc.svc.begin(req("acme.alice")).await;
        assert!(matches!(l, ResidentLogin::Relay(_)), "{l:?}");
        assert!(!l.check_clear_fast(b"pw"), "no fast path at the source");
        assert!(l.check_password(Zeroizing::new(b"pw".to_vec())).await);
        assert_eq!(
            l.decide(Outcome::Verified, AuthPath::Full).await,
            AuthVerdict::Accept
        );
        let p = l.principal().unwrap();
        assert_eq!(
            (p.user.as_str(), p.tenant),
            ("alice", Some(crate::mysql::tests::ACME))
        );
        // The wire form round-trips.
        assert_eq!(&PrincipalWire::of(p).principal().unwrap(), p);
        // Wrong password: fra1's 1045.
        let mut l = nyc.svc.begin(req("acme.alice")).await;
        assert!(!l.check_password(Zeroizing::new(b"no".to_vec())).await);
        let v = l.decide(Outcome::WrongCredentials, AuthPath::Full).await;
        assert!(
            matches!(v, AuthVerdict::Refuse(ref p) if p.code == 1045),
            "{v:?}"
        );
        // deny_remote_access: 39302 after the exchange.
        nyc.tenants.set_residency("acme", pinned(&["fra1"], true));
        let mut l = nyc.svc.begin(req("acme.alice")).await;
        assert!(matches!(l, ResidentLogin::Deny(_)));
        assert!(!l.check_password(Zeroizing::new(b"pw".to_vec())).await);
        let v = l.decide(Outcome::WrongCredentials, AuthPath::Full).await;
        assert!(
            matches!(v, AuthVerdict::Refuse(ref p) if p.code == 39302),
            "{v:?}"
        );
        // A block without a readable set allows nowhere: 39302.
        nyc.tenants.set_residency("acme", pinned(&[], false));
        assert!(matches!(
            nyc.svc.begin(req("acme.alice")).await,
            ResidentLogin::Deny(_)
        ));
        // Unpinned tenants and logins naming no tenant decide locally.
        nyc.tenants.set_residency("acme", None);
        assert!(matches!(
            nyc.svc.begin(req("acme.alice")).await,
            ResidentLogin::Local(_)
        ));
        assert!(matches!(
            nyc.svc.begin(req("nodots")).await,
            ResidentLogin::Local(_)
        ));
        // In an allowed DC: local.
        assert!(matches!(
            fra.svc.begin(req("acme.alice")).await,
            ResidentLogin::Local(_)
        ));
        // A handoff reaching a DC that isn't allowed is refused there
        // (39301), without deciding.
        fra.tenants.set_residency("acme", pinned(&["ams1"], false));
        let start = HandoffStart::of(&req("acme.alice"), dc("nyc1"));
        let r = serve_handoff(&fra.svc, start, b"pw").await;
        assert!(
            matches!(r, HandoffResult::Refused(ref p) if p.code == 39301),
            "{r:?}"
        );
    }
}
