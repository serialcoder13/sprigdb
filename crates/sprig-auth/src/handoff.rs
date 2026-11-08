//! Credential residency: handing a login to an allowed DC (chapter 14,
//! "Credential residency"; build step 9).
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
//! **Transport.** Chapter 14 runs this over `GatewayPeer.HandoffLogin` on
//! port 7403 with mutual TLS (chapter 04). The peer service doesn't exist
//! yet, and neither do chapter 26's pins, so [`HandoffTarget`] is the seam:
//! [`LocalPeers`] (tests: DCs as in-process services) implements it now,
//! the gRPC client later. The messages ([`HandoffStart`],
//! [`HandoffResult`]) are serde types, ready for either. A handoff costs
//! one WAN round trip: the whole exchange happens at the source, and only
//! the decision crosses.

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

use crate::audit::{self, AuditEvent};
use crate::mysql::{LoginRequest, LoginService, Principal};

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

/// Reaches gateways of other DCs (chapter 04's `GatewayPeer`).
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
/// failures.
pub async fn serve_handoff(
    svc: &Arc<LoginService>,
    start: HandoffStart,
    password: &[u8],
) -> HandoffResult {
    let mut login = svc.start(start.request()).await;
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
}

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
        }
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
}
