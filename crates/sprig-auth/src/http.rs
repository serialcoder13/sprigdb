//! HTTP authentication on 8080 and 8443 (chapter 14, "HTTP on 8080 and
//! 8443"; build step 11), as a library: the HTTP front end (chapter 37,
//! milestone M0.7) calls [`HttpAuth::authenticate`] once per request with
//! the request's `Authorization`, `Host` and `x-sprig-tenant` headers, the
//! client address and whether it arrived over TLS, and gets back the
//! [`Principal`] or an [`HttpAuthError`] with the status, the
//! `WWW-Authenticate` challenge and `Retry-After` to send.
//!
//! | `Authorization` | Check |
//! |---|---|
//! | `Basic` base64(`user:password`) | Like Redis `AUTH`: the fast cache (`SHA256(SHA256(password))`), else the `caching_sha2_password` verifiers on a hashing slot; then the MySQL login's policy order (temporary lock, credentials, suspension, `ACCOUNT LOCK`, TLS requirement, expiry: HTTP has no sandbox mode, so an expired password is refused with 1862) |
//! | `Bearer sprig_<tenant>_<key_id>_<secret>` | API key ([`crate::apikey`]): `SHA256(secret)` in constant time; revocation, expiry, `allowed_cidrs`; the user live, with the `api_key` method; temporary lock, `ACCOUNT LOCK`, TLS requirement, suspension |
//! | `Bearer` anything else | 401 until SprigDB tokens (R2) and external JWTs (R6) |
//!
//! **The tenant.** For Basic: the `Host` header (or TLS SNI) under
//! `listener.tenant_sni_suffixes` names it, then the user is the whole
//! name; else `x-sprig-tenant`; else the user name's prefix
//! (`acme.alice`). For API keys: the key's prefix.
//!
//! Every Basic request goes through the same [`LoginService`] as MySQL
//! logins, so lockout counters, address limits, failure delays and audit
//! events (`protocol = http`, mechanism `basic` or `api_key`) are shared.
//! Successful Basic requests take the fast path after the first, so a
//! client sending its password with every request costs two SHA-256
//! hashes, not a SHA-256 crypt.
//!
//! Responses: 401 with `WWW-Authenticate: Basic realm="sprigdb", Bearer`
//! for credential and account refusals (the body carries the MySQL error,
//! 1045, 3118, 3955, 1862), 403 for a suspended tenant (39303), 429 with
//! `Retry-After` for an address refused for too many failures (39300), 503
//! when credentials can't be read (39301).

use std::net::IpAddr;
use std::sync::Arc;

use sprig_common::{SprigCode, SprigError};
use sprig_proto_mysql::auth::{AuthPath, AuthVerdict, LoginFlow, Outcome};
use sprig_proto_mysql::{ErrPacket, Persona};
use zeroize::Zeroizing;

use crate::apikey::parse_key;
use crate::audit::{self, AuditEvent};
use crate::mysql::{LoginRequest, LoginService, LoginSource, Principal, locked, temp_locked};
use crate::store::{AuthMethods, RequireTls};

/// The challenge of every 401.
pub const WWW_AUTHENTICATE: &str = "Basic realm=\"sprigdb\", Bearer";

/// One request's credentials and context.
#[derive(Clone, Copy)]
pub struct HttpRequest<'a> {
    /// The `Authorization` header, if any.
    pub authorization: Option<&'a str>,
    /// The `Host` header (a port is ignored).
    pub host: Option<&'a str>,
    /// The TLS SNI, if any (wins over `Host`).
    pub sni: Option<&'a str>,
    /// The `x-sprig-tenant` header.
    pub tenant_header: Option<&'a str>,
    /// The client address (after a trusted proxy's header).
    pub client_ip: IpAddr,
    /// Whether the request arrived over TLS (8443, or a trusted proxy's
    /// TLS).
    pub tls: bool,
}

impl std::fmt::Debug for HttpRequest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpRequest")
            .field("host", &self.host)
            .field("tenant_header", &self.tenant_header)
            .field("client_ip", &self.client_ip)
            .field("tls", &self.tls)
            .finish_non_exhaustive()
    }
}

/// A refused request: what to answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpAuthError {
    /// 401, 403, 429, 500 or 503.
    pub status: u16,
    /// The MySQL error for the body (code, SQLSTATE, message).
    pub error: ErrPacket,
    /// `WWW-Authenticate` to send (401 only).
    pub www_authenticate: Option<&'static str>,
    /// `Retry-After` in seconds (429 only).
    pub retry_after: Option<u64>,
}

impl HttpAuthError {
    fn from_packet(p: ErrPacket) -> Self {
        let status = match p.code {
            39303 => 403,
            39300 => 429,
            39301 => 503,
            39998 | 39999 => 500,
            _ => 401,
        };
        Self {
            status,
            www_authenticate: (status == 401).then_some(WWW_AUTHENTICATE),
            retry_after: None,
            error: p,
        }
    }

    fn from_error(e: &SprigError) -> Self {
        Self::from_packet(ErrPacket::from_error(Persona::MySql84, e))
    }

    fn denied(user: &str, ip: IpAddr, using_password: bool) -> Self {
        Self::from_packet(ErrPacket::access_denied(
            Persona::MySql84,
            user,
            &ip.to_string(),
            using_password,
        ))
    }
}

/// The parsed `Authorization` header.
enum Credential {
    Basic {
        user: String,
        password: Zeroizing<Vec<u8>>,
    },
    ApiKey(String),
    OtherBearer,
}

fn parse_authorization(h: &str) -> Option<Credential> {
    let (scheme, rest) = h.trim().split_once(' ')?;
    let rest = rest.trim();
    if scheme.eq_ignore_ascii_case("basic") {
        let raw = Zeroizing::new(base64_decode(rest)?);
        let colon = raw.iter().position(|&b| b == b':')?;
        let user = std::str::from_utf8(&raw[..colon]).ok()?.to_owned();
        let password = Zeroizing::new(raw[colon + 1..].to_vec());
        return Some(Credential::Basic { user, password });
    }
    if scheme.eq_ignore_ascii_case("bearer") {
        return Some(if rest.starts_with("sprig_") {
            Credential::ApiKey(rest.to_owned())
        } else {
            Credential::OtherBearer
        });
    }
    None
}

/// Standard base64 with optional padding.
fn base64_decode(s: &str) -> Option<Vec<u8>> {
    let val = |c: u8| -> Option<u32> {
        Some(match c {
            b'A'..=b'Z' => u32::from(c - b'A'),
            b'a'..=b'z' => u32::from(c - b'a') + 26,
            b'0'..=b'9' => u32::from(c - b'0') + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        })
    };
    let bytes = s.trim_end_matches('=').as_bytes();
    if bytes.len() % 4 == 1 {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    for chunk in bytes.chunks(4) {
        let mut n = 0_u32;
        for (i, &c) in chunk.iter().enumerate() {
            n |= val(c)? << (18 - 6 * i);
        }
        let b = n.to_be_bytes();
        out.extend_from_slice(&b[1..chunk.len()]);
    }
    Some(out)
}

/// HTTP authentication over a [`LoginService`] (see the module docs).
#[derive(Debug, Clone)]
pub struct HttpAuth {
    svc: Arc<LoginService>,
}

impl HttpAuth {
    /// Requests checked through `svc`, the process's login service.
    #[must_use]
    pub fn new(svc: Arc<LoginService>) -> Self {
        Self { svc }
    }

    /// Authenticates one request.
    ///
    /// # Errors
    /// [`HttpAuthError`]: 401 (1045, 3118, 3955, 1862), 403 (39303), 429
    /// (39300), 500 (39998), 503 (39301).
    pub async fn authenticate(&self, req: &HttpRequest<'_>) -> Result<Principal, HttpAuthError> {
        if let Err(e) = self.svc.admit(req.client_ip) {
            let mut err = HttpAuthError::from_error(&e);
            err.retry_after = self
                .svc
                .ip_limiter()
                .blocked(req.client_ip)
                .map(|d| d.as_secs().max(1));
            return Err(err);
        }
        let Some(h) = req.authorization else {
            return Err(HttpAuthError::denied("", req.client_ip, false));
        };
        match parse_authorization(h) {
            Some(Credential::Basic { user, password }) => self.basic(req, &user, &password).await,
            Some(Credential::ApiKey(k)) => self.api_key(req, &k).await,
            Some(Credential::OtherBearer) | None => {
                self.refuse_unparsed(req);
                Err(HttpAuthError::denied("", req.client_ip, true))
            }
        }
    }

    fn refuse_unparsed(&self, req: &HttpRequest<'_>) {
        let mut e = AuditEvent::new(audit::events::LOGIN_FAILED, "http", self.svc_now());
        e.client_ip = Some(req.client_ip);
        e.outcome = "refused";
        e.reason = Some("bad_credentials");
        e.mechanism = Some("bearer");
        self.svc.audit().emit(&e);
        self.svc.ip_failure(req.client_ip, "http");
    }

    fn svc_now(&self) -> std::time::SystemTime {
        self.svc.clock().wall()
    }

    async fn basic(
        &self,
        req: &HttpRequest<'_>,
        user: &str,
        password: &[u8],
    ) -> Result<Principal, HttpAuthError> {
        let host = req
            .sni
            .or(req.host)
            .map(|h| h.rsplit_once(':').map_or(h, |(name, _)| name).to_owned());
        let login_name = match req.tenant_header {
            Some(t) if host.is_none() && !t.is_empty() => format!("{t}.{user}"),
            _ => user.to_owned(),
        };
        let mut login = self
            .svc
            .start_http(LoginRequest {
                persona: Persona::MySql84,
                login_name: login_name.into_bytes(),
                sni: host,
                client_ip: req.client_ip,
                tls: req.tls,
                client_cert: false,
                can_handle_expired_password: false,
            })
            .await;
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
        match login.decide(outcome, path).await {
            AuthVerdict::Accept => login
                .principal()
                .cloned()
                .ok_or_else(|| HttpAuthError::from_error(&SprigError::internal("no principal"))),
            AuthVerdict::Refuse(p) => Err(HttpAuthError::from_packet(p)),
        }
    }

    async fn api_key(&self, req: &HttpRequest<'_>, key: &str) -> Result<Principal, HttpAuthError> {
        let now = self.svc_now();
        let mut ev = AuditEvent::new(audit::events::LOGIN_SUCCEEDED, "http", now);
        ev.client_ip = Some(req.client_ip);
        ev.mechanism = Some("api_key");
        let r = self.check_key(req, key, &mut ev).await;
        if let Err((reason, _)) = &r {
            ev.event = audit::events::LOGIN_FAILED;
            ev.outcome = "refused";
            ev.reason = Some(reason);
        }
        self.svc.audit().emit(&ev);
        match r {
            Ok(p) => Ok(p),
            Err((reason, err)) => {
                if reason == "bad_credentials" {
                    self.svc.ip_failure(req.client_ip, "http");
                }
                Err(err)
            }
        }
    }

    #[allow(clippy::too_many_lines)] // the checks, in order
    async fn check_key(
        &self,
        req: &HttpRequest<'_>,
        key: &str,
        ev: &mut AuditEvent,
    ) -> Result<Principal, (&'static str, HttpAuthError)> {
        let bad = || {
            (
                "bad_credentials",
                HttpAuthError::denied("", req.client_ip, true),
            )
        };
        let LoginSource::Catalog(cl) = self.svc.source() else {
            return Err(bad());
        };
        let Some(k) = parse_key(key) else {
            return Err(bad());
        };
        let tenant = match cl.tenants().lookup(&k.tenant).await {
            Ok(Some(t)) if t.can_log_in => t,
            Ok(_) => return Err(bad()),
            Err(e) => {
                tracing::warn!(error = %e, "API key: tenant directory unreadable");
                return Err((
                    "unavailable",
                    HttpAuthError::from_error(&SprigError::new(
                        SprigCode::AuthUnavailable,
                        "authentication unavailable; retry",
                    )),
                ));
            }
        };
        ev.tenant = Some(tenant.id);
        ev.tenant_name = Some(tenant.name.clone());
        let catalog = match cl.catalogs().catalog(tenant.id).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(tenant = %tenant.name, error = %e, "API key: catalog unreadable");
                return Err((
                    "unavailable",
                    HttpAuthError::from_error(&SprigError::new(
                        SprigCode::AuthUnavailable,
                        "authentication unavailable; retry",
                    )),
                ));
            }
        };
        let Some(auth) = crate::cache::TenantAuth::of(&catalog) else {
            return Err(bad());
        };
        let Some(api) = auth.api_key(&k.key_id).cloned() else {
            return Err(bad());
        };
        let secret = k.secret.clone();
        // A SHA-256: cheap, no hashing slot.
        if !api.secret_matches(secret.as_bytes()) {
            return Err(bad());
        }
        let now = self.svc_now();
        if let Some(why) = api.unusable(req.client_ip, now) {
            return Err((why, HttpAuthError::denied("", req.client_ip, true)));
        }
        let Some(user) = auth.user(api.row.user_id).cloned() else {
            return Err(bad());
        };
        ev.user = Some(user.row.name.clone());
        ev.host = Some(user.row.host.clone());
        ev.user_id = Some(user.row.user_id);
        ev.detail = Some(format!("key {}", api.row.key_id));
        if !user.row.auth_methods.contains(AuthMethods::API_KEY) {
            return Err(bad());
        }
        let (name, host) = (user.row.name.as_str(), user.row.host.as_str());
        if let Some(lock) = auth
            .temp_lock(user.row.user_id)
            .filter(|l| l.locked_until > now)
        {
            let p = temp_locked(Persona::MySql84, name, host, &lock, now);
            return Err(("temp_locked", HttpAuthError::from_packet(p)));
        }
        if tenant.suspended {
            return Err((
                "suspended",
                HttpAuthError::from_error(&SprigError::new(
                    SprigCode::TenantSuspended,
                    format!("tenant {} is suspended", tenant.name),
                )),
            ));
        }
        if user.row.account_locked {
            return Err((
                "locked",
                HttpAuthError::from_packet(locked(Persona::MySql84, name, host)),
            ));
        }
        let need_tls = user.row.require_tls != RequireTls::None || cl.require_tls(&catalog);
        if (need_tls && !req.tls) || user.row.require_tls == RequireTls::X509 {
            return Err((
                "tls_required",
                HttpAuthError::denied(name, req.client_ip, true),
            ));
        }
        let backend = cl.backend_of(&tenant, &user).map_err(|e| {
            tracing::error!(tenant = %tenant.name, error = %e, "no backend for a verified API key");
            ("no_backend", HttpAuthError::from_error(&e))
        })?;
        Ok(Principal {
            tenant: Some(tenant.id),
            tenant_name: Some(tenant.name.clone()),
            user_id: Some(user.row.user_id),
            user: user.row.name.clone(),
            host: user.row.host.clone(),
            client_ip: req.client_ip,
            must_change_password: false,
            limits: user.row.limits,
            auth_version: catalog.auth_version,
            backend,
        })
    }
}

#[cfg(test)]
mod tests;
