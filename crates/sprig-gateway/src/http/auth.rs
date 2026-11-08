//! Authentication of HTTP requests (chapter 37, "Authentication"; chapter
//! 14, "HTTP on 8080 and 8443").
//!
//! The HTTP front end asks an [`HttpAuthenticator`] for the principal of
//! each data request. Chapter 14 step 11 builds the real one in
//! `sprig-auth` (HTTP Basic and API keys); until its signature is in the
//! book, the gateway codes against this small trait.
//!
//! [`BasicLogins`] is the interim implementation: HTTP Basic over the same
//! [`LoginService`] the MySQL listeners use, so a request is checked exactly
//! like a MySQL login of the same user: tenant resolution (a dotted user
//! name, `x-sprig-tenant`, or SNI on a tenant endpoint), the decoy for
//! unknown accounts, the `caching_sha2_password` fast cache (a scramble of
//! the password against a fresh nonce) and then the full verifier, and
//! chapter 14's policy order (temporary lock, suspension, `ACCOUNT LOCK`,
//! TLS requirements, expiry). An expired password is refused (sandbox mode
//! has no HTTP form). Failures are MySQL's 1045 text, sent as 401.

use std::net::IpAddr;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use sprig_auth::{LoginRequest, LoginService, Principal};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::Persona;
use sprig_proto_mysql::auth::{AuthPath, AuthVerdict, LoginFlow, Outcome};
use zeroize::Zeroizing;

/// What a request carries that authentication may use.
#[derive(Debug, Clone)]
pub struct HttpCredentials {
    /// The `authorization` header.
    pub authorization: Option<String>,
    /// The `x-sprig-tenant` header.
    pub tenant_header: Option<String>,
    /// The TLS SNI.
    pub sni: Option<String>,
    /// The client address, after the PROXY header.
    pub client_ip: IpAddr,
    /// The request arrived over TLS.
    pub tls: bool,
    /// The client presented a certificate the listener verified.
    pub client_cert: bool,
}

/// Decides who sent a request.
#[async_trait]
pub trait HttpAuthenticator: Send + Sync + 'static {
    /// The principal, or the error to answer with (1045 → 401).
    ///
    /// # Errors
    /// 1045 for missing or bad credentials; 39786 when no tenant is named;
    /// 39301 when credentials can't be checked now.
    async fn authenticate(&self, creds: &HttpCredentials) -> Result<Principal>;
}

/// The `WWW-Authenticate` value of a 401 (chapter 14).
pub const WWW_AUTHENTICATE: &str = "Basic realm=\"sprigdb\", Bearer";

fn denied(user: &str, host: &IpAddr, used_password: bool) -> SprigError {
    SprigError::backend(
        1045,
        *b"28000",
        format!(
            "Access denied for user '{user}'@'{host}' (using password: {})",
            if used_password { "YES" } else { "NO" }
        ),
    )
}

/// HTTP Basic over the MySQL logins (interim; see the module notes).
pub struct BasicLogins {
    svc: Arc<LoginService>,
}

impl std::fmt::Debug for BasicLogins {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BasicLogins").finish_non_exhaustive()
    }
}

impl BasicLogins {
    /// Basic logins checked by `svc`.
    #[must_use]
    pub fn new(svc: Arc<LoginService>) -> Self {
        Self { svc }
    }
}

/// `Basic base64(user:password)` split into its parts.
fn parse_basic(header: &str) -> Option<(String, Zeroizing<Vec<u8>>)> {
    let (scheme, rest) = header.trim().split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }
    let raw = Zeroizing::new(STANDARD.decode(rest.trim()).ok()?);
    let colon = raw.iter().position(|b| *b == b':')?;
    let user = String::from_utf8(raw[..colon].to_vec()).ok()?;
    Some((user, Zeroizing::new(raw[colon + 1..].to_vec())))
}

#[async_trait]
impl HttpAuthenticator for BasicLogins {
    async fn authenticate(&self, creds: &HttpCredentials) -> Result<Principal> {
        let Some(header) = creds.authorization.as_deref() else {
            metrics::counter!("sprig_auth_http_requests_total", "scheme" => "none", "outcome" => "refused").increment(1);
            return Err(denied("", &creds.client_ip, false));
        };
        let Some((user, password)) = parse_basic(header) else {
            metrics::counter!("sprig_auth_http_requests_total", "scheme" => "other", "outcome" => "refused").increment(1);
            return Err(denied("", &creds.client_ip, false));
        };
        // `x-sprig-tenant` names the tenant for an undotted user (chapter
        // 14, "Finding the tenant"); a dotted name or SNI wins.
        let login_name = match &creds.tenant_header {
            Some(t) if !user.contains('.') && creds.sni.is_none() => format!("{t}.{user}"),
            _ => user.clone(),
        };
        if !login_name.contains('.') && creds.sni.is_none() {
            return Err(SprigError::new(
                SprigCode::TenantAmbiguous,
                "name the tenant: log in as <tenant>.<user>, or send x-sprig-tenant",
            ));
        }
        let mut login = self
            .svc
            .start(LoginRequest {
                persona: Persona::MySql84,
                login_name: login_name.into_bytes(),
                sni: creds.sni.clone(),
                client_ip: creds.client_ip,
                tls: creds.tls,
                client_cert: creds.client_cert,
                can_handle_expired_password: false,
            })
            .await;
        // The fast path first: a scramble of the password against a fresh
        // nonce, checked against the fast cache, as a MySQL client would.
        let nonce = sprig_proto_mysql::handshake::new_nonce()
            .map_err(|e| SprigError::internal(format!("no nonce: {e}")))?;
        let (outcome, path) = if password.is_empty() {
            let ok = login.check_password(password).await;
            (ok, AuthPath::Empty)
        } else {
            let scramble = sprig_auth::verifier::caching_sha2_scramble(&password, &nonce);
            if login.check_fast(&nonce, &scramble) {
                (true, AuthPath::Fast)
            } else {
                (login.check_password(password).await, AuthPath::Full)
            }
        };
        let outcome = if outcome {
            Outcome::Verified
        } else {
            Outcome::WrongCredentials
        };
        match login.decide(outcome, path).await {
            AuthVerdict::Accept => {
                let p = login
                    .principal()
                    .cloned()
                    .ok_or_else(|| SprigError::internal("login accepted without a principal"))?;
                metrics::counter!("sprig_auth_http_requests_total", "scheme" => "basic", "outcome" => "ok").increment(1);
                Ok(p)
            }
            AuthVerdict::Refuse(e) => {
                metrics::counter!("sprig_auth_http_requests_total", "scheme" => "basic", "outcome" => "refused").increment(1);
                Err(sprig_api::status::from_parts(e.code, &e.sqlstate, &e.message))
            }
        }
    }
}

/// Accepts every request as one principal (tests of the HTTP layer).
#[derive(Debug, Clone)]
pub struct FixedPrincipal(pub Principal);

#[async_trait]
impl HttpAuthenticator for FixedPrincipal {
    async fn authenticate(&self, _creds: &HttpCredentials) -> Result<Principal> {
        Ok(self.0.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_headers_parse() {
        let h = format!("Basic {}", STANDARD.encode("acme.app:pa:ss"));
        let (u, p) = parse_basic(&h).unwrap();
        assert_eq!(u, "acme.app");
        assert_eq!(p.as_slice(), b"pa:ss");
        assert!(parse_basic("Bearer x").is_none());
        assert!(parse_basic("Basic !!!").is_none());
        let h = format!("basic {}", STANDARD.encode("nopassword"));
        assert!(parse_basic(&h).is_none());
    }
}
