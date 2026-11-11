//! SprigDB tokens and sessions (chapter 14, "SprigDB tokens and sessions";
//! build step 12), as Studio's login uses them (chapter 44).
//!
//! ```text
//! login(principal):  token_sessions row (sid, SHA256(refresh), expires_at)
//!                    access  = "sprigat_" + JWS EdDSA {iss, aud, sub, ten,
//!                              sid, amr, cli, iat, exp}, kid = this gateway's key
//!                    refresh = "<tenant>.<sid>.<32 random bytes>", base64url
//! verify(access):    any gateway: kid → public key (token-keys/), signature,
//!                    aud, exp; the session live in the tenant's auth cache;
//!                    the account checked now (SessionStore::principal)
//! refresh(token):    on the catalog writer, under a row lock: current hash →
//!                    rotate; previous hash → reuse, revoke; idle, max age
//! logout:            revoke (an auth change: every gateway refuses at once)
//! ```
//!
//! The access token keeps the `sprigat_` prefix of R0's opaque tokens
//! (decision ST17), so `Core::caller` tells it from an API key and secret
//! scanners find it. Export tickets are JWS of `typ: sprig-export` signed
//! with the same keys ([`Tokens::sign_ticket`]).

pub mod jws;
pub mod keys;
pub mod sessions;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sprig_common::{Result, SprigCode, SprigError};

pub use self::keys::KeyRing;
pub use self::sessions::{
    CatalogSessions, MemorySessions, NewSession, RefreshRules, Rotated, SessionStore, TokenSession,
    Who,
};
use crate::mysql::Principal;

/// The access token's prefix.
pub const ACCESS_PREFIX: &str = "sprigat_";
/// `iss` and `aud` of every token.
pub const ISSUER: &str = "sprigdb";
/// Clock skew allowed on `exp` and `iat` (`auth.token_clock_skew_seconds`'
/// default).
pub const SKEW: Duration = Duration::from_secs(60);
/// A session's `last_used_at` is written at most this often per gateway.
pub const TOUCH_EVERY: Duration = Duration::from_secs(60);

/// The claims of an access token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claims {
    /// `sprigdb`.
    pub iss: String,
    /// `sprigdb`.
    pub aud: String,
    /// `<tenant_id>:<user_id>`.
    pub sub: String,
    /// The tenant's name (`-` for none).
    pub ten: String,
    /// The session id, base64url.
    pub sid: String,
    /// Authentication methods.
    pub amr: Vec<String>,
    /// `studio`, `api` or `cli`.
    pub cli: String,
    /// Issued at, Unix seconds.
    pub iat: u64,
    /// Expires at, Unix seconds.
    pub exp: u64,
    /// Impersonation (chapter 14 step 14): the administrator acting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub act: Option<Act>,
}

/// The `act` claim of an impersonation session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Act {
    /// The administrator, `<tenant>.<user>`.
    pub sub: String,
    /// The administrator's tenant id and user id, `<tenant_id>:<user_id>`.
    pub id: String,
}

/// The idle and age limits in force.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// `studio.session_idle_minutes`.
    pub idle: Duration,
    /// The smaller of `studio.session_max_hours` and
    /// `auth.refresh_ttl_hours`.
    pub max_age: Duration,
    /// `auth.token_ttl_seconds`.
    pub access_ttl: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            idle: Duration::from_secs(30 * 60),
            max_age: Duration::from_secs(12 * 3600),
            access_ttl: Duration::from_secs(900),
        }
    }
}

/// Tokens handed to the client.
#[derive(Clone)]
pub struct Issued {
    /// The session id, base64url.
    pub sid: String,
    /// `sprigat_<JWS>`, for `Authorization: Bearer`.
    pub access_token: String,
    /// Seconds until the access token expires.
    pub expires_in: u64,
    /// `<tenant>.<sid>.<secret>`, for the cookie or the body.
    pub refresh_token: String,
}

impl std::fmt::Debug for Issued {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Issued")
            .field("sid", &self.sid)
            .field("expires_in", &self.expires_in)
            .finish_non_exhaustive()
    }
}

/// A verified access token.
#[derive(Debug, Clone)]
pub struct Verified {
    /// Who.
    pub principal: Principal,
    /// Its claims.
    pub claims: Claims,
    /// When the session began, Unix milliseconds.
    pub created_ms: u64,
}

fn now_secs(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

fn ms(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn hash(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

fn random<const N: usize>() -> Result<[u8; N]> {
    let mut b = [0u8; N];
    getrandom::fill(&mut b)
        .map_err(|e| SprigError::internal(format!("no randomness for a token: {e}")))?;
    Ok(b)
}

fn denied(why: &str) -> SprigError {
    sessions::denied(why)
}

/// A refresh token, split: `(tenant, sid)`.
#[must_use]
pub fn parse_refresh(token: &str) -> Option<(&str, [u8; 16])> {
    let mut parts = token.split('.');
    let (Some(t), Some(sid), Some(secret), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return None;
    };
    if t.is_empty() || t.len() > 63 || secret.len() < 40 {
        return None;
    }
    let sid: [u8; 16] = URL_SAFE_NO_PAD.decode(sid).ok()?.try_into().ok()?;
    Some((t, sid))
}

/// Signs, verifies, refreshes and ends token sessions.
pub struct Tokens {
    keys: Arc<KeyRing>,
    store: Arc<dyn SessionStore>,
    touched: Mutex<HashMap<[u8; 16], Instant>>,
}

impl std::fmt::Debug for Tokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tokens")
            .field("keys", &self.keys)
            .finish_non_exhaustive()
    }
}

impl Tokens {
    /// Tokens signed with `keys`, sessions in `store`.
    #[must_use]
    pub fn new(keys: Arc<KeyRing>, store: Arc<dyn SessionStore>) -> Self {
        Self {
            keys,
            store,
            touched: Mutex::new(HashMap::new()),
        }
    }

    /// The signing keys.
    #[must_use]
    pub fn keys(&self) -> &Arc<KeyRing> {
        &self.keys
    }

    /// Where sessions live.
    #[must_use]
    pub fn store(&self) -> Arc<dyn SessionStore> {
        Arc::clone(&self.store)
    }

    fn access(&self, claims: &Claims) -> Result<String> {
        let k = self.keys.signer();
        let jws = jws::encode(&k.kid, jws::TYP_ACCESS, claims, |m| k.sign(m))?;
        Ok(format!("{ACCESS_PREFIX}{jws}"))
    }

    #[allow(clippy::too_many_arguments)] // the claims, one by one
    fn claims(
        tenant: &str,
        tenant_id: u64,
        user_id: u32,
        sid: &str,
        amr: Vec<String>,
        client: &str,
        now: SystemTime,
        ttl: Duration,
    ) -> Claims {
        let iat = now_secs(now);
        Claims {
            iss: ISSUER.to_owned(),
            aud: ISSUER.to_owned(),
            sub: format!("{tenant_id}:{user_id}"),
            ten: tenant.to_owned(),
            sid: sid.to_owned(),
            amr,
            cli: client.to_owned(),
            iat,
            exp: iat + ttl.as_secs(),
            act: None,
        }
    }

    /// Starts a session for `principal` after a password login.
    ///
    /// # Errors
    /// The store's.
    pub async fn login(
        &self,
        principal: Principal,
        client: &str,
        limits: Limits,
        now: SystemTime,
    ) -> Result<Issued> {
        self.issue(principal, client, limits, now, vec!["pwd".to_owned()], None)
            .await
    }

    /// Starts a session with the methods `amr` (`pwd`, `otp`, `rec`), and
    /// for an impersonation the administrator acting (chapter 14 steps 13
    /// and 14). An impersonation session has no refresh token: its access
    /// token lives `limits.access_ttl` (the impersonation's length) and the
    /// session row is checked on every use, so it can still be revoked.
    ///
    /// # Errors
    /// The store's.
    pub async fn issue(
        &self,
        principal: Principal,
        client: &str,
        limits: Limits,
        now: SystemTime,
        amr: Vec<String>,
        act: Option<(Act, sessions::Impersonator)>,
    ) -> Result<Issued> {
        let sid_bytes: [u8; 16] = *uuid::Uuid::now_v7().as_bytes();
        let sid = URL_SAFE_NO_PAD.encode(sid_bytes);
        let tenant = principal.tenant_name.clone().unwrap_or_else(|| "-".into());
        let refresh_token = format!("{tenant}.{sid}.{}", URL_SAFE_NO_PAD.encode(random::<32>()?));
        let (act, impersonator) = act.map_or((None, None), |(a, i)| (Some(a), Some(i)));
        let mut claims = Self::claims(
            &tenant,
            principal.tenant.map_or(0, |t| t.0),
            principal.user_id.map_or(0, |u| u.0),
            &sid,
            amr.clone(),
            client,
            now,
            limits.access_ttl,
        );
        claims.act = act;
        let kind = if impersonator.is_some() {
            "impersonation"
        } else {
            "login"
        };
        let expires_at = if impersonator.is_some() {
            now + limits.access_ttl
        } else {
            now + limits.max_age
        };
        let refresh_token = if impersonator.is_some() {
            String::new()
        } else {
            refresh_token
        };
        self.store
            .create(&NewSession {
                sid: sid_bytes,
                principal,
                refresh_sha256: hash(&if refresh_token.is_empty() {
                    URL_SAFE_NO_PAD.encode(random::<32>()?)
                } else {
                    refresh_token.clone()
                }),
                amr,
                client: client.to_owned(),
                expires_at,
                now,
                impersonator,
            })
            .await?;
        metrics::counter!("sprig_auth_tokens_issued_total", "kind" => kind).increment(1);
        Ok(Issued {
            sid,
            access_token: self.access(&claims)?,
            expires_in: limits.access_ttl.as_secs(),
            refresh_token,
        })
    }

    /// The claims of a token whose signature, audience and times check
    /// out (the session isn't looked at).
    ///
    /// # Errors
    /// 1045.
    pub async fn claims_of(&self, token: &str, now: SystemTime) -> Result<Claims> {
        let jws = token
            .strip_prefix(ACCESS_PREFIX)
            .ok_or_else(|| denied("not a SprigDB token"))?;
        let p = jws::parse(jws, jws::TYP_ACCESS)?;
        let Some(public) = self.keys.public_key(&p.header.kid, now).await? else {
            return Err(denied("the token's signing key is unknown or retired"));
        };
        jws::verify_signature(&p, &public)?;
        let c: Claims =
            serde_json::from_slice(&p.payload).map_err(|_| denied("the token is malformed"))?;
        let t = now_secs(now);
        if c.aud != ISSUER || c.iss != ISSUER {
            return Err(denied("the token is for another audience"));
        }
        if c.exp + SKEW.as_secs() <= t {
            return Err(denied("the access token expired; refresh it"));
        }
        if c.iat > t + SKEW.as_secs() {
            return Err(denied("the token is from the future"));
        }
        Ok(c)
    }

    /// Verifies an access token on any gateway: signature, times, the
    /// session live, the account allowed now.
    ///
    /// # Errors
    /// 1045 and the account's refusals (3118, 3955, 39303); 39301.
    pub async fn verify(
        &self,
        token: &str,
        client_ip: IpAddr,
        tls: bool,
        now: SystemTime,
    ) -> Result<Verified> {
        let claims = self.claims_of(token, now).await?;
        let (tid, uid) = claims
            .sub
            .split_once(':')
            .and_then(|(t, u)| Some((t.parse::<u64>().ok()?, u.parse::<u32>().ok()?)))
            .ok_or_else(|| denied("the token is malformed"))?;
        let sid: [u8; 16] = URL_SAFE_NO_PAD
            .decode(&claims.sid)
            .ok()
            .and_then(|b| b.try_into().ok())
            .ok_or_else(|| denied("the token is malformed"))?;
        let (principal, created) = self
            .store
            .principal(
                &Who {
                    tenant: &claims.ten,
                    tenant_id: tid,
                    user_id: uid,
                    sid,
                    client_ip,
                    tls,
                },
                now,
            )
            .await?;
        self.maybe_touch(&claims.ten, sid, now);
        Ok(Verified {
            principal,
            claims,
            created_ms: ms(created),
        })
    }

    fn maybe_touch(&self, tenant: &str, sid: [u8; 16], now: SystemTime) {
        let due = {
            let mut t = self.touched.lock().unwrap_or_else(PoisonError::into_inner);
            if t.len() > 100_000 {
                t.retain(|_, at| at.elapsed() < TOUCH_EVERY);
            }
            match t.get(&sid) {
                Some(at) if at.elapsed() < TOUCH_EVERY => false,
                _ => {
                    t.insert(sid, Instant::now());
                    true
                }
            }
        };
        if due && let Ok(h) = tokio::runtime::Handle::try_current() {
            let store = Arc::clone(&self.store);
            let tenant = tenant.to_owned();
            h.spawn(async move { store.touch(&tenant, sid, now).await });
        }
    }

    /// Rotates a refresh token: a new access token and refresh token.
    ///
    /// # Errors
    /// 1045 for an unknown or reused token (reuse revokes the session);
    /// 39924 idle; 39925 too old; the store's.
    pub async fn refresh(&self, token: &str, limits: Limits, now: SystemTime) -> Result<Issued> {
        let Some((tenant, sid_bytes)) = parse_refresh(token) else {
            return Err(denied("the refresh token is not valid; sign in again"));
        };
        let sid = URL_SAFE_NO_PAD.encode(sid_bytes);
        let next = format!("{tenant}.{sid}.{}", URL_SAFE_NO_PAD.encode(random::<32>()?));
        let r = self
            .store
            .rotate(
                tenant,
                sid_bytes,
                hash(token),
                hash(&next),
                RefreshRules {
                    idle: limits.idle,
                    max_age: limits.max_age,
                },
                now,
            )
            .await?;
        match r {
            Rotated::Ok {
                tenant_id,
                user_id,
                amr,
                client,
                ..
            } => {
                let claims = Self::claims(
                    tenant,
                    tenant_id,
                    user_id.map_or(0, |u| u.0),
                    &sid,
                    amr,
                    &client,
                    now,
                    limits.access_ttl,
                );
                metrics::counter!("sprig_auth_tokens_issued_total", "kind" => "refresh")
                    .increment(1);
                Ok(Issued {
                    sid,
                    access_token: self.access(&claims)?,
                    expires_in: limits.access_ttl.as_secs(),
                    refresh_token: next,
                })
            }
            Rotated::Reused => {
                tracing::warn!(target: "sprig_audit", event = "auth.session_revoked", sid = %sid,
                    tenant = %tenant, reason = "refresh_reuse", "refresh token reused: session revoked");
                Err(denied("the refresh token was already used; sign in again"))
            }
            Rotated::Invalid => Err(denied("the refresh token is not valid; sign in again")),
            Rotated::Idle => Err(SprigError::new(
                SprigCode::StudioSessionIdle,
                "the session was idle longer than studio.session_idle_minutes; sign in again",
            )),
            Rotated::Expired => Err(SprigError::new(
                SprigCode::StudioSessionExpired,
                "the session is older than studio.session_max_hours; sign in again",
            )),
        }
    }

    /// Ends the session a refresh token names (its secret must be the
    /// current or the previous one; anything else ends nothing). Returns
    /// the session id when one ended.
    ///
    /// # Errors
    /// The store's.
    pub async fn logout_refresh(&self, token: &str, now: SystemTime) -> Result<Option<String>> {
        let Some((tenant, sid)) = parse_refresh(token) else {
            return Ok(None);
        };
        // Prove the token by rotating it to a dead value first: a forged
        // cookie naming someone else's sid ends nothing.
        let dead = random::<32>()?;
        let r = self
            .store
            .rotate(
                tenant,
                sid,
                hash(token),
                dead,
                RefreshRules {
                    idle: Duration::MAX,
                    max_age: Duration::MAX,
                },
                now,
            )
            .await?;
        match r {
            Rotated::Ok { .. } => {
                self.store.revoke(tenant, sid, now).await?;
                Ok(Some(URL_SAFE_NO_PAD.encode(sid)))
            }
            Rotated::Reused | Rotated::Idle | Rotated::Expired => {
                Ok(Some(URL_SAFE_NO_PAD.encode(sid)))
            }
            Rotated::Invalid => Ok(None),
        }
    }

    /// Ends the session of a verified access token.
    ///
    /// # Errors
    /// The store's.
    pub async fn logout_access(&self, claims: &Claims, now: SystemTime) -> Result<bool> {
        let Some(sid) = URL_SAFE_NO_PAD
            .decode(&claims.sid)
            .ok()
            .and_then(|b| <[u8; 16]>::try_from(b).ok())
        else {
            return Ok(false);
        };
        self.store.revoke(&claims.ten, sid, now).await
    }

    /// Signs an export ticket (chapter 44: `typ: sprig-export`).
    ///
    /// # Errors
    /// `Internal`.
    pub fn sign_ticket<T: Serialize>(&self, payload: &T) -> Result<String> {
        let k = self.keys.signer();
        jws::encode(&k.kid, jws::TYP_EXPORT, payload, |m| k.sign(m))
    }

    /// Verifies an export ticket's signature (any gateway) and decodes it;
    /// the caller checks its expiry and session.
    ///
    /// # Errors
    /// 1045 for a bad signature or an unknown key.
    pub async fn verify_ticket<T: DeserializeOwned>(
        &self,
        ticket: &str,
        now: SystemTime,
    ) -> Result<T> {
        let p = jws::parse(ticket, jws::TYP_EXPORT)?;
        let Some(public) = self.keys.public_key(&p.header.kid, now).await? else {
            return Err(denied("the ticket's signing key is unknown"));
        };
        jws::verify_signature(&p, &public)?;
        serde_json::from_slice(&p.payload).map_err(|_| denied("the ticket is malformed"))
    }

    /// Whether the session of a ticket is still live: the account checked
    /// as for an access token.
    ///
    /// # Errors
    /// As [`SessionStore::principal`].
    pub async fn session_principal(
        &self,
        who: &Who<'_>,
        now: SystemTime,
    ) -> Result<(Principal, SystemTime)> {
        self.store.principal(who, now).await
    }
}
