//! Token sessions (the catalog's `token_sessions`): where a session lives, so
//! any gateway can verify, refresh and end it.
//!
//! [`CatalogSessions`] keeps them in the tenant catalog's `token_sessions`
//! (the auth module's format 4):
//!
//! - **create** is an auth change (the row with the change's version), so
//!   every gateway's [`TenantAuth`] learns the session;
//! - **verify** looks the session up in the cached [`TenantAuth`] (not
//!   revoked, not expired), then checks the account as an API key does
//!   (live, not locked, tenant not suspended, TLS, password unchanged since
//!   the session began). A session too new for this gateway's snapshot is
//!   read once from the catalog writer;
//! - **refresh** is a plain transaction on the catalog writer: `SELECT ...
//!   FOR UPDATE` serializes refreshes from any gateway, so the token
//!   presented is compared with the *current* `refresh_sha256` wherever the
//!   last refresh happened. Equal: both hashes rotate and `last_used_at`
//!   moves (not an auth change: nothing a cache holds changed). Equal to
//!   `refresh_prev`: reuse means theft, so the session is revoked in an
//!   auth change and every gateway refuses its access tokens within
//!   moments;
//! - **logout** revokes in an auth change;
//! - **`last_used_at`** moves on refresh and, at most once a minute per
//!   session and gateway, when an access token is used.
//!
//! [`MemorySessions`] keeps them in the process, for logins that don't come
//! from a tenant catalog (the accounts file) and for tests.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use mysql_async::{Row, Value};
use sprig_catalog::migrate::MigrateOpts;
use sprig_catalog::{ChangeOpts, MariaCatalog, Migrator, VersionKind};
use sprig_common::ids::{TenantId, UserId};
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};
use subtle::ConstantTimeEq as _;

use crate::cache::TenantAuth;
use crate::mysql::{LoginService, LoginSource, Principal, locked, temp_locked};
use crate::store::{AT, AuthStore, RequireTls, col, exec_count, map_err, micros, opt_time, time};
use sprig_proto_mysql::Persona;

/// `token_sessions`: the auth module's format 4.
pub const DDL_TOKEN_SESSIONS: &str = "CREATE TABLE IF NOT EXISTS token_sessions (
  session_id      BINARY(16)      NOT NULL,
  user_id         INT UNSIGNED    NOT NULL,
  amr             SET('pwd','otp','hwk','rec') NOT NULL,
  refresh_sha256  BINARY(32)      NOT NULL,
  refresh_prev    BINARY(32)      NULL,
  impersonator_tenant BIGINT UNSIGNED NULL,
  impersonator_user   INT UNSIGNED NULL,
  reason          VARCHAR(256)    NULL,
  expires_at      DATETIME(6)     NOT NULL,
  revoked_at      DATETIME(6)     NULL,
  client          ENUM('studio','api','cli') NOT NULL DEFAULT 'api',
  last_used_at    DATETIME(6)     NULL,
  created_at      DATETIME(6)     NOT NULL,
  changed_version BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (session_id),
  KEY k_user (user_id)
) ENGINE=InnoDB";

/// A session as caches keep it (no refresh hashes: those are read from the
/// writer when a refresh needs them).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenSession {
    /// The `sid` claim.
    pub sid: [u8; 16],
    /// The user.
    pub user_id: UserId,
    /// Authentication methods (`pwd`).
    pub amr: Vec<String>,
    /// `studio`, `api` or `cli`.
    pub client: String,
    /// When the session ends at the latest.
    pub expires_at: SystemTime,
    /// When it was revoked.
    pub revoked_at: Option<SystemTime>,
    /// When it began.
    pub created_at: SystemTime,
    /// The auth version of its last versioned change.
    pub changed_version: u64,
}

impl TokenSession {
    /// Not revoked and not past `expires_at`.
    #[must_use]
    pub fn live(&self, now: SystemTime) -> bool {
        self.revoked_at.is_none() && self.expires_at > now
    }
}

const COLS: &str = "session_id, user_id, amr, client, \
    TIMESTAMPDIFF(MICROSECOND, '1970-01-01', expires_at), \
    TIMESTAMPDIFF(MICROSECOND, '1970-01-01', revoked_at), \
    TIMESTAMPDIFF(MICROSECOND, '1970-01-01', created_at), changed_version";

fn table(store: &AuthStore) -> String {
    format!(
        "{}.token_sessions",
        sprig_common::names::quote(store.schema())
    )
}

fn session_row(mut r: Row) -> Result<TokenSession> {
    let sid: Vec<u8> = col(&mut r, 0, "session_id")?;
    let amr: Vec<u8> = col(&mut r, 2, "amr")?;
    let client: Vec<u8> = col(&mut r, 3, "client")?;
    Ok(TokenSession {
        sid: sid
            .try_into()
            .map_err(|_| SprigError::internal("auth catalog column session_id: not 16 bytes"))?,
        user_id: UserId(col(&mut r, 1, "user_id")?),
        amr: String::from_utf8_lossy(&amr)
            .split(',')
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect(),
        client: String::from_utf8_lossy(&client).into_owned(),
        expires_at: time(&mut r, 4, "expires_at")?,
        revoked_at: opt_time(&mut r, 5, "revoked_at")?,
        created_at: time(&mut r, 6, "created_at")?,
        changed_version: col(&mut r, 7, "changed_version")?,
    })
}

/// Sessions changed after `since`; with `None`, every live one. A catalog
/// below the auth module's format 4 has no `token_sessions`: none.
///
/// # Errors
/// A backend error; `Internal` for a malformed row.
pub async fn sessions_since<Q: Queryable + Send>(
    store: &AuthStore,
    q: &mut Q,
    since: Option<u64>,
) -> Result<Vec<TokenSession>> {
    let rows: std::result::Result<Vec<Row>, _> = match since {
        Some(v) => {
            q.exec(
                format!(
                    "SELECT {COLS} FROM {} WHERE changed_version > ?",
                    table(store)
                ),
                (v,),
            )
            .await
        }
        None => {
            q.query(format!(
                "SELECT {COLS} FROM {} WHERE revoked_at IS NULL AND expires_at > UTC_TIMESTAMP(6)",
                table(store)
            ))
            .await
        }
    };
    match rows {
        Ok(rows) => rows.into_iter().map(session_row).collect(),
        Err(mysql_async::Error::Server(s)) if s.code == 1146 => Ok(Vec::new()),
        Err(e) => Err(map_err(e)),
    }
}

/// A session to create.
#[derive(Debug, Clone)]
pub struct NewSession {
    /// The `sid`.
    pub sid: [u8; 16],
    /// Who signed in.
    pub principal: Principal,
    /// `SHA256` of the first refresh token.
    pub refresh_sha256: [u8; 32],
    /// `pwd`, ...
    pub amr: Vec<String>,
    /// `studio`, `api` or `cli`.
    pub client: String,
    /// The session's end at the latest.
    pub expires_at: SystemTime,
    /// Now.
    pub now: SystemTime,
    /// For an impersonation session: who acts, why.
    pub impersonator: Option<Impersonator>,
}

/// The administrator behind an impersonation session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Impersonator {
    /// The administrator's tenant id.
    pub tenant_id: u64,
    /// The administrator's user id.
    pub user_id: u32,
    /// The reason given (required).
    pub reason: String,
}

/// What a refresh found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rotated {
    /// The token was the current one: rotated to the new hash.
    Ok {
        /// The tenant, `0` for none.
        tenant_id: u64,
        /// The user.
        user_id: Option<UserId>,
        /// When the session began.
        created_at: SystemTime,
        /// `amr`.
        amr: Vec<String>,
        /// `client`.
        client: String,
    },
    /// The token was the previous one: the session is now revoked.
    Reused,
    /// No such live session, or neither hash matches.
    Invalid,
    /// Idle longer than the limit: the session is now revoked.
    Idle,
    /// Older than the limit: the session is now revoked.
    Expired,
}

/// The checks a refresh makes besides the token.
#[derive(Debug, Clone, Copy)]
pub struct RefreshRules {
    /// Refused when `last_used_at` is older than this.
    pub idle: Duration,
    /// Refused when the session is older than this.
    pub max_age: Duration,
}

/// Where sessions live.
#[async_trait]
pub trait SessionStore: Send + Sync + 'static {
    /// Stores a new session.
    ///
    /// # Errors
    /// The store's; 39301 when the catalog writer can't be reached.
    async fn create(&self, s: &NewSession) -> Result<()>;

    /// Rotates `tenant`'s session `sid` from `presented` to `next` when
    /// `presented` is its current refresh hash (see [`Rotated`]).
    ///
    /// # Errors
    /// The store's.
    async fn rotate(
        &self,
        tenant: &str,
        sid: [u8; 16],
        presented: [u8; 32],
        next: [u8; 32],
        rules: RefreshRules,
        now: SystemTime,
    ) -> Result<Rotated>;

    /// Ends a session. Returns whether a live one ended.
    ///
    /// # Errors
    /// The store's.
    async fn revoke(&self, tenant: &str, sid: [u8; 16], now: SystemTime) -> Result<bool>;

    /// The principal of a live session, its account checked now: `tenant`
    /// and `tenant_id` from the token, `user_id` and `sid` from it too.
    ///
    /// # Errors
    /// 1045 when the session or account no longer allows it; 39303
    /// suspended; 3118 locked; 39301 when the catalog can't be read.
    async fn principal(&self, who: &Who<'_>, now: SystemTime) -> Result<(Principal, SystemTime)>;

    /// Records a use of the session (at most once a minute per gateway is
    /// enough; the caller limits).
    async fn touch(&self, tenant: &str, sid: [u8; 16], now: SystemTime);
}

/// Who a verified token names.
#[derive(Debug, Clone, Copy)]
pub struct Who<'a> {
    /// The tenant's name (`-` for none).
    pub tenant: &'a str,
    /// Its id, `0` for none.
    pub tenant_id: u64,
    /// The user's id, `0` for none.
    pub user_id: u32,
    /// The session.
    pub sid: [u8; 16],
    /// The client's address.
    pub client_ip: IpAddr,
    /// Over TLS.
    pub tls: bool,
}

pub(crate) fn denied(why: &str) -> SprigError {
    SprigError::standard(1045, *b"28000", format!("Access denied: {why}"))
}

fn packet_error(p: sprig_proto_mysql::ErrPacket) -> SprigError {
    SprigError::standard(p.code, p.sqlstate, p.message)
}

fn ct_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    bool::from(a.ct_eq(b))
}

// ---- in memory ----

#[derive(Clone)]
struct MemSession {
    principal: Principal,
    refresh: [u8; 32],
    refresh_prev: Option<[u8; 32]>,
    amr: Vec<String>,
    client: String,
    created_at: SystemTime,
    expires_at: SystemTime,
    last_used: SystemTime,
}

/// Sessions in this process: a token works only here, and a restart ends
/// them. For the accounts file and tests.
#[derive(Default)]
pub struct MemorySessions {
    inner: Mutex<HashMap<[u8; 16], MemSession>>,
}

impl std::fmt::Debug for MemorySessions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemorySessions")
            .field("open", &self.len())
            .finish()
    }
}

/// Sessions kept at most in memory; the least recently used go first.
pub const MAX_MEMORY_SESSIONS: usize = 100_000;

impl MemorySessions {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<[u8; 16], MemSession>> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Open sessions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// None open.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl SessionStore for MemorySessions {
    async fn create(&self, s: &NewSession) -> Result<()> {
        let mut g = self.lock();
        g.retain(|_, m| m.expires_at > s.now);
        if g.len() >= MAX_MEMORY_SESSIONS
            && let Some(oldest) = g.iter().min_by_key(|(_, m)| m.last_used).map(|(k, _)| *k)
        {
            g.remove(&oldest);
        }
        g.insert(
            s.sid,
            MemSession {
                principal: s.principal.clone(),
                refresh: s.refresh_sha256,
                refresh_prev: None,
                amr: s.amr.clone(),
                client: s.client.clone(),
                created_at: s.now,
                expires_at: s.expires_at,
                last_used: s.now,
            },
        );
        Ok(())
    }

    async fn rotate(
        &self,
        _tenant: &str,
        sid: [u8; 16],
        presented: [u8; 32],
        next: [u8; 32],
        rules: RefreshRules,
        now: SystemTime,
    ) -> Result<Rotated> {
        let mut g = self.lock();
        let Some(m) = g.get_mut(&sid) else {
            return Ok(Rotated::Invalid);
        };
        if m.refresh_prev.is_some_and(|p| ct_eq(&p, &presented)) {
            g.remove(&sid);
            return Ok(Rotated::Reused);
        }
        if !ct_eq(&m.refresh, &presented) {
            return Ok(Rotated::Invalid);
        }
        if now.duration_since(m.created_at).unwrap_or_default() >= rules.max_age
            || m.expires_at <= now
        {
            g.remove(&sid);
            return Ok(Rotated::Expired);
        }
        if now.duration_since(m.last_used).unwrap_or_default() >= rules.idle {
            g.remove(&sid);
            return Ok(Rotated::Idle);
        }
        m.refresh_prev = Some(m.refresh);
        m.refresh = next;
        m.last_used = now;
        Ok(Rotated::Ok {
            tenant_id: m.principal.tenant.map_or(0, |t| t.0),
            user_id: m.principal.user_id,
            created_at: m.created_at,
            amr: m.amr.clone(),
            client: m.client.clone(),
        })
    }

    async fn revoke(&self, _tenant: &str, sid: [u8; 16], _now: SystemTime) -> Result<bool> {
        Ok(self.lock().remove(&sid).is_some())
    }

    async fn principal(&self, who: &Who<'_>, now: SystemTime) -> Result<(Principal, SystemTime)> {
        let g = self.lock();
        match g.get(&who.sid) {
            Some(m) if m.expires_at > now => {
                let mut p = m.principal.clone();
                p.client_ip = who.client_ip;
                Ok((p, m.created_at))
            }
            _ => Err(denied("the session ended; sign in again")),
        }
    }

    async fn touch(&self, _tenant: &str, sid: [u8; 16], now: SystemTime) {
        if let Some(m) = self.lock().get_mut(&sid) {
            m.last_used = now;
        }
    }
}

// ---- in the tenant catalog ----

/// Sessions in the tenant catalogs (format 4 of the auth module).
pub struct CatalogSessions {
    login: Arc<LoginService>,
    cat: MariaCatalog,
}

impl std::fmt::Debug for CatalogSessions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogSessions").finish_non_exhaustive()
    }
}

fn unavailable(what: &str, e: &SprigError) -> SprigError {
    tracing::warn!(error = %e, "token sessions: {what}");
    SprigError::new(
        SprigCode::AuthUnavailable,
        "authentication unavailable; retry",
    )
}

impl CatalogSessions {
    /// Sessions of the tenants `login` knows, written through `cat`.
    #[must_use]
    pub fn new(login: Arc<LoginService>, cat: MariaCatalog) -> Self {
        Self { login, cat }
    }

    fn logins(&self) -> Result<&Arc<crate::mysql::CatalogLogins>> {
        match self.login.source() {
            LoginSource::Catalog(c) => Ok(c),
            LoginSource::File(_) => Err(denied("token sessions need tenant catalogs")),
        }
    }

    async fn tenant_id(&self, name: &str) -> Result<TenantId> {
        match self.logins()?.tenants().lookup(name).await {
            Ok(Some(t)) => Ok(t.id),
            Ok(None) => Err(denied("the tenant no longer exists")),
            Err(e) => Err(unavailable("tenant directory unreadable", &e)),
        }
    }

    /// Brings the tenant's auth module to format 4 (`token_sessions`).
    async fn migrate(&self, tenant: TenantId) -> Result<()> {
        let ep = self.cat.backend().locator().writer(tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(map_err)?;
        Migrator::new(&[crate::store::AUTH_MODULE])?
            .run(&mut conn, ep.store.schema(), &MigrateOpts::default())
            .await?;
        tracing::info!(%tenant, "auth module migrated for token sessions");
        Ok(())
    }

    async fn insert(&self, tenant: TenantId, s: &NewSession) -> Result<()> {
        let user = s
            .principal
            .user_id
            .ok_or_else(|| denied("token sessions need a catalog user"))?;
        let mut c = self
            .cat
            .change_with(tenant, VersionKind::Auth, ChangeOpts::default())
            .await?;
        let version = c.version();
        let r = {
            let (core, q) = c.tx().parts();
            let store = AuthStore::of(core);
            // Expired and long-revoked rows go now and then (no version:
            // caches never treat a missing row as live).
            let _ = exec_count(
                q,
                format!(
                    "DELETE FROM {} WHERE expires_at < UTC_TIMESTAMP(6) - INTERVAL 1 DAY \
                     OR revoked_at < UTC_TIMESTAMP(6) - INTERVAL 1 DAY LIMIT 100",
                    table(&store)
                ),
                Vec::new(),
            )
            .await;
            exec_count(
                q,
                format!(
                    "INSERT INTO {} (session_id, user_id, amr, refresh_sha256, expires_at, \
                     client, last_used_at, created_at, changed_version, impersonator_tenant, \
                     impersonator_user, reason) \
                     VALUES (?, ?, ?, ?, {AT}, ?, {AT}, {AT}, ?, ?, ?, ?)",
                    table(&store)
                ),
                vec![
                    Value::from(s.sid.to_vec()),
                    Value::from(user.0),
                    Value::from(s.amr.join(",")),
                    Value::from(s.refresh_sha256.to_vec()),
                    Value::from(micros(s.expires_at)),
                    Value::from(s.client.as_str()),
                    Value::from(micros(s.now)),
                    Value::from(micros(s.now)),
                    Value::from(version),
                    Value::from(s.impersonator.as_ref().map(|i| i.tenant_id)),
                    Value::from(s.impersonator.as_ref().map(|i| i.user_id)),
                    Value::from(
                        s.impersonator
                            .as_ref()
                            .map(|i| i.reason.chars().take(256).collect::<String>()),
                    ),
                ],
            )
            .await
        };
        match r {
            Ok(_) => {
                c.commit().await?;
                Ok(())
            }
            Err(e) => {
                let _ = c.rollback().await;
                let _ = self.cat.settle(tenant, VersionKind::Auth).await;
                Err(e)
            }
        }
    }

    async fn revoke_in_change(
        &self,
        tenant: TenantId,
        sid: [u8; 16],
        now: SystemTime,
    ) -> Result<bool> {
        let mut c = self
            .cat
            .change_with(tenant, VersionKind::Auth, ChangeOpts::default())
            .await?;
        let version = c.version();
        let r = {
            let (core, q) = c.tx().parts();
            let store = AuthStore::of(core);
            exec_count(
                q,
                format!(
                    "UPDATE {} SET revoked_at = {AT}, changed_version = ? \
                     WHERE session_id = ? AND revoked_at IS NULL",
                    table(&store)
                ),
                vec![
                    Value::from(micros(now)),
                    Value::from(version),
                    Value::from(sid.to_vec()),
                ],
            )
            .await
        };
        match r {
            Ok(n) if n > 0 => {
                c.commit().await?;
                Ok(true)
            }
            Ok(_) => {
                let _ = c.rollback().await;
                let _ = self.cat.settle(tenant, VersionKind::Auth).await;
                Ok(false)
            }
            Err(e) => {
                let _ = c.rollback().await;
                let _ = self.cat.settle(tenant, VersionKind::Auth).await;
                Err(e)
            }
        }
    }

    /// The session row from the writer, for a session newer than this
    /// gateway's snapshot.
    async fn read_session(&self, tenant: TenantId, sid: [u8; 16]) -> Result<Option<TokenSession>> {
        let ep = self.cat.backend().locator().writer(tenant).await?;
        let store = AuthStore::of(&ep.store);
        let mut conn = ep.pool.get_conn().await.map_err(map_err)?;
        let rows: std::result::Result<Vec<Row>, _> = conn
            .exec(
                format!("SELECT {COLS} FROM {} WHERE session_id = ?", table(&store)),
                (sid.to_vec(),),
            )
            .await;
        match rows {
            Ok(rows) => rows.into_iter().next().map(session_row).transpose(),
            Err(mysql_async::Error::Server(s)) if s.code == 1146 => Ok(None),
            Err(e) => Err(map_err(e)),
        }
    }
}

#[async_trait]
impl SessionStore for CatalogSessions {
    async fn create(&self, s: &NewSession) -> Result<()> {
        let tenant = s
            .principal
            .tenant
            .ok_or_else(|| denied("token sessions need a tenant login"))?;
        match self.insert(tenant, s).await {
            Err(e) if matches!(e.kind, ErrorKind::Backend { errno: 1146, .. }) => {
                self.migrate(tenant).await?;
                self.insert(tenant, s).await
            }
            r => r,
        }
    }

    #[allow(clippy::too_many_lines)] // one transaction, its outcomes in order
    async fn rotate(
        &self,
        tenant: &str,
        sid: [u8; 16],
        presented: [u8; 32],
        next: [u8; 32],
        rules: RefreshRules,
        now: SystemTime,
    ) -> Result<Rotated> {
        let tid = self.tenant_id(tenant).await?;
        let ep = self
            .cat
            .backend()
            .locator()
            .writer(tid)
            .await
            .map_err(|e| unavailable("catalog writer unreachable", &e))?;
        let store = AuthStore::of(&ep.store);
        let mut conn = ep.pool.get_conn().await.map_err(map_err)?;
        let mut tx = conn
            .start_transaction(mysql_async::TxOpts::default())
            .await
            .map_err(map_err)?;
        let rows: std::result::Result<Vec<Row>, _> = tx
            .exec(
                format!(
                    "SELECT user_id, refresh_sha256, refresh_prev, amr, client, \
                     TIMESTAMPDIFF(MICROSECOND, '1970-01-01', expires_at), \
                     TIMESTAMPDIFF(MICROSECOND, '1970-01-01', revoked_at), \
                     TIMESTAMPDIFF(MICROSECOND, '1970-01-01', created_at), \
                     TIMESTAMPDIFF(MICROSECOND, '1970-01-01', last_used_at) \
                     FROM {} WHERE session_id = ? FOR UPDATE",
                    table(&store)
                ),
                (sid.to_vec(),),
            )
            .await;
        let rows = match rows {
            Ok(r) => r,
            Err(mysql_async::Error::Server(s)) if s.code == 1146 => return Ok(Rotated::Invalid),
            Err(e) => return Err(map_err(e)),
        };
        let Some(mut r) = rows.into_iter().next() else {
            let _ = tx.rollback().await;
            return Ok(Rotated::Invalid);
        };
        let user_id = UserId(col(&mut r, 0, "user_id")?);
        let current: Vec<u8> = col(&mut r, 1, "refresh_sha256")?;
        let prev: Option<Vec<u8>> = col(&mut r, 2, "refresh_prev")?;
        let amr: Vec<u8> = col(&mut r, 3, "amr")?;
        let client: Vec<u8> = col(&mut r, 4, "client")?;
        let expires_at = time(&mut r, 5, "expires_at")?;
        let revoked_at = opt_time(&mut r, 6, "revoked_at")?;
        let created_at = time(&mut r, 7, "created_at")?;
        let last_used = opt_time(&mut r, 8, "last_used_at")?.unwrap_or(created_at);
        let as32 = |v: &[u8]| -> [u8; 32] { v.try_into().unwrap_or([0; 32]) };
        if revoked_at.is_some() {
            let _ = tx.rollback().await;
            return Ok(Rotated::Invalid);
        }
        let outcome = if prev.as_deref().is_some_and(|p| ct_eq(&as32(p), &presented)) {
            Some(Rotated::Reused)
        } else if !ct_eq(&as32(&current), &presented) {
            let _ = tx.rollback().await;
            return Ok(Rotated::Invalid);
        } else if expires_at <= now
            || now.duration_since(created_at).unwrap_or_default() >= rules.max_age
        {
            Some(Rotated::Expired)
        } else if now.duration_since(last_used).unwrap_or_default() >= rules.idle {
            Some(Rotated::Idle)
        } else {
            None
        };
        if let Some(end) = outcome {
            let _ = tx.rollback().await;
            drop(conn);
            self.revoke_in_change(tid, sid, now).await?;
            return Ok(end);
        }
        exec_count(
            &mut tx,
            format!(
                "UPDATE {} SET refresh_prev = refresh_sha256, refresh_sha256 = ?, \
                 last_used_at = {AT} WHERE session_id = ?",
                table(&store)
            ),
            vec![
                Value::from(next.to_vec()),
                Value::from(micros(now)),
                Value::from(sid.to_vec()),
            ],
        )
        .await?;
        tx.commit().await.map_err(map_err)?;
        Ok(Rotated::Ok {
            tenant_id: tid.0,
            user_id: Some(user_id),
            created_at,
            amr: String::from_utf8_lossy(&amr)
                .split(',')
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect(),
            client: String::from_utf8_lossy(&client).into_owned(),
        })
    }

    async fn revoke(&self, tenant: &str, sid: [u8; 16], now: SystemTime) -> Result<bool> {
        let tid = self.tenant_id(tenant).await?;
        self.revoke_in_change(tid, sid, now).await
    }

    #[allow(clippy::too_many_lines)] // the checks, in order
    async fn principal(&self, who: &Who<'_>, now: SystemTime) -> Result<(Principal, SystemTime)> {
        let cl = self.logins()?;
        let tenant = match cl.tenants().lookup(who.tenant).await {
            Ok(Some(t)) if t.can_log_in && t.id.0 == who.tenant_id => t,
            Ok(_) => return Err(denied("the tenant no longer exists")),
            Err(e) => return Err(unavailable("tenant directory unreadable", &e)),
        };
        let catalog = cl
            .catalogs()
            .catalog(tenant.id)
            .await
            .map_err(|e| unavailable("catalog unreadable", &e))?;
        let Some(auth) = TenantAuth::of(&catalog) else {
            return Err(denied("the tenant has no accounts"));
        };
        let session = match auth.session(&who.sid) {
            Some(s) => (**s).clone(),
            // Newer than this snapshot (signed in on another gateway a
            // moment ago), or not there at all.
            None => self
                .read_session(tenant.id, who.sid)
                .await
                .map_err(|e| unavailable("catalog writer unreachable", &e))?
                .ok_or_else(|| denied("the session ended; sign in again"))?,
        };
        if !session.live(now) || session.user_id.0 != who.user_id {
            return Err(denied("the session ended; sign in again"));
        }
        let Some(user) = auth.user(session.user_id).cloned() else {
            return Err(denied("the account no longer exists"));
        };
        if user
            .row
            .password_changed_at
            .is_some_and(|t| t > session.created_at)
        {
            return Err(denied(
                "the password changed since the session began; sign in again",
            ));
        }
        let (name, host) = (user.row.name.as_str(), user.row.host.as_str());
        if let Some(lock) = auth
            .temp_lock(user.row.user_id)
            .filter(|l| l.locked_until > now)
        {
            return Err(packet_error(temp_locked(
                Persona::MySql84,
                name,
                host,
                &lock,
                now,
            )));
        }
        if tenant.suspended {
            return Err(SprigError::new(
                SprigCode::TenantSuspended,
                format!("tenant {} is suspended", tenant.name),
            ));
        }
        if user.row.account_locked {
            return Err(packet_error(locked(Persona::MySql84, name, host)));
        }
        let need_tls = user.row.require_tls != RequireTls::None || cl.require_tls(&catalog);
        if (need_tls && !who.tls) || user.row.require_tls == RequireTls::X509 {
            return Err(denied("the account requires TLS"));
        }
        let backend = cl.backend_of(&tenant, &user)?;
        Ok((
            Principal {
                tenant: Some(tenant.id),
                tenant_name: Some(tenant.name.clone()),
                user_id: Some(user.row.user_id),
                user: user.row.name.clone(),
                host: user.row.host.clone(),
                client_ip: who.client_ip,
                must_change_password: false,
                limits: user.row.limits,
                auth_version: catalog.auth_version,
                backend,
            },
            session.created_at,
        ))
    }

    async fn touch(&self, tenant: &str, sid: [u8; 16], now: SystemTime) {
        let r = async {
            let tid = self.tenant_id(tenant).await?;
            let ep = self.cat.backend().locator().writer(tid).await?;
            let store = AuthStore::of(&ep.store);
            let mut conn = ep.pool.get_conn().await.map_err(map_err)?;
            exec_count(
                &mut conn,
                format!(
                    "UPDATE {} SET last_used_at = {AT} WHERE session_id = ? \
                     AND revoked_at IS NULL",
                    table(&store)
                ),
                vec![Value::from(micros(now)), Value::from(sid.to_vec())],
            )
            .await
        };
        if let Err(e) = r.await {
            tracing::debug!(error = %e, "token session: last_used_at not written");
        }
    }
}
