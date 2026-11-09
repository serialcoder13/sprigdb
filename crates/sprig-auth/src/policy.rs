//! Lockout and password policies (chapter 14, "Lockout and password
//! policies"; build step 5).
//!
//! **Lockout.** Failed-login tracking is on when the account's
//! `FAILED_LOGIN_ATTEMPTS` (else `auth.failed_login_attempts`) and its lock
//! time (`password_lock_minutes`, else `auth.password_lock_minutes`) are
//! both non-zero ([`LockoutPolicy`]). Only wrong credentials for an
//! existing account count. After N in a row, `login_state.locked_until` is
//! set in an auth change, so every gateway's cache sees the lock at once;
//! the lock ends by itself when `locked_until` passes (the next login
//! clears it, also an auth change) or at `ACCOUNT UNLOCK`. `-1` locks until
//! `ACCOUNT UNLOCK` (MySQL's `UNBOUNDED`). Unlike MySQL the state survives
//! restarts and is shared by every gateway.
//!
//! ```text
//! record_failure(user):                 # a wrong password, the account exists
//!   tx on the catalog writer: SELECT login_state FOR UPDATE
//!   failures := failures + 1 (+ failures counted in memory while the writer was down)
//!   if failures < N: write failures, last_failure_at; commit
//!   else: in an auth change: write failures, locked_until = now + lock time
//! record_success(user):                 # only when tracking is on
//!   UPDATE login_state SET consecutive_failures = 0 WHERE ... > 0 AND not locked
//! unlock_expired(user, until):          # the login after locked_until passed
//!   in an auth change: clear the lock if it still ends at `until`
//! ```
//!
//! If the writer can't be reached, failures are counted in memory
//! ([`CatalogLoginState`]) and added to the next write that succeeds.
//!
//! **Passwords.** [`PasswordRules`] resolve strength (`auth.password_policy`,
//! the levels of MySQL's `validate_password`), history and reuse interval
//! (account clause, else `auth.password_history` and
//! `auth.password_reuse_interval_days`) and `PASSWORD REQUIRE CURRENT`
//! (account clause, else `auth.password_require_current`).
//! [`change_password`] checks them and sets the password in one call.
//!
//! **Account clauses within the tree's bounds.** An account clause
//! (`FAILED_LOGIN_ATTEMPTS 4`, `PASSWORD EXPIRE INTERVAL 30 DAY`) is
//! narrower than any tenant scope, and must lie within the bounds the
//! tenant's ancestors set ([`AccountClauses::validate`], else 39080).
//!
//! **Personas.** MySQL 8.4's errors on 3306; on 3307 MariaDB's: 4150 for a
//! temporary lock, 1819 for a reused password (MariaDB's
//! `password_reuse_check`), and no `REPLACE` clause, so `REQUIRE CURRENT`
//! doesn't apply there.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use sprig_catalog::{ChangeOpts, MariaCatalog, TenantCatalog, VersionKind};
use sprig_common::ids::{TenantId, UserId};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::Persona;
use sprig_settings::{Setting, SettingValue, Value};
use zeroize::Zeroizing;

use crate::admin::{self, Changed, Hashing, SetPassword};
use crate::audit::{Actor, AuditEvent, AuditSink, events};
use crate::mysql::PolicySettings;
use crate::settings::{
    FAILED_LOGIN_ATTEMPTS, PASSWORD_HISTORY, PASSWORD_LOCK_MINUTES, PASSWORD_POLICY,
    PASSWORD_REQUIRE_CURRENT, PASSWORD_REUSE_INTERVAL_DAYS,
};
use crate::store::{
    AuthStore, LoginStateRow, RequireCurrent, Slot, UNBOUNDED_LOCK_MICROS, UserRow, VerifierKind,
    VerifierRow, from_micros, map_err,
};
use crate::verifier::CachingSha2Verifier;

// ---- resolving keys ----

fn get<T: SettingValue>(p: &dyn PolicySettings, s: &'static Setting<T>, c: &TenantCatalog) -> T {
    p.resolve(s.key, c)
        .and_then(|v| T::from_value(&v))
        .unwrap_or_else(|| s.default.clone())
}

/// The tenant's [`Hashing`]: `auth.caching_sha2_rounds`,
/// `auth.mysql_native_password`, `auth.scram_sha256_iterations` and
/// `auth.scram_sha512_iterations`.
#[must_use]
pub fn hashing_of(p: &dyn PolicySettings, c: &TenantCatalog) -> Hashing {
    use crate::scram::DEFAULT_SCRAM_ITERATIONS;
    use crate::settings::{
        CACHING_SHA2_ROUNDS, MYSQL_NATIVE_PASSWORD, SCRAM_SHA256_ITERATIONS,
        SCRAM_SHA512_ITERATIONS,
    };
    Hashing {
        caching_sha2_rounds: get(p, &CACHING_SHA2_ROUNDS, c)
            .try_into()
            .unwrap_or(crate::verifier::DEFAULT_CACHING_SHA2_ROUNDS),
        native: get(p, &MYSQL_NATIVE_PASSWORD, c),
        scram_sha256_iterations: get(p, &SCRAM_SHA256_ITERATIONS, c)
            .try_into()
            .unwrap_or(DEFAULT_SCRAM_ITERATIONS),
        scram_sha512_iterations: get(p, &SCRAM_SHA512_ITERATIONS, c)
            .try_into()
            .unwrap_or(DEFAULT_SCRAM_ITERATIONS),
    }
}

// ---- lockout ----

/// How long a temporary lock lasts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockLength {
    /// No lock: tracking is off.
    Off,
    /// This many minutes.
    Minutes(u32),
    /// Until `ACCOUNT UNLOCK` (`PASSWORD_LOCK_TIME UNBOUNDED`).
    Unbounded,
}

impl LockLength {
    /// From the stored or configured minutes: 0 off, -1 unbounded.
    #[must_use]
    pub fn from_minutes(m: i64) -> Self {
        match m {
            0 => LockLength::Off,
            m if m < 0 => LockLength::Unbounded,
            m => LockLength::Minutes(u32::try_from(m).unwrap_or(u32::MAX)),
        }
    }

    /// When a lock that starts at `now` ends.
    #[must_use]
    pub fn until(self, now: SystemTime) -> Option<SystemTime> {
        match self {
            LockLength::Off => None,
            LockLength::Minutes(m) => Some(now + Duration::from_secs(u64::from(m) * 60)),
            LockLength::Unbounded => Some(from_micros(UNBOUNDED_LOCK_MICROS)),
        }
    }
}

/// An account's failed-login tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LockoutPolicy {
    /// Failures in a row before the lock; 0 off.
    pub attempts: u32,
    /// The lock's length.
    pub lock: LockLength,
}

impl LockoutPolicy {
    /// No tracking.
    pub const OFF: LockoutPolicy = LockoutPolicy {
        attempts: 0,
        lock: LockLength::Off,
    };

    /// The account clause, else the tenant's resolved `auth.*` values.
    #[must_use]
    pub fn of(row: &UserRow, policy: &dyn PolicySettings, catalog: &TenantCatalog) -> Self {
        let attempts = row
            .failed_login_attempts
            .map_or_else(|| get(policy, &FAILED_LOGIN_ATTEMPTS, catalog), u64::from);
        let minutes = row
            .password_lock_minutes
            .map_or_else(|| get(policy, &PASSWORD_LOCK_MINUTES, catalog), i64::from);
        Self {
            attempts: u32::try_from(attempts).unwrap_or(u32::MAX),
            lock: LockLength::from_minutes(minutes),
        }
    }

    /// Whether failures are counted.
    #[must_use]
    pub fn tracking(&self) -> bool {
        self.attempts > 0 && self.lock != LockLength::Off
    }
}

/// What counting a failure did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FailureOutcome {
    /// Failures in a row now.
    pub failures: u32,
    /// The lock this failure started, if it did.
    pub locked_until: Option<SystemTime>,
}

/// The shared failure counters and locks (`login_state`).
#[async_trait]
pub trait LoginStateStore: Send + Sync + 'static {
    /// Counts a wrong-credentials failure of `user`; locks at `policy`'s
    /// threshold. Only called when [`LockoutPolicy::tracking`].
    ///
    /// # Errors
    /// The catalog's; the failure is then counted in memory.
    async fn record_failure(
        &self,
        tenant: TenantId,
        user: UserId,
        policy: LockoutPolicy,
        now: SystemTime,
    ) -> Result<FailureOutcome>;

    /// A successful login: a non-zero count goes back to 0.
    ///
    /// # Errors
    /// The catalog's.
    async fn record_success(&self, tenant: TenantId, user: UserId, now: SystemTime) -> Result<()>;

    /// Clears the lock that ends at `until` (it has passed). Returns
    /// whether this call cleared it (another gateway may have).
    ///
    /// # Errors
    /// The catalog's.
    async fn unlock_expired(
        &self,
        tenant: TenantId,
        user: UserId,
        until: SystemTime,
        now: SystemTime,
    ) -> Result<bool>;

    /// Stores the SCRAM verifiers missing from the password generation of
    /// `verifier_id`, which `password` just matched at a login (chapter 14,
    /// "SCRAM verifiers": upgrade on login). Returns the mechanisms added,
    /// comma-separated, or `None` if nothing was missing. The default
    /// stores nothing (stores without a catalog writer).
    ///
    /// # Errors
    /// The catalog's.
    async fn backfill_scram(
        &self,
        _tenant: TenantId,
        _user: UserId,
        _verifier_id: u64,
        _password: Zeroizing<Vec<u8>>,
        _hashing: Hashing,
    ) -> Result<Option<String>> {
        Ok(None)
    }
}

/// `login_state` in the tenant's catalog, through its writer; lock
/// transitions are auth changes.
pub struct CatalogLoginState {
    cat: MariaCatalog,
    /// Failures counted while the writer couldn't be reached.
    pending: Mutex<HashMap<(TenantId, UserId), u32>>,
}

impl std::fmt::Debug for CatalogLoginState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogLoginState").finish_non_exhaustive()
    }
}

impl CatalogLoginState {
    /// Counters in the catalogs `cat` changes.
    #[must_use]
    pub fn new(cat: MariaCatalog) -> Self {
        Self {
            cat,
            pending: Mutex::new(HashMap::new()),
        }
    }

    fn take_pending(&self, key: (TenantId, UserId)) -> u32 {
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&key)
            .unwrap_or(0)
    }

    fn add_pending(&self, key: (TenantId, UserId), n: u32) {
        let mut p = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        let e = p.entry(key).or_insert(0);
        *e = e.saturating_add(n);
    }

    async fn failure(
        &self,
        tenant: TenantId,
        user: UserId,
        policy: LockoutPolicy,
        now: SystemTime,
        extra: u32,
    ) -> Result<FailureOutcome> {
        let ep = self.cat.backend().locator().writer(tenant).await?;
        let store = AuthStore::of(&ep.store);
        let mut conn = ep.pool.get_conn().await.map_err(map_err)?;
        let mut tx = conn
            .start_transaction(mysql_async::TxOpts::default())
            .await
            .map_err(map_err)?;
        let row = store.login_state_for_update(&mut tx, user).await?;
        let mut row = row.unwrap_or_else(|| empty_state(user, now));
        if row.locked_until.is_some_and(|u| u > now) {
            // Locked meanwhile (another gateway): failures don't count.
            tx.rollback().await.map_err(map_err)?;
            return Ok(FailureOutcome {
                failures: row.consecutive_failures,
                locked_until: None,
            });
        }
        restart_after_lock(&mut row);
        let failures = row
            .consecutive_failures
            .saturating_add(1)
            .saturating_add(extra);
        if failures < policy.attempts {
            row.consecutive_failures = failures;
            row.last_failure_at = Some(now);
            row.updated_at = now;
            store.put_login_state(&mut tx, &row).await?;
            tx.commit().await.map_err(map_err)?;
            return Ok(FailureOutcome {
                failures,
                locked_until: None,
            });
        }
        tx.rollback().await.map_err(map_err)?;
        drop(conn);
        // The lock: an auth change, so every gateway reloads.
        let until = policy.lock.until(now);
        let r = auth_change(&self.cat, tenant, move |store, q| {
            Box::pin(async move {
                let mut row = store
                    .login_state_for_update(q, user)
                    .await?
                    .unwrap_or_else(|| empty_state(user, now));
                restart_after_lock(&mut row);
                row.consecutive_failures = row.consecutive_failures.saturating_add(1 + extra);
                row.last_failure_at = Some(now);
                row.locked_until = until;
                row.updated_at = now;
                store.put_login_state(q, &row).await?;
                Ok(row.consecutive_failures)
            })
        })
        .await?;
        Ok(FailureOutcome {
            failures: r.0,
            locked_until: until,
        })
    }
}

/// A lock that has passed (the caller checked) ends the run of failures:
/// counting starts again, as in MySQL.
fn restart_after_lock(row: &mut LoginStateRow) {
    if row.locked_until.take().is_some() {
        row.consecutive_failures = 0;
    }
}

fn empty_state(user: UserId, now: SystemTime) -> LoginStateRow {
    LoginStateRow {
        user_id: user,
        consecutive_failures: 0,
        locked_until: None,
        last_failure_at: None,
        last_success_at: None,
        last_success_ip: None,
        updated_at: now,
    }
}

type ChangeBody<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<T>> + Send + 'a>>;

/// Runs `body` in one auth change of `tenant`; returns its value and the
/// committed version. On failure the change rolls back and the allocated
/// version is settled.
async fn auth_change<T, F>(cat: &MariaCatalog, tenant: TenantId, body: F) -> Result<(T, u64)>
where
    T: Send,
    F: for<'a> FnOnce(
            &'a AuthStore,
            &'a mut mysql_async::Transaction<'static>,
        ) -> ChangeBody<'a, T>
        + Send,
{
    let mut c = cat
        .change_with(tenant, VersionKind::Auth, ChangeOpts::default())
        .await?;
    let r = {
        let (core, q) = c.tx().parts();
        let store = AuthStore::of(core);
        body(&store, q).await
    };
    match r {
        Ok(v) => Ok((v, c.commit().await?)),
        Err(e) => {
            let _ = c.rollback().await;
            let _ = cat.settle(tenant, VersionKind::Auth).await;
            Err(e)
        }
    }
}

#[async_trait]
impl LoginStateStore for CatalogLoginState {
    async fn record_failure(
        &self,
        tenant: TenantId,
        user: UserId,
        policy: LockoutPolicy,
        now: SystemTime,
    ) -> Result<FailureOutcome> {
        let key = (tenant, user);
        let extra = self.take_pending(key);
        match self.failure(tenant, user, policy, now, extra).await {
            Ok(o) => Ok(o),
            Err(e) => {
                self.add_pending(key, extra.saturating_add(1));
                tracing::warn!(%tenant, user = %user, error = %e, "failed login counted in memory: the catalog writer can't be reached");
                Err(e)
            }
        }
    }

    async fn record_success(&self, tenant: TenantId, user: UserId, now: SystemTime) -> Result<()> {
        self.take_pending((tenant, user));
        let ep = self.cat.backend().locator().writer(tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(map_err)?;
        AuthStore::of(&ep.store)
            .reset_failures(&mut conn, user, now)
            .await?;
        Ok(())
    }

    async fn unlock_expired(
        &self,
        tenant: TenantId,
        user: UserId,
        until: SystemTime,
        now: SystemTime,
    ) -> Result<bool> {
        // Read first, so gateways racing to unlock allocate one version.
        {
            let ep = self.cat.backend().locator().writer(tenant).await?;
            let mut conn = ep.pool.get_conn().await.map_err(map_err)?;
            let row = AuthStore::of(&ep.store)
                .login_state(&mut conn, user)
                .await?;
            if row.and_then(|r| r.locked_until) != Some(until) {
                return Ok(false);
            }
        }
        let (cleared, _) = auth_change(&self.cat, tenant, move |store, q| {
            Box::pin(async move {
                let row = store.login_state_for_update(q, user).await?;
                if row.and_then(|r| r.locked_until) != Some(until) || until > now {
                    return Ok(false);
                }
                store.clear_login_lock(q, user, now).await
            })
        })
        .await?;
        Ok(cleared)
    }

    async fn backfill_scram(
        &self,
        tenant: TenantId,
        user: UserId,
        verifier_id: u64,
        password: Zeroizing<Vec<u8>>,
        hashing: Hashing,
    ) -> Result<Option<String>> {
        let added =
            admin::backfill_scram(&self.cat, tenant, user, verifier_id, &password, hashing).await?;
        Ok(added.map(|(_, kinds)| {
            kinds
                .iter()
                .map(|k| k.verifier_kind().as_str())
                .collect::<Vec<_>>()
                .join(",")
        }))
    }
}

/// Counters in memory: tests, and processes without a catalog writer.
#[derive(Debug, Default)]
pub struct MemLoginState {
    rows: Mutex<HashMap<(TenantId, UserId), LoginStateRow>>,
    backfills: Mutex<Vec<(TenantId, UserId, u64)>>,
}

impl MemLoginState {
    /// Empty.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The row of `user`, if written.
    #[must_use]
    pub fn row(&self, tenant: TenantId, user: UserId) -> Option<LoginStateRow> {
        self.rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(tenant, user))
            .cloned()
    }

    /// The SCRAM backfills asked for: tenant, user and matched verifier.
    #[must_use]
    pub fn backfills(&self) -> Vec<(TenantId, UserId, u64)> {
        self.backfills
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl LoginStateStore for MemLoginState {
    async fn record_failure(
        &self,
        tenant: TenantId,
        user: UserId,
        policy: LockoutPolicy,
        now: SystemTime,
    ) -> Result<FailureOutcome> {
        let mut rows = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        let row = rows
            .entry((tenant, user))
            .or_insert_with(|| empty_state(user, now));
        if row.locked_until.is_some_and(|u| u > now) {
            return Ok(FailureOutcome {
                failures: row.consecutive_failures,
                locked_until: None,
            });
        }
        restart_after_lock(row);
        row.consecutive_failures = row.consecutive_failures.saturating_add(1);
        row.last_failure_at = Some(now);
        let locked_until = if row.consecutive_failures >= policy.attempts {
            policy.lock.until(now)
        } else {
            None
        };
        if locked_until.is_some() {
            row.locked_until = locked_until;
        }
        Ok(FailureOutcome {
            failures: row.consecutive_failures,
            locked_until,
        })
    }

    async fn record_success(&self, tenant: TenantId, user: UserId, _now: SystemTime) -> Result<()> {
        if let Some(r) = self
            .rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_mut(&(tenant, user))
            && r.locked_until.is_none()
        {
            r.consecutive_failures = 0;
        }
        Ok(())
    }

    async fn unlock_expired(
        &self,
        tenant: TenantId,
        user: UserId,
        until: SystemTime,
        now: SystemTime,
    ) -> Result<bool> {
        let mut rows = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        match rows.get_mut(&(tenant, user)) {
            Some(r) if r.locked_until == Some(until) && until <= now => {
                r.locked_until = None;
                r.consecutive_failures = 0;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn backfill_scram(
        &self,
        tenant: TenantId,
        user: UserId,
        verifier_id: u64,
        _password: Zeroizing<Vec<u8>>,
        _hashing: Hashing,
    ) -> Result<Option<String>> {
        self.backfills
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((tenant, user, verifier_id));
        Ok(Some("scram_sha_256,scram_sha_512".into()))
    }
}

// ---- passwords ----

/// `auth.password_policy`: the levels of MySQL's `validate_password`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum Strength {
    /// No checks.
    #[default]
    Off,
    /// At least 8 characters.
    Low,
    /// `Low`, plus a digit, a lower-case and an upper-case letter and a
    /// special character.
    Medium,
    /// `Medium`, plus the password must not contain the user name (MySQL's
    /// dictionary check, with the user name as the dictionary).
    Strong,
}

/// The shortest password `Low` and above accept (MySQL's
/// `validate_password.length`).
pub const MIN_LENGTH: usize = 8;

impl Strength {
    /// Parses a level; anything unknown is `Off`.
    #[must_use]
    pub fn parse(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "low" => Strength::Low,
            "medium" => Strength::Medium,
            "strong" => Strength::Strong,
            _ => Strength::Off,
        }
    }

    /// Whether `password` satisfies the level for `user`. Above `Off`, the
    /// password may also not be the user name or the user name reversed
    /// (MySQL's `validate_password.check_user_name`).
    #[must_use]
    pub fn accepts(self, password: &[u8], user: &str) -> bool {
        if self == Strength::Off {
            return true;
        }
        let pw = String::from_utf8_lossy(password);
        let lower_user = user.to_lowercase();
        let reversed: String = user.chars().rev().collect();
        if pw == user || *pw == reversed {
            return false;
        }
        if pw.chars().count() < MIN_LENGTH {
            return false;
        }
        if self >= Strength::Medium {
            let digit = pw.chars().any(|c| c.is_ascii_digit());
            let lower = pw.chars().any(char::is_lowercase);
            let upper = pw.chars().any(char::is_uppercase);
            let special = pw.chars().any(|c| !c.is_alphanumeric());
            if !(digit && lower && upper && special) {
                return false;
            }
        }
        if self == Strength::Strong
            && lower_user.chars().count() >= 4
            && pw.to_lowercase().contains(&lower_user)
        {
            return false;
        }
        true
    }
}

/// The rules a new password of one account must meet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PasswordRules {
    /// Strength.
    pub strength: Strength,
    /// `PASSWORD HISTORY n`: the new password may not be any of the `n`
    /// newest (the current one included).
    pub history: u64,
    /// `PASSWORD REUSE INTERVAL n DAY`: nor any set in the last `n` days.
    pub reuse_days: u64,
    /// `PASSWORD REQUIRE CURRENT`: the change must name the current
    /// password.
    pub require_current: bool,
}

impl PasswordRules {
    /// The account's clauses, else the tenant's resolved `auth.*` values.
    #[must_use]
    pub fn of(row: &UserRow, policy: &dyn PolicySettings, catalog: &TenantCatalog) -> Self {
        let n = |v: Option<i16>, s: &'static Setting<u64>| {
            v.map_or_else(
                || get(policy, s, catalog),
                |d| u64::try_from(d).unwrap_or(0),
            )
        };
        Self {
            strength: Strength::parse(&get(policy, &PASSWORD_POLICY, catalog)),
            history: n(row.password_history, &PASSWORD_HISTORY),
            reuse_days: n(row.password_reuse_days, &PASSWORD_REUSE_INTERVAL_DAYS),
            require_current: match row.require_current {
                RequireCurrent::Yes => true,
                RequireCurrent::Optional => false,
                RequireCurrent::Default => get(policy, &PASSWORD_REQUIRE_CURRENT, catalog),
            },
        }
    }

    /// History generations a password change keeps for these rules.
    #[must_use]
    pub fn keep_history(&self) -> u64 {
        if self.reuse_days > 0 {
            // Every generation inside the interval must stay; the history
            // can't be cut by count alone. A generous bound: one change a
            // day over the interval, and never fewer than `history`.
            self.history.max(self.reuse_days).max(1)
        } else {
            self.history
        }
    }

    /// Whether `password` repeats one of `verifiers` (the account's
    /// `caching_sha2_password` verifiers, every slot) under these rules.
    /// One SHA-256 crypt per candidate: run it off the async threads.
    #[must_use]
    pub fn reused(&self, password: &[u8], verifiers: &[VerifierRow], now: SystemTime) -> bool {
        if self.history == 0 && self.reuse_days == 0 {
            return false;
        }
        let newest = verifiers.iter().map(|v| v.generation).max().unwrap_or(0);
        let window = Duration::from_secs(self.reuse_days.saturating_mul(86_400));
        verifiers
            .iter()
            .filter(|v| v.kind == VerifierKind::CachingSha2)
            .filter(|v| {
                let by_count = self.history > 0 && newest - v.generation < self.history;
                let by_age = self.reuse_days > 0
                    && now
                        .duration_since(v.created_at)
                        .map_or(true, |age| age < window);
                by_count || by_age
            })
            .filter_map(|v| v.auth_string.as_deref())
            .filter_map(|s| CachingSha2Verifier::parse(s).ok())
            .any(|v| v.verify(password))
    }
}

/// Why a new password was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PasswordProblem {
    /// Too weak for `auth.password_policy`: 1819.
    Weak,
    /// Repeats a recent password: 3638 (1819 on 3307).
    Reused,
    /// `REQUIRE CURRENT` applies and no `REPLACE` was given: 3892.
    CurrentMissing,
    /// `REPLACE` named a wrong current password: 3891.
    CurrentWrong,
}

impl PasswordProblem {
    /// The audit reason.
    #[must_use]
    pub fn reason(self) -> &'static str {
        match self {
            PasswordProblem::Weak => "weak",
            PasswordProblem::Reused => "reused",
            PasswordProblem::CurrentMissing => "current_missing",
            PasswordProblem::CurrentWrong => "current_wrong",
        }
    }

    /// The persona's error for `'user'@'host'`.
    #[must_use]
    pub fn error(self, persona: Persona, user: &str, host: &str) -> SprigError {
        const WEAK: &str = "Your password does not satisfy the current policy requirements";
        match (self, persona) {
            (PasswordProblem::Weak, _) | (PasswordProblem::Reused, Persona::MariaDb) => {
                SprigError::standard(1819, *b"HY000", WEAK)
            }
            (PasswordProblem::Reused, Persona::MySql84) => SprigError::standard(
                3638,
                *b"HY000",
                format!(
                    "Cannot use these credentials for '{user}@{host}' because they contradict the password history policy"
                ),
            ),
            (PasswordProblem::CurrentMissing, _) => SprigError::standard(
                3892,
                *b"HY000",
                "Current password needs to be specified in the REPLACE clause in order to change it.",
            ),
            (PasswordProblem::CurrentWrong, _) => SprigError::standard(
                3891,
                *b"HY000",
                "Incorrect current password. Specify the correct password which has to be replaced.",
            ),
        }
    }
}

/// A password change to check and make ([`change_password`]).
pub struct PasswordChange<'a> {
    /// The new password.
    pub password: &'a [u8],
    /// The `REPLACE 'current'` clause, if given.
    pub current: Option<&'a [u8]>,
    /// `RETAIN CURRENT PASSWORD`.
    pub retain_current: bool,
    /// `DISCARD OLD PASSWORD` in the same statement.
    pub discard_old: bool,
    /// Who makes the change, when not the user (an administrator, possibly
    /// of an ancestor tenant). Only the user's own change checks `REQUIRE
    /// CURRENT`, as in MySQL.
    pub actor: Option<Actor>,
    /// The listener's persona, for the errors.
    pub persona: Persona,
}

impl std::fmt::Debug for PasswordChange<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PasswordChange")
            .field("retain_current", &self.retain_current)
            .field("discard_old", &self.discard_old)
            .field("actor", &self.actor)
            .finish_non_exhaustive()
    }
}

/// What [`change_password`] needs besides the change.
pub struct PasswordCtx<'a> {
    /// The tenant's name, for audit events.
    pub tenant_name: Option<&'a str>,
    /// The catalog writer path.
    pub cat: &'a MariaCatalog,
    /// The tenant's snapshot, for its settings and the account.
    pub catalog: &'a TenantCatalog,
    /// Resolution of the `auth.*` keys.
    pub policy: &'a dyn PolicySettings,
    /// Where audit events go.
    pub audit: &'a dyn AuditSink,
    /// Wall-clock time.
    pub clock: &'a dyn Clock,
}

impl std::fmt::Debug for PasswordCtx<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PasswordCtx")
            .field("tenant", &self.catalog.tenant)
            .finish_non_exhaustive()
    }
}

/// Checks a new password of `user` against its rules (strength, history,
/// reuse interval, `REQUIRE CURRENT`) and sets it, auditing either way
/// (`auth.password_changed` or `auth.password_rejected`).
///
/// # Errors
/// 1819, 3638, 3891 or 3892 as [`PasswordProblem::error`]; 1396 if the
/// user is gone; the catalog's errors.
pub async fn change_password(
    ctx: &PasswordCtx<'_>,
    user: UserId,
    change: &PasswordChange<'_>,
) -> Result<Changed> {
    let tenant = ctx.catalog.tenant;
    // The account as the writer has it (the snapshot may be older).
    let ep = ctx.cat.backend().locator().writer(tenant).await?;
    let store = AuthStore::of(&ep.store);
    let (row, verifiers) = {
        let mut conn = ep.pool.get_conn().await.map_err(map_err)?;
        let row = match store.user(&mut conn, user).await? {
            Some(r) if r.is_live() => r,
            _ => {
                return Err(SprigError::backend(
                    1396,
                    *b"HY000",
                    format!("Operation ALTER USER failed for '#{user}'@'%'"),
                ));
            }
        };
        let verifiers = store.verifiers_of(&mut conn, &[user]).await?;
        (row, verifiers)
    };
    let rules = PasswordRules::of(&row, ctx.policy, ctx.catalog);
    let now = ctx.clock.wall();
    let audit = |event: &'static str, reason: Option<&'static str>| {
        let mut e = AuditEvent::new(event, "admin", now);
        e.tenant = Some(tenant);
        e.tenant_name = ctx.tenant_name.map(str::to_owned);
        e.user = Some(row.name.clone());
        e.host = Some(row.host.clone());
        e.user_id = Some(user);
        e.actor.clone_from(&change.actor);
        e.protocol = persona_protocol(change.persona);
        if reason.is_some() {
            e.outcome = "refused";
        }
        e.reason = reason;
        ctx.audit.emit(&e);
    };
    let problem = check_new_password(&rules, &row, &verifiers, change, now).await;
    if let Some(p) = problem {
        audit(events::PASSWORD_REJECTED, Some(p.reason()));
        return Err(p.error(change.persona, &row.name, &row.host));
    }
    let hashing = hashing_of(ctx.policy, ctx.catalog);
    let changed = admin::set_password(
        ctx.cat,
        tenant,
        user,
        change.password,
        SetPassword {
            retain_current: change.retain_current,
            discard_old: change.discard_old,
            keep_history: rules.keep_history(),
            hashing,
        },
        ctx.clock,
    )
    .await?;
    audit(events::PASSWORD_CHANGED, None);
    Ok(changed)
}

fn persona_protocol(p: Persona) -> &'static str {
    match p {
        Persona::MySql84 => "mysql",
        Persona::MariaDb => "mariadb",
    }
}

/// The checks of [`change_password`], without I/O: `None` if the change
/// may go ahead.
pub async fn check_new_password(
    rules: &PasswordRules,
    row: &UserRow,
    verifiers: &[VerifierRow],
    change: &PasswordChange<'_>,
    now: SystemTime,
) -> Option<PasswordProblem> {
    if !rules.strength.accepts(change.password, &row.name) {
        return Some(PasswordProblem::Weak);
    }
    let own = change.actor.is_none();
    let current: Vec<VerifierRow> = verifiers
        .iter()
        .filter(|v| v.kind == VerifierKind::CachingSha2 && v.slot == Slot::Primary)
        .cloned()
        .collect();
    let hist: Vec<VerifierRow> = verifiers.to_vec();
    let pw = Zeroizing::new(change.password.to_vec());
    let given = change.current.map(|c| Zeroizing::new(c.to_vec()));
    let require = own && rules.require_current && change.persona == Persona::MySql84;
    let rules = *rules;
    tokio::task::spawn_blocking(move || {
        if require || given.is_some() {
            let Some(cur) = given else {
                return Some(PasswordProblem::CurrentMissing);
            };
            let ok = current
                .iter()
                .filter_map(|v| v.auth_string.as_deref())
                .filter_map(|s| CachingSha2Verifier::parse(s).ok())
                .any(|v| v.verify(&cur));
            if !ok {
                return Some(PasswordProblem::CurrentWrong);
            }
        }
        rules
            .reused(&pw, &hist, now)
            .then_some(PasswordProblem::Reused)
    })
    .await
    .unwrap_or(Some(PasswordProblem::Weak))
}

// ---- account clauses ----

/// Account-level policy clauses of `CREATE USER` / `ALTER USER`: each
/// `None` leaves the attribute as it is; `Some(None)` clears it (back to
/// the tenant's setting).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AccountClauses {
    /// `FAILED_LOGIN_ATTEMPTS n`.
    pub failed_login_attempts: Option<Option<u16>>,
    /// `PASSWORD_LOCK_TIME n | UNBOUNDED`, in minutes (-1 unbounded).
    pub password_lock_minutes: Option<Option<i32>>,
    /// `PASSWORD EXPIRE INTERVAL n DAY | NEVER (0) | DEFAULT (None)`.
    pub password_lifetime: Option<Option<i16>>,
    /// `PASSWORD HISTORY n | DEFAULT`.
    pub password_history: Option<Option<i16>>,
    /// `PASSWORD REUSE INTERVAL n DAY | DEFAULT`.
    pub password_reuse_days: Option<Option<i16>>,
    /// `PASSWORD REQUIRE CURRENT [OPTIONAL | DEFAULT]`.
    pub require_current: Option<RequireCurrent>,
}

impl AccountClauses {
    /// Checks every set clause against the bounds the tenant's ancestors
    /// put on its `auth.*` key.
    ///
    /// # Errors
    /// 39080 for a clause outside them.
    pub fn validate(&self, policy: &dyn PolicySettings, catalog: &TenantCatalog) -> Result<()> {
        let check = |key: &'static str, v: Option<i64>| -> Result<()> {
            match v {
                Some(v) => clause_within_bounds(policy, catalog, key, v),
                None => Ok(()),
            }
        };
        check(
            FAILED_LOGIN_ATTEMPTS.key,
            self.failed_login_attempts.flatten().map(i64::from),
        )?;
        check(
            PASSWORD_LOCK_MINUTES.key,
            self.password_lock_minutes.flatten().map(i64::from),
        )?;
        check(
            crate::settings::DEFAULT_PASSWORD_LIFETIME_DAYS.key,
            self.password_lifetime.flatten().map(i64::from),
        )?;
        check(
            PASSWORD_HISTORY.key,
            self.password_history.flatten().map(i64::from),
        )?;
        check(
            PASSWORD_REUSE_INTERVAL_DAYS.key,
            self.password_reuse_days.flatten().map(i64::from),
        )?;
        Ok(())
    }

    /// Applies the clauses to `row`.
    pub fn apply(&self, row: &mut UserRow) {
        if let Some(v) = self.failed_login_attempts {
            row.failed_login_attempts = v;
        }
        if let Some(v) = self.password_lock_minutes {
            row.password_lock_minutes = v;
        }
        if let Some(v) = self.password_lifetime {
            row.password_lifetime = v;
        }
        if let Some(v) = self.password_history {
            row.password_history = v;
        }
        if let Some(v) = self.password_reuse_days {
            row.password_reuse_days = v;
        }
        if let Some(v) = self.require_current {
            row.require_current = v;
        }
    }
}

/// Whether an account clause setting `key` to `value` lies within the
/// bounds in force for the tenant (an ancestor's min, max, allowed list,
/// or lock).
///
/// # Errors
/// 39080 when it doesn't.
pub fn clause_within_bounds(
    policy: &dyn PolicySettings,
    catalog: &TenantCatalog,
    key: &'static str,
    value: i64,
) -> Result<()> {
    let Some(b) = policy.bounds(key, catalog) else {
        return Ok(());
    };
    #[allow(clippy::cast_precision_loss)] // clause values are small
    let v = value as f64;
    let out = b.min.is_some_and(|m| v < m)
        || b.max.is_some_and(|m| v > m)
        || b.allowed
            .as_ref()
            .is_some_and(|a| !a.contains(&Value::Int(value)))
        || (b.locked
            && policy
                .resolve(key, catalog)
                .is_some_and(|r| r != Value::Int(value)));
    if out {
        return Err(SprigError::new(
            SprigCode::SettingOutOfBounds,
            format!("{value} is outside the bounds of {key} for this tenant"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
