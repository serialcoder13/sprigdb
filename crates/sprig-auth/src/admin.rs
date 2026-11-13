//! Account changes as library calls: creating a user with a password,
//! setting a password (dual passwords, history), locking, expiring and
//! dropping (chapter 14, "Setting a password"). `CREATE USER` and the other
//! account statements (chapter 29) and `sprigctl` call these.
//!
//! Every call is one auth change ([`Versioner::change_with`] with
//! [`VersionKind::Auth`]): the rows commit with a new auth version, which
//! every gateway's cache follows. Hashing happens before the change opens,
//! on the blocking pool, so the version row isn't held during it.
//!
//! ```text
//! set_password(user, pw, opts):
//!   new = [ caching_sha2(pw, fresh salt, rounds) ] (+ native(pw) when enabled)
//!   new += scram_sha_256(SASLprep(pw)) (if SASLprep accepts pw), scram_sha_512(pw)
//!          (with RETAIN CURRENT PASSWORD: the current primary's SCRAM salt and
//!          iterations, so one SCRAM exchange can check both passwords)
//!   change(tenant, Auth):
//!      opts.retain_current ? primary → secondary : primary → history
//!      discard_old deletes secondaries; trim history to the policy
//!      insert new as primary with generation + 1
//!      users.password_changed_at = now, password_expired = 0
//! ```
//!
//! These calls don't check the password policy: [`crate::policy::change_password`]
//! does, then calls [`set_password`]. [`AccountOps`] wraps them with audit
//! events and the acting administrator (step 6). The caller's privileges
//! are chapter 29's.
//!
//! [`backfill_scram`] adds the SCRAM verifiers a password set before they
//! existed lacks, at a login that saw the clear password ("upgrade on
//! login", chapter 14).

use std::sync::Arc;
use std::time::SystemTime;

use sprig_catalog::{ChangeOpts, MariaCatalog, VersionKind};
use sprig_common::ids::{TenantId, UserId};
use sprig_common::time::Clock;
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};
use zeroize::Zeroizing;

use crate::audit::{Actor, AuditEvent, AuditSink, events};

use crate::policy::AccountClauses;
use crate::scram::{DEFAULT_SCRAM_ITERATIONS, ScramKind, ScramVerifier};
use crate::store::{
    AuthStore, RequireTls, Slot, UserKind, UserRow, UserSource, UserState, VerifierKind,
    VerifierRow,
};
use crate::verifier::{CachingSha2Verifier, DEFAULT_CACHING_SHA2_ROUNDS, NativeVerifier};

/// The longest user name and host, in characters (chapter 14).
pub const MAX_NAME: usize = 255;

/// A user to create.
#[derive(Clone)]
pub struct NewUser {
    /// The user name (no tenant prefix).
    pub name: String,
    /// The host pattern; `%` for any.
    pub host: String,
    /// The password.
    pub password: Zeroizing<Vec<u8>>,
    /// User or service account.
    pub kind: UserKind,
    /// How it was created.
    pub source: UserSource,
    /// `REQUIRE`.
    pub require_tls: RequireTls,
    /// `REQUIRE SUBJECT` and `ISSUER`.
    pub require_dn: (Option<String>, Option<String>),
    /// Created locked (`ACCOUNT LOCK`).
    pub account_locked: bool,
    /// Created expired (`PASSWORD EXPIRE`).
    pub password_expired: bool,
    /// 0, or the ancestor tenant creating it (chapter 14, "Management from
    /// above").
    pub created_by_tenant: u64,
    /// Hashing and stored mechanisms.
    pub hashing: Hashing,
}

impl std::fmt::Debug for NewUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NewUser")
            .field("name", &self.name)
            .field("host", &self.host)
            .field("kind", &self.kind)
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

impl NewUser {
    /// `'name'@'%'` with `password` and every other option at its default.
    #[must_use]
    pub fn new(name: impl Into<String>, password: &[u8]) -> Self {
        Self {
            name: name.into(),
            host: "%".to_owned(),
            password: Zeroizing::new(password.to_vec()),
            kind: UserKind::User,
            source: UserSource::Sql,
            require_tls: RequireTls::None,
            require_dn: (None, None),
            account_locked: false,
            password_expired: false,
            created_by_tenant: 0,
            hashing: Hashing::default(),
        }
    }
}

/// How passwords are hashed and which verifiers are stored: the
/// `auth.caching_sha2_rounds`, `auth.mysql_native_password`,
/// `auth.scram_sha256_iterations` and `auth.scram_sha512_iterations`
/// values the caller resolved for the tenant. SCRAM-SHA-256 and
/// SCRAM-SHA-512 verifiers are always stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hashing {
    /// SHA-256 crypt rounds.
    pub caching_sha2_rounds: u32,
    /// Also store a `mysql_native_password` verifier.
    pub native: bool,
    /// PBKDF2 iterations of SCRAM-SHA-256 verifiers.
    pub scram_sha256_iterations: u32,
    /// PBKDF2 iterations of SCRAM-SHA-512 verifiers.
    pub scram_sha512_iterations: u32,
}

impl Default for Hashing {
    fn default() -> Self {
        Self {
            caching_sha2_rounds: DEFAULT_CACHING_SHA2_ROUNDS,
            native: false,
            scram_sha256_iterations: DEFAULT_SCRAM_ITERATIONS,
            scram_sha512_iterations: DEFAULT_SCRAM_ITERATIONS,
        }
    }
}

impl Hashing {
    /// The iterations of `kind`'s new verifiers.
    #[must_use]
    pub const fn scram_iterations(&self, kind: ScramKind) -> u32 {
        match kind {
            ScramKind::Sha256 => self.scram_sha256_iterations,
            ScramKind::Sha512 => self.scram_sha512_iterations,
        }
    }
}

/// Options of [`set_password`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SetPassword {
    /// `RETAIN CURRENT PASSWORD`: the current password stays valid as the
    /// secondary.
    pub retain_current: bool,
    /// `DISCARD OLD PASSWORD` in the same statement: drop the secondary.
    pub discard_old: bool,
    /// History verifiers to keep (`PASSWORD HISTORY`); older ones are
    /// deleted.
    pub keep_history: u64,
    /// Hashing.
    pub hashing: Hashing,
}

/// What a change did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Changed {
    /// The user.
    pub user_id: UserId,
    /// The auth version it committed at.
    pub auth_version: u64,
}

struct Hashed {
    sha2: CachingSha2Verifier,
    native: Option<NativeVerifier>,
    scram: Vec<ScramVerifier>,
}

/// A SCRAM salt and iteration count to reuse: a new password's verifier of
/// that kind shares them with the retained one.
type ScramSalt = (ScramKind, Vec<u8>, u32);

/// The SCRAM verifiers of `pw`, for each kind with `reuse`'s salt and
/// iterations when it has that kind, else a fresh salt and `h`'s
/// iterations. A password `SASLprep` refuses gets no SCRAM-SHA-256 one.
fn scram_of(pw: &[u8], h: Hashing, reuse: &[ScramSalt]) -> Result<Vec<ScramVerifier>> {
    let mut out = Vec::with_capacity(2);
    for kind in ScramKind::ALL {
        let v = match reuse.iter().find(|(k, _, _)| *k == kind) {
            Some((_, salt, iterations)) => ScramVerifier::create(kind, pw, Some(salt), *iterations)
                .or_else(|_| ScramVerifier::create(kind, pw, None, h.scram_iterations(kind)))?,
            None => ScramVerifier::create(kind, pw, None, h.scram_iterations(kind))?,
        };
        out.extend(v);
    }
    Ok(out)
}

async fn hash(password: &Zeroizing<Vec<u8>>, h: Hashing, reuse: Vec<ScramSalt>) -> Result<Hashed> {
    let pw = password.clone();
    tokio::task::spawn_blocking(move || {
        Ok(Hashed {
            sha2: CachingSha2Verifier::create(&pw, h.caching_sha2_rounds)?,
            native: h.native.then(|| NativeVerifier::create(&pw)),
            scram: scram_of(&pw, h, &reuse)?,
        })
    })
    .await
    .map_err(|e| SprigError::internal(format!("hashing task failed: {e}")))?
}

/// The SCRAM salts of `user`'s live verifiers in `slot`, read from the
/// writer outside any change. Empty when there are none or the read fails
/// (the new password then gets fresh salts).
async fn scram_salts(
    cat: &MariaCatalog,
    tenant: TenantId,
    user: UserId,
    slot: Slot,
) -> Vec<ScramSalt> {
    let read = async {
        let ep = cat.backend().locator().writer(tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(crate::store::map_err)?;
        AuthStore::of(&ep.store)
            .verifiers_of(&mut conn, &[user])
            .await
    };
    match read.await {
        Ok(rows) => rows
            .iter()
            .filter(|r| r.slot == slot)
            .filter_map(|r| ScramVerifier::from_row(r).ok())
            .map(|v| (v.kind(), v.salt().to_vec(), v.iterations()))
            .collect(),
        Err(e) => {
            tracing::warn!(%tenant, user = %user, error = %e, "the current SCRAM salts can't be read; the new password gets fresh ones");
            Vec::new()
        }
    }
}

fn check_account(name: &str, host: &str) -> Result<()> {
    let bad = |m: String| SprigError::new(SprigCode::ConfigInvalid, m);
    if name.is_empty() || name.chars().count() > MAX_NAME {
        return Err(bad(format!("user name must be 1 to {MAX_NAME} characters")));
    }
    if host.is_empty() || host.len() > MAX_NAME || !host.is_ascii() {
        return Err(bad(format!(
            "host must be 1 to {MAX_NAME} ASCII characters"
        )));
    }
    Ok(())
}

/// MySQL's 1396 `ER_CANNOT_USER` for a duplicate or missing account.
fn cannot_user(op: &str, name: &str, host: &str) -> SprigError {
    SprigError::backend(
        1396,
        *b"HY000",
        format!("Operation {op} failed for '{name}'@'{host}'"),
    )
}

fn is_duplicate(e: &SprigError) -> bool {
    matches!(e.kind, ErrorKind::Backend { errno: 1062, .. })
}

async fn insert_verifiers(
    store: &AuthStore,
    q: &mut mysql_async::Transaction<'static>,
    user: UserId,
    h: &Hashed,
    generation: u64,
    at: SystemTime,
) -> Result<()> {
    store
        .insert_verifier(
            q,
            &VerifierRow::mysql(
                user,
                VerifierKind::CachingSha2,
                Slot::Primary,
                h.sha2.to_bytes(),
                generation,
                at,
            ),
        )
        .await?;
    if let Some(n) = &h.native {
        store
            .insert_verifier(
                q,
                &VerifierRow::mysql(
                    user,
                    VerifierKind::Native,
                    Slot::Primary,
                    n.to_string().into_bytes(),
                    generation,
                    at,
                ),
            )
            .await?;
    }
    for v in &h.scram {
        store
            .insert_verifier(q, &v.to_row(user, Slot::Primary, generation, at))
            .await?;
    }
    Ok(())
}

/// Creates a user with a password in `tenant`'s catalog.
///
/// # Errors
/// 1396 if the live account `'name'@'host'` exists; 39998 for a bad name
/// or host; the change's errors (39070, 39071, 39072, 39504).
pub async fn create_user(
    cat: &MariaCatalog,
    tenant: TenantId,
    new: &NewUser,
    clock: &dyn Clock,
) -> Result<Changed> {
    check_account(&new.name, &new.host)?;
    let hashed = hash(&new.password, new.hashing, Vec::new()).await?;
    let now = clock.wall();
    let mut c = cat
        .change_with(tenant, VersionKind::Auth, ChangeOpts::default())
        .await?;
    let version = c.version();
    let result = async {
        let (core, q) = c.tx().parts();
        let store = AuthStore::of(core);
        let id = store.next_user_id(core, q).await?;
        let mut row = UserRow::new(id, &new.name, &new.host, new.source, now, version);
        row.kind = new.kind;
        row.require_tls = new.require_tls;
        row.require_subject.clone_from(&new.require_dn.0);
        row.require_issuer.clone_from(&new.require_dn.1);
        row.account_locked = new.account_locked;
        row.password_expired = new.password_expired;
        row.created_by_tenant = new.created_by_tenant;
        row.password_changed_at = Some(now);
        store.insert_user(q, &row).await.map_err(|e| {
            if is_duplicate(&e) {
                cannot_user("CREATE USER", &new.name, &new.host)
            } else {
                e
            }
        })?;
        insert_verifiers(&store, q, id, &hashed, 1, now).await?;
        Ok(id)
    }
    .await;
    match result {
        Ok(user_id) => Ok(Changed {
            user_id,
            auth_version: c.commit().await?,
        }),
        Err(e) => {
            let _ = c.rollback().await;
            // The version was allocated in the directory: close the gap at
            // once, so caches don't wait `catalog.version_wait_ms` for a
            // version that will never commit.
            let _ = cat.settle(tenant, VersionKind::Auth).await;
            Err(e)
        }
    }
}

/// Runs `body` on the live user `user` in one auth change; the user's row
/// is rewritten with the change's version.
async fn with_user<F>(
    cat: &MariaCatalog,
    tenant: TenantId,
    user: UserId,
    op: &'static str,
    body: F,
) -> Result<Changed>
where
    F: for<'a> FnOnce(
            &'a AuthStore,
            &'a mut mysql_async::Transaction<'static>,
            &'a mut UserRow,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>,
        > + Send,
{
    let mut c = cat
        .change_with(tenant, VersionKind::Auth, ChangeOpts::default())
        .await?;
    let version = c.version();
    let result = async {
        let (core, q) = c.tx().parts();
        let store = AuthStore::of(core);
        let mut row = match store.user(q, user).await? {
            Some(r) if r.is_live() => r,
            _ => return Err(cannot_user(op, &format!("#{user}"), "%")),
        };
        body(&store, q, &mut row).await?;
        row.changed_version = version;
        store.update_user(q, &row).await?;
        Ok(())
    }
    .await;
    match result {
        Ok(()) => Ok(Changed {
            user_id: user,
            auth_version: c.commit().await?,
        }),
        Err(e) => {
            let _ = c.rollback().await;
            // The version was allocated in the directory: close the gap at
            // once, so caches don't wait `catalog.version_wait_ms` for a
            // version that will never commit.
            let _ = cat.settle(tenant, VersionKind::Auth).await;
            Err(e)
        }
    }
}

/// Sets `user`'s password (chapter 14, "Setting a password").
///
/// # Errors
/// 1396 if the user doesn't exist or is dropped; the change's errors.
pub async fn set_password(
    cat: &MariaCatalog,
    tenant: TenantId,
    user: UserId,
    password: &[u8],
    opts: SetPassword,
    clock: &dyn Clock,
) -> Result<Changed> {
    // RETAIN CURRENT PASSWORD: the new SCRAM verifiers take the current
    // primary's salts, so a SCRAM exchange (one salt) can check both.
    let reuse = if opts.retain_current {
        scram_salts(cat, tenant, user, Slot::Primary).await
    } else {
        Vec::new()
    };
    let hashed = Arc::new(hash(&Zeroizing::new(password.to_vec()), opts.hashing, reuse).await?);
    let now = clock.wall();
    with_user(cat, tenant, user, "SET PASSWORD", move |store, q, row| {
        let hashed = Arc::clone(&hashed);
        Box::pin(async move {
            let id = row.user_id;
            let generation = store.max_generation(q, id).await? + 1;
            if opts.retain_current {
                // The old secondary goes to history; the primary becomes
                // the secondary.
                store
                    .move_verifiers(q, id, Slot::Secondary, Slot::History)
                    .await?;
                store
                    .move_verifiers(q, id, Slot::Primary, Slot::Secondary)
                    .await?;
            } else {
                store
                    .move_verifiers(q, id, Slot::Primary, Slot::History)
                    .await?;
            }
            if opts.discard_old {
                store
                    .move_verifiers(q, id, Slot::Secondary, Slot::History)
                    .await?;
            }
            store.trim_history(q, id, opts.keep_history).await?;
            insert_verifiers(store, q, id, &hashed, generation, now).await?;
            row.password_changed_at = Some(now);
            row.password_expired = false;
            Ok(())
        })
    })
    .await
}

/// Adds the SCRAM verifiers missing from the password generation of
/// `verifier_id` (the `caching_sha2_password` or `mysql_native_password`
/// verifier `password` just matched at a login): chapter 14's "upgrade on
/// login" for passwords set before SCRAM verifiers were stored, or
/// imported as hashes. A new verifier takes the salt and iterations of the
/// same kind in the user's other live slot, if there is one, so dual
/// passwords keep sharing a salt. Returns the change and the mechanisms
/// added; `Ok(None)` when nothing is missing, or the verifier is gone (the
/// password changed meanwhile) or is history.
///
/// The caller has verified `password`; this doesn't check it again.
///
/// # Errors
/// The catalog's errors; 39998 for iterations outside the settings' range.
pub async fn backfill_scram(
    cat: &MariaCatalog,
    tenant: TenantId,
    user: UserId,
    verifier_id: u64,
    password: &[u8],
    hashing: Hashing,
) -> Result<Option<(Changed, Vec<ScramKind>)>> {
    let rows = {
        let ep = cat.backend().locator().writer(tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(crate::store::map_err)?;
        AuthStore::of(&ep.store)
            .verifiers_of(&mut conn, &[user])
            .await?
    };
    let Some((slot, generation)) = rows
        .iter()
        .find(|r| r.verifier_id == verifier_id && r.slot != Slot::History)
        .map(|r| (r.slot, r.generation))
    else {
        return Ok(None);
    };
    let has = move |rows: &[VerifierRow], kind: ScramKind| {
        rows.iter()
            .any(|r| r.kind == kind.verifier_kind() && r.slot == slot && r.generation == generation)
    };
    let missing: Vec<ScramKind> = ScramKind::ALL
        .into_iter()
        .filter(|k| !has(&rows, *k) && k.prepare(password).is_some())
        .collect();
    if missing.is_empty() {
        return Ok(None);
    }
    let other = match slot {
        Slot::Primary => Slot::Secondary,
        _ => Slot::Primary,
    };
    let reuse: Vec<ScramSalt> = rows
        .iter()
        .filter(|r| r.slot == other)
        .filter_map(|r| ScramVerifier::from_row(r).ok())
        .map(|v| (v.kind(), v.salt().to_vec(), v.iterations()))
        .collect();
    let pw = Zeroizing::new(password.to_vec());
    let new: Vec<ScramVerifier> =
        tokio::task::spawn_blocking(move || scram_of(&pw, hashing, &reuse))
            .await
            .map_err(|e| SprigError::internal(format!("hashing task failed: {e}")))??
            .into_iter()
            .filter(|v| missing.contains(&v.kind()))
            .collect();
    if new.is_empty() {
        return Ok(None);
    }
    let kinds: Vec<ScramKind> = new.iter().map(ScramVerifier::kind).collect();
    let new = Arc::new(new);
    let now = SystemTime::now();
    let changed = with_user(cat, tenant, user, "ALTER USER", move |store, q, row| {
        let new = Arc::clone(&new);
        Box::pin(async move {
            let rows = store.verifiers_of(q, &[row.user_id]).await?;
            let still = rows.iter().any(|r| {
                r.verifier_id == verifier_id && r.slot == slot && r.generation == generation
            });
            if !still {
                return Ok(());
            }
            for v in new.iter().filter(|v| !has(&rows, v.kind())) {
                store
                    .insert_verifier(q, &v.to_row(row.user_id, slot, generation, now))
                    .await?;
            }
            Ok(())
        })
    })
    .await?;
    Ok(Some((changed, kinds)))
}

/// `DISCARD OLD PASSWORD`: the secondary password stops working.
///
/// # Errors
/// As [`set_password`].
pub async fn discard_old_password(
    cat: &MariaCatalog,
    tenant: TenantId,
    user: UserId,
    keep_history: u64,
) -> Result<Changed> {
    with_user(cat, tenant, user, "ALTER USER", move |store, q, row| {
        Box::pin(async move {
            store
                .move_verifiers(q, row.user_id, Slot::Secondary, Slot::History)
                .await?;
            store.trim_history(q, row.user_id, keep_history).await?;
            Ok(())
        })
    })
    .await
}

/// `ACCOUNT LOCK` / `ACCOUNT UNLOCK`. Unlocking also clears a temporary
/// lock and the failure count, as MySQL's `ACCOUNT UNLOCK` does.
///
/// # Errors
/// As [`set_password`].
pub async fn set_account_locked(
    cat: &MariaCatalog,
    tenant: TenantId,
    user: UserId,
    locked: bool,
) -> Result<Changed> {
    let now = SystemTime::now();
    with_user(cat, tenant, user, "ALTER USER", move |store, q, row| {
        row.account_locked = locked;
        let id = row.user_id;
        Box::pin(async move {
            if !locked {
                store.clear_login_lock(q, id, now).await?;
            }
            Ok(())
        })
    })
    .await
}

/// Account policy clauses (`FAILED_LOGIN_ATTEMPTS`, `PASSWORD_LOCK_TIME`,
/// `PASSWORD EXPIRE INTERVAL`, `PASSWORD HISTORY`, `PASSWORD REUSE
/// INTERVAL`, `PASSWORD REQUIRE CURRENT`). The caller checks them against
/// the tenant's bounds first ([`AccountClauses::validate`]).
///
/// # Errors
/// As [`set_password`].
pub async fn set_clauses(
    cat: &MariaCatalog,
    tenant: TenantId,
    user: UserId,
    clauses: AccountClauses,
) -> Result<Changed> {
    with_user(cat, tenant, user, "ALTER USER", move |_, _, row| {
        clauses.apply(row);
        Box::pin(async { Ok(()) })
    })
    .await
}

/// `PASSWORD EXPIRE` (or clears the flag).
///
/// # Errors
/// As [`set_password`].
pub async fn set_password_expired(
    cat: &MariaCatalog,
    tenant: TenantId,
    user: UserId,
    expired: bool,
) -> Result<Changed> {
    with_user(cat, tenant, user, "ALTER USER", move |_, _, row| {
        row.password_expired = expired;
        Box::pin(async { Ok(()) })
    })
    .await
}

/// `REQUIRE NONE | SSL | X509` (clearing `SUBJECT` and `ISSUER`).
///
/// # Errors
/// As [`set_password`].
pub async fn set_require_tls(
    cat: &MariaCatalog,
    tenant: TenantId,
    user: UserId,
    require: RequireTls,
) -> Result<Changed> {
    set_require(cat, tenant, user, require, (None, None)).await
}

/// `REQUIRE ...` with `SUBJECT` and `ISSUER` (`dn`); a `dn` naming either
/// first brings the catalog to the auth module's format 6.
///
/// # Errors
/// As [`set_password`]; the migrator's.
pub async fn set_require(
    cat: &MariaCatalog,
    tenant: TenantId,
    user: UserId,
    require: RequireTls,
    dn: (Option<String>, Option<String>),
) -> Result<Changed> {
    if dn.0.is_some() || dn.1.is_some() {
        migrate_dn(cat, tenant).await?;
    }
    with_user(cat, tenant, user, "ALTER USER", move |_, _, row| {
        row.require_tls = require;
        row.require_subject.clone_from(&dn.0);
        row.require_issuer.clone_from(&dn.1);
        Box::pin(async { Ok(()) })
    })
    .await
}

/// `ATTRIBUTE '{...}'` or `COMMENT '...'` (chapter 29's
/// `SPRIG_USER_ATTR()`): `patch`, a JSON object, merged into the user's
/// `attributes` as MySQL 8.4 merges `user_attributes.metadata` (RFC 7396:
/// a `null` value removes its key).
///
/// # Errors
/// As [`set_password`].
pub async fn merge_attributes(
    cat: &MariaCatalog,
    tenant: TenantId,
    user: UserId,
    patch: serde_json::Value,
) -> Result<Changed> {
    with_user(cat, tenant, user, "ALTER USER", move |_, _, row| {
        row.attributes = merge_patch(row.attributes.take(), &patch);
        Box::pin(async { Ok(()) })
    })
    .await
}

/// RFC 7396's merge of `patch` into `base` (objects merge key by key; a
/// `null` removes; anything else replaces). An empty result is `None`.
#[must_use]
pub fn merge_patch(
    base: Option<serde_json::Value>,
    patch: &serde_json::Value,
) -> Option<serde_json::Value> {
    fn merge(base: &mut serde_json::Value, patch: &serde_json::Value) {
        let serde_json::Value::Object(p) = patch else {
            *base = patch.clone();
            return;
        };
        if !base.is_object() {
            *base = serde_json::Value::Object(serde_json::Map::new());
        }
        if let serde_json::Value::Object(b) = base {
            for (k, v) in p {
                if v.is_null() {
                    b.remove(k);
                } else {
                    merge(b.entry(k.clone()).or_insert(serde_json::Value::Null), v);
                }
            }
        }
    }
    let mut b = base.unwrap_or(serde_json::Value::Null);
    merge(&mut b, patch);
    match &b {
        serde_json::Value::Object(m) if m.is_empty() => None,
        _ => Some(b),
    }
}

/// The auth module at format 6 (`REQUIRE SUBJECT` and `ISSUER`) in
/// `tenant`'s catalog, before a change writes them.
///
/// # Errors
/// 39070 if the catalog is unreachable; the migrator's.
pub async fn migrate_dn(cat: &MariaCatalog, tenant: TenantId) -> Result<()> {
    let ep = cat.backend().locator().writer(tenant).await?;
    let mut conn = ep.pool.get_conn().await.map_err(crate::store::map_err)?;
    crate::store::migrate_dn(&mut conn, ep.store.schema()).await
}

/// `DROP USER`: the row becomes a tombstone (its name is free, its id
/// never reused) and its verifiers are deleted.
///
/// # Errors
/// As [`set_password`].
pub async fn drop_user(cat: &MariaCatalog, tenant: TenantId, user: UserId) -> Result<Changed> {
    with_user(cat, tenant, user, "DROP USER", move |store, q, row| {
        row.state = UserState::Dropped;
        Box::pin(async move {
            for slot in Slot::ALL {
                store.delete_verifiers(q, row.user_id, *slot).await?;
            }
            Ok(())
        })
    })
    .await
}

/// The live account `'name'@'host'`'s id, read from the writer.
///
/// # Errors
/// The catalog's errors.
pub async fn find_user(
    cat: &MariaCatalog,
    tenant: TenantId,
    name: &str,
    host: &str,
) -> Result<Option<UserId>> {
    let ep = cat.backend().locator().writer(tenant).await?;
    let mut conn = ep.pool.get_conn().await.map_err(crate::store::map_err)?;
    Ok(AuthStore::of(&ep.store)
        .user_by_account(&mut conn, name, host)
        .await?
        .map(|u| u.user_id))
}

/// Account changes with their audit events (chapter 14 step 6,
/// "Management from above", and step 10): the calls above, made on behalf
/// of an actor, each emitting one event to the sink, kept in the target
/// tenant's stream and, when the actor belongs to another (ancestor)
/// tenant, in the actor's too. A user created by an ancestor's
/// administrator records the ancestor in `created_by_tenant`.
///
/// Chapter 29's account statements (`CREATE USER`, `ALTER USER`, `DROP
/// USER` after `USE TENANT`) and the Admin API call these.
pub struct AccountOps<'a> {
    /// The catalog writer path.
    pub cat: &'a MariaCatalog,
    /// The target tenant.
    pub tenant: TenantId,
    /// Its name, for the events.
    pub tenant_name: Option<String>,
    /// Who acts: an administrator of the tenant or of an ancestor, or an
    /// operator.
    pub actor: Actor,
    /// Where the events go.
    pub audit: &'a dyn AuditSink,
    /// Wall-clock time.
    pub clock: &'a dyn Clock,
    /// `mysql`, `mariadb` or `admin`: where the change came from.
    pub protocol: &'static str,
}

impl std::fmt::Debug for AccountOps<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccountOps")
            .field("tenant", &self.tenant)
            .field("actor", &self.actor)
            .finish_non_exhaustive()
    }
}

impl AccountOps<'_> {
    fn event(
        &self,
        event: &'static str,
        user: Option<UserId>,
        name: &str,
        host: &str,
    ) -> AuditEvent {
        let mut e = AuditEvent::new(event, self.protocol, self.clock.wall());
        e.tenant = Some(self.tenant);
        e.tenant_name.clone_from(&self.tenant_name);
        e.user = Some(name.to_owned());
        e.host = Some(host.to_owned());
        e.user_id = user;
        e.actor = Some(self.actor.clone());
        e
    }

    fn done<T>(&self, r: Result<T>, mut e: AuditEvent, user: impl Fn(&T) -> UserId) -> Result<T> {
        match &r {
            Ok(v) => e.user_id = Some(user(v)),
            Err(err) => {
                e.outcome = "refused";
                e.detail = Some(err.message.clone());
            }
        }
        self.audit.emit(&e);
        r
    }

    /// [`create_user`]; `created_by_tenant` is the actor's tenant when it
    /// isn't the target.
    ///
    /// # Errors
    /// As [`create_user`].
    pub async fn create_user(&self, new: &NewUser) -> Result<Changed> {
        let mut new = new.clone();
        if self.actor.tenant != self.tenant {
            new.created_by_tenant = self.actor.tenant.0;
        }
        let e = self.event(events::USER_CREATED, None, &new.name, &new.host);
        let r = create_user(self.cat, self.tenant, &new, self.clock).await;
        self.done(r, e, |c| c.user_id)
    }

    /// [`drop_user`] of `'name'@'host'` (id `user`).
    ///
    /// # Errors
    /// As [`drop_user`].
    pub async fn drop_user(&self, user: UserId, name: &str, host: &str) -> Result<Changed> {
        let e = self.event(events::USER_DROPPED, Some(user), name, host);
        let r = drop_user(self.cat, self.tenant, user).await;
        self.done(r, e, |c| c.user_id)
    }

    /// [`set_account_locked`].
    ///
    /// # Errors
    /// As [`set_account_locked`].
    pub async fn set_locked(
        &self,
        user: UserId,
        name: &str,
        host: &str,
        locked: bool,
    ) -> Result<Changed> {
        let ev = if locked {
            events::ACCOUNT_LOCKED
        } else {
            events::ACCOUNT_UNLOCKED
        };
        let mut e = self.event(ev, Some(user), name, host);
        e.reason = Some("manual");
        let r = set_account_locked(self.cat, self.tenant, user, locked).await;
        self.done(r, e, |c| c.user_id)
    }

    /// [`set_password_expired`].
    ///
    /// # Errors
    /// As [`set_password_expired`].
    pub async fn set_expired(
        &self,
        user: UserId,
        name: &str,
        host: &str,
        expired: bool,
    ) -> Result<Changed> {
        let mut e = self.event(events::ACCOUNT_CHANGED, Some(user), name, host);
        e.detail = Some(
            if expired {
                "PASSWORD EXPIRE"
            } else {
                "PASSWORD EXPIRE cleared"
            }
            .into(),
        );
        let r = set_password_expired(self.cat, self.tenant, user, expired).await;
        self.done(r, e, |c| c.user_id)
    }

    /// [`set_clauses`].
    ///
    /// # Errors
    /// As [`set_clauses`].
    pub async fn set_clauses(
        &self,
        user: UserId,
        name: &str,
        host: &str,
        clauses: AccountClauses,
    ) -> Result<Changed> {
        let mut e = self.event(events::ACCOUNT_CHANGED, Some(user), name, host);
        e.detail = Some(format!("{clauses:?}"));
        let r = set_clauses(self.cat, self.tenant, user, clauses).await;
        self.done(r, e, |c| c.user_id)
    }

    /// [`set_password`] without policy checks: an administrator's reset
    /// ([`crate::policy::change_password`] checks the rules).
    ///
    /// # Errors
    /// As [`set_password`].
    pub async fn reset_password(
        &self,
        user: UserId,
        name: &str,
        host: &str,
        password: &[u8],
        opts: SetPassword,
    ) -> Result<Changed> {
        let e = self.event(events::PASSWORD_CHANGED, Some(user), name, host);
        let r = set_password(self.cat, self.tenant, user, password, opts, self.clock).await;
        self.done(r, e, |c| c.user_id)
    }
}
