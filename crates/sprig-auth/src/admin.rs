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
//!   change(tenant, Auth):
//!      opts.retain_current ? primary → secondary : primary → history
//!      discard_old deletes secondaries; trim history to the policy
//!      insert new as primary with generation + 1
//!      users.password_changed_at = now, password_expired = 0
//! ```
//!
//! Not yet (later steps): the password policy and history checks
//! (`auth.password_policy`, `PASSWORD HISTORY`, `REUSE INTERVAL`, `REQUIRE
//! CURRENT`: step 5), SCRAM verifiers (they need `SASLprep`, step 2's
//! other half), privileges of the caller (chapter 29) and audit events
//! (step 10).

use std::sync::Arc;
use std::time::SystemTime;

use sprig_catalog::{ChangeOpts, MariaCatalog, VersionKind};
use sprig_common::ids::{TenantId, UserId};
use sprig_common::time::Clock;
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};
use zeroize::Zeroizing;

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
            account_locked: false,
            password_expired: false,
            created_by_tenant: 0,
            hashing: Hashing::default(),
        }
    }
}

/// How passwords are hashed and which verifiers are stored: the
/// `auth.caching_sha2_rounds` and `auth.mysql_native_password` values the
/// caller resolved for the tenant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hashing {
    /// SHA-256 crypt rounds.
    pub caching_sha2_rounds: u32,
    /// Also store a `mysql_native_password` verifier.
    pub native: bool,
}

impl Default for Hashing {
    fn default() -> Self {
        Self {
            caching_sha2_rounds: DEFAULT_CACHING_SHA2_ROUNDS,
            native: false,
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
}

async fn hash(password: &Zeroizing<Vec<u8>>, h: Hashing) -> Result<Hashed> {
    let pw = password.clone();
    tokio::task::spawn_blocking(move || {
        Ok(Hashed {
            sha2: CachingSha2Verifier::create(&pw, h.caching_sha2_rounds)?,
            native: h.native.then(|| NativeVerifier::create(&pw)),
        })
    })
    .await
    .map_err(|e| SprigError::internal(format!("hashing task failed: {e}")))?
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
    let hashed = hash(&new.password, new.hashing).await?;
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
    let hashed = Arc::new(hash(&Zeroizing::new(password.to_vec()), opts.hashing).await?);
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

/// `ACCOUNT LOCK` / `ACCOUNT UNLOCK`.
///
/// # Errors
/// As [`set_password`].
pub async fn set_account_locked(
    cat: &MariaCatalog,
    tenant: TenantId,
    user: UserId,
    locked: bool,
) -> Result<Changed> {
    with_user(cat, tenant, user, "ALTER USER", move |_, _, row| {
        row.account_locked = locked;
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

/// `REQUIRE NONE | SSL | X509`.
///
/// # Errors
/// As [`set_password`].
pub async fn set_require_tls(
    cat: &MariaCatalog,
    tenant: TenantId,
    user: UserId,
    require: RequireTls,
) -> Result<Changed> {
    with_user(cat, tenant, user, "ALTER USER", move |_, _, row| {
        row.require_tls = require;
        Box::pin(async { Ok(()) })
    })
    .await
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
