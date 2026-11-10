//! The auth module in the gateway's catalog cache (chapter 14, "Gateway
//! caches": `TenantAuth`, per tenant and auth version).
//!
//! [`AuthExtension`] is `sprig-auth`'s [`CatalogExtension`]: it registers
//! the auth migrations ([`AUTH_MIGRATIONS`]) and, versioned by
//! [`VersionKind::Auth`], loads [`TenantAuth`] into every tenant snapshot
//! in the snapshot's own transaction. Pass it to
//! [`MariaSource::new`](sprig_catalog::MariaSource::new) (loads and the
//! format check) and its [`module_of`](sprig_catalog::cache::module_of) to
//! the [`Migrator`](sprig_catalog::Migrator) (bootstrap and upgrades).
//!
//! **Incremental loads.** A load after `since` reads the users whose
//! `changed_version` is above it, with all their verifiers (every verifier
//! change rewrites its user's `changed_version`, see [`crate::store`]), and
//! applies them to the previous state: changed users replace their old
//! entries, dropped users leave. Temporary locks (`login_state` rows with
//! `locked_until`) carry no version, so every load re-reads them; there
//! are few. An incremental load equals a full load at the same version.
//!
//! **What is kept.** Live accounts in matching order per name, their
//! primary and secondary `caching_sha2_password`, `mysql_native_password`,
//! SCRAM-SHA-256 and SCRAM-SHA-512 verifiers (never history verifiers,
//! which only password changes read, from the catalog) and temporary
//! locks. A verifier that doesn't parse is left out and logged; the
//! account then fails its logins as with a wrong password. SCRAM-SHA-1
//! verifiers arrive with chapter 14 step 15.

use std::any::Any;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::net::IpAddr;
use std::sync::Arc;

use sprig_catalog::{
    CatalogExtension, ExtState, Migration, SnapshotTx, TenantCatalog, VersionKind,
};
use sprig_common::Result;
use sprig_common::ids::UserId;

use crate::account::HostPattern;
use crate::apikey::{ApiKey, ApiKeyRow};
use crate::scram::{ScramKind, ScramVerifier};
use crate::store::{
    AUTH_MIGRATIONS, AuthStore, MODULE, Slot, TempLock, UserRow, VerifierKind, VerifierRow,
};
use crate::token::TokenSession;
use crate::verifier::{CachingSha2Verifier, NativeVerifier};

/// A verifier usable at login, with its id (the fast cache's key) and
/// slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotVerifier<V> {
    /// `verifiers.verifier_id`; never reused.
    pub id: u64,
    /// Primary or secondary.
    pub slot: Slot,
    /// The verifier.
    pub verifier: V,
}

/// A live account with its login verifiers.
#[derive(Debug, Clone, PartialEq)]
pub struct AuthUser {
    /// The `users` row.
    pub row: UserRow,
    /// Its parsed host.
    pub host: HostPattern,
    /// `caching_sha2_password` verifiers: primary first, then secondary.
    pub caching_sha2: Vec<SlotVerifier<CachingSha2Verifier>>,
    /// `mysql_native_password` verifiers: primary first, then secondary.
    pub native: Vec<SlotVerifier<NativeVerifier>>,
    /// SCRAM-SHA-256 and SCRAM-SHA-512 verifiers: primary first, then
    /// secondary.
    pub scram: Vec<SlotVerifier<ScramVerifier>>,
}

impl AuthUser {
    /// Builds the account from its row and verifier rows (other users'
    /// rows and history verifiers are ignored). Returns the account and
    /// how many verifiers didn't parse.
    #[must_use]
    pub fn new(row: UserRow, verifiers: &[VerifierRow]) -> (Self, usize) {
        let mut sha2 = Vec::new();
        let mut native = Vec::new();
        let mut scram = Vec::new();
        let mut bad = 0;
        for v in verifiers
            .iter()
            .filter(|v| v.user_id == row.user_id && v.slot != Slot::History)
        {
            let raw = v.auth_string.as_deref().unwrap_or_default();
            match v.kind {
                VerifierKind::CachingSha2 => match CachingSha2Verifier::parse(raw) {
                    Ok(verifier) => sha2.push(SlotVerifier {
                        id: v.verifier_id,
                        slot: v.slot,
                        verifier,
                    }),
                    Err(_) => bad += 1,
                },
                VerifierKind::Native => match std::str::from_utf8(raw).map(NativeVerifier::parse) {
                    Ok(Ok(verifier)) => native.push(SlotVerifier {
                        id: v.verifier_id,
                        slot: v.slot,
                        verifier,
                    }),
                    _ => bad += 1,
                },
                VerifierKind::ScramSha256 | VerifierKind::ScramSha512 => {
                    match ScramVerifier::from_row(v) {
                        Ok(verifier) => scram.push(SlotVerifier {
                            id: v.verifier_id,
                            slot: v.slot,
                            verifier,
                        }),
                        Err(_) => bad += 1,
                    }
                }
                VerifierKind::ScramSha1 => {}
            }
        }
        sha2.sort_by_key(|v| (v.slot, v.id));
        native.sort_by_key(|v| (v.slot, v.id));
        scram.sort_by_key(|v| (v.slot, v.id));
        if bad > 0 {
            tracing::warn!(user_id = %row.user_id, bad, "auth cache: verifiers that don't parse are ignored");
        }
        (
            Self {
                host: HostPattern::parse(&row.host),
                row,
                caching_sha2: sha2,
                native,
                scram,
            },
            bad,
        )
    }

    /// The SCRAM verifier of `kind` in `slot`, if any (the newest, should
    /// there be several).
    #[must_use]
    pub fn scram(&self, kind: ScramKind, slot: Slot) -> Option<&ScramVerifier> {
        self.scram
            .iter()
            .rev()
            .find(|v| v.slot == slot && v.verifier.kind() == kind)
            .map(|v| &v.verifier)
    }

    /// Whether `slot` has a SCRAM verifier of `kind`.
    #[must_use]
    pub fn has_scram(&self, kind: ScramKind, slot: Slot) -> bool {
        self.scram(kind, slot).is_some()
    }

    fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + 160
            + self.row.name.len() * 2
            + self.row.host.len() * 2
            + self.row.plugin.len()
            + self.caching_sha2.len()
                * (std::mem::size_of::<SlotVerifier<CachingSha2Verifier>>() + 70)
            + self.native.len() * std::mem::size_of::<SlotVerifier<NativeVerifier>>()
            + self.scram.len() * (std::mem::size_of::<SlotVerifier<ScramVerifier>>() + 160)
    }
}

/// One tenant's auth data at one auth version: immutable, shared through
/// the snapshot.
#[derive(Clone, Default)]
pub struct TenantAuth {
    users: Arc<HashMap<UserId, Arc<AuthUser>>>,
    by_name: Arc<HashMap<String, Arc<[Arc<AuthUser>]>>>,
    locks: Arc<HashMap<UserId, TempLock>>,
    /// API keys by visible id (revoked ones too, until they're deleted).
    api_keys: Arc<HashMap<String, Arc<ApiKey>>>,
    /// Live token sessions by id (chapter 14; [`crate::token`]).
    sessions: Arc<HashMap<[u8; 16], Arc<TokenSession>>>,
    bytes: usize,
}

impl fmt::Debug for TenantAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TenantAuth")
            .field("users", &self.users.len())
            .field("locks", &self.locks.len())
            .finish_non_exhaustive()
    }
}

/// Equal live users (rows and verifiers) and locks.
impl PartialEq for TenantAuth {
    fn eq(&self, other: &Self) -> bool {
        let users = |a: &TenantAuth| -> BTreeMap<UserId, AuthUser> {
            a.users.iter().map(|(k, v)| (*k, (**v).clone())).collect()
        };
        let order = |a: &TenantAuth| -> BTreeMap<String, Vec<UserId>> {
            a.by_name
                .iter()
                .map(|(k, v)| (k.clone(), v.iter().map(|u| u.row.user_id).collect()))
                .collect()
        };
        users(self) == users(other)
            && order(self) == order(other)
            && self.locks == other.locks
            && self.api_keys == other.api_keys
            && self.sessions == other.sessions
    }
}

impl ExtState for TenantAuth {
    fn bytes(&self) -> usize {
        self.bytes
    }
}

impl TenantAuth {
    /// The tenant's auth state in `catalog`, if the auth module is loaded.
    #[must_use]
    pub fn of(catalog: &TenantCatalog) -> Option<&TenantAuth> {
        catalog.ext.get::<TenantAuth>(MODULE)
    }

    /// The state from every user row (live and dropped), every verifier
    /// and every temporary lock: a full load.
    #[must_use]
    pub fn build(users: Vec<UserRow>, verifiers: &[VerifierRow], locks: Vec<TempLock>) -> Self {
        Self::default().apply(users, verifiers, locks)
    }

    /// The next state: `changed` users (live or dropped) with all their
    /// verifiers in `verifiers`, replacing their entries; `locks` replace
    /// every temporary lock.
    #[must_use]
    pub fn apply(
        &self,
        changed: Vec<UserRow>,
        verifiers: &[VerifierRow],
        locks: Vec<TempLock>,
    ) -> Self {
        let mut by_user: HashMap<UserId, Vec<VerifierRow>> = HashMap::new();
        for v in verifiers {
            by_user.entry(v.user_id).or_default().push(v.clone());
        }
        let mut users = (*self.users).clone();
        let mut bytes = self.bytes.saturating_sub(self.locks.len() * LOCK_BYTES);
        let mut touched: HashSet<String> = HashSet::new();
        for row in changed {
            if let Some(old) = users.remove(&row.user_id) {
                bytes = bytes.saturating_sub(old.bytes());
                touched.insert(old.row.name.clone());
            }
            if row.is_live() {
                touched.insert(row.name.clone());
                let vs = by_user.remove(&row.user_id).unwrap_or_default();
                let (u, _) = AuthUser::new(row, &vs);
                bytes += u.bytes();
                users.insert(u.row.user_id, Arc::new(u));
            }
        }
        let by_name = if touched.is_empty() {
            Arc::clone(&self.by_name)
        } else {
            let mut m = (*self.by_name).clone();
            for name in &touched {
                m.remove(name);
            }
            let mut add: HashMap<&str, Vec<Arc<AuthUser>>> = HashMap::new();
            for u in users.values().filter(|u| touched.contains(&u.row.name)) {
                add.entry(u.row.name.as_str())
                    .or_default()
                    .push(Arc::clone(u));
            }
            for (name, mut accts) in add {
                accts.sort_by(|a, b| {
                    HostPattern::match_order(&a.host, &a.row.host, &b.host, &b.row.host)
                });
                m.insert(name.to_owned(), accts.into());
            }
            Arc::new(m)
        };
        let locks: HashMap<UserId, TempLock> = locks.into_iter().map(|l| (l.user_id, l)).collect();
        bytes += locks.len() * LOCK_BYTES;
        Self {
            users: Arc::new(users),
            by_name,
            locks: Arc::new(locks),
            api_keys: Arc::clone(&self.api_keys),
            sessions: Arc::clone(&self.sessions),
            bytes,
        }
    }

    /// The same state with `changed` API keys replacing their entries
    /// (`full`: replacing every key).
    #[must_use]
    pub fn with_api_keys(mut self, changed: Vec<ApiKeyRow>, full: bool) -> Self {
        if changed.is_empty() && !full {
            return self;
        }
        let mut keys = if full {
            HashMap::new()
        } else {
            (*self.api_keys).clone()
        };
        let old = self.api_keys.len();
        for r in changed {
            keys.insert(r.key_id.clone(), Arc::new(ApiKey::new(r)));
        }
        self.bytes = self.bytes.saturating_sub(old * KEY_BYTES) + keys.len() * KEY_BYTES;
        self.api_keys = Arc::new(keys);
        self
    }

    /// The same state with `changed` token sessions replacing their
    /// entries (`full`: replacing every session). Revoked and expired
    /// sessions leave.
    #[must_use]
    pub fn with_sessions(mut self, changed: Vec<TokenSession>, full: bool) -> Self {
        if changed.is_empty() && !full {
            return self;
        }
        let now = std::time::SystemTime::now();
        let mut m = if full {
            HashMap::new()
        } else {
            (*self.sessions).clone()
        };
        let old = self.sessions.len();
        m.retain(|_, s| s.live(now));
        for s in changed {
            if s.live(now) {
                m.insert(s.sid, Arc::new(s));
            } else {
                m.remove(&s.sid);
            }
        }
        self.bytes = self.bytes.saturating_sub(old * SESSION_BYTES) + m.len() * SESSION_BYTES;
        self.sessions = Arc::new(m);
        self
    }

    /// The live token session `sid`, as of this snapshot (check
    /// [`TokenSession::live`] against the time of use).
    #[must_use]
    pub fn session(&self, sid: &[u8; 16]) -> Option<&Arc<TokenSession>> {
        self.sessions.get(sid)
    }

    /// The API key `key_id`, revoked or not.
    #[must_use]
    pub fn api_key(&self, key_id: &str) -> Option<&Arc<ApiKey>> {
        self.api_keys.get(key_id)
    }

    /// Every API key, revoked or not, in no order (Studio's key list,
    /// chapter 44).
    pub fn api_keys(&self) -> impl Iterator<Item = &Arc<ApiKey>> {
        self.api_keys.values()
    }

    /// The live user `id`.
    #[must_use]
    pub fn user(&self, id: UserId) -> Option<&Arc<AuthUser>> {
        self.users.get(&id)
    }

    /// Every live user, in no order.
    pub fn users(&self) -> impl Iterator<Item = &Arc<AuthUser>> {
        self.users.values()
    }

    /// Number of live users.
    #[must_use]
    pub fn len(&self) -> usize {
        self.users.len()
    }

    /// Whether there are no live users.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.users.is_empty()
    }

    /// The live accounts named `name`, in matching order.
    #[must_use]
    pub fn accounts(&self, name: &str) -> &[Arc<AuthUser>] {
        self.by_name.get(name).map_or(&[], |a| a)
    }

    /// The account a login as `name` from `ip` uses: the first in matching
    /// order whose host matches (chapter 14, "Account matching").
    #[must_use]
    pub fn match_account(&self, name: &str, ip: IpAddr) -> Option<&Arc<AuthUser>> {
        self.accounts(name).iter().find(|u| u.host.matches(ip))
    }

    /// The user's temporary lock, if `login_state.locked_until` is set
    /// (also when it has passed: the login then unlocks).
    #[must_use]
    pub fn temp_lock(&self, id: UserId) -> Option<TempLock> {
        self.locks.get(&id).copied()
    }
}

const LOCK_BYTES: usize = 64;
const KEY_BYTES: usize = 256;
const SESSION_BYTES: usize = 160;

/// `sprig-auth`'s catalog module: the auth tables' migrations, and
/// [`TenantAuth`] in every snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub struct AuthExtension;

#[async_trait::async_trait]
impl CatalogExtension for AuthExtension {
    fn module(&self) -> &'static str {
        MODULE
    }

    fn migrations(&self) -> &'static [Migration] {
        AUTH_MIGRATIONS
    }

    fn versioned(&self) -> Option<VersionKind> {
        Some(VersionKind::Auth)
    }

    async fn load(
        &self,
        tx: &mut SnapshotTx<'_>,
        since: Option<u64>,
        prev: Option<&Arc<dyn ExtState>>,
    ) -> Result<Arc<dyn ExtState>> {
        let store = AuthStore::new(tx.schema());
        let prev = prev.and_then(|p| {
            let any: &dyn Any = &**p;
            any.downcast_ref::<TenantAuth>()
        });
        let conn = tx.conn();
        let locks = store.temp_locks(conn).await?;
        let state = if let (Some(since), Some(prev)) = (since, prev) {
            let users = store.users_since(conn, Some(since)).await?;
            let ids: Vec<UserId> = users.iter().map(|u| u.user_id).collect();
            let verifiers = if ids.is_empty() {
                Vec::new()
            } else {
                store.verifiers_of(conn, &ids).await?
            };
            let keys = crate::apikey::keys_since(&store, conn, Some(since)).await?;
            let sessions =
                crate::token::sessions::sessions_since(&store, conn, Some(since)).await?;
            prev.apply(users, &verifiers, locks)
                .with_api_keys(keys, false)
                .with_sessions(sessions, false)
        } else {
            let users = store.users_since(conn, None).await?;
            let verifiers = store.all_verifiers(conn).await?;
            let keys = crate::apikey::keys_since(&store, conn, None).await?;
            let sessions = crate::token::sessions::sessions_since(&store, conn, None).await?;
            TenantAuth::build(users, &verifiers, locks)
                .with_api_keys(keys, true)
                .with_sessions(sessions, true)
        };
        Ok(Arc::new(state))
    }
}

#[cfg(test)]
pub(crate) mod tests;
