//! The `caching_sha2_password` fast cache (chapter 14, "Gateway caches").
//!
//! In gateway memory only: an account's `SHA256(SHA256(password))`, filled
//! after a full authentication succeeded, so later logins take chapter 05's
//! fast path (one scramble check, no SHA-256 crypt). Entries expire after
//! `auth.fast_cache_ttl_hours` (24 by default).
//!
//! Two kinds of entries:
//! - **Catalog accounts** ([`FastKey`]): `(tenant, user_id, verifier_id)`,
//!   up to two per account (primary and secondary password). Verifier ids
//!   are never reused, so a password change makes the old entries
//!   unreachable at once; they age out or are pruned on the next fill.
//! - **Interim file accounts** (decision D1): keyed by user name and bound
//!   to the account's verifiers ([`Account::verifier_key`]); a changed
//!   verifier misses.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sprig_common::ids::{TenantId, UserId};
use sprig_common::time::Clock;

use crate::accounts::Account;
use crate::verifier::{FastCacheValue, verify_scramble};

/// `auth.fast_cache_ttl_hours`' default, 24 hours.
pub const DEFAULT_TTL: Duration = Duration::from_secs(24 * 3600);

/// A catalog account's fast-cache key: one verifier of one user.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FastKey {
    /// The tenant.
    pub tenant: TenantId,
    /// The user.
    pub user: UserId,
    /// `verifiers.verifier_id` of the `caching_sha2_password` verifier the
    /// password matched.
    pub verifier: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Key {
    Catalog(FastKey),
    File(String),
}

struct Entry {
    verifier_key: String,
    value: FastCacheValue,
    filled_at: Instant,
}

/// The fast cache. Cheap to share behind an `Arc`.
pub struct FastCache {
    clock: Arc<dyn Clock>,
    ttl: Duration,
    entries: Mutex<HashMap<Key, Entry>>,
}

impl std::fmt::Debug for FastCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FastCache")
            .field("ttl", &self.ttl)
            .field("entries", &self.len())
            .finish_non_exhaustive()
    }
}

impl FastCache {
    /// An empty cache whose entries live `ttl`.
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>, ttl: Duration) -> Self {
        Self {
            clock,
            ttl,
            entries: Mutex::new(HashMap::new()),
        }
    }

    fn lookup(&self, key: &Key, verifier_key: &str) -> Option<FastCacheValue> {
        let now = self.clock.now();
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        let hit = match entries.get(key) {
            Some(e) if e.verifier_key != verifier_key => None,
            Some(e) if now.saturating_duration_since(e.filled_at) >= self.ttl => None,
            Some(e) => Some(e.value.clone()),
            None => None,
        };
        if hit.is_none() {
            entries.remove(key);
        }
        drop(entries);
        count(if hit.is_some() { "hit" } else { "miss" });
        hit
    }

    fn insert(&self, key: Key, verifier_key: String, password: &[u8]) {
        let entry = Entry {
            verifier_key,
            value: FastCacheValue::from_password(password),
            filled_at: self.clock.now(),
        };
        let now = entry.filled_at;
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        entries.retain(|_, e| now.saturating_duration_since(e.filled_at) < self.ttl);
        entries.insert(key, entry);
    }

    fn check(value: Option<FastCacheValue>, nonce: &[u8], client_response: &[u8]) -> bool {
        let Some(value) = value else {
            return false;
        };
        let ok = verify_scramble(nonce, client_response, &value);
        if !ok {
            count("mismatch");
        }
        ok
    }

    // ---- catalog accounts ----

    /// The cached value for `key`, if filled and not expired.
    #[must_use]
    pub fn get_key(&self, key: &FastKey) -> Option<FastCacheValue> {
        self.lookup(&Key::Catalog(*key), "")
    }

    /// Records `password` for `key`. Call only after a full authentication
    /// with this password matched that verifier.
    pub fn fill_key(&self, key: &FastKey, password: &[u8]) {
        self.insert(Key::Catalog(*key), String::new(), password);
    }

    /// The fast path for one catalog verifier: whether `client_response`
    /// to `nonce` matches the value cached for `key`. `false` on a miss or
    /// a mismatch.
    #[must_use]
    pub fn verify_scramble_key(&self, key: &FastKey, nonce: &[u8], client_response: &[u8]) -> bool {
        Self::check(self.get_key(key), nonce, client_response)
    }

    /// The fast path for a clear-text password (HTTP Basic, Redis `AUTH`):
    /// whether `SHA256(SHA256(password))` equals the value cached for
    /// `key`, compared in constant time. `false` on a miss.
    #[must_use]
    pub fn verify_password_key(&self, key: &FastKey, password: &[u8]) -> bool {
        use subtle::ConstantTimeEq as _;
        self.get_key(key).is_some_and(|v| {
            let given = FastCacheValue::from_password(password);
            bool::from(v.as_bytes().ct_eq(given.as_bytes()))
        })
    }

    /// Drops every entry of `tenant`'s user `user`.
    pub fn forget_user(&self, tenant: TenantId, user: UserId) {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|k, _| !matches!(k, Key::Catalog(f) if f.tenant == tenant && f.user == user));
    }

    // ---- interim file accounts ----

    /// The cached value for `account`, if one was filled under its current
    /// verifiers and hasn't expired.
    #[must_use]
    pub fn get(&self, account: &Account) -> Option<FastCacheValue> {
        self.lookup(&Key::File(account.user.clone()), &account.verifier_key())
    }

    /// Records `password` for `account`. Call only after a full
    /// authentication with this password succeeded.
    pub fn fill(&self, account: &Account, password: &[u8]) {
        self.insert(
            Key::File(account.user.clone()),
            account.verifier_key(),
            password,
        );
    }

    /// The fast path in one call: whether `client_response` to `nonce`
    /// matches the cached value. `false` on a miss or a mismatch; either way
    /// the caller falls through to the full path (chapter 05, step 3).
    #[must_use]
    pub fn verify_scramble(&self, account: &Account, nonce: &[u8], client_response: &[u8]) -> bool {
        Self::check(self.get(account), nonce, client_response)
    }

    /// Drops `user`'s entry (file accounts).
    pub fn forget(&self, user: &str) {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&Key::File(user.to_owned()));
    }

    /// How many entries are held, expired ones included.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Whether the cache holds nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn count(result: &'static str) {
    metrics::counter!("sprig_auth_fast_cache_total", "result" => result).increment(1);
}

#[cfg(test)]
mod tests {
    use sprig_common::time::ManualClock;

    use super::*;
    use crate::accounts::BackendTarget;
    use crate::verifier::{CachingSha2Verifier, caching_sha2_scramble};

    fn account(pw: &[u8]) -> Account {
        Account {
            user: "app".into(),
            caching_sha2: Some(CachingSha2Verifier::create(pw, 5000).unwrap()),
            native: None,
            backend: BackendTarget {
                cluster: sprig_common::ids::ClusterId::new("dc-a-c1").unwrap(),
                user: "root".into(),
                credentials: "env:X".parse().unwrap(),
            },
        }
    }

    #[test]
    fn filled_after_full_auth_then_hit_until_ttl() {
        let clock = Arc::new(ManualClock::new());
        let cache = FastCache::new(clock.clone(), Duration::from_secs(60));
        let a = account(b"pw");
        let nonce = [9u8; 20];
        let resp = caching_sha2_scramble(b"pw", &nonce);
        assert!(!cache.verify_scramble(&a, &nonce, &resp), "empty cache");
        cache.fill(&a, b"pw");
        assert!(cache.verify_scramble(&a, &nonce, &resp));
        assert!(!cache.verify_scramble(&a, &nonce, &caching_sha2_scramble(b"px", &nonce)));
        clock.advance(Duration::from_secs(61));
        assert!(!cache.verify_scramble(&a, &nonce, &resp), "expired");
        assert!(cache.is_empty());
    }

    #[test]
    fn a_changed_verifier_misses() {
        let cache = FastCache::new(Arc::new(ManualClock::new()), DEFAULT_TTL);
        let old = account(b"old");
        cache.fill(&old, b"old");
        assert!(cache.get(&old).is_some());
        // The accounts file now holds a new password for the same user.
        let new = account(b"new");
        assert!(cache.get(&new).is_none());
        assert_eq!(cache.len(), 0);
        cache.fill(&new, b"new");
        cache.forget("app");
        assert!(cache.get(&new).is_none());
    }

    #[test]
    fn catalog_keys_are_per_verifier() {
        let cache = FastCache::new(Arc::new(ManualClock::new()), DEFAULT_TTL);
        let t = TenantId(7);
        let primary = FastKey {
            tenant: t,
            user: UserId(1),
            verifier: 10,
        };
        let secondary = FastKey {
            verifier: 11,
            ..primary
        };
        let nonce = [3u8; 20];
        cache.fill_key(&primary, b"new");
        cache.fill_key(&secondary, b"old");
        assert!(cache.verify_scramble_key(
            &primary,
            &nonce,
            &caching_sha2_scramble(b"new", &nonce)
        ));
        assert!(cache.verify_scramble_key(
            &secondary,
            &nonce,
            &caching_sha2_scramble(b"old", &nonce)
        ));
        assert!(!cache.verify_scramble_key(
            &primary,
            &nonce,
            &caching_sha2_scramble(b"old", &nonce)
        ));
        // A new verifier id (a password change) misses.
        assert!(
            cache
                .get_key(&FastKey {
                    verifier: 12,
                    ..primary
                })
                .is_none()
        );
        cache.forget_user(t, UserId(1));
        assert!(cache.is_empty());
    }
}
