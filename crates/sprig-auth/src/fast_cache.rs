//! The `caching_sha2_password` fast cache (chapter 14, "Gateway caches").
//!
//! In gateway memory only: an account's `SHA256(SHA256(password))`, filled
//! after a full authentication succeeded, so later logins take chapter 05's
//! fast path (one scramble check, no SHA-256 crypt). An entry is bound to
//! the account's verifiers ([`Account::verifier_key`]): when they change, the
//! entry no longer matches and the next login takes the full path. Entries
//! expire after `auth.fast_cache_ttl_hours` (24 by default).
//!
//! Interim (M0.2): keyed by user name and verifier key; chapter 14's
//! `(tenant, user_id, verifier_id)` key and auth-version invalidation come
//! with the catalog (M0.3).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sprig_common::time::Clock;

use crate::accounts::Account;
use crate::verifier::{FastCacheValue, verify_scramble};

/// `auth.fast_cache_ttl_hours`' default, 24 hours.
pub const DEFAULT_TTL: Duration = Duration::from_secs(24 * 3600);

struct Entry {
    verifier_key: String,
    value: FastCacheValue,
    filled_at: Instant,
}

/// The fast cache. Cheap to share behind an `Arc`.
pub struct FastCache {
    clock: Arc<dyn Clock>,
    ttl: Duration,
    entries: Mutex<HashMap<String, Entry>>,
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

    /// The cached value for `account`, if one was filled under its current
    /// verifiers and hasn't expired.
    #[must_use]
    pub fn get(&self, account: &Account) -> Option<FastCacheValue> {
        let now = self.clock.now();
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        let hit = match entries.get(&account.user) {
            Some(e) if e.verifier_key != account.verifier_key() => None,
            Some(e) if now.saturating_duration_since(e.filled_at) >= self.ttl => None,
            Some(e) => Some(e.value.clone()),
            None => None,
        };
        if hit.is_none() {
            entries.remove(&account.user);
        }
        drop(entries);
        count(if hit.is_some() { "hit" } else { "miss" });
        hit
    }

    /// Records `password` for `account`. Call only after a full
    /// authentication with this password succeeded.
    pub fn fill(&self, account: &Account, password: &[u8]) {
        let entry = Entry {
            verifier_key: account.verifier_key(),
            value: FastCacheValue::from_password(password),
            filled_at: self.clock.now(),
        };
        let now = entry.filled_at;
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        entries.retain(|_, e| now.saturating_duration_since(e.filled_at) < self.ttl);
        entries.insert(account.user.clone(), entry);
    }

    /// The fast path in one call: whether `client_response` to `nonce`
    /// matches the cached value. `false` on a miss or a mismatch; either way
    /// the caller falls through to the full path (chapter 05, step 3).
    #[must_use]
    pub fn verify_scramble(&self, account: &Account, nonce: &[u8], client_response: &[u8]) -> bool {
        let Some(value) = self.get(account) else {
            return false;
        };
        let ok = verify_scramble(nonce, client_response, &value);
        if !ok {
            count("mismatch");
        }
        ok
    }

    /// Drops `user`'s entry.
    pub fn forget(&self, user: &str) {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(user);
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
}
