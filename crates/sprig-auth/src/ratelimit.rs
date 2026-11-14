//! Login rate limits.
//!
//! | Scope | Rule | Response |
//! |---|---|---|
//! | Source address, per gateway | more than `auth.ip_failures_per_minute` failures in 60 s | refuse the address for `auth.ip_block_seconds` with 39300 before the greeting |
//! | Account and source address | after `auth.delay_after_failures` failures in a row | delay each further failure by `auth.failure_delay_ms`, doubling up to `auth.failure_delay_max_ms` |
//!
//! Hashing CPU slots (39301 after a 2 s wait) are in [`crate::mysql`].
//!
//! Both limiters live in gateway memory, per process:
//! an attacker spreading guesses over many gateways meets the account
//! lockout ([`crate::policy`]), which is shared through the catalog.
//! Delays are timers (`tokio::time::sleep`) that block no thread, and they
//! are keyed by account *and* address, so an attacker can't slow a
//! victim's logins from elsewhere. Unknown accounts are keyed by the login
//! name as sent, so they are delayed exactly like known ones.
//!
//! Memory is bounded: each map keeps at most [`MAX_ENTRIES`] keys; idle
//! keys are pruned first, then the oldest.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::net::IpAddr;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use sprig_common::ids::TenantId;
use sprig_common::time::Clock;
use sprig_common::{SprigCode, SprigError};

/// The most keys each limiter keeps.
pub const MAX_ENTRIES: usize = 100_000;

/// The failure window of the address limit.
pub const WINDOW: Duration = Duration::from_secs(60);

/// How long a failure-delay key is remembered without failures.
pub const DELAY_IDLE: Duration = Duration::from_secs(15 * 60);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Debug, Default)]
struct IpState {
    failures: VecDeque<Instant>,
    blocked_until: Option<Instant>,
}

/// Failed logins per source address, per gateway.
pub struct IpLimiter {
    clock: Arc<dyn Clock>,
    ips: Mutex<HashMap<IpAddr, IpState>>,
}

impl std::fmt::Debug for IpLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IpLimiter")
            .field("addresses", &self.len())
            .finish_non_exhaustive()
    }
}

impl IpLimiter {
    /// An empty limiter timed by `clock`.
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            clock,
            ips: Mutex::new(HashMap::new()),
        }
    }

    /// How much longer `ip` is refused, if it is.
    #[must_use]
    pub fn blocked(&self, ip: IpAddr) -> Option<Duration> {
        let now = self.clock.now();
        let ips = lock(&self.ips);
        ips.get(&ip)
            .and_then(|s| s.blocked_until)
            .and_then(|u| u.checked_duration_since(now))
            .filter(|d| !d.is_zero())
    }

    /// Counts a failed login from `ip`. With more than `per_minute`
    /// failures in the last 60 s the address is refused for `block`;
    /// returns `true` when this failure started the block. `per_minute =
    /// 0` never blocks.
    pub fn record_failure(&self, ip: IpAddr, per_minute: u64, block: Duration) -> bool {
        if per_minute == 0 {
            return false;
        }
        let now = self.clock.now();
        let mut ips = lock(&self.ips);
        if ips.len() >= MAX_ENTRIES && !ips.contains_key(&ip) {
            prune_ips(&mut ips, now);
        }
        let s = ips.entry(ip).or_default();
        while s
            .failures
            .front()
            .is_some_and(|t| now.duration_since(*t) >= WINDOW)
        {
            s.failures.pop_front();
        }
        s.failures.push_back(now);
        let cap = usize::try_from(per_minute)
            .unwrap_or(usize::MAX)
            .saturating_add(1);
        while s.failures.len() > cap {
            s.failures.pop_front();
        }
        let blocked_now = s.blocked_until.is_some_and(|u| u > now);
        if !blocked_now && s.failures.len() as u64 > per_minute {
            s.blocked_until = Some(now + block);
            s.failures.clear();
            return true;
        }
        false
    }

    /// Lifts every block and forgets every failure of `ip`.
    pub fn clear(&self, ip: IpAddr) {
        lock(&self.ips).remove(&ip);
    }

    /// Addresses tracked now.
    #[must_use]
    pub fn len(&self) -> usize {
        lock(&self.ips).len()
    }

    /// Whether no address is tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn prune_ips(ips: &mut HashMap<IpAddr, IpState>, now: Instant) {
    ips.retain(|_, s| {
        s.blocked_until.is_some_and(|u| u > now)
            || s.failures
                .back()
                .is_some_and(|t| now.duration_since(*t) < WINDOW)
    });
    evict_oldest(ips, |s| {
        s.failures
            .back()
            .copied()
            .or(s.blocked_until)
            .unwrap_or(now)
    });
}

/// Drops the oldest tenth of `m` when it is still full.
fn evict_oldest<K: Eq + Hash + Clone, V>(m: &mut HashMap<K, V>, at: impl Fn(&V) -> Instant) {
    if m.len() < MAX_ENTRIES {
        return;
    }
    let mut ages: Vec<(Instant, K)> = m.iter().map(|(k, v)| (at(v), k.clone())).collect();
    ages.sort_by_key(|(t, _)| *t);
    for (_, k) in ages.into_iter().take(MAX_ENTRIES / 10) {
        m.remove(&k);
    }
}

/// 39300 for an address refused until `left` passes.
#[must_use]
pub fn rate_limited(ip: IpAddr, left: Duration) -> SprigError {
    SprigError::new(
        SprigCode::LoginRateLimited,
        format!(
            "Host '{ip}' is blocked for {} s because of too many failed logins",
            left.as_secs().max(1)
        ),
    )
}

/// The key of a failure delay: the account (or the login name as sent, for
/// unknown accounts) and the source address.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DelayKey {
    /// The tenant, if one resolved.
    pub tenant: Option<TenantId>,
    /// The user name, or the login name as sent.
    pub user: String,
    /// The source address.
    pub ip: IpAddr,
}

/// The failure delay rule: `auth.delay_after_failures`,
/// `auth.failure_delay_ms` and `auth.failure_delay_max_ms`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DelayRule {
    /// Failures in a row before failures are delayed; 0 never.
    pub after: u64,
    /// The first delay.
    pub base: Duration,
    /// The longest delay.
    pub max: Duration,
}

impl Default for DelayRule {
    fn default() -> Self {
        Self {
            after: 3,
            base: Duration::from_millis(1000),
            max: Duration::from_millis(10_000),
        }
    }
}

impl DelayRule {
    /// The delay of a failure that is the `n`th in a row (1-based):
    /// nothing up to `after`, then `base`, doubling, up to `max`.
    #[must_use]
    pub fn delay(&self, n: u64) -> Duration {
        if self.after == 0 || n <= self.after {
            return Duration::ZERO;
        }
        let doublings = u32::try_from((n - self.after - 1).min(30)).unwrap_or(30);
        self.base
            .checked_mul(1 << doublings)
            .unwrap_or(self.max)
            .min(self.max)
    }
}

#[derive(Debug, Clone, Copy)]
struct DelayState {
    failures: u64,
    last: Instant,
}

/// Consecutive failures per account and address, for failure delays.
pub struct FailureDelays {
    clock: Arc<dyn Clock>,
    keys: Mutex<HashMap<DelayKey, DelayState>>,
}

impl std::fmt::Debug for FailureDelays {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FailureDelays")
            .field("keys", &self.len())
            .finish_non_exhaustive()
    }
}

impl FailureDelays {
    /// An empty table timed by `clock`.
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            clock,
            keys: Mutex::new(HashMap::new()),
        }
    }

    /// Counts a failure of `key` and returns how long to delay its
    /// response under `rule`.
    pub fn on_failure(&self, key: DelayKey, rule: DelayRule) -> Duration {
        let now = self.clock.now();
        let mut keys = lock(&self.keys);
        if keys.len() >= MAX_ENTRIES && !keys.contains_key(&key) {
            keys.retain(|_, s| now.duration_since(s.last) < DELAY_IDLE);
            evict_oldest(&mut keys, |s| s.last);
        }
        let s = keys.entry(key).or_insert(DelayState {
            failures: 0,
            last: now,
        });
        if now.duration_since(s.last) >= DELAY_IDLE {
            s.failures = 0;
        }
        s.failures = s.failures.saturating_add(1);
        s.last = now;
        rule.delay(s.failures)
    }

    /// A success of `key`: its count starts again.
    pub fn on_success(&self, key: &DelayKey) {
        lock(&self.keys).remove(key);
    }

    /// Keys tracked now.
    #[must_use]
    pub fn len(&self) -> usize {
        lock(&self.keys).len()
    }

    /// Whether no key is tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::net::Ipv4Addr;

    use sprig_common::time::ManualClock;

    use super::*;

    fn ip(n: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, n))
    }

    #[test]
    fn address_is_blocked_after_too_many_failures_in_a_minute() {
        let clock = Arc::new(ManualClock::new());
        let l = IpLimiter::new(clock.clone());
        let block = Duration::from_secs(300);
        for _ in 0..3 {
            assert!(!l.record_failure(ip(1), 3, block));
        }
        assert!(l.blocked(ip(1)).is_none());
        assert!(l.record_failure(ip(1), 3, block), "the 4th failure blocks");
        assert_eq!(l.blocked(ip(1)), Some(block));
        assert!(l.blocked(ip(2)).is_none(), "other addresses are unaffected");
        clock.advance(Duration::from_secs(299));
        assert_eq!(l.blocked(ip(1)), Some(Duration::from_secs(1)));
        clock.advance(Duration::from_secs(1));
        assert!(l.blocked(ip(1)).is_none(), "the block ends by itself");
    }

    #[test]
    fn failures_older_than_a_minute_dont_count() {
        let clock = Arc::new(ManualClock::new());
        let l = IpLimiter::new(clock.clone());
        let block = Duration::from_secs(10);
        for _ in 0..3 {
            l.record_failure(ip(1), 3, block);
            clock.advance(Duration::from_secs(25));
        }
        // 3 failures, but the first is 75 s old.
        assert!(!l.record_failure(ip(1), 3, block));
        assert!(l.blocked(ip(1)).is_none());
        assert!(!l.record_failure(ip(1), 0, block), "0 never blocks");
    }

    #[test]
    fn delays_double_up_to_the_maximum() {
        let r = DelayRule {
            after: 3,
            base: Duration::from_millis(1000),
            max: Duration::from_millis(5000),
        };
        let d: Vec<u64> = (1..=7)
            .map(|n| u64::try_from(r.delay(n).as_millis()).unwrap())
            .collect();
        assert_eq!(d, vec![0, 0, 0, 1000, 2000, 4000, 5000]);
        assert_eq!(
            DelayRule { after: 0, ..r }.delay(100),
            Duration::ZERO,
            "after = 0 never delays"
        );
        assert_eq!(r.delay(u64::MAX), Duration::from_millis(5000));
    }

    #[test]
    fn delays_are_per_account_and_address_and_reset_on_success() {
        let clock = Arc::new(ManualClock::new());
        let d = FailureDelays::new(clock.clone());
        let rule = DelayRule {
            after: 1,
            base: Duration::from_millis(100),
            max: Duration::from_secs(1),
        };
        let k = |u: &str, n| DelayKey {
            tenant: Some(TenantId(1)),
            user: u.into(),
            ip: ip(n),
        };
        assert_eq!(d.on_failure(k("alice", 1), rule), Duration::ZERO);
        assert_eq!(
            d.on_failure(k("alice", 1), rule),
            Duration::from_millis(100)
        );
        // The victim, from elsewhere, isn't slowed.
        assert_eq!(d.on_failure(k("alice", 2), rule), Duration::ZERO);
        assert_eq!(
            d.on_failure(k("alice", 1), rule),
            Duration::from_millis(200)
        );
        d.on_success(&k("alice", 1));
        assert_eq!(d.on_failure(k("alice", 1), rule), Duration::ZERO);
        clock.advance(DELAY_IDLE);
        assert_eq!(
            d.on_failure(k("alice", 1), rule),
            Duration::ZERO,
            "an idle key starts again"
        );
    }

    #[test]
    fn rate_limited_is_39300() {
        let e = rate_limited(ip(1), Duration::from_millis(10));
        assert_eq!(e.code(), Some(SprigCode::LoginRateLimited));
        assert!(e.message.contains("1 s"));
    }
}
