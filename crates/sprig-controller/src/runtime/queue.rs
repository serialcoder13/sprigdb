//! The deduplicated key queue of one loop (chapter 23, "The reconciler
//! pattern and the loops").
//!
//! ```text
//! 1. a deduplicated key queue; at most one reconcile per key at a time
//! 3. Done -> reset backoff; RequeueAfter(d) -> add_after(k, d);
//!    Blocked -> gauge, add_after(k, 30 s); Err -> add_after(k, backoff(k))
//!    backoff: 100 ms doubling to 5 min, ±20% jitter
//! ```
//!
//! A key added while it is being reconciled is marked dirty and queued
//! again when that reconcile ends, so no change is lost and no key runs
//! twice at once. Time is tokio's, so a paused runtime drives it.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::time::Duration;

use tokio::time::Instant;

use super::Outcome;

/// First retry delay after an error.
pub const BACKOFF_BASE: Duration = Duration::from_millis(100);
/// Longest retry delay after errors.
pub const BACKOFF_MAX: Duration = Duration::from_secs(300);
/// Wait before looking at a blocked key again.
pub const BLOCKED_RETRY: Duration = Duration::from_secs(30);

/// The delay after `failures` consecutive errors (1 for the first), before
/// jitter: 100 ms doubling to 5 min.
#[must_use]
pub fn backoff(failures: u32) -> Duration {
    let shift = failures.saturating_sub(1).min(31);
    BACKOFF_BASE.saturating_mul(1u32 << shift).min(BACKOFF_MAX)
}

/// A small deterministic generator (`SplitMix64`) for jitter, so a seeded
/// simulation replays exactly.
#[derive(Debug, Clone)]
pub(crate) struct Jitter(u64);

impl Jitter {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// `d` scaled by a factor in [0.8, 1.2).
    pub(crate) fn apply(&mut self, d: Duration) -> Duration {
        // 0..=4095 maps onto 0.8..1.2 in steps of 0.4/4096.
        let r = self.next_u64() % 4096;
        let factor = 0.8 + 0.4 * (f64::from(u32::try_from(r).unwrap_or(0)) / 4096.0);
        d.mul_f64(factor)
    }
}

/// The queue of one loop.
pub(crate) struct WorkQueue<K> {
    ready: VecDeque<K>,
    queued: HashSet<K>,
    active: HashSet<K>,
    dirty: HashSet<K>,
    delayed: BTreeMap<(Instant, u64), K>,
    delayed_at: HashMap<K, Instant>,
    seq: u64,
    failures: HashMap<K, u32>,
    blocked: HashSet<K>,
    jitter: Jitter,
}

impl<K: Clone + Eq + Hash> WorkQueue<K> {
    pub(crate) fn new(seed: u64) -> Self {
        Self {
            ready: VecDeque::new(),
            queued: HashSet::new(),
            active: HashSet::new(),
            dirty: HashSet::new(),
            delayed: BTreeMap::new(),
            delayed_at: HashMap::new(),
            seq: 0,
            failures: HashMap::new(),
            blocked: HashSet::new(),
            jitter: Jitter::new(seed),
        }
    }

    /// Queues `k` now, unless it is already queued; a key being reconciled
    /// is marked dirty instead.
    pub(crate) fn add(&mut self, k: K) {
        if self.active.contains(&k) {
            self.dirty.insert(k);
        } else if self.queued.insert(k.clone()) {
            self.ready.push_back(k);
        }
    }

    /// Queues `k` once `d` has passed. An earlier pending time wins.
    pub(crate) fn add_after(&mut self, k: K, d: Duration, now: Instant) {
        if d.is_zero() {
            self.add(k);
            return;
        }
        let at = now + d;
        if self.delayed_at.get(&k).is_some_and(|&t| t <= at) {
            return;
        }
        self.seq += 1;
        self.delayed.insert((at, self.seq), k.clone());
        self.delayed_at.insert(k, at);
    }

    /// The next key to reconcile, now marked active.
    pub(crate) fn pop(&mut self) -> Option<K> {
        let k = self.ready.pop_front()?;
        self.queued.remove(&k);
        self.active.insert(k.clone());
        Some(k)
    }

    /// Records how a reconcile of `k` ended and schedules what follows.
    /// `Err` carries nothing: every error backs off the same way.
    pub(crate) fn done(&mut self, k: K, outcome: Result<&Outcome, ()>, now: Instant) {
        self.active.remove(&k);
        match outcome {
            Ok(Outcome::Done) => {
                self.failures.remove(&k);
                self.blocked.remove(&k);
            }
            Ok(Outcome::RequeueAfter(d)) => {
                self.failures.remove(&k);
                self.blocked.remove(&k);
                self.add_after(k.clone(), *d, now);
            }
            Ok(Outcome::Blocked { .. }) => {
                self.failures.remove(&k);
                self.blocked.insert(k.clone());
                self.add_after(k.clone(), BLOCKED_RETRY, now);
            }
            Err(()) => {
                let n = self.failures.entry(k.clone()).or_insert(0);
                *n = n.saturating_add(1);
                let d = self.jitter.apply(backoff(*n));
                self.add_after(k.clone(), d, now);
            }
        }
        if self.dirty.remove(&k) {
            self.add(k);
        }
    }

    /// Moves every key whose time has come to the ready queue.
    pub(crate) fn promote_due(&mut self, now: Instant) {
        while let Some(entry) = self.delayed.first_entry() {
            if entry.key().0 > now {
                break;
            }
            let ((at, _), k) = entry.remove_entry();
            if self.delayed_at.get(&k) == Some(&at) {
                self.delayed_at.remove(&k);
                self.add(k);
            }
        }
    }

    /// When the next delayed key is due.
    pub(crate) fn next_due(&self) -> Option<Instant> {
        self.delayed.first_key_value().map(|((at, _), _)| *at)
    }

    /// Keys waiting: ready plus delayed.
    pub(crate) fn depth(&self) -> usize {
        self.ready.len() + self.delayed_at.len()
    }

    /// Keys whose last reconcile reported `Blocked`.
    pub(crate) fn blocked(&self) -> usize {
        self.blocked.len()
    }

    /// Consecutive errors of `k`.
    #[cfg(test)]
    pub(crate) fn failures(&self, k: &K) -> u32 {
        self.failures.get(k).copied().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_to_five_minutes() {
        assert_eq!(backoff(1), Duration::from_millis(100));
        assert_eq!(backoff(2), Duration::from_millis(200));
        assert_eq!(backoff(4), Duration::from_millis(800));
        assert_eq!(backoff(20), BACKOFF_MAX);
        assert_eq!(backoff(u32::MAX), BACKOFF_MAX);
    }

    #[test]
    fn jitter_stays_within_twenty_percent() {
        let mut j = Jitter::new(7);
        let d = Duration::from_secs(10);
        let mut seen_low = false;
        let mut seen_high = false;
        for _ in 0..1000 {
            let x = j.apply(d);
            assert!(
                x >= Duration::from_secs(8) && x < Duration::from_secs(12),
                "{x:?}"
            );
            seen_low |= x < Duration::from_secs(9);
            seen_high |= x > Duration::from_secs(11);
        }
        assert!(seen_low && seen_high);
        // Deterministic per seed.
        assert_eq!(Jitter::new(1).apply(d), Jitter::new(1).apply(d));
    }

    #[test]
    fn keys_are_deduplicated_and_run_once_at_a_time() {
        let now = Instant::now();
        let mut q = WorkQueue::new(1);
        q.add("a");
        q.add("b");
        q.add("a");
        assert_eq!(q.depth(), 2);
        assert_eq!(q.pop(), Some("a"));
        // Added while active: not runnable again until done.
        q.add("a");
        assert_eq!(q.pop(), Some("b"));
        assert_eq!(q.pop(), None);
        q.done("a", Ok(&Outcome::Done), now);
        assert_eq!(q.pop(), Some("a"), "dirty key runs again after done");
        q.done("a", Ok(&Outcome::Done), now);
        q.done("b", Ok(&Outcome::Done), now);
        assert_eq!(q.pop(), None);
        assert_eq!(q.depth(), 0);
    }

    #[test]
    fn delays_errors_and_blocks() {
        let t0 = Instant::now();
        let mut q = WorkQueue::new(3);
        q.add("k");
        q.pop();
        q.done("k", Ok(&Outcome::RequeueAfter(Duration::from_secs(5))), t0);
        assert_eq!(q.pop(), None);
        assert_eq!(q.next_due(), Some(t0 + Duration::from_secs(5)));
        // An earlier time wins; a later one doesn't push it back.
        q.add_after("k", Duration::from_secs(2), t0);
        q.add_after("k", Duration::from_secs(9), t0);
        assert_eq!(q.depth(), 1);
        q.promote_due(t0 + Duration::from_secs(1));
        assert_eq!(q.pop(), None);
        q.promote_due(t0 + Duration::from_secs(2));
        assert_eq!(q.pop(), Some("k"));
        // The stale 5 s entry doesn't queue it twice.
        q.promote_due(t0 + Duration::from_secs(6));
        assert_eq!(q.pop(), None);

        // Errors back off, growing; success resets.
        q.done("k", Err(()), t0);
        assert_eq!(q.failures(&"k"), 1);
        let first = q.next_due().unwrap() - t0;
        assert!(first >= Duration::from_millis(80) && first < Duration::from_millis(120));
        q.promote_due(t0 + Duration::from_secs(1));
        q.pop();
        q.done("k", Err(()), t0);
        let second = q.next_due().unwrap() - t0;
        assert!(second >= Duration::from_millis(160) && second < Duration::from_millis(240));
        q.promote_due(t0 + Duration::from_secs(1));
        q.pop();
        q.done("k", Ok(&Outcome::Blocked { reason: "slots" }), t0);
        assert_eq!((q.failures(&"k"), q.blocked()), (0, 1));
        assert_eq!(q.next_due(), Some(t0 + BLOCKED_RETRY));
        q.promote_due(t0 + BLOCKED_RETRY);
        q.pop();
        q.done("k", Ok(&Outcome::Done), t0);
        assert_eq!(q.blocked(), 0);
    }
}
