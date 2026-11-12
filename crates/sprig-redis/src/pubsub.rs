//! The gateway's pub/sub hub and wait table (chapter 40, "Pub/sub and
//! keyspace notifications", "Blocking commands").
//!
//! These are one gateway's subscribers and waiters. Across gateways,
//! [`crate::fabric`]'s hubs carry publishes and wake-ups between them;
//! without a mesh (tests, a gateway without the peer service) both stay on
//! this gateway. Channels are per tenant.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::Notify;
use tokio::sync::mpsc::UnboundedSender;

use crate::glob;

/// A message for a subscriber.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Msg {
    /// `message channel data`.
    Message(Bytes, Bytes),
    /// `pmessage pattern channel data`.
    PMessage(Bytes, Bytes, Bytes),
    /// `smessage channel data`.
    SMessage(Bytes, Bytes),
}

type Subs = HashMap<u64, UnboundedSender<Msg>>;

#[derive(Debug, Default)]
struct HubInner {
    channels: HashMap<(String, Bytes), Subs>,
    patterns: HashMap<(String, Bytes), Subs>,
    shard: HashMap<(String, Bytes), Subs>,
}

/// Which kind of subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// `SUBSCRIBE`.
    Channel,
    /// `PSUBSCRIBE`.
    Pattern,
    /// `SSUBSCRIBE`.
    Shard,
}

/// Every tenant's channels on this gateway.
#[derive(Debug, Default)]
pub struct Hub {
    inner: Mutex<HubInner>,
}

impl Hub {
    fn with<T>(&self, f: impl FnOnce(&mut HubInner) -> T) -> T {
        let mut g = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f(&mut g)
    }

    /// Subscribes connection `id`.
    pub fn subscribe(
        &self,
        scope: &str,
        kind: Kind,
        name: &Bytes,
        id: u64,
        tx: UnboundedSender<Msg>,
    ) {
        self.with(|h| {
            let m = match kind {
                Kind::Channel => &mut h.channels,
                Kind::Pattern => &mut h.patterns,
                Kind::Shard => &mut h.shard,
            };
            m.entry((scope.to_owned(), name.clone()))
                .or_default()
                .insert(id, tx);
        });
    }

    /// Unsubscribes connection `id`.
    pub fn unsubscribe(&self, scope: &str, kind: Kind, name: &Bytes, id: u64) {
        self.with(|h| {
            let m = match kind {
                Kind::Channel => &mut h.channels,
                Kind::Pattern => &mut h.patterns,
                Kind::Shard => &mut h.shard,
            };
            let key = (scope.to_owned(), name.clone());
            if let Some(s) = m.get_mut(&key) {
                s.remove(&id);
                if s.is_empty() {
                    m.remove(&key);
                }
            }
        });
    }

    /// Publishes; the number of receivers.
    pub fn publish(&self, scope: &str, channel: &Bytes, data: &Bytes, shard: bool) -> i64 {
        self.with(|h| {
            let mut n = 0;
            if shard {
                if let Some(s) = h.shard.get(&(scope.to_owned(), channel.clone())) {
                    for tx in s.values() {
                        if tx
                            .send(Msg::SMessage(channel.clone(), data.clone()))
                            .is_ok()
                        {
                            n += 1;
                        }
                    }
                }
                return n;
            }
            if let Some(s) = h.channels.get(&(scope.to_owned(), channel.clone())) {
                for tx in s.values() {
                    if tx.send(Msg::Message(channel.clone(), data.clone())).is_ok() {
                        n += 1;
                    }
                }
            }
            for ((sc, pat), s) in &h.patterns {
                if sc == scope && glob::matches(pat, channel) {
                    for tx in s.values() {
                        if tx
                            .send(Msg::PMessage(pat.clone(), channel.clone(), data.clone()))
                            .is_ok()
                        {
                            n += 1;
                        }
                    }
                }
            }
            n
        })
    }

    /// Whether tenant `scope` has a subscription here a keyspace
    /// notification could reach: a pattern, or a `__key...` channel (RL51:
    /// the gateway's notifications from the change feed run while one
    /// does).
    #[must_use]
    pub fn keyspace_interest(&self, scope: &str) -> bool {
        self.with(|h| {
            h.patterns.keys().any(|(sc, _)| sc == scope)
                || h.channels
                    .keys()
                    .any(|(sc, c)| sc == scope && c.starts_with(b"__key"))
        })
    }

    /// `PUBSUB CHANNELS` / `SHARDCHANNELS` with an optional pattern.
    #[must_use]
    pub fn channels(&self, scope: &str, pattern: Option<&[u8]>, shard: bool) -> Vec<Bytes> {
        self.with(|h| {
            let m = if shard { &h.shard } else { &h.channels };
            let mut v: Vec<Bytes> = m
                .keys()
                .filter(|(s, c)| s == scope && pattern.is_none_or(|p| glob::matches(p, c)))
                .map(|(_, c)| c.clone())
                .collect();
            v.sort();
            v
        })
    }

    /// `PUBSUB NUMSUB` / `SHARDNUMSUB` of one channel.
    #[must_use]
    pub fn numsub(&self, scope: &str, channel: &Bytes, shard: bool) -> i64 {
        self.with(|h| {
            let m = if shard { &h.shard } else { &h.channels };
            m.get(&(scope.to_owned(), channel.clone()))
                .map_or(0, |s| s.len() as i64)
        })
    }

    /// One tenant's channels with their subscriber counts and its
    /// patterns (`PUBSUB` across gateways).
    #[must_use]
    pub fn snapshot(&self, scope: &str, shard: bool) -> crate::fabric::Introspection {
        self.with(|h| {
            let m = if shard { &h.shard } else { &h.channels };
            let mut out = crate::fabric::Introspection::default();
            for ((s, c), subs) in m {
                if s == scope {
                    out.channels.insert(c.clone(), subs.len() as i64);
                }
            }
            if !shard {
                for (s, p) in h.patterns.keys() {
                    if s == scope {
                        out.patterns.insert(p.clone());
                    }
                }
            }
            out
        })
    }

    /// `PUBSUB NUMPAT`: distinct patterns.
    #[must_use]
    pub fn numpat(&self, scope: &str) -> i64 {
        self.with(|h| h.patterns.keys().filter(|(s, _)| s == scope).count() as i64)
    }
}

/// Blocked clients by `(tenant, database, key)`.
#[derive(Debug, Default)]
pub struct Waits {
    map: Mutex<HashMap<(String, u32, Bytes), Arc<Notify>>>,
    blocked: Mutex<HashMap<String, usize>>,
}

impl Waits {
    /// The wake-up for a key (created on first use).
    #[must_use]
    pub fn notify(&self, scope: &str, db: u32, key: &Bytes) -> Arc<Notify> {
        let mut m = self
            .map
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(
            m.entry((scope.to_owned(), db, key.clone()))
                .or_insert_with(|| Arc::new(Notify::new())),
        )
    }

    /// Wakes clients blocked on a key.
    pub fn wake(&self, scope: &str, db: u32, key: &[u8]) {
        let mut m = self
            .map
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let k = (scope.to_owned(), db, Bytes::copy_from_slice(key));
        if let Some(n) = m.get(&k) {
            n.notify_waiters();
            if Arc::strong_count(n) == 1 {
                m.remove(&k);
            }
        }
    }

    /// Counts a blocked client of `scope`; `false` past `max`.
    pub fn enter(&self, scope: &str, max: usize) -> bool {
        let mut b = self
            .blocked
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let n = b.entry(scope.to_owned()).or_insert(0);
        if *n >= max {
            return false;
        }
        *n += 1;
        true
    }

    /// Uncounts a blocked client.
    pub fn leave(&self, scope: &str) {
        let mut b = self
            .blocked
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(n) = b.get_mut(scope) {
            *n = n.saturating_sub(1);
        }
    }

    /// Blocked clients now, all tenants.
    #[must_use]
    pub fn blocked(&self) -> usize {
        self.blocked.lock().map(|b| b.values().sum()).unwrap_or(0)
    }
}
