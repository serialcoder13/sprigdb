//! `MemStore`: a deterministic, in-memory [`MetaStore`] for tests and
//! `sprig-testkit` (chapter 23, "Interfaces").
//!
//! It keeps etcd's semantics where SprigDB relies on them: one revision per
//! write transaction, multi-version history for listings at a revision,
//! compaction, watches that replay history and then follow live writes, and
//! leases that expire on an injectable [`Clock`]. Lease expiry is checked
//! lazily, at the start of every operation, so a test controls it by
//! advancing a [`ManualClock`](sprig_common::time::ManualClock).
//!
//! A live watcher has a bounded queue. If a consumer falls that far behind,
//! its stream ends, and the consumer resumes from its last revision + 1, as
//! it would after an etcd disconnect.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use futures::StreamExt;
use sprig_common::time::{Clock, SystemClock};
use sprig_common::{Result, SprigCode, SprigError};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    Cmp, Consistency, Key, KeyValue, LeaseId, ListOpts, ListPage, MetaStore, Op, OpResult,
    Revision, Tier, Txn, TxnResult, WatchEvent, WatchStream,
};

/// Queue length of one live watcher.
pub const WATCH_QUEUE: usize = 1024;

/// An in-memory [`MetaStore`].
#[derive(Clone)]
pub struct MemStore {
    tier: Tier,
    clock: Arc<dyn Clock>,
    state: Arc<Mutex<State>>,
}

impl std::fmt::Debug for MemStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemStore")
            .field("tier", &self.tier)
            .finish_non_exhaustive()
    }
}

/// One version of a key; `value: None` is a deletion.
#[derive(Debug, Clone)]
struct KeyVersion {
    mod_revision: Revision,
    create_revision: Revision,
    version: u64,
    value: Option<Vec<u8>>,
    lease: Option<LeaseId>,
}

#[derive(Debug)]
struct Lease {
    ttl: Duration,
    deadline: Instant,
    keys: BTreeSet<Key>,
}

struct Watcher {
    prefix: Key,
    tx: mpsc::Sender<Result<WatchEvent>>,
}

struct State {
    revision: Revision,
    compacted: Revision,
    versions: BTreeMap<Key, Vec<KeyVersion>>,
    /// Every event at or after `compacted`, in order, for watch replay.
    events: Vec<(Revision, WatchEvent)>,
    leases: HashMap<LeaseId, Lease>,
    next_lease: i64,
    watchers: Vec<Watcher>,
    unavailable: bool,
}

impl MemStore {
    /// An empty store on the system clock.
    #[must_use]
    pub fn new(tier: Tier) -> Self {
        Self::with_clock(tier, Arc::new(SystemClock))
    }

    /// An empty store whose leases follow `clock`.
    #[must_use]
    pub fn with_clock(tier: Tier, clock: Arc<dyn Clock>) -> Self {
        Self {
            tier,
            clock,
            state: Arc::new(Mutex::new(State {
                // etcd starts at revision 1.
                revision: 1,
                compacted: 0,
                versions: BTreeMap::new(),
                events: Vec::new(),
                leases: HashMap::new(),
                next_lease: 1,
                watchers: Vec::new(),
                unavailable: false,
            })),
        }
    }

    /// Another tier over the same state, as one etcd serves both tiers in
    /// single-etcd mode.
    #[must_use]
    pub fn with_tier(&self, tier: Tier) -> Self {
        Self {
            tier,
            clock: Arc::clone(&self.clock),
            state: Arc::clone(&self.state),
        }
    }

    /// Fault injection: while `true`, every operation fails with 39500
    /// `ER_SPRIG_META_UNAVAILABLE` and live watch streams end.
    pub fn set_unavailable(&self, unavailable: bool) {
        let mut s = self.lock();
        s.unavailable = unavailable;
        if unavailable {
            s.watchers.clear();
        }
    }

    /// Compacts history at `revision`, as etcd does: listings at older
    /// revisions and watches from them fail afterwards; `revision` itself
    /// stays readable.
    pub fn compact(&self, revision: Revision) {
        let mut s = self.lock();
        let revision = revision.min(s.revision);
        if revision <= s.compacted {
            return;
        }
        s.compacted = revision;
        // etcd keeps the compaction revision itself readable.
        s.events.retain(|(r, _)| *r >= revision);
        s.versions.retain(|_, history| {
            // Keep the newest version at or below the compaction revision,
            // unless it is a deletion, plus everything newer.
            let keep_from = history.iter().rposition(|v| v.mod_revision <= revision);
            if let Some(i) = keep_from {
                let drop = if history[i].value.is_none() { i + 1 } else { i };
                history.drain(..drop);
            }
            !history.is_empty()
        });
    }

    /// Sends a progress mark with the current revision to every watcher.
    pub fn request_progress(&self) {
        let mut s = self.lock();
        let ev = WatchEvent::Progress(s.revision);
        s.watchers.retain(|w| w.tx.try_send(Ok(ev.clone())).is_ok());
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Locks the state, fails if unavailable, and expires due leases.
    fn begin(&self) -> Result<MutexGuard<'_, State>> {
        let mut s = self.lock();
        if s.unavailable {
            return Err(SprigError::new(
                SprigCode::MetaUnavailable,
                "metadata store unavailable (injected)",
            ));
        }
        let now = self.clock.now();
        let mut due: Vec<LeaseId> = s
            .leases
            .iter()
            .filter(|(_, l)| l.deadline <= now)
            .map(|(id, _)| *id)
            .collect();
        due.sort();
        for id in due {
            s.end_lease(id);
        }
        Ok(s)
    }
}

impl State {
    fn current(&self, key: &Key) -> Option<&KeyVersion> {
        self.versions.get(key)?.last().filter(|v| v.value.is_some())
    }

    fn kv(key: &Key, v: &KeyVersion) -> Option<KeyValue> {
        Some(KeyValue {
            key: key.clone(),
            value: v.value.clone()?,
            create_revision: v.create_revision,
            mod_revision: v.mod_revision,
            version: v.version,
            lease: v.lease,
        })
    }

    fn check(&self, cmp: &Cmp) -> bool {
        match cmp {
            Cmp::Missing(k) => self.current(k).is_none(),
            Cmp::CreateRev(k, r) => self.current(k).is_some_and(|v| v.create_revision == *r),
            Cmp::ModRev(k, r) => self.current(k).is_some_and(|v| v.mod_revision == *r),
        }
    }

    /// Writes a version at revision `rev` and records its event.
    fn put(&mut self, key: &Key, value: Vec<u8>, lease: Option<LeaseId>, rev: Revision) {
        let prev = self.current(key).cloned();
        if let Some(l) = prev
            .as_ref()
            .and_then(|p| p.lease)
            .and_then(|old| self.leases.get_mut(&old))
        {
            l.keys.remove(key);
        }
        if let Some(l) = lease.and_then(|id| self.leases.get_mut(&id)) {
            l.keys.insert(key.clone());
        }
        let v = KeyVersion {
            mod_revision: rev,
            create_revision: prev.as_ref().map_or(rev, |p| p.create_revision),
            version: prev.as_ref().map_or(1, |p| p.version + 1),
            value: Some(value),
            lease,
        };
        let kv = Self::kv(key, &v);
        self.versions.entry(key.clone()).or_default().push(v);
        if let Some(kv) = kv {
            self.events.push((rev, WatchEvent::Put(kv)));
        }
    }

    /// Deletes `key` at revision `rev`; `false` if it didn't exist.
    fn delete(&mut self, key: &Key, rev: Revision) -> bool {
        let Some(prev) = self.current(key).cloned() else {
            return false;
        };
        if let Some(l) = prev.lease.and_then(|id| self.leases.get_mut(&id)) {
            l.keys.remove(key);
        }
        self.versions
            .entry(key.clone())
            .or_default()
            .push(KeyVersion {
                mod_revision: rev,
                create_revision: 0,
                version: 0,
                value: None,
                lease: None,
            });
        self.events.push((
            rev,
            WatchEvent::Delete {
                key: key.clone(),
                mod_revision: rev,
            },
        ));
        true
    }

    /// Removes a lease and deletes its keys in one revision.
    fn end_lease(&mut self, id: LeaseId) {
        let Some(lease) = self.leases.remove(&id) else {
            return;
        };
        if lease.keys.is_empty() {
            return;
        }
        let first = self.events.len();
        self.revision += 1;
        let rev = self.revision;
        for k in &lease.keys {
            self.delete(k, rev);
        }
        self.notify(first);
    }

    /// Delivers events from index `first` of the log to live watchers,
    /// dropping any watcher whose queue is full or closed.
    fn notify(&mut self, first: usize) {
        let events = &self.events[first..];
        self.watchers.retain(|w| {
            events
                .iter()
                .filter(|(_, ev)| event_key(ev).is_some_and(|k| k.starts_with(&w.prefix)))
                .all(|(_, ev)| w.tx.try_send(Ok(ev.clone())).is_ok())
        });
    }
}

fn event_key(ev: &WatchEvent) -> Option<&Key> {
    match ev {
        WatchEvent::Put(kv) => Some(&kv.key),
        WatchEvent::Delete { key, .. } => Some(key),
        WatchEvent::Progress(_) | WatchEvent::Compacted { .. } => None,
    }
}

fn compacted_error(at: Revision, compacted: Revision) -> SprigError {
    SprigError::new(
        SprigCode::MetaUnavailable,
        format!("revision {at} is compacted (compaction at {compacted}); list again"),
    )
}

#[async_trait::async_trait]
impl MetaStore for MemStore {
    fn tier(&self) -> &Tier {
        &self.tier
    }

    async fn get(&self, key: &Key, _c: Consistency) -> Result<Option<KeyValue>> {
        let s = self.begin()?;
        Ok(s.current(key).and_then(|v| State::kv(key, v)))
    }

    async fn list(&self, prefix: &Key, opts: ListOpts) -> Result<ListPage> {
        let s = self.begin()?;
        let at = opts.at.unwrap_or(s.revision);
        if at < s.compacted {
            return Err(compacted_error(at, s.compacted));
        }
        if at > s.revision {
            return Err(SprigError::internal(format!(
                "revision {at} is in the future"
            )));
        }
        let start = match &opts.after {
            Some(k) => Bound::Excluded(k),
            None => Bound::Included(prefix),
        };
        let limit = opts.limit.max(1);
        let mut kvs = Vec::new();
        let mut more = false;
        let live = s
            .versions
            .range::<Key, _>((start, Bound::Unbounded))
            .take_while(|(k, _)| k.starts_with(prefix))
            .filter_map(|(k, history)| {
                let v = history.iter().rev().find(|v| v.mod_revision <= at)?;
                State::kv(k, v)
            });
        for kv in live {
            if kvs.len() == limit {
                more = true;
                break;
            }
            kvs.push(kv);
        }
        Ok(ListPage {
            kvs,
            revision: at,
            more,
        })
    }

    async fn txn(&self, txn: Txn) -> Result<TxnResult> {
        let mut s = self.begin()?;
        let succeeded = txn.when.iter().all(|c| s.check(c));
        let ops = if succeeded { txn.then } else { txn.otherwise };
        for op in &ops {
            if let Op::Put {
                lease: Some(id), ..
            } = op
                && !s.leases.contains_key(id)
            {
                return Err(SprigError::internal(format!("lease {} not found", id.0)));
            }
        }
        let writes = ops.iter().any(|op| !matches!(op, Op::Get(_)));
        let rev = s.revision + 1;
        let first = s.events.len();
        let mut results = Vec::with_capacity(ops.len());
        for op in ops {
            results.push(match op {
                Op::Put { key, value, lease } => {
                    s.put(&key, value, lease, rev);
                    OpResult::Put
                }
                Op::Delete(key) => OpResult::Delete(s.delete(&key, rev)),
                Op::Get(key) => OpResult::Get(s.current(&key).and_then(|v| State::kv(&key, v))),
            });
        }
        // A transaction of deletes of missing keys and gets changes nothing.
        if writes && s.events.len() > first {
            s.revision = rev;
            s.notify(first);
        }
        Ok(TxnResult {
            succeeded,
            revision: s.revision,
            results,
        })
    }

    async fn watch(&self, prefix: &Key, from: Revision) -> Result<WatchStream> {
        let mut s = self.begin()?;
        if from != 0 && from < s.compacted {
            let ev = WatchEvent::Compacted {
                compact_revision: s.compacted,
            };
            return Ok(Box::pin(futures::stream::iter([Ok(ev)])));
        }
        let replay: Vec<Result<WatchEvent>> = if from == 0 {
            Vec::new()
        } else {
            s.events
                .iter()
                .filter(|(r, ev)| {
                    *r >= from && event_key(ev).is_some_and(|k| k.starts_with(prefix))
                })
                .map(|(_, ev)| Ok(ev.clone()))
                .collect()
        };
        let (tx, rx) = mpsc::channel(WATCH_QUEUE);
        s.watchers.push(Watcher {
            prefix: prefix.clone(),
            tx,
        });
        Ok(Box::pin(
            futures::stream::iter(replay).chain(ReceiverStream::new(rx)),
        ))
    }

    async fn grant_lease(&self, ttl: Duration) -> Result<LeaseId> {
        let mut s = self.begin()?;
        let id = LeaseId(s.next_lease);
        s.next_lease += 1;
        let deadline = self.clock.now() + ttl;
        s.leases.insert(
            id,
            Lease {
                ttl,
                deadline,
                keys: BTreeSet::new(),
            },
        );
        Ok(id)
    }

    async fn keep_alive(&self, lease: LeaseId) -> Result<Option<Duration>> {
        let mut s = self.begin()?;
        let now = self.clock.now();
        Ok(s.leases.get_mut(&lease).map(|l| {
            l.deadline = now + l.ttl;
            l.ttl
        }))
    }

    async fn revoke_lease(&self, lease: LeaseId) -> Result<()> {
        let mut s = self.begin()?;
        s.end_lease(lease);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
