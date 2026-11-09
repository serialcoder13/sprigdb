//! An in-memory [`MsgStore`]: temporary queues (exclusive, auto-delete,
//! server-named), unit tests and the owner simulation. It keeps the same
//! rules as MariaDB: one atomic batch, the epoch fence first, `uk_msg`.
//!
//! [`Faults`] injects the simulation's failures: a write that fails before
//! committing, and a write that commits but whose answer is lost.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;

use crate::store::{
    Claim, DelayedRow, LaneState, MsgId, MsgStore, OutDest, OutKind, OutboxRow, StoreError,
    StoredMsg, WriteBatch,
};

/// Failures to inject, each used once.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Faults {
    /// The next write fails without committing.
    pub fail_next_write: bool,
    /// The next write commits, then reports a failure.
    pub lose_next_answer: bool,
}

#[derive(Default)]
struct Lane {
    epoch: u64,
    state: LaneState,
    msgs: BTreeMap<(u8, u64), StoredMsg>,
}

#[derive(Default)]
struct Inner {
    lanes: HashMap<(u64, u16), Lane>,
    ids: HashMap<(u64, MsgId), (u16, u8, u64)>,
    outbox: BTreeMap<MsgId, OutboxRow>,
    delayed: BTreeMap<(i64, MsgId), DelayedRow>,
    faults: Faults,
    writes: u64,
}

/// The in-memory store.
#[derive(Clone, Default)]
pub struct MemStore {
    inner: Arc<Mutex<Inner>>,
}

impl std::fmt::Debug for MemStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemStore").finish_non_exhaustive()
    }
}

impl MemStore {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Arms faults.
    pub fn inject(&self, f: Faults) {
        self.lock().faults = f;
    }

    /// Messages of a queue on "disk", in key order.
    #[must_use]
    pub fn messages(&self, queue: u64) -> Vec<StoredMsg> {
        let g = self.lock();
        g.lanes
            .iter()
            .filter(|((q, _), _)| *q == queue)
            .flat_map(|(_, l)| l.msgs.values().cloned())
            .collect()
    }

    /// The lane state and epoch.
    #[must_use]
    pub fn lane(&self, queue: u64, lane: u16) -> Option<(u64, LaneState)> {
        self.lock()
            .lanes
            .get(&(queue, lane))
            .map(|l| (l.epoch, l.state))
    }

    /// Committed writes so far.
    #[must_use]
    pub fn writes(&self) -> u64 {
        self.lock().writes
    }

    /// Outbox rows waiting.
    #[must_use]
    pub fn outbox_len(&self) -> usize {
        self.lock().outbox.len()
    }

    /// Adds outbox rows (a transactional publish in tests).
    pub fn put_outbox(&self, rows: Vec<OutboxRow>) {
        let mut g = self.lock();
        for r in rows {
            g.outbox.insert(r.out_id, r);
        }
    }
}

#[async_trait]
impl MsgStore for MemStore {
    async fn claim(&self, queue: u64, lane: u16) -> Result<Claim, StoreError> {
        let mut g = self.lock();
        let l = g.lanes.entry((queue, lane)).or_insert_with(|| Lane {
            epoch: 0,
            state: LaneState {
                next_seq: 1,
                ..LaneState::default()
            },
            msgs: BTreeMap::new(),
        });
        l.epoch += 1;
        let count = l.msgs.len() as u64;
        let bytes = l.msgs.values().map(StoredMsg::size).sum();
        Ok(Claim {
            epoch: l.epoch,
            state: l.state,
            count,
            bytes,
        })
    }

    async fn load(
        &self,
        queue: u64,
        lane: u16,
        after: Option<(u8, u64)>,
        limit: usize,
    ) -> Result<Vec<StoredMsg>, StoreError> {
        let g = self.lock();
        let Some(l) = g.lanes.get(&(queue, lane)) else {
            return Ok(Vec::new());
        };
        Ok(match after {
            None => l.msgs.values().take(limit).cloned().collect(),
            Some(k) => l
                .msgs
                .range((std::ops::Bound::Excluded(k), std::ops::Bound::Unbounded))
                .take(limit)
                .map(|(_, m)| m.clone())
                .collect(),
        })
    }

    async fn existing(&self, queue: u64, ids: &[MsgId]) -> Result<Vec<MsgId>, StoreError> {
        let g = self.lock();
        Ok(ids
            .iter()
            .filter(|id| g.ids.contains_key(&(queue, **id)))
            .copied()
            .collect())
    }

    async fn write(&self, b: &WriteBatch) -> Result<(), StoreError> {
        let mut g = self.lock();
        if std::mem::take(&mut g.faults.fail_next_write) {
            return Err(StoreError::Failed("injected: write failed".into()));
        }
        let lose = std::mem::take(&mut g.faults.lose_next_answer);
        let g = &mut *g;
        let Some(l) = g.lanes.get_mut(&(b.queue, b.lane)) else {
            return Err(StoreError::Fenced);
        };
        if l.epoch != b.epoch {
            return Err(StoreError::Fenced);
        }
        // uk_msg: a duplicate insert fails the batch, as in MariaDB.
        for m in &b.inserts {
            if g.ids.contains_key(&(b.queue, m.msg_id)) {
                return Err(StoreError::Failed("duplicate msg_id (uk_msg)".into()));
            }
        }
        for m in &b.inserts {
            g.ids.insert((b.queue, m.msg_id), (m.lane, m.pclass, m.seq));
            l.msgs.insert((m.pclass, m.seq), m.clone());
        }
        for d in &b.deletes {
            if l
                .msgs
                .get(&(d.pclass, d.seq))
                .is_some_and(|m| m.msg_id == d.msg_id)
            {
                l.msgs.remove(&(d.pclass, d.seq));
                g.ids.remove(&(b.queue, d.msg_id));
            }
        }
        for (r, n) in &b.counts {
            if let Some(m) = l.msgs.get_mut(&(r.pclass, r.seq))
                && m.msg_id == r.msg_id
            {
                m.delivery_count = *n;
            }
        }
        for o in &b.outbox {
            g.outbox.insert(o.out_id, o.clone());
        }
        l.state = b.state;
        g.writes += 1;
        if lose {
            return Err(StoreError::Failed("injected: answer lost".into()));
        }
        Ok(())
    }

    async fn purge(
        &self,
        queue: u64,
        lane: u16,
        epoch: u64,
        keep: &[u64],
    ) -> Result<u64, StoreError> {
        let mut g = self.lock();
        let g = &mut *g;
        let Some(l) = g.lanes.get_mut(&(queue, lane)) else {
            return Ok(0);
        };
        if l.epoch != epoch {
            return Err(StoreError::Fenced);
        }
        let gone: Vec<(u8, u64)> = l
            .msgs
            .keys()
            .filter(|(_, s)| !keep.contains(s))
            .copied()
            .collect();
        for k in &gone {
            if let Some(m) = l.msgs.remove(k) {
                g.ids.remove(&(queue, m.msg_id));
            }
        }
        Ok(gone.len() as u64)
    }

    async fn drop_queue(&self, queue: u64) -> Result<(), StoreError> {
        let mut g = self.lock();
        g.lanes.retain(|(q, _), _| *q != queue);
        g.ids.retain(|(q, _), _| *q != queue);
        Ok(())
    }

    async fn outbox(&self, limit: usize) -> Result<Vec<OutboxRow>, StoreError> {
        Ok(self.lock().outbox.values().take(limit).cloned().collect())
    }

    async fn outbox_done(&self, ids: &[MsgId]) -> Result<(), StoreError> {
        let mut g = self.lock();
        for id in ids {
            g.outbox.remove(id);
        }
        Ok(())
    }

    async fn delay(&self, row: &DelayedRow) -> Result<(), StoreError> {
        self.lock()
            .delayed
            .insert((row.due_ms, row.msg_id), row.clone());
        Ok(())
    }

    async fn release_due(&self, now_ms: i64, limit: usize) -> Result<usize, StoreError> {
        let mut g = self.lock();
        let due: Vec<(i64, MsgId)> = g
            .delayed
            .range(..=(now_ms, [0xff; 16]))
            .take(limit)
            .map(|(k, _)| *k)
            .collect();
        for k in &due {
            if let Some(d) = g.delayed.remove(k) {
                let out_id = crate::store::new_msg_id();
                g.outbox.insert(
                    out_id,
                    OutboxRow {
                        out_id,
                        kind: OutKind::Delayed,
                        dest: OutDest::Exchange {
                            name: d.exchange.clone(),
                            key: d.routing_key.clone(),
                        },
                        msg_id: d.msg_id,
                        fmt: d.fmt,
                        exchange: d.exchange,
                        routing_key: d.routing_key,
                        props: d.props,
                        body: d.body,
                        deaths: None,
                    },
                );
            }
        }
        Ok(due.len())
    }

    async fn next_due(&self) -> Result<Option<i64>, StoreError> {
        Ok(self.lock().delayed.keys().next().map(|(t, _)| *t))
    }
}
