//! An in-memory [`LogStore`] with the stream log's rules: the leader-epoch
//! fence, `uk_source`, next offsets per partition and the stored feed
//! position, all changed together or not at all. The unit tests and the
//! simulation (`sprig-testkit::sim`) run the real sequencer on it; a hook
//! can fail appends before or after their commit (an ambiguous commit).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};

use sprig_common::{Result, SprigCode, SprigError};

use crate::model::{LeaderEpoch, NewRecord, Offset, PartitionNo, Source};
use crate::sequencer::{AppendOutcome, Appended, Acquired, Batch, LogStore, Start};

/// A record in the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    /// Its offset.
    pub offset: Offset,
    /// The epoch that appended it.
    pub epoch: LeaderEpoch,
    /// The record.
    pub record: NewRecord,
}

/// The log of one (stream, log shard).
#[derive(Debug, Default)]
pub struct MemState {
    /// The shard row's epoch.
    pub epoch: LeaderEpoch,
    /// The shard row's position.
    pub resume: Vec<u8>,
    /// Records per partition, in offset order.
    pub parts: BTreeMap<PartitionNo, Vec<Stored>>,
    /// `uk_source`.
    pub sources: BTreeSet<(PartitionNo, Source)>,
    /// Outbox rows still present (event ids), for cleanup checks.
    pub outbox: BTreeSet<[u8; 16]>,
    /// Appends committed.
    pub commits: u64,
}

/// What a fault hook decides for one append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// Run normally.
    None,
    /// Fail before anything is written (retryable).
    Before,
    /// Commit, then lose the answer (retryable): the sequencer retries a
    /// batch that is already in the log.
    After,
}

/// Decides a fault per append.
pub type FaultHook = Arc<dyn Fn() -> Fault + Send + Sync>;

/// See the module docs.
#[derive(Clone)]
pub struct MemLog {
    /// The state, shared by every "sequencer" of the shard.
    pub state: Arc<Mutex<MemState>>,
    fault: Option<FaultHook>,
}

impl std::fmt::Debug for MemLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemLog").finish_non_exhaustive()
    }
}

fn lost() -> SprigError {
    let mut e = SprigError::new(SprigCode::BackendUnavailable, "injected: log shard unreachable");
    e.retryable = true;
    e
}

impl MemLog {
    /// A log whose feed starts at `start`.
    #[must_use]
    pub fn new(start: &Start) -> Self {
        let state = MemState {
            resume: start.encode(),
            ..MemState::default()
        };
        Self {
            state: Arc::new(Mutex::new(state)),
            fault: None,
        }
    }

    /// The same log with a fault hook.
    #[must_use]
    pub fn with_faults(mut self, hook: FaultHook) -> Self {
        self.fault = Some(hook);
        self
    }

    /// The log's state.
    pub fn lock(&self) -> std::sync::MutexGuard<'_, MemState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records of partition `p` in offset order.
    #[must_use]
    pub fn records(&self, p: PartitionNo) -> Vec<Stored> {
        self.lock().parts.get(&p).cloned().unwrap_or_default()
    }
}

#[async_trait::async_trait]
impl LogStore for MemLog {
    async fn acquire(&self) -> Result<Acquired> {
        let mut s = self.lock();
        s.epoch = LeaderEpoch(s.epoch.0 + 1);
        Ok(Acquired {
            epoch: s.epoch,
            start: Start::decode(&s.resume)?,
        })
    }

    async fn append(&self, epoch: LeaderEpoch, batch: &Batch) -> Result<AppendOutcome> {
        let fault = self.fault.as_ref().map_or(Fault::None, |f| f());
        if fault == Fault::Before {
            return Err(lost());
        }
        let mut s = self.lock();
        if s.epoch != epoch {
            return Ok(AppendOutcome::Fenced { now: s.epoch });
        }
        let mut a = Appended::default();
        for r in &batch.records {
            if !s.sources.insert((r.partition, r.source.clone())) {
                a.duplicates += 1;
                continue;
            }
            let part = s.parts.entry(r.partition).or_default();
            let offset = Offset(part.len() as u64);
            part.push(Stored {
                offset,
                epoch,
                record: r.clone(),
            });
            a.records += 1;
            a.high_watermarks.insert(r.partition, Offset(offset.0 + 1));
        }
        s.resume.clone_from(&batch.resume);
        s.commits += 1;
        drop(s);
        if fault == Fault::After {
            return Err(lost());
        }
        Ok(AppendOutcome::Appended(a))
    }

    async fn cleanup(&self, outbox_ids: &[[u8; 16]]) -> Result<()> {
        let mut s = self.lock();
        for id in outbox_ids {
            s.outbox.remove(id);
        }
        Ok(())
    }
}
