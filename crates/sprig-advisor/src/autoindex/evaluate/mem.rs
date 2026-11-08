//! In-memory [`EvalStore`], [`StatsSource`] and [`DecisionLog`], for
//! tests and for replaying recorded workloads with simulated time
//! (chapter 13, "Testing": stability).

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};
use std::time::SystemTime;

use sprig_common::Result;
use sprig_common::ids::ObjectId;

use super::{EvalStore, StatsSource};
use crate::autoindex::candidates::FpInput;
use crate::autoindex::model::AutoIndexId;
use crate::autoindex::stats::{AnalyzePlan, TableStats};
use crate::autoindex::store::{
    AutoIndexRow, CandidateRow, CandidateStatus, DecisionLog, IndexDecision, IndexState,
};

#[derive(Debug, Default)]
struct State {
    run: u64,
    candidates: Vec<CandidateRow>,
    indexes: Vec<AutoIndexRow>,
}

/// The catalog tables in memory.
#[derive(Debug, Default)]
pub struct MemStore {
    state: Mutex<State>,
}

impl MemStore {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Every candidate row, by id.
    #[must_use]
    pub fn candidates(&self) -> Vec<CandidateRow> {
        self.lock().candidates.clone()
    }

    /// Every index row, by id.
    #[must_use]
    pub fn indexes(&self) -> Vec<AutoIndexRow> {
        self.lock().indexes.clone()
    }

    /// Adds an index row as it is (an existing automatic index); returns
    /// its id.
    pub fn add_index(&self, mut r: AutoIndexRow) -> AutoIndexId {
        let mut s = self.lock();
        let id = AutoIndexId(s.indexes.len() as u64 + 1);
        r.index_id = id;
        s.indexes.push(r);
        id
    }

    /// Changes an index row's state, as the loops after evaluation would.
    pub fn set_state(&self, id: AutoIndexId, state: IndexState, at: SystemTime) {
        let mut s = self.lock();
        if let Some(r) = s.indexes.iter_mut().find(|r| r.index_id == id) {
            r.state = state;
            r.state_changed_at = at;
        }
    }
}

#[async_trait::async_trait]
impl EvalStore for MemStore {
    async fn next_run_id(&self) -> Result<u64> {
        let mut s = self.lock();
        s.run += 1;
        Ok(s.run)
    }

    async fn indexes_on(&self, object: ObjectId) -> Result<Vec<AutoIndexRow>> {
        Ok(self
            .lock()
            .indexes
            .iter()
            .filter(|r| r.object == object)
            .cloned()
            .collect())
    }

    async fn decided_since(&self, since: SystemTime) -> Result<Vec<CandidateRow>> {
        Ok(self
            .lock()
            .candidates
            .iter()
            .filter(|c| {
                c.created_at >= since
                    && !c.is_skip()
                    && matches!(
                        c.status,
                        CandidateStatus::Selected
                            | CandidateStatus::Rejected
                            | CandidateStatus::Recommended
                            | CandidateStatus::Deferred
                    )
            })
            .cloned()
            .collect())
    }

    async fn created_since(&self, since: SystemTime) -> Result<Vec<AutoIndexRow>> {
        Ok(self
            .lock()
            .indexes
            .iter()
            .filter(|r| r.created_at >= since)
            .cloned()
            .collect())
    }

    async fn insert_candidate(&self, r: &CandidateRow) -> Result<u64> {
        let mut s = self.lock();
        let id = s.candidates.len() as u64 + 1;
        let mut r = r.clone();
        r.candidate_id = id;
        s.candidates.push(r);
        Ok(id)
    }

    async fn put_index(&self, r: &AutoIndexRow) -> Result<Option<AutoIndexId>> {
        let mut s = self.lock();
        if let Some(e) = s
            .indexes
            .iter_mut()
            .find(|e| e.object == r.object && e.name == r.name)
        {
            if !matches!(
                e.state,
                IndexState::Dropped | IndexState::Rejected | IndexState::Failed
            ) {
                return Ok(None);
            }
            let id = e.index_id;
            *e = AutoIndexRow {
                index_id: id,
                ..r.clone()
            };
            return Ok(Some(id));
        }
        let id = AutoIndexId(s.indexes.len() as u64 + 1);
        let mut r = r.clone();
        r.index_id = id;
        s.indexes.push(r);
        Ok(Some(id))
    }
}

/// Statistics in memory; `analyze` only records the plan.
#[derive(Debug, Default)]
pub struct MemStats {
    /// Each object's statistics.
    pub tables: BTreeMap<ObjectId, TableStats>,
    /// Plans run.
    pub analyzed: Mutex<Vec<AnalyzePlan>>,
}

impl MemStats {
    /// Statistics for `tables`.
    #[must_use]
    pub fn new(tables: impl IntoIterator<Item = (ObjectId, TableStats)>) -> Self {
        Self {
            tables: tables.into_iter().collect(),
            analyzed: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl StatsSource for MemStats {
    async fn read(&self, object: ObjectId) -> Result<Option<TableStats>> {
        Ok(self.tables.get(&object).cloned())
    }

    async fn analyze(&self, _object: ObjectId, plan: &AnalyzePlan) -> Result<()> {
        self.analyzed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(plan.clone());
        Ok(())
    }

    async fn prepare(&self, _alias: &str, _stats: &TableStats, _f: &mut FpInput) -> Result<()> {
        Ok(())
    }
}

/// Keeps decisions in memory, numbering them from 1.
#[derive(Debug, Default)]
pub struct MemDecisions {
    /// The decisions, in order.
    pub decisions: Mutex<Vec<IndexDecision>>,
}

impl MemDecisions {
    /// A copy of the decisions so far.
    #[must_use]
    pub fn all(&self) -> Vec<IndexDecision> {
        self.decisions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[async_trait::async_trait]
impl DecisionLog for MemDecisions {
    async fn record(&self, d: &IndexDecision) -> Result<Option<u64>> {
        let mut v = self
            .decisions
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        v.push(d.clone());
        Ok(Some(v.len() as u64))
    }
}
