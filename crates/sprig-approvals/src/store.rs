//! The store interface and the memory store (chapter 52, "Where
//! recommendations live"). [`catalog`](crate::catalog) and
//! [`etcd`](crate::etcd) are the durable ones.

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sprig_common::ids::{DbId, ObjectId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};

use crate::machine::{self, EventKind, Ticket, Verdict, VerdictPlan};
use crate::model::{Kind, Level, Micros, Proposal, RecId, Recommendation, State};
use crate::policy::Policy;

/// The due index assumes the shortest applying timeout (1 hour); the tick
/// itself uses the real one.
pub const DUE_APPLYING_TIMEOUT: Duration = Duration::from_secs(3600);

/// What a raise did.
#[derive(Debug, Clone, PartialEq)]
pub struct Raised {
    /// The gate's answer.
    pub verdict: Verdict,
    /// The item the answer is about, as stored.
    pub rec: Option<Recommendation>,
    /// The event to publish about it.
    pub event: Option<EventKind>,
    /// Items superseded on the way.
    pub superseded: Vec<Recommendation>,
}

impl Raised {
    pub(crate) fn from_plan(plan: &machine::RaisePlan, stored: &[Recommendation]) -> Self {
        let main = plan.main.map(|i| stored[i].clone());
        let superseded = stored
            .iter()
            .enumerate()
            .filter(|(i, _)| Some(*i) != plan.main)
            .map(|(_, r)| r.clone())
            .collect();
        let verdict = match (plan.verdict, &main) {
            (VerdictPlan::Proceed, Some(r)) => Verdict::Proceed(Ticket {
                id: Some(r.id.clone()),
                kind: r.kind,
            }),
            (VerdictPlan::Wait, Some(r)) => Verdict::Wait(r.id.clone()),
            (VerdictPlan::Advice, Some(r)) => Verdict::Advice(r.id.clone()),
            (VerdictPlan::Suppressed, _) | (_, None) => Verdict::Off,
        };
        Self {
            verdict,
            rec: main,
            event: plan.event,
            superseded,
        }
    }
}

/// A listing's filter.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Filter {
    /// Only these states; empty: the live ones (open, approved, applying,
    /// snoozed) unless `all`.
    #[serde(default)]
    pub states: Vec<State>,
    /// Every state.
    #[serde(default)]
    pub all: bool,
    /// Only these kinds (or families, as text).
    #[serde(default)]
    pub kinds: Vec<String>,
    /// Only this level.
    #[serde(default)]
    pub level: Option<Level>,
    /// Only this database.
    #[serde(default)]
    pub db: Option<DbId>,
    /// Only this object.
    #[serde(default)]
    pub object: Option<ObjectId>,
    /// At most this many, newest first; 0 is 500.
    #[serde(default)]
    pub limit: usize,
}

impl Filter {
    /// Whether `r` passes the filter.
    #[must_use]
    pub fn matches(&self, r: &Recommendation) -> bool {
        let state_ok = if self.states.is_empty() {
            self.all || r.state.is_live()
        } else {
            self.states.contains(&r.state)
        };
        state_ok
            && (self.kinds.is_empty()
                || self
                    .kinds
                    .iter()
                    .any(|k| k == r.kind.as_str() || k == r.kind.family()))
            && self.level.is_none_or(|l| l == r.level)
            && self.db.is_none_or(|d| r.subject.db == Some(d))
            && self.object.is_none_or(|o| r.subject.object == Some(o))
    }

    /// The effective limit.
    #[must_use]
    pub const fn limit(&self) -> usize {
        if self.limit == 0 { 500 } else { self.limit }
    }
}

/// One store of recommendations: a tenant catalog, a DC tier or the global
/// tier.
#[async_trait::async_trait]
pub trait RecStore: Send + Sync {
    /// The gate's raise, atomically (chapter 52, "The gate").
    async fn raise(
        &self,
        p: &Proposal,
        policy: &Policy,
        ancestry: &[TenantId],
        now: Micros,
    ) -> Result<Raised>;
    /// One item by its number.
    async fn get(&self, n: u64) -> Result<Option<Recommendation>>;
    /// Items passing `f`, newest first.
    async fn list(&self, f: &Filter) -> Result<Vec<Recommendation>>;
    /// Writes `r` if the stored version is `expected`; `false` on a
    /// conflict.
    async fn put(&self, r: &Recommendation, expected: u64) -> Result<bool>;
    /// Items whose timers may fire at or before `now`.
    async fn due(&self, now: Micros, limit: usize) -> Result<Vec<Recommendation>>;
    /// Deletes closed items last changed before `before`.
    async fn purge(&self, before: Micros) -> Result<u64>;
}

/// Reads item `n`, applies `f`, writes it back; retries on conflicts.
/// `f` returns `Ok(None)` to write nothing.
///
/// # Errors
/// 39972 if missing; `f`'s errors; 39071 after repeated conflicts.
pub async fn modify<T>(
    store: &dyn RecStore,
    id: &RecId,
    mut f: impl FnMut(&mut Recommendation) -> Result<Option<T>> + Send,
) -> Result<(Recommendation, Option<T>)> {
    for _ in 0..5 {
        let Some(mut r) = store.get(id.number()).await? else {
            return Err(not_found(id));
        };
        let expected = r.version;
        let out = f(&mut r)?;
        if out.is_none() {
            return Ok((r, None));
        }
        if r.version == expected {
            r.version += 1;
        }
        if store.put(&r, expected).await? {
            return Ok((r, out));
        }
    }
    Err(SprigError::new(
        SprigCode::CatalogConflict,
        format!("recommendation {id} kept changing; try again"),
    ))
}

/// 39972.
#[must_use]
pub fn not_found(id: &RecId) -> SprigError {
    SprigError::new(
        SprigCode::RecommendationNotFound,
        format!("no recommendation {id}"),
    )
}

/// Items in memory: tests and the simulation.
pub struct MemStore {
    home: Box<dyn Fn(u64) -> RecId + Send + Sync>,
    rows: Mutex<(u64, BTreeMap<u64, Recommendation>)>,
}

impl std::fmt::Debug for MemStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemStore").finish_non_exhaustive()
    }
}

impl MemStore {
    /// A store whose items get ids from `home`.
    #[must_use]
    pub fn new(home: impl Fn(u64) -> RecId + Send + Sync + 'static) -> Self {
        Self {
            home: Box::new(home),
            rows: Mutex::new((0, BTreeMap::new())),
        }
    }

    /// A tenant's store.
    #[must_use]
    pub fn tenant(t: TenantId) -> Self {
        Self::new(move |n| RecId::Tenant(t, n))
    }

    /// Every item, oldest first.
    #[must_use]
    pub fn all(&self) -> Vec<Recommendation> {
        self.rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .1
            .values()
            .cloned()
            .collect()
    }
}

#[async_trait::async_trait]
impl RecStore for MemStore {
    async fn raise(
        &self,
        p: &Proposal,
        policy: &Policy,
        ancestry: &[TenantId],
        now: Micros,
    ) -> Result<Raised> {
        let mut g = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        let fp = p.fingerprint();
        let same: Vec<Recommendation> =
            g.1.values()
                .filter(|r| r.kind == p.kind && r.target == p.target && r.state.is_live())
                .cloned()
                .collect();
        let ignored =
            g.1.values()
                .filter(|r| r.fingerprint == fp && r.state == State::Ignored)
                .max_by_key(|r| r.updated_at_us)
                .cloned();
        let next = g.0 + 1;
        let plan = machine::raise(
            p,
            policy,
            &same,
            ignored.as_ref(),
            ancestry,
            (self.home)(next),
            now,
        );
        let mut stored = Vec::new();
        for w in &plan.writes {
            if w.id == (self.home)(next) {
                g.0 = next;
            }
            g.1.insert(w.id.number(), w.clone());
            stored.push(w.clone());
        }
        let mut out = Raised::from_plan(&plan, &stored);
        if plan.verdict == VerdictPlan::Suppressed
            && let Some(i) = ignored
        {
            out.verdict = Verdict::Suppressed(i.id);
        }
        Ok(out)
    }

    async fn get(&self, n: u64) -> Result<Option<Recommendation>> {
        Ok(self
            .rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .1
            .get(&n)
            .cloned())
    }

    async fn list(&self, f: &Filter) -> Result<Vec<Recommendation>> {
        let g = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(g.1
            .values()
            .rev()
            .filter(|r| f.matches(r))
            .take(f.limit())
            .cloned()
            .collect())
    }

    async fn put(&self, r: &Recommendation, expected: u64) -> Result<bool> {
        let mut g = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        match g.1.get(&r.id.number()) {
            Some(cur) if cur.version == expected => {
                g.1.insert(r.id.number(), r.clone());
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn due(&self, now: Micros, limit: usize) -> Result<Vec<Recommendation>> {
        let g = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(g.1
            .values()
            .filter(|r| machine::next_due(r, DUE_APPLYING_TIMEOUT).is_some_and(|t| t <= now))
            .take(limit)
            .cloned()
            .collect())
    }

    async fn purge(&self, before: Micros) -> Result<u64> {
        let mut g = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        let n = g.1.len();
        g.1.retain(|_, r| !(r.state.is_closed() && r.updated_at_us < before));
        Ok((n - g.1.len()) as u64)
    }
}

/// Whether `kind` text names a known kind or family.
#[must_use]
pub fn known_kind(k: &str) -> bool {
    Kind::ALL.iter().any(|x| x.as_str() == k || x.family() == k)
}
