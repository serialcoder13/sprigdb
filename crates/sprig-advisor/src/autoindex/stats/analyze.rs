//! The analyze job (`auto_index_analyze`, chapter 13 "Statistics").
//!
//! Statistics are filled only by `ANALYZE ... PERSISTENT`. When a
//! candidate's columns have no statistics, or the table changed by more
//! than [`AnalyzeLimits::stale_ratio`] (20%) since they were collected,
//! the generator asks for them with an [`AnalyzePlan`]: only the needed
//! columns (and indexes), on the validation node, or on the writer in the
//! build window. [`analyze`] runs it:
//!
//! ```sql
//! SET SESSION analyze_sample_percentage = 0;           -- automatic sample size
//! ANALYZE LOCAL TABLE `s`.`t` PERSISTENT FOR COLUMNS (`a`, `b`) INDEXES ();
//! ```
//!
//! `LOCAL` keeps it out of the binlog. Measured on 11.8.9:
//! `analyze_sample_percentage` is settable per session (chapter 13's open
//! question 2, second half).
//!
//! [`AnalyzeThrottle`] keeps the job from hammering a table: at most one
//! analyze per table per [`AnalyzeLimits::per_table`], and at most
//! [`AnalyzeLimits::max_per_window`] per tenant per window. The chapter
//! names no settings for these; the defaults are this crate's (one per
//! table per 6 hours, 20 per tenant per hour).

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use sprig_common::Result;
use sprig_common::names::quote;
use sprig_common::time::Clock;

use super::TableStats;
use crate::sql::map_err;

/// Why statistics are collected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnalyzeReason {
    /// Columns without statistics or histograms.
    Missing,
    /// The table changed by more than the stale ratio.
    Stale,
    /// An existing index covers a candidate but the optimizer avoids it
    /// (status `analyze`).
    Coverage,
    /// A new index on a validation node.
    Validation,
}

/// One `ANALYZE` to run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalyzePlan {
    /// Physical schema.
    pub schema: String,
    /// Table.
    pub table: String,
    /// Columns, sorted, lower case.
    pub columns: Vec<String>,
    /// Indexes; empty for `INDEXES ()`.
    pub indexes: Vec<String>,
    /// Why.
    pub reason: AnalyzeReason,
}

impl AnalyzePlan {
    /// The statement (without the session setting).
    #[must_use]
    pub fn statement(&self) -> String {
        let list = |v: &[String]| v.iter().map(|c| quote(c)).collect::<Vec<_>>().join(", ");
        format!(
            "ANALYZE LOCAL TABLE {}.{} PERSISTENT FOR COLUMNS ({}) INDEXES ({})",
            quote(&self.schema),
            quote(&self.table),
            list(&self.columns),
            list(&self.indexes)
        )
    }
}

/// Throttle and staleness limits.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AnalyzeLimits {
    /// Stale above this relative change in rows (chapter 13: 0.2).
    pub stale_ratio: f64,
    /// At most one analyze per table per this.
    pub per_table: Duration,
    /// At most this many analyzes per tenant per `window`.
    pub max_per_window: usize,
    /// The window of `max_per_window`.
    pub window: Duration,
}

impl Default for AnalyzeLimits {
    fn default() -> Self {
        Self {
            stale_ratio: 0.2,
            per_table: Duration::from_secs(6 * 3600),
            max_per_window: 20,
            window: Duration::from_secs(3600),
        }
    }
}

/// The analyze `t` needs for `columns` (lower case), if any: the columns
/// with no statistics, those of `want_histogram` without a histogram
/// (range and `IN` columns, and equalities with sampled values), or all
/// of them when the table is stale.
#[must_use]
pub fn plan_analyze(
    t: &TableStats,
    columns: &[&str],
    want_histogram: &[&str],
    limits: &AnalyzeLimits,
) -> Option<AnalyzePlan> {
    let mut wanted: BTreeSet<String> = columns
        .iter()
        .chain(want_histogram)
        .map(|c| c.to_ascii_lowercase())
        .collect();
    wanted.retain(|c| t.columns.get(c).is_some_and(|i| !i.lob));
    if wanted.is_empty() || !t.eits {
        return None;
    }
    let refs: Vec<&str> = wanted.iter().map(String::as_str).collect();
    let (cols, reason) = if t.is_stale(limits.stale_ratio) {
        (wanted.into_iter().collect(), AnalyzeReason::Stale)
    } else {
        let hist: Vec<&str> = want_histogram.to_vec();
        let mut missing: BTreeSet<String> = t.missing(&refs, false).into_iter().collect();
        missing.extend(t.missing(&hist, true));
        missing.retain(|c| wanted.contains(c));
        if missing.is_empty() {
            return None;
        }
        (missing.into_iter().collect(), AnalyzeReason::Missing)
    };
    Some(AnalyzePlan {
        schema: t.schema.clone(),
        table: t.table.clone(),
        columns: cols,
        indexes: Vec::new(),
        reason,
    })
}

/// Runs `plan` on `q`'s connection: the session's sample size set to
/// automatic, then `ANALYZE LOCAL TABLE ... PERSISTENT FOR`.
///
/// # Errors
/// 39070 or a backend error (1146 for a table gone).
pub async fn analyze<Q: Queryable + Send>(q: &mut Q, plan: &AnalyzePlan) -> Result<()> {
    q.query_drop("SET SESSION analyze_sample_percentage = 0")
        .await
        .map_err(map_err)?;
    // ANALYZE returns a result set; a failure is a row with Msg_type
    // 'error', not a server error.
    let rows: Vec<(String, String, String, String)> =
        q.query(plan.statement()).await.map_err(map_err)?;
    if let Some((_, _, _, msg)) = rows.iter().find(|r| r.2.eq_ignore_ascii_case("error")) {
        return Err(sprig_common::SprigError::internal(format!(
            "ANALYZE of {}.{} failed: {msg}",
            plan.schema, plan.table
        )));
    }
    Ok(())
}

/// Limits how often [`analyze`] runs, per table and per tenant. Shared by
/// the analyze job's runner; reads time through a [`Clock`].
pub struct AnalyzeThrottle<C: Clock> {
    clock: C,
    limits: AnalyzeLimits,
    state: Mutex<ThrottleState>,
}

#[derive(Default)]
struct ThrottleState {
    last: HashMap<(u64, String, String), Instant>,
    recent: HashMap<u64, VecDeque<Instant>>,
}

impl<C: Clock> std::fmt::Debug for AnalyzeThrottle<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnalyzeThrottle")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl<C: Clock> AnalyzeThrottle<C> {
    /// A throttle with `limits`.
    pub fn new(clock: C, limits: AnalyzeLimits) -> Self {
        Self {
            clock,
            limits,
            state: Mutex::new(ThrottleState::default()),
        }
    }

    /// Whether tenant `tenant` may analyze `plan`'s table now; if so, the
    /// run is counted. A refused plan is retried at the next evaluation.
    pub fn admit(&self, tenant: u64, plan: &AnalyzePlan) -> bool {
        let now = self.clock.now();
        let mut s = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = (tenant, plan.schema.clone(), plan.table.clone());
        if s.last
            .get(&key)
            .is_some_and(|t| now.saturating_duration_since(*t) < self.limits.per_table)
        {
            return false;
        }
        let window = self.limits.window;
        let q = s.recent.entry(tenant).or_default();
        while q
            .front()
            .is_some_and(|t| now.saturating_duration_since(*t) >= window)
        {
            q.pop_front();
        }
        if q.len() >= self.limits.max_per_window {
            return false;
        }
        q.push_back(now);
        s.last.insert(key, now);
        true
    }
}
