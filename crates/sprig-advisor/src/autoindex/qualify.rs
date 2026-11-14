//! Trigger evaluation: which fingerprints of a tenant's window deserve an
//! index, and why.
//!
//! ```text
//! qualify(tenant):
//!   tenant mode off                                     → nothing
//!   W   := the window, minus Studio runs unless include_studio (main object)
//!   top := rank of each fingerprint of W by total time (statements only)
//!   for f in W, for each object o that f references, thresholds resolved at o:
//!     mode(o) = off                                     → miss mode_off
//!     slow  := runs >= min_runs AND p95 > p95_threshold_ms
//!     heavy := rank <= top_n AND examined_samples >= min_examined_samples
//!              AND rows_examined / max(1, returned_sampled) > min_examined_per_returned
//!     scan  := scan_runs >= scan_min_runs AND |scan_sessions| >= scan_min_sessions
//!     none of them                                      → miss below_thresholds
//!     a candidate on o serving f decided within cooldown → miss cooldown
//!     every predicate of f unsargable                   → miss unsargable (for explain)
//!     trigger := slow_p95 | top_total_time | large_scan (first that holds)
//!     advise  := trigger = large_scan AND data size of o > scan_recommend_above_gb
//! ```
//!
//! The fast trigger's per-run conditions (`scan_min_examined` rows
//! examined, at most `scan_max_returned_ratio` of them returned) are
//! applied by the workload recorder when it counts `scan_runs` and
//! `scan_sessions`; qualification only counts runs and sessions.
//!
//! **Choices:**
//!
//! - **Per object.** A fingerprint over several objects is qualified once
//!   per object, with that object's thresholds and mode; `include_studio`
//!   is resolved at the fingerprint's main object, because it decides
//!   which runs make up the fingerprint's statistics.
//! - **The ranking** for `top_total_time` is over the tenant's statement
//!   fingerprints in `W` (transaction footprints and logic-function
//!   invocations are left out: their time is their statements' time), by
//!   total time, ties by id.
//! - **Cooldown** counts candidates decided on the same object: a
//!   decision about one table of a join doesn't hold back the other.
//! - **Rows examined per row returned** counts each probed run as
//!   returning at least one row ([`examined_per_returned`]), so writes and
//!   lookups that return nothing don't look like scans.
//! - **Strictness:** "50 runs" is at least 50,
//!   "p95 over 100 ms" and "more than 100 rows per row returned" are
//!   strict.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use sprig_common::Result;
use sprig_common::ids::{Fingerprint, ObjectId, TenantId};
use sprig_workload::fingerprint::FpKind;
use sprig_workload::recorder::Recorder;
use sprig_workload::window::FpWindowStats;

use super::policy::{Mode, ObjectPolicy};
use super::store::CandidateRow;

/// Bytes in a gigabyte, for `auto_index.scan_recommend_above_gb`.
pub const GB: f64 = 1e9;

/// Where a window's runs came from (Studio's statements are
/// recorded with `caller = studio` in their own statistics bucket).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunSource {
    /// Applications: every front end except Studio.
    Client,
    /// Studio; counted only with `auto_index.include_studio`.
    Studio,
}

/// One fingerprint's statistics from one source over the window.
#[derive(Debug, Clone)]
pub struct SourcedWindow {
    /// Where the runs came from.
    pub source: RunSource,
    /// The merged statistics and samples.
    pub window: FpWindowStats,
}

impl SourcedWindow {
    /// Client runs.
    #[must_use]
    pub fn client(window: FpWindowStats) -> Self {
        Self {
            source: RunSource::Client,
            window,
        }
    }

    /// Studio runs.
    #[must_use]
    pub fn studio(window: FpWindowStats) -> Self {
        Self {
            source: RunSource::Studio,
            window,
        }
    }
}

/// Where trigger evaluation reads the workload: the workload store's
/// `WorkloadStore::window`, plus the request to watch a fingerprint.
///
/// The catalog-backed store is being built; until it
/// exists, [`RecorderSource`] reads a gateway's open buckets.
#[async_trait::async_trait]
pub trait WorkloadSource: Send + Sync {
    /// Every fingerprint of `tenant` with runs in `[from, to)`, one entry
    /// per fingerprint and source.
    ///
    /// # Errors
    /// The store's.
    async fn window(
        &self,
        tenant: TenantId,
        from: SystemTime,
        to: SystemTime,
    ) -> Result<Vec<SourcedWindow>>;

    /// Asks the workload recorder to watch `fp` (probe rows examined,
    /// capture parameter samples): every qualifying fingerprint is watched.
    fn watch(&self, tenant: TenantId, fp: Fingerprint);
}

/// The in-memory [`WorkloadSource`]: one recorder's open buckets
/// ([`Recorder::window`]), all as client runs, whatever the range.
#[derive(Clone)]
pub struct RecorderSource(pub Arc<Recorder>);

impl fmt::Debug for RecorderSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecorderSource").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl WorkloadSource for RecorderSource {
    async fn window(
        &self,
        tenant: TenantId,
        _from: SystemTime,
        _to: SystemTime,
    ) -> Result<Vec<SourcedWindow>> {
        Ok(self
            .0
            .window(tenant)
            .into_iter()
            .map(SourcedWindow::client)
            .collect())
    }

    fn watch(&self, tenant: TenantId, fp: Fingerprint) {
        self.0.watch(tenant, fp, true);
    }
}

/// A fixed list of windows, for tests and replays of recorded workloads.
#[derive(Debug, Clone, Default)]
pub struct StaticSource {
    /// The windows, returned for every tenant they belong to.
    pub windows: Vec<SourcedWindow>,
    /// Fingerprints asked to be watched.
    pub watched: Arc<std::sync::Mutex<BTreeSet<(TenantId, Fingerprint)>>>,
}

#[async_trait::async_trait]
impl WorkloadSource for StaticSource {
    async fn window(
        &self,
        tenant: TenantId,
        _from: SystemTime,
        _to: SystemTime,
    ) -> Result<Vec<SourcedWindow>> {
        Ok(self
            .windows
            .iter()
            .filter(|w| w.window.tenant == tenant)
            .cloned()
            .collect())
    }

    fn watch(&self, tenant: TenantId, fp: Fingerprint) {
        self.watched
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert((tenant, fp));
    }
}

/// Why a fingerprint qualified (`auto_index_candidates` evidence and the
/// decision's `reason_code`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Trigger {
    /// `min_runs` runs and a p95 above `p95_threshold_ms`.
    SlowP95,
    /// Among the tenant's `top_n` by total time, examining more than
    /// `min_examined_per_returned` rows per row returned.
    TopTotalTime,
    /// The fast trigger: large scans from several sessions.
    LargeScan,
}

impl Trigger {
    /// The reason text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Trigger::SlowP95 => "slow_p95",
            Trigger::TopTotalTime => "top_total_time",
            Trigger::LargeScan => "large_scan",
        }
    }
}

impl fmt::Display for Trigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The numbers the triggers read, from one fingerprint's window.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Signals {
    /// Runs in the window.
    pub runs: u64,
    /// p95, ms (the histogram bucket's upper bound).
    pub p95_ms: f64,
    /// Total time, ms.
    pub total_ms: f64,
    /// Probed runs.
    pub examined_samples: u64,
    /// Rows examined per row returned over the probed runs.
    pub examined_per_returned: Option<f64>,
    /// Probed runs that were large scans.
    pub scan_runs: u64,
    /// Distinct sessions among them (at most 8).
    pub scan_sessions: usize,
}

impl Signals {
    /// The signals of `w`.
    #[must_use]
    pub fn of(w: &FpWindowStats) -> Self {
        Self {
            runs: w.runs(),
            p95_ms: w.p95_ms(),
            total_ms: w.total_ms(),
            examined_samples: w.stats.examined_samples,
            examined_per_returned: examined_per_returned(w),
            scan_runs: w.stats.scan_runs,
            scan_sessions: w.stats.scan_sessions.len(),
        }
    }
}

/// Rows examined per row returned over the probed runs, each probed run
/// counted as returning at least one row: `rows_examined /
/// max(examined_samples, returned_sampled)`. The workload store's
/// `rows_examined / max(1, returned_sampled)` floors only the sum, so
/// statements that return nothing (writes by primary key, lookups that
/// miss) would look like scans. `None` without probes.
#[must_use]
#[allow(clippy::cast_precision_loss)] // counters far below 2^52
pub fn examined_per_returned(w: &FpWindowStats) -> Option<f64> {
    let s = &w.stats;
    (s.examined_samples > 0)
        .then(|| s.rows_examined as f64 / s.examined_samples.max(s.returned_sampled) as f64)
}

/// Why the slow trigger didn't fire.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "why")]
pub enum SlowMiss {
    /// Fewer than `min_runs` runs.
    FewRuns {
        /// Runs.
        runs: u64,
        /// `min_runs`.
        need: u32,
    },
    /// p95 at or below the threshold.
    FastP95 {
        /// p95, ms.
        p95_ms: f64,
        /// `p95_threshold_ms`.
        threshold_ms: f64,
    },
}

/// Why the top-N trigger didn't fire.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "why")]
pub enum HeavyMiss {
    /// Not among the top `top_n` by total time (`rank` counts from 1).
    NotTop {
        /// Rank by total time.
        rank: usize,
        /// `top_n`.
        top_n: u32,
    },
    /// Too few probed runs to trust the ratio.
    FewProbes {
        /// Probed runs.
        samples: u64,
        /// `min_examined_samples`.
        need: u32,
    },
    /// Rows examined per row returned at or below the limit.
    LowRatio {
        /// The ratio.
        ratio: f64,
        /// `min_examined_per_returned`.
        need: f64,
    },
}

/// Why the fast trigger didn't fire.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "why")]
pub enum ScanMiss {
    /// Fewer than `scan_min_runs` large-scan runs.
    FewScanRuns {
        /// Scan runs.
        runs: u64,
        /// `scan_min_runs`.
        need: u32,
    },
    /// The scans came from fewer than `scan_min_sessions` sessions
    /// (someone exploring data).
    FewSessions {
        /// Sessions.
        sessions: usize,
        /// `scan_min_sessions`.
        need: u32,
    },
}

/// Why none of the three triggers fired.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Shortfall {
    /// The slow trigger.
    pub slow: SlowMiss,
    /// The top-N trigger.
    pub heavy: HeavyMiss,
    /// The fast trigger.
    pub scan: ScanMiss,
}

/// Why a fingerprint (on an object) didn't qualify.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Miss {
    /// The tenant's `auto_index.mode` is `off`: nothing is evaluated.
    TenantOff,
    /// The object's `auto_index.mode` is `off`.
    ModeOff,
    /// Not a statement kind or type automatic indexing considers.
    NotIndexable,
    /// It references no catalog object.
    NoObjects,
    /// The catalog doesn't describe the object.
    UnknownObject,
    /// Every run came from an excluded source (Studio).
    ExcludedSource,
    /// No trigger fired.
    Below(Shortfall),
    /// A candidate serving it on this object was decided within
    /// `cooldown_hours`.
    Cooldown {
        /// That candidate.
        candidate_id: u64,
    },
    /// Every predicate is unsargable: no index can serve it (the reasons
    /// are for `sprigctl explain query`).
    Unsargable {
        /// `function:LOWER`, ...
        reasons: Vec<String>,
    },
}

impl Miss {
    /// A short code for logs and explanations.
    #[must_use]
    pub fn code(&self) -> String {
        match self {
            Miss::TenantOff => "tenant_mode_off".into(),
            Miss::ModeOff => "mode_off".into(),
            Miss::NotIndexable => "not_indexable".into(),
            Miss::NoObjects => "no_objects".into(),
            Miss::UnknownObject => "unknown_object".into(),
            Miss::ExcludedSource => "excluded_source".into(),
            Miss::Below(_) => "below_thresholds".into(),
            Miss::Cooldown { .. } => "cooldown".into(),
            Miss::Unsargable { reasons } => reasons
                .first()
                .cloned()
                .unwrap_or_else(|| "unsargable".into()),
        }
    }
}

/// A fingerprint that qualified on one object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Qualified {
    /// The fingerprint.
    pub fp: Fingerprint,
    /// The object it qualified on.
    pub object: ObjectId,
    /// The reason: the first trigger that held, in the order
    /// slow, top-N, fast.
    pub trigger: Trigger,
    /// Every trigger that held.
    pub fired: Vec<Trigger>,
    /// Only the fast trigger qualified it and the object is larger than
    /// `scan_recommend_above_gb`: stop at a recommendation.
    pub advise: bool,
    /// What the triggers read.
    pub signals: Signals,
    /// Rank by total time in the tenant's window, from 1.
    pub rank: usize,
}

impl Qualified {
    /// Only the fast trigger qualified it: its candidates never displace
    /// an active automatic index.
    #[must_use]
    pub fn fast_only(&self) -> bool {
        self.trigger == Trigger::LargeScan
    }
}

/// A fingerprint that didn't qualify, on one object or as a whole.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotQualified {
    /// The fingerprint.
    pub fp: Fingerprint,
    /// The object, when the reason is about one.
    pub object: Option<ObjectId>,
    /// Why.
    pub why: Miss,
}

/// Qualification of a tenant's window.
#[derive(Debug, Clone, Default)]
pub struct Qualification {
    /// Per (fingerprint, object), in fingerprint then object order.
    pub qualified: Vec<Qualified>,
    /// The others, with reasons.
    pub not_qualified: Vec<NotQualified>,
    /// The tenant's window after source filtering, one per fingerprint,
    /// for the cost model's table contexts.
    pub windows: BTreeMap<Fingerprint, FpWindowStats>,
}

impl Qualification {
    /// The fingerprints qualified on `object`.
    pub fn on(&self, object: ObjectId) -> impl Iterator<Item = &Qualified> + '_ {
        self.qualified.iter().filter(move |q| q.object == object)
    }

    /// The objects with a qualified fingerprint, sorted.
    #[must_use]
    pub fn objects(&self) -> Vec<ObjectId> {
        let s: BTreeSet<ObjectId> = self.qualified.iter().map(|q| q.object).collect();
        s.into_iter().collect()
    }
}

/// What qualification needs besides the windows.
pub struct QualifyInput<'a> {
    /// The tenant's mode (resolved at the tenant).
    pub tenant_mode: Mode,
    /// Each object's resolved settings; `None` for an object the catalog
    /// doesn't know.
    pub policy: &'a dyn Fn(ObjectId) -> Option<&'a ObjectPolicy>,
    /// Each object's data size, bytes, when known (else the fast trigger
    /// can't decide `advise` here; evaluation decides it from the
    /// statistics).
    pub data_bytes: &'a dyn Fn(ObjectId) -> Option<u64>,
    /// Candidates decided recently (cooldown).
    pub decided: &'a [CandidateRow],
    /// Now.
    pub now: SystemTime,
}

/// Fingerprint kinds automatic indexing reads: SQL statements and
/// MongoDB shapes (translated to field columns).
#[must_use]
pub const fn indexable_kind(k: FpKind) -> bool {
    matches!(k, FpKind::Sql | FpKind::Mongo)
}

/// Kinds left out of the top-N ranking: their time is their statements'.
const fn ranked_kind(k: FpKind) -> bool {
    !matches!(k, FpKind::Txn | FpKind::Logic)
}

/// Merges `b` into `a`: counters summed, maxima kept, histograms and
/// session sets merged, samples appended.
fn merge_into(a: &mut FpWindowStats, b: &FpWindowStats) {
    a.stats.merge(&b.stats);
    a.samples.extend(b.samples.iter().cloned());
}

/// The window `W`: one entry per fingerprint, Studio runs left out unless
/// `include_studio(fingerprint)`. Fingerprints with only excluded runs are
/// returned separately.
#[must_use]
pub fn window_of(
    entries: Vec<SourcedWindow>,
    include_studio: &dyn Fn(&FpWindowStats) -> bool,
) -> (BTreeMap<Fingerprint, FpWindowStats>, Vec<Fingerprint>) {
    let mut client: BTreeMap<Fingerprint, FpWindowStats> = BTreeMap::new();
    let mut studio: BTreeMap<Fingerprint, FpWindowStats> = BTreeMap::new();
    for e in entries {
        let m = match e.source {
            RunSource::Client => &mut client,
            RunSource::Studio => &mut studio,
        };
        match m.get_mut(&e.window.id) {
            Some(w) => merge_into(w, &e.window),
            None => {
                m.insert(e.window.id, e.window);
            }
        }
    }
    let mut excluded = Vec::new();
    for (id, s) in studio {
        if !include_studio(&s) {
            if !client.contains_key(&id) {
                excluded.push(id);
            }
            continue;
        }
        match client.get_mut(&id) {
            Some(w) => merge_into(w, &s),
            None => {
                client.insert(id, s);
            }
        }
    }
    (client, excluded)
}

/// Ranks of the window's statement fingerprints by total time, from 1;
/// ties by id.
#[must_use]
pub fn ranks(w: &BTreeMap<Fingerprint, FpWindowStats>) -> BTreeMap<Fingerprint, usize> {
    let mut v: Vec<(&Fingerprint, f64)> = w
        .iter()
        .filter(|(_, s)| ranked_kind(s.kind))
        .map(|(id, s)| (id, s.total_ms()))
        .collect();
    v.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(b.0)));
    v.into_iter()
        .enumerate()
        .map(|(i, (id, _))| (*id, i + 1))
        .collect()
}

/// The triggers that fire for `s` at rank `rank` under `p`, or why none
/// does.
///
/// # Errors
/// The [`Shortfall`] when no trigger fires.
pub fn triggers(
    s: &Signals,
    rank: usize,
    p: &ObjectPolicy,
) -> std::result::Result<Vec<Trigger>, Shortfall> {
    let t = &p.trigger;
    let slow = if s.runs < u64::from(t.min_runs) {
        Err(SlowMiss::FewRuns {
            runs: s.runs,
            need: t.min_runs,
        })
    } else if s.p95_ms <= t.p95_threshold_ms {
        Err(SlowMiss::FastP95 {
            p95_ms: s.p95_ms,
            threshold_ms: t.p95_threshold_ms,
        })
    } else {
        Ok(())
    };
    let ratio = s.examined_per_returned.unwrap_or(0.0);
    let heavy = if rank == 0 || rank > t.top_n as usize {
        Err(HeavyMiss::NotTop {
            rank,
            top_n: t.top_n,
        })
    } else if s.examined_samples < u64::from(t.min_examined_samples) {
        Err(HeavyMiss::FewProbes {
            samples: s.examined_samples,
            need: t.min_examined_samples,
        })
    } else if ratio <= t.min_examined_per_returned {
        Err(HeavyMiss::LowRatio {
            ratio,
            need: t.min_examined_per_returned,
        })
    } else {
        Ok(())
    };
    let scan = if s.scan_runs < u64::from(t.scan_min_runs) {
        Err(ScanMiss::FewScanRuns {
            runs: s.scan_runs,
            need: t.scan_min_runs,
        })
    } else if s.scan_sessions < t.scan_min_sessions as usize {
        Err(ScanMiss::FewSessions {
            sessions: s.scan_sessions,
            need: t.scan_min_sessions,
        })
    } else {
        Ok(())
    };
    match (slow, heavy, scan) {
        (Err(slow), Err(heavy), Err(scan)) => Err(Shortfall { slow, heavy, scan }),
        (slow, heavy, scan) => Ok([
            (slow.is_ok(), Trigger::SlowP95),
            (heavy.is_ok(), Trigger::TopTotalTime),
            (scan.is_ok(), Trigger::LargeScan),
        ]
        .into_iter()
        .filter_map(|(ok, t)| ok.then_some(t))
        .collect()),
    }
}

/// Whether a fast-trigger qualification on an object of `data_bytes`
/// stops at a recommendation.
#[must_use]
#[allow(clippy::cast_precision_loss)] // sizes far below 2^52
pub fn advise_for(trigger: Trigger, data_bytes: Option<u64>, p: &ObjectPolicy) -> bool {
    trigger == Trigger::LargeScan
        && data_bytes.is_some_and(|b| b as f64 > p.trigger.scan_recommend_above_gb * GB)
}

/// `qualify` over a window `W` (see [`window_of`]): one
/// verdict per fingerprint and object.
#[must_use]
#[allow(clippy::too_many_lines)] // the whole loop, in one place
pub fn qualify(
    w: BTreeMap<Fingerprint, FpWindowStats>,
    excluded: &[Fingerprint],
    input: &QualifyInput<'_>,
) -> Qualification {
    let mut out = Qualification::default();
    if input.tenant_mode == Mode::Off {
        out.not_qualified = w
            .keys()
            .chain(excluded)
            .map(|fp| NotQualified {
                fp: *fp,
                object: None,
                why: Miss::TenantOff,
            })
            .collect();
        return out;
    }
    for fp in excluded {
        out.not_qualified.push(NotQualified {
            fp: *fp,
            object: None,
            why: Miss::ExcludedSource,
        });
    }
    let rank = ranks(&w);
    for (id, f) in &w {
        let miss = |why: Miss, object: Option<ObjectId>| NotQualified {
            fp: *id,
            object,
            why,
        };
        if !indexable_kind(f.kind) || !f.shape.stmt_type.is_indexable() {
            out.not_qualified.push(miss(Miss::NotIndexable, None));
            continue;
        }
        let objects = f.shape.object_ids();
        if objects.is_empty() {
            out.not_qualified.push(miss(Miss::NoObjects, None));
            continue;
        }
        let signals = Signals::of(f);
        let r = rank.get(id).copied().unwrap_or(0);
        for object in objects {
            let Some(p) = (input.policy)(object) else {
                out.not_qualified
                    .push(miss(Miss::UnknownObject, Some(object)));
                continue;
            };
            if p.mode == Mode::Off {
                out.not_qualified.push(miss(Miss::ModeOff, Some(object)));
                continue;
            }
            let fired = match triggers(&signals, r, p) {
                Ok(fired) => fired,
                Err(short) => {
                    out.not_qualified
                        .push(miss(Miss::Below(short), Some(object)));
                    continue;
                }
            };
            let cooldown = Duration::from_secs(u64::from(p.trigger.cooldown_hours) * 3600);
            if let Some(c) = input.decided.iter().find(|c| {
                c.object == object
                    && c.fingerprints.iter().any(|(g, _)| g == id)
                    && input
                        .now
                        .duration_since(c.created_at)
                        .map_or(true, |a| a < cooldown)
            }) {
                out.not_qualified.push(miss(
                    Miss::Cooldown {
                        candidate_id: c.candidate_id,
                    },
                    Some(object),
                ));
                continue;
            }
            if f.shape.all_unsargable() {
                let reasons = f
                    .shape
                    .predicates
                    .iter()
                    .map(|p| match &p.op {
                        sprig_workload::shape::PredOp::Unsargable { why } => why.as_string(),
                        other => other.name().to_owned(),
                    })
                    .collect();
                out.not_qualified
                    .push(miss(Miss::Unsargable { reasons }, Some(object)));
                continue;
            }
            let trigger = fired[0];
            out.qualified.push(Qualified {
                fp: *id,
                object,
                trigger,
                advise: advise_for(trigger, (input.data_bytes)(object), p),
                fired,
                signals,
                rank: r,
            });
        }
    }
    out.windows = w;
    out
}
