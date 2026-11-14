//! The verdict of a replay: noise
//! handling, per-statement medians, per-fingerprint p95s, and accept or
//! reject against the thresholds. Pure functions, so every rule has a
//! table-driven test.
//!
//! The rules, with this module's own choices marked *(choice)*:
//!
//! - **A statement's time** is the median of its runs, A (before, with
//!   `IGNORE INDEX`) and B (after) separately.
//! - **Noise.** A statement whose A runs spread by more than
//!   [`Thresholds::noise_ratio`] (1.5 times) gets two more rounds. *(choice)*
//!   A spread also has to exceed [`Thresholds::noise_floor_ms`] (1 ms), the
//!   regression noise floor, or sub-millisecond statements would
//!   always look noisy. After the extra rounds the slowest A run is dropped
//!   as an outlier *(choice)* and the spread is checked again; a statement
//!   still noisy is left out of the regression check but counted. A
//!   statement whose A and B runs all hit the replay timeout is noisy too:
//!   it can't be measured.
//! - **Inconclusive** when more than [`Thresholds::max_noisy_share`]
//!   (20%) of the replayed statements are noisy or failed to run.
//!   Statements with no read (`INSERT ... VALUES`) don't count.
//! - **Regression.** Any non-noisy statement with
//!   `B > (1 + max_regression_pct / 100) × A + noise_floor_ms` rejects.
//! - **Improvement.** For each primary (target) fingerprint, the p95 over
//!   its samples (nearest rank of the per-sample medians, noisy samples
//!   included) must improve by at least `min_improvement_pct`, and B must
//!   use the index in at least one sample *(choice: any sample; the p95
//!   gate already demands a broad gain)*.
//! - **Covering hint.** A rejection by a statement whose B plan reads the
//!   index with many primary-key lookups names that statement, so the
//!   validator can retry with a covering index.

use serde::{Deserialize, Serialize};
use sprig_common::ids::Fingerprint;

/// The thresholds of a verdict.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Thresholds {
    /// `auto_index.min_improvement_pct` (30).
    pub min_improvement_pct: f64,
    /// `auto_index.max_regression_pct` (10).
    pub max_regression_pct: f64,
    /// Absolute slack added to the regression bound, ms (1).
    pub noise_floor_ms: f64,
    /// Largest max/min ratio of A runs that isn't noise (1.5).
    pub noise_ratio: f64,
    /// Largest share of noisy or failed statements (0.20).
    pub max_noisy_share: f64,
    /// Fewest primary-key lookups through the index that make a regression
    /// a covering-retry case.
    pub covering_min_lookups: f64,
}

impl Default for Thresholds {
    fn default() -> Self {
        Self {
            min_improvement_pct: 30.0,
            max_regression_pct: 10.0,
            noise_floor_ms: 1.0,
            noise_ratio: 1.5,
            max_noisy_share: 0.20,
            covering_min_lookups: 1_000.0,
        }
    }
}

/// How a replayed statement relates to the candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatementRole {
    /// A fingerprint the candidate was generated for: it must improve.
    Target,
    /// Another fingerprint on the table: it must not regress.
    Regression,
}

/// One run of one side.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Run {
    /// Time, ms (the timeout when it timed out).
    pub ms: f64,
    /// It hit `max_statement_time`.
    pub timed_out: bool,
    /// The plan read the candidate index.
    pub uses_index: bool,
    /// Rows read over all tables.
    pub rows_read: f64,
    /// Rows read through the candidate with primary-key lookups.
    pub lookups_via: f64,
}

/// What happened to one sampled statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status", content = "reason")]
pub enum Outcome {
    /// It ran on both sides.
    Measured,
    /// It has no read to replay; not counted.
    Unreplayable(String),
    /// It failed to run (an error other than the timeout); counted as
    /// noise.
    Failed(String),
}

/// Raw results for one sampled statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatementRuns {
    /// Its fingerprint.
    pub fingerprint: Fingerprint,
    /// The sample (`workload_samples.sample_id`).
    pub sample_id: u64,
    /// Target or regression check.
    pub role: StatementRole,
    /// How it went.
    pub outcome: Outcome,
    /// Before runs.
    pub a: Vec<Run>,
    /// After runs.
    pub b: Vec<Run>,
    /// It got the extra rounds.
    pub extra_rounds: bool,
}

/// One statement's evidence, as `auto_index_validations.details` keeps
/// it: sample ids and timings, never values.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatementEvidence {
    /// Fingerprint.
    pub fingerprint: Fingerprint,
    /// Sample id.
    pub sample_id: u64,
    /// Role.
    pub role: StatementRole,
    /// Outcome.
    #[serde(flatten)]
    pub outcome: Outcome,
    /// Median before, ms.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub a_ms: Option<f64>,
    /// Median after, ms.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub b_ms: Option<f64>,
    /// Change, percent (positive is slower).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub change_pct: Option<f64>,
    /// Runs per side.
    pub runs: usize,
    /// Left out of the regression check.
    pub noisy: bool,
    /// Over the regression bound.
    pub regressed: bool,
    /// B's plan used the index.
    pub uses_index: bool,
    /// Rows read before and after (median runs).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<(f64, f64)>,
    /// Primary-key lookups through the index after.
    #[serde(skip_serializing_if = "is_zero")]
    pub lookups_via: f64,
    /// A side timed out at least once.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub timed_out: bool,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_zero(v: &f64) -> bool {
    *v == 0.0
}

/// One fingerprint's summary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FingerprintSummary {
    /// Fingerprint.
    pub fingerprint: Fingerprint,
    /// Role.
    pub role: StatementRole,
    /// Measured samples.
    pub samples: usize,
    /// p95 of the per-sample medians before, ms.
    pub p95_before: f64,
    /// And after.
    pub p95_after: f64,
    /// `100 × (1 − after / before)`.
    pub improvement_pct: f64,
    /// Some sample's B plan used the index.
    pub uses_index: bool,
}

/// The verdict values of `auto_index_validations.verdict`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerdictKind {
    /// Helps and hurts nothing.
    Accepted,
    /// Doesn't help, or hurts.
    Rejected,
    /// Too noisy or too little evidence: retry in another window.
    Inconclusive,
    /// Stopped before a verdict (node lost, lag, user DDL).
    Aborted,
}

impl VerdictKind {
    /// The column text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            VerdictKind::Accepted => "accepted",
            VerdictKind::Rejected => "rejected",
            VerdictKind::Inconclusive => "inconclusive",
            VerdictKind::Aborted => "aborted",
        }
    }
}

/// A statement that could make a covering index worth a retry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CoveringHint {
    /// The regressing fingerprint.
    pub fingerprint: Fingerprint,
    /// Its sample.
    pub sample_id: u64,
    /// Primary-key lookups through the index in its B plan.
    pub lookups: f64,
}

/// The verdict with its evidence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Verdict {
    /// Accepted, rejected, inconclusive (aborted comes from validators).
    pub kind: VerdictKind,
    /// Why, at most 128 characters (`auto_index_validations.reason`).
    pub reason: String,
    /// Statements replayed (measured or failed).
    pub statements: usize,
    /// Statements left out as noisy or failed.
    pub noisy: usize,
    /// p95 over every target sample before, ms.
    pub target_p95_before: Option<f64>,
    /// And after.
    pub target_p95_after: Option<f64>,
    /// The largest slowdown of a non-noisy statement beyond the noise
    /// floor, percent; 0 when none.
    pub worst_regression_pct: f64,
    /// Its fingerprint.
    pub worst_fp: Option<Fingerprint>,
    /// Per fingerprint.
    pub fingerprints: Vec<FingerprintSummary>,
    /// Per statement.
    pub details: Vec<StatementEvidence>,
    /// Set on a regression that a covering index might fix.
    pub covering_hint: Option<CoveringHint>,
}

/// Median; 0 for an empty slice.
#[must_use]
pub fn median(v: &[f64]) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    let mut s = v.to_vec();
    s.sort_by(f64::total_cmp);
    let n = s.len();
    if n % 2 == 1 {
        s[n / 2]
    } else {
        f64::midpoint(s[n / 2 - 1], s[n / 2])
    }
}

/// Nearest-rank p95; 0 for an empty slice.
#[must_use]
pub fn p95(v: &[f64]) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    let mut s = v.to_vec();
    s.sort_by(f64::total_cmp);
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let rank = ((0.95 * s.len() as f64).ceil() as usize).clamp(1, s.len());
    s[rank - 1]
}

/// Whether runs spread more than `ratio` times and by more than `floor`
/// ms.
#[must_use]
pub fn is_noisy(ms: &[f64], t: &Thresholds) -> bool {
    if ms.len() < 2 {
        return false;
    }
    let min = ms.iter().copied().fold(f64::INFINITY, f64::min);
    let max = ms.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    max - min > t.noise_floor_ms && max > t.noise_ratio * min.max(f64::MIN_POSITIVE)
}

/// The runs without the single slowest one (from four runs up).
#[must_use]
pub fn trim_slowest(ms: &[f64]) -> Vec<f64> {
    let mut s = ms.to_vec();
    if s.len() >= 4 {
        s.sort_by(f64::total_cmp);
        s.pop();
    }
    s
}

fn times(runs: &[Run]) -> Vec<f64> {
    runs.iter().map(|r| r.ms).collect()
}

/// The run nearest the median, for its plan facts.
fn median_run(runs: &[Run]) -> Option<&Run> {
    let m = median(&times(runs));
    runs.iter()
        .min_by(|a, b| (a.ms - m).abs().total_cmp(&(b.ms - m).abs()))
}

/// Summarizes one statement.
#[must_use]
pub fn evidence(s: &StatementRuns, t: &Thresholds) -> StatementEvidence {
    let mut e = StatementEvidence {
        fingerprint: s.fingerprint,
        sample_id: s.sample_id,
        role: s.role,
        outcome: s.outcome.clone(),
        a_ms: None,
        b_ms: None,
        change_pct: None,
        runs: s.a.len().min(s.b.len()),
        noisy: false,
        regressed: false,
        uses_index: s.b.iter().any(|r| r.uses_index),
        rows: None,
        lookups_via: 0.0,
        timed_out: s.a.iter().chain(&s.b).any(|r| r.timed_out),
    };
    if s.outcome != Outcome::Measured || s.a.is_empty() || s.b.is_empty() {
        return e;
    }
    let before = median(&times(&s.a));
    let after = median(&times(&s.b));
    e.a_ms = Some(before);
    e.b_ms = Some(after);
    if before > 0.0 {
        e.change_pct = Some(100.0 * (after - before) / before);
    }
    let all_timed_out = s.a.iter().chain(&s.b).all(|r| r.timed_out);
    let a_times = times(&s.a);
    // After extra rounds the slowest A run is trimmed before the re-check.
    e.noisy = all_timed_out
        || (is_noisy(&a_times, t) && (!s.extra_rounds || is_noisy(&trim_slowest(&a_times), t)));
    e.regressed =
        !e.noisy && after > (1.0 + t.max_regression_pct / 100.0) * before + t.noise_floor_ms;
    let (ra, rb) = (median_run(&s.a), median_run(&s.b));
    if let (Some(ra), Some(rb)) = (ra, rb) {
        e.rows = Some((ra.rows_read, rb.rows_read));
        e.lookups_via = rb.lookups_via;
    }
    e
}

fn truncate(mut s: String, max: usize) -> String {
    if s.len() > max {
        let mut cut = max;
        while !s.is_char_boundary(cut) {
            cut -= 1;
        }
        s.truncate(cut);
    }
    s
}

/// The verdict over every statement replayed for one candidate.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn decide(statements: &[StatementRuns], t: &Thresholds) -> Verdict {
    let details: Vec<StatementEvidence> = statements.iter().map(|s| evidence(s, t)).collect();
    let attempted = details
        .iter()
        .filter(|d| !matches!(d.outcome, Outcome::Unreplayable(_)))
        .count();
    let failed = details
        .iter()
        .filter(|d| matches!(d.outcome, Outcome::Failed(_)))
        .count();
    let noisy = details.iter().filter(|d| d.noisy).count() + failed;

    // Per fingerprint, in first-seen order.
    let mut fps: Vec<(Fingerprint, StatementRole)> = Vec::new();
    for s in statements {
        if !fps.iter().any(|(f, _)| *f == s.fingerprint) {
            fps.push((s.fingerprint, s.role));
        }
    }
    let summaries: Vec<FingerprintSummary> = fps
        .iter()
        .map(|&(f, role)| {
            let measured: Vec<&StatementEvidence> = details
                .iter()
                .filter(|d| d.fingerprint == f && d.a_ms.is_some())
                .collect();
            let before: Vec<f64> = measured.iter().filter_map(|d| d.a_ms).collect();
            let after: Vec<f64> = measured.iter().filter_map(|d| d.b_ms).collect();
            let (pb, pa) = (p95(&before), p95(&after));
            FingerprintSummary {
                fingerprint: f,
                role,
                samples: measured.len(),
                p95_before: pb,
                p95_after: pa,
                improvement_pct: if pb > 0.0 {
                    100.0 * (1.0 - pa / pb)
                } else {
                    0.0
                },
                uses_index: measured.iter().any(|d| d.uses_index),
            }
        })
        .collect();

    let target_before: Vec<f64> = details
        .iter()
        .filter(|d| d.role == StatementRole::Target)
        .filter_map(|d| d.a_ms)
        .collect();
    let target_after: Vec<f64> = details
        .iter()
        .filter(|d| d.role == StatementRole::Target)
        .filter_map(|d| d.b_ms)
        .collect();

    // Worst slowdown beyond the floor.
    let mut worst: Option<&StatementEvidence> = None;
    let mut worst_pct = 0.0;
    for d in details.iter().filter(|d| !d.noisy) {
        if let (Some(a), Some(b)) = (d.a_ms, d.b_ms)
            && a > 0.0
            && b - a > t.noise_floor_ms
        {
            let pct = 100.0 * (b - a) / a;
            if pct > worst_pct {
                worst_pct = pct;
                worst = Some(d);
            }
        }
    }

    let mut v = Verdict {
        kind: VerdictKind::Accepted,
        reason: String::new(),
        statements: attempted,
        noisy,
        target_p95_before: (!target_before.is_empty()).then(|| p95(&target_before)),
        target_p95_after: (!target_after.is_empty()).then(|| p95(&target_after)),
        worst_regression_pct: worst_pct,
        worst_fp: worst.map(|d| d.fingerprint),
        fingerprints: summaries,
        details: Vec::new(),
        covering_hint: None,
    };

    let measured = details.iter().filter(|d| d.a_ms.is_some()).count();
    #[allow(clippy::cast_precision_loss)]
    let share = if attempted == 0 {
        0.0
    } else {
        noisy as f64 / attempted as f64
    };
    if measured == 0 {
        v.kind = VerdictKind::Inconclusive;
        v.reason = "nothing_replayed".into();
    } else if share > t.max_noisy_share {
        v.kind = VerdictKind::Inconclusive;
        v.reason = format!("too_noisy: {noisy} of {attempted} statements noisy or failed");
    } else if let Some(f) = v
        .fingerprints
        .iter()
        .find(|f| f.role == StatementRole::Target && f.samples == 0)
    {
        v.kind = VerdictKind::Inconclusive;
        v.reason = format!("no_target_samples: {}", f.fingerprint);
    } else if let Some(r) = details.iter().filter(|d| d.regressed).max_by(|a, b| {
        a.change_pct
            .unwrap_or(0.0)
            .total_cmp(&b.change_pct.unwrap_or(0.0))
    }) {
        v.kind = VerdictKind::Rejected;
        v.reason = format!(
            "regression: {} sample {} {:+.1}% ({:.2} -> {:.2} ms)",
            r.fingerprint,
            r.sample_id,
            r.change_pct.unwrap_or(0.0),
            r.a_ms.unwrap_or(0.0),
            r.b_ms.unwrap_or(0.0)
        );
        if r.uses_index && r.lookups_via >= t.covering_min_lookups {
            v.covering_hint = Some(CoveringHint {
                fingerprint: r.fingerprint,
                sample_id: r.sample_id,
                lookups: r.lookups_via,
            });
        }
    } else if let Some(f) = v
        .fingerprints
        .iter()
        .find(|f| f.role == StatementRole::Target && !f.uses_index)
    {
        v.kind = VerdictKind::Rejected;
        v.reason = format!("index_unused: {} never read the index", f.fingerprint);
    } else if let Some(f) = v
        .fingerprints
        .iter()
        .find(|f| f.role == StatementRole::Target && f.improvement_pct < t.min_improvement_pct)
    {
        v.kind = VerdictKind::Rejected;
        v.reason = format!(
            "no_gain: {} p95 {:.2} -> {:.2} ms ({:.1}% < {:.0}%)",
            f.fingerprint, f.p95_before, f.p95_after, f.improvement_pct, t.min_improvement_pct
        );
    } else {
        v.reason = format!(
            "target p95 {:.2} -> {:.2} ms; worst regression {:.1}%",
            v.target_p95_before.unwrap_or(0.0),
            v.target_p95_after.unwrap_or(0.0),
            worst_pct
        );
    }
    v.reason = truncate(std::mem::take(&mut v.reason), 128);
    v.details = details;
    v
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::float_cmp, clippy::many_single_char_names)]

    use super::*;

    fn run(ms: f64, uses: bool) -> Run {
        Run {
            ms,
            timed_out: false,
            uses_index: uses,
            rows_read: 0.0,
            lookups_via: 0.0,
        }
    }

    fn stmt(fp: u64, role: StatementRole, a: &[f64], b: &[f64], uses: bool) -> StatementRuns {
        StatementRuns {
            fingerprint: Fingerprint(fp),
            sample_id: 1,
            role,
            outcome: Outcome::Measured,
            a: a.iter().map(|&m| run(m, false)).collect(),
            b: b.iter().map(|&m| run(m, uses)).collect(),
            extra_rounds: a.len() > 3,
        }
    }

    use StatementRole::{Regression, Target};

    #[test]
    fn medians_and_p95() {
        assert_eq!(median(&[3.0, 1.0, 2.0]), 2.0);
        assert_eq!(median(&[4.0, 1.0, 2.0, 3.0]), 2.5);
        assert_eq!(median(&[]), 0.0);
        let v: Vec<f64> = (1..=20).map(f64::from).collect();
        assert_eq!(p95(&v), 19.0);
        assert_eq!(p95(&[5.0]), 5.0);
        let v: Vec<f64> = (1..=100).map(f64::from).collect();
        assert_eq!(p95(&v), 95.0);
    }

    #[test]
    fn noise_needs_ratio_and_floor() {
        let t = Thresholds::default();
        // 1.6 times but only 0.06 ms apart: not noise.
        assert!(!is_noisy(&[0.10, 0.16, 0.12], &t));
        // 1.6 times and 6 ms apart: noise.
        assert!(is_noisy(&[10.0, 16.0, 11.0], &t));
        // 1.4 times: fine.
        assert!(!is_noisy(&[10.0, 14.0, 12.0], &t));
        // One outlier among five is trimmed.
        assert!(!is_noisy(
            &trim_slowest(&[10.0, 11.0, 10.5, 10.2, 40.0]),
            &t
        ));
        assert!(is_noisy(&trim_slowest(&[10.0, 25.0, 10.5, 10.2, 40.0]), &t));
    }

    #[test]
    fn a_good_index_is_accepted() {
        let t = Thresholds::default();
        let s = vec![
            stmt(1, Target, &[10.0, 10.5, 10.2], &[0.3, 0.2, 0.25], true),
            stmt(1, Target, &[9.0, 9.5, 9.2], &[0.4, 0.3, 0.35], true),
            stmt(2, Regression, &[5.0, 5.1, 5.2], &[5.2, 5.3, 5.1], false),
        ];
        let v = decide(&s, &t);
        assert_eq!(v.kind, VerdictKind::Accepted, "{}", v.reason);
        assert_eq!(v.statements, 3);
        assert_eq!(v.noisy, 0);
        assert_eq!(v.target_p95_before, Some(10.2));
        assert_eq!(v.target_p95_after, Some(0.35));
        assert_eq!(v.fingerprints[0].samples, 2);
        assert!(v.fingerprints[0].improvement_pct > 95.0);
        assert_eq!(v.worst_regression_pct, 0.0);
        assert!(v.reason.starts_with("target p95 10.20 -> 0.35 ms"));
    }

    #[test]
    fn a_slower_statement_rejects() {
        let t = Thresholds::default();
        let mut slow = stmt(2, Regression, &[7.7, 7.8, 7.6], &[29.8, 30.1, 29.5], true);
        for r in &mut slow.b {
            r.lookups_via = 99_010.0;
        }
        let s = vec![
            stmt(1, Target, &[10.0, 10.0, 10.0], &[0.2, 0.2, 0.2], true),
            slow,
        ];
        let v = decide(&s, &t);
        assert_eq!(v.kind, VerdictKind::Rejected);
        assert!(
            v.reason
                .starts_with("regression: 0000000000000002 sample 1 +287.0%"),
            "{}",
            v.reason
        );
        assert_eq!(v.worst_fp, Some(Fingerprint(2)));
        assert!((v.worst_regression_pct - 287.01).abs() < 0.1);
        let h = v.covering_hint.unwrap();
        assert_eq!(h.fingerprint, Fingerprint(2));
        assert_eq!(h.lookups, 99_010.0);
    }

    #[test]
    fn regression_bound_has_a_floor() {
        let t = Thresholds::default();
        // 0.5 → 1.4 ms is +180% but within 1.10 × 0.5 + 1 = 1.55 ms.
        let s = vec![
            stmt(1, Target, &[10.0; 3], &[1.0; 3], true),
            stmt(2, Regression, &[0.5; 3], &[1.4; 3], false),
        ];
        let v = decide(&s, &t);
        assert_eq!(v.kind, VerdictKind::Accepted, "{}", v.reason);
        assert_eq!(v.worst_regression_pct, 0.0);
        // 20 → 23 ms is 15% and 3 ms: over 1.10 × 20 + 1 = 23 ms? No, equal: allowed.
        let s = vec![
            stmt(1, Target, &[10.0; 3], &[1.0; 3], true),
            stmt(2, Regression, &[20.0; 3], &[23.0; 3], false),
        ];
        assert_eq!(decide(&s, &t).kind, VerdictKind::Accepted);
        let s = vec![
            stmt(1, Target, &[10.0; 3], &[1.0; 3], true),
            stmt(2, Regression, &[20.0; 3], &[23.1; 3], false),
        ];
        let v = decide(&s, &t);
        assert_eq!(v.kind, VerdictKind::Rejected);
        assert!(v.covering_hint.is_none());
    }

    #[test]
    fn an_unused_or_weak_index_is_rejected() {
        let t = Thresholds::default();
        let s = vec![stmt(1, Target, &[10.0; 3], &[10.1; 3], false)];
        let v = decide(&s, &t);
        assert_eq!(v.kind, VerdictKind::Rejected);
        assert!(v.reason.starts_with("index_unused"), "{}", v.reason);
        // Used but only 20% better.
        let s = vec![stmt(1, Target, &[10.0; 3], &[8.0; 3], true)];
        let v = decide(&s, &t);
        assert_eq!(v.kind, VerdictKind::Rejected);
        assert!(v.reason.starts_with("no_gain"), "{}", v.reason);
        // Exactly 30%: accepted.
        let s = vec![stmt(1, Target, &[10.0; 3], &[7.0; 3], true)];
        assert_eq!(decide(&s, &t).kind, VerdictKind::Accepted);
    }

    #[test]
    fn p95_is_over_samples_not_the_mean() {
        let t = Thresholds::default();
        // 19 samples improve hugely, the slowest sample barely: p95 sees it.
        let mut s: Vec<StatementRuns> = (0..19)
            .map(|i| stmt(1, Target, &[10.0 + f64::from(i); 3], &[0.5; 3], true))
            .collect();
        s.push(stmt(1, Target, &[100.0; 3], &[90.0; 3], true));
        let v = decide(&s, &t);
        // p95 of 20 is the 19th value: before 28, after 0.5.
        assert_eq!(v.fingerprints[0].p95_before, 28.0);
        assert_eq!(v.kind, VerdictKind::Accepted);
        // With 10 samples the slowest is the p95.
        s.drain(0..10);
        let v = decide(&s, &t);
        assert_eq!(v.fingerprints[0].p95_before, 100.0);
        assert_eq!(v.fingerprints[0].p95_after, 90.0);
        assert_eq!(v.kind, VerdictKind::Rejected);
    }

    #[test]
    fn noisy_statements_skip_the_regression_check_but_count() {
        let t = Thresholds::default();
        // Still noisy after extra rounds (five A runs), and slower: ignored.
        let noisy = stmt(
            2,
            Regression,
            &[10.0, 30.0, 12.0, 25.0, 40.0],
            &[30.0, 31.0, 29.0, 30.0, 30.0],
            false,
        );
        let mut s = vec![noisy];
        for i in 0..4 {
            let mut x = stmt(1, Target, &[10.0; 3], &[1.0; 3], true);
            x.sample_id = i;
            s.push(x);
        }
        let v = decide(&s, &t);
        assert_eq!(v.noisy, 1);
        assert_eq!(v.kind, VerdictKind::Accepted, "{}", v.reason);
        assert!(v.details[0].noisy);
        // One noisy in four: 25% > 20%: inconclusive.
        s.truncate(4);
        let v = decide(&s, &t);
        assert_eq!(v.kind, VerdictKind::Inconclusive);
        assert!(v.reason.starts_with("too_noisy: 1 of 4"), "{}", v.reason);
    }

    #[test]
    fn extra_rounds_that_settle_are_not_noise() {
        let t = Thresholds::default();
        // The first rounds spread (10 vs 16), the extra ones settle; the
        // 16 is trimmed.
        let s = stmt(
            2,
            Regression,
            &[10.0, 16.0, 10.4, 10.2, 10.1],
            &[10.0; 5],
            false,
        );
        let e = evidence(&s, &t);
        assert!(!e.noisy);
    }

    #[test]
    fn failures_and_unreplayables() {
        let t = Thresholds::default();
        let mut s = vec![stmt(1, Target, &[10.0; 3], &[1.0; 3], true)];
        for i in 0..5 {
            let mut x = stmt(1, Target, &[10.0; 3], &[1.0; 3], true);
            x.sample_id = 10 + i;
            s.push(x);
        }
        s.push(StatementRuns {
            fingerprint: Fingerprint(3),
            sample_id: 9,
            role: Regression,
            outcome: Outcome::Unreplayable("INSERT ... VALUES".into()),
            a: vec![],
            b: vec![],
            extra_rounds: false,
        });
        let v = decide(&s, &t);
        assert_eq!(v.statements, 6);
        assert_eq!(v.kind, VerdictKind::Accepted);
        s.push(StatementRuns {
            fingerprint: Fingerprint(4),
            sample_id: 9,
            role: Regression,
            outcome: Outcome::Failed("1146".into()),
            a: vec![],
            b: vec![],
            extra_rounds: false,
        });
        s.push(s[7].clone());
        let v = decide(&s, &t);
        assert_eq!(v.noisy, 2);
        assert_eq!(v.kind, VerdictKind::Inconclusive, "{}", v.reason);
    }

    #[test]
    fn timeouts() {
        let t = Thresholds::default();
        // Before timed out at 10 s, after is fast: a big gain.
        let mut x = stmt(1, Target, &[10_000.0; 3], &[5.0; 3], true);
        for r in &mut x.a {
            r.timed_out = true;
        }
        let v = decide(std::slice::from_ref(&x), &t);
        assert_eq!(v.kind, VerdictKind::Accepted);
        assert!(v.details[0].timed_out);
        // After times out where before didn't: a regression.
        let mut y = stmt(2, Regression, &[900.0; 3], &[10_000.0; 3], true);
        for r in &mut y.b {
            r.timed_out = true;
        }
        let v = decide(&[x.clone(), y.clone()], &t);
        assert_eq!(v.kind, VerdictKind::Rejected);
        // Both sides time out: unmeasurable, counted as noisy.
        for r in &mut y.a {
            r.timed_out = true;
        }
        let e = evidence(&y, &t);
        assert!(e.noisy);
    }

    #[test]
    fn no_target_samples_is_inconclusive() {
        let t = Thresholds::default();
        let s = vec![
            StatementRuns {
                fingerprint: Fingerprint(1),
                sample_id: 1,
                role: Target,
                outcome: Outcome::Unreplayable("x".into()),
                a: vec![],
                b: vec![],
                extra_rounds: false,
            },
            stmt(2, Regression, &[1.0; 3], &[1.0; 3], false),
        ];
        let v = decide(&s, &t);
        assert_eq!(v.kind, VerdictKind::Inconclusive);
        assert!(v.reason.starts_with("no_target_samples"));
        assert_eq!(decide(&[], &t).reason, "nothing_replayed");
    }

    #[test]
    fn evidence_serializes_without_values() {
        let t = Thresholds::default();
        let v = decide(&[stmt(1, Target, &[10.0; 3], &[1.0; 3], true)], &t);
        let j = serde_json::to_value(&v.details).unwrap();
        assert_eq!(j[0]["status"], "measured");
        assert_eq!(j[0]["fingerprint"], 1);
        assert_eq!(j[0]["a_ms"], 10.0);
        assert!(j[0].get("sql").is_none());
    }
}
