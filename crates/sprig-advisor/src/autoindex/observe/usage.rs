//! Decision O1: an index is removed by how its table is used, never by how
//! much time has passed. The pure part: reads counted, the usage ratio,
//! the value check, periodic jobs, and the regression check while an index
//! is `IGNORED`.
//!
//! ```text
//! judge(x) once the table has had usage_sample_reads reads since x was built
//! or last judged (the "sample"):
//!   ratio  := runs of read fingerprints whose plan uses x / all reads
//!   saved  := Σ over those fingerprints runs × saved per run
//!           + Σ over remembered fingerprints still in the workload but not in
//!             this sample, saved per run × runs when last seen   (periodic jobs)
//!   cost   := index entry changes by the sample's writes × maintenance_cost_ms
//!   ratio < min_usage_ratio and saved < cost   → remove (IGNORED, then dropped)
//!   else                                       → keep; the next sample starts now
//! IGNORED: a fingerprint x served got slower than max_regression_pct (and by
//!   more than NOISE_FLOOR_MS) → visible again and kept; once the table has had
//!   usage_sample_reads reads since x was ignored with no regression → dropped
//! ```
//!
//! **Saved per run.** For an automatic index whose fingerprint was measured
//! before the build (its baseline), the measured difference of means. For
//! any other index, an estimate that errs toward keeping: the statement's
//! time per row read through the index, times the rows a full scan would
//! read instead. A fingerprint the plan can't tell about (its sample
//! didn't bind or `EXPLAIN` failed) counts as "unknown": when the unknown
//! reads alone could lift the index over `min_usage_ratio`, nothing is
//! decided.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sprig_common::ids::Fingerprint;

use super::FpLatency;

/// A regression smaller than this, in ms, is noise even when it is above
/// `max_regression_pct` (a 0.1 ms lookup measured at 0.12 ms).
pub const NOISE_FLOOR_MS: f64 = 0.5;

/// One read fingerprint of the sample, with what its plan says about the
/// index.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReadUse {
    /// The fingerprint.
    pub fp: Fingerprint,
    /// Runs in the sample.
    pub runs: u64,
    /// Its plan reads the index; `None` when it couldn't be explained.
    pub uses: Option<bool>,
    /// Time saved per run when it uses the index, ms ([`saved_per_run`]).
    pub saved_ms_per_run: f64,
}

/// A fingerprint the index served, remembered across samples so a rare
/// but valuable query (a month-end report) keeps its index.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Remembered {
    /// Time saved per run, ms.
    pub saved_ms_per_run: f64,
    /// Runs in the last sample it appeared in.
    pub runs: u64,
    /// When it was last seen using the index, Unix ms.
    pub seen_ms: u64,
}

/// The rule's settings, resolved at the object.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UsageRules {
    /// `auto_index.usage_sample_reads`.
    pub sample_reads: u64,
    /// `auto_index.min_usage_ratio`, a fraction.
    pub min_usage_ratio: f64,
    /// `auto_index.maintenance_cost_ms`.
    pub maintenance_cost_ms: f64,
}

/// What a judgement measured, kept in `observed.usage.last`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct UsageMeasures {
    /// Reads in the sample.
    pub reads: u64,
    /// Reads whose plan used the index.
    pub served: u64,
    /// Reads whose plan couldn't be told.
    pub unknown: u64,
    /// `served / reads`.
    pub ratio: f64,
    /// Time saved by the sample's served reads, ms.
    pub saved_ms: f64,
    /// Time saved by remembered periodic fingerprints, ms.
    pub periodic_saved_ms: f64,
    /// Index entry changes by the sample's writes.
    pub entry_changes: f64,
    /// Their cost, ms.
    pub write_cost_ms: f64,
    /// The fingerprints that used it (16 hex digits).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub served_fps: Vec<String>,
}

/// The judgement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageVerdict {
    /// Fewer reads than `usage_sample_reads` so far.
    NotYet,
    /// The unknown reads could change the answer: decide nothing.
    Unknown,
    /// Used enough, or worth its write cost.
    Keep,
    /// Rarely used and costs more than it saves.
    Remove,
}

/// Reads on the table in `lat`: the runs of every read fingerprint.
#[must_use]
pub fn reads_in(lat: &BTreeMap<Fingerprint, FpLatency>) -> u64 {
    lat.values().filter(|l| !l.write).map(|l| l.runs).sum()
}

/// Index entry changes the writes of `lat` make on an index over
/// `columns` (lower case): inserts and deletes change every index, an
/// update only the indexes over a column it sets (all of them when its
/// columns aren't known), each run changing `affected_per_run` rows
/// (at least one).
#[must_use]
pub fn entry_changes(lat: &BTreeMap<Fingerprint, FpLatency>, columns: &[String]) -> f64 {
    lat.values()
        .filter(|l| l.write)
        .filter(|l| {
            l.updated.as_ref().is_none_or(|set| {
                set.iter()
                    .any(|c| columns.iter().any(|x| x.eq_ignore_ascii_case(c)))
            })
        })
        .map(|l| f(l.runs) * l.affected_per_run.max(1.0))
        .sum::<f64>()
        // An empty sum is -0.0.
        + 0.0
}

#[allow(clippy::cast_precision_loss)] // runs are far below 2^52
fn f(n: u64) -> f64 {
    n as f64
}

/// Time one run saves through the index, ms. `baseline_mean`: its mean
/// before an automatic index was built (measured); otherwise the time per
/// row read through the index (`mean_ms / rows_with`) times the rows a full
/// scan of `table_rows` would read instead: an estimate that errs toward
/// keeping the index.
#[must_use]
pub fn saved_per_run(
    mean_ms: f64,
    baseline_mean: Option<f64>,
    rows_with: f64,
    table_rows: f64,
) -> f64 {
    if let Some(b) = baseline_mean {
        return (b - mean_ms).max(0.0);
    }
    let r = rows_with.max(1.0);
    (mean_ms / r * (table_rows - r)).max(0.0)
}

/// Judges one index over a sample (module docs). `alive` holds the
/// fingerprints still in the workload; `None` means it couldn't be read,
/// and every remembered one counts.
#[must_use]
pub fn judge(
    reads: &[ReadUse],
    entry_changes: f64,
    remembered: &BTreeMap<String, Remembered>,
    alive: Option<&BTreeSet<Fingerprint>>,
    rules: &UsageRules,
) -> (UsageVerdict, UsageMeasures) {
    let mut m = UsageMeasures {
        reads: reads.iter().map(|r| r.runs).sum(),
        entry_changes,
        write_cost_ms: entry_changes * rules.maintenance_cost_ms,
        ..UsageMeasures::default()
    };
    if m.reads < rules.sample_reads.max(1) {
        return (UsageVerdict::NotYet, m);
    }
    for r in reads {
        match r.uses {
            Some(true) => {
                m.served += r.runs;
                m.saved_ms += f(r.runs) * r.saved_ms_per_run;
                m.served_fps.push(r.fp.to_string());
            }
            Some(false) => {}
            None => m.unknown += r.runs,
        }
    }
    let here: BTreeSet<String> = m.served_fps.iter().cloned().collect();
    for (fp, rem) in remembered {
        if here.contains(fp) {
            continue;
        }
        let still = match (alive, fp.parse::<Fingerprint>()) {
            (Some(a), Ok(id)) => a.contains(&id),
            _ => true,
        };
        if still {
            m.periodic_saved_ms += f(rem.runs) * rem.saved_ms_per_run;
        }
    }
    m.ratio = f(m.served) / f(m.reads);
    let floor = rules.min_usage_ratio * f(m.reads);
    // Strictly less (Kiran, decision O1): an index on a table nobody
    // writes costs no write time, so even an unused one is kept there.
    if f(m.served) >= floor || m.saved_ms + m.periodic_saved_ms >= m.write_cost_ms {
        return (UsageVerdict::Keep, m);
    }
    if m.unknown > 0 && f(m.served + m.unknown) >= floor {
        return (UsageVerdict::Unknown, m);
    }
    (UsageVerdict::Remove, m)
}

/// Updates the remembered fingerprints after a judgement: the sample's
/// served ones with their savings, and those no longer in the workload
/// forgotten (`alive` known).
pub fn remember(
    memory: &mut BTreeMap<String, Remembered>,
    reads: &[ReadUse],
    alive: Option<&BTreeSet<Fingerprint>>,
    now_ms: u64,
) {
    for r in reads
        .iter()
        .filter(|r| r.uses == Some(true) && r.saved_ms_per_run > 0.0)
    {
        memory.insert(
            r.fp.to_string(),
            Remembered {
                saved_ms_per_run: r.saved_ms_per_run,
                runs: r.runs,
                seen_ms: now_ms,
            },
        );
    }
    if let Some(a) = alive {
        memory.retain(|fp, _| fp.parse::<Fingerprint>().is_ok_and(|id| a.contains(&id)));
    }
}

/// A served fingerprint's latency before its index was ignored.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Before {
    /// Runs.
    pub runs: u64,
    /// p95, ms.
    pub p95_ms: f64,
    /// Mean, ms.
    pub mean_ms: f64,
}

/// The fingerprints of `fps` in `lat` as [`Before`]s, by 16-hex id.
#[must_use]
pub fn before_of(
    fps: impl IntoIterator<Item = Fingerprint>,
    lat: &BTreeMap<Fingerprint, FpLatency>,
) -> BTreeMap<String, Before> {
    fps.into_iter()
        .filter_map(|f| {
            lat.get(&f).map(|l| {
                (
                    f.to_string(),
                    Before {
                        runs: l.runs,
                        p95_ms: l.p95_ms,
                        mean_ms: l.mean_ms,
                    },
                )
            })
        })
        .collect()
}

/// A regression while ignored: the fingerprint, its p95 before and now.
#[derive(Debug, Clone, PartialEq)]
pub struct Regressed {
    /// The fingerprint.
    pub fp: String,
    /// p95 before, ms.
    pub before_ms: f64,
    /// p95 since the index was ignored, ms.
    pub now_ms: f64,
}

/// The worst fingerprint of `before` that got slower than
/// `max_regression_pct` and by more than [`NOISE_FLOOR_MS`] since the index
/// was ignored, with at least `min_runs` runs since.
#[must_use]
pub fn regressed(
    before: &BTreeMap<String, Before>,
    now: &BTreeMap<Fingerprint, FpLatency>,
    max_regression_pct: f64,
    min_runs: u64,
) -> Option<Regressed> {
    let mut worst: Option<(f64, Regressed)> = None;
    for (fp, b) in before {
        let Ok(id) = fp.parse::<Fingerprint>() else {
            continue;
        };
        let Some(n) = now.get(&id) else { continue };
        if n.runs < min_runs || b.p95_ms <= 0.0 {
            continue;
        }
        let pct = 100.0 * (n.p95_ms / b.p95_ms - 1.0);
        if pct > max_regression_pct
            && n.p95_ms - b.p95_ms > NOISE_FLOOR_MS
            && worst.as_ref().is_none_or(|(w, _)| pct > *w)
        {
            worst = Some((
                pct,
                Regressed {
                    fp: fp.clone(),
                    before_ms: b.p95_ms,
                    now_ms: n.p95_ms,
                },
            ));
        }
    }
    worst.map(|(_, r)| r)
}
