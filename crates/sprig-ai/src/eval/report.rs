//! Results: JSON lines per case, a summary per run, the summary table and
//! the comparison with a baseline.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::run::{CaseRecord, Mode};
use crate::provider::Usage;
use crate::role::Role;

/// One run of one set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Summary {
    /// The role.
    pub role: Role,
    /// Stub or live.
    pub mode: Mode,
    /// `provider/model`, or `stub`.
    pub model: String,
    /// The prompt version: a hash of the base prompt and the role's tools.
    pub prompt: String,
    /// The set's version: a hash of its file.
    pub set: String,
    /// When the run started, seconds since the Unix epoch.
    pub started: u64,
    /// Cases run.
    pub cases: usize,
    /// Cases passed.
    pub passed: usize,
    /// Cases with a safety failure.
    pub safety_failures: usize,
    /// Mean case score, 0 to 1.
    pub score: f64,
    /// Mean tool-choice score.
    pub tools: f64,
    /// Mean answer score.
    pub answer: f64,
    /// Tokens used.
    pub usage: Usage,
    /// Each case's score.
    pub per_case: BTreeMap<String, f64>,
    /// Mean score and count per tag.
    pub by_tag: BTreeMap<String, (f64, usize)>,
}

#[allow(clippy::cast_precision_loss)] // small counts
fn mean(xs: impl Iterator<Item = f64>) -> f64 {
    let (s, n) = xs.fold((0.0, 0usize), |(s, n), x| (s + x, n + 1));
    if n == 0 { 0.0 } else { s / n as f64 }
}

impl Summary {
    /// The summary of `records`.
    #[must_use]
    pub fn of(
        role: Role,
        mode: Mode,
        model: &str,
        prompt: &str,
        set: &str,
        started: u64,
        records: &[CaseRecord],
    ) -> Self {
        let mut usage = Usage::default();
        let mut tags: BTreeMap<String, Vec<f64>> = BTreeMap::new();
        for r in records {
            usage.add(&r.usage);
            for t in &r.tags {
                tags.entry(t.clone()).or_default().push(r.score);
            }
        }
        Self {
            role,
            mode,
            model: model.to_owned(),
            prompt: prompt.to_owned(),
            set: set.to_owned(),
            started,
            cases: records.len(),
            passed: records.iter().filter(|r| r.pass).count(),
            safety_failures: records.iter().filter(|r| !r.safe).count(),
            score: mean(records.iter().map(|r| r.score)),
            tools: mean(records.iter().map(|r| r.tools)),
            answer: mean(records.iter().map(|r| r.answer)),
            usage,
            per_case: records.iter().map(|r| (r.id.clone(), r.score)).collect(),
            by_tag: tags
                .into_iter()
                .map(|(k, v)| {
                    let n = v.len();
                    (k, (mean(v.into_iter()), n))
                })
                .collect(),
        }
    }
}

/// The summary table of `runs`.
#[must_use]
pub fn table(runs: &[Summary]) -> String {
    let mut s = String::new();
    let _ = writeln!(
        s,
        "{:<7} {:<5} {:<32} {:<12} {:>5} {:>6} {:>6} {:>6} {:>6} {:>7}",
        "role", "mode", "model", "prompt", "cases", "passed", "score", "tools", "answer", "unsafe"
    );
    for r in runs {
        let _ = writeln!(
            s,
            "{:<7} {:<5} {:<32} {:<12} {:>5} {:>6} {:>6.3} {:>6.3} {:>6.3} {:>7}",
            r.role.as_str(),
            r.mode.as_str(),
            r.model,
            r.prompt,
            r.cases,
            r.passed,
            r.score,
            r.tools,
            r.answer,
            r.safety_failures
        );
    }
    s
}

/// A comparison with a baseline.
#[derive(Debug, Clone, PartialEq)]
pub struct Verdict {
    /// No score dropped.
    pub ok: bool,
    /// What changed, one line each.
    pub lines: Vec<String>,
}

/// Compares `cur` with `base`: it fails when the mean score drops by more
/// than `tolerance`, or more cases fail on safety.
#[must_use]
pub fn compare(cur: &Summary, base: &Summary, tolerance: f64) -> Verdict {
    let mut lines = Vec::new();
    let mut ok = true;
    if cur.role != base.role {
        return Verdict {
            ok: false,
            lines: vec![format!(
                "the baseline is for the {} set, not {}",
                base.role, cur.role
            )],
        };
    }
    if cur.set != base.set {
        lines.push(format!(
            "the set changed ({} → {}); cases are compared by id",
            base.set, cur.set
        ));
    }
    let d = cur.score - base.score;
    if d < -tolerance {
        ok = false;
        lines.push(format!(
            "{}: score dropped {:.3} → {:.3} ({:+.3}, tolerance {tolerance})",
            cur.role, base.score, cur.score, d
        ));
    } else {
        lines.push(format!(
            "{}: score {:.3} → {:.3} ({:+.3})",
            cur.role, base.score, cur.score, d
        ));
    }
    if cur.safety_failures > base.safety_failures {
        ok = false;
        lines.push(format!(
            "{}: safety failures rose {} → {}",
            cur.role, base.safety_failures, cur.safety_failures
        ));
    }
    for (id, b) in &base.per_case {
        if let Some(c) = cur.per_case.get(id)
            && *c + 0.25 < *b
        {
            lines.push(format!("  {id}: {b:.2} → {c:.2}"));
        }
    }
    Verdict { ok, lines }
}

/// Where a run's files go: `<out>/<role>/<model>/`.
#[must_use]
pub fn run_dir(out: &Path, role: Role, model: &str) -> PathBuf {
    let slug: String = model
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect();
    out.join(role.as_str()).join(slug)
}

/// Writes `records` as JSON lines and `summary` as JSON, under
/// [`run_dir`], named by prompt version and start time. Returns the
/// summary's path.
///
/// # Errors
/// The file system's.
pub fn write(out: &Path, summary: &Summary, records: &[CaseRecord]) -> std::io::Result<PathBuf> {
    let dir = run_dir(out, summary.role, &summary.model);
    std::fs::create_dir_all(&dir)?;
    let stem = format!(
        "{}-{}-{}",
        summary.started,
        summary.mode.as_str(),
        summary.prompt
    );
    let mut lines = String::new();
    for r in records {
        lines.push_str(&serde_json::to_string(r).map_err(std::io::Error::other)?);
        lines.push('\n');
    }
    std::fs::write(dir.join(format!("{stem}.jsonl")), lines)?;
    let path = dir.join(format!("{stem}.summary.json"));
    std::fs::write(
        &path,
        serde_json::to_string_pretty(summary).map_err(std::io::Error::other)?,
    )?;
    Ok(path)
}

/// The newest summary under [`run_dir`] other than `except`.
#[must_use]
pub fn last_summary(out: &Path, role: Role, model: &str, except: &Path) -> Option<PathBuf> {
    let dir = run_dir(out, role, model);
    let mut found: Vec<PathBuf> = std::fs::read_dir(dir)
        .ok()?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.to_string_lossy().ends_with(".summary.json") && p.as_path() != except)
        .collect();
    // Names start with the start time, so the last one is the newest.
    found.sort();
    found.pop()
}

/// Reads a summary.
///
/// # Errors
/// The file system's, or a file that isn't a summary.
pub fn read_summary(path: &Path) -> std::io::Result<Summary> {
    let text = std::fs::read_to_string(path)?;
    serde_json::from_str(&text).map_err(std::io::Error::other)
}
