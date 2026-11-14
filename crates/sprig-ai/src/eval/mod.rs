//! Evaluation sets and their runner.
//!
//! Each assistant has a set of cases in `crates/sprig-ai/evals/<role>.toml`
//! (see the README there): the user's text and context, a fake tool world
//! (what each tool returns), the tool calls a good answer makes, forbidden
//! behaviour, and a rubric for the answer.
//!
//! Two modes:
//! - **stub** (CI, every `cargo test`): a scripted model plays each case's
//!   expected calls and reference answer through [`crate::turn::Agent`].
//!   It checks the plumbing deterministically: every call is checked
//!   against the role and its schema, reads reach the host and proposals
//!   never do, nothing the gate must keep back is in any body sent, round
//!   and budget limits hold, and the reference answer passes its own
//!   rubric. [`probes`] add scripted misbehaviour: tools outside the role,
//!   writes through read tools, injected text, secrets.
//! - **live** (nightly, opt-in): a real model, scored by expected-tool
//!   match and the rubric, graded by deterministic checks and optionally
//!   by a judge model through the same [`crate::Provider`] trait.
//!
//! Results are JSON lines per case and a [`report::Summary`] per run,
//! recorded per model and prompt version ([`prompt_version`]); a run can
//! be compared with a baseline, and fails when a score drops.

pub mod case;
pub mod live;
pub mod probes;
pub mod report;
pub mod run;
pub mod world;

use std::sync::Arc;

use sha2::{Digest, Sha256};

pub use case::{Case, Set, check_set};
pub use report::{Summary, Verdict, compare};
pub use run::{CaseRecord, Judge, Mode, run_case, run_stub};

use crate::provider::{Provider, hex};
use crate::role::Role;
use crate::tools::ToolRegistry;

/// The shipped sets, by role: the file name and its text.
pub const SETS: [(Role, &str, &str); 4] = [
    (Role::Admin, "admin.toml", include_str!("../../evals/admin.toml")),
    (Role::Triage, "triage.toml", include_str!("../../evals/triage.toml")),
    (Role::Dba, "dba.toml", include_str!("../../evals/dba.toml")),
    (Role::Data, "data.toml", include_str!("../../evals/data.toml")),
];

/// The shipped set text of `role`.
#[must_use]
pub fn shipped(role: Role) -> &'static str {
    SETS.iter()
        .find(|(r, _, _)| *r == role)
        .map_or("", |(_, _, t)| t)
}

/// Reads a set.
///
/// # Errors
/// What doesn't parse.
pub fn load(text: &str) -> Result<Set, String> {
    toml::from_str(text).map_err(|e| e.to_string())
}

/// The prompt version of `role`: the first 12 hex digits of the SHA-256
/// of its base prompt and its tools' definitions. A change to either is a
/// new version, recorded with every result.
#[must_use]
pub fn prompt_version(role: Role) -> String {
    let mut h = Sha256::new();
    h.update(serde_json::to_string(&crate::prompt::base(role)).unwrap_or_default());
    for s in ToolRegistry::builtin().for_role(role) {
        h.update(serde_json::to_string(&s.def()).unwrap_or_default());
    }
    hex(&h.finalize())[..12].to_owned()
}

/// A set's version: the first 12 hex digits of its text's SHA-256.
#[must_use]
pub fn set_version(text: &str) -> String {
    hex(&Sha256::digest(text.as_bytes()))[..12].to_owned()
}

/// Runs the cases of `set` (those in `only`, when given) in stub mode.
pub async fn run_set_stub(set: &Set, only: &[String]) -> Vec<CaseRecord> {
    let mut out = Vec::new();
    for c in selected(set, only) {
        out.push(run_stub(set.role, c).await);
    }
    out
}

/// Runs the cases of `set` (those in `only`, when given) against a real
/// model, one at a time; `progress` hears of each case as it ends.
pub async fn run_set_live(
    set: &Set,
    only: &[String],
    provider: &Arc<dyn Provider>,
    model: &str,
    judge: Option<&Judge>,
    progress: &mut dyn FnMut(&CaseRecord),
) -> Vec<CaseRecord> {
    let mut out = Vec::new();
    for c in selected(set, only) {
        let r = run_case(set.role, c, Arc::clone(provider), model, Mode::Live, judge).await;
        progress(&r);
        out.push(r);
    }
    out
}

fn selected<'a>(set: &'a Set, only: &'a [String]) -> impl Iterator<Item = &'a Case> {
    set.cases
        .iter()
        .filter(move |c| only.is_empty() || only.iter().any(|o| *o == c.id))
}

#[cfg(test)]
mod tests;
