//! The `EXPLAIN` check of a covered candidate (chapter 13, "Merging
//! candidates and checking existing indexes").
//!
//! When an existing index `e` covers a candidate on paper, the candidate's
//! fingerprints are explained with sampled parameters
//! (`EXPLAIN FORMAT=JSON`). If the optimizer uses `e`, the candidate is
//! `covered` (reason `covered_by:<e>`). If it doesn't, the candidate
//! becomes `analyze`: an `auto_index_analyze` job refreshes the table's
//! statistics and the next evaluation checks again; if the optimizer still
//! avoids `e`, the reason is recorded (`optimizer_avoids:<e>`) and no
//! duplicate index is created.
//!
//! The statement comes bound and in physical names, as the replay harness
//! prepares it ([`ReplayStatement::bind`](super::super::replay::ReplayStatement::bind)
//! and chapter 07's rewriter); this module never sees or logs values.

use mysql_async::prelude::Queryable;
use sprig_common::{Result, SprigError};

use super::super::replay::parse_plan;
use super::super::store::CandidateStatus;
use crate::sql::map_err;

/// What `EXPLAIN` showed for one statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainOutcome {
    /// The plan reads the index.
    pub uses: bool,
    /// The optimizer's row estimate over the statement's tables.
    pub rows_estimate: f64,
}

/// Runs `EXPLAIN FORMAT=JSON <sql>` on `q`'s connection (whose default
/// database is the statement's) and reports whether the plan uses
/// `index`.
///
/// # Errors
/// 39070 or a backend error; `Internal` for a plan that doesn't parse.
pub async fn explain_uses<Q: Queryable + Send>(
    q: &mut Q,
    sql: &str,
    index: &str,
) -> Result<ExplainOutcome> {
    let json: Option<String> = q
        .query_first(format!("EXPLAIN FORMAT=JSON {sql}"))
        .await
        .map_err(map_err)?;
    let json = json.ok_or_else(|| SprigError::internal("EXPLAIN returned nothing"))?;
    let plan = parse_plan(&json).map_err(|e| SprigError::internal(e.to_string()))?;
    Ok(ExplainOutcome {
        uses: plan.uses_index(index),
        rows_estimate: plan.rows_estimate(),
    })
}

/// The status of a candidate an existing index covers, from the `EXPLAIN`
/// outcomes of its fingerprints' sampled statements and whether its
/// statistics were refreshed since the last check: `covered` when every
/// statement uses the index (or there was nothing to explain), else
/// `analyze` the first time and `covered` with `optimizer_avoids` once
/// statistics are fresh. Returns the status and its reason.
#[must_use]
pub fn coverage_status(
    index: &str,
    outcomes: &[ExplainOutcome],
    analyzed_since_last_check: bool,
) -> (CandidateStatus, String) {
    if outcomes.iter().all(|o| o.uses) {
        return (CandidateStatus::Covered, format!("covered_by:{index}"));
    }
    if analyzed_since_last_check {
        (
            CandidateStatus::Covered,
            format!("optimizer_avoids:{index}"),
        )
    } else {
        (CandidateStatus::Analyze, format!("analyze_for:{index}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coverage_status_follows_the_chapter() {
        let yes = ExplainOutcome {
            uses: true,
            rows_estimate: 2.0,
        };
        let no = ExplainOutcome {
            uses: false,
            rows_estimate: 2e6,
        };
        assert_eq!(
            coverage_status("idx_a", std::slice::from_ref(&yes), false),
            (CandidateStatus::Covered, "covered_by:idx_a".into())
        );
        assert_eq!(
            coverage_status("idx_a", &[yes, no.clone()], false).0,
            CandidateStatus::Analyze
        );
        assert_eq!(
            coverage_status("idx_a", &[no], true),
            (CandidateStatus::Covered, "optimizer_avoids:idx_a".into())
        );
        assert_eq!(
            coverage_status("idx_a", &[], false).0,
            CandidateStatus::Covered
        );
    }
}
