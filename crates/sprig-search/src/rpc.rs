//! The messages between a gateway and a copy
//! (`sprig.internal.v1.SearchPartition`), carried as JSON inside the gRPC
//! messages, and the work a copy does for one.
//!
//! ```text
//! Query(index, generation, query, ops, wait):
//!   the copy of that generation, else 39825 (retryable: another copy)
//!   wait: ready (stale_ok), or every target position applied (latest)
//!   each op over the same query: top k, all (up to a window), count, facet
//!   → answers, and the positions the copy had applied
//! ```

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sprig_common::{SprigCode, SprigError};

use crate::query::SearchQuery;
use crate::search::{Bucket, Hit, Scoring, SearchIndex};

/// One piece of work over the query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Op {
    /// The best `k` hits.
    Top {
        /// Hits.
        k: usize,
        /// Statistics.
        scoring: Scoring,
    },
    /// Every hit, 39827 past `max`.
    All {
        /// `search.max_window`.
        max: usize,
    },
    /// The number of hits.
    Count,
    /// A facet's buckets.
    Facet {
        /// The field.
        path: String,
        /// Buckets.
        n: usize,
        /// Number boundaries.
        bounds: Option<Vec<f64>>,
    },
}

/// What to wait for before answering.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Wait {
    /// Longest wait (`search.wait_timeout_ms`).
    pub timeout_ms: u64,
    /// Each cluster's position to reach (`latest`); empty: only ready.
    pub targets: Vec<(String, String)>,
}

/// A query for one copy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryRequest {
    /// The tenant.
    pub tenant: u64,
    /// The index.
    pub index: u64,
    /// Its generation; a copy of another answers 39825.
    pub generation: u32,
    /// The query.
    pub query: SearchQuery,
    /// What to compute.
    pub ops: Vec<Op>,
    /// `search.max_expansions`.
    pub max_expansions: usize,
    /// The wait.
    pub wait: Wait,
}

/// One op's answer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Answer {
    /// Hits, best first.
    Hits(Vec<Hit>),
    /// A count.
    Count(u64),
    /// Facet buckets.
    Buckets(Vec<Bucket>),
}

/// An error across the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireError {
    /// The SprigDB code's number (39xxx); 0 for internal.
    pub code: u16,
    /// The message.
    pub message: String,
    /// Whether another copy may answer.
    pub retryable: bool,
}

impl WireError {
    /// From an error.
    #[must_use]
    pub fn of(e: &SprigError) -> Self {
        Self {
            code: e.code().map_or(0, SprigCode::number),
            message: e.message.clone(),
            retryable: e.retryable,
        }
    }

    /// Back to an error.
    #[must_use]
    pub fn error(&self) -> SprigError {
        let code = SprigCode::ALL
            .iter()
            .copied()
            .find(|c| c.number() == self.code)
            .unwrap_or(SprigCode::Internal);
        let mut e = SprigError::new(code, self.message.clone());
        e.retryable = self.retryable;
        e
    }
}

/// A copy's reply.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryReply {
    /// One answer per op, or the error.
    pub answers: Result<Vec<Answer>, WireError>,
    /// The positions the copy had applied when it answered.
    pub applied: BTreeMap<String, String>,
}

/// Runs `ops` over `query` on `index` (blocking: call from a blocking
/// thread).
///
/// # Errors
/// 39822, 39827 and Tantivy's.
pub fn execute(
    index: &SearchIndex,
    query: &SearchQuery,
    ops: &[Op],
    max_expansions: usize,
) -> sprig_common::Result<Vec<Answer>> {
    ops.iter()
        .map(|op| {
            Ok(match op {
                Op::Top { k, scoring } => {
                    Answer::Hits(index.top(query, *k, *scoring, max_expansions)?)
                }
                Op::All { max } => Answer::Hits(index.all(query, *max, max_expansions)?),
                Op::Count => Answer::Count(index.count(query, max_expansions)?),
                Op::Facet { path, n, bounds } => Answer::Buckets(index.facet(
                    query,
                    path,
                    *n,
                    bounds.as_deref(),
                    max_expansions,
                )?),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::Lit;

    #[test]
    fn requests_and_errors_round_trip() {
        let r = QueryRequest {
            tenant: 7,
            index: 42,
            generation: 3,
            query: SearchQuery::Bool {
                must: vec![SearchQuery::Match {
                    fields: vec!["name".into()],
                    text: "red shoe".into(),
                    all: false,
                    fuzzy: Some(1),
                }],
                should: vec![],
                must_not: vec![SearchQuery::Prefix {
                    fields: vec![],
                    text: "kid".into(),
                }],
                filter: vec![SearchQuery::Range {
                    field: "price".into(),
                    lo: std::ops::Bound::Included(Lit::Num(1.5)),
                    hi: std::ops::Bound::Unbounded,
                }],
                min_should: 0,
            },
            ops: vec![
                Op::Top {
                    k: 10,
                    scoring: Scoring::Global,
                },
                Op::Count,
                Op::Facet {
                    path: "brand".into(),
                    n: 5,
                    bounds: None,
                },
            ],
            max_expansions: 50,
            wait: Wait {
                timeout_ms: 2000,
                targets: vec![("dc-a-c1".into(), "0-1-5".into())],
            },
        };
        let s = serde_json::to_string(&r).unwrap();
        assert_eq!(serde_json::from_str::<QueryRequest>(&s).unwrap(), r);
        let e = SprigError::new(SprigCode::SearchWaitTimeout, "late");
        let back = WireError::of(&e).error();
        assert_eq!(back.code(), Some(SprigCode::SearchWaitTimeout));
        assert!(back.retryable == e.retryable);
    }
}
