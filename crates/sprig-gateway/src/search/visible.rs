//! Counts and facets over the rows a caller may see.
//!
//! A copy is filled without row-level security, so its counts and
//! facet buckets include rows a policy hides from the caller. Hits are
//! already re-read through the caller's session; counts and facets are
//! not, so for a caller that a policy on the indexed object binds:
//!
//! ```text
//! every hit of the query (39827 past search.max_window, without the count)
//! visible := the hits whose rows the caller's session reads
//! count  := |visible|
//! facets := the copy's facets over (query AND _key IN visible)
//! ```
//!
//! Callers no policy binds keep the single call to the copy. A front end
//! that can't tell whether a policy binds (`None`) takes the visible path.

use std::sync::Arc;

use sprig_catalog::TenantCatalog;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_search::Hit;
use sprig_search::def::IndexDef;
use sprig_search::query::SearchQuery;
use sprig_search::schema::Key;

use super::{Consistency, SearchService};

/// Whether `binds` (a session's answer for the index's object) means the
/// visible path: bound, or unknown, or an index whose object isn't known.
#[must_use]
pub fn needs_visible(def: &IndexDef, binds: Option<bool>) -> bool {
    def.object_id == 0 || binds != Some(false)
}

/// The refusal past the window, worded without the number of matches
/// (which counts hidden rows).
#[must_use]
pub fn too_many(max: usize) -> SprigError {
    SprigError::new(
        SprigCode::SearchWindowExceeded,
        format!(
            "counts and facets for a user bound by row-level security on this table are taken \
             over the rows the user may see, which needs the query to match at most \
             search.max_window ({max}) rows; narrow the query or ask for no facets"
        ),
    )
}

/// Every hit of `query` (for re-reading), 39827 as [`too_many`].
///
/// # Errors
/// [`too_many`]; the copy's.
pub async fn all_hits(
    svc: &Arc<SearchService>,
    tenant: TenantId,
    cat: &TenantCatalog,
    def: &Arc<IndexDef>,
    query: SearchQuery,
    consistency: Consistency,
) -> Result<Vec<Hit>> {
    let max = svc.knobs().max_window;
    crate::exec::search::run_search(svc, tenant, cat, def, query, None, consistency)
        .await
        .map_err(|e| {
            if e.code() == Some(SprigCode::SearchWindowExceeded) {
                metrics::counter!("sprig_search_window_exceeded_total").increment(1);
                too_many(max)
            } else {
                e
            }
        })
}

/// `query` restricted to the documents with these keys.
#[must_use]
pub fn within(query: SearchQuery, visible: Vec<Key>) -> SearchQuery {
    SearchQuery::Bool {
        must: vec![query],
        should: Vec::new(),
        must_not: Vec::new(),
        filter: vec![SearchQuery::Keys(visible)],
        min_should: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_search::def::{Flavor, Model};

    #[test]
    fn only_known_unbound_callers_skip_the_visible_path() {
        let mut d = IndexDef::new("ix", "db", "t", Model::Table, Flavor::Sql);
        d.object_id = 42;
        assert!(!needs_visible(&d, Some(false)));
        assert!(needs_visible(&d, Some(true)));
        assert!(needs_visible(&d, None));
        d.object_id = 0;
        assert!(needs_visible(&d, Some(false)));
        let e = too_many(10_000);
        assert_eq!(e.code(), Some(SprigCode::SearchWindowExceeded));
        assert!(
            e.message.contains("search.max_window (10000)"),
            "{}",
            e.message
        );
        match within(SearchQuery::All, vec![Key(vec![1])]) {
            SearchQuery::Bool { must, filter, .. } => {
                assert_eq!(must, vec![SearchQuery::All]);
                assert_eq!(filter, vec![SearchQuery::Keys(vec![Key(vec![1])])]);
            }
            other => panic!("{other:?}"),
        }
    }
}
