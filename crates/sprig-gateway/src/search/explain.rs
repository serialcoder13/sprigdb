//! `EXPLAIN` of searches: what a search ran on, shared by SQL's `EXPLAIN` and
//! MongoDB's `explain`.
//!
//! ```text
//! SearchScan {index, object, model, flavor, generation, partitions,
//!             consistency, scoring, copies (where, ready), query (the AST),
//!             hits and search_ms when the search ran and no row policy
//!             binds the caller}
//! SQL EXPLAIN [FORMAT=...] <statement with MATCH or sprig_search()>:
//!   the searches run (as the statement would), the statement's EXPLAIN
//!   from MariaDB, then one row per search: select_type SEARCH, type
//!   fulltext, key = the index, rows = hits, Extra = SearchScan(...);
//!   single-column formats (JSON, TREE) get one row {"sprig_search_scans": [...]}
//! ```

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use sprig_common::ids::TenantId;
use sprig_search::def::IndexDef;
use sprig_search::query::SearchQuery;

use super::{Consistency, SearchService};

/// One search a statement or command ran.
#[derive(Debug, Clone)]
pub struct Scan {
    /// The `SearchScan` document.
    pub doc: Value,
    /// The object's name as the statement names it (alias, else table).
    pub table: String,
    /// The index's name.
    pub index: String,
    /// Hits the copy answered (None when not shown).
    pub hits: Option<usize>,
}

/// The consistency's name.
#[must_use]
pub fn consistency_name(c: Consistency) -> &'static str {
    match c {
        Consistency::StaleOk => "stale_ok",
        Consistency::Latest => "latest",
    }
}

/// The `SearchScan` document of a search of `def` with `query`. `hits` and
/// `took` are shown only when given (callers leave them out for callers a
/// row policy binds: the copy's hit count includes hidden rows).
pub async fn describe(
    svc: &Arc<SearchService>,
    tenant: TenantId,
    def: &IndexDef,
    query: &SearchQuery,
    consistency: Consistency,
    hits: Option<usize>,
    took: Option<Duration>,
) -> Value {
    let knobs = svc.knobs();
    let (ready, place) = svc.state(tenant, def).await;
    let mut v = json!({
        "index": format!("{}.{}", def.db, def.name),
        "object": def.object,
        "model": def.model.as_str(),
        "flavor": def.flavor.as_str(),
        "generation": def.generation,
        "partitions": def.partitions,
        "consistency": consistency_name(consistency),
        "scoring": match knobs.scoring {
            sprig_search::Scoring::Global => "global",
            sprig_search::Scoring::Local => "local",
        },
        "copies": place,
        "ready": ready,
        "query": serde_json::to_value(query).unwrap_or(Value::Null),
    });
    if let Some(n) = hits {
        v["hits"] = json!(n);
    }
    if let Some(t) = took {
        v["search_ms"] = json!((t.as_secs_f64() * 1000.0 * 10.0).round() / 10.0);
    }
    v
}

/// The `Extra` text of a SQL `EXPLAIN` row: `SearchScan(...)` on one line.
#[must_use]
pub fn extra_text(doc: &Value) -> String {
    let g = |k: &str| match doc.get(k) {
        Some(Value::String(s)) => s.clone(),
        Some(v) => v.to_string(),
        None => String::new(),
    };
    let mut s = format!(
        "SearchScan(index {}, {} partitions, consistency {}, scoring {}, copies: {}",
        g("index"),
        g("partitions"),
        g("consistency"),
        g("scoring"),
        g("copies")
    );
    if doc.get("hits").is_some() {
        let _ = write!(s, ", {} hits in {} ms", g("hits"), g("search_ms"));
    }
    let _ = write!(s, "; query {})", g("query"));
    s
}

/// Whether a statement is an `EXPLAIN` (`EXPLAIN`, `DESCRIBE` or `DESC`
/// first, after comments and spaces).
#[must_use]
pub fn is_explain(sql: &str) -> bool {
    let mut s = sql.trim_start();
    loop {
        if let Some(r) = s.strip_prefix("/*") {
            match r.find("*/") {
                Some(i) => s = r[i + 2..].trim_start(),
                None => return false,
            }
        } else if s.starts_with("-- ") || s.starts_with('#') {
            match s.find('\n') {
                Some(i) => s = s[i + 1..].trim_start(),
                None => return false,
            }
        } else {
            break;
        }
    }
    let word: String = s
        .chars()
        .take_while(char::is_ascii_alphabetic)
        .collect::<String>()
        .to_ascii_uppercase();
    matches!(word.as_str(), "EXPLAIN" | "DESCRIBE" | "DESC")
}

/// The values of one appended `EXPLAIN` row for `scan`, by the result's
/// column names (`None` for NULL); single-column results get the scans as
/// JSON.
#[must_use]
pub fn row_for(columns: &[String], scan: &Scan) -> Vec<Option<String>> {
    columns
        .iter()
        .map(|c| match c.to_ascii_lowercase().as_str() {
            "id" => Some("1".to_owned()),
            "select_type" => Some("SEARCH".to_owned()),
            "table" => Some(scan.table.clone()),
            "type" => Some("fulltext".to_owned()),
            "possible_keys" | "key" => Some(scan.index.clone()),
            "rows" => scan.hits.map(|n| n.to_string()),
            "filtered" => Some("100.00".to_owned()),
            "extra" => Some(extra_text(&scan.doc)),
            _ => None,
        })
        .collect()
}

/// The single-column form: `{"sprig_search_scans": [...]}`.
#[must_use]
pub fn json_row(scans: &[Scan]) -> String {
    json!({"sprig_search_scans": scans.iter().map(|s| s.doc.clone()).collect::<Vec<_>>()})
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explain_statements_are_recognised() {
        assert!(is_explain("EXPLAIN SELECT 1"));
        assert!(is_explain("  /* hint */ explain format=json select 1"));
        assert!(is_explain("-- c\nDESC SELECT * FROM t"));
        assert!(is_explain("describe t"));
        assert!(!is_explain("SELECT 'EXPLAIN'"));
        assert!(!is_explain("DESCENDING"));
        assert!(!is_explain("/* unclosed"));
    }

    #[test]
    fn rows_follow_the_result_columns() {
        let scan = Scan {
            doc: json!({"index": "db.ix", "partitions": 2, "consistency": "latest",
                "scoring": "global", "copies": "ready on wkr-1", "hits": 3, "search_ms": 1.5,
                "query": {"Match": {"fields": ["name"], "text": "shoe", "all": false, "fuzzy": null}}}),
            table: "items".into(),
            index: "ix".into(),
            hits: Some(3),
        };
        let cols: Vec<String> = [
            "id",
            "select_type",
            "table",
            "type",
            "possible_keys",
            "key",
            "key_len",
            "ref",
            "rows",
            "Extra",
        ]
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
        let r = row_for(&cols, &scan);
        assert_eq!(r[1].as_deref(), Some("SEARCH"));
        assert_eq!(r[5].as_deref(), Some("ix"));
        assert_eq!(r[6], None);
        assert_eq!(r[8].as_deref(), Some("3"));
        let extra = r[9].as_deref().unwrap();
        assert!(extra.starts_with("SearchScan(index db.ix, 2 partitions, consistency latest"));
        assert!(extra.contains("3 hits in 1.5 ms"), "{extra}");
        let hidden = Scan {
            hits: None,
            doc: json!({"index": "db.ix"}),
            ..scan
        };
        assert_eq!(row_for(&cols, &hidden)[8], None);
        assert!(!extra_text(&hidden.doc).contains("hits"));
        assert!(json_row(&[hidden]).contains("sprig_search_scans"));
    }
}
