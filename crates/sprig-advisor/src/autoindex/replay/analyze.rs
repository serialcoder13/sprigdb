//! Parsing `ANALYZE FORMAT=JSON` and `EXPLAIN FORMAT=JSON` output (MariaDB
//! 11.8 and 12.3).
//!
//! The plan is a tree of query blocks (`nested_loop`, `filesort`,
//! `temporary_table`, `duplicates_removal`, `union_result`,
//! `materialized`, subqueries). Rather than follow every operator, the
//! parser walks the whole tree and takes every object with a `table_name`:
//! those are the table accesses, wherever they sit. Internal tables
//! (`<derived2>`, `<union1,2>`, `<subquery3>`) are kept but marked.
//!
//! Times: the statement's time is `query_optimization.r_total_time_ms`
//! plus `query_block.r_total_time_ms`. A top-level union has no total of
//! its own; then [`PlanRun::engine_ms`] is `None` and the caller uses the
//! wall-clock time it measured.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One table access in a plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableAccess {
    /// The table or alias as the plan names it.
    pub name: String,
    /// `ALL`, `index`, `range`, `ref`, `eq_ref`, `const`, `index_merge`, ...
    pub access_type: Option<String>,
    /// Indexes the access reads: `key`, and every `key` of an index merge.
    pub keys: Vec<String>,
    /// Rows read: `r_loops × r_rows` (`ANALYZE`); for a plan without
    /// `r_` fields, `loops × rows` (the estimate).
    pub rows_read: f64,
    /// The optimizer's estimate, `loops × rows`.
    pub rows_estimate: f64,
    /// Only the index was read (`using_index`), so no primary-key lookups.
    pub index_only: bool,
    /// An internal table (`<derived2>`, `<union1,2>`).
    pub internal: bool,
}

/// One parsed plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PlanRun {
    /// Optimization plus execution time reported by the server, in ms.
    pub engine_ms: Option<f64>,
    /// Every table access, in document order.
    pub tables: Vec<TableAccess>,
    /// A filesort ran.
    pub filesort: bool,
    /// The optimizer proved the result empty (`Impossible WHERE` and
    /// similar messages).
    pub message: Option<String>,
}

impl PlanRun {
    /// Whether any access reads index `name`.
    #[must_use]
    pub fn uses_index(&self, name: &str) -> bool {
        self.tables
            .iter()
            .any(|t| t.keys.iter().any(|k| k.eq_ignore_ascii_case(name)))
    }

    /// Rows read over all real tables.
    #[must_use]
    pub fn rows_read(&self) -> f64 {
        self.tables
            .iter()
            .filter(|t| !t.internal)
            .map(|t| t.rows_read)
            .sum()
    }

    /// The optimizer's row estimate over all real tables.
    #[must_use]
    pub fn rows_estimate(&self) -> f64 {
        self.tables
            .iter()
            .filter(|t| !t.internal)
            .map(|t| t.rows_estimate)
            .sum()
    }

    /// Rows read through `name` that needed a primary-key lookup: the
    /// cost the covering retry removes (worked example 3).
    #[must_use]
    pub fn lookups_via(&self, name: &str) -> f64 {
        self.tables
            .iter()
            .filter(|t| !t.index_only && t.keys.iter().any(|k| k.eq_ignore_ascii_case(name)))
            .map(|t| t.rows_read)
            .sum()
    }
}

/// Why a plan didn't parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanParseError(pub String);

impl std::fmt::Display for PlanParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unexpected plan JSON: {}", self.0)
    }
}

impl std::error::Error for PlanParseError {}

/// Parses one `ANALYZE FORMAT=JSON` or `EXPLAIN FORMAT=JSON` document.
///
/// # Errors
/// The text isn't JSON or has no `query_block`.
pub fn parse_plan(json: &str) -> Result<PlanRun, PlanParseError> {
    let v: Value = match serde_json::from_str(json) {
        Ok(v) => v,
        // MariaDB prints a condition's text unescaped, so a JSON path with
        // a quoted key (`'$."email"'`, the document engine's paths) breaks
        // its own JSON (S132): mend those lines and try once more.
        Err(e) => serde_json::from_str(&mend_quotes(json))
            .map_err(|_| PlanParseError(e.to_string()))?,
    };
    let qb = v
        .get("query_block")
        .ok_or_else(|| PlanParseError("no query_block".into()))?;
    let opt = v
        .get("query_optimization")
        .and_then(|o| o.get("r_total_time_ms"))
        .and_then(Value::as_f64);
    let exec = qb.get("r_total_time_ms").and_then(Value::as_f64);
    let engine_ms = match (opt, exec) {
        (o, Some(e)) => Some(o.unwrap_or(0.0) + e),
        // A plan the optimizer resolved alone (Impossible WHERE) ran no
        // execution step at all.
        (Some(o), None) if qb.get("union_result").is_none() => Some(o),
        _ => None,
    };
    let mut run = PlanRun {
        engine_ms,
        ..PlanRun::default()
    };
    walk(qb, &mut run);
    Ok(run)
}

/// Escapes the bare `"` inside one-line string values (`"key": "text",`)
/// of MariaDB's pretty-printed plan JSON.
fn mend_quotes(json: &str) -> String {
    let mut out = String::with_capacity(json.len() + 16);
    for line in json.lines() {
        let fixed = (|| {
            let (head, rest) = line.split_once("\": \"")?;
            let comma = rest.ends_with(',');
            let body = rest.strip_suffix(',').unwrap_or(rest).strip_suffix('"')?;
            if !body.contains('"') {
                return None;
            }
            let mut v = String::with_capacity(body.len() + 8);
            let mut prev = '\0';
            for ch in body.chars() {
                if ch == '"' && prev != '\\' {
                    v.push('\\');
                }
                v.push(ch);
                prev = ch;
            }
            Some(format!("{head}\": \"{v}\"{}", if comma { "," } else { "" }))
        })();
        out.push_str(fixed.as_deref().unwrap_or(line));
        out.push('\n');
    }
    out
}

fn walk(v: &Value, run: &mut PlanRun) {
    match v {
        Value::Object(m) => {
            if m.contains_key("filesort") {
                run.filesort = true;
            }
            if let Some(name) = m.get("table_name").and_then(Value::as_str) {
                run.tables.push(access(name, m));
            } else if let Some(msg) = m.get("message").and_then(Value::as_str)
                && run.message.is_none()
            {
                run.message = Some(msg.to_owned());
            }
            // An access can hold more accesses: a materialized derived
            // table, attached subqueries.
            for child in m.values() {
                walk(child, run);
            }
        }
        Value::Array(a) => {
            for child in a {
                walk(child, run);
            }
        }
        _ => {}
    }
}

fn num(m: &serde_json::Map<String, Value>, k: &str) -> Option<f64> {
    m.get(k).and_then(Value::as_f64)
}

fn access(name: &str, m: &serde_json::Map<String, Value>) -> TableAccess {
    let mut keys = Vec::new();
    if let Some(k) = m.get("key").and_then(Value::as_str) {
        keys.push(k.to_owned());
    }
    if let Some(im) = m.get("index_merge") {
        collect_keys(im, &mut keys);
    }
    let access_type = m
        .get("access_type")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let loops = num(m, "loops").unwrap_or(1.0);
    let rows = num(m, "rows").unwrap_or(0.0);
    let rows_estimate = loops * rows;
    let rows_read = match (num(m, "r_loops"), m.get("r_rows")) {
        (Some(l), Some(r)) if !r.is_null() => l * r.as_f64().unwrap_or(0.0),
        // A const or system table is read during optimization: r_loops 0
        // and r_rows null.
        (Some(_), Some(_)) => {
            if matches!(access_type.as_deref(), Some("const" | "system")) {
                rows
            } else {
                0.0
            }
        }
        // EXPLAIN: no r_ fields, the estimate stands in.
        _ => rows_estimate,
    };
    TableAccess {
        name: name.to_owned(),
        access_type,
        keys,
        rows_read,
        rows_estimate,
        index_only: m
            .get("using_index")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        internal: name.starts_with('<'),
    }
}

fn collect_keys(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::Object(m) => {
            for (k, child) in m {
                if k == "key" {
                    if let Some(s) = child.as_str()
                        && !out.iter().any(|o| o == s)
                    {
                        out.push(s.to_owned());
                    }
                } else {
                    collect_keys(child, out);
                }
            }
        }
        Value::Array(a) => a.iter().for_each(|c| collect_keys(c, out)),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::float_cmp)]

    use super::*;

    #[test]
    fn mends_bare_quotes_in_conditions() {
        // MariaDB 11.8's own output for a document-engine filter (S132).
        let json = r#"{
  "query_block": {
    "select_id": 1,
    "r_total_time_ms": 1.5,
    "nested_loop": [
      {
        "table": {
          "table_name": "orders",
          "access_type": "ALL",
          "r_rows": 60000,
          "attached_condition": "json_value(orders.doc,'$."email"') = 'user5@m6.io'"
        }
      }
    ]
  }
}"#;
        assert!(serde_json::from_str::<Value>(json).is_err());
        let run = parse_plan(json).unwrap();
        assert_eq!(run.engine_ms, Some(1.5));
    }

    /// Captured from MariaDB 11.8.9 on the Compose single node, on a
    /// 100,000-row table `t (id PK, a, b, c, d, pad)` and a 2,000-row
    /// `u (id PK, t_id, v)`, with `sprig_auto_0000000a (a)`,
    /// `sprig_auto_0000000b (b)` and `sprig_auto_0000000c (c)` where noted.
    fn fx(name: &str) -> PlanRun {
        let text = match name {
            "range_scan" => include_str!("fixtures/range_scan.json"),
            "range_index" => include_str!("fixtures/range_index.json"),
            "order_limit_scan" => include_str!("fixtures/order_limit_scan.json"),
            "order_limit_index" => include_str!("fixtures/order_limit_index.json"),
            "b_eq" => include_str!("fixtures/b_eq.json"),
            "join" => include_str!("fixtures/join.json"),
            "alias_join" => include_str!("fixtures/alias_join.json"),
            "group" => include_str!("fixtures/group.json"),
            "subq" => include_str!("fixtures/subq.json"),
            "union" => include_str!("fixtures/union.json"),
            "derived" => include_str!("fixtures/derived.json"),
            "empty" => include_str!("fixtures/empty.json"),
            "index_merge" => include_str!("fixtures/index_merge.json"),
            "impossible" => include_str!("fixtures/impossible.json"),
            "const_pk" => include_str!("fixtures/const_pk.json"),
            "explain_range" => include_str!("fixtures/explain_range.json"),
            "attached_subq" => include_str!("fixtures/attached_subq.json"),
            other => panic!("no fixture {other}"),
        };
        parse_plan(text).unwrap()
    }

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-6
    }

    #[test]
    fn full_scan_before_and_range_after() {
        let a = fx("range_scan");
        assert!(close(a.engine_ms.unwrap(), 0.038_839_976 + 10.549_246_06));
        assert_eq!(a.tables.len(), 1);
        assert_eq!(a.tables[0].access_type.as_deref(), Some("ALL"));
        assert!(a.tables[0].keys.is_empty());
        assert_eq!(a.rows_read(), 100_000.0);
        assert!(!a.uses_index("sprig_auto_0000000a"));

        let b = fx("range_index");
        assert!(b.uses_index("sprig_auto_0000000a"));
        assert!(b.uses_index("SPRIG_AUTO_0000000A"));
        assert_eq!(b.rows_read(), 101.0);
        assert!(b.engine_ms.unwrap() < 0.5);
        assert_eq!(b.lookups_via("sprig_auto_0000000a"), 101.0);
    }

    #[test]
    fn order_by_limit_trap_reads_the_index_through() {
        let a = fx("order_limit_scan");
        assert!(a.filesort);
        assert_eq!(a.rows_read(), 100_000.0);
        let b = fx("order_limit_index");
        assert!(!b.filesort);
        assert!(b.uses_index("sprig_auto_0000000c"));
        assert_eq!(b.tables[0].access_type.as_deref(), Some("index"));
        assert_eq!(b.rows_read(), 99_010.0);
        assert_eq!(b.lookups_via("sprig_auto_0000000c"), 99_010.0);
        assert!(b.engine_ms.unwrap() > a.engine_ms.unwrap());
    }

    #[test]
    fn unused_index_is_only_possible() {
        let p = fx("b_eq");
        assert!(!p.uses_index("sprig_auto_0000000b"));
        assert_eq!(p.tables[0].access_type.as_deref(), Some("ALL"));
    }

    #[test]
    fn joins_count_loops() {
        let p = fx("join");
        assert_eq!(p.tables.len(), 2);
        assert_eq!(p.tables[0].name, "u");
        assert_eq!(p.tables[1].keys, vec!["PRIMARY".to_owned()]);
        // 2,000 from u, 286 eq_ref lookups of one row each into t.
        assert_eq!(p.rows_read(), 2_286.0);
        let p = fx("alias_join");
        assert_eq!(p.tables[1].name, "o");
    }

    #[test]
    fn nested_operators_are_found() {
        let g = fx("group");
        assert!(g.filesort);
        assert_eq!(g.tables.len(), 1);
        assert_eq!(g.rows_read(), 100_000.0);
        let s = fx("subq");
        assert_eq!(s.tables.len(), 2);
        assert!(s.uses_index("sprig_auto_0000000a"));
        let d = fx("derived");
        assert_eq!(d.tables.len(), 2);
        assert!(d.tables[0].internal);
        assert!(d.uses_index("sprig_auto_0000000b"));
        assert!(d.tables[1].index_only);
        assert_eq!(d.lookups_via("sprig_auto_0000000b"), 0.0);
        assert_eq!(d.rows_read(), 100_000.0);
    }

    #[test]
    fn attached_subqueries_count_their_loops() {
        let p = fx("attached_subq");
        assert_eq!(p.tables.len(), 2);
        assert!(p.uses_index("sprig_auto_0000000c"));
        // 99 range rows from t; 99 executions of a 2,000-row scan of u.
        assert_eq!(p.rows_read(), 99.0 + 99.0 * 2_000.0);
    }

    #[test]
    fn union_has_no_top_level_time() {
        let u = fx("union");
        assert_eq!(u.engine_ms, None);
        assert_eq!(u.tables.len(), 3);
        assert!(u.tables[0].internal);
        assert!(u.uses_index("sprig_auto_0000000a"));
        assert!(u.uses_index("sprig_auto_0000000c"));
    }

    #[test]
    fn index_merge_lists_every_key() {
        let m = fx("index_merge");
        assert_eq!(
            m.tables[0].keys,
            vec![
                "sprig_auto_0000000a".to_owned(),
                "sprig_auto_0000000c".to_owned()
            ]
        );
        assert!(m.uses_index("sprig_auto_0000000c"));
    }

    #[test]
    fn trivial_plans() {
        let i = fx("impossible");
        assert!(i.tables.is_empty());
        assert_eq!(i.message.as_deref(), Some("Impossible WHERE"));
        assert!(close(i.engine_ms.unwrap(), 0.030_454_546));
        let c = fx("const_pk");
        assert_eq!(c.rows_read(), 1.0);
        let e = fx("empty");
        assert_eq!(e.rows_read(), 0.0);
        assert!(e.tables[0].index_only);
    }

    #[test]
    fn explain_without_r_fields_uses_estimates() {
        let e = fx("explain_range");
        assert_eq!(e.engine_ms, None);
        assert_eq!(e.tables[0].access_type.as_deref(), Some("ALL"));
        assert!(e.rows_estimate() > 90_000.0);
        assert_eq!(e.rows_read(), e.rows_estimate());
    }

    #[test]
    fn garbage_is_an_error() {
        assert!(parse_plan("not json").is_err());
        assert!(parse_plan("{}").is_err());
    }
}
