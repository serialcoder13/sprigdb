//! `EXPLAIN`'s shape on port 3306 (chapter 28, "Output on port 3306";
//! build step 11; R4 round 3).
//!
//! MySQL 8.4's traditional `EXPLAIN` has twelve columns: `id`,
//! `select_type`, `table`, `partitions`, `type`, `possible_keys`, `key`,
//! `key_len`, `ref`, `rows`, `filtered`, `Extra`. MariaDB's has ten (no
//! `partitions`, no `filtered`); its `EXPLAIN EXTENDED` adds `filtered`.
//! So on 3306 a traditional `EXPLAIN` of a statement runs as `EXPLAIN
//! EXTENDED` ([`rewrite`]), and the gateway's output sink inserts
//! `partitions` (always `NULL`: SprigDB's tables aren't partitioned the
//! MySQL way) and types `rows` as MySQL does ([`EXPLAIN_EXTENDED_COLUMNS`]).
//! The plans themselves are MariaDB's optimizer's (chapter 28 non-goal).
//!
//! `EXPLAIN FORMAT=JSON` passes through: MariaDB's JSON has its own
//! structure (`query_block` with MariaDB's members). `EXPLAIN tbl`
//! (`DESCRIBE`) is [`super::schema::describe_target`]'s.
//!
//! **Tree formats (R4 round 4).** `EXPLAIN FORMAT=TREE` runs as MariaDB's
//! `EXPLAIN FORMAT=JSON`, and `EXPLAIN ANALYZE` (with or without
//! `FORMAT=TREE`) as `ANALYZE FORMAT=JSON`, which runs the statement as
//! MySQL's does. The output sink renders the JSON plan as MySQL's tree
//! ([`tree`]): one `EXPLAIN` column, one line per iterator (`-> Table scan
//! on t`, `-> Filter: (cond)`, `-> Nested loop inner join`, `-> Sort: ..`,
//! `-> Limit: n row(s)`, ...), indented four spaces per level. The plan is
//! MariaDB's, and so are the numbers: `cost` is MariaDB's cost unit (not
//! MySQL's), `rows` its estimate, and `actual time` the total of MariaDB's
//! `r_*_time_ms` per loop. MariaDB's JSON names no aggregation, so MySQL's
//! `Group aggregate` lines don't appear.

use crate::lex::{self, LexOptions, TokenKind};

/// The column names of MariaDB's `EXPLAIN EXTENDED`, in order.
pub const EXPLAIN_EXTENDED_COLUMNS: [&str; 11] = [
    "id",
    "select_type",
    "table",
    "type",
    "possible_keys",
    "key",
    "key_len",
    "ref",
    "rows",
    "filtered",
    "Extra",
];

/// Where MySQL's `partitions` column goes.
pub const PARTITIONS_AT: usize = 3;

/// Statements `EXPLAIN` describes.
const EXPLAINABLE: &[&[u8]] = &[
    b"SELECT", b"WITH", b"INSERT", b"UPDATE", b"DELETE", b"REPLACE", b"TABLE", b"VALUES",
];

/// `EXPLAIN [FORMAT = TRADITIONAL] <statement>` as `EXPLAIN EXTENDED
/// <statement>`; `None` for anything else.
#[must_use]
pub fn rewrite(sql: &[u8], opts: &LexOptions) -> Option<Vec<u8>> {
    let head = sql.trim_ascii_start();
    if head.len() < 8 || !head[..7].eq_ignore_ascii_case(b"EXPLAIN") {
        return None;
    }
    let tokens = lex::lex(sql, opts).ok()?;
    let t: Vec<_> = tokens.into_iter().filter(|t| !t.kind.is_trivia()).collect();
    let word = |j: usize, w: &[u8]| {
        t.get(j)
            .is_some_and(|tk| tk.kind == TokenKind::Word && tk.bytes(sql).eq_ignore_ascii_case(w))
    };
    if !word(0, b"EXPLAIN") {
        return None;
    }
    // `EXPLAIN ANALYZE [FORMAT = TREE] stmt`: MariaDB's `ANALYZE
    // FORMAT=JSON stmt`, rendered as a tree by the sink.
    if word(1, b"ANALYZE") {
        let mut stmt = 2;
        if word(2, b"FORMAT") {
            let v = if t.get(3).is_some_and(|tk| tk.bytes(sql) == b"=") {
                4
            } else {
                3
            };
            if !word(v, b"TREE") {
                return None;
            }
            stmt = v + 1;
        }
        let at = t.get(stmt)?.start as usize;
        let mut out = b"ANALYZE FORMAT=JSON ".to_vec();
        out.extend_from_slice(&sql[at..]);
        return Some(out);
    }
    // `FORMAT = TRADITIONAL` is the default: it goes. `FORMAT = TREE` asks
    // MariaDB for JSON.
    let (from, stmt) = if word(1, b"FORMAT") {
        let v = if t.get(2).is_some_and(|tk| tk.bytes(sql) == b"=") {
            3
        } else {
            2
        };
        if word(v, b"TREE") {
            let at = t.get(v + 1)?.start as usize;
            let mut out = sql[..t[0].end as usize].to_vec();
            out.extend_from_slice(b" FORMAT=JSON ");
            out.extend_from_slice(&sql[at..]);
            return Some(out);
        }
        if !word(v, b"TRADITIONAL") {
            return None;
        }
        (t[1].start as usize, v + 1)
    } else {
        (t[0].end as usize, 1)
    };
    let explainable = EXPLAINABLE.iter().any(|w| word(stmt, w))
        || t.get(stmt)
            .is_some_and(|tk| tk.kind == TokenKind::Punct && tk.bytes(sql) == b"(");
    if !explainable {
        return None;
    }
    let to = t[stmt].start as usize;
    let mut out = Vec::with_capacity(sql.len() + 10);
    out.extend_from_slice(&sql[..t[0].end as usize]);
    out.extend_from_slice(b" EXTENDED ");
    out.extend_from_slice(&sql[to..]);
    let _ = from;
    Some(out)
}

/// Whether a resultset's column names are MariaDB's `EXPLAIN EXTENDED`.
#[must_use]
pub fn is_explain_extended<'a>(names: impl IntoIterator<Item = &'a [u8]>) -> bool {
    let mut n = 0;
    for (i, name) in names.into_iter().enumerate() {
        if EXPLAIN_EXTENDED_COLUMNS
            .get(i)
            .is_none_or(|c| c.as_bytes() != name)
        {
            return false;
        }
        n += 1;
    }
    n == EXPLAIN_EXTENDED_COLUMNS.len()
}

/// Which tree a client's `EXPLAIN` asked for: `Some(false)` for `EXPLAIN
/// FORMAT=TREE`, `Some(true)` for `EXPLAIN ANALYZE`; `None` otherwise.
#[must_use]
pub fn tree_request(sql: &[u8], opts: &LexOptions) -> Option<bool> {
    let head = sql.trim_ascii_start();
    if head.len() < 8 || !head[..7].eq_ignore_ascii_case(b"EXPLAIN") {
        return None;
    }
    let tokens = lex::lex(sql, opts).ok()?;
    let t: Vec<_> = tokens.into_iter().filter(|t| !t.kind.is_trivia()).collect();
    let word = |j: usize, w: &[u8]| {
        t.get(j)
            .is_some_and(|tk| tk.kind == TokenKind::Word && tk.bytes(sql).eq_ignore_ascii_case(w))
    };
    if word(1, b"ANALYZE") {
        return Some(true);
    }
    let v = if t.get(2).is_some_and(|tk| tk.bytes(sql) == b"=") {
        3
    } else {
        2
    };
    (word(1, b"FORMAT") && word(v, b"TREE")).then_some(false)
}

/// One iterator of the tree.
struct Node {
    label: String,
    /// `(cost, rows)`.
    est: Option<(f64, f64)>,
    /// `(time per loop in ms, rows, loops)` for `EXPLAIN ANALYZE`.
    actual: Option<(f64, f64, f64)>,
    children: Vec<Node>,
}

impl Node {
    fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            est: None,
            actual: None,
            children: Vec::new(),
        }
    }

    fn with(mut self, child: Node) -> Self {
        self.children.push(child);
        self
    }

    fn render(&self, depth: usize, out: &mut String) {
        use std::fmt::Write as _;
        let _ = write!(out, "{}-> {}", " ".repeat(depth * 4), self.label);
        if let Some((c, r)) = self.est {
            let _ = write!(out, "  (cost={} rows={})", num(c), num(r));
        }
        if let Some((t, r, l)) = self.actual {
            let _ = write!(
                out,
                " (actual time={}..{} rows={} loops={})",
                num(t),
                num(t),
                num(r),
                num(l)
            );
        }
        out.push('\n');
        for c in &self.children {
            c.render(depth + 1, out);
        }
    }
}

/// A number as MySQL's tree prints it: three significant digits, no
/// trailing zeros.
fn num(v: f64) -> String {
    if v == 0.0 || !v.is_finite() {
        return "0".to_owned();
    }
    if v.abs() >= 1000.0 {
        return format!("{v:.0}");
    }
    let mag = v.abs().log10().floor();
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let places = (2.0 - mag).clamp(0.0, 12.0) as usize;
    let s = format!("{v:.places$}");
    if s.contains('.') {
        s.trim_end_matches('0').trim_end_matches('.').to_owned()
    } else {
        s
    }
}

/// `db.t.c` as `t.c`; a condition's text unchanged otherwise.
fn short_ref(r: &str) -> &str {
    let parts: Vec<&str> = r.rsplitn(3, '.').collect();
    if parts.len() == 3 {
        let at = r.len() - parts[0].len() - parts[1].len() - 1;
        &r[at..]
    } else {
        r
    }
}

fn f(v: &serde_json::Value, k: &str) -> Option<f64> {
    v.get(k).and_then(serde_json::Value::as_f64)
}

fn st<'a>(v: &'a serde_json::Value, k: &str) -> Option<&'a str> {
    v.get(k).and_then(serde_json::Value::as_str)
}

/// MariaDB's `trigcond(x)` and back-quoted names, as MySQL prints a
/// condition.
fn cond(c: &str) -> String {
    let mut c = c.replace('`', "");
    while let Some(i) = c.find("trigcond(") {
        let rest = &c[i + "trigcond(".len()..];
        let mut depth = 1;
        let mut end = None;
        for (j, ch) in rest.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = Some(j);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(e) = end else { break };
        c = format!("{}{}{}", &c[..i], &rest[..e], &rest[e + 1..]);
    }
    format!("({c})")
}

/// One table access, with its filter on top.
fn table_node(t: &serde_json::Value, analyze: bool) -> Node {
    let name = st(t, "table_name").unwrap_or("?").to_owned();
    let key = st(t, "key").unwrap_or("");
    let covering = t.get("using_index").and_then(serde_json::Value::as_bool) == Some(true);
    let cov = if covering { "Covering index" } else { "Index" };
    let lookup = || {
        let parts: Vec<&str> = t
            .get("used_key_parts")
            .and_then(serde_json::Value::as_array)
            .map(|a| a.iter().filter_map(serde_json::Value::as_str).collect())
            .unwrap_or_default();
        let refs: Vec<&str> = t
            .get("ref")
            .and_then(serde_json::Value::as_array)
            .map(|a| a.iter().filter_map(serde_json::Value::as_str).collect())
            .unwrap_or_default();
        let pairs: Vec<String> = parts
            .iter()
            .zip(&refs)
            .map(|(p, r)| format!("{p}={}", short_ref(r)))
            .collect();
        if pairs.is_empty() {
            String::new()
        } else {
            format!(" ({})", pairs.join(", "))
        }
    };
    let label = match st(t, "access_type").unwrap_or("ALL") {
        "ALL" => format!("Table scan on {name}"),
        "index" => format!("{cov} scan on {name} using {key}"),
        "range" => format!("{cov} range scan on {name} using {key}"),
        "ref" | "ref_or_null" => format!("{cov} lookup on {name} using {key}{}", lookup()),
        "eq_ref" => format!(
            "Single-row {} lookup on {name} using {key}{}",
            cov.to_lowercase(),
            lookup()
        ),
        "const" | "system" => format!("Constant row from {name}"),
        "fulltext" => format!("Full-text index search on {name} using {key}"),
        other => format!("Index scan on {name} using {key} ({other})"),
    };
    let label = match st(t, "index_condition") {
        Some(ic) => format!("{label}, with index condition: {}", cond(ic)),
        None => label,
    };
    let rows = f(t, "rows").unwrap_or(1.0);
    let cost = f(t, "cost").unwrap_or(0.0);
    let mut node = Node::new(label);
    node.est = Some((cost, rows));
    if analyze {
        node.actual = actual(t);
    }
    if let Some(m) = t.get("materialized").and_then(|m| m.get("query_block")) {
        node = node.with(Node::new("Materialize").with(block(m, analyze)));
    }
    match st(t, "attached_condition") {
        Some(c) => {
            let filtered = f(t, "filtered").unwrap_or(100.0);
            let mut fnode = Node::new(format!("Filter: {}", cond(c)));
            fnode.est = Some((cost, rows * filtered / 100.0));
            if analyze {
                fnode.actual = actual(t)
                    .map(|(tm, r, l)| (tm, r * f(t, "r_filtered").unwrap_or(100.0) / 100.0, l));
            }
            fnode.with(node)
        }
        None => node,
    }
}

/// `EXPLAIN ANALYZE`'s numbers of a table: time per loop, rows, loops.
fn actual(t: &serde_json::Value) -> Option<(f64, f64, f64)> {
    let loops = f(t, "r_loops")?;
    let rows = f(t, "r_rows").unwrap_or(0.0);
    let time = f(t, "r_table_time_ms").unwrap_or(0.0) + f(t, "r_other_time_ms").unwrap_or(0.0);
    Some((if loops > 0.0 { time / loops } else { time }, rows, loops))
}

/// One element of a `nested_loop`.
fn access(item: &serde_json::Value, analyze: bool) -> Option<(Node, bool, bool)> {
    // (node, a BNL join, the table's condition is a join's trigcond)
    if let Some(t) = item.get("table") {
        let outer = st(t, "attached_condition").is_some_and(|c| c.contains("trigcond("));
        return Some((table_node(t, analyze), false, outer));
    }
    if let Some(b) = item.get("block-nl-join") {
        let t = b.get("table")?;
        let mut n = table_node(t, analyze);
        if let Some(c) = st(b, "attached_condition") {
            let outer = c.contains("trigcond(");
            n = Node {
                label: format!("{}{}", if outer { "Left " } else { "Inner " }, cond(c)),
                ..Node::new("")
            }
            .with(Node::new("Hash").with(n));
            return Some((n, true, outer));
        }
        return Some((Node::new("Hash").with(n), true, false));
    }
    if let Some(r) = item.get("read_sorted_file") {
        return access(r, analyze);
    }
    if let Some(fs) = item.get("filesort") {
        let key = st(fs, "sort_key").unwrap_or("").replace(" desc", " DESC");
        let inner = sorted_input(fs, analyze)?;
        return Some((Node::new(format!("Sort: {key}")).with(inner), false, false));
    }
    if let Some(tt) = item.get("temporary_table") {
        let inner = sorted_input(tt, analyze)?;
        return Some((
            Node::new("Table scan on <temporary>").with(Node::new("Temporary table").with(inner)),
            false,
            false,
        ));
    }
    None
}

/// What a `filesort` or `temporary_table` reads.
fn sorted_input(v: &serde_json::Value, analyze: bool) -> Option<Node> {
    if let Some(t) = v.get("table") {
        return Some(table_node(t, analyze));
    }
    if v.get("nested_loop").is_some() {
        return Some(nested(v, analyze));
    }
    for k in ["temporary_table", "filesort", "read_sorted_file"] {
        if let Some(x) = v.get(k) {
            let wrapped = serde_json::json!({ k: x });
            return access(&wrapped, analyze).map(|(n, _, _)| n);
        }
    }
    None
}

/// A `nested_loop`: the tables joined left to right.
fn nested(v: &serde_json::Value, analyze: bool) -> Node {
    let items = v
        .get("nested_loop")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let all_const = !items.is_empty()
        && items.iter().all(|i| {
            i.get("table")
                .and_then(|t| st(t, "access_type"))
                .is_some_and(|a| a == "const" || a == "system")
        });
    if all_const {
        let mut n = Node::new("Rows fetched before execution");
        n.est = Some((0.0, 1.0));
        return n;
    }
    let mut acc: Option<Node> = None;
    for item in &items {
        let Some((n, hash, outer)) = access(item, analyze) else {
            continue;
        };
        acc = Some(match acc {
            None => n,
            Some(left) if hash => {
                // The BNL node carries the condition in its label.
                let (label, rest) = match n.label.split_once(' ') {
                    Some((side, c))
                        if n.label.starts_with("Left ") || n.label.starts_with("Inner ") =>
                    {
                        (format!("{side} hash join {c}"), n.children)
                    }
                    _ => ("Inner hash join".to_owned(), vec![n]),
                };
                let mut j = Node::new(label);
                j.children.push(left);
                j.children.extend(rest);
                j
            }
            Some(left) => {
                let semi = item
                    .get("table")
                    .is_some_and(|t| t.get("first_match").is_some());
                let kind = if semi {
                    "Nested loop semijoin"
                } else if outer {
                    "Nested loop left join"
                } else {
                    "Nested loop inner join"
                };
                Node::new(kind).with(left).with(n)
            }
        });
    }
    acc.unwrap_or_else(|| Node::new("Zero rows (no matching row in const table)"))
}

/// A `query_block`.
fn block(qb: &serde_json::Value, analyze: bool) -> Node {
    let mut node = if let Some(u) = qb.get("union_result") {
        let specs = u
            .get("query_specifications")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        let dedup = u.get("table_name").is_some();
        let mut m = Node::new(if dedup {
            "Union materialize with deduplication"
        } else {
            "Append"
        });
        for s in &specs {
            if let Some(q) = s.get("query_block") {
                m.children.push(block(q, analyze));
            }
        }
        if dedup {
            Node::new("Table scan on <union temporary>").with(m)
        } else {
            m
        }
    } else if qb.get("nested_loop").is_some() {
        nested(qb, analyze)
    } else if let Some(t) = qb.get("table") {
        table_node(t, analyze)
    } else if let Some((n, _, _)) = ["filesort", "temporary_table", "read_sorted_file"]
        .iter()
        .find_map(|k| qb.get(*k).map(|x| serde_json::json!({ *k: x })))
        .and_then(|w| access(&w, analyze))
    {
        n
    } else {
        Node::new(
            st(qb, "message")
                .unwrap_or("Rows fetched before execution")
                .to_owned(),
        )
    };
    if let Some(h) = st(qb, "having_condition") {
        node = Node::new(format!("Filter: {}", cond(h))).with(node);
    }
    if let Some(subs) = qb.get("subqueries").and_then(serde_json::Value::as_array) {
        for s in subs {
            if let Some(q) = s.get("query_block") {
                let id = q
                    .get("select_id")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                node.children.push(
                    Node::new(format!("Select #{id} (subquery in condition; dependent)"))
                        .with(block(q, analyze)),
                );
            }
        }
    }
    node
}

/// MariaDB's JSON plan as MySQL's tree, with `Limit: n row(s)` on top
/// when the statement `sql` (the client's text) ends with a literal
/// `LIMIT`; `None` when the text isn't a plan.
#[must_use]
pub fn tree(json: &[u8], analyze: bool, limit: Option<u64>) -> Option<String> {
    let v: serde_json::Value = serde_json::from_slice(json).ok()?;
    let qb = v.get("query_block")?;
    let mut root = block(qb, analyze);
    if let Some(n) = limit {
        root = Node::new(format!("Limit: {n} row(s)")).with(root);
    }
    let mut out = String::new();
    root.render(0, &mut out);
    Some(out)
}

/// The literal row count of the outermost `LIMIT` of `sql`, an `EXPLAIN`.
#[must_use]
pub fn outer_limit(sql: &[u8], opts: &LexOptions) -> Option<u64> {
    let tokens = lex::lex(sql, opts).ok()?;
    let t: Vec<_> = tokens.into_iter().filter(|t| !t.kind.is_trivia()).collect();
    let mut depth = 0i32;
    let mut found = None;
    for (j, tk) in t.iter().enumerate() {
        match tk.kind {
            TokenKind::Punct if tk.bytes(sql) == b"(" => depth += 1,
            TokenKind::Punct if tk.bytes(sql) == b")" => depth -= 1,
            TokenKind::Word if depth == 0 && tk.bytes(sql).eq_ignore_ascii_case(b"LIMIT") => {
                let n = t.get(j + 1)?;
                // `LIMIT off, n` and `LIMIT n OFFSET off`.
                let count = if t.get(j + 2).is_some_and(|c| c.bytes(sql) == b",") {
                    t.get(j + 3)?
                } else {
                    n
                };
                found = std::str::from_utf8(count.bytes(sql)).ok()?.parse().ok();
            }
            _ => {}
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rw(s: &str) -> Option<String> {
        rewrite(s.as_bytes(), &LexOptions::mysql84()).map(|v| String::from_utf8(v).unwrap())
    }

    #[test]
    fn traditional_explains_ask_for_filtered() {
        assert_eq!(
            rw("EXPLAIN SELECT * FROM t").unwrap(),
            "EXPLAIN EXTENDED SELECT * FROM t"
        );
        assert_eq!(
            rw("explain format = traditional update t set a = 1").unwrap(),
            "explain EXTENDED update t set a = 1"
        );
        assert_eq!(
            rw("EXPLAIN (SELECT 1) UNION (SELECT 2)").unwrap(),
            "EXPLAIN EXTENDED (SELECT 1) UNION (SELECT 2)"
        );
        assert_eq!(rw("EXPLAIN FORMAT=JSON SELECT 1"), None);
        assert_eq!(rw("EXPLAIN t"), None);
        assert_eq!(rw("EXPLAIN EXTENDED SELECT 1"), None);
        assert_eq!(rw("SELECT 'EXPLAIN SELECT'"), None);
    }

    #[test]
    fn tree_formats_ask_for_json() {
        assert_eq!(
            rw("EXPLAIN FORMAT=TREE SELECT * FROM t").unwrap(),
            "EXPLAIN FORMAT=JSON SELECT * FROM t"
        );
        assert_eq!(
            rw("explain analyze select 1").unwrap(),
            "ANALYZE FORMAT=JSON select 1"
        );
        assert_eq!(
            rw("EXPLAIN ANALYZE FORMAT = TREE SELECT 1").unwrap(),
            "ANALYZE FORMAT=JSON SELECT 1"
        );
        let o = LexOptions::mysql84();
        assert_eq!(
            tree_request(b"EXPLAIN FORMAT = tree SELECT 1", &o),
            Some(false)
        );
        assert_eq!(tree_request(b"EXPLAIN ANALYZE SELECT 1", &o), Some(true));
        assert_eq!(tree_request(b"EXPLAIN SELECT 1", &o), None);
        assert_eq!(outer_limit(b"EXPLAIN FORMAT=TREE SELECT a FROM t WHERE b IN (SELECT c FROM u LIMIT 9) LIMIT 5, 2", &o), Some(2));
        assert_eq!(outer_limit(b"EXPLAIN SELECT 1", &o), None);
    }

    #[test]
    fn trees_from_mariadb_plans() {
        // Captured from MariaDB 11.8.9 (R4 round 4).
        let scan = br#"{"query_block":{"select_id":1,"cost":0.00282078,"nested_loop":[{"table":{"table_name":"c","access_type":"range","possible_keys":["PRIMARY"],"key":"PRIMARY","key_length":"4","used_key_parts":["id"],"loops":1,"rows":2,"cost":0.00282078,"filtered":100,"attached_condition":"c.`id` > 1"}}]}}"#;
        assert_eq!(
            tree(scan, false, None).unwrap(),
            "-> Filter: (c.id > 1)  (cost=0.00282 rows=2)\n    -> Index range scan on c using PRIMARY  (cost=0.00282 rows=2)\n"
        );
        let join = br#"{"query_block":{"select_id":1,"cost":0.01801864,"nested_loop":[{"read_sorted_file":{"filesort":{"sort_key":"o.amt desc","table":{"table_name":"o","access_type":"ALL","possible_keys":["kc"],"loops":1,"rows":6,"cost":0.0118328,"filtered":100,"attached_condition":"o.amt > 6 and o.cid is not null"}}}},{"table":{"table_name":"c","access_type":"eq_ref","possible_keys":["PRIMARY"],"key":"PRIMARY","key_length":"4","used_key_parts":["id"],"ref":["acme.o.cid"],"loops":6,"rows":1,"cost":0.00618584,"filtered":100}}]}}"#;
        assert_eq!(
            tree(join, false, Some(2)).unwrap(),
            "-> Limit: 2 row(s)\n    -> Nested loop inner join\n        -> Sort: o.amt DESC\n            -> Filter: (o.amt > 6 and o.cid is not null)  (cost=0.0118 rows=6)\n                -> Table scan on o  (cost=0.0118 rows=6)\n        -> Single-row index lookup on c using PRIMARY (id=o.cid)  (cost=0.00619 rows=1)\n"
        );
        let bnl = br#"{"query_block":{"select_id":1,"nested_loop":[{"table":{"table_name":"c","access_type":"index","key":"kn","loops":1,"rows":3,"cost":0.007,"filtered":100,"using_index":true}},{"block-nl-join":{"table":{"table_name":"o","access_type":"ALL","loops":3,"rows":6,"cost":0.03386,"filtered":100},"buffer_type":"flat","join_type":"BNL","attached_condition":"trigcond(o.amt = c.`id`)"}}]}}"#;
        assert_eq!(
            tree(bnl, false, None).unwrap(),
            "-> Left hash join (o.amt = c.id)\n    -> Covering index scan on c using kn  (cost=0.007 rows=3)\n    -> Hash\n        -> Table scan on o  (cost=0.0339 rows=6)\n"
        );
        let konst = br#"{"query_block":{"select_id":1,"nested_loop":[{"table":{"table_name":"c","access_type":"const","rows":1,"filtered":100}}]}}"#;
        assert_eq!(
            tree(konst, false, None).unwrap(),
            "-> Rows fetched before execution  (cost=0 rows=1)\n"
        );
        let analyzed = br#"{"query_optimization":{"r_total_time_ms":0.05},"query_block":{"select_id":1,"cost":0.00282078,"r_loops":1,"nested_loop":[{"table":{"table_name":"c","access_type":"ALL","loops":1,"r_loops":1,"rows":3,"r_rows":3,"cost":0.0028,"r_table_time_ms":0.008,"r_other_time_ms":0.0047,"filtered":100,"r_filtered":66.67,"attached_condition":"c.id > 1"}}]}}"#;
        assert_eq!(
            tree(analyzed, true, None).unwrap(),
            "-> Filter: (c.id > 1)  (cost=0.0028 rows=3) (actual time=0.0127..0.0127 rows=2 loops=1)\n    -> Table scan on c  (cost=0.0028 rows=3) (actual time=0.0127..0.0127 rows=3 loops=1)\n"
        );
        assert!(tree(b"not json", false, None).is_none());
    }

    #[test]
    fn explain_columns_are_recognised() {
        let names: Vec<&[u8]> = EXPLAIN_EXTENDED_COLUMNS
            .iter()
            .map(|s| s.as_bytes())
            .collect();
        assert!(is_explain_extended(names.clone()));
        assert!(!is_explain_extended(names[..10].to_vec()));
    }
}
