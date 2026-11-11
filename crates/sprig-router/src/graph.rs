//! `GRAPH_TABLE` in SQL as a router plan (chapter 17's `Plan::Graph`,
//! chapter 43's SQL/PGQ; R8 round 6, decision G52).
//!
//! ```text
//! before the pipeline (gateway):
//!   find(sql)                       → each GRAPH_TABLE (g MATCH … COLUMNS (…)): span, graph, Cypher, columns
//!   with_placeholders(sql, nonce)   → each one replaced by a placeholder derived table:
//!                                     (SELECT NULL AS `c1`, … FROM DUAL WHERE sprig_graph_table_pending(nonce, i))
//!   the statement runs through chapter 07's rewrite and the other steps; the
//!   graph text never does (it isn't SQL), nor do the rows (they may be 16 MB)
//! in the router path (gateway, after the pipeline):
//!   planner::plan_graph(sql, …)     → Plan::Graph(GraphPlan) (or Explain of it): the calls, the
//!                                     outer statement with its placeholders, each call's graph
//!                                     shards (from the topology's group) and tiers, the outer
//!                                     statement planned with empty rows, and the joins of a graph
//!                                     column with a sharded table's key
//!   the gateway runs each call as a Cypher read, then
//!   GraphPlan::bind(rows)           → the outer statement with each placeholder replaced by its
//!                                     rows (SELECT … UNION ALL VALUES (…), …)
//!   planner::plan(bound)            → the router's own plan: a derived table of literal rows is
//!                                     the same on every shard, so it joins inside each shard
//!                                     like a reference table (Pass, Scatter, Merge, Aggregate)
//!   GraphPlan::narrow(plan, rows)   → a read keeps only the shards holding the graph rows' keys
//! ```
//!
//! A placeholder that reaches MariaDB unresolved fails (the function doesn't
//! exist), so a missed path can't answer with no rows.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::sync::Arc;

use sprig_common::{Result, SprigCode, SprigError};

use crate::plan::Plan;
use crate::topology::{Group, KeyClass, KeyValue, ShardLoc};

/// The function a placeholder calls: it doesn't exist, so an unresolved
/// placeholder fails rather than reading as an empty table.
pub const PENDING_FN: &str = "sprig_graph_table_pending";

/// One `GRAPH_TABLE (...)` in a statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphCall {
    /// Byte range of `GRAPH_TABLE ( ... )` in the statement it was found in.
    pub span: (usize, usize),
    /// The database, if the graph name is qualified.
    pub db: Option<String>,
    /// The graph.
    pub graph: String,
    /// The Cypher read it runs.
    pub cypher: String,
    /// The output column names.
    pub columns: Vec<String>,
}

fn syntax(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::GraphSyntax, m.into())
}

/// The index after the `)` matching the `(` at `open`, skipping quoted
/// text.
fn matching(b: &[u8], open: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut i = open;
    while i < b.len() {
        match b[i] {
            q @ (b'\'' | b'"' | b'`') => {
                i += 1;
                while i < b.len() {
                    if b[i] == b'\\' && q != b'`' {
                        i += 2;
                        continue;
                    }
                    if b[i] == q {
                        if b.get(i + 1) == Some(&q) {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'(' => depth += 1,
            b')' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(i + 1);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// The position of keyword `kw` at paren depth 0 of `s` (outside quotes),
/// as a whole word.
fn keyword(s: &str, kw: &str) -> Option<usize> {
    let b = s.as_bytes();
    let k = kw.as_bytes();
    let mut depth = 0i32;
    let mut i = 0;
    let mut quote: Option<u8> = None;
    while i < b.len() {
        let c = b[i];
        if let Some(q) = quote {
            if c == b'\\' && q != b'`' {
                i += 2;
                continue;
            }
            if c == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        match c {
            b'\'' | b'"' | b'`' => quote = Some(c),
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            _ if depth == 0
                && i + k.len() <= b.len()
                && b[i..i + k.len()].eq_ignore_ascii_case(k)
                && (i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_'))
                && b.get(i + k.len())
                    .is_none_or(|x| !(x.is_ascii_alphanumeric() || *x == b'_')) =>
            {
                return Some(i);
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// A pattern in SQL/PGQ form as Cypher: `(c IS Person)` → `(c:Person)`,
/// `[o IS BOUGHT]` → `[o:BOUGHT]`.
#[must_use]
pub fn pattern_to_cypher(p: &str) -> String {
    let mut out = String::with_capacity(p.len());
    let b = p.as_bytes();
    let mut i = 0;
    let mut quote: Option<u8> = None;
    while i < b.len() {
        let c = b[i];
        if let Some(q) = quote {
            let ch = p[i..].chars().next().unwrap_or(' ');
            out.push(ch);
            if c == q {
                quote = None;
            }
            i += ch.len_utf8();
            continue;
        }
        if matches!(c, b'\'' | b'"' | b'`') {
            quote = Some(c);
            out.push(c as char);
            i += 1;
            continue;
        }
        let word_start = i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_');
        if word_start
            && i + 2 <= b.len()
            && b[i..i + 2].eq_ignore_ascii_case(b"IS")
            && b.get(i + 2).is_some_and(u8::is_ascii_whitespace)
        {
            while out.ends_with(' ') {
                out.pop();
            }
            out.push(':');
            i += 2;
            while i < b.len() && b[i].is_ascii_whitespace() {
                i += 1;
            }
            continue;
        }
        // Copy one UTF-8 character.
        let ch = p[i..].chars().next().unwrap_or(' ');
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// Splits `s` at commas of paren depth 0.
fn split_top(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    for c in s.chars() {
        if let Some(q) = quote {
            cur.push(c);
            if c == q {
                quote = None;
            }
            continue;
        }
        match c {
            '\'' | '"' | '`' => {
                quote = Some(c);
                cur.push(c);
            }
            '(' | '[' | '{' => {
                depth += 1;
                cur.push(c);
            }
            ')' | ']' | '}' => {
                depth -= 1;
                cur.push(c);
            }
            ',' if depth == 0 => out.push(std::mem::take(&mut cur)),
            _ => cur.push(c),
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur);
    }
    out
}

fn unquote(s: &str) -> String {
    let s = s.trim();
    s.strip_prefix('`')
        .and_then(|x| x.strip_suffix('`'))
        .map_or_else(|| s.to_owned(), |x| x.replace("``", "`"))
}

/// Whether `sql` may hold a `GRAPH_TABLE` (a cheap word scan).
#[must_use]
pub fn mentions(sql: &str) -> bool {
    sql.len() < (1 << 20)
        && sql
            .as_bytes()
            .windows(11)
            .any(|w| w.eq_ignore_ascii_case(b"graph_table"))
}

/// Every `GRAPH_TABLE (...)` of `sql`, in order.
///
/// # Errors
/// 39900 for a malformed one.
pub fn find(sql: &str) -> Result<Vec<GraphCall>> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(rel) = keyword(&sql[from..], "GRAPH_TABLE") {
        let start = from + rel;
        let mut open = start + "GRAPH_TABLE".len();
        while open < b.len() && b[open].is_ascii_whitespace() {
            open += 1;
        }
        if b.get(open) != Some(&b'(') {
            return Err(syntax(
                "GRAPH_TABLE needs ( graph MATCH ... COLUMNS (...) )",
            ));
        }
        let end = matching(b, open).ok_or_else(|| syntax("unbalanced GRAPH_TABLE ( ... )"))?;
        let inner = sql[open + 1..end - 1].trim();
        let m = keyword(inner, "MATCH").ok_or_else(|| syntax("GRAPH_TABLE needs MATCH"))?;
        let name = inner[..m].trim().trim_end_matches(',').trim();
        if name.is_empty() {
            return Err(syntax("GRAPH_TABLE needs a graph name"));
        }
        let (db, graph) = match name.split_once('.') {
            Some((d, g)) => (Some(unquote(d)), unquote(g)),
            None => (None, unquote(name)),
        };
        let rest = &inner[m + "MATCH".len()..];
        let c =
            keyword(rest, "COLUMNS").ok_or_else(|| syntax("GRAPH_TABLE needs COLUMNS (...)"))?;
        let body = &rest[..c];
        let (pattern, cond) = match keyword(body, "WHERE") {
            Some(w) => (&body[..w], Some(&body[w + "WHERE".len()..])),
            None => (body, None),
        };
        let cols_text = rest[c + "COLUMNS".len()..].trim();
        let cols_text = cols_text
            .strip_prefix('(')
            .and_then(|x| x.strip_suffix(')'))
            .ok_or_else(|| syntax("COLUMNS needs ( expr AS name, ... )"))?;
        let mut returned = Vec::new();
        let mut columns = Vec::new();
        for item in split_top(cols_text) {
            let item = item.trim();
            let (expr, name) = if let Some(a) = keyword(item, "AS") {
                (item[..a].trim(), unquote(&item[a + 2..]))
            } else {
                let n = item.rsplit('.').next().unwrap_or(item);
                (item, unquote(n))
            };
            returned.push(format!(
                "{} AS `{}`",
                pattern_to_cypher(expr),
                name.replace('`', "``")
            ));
            columns.push(name);
        }
        if columns.is_empty() {
            return Err(syntax("COLUMNS needs at least one column"));
        }
        let mut cypher = format!("MATCH {}", pattern_to_cypher(pattern.trim()));
        if let Some(w) = cond {
            let _ = write!(cypher, " WHERE {}", pattern_to_cypher(w.trim()));
        }
        let _ = write!(cypher, " RETURN {}", returned.join(", "));
        out.push(GraphCall {
            span: (start, end),
            db,
            graph,
            cypher,
            columns,
        });
        from = end;
    }
    Ok(out)
}

fn quote_ident(c: &str) -> String {
    format!("`{}`", c.replace('`', "``"))
}

/// Call `call`'s placeholder: a derived table with its columns and no row,
/// whose condition names the statement's `nonce` and the call.
#[must_use]
pub fn placeholder(nonce: u64, call: usize, columns: &[String]) -> String {
    let cols: Vec<String> = columns
        .iter()
        .map(|c| format!("NULL AS {}", quote_ident(c)))
        .collect();
    format!(
        "(SELECT {} FROM DUAL WHERE {PENDING_FN}({nonce}, {call}))",
        cols.join(", ")
    )
}

/// `sql` with each call of `calls` (found in `sql`) replaced by its
/// placeholder.
#[must_use]
pub fn with_placeholders(sql: &str, calls: &[GraphCall], nonce: u64) -> String {
    let mut s = String::with_capacity(sql.len());
    let mut last = 0;
    for (i, c) in calls.iter().enumerate() {
        s.push_str(&sql[last..c.span.0]);
        s.push_str(&placeholder(nonce, i, &c.columns));
        last = c.span.1;
    }
    s.push_str(&sql[last..]);
    s
}

/// Where the placeholders of `nonce` are in `sql` (the text after the
/// pipeline): per placeholder, its call and the byte range of its
/// parenthesized derived table, in text order.
#[must_use]
pub fn slots(sql: &str, nonce: u64) -> Vec<(usize, (usize, usize))> {
    let lower = sql.to_ascii_lowercase();
    let b = sql.as_bytes();
    let head = format!("{PENDING_FN}({nonce},");
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(rel) = lower[from..].find(&head) {
        let at = from + rel;
        from = at + head.len();
        let rest = &sql[from..];
        let digits: String = rest
            .trim_start()
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        let Ok(call) = digits.parse::<usize>() else {
            continue;
        };
        // The derived table: the nearest `(SELECT` before the marker whose
        // matching `)` closes right after the marker's own `)`.
        let Some(close) = matching(b, at + PENDING_FN.len()) else {
            continue;
        };
        let mut search = at;
        let found = loop {
            let Some(open) = lower[..search].rfind("(select") else {
                break None;
            };
            if let Some(end) = matching(b, open)
                && end > close
            {
                let tail = sql[close..end - 1].trim();
                break tail.is_empty().then_some((open, end));
            }
            search = open;
        };
        if let Some(r) = found {
            out.push((call, r));
        }
    }
    out
}

/// One value of a graph row, as the router needs it: its SQL literal and,
/// for integers and strings, the value (to route a key).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphLit {
    /// `NULL`.
    Null,
    /// An integer.
    Int(i64),
    /// A string (its literal is quoted and escaped).
    Str(String),
    /// Anything else, as its SQL literal (floats, booleans, JSON text).
    Other(String),
}

impl GraphLit {
    /// The SQL literal.
    #[must_use]
    pub fn sql(&self) -> String {
        match self {
            Self::Null => "NULL".into(),
            Self::Int(i) => i.to_string(),
            Self::Str(s) => sql_string(s),
            Self::Other(t) => t.clone(),
        }
    }
}

/// A string as a MariaDB literal (backslashes escaped, as the default
/// `sql_mode` reads them).
#[must_use]
pub fn sql_string(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    o.push('\'');
    for c in s.chars() {
        match c {
            '\'' => o.push_str("''"),
            '\\' => o.push_str("\\\\"),
            '\0' => o.push_str("\\0"),
            _ => o.push(c),
        }
    }
    o.push('\'');
    o
}

/// A call's result.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CallRows {
    /// Column names (the call's `COLUMNS`).
    pub columns: Vec<String>,
    /// Rows.
    pub rows: Vec<Vec<GraphLit>>,
}

/// The rows as a derived table's body: `SELECT a AS c1, b AS c2 UNION ALL
/// VALUES (…), (…)` (one flat list, so parsers don't nest a set operation
/// per row), or an empty one.
#[must_use]
pub fn rows_sql(columns: &[String], rows: &[Vec<GraphLit>]) -> String {
    let Some((first, rest)) = rows.split_first() else {
        let cols: Vec<String> = columns
            .iter()
            .map(|c| format!("NULL AS {}", quote_ident(c)))
            .collect();
        return format!("SELECT {} FROM DUAL WHERE FALSE", cols.join(", "));
    };
    let mut s = String::from("SELECT ");
    for (k, v) in first.iter().enumerate() {
        if k > 0 {
            s.push_str(", ");
        }
        s.push_str(&v.sql());
        let _ = write!(
            s,
            " AS {}",
            quote_ident(columns.get(k).map_or("c", String::as_str))
        );
    }
    for (i, r) in rest.iter().enumerate() {
        s.push_str(if i == 0 { " UNION ALL VALUES (" } else { ", (" });
        for (k, v) in r.iter().enumerate() {
            if k > 0 {
                s.push_str(", ");
            }
            s.push_str(&v.sql());
        }
        s.push(')');
    }
    s
}

/// Where a call's graph is: its shards (cluster, schema) and, filled by
/// the gateway from the graph executor's `EXPLAIN`, each step's tier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphStep {
    /// The call (index into [`GraphPlan::calls`]).
    pub call: usize,
    /// `db.graph`.
    pub graph: String,
    /// The graph's table group, when it is a sharded group (round 3 on).
    pub group: Option<u32>,
    /// Its shards: from the topology's group, or the one schema of a graph
    /// from rounds 1 and 2 or a mapped graph.
    pub shards: Vec<ShardLoc>,
    /// One line per clause and hop, with its tier (`join`, `cte`,
    /// `gateway`).
    pub tiers: Vec<String>,
}

/// An equality between a graph column and a sharded table's routing key,
/// in the top `SELECT`'s `WHERE` or an inner join's `ON`: only the shards
/// holding the graph rows' values of that column can produce rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyJoin {
    /// The call.
    pub call: usize,
    /// The column of the call's rows (index into its columns).
    pub column: usize,
    /// The table, for EXPLAIN.
    pub table: String,
    /// The table's group.
    pub group: Arc<Group>,
}

/// A statement with `GRAPH_TABLE` (chapter 17's `Plan::Graph`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphPlan {
    /// The placeholders' nonce.
    pub nonce: u64,
    /// The calls, in statement order.
    pub calls: Vec<GraphCall>,
    /// The outer statement, its calls still placeholders.
    pub outer: String,
    /// The placeholders in `outer`: call and byte range.
    pub slots: Vec<(usize, (usize, usize))>,
    /// Per call, where its graph is.
    pub steps: Vec<GraphStep>,
    /// Graph columns joined to routing keys.
    pub keys: Vec<KeyJoin>,
    /// The outer statement planned with empty graph rows (EXPLAIN; the
    /// executed plan is made from the rows).
    pub outer_plan: Box<Plan>,
}

impl GraphPlan {
    /// The outer statement with each placeholder replaced by its call's
    /// rows.
    #[must_use]
    pub fn bind(&self, rows: &[CallRows]) -> String {
        bind_text(&self.outer, &self.slots, |call| {
            rows.get(call).map_or_else(
                || rows_sql(&self.calls[call].columns, &[]),
                |r| rows_sql(&r.columns, &r.rows),
            )
        })
    }

    /// The outer statement with every call empty (EXPLAIN's outer plan).
    #[must_use]
    pub fn bind_empty(
        outer: &str,
        slots: &[(usize, (usize, usize))],
        calls: &[GraphCall],
    ) -> String {
        bind_text(outer, slots, |call| {
            rows_sql(calls.get(call).map_or(&[][..], |c| &c.columns), &[])
        })
    }

    /// The shards a read of `group` may keep: those holding a value the
    /// joined graph columns hold; `None` when no key join restricts it (or
    /// a value doesn't route exactly).
    #[must_use]
    pub fn key_shards(&self, rows: &[CallRows], group: u32, db: u32) -> Option<BTreeSet<u32>> {
        let mut out: Option<BTreeSet<u32>> = None;
        for k in self
            .keys
            .iter()
            .filter(|k| k.group.id == group && k.group.db == db)
        {
            let Some(r) = rows.get(k.call) else { continue };
            let Some(set) = shards_of(&k.group, r, k.column) else {
                continue;
            };
            out = Some(match out {
                None => set,
                Some(o) => o.intersection(&set).copied().collect(),
            });
        }
        out
    }

    /// `plan` (the bound statement's plan) narrowed by the key joins: a
    /// read over the joined group keeps the shards holding the graph
    /// rows' keys (at least one, so the answer keeps its columns and
    /// aggregates their empty values).
    #[must_use]
    pub fn narrow(&self, plan: Plan, rows: &[CallRows]) -> Plan {
        let Plan::Read(mut r) = plan else {
            return plan;
        };
        let Some(db) = r.targets.first().map(|t| t.at.db) else {
            return Plan::Read(r);
        };
        let Some(keep) = self.key_shards(rows, r.group, db) else {
            return Plan::Read(r);
        };
        let first = r.targets.first().cloned();
        r.targets.retain(|t| keep.contains(&t.shard));
        if r.targets.is_empty() {
            r.targets.extend(first);
        }
        metrics::counter!("sprig_router_graph_narrowed_total").increment(1);
        Plan::Read(r)
    }
}

fn bind_text(
    outer: &str,
    slots: &[(usize, (usize, usize))],
    rows: impl Fn(usize) -> String,
) -> String {
    let mut s = String::with_capacity(outer.len());
    let mut last = 0;
    let mut ordered: Vec<&(usize, (usize, usize))> = slots.iter().collect();
    ordered.sort_by_key(|(_, r)| r.0);
    for (call, (a, b)) in ordered {
        if *a < last {
            continue;
        }
        s.push_str(&outer[last..*a]);
        s.push('(');
        s.push_str(&rows(*call));
        s.push(')');
        last = *b;
    }
    s.push_str(&outer[last..]);
    s
}

/// The shard ids of `group` holding column `column`'s values in `r`;
/// `None` when a value isn't a key value of the group's class (a float, a
/// boolean, JSON, an integer against a string key), so no shard may be
/// left out. `NULL`s never equal a key.
fn shards_of(group: &Group, r: &CallRows, column: usize) -> Option<BTreeSet<u32>> {
    let utf8 = |c: u16| crate::collation::info(c).is_some_and(|i| i.charset == "utf8mb4");
    let mut set = BTreeSet::new();
    for row in &r.rows {
        let v = match (row.get(column)?, group.key_class) {
            (GraphLit::Null, _) => continue,
            (GraphLit::Int(i), KeyClass::Int) => KeyValue::Int(i128::from(*i)),
            (GraphLit::Str(s), KeyClass::Str { collation }) if utf8(collation) => {
                KeyValue::Bytes(s.as_bytes().to_vec())
            }
            _ => return None,
        };
        // Not `shard_of`: a read's keys aren't write load (chapter 20).
        let i = group.route(group.hash_of(&v)?);
        set.insert(group.shards.get(i)?.shard);
    }
    Some(set)
}

#[cfg(test)]
mod tests;
