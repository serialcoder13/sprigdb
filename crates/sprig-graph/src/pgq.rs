//! SQL/PGQ's `GRAPH_TABLE` (chapter 43, "Language surface"; build step
//! 8), as built in round 3: the gateway finds each `GRAPH_TABLE (g MATCH
//! pattern [WHERE condition] COLUMNS (expr AS name, ...))` in a SQL
//! statement, runs it as the equivalent Cypher read (`MATCH pattern' WHERE
//! condition RETURN expr AS name, ...`, with `IS Label` written `:Label`),
//! and puts the rows in its place as a derived table of literal rows
//! (`SELECT ... UNION ALL SELECT ...`), which the SQL pipeline then plans
//! like any derived table: it joins, groups and filters with the rest of the
//! statement.
//!
//! Values come out as SQL literals: integers, floats, strings and booleans
//! as themselves, `NULL`, and nodes, relationships, paths, lists, maps and
//! temporal values as their JSON or text form (chapter 43: "Native
//! properties come out as JSON unless cast"). The rows are bounded by
//! [`MAX_INLINE_BYTES`] (39903 past it).

use std::fmt::Write as _;

use sprig_common::{Result, SprigCode, SprigError};

use crate::value::Value;

/// Largest inlined result of one `GRAPH_TABLE`, in bytes of SQL text.
pub const MAX_INLINE_BYTES: usize = 16 << 20;

/// One `GRAPH_TABLE (...)` in a statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphTable {
    /// Byte range of `GRAPH_TABLE ( ... )` in the statement.
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
                depth -= 1;
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
            out.push(c as char);
            if c == q {
                quote = None;
            }
            i += 1;
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

/// Every `GRAPH_TABLE (...)` of `sql`, in order.
///
/// # Errors
/// 39900 for a malformed one.
pub fn find(sql: &str) -> Result<Vec<GraphTable>> {
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
        out.push(GraphTable {
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

fn sql_string(s: &str) -> String {
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

/// One value as a SQL literal.
#[must_use]
pub fn literal(v: &Value) -> String {
    match v {
        Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.into(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) if f.is_finite() => format!("{f:e}"),
        Value::Null | Value::Float(_) => "NULL".into(),
        Value::String(s) => sql_string(s),
        Value::List(_) | Value::Map(_) | Value::Node(_) | Value::Rel(_) | Value::Path(_) => {
            sql_string(&crate::value::to_json(v).to_string())
        }
        other => sql_string(&other.display()),
    }
}

/// The rows as a derived table's body:
/// `SELECT a AS c1, b AS c2 UNION ALL SELECT ...`, or an empty one.
///
/// # Errors
/// 39903 past [`MAX_INLINE_BYTES`].
pub fn rows_sql(columns: &[String], rows: &[Vec<Value>]) -> Result<String> {
    let q = |c: &str| format!("`{}`", c.replace('`', "``"));
    if rows.is_empty() {
        let cols: Vec<String> = columns
            .iter()
            .map(|c| format!("NULL AS {}", q(c)))
            .collect();
        return Ok(format!("SELECT {} FROM DUAL WHERE FALSE", cols.join(", ")));
    }
    let mut s = String::new();
    for (i, r) in rows.iter().enumerate() {
        if i > 0 {
            s.push_str(" UNION ALL ");
        }
        s.push_str("SELECT ");
        for (k, v) in r.iter().enumerate() {
            if k > 0 {
                s.push_str(", ");
            }
            s.push_str(&literal(v));
            if i == 0 {
                let _ = write!(s, " AS {}", q(columns.get(k).map_or("c", String::as_str)));
            }
        }
        if s.len() > MAX_INLINE_BYTES {
            return Err(crate::exec::budget_error(
                "rows",
                format!("a GRAPH_TABLE result is larger than {MAX_INLINE_BYTES} bytes"),
            ));
        }
    }
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_and_translates_the_chapters_example() {
        let sql = "SELECT g.name, SUM(g.total) AS spent
FROM GRAPH_TABLE (shop
       MATCH (c IS Person)-[o IS BOUGHT]->(p IS Product)
       WHERE c.email = 'alice@example.com'
       COLUMNS (p.name AS name, o.qty * p.price AS total)) AS g
GROUP BY g.name";
        let v = find(sql).expect("ok");
        assert_eq!(v.len(), 1);
        let g = &v[0];
        assert_eq!(g.graph, "shop");
        assert_eq!(g.columns, ["name", "total"]);
        assert_eq!(
            g.cypher,
            "MATCH (c:Person)-[o:BOUGHT]->(p:Product) WHERE c.email = 'alice@example.com' RETURN p.name AS `name`, o.qty * p.price AS `total`"
        );
        assert!(sql[g.span.0..g.span.1].starts_with("GRAPH_TABLE"));
        assert!(sql[g.span.1..].trim_start().starts_with("AS g"));
        // The keyword inside a string is not a pattern word.
        assert_eq!(
            pattern_to_cypher("(a IS L {n: 'x IS y'})"),
            "(a:L {n: 'x IS y'})"
        );
        assert!(find("SELECT 1").expect("ok").is_empty());
        assert!(find("SELECT * FROM GRAPH_TABLE (g COLUMNS (a))").is_err());
    }

    #[test]
    fn rows_become_literals() {
        let cols = vec!["a".to_owned(), "b".to_owned()];
        let s = rows_sql(
            &cols,
            &[
                vec![Value::Int(1), Value::String("it's".into())],
                vec![Value::Null, Value::Float(2.5)],
            ],
        )
        .expect("ok");
        assert_eq!(
            s,
            "SELECT 1 AS `a`, 'it''s' AS `b` UNION ALL SELECT NULL, 2.5e0"
        );
        assert_eq!(
            rows_sql(&cols, &[]).expect("ok"),
            "SELECT NULL AS `a`, NULL AS `b` FROM DUAL WHERE FALSE"
        );
    }
}
