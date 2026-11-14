//! Finding search calls in SQL text: `MATCH (columns) AGAINST ('text' [modifier])` and
//! `sprig_search('index', 'query' [, options])`, with their byte spans,
//! the clause each sits in, and the tables named after `FROM` and `JOIN`.
//!
//! The gateway replaces each call by SQL over the table's key, so the
//! statement then runs as any other: names, privileges, row-level
//! security and the router apply to it unchanged.

use sprig_common::{Result, SprigCode, SprigError};

/// `AGAINST`'s modifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Natural language mode (the default).
    Natural,
    /// `IN BOOLEAN MODE`.
    Boolean,
    /// `WITH QUERY EXPANSION` (refused: 39823).
    Expansion,
}

/// One `MATCH ... AGAINST`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchCall {
    /// Byte span of the whole call.
    pub span: (usize, usize),
    /// Columns: (qualifier, name).
    pub columns: Vec<(Option<String>, String)>,
    /// The search text; `None` for a placeholder or an expression.
    pub text: Option<String>,
    /// The modifier.
    pub mode: Mode,
    /// In `WHERE` or `ON` (a condition) rather than a value.
    pub condition: bool,
}

/// One `sprig_search(...)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchCall {
    /// Byte span of the call.
    pub span: (usize, usize),
    /// The index name (maybe `db.name`).
    pub index: String,
    /// The query string.
    pub query: String,
    /// The options (`JSON_OBJECT(...)` or a JSON string), as SQL text.
    pub options: Option<String>,
}

/// A table after `FROM` or `JOIN`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRef {
    /// Database, when qualified.
    pub db: Option<String>,
    /// The table.
    pub table: String,
    /// Its alias.
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tk {
    Word(String),
    Quoted(String),
    Str(String),
    Other(char),
    Param,
}

#[derive(Debug, Clone)]
struct Tok {
    t: Tk,
    from: usize,
    to: usize,
}

fn syntax(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::SearchQuerySyntax, m)
}

fn lex(sql: &str) -> Vec<Tok> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
        } else if c == b'#' || (c == b'-' && b.get(i + 1) == Some(&b'-')) {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
        } else if c == b'/' && b.get(i + 1) == Some(&b'*') {
            i += 2;
            while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(b.len());
        } else if c == b'\'' || c == b'"' || c == b'`' {
            let start = i;
            let q = c;
            let mut v = Vec::new();
            i += 1;
            while i < b.len() {
                let x = b[i];
                if x == b'\\' && q != b'`' && i + 1 < b.len() {
                    v.push(match b[i + 1] {
                        b'n' => b'\n',
                        b't' => b'\t',
                        b'0' => 0,
                        o => o,
                    });
                    i += 2;
                    continue;
                }
                if x == q {
                    if b.get(i + 1) == Some(&q) {
                        v.push(q);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                v.push(x);
                i += 1;
            }
            let s = String::from_utf8_lossy(&v).into_owned();
            out.push(Tok {
                t: if q == b'`' { Tk::Quoted(s) } else { Tk::Str(s) },
                from: start,
                to: i,
            });
        } else if c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80 {
            let start = i;
            while i < b.len()
                && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$' || b[i] >= 0x80)
            {
                i += 1;
            }
            out.push(Tok {
                t: Tk::Word(sql[start..i].to_owned()),
                from: start,
                to: i,
            });
        } else if c == b'?' {
            out.push(Tok {
                t: Tk::Param,
                from: i,
                to: i + 1,
            });
            i += 1;
        } else {
            let ch = sql[i..].chars().next().unwrap_or('?');
            out.push(Tok {
                t: Tk::Other(ch),
                from: i,
                to: i + ch.len_utf8(),
            });
            i += ch.len_utf8();
        }
    }
    out
}

fn is_kw(t: &Tk, w: &str) -> bool {
    matches!(t, Tk::Word(x) if x.eq_ignore_ascii_case(w))
}

fn name(t: &Tk) -> Option<String> {
    match t {
        Tk::Word(x) | Tk::Quoted(x) => Some(x.clone()),
        _ => None,
    }
}

const CLAUSES: [&str; 12] = [
    "SELECT", "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "ON", "SET", "VALUES", "JOIN",
    "UNION",
];

const NOT_ALIAS: [&str; 22] = [
    "WHERE",
    "JOIN",
    "INNER",
    "LEFT",
    "RIGHT",
    "CROSS",
    "NATURAL",
    "ON",
    "USING",
    "GROUP",
    "ORDER",
    "LIMIT",
    "HAVING",
    "UNION",
    "STRAIGHT_JOIN",
    "FOR",
    "LOCK",
    "WINDOW",
    "SET",
    "INTO",
    "PARTITION",
    "FULL",
];

/// What a statement holds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Scan {
    /// `MATCH ... AGAINST` calls, in order.
    pub matches: Vec<MatchCall>,
    /// `sprig_search(...)` calls, in order.
    pub searches: Vec<SearchCall>,
    /// Tables after `FROM` and `JOIN`.
    pub tables: Vec<TableRef>,
}

/// Whether the text may hold a search call (cheap pre-check).
#[must_use]
pub fn mentions(sql: &[u8]) -> bool {
    let has = |w: &[u8]| sql.windows(w.len()).any(|x| x.eq_ignore_ascii_case(w));
    has(b"against") || has(b"sprig_search")
}

/// The byte spans of `sql`'s `?` placeholders, in order (outside strings,
/// quoted names and comments).
#[must_use]
pub fn placeholders(sql: &str) -> Vec<(usize, usize)> {
    lex(sql)
        .into_iter()
        .filter(|t| t.t == Tk::Param)
        .map(|t| (t.from, t.to))
        .collect()
}

/// `sql` with its placeholders replaced, in order, by `values` (SQL
/// literals); `None` when the counts differ.
#[must_use]
pub fn interpolate(sql: &str, values: &[String]) -> Option<String> {
    let spans = placeholders(sql);
    if spans.len() != values.len() {
        return None;
    }
    let mut out = String::with_capacity(sql.len() + values.iter().map(String::len).sum::<usize>());
    let mut last = 0;
    for ((a, b), v) in spans.iter().zip(values) {
        out.push_str(&sql[last..*a]);
        out.push_str(v);
        last = *b;
    }
    out.push_str(&sql[last..]);
    Some(out)
}

/// Scans `sql`.
///
/// # Errors
/// 39822 for a `MATCH` or `sprig_search` that isn't well formed.
pub fn scan(sql: &str) -> Result<Scan> {
    let t = lex(sql);
    let mut out = Scan::default();
    // The clause each parenthesis depth is in.
    let mut clause: Vec<String> = vec![String::new()];
    let mut i = 0;
    while i < t.len() {
        match &t[i].t {
            Tk::Other('(') => {
                clause.push(String::new());
                i += 1;
                continue;
            }
            Tk::Other(')') => {
                if clause.len() > 1 {
                    clause.pop();
                }
                i += 1;
                continue;
            }
            Tk::Word(w) => {
                let up = w.to_ascii_uppercase();
                if is_kw(&t[i].t, "MATCH")
                    && matches!(t.get(i + 1).map(|x| &x.t), Some(Tk::Other('(')))
                {
                    let (m, next) =
                        match_call(sql, &t, i, clause.last().map_or("", String::as_str))?;
                    out.matches.push(m);
                    i = next;
                    continue;
                }
                if up == "SPRIG_SEARCH"
                    && matches!(t.get(i + 1).map(|x| &x.t), Some(Tk::Other('(')))
                {
                    let (s, next) = search_call(sql, &t, i)?;
                    out.searches.push(s);
                    i = next;
                    continue;
                }
                if CLAUSES.contains(&up.as_str())
                    && let Some(c) = clause.last_mut()
                {
                    c.clone_from(&up);
                }
                if up == "FROM" || up == "JOIN" || up == "UPDATE" || up == "INTO" {
                    i = table_list(&t, i + 1, &mut out.tables);
                    continue;
                }
            }
            _ => {}
        }
        i += 1;
    }
    Ok(out)
}

fn table_list(t: &[Tok], mut i: usize, out: &mut Vec<TableRef>) -> usize {
    loop {
        let Some(first) = t.get(i).and_then(|x| name(&x.t)) else {
            return i;
        };
        if matches!(t[i].t, Tk::Word(ref w) if NOT_ALIAS.iter().any(|k| w.eq_ignore_ascii_case(k)) || w.eq_ignore_ascii_case("SELECT"))
        {
            return i;
        }
        // A function call (`sprig_search(...)`, `JSON_TABLE(...)`) isn't a table.
        if matches!(t.get(i + 1).map(|x| &x.t), Some(Tk::Other('('))) {
            return i;
        }
        i += 1;
        let (db, table) = if matches!(t.get(i).map(|x| &x.t), Some(Tk::Other('.'))) {
            let Some(tb) = t.get(i + 1).and_then(|x| name(&x.t)) else {
                return i;
            };
            i += 2;
            (Some(first), tb)
        } else {
            (None, first)
        };
        if t.get(i).is_some_and(|x| is_kw(&x.t, "AS")) {
            i += 1;
        }
        let alias = match t.get(i).map(|x| &x.t) {
            Some(Tk::Word(w)) if !NOT_ALIAS.iter().any(|k| w.eq_ignore_ascii_case(k)) => {
                i += 1;
                Some(w.clone())
            }
            Some(Tk::Quoted(w)) => {
                i += 1;
                Some(w.clone())
            }
            _ => None,
        };
        out.push(TableRef { db, table, alias });
        if matches!(t.get(i).map(|x| &x.t), Some(Tk::Other(','))) {
            i += 1;
            continue;
        }
        return i;
    }
}

fn need(t: &[Tok], i: usize, c: char) -> Result<()> {
    if matches!(t.get(i).map(|x| &x.t), Some(Tk::Other(x)) if *x == c) {
        Ok(())
    } else {
        Err(syntax(format!("expected '{c}' in a search call")))
    }
}

fn match_call(sql: &str, t: &[Tok], at: usize, clause: &str) -> Result<(MatchCall, usize)> {
    let mut i = at + 2;
    let mut columns = Vec::new();
    loop {
        let a = t
            .get(i)
            .and_then(|x| name(&x.t))
            .ok_or_else(|| syntax("MATCH needs column names"))?;
        i += 1;
        if matches!(t.get(i).map(|x| &x.t), Some(Tk::Other('.'))) {
            let b = t
                .get(i + 1)
                .and_then(|x| name(&x.t))
                .ok_or_else(|| syntax("MATCH needs column names"))?;
            i += 2;
            // `db.table.column`: keep the table as the qualifier.
            if matches!(t.get(i).map(|x| &x.t), Some(Tk::Other('.'))) {
                let c = t
                    .get(i + 1)
                    .and_then(|x| name(&x.t))
                    .ok_or_else(|| syntax("MATCH needs column names"))?;
                i += 2;
                columns.push((Some(b), c));
            } else {
                columns.push((Some(a), b));
            }
        } else {
            columns.push((None, a));
        }
        if matches!(t.get(i).map(|x| &x.t), Some(Tk::Other(','))) {
            i += 1;
            continue;
        }
        break;
    }
    need(t, i, ')')?;
    i += 1;
    if !t.get(i).is_some_and(|x| is_kw(&x.t, "AGAINST")) {
        return Err(syntax("MATCH (...) must be followed by AGAINST"));
    }
    i += 1;
    need(t, i, '(')?;
    i += 1;
    let text = match t.get(i).map(|x| &x.t) {
        Some(Tk::Str(s)) => Some(s.clone()),
        _ => None,
    };
    // Skip to the matching ')', reading the modifier.
    let mut depth = 0i32;
    let mut mode = Mode::Natural;
    while i < t.len() {
        match &t[i].t {
            Tk::Other('(') => depth += 1,
            Tk::Other(')') => {
                if depth == 0 {
                    break;
                }
                depth -= 1;
            }
            x if is_kw(x, "BOOLEAN") => mode = Mode::Boolean,
            x if is_kw(x, "EXPANSION") => mode = Mode::Expansion,
            _ => {}
        }
        i += 1;
    }
    need(t, i, ')')?;
    let end = t[i].to;
    let _ = sql;
    Ok((
        MatchCall {
            span: (t[at].from, end),
            columns,
            text,
            mode,
            condition: matches!(clause, "WHERE" | "ON"),
        },
        i + 1,
    ))
}

fn search_call(sql: &str, t: &[Tok], at: usize) -> Result<(SearchCall, usize)> {
    let mut i = at + 2;
    let str_at = |i: usize| match t.get(i).map(|x| &x.t) {
        Some(Tk::Str(s)) => Ok(s.clone()),
        _ => Err(syntax(
            "sprig_search() takes an index name and a query string",
        )),
    };
    let index = str_at(i)?;
    i += 1;
    need(t, i, ',')?;
    i += 1;
    let query = str_at(i)?;
    i += 1;
    let mut options = None;
    if matches!(t.get(i).map(|x| &x.t), Some(Tk::Other(','))) {
        i += 1;
        let start = t.get(i).map_or(sql.len(), |x| x.from);
        let mut depth = 0i32;
        while i < t.len() {
            match &t[i].t {
                Tk::Other('(') => depth += 1,
                Tk::Other(')') => {
                    if depth == 0 {
                        break;
                    }
                    depth -= 1;
                }
                _ => {}
            }
            i += 1;
        }
        let end = t.get(i).map_or(sql.len(), |x| x.from);
        options = Some(sql[start..end].trim().to_owned());
    }
    need(t, i, ')')?;
    Ok((
        SearchCall {
            span: (t[at].from, t[i].to),
            index,
            query,
            options,
        },
        i + 1,
    ))
}

/// The options of `sprig_search()` as JSON: a JSON string literal's text,
/// or `JSON_OBJECT('k', v, ...)` with literal values.
///
/// # Errors
/// 39822 for anything else.
pub fn options_json(text: &str) -> Result<serde_json::Value> {
    let t = lex(text);
    match t.first().map(|x| &x.t) {
        None => Ok(serde_json::Value::Null),
        Some(Tk::Str(s)) if t.len() == 1 => {
            serde_json::from_str(s).map_err(|e| syntax(format!("sprig_search() options: {e}")))
        }
        Some(x) if is_kw(x, "JSON_OBJECT") => {
            let mut m = serde_json::Map::new();
            let mut i = 2;
            loop {
                let Some(Tk::Str(k)) = t.get(i).map(|x| x.t.clone()) else {
                    break;
                };
                i += 1;
                need(&t, i, ',')?;
                i += 1;
                let (v, next) = json_value(&t, i)?;
                m.insert(k, v);
                i = next;
                if matches!(t.get(i).map(|x| &x.t), Some(Tk::Other(','))) {
                    i += 1;
                    continue;
                }
                break;
            }
            Ok(serde_json::Value::Object(m))
        }
        _ => Err(syntax(
            "sprig_search() options must be JSON_OBJECT(...) or a JSON string",
        )),
    }
}

fn json_value(t: &[Tok], i: usize) -> Result<(serde_json::Value, usize)> {
    match t.get(i).map(|x| &x.t) {
        Some(Tk::Str(s)) => Ok((serde_json::Value::String(s.clone()), i + 1)),
        Some(Tk::Word(w)) if w.eq_ignore_ascii_case("true") => {
            Ok((serde_json::Value::Bool(true), i + 1))
        }
        Some(Tk::Word(w)) if w.eq_ignore_ascii_case("false") => {
            Ok((serde_json::Value::Bool(false), i + 1))
        }
        Some(Tk::Word(w)) if w.eq_ignore_ascii_case("JSON_ARRAY") => {
            let mut v = Vec::new();
            let mut j = i + 2;
            while !matches!(t.get(j).map(|x| &x.t), Some(Tk::Other(')')) | None) {
                let (x, n) = json_value(t, j)?;
                v.push(x);
                j = n;
                if matches!(t.get(j).map(|x| &x.t), Some(Tk::Other(','))) {
                    j += 1;
                }
            }
            Ok((serde_json::Value::Array(v), j + 1))
        }
        Some(Tk::Word(w)) => serde_json::from_str::<serde_json::Value>(w)
            .map(|v| (v, i + 1))
            .map_err(|_| syntax(format!("unsupported option value {w}"))),
        _ => Err(syntax("unsupported option value")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_match_calls_and_context() {
        let sql = "SELECT id, MATCH(name, p.description) AGAINST ('red shoe') AS score \
                   FROM shop.products AS p JOIN inv i ON i.pid = p.id \
                   WHERE MATCH(name) AGAINST ('+red -kids' IN BOOLEAN MODE) AND price < 100 \
                   ORDER BY score DESC";
        let s = scan(sql).unwrap();
        assert_eq!(s.matches.len(), 2);
        assert!(!s.matches[0].condition);
        assert!(s.matches[1].condition);
        assert_eq!(s.matches[1].mode, Mode::Boolean);
        assert_eq!(
            s.matches[0].columns[1],
            (Some("p".into()), "description".into())
        );
        assert_eq!(
            &sql[s.matches[0].span.0..s.matches[0].span.1],
            "MATCH(name, p.description) AGAINST ('red shoe')"
        );
        assert_eq!(
            s.tables[0],
            TableRef {
                db: Some("shop".into()),
                table: "products".into(),
                alias: Some("p".into())
            }
        );
        assert_eq!(s.tables[1].alias.as_deref(), Some("i"));
    }

    #[test]
    fn placeholders_and_subqueries() {
        let s = scan("SELECT * FROM t WHERE id IN (SELECT id FROM u WHERE MATCH(a) AGAINST (?))")
            .unwrap();
        assert!(s.matches[0].text.is_none());
        assert!(s.matches[0].condition);
        let s = scan("SELECT MATCH(a) AGAINST ('x' WITH QUERY EXPANSION) FROM t").unwrap();
        assert_eq!(s.matches[0].mode, Mode::Expansion);
        // In a string, not a call.
        assert!(
            scan("SELECT 'MATCH(a) AGAINST (1)'")
                .unwrap()
                .matches
                .is_empty()
        );
    }

    #[test]
    fn finds_sprig_search() {
        let sql = "SELECT p.id, s.score FROM sprig_search('product_text', 'red \"running shoe\"', \
                   JSON_OBJECT('limit', 20, 'consistency', 'own_writes', 'highlight', JSON_ARRAY('name'))) AS s \
                   JOIN products p ON p.id = s.id";
        let s = scan(sql).unwrap();
        assert_eq!(s.searches.len(), 1);
        let c = &s.searches[0];
        assert_eq!(c.index, "product_text");
        let o = options_json(c.options.as_deref().unwrap()).unwrap();
        assert_eq!(o["limit"], 20);
        assert_eq!(o["highlight"][0], "name");
        assert_eq!(s.tables.last().unwrap().table, "products");
    }

    #[test]
    fn interpolates_placeholders_only() {
        let sql = "SELECT '?', `a?` FROM t WHERE MATCH(a) AGAINST (?) AND id > ? -- ?";
        assert_eq!(placeholders(sql).len(), 2);
        assert_eq!(
            interpolate(sql, &["'red shoe'".into(), "5".into()]).unwrap(),
            "SELECT '?', `a?` FROM t WHERE MATCH(a) AGAINST ('red shoe') AND id > 5 -- ?"
        );
        assert!(interpolate(sql, &["1".into()]).is_none());
    }
}
