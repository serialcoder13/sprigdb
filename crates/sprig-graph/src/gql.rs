//! The GQL subset: GQL's forms that differ from Cypher are rewritten into
//! the Cypher the parser already reads, before parsing, so both languages
//! share one parser, one executor and one set of answers. Neo4j 5 reads `IS`
//! label expressions too, and Cypher 25 the rest, so the forms are accepted
//! in every statement.
//!
//! | GQL | Cypher |
//! |---|---|
//! | `(n IS Person)`, `[r IS KNOWS]`, `IS A\|B` | `(n:Person)`, `[r:KNOWS]`, `:A\|B` |
//! | `FILTER cond` | `WITH * WHERE cond` |
//! | `LET a = e, b = f` | `WITH *, e AS a, f AS b` |
//! | `FOR x IN list` | `UNWIND list AS x` |
//! | `INSERT pattern` | `CREATE pattern` |
//! | `RETURN a NEXT ...` | `WITH a ...` |
//! | `OFFSET n` | `SKIP n` |
//! | `MATCH TRAIL p = ...` | `MATCH p = ...` (Cypher's mode) |
//!
//! Not built: the `WALK`, `ACYCLIC` and `SIMPLE` path modes (39901: Cypher's
//! executor matches trails), `CREATE GRAPH g ANY` through GQL (SQL has it),
//! closed graph types.

use sprig_common::{Result, SprigCode, SprigError};

#[derive(Debug, Clone, PartialEq, Eq)]
enum T {
    Word(String),
    Str(String),
    Punct(char),
    Space(String),
}

fn lex(s: &str) -> Vec<T> {
    let c: Vec<char> = s.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < c.len() {
        let ch = c[i];
        if ch.is_whitespace() {
            let mut w = String::new();
            while i < c.len() && c[i].is_whitespace() {
                w.push(c[i]);
                i += 1;
            }
            out.push(T::Space(w));
        } else if ch == '/' && c.get(i + 1) == Some(&'/') {
            let mut w = String::new();
            while i < c.len() && c[i] != '\n' {
                w.push(c[i]);
                i += 1;
            }
            out.push(T::Space(w));
        } else if ch == '\'' || ch == '"' || ch == '`' {
            let mut w = String::from(ch);
            i += 1;
            while i < c.len() {
                w.push(c[i]);
                if c[i] == '\\' && ch != '`' && i + 1 < c.len() {
                    w.push(c[i + 1]);
                    i += 2;
                    continue;
                }
                if c[i] == ch {
                    i += 1;
                    break;
                }
                i += 1;
            }
            if ch == '`' {
                out.push(T::Word(w));
            } else {
                out.push(T::Str(w));
            }
        } else if ch.is_alphanumeric() || ch == '_' || ch == '$' {
            let mut w = String::new();
            while i < c.len() && (c[i].is_alphanumeric() || c[i] == '_' || c[i] == '$') {
                w.push(c[i]);
                i += 1;
            }
            out.push(T::Word(w));
        } else {
            out.push(T::Punct(ch));
            i += 1;
        }
    }
    out
}

fn is_kw(t: &T, k: &str) -> bool {
    matches!(t, T::Word(w) if w.eq_ignore_ascii_case(k))
}

const CLAUSES: &[&str] = &[
    "MATCH", "OPTIONAL", "WHERE", "WITH", "RETURN", "UNWIND", "CREATE", "MERGE", "SET", "REMOVE",
    "DELETE", "DETACH", "CALL", "FOREACH", "ORDER", "SKIP", "LIMIT", "UNION", "FILTER", "LET",
    "FOR", "INSERT", "NEXT", "OFFSET",
];

fn render(ts: &[T]) -> String {
    let mut s = String::new();
    for t in ts {
        match t {
            T::Word(w) | T::Str(w) | T::Space(w) => s.push_str(w),
            T::Punct(c) => s.push(*c),
        }
    }
    s
}

/// The index of the next clause keyword at depth 0 from `i`, or the end.
fn clause_end(ts: &[T], mut i: usize) -> usize {
    let mut depth = 0i32;
    while i < ts.len() {
        match &ts[i] {
            T::Punct('(' | '[' | '{') => depth += 1,
            T::Punct(')' | ']' | '}') => depth -= 1,
            T::Punct(';') if depth == 0 => return i,
            T::Word(w) if depth == 0 && CLAUSES.iter().any(|k| w.eq_ignore_ascii_case(k)) => {
                return i;
            }
            _ => {}
        }
        if depth < 0 {
            return i;
        }
        i += 1;
    }
    i
}

/// Splits tokens at commas of depth 0.
fn split_commas(ts: &[T]) -> Vec<Vec<T>> {
    let mut out = vec![Vec::new()];
    let mut depth = 0i32;
    for t in ts {
        match t {
            T::Punct('(' | '[' | '{') => depth += 1,
            T::Punct(')' | ']' | '}') => depth -= 1,
            _ => {}
        }
        if depth == 0 && *t == T::Punct(',') {
            out.push(Vec::new());
        } else if let Some(l) = out.last_mut() {
            l.push(t.clone());
        }
    }
    out
}

/// The previous non-space token's index before `i`.
fn prev(ts: &[T], i: usize) -> Option<usize> {
    (0..i).rev().find(|&j| !matches!(ts[j], T::Space(_)))
}

fn next(ts: &[T], i: usize) -> Option<usize> {
    (i + 1..ts.len()).find(|&j| !matches!(ts[j], T::Space(_)))
}

fn unsupported(m: &str) -> SprigError {
    SprigError::new(SprigCode::GraphUnsupported, m.to_owned())
}

/// Rewrites the GQL forms of `text` into Cypher (the table above); text
/// without them comes back unchanged.
///
/// # Errors
/// 39901 for the path modes `WALK`, `ACYCLIC` and `SIMPLE`; 39900 for a
/// malformed `LET` or `FOR`.
#[allow(clippy::too_many_lines)] // one pass, one rule per form
pub fn rewrite(text: &str) -> Result<String> {
    let ts = lex(text);
    if !ts.iter().any(|t| {
        [
            "IS", "FILTER", "LET", "FOR", "INSERT", "NEXT", "OFFSET", "TRAIL", "WALK", "ACYCLIC",
            "SIMPLE",
        ]
        .iter()
        .any(|k| is_kw(t, k))
    }) {
        return Ok(text.to_owned());
    }
    let mut out: Vec<T> = Vec::with_capacity(ts.len());
    let mut last_return: Option<usize> = None;
    let mut depth = 0i32;
    let mut i = 0;
    while i < ts.len() {
        let t = &ts[i];
        match t {
            T::Punct('(' | '[' | '{') => depth += 1,
            T::Punct(')' | ']' | '}') => depth -= 1,
            _ => {}
        }
        // `(n IS L)` / `[r IS T]` / `(IS L)`: a label expression.
        if is_kw(t, "IS") {
            let p = prev(&ts, i);
            let opener = p.and_then(|p| match &ts[p] {
                T::Punct('(' | '[') => Some(p),
                T::Word(_) => prev(&ts, p).filter(|&q| matches!(ts[q], T::Punct('(' | '['))),
                _ => None,
            });
            let n = next(&ts, i);
            let label_follows = n.is_some_and(|n| match &ts[n] {
                T::Word(w) => !["NULL", "NOT", "TYPED", "NORMALIZED"]
                    .iter()
                    .any(|k| w.eq_ignore_ascii_case(k)),
                T::Punct('%' | '!') => true,
                _ => false,
            });
            if opener.is_some() && label_follows {
                while matches!(out.last(), Some(T::Space(_))) {
                    out.pop();
                }
                out.push(T::Punct(':'));
                i = n.unwrap_or(i + 1);
                continue;
            }
        }
        if depth == 0 {
            if is_kw(t, "FILTER") && !next(&ts, i).is_some_and(|n| ts[n] == T::Punct('(')) {
                out.push(T::Word("WITH * WHERE".into()));
                i += 1;
                continue;
            }
            if is_kw(t, "INSERT") {
                out.push(T::Word("CREATE".into()));
                i += 1;
                continue;
            }
            if is_kw(t, "OFFSET") {
                out.push(T::Word("SKIP".into()));
                i += 1;
                continue;
            }
            if is_kw(t, "RETURN") {
                last_return = Some(out.len());
            }
            if is_kw(t, "NEXT") {
                let Some(r) = last_return.take() else {
                    return Err(SprigError::new(
                        SprigCode::GraphSyntax,
                        "NEXT must follow a RETURN",
                    ));
                };
                out[r] = T::Word("WITH".into());
                while matches!(out.last(), Some(T::Space(_))) {
                    out.pop();
                }
                i += 1;
                continue;
            }
            if is_kw(t, "MATCH") {
                out.push(t.clone());
                if let Some(n) = next(&ts, i) {
                    for m in ["WALK", "ACYCLIC", "SIMPLE"] {
                        if is_kw(&ts[n], m) {
                            return Err(unsupported(&format!(
                                "the {m} path mode isn't supported; Cypher's matching (TRAIL) is"
                            )));
                        }
                    }
                    if is_kw(&ts[n], "TRAIL") {
                        i = n + 1;
                        continue;
                    }
                }
                i += 1;
                continue;
            }
            if is_kw(t, "LET") {
                let end = clause_end(&ts, i + 1);
                let mut items = Vec::new();
                for part in split_commas(&ts[i + 1..end]) {
                    let eq = part.iter().position(|x| *x == T::Punct('='));
                    let (Some(eq), Some(name)) =
                        (eq, part.iter().find(|x| matches!(x, T::Word(_))).cloned())
                    else {
                        return Err(SprigError::new(
                            SprigCode::GraphSyntax,
                            "LET needs name = expression",
                        ));
                    };
                    let expr = render(&part[eq + 1..]);
                    items.push(format!("{} AS {}", expr.trim(), render(&[name])));
                }
                out.push(T::Word(format!("WITH *, {} ", items.join(", "))));
                i = end;
                continue;
            }
            // `FOR x IN list` (not `FOR (n:L)` of index commands).
            let for_in = next(&ts, i)
                .filter(|&n| matches!(ts[n], T::Word(_)))
                .and_then(|n| next(&ts, n))
                .is_some_and(|m| is_kw(&ts[m], "IN"));
            if is_kw(t, "FOR") && for_in {
                let end = clause_end(&ts, i + 1);
                let body = &ts[i + 1..end];
                let in_at = body.iter().position(|x| is_kw(x, "IN"));
                let (Some(in_at), Some(var)) = (
                    in_at,
                    body.iter().find(|x| matches!(x, T::Word(_))).cloned(),
                ) else {
                    return Err(SprigError::new(
                        SprigCode::GraphSyntax,
                        "FOR needs name IN list",
                    ));
                };
                let list = render(&body[in_at + 1..]);
                out.push(T::Word(format!(
                    "UNWIND {} AS {} ",
                    list.trim(),
                    render(&[var])
                )));
                i = end;
                continue;
            }
        }
        out.push(t.clone());
        i += 1;
    }
    Ok(render(&out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_each_form() {
        let r = |s: &str| rewrite(s).expect(s);
        assert_eq!(
            r("MATCH (n IS Person) RETURN n"),
            "MATCH (n:Person) RETURN n"
        );
        assert_eq!(
            r("MATCH (a IS Person)-[k IS KNOWS|LIKES]->(IS Post) RETURN a"),
            "MATCH (a:Person)-[k:KNOWS|LIKES]->(:Post) RETURN a"
        );
        assert_eq!(
            r("MATCH (n) WHERE n.x IS NULL AND n.y IS NOT NULL RETURN n"),
            "MATCH (n) WHERE n.x IS NULL AND n.y IS NOT NULL RETURN n"
        );
        assert_eq!(
            r("MATCH (n) FILTER n.age > 3 RETURN n"),
            "MATCH (n) WITH * WHERE n.age > 3 RETURN n"
        );
        assert_eq!(
            r("MATCH (n) LET a = n.x + 1, b = 2 RETURN a, b"),
            "MATCH (n) WITH *, n.x + 1 AS a, 2 AS b RETURN a, b"
        );
        assert_eq!(r("FOR x IN [1, 2] RETURN x"), "UNWIND [1, 2] AS x RETURN x");
        assert_eq!(r("INSERT (:P {k: 1})"), "CREATE (:P {k: 1})");
        assert_eq!(
            r("MATCH (n) RETURN n.k AS k NEXT RETURN count(k) AS c"),
            "MATCH (n) WITH n.k AS k RETURN count(k) AS c"
        );
        assert_eq!(
            r("MATCH (n) RETURN n ORDER BY n.k OFFSET 1 LIMIT 2"),
            "MATCH (n) RETURN n ORDER BY n.k SKIP 1 LIMIT 2"
        );
        assert_eq!(
            r("MATCH TRAIL p = (a)-[*1..2]->(b) RETURN p"),
            "MATCH p = (a)-[*1..2]->(b) RETURN p"
        );
        assert_eq!(r("RETURN 'IS FILTER LET'"), "RETURN 'IS FILTER LET'");
        assert!(rewrite("MATCH WALK p = (a)-->(b) RETURN p").is_err());
        let ddl = "CREATE INDEX FOR (n:P) ON (n.k)";
        assert_eq!(r(ddl), ddl);
    }
}
