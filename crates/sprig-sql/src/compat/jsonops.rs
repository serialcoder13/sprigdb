//! MySQL's JSON operators on port 3306.
//!
//! - **`c->'$.p'` and `c->>'$.p'`.** MariaDB before 13.1 has neither and
//!   fails with 1064. In MySQL the left side is always a column (perhaps
//!   qualified) and the right side a string literal, so the rewrite is
//!   exact: `JSON_EXTRACT(c, '$.p')` and
//!   `JSON_UNQUOTE(JSON_EXTRACT(c, '$.p'))`, as MySQL itself defines them.
//!   A whole select item without an alias keeps MySQL's column name
//!   (`c->>'$.p'`). Everywhere else the operators appear (generated
//!   columns, functional key parts, checks, stored bodies) the same text
//!   goes in: Django's `KT()` generated fields and JSON-key indexes need it.
//! - **A JSON string constant in a comparison.** MySQL compares
//!   `JSON_EXTRACT(c, '$.k') = JSON_EXTRACT('"stable"', '$')` as two JSON
//!   values: equal. MariaDB 11.8 unquotes one side and not the other, so
//!   two equal strings compare unequal (Django's JSON key lookups and
//!   check constraints build exactly this). When `JSON_EXTRACT(<string
//!   literal>, '$')`, whose literal is a JSON string, is an operand of `=`,
//!   `<>`, `!=` or `<=>`, or an item of an `IN` list, it becomes
//!   `JSON_UNQUOTE(..)`: MariaDB then compares the unquoted texts, which
//!   is MySQL's answer for strings. An `IN` list compares as plain
//!   strings in MariaDB, so `JSON_EXTRACT(c, p) IN (..)` whose items are
//!   all such constants unquotes the left side too (a JSON number `1` then
//!   matches the string `"1"`, which MySQL wouldn't). Numbers, booleans, `null`, objects and
//!   arrays already compare alike on both servers and are left alone.

use crate::lex::{self, LexOptions, Token, TokenKind};

fn sig(sql: &[u8], opts: LexOptions) -> Option<Vec<Token>> {
    let tokens = lex::lex(sql, &opts).ok()?;
    Some(tokens.into_iter().filter(|t| !t.kind.is_trivia()).collect())
}

fn apply(sql: &[u8], mut edits: Vec<(usize, usize, String)>) -> Vec<u8> {
    edits.sort_by_key(|e| e.0);
    let mut out = Vec::with_capacity(sql.len() + 32 * edits.len());
    let mut at = 0;
    for (from, to, text) in edits {
        out.extend_from_slice(&sql[at..from]);
        out.extend_from_slice(text.as_bytes());
        at = to;
    }
    out.extend_from_slice(&sql[at..]);
    out
}

/// `c->'p'` and `c->>'p'` as `JSON_EXTRACT` (see the module docs). `None`
/// when nothing changes.
#[must_use]
pub fn rewrite_arrows(sql: &[u8], opts: &LexOptions) -> Option<Vec<u8>> {
    if !sql.windows(2).any(|w| w == b"->") {
        return None;
    }
    let t = sig(sql, *opts)?;
    let op = |j: usize, s: &[u8]| {
        t.get(j)
            .is_some_and(|x| x.kind == TokenKind::Op && x.bytes(sql) == s)
    };
    let dot = |j: usize| {
        t.get(j)
            .is_some_and(|x| x.kind == TokenKind::Punct && x.bytes(sql) == b".")
    };
    let ident = |j: usize| t.get(j).is_some_and(|x| x.kind.is_ident_like());
    let mut edits = Vec::new();
    for j in 1..t.len() {
        let unquote = op(j, b"->>");
        if !(unquote || op(j, b"->")) {
            continue;
        }
        if !t.get(j + 1).is_some_and(|x| x.kind == TokenKind::String) || !ident(j - 1) {
            continue;
        }
        // `c`, `t.c` or `d.t.c`.
        let mut k = j - 1;
        for _ in 0..2 {
            if k >= 2 && dot(k - 1) && ident(k - 2) {
                k -= 2;
            } else {
                break;
            }
        }
        if k > 0 && dot(k - 1) {
            continue;
        }
        let col = String::from_utf8_lossy(&sql[t[k].start as usize..t[j - 1].end as usize]);
        let path = String::from_utf8_lossy(t[j + 1].bytes(sql));
        let mut rep = if unquote {
            format!("JSON_UNQUOTE(JSON_EXTRACT({col}, {path}))")
        } else {
            format!("JSON_EXTRACT({col}, {path})")
        };
        if super::perfschema::whole_select_item(sql, &t, k, j + 1) {
            let written = String::from_utf8_lossy(&sql[t[k].start as usize..t[j + 1].end as usize]);
            rep = format!("{rep} AS `{}`", written.replace('`', "``"));
        }
        edits.push((t[k].start as usize, t[j + 1].end as usize, rep));
    }
    (!edits.is_empty()).then(|| apply(sql, edits))
}

/// A JSON string constant compared with `JSON_EXTRACT(..)` as
/// `JSON_UNQUOTE(..)` (see the module docs). `None` when nothing changes.
#[must_use]
#[allow(clippy::too_many_lines)] // the IN-list and comparison forms
pub fn rewrite_string_constants(sql: &[u8], opts: &LexOptions) -> Option<Vec<u8>> {
    if !sql
        .windows(12)
        .any(|w| w.eq_ignore_ascii_case(b"json_extract"))
    {
        return None;
    }
    let t = sig(sql, *opts)?;
    let word = |j: usize, w: &[u8]| {
        t.get(j)
            .is_some_and(|x| x.kind == TokenKind::Word && x.bytes(sql).eq_ignore_ascii_case(w))
    };
    let punct = |j: usize, c: u8| {
        t.get(j)
            .is_some_and(|x| x.kind == TokenKind::Punct && x.bytes(sql) == [c])
    };
    let cmp = |j: usize| {
        t.get(j).is_some_and(|x| {
            x.kind == TokenKind::Op && matches!(x.bytes(sql), b"=" | b"<>" | b"!=" | b"<=>")
        })
    };
    let nbe = opts.no_backslash_escapes;
    // `JSON_EXTRACT('<JSON string>', '$')` at `j`.
    let string_const = |j: usize| {
        word(j, b"JSON_EXTRACT")
            && punct(j + 1, b'(')
            && t.get(j + 2).is_some_and(|x| x.kind == TokenKind::String)
            && punct(j + 3, b',')
            && t.get(j + 4).is_some_and(|x| {
                x.kind == TokenKind::String
                    && lex::string_value(x.bytes(sql), nbe).as_deref() == Some(b"$")
            })
            && punct(j + 5, b')')
            && lex::string_value(t[j + 2].bytes(sql), nbe).is_some_and(|lit| {
                let lit = String::from_utf8_lossy(&lit);
                let s = lit.trim();
                s.len() >= 2 && s.starts_with('"') && s.ends_with('"')
            })
    };
    let wrap = |a: usize, b: usize| {
        (
            t[a].start as usize,
            t[b].end as usize,
            format!(
                "JSON_UNQUOTE({})",
                String::from_utf8_lossy(&sql[t[a].start as usize..t[b].end as usize])
            ),
        )
    };
    let mut edits = Vec::new();
    let mut taken = vec![false; t.len()];
    // `JSON_EXTRACT(c, p) IN (<JSON string constants>)`: MariaDB compares
    // an IN list as strings without unquoting either side, so both go
    // unquoted (only when every item is a JSON string constant).
    for i in 1..t.len() {
        if !(word(i, b"IN") && punct(i + 1, b'(') && punct(i - 1, b')')) {
            continue;
        }
        // The left operand: `JSON_EXTRACT( ... )` ending at i - 1.
        let mut d = 0i32;
        let mut k = i - 1;
        let open = loop {
            if punct(k, b')') {
                d += 1;
            } else if punct(k, b'(') {
                d -= 1;
                if d == 0 {
                    break Some(k);
                }
            }
            if k == 0 {
                break None;
            }
            k -= 1;
        };
        let Some(open) = open else { continue };
        if open == 0 || !word(open - 1, b"JSON_EXTRACT") || (open >= 2 && punct(open - 2, b'.')) {
            continue;
        }
        let mut items = Vec::new();
        let mut j = i + 2;
        let ok = loop {
            if !string_const(j) {
                break false;
            }
            items.push(j);
            if punct(j + 6, b')') {
                break true;
            }
            if !punct(j + 6, b',') {
                break false;
            }
            j += 7;
        };
        if !ok {
            continue;
        }
        edits.push(wrap(open - 1, i - 1));
        for &a in &items {
            edits.push(wrap(a, a + 5));
            taken[a] = true;
        }
    }
    // A constant operand of `=`, `<>`, `!=` or `<=>`.
    for (j, &was_taken) in taken.iter().enumerate() {
        if was_taken || !string_const(j) {
            continue;
        }
        if !((j > 0 && cmp(j - 1)) || cmp(j + 6)) {
            continue;
        }
        edits.push(wrap(j, j + 5));
    }
    (!edits.is_empty()).then(|| apply(sql, edits))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arrows(s: &str) -> Option<String> {
        rewrite_arrows(s.as_bytes(), &LexOptions::mysql84()).map(|v| String::from_utf8(v).unwrap())
    }

    fn consts(s: &str) -> Option<String> {
        rewrite_string_constants(s.as_bytes(), &LexOptions::mysql84())
            .map(|v| String::from_utf8(v).unwrap())
    }

    #[test]
    fn arrows_become_json_extract() {
        // Django's KT() generated field (test_add_generated_field_with_kt_model).
        assert_eq!(
            arrows("CREATE TABLE g (`data` json NOT NULL, `status` longtext GENERATED ALWAYS AS ((`data` ->> '$.\"status\"')) STORED)").as_deref(),
            Some("CREATE TABLE g (`data` json NOT NULL, `status` longtext GENERATED ALWAYS AS ((JSON_UNQUOTE(JSON_EXTRACT(`data`, '$.\"status\"')))) STORED)")
        );
        // A JSON key in a functional key part.
        assert_eq!(
            arrows("CREATE INDEX i ON t ((CAST((`field` ->> '$.\"k\"') AS signed integer)))")
                .as_deref(),
            Some(
                "CREATE INDEX i ON t ((CAST((JSON_UNQUOTE(JSON_EXTRACT(`field`, '$.\"k\"'))) AS signed integer)))"
            )
        );
        // Qualified columns, `->`, and MySQL's column names.
        assert_eq!(
            arrows("SELECT t.j->'$.a', d.t.j->>'$.b' AS b FROM d.t WHERE j->>'$.c' = 'x'")
                .as_deref(),
            Some(
                "SELECT JSON_EXTRACT(t.j, '$.a') AS `t.j->'$.a'`, JSON_UNQUOTE(JSON_EXTRACT(d.t.j, '$.b')) AS b FROM d.t WHERE JSON_UNQUOTE(JSON_EXTRACT(j, '$.c')) = 'x'"
            )
        );
        assert_eq!(arrows("SELECT '->>', a - >b FROM t"), None);
        assert_eq!(arrows("SELECT a->? FROM t"), None);
    }

    #[test]
    fn json_string_constants_in_comparisons() {
        // Django's JSON key lookup and check (test_check_constraint_exact_jsonfield).
        assert_eq!(
            consts("ALTER TABLE m ADD CONSTRAINT c CHECK (JSON_EXTRACT(`data`, '$.\"version\"') = JSON_EXTRACT('\"stable\"', '$'))").as_deref(),
            Some("ALTER TABLE m ADD CONSTRAINT c CHECK (JSON_EXTRACT(`data`, '$.\"version\"') = JSON_UNQUOTE(JSON_EXTRACT('\"stable\"', '$')))")
        );
        assert_eq!(
            consts("SELECT * FROM m WHERE JSON_EXTRACT('\"a\"', '$') <> JSON_EXTRACT(d, '$.k') OR JSON_EXTRACT(d, '$.k') IN (JSON_EXTRACT('\"x\"', '$'), JSON_EXTRACT('1', '$'))").as_deref(),
            Some("SELECT * FROM m WHERE JSON_UNQUOTE(JSON_EXTRACT('\"a\"', '$')) <> JSON_EXTRACT(d, '$.k') OR JSON_EXTRACT(d, '$.k') IN (JSON_EXTRACT('\"x\"', '$'), JSON_EXTRACT('1', '$'))")
        );
        // Django's `__in` on a key: every item a JSON string.
        assert_eq!(
            consts("SELECT * FROM m WHERE JSON_EXTRACT(`d`, '$.\"k\"') IN (JSON_EXTRACT('\"x\"', '$'), JSON_EXTRACT('\"y\"', '$'))").as_deref(),
            Some("SELECT * FROM m WHERE JSON_UNQUOTE(JSON_EXTRACT(`d`, '$.\"k\"')) IN (JSON_UNQUOTE(JSON_EXTRACT('\"x\"', '$')), JSON_UNQUOTE(JSON_EXTRACT('\"y\"', '$')))")
        );
        // Not a comparison, or not a string: unchanged.
        assert_eq!(consts("SELECT JSON_EXTRACT('\"a\"', '$')"), None);
        assert_eq!(
            consts(
                "SELECT * FROM m WHERE JSON_EXTRACT(d, '$.k') = JSON_EXTRACT('{\"a\": 1}', '$')"
            ),
            None
        );
    }
}
