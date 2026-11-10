//! MySQL 8.4 functions MariaDB 11.8 doesn't have, rewritten on port 3306
//! (chapter 28's rule catalogue, build step 7):
//!
//! - `ANY_VALUE(x)` becomes `(x)`: the backend runs without
//!   `ONLY_FULL_GROUP_BY` (the gateway checks it, [`super::groupby`]), so
//!   any column may be selected as is. The rewrite runs after that check,
//!   which needs the call.
//! - `UUID_TO_BIN(s)` becomes `UNHEX(REPLACE(s, '-', ''))`, and
//!   `UUID_TO_BIN(s, 1)` swaps the time-low and time-high parts as MySQL
//!   does.
//! - `BIN_TO_UUID(b)` becomes the lower-case, dashed hex of `b`, and
//!   `BIN_TO_UUID(b, 1)` swaps the parts back.
//! - `IS_UUID(s)` tests MySQL's three accepted forms (32 hex digits, the
//!   dashed form, and the dashed form in braces).
//!
//! Every form uses its argument once (the swaps are one `REGEXP_REPLACE`),
//! so a `?` parameter or a volatile argument (`UUID()`) stays one value,
//! and an argument may name the outer query's columns. A whole select item
//! without an alias keeps MySQL's column name, the call as the client
//! wrote it.
//!
//! Differences that remain: MySQL refuses a malformed UUID with 1411,
//! where these return `NULL`; and the swap flag must be a literal (`0`,
//! `1`, `TRUE`, `FALSE`), or the call is left alone.

use crate::lex::{LexOptions, Token, TokenKind, lex};

/// Calls of one function, rewritten innermost first; `None` when nothing
/// changes.
#[must_use]
pub fn rewrite(sql: &[u8], opts: &LexOptions) -> Option<Vec<u8>> {
    let lower = sql.to_ascii_lowercase();
    let has = |w: &[u8]| lower.windows(w.len()).any(|x| x == w);
    if !(has(b"any_value") || has(b"uuid_to_bin") || has(b"bin_to_uuid") || has(b"is_uuid")) {
        return None;
    }
    let mut cur: Option<Vec<u8>> = None;
    // Nested calls: each pass rewrites the calls with no target inside.
    for _ in 0..8 {
        match pass(cur.as_deref().unwrap_or(sql), *opts) {
            Some(next) => cur = Some(next),
            None => break,
        }
    }
    cur
}

const TARGETS: &[&[u8]] = &[b"ANY_VALUE", b"UUID_TO_BIN", b"BIN_TO_UUID", b"IS_UUID"];

/// Clauses that end a select list.
const ENDS_LIST: &[&[u8]] = &[
    b"FROM",
    b"INTO",
    b"WHERE",
    b"UNION",
    b"EXCEPT",
    b"INTERSECT",
    b"ORDER",
    b"GROUP",
    b"HAVING",
    b"LIMIT",
    b"WINDOW",
    b"FOR",
    b"LOCK",
];

#[allow(clippy::too_many_lines)] // one walk over the tokens
fn pass(sql: &[u8], opts: LexOptions) -> Option<Vec<u8>> {
    let tokens = lex(sql, &opts).ok()?;
    let sig: Vec<Token> = tokens.into_iter().filter(|t| !t.kind.is_trivia()).collect();
    let word_at = |j: usize| -> Option<&[u8]> {
        sig.get(j)
            .filter(|t| t.kind == TokenKind::Word)
            .map(|t| t.bytes(sql))
    };
    let is_word = |j: usize, w: &[u8]| word_at(j).is_some_and(|b| b.eq_ignore_ascii_case(w));
    let is_punct = |j: usize, c: u8| {
        sig.get(j)
            .is_some_and(|t| t.kind == TokenKind::Punct && t.bytes(sql) == [c])
    };
    let target = |j: usize| {
        word_at(j).is_some_and(|b| TARGETS.iter().any(|t| b.eq_ignore_ascii_case(t)))
            && is_punct(j + 1, b'(')
    };
    let mut depth_at = Vec::with_capacity(sig.len());
    let mut depth = 0usize;
    for j in 0..sig.len() {
        depth_at.push(depth);
        if is_punct(j, b'(') {
            depth += 1;
        } else if is_punct(j, b')') {
            depth = depth.saturating_sub(1);
        }
    }
    let mut in_list = vec![false; sig.len() + 2];
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    let mut j = 0;
    while j < sig.len() {
        let d = depth_at[j];
        if is_word(j, b"SELECT") {
            in_list[d] = true;
        } else if ENDS_LIST.iter().any(|w| is_word(j, w)) {
            in_list[d] = false;
        }
        if !target(j) {
            j += 1;
            continue;
        }
        // The matching `)` and the top-level commas.
        let mut k = j + 2;
        let mut commas = Vec::new();
        let mut inner_target = false;
        while k < sig.len() && !(is_punct(k, b')') && depth_at[k] == d + 1) {
            if is_punct(k, b',') && depth_at[k] == d + 1 {
                commas.push(k);
            }
            if target(k) {
                inner_target = true;
            }
            k += 1;
        }
        if k >= sig.len() {
            return None;
        }
        if inner_target {
            // Rewritten in a later pass, once its arguments are.
            j += 1;
            continue;
        }
        let text = |a: usize, b: usize| -> String {
            if a > b {
                return String::new();
            }
            String::from_utf8_lossy(&sql[sig[a].start as usize..sig[b].end as usize]).into_owned()
        };
        let mut args = Vec::new();
        let mut from = j + 2;
        for &c in &commas {
            args.push(text(from, c - 1));
            from = c + 1;
        }
        if from < k {
            args.push(text(from, k - 1));
        }
        let name = String::from_utf8_lossy(word_at(j).unwrap_or_default()).to_ascii_uppercase();
        let Some(body) = replacement(&name, &args, opts.no_backslash_escapes) else {
            j = k + 1;
            continue;
        };
        let item_start = j == 0
            || is_punct(j - 1, b',')
            || is_word(j - 1, b"SELECT")
            || is_word(j - 1, b"DISTINCT")
            || is_word(j - 1, b"ALL");
        let item_end = k + 1 >= sig.len()
            || is_punct(k + 1, b',')
            || is_punct(k + 1, b';')
            || (is_punct(k + 1, b')') && depth_at[k + 1] < d + 1)
            || ENDS_LIST.iter().any(|w| is_word(k + 1, w));
        let mut out = body;
        if in_list.get(d).copied().unwrap_or(false) && item_start && item_end {
            out.push_str(" AS `");
            out.push_str(&text(j, k).replace('`', "``"));
            out.push('`');
        }
        edits.push((sig[j].start as usize, sig[k].end as usize, out));
        j = k + 1;
    }
    if edits.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(sql.len() + 64);
    let mut at = 0;
    for (a, b, ins) in edits {
        out.extend_from_slice(&sql[at..a]);
        out.extend_from_slice(ins.as_bytes());
        at = b;
    }
    out.extend_from_slice(&sql[at..]);
    Some(out)
}

/// A literal swap flag: `Some(true)` for 1 / `TRUE`.
fn swap_flag(arg: Option<&String>) -> Option<bool> {
    match arg.map(|a| a.trim().to_ascii_uppercase()).as_deref() {
        None | Some("0" | "FALSE") => Some(false),
        Some("1" | "TRUE") => Some(true),
        _ => None,
    }
}

/// A regular-expression back reference in a string literal: `\\1`, or
/// `\1` under `NO_BACKSLASH_ESCAPES`.
fn backref(n: u8, nbe: bool) -> String {
    if nbe {
        format!("\\{n}")
    } else {
        format!("\\\\{n}")
    }
}

fn replacement(name: &str, args: &[String], nbe: bool) -> Option<String> {
    let r = |n| backref(n, nbe);
    match (name, args) {
        ("ANY_VALUE", [x]) => Some(format!("({x})")),
        ("UUID_TO_BIN", [s, rest @ ..]) if rest.len() <= 1 => {
            if swap_flag(rest.first())? {
                Some(format!(
                    "UNHEX(REGEXP_REPLACE(REPLACE({s}, '-', ''), '^(.{{8}})(.{{4}})(.{{4}})(.{{16}})$', '{}{}{}{}'))",
                    r(3),
                    r(2),
                    r(1),
                    r(4)
                ))
            } else {
                Some(format!("UNHEX(REPLACE({s}, '-', ''))"))
            }
        }
        ("BIN_TO_UUID", [b, rest @ ..]) if rest.len() <= 1 => {
            if swap_flag(rest.first())? {
                Some(format!(
                    "REGEXP_REPLACE(LOWER(HEX({b})), '^(.{{4}})(.{{4}})(.{{8}})(.{{4}})(.{{12}})$', '{}-{}-{}-{}-{}')",
                    r(3),
                    r(2),
                    r(1),
                    r(4),
                    r(5)
                ))
            } else {
                Some(format!(
                    "LOWER(INSERT(INSERT(INSERT(INSERT(HEX({b}), 9, 0, '-'), 14, 0, '-'), 19, 0, '-'), 24, 0, '-'))"
                ))
            }
        }
        ("IS_UUID", [s]) => {
            let h = |n: u8| format!("[0-9a-fA-F]{{{n}}}");
            let dashed = format!("{}-{}-{}-{}-{}", h(8), h(4), h(4), h(4), h(12));
            Some(format!(
                "(({s}) REGEXP '^({}|{dashed}|[{{]{dashed}[}}])$')",
                h(32)
            ))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(q: &str) -> Option<String> {
        rewrite(q.as_bytes(), &LexOptions::mysql84()).map(|v| String::from_utf8(v).unwrap())
    }

    #[test]
    fn any_value_becomes_its_argument() {
        assert_eq!(
            r("SELECT ANY_VALUE(b), a FROM t GROUP BY a").as_deref(),
            Some("SELECT (b) AS `ANY_VALUE(b)`, a FROM t GROUP BY a")
        );
        assert_eq!(
            r("SELECT a, any_value(b + 1) AS x FROM t GROUP BY a ORDER BY ANY_VALUE(c)").as_deref(),
            Some("SELECT a, (b + 1) AS x FROM t GROUP BY a ORDER BY (c)")
        );
        assert_eq!(
            r("SELECT CONCAT(ANY_VALUE(b), 'x'), (SELECT ANY_VALUE(c)) FROM t").as_deref(),
            Some("SELECT CONCAT((b), 'x'), (SELECT (c) AS `ANY_VALUE(c)`) FROM t")
        );
        assert_eq!(r("SELECT 'ANY_VALUE(b)' FROM t"), None);
        assert_eq!(r("SELECT a FROM t"), None);
    }

    #[test]
    fn uuid_functions() {
        assert_eq!(
            r("INSERT INTO t VALUES (UUID_TO_BIN(?))").as_deref(),
            Some("INSERT INTO t VALUES (UNHEX(REPLACE(?, '-', '')))")
        );
        assert_eq!(
            r("SELECT BIN_TO_UUID(id) FROM t").as_deref(),
            Some(
                "SELECT LOWER(INSERT(INSERT(INSERT(INSERT(HEX(id), 9, 0, '-'), 14, 0, '-'), 19, 0, '-'), 24, 0, '-')) AS `BIN_TO_UUID(id)` FROM t"
            )
        );
        // Nested: the inner call first; a swap evaluates its argument once.
        let s = r("SELECT BIN_TO_UUID(UUID_TO_BIN(UUID(), 1), 1) AS u").unwrap();
        assert_eq!(
            s,
            "SELECT REGEXP_REPLACE(LOWER(HEX(UNHEX(REGEXP_REPLACE(REPLACE(UUID(), '-', ''), \
             '^(.{8})(.{4})(.{4})(.{16})$', '\\\\3\\\\2\\\\1\\\\4')))), \
             '^(.{4})(.{4})(.{8})(.{4})(.{12})$', '\\\\3-\\\\2-\\\\1-\\\\4-\\\\5') AS u"
        );
        // A swap flag that isn't a literal: left alone.
        assert_eq!(r("SELECT UUID_TO_BIN(u, @f) FROM t"), None);
        assert!(
            r("SELECT IS_UUID(x) FROM t")
                .unwrap()
                .contains("AS `IS_UUID(x)`")
        );
    }
}
