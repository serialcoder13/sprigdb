//! MySQL 8.0's row syntax that MariaDB 11.8 lacks, rewritten on port 3306
//! (chapter 28's rule catalogue, build step 7):
//!
//! - `TABLE t ...` at the start of a statement becomes `SELECT * FROM t ...`.
//! - `INSERT ... VALUES ROW(1, 2), ROW(3, 4)` loses the `ROW` keywords.
//! - The row alias of `INSERT ... VALUES (...) AS new [(c1, ...)] ON
//!   DUPLICATE KEY UPDATE ...` (and of `INSERT ... SET ... AS new`), missing
//!   in MariaDB (MDEV-35586): the alias goes, and in `ON DUPLICATE KEY
//!   UPDATE` each `new.col`, and each column alias `c1` on its own, becomes
//!   `VALUE(col)` (MariaDB's name for MySQL's deprecated `VALUES(col)`).
//!   With column aliases the columns are taken from the statement's column
//!   list, which must then be given; otherwise the statement is left as is.
//!
//! `None` when nothing changes.

use crate::lex::{LexOptions, Token, TokenKind, lex};

/// Rewrites one statement (see the module docs).
#[must_use]
#[allow(clippy::too_many_lines)] // one walk over the tokens
pub fn rewrite(sql: &[u8], opts: &LexOptions) -> Option<Vec<u8>> {
    let lower = sql.to_ascii_lowercase();
    let has = |w: &[u8]| lower.windows(w.len()).any(|x| x == w);
    if !(has(b"table") || has(b"row") || has(b"as")) {
        return None;
    }
    let tokens = lex(sql, opts).ok()?;
    let sig: Vec<Token> = tokens.into_iter().filter(|t| !t.kind.is_trivia()).collect();
    let bytes = |j: usize| sig.get(j).map(|t| t.bytes(sql)).unwrap_or_default();
    let is_word = |j: usize, w: &[u8]| {
        sig.get(j)
            .is_some_and(|t| t.kind == TokenKind::Word && t.bytes(sql).eq_ignore_ascii_case(w))
    };
    let is_punct = |j: usize, c: u8| {
        sig.get(j)
            .is_some_and(|t| t.kind == TokenKind::Punct && t.bytes(sql) == [c])
    };
    let is_ident = |j: usize| {
        sig.get(j)
            .is_some_and(|t| matches!(t.kind, TokenKind::Word | TokenKind::QuotedIdent))
    };
    let ident = |j: usize| -> String {
        let b = bytes(j);
        let s = String::from_utf8_lossy(b);
        if b.first() == Some(&b'`') && b.len() >= 2 {
            s[1..s.len() - 1].replace("``", "`")
        } else {
            s.into_owned()
        }
    };
    let mut edits: Vec<(usize, usize, String)> = Vec::new();

    // VALUES ROW(..), ROW(..): a union of one-row selects, columns
    // column_0, column_1, ... as MySQL names them.
    if is_word(0, b"VALUES") && is_word(1, b"ROW") {
        return values_statement(sql, &sig);
    }
    // TABLE t
    if is_word(0, b"TABLE") && is_ident(1) {
        edits.push((
            sig[0].start as usize,
            sig[0].end as usize,
            "SELECT * FROM".to_owned(),
        ));
        return apply(sql, edits);
    }
    if !(is_word(0, b"INSERT") || is_word(0, b"REPLACE")) {
        return None;
    }
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
    let top = |j: usize| depth_at[j] == 0;
    let find = |from: usize, w: &[u8]| (from..sig.len()).find(|&j| top(j) && is_word(j, w));
    if find(0, b"SELECT").is_some() {
        return None;
    }
    let values = find(1, b"VALUES").or_else(|| find(1, b"VALUE"));
    let set = find(1, b"SET");
    // ROW( ... ) in a VALUES list.
    if let Some(v) = values {
        for (j, t) in sig.iter().enumerate().skip(v + 1) {
            if top(j)
                && is_word(j, b"ROW")
                && is_punct(j + 1, b'(')
                && (j == v + 1 || is_punct(j - 1, b','))
            {
                edits.push((t.start as usize, t.end as usize, String::new()));
            }
        }
    }
    // The row alias.
    let list_start = values.or(set)?;
    let odku = (list_start..sig.len()).find(|&j| {
        top(j)
            && is_word(j, b"ON")
            && is_word(j + 1, b"DUPLICATE")
            && is_word(j + 2, b"KEY")
            && is_word(j + 3, b"UPDATE")
    });
    let Some(as_at) =
        (list_start + 1..odku.unwrap_or(sig.len())).find(|&j| top(j) && is_word(j, b"AS"))
    else {
        return apply(sql, edits);
    };
    // MySQL 8.4 refuses a row alias after `VALUES ROW(..)` (1064, measured;
    // Connector/J's Bug#33917022): left for MariaDB, which refuses it too.
    if !edits.is_empty() {
        return None;
    }
    if !is_ident(as_at + 1) {
        return None;
    }
    let alias = ident(as_at + 1).to_ascii_lowercase();
    let mut alias_cols: Vec<String> = Vec::new();
    let mut alias_end = as_at + 1;
    if is_punct(as_at + 2, b'(') {
        let mut k = as_at + 3;
        while k < sig.len() && !is_punct(k, b')') {
            if is_ident(k) {
                alias_cols.push(ident(k).to_ascii_lowercase());
            }
            k += 1;
        }
        alias_end = k;
    }
    // The statement's columns, in order: `(c1, ...)` before VALUES, or the
    // SET targets.
    let mut cols: Vec<String> = Vec::new();
    if let Some(v) = values {
        if v > 0 && is_punct(v - 1, b')') {
            let mut k = v - 1;
            while k > 0 && !(is_punct(k, b'(') && depth_at[k] == 0) {
                k -= 1;
            }
            for c in k + 1..v - 1 {
                if is_ident(c) && (is_punct(c + 1, b',') || c + 1 == v - 1) {
                    cols.push(ident(c));
                }
            }
        }
    } else if let Some(s) = set {
        let mut k = s + 1;
        while k < as_at {
            if is_ident(k) && is_punct(k + 1, b'=') && (k == s + 1 || is_punct(k - 1, b',')) {
                cols.push(ident(k));
            }
            k += 1;
        }
    }
    // Column aliases need the column list, unless nothing reads them.
    if !alias_cols.is_empty() && alias_cols.len() != cols.len() && odku.is_some() {
        return None;
    }
    let target = |name: &str| -> Option<String> {
        if alias_cols.is_empty() {
            Some(name.to_owned())
        } else {
            alias_cols
                .iter()
                .position(|a| a.eq_ignore_ascii_case(name))
                .map(|i| cols[i].clone())
        }
    };
    let quote = |c: &str| format!("`{}`", c.replace('`', "``"));
    edits.push((
        sig[as_at].start as usize,
        sig[alias_end].end as usize,
        String::new(),
    ));
    if let Some(o) = odku {
        let mut k = o + 4;
        while k < sig.len() {
            // alias.col
            if is_ident(k)
                && ident(k).eq_ignore_ascii_case(&alias)
                && is_punct(k + 1, b'.')
                && is_ident(k + 2)
            {
                let col = target(&ident(k + 2))?;
                edits.push((
                    sig[k].start as usize,
                    sig[k + 2].end as usize,
                    format!("VALUE({})", quote(&col)),
                ));
                k += 3;
                continue;
            }
            // A column alias on its own, not an assignment's target.
            let assigned =
                is_punct(k + 1, b'=') && (k == o + 4 || (is_punct(k - 1, b',') && top(k)));
            let qualified = is_punct(k + 1, b'.') || (k > 0 && is_punct(k - 1, b'.'));
            let alone = is_ident(k) && !assigned && !qualified && !is_punct(k + 1, b'(');
            if !alias_cols.is_empty()
                && alone
                && let Some(col) = target(&ident(k))
            {
                edits.push((
                    sig[k].start as usize,
                    sig[k].end as usize,
                    format!("VALUE({})", quote(&col)),
                ));
            }
            k += 1;
        }
    }
    apply(sql, edits)
}

/// `VALUES ROW(a, b), ROW(c, d)` as `SELECT a AS column_0, b AS column_1
/// UNION ALL SELECT c, d`; `None` for anything more (a trailing clause).
fn values_statement(sql: &[u8], sig: &[Token]) -> Option<Vec<u8>> {
    let punct = |j: usize, c: u8| {
        sig.get(j)
            .is_some_and(|t| t.kind == TokenKind::Punct && t.bytes(sql) == [c])
    };
    let row = |j: usize| {
        sig.get(j)
            .is_some_and(|t| t.kind == TokenKind::Word && t.bytes(sql).eq_ignore_ascii_case(b"ROW"))
    };
    let mut rows: Vec<Vec<String>> = Vec::new();
    let mut j = 1;
    while row(j) && punct(j + 1, b'(') {
        let mut depth = 0usize;
        let mut k = j + 1;
        let mut from = j + 2;
        let mut vals = Vec::new();
        loop {
            if punct(k, b'(') {
                depth += 1;
            } else if punct(k, b')') {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            } else if punct(k, b',') && depth == 1 {
                vals.push(from..k);
                from = k + 1;
            }
            k += 1;
            if k >= sig.len() {
                return None;
            }
        }
        vals.push(from..k);
        let text = |r: &std::ops::Range<usize>| -> Option<String> {
            let (a, b) = (sig.get(r.start)?, sig.get(r.end.checked_sub(1)?)?);
            Some(String::from_utf8_lossy(&sql[a.start as usize..b.end as usize]).into_owned())
        };
        rows.push(vals.iter().map(text).collect::<Option<Vec<_>>>()?);
        j = k + 1;
        if punct(j, b',') {
            j += 1;
        } else {
            break;
        }
    }
    let end = j == sig.len() || (punct(j, b';') && j + 1 == sig.len());
    if rows.is_empty() || !end || rows.iter().any(|r| r.len() != rows[0].len()) {
        return None;
    }
    let mut out = String::new();
    for (i, r) in rows.iter().enumerate() {
        out.push_str(if i == 0 {
            "SELECT "
        } else {
            " UNION ALL SELECT "
        });
        let cols: Vec<String> = r
            .iter()
            .enumerate()
            .map(|(c, v)| {
                if i == 0 {
                    format!("{v} AS column_{c}")
                } else {
                    v.clone()
                }
            })
            .collect();
        out.push_str(&cols.join(", "));
    }
    Some(out.into_bytes())
}

fn apply(sql: &[u8], mut edits: Vec<(usize, usize, String)>) -> Option<Vec<u8>> {
    if edits.is_empty() {
        return None;
    }
    edits.sort_by_key(|e| e.0);
    let mut out = Vec::with_capacity(sql.len() + 32);
    let mut at = 0;
    for (a, b, ins) in edits {
        if a < at {
            return None;
        }
        out.extend_from_slice(&sql[at..a]);
        out.extend_from_slice(ins.as_bytes());
        at = b;
    }
    out.extend_from_slice(&sql[at..]);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(q: &str) -> Option<String> {
        rewrite(q.as_bytes(), &LexOptions::mysql84()).map(|v| String::from_utf8(v).unwrap())
    }

    #[test]
    fn table_statement() {
        assert_eq!(
            r("TABLE t ORDER BY a LIMIT 2").as_deref(),
            Some("SELECT * FROM t ORDER BY a LIMIT 2")
        );
        assert_eq!(r("CREATE TABLE t (a INT)"), None);
    }

    #[test]
    fn values_statement() {
        assert_eq!(
            r("VALUES ROW (1, 'a'), ROW (2, CONCAT('b', 'c'))").as_deref(),
            Some("SELECT 1 AS column_0, 'a' AS column_1 UNION ALL SELECT 2, CONCAT('b', 'c')")
        );
        assert_eq!(r("VALUES ROW(1) ORDER BY 1"), None);
    }

    #[test]
    fn values_row() {
        assert_eq!(
            r("INSERT INTO t VALUES ROW(1, 'a'), ROW(?, ?)").as_deref(),
            Some("INSERT INTO t VALUES (1, 'a'), (?, ?)")
        );
    }

    #[test]
    fn row_alias_after_values_row_is_refused_as_mysql_does() {
        assert_eq!(r("INSERT INTO t VALUES ROW(?, ?) AS new(v1, v2)"), None);
        assert_eq!(
            r("INSERT INTO t VALUES ROW(1, 2) AS n ON DUPLICATE KEY UPDATE b = n.b"),
            None
        );
    }

    #[test]
    fn row_alias() {
        assert_eq!(
            r("INSERT INTO t (a, b) VALUES (1, 2) AS new ON DUPLICATE KEY UPDATE b = new.b + 1")
                .as_deref(),
            Some("INSERT INTO t (a, b) VALUES (1, 2)  ON DUPLICATE KEY UPDATE b = VALUE(`b`) + 1")
        );
        assert_eq!(
            r("INSERT INTO t (a, b) VALUES (?, ?) AS new(v1, v2) ON DUPLICATE KEY UPDATE b = v1 + new.v2").as_deref(),
            Some("INSERT INTO t (a, b) VALUES (?, ?)  ON DUPLICATE KEY UPDATE b = VALUE(`a`) + VALUE(`b`)")
        );
        assert_eq!(
            r("INSERT INTO t SET a = 1, b = 2 AS n ON DUPLICATE KEY UPDATE b = n.a").as_deref(),
            Some("INSERT INTO t SET a = 1, b = 2  ON DUPLICATE KEY UPDATE b = VALUE(`a`)")
        );
        // Column aliases without a column list: left to MariaDB.
        assert_eq!(
            r("INSERT INTO t VALUES (1, 2) AS n(x, y) ON DUPLICATE KEY UPDATE b = x"),
            None
        );
        // INSERT ... SELECT keeps its aliases.
        assert_eq!(r("INSERT INTO t SELECT a AS b FROM s"), None);
    }
}
