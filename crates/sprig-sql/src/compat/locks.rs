//! User-level locks per tenant.
//!
//! `GET_LOCK('jobs', 10)` names a lock in the server's one namespace, so
//! two tenants on the same cluster would wait on each other's locks (and
//! could probe them with `IS_USED_LOCK`). On both ports the name argument
//! of `GET_LOCK`, `RELEASE_LOCK`, `IS_FREE_LOCK` and `IS_USED_LOCK` becomes
//! the tenant's prefix plus a hash of the name:
//!
//! ```text
//! GET_LOCK(n, t)  ->  GET_LOCK(CONCAT('t42:', LEFT(SHA2(LOWER(n), 256), 50)), t)   -- 3306
//! GET_LOCK(n, t)  ->  GET_LOCK(CONCAT('t42:', LEFT(SHA2(n, 256), 50)), t)          -- 3307
//! ```
//!
//! - **The hash** keeps every name within MariaDB's 64 characters with the
//!   prefix, and uses the argument once, so a `?` parameter stays one
//!   parameter. 50 hex digits are 200 bits.
//! - **Case:** MySQL 8.4 compares lock names without case (measured:
//!   `IS_FREE_LOCK('abc')` is 0 while `'AbC'` is held), MariaDB 11.8 with
//!   case; 3306 hashes the lower-cased name.
//! - **A whole select item** without an alias keeps the call's text as its
//!   column name, as with [`super::funcs`].
//! - **Differences that remain:** an empty or `NULL` name gives `NULL`
//!   instead of MySQL's 3057, and a name over 64 characters is accepted
//!   (MySQL: 4163, MariaDB: 1059). `IS_USED_LOCK` returns the backend
//!   connection's id, not the gateway's. `RELEASE_ALL_LOCKS()` needs
//!   nothing: it releases the session's own.

use crate::lex::{LexOptions, Token, TokenKind, lex};
use crate::names::rewrite::sql_string;
use sprig_proto_mysql::Persona;

const TARGETS: &[&[u8]] = &[
    b"GET_LOCK",
    b"RELEASE_LOCK",
    b"IS_FREE_LOCK",
    b"IS_USED_LOCK",
];

/// Words that end a select item.
const ITEM_END: &[&[u8]] = &[
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

/// Words that may stand between `SELECT` and the first item.
const MODIFIERS: &[&[u8]] = &[
    b"SELECT",
    b"DISTINCT",
    b"ALL",
    b"DISTINCTROW",
    b"HIGH_PRIORITY",
    b"STRAIGHT_JOIN",
    b"SQL_SMALL_RESULT",
    b"SQL_BIG_RESULT",
    b"SQL_BUFFER_RESULT",
    b"SQL_NO_CACHE",
    b"SQL_CACHE",
    b"SQL_CALC_FOUND_ROWS",
];

/// The lock prefix of a tenant: `t<id>:`.
#[must_use]
pub fn prefix(tenant_id: u64) -> String {
    format!("t{tenant_id}:")
}

/// `sql` with each lock function's name argument scoped to `prefix` (see
/// the module docs), or `None` when it calls none.
#[must_use]
#[allow(clippy::too_many_lines, clippy::many_single_char_names)] // one walk over the tokens
pub fn rewrite(sql: &[u8], opts: &LexOptions, prefix: &str) -> Option<Vec<u8>> {
    let lower = sql.to_ascii_lowercase();
    if !lower.windows(5).any(|w| w == b"_lock") {
        return None;
    }
    let tokens = lex(sql, opts).ok()?;
    let sig: Vec<Token> = tokens.into_iter().filter(|t| !t.kind.is_trivia()).collect();
    let word = |j: usize, w: &[u8]| {
        sig.get(j)
            .is_some_and(|t| t.kind == TokenKind::Word && t.bytes(sql).eq_ignore_ascii_case(w))
    };
    let punct = |j: usize, c: u8| {
        sig.get(j).is_some_and(|t| {
            matches!(t.kind, TokenKind::Punct | TokenKind::Op) && t.bytes(sql) == [c]
        })
    };
    let lower_case = opts.persona == Persona::MySql84;
    let lit = sql_string(prefix, opts.no_backslash_escapes);
    let mut edits: Vec<(usize, usize, Vec<u8>)> = Vec::new();
    let mut select_list: Vec<bool> = vec![false];
    let mut j = 0;
    while j < sig.len() {
        if punct(j, b'(') {
            select_list.push(false);
            j += 1;
            continue;
        }
        if punct(j, b')') {
            if select_list.len() > 1 {
                select_list.pop();
            }
            j += 1;
            continue;
        }
        if punct(j, b';') {
            if let Some(s) = select_list.last_mut() {
                *s = false;
            }
            j += 1;
            continue;
        }
        if word(j, b"SELECT") {
            if let Some(s) = select_list.last_mut() {
                *s = true;
            }
            j += 1;
            continue;
        }
        if ITEM_END.iter().any(|w| word(j, w))
            && let Some(s) = select_list.last_mut()
        {
            *s = false;
        }
        let is_target = TARGETS.iter().any(|w| word(j, w))
            && punct(j + 1, b'(')
            && !(j > 0 && punct(j - 1, b'.'));
        if !is_target {
            j += 1;
            continue;
        }
        // The first argument: up to a `,` or `)` at its own depth.
        let start = j + 2;
        let mut depth = 0usize;
        let mut k = start;
        while k < sig.len() {
            if punct(k, b'(') {
                depth += 1;
            } else if punct(k, b')') {
                if depth == 0 {
                    break;
                }
                depth -= 1;
            } else if punct(k, b',') && depth == 0 {
                break;
            }
            k += 1;
        }
        if k >= sig.len() || k == start {
            return None;
        }
        // The call's closing parenthesis.
        let mut close = k;
        let mut d = 0usize;
        while close < sig.len() {
            if punct(close, b'(') {
                d += 1;
            } else if punct(close, b')') {
                if d == 0 {
                    break;
                }
                d -= 1;
            }
            close += 1;
        }
        if close >= sig.len() {
            return None;
        }
        let (a, b) = (sig[start].start as usize, sig[k - 1].end as usize);
        let arg = &sql[a..b];
        let mut rep = format!("CONCAT({lit}, LEFT(SHA2(").into_bytes();
        if lower_case {
            rep.extend_from_slice(b"LOWER(");
            rep.extend_from_slice(arg);
            rep.push(b')');
        } else {
            rep.extend_from_slice(arg);
        }
        rep.extend_from_slice(b", 256), 50))");
        edits.push((a, b, rep));
        // A whole select item keeps the call's text as its name.
        let in_list = select_list.last().copied().unwrap_or(false);
        let starts = j > 0 && (punct(j - 1, b',') || MODIFIERS.iter().any(|w| word(j - 1, w)));
        let n = close + 1;
        let ends = n >= sig.len()
            || punct(n, b',')
            || punct(n, b';')
            || punct(n, b')')
            || ITEM_END.iter().any(|w| word(n, w));
        if in_list && starts && ends {
            let text =
                String::from_utf8_lossy(&sql[sig[j].start as usize..sig[close].end as usize]);
            let alias = format!(" AS `{}`", text.replace('`', "``"));
            let at = sig[close].end as usize;
            edits.push((at, at, alias.into_bytes()));
        }
        // Calls nested in the argument are left alone (rare).
        j = close + 1;
    }
    if edits.is_empty() {
        return None;
    }
    edits.sort_by_key(|e| (e.0, e.1));
    let mut out = Vec::with_capacity(sql.len() + edits.len() * 64);
    let mut copied = 0;
    for (a, b, rep) in edits {
        if a < copied {
            return None;
        }
        out.extend_from_slice(&sql[copied..a]);
        out.extend_from_slice(&rep);
        copied = b;
    }
    out.extend_from_slice(&sql[copied..]);
    Some(out)
}

/// The text of a stored program or view as the client wrote it: every
/// scoped lock name ([`rewrite`]'s `CONCAT('t<id>:', LEFT(SHA2(..), 50))`)
/// back to its argument, and the column alias the rewrite added dropped
/// (otherwise `SHOW CREATE` and `information_schema` would show the
/// tenant's internal id). `None` when there is nothing to undo.
#[must_use]
pub fn unscope(text: &str) -> Option<String> {
    if !text.contains("CONCAT('t") || !text.contains(", 256), 50))") {
        return None;
    }
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut at = 0;
    let mut i = 0;
    let mut changed = false;
    while let Some(off) = text[i..].find("CONCAT('t") {
        let begin = i + off;
        let mut digits = begin + "CONCAT('t".len();
        while digits < bytes.len() && bytes[digits].is_ascii_digit() {
            digits += 1;
        }
        let head = ":', LEFT(SHA2(";
        if digits == begin + "CONCAT('t".len() || !text[digits..].starts_with(head) {
            i = begin + 1;
            continue;
        }
        let mut arg = digits + head.len();
        let lower = text[arg..].starts_with("LOWER(");
        if lower {
            arg += "LOWER(".len();
        }
        // The argument: up to the matching parenthesis.
        let mut depth = 0i32;
        let mut pos = arg;
        let mut quote: Option<u8> = None;
        while pos < bytes.len() {
            let ch = bytes[pos];
            match quote {
                Some(open) if ch == b'\\' && open != b'`' => pos += 1,
                Some(open) if ch == open => quote = None,
                None if matches!(ch, b'\'' | b'"' | b'`') => quote = Some(ch),
                None if ch == b'(' => depth += 1,
                None if ch == b')' && depth == 0 => break,
                None if ch == b')' => depth -= 1,
                None if ch == b',' && depth == 0 && !lower => break,
                _ => {}
            }
            pos += 1;
        }
        let arg_end = pos;
        let mut tail = pos;
        if lower {
            tail += 1; // `)` of LOWER
        }
        if !text[tail..].starts_with(", 256), 50))") {
            i = begin + 1;
            continue;
        }
        let end = tail + ", 256), 50))".len();
        out.push_str(&text[at..begin]);
        out.push_str(&text[arg..arg_end]);
        at = end;
        i = end;
        changed = true;
    }
    if !changed {
        return None;
    }
    out.push_str(&text[at..]);
    // The aliases the rewrite added: ` AS `F(arg, ..)`` right after `F(arg, ..)`.
    for f in ["GET_LOCK", "RELEASE_LOCK", "IS_FREE_LOCK", "IS_USED_LOCK"] {
        let mut s = 0;
        while let Some(p) = out[s..]
            .to_ascii_uppercase()
            .find(&format!("{f}("))
            .map(|p| p + s)
        {
            // The call's end.
            let ob = out.as_bytes();
            let mut d = 0i32;
            let mut e = p + f.len();
            while e < ob.len() {
                if ob[e] == b'(' {
                    d += 1;
                } else if ob[e] == b')' {
                    d -= 1;
                    if d == 0 {
                        break;
                    }
                }
                e += 1;
            }
            if e >= ob.len() {
                break;
            }
            let call = out[p..=e].to_owned();
            let alias = format!(" AS `{}`", call.replace('`', "``"));
            if out[e + 1..].starts_with(&alias) {
                out.replace_range(e + 1..e + 1 + alias.len(), "");
            }
            s = e + 1;
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_text_shows_the_written_names() {
        let body = "CREATE PROCEDURE lp() SELECT GET_LOCK(CONCAT('t2947606000000:', LEFT(SHA2(LOWER('jobs'), 256), 50)), 0) AS `GET_LOCK('jobs', 0)`, SLEEP(4), RELEASE_LOCK(CONCAT('t42:', LEFT(SHA2(@n, 256), 50))) x";
        assert_eq!(
            unscope(body).unwrap(),
            "CREATE PROCEDURE lp() SELECT GET_LOCK('jobs', 0), SLEEP(4), RELEASE_LOCK(@n) x"
        );
        assert_eq!(unscope("SELECT CONCAT('t1', 'x')"), None);
        // Round trip through the rewrite.
        let opts = LexOptions::for_persona(Persona::MySql84);
        let src = "CREATE FUNCTION f() RETURNS INT RETURN IS_FREE_LOCK(CONCAT('a', ')'))";
        let scoped = String::from_utf8(rewrite(src.as_bytes(), &opts, "t7:").unwrap()).unwrap();
        assert_eq!(unscope(&scoped).unwrap(), src);
    }

    fn rw(sql: &str, persona: Persona) -> String {
        let opts = LexOptions::for_persona(persona);
        String::from_utf8(rewrite(sql.as_bytes(), &opts, "t42:").unwrap_or_else(|| sql.into()))
            .unwrap()
    }

    #[test]
    fn names_are_scoped_and_items_keep_their_names() {
        assert_eq!(
            rw("SELECT GET_LOCK('jobs', 10)", Persona::MySql84),
            "SELECT GET_LOCK(CONCAT('t42:', LEFT(SHA2(LOWER('jobs'), 256), 50)), 10) AS `GET_LOCK('jobs', 10)`"
        );
        assert_eq!(
            rw(
                "select release_lock(?) as r, is_free_lock(@n)",
                Persona::MariaDb
            ),
            "select release_lock(CONCAT('t42:', LEFT(SHA2(?, 256), 50))) as r, \
             is_free_lock(CONCAT('t42:', LEFT(SHA2(@n, 256), 50))) AS `is_free_lock(@n)`"
        );
        // Not a whole item: no alias; an expression argument stays whole.
        assert_eq!(
            rw("DO IF(GET_LOCK(CONCAT('a', x), 0), 1, 0)", Persona::MariaDb),
            "DO IF(GET_LOCK(CONCAT('t42:', LEFT(SHA2(CONCAT('a', x), 256), 50)), 0), 1, 0)"
        );
        assert_eq!(
            rw("SELECT IS_USED_LOCK('a') FROM t", Persona::MariaDb),
            "SELECT IS_USED_LOCK(CONCAT('t42:', LEFT(SHA2('a', 256), 50))) AS `IS_USED_LOCK('a')` FROM t"
        );
    }

    #[test]
    fn other_text_is_left_alone() {
        for s in [
            "SELECT 'GET_LOCK(1)'",
            "SELECT RELEASE_ALL_LOCKS()",
            "SELECT get_lock FROM t",
            "SELECT db.get_lock('x')",
            "LOCK TABLES t WRITE",
        ] {
            assert!(
                rewrite(s.as_bytes(), &LexOptions::mysql84(), "t42:").is_none(),
                "{s}"
            );
        }
    }
}
