//! The translator's R0 subset (chapter 28, build steps 1 to 5): what lets
//! MySQL and MariaDB clients connect and introspect through either persona.
//!
//! - Step 1, executable comments per persona: [`crate::lex::versioned`]
//!   (the lexer's rules and neutralization) and
//!   [`crate::lex::versioned::expand_for_mariadb`] (MySQL-only comments whose
//!   body MariaDB understands).
//! - Step 2, emulated variables and version strings: [`vars`].
//! - Step 3, the collation map with metadata ids: [`collation`].
//! - Step 4, the core `information_schema` providers and `SHOW` rewrites:
//!   [`schema`] (persona shapes on top of chapter 07's inline views), and
//!   [`meta`] for result metadata (MySQL's `JSON` type, 245).
//! - Step 5, error mapping: [`errmap`].
//!
//! The rule framework, translation cache and the R4 rule catalogue come
//! with step 6 (`sprig_sql::translate`).

pub mod collation;
pub mod errmap;
pub mod keywords;
pub mod meta;
pub mod schema;
pub mod vars;

use crate::lex::{self, LexOptions, TokenKind};

/// `CAST(<operand> AS JSON)` with a parameter, a string or number literal
/// or `NULL` as the operand (a client-side prepared statement inlines the
/// parameter) becomes `JSON_COMPACT(<operand>)` (chapter 28, "Queries and
/// expressions": MariaDB has no `JSON` cast; Hibernate's `MySQLDialect`
/// binds every JSON value this way). Other operands are left for R4's
/// rule. `None` when nothing changes.
#[must_use]
pub fn rewrite_json_casts(sql: &[u8], opts: &LexOptions) -> Option<Vec<u8>> {
    let lower_has = sql.windows(4).any(|w| w.eq_ignore_ascii_case(b"json"));
    if !lower_has {
        return None;
    }
    let tokens = lex::lex(sql, opts).ok()?;
    let sig: Vec<lex::Token> = tokens.into_iter().filter(|t| !t.kind.is_trivia()).collect();
    let word = |j: usize, w: &[u8]| {
        sig.get(j)
            .is_some_and(|t| t.kind == TokenKind::Word && t.bytes(sql).eq_ignore_ascii_case(w))
    };
    let punct = |j: usize, c: u8| {
        sig.get(j)
            .is_some_and(|t| t.kind == TokenKind::Punct && sql[t.start as usize] == c)
    };
    let mut edits: Vec<(usize, usize, usize, usize)> = Vec::new();
    let mut j = 0;
    while j < sig.len() {
        if word(j, b"CAST")
            && punct(j + 1, b'(')
            && sig.get(j + 2).is_some_and(|t| {
                matches!(
                    t.kind,
                    TokenKind::Param
                        | TokenKind::String
                        | TokenKind::NationalString
                        | TokenKind::Number
                ) || (t.kind == TokenKind::Word && t.bytes(sql).eq_ignore_ascii_case(b"NULL"))
            })
            && word(j + 3, b"AS")
            && word(j + 4, b"JSON")
            && punct(j + 5, b')')
        {
            let op = sig[j + 2];
            edits.push((
                sig[j].start as usize,
                sig[j + 5].end as usize,
                op.start as usize,
                op.end as usize,
            ));
            j += 6;
        } else {
            j += 1;
        }
    }
    if edits.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(sql.len() + 8);
    let mut at = 0;
    for (start, end, op_start, op_end) in edits {
        out.extend_from_slice(&sql[at..start]);
        out.extend_from_slice(b"JSON_COMPACT(");
        out.extend_from_slice(&sql[op_start..op_end]);
        out.push(b')');
        at = end;
    }
    out.extend_from_slice(&sql[at..]);
    Some(out)
}

/// The text rewrites the MySQL persona applies before name planning:
/// executable comments MariaDB would skip ([`lex::versioned::expand_text_for_mariadb`])
/// and `CAST(? AS JSON)` ([`rewrite_json_casts`]). `None` on the MariaDB
/// persona and when nothing changes.
#[must_use]
pub fn mysql_text_rewrites(sql: &[u8], opts: &LexOptions, backend_version: u32) -> Option<Vec<u8>> {
    if opts.persona != sprig_proto_mysql::packets::Persona::MySql84 {
        return None;
    }
    let expanded = lex::versioned::expand_text_for_mariadb(sql, opts, backend_version);
    let cur = expanded.as_deref().unwrap_or(sql);
    match rewrite_json_casts(cur, opts) {
        Some(c) => Some(c),
        None => expanded,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_casts_of_parameters_and_strings() {
        let o = LexOptions::mysql84();
        let r = |s: &str| {
            mysql_text_rewrites(s.as_bytes(), &o, 999_999).map(|v| String::from_utf8(v).unwrap())
        };
        assert_eq!(
            r("insert into t (a,j,b) values (?,cast(? as json),?)").as_deref(),
            Some("insert into t (a,j,b) values (?,JSON_COMPACT(?),?)")
        );
        assert_eq!(
            r("SELECT CAST('[1, 2]' AS JSON), CAST(c AS JSON)").as_deref(),
            Some("SELECT JSON_COMPACT('[1, 2]'), CAST(c AS JSON)")
        );
        assert_eq!(r("SELECT 'CAST(? AS JSON)'"), None);
        assert_eq!(
            r("values (cast(null as json), CAST(1 AS JSON))").as_deref(),
            Some("values (JSON_COMPACT(null), JSON_COMPACT(1))")
        );
        assert_eq!(
            r("CREATE TABLE t (k int /*!80023 INVISIBLE */, j json DEFAULT (CAST('{}' AS JSON)))")
                .as_deref(),
            Some("CREATE TABLE t (k int INVISIBLE, j json DEFAULT (JSON_COMPACT('{}')))")
        );
        assert_eq!(
            mysql_text_rewrites(b"SELECT CAST(? AS JSON)", &LexOptions::mariadb(), 999_999),
            None
        );
    }
}
