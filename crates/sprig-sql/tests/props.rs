//! Property tests (chapter 07, "Testing"): rewriting changes only name
//! bytes; random database names round-trip; re-parsing the rewritten text
//! gives the original AST with only schema parts changed.

#![allow(clippy::unwrap_used)]

mod common;

use std::ops::ControlFlow;

use proptest::prelude::*;
use sprig_common::ids::TenantId;
use sprig_proto_mysql::packets::Persona;
use sprig_sql::lex::{self, LexOptions, TokenKind};
use sprig_sql::names::{EditKind, Engine, MemoryNameMap, NameMap, RewriteCtx, rewrite, unrewrite};
use sqlparser::ast::{Ident, ObjectName, ObjectNamePart, VisitMut, VisitorMut};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

/// Database names: plain, with spaces, dots, backticks, quotes, unicode.
fn db_name() -> impl Strategy<Value = String> {
    prop_oneof![
        "[a-z][a-z0-9_]{0,12}",
        "[a-zA-Z0-9 ._`'\"$-]{1,16}",
        "[\u{80}-\u{2fff}]{1,6}",
    ]
    .prop_filter("not reserved or system", |n| {
        !sprig_common::names::is_reserved_database(n) && !n.trim().is_empty()
    })
}

fn backquote(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// Trivia between tokens.
fn trivia() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(" ".to_owned()),
        Just("\n\t".to_owned()),
        Just(" /* c */ ".to_owned()),
        Just(" -- line\n".to_owned()),
        Just(" #x\n".to_owned()),
        Just(" /*!50001 */ ".to_owned()),
    ]
}

fn statement() -> impl Strategy<Value = (String, String, Vec<String>)> {
    (
        db_name(),
        db_name(),
        proptest::collection::vec(trivia(), 8),
        0usize..6,
    )
        .prop_map(|(d1, d2, ws, shape)| {
            let (a, b) = (backquote(&d1), backquote(&d2));
            let w = |i: usize| ws[i].clone();
            let sql = match shape {
                0 => format!(
                    "SELECT{}*{}FROM{}{a}.t{}JOIN {b}.u ON t.x = u.x",
                    w(0),
                    w(1),
                    w(2),
                    w(3)
                ),
                1 => format!("INSERT INTO {a}.t{}SELECT {b}.u.c FROM{}{b}.u", w(0), w(1)),
                2 => format!(
                    "UPDATE {a}.t,{}{b}.u SET t.c = 'x{}y' WHERE{}u.k = {a}.f(1)",
                    w(0),
                    w(1),
                    w(2)
                ),
                3 => format!(
                    "DELETE FROM {a}.t WHERE id IN (SELECT id FROM{}{b}.u)",
                    w(0)
                ),
                4 => format!(
                    "CREATE PROCEDURE {a}.p() BEGIN{}SELECT 1 FROM {b}.u;{}END",
                    w(0),
                    w(1)
                ),
                _ => format!(
                    "WITH c AS (SELECT * FROM {a}.t) SELECT * FROM c,{}{b}.u",
                    w(0)
                ),
            };
            (sql, d1, vec![d2])
        })
}

fn map_for(names: &[&str]) -> MemoryNameMap {
    let mut m = MemoryNameMap::new(TenantId(7));
    for (i, n) in names.iter().enumerate() {
        let id = u32::try_from(i).unwrap() + 1;
        m = m.with_db(id, n, id, id);
    }
    m
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(400))]

    /// Only name tokens change, every other byte survives, and the reverse
    /// rewrite restores the original exactly.
    #[test]
    fn only_names_change((sql, d1, rest) in statement(), mariadb in any::<bool>()) {
        let d2 = &rest[0];
        let map = map_for(&[&d1, d2]);
        let lex_opts = if mariadb { LexOptions::mariadb() } else { LexOptions::mysql84() };
        for engine in [Engine::Lexer, Engine::Auto] {
            let ctx = RewriteCtx::new(&map, lex_opts.persona).lex(lex_opts).engine(engine);
            let r = rewrite(sql.as_bytes(), &ctx).unwrap();
            // Rebuild the output from the input and the edits.
            let mut out = Vec::new();
            let mut at = 0;
            for e in &r.edits {
                prop_assert!(e.range.start >= at);
                out.extend_from_slice(&sql.as_bytes()[at..e.range.start]);
                out.extend_from_slice(e.replacement.as_bytes());
                at = e.range.end;
                let EditKind::Schema { logical, .. } = &e.kind else {
                    return Err(TestCaseError::fail(format!("unexpected edit {e:?}")));
                };
                let orig = lex::ident_value(&sql.as_bytes()[e.range.clone()]);
                prop_assert_eq!(&*orig, logical.as_bytes());
                prop_assert!(logical == &d1 || logical == d2);
            }
            out.extend_from_slice(&sql.as_bytes()[at..]);
            prop_assert_eq!(&out, &r.sql);
            prop_assert!(!r.edits.is_empty());
            let back = unrewrite(&r.sql, &map as &dyn NameMap, &lex_opts);
            prop_assert_eq!(String::from_utf8(back).unwrap(), sql.clone());
        }
    }

    /// Arbitrary token soup never panics; when it rewrites, edits sit on
    /// whole tokens and nothing else moves.
    #[test]
    fn token_soup_is_safe(words in proptest::collection::vec(soup_word(), 0..40)) {
        let sql = words.join("");
        let map = map_for(&["billing", "crm"]);
        for persona in [Persona::MySql84, Persona::MariaDb] {
            let ctx = RewriteCtx::new(&map, persona).current_db(Some("billing"));
            if let Ok(r) = rewrite(sql.as_bytes(), &ctx) {
                let toks = lex::lex(sql.as_bytes(), &ctx.lex).unwrap();
                for e in &r.edits {
                    if matches!(e.kind, EditKind::Schema { .. }) {
                        prop_assert!(toks.iter().any(|t| t.range() == e.range
                            && matches!(t.kind, TokenKind::Word | TokenKind::QuotedIdent)));
                    }
                }
            }
        }
    }

    /// Chapter 07: re-parsing the rewritten text gives the same AST with
    /// only schema parts changed.
    #[test]
    fn reparse_changes_only_schemas(shape in 0usize..5, d in "[a-z]{1,8}", t in "[a-z]{1,8}") {
        prop_assume!(!sprig_common::names::is_reserved_database(&d));
        let sql = match shape {
            0 => format!("SELECT a FROM {d}.{t} JOIN {d}.x ON {t}.a = x.a WHERE a IN (SELECT b FROM {d}.y)"),
            1 => format!("INSERT INTO {d}.{t} (a) SELECT a FROM {d}.y"),
            2 => format!("UPDATE {d}.{t} SET a = 1 WHERE b = 2"),
            3 => format!("DELETE FROM {d}.{t} WHERE a = 1"),
            _ => format!("SELECT * FROM (SELECT * FROM {d}.{t}) AS s"),
        };
        let map = map_for(&[&d]);
        let ctx = RewriteCtx::new(&map, Persona::MySql84);
        let r = rewrite(sql.as_bytes(), &ctx).unwrap();
        let mut original = Parser::parse_sql(&MySqlDialect {}, &sql).unwrap();
        let rewritten = Parser::parse_sql(&MySqlDialect {}, &r.sql_lossy()).unwrap();
        let physical = map.physical(&d, sprig_sql::names::Purpose::Read).unwrap().single().unwrap();
        let mut swap = Swap { from: d.clone(), to: physical.to_string() };
        let _ = original.visit(&mut swap);
        prop_assert_eq!(strip(&format!("{original:?}")), strip(&format!("{rewritten:?}")));
    }
}

fn soup_word() -> impl Strategy<Value = String> {
    prop_oneof![
        prop::sample::select(vec![
            "SELECT ",
            "FROM ",
            "crm",
            "billing",
            "nope",
            ".",
            "*",
            ",",
            "(",
            ")",
            ";",
            " ",
            "`crm`",
            "'s'",
            "\"q\"",
            "--",
            "-- c\n",
            "/*",
            "*/",
            "/*!50001 ",
            "/*M!100100 ",
            "#",
            "JOIN ",
            "ON ",
            "WHERE ",
            "USE ",
            "CREATE ",
            "TABLE ",
            "PROCEDURE ",
            "BEGIN ",
            "END ",
            "IF ",
            "THEN ",
            "CASE ",
            "DATABASE()",
            "USER()",
            "CURRENT_USER",
            "INSERT INTO ",
            "UPDATE ",
            "SET ",
            "DELETE ",
            "GRANT ",
            "ALL ",
            "TO ",
            "LOCK TABLES ",
            "SHOW ",
            "TABLES ",
            "\\",
            "@",
            "@@",
            "?",
            "0x",
            "1",
            "e",
            "_sprig_x",
            "__sprig_t",
            "PREPARE s FROM ",
        ])
        .prop_map(str::to_owned),
        "[ -~]{1,3}",
    ]
}

/// Replaces database identifiers in relation positions.
struct Swap {
    from: String,
    to: String,
}

impl VisitorMut for Swap {
    type Break = ();
    fn pre_visit_relation(&mut self, relation: &mut ObjectName) -> ControlFlow<()> {
        if relation.0.len() == 2
            && let ObjectNamePart::Identifier(i) = &mut relation.0[0]
            && i.value == self.from
        {
            *i = Ident::with_quote('`', self.to.clone());
        }
        ControlFlow::Continue(())
    }
}

/// Removes spans from a Debug dump, which differ by the name lengths.
fn strip(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find("span: Span(") {
        out.push_str(&rest[..i]);
        let tail = &rest[i..];
        let end = tail.find("))").map_or(tail.len(), |e| e + 2);
        rest = &tail[end..];
    }
    out.push_str(rest);
    out
}
