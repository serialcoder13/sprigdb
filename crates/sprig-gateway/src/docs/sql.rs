//! The virtual database `sprig_docs` in SQL sessions: read-only, the same
//! for every tenant.
//!
//! ```sql
//! SELECT page, heading_path, text FROM sprig_docs.chunks
//! WHERE MATCH(text) AGAINST ('replica lag') LIMIT 5;
//!
//! SELECT page FROM sprig_docs.chunks
//! ORDER BY VEC_DISTANCE_COSINE(embedding, sprig_docs.embed('why is replication slow'))
//! LIMIT 5;
//! ```
//!
//! A statement that names `sprig_docs.<x>` is answered here, before the
//! name layer sees it:
//!
//! - **Reads** (`SELECT`, `WITH`, a parenthesised `SELECT`, `EXPLAIN` of
//!   one) run on the docs-reader account, which can read `__sprig_docs`
//!   and nothing else, with `sprig_docs.chunks` replaced by the current
//!   release's rows: `(SELECT * FROM __sprig_docs.chunks WHERE version =
//!   '<v>') AS chunks`. Full-text `MATCH` and `VEC_DISTANCE_COSINE` work on
//!   it as on a table. `sprig_docs.embed('<text>')` is the text's embedding
//!   by the bundled model (a string literal only), as `VEC_FromText(...)`.
//! - **Descriptions** (`DESCRIBE`, `SHOW COLUMNS | INDEX | CREATE TABLE`)
//!   of `sprig_docs.chunks` describe the table.
//! - **Anything else** (`INSERT`, `UPDATE`, `DELETE`, DDL, `USE
//!   sprig_docs`, ...) is refused: writes with 1044, as MySQL refuses
//!   writes to `information_schema`. A statement can't mix `sprig_docs`
//!   with a tenant's tables: the reader account sees no other schema.
//!
//! `sprig_docs` is a reserved database name, so no tenant database can
//! take it on any protocol.

use sprig_common::{SprigCode, SprigError};
use sprig_sql::lex::{LexOptions, Token, TokenKind, ident_value, lex, string_value};

use sprig_docs::table::{self, VIRTUAL_DATABASE};

/// Whether `text` might name `sprig_docs` (a byte search, before lexing).
#[must_use]
pub fn mentions(text: &[u8]) -> bool {
    let n = VIRTUAL_DATABASE.len();
    text.windows(n)
        .any(|w| w.eq_ignore_ascii_case(VIRTUAL_DATABASE.as_bytes()))
}

/// What the statement does with `sprig_docs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A query: runs on the current release's rows.
    Read,
    /// A description of the table.
    Describe,
}

/// One change to the statement's text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Edit {
    /// `sprig_docs.chunks` as a table; `aliased`: the statement gives it
    /// an alias.
    Chunks {
        /// The bytes replaced.
        range: std::ops::Range<usize>,
        /// An alias follows.
        aliased: bool,
    },
    /// `sprig_docs.chunks` qualifying a column: becomes `chunks`.
    Qualifier(std::ops::Range<usize>),
    /// `sprig_docs.embed('<text>')`: the call's bytes and the text.
    Embed(std::ops::Range<usize>, String),
}

/// A statement that names `sprig_docs`, ready to render.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Analysis {
    /// Read or describe.
    pub kind: Kind,
    /// The edits, in text order.
    pub edits: Vec<Edit>,
}

/// Why a statement naming `sprig_docs` is refused.
#[derive(Debug)]
pub enum Refusal {
    /// A write or DDL: 1044 for the session's user.
    Write,
    /// Anything else, with its error.
    Error(SprigError),
}

/// 1044, as MySQL answers a write to `information_schema`.
#[must_use]
pub fn write_denied(user: &str, host: &str) -> SprigError {
    SprigError::standard(
        1044,
        *b"42000",
        format!("Access denied for user '{user}'@'{host}' to database '{VIRTUAL_DATABASE}'"),
    )
}

fn unsupported(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::NameRewriteUnsupported, m)
}

/// Keywords that can follow a table reference and aren't its alias.
const NOT_ALIAS: &[&str] = &[
    "WHERE",
    "JOIN",
    "LEFT",
    "RIGHT",
    "INNER",
    "CROSS",
    "ON",
    "GROUP",
    "ORDER",
    "LIMIT",
    "UNION",
    "HAVING",
    "NATURAL",
    "STRAIGHT_JOIN",
    "USING",
    "WINDOW",
    "FOR",
    "LOCK",
    "INTO",
    "EXCEPT",
    "INTERSECT",
    "PROCEDURE",
    "FULL",
    "OUTER",
    "OFFSET",
    "FETCH",
    "USE",
    "IGNORE",
    "FORCE",
    "PARTITION",
    "RETURNING",
];

/// Looks at a statement: `None` when it doesn't name `sprig_docs` (it goes
/// on as usual); the edits to make, or why it is refused.
///
/// # Errors
/// See [`Refusal`].
#[must_use]
#[allow(clippy::too_many_lines)] // one pass over the tokens, case by case
pub fn analyze(text: &[u8], opts: &LexOptions) -> Option<Result<Analysis, Refusal>> {
    if !mentions(text) {
        return None;
    }
    let tokens = lex(text, opts).ok()?;
    let sig: Vec<&Token> = tokens.iter().filter(|t| !t.kind.is_trivia()).collect();
    let word = |t: &Token| -> Option<String> {
        t.kind
            .is_ident_like()
            .then(|| String::from_utf8_lossy(&ident_value(t.bytes(text))).into_owned())
    };
    let is = |t: &Token, w: &str| word(t).is_some_and(|x| x.eq_ignore_ascii_case(w));
    let punct = |t: Option<&&Token>, p: &[u8]| t.is_some_and(|t| t.bytes(text) == p);

    // The references: `sprig_docs .` not after a `.`, and `USE sprig_docs`.
    let mut refs = Vec::new();
    let mut used = false;
    for (i, t) in sig.iter().enumerate() {
        if !is(t, VIRTUAL_DATABASE) || (i > 0 && punct(sig.get(i - 1), b".")) {
            continue;
        }
        if punct(sig.get(i + 1), b".") {
            refs.push(i);
        } else if i > 0 && is(sig[i - 1], "USE") {
            used = true;
        }
    }
    if refs.is_empty() && !used {
        return None;
    }
    // The statement's first word, past opening parentheses.
    let first = sig
        .iter()
        .position(|t| t.bytes(text) != b"(")
        .and_then(|i| word(sig[i]).map(|w| (i, w.to_ascii_uppercase())));
    let Some((fi, first)) = first else {
        return Some(Err(Refusal::Write));
    };
    if first == "USE" {
        return Some(Err(Refusal::Error(unsupported(
            "sprig_docs can't be the current database: name its table, sprig_docs.chunks",
        ))));
    }
    let next = sig
        .get(fi + 1)
        .and_then(|t| word(t))
        .map(|w| w.to_ascii_uppercase());
    let kind = match first.as_str() {
        "SELECT" | "WITH" | "VALUES" => Kind::Read,
        "EXPLAIN" | "DESCRIBE" | "DESC"
            if matches!(
                next.as_deref(),
                Some("SELECT" | "WITH" | "FORMAT" | "EXTENDED" | "PARTITIONS")
            ) || punct(sig.get(fi + 1), b"(") =>
        {
            Kind::Read
        }
        "EXPLAIN" | "DESCRIBE" | "DESC" => Kind::Describe,
        "SHOW"
            if matches!(
                next.as_deref(),
                Some("COLUMNS" | "FIELDS" | "INDEX" | "INDEXES" | "KEYS" | "CREATE" | "FULL")
            ) =>
        {
            Kind::Describe
        }
        "SHOW" => {
            return Some(Err(Refusal::Error(unsupported(
                "this SHOW of sprig_docs isn't supported; its one table is sprig_docs.chunks",
            ))));
        }
        _ => return Some(Err(Refusal::Write)),
    };
    if used {
        return Some(Err(Refusal::Write));
    }
    // System names can't be reached through the docs either.
    if let Some(t) = sig
        .iter()
        .find(|t| word(t).is_some_and(|w| sprig_common::names::is_internal_object(&w)))
    {
        let name = word(t).unwrap_or_default();
        return Some(Err(Refusal::Error(SprigError::new(
            SprigCode::ReservedName,
            format!("{name} is a reserved name"),
        ))));
    }
    let mut edits = Vec::new();
    for i in refs {
        let Some(name_tok) = sig.get(i + 2).filter(|t| t.kind.is_ident_like()) else {
            return Some(Err(Refusal::Error(unsupported(
                "sprig_docs. must be followed by a name: sprig_docs.chunks",
            ))));
        };
        let name = word(name_tok).unwrap_or_default();
        let start = sig[i].start as usize;
        let end = name_tok.end as usize;
        if name.eq_ignore_ascii_case("chunks") {
            if punct(sig.get(i + 3), b".") {
                edits.push(Edit::Qualifier(start..end));
            } else {
                let aliased = sig.get(i + 3).is_some_and(|t| {
                    is(t, "AS") || (t.kind.is_ident_like() && !NOT_ALIAS.iter().any(|w| is(t, w)))
                });
                edits.push(Edit::Chunks {
                    range: start..end,
                    aliased,
                });
            }
        } else if name.eq_ignore_ascii_case("embed") && kind == Kind::Read {
            let arg = sig.get(i + 4);
            let ok = punct(sig.get(i + 3), b"(")
                && arg.is_some_and(|t| t.kind == TokenKind::String)
                && punct(sig.get(i + 5), b")");
            let value = arg
                .filter(|_| ok)
                .and_then(|t| string_value(t.bytes(text), opts.no_backslash_escapes));
            let Some(value) = value else {
                return Some(Err(Refusal::Error(unsupported(
                    "sprig_docs.embed takes one string literal: sprig_docs.embed('text')",
                ))));
            };
            let close = sig[i + 5].end as usize;
            edits.push(Edit::Embed(
                start..close,
                String::from_utf8_lossy(&value).into_owned(),
            ));
        } else {
            return Some(Err(Refusal::Error(SprigError::standard(
                1146,
                *b"42S02",
                format!("Table '{VIRTUAL_DATABASE}.{name}' doesn't exist"),
            ))));
        }
    }
    Some(Ok(Analysis { kind, edits }))
}

/// The statement to run: `text` with `analysis`'s edits, reading
/// `version`, with `embeds` (one per [`Edit::Embed`], in order) in place
/// of the `sprig_docs.embed` calls. Reads are bounded to 30 seconds.
#[must_use]
pub fn render(text: &[u8], analysis: &Analysis, version: &str, embeds: &[Vec<f32>]) -> String {
    let lit = sprig_cluster::sql::literal(version);
    let mut out = String::with_capacity(text.len() + 128);
    if analysis.kind == Kind::Read {
        out.push_str("SET STATEMENT max_statement_time = 30 FOR ");
    }
    let mut at = 0;
    let mut embeds = embeds.iter();
    for e in &analysis.edits {
        let range = match e {
            Edit::Chunks { range, .. } | Edit::Qualifier(range) | Edit::Embed(range, _) => range,
        };
        out.push_str(&String::from_utf8_lossy(&text[at..range.start]));
        match (e, analysis.kind) {
            (Edit::Chunks { .. }, Kind::Describe) => out.push_str(&table::qualified(table::CHUNKS)),
            (Edit::Chunks { aliased, .. }, Kind::Read) => {
                out.push_str("(SELECT * FROM ");
                out.push_str(&table::qualified(table::CHUNKS));
                out.push_str(" WHERE version = ");
                out.push_str(&lit);
                out.push(')');
                if !aliased {
                    out.push_str(" AS `chunks`");
                }
            }
            (Edit::Qualifier(_), _) => out.push_str("`chunks`"),
            (Edit::Embed(..), _) => {
                let v = embeds
                    .next()
                    .map(|v| table::vector_text(v))
                    .unwrap_or_default();
                out.push_str("VEC_FromText('");
                out.push_str(&v);
                out.push_str("')");
            }
        }
        at = range.end;
    }
    out.push_str(&String::from_utf8_lossy(&text[at..]));
    out
}

/// The texts of a statement's `sprig_docs.embed` calls, in order.
#[must_use]
pub fn embed_texts(analysis: &Analysis) -> Vec<&str> {
    analysis
        .edits
        .iter()
        .filter_map(|e| match e {
            Edit::Embed(_, t) => Some(t.as_str()),
            _ => None,
        })
        .collect()
}

/// A backend error of the docs-reader account as the client should see
/// it: system names in their logical form, the reader account's refusals
/// of other schemas explained.
#[must_use]
pub fn client_error(
    code: u16,
    state: [u8; 5],
    message: &str,
    user: &str,
    host: &str,
) -> SprigError {
    if matches!(code, 1142 | 1143 | 1044 | 1045 | 1227) {
        return SprigError::standard(
            1142,
            *b"42000",
            format!(
                "a statement on {VIRTUAL_DATABASE} can only read {VIRTUAL_DATABASE}.chunks \
                 (user '{user}'@'{host}')"
            ),
        );
    }
    let m = message
        .replace(&format!("`{}`.", table::SCHEMA), "")
        .replace(table::SCHEMA, VIRTUAL_DATABASE)
        .replace(sprig_provision::docs_account::DOCS_USER, user);
    SprigError::standard(code, state, m)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a(sql: &str) -> Option<Result<Analysis, Refusal>> {
        analyze(sql.as_bytes(), &LexOptions::mysql84())
    }

    fn run(sql: &str) -> String {
        let an = a(sql).expect("docs").expect("ok");
        let n = embed_texts(&an).len();
        render(sql.as_bytes(), &an, "0.9.0", &vec![vec![0.5, -1.0]; n])
    }

    #[test]
    fn statements_without_the_database_pass() {
        assert!(a("SELECT 1").is_none());
        assert!(a("SELECT sprig_docs FROM t").is_none());
        assert!(a("SELECT t.sprig_docs.x FROM t").is_none());
        assert!(a("SELECT 'sprig_docs.chunks'").is_none());
        assert!(a("SELECT 1 -- sprig_docs.chunks").is_none());
        assert!(!mentions(b"SELECT 1"));
    }

    #[test]
    fn reads_become_the_current_release() {
        assert_eq!(
            run(
                "SELECT page FROM sprig_docs.chunks WHERE MATCH(text) AGAINST ('replica lag') LIMIT 5"
            ),
            "SET STATEMENT max_statement_time = 30 FOR SELECT page FROM (SELECT * FROM \
             `__sprig_docs`.`chunks` WHERE version = '0.9.0') AS `chunks` WHERE MATCH(text) \
             AGAINST ('replica lag') LIMIT 5"
        );
        // An alias is kept; quoted and mixed-case names are found.
        let s = run("select c.page from `SPRIG_DOCS`.`Chunks` AS c");
        assert!(s.ends_with("version = '0.9.0') AS c"), "{s}");
        let s = run("SELECT x.page FROM sprig_docs.chunks x JOIN sprig_docs.chunks y USING (page)");
        assert!(s.contains("'0.9.0') x JOIN"), "{s}");
        assert!(s.contains("'0.9.0') y USING"), "{s}");
        // A qualified column.
        let s = run("SELECT sprig_docs.chunks.page FROM sprig_docs.chunks");
        assert!(s.contains("SELECT `chunks`.page FROM ("), "{s}");
        // WITH and parentheses.
        assert!(matches!(
            a("WITH x AS (SELECT * FROM sprig_docs.chunks) SELECT * FROM x"),
            Some(Ok(Analysis {
                kind: Kind::Read,
                ..
            }))
        ));
        assert!(matches!(
            a("(SELECT page FROM sprig_docs.chunks) UNION (SELECT 'x')"),
            Some(Ok(Analysis {
                kind: Kind::Read,
                ..
            }))
        ));
    }

    #[test]
    fn embed_becomes_a_vector() {
        let s = run("SELECT page FROM sprig_docs.chunks ORDER BY \
             VEC_DISTANCE_COSINE(embedding, sprig_docs.embed('why is it slow')) LIMIT 5");
        assert!(
            s.contains("VEC_DISTANCE_COSINE(embedding, VEC_FromText('[0.5,-1.0]')) LIMIT 5"),
            "{s}"
        );
        let an = a("SELECT sprig_docs.embed('it''s')").unwrap().unwrap();
        assert_eq!(embed_texts(&an), vec!["it's"]);
        for bad in [
            "SELECT sprig_docs.embed(page) FROM sprig_docs.chunks",
            "SELECT sprig_docs.embed('a', 'b')",
            "SELECT sprig_docs.embed",
        ] {
            assert!(matches!(a(bad), Some(Err(Refusal::Error(_)))), "{bad}");
        }
    }

    #[test]
    fn writes_and_other_forms_are_refused() {
        for w in [
            "UPDATE sprig_docs.chunks SET text = 'x'",
            "INSERT INTO sprig_docs.chunks (page) VALUES ('x')",
            "DELETE FROM sprig_docs.chunks",
            "REPLACE INTO sprig_docs.chunks SELECT * FROM sprig_docs.chunks",
            "DROP TABLE sprig_docs.chunks",
            "TRUNCATE sprig_docs.chunks",
            "ALTER TABLE sprig_docs.chunks ADD x INT",
            "CREATE TABLE sprig_docs.x (a INT)",
            "LOAD DATA INFILE 'f' INTO TABLE sprig_docs.chunks",
            "  /* c */ update sprig_docs.chunks set text = ''",
            "GRANT SELECT ON sprig_docs.chunks TO u",
        ] {
            assert!(matches!(a(w), Some(Err(Refusal::Write))), "{w}");
        }
        assert!(matches!(a("USE sprig_docs"), Some(Err(Refusal::Error(_)))));
        let e = match a("SELECT * FROM sprig_docs.packs") {
            Some(Err(Refusal::Error(e))) => e,
            x => panic!("{x:?}"),
        };
        assert!(e.message.contains("doesn't exist"), "{e}");
        assert!(matches!(
            a("SELECT * FROM sprig_docs.chunks JOIN __sprig_docs.packs"),
            Some(Err(Refusal::Error(_)))
        ));
        assert!(matches!(
            a("SHOW TABLES FROM sprig_docs"),
            Some(Err(Refusal::Write | Refusal::Error(_))) | None
        ));
        let e = write_denied("app", "%");
        assert_eq!(
            e.message,
            "Access denied for user 'app'@'%' to database 'sprig_docs'"
        );
    }

    #[test]
    fn descriptions_name_the_table() {
        for d in [
            "DESCRIBE sprig_docs.chunks",
            "SHOW COLUMNS FROM sprig_docs.chunks",
            "SHOW CREATE TABLE sprig_docs.chunks",
            "EXPLAIN sprig_docs.chunks",
        ] {
            let s = run(d);
            assert!(s.contains("`__sprig_docs`.`chunks`"), "{s}");
            assert!(!s.starts_with("SET STATEMENT"), "{s}");
        }
        let s = run("EXPLAIN SELECT * FROM sprig_docs.chunks");
        assert!(s.contains("WHERE version = '0.9.0')"), "{s}");
    }

    #[test]
    fn backend_errors_are_shown_logically() {
        let e = client_error(
            1142,
            *b"42000",
            "SELECT command denied to user 'sprig_docs_r'",
            "app",
            "%",
        );
        assert!(e.message.contains("can only read sprig_docs.chunks"), "{e}");
        let e = client_error(
            1054,
            *b"42S22",
            "Unknown column 'x' in '`__sprig_docs`.`chunks`'",
            "app",
            "%",
        );
        assert!(!e.message.contains("__sprig"), "{e}");
    }
}
