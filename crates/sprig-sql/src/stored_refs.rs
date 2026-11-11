//! What a stored program's text names: the tables it reads or writes, the
//! routines it calls and whether it runs dynamic SQL (chapter 16, "Stored
//! code keeps its tables in place"; decision bugfix-31).
//!
//! A view, procedure, function, package, trigger or event runs inside
//! MariaDB in one schema, and its unqualified names resolve there. The
//! sharding advisor moves tables into other schemas (and its groups later
//! split and move to other clusters), so before it moves a table it asks
//! which stored code names it. [`references`] answers from the text alone:
//!
//! - **Tables** at every table-reference position the row-level security
//!   view swap knows ([`crate::rls::views::swap`]: `FROM`, `JOIN`, `UPDATE`,
//!   `INSERT` and `REPLACE` targets, `TABLE t`, subqueries, CTE names
//!   excluded), plus every identifier of statements that name tables
//!   outside those positions (`TRUNCATE`, `LOCK TABLES`, `ALTER`, `DROP`,
//!   `RENAME`, `LOAD`, `HANDLER`, `ANALYZE`, `OPTIMIZE`, `CHECK`,
//!   `REPAIR`, `CHECKSUM`, `SHOW`, `DESCRIBE`, `EXPLAIN`, `CREATE`,
//!   `GRANT`, `REVOKE`). Over-reporting is safe: the caller keeps a table
//!   in place that didn't need it.
//! - **Calls**: `CALL [s.]p` and every `[s.]f(` (built-in functions too;
//!   the caller matches them against the schema's routines).
//! - **Dynamic SQL**: `PREPARE x FROM '...'` and `EXECUTE IMMEDIATE '...'`
//!   with one string literal are read as statements of their own; any
//!   other operand (`@v`, `CONCAT(...)`, a variable) makes the text
//!   *opaque*: what it runs can't be known.

use std::cell::RefCell;
use std::collections::HashSet;

use crate::lex::{self, LexOptions, Token, TokenKind};

/// What one stored program's text names. Names are as written: the
/// schema `None` when unqualified (the code's own schema).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CodeRefs {
    /// Tables (and views) it may read or write.
    pub tables: Vec<(Option<String>, String)>,
    /// Routines it may call (and built-in functions: callers match).
    pub calls: Vec<(Option<String>, String)>,
    /// It runs dynamic SQL (`PREPARE`, `EXECUTE IMMEDIATE`).
    pub dynamic: bool,
    /// Some dynamic SQL's text isn't a string literal, or the text
    /// couldn't be read: it may name any table of its schema.
    pub opaque: bool,
}

/// Statement starts whose identifiers may all be table names.
const DDLISH: &[&str] = &[
    "TRUNCATE", "LOCK", "ALTER", "DROP", "RENAME", "LOAD", "HANDLER", "CHECKSUM", "OPTIMIZE",
    "REPAIR", "CHECK", "ANALYZE", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "CREATE", "GRANT",
    "REVOKE",
];

/// Words after which a new statement starts inside a stored program (as
/// the view swap reads bodies).
const BODY_STARTS: &[&str] = &["BEGIN", "THEN", "ELSE", "DO", "LOOP", "REPEAT", "ROW"];

/// Records every table reference [`crate::rls::views::swap`] meets.
struct Recorder(RefCell<Vec<(Option<String>, String)>>);

impl crate::rls::views::Lookup for Recorder {
    fn view(&self, schema: Option<&str>, table: &str) -> Option<String> {
        self.0
            .borrow_mut()
            .push((schema.map(str::to_owned), table.to_owned()));
        None
    }
}

struct Walk<'a> {
    src: &'a [u8],
    toks: Vec<Token>,
}

impl Walk<'_> {
    fn kind(&self, i: usize) -> Option<TokenKind> {
        self.toks.get(i).map(|t| t.kind)
    }

    fn word(&self, i: usize) -> String {
        self.toks
            .get(i)
            .filter(|t| t.kind == TokenKind::Word)
            .map(|t| String::from_utf8_lossy(t.bytes(self.src)).to_ascii_uppercase())
            .unwrap_or_default()
    }

    fn ident(&self, i: usize) -> Option<String> {
        let t = self.toks.get(i).filter(|t| t.kind.is_ident_like())?;
        Some(String::from_utf8_lossy(&lex::ident_value(t.bytes(self.src))).into_owned())
    }

    fn punct(&self, i: usize, p: &str) -> bool {
        self.toks.get(i).is_some_and(|t| {
            matches!(t.kind, TokenKind::Punct | TokenKind::Op) && t.bytes(self.src) == p.as_bytes()
        })
    }

    /// A possibly qualified name at `i`: (schema, name, index after it).
    fn name_at(&self, i: usize) -> Option<(Option<String>, String, usize)> {
        let first = self.ident(i)?;
        if self.punct(i + 1, ".")
            && let Some(second) = self.ident(i + 2)
        {
            return Some((Some(first), second, i + 3));
        }
        Some((None, first, i + 1))
    }
}

/// What `text` (a stored program's body or a view's definition, lexed with
/// `lex`) names; see the module docs.
#[must_use]
pub fn references(text: &str, lex: &LexOptions) -> CodeRefs {
    let mut out = CodeRefs::default();
    collect(text, *lex, &mut out, 0);
    out.tables.sort();
    out.tables.dedup();
    out.calls.sort();
    out.calls.dedup();
    out
}

fn collect(text: &str, lex_opts: LexOptions, out: &mut CodeRefs, depth: u8) {
    let rec = Recorder(RefCell::new(Vec::new()));
    if crate::rls::views::swap(text, &lex_opts, &rec, &HashSet::new()).is_err() {
        out.opaque = true;
    }
    out.tables.extend(rec.0.into_inner());
    let Ok(all) = lex::lex(text.as_bytes(), &lex_opts) else {
        out.opaque = true;
        return;
    };
    let w = Walk {
        src: text.as_bytes(),
        toks: all.into_iter().filter(|t| !t.kind.is_trivia()).collect(),
    };
    let n = w.toks.len();
    let mut start = true;
    let mut ddlish = false;
    let mut i = 0;
    while i < n {
        if w.punct(i, ";") {
            start = true;
            ddlish = false;
            i += 1;
            continue;
        }
        let up = w.word(i);
        let was_start = start;
        start = false;
        if BODY_STARTS.contains(&up.as_str())
            || (w.punct(i, ":") && i > 0 && w.kind(i - 1).is_some_and(TokenKind::is_ident_like))
        {
            start = true;
        }
        if was_start {
            ddlish = DDLISH.contains(&up.as_str());
        }
        match up.as_str() {
            "PREPARE" if was_start => {
                out.dynamic = true;
                // PREPARE name FROM <operand>
                let mut k = i + 1;
                while k < n && !w.punct(k, ";") && w.word(k) != "FROM" {
                    k += 1;
                }
                dynamic_operand(&w, k + 1, lex_opts, out, depth);
            }
            "EXECUTE" if w.word(i + 1) == "IMMEDIATE" => {
                out.dynamic = true;
                dynamic_operand(&w, i + 2, lex_opts, out, depth);
            }
            "CALL" => {
                if let Some((s, name, _)) = w.name_at(i + 1) {
                    out.calls.push((s, name));
                }
            }
            _ => {}
        }
        if w.kind(i).is_some_and(TokenKind::is_ident_like)
            && !(i > 0 && w.punct(i - 1, "."))
            && let Some((s, name, after)) = w.name_at(i)
        {
            if w.punct(after, "(") {
                out.calls.push((s.clone(), name.clone()));
            }
            if ddlish && !was_start {
                out.tables.push((s, name));
            }
        }
        i += 1;
    }
}

/// The operand of `PREPARE ... FROM` or `EXECUTE IMMEDIATE` at token `k`:
/// one string literal ending the statement is read as SQL; anything else
/// is opaque.
fn dynamic_operand(w: &Walk<'_>, k: usize, lex_opts: LexOptions, out: &mut CodeRefs, depth: u8) {
    let literal = w
        .toks
        .get(k)
        .filter(|t| matches!(t.kind, TokenKind::String | TokenKind::NationalString))
        .filter(|_| k + 1 >= w.toks.len() || w.punct(k + 1, ";"));
    let Some(t) = literal else {
        out.opaque = true;
        return;
    };
    let bytes = t.bytes(w.src);
    // N'...': the quote starts one byte in.
    let body = if bytes.first().is_some_and(|b| b.eq_ignore_ascii_case(&b'N')) {
        &bytes[1..]
    } else {
        bytes
    };
    match lex::string_value(body, lex_opts.no_backslash_escapes) {
        Some(v) if depth < 4 => {
            collect(&String::from_utf8_lossy(&v), lex_opts, out, depth + 1);
        }
        _ => out.opaque = true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refs(s: &str) -> CodeRefs {
        references(s, &LexOptions::mariadb())
    }

    fn tables(s: &str) -> Vec<String> {
        refs(s)
            .tables
            .into_iter()
            .map(|(s, t)| match s {
                Some(s) => format!("{s}.{t}"),
                None => t,
            })
            .collect()
    }

    #[test]
    fn reads_and_writes_are_found() {
        assert_eq!(tables("SELECT COUNT(*) FROM la"), vec!["la"]);
        assert_eq!(
            tables(
                "BEGIN DECLARE n INT; SELECT COUNT(*) INTO n FROM la l JOIN `t1_d1_g1_s1`.`cust` c \
                 ON c.id = l.k; INSERT INTO la_log (n) VALUES (n); UPDATE ord SET v = 1; \
                 DELETE FROM gone WHERE 1; REPLACE INTO rep VALUES (1); END"
            ),
            vec!["gone", "la", "la_log", "ord", "rep", "t1_d1_g1_s1.cust"]
        );
        // A subquery, a CTE (not a table) and a cursor.
        assert_eq!(
            tables(
                "BEGIN DECLARE c CURSOR FOR SELECT k FROM a WHERE k IN (SELECT k FROM b); \
                 WITH x AS (SELECT 1) SELECT * FROM x; END"
            ),
            vec!["a", "b"]
        );
        // A trigger body and an event body.
        assert_eq!(
            tables("INSERT INTO la_log (k) VALUES (NEW.k)"),
            vec!["la_log"]
        );
        assert_eq!(
            tables("INSERT INTO la_log (n) SELECT COUNT(*) FROM la"),
            vec!["la", "la_log"]
        );
    }

    #[test]
    fn statements_that_name_tables_elsewhere_are_found() {
        let t = tables("BEGIN TRUNCATE TABLE scratch; LOCK TABLES held WRITE; UNLOCK TABLES; END");
        assert!(t.contains(&"scratch".to_owned()), "{t:?}");
        assert!(t.contains(&"held".to_owned()), "{t:?}");
    }

    #[test]
    fn calls_are_found() {
        let r = refs("BEGIN CALL p2(1); CALL s.p3; SET @x = f(NEW.k) + CONCAT('a', 'b'); END");
        assert!(r.calls.contains(&(None, "p2".into())));
        assert!(r.calls.contains(&(Some("s".into()), "p3".into())));
        assert!(r.calls.contains(&(None, "f".into())));
        assert!(!r.dynamic && !r.opaque);
    }

    #[test]
    fn dynamic_sql_is_read_when_it_is_a_literal() {
        let r = refs("BEGIN PREPARE s FROM 'SELECT COUNT(*) FROM dyn_t'; EXECUTE s; END");
        assert!(r.dynamic && !r.opaque, "{r:?}");
        assert_eq!(r.tables, vec![(None, "dyn_t".to_owned())]);
        let r = refs("EXECUTE IMMEDIATE 'DELETE FROM d2'");
        assert!(r.dynamic && !r.opaque, "{r:?}");
        assert_eq!(r.tables, vec![(None, "d2".to_owned())]);
    }

    #[test]
    fn other_dynamic_sql_is_opaque() {
        for s in [
            "BEGIN SET @q = 'SELECT 1'; PREPARE s FROM @q; EXECUTE s; END",
            "EXECUTE IMMEDIATE CONCAT('SELECT COUNT(*) FROM ', 'dyn_x')",
            "BEGIN DECLARE q TEXT DEFAULT 'x'; EXECUTE IMMEDIATE q; END",
        ] {
            let r = refs(s);
            assert!(r.dynamic && r.opaque, "{s}: {r:?}");
        }
    }

    #[test]
    fn code_without_tables_names_none() {
        let r = refs("SELECT 41 + 1");
        assert!(r.tables.is_empty() && !r.opaque, "{r:?}");
        let r = refs("SET NEW.v_upper = UPPER(NEW.v)");
        assert!(r.tables.is_empty(), "{r:?}");
    }
}
