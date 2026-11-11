//! Policy views (chapter 29, "Inside MariaDB: defence in depth"; build
//! step 24's reads, decisions RL18 to RL21 in `decisions/rls-interim.md`).
//!
//! Each shard schema of a protected table holds an updatable view
//! `__sprig_rls_<table>` over it that keeps only the rows the
//! session's context (`@sprig_rls`, read through the function
//! `__sprig_rls_ctx()`, since MariaDB refuses user variables in a view,
//! 1351) lets the `SELECT` policies show. Accounts of users a table's
//! policies bind hold no `SELECT` on the table itself, only on the view,
//! so MariaDB filters their reads even if the gateway's rewrite missed a
//! reference.
//!
//! [`swap`] puts the views into a statement's text in place of the
//! tables (`FROM t` → `` FROM `__sprig_rls_docs` AS `t` ``). It works on
//! tokens, not a parse tree, so it also reads stored-program bodies and
//! every MariaDB form; a reference it misses still names the table, which
//! the account can't read (1142): a miss fails closed.
//!
//! ```text
//! table positions:  FROM x, JOIN x, STRAIGHT_JOIN x (as a join), the
//!                   comma-separated rest of a FROM list (and of a
//!                   multi-table UPDATE's list), UPDATE x and TABLE x at a
//!                   statement's start; INSERT/REPLACE INTO x only with
//!                   ON DUPLICATE KEY UPDATE (which reads the row)
//! x protected:      x → view, " AS x" unless aliased; index hints dropped
//!                   (views have no indexes); schema.x.col → x.col
//! left alone:       CTE names, the caller's temporary tables, functions
//!                   (x(...)), SHOW / DESCRIBE / DDL statements
//! ```
//!
//! Views name their columns (decision RL28): `__sprig_rls_<table>` holds
//! the columns `SELECT *` on the table shows, so invisible columns stay
//! out of `*` and `_sprig_` columns out of every user's results. A table
//! with invisible columns (a split table's `_sprig_rk`, the advisor's and
//! the key-value layer's `_sprig_p_` columns, a user's own) also gets
//! `__sprig_rlsh_<table>` with every column; a statement that names one of
//! the invisible columns reads that one. Such a statement with a `*`
//! select list isn't swapped (its account then gets 1142): `*` on the
//! second view would show the invisible columns.

#![allow(clippy::many_single_char_names)] // token indexes: t, i, k, m, c, n

use std::collections::HashSet;

use sprig_common::SprigError;

use super::text::{Edit, Toks, apply, quote};
use crate::lex::{self, LexOptions, TokenKind, ident_value};

/// The prefix of policy views; the table's name follows.
pub const VIEW_PREFIX: &str = "__sprig_rls_";

/// The prefix of the policy views that also show a table's invisible
/// columns (RL28); the table's name follows.
pub const HIDDEN_PREFIX: &str = "__sprig_rlsh_";

/// The function returning the session's context inside a view.
pub const CTX_FUNCTION: &str = "__sprig_rls_ctx";

/// The longest table name a view name holds whole (64 - 12).
const NAME_MAX: usize = 52;

/// The policy view of table `table` (object `object`):
/// `__sprig_rls_<table>`, or for names over 52 characters
/// `__sprig_rls_<first 40>~<object id>`.
#[must_use]
pub fn view_name(table: &str, object: u64) -> String {
    if table.chars().count() <= NAME_MAX {
        format!("{VIEW_PREFIX}{table}")
    } else {
        let head: String = table.chars().take(40).collect();
        format!("{VIEW_PREFIX}{head}~{object}")
    }
}

/// The policy view of table `table` (object `object`) with every column,
/// invisible ones included (RL28): `__sprig_rlsh_<table>`, or for names
/// over 51 characters `__sprig_rlsh_<first 39>~<object id>`.
#[must_use]
pub fn hidden_view_name(table: &str, object: u64) -> String {
    if table.chars().count() < NAME_MAX {
        format!("{HIDDEN_PREFIX}{table}")
    } else {
        let head: String = table.chars().take(39).collect();
        format!("{HIDDEN_PREFIX}{head}~{object}")
    }
}

/// `DROP VIEW IF EXISTS` for a table's view in `schema`.
#[must_use]
pub fn view_drop(schema: &str, table: &str, object: u64) -> String {
    format!(
        "DROP VIEW IF EXISTS {}.{}",
        quote(schema),
        quote(&view_name(table, object))
    )
}

/// `DROP VIEW IF EXISTS` for a table's all-columns view in `schema`.
#[must_use]
pub fn hidden_view_drop(schema: &str, table: &str, object: u64) -> String {
    format!(
        "DROP VIEW IF EXISTS {}.{}",
        quote(schema),
        quote(&hidden_view_name(table, object))
    )
}

/// Whether `name` is a policy view's name (either kind).
#[must_use]
pub fn is_policy_view(name: &str) -> bool {
    let l = name.to_ascii_lowercase();
    (l.starts_with(VIEW_PREFIX) || l.starts_with(HIDDEN_PREFIX)) && l != CTX_FUNCTION
}

/// Stored-code text as written: every `` `__sprig_rls_t` AS `t` `` back to
/// `` `t` ``, and other references to a policy view back to its table's
/// name (`SHOW CREATE VIEW`, `PROCEDURE`, ... of code the gateway pointed
/// at the views, RL20).
#[must_use]
pub fn unswap(text: &str) -> std::borrow::Cow<'_, str> {
    if !text.contains("`__sprig_rls") {
        return std::borrow::Cow::Borrowed(text);
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    let pats = [format!("`{HIDDEN_PREFIX}"), format!("`{VIEW_PREFIX}")];
    loop {
        let Some((at, pat)) = pats
            .iter()
            .filter_map(|p| rest.find(p.as_str()).map(|a| (a, p)))
            .min_by_key(|(a, _)| *a)
        else {
            break;
        };
        out.push_str(&rest[..at]);
        let after = &rest[at + pat.len()..];
        let Some(end) = after.find('`') else {
            out.push_str(&rest[at..]);
            rest = "";
            break;
        };
        let name = &after[..end];
        if (pat.len() == VIEW_PREFIX.len() + 1 && name == &CTX_FUNCTION[VIEW_PREFIX.len()..])
            || name.contains('~')
        {
            out.push_str(&rest[at..=at + pat.len() + end]);
            rest = &after[end + 1..];
            continue;
        }
        out.push('`');
        out.push_str(name);
        out.push('`');
        let tail = &after[end + 1..];
        // ` AS `name`` (or MariaDB's ` `name``) naming the table again.
        let again = format!("`{name}`");
        let skip = [format!(" AS {again}"), format!(" {again}")]
            .into_iter()
            .find(|p| tail.starts_with(p.as_str()) && !tail[p.len()..].starts_with('.'))
            .map_or(0, |p| p.len());
        rest = &tail[skip..];
    }
    out.push_str(rest);
    std::borrow::Cow::Owned(out)
}

/// Statement starts whose table references are never swapped: metadata,
/// DDL on the table itself (which needs no `SELECT`), and session control.
const SKIPPED: &[&str] = &[
    "SHOW",
    "DESCRIBE",
    "DESC",
    "EXPLAIN",
    "ALTER",
    "DROP",
    "TRUNCATE",
    "RENAME",
    "LOAD",
    "HANDLER",
    "CHECKSUM",
    "OPTIMIZE",
    "REPAIR",
    "CHECK",
    "GRANT",
    "REVOKE",
    "USE",
    "LOCK",
    "UNLOCK",
    "KILL",
    "FLUSH",
    "XA",
    "START",
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT",
    "RELEASE",
    "PREPARE",
    "EXECUTE",
    "DEALLOCATE",
    "BINLOG",
    "PURGE",
    "RESET",
    "INSTALL",
    "UNINSTALL",
    "BACKUP",
];

/// Words after which a new statement starts inside a stored program.
const BODY_STARTS: &[&str] = &["BEGIN", "THEN", "ELSE", "DO", "LOOP", "REPEAT", "ROW"];

/// Words that end a FROM list at its depth.
const LIST_ENDS: &[&str] = &[
    "WHERE",
    "GROUP",
    "HAVING",
    "ORDER",
    "LIMIT",
    "UNION",
    "EXCEPT",
    "INTERSECT",
    "WINDOW",
    "INTO",
    "FOR",
    "LOCK",
    "RETURNING",
    "PROCEDURE",
    "SET",
    "VALUES",
    "SELECT",
    "FETCH",
    "OFFSET",
];

/// Words that can follow a table reference and are never its alias.
const NOT_ALIAS: &[&str] = &[
    "WHERE",
    "ON",
    "USING",
    "JOIN",
    "INNER",
    "LEFT",
    "RIGHT",
    "CROSS",
    "NATURAL",
    "STRAIGHT_JOIN",
    "FULL",
    "OUTER",
    "SET",
    "GROUP",
    "ORDER",
    "LIMIT",
    "HAVING",
    "UNION",
    "EXCEPT",
    "INTERSECT",
    "FOR",
    "LOCK",
    "WINDOW",
    "INTO",
    "USE",
    "IGNORE",
    "FORCE",
    "PARTITION",
    "VALUES",
    "VALUE",
    "SELECT",
    "RETURNING",
    "PROCEDURE",
    "WITH",
    "FETCH",
    "OFFSET",
    "END",
    "DO",
    "THEN",
    "ELSE",
    "ELSEIF",
    "WHEN",
    "LATERAL",
    "TABLE",
    "DEFAULT",
    "ROWS",
    "NATURAL",
];

/// Where a table reference sits.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Pos {
    /// Read (or an UPDATE / DELETE target): view with the table's name as
    /// alias.
    Read,
    /// An INSERT or REPLACE target: the view's name only (no alias
    /// allowed), and only when the statement reads the row (ODKU).
    Insert,
}

struct Frame {
    from_list: bool,
}

/// What [`swap`] needs to know.
pub trait Lookup {
    /// The policy view to read instead of table `table` (in `schema` as
    /// written, `None` when unqualified), if the reader is bound by it.
    fn view(&self, schema: Option<&str>, table: &str) -> Option<String>;

    /// For a table with invisible columns: its all-columns view and the
    /// invisible columns' lower-case names (RL28). `view` is what
    /// [`Lookup::view`] returned.
    fn hidden(&self, view: &str) -> Option<(String, Vec<String>)> {
        let _ = view;
        None
    }
}

impl<F: Fn(Option<&str>, &str) -> Option<String>> Lookup for F {
    fn view(&self, schema: Option<&str>, table: &str) -> Option<String> {
        self(schema, table)
    }
}

fn val(t: &Toks<'_>, i: usize) -> String {
    t.toks
        .get(i)
        .map(|k| String::from_utf8_lossy(&ident_value(k.bytes(t.src))).into_owned())
        .unwrap_or_default()
}

fn ident(t: &Toks<'_>, i: usize) -> bool {
    t.toks.get(i).is_some_and(|k| k.kind.is_ident_like())
}

fn upper(t: &Toks<'_>, i: usize) -> String {
    t.toks
        .get(i)
        .filter(|k| k.kind == TokenKind::Word)
        .map(|k| String::from_utf8_lossy(k.bytes(t.src)).to_ascii_uppercase())
        .unwrap_or_default()
}

/// The `WITH` clauses' names: (lower-case name, first and last token of
/// its own body, recursive).
fn cte_names(t: &Toks<'_>) -> Vec<(String, usize, usize, bool)> {
    let mut out = Vec::new();
    let n = t.toks.len();
    let mut i = 0;
    while i < n {
        if t.word(i, "WITH") && !t.word(i + 1, "ROLLUP") && !t.word(i + 1, "CHECK") {
            let mut k = i + 1;
            let recursive = t.word(k, "RECURSIVE");
            if recursive {
                k += 1;
            }
            loop {
                if !ident(t, k) {
                    break;
                }
                let name = val(t, k).to_lowercase();
                let mut m = k + 1;
                if t.punct(m, "(") {
                    match t.matching(m) {
                        Some(c) => m = c + 1,
                        None => break,
                    }
                }
                if !t.word(m, "AS") {
                    break;
                }
                m += 1;
                if !t.punct(m, "(") {
                    break;
                }
                let Some(c) = t.matching(m) else { break };
                out.push((name, m, c, recursive));
                if t.punct(c + 1, ",") {
                    k = c + 2;
                } else {
                    break;
                }
            }
        }
        i += 1;
    }
    out
}

/// Whether an unqualified `name` at token `at` names a CTE: some CTE has
/// that name, and `at` isn't inside the body of a non-recursive one of
/// that name (which still reads the table).
fn is_cte(ctes: &[(String, usize, usize, bool)], name: &str, at: usize) -> bool {
    let mine: Vec<_> = ctes.iter().filter(|c| c.0 == name).collect();
    !mine.is_empty() && !mine.iter().any(|c| !c.3 && c.1 <= at && at <= c.2)
}

/// Whether the statement from token `from` (to its `;`) has `ON DUPLICATE
/// KEY UPDATE`.
fn has_odku(t: &Toks<'_>, from: usize) -> bool {
    let mut k = from;
    while k < t.toks.len() && !t.punct(k, ";") {
        if t.word(k, "DUPLICATE") && t.word(k + 1, "KEY") {
            return true;
        }
        k += 1;
    }
    false
}

/// Lower-case values of the statement's identifiers (and words): the
/// names it could mean a column by.
fn idents(t: &Toks<'_>) -> HashSet<String> {
    (0..t.toks.len())
        .filter(|i| ident(t, *i))
        .map(|i| val(t, i).to_lowercase())
        .collect()
}

/// Whether the statement has a `*` or `x.*` select list item (or
/// `RETURNING *`): what a view showing invisible columns would widen.
fn has_wildcard(t: &Toks<'_>) -> bool {
    (0..t.toks.len()).any(|i| {
        let star = t
            .toks
            .get(i)
            .is_some_and(|k| k.bytes(t.src) == b"*" && !k.kind.is_ident_like());
        star && i > 0
            && (t.punct(i - 1, ".")
                || t.punct(i - 1, ",")
                || matches!(
                    upper(t, i - 1).as_str(),
                    "SELECT"
                        | "ALL"
                        | "DISTINCT"
                        | "DISTINCTROW"
                        | "HIGH_PRIORITY"
                        | "STRAIGHT_JOIN"
                        | "SQL_SMALL_RESULT"
                        | "SQL_BIG_RESULT"
                        | "SQL_BUFFER_RESULT"
                        | "SQL_CACHE"
                        | "SQL_NO_CACHE"
                        | "SQL_CALC_FOUND_ROWS"
                        | "RETURNING"
                ))
    })
}

/// Token index of the end of index hints starting at `k` (`USE INDEX
/// (...)`, repeated), or `k` when there are none.
fn hints_end(t: &Toks<'_>, mut k: usize) -> usize {
    loop {
        let w = upper(t, k);
        if !matches!(w.as_str(), "USE" | "IGNORE" | "FORCE")
            || !(t.word(k + 1, "INDEX") || t.word(k + 1, "KEY"))
        {
            return k;
        }
        let mut m = k + 2;
        if t.word(m, "FOR") {
            m += 1;
            if t.word(m, "JOIN") {
                m += 1;
            } else if t.word(m, "ORDER") || t.word(m, "GROUP") {
                m += 2;
            }
        }
        if !t.punct(m, "(") {
            return k;
        }
        match t.matching(m) {
            Some(c) => k = c + 1,
            None => return k,
        }
    }
}

/// Replaces protected table references in `src` with their policy views
/// (see the module docs); `Ok(None)` when nothing changed. `exclude`
/// holds lower-case names that aren't the tables here (temporary tables).
///
/// # Errors
/// 39366 if the text can't be lexed or the edits collide.
#[allow(clippy::too_many_lines, clippy::implicit_hasher)] // one pass over the tokens
pub fn swap(
    src: &str,
    lex_opts: &LexOptions,
    lookup: &dyn Lookup,
    exclude: &HashSet<String>,
) -> Result<Option<String>, SprigError> {
    let all = lex::lex(src.as_bytes(), lex_opts)
        .map_err(|_| super::unsupported("a statement too long to lex"))?;
    let t = Toks::new(src.as_bytes(), &all);
    let n = t.toks.len();
    if n == 0 {
        return Ok(None);
    }
    let ctes = cte_names(&t);
    let mut ident_cache: Option<HashSet<String>> = None;
    let mut widened = false;
    // RL28: the all-columns view when the statement names an invisible
    // column of the table.
    let mut choose = |view: String| -> String {
        let Some((full, cols)) = lookup.hidden(&view) else {
            return view;
        };
        let seen = ident_cache.get_or_insert_with(|| idents(&t));
        if cols.iter().any(|c| seen.contains(c)) {
            widened = true;
            full
        } else {
            view
        }
    };
    let mut edits: Vec<Edit> = Vec::new();
    let mut swapped: Vec<(Option<String>, String)> = Vec::new();
    let mut stack = vec![Frame { from_list: false }];
    let mut stmt_start = true;
    let mut skip_stmt = false;
    let mut odku = false;
    // Token index → position kind.
    let mut refs: Vec<(usize, Pos)> = Vec::new();
    let mut prev_upper = String::new();
    let mut i = 0;
    while i < n {
        let w = upper(&t, i);
        if stmt_start {
            stmt_start = false;
            skip_stmt = SKIPPED.contains(&w.as_str())
                || (w == "ANALYZE"
                    && matches!(
                        upper(&t, i + 1).as_str(),
                        "TABLE" | "LOCAL" | "NO_WRITE_TO_BINLOG"
                    ));
            odku = has_odku(&t, i);
            if !skip_stmt {
                match w.as_str() {
                    "UPDATE" => {
                        let mut k = i + 1;
                        while matches!(upper(&t, k).as_str(), "LOW_PRIORITY" | "IGNORE") {
                            k += 1;
                        }
                        if let Some(f) = stack.last_mut() {
                            f.from_list = true;
                        }
                        refs.push((k, Pos::Read));
                    }
                    "INSERT" | "REPLACE" => {
                        let mut k = i + 1;
                        while matches!(
                            upper(&t, k).as_str(),
                            "LOW_PRIORITY" | "DELAYED" | "HIGH_PRIORITY" | "IGNORE" | "INTO"
                        ) {
                            k += 1;
                        }
                        refs.push((k, Pos::Insert));
                    }
                    "TABLE" => refs.push((i + 1, Pos::Read)),
                    _ => {}
                }
            }
        }
        if t.punct(i, ";") {
            stack = vec![Frame { from_list: false }];
            stmt_start = true;
            prev_upper.clear();
            i += 1;
            continue;
        }
        if BODY_STARTS.contains(&w.as_str()) || t.punct(i, ":") && ident(&t, i.wrapping_sub(1)) {
            // A label (`l1:`) or a compound statement's keyword: the next
            // token starts a statement (THEN and ELSE inside CASE start an
            // expression, which never begins with these keywords).
            stmt_start = true;
        }
        if skip_stmt {
            prev_upper = w;
            i += 1;
            continue;
        }
        if t.punct(i, "(") {
            let at_ref = refs.last().is_some_and(|(k, _)| *k == i);
            stack.push(Frame { from_list: at_ref });
            if at_ref {
                refs.push((i + 1, Pos::Read));
            }
        } else if t.punct(i, ")") {
            if stack.len() > 1 {
                stack.pop();
            }
        } else if t.punct(i, ",") {
            if stack.last().is_some_and(|f| f.from_list) {
                refs.push((i + 1, Pos::Read));
            }
        } else if w == "FROM" {
            if let Some(f) = stack.last_mut() {
                f.from_list = true;
            }
            refs.push((i + 1, Pos::Read));
        } else if w == "JOIN"
            || (w == "STRAIGHT_JOIN"
                && !matches!(
                    prev_upper.as_str(),
                    "SELECT" | "ALL" | "DISTINCT" | "DISTINCTROW" | "HIGH_PRIORITY"
                ))
        {
            refs.push((i + 1, Pos::Read));
        } else if LIST_ENDS.contains(&w.as_str())
            && let Some(f) = stack.last_mut()
        {
            f.from_list = false;
        }
        // A reference position: is it a protected table?
        if let Some(&(k, pos)) = refs.last()
            && k == i
            && ident(&t, i)
            && !matches!(
                w.as_str(),
                "SELECT" | "WITH" | "LATERAL" | "DUAL" | "VALUES"
            )
        {
            let (schema, last) = if t.punct(i + 1, ".") && ident(&t, i + 2) {
                (Some(val(&t, i)), i + 2)
            } else {
                (None, i)
            };
            let table = val(&t, last);
            let lower = table.to_lowercase();
            let shadowed =
                schema.is_none() && (is_cte(&ctes, &lower, i) || exclude.contains(&lower));
            // `x(` is a function, except an INSERT target's column list.
            let func = pos == Pos::Read && t.punct(last + 1, "(");
            if !shadowed
                && !func
                && let Some(view) = lookup.view(schema.as_deref(), &table)
            {
                let view = choose(view);
                let tok = t.toks[last];
                match pos {
                    Pos::Insert => {
                        if odku {
                            edits.push(Edit {
                                start: tok.start as usize,
                                end: tok.end as usize,
                                with: quote(&view),
                            });
                            swapped.push((schema.clone(), lower.clone()));
                        }
                    }
                    Pos::Read => {
                        edits.push(Edit {
                            start: tok.start as usize,
                            end: tok.end as usize,
                            with: quote(&view),
                        });
                        let mut k = last + 1;
                        let nw = upper(&t, k);
                        let aliased = t.word(k, "AS")
                            || t.toks
                                .get(k)
                                .is_some_and(|x| x.kind == TokenKind::QuotedIdent)
                            || (t.toks.get(k).is_some_and(|x| x.kind == TokenKind::Word)
                                && !NOT_ALIAS.contains(&nw.as_str()));
                        if aliased {
                            k += if t.word(k, "AS") { 2 } else { 1 };
                        } else {
                            edits.push(Edit {
                                start: tok.end as usize,
                                end: tok.end as usize,
                                with: format!(" AS {}", quote(&table)),
                            });
                        }
                        let h = hints_end(&t, k);
                        if h > k {
                            edits.push(Edit {
                                start: t.start(k),
                                end: t.end(h - 1),
                                with: String::new(),
                            });
                        }
                        swapped.push((schema.clone(), lower.clone()));
                    }
                }
            }
            prev_upper = w;
            i = last + 1;
            continue;
        }
        prev_upper = w;
        i += 1;
    }
    if edits.is_empty() {
        return Ok(None);
    }
    if widened && has_wildcard(&t) {
        return Err(super::unsupported(
            "`*` in a statement that names an invisible column of a protected table",
        ));
    }
    // schema.t.col (and schema.t.*) of a swapped qualified reference: the
    // alias is `t` now.
    let qualified: HashSet<(String, String)> = swapped
        .iter()
        .filter_map(|(s, t)| s.as_ref().map(|s| (s.to_lowercase(), t.clone())))
        .collect();
    if !qualified.is_empty() {
        for k in 0..n.saturating_sub(4) {
            if ident(&t, k)
                && t.punct(k + 1, ".")
                && ident(&t, k + 2)
                && t.punct(k + 3, ".")
                && (ident(&t, k + 4) || t.punct(k + 4, "*") || {
                    t.toks
                        .get(k + 4)
                        .is_some_and(|x| x.kind == TokenKind::Op && x.bytes(t.src) == b"*")
                })
                && !(k > 0 && t.punct(k - 1, "."))
                && qualified.contains(&(val(&t, k).to_lowercase(), val(&t, k + 2).to_lowercase()))
            {
                edits.push(Edit {
                    start: t.start(k),
                    end: t.start(k + 2),
                    with: String::new(),
                });
            }
        }
    }
    apply(src, &edits).map(Some)
}

/// One tenant's protected tables, as a backend connection's statements
/// see them: `(db id, lower-case table name)` → object id. Set on the
/// connections of sessions the tables' policies bind.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct ViewSwap {
    /// The tenant.
    pub tenant: u64,
    /// db id → lower-case table name → its policy view.
    pub tables: std::collections::BTreeMap<u32, std::collections::BTreeMap<String, String>>,
    /// Lower-case names of the session's temporary tables (they shadow
    /// tables of the same name).
    pub temporary: std::collections::BTreeSet<String>,
    /// Policy view → the table's all-columns view and its invisible
    /// columns (lower case), for tables with invisible columns (RL28).
    pub hidden: std::collections::BTreeMap<String, (String, Vec<String>)>,
    /// Temporary tables by (lower-case schema, lower-case name) that shadow
    /// a protected table on this connection: a gathered read's copies,
    /// filled through the views already (RL28).
    pub shadowed: std::collections::BTreeSet<(String, String)>,
}

impl ViewSwap {
    /// Notes the temporary table `CREATE TEMPORARY TABLE` statement `sql`
    /// makes (`` `schema`.`name` `` or `name` in `default_schema`), so the
    /// connection's later statements read it, not the policy view. `false`
    /// when `sql` isn't such a statement.
    pub fn note_temporary(&mut self, sql: &str, default_schema: Option<&str>) -> bool {
        let Ok(all) = lex::lex(sql.as_bytes(), &LexOptions::mariadb()) else {
            return false;
        };
        let t = Toks::new(sql.as_bytes(), &all);
        let Some(at) = (0..t.toks.len())
            .find(|i| t.word(*i, "CREATE") && t.word(i + 1, "TEMPORARY") && t.word(i + 2, "TABLE"))
        else {
            return false;
        };
        let mut k = at + 3;
        if t.word(k, "IF") && t.word(k + 1, "NOT") && t.word(k + 2, "EXISTS") {
            k += 3;
        }
        if !ident(&t, k) {
            return false;
        }
        let (schema, name) = if t.punct(k + 1, ".") && ident(&t, k + 2) {
            (Some(val(&t, k)), val(&t, k + 2))
        } else {
            (default_schema.map(str::to_owned), val(&t, k))
        };
        let Some(schema) = schema else {
            return false;
        };
        self.shadowed
            .insert((schema.to_lowercase(), name.to_lowercase()));
        true
    }
}

struct SwapLookup<'a> {
    swap: &'a ViewSwap,
    default_schema: Option<&'a str>,
}

impl Lookup for SwapLookup<'_> {
    fn view(&self, schema: Option<&str>, table: &str) -> Option<String> {
        let s = schema.or(self.default_schema)?;
        if self
            .swap
            .shadowed
            .contains(&(s.to_lowercase(), table.to_lowercase()))
        {
            return None;
        }
        let db = self.swap.db_of(s)?;
        self.swap
            .tables
            .get(&db)?
            .get(&table.to_lowercase())
            .cloned()
    }

    fn hidden(&self, view: &str) -> Option<(String, Vec<String>)> {
        self.swap.hidden.get(view).cloned()
    }
}

impl ViewSwap {
    /// The database id of physical schema `schema` of this tenant
    /// (`t<tenant>_d<db>_...`).
    #[must_use]
    pub fn db_of(&self, schema: &str) -> Option<u32> {
        let rest = schema.strip_prefix(&format!("t{}_d", self.tenant))?;
        let end = rest.find('_')?;
        rest[..end].parse().ok()
    }

    /// `sql` with the views in, if it reads a protected table;
    /// `default_schema` is the connection's.
    #[must_use]
    pub fn apply(
        &self,
        sql: &[u8],
        lex_opts: &LexOptions,
        default_schema: Option<&str>,
    ) -> Option<Vec<u8>> {
        if self.tables.is_empty() {
            return None;
        }
        let text = std::str::from_utf8(sql).ok()?;
        // Cheap filter: some protected name must appear at all.
        let lower = text.to_ascii_lowercase();
        if !self
            .tables
            .values()
            .flat_map(|m| m.keys())
            .any(|n| lower.contains(n.as_str()))
        {
            return None;
        }
        let lookup = SwapLookup {
            swap: self,
            default_schema,
        };
        let exclude: HashSet<String> = self.temporary.iter().cloned().collect();
        swap(text, lex_opts, &lookup, &exclude)
            .ok()
            .flatten()
            .map(String::into_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sw(sql: &str) -> String {
        let lookup = |s: Option<&str>, t: &str| -> Option<String> {
            let ok_schema = s.is_none_or(|s| s == "t1_d2_g1_s1" || s == "shop");
            (ok_schema && t.eq_ignore_ascii_case("docs")).then(|| "__sprig_rls_docs".to_owned())
        };
        swap(sql, &LexOptions::mariadb(), &lookup, &HashSet::new())
            .unwrap()
            .unwrap_or_else(|| sql.to_owned())
    }

    #[test]
    fn reads_go_through_the_view() {
        assert_eq!(
            sw("SELECT * FROM docs WHERE id = 1"),
            "SELECT * FROM `__sprig_rls_docs` AS `docs` WHERE id = 1"
        );
        assert_eq!(
            sw("SELECT d.id FROM `t1_d2_g1_s1`.`docs` d JOIN notes n ON n.d = d.id"),
            "SELECT d.id FROM `t1_d2_g1_s1`.`__sprig_rls_docs` d JOIN notes n ON n.d = d.id"
        );
        assert_eq!(
            sw("SELECT 1 FROM notes n LEFT JOIN docs AS x ON x.id = n.id, docs"),
            "SELECT 1 FROM notes n LEFT JOIN `__sprig_rls_docs` AS x ON x.id = n.id, `__sprig_rls_docs` AS `docs`"
        );
        assert_eq!(
            sw("SELECT t1_d2_g1_s1.docs.id, t1_d2_g1_s1.docs.* FROM t1_d2_g1_s1.docs"),
            "SELECT docs.id, docs.* FROM t1_d2_g1_s1.`__sprig_rls_docs` AS `docs`"
        );
        // Subqueries, nested joins, derived tables.
        assert_eq!(
            sw(
                "SELECT (SELECT COUNT(*) FROM docs) FROM (notes JOIN docs ON 1) WHERE x IN (SELECT id FROM docs)"
            ),
            "SELECT (SELECT COUNT(*) FROM `__sprig_rls_docs` AS `docs`) FROM (notes JOIN `__sprig_rls_docs` AS `docs` ON 1) WHERE x IN (SELECT id FROM `__sprig_rls_docs` AS `docs`)"
        );
        // Index hints go: a view has no indexes (1176).
        assert_eq!(
            sw("SELECT id FROM docs USE INDEX (k) FORCE KEY FOR JOIN (k2) WHERE 1"),
            "SELECT id FROM `__sprig_rls_docs` AS `docs`  WHERE 1"
        );
        // Columns, other schemas, functions and CTEs named like the table.
        assert_eq!(sw("SELECT docs FROM notes"), "SELECT docs FROM notes");
        assert_eq!(sw("SELECT 1 FROM other.docs"), "SELECT 1 FROM other.docs");
        assert_eq!(
            sw("WITH docs AS (SELECT 1) SELECT * FROM docs"),
            "WITH docs AS (SELECT 1) SELECT * FROM docs"
        );
        // A non-recursive CTE's own body still reads the table.
        assert_eq!(
            sw("WITH docs AS (SELECT * FROM docs) SELECT id FROM docs"),
            "WITH docs AS (SELECT * FROM `__sprig_rls_docs` AS `docs`) SELECT id FROM docs"
        );
        assert_eq!(
            sw("SELECT 1 FROM JSON_TABLE('[]', '$' COLUMNS (a INT PATH '$')) j"),
            "SELECT 1 FROM JSON_TABLE('[]', '$' COLUMNS (a INT PATH '$')) j"
        );
        assert_eq!(
            sw("SELECT STRAIGHT_JOIN a FROM notes STRAIGHT_JOIN docs"),
            "SELECT STRAIGHT_JOIN a FROM notes STRAIGHT_JOIN `__sprig_rls_docs` AS `docs`"
        );
    }

    #[test]
    fn writes_target_the_view_when_they_read() {
        assert_eq!(
            sw("UPDATE docs SET n = n + 1 WHERE id = 2"),
            "UPDATE `__sprig_rls_docs` AS `docs` SET n = n + 1 WHERE id = 2"
        );
        assert_eq!(
            sw("UPDATE LOW_PRIORITY docs d, notes SET d.n = 1"),
            "UPDATE LOW_PRIORITY `__sprig_rls_docs` d, notes SET d.n = 1"
        );
        assert_eq!(
            sw("DELETE FROM docs WHERE id = 2"),
            "DELETE FROM `__sprig_rls_docs` AS `docs` WHERE id = 2"
        );
        assert_eq!(
            sw("DELETE d FROM docs d JOIN notes n ON n.d = d.id"),
            "DELETE d FROM `__sprig_rls_docs` d JOIN notes n ON n.d = d.id"
        );
        // A plain insert needs no SELECT; an upsert reads the row.
        assert_eq!(
            sw("INSERT INTO docs (id) VALUES (1)"),
            "INSERT INTO docs (id) VALUES (1)"
        );
        assert_eq!(
            sw("INSERT INTO docs (id) VALUES (1) ON DUPLICATE KEY UPDATE n = n + 1"),
            "INSERT INTO `__sprig_rls_docs` (id) VALUES (1) ON DUPLICATE KEY UPDATE n = n + 1"
        );
        assert_eq!(
            sw("INSERT INTO notes SELECT * FROM docs"),
            "INSERT INTO notes SELECT * FROM `__sprig_rls_docs` AS `docs`"
        );
        assert_eq!(sw("TABLE docs"), "TABLE `__sprig_rls_docs` AS `docs`");
        // Metadata and DDL keep the table.
        for s in [
            "SHOW COLUMNS FROM docs",
            "DESCRIBE docs",
            "ALTER TABLE docs ADD c INT",
            "ANALYZE TABLE docs",
        ] {
            assert_eq!(sw(s), s);
        }
        assert_eq!(
            sw("CREATE TEMPORARY TABLE x AS SELECT * FROM docs"),
            "CREATE TEMPORARY TABLE x AS SELECT * FROM `__sprig_rls_docs` AS `docs`"
        );
    }

    #[test]
    fn stored_program_bodies_are_swapped_per_statement() {
        let body = "CREATE DEFINER=`t1_u2`@`%` PROCEDURE p() BEGIN DECLARE n INT; \
                    SELECT COUNT(*) INTO n FROM docs; IF n > 0 THEN UPDATE docs SET a = 1; \
                    ELSE DELETE FROM docs; END IF; l1: LOOP LEAVE l1; END LOOP; END";
        assert_eq!(
            sw(body),
            "CREATE DEFINER=`t1_u2`@`%` PROCEDURE p() BEGIN DECLARE n INT; \
             SELECT COUNT(*) INTO n FROM `__sprig_rls_docs` AS `docs`; IF n > 0 THEN UPDATE `__sprig_rls_docs` AS `docs` SET a = 1; \
             ELSE DELETE FROM `__sprig_rls_docs` AS `docs`; END IF; l1: LOOP LEAVE l1; END LOOP; END"
        );
        let trig = "CREATE TRIGGER tr AFTER INSERT ON notes FOR EACH ROW UPDATE docs SET n = n + 1";
        assert_eq!(
            sw(trig),
            "CREATE TRIGGER tr AFTER INSERT ON notes FOR EACH ROW UPDATE `__sprig_rls_docs` AS `docs` SET n = n + 1"
        );
        let view = "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `uv` AS select `d`.`id` AS `id` from (`docs` `d` left join `other` `o` on(`o`.`id` = `d`.`id`))";
        assert_eq!(
            sw(view),
            "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `uv` AS select `d`.`id` AS `id` from (`__sprig_rls_docs` `d` left join `other` `o` on(`o`.`id` = `d`.`id`))"
        );
    }

    #[test]
    fn the_swap_knows_its_tenant_and_temporary_tables() {
        let mut v = ViewSwap {
            tenant: 1,
            ..ViewSwap::default()
        };
        v.tables
            .entry(2)
            .or_default()
            .insert("docs".into(), view_name("docs", 7));
        let lx = LexOptions::mariadb();
        let a = |v: &ViewSwap, s: &str, d: Option<&str>| {
            v.apply(s.as_bytes(), &lx, d)
                .map(|b| String::from_utf8(b).unwrap())
        };
        assert_eq!(
            a(&v, "SELECT * FROM docs", Some("t1_d2_g3_s1")).as_deref(),
            Some("SELECT * FROM `__sprig_rls_docs` AS `docs`")
        );
        assert_eq!(a(&v, "SELECT * FROM docs", Some("t1_d22_g1_s1")), None);
        assert_eq!(a(&v, "SELECT * FROM t11_d2_g1_s1.docs", None), None);
        assert_eq!(a(&v, "SELECT * FROM notes", Some("t1_d2_g1_s1")), None);
        v.temporary.insert("docs".into());
        assert_eq!(a(&v, "SELECT * FROM docs", Some("t1_d2_g1_s1")), None);
        // A gathered read's copy shadows the table in its own schema only.
        let mut w = ViewSwap {
            tenant: 1,
            ..ViewSwap::default()
        };
        w.tables
            .entry(2)
            .or_default()
            .insert("docs".into(), view_name("docs", 7));
        assert!(w.note_temporary(
            "SET STATEMENT sql_mode='' FOR CREATE TEMPORARY TABLE `t1_d2_g1_s1`.`docs` (id INT)",
            None
        ));
        assert_eq!(a(&w, "SELECT * FROM docs", Some("t1_d2_g1_s1")), None);
        assert_eq!(a(&w, "SELECT * FROM t1_d2_g1_s1.docs", Some("x")), None);
        assert_eq!(
            a(
                &w,
                "INSERT INTO docs SELECT * FROM `t1_d2_g4_s2`.`docs`",
                Some("t1_d2_g1_s1")
            )
            .as_deref(),
            Some("INSERT INTO docs SELECT * FROM `t1_d2_g4_s2`.`__sprig_rls_docs` AS `docs`")
        );
        assert!(!w.note_temporary("SELECT 1", None));
        let long = "x".repeat(60);
        assert_eq!(
            view_name(&long, 9),
            format!("__sprig_rls_{}~9", "x".repeat(40))
        );
        assert!(view_name(&long, 9).len() <= 64);
    }

    #[test]
    fn statements_naming_invisible_columns_read_the_all_columns_view() {
        let mut v = ViewSwap {
            tenant: 1,
            ..ViewSwap::default()
        };
        v.tables
            .entry(2)
            .or_default()
            .insert("docs".into(), view_name("docs", 7));
        v.hidden.insert(
            view_name("docs", 7),
            (
                hidden_view_name("docs", 7),
                vec!["_sprig_rk".into(), "note".into()],
            ),
        );
        let lx = LexOptions::mariadb();
        let a = |s: &str| {
            v.apply(s.as_bytes(), &lx, Some("t1_d2_g1_s1"))
                .map(|b| String::from_utf8(b).unwrap())
        };
        // `*` reads the visible columns' view.
        assert_eq!(
            a("SELECT * FROM docs").as_deref(),
            Some("SELECT * FROM `__sprig_rls_docs` AS `docs`")
        );
        // The router's and the user's names of invisible columns.
        assert_eq!(
            a("SELECT id, `_sprig_rk` FROM docs WHERE _sprig_rk > 5").as_deref(),
            Some("SELECT id, `_sprig_rk` FROM `__sprig_rlsh_docs` AS `docs` WHERE _sprig_rk > 5")
        );
        assert_eq!(
            a("SELECT d.NOTE FROM docs d").as_deref(),
            Some("SELECT d.NOTE FROM `__sprig_rlsh_docs` d")
        );
        assert_eq!(
            a("SELECT COUNT(*), a * b FROM docs WHERE note IS NULL").as_deref(),
            Some("SELECT COUNT(*), a * b FROM `__sprig_rlsh_docs` AS `docs` WHERE note IS NULL")
        );
        // `*` with an invisible column isn't swapped (the account's 1142).
        assert_eq!(a("SELECT *, note FROM docs"), None);
        assert_eq!(a("SELECT d.*, d.note FROM docs d"), None);
        assert_eq!(a("SELECT DISTINCT * FROM docs WHERE note = 1"), None);
        assert_eq!(
            hidden_view_name(&"y".repeat(60), 12),
            format!("__sprig_rlsh_{}~12", "y".repeat(39))
        );
        assert!(hidden_view_name(&"y".repeat(51), 123_456_789).len() <= 64);
        assert!(is_policy_view("__sprig_rlsh_docs") && is_policy_view("__SPRIG_RLS_docs"));
        assert!(!is_policy_view("__sprig_rls_ctx") && !is_policy_view("docs"));
    }

    #[test]
    fn definitions_read_as_written() {
        for (swapped, orig) in [
            (
                "select 1 from `__sprig_rls_docs` AS `docs` where x",
                "select 1 from `docs` where x",
            ),
            (
                "from (`s`.`__sprig_rls_docs` `d` join `__sprig_rls_docs` `docs` on(`docs`.`id` = `d`.`id`))",
                "from (`s`.`docs` `d` join `docs` on(`docs`.`id` = `d`.`id`))",
            ),
            (
                "UPDATE `__sprig_rls_docs` AS `docs` SET a = 1",
                "UPDATE `docs` SET a = 1",
            ),
            ("select `__sprig_rls_ctx`()", "select `__sprig_rls_ctx`()"),
            (
                "select `n` from `__sprig_rlsh_docs` `docs` join `__sprig_rls_h_x` AS `h_x`",
                "select `n` from `docs` join `h_x`",
            ),
            ("nothing here", "nothing here"),
        ] {
            assert_eq!(unswap(swapped), orig);
        }
    }
}
