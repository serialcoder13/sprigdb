//! The SQL side of hiding automatic indexes from metadata:
//!
//! - **Reserved names.** User DDL that names an index `sprig_auto_*`
//!   (case-insensitive) fails with 39050 `ER_SPRIG_RESERVED_NAME`: `CREATE
//!   INDEX`, `ALTER TABLE ... ADD INDEX|KEY|UNIQUE|...`, an index or constraint
//!   in `CREATE TABLE`, `RENAME INDEX ... TO`, `ALTER INDEX ... IGNORED`. A
//!   `DROP INDEX` of one is allowed only while the session shows automatic
//!   indexes (`@sprig_show_auto_indexes = 1`); otherwise it fails the same way.
//!   [`index_changes`] reads the index names of a DDL statement and
//!   [`check_reserved`] applies the rule; [`plan`](super::plan()) calls both
//!   for every DDL statement.
//! - **The session flag.** `SET @sprig_show_auto_indexes = 1` (or `0`,
//!   `ON`, `OFF`, `TRUE`, `FALSE`, a quoted `'1'`) is the gateway's, never
//!   MariaDB's: [`session_flag`] recognizes it and the gateway keeps it in
//!   the session's [`Visibility`].
//! - **The 64-index rule.** MariaDB allows 64 indexes per table (error
//!   1069). Before a user statement that adds indexes would pass the
//!   limit, the gateway drops the lowest-value automatic indexes on that
//!   table: [`IndexChanges::net_added`] counts what a statement adds and
//!   [`make_room`] picks what to drop.
//!
//! `CREATE TABLE ... LIKE` never copies automatic indexes:
//! [`create_like_sql`](super::hide::create_like_sql) filters them whatever
//! the session may see.

use sprig_common::names::is_auto_index;

use super::NameError;
use super::hide::Visibility;
use crate::lex::{self, LexOptions, Token, TokenKind, ident_value};

/// MariaDB's limit of indexes per table (`MAX_KEY`, error 1069).
pub const MAX_INDEXES_PER_TABLE: usize = 64;

/// The user variable that shows automatic indexes.
pub const SHOW_AUTO_INDEXES_VAR: &str = "sprig_show_auto_indexes";

/// What one DDL statement does to index names.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IndexChanges {
    /// Indexes it creates: the name, `None` for an unnamed one (MariaDB
    /// names it after its first column), `PRIMARY` for a primary key.
    pub created: Vec<Option<String>>,
    /// Constraint and foreign-key names, which may become index names.
    pub constraints: Vec<String>,
    /// Indexes it drops (`PRIMARY` for `DROP PRIMARY KEY`).
    pub dropped: Vec<String>,
    /// `RENAME INDEX old TO new`.
    pub renamed: Vec<(String, String)>,
    /// `ALTER INDEX name [NOT] IGNORED|INVISIBLE|VISIBLE`.
    pub altered: Vec<String>,
}

impl IndexChanges {
    /// Whether the statement touches no index name.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.created.is_empty()
            && self.constraints.is_empty()
            && self.dropped.is_empty()
            && self.renamed.is_empty()
            && self.altered.is_empty()
    }

    /// Indexes the statement adds to its table, net of the ones it drops.
    /// Foreign keys aren't counted: MariaDB adds an index for one only
    /// when no existing index starts with its columns.
    #[must_use]
    pub fn net_added(&self) -> i64 {
        let n = |v: usize| i64::try_from(v).unwrap_or(i64::MAX);
        n(self.created.len()) - n(self.dropped.len())
    }
}

/// Non-trivia tokens with their text.
struct Words<'a> {
    text: &'a [u8],
    toks: Vec<Token>,
}

impl Words<'_> {
    fn kw(&self, i: usize, w: &str) -> bool {
        self.toks.get(i).is_some_and(|t| {
            t.kind == TokenKind::Word && t.bytes(self.text).eq_ignore_ascii_case(w.as_bytes())
        })
    }

    fn kw_any(&self, i: usize, ws: &[&str]) -> bool {
        ws.iter().any(|w| self.kw(i, w))
    }

    fn punct(&self, i: usize, c: u8) -> bool {
        self.toks
            .get(i)
            .is_some_and(|t| t.kind == TokenKind::Punct && self.text[t.start as usize] == c)
    }

    /// An identifier (a word that isn't a keyword here, or a quoted name).
    fn name(&self, i: usize) -> Option<String> {
        let t = self.toks.get(i)?;
        match t.kind {
            TokenKind::QuotedIdent => {
                Some(String::from_utf8_lossy(&ident_value(t.bytes(self.text))).into_owned())
            }
            TokenKind::Word
                if !self.kw_any(
                    i,
                    &[
                        "USING",
                        "ON",
                        "TO",
                        "IF",
                        "TYPE",
                        "COMMENT",
                        "KEY_BLOCK_SIZE",
                    ],
                ) =>
            {
                Some(String::from_utf8_lossy(t.bytes(self.text)).into_owned())
            }
            _ => None,
        }
    }

    /// Skips `IF EXISTS` / `IF NOT EXISTS` at `i`.
    fn skip_if(&self, mut i: usize) -> usize {
        if self.kw(i, "IF") {
            i += 1;
            if self.kw(i, "NOT") {
                i += 1;
            }
            if self.kw(i, "EXISTS") {
                i += 1;
            }
        }
        i
    }

    /// The index after the `)` matching the `(` at `i`.
    fn after_group(&self, i: usize) -> usize {
        let mut depth = 0usize;
        let mut j = i;
        while j < self.toks.len() {
            if self.punct(j, b'(') {
                depth += 1;
            } else if self.punct(j, b')') {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return j + 1;
                }
            }
            j += 1;
        }
        j
    }

    /// The start of each comma-separated item between `from` and `to` at
    /// the outermost level.
    fn items(&self, from: usize, to: usize) -> Vec<usize> {
        let mut out = vec![from];
        let mut depth = 0usize;
        for j in from..to {
            if self.punct(j, b'(') {
                depth += 1;
            } else if self.punct(j, b')') {
                depth = depth.saturating_sub(1);
            } else if depth == 0 && self.punct(j, b',') {
                out.push(j + 1);
            }
        }
        out
    }
}

/// A key definition at `i` (`ADD`'s argument, or an element of `CREATE
/// TABLE`): `[CONSTRAINT [sym]] {INDEX|KEY|UNIQUE [INDEX|KEY]|FULLTEXT
/// [INDEX|KEY]|SPATIAL [INDEX|KEY]|VECTOR {INDEX|KEY}|PRIMARY KEY|FOREIGN
/// KEY} [IF NOT EXISTS] [name] (...)`. Anything else (a column, `CHECK`,
/// `PERIOD`, `PARTITION`) records nothing.
fn key_def(w: &Words<'_>, mut i: usize, out: &mut IndexChanges) {
    let mut symbol = None;
    if w.kw(i, "CONSTRAINT") {
        i += 1;
        i = w.skip_if(i);
        if !w.kw_any(i, &["PRIMARY", "UNIQUE", "FOREIGN", "CHECK"]) {
            symbol = w.name(i);
            i += 1;
        }
    }
    let unique_like = ["UNIQUE", "FULLTEXT", "SPATIAL"];
    let named = if w.kw_any(i, &["INDEX", "KEY"]) {
        i += 1;
        true
    } else if w.kw_any(i, &unique_like) {
        i += 1;
        if w.kw_any(i, &["INDEX", "KEY"]) {
            i += 1;
        }
        true
    } else if w.kw(i, "VECTOR") && w.kw_any(i + 1, &["INDEX", "KEY"]) {
        i += 2;
        true
    } else if w.kw(i, "PRIMARY") && w.kw(i + 1, "KEY") {
        out.created.push(Some("PRIMARY".to_owned()));
        return;
    } else if w.kw(i, "FOREIGN") && w.kw(i + 1, "KEY") {
        i = w.skip_if(i + 2);
        if let Some(n) = w.name(i) {
            out.constraints.push(n);
        }
        if let Some(s) = symbol {
            out.constraints.push(s);
        }
        return;
    } else {
        false
    };
    if !named {
        if let Some(s) = symbol {
            // CONSTRAINT x CHECK (...): not an index.
            if !w.kw(i, "CHECK") {
                out.constraints.push(s);
            }
        }
        return;
    }
    i = w.skip_if(i);
    let name = if w.punct(i, b'(') { None } else { w.name(i) };
    out.created.push(name.or_else(|| symbol.clone()));
    if let Some(s) = symbol {
        out.constraints.push(s);
    }
}

/// One `ALTER TABLE` specification at `i`.
fn alter_spec(w: &Words<'_>, i: usize, out: &mut IndexChanges) {
    if w.kw(i, "ADD") {
        let j = w.skip_if(i + 1);
        key_def(w, j, out);
    } else if w.kw(i, "DROP") {
        if w.kw_any(i + 1, &["INDEX", "KEY"]) {
            if let Some(n) = w.name(w.skip_if(i + 2)) {
                out.dropped.push(n);
            }
        } else if w.kw(i + 1, "PRIMARY") && w.kw(i + 2, "KEY") {
            out.dropped.push("PRIMARY".to_owned());
        } else if w.kw(i + 1, "CONSTRAINT") {
            // A UNIQUE constraint's index goes with it; MariaDB says which.
            if let Some(n) = w.name(w.skip_if(i + 2)) {
                out.constraints.push(n);
            }
        }
    } else if w.kw(i, "RENAME") && w.kw_any(i + 1, &["INDEX", "KEY"]) {
        if let (Some(from), true, Some(to)) = (w.name(i + 2), w.kw(i + 3, "TO"), w.name(i + 4)) {
            out.renamed.push((from, to));
        }
    } else if w.kw(i, "ALTER")
        && w.kw_any(i + 1, &["INDEX", "KEY"])
        && let Some(n) = w.name(i + 2)
    {
        out.altered.push(n);
    }
}

/// The index names a DDL statement (one statement's text) creates,
/// drops, renames or alters; `None` when it isn't `CREATE TABLE`, `ALTER
/// TABLE`, `CREATE INDEX` or `DROP INDEX`.
#[must_use]
pub fn index_changes(text: &[u8], opts: &LexOptions) -> Option<IndexChanges> {
    let toks: Vec<Token> = lex::lex(text, opts)
        .ok()?
        .into_iter()
        .filter(|t| !t.kind.is_trivia())
        .collect();
    let w = Words { text, toks };
    let mut out = IndexChanges::default();
    let mut i = 1;
    if w.kw(0, "CREATE") {
        if w.kw(i, "OR") && w.kw(i + 1, "REPLACE") {
            i += 2;
        }
        while w.kw_any(i, &["ONLINE", "OFFLINE", "TEMPORARY"]) {
            i += 1;
        }
        let kind_at = i;
        if w.kw_any(i, &["UNIQUE", "FULLTEXT", "SPATIAL", "VECTOR"]) {
            i += 1;
        }
        if w.kw(i, "INDEX") {
            let j = w.skip_if(i + 1);
            out.created.push(w.name(j));
            return Some(out);
        }
        if i != kind_at || !w.kw(i, "TABLE") {
            return None;
        }
        // CREATE TABLE [IF NOT EXISTS] name ( elements ) ...
        let mut j = w.skip_if(i + 1);
        while j < w.toks.len() && !w.punct(j, b'(') && !w.kw_any(j, &["LIKE", "SELECT", "AS"]) {
            j += 1;
        }
        if !w.punct(j, b'(') {
            return Some(out);
        }
        let end = w.after_group(j);
        for start in w.items(j + 1, end.saturating_sub(1)) {
            key_def(&w, start, &mut out);
        }
        return Some(out);
    }
    if w.kw(0, "DROP") {
        if w.kw_any(i, &["ONLINE", "OFFLINE"]) {
            i += 1;
        }
        if !w.kw(i, "INDEX") {
            return None;
        }
        let j = w.skip_if(i + 1);
        if let Some(n) = w.name(j) {
            out.dropped.push(n);
        }
        return Some(out);
    }
    if w.kw(0, "ALTER") {
        while w.kw_any(i, &["ONLINE", "OFFLINE", "IGNORE"]) {
            i += 1;
        }
        if !w.kw(i, "TABLE") {
            return None;
        }
        // ALTER TABLE [IF EXISTS] [db.]name [WAIT n | NOWAIT] spec, spec ...
        let mut j = w.skip_if(i + 1);
        j += 1;
        while w.punct(j, b'.') {
            j += 2;
        }
        if w.kw(j, "WAIT") {
            j += 2;
        } else if w.kw(j, "NOWAIT") {
            j += 1;
        }
        for start in w.items(j, w.toks.len()) {
            alter_spec(&w, start, &mut out);
        }
        return Some(out);
    }
    None
}

/// Applies the reserved-name rule to `ch`: creating, renaming to or from, or
/// altering a `sprig_auto_*` index is 39050; dropping one is allowed only
/// while the session shows automatic indexes (`vis`).
///
/// # Errors
/// [`NameError::Reserved`] with the name.
pub fn check_reserved(ch: &IndexChanges, vis: Visibility) -> Result<(), NameError> {
    let reserved = |n: &str| is_auto_index(n);
    let bad = ch
        .created
        .iter()
        .flatten()
        .chain(&ch.constraints)
        .chain(&ch.altered)
        .chain(ch.renamed.iter().flat_map(|(a, b)| [a, b]))
        .find(|n| reserved(n));
    if let Some(n) = bad {
        return Err(NameError::Reserved(n.clone()));
    }
    let shown = vis.auto_indexes || vis.internal;
    if !shown && let Some(n) = ch.dropped.iter().find(|n| reserved(n)) {
        return Err(NameError::Reserved(n.clone()));
    }
    Ok(())
}

/// `SET @sprig_show_auto_indexes = <value>`, alone in its statement: the
/// new value. `None` when the statement doesn't set that variable.
///
/// # Errors
/// 39051 when the variable is set together with others or to something
/// that isn't a boolean literal (`1`, `0`, `ON`, `OFF`, `TRUE`, `FALSE`,
/// quoted or not).
pub fn session_flag(text: &[u8], opts: &LexOptions) -> Option<Result<bool, NameError>> {
    let toks: Vec<Token> = lex::lex(text, opts)
        .ok()?
        .into_iter()
        .filter(|t| {
            !(t.kind.is_trivia() || t.kind == TokenKind::Punct && text[t.start as usize] == b';')
        })
        .collect();
    let w = Words { text, toks };
    if !w.kw(0, "SET") {
        return None;
    }
    let is_flag = |t: &Token| {
        t.kind == TokenKind::UserVar && {
            let raw = &t.bytes(text)[1..];
            String::from_utf8_lossy(&ident_value(raw)).eq_ignore_ascii_case(SHOW_AUTO_INDEXES_VAR)
                || unquote_single(raw)
                    .is_some_and(|s| s.eq_ignore_ascii_case(SHOW_AUTO_INDEXES_VAR))
        }
    };
    let at = w.toks.iter().position(is_flag)?;
    let unsupported = || {
        Some(Err(NameError::Unsupported(format!(
            "set @{SHOW_AUTO_INDEXES_VAR} alone, to 1 or 0"
        ))))
    };
    if at != 1 || w.toks.len() != 4 {
        return unsupported();
    }
    let op = w.toks[2].bytes(text);
    if op != b"=" && op != b":=" {
        return unsupported();
    }
    let v = &w.toks[3];
    let raw = v.bytes(text);
    let value = match v.kind {
        TokenKind::String => lex::string_value(raw, opts.no_backslash_escapes)
            .map(|b| String::from_utf8_lossy(&b).into_owned()),
        TokenKind::Number | TokenKind::Word => Some(String::from_utf8_lossy(raw).into_owned()),
        _ => None,
    };
    match value.as_deref().map(str::to_ascii_uppercase).as_deref() {
        Some("1" | "ON" | "TRUE") => Some(Ok(true)),
        Some("0" | "OFF" | "FALSE") => Some(Ok(false)),
        _ => unsupported(),
    }
}

fn unquote_single(raw: &[u8]) -> Option<String> {
    let inner = raw.strip_prefix(b"'")?.strip_suffix(b"'")?;
    Some(String::from_utf8_lossy(inner).into_owned())
}

/// An index on a table, for [`make_room`].
#[derive(Debug, Clone, PartialEq)]
pub struct ExistingIndex {
    /// Its name.
    pub name: String,
    /// For an automatic index, its value (net ms saved per hour, measured
    /// or estimated); `None` when no catalog row explains it.
    pub value: Option<f64>,
}

/// The automatic indexes to drop so that a statement adding `adding`
/// indexes to a table with `existing` stays within
/// [`MAX_INDEXES_PER_TABLE`]: lowest value first (an index no row explains
/// first of all), ties by name. Empty when there is room already, or when
/// dropping every automatic index wouldn't make enough room: the statement
/// then fails with MariaDB's own 1069 and nothing is dropped for it.
#[must_use]
pub fn make_room(existing: &[ExistingIndex], adding: usize) -> Vec<String> {
    let total = existing.len() + adding;
    if total <= MAX_INDEXES_PER_TABLE {
        return Vec::new();
    }
    let need = total - MAX_INDEXES_PER_TABLE;
    let mut auto: Vec<&ExistingIndex> =
        existing.iter().filter(|e| is_auto_index(&e.name)).collect();
    if auto.len() < need {
        return Vec::new();
    }
    auto.sort_by(|a, b| {
        let va = a.value.unwrap_or(f64::NEG_INFINITY);
        let vb = b.value.unwrap_or(f64::NEG_INFINITY);
        va.total_cmp(&vb).then_with(|| a.name.cmp(&b.name))
    });
    auto.into_iter()
        .take(need)
        .map(|e| e.name.clone())
        .collect()
}

#[cfg(test)]
mod tests;
