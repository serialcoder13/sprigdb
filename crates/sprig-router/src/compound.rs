//! Compound statements keyed by one distribution key (the Redis front
//! end's guarded batches on a sharded Redis database or key-value
//! namespace).
//!
//! The planner can't parse `BEGIN NOT ATOMIC ... END`, so a compound
//! statement over sharded tables is refused (39420). The key-value front
//! ends send one such statement per command, every table in it one of
//! the key-value tables of a single key: the key table and its element
//! tables, grouped by the advisor on `_sprig_dk`. [`plan_keyed_compound`]
//! routes it to that key's shard without parsing it: it finds the
//! qualified table names and the `_sprig_dk = X'…'` literals in the text,
//! and when every sharded table it names is keyed by `_sprig_dk` and the
//! text names exactly one distribution key, it is a [`Plan::Pass`] to the
//! shard owning that key, with each table's schema replaced.
//!
//! Only the gateway's own internal statements take this path (the caller
//! checks), since nothing here checks that each statement stays on the
//! named key.

use std::ops::Range;

use crate::plan::{Plan, Target};
use crate::planner::PlanCtx;
use crate::topology::{KeyValue, TableEntry, TableKind};

/// The distribution-key column the key-value tables route on.
pub const DK_COLUMN: &str = "_sprig_dk";

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    /// An identifier, unquoted (`range` covers it) or backquoted (`range`
    /// covers the quotes).
    Ident(String, Range<usize>),
    Dot,
    Eq,
    /// A hex literal's bytes, or a quoted string's raw bytes.
    Lit(Vec<u8>),
    Other,
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Splits `sql` into the few tokens this module reads; `None` on text it
/// doesn't understand (an unterminated quote or comment).
fn tokens(sql: &str) -> Option<Vec<Tok>> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        match c {
            b' ' | b'\t' | b'\r' | b'\n' => i += 1,
            b'/' if b.get(i + 1) == Some(&b'*') => {
                let end = sql[i + 2..].find("*/")?;
                i += 2 + end + 2;
            }
            b'-' if b.get(i + 1) == Some(&b'-')
                && b.get(i + 2).is_none_or(u8::is_ascii_whitespace) =>
            {
                i = sql[i..].find('\n').map_or(b.len(), |n| i + n + 1);
            }
            b'#' => i = sql[i..].find('\n').map_or(b.len(), |n| i + n + 1),
            b'`' => {
                let start = i;
                let mut name = Vec::new();
                i += 1;
                loop {
                    let x = *b.get(i)?;
                    if x == b'`' {
                        if b.get(i + 1) == Some(&b'`') {
                            name.push(b'`');
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    name.push(x);
                    i += 1;
                }
                out.push(Tok::Ident(String::from_utf8(name).ok()?, start..i));
            }
            b'\'' | b'"' => {
                let q = c;
                let mut raw = Vec::new();
                i += 1;
                loop {
                    let x = *b.get(i)?;
                    if x == b'\\' {
                        raw.push(x);
                        raw.push(*b.get(i + 1)?);
                        i += 2;
                        continue;
                    }
                    if x == q {
                        if b.get(i + 1) == Some(&q) {
                            raw.push(q);
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    raw.push(x);
                    i += 1;
                }
                out.push(Tok::Lit(raw));
            }
            b'x' | b'X' if b.get(i + 1) == Some(&b'\'') => {
                let start = i + 2;
                let len = sql[start..].find('\'')?;
                let hex = &b[start..start + len];
                if !hex.len().is_multiple_of(2) {
                    return None;
                }
                let mut v = Vec::with_capacity(hex.len() / 2);
                for p in hex.chunks(2) {
                    v.push(hex_val(p[0])? << 4 | hex_val(p[1])?);
                }
                out.push(Tok::Lit(v));
                i = start + len + 1;
            }
            b'.' => {
                out.push(Tok::Dot);
                i += 1;
            }
            b'=' => {
                out.push(Tok::Eq);
                i += 1;
            }
            _ if c.is_ascii_alphanumeric() || c == b'_' || c == b'$' => {
                let start = i;
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$')
                {
                    i += 1;
                }
                out.push(Tok::Ident(sql[start..i].to_owned(), start..i));
            }
            _ => {
                out.push(Tok::Other);
                i += if c.is_ascii() {
                    1
                } else {
                    sql[i..].chars().next().map_or(1, char::len_utf8)
                };
            }
        }
    }
    Some(out)
}

/// Whether `sql` is a compound statement (`BEGIN NOT ATOMIC`).
#[must_use]
pub fn is_compound(sql: &str) -> bool {
    let Some(t) = tokens(sql) else {
        return false;
    };
    let word = |i: usize, w: &str| matches!(t.get(i), Some(Tok::Ident(s, r)) if s.eq_ignore_ascii_case(w) && sql.as_bytes()[r.start] != b'`');
    word(0, "begin") && word(1, "not") && word(2, "atomic")
}

/// `sql` (a compound statement) as a [`Plan::Pass`] to the shard of its one
/// distribution key, or `None` when it isn't such a statement: not a
/// compound statement, no sharded table, a sharded table not keyed by
/// `_sprig_dk`, a reference table, no or several distinct `_sprig_dk`
/// literals, or shards on several clusters.
#[must_use]
pub fn plan_keyed_compound(sql: &str, cx: &PlanCtx<'_>) -> Option<Plan> {
    if !is_compound(sql) {
        return None;
    }
    let t = tokens(sql)?;
    let mut tables: Vec<(Range<usize>, &TableEntry)> = Vec::new();
    let mut dk: Option<Vec<u8>> = None;
    let mut i = 0;
    while i < t.len() {
        match (&t[i], t.get(i + 1), t.get(i + 2)) {
            (Tok::Ident(schema, r), Some(Tok::Dot), Some(Tok::Ident(table, _))) => {
                if let Some(e) = cx.topo.table(schema, table) {
                    tables.push((r.clone(), e));
                }
                i += 2;
                continue;
            }
            (Tok::Ident(col, _), Some(Tok::Eq), Some(Tok::Lit(v)))
                if col.eq_ignore_ascii_case(DK_COLUMN) =>
            {
                match &dk {
                    Some(d) if d != v => return None,
                    _ => dk = Some(v.clone()),
                }
                i += 3;
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    let dk = dk?;
    if tables.is_empty() {
        return None;
    }
    let mut target: Option<Target> = None;
    let mut edits: Vec<(Range<usize>, String)> = Vec::new();
    for (r, e) in &tables {
        match &e.kind {
            TableKind::Sharded { key, .. } if key.eq_ignore_ascii_case(DK_COLUMN) => {}
            _ => return None,
        }
        let g = &e.group;
        let s = g.shard_of(&KeyValue::Bytes(dk.clone()))?;
        let loc = g.shards.get(s)?;
        match &target {
            Some(t) if t.cluster != loc.cluster => return None,
            Some(_) => {}
            None => {
                target = Some(Target {
                    cluster: loc.cluster.clone(),
                    shard: loc.shard,
                    sql: String::new(),
                    at: g.at(s),
                });
            }
        }
        edits.push((r.clone(), format!("`{}`", loc.schema.replace('`', "``"))));
    }
    let mut target = target?;
    let mut out = String::with_capacity(sql.len() + 32);
    let mut at = 0;
    for (r, s) in edits {
        out.push_str(&sql[at..r.start]);
        out.push_str(&s);
        at = r.end;
    }
    out.push_str(&sql[at..]);
    target.sql = out;
    Some(Plan::Pass(target))
}

#[cfg(test)]
mod tests;
