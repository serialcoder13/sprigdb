//! Foreign keys between inheritable tables (Kiran's decisions of 11
//! November 2026, chapter 42 "Foreign keys and groups").
//!
//! A foreign key whose either end is an inheritable table is kept by the
//! gateway, never by MariaDB: the referenced row may live in an ancestor's
//! layer, in another tenant's schema. Declaring one links the two tables
//! into one **group**; a plain table at the other end becomes inheritable
//! (its primary key is its key), and sharing any table of a group shares
//! the group ([`crate::model::InheritState::group_of`]).
//!
//! ```text
//! CREATE TABLE t3 (..., FOREIGN KEY (a) REFERENCES t1 (id))
//!     the clause leaves the statement; t3 is created and made inheritable;
//!     the link is recorded (inherit_ext)
//! ALTER TABLE t1 ADD [CONSTRAINT n] FOREIGN KEY (a) REFERENCES t2 (id)
//!     existing rows are checked in every space (1452), then as above;
//!     refused while t1 is shared (39895: children's rows could violate it)
//! ALTER TABLE t1 DROP FOREIGN KEY n           the link goes
//! ALTER TABLE t SPRIG INHERITABLE ...          t's own MariaDB foreign keys
//!     (to and from t) are dropped in MariaDB and become links; the tables at
//!     their other ends become inheritable too
//! ```
//!
//! **Checks.** A write in space `s` of tenant `T` checks the values its
//! rows set against `s`'s merged view of the referenced table (1452); a
//! delete in `s` is refused (1451) while a row of `s`'s view or of any
//! space or tenant below `s` still references a deleted key. The second
//! uses an index of references per referenced key, `inherit_refs`, in the
//! catalog of the referenced table's tree root (decision IG4), so a delete
//! reads one index range instead of every descendant's table:
//!
//! ```text
//! writer (T, s), rows of F referencing keys K of R, after its write:
//!   claims (R's root, k, T, s, F, row key) for its rows' own values (upsert)
//!   deletion markers of K from s's levels and above: one whose deleter is
//!     still running (its layer row is locked) or whose deletion stands → 1452
//!   K in s's merged view of R, read fresh (as the provisioning account)  → else 1452
//! deleter (T, s), keys K of R leaving s's view, before its change:
//!   deletion markers (R's root, k, T, s)
//!   claims of K: from s or below → the claimed row, read with a NOWAIT lock:
//!     locked (a write in flight) or still referencing → 1451; else the
//!     claim is stale and goes;
//!   from above s → the claimed record in s's view still referencing → 1451
//! ```
//!
//! Claims are only added by writers and removed by deleters that find
//! them stale, so a rolled-back write leaves a claim that the next delete
//! clears. Every step outside the session's transaction runs as the
//! provisioning account, so the index works across clusters too.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use sprig_catalog::TenantCatalog;
use sprig_catalog::store::{Namespace, ObjectKind};
use sprig_common::ids::{DbId, ObjectId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_sql::lex::LexOptions;
use sprig_types::Done;

use crate::grammar::{Name, Toks, unsupported};
use crate::model::{InheritableObject, Link, ROOT_SPACE, ShareTarget, SpaceId};
use crate::plan::{Ctx, Resolved, physical_schema, state_of};
use crate::view::{ViewSql, q};
use crate::{Handled, RawRows, Runner};

// ------------------------------------------------------------ parsing

/// One foreign-key clause as the client wrote it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FkClause {
    /// `CONSTRAINT name`, if given.
    pub name: Option<String>,
    /// The referencing columns.
    pub columns: Vec<String>,
    /// The referenced table.
    pub ref_table: Name,
    /// The referenced columns (empty: the referenced table's key).
    pub ref_columns: Vec<String>,
    /// `ON DELETE` other than `RESTRICT` / `NO ACTION`, if any.
    pub unsupported_action: Option<String>,
    /// The bytes the clause takes in the statement, with one separating
    /// comma (removing them leaves a valid statement).
    pub span: (usize, usize),
}

/// One `ALTER TABLE` clause about foreign keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterFk {
    /// `ADD [CONSTRAINT n] FOREIGN KEY ...`.
    Add(FkClause),
    /// `DROP FOREIGN KEY [IF EXISTS] n`.
    Drop {
        /// The constraint.
        name: String,
        /// Its bytes, with one separating comma.
        span: (usize, usize),
    },
}

/// Reads `REFERENCES t [(cols)] [MATCH ...] [ON DELETE a] [ON UPDATE a]`
/// at `i` (the `REFERENCES` token), up to `end` (exclusive).
fn references(t: &Toks<'_>, i: usize, end: usize) -> Option<(Name, Vec<String>, Option<String>)> {
    if !t.is(i, "REFERENCES") {
        return None;
    }
    let (table, mut j) = t.name(i + 1)?;
    let mut cols = Vec::new();
    if t.punct(j, b'(') {
        let (c, next) = t.ident_list(j)?;
        cols = c;
        j = next;
    }
    let mut bad = None;
    while j < end {
        if t.is(j, "MATCH") {
            j += 2;
        } else if t.is(j, "ON") && (t.is(j + 1, "DELETE") || t.is(j + 1, "UPDATE")) {
            let delete = t.is(j + 1, "DELETE");
            let (act, n) = if t.is(j + 2, "SET") || t.is(j + 2, "NO") {
                (t.text(j + 2, j + 3), 4)
            } else {
                (t.text(j + 2, j + 2), 3)
            };
            let upper = act.to_ascii_uppercase();
            if delete && upper != "RESTRICT" && upper != "NO ACTION" {
                bad = Some(format!("ON DELETE {upper}"));
            }
            j += n;
        } else {
            j += 1;
        }
    }
    Some((table, cols, bad))
}

/// The top-level items of the parenthesis at `open`: `(start, end)` token
/// ranges, end exclusive.
fn items(t: &Toks<'_>, open: usize) -> Option<Vec<(usize, usize)>> {
    let close = t.close_of(open)?;
    let mut out = Vec::new();
    let mut a = open + 1;
    while a < close {
        let b = t.until(a, |j| t.punct(j, b',') || j >= close).min(close);
        out.push((a, b));
        a = b + 1;
    }
    Some(out)
}

/// The byte span of items `k` of `all`, with the comma before it (or,
/// for the first, the comma after it).
fn item_span(t: &Toks<'_>, all: &[(usize, usize)], k: usize, from: usize) -> (usize, usize) {
    let (a, b) = all[k];
    if k > 0 {
        // From the end of the previous item (its comma follows it).
        (t.end(all[k - 1].1 - 1), t.end(b - 1))
    } else if all.len() > 1 {
        (t.start(from.max(a)), t.start(all[1].0))
    } else {
        (t.start(from.max(a)), t.end(b - 1))
    }
}

/// `CREATE TABLE`'s name and foreign keys (table-level and column-level).
#[must_use]
pub fn scan_create(sql: &str, lex: &LexOptions) -> Option<(Name, Vec<FkClause>)> {
    let t = Toks::new(sql.as_bytes(), lex)?;
    let mut i = 1;
    if t.is(i, "OR") && t.is(i + 1, "REPLACE") {
        i += 2;
    }
    if t.is(i, "TEMPORARY") {
        i += 1;
    }
    if !t.is(i, "TABLE") {
        return None;
    }
    i += 1;
    if t.is(i, "IF") && t.is(i + 1, "NOT") && t.is(i + 2, "EXISTS") {
        i += 3;
    }
    let (table, open) = t.name(i)?;
    if !t.punct(open, b'(') {
        return None;
    }
    let all = items(&t, open)?;
    let mut out = Vec::new();
    for (k, &(a, b)) in all.iter().enumerate() {
        let mut j = a;
        let mut name = None;
        if t.is(j, "CONSTRAINT") {
            if t.is(j + 1, "FOREIGN") {
                j += 1;
            } else {
                name = t.ident(j + 1);
                j += 2;
            }
        }
        if t.is(j, "FOREIGN") && t.is(j + 1, "KEY") {
            j += 2;
            if !t.punct(j, b'(') {
                // An index name.
                j += 1;
            }
            let (columns, next) = t.ident_list(j)?;
            let (ref_table, ref_columns, bad) = references(&t, next, b)?;
            out.push(FkClause {
                name,
                columns,
                ref_table,
                ref_columns,
                unsupported_action: bad,
                span: item_span(&t, &all, k, a),
            });
            continue;
        }
        // A column definition with REFERENCES.
        if let Some(r) = (a..b).find(|&x| t.is(x, "REFERENCES")) {
            let col = t.ident(a)?;
            let (ref_table, ref_columns, bad) = references(&t, r, b)?;
            out.push(FkClause {
                name: None,
                columns: vec![col],
                ref_table,
                ref_columns,
                unsupported_action: bad,
                span: (t.end(r - 1), t.end(b - 1)),
            });
        }
    }
    Some((table, out))
}

/// `ALTER TABLE`'s foreign-key clauses.
#[must_use]
pub fn scan_alter(sql: &str, lex: &LexOptions) -> Option<(Name, Vec<AlterFk>, usize)> {
    let t = Toks::new(sql.as_bytes(), lex)?;
    let mut i = 1;
    if t.is(i, "ONLINE") || t.is(i, "IGNORE") {
        i += 1;
    }
    if !t.is(i, "TABLE") {
        return None;
    }
    i += 1;
    if t.is(i, "IF") && t.is(i + 1, "EXISTS") {
        i += 2;
    }
    let (table, start) = t.name(i)?;
    let mut all = Vec::new();
    let mut a = start;
    while a < t.len() {
        let b = t.until(a, |j| t.punct(j, b','));
        all.push((a, b));
        a = b + 1;
    }
    let mut out = Vec::new();
    for (k, &(a, b)) in all.iter().enumerate() {
        let span = || {
            if k > 0 {
                (t.end(all[k - 1].1 - 1), t.end(b - 1))
            } else if all.len() > 1 {
                (t.start(a), t.start(all[1].0))
            } else {
                (t.start(a), t.end(b - 1))
            }
        };
        if t.is(a, "DROP") && t.is(a + 1, "FOREIGN") && t.is(a + 2, "KEY") {
            let j = if t.is(a + 3, "IF") { a + 5 } else { a + 3 };
            out.push(AlterFk::Drop {
                name: t.ident(j)?,
                span: span(),
            });
            continue;
        }
        if !t.is(a, "ADD") {
            continue;
        }
        let mut j = a + 1;
        let mut name = None;
        if t.is(j, "CONSTRAINT") {
            if t.is(j + 1, "FOREIGN") {
                j += 1;
            } else {
                name = t.ident(j + 1);
                j += 2;
            }
        }
        if !(t.is(j, "FOREIGN") && t.is(j + 1, "KEY")) {
            continue;
        }
        j += 2;
        if t.is(j, "IF") && t.is(j + 1, "NOT") {
            j += 3;
        }
        if !t.punct(j, b'(') {
            j += 1;
        }
        let (columns, next) = t.ident_list(j)?;
        let (ref_table, ref_columns, bad) = references(&t, next, b)?;
        out.push(AlterFk::Add(FkClause {
            name,
            columns,
            ref_table,
            ref_columns,
            unsupported_action: bad,
            span: span(),
        }));
    }
    Some((table, out, start))
}

/// `sql` without the byte spans `cut`.
#[must_use]
pub fn strip(sql: &str, cut: &[(usize, usize)]) -> String {
    let mut spans = cut.to_vec();
    spans.sort_unstable();
    let mut out = String::with_capacity(sql.len());
    let mut at = 0;
    for (a, b) in spans {
        if a < at {
            continue;
        }
        out.push_str(&sql[at..a]);
        at = b;
    }
    out.push_str(&sql[at..]);
    out
}

/// The foreign keys of a `SHOW CREATE TABLE` text (MariaDB's form:
/// `` CONSTRAINT `n` FOREIGN KEY (`a`) REFERENCES `t` (`id`) ``), with the
/// referenced table's database when the text names one.
#[must_use]
pub fn scan_show_create(text: &str, lex: &LexOptions) -> Vec<FkClause> {
    let Some(t) = Toks::new(text.as_bytes(), lex) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for i in 0..t.len() {
        if !(t.is(i, "FOREIGN") && t.is(i + 1, "KEY")) {
            continue;
        }
        let name = (i >= 2 && t.is(i - 2, "CONSTRAINT"))
            .then(|| t.ident(i - 1))
            .flatten();
        let mut j = i + 2;
        if !t.punct(j, b'(') {
            j += 1;
        }
        let Some((columns, next)) = t.ident_list(j) else {
            continue;
        };
        let end = t.until(next, |x| t.punct(x, b',') || t.punct(x, b')'));
        if let Some((ref_table, ref_columns, bad)) = references(&t, next, end) {
            out.push(FkClause {
                name,
                columns,
                ref_table,
                ref_columns,
                unsupported_action: bad,
                span: (0, 0),
            });
        }
    }
    out
}

/// A clause as a [`Link`] of `o` onto `r` (whose key the clause must
/// reference), named `fallback` when unnamed.
///
/// # Errors
/// 39891 for an `ON DELETE` action other than `RESTRICT` / `NO ACTION`, a
/// key of another length or other columns than the referenced key, or
/// unknown columns.
pub fn link_of(
    c: &FkClause,
    o_columns: &[String],
    r: &InheritableObject,
    fallback: &str,
) -> Result<Link> {
    if let Some(a) = &c.unsupported_action {
        return Err(cascade_refused(a));
    }
    let ref_columns = if c.ref_columns.is_empty() {
        r.key.clone()
    } else {
        c.ref_columns.clone()
    };
    let same = ref_columns.len() == r.key.len()
        && ref_columns
            .iter()
            .zip(&r.key)
            .all(|(a, b)| a.eq_ignore_ascii_case(b));
    if !same || c.columns.len() != r.key.len() {
        return Err(unsupported(format!(
            "a foreign key onto inheritable table {} must reference its key ({})",
            r.table,
            r.key.join(", ")
        )));
    }
    let mut columns = Vec::new();
    for col in &c.columns {
        let found = o_columns
            .iter()
            .find(|x| x.eq_ignore_ascii_case(col))
            .ok_or_else(|| {
                SprigError::standard(
                    1072,
                    *b"42000",
                    format!("Key column '{col}' doesn't exist in table"),
                )
            })?;
        columns.push(found.clone());
    }
    Ok(Link {
        name: c.name.clone().unwrap_or_else(|| fallback.to_owned()),
        columns,
        ref_table: r.table.clone(),
        ref_columns: r.key.clone(),
    })
}

/// 39462 for `ON DELETE CASCADE | SET NULL | SET DEFAULT` on a foreign key
/// of an inheritable table.
#[must_use]
pub fn cascade_refused(action: &str) -> SprigError {
    SprigError::new(
        SprigCode::FkActionNotDistributed,
        format!(
            "{action} on a foreign key of an inheritable table: a delete of a referenced row is \
             refused while any space or tenant below still references it (use RESTRICT or NO \
             ACTION)"
        ),
    )
}

/// 39897: a descendant tenant already has `what`, which the parent's
/// change would bring down to it (Kiran's question Q3; the lead's default:
/// refuse the parent's change).
#[must_use]
pub fn clash(what: &str, named: Option<&str>) -> SprigError {
    SprigError::new(
        SprigCode::InheritNotOverridable,
        format!(
            "{what} already exists in {}: the change would come down to it through the share; \
             rename one of them first",
            named.map_or_else(
                || "a descendant tenant".to_owned(),
                |n| format!("tenant '{n}'")
            )
        ),
    )
}

/// MariaDB's name for the `n`th unnamed foreign key of `table`.
#[must_use]
pub fn default_name(table: &str, existing: &[Link], n: usize) -> String {
    let mut k = n;
    loop {
        let name = format!("{table}_ibfk_{k}");
        if !existing.iter().any(|l| l.name.eq_ignore_ascii_case(&name)) {
            return name;
        }
        k += 1;
    }
}

// --------------------------------------------------------- the errors

/// MySQL's 1452 for a link.
#[must_use]
pub fn child_row_fails(db: &str, table: &str, l: &Link) -> SprigError {
    SprigError::standard(
        1452,
        *b"23000",
        format!(
            "Cannot add or update a child row: a foreign key constraint fails ({}.{}, \
             CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({}))",
            q(db),
            q(table),
            q(&l.name),
            cols(&l.columns),
            q(&l.ref_table),
            cols(&l.ref_columns)
        ),
    )
}

/// MySQL's 1451 for a link, naming where the reference is when `where_`
/// says.
#[must_use]
pub fn parent_row_fails(db: &str, table: &str, l: &Link, where_: &str) -> SprigError {
    SprigError::standard(
        1451,
        *b"23000",
        format!(
            "Cannot delete or update a parent row: a foreign key constraint fails ({}.{}, \
             CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({})){where_}",
            q(db),
            q(table),
            q(&l.name),
            cols(&l.columns),
            q(&l.ref_table),
            cols(&l.ref_columns)
        ),
    )
}

fn cols(c: &[String]) -> String {
    c.iter().map(|x| q(x)).collect::<Vec<_>>().join(", ")
}

// ------------------------------------------------- levels of the tree

/// A space of a tenant: where a layer row, a claim or a marker is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct At {
    /// The tenant.
    pub tenant: TenantId,
    /// The space in it.
    pub space: SpaceId,
}

/// How a level relates to the session's level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kin {
    /// The session's level or one below it.
    AtOrBelow,
    /// Above it: its rows reach the session's view through the shares.
    Above,
    /// Another branch.
    Unrelated,
}

/// The session's level and what it knows of the tree around it.
#[derive(Debug, Clone)]
pub struct Here {
    /// The session's tenant and space.
    pub at: At,
    /// The session's space path (root first, itself last).
    pub path: Vec<SpaceId>,
    /// Spaces below the session's, in its tenant.
    pub below: BTreeSet<SpaceId>,
    /// The session tenant's ancestors, root first.
    pub ancestors: Vec<TenantId>,
}

impl Here {
    /// How `x` relates to the session's level, given `x`'s tenant's
    /// ancestors (root first).
    #[must_use]
    pub fn kin(&self, x: At, x_ancestors: &[TenantId]) -> Kin {
        if x.tenant == self.at.tenant {
            if x.space == self.at.space || self.below.contains(&x.space) {
                return Kin::AtOrBelow;
            }
            if self.path.contains(&x.space) {
                return Kin::Above;
            }
            return Kin::Unrelated;
        }
        if self.ancestors.contains(&x.tenant) {
            // A parent tenant's root space shares into this tenant's root.
            return if x.space == ROOT_SPACE {
                Kin::Above
            } else {
                Kin::Unrelated
            };
        }
        if x_ancestors.contains(&self.at.tenant) {
            // Child tenants hang below this tenant's root space.
            return if self.at.space == ROOT_SPACE {
                Kin::AtOrBelow
            } else {
                Kin::Unrelated
            };
        }
        Kin::Unrelated
    }
}

// ------------------------------------------------------ inherit_refs

/// The index of references and deletion markers, in the catalog of a
/// referenced table's tree root (`t<root>_sprig.inherit_refs`); `{refs}` in
/// the statements below stands for it.
pub const DDL_REFS: &str = "CREATE TABLE IF NOT EXISTS {refs} (
  ref_obj           BIGINT UNSIGNED NOT NULL,
  ref_key           VARBINARY(512)  NOT NULL,
  kind              ENUM('ref','del') NOT NULL,
  tenant            BIGINT UNSIGNED NOT NULL,
  space             BIGINT UNSIGNED NOT NULL,
  from_obj          BIGINT UNSIGNED NOT NULL,
  from_key          VARBINARY(512)  NOT NULL,
  link              VARCHAR(64)     NOT NULL,
  at                DATETIME(6)     NOT NULL,
  PRIMARY KEY (ref_obj, ref_key, kind, tenant, space, from_obj, from_key, link),
  KEY by_from (from_obj, from_key)
) ENGINE=InnoDB";

/// Adds [`DDL_REFS`]'s `by_from` index to a table made before it existed.
pub const DDL_REFS_BY_FROM: &str = "ALTER TABLE {refs} ADD INDEX IF NOT EXISTS by_from (from_obj, \
                                    from_key)";

/// The prefix of a **handover** claim's link name. A level that deletes or
/// hides rows of a referencing table that reaches child tenants hands their
/// references over: descendants' overrides of those rows become orphans
/// (the copy worker, asynchronously) and then hold the values themselves.
/// Until the worker has claimed them, the handover stands in for their
/// claims, so a delete of a referenced key can't slip between the two.
pub const HANDOVER: char = '~';

/// A key's canonical form in `inherit_refs`: its columns' text, joined by
/// `0x1f`, as a SQL hex literal.
#[must_use]
pub fn key_hex(parts: &[Vec<u8>]) -> String {
    let mut out = Vec::new();
    for (i, p) in parts.iter().enumerate() {
        if i > 0 {
            out.push(0x1f);
        }
        out.extend_from_slice(p);
    }
    if out.is_empty() {
        return "''".into();
    }
    let mut s = String::with_capacity(out.len() * 2 + 3);
    s.push_str("X'");
    for b in out {
        s.push_str(&format!("{b:02X}"));
    }
    s.push('\'');
    s
}

/// One reference of a written row: what the claim and the checks need.
#[derive(Debug, Clone)]
pub struct Ref {
    /// The referencing row's key, as `inherit_refs` stores it.
    pub from_hex: String,
    /// The referenced key, as `inherit_refs` stores it.
    pub ref_hex: String,
    /// The referenced key as a SQL tuple (point reads).
    pub ref_sql: String,
}

/// A claim or a marker read back from `inherit_refs`.
#[derive(Debug, Clone)]
pub struct Entry {
    /// Where it was made.
    pub at: At,
    /// The referencing object, in `at.tenant` (0 for a marker).
    pub from_obj: ObjectId,
    /// The referencing row's key, hex.
    pub from_hex: String,
    /// The referenced key, hex.
    pub ref_hex: String,
    /// The link's name (a marker: what the delete did, `hide` or `drop`).
    pub link: String,
}

/// The upsert of `refs`' claims.
#[must_use]
pub fn claims_sql(
    ref_obj: ObjectId,
    here: At,
    from_obj: ObjectId,
    link: &str,
    refs: &[Ref],
) -> String {
    let rows: Vec<String> = refs
        .iter()
        .map(|r| {
            format!(
                "({}, {}, 'ref', {}, {}, {}, {}, {}, UTC_TIMESTAMP(6))",
                ref_obj.0,
                r.ref_hex,
                here.tenant.0,
                here.space.0,
                from_obj.0,
                r.from_hex,
                crate::view::lit(link)
            )
        })
        .collect();
    format!(
        "INSERT INTO {{refs}} (ref_obj, ref_key, kind, tenant, space, from_obj, from_key, link, \
         at) VALUES {} ON DUPLICATE KEY UPDATE at = VALUES(at)",
        rows.join(", ")
    )
}

/// Reads entries of `kind` for keys `hex` of `ref_obj`.
#[must_use]
pub fn entries_sql(ref_obj: ObjectId, kind: &str, hex: &[String]) -> String {
    format!(
        "SELECT tenant, space, from_obj, HEX(from_key), HEX(ref_key), link FROM {{refs}} \
         WHERE ref_obj = {} AND kind = '{kind}' AND ref_key IN ({})",
        ref_obj.0,
        hex.join(", ")
    )
}

/// Parses [`entries_sql`]'s rows.
#[must_use]
pub fn parse_entries(rows: &RawRows) -> Vec<Entry> {
    let text = |row: &[Option<Vec<u8>>], i: usize| {
        row.get(i)
            .and_then(Option::as_ref)
            .map(|v| String::from_utf8_lossy(v).into_owned())
            .unwrap_or_default()
    };
    rows.iter()
        .map(|row| Entry {
            at: At {
                tenant: TenantId(text(row, 0).parse().unwrap_or(0)),
                space: SpaceId(text(row, 1).parse().unwrap_or(0)),
            },
            from_obj: ObjectId(text(row, 2).parse().unwrap_or(0)),
            from_hex: hex_lit(&text(row, 3)),
            ref_hex: hex_lit(&text(row, 4)),
            link: text(row, 5),
        })
        .collect()
}

fn hex_lit(h: &str) -> String {
    if h.is_empty() {
        "''".into()
    } else {
        format!("X'{h}'")
    }
}

/// The deletion markers of `hex` made at `here` (`what`: `hide` or
/// `drop`).
#[must_use]
pub fn markers_sql(ref_obj: ObjectId, here: At, what: &str, hex: &[String]) -> String {
    let rows: Vec<String> = hex
        .iter()
        .map(|h| {
            format!(
                "({}, {h}, 'del', {}, {}, 0, '', '{what}', UTC_TIMESTAMP(6))",
                ref_obj.0, here.tenant.0, here.space.0
            )
        })
        .collect();
    format!(
        "INSERT INTO {{refs}} (ref_obj, ref_key, kind, tenant, space, from_obj, from_key, link, \
         at) VALUES {} ON DUPLICATE KEY UPDATE link = VALUES(link), at = VALUES(at)",
        rows.join(", ")
    )
}

/// Deletes one entry.
#[must_use]
pub fn forget_sql(ref_obj: ObjectId, kind: &str, e: &Entry) -> String {
    format!(
        "DELETE FROM {{refs}} WHERE ref_obj = {} AND ref_key = {} AND kind = '{kind}' AND \
         tenant = {} AND space = {} AND from_obj = {} AND from_key = {} AND link = {}",
        ref_obj.0,
        e.ref_hex,
        e.at.tenant.0,
        e.at.space.0,
        e.from_obj.0,
        e.from_hex,
        crate::view::lit(&e.link)
    )
}

/// Deletes the markers of `hex` made at `here` (a re-insert of those keys
/// there, or a refused delete).
#[must_use]
pub fn unmark_sql(ref_obj: ObjectId, here: At, hex: &[String]) -> String {
    format!(
        "DELETE FROM {{refs}} WHERE ref_obj = {} AND kind = 'del' AND tenant = {} AND space = {} \
         AND ref_key IN ({})",
        ref_obj.0,
        here.tenant.0,
        here.space.0,
        hex.join(", ")
    )
}

/// Groups `refs` by referenced key (the checks read each key once).
#[must_use]
pub fn by_ref(refs: &[Ref]) -> BTreeMap<String, String> {
    refs.iter()
        .map(|r| (r.ref_hex.clone(), r.ref_sql.clone()))
        .collect()
}

// ------------------------------------------------------------ runtime

fn text(v: Option<&Option<Vec<u8>>>) -> Option<String> {
    v.and_then(Option::as_ref)
        .map(|b| String::from_utf8_lossy(b).into_owned())
}

/// The session's level.
#[must_use]
pub fn here(ctx: &Ctx<'_>) -> Here {
    Here {
        at: At {
            tenant: ctx.cat.tenant,
            space: ctx.space,
        },
        path: ctx.state.path(ctx.space),
        below: ctx.state.descendants(ctx.space).into_iter().collect(),
        ancestors: ctx.ancestors.iter().map(|a| a.tenant).collect(),
    }
}

/// `X'..'` literals of a key's parts, from its canonical hex.
#[must_use]
pub fn hex_parts(hex: &str) -> Vec<String> {
    let h = hex
        .trim_start_matches("X'")
        .trim_start_matches("x'")
        .trim_end_matches('\'');
    let bytes: Vec<u8> = (0..h.len() / 2)
        .filter_map(|i| u8::from_str_radix(&h[i * 2..i * 2 + 2], 16).ok())
        .collect();
    bytes
        .split(|b| *b == 0x1f)
        .map(|p| key_hex(&[p.to_vec()]))
        .collect()
}

/// A key's parts as values comparable with its columns.
#[must_use]
pub fn hex_values(hex: &str) -> Vec<String> {
    hex_parts(hex)
        .into_iter()
        .map(|p| {
            if p == "''" {
                p
            } else {
                format!("CONVERT({p} USING utf8mb4)")
            }
        })
        .collect()
}

/// `k1 = v1 AND k2 = v2` for the key in `hex`.
fn key_pred(key: &[String], hex: &str) -> String {
    key.iter()
        .zip(hex_values(hex))
        .map(|(k, v)| format!("{} = {v}", q(k)))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// The key in `hex` as a SQL tuple (key push-down).
fn key_tuple(hex: &str) -> String {
    let v = hex_values(hex);
    if v.len() == 1 {
        v.into_iter().next().unwrap_or_default()
    } else {
        format!("({})", v.join(", "))
    }
}

/// The canonical form of `cols`, in SQL.
#[must_use]
pub fn canon(cols: &[String], alias: Option<&str>) -> String {
    let parts: Vec<String> = cols
        .iter()
        .map(|c| match alias {
            Some(a) => format!("CAST({a}.{} AS BINARY)", q(c)),
            None => format!("CAST({} AS BINARY)", q(c)),
        })
        .collect();
    if parts.len() == 1 {
        parts.into_iter().next().unwrap_or_default()
    } else {
        format!("CONCAT_WS(X'1F', {})", parts.join(", "))
    }
}

/// The override bits of `cols`.
#[must_use]
pub fn bits(o: &InheritableObject, cols: &[String]) -> u64 {
    cols.iter().fold(0, |b, c| b | o.bit(c))
}

/// A lock conflict of a `NOWAIT` read.
fn locked(e: &SprigError) -> bool {
    matches!(
        e.kind,
        sprig_common::ErrorKind::Backend {
            errno: 1205 | 3572,
            ..
        } | sprig_common::ErrorKind::Standard {
            errno: 1205 | 3572,
            ..
        }
    ) || e.message.contains("(1205)")
        || e.message.contains("(3572)")
}

/// Whether deletes of `o` must look for references: something in this
/// tenant references it, or it goes to child tenants (whose own tables may
/// reference it).
#[must_use]
pub fn deletes_need_check(ctx: &Ctx<'_>, o: &InheritableObject) -> bool {
    !ctx.state.referencing(o).is_empty()
        || ctx.state.shares_of(o.object_id).any(|s| {
            matches!(s.to, ShareTarget::Tenant(_))
                || (s.to == ShareTarget::AllChildren && s.from == ROOT_SPACE)
        })
}

/// Whether a delete in the session's space can orphan overrides in child
/// tenants: `o` goes to child tenants from this space.
#[must_use]
pub fn reaches_child_tenants(ctx: &Ctx<'_>, o: &InheritableObject) -> bool {
    ctx.state.shares_of(o.object_id).any(|s| {
        s.from == ctx.space
            && (matches!(s.to, ShareTarget::Tenant(_))
                || (s.to == ShareTarget::AllChildren && s.from == ROOT_SPACE))
    })
}

/// Before rows `keys` of `res` leave the session space's view (a delete,
/// a hide or an orphan reset): handover claims for the values they show
/// for each foreign key, when the table goes to child tenants (see
/// [`HANDOVER`]). The copy worker replaces them with the orphans' own
/// claims; a delete that finds one checks the descendants itself
/// ([`before_delete`]).
///
/// # Errors
/// The backend's and the catalog's.
pub async fn hand_over(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    res: &Resolved,
    chain: &crate::view::Chain,
    keys: &[String],
) -> Result<()> {
    let o = &res.obj;
    if o.links.is_empty() || keys.is_empty() || !reaches_child_tenants(ctx, o) {
        return Ok(());
    }
    let here = here(ctx);
    for l in &o.links {
        let Some(rr) = ctx.resolve(&Name {
            db: Some(res.db_name.clone()),
            name: l.ref_table.clone(),
        })?
        else {
            continue;
        };
        let view = ViewSql {
            o,
            layer: res.layer.clone(),
            keys: Some(keys.to_vec()),
            origin: false,
        }
        .view(chain);
        let rows = r
            .query(&format!(
                "SELECT HEX({}), HEX({}) FROM ({view}) AS `__sprig_v` WHERE {}",
                canon(&o.key, Some("`__sprig_v`")),
                canon(&l.columns, Some("`__sprig_v`")),
                l.columns
                    .iter()
                    .map(|c| format!("`__sprig_v`.{} IS NOT NULL", q(c)))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ))
            .await?;
        let found: Vec<Ref> = rows
            .rows
            .iter()
            .filter_map(|row| {
                let from = text(row.first())?;
                let to = text(row.get(1))?;
                Some(Ref {
                    from_hex: hex_lit(&from),
                    ref_hex: hex_lit(&to),
                    ref_sql: String::new(),
                })
            })
            .collect();
        if found.is_empty() {
            continue;
        }
        let root = crate::ids::tree_root(ctx, &rr.obj)?;
        let name = format!("{HANDOVER}{}", l.name);
        for chunk in found.chunks(500) {
            r.refs(
                root.0,
                &claims_sql(root.1, here.at, o.object_id, &name, chunk),
            )
            .await?;
        }
        metrics::counter!("sprig_inherit_handovers_total").increment(found.len() as u64);
    }
    Ok(())
}

/// Whether handover claim `c` (see [`HANDOVER`]) still stands for a
/// delete at the session's level, related to it by `kin`: the handed-over
/// row is gone (or going) at its level, and a tenant below that level
/// (and at or below the session's, when the handover was made above it)
/// still has an override of that row that the copy worker hasn't made an
/// orphan with its own claim, or an orphan of it holding the key. The
/// blocking tenant's level and the link, or `None` (the handover is then
/// removed when nothing below needs it).
async fn handover_live(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    root: (TenantId, ObjectId),
    c: &Entry,
    kin: Kin,
) -> Result<Option<(At, String, String, Link)>> {
    let cat = if c.at.tenant == ctx.cat.tenant {
        None
    } else {
        Some(catalog_of(ctx, r, c.at.tenant).await?)
    };
    let cc: &TenantCatalog = cat.as_deref().unwrap_or(ctx.cat);
    let Some(st) = state_of(cc) else {
        return Ok(None);
    };
    let Some(f) = st.object(c.from_obj).cloned() else {
        r.refs(root.0, &forget_sql(root.1, "ref", c)).await?;
        return Ok(None);
    };
    let name = c.link.trim_start_matches(HANDOVER);
    let Some(l) = f.links.iter().find(|l| l.name == name).cloned() else {
        r.refs(root.0, &forget_sql(root.1, "ref", c)).await?;
        return Ok(None);
    };
    let Some(db) = cc.databases.get(f.db).map(|d| d.name.clone()) else {
        return Ok(None);
    };
    // The handed-over row at its own level: still there (the delete
    // rolled back), or locked (the delete is in flight).
    let phys = physical_schema(cc, &db, ctx.lctn)?;
    let at_home = format!(
        "SELECT CAST(`_sprig_state` AS CHAR) FROM {}.{} WHERE {} AND `_sprig_space` = {} LOCK IN \
         SHARE MODE NOWAIT",
        q(&phys),
        q(&f.table),
        key_pred(&f.key, &c.from_hex),
        c.at.space.0
    );
    match r.ctl_query(c.at.tenant, &at_home).await {
        Ok(rows) => {
            let state = rows.first().and_then(|row| text(row.first()));
            if matches!(state.as_deref(), Some("own" | "orphan" | "ovr")) {
                r.refs(root.0, &forget_sql(root.1, "ref", c)).await?;
                return Ok(None);
            }
        }
        Err(e) if locked(&e) => return Ok(Some((c.at, db, f.table.clone(), l))),
        Err(e) => return Err(e),
    }
    // The tenants below that may hold an override of the row.
    let (top, skip) = if kin == Kin::Above {
        (ctx.cat.tenant, None)
    } else {
        (c.at.tenant, Some(c.at.tenant))
    };
    let tables = r.tree_tables(top, &db, &f.table).await?;
    let b = bits(&f, &l.columns);
    for tt in tables.iter().filter(|t| Some(t.tenant) != skip) {
        let sql = format!(
            "SELECT 1 FROM {}.{} WHERE {} AND `_sprig_space` = {} AND ((`_sprig_state` = 'ovr' \
             AND `_sprig_ovr` & {b} <> {b}) OR (`_sprig_state` IN ('ovr', 'orphan') AND {} = {})) \
             LIMIT 1 LOCK IN SHARE MODE NOWAIT",
            q(&tt.schema),
            q(&f.table),
            key_pred(&f.key, &c.from_hex),
            ROOT_SPACE.0,
            canon(&l.columns, None),
            c.ref_hex
        );
        let held = match r.ctl_query(tt.tenant, &sql).await {
            Ok(rows) => !rows.is_empty(),
            Err(e) if locked(&e) => true,
            // A plain table of that name (no layer columns) shadows the
            // inherited one there: nothing to orphan.
            Err(e) if matches!(errno(&e), Some(1054 | 1146)) => false,
            Err(e) => return Err(e),
        };
        if held {
            let at = At {
                tenant: tt.tenant,
                space: ROOT_SPACE,
            };
            return Ok(Some((at, db, f.table.clone(), l)));
        }
    }
    r.refs(root.0, &forget_sql(root.1, "ref", c)).await?;
    Ok(None)
}

/// A backend error's number.
fn errno(e: &SprigError) -> Option<u16> {
    match e.kind {
        sprig_common::ErrorKind::Backend { errno, .. }
        | sprig_common::ErrorKind::Standard { errno, .. } => Some(errno),
        sprig_common::ErrorKind::Sprig(_) => None,
    }
}

/// A tenant's snapshot on the session's line (itself or an ancestor), or
/// from the runner.
async fn catalog_of(ctx: &Ctx<'_>, r: &mut dyn Runner, t: TenantId) -> Result<Arc<TenantCatalog>> {
    if let Some(a) = ctx.ancestors.iter().find(|a| a.tenant == t) {
        return Ok(Arc::clone(a));
    }
    r.tenant_catalog(t).await
}

/// `t`'s ancestors, root first.
async fn ancestors_of(ctx: &Ctx<'_>, r: &mut dyn Runner, t: TenantId) -> Vec<TenantId> {
    if t == ctx.cat.tenant {
        return ctx.ancestors.iter().map(|a| a.tenant).collect();
    }
    if let Some(i) = ctx.ancestors.iter().position(|a| a.tenant == t) {
        return ctx.ancestors[..i].iter().map(|a| a.tenant).collect();
    }
    match r.tenant_catalog(t).await {
        Ok(c) => c.ancestry.iter().map(|a| a.id).collect(),
        Err(_) => Vec::new(),
    }
}

/// The rows of `res`'s layer in space `s` with keys `keys` whose values of
/// `l`'s columns are their own (an `own` or `orphan` row, or an `ovr` row
/// overriding them): their references.
async fn own_refs(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    res: &Resolved,
    l: &Link,
    s: SpaceId,
    keys: Option<&[String]>,
) -> Result<Vec<Ref>> {
    let o = &res.obj;
    let n = o.key.len();
    let kp = match keys {
        Some(k) => format!(" AND {}", crate::write::key_in_sql(o, k)),
        None => String::new(),
    };
    let rows = r
        .query(&format!(
            "SELECT {}, {} FROM {} WHERE `_sprig_space` = {} AND (`_sprig_state` IN ('own', \
             'orphan') OR (`_sprig_state` = 'ovr' AND `_sprig_ovr` & {} <> 0)){kp}",
            o.key.iter().map(|k| q(k)).collect::<Vec<_>>().join(", "),
            l.columns
                .iter()
                .map(|c| q(c))
                .collect::<Vec<_>>()
                .join(", "),
            res.layer,
            s.0,
            bits(o, &l.columns),
        ))
        .await?;
    let nbe = ctx.lex.no_backslash_escapes;
    let mut out = Vec::new();
    for row in &rows.rows {
        let vals: Vec<Option<Vec<u8>>> = row[n..n + l.columns.len()].to_vec();
        if vals.iter().any(Option::is_none) {
            continue;
        }
        let from: Vec<Vec<u8>> = row[..n]
            .iter()
            .map(|v| v.clone().unwrap_or_default())
            .collect();
        let to: Vec<Vec<u8>> = vals.into_iter().map(Option::unwrap_or_default).collect();
        let lits: Vec<String> = (n..n + l.columns.len())
            .map(|i| crate::write::literal_of(&rows, row, i, nbe))
            .collect();
        out.push(Ref {
            from_hex: key_hex(&from),
            ref_hex: key_hex(&to),
            ref_sql: if lits.len() == 1 {
                lits.into_iter().next().unwrap_or_default()
            } else {
                format!("({})", lits.join(", "))
            },
        });
    }
    Ok(out)
}

/// Writes the claims of `refs` in chunks.
async fn claim(
    r: &mut dyn Runner,
    root: (TenantId, ObjectId),
    at: At,
    from: ObjectId,
    l: &Link,
    refs: &[Ref],
) -> Result<()> {
    for chunk in refs.chunks(500) {
        r.refs(root.0, &claims_sql(root.1, at, from, &l.name, chunk))
            .await?;
    }
    Ok(())
}

/// Whether deletion marker `m` (made at an ancestor level of the session,
/// or at its own) still stands: its delete is running (its layer row is
/// locked) or done and not undone.
async fn marker_holds(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    db_name: &str,
    table: &str,
    m: &Entry,
) -> Result<bool> {
    let cat = if m.at.tenant == ctx.cat.tenant {
        None
    } else {
        Some(catalog_of(ctx, r, m.at.tenant).await?)
    };
    let c: &TenantCatalog = cat.as_deref().unwrap_or(ctx.cat);
    let Some(st) = state_of(c) else {
        return Ok(false);
    };
    let Some(d) = c.database(&sprig_catalog::ddl::database_key(db_name, ctx.lctn)) else {
        return Ok(false);
    };
    let Some(o) = st.object_named_ci(d.db_id, table) else {
        return Ok(false);
    };
    let phys = physical_schema(c, db_name, ctx.lctn)?;
    let sql = format!(
        "SELECT `_sprig_state` FROM {}.{} WHERE {} AND `_sprig_space` = {} LOCK IN SHARE MODE \
         NOWAIT",
        q(&phys),
        q(&o.table),
        key_pred(&o.key, &m.ref_hex),
        m.at.space.0
    );
    let rows = match r.ctl_query(m.at.tenant, &sql).await {
        Ok(rows) => rows,
        Err(e) if locked(&e) => return Ok(true),
        Err(e) => return Err(e),
    };
    let state = rows.first().and_then(|row| text(row.first()));
    Ok(if m.link == "hide" {
        state.as_deref() == Some("hide")
    } else {
        !matches!(state.as_deref(), Some("own" | "orphan"))
    })
}

/// After a write of `keys` in the session's space: claims for the rows'
/// own references, then the checks (1452).
///
/// # Errors
/// 1452; the backend's and the catalog's.
pub async fn after_write(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    res: &Resolved,
    keys: &[String],
) -> Result<()> {
    let o = &res.obj;
    if o.links.is_empty() || keys.is_empty() {
        return Ok(());
    }
    let here = here(ctx);
    for l in &o.links {
        let Some(rr) = ctx.resolve(&Name {
            db: Some(res.db_name.clone()),
            name: l.ref_table.clone(),
        })?
        else {
            continue;
        };
        let found = own_refs(ctx, r, res, l, ctx.space, Some(keys)).await?;
        if found.is_empty() {
            continue;
        }
        let root = crate::ids::tree_root(ctx, &rr.obj)?;
        claim(r, root, here.at, o.object_id, l, &found).await?;
        let wanted = by_ref(&found);
        let hexes: Vec<String> = wanted.keys().cloned().collect();
        // Deletion markers at this level and above.
        let markers = parse_entries(&r.refs(root.0, &entries_sql(root.1, "del", &hexes)).await?);
        for m in markers {
            let mine = m.at.tenant == ctx.cat.tenant
                || ctx.ancestors.iter().any(|a| a.tenant == m.at.tenant);
            if !mine {
                continue;
            }
            let anc = ancestors_of(ctx, r, m.at.tenant).await;
            if m.at != here.at && here.kin(m.at, &anc) != Kin::Above {
                continue;
            }
            if marker_holds(ctx, r, &res.db_name, &rr.obj.table, &m).await? {
                // A level below the deleter's that holds the key as its
                // own row or orphan (an override the copy worker or the
                // deleting statement made an orphan) still provides it.
                if m.at == here.at || !provided_below(r, &rr, &here, &m).await? {
                    return Err(child_row_fails(&res.db_name, &o.table, l));
                }
                continue;
            }
            r.refs(root.0, &forget_sql(root.1, "del", &m)).await?;
        }
        // The referenced keys in this space's view, read fresh.
        crate::upstream::ensure(ctx, r, &rr).await?;
        let phys = physical_schema(ctx.cat, &rr.db_name, ctx.lctn)?;
        let prefix = format!("{}.", q(&phys));
        let chain = ctx.chain(&rr.obj, ctx.space, &prefix)?;
        let view = ViewSql {
            o: &rr.obj,
            layer: format!("{prefix}{}", q(&rr.obj.table)),
            keys: Some(wanted.values().cloned().collect()),
            origin: false,
        }
        .view(&chain);
        let rows = r
            .ctl_query(
                ctx.cat.tenant,
                &format!(
                    "SELECT HEX({}) FROM ({view}) AS `__sprig_v`",
                    canon(&rr.obj.key, None)
                ),
            )
            .await?;
        let present: BTreeSet<String> = rows
            .iter()
            .filter_map(|row| text(row.first()))
            .map(|h| format!("X'{h}'"))
            .collect();
        let missing: Vec<&String> = hexes.iter().filter(|h| !present.contains(*h)).collect();
        if missing.is_empty() {
            continue;
        }
        // Rows this transaction wrote itself aren't visible to the
        // provisioning account yet: the session's own view decides those.
        let tuples: Vec<String> = missing
            .iter()
            .filter_map(|h| wanted.get(*h).cloned())
            .collect();
        let chain = ctx.chain(&rr.obj, ctx.space, &rr.prefix)?;
        let mine = r
            .query(&format!(
                "SELECT HEX({}) FROM ({}) AS `__sprig_v`",
                canon(&rr.obj.key, None),
                ViewSql {
                    o: &rr.obj,
                    layer: rr.layer.clone(),
                    keys: Some(tuples),
                    origin: false,
                }
                .view(&chain)
            ))
            .await?;
        let own: BTreeSet<String> = mine
            .rows
            .iter()
            .filter_map(|row| text(row.first()))
            .map(|h| format!("X'{h}'"))
            .collect();
        let uncommitted = r
            .query(&format!(
                "SELECT HEX({}) FROM {} WHERE `_sprig_space` = {} AND `_sprig_state` IN ('own', \
                 'orphan', 'ovr') AND {}",
                canon(&rr.obj.key, None),
                rr.layer,
                ctx.space.0,
                crate::write::key_in_sql(
                    &rr.obj,
                    &missing.iter().map(|h| key_tuple(h)).collect::<Vec<_>>()
                )
            ))
            .await?;
        let layer: BTreeSet<String> = uncommitted
            .rows
            .iter()
            .filter_map(|row| text(row.first()))
            .map(|h| format!("X'{h}'"))
            .collect();
        if missing
            .iter()
            .any(|h| !(own.contains(*h) && layer.contains(*h)))
        {
            return Err(child_row_fails(&res.db_name, &o.table, l));
        }
    }
    Ok(())
}

/// Whether a space of this tenant on the session's path, below deletion
/// marker `m`'s level, holds `m`'s key as its own row or orphan.
async fn provided_below(
    r: &mut dyn Runner,
    rr: &Resolved,
    here: &Here,
    m: &Entry,
) -> Result<bool> {
    let spaces: Vec<String> = if m.at.tenant == here.at.tenant {
        here.path
            .iter()
            .skip_while(|s| **s != m.at.space)
            .skip(1)
            .map(|s| s.0.to_string())
            .collect()
    } else {
        here.path.iter().map(|s| s.0.to_string()).collect()
    };
    if spaces.is_empty() {
        return Ok(false);
    }
    let rows = r
        .query(&format!(
            "SELECT 1 FROM {} WHERE `_sprig_space` IN ({}) AND `_sprig_state` IN ('own', \
             'orphan') AND {} LIMIT 1",
            rr.layer,
            spaces.join(", "),
            key_pred(&rr.obj.key, &m.ref_hex)
        ))
        .await?;
    Ok(!rows.rows.is_empty())
}

/// Registers claims for every row of `res` with its own references, in
/// every space of this tenant (a foreign key declared over existing rows).
///
/// # Errors
/// The backend's and the catalog's.
pub async fn backfill(ctx: &Ctx<'_>, r: &mut dyn Runner, res: &Resolved, l: &Link) -> Result<()> {
    let Some(rr) = ctx.resolve(&Name {
        db: Some(res.db_name.clone()),
        name: l.ref_table.clone(),
    })?
    else {
        return Ok(());
    };
    let root = crate::ids::tree_root(ctx, &rr.obj)?;
    let spaces = r
        .query(&format!(
            "SELECT DISTINCT `_sprig_space` FROM {}",
            res.layer
        ))
        .await?;
    for row in &spaces.rows {
        let Some(s) = text(row.first()).and_then(|v| v.parse().ok()).map(SpaceId) else {
            continue;
        };
        let found = own_refs(ctx, r, res, l, s, None).await?;
        if found.is_empty() {
            continue;
        }
        let at = At {
            tenant: ctx.cat.tenant,
            space: s,
        };
        claim(r, root, at, res.obj.object_id, l, &found).await?;
    }
    Ok(())
}

/// Claims for the own references of rows `keys` of `res` in space `s`
/// (rows that became orphans, which now hold their values themselves).
///
/// # Errors
/// The backend's and the catalog's.
pub async fn claim_rows(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    res: &Resolved,
    s: SpaceId,
    keys: &[String],
) -> Result<()> {
    for l in &res.obj.links {
        let Some(rr) = ctx.resolve(&Name {
            db: Some(res.db_name.clone()),
            name: l.ref_table.clone(),
        })?
        else {
            continue;
        };
        let found = own_refs(ctx, r, res, l, s, Some(keys)).await?;
        if found.is_empty() {
            continue;
        }
        let root = crate::ids::tree_root(ctx, &rr.obj)?;
        let at = At {
            tenant: ctx.cat.tenant,
            space: s,
        };
        claim(r, root, at, res.obj.object_id, l, &found).await?;
    }
    Ok(())
}

/// The canonical keys of the first `n` columns of each row, hex.
#[must_use]
pub fn row_hexes(rows: &sprig_types::Rows, n: usize) -> Vec<String> {
    rows.rows
        .iter()
        .map(|row| {
            let parts: Vec<Vec<u8>> = row
                .iter()
                .take(n)
                .map(|v| v.clone().unwrap_or_default())
                .collect();
            key_hex(&parts)
        })
        .collect()
}

/// What a claim of a level at or below the session's says now.
async fn claim_live(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    c: &Entry,
) -> Result<Option<(String, String, Link)>> {
    let cat = if c.at.tenant == ctx.cat.tenant {
        None
    } else {
        Some(catalog_of(ctx, r, c.at.tenant).await?)
    };
    let cc: &TenantCatalog = cat.as_deref().unwrap_or(ctx.cat);
    let Some(st) = state_of(cc) else {
        return Ok(None);
    };
    let Some(f) = st.object(c.from_obj) else {
        return Ok(None);
    };
    let Some(l) = f.links.iter().find(|l| l.name == c.link) else {
        return Ok(None);
    };
    let Some(db) = cc.databases.get(f.db).map(|d| d.name.clone()) else {
        return Ok(None);
    };
    let phys = physical_schema(cc, &db, ctx.lctn)?;
    let sql = format!(
        "SELECT ({} = {}) AND (`_sprig_state` IN ('own', 'orphan') OR (`_sprig_state` = 'ovr' \
         AND `_sprig_ovr` & {} <> 0)) FROM {}.{} WHERE {} AND `_sprig_space` = {} LOCK IN SHARE \
         MODE NOWAIT",
        canon(&l.columns, None),
        c.ref_hex,
        bits(f, &l.columns),
        q(&phys),
        q(&f.table),
        key_pred(&f.key, &c.from_hex),
        c.at.space.0
    );
    let live = match r.ctl_query(c.at.tenant, &sql).await {
        Ok(rows) => rows
            .first()
            .and_then(|row| text(row.first()))
            .is_some_and(|v| v == "1"),
        Err(e) if locked(&e) => true,
        Err(e) => return Err(e),
    };
    Ok(live.then(|| (db, f.table.clone(), l.clone())))
}

/// Whether a claim made above the session's level still references the
/// key in the session space's view.
async fn claim_seen(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    db_name: &str,
    c: &Entry,
) -> Result<Option<(String, Link)>> {
    let cat = if c.at.tenant == ctx.cat.tenant {
        None
    } else {
        Some(catalog_of(ctx, r, c.at.tenant).await?)
    };
    let cc: &TenantCatalog = cat.as_deref().unwrap_or(ctx.cat);
    let Some(f) = state_of(cc).and_then(|st| st.object(c.from_obj).cloned()) else {
        return Ok(None);
    };
    let Some(res) = ctx.resolve(&Name {
        db: Some(db_name.to_owned()),
        name: f.table.clone(),
    })?
    else {
        return Ok(None);
    };
    let Some(l) = res.obj.links.iter().find(|l| l.name == c.link).cloned() else {
        return Ok(None);
    };
    crate::upstream::ensure(ctx, r, &res).await?;
    let chain = ctx.chain(&res.obj, ctx.space, &res.prefix)?;
    let rows = r
        .query(&format!(
            "SELECT 1 FROM ({}) AS `__sprig_v` WHERE {} = {} LIMIT 1",
            ViewSql {
                o: &res.obj,
                layer: res.layer.clone(),
                keys: Some(vec![key_tuple(&c.from_hex)]),
                origin: false,
            }
            .view(&chain),
            canon(&l.columns, Some("`__sprig_v`")),
            c.ref_hex
        ))
        .await?;
    Ok((!rows.rows.is_empty()).then(|| (f.table.clone(), l)))
}

/// Before keys `hexes` of `res` leave the session space's view (a hide for
/// those in `hidden`, a physical delete for the rest): the deletion
/// markers, then the claims (1451 while one holds).
///
/// # Errors
/// 1451; the backend's and the catalog's.
pub async fn before_delete(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    res: &Resolved,
    hexes: &[String],
    hidden: &BTreeSet<String>,
) -> Result<()> {
    if hexes.is_empty() || !deletes_need_check(ctx, &res.obj) {
        return Ok(());
    }
    let here = here(ctx);
    let root = crate::ids::tree_root(ctx, &res.obj)?;
    let (hide, drop): (Vec<String>, Vec<String>) =
        hexes.iter().cloned().partition(|h| hidden.contains(h));
    for (what, hs) in [("hide", &hide), ("drop", &drop)] {
        for chunk in hs.chunks(500) {
            r.refs(root.0, &markers_sql(root.1, here.at, what, chunk))
                .await?;
        }
    }
    let mut blocking = None;
    for chunk in hexes.chunks(500) {
        let claims = parse_entries(&r.refs(root.0, &entries_sql(root.1, "ref", chunk)).await?);
        for c in claims {
            let anc = ancestors_of(ctx, r, c.at.tenant).await;
            let kin = here.kin(c.at, &anc);
            if c.link.starts_with(HANDOVER) {
                if kin != Kin::Unrelated
                    && let Some(b) = handover_live(ctx, r, root, &c, kin).await?
                {
                    blocking = Some(b);
                    break;
                }
                continue;
            }
            match kin {
                Kin::AtOrBelow => match claim_live(ctx, r, &c).await? {
                    Some((db, table, l)) => {
                        blocking = Some((c.at, db, table, l));
                        break;
                    }
                    None => {
                        r.refs(root.0, &forget_sql(root.1, "ref", &c)).await?;
                    }
                },
                Kin::Above => {
                    if let Some((table, l)) = claim_seen(ctx, r, &res.db_name, &c).await? {
                        blocking = Some((c.at, res.db_name.clone(), table, l));
                        break;
                    }
                }
                Kin::Unrelated => {}
            }
        }
        if blocking.is_some() {
            break;
        }
    }
    let Some((at, db, table, l)) = blocking else {
        return Ok(());
    };
    for chunk in hexes.chunks(500) {
        r.refs(root.0, &unmark_sql(root.1, here.at, chunk)).await?;
    }
    let where_ = if at.tenant == ctx.cat.tenant {
        if at.space == ctx.space {
            String::new()
        } else {
            format!(" (referenced in space '{}')", ctx.space_name(at.space))
        }
    } else if ctx.privileges.space_admin() && !ctx.ancestors.iter().any(|a| a.tenant == at.tenant) {
        match r.tenant_catalog(at.tenant).await {
            Ok(c) => format!(
                " (referenced in tenant '{}')",
                tenant_name(&c).unwrap_or_else(|| at.tenant.to_string())
            ),
            Err(_) => " (referenced in a descendant tenant)".to_owned(),
        }
    } else if ctx.ancestors.iter().any(|a| a.tenant == at.tenant) {
        let n = ctx
            .cat
            .ancestry
            .iter()
            .find(|a| a.id == at.tenant)
            .map_or_else(|| at.tenant.to_string(), |a| a.name.clone());
        format!(" (referenced in tenant '{n}')")
    } else {
        " (referenced in a descendant tenant)".to_owned()
    };
    metrics::counter!("sprig_inherit_deletes_refused_total").increment(1);
    Err(parent_row_fails(&db, &table, &l, &where_))
}

/// How a level shows to the caller: (tenant, space). A level in this
/// tenant is named by its space; an ancestor by its name; a descendant
/// tenant by its name for tenant administrators, otherwise as "a
/// descendant tenant".
async fn level_label(ctx: &Ctx<'_>, r: &mut dyn Runner, at: At) -> (String, String) {
    if at.tenant == ctx.cat.tenant {
        return (ctx.tenant_name.to_owned(), ctx.space_name(at.space));
    }
    if let Some(a) = ctx.cat.ancestry.iter().find(|a| a.id == at.tenant) {
        return (a.name.clone(), String::new());
    }
    if ctx.privileges.space_admin()
        && let Ok(c) = r.tenant_catalog(at.tenant).await
    {
        return (
            tenant_name(&c).unwrap_or_else(|| at.tenant.to_string()),
            String::new(),
        );
    }
    ("a descendant tenant".to_owned(), String::new())
}

/// The text of a canonical key (`X'..'` parts joined by `, `).
fn key_text(hex: &str) -> String {
    let h = hex.trim_start_matches("X'").trim_end_matches('\'');
    let bytes: Vec<u8> = (0..h.len() / 2)
        .filter_map(|i| u8::from_str_radix(&h[i * 2..i * 2 + 2], 16).ok())
        .collect();
    bytes
        .split(|b| *b == 0x1f)
        .map(|p| String::from_utf8_lossy(p).into_owned())
        .collect::<Vec<_>>()
        .join(", ")
}

/// `SPRIG CHECK REFERENCES t [WHERE e]`: for the rows of `t` in the
/// session space's view (at most 1,000), what would refuse their delete
/// (1451) now: each row elsewhere that references one of them, by level,
/// and handovers still waiting for the copy worker. Tenant administrators
/// see descendant tenants' names and the referencing rows' keys; others
/// see "a descendant tenant" and no key for rows outside this tenant and
/// its ancestors.
///
/// # Errors
/// 39885 for a table that isn't inheritable; the backend's.
pub async fn check_refs(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    table: &Name,
    filter: Option<&str>,
) -> Result<sprig_types::Rows> {
    let Some(res) = ctx.resolve(table)? else {
        return Err(SprigError::new(
            SprigCode::NotInheritable,
            format!("{} is not inheritable", table.name),
        ));
    };
    let names = ["Key", "Tenant", "Space", "Table", "Row", "Link", "Kind"];
    let mut out = Vec::new();
    if deletes_need_check(ctx, &res.obj) {
        let chain = ctx.chain(&res.obj, ctx.space, &res.prefix)?;
        let view = ViewSql {
            o: &res.obj,
            layer: res.layer.clone(),
            keys: None,
            origin: false,
        }
        .view(&chain);
        let rows = r
            .query(&format!(
                "SELECT HEX({}) FROM ({view}) AS `__sprig_v`{} LIMIT 1000",
                canon(&res.obj.key, Some("`__sprig_v`")),
                filter.map(|f| format!(" WHERE {f}")).unwrap_or_default()
            ))
            .await?;
        let hexes: Vec<String> = rows
            .rows
            .iter()
            .filter_map(|row| text(row.first()))
            .map(|h| hex_lit(&h))
            .collect();
        let here = here(ctx);
        let root = crate::ids::tree_root(ctx, &res.obj)?;
        let admin = ctx.privileges.space_admin();
        for chunk in hexes.chunks(500) {
            let claims = parse_entries(&r.refs(root.0, &entries_sql(root.1, "ref", chunk)).await?);
            for c in claims {
                let anc = ancestors_of(ctx, r, c.at.tenant).await;
                let kin = here.kin(c.at, &anc);
                let found = if c.link.starts_with(HANDOVER) {
                    if kin == Kin::Unrelated {
                        None
                    } else {
                        handover_live(ctx, r, root, &c, kin)
                            .await?
                            .map(|(at, db, t, l)| (at, db, t, l, "orphaning in progress"))
                    }
                } else {
                    match kin {
                        Kin::AtOrBelow => claim_live(ctx, r, &c)
                            .await?
                            .map(|(db, t, l)| (c.at, db, t, l, "reference")),
                        Kin::Above => claim_seen(ctx, r, &res.db_name, &c)
                            .await?
                            .map(|(t, l)| (c.at, res.db_name.clone(), t, l, "reference")),
                        Kin::Unrelated => None,
                    }
                };
                let Some((at, db, t, l, kind)) = found else {
                    continue;
                };
                let (tenant, space) = level_label(ctx, r, at).await;
                let on_line = at.tenant == ctx.cat.tenant
                    || ctx.ancestors.iter().any(|a| a.tenant == at.tenant);
                let row_key = (admin || on_line).then(|| key_text(&c.from_hex));
                out.push(
                    [
                        Some(key_text(&c.ref_hex)),
                        Some(tenant),
                        Some(space),
                        Some(format!("{db}.{t}")),
                        row_key,
                        Some(l.name.clone()),
                        Some(kind.to_owned()),
                    ]
                    .into_iter()
                    .map(|v| v.map(String::into_bytes))
                    .collect(),
                );
            }
        }
    }
    Ok(crate::show::text_rows(&names, out))
}

/// A tenant's name as its catalog's ancestry can't tell: its root space's
/// row, when it has one.
fn tenant_name(c: &TenantCatalog) -> Option<String> {
    state_of(c).and_then(|s| {
        s.spaces()
            .find(|x| x.id == ROOT_SPACE)
            .map(|x| x.name.clone())
    })
}

// --------------------------------------------------------------- DDL

/// 39897 when a descendant tenant already has table `res`'s column
/// `column` (or, with `None`, a table named like `res`), which a change
/// here would bring down to it.
///
/// # Errors
/// 39897; the directory's.
pub async fn refuse_clash(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    res: &Resolved,
    column: Option<&str>,
) -> Result<()> {
    let reaches = ctx.state.shares_of(res.obj.object_id).any(|s| {
        matches!(s.to, ShareTarget::Tenant(_))
            || (s.to == ShareTarget::AllChildren && s.from == ROOT_SPACE)
    });
    if column.is_some() && !reaches {
        return Ok(());
    }
    let tables = r
        .tree_tables(ctx.cat.tenant, &res.db_name, &res.obj.table)
        .await?;
    for tt in tables.iter().filter(|t| t.tenant != ctx.cat.tenant) {
        let hit = match column {
            None => true,
            Some(col) => {
                let Ok(c) = r.tenant_catalog(tt.tenant).await else {
                    continue;
                };
                c.database(&sprig_catalog::ddl::database_key(&res.db_name, ctx.lctn))
                    .and_then(|d| {
                        state_of(&c)
                            .and_then(|s| s.object_named_ci(d.db_id, &res.obj.table).cloned())
                    })
                    .is_some_and(|o| o.column(col).is_some())
            }
        };
        if hit {
            let what = match column {
                Some(c) => format!("column {}.{c}", res.obj.table),
                None => format!("table {}", res.obj.table),
            };
            let named = ctx.privileges.space_admin().then_some(tt.name.as_str());
            return Err(clash(&what, named));
        }
    }
    Ok(())
}

/// The tables of database `db` and their MariaDB foreign keys within it.
async fn native_fks(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    db: DbId,
    db_name: &str,
) -> Result<Vec<(String, FkClause)>> {
    let mut out = Vec::new();
    let names: Vec<String> = ctx
        .cat
        .objects
        .names(db, Namespace::Relation)
        .filter(|(_, id)| {
            ctx.cat
                .objects
                .get(*id)
                .is_some_and(|o| o.kind == ObjectKind::Table)
        })
        .map(|(n, _)| n.to_owned())
        .collect();
    for t in names {
        let Ok(rows) = r
            .query(&format!("SHOW CREATE TABLE {}.{}", q(db_name), q(&t)))
            .await
        else {
            continue;
        };
        let Some(text) = rows.rows.first().and_then(|row| text(row.get(1))) else {
            continue;
        };
        for f in scan_show_create(&text, &ctx.lex) {
            if f.ref_table
                .db
                .as_deref()
                .is_some_and(|d| !d.eq_ignore_ascii_case(db_name))
            {
                return Err(unsupported(format!(
                    "{t} has a foreign key into database {}: a table linked to an inheritable \
                     table must keep its foreign keys inside its database",
                    f.ref_table.db.clone().unwrap_or_default()
                )));
            }
            out.push((t.clone(), f));
        }
    }
    Ok(out)
}

/// Makes `table` inheritable together with every table its MariaDB
/// foreign keys link it to (through the closure): their foreign keys are
/// dropped in MariaDB and recorded as links, and existing rows get claims.
/// Returns the objects to commit (new ones and existing ones with new
/// links).
///
/// # Errors
/// The steps' errors.
pub async fn convert_native(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    db: DbId,
    db_name: &str,
    table: &Name,
    key: &[String],
    tag: Option<&str>,
) -> Result<Vec<InheritableObject>> {
    let fks = native_fks(ctx, r, db, db_name).await?;
    let mut group: Vec<String> = vec![table.name.clone()];
    loop {
        let before = group.len();
        for (c, f) in &fks {
            let ci = group.iter().any(|g| g.eq_ignore_ascii_case(c));
            let pi = group
                .iter()
                .any(|g| g.eq_ignore_ascii_case(&f.ref_table.name));
            if ci && !pi {
                group.push(f.ref_table.name.clone());
            } else if pi && !ci {
                group.push(c.clone());
            }
        }
        if group.len() == before {
            break;
        }
    }
    let in_group = |t: &str| group.iter().any(|g| g.eq_ignore_ascii_case(t));
    let mine: Vec<&(String, FkClause)> = fks.iter().filter(|(c, _)| in_group(c)).collect();
    for (c, f) in &mine {
        if let Some(n) = &f.name {
            r.exec(&format!(
                "ALTER TABLE {}.{} DROP FOREIGN KEY {}",
                q(db_name),
                q(c),
                q(n)
            ))
            .await?;
        }
    }
    let mut objs: Vec<InheritableObject> = Vec::new();
    for g in &group {
        if let Some(o) = ctx.state.object_named_ci(db, g) {
            objs.push((**o).clone());
            continue;
        }
        let n = Name {
            db: Some(db_name.to_owned()),
            name: g.clone(),
        };
        let first = g.eq_ignore_ascii_case(&table.name);
        let o = crate::ddl::enable(
            ctx,
            r,
            db,
            &n,
            first.then_some(key),
            if first { tag } else { None },
        )
        .await?;
        objs.push(o);
    }
    for (c, f) in &mine {
        let Some(ri) = objs
            .iter()
            .position(|o| o.table.eq_ignore_ascii_case(&f.ref_table.name))
        else {
            continue;
        };
        let target = objs[ri].clone();
        let Some(ci) = objs.iter().position(|o| o.table.eq_ignore_ascii_case(c)) else {
            continue;
        };
        let n = objs[ci].links.len() + 1;
        let name = default_name(&objs[ci].table, &objs[ci].links, n);
        let l = link_of(f, &objs[ci].columns.clone(), &target, &name)?;
        if !objs[ci]
            .links
            .iter()
            .any(|x| x.name.eq_ignore_ascii_case(&l.name))
        {
            objs[ci].links.push(l);
        }
    }
    // Claims for the rows already there.
    for o in &objs {
        if o.links.is_empty() {
            continue;
        }
        let res = Resolved {
            db,
            db_name: db_name.to_owned(),
            obj: Arc::new(o.clone()),
            layer: format!("{}.{}", q(db_name), q(&o.table)),
            prefix: format!("{}.", q(db_name)),
        };
        for l in &o.links {
            backfill(ctx, r, &res, l).await?;
        }
    }
    Ok(objs)
}

/// `CREATE TABLE ... REFERENCES t` where `t` is inheritable: the clauses
/// leave the statement, the table is created and made inheritable with
/// its primary key, plain tables it references become inheritable too,
/// and the links are recorded.
///
/// # Errors
/// 39891 for an unsupported action, a foreign key into another database,
/// or one that isn't onto the referenced table's key; the steps' errors.
pub async fn create_table(
    sql: &str,
    n: &Name,
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
) -> Result<Handled> {
    let Some((table, fks)) = scan_create(sql, &ctx.lex) else {
        return Ok(Handled::NotInherit);
    };
    let _ = n;
    let Some((db, db_name)) = ctx.db_of(&table) else {
        return Ok(Handled::NotInherit);
    };
    let same_db = |f: &FkClause| {
        f.ref_table
            .db
            .as_deref()
            .is_none_or(|d| d.eq_ignore_ascii_case(&db_name))
    };
    let inh = fks
        .iter()
        .any(|f| same_db(f) && ctx.state.object_named_ci(db, &f.ref_table.name).is_some());
    if !inh {
        return Ok(Handled::NotInherit);
    }
    if let Some(f) = fks.iter().find(|f| !same_db(f)) {
        return Err(unsupported(format!(
            "a foreign key into database {} from a table linked to an inheritable table",
            f.ref_table.db.clone().unwrap_or_default()
        )));
    }
    if let Some(f) = fks.iter().find(|f| f.unsupported_action.is_some()) {
        return Err(cascade_refused(
            f.unsupported_action.as_deref().unwrap_or_default(),
        ));
    }
    // A descendant tenant with its own table of this name would receive
    // this one too (question Q3: refused).
    let probe = Resolved {
        db,
        db_name: db_name.clone(),
        obj: Arc::new(InheritableObject {
            object_id: ObjectId(0),
            db,
            table: table.name.clone(),
            key: Vec::new(),
            columns: Vec::new(),
            slots: BTreeMap::new(),
            uniques: Vec::new(),
            tag_field: None,
            version: 0,
            auto: None,
            links: Vec::new(),
            adopted: false,
        }),
        layer: String::new(),
        prefix: String::new(),
    };
    let goes_down = fks.iter().any(|f| {
        ctx.state
            .object_named_ci(db, &f.ref_table.name)
            .is_some_and(|o| {
                ctx.state.shares_of(o.object_id).any(|s| {
                    matches!(s.to, ShareTarget::Tenant(_))
                        || (s.to == ShareTarget::AllChildren && s.from == ROOT_SPACE)
                })
            })
    });
    if goes_down {
        refuse_clash(ctx, r, &probe, None).await?;
    }
    let spans: Vec<(usize, usize)> = fks.iter().map(|f| f.span).collect();
    r.exec(&strip(sql, &spans)).await?;
    let me = Name {
        db: Some(db_name.clone()),
        name: table.name.clone(),
    };
    let mut new = crate::ddl::enable(ctx, r, db, &me, None, None).await?;
    let mut objs: Vec<InheritableObject> = Vec::new();
    for (i, f) in fks.iter().enumerate() {
        let target = if f.ref_table.name.eq_ignore_ascii_case(&table.name) {
            new.clone()
        } else if let Some(o) = ctx.state.object_named_ci(db, &f.ref_table.name) {
            (**o).clone()
        } else if let Some(o) = objs
            .iter()
            .find(|o| o.table.eq_ignore_ascii_case(&f.ref_table.name))
        {
            o.clone()
        } else {
            let more = convert_native(
                ctx,
                r,
                db,
                &db_name,
                &Name {
                    db: Some(db_name.clone()),
                    name: f.ref_table.name.clone(),
                },
                &[],
                None,
            )
            .await;
            // A plain table: its primary key is its key.
            let more = match more {
                Ok(m) => m,
                Err(e) => return Err(e),
            };
            objs.extend(more);
            objs.iter()
                .find(|o| o.table.eq_ignore_ascii_case(&f.ref_table.name))
                .cloned()
                .ok_or_else(|| SprigError::internal("a referenced table wasn't enabled"))?
        };
        let name = default_name(&new.table, &new.links, i + 1);
        let l = link_of(f, &new.columns, &target, &name)?;
        new.links.push(l);
    }
    objs.retain(|o| o.object_id != new.object_id);
    objs.push(new);
    let mut c = crate::ddl::root_change(ctx);
    c.upsert_objects = objs;
    r.commit(&c).await?;
    Ok(Handled::Ok(Done::default()))
}

/// `ALTER TABLE` with foreign-key clauses where either end is inheritable
/// (`None` when the statement is someone else's).
///
/// # Errors
/// 39895 when adding one to a shared table; 1452 for existing rows that
/// violate it; 39891; the steps' errors.
pub async fn alter(
    sql: &str,
    n: &Name,
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
) -> Result<Option<Handled>> {
    let Some((table, clauses, _)) = scan_alter(sql, &ctx.lex) else {
        return Ok(None);
    };
    if clauses.is_empty() {
        return Ok(None);
    }
    let Some((db, db_name)) = ctx.db_of(&table) else {
        return Ok(None);
    };
    let this = ctx.state.object_named_ci(db, &table.name).cloned();
    let refs_inh = clauses.iter().any(|c| match c {
        AlterFk::Add(f) => ctx.state.object_named_ci(db, &f.ref_table.name).is_some(),
        AlterFk::Drop { .. } => false,
    });
    if this.is_none() && !refs_inh {
        return Ok(None);
    }
    let mut spans = Vec::new();
    let mut adds = Vec::new();
    let mut drops = Vec::new();
    for c in &clauses {
        match c {
            AlterFk::Add(f) => {
                if f.ref_table
                    .db
                    .as_deref()
                    .is_some_and(|d| !d.eq_ignore_ascii_case(&db_name))
                {
                    return Err(unsupported(
                        "a foreign key into another database from an inheritable table",
                    ));
                }
                spans.push(f.span);
                adds.push(f.clone());
            }
            AlterFk::Drop { name, span } => {
                if this
                    .as_ref()
                    .is_some_and(|o| o.links.iter().any(|l| l.name.eq_ignore_ascii_case(name)))
                {
                    spans.push(*span);
                    drops.push(name.clone());
                }
            }
        }
    }
    if let Some(o) = &this
        && !adds.is_empty()
    {
        crate::guard::refuse_if_shared(ctx, o, "ALTER TABLE (a foreign key)")?;
    }
    if let Some(o) = &this
        && !drops.is_empty()
    {
        // Dropping a link may take a table out of a shared group.
        crate::guard::refuse_if_shared(ctx, o, "ALTER TABLE (DROP FOREIGN KEY)")?;
        for d in &drops {
            if let Some(l) = o.links.iter().find(|l| l.name.eq_ignore_ascii_case(d))
                && let Some(t) = ctx.state.object_named_ci(db, &l.ref_table)
            {
                crate::guard::refuse_if_shared(ctx, t, "ALTER TABLE (DROP FOREIGN KEY)")?;
            }
        }
        crate::guard::refuse_if_received(ctx, o, "ALTER TABLE (DROP FOREIGN KEY)")?;
    }
    let rest = strip(sql, &spans);
    let has_rest = {
        let t = Toks::new(rest.as_bytes(), &ctx.lex);
        let (_, _, start) = scan_alter(&rest, &ctx.lex).unwrap_or((table.clone(), Vec::new(), 0));
        t.is_some_and(|t| start < t.len())
    };
    if has_rest {
        match &this {
            Some(_) => {
                Box::pin(crate::guard::alter(&rest, n, ctx, r)).await?;
            }
            None => {
                r.exec(&rest).await?;
            }
        }
    }
    let me = Name {
        db: Some(db_name.clone()),
        name: table.name.clone(),
    };
    let mut objs: Vec<InheritableObject> = Vec::new();
    let mut obj = if let Some(o) = &this {
        // Columns may have changed with the rest of the statement.
        let mut o = (**o).clone();
        let cols: Vec<String> =
            crate::guard::show_columns(r, &format!("{}.{}", q(&db_name), q(&o.table)))
                .await?
                .into_iter()
                .map(|c| c.name)
                .collect();
        if let Some(w) = o.with_slots_for(&cols) {
            o = w;
        }
        o
    } else {
        let more = convert_native(ctx, r, db, &db_name, &me, &[], None).await?;
        objs.extend(more);
        let i = objs
            .iter()
            .position(|o| o.table.eq_ignore_ascii_case(&table.name))
            .ok_or_else(|| SprigError::internal("the altered table wasn't enabled"))?;
        objs.remove(i)
    };
    obj.links
        .retain(|l| !drops.iter().any(|d| d.eq_ignore_ascii_case(&l.name)));
    let mut new_links = Vec::new();
    for f in &adds {
        let target = if f.ref_table.name.eq_ignore_ascii_case(&table.name) {
            obj.clone()
        } else if let Some(o) = ctx.state.object_named_ci(db, &f.ref_table.name) {
            (**o).clone()
        } else if let Some(o) = objs
            .iter()
            .find(|o| o.table.eq_ignore_ascii_case(&f.ref_table.name))
        {
            o.clone()
        } else {
            let more = convert_native(
                ctx,
                r,
                db,
                &db_name,
                &Name {
                    db: Some(db_name.clone()),
                    name: f.ref_table.name.clone(),
                },
                &[],
                None,
            )
            .await?;
            objs.extend(more);
            objs.iter()
                .find(|o| o.table.eq_ignore_ascii_case(&f.ref_table.name))
                .cloned()
                .ok_or_else(|| SprigError::internal("a referenced table wasn't enabled"))?
        };
        let name = default_name(&obj.table, &obj.links, obj.links.len() + 1);
        let l = link_of(f, &obj.columns, &target, &name)?;
        obj.links.push(l.clone());
        new_links.push((l, target));
    }
    // Existing rows must satisfy the new links, in every space.
    if this.is_some() {
        let res = Resolved {
            db,
            db_name: db_name.clone(),
            obj: Arc::new(obj.clone()),
            layer: format!("{}.{}", q(&db_name), q(&obj.table)),
            prefix: format!("{}.", q(&db_name)),
        };
        for (l, target) in &new_links {
            check_existing(ctx, r, &res, l, target).await?;
            backfill(ctx, r, &res, l).await?;
        }
    }
    objs.retain(|o| o.object_id != obj.object_id);
    objs.push(obj);
    let mut c = crate::ddl::root_change(ctx);
    c.upsert_objects = objs;
    r.commit(&c).await?;
    Ok(Some(Handled::Ok(Done::default())))
}

/// 1452 when a row of `res` in any space holds a value of `l` that space's
/// view of `target` doesn't have.
async fn check_existing(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    res: &Resolved,
    l: &Link,
    target: &InheritableObject,
) -> Result<()> {
    let spaces = r
        .query(&format!(
            "SELECT DISTINCT `_sprig_space` FROM {}",
            res.layer
        ))
        .await?;
    let tres = Resolved {
        db: res.db,
        db_name: res.db_name.clone(),
        obj: Arc::new(target.clone()),
        layer: format!("{}.{}", q(&res.db_name), q(&target.table)),
        prefix: res.prefix.clone(),
    };
    crate::upstream::ensure(ctx, r, &tres).await?;
    for row in &spaces.rows {
        let Some(s) = text(row.first()).and_then(|v| v.parse().ok()).map(SpaceId) else {
            continue;
        };
        let mine = ctx.chain(&res.obj, s, &res.prefix)?;
        let theirs = ctx.chain(target, s, &res.prefix)?;
        let lhs = if l.columns.len() == 1 {
            format!("v.{}", q(&l.columns[0]))
        } else {
            format!(
                "({})",
                l.columns
                    .iter()
                    .map(|c| format!("v.{}", q(c)))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let rows = r
            .query(&format!(
                "SELECT 1 FROM ({}) AS v WHERE {} AND {lhs} NOT IN (SELECT {} FROM ({}) AS w) \
                 LIMIT 1",
                ViewSql {
                    o: &res.obj,
                    layer: res.layer.clone(),
                    keys: None,
                    origin: false,
                }
                .view(&mine),
                l.columns
                    .iter()
                    .map(|c| format!("v.{} IS NOT NULL", q(c)))
                    .collect::<Vec<_>>()
                    .join(" AND "),
                target
                    .key
                    .iter()
                    .map(|k| q(k))
                    .collect::<Vec<_>>()
                    .join(", "),
                ViewSql {
                    o: target,
                    layer: tres.layer.clone(),
                    keys: None,
                    origin: false,
                }
                .view(&theirs)
            ))
            .await?;
        if !rows.rows.is_empty() {
            return Err(child_row_fails(&res.db_name, &res.obj.table, l));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lex() -> LexOptions {
        LexOptions::mariadb()
    }

    #[test]
    fn create_table_clauses() {
        let sql = "CREATE TABLE t1 (id INT AUTO_INCREMENT PRIMARY KEY, t2_id INT, \
                   name VARCHAR(10), CONSTRAINT fk_t2 FOREIGN KEY (t2_id) REFERENCES t2 (id))";
        let (n, fks) = scan_create(sql, &lex()).unwrap();
        assert_eq!(n.name, "t1");
        assert_eq!(fks.len(), 1);
        assert_eq!(fks[0].name.as_deref(), Some("fk_t2"));
        assert_eq!(fks[0].columns, vec!["t2_id"]);
        assert_eq!(fks[0].ref_table.name, "t2");
        assert_eq!(fks[0].ref_columns, vec!["id"]);
        let spans: Vec<_> = fks.iter().map(|f| f.span).collect();
        assert_eq!(
            strip(sql, &spans),
            "CREATE TABLE t1 (id INT AUTO_INCREMENT PRIMARY KEY, t2_id INT, name VARCHAR(10))"
        );
        // Column-level, unnamed, and a refused action.
        let sql = "CREATE TABLE t3 (id INT PRIMARY KEY, a INT REFERENCES t1 (id) ON DELETE \
                   CASCADE, b INT)";
        let (_, fks) = scan_create(sql, &lex()).unwrap();
        assert_eq!(fks.len(), 1);
        assert_eq!(fks[0].columns, vec!["a"]);
        assert_eq!(
            fks[0].unsupported_action.as_deref(),
            Some("ON DELETE CASCADE")
        );
        assert_eq!(
            strip(sql, &[fks[0].span]),
            "CREATE TABLE t3 (id INT PRIMARY KEY, a INT, b INT)"
        );
        // First item.
        let sql = "CREATE TABLE x (FOREIGN KEY (a) REFERENCES t1 (id), a INT, id INT PRIMARY KEY)";
        let (_, fks) = scan_create(sql, &lex()).unwrap();
        assert_eq!(
            strip(sql, &[fks[0].span]),
            "CREATE TABLE x (a INT, id INT PRIMARY KEY)"
        );
    }

    #[test]
    fn alter_table_clauses() {
        let sql = "ALTER TABLE t1 ADD COLUMN x INT, ADD CONSTRAINT f1 FOREIGN KEY (x) REFERENCES \
                   t2 (id) ON DELETE RESTRICT, DROP FOREIGN KEY f0";
        let (n, fks, _) = scan_alter(sql, &lex()).unwrap();
        assert_eq!(n.name, "t1");
        assert_eq!(fks.len(), 2);
        let spans: Vec<_> = fks
            .iter()
            .map(|f| match f {
                AlterFk::Add(c) => c.span,
                AlterFk::Drop { span, .. } => *span,
            })
            .collect();
        assert_eq!(strip(sql, &spans), "ALTER TABLE t1 ADD COLUMN x INT");
        let sql = "ALTER TABLE t1 ADD FOREIGN KEY (x) REFERENCES t2 (id)";
        let (_, fks, _) = scan_alter(sql, &lex()).unwrap();
        let AlterFk::Add(c) = &fks[0] else { panic!() };
        assert_eq!(c.name, None);
        assert_eq!(strip(sql, &[c.span]), "ALTER TABLE t1 ");
    }

    #[test]
    fn show_create_foreign_keys() {
        let text = "CREATE TABLE `t1` (\n  `id` int(11) NOT NULL AUTO_INCREMENT,\n  `t2_id` \
                    int(11) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `t2_id` (`t2_id`),\n  \
                    CONSTRAINT `t1_ibfk_1` FOREIGN KEY (`t2_id`) REFERENCES `t2` (`id`)\n) \
                    ENGINE=InnoDB";
        let f = scan_show_create(text, &lex());
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].name.as_deref(), Some("t1_ibfk_1"));
        assert_eq!(f[0].ref_table.name, "t2");
    }

    #[test]
    fn kinship() {
        let here = Here {
            at: At {
                tenant: TenantId(20),
                space: ROOT_SPACE,
            },
            path: vec![ROOT_SPACE],
            below: [SpaceId(5)].into_iter().collect(),
            ancestors: vec![TenantId(10)],
        };
        let at = |t, s| At {
            tenant: TenantId(t),
            space: SpaceId(s),
        };
        assert_eq!(here.kin(at(20, 1), &[TenantId(10)]), Kin::AtOrBelow);
        assert_eq!(here.kin(at(20, 5), &[TenantId(10)]), Kin::AtOrBelow);
        assert_eq!(here.kin(at(10, 1), &[]), Kin::Above);
        assert_eq!(here.kin(at(10, 3), &[]), Kin::Unrelated);
        assert_eq!(
            here.kin(at(30, 7), &[TenantId(10), TenantId(20)]),
            Kin::AtOrBelow
        );
        assert_eq!(here.kin(at(21, 1), &[TenantId(10)]), Kin::Unrelated);
        assert_eq!(key_hex(&[b"5".to_vec()]), "X'35'");
        assert_eq!(key_hex(&[b"1".to_vec(), b"a".to_vec()]), "X'311F61'");
    }
}
