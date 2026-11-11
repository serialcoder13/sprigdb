//! One id sequence per table for the whole tree (Kiran's decision 3, 11
//! November 2026).
//!
//! Each tenant's copy of an inheritable table used to keep its own
//! `AUTO_INCREMENT` counter, so a child's new row could take an id its
//! parent already used and hide the parent's row in its view (nearest level
//! wins). Now every `INSERT` into an inheritable object with an
//! `AUTO_INCREMENT` column takes the generated values from one sequence
//! for the table's whole tree: chapter 19's ID blocks, kept in the
//! catalog of the tree's **root** tenant (the topmost tenant from which
//! the table is shared down to this one), keyed by the root's object id.
//! An id then names the same row at every level, and foreign keys are
//! unambiguous.
//!
//! ```text
//! INSERT into o (o.auto = c)
//!   rows that leave c out, or give NULL, DEFAULT or 0 → values from the
//!     tree's sequence, consecutive in the session's increment and offset;
//!     the first is written LAST_INSERT_ID(v), so the OK packet and
//!     LAST_INSERT_ID() return it as MariaDB would
//!   explicit values → the sequence moves past the largest (39463 when
//!     another gateway's block holds it)
//! the sequence's first use (migration): its first value is above every
//!   tenant's rows and AUTO_INCREMENT counter in the tree; ids found in
//!   more than one tenant are reported (SPRIG CHECK IDS), never renumbered
//! ```
//!
//! Inside one tenant every space's rows were already in one table with one
//! counter; the sequence keeps that true when the tree spans tenants.

use std::collections::{BTreeMap, BTreeSet};

use sprig_common::ids::{ObjectId, TenantId};
use sprig_common::{Result, SprigError};
use sqlparser::ast::{Expr, Value, ValueWithSpan};

use crate::grammar::Name;
use crate::model::InheritableObject;
use crate::plan::{Ctx, tenant_edge};
use crate::view::q;
use crate::{Handled, Runner};

/// Tree sequences' ids in the root's `sequences`: the root object's id
/// with this bit, apart from chapter 19's (an object's own id).
pub const TREE_SEQ: u64 = 1 << 62;

/// A table's tree sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeSeq {
    /// The tree's root tenant, whose catalog holds the sequence.
    pub root: TenantId,
    /// The table's object id in the root.
    pub root_object: ObjectId,
    /// `sequences.seq_id`.
    pub seq: u64,
    /// The table's logical database (the same name in every tenant).
    pub db: String,
    /// The table.
    pub table: String,
    /// The `AUTO_INCREMENT` column.
    pub column: String,
}

/// The tenant and object at the top of `o`'s tree: the topmost ancestor
/// tenant whose share reaches this tenant through every tenant between
/// (this tenant itself when nothing is shared to it).
///
/// # Errors
/// 39892.
pub fn tree_root(ctx: &Ctx<'_>, o: &InheritableObject) -> Result<(TenantId, ObjectId)> {
    let mut root = (ctx.cat.tenant, o.object_id);
    let Some(db) = ctx.db_name(o.db) else {
        return Ok(root);
    };
    let mut lineage: Vec<&sprig_catalog::TenantCatalog> =
        ctx.ancestors.iter().map(AsRef::as_ref).collect();
    lineage.push(ctx.cat);
    let mut j = lineage.len() - 1;
    while j >= 1 {
        match tenant_edge(lineage[j - 1], lineage[j], &db, &o.table, ctx.lctn)? {
            Some(up) => {
                root = (lineage[j - 1].tenant, up.parent_obj.object_id);
                j -= 1;
            }
            None => break,
        }
    }
    Ok(root)
}

/// `o`'s tree sequence, when it has an `AUTO_INCREMENT` column.
///
/// # Errors
/// 39892.
pub fn tree_seq(ctx: &Ctx<'_>, o: &InheritableObject) -> Result<Option<TreeSeq>> {
    let Some(column) = o.auto.clone() else {
        return Ok(None);
    };
    let (root, root_object) = tree_root(ctx, o)?;
    Ok(Some(TreeSeq {
        root,
        root_object,
        seq: TREE_SEQ | root_object.0,
        db: ctx.db_name(o.db).unwrap_or_default(),
        table: o.table.clone(),
        column,
    }))
}

/// What a row gives the `AUTO_INCREMENT` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Given {
    /// Nothing, `NULL`, `DEFAULT` or `0`: generate.
    Generate,
    /// An integer literal.
    Explicit(i128),
    /// Another expression: written as given.
    Other,
}

fn given(e: Option<&Expr>) -> Given {
    let Some(e) = e else {
        return Given::Generate;
    };
    let text = e.to_string();
    let t = text.trim();
    if t.eq_ignore_ascii_case("NULL") || t.eq_ignore_ascii_case("DEFAULT") {
        return Given::Generate;
    }
    let digits = t.trim_matches('\'');
    match digits.parse::<i128>() {
        Ok(0) => Given::Generate,
        Ok(v) => Given::Explicit(v),
        Err(_) => Given::Other,
    }
}

fn number(v: i128) -> Expr {
    Expr::Value(ValueWithSpan {
        value: Value::Number(v.to_string(), false),
        span: sqlparser::tokenizer::Span::empty(),
    })
}

/// Fills the `AUTO_INCREMENT` column of `rows` (in `columns`' order) from
/// `o`'s tree sequence. Returns the rows that got a generated value (no
/// level can hold those keys yet).
///
/// # Errors
/// 39460, 39463, 1467 from the sequence; 39892.
pub async fn fill(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    o: &InheritableObject,
    columns: &mut Vec<String>,
    rows: &mut [Vec<Expr>],
) -> Result<Vec<usize>> {
    let Some(seq) = tree_seq(ctx, o)? else {
        return Ok(Vec::new());
    };
    let pos = columns
        .iter()
        .position(|c| c.eq_ignore_ascii_case(&seq.column));
    let mut need = Vec::new();
    let mut explicit: Option<i128> = None;
    for (i, row) in rows.iter().enumerate() {
        match given(pos.and_then(|p| row.get(p))) {
            Given::Generate => need.push(i),
            Given::Explicit(v) => explicit = Some(explicit.map_or(v, |m| m.max(v))),
            Given::Other => {}
        }
    }
    if need.is_empty() && explicit.is_none() {
        return Ok(Vec::new());
    }
    let taken = r.tree_ids(&seq, need.len() as u64, explicit).await?;
    let pos = if let Some(p) = pos {
        p
    } else {
        columns.push(seq.column.clone());
        for row in rows.iter_mut() {
            row.push(number(0));
        }
        columns.len() - 1
    };
    let Some((first, step)) = taken else {
        return Ok(Vec::new());
    };
    for (k, i) in need.iter().enumerate() {
        let v = first + step * i128::try_from(k).unwrap_or(0);
        rows[*i][pos] = if k == 0 {
            crate::rewrite::parse_expr(&format!("LAST_INSERT_ID({v})"))?
        } else {
            number(v)
        };
    }
    Ok(need)
}

/// `SPRIG CHECK IDS t`: the ids of `t` that more than one tenant of its
/// tree holds as its own (`own` or `orphan` rows in any space), with those
/// tenants. A tenant the session may not see (one outside its line) is
/// shown as `another tenant`.
///
/// # Errors
/// 39885 for an ordinary object; the backend's.
pub async fn check(ctx: &Ctx<'_>, r: &mut dyn Runner, table: &Name) -> Result<Handled> {
    let Some(res) = ctx.resolve(table)? else {
        return Err(SprigError::new(
            sprig_common::SprigCode::NotInheritable,
            format!("{} is not inheritable", table.name),
        ));
    };
    let o = &res.obj;
    let (root, _) = tree_root(ctx, o)?;
    let tables = r.tree_tables(root, &res.db_name, &o.table).await?;
    let mut holders: BTreeMap<String, BTreeSet<TenantId>> = BTreeMap::new();
    let mut names: BTreeMap<TenantId, (String, bool)> = BTreeMap::new();
    let key = o.key.iter().map(|k| q(k)).collect::<Vec<_>>().join(", ");
    for tt in &tables {
        names.insert(
            tt.tenant,
            (tt.name.clone(), tt.ancestors.contains(&ctx.cat.tenant)),
        );
        let rows = r
            .ctl_query(
                tt.tenant,
                &format!(
                    "SELECT DISTINCT CONCAT_WS(',', {key}) FROM {}.{} WHERE `_sprig_state` IN \
                     ('own', 'orphan')",
                    q(&tt.schema),
                    q(&o.table)
                ),
            )
            .await?;
        for row in rows {
            if let Some(Some(v)) = row.first() {
                holders
                    .entry(String::from_utf8_lossy(v).into_owned())
                    .or_default()
                    .insert(tt.tenant);
            }
        }
    }
    let line: BTreeSet<TenantId> = ctx
        .ancestors
        .iter()
        .map(|a| a.tenant)
        .chain(std::iter::once(ctx.cat.tenant))
        .collect();
    let mut out = Vec::new();
    for (id, ts) in holders.into_iter().filter(|(_, ts)| ts.len() > 1) {
        let shown: Vec<String> = ts
            .iter()
            .map(|t| match names.get(t) {
                Some((n, below)) if line.contains(t) || *below => n.clone(),
                _ => "another tenant".to_owned(),
            })
            .collect();
        out.push(vec![
            Some(id.into_bytes()),
            Some(shown.join(", ").into_bytes()),
        ]);
    }
    metrics::counter!("sprig_inherit_id_collisions_found_total").increment(out.len() as u64);
    Ok(Handled::Rows(crate::show::text_rows(&["Id", "Tenants"], out)))
}

/// One tenant's copy of a table in a tree ([`Runner::tree_tables`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeTable {
    /// The tenant.
    pub tenant: TenantId,
    /// Its name.
    pub name: String,
    /// Its ancestors, root first.
    pub ancestors: Vec<TenantId>,
    /// The physical schema holding the table.
    pub schema: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn what_a_row_gives() {
        let e = |s: &str| crate::rewrite::parse_expr(s).unwrap();
        assert_eq!(given(None), Given::Generate);
        assert_eq!(given(Some(&e("NULL"))), Given::Generate);
        assert_eq!(given(Some(&e("0"))), Given::Generate);
        assert_eq!(given(Some(&e("42"))), Given::Explicit(42));
        assert_eq!(given(Some(&e("'7'"))), Given::Explicit(7));
        assert_eq!(given(Some(&e("1 + 1"))), Given::Other);
    }
}
