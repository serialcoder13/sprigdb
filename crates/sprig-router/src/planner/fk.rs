//! The planner's side of chapter 19's foreign keys across groups (R3):
//! which values a write's child rows must find parents for, and which
//! parent rows a write changes ([`crate::fk`] runs both).

use std::sync::Arc;

use sqlparser::ast::{Assignment, Expr, Insert, OnInsert, Statement, UnaryOperator, Value};

use super::{PlanCtx, R, assigns_key, ident_parts, key_literal, plan_text};
use crate::fk::{FkCheck, FkGraph, FkParent, FkVal, FkWork, ParentKind, ValClass};
use crate::plan::Plan;
use crate::reject::{Reason, Rejection, reject};
use crate::topology::{KeyClass, TableEntry};

/// The database's graph and the table's index in it, when the table is an
/// end of a key the gateway keeps.
pub(super) fn graph_for(
    cx: &PlanCtx<'_>,
    default_schema: &str,
    table: &str,
) -> Option<(Arc<FkGraph>, usize)> {
    let g = cx.topo.fk_graph(default_schema)?;
    let t = g.index(table)?;
    Some((g, t))
}

/// Whether a table named in `stmt` is an end of a key the gateway keeps.
pub(super) fn touches(stmt: &Statement, cx: &PlanCtx<'_>) -> bool {
    let mut any = false;
    let _ = sqlparser::ast::visit_relations(stmt, |n| {
        if let Some((e, s)) = super::resolve_name(n, cx)
            && graph_for(cx, &s, &e.name).is_some()
        {
            any = true;
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
    any
}

/// A literal's value: `Ok(None)` for `NULL`; anything that isn't a
/// literal is refused (the gateway checks values it knows).
fn literal(e: &Expr, class: ValClass) -> R<Option<FkVal>> {
    let bad = || {
        Rejection::new(
            Reason::ForeignKey,
            format!("a foreign-key value that isn't a literal: {e}"),
        )
    };
    let key_class = match class {
        ValClass::Num => Some(KeyClass::Int),
        ValClass::Str => Some(KeyClass::Str { collation: 0 }),
        ValClass::Bin => Some(KeyClass::Binary),
        ValClass::Text => None,
    };
    match e {
        Expr::Nested(x) => literal(x, class),
        Expr::Value(v) if matches!(v.value, Value::Null) => Ok(None),
        Expr::Value(v)
            if matches!(
                v.value,
                Value::Number(..)
                    | Value::SingleQuotedString(_)
                    | Value::DoubleQuotedString(_)
                    | Value::HexStringLiteral(_)
            ) =>
        {
            Ok(Some(FkVal {
                sql: e.to_string(),
                key: key_class.and_then(|k| key_literal(e, k)),
            }))
        }
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } if matches!(&**expr, Expr::Value(v) if matches!(v.value, Value::Number(..))) => {
            Ok(Some(FkVal {
                sql: e.to_string(),
                key: key_class.and_then(|k| key_literal(e, k)),
            }))
        }
        Expr::Prefixed { .. } => Ok(Some(FkVal {
            sql: e.to_string(),
            key: key_class.and_then(|k| key_literal(e, k)),
        })),
        _ => Err(bad()),
    }
}

fn col_name(n: &sqlparser::ast::ObjectName) -> Option<String> {
    ident_parts(n).and_then(|p| p.last().map(|i| i.value.to_ascii_lowercase()))
}

fn assigned<'a>(assignments: &[&'a Assignment], col: &str) -> Option<&'a Expr> {
    assignments
        .iter()
        .find(|a| assigns_key(&a.target, col))
        .map(|a| &a.value)
}

fn work(
    g: Arc<FkGraph>,
    checks: Vec<FkCheck>,
    parent: Option<FkParent>,
    cx: &PlanCtx<'_>,
) -> Option<Box<FkWork>> {
    let w = FkWork {
        graph: g,
        checks,
        parent,
        session_cluster: cx.session_cluster.to_owned(),
        max_route_values: cx.max_route_values,
        batch: cx.lookup_batch,
    };
    (!w.is_empty()).then(|| Box::new(w))
}

/// An `INSERT`'s foreign-key work: the parents its rows need (`near`: the
/// cluster of its first row, whose reference copies are read).
pub(super) fn insert_work(
    ins: &Insert,
    entry: &TableEntry,
    rows: &[impl std::ops::Deref<Target = Vec<Expr>>],
    default_schema: &str,
    near: Option<&str>,
    cx: &PlanCtx<'_>,
) -> R<Option<Box<FkWork>>> {
    let Some((g, t)) = graph_for(cx, default_schema, &entry.name) else {
        return Ok(None);
    };
    let odku: Vec<&Assignment> = match &ins.on {
        Some(OnInsert::DuplicateKeyUpdate(a)) => a.iter().collect(),
        _ => Vec::new(),
    };
    for (_, f) in g.as_parent(t) {
        if ins.replace_into {
            return reject(
                Reason::ForeignKey,
                "REPLACE into a table other groups' foreign keys reference",
            );
        }
        if f.parent_columns
            .iter()
            .any(|c| assigned(&odku, c).is_some())
        {
            return reject(
                Reason::ForeignKey,
                "ON DUPLICATE KEY UPDATE of a column other groups' foreign keys reference",
            );
        }
    }
    let mut checks = Vec::new();
    for (fi, f) in g.as_child(t) {
        if ins.ignore {
            return reject(
                Reason::ForeignKey,
                "INSERT IGNORE into a table whose foreign key the gateway checks",
            );
        }
        if f.child_columns.iter().any(|c| assigned(&odku, c).is_some()) {
            return reject(
                Reason::ForeignKey,
                "ON DUPLICATE KEY UPDATE of a foreign-key column the gateway checks",
            );
        }
        // Where each column's value is: a position in the rows, a SET
        // assignment, or absent (its default).
        let mut tuples: Vec<Vec<FkVal>> = Vec::new();
        if ins.source.is_none() {
            let set: Vec<&Assignment> = ins.assignments.iter().collect();
            let mut tuple = Vec::new();
            let mut null = false;
            for (j, c) in f.child_columns.iter().enumerate() {
                match assigned(&set, c) {
                    Some(e) => match literal(e, f.classes[j])? {
                        Some(v) => tuple.push(v),
                        None => null = true,
                    },
                    None if f.child_null_default.get(j).copied().unwrap_or(false) => null = true,
                    None => {
                        return reject(
                            Reason::ForeignKey,
                            format!("an INSERT that leaves out foreign-key column {c} (a default)"),
                        );
                    }
                }
            }
            if !null {
                tuples.push(tuple);
            }
        } else {
            let mut at: Vec<Option<usize>> = Vec::new();
            for (j, c) in f.child_columns.iter().enumerate() {
                let p = if ins.columns.is_empty() {
                    Some(f.child_ordinals.get(j).copied().flatten().ok_or_else(|| {
                        Rejection::new(
                            Reason::ForeignKey,
                            "an INSERT without a column list (a foreign-key column's position is unknown)",
                        )
                    })?)
                } else {
                    ins.columns
                        .iter()
                        .position(|n| col_name(n).as_deref() == Some(c.as_str()))
                };
                if p.is_none() && !f.child_null_default.get(j).copied().unwrap_or(false) {
                    return reject(
                        Reason::ForeignKey,
                        format!("an INSERT that leaves out foreign-key column {c} (a default)"),
                    );
                }
                at.push(p);
            }
            for row in rows {
                let mut tuple = Vec::new();
                let mut null = false;
                for (j, p) in at.iter().enumerate() {
                    let Some(p) = p else {
                        null = true;
                        continue;
                    };
                    let Some(e) = row.get(*p) else {
                        // A count mismatch: MariaDB's own 1136 says so.
                        null = true;
                        continue;
                    };
                    match literal(e, f.classes[j])? {
                        Some(v) => tuple.push(v),
                        None => null = true,
                    }
                }
                if !null {
                    tuples.push(tuple);
                }
            }
        }
        checks.push(FkCheck {
            fk: fi,
            tuples,
            near: near.map(str::to_owned),
        });
    }
    Ok(work(g, checks, None, cx))
}

/// What an `UPDATE` or `DELETE` is, for [`update_delete_work`].
pub(super) struct Dml<'s> {
    /// `DELETE`.
    pub delete: bool,
    /// The target as written (`FROM` item text).
    pub relation: String,
    /// `WHERE`.
    pub selection: Option<&'s Expr>,
    /// `ORDER BY … LIMIT …` as written, or empty.
    pub tail: String,
    /// `SET` items.
    pub assignments: Vec<&'s Assignment>,
}

/// An `UPDATE`'s or `DELETE`'s foreign-key work: the parents its new
/// child values need, and the parent rows it changes.
pub(super) fn update_delete_work(
    d: &Dml<'_>,
    entry: &TableEntry,
    default_schema: &str,
    near: Option<&str>,
    cx: &PlanCtx<'_>,
) -> R<Option<Box<FkWork>>> {
    let Some((g, t)) = graph_for(cx, default_schema, &entry.name) else {
        return Ok(None);
    };
    let mut checks = Vec::new();
    if !d.delete {
        for (fi, f) in g.as_child(t) {
            let set: Vec<Option<&Expr>> = f
                .child_columns
                .iter()
                .map(|c| assigned(&d.assignments, c))
                .collect();
            if set.iter().all(Option::is_none) {
                continue;
            }
            if set.iter().any(Option::is_none) {
                return reject(
                    Reason::ForeignKey,
                    "an UPDATE of some columns of a multi-column foreign key",
                );
            }
            let mut tuple = Vec::new();
            let mut null = false;
            for (j, e) in set.iter().enumerate() {
                match literal(e.expect("all set"), f.classes[j])? {
                    Some(v) => tuple.push(v),
                    None => null = true,
                }
            }
            checks.push(FkCheck {
                fk: fi,
                tuples: if null { Vec::new() } else { vec![tuple] },
                near: near.map(str::to_owned),
            });
        }
    }
    // The parent side: the keys whose referenced values this write
    // removes or changes.
    let fks: Vec<usize> = g
        .as_parent(t)
        .filter(|(_, f)| {
            d.delete
                || f.parent_columns
                    .iter()
                    .any(|c| assigned(&d.assignments, c).is_some())
        })
        .map(|(i, _)| i)
        .collect();
    let parent = if fks.is_empty() {
        None
    } else {
        Some(parent_read(&g, t, fks, d, cx)?)
    };
    Ok(work(g, checks, parent, cx))
}

fn parent_read(
    g: &FkGraph,
    t: usize,
    fks: Vec<usize>,
    d: &Dml<'_>,
    cx: &PlanCtx<'_>,
) -> R<FkParent> {
    let q = |s: &str| format!("`{}`", s.replace('`', "``"));
    let mut cols: Vec<String> = Vec::new();
    let mut classes: Vec<ValClass> = Vec::new();
    for &fi in &fks {
        let f = &g.fks[fi];
        for (c, class) in f.parent_columns.iter().zip(&f.classes) {
            if !cols.contains(c) {
                cols.push(c.clone());
                classes.push(*class);
            }
        }
    }
    let mut items: Vec<String> = cols
        .iter()
        .zip(&classes)
        .map(|(c, k)| k.read_expr(&q(c)))
        .collect();
    let mut changed: Vec<String> = Vec::new();
    let kind = if d.delete {
        ParentKind::Delete
    } else {
        let mut news = Vec::new();
        for (c, k) in cols.clone().iter().zip(classes.clone()) {
            if let Some(e) = assigned(&d.assignments, c) {
                news.push((c.clone(), items.len()));
                items.push(k.read_expr(&format!("({e})")));
                classes.push(k);
                changed.push(format!("NOT ({} <=> ({e}))", q(c)));
            }
        }
        ParentKind::Update(news)
    };
    let mut sql = format!("SELECT {} FROM {}", items.join(", "), d.relation);
    let mut conds: Vec<String> = Vec::new();
    if let Some(w) = d.selection {
        conds.push(format!("({w})"));
    }
    if !changed.is_empty() {
        conds.push(format!("({})", changed.join(" OR ")));
    }
    if !conds.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conds.join(" AND "));
    }
    if !d.tail.is_empty() {
        sql.push(' ');
        sql.push_str(&d.tail);
    }
    sql.push_str(" FOR UPDATE");
    let read = match plan_text(&sql, cx)? {
        p @ (Plan::Pass(_) | Plan::Read(_) | Plan::ViaLookup(_)) => p,
        Plan::Reject(r) => return Err(r),
        other => {
            return reject(
                Reason::ForeignKey,
                format!(
                    "the parent rows a write changes can't be read and locked exactly ({})",
                    other.kind()
                ),
            );
        }
    };
    Ok(FkParent {
        table: t,
        read: Box::new(read),
        cols,
        classes,
        kind,
        fks,
    })
}
