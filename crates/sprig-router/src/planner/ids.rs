//! `AUTO_INCREMENT` values from ID blocks (chapter 19, "ID blocks";
//! chapter 17, "INSERT routing").
//!
//! Every member of a sharded group with an `AUTO_INCREMENT` column takes
//! its values from the gateway's block of the table's `sequences` row, so
//! no two shards hand out the same value:
//!
//! ```text
//! plan_insert(stmt) into such a table:
//!   the column's value in each row:
//!     left out, NULL, DEFAULT, or 0 (without NO_AUTO_VALUE_ON_ZERO) → generate
//!     an integer literal                                           → explicit
//!     anything else                                                → reason auto_increment
//!   → NeedIds { rows to generate, highest explicit value }
//! the gateway: values ← take(seq, rows); raise(seq, highest explicit)
//! IdFill::finish(first, step): each such row gets first + k × step, written
//!   into the statement, which is planned as usual (routed by the value
//!   when the column is the key, claimed in lookups, `_sprig_rk` added)
//! ```
//!
//! A statement that needs no value and writes no positive explicit one is
//! planned at once.

use std::sync::Arc;

use sqlparser::ast::{
    Assignment, AssignmentTarget, Expr, Ident, ObjectName, ObjectNamePart, SetExpr, Statement,
    Value,
};

use super::{IdMode, PlanCtx, R, Src, assigns_key, ident_parts, key_literal, plan_insert};
use crate::plan::{IdFill, Plan};
use crate::reject::{Reason, reject};
use crate::topology::{AutoInc, KeyClass, KeyValue, TableEntry};

/// What one value of the column asks for.
enum Ask {
    /// A value from the block.
    Generate,
    /// This value.
    Explicit(i128),
}

fn ask(e: &Expr, mode: IdMode) -> R<Ask> {
    match e {
        Expr::Value(v) if matches!(v.value, Value::Null) => Ok(Ask::Generate),
        Expr::Identifier(i) if i.value.eq_ignore_ascii_case("DEFAULT") => Ok(Ask::Generate),
        Expr::Nested(x) => ask(x, mode),
        _ => match key_literal(e, KeyClass::Int) {
            Some(KeyValue::Int(0)) if !mode.no_auto_value_on_zero => Ok(Ask::Generate),
            Some(KeyValue::Int(i)) => Ok(Ask::Explicit(i)),
            _ => reject(
                Reason::AutoIncrement,
                format!("an AUTO_INCREMENT value that isn't an integer literal: {e}"),
            ),
        },
    }
}

fn column(n: &str) -> ObjectName {
    ObjectName(vec![ObjectNamePart::Identifier(Ident::new(n))])
}

fn named(c: &ObjectName, n: &str) -> bool {
    ident_parts(c)
        .and_then(|p| p.last().map(|i| i.value.eq_ignore_ascii_case(n)))
        .unwrap_or(false)
}

fn null() -> Expr {
    Expr::value(Value::Null)
}

/// Plans an `INSERT` into `entry`, whose column `ai` takes ID blocks:
/// [`Plan::NeedIds`], or the plan itself when no value is needed.
pub(super) fn plan(
    stmt: &Statement,
    src: &Src<'_>,
    entry: &TableEntry,
    ai: &AutoInc,
    cx: &PlanCtx<'_>,
) -> R<Plan> {
    let Statement::Insert(ins) = stmt else {
        return reject(Reason::Unsupported, "not an INSERT");
    };
    let mut out = ins.clone();
    let mut rows = Vec::new();
    let mut explicit_max: Option<i128> = None;
    let mut note = |v: i128| {
        if v > 0 {
            explicit_max = Some(explicit_max.map_or(v, |m| m.max(v)));
        }
    };
    let pos = match &mut out.source {
        None => {
            // INSERT … SET: one row.
            if let Some(a) = out
                .assignments
                .iter()
                .find(|a| assigns_key(&a.target, &ai.column))
            {
                match ask(&a.value, cx.ids)? {
                    Ask::Generate => rows.push(0),
                    Ask::Explicit(v) => note(v),
                }
            } else {
                out.assignments.push(Assignment {
                    target: AssignmentTarget::ColumnName(column(&ai.column)),
                    value: null(),
                });
                rows.push(0);
            }
            None
        }
        Some(body) => {
            let SetExpr::Values(values) = &mut *body.body else {
                return reject(Reason::InsertSelect, "INSERT … SELECT into a sharded table");
            };
            let width = if out.columns.is_empty() {
                entry.columns.len()
            } else {
                out.columns.len()
            };
            if values.rows.iter().any(|r| r.len() != width) {
                // A count mismatch (or columns the router doesn't know):
                // nothing to fill; MariaDB's 1136 answers, or the routing
                // refuses the rows.
                if out.columns.is_empty() && entry.columns.is_empty() {
                    return reject(
                        Reason::AutoIncrement,
                        "an INSERT without a column list into a table whose columns are unknown",
                    );
                }
                return plan_insert(stmt, src, &filled(cx));
            }
            let p = if out.columns.is_empty() {
                ai.ordinal.ok_or_else(|| {
                    crate::reject::Rejection::new(
                        Reason::AutoIncrement,
                        "an INSERT without a column list (the AUTO_INCREMENT column's position \
                         is unknown)",
                    )
                })?
            } else if let Some(p) = out.columns.iter().position(|c| named(c, &ai.column)) {
                p
            } else {
                out.columns.push(column(&ai.column));
                for r in &mut values.rows {
                    r.push(null());
                }
                out.columns.len() - 1
            };
            for (ri, row) in values.rows.iter().enumerate() {
                match row.get(p).map(|e| ask(e, cx.ids)).transpose()? {
                    Some(Ask::Generate) | None => rows.push(ri),
                    Some(Ask::Explicit(v)) => note(v),
                }
            }
            Some(p)
        }
    };
    if rows.is_empty() && explicit_max.is_none() {
        // Negative or no values of the column: nothing to take or raise.
        return plan_insert(stmt, src, &filled(cx));
    }
    Ok(Plan::NeedIds(Box::new(IdFill {
        stmt: Statement::Insert(out),
        seq: ai.seq,
        // As the statement names it (MariaDB compares table names by case
        // on Linux).
        table: match &ins.table {
            sqlparser::ast::TableObject::TableName(n) => ident_parts(n)
                .and_then(|p| p.last().map(|i| i.value.clone()))
                .unwrap_or_else(|| entry.name.clone()),
            _ => entry.name.clone(),
        },
        column: ai.column.clone(),
        group: Arc::clone(&entry.group),
        rows,
        pos,
        explicit_max,
    })))
}

fn filled<'a>(cx: &PlanCtx<'a>) -> PlanCtx<'a> {
    PlanCtx {
        ids: IdMode {
            filled: true,
            ..cx.ids
        },
        ..*cx
    }
}

impl IdFill {
    /// The statement with the generated values written in (`first`,
    /// `first + step`, … for [`IdFill::rows`] in order), planned as usual.
    /// Planning errors come back as [`Plan::Reject`].
    #[must_use]
    pub fn finish(&self, first: i128, step: i128, cx: &PlanCtx<'_>) -> Plan {
        let mut stmt = self.stmt.clone();
        let num = |v: i128| Expr::value(Value::Number(v.to_string(), false));
        if let Statement::Insert(ins) = &mut stmt {
            match (&mut ins.source, self.pos) {
                (Some(body), Some(p)) => {
                    if let SetExpr::Values(values) = &mut *body.body {
                        let mut v = first;
                        for &r in &self.rows {
                            if let Some(cell) =
                                values.rows.get_mut(r).and_then(|row| row.get_mut(p))
                            {
                                *cell = num(v);
                            }
                            v += step;
                        }
                    }
                }
                (None, _) if !self.rows.is_empty() => {
                    if let Some(a) = ins
                        .assignments
                        .iter_mut()
                        .find(|a| assigns_key(&a.target, &self.column))
                    {
                        a.value = num(first);
                    }
                }
                _ => {}
            }
        }
        plan_insert(&stmt, &Src::text(""), &filled(cx)).unwrap_or_else(Plan::Reject)
    }
}

impl IdFill {
    /// For an `INSERT IGNORE` into an unqualified table: the read of the
    /// lowest of the generated values `first`, `first + step`, … (`count`)
    /// that was inserted, and the statement's row count. MariaDB reports
    /// the first generated value it really inserted; when the statement
    /// skipped rows, the gateway reads it back.
    #[must_use]
    pub fn readback_sql(&self, first: i128, step: i128, count: u64) -> Option<(String, u64)> {
        let Statement::Insert(ins) = &self.stmt else {
            return None;
        };
        let one_name =
            matches!(&ins.table, sqlparser::ast::TableObject::TableName(n) if n.0.len() == 1);
        if !ins.ignore || !one_name || count == 0 {
            return None;
        }
        let rows = match &ins.source {
            Some(q) => match &*q.body {
                SetExpr::Values(v) => v.rows.len() as u64,
                _ => return None,
            },
            None => 1,
        };
        let q = |s: &str| format!("`{}`", s.replace('`', "``"));
        let values: Vec<String> = (0..i128::from(count))
            .map(|k| (first + k * step).to_string())
            .collect();
        Some((
            format!(
                "SELECT MIN({c}) FROM {t} WHERE {c} IN ({v})",
                c = q(&self.column),
                t = q(&self.table),
                v = values.join(", ")
            ),
            rows,
        ))
    }
}

#[cfg(test)]
mod tests;
