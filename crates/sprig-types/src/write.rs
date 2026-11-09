//! Writes on typed tables (chapter 32, "INSERT", "UPDATE", "DELETE"; build
//! step 5): one statement per physical table, in one transaction (or under
//! a savepoint inside the client's), with MySQL's row counts.
//!
//! Not built in this release (39710): `INSERT ... SELECT`, `INSERT
//! IGNORE`, `REPLACE` and `ON DUPLICATE KEY UPDATE` into a subtype, and
//! multi-table `UPDATE` and `DELETE` naming a typed table. A root without
//! subtypes is an ordinary table, and an insert into a root passes through
//! (its column defaults describe the root).

use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::sync::Arc;

use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};
use sqlparser::ast::{
    AssignmentTarget, Delete, Expr, FromTable, Insert, ObjectName, ObjectNamePart, SetExpr,
    TableFactor, TableObject, Update, Value, Visit, Visitor,
};

use crate::Ctx;
use crate::exec::{Done, Rows, Runner, Txn, unsupported};
use crate::model::{DbTypes, TypeDef, TypeId};
use crate::rewrite::{Pass, RefPlan, TYPE_COLUMN, TYPE_PATH_COLUMN, Walker, build_plan, quote_str};

/// What a write plan wants the caller to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOutcome {
    /// Not a typed write: the statement goes through unchanged.
    NotTyped,
    /// Send this one statement instead; the backend's answer is the
    /// client's.
    Forward(String),
    /// The plan ran; answer OK with this.
    Done(Done),
}

fn name_parts(n: &ObjectName) -> Vec<String> {
    n.0.iter()
        .filter_map(|p| match p {
            ObjectNamePart::Identifier(i) => Some(i.value.clone()),
            ObjectNamePart::Function(_) => None,
        })
        .collect()
}

fn split_name(n: &ObjectName) -> Option<(Option<String>, String)> {
    match name_parts(n).as_slice() {
        [t] => Some((None, t.clone())),
        [d, t] => Some((Some(d.clone()), t.clone())),
        _ => None,
    }
}

fn is_type_column(c: &str) -> bool {
    c.eq_ignore_ascii_case(TYPE_COLUMN) || c.eq_ignore_ascii_case(TYPE_PATH_COLUMN)
}

fn read_only(c: &str) -> SprigError {
    SprigError::new(
        SprigCode::TypeColumnReadOnly,
        format!("Column '{c}' is maintained by SprigDB and can't be written"),
    )
}

fn table_sql(db: Option<&str>, t: &str) -> String {
    match db {
        Some(d) => format!("{}.{}", quote(d), quote(t)),
        None => quote(t),
    }
}

/// A value of a text-protocol row as a literal.
pub(crate) fn literal(rows: &Rows, col: usize, v: Option<&Vec<u8>>, nbe: bool) -> String {
    match v {
        None => "NULL".into(),
        Some(b) if rows.numeric(col) => String::from_utf8_lossy(b).into_owned(),
        Some(b) => match std::str::from_utf8(b) {
            Ok(s) => quote_str(s, nbe),
            Err(_) => crate::rewrite::hex_lit(b),
        },
    }
}

/// `k IN (...)` or `(k1, k2) IN ((...), ...)` over `keys` (rows of
/// literals).
pub(crate) fn key_in(alias: Option<&str>, key: &[String], keys: &[Vec<String>]) -> String {
    let col = |k: &str| match alias {
        Some(a) => format!("{}.{}", quote(a), quote(k)),
        None => quote(k),
    };
    if key.len() == 1 {
        let vals: Vec<&str> = keys.iter().map(|r| r[0].as_str()).collect();
        format!("{} IN ({})", col(&key[0]), vals.join(", "))
    } else {
        let cols: Vec<String> = key.iter().map(|k| col(k)).collect();
        let vals: Vec<String> = keys.iter().map(|r| format!("({})", r.join(", "))).collect();
        format!("({}) IN ({})", cols.join(", "), vals.join(", "))
    }
}

/// Plans and runs an `INSERT` into a subtype.
///
/// # Errors
/// 39703, 39705, 39710, 1054, 1136, or a step's error.
pub async fn insert(ins: &Insert, ctx: &Ctx<'_>, r: &mut dyn Runner) -> Result<WriteOutcome> {
    let TableObject::TableName(name) = &ins.table else {
        return Ok(WriteOutcome::NotTyped);
    };
    let Some((db_name, table)) = split_name(name) else {
        return Ok(WriteOutcome::NotTyped);
    };
    let Some(db) = ctx.lookup.db(db_name.as_deref()) else {
        return Ok(WriteOutcome::NotTyped);
    };
    let Some(t) = db.get(&table, ctx.lookup.lctn()).cloned() else {
        return Ok(WriteOutcome::NotTyped);
    };
    let mut columns: Vec<String> = ins
        .columns
        .iter()
        .filter_map(|c| name_parts(c).last().cloned())
        .collect();
    if let Some(c) = columns.iter().find(|c| is_type_column(c)) {
        return Err(read_only(c));
    }
    if t.is_abstract {
        return Err(SprigError::new(
            SprigCode::TypeAbstract,
            format!(
                "Type '{}' is abstract: insert a row of one of its subtypes",
                t.name
            ),
        ));
    }
    if t.is_root() {
        return Ok(WriteOutcome::NotTyped);
    }
    if ins.on.is_some() || ins.replace_into || ins.ignore {
        return Err(unsupported(
            "INSERT IGNORE, REPLACE and ON DUPLICATE KEY UPDATE into a subtype are not supported \
             yet",
        ));
    }
    let mut rows: Vec<Vec<Expr>> = Vec::new();
    if ins.assignments.is_empty() {
        let Some(src) = &ins.source else {
            return Err(unsupported("this INSERT form into a subtype"));
        };
        let SetExpr::Values(v) = &*src.body else {
            return Err(unsupported(
                "INSERT ... SELECT into a subtype is not supported yet",
            ));
        };
        rows = v.rows.iter().map(|r| r.content.clone()).collect();
    } else {
        let mut row = Vec::new();
        for a in &ins.assignments {
            let AssignmentTarget::ColumnName(c) = &a.target else {
                return Err(unsupported("a tuple assignment in INSERT ... SET"));
            };
            let c = name_parts(c).last().cloned().unwrap_or_default();
            if is_type_column(&c) {
                return Err(read_only(&c));
            }
            columns.push(c);
            row.push(a.value.clone());
        }
        rows.push(row);
    }
    let fields = db.all_fields(t.id);
    if columns.is_empty() {
        columns = fields.iter().map(|f| f.name.clone()).collect();
    }
    let mut owner_of = Vec::with_capacity(columns.len());
    for c in &columns {
        let Some(f) = fields.iter().find(|f| f.name.eq_ignore_ascii_case(c)) else {
            return Err(SprigError::standard(
                1054,
                *b"42S22",
                format!("Unknown column '{c}' in 'field list'"),
            ));
        };
        owner_of.push((f.owner, f.is_key));
    }
    for (i, row) in rows.iter().enumerate() {
        if row.len() != columns.len() {
            return Err(SprigError::standard(
                1136,
                *b"21S01",
                format!("Column count doesn't match value count at row {}", i + 1),
            ));
        }
    }
    let key_defs = db.key_defs(t.id);
    let key: Vec<String> = key_defs.iter().map(|k| k.name.clone()).collect();
    let key_idx: Vec<Option<usize>> = key
        .iter()
        .map(|k| columns.iter().position(|c| c.eq_ignore_ascii_case(k)))
        .collect();
    let is_absent = |e: &Expr| {
        matches!(e, Expr::Value(v) if matches!(v.value, Value::Null))
            || matches!(e, Expr::Identifier(i) if i.value.eq_ignore_ascii_case("DEFAULT"))
    };
    let auto = key_defs.len() == 1
        && key_defs[0].auto_increment()
        && match key_idx[0] {
            None => true,
            Some(i) => rows.iter().all(|r| is_absent(&r[i])),
        };
    if !auto {
        for (ki, k) in key.iter().enumerate() {
            let Some(i) = key_idx[ki] else {
                return Err(SprigError::standard(
                    1364,
                    *b"HY000",
                    format!("Field '{k}' doesn't have a default value"),
                ));
            };
            for row in &rows {
                let ok = match &row[i] {
                    Expr::Value(v) => !matches!(v.value, Value::Null | Value::Placeholder(_)),
                    Expr::UnaryOp { expr, .. } => matches!(**expr, Expr::Value(_)),
                    _ => false,
                };
                if !ok {
                    return Err(unsupported(format!(
                        "the key '{k}' of a subtype row must be a literal"
                    )));
                }
            }
        }
    }
    let chain = db.chain(t.id);
    let db_sql = db_name.as_deref();
    // Per chain table: the column indexes it owns (keys excluded).
    let own: Vec<Vec<usize>> = chain
        .iter()
        .map(|x| {
            (0..columns.len())
                .filter(|&i| !owner_of[i].1 && owner_of[i].0 == x.id)
                .collect()
        })
        .collect();
    let type_vals = format!(
        "{}, {}",
        quote_str(&t.name, false),
        quote_str(&t.path, false)
    );
    let stmt_for = |xi: usize, x: &TypeDef, rows: &[Vec<String>]| -> String {
        let mut cols: Vec<String> = if auto && xi == 0 {
            Vec::new()
        } else {
            key.iter().map(|k| quote(k)).collect()
        };
        cols.extend(own[xi].iter().map(|&i| quote(&columns[i])));
        if xi == 0 {
            cols.push(quote(TYPE_COLUMN));
            cols.push(quote(TYPE_PATH_COLUMN));
        }
        let vals: Vec<String> = rows.iter().map(|r| format!("({})", r.join(", "))).collect();
        format!(
            "INSERT INTO {} ({}) VALUES {}",
            table_sql(db_sql, &x.name),
            cols.join(", "),
            vals.join(", ")
        )
    };
    // The literal rows per table.
    let values = |xi: usize, row: &[Expr], key_vals: &[String]| -> Vec<String> {
        let mut v: Vec<String> = if auto && xi == 0 {
            Vec::new()
        } else {
            key_vals.to_vec()
        };
        v.extend(own[xi].iter().map(|&i| row[i].to_string()));
        if xi == 0 {
            v.push(type_vals.clone());
        }
        v
    };
    let n = rows.len();
    let tx = Txn::begin(r).await?;
    let result: Result<u64> = async {
        let mut first_id = 0u64;
        if auto {
            for row in &rows {
                let d = r
                    .exec(&stmt_for(0, &chain[0], &[values(0, row, &[])]))
                    .await?;
                let id = d.last_insert_id;
                if first_id == 0 {
                    first_id = id;
                }
                let kv = vec![id.to_string()];
                for (xi, x) in chain.iter().enumerate().skip(1) {
                    r.exec(&stmt_for(xi, x, &[values(xi, row, &kv)])).await?;
                }
            }
        } else {
            for chunk in rows.chunks(ctx.limits.write_batch_rows.max(1)) {
                for (xi, x) in chain.iter().enumerate() {
                    let lits: Vec<Vec<String>> = chunk
                        .iter()
                        .map(|row| {
                            let kv: Vec<String> = key_idx
                                .iter()
                                .map(|i| i.map(|i| row[i].to_string()).unwrap_or_default())
                                .collect();
                            values(xi, row, &kv)
                        })
                        .collect();
                    r.exec(&stmt_for(xi, x, &lits)).await?;
                }
            }
        }
        Ok(first_id)
    }
    .await;
    let first_id = tx.end(r, result).await?;
    metrics::counter!("sprig_types_statements_total", "kind" => "insert", "plan" => "steps")
        .increment(1);
    Ok(WriteOutcome::Done(Done {
        affected_rows: n as u64,
        last_insert_id: first_id,
        info: if n > 1 {
            format!("Records: {n}  Duplicates: 0  Warnings: 0")
        } else {
            String::new()
        },
        warnings: 0,
    }))
}

/// Column names an expression reads.
fn read_names(e: &Expr) -> BTreeSet<String> {
    struct V(BTreeSet<String>);
    impl Visitor for V {
        type Break = ();
        fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
            match e {
                Expr::Identifier(i) => {
                    self.0.insert(i.value.to_lowercase());
                }
                Expr::CompoundIdentifier(ids) => {
                    if let Some(l) = ids.last() {
                        self.0.insert(l.value.to_lowercase());
                    }
                }
                Expr::Function(f) => {
                    let n = f.name.to_string().to_uppercase();
                    if matches!(n.as_str(), "RAND" | "UUID" | "UUID_SHORT" | "SYSDATE") {
                        self.0.insert("\u{0}nondeterministic".into());
                    }
                }
                _ => {}
            }
            ControlFlow::Continue(())
        }
    }
    let mut v = V(BTreeSet::new());
    let _ = e.visit(&mut v);
    v.0
}

fn order_limit(order_by: &[sqlparser::ast::OrderByExpr], limit: Option<&Expr>) -> String {
    let mut s = String::new();
    if !order_by.is_empty() {
        let o: Vec<String> = order_by.iter().map(ToString::to_string).collect();
        s.push_str(&format!(" ORDER BY {}", o.join(", ")));
    }
    if let Some(l) = limit {
        s.push_str(&format!(" LIMIT {l}"));
    }
    s
}

fn where_sql(selection: Option<&Expr>, filter: Option<&str>) -> String {
    match (selection, filter) {
        (Some(w), Some(f)) => format!(" WHERE ({w}) AND {f}"),
        (Some(w), None) => format!(" WHERE {w}"),
        (None, Some(f)) => format!(" WHERE {f}"),
        (None, None) => String::new(),
    }
}

/// Plans and runs an `UPDATE` of a typed table.
///
/// # Errors
/// 39705, 39706, 39710, or a step's error.
pub async fn update(upd: &Update, ctx: &Ctx<'_>, r: &mut dyn Runner) -> Result<WriteOutcome> {
    let TableFactor::Table { name, alias, .. } = &upd.table.relation else {
        return Ok(WriteOutcome::NotTyped);
    };
    let mut w = Walker::new(ctx.lookup);
    w.only_all = ctx.inheritance_only;
    w.no_backslash_escapes = ctx.lex.no_backslash_escapes;
    let Some(idx) = w.register_target(name, alias.as_ref()) else {
        return Ok(WriteOutcome::NotTyped);
    };
    if !upd.table.joins.is_empty() || upd.from.is_some() {
        return Err(unsupported("a multi-table UPDATE naming a typed table"));
    }
    let tref = w.refs[idx].clone();
    let mut assigned: Vec<(String, Expr)> = Vec::new();
    for a in &upd.assignments {
        let AssignmentTarget::ColumnName(c) = &a.target else {
            return Err(unsupported("a tuple assignment on a typed table"));
        };
        let c = name_parts(c).last().cloned().unwrap_or_default();
        if is_type_column(&c) {
            return Err(read_only(&c));
        }
        if tref
            .db
            .key_defs(tref.t.id)
            .iter()
            .any(|k| k.name.eq_ignore_ascii_case(&c))
            && (tref.has_subtypes() || !tref.t.is_root())
        {
            return Err(SprigError::new(
                SprigCode::TypeKeyUpdate,
                format!(
                    "The key '{c}' of a typed row can't change: delete the row and insert it again"
                ),
            ));
        }
        assigned.push((c, a.value.clone()));
    }
    let only = tref.only && tref.has_subtypes();
    if tref.t.is_root() && !only {
        return Ok(WriteOutcome::NotTyped);
    }
    let mut values: Vec<Expr> = assigned.iter().map(|(_, e)| e.clone()).collect();
    let mut selection = upd.selection.clone();
    let mut order_by = upd.order_by.clone();
    {
        let mut exprs: Vec<&mut Expr> = values.iter_mut().collect();
        if let Some(s) = selection.as_mut() {
            exprs.push(s);
        }
        w.target_exprs(Pass::Collect, idx, &mut exprs, &mut order_by)?;
    }
    let mut owners: BTreeSet<TypeId> = BTreeSet::new();
    let keys = tref.db.key_defs(tref.t.id);
    let is_key = |c: &str| keys.iter().any(|k| k.name.eq_ignore_ascii_case(c));
    for u in w.refs[idx]
        .used
        .iter()
        .chain(assigned.iter().map(|(c, _)| c))
    {
        if is_key(u) {
            continue;
        }
        match w.refs[idx].owner_of(u) {
            Some(o) => {
                owners.insert(o);
            }
            None if assigned.iter().any(|(c, _)| c.eq_ignore_ascii_case(u)) => {
                return Err(SprigError::standard(
                    1054,
                    *b"42S22",
                    format!("Unknown column '{u}' in 'field list'"),
                ));
            }
            None => {}
        }
    }
    let plan = build_plan(&w.refs[idx], &owners, &[]);
    w.refs[idx].plan = Some(plan.clone());
    {
        let mut exprs: Vec<&mut Expr> = values.iter_mut().collect();
        if let Some(s) = selection.as_mut() {
            exprs.push(s);
        }
        w.target_exprs(Pass::Rewrite, idx, &mut exprs, &mut order_by)?;
    }
    let tr = &w.refs[idx];
    let set: Vec<String> = assigned
        .iter()
        .zip(&values)
        .map(|((c, _), v)| {
            let a = plan.alias_for(tr.owner_of(c), false);
            format!("{}.{} = {v}", quote(a), quote(c))
        })
        .collect();
    let key_names: Vec<String> = keys.iter().map(|k| k.name.clone()).collect();
    let join = plan.join_sql(tr.db_name.as_deref(), &key_names);
    let wh = where_sql(selection.as_ref(), plan.filter.as_deref());
    if plan.tables.len() == 1 {
        metrics::counter!("sprig_types_statements_total", "kind" => "update", "plan" => "single")
            .increment(1);
        return Ok(WriteOutcome::Forward(format!(
            "UPDATE {join} SET {}{wh}{}",
            set.join(", "),
            order_limit(&order_by, upd.limit.as_ref())
        )));
    }
    // Several tables: step A finds the rows and MySQL's counts.
    for (i, v) in values.iter().enumerate() {
        let names = read_names(v);
        if names.contains("\u{0}nondeterministic") {
            return Err(unsupported(
                "a non-deterministic SET expression on a subtype stored in several tables",
            ));
        }
        // MySQL reads a column assigned earlier in the same UPDATE as its
        // new value; one statement per table can't.
        if assigned[..i]
            .iter()
            .any(|(c, _)| names.contains(&c.to_lowercase()))
        {
            return Err(unsupported(
                "a SET expression reading a column an earlier assignment of the same UPDATE \
                 sets, on a subtype stored in several tables",
            ));
        }
    }
    let ka = plan.key_alias().to_owned();
    let unchanged: Vec<String> = assigned
        .iter()
        .zip(&values)
        .map(|((c, _), v)| {
            let a = plan.alias_for(tr.owner_of(c), false);
            format!("({v}) <=> {}.{}", quote(a), quote(c))
        })
        .collect();
    let key_cols: Vec<String> = key_names
        .iter()
        .map(|k| format!("{}.{}", quote(&ka), quote(k)))
        .collect();
    let step_a = format!(
        "SELECT {}, ({}) FROM {join}{wh}{} FOR UPDATE",
        key_cols.join(", "),
        unchanged.join(" AND "),
        order_limit(&order_by, upd.limit.as_ref())
    );
    let nbe = ctx.lex.no_backslash_escapes;
    let batch = ctx.limits.write_batch_rows.max(1);
    let tx = Txn::begin(r).await?;
    let result: Result<(u64, u64)> = async {
        let found = r.query(&step_a).await?;
        let nk = key_names.len();
        let matched = found.rows.len() as u64;
        let changed = found
            .rows
            .iter()
            .filter(|row| row.get(nk).and_then(|v| v.as_deref()) != Some(b"1".as_slice()))
            .count() as u64;
        let keys: Vec<Vec<String>> = found
            .rows
            .iter()
            .map(|row| {
                (0..nk)
                    .map(|i| literal(&found, i, row[i].as_ref(), nbe))
                    .collect()
            })
            .collect();
        for chunk in keys.chunks(batch) {
            r.exec(&format!(
                "UPDATE {join} SET {} WHERE {}",
                set.join(", "),
                key_in(Some(&ka), &key_names, chunk)
            ))
            .await?;
        }
        Ok((matched, changed))
    }
    .await;
    let (matched, changed) = tx.end(r, result).await?;
    metrics::counter!("sprig_types_statements_total", "kind" => "update", "plan" => "steps")
        .increment(1);
    Ok(WriteOutcome::Done(Done {
        affected_rows: if r.found_rows() { matched } else { changed },
        last_insert_id: 0,
        info: format!("Rows matched: {matched}  Changed: {changed}  Warnings: 0"),
        warnings: 0,
    }))
}

/// The extension tables a delete of rows of `t` (and its subtypes) must
/// clean, deepest first.
fn extension_tables(db: &DbTypes, t: &TypeDef) -> Vec<Arc<TypeDef>> {
    let mut v: Vec<Arc<TypeDef>> = db.chain(t.id).into_iter().skip(1).collect();
    v.extend(db.descendants(t.id));
    v.sort_by(|a, b| b.depth.cmp(&a.depth).then(a.id.cmp(&b.id)));
    v
}

/// Plans and runs a `DELETE` from a typed table.
///
/// # Errors
/// 39710, or a step's error.
pub async fn delete(del: &Delete, ctx: &Ctx<'_>, r: &mut dyn Runner) -> Result<WriteOutcome> {
    let from = match &del.from {
        FromTable::WithFromKeyword(v) | FromTable::WithoutKeyword(v) => v,
    };
    let Some(first) = from.first() else {
        return Ok(WriteOutcome::NotTyped);
    };
    let TableFactor::Table { name, alias, .. } = &first.relation else {
        return Ok(WriteOutcome::NotTyped);
    };
    let mut w = Walker::new(ctx.lookup);
    w.only_all = ctx.inheritance_only;
    w.no_backslash_escapes = ctx.lex.no_backslash_escapes;
    let Some(idx) = w.register_target(name, alias.as_ref()) else {
        return Ok(WriteOutcome::NotTyped);
    };
    if from.len() > 1 || !first.joins.is_empty() || !del.tables.is_empty() || del.using.is_some() {
        return Err(unsupported("a multi-table DELETE naming a typed table"));
    }
    let tref = w.refs[idx].clone();
    if tref.t.is_root() && !tref.has_subtypes() {
        return Ok(WriteOutcome::NotTyped);
    }
    let mut selection = del.selection.clone();
    let mut order_by = del.order_by.clone();
    {
        let mut exprs: Vec<&mut Expr> = selection.iter_mut().collect();
        w.target_exprs(Pass::Collect, idx, &mut exprs, &mut order_by)?;
    }
    let keys = tref.db.key_defs(tref.t.id);
    let key_names: Vec<String> = keys.iter().map(|k| k.name.clone()).collect();
    let owners: BTreeSet<TypeId> = w.refs[idx]
        .used
        .iter()
        .filter(|u| !key_names.iter().any(|k| k.eq_ignore_ascii_case(u)))
        .filter_map(|u| w.refs[idx].owner_of(u))
        .collect();
    let plan: RefPlan = build_plan(&w.refs[idx], &owners, &[tref.t.root]);
    w.refs[idx].plan = Some(plan.clone());
    {
        let mut exprs: Vec<&mut Expr> = selection.iter_mut().collect();
        w.target_exprs(Pass::Rewrite, idx, &mut exprs, &mut order_by)?;
    }
    let root_alias = plan
        .tables
        .iter()
        .find(|(t, _)| t.is_root())
        .map(|(_, a)| a.clone())
        .ok_or_else(|| SprigError::internal("delete plan without its root"))?;
    let join = plan.join_sql(tref.db_name.as_deref(), &key_names);
    let key_cols: Vec<String> = key_names
        .iter()
        .map(|k| format!("{}.{}", quote(&root_alias), quote(k)))
        .collect();
    let step_a = format!(
        "SELECT {}, {}.{} FROM {join}{}{} FOR UPDATE",
        key_cols.join(", "),
        quote(&root_alias),
        quote(TYPE_PATH_COLUMN),
        where_sql(selection.as_ref(), plan.filter.as_deref()),
        order_limit(&order_by, del.limit.as_ref())
    );
    let exts = extension_tables(&tref.db, &tref.t);
    let root = tref
        .db
        .root_of(tref.t.id)
        .ok_or_else(|| SprigError::internal("typed table without a root"))?;
    let db_sql = tref.db_name.clone();
    let nbe = ctx.lex.no_backslash_escapes;
    let batch = ctx.limits.write_batch_rows.max(1);
    let tx = Txn::begin(r).await?;
    let result: Result<u64> = async {
        let found = r.query(&step_a).await?;
        let nk = key_names.len();
        let rows: Vec<(Vec<String>, Vec<u8>)> = found
            .rows
            .iter()
            .map(|row| {
                (
                    (0..nk)
                        .map(|i| literal(&found, i, row[i].as_ref(), nbe))
                        .collect(),
                    row.get(nk).cloned().flatten().unwrap_or_default(),
                )
            })
            .collect();
        for e in &exts {
            let keys: Vec<Vec<String>> = rows
                .iter()
                .filter(|(_, p)| p.starts_with(e.path.as_bytes()))
                .map(|(k, _)| k.clone())
                .collect();
            for chunk in keys.chunks(batch) {
                r.exec(&format!(
                    "DELETE FROM {} WHERE {}",
                    table_sql(db_sql.as_deref(), &e.name),
                    key_in(None, &key_names, chunk)
                ))
                .await?;
            }
        }
        let keys: Vec<Vec<String>> = rows.into_iter().map(|(k, _)| k).collect();
        for chunk in keys.chunks(batch) {
            r.exec(&format!(
                "DELETE FROM {} WHERE {}",
                table_sql(db_sql.as_deref(), &root.name),
                key_in(None, &key_names, chunk)
            ))
            .await?;
        }
        Ok(keys.len() as u64)
    }
    .await;
    let n = tx.end(r, result).await?;
    metrics::counter!("sprig_types_statements_total", "kind" => "delete", "plan" => "steps")
        .increment(1);
    Ok(WriteOutcome::Done(Done {
        affected_rows: n,
        ..Done::default()
    }))
}
