//! Planning statements with `GRAPH_TABLE` (chapter 17's `Plan::Graph`,
//! R8 round 6; the plan's types and binding are in [`crate::graph`]).
//!
//! ```text
//! plan_graph(sql with placeholders, nonce, calls, steps):
//!   EXPLAIN FORMAT=SPRIG s       → Explain(plan_graph(s))
//!   each call has its placeholder → else Reject (a rewrite lost it)
//!   steps: a grouped graph's shards from the topology's group
//!   outer_plan: plan(s with every call empty)
//!   keys: in the top SELECT, `t.k = g.c` in WHERE or an inner join's ON,
//!         t a sharded table and k its routing key, g a call's placeholder
//! ```
//!
//! A derived table of literal rows (what a call becomes once bound, or
//! any `(SELECT 1 AS a UNION ALL VALUES (2), …)`) reads no table and gives
//! the same rows on every shard: the planner treats it like a reference
//! table with a copy everywhere ([`constant_relation`]).

use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, ObjectNamePart, Query, Select, SetExpr, Statement,
    TableFactor, UnaryOperator, Value,
};

use super::{PlanCtx, Rel, analyse, col_rel, key_rel, parse, strip_explain_sprig};
use crate::graph::{GraphCall, GraphPlan, GraphStep, KeyJoin, PENDING_FN, slots};
use crate::plan::Plan;
use crate::reject::{Reason, Rejection};
use crate::topology::TableEntry;

/// A literal: a value (not a `?`), a signed number, a string with an
/// introducer, in parentheses or not.
fn literal(e: &Expr) -> bool {
    match e {
        Expr::Value(v) => !matches!(v.value, Value::Placeholder(_)),
        Expr::Nested(x) => literal(x),
        Expr::UnaryOp {
            op: UnaryOperator::Minus | UnaryOperator::Plus,
            expr,
        } => matches!(&**expr, Expr::Value(v) if matches!(v.value, Value::Number(..))),
        Expr::Prefixed { value, .. } => literal(value),
        _ => false,
    }
}

/// The call a placeholder's condition names, if `e` is one.
fn pending_call(e: &Expr) -> Option<usize> {
    let Expr::Function(f) = e else { return None };
    // The last part: a rewrite may qualify the function's name.
    let Some(ObjectNamePart::Identifier(n)) = f.name.0.last() else {
        return None;
    };
    if !n.value.eq_ignore_ascii_case(PENDING_FN) {
        return None;
    }
    let sqlparser::ast::FunctionArguments::List(l) = &f.args else {
        return None;
    };
    let mut nums = l.args.iter().filter_map(|a| match a {
        sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
            Expr::Value(v),
        )) => match &v.value {
            Value::Number(n, _) => n.parse::<usize>().ok(),
            _ => None,
        },
        _ => None,
    });
    let _nonce = nums.next()?;
    nums.next()
}

/// What a constant derived table is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Constant {
    /// Literal rows.
    Rows,
    /// Call `n`'s placeholder.
    Call(usize),
}

impl Constant {
    fn call(self) -> Option<usize> {
        match self {
            Self::Rows => None,
            Self::Call(c) => Some(c),
        }
    }
}

/// A `SELECT` of literals with no table (or `DUAL`) and a literal or no
/// condition, or a call's placeholder.
fn constant_select(s: &Select) -> Option<Constant> {
    let simple = s.into.is_none()
        && s.having.is_none()
        && s.distinct.is_none()
        && s.top.is_none()
        && s.prewhere.is_none()
        && s.qualify.is_none()
        && s.named_window.is_empty()
        && s.lateral_views.is_empty()
        && s.connect_by.is_empty()
        && matches!(&s.group_by, GroupByExpr::Expressions(v, m) if v.is_empty() && m.is_empty());
    if !simple {
        return None;
    }
    let dual = match s.from.as_slice() {
        [] => true,
        [t] => {
            t.joins.is_empty()
                && matches!(&t.relation, TableFactor::Table { name, args: None, alias: None, .. }
                    if matches!(name.0.as_slice(), [ObjectNamePart::Identifier(i)]
                        if i.value.eq_ignore_ascii_case("dual")))
        }
        _ => false,
    };
    if !dual {
        return None;
    }
    let items = s.projection.iter().all(|p| match p {
        sqlparser::ast::SelectItem::UnnamedExpr(e)
        | sqlparser::ast::SelectItem::ExprWithAlias { expr: e, .. } => literal(e),
        _ => false,
    });
    if !items {
        return None;
    }
    match &s.selection {
        None => Some(Constant::Rows),
        Some(e) if literal(e) => Some(Constant::Rows),
        Some(e) => pending_call(e).map(Constant::Call),
    }
}

fn constant_body(s: &SetExpr) -> Option<Constant> {
    match s {
        SetExpr::Select(sel) => constant_select(sel),
        SetExpr::Query(q) => constant_query(q),
        SetExpr::Values(v) => v
            .rows
            .iter()
            .all(|r| r.content.iter().all(literal))
            .then_some(Constant::Rows),
        SetExpr::SetOperation {
            op: sqlparser::ast::SetOperator::Union,
            left,
            right,
            ..
        } => {
            constant_body(left)?;
            constant_body(right)?;
            Some(Constant::Rows)
        }
        _ => None,
    }
}

/// Whether `q` gives the same literal rows wherever it runs (or is a
/// call's placeholder).
pub(super) fn constant_query(q: &Query) -> Option<Constant> {
    if q.with.is_some()
        || q.order_by.is_some()
        || q.limit_clause.is_some()
        || q.fetch.is_some()
        || !q.locks.is_empty()
        || q.for_clause.is_some()
        || !q.pipe_operators.is_empty()
    {
        return None;
    }
    constant_body(&q.body)
}

/// A derived table of literal rows as a relation of its block: no table,
/// on every shard (see the module docs). `None` for any other FROM item.
pub(super) fn constant_relation<'a>(tf: &TableFactor, item: usize) -> Option<Rel<'a>> {
    // `FROM DUAL` names no table either (an empty call's rows).
    if let TableFactor::Table {
        name, args: None, ..
    } = tf
        && matches!(name.0.as_slice(), [ObjectNamePart::Identifier(i)]
            if i.value.eq_ignore_ascii_case("dual") && i.quote_style.is_none())
    {
        return Some(Rel {
            entry: None,
            default_schema: String::new(),
            table: "dual".into(),
            alias: None,
            loc: (0, 0),
            nullable: false,
            item,
            constant: true,
            graph: None,
        });
    }
    let TableFactor::Derived {
        lateral: false,
        subquery,
        alias,
        sample: None,
    } = tf
    else {
        return None;
    };
    // `AS g (a, b)` renames columns: not modelled, so not constant.
    if alias.as_ref().is_some_and(|a| !a.columns.is_empty()) {
        return None;
    }
    let graph = constant_query(subquery)?.call();
    let alias = alias.as_ref().map(|a| a.name.value.to_ascii_lowercase());
    Some(Rel {
        entry: None,
        default_schema: String::new(),
        table: alias.clone().unwrap_or_default(),
        alias,
        loc: (0, 0),
        nullable: false,
        item,
        constant: true,
        graph,
    })
}

/// Plans `sql`, a statement whose `GRAPH_TABLE` calls (`calls`, found
/// before the pipeline) are placeholders of `nonce`. `steps` give each
/// call's graph; a grouped graph's shards are filled from the topology.
#[must_use]
pub fn plan_graph(
    sql: &str,
    nonce: u64,
    calls: &[GraphCall],
    steps: Vec<GraphStep>,
    cx: &PlanCtx<'_>,
) -> Plan {
    if let Some(rest) = strip_explain_sprig(sql) {
        return match plan_graph(rest, nonce, calls, steps, cx) {
            p @ Plan::Graph(_) => Plan::Explain(Box::new(p)),
            p => p,
        };
    }
    let slots = slots(sql, nonce);
    if (0..calls.len()).any(|c| !slots.iter().any(|(s, _)| *s == c)) {
        return Plan::Reject(Rejection::new(
            Reason::Unsupported,
            "a GRAPH_TABLE call was lost by a rewrite of the statement",
        ));
    }
    let steps = steps
        .into_iter()
        .map(|mut s| {
            if s.shards.is_empty()
                && let Some(g) = s.group.and_then(|id| cx.topo.group(id))
            {
                s.shards.clone_from(&g.shards);
            }
            s
        })
        .collect();
    let empty = GraphPlan::bind_empty(sql, &slots, calls);
    let outer_plan = super::plan(&empty, cx);
    let keys = parse(sql)
        .ok()
        .map(|stmt| key_joins(&stmt, calls, cx))
        .unwrap_or_default();
    Plan::Graph(Box::new(GraphPlan {
        nonce,
        calls: calls.to_vec(),
        outer: sql.to_owned(),
        slots,
        steps,
        keys,
        outer_plan: Box::new(outer_plan),
    }))
}

/// The key joins of the top `SELECT` (see [`KeyJoin`]).
fn key_joins(stmt: &Statement, calls: &[GraphCall], cx: &PlanCtx<'_>) -> Vec<KeyJoin> {
    let Statement::Query(q) = stmt else {
        return Vec::new();
    };
    let SetExpr::Select(select) = &*q.body else {
        return Vec::new();
    };
    let Ok(b) = analyse(select, cx) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    // WHERE rejects NULL-extended rows of an outer join's table too, so
    // its equalities hold for every result row; an inner join's ON
    // likewise. A LEFT JOIN's ON doesn't.
    for c in b.where_conj.iter().chain(b.inner_on.iter()) {
        let Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } = c
        else {
            continue;
        };
        for (k, g) in [(&**left, &**right), (&**right, &**left)] {
            let Some(i) = key_rel(&b.rels, k) else {
                continue;
            };
            let Some(entry) = b.rels[i].entry.filter(|e| !e.is_reference()) else {
                continue;
            };
            let Some((j, col)) = col_rel(&b.rels, g) else {
                continue;
            };
            let Some(call) = b.rels[j].graph else {
                continue;
            };
            let Some(column) = calls
                .get(call)
                .and_then(|c| c.columns.iter().position(|x| x.eq_ignore_ascii_case(&col)))
            else {
                continue;
            };
            out.push(KeyJoin {
                call,
                column,
                table: TableEntry::key(entry)
                    .map_or_else(|| entry.name.clone(), |k| format!("{}.{k}", entry.name)),
                group: std::sync::Arc::clone(&entry.group),
            });
        }
    }
    out
}
