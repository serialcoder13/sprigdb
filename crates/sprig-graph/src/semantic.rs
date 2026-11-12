//! Compile-time semantic checks (chapter 43 build step 5, the binder;
//! decision G43): the errors openCypher requires before a statement runs,
//! as Neo4j raises them (`Neo.ClientError.Statement.SyntaxError`, 39900).
//!
//! - **Variables.** A name used as a node, a relationship and a path, or
//!   matched as an element after it was bound to a value, is a
//!   `VariableTypeConflict`; a path variable that is already bound, or a
//!   variable `CREATE` or `MERGE` would bind again, is `VariableAlreadyBound`;
//!   a name nothing bound is `UndefinedVariable` (patterns used as
//!   predicates may not introduce names); one relationship variable twice
//!   in a `MATCH` is a `RelationshipUniquenessViolation`.
//! - **Aggregation.** Aggregates in `WHERE`, nested aggregates, and
//!   aggregates in `ORDER BY` of a projection that doesn't aggregate are
//!   `InvalidAggregation`.
//! - **Argument types** known from the text alone: boolean operators over
//!   literals of other types, `length()` of a node or relationship,
//!   `size()` of a path, `type()` of a node, properties of a path,
//!   `DELETE` of something that isn't an element or path.
//! - **Patterns as values** (`RETURN (a)-->()`, `size((a)-->())`) are
//!   `UnexpectedSyntax`: Cypher 5 allows them only as predicates.
//! - **`CALL { } IN TRANSACTIONS` after a write** (`CREATE`, `MERGE`,
//!   `SET`, `REMOVE`, `DELETE`, `FOREACH`, or a subquery that writes) in
//!   the same query is refused, with Neo4j 5.26's message.
//!
//! The checks are conservative: a variable whose kind isn't known from
//! the text (a parameter, a function result, a subquery's column) never
//! conflicts, and scopes the checker can't follow (`CALL` without `YIELD`,
//! `RETURN *` of a subquery) turn the undefined-variable check off rather
//! than guess.

use std::collections::HashMap;

use sprig_common::{Result, SprigCode, SprigError};

use crate::ast::{
    BinOp, Clause, Expr, MapItem, PathPattern, Projection, Query, RemoveItem, SetItem, UnOp,
    is_aggregate,
};
use crate::value::Value;

/// What a variable is bound to, as far as the text says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Node,
    Rel,
    /// A variable-length relationship variable (a list of relationships).
    RelList,
    Path,
    /// A value of a type that is no element (a literal, list or map).
    Value,
    /// Unknown.
    Any,
}

fn err(kind: &str, msg: impl std::fmt::Display) -> SprigError {
    SprigError::new(
        SprigCode::GraphSyntax,
        format!("[Neo.ClientError.Statement.SyntaxError] {kind}: {msg}"),
    )
}

#[derive(Debug, Clone, Default)]
struct Scope {
    vars: HashMap<String, Kind>,
    /// Names may be bound in ways the checker doesn't follow: no
    /// undefined-variable errors.
    open: bool,
}

impl Scope {
    fn has(&self, v: &str) -> bool {
        self.open || self.vars.contains_key(v)
    }
    fn kind(&self, v: &str) -> Kind {
        self.vars.get(v).copied().unwrap_or(Kind::Any)
    }
}

/// Checks `q`.
///
/// # Errors
/// 39900 naming the rule, as the module docs list.
pub fn check(q: &Query) -> Result<()> {
    check_query(q, &Scope::default())
}

fn check_query(q: &Query, outer: &Scope) -> Result<()> {
    for part in &q.parts {
        let mut s = outer.clone();
        check_clauses(&part.clauses, &mut s)?;
    }
    Ok(())
}

/// The kind an expression has, when the text says.
fn kind_of(e: &Expr, s: &Scope) -> Kind {
    match e {
        Expr::Var(v) => s.kind(v),
        Expr::Lit(Value::Null) => Kind::Any,
        Expr::Lit(_) | Expr::List(_) | Expr::Map(_) | Expr::ListComp { .. } => Kind::Value,
        Expr::Func { name, .. } if matches!(name.as_str(), "startnode" | "endnode") => Kind::Node,
        _ => Kind::Any,
    }
}

fn is_element(k: Kind) -> bool {
    matches!(k, Kind::Node | Kind::Rel | Kind::RelList | Kind::Path)
}

fn check_clauses(clauses: &[Clause], s: &mut Scope) -> Result<()> {
    for (i, c) in clauses.iter().enumerate() {
        // R8 round 9 (decision G76): as Neo4j 5.26, a batched `CALL` may
        // not follow a write of the same query (those writes would belong
        // to no batch). An earlier batched `CALL` committed its own.
        let batched = |c: &Clause| matches!(c, Clause::Subquery { batch: Some(_), .. });
        if batched(c)
            && clauses[..i]
                .iter()
                .any(|p| !batched(p) && crate::exec::clauses_write(std::slice::from_ref(p)))
        {
            return Err(SprigError::new(
                SprigCode::GraphSyntax,
                "[Neo.ClientError.Statement.SyntaxError] CALL { ... } IN TRANSACTIONS after a write clause is not supported",
            ));
        }
        check_clause(c, s)?;
    }
    Ok(())
}

#[allow(clippy::too_many_lines)] // one arm per clause
fn check_clause(c: &Clause, s: &mut Scope) -> Result<()> {
    match c {
        Clause::Match {
            patterns, where_, ..
        } => {
            bind_match(patterns, s)?;
            for p in patterns {
                pattern_exprs(p, s)?;
            }
            if let Some(w) = where_ {
                no_aggregate(w, "WHERE")?;
                expr(w, s)?;
                predicate(w, s)?;
            }
        }
        Clause::Unwind { expr: e, alias } => {
            expr(e, s)?;
            s.vars.insert(alias.clone(), Kind::Any);
        }
        Clause::With(p) | Clause::Return(p) => projection(p, s)?,
        Clause::Create(patterns) => {
            bind_create(patterns, s, "CREATE")?;
            for p in patterns {
                pattern_exprs(p, s)?;
            }
        }
        Clause::Merge {
            pattern,
            on_create,
            on_match,
        } => {
            bind_create(std::slice::from_ref(pattern), s, "MERGE")?;
            pattern_exprs(pattern, s)?;
            for i in on_create.iter().chain(on_match) {
                set_item(i, s)?;
            }
        }
        Clause::Set(items) => {
            for i in items {
                set_item(i, s)?;
            }
        }
        Clause::Remove(items) => {
            for i in items {
                match i {
                    RemoveItem::Prop(e, _) => expr(e, s)?,
                    RemoveItem::Labels(v, _) => var(v, s)?,
                }
            }
        }
        Clause::Delete { exprs, .. } => {
            for e in exprs {
                expr(e, s)?;
                if matches!(
                    e,
                    Expr::Lit(_)
                        | Expr::Binary(..)
                        | Expr::Unary(..)
                        | Expr::List(_)
                        | Expr::Map(_)
                ) && !matches!(e, Expr::Lit(Value::Null))
                {
                    return Err(err(
                        "InvalidArgumentType",
                        "DELETE takes nodes, relationships or paths",
                    ));
                }
                if matches!(e, Expr::HasLabels(..)) {
                    return Err(err(
                        "InvalidDelete",
                        "DELETE can't remove labels; use REMOVE",
                    ));
                }
            }
        }
        Clause::Subquery {
            imports,
            query,
            batch,
        } => {
            let mut inner = Scope {
                vars: s.vars.clone(),
                open: true,
            };
            if let Some(names) = imports {
                for n in names {
                    var(n, s)?;
                }
                inner.vars.retain(|k, _| names.contains(k));
            }
            check_query(query, &inner)?;
            match returns_of(query) {
                Some(cols) => {
                    for c in cols {
                        s.vars.insert(c, Kind::Any);
                    }
                }
                None => s.open = true,
            }
            if let Some(r) = batch.as_ref().and_then(|b| b.report.as_ref()) {
                s.vars.insert(r.clone(), Kind::Any);
            }
        }
        Clause::Foreach { var: v, list, body } => {
            expr(list, s)?;
            let mut inner = s.clone();
            inner.vars.insert(v.clone(), Kind::Any);
            check_clauses(body, &mut inner)?;
        }
        Clause::Schema(_) => {}
        Clause::Call {
            args,
            yields,
            where_,
            ..
        } => {
            for a in args.iter().flatten() {
                expr(a, s)?;
            }
            match yields {
                Some(y) => {
                    for (_, alias) in y {
                        s.vars.insert(alias.clone(), Kind::Any);
                    }
                }
                None => s.open = true,
            }
            if let Some(w) = where_ {
                expr(w, s)?;
            }
        }
    }
    Ok(())
}

/// The columns a subquery returns; `None` for `RETURN *` or none known.
pub(crate) fn returns_of(q: &Query) -> Option<Vec<String>> {
    let last = q.parts.first()?.clauses.last()?;
    match last {
        Clause::Return(p) if !p.star => Some(p.items.iter().map(|(_, n)| n.clone()).collect()),
        Clause::Return(_) => None,
        // A unit subquery returns nothing.
        _ => Some(Vec::new()),
    }
}

fn set_item(i: &SetItem, s: &Scope) -> Result<()> {
    match i {
        SetItem::Prop(e, _, v) => {
            expr(e, s)?;
            expr(v, s)?;
            value_expr(v)
        }
        SetItem::Replace(n, v) | SetItem::Merge(n, v) => {
            var(n, s)?;
            expr(v, s)?;
            value_expr(v)
        }
        SetItem::Labels(n, _) => var(n, s),
    }
}

fn var(v: &str, s: &Scope) -> Result<()> {
    if s.has(v) {
        Ok(())
    } else {
        Err(err(
            "UndefinedVariable",
            format!("Variable `{v}` not defined"),
        ))
    }
}

/// Binds the variables of a `MATCH`'s patterns.
fn bind_match(patterns: &[PathPattern], s: &mut Scope) -> Result<()> {
    let before = s.clone();
    // Element kinds the clause gives each name.
    let mut here: HashMap<String, Kind> = HashMap::new();
    let mut rels_here: Vec<String> = Vec::new();
    let mut paths_here: Vec<String> = Vec::new();
    let put = |name: &str, k: Kind, here: &mut HashMap<String, Kind>| -> Result<()> {
        let prev = here.get(name).copied().unwrap_or_else(|| {
            if before.vars.contains_key(name) {
                before.kind(name)
            } else {
                Kind::Any
            }
        });
        // A list of relationships may be matched as a variable-length one
        // (deprecated, but openCypher allows it).
        let same = prev == Kind::Any
            || prev == k
            || matches!(
                (prev, k),
                (Kind::RelList, Kind::Rel) | (Kind::Rel | Kind::Value, Kind::RelList)
            );
        if !same {
            return Err(err(
                "VariableTypeConflict",
                format!("Variable `{name}` already declared as another type"),
            ));
        }
        here.insert(name.to_owned(), k);
        Ok(())
    };
    for p in patterns {
        if let Some(pv) = &p.var {
            if before.vars.contains_key(pv) || here.contains_key(pv) {
                return Err(err(
                    "VariableAlreadyBound",
                    format!("Variable `{pv}` already declared"),
                ));
            }
            paths_here.push(pv.clone());
        }
        let mut nodes = vec![&p.start];
        for (r, n) in &p.steps {
            nodes.push(n);
            if let Some(rv) = &r.var {
                if rels_here.contains(rv) {
                    return Err(err(
                        "RelationshipUniquenessViolation",
                        format!(
                            "Cannot use the same relationship variable '{rv}' for multiple relationships"
                        ),
                    ));
                }
                rels_here.push(rv.clone());
                let k = if r.range.is_some() {
                    Kind::RelList
                } else {
                    Kind::Rel
                };
                if paths_here.contains(rv) {
                    return Err(err(
                        "VariableAlreadyBound",
                        format!("Variable `{rv}` already declared"),
                    ));
                }
                put(rv, k, &mut here)?;
            }
        }
        for n in nodes {
            if let Some(nv) = &n.var {
                if paths_here.contains(nv) {
                    return Err(err(
                        "VariableAlreadyBound",
                        format!("Variable `{nv}` already declared"),
                    ));
                }
                put(nv, Kind::Node, &mut here)?;
            }
        }
    }
    // A path variable named after an element of a later pattern.
    for pv in &paths_here {
        if here.contains_key(pv) {
            return Err(err(
                "VariableAlreadyBound",
                format!("Variable `{pv}` already declared"),
            ));
        }
    }
    for (k, v) in here {
        s.vars.insert(k, v);
    }
    for pv in paths_here {
        s.vars.insert(pv, Kind::Path);
    }
    Ok(())
}

/// Binds the variables of `CREATE` or `MERGE` patterns.
fn bind_create(patterns: &[PathPattern], s: &mut Scope, what: &str) -> Result<()> {
    let bound = |n: &str, s: &Scope| s.vars.contains_key(n);
    for p in patterns {
        if let Some(pv) = &p.var
            && bound(pv, s)
        {
            return Err(err(
                "VariableAlreadyBound",
                format!("Variable `{pv}` already declared"),
            ));
        }
        if let Some(nv) = &p.start.var
            && bound(nv, s)
            && (p.steps.is_empty() || p.start.labels.is_some() || p.start.props.is_some())
        {
            return Err(err(
                "VariableAlreadyBound",
                format!("Can't {what} `{nv}` with properties or labels here: it already exists"),
            ));
        }
        for (r, n) in &p.steps {
            if r.types.len() != 1 {
                return Err(err(
                    "NoSingleRelationshipType",
                    format!("Exactly one relationship type must be specified for {what}"),
                ));
            }
            if let Some(rv) = &r.var
                && bound(rv, s)
            {
                return Err(err(
                    "VariableAlreadyBound",
                    format!("Can't {what} relationship `{rv}`: it already exists"),
                ));
            }
            if let Some(nv) = &n.var
                && bound(nv, s)
                && (n.labels.is_some() || n.props.is_some())
            {
                return Err(err(
                    "VariableAlreadyBound",
                    format!(
                        "Can't {what} `{nv}` with properties or labels here: it already exists"
                    ),
                ));
            }
            if let Some(rv) = &r.var {
                s.vars.insert(rv.clone(), Kind::Rel);
            }
            if let Some(nv) = &n.var
                && !bound(nv, s)
            {
                s.vars.insert(nv.clone(), Kind::Node);
            }
        }
        if let Some(nv) = &p.start.var
            && !bound(nv, s)
        {
            s.vars.insert(nv.clone(), Kind::Node);
        }
        if let Some(pv) = &p.var {
            s.vars.insert(pv.clone(), Kind::Path);
        }
    }
    Ok(())
}

/// The property maps of a pattern.
fn pattern_exprs(p: &PathPattern, s: &Scope) -> Result<()> {
    if let Some(e) = &p.start.props {
        expr(e, s)?;
    }
    for (r, n) in &p.steps {
        if let Some(e) = &r.props {
            expr(e, s)?;
        }
        if let Some(e) = &n.props {
            expr(e, s)?;
        }
    }
    Ok(())
}

fn no_aggregate(e: &Expr, where_: &str) -> Result<()> {
    if e.has_aggregate() {
        return Err(err(
            "InvalidAggregation",
            format!("Invalid use of aggregating function in {where_}"),
        ));
    }
    Ok(())
}

/// A `WHERE`: an element used as the whole predicate.
fn predicate(e: &Expr, s: &Scope) -> Result<()> {
    if let Expr::Pattern(p) = e
        && p.steps.is_empty()
    {
        return Err(err(
            "InvalidArgumentType",
            "Type mismatch: a node is not a boolean",
        ));
    }
    if let Expr::Var(v) = e
        && is_element(s.kind(v))
    {
        return Err(err(
            "InvalidArgumentType",
            format!("Type mismatch: `{v}` is not a boolean"),
        ));
    }
    Ok(())
}

/// Patterns may only be predicates.
fn value_expr(e: &Expr) -> Result<()> {
    let mut bad = false;
    e.walk(&mut |x| {
        if let Expr::Func { name, args, .. } = x
            && name != "exists"
            && args.iter().any(|a| matches!(a, Expr::Pattern(_)))
        {
            bad = true;
        }
    });
    if bad || matches!(e, Expr::Pattern(_)) {
        return Err(err(
            "UnexpectedSyntax",
            "a pattern expression is only allowed as a predicate here",
        ));
    }
    Ok(())
}

fn projection(p: &Projection, s: &mut Scope) -> Result<()> {
    let aggregating = p.items.iter().any(|(e, _)| e.has_aggregate());
    for (e, _) in &p.items {
        expr(e, s)?;
        value_expr(e)?;
        nested_aggregate(e)?;
    }
    let mut next = Scope {
        vars: HashMap::new(),
        open: s.open,
    };
    if p.star {
        next.vars.clone_from(&s.vars);
    }
    for (e, n) in &p.items {
        next.vars.insert(n.clone(), kind_of(e, s));
    }
    // ORDER BY and WHERE see the projected names, and (without
    // aggregation or DISTINCT) the names before it.
    let mut after = next.clone();
    let keeps_old = !aggregating && !p.distinct;
    if keeps_old {
        for (k, v) in &s.vars {
            after.vars.entry(k.clone()).or_insert(*v);
        }
    }
    for (o, _) in &p.order {
        if !aggregating && o.has_aggregate() {
            return Err(err(
                "InvalidAggregation",
                "Invalid use of aggregating function in ORDER BY of a projection without aggregation",
            ));
        }
        if keeps_old {
            expr(o, &after)?;
            continue;
        }
        // After DISTINCT or aggregation: a projected expression, or one
        // over projected names only.
        if p.items.iter().any(|(e, _)| e == o) || o.has_aggregate() || after.open {
            continue;
        }
        if let Some(v) = uncovered(o, &p.items, &after) {
            return Err(err(
                "UndefinedVariable",
                format!("Variable `{v}` not defined (the projection removed it)"),
            ));
        }
    }
    if let Some(w) = &p.where_ {
        no_aggregate(w, "WHERE")?;
        if keeps_old {
            expr(w, &after)?;
        }
    }
    for e in p.skip.iter().chain(p.limit.iter()) {
        expr(e, &Scope::default())?;
    }
    *s = next;
    Ok(())
}

/// A variable of `o` that is neither projected nor inside a sub-expression
/// equal to a projected one.
fn uncovered(o: &Expr, items: &[(Expr, String)], after: &Scope) -> Option<String> {
    if items.iter().any(|(e, _)| e == o) {
        return None;
    }
    let all = |v: &[&Expr]| v.iter().find_map(|x| uncovered(x, items, after));
    match o {
        Expr::Var(v) => (!after.vars.contains_key(v)).then(|| v.clone()),
        Expr::Prop(b, _) | Expr::Unary(_, b) | Expr::IsNull(b, _) | Expr::HasLabels(b, _) => {
            uncovered(b, items, after)
        }
        Expr::Binary(_, a, b) | Expr::Index(a, b) => all(&[a, b]),
        Expr::Func { args, .. } | Expr::List(args) => {
            args.iter().find_map(|x| uncovered(x, items, after))
        }
        // Other forms are left to run time.
        _ => None,
    }
}

fn nested_aggregate(e: &Expr) -> Result<()> {
    let mut bad = false;
    e.walk(&mut |x| {
        if let Expr::Func { name, args, .. } = x
            && is_aggregate(name)
            && args.iter().any(Expr::has_aggregate)
        {
            bad = true;
        }
    });
    if bad {
        return Err(err(
            "NestedAggregation",
            "Can't use aggregate functions inside of aggregate functions",
        ));
    }
    Ok(())
}

fn lit_not_bool(e: &Expr) -> bool {
    matches!(
        e,
        Expr::Lit(Value::Int(_) | Value::Float(_) | Value::String(_))
            | Expr::List(_)
            | Expr::Map(_)
    )
}

/// Checks names and argument types in `e` under `s`.
fn expr(e: &Expr, s: &Scope) -> Result<()> {
    match e {
        Expr::Var(v) => var(v, s),
        Expr::Lit(_) | Expr::Param(_) | Expr::CountStar | Expr::Slot(_) => Ok(()),
        Expr::Prop(b, _) => {
            if let Expr::Var(v) = &**b
                && s.kind(v) == Kind::Path
            {
                return Err(err(
                    "InvalidArgumentType",
                    format!("Type mismatch: `{v}` is a path and has no properties"),
                ));
            }
            expr(b, s)
        }
        Expr::Index(a, b) => {
            expr(a, s)?;
            expr(b, s)
        }
        Expr::Slice(a, b, c) => {
            expr(a, s)?;
            for x in b.iter().chain(c.iter()) {
                expr(x, s)?;
            }
            Ok(())
        }
        Expr::List(l) => l.iter().try_for_each(|x| expr(x, s)),
        Expr::Map(m) => m.iter().try_for_each(|(_, x)| expr(x, s)),
        Expr::Unary(op, x) => {
            if *op == UnOp::Not && lit_not_bool(x) {
                return Err(err("InvalidArgumentType", "NOT takes a boolean"));
            }
            expr(x, s)
        }
        Expr::Binary(op, a, b) => {
            if matches!(op, BinOp::And | BinOp::Or | BinOp::Xor)
                && (lit_not_bool(a) || lit_not_bool(b))
            {
                return Err(err("InvalidArgumentType", "AND, OR and XOR take booleans"));
            }
            expr(a, s)?;
            expr(b, s)
        }
        Expr::IsNull(x, _) | Expr::HasLabels(x, _) => expr(x, s),
        Expr::Func { name, args, .. } => {
            if let [Expr::Var(v)] = args.as_slice() {
                let k = s.kind(v);
                let bad = match name.as_str() {
                    "length" => matches!(k, Kind::Node | Kind::Rel),
                    "size" => k == Kind::Path,
                    "type" => k == Kind::Node,
                    _ => false,
                };
                if bad {
                    return Err(err(
                        "InvalidArgumentType",
                        format!("Type mismatch: {name}() of `{v}`"),
                    ));
                }
            }
            args.iter().try_for_each(|x| expr(x, s))
        }
        Expr::Case {
            subject,
            whens,
            else_,
        } => {
            for x in subject.iter().chain(else_.iter()) {
                expr(x, s)?;
            }
            for (a, b) in whens {
                expr(a, s)?;
                expr(b, s)?;
            }
            Ok(())
        }
        Expr::ListComp {
            var: v,
            list,
            pred,
            map,
        } => {
            expr(list, s)?;
            let mut inner = s.clone();
            inner.vars.insert(v.clone(), Kind::Any);
            for x in pred.iter().chain(map.iter()) {
                expr(x, &inner)?;
            }
            Ok(())
        }
        Expr::Quantifier {
            var: v, list, pred, ..
        } => {
            expr(list, s)?;
            let mut inner = s.clone();
            inner.vars.insert(v.clone(), Kind::Any);
            expr(pred, &inner)
        }
        Expr::Reduce {
            acc,
            init,
            var: v,
            list,
            expr: x,
        } => {
            expr(init, s)?;
            expr(list, s)?;
            let mut inner = s.clone();
            inner.vars.insert(acc.clone(), Kind::Any);
            inner.vars.insert(v.clone(), Kind::Any);
            expr(x, &inner)
        }
        Expr::Pattern(p) => {
            // A pattern predicate may not introduce names.
            let mut names: Vec<&String> = p.start.var.iter().collect();
            for (r, n) in &p.steps {
                names.extend(r.var.iter());
                names.extend(n.var.iter());
            }
            for n in names {
                var(n, s)?;
            }
            pattern_exprs(p, s)
        }
        Expr::MapProjection(b, items) => {
            expr(b, s)?;
            for i in items {
                match i {
                    MapItem::Lit(_, x) => expr(x, s)?,
                    MapItem::Var(v) => var(v, s)?,
                    MapItem::Prop(_) | MapItem::All => {}
                }
            }
            Ok(())
        }
        Expr::Subquery(_, q) => {
            let inner = Scope {
                vars: s.vars.clone(),
                open: true,
            };
            check_query(q, &inner)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::parser::parse;

    fn rule(q: &str) -> String {
        match parse(q) {
            Ok(_) => "ok".into(),
            Err(e) => e
                .message
                .split(": ")
                .next()
                .unwrap_or_default()
                .trim_start_matches("[Neo.ClientError.Statement.SyntaxError] ")
                .to_owned(),
        }
    }

    #[test]
    fn the_rules() {
        for (q, want) in [
            ("MATCH ()-[r]-() MATCH (r) RETURN r", "VariableTypeConflict"),
            ("WITH 1 AS n MATCH (n) RETURN n", "VariableTypeConflict"),
            (
                "MATCH (n) WITH [n] AS u MATCH (u)-->(m) RETURN m",
                "VariableTypeConflict",
            ),
            (
                "MATCH (p)-[]-() MATCH p = ()-[]-() RETURN p",
                "VariableAlreadyBound",
            ),
            ("MATCH p = (p)-[]-() RETURN p", "VariableAlreadyBound"),
            (
                "MATCH (a)-[r]->()-[r]->(a) RETURN r",
                "RelationshipUniquenessViolation",
            ),
            ("MATCH (a) CREATE (a)", "VariableAlreadyBound"),
            ("MATCH (a) CREATE (a {x: 1})", "VariableAlreadyBound"),
            (
                "MATCH ()-[r]->() CREATE ()-[r:R]->()",
                "VariableAlreadyBound",
            ),
            ("MATCH (a) RETURN foo", "UndefinedVariable"),
            ("MATCH (n) WHERE (n)-[r]->(a) RETURN n", "UndefinedVariable"),
            (
                "MATCH (a) WHERE count(a) > 10 RETURN a",
                "InvalidAggregation",
            ),
            (
                "MATCH (n) WITH n.a AS f ORDER BY max(n.b) RETURN f",
                "InvalidAggregation",
            ),
            ("RETURN 1 AND true", "InvalidArgumentType"),
            ("MATCH (n) RETURN length(n)", "InvalidArgumentType"),
            ("MATCH ()-[]-() DELETE 1 + 1", "InvalidArgumentType"),
            ("MATCH (n) RETURN (n)-[]->()", "UnexpectedSyntax"),
            // Valid statements stay valid.
            ("MATCH (a)-[r]->(b) MATCH (a)-[r]->(c) RETURN c", "ok"),
            ("MATCH (a) CREATE (a)-[:R]->(b) RETURN b", "ok"),
            ("MATCH (a) WITH a ORDER BY a.x RETURN a", "ok"),
            (
                "MATCH (a) RETURN a.x AS x, count(*) AS c ORDER BY c DESC, x",
                "ok",
            ),
            ("MATCH (a) WHERE (a)-->() RETURN [x IN [1] | x + 1]", "ok"),
            ("UNWIND [1, 2] AS x WITH x WHERE x > 1 RETURN x", "ok"),
            ("MATCH (a) RETURN DISTINCT a.name ORDER BY a.name", "ok"),
            (
                "MATCH (a) RETURN DISTINCT a.name ORDER BY a.age",
                "UndefinedVariable",
            ),
            ("MATCH (n) WHERE (n) RETURN n", "InvalidArgumentType"),
            ("CALL { MATCH (n) RETURN n } RETURN n", "ok"),
            ("MATCH (n) RETURN n {.a, b: 1}", "ok"),
            (
                "MATCH (a) WHERE EXISTS { MATCH (a)-->(b) WHERE b.x = 1 } RETURN a",
                "ok",
            ),
            (
                "MATCH p = (a)-[*]->(b) RETURN length(p), size(nodes(p))",
                "ok",
            ),
        ] {
            assert_eq!(rule(q), want, "{q}");
        }
    }
}
