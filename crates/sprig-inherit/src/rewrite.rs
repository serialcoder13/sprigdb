//! Statements reading inheritable objects (chapter 42, "The merged view";
//! build step 4): every reference to an inheritable object becomes the
//! session space's merged view, as a derived table under the name the
//! statement used, and `SPRIG_ORIGIN()` becomes the view's origin column.
//!
//! The statement is parsed with sqlparser-rs; each reference is replaced by
//! a placeholder name before printing and the placeholder by the view's SQL
//! after, so the generated SQL is never parsed again.

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use sprig_common::{Result, SprigError};
use sqlparser::ast::{
    Expr, Ident, ObjectName, ObjectNamePart, Query, SelectItem, Statement, TableAlias, TableFactor,
    VisitMut, VisitorMut,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::{Parser, ParserOptions};

use crate::grammar::{Name, unsupported};
use crate::plan::{Ctx, Resolved};
use crate::view::ViewSql;
use crate::{Handled, Runner, upstream};

/// Parses one statement.
///
/// # Errors
/// 39891 when sqlparser-rs can't read it.
pub fn parse_one(sql: &str) -> Result<Statement> {
    let mut stmts = Parser::new(&MySqlDialect {})
        .with_options(ParserOptions::new().with_unescape(false))
        .try_with_sql(sql)
        .and_then(|mut p| p.parse_statements())
        .map_err(|e| {
            unsupported(format!(
                "a statement on an inheritable object must parse: {e}"
            ))
        })?;
    if stmts.len() != 1 {
        return Err(unsupported(
            "one statement at a time on inheritable objects",
        ));
    }
    Ok(stmts.remove(0))
}

/// Parses an expression.
///
/// # Errors
/// 39891 when sqlparser-rs can't read it.
pub fn parse_expr(sql: &str) -> Result<Expr> {
    Parser::new(&MySqlDialect {})
        .with_options(ParserOptions::new().with_unescape(false))
        .try_with_sql(sql)
        .and_then(|mut p| p.parse_expr())
        .map_err(|e| unsupported(format!("an expression must parse: {e}")))
}

/// The parts of a name.
#[must_use]
pub fn name_parts(n: &ObjectName) -> Vec<String> {
    n.0.iter()
        .filter_map(|p| match p {
            ObjectNamePart::Identifier(i) => Some(i.value.clone()),
            ObjectNamePart::Function(_) => None,
        })
        .collect()
}

/// An `ObjectName` as a [`Name`].
#[must_use]
pub fn as_name(n: &ObjectName) -> Option<Name> {
    match name_parts(n).as_slice() {
        [t] => Some(Name {
            db: None,
            name: t.clone(),
        }),
        [d, t] => Some(Name {
            db: Some(d.clone()),
            name: t.clone(),
        }),
        _ => None,
    }
}

/// One replaced reference.
pub(crate) struct Ref {
    pub(crate) r: Resolved,
    pub(crate) alias: String,
    pub(crate) placeholder: String,
}

struct Replacer<'c, 'a> {
    ctx: &'c Ctx<'a>,
    ctes: BTreeSet<String>,
    refs: Vec<Ref>,
    error: Option<SprigError>,
}

impl VisitorMut for Replacer<'_, '_> {
    type Break = ();

    fn pre_visit_query(&mut self, q: &mut Query) -> ControlFlow<()> {
        if let Some(w) = &q.with {
            for c in &w.cte_tables {
                self.ctes.insert(c.alias.name.value.to_lowercase());
            }
        }
        ControlFlow::Continue(())
    }

    fn post_visit_table_factor(&mut self, tf: &mut TableFactor) -> ControlFlow<()> {
        let TableFactor::Table {
            name, alias, args, ..
        } = tf
        else {
            return ControlFlow::Continue(());
        };
        if args.is_some() {
            return ControlFlow::Continue(());
        }
        let Some(n) = as_name(name) else {
            return ControlFlow::Continue(());
        };
        if n.db.is_none() && self.ctes.contains(&n.name.to_lowercase()) {
            return ControlFlow::Continue(());
        }
        let r = match self.ctx.resolve(&n) {
            Ok(Some(r)) => r,
            Ok(None) => return ControlFlow::Continue(()),
            Err(e) => {
                self.error = Some(e);
                return ControlFlow::Break(());
            }
        };
        let alias_name = alias
            .as_ref()
            .map_or_else(|| n.name.clone(), |a| a.name.value.clone());
        let placeholder = format!("__sprig_ph{}x", self.refs.len());
        *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
            placeholder.clone(),
        ))]);
        if alias.is_none() {
            *alias = Some(TableAlias {
                explicit: true,
                name: Ident::with_quote('`', alias_name.clone()),
                columns: Vec::new(),
                at: None,
            });
        }
        self.refs.push(Ref {
            r,
            alias: alias_name,
            placeholder,
        });
        ControlFlow::Continue(())
    }
}

fn is_origin_call(e: &Expr) -> bool {
    matches!(e, Expr::Function(f)
        if f.name.to_string().eq_ignore_ascii_case("SPRIG_ORIGIN"))
}

struct Origin {
    alias: String,
}

impl VisitorMut for Origin {
    type Break = ();
    fn post_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<()> {
        if is_origin_call(e) {
            *e = Expr::CompoundIdentifier(vec![
                Ident::with_quote('`', self.alias.clone()),
                Ident::with_quote('`', "_sprig_o"),
            ]);
        }
        ControlFlow::Continue(())
    }
}

/// Names `SPRIG_ORIGIN()` select items after the call, as MariaDB would.
fn alias_origin_items(stmt: &mut Statement) {
    struct V;
    impl VisitorMut for V {
        type Break = ();
        fn pre_visit_query(&mut self, q: &mut Query) -> ControlFlow<()> {
            if let sqlparser::ast::SetExpr::Select(s) = q.body.as_mut() {
                for item in &mut s.projection {
                    if let SelectItem::UnnamedExpr(e) = item
                        && is_origin_call(e)
                    {
                        *item = SelectItem::ExprWithAlias {
                            expr: e.clone(),
                            alias: Ident::with_quote('`', "SPRIG_ORIGIN()"),
                        };
                    }
                }
            }
            ControlFlow::Continue(())
        }
    }
    let _ = stmt.visit(&mut V);
}

/// `*` and `alias.*` over the one inheritable reference, as its columns,
/// so the origin column the view carries for `SPRIG_ORIGIN()` stays out of
/// the result.
fn expand_wildcards(stmt: &mut Statement, rf: &Ref) {
    struct V<'a> {
        rf: &'a Ref,
    }
    impl VisitorMut for V<'_> {
        type Break = ();
        fn pre_visit_query(&mut self, q: &mut Query) -> ControlFlow<()> {
            let sqlparser::ast::SetExpr::Select(s) = q.body.as_mut() else {
                return ControlFlow::Continue(());
            };
            let single = s.from.len() == 1 && s.from[0].joins.is_empty();
            // A typed table's type columns are invisible, as in its table.
            let typed = self.rf.r.obj.type_root.is_some();
            let cols = |a: &str| -> Vec<SelectItem> {
                self.rf
                    .r
                    .obj
                    .columns
                    .iter()
                    .filter(|c| {
                        !(typed
                            && (c.eq_ignore_ascii_case("_type")
                                || c.eq_ignore_ascii_case("_type_path")))
                    })
                    .map(|c| {
                        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(vec![
                            Ident::with_quote('`', a.to_owned()),
                            Ident::with_quote('`', c.clone()),
                        ]))
                    })
                    .collect()
            };
            let mut out = Vec::with_capacity(s.projection.len());
            for item in std::mem::take(&mut s.projection) {
                match &item {
                    SelectItem::Wildcard(_) if single => out.extend(cols(&self.rf.alias)),
                    SelectItem::QualifiedWildcard(
                        sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(n),
                        _,
                    ) if name_parts(n)
                        .last()
                        .is_some_and(|x| x.eq_ignore_ascii_case(&self.rf.alias)) =>
                    {
                        out.extend(cols(&self.rf.alias));
                    }
                    _ => out.push(item),
                }
            }
            s.projection = out;
            ControlFlow::Continue(())
        }
    }
    let _ = stmt.visit(&mut V { rf });
}

/// The SQL of `r`'s merged view in the session's space.
///
/// # Errors
/// The chain's.
pub(crate) fn view_of(
    ctx: &Ctx<'_>,
    r: &Resolved,
    origin: bool,
    keys: Option<Vec<String>>,
) -> Result<String> {
    let chain = ctx.chain(&r.obj, ctx.space, &r.prefix)?;
    Ok(ViewSql {
        o: &r.obj,
        layer: r.layer.clone(),
        keys,
        origin,
        derived: r.derived.clone(),
    }
    .view(&chain))
}

fn is_literal(e: &Expr) -> bool {
    match e {
        Expr::Value(_) => true,
        Expr::UnaryOp { expr, .. } => matches!(expr.as_ref(), Expr::Value(_)),
        _ => false,
    }
}

fn names_column(e: &Expr, alias: &str, col: &str) -> bool {
    match e {
        Expr::Identifier(i) => i.value.eq_ignore_ascii_case(col),
        Expr::CompoundIdentifier(p) => {
            p.len() == 2
                && p[0].value.eq_ignore_ascii_case(alias)
                && p[1].value.eq_ignore_ascii_case(col)
        }
        _ => false,
    }
}

/// Literal key values a single-table `SELECT` restricts the reference to
/// (`key = v` or `key IN (v, ...)` among the top-level `AND`s), pushed
/// into every level of the view (chapter 42, "Key predicates"). MariaDB
/// pushes the outer predicate into the `UNION` branches itself, but not
/// into the `NOT EXISTS` branch's reference to the parent level.
fn pushed_keys(stmt: &Statement, rf: &Ref) -> Option<Vec<String>> {
    let [key] = rf.r.obj.key.as_slice() else {
        return None;
    };
    let q = match stmt {
        Statement::Query(q) => q,
        Statement::Explain { statement, .. } => match statement.as_ref() {
            Statement::Query(q) => q,
            _ => return None,
        },
        _ => return None,
    };
    let sqlparser::ast::SetExpr::Select(s) = q.body.as_ref() else {
        return None;
    };
    if s.from.len() != 1 || !s.from[0].joins.is_empty() {
        return None;
    }
    match &s.from[0].relation {
        TableFactor::Table { name, .. }
            if name_parts(name).last().map(String::as_str) == Some(&rf.placeholder) => {}
        _ => return None,
    }
    let mut stack = vec![s.selection.as_ref()?];
    while let Some(e) = stack.pop() {
        match e {
            Expr::BinaryOp {
                left,
                op: sqlparser::ast::BinaryOperator::And,
                right,
            } => {
                stack.push(left);
                stack.push(right);
            }
            Expr::Nested(inner) => stack.push(inner),
            Expr::BinaryOp {
                left,
                op: sqlparser::ast::BinaryOperator::Eq,
                right,
            } => {
                if names_column(left, &rf.alias, key) && is_literal(right) {
                    return Some(vec![right.to_string()]);
                }
                if names_column(right, &rf.alias, key) && is_literal(left) {
                    return Some(vec![left.to_string()]);
                }
            }
            Expr::InList {
                expr,
                list,
                negated: false,
            } if names_column(expr, &rf.alias, key)
                && !list.is_empty()
                && list.iter().all(is_literal) =>
            {
                return Some(list.iter().map(ToString::to_string).collect());
            }
            _ => {}
        }
    }
    None
}

/// Replaces every reference to an inheritable object in `stmt` by its
/// view; `None` when there is none.
///
/// # Errors
/// 39891 for `SPRIG_ORIGIN()` without exactly one inheritable reference;
/// the chain's and the share views'.
pub(crate) async fn replace_refs(
    stmt: &mut Statement,
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
) -> Result<Option<String>> {
    let origin = stmt.to_string().to_uppercase().contains("SPRIG_ORIGIN");
    if origin {
        alias_origin_items(stmt);
    }
    let mut v = Replacer {
        ctx,
        ctes: BTreeSet::new(),
        refs: Vec::new(),
        error: None,
    };
    let _ = stmt.visit(&mut v);
    if let Some(e) = v.error {
        return Err(e);
    }
    if v.refs.is_empty() {
        return Ok(None);
    }
    if origin {
        if v.refs.len() != 1 {
            return Err(unsupported(
                "SPRIG_ORIGIN() needs exactly one inheritable object in the statement",
            ));
        }
        let _ = stmt.visit(&mut Origin {
            alias: v.refs[0].alias.clone(),
        });
        expand_wildcards(stmt, &v.refs[0]);
    }
    let mut text = stmt.to_string();
    for rf in v.refs.iter().rev() {
        upstream::ensure(ctx, r, &rf.r).await?;
        let view = view_of(ctx, &rf.r, origin, pushed_keys(stmt, rf))?;
        text = text.replace(&rf.placeholder, &format!("({view})"));
    }
    metrics::counter!("sprig_inherit_reads_total", "mode" => "read_through")
        .increment(v.refs.len() as u64);
    Ok(Some(text))
}

/// A statement reading inheritable objects.
///
/// # Errors
/// See [`replace_refs`].
pub async fn query(sql: &str, ctx: &Ctx<'_>, r: &mut dyn Runner) -> Result<Handled> {
    let mut stmt = parse_one(sql)?;
    match replace_refs(&mut stmt, ctx, r).await? {
        Some(text) => Ok(Handled::Forward(text)),
        None => Ok(Handled::NotInherit),
    }
}

/// Replaces references inside one expression (a write's `WHERE` or `SET`
/// value), returning its text.
///
/// # Errors
/// See [`replace_refs`].
pub(crate) async fn expr_text(e: &Expr, ctx: &Ctx<'_>, r: &mut dyn Runner) -> Result<String> {
    // Wrap the expression in a statement so the same visitor applies.
    let mut stmt = parse_one(&format!("SELECT {e}"))?;
    let Some(text) = replace_refs(&mut stmt, ctx, r).await? else {
        return Ok(e.to_string());
    };
    Ok(text
        .strip_prefix("SELECT ")
        .map_or_else(|| e.to_string(), str::to_owned))
}
