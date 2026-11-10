//! `WITH ROLLUP` with `ORDER BY` and `GROUPING()` on port 3306 (chapter
//! 28, "`GROUPING()` with `WITH ROLLUP`", build step 9).
//!
//! MariaDB 11.8 refuses `ORDER BY` after `WITH ROLLUP` (1221) and has no
//! `GROUPING()` (MDEV-22269, 1305). MySQL 8.4 has both. Two rewrites:
//!
//! - **`ORDER BY` only:** the grouped query runs as a derived table and is
//!   ordered (and limited) outside it, by the output columns:
//!
//!   ```text
//!   SELECT `_sprig_c0` AS `a`, `_sprig_c1` AS `SUM(c)` FROM (
//!     SELECT a AS `_sprig_c0`, SUM(c) AS `_sprig_c1` FROM t GROUP BY a WITH ROLLUP
//!   ) AS `_sprig_r` ORDER BY `_sprig_c0` DESC
//!   ```
//!
//! - **`GROUPING()`:** one branch per rollup level, from all `n` grouped
//!   expressions down to none, joined with `UNION ALL`. In branch `L` the
//!   query is grouped by the first `L` expressions; the others read `NULL`
//!   outside aggregates, `GROUPING(g)` is 1 for them and 0 otherwise
//!   (several arguments make a bit mask, the last argument the lowest bit,
//!   as in MySQL), and hidden columns carry each level's flags and keys.
//!   Without an `ORDER BY` of its own, the result is ordered as MySQL
//!   outputs a rollup: `grp1, g1, grp2, g2, ...`, so each subtotal follows
//!   its group and the grand total comes last. The level with no grouped
//!   expression has `HAVING COUNT(*) > 0`, so an empty table gives no row,
//!   as in MySQL.
//!
//! Each output column keeps the name MySQL would give it: its alias, its
//! column name, or the expression's text as the client wrote it. An
//! `ORDER BY` item must name an output column (by alias, name, position or
//! the same expression); otherwise the statement is left as it is and
//! MariaDB's error stands. Statements with `*` in the select list, more
//! grouped expressions than `translator.rollup_max_columns`, or a shape
//! the parser doesn't read are left alone too.

use std::ops::ControlFlow;

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, GroupByWithModifier,
    OrderByKind, Query, Select, SelectItem, SetExpr, Spanned, Statement, Value, VisitMut,
    VisitorMut,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

/// `translator.rollup_max_columns`' default.
pub const DEFAULT_MAX_COLUMNS: usize = 8;

/// The rewritten statement, or `None` when it needs none (or can't have
/// one; see the module docs).
#[must_use]
pub fn rewrite(sql: &[u8], max_columns: usize) -> Option<String> {
    if !sql.windows(6).any(|w| w.eq_ignore_ascii_case(b"ROLLUP")) {
        return None;
    }
    let text = std::str::from_utf8(sql).ok()?;
    let text = text.trim_end_matches(|c: char| c == ';' || c.is_whitespace());
    let mut stmts = Parser::new(&MySqlDialect {})
        .with_recursion_limit(64)
        .try_with_sql(text)
        .ok()?
        .parse_statements()
        .ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let Statement::Query(q) = stmts.pop()? else {
        return None;
    };
    let SetExpr::Select(sel) = q.body.as_ref() else {
        return None;
    };
    let GroupByExpr::Expressions(keys, mods) = &sel.group_by else {
        return None;
    };
    if !mods.contains(&GroupByWithModifier::Rollup) || keys.is_empty() || keys.len() > max_columns {
        return None;
    }
    let grouping = has_grouping(&q);
    if !grouping && q.order_by.is_none() {
        return None;
    }
    // The levels repeat the query: a `?` would be bound once per level.
    if grouping && has_placeholder(&q) {
        return None;
    }
    let names = output_names(text, sel)?;
    let order = order_items(&q, sel, &names)?;
    let outer_cols = names
        .iter()
        .enumerate()
        .map(|(i, n)| format!("`_sprig_c{i}` AS {}", quote(n)))
        .collect::<Vec<_>>()
        .join(", ");
    let with = q.with.as_ref().map(|w| format!("{w} ")).unwrap_or_default();
    let limit = q
        .limit_clause
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();
    let inner = if grouping {
        let mut branches = Vec::with_capacity(keys.len() + 1);
        for level in (0..=keys.len()).rev() {
            branches.push(branch(sel, keys, level)?);
        }
        branches.join(" UNION ALL ")
    } else {
        let mut s = sel.as_ref().clone();
        alias_items(&mut s);
        s.to_string()
    };
    let order = match order {
        Some(o) => o,
        None if grouping => (1..=keys.len())
            .map(|j| format!("`_sprig_g{j}`, `_sprig_k{j}`"))
            .collect::<Vec<_>>()
            .join(", "),
        None => String::new(),
    };
    let order = if order.is_empty() {
        String::new()
    } else {
        format!(" ORDER BY {order}")
    };
    Some(format!(
        "{with}SELECT {outer_cols} FROM ({inner}) AS `_sprig_r`{order}{limit}"
    ))
}

fn quote(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn fn_name(f: &sqlparser::ast::Function) -> String {
    f.name
        .0
        .last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.to_ascii_uppercase())
        .unwrap_or_default()
}

fn has_grouping(q: &Query) -> bool {
    struct Find(bool);
    impl sqlparser::ast::Visitor for Find {
        type Break = ();
        fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
            if let Expr::Function(f) = e
                && fn_name(f) == "GROUPING"
            {
                self.0 = true;
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }
    let mut v = Find(false);
    let _ = sqlparser::ast::Visit::visit(q, &mut v);
    v.0
}

fn has_placeholder(q: &Query) -> bool {
    struct Find(bool);
    impl sqlparser::ast::Visitor for Find {
        type Break = ();
        fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
            if let Expr::Value(v) = e
                && matches!(v.value, Value::Placeholder(_))
            {
                self.0 = true;
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }
    let mut v = Find(false);
    let _ = sqlparser::ast::Visit::visit(q, &mut v);
    v.0
}

/// The text of `e` as written in `text`, from its span.
fn source(text: &str, e: &Expr) -> Option<String> {
    let sp = e.span();
    let off = |line: u64, col: u64| -> Option<usize> {
        let mut l = 1u64;
        let mut c = 1u64;
        for (i, ch) in text.char_indices() {
            if l == line && c == col {
                return Some(i);
            }
            if ch == '\n' {
                l += 1;
                c = 1;
            } else {
                c += 1;
            }
        }
        (l == line && c == col).then_some(text.len())
    };
    let a = off(sp.start.line, sp.start.column)?;
    let mut b = off(sp.end.line, sp.end.column)?;
    if a >= b {
        return None;
    }
    // A call's span can end before its `)`: close what it opened.
    let open = |t: &str| {
        t.bytes().filter(|c| *c == b'(').count() as isize
            - t.bytes().filter(|c| *c == b')').count() as isize
    };
    let mut depth = open(&text[a..b]);
    while depth > 0 {
        let rest = &text[b..];
        let ch = rest.chars().next()?;
        if ch == ')' {
            depth -= 1;
        } else if !ch.is_whitespace() {
            return None;
        }
        b += ch.len_utf8();
    }
    Some(text[a..b].to_owned())
}

/// The names MySQL gives the select list's columns.
fn output_names(text: &str, sel: &Select) -> Option<Vec<String>> {
    sel.projection
        .iter()
        .map(|it| match it {
            SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
            SelectItem::UnnamedExpr(Expr::Identifier(i)) => Some(i.value.clone()),
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(p)) => {
                p.last().map(|i| i.value.clone())
            }
            SelectItem::UnnamedExpr(e) => Some(source(text, e).unwrap_or_else(|| e.to_string())),
            _ => None,
        })
        .collect()
}

fn item_expr(it: &SelectItem) -> Option<&Expr> {
    match it {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => Some(e),
        _ => None,
    }
}

/// The client's `ORDER BY` over the output columns: `Some(None)` when
/// there is none, `None` when an item names no output column.
fn order_items(q: &Query, sel: &Select, names: &[String]) -> Option<Option<String>> {
    let Some(ob) = &q.order_by else {
        return Some(None);
    };
    let OrderByKind::Expressions(items) = &ob.kind else {
        return None;
    };
    let mut out = Vec::with_capacity(items.len());
    for o in items {
        let i = column_of(&o.expr, sel, names)?;
        let mut s = format!("`_sprig_c{i}`");
        let opts = o.options.to_string();
        if !opts.is_empty() {
            s.push_str(&opts);
        }
        out.push(s);
    }
    Some(Some(out.join(", ")))
}

fn column_of(e: &Expr, sel: &Select, names: &[String]) -> Option<usize> {
    if let Expr::Value(v) = e
        && let Value::Number(n, _) = &v.value
    {
        let k: usize = n.parse().ok()?;
        return (1..=names.len()).contains(&k).then(|| k - 1);
    }
    let shown = e.to_string();
    if let Some(i) = sel
        .projection
        .iter()
        .position(|it| item_expr(it).is_some_and(|x| x.to_string().eq_ignore_ascii_case(&shown)))
    {
        return Some(i);
    }
    let last = match e {
        Expr::Identifier(i) => &i.value,
        Expr::CompoundIdentifier(p) => &p.last()?.value,
        _ => return None,
    };
    let hits: Vec<usize> = names
        .iter()
        .enumerate()
        .filter(|(_, n)| n.eq_ignore_ascii_case(last))
        .map(|(i, _)| i)
        .collect();
    match hits.as_slice() {
        [i] => Some(*i),
        _ => None,
    }
}

/// Gives every select item the alias `_sprig_c<i>`.
fn alias_items(s: &mut Select) {
    for (i, it) in s.projection.iter_mut().enumerate() {
        let e = match it {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e.clone(),
            _ => continue,
        };
        *it = SelectItem::ExprWithAlias {
            expr: e,
            alias: sqlparser::ast::Ident::with_quote('`', format!("_sprig_c{i}")),
        };
    }
}

const AGGREGATES: &[&str] = &[
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "GROUP_CONCAT",
    "BIT_AND",
    "BIT_OR",
    "BIT_XOR",
    "STD",
    "STDDEV",
    "STDDEV_POP",
    "STDDEV_SAMP",
    "VAR_POP",
    "VAR_SAMP",
    "VARIANCE",
    "JSON_ARRAYAGG",
    "JSON_OBJECTAGG",
];

/// Replaces, outside aggregates, the keys rolled up at this level with
/// `NULL` and `GROUPING(..)` with its value.
struct Level<'a> {
    keys: &'a [String],
    level: usize,
    in_aggregate: usize,
    failed: bool,
}

impl Level<'_> {
    fn key(&self, e: &Expr) -> Option<usize> {
        let s = e.to_string();
        self.keys.iter().position(|k| k.eq_ignore_ascii_case(&s))
    }
}

impl VisitorMut for Level<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<()> {
        if let Expr::Function(f) = e {
            let name = fn_name(f);
            if name == "GROUPING" {
                let FunctionArguments::List(l) = &f.args else {
                    self.failed = true;
                    return ControlFlow::Break(());
                };
                let mut v: u64 = 0;
                for a in &l.args {
                    let FunctionArg::Unnamed(FunctionArgExpr::Expr(x)) = a else {
                        self.failed = true;
                        return ControlFlow::Break(());
                    };
                    let Some(j) = self.key(x) else {
                        // MySQL: 3580, an argument that isn't grouped.
                        self.failed = true;
                        return ControlFlow::Break(());
                    };
                    v = (v << 1) | u64::from(j >= self.level);
                }
                *e = Expr::value(Value::Number(v.to_string(), false));
                return ControlFlow::Continue(());
            }
            if AGGREGATES.contains(&name.as_str()) || f.over.is_some() {
                self.in_aggregate += 1;
            }
            return ControlFlow::Continue(());
        }
        if self.in_aggregate == 0
            && let Some(j) = self.key(e)
            && j >= self.level
        {
            *e = Expr::value(Value::Null);
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<()> {
        if let Expr::Function(f) = e {
            let name = fn_name(f);
            if AGGREGATES.contains(&name.as_str()) || f.over.is_some() {
                self.in_aggregate = self.in_aggregate.saturating_sub(1);
            }
        }
        ControlFlow::Continue(())
    }
}

/// The branch of rollup level `level` (grouped by the first `level` keys).
fn branch(sel: &Select, keys: &[Expr], level: usize) -> Option<String> {
    let shown: Vec<String> = keys.iter().map(ToString::to_string).collect();
    let mut s = sel.clone();
    s.group_by = GroupByExpr::Expressions(keys[..level].to_vec(), Vec::new());
    let mut v = Level {
        keys: &shown,
        level,
        in_aggregate: 0,
        failed: false,
    };
    for it in &mut s.projection {
        let _ = it.visit(&mut v);
        if v.failed {
            return None;
        }
    }
    if let Some(h) = &mut s.having {
        let _ = h.visit(&mut v);
        if v.failed {
            return None;
        }
    }
    if level == 0 {
        // An empty input gives no row, as MySQL's rollup.
        let nonempty = Parser::new(&MySqlDialect {})
            .try_with_sql("COUNT(*) > 0")
            .ok()?
            .parse_expr()
            .ok()?;
        s.having = Some(match s.having.take() {
            Some(h) => Expr::BinaryOp {
                left: Box::new(Expr::Nested(Box::new(h))),
                op: sqlparser::ast::BinaryOperator::And,
                right: Box::new(nonempty),
            },
            None => nonempty,
        });
    }
    alias_items(&mut s);
    for (j, k) in keys.iter().enumerate() {
        let rolled = j >= level;
        s.projection.push(SelectItem::ExprWithAlias {
            expr: Expr::value(Value::Number(u8::from(rolled).to_string(), false)),
            alias: sqlparser::ast::Ident::with_quote('`', format!("_sprig_g{}", j + 1)),
        });
        s.projection.push(SelectItem::ExprWithAlias {
            expr: if rolled {
                Expr::value(Value::Null)
            } else {
                k.clone()
            },
            alias: sqlparser::ast::Ident::with_quote('`', format!("_sprig_k{}", j + 1)),
        });
    }
    Some(s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rw(sql: &str) -> Option<String> {
        rewrite(sql.as_bytes(), DEFAULT_MAX_COLUMNS)
    }

    #[test]
    fn order_by_wraps_the_rollup() {
        assert_eq!(
            rw("SELECT a, SUM(c) FROM t GROUP BY a WITH ROLLUP ORDER BY a DESC").unwrap(),
            "SELECT `_sprig_c0` AS `a`, `_sprig_c1` AS `SUM(c)` FROM (SELECT a AS `_sprig_c0`, \
             SUM(c) AS `_sprig_c1` FROM t GROUP BY a WITH ROLLUP) AS `_sprig_r` \
             ORDER BY `_sprig_c0` DESC"
        );
        let s =
            rw("SELECT a, b, SUM(c) AS s FROM t GROUP BY a, b WITH ROLLUP ORDER BY s DESC LIMIT 3")
                .unwrap();
        assert!(s.ends_with("ORDER BY `_sprig_c2` DESC LIMIT 3"), "{s}");
        // By position and by the same expression.
        assert!(
            rw("SELECT a, sum( c ) FROM t GROUP BY a WITH ROLLUP ORDER BY 2, SUM(c)")
                .unwrap()
                .contains("`_sprig_c1` AS `sum( c )`")
        );
    }

    #[test]
    fn grouping_becomes_levels() {
        let s = rw("SELECT a, GROUPING(a), SUM(c) FROM t GROUP BY a WITH ROLLUP").unwrap();
        assert_eq!(
            s,
            "SELECT `_sprig_c0` AS `a`, `_sprig_c1` AS `GROUPING(a)`, `_sprig_c2` AS `SUM(c)` \
             FROM (SELECT a AS `_sprig_c0`, 0 AS `_sprig_c1`, SUM(c) AS `_sprig_c2`, \
             0 AS `_sprig_g1`, a AS `_sprig_k1` FROM t GROUP BY a UNION ALL \
             SELECT NULL AS `_sprig_c0`, 1 AS `_sprig_c1`, SUM(c) AS `_sprig_c2`, \
             1 AS `_sprig_g1`, NULL AS `_sprig_k1` FROM t HAVING COUNT(*) > 0) AS `_sprig_r` \
             ORDER BY `_sprig_g1`, `_sprig_k1`"
        );
        let s = rw("SELECT a, b, GROUPING(a, b), SUM(a) FROM t GROUP BY a, b WITH ROLLUP").unwrap();
        // Level 1: b rolled up, a kept; aggregates keep their columns.
        assert!(
            s.contains("SELECT a AS `_sprig_c0`, NULL AS `_sprig_c1`, 1 AS `_sprig_c2`, SUM(a) AS `_sprig_c3`"),
            "{s}"
        );
        assert!(
            s.contains("NULL AS `_sprig_c0`, NULL AS `_sprig_c1`, 3 AS `_sprig_c2`"),
            "{s}"
        );
    }

    #[test]
    fn other_statements_are_left_alone() {
        for s in [
            "SELECT a, SUM(c) FROM t GROUP BY a WITH ROLLUP",
            "SELECT a, SUM(c) FROM t GROUP BY a ORDER BY a",
            "SELECT * FROM t GROUP BY a WITH ROLLUP ORDER BY a",
            "SELECT a, SUM(c) FROM t GROUP BY a WITH ROLLUP ORDER BY c",
            "SELECT 'rollup'",
            "SELECT a, GROUPING(a) FROM t WHERE b = ? GROUP BY a WITH ROLLUP",
        ] {
            assert!(rw(s).is_none(), "{s}");
        }
    }
}
