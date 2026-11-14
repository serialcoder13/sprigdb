//! `LATERAL` derived tables on port 3306.
//!
//! MariaDB 11.8 has no `LATERAL` (MDEV-19078): every such statement fails
//! with 1064. Two shapes, the ones applications write, become plain SQL
//! MariaDB runs, with MySQL 8.4's results:
//!
//! - **Aggregates.** `LATERAL (SELECT SUM(o.amt) AS total FROM o WHERE
//!   o.cid = c.id) AS l`: the derived table has exactly one row per outer
//!   row, so the join changes nothing but adds columns. It goes, and each
//!   reference `l.total` becomes the correlated scalar subquery
//!   `(SELECT SUM(o.amt) FROM o WHERE o.cid = c.id)`, which MariaDB runs;
//!   `COUNT` of no rows stays 0, as in MySQL.
//! - **Rows, optionally the top N.** `LATERAL (SELECT o.amt FROM o WHERE
//!   o.cid = c.id [AND p] ORDER BY o.amt DESC LIMIT 2) AS l`: the derived
//!   table is computed for all outer keys at once, numbered per key with
//!   `ROW_NUMBER() OVER (PARTITION BY o.cid ORDER BY o.amt DESC)`, and
//!   joined on the key and the number:
//!
//!   ```text
//!   (SELECT o.amt, o.cid AS `_lat_k0`, ROW_NUMBER() OVER (...) AS `_lat_rn`
//!      FROM o) AS l ... ON l.`_lat_k0` = c.id AND l.`_lat_rn` <= 2
//!   ```
//!
//!   The conditions go into the join's `ON` (`JOIN`, `LEFT JOIN ... ON
//!   TRUE`) or, for a comma join, the `WHERE` clause.
//!
//! The correlation must be equalities between an outer column (qualified
//! by an outer table's name) and an expression of the derived table's own
//! tables, `AND`ed with conditions on its own tables only. Anything else
//! (another correlation, a `GROUP BY`, `DISTINCT`, `HAVING` or `UNION`
//! inside, `*` or `l.*` outside, an unqualified outer reference to a
//! lateral column, `ON` conditions other than `TRUE` around aggregates)
//! is left as it is, for the gateway's apply ([`ApplyPlan`]), which runs the
//! derived table per batch of outer rows. The rewrite is plain SQL, so stored
//! bodies use it too ([`super::stored`]).

use std::fmt::Write as _;

use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArguments, LimitClause, Query, Select, SelectItem, SetExpr,
    Statement, TableFactor, Value,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use crate::lex::{self, LexOptions, Token, TokenKind};

/// Aggregate functions: a lateral whose items are all aggregates has one
/// row per outer row.
const AGGREGATES: &[&str] = &[
    "COUNT",
    "SUM",
    "MIN",
    "MAX",
    "AVG",
    "GROUP_CONCAT",
    "BIT_AND",
    "BIT_OR",
    "BIT_XOR",
    "STD",
    "STDDEV",
    "STDDEV_POP",
    "STDDEV_SAMP",
    "VARIANCE",
    "VAR_POP",
    "VAR_SAMP",
    "JSON_ARRAYAGG",
    "JSON_OBJECTAGG",
];

/// Words that end a table reference's `ON` condition at depth 0.
const CLAUSE_END: &[&[u8]] = &[
    b"WHERE",
    b"GROUP",
    b"HAVING",
    b"ORDER",
    b"LIMIT",
    b"WINDOW",
    b"UNION",
    b"EXCEPT",
    b"INTERSECT",
    b"FOR",
    b"INTO",
    b"JOIN",
    b"INNER",
    b"CROSS",
    b"LEFT",
    b"RIGHT",
    b"STRAIGHT_JOIN",
    b"NATURAL",
    b"LOCK",
];

fn quote(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

struct Toks<'a> {
    sql: &'a [u8],
    t: Vec<Token>,
}

impl Toks<'_> {
    fn word(&self, j: usize, w: &[u8]) -> bool {
        self.t.get(j).is_some_and(|tk| {
            tk.kind == TokenKind::Word && tk.bytes(self.sql).eq_ignore_ascii_case(w)
        })
    }

    fn punct(&self, j: usize, c: u8) -> bool {
        self.t.get(j).is_some_and(|tk| {
            matches!(tk.kind, TokenKind::Punct | TokenKind::Op) && tk.bytes(self.sql) == [c]
        })
    }

    fn ident(&self, j: usize) -> Option<String> {
        self.t
            .get(j)
            .filter(|tk| tk.kind.is_ident_like())
            .map(|tk| String::from_utf8_lossy(&lex::ident_value(tk.bytes(self.sql))).into_owned())
    }

    fn matching(&self, from: usize) -> Option<usize> {
        let mut d = 0;
        for k in from..self.t.len() {
            if self.punct(k, b'(') {
                d += 1;
            } else if self.punct(k, b')') {
                d -= 1;
                if d == 0 {
                    return Some(k);
                }
            }
        }
        None
    }

    fn start(&self, j: usize) -> usize {
        self.t[j].start as usize
    }

    fn end(&self, j: usize) -> usize {
        self.t[j].end as usize
    }
}

/// One `LATERAL` table reference, by token index.
struct Lateral {
    /// The first token of the join (`,` or the first join keyword).
    join_start: usize,
    /// The join is a comma or `[INNER | CROSS] JOIN`, or `LEFT [OUTER] JOIN`.
    comma: bool,
    left: bool,
    /// `LATERAL`, `(`, `)`.
    kw: usize,
    open: usize,
    close: usize,
    alias: String,
    /// The last token of the alias.
    alias_end: usize,
    /// `ON`, and the last token of its condition.
    on: Option<(usize, usize)>,
}

/// The rewritten statement, or `None` (see the module docs).
#[must_use]
pub fn rewrite(sql: &[u8], opts: &LexOptions) -> Option<Vec<u8>> {
    if !sql.windows(7).any(|w| w.eq_ignore_ascii_case(b"LATERAL")) {
        return None;
    }
    let tokens = lex::lex(sql, opts).ok()?;
    let tk = Toks {
        sql,
        t: tokens.into_iter().filter(|t| !t.kind.is_trivia()).collect(),
    };
    if !(tk.word(0, b"SELECT") || tk.word(0, b"WITH") || tk.punct(0, b'(')) {
        return None;
    }
    // One lateral per statement: the next pass handles the next.
    let l = find_lateral(&tk)?;
    let inner = std::str::from_utf8(&sql[tk.end(l.open)..tk.start(l.close)]).ok()?;
    let q = parse_query(inner)?;
    let sel = plain_select(&q)?;
    let inner_tables = table_names(sel)?;
    let (keys, rest) = correlation(sel.selection.as_ref(), &inner_tables)?;
    if keys.is_empty() {
        return None;
    }
    // No `*` or `l.*` outside: the hidden columns would show.
    for j in 0..tk.t.len() {
        if (j < l.kw || j > l.alias_end)
            && tk.punct(j, b'*')
            && j > 0
            && (tk.punct(j - 1, b',')
                || tk.punct(j - 1, b'.')
                || tk.word(j - 1, b"SELECT")
                || tk.word(j - 1, b"DISTINCT"))
        {
            return None;
        }
    }
    let from = from_text(sel);
    let out = if all_aggregates(sel) {
        aggregate_rewrite(&tk, &l, sel, &from, &q)?
    } else {
        rows_rewrite(&tk, &l, sel, &q, &from, &keys, rest.as_ref())?
    };
    // Laterals left (several in one statement): one more pass each.
    Some(rewrite(&out, opts).unwrap_or(out))
}

fn parse_query(text: &str) -> Option<Query> {
    let stmts = Parser::new(&MySqlDialect {})
        .with_recursion_limit(64)
        .try_with_sql(text)
        .ok()?
        .parse_statements()
        .ok()?;
    match stmts.into_iter().next()? {
        Statement::Query(q) => Some(*q),
        _ => None,
    }
}

fn plain_select(q: &Query) -> Option<&Select> {
    if q.with.is_some() || q.fetch.is_some() || !q.locks.is_empty() {
        return None;
    }
    let SetExpr::Select(sel) = q.body.as_ref() else {
        return None;
    };
    let grouped = match &sel.group_by {
        sqlparser::ast::GroupByExpr::Expressions(e, m) => !e.is_empty() || !m.is_empty(),
        sqlparser::ast::GroupByExpr::All(_) => true,
    };
    if grouped
        || sel.distinct.is_some()
        || sel.having.is_some()
        || !sel.named_window.is_empty()
        || sel.from.is_empty()
        || sel.into.is_some()
    {
        return None;
    }
    Some(sel)
}

/// The names the derived table's own tables go by (aliases, else names).
fn table_names(sel: &Select) -> Option<Vec<String>> {
    let mut out = Vec::new();
    let mut add = |f: &TableFactor| -> Option<()> {
        match f {
            TableFactor::Table { name, alias, .. } => {
                out.push(match alias {
                    Some(a) => a.name.value.to_lowercase(),
                    None => name.0.last()?.as_ident()?.value.to_lowercase(),
                });
                Some(())
            }
            _ => None,
        }
    };
    for twj in &sel.from {
        add(&twj.relation)?;
        for j in &twj.joins {
            add(&j.relation)?;
        }
    }
    Some(out)
}

/// The qualifiers of column references in `e` (lower case), and whether
/// it has an unqualified column.
fn qualifiers(e: &Expr, out: &mut Vec<String>) {
    use sqlparser::ast::Visit;
    use std::ops::ControlFlow;
    struct V<'a>(&'a mut Vec<String>);
    impl sqlparser::ast::Visitor for V<'_> {
        type Break = ();
        fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
            if let Expr::CompoundIdentifier(parts) = e
                && parts.len() >= 2
            {
                self.0.push(parts[parts.len() - 2].value.to_lowercase());
            }
            ControlFlow::Continue(())
        }
    }
    let _ = e.visit(&mut V(out));
}

fn is_outer(e: &Expr, inner: &[String]) -> bool {
    let mut q = Vec::new();
    qualifiers(e, &mut q);
    q.iter().any(|x| !inner.contains(x))
}

/// Splits `WHERE` into the correlation equalities `(inner expr, outer
/// expr)` and the rest. `None` when an outer reference appears elsewhere.
/// The correlation equalities `(inner, outer)` and the other conditions.
type Correlation = (Vec<(Expr, Expr)>, Option<Expr>);

fn correlation(where_: Option<&Expr>, inner: &[String]) -> Option<Correlation> {
    let mut conj = Vec::new();
    if let Some(w) = where_ {
        split_and(w, &mut conj);
    }
    let mut keys = Vec::new();
    let mut rest: Vec<Expr> = Vec::new();
    for c in conj {
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } = &c
        {
            let lo = matches!(left.as_ref(), Expr::CompoundIdentifier(_)) && is_outer(left, inner);
            let ro =
                matches!(right.as_ref(), Expr::CompoundIdentifier(_)) && is_outer(right, inner);
            if lo && !ro && !is_outer(right, inner) {
                keys.push(((**right).clone(), (**left).clone()));
                continue;
            }
            if ro && !lo && !is_outer(left, inner) {
                keys.push(((**left).clone(), (**right).clone()));
                continue;
            }
        }
        if is_outer(&c, inner) {
            return None;
        }
        rest.push(c);
    }
    let rest = rest.into_iter().reduce(|a, b| Expr::BinaryOp {
        left: Box::new(a),
        op: BinaryOperator::And,
        right: Box::new(b),
    });
    Some((keys, rest))
}

fn split_and(e: &Expr, out: &mut Vec<Expr>) {
    match e {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            split_and(left, out);
            split_and(right, out);
        }
        Expr::Nested(inner)
            if matches!(
                inner.as_ref(),
                Expr::BinaryOp {
                    op: BinaryOperator::And,
                    ..
                }
            ) =>
        {
            split_and(inner, out);
        }
        _ => out.push(e.clone()),
    }
}

fn item_expr(it: &SelectItem) -> Option<(&Expr, Option<String>)> {
    match it {
        SelectItem::UnnamedExpr(e) => Some((e, None)),
        SelectItem::ExprWithAlias { expr, alias } => Some((expr, Some(alias.value.clone()))),
        _ => None,
    }
}

fn all_aggregates(sel: &Select) -> bool {
    sel.projection.iter().all(|it| {
        item_expr(it).is_some_and(|(e, _)| match e {
            Expr::Function(f) => {
                let name = f.name.to_string().to_ascii_uppercase();
                AGGREGATES.contains(&name.as_str())
                    && f.over.is_none()
                    && !matches!(f.args, FunctionArguments::None)
            }
            _ => false,
        })
    })
}

fn from_text(sel: &Select) -> String {
    sel.from
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// The name an item's column goes by outside: its alias, its column name,
/// or its text.
fn output_name(e: &Expr, alias: Option<&str>) -> String {
    match (alias, e) {
        (Some(a), _) => a.to_owned(),
        (None, Expr::Identifier(i)) => i.value.clone(),
        (None, Expr::CompoundIdentifier(p)) => {
            p.last().map(|i| i.value.clone()).unwrap_or_default()
        }
        (None, e) => e.to_string(),
    }
}

/// The lateral's table reference and where it ends.
fn find_lateral(tk: &Toks<'_>) -> Option<Lateral> {
    let lateral = (1..tk.t.len()).find(|&j| tk.word(j, b"LATERAL") && tk.punct(j + 1, b'('))?;
    let open = lateral + 1;
    let close = tk.matching(open)?;
    // The join before it.
    let (join_start, comma, left) = if tk.punct(lateral - 1, b',') {
        (lateral - 1, true, false)
    } else if tk.word(lateral - 1, b"JOIN") {
        let mut s = lateral - 1;
        let mut left = false;
        if s >= 1 && tk.word(s - 1, b"OUTER") {
            s -= 1;
        }
        if s >= 1 && (tk.word(s - 1, b"INNER") || tk.word(s - 1, b"CROSS")) {
            s -= 1;
        } else if s >= 1 && tk.word(s - 1, b"LEFT") {
            s -= 1;
            left = true;
        } else if s >= 1 && (tk.word(s - 1, b"RIGHT") || tk.word(s - 1, b"NATURAL")) {
            return None;
        }
        (s, false, left)
    } else {
        return None;
    };
    let mut j = close + 1;
    if tk.word(j, b"AS") {
        j += 1;
    }
    let alias = tk.ident(j)?;
    let alias_end = j;
    // Column aliases `l (a, b)` aren't handled.
    if tk.punct(j + 1, b'(') {
        return None;
    }
    let mut on = None;
    if tk.word(j + 1, b"ON") {
        let start = j + 1;
        let mut k = start + 1;
        let mut d = 0;
        let mut last = start;
        while k < tk.t.len() {
            if tk.punct(k, b'(') {
                d += 1;
            } else if tk.punct(k, b')') {
                if d == 0 {
                    break;
                }
                d -= 1;
            } else if d == 0
                && (tk.punct(k, b',')
                    || tk.punct(k, b';')
                    || CLAUSE_END.iter().any(|w| tk.word(k, w)))
            {
                break;
            }
            last = k;
            k += 1;
        }
        if last == start {
            return None;
        }
        on = Some((start, last));
    }
    Some(Lateral {
        join_start,
        comma,
        left,
        kw: lateral,
        open,
        close,
        alias,
        alias_end,
        on,
    })
}

/// Whether the `ON` condition is `TRUE` (or `1`).
fn on_is_true(tk: &Toks<'_>, l: &Lateral) -> bool {
    match l.on {
        None => true,
        Some((s, e)) => e == s + 1 && (tk.word(e, b"TRUE") || tk.t[e].bytes(tk.sql) == b"1"),
    }
}

fn apply(sql: &[u8], mut edits: Vec<(usize, usize, String)>) -> Vec<u8> {
    edits.sort_by_key(|e| e.0);
    let mut out = Vec::with_capacity(sql.len() + 128);
    let mut at = 0;
    for (from, to, text) in edits {
        out.extend_from_slice(&sql[at..from]);
        out.extend_from_slice(text.as_bytes());
        at = to;
    }
    out.extend_from_slice(&sql[at..]);
    out
}

/// The aggregate shape: the join goes, `l.c` becomes a scalar subquery.
fn aggregate_rewrite(
    tk: &Toks<'_>,
    l: &Lateral,
    sel: &Select,
    from: &str,
    q: &Query,
) -> Option<Vec<u8>> {
    if !on_is_true(tk, l) || q.limit_clause.is_some() {
        return None;
    }
    let where_ = sel
        .selection
        .as_ref()
        .map(|w| format!(" WHERE {w}"))
        .unwrap_or_default();
    let mut cols = Vec::new();
    for it in &sel.projection {
        let (e, alias) = item_expr(it)?;
        let name = output_name(e, alias.as_deref());
        cols.push((
            name.to_lowercase(),
            name,
            format!("(SELECT {e} FROM {from}{where_})"),
        ));
    }
    let a = l.alias.to_lowercase();
    let end = l.on.map_or(l.alias_end, |(_, e)| e);
    let from_byte = if l.join_start > 0 {
        tk.end(l.join_start - 1)
    } else {
        tk.start(l.join_start)
    };
    let mut edits = vec![(from_byte, tk.end(end), String::new())];
    let mut named_items: Vec<usize> = Vec::new();
    for j in 0..tk.t.len() {
        if j >= l.join_start && j <= end {
            continue;
        }
        // `l.c`
        if tk.ident(j).is_some_and(|x| x.to_lowercase() == a) && tk.punct(j + 1, b'.') {
            let c = tk.ident(j + 2)?.to_lowercase();
            let (_, name, sub) = cols.iter().find(|(n, _, _)| *n == c)?;
            // A whole select item keeps the column's name, as on MySQL.
            let text = if whole_item(tk, j, j + 2) {
                format!("{sub} AS {}", quote(name))
            } else {
                sub.clone()
            };
            edits.push((tk.start(j), tk.end(j + 2), text));
            // A select item over it (`MAX(l.t)`, `l.t + 1`) keeps its
            // text as its name, as on MySQL.
            if let Some((a, b)) = select_item(tk, j)
                && !(a == j && b == j + 2)
                && !named_items.contains(&a)
                && !has_alias(tk, a, b)
            {
                named_items.push(a);
                let written = String::from_utf8_lossy(&tk.sql[tk.start(a)..tk.end(b)]);
                edits.push((tk.end(b), tk.end(b), format!(" AS {}", quote(&written))));
            }
        } else if tk.t[j].kind.is_ident_like() && !(j > 0 && tk.punct(j - 1, b'.')) {
            // An unqualified name of a lateral column: give up.
            let x = tk.ident(j)?.to_lowercase();
            if cols.iter().any(|(n, _, _)| *n == x)
                && !tk.punct(j + 1, b'.')
                && !tk.punct(j + 1, b'(')
            {
                return None;
            }
        }
    }
    Some(apply(tk.sql, edits))
}

/// The outermost select list's item holding token `j`: its first and
/// last token; `None` when `j` isn't in that list.
fn select_item(tk: &Toks<'_>, j: usize) -> Option<(usize, usize)> {
    let mut d = 0i32;
    let mut start = None;
    for k in 0..tk.t.len() {
        if tk.punct(k, b'(') {
            d += 1;
        } else if tk.punct(k, b')') {
            d -= 1;
        } else if d == 0 && tk.word(k, b"SELECT") && start.is_none() {
            let mut s = k + 1;
            while tk.word(s, b"DISTINCT") || tk.word(s, b"ALL") || tk.word(s, b"STRAIGHT_JOIN") {
                s += 1;
            }
            start = Some(s);
            break;
        }
    }
    let first = start?;
    let mut a = first;
    let mut d = 0i32;
    for k in first..tk.t.len() {
        if tk.punct(k, b'(') {
            d += 1;
        } else if tk.punct(k, b')') {
            d -= 1;
        } else if d == 0 && (tk.punct(k, b',') || tk.word(k, b"FROM") || tk.word(k, b"INTO")) {
            if j >= a && j < k {
                return Some((a, k - 1));
            }
            if !tk.punct(k, b',') {
                return None;
            }
            a = k + 1;
        }
    }
    None
}

/// Whether the select item `a..=b` ends with an alias (`AS x` or `x`).
fn has_alias(tk: &Toks<'_>, a: usize, b: usize) -> bool {
    if b <= a || !tk.t[b].kind.is_ident_like() {
        return false;
    }
    if tk.word(b - 1, b"AS") {
        return true;
    }
    let prev = &tk.t[b - 1];
    !(tk.punct(b - 1, b'.')
        || tk.punct(b - 1, b'(')
        || tk.punct(b - 1, b',')
        || prev.kind == TokenKind::Op
        || [&b"END"[..], b"NULL", b"TRUE", b"FALSE"]
            .iter()
            .any(|w| tk.word(b, w)))
}

/// Whether tokens `a..=b` are a whole select item without an alias.
fn whole_item(tk: &Toks<'_>, a: usize, b: usize) -> bool {
    let mut d = 0i32;
    let mut select_at = None;
    for j in 0..a {
        if tk.punct(j, b'(') {
            d += 1;
        } else if tk.punct(j, b')') {
            d -= 1;
        } else if d == 0 && tk.word(j, b"SELECT") {
            select_at = Some(j);
        } else if d == 0 && tk.word(j, b"FROM") {
            select_at = None;
        }
    }
    if d != 0 || select_at.is_none() || a == 0 {
        return false;
    }
    let before = tk.punct(a - 1, b',') || tk.word(a - 1, b"SELECT") || tk.word(a - 1, b"DISTINCT");
    let after = b + 1 >= tk.t.len()
        || tk.punct(b + 1, b',')
        || tk.punct(b + 1, b';')
        || tk.word(b + 1, b"FROM");
    before && after
}

/// The rows shape: numbered per key, joined on the key and the number.
#[allow(clippy::too_many_lines, clippy::many_single_char_names)] // one statement assembled
fn rows_rewrite(
    tk: &Toks<'_>,
    l: &Lateral,
    sel: &Select,
    q: &Query,
    from: &str,
    keys: &[(Expr, Expr)],
    rest: Option<&Expr>,
) -> Option<Vec<u8>> {
    let (limit, offset) = match &q.limit_clause {
        None => (None, None),
        Some(LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) => {
            if !limit_by.is_empty() {
                return None;
            }
            let n = |e: &Expr| match e {
                Expr::Value(v) => match &v.value {
                    Value::Number(n, _) => n.parse::<u64>().ok(),
                    _ => None,
                },
                _ => None,
            };
            (
                limit
                    .as_ref()
                    .map(n)
                    .map(|x| x.ok_or(()))
                    .transpose()
                    .ok()?,
                offset
                    .as_ref()
                    .map(|o| n(&o.value))
                    .map(|x| x.ok_or(()))
                    .transpose()
                    .ok()?,
            )
        }
        Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
            let n = |e: &Expr| match e {
                Expr::Value(v) => match &v.value {
                    Value::Number(n, _) => n.parse::<u64>().ok(),
                    _ => None,
                },
                _ => None,
            };
            (Some(n(limit)?), Some(n(offset)?))
        }
    };
    let proj: Vec<String> = sel.projection.iter().map(ToString::to_string).collect();
    let mut inner = format!("SELECT {}", proj.join(", "));
    let a = quote(&l.alias);
    let mut conds = Vec::new();
    let partition: Vec<String> = keys.iter().map(|(i, _)| i.to_string()).collect();
    for (n, (i, o)) in keys.iter().enumerate() {
        let _ = write!(inner, ", {i} AS `_lat_k{n}`");
        conds.push(format!("{a}.`_lat_k{n}` = {o}"));
    }
    if limit.is_some() || offset.is_some() {
        let order = q
            .order_by
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let _ = write!(
            inner,
            ", ROW_NUMBER() OVER (PARTITION BY {}{}{}) AS `_lat_rn`",
            partition.join(", "),
            if order.is_empty() { "" } else { " " },
            order
        );
        let off = offset.unwrap_or(0);
        if off > 0 {
            conds.push(format!("{a}.`_lat_rn` > {off}"));
        }
        if let Some(n) = limit {
            conds.push(format!("{a}.`_lat_rn` <= {}", off + n));
        }
    }
    let _ = write!(inner, " FROM {from}");
    if let Some(r) = rest {
        let _ = write!(inner, " WHERE {r}");
    }
    let cond = conds.join(" AND ");
    let mut edits = vec![(tk.start(l.kw), tk.end(l.close), format!("({inner})"))];
    edits.extend(placement(tk, l, &cond));
    let _ = l.left;
    Some(apply(tk.sql, edits))
}

/// What marks the derived table in [`ApplyPlan`]'s final statement.
const DERIVED_MARK: &str = "\u{0}sprig-lateral\u{0}";

/// Most outer key tuples one statement may apply
/// (`translator.lateral_max_outer_rows`, a constant until settings carry
/// it): more fail with 39601.
pub const MAX_OUTER_ROWS: usize = 10_000;

/// Outer key tuples per round trip (`translator.lateral_batch`).
pub const BATCH: usize = 500;

/// The gateway apply for a `LATERAL` derived table the rewrite can't express as
/// SQL: the gateway reads the distinct outer values the subquery uses
/// ([`ApplyPlan::keys_sql`]), runs the subquery for each batch of them with the
/// values written in ([`ApplyPlan::batch_sql`]), and runs the statement with
/// the rows as a derived table joined on those values
/// ([`ApplyPlan::final_sql`]), so MariaDB still computes everything outside the
/// derived table.
#[derive(Debug, Clone)]
pub struct ApplyPlan {
    /// `SELECT DISTINCT <outer references> FROM <the outer tables>`.
    pub keys_sql: String,
    /// The outer references, as written (`c.id`).
    pub outer: Vec<String>,
    /// The subquery's text in pieces: text, or the index of an outer
    /// reference.
    template: Vec<Piece>,
    /// The statement with [`DERIVED_MARK`] in place of the derived table.
    final_template: String,
}

#[derive(Debug, Clone)]
enum Piece {
    Text(String),
    Outer(usize),
}

impl ApplyPlan {
    /// One batch: the subquery for each key tuple (SQL literals, in
    /// [`Self::outer`]'s order), tagged with the tuple.
    #[must_use]
    pub fn batch_sql(&self, keys: &[Vec<String>]) -> String {
        let mut parts = Vec::with_capacity(keys.len());
        for k in keys {
            let mut q = String::new();
            for p in &self.template {
                match p {
                    Piece::Text(t) => q.push_str(t),
                    Piece::Outer(i) => q.push_str(&k[*i]),
                }
            }
            let tags: Vec<String> = k
                .iter()
                .enumerate()
                .map(|(i, v)| format!("{v} AS `_lat_k{i}`"))
                .collect();
            parts.push(format!(
                "SELECT {}, `_lat_q`.* FROM ({q}) AS `_lat_q`",
                tags.join(", ")
            ));
        }
        parts.join(" UNION ALL ")
    }

    /// The statement with `derived` (the batches' rows, keys first) as the
    /// lateral's derived table.
    #[must_use]
    pub fn final_sql(&self, derived: &str) -> String {
        self.final_template.replacen(DERIVED_MARK, derived, 1)
    }
}

/// A plan for applying the statement's (first) `LATERAL` in the gateway;
/// `None` when the statement isn't a `SELECT` with a `LATERAL` at its top
/// level that the plan can carry (column aliases on the lateral, `*`
/// outside, a subquery other than one `SELECT`, no outer reference).
#[must_use]
#[allow(clippy::too_many_lines, clippy::many_single_char_names)] // one pass over the statement
pub fn apply_plan(sql: &[u8], opts: &LexOptions) -> Option<ApplyPlan> {
    if !sql.windows(7).any(|w| w.eq_ignore_ascii_case(b"LATERAL")) {
        return None;
    }
    let tokens = lex::lex(sql, opts).ok()?;
    let tk = Toks {
        sql,
        t: tokens.into_iter().filter(|t| !t.kind.is_trivia()).collect(),
    };
    if !tk.word(0, b"SELECT") {
        return None;
    }
    let l = find_lateral(&tk)?;
    // At the statement's top level, after its FROM.
    let mut depth = 0i32;
    let mut from = None;
    for j in 0..l.kw {
        if tk.punct(j, b'(') {
            depth += 1;
        } else if tk.punct(j, b')') {
            depth -= 1;
        } else if depth == 0 && tk.word(j, b"FROM") {
            from = Some(j);
        }
    }
    let from = from?;
    if depth != 0 {
        return None;
    }
    // No `*` or `l.*` outside: the key columns would show.
    for j in 1..tk.t.len() {
        if (j < l.kw || j > l.alias_end)
            && tk.punct(j, b'*')
            && (tk.punct(j - 1, b',')
                || tk.punct(j - 1, b'.')
                || tk.word(j - 1, b"SELECT")
                || tk.word(j - 1, b"DISTINCT"))
        {
            return None;
        }
    }
    let inner_text = std::str::from_utf8(&sql[tk.end(l.open)..tk.start(l.close)]).ok()?;
    let q = parse_query(inner_text)?;
    let SetExpr::Select(sel) = q.body.as_ref() else {
        return None;
    };
    let inner = table_names(sel)?;
    // The outer references: `x.c` with `x` not one of the subquery's
    // tables, as sqlparser reads them.
    let mut pairs: Vec<(String, String)> = Vec::new();
    {
        use sqlparser::ast::Visit;
        use std::ops::ControlFlow;
        struct V<'a>(&'a [String], &'a mut Vec<(String, String)>);
        impl sqlparser::ast::Visitor for V<'_> {
            type Break = ();
            fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
                if let Expr::CompoundIdentifier(p) = e
                    && p.len() == 2
                {
                    let qual = p[0].value.to_lowercase();
                    let col = p[1].value.to_lowercase();
                    if !self.0.contains(&qual) && !self.1.contains(&(qual.clone(), col.clone())) {
                        self.1.push((qual, col));
                    }
                }
                ControlFlow::Continue(())
            }
        }
        let _ = q.visit(&mut V(&inner, &mut pairs));
    }
    if pairs.is_empty() {
        return None;
    }
    // The subquery's tokens, outer references as pieces.
    let inner_toks: Vec<Token> = lex::lex(inner_text.as_bytes(), opts)
        .ok()?
        .into_iter()
        .filter(|t| !t.kind.is_trivia())
        .collect();
    let it = Toks {
        sql: inner_text.as_bytes(),
        t: inner_toks,
    };
    let mut outer: Vec<String> = Vec::new();
    let mut template = Vec::new();
    let mut at = 0;
    let mut j = 0;
    while j + 2 < it.t.len() + 1 {
        let pair = (it.ident(j), it.punct(j + 1, b'.'), it.ident(j + 2));
        let after_dot = j > 0 && it.punct(j - 1, b'.');
        let table_pos = j > 0 && (it.word(j - 1, b"FROM") || it.word(j - 1, b"JOIN"));
        if let (Some(a), true, Some(b)) = pair
            && !after_dot
            && !table_pos
            && !it.punct(j + 3, b'.')
            && pairs.contains(&(a.to_lowercase(), b.to_lowercase()))
        {
            let written = String::from_utf8_lossy(&it.sql[it.start(j)..it.end(j + 2)]).into_owned();
            let idx = if let Some(i) = outer.iter().position(|o| o.eq_ignore_ascii_case(&written)) {
                i
            } else {
                outer.push(written);
                outer.len() - 1
            };
            template.push(Piece::Text(inner_text[at..it.start(j)].to_owned()));
            template.push(Piece::Outer(idx));
            at = it.end(j + 2);
            j += 3;
            continue;
        }
        j += 1;
    }
    template.push(Piece::Text(inner_text[at..].to_owned()));
    if outer.is_empty() {
        return None;
    }
    // The outer tables: FROM's list without the lateral's join.
    let join_end = l.on.map_or(l.alias_end, |(_, e)| e);
    let mut stop = tk.t.len();
    let mut d = 0i32;
    for k in join_end + 1..tk.t.len() {
        if tk.punct(k, b'(') {
            d += 1;
        } else if tk.punct(k, b')') {
            d -= 1;
        } else if d == 0
            && (tk.punct(k, b';')
                || [
                    &b"WHERE"[..],
                    b"GROUP",
                    b"HAVING",
                    b"ORDER",
                    b"LIMIT",
                    b"WINDOW",
                    b"UNION",
                    b"FOR",
                    b"LOCK",
                    b"INTO",
                ]
                .iter()
                .any(|w| tk.word(k, w)))
        {
            stop = k;
            break;
        }
    }
    let head = String::from_utf8_lossy(&sql[tk.end(from)..tk.start(l.join_start)]);
    let tail = if stop > join_end + 1 {
        String::from_utf8_lossy(&sql[tk.end(join_end)..tk.start(stop)]).into_owned()
    } else {
        String::new()
    };
    let keys_sql = format!("SELECT DISTINCT {} FROM{head}{tail}", outer.join(", "));
    // The final statement: the derived table, joined on the keys.
    let a = quote(&l.alias);
    let cond: Vec<String> = outer
        .iter()
        .enumerate()
        .map(|(i, o)| format!("{a}.`_lat_k{i}` <=> {o}"))
        .collect();
    let mut edits = vec![(tk.start(l.kw), tk.end(l.close), DERIVED_MARK.to_owned())];
    edits.extend(placement(&tk, &l, &cond.join(" AND ")));
    let final_template = String::from_utf8(apply(sql, edits)).ok()?;
    Some(ApplyPlan {
        keys_sql,
        outer,
        template,
        final_template,
    })
}

/// A value of an internal result as an SQL literal: numbers (`numeric`
/// columns) as written, `NULL`, anything else a string.
#[must_use]
pub fn literal(v: Option<&[u8]>, numeric: bool, no_backslash_escapes: bool) -> String {
    match v {
        None => "NULL".to_owned(),
        Some(b)
            if numeric
                && !b.is_empty()
                && b.iter().all(|c| {
                    c.is_ascii_digit() || matches!(c, b'-' | b'.' | b'e' | b'E' | b'+')
                }) =>
        {
            String::from_utf8_lossy(b).into_owned()
        }
        Some(b) => {
            crate::names::rewrite::sql_string(&String::from_utf8_lossy(b), no_backslash_escapes)
        }
    }
}

/// Where the join conditions `cond` of the lateral `l` go: its `ON`, a new
/// `ON`, or (a comma join) the statement's `WHERE`.
fn placement(tk: &Toks<'_>, l: &Lateral, cond: &str) -> Vec<(usize, usize, String)> {
    let mut edits = Vec::new();
    match (l.comma, l.on) {
        (_, Some((_, e))) => {
            // `ON TRUE AND ...` keeps the user's condition.
            edits.push((tk.end(e), tk.end(e), format!(" AND {cond}")));
        }
        (false, None) => edits.push((
            tk.end(l.alias_end),
            tk.end(l.alias_end),
            format!(" ON {cond}"),
        )),
        (true, None) => {
            // A comma join: the conditions join the WHERE clause.
            let mut d = 0;
            let mut where_at = None;
            let mut stop = None;
            for j in l.alias_end + 1..tk.t.len() {
                if tk.punct(j, b'(') {
                    d += 1;
                } else if tk.punct(j, b')') {
                    d -= 1;
                } else if d == 0 && tk.word(j, b"WHERE") {
                    where_at = Some(j);
                    break;
                } else if d == 0
                    && ([
                        &b"GROUP"[..],
                        b"HAVING",
                        b"ORDER",
                        b"LIMIT",
                        b"WINDOW",
                        b"UNION",
                        b"FOR",
                        b"INTO",
                        b"LOCK",
                    ]
                    .iter()
                    .any(|w| tk.word(j, w))
                        || tk.punct(j, b';'))
                {
                    stop = Some(j);
                    break;
                }
                if d < 0 {
                    stop = Some(j);
                    break;
                }
            }
            match (where_at, stop) {
                (Some(w), _) => edits.push((tk.end(w), tk.end(w), format!(" {cond} AND"))),
                (None, Some(s)) => edits.push((tk.start(s), tk.start(s), format!("WHERE {cond} "))),
                (None, None) => {
                    let last = tk.t.len() - 1;
                    edits.push((tk.end(last), tk.end(last), format!(" WHERE {cond}")));
                }
            }
        }
    }
    edits
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rw(s: &str) -> Option<String> {
        rewrite(s.as_bytes(), &LexOptions::mysql84()).map(|v| String::from_utf8(v).unwrap())
    }

    #[test]
    fn gateway_apply_plans() {
        let o = LexOptions::mysql84();
        // A correlation the rewrite can't express: an inequality.
        let sql = b"SELECT c.name, l.amt FROM c JOIN LATERAL (SELECT o.amt FROM o WHERE o.cid > c.id AND o.amt < c.lim ORDER BY o.amt LIMIT 1) AS l ON TRUE WHERE c.id < 9 ORDER BY 1";
        assert!(rewrite(sql, &o).is_none());
        let p = apply_plan(sql, &o).unwrap();
        assert_eq!(p.outer, vec!["c.id".to_owned(), "c.lim".to_owned()]);
        assert_eq!(p.keys_sql, "SELECT DISTINCT c.id, c.lim FROM c ");
        assert_eq!(
            p.batch_sql(&[
                vec!["1".into(), "5".into()],
                vec!["2".into(), "NULL".into()]
            ]),
            "SELECT 1 AS `_lat_k0`, 5 AS `_lat_k1`, `_lat_q`.* FROM (SELECT o.amt FROM o WHERE o.cid > 1 AND o.amt < 5 ORDER BY o.amt LIMIT 1) AS `_lat_q` UNION ALL SELECT 2 AS `_lat_k0`, NULL AS `_lat_k1`, `_lat_q`.* FROM (SELECT o.amt FROM o WHERE o.cid > 2 AND o.amt < NULL ORDER BY o.amt LIMIT 1) AS `_lat_q`"
        );
        assert_eq!(
            p.final_sql("(SELECT 1 AS `_lat_k0`, 5 AS `_lat_k1`, 10 AS amt)"),
            "SELECT c.name, l.amt FROM c JOIN (SELECT 1 AS `_lat_k0`, 5 AS `_lat_k1`, 10 AS amt) AS l ON TRUE AND `l`.`_lat_k0` <=> c.id AND `l`.`_lat_k1` <=> c.lim WHERE c.id < 9 ORDER BY 1"
        );
        // A comma join with other tables: the keys come from all of them.
        let p = apply_plan(b"SELECT t.a, l.x FROM t, u, LATERAL (SELECT v.x FROM v WHERE v.k BETWEEN t.a AND u.b) l WHERE t.a = u.a", &o).unwrap();
        assert_eq!(p.keys_sql, "SELECT DISTINCT t.a, u.b FROM t, u");
        assert!(p.final_sql("(D)").ends_with("FROM t, u, (D) l WHERE `l`.`_lat_k0` <=> t.a AND `l`.`_lat_k1` <=> u.b AND t.a = u.a"), "{}", p.final_sql("(D)"));
        // `*` outside, or no outer reference: no plan.
        assert!(
            apply_plan(
                b"SELECT * FROM c, LATERAL (SELECT o.a FROM o WHERE o.c > c.id) l",
                &o
            )
            .is_none()
        );
        assert!(apply_plan(b"SELECT l.a FROM c, LATERAL (SELECT o.a FROM o) l", &o).is_none());
        assert_eq!(literal(Some(b"007"), false, false), "'007'");
        assert_eq!(literal(Some(b"-1.5"), true, false), "-1.5");
        assert_eq!(literal(None, true, false), "NULL");
    }

    #[test]
    fn top_n_becomes_a_numbered_join() {
        assert_eq!(
            rw("SELECT c.name, l.amt FROM c JOIN LATERAL (SELECT o.amt FROM o WHERE o.cid = c.id ORDER BY o.amt DESC LIMIT 2) AS l ORDER BY c.name, l.amt").unwrap(),
            "SELECT c.name, l.amt FROM c JOIN (SELECT o.amt, o.cid AS `_lat_k0`, ROW_NUMBER() OVER (PARTITION BY o.cid ORDER BY o.amt DESC) AS `_lat_rn` FROM o) AS l \
             ON `l`.`_lat_k0` = c.id AND `l`.`_lat_rn` <= 2 ORDER BY c.name, l.amt"
        );
    }

    #[test]
    fn comma_joins_use_where() {
        assert_eq!(
            rw("SELECT c.name, l.amt FROM c, LATERAL (SELECT amt FROM o WHERE o.cid = c.id AND o.amt > 6 ORDER BY amt LIMIT 1) l ORDER BY c.name").unwrap(),
            "SELECT c.name, l.amt FROM c, (SELECT amt, o.cid AS `_lat_k0`, ROW_NUMBER() OVER (PARTITION BY o.cid ORDER BY amt) AS `_lat_rn` FROM o WHERE o.amt > 6) l \
             WHERE `l`.`_lat_k0` = c.id AND `l`.`_lat_rn` <= 1 ORDER BY c.name"
        );
        assert_eq!(
            rw("SELECT * FROM c, LATERAL (SELECT amt FROM o WHERE o.cid = c.id) l"),
            None
        );
        assert_eq!(
            rw("SELECT c.id, l.amt FROM c, LATERAL (SELECT amt FROM o WHERE o.cid = c.id) l WHERE c.id > 1").unwrap(),
            "SELECT c.id, l.amt FROM c, (SELECT amt, o.cid AS `_lat_k0` FROM o) l WHERE `l`.`_lat_k0` = c.id AND c.id > 1"
        );
    }

    #[test]
    fn left_join_on_true_keeps_outer_rows() {
        assert_eq!(
            rw("SELECT c.name, l.amt FROM c LEFT JOIN LATERAL (SELECT o.amt FROM o WHERE o.cid = c.id ORDER BY o.amt LIMIT 1) AS l ON TRUE ORDER BY c.name").unwrap(),
            "SELECT c.name, l.amt FROM c LEFT JOIN (SELECT o.amt, o.cid AS `_lat_k0`, ROW_NUMBER() OVER (PARTITION BY o.cid ORDER BY o.amt) AS `_lat_rn` FROM o) AS l \
             ON TRUE AND `l`.`_lat_k0` = c.id AND `l`.`_lat_rn` <= 1 ORDER BY c.name"
        );
    }

    #[test]
    fn aggregates_become_scalar_subqueries() {
        assert_eq!(
            rw("SELECT c.name, l.total, l.top FROM c JOIN LATERAL (SELECT SUM(o.amt) AS total, MAX(o.amt) AS top FROM o WHERE o.cid = c.id) AS l ORDER BY c.name").unwrap(),
            "SELECT c.name, (SELECT SUM(o.amt) FROM o WHERE o.cid = c.id) AS `total`, (SELECT MAX(o.amt) FROM o WHERE o.cid = c.id) AS `top` FROM c ORDER BY c.name"
        );
        assert_eq!(
            rw("SELECT c.name, l.n FROM c, LATERAL (SELECT COUNT(*) AS n FROM o WHERE o.cid = c.id) AS l WHERE l.n > 0").unwrap(),
            "SELECT c.name, (SELECT COUNT(*) FROM o WHERE o.cid = c.id) AS `n` FROM c WHERE (SELECT COUNT(*) FROM o WHERE o.cid = c.id) > 0"
        );
        // An unqualified reference to the lateral's column: left alone.
        assert_eq!(
            rw(
                "SELECT name, n FROM c, LATERAL (SELECT COUNT(*) AS n FROM o WHERE o.cid = c.id) AS l"
            ),
            None
        );
        // Inside an expression or an outer aggregate: the item keeps its text
        // as its name, as on MySQL.
        assert_eq!(
            rw("SELECT MAX(l.t), COUNT(*), l.t + 1 AS u, MAX(l.t) m FROM c, LATERAL (SELECT SUM(o.amt) AS t FROM o WHERE o.cid = c.id) AS l")
                .unwrap(),
            "SELECT MAX((SELECT SUM(o.amt) FROM o WHERE o.cid = c.id)) AS `MAX(l.t)`, COUNT(*), \
             (SELECT SUM(o.amt) FROM o WHERE o.cid = c.id) + 1 AS u, MAX((SELECT SUM(o.amt) FROM o WHERE o.cid = c.id)) m FROM c"
        );
    }

    #[test]
    fn other_shapes_are_left_alone() {
        assert_eq!(rw("SELECT 1"), None);
        assert_eq!(rw("SELECT lateral FROM t"), None);
        // No correlation, a GROUP BY inside, a non-equality correlation.
        assert_eq!(rw("SELECT x.a FROM c, LATERAL (SELECT a FROM o) x"), None);
        assert_eq!(
            rw(
                "SELECT x.a FROM c, LATERAL (SELECT cid AS a FROM o WHERE o.cid = c.id GROUP BY cid) x"
            ),
            None
        );
        assert_eq!(
            rw("SELECT x.a FROM c, LATERAL (SELECT amt AS a FROM o WHERE o.amt > c.id) x"),
            None
        );
    }
}
