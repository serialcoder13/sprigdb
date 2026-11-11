//! `LATERAL` derived tables on port 3306 (chapter 28, "`LATERAL`"; build
//! step 9; R4 round 3).
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
//!   (SELECT o.amt, o.cid AS `_sprig_lk0`, ROW_NUMBER() OVER (...) AS `_sprig_rn`
//!      FROM o) AS l ... ON l.`_sprig_lk0` = c.id AND l.`_sprig_rn` <= 2
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
//! is left as it is, and MariaDB's 1064 stands. Chapter 28's gateway
//! apply (running the derived table per batch of outer rows) isn't built.
//! The rewrite is plain SQL, so stored bodies could use it too; they don't
//! yet (chapter 28 step 12).

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
        self.t
            .get(j)
            .is_some_and(|tk| tk.kind == TokenKind::Word && tk.bytes(self.sql).eq_ignore_ascii_case(w))
    }

    fn punct(&self, j: usize, c: u8) -> bool {
        self.t
            .get(j)
            .is_some_and(|tk| matches!(tk.kind, TokenKind::Punct | TokenKind::Op) && tk.bytes(self.sql) == [c])
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
    lateral: usize,
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
        if (j < l.lateral || j > l.alias_end) && tk.punct(j, b'*') && j > 0 && (tk.punct(j - 1, b',') || tk.punct(j - 1, b'.') || tk.word(j - 1, b"SELECT") || tk.word(j - 1, b"DISTINCT")) {
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
    if grouped || sel.distinct.is_some() || sel.having.is_some() || !sel.named_window.is_empty() || sel.from.is_empty() || sel.into.is_some() {
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
    use std::ops::ControlFlow;
    use sqlparser::ast::Visit;
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
fn correlation(where_: Option<&Expr>, inner: &[String]) -> Option<(Vec<(Expr, Expr)>, Option<Expr>)> {
    let mut conj = Vec::new();
    if let Some(w) = where_ {
        split_and(w, &mut conj);
    }
    let mut keys = Vec::new();
    let mut rest: Vec<Expr> = Vec::new();
    for c in conj {
        if let Expr::BinaryOp { left, op: BinaryOperator::Eq, right } = &c {
            let lo = matches!(left.as_ref(), Expr::CompoundIdentifier(_)) && is_outer(left, inner);
            let ro = matches!(right.as_ref(), Expr::CompoundIdentifier(_)) && is_outer(right, inner);
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
        Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
            split_and(left, out);
            split_and(right, out);
        }
        Expr::Nested(inner) if matches!(inner.as_ref(), Expr::BinaryOp { op: BinaryOperator::And, .. }) => {
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
                AGGREGATES.contains(&name.as_str()) && f.over.is_none() && !matches!(f.args, FunctionArguments::None)
            }
            _ => false,
        })
    })
}

fn from_text(sel: &Select) -> String {
    sel.from.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
}

/// The name an item's column goes by outside: its alias, its column name,
/// or its text.
fn output_name(e: &Expr, alias: Option<&str>) -> String {
    match (alias, e) {
        (Some(a), _) => a.to_owned(),
        (None, Expr::Identifier(i)) => i.value.clone(),
        (None, Expr::CompoundIdentifier(p)) => p.last().map(|i| i.value.clone()).unwrap_or_default(),
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
            } else if d == 0 && (tk.punct(k, b',') || tk.punct(k, b';') || CLAUSE_END.iter().any(|w| tk.word(k, w))) {
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
        lateral,
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
fn aggregate_rewrite(tk: &Toks<'_>, l: &Lateral, sel: &Select, from: &str, q: &Query) -> Option<Vec<u8>> {
    if !on_is_true(tk, l) || q.limit_clause.is_some() {
        return None;
    }
    let where_ = sel.selection.as_ref().map(|w| format!(" WHERE {w}")).unwrap_or_default();
    let mut cols = Vec::new();
    for it in &sel.projection {
        let (e, alias) = item_expr(it)?;
        let name = output_name(e, alias.as_deref());
        cols.push((name.to_lowercase(), name, format!("(SELECT {e} FROM {from}{where_})")));
    }
    let a = l.alias.to_lowercase();
    let end = l.on.map_or(l.alias_end, |(_, e)| e);
    let from_byte = if l.join_start > 0 {
        tk.end(l.join_start - 1)
    } else {
        tk.start(l.join_start)
    };
    let mut edits = vec![(from_byte, tk.end(end), String::new())];
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
        } else if tk.t[j].kind.is_ident_like() && !(j > 0 && tk.punct(j - 1, b'.')) {
            // An unqualified name of a lateral column: give up.
            let x = tk.ident(j)?.to_lowercase();
            if cols.iter().any(|(n, _, _)| *n == x) && !tk.punct(j + 1, b'.') && !tk.punct(j + 1, b'(') {
                return None;
            }
        }
    }
    Some(apply(tk.sql, edits))
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
    let after = b + 1 >= tk.t.len() || tk.punct(b + 1, b',') || tk.punct(b + 1, b';') || tk.word(b + 1, b"FROM");
    before && after
}

/// The rows shape: numbered per key, joined on the key and the number.
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
        Some(LimitClause::LimitOffset { limit, offset, limit_by }) => {
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
            (limit.as_ref().map(n).map(|x| x.ok_or(())).transpose().ok()?, offset.as_ref().map(|o| n(&o.value)).map(|x| x.ok_or(())).transpose().ok()?)
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
        inner.push_str(&format!(", {i} AS `_sprig_lk{n}`"));
        conds.push(format!("{a}.`_sprig_lk{n}` = {o}"));
    }
    if limit.is_some() || offset.is_some() {
        let order = q
            .order_by
            .as_ref()
            .map(|o| o.to_string())
            .unwrap_or_default();
        inner.push_str(&format!(
            ", ROW_NUMBER() OVER (PARTITION BY {}{}{}) AS `_sprig_rn`",
            partition.join(", "),
            if order.is_empty() { "" } else { " " },
            order
        ));
        let off = offset.unwrap_or(0);
        if off > 0 {
            conds.push(format!("{a}.`_sprig_rn` > {off}"));
        }
        if let Some(n) = limit {
            conds.push(format!("{a}.`_sprig_rn` <= {}", off + n));
        }
    }
    inner.push_str(&format!(" FROM {from}"));
    if let Some(r) = rest {
        inner.push_str(&format!(" WHERE {r}"));
    }
    let cond = conds.join(" AND ");
    let mut edits = vec![(tk.start(l.lateral), tk.end(l.close), format!("({inner})"))];
    match (l.comma, l.on) {
        (_, Some((_, e))) => {
            // `ON TRUE AND ...` keeps the user's condition.
            edits.push((tk.end(e), tk.end(e), format!(" AND {cond}")));
        }
        (false, None) => edits.push((tk.end(l.alias_end), tk.end(l.alias_end), format!(" ON {cond}"))),
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
                    && ([&b"GROUP"[..], b"HAVING", b"ORDER", b"LIMIT", b"WINDOW", b"UNION", b"FOR", b"INTO", b"LOCK"]
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
    let _ = l.left;
    Some(apply(tk.sql, edits))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rw(s: &str) -> Option<String> {
        rewrite(s.as_bytes(), &LexOptions::mysql84()).map(|v| String::from_utf8(v).unwrap())
    }

    #[test]
    fn top_n_becomes_a_numbered_join() {
        assert_eq!(
            rw("SELECT c.name, l.amt FROM c JOIN LATERAL (SELECT o.amt FROM o WHERE o.cid = c.id ORDER BY o.amt DESC LIMIT 2) AS l ORDER BY c.name, l.amt").unwrap(),
            "SELECT c.name, l.amt FROM c JOIN (SELECT o.amt, o.cid AS `_sprig_lk0`, ROW_NUMBER() OVER (PARTITION BY o.cid ORDER BY o.amt DESC) AS `_sprig_rn` FROM o) AS l \
             ON `l`.`_sprig_lk0` = c.id AND `l`.`_sprig_rn` <= 2 ORDER BY c.name, l.amt"
        );
    }

    #[test]
    fn comma_joins_use_where() {
        assert_eq!(
            rw("SELECT c.name, l.amt FROM c, LATERAL (SELECT amt FROM o WHERE o.cid = c.id AND o.amt > 6 ORDER BY amt LIMIT 1) l ORDER BY c.name").unwrap(),
            "SELECT c.name, l.amt FROM c, (SELECT amt, o.cid AS `_sprig_lk0`, ROW_NUMBER() OVER (PARTITION BY o.cid ORDER BY amt) AS `_sprig_rn` FROM o WHERE o.amt > 6) l \
             WHERE `l`.`_sprig_lk0` = c.id AND `l`.`_sprig_rn` <= 1 ORDER BY c.name"
        );
        assert_eq!(
            rw("SELECT * FROM c, LATERAL (SELECT amt FROM o WHERE o.cid = c.id) l"),
            None
        );
        assert_eq!(
            rw("SELECT c.id, l.amt FROM c, LATERAL (SELECT amt FROM o WHERE o.cid = c.id) l WHERE c.id > 1").unwrap(),
            "SELECT c.id, l.amt FROM c, (SELECT amt, o.cid AS `_sprig_lk0` FROM o) l WHERE `l`.`_sprig_lk0` = c.id AND c.id > 1"
        );
    }

    #[test]
    fn left_join_on_true_keeps_outer_rows() {
        assert_eq!(
            rw("SELECT c.name, l.amt FROM c LEFT JOIN LATERAL (SELECT o.amt FROM o WHERE o.cid = c.id ORDER BY o.amt LIMIT 1) AS l ON TRUE ORDER BY c.name").unwrap(),
            "SELECT c.name, l.amt FROM c LEFT JOIN (SELECT o.amt, o.cid AS `_sprig_lk0`, ROW_NUMBER() OVER (PARTITION BY o.cid ORDER BY o.amt) AS `_sprig_rn` FROM o) AS l \
             ON TRUE AND `l`.`_sprig_lk0` = c.id AND `l`.`_sprig_rn` <= 1 ORDER BY c.name"
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
            rw("SELECT name, n FROM c, LATERAL (SELECT COUNT(*) AS n FROM o WHERE o.cid = c.id) AS l"),
            None
        );
    }

    #[test]
    fn other_shapes_are_left_alone() {
        assert_eq!(rw("SELECT 1"), None);
        assert_eq!(rw("SELECT lateral FROM t"), None);
        // No correlation, a GROUP BY inside, a non-equality correlation.
        assert_eq!(rw("SELECT x.a FROM c, LATERAL (SELECT a FROM o) x"), None);
        assert_eq!(rw("SELECT x.a FROM c, LATERAL (SELECT cid AS a FROM o WHERE o.cid = c.id GROUP BY cid) x"), None);
        assert_eq!(rw("SELECT x.a FROM c, LATERAL (SELECT amt AS a FROM o WHERE o.amt > c.id) x"), None);
    }
}
