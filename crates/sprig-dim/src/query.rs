//! An Analytics query (chapter 35, "Analytics query") and its compilation
//! to SQL (chapter 35, "Compiling a query").
//!
//! ```text
//! SELECT <level keys> AS k1.., <partial aggregates> AS p1..
//! FROM <db>.<fact> AS f
//! LEFT JOIN <db>.<dimension> [FOR SYSTEM_TIME ALL] AS d1 ON d1.<key> = f.<column>
//!        [AND f.<time> >= d1.ROW_START AND f.<time> < d1.ROW_END]    -- at_fact_time
//! WHERE <filters>     -- time filters as UTC ranges on the raw column
//! GROUP BY k1..
//! ```
//!
//! Every measure is decomposable (sum, count, min, max; avg as a sum and
//! a count), so the same statement is right on one shard and, through the
//! router, on many. Subtotals are computed in the gateway ([`crate::merge`]),
//! except a `count_distinct` measure's: distinct counts don't add up across
//! groups, so with `totals` each rollup level is its own statement
//! (`PerLevel`).

use serde_json::Value;
use sprig_common::{Result, SprigCode, SprigError};

use crate::expr;
use crate::model::{Agg, Attribution, DimKind, Model, Versioning};

/// A comparison in a filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// `=`.
    Eq,
    /// `<>`.
    Ne,
    /// `IN`.
    In,
    /// `NOT IN`.
    NotIn,
    /// Inclusive range of two values.
    Between,
    /// `<`.
    Lt,
    /// `<=`.
    Le,
    /// `>`.
    Gt,
    /// `>=`.
    Ge,
}

impl Op {
    fn parse(s: &str) -> Result<Self> {
        Ok(match s.to_ascii_lowercase().as_str() {
            "eq" | "=" => Self::Eq,
            "ne" | "<>" | "!=" => Self::Ne,
            "in" => Self::In,
            "not_in" => Self::NotIn,
            "between" => Self::Between,
            "lt" | "<" => Self::Lt,
            "le" | "<=" => Self::Le,
            "gt" | ">" => Self::Gt,
            "ge" | ">=" => Self::Ge,
            other => return Err(unknown(format!("filter op '{other}'"))),
        })
    }
}

/// A filter on a dimension level.
#[derive(Debug, Clone, PartialEq)]
pub struct Filter {
    /// The dimension.
    pub dimension: String,
    /// The level.
    pub level: String,
    /// The comparison.
    pub op: Op,
    /// Its values.
    pub values: Vec<Value>,
}

/// What a sort uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderBy {
    /// A measure's value.
    Measure(String),
    /// A grouped level, by its index in `dims`.
    Level(usize),
}

/// A query.
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    /// The model.
    pub model: String,
    /// Measures wanted.
    pub measures: Vec<String>,
    /// `(dimension, level)` to group by, outermost first.
    pub dims: Vec<(String, String)>,
    /// Filters.
    pub filters: Vec<Filter>,
    /// Subtotals and a grand total.
    pub totals: bool,
    /// Sort keys, with `desc`.
    pub order: Vec<(OrderBy, bool)>,
    /// Detail rows returned.
    pub limit: Option<u64>,
    /// Overrides every link's attribution.
    pub attribution: Option<Attribution>,
}

/// 39751.
pub fn unknown(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::DimUnknownMember, m)
}

impl Query {
    /// Parses chapter 35's JSON.
    ///
    /// # Errors
    /// 39751 for unknown names or shapes.
    pub fn parse(v: &Value) -> Result<Self> {
        let s = |x: &Value, k: &str| x.get(k).and_then(Value::as_str).unwrap_or("").to_owned();
        let measures: Vec<String> = v
            .get("measures")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect();
        let dims: Vec<(String, String)> = v
            .get("dimensions")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|d| (s(d, "dimension"), s(d, "level")))
            .collect();
        let mut filters = Vec::new();
        for f in v
            .get("filters")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let values = match f.get("values") {
                Some(Value::Array(a)) => a.clone(),
                Some(x) if !x.is_null() => vec![x.clone()],
                _ => f.get("value").cloned().into_iter().collect(),
            };
            filters.push(Filter {
                dimension: s(f, "dimension"),
                level: s(f, "level"),
                op: Op::parse(&s(f, "op"))?,
                values,
            });
        }
        let mut order = Vec::new();
        for o in v
            .get("order")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let desc = o.get("desc").and_then(Value::as_bool).unwrap_or(false);
            let by = if let Some(m) = o.get("measure").and_then(Value::as_str) {
                OrderBy::Measure(m.to_owned())
            } else {
                let (d, l) = (s(o, "dimension"), s(o, "level"));
                let i = dims
                    .iter()
                    .position(|x| x.0 == d && (l.is_empty() || x.1 == l))
                    .ok_or_else(|| {
                        unknown(format!("order by '{d}', which the query doesn't group by"))
                    })?;
                OrderBy::Level(i)
            };
            order.push((by, desc));
        }
        let attribution = match s(v, "attribution").to_ascii_lowercase().as_str() {
            "" => None,
            "current" => Some(Attribution::Current),
            "at_fact_time" => Some(Attribution::AtFactTime),
            other => {
                return Err(unknown(format!(
                    "attribution '{other}': current or at_fact_time (as_of isn't built)"
                )));
            }
        };
        Ok(Self {
            model: s(v, "model"),
            measures,
            dims,
            filters,
            totals: v.get("totals").and_then(Value::as_bool).unwrap_or(false),
            order,
            limit: match v.get("limit") {
                Some(Value::Number(n)) => n.as_u64().filter(|l| *l > 0),
                Some(Value::String(s)) => s.parse().ok().filter(|l| *l > 0),
                _ => None,
            },
            attribution,
        })
    }
}

impl Query {
    /// Back to chapter 35's JSON (`Drill` returns it).
    #[must_use]
    pub fn to_json(&self) -> Value {
        let op = |o: Op| match o {
            Op::Eq => "eq",
            Op::Ne => "ne",
            Op::In => "in",
            Op::NotIn => "not_in",
            Op::Between => "between",
            Op::Lt => "lt",
            Op::Le => "le",
            Op::Gt => "gt",
            Op::Ge => "ge",
        };
        let mut v = serde_json::json!({
            "model": self.model,
            "measures": self.measures,
            "dimensions": self.dims.iter().map(|(d, l)| serde_json::json!({"dimension": d, "level": l})).collect::<Vec<_>>(),
            "filters": self.filters.iter().map(|f| serde_json::json!({"dimension": f.dimension, "level": f.level, "op": op(f.op), "values": f.values})).collect::<Vec<_>>(),
            "totals": self.totals,
            "order": self.order.iter().map(|(by, desc)| match by {
                OrderBy::Measure(m) => serde_json::json!({"measure": m, "desc": desc}),
                OrderBy::Level(i) => serde_json::json!({"dimension": self.dims[*i].0, "level": self.dims[*i].1, "desc": desc}),
            }).collect::<Vec<_>>(),
        });
        if let Some(l) = self.limit {
            v["limit"] = serde_json::json!(l);
        }
        match self.attribution {
            Some(Attribution::Current) => v["attribution"] = serde_json::json!("current"),
            Some(Attribution::AtFactTime) => v["attribution"] = serde_json::json!("at_fact_time"),
            None => {}
        }
        v
    }
}

/// The aggregates of one measure's partial columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MeasureOut {
    /// The aggregate.
    pub agg: Agg,
    /// Its first partial column (avg: the sum, then the count).
    pub col: usize,
}

/// How a query runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Compiled {
    /// `local` or `per_level`.
    pub plan: &'static str,
    /// The statement grouping by every level.
    pub sql: String,
    /// With `per_level`: the statement of each rollup level `j` (grouping
    /// by the first `j` levels), for `j` from 0 to n − 1.
    pub levels: Vec<String>,
    /// Number of key columns.
    pub keys: usize,
    /// Per measure, in the query's order.
    pub measures: Vec<MeasureOut>,
}

/// A JSON value as a SQL literal.
fn lit(v: &Value) -> Result<String> {
    Ok(match v {
        Value::String(s) => format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''")),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.into(),
        Value::Null => "NULL".into(),
        other => {
            return Err(unknown(format!(
                "a filter value {other} must be a string, number or boolean"
            )));
        }
    })
}

fn ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

fn zone_lit(z: &str) -> String {
    format!("'{}'", z.replace('\'', ""))
}

/// The time zone shift of a raw UTC column.
fn local_time(col: &str, zone: &str) -> String {
    if zone.eq_ignore_ascii_case("UTC") || zone == "+00:00" {
        col.to_owned()
    } else {
        format!("CONVERT_TZ({col}, '+00:00', {})", zone_lit(zone))
    }
}

/// A local instant (`'YYYY-MM-DD HH:MM:SS'` in the model's zone) as UTC SQL.
fn utc_of(local: &str, zone: &str) -> String {
    if zone.eq_ignore_ascii_case("UTC") || zone == "+00:00" {
        format!("'{local}'")
    } else {
        format!("CONVERT_TZ('{local}', {}, '+00:00')", zone_lit(zone))
    }
}

fn time_key(level: &str, t: &str, fsm: u8) -> Option<String> {
    Some(match level {
        "year" => format!("YEAR({t})"),
        "fiscal_year" => format!("YEAR({t} - INTERVAL {} MONTH)", fsm - 1),
        "quarter" => format!("(YEAR({t}) * 10 + QUARTER({t}))"),
        "month" => format!("EXTRACT(YEAR_MONTH FROM {t})"),
        "week" => format!("YEARWEEK({t}, 3)"),
        "day" => format!("DATE({t})"),
        "hour" => format!("DATE_FORMAT({t}, '%Y-%m-%d %H:00')"),
        _ => return None,
    })
}

/// The local `[start, end)` of a day, month or year value, as text.
fn period(level: &str, v: &Value) -> Option<(String, String)> {
    let s = match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => return None,
    };
    let num = |x: &str| x.parse::<i32>().ok();
    match level {
        "day" if s.len() == 10 => {
            let (y, m, d) = (num(&s[0..4])?, num(&s[5..7])?, num(&s[8..10])?);
            let next = next_day(y, m, d);
            Some((
                format!("{s} 00:00:00"),
                format!("{:04}-{:02}-{:02} 00:00:00", next.0, next.1, next.2),
            ))
        }
        "month" => {
            let digits: String = s.chars().filter(char::is_ascii_digit).collect();
            if digits.len() != 6 {
                return None;
            }
            let (y, m) = (num(&digits[0..4])?, num(&digits[4..6])?);
            if !(1..=12).contains(&m) {
                return None;
            }
            let (ny, nm) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
            Some((
                format!("{y:04}-{m:02}-01 00:00:00"),
                format!("{ny:04}-{nm:02}-01 00:00:00"),
            ))
        }
        "year" => {
            let y = num(&s)?;
            Some((
                format!("{y:04}-01-01 00:00:00"),
                format!("{:04}-01-01 00:00:00", y + 1),
            ))
        }
        _ => None,
    }
}

fn next_day(y: i32, m: i32, d: i32) -> (i32, i32, i32) {
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let dim = match m {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d < dim {
        (y, m, d + 1)
    } else if m < 12 {
        (y, m + 1, 1)
    } else {
        (y + 1, 1, 1)
    }
}

fn cmp_sql(lhs: &str, op: Op, values: &[Value]) -> Result<String> {
    let one = || {
        values
            .first()
            .ok_or_else(|| unknown("a filter needs a value"))
            .and_then(lit)
    };
    Ok(match op {
        Op::Eq => format!("{lhs} = {}", one()?),
        Op::Ne => format!("{lhs} <> {}", one()?),
        Op::Lt => format!("{lhs} < {}", one()?),
        Op::Le => format!("{lhs} <= {}", one()?),
        Op::Gt => format!("{lhs} > {}", one()?),
        Op::Ge => format!("{lhs} >= {}", one()?),
        Op::In | Op::NotIn => {
            if values.is_empty() {
                return Ok(if op == Op::In {
                    "FALSE".into()
                } else {
                    "TRUE".into()
                });
            }
            let l: Vec<String> = values.iter().map(lit).collect::<Result<_>>()?;
            format!(
                "{lhs} {}IN ({})",
                if op == Op::NotIn { "NOT " } else { "" },
                l.join(", ")
            )
        }
        Op::Between => {
            if values.len() != 2 {
                return Err(unknown("between needs two values"));
            }
            format!(
                "{lhs} BETWEEN {} AND {}",
                lit(&values[0])?,
                lit(&values[1])?
            )
        }
    })
}

/// Compiles `q` over `m`.
///
/// # Errors
/// 39751 for unknown measures, dimensions or levels; 39754 for what this
/// release can't answer (measures of several facts).
#[allow(clippy::too_many_lines)]
pub fn compile(m: &Model, q: &Query) -> Result<Compiled> {
    if q.measures.is_empty() {
        return Err(unknown("a query needs a measure"));
    }
    let mut fact = None;
    for name in &q.measures {
        let (f, _) = m
            .measure(name)
            .ok_or_else(|| unknown(format!("unknown measure '{name}'")))?;
        match fact {
            None => fact = Some(f),
            Some(x) if x.name == f.name => {}
            Some(_) => {
                return Err(SprigError::new(
                    SprigCode::DimNotDecomposable,
                    "measures of several facts (drill-across) aren't answered in this release",
                ));
            }
        }
    }
    let fact = fact.ok_or_else(|| unknown("no fact"))?;
    let db = ident(&m.database);
    // Dimensions used, with their aliases and join clauses.
    let mut used: Vec<String> = Vec::new();
    for d in q
        .dims
        .iter()
        .map(|x| &x.0)
        .chain(q.filters.iter().map(|f| &f.dimension))
    {
        if !used.contains(d) {
            used.push(d.clone());
        }
    }
    let mut joins = Vec::new();
    let mut alias = std::collections::HashMap::new();
    let mut time_col = std::collections::HashMap::new();
    for (i, dname) in used.iter().enumerate() {
        let d = m
            .dimension(dname)
            .ok_or_else(|| unknown(format!("unknown dimension '{dname}'")))?;
        let link = fact
            .links
            .iter()
            .find(|l| &l.dimension == dname)
            .ok_or_else(|| unknown(format!("fact '{}' has no link to '{dname}'", fact.name)))?;
        match &d.kind {
            DimKind::Time => {
                time_col.insert(dname.clone(), format!("f.{}", ident(&link.column)));
            }
            DimKind::Table {
                table,
                key,
                versioning,
                ..
            } => {
                let a = format!("d{}", i + 1);
                let attribution = q.attribution.unwrap_or(link.attribution);
                let base = format!("LEFT JOIN {db}.{}", ident(table));
                let on = format!("{a}.{} = f.{}", ident(key), ident(&link.column));
                let clause = match (attribution, versioning, &link.time) {
                    (Attribution::AtFactTime, Versioning::System, Some(t)) => format!(
                        "{base} FOR SYSTEM_TIME ALL AS {a} ON {on} AND f.{t} >= {a}.ROW_START AND f.{t} < {a}.ROW_END",
                        t = ident(t)
                    ),
                    (Attribution::AtFactTime, Versioning::Application { from, to }, Some(t)) => {
                        format!(
                            "{base} AS {a} ON {on} AND f.{t} >= {a}.{} AND f.{t} < {a}.{}",
                            ident(from),
                            ident(to),
                            t = ident(t)
                        )
                    }
                    _ => format!("{base} AS {a} ON {on}"),
                };
                joins.push(clause);
                alias.insert(dname.clone(), a);
            }
        }
    }
    let level_sql = |dname: &str, level: &str| -> Result<String> {
        let d = m
            .dimension(dname)
            .ok_or_else(|| unknown(format!("unknown dimension '{dname}'")))?;
        match &d.kind {
            DimKind::Time => {
                let t = local_time(&time_col[dname], &m.time_zone);
                time_key(level, &t, m.fiscal_start_month)
                    .ok_or_else(|| unknown(format!("unknown level '{level}' of '{dname}'")))
            }
            DimKind::Table { levels, .. } => {
                if !levels.iter().any(|l| l == level) {
                    return Err(unknown(format!("unknown level '{level}' of '{dname}'")));
                }
                Ok(format!("{}.{}", alias[dname], ident(level)))
            }
        }
    };
    let keys: Vec<String> = q
        .dims
        .iter()
        .map(|(d, l)| level_sql(d, l))
        .collect::<Result<_>>()?;
    // Filters.
    let mut wheres = Vec::new();
    for f in &q.filters {
        let d = m
            .dimension(&f.dimension)
            .ok_or_else(|| unknown(format!("unknown dimension '{}'", f.dimension)))?;
        if matches!(d.kind, DimKind::Time) {
            // Day, month and year values become UTC ranges on the raw
            // column, so its index serves them.
            let col = &time_col[&f.dimension];
            let range = |v: &Value| period(&f.level, v);
            let ranged = match f.op {
                Op::Eq | Op::In => f
                    .values
                    .iter()
                    .map(|v| {
                        range(v).map(|(a, b)| {
                            format!(
                                "({col} >= {} AND {col} < {})",
                                utc_of(&a, &m.time_zone),
                                utc_of(&b, &m.time_zone)
                            )
                        })
                    })
                    .collect::<Option<Vec<_>>>()
                    .filter(|v| !v.is_empty())
                    .map(|v| format!("({})", v.join(" OR "))),
                Op::Between if f.values.len() == 2 => {
                    match (range(&f.values[0]), range(&f.values[1])) {
                        (Some((a, _)), Some((_, b))) => Some(format!(
                            "{col} >= {} AND {col} < {}",
                            utc_of(&a, &m.time_zone),
                            utc_of(&b, &m.time_zone)
                        )),
                        _ => None,
                    }
                }
                Op::Ge | Op::Gt | Op::Le | Op::Lt if f.values.len() == 1 => range(&f.values[0])
                    .map(|(a, b)| match f.op {
                        Op::Ge => format!("{col} >= {}", utc_of(&a, &m.time_zone)),
                        Op::Gt => format!("{col} >= {}", utc_of(&b, &m.time_zone)),
                        Op::Lt => format!("{col} < {}", utc_of(&a, &m.time_zone)),
                        _ => format!("{col} < {}", utc_of(&b, &m.time_zone)),
                    }),
                _ => None,
            };
            if let Some(r) = ranged {
                wheres.push(r);
                continue;
            }
        }
        let lhs = level_sql(&f.dimension, &f.level)?;
        wheres.push(cmp_sql(&lhs, f.op, &f.values)?);
    }
    // Measures as partial aggregates.
    let mut parts = Vec::new();
    let mut outs = Vec::new();
    let mut distinct = false;
    for name in &q.measures {
        let (_, me) = m
            .measure(name)
            .ok_or_else(|| unknown(format!("unknown measure '{name}'")))?;
        let x = expr::qualify(&me.expr, "f");
        let x = match &me.filter {
            Some(fx) => format!("CASE WHEN {} THEN {x} END", expr::qualify(fx, "f")),
            None => x,
        };
        let col = parts.len();
        match me.agg {
            Agg::Sum => parts.push(format!("SUM({x})")),
            Agg::Count => parts.push(format!("COUNT({x})")),
            Agg::CountDistinct => {
                distinct = true;
                parts.push(format!("COUNT(DISTINCT {x})"));
            }
            Agg::Min => parts.push(format!("MIN({x})")),
            Agg::Max => parts.push(format!("MAX({x})")),
            Agg::Avg => {
                parts.push(format!("SUM({x})"));
                parts.push(format!("COUNT({x})"));
            }
        }
        outs.push(MeasureOut { agg: me.agg, col });
    }
    let statement = |n: usize| {
        let mut sel: Vec<String> = keys[..n]
            .iter()
            .enumerate()
            .map(|(i, k)| format!("{k} AS k{}", i + 1))
            .collect();
        sel.extend(
            parts
                .iter()
                .enumerate()
                .map(|(i, p)| format!("{p} AS p{}", i + 1)),
        );
        let mut s = format!(
            "SELECT {} FROM {db}.{} AS f",
            sel.join(", "),
            ident(&fact.table)
        );
        for j in &joins {
            s.push(' ');
            s.push_str(j);
        }
        if !wheres.is_empty() {
            s.push_str(" WHERE ");
            s.push_str(&wheres.join(" AND "));
        }
        if n > 0 {
            let g: Vec<String> = (1..=n).map(|i| format!("k{i}")).collect();
            s.push_str(" GROUP BY ");
            s.push_str(&g.join(", "));
        }
        s
    };
    let per_level = q.totals && distinct;
    Ok(Compiled {
        plan: if per_level { "per_level" } else { "local" },
        sql: statement(keys.len()),
        levels: if per_level {
            (0..keys.len()).map(statement).collect()
        } else {
            Vec::new()
        },
        keys: keys.len(),
        measures: outs,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn model() -> Model {
        Model::parse(
            "shop",
            &json!({
                "model": "sales", "time_zone": "Asia/Kolkata",
                "dimensions": [
                    {"name": "store", "table": "store", "key": "store_id", "scd": 2,
                     "hierarchies": [{"name": "geo", "levels": ["country", "region", "city", "store_id"]}]},
                    {"name": "date", "builtin": "time"}
                ],
                "facts": [{"name": "sales", "table": "sales_line", "key": ["order_id", "line_no"],
                    "links": [{"dimension": "store", "column": "store_id", "attribution": "at_fact_time", "time": "sold_at"},
                              {"dimension": "date", "column": "sold_at"}],
                    "measures": [{"name": "revenue", "expr": "amount", "agg": "sum"},
                                 {"name": "buyers", "expr": "customer_id", "agg": "count_distinct"},
                                 {"name": "paid", "expr": "amount", "agg": "sum", "filter": "status = 'paid'"},
                                 {"name": "avg_qty", "expr": "qty", "agg": "avg"}]}]
            }),
        )
        .unwrap()
    }

    #[test]
    fn the_chapters_example() {
        let m = model();
        let q = Query::parse(&json!({
            "model": "sales", "measures": ["revenue", "paid"],
            "dimensions": [{"dimension": "store", "level": "region"}, {"dimension": "date", "level": "month"}],
            "filters": [{"dimension": "store", "level": "country", "op": "in", "values": ["IN"]},
                        {"dimension": "date", "level": "day", "op": "between", "values": ["2026-01-01", "2026-06-30"]}],
            "totals": true, "order": [{"measure": "revenue", "desc": true}], "limit": 100
        }))
        .unwrap();
        let c = compile(&m, &q).unwrap();
        assert_eq!(c.plan, "local");
        assert_eq!(
            c.sql,
            "SELECT d1.`region` AS k1, EXTRACT(YEAR_MONTH FROM CONVERT_TZ(f.`sold_at`, '+00:00', 'Asia/Kolkata')) AS k2, \
             SUM(f.`amount`) AS p1, SUM(CASE WHEN f.`status` = 'paid' THEN f.`amount` END) AS p2 \
             FROM `shop`.`sales_line` AS f \
             LEFT JOIN `shop`.`store` FOR SYSTEM_TIME ALL AS d1 ON d1.`store_id` = f.`store_id` \
             AND f.`sold_at` >= d1.ROW_START AND f.`sold_at` < d1.ROW_END \
             WHERE d1.`country` IN ('IN') AND f.`sold_at` >= CONVERT_TZ('2026-01-01 00:00:00', 'Asia/Kolkata', '+00:00') \
             AND f.`sold_at` < CONVERT_TZ('2026-07-01 00:00:00', 'Asia/Kolkata', '+00:00') GROUP BY k1, k2"
        );
        // count_distinct with totals: one statement per rollup level.
        let q = Query::parse(&json!({"model": "sales", "measures": ["buyers", "avg_qty"],
            "dimensions": [{"dimension": "store", "level": "region"}], "totals": true, "attribution": "current"}))
        .unwrap();
        let c = compile(&m, &q).unwrap();
        assert_eq!(c.plan, "per_level");
        assert_eq!(c.levels.len(), 1);
        assert!(c.levels[0].starts_with("SELECT COUNT(DISTINCT f.`customer_id`) AS p1, SUM(f.`qty`) AS p2, COUNT(f.`qty`) AS p3 FROM"));
        assert!(!c.sql.contains("FOR SYSTEM_TIME"));
        assert_eq!(
            c.measures[1],
            MeasureOut {
                agg: Agg::Avg,
                col: 1
            }
        );
        // Unknown names.
        let bad = Query::parse(&json!({"model": "sales", "measures": ["nope"]})).unwrap();
        assert_eq!(
            compile(&m, &bad).unwrap_err().code(),
            Some(SprigCode::DimUnknownMember)
        );
        let bad = Query::parse(&json!({"model": "sales", "measures": ["revenue"], "dimensions": [{"dimension": "store", "level": "planet"}]})).unwrap();
        assert!(compile(&m, &bad).is_err());
    }

    #[test]
    fn periods() {
        assert_eq!(
            period("day", &json!("2024-02-28")).unwrap().1,
            "2024-02-29 00:00:00"
        );
        assert_eq!(
            period("day", &json!("2023-12-31")).unwrap().1,
            "2024-01-01 00:00:00"
        );
        assert_eq!(
            period("month", &json!(202_612)).unwrap(),
            ("2026-12-01 00:00:00".into(), "2027-01-01 00:00:00".into())
        );
        assert_eq!(
            period("year", &json!("2026")).unwrap().0,
            "2026-01-01 00:00:00"
        );
        assert!(period("week", &json!("2026")).is_none());
    }
}
