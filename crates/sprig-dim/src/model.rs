//! Model definitions: parsing and validation. A model is the JSON
//! document `Analytics.CreateModel` takes.

use serde_json::Value;
use sprig_common::{Result, SprigCode, SprigError};

use crate::expr;

/// How a fact row picks its dimension row (type 2 dimensions).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Attribution {
    /// The dimension's current row.
    Current,
    /// The row valid when the fact happened.
    AtFactTime,
}

impl Attribution {
    fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "" | "current" => Ok(Self::Current),
            "at_fact_time" => Ok(Self::AtFactTime),
            other => Err(invalid(format!(
                "attribution '{other}': use current or at_fact_time"
            ))),
        }
    }
}

/// A dimension's history (type 2 dimensions with system versioning).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Versioning {
    /// No history.
    None,
    /// MariaDB system versioning (`ROW_START`, `ROW_END`).
    System,
    /// An application-time period over two columns.
    Application {
        /// Valid from.
        from: String,
        /// Valid to (exclusive).
        to: String,
    },
}

/// A dimension.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DimKind {
    /// A table with levels as columns.
    Table {
        /// The table, in the model's database.
        table: String,
        /// Its key column.
        key: String,
        /// The levels, coarsest first (one hierarchy).
        levels: Vec<String>,
        /// Its history.
        versioning: Versioning,
    },
    /// The built-in time dimension over a fact's time column.
    Time,
}

/// A dimension of a model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dimension {
    /// Its name.
    pub name: String,
    /// What it is.
    pub kind: DimKind,
}

/// The time levels, coarsest first.
pub const TIME_LEVELS: &[&str] = &[
    "year",
    "fiscal_year",
    "quarter",
    "month",
    "week",
    "day",
    "hour",
];

impl Dimension {
    /// The levels, coarsest first.
    #[must_use]
    pub fn levels(&self) -> Vec<String> {
        match &self.kind {
            DimKind::Table { levels, .. } => levels.clone(),
            DimKind::Time => TIME_LEVELS.iter().map(|s| (*s).to_owned()).collect(),
        }
    }
}

/// How a fact reaches a dimension.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Link {
    /// The dimension.
    pub dimension: String,
    /// The fact's column: the dimension's key, or the time column.
    pub column: String,
    /// Which dimension row a fact sees.
    pub attribution: Attribution,
    /// For `at_fact_time`: the fact's time column.
    pub time: Option<String>,
}

/// An aggregate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Agg {
    /// `SUM`.
    Sum,
    /// `COUNT`.
    Count,
    /// `COUNT(DISTINCT ...)`.
    CountDistinct,
    /// `MIN`.
    Min,
    /// `MAX`.
    Max,
    /// `AVG`, as a sum over a count.
    Avg,
}

impl Agg {
    fn parse(s: &str) -> Result<Self> {
        Ok(match s.to_ascii_lowercase().as_str() {
            "sum" => Self::Sum,
            "count" => Self::Count,
            "count_distinct" => Self::CountDistinct,
            "min" => Self::Min,
            "max" => Self::Max,
            "avg" => Self::Avg,
            other => {
                return Err(invalid(format!(
                    "agg '{other}': use sum, count, count_distinct, min, max or avg"
                )));
            }
        })
    }
}

/// A measure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Measure {
    /// Its name, unique in the model.
    pub name: String,
    /// SQL over the fact row, checked by [`expr`].
    pub expr: String,
    /// The aggregate.
    pub agg: Agg,
    /// Only rows where this holds count.
    pub filter: Option<String>,
}

/// A fact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fact {
    /// Its name.
    pub name: String,
    /// The table, in the model's database.
    pub table: String,
    /// The grain.
    pub key: Vec<String>,
    /// Its dimensions.
    pub links: Vec<Link>,
    /// Its measures.
    pub measures: Vec<Measure>,
}

/// A model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Model {
    /// Its name.
    pub name: String,
    /// The logical database holding its tables.
    pub database: String,
    /// The zone of the time dimension (`UTC`, `+05:30`, `Asia/Kolkata`).
    pub time_zone: String,
    /// First month of the fiscal year.
    pub fiscal_start_month: u8,
    /// Dimensions.
    pub dimensions: Vec<Dimension>,
    /// Facts.
    pub facts: Vec<Fact>,
}

/// 39752.
pub fn invalid(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::DimInvalidModel, m)
}

fn name_ok(n: &str) -> bool {
    !n.is_empty() && n.len() <= 64 && n.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn ident(v: &Value, k: &str, what: &str) -> Result<String> {
    let s = v.get(k).and_then(Value::as_str).unwrap_or("").to_owned();
    if !name_ok(&s) {
        return Err(invalid(format!(
            "{what} '{s}' must be 1 to 64 letters, digits or underscores"
        )));
    }
    Ok(s)
}

fn strs(v: Option<&Value>) -> Vec<String> {
    match v {
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        Some(Value::String(s)) => vec![s.clone()],
        _ => Vec::new(),
    }
}

impl Model {
    /// Parses a definition (the model document as JSON) for `database`.
    ///
    /// # Errors
    /// 39752 with what is wrong.
    #[allow(clippy::too_many_lines)]
    pub fn parse(database: &str, v: &Value) -> Result<Self> {
        if !name_ok(database) {
            return Err(invalid(format!("database '{database}'")));
        }
        let name = ident(v, "model", "model")?;
        let tz = v
            .get("time_zone")
            .and_then(Value::as_str)
            .unwrap_or("UTC")
            .to_owned();
        if tz.is_empty()
            || tz.len() > 64
            || !tz
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"/_+-:".contains(&b))
        {
            return Err(invalid(format!("time_zone '{tz}'")));
        }
        let fsm = u8::try_from(
            v.get("fiscal_start_month")
                .and_then(Value::as_u64)
                .unwrap_or(1),
        )
        .unwrap_or(0);
        if !(1..=12).contains(&fsm) {
            return Err(invalid("fiscal_start_month must be 1 to 12"));
        }
        let mut dimensions = Vec::new();
        for d in v
            .get("dimensions")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let dname = ident(d, "name", "dimension")?;
            let kind = match d.get("builtin").and_then(Value::as_str) {
                Some("time") => DimKind::Time,
                Some(other) => {
                    return Err(invalid(format!(
                        "builtin dimension '{other}': this release has time (geography needs cell columns, not built)"
                    )));
                }
                None => {
                    let table = ident(d, "table", "table")?;
                    let key = ident(d, "key", "key")?;
                    let mut levels = Vec::new();
                    for h in d
                        .get("hierarchies")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                    {
                        if !levels.is_empty() {
                            return Err(invalid(format!(
                                "dimension '{dname}': one hierarchy per dimension in this release"
                            )));
                        }
                        levels = strs(h.get("levels"));
                    }
                    if levels.is_empty() {
                        levels = strs(d.get("levels"));
                    }
                    if levels.is_empty() {
                        levels = vec![key.clone()];
                    }
                    for l in &levels {
                        if !name_ok(l) {
                            return Err(invalid(format!("level '{l}' of '{dname}'")));
                        }
                    }
                    let scd = d.get("scd").and_then(Value::as_u64).unwrap_or(1);
                    let versioning = match d.get("versioning").and_then(Value::as_str) {
                        Some("application") => Versioning::Application {
                            from: ident(d, "valid_from", "valid_from")?,
                            to: ident(d, "valid_to", "valid_to")?,
                        },
                        Some("system") => Versioning::System,
                        None if scd == 2 => Versioning::System,
                        Some("none") | None => Versioning::None,
                        Some(other) => {
                            return Err(invalid(format!(
                                "versioning '{other}': use none, system or application (bitemporal isn't built)"
                            )));
                        }
                    };
                    DimKind::Table {
                        table,
                        key,
                        levels,
                        versioning,
                    }
                }
            };
            if dimensions.iter().any(|x: &Dimension| x.name == dname) {
                return Err(invalid(format!("dimension '{dname}' twice")));
            }
            dimensions.push(Dimension { name: dname, kind });
        }
        let mut facts = Vec::new();
        let mut measure_names = std::collections::HashSet::new();
        for f in v
            .get("facts")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let fname = ident(f, "name", "fact")?;
            let table = ident(f, "table", "table")?;
            let key = strs(f.get("key"));
            let mut links = Vec::new();
            for l in f
                .get("links")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let dim = ident(l, "dimension", "dimension")?;
                if !dimensions.iter().any(|d| d.name == dim) {
                    return Err(invalid(format!(
                        "fact '{fname}' links unknown dimension '{dim}'"
                    )));
                }
                let time = match l.get("time").and_then(Value::as_str) {
                    Some(t) if name_ok(t) => Some(t.to_owned()),
                    Some(t) => return Err(invalid(format!("time column '{t}'"))),
                    None => None,
                };
                let attribution =
                    Attribution::parse(l.get("attribution").and_then(Value::as_str).unwrap_or(""))?;
                if attribution == Attribution::AtFactTime && time.is_none() {
                    return Err(invalid(format!(
                        "link to '{dim}': at_fact_time needs the fact's time column"
                    )));
                }
                links.push(Link {
                    dimension: dim,
                    column: ident(l, "column", "column")?,
                    attribution,
                    time,
                });
            }
            let mut measures = Vec::new();
            for m in f
                .get("measures")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let mname = ident(m, "name", "measure")?;
                if !measure_names.insert(mname.clone()) {
                    return Err(invalid(format!("measure '{mname}' twice in the model")));
                }
                let e = m
                    .get("expr")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                expr::check(&e).map_err(|x| invalid(format!("measure '{mname}': {x}")))?;
                let filter = m.get("filter").and_then(Value::as_str).map(str::to_owned);
                if let Some(fx) = &filter {
                    expr::check(fx)
                        .map_err(|x| invalid(format!("measure '{mname}' filter: {x}")))?;
                }
                measures.push(Measure {
                    name: mname,
                    expr: e,
                    agg: Agg::parse(m.get("agg").and_then(Value::as_str).unwrap_or(""))?,
                    filter,
                });
            }
            facts.push(Fact {
                name: fname,
                table,
                key,
                links,
                measures,
            });
        }
        if facts.is_empty() {
            return Err(invalid("a model needs a fact"));
        }
        Ok(Self {
            name,
            database: database.to_owned(),
            time_zone: tz,
            fiscal_start_month: fsm,
            dimensions,
            facts,
        })
    }

    /// The dimension named `n`.
    #[must_use]
    pub fn dimension(&self, n: &str) -> Option<&Dimension> {
        self.dimensions.iter().find(|d| d.name == n)
    }

    /// The fact holding measure `n`, and the measure.
    #[must_use]
    pub fn measure(&self, n: &str) -> Option<(&Fact, &Measure)> {
        self.facts
            .iter()
            .find_map(|f| f.measures.iter().find(|m| m.name == n).map(|m| (f, m)))
    }

    /// Every column the model reads, as `(table, column)`, for validation
    /// against the database.
    #[must_use]
    pub fn columns(&self) -> Vec<(String, String)> {
        let mut out = Vec::new();
        for d in &self.dimensions {
            if let DimKind::Table {
                table,
                key,
                levels,
                versioning,
            } = &d.kind
            {
                out.push((table.clone(), key.clone()));
                for l in levels {
                    out.push((table.clone(), l.clone()));
                }
                if let Versioning::Application { from, to } = versioning {
                    out.push((table.clone(), from.clone()));
                    out.push((table.clone(), to.clone()));
                }
            }
        }
        for f in &self.facts {
            for k in &f.key {
                out.push((f.table.clone(), k.clone()));
            }
            for l in &f.links {
                out.push((f.table.clone(), l.column.clone()));
                if let Some(t) = &l.time {
                    out.push((f.table.clone(), t.clone()));
                }
            }
            for m in &f.measures {
                for c in expr::columns(&m.expr)
                    .into_iter()
                    .chain(m.filter.as_deref().map(expr::columns).unwrap_or_default())
                {
                    out.push((f.table.clone(), c));
                }
            }
        }
        out.sort();
        out.dedup();
        out
    }
}
