//! The statistics reader (chapter 13, "Statistics"; build step 3): MariaDB's
//! engine-independent statistics, the `JSON_HB` histogram parser, the
//! selectivity rules and the analyze job.
//!
//! [`reader`] loads one physical table's statistics into a [`TableStats`]:
//! `mysql.table_stats`, `mysql.column_stats` and `mysql.index_stats`,
//! `InnoDB`'s `n_rows`, the column types and existing indexes from
//! `information_schema`, and the table's size. [`histogram`] parses
//! `JSON_HB`. [`analyze`](mod@analyze) decides when statistics are missing or stale
//! and collects them with `ANALYZE LOCAL TABLE ... PERSISTENT FOR`,
//! throttled.
//!
//! **Selectivity** (fractions of all rows, clamped to `[1/N, 1]`):
//!
//! ```text
//! sel_eq(c, values):  with a JSON_HB histogram and sampled values:
//!                       (1 - nulls_ratio) * mean over v of Histogram::point(v)
//!                     else: avg_frequency / cardinality
//!                     else (no EITS row): 1 / SHOW INDEX cardinality of an
//!                       index starting with c, else 0.10
//! sel_in(c, values):  min(1, sum of sel_eq(v)), or min(1, k * sel_eq(c))
//! sel_range(c, lo, hi): (1 - nulls_ratio) * Histogram::range(lo, hi);
//!                     0.10 without a histogram or values
//! sel_null(c):        nulls_ratio;   sel_not_null(c): 1 - nulls_ratio
//! sel(prefix):        avg_frequency / cardinality of index_stats when an
//!                     existing index's first columns are the same set,
//!                     else the product of the parts
//! ```
//!
//! Two corrections to chapter 13's text, measured on 11.8.9: `JSON_HB`
//! bucket sizes and `avg_frequency` are over **non-NULL** rows, so the
//! histogram estimate is scaled by `1 - nulls_ratio` and the
//! `avg_frequency` estimate is not (the chapter multiplies the latter).

pub mod analyze;
pub mod histogram;
pub mod reader;

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub use analyze::{AnalyzeLimits, AnalyzePlan, AnalyzeThrottle, analyze, plan_analyze};
pub use histogram::{Bucket, Histogram, Key, KeyClass, temporal_seconds};
pub use reader::{StatsReader, weigh};

use super::model::IndexInfo;

/// The selectivity used where nothing better is known (chapter 13: a
/// range without a histogram or values).
pub const DEFAULT_SEL: f64 = 0.10;

/// A sampled parameter value. Values never leave the process that
/// decrypted them; only selectivities are stored.
#[derive(Debug, Clone, PartialEq)]
pub enum Param {
    /// `NULL`.
    Null,
    /// A number.
    Num(f64),
    /// Text, a date or a time as written.
    Text(String),
    /// Text already turned into its column's `WEIGHT_STRING()`
    /// ([`weigh_samples`](super::candidates::weigh_samples)), so it compares
    /// with reweighed histogram bounds.
    Weight(Vec<u8>),
}

/// A column's type, from `information_schema.COLUMNS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    /// Lower-case name.
    pub name: String,
    /// `DATA_TYPE`, lower case: `int`, `varchar`, `datetime`, ...
    pub data_type: String,
    /// How values compare, `None` for types histograms don't order
    /// usefully (`blob`, `json`, `geometry`, ...).
    pub class: Option<KeyClass>,
    /// `CHARACTER_SET_NAME`.
    pub charset: Option<String>,
    /// `COLLATION_NAME`.
    pub collation: Option<String>,
    /// `IS_NULLABLE`.
    pub nullable: bool,
    /// Bytes of a fixed-width type (`int` 4, `datetime` 5), or the
    /// largest a variable one can take (`CHARACTER_OCTET_LENGTH`).
    pub max_bytes: Option<u64>,
    /// A `BLOB`, `TEXT`, `JSON` or `GEOMETRY` column: never added for
    /// covering, and only indexable with a prefix.
    pub lob: bool,
    /// A `VIRTUAL` or `PERSISTENT` generated column (field columns).
    pub generated: bool,
}

/// One column's engine-independent statistics.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStats {
    /// `nulls_ratio`.
    pub nulls_ratio: f64,
    /// `avg_length`, bytes.
    pub avg_length: f64,
    /// `avg_frequency`: non-NULL rows per distinct value.
    pub avg_frequency: f64,
    /// The `JSON_HB` histogram, if any.
    pub histogram: Option<Histogram>,
}

/// Where a selectivity came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelSource {
    /// A `JSON_HB` histogram with sampled values.
    Histogram,
    /// `avg_frequency / cardinality`.
    AvgFrequency,
    /// `mysql.index_stats` of an existing index.
    IndexStats,
    /// `SHOW INDEX` cardinality (no access to the statistics tables).
    ShowIndex,
    /// `nulls_ratio`.
    NullsRatio,
    /// [`DEFAULT_SEL`].
    Default,
}

impl SelSource {
    /// The text form kept in evidence.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            SelSource::Histogram => "histogram",
            SelSource::AvgFrequency => "avg_frequency",
            SelSource::IndexStats => "index_stats",
            SelSource::ShowIndex => "show_index",
            SelSource::NullsRatio => "nulls_ratio",
            SelSource::Default => "default",
        }
    }
}

/// A selectivity and its source.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sel {
    /// Fraction of all rows.
    pub value: f64,
    /// Where it came from.
    pub source: SelSource,
}

/// Everything the generator and the cost model know about one physical
/// table.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TableStats {
    /// The physical schema.
    pub schema: String,
    /// The table.
    pub table: String,
    /// `mysql.table_stats.cardinality`: rows when statistics were
    /// collected.
    pub cardinality: Option<u64>,
    /// `InnoDB`'s `n_rows` now (`mysql.innodb_table_stats`, else
    /// `information_schema.TABLES.TABLE_ROWS`).
    pub n_rows: Option<u64>,
    /// `DATA_LENGTH`.
    pub data_bytes: u64,
    /// `INDEX_LENGTH`.
    pub index_bytes: u64,
    /// Column types, by lower-case name.
    pub columns: BTreeMap<String, ColumnInfo>,
    /// Column statistics, by lower-case name.
    pub column_stats: BTreeMap<String, ColumnStats>,
    /// `mysql.index_stats.avg_frequency` by index name, for prefix arity
    /// 1, 2, ... (the primary-key columns `InnoDB` appends included).
    pub index_stats: BTreeMap<String, Vec<f64>>,
    /// `information_schema.STATISTICS.CARDINALITY` by index name and
    /// prefix arity: the fallback without statistics tables.
    pub show_index: BTreeMap<String, Vec<Option<u64>>>,
    /// Existing indexes (origins as derived from the names; the caller
    /// applies recorded origins).
    pub indexes: Vec<IndexInfo>,
    /// Primary-key columns, in order.
    pub primary_key: Vec<String>,
    /// The `mysql.*` statistics tables could be read.
    pub eits: bool,
}

#[allow(clippy::cast_precision_loss)]
fn f(n: u64) -> f64 {
    n as f64
}

impl TableStats {
    /// Rows now, at least 1: `InnoDB`'s `n_rows`, else the statistics'
    /// cardinality.
    #[must_use]
    pub fn rows(&self) -> f64 {
        f(self.n_rows.or(self.cardinality).unwrap_or(1).max(1))
    }

    /// Rows the statistics were collected over, at least 1.
    #[must_use]
    pub fn stats_rows(&self) -> f64 {
        f(self.cardinality.or(self.n_rows).unwrap_or(1).max(1))
    }

    /// Data plus all indexes, bytes: `size(T)` of the storage budget.
    #[must_use]
    pub fn size_bytes(&self) -> u64 {
        self.data_bytes.saturating_add(self.index_bytes)
    }

    fn clamp(&self, s: f64) -> f64 {
        let lo = 1.0 / self.rows();
        if s.is_nan() { 1.0 } else { s.clamp(lo, 1.0) }
    }

    /// `value` as a key of `column`, `None` if it can't be compared (a
    /// `NULL`, an unknown column, a type mismatch).
    #[must_use]
    pub fn key(&self, column: &str, value: &Param) -> Option<Key> {
        let class = self.columns.get(column)?.class?;
        match (class, value) {
            (KeyClass::Numeric, Param::Num(x)) => Some(Key::Num(*x)),
            (KeyClass::Numeric | KeyClass::Temporal | KeyClass::Text, Param::Text(s)) => {
                Key::parse(class, s)
            }
            (KeyClass::Text, Param::Num(x)) => Key::parse(class, &x.to_string()),
            (KeyClass::Text, Param::Weight(w)) => Some(Key::Bytes(w.clone())),
            (_, Param::Null)
            | (KeyClass::Temporal, Param::Num(_))
            | (KeyClass::Numeric | KeyClass::Temporal, Param::Weight(_)) => None,
        }
    }

    /// The average length of `column` in bytes: `avg_length` from the
    /// statistics, else the type's size, else 8.
    #[must_use]
    pub fn avg_length(&self, column: &str) -> f64 {
        if let Some(s) = self.column_stats.get(column)
            && s.avg_length > 0.0
        {
            return s.avg_length;
        }
        self.columns
            .get(column)
            .and_then(|c| c.max_bytes)
            .map_or(8.0, |b| f(b.min(64)))
    }

    /// The primary key's length in bytes (`InnoDB`'s secondary entries carry
    /// it); 6 for the hidden row id of a table without one.
    #[must_use]
    pub fn pk_length(&self) -> f64 {
        if self.primary_key.is_empty() {
            return 6.0;
        }
        self.primary_key.iter().map(|c| self.avg_length(c)).sum()
    }

    /// Equality on `column` with sampled `values` (empty: no samples).
    #[must_use]
    pub fn sel_eq(&self, column: &str, values: &[Key]) -> Sel {
        let Some(s) = self.column_stats.get(column) else {
            return self.fallback_eq(column);
        };
        if let (Some(h), false) = (&s.histogram, values.is_empty()) {
            let mean = values.iter().map(|v| h.point(v)).sum::<f64>() / f(values.len() as u64);
            return Sel {
                value: self.clamp((1.0 - s.nulls_ratio) * mean),
                source: SelSource::Histogram,
            };
        }
        if s.avg_frequency > 0.0 {
            return Sel {
                value: self.clamp(s.avg_frequency / self.stats_rows()),
                source: SelSource::AvgFrequency,
            };
        }
        self.fallback_eq(column)
    }

    fn fallback_eq(&self, column: &str) -> Sel {
        let card = self.indexes.iter().find_map(|i| {
            (i.parts.first().map(|p| p.column.as_str()) == Some(column))
                .then(|| self.show_index.get(&i.name)?.first().copied().flatten())
                .flatten()
        });
        match card {
            Some(c) if c > 0 => Sel {
                value: self.clamp(1.0 / f(c)),
                source: SelSource::ShowIndex,
            },
            _ => Sel {
                value: DEFAULT_SEL,
                source: SelSource::Default,
            },
        }
    }

    /// `IN` on `column`: with sampled `values` (one sample's list), the
    /// sum of their equalities; else `k` times the column's equality.
    #[must_use]
    pub fn sel_in(&self, column: &str, values: &[Key], k: u64) -> Sel {
        if values.is_empty() {
            let one = self.sel_eq(column, &[]);
            return Sel {
                value: self.clamp(f(k.max(1)) * one.value),
                source: one.source,
            };
        }
        let mut source = SelSource::Default;
        let mut total = 0.0;
        for v in values {
            let s = self.sel_eq(column, std::slice::from_ref(v));
            total += s.value;
            source = s.source;
        }
        Sel {
            value: self.clamp(total),
            source,
        }
    }

    /// A range on `column` from `lo` to `hi` (either open).
    #[must_use]
    pub fn sel_range(&self, column: &str, lo: Option<&Key>, hi: Option<&Key>) -> Sel {
        let s = self.column_stats.get(column);
        match (
            s.and_then(|s| s.histogram.as_ref()),
            lo.is_some() || hi.is_some(),
        ) {
            (Some(h), true) => {
                let nn = 1.0 - s.map_or(0.0, |s| s.nulls_ratio);
                Sel {
                    value: self.clamp(nn * h.range(lo, hi)),
                    source: SelSource::Histogram,
                }
            }
            _ => Sel {
                value: DEFAULT_SEL,
                source: SelSource::Default,
            },
        }
    }

    /// `IS NULL` on `column`.
    #[must_use]
    pub fn sel_null(&self, column: &str) -> Sel {
        match self.column_stats.get(column) {
            Some(s) => Sel {
                value: self.clamp(s.nulls_ratio),
                source: SelSource::NullsRatio,
            },
            None => Sel {
                value: DEFAULT_SEL,
                source: SelSource::Default,
            },
        }
    }

    /// `IS NOT NULL` on `column`.
    #[must_use]
    pub fn sel_not_null(&self, column: &str) -> Sel {
        match self.column_stats.get(column) {
            Some(s) => Sel {
                value: self.clamp(1.0 - s.nulls_ratio),
                source: SelSource::NullsRatio,
            },
            None => Sel {
                value: 1.0,
                source: SelSource::Default,
            },
        }
    }

    /// The combined selectivity of equality columns `columns` with
    /// per-part selectivities `parts`: `avg_frequency / cardinality` from
    /// `index_stats` when an existing index's first `columns.len()`
    /// columns are the same set, else the product of `parts`.
    #[must_use]
    pub fn sel_prefix(&self, columns: &[&str], parts: &[f64]) -> Sel {
        let k = columns.len();
        if k > 0 {
            for i in &self.indexes {
                if i.parts.len() < k {
                    continue;
                }
                let mut a: Vec<&str> = i.parts[..k].iter().map(|p| p.column.as_str()).collect();
                let mut b = columns.to_vec();
                a.sort_unstable();
                b.sort_unstable();
                if a != b {
                    continue;
                }
                if let Some(freq) = self.index_stats.get(&i.name).and_then(|v| v.get(k - 1))
                    && *freq > 0.0
                {
                    return Sel {
                        value: self.clamp(freq / self.stats_rows()),
                        source: SelSource::IndexStats,
                    };
                }
            }
        }
        Sel {
            value: self.clamp(parts.iter().product()),
            source: SelSource::AvgFrequency,
        }
    }

    /// The columns among `wanted` with no statistics row, or with no
    /// histogram where one is wanted (`histogram` set).
    #[must_use]
    pub fn missing(&self, wanted: &[&str], histogram: bool) -> Vec<String> {
        wanted
            .iter()
            .filter(|c| {
                self.columns
                    .get(**c)
                    .is_some_and(|i| i.class.is_some() || !i.lob)
            })
            .filter(|c| match self.column_stats.get(**c) {
                None => true,
                Some(s) => histogram && s.histogram.is_none(),
            })
            .map(|c| (*c).to_owned())
            .collect()
    }

    /// Statistics were collected before the table changed by more than
    /// `ratio` (chapter 13: 0.2) according to `InnoDB`'s `n_rows`.
    #[must_use]
    pub fn is_stale(&self, ratio: f64) -> bool {
        match (self.cardinality, self.n_rows) {
            (Some(c), Some(n)) => {
                let c = f(c.max(1));
                (f(n) - c).abs() / c > ratio
            }
            (None, _) => true,
            (Some(_), None) => false,
        }
    }
}

#[cfg(test)]
mod tests;
