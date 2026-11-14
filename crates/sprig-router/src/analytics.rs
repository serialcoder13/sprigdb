//! Analytic reads: a `SELECT` the
//! router would gather runs in DataFusion inside the gateway instead, over
//! streams of the shards' rows, when its guard can show the answer is the
//! one MariaDB gives.
//!
//! ```text
//! choose(statement the planner gathers):
//!   router.analytics.mode = off      → gather
//!   hint /*+ SPRIG_GATHER */         → gather
//!   mode = prefer, or hint /*+ SPRIG_ANALYTICS */
//!                                    → analytic read; refused → gather
//!   mode = fallback (default)        → gather; past router.gather.max_rows (39421),
//!                                      or gathering off → analytic read; refused → the
//!                                      gather's error
//! analytic read:
//!   1. each table's SHOW CREATE TABLE (one shard)        → its columns
//!   2. the guard (translate.rs) over the statement       → scans, joins, groups, items
//!   3. on a scratch connection: empty temporary copies, the statement itself
//!      (MariaDB's own column definitions of the answer) and each scan's columns
//!   4. the keys (keys.rs) each column needs, from those definitions; the answer's
//!      types checked against what the plan computes
//!   5. one stream per shard: SELECT value, key… FROM shard.t AS a WHERE pushed…
//!   6. DataFusion over the streams: joins on keys, GROUP BY keys, aggregates in
//!      exact integer and DECIMAL arithmetic, windows, ORDER BY keys, LIMIT
//!   7. the rows rendered as MariaDB prints those types, then sent
//! ```
//!
//! The answer is buffered (charged to `router.max_statement_memory_mb`) so
//! a refusal found while running still falls back with nothing sent.
//! DataFusion runs with a memory pool of `analytics.datafusion_memory_mb`
//! and spills sorts and aggregates to disk up to `router.analytics.spill_mb`
//! (0: no spilling); past them, and past `router.analytics.max_rows` rows
//! read from the shards, the statement fails with 39421. `KILL QUERY` drops
//! the statement's future, which drops DataFusion's tasks and the shard
//! streams with it.

pub(crate) mod keys;
pub(crate) mod translate;

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use bytes::Bytes;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, Decimal128Array, Decimal128Builder, Int64Array,
    Int64Builder, StringBuilder, UInt64Array,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::error::DataFusionError;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::{SessionConfig, SessionContext};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::ColumnDef;
use sprig_proto_mysql::resultset::column_type as ct;

use self::keys::Kind;
use self::translate::{Agg, ColRef, Item, OrderKey, Query, TableCols, WinFn};
use crate::exec::{Budget, Output, Row, RowSink, ShardBackend, ShardOutcome, ShardStream};
use crate::gather::{GatherPlan, column_names_of_ddl, temp_ddl};

/// `router.analytics.mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AnalyticsMode {
    /// Never.
    #[default]
    Off,
    /// When a gathered read would copy too much, or is off, or a hint asks.
    Fallback,
    /// Before gathering, whenever the guard accepts.
    Prefer,
}

impl AnalyticsMode {
    /// From the setting's text (`off`, `fallback`, `prefer`).
    #[must_use]
    pub fn parse(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "prefer" => Self::Prefer,
            "fallback" => Self::Fallback,
            _ => Self::Off,
        }
    }
}

/// What an analytic read runs under (`router.analytics.*`,
/// `analytics.datafusion_memory_mb` and
/// `analytics.max_concurrent_per_gateway`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnalyticsLimits {
    /// `router.analytics.mode`.
    pub mode: AnalyticsMode,
    /// `analytics.datafusion_memory_mb`.
    pub memory_mb: u64,
    /// `router.analytics.spill_mb` (0: no spilling).
    pub spill_mb: u64,
    /// `router.analytics.max_rows`: rows read from the shards.
    pub max_rows: u64,
    /// `analytics.max_concurrent_per_gateway`.
    pub max_concurrent: usize,
}

impl Default for AnalyticsLimits {
    fn default() -> Self {
        Self {
            mode: AnalyticsMode::Off,
            memory_mb: 1024,
            spill_mb: 4096,
            max_rows: 50_000_000,
            max_concurrent: 8,
        }
    }
}

/// The statement's hint: `Some(true)` for `/*+ SPRIG_ANALYTICS */`,
/// `Some(false)` for `/*+ SPRIG_GATHER */`.
#[must_use]
pub fn hint(sql: &str) -> Option<bool> {
    let mut rest = sql;
    while let Some(i) = rest.find("/*+") {
        let body = &rest[i + 3..];
        let end = body.find("*/").unwrap_or(body.len());
        let words = body[..end].to_ascii_uppercase();
        for w in words.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_')) {
            match w {
                "SPRIG_ANALYTICS" => return Some(true),
                "SPRIG_GATHER" => return Some(false),
                _ => {}
            }
        }
        rest = &body[end..];
    }
    None
}

/// What an analytic read did.
#[derive(Debug)]
pub(crate) enum Outcome {
    /// Answered.
    Done(Output),
    /// Refused before anything was sent: why (the statement is gathered,
    /// or gets the gather's error).
    Refused(String),
}

#[allow(clippy::unnecessary_wraps)] // returned straight from `execute`
fn refused(why: impl Into<String>) -> Result<Outcome> {
    let why = why.into();
    tracing::debug!(why = %why, "analytic read refused");
    metrics::counter!("sprig_router_analytics_total", "result" => "refused").increment(1);
    Ok(Outcome::Refused(why))
}

fn limit_error(detail: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::PlanLimitExceeded, detail.into())
}

static GATE: OnceLock<tokio::sync::Semaphore> = OnceLock::new();

/// Rows per Arrow batch from one shard.
const BATCH_ROWS: usize = 8192;

/// One column a scan reads: its kind and where its value and key are in
/// the shard's row.
#[derive(Debug, Clone)]
struct ScanCol {
    name: String,
    kind: Kind,
    /// The plan compares, groups, sorts or adds it.
    keyed: bool,
    /// Position of the value in the shard's row.
    value_at: usize,
    /// Positions of the key's extra expressions (weights, Unix time).
    key_at: Vec<usize>,
}

/// One scan: its columns and the Arrow schema of its batches.
#[derive(Debug)]
struct ScanLayout {
    cols: Vec<ScanCol>,
    schema: SchemaRef,
}

impl ScanLayout {
    fn find(&self, col: &str) -> Option<(usize, &ScanCol)> {
        self.cols.iter().enumerate().find(|(_, c)| c.name == col)
    }
}

fn key_type(k: Kind) -> DataType {
    match k {
        Kind::Int => DataType::Int64,
        Kind::BigUnsigned => DataType::Decimal128(20, 0),
        Kind::Decimal { p, s } => DataType::Decimal128(p, s.cast_signed()),
        Kind::Str { .. } | Kind::Bytes | Kind::Opaque => DataType::Binary,
        Kind::Temporal { .. } => DataType::Utf8,
        Kind::Timestamp { fsp } => DataType::Decimal128(20 + fsp, fsp.cast_signed()),
    }
}

/// Builds one batch of a scan from shard rows.
fn build_batch(layout: &ScanLayout, rows: &[Row]) -> std::result::Result<RecordBatch, String> {
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for c in &layout.cols {
        let mut vb = BinaryBuilder::with_capacity(rows.len(), rows.len() * 8);
        for r in rows {
            match r.get(c.value_at).and_then(Option::as_ref) {
                Some(b) => vb.append_value(b),
                None => vb.append_null(),
            }
        }
        arrays.push(Arc::new(vb.finish()));
        if !c.keyed {
            continue;
        }
        let val = |r: &Row| r.get(c.value_at).and_then(Option::as_ref).cloned();
        let bad = |r: &Row| {
            format!(
                "{}: a value the key doesn't read: {:?}",
                c.name,
                val(r).map(|b| String::from_utf8_lossy(&b).into_owned())
            )
        };
        let a: ArrayRef = match c.kind {
            Kind::Int => {
                let mut b = Int64Builder::with_capacity(rows.len());
                for r in rows {
                    match val(r) {
                        None => b.append_null(),
                        Some(v) => b.append_value(
                            std::str::from_utf8(&v)
                                .ok()
                                .and_then(|t| t.parse::<i64>().ok())
                                .ok_or_else(|| bad(r))?,
                        ),
                    }
                }
                Arc::new(b.finish())
            }
            Kind::BigUnsigned | Kind::Decimal { .. } | Kind::Timestamp { .. } => {
                let DataType::Decimal128(p, s) = key_type(c.kind) else {
                    return Err("decimal key".into());
                };
                let scale = u8::try_from(s).map_err(|_| "scale")?;
                let mut b = Decimal128Builder::with_capacity(rows.len());
                for r in rows {
                    let src = match c.kind {
                        Kind::Timestamp { .. } => c
                            .key_at
                            .first()
                            .and_then(|&i| r.get(i))
                            .and_then(Option::as_ref)
                            .cloned(),
                        _ => val(r),
                    };
                    match src {
                        None => b.append_null(),
                        Some(v) => {
                            b.append_value(keys::parse_decimal(&v, scale).ok_or_else(|| bad(r))?);
                        }
                    }
                }
                Arc::new(
                    b.finish()
                        .with_precision_and_scale(p, s)
                        .map_err(|e| e.to_string())?,
                )
            }
            Kind::Str { coll } => {
                let mut b = BinaryBuilder::with_capacity(rows.len(), rows.len() * 16);
                for r in rows {
                    if val(r).is_none() {
                        b.append_null();
                        continue;
                    }
                    let lv = |k: usize| -> &[u8] {
                        c.key_at
                            .get(k)
                            .and_then(|&i| r.get(i))
                            .and_then(Option::as_ref)
                            .map_or(&[][..], |x| &x[..])
                    };
                    let key =
                        keys::encode_weights(coll, [lv(0), lv(1), lv(2)]).ok_or_else(|| bad(r))?;
                    b.append_value(key);
                }
                Arc::new(b.finish())
            }
            Kind::Bytes | Kind::Opaque => {
                let mut b = BinaryBuilder::with_capacity(rows.len(), rows.len() * 8);
                for r in rows {
                    match val(r) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            Kind::Temporal { .. } => {
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 20);
                for r in rows {
                    match val(r) {
                        Some(v) => b.append_value(std::str::from_utf8(&v).map_err(|_| bad(r))?),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
        };
        arrays.push(a);
    }
    RecordBatch::try_new(Arc::clone(&layout.schema), arrays).map_err(|e| e.to_string())
}

/// One shard's rows of one scan, as a DataFusion partition: the shard's
/// statement is sent when DataFusion first polls it.
struct ShardPart {
    schema: SchemaRef,
    state: Mutex<Option<PartState>>,
}

struct PartState {
    stream: Box<dyn ShardStream>,
    sql: String,
    layout: Arc<ScanLayout>,
    rows: Arc<AtomicU64>,
    max_rows: u64,
}

impl std::fmt::Debug for ShardPart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardPart").finish_non_exhaustive()
    }
}

fn external(e: SprigError) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

impl PartitionStream for ShardPart {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let st = self
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let schema = Arc::clone(&self.schema);
        let s = futures::stream::unfold((st, false), |(st, started)| async move {
            let mut st = st?;
            if !started {
                match st.stream.start(&st.sql).await {
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        return Some((
                            Err(external(SprigError::internal("a shard scan without rows"))),
                            (None, true),
                        ));
                    }
                    Err(e) => return Some((Err(external(e)), (None, true))),
                }
            }
            let mut rows = Vec::with_capacity(BATCH_ROWS);
            let mut done = false;
            while rows.len() < BATCH_ROWS {
                match st.stream.next_row().await {
                    Ok(Some(r)) => rows.push(r),
                    Ok(None) => {
                        done = true;
                        break;
                    }
                    Err(e) => return Some((Err(external(e)), (None, true))),
                }
            }
            if rows.is_empty() && done {
                return None;
            }
            let n = st
                .rows
                .fetch_add(rows.len() as u64, AtomicOrdering::Relaxed)
                + rows.len() as u64;
            if n > st.max_rows {
                return Some((
                    Err(external(limit_error(format!(
                        "An analytic read read more than router.analytics.max_rows ({}) rows",
                        st.max_rows
                    )))),
                    (None, true),
                ));
            }
            let b = build_batch(&st.layout, &rows)
                .map_err(|m| external(SprigError::internal(format!("analytic scan: {m}"))));
            Some((b, (if done { None } else { Some(st) }, true)))
        });
        Box::pin(RecordBatchStreamAdapter::new(schema, s))
    }
}

/// How one output column of the answer is printed.
#[derive(Debug, Clone)]
enum OutCol {
    /// The shard's bytes (a column, `MIN`, `MAX`, a group's value).
    Bytes(usize),
    /// An integer (`COUNT`, `ROW_NUMBER`, …).
    Int(usize),
    /// A `DECIMAL` with this scale (`SUM`).
    Dec(usize, u8),
    /// `AVG`: the sum (with its scale) and count columns, printed with the
    /// answer's scale.
    Avg {
        sum: usize,
        count: usize,
        in_scale: u8,
        out_scale: u8,
    },
}

/// The DataFusion statement for `q` and how to print each answer column.
fn datafusion_sql(
    q: &Query,
    layouts: &[Arc<ScanLayout>],
    answer: &[ColumnDef],
) -> std::result::Result<(String, Vec<OutCol>), String> {
    if answer.len() != q.items.len() {
        return Err("the answer's columns don't match the select list".into());
    }
    let refer = |c: &ColRef, key: bool| -> std::result::Result<(String, Kind), String> {
        let (j, sc) = layouts[c.scan]
            .find(&c.col)
            .ok_or_else(|| format!("column {} not scanned", c.col))?;
        if key && !(sc.keyed && sc.kind.comparable()) {
            return Err(format!("{} can't be compared exactly", c.col));
        }
        Ok((
            format!("s{}.{}{j}", c.scan, if key { "k" } else { "v" }),
            sc.kind,
        ))
    };
    let num = |c: &ColRef| -> std::result::Result<(String, u8), String> {
        let (k, kind) = refer(c, true)?;
        if !kind.numeric() {
            return Err(format!("{} isn't an integer or DECIMAL", c.col));
        }
        // Sums of up to 10^10 rows of 28 digits stay inside 38 digits, so
        // DataFusion's 128-bit sums can't wrap.
        if let Kind::Decimal { p, .. } = kind
            && p > 28
        {
            return Err(format!("{}: DECIMAL wider than 28 digits", c.col));
        }
        let s = kind.scale();
        Ok((format!("CAST({k} AS DECIMAL(38,{s}))"), s))
    };
    let check = |i: usize, want: &[u8]| -> std::result::Result<(), String> {
        if want.contains(&answer[i].col_type) {
            Ok(())
        } else {
            Err(format!(
                "column {} of the answer is of type {}, not what the plan computes",
                i + 1,
                answer[i].col_type
            ))
        }
    };
    let order_of = |desc: bool| {
        if desc {
            "DESC NULLS LAST"
        } else {
            "ASC NULLS FIRST"
        }
    };
    let grouped = q.aggregated() || q.distinct;
    let mut select: Vec<String> = Vec::new();
    let mut out: Vec<OutCol> = Vec::new();
    // Hidden sort keys of MIN and MAX outputs, by item.
    let mut hidden: Vec<(usize, String)> = Vec::new();
    for (i, it) in q.items.iter().enumerate() {
        let n = select.len();
        match it {
            Item::Col(c) => {
                let (v, _) = refer(c, false)?;
                if grouped {
                    // A group's value: every row of the group has an equal
                    // key; MariaDB too prints one of their values.
                    let _ = refer(c, true)?;
                    select.push(format!("first_value({v}) AS o{i}"));
                } else {
                    select.push(format!("{v} AS o{i}"));
                }
                out.push(OutCol::Bytes(n));
            }
            Item::Agg(a) => match a {
                Agg::CountStar => {
                    check(i, &[ct::LONGLONG])?;
                    select.push(format!("count(*) AS o{i}"));
                    out.push(OutCol::Int(n));
                }
                Agg::Count(c) => {
                    check(i, &[ct::LONGLONG])?;
                    let (v, _) = refer(c, false)?;
                    select.push(format!("count({v}) AS o{i}"));
                    out.push(OutCol::Int(n));
                }
                Agg::CountDistinct(c) => {
                    check(i, &[ct::LONGLONG])?;
                    let (k, _) = refer(c, true)?;
                    select.push(format!("count(DISTINCT {k}) AS o{i}"));
                    out.push(OutCol::Int(n));
                }
                Agg::Sum(c) => {
                    let (e, s) = num(c)?;
                    check(i, &[ct::NEWDECIMAL])?;
                    if answer[i].decimals != s {
                        return Err(format!("SUM's scale is {}", answer[i].decimals));
                    }
                    select.push(format!("sum({e}) AS o{i}"));
                    out.push(OutCol::Dec(n, s));
                }
                Agg::Avg(c) => {
                    let (e, s) = num(c)?;
                    check(i, &[ct::NEWDECIMAL])?;
                    let out_scale = answer[i].decimals;
                    if out_scale < s || out_scale > 38 {
                        return Err(format!("AVG's scale is {out_scale}"));
                    }
                    let (k, _) = refer(c, true)?;
                    select.push(format!("sum({e}) AS o{i}"));
                    select.push(format!("count({k}) AS c{i}"));
                    out.push(OutCol::Avg {
                        sum: n,
                        count: n + 1,
                        in_scale: s,
                        out_scale,
                    });
                }
                Agg::Min(c) | Agg::Max(c) => {
                    let (v, kind) = refer(c, false)?;
                    let (k, _) = refer(c, true)?;
                    if !kind.comparable() {
                        return Err(format!("MIN or MAX of {}", c.col));
                    }
                    let (dir, f) = if matches!(a, Agg::Min(_)) {
                        ("ASC", "min")
                    } else {
                        ("DESC", "max")
                    };
                    select.push(format!(
                        "first_value({v} ORDER BY {k} {dir} NULLS LAST) AS o{i}"
                    ));
                    out.push(OutCol::Bytes(n));
                    hidden.push((i, format!("{f}({k})")));
                }
            },
            Item::Win(w) => {
                let mut over = String::new();
                if !w.partition.is_empty() {
                    let ps = w
                        .partition
                        .iter()
                        .map(|c| refer(c, true).map(|r| r.0))
                        .collect::<std::result::Result<Vec<_>, _>>()?;
                    let _ = write!(over, "PARTITION BY {}", ps.join(", "));
                }
                if !w.order.is_empty() {
                    let os = w
                        .order
                        .iter()
                        .map(|(c, d)| refer(c, true).map(|r| format!("{} {}", r.0, order_of(*d))))
                        .collect::<std::result::Result<Vec<_>, _>>()?;
                    if !over.is_empty() {
                        over.push(' ');
                    }
                    let _ = write!(over, "ORDER BY {}", os.join(", "));
                }
                match &w.func {
                    WinFn::RowNumber | WinFn::Rank | WinFn::DenseRank => {
                        check(i, &[ct::LONGLONG])?;
                        let f = match w.func {
                            WinFn::RowNumber => "row_number",
                            WinFn::Rank => "rank",
                            _ => "dense_rank",
                        };
                        select.push(format!("{f}() OVER ({over}) AS o{i}"));
                        out.push(OutCol::Int(n));
                    }
                    WinFn::CountStar => {
                        check(i, &[ct::LONGLONG])?;
                        select.push(format!("count(*) OVER ({over}) AS o{i}"));
                        out.push(OutCol::Int(n));
                    }
                    WinFn::Count(c) => {
                        check(i, &[ct::LONGLONG])?;
                        let (v, _) = refer(c, false)?;
                        select.push(format!("count({v}) OVER ({over}) AS o{i}"));
                        out.push(OutCol::Int(n));
                    }
                    WinFn::Sum(c) => {
                        let (e, s) = num(c)?;
                        check(i, &[ct::NEWDECIMAL])?;
                        if answer[i].decimals != s {
                            return Err(format!("SUM's scale is {}", answer[i].decimals));
                        }
                        select.push(format!("sum({e}) OVER ({over}) AS o{i}"));
                        out.push(OutCol::Dec(n, s));
                    }
                }
            }
        }
    }
    for (i, e) in &hidden {
        select.push(format!("{e} AS h{i}"));
    }
    let mut sql = format!("SELECT {} FROM s0", select.join(", "));
    for j in &q.joins {
        let kind = match j.kind {
            translate::JoinKind::Inner => "JOIN",
            translate::JoinKind::Left => "LEFT JOIN",
        };
        let mut on = Vec::new();
        for (a, b) in &j.on {
            let (ka, ta) = refer(a, true)?;
            let (kb, tb) = refer(b, true)?;
            let common = keys::join_cast(ta, tb).ok_or_else(|| {
                format!("{} = {}: types MariaDB compares otherwise", a.col, b.col)
            })?;
            if common.is_empty() {
                on.push(format!("{ka} = {kb}"));
            } else {
                on.push(format!("CAST({ka} AS {common}) = CAST({kb} AS {common})"));
            }
        }
        let _ = write!(sql, " {kind} s{} ON {}", j.scan, on.join(" AND "));
    }
    // GROUP BY: the group columns' keys; DISTINCT: every output column's.
    let mut groups: Vec<String> = Vec::new();
    for c in &q.group {
        groups.push(refer(c, true)?.0);
    }
    if q.distinct {
        for it in &q.items {
            if let Item::Col(c) = it {
                groups.push(refer(c, true)?.0);
            }
        }
    }
    if !groups.is_empty() {
        let _ = write!(sql, " GROUP BY {}", groups.join(", "));
    }
    let mut order = Vec::new();
    for o in &q.order {
        match o {
            OrderKey::Item(i, desc) => {
                let e = match &q.items[*i] {
                    Item::Col(c) => refer(c, true)?.0,
                    Item::Agg(Agg::Avg(_)) => {
                        return Err("ORDER BY an average".into());
                    }
                    Item::Agg(Agg::Min(_) | Agg::Max(_)) => format!("h{i}"),
                    _ => format!("o{i}"),
                };
                order.push(format!("{e} {}", order_of(*desc)));
            }
            OrderKey::Col(c, desc) => {
                order.push(format!("{} {}", refer(c, true)?.0, order_of(*desc)));
            }
        }
    }
    if !order.is_empty() {
        let _ = write!(sql, " ORDER BY {}", order.join(", "));
    }
    if let Some(l) = q.limit {
        let _ = write!(sql, " LIMIT {l}");
    }
    if q.offset > 0 {
        let _ = write!(sql, " OFFSET {}", q.offset);
    }
    Ok((sql, out))
}

/// One answer column, as Arrow gave it.
enum Col {
    B(BinaryArray),
    I(Int64Array),
    D(Decimal128Array, u8),
    A(Decimal128Array, Int64Array, u8, u8),
}

/// The answer's rows, printed as MariaDB prints them.
fn render(
    batches: &[RecordBatch],
    out: &[OutCol],
    budget: &mut Budget,
) -> std::result::Result<Vec<Row>, String> {
    let mut rows = Vec::new();
    for b in batches {
        let cols: Vec<ArrayRef> = b.columns().to_vec();
        let as_bin = |i: usize| -> std::result::Result<BinaryArray, String> {
            let a = cast(&cols[i], &DataType::Binary).map_err(|e| e.to_string())?;
            a.as_any()
                .downcast_ref::<BinaryArray>()
                .cloned()
                .ok_or_else(|| "binary column".to_owned())
        };
        let as_int = |i: usize| -> std::result::Result<Int64Array, String> {
            if let Some(u) = cols[i].as_any().downcast_ref::<UInt64Array>() {
                let v: Int64Array = u
                    .iter()
                    .map(|x| x.and_then(|x| i64::try_from(x).ok()))
                    .collect();
                return Ok(v);
            }
            let a = cast(&cols[i], &DataType::Int64).map_err(|e| e.to_string())?;
            a.as_any()
                .downcast_ref::<Int64Array>()
                .cloned()
                .ok_or_else(|| "integer column".to_owned())
        };
        let as_dec = |i: usize, scale: u8| -> std::result::Result<Decimal128Array, String> {
            let a = cols[i]
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "decimal column".to_owned())?;
            if a.scale() != scale.cast_signed() {
                return Err(format!("a sum of scale {}", a.scale()));
            }
            Ok(a.clone())
        };
        let mut cs = Vec::with_capacity(out.len());
        for o in out {
            cs.push(match *o {
                OutCol::Bytes(i) => Col::B(as_bin(i)?),
                OutCol::Int(i) => Col::I(as_int(i)?),
                OutCol::Dec(i, s) => Col::D(as_dec(i, s)?, s),
                OutCol::Avg {
                    sum,
                    count,
                    in_scale,
                    out_scale,
                } => Col::A(as_dec(sum, in_scale)?, as_int(count)?, in_scale, out_scale),
            });
        }
        for r in 0..b.num_rows() {
            let mut row: Row = Vec::with_capacity(out.len());
            for c in &cs {
                let v: Option<Bytes> = match c {
                    Col::B(a) => (!a.is_null(r)).then(|| Bytes::copy_from_slice(a.value(r))),
                    Col::I(a) => (!a.is_null(r)).then(|| Bytes::from(a.value(r).to_string())),
                    Col::D(a, s) => {
                        (!a.is_null(r)).then(|| Bytes::from(keys::decimal_text(a.value(r), *s)))
                    }
                    Col::A(sum, cnt, si, so) => {
                        if sum.is_null(r) || cnt.is_null(r) || cnt.value(r) == 0 {
                            None
                        } else {
                            let v = keys::avg(sum.value(r), *si, cnt.value(r), *so)
                                .ok_or_else(|| "an average past 38 digits".to_owned())?;
                            Some(Bytes::from(keys::decimal_text(v, *so)))
                        }
                    }
                };
                row.push(v);
            }
            let n: u64 = row
                .iter()
                .map(|v| v.as_ref().map_or(1, |b| b.len() as u64 + 1))
                .sum();
            budget.charge(n).map_err(|e| e.to_string())?;
            rows.push(row);
        }
    }
    Ok(rows)
}

fn qi(id: &str) -> String {
    format!("`{}`", id.replace('`', "``"))
}

/// The answer's column definitions and each scan's, read on a scratch
/// connection over empty temporary copies of the tables.
struct Probe {
    answer: Vec<ColumnDef>,
    scans: Vec<Vec<ColumnDef>>,
}

async fn probe(
    p: &GatherPlan,
    q: &Query,
    ddls: &[String],
    be: &mut dyn ShardBackend,
    budget: &mut Budget,
) -> Result<std::result::Result<Probe, String>> {
    let Some(mut scratch) = be.open_scratch().await? else {
        return Ok(Err("no scratch connection".into()));
    };
    let mut created: Vec<String> = Vec::new();
    let r = async {
        for (t, ddl) in p.tables.iter().zip(ddls) {
            let Some(create) = temp_ddl(ddl, &t.schema, &t.name) else {
                return Ok(Err(format!("{}: no temporary copy", t.name)));
            };
            match scratch.run(&create, budget).await {
                Ok(_) => created.push(format!("{}.{}", qi(&t.schema), qi(&t.name))),
                Err(e) => return Ok(Err(format!("{}: {}", t.name, e.message))),
            }
        }
        let answer = match scratch.start(&p.sql).await {
            Ok(Some(c)) => c,
            Ok(None) => return Ok(Err("the statement answers with OK".into())),
            Err(e) => return Err(e),
        };
        while scratch.next_row().await?.is_some() {}
        let mut scans = Vec::new();
        for (i, s) in q.scans.iter().enumerate() {
            let t = &p.tables[s.table];
            let cols = translate::columns_of_scan(q, i);
            let list = if cols.is_empty() {
                "1".to_owned()
            } else {
                cols.iter()
                    .map(|c| format!("{}.{}", qi(&s.alias), qi(c)))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            let sql = format!(
                "SELECT {list} FROM {}.{} AS {} LIMIT 0",
                qi(&t.schema),
                qi(&t.name),
                qi(&s.alias)
            );
            match scratch.run(&sql, budget).await? {
                ShardOutcome::Rows(r) => scans.push(r.columns),
                ShardOutcome::Ok(_) => return Ok(Err("a scan answered with OK".into())),
            }
        }
        Ok(Ok(Probe { answer, scans }))
    }
    .await;
    let clean = if created.is_empty() {
        true
    } else {
        let drop = format!("DROP TEMPORARY TABLE IF EXISTS {}", created.join(", "));
        let mut b = Budget::mb(1);
        scratch.run(&drop, &mut b).await.is_ok()
    };
    scratch.finish(clean && r.is_ok()).await;
    r
}

/// Every column `q` uses as a key (compares, groups, sorts, adds).
fn keyed_cols(q: &Query) -> Vec<ColRef> {
    let mut out: Vec<ColRef> = Vec::new();
    let mut add = |c: &ColRef| {
        if !out.contains(c) {
            out.push(c.clone());
        }
    };
    for j in &q.joins {
        for (a, b) in &j.on {
            add(a);
            add(b);
        }
    }
    for c in &q.group {
        add(c);
    }
    for (i, it) in q.items.iter().enumerate() {
        match it {
            Item::Col(c) => {
                if q.distinct || !q.group.is_empty() {
                    add(c);
                }
                if q.order
                    .iter()
                    .any(|o| *o == OrderKey::Item(i, false) || *o == OrderKey::Item(i, true))
                {
                    add(c);
                }
            }
            Item::Agg(a) => match a {
                Agg::CountStar | Agg::Count(_) => {}
                Agg::CountDistinct(c) | Agg::Sum(c) | Agg::Avg(c) | Agg::Min(c) | Agg::Max(c) => {
                    add(c);
                }
            },
            Item::Win(w) => {
                if let WinFn::Sum(c) = &w.func {
                    add(c);
                }
                for c in &w.partition {
                    add(c);
                }
                for (c, _) in &w.order {
                    add(c);
                }
            }
        }
    }
    for o in &q.order {
        if let OrderKey::Col(c, _) = o {
            add(c);
        }
    }
    out
}

/// The DataFusion session for one statement.
fn session(lim: &AnalyticsLimits) -> std::result::Result<SessionContext, DataFusionError> {
    let mem = usize::try_from(lim.memory_mb.max(16).saturating_mul(1 << 20)).unwrap_or(usize::MAX);
    let disk = if lim.spill_mb == 0 {
        DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled)
    } else {
        DiskManagerBuilder::default()
            .with_mode(DiskManagerMode::OsTmpDirectory)
            .with_max_temp_directory_size(lim.spill_mb.saturating_mul(1 << 20))
    };
    let rt = RuntimeEnvBuilder::new()
        .with_memory_limit(mem, 1.0)
        .with_disk_manager_builder(disk)
        .build_arc()?;
    let mut cfg = SessionConfig::new()
        .with_target_partitions(4)
        .with_batch_size(BATCH_ROWS)
        .with_information_schema(false);
    // Each sort holds back memory for merging its spill files (10 MB by
    // default); four partitions of a small pool couldn't, so a statement
    // that spilling would have saved failed instead. A sixteenth of the
    // pool each keeps spilling possible at any `memory_mb`.
    cfg.options_mut().execution.sort_spill_reservation_bytes = (mem / 16).min(10 << 20);
    cfg.options_mut().execution.sort_in_place_threshold_bytes = (mem / 32).min(1 << 20);
    Ok(SessionContext::new_with_config_rt(cfg, rt))
}

/// `plan` with every operator's input made cooperative: each batch it
/// passes up spends Tokio's task budget, so even an operator that never
/// waits (a join emitting many rows from one probe batch, an aggregate
/// draining it) yields every so often. Dropping the statement's future
/// (`KILL QUERY`) aborts DataFusion's tasks only where they yield; without
/// this a killed self-join ran on for minutes.
fn cooperative(
    plan: Arc<dyn ExecutionPlan>,
) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let kids: Vec<Arc<dyn ExecutionPlan>> = plan.children().into_iter().cloned().collect();
    if kids.is_empty() {
        return Ok(plan);
    }
    let wrapped = kids
        .into_iter()
        .map(|c| {
            cooperative(c).map(|c| Arc::new(CooperativeExec::new(c)) as Arc<dyn ExecutionPlan>)
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    // `with_new_children` is deprecated in DataFusion 55; its replacement
    // takes options this rebuild doesn't need.
    #[allow(deprecated)]
    plan.with_new_children(wrapped)
}

/// The `SprigError` a shard stream raised, from DataFusion's error.
fn shard_error(e: &DataFusionError) -> Option<SprigError> {
    match e {
        DataFusionError::External(x) => x.downcast_ref::<SprigError>().map(|e| SprigError {
            kind: e.kind,
            message: e.message.clone(),
            retryable: e.retryable,
            source: None,
        }),
        DataFusionError::Context(_, inner) => shard_error(inner),
        DataFusionError::Shared(x) => shard_error(x),
        _ => None,
    }
}

fn exhausted(e: &DataFusionError) -> bool {
    match e {
        DataFusionError::ResourcesExhausted(_) => true,
        DataFusionError::Context(_, inner) => exhausted(inner),
        DataFusionError::Shared(x) => exhausted(x),
        _ => false,
    }
}

/// Runs `p` as an analytic read (see the module docs). `Refused` before
/// anything was sent; errors from the shards, 39421 for the limits.
///
/// # Errors
/// Backend errors, 39421, 39422, a client gone.
pub(crate) fn execute<'a, 'b: 'a, 'c: 'a>(
    p: &'a GatherPlan,
    be: &'a mut (dyn ShardBackend + 'b),
    budget: &'a mut Budget,
    sink: Option<&'a mut (dyn RowSink + 'c)>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Outcome>> + Send + 'a>> {
    // Boxed as a plain `dyn Future`: DataFusion's future types are deep
    // enough that every crate awaiting them would need a higher
    // recursion limit to prove them `Send`.
    Box::pin(execute_inner(p, be, budget, sink))
}

async fn execute_inner<'a, 'b: 'a, 'c: 'a>(
    p: &'a GatherPlan,
    be: &'a mut (dyn ShardBackend + 'b),
    budget: &'a mut Budget,
    sink: Option<&'a mut (dyn RowSink + 'c)>,
) -> Result<Outcome> {
    let lim = be.gather_limits().analytics;
    if lim.mode == AnalyticsMode::Off {
        return refused("router.analytics.mode is off");
    }
    if be.in_transaction() {
        return refused("inside a transaction (each shard is read on its branch)");
    }
    if p.reads_unsharded {
        return refused("the statement reads unsharded tables");
    }
    let gate = GATE.get_or_init(|| tokio::sync::Semaphore::new(lim.max_concurrent.max(1)));
    let _permit = gate
        .acquire()
        .await
        .map_err(|_| SprigError::internal("the analytics gate closed"))?;
    let started = std::time::Instant::now();
    // 1. Each table's definition, from one of its shards.
    let mut ddls = Vec::with_capacity(p.tables.len());
    let mut tables = Vec::with_capacity(p.tables.len());
    let mut collations: Vec<HashMap<String, u16>> = Vec::with_capacity(p.tables.len());
    for t in &p.tables {
        let Some(first) = t.sources.first() else {
            return refused(format!("{}: no shard", t.name));
        };
        let show = format!("SHOW CREATE TABLE {}.{}", qi(&first.at.schema), qi(&t.name));
        let ddl = match be.run(first, &show, budget).await? {
            ShardOutcome::Rows(r) => r
                .rows
                .first()
                .and_then(|row| row.get(1).cloned().flatten())
                .map(|b| String::from_utf8_lossy(&b).into_owned())
                .unwrap_or_default(),
            ShardOutcome::Ok(_) => String::new(),
        };
        let columns = column_names_of_ddl(&ddl);
        if columns.is_empty() {
            return refused(format!("{}: no columns read", t.name));
        }
        // Each character column's own collation.
        let lit = |x: &str| format!("'{}'", x.replace('\\', "\\\\").replace('\'', "''"));
        let sql = format!(
            "SELECT LOWER(COLUMN_NAME), COLLATION_NAME FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {}",
            lit(&first.at.schema),
            lit(&t.name)
        );
        let mut colls = HashMap::new();
        if let ShardOutcome::Rows(r) = be.run(first, &sql, budget).await? {
            for row in r.rows {
                let name = row.first().cloned().flatten();
                let coll = row.get(1).cloned().flatten();
                if let (Some(n), Some(c)) = (name, coll)
                    && let Some(id) = crate::collation::id_of(&c)
                {
                    colls.insert(String::from_utf8_lossy(&n).into_owned(), id);
                }
            }
        }
        collations.push(colls);
        tables.push(TableCols {
            schema: t.schema.clone(),
            name: t.name.to_ascii_lowercase(),
            columns,
        });
        ddls.push(ddl);
    }
    // 2. The guard.
    let query = match translate::translate(&p.sql, &tables) {
        Ok(q) => q,
        Err(why) => return refused(why),
    };
    // 3. MariaDB's own column definitions.
    let pr = match probe(p, &query, &ddls, be, budget).await? {
        Ok(pr) => pr,
        Err(why) => return refused(why),
    };
    // 4. Keys and scan layouts.
    let keyed = keyed_cols(&query);
    let mut layouts: Vec<Arc<ScanLayout>> = Vec::new();
    let mut scan_sql: Vec<(String, String)> = Vec::new();
    for (i, s) in query.scans.iter().enumerate() {
        let names = translate::columns_of_scan(&query, i);
        let defs = &pr.scans[i];
        if defs.len() != names.len().max(1) && !names.is_empty() {
            return refused("a scan's columns don't match");
        }
        let mut exprs: Vec<String> = Vec::new();
        let mut cols = Vec::new();
        let mut fields = Vec::new();
        for (j, n) in names.iter().enumerate() {
            let kind = Kind::of(&defs[j], collations[s.table].get(n).copied());
            let is_keyed = keyed.contains(&ColRef {
                scan: i,
                col: n.clone(),
            });
            if is_keyed && !kind.comparable() {
                return refused(format!(
                    "{n} is of a type the keys don't compare exactly (type {})",
                    defs[j].col_type
                ));
            }
            let column = format!("{}.{}", qi(&s.alias), qi(n));
            let value_at = exprs.len();
            exprs.push(column.clone());
            let mut key_at = Vec::new();
            if is_keyed {
                for e in kind.key_exprs(&column) {
                    key_at.push(exprs.len());
                    exprs.push(e);
                }
            }
            fields.push(Field::new(format!("v{j}"), DataType::Binary, true));
            if is_keyed {
                fields.push(Field::new(format!("k{j}"), key_type(kind), true));
            }
            cols.push(ScanCol {
                name: n.clone(),
                kind,
                keyed: is_keyed,
                value_at,
                key_at,
            });
        }
        if exprs.is_empty() {
            // COUNT(*) alone: one constant per row.
            exprs.push("1".to_owned());
            fields.push(Field::new("v0", DataType::Binary, true));
            cols.push(ScanCol {
                name: String::new(),
                kind: Kind::Opaque,
                keyed: false,
                value_at: 0,
                key_at: Vec::new(),
            });
        }
        let where_ = if s.filter.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", s.filter.join(" AND "))
        };
        // The shard's schema goes between the two halves.
        scan_sql.push((
            format!("SELECT {} FROM ", exprs.join(", ")),
            format!(
                ".{} AS {}{where_}",
                qi(&p.tables[s.table].name),
                qi(&s.alias)
            ),
        ));
        layouts.push(Arc::new(ScanLayout {
            cols,
            schema: Arc::new(Schema::new(fields)),
        }));
    }
    let (df_sql, out) = match datafusion_sql(&query, &layouts, &pr.answer) {
        Ok(x) => x,
        Err(why) => return refused(why),
    };
    // 5. One stream per shard.
    let ctx = match session(&lim) {
        Ok(c) => c,
        Err(e) => return refused(format!("datafusion: {e}")),
    };
    let rows_read = Arc::new(AtomicU64::new(0));
    for (i, s) in query.scans.iter().enumerate() {
        let t = &p.tables[s.table];
        let mut parts: Vec<Arc<dyn PartitionStream>> = Vec::new();
        for src in &t.sources {
            let Some(stream) = be.open_stream(&src.cluster).await? else {
                return refused("no stream for a shard");
            };
            parts.push(Arc::new(ShardPart {
                schema: Arc::clone(&layouts[i].schema),
                state: Mutex::new(Some(PartState {
                    stream,
                    sql: format!("{}{}{}", scan_sql[i].0, qi(&src.at.schema), scan_sql[i].1),
                    layout: Arc::clone(&layouts[i]),
                    rows: Arc::clone(&rows_read),
                    max_rows: lim.max_rows,
                })),
            }));
        }
        let table = match StreamingTable::try_new(Arc::clone(&layouts[i].schema), parts) {
            Ok(t) => t,
            Err(e) => return refused(format!("datafusion: {e}")),
        };
        if let Err(e) = ctx.register_table(format!("s{i}"), Arc::new(table)) {
            return refused(format!("datafusion: {e}"));
        }
    }
    // 6. DataFusion.
    tracing::debug!(sql = %df_sql, "analytic read");
    let batches = match ctx.sql(&df_sql).await {
        Ok(df) => {
            let task = Arc::new(df.task_ctx());
            match df.create_physical_plan().await.and_then(cooperative) {
                Ok(plan) => datafusion::physical_plan::collect(plan, task).await,
                Err(e) => Err(e),
            }
        }
        Err(e) => Err(e),
    };
    let batches = match batches {
        Ok(b) => b,
        Err(e) => {
            if let Some(se) = shard_error(&e) {
                return Err(se);
            }
            if exhausted(&e) {
                return Err(limit_error(format!(
                    "An analytic read needed more than analytics.datafusion_memory_mb ({} MB) \
                     and router.analytics.spill_mb ({} MB)",
                    lim.memory_mb, lim.spill_mb
                )));
            }
            return refused(format!("datafusion: {e}"));
        }
    };
    metrics::counter!("sprig_router_analytics_rows_total")
        .increment(rows_read.load(AtomicOrdering::Relaxed));
    // 7. The answer.
    // Rendering many rows runs off the runtime's worker.
    // DataFusion itself stays on the runtime: its operators
    // yield per batch (`cooperative`), and `KILL QUERY` must still drop it.
    let n: usize = batches
        .iter()
        .map(datafusion::arrow::array::RecordBatch::num_rows)
        .sum();
    let rows = match sprig_common::cpu::heavy(n, || render(&batches, &out, budget)) {
        Ok(r) => r,
        Err(why) => return refused(why),
    };
    metrics::histogram!("sprig_router_datafusion_duration_seconds")
        .record(started.elapsed().as_secs_f64());
    metrics::counter!("sprig_router_analytics_total", "result" => "done").increment(1);
    let columns = pr.answer;
    if let Some(sink) = sink {
        sink.columns(&columns).await?;
        for r in &rows {
            sink.row(r).await?;
        }
        return Ok(Outcome::Done(Output::Streamed { warnings: 0 }));
    }
    Ok(Outcome::Done(Output::Rows {
        columns,
        rows,
        warnings: 0,
    }))
}

#[cfg(test)]
mod tests;
