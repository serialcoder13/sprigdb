//! The executor (chapter 17, "Scatter" to "Lookup joins"): runs a plan's
//! per-shard statements through a [`ShardBackend`] and combines the
//! answers into one result.
//!
//! R1's first executor holds each shard's rows in gateway memory, charged
//! to `router.max_statement_memory_mb` (39422 past it), and runs shards
//! one after another on the session's connection to each cluster, so a
//! statement inside a client transaction sees that transaction. Streaming
//! with backpressure and parallel shard streams are chapter 17's build
//! step 3 still to come.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Write as _;

use bytes::Bytes;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::ColumnDef;

use crate::collation;
use crate::plan::{
    AggCall, AggFunc, AggPlan, CmpOp, ColRef, FinalSort, JoinCol, KeyCols, LookupJoinPlan, Operand,
    OutCol, Plan, Pred, ReadKind, ReadPlan, SortKey, Target, WriteKind, WritePlan,
};
use crate::reject::{Reason, Rejection};
use crate::topology::{KeyClass, KeyValue};
use crate::value::{ColClass, Dec, canonical, class_of, cmp_decimal_text, cmp_value, cmp_weights};

/// One text row: a value per column, `None` for NULL.
pub type Row = Vec<Option<Bytes>>;

/// A shard's resultset.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShardRows {
    /// Column definitions, as the backend sent them.
    pub columns: Vec<ColumnDef>,
    /// Rows.
    pub rows: Vec<Row>,
    /// Warnings.
    pub warnings: u16,
}

/// A shard's OK packet.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShardOk {
    /// Affected rows.
    pub affected: u64,
    /// Last insert id.
    pub last_insert_id: u64,
    /// Warnings.
    pub warnings: u16,
    /// Info text.
    pub info: String,
}

/// What a shard statement returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardOutcome {
    /// A resultset.
    Rows(ShardRows),
    /// An OK packet.
    Ok(ShardOk),
}

/// The bytes a statement may still hold in the gateway.
#[derive(Debug, Clone, Copy)]
pub struct Budget {
    /// Bytes left.
    pub left: u64,
}

impl Budget {
    /// A budget of `mb` megabytes.
    #[must_use]
    pub fn mb(mb: u64) -> Self {
        Self {
            left: mb.saturating_mul(1 << 20),
        }
    }

    /// Charges `n` bytes.
    ///
    /// # Errors
    /// 39422 when the budget is spent.
    pub fn charge(&mut self, n: u64) -> Result<()> {
        match self.left.checked_sub(n) {
            Some(l) => {
                self.left = l;
                Ok(())
            }
            None => Err(SprigError::new(
                SprigCode::RouterMemoryLimit,
                "The statement's rows held in the gateway passed router.max_statement_memory_mb",
            )),
        }
    }
}

/// Runs statements on shards for the executor. The gateway implements it
/// over the session's backend connections.
#[async_trait::async_trait]
pub trait ShardBackend: Send {
    /// Runs `sql` on `cluster` and returns the whole answer, charging rows
    /// to `budget`.
    ///
    /// # Errors
    /// The backend's error (an ERR packet becomes a `Backend` error).
    async fn run(&mut self, cluster: &str, sql: &str, budget: &mut Budget) -> Result<ShardOutcome>;

    /// Whether the session is inside a transaction (writes then use a
    /// statement savepoint instead of their own transaction).
    fn in_transaction(&self) -> bool;
}

/// The combined answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output {
    /// A resultset.
    Rows {
        /// Column definitions (from the first shard, physical names).
        columns: Vec<ColumnDef>,
        /// Rows, text protocol values.
        rows: Vec<Row>,
        /// Warnings, summed.
        warnings: u16,
    },
    /// An OK packet.
    Ok(ShardOk),
}

fn rejected(reason: Reason, detail: impl Into<String>) -> SprigError {
    Rejection::new(reason, detail).error()
}

/// Runs `plan` (any plan but `NotRouted`, `Pass` and `Reject`, which the
/// caller handles).
///
/// # Errors
/// Shard errors, 39420 for rejections found at run time (an `ENUM` sort
/// key, shards that disagree on columns), 39422 past the memory budget.
pub async fn execute(
    plan: &Plan,
    be: &mut dyn ShardBackend,
    budget: &mut Budget,
) -> Result<Output> {
    metrics::counter!("sprig_router_statements_total", "plan" => plan.kind()).increment(1);
    match plan {
        Plan::Read(r) => {
            metrics::histogram!("sprig_router_fanout_shards")
                .record(u32::try_from(r.targets.len()).map_or(f64::MAX, f64::from));
            let (columns, rows, visible, warnings) = run_read(r, be, budget).await?;
            Ok(Output::Rows {
                columns: columns[..visible].to_vec(),
                rows: rows
                    .into_iter()
                    .map(|mut row| {
                        row.truncate(visible);
                        row
                    })
                    .collect(),
                warnings,
            })
        }
        Plan::LookupJoin(j) => lookup_join(j, be, budget).await,
        Plan::Write(w) => write(w, be, budget).await,
        Plan::Explain(p) => explain(p, be, budget).await,
        Plan::Pass(t) => match be.run(&t.cluster, &t.sql, budget).await? {
            ShardOutcome::Rows(r) => Ok(Output::Rows {
                columns: r.columns,
                rows: r.rows,
                warnings: r.warnings,
            }),
            ShardOutcome::Ok(ok) => Ok(Output::Ok(ok)),
        },
        Plan::Reject(r) => Err(r.error()),
        Plan::NotRouted => Err(SprigError::internal(
            "execute of a statement the router doesn't route",
        )),
    }
}

async fn rows_of(t: &Target, be: &mut dyn ShardBackend, budget: &mut Budget) -> Result<ShardRows> {
    match be.run(&t.cluster, &t.sql, budget).await? {
        ShardOutcome::Rows(r) => Ok(r),
        ShardOutcome::Ok(_) => Err(SprigError::internal("a shard read answered with OK")),
    }
}

/// Every target's rows, combined: all columns (hidden too), the number of
/// visible ones, warnings.
async fn run_read(
    plan: &ReadPlan,
    be: &mut dyn ShardBackend,
    budget: &mut Budget,
) -> Result<(Vec<ColumnDef>, Vec<Row>, usize, u16)> {
    let mut columns: Option<Vec<ColumnDef>> = None;
    let mut rows: Vec<Row> = Vec::new();
    let mut warnings: u16 = 0;
    for t in &plan.targets {
        let r = rows_of(t, be, budget).await?;
        match &columns {
            None => columns = Some(r.columns),
            Some(c) => {
                if c.len() != r.columns.len()
                    || c.iter()
                        .zip(&r.columns)
                        .any(|(a, b)| a.col_type != b.col_type || a.charset != b.charset)
                {
                    return Err(rejected(
                        Reason::SchemaSkew,
                        "shards disagree on result columns",
                    ));
                }
            }
        }
        warnings = warnings.saturating_add(r.warnings);
        rows.extend(r.rows);
    }
    let columns = columns.unwrap_or_default();
    let ncols = columns.len();
    let visible = ncols
        .checked_sub(plan.hidden)
        .ok_or_else(|| SprigError::internal("fewer columns than hidden ones"))?;
    let at = |c: ColRef| c.index(ncols, plan.hidden);
    let rows = match plan.kind {
        ReadKind::Scatter => window(rows, plan.offset, plan.limit),
        ReadKind::Merge => {
            let classes = key_classes(plan.order.iter().map(|k| &k.cols), &columns, &rows, &at)?;
            let mut rows = rows;
            rows.sort_by(|a, b| cmp_rows(a, b, &plan.order, &classes, &at));
            window(rows, plan.offset, plan.limit)
        }
        ReadKind::Aggregate => {
            let agg = plan
                .agg
                .as_ref()
                .ok_or_else(|| SprigError::internal("aggregate plan without aggregates"))?;
            let rows = aggregate(agg, rows, &columns, &at)?;
            window(rows, plan.offset, plan.limit)
        }
    };
    Ok((columns, rows, visible, warnings))
}

fn window(rows: Vec<Row>, offset: u64, limit: Option<u64>) -> Vec<Row> {
    let skip = usize::try_from(offset).unwrap_or(usize::MAX);
    let take = limit.map_or(usize::MAX, |l| usize::try_from(l).unwrap_or(usize::MAX));
    rows.into_iter().skip(skip).take(take).collect()
}

/// The class each key compares by. Column definitions carry the
/// connection's collation, so a string key's own collation comes from its
/// hidden `COLLATION(e)` column; keys the gateway can't order exactly are
/// rejected.
fn key_classes<'k>(
    keys: impl Iterator<Item = &'k KeyCols>,
    columns: &[ColumnDef],
    rows: &[Row],
    at: &dyn Fn(ColRef) -> usize,
) -> Result<Vec<ColClass>> {
    keys.map(|k| resolve_class(columns, rows, k.value, &k.weights, at))
        .collect()
}

fn resolve_class(
    columns: &[ColumnDef],
    rows: &[Row],
    value: ColRef,
    w: &[ColRef; 4],
    at: &dyn Fn(ColRef) -> usize,
) -> Result<ColClass> {
    let def = columns
        .get(at(value))
        .ok_or_else(|| SprigError::internal("key column out of range"))?;
    match class_of(def) {
        ColClass::Unsupported => Err(rejected(
            Reason::Ordering,
            "ordering or grouping on ENUM, SET or GEOMETRY",
        )),
        base @ ColClass::String { .. } => {
            let name = rows
                .iter()
                .find_map(|r| r.get(at(w[3])).and_then(Option::as_deref));
            let Some(name) = name else { return Ok(base) };
            collation::id_of(name)
                .filter(|id| collation::info(*id).is_some())
                .map(|collation| ColClass::String { collation })
                .ok_or_else(|| {
                    rejected(
                        Reason::Ordering,
                        format!("collation {}", String::from_utf8_lossy(name)),
                    )
                })
        }
        c => Ok(c),
    }
}

fn weights_of<'r>(row: &'r Row, w: &[ColRef; 4], at: &dyn Fn(ColRef) -> usize) -> [&'r [u8]; 3] {
    [w[0], w[1], w[2]].map(|c| row.get(at(c)).and_then(Option::as_deref).unwrap_or(&[]))
}

/// Compares one key of two rows: NULL first, strings by weights, others by
/// value.
fn cmp_key(
    a: &Row,
    b: &Row,
    k: &KeyCols,
    class: ColClass,
    at: &dyn Fn(ColRef) -> usize,
) -> Ordering {
    let i = at(k.value);
    let va = a.get(i).and_then(Option::as_deref);
    let vb = b.get(i).and_then(Option::as_deref);
    match (va, vb) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(x), Some(y)) => match class {
            ColClass::String { collation } => cmp_weights(
                collation,
                &weights_of(a, &k.weights, at),
                &weights_of(b, &k.weights, at),
            )
            .unwrap_or(Ordering::Equal),
            c => cmp_value(c, x, y),
        },
    }
}

fn cmp_rows(
    a: &Row,
    b: &Row,
    keys: &[SortKey],
    classes: &[ColClass],
    at: &dyn Fn(ColRef) -> usize,
) -> Ordering {
    for (k, c) in keys.iter().zip(classes) {
        let o = cmp_key(a, b, &k.cols, *c, at);
        let o = if k.desc { o.reverse() } else { o };
        if o != Ordering::Equal {
            return o;
        }
    }
    Ordering::Equal
}

// ---------------------------------------------------------------------------
// Aggregates

/// One aggregate's running state.
#[derive(Debug, Clone)]
enum Acc {
    Count(i128),
    SumDec(Option<Dec>),
    SumF(Option<f64>),
    Best(Option<(Bytes, [Vec<u8>; 3])>),
    Avg {
        dec: Option<Dec>,
        f: Option<f64>,
        count: i128,
    },
}

/// A final aggregate value: text, class, and weights for strings.
#[derive(Debug, Clone)]
struct Final {
    value: Option<Bytes>,
    class: ColClass,
    weights: [Vec<u8>; 3],
}

struct GroupState {
    first: Row,
    accs: Vec<Acc>,
}

fn parse_f64(b: &[u8]) -> Option<f64> {
    std::str::from_utf8(b).ok()?.trim().parse().ok()
}

/// MariaDB's text for a DOUBLE, close enough for the differential's
/// relative tolerance and exact for integral values below 10^15.
#[must_use]
pub fn format_f64(f: f64) -> String {
    if f == 0.0 {
        return "0".into();
    }
    let a = f.abs();
    if (1e-5..1e15).contains(&a) {
        format!("{f}")
    } else {
        format!("{f:e}")
    }
}

fn overflow() -> SprigError {
    rejected(Reason::Aggregate, "a sum beyond 38 digits")
}

#[allow(clippy::too_many_lines)] // one arm per function
fn aggregate(
    agg: &AggPlan,
    rows: Vec<Row>,
    columns: &[ColumnDef],
    at: &dyn Fn(ColRef) -> usize,
) -> Result<Vec<Row>> {
    let gclasses = key_classes(agg.groups.iter(), columns, &rows, at)?;
    let class = |c: ColRef| columns.get(at(c)).map_or(ColClass::Null, class_of);
    // MIN and MAX of strings compare under their own collation.
    let aclasses: Vec<ColClass> = agg
        .aggs
        .iter()
        .map(|a| match a.weights {
            Some(w) => resolve_class(columns, &rows, a.value, &w, at),
            None => Ok(class(a.value)),
        })
        .collect::<Result<_>>()?;
    let mut order: Vec<Vec<u8>> = Vec::new();
    let mut groups: HashMap<Vec<u8>, GroupState> = HashMap::new();
    for row in rows {
        let mut key = Vec::new();
        for (g, gc) in agg.groups.iter().zip(&gclasses) {
            let i = at(g.value);
            match row.get(i).and_then(Option::as_deref) {
                None => key.extend_from_slice(b"\x00N"),
                Some(v) => {
                    let w = weights_of(&row, &g.weights, at);
                    let c = canonical(*gc, v, Some(&w)).ok_or_else(|| {
                        rejected(Reason::Aggregate, "a group key the gateway can't compare")
                    })?;
                    key.extend_from_slice(
                        &u32::try_from(c.len()).unwrap_or(u32::MAX).to_be_bytes(),
                    );
                    key.extend_from_slice(&c);
                }
            }
        }
        let st = if let Some(s) = groups.get_mut(&key) {
            s
        } else {
            {
                order.push(key.clone());
                let accs = agg
                    .aggs
                    .iter()
                    .map(|a| match a.func {
                        AggFunc::Count => Acc::Count(0),
                        AggFunc::Sum => match class(a.value) {
                            ColClass::Float => Acc::SumF(None),
                            _ => Acc::SumDec(None),
                        },
                        AggFunc::Min | AggFunc::Max => Acc::Best(None),
                        AggFunc::Avg => Acc::Avg {
                            dec: None,
                            f: None,
                            count: 0,
                        },
                    })
                    .collect();
                groups.entry(key).or_insert(GroupState {
                    first: row.clone(),
                    accs,
                })
            }
        };
        for ((a, acc), ac) in agg.aggs.iter().zip(st.accs.iter_mut()).zip(&aclasses) {
            let v = row.get(at(a.value)).and_then(Option::as_deref);
            match acc {
                Acc::Count(n) => {
                    let p = v.and_then(Dec::parse).map_or(0, |d| d.mant);
                    *n = n.checked_add(p).ok_or_else(overflow)?;
                }
                Acc::SumDec(s) => {
                    if let Some(x) = v {
                        let d = Dec::parse(x).ok_or_else(|| {
                            rejected(Reason::Aggregate, "a SUM partial that isn't a decimal")
                        })?;
                        *s = Some(match s.take() {
                            None => d,
                            Some(prev) => prev.plus(d).ok_or_else(overflow)?,
                        });
                    }
                }
                Acc::SumF(s) => {
                    if let Some(x) = v.and_then(parse_f64) {
                        *s = Some(s.unwrap_or(0.0) + x);
                    }
                }
                Acc::Best(best) => {
                    if let Some(x) = v {
                        let w = a.weights.map_or([&[][..]; 3], |w| weights_of(&row, &w, at));
                        let c = *ac;
                        let better = match best {
                            None => true,
                            Some((bv, bw)) => {
                                let o = match c {
                                    ColClass::String { collation } => cmp_weights(
                                        collation,
                                        &w,
                                        &[bw[0].as_slice(), bw[1].as_slice(), bw[2].as_slice()],
                                    )
                                    .ok_or_else(|| {
                                        rejected(
                                            Reason::Aggregate,
                                            "MIN or MAX under an unknown collation",
                                        )
                                    })?,
                                    ColClass::Unsupported => {
                                        return Err(rejected(
                                            Reason::Aggregate,
                                            "MIN or MAX of ENUM or SET",
                                        ));
                                    }
                                    c => cmp_value(c, x, bv),
                                };
                                if a.func == AggFunc::Min {
                                    o == Ordering::Less
                                } else {
                                    o == Ordering::Greater
                                }
                            }
                        };
                        if better {
                            *best = Some((Bytes::copy_from_slice(x), w.map(<[u8]>::to_vec)));
                        }
                    }
                }
                Acc::Avg { dec, f, count } => {
                    let cnt = a
                        .count
                        .and_then(|c| row.get(at(c)).and_then(Option::as_deref))
                        .and_then(Dec::parse)
                        .map_or(0, |d| d.mant);
                    *count = count.checked_add(cnt).ok_or_else(overflow)?;
                    if let Some(x) = v {
                        if class(a.value) == ColClass::Float {
                            *f = Some(f.unwrap_or(0.0) + parse_f64(x).unwrap_or(0.0));
                        } else {
                            {
                                let d = Dec::parse(x).ok_or_else(|| {
                                    rejected(
                                        Reason::Aggregate,
                                        "an AVG partial that isn't a decimal",
                                    )
                                })?;
                                *dec = Some(match dec.take() {
                                    None => d,
                                    Some(p) => p.plus(d).ok_or_else(overflow)?,
                                });
                            }
                        }
                    }
                }
            }
        }
    }
    // Final values, HAVING, output rows.
    let mut out: Vec<(Row, Vec<Final>, Row)> = Vec::new();
    for key in order {
        let Some(st) = groups.remove(&key) else {
            continue;
        };
        let finals = finals(agg, &st, &aclasses, columns, at)?;
        if let Some(h) = &agg.having
            && eval(h, &st.first, &finals, columns, at) != Some(true)
        {
            continue;
        }
        let row: Row = agg
            .outputs
            .iter()
            .enumerate()
            .map(|(i, o)| match o {
                OutCol::First => st.first.get(i).cloned().flatten(),
                OutCol::Agg(k) => finals[*k].value.clone(),
            })
            .collect();
        out.push((row, finals, st.first));
    }
    let sorts: Vec<FinalSort> = if agg.order.is_empty() {
        (0..agg.groups.len())
            .map(|g| FinalSort::Group(g, false))
            .collect()
    } else {
        agg.order.clone()
    };
    out.sort_by(|a, b| {
        for s in &sorts {
            let (o, desc) = match *s {
                FinalSort::Group(g, d) => (cmp_key(&a.2, &b.2, &agg.groups[g], gclasses[g], at), d),
                FinalSort::Agg(k, d) => (cmp_final(&a.1[k], &b.1[k]), d),
            };
            let o = if desc { o.reverse() } else { o };
            if o != Ordering::Equal {
                return o;
            }
        }
        Ordering::Equal
    });
    // The visible columns plus the hidden ones, so the caller's truncation
    // finds a full row.
    let hidden = columns.len() - agg.outputs.len();
    Ok(out
        .into_iter()
        .map(|(mut row, _, _)| {
            row.extend(std::iter::repeat_n(None, hidden));
            row
        })
        .collect())
}

fn finals(
    agg: &AggPlan,
    st: &GroupState,
    aclasses: &[ColClass],
    columns: &[ColumnDef],
    at: &dyn Fn(ColRef) -> usize,
) -> Result<Vec<Final>> {
    let mut out = Vec::with_capacity(agg.aggs.len());
    for ((a, acc), ac) in agg.aggs.iter().zip(&st.accs).zip(aclasses) {
        let f = match acc {
            Acc::Count(n) => Final {
                value: Some(Bytes::from(n.to_string())),
                class: ColClass::Exact,
                weights: Default::default(),
            },
            Acc::SumDec(s) => Final {
                value: s.map(|d| Bytes::from(d.to_text())),
                class: ColClass::Exact,
                weights: Default::default(),
            },
            Acc::SumF(s) => Final {
                value: s.map(|x| Bytes::from(format_f64(x))),
                class: ColClass::Float,
                weights: Default::default(),
            },
            Acc::Best(b) => Final {
                value: b.as_ref().map(|(v, _)| v.clone()),
                class: *ac,
                weights: b.as_ref().map(|(_, w)| w.clone()).unwrap_or_default(),
            },
            Acc::Avg { dec, f, count } => avg_final(a, *dec, *f, *count, columns, at)?,
        };
        out.push(f);
    }
    Ok(out)
}

fn avg_final(
    a: &AggCall,
    dec: Option<Dec>,
    f: Option<f64>,
    count: i128,
    columns: &[ColumnDef],
    at: &dyn Fn(ColRef) -> usize,
) -> Result<Final> {
    if count == 0 {
        return Ok(Final {
            value: None,
            class: ColClass::Exact,
            weights: Default::default(),
        });
    }
    if let Some(sum) = f {
        #[allow(clippy::cast_precision_loss)] // counts stay far below 2^53
        let v = sum / count as f64;
        return Ok(Final {
            value: Some(Bytes::from(format_f64(v))),
            class: ColClass::Float,
            weights: Default::default(),
        });
    }
    let Some(sum) = dec else {
        return Ok(Final {
            value: None,
            class: ColClass::Exact,
            weights: Default::default(),
        });
    };
    let scale = a
        .scale_from
        .and_then(|c| columns.get(at(c)))
        .map_or(sum.scale + 4, |d| u32::from(d.decimals));
    // A DOUBLE AVG (over FLOAT or string columns) keeps double arithmetic.
    if a.scale_from
        .and_then(|c| columns.get(at(c)))
        .is_some_and(|d| matches!(class_of(d), ColClass::Float))
    {
        let s: f64 = sum.to_text().parse().unwrap_or(0.0);
        #[allow(clippy::cast_precision_loss)]
        let v = s / count as f64;
        return Ok(Final {
            value: Some(Bytes::from(format_f64(v))),
            class: ColClass::Float,
            weights: Default::default(),
        });
    }
    let q = sum.div_int(count, scale).ok_or_else(overflow)?;
    Ok(Final {
        value: Some(Bytes::from(q.to_text())),
        class: ColClass::Exact,
        weights: Default::default(),
    })
}

fn cmp_final(a: &Final, b: &Final) -> Ordering {
    match (&a.value, &b.value) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(x), Some(y)) => match a.class {
            ColClass::String { collation } => cmp_weights(
                collation,
                &[
                    a.weights[0].as_slice(),
                    a.weights[1].as_slice(),
                    a.weights[2].as_slice(),
                ],
                &[
                    b.weights[0].as_slice(),
                    b.weights[1].as_slice(),
                    b.weights[2].as_slice(),
                ],
            )
            .unwrap_or(Ordering::Equal),
            c => cmp_value(c, x, y),
        },
    }
}

/// An operand's value and whether it is exact.
fn operand<'a>(
    o: &'a Operand,
    first: &'a Row,
    finals: &'a [Final],
    at: &dyn Fn(ColRef) -> usize,
    columns: &[ColumnDef],
) -> (Option<&'a [u8]>, bool) {
    match o {
        Operand::Agg(k) => (
            finals[*k].value.as_deref(),
            finals[*k].class == ColClass::Exact,
        ),
        Operand::Col(c) => {
            let i = at(*c);
            (
                first.get(i).and_then(Option::as_deref),
                columns
                    .get(i)
                    .is_some_and(|d| class_of(d) == ColClass::Exact),
            )
        }
        Operand::Num(n) => (Some(n.as_bytes()), !n.contains(['e', 'E'])),
        Operand::Null => (None, true),
    }
}

/// Three-valued HAVING.
fn eval(
    p: &Pred,
    first: &Row,
    finals: &[Final],
    columns: &[ColumnDef],
    at: &dyn Fn(ColRef) -> usize,
) -> Option<bool> {
    match p {
        Pred::And(a, b) => match (
            eval(a, first, finals, columns, at),
            eval(b, first, finals, columns, at),
        ) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        },
        Pred::Or(a, b) => match (
            eval(a, first, finals, columns, at),
            eval(b, first, finals, columns, at),
        ) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        },
        Pred::Not(a) => eval(a, first, finals, columns, at).map(|b| !b),
        Pred::IsNull(o, negated) => {
            let (v, _) = operand(o, first, finals, at, columns);
            Some(v.is_none() != *negated)
        }
        Pred::Cmp(op, l, r) => {
            let (a, ea) = operand(l, first, finals, at, columns);
            let (b, eb) = operand(r, first, finals, at, columns);
            let (a, b) = (a?, b?);
            let o = if ea && eb {
                cmp_decimal_text(a, b)
            } else {
                parse_f64(a)?.total_cmp(&parse_f64(b)?)
            };
            Some(match op {
                CmpOp::Eq => o == Ordering::Equal,
                CmpOp::Ne => o != Ordering::Equal,
                CmpOp::Lt => o == Ordering::Less,
                CmpOp::Le => o != Ordering::Greater,
                CmpOp::Gt => o == Ordering::Greater,
                CmpOp::Ge => o != Ordering::Less,
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Lookup joins

/// A typed literal for `y IN (…)` that MariaDB compares as it would
/// `x = y`, or `None` when the value can't match exactly.
fn literal(class: ColClass, charset: &str, v: &[u8]) -> Option<String> {
    let hexed = |b: &[u8]| {
        b.iter()
            .fold(String::with_capacity(b.len() * 2), |mut s, c| {
                let _ = write!(s, "{c:02X}");
                s
            })
    };
    match class {
        ColClass::Exact => v
            .iter()
            .all(|c| c.is_ascii_digit() || *c == b'.' || *c == b'-')
            .then(|| String::from_utf8_lossy(v).into_owned()),
        ColClass::Float => {
            let t = String::from_utf8_lossy(v).into_owned();
            let ok = t
                .bytes()
                .all(|c| c.is_ascii_digit() || b".-+eE".contains(&c));
            ok.then(|| {
                if t.contains(['e', 'E']) {
                    t
                } else {
                    format!("{t}e0")
                }
            })
        }
        ColClass::String { .. } => Some(format!("_{charset} X'{}'", hexed(v))),
        ColClass::Binary => Some(format!("X'{}'", hexed(v))),
        ColClass::FixedText | ColClass::Time => v
            .iter()
            .all(|c| c.is_ascii_digit() || b"-: .".contains(c))
            .then(|| format!("'{}'", String::from_utf8_lossy(v))),
        ColClass::Null | ColClass::Unsupported => None,
    }
}

/// The comparison family of a join column.
fn family(c: ColClass) -> u8 {
    match c {
        ColClass::Exact | ColClass::Float => 0,
        ColClass::String { .. } => 1,
        ColClass::Binary => 2,
        ColClass::FixedText | ColClass::Time => 3,
        ColClass::Null | ColClass::Unsupported => 9,
    }
}

/// The join key of a value: double when either side is floating point.
fn join_key(class: ColClass, as_float: bool, v: &[u8], w: &[&[u8]; 3]) -> Option<Vec<u8>> {
    if as_float {
        return canonical(ColClass::Float, v, None);
    }
    canonical(class, v, Some(w))
}

#[allow(clippy::too_many_lines)] // outer side, batches, matching
async fn lookup_join(
    j: &LookupJoinPlan,
    be: &mut dyn ShardBackend,
    budget: &mut Budget,
) -> Result<Output> {
    metrics::counter!("sprig_router_statements_total", "plan" => "lookup_join").increment(0);
    let (ocols, orows, _ovisible, mut warnings) = run_read(&j.outer, be, budget).await?;
    let ohidden = j.outer.hidden;
    let on = ocols.len();
    let oat = |c: ColRef| c.index(on, ohidden);
    let xi = oat(j.outer_key.value);
    // Values arrive in the connection's character set (the definition's).
    let charset = ocols
        .get(xi)
        .and_then(|d| collation::info(d.charset))
        .map_or("utf8mb4", |i| i.charset);
    let xclass = resolve_class(
        &ocols,
        &orows,
        j.outer_key.value,
        &j.outer_key.weights,
        &oat,
    )
    .map_err(|_| {
        rejected(
            Reason::JoinTypes,
            "a join on ENUM, SET, GEOMETRY or an unknown collation",
        )
    })?;
    if matches!(xclass, ColClass::Unsupported) {
        return Err(rejected(
            Reason::JoinTypes,
            "a join on ENUM, SET or GEOMETRY",
        ));
    }
    let mut icols: Option<Vec<ColumnDef>> = None;
    let mut out_rows: Vec<Row> = Vec::new();
    let empty: [&[u8]; 3] = [&[], &[], &[]];
    for batch in orows.chunks(j.batch) {
        // Distinct non-NULL keys of the batch.
        let mut keys: Vec<(&[u8], [&[u8]; 3])> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for r in batch {
            if let Some(v) = r.get(xi).and_then(Option::as_deref) {
                let w = weights_of(r, &j.outer_key.weights, &oat);
                let Some(k) = canonical(xclass, v, Some(&w)) else {
                    continue;
                };
                if seen.insert(k) {
                    keys.push((v, w));
                }
            }
        }
        // Keys per inner shard.
        let n = j.inner_group.shards.len();
        let mut per: Vec<Vec<String>> = vec![Vec::new(); n];
        for (v, _) in &keys {
            let Some(lit) = literal(xclass, charset, v) else {
                continue;
            };
            if j.inner_by_key {
                let kv = match j.inner_group.key_class {
                    KeyClass::Int => match Dec::parse(v) {
                        Some(d) if d.scale == 0 || d.mant % 10i128.pow(d.scale) == 0 => {
                            KeyValue::Int(d.mant / 10i128.pow(d.scale))
                        }
                        _ => continue,
                    },
                    KeyClass::Str { .. } | KeyClass::Binary => KeyValue::Bytes(v.to_vec()),
                };
                let Some(shard) = j.inner_group.shard_of(&kv) else {
                    continue;
                };
                per[shard].push(lit);
            } else {
                for p in &mut per {
                    p.push(lit.clone());
                }
            }
        }
        let mut matches: HashMap<Vec<u8>, Vec<Row>> = HashMap::new();
        let mut yclass: Option<ColClass> = None;
        for (s, lits) in per.iter().enumerate() {
            if lits.is_empty() {
                continue;
            }
            let loc = &j.inner_group.shards[s];
            let sql = j
                .inner_template
                .replace("\u{1}S", &loc.schema.replace('`', "``"))
                .replace("\u{1}K", &lits.join(", "));
            metrics::counter!("sprig_router_lookup_batches_total").increment(1);
            let r = rows_of(
                &Target {
                    cluster: loc.cluster.clone(),
                    shard: loc.shard,
                    sql,
                },
                be,
                budget,
            )
            .await?;
            warnings = warnings.saturating_add(r.warnings);
            let n_i = r.columns.len();
            let iat = |c: ColRef| c.index(n_i, j.inner_hidden);
            let yi = iat(j.inner_key.value);
            let yc = resolve_class(
                &r.columns,
                &r.rows,
                j.inner_key.value,
                &j.inner_key.weights,
                &iat,
            )
            .map_err(|_| {
                rejected(
                    Reason::JoinTypes,
                    "a join on ENUM, SET, GEOMETRY or an unknown collation",
                )
            })?;
            yclass = Some(yc);
            if family(yc) != family(xclass) {
                return Err(rejected(
                    Reason::JoinTypes,
                    "a join of different types across shards",
                ));
            }
            if let (ColClass::String { collation: a }, ColClass::String { collation: b }) =
                (xclass, yc)
                && a != b
            {
                return Err(rejected(
                    Reason::JoinTypes,
                    "a join of different collations across shards",
                ));
            }
            let as_float = matches!(xclass, ColClass::Float) || matches!(yc, ColClass::Float);
            for row in r.rows {
                let Some(v) = row.get(yi).and_then(Option::as_deref) else {
                    continue;
                };
                let w = weights_of(&row, &j.inner_key.weights, &iat);
                let Some(k) = join_key(yc, as_float, v, &w) else {
                    continue;
                };
                matches.entry(k).or_default().push(row);
            }
            if icols.is_none() {
                icols = Some(r.columns);
            }
        }
        let as_float = matches!(xclass, ColClass::Float) || matches!(yclass, Some(ColClass::Float));
        for o in batch {
            let found = o
                .get(xi)
                .and_then(Option::as_deref)
                .and_then(|v| {
                    let w = weights_of(o, &j.outer_key.weights, &oat);
                    join_key(xclass, as_float, v, &w)
                })
                .and_then(|k| matches.get(&k));
            match found {
                Some(rows) => {
                    for i in rows {
                        out_rows.push(join_row(&j.outputs, o, Some(i)));
                    }
                }
                None if j.left => out_rows.push(join_row(&j.outputs, o, None)),
                None => {}
            }
        }
        let bytes: usize = out_rows.iter().flatten().flatten().map(Bytes::len).sum();
        budget.charge(bytes as u64 / 4)?;
    }
    let _ = empty;
    // Inner definitions even when no batch ran.
    let icols = if let Some(c) = icols {
        c
    } else {
        {
            let loc = &j.inner_group.shards[0];
            let sql = j
                .inner_template
                .replace("\u{1}S", &loc.schema.replace('`', "``"))
                .replace("\u{1}K", "NULL");
            rows_of(
                &Target {
                    cluster: loc.cluster.clone(),
                    shard: loc.shard,
                    sql,
                },
                be,
                budget,
            )
            .await?
            .columns
        }
    };
    let columns = j
        .outputs
        .iter()
        .map(|c| match c {
            JoinCol::Outer(i) => ocols.get(*i).cloned(),
            JoinCol::Inner(i) => icols.get(*i).cloned(),
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| SprigError::internal("join output column out of range"))?;
    Ok(Output::Rows {
        columns,
        rows: window(out_rows, j.offset, j.limit),
        warnings,
    })
}

fn join_row(outputs: &[JoinCol], o: &Row, i: Option<&Row>) -> Row {
    outputs
        .iter()
        .map(|c| match c {
            JoinCol::Outer(k) => o.get(*k).cloned().flatten(),
            JoinCol::Inner(k) => i.and_then(|r| r.get(*k).cloned().flatten()),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Writes

/// Numbers after each label of an info text (`Records: 3  Duplicates: 0`).
fn info_numbers(info: &str) -> Vec<(String, u64)> {
    let mut out = Vec::new();
    let mut parts = info.split(':');
    let mut label = parts.next().unwrap_or("").trim().to_owned();
    for p in parts {
        let p = p.trim();
        let (num, rest) = p.split_at(p.find(|c: char| !c.is_ascii_digit()).unwrap_or(p.len()));
        out.push((label.clone(), num.parse().unwrap_or(0)));
        rest.trim().clone_into(&mut label);
    }
    out
}

async fn write(w: &WritePlan, be: &mut dyn ShardBackend, budget: &mut Budget) -> Result<Output> {
    metrics::histogram!("sprig_router_fanout_shards")
        .record(u32::try_from(w.targets.len()).map_or(f64::MAX, f64::from));
    let Some(first) = w.targets.first() else {
        return Ok(Output::Ok(ShardOk::default()));
    };
    let cluster = first.cluster.clone();
    let in_txn = be.in_transaction();
    let (begin, undo, end) = if in_txn {
        (
            "SAVEPOINT __sprig_stmt",
            "ROLLBACK TO SAVEPOINT __sprig_stmt",
            "RELEASE SAVEPOINT __sprig_stmt",
        )
    } else {
        ("BEGIN", "ROLLBACK", "COMMIT")
    };
    be.run(&cluster, begin, budget).await?;
    let mut total = ShardOk::default();
    let (mut a, mut b, mut warn_info) = (0u64, 0u64, 0u64);
    for t in &w.targets {
        match be.run(&t.cluster, &t.sql, budget).await {
            Ok(ShardOutcome::Ok(ok)) => {
                total.affected += ok.affected;
                if total.last_insert_id == 0 {
                    total.last_insert_id = ok.last_insert_id;
                }
                total.warnings = total.warnings.saturating_add(ok.warnings);
                let nums = info_numbers(&ok.info);
                match w.kind {
                    WriteKind::Insert => {
                        if nums.is_empty() {
                            a += 1;
                        }
                        for (l, n) in nums {
                            match l.as_str() {
                                "Records" => a += n,
                                "Duplicates" => b += n,
                                "Warnings" => warn_info += n,
                                _ => {}
                            }
                        }
                    }
                    WriteKind::Update => {
                        for (l, n) in nums {
                            match l.as_str() {
                                "Rows matched" => a += n,
                                "Changed" => b += n,
                                "Warnings" => warn_info += n,
                                _ => {}
                            }
                        }
                    }
                    WriteKind::Delete => {}
                }
            }
            Ok(ShardOutcome::Rows(_)) => {}
            Err(e) => {
                if let Err(u) = be.run(&cluster, undo, budget).await {
                    tracing::warn!(error = %u, "undoing a failed multi-shard write");
                }
                return Err(e);
            }
        }
    }
    be.run(&cluster, end, budget).await?;
    total.info = match w.kind {
        WriteKind::Insert if w.rows > 1 => {
            format!("Records: {a}  Duplicates: {b}  Warnings: {warn_info}")
        }
        WriteKind::Update => format!("Rows matched: {a}  Changed: {b}  Warnings: {warn_info}"),
        _ => String::new(),
    };
    Ok(Output::Ok(total))
}

// ---------------------------------------------------------------------------
// EXPLAIN FORMAT=SPRIG

/// A VARCHAR column for answers the router makes itself.
#[must_use]
pub fn text_column(name: &str) -> ColumnDef {
    ColumnDef {
        name: Bytes::copy_from_slice(name.as_bytes()),
        org_name: Bytes::copy_from_slice(name.as_bytes()),
        charset: 45,
        length: 4096,
        col_type: 253,
        ..ColumnDef::default()
    }
}

/// EXPLAIN rows: id, parent, operator, target, detail.
#[derive(Default)]
struct ExplainRows(Vec<[String; 5]>);

impl ExplainRows {
    fn push(&mut self, parent: Option<usize>, op: &str, target: String, detail: String) -> usize {
        let id = self.0.len() + 1;
        self.0.push([
            id.to_string(),
            parent.map_or_else(|| "NULL".into(), |x| x.to_string()),
            op.into(),
            target,
            detail,
        ]);
        id
    }

    fn route(&mut self, parent: Option<usize>, r: &ReadPlan) -> usize {
        let mut parent = parent;
        if r.offset > 0 || r.limit.is_some() {
            parent = Some(self.push(
                parent,
                "Limit",
                "gateway".into(),
                format!(
                    "offset {}, count {}",
                    r.offset,
                    r.limit.map_or_else(|| "all".into(), |l| l.to_string())
                ),
            ));
        }
        match r.kind {
            ReadKind::Merge => {
                parent = Some(self.push(
                    parent,
                    "MergeSort",
                    "gateway".into(),
                    format!("{} keys (weights, PAD SPACE)", r.order.len()),
                ));
            }
            ReadKind::Aggregate => {
                let a = r.agg.as_ref();
                parent = Some(self.push(
                    parent,
                    "Aggregate",
                    "gateway".into(),
                    format!(
                        "{} group keys, {} aggregates{}",
                        a.map_or(0, |a| a.groups.len()),
                        a.map_or(0, |a| a.aggs.len()),
                        if a.is_some_and(|a| a.having.is_some()) {
                            ", HAVING"
                        } else {
                            ""
                        }
                    ),
                ));
            }
            ReadKind::Scatter => {}
        }
        let detail = r.targets.first().map(|t| t.sql.clone()).unwrap_or_default();
        self.push(
            parent,
            "Route",
            format!("group {}: {} shards", r.group, r.targets.len()),
            match r.pushed_limit {
                Some(l) => format!("{detail}  [LIMIT {l} per shard]"),
                None => detail,
            },
        )
    }
}

async fn explain(p: &Plan, be: &mut dyn ShardBackend, budget: &mut Budget) -> Result<Output> {
    let mut rows = ExplainRows::default();
    let mut shard_sql: Vec<&Target> = Vec::new();
    match p {
        Plan::Pass(t) => {
            rows.push(None, "Route", format!("shard {}", t.shard), t.sql.clone());
            shard_sql.push(t);
        }
        Plan::Read(r) => {
            rows.route(None, r);
            shard_sql.extend(r.targets.first());
        }
        Plan::LookupJoin(j) => {
            let top = rows.push(
                None,
                "LookupJoin",
                "gateway".into(),
                format!(
                    "{} join, batches of {}",
                    if j.left { "left" } else { "inner" },
                    j.batch
                ),
            );
            rows.route(Some(top), &j.outer);
            rows.push(
                Some(top),
                "Lookup",
                format!(
                    "group {}: {}",
                    j.inner_group.id,
                    if j.inner_by_key {
                        "keys to their shards"
                    } else {
                        "every shard"
                    }
                ),
                j.inner_template
                    .replace('\u{1}', "")
                    .replacen("`S`", "<shard>", 1)
                    .replacen("(K)", "(…)", 1),
            );
            shard_sql.extend(j.outer.targets.first());
        }
        Plan::Write(w) => {
            let top = rows.push(
                None,
                "Write",
                "gateway".into(),
                format!(
                    "{:?} on {} shards, one transaction",
                    w.kind,
                    w.targets.len()
                ),
            );
            for t in &w.targets {
                rows.push(
                    Some(top),
                    "Shard",
                    format!("shard {}", t.shard),
                    t.sql.clone(),
                );
            }
        }
        Plan::Reject(r) => {
            rows.push(
                None,
                "Reject",
                "gateway".into(),
                format!("{}: {}", r.reason.as_str(), r.detail),
            );
        }
        Plan::Explain(_) | Plan::NotRouted => {}
    }
    // One representative shard's MariaDB plan.
    for t in shard_sql {
        let parent = rows.0.len();
        let json = match be
            .run(
                &t.cluster,
                &format!("EXPLAIN FORMAT=JSON {}", t.sql),
                budget,
            )
            .await
        {
            Ok(ShardOutcome::Rows(r)) => r
                .rows
                .first()
                .and_then(|row| row.first().cloned().flatten())
                .map(|b| String::from_utf8_lossy(&b).into_owned())
                .unwrap_or_default(),
            Ok(ShardOutcome::Ok(_)) => String::new(),
            Err(e) => format!("(EXPLAIN failed: {e})"),
        };
        rows.push(Some(parent), "Shard", format!("shard {}", t.shard), json);
    }
    let columns = ["id", "parent", "operator", "target", "detail"]
        .iter()
        .map(|n| text_column(n))
        .collect();
    Ok(Output::Rows {
        columns,
        rows: rows
            .0
            .into_iter()
            .map(|r| r.into_iter().map(|s| Some(Bytes::from(s))).collect())
            .collect(),
        warnings: 0,
    })
}

#[cfg(test)]
mod tests;
