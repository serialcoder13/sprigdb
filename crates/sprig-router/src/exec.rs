//! The executor (chapter 17, "Scatter" to "Lookup joins"): runs a plan's
//! per-shard statements through a [`ShardBackend`] and combines the
//! answers into one result.
//!
//! Outside a transaction, the shards of a read run in parallel, each on a
//! backend connection of its own ([`ShardBackend::open_stream`]), at most
//! `router.max_parallel_shards` at once. A scatter streams: rows go to the
//! client's [`RowSink`] as they arrive, through a bounded buffer whose
//! bytes are charged to `router.max_statement_memory_mb` until written.
//! Merges, aggregates and lookup joins gather their shards' rows in
//! parallel, charged to the same budget (39422 past it), and combine them
//! in gateway memory. Inside a transaction every statement runs on the
//! session's own connection to each cluster, one shard after another, so it
//! sees the transaction.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use bytes::Bytes;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::ColumnDef;

use crate::collation;
use sprig_shard::KeyValueOwned;

use crate::plan::{
    AggCall, AggFunc, AggPlan, Claim, CmpOp, ColRef, FinalSort, JoinCol, KeyCols, LookupJoinPlan,
    LookupProbe, Operand, OutCol, ParentHits, Plan, Pred, ReadKind, ReadPlan, SortKey, Target,
    ViaLookup, ViaParent, WriteKind, WritePlan,
};
use crate::reject::{Reason, Rejection};
use crate::topology::{KeyClass, KeyValue, LookupRef};
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
/// over the session's backend connections (one per cluster).
#[async_trait::async_trait]
pub trait ShardBackend: Send {
    /// Runs `sql` on the shard of `target` (its cluster; inside a
    /// transaction, that cluster's branch) and returns the whole answer,
    /// charging rows to `budget`.
    ///
    /// # Errors
    /// The backend's error (an ERR packet becomes a `Backend` error), or
    /// what the transaction coordinator returns instead.
    async fn run(
        &mut self,
        target: &Target,
        sql: &str,
        budget: &mut Budget,
    ) -> Result<ShardOutcome>;

    /// Whether the session is inside a transaction (writes then use a
    /// statement savepoint instead of their own transaction).
    fn in_transaction(&self) -> bool;

    /// `fk.lock_wait_seconds` (chapter 19): how long a foreign-key probe
    /// waits for a row lock (`… LOCK IN SHARE MODE WAIT n`); `None` waits
    /// as the connection's `innodb_lock_wait_timeout` says.
    fn fk_lock_wait(&self) -> Option<u64> {
        None
    }

    /// Before a multi-shard write on `targets`: chapter 18's write rule,
    /// then an implicit transaction (autocommit) or a statement savepoint.
    /// The default runs `BEGIN` or `SAVEPOINT __sprig_stmt` on the first
    /// target's cluster (backends without a coordinator, tests).
    ///
    /// # Errors
    /// 39440 or 39444 before anything ran; the backend's errors.
    async fn begin_write(&mut self, targets: &[Target], budget: &mut Budget) -> Result<()> {
        let Some(first) = targets.first() else {
            return Ok(());
        };
        let sql = if self.in_transaction() {
            "SAVEPOINT __sprig_stmt"
        } else {
            "BEGIN"
        };
        self.run(first, sql, budget).await.map(|_| ())
    }

    /// After a multi-shard write: commit (or roll back) the implicit
    /// transaction, or release (or roll back to) the statement savepoint.
    ///
    /// # Errors
    /// The backend's or the coordinator's errors.
    async fn end_write(&mut self, targets: &[Target], ok: bool, budget: &mut Budget) -> Result<()> {
        let Some(first) = targets.first() else {
            return Ok(());
        };
        let sql = match (self.in_transaction(), ok) {
            (true, true) => "RELEASE SAVEPOINT __sprig_stmt",
            (true, false) => "ROLLBACK TO SAVEPOINT __sprig_stmt",
            (false, true) => "COMMIT",
            (false, false) => "ROLLBACK",
        };
        self.run(first, sql, budget).await.map(|_| ())
    }

    /// A backend connection of its own to `cluster`, for one parallel
    /// stream of a read; `None` when statements must stay on the session's
    /// connections (inside a transaction, or a backend without streams).
    ///
    /// # Errors
    /// The backend's errors.
    async fn open_stream(&mut self, cluster: &str) -> Result<Option<Box<dyn ShardStream>>> {
        let _ = cluster;
        Ok(None)
    }

    /// `router.max_parallel_shards`.
    fn max_parallel(&self) -> usize {
        1
    }

    /// A connection of its own on the session's cluster, with the
    /// session's state, for a gathered read (R3); it serves inside a
    /// transaction too (it reads only its own temporary tables). `None`
    /// when the backend can't open one (the statement is refused as
    /// before).
    ///
    /// # Errors
    /// The backend's errors.
    async fn open_scratch(&mut self) -> Result<Option<Box<dyn crate::gather::Scratch>>> {
        Ok(None)
    }

    /// `router.gather.*` for this statement; the default turns gathering
    /// off.
    fn gather_limits(&self) -> crate::gather::GatherLimits {
        crate::gather::GatherLimits::default()
    }

    /// Whether the session holds temporary tables of its own (a gathered
    /// read that also reads unsharded tables would miss them).
    fn has_temporary_tables(&self) -> bool {
        false
    }

    /// Chapter 19's `claim` of one value an inserted row holds, in the
    /// write's transaction (after [`Self::begin_write`]). The default
    /// refuses: a backend without lookup sessions can't keep them exact.
    ///
    /// # Errors
    /// 1062 when another row holds the value; 39461; lock waits.
    async fn lookup_claim(&mut self, claim: &Claim) -> Result<()> {
        let _ = claim;
        Err(rejected(
            Reason::Lookup,
            "this backend doesn't maintain lookups",
        ))
    }

    /// Chapter 19's `release` of a value a deleted row held (buffered
    /// until the transaction commits).
    ///
    /// # Errors
    /// Normalization errors; the default refuses.
    async fn lookup_release(
        &mut self,
        lookup: &LookupRef,
        value: Vec<KeyValueOwned>,
        owner_pk: Vec<KeyValueOwned>,
    ) -> Result<()> {
        let _ = (lookup, value, owner_pk);
        Err(rejected(
            Reason::Lookup,
            "this backend doesn't maintain lookups",
        ))
    }

    /// `@@foreign_key_checks` of the session: when off, the gateway's
    /// foreign-key checks and actions are skipped (chapter 19).
    fn foreign_key_checks(&self) -> bool {
        true
    }

    /// The routing hashes of the rows holding `probe`'s values (chapter
    /// 19's `route`), as the session's transaction sees them.
    ///
    /// # Errors
    /// 39461 when a lookup shard is unreachable; the default refuses.
    async fn lookup_owners(&mut self, probe: &LookupProbe) -> Result<Vec<u64>> {
        let _ = probe;
        Err(rejected(
            Reason::Lookup,
            "this backend doesn't read lookups",
        ))
    }
}

/// One shard statement on a backend connection of its own, read row by
/// row (chapter 17's `ShardStream`).
#[async_trait::async_trait]
pub trait ShardStream: Send {
    /// Sends `sql`; the result's column definitions, or `None` for an OK
    /// packet.
    ///
    /// # Errors
    /// The backend's error.
    async fn start(&mut self, sql: &str) -> Result<Option<Vec<ColumnDef>>>;

    /// The next row, `None` after the last.
    ///
    /// # Errors
    /// The backend's error (an ERR packet after rows).
    async fn next_row(&mut self) -> Result<Option<Row>>;

    /// Warnings, once the last row was read.
    fn warnings(&self) -> u16;
}

/// Where a streamed scatter's rows go (the gateway encodes them for the
/// client as they arrive).
#[async_trait::async_trait]
pub trait RowSink: Send {
    /// The result's visible columns, before the first row.
    ///
    /// # Errors
    /// The client is gone.
    async fn columns(&mut self, columns: &[ColumnDef]) -> Result<()>;

    /// One row, visible columns only.
    ///
    /// # Errors
    /// The client is gone.
    async fn row(&mut self, row: &[Option<Bytes>]) -> Result<()>;
}

/// The combined answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output {
    /// The rows went to the [`RowSink`] already.
    Streamed {
        /// Warnings, summed.
        warnings: u16,
    },
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
    let narrowed;
    let plan = match plan {
        Plan::ViaLookup(v) => {
            narrowed = narrow(v, be).await?;
            &narrowed
        }
        Plan::ViaParent(v) => {
            narrowed = Box::pin(parents(v, be, budget)).await?;
            &narrowed
        }
        p => p,
    };
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
        Plan::Gather(g) => gather_or_analytics(g, be, budget, None).await,
        Plan::Pass(t) => match be.run(t, &t.sql, budget).await? {
            ShardOutcome::Rows(r) => Ok(Output::Rows {
                columns: r.columns,
                rows: r.rows,
                warnings: r.warnings,
            }),
            ShardOutcome::Ok(ok) => Ok(Output::Ok(ok)),
        },
        Plan::Reject(r) => Err(r.error()),
        Plan::NotRouted | Plan::ViaLookup(_) | Plan::ViaParent(_) | Plan::NeedIds(_) => Err(
            SprigError::internal("execute of a statement the router doesn't route"),
        ),
    }
}

/// A parent member's `INSERT`, placed: where each row's parent is, read
/// through the parent's unique lookup or from every shard, then each row
/// sent to its parent's shard (`sprig_router_parent_routes_total`).
async fn parents(v: &ViaParent, be: &mut dyn ShardBackend, budget: &mut Budget) -> Result<Plan> {
    let n = v.probes.len();
    let mut hits: Vec<ParentHits> = vec![Vec::new(); n];
    let mut fk_checks: Option<bool> = None;
    let flag = |b: &[u8]| b != b"0";
    if let Some((lookup, values)) = &v.lookup {
        for (p, value) in values.iter().enumerate() {
            let probe = LookupProbe {
                lookup: Arc::clone(lookup),
                values: vec![vec![value.clone()]],
            };
            for h in be.lookup_owners(&probe).await? {
                let s = v.group.route(h);
                if !hits[p].iter().any(|(x, _)| *x == s) {
                    hits[p].push((s, Some(h)));
                }
            }
        }
    } else {
        let probes: Vec<usize> = (0..n).collect();
        for (i, sh) in v.group.shards.iter().enumerate() {
            for (c, chunk) in probes.chunks(v.batch.max(1)).enumerate() {
                let (Some(&a), Some(&b)) = (chunk.first(), chunk.last()) else {
                    continue;
                };
                let mut sql = v.lookup_sql(&sh.schema, a..b + 1);
                if i == 0 && c == 0 {
                    sql.push_str(" UNION ALL SELECT -1, @@foreign_key_checks");
                }
                let t = Target {
                    cluster: sh.cluster.clone(),
                    shard: sh.shard,
                    sql: sql.clone(),
                    at: v.group.at(i),
                };
                let ShardOutcome::Rows(r) = be.run(&t, &sql, budget).await? else {
                    return Err(SprigError::internal("a parent lookup answered no rows"));
                };
                for row in r.rows {
                    let p = row
                        .first()
                        .and_then(Option::as_ref)
                        .and_then(|b| std::str::from_utf8(b).ok())
                        .and_then(|s| s.parse::<i64>().ok());
                    let val = row.get(1).and_then(Option::as_ref);
                    match p {
                        Some(-1) => fk_checks = Some(val.is_none_or(|b| flag(b))),
                        Some(p) => {
                            let rk = val
                                .and_then(|b| std::str::from_utf8(b).ok())
                                .and_then(|s| s.parse::<u64>().ok());
                            if let Some(h) = usize::try_from(p).ok().and_then(|p| hits.get_mut(p))
                                && !h.iter().any(|(x, _)| *x == i)
                            {
                                h.push((i, rk));
                            }
                        }
                        None => {}
                    }
                }
            }
        }
    }
    let fk_checks = match fk_checks {
        Some(f) => f,
        None if hits.iter().any(Vec::is_empty) => {
            let t = Target {
                cluster: v
                    .group
                    .shards
                    .first()
                    .map(|s| s.cluster.clone())
                    .unwrap_or_default(),
                shard: v.group.shards.first().map_or(0, |s| s.shard),
                sql: "SELECT @@foreign_key_checks".into(),
                at: v.group.at(0),
            };
            match be.run(&t, &t.sql, budget).await? {
                ShardOutcome::Rows(r) => r
                    .rows
                    .first()
                    .and_then(|row| row.first().cloned().flatten())
                    .is_none_or(|b| flag(&b)),
                ShardOutcome::Ok(_) => true,
            }
        }
        None => true,
    };
    metrics::counter!("sprig_router_parent_routes_total").increment(1);
    v.finish(&hits, fk_checks).map_err(|r| r.error())
}

/// A lookup-narrowed plan, narrowed: the owners' hashes read through the
/// backend (`sprig_router_lookup_routes_total`).
async fn narrow(v: &ViaLookup, be: &mut dyn ShardBackend) -> Result<Plan> {
    let rks = be.lookup_owners(&v.probe).await?;
    metrics::counter!("sprig_router_lookup_routes_total").increment(1);
    Ok(v.narrow(&rks))
}

/// Runs `plan` as [`execute`] does, except that a scatter outside a
/// transaction streams its rows to `sink` from parallel shard streams
/// ([`Output::Streamed`]); other plans return their rows.
///
/// # Errors
/// As [`execute`]. An error after rows went to `sink` ends the result.
pub async fn execute_to(
    plan: &Plan,
    be: &mut dyn ShardBackend,
    budget: &mut Budget,
    sink: &mut dyn RowSink,
) -> Result<Output> {
    let narrowed;
    let plan = match plan {
        Plan::ViaLookup(v) => {
            narrowed = narrow(v, be).await?;
            &narrowed
        }
        Plan::ViaParent(v) => {
            narrowed = Box::pin(parents(v, be, budget)).await?;
            &narrowed
        }
        p => p,
    };
    if let Plan::Read(r) = plan
        && r.kind == ReadKind::Scatter
        && let Some(streams) = open_streams(&r.targets, be).await?
    {
        metrics::counter!("sprig_router_statements_total", "plan" => "scatter").increment(1);
        metrics::histogram!("sprig_router_fanout_shards")
            .record(u32::try_from(r.targets.len()).map_or(f64::MAX, f64::from));
        return stream_scatter(r, streams, be.max_parallel(), budget, sink).await;
    }
    if let Plan::Gather(g) = plan {
        metrics::counter!("sprig_router_statements_total", "plan" => "gather").increment(1);
        return gather_or_analytics(g, be, budget, Some(sink)).await;
    }
    execute(plan, be, budget).await
}

/// A statement the planner gathers: a gathered read or an analytic read
/// (chapter 17, "Analytic reads (R3)", the selection rule):
/// `router.analytics.mode = prefer` or the hint `/*+ SPRIG_ANALYTICS */`
/// tries the analytic read first and gathers when its guard refuses;
/// `fallback` (the default) gathers first and reads analytically when the
/// gathered read would copy more than `router.gather.max_rows` (39421) or
/// gathering is off; `/*+ SPRIG_GATHER */` and `off` only gather.
async fn gather_or_analytics(
    g: &crate::gather::GatherPlan,
    be: &mut dyn ShardBackend,
    budget: &mut Budget,
    mut sink: Option<&mut dyn RowSink>,
) -> Result<Output> {
    use crate::analytics::{AnalyticsMode, Outcome};
    let lim = be.gather_limits();
    let hint = crate::analytics::hint(&g.sql);
    let mode = lim.analytics.mode;
    if mode == AnalyticsMode::Off || hint == Some(false) {
        return crate::gather::execute(g, be, budget, sink).await;
    }
    let first = mode == AnalyticsMode::Prefer || hint == Some(true) || lim.max_rows == 0;
    if first {
        match crate::analytics::execute(g, be, budget, reborrow(&mut sink)).await? {
            Outcome::Done(o) => {
                metrics::counter!("sprig_router_statements_total", "plan" => "analytics")
                    .increment(1);
                return Ok(o);
            }
            Outcome::Refused(_) => {}
        }
        return crate::gather::execute(g, be, budget, sink).await;
    }
    match crate::gather::execute(g, be, budget, reborrow(&mut sink)).await {
        Err(e) if e.code() == Some(sprig_common::SprigCode::PlanLimitExceeded) => {
            match crate::analytics::execute(g, be, budget, sink).await? {
                Outcome::Done(o) => {
                    metrics::counter!("sprig_router_statements_total", "plan" => "analytics")
                        .increment(1);
                    Ok(o)
                }
                Outcome::Refused(why) => {
                    let mut e = e;
                    e.message = format!("{}; an analytic read was refused: {why}", e.message);
                    Err(e)
                }
            }
        }
        r => r,
    }
}

fn reborrow<'s>(sink: &'s mut Option<&mut dyn RowSink>) -> Option<&'s mut dyn RowSink> {
    match sink {
        Some(s) => Some(&mut **s),
        None => None,
    }
}

/// What a scatter's stream tasks report.
enum Ev {
    Head(Vec<ColumnDef>),
    Row(Row, u32),
    Done(u16),
    Failed(SprigError),
}

/// A row's share of the stream buffer, in KiB.
fn kib(r: &Row) -> u32 {
    u32::try_from(row_bytes(r) / 1024 + 1).unwrap_or(u32::MAX)
}

/// A scatter, streamed: each shard's rows go to `sink` as they arrive
/// (rows of one shard stay in order). Rows wait in a buffer of at most the
/// statement's memory budget; a full buffer stops the streams reading
/// (backpressure). `OFFSET` and `LIMIT` apply here; the rest of each shard's
/// answer (at most `m+n` rows, pushed down) is drained so connections stay
/// reusable.
async fn stream_scatter(
    plan: &ReadPlan,
    streams: Vec<Box<dyn ShardStream>>,
    width: usize,
    budget: &Budget,
    sink: &mut dyn RowSink,
) -> Result<Output> {
    let total_kib = u32::try_from((budget.left / 1024).max(1)).unwrap_or(u32::MAX >> 4);
    let total_kib =
        total_kib.min(u32::try_from(tokio::sync::Semaphore::MAX_PERMITS).unwrap_or(u32::MAX));
    let buffer = Arc::new(tokio::sync::Semaphore::new(total_kib as usize));
    let gate = Arc::new(tokio::sync::Semaphore::new(width.max(1)));
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Ev>(64);
    let mut set = tokio::task::JoinSet::new();
    let n = streams.len();
    for (mut s, t) in streams.into_iter().zip(&plan.targets) {
        let (tx, buffer, gate, sql) = (
            tx.clone(),
            Arc::clone(&buffer),
            Arc::clone(&gate),
            t.sql.clone(),
        );
        set.spawn(async move {
            let Ok(_slot) = gate.acquire_owned().await else {
                return;
            };
            let run = async {
                let cols = s
                    .start(&sql)
                    .await?
                    .ok_or_else(|| SprigError::internal("a shard read answered with OK"))?;
                if tx.send(Ev::Head(cols)).await.is_err() {
                    return Ok(None);
                }
                while let Some(r) = s.next_row().await? {
                    let k = kib(&r);
                    if k > total_kib {
                        return Err(over_budget());
                    }
                    let Ok(p) = buffer.acquire_many(k).await else {
                        return Ok(None);
                    };
                    p.forget();
                    if tx.send(Ev::Row(r, k)).await.is_err() {
                        return Ok(None);
                    }
                }
                Ok(Some(s.warnings()))
            };
            let ev = match run.await {
                Ok(Some(w)) => Ev::Done(w),
                Ok(None) => return,
                Err(e) => Ev::Failed(e),
            };
            let _ = tx.send(ev).await;
        });
    }
    drop(tx);
    let mut columns: Option<Vec<ColumnDef>> = None;
    let mut visible = 0usize;
    let mut skip = plan.offset;
    let mut left = plan.limit.unwrap_or(u64::MAX);
    let mut warnings: u16 = 0;
    let mut done = 0usize;
    while done < n {
        let Some(ev) = rx.recv().await else {
            break;
        };
        match ev {
            Ev::Head(c) => match &columns {
                None => {
                    visible = c
                        .len()
                        .checked_sub(plan.hidden)
                        .ok_or_else(|| SprigError::internal("fewer columns than hidden ones"))?;
                    sink.columns(&c[..visible]).await?;
                    columns = Some(c);
                }
                Some(first) => {
                    if let Err(e) = same_columns(first, &c) {
                        set.abort_all();
                        return Err(e);
                    }
                }
            },
            Ev::Row(r, k) => {
                let out = if skip > 0 {
                    skip -= 1;
                    Ok(())
                } else if left > 0 {
                    left -= 1;
                    sink.row(&r[..visible.min(r.len())]).await
                } else {
                    Ok(())
                };
                buffer.add_permits(k as usize);
                if let Err(e) = out {
                    set.abort_all();
                    return Err(e);
                }
            }
            Ev::Done(w) => {
                done += 1;
                warnings = warnings.saturating_add(w);
            }
            Ev::Failed(e) => {
                set.abort_all();
                return Err(e);
            }
        }
    }
    if done < n {
        return Err(SprigError::internal(
            "a shard stream ended without an answer",
        ));
    }
    if columns.is_none() {
        return Err(SprigError::internal("a scatter without column definitions"));
    }
    Ok(Output::Streamed { warnings })
}

async fn rows_of(t: &Target, be: &mut dyn ShardBackend, budget: &mut Budget) -> Result<ShardRows> {
    match be.run(t, &t.sql, budget).await? {
        ShardOutcome::Rows(r) => Ok(r),
        ShardOutcome::Ok(_) => Err(SprigError::internal("a shard read answered with OK")),
    }
}

/// The bytes a row holds in gateway memory, for the budget.
fn row_bytes(r: &Row) -> u64 {
    r.iter()
        .map(|v| v.as_ref().map_or(0, Bytes::len) as u64 + 16)
        .sum()
}

/// The streams of `targets` (in target order), or `None` when they must
/// run on the session's connections.
async fn open_streams(
    targets: &[Target],
    be: &mut dyn ShardBackend,
) -> Result<Option<Vec<Box<dyn ShardStream>>>> {
    if targets.len() < 2 || be.max_parallel() < 2 {
        return Ok(None);
    }
    let mut out = Vec::with_capacity(targets.len());
    for t in targets {
        match be.open_stream(&t.cluster).await? {
            Some(s) => out.push(s),
            None => return Ok(None),
        }
    }
    Ok(Some(out))
}

/// One stream's whole answer, its rows charged to the shared budget.
async fn collect(
    mut s: Box<dyn ShardStream>,
    sql: String,
    left: Arc<AtomicU64>,
) -> Result<ShardRows> {
    let columns = s
        .start(&sql)
        .await?
        .ok_or_else(|| SprigError::internal("a shard read answered with OK"))?;
    let mut rows = Vec::new();
    while let Some(r) = s.next_row().await? {
        take(&left, row_bytes(&r))?;
        rows.push(r);
    }
    Ok(ShardRows {
        columns,
        rows,
        warnings: s.warnings(),
    })
}

fn over_budget() -> SprigError {
    SprigError::new(
        SprigCode::RouterMemoryLimit,
        "The statement's rows held in the gateway passed router.max_statement_memory_mb",
    )
}

/// Takes `n` bytes from a shared budget.
fn take(left: &AtomicU64, n: u64) -> Result<()> {
    left.fetch_update(AtomicOrdering::Relaxed, AtomicOrdering::Relaxed, |l| {
        l.checked_sub(n)
    })
    .map(|_| ())
    .map_err(|_| over_budget())
}

/// Every target's answer, in target order: in parallel on streams of their
/// own when the backend offers them (at most `max_parallel()` at once),
/// else one after another on the session's connections.
async fn gather(
    targets: &[Target],
    be: &mut dyn ShardBackend,
    budget: &mut Budget,
) -> Result<Vec<ShardRows>> {
    let Some(streams) = open_streams(targets, be).await? else {
        let mut out = Vec::with_capacity(targets.len());
        for t in targets {
            out.push(rows_of(t, be, budget).await?);
        }
        return Ok(out);
    };
    let left = Arc::new(AtomicU64::new(budget.left));
    let width = be.max_parallel().max(1);
    let mut results: Vec<Option<ShardRows>> = vec![None; targets.len()];
    let mut set = tokio::task::JoinSet::new();
    let mut pending = streams.into_iter().zip(targets).enumerate();
    let mut running = 0usize;
    loop {
        while running < width {
            let Some((i, (s, t))) = pending.next() else {
                break;
            };
            let left = Arc::clone(&left);
            let sql = t.sql.clone();
            set.spawn(async move { (i, collect(s, sql, left).await) });
            running += 1;
        }
        let Some(done) = set.join_next().await else {
            break;
        };
        running -= 1;
        let (i, r) = done.map_err(|e| SprigError::internal(format!("shard stream task: {e}")))?;
        match r {
            Ok(rows) => results[i] = Some(rows),
            Err(e) => {
                set.abort_all();
                return Err(e);
            }
        }
    }
    budget.left = left.load(AtomicOrdering::Relaxed);
    results
        .into_iter()
        .map(|r| r.ok_or_else(|| SprigError::internal("a shard stream without an answer")))
        .collect()
}

/// Checks that a shard's columns match the first shard's.
fn same_columns(first: &[ColumnDef], other: &[ColumnDef]) -> Result<()> {
    if first.len() != other.len()
        || first
            .iter()
            .zip(other)
            .any(|(a, b)| a.col_type != b.col_type || a.charset != b.charset)
    {
        return Err(rejected(
            Reason::SchemaSkew,
            "shards disagree on result columns",
        ));
    }
    Ok(())
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
    for r in gather(&plan.targets, be, budget).await? {
        match &columns {
            None => columns = Some(r.columns),
            Some(c) => same_columns(c, &r.columns)?,
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
    // A sort or group-by of many rows runs off the runtime's worker (G40,
    // decision bugfix-1): `router.max_statement_memory_mb` allows millions.
    let n = rows.len();
    let rows = sprig_common::cpu::heavy(n, || -> Result<Vec<Row>> {
        Ok(match plan.kind {
            ReadKind::Scatter => window(rows, plan.offset, plan.limit),
            ReadKind::Merge => {
                let classes =
                    key_classes(plan.order.iter().map(|k| &k.cols), &columns, &rows, &at)?;
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
        })
    })?;
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
        let inner: Vec<Target> = per
            .iter()
            .enumerate()
            .filter(|(_, lits)| !lits.is_empty())
            .map(|(s, lits)| {
                let loc = &j.inner_group.shards[s];
                Target {
                    cluster: loc.cluster.clone(),
                    shard: loc.shard,
                    at: j.inner_group.at(s),
                    sql: j
                        .inner_template
                        .replace("\u{1}S", &loc.schema.replace('`', "``"))
                        .replace("\u{1}K", &lits.join(", ")),
                }
            })
            .collect();
        metrics::counter!("sprig_router_lookup_batches_total").increment(inner.len() as u64);
        for r in gather(&inner, be, budget).await? {
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
                    at: j.inner_group.at(0),
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

/// Chapter 19 before a write's rows: the deleted rows' values are read
/// (locked) and released, the inserted rows' values claimed.
async fn lookups_before(
    w: &WritePlan,
    be: &mut dyn ShardBackend,
    budget: &mut Budget,
) -> Result<()> {
    for rel in &w.releases {
        let l = &rel.lookup;
        let mut parts: Vec<sprig_shard::KeyClass> =
            l.index.columns.iter().map(|c| c.class).collect();
        parts.extend(l.index.owner_pk.iter().map(|c| c.class));
        for t in &rel.reads {
            let ShardOutcome::Rows(rows) = be.run(t, &t.sql, budget).await? else {
                continue;
            };
            for row in rows.rows {
                let mut vals = Vec::with_capacity(parts.len());
                for (i, part) in parts.iter().enumerate() {
                    let v = row.get(i).and_then(|v| v.as_deref());
                    vals.push(crate::planner::decode_read(v, *part).ok_or_else(|| {
                        SprigError::internal("a lookup value the release read can't decode")
                    })?);
                }
                let value = vals.drain(..l.index.columns.len()).collect();
                be.lookup_release(l, value, vals).await?;
            }
        }
    }
    // Two rows of one statement with equal values (under the column's
    // collation): the second claim would take the first's value as an
    // orphan, since neither row is written yet. MariaDB refuses the
    // statement; so does the router.
    let mut seen = std::collections::HashSet::new();
    for c in &w.claims {
        if c.value.iter().any(|v| matches!(v, KeyValueOwned::Null)) {
            continue;
        }
        let h = sprig_shard::lookup::value_hash(&c.lookup.index, &c.value)?;
        if !seen.insert((c.lookup.index.id, h)) {
            let shown = c
                .value
                .iter()
                .map(|v| match v {
                    KeyValueOwned::Str { bytes, .. } | KeyValueOwned::Bytes(bytes) => {
                        String::from_utf8_lossy(bytes).into_owned()
                    }
                    KeyValueOwned::Int(i) => i.to_string(),
                    other => format!("{other:?}"),
                })
                .collect::<Vec<_>>()
                .join("-");
            return Err(SprigError::standard(
                1062,
                *b"23000",
                format!(
                    "Duplicate entry '{shown}' for key '{}'",
                    c.lookup.index.index_name
                ),
            ));
        }
    }
    for c in &w.claims {
        be.lookup_claim(c).await?;
    }
    if !w.claims.is_empty() {
        metrics::counter!("sprig_router_lookup_claims_total").increment(w.claims.len() as u64);
    }
    Ok(())
}

/// A statement a foreign-key action planned (chapter 19, R3), run inside
/// the write that caused it: no transaction or savepoint of its own.
///
/// # Errors
/// The shards' errors, 1451, 1452, 39462.
pub(crate) async fn run_nested(
    plan: &Plan,
    be: &mut dyn ShardBackend,
    budget: &mut Budget,
    depth: usize,
) -> Result<()> {
    match plan {
        Plan::Pass(t) => be.run(t, &t.sql, budget).await.map(|_| ()),
        Plan::Write(w) => {
            lookups_before(w, be, budget).await?;
            if let Some(f) = &w.fk {
                Box::pin(crate::fk::before_write(f, be, budget, depth)).await?;
            }
            for t in &w.targets {
                be.run(t, &t.sql, budget).await?;
            }
            Ok(())
        }
        Plan::ViaLookup(v) => {
            let p = narrow(v, be).await?;
            Box::pin(run_nested(&p, be, budget, depth)).await
        }
        Plan::Reject(r) => Err(r.error()),
        _ => Err(SprigError::internal(
            "a foreign-key action planned as something other than a write",
        )),
    }
}

/// R3's limited DML: the rows the write changes, read in order and
/// locked, then each shard's statement naming its rows by primary key
/// (shards without rows run nothing).
async fn limited_targets(
    l: &crate::plan::Limited,
    templates: &[Target],
    be: &mut dyn ShardBackend,
    budget: &mut Budget,
) -> Result<Option<Vec<Target>>> {
    let Output::Rows { rows, .. } = Box::pin(execute(&l.read, be, budget)).await? else {
        return Err(SprigError::internal(
            "a limited write's read answered no rows",
        ));
    };
    let q = |c: &str| format!("`{}`", c.replace('`', "``"));
    let mut conds: HashMap<u32, Vec<String>> = HashMap::new();
    for r in &rows {
        let val = |i: usize| {
            l.classes
                .get(i)
                .and_then(|c| c.value(r.get(i).and_then(|v| v.as_deref())))
        };
        let key = val(0).and_then(|v| v.key);
        let Some(shard) = key
            .as_ref()
            .and_then(|k| l.group.shard_of(k))
            .and_then(|i| l.group.shards.get(i))
            .map(|s| s.shard)
        else {
            return Err(SprigError::internal(
                "a limited write's row without a routable key",
            ));
        };
        let mut parts = Vec::new();
        for (j, c) in l.pk.iter().enumerate() {
            match val(j + 1) {
                Some(v) => parts.push(format!("{} = {}", q(c), v.sql)),
                None => parts.push(format!("{} IS NULL", q(c))),
            }
        }
        conds
            .entry(shard)
            .or_default()
            .push(format!("({})", parts.join(" AND ")));
    }
    Ok(Some(
        templates
            .iter()
            .filter_map(|t| {
                let c = conds.get(&t.shard)?;
                Some(Target {
                    sql: t
                        .sql
                        .replace(crate::plan::LIMITED_ROWS, &format!("({})", c.join(" OR "))),
                    ..t.clone()
                })
            })
            .collect(),
    ))
}

async fn write(w: &WritePlan, be: &mut dyn ShardBackend, budget: &mut Budget) -> Result<Output> {
    metrics::histogram!("sprig_router_fanout_shards")
        .record(u32::try_from(w.targets.len()).map_or(f64::MAX, f64::from));
    let Some(first) = w.targets.first() else {
        return Ok(Output::Ok(ShardOk::default()));
    };
    let _ = first;
    be.begin_write(&w.targets, budget).await?;
    let before = async {
        lookups_before(w, be, budget).await?;
        if let Some(f) = &w.fk {
            crate::fk::before_write(f, be, budget, 0).await?;
        }
        match &w.limited {
            Some(l) => limited_targets(l, &w.targets, be, budget).await,
            None => Ok::<Option<Vec<Target>>, SprigError>(None),
        }
    };
    let limited = match before.await {
        Ok(t) => t,
        Err(e) => {
            if let Err(u) = be.end_write(&w.targets, false, budget).await {
                tracing::warn!(error = %u, "undoing a failed write's lookups");
            }
            return Err(e);
        }
    };
    let mut total = ShardOk::default();
    let (mut a, mut b, mut warn_info) = (0u64, 0u64, 0u64);
    for t in limited.as_ref().unwrap_or(&w.targets) {
        match be.run(t, &t.sql, budget).await {
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
                if let Err(u) = be.end_write(&w.targets, false, budget).await {
                    tracing::warn!(error = %u, "undoing a failed multi-shard write");
                }
                return Err(e);
            }
        }
    }
    be.end_write(&w.targets, true, budget).await?;
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
    let p = match p {
        Plan::ViaLookup(v) => {
            rows.push(
                None,
                "Lookup",
                format!(
                    "{} ({})",
                    v.probe.lookup.index.index_name, v.probe.lookup.column
                ),
                format!(
                    "{} value(s): only the shards of the rows holding them",
                    v.probe.values.len()
                ),
            );
            &v.plan
        }
        Plan::NeedIds(f) => {
            rows.push(
                None,
                "IdBlocks",
                format!("{}.{}", f.table, f.column),
                format!(
                    "{} row(s) take values from the gateway's ID block (sequence {}){}",
                    f.rows.len(),
                    f.seq,
                    f.explicit_max
                        .map(|m| format!(", explicit values raise it past {m}"))
                        .unwrap_or_default()
                ),
            );
            p
        }
        Plan::ViaParent(v) => {
            let top = rows.push(
                None,
                "ParentRoute",
                format!("group {}: {} shards", v.group.id, v.group.shards.len()),
                format!(
                    "{} row(s) of {} follow {} parent key(s) of {}{}",
                    v.row_probe.len(),
                    v.table,
                    v.probes.len(),
                    v.parent,
                    if v.lookup.is_some() {
                        ", found through its unique lookup"
                    } else {
                        ", found by asking every shard"
                    }
                ),
            );
            if let Some(s) = v.group.shards.first() {
                rows.push(
                    Some(top),
                    "Lookup",
                    format!("shard {}", s.shard),
                    v.lookup_sql(&s.schema, 0..v.probes.len().min(v.batch.max(1))),
                );
            }
            p
        }
        p => p,
    };
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
        Plan::Gather(g) => {
            let top = rows.push(
                None,
                "Gather",
                "session's cluster".into(),
                format!(
                    "the statement runs once over temporary copies ({}): {}",
                    g.fallback.reason.as_str(),
                    g.sql
                ),
            );
            for t in &g.tables {
                let detail = match &t.filter {
                    Some(f) => format!("rows where {f}"),
                    None => "every row".into(),
                };
                rows.push(
                    Some(top),
                    "Copy",
                    format!(
                        "{}.{}: {} of {} shards",
                        t.schema,
                        t.name,
                        t.sources.len(),
                        t.shards_total
                    ),
                    detail,
                );
            }
        }
        Plan::Explain(_)
        | Plan::NotRouted
        | Plan::ViaLookup(_)
        | Plan::ViaParent(_)
        | Plan::NeedIds(_) => {}
    }
    // One representative shard's MariaDB plan.
    for t in shard_sql {
        let parent = rows.0.len();
        let json = match be
            .run(t, &format!("EXPLAIN FORMAT=JSON {}", t.sql), budget)
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
