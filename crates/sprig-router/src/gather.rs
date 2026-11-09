//! Gathered reads (chapter 17, "Gathered reads (R3)"): a `SELECT` the
//! router can't split exactly runs once, in MariaDB, over copies of the
//! rows it reads.
//!
//! ```text
//! exec_gather(plan):
//!   scratch = a new connection to the session's cluster, with its state
//!   for each sharded or reference table T the statement names:
//!       CREATE TEMPORARY TABLE <T's schema>.<T> from a shard's SHOW CREATE TABLE
//!           (it hides the base table of that name for this connection only)
//!       for each shard of T (pruned by key equalities):
//!           same cluster, outside a transaction: INSERT INTO T SELECT … FROM shard
//!           otherwise: read the rows (inside a transaction, on its branch) and
//!                      INSERT them as typed literals
//!   run the statement's own text on scratch; relay its result
//!   DROP TEMPORARY TABLE …
//! ```
//!
//! MariaDB evaluates the whole statement, so its answer is the one one
//! unsharded MariaDB gives: window functions, subqueries, set operations,
//! any join condition, collations, `NULL`s, number types and functions,
//! and the column definitions too. The copies keep every value exactly:
//! strings and other bytes as `X'…'` of the column's own bytes, `FLOAT`
//! through `DOUBLE`, `TIMESTAMP` as its Unix time inserted at `+00:00`
//! (no daylight-saving gaps), dates and times as their text.
//!
//! Rows a shard filter can drop are dropped there: a table named once in
//! the statement's top `SELECT`, not on the null side of an outer join,
//! reads only the shards its key equalities allow and only the rows its own
//! `WHERE` conjuncts keep (simple comparisons the router prints exactly).
//! The statement still applies its whole `WHERE` afterwards.

use std::fmt::Write as _;
use std::sync::OnceLock;

use bytes::Bytes;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::ColumnDef;

use crate::exec::{Budget, Output, Row, RowSink, ShardBackend, ShardOutcome};
use crate::plan::Target;
use crate::reject::{Reason, Rejection};

/// A gathered read: the tables to copy and the statement to run over the
/// copies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatherPlan {
    /// The tables to copy, each once.
    pub tables: Vec<GatherTable>,
    /// The statement's text, as chapter 07's rewrite left it (a prepared
    /// statement's parameters written in as literals).
    pub sql: String,
    /// The statement also names tables the router doesn't distribute
    /// (they live on the session's cluster and are read there directly).
    pub reads_unsharded: bool,
    /// Why the router had no other plan (the error when gathering is off
    /// or impossible).
    pub fallback: Rejection,
}

/// One table to copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatherTable {
    /// The schema the statement names it in (physical: chapter 07's).
    pub schema: String,
    /// The table name, as the statement writes it.
    pub name: String,
    /// Where its rows are: one target per shard to read (a reference
    /// table: one copy). `sql` is empty; `at` anchors transaction
    /// branches.
    pub sources: Vec<Target>,
    /// The alias the filter's columns use (the statement's alias, or the
    /// table name).
    pub alias: String,
    /// Conjuncts of the statement's `WHERE` that only read this table,
    /// printed for the shard (`None`: every row).
    pub filter: Option<String>,
    /// How many shards the table has (`sources` may be fewer).
    pub shards_total: usize,
}

/// A connection of its own on the session's cluster, with the session's
/// state, for one gathered read: its temporary tables hide the base tables
/// of the same names for this connection only.
#[async_trait::async_trait]
pub trait Scratch: Send {
    /// The cluster it is on.
    fn cluster(&self) -> &str;

    /// Runs `sql` and collects the whole answer, charging rows to
    /// `budget`.
    ///
    /// # Errors
    /// The backend's error.
    async fn run(&mut self, sql: &str, budget: &mut Budget) -> Result<ShardOutcome>;

    /// Sends `sql`; its column definitions, or `None` for an OK packet.
    ///
    /// # Errors
    /// The backend's error.
    async fn start(&mut self, sql: &str) -> Result<Option<Vec<ColumnDef>>>;

    /// The next row of the statement [`Self::start`] sent; `None` after the
    /// last.
    ///
    /// # Errors
    /// The backend's error.
    async fn next_row(&mut self) -> Result<Option<Row>>;

    /// The warnings of the last statement, once its last row was read.
    fn warnings(&self) -> u16;

    /// Ends it. `clean`: its temporary tables were dropped, so the
    /// connection may serve others; otherwise it is closed.
    async fn finish(self: Box<Self>, clean: bool);
}

/// How a column is copied: what the shard selects and how the value is
/// written back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Copy {
    /// Integers, `DECIMAL`, `DOUBLE`, `YEAR`: the text as it came.
    Number,
    /// `FLOAT`: selected as `DOUBLE` (exact), written as that number.
    Float,
    /// `TIMESTAMP`: its Unix time, written with `FROM_UNIXTIME` at
    /// `+00:00`; `0` for the zero value.
    Timestamp,
    /// `DATE`, `DATETIME`, `TIME`: the text, quoted.
    Temporal,
    /// Everything else: the column's own bytes (`CAST(c AS BINARY)`),
    /// written as `X'…'`.
    Bytes,
}

/// A column of a temporary table, as `SHOW FULL COLUMNS` describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Col {
    name: String,
    copy: Copy,
}

impl Copy {
    fn of(sql_type: &str) -> Self {
        let t = sql_type.trim().to_ascii_lowercase();
        let base = t
            .split(|c: char| c == '(' || c.is_whitespace())
            .next()
            .unwrap_or_default();
        match base {
            "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "decimal"
            | "numeric" | "double" | "real" | "year" | "bool" | "boolean" | "dec" | "fixed" => {
                Self::Number
            }
            "float" => Self::Float,
            "timestamp" => Self::Timestamp,
            "date" | "datetime" | "time" => Self::Temporal,
            _ => Self::Bytes,
        }
    }
}

fn q(id: &str) -> String {
    format!("`{}`", id.replace('`', "``"))
}

fn hex(b: &[u8]) -> String {
    let mut h = String::with_capacity(b.len() * 2 + 3);
    h.push_str("X'");
    for x in b {
        let _ = write!(h, "{x:02X}");
    }
    h.push('\'');
    h
}

/// `SHOW CREATE TABLE` of a shard table, as a temporary table named
/// `schema`.`name`: foreign keys, `CHECK` constraints, full-text, spatial
/// and vector indexes and partitioning are left out (the rows already
/// passed them; temporary tables can't hold them). `None` for a table a
/// temporary copy can't stand for (system-versioned).
pub(crate) fn temp_ddl(show: &str, schema: &str, name: &str) -> Option<String> {
    let mut lines = show.lines();
    let first = lines.next()?;
    if !first.trim_start().starts_with("CREATE TABLE") || !first.trim_end().ends_with('(') {
        return None;
    }
    let mut body: Vec<String> = Vec::new();
    let mut tail: Option<String> = None;
    for l in lines.by_ref() {
        if l.starts_with(')') {
            tail = Some(l.to_owned());
            break;
        }
        let t = l.trim_start();
        let up = t.to_ascii_uppercase();
        if up.starts_with("CONSTRAINT ")
            || up.starts_with("FOREIGN KEY")
            || up.starts_with("CHECK ")
            || up.starts_with("FULLTEXT ")
            || up.starts_with("SPATIAL ")
            || up.starts_with("VECTOR ")
        {
            continue;
        }
        body.push(l.trim_end().trim_end_matches(',').to_owned());
    }
    let tail = tail?;
    if tail.to_ascii_uppercase().contains("WITH SYSTEM VERSIONING")
        || body
            .iter()
            .any(|l| l.to_ascii_uppercase().contains("AS ROW START"))
    {
        return None;
    }
    // Partitioning comes after the table options, on lines of its own.
    let mut out = format!("CREATE TEMPORARY TABLE {}.{} (\n", q(schema), q(name));
    out.push_str(&body.join(",\n"));
    out.push('\n');
    out.push_str(&tail);
    Some(out)
}

/// The session's `sql_mode` for the copies: `NO_AUTO_VALUE_ON_ZERO` added
/// (a stored 0 stays 0), `NO_ZERO_DATE` and `NO_ZERO_IN_DATE` removed (a
/// stored zero date stays). Everything that changes how the shard's filter
/// reads stays as the session has it. `None` for an unexpected value.
fn copy_mode(session: &str) -> Option<String> {
    let mut v: Vec<&str> = Vec::new();
    for m in session.split(',').map(str::trim).filter(|m| !m.is_empty()) {
        if !m.bytes().all(|b| b.is_ascii_uppercase() || b == b'_') {
            return None;
        }
        if m != "NO_ZERO_DATE" && m != "NO_ZERO_IN_DATE" && !v.contains(&m) {
            v.push(m);
        }
    }
    if !v.contains(&"NO_AUTO_VALUE_ON_ZERO") {
        v.push("NO_AUTO_VALUE_ON_ZERO");
    }
    Some(v.join(","))
}

/// One value, as a literal the temporary table's column takes back
/// unchanged.
fn literal(copy: Copy, v: Option<&[u8]>) -> Result<String> {
    let Some(v) = v else {
        return Ok("NULL".into());
    };
    let text = || {
        std::str::from_utf8(v).map_err(|_| SprigError::internal("a copied value that isn't text"))
    };
    match copy {
        Copy::Number | Copy::Float => {
            let t = text()?;
            if t.is_empty()
                || !t
                    .bytes()
                    .all(|b| b.is_ascii_digit() || matches!(b, b'-' | b'+' | b'.' | b'e' | b'E'))
            {
                return Err(SprigError::internal(format!(
                    "a copied number that isn't one: {t}"
                )));
            }
            Ok(t.to_owned())
        }
        Copy::Timestamp => {
            let t = text()?;
            if !t.bytes().all(|b| b.is_ascii_digit() || b == b'.') || t.is_empty() {
                return Err(SprigError::internal(format!(
                    "a copied TIMESTAMP that isn't a Unix time: {t}"
                )));
            }
            if t.bytes().all(|b| b == b'0' || b == b'.') {
                Ok("0".into())
            } else {
                Ok(format!("FROM_UNIXTIME({t})"))
            }
        }
        Copy::Temporal => {
            let t = text()?;
            if !t
                .bytes()
                .all(|b| b.is_ascii_digit() || matches!(b, b'-' | b':' | b'.' | b' '))
            {
                return Err(SprigError::internal(format!(
                    "a copied date or time that isn't one: {t}"
                )));
            }
            Ok(format!("'{t}'"))
        }
        Copy::Bytes => Ok(hex(v)),
    }
}

/// What a shard selects for column `c`.
fn select_expr(c: &Col) -> String {
    let n = q(&c.name);
    match c.copy {
        Copy::Number | Copy::Temporal => n,
        Copy::Float => format!("CAST({n} AS DOUBLE)"),
        Copy::Timestamp => format!("UNIX_TIMESTAMP({n})"),
        Copy::Bytes => format!("CAST({n} AS BINARY)"),
    }
}

/// The columns a copy writes, read from `SHOW CREATE TABLE`'s column lines
/// (two spaces, then a backquoted name); generated columns left out.
/// Empty when the text doesn't read that way.
fn columns_of_ddl(show: &str) -> Vec<Col> {
    let mut out = Vec::new();
    for l in show.lines().skip(1) {
        let Some(rest) = l.strip_prefix("  `") else {
            continue;
        };
        // The name ends at the first backquote that isn't doubled.
        let b = rest.as_bytes();
        let mut i = 0;
        let mut name = String::new();
        loop {
            match b.get(i) {
                Some(b'`') if b.get(i + 1) == Some(&b'`') => {
                    name.push('`');
                    i += 2;
                }
                Some(b'`') => break,
                Some(_) => {
                    let ch = rest[i..].chars().next().unwrap_or_default();
                    name.push(ch);
                    i += ch.len_utf8();
                }
                None => return Vec::new(),
            }
        }
        let def = rest[i + 1..].trim_start();
        let def_up = def.split(" COMMENT '").next().unwrap_or_default();
        if def_up.contains(" GENERATED ALWAYS AS ") {
            continue;
        }
        if name.is_empty() || def.is_empty() {
            return Vec::new();
        }
        out.push(Col {
            name,
            copy: Copy::of(def),
        });
    }
    out
}

/// `SHOW FULL COLUMNS` rows: the columns a copy writes (generated columns
/// compute themselves).
fn columns_of(rows: &[Row]) -> Result<Vec<Col>> {
    let s = |r: &Row, i: usize| -> String {
        r.get(i)
            .and_then(Option::as_ref)
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .unwrap_or_default()
    };
    let mut out = Vec::new();
    for r in rows {
        let name = s(r, 0);
        let ty = s(r, 1);
        let extra = s(r, 6).to_ascii_uppercase();
        if name.is_empty() {
            return Err(SprigError::internal("SHOW FULL COLUMNS without a name"));
        }
        if extra.contains("GENERATED") {
            continue;
        }
        out.push(Col {
            name,
            copy: Copy::of(&ty),
        });
    }
    Ok(out)
}

/// Gathered reads at once per gateway (`router.gather.max_concurrent`,
/// read when the first one runs).
static GATE: OnceLock<tokio::sync::Semaphore> = OnceLock::new();

/// Bytes per `INSERT … VALUES` of copied rows.
const BATCH_BYTES: usize = 1 << 20;

/// The limits a gathered read runs under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GatherLimits {
    /// `router.gather.max_rows`: rows copied per statement (0: gathering
    /// is off and the statement is refused as before).
    pub max_rows: u64,
    /// `router.gather.max_concurrent`.
    pub max_concurrent: usize,
}

impl Default for GatherLimits {
    fn default() -> Self {
        Self {
            max_rows: 0,
            max_concurrent: 4,
        }
    }
}

fn gather_error(detail: impl Into<String>) -> SprigError {
    Rejection::new(Reason::Gather, detail).error()
}

fn limit_error(max: u64) -> SprigError {
    SprigError::new(
        SprigCode::PlanLimitExceeded,
        format!("A gathered read copied more than router.gather.max_rows ({max}) rows"),
    )
}

/// Runs a gathered read: copies, then the statement over them, its rows to
/// `sink` (or collected when `sink` is `None`).
///
/// # Errors
/// The fallback rejection when gathering is off or the session can't
/// gather (39420); 39421 past `router.gather.max_rows`; backend errors.
pub(crate) async fn execute(
    p: &GatherPlan,
    be: &mut dyn ShardBackend,
    budget: &mut Budget,
    sink: Option<&mut dyn RowSink>,
) -> Result<Output> {
    let limits = be.gather_limits();
    if limits.max_rows == 0 {
        return Err(p.fallback.error());
    }
    if p.reads_unsharded && (be.in_transaction() || be.has_temporary_tables()) {
        // The scratch connection would read the base tables outside the
        // transaction, or miss the session's own temporary tables.
        return Err(p.fallback.error());
    }
    let gate = GATE.get_or_init(|| tokio::sync::Semaphore::new(limits.max_concurrent.max(1)));
    let _permit = gate
        .acquire()
        .await
        .map_err(|_| SprigError::internal("the gather gate closed"))?;
    let Some(mut scratch) = be.open_scratch().await? else {
        return Err(p.fallback.error());
    };
    let started = std::time::Instant::now();
    let mut created: Vec<String> = Vec::new();
    let r = run(p, be, scratch.as_mut(), budget, sink, limits, &mut created).await;
    let clean = if created.is_empty() {
        true
    } else {
        let drop = format!("DROP TEMPORARY TABLE IF EXISTS {}", created.join(", "));
        let mut b = Budget::mb(1);
        scratch.run(&drop, &mut b).await.is_ok()
    };
    scratch.finish(clean && r.is_ok()).await;
    metrics::histogram!("sprig_router_gather_duration_seconds")
        .record(started.elapsed().as_secs_f64());
    r
}

async fn run(
    p: &GatherPlan,
    be: &mut dyn ShardBackend,
    scratch: &mut dyn Scratch,
    budget: &mut Budget,
    sink: Option<&mut dyn RowSink>,
    limits: GatherLimits,
    created: &mut Vec<String>,
) -> Result<Output> {
    // The copies must stay out of the binlog (ROW format never logs
    // temporary tables), and their sql_mode follows the session's.
    let (binlog, mode) = match scratch
        .run("SELECT @@binlog_format, @@sql_mode", budget)
        .await?
    {
        ShardOutcome::Rows(r) => {
            let row = r.rows.into_iter().next().unwrap_or_default();
            let s = |i: usize| {
                row.get(i)
                    .and_then(Option::as_ref)
                    .map(|b| String::from_utf8_lossy(b).into_owned())
                    .unwrap_or_default()
            };
            (s(0), s(1))
        }
        ShardOutcome::Ok(_) => (String::new(), String::new()),
    };
    if !binlog.eq_ignore_ascii_case("ROW") {
        return Err(gather_error(format!(
            "the session's cluster logs {binlog} binlog events; gathering needs ROW"
        )));
    }
    let mode = copy_mode(&mode).ok_or_else(|| gather_error("an unexpected sql_mode"))?;
    let in_txn = be.in_transaction();
    let mut copied: u64 = 0;
    for t in &p.tables {
        let Some(first) = t.sources.first() else {
            continue;
        };
        let src_schema = &first.at.schema;
        let show = format!("SHOW CREATE TABLE {}.{}", q(src_schema), q(&t.name));
        let ddl = match be.run(first, &show, budget).await? {
            ShardOutcome::Rows(r) => r
                .rows
                .first()
                .and_then(|row| row.get(1).cloned().flatten())
                .map(|b| String::from_utf8_lossy(&b).into_owned())
                .unwrap_or_default(),
            ShardOutcome::Ok(_) => String::new(),
        };
        let create = temp_ddl(&ddl, &t.schema, &t.name).ok_or_else(|| {
            gather_error(format!(
                "{}: a table a temporary copy can't stand for",
                t.name
            ))
        })?;
        let target = format!("{}.{}", q(&t.schema), q(&t.name));
        match scratch
            .run(
                &format!("SET STATEMENT sql_mode='{mode}' FOR {create}"),
                budget,
            )
            .await
        {
            Ok(_) => created.push(target.clone()),
            Err(e) if matches!(e.kind, sprig_common::ErrorKind::Backend { errno, .. } if errno == 1044 || errno == 1142) =>
            {
                return Err(gather_error(format!(
                    "the account can't create temporary tables in {} ({})",
                    t.schema, e.message
                )));
            }
            Err(e) => return Err(e),
        }
        // The columns from the definition itself; `SHOW FULL COLUMNS` when
        // it doesn't read.
        let mut cols = columns_of_ddl(&ddl);
        if cols.is_empty() {
            cols = match scratch
                .run(&format!("SHOW FULL COLUMNS FROM {target}"), budget)
                .await?
            {
                ShardOutcome::Rows(r) => columns_of(&r.rows)?,
                ShardOutcome::Ok(_) => Vec::new(),
            };
        }
        if cols.is_empty() {
            return Err(gather_error(format!("{}: no columns to copy", t.name)));
        }
        let names = cols
            .iter()
            .map(|c| q(&c.name))
            .collect::<Vec<_>>()
            .join(", ");
        let exprs = cols.iter().map(select_expr).collect::<Vec<_>>().join(", ");
        let filter = t
            .filter
            .as_ref()
            .map(|f| format!(" WHERE {f}"))
            .unwrap_or_default();
        // Shards on the scratch connection's cluster: one INSERT … SELECT
        // inside MariaDB for all of them.
        let local: Vec<String> = t
            .sources
            .iter()
            .filter(|s| !in_txn && s.cluster == scratch.cluster())
            .map(|s| {
                format!(
                    "SELECT {names} FROM {}.{} AS {}{filter}",
                    q(&s.at.schema),
                    q(&t.name),
                    q(&t.alias)
                )
            })
            .collect();
        if !local.is_empty() {
            let sql = format!(
                "SET STATEMENT sql_mode='{mode}' FOR INSERT INTO {target} ({names}) {}",
                local.join(" UNION ALL ")
            );
            if let ShardOutcome::Ok(ok) = scratch.run(&sql, budget).await? {
                copied += ok.affected;
            }
        }
        // The other shards: read (in parallel outside a transaction, on the
        // transaction's branches inside one), then written as literals.
        let remote: Vec<(&Target, String)> = t
            .sources
            .iter()
            .filter(|s| in_txn || s.cluster != scratch.cluster())
            .map(|s| {
                (
                    s,
                    format!(
                        "SELECT {exprs} FROM {}.{} AS {}{filter}",
                        q(&s.at.schema),
                        q(&t.name),
                        q(&t.alias)
                    ),
                )
            })
            .collect();
        if remote.is_empty() {
            continue;
        }
        let before = budget.left;
        let rows = read_all(be, &remote, budget).await?;
        copied += rows.len() as u64;
        if copied > limits.max_rows {
            return Err(limit_error(limits.max_rows));
        }
        let head = format!(
            "SET STATEMENT sql_mode='{mode}', time_zone='+00:00' FOR INSERT INTO {target} ({names}) VALUES "
        );
        let mut sql = head.clone();
        let mut n = 0usize;
        for row in &rows {
            if n > 0 {
                sql.push(',');
            }
            sql.push('(');
            for (i, c) in cols.iter().enumerate() {
                if i > 0 {
                    sql.push(',');
                }
                sql.push_str(&literal(c.copy, row.get(i).and_then(|v| v.as_deref()))?);
            }
            sql.push(')');
            n += 1;
            if sql.len() >= BATCH_BYTES {
                let mut b = Budget::mb(1);
                scratch.run(&sql, &mut b).await?;
                sql.clone_from(&head);
                n = 0;
            }
        }
        if n > 0 {
            let mut b = Budget::mb(1);
            scratch.run(&sql, &mut b).await?;
        }
        drop(rows);
        budget.left = before;
    }
    metrics::counter!("sprig_router_gather_rows_total").increment(copied);
    // The statement itself, over the copies.
    let columns = scratch.start(&p.sql).await?;
    let Some(columns) = columns else {
        return Ok(Output::Ok(crate::exec::ShardOk::default()));
    };
    if let Some(sink) = sink {
        sink.columns(&columns).await?;
        while let Some(row) = scratch.next_row().await? {
            sink.row(&row).await?;
        }
        return Ok(Output::Streamed {
            warnings: scratch.warnings(),
        });
    }
    let mut rows = Vec::new();
    while let Some(row) = scratch.next_row().await? {
        budget.charge(row_bytes(&row))?;
        rows.push(row);
    }
    Ok(Output::Rows {
        columns,
        rows,
        warnings: scratch.warnings(),
    })
}

/// Every row `reads` return, in their order. Outside a transaction each
/// read runs on a connection of its own, all at once; inside one (or a
/// backend without streams) one after another through `be.run`. The rows
/// are charged to `budget`.
async fn read_all(
    be: &mut dyn ShardBackend,
    reads: &[(&Target, String)],
    budget: &mut Budget,
) -> Result<Vec<Row>> {
    let mut streams = Vec::with_capacity(reads.len());
    if !be.in_transaction() && reads.len() > 1 {
        for (t, _) in reads {
            match be.open_stream(&t.cluster).await? {
                Some(s) => streams.push(s),
                None => break,
            }
        }
    }
    let mut out = Vec::new();
    if streams.len() == reads.len() {
        let mut tasks = tokio::task::JoinSet::new();
        for (i, (mut st, (_, sql))) in streams.into_iter().zip(reads).enumerate() {
            let sql = sql.clone();
            tasks.spawn(async move {
                let mut rows = Vec::new();
                if st.start(&sql).await?.is_some() {
                    while let Some(r) = st.next_row().await? {
                        rows.push(r);
                    }
                }
                Ok::<_, SprigError>((i, rows))
            });
        }
        let mut parts: Vec<(usize, Vec<Row>)> = Vec::with_capacity(reads.len());
        while let Some(j) = tasks.join_next().await {
            let (i, rows) = j.map_err(|e| SprigError::internal(format!("a shard read: {e}")))??;
            for r in &rows {
                budget.charge(row_bytes(r))?;
            }
            parts.push((i, rows));
        }
        parts.sort_by_key(|(i, _)| *i);
        for (_, rows) in parts {
            out.extend(rows);
        }
        return Ok(out);
    }
    for (t, sql) in reads {
        if let ShardOutcome::Rows(r) = be.run(t, sql, budget).await? {
            out.extend(r.rows);
        }
    }
    Ok(out)
}

fn row_bytes(r: &Row) -> u64 {
    r.iter()
        .map(|v| v.as_ref().map_or(1, |b: &Bytes| b.len() as u64 + 1))
        .sum()
}

#[cfg(test)]
mod tests;
