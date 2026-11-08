//! The replay harness (chapter 13, build step 6; "Replay and acceptance").
//!
//! A validator builds a candidate index on a node nobody else reads from,
//! then replays the tenant's own sampled statements there, each one before
//! (A: `IGNORE INDEX (x)`) and after (B: as written), with
//! `ANALYZE FORMAT=JSON`, and decides from the timings whether the index
//! helps without hurting. This module is the part that doesn't care how the
//! node was prepared:
//!
//! - [`sqltext`]: binding a sample's literals into a fingerprint's text,
//!   the select form of a write, the `IGNORE INDEX` hint;
//! - [`analyze`]: parsing `ANALYZE FORMAT=JSON` (time, rows read per
//!   table, indexes used);
//! - [`verdict`]: noise handling, medians, p95s and the verdict;
//! - [`replay`]: the A/B rounds over one connection, with a guard that can
//!   stop the replay when the node becomes unsafe.
//!
//! **Inputs.** A [`ReplayStatement`] is one decrypted sample, already bound
//! and already in physical names: the worker decrypts the sample
//! (chapter 12, `sprig-workload::samples`), binds it with
//! [`ReplayStatement::bind`], and rewrites logical names to the shard's
//! physical names with chapter 07's rewriter (`sprig-sql::names`) before
//! handing it here. Values stay in memory: nothing here logs or stores a
//! statement's text, only fingerprints, sample ids and timings.
//!
//! **Session.** Replays run on their own connection with
//! `max_statement_time = auto_index.replay_timeout_seconds` and
//! `SET SESSION TRANSACTION READ ONLY`, so every statement is its own
//! read-only transaction (no write can happen, and metadata locks are
//! released after each statement, so replicated DDL never waits long).
//! Each sample's database, `sql_mode`, `time_zone` and character set are
//! set before it runs.
//!
//! **Rounds** *(the chapter's "A B B A order", read as alternating
//! rounds)*: one warm-up run of each side *(choice: both sides, so neither
//! plan starts with a cold buffer pool)*, then `auto_index.replay_rounds`
//! rounds, A then B in even rounds and B then A in odd ones; a statement
//! whose A runs are noisy gets two more rounds.

pub mod analyze;
pub mod sqltext;
pub mod verdict;

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sprig_cluster::ProbeSession;
use sprig_cluster::sql::ProbeError;
use sprig_common::ids::Fingerprint;

pub use analyze::{PlanRun, TableAccess, parse_plan};
pub use sqltext::{
    SampleValue, SelectForm, Unreplayable, bind_literals, select_form, with_ignore_index,
};
pub use verdict::{
    CoveringHint, FingerprintSummary, Outcome, Run, StatementEvidence, StatementRole,
    StatementRuns, Thresholds, Verdict, VerdictKind, decide,
};

/// The session context a sample was captured with (chapter 12).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionContext {
    /// The default database, physical.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    /// `sql_mode`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql_mode: Option<String>,
    /// `time_zone`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_zone: Option<String>,
    /// The connection character set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub charset: Option<String>,
    /// The connection collation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
}

impl SessionContext {
    /// `sql_mode` has `NO_BACKSLASH_ESCAPES`.
    #[must_use]
    pub fn no_backslash_escapes(&self) -> bool {
        self.sql_mode.as_deref().is_some_and(|m| {
            m.split(',')
                .any(|x| x.trim().eq_ignore_ascii_case("NO_BACKSLASH_ESCAPES"))
        })
    }
}

/// One sampled statement, ready to replay.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplayStatement {
    /// Its fingerprint.
    pub fingerprint: Fingerprint,
    /// `workload_samples.sample_id`.
    pub sample_id: u64,
    /// Target or regression check.
    pub role: StatementRole,
    /// The bound statement in physical names. Holds sample values: never
    /// logged.
    pub sql: String,
    /// The sample's session context.
    pub context: SessionContext,
}

impl std::fmt::Debug for ReplayStatement {
    // The text carries sample values: never print it.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayStatement")
            .field("fingerprint", &self.fingerprint)
            .field("sample_id", &self.sample_id)
            .field("role", &self.role)
            .field("sql_len", &self.sql.len())
            .finish_non_exhaustive()
    }
}

impl ReplayStatement {
    /// Binds a sample into a fingerprint's normalized text
    /// ([`bind_literals`]). The result is still in the fingerprint's
    /// logical names; the caller rewrites them.
    ///
    /// # Errors
    /// The values don't fit the text.
    pub fn bind(
        fingerprint: Fingerprint,
        sample_id: u64,
        role: StatementRole,
        normalized_text: &str,
        values: &[SampleValue],
        context: SessionContext,
    ) -> Result<Self, Unreplayable> {
        let sql = bind_literals(normalized_text, values, context.no_backslash_escapes())?;
        Ok(Self {
            fingerprint,
            sample_id,
            role,
            sql,
            context,
        })
    }
}

/// How much to replay and how to judge it (the `auto_index.replay_*`
/// keys, resolved for the table).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ReplaySettings {
    /// `auto_index.replay_samples` (20): samples per fingerprint.
    pub samples_per_fingerprint: usize,
    /// `auto_index.replay_rounds` (3).
    pub rounds: usize,
    /// Rounds added to a noisy statement (2).
    pub extra_rounds: usize,
    /// `auto_index.replay_timeout_seconds` (10).
    pub timeout: Duration,
    /// `auto_index.regression_fingerprints` (20): other fingerprints on the
    /// table, by total time.
    pub regression_fingerprints: usize,
    /// Run each side once before measuring.
    pub warm_up: bool,
    /// The verdict's thresholds.
    pub thresholds: Thresholds,
}

impl Default for ReplaySettings {
    fn default() -> Self {
        Self {
            samples_per_fingerprint: 20,
            rounds: 3,
            extra_rounds: 2,
            timeout: Duration::from_secs(10),
            regression_fingerprints: 20,
            warm_up: true,
            thresholds: Thresholds::default(),
        }
    }
}

/// The index under test and its table, in physical names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayTarget {
    /// The shard schema.
    pub schema: String,
    /// The table.
    pub table: String,
    /// The candidate's name, `sprig_auto_<8 hex>`.
    pub index: String,
    /// The table's primary-key columns, for select forms of writes.
    pub pk: Vec<String>,
}

/// A connection replays run on.
#[async_trait::async_trait]
pub trait ReplayConn: Send {
    /// Runs a statement, dropping any result.
    ///
    /// # Errors
    /// The statement failed or the connection broke.
    async fn exec(&mut self, sql: &str) -> Result<(), ProbeError>;
    /// The first column of the first row.
    ///
    /// # Errors
    /// The statement failed or the connection broke.
    async fn first_value(&mut self, sql: &str) -> Result<Option<String>, ProbeError>;
}

#[async_trait::async_trait]
impl ReplayConn for ProbeSession {
    async fn exec(&mut self, sql: &str) -> Result<(), ProbeError> {
        ProbeSession::exec(self, sql).await
    }
    async fn first_value(&mut self, sql: &str) -> Result<Option<String>, ProbeError> {
        self.scalar(sql).await
    }
}

/// Checked before each statement; an `Err` stops the replay (the node
/// started lagging, left the cluster, was promoted, ...).
#[async_trait::async_trait]
pub trait ReplayGuard: Send {
    /// `Err(reason)` aborts.
    async fn check(&mut self) -> Result<(), String>;
}

/// A guard that never stops a replay.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoGuard;

#[async_trait::async_trait]
impl ReplayGuard for NoGuard {
    async fn check(&mut self) -> Result<(), String> {
        Ok(())
    }
}

/// How a replay ended.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplayResult {
    /// Every statement's runs, in replay order.
    pub statements: Vec<StatementRuns>,
    /// Set when the guard or the connection stopped the replay early.
    pub aborted: Option<String>,
    /// Wall-clock time of the replay, ms.
    pub elapsed_ms: u64,
}

/// Picks what to replay: every target fingerprint and the first
/// `regression_fingerprints` regression fingerprints in the order given
/// (the caller orders them by total time), at most
/// `samples_per_fingerprint` samples each.
#[must_use]
pub fn select_statements(all: &[ReplayStatement], s: &ReplaySettings) -> Vec<ReplayStatement> {
    let mut regression_fps: Vec<Fingerprint> = Vec::new();
    let mut per_fp: std::collections::HashMap<Fingerprint, usize> =
        std::collections::HashMap::new();
    let mut out = Vec::new();
    for st in all {
        if st.role == StatementRole::Regression && !regression_fps.contains(&st.fingerprint) {
            if regression_fps.len() >= s.regression_fingerprints {
                continue;
            }
            regression_fps.push(st.fingerprint);
        }
        let n = per_fp.entry(st.fingerprint).or_insert(0);
        if *n >= s.samples_per_fingerprint {
            continue;
        }
        *n += 1;
        out.push(st.clone());
    }
    out
}

/// MariaDB's "`max_statement_time` exceeded" (1969), and MySQL's 3024.
fn is_timeout(e: &ProbeError) -> bool {
    matches!(e.errno, Some(1969 | 3024))
}

fn safe_setting(v: &str) -> bool {
    !v.is_empty() && !v.contains(['\'', '\\', '\0'])
}

/// Why a sample's session context couldn't be applied.
enum CtxError {
    /// A value that can't be sent safely; the sample is skipped.
    Unsafe(&'static str),
    /// The statement failed.
    Sql(ProbeError),
}

impl From<ProbeError> for CtxError {
    fn from(e: ProbeError) -> Self {
        CtxError::Sql(e)
    }
}

/// Applies the parts of `ctx` that differ from `cur`.
async fn apply_context(
    conn: &mut dyn ReplayConn,
    cur: &mut SessionContext,
    ctx: &SessionContext,
) -> Result<(), CtxError> {
    let bad = CtxError::Unsafe;
    if ctx.database.is_some() && ctx.database != cur.database {
        let db = ctx.database.as_deref().unwrap_or_default();
        conn.exec(&format!("USE {}", sprig_common::names::quote(db)))
            .await?;
        cur.database.clone_from(&ctx.database);
    }
    if let Some(m) = &ctx.sql_mode
        && ctx.sql_mode != cur.sql_mode
    {
        if !m.is_empty() && !safe_setting(m) {
            return Err(bad("sql_mode"));
        }
        conn.exec(&format!("SET SESSION sql_mode = '{m}'")).await?;
        cur.sql_mode.clone_from(&ctx.sql_mode);
    }
    if let Some(z) = &ctx.time_zone
        && ctx.time_zone != cur.time_zone
    {
        if !safe_setting(z) {
            return Err(bad("time_zone"));
        }
        conn.exec(&format!("SET SESSION time_zone = '{z}'")).await?;
        cur.time_zone.clone_from(&ctx.time_zone);
    }
    if let Some(cs) = &ctx.charset
        && (ctx.charset != cur.charset || ctx.collation != cur.collation)
    {
        let ok =
            |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
        if !ok(cs) || ctx.collation.as_deref().is_some_and(|c| !ok(c)) {
            return Err(bad("character set"));
        }
        match &ctx.collation {
            Some(c) => conn.exec(&format!("SET NAMES {cs} COLLATE {c}")).await?,
            None => conn.exec(&format!("SET NAMES {cs}")).await?,
        }
        cur.charset.clone_from(&ctx.charset);
        cur.collation.clone_from(&ctx.collation);
    }
    Ok(())
}

/// One measured run, or why there is none.
enum RunResult {
    Ok(Run),
    /// The statement failed (not a timeout): the sample can't be replayed.
    Failed(String),
    /// The connection broke: the replay must stop.
    Fatal(String),
}

async fn run_once(
    conn: &mut dyn ReplayConn,
    sql: &str,
    index: &str,
    timeout: Duration,
) -> RunResult {
    let started = Instant::now();
    match conn.first_value(sql).await {
        Ok(Some(json)) => {
            let wall = started.elapsed().as_secs_f64() * 1000.0;
            match parse_plan(&json) {
                Ok(p) => RunResult::Ok(Run {
                    ms: p.engine_ms.unwrap_or(wall),
                    timed_out: false,
                    uses_index: p.uses_index(index),
                    rows_read: p.rows_read(),
                    lookups_via: p.lookups_via(index),
                }),
                Err(e) => RunResult::Failed(e.to_string()),
            }
        }
        Ok(None) => RunResult::Failed("ANALYZE returned no plan".into()),
        Err(e) if is_timeout(&e) => RunResult::Ok(Run {
            ms: timeout.as_secs_f64() * 1000.0,
            timed_out: true,
            uses_index: false,
            rows_read: 0.0,
            lookups_via: 0.0,
        }),
        Err(e) if e.is_server_error() => {
            RunResult::Failed(format!("error {}", e.errno.unwrap_or_default()))
        }
        Err(e) => RunResult::Fatal(format!("connection: {e}")),
    }
}

/// Prepares a replay connection: statement time limit and read-only
/// transactions.
///
/// # Errors
/// A statement failed.
pub async fn prepare_session(
    conn: &mut dyn ReplayConn,
    s: &ReplaySettings,
) -> Result<(), ProbeError> {
    conn.exec(&format!(
        "SET SESSION max_statement_time = {:.3}",
        s.timeout.as_secs_f64()
    ))
    .await?;
    conn.exec("SET SESSION TRANSACTION READ ONLY").await?;
    conn.exec("SET SESSION autocommit = 1").await?;
    Ok(())
}

/// Replays `statements` A/B on `conn` (already prepared with
/// [`prepare_session`]), checking `guard` before each statement.
pub async fn replay(
    conn: &mut dyn ReplayConn,
    guard: &mut dyn ReplayGuard,
    target: &ReplayTarget,
    statements: &[ReplayStatement],
    s: &ReplaySettings,
) -> ReplayResult {
    let started = Instant::now();
    let mut cur = SessionContext::default();
    let mut out = Vec::with_capacity(statements.len());
    let mut aborted = None;
    for st in statements {
        if let Err(reason) = guard.check().await {
            aborted = Some(reason);
            break;
        }
        let mut runs = StatementRuns {
            fingerprint: st.fingerprint,
            sample_id: st.sample_id,
            role: st.role,
            outcome: Outcome::Measured,
            a: Vec::new(),
            b: Vec::new(),
            extra_rounds: false,
        };
        let forms = select_form(&st.sql, &target.pk).and_then(|f| {
            let db = st.context.database.as_deref().or(cur.database.as_deref());
            let a = with_ignore_index(&f.sql, &target.schema, &target.table, &target.index, db)?;
            Ok((
                format!("ANALYZE FORMAT=JSON {a}"),
                format!("ANALYZE FORMAT=JSON {}", f.sql),
            ))
        });
        let (a_sql, b_sql) = match forms {
            Ok(x) => x,
            Err(u) => {
                runs.outcome = Outcome::Unreplayable(u.to_string());
                out.push(runs);
                continue;
            }
        };
        match apply_context(conn, &mut cur, &st.context).await {
            Ok(()) => {}
            Err(CtxError::Unsafe(what)) => {
                runs.outcome = Outcome::Failed(format!("context: unsafe {what}"));
                out.push(runs);
                continue;
            }
            Err(CtxError::Sql(e)) if e.is_server_error() => {
                runs.outcome =
                    Outcome::Failed(format!("context: error {}", e.errno.unwrap_or_default()));
                out.push(runs);
                // A failed USE or SET leaves the session state unknown.
                cur = SessionContext::default();
                continue;
            }
            Err(CtxError::Sql(e)) => {
                aborted = Some(format!("connection: {e}"));
                break;
            }
        }
        match measure(conn, &a_sql, &b_sql, target, s, &mut runs).await {
            Ok(()) => {}
            Err(Stop::Statement(why)) => {
                runs.outcome = Outcome::Failed(why);
                runs.a.clear();
                runs.b.clear();
            }
            Err(Stop::Replay(why)) => {
                aborted = Some(why);
                break;
            }
        }
        out.push(runs);
    }
    ReplayResult {
        statements: out,
        aborted,
        elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
    }
}

enum Stop {
    Statement(String),
    Replay(String),
}

async fn side(
    conn: &mut dyn ReplayConn,
    sql: &str,
    target: &ReplayTarget,
    s: &ReplaySettings,
    is_a: bool,
) -> Result<Run, Stop> {
    match run_once(conn, sql, &target.index, s.timeout).await {
        RunResult::Ok(r) => {
            if is_a && r.uses_index {
                // The hint didn't take: the before plan isn't a before plan.
                return Err(Stop::Statement("hint_not_applied".into()));
            }
            Ok(r)
        }
        RunResult::Failed(why) => Err(Stop::Statement(why)),
        RunResult::Fatal(why) => Err(Stop::Replay(why)),
    }
}

async fn measure(
    conn: &mut dyn ReplayConn,
    a_sql: &str,
    b_sql: &str,
    target: &ReplayTarget,
    s: &ReplaySettings,
    runs: &mut StatementRuns,
) -> Result<(), Stop> {
    if s.warm_up {
        side(conn, a_sql, target, s, true).await?;
        side(conn, b_sql, target, s, false).await?;
    }
    let mut round = 0;
    let mut total = s.rounds.max(1);
    while round < total {
        let a_first = round % 2 == 0;
        for a_side in [a_first, !a_first] {
            let r = side(conn, if a_side { a_sql } else { b_sql }, target, s, a_side).await?;
            if a_side {
                runs.a.push(r);
            } else {
                runs.b.push(r);
            }
        }
        round += 1;
        if round == total && !runs.extra_rounds && s.extra_rounds > 0 {
            let a: Vec<f64> = runs.a.iter().map(|r| r.ms).collect();
            if verdict::is_noisy(&a, &s.thresholds) {
                runs.extra_rounds = true;
                total += s.extra_rounds;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
