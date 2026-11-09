//! One job's state machine: prepare, watermark copy with catch-up,
//! verification, the cutover and what follows (chapter 10, "Algorithms",
//! "State machines"; build steps 4 to 8).
//!
//! The engine owns the job's stream and its one ordered apply lane. Every
//! target transaction carries the applied source position and the copy
//! cursors, guarded by the job's fencing token, so after any failure the
//! engine (or a successor on another worker) resumes from exactly what the
//! target holds: it reloads the position row and reopens the stream there.
//! Uncommitted work is simply read again, which the idempotent applier
//! tolerates.
//!
//! The invariant (chapter 10, "The watermark copy"): at every applied
//! position `p`, each row present in the target equals the source row at
//! `p`, though rows may be missing until the copy is done.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_binlog::GtidPos;
use sprig_common::ids::{ClusterId, JobId};
use sprig_common::time::Clock;
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::api::{CutoverMode, JobKind, JobState, VerifyMode};
use crate::io::{ChangeStream, CommitOutcome, HandoffPos, JobEnv, Released};
use crate::model::{
    ApplyOp, CopyState, KeyBytes, KeyPart, PosRow, Row, RowChange, SourceTxn, StreamItem,
    TableCopy, TableId, TableMeta, Watermark, WmKind, XaPhase,
};
use crate::record::{Claim, CutoverStep, VerifyCursor, wall_ms};
use crate::settings::MoverSettings;

/// The job's only stream in R0.
const STREAM: u16 = 0;
/// How often operator fields (cancel, manual go) are re-read.
const CANCEL_CHECK: Duration = Duration::from_secs(1);
/// How often progress is written to the job record.
const SAVE_EVERY: Duration = Duration::from_secs(2);
/// How often the applied position is committed when nothing else is.
const POS_COMMIT_EVERY: Duration = Duration::from_secs(1);
/// Items buffered between the stream reader and the apply lane.
const STREAM_BUFFER: usize = 256;
/// Consecutive failures before a job gives up and rolls back.
const MAX_FAILURES: u32 = 200;
/// The fence's own `RENAME`s (and the unfence's) in the source binlog.
const FENCE_MARK: &str = "__sprig_f_";

/// What a pump waits for.
#[derive(Debug, Clone)]
enum Until {
    /// The job's watermark of this kind and sequence is applied.
    Wm(WmKind, u64),
    /// The applied position has reached this one.
    Pos(GtidPos),
    /// This much time has passed.
    For(Duration),
}

/// Changes seen on one table between a low and a high watermark.
#[derive(Debug)]
struct Window {
    table: TableId,
    keys: HashSet<KeyBytes>,
    ddl: bool,
}

/// The stream, read by its own task so a pump can wait with a deadline.
struct Reader {
    rx: mpsc::Receiver<Result<StreamItem>>,
    task: JoinHandle<()>,
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// What one state's step decided.
/// A retaining job with longer than this to wait is given back to the
/// job store rather than kept in a worker slot.
const RETAIN_HAND_BACK_MS: u64 = 120_000;

enum Step {
    /// Go on with the state machine.
    Next,
    /// The job ended in this state.
    End(JobState),
}

/// One job's engine. Build it with the worker's claim and the job's
/// environment, then [`Engine::run`] it.
pub struct Engine {
    claim: Claim,
    env: JobEnv,
    clock: Arc<dyn Clock>,
    settings: MoverSettings,
    job: JobId,
    token: u64,
    tables: Vec<Arc<TableMeta>>,
    reader: Option<Reader>,
    /// Committed on the target.
    applied: GtidPos,
    /// Read from the stream (applied once the pending batch commits).
    read: GtidPos,
    copy: CopyState,
    pending: Vec<ApplyOp>,
    /// Per table: the index in `pending` of its latest op, and that op's
    /// keys.
    pending_keys: std::collections::HashMap<TableId, (usize, HashSet<KeyBytes>)>,
    /// The last batch op touching each (table name, key): tables of one
    /// name in different source schemas share a target table in a merge
    /// (chapter 20), so their ops on one key keep the source's order.
    pending_names: std::collections::HashMap<(String, KeyBytes), usize>,
    pending_rows: usize,
    pending_since: Option<Instant>,
    window: Option<Window>,
    /// The last sequence read per watermark kind.
    /// The last sequence read per watermark kind, with the position just
    /// after it: a watermark counts as applied once that position is.
    seen: [(u64, GtidPos); 3],
    seq: u64,
    ticks: VecDeque<(u64, Instant)>,
    last_tick: Option<Instant>,
    lag: Option<Duration>,
    lag_ok_since: Option<Instant>,
    last_check: Instant,
    last_save: Instant,
    /// The last commit of the applied position on the target.
    last_commit: Instant,
    failures: u32,
    copied_since: (Instant, u64),
    /// Prepared XA groups waiting for their outcome, oldest first, with the
    /// position just before each: while any waits, the persisted position
    /// stays at the oldest one's, so a restart reads the group again.
    xa_open: Vec<(String, GtidPos, Vec<RowChange>)>,
    /// The feed handoff of the cutover attempt in progress (`G`, `H`).
    handoff: Option<HandoffPos>,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("job", &self.job)
            .field("token", &self.token)
            .field("applied", &self.applied.to_string())
            .finish_non_exhaustive()
    }
}

fn kind_idx(k: WmKind) -> usize {
    match k {
        WmKind::Low => 0,
        WmKind::High => 1,
        WmKind::Tick => 2,
    }
}

fn code(e: &SprigError) -> Option<SprigCode> {
    match e.kind {
        ErrorKind::Sprig(c) => Some(c),
        _ => None,
    }
}

fn aborted(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::MoveCutoverAborted, m.into())
}

impl Engine {
    /// An engine for the job `claim` owns.
    #[must_use]
    pub fn new(claim: Claim, env: JobEnv, clock: Arc<dyn Clock>, settings: MoverSettings) -> Self {
        let now = clock.now();
        // Watermark sequences only grow across restarts: start from the
        // wall clock in microseconds.
        let seq = u64::try_from(
            clock
                .wall()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_micros()),
        )
        .unwrap_or(0);
        Self {
            job: claim.rec.job_id,
            token: claim.token(),
            claim,
            env,
            clock,
            settings,
            tables: Vec::new(),
            reader: None,
            applied: GtidPos::new(),
            read: GtidPos::new(),
            copy: CopyState::default(),
            pending: Vec::new(),
            pending_keys: std::collections::HashMap::new(),
            pending_names: std::collections::HashMap::new(),
            pending_rows: 0,
            pending_since: None,
            window: None,
            seen: Default::default(),
            seq,
            ticks: VecDeque::new(),
            last_tick: None,
            lag: None,
            lag_ok_since: None,
            last_check: now,
            last_save: now,
            last_commit: now,
            failures: 0,
            copied_since: (now, 0),
            xa_open: Vec::new(),
            handoff: None,
        }
    }

    /// The job's current record.
    #[must_use]
    pub fn record(&self) -> &crate::record::JobRecord {
        &self.claim.rec
    }

    /// Runs the job until it ends (`Done`, `RolledBack`, or `Paused`).
    ///
    /// # Errors
    /// 39133 when this worker lost the job (another worker carries on);
    /// the store's errors when the job record can't be written.
    pub async fn run(mut self) -> Result<JobState> {
        loop {
            let state = self.claim.rec.state;
            let r = match state {
                JobState::Planned | JobState::Preparing => self.prepare().await,
                JobState::Copying => self.copying().await,
                JobState::CatchingUp => self.catching_up().await,
                JobState::Verifying => self.verifying().await,
                JobState::Ready => self.ready().await,
                JobState::CuttingOver => self.recover_cutover().await,
                JobState::PostCutover => self.post_cutover().await,
                JobState::Retaining => self.retaining().await,
                JobState::Cleaning => self.cleaning().await,
                JobState::RollingBack => self.rolling_back(None).await,
                JobState::Paused | JobState::Streaming | JobState::Done | JobState::RolledBack => {
                    return Ok(state);
                }
            };
            match r {
                Ok(Step::Next) => self.failures = 0,
                Ok(Step::End(s)) => return Ok(s),
                Err(e) => self.on_error(e).await?,
            }
        }
    }

    /// Sorts an error: lease loss stops, divergence restarts the copy,
    /// unsupported changes fail the job, anything else is retried from the
    /// target's position.
    async fn on_error(&mut self, e: SprigError) -> Result<()> {
        let c = code(&e);
        tracing::warn!(job = %self.job.0, state = %self.claim.rec.state, error = %e, "mover step failed");
        metrics::counter!("sprig_mover_errors_total", "code" => c.map_or("backend", SprigCode::name)).increment(1);
        self.drop_stream();
        match c {
            Some(SprigCode::JobLeaseLost) => return Err(e),
            Some(SprigCode::MoveSourceDiverged)
                if !self.claim.rec.state.is_committed()
                    && self.claim.rec.state != JobState::CuttingOver =>
            {
                return self.restart_copy(&e).await;
            }
            Some(
                SprigCode::MoveUnsupported
                | SprigCode::MoveVerifyFailed
                | SprigCode::MoveTargetIncompatible,
            ) if !self.claim.rec.state.is_committed() => {
                let msg = e.to_string();
                self.claim
                    .save(|r| {
                        r.state = JobState::RollingBack;
                        r.error = Some(msg);
                    })
                    .await?;
                return Ok(());
            }
            _ => {}
        }
        self.failures += 1;
        let msg = e.to_string();
        if self.failures >= MAX_FAILURES && !self.claim.rec.state.is_committed() {
            self.claim
                .save(|r| {
                    r.state = JobState::RollingBack;
                    r.error = Some(format!("gave up after {MAX_FAILURES} failures: {msg}"));
                })
                .await?;
            return Ok(());
        }
        if self.claim.rec.error.as_deref() != Some(msg.as_str()) {
            self.claim.save(|r| r.error = Some(msg)).await?;
        }
        let backoff = Duration::from_millis(50 * u64::from(self.failures.min(40)));
        tokio::time::sleep(backoff).await;
        Ok(())
    }

    fn drop_stream(&mut self) {
        self.reader = None;
        self.xa_open.clear();
        self.pending.clear();
        self.pending_keys.clear();
        self.pending_names.clear();
        self.pending_rows = 0;
        self.pending_since = None;
        self.window = None;
        self.ticks.clear();
    }

    // ---- preparing -------------------------------------------------------

    async fn prepare(&mut self) -> Result<Step> {
        if self.claim.rec.state == JobState::Planned {
            self.claim.save(|r| r.state = JobState::Preparing).await?;
        }
        self.check_cancel(true).await?;
        if self.cancelled() {
            return self.rolling_back(None).await;
        }
        // P0 before the table list: a table created in between is in the
        // binlog after P0 and is copied as well, harmlessly.
        let p0 = self.env.source.binlog_pos().await?;
        let tables = self.env.source.tables().await?;
        let objects = self.env.source.objects().await?;
        for t in &tables {
            if t.pk.is_empty() {
                return Err(SprigError::new(
                    SprigCode::MoveUnsupported,
                    format!("table {}.{} has no primary key", t.schema, t.name),
                ));
            }
        }
        let init = PosRow {
            token: self.token,
            gtid_pos: p0,
            copy: CopyState {
                tables: tables
                    .iter()
                    .map(|t| (t.id().0, TableCopy::default()))
                    .collect(),
            },
        };
        // The position row first: it marks the target schemas as this
        // job's, so a retry after a half-done prepare isn't refused.
        let row = self.env.target.reserve(self.job, self.token, &init).await?;
        self.env.target.prepare(&tables, &objects).await?;
        self.applied = row.gtid_pos.clone();
        self.read = row.gtid_pos;
        self.copy = row.copy;
        let n = u32::try_from(tables.len()).unwrap_or(u32::MAX);
        self.tables = tables.into_iter().map(Arc::new).collect();
        tracing::info!(job = %self.job.0, tables = n, from = %self.applied, "prepared");
        self.claim
            .save(|r| {
                r.state = JobState::Copying;
                r.progress.tables = n;
            })
            .await?;
        Ok(Step::Next)
    }

    /// Loads tables and the position row after a restart (any state after
    /// preparing).
    async fn attach(&mut self) -> Result<()> {
        if self.reader.is_some() {
            return Ok(());
        }
        if self.tables.is_empty() {
            self.tables = self
                .env
                .source
                .tables()
                .await?
                .into_iter()
                .map(Arc::new)
                .collect();
        }
        let Some(row) = self.env.target.load_pos(self.job).await? else {
            return Err(SprigError::new(
                SprigCode::MoveSourceDiverged,
                "the target lost the job's position row",
            ));
        };
        let row = self.env.target.claim(self.job, self.token, &row).await?;
        self.applied = row.gtid_pos.clone();
        self.read = row.gtid_pos;
        self.copy = row.copy;
        let tables = Arc::new(self.tables.clone());
        // The stream replays from the persisted position: held XA groups
        // come again.
        self.xa_open.clear();
        let stream = self.env.source.open_stream(&self.applied, tables).await?;
        self.reader = Some(spawn_reader(stream));
        self.last_tick = None;
        Ok(())
    }

    /// The copy can't continue from the target's position: empty the
    /// target and copy again from a new start position.
    async fn restart_copy(&mut self, why: &SprigError) -> Result<()> {
        tracing::warn!(job = %self.job.0, error = %why, "restarting the copy");
        metrics::counter!("sprig_mover_copy_restarts_total").increment(1);
        let msg = why.to_string();
        self.env.target.reset(self.job, true).await?;
        self.tables.clear();
        self.claim
            .save(|r| {
                r.state = JobState::Preparing;
                r.progress.restarts += 1;
                r.verify_progress.clear();
                r.verified = false;
                r.progress.tables_done = 0;
                r.progress.rows_copied = 0;
                r.error = Some(msg);
            })
            .await?;
        Ok(())
    }

    // ---- copying ---------------------------------------------------------

    async fn copying(&mut self) -> Result<Step> {
        self.attach().await?;
        let source = self.claim.rec.spec.sources[0].cluster.clone();
        let target = self.claim.rec.spec.targets[0].cluster.clone();
        let max = self.settings.max_copies_per_cluster;
        let lease = self.claim.lease();
        let mut waited = false;
        loop {
            let store = self.claim.store().clone();
            if store.take_slot(&source, &self.job, max, lease).await? {
                if store.take_slot(&target, &self.job, max, lease).await? {
                    break;
                }
                // Both or neither: a job holding its source's slot while it
                // waits for its target's could wait forever on one moving
                // the other way.
                if source != target {
                    store.free_slots(&source, &self.job, max).await?;
                }
            }
            if !waited {
                waited = true;
                tracing::info!(job = %self.job.0, %source, %target, "waiting for a copy slot");
            }
            self.pump(Until::For(Duration::from_secs(1)), None).await?;
            self.check_cancel(true).await?;
            if self.cancelled() {
                // A cancel while waiting rolls back (it used to return
                // here without changing state, so the job waited again).
                return self.rolling_back(None).await;
            }
        }
        let ids: Vec<Arc<TableMeta>> = self.tables.clone();
        for t in ids {
            loop {
                if self.copy.tables.get(&t.id().0).is_some_and(|c| c.done) {
                    break;
                }
                if !self.tables.iter().any(|x| x.id() == t.id()) {
                    break; // dropped meanwhile
                }
                self.check_cancel(false).await?;
                if self.cancelled() {
                    return self.rolling_back(None).await;
                }
                self.throttle().await?;
                self.copy_chunk(&t).await?;
                self.maybe_save().await?;
            }
        }
        let done =
            u32::try_from(self.copy.tables.values().filter(|c| c.done).count()).unwrap_or(u32::MAX);
        self.claim
            .save(|r| {
                r.state = JobState::CatchingUp;
                r.progress.tables_done = done;
            })
            .await?;
        Ok(Step::Next)
    }

    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    async fn watermark(&mut self, kind: WmKind) -> Result<u64> {
        let seq = self.next_seq();
        self.env
            .source
            .write_watermark(&Watermark {
                job: self.job,
                stream: STREAM,
                kind,
                seq,
            })
            .await?;
        if kind == WmKind::Tick {
            self.ticks.push_back((seq, self.clock.now()));
        }
        Ok(seq)
    }

    /// One `DBLog` chunk (chapter 10, "The watermark copy").
    async fn copy_chunk(&mut self, t: &Arc<TableMeta>) -> Result<()> {
        let id = t.id();
        let after: Option<Row> = self
            .copy
            .tables
            .get(&id.0)
            .and_then(|c| c.cursor.as_ref())
            .map(|k| k.iter().map(KeyPart::to_value).collect());
        let n = self.settings.copy_chunk_rows.max(1);
        let lo = self.watermark(WmKind::Low).await?;
        if !self.pump(Until::Wm(WmKind::Low, lo), None).await? {
            return Ok(()); // cancelled
        }
        let rows = self.env.source.read_chunk(t, after.as_ref(), n).await?;
        self.window = Some(Window {
            table: id.clone(),
            keys: HashSet::new(),
            ddl: false,
        });
        let hi = self.watermark(WmKind::High).await?;
        if !self.pump(Until::Wm(WmKind::High, hi), None).await? {
            self.window = None;
            return Ok(()); // cancelled
        }
        let Some(w) = self.window.take() else {
            return Ok(());
        };
        if w.ddl {
            metrics::counter!("sprig_mover_chunk_retries_total", "reason" => "ddl").increment(1);
            return Ok(()); // redo the chunk against the new definition
        }
        let Some(t) = self.tables.iter().find(|x| x.id() == id).cloned() else {
            return Ok(()); // dropped in the window
        };
        let keep: Vec<Row> = rows
            .iter()
            .filter(|r| !w.keys.contains(&t.key_of(r)))
            .cloned()
            .collect();
        let entry = self.copy.tables.entry(id.0.clone()).or_default();
        if let Some(last) = rows.last() {
            entry.cursor = Some(t.key_values(last).iter().map(KeyPart::from_value).collect());
        }
        entry.done = rows.len() < n;
        let copied = keep.len() as u64;
        let ops = if keep.is_empty() {
            Vec::new()
        } else {
            vec![ApplyOp::Upsert {
                table: t.clone(),
                rows: keep,
            }]
        };
        self.env
            .target
            .apply(
                self.job,
                self.token,
                &ops,
                &self.persist_pos(&self.applied.clone()),
                &self.copy,
            )
            .await?;
        metrics::counter!("sprig_mover_copy_rows_total").increment(copied);
        self.claim.rec.progress.rows_copied += copied;
        self.copied_since.1 += copied;
        if self.copy.tables.get(&id.0).is_some_and(|c| c.done) {
            self.claim.rec.progress.tables_done =
                u32::try_from(self.copy.tables.values().filter(|c| c.done).count())
                    .unwrap_or(u32::MAX);
        }
        Ok(())
    }

    // ---- the stream and the apply lane ----------------------------------

    fn reached(&self, until: &Until, started: Instant) -> bool {
        match until {
            Until::Wm(k, seq) => {
                let (s, p) = &self.seen[kind_idx(*k)];
                *s >= *seq && self.applied.reached(p)
            }
            // A prepared XA transaction postpones the cutover.
            Until::Pos(p) => self.applied.reached(p) && self.xa_open.is_empty(),
            Until::For(d) => self.clock.now().saturating_duration_since(started) >= *d,
        }
    }

    /// Reads and applies the stream until `until` holds (returns `true`)
    /// or `deadline` passes (`false`). Writes lag ticks and flushes batches
    /// as it goes.
    async fn pump(&mut self, until: Until, deadline: Option<Instant>) -> Result<bool> {
        self.attach().await?;
        let started = self.clock.now();
        let mut recoveries = 0u32;
        loop {
            if let Until::Pos(p) = &until
                && self.read.reached(p)
                && !self.applied.reached(p)
                && let Err(e) = self.flush(true).await
            {
                self.recover(e, &mut recoveries).await?;
                continue;
            }
            if self.reached(&until, started) {
                return Ok(true);
            }
            let now = self.clock.now();
            if deadline.is_some_and(|d| now >= d) {
                self.flush(false).await?;
                return Ok(false);
            }
            if self
                .last_tick
                .is_none_or(|t| now.saturating_duration_since(t) >= self.settings.tick_interval)
            {
                self.last_tick = Some(now);
                self.watermark(WmKind::Tick).await?;
            }
            let mut wait = self.settings.tick_interval;
            if let Some(d) = deadline {
                wait = wait.min(d.saturating_duration_since(now));
            }
            if let Until::For(d) = &until {
                wait = wait.min(d.saturating_sub(now.saturating_duration_since(started)));
            }
            if let Some(since) = self.pending_since {
                wait = wait.min(
                    self.settings
                        .apply_batch
                        .saturating_sub(now.saturating_duration_since(since)),
                );
            }
            let item = {
                let Some(r) = self.reader.as_mut() else {
                    return Err(SprigError::new(SprigCode::BinlogDisconnected, "no stream"));
                };
                tokio::time::timeout(wait.max(Duration::from_millis(1)), r.rx.recv()).await
            };
            match item {
                Ok(Some(Ok(StreamItem::Txn(txn)))) => {
                    let r = async {
                        let wm_hit = self.ingest(txn).await?;
                        let full = self.pending_rows >= self.settings.apply_batch_rows;
                        if wm_hit || full {
                            // A watermark is reached only once the target
                            // committed up to it: with nothing else pending
                            // (an idle tenant, or writers paused), commit the
                            // position alone, or the copy waits forever
                            // (found by chapter 24's move tests).
                            self.flush(wm_hit).await?;
                        }
                        Ok(())
                    }
                    .await;
                    if let Err(e) = r {
                        self.recover(e, &mut recoveries).await?;
                        continue;
                    }
                }
                Ok(Some(Ok(StreamItem::Idle))) => {
                    if let Err(e) = self.flush(false).await {
                        self.recover(e, &mut recoveries).await?;
                        continue;
                    }
                }
                Ok(Some(Err(e))) => {
                    self.recover(e, &mut recoveries).await?;
                    continue;
                }
                Ok(None) => {
                    let e = SprigError::new(SprigCode::BinlogDisconnected, "the stream ended");
                    self.recover(e, &mut recoveries).await?;
                    continue;
                }
                Err(_) => {}
            }
            if self.pending_since.is_some_and(|s| {
                self.clock.now().saturating_duration_since(s) >= self.settings.apply_batch
            }) && let Err(e) = self.flush(false).await
            {
                self.recover(e, &mut recoveries).await?;
                continue;
            }
            recoveries = 0;
            self.check_cancel(false).await?;
            if self.stop_for_cancel() {
                // Callers check `cancelled()` and roll back: a watermark
                // that never comes (an idle source before the position-only
                // commit, a stream that keeps failing) can't hold a cancel.
                return Ok(false);
            }
        }
    }

    /// A cancel stops a pump in every state before the cutover; the
    /// cutover's own pumps have deadlines and abort by themselves.
    fn stop_for_cancel(&self) -> bool {
        self.cancelled() && self.claim.rec.state != JobState::CuttingOver
    }

    /// A stream or apply failure inside a pump: reopen from what the
    /// target committed and go on, keeping the open window (its keys only
    /// grow, which is safe). Fatal errors, and too many failures in a row,
    /// return.
    async fn recover(&mut self, e: SprigError, count: &mut u32) -> Result<()> {
        let c = code(&e);
        if matches!(
            c,
            Some(
                SprigCode::JobLeaseLost
                    | SprigCode::MoveSourceDiverged
                    | SprigCode::MoveUnsupported
                    | SprigCode::BinlogCorrupt
            )
        ) || *count >= 10
        {
            return Err(e);
        }
        *count += 1;
        tracing::debug!(job = %self.job.0, error = %e, "stream or apply failed; reopening");
        metrics::counter!("sprig_binlog_reconnects_total").increment(1);
        self.reader = None;
        self.xa_open.clear();
        self.pending.clear();
        self.pending_keys.clear();
        self.pending_names.clear();
        self.pending_rows = 0;
        self.pending_since = None;
        tokio::time::sleep(Duration::from_millis(10 * u64::from(*count))).await;
        self.attach().await
    }

    /// The position to commit with `pos`: the oldest open XA group's
    /// floor while one waits.
    fn persist_pos(&self, pos: &GtidPos) -> GtidPos {
        self.xa_open
            .first()
            .map_or_else(|| pos.clone(), |(_, floor, _)| floor.clone())
    }

    /// Takes one source transaction into the batch. Returns whether it
    /// carried a watermark or DDL (so the batch should commit now).
    async fn ingest(&mut self, txn: SourceTxn) -> Result<bool> {
        let prev = self.read.clone();
        if let Some(g) = txn.gtid {
            self.read.update(g);
        }
        if let Some(ddl) = &txn.ddl {
            if ddl.sql.contains(FENCE_MARK) {
                return Ok(false);
            }
            // Commit what came before, then the DDL with its own position.
            self.flush_at(Some(prev)).await?;
            if let Some(w) = self.window.as_mut() {
                w.ddl = true;
            }
            let pos = self.read.clone();
            self.env
                .target
                .apply_ddl(
                    self.job,
                    self.token,
                    &ddl.db,
                    &ddl.sql,
                    &self.persist_pos(&pos),
                    &self.copy,
                )
                .await?;
            self.applied = pos;
            self.refresh_tables().await?;
            // Row images after the DDL follow the new definitions.
            self.reader = None;
            self.attach().await?;
            return Ok(true);
        }
        let mut changes = txn.changes;
        match txn.xa {
            Some(XaPhase::Prepare(xid)) => {
                self.xa_open.push((xid, prev, std::mem::take(&mut changes)));
            }
            Some(XaPhase::Commit(xid)) => {
                if let Some(i) = self.xa_open.iter().position(|(x, _, _)| *x == xid) {
                    let (_, _, held) = self.xa_open.remove(i);
                    changes.splice(0..0, held);
                }
            }
            Some(XaPhase::Rollback(xid)) => self.xa_open.retain(|(x, _, _)| *x != xid),
            None => {}
        }
        let mut wm = false;
        for ch in changes {
            let t = ch.table.clone();
            let before_key = ch.before.as_ref().map(|r| t.key_of(r));
            let after_key = ch.after.as_ref().map(|r| t.key_of(r));
            if let Some(w) = self.window.as_mut()
                && w.table == t.id()
            {
                w.keys.extend(before_key.clone());
                w.keys.extend(after_key.clone());
            }
            self.claim.rec.progress.rows_applied += 1;
            match (ch.before, ch.after) {
                (before, Some(after)) => {
                    if let (Some(b), Some(bk), Some(ak)) = (&before, &before_key, &after_key)
                        && bk != ak
                    {
                        // A key change: delete the old key, then upsert.
                        let kv = t.key_values(b);
                        self.push(false, &t, bk.clone(), kv);
                    }
                    let k = after_key.unwrap_or_else(|| t.key_of(&after));
                    self.push(true, &t, k, after);
                }
                (Some(before), None) => {
                    let k = before_key.unwrap_or_else(|| t.key_of(&before));
                    let kv = t.key_values(&before);
                    self.push(false, &t, k, kv);
                }
                (None, None) => {}
            }
        }
        for w in &txn.watermarks {
            if w.job == self.job && w.stream == STREAM {
                let i = kind_idx(w.kind);
                if w.seq >= self.seen[i].0 {
                    self.seen[i] = (w.seq, self.read.clone());
                }
                if w.kind != WmKind::Tick {
                    wm = true;
                }
            }
        }
        if self.pending_since.is_none() {
            self.pending_since = Some(self.clock.now());
        }
        Ok(wm)
    }

    /// Adds one row op to the batch: consecutive same-kind rows of a table
    /// share a statement; a repeated key or another kind starts a new one.
    fn push(&mut self, upsert: bool, t: &Arc<TableMeta>, key: KeyBytes, row: Row) {
        // Ops of different tables may run in any order inside the batch's
        // transaction (no constraint spans tables; foreign key checks are
        // off), so each table keeps its own latest op; within a table the
        // order is the source's, and a repeated key starts a new statement.
        // A merge writes same-named tables of several source schemas into
        // one target table: an op on a key never joins an earlier op once a
        // same-named table's later op touched that key (found by the split
        // simulation: a key deleted in one source and re-inserted in the
        // other was lost).
        let id = t.id();
        let named = (t.name.clone(), key.clone());
        let later = self.pending_names.get(&named).copied();
        let reuse = self.pending_keys.get(&id).and_then(|(i, keys)| {
            let same = matches!(
                (self.pending.get(*i), upsert),
                (Some(ApplyOp::Upsert { .. }), true) | (Some(ApplyOp::Delete { .. }), false)
            );
            // Not behind an op of a same-named table on this key.
            let in_order = later.is_none_or(|l| l <= *i);
            (same && in_order && !keys.contains(&key)).then_some(*i)
        });
        let i = if let Some(i) = reuse {
            i
        } else {
            self.pending.push(if upsert {
                ApplyOp::Upsert {
                    table: t.clone(),
                    rows: Vec::new(),
                }
            } else {
                ApplyOp::Delete {
                    table: t.clone(),
                    keys: Vec::new(),
                }
            });
            let i = self.pending.len() - 1;
            self.pending_keys.insert(id.clone(), (i, HashSet::new()));
            i
        };
        if let Some((_, keys)) = self.pending_keys.get_mut(&id) {
            keys.insert(key);
        }
        self.pending_names.insert(named, i);
        match self.pending.get_mut(i) {
            Some(ApplyOp::Upsert { rows, .. }) => rows.push(row),
            Some(ApplyOp::Delete { keys, .. }) => keys.push(row),
            None => {}
        }
        self.pending_rows += 1;
    }

    /// Commits the batch at the read position. With `force`, commits the
    /// position even without row changes.
    async fn flush(&mut self, force: bool) -> Result<()> {
        // With nothing pending the position is still committed now and then
        // (POS_COMMIT_EVERY): otherwise an idle tenant's applied position
        // stays where the job started, every reconnect re-reads all the
        // ticks since, and a lossy stream never catches up (found by the
        // idle-tenant simulation).
        let stale = self.read != self.applied
            && self.clock.now().saturating_duration_since(self.last_commit) >= POS_COMMIT_EVERY;
        if self.pending.is_empty() && !((force || stale) && self.read != self.applied) {
            // Nothing to commit: the batch timer stops too (left running,
            // an idle tenant's pump polled every millisecond).
            self.pending_since = None;
            self.note_ticks();
            return Ok(());
        }
        self.flush_at(None).await
    }

    async fn flush_at(&mut self, pos: Option<GtidPos>) -> Result<()> {
        let pos = pos.unwrap_or_else(|| self.read.clone());
        if self.pending.is_empty() && pos == self.applied {
            self.note_ticks();
            return Ok(());
        }
        let ops = std::mem::take(&mut self.pending);
        let rows = self.pending_rows;
        self.pending_keys.clear();
        self.pending_names.clear();
        self.pending_rows = 0;
        self.pending_since = None;
        let persist = self.persist_pos(&pos);
        self.env
            .target
            .apply(self.job, self.token, &ops, &persist, &self.copy)
            .await?;
        metrics::counter!("sprig_mover_apply_rows_total").increment(rows as u64);
        self.applied = pos;
        self.last_commit = self.clock.now();
        self.note_ticks();
        Ok(())
    }

    /// Ticks read up to now are applied (the batch committed): measure lag.
    fn note_ticks(&mut self) {
        let now = self.clock.now();
        let seen = self.seen[kind_idx(WmKind::Tick)].0;
        while let Some(&(seq, at)) = self.ticks.front() {
            if seq > seen || !self.pending.is_empty() {
                break;
            }
            self.ticks.pop_front();
            self.set_lag(now.saturating_duration_since(at));
        }
        // An unapplied tick older than the lag so far raises it.
        if let Some(&(_, at)) = self.ticks.front() {
            let age = now.saturating_duration_since(at);
            if self.lag.is_none_or(|l| age > l) {
                self.set_lag(age);
            }
        }
    }

    fn set_lag(&mut self, lag: Duration) {
        self.lag = Some(lag);
        let now = self.clock.now();
        if lag <= self.settings.ready_max_lag {
            self.lag_ok_since.get_or_insert(now);
        } else {
            self.lag_ok_since = None;
        }
        self.claim.rec.progress.lag_ms = Some(u64::try_from(lag.as_millis()).unwrap_or(u64::MAX));
        metrics::gauge!("sprig_mover_catchup_lag_seconds").set(lag.as_secs_f64());
    }

    async fn refresh_tables(&mut self) -> Result<()> {
        let fresh = self.env.source.tables().await?;
        let mut out = Vec::with_capacity(fresh.len());
        for t in fresh {
            let id = t.id();
            // A table that appeared after the start is all in the binlog.
            self.copy.tables.entry(id.0.clone()).or_insert(TableCopy {
                cursor: None,
                done: true,
            });
            out.push(Arc::new(t));
        }
        let live: HashSet<String> = out.iter().map(|t| t.id().0).collect();
        self.copy.tables.retain(|k, _| live.contains(k));
        self.tables = out;
        Ok(())
    }

    // ---- throttling ------------------------------------------------------

    /// Waits, catching up, while the source or target is busy, catch-up
    /// lags, or the copy runs above its rate (chapter 10, "Throttling").
    async fn throttle(&mut self) -> Result<()> {
        loop {
            let reason = self.throttle_reason().await?;
            let Some(reason) = reason else { return Ok(()) };
            let t0 = self.clock.now();
            self.pump(Until::For(Duration::from_millis(100)), None)
                .await?;
            let waited = self.clock.now().saturating_duration_since(t0);
            metrics::counter!("sprig_mover_throttled_milliseconds_total", "reason" => reason)
                .increment(u64::try_from(waited.as_millis()).unwrap_or(0));
            if self.cancelled() {
                return Ok(());
            }
        }
    }

    async fn throttle_reason(&mut self) -> Result<Option<&'static str>> {
        let s = &self.settings;
        if self.lag.is_some_and(|l| l > s.throttle_apply_lag) {
            return Ok(Some("apply_lag"));
        }
        // The token bucket: rows copied over the last second against the cap.
        let now = self.clock.now();
        let (since, rows) = self.copied_since;
        let elapsed = now.saturating_duration_since(since);
        if elapsed >= Duration::from_secs(1) {
            self.copied_since = (now, 0);
        } else if rows >= s.copy_max_rows_per_sec {
            return Ok(Some("rate"));
        }
        let src = self.env.source.load().await?;
        if src.threads_running > s.throttle_threads_running {
            return Ok(Some("threads"));
        }
        if src.replica_lag_ms > s.throttle_replica_lag_ms {
            return Ok(Some("replica_lag"));
        }
        let tgt = self.env.target.load().await?;
        if tgt.replica_lag_ms > s.throttle_replica_lag_ms {
            return Ok(Some("replica_lag"));
        }
        if tgt.flow_control_pct > s.throttle_flow_control_pct {
            return Ok(Some("flow_control"));
        }
        Ok(None)
    }

    // ---- catching up -----------------------------------------------------

    fn lag_ready(&self) -> bool {
        self.lag_ok_since.is_some_and(|since| {
            self.clock.now().saturating_duration_since(since) >= self.settings.ready_sustain
        }) && self.lag.is_some_and(|l| l <= self.settings.ready_max_lag)
    }

    async fn catching_up(&mut self) -> Result<Step> {
        self.attach().await?;
        loop {
            self.check_cancel(false).await?;
            if self.cancelled() {
                return self.rolling_back(None).await;
            }
            if self.lag_ready() {
                break;
            }
            self.pump(Until::For(Duration::from_millis(500)), None)
                .await?;
            self.maybe_save().await?;
        }
        let verify = self.claim.rec.spec.verify != VerifyMode::Off && !self.verified();
        self.claim
            .save(|r| {
                r.state = if verify {
                    JobState::Verifying
                } else {
                    JobState::Ready
                };
            })
            .await?;
        Ok(Step::Next)
    }

    fn verified(&self) -> bool {
        self.claim.rec.verified
    }

    // ---- verification ----------------------------------------------------

    async fn verifying(&mut self) -> Result<Step> {
        self.attach().await?;
        let sample = match self.claim.rec.spec.verify {
            crate::api::VerifyMode::Sample { pct } => Some(u64::from(pct.clamp(1, 100))),
            _ => None,
        };
        // Where verification got to survives errors and worker changes:
        // under steady faults it must still finish.
        let tables = self.tables.clone();
        for t in tables {
            let id = t.id().0;
            let mut after: Option<Row> = match self.claim.rec.verify_progress.get(&id) {
                Some(VerifyCursor::Done) => continue,
                Some(VerifyCursor::After(k)) => Some(k.iter().map(KeyPart::to_value).collect()),
                None => None,
            };
            let mut chunk = 0u64;
            loop {
                self.check_cancel(false).await?;
                if self.cancelled() {
                    return self.rolling_back(None).await;
                }
                chunk += 1;
                let check = sample.is_none_or(|pct| (chunk * 37) % 100 < pct);
                let next = self.verify_chunk(&t, after.as_ref(), check).await?;
                if self.cancelled() {
                    return self.rolling_back(None).await;
                }
                let cursor = match &next {
                    Some(last) => {
                        VerifyCursor::After(last.iter().map(KeyPart::from_value).collect())
                    }
                    None => VerifyCursor::Done,
                };
                self.claim.rec.verify_progress.insert(id.clone(), cursor);
                match next {
                    Some(last) => after = Some(last),
                    None => break,
                }
                self.maybe_save().await?;
            }
            self.claim.save(|_| {}).await?;
        }
        self.claim
            .save(|r| {
                r.state = JobState::Ready;
                r.verified = true;
            })
            .await?;
        Ok(Step::Next)
    }

    /// Verifies the chunk after `after`; returns the last key, or `None` at
    /// the end of the table.
    async fn verify_chunk(
        &mut self,
        t: &Arc<TableMeta>,
        after: Option<&Row>,
        check: bool,
    ) -> Result<Option<Row>> {
        let n = self.settings.copy_chunk_rows.max(1);
        let mut repairs = 0u32;
        loop {
            let lo = self.watermark(WmKind::Low).await?;
            if !self.pump(Until::Wm(WmKind::Low, lo), None).await? {
                return Ok(None); // cancelled: the caller rolls back
            }
            let src = self.env.source.digests(t, after, n).await?;
            let Some(last) = src.last().map(|d| d.key_values.clone()) else {
                // The source has nothing after `after`: rows the target has
                // there must not exist.
                return Ok(None);
            };
            if !check {
                return Ok((src.len() >= n).then_some(last));
            }
            self.window = Some(Window {
                table: t.id(),
                keys: HashSet::new(),
                ddl: false,
            });
            let hi = self.watermark(WmKind::High).await?;
            if !self.pump(Until::Wm(WmKind::High, hi), None).await? {
                self.window = None;
                return Ok(None); // cancelled: the caller rolls back
            }
            let Some(w) = self.window.take() else {
                return Ok(Some(last));
            };
            if w.ddl {
                continue;
            }
            let tgt = self.env.target.digests(t, after, &last).await?;
            let src_map: std::collections::HashMap<&KeyBytes, u64> = src
                .iter()
                .filter(|d| !w.keys.contains(&d.key))
                .map(|d| (&d.key, d.hash))
                .collect();
            let tgt_map: std::collections::HashMap<&KeyBytes, u64> = tgt
                .iter()
                .filter(|d| !w.keys.contains(&d.key))
                .map(|d| (&d.key, d.hash))
                .collect();
            if src_map == tgt_map {
                return Ok((src.len() >= n).then_some(last));
            }
            repairs += 1;
            self.claim.rec.progress.verify_mismatches += 1;
            metrics::counter!("sprig_mover_verify_mismatches_total").increment(1);
            tracing::error!(job = %self.job.0, table = %t.id(), "verification mismatch; repairing the range");
            if repairs > self.settings.verify_max_repairs {
                return Err(SprigError::new(
                    SprigCode::MoveVerifyFailed,
                    format!("table {} still differs after {repairs} repairs", t.id()),
                ));
            }
            self.repair_range(t, after, &last).await?;
        }
    }

    /// Makes `(after, last]` on the target equal the source at a high
    /// watermark.
    async fn repair_range(
        &mut self,
        t: &Arc<TableMeta>,
        after: Option<&Row>,
        last: &Row,
    ) -> Result<()> {
        let lo = self.watermark(WmKind::Low).await?;
        if !self.pump(Until::Wm(WmKind::Low, lo), None).await? {
            return Ok(()); // cancelled
        }
        let rows = self.env.source.read_range(t, after, last).await?;
        self.window = Some(Window {
            table: t.id(),
            keys: HashSet::new(),
            ddl: false,
        });
        let hi = self.watermark(WmKind::High).await?;
        if !self.pump(Until::Wm(WmKind::High, hi), None).await? {
            self.window = None;
            return Ok(()); // cancelled
        }
        let Some(w) = self.window.take() else {
            return Ok(());
        };
        if w.ddl {
            return Ok(());
        }
        let src_keys: HashSet<KeyBytes> = rows.iter().map(|r| t.key_of(r)).collect();
        let tgt = self.env.target.digests(t, after, last).await?;
        let gone: Vec<Row> = tgt
            .into_iter()
            .filter(|d| !src_keys.contains(&d.key) && !w.keys.contains(&d.key))
            .map(|d| d.key_values)
            .collect();
        if !gone.is_empty() {
            self.env
                .target
                .delete_keys(self.job, self.token, t, &gone)
                .await?;
        }
        let keep: Vec<Row> = rows
            .into_iter()
            .filter(|r| !w.keys.contains(&t.key_of(r)))
            .collect();
        let ops = vec![ApplyOp::Upsert {
            table: t.clone(),
            rows: keep,
        }];
        self.env
            .target
            .apply(
                self.job,
                self.token,
                &ops,
                &self.persist_pos(&self.applied.clone()),
                &self.copy,
            )
            .await
    }

    // ---- ready and the cutover ------------------------------------------

    async fn ready(&mut self) -> Result<Step> {
        self.attach().await?;
        let copy_source = self.claim.rec.spec.sources[0].cluster.clone();
        let copy_target = self.claim.rec.spec.targets[0].cluster.clone();
        let max = self.settings.max_copies_per_cluster;
        self.claim
            .store()
            .free_slots(&copy_source, &self.job, max)
            .await?;
        self.claim
            .store()
            .free_slots(&copy_target, &self.job, max)
            .await?;
        loop {
            self.check_cancel(true).await?;
            if self.cancelled() {
                return self.rolling_back(None).await;
            }
            let manual_wait =
                self.claim.rec.spec.cutover == CutoverMode::Manual && !self.claim.rec.go;
            let now_ms = wall_ms(self.clock.wall());
            if manual_wait || now_ms < self.claim.rec.not_before_ms || !self.lag_ready() {
                self.pump(Until::For(Duration::from_millis(250)), None)
                    .await?;
                self.maybe_save().await?;
                continue;
            }
            break;
        }
        if self.claim.rec.progress.cutover_attempts >= self.settings.max_cutover_attempts {
            let n = self.claim.rec.progress.cutover_attempts;
            self.claim
                .save(|r| {
                    r.paused_from = Some(JobState::Ready);
                    r.state = JobState::Paused;
                    r.error = Some(format!("{n} cutover attempts aborted; paused"));
                })
                .await?;
            return Ok(Step::End(JobState::Paused));
        }
        self.cutover().await
    }

    /// The cutover (chapter 10, "Cutover"). Every failure before the commit
    /// aborts: the hold is cleared first (so a commit still in flight can't
    /// land afterwards), then the sources are unfenced.
    #[allow(clippy::too_many_lines)] // the chapter's nine steps, in order
    async fn cutover(&mut self) -> Result<Step> {
        let s = self.settings.clone();
        let swap = self.claim.rec.kind == JobKind::CopySwapDdl;
        let to = self.claim.rec.spec.targets[0].cluster.clone();
        self.handoff = None;
        self.claim
            .save(|r| {
                r.state = JobState::CuttingOver;
                r.cutover_step = Some(CutoverStep::Holding);
                r.receipts.clear();
                r.progress.cutover_attempts += 1;
            })
            .await?;
        let t0 = self.clock.now();
        let end = t0 + s.cutover_max_hold;
        let reserve_at = end.checked_sub(s.flip_reserve).unwrap_or(t0);
        let until_ms = wall_ms(self.clock.wall())
            + u64::try_from((s.cutover_max_hold + s.hold_grace).as_millis()).unwrap_or(u64::MAX);
        let triggers = self.env.source.triggers().await?;
        // The whole attempt runs under a hard deadline: whatever step
        // overruns (a stalled fence, a slow commit), the hold ends by
        // `mover.cutover_max_hold_ms`. A step cut short is undone by the
        // abort: the planned receipt unfences a half-done fence, and the
        // release decides an interrupted commit.
        let budget = end.saturating_duration_since(self.clock.now());
        let attempt = async {
            // 1. Hold.
            self.claim.check_lease()?;
            self.env.maps.hold(self.job, self.token, until_ms).await?;
            // 2. Gateways drain open transactions; keep applying meanwhile.
            self.pump(Until::For(s.txn_grace), Some(reserve_at)).await?;
            // 2b. Prepared distributed-transaction branches (chapter 18):
            // finish them now, or a prepared branch's metadata locks stall
            // the rename fence until it times out.
            self.drain_prepared(reserve_at, "before the fence").await?;
            // 3. Fence the source.
            let planned = self.env.source.plan_fence().await?;
            self.claim
                .save(|r| {
                    r.cutover_step = Some(CutoverStep::Fencing);
                    r.receipts = vec![planned];
                })
                .await?;
            let left = reserve_at.saturating_duration_since(self.clock.now());
            if left.is_zero() {
                return Err(aborted("no time left to fence"));
            }
            self.claim.check_lease()?;
            let fenced = self.env.source.fence(left).await?;
            let receipt = fenced.receipt.clone();
            self.claim
                .save(|r| {
                    r.cutover_step = Some(CutoverStep::Fenced);
                    r.receipts = vec![receipt];
                })
                .await?;
            // 3b. Again, now that no session can prepare a new branch: a
            // branch prepared after 2b (one that touched none of the
            // fenced tables, or any branch under a fence that doesn't wait
            // for locks) must not commit after G. When this drain finished
            // something, G moves past its commits.
            let mut g = fenced.pos.clone();
            if self
                .drain_prepared(reserve_at, "after the fence")
                .await?
                .finished_any()
            {
                g = self.env.source.binlog_pos().await?;
            }
            let fenced = crate::io::Fenced {
                receipt: fenced.receipt,
                pos: g,
            };
            // 4-5. Apply up to G.
            if !self
                .pump(Until::Pos(fenced.pos.clone()), Some(reserve_at))
                .await?
            {
                return Err(aborted(format!(
                    "catch-up didn't reach the fence position {} in time (at {})",
                    fenced.pos, self.applied
                )));
            }
            // 6. Counters, triggers; H (copy-and-swap: the swap itself).
            let counters = self.env.source.counters().await?;
            self.env.target.raise_counters(&counters).await?;
            if !swap {
                let h = self.env.target.finalize(&triggers).await?;
                self.handoff = Some(HandoffPos {
                    fence: fenced.pos.clone(),
                    start: h,
                    at_ms: wall_ms(self.clock.wall()),
                });
                // 7. Stale gateways can't read the source either.
                self.env.source.fence_reads(&fenced.receipt).await?;
            }
            // 8. Budget.
            if self.clock.now() > reserve_at {
                return Err(aborted("the cutover ran over its budget"));
            }
            // 9. Commit.
            self.claim
                .save(|r| r.cutover_step = Some(CutoverStep::Committing))
                .await?;
            if swap {
                // The swap is the commit point; the map then raises the
                // epoch so gateways refresh table metadata.
                self.env.target.finalize(&triggers).await?;
                self.env.maps.bump(self.job, self.token).await?;
                return Ok(CommitOutcome::Committed);
            }
            self.claim.check_lease()?;
            let mut o = self.commit_map(&to).await?;
            while o == CommitOutcome::Unknown {
                tokio::time::sleep(Duration::from_millis(100)).await;
                o = self.settle_unknown().await?;
            }
            Ok(o)
        };
        let r: Result<CommitOutcome> = match tokio::time::timeout(budget, attempt).await {
            Ok(r) => r,
            Err(_) => Err(aborted("the cutover ran over mover.cutover_max_hold_ms")),
        };
        let held = self.clock.now().saturating_duration_since(t0);
        let held_ms = u64::try_from(held.as_millis()).unwrap_or(u64::MAX);
        metrics::histogram!("sprig_mover_cutover_hold_seconds").record(held.as_secs_f64());
        match r {
            Ok(CommitOutcome::Committed) => {
                metrics::counter!("sprig_mover_cutovers_total", "outcome" => "committed")
                    .increment(1);
                let epoch = self.env.maps.read().await.ok().map(|m| m.epoch);
                tracing::info!(job = %self.job.0, held_ms, ?epoch, "cutover committed");
                self.claim
                    .save(|r| {
                        r.state = JobState::PostCutover;
                        r.cutover_step = None;
                        r.epoch = epoch;
                        r.progress.holds_ms.push(held_ms);
                        r.error = None;
                    })
                    .await?;
                Ok(Step::Next)
            }
            Ok(_) => {
                self.abort_cutover("the commit was rejected", held_ms, &triggers)
                    .await
            }
            Err(e) if code(&e) == Some(SprigCode::JobLeaseLost) => Err(e),
            Err(e) => self.abort_cutover(&e.to_string(), held_ms, &triggers).await,
        }
    }

    /// Chapter 18's `drain_prepared` on the source, given half the time
    /// left before `reserve_at`: a branch still prepared afterwards aborts
    /// the attempt (it is retried after `mover.cutover_retry_seconds`).
    async fn drain_prepared(&self, reserve_at: Instant, when: &str) -> Result<crate::io::Drained> {
        let left = reserve_at.saturating_duration_since(self.clock.now());
        let d = self.env.source.drain_prepared(left / 2).await?;
        if d.finished_any() {
            metrics::counter!("sprig_mover_xa_drained_total", "outcome" => "commit")
                .increment(d.committed as u64);
            metrics::counter!("sprig_mover_xa_drained_total", "outcome" => "rollback")
                .increment(d.rolled_back as u64);
            tracing::info!(
                job = %self.job.0,
                committed = d.committed,
                rolled_back = d.rolled_back,
                "{when}: finished prepared distributed-transaction branches"
            );
        }
        if d.remaining > 0 {
            return Err(aborted(format!(
                "{} prepared distributed-transaction branches of the tenant are still in doubt {when}",
                d.remaining
            )));
        }
        Ok(d)
    }

    /// The commit point: the flip, with the feed handoff when this attempt
    /// read `H` (chapter 10, "The feed handoff").
    async fn commit_map(&self, to: &ClusterId) -> Result<CommitOutcome> {
        match &self.handoff {
            Some(h) => {
                self.env
                    .maps
                    .commit_handoff(self.job, self.token, to, h)
                    .await
            }
            None => self.env.maps.commit(self.job, self.token, to).await,
        }
    }

    /// After an `Unknown` commit: read the map until the outcome is known.
    async fn settle_unknown(&mut self) -> Result<CommitOutcome> {
        let m = self.env.maps.read().await?;
        if m.held_by.is_none() && m.moved_by.as_ref().is_some_and(|(j, _)| *j == self.job) {
            return Ok(CommitOutcome::Committed);
        }
        if m.held_by == Some((self.job, self.token)) {
            // Still held by us: the commit didn't land; try it again.
            self.claim.check_lease()?;
            let to = self.claim.rec.spec.targets[0].cluster.clone();
            return self.commit_map(&to).await;
        }
        Ok(CommitOutcome::Rejected)
    }

    async fn abort_cutover(
        &mut self,
        why: &str,
        held_ms: u64,
        triggers: &[String],
    ) -> Result<Step> {
        tracing::warn!(job = %self.job.0, why, held_ms, "cutover aborted");
        metrics::counter!("sprig_mover_cutovers_total", "outcome" => "aborted").increment(1);
        self.drop_stream();
        if self.undo_cutover(triggers).await? {
            // The commit landed after all.
            let epoch = self.env.maps.read().await.ok().map(|m| m.epoch);
            self.claim
                .save(|r| {
                    r.state = JobState::PostCutover;
                    r.cutover_step = None;
                    r.epoch = epoch;
                    r.progress.holds_ms.push(held_ms);
                })
                .await?;
            return Ok(Step::Next);
        }
        let attempts = self.claim.rec.progress.cutover_attempts;
        let backoff = self
            .settings
            .cutover_retry
            .saturating_mul(1u32 << attempts.saturating_sub(1).min(6));
        let not_before =
            wall_ms(self.clock.wall()) + u64::try_from(backoff.as_millis()).unwrap_or(0);
        let why = why.to_owned();
        self.claim
            .save(|r| {
                r.state = JobState::Ready;
                r.cutover_step = None;
                r.receipts.clear();
                r.not_before_ms = not_before;
                r.progress.holds_ms.push(held_ms);
                r.error = Some(why);
            })
            .await?;
        Ok(Step::Next)
    }

    /// Clears the hold, then undoes fences and target finalization. Returns
    /// `true` if the map shows the cutover committed after all.
    ///
    /// Only an owner whose lease holds may unfence, and only after its
    /// release proved no newer owner of the job holds the map: a worker that
    /// lost the job must never undo its successor's fence.
    async fn undo_cutover(&mut self, triggers: &[String]) -> Result<bool> {
        self.claim.check_lease()?;
        let m = self.env.maps.read().await?;
        if m.held_by.is_none() && m.moved_by.as_ref().is_some_and(|(j, _)| *j == self.job) {
            let swap = self.claim.rec.kind == JobKind::CopySwapDdl;
            let to = &self.claim.rec.spec.targets[0].cluster;
            if swap || &m.cluster == to {
                return Ok(true);
            }
        }
        if self.claim.rec.kind == JobKind::CopySwapDdl && self.env.target.swapped().await? {
            self.env.maps.bump(self.job, self.token).await?;
            return Ok(true);
        }
        match self.env.maps.release(self.job, self.token).await? {
            Released::Committed => return Ok(true),
            Released::Newer => {
                return Err(SprigError::new(
                    SprigCode::JobLeaseLost,
                    "a newer owner of the job holds the map",
                ));
            }
            Released::Ours | Released::NotHeld => {}
        }
        // Released: a commit can no longer land. Now the sources may
        // accept writes again.
        self.claim.check_lease()?;
        let _ = self.env.target.unfinalize(triggers).await;
        let receipts = self.claim.rec.receipts.clone();
        if receipts.is_empty() && self.claim.rec.cutover_step == Some(CutoverStep::Fencing) {
            self.env.source.unfence_lost().await?;
        }
        for r in &receipts {
            self.env.source.unfence(r).await?;
        }
        Ok(false)
    }

    /// A worker took over a job that was cutting over.
    async fn recover_cutover(&mut self) -> Result<Step> {
        let triggers = self.env.source.triggers().await.unwrap_or_default();
        if self.undo_cutover(&triggers).await? {
            let epoch = self.env.maps.read().await.ok().map(|m| m.epoch);
            self.claim
                .save(|r| {
                    r.state = JobState::PostCutover;
                    r.cutover_step = None;
                    r.epoch = epoch;
                })
                .await?;
            return Ok(Step::Next);
        }
        let attempts = self.claim.rec.progress.cutover_attempts;
        let backoff = self
            .settings
            .cutover_retry
            .saturating_mul(1u32 << attempts.saturating_sub(1).min(6));
        let not_before =
            wall_ms(self.clock.wall()) + u64::try_from(backoff.as_millis()).unwrap_or(0);
        self.claim
            .save(|r| {
                r.state = JobState::Ready;
                r.cutover_step = None;
                r.receipts.clear();
                r.not_before_ms = not_before;
                r.error = Some("cutover interrupted by a worker change; undone".to_owned());
            })
            .await?;
        Ok(Step::Next)
    }

    // ---- after the commit -----------------------------------------------

    async fn post_cutover(&mut self) -> Result<Step> {
        self.drop_stream();
        let retain = self.settings.source_retain;
        let until = wall_ms(self.clock.wall()) + u64::try_from(retain.as_millis()).unwrap_or(0);
        let src = self.claim.rec.spec.sources[0].cluster.clone();
        let tgt = self.claim.rec.spec.targets[0].cluster.clone();
        let max = self.settings.max_copies_per_cluster;
        self.claim.store().free_slots(&src, &self.job, max).await?;
        self.claim.store().free_slots(&tgt, &self.job, max).await?;
        self.claim
            .save(|r| {
                r.state = JobState::Retaining;
                r.retain_until_ms = Some(until);
            })
            .await?;
        Ok(Step::Next)
    }

    async fn retaining(&mut self) -> Result<Step> {
        loop {
            let now = wall_ms(self.clock.wall());
            let until = self.claim.rec.retain_until_ms.unwrap_or(0);
            if now >= until {
                break;
            }
            if until - now > RETAIN_HAND_BACK_MS {
                // Nothing to do for a while (`sharding.retire_hours`, 24 by
                // default): give the job back so it holds no worker slot;
                // workers claim it again once the retention ends.
                self.claim.release().await?;
                return Ok(Step::End(JobState::Retaining));
            }
            tokio::time::sleep(Duration::from_millis((until - now).min(60_000))).await;
            // Keep proving ownership while waiting.
            self.claim.save(|_| {}).await?;
        }
        self.claim.save(|r| r.state = JobState::Cleaning).await?;
        Ok(Step::Next)
    }

    async fn cleaning(&mut self) -> Result<Step> {
        // A move drops the retained source schemas; a copy-and-swap the
        // swapped-out table.
        self.env.source.drop_schemas().await?;
        self.env.target.forget(self.job).await?;
        self.claim.finish(JobState::Done, None).await?;
        tracing::info!(job = %self.job.0, "job done");
        Ok(Step::End(JobState::Done))
    }

    // ---- cancellation and rollback --------------------------------------

    fn cancelled(&self) -> bool {
        self.claim.rec.cancel.is_some() && !self.claim.rec.state.is_committed()
    }

    async fn check_cancel(&mut self, now: bool) -> Result<()> {
        let t = self.clock.now();
        if now || t.saturating_duration_since(self.last_check) >= CANCEL_CHECK {
            self.last_check = t;
            self.claim.refresh().await?;
        }
        Ok(())
    }

    async fn maybe_save(&mut self) -> Result<()> {
        let t = self.clock.now();
        if t.saturating_duration_since(self.last_save) >= SAVE_EVERY {
            self.last_save = t;
            self.claim.save(|_| {}).await?;
        }
        Ok(())
    }

    async fn rolling_back(&mut self, why: Option<String>) -> Result<Step> {
        if self.claim.rec.state != JobState::RollingBack {
            self.claim
                .save(|r| {
                    r.state = JobState::RollingBack;
                    if why.is_some() {
                        r.error.clone_from(&why);
                    }
                })
                .await?;
        }
        self.drop_stream();
        if !self.claim.rec.receipts.is_empty() || self.claim.rec.cutover_step.is_some() {
            let triggers = self.env.source.triggers().await.unwrap_or_default();
            if self.undo_cutover(&triggers).await? {
                // Committed after all: roll forward instead.
                self.claim
                    .save(|r| {
                        r.state = JobState::PostCutover;
                        r.cutover_step = None;
                    })
                    .await?;
                return Ok(Step::Next);
            }
        }
        self.env.target.reset(self.job, false).await?;
        let src = self.claim.rec.spec.sources[0].cluster.clone();
        let tgt = self.claim.rec.spec.targets[0].cluster.clone();
        let max = self.settings.max_copies_per_cluster;
        self.claim.store().free_slots(&src, &self.job, max).await?;
        self.claim.store().free_slots(&tgt, &self.job, max).await?;
        self.claim.finish(JobState::RolledBack, None).await?;
        tracing::info!(job = %self.job.0, "job rolled back");
        Ok(Step::End(JobState::RolledBack))
    }
}

fn spawn_reader(mut stream: Box<dyn ChangeStream>) -> Reader {
    let (tx, rx) = mpsc::channel(STREAM_BUFFER);
    let task = tokio::spawn(async move {
        loop {
            let item = stream.next().await;
            let stop = item.is_err();
            if tx.send(item).await.is_err() || stop {
                return;
            }
        }
    });
    Reader { rx, task }
}
