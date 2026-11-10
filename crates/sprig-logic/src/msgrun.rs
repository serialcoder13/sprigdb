//! The runners of triggers on streams and queues (chapter 41, "Triggers on
//! streams and queues"; R9), as a worker's job runs them. Every run itself
//! happens in a gateway ([`crate::messages`]); these loops decide what to
//! run, in what order, and what to do with failures.
//!
//! ```text
//! run_stream: one task per partition, from the group's committed offset:
//!   records below the log start (retention removed them): skip them
//!   a gap (compaction): move the offset to the next record first
//!   per record: run; Ok → next; Moved → read the committed offset again
//!   (another run did it); Failed → retry after backoff(n); failures that
//!   aren't transient count, and at logic.trigger_max_attempts the record
//!   becomes a dead letter and the offset moves past it
//! run_queue: logic.trigger_lanes loops, each asking for the next message;
//!   Empty → wait (100 ms doubling to 1 s); Failed → wait backoff(the
//!   message's failures) before asking again
//! ```
//!
//! Positions live only in `stream_offsets`, so a runner that dies is
//! replaced by one that starts from what committed: nothing runs twice
//! (the compare-and-set) and nothing is skipped.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use sprig_common::{CancellationToken, Result};
use tokio::sync::watch;

use crate::messages::{RecordIn, RunOutcome, RunReply, RunRequest};
use crate::triggers::{RunOpts, backoff};

/// Where runs happen: a gateway (the worker's peer client), or a fake.
#[async_trait]
pub trait Gateway: Send + Sync {
    /// One run.
    ///
    /// # Errors
    /// No gateway could be reached (the runner retries).
    async fn run(&self, req: &RunRequest) -> Result<RunReply>;

    /// A line for the trigger's run log (`logic_log`): its start, the
    /// first failure of a record or message. Best effort.
    async fn note(&self, level: &str, message: &str) {
        let _ = (level, message);
    }
}

/// A stream trigger's view of its stream and its group's offsets.
#[async_trait]
pub trait StreamSide: Send + Sync {
    /// The group's committed offsets, by partition.
    ///
    /// # Errors
    /// The catalog's.
    async fn positions(&self) -> Result<HashMap<u32, u64>>;
    /// Each partition's `(log start, high watermark)`.
    ///
    /// # Errors
    /// The log's.
    async fn bounds(&self) -> Result<HashMap<u32, (u64, u64)>>;
    /// Up to `max` records of partition `p` from offset `from`.
    ///
    /// # Errors
    /// The log's.
    async fn read(&self, p: u32, from: u64, max: u32) -> Result<Vec<RecordIn>>;
    /// Creates partition `p`'s offset at `at` when it has none.
    ///
    /// # Errors
    /// The catalog's.
    async fn init(&self, p: u32, at: u64) -> Result<()>;
    /// Moves partition `p`'s offset from `expect` to `to`; `false` when it
    /// held another offset.
    ///
    /// # Errors
    /// The catalog's.
    async fn advance(&self, p: u32, expect: u64, to: u64) -> Result<bool>;
    /// Records a dead letter for `r` after `attempts` failures.
    ///
    /// # Errors
    /// The catalog's.
    async fn dead_letter(&self, r: &RecordIn, attempts: u32, last: &RunReply) -> Result<()>;
}

/// Counters of one runner.
#[derive(Debug, Default)]
pub struct MsgStats {
    /// Runs that committed.
    pub ran: AtomicU64,
    /// Runs another run had already done.
    pub moved: AtomicU64,
    /// Failed runs.
    pub failed: AtomicU64,
    /// Dead letters.
    pub dead: AtomicU64,
    /// Records skipped because retention removed them first.
    pub skipped: AtomicU64,
}

impl MsgStats {
    /// The counters as JSON (job progress).
    #[must_use]
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "ran": self.ran.load(Ordering::Relaxed),
            "moved": self.moved.load(Ordering::Relaxed),
            "failed": self.failed.load(Ordering::Relaxed),
            "dead": self.dead.load(Ordering::Relaxed),
            "skipped": self.skipped.load(Ordering::Relaxed),
        })
    }
}

/// What every run of one trigger shares: the request's fixed part and the
/// version to run (the job updates it when a new version is deployed).
#[derive(Clone)]
pub struct Template {
    /// The request, with `record` empty.
    pub req: RunRequest,
    /// The active version.
    pub version: Arc<AtomicU64>,
}

impl Template {
    fn request(&self, record: Option<RecordIn>) -> RunRequest {
        let mut r = self.req.clone();
        r.version = self.version.load(Ordering::Relaxed);
        r.record = record;
        r
    }
}

async fn sleep_or_cancel(d: Duration, cancel: &CancellationToken) -> bool {
    tokio::select! {
        () = cancel.cancelled() => false,
        () = tokio::time::sleep(d) => true,
    }
}

/// How one record ended.
enum Done {
    /// Ran (or dead-lettered): the offset is past it.
    Next,
    /// Another run moved the offset: read it again.
    Moved,
    /// Cancelled.
    Stop,
}

/// Runs one record until it commits, another run did it, or it becomes a
/// dead letter.
async fn run_record(
    side: &dyn StreamSide,
    gw: &dyn Gateway,
    t: &Template,
    opts: &RunOpts,
    stats: &MsgStats,
    r: &RecordIn,
    cancel: &CancellationToken,
) -> Result<Done> {
    let mut attempts = 0u32;
    let mut transient = 0u32;
    let seed = format!("{}:{}", r.partition, r.offset);
    loop {
        if cancel.is_cancelled() {
            return Ok(Done::Stop);
        }
        let reply = match gw.run(&t.request(Some(r.clone()))).await {
            Ok(x) => x,
            Err(e) => {
                transient += 1;
                tracing::debug!(error = %e, "trigger run: no gateway answered");
                let opts = opts.current();
                if !sleep_or_cancel(
                    backoff(transient, opts.retry_initial, opts.retry_max, &seed),
                    cancel,
                )
                .await
                {
                    return Ok(Done::Stop);
                }
                continue;
            }
        };
        match reply.outcome {
            RunOutcome::Ok => {
                stats.ran.fetch_add(1, Ordering::Relaxed);
                return Ok(Done::Next);
            }
            RunOutcome::Moved | RunOutcome::Empty => {
                stats.moved.fetch_add(1, Ordering::Relaxed);
                return Ok(Done::Moved);
            }
            RunOutcome::Dead => {
                stats.dead.fetch_add(1, Ordering::Relaxed);
                return Ok(Done::Next);
            }
            RunOutcome::Failed => {
                stats.failed.fetch_add(1, Ordering::Relaxed);
                // `logic.trigger_*` as they resolve now.
                let opts = opts.current();
                let n = if reply.transient {
                    transient += 1;
                    transient
                } else {
                    attempts += 1;
                    if attempts == 1 {
                        gw.note(
                            "warn",
                            &format!(
                                "partition {} offset {}: attempt 1 failed ({} {}); retrying",
                                r.partition, r.offset, reply.code, reply.message
                            ),
                        )
                        .await;
                    }
                    if attempts >= opts.max_attempts.max(1) {
                        side.dead_letter(r, attempts, &reply).await?;
                        stats.dead.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(partition = r.partition, offset = r.offset, attempts, code = reply.code, message = %reply.message, "trigger record dead-lettered");
                        return Ok(
                            if side.advance(r.partition, r.offset, r.offset + 1).await? {
                                Done::Next
                            } else {
                                Done::Moved
                            },
                        );
                    }
                    attempts
                };
                if !sleep_or_cancel(
                    backoff(n, opts.retry_initial, opts.retry_max, &seed),
                    cancel,
                )
                .await
                {
                    return Ok(Done::Stop);
                }
            }
        }
    }
}

/// One partition of a stream trigger (see the module docs).
#[allow(clippy::too_many_arguments)]
async fn partition(
    side: Arc<dyn StreamSide>,
    gw: Arc<dyn Gateway>,
    t: Template,
    opts: RunOpts,
    stats: Arc<MsgStats>,
    p: u32,
    mut marks: watch::Receiver<HashMap<u32, (u64, u64)>>,
    cancel: CancellationToken,
) -> Result<()> {
    let mut pos: Option<u64> = None;
    loop {
        if cancel.is_cancelled() {
            return Ok(());
        }
        let (start, hw) = marks.borrow().get(&p).copied().unwrap_or((0, 0));
        let at = if let Some(o) = pos {
            o
        } else {
            let o = if let Some(o) = side.positions().await?.get(&p) {
                *o
            } else {
                // A partition added after the trigger: from its start.
                side.init(p, start).await?;
                start
            };
            pos = Some(o);
            o
        };
        if at < start {
            // Retention removed records the trigger never ran.
            if side.advance(p, at, start).await? {
                stats.skipped.fetch_add(start - at, Ordering::Relaxed);
                tracing::warn!(
                    partition = p,
                    from = at,
                    to = start,
                    "trigger records expired before they ran"
                );
            }
            pos = None;
            continue;
        }
        if at >= hw {
            tokio::select! {
                () = cancel.cancelled() => return Ok(()),
                r = marks.changed() => if r.is_err() { return Ok(()); },
                () = tokio::time::sleep(Duration::from_millis(500)) => {}
            }
            continue;
        }
        let recs = side.read(p, at, 100).await?;
        if recs.is_empty() {
            if !sleep_or_cancel(Duration::from_millis(100), &cancel).await {
                return Ok(());
            }
            continue;
        }
        let mut cur = at;
        for r in recs {
            if r.offset < cur {
                continue;
            }
            if r.offset > cur {
                // A gap left by compaction: the offset moves to the record.
                if !side.advance(p, cur, r.offset).await? {
                    pos = None;
                    break;
                }
            }
            match run_record(&*side, &*gw, &t, &opts, &stats, &r, &cancel).await? {
                Done::Next => {
                    cur = r.offset + 1;
                    pos = Some(cur);
                }
                Done::Moved => {
                    pos = None;
                    break;
                }
                Done::Stop => return Ok(()),
            }
        }
    }
}

/// A stream trigger until `cancel` (see the module docs). `partitions`
/// is the stream's count; `every` how often the high watermarks are read.
///
/// # Errors
/// A catalog or log error a partition met (the job then restarts).
#[allow(clippy::too_many_arguments)]
pub async fn run_stream(
    side: Arc<dyn StreamSide>,
    gw: Arc<dyn Gateway>,
    t: Template,
    opts: RunOpts,
    stats: Arc<MsgStats>,
    partitions: u32,
    every: Duration,
    cancel: CancellationToken,
) -> Result<()> {
    let (tx, rx) = watch::channel(side.bounds().await?);
    let inner = cancel.child_token();
    let mut tasks = tokio::task::JoinSet::new();
    for p in 0..partitions {
        tasks.spawn(partition(
            Arc::clone(&side),
            Arc::clone(&gw),
            t.clone(),
            opts.clone(),
            Arc::clone(&stats),
            p,
            rx.clone(),
            inner.clone(),
        ));
    }
    let mut tick = tokio::time::interval(every);
    let r = loop {
        tokio::select! {
            () = cancel.cancelled() => break Ok(()),
            done = tasks.join_next() => match done {
                None => break Ok(()),
                Some(Ok(Ok(()))) => {}
                Some(Ok(Err(e))) => break Err(e),
                Some(Err(e)) => break Err(sprig_common::SprigError::internal(format!("a trigger partition task: {e}"))),
            },
            _ = tick.tick() => {
                match side.bounds().await {
                    Ok(b) => {
                        tx.send_if_modified(|cur| {
                            if *cur == b { false } else { *cur = b; true }
                        });
                    }
                    Err(e) => tracing::debug!(error = %e, "trigger: high watermarks unreadable"),
                }
            }
        }
    };
    inner.cancel();
    while tasks.join_next().await.is_some() {}
    r
}

/// A queue trigger until `cancel` (see the module docs).
pub async fn run_queue(
    gw: Arc<dyn Gateway>,
    t: Template,
    opts: RunOpts,
    stats: Arc<MsgStats>,
    cancel: CancellationToken,
) {
    let mut tasks = tokio::task::JoinSet::new();
    for lane in 0..opts.lanes.max(1) {
        let (gw, t, opts, stats, cancel) = (
            Arc::clone(&gw),
            t.clone(),
            opts.clone(),
            Arc::clone(&stats),
            cancel.clone(),
        );
        tasks.spawn(async move {
            let mut idle = 0u32;
            let mut errs = 0u32;
            let seed = format!("lane{lane}");
            while !cancel.is_cancelled() {
                let wait = match gw.run(&t.request(None)).await {
                    Err(e) => {
                        errs += 1;
                        tracing::debug!(error = %e, "queue trigger: no gateway answered");
                        let opts = opts.current();
                        backoff(errs, opts.retry_initial, opts.retry_max, &seed)
                    }
                    Ok(r) => {
                        errs = 0;
                        match r.outcome {
                            RunOutcome::Ok => {
                                idle = 0;
                                stats.ran.fetch_add(1, Ordering::Relaxed);
                                Duration::ZERO
                            }
                            RunOutcome::Moved => {
                                stats.moved.fetch_add(1, Ordering::Relaxed);
                                Duration::ZERO
                            }
                            RunOutcome::Dead => {
                                idle = 0;
                                stats.dead.fetch_add(1, Ordering::Relaxed);
                                tracing::warn!(message = ?r.message_id, attempts = r.attempts + 1, code = r.code, error = %r.message, "queue trigger message dead-lettered");
                                Duration::ZERO
                            }
                            RunOutcome::Empty => {
                                idle = (idle + 1).min(4);
                                Duration::from_millis(100 << (idle - 1)).min(Duration::from_secs(1))
                            }
                            RunOutcome::Failed => {
                                stats.failed.fetch_add(1, Ordering::Relaxed);
                                if !r.transient && r.attempts == 0 {
                                    gw.note(
                                        "warn",
                                        &format!(
                                            "message {}: attempt 1 failed ({} {}); retrying",
                                            r.message_id.as_deref().unwrap_or("?"),
                                            r.code,
                                            r.message
                                        ),
                                    )
                                    .await;
                                }
                                let n = if r.transient { 1 } else { r.attempts + 1 };
                                let opts = opts.current();
                                backoff(
                                    n,
                                    opts.retry_initial,
                                    opts.retry_max,
                                    r.message_id.as_deref().unwrap_or(&seed),
                                )
                            }
                        }
                    }
                };
                if !wait.is_zero() && !sleep_or_cancel(wait, &cancel).await {
                    break;
                }
            }
        });
    }
    while tasks.join_next().await.is_some() {}
}

#[cfg(test)]
mod tests;
