//! The sequencer (chapter 45, "Sequencing and offsets"; build step 5):
//! one per (stream, log shard), reading the tenant's feed in commit order,
//! giving each record of its partitions the next offset, and appending
//! records and the feed position in one local transaction on the log shard.
//!
//! ```text
//! run(stream s, log shard L):
//!  1. acquire: in one transaction on L, lock L's shard row of
//!     __sprig_sl_ckpt, raise its leader epoch, read the feed position
//!  2. open the tenant's feed after that position (chapter 38)
//!  3. per feed item, in commit order:
//!       event of s's source (an outbox insert, or a change of the watched
//!         object) whose partition is on L → a record of the open transaction
//!       TxnEnd → the transaction's records join the batch; resume := its token
//!       Progress → resume := its token
//!  4. flush when the batch has stream.sequencer_batch_ms of age or 1,000
//!     records, or the position moved and nothing was written for 5 s:
//!       append(epoch, records, resume) — fenced: a different epoch in the
//!       shard row means another sequencer took over; stop at once
//!       publish the high watermarks; delete the appended outbox rows
//! ```
//!
//! **Exactly once.** The records and the position commit together, so a
//! restart resumes just after the last appended batch. A batch replayed
//! anyway (a commit whose answer was lost, then retried) is caught by
//! `uk_source`: its records are skipped and their offsets not reused.
//!
//! **Leases (as built).** The worker's job runtime (chapter 23) gives each
//! (stream, log shard) one sequencer job under its lease. The lease alone
//! can't stop a sequencer that hasn't noticed it lost the job, so the
//! leader epoch in the shard row fences it inside MariaDB: every append
//! locks that row and checks its epoch first, so a stale sequencer's next
//! append fails and it stops.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use sprig_binlog::{GtidPos, Value};
use sprig_common::ids::{ClusterId, DbId, ObjectId};
use sprig_common::{CancellationToken, ErrorKind, Result, SprigCode, SprigError};
use sprig_feed::{ChangeEvent, FeedItem, Op, ResumeToken};
use tokio::time::Instant;

use crate::ddl::days_from_civil;
use crate::envelope::{ChangeCtx, record_of};
use crate::model::{LeaderEpoch, NewRecord, Offset, PartitionNo, Source};
use crate::partition;

/// Where a sequencer's feed starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Start {
    /// After a token (the position of the last appended batch).
    After(ResumeToken),
    /// A cluster at a position: a new stream's start (its tables' creation).
    At {
        /// The cluster.
        cluster: ClusterId,
        /// The position.
        pos: GtidPos,
    },
}

impl Start {
    /// The bytes kept in the shard row's `resume_token`: a feed token, or
    /// `@<cluster>|<gtid position>`.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Start::After(t) => t.encode(),
            Start::At { cluster, pos } => format!("@{cluster}|{pos}").into_bytes(),
        }
    }

    /// From [`encode`](Self::encode)'s bytes.
    ///
    /// # Errors
    /// 39801 for bytes that are neither.
    pub fn decode(b: &[u8]) -> Result<Self> {
        if let Some(rest) = b.strip_prefix(b"@") {
            let text = std::str::from_utf8(rest).map_err(|_| bad_start())?;
            let (c, p) = text.split_once('|').ok_or_else(bad_start)?;
            return Ok(Start::At {
                cluster: ClusterId::new(c).map_err(|_| bad_start())?,
                pos: p.parse()?,
            });
        }
        Ok(Start::After(ResumeToken::decode(b)?))
    }
}

fn bad_start() -> SprigError {
    SprigError::new(
        SprigCode::FeedBadResumeToken,
        "a stream checkpoint holds no feed position",
    )
}

/// What [`LogStore::acquire`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Acquired {
    /// The new leader epoch, now in the shard row.
    pub epoch: LeaderEpoch,
    /// Where the feed resumes.
    pub start: Start,
}

/// One append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch {
    /// Records in feed order (their offsets are given in this order, per
    /// partition).
    pub records: Vec<NewRecord>,
    /// The feed position after them ([`Start::encode`]).
    pub resume: Vec<u8>,
}

/// What an append did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Appended {
    /// Records appended.
    pub records: u64,
    /// Records already in the log (a replay), skipped.
    pub duplicates: u64,
    /// The touched partitions' high watermarks (next offsets) after it.
    pub high_watermarks: BTreeMap<PartitionNo, Offset>,
}

/// An append's outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendOutcome {
    /// Committed.
    Appended(Appended),
    /// Another sequencer raised the epoch: nothing was written.
    Fenced {
        /// The epoch in the shard row.
        now: LeaderEpoch,
    },
}

/// The stream log of one (stream, log shard), with its checkpoints.
#[async_trait::async_trait]
pub trait LogStore: Send + Sync {
    /// Raises the shard row's leader epoch and reads the feed position.
    ///
    /// # Errors
    /// The backend's (retryable when the shard is unreachable).
    async fn acquire(&self) -> Result<Acquired>;

    /// In one transaction: checks the epoch, appends the records not yet in
    /// the log at the next offsets of their partitions, and stores the
    /// position.
    ///
    /// # Errors
    /// The backend's (retryable when the shard is unreachable).
    async fn append(&self, epoch: LeaderEpoch, batch: &Batch) -> Result<AppendOutcome>;

    /// Deletes appended outbox rows (idempotent; a sweep catches misses).
    ///
    /// # Errors
    /// The backend's.
    async fn cleanup(&self, outbox_ids: &[[u8; 16]]) -> Result<()>;
}

/// A feed the sequencer reads (the tenant's [`sprig_feed::TenantTail`]).
#[async_trait::async_trait]
pub trait FeedSource: Send {
    /// The next item.
    ///
    /// # Errors
    /// The feed's.
    async fn next(&mut self) -> Result<FeedItem>;
}

#[async_trait::async_trait]
impl FeedSource for sprig_feed::TenantTail {
    async fn next(&mut self) -> Result<FeedItem> {
        sprig_feed::TenantTail::next(self).await
    }
}

/// Opens feeds.
#[async_trait::async_trait]
pub trait FeedOpener: Send + Sync {
    /// The tenant's feed from `start`.
    ///
    /// # Errors
    /// The feed's.
    async fn open(&self, start: &Start) -> Result<Box<dyn FeedSource>>;
}

/// Where high watermarks go (chapter 45: etcd and waiting fetches).
#[async_trait::async_trait]
pub trait Watermarks: Send + Sync {
    /// The partitions' new high watermarks.
    async fn publish(&self, epoch: LeaderEpoch, marks: &BTreeMap<PartitionNo, Offset>);
}

/// Watermarks that go nowhere.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoWatermarks;

#[async_trait::async_trait]
impl Watermarks for NoWatermarks {
    async fn publish(&self, _epoch: LeaderEpoch, _marks: &BTreeMap<PartitionNo, Offset>) {}
}

/// Which feed events are the stream's records.
#[derive(Debug, Clone)]
pub enum Filter {
    /// Event streams: inserts into the outbox table `table` of database
    /// `db`.
    Outbox {
        /// The database.
        db: DbId,
        /// `__sprig_eo_<id>`.
        table: String,
        /// The stream's partitions.
        partitions: u32,
    },
    /// Change topics: changes of the watched object.
    Changes {
        /// The database.
        db: DbId,
        /// The object (0 if only the name is known).
        object: ObjectId,
        /// Its table name.
        table: String,
        /// Envelope settings.
        cx: ChangeCtx,
    },
}

/// The sequencer's timing.
#[derive(Debug, Clone)]
pub struct SeqOpts {
    /// `stream.sequencer_batch_ms`.
    pub batch_window: Duration,
    /// Records per append at most.
    pub max_batch: usize,
    /// How long a moved position waits before it is written alone.
    pub idle_checkpoint: Duration,
    /// First retry delay of a failed append; doubles up to 2 s.
    pub retry: Duration,
    /// Delete appended outbox rows.
    pub cleanup: bool,
}

impl Default for SeqOpts {
    fn default() -> Self {
        Self {
            batch_window: Duration::from_millis(10),
            max_batch: 1000,
            idle_checkpoint: Duration::from_secs(5),
            retry: Duration::from_millis(100),
            cleanup: true,
        }
    }
}

/// Counters a sequencer keeps (the job reports them as progress).
#[derive(Debug, Default)]
pub struct SeqStats {
    /// Records appended.
    pub appended: AtomicU64,
    /// Duplicates skipped.
    pub duplicates: AtomicU64,
    /// Appends committed.
    pub batches: AtomicU64,
    /// Feed items read.
    pub items: AtomicU64,
}

/// Why [`Sequencer::run`] returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunEnd {
    /// Asked to stop.
    Stopped,
    /// Another sequencer took the log shard.
    Fenced,
}

/// One sequencer (see the module docs).
pub struct Sequencer {
    /// The source filter.
    pub filter: Filter,
    /// Partitions on this log shard (empty: all).
    pub owned: Vec<PartitionNo>,
    /// The log shard.
    pub store: Arc<dyn LogStore>,
    /// The feed.
    pub feed: Arc<dyn FeedOpener>,
    /// High watermarks.
    pub marks: Arc<dyn Watermarks>,
    /// Timing.
    pub opts: SeqOpts,
    /// Counters.
    pub stats: Arc<SeqStats>,
}

impl std::fmt::Debug for Sequencer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sequencer")
            .field("filter", &self.filter)
            .finish_non_exhaustive()
    }
}

fn retryable(e: &SprigError) -> bool {
    e.retryable
        || matches!(
            e.kind,
            ErrorKind::Sprig(
                SprigCode::BackendUnavailable
                    | SprigCode::CatalogUnavailable
                    | SprigCode::TenantNotReady
                    | SprigCode::CatalogConflict
            )
        )
        || matches!(e.kind, ErrorKind::Backend { errno, .. } if matches!(errno, 1205 | 1213 | 1146 | 1049 | 2006 | 2013 | 1927 | 1053))
}

/// Milliseconds since the Unix epoch of a `DATETIME` value (UTC).
#[must_use]
pub fn datetime_ms(v: &Value) -> Option<i64> {
    match v {
        Value::DateTime(d) => {
            let day = days_from_civil(
                i64::from(d.date.year),
                u32::from(d.date.month),
                u32::from(d.date.day),
            );
            Some(
                day * 86_400_000
                    + i64::from(d.hour) * 3_600_000
                    + i64::from(d.minute) * 60_000
                    + i64::from(d.second) * 1000
                    + i64::from(d.micros / 1000),
            )
        }
        Value::Timestamp(t) => Some(i64::from(t.secs) * 1000 + i64::from(t.micros / 1000)),
        _ => None,
    }
}

fn bytes(v: Option<&Value>) -> Option<Vec<u8>> {
    match v? {
        Value::Bytes(b) | Value::String(b) => Some(b.clone()),
        Value::Null => None,
        Value::Int(i) => Some(i.to_string().into_bytes()),
        Value::UInt(u) => Some(u.to_string().into_bytes()),
        other => Some(crate::envelope::json_of(other).to_string().into_bytes()),
    }
}

/// The record of an outbox insert, and its event id.
#[must_use]
pub fn outbox_record(e: &ChangeEvent, partitions: u32) -> Option<(NewRecord, [u8; 16])> {
    if e.op != Op::Insert {
        return None;
    }
    let row = e.after.as_ref()?;
    let id: [u8; 16] = match row.get("event_id")? {
        Value::Bytes(b) | Value::String(b) => b.as_slice().try_into().ok()?,
        _ => return None,
    };
    let explicit = match row.get("partition_no") {
        Some(Value::UInt(p)) => u32::try_from(*p).ok(),
        Some(Value::Int(p)) => u32::try_from(*p).ok(),
        _ => None,
    };
    let key = bytes(row.get("k"));
    let value = bytes(row.get("v"));
    let headers = bytes(row.get("headers")).map(|h| String::from_utf8_lossy(&h).into_owned());
    let ts_ms = row.get("ts").and_then(datetime_ms).unwrap_or(0);
    let partition = partition::of_record(explicit, key.as_deref(), &id, partitions);
    Some((
        NewRecord {
            partition,
            ts_ms,
            key,
            value,
            headers,
            source: Source::Outbox(id),
        },
        id,
    ))
}

impl Sequencer {
    fn owns(&self, p: PartitionNo) -> bool {
        self.owned.is_empty() || self.owned.contains(&p)
    }

    /// The record of feed event `e`, if it is this sequencer's.
    fn record(&self, e: &ChangeEvent) -> Option<(NewRecord, Option<[u8; 16]>)> {
        match &self.filter {
            Filter::Outbox {
                db,
                table,
                partitions,
            } => {
                if e.db != *db || &e.table != table {
                    return None;
                }
                let (r, id) = outbox_record(e, *partitions)?;
                Some((r, Some(id)))
            }
            Filter::Changes {
                db,
                object,
                table,
                cx,
            } => {
                if e.db != *db {
                    return None;
                }
                let same = if object.0 != 0 && e.object != 0 {
                    e.object == object.0
                } else {
                    &e.table == table
                };
                if !same {
                    return None;
                }
                Some((record_of(e, cx)?, None))
            }
        }
    }

    /// Appends `batch`, retrying while the log shard is unreachable.
    async fn append(
        &self,
        epoch: LeaderEpoch,
        batch: &Batch,
        stop: &CancellationToken,
    ) -> Result<Option<AppendOutcome>> {
        let mut delay = self.opts.retry;
        loop {
            match self.store.append(epoch, batch).await {
                Ok(o) => return Ok(Some(o)),
                Err(e) if retryable(&e) => {
                    tracing::warn!(error = %e, ?delay, "stream append failed; retrying");
                    tokio::select! {
                        () = stop.cancelled() => return Ok(None),
                        () = tokio::time::sleep(delay) => {}
                    }
                    delay = (delay * 2).min(Duration::from_secs(2));
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Runs until `stop`, or until fenced.
    ///
    /// # Errors
    /// The feed's errors (39800 when history is lost), and append errors
    /// that aren't retryable.
    #[allow(clippy::too_many_lines)] // the module docs' steps in one loop
    pub async fn run(&self, stop: &CancellationToken) -> Result<RunEnd> {
        let acq = loop {
            match self.store.acquire().await {
                Ok(a) => break a,
                Err(e) if retryable(&e) => {
                    tracing::warn!(error = %e, "stream sequencer can't acquire its log shard yet");
                    tokio::select! {
                        () = stop.cancelled() => return Ok(RunEnd::Stopped),
                        () = tokio::time::sleep(Duration::from_millis(500)) => {}
                    }
                }
                Err(e) => return Err(e),
            }
        };
        let epoch = acq.epoch;
        tracing::info!(epoch = epoch.0, start = ?acq.start, "stream sequencer acquired its log shard");
        let mut feed = self.feed.open(&acq.start).await?;
        let kind = match self.filter {
            Filter::Outbox { .. } => "events",
            Filter::Changes { .. } => "changes",
        };
        let mut txn: Vec<(NewRecord, Option<[u8; 16]>)> = Vec::new();
        let mut pending: Vec<(NewRecord, Option<[u8; 16]>)> = Vec::new();
        let mut first_pending: Option<Instant> = None;
        let mut resume: Option<ResumeToken> = None;
        let mut dirty = false;
        let mut last_write = Instant::now();
        let far = Duration::from_secs(3600);
        loop {
            let now = Instant::now();
            let deadline = if let Some(f) = first_pending {
                f + self.opts.batch_window
            } else if dirty {
                last_write + self.opts.idle_checkpoint
            } else {
                now + far
            };
            let item = tokio::select! {
                biased;
                () = stop.cancelled() => return Ok(RunEnd::Stopped),
                r = feed.next() => Some(r?),
                () = tokio::time::sleep_until(deadline) => None,
            };
            if let Some(item) = item {
                self.stats.items.fetch_add(1, Ordering::Relaxed);
                match item {
                    FeedItem::Event(e) => {
                        if let Some((r, id)) = self.record(&e)
                            && self.owns(r.partition)
                        {
                            txn.push((r, id));
                        }
                    }
                    FeedItem::TxnEnd { token } => {
                        if !txn.is_empty() {
                            pending.append(&mut txn);
                            first_pending.get_or_insert_with(Instant::now);
                        }
                        resume = Some(token);
                        dirty = true;
                    }
                    FeedItem::Progress { token } => {
                        if txn.is_empty() {
                            resume = Some(token);
                            dirty = true;
                        }
                    }
                }
            }
            let now = Instant::now();
            let due = pending.len() >= self.opts.max_batch
                || first_pending.is_some_and(|f| now >= f + self.opts.batch_window)
                || (pending.is_empty() && dirty && now >= last_write + self.opts.idle_checkpoint);
            if !due {
                continue;
            }
            let Some(token) = resume.clone() else {
                continue;
            };
            let ids: Vec<[u8; 16]> = pending.iter().filter_map(|(_, id)| *id).collect();
            let batch = Batch {
                records: pending.iter().map(|(r, _)| r.clone()).collect(),
                resume: Start::After(token).encode(),
            };
            let Some(outcome) = self.append(epoch, &batch, stop).await? else {
                return Ok(RunEnd::Stopped);
            };
            match outcome {
                AppendOutcome::Fenced { now } => {
                    tracing::warn!(
                        epoch = epoch.0,
                        now = now.0,
                        "stream sequencer fenced: another one took the log shard"
                    );
                    return Ok(RunEnd::Fenced);
                }
                AppendOutcome::Appended(a) => {
                    self.stats.appended.fetch_add(a.records, Ordering::Relaxed);
                    self.stats
                        .duplicates
                        .fetch_add(a.duplicates, Ordering::Relaxed);
                    self.stats.batches.fetch_add(1, Ordering::Relaxed);
                    metrics::counter!("sprig_stream_records_appended_total", "kind" => kind)
                        .increment(a.records);
                    #[allow(clippy::cast_precision_loss)]
                    if let Some(oldest) = batch
                        .records
                        .iter()
                        .map(|r| r.ts_ms)
                        .filter(|t| *t > 0)
                        .min()
                    {
                        let wall = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(0));
                        metrics::histogram!("sprig_stream_sequencing_lag_seconds")
                            .record((wall - oldest).max(0) as f64 / 1000.0);
                    }
                    if !a.high_watermarks.is_empty() {
                        self.marks.publish(epoch, &a.high_watermarks).await;
                    }
                    if self.opts.cleanup
                        && !ids.is_empty()
                        && let Err(e) = self.store.cleanup(&ids).await
                    {
                        tracing::debug!(error = %e, "outbox cleanup failed; the sweep will retry");
                    }
                }
            }
            pending.clear();
            first_pending = None;
            dirty = false;
            last_write = Instant::now();
        }
    }
}

#[cfg(test)]
mod tests;
