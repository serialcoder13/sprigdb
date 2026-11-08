//! The seams between the mover's logic and the outside world (chapter 10;
//! chapter 46, "Deterministic simulation").
//!
//! The engine ([`crate::engine`]) talks to MariaDB only through
//! [`SourceIo`], [`ChangeStream`] and [`TargetIo`], and to the shard map
//! only through [`MapIo`]. Production implements them on `mysql_async`,
//! `sprig-binlog`, chapter 09's fences and chapter 15's map
//! ([`crate::mysql`], [`crate::maps`]); the simulation in `sprig-testkit`
//! implements the MariaDB side on a model, with faults at every call, and
//! uses the production [`MapIo`] on its simulated etcd.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sprig_binlog::GtidPos;
use sprig_common::Result;
use sprig_common::ids::{ClusterId, JobId};

use crate::model::{ApplyOp, CopyState, PosRow, Row, RowDigest, StreamItem, TableMeta, Watermark};

/// A stream of the source's committed transactions, filtered to the job.
#[async_trait::async_trait]
pub trait ChangeStream: Send {
    /// The next transaction, or [`StreamItem::Idle`] after a quiet
    /// heartbeat period.
    ///
    /// # Errors
    /// 39113 when the stream broke (reconnect from the target's position);
    /// 39134 when the source no longer has the position; 39112 on a corrupt
    /// event; 39137 on statement-format DML in a moved schema.
    async fn next(&mut self) -> Result<StreamItem>;
}

/// What a source fence returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fenced {
    /// The fence's receipt, opaque to the engine (chapter 09's
    /// `FenceReceipt` in production), kept in the job record so any
    /// worker can unfence.
    pub receipt: serde_json::Value,
    /// `G`: the source position no write to the shard follows.
    pub pos: GtidPos,
}

/// Load the throttle looks at (chapter 10, "Throttling").
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Load {
    /// The writer's `Threads_running`.
    pub threads_running: u64,
    /// The largest replica lag, in milliseconds.
    pub replica_lag_ms: u64,
    /// Galera flow control paused, in percent.
    pub flow_control_pct: u64,
}

/// An `AUTO_INCREMENT` counter to carry over at cutover.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counter {
    /// The schema.
    pub schema: String,
    /// The table.
    pub table: String,
    /// The next value.
    pub next: u64,
}

/// The source side of one job's stream: the writer and the node the stream
/// and chunks read from.
#[async_trait::async_trait]
pub trait SourceIo: Send + Sync {
    /// The job's schemas (names resolved: an empty spec means every schema
    /// of the tenant).
    async fn schemas(&self) -> Result<Vec<String>>;
    /// The job's tables with their definitions.
    async fn tables(&self) -> Result<Vec<TableMeta>>;
    /// Statements that recreate the schemas' other objects on a target
    /// (views and routines; triggers come at cutover). In order.
    async fn objects(&self) -> Result<Vec<String>>;
    /// The source triggers' `CREATE TRIGGER` statements, for cutover.
    async fn triggers(&self) -> Result<Vec<String>>;
    /// `@@gtid_binlog_pos` on the stream node (on Galera, only the
    /// cluster's domain).
    async fn binlog_pos(&self) -> Result<GtidPos>;
    /// A stream from just after `from`.
    async fn open_stream(
        &self,
        from: &GtidPos,
        tables: Arc<Vec<Arc<TableMeta>>>,
    ) -> Result<Box<dyn ChangeStream>>;
    /// Upserts a watermark row on the writer.
    async fn write_watermark(&self, wm: &Watermark) -> Result<()>;
    /// Up to `limit` rows after `after` (key values) in key order, from the
    /// stream node, autocommit.
    async fn read_chunk(
        &self,
        t: &TableMeta,
        after: Option<&Row>,
        limit: usize,
    ) -> Result<Vec<Row>>;
    /// Per-row digests of up to `limit` rows after `after`, in key order.
    async fn digests(
        &self,
        t: &TableMeta,
        after: Option<&Row>,
        limit: usize,
    ) -> Result<Vec<RowDigest>>;
    /// Rows with keys in `(after, last]`.
    async fn read_range(&self, t: &TableMeta, after: Option<&Row>, last: &Row) -> Result<Vec<Row>>;
    /// The receipt a fence would return, recorded before fencing so any
    /// worker can undo a fence whose worker crashed before recording the
    /// real receipt (undoing a planned receipt skips what isn't fenced).
    async fn plan_fence(&self) -> Result<serde_json::Value>;
    /// Fences the schemas against writes (chapter 09), by `deadline` from
    /// now.
    async fn fence(&self, deadline: Duration) -> Result<Fenced>;
    /// Extends a writes fence to reads (cutover step 7). A no-op for a
    /// rename fence, which already blocks both.
    async fn fence_reads(&self, receipt: &serde_json::Value) -> Result<()>;
    /// Undoes a fence from its receipt.
    async fn unfence(&self, receipt: &serde_json::Value) -> Result<()>;
    /// Undoes a fence whose receipt was lost (a worker crashed between
    /// fencing and recording it): renames fenced tables back.
    async fn unfence_lost(&self) -> Result<()>;
    /// `AUTO_INCREMENT` counters of the job's tables.
    async fn counters(&self) -> Result<Vec<Counter>>;
    /// Current load.
    async fn load(&self) -> Result<Load>;
    /// Drops the job's schemas (a retained source after the retention).
    async fn drop_schemas(&self) -> Result<()>;
}

/// The target side of one job: its writer.
#[async_trait::async_trait]
pub trait TargetIo: Send + Sync {
    /// Creates the internal tables, the schemas, the tables (and runs the
    /// object statements). Idempotent.
    async fn prepare(&self, tables: &[TableMeta], objects: &[String]) -> Result<()>;
    /// Before a job's first prepare: refuses a target that already holds
    /// the job's schemas without the job's position row (they belong to
    /// someone else), then claims as [`Self::claim`] does. The default only
    /// claims.
    async fn reserve(&self, job: JobId, token: u64, init: &PosRow) -> Result<PosRow> {
        self.claim(job, token, init).await
    }
    /// The job's position row, if any.
    async fn load_pos(&self, job: JobId) -> Result<Option<PosRow>>;
    /// Takes ownership with `token`: inserts the position row with `init`
    /// if it is missing, otherwise raises its token (never lowers it).
    /// Returns the row as stored.
    ///
    /// # Errors
    /// 39133 if the row's token is already higher.
    async fn claim(&self, job: JobId, token: u64, init: &PosRow) -> Result<PosRow>;
    /// Applies `ops` and moves the position to `pos` and `copy` in one
    /// transaction, guarded by `token`.
    ///
    /// # Errors
    /// 39133 if the token no longer matches (rolled back); the backend's
    /// errors (the transaction rolled back; its outcome is known).
    async fn apply(
        &self,
        job: JobId,
        token: u64,
        ops: &[ApplyOp],
        pos: &GtidPos,
        copy: &CopyState,
    ) -> Result<()>;
    /// Runs a DDL statement (in `db`), then moves the position in its own
    /// transaction. Re-running a statement already applied is harmless.
    async fn apply_ddl(
        &self,
        job: JobId,
        token: u64,
        db: &str,
        sql: &str,
        pos: &GtidPos,
        copy: &CopyState,
    ) -> Result<()>;
    /// Per-row digests of the rows with keys in `(after, last]`.
    async fn digests(
        &self,
        t: &TableMeta,
        after: Option<&Row>,
        last: &Row,
    ) -> Result<Vec<RowDigest>>;
    /// Deletes the rows with these keys.
    async fn delete_keys(&self, job: JobId, token: u64, t: &TableMeta, keys: &[Row]) -> Result<()>;
    /// Raises `AUTO_INCREMENT` counters to at least the given values.
    async fn raise_counters(&self, counters: &[Counter]) -> Result<()>;
    /// Creates triggers and enables events at cutover; returns `H`, the
    /// target writer's `@@gtid_binlog_pos`.
    async fn finalize(&self, triggers: &[String]) -> Result<GtidPos>;
    /// A copy-and-swap target: whether the swap (its commit point) has
    /// happened. Other targets: `false`.
    async fn swapped(&self) -> Result<bool> {
        Ok(false)
    }
    /// Drops what [`Self::finalize`] created (an aborted cutover).
    async fn unfinalize(&self, triggers: &[String]) -> Result<()>;
    /// Drops the job's target schemas and position rows (rollback), or
    /// only empties the tables when `keep_schemas` (a restarted copy).
    async fn reset(&self, job: JobId, keep_schemas: bool) -> Result<()>;
    /// Removes the job's position and XA rows (after the commit).
    async fn forget(&self, job: JobId) -> Result<()>;
    /// Current load.
    async fn load(&self) -> Result<Load>;
}

/// The outcome of a cutover's commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitOutcome {
    /// The map now points at the targets with the new epoch.
    Committed,
    /// The map didn't change.
    Rejected,
    /// Not known (a timeout): sources stay fenced until it is.
    Unknown,
}

/// What a map says about a job's tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapState {
    /// The cluster it routes to.
    pub cluster: ClusterId,
    /// The epoch.
    pub epoch: u64,
    /// The job and owner token holding it, if held.
    pub held_by: Option<(JobId, u64)>,
    /// The job (and its owner's token) that last moved it.
    pub moved_by: Option<(JobId, u64)>,
}

/// What [`MapIo::release`] found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Released {
    /// The job's hold (by this owner or an older one) is cleared.
    Ours,
    /// The map wasn't held by the job.
    NotHeld,
    /// A newer owner of the job holds it: this worker lost the job.
    Newer,
    /// The job's cutover committed.
    Committed,
}

/// The shard map side of a cutover: the hold, the single commit point
/// (chapter 10's `CutoverCommit`), and the release. Holds name the job and
/// its owner's fencing token, so a worker that lost the job can't clear or
/// commit its successor's hold.
#[async_trait::async_trait]
pub trait MapIo: Send + Sync {
    /// Holds the tenant's map for `job` (owner `token`) until `until_ms`
    /// (wall-clock ms). Returns the epoch `E` held.
    async fn hold(&self, job: JobId, token: u64, until_ms: u64) -> Result<u64>;
    /// Clears the job's hold if it was made by `token` or an older owner.
    async fn release(&self, job: JobId, token: u64) -> Result<Released>;
    /// Points the map at `to` with epoch `E + 1`, if `job`/`token` holds it.
    async fn commit(&self, job: JobId, token: u64, to: &ClusterId) -> Result<CommitOutcome>;
    /// Raises the epoch without moving (copy-and-swap: gateways refresh
    /// table metadata), if `job`/`token` holds the map.
    async fn bump(&self, job: JobId, token: u64) -> Result<CommitOutcome>;
    /// The map now.
    async fn read(&self) -> Result<MapState>;
}

/// Everything one job's engine needs.
#[derive(Clone)]
pub struct JobEnv {
    /// The source.
    pub source: Arc<dyn SourceIo>,
    /// The target.
    pub target: Arc<dyn TargetIo>,
    /// The shard map.
    pub maps: Arc<dyn MapIo>,
}

impl std::fmt::Debug for JobEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobEnv").finish_non_exhaustive()
    }
}
