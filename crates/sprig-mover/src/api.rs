//! The mover's public API (chapter 10, "Interfaces"): what callers such as
//! the balancer (chapter 11), `sprigctl shard move` and copy-and-swap
//! submit, observe and cancel.
//!
//! ```no_run
//! # async fn demo(mover: &dyn sprig_mover::Mover) -> sprig_common::Result<()> {
//! use sprig_common::ids::{ClusterId, TenantId};
//! use sprig_mover::{JobSpec, JobState};
//!
//! let spec = JobSpec::move_tenant(
//!     TenantId(42),
//!     ClusterId::new("dc-a-c1").unwrap(),
//!     ClusterId::new("dc-a-c2").unwrap(),
//! )
//! .with_decision(9133);
//! let id = mover.submit(spec).await?;          // 39131 if a job holds the tenant
//! let st = mover.status(&id).await?;           // state, progress, last error
//! if st.state == JobState::Copying { /* ... */ }
//! mover.cancel(&id, "operator asked").await?;  // 39132 once the cutover committed
//! # Ok(()) }
//! ```

use serde::{Deserialize, Serialize};
use sprig_common::Result;
use sprig_common::ids::{ClusterId, JobId, TenantId};

/// What a job does (chapter 10, "Job kinds"). R0 runs `MoveShard` and
/// `CopySwapDdl`; the others are accepted only by later releases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    /// One shard (R0: one tenant's placement unit) to another cluster.
    MoveShard,
    /// One range into two (R1).
    SplitShard,
    /// Two adjacent ranges into one (R1).
    MergeShards,
    /// A table group to a new key (R3).
    Rekey,
    /// All of a tenant's schemas to another data center (R2).
    TenantMigration,
    /// A table copied to a shadow with new DDL and swapped in by rename.
    CopySwapDdl,
    /// A reference schema kept streaming to another cluster (R1).
    ReferenceCopy,
}

impl JobKind {
    /// The snake-case name used in records and metrics.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            JobKind::MoveShard => "move_shard",
            JobKind::SplitShard => "split_shard",
            JobKind::MergeShards => "merge_shards",
            JobKind::Rekey => "rekey",
            JobKind::TenantMigration => "tenant_migration",
            JobKind::CopySwapDdl => "copy_swap_ddl",
            JobKind::ReferenceCopy => "reference_copy",
        }
    }
}

/// Where a job reads from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSpec {
    /// The source cluster.
    pub cluster: ClusterId,
    /// The shard id; 0 for an R0 tenant move (the catalog's group 0 map).
    #[serde(default)]
    pub shard: u32,
    /// Physical schemas; empty means every schema of the tenant on the
    /// cluster (catalog, shard and reference schemas), resolved while
    /// preparing.
    #[serde(default)]
    pub schemas: Vec<String>,
    /// Tables in every schema; `None` for all (table-scope copy-and-swap
    /// names one).
    #[serde(default)]
    pub tables: Option<Vec<String>>,
    /// The source shard's hash range (hash-range jobs: split, merge and
    /// shard moves, chapter 20).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<HashRange>,
}

/// A hash range `[lo, hi)`; `hi = None` means 2^64.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HashRange {
    /// First hash.
    pub lo: u64,
    /// One past the last; `None` is 2^64.
    pub hi: Option<u64>,
}

/// Where a job writes to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetSpec {
    /// The target cluster.
    pub cluster: ClusterId,
    /// The new shard id; 0 for an R0 tenant move, which keeps the schema
    /// names (the target is another cluster, so they can't collide).
    #[serde(default)]
    pub shard: u32,
    /// Physical schemas, in the order of the source's; empty means the
    /// source's names.
    #[serde(default)]
    pub schemas: Vec<String>,
    /// The hash range the target owns (R1 splits); `None` for all.
    #[serde(default)]
    pub range: Option<HashRange>,
    /// The DDL a copy-and-swap applies to the shadow.
    #[serde(default)]
    pub ddl: Option<DdlTransform>,
}

/// A copy-and-swap's change (chapter 10, "Job kinds", `copy_swap_ddl`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DdlTransform {
    /// The table's schema.
    pub schema: String,
    /// The table.
    pub table: String,
    /// What follows `ALTER TABLE <shadow>`: `ADD COLUMN c INT, ADD KEY (c)`.
    pub alter: String,
}

/// How rows are routed to targets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Routing {
    /// Every row to the one target.
    Whole,
    /// By `_sprig_rk` into the targets' ranges (R1).
    HashRange {
        /// The table group.
        group: u32,
    },
    /// By a new key's hash (R3, chapter 16's online re-key): every row of
    /// the group's members is hashed again under `key` from its own
    /// `columns`, and goes to the target whose range holds the new hash.
    Rekey {
        /// The table group (it keeps its id; its shards are replaced).
        group: u32,
        /// The new key's `KeySpec`, as `table_groups.key_spec` stores it.
        key: serde_json::Value,
        /// Per member table name, the columns that supply the new key,
        /// in key order.
        columns: std::collections::BTreeMap<String, Vec<String>>,
    },
}

/// When the job cuts over.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum CutoverMode {
    /// As soon as the job is ready.
    Auto,
    /// Wait in `ready` until [`Mover::cut_over`] (operator-driven moves).
    Manual,
    /// Together with the parent job's other children (R1).
    Group {
        /// The parent job.
        parent: JobId,
    },
    /// Never; keep streaming (reference copies, R1).
    Continuous,
}

/// How much the job verifies before it is ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum VerifyMode {
    /// Every row, chunk by chunk.
    Full,
    /// A share of the chunks.
    Sample {
        /// Percent of chunks, 1 to 100.
        pct: u8,
    },
    /// No verification.
    Off,
}

/// A job to run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobSpec {
    /// What it does.
    pub kind: JobKind,
    /// The tenant whose data moves.
    pub tenant: TenantId,
    /// Sources (R0: one).
    pub sources: Vec<SourceSpec>,
    /// Targets (R0: one).
    pub targets: Vec<TargetSpec>,
    /// Row routing.
    pub routing: Routing,
    /// When to cut over.
    pub cutover: CutoverMode,
    /// Verification.
    pub verify: VerifyMode,
    /// Chapter 11's decision row, when the balancer planned the job.
    #[serde(default)]
    pub decision_id: Option<u64>,
    /// Who asked (operator, `balancer`), for the decision log.
    #[serde(default)]
    pub requested_by: Option<String>,
    /// The cutover's hold budget for this job, overriding
    /// `mover.cutover_max_hold_ms`: a move between DCs passes the
    /// tenant's `migration.write_pause_target_ms` (or `--max-pause-ms`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_hold_ms: Option<u64>,
    /// How long a committed job keeps its sources fenced before dropping
    /// them, in ms; `None` is `mover.source_retain_hours`. Split and merge
    /// jobs pass `sharding.retire_hours` (chapter 20).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retain_ms: Option<u64>,
}

impl JobSpec {
    /// Moves a tenant's whole R0 placement unit (its catalog and every
    /// database's shard schema, which R0 keeps on one cluster) from `from`
    /// to `to`, cutting over automatically, with full verification.
    #[must_use]
    pub fn move_tenant(tenant: TenantId, from: ClusterId, to: ClusterId) -> Self {
        Self {
            kind: JobKind::MoveShard,
            tenant,
            sources: vec![SourceSpec {
                cluster: from,
                shard: 0,
                schemas: Vec::new(),
                tables: None,
                range: None,
            }],
            targets: vec![TargetSpec {
                cluster: to,
                shard: 0,
                schemas: Vec::new(),
                range: None,
                ddl: None,
            }],
            routing: Routing::Whole,
            cutover: CutoverMode::Auto,
            verify: VerifyMode::Full,
            decision_id: None,
            requested_by: None,
            max_hold_ms: None,
            retain_ms: None,
        }
    }

    /// A table-scope copy-and-swap on `cluster`: `schema.table` is copied
    /// to a shadow altered by `alter`, then swapped in by rename.
    #[must_use]
    pub fn copy_swap(
        tenant: TenantId,
        cluster: ClusterId,
        schema: &str,
        table: &str,
        alter: &str,
    ) -> Self {
        Self {
            kind: JobKind::CopySwapDdl,
            tenant,
            sources: vec![SourceSpec {
                cluster: cluster.clone(),
                shard: 0,
                schemas: vec![schema.to_owned()],
                tables: Some(vec![table.to_owned()]),
                range: None,
            }],
            targets: vec![TargetSpec {
                cluster,
                shard: 0,
                schemas: vec![schema.to_owned()],
                range: None,
                ddl: Some(DdlTransform {
                    schema: schema.to_owned(),
                    table: table.to_owned(),
                    alter: alter.to_owned(),
                }),
            }],
            routing: Routing::Whole,
            cutover: CutoverMode::Auto,
            verify: VerifyMode::Full,
            decision_id: None,
            requested_by: None,
            max_hold_ms: None,
            retain_ms: None,
        }
    }

    /// The same spec carrying chapter 11's decision id.
    #[must_use]
    pub fn with_decision(mut self, decision_id: u64) -> Self {
        self.decision_id = Some(decision_id);
        self
    }

    /// The same spec with another cutover mode.
    #[must_use]
    pub fn with_cutover(mut self, mode: CutoverMode) -> Self {
        self.cutover = mode;
        self
    }

    /// The same spec with another verification mode.
    #[must_use]
    pub fn with_verify(mut self, mode: VerifyMode) -> Self {
        self.verify = mode;
        self
    }

    /// The same spec with its own hold budget (see [`JobSpec::max_hold_ms`]).
    #[must_use]
    pub fn with_max_hold_ms(mut self, ms: u64) -> Self {
        self.max_hold_ms = Some(ms);
        self
    }

    /// The first source cluster.
    #[must_use]
    pub fn source_cluster(&self) -> Option<&ClusterId> {
        self.sources.first().map(|s| &s.cluster)
    }

    /// The first target cluster.
    #[must_use]
    pub fn target_cluster(&self) -> Option<&ClusterId> {
        self.targets.first().map(|t| &t.cluster)
    }

    /// A hash-range job of table group `group` (chapter 20): `sources` are
    /// shards on one cluster, each `(shard, its schemas, its range)`;
    /// `targets` are new shards `(shard, range)` on `to`. The kind follows
    /// from the counts: one source to several targets is a split, several
    /// sources to one target a merge, one to one a shard move.
    #[must_use]
    pub fn reshard(
        tenant: TenantId,
        group: u32,
        from: &ClusterId,
        sources: Vec<(u32, Vec<String>, HashRange)>,
        to: &ClusterId,
        targets: Vec<(u32, HashRange)>,
    ) -> Self {
        let kind = match (sources.len(), targets.len()) {
            (1, n) if n > 1 => JobKind::SplitShard,
            (n, 1) if n > 1 => JobKind::MergeShards,
            _ => JobKind::MoveShard,
        };
        Self {
            kind,
            tenant,
            sources: sources
                .into_iter()
                .map(|(shard, schemas, range)| SourceSpec {
                    cluster: from.clone(),
                    shard,
                    schemas,
                    tables: None,
                    range: Some(range),
                })
                .collect(),
            targets: targets
                .into_iter()
                .map(|(shard, range)| TargetSpec {
                    cluster: to.clone(),
                    shard,
                    schemas: Vec::new(),
                    range: Some(range),
                    ddl: None,
                })
                .collect(),
            routing: Routing::HashRange { group },
            cutover: CutoverMode::Auto,
            verify: VerifyMode::Full,
            decision_id: None,
            requested_by: None,
            max_hold_ms: None,
            retain_ms: None,
        }
    }

    /// The table group of a hash-range job (split, merge, shard move or
    /// re-key).
    #[must_use]
    pub fn group(&self) -> Option<u32> {
        match self.routing {
            Routing::HashRange { group } | Routing::Rekey { group, .. } => Some(group),
            _ => None,
        }
    }

    /// A re-key of table group `group` (chapter 16, "Online re-key"):
    /// `sources` are all of the group's shards, on one cluster, each
    /// `(shard, its schemas, its range)`; `targets` are new shards
    /// `(shard, range)` on `to` covering the whole hash space. Rows route
    /// by `key` (a `KeySpec` as JSON) over each member's `columns`.
    #[must_use]
    #[allow(clippy::too_many_arguments)] // the job's parts, as `reshard` plus the key
    pub fn rekey(
        tenant: TenantId,
        group: u32,
        from: &ClusterId,
        sources: Vec<(u32, Vec<String>, HashRange)>,
        to: &ClusterId,
        targets: Vec<(u32, HashRange)>,
        key: serde_json::Value,
        columns: std::collections::BTreeMap<String, Vec<String>>,
    ) -> Self {
        let mut s = Self::reshard(tenant, group, from, sources, to, targets);
        s.kind = JobKind::Rekey;
        s.routing = Routing::Rekey {
            group,
            key,
            columns,
        };
        s
    }
}

/// A job's state (chapter 10, "State machines").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    /// Created; waiting for a worker.
    Planned,
    /// Creating target schemas, tables and position rows.
    Preparing,
    /// Chunked copy with catch-up running alongside.
    Copying,
    /// Copy done; applying the binlog until lag is low.
    CatchingUp,
    /// Checksumming source against target.
    Verifying,
    /// Lag low and verified; waiting to cut over.
    Ready,
    /// Inside the write hold.
    CuttingOver,
    /// Committed: triggers, events and catalog updates roll forward.
    PostCutover,
    /// The fenced source is kept for `mover.source_retain_hours`.
    Retaining,
    /// Dropping the retained source.
    Cleaning,
    /// Finished.
    Done,
    /// Cancelled or failed before the commit: undoing.
    RollingBack,
    /// Undone; the source stayed authoritative throughout.
    RolledBack,
    /// Stopped by an operator or a guardrail; resumes into the same state.
    Paused,
    /// Reference copies after their seed (R1).
    Streaming,
}

impl JobState {
    /// The snake-case name used in records and metrics.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            JobState::Planned => "planned",
            JobState::Preparing => "preparing",
            JobState::Copying => "copying",
            JobState::CatchingUp => "catching_up",
            JobState::Verifying => "verifying",
            JobState::Ready => "ready",
            JobState::CuttingOver => "cutting_over",
            JobState::PostCutover => "post_cutover",
            JobState::Retaining => "retaining",
            JobState::Cleaning => "cleaning",
            JobState::Done => "done",
            JobState::RollingBack => "rolling_back",
            JobState::RolledBack => "rolled_back",
            JobState::Paused => "paused",
            JobState::Streaming => "streaming",
        }
    }

    /// `Done` or `RolledBack`: nothing more happens.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, JobState::Done | JobState::RolledBack)
    }

    /// The cutover committed: the targets own the data and the job only
    /// rolls forward.
    #[must_use]
    pub fn is_committed(self) -> bool {
        matches!(
            self,
            JobState::PostCutover | JobState::Retaining | JobState::Cleaning | JobState::Done
        )
    }

    /// Whether a worker should be running the job.
    #[must_use]
    pub fn is_runnable(self) -> bool {
        !self.is_terminal() && self != JobState::Paused
    }
}

impl std::fmt::Display for JobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How far a job got.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Progress {
    /// Tables to copy.
    pub tables: u32,
    /// Tables copied.
    pub tables_done: u32,
    /// Rows written by chunks.
    pub rows_copied: u64,
    /// Row changes applied from the binlog.
    pub rows_applied: u64,
    /// The last measured catch-up lag.
    pub lag_ms: Option<u64>,
    /// Cutover attempts so far (aborted ones included).
    pub cutover_attempts: u32,
    /// Write holds of the attempts, in milliseconds, oldest first.
    #[serde(default)]
    pub holds_ms: Vec<u64>,
    /// Ranges verification found different and repaired.
    pub verify_mismatches: u32,
    /// Copies restarted from scratch (purged binlog, diverged source).
    pub restarts: u32,
}

/// What [`Mover::status`] returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobStatus {
    /// The job.
    pub id: JobId,
    /// What it does.
    pub kind: JobKind,
    /// The tenant.
    pub tenant: TenantId,
    /// Its state.
    pub state: JobState,
    /// The source cluster.
    pub source: Option<ClusterId>,
    /// The target cluster.
    pub target: Option<ClusterId>,
    /// Progress counters.
    pub progress: Progress,
    /// The last error, kept while the job retries and after it fails.
    pub error: Option<String>,
    /// Cancellation was requested (applied at the next step).
    pub cancel_requested: bool,
    /// Chapter 11's decision row.
    pub decision_id: Option<u64>,
    /// The new epoch, once the cutover committed.
    pub epoch: Option<u64>,
    /// Created, ms since the Unix epoch.
    pub created_ms: u64,
    /// Last changed, ms since the Unix epoch.
    pub updated_ms: u64,
}

/// Which jobs [`Mover::list`] returns.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobFilter {
    /// Only this tenant's.
    pub tenant: Option<TenantId>,
    /// Only jobs that haven't finished.
    pub active_only: bool,
}

/// Submitting, observing and cancelling jobs. Implemented on the DC
/// tier's job records ([`crate::MetaMover`]); workers run the jobs.
#[async_trait::async_trait]
pub trait Mover: Send + Sync {
    /// Creates a job and takes its shard locks. Fails with 39131 if
    /// another job holds the tenant (or shard), with `Internal` for a spec
    /// R0 can't run.
    async fn submit(&self, spec: JobSpec) -> Result<JobId>;
    /// The job's state and progress. 39130 if there is no such job.
    async fn status(&self, id: &JobId) -> Result<JobStatus>;
    /// Asks the job to stop and roll back. Idempotent; 39132 once its
    /// cutover has committed (a committed move is undone by a new move).
    async fn cancel(&self, id: &JobId, reason: &str) -> Result<()>;
    /// Jobs matching `filter`, newest first.
    async fn list(&self, filter: &JobFilter) -> Result<Vec<JobStatus>>;
    /// Lets a `Manual` job in `ready` cut over.
    async fn cut_over(&self, id: &JobId) -> Result<()>;
}
