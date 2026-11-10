//! Rollout (chapter 13 build step 10, "Rollout"): an accepted index is
//! built shard by shard, online, inside the build window, one build per
//! cluster slot, with lock-safe retries and aborts.
//!
//! ```text
//! advance(x):                                  # driver::advance, one step per call
//!   not_before not reached                     → wait
//!   for the first shard not built:
//!     gates: the build window is open; a slot under max_builds_per_cluster is
//!            free (in-process holders + sprig builds in the writer's
//!            PROCESSLIST + build jobs); the shard isn't moving (chapter 10's
//!            hook); the cluster is healthy                     else → wait
//!     method := choose_method(topology, ddl_method, table size, mover?)
//!     Online       (single, primary + replicas): online::build
//!     GaleraRsu    rolling RSU, one node at a time:            galera::build_rsu
//!     GaleraToi    total order, tables under toi_max_bytes:    galera::build_toi
//!     CopySwap     the M0.6 mover (CopySwapHook), when there is one
//!     Recommend    nothing safe: stop at a recommendation
//!     outcome:
//!       built          → shard built; next shard after rollout_pause
//!       lock timeouts  → 5 tries 30 s apart inside the build, then the next window
//!       user DDL       → user wins: retried after cooldown_hours
//!       lag, degraded  → retried after retry_after
//!       online log too big, other failures → failures += 1; at 3 → failed
//!   every shard built → observing
//! ```
//!
//! **Online builds** run on the shard's writer, replicated normally (the
//! binary log stays on), as `ALTER TABLE ... ADD INDEX IF NOT EXISTS x
//! (...), ALGORITHM=INPLACE, LOCK=NONE` with `lock_wait_timeout =
//! auto_index.ddl_lock_wait_seconds`. While it runs a watcher checks, every
//! `poll`:
//!
//! - **user DDL** on the table in the `PROCESSLIST` (running or waiting for
//!   our metadata lock): the build is killed at once, so the user's
//!   statement waits at most one poll;
//! - **the metadata-lock queue**: while the build itself still waits for
//!   its first metadata lock and client statements queue behind it for
//!   longer than `queue_kill_after`, the build is killed and retried later
//!   (the chapter's short lock wait, tightened: clients never wait more
//!   than about a second behind a build that hasn't started);
//! - **replicas**: replication stopped, or lag above
//!   `auto_index.abort_replica_lag_seconds`;
//! - **the writer**: `read_only` turned on (failover).
//!
//! A build abandoned by its caller (a deposed leader's reconcile dropped)
//! is killed too ([`node::KillGuard`]), so no orphaned `ALTER` keeps a slot.
//!
//! **Galera.** The chapter builds by copy-and-swap through the mover, which
//! is M0.6. Until then [`GaleraPolicy`] chooses (see [`choose_method`]),
//! by default [`GaleraSwitch::ToiOnly`]: total order (TOI) for tables up
//! to `toi_max_bytes` (64 MiB), whose build holds the cluster's writes for
//! about the build time (measured: 0.3 s for a 21 MB table), and only a
//! recommendation above it. A rolling RSU build ([`galera::build_rsu`]),
//! one node at a time with the safety checks of
//! [`RsuValidator`](super::validate::galera::RsuValidator), is implemented
//! behind [`GaleraSwitch::RollingRsu`] but measured unsafe without a way to
//! drain the node first (see the switch). [`GaleraSwitch::Mover`] hands the
//! build to a [`CopySwapHook`], which is what M0.6 plugs in.
//!
//! **Never during a move or cutover**: [`ShardLocks`] is chapter 10's hook.
//! [`NoMover`] says no shard ever moves (true until M0.6); the controller
//! checks the tenant's copy jobs.

pub mod driver;
pub mod galera;
pub mod node;
pub mod online;

#[cfg(test)]
mod live_tests;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sprig_cluster::driver::DdlMethod;
use sprig_cluster::sql::NodeEndpoint;
use sprig_common::Result;
use sprig_common::ids::{ClusterId, NodeId, TenantId};

use super::model::IndexDef;
use super::policy::BuildWindow;

/// The nodes of a cluster as a build needs them.
#[derive(Debug, Clone)]
pub enum Nodes {
    /// One node.
    Single {
        /// The node.
        writer: NodeEndpoint,
    },
    /// A primary and its replicas; builds run on the primary and replicate.
    PrimaryReplica {
        /// The primary.
        writer: NodeEndpoint,
        /// The replicas, watched for lag and for the replicated index.
        replicas: Vec<NodeEndpoint>,
    },
    /// A Galera cluster.
    Galera {
        /// Every node.
        nodes: Vec<NodeEndpoint>,
        /// The node clients write the shard through now (chapter 09's
        /// writer choice); built last by a rolling build.
        writer: NodeId,
    },
}

impl Nodes {
    /// The node DDL is sent to.
    #[must_use]
    pub fn writer(&self) -> Option<&NodeEndpoint> {
        match self {
            Nodes::Single { writer } | Nodes::PrimaryReplica { writer, .. } => Some(writer),
            Nodes::Galera { nodes, writer } => {
                nodes.iter().find(|n| &n.node == writer).or(nodes.first())
            }
        }
    }

    /// Every node.
    #[must_use]
    pub fn all(&self) -> Vec<&NodeEndpoint> {
        match self {
            Nodes::Single { writer } => vec![writer],
            Nodes::PrimaryReplica { writer, replicas } => {
                std::iter::once(writer).chain(replicas).collect()
            }
            Nodes::Galera { nodes, .. } => nodes.iter().collect(),
        }
    }

    /// Whether this is Galera.
    #[must_use]
    pub const fn is_galera(&self) -> bool {
        matches!(self, Nodes::Galera { .. })
    }
}

/// A cluster as the rollout sees it: its nodes, chapter 09's verdicts, and
/// whether the cluster is healthy now.
#[derive(Debug, Clone)]
pub struct ClusterView {
    /// The cluster.
    pub cluster: ClusterId,
    /// Its nodes.
    pub nodes: Nodes,
    /// Chapter 09's `ddl_method(AddIndex, traits)` for the table.
    pub ddl_method: DdlMethod,
    /// `features.instant_ignored_toggle`: retirement passes through
    /// `IGNORED`.
    pub instant_ignored_toggle: bool,
    /// `None` when healthy; else why not (chapter 09's health).
    pub unhealthy: Option<String>,
}

/// One shard of an object, in physical names.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShardTable {
    /// The shard id, as `auto_indexes.shards` keys it.
    pub shard: String,
    /// The shard schema (`t42_d1_g3_s17`).
    pub schema: String,
    /// The table.
    pub table: String,
}

/// What to do on Galera until the mover exists (chapter 13 says
/// copy-and-swap; that is M0.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GaleraSwitch {
    /// TOI up to `toi_max_bytes`, rolling RSU above it. Not the default:
    /// rolling RSU is unsafe until gateways drain a node (below).
    Auto,
    /// Always rolling RSU. Measured on 11.8 (Galera 4): an RSU `ALTER`
    /// pauses the node's provider, so every commit on that node waits;
    /// a client writing through the node holds the table's metadata lock
    /// while it waits, the `ALTER` waits for that lock, and only
    /// `lock_wait_timeout` breaks the cycle (writes stalled 15-18 s for a
    /// 21 MB table). Safe only once [`NodeMarker`](super::validate::NodeMarker)
    /// really moves writes and reads off the node (chapter 09's writer
    /// weight 0 honoured by gateways), which R0 doesn't have.
    RollingRsu,
    /// TOI up to `toi_max_bytes`; recommend above it. The M0.4 default:
    /// the cluster's writes wait for the build (measured 0.3 s for 21 MB),
    /// which the size limit bounds.
    ToiOnly,
    /// Copy-and-swap through a [`CopySwapHook`] (M0.6); recommend when
    /// there is no hook.
    Mover,
    /// Never build on Galera; recommend.
    RecommendOnly,
}

/// The Galera rules.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct GaleraPolicy {
    /// The switch.
    pub switch: GaleraSwitch,
    /// The largest table (data plus indexes) built in total order.
    pub toi_max_bytes: u64,
    /// Rolling RSU needs at least this many nodes, all `Synced`.
    pub min_cluster_size: u32,
    /// The least time to wait for `Synced` after a node's build; the wait is
    /// `max(this, 2 × the build)`.
    pub resync_floor: Duration,
}

impl Default for GaleraPolicy {
    fn default() -> Self {
        Self {
            switch: GaleraSwitch::ToiOnly,
            toi_max_bytes: 64 << 20,
            min_cluster_size: 3,
            resync_floor: Duration::from_secs(30),
        }
    }
}

/// How one shard is built.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "method", content = "why")]
pub enum BuildMethod {
    /// `ALGORITHM=INPLACE, LOCK=NONE` on the writer, replicated.
    Online,
    /// Galera, total order, small tables.
    GaleraToi,
    /// Galera, rolling RSU, node by node.
    GaleraRsu,
    /// The mover's copy-and-swap (M0.6).
    CopySwap,
    /// No safe method: stop at a recommendation.
    Recommend(String),
}

impl BuildMethod {
    /// The metric label.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            BuildMethod::Online => "online",
            BuildMethod::GaleraToi => "galera_toi",
            BuildMethod::GaleraRsu => "galera_rsu",
            BuildMethod::CopySwap => "copy_swap",
            BuildMethod::Recommend(_) => "recommend",
        }
    }
}

/// Chooses the method for one table.
///
/// - Not Galera: chapter 09's method; `Online` builds online, anything else
///   needs the mover (or is recommended without one).
/// - Galera, by [`GaleraPolicy::switch`]: see [`GaleraSwitch`]. Rolling RSU
///   needs `min_cluster_size` nodes.
#[must_use]
pub fn choose_method(
    view: &ClusterView,
    table_bytes: u64,
    policy: &GaleraPolicy,
    has_mover: bool,
) -> BuildMethod {
    let Nodes::Galera { nodes, .. } = &view.nodes else {
        return match view.ddl_method {
            DdlMethod::Online => BuildMethod::Online,
            DdlMethod::CopySwap | DdlMethod::Toi if has_mover => BuildMethod::CopySwap,
            DdlMethod::CopySwap | DdlMethod::Toi => {
                BuildMethod::Recommend("no online build here and no mover (M0.6)".into())
            }
        };
    };
    let small = table_bytes <= policy.toi_max_bytes;
    let rsu_ok = u32::try_from(nodes.len()).unwrap_or(u32::MAX) >= policy.min_cluster_size;
    let rsu = || {
        if rsu_ok {
            BuildMethod::GaleraRsu
        } else {
            BuildMethod::Recommend(format!(
                "rolling RSU needs {} nodes; the cluster has {}",
                policy.min_cluster_size,
                nodes.len()
            ))
        }
    };
    match policy.switch {
        GaleraSwitch::Mover if has_mover => BuildMethod::CopySwap,
        GaleraSwitch::Mover => {
            BuildMethod::Recommend("copy-and-swap needs the mover (M0.6)".into())
        }
        GaleraSwitch::RecommendOnly => BuildMethod::Recommend("galera builds are off".into()),
        GaleraSwitch::ToiOnly | GaleraSwitch::Auto if small => BuildMethod::GaleraToi,
        GaleraSwitch::ToiOnly => BuildMethod::Recommend(format!(
            "{table_bytes} bytes is over the TOI limit of {}",
            policy.toi_max_bytes
        )),
        GaleraSwitch::Auto | GaleraSwitch::RollingRsu => rsu(),
    }
}

/// Settings of rollout (resolved `auto_index.*` keys plus the M0.4
/// constants the book doesn't name yet).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RolloutSettings {
    /// `auto_index.ddl_lock_wait_seconds` (5).
    pub ddl_lock_wait: Duration,
    /// Lock-wait retries inside one build attempt (the chapter's 5).
    pub lock_retries: u32,
    /// Between those retries (the chapter's 30 s).
    pub lock_retry_pause: Duration,
    /// Kill a build still waiting for its first metadata lock when client
    /// statements have queued behind it this long.
    pub queue_kill_after: Duration,
    /// `auto_index.abort_replica_lag_seconds` (300).
    pub abort_replica_lag: Duration,
    /// `auto_index.max_build_hours` (6): the statement time limit.
    pub max_build: Duration,
    /// How often a running build is checked.
    pub poll: Duration,
    /// The least time to wait for replicas to carry the index after the
    /// build; the wait is `max(this, 2 × the build)`.
    pub catch_up_floor: Duration,
    /// `auto_index.rollout_pause_minutes` (10): between shards.
    pub rollout_pause: Duration,
    /// `auto_index.cooldown_hours` (24): after user DDL won.
    pub user_ddl_cooldown: Duration,
    /// After an abort for lag or degradation.
    pub retry_after: Duration,
    /// Failures before `failed` (the chapter's 3).
    pub max_failures: u32,
    /// `auto_index.max_builds_per_cluster` (1).
    pub max_builds_per_cluster: u32,
    /// `auto_index.build_window`, resolved and tightened.
    #[serde(skip, default = "auto_window")]
    pub build_window: BuildWindow,
    /// Galera.
    pub galera: GaleraPolicy,
}

fn auto_window() -> BuildWindow {
    BuildWindow::Auto
}

impl Default for RolloutSettings {
    fn default() -> Self {
        Self {
            ddl_lock_wait: Duration::from_secs(5),
            lock_retries: 5,
            lock_retry_pause: Duration::from_secs(30),
            queue_kill_after: Duration::from_secs(1),
            abort_replica_lag: Duration::from_secs(300),
            max_build: Duration::from_secs(6 * 3600),
            poll: Duration::from_millis(250),
            catch_up_floor: Duration::from_secs(60),
            rollout_pause: Duration::from_secs(600),
            user_ddl_cooldown: Duration::from_secs(24 * 3600),
            retry_after: Duration::from_secs(300),
            max_failures: 3,
            max_builds_per_cluster: 1,
            build_window: BuildWindow::Auto,
            galera: GaleraPolicy::default(),
        }
    }
}

/// Why a build stopped early.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AbortReason {
    /// User DDL on the table: the user wins.
    UserDdl,
    /// A replica lagged past the limit.
    ReplicaLag,
    /// The cluster degraded: replication stopped, a node left, the writer
    /// turned read-only.
    Degraded,
    /// The shard is moving (chapter 10).
    Moving,
}

impl AbortReason {
    /// The text form.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            AbortReason::UserDdl => "user_ddl",
            AbortReason::ReplicaLag => "replica_lag",
            AbortReason::Degraded => "degraded",
            AbortReason::Moving => "moving",
        }
    }
}

/// One node's build, for the report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeBuild {
    /// The node.
    pub node: String,
    /// The statement, ms.
    pub build_ms: u64,
    /// Waiting for `Synced` afterwards (Galera RSU), ms.
    pub resync_ms: Option<u64>,
    /// The index was already there.
    pub skipped: bool,
}

/// What one shard's build did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildReport {
    /// The shard.
    pub shard: String,
    /// How.
    pub method: String,
    /// The index was already on the shard (an earlier attempt, or kept by
    /// replica validation); nothing ran.
    pub already_present: bool,
    /// The build statement(s), ms, wall clock.
    pub build_ms: u64,
    /// Lock-wait timeouts (and queue kills) before it went through.
    pub lock_retries: u32,
    /// Replicas: until every replica had the index, ms.
    pub replicas_ms: Option<u64>,
    /// The highest replica lag seen during and after the build, s.
    pub max_replica_lag_s: Option<u64>,
    /// Per node (Galera).
    pub nodes: Vec<NodeBuild>,
}

/// The result of one build attempt on one shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "outcome")]
pub enum BuildOutcome {
    /// Built.
    Built(BuildReport),
    /// The metadata lock wasn't granted within `lock_retries` tries.
    LockTimeout {
        /// Tries made.
        tries: u32,
    },
    /// Stopped by the watcher or a pre-check; retried later.
    Aborted {
        /// Why.
        reason: AbortReason,
        /// Details for the log.
        detail: String,
    },
    /// `innodb_online_alter_log_max_size` exceeded (1799).
    OnlineLogTooBig,
    /// Anything else; counts towards `max_failures`.
    Failed {
        /// The error.
        error: String,
    },
}

/// Build slots per cluster (`auto_index.max_builds_per_cluster`).
pub trait BuildSlots: Send + Sync {
    /// Takes a slot of `cluster` if fewer than `limit` are in use,
    /// counting `elsewhere` builds this process doesn't hold (sprig builds
    /// seen running on the cluster, build jobs of the scheduler).
    fn try_acquire(&self, cluster: &ClusterId, limit: u32, elsewhere: u32) -> Option<SlotGuard>;
}

/// A held slot; released on drop.
pub struct SlotGuard {
    release: Option<Box<dyn FnOnce() + Send>>,
}

impl std::fmt::Debug for SlotGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlotGuard").finish_non_exhaustive()
    }
}

impl SlotGuard {
    /// A guard running `release` when dropped.
    #[must_use]
    pub fn new(release: impl FnOnce() + Send + 'static) -> Self {
        Self {
            release: Some(Box::new(release)),
        }
    }
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        if let Some(f) = self.release.take() {
            f();
        }
    }
}

/// Slots counted in this process.
#[derive(Debug, Clone, Default)]
pub struct LocalSlots {
    held: Arc<Mutex<HashMap<ClusterId, u32>>>,
}

impl LocalSlots {
    /// No slots held.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Slots held for `cluster`.
    #[must_use]
    pub fn held(&self, cluster: &ClusterId) -> u32 {
        self.held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(cluster)
            .copied()
            .unwrap_or(0)
    }
}

impl BuildSlots for LocalSlots {
    fn try_acquire(&self, cluster: &ClusterId, limit: u32, elsewhere: u32) -> Option<SlotGuard> {
        let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        let n = held.entry(cluster.clone()).or_insert(0);
        if n.saturating_add(elsewhere) >= limit.max(1) {
            return None;
        }
        *n += 1;
        let map = Arc::clone(&self.held);
        let c = cluster.clone();
        Some(SlotGuard::new(move || {
            let mut held = map.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(n) = held.get_mut(&c) {
                *n = n.saturating_sub(1);
            }
        }))
    }
}

/// Whether a shard may be built now (chapter 10's shard lock).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardLock {
    /// Nothing moves it.
    Free,
    /// A move, split, copy-and-swap or cutover holds it.
    Moving(String),
}

/// Chapter 10's hook: builds never run during a move or cutover.
#[async_trait::async_trait]
pub trait ShardLocks: Send + Sync {
    /// The lock state of `shard` of `tenant`.
    ///
    /// # Errors
    /// The lock couldn't be read; the build waits.
    async fn state(&self, tenant: TenantId, shard: &str) -> Result<ShardLock>;
}

/// No mover yet (M0.6): nothing ever moves.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoMover;

#[async_trait::async_trait]
impl ShardLocks for NoMover {
    async fn state(&self, _tenant: TenantId, _shard: &str) -> Result<ShardLock> {
        Ok(ShardLock::Free)
    }
}

/// A copy-and-swap build request (chapter 10's `copy_swap_ddl`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopySwapRequest {
    /// The tenant.
    pub tenant: TenantId,
    /// The cluster.
    pub cluster: ClusterId,
    /// The shard and table.
    pub table: ShardTable,
    /// The DDL the shadow gets: `ADD INDEX x (...)`.
    pub ddl: String,
}

/// Where a copy-and-swap stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopySwapStatus {
    /// Submitted or running; ask again later.
    Running,
    /// Swapped; the index is on the shard.
    Done,
    /// Failed; counts towards `max_failures`.
    Failed(String),
}

/// The M0.6 mover's copy-and-swap, as the rollout uses it. Submitting the
/// same request again returns the existing job's status.
#[async_trait::async_trait]
pub trait CopySwapHook: Send + Sync {
    /// Submits (or looks up) the job.
    ///
    /// # Errors
    /// The job couldn't be submitted or read.
    async fn copy_swap(&self, req: &CopySwapRequest) -> Result<CopySwapStatus>;
}

/// The build window at `now`: `Ok` when open, else how long until it
/// opens. `Auto` uses `auto` (the least busy 4 hours, [`auto_window`])
/// when known, else is always open.
///
/// # Errors
/// The time until the window opens.
pub fn window_gate(
    window: BuildWindow,
    auto: Option<BuildWindow>,
    now: SystemTime,
) -> std::result::Result<(), Duration> {
    let w = match (window, auto) {
        (BuildWindow::Auto, Some(a)) => a,
        (w, _) => w,
    };
    let secs = now.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
    let minute = u16::try_from((secs / 60) % 1440).unwrap_or(0);
    if w.contains(minute) {
        return Ok(());
    }
    let wait = (1..=1440u16)
        .find(|m| w.contains((minute + m) % 1440))
        .unwrap_or(1);
    let into_minute = secs % 60;
    Err(Duration::from_secs(u64::from(wait) * 60 - into_minute))
}

/// `auto_index.build_window = auto`: the least busy 4 contiguous hours of
/// the day, from load per hour of day (UTC) summed over the past 7 days.
/// Ties go to the earliest start.
#[must_use]
pub fn least_busy_window(load_by_hour: &[f64; 24]) -> BuildWindow {
    let mut best = (f64::INFINITY, 0u16);
    for start in 0..24u16 {
        let sum: f64 = (0..4u16)
            .map(|i| load_by_hour[usize::from((start + i) % 24)])
            .sum();
        if sum < best.0 {
            best = (sum, start);
        }
    }
    BuildWindow::Fixed {
        start: best.1 * 60,
        end: ((best.1 + 4) % 24) * 60,
    }
}

/// The rollout's progress, kept in `auto_indexes.observed.rollout`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Progress {
    /// Build attempts (per shard attempt, any outcome).
    #[serde(default)]
    pub attempts: u32,
    /// Failures counting towards `max_failures`.
    #[serde(default)]
    pub failures: u32,
    /// Online-log failures (copy-and-swap after 3, once the mover exists).
    #[serde(default)]
    pub online_log_failures: u32,
    /// Builds that ran out of lock retries.
    #[serde(default)]
    pub lock_timeouts: u32,
    /// Not before this time, Unix ms.
    #[serde(default)]
    pub not_before_ms: u64,
    /// The last thing that went wrong.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Why it is waiting, for `sprigctl index show`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting: Option<String>,
    /// Per built shard.
    #[serde(default)]
    pub builds: Vec<BuildReport>,
    /// When the last shard was built, Unix ms: observation starts here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub built_at_ms: Option<u64>,
}

impl Progress {
    /// Reads `observed.rollout`.
    #[must_use]
    pub fn of(observed: Option<&serde_json::Value>) -> Self {
        observed
            .and_then(|o| o.get("rollout"))
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default()
    }

    /// `observed` with `rollout` replaced.
    #[must_use]
    pub fn into_observed(&self, observed: Option<&serde_json::Value>) -> serde_json::Value {
        let mut o = match observed {
            Some(serde_json::Value::Object(m)) => serde_json::Value::Object(m.clone()),
            _ => serde_json::json!({}),
        };
        if let (Some(m), Ok(v)) = (o.as_object_mut(), serde_json::to_value(self)) {
            m.insert("rollout".into(), v);
        }
        o
    }
}

/// Unix ms of `t`.
#[must_use]
pub fn unix_ms(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// The `ADD INDEX` clause of `def`: `ADD INDEX IF NOT EXISTS x (...)`.
#[must_use]
pub fn add_index_clause(def: &IndexDef) -> String {
    // `SPATIAL INDEX` and `VECTOR INDEX` too (R6 round 3).
    format!("ADD {}", def.add_clause())
}
