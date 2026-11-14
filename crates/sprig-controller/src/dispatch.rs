//! Registrations, jobs, dispatch and claims: one job scheduler for every
//! loop.
//!
//! Every automatic loop submits work as jobs to one scheduler, so loops
//! can't overload a cluster between them.
//!
//! ```text
//! dispatch (DC leader), on jobs/ or workers/ events and every 5 s:
//! 1. live = workers whose registration key exists
//! 2. each assigned or running job whose owner isn't live: guarded txn on ModRev(job):
//!    queued, owner = null, not_before = now + backoff (failed at control.job_max_attempts)
//! 3. each queued job by (priority desc, created_at), not_before passed:
//!    a. per-cluster slots by class: copy, build, recovery
//!    b. one exclusive job per tenant (migrate, promote); no index build while a
//!       copy job runs for the tenant, and no automatic copy while a build runs
//!    c. automatic jobs (cause != operator) for a tenant already started
//!       control.tenant_jobs_per_hour times in the last hour wait, and log Blocked
//!    d. w = live worker with a free slot: same zone as the target, fewest jobs, lowest id
//!    e. guarded txn [ModRev(job), CreateRev(workers/w)]: assigned, owner = {w, attempt+1}
//! worker: claim with txn [ModRev(job) == seen] -> running; every progress write compares
//! ModRev and, if the owner changed, aborts the work at once; losing its session aborts all
//! ```
//!
//! Job ids are UUIDv7, so ordering by id is ordering by creation. Times a
//! scheduler acts on (`not_before_ms`, `started_ms`, `finished_ms`) are Unix
//! milliseconds; `created_at` is RFC 3339 for people.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sprig_common::ids::{ClusterId, JobId, TenantId};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::elect::Leadership;
use sprig_meta::session::Session;
use sprig_meta::{
    Cmp, Consistency, Key, KeyValue, ListOpts, MetaStore, Op, Record, Revision, Txn, Versioned,
};

use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler, WatchSpec, backoff};

/// How often dispatch runs without events.
pub const DISPATCH_PERIOD: Duration = Duration::from_secs(5);
/// Finished jobs are deleted this long after they end.
pub const JOB_RETENTION: Duration = Duration::from_secs(7 * 24 * 3600);
/// The `cause` of jobs an operator started; they skip the per-tenant rate.
pub const OPERATOR: &str = "operator";
/// Job kinds of which a tenant may run only one at a time.
pub const EXCLUSIVE_KINDS: &[&str] = &["migrate", "promote"];

/// A job's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    /// Waiting for a worker.
    Queued,
    /// Given to a worker that hasn't claimed it yet.
    Assigned,
    /// Claimed and running.
    Running,
    /// Stopped by an operator; resumes to `running`.
    Paused,
    /// Finished.
    Succeeded,
    /// Gave up.
    Failed,
    /// Cancelled.
    Cancelled,
}

impl JobState {
    /// Every state.
    pub const ALL: [JobState; 7] = [
        JobState::Queued,
        JobState::Assigned,
        JobState::Running,
        JobState::Paused,
        JobState::Succeeded,
        JobState::Failed,
        JobState::Cancelled,
    ];

    /// `succeeded`, `failed` or `cancelled`.
    #[must_use]
    pub const fn is_final(self) -> bool {
        matches!(
            self,
            JobState::Succeeded | JobState::Failed | JobState::Cancelled
        )
    }

    /// `assigned` or `running`: a worker owns it and it holds a slot.
    #[must_use]
    pub const fn is_owned(self) -> bool {
        matches!(self, JobState::Assigned | JobState::Running)
    }

    /// The name in records and metrics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            JobState::Queued => "queued",
            JobState::Assigned => "assigned",
            JobState::Running => "running",
            JobState::Paused => "paused",
            JobState::Succeeded => "succeeded",
            JobState::Failed => "failed",
            JobState::Cancelled => "cancelled",
        }
    }
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Which slot pool a job uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobClass {
    /// Copies (`mover.max_copies_per_cluster`).
    Copy,
    /// Index and search builds (`auto_index.max_builds_per_cluster`).
    Build,
    /// Rebuilds and reseeds (`recovery.max_rebuilds_per_cluster`).
    Recovery,
    /// Everything else: no per-cluster limit.
    Other,
    /// Long-running consumers that mostly wait: stream sequencers and
    /// triggers on streams and queues. One job per
    /// stream or binding, so they get their own, larger pool
    /// ([`CONSUMER_SLOTS`] per worker) and never starve `Other` jobs or each
    /// other. No per-cluster limit.
    Consumer,
    /// Search copies: one long-running job per
    /// copy of an index. Only workers with the search role offer these
    /// slots. No per-cluster limit. Jobs whose spec names a `spread` key
    /// go to a worker holding no other live job with that key when one
    /// has a free slot (the copies of one index on different workers).
    Search,
}

/// Slots a worker with the search role offers for [`JobClass::Search`].
pub const SEARCH_SLOTS: u32 = 256;

/// Slots a worker offers for [`JobClass::Consumer`] jobs.
pub const CONSUMER_SLOTS: u32 = 256;

/// Who owns a job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Owner {
    /// The worker's registration id.
    pub worker_id: String,
    /// Which assignment this is, from 1.
    pub attempt: u32,
}

/// `jobs/<job_id>` in the DC tier. Each kind's own module owns `spec` and
/// `progress`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Job {
    /// The job.
    pub job_id: JobId,
    /// What it does: `copy`, `index_build`, `node_rebuild`, ...
    pub kind: String,
    /// Its slot pool.
    pub class: JobClass,
    /// The tenant it works for, if any.
    #[serde(default)]
    pub tenant_id: Option<TenantId>,
    /// The clusters it reads or writes; each needs a free slot.
    #[serde(default)]
    pub clusters: Vec<ClusterId>,
    /// Higher runs first.
    #[serde(default)]
    pub priority: i32,
    /// Its state.
    pub state: JobState,
    /// The worker that owns it while assigned or running.
    #[serde(default)]
    pub owner: Option<Owner>,
    /// Assignments so far; the next owner's `attempt` is one more.
    #[serde(default)]
    pub attempts: u32,
    /// Not assigned before this time (Unix ms).
    #[serde(default)]
    pub not_before_ms: u64,
    /// The loop that submitted it, or `operator`.
    pub cause: String,
    /// The zone to prefer for the worker, when the submitter knows it.
    #[serde(default)]
    pub zone: Option<String>,
    /// When it was submitted, RFC 3339 UTC; for people.
    #[serde(default)]
    pub created_at: String,
    /// When it was first assigned (Unix ms): what the per-tenant rate counts.
    #[serde(default)]
    pub started_ms: Option<u64>,
    /// When it reached a final state (Unix ms).
    #[serde(default)]
    pub finished_ms: Option<u64>,
    /// What to do; owned by the kind's own module.
    #[serde(default)]
    pub spec: Value,
    /// How far it got; owned by the kind's own module.
    #[serde(default)]
    pub progress: Value,
    /// Why it failed.
    #[serde(default)]
    pub error: Option<String>,
}

impl Versioned for Job {
    const V: u32 = 1;
}

impl Job {
    /// A new `queued` job.
    #[must_use]
    pub fn queued(job_id: JobId, new: NewJob, clock: &dyn Clock) -> Self {
        Self {
            job_id,
            kind: new.kind,
            class: new.class,
            tenant_id: new.tenant_id,
            clusters: new.clusters,
            priority: new.priority,
            state: JobState::Queued,
            owner: None,
            attempts: 0,
            not_before_ms: 0,
            cause: new.cause,
            zone: new.zone,
            created_at: rfc3339(clock.wall()),
            started_ms: None,
            finished_ms: None,
            spec: new.spec,
            progress: Value::Null,
            error: None,
        }
    }

    /// Whether an operator started it.
    #[must_use]
    pub fn by_operator(&self) -> bool {
        self.cause == OPERATOR
    }
}

/// What a loop or an operator submits.
#[derive(Debug, Clone, PartialEq)]
pub struct NewJob {
    /// What it does.
    pub kind: String,
    /// Its slot pool.
    pub class: JobClass,
    /// The tenant it works for.
    pub tenant_id: Option<TenantId>,
    /// The clusters it touches.
    pub clusters: Vec<ClusterId>,
    /// Higher runs first.
    pub priority: i32,
    /// The submitting loop, or [`OPERATOR`].
    pub cause: String,
    /// The zone to prefer.
    pub zone: Option<String>,
    /// The kind's spec.
    pub spec: Value,
}

/// The kinds of registered processes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RegistrationKind {
    /// `gateways/<id>`.
    Gateway,
    /// `workers/<id>`.
    Worker,
    /// `agents/<id>`.
    Agent,
}

impl RegistrationKind {
    /// The DC-tier prefix, ending in `/`.
    #[must_use]
    pub const fn prefix(self) -> &'static str {
        match self {
            RegistrationKind::Gateway => "gateways/",
            RegistrationKind::Worker => "workers/",
            RegistrationKind::Agent => "agents/",
        }
    }
}

/// `gateways/<id>`, `workers/<id>`, `agents/<id>`: a live process, bound to
/// its session's lease.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Registration {
    /// Where it serves.
    pub address: String,
    /// Its zone.
    #[serde(default)]
    pub zone: String,
    /// Its build.
    #[serde(default)]
    pub version: String,
    /// Its load, as it reports it.
    #[serde(default)]
    pub load: Value,
    /// Workers only: concurrent jobs per class. A class not listed gets
    /// none.
    #[serde(default)]
    pub slots: BTreeMap<JobClass, u32>,
}

impl Versioned for Registration {
    const V: u32 = 1;
}

/// Registers a process under its session's lease: the key goes when the
/// session ends.
///
/// # Errors
/// Internal for a bad id; the store's errors.
pub async fn register(
    session: &Session,
    kind: RegistrationKind,
    id: &str,
    reg: &Registration,
) -> Result<Revision> {
    let store = session.store();
    let key = store.tier().key(&format!("{}{id}", kind.prefix()))?;
    let r = store
        .txn(Txn {
            when: vec![],
            then: vec![Op::Put {
                key,
                value: Record::new(reg.clone()).encode()?,
                lease: Some(session.lease()),
            }],
            otherwise: vec![],
        })
        .await?;
    Ok(r.revision)
}

/// The key of a job in `store`'s tier.
///
/// # Errors
/// Internal if the key can't be built.
pub fn job_key(store: &dyn MetaStore, id: JobId) -> Result<Key> {
    store.tier().key(&format!("jobs/{id}"))
}

/// A stored job.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredJob {
    /// The record, with the fields this build doesn't know.
    pub record: Record<Job>,
    /// Its mod revision: the `ModRev` every change compares.
    pub mod_revision: Revision,
}

impl StoredJob {
    /// The job.
    #[must_use]
    pub fn job(&self) -> &Job {
        &self.record.body
    }

    fn decode(kv: &KeyValue) -> Result<Self> {
        Ok(Self {
            record: Record::decode(&kv.value)?,
            mod_revision: kv.mod_revision,
        })
    }
}

/// Reads one job.
///
/// # Errors
/// The store's errors; internal for a bad record.
pub async fn get_job(store: &dyn MetaStore, id: JobId) -> Result<Option<StoredJob>> {
    store
        .get(&job_key(store, id)?, Consistency::Linearizable)
        .await?
        .map(|kv| StoredJob::decode(&kv))
        .transpose()
}

/// Lists every key under `rel` at one revision.
async fn list_all(store: &dyn MetaStore, rel: &str) -> Result<Vec<KeyValue>> {
    let prefix = store.tier().key(rel)?;
    let mut out = Vec::new();
    let mut opts = ListOpts::default();
    loop {
        let page = store.list(&prefix, opts.clone()).await?;
        let more = page.more;
        opts.at = Some(page.revision);
        opts.after = page.kvs.last().map(|kv| kv.key.clone());
        out.extend(page.kvs);
        if !more {
            return Ok(out);
        }
    }
}

/// Every job of this DC; records that don't decode are logged and skipped.
///
/// # Errors
/// The store's errors.
pub async fn list_jobs(store: &dyn MetaStore) -> Result<Vec<StoredJob>> {
    Ok(list_all(store, "jobs/")
        .await?
        .iter()
        .filter_map(|kv| match StoredJob::decode(kv) {
            Ok(j) => Some(j),
            Err(e) => {
                tracing::warn!(key = %kv.key, error = %e, "skipping a bad job record");
                None
            }
        })
        .collect())
}

/// A live registration with its create revision.
#[derive(Debug, Clone, PartialEq)]
pub struct LiveWorker {
    /// The worker's id.
    pub id: String,
    /// What it registered.
    pub registration: Registration,
    /// The registration key's create revision, compared at assignment.
    pub create_revision: Revision,
}

/// Every live worker of this DC.
///
/// # Errors
/// The store's errors.
pub async fn live_workers(store: &dyn MetaStore) -> Result<Vec<LiveWorker>> {
    let prefix = store.tier().key(RegistrationKind::Worker.prefix())?;
    Ok(list_all(store, RegistrationKind::Worker.prefix())
        .await?
        .into_iter()
        .filter_map(|kv| {
            let id = kv.key.as_str().strip_prefix(prefix.as_str())?.to_owned();
            match Record::<Registration>::decode(&kv.value) {
                Ok(r) => Some(LiveWorker {
                    id,
                    registration: r.body,
                    create_revision: kv.create_revision,
                }),
                Err(e) => {
                    tracing::warn!(key = %kv.key, error = %e, "skipping a bad registration");
                    None
                }
            }
        })
        .collect())
}

fn unix_ms(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn put_job(key: Key, rec: &Record<Job>) -> Result<Op> {
    Ok(Op::Put {
        key,
        value: rec.encode()?,
        lease: None,
    })
}

// ---- leader side ------------------------------------------------------------

/// Submits a job as `queued`, guarded by the leadership.
///
/// # Errors
/// 39504 `ER_SPRIG_NOT_LEADER` if `l` no longer leads; the store's errors.
pub async fn submit(l: &Leadership, clock: &dyn Clock, new: NewJob) -> Result<JobId> {
    let store = l.session.store();
    let id = JobId::new_v7();
    let key = job_key(&**store, id)?;
    let job = Job::queued(id, new, clock);
    let r = l
        .commit_if(
            vec![Cmp::Missing(key.clone())],
            vec![put_job(key, &Record::new(job))?],
        )
        .await?;
    if !r.succeeded {
        return Err(SprigError::internal(format!("job id {id} already exists")));
    }
    tracing::info!(job = %id, "job submitted");
    Ok(id)
}

/// Submits a job as `queued` under an Admin API leader guard (the
/// leader's key compare) rather than a [`Leadership`] (an operator's
/// `node_rebuild` and `errant_extract`).
///
/// # Errors
/// 39504 `ER_SPRIG_NOT_LEADER` when the guard fails; the store's errors.
pub async fn submit_guarded(
    store: &dyn MetaStore,
    guard: Vec<Cmp>,
    clock: &dyn Clock,
    new: NewJob,
) -> Result<JobId> {
    let id = JobId::new_v7();
    let key = job_key(store, id)?;
    let job = Job::queued(id, new, clock);
    let mut when = guard;
    when.push(Cmp::Missing(key.clone()));
    let r = store
        .txn(Txn {
            when,
            then: vec![put_job(key, &Record::new(job))?],
            otherwise: Vec::new(),
        })
        .await?;
    if !r.succeeded {
        return Err(SprigError::new(
            SprigCode::NotLeader,
            "this controller no longer leads its DC; try again",
        ));
    }
    tracing::info!(job = %id, "job submitted");
    Ok(id)
}

/// Cancels a job that isn't final, guarded by the leadership. `false` if it
/// is missing or already final. Its worker sees the change at its next
/// progress write and stops.
///
/// # Errors
/// 39504 `ER_SPRIG_NOT_LEADER` if `l` no longer leads; the store's errors.
pub async fn cancel(l: &Leadership, clock: &dyn Clock, id: JobId) -> Result<bool> {
    let store = Arc::clone(l.session.store());
    loop {
        let Some(mut j) = get_job(&*store, id).await? else {
            return Ok(false);
        };
        if j.job().state.is_final() {
            return Ok(false);
        }
        let key = job_key(&*store, id)?;
        let body = &mut j.record.body;
        body.state = JobState::Cancelled;
        body.owner = None;
        body.finished_ms = Some(unix_ms(clock.wall()));
        let r = l
            .commit_if(
                vec![Cmp::ModRev(key.clone(), j.mod_revision)],
                vec![put_job(key, &j.record)?],
            )
            .await?;
        if r.succeeded {
            tracing::info!(job = %id, "job cancelled");
            return Ok(true);
        }
    }
}

// ---- worker side ------------------------------------------------------------

/// A job this worker claimed, with the revision its next write compares.
#[derive(Debug, Clone, PartialEq)]
pub struct Claimed {
    /// The job as last written by this worker.
    pub job: StoredJob,
}

/// Whether a worker still owns a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ownership {
    /// The write committed; carry on.
    Held,
    /// The job changed under the worker (reassigned or cancelled): stop the
    /// work at once.
    Lost,
}

/// The jobs assigned to `worker_id` and not yet claimed.
///
/// # Errors
/// The store's errors.
pub async fn assigned_to(store: &dyn MetaStore, worker_id: &str) -> Result<Vec<StoredJob>> {
    Ok(list_jobs(store)
        .await?
        .into_iter()
        .filter(|j| {
            j.job().state == JobState::Assigned
                && j.job()
                    .owner
                    .as_ref()
                    .is_some_and(|o| o.worker_id == worker_id)
        })
        .collect())
}

/// Claims an assigned job: `assigned` -> `running`, compared on the
/// revision the worker saw. `None` if it isn't assigned to `worker_id` any
/// more.
///
/// # Errors
/// The store's errors.
pub async fn claim(
    store: &dyn MetaStore,
    worker_id: &str,
    seen: StoredJob,
) -> Result<Option<Claimed>> {
    let owned = seen.job().state == JobState::Assigned
        && seen
            .job()
            .owner
            .as_ref()
            .is_some_and(|o| o.worker_id == worker_id);
    if !owned {
        return Ok(None);
    }
    let mut job = seen;
    job.record.body.state = JobState::Running;
    match write_owned(store, &mut job).await? {
        Ownership::Held => Ok(Some(Claimed { job })),
        Ownership::Lost => Ok(None),
    }
}

/// Writes progress, compared on the revision of the worker's last write.
///
/// # Errors
/// The store's errors.
pub async fn report_progress(
    store: &dyn MetaStore,
    claimed: &mut Claimed,
    progress: Value,
) -> Result<Ownership> {
    let before = claimed.job.record.body.progress.clone();
    claimed.job.record.body.progress = progress;
    let r = write_owned(store, &mut claimed.job).await;
    if !matches!(r, Ok(Ownership::Held)) {
        claimed.job.record.body.progress = before;
    }
    r
}

/// Ends a claimed job: `Ok(progress)` succeeds, `Err(reason)` fails.
///
/// # Errors
/// The store's errors.
pub async fn finish(
    store: &dyn MetaStore,
    clock: &dyn Clock,
    mut claimed: Claimed,
    result: std::result::Result<Value, String>,
) -> Result<Ownership> {
    let body = &mut claimed.job.record.body;
    match result {
        Ok(progress) => {
            body.state = JobState::Succeeded;
            body.progress = progress;
        }
        Err(reason) => {
            body.state = JobState::Failed;
            body.error = Some(reason);
        }
    }
    body.finished_ms = Some(unix_ms(clock.wall()));
    write_owned(store, &mut claimed.job).await
}

/// Writes `job` if its record is still at `job.mod_revision`.
async fn write_owned(store: &dyn MetaStore, job: &mut StoredJob) -> Result<Ownership> {
    let key = job_key(store, job.job().job_id)?;
    let r = store
        .txn(Txn {
            when: vec![Cmp::ModRev(key.clone(), job.mod_revision)],
            then: vec![put_job(key, &job.record)?],
            otherwise: vec![],
        })
        .await?;
    if r.succeeded {
        job.mod_revision = r.revision;
        Ok(Ownership::Held)
    } else {
        tracing::warn!(job = %job.job().job_id, "job changed under its worker; stopping it");
        Ok(Ownership::Lost)
    }
}

// ---- the dispatch loop ------------------------------------------------------

/// The one key of the dispatch loop: it looks at every job at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Pass;

impl fmt::Display for Pass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("pass")
    }
}

/// The dispatch loop (DC leader).
#[derive(Debug, Clone, Copy, Default)]
pub struct Dispatch;

/// What one dispatch pass did, for tests and logs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PassReport {
    /// Jobs given to workers: (job, worker).
    pub assigned: Vec<(JobId, String)>,
    /// Jobs whose owner was gone, put back in the queue.
    pub requeued: Vec<JobId>,
    /// Jobs that failed after `control.job_max_attempts`.
    pub failed: Vec<JobId>,
    /// Jobs waiting on the per-tenant rate.
    pub rate_limited: Vec<JobId>,
    /// The earliest `not_before_ms` still in the future.
    pub next_not_before_ms: Option<u64>,
    /// Queued jobs held because their tenant is suspended.
    pub held_suspended: Vec<JobId>,
    /// Jobs of a suspended tenant cancelled (running consumers, due
    /// schedule runs); their loops submit them again, held, until resume.
    pub cancelled_suspended: Vec<JobId>,
}

/// Job kinds that wait while their tenant is suspended: work that runs the tenant's own
/// code or sends its data out. Housekeeping (moves, index builds, search
/// copies, key-value and TTL sweeps, stream sequencers, outbox forwarders,
/// change exchanges) goes on, so the data stays as the tenant left it.
pub const PAUSED_WHILE_SUSPENDED: &[&str] = &[
    sprig_sinks::meta::JOB_KIND,
    crate::logic_loop::TRIGGER_KIND,
    sprig_logic::messages::STREAM_JOB_KIND,
    sprig_logic::messages::QUEUE_JOB_KIND,
    sprig_logic::schedule::JOB_KIND,
];

/// The tenant a job works for: its `tenant_id`, or `spec.tenant` (logic
/// and sink jobs carry it there).
fn tenant_of(job: &Job) -> Option<u64> {
    job.tenant_id
        .map(|t| t.0)
        .or_else(|| job.spec["tenant"].as_u64())
}

/// Which of `tenants` are suspended, themselves or through an ancestor on
/// their directory path. A tenant whose entry can't be read counts as not
/// suspended: a read error never stops work.
async fn suspended_tenants(
    global: &dyn MetaStore,
    tenants: impl IntoIterator<Item = u64>,
) -> HashSet<u64> {
    #[derive(Serialize, Deserialize)]
    struct Entry {
        #[serde(default)]
        suspended: bool,
        #[serde(default)]
        path: String,
    }
    impl Versioned for Entry {
        const V: u32 = 1;
    }
    async fn entry(global: &dyn MetaStore, id: u64) -> Option<Entry> {
        let key = global.tier().key(&format!("tenants/{id}")).ok()?;
        let kv = global.get(&key, Consistency::Serializable).await.ok()??;
        Record::<Entry>::decode(&kv.value).ok().map(|r| r.body)
    }
    let mut seen: HashMap<u64, (bool, String)> = HashMap::new();
    let mut out = HashSet::new();
    for t in tenants.into_iter().collect::<HashSet<_>>() {
        if let std::collections::hash_map::Entry::Vacant(v) = seen.entry(t) {
            let e = entry(global, t).await;
            v.insert(e.map_or((false, String::new()), |e| (e.suspended, e.path)));
        }
        let (own, path) = seen.get(&t).cloned().unwrap_or_default();
        if own {
            out.insert(t);
            continue;
        }
        for anc in path
            .split('/')
            .filter_map(|p| p.parse::<u64>().ok())
            .filter(|a| *a != t)
        {
            if let std::collections::hash_map::Entry::Vacant(v) = seen.entry(anc) {
                let e = entry(global, anc).await;
                v.insert(e.map_or((false, String::new()), |e| (e.suspended, e.path)));
            }
            if seen.get(&anc).is_some_and(|e| e.0) {
                out.insert(t);
                break;
            }
        }
    }
    out
}

#[async_trait::async_trait]
impl Reconciler for Dispatch {
    type Key = Pass;

    fn name(&self) -> &'static str {
        "dispatch"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn watches(&self) -> Vec<WatchSpec> {
        vec![
            WatchSpec::dc("jobs/"),
            WatchSpec::dc(RegistrationKind::Worker.prefix()),
        ]
    }

    fn keys_for(&self, ev: &MetaEvent) -> Vec<Pass> {
        let path = ev.path();
        if path.starts_with(RegistrationKind::Worker.prefix()) {
            return vec![Pass];
        }
        if !path.starts_with("jobs/") {
            return vec![];
        }
        // Progress of a running job changes nothing dispatch decides.
        let busy = ev.kv.as_ref().is_some_and(|kv| {
            Record::<Job>::decode(&kv.value)
                .is_ok_and(|r| matches!(r.body.state, JobState::Running | JobState::Paused))
        });
        if busy { vec![] } else { vec![Pass] }
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<Pass>> {
        Ok(vec![Pass])
    }

    async fn reconcile(&self, cx: &LoopCx, _key: &Pass) -> Result<Outcome> {
        let report = dispatch_pass(cx).await?;
        let now = unix_ms(cx.clock.wall());
        let wait = report
            .next_not_before_ms
            .map_or(DISPATCH_PERIOD, |t| {
                Duration::from_millis(t.saturating_sub(now)).min(DISPATCH_PERIOD)
            })
            .max(Duration::from_millis(10));
        Ok(Outcome::RequeueAfter(wait))
    }
}

/// Per-cluster slot limit of a class; `None` for no limit.
fn slot_limit(cx: &LoopCx, class: JobClass) -> Option<u32> {
    let s = cx.settings();
    match class {
        JobClass::Copy => Some(s.max_copies_per_cluster),
        JobClass::Build => Some(s.max_builds_per_cluster),
        JobClass::Recovery => Some(s.max_rebuilds_per_cluster),
        JobClass::Other | JobClass::Consumer | JobClass::Search => None,
    }
}

/// What the jobs in flight hold.
#[derive(Default)]
struct Held {
    /// Slots in use per (class, cluster).
    slots: HashMap<(JobClass, ClusterId), u32>,
    /// Jobs per worker, and per (worker, class).
    per_worker: HashMap<String, u32>,
    per_worker_class: HashMap<(String, JobClass), u32>,
    /// Per tenant: active classes and whether an exclusive job is active.
    tenant_classes: HashMap<TenantId, HashSet<JobClass>>,
    tenant_active: HashSet<TenantId>,
    tenant_exclusive: HashSet<TenantId>,
    /// (worker, spread key) of live jobs with a `spread` key.
    spread: HashSet<(String, String)>,
}

/// A job's `spread` key (`spec.spread`), if any.
fn spread_of(job: &Job) -> Option<&str> {
    job.spec.get("spread").and_then(Value::as_str)
}

impl Held {
    fn take(&mut self, job: &Job) {
        if job.state.is_owned()
            && let (Some(o), Some(k)) = (&job.owner, spread_of(job))
        {
            self.spread.insert((o.worker_id.clone(), k.to_owned()));
        }
        if job.state.is_owned() {
            for c in &job.clusters {
                *self.slots.entry((job.class, c.clone())).or_default() += 1;
            }
            if let Some(o) = &job.owner {
                *self.per_worker.entry(o.worker_id.clone()).or_default() += 1;
                *self
                    .per_worker_class
                    .entry((o.worker_id.clone(), job.class))
                    .or_default() += 1;
            }
        }
        if (job.state.is_owned() || job.state == JobState::Paused)
            && let Some(t) = job.tenant_id
        {
            self.tenant_classes.entry(t).or_default().insert(job.class);
            self.tenant_active.insert(t);
            if EXCLUSIVE_KINDS.contains(&job.kind.as_str()) {
                self.tenant_exclusive.insert(t);
            }
        }
    }
}

/// One dispatch pass: steps 1 to 3.
///
/// # Errors
/// 39504 `ER_SPRIG_NOT_LEADER` if leadership is lost; the store's errors.
#[allow(clippy::too_many_lines)] // the algorithm reads best in one piece
pub async fn dispatch_pass(cx: &LoopCx) -> Result<PassReport> {
    let l = cx.leader()?;
    let store = &*cx.dc;
    let settings = cx.settings();
    let now = unix_ms(cx.clock.wall());
    let mut report = PassReport::default();

    // 1. Live workers.
    let workers = live_workers(store).await?;
    let live: HashMap<&str, &LiveWorker> = workers.iter().map(|w| (w.id.as_str(), w)).collect();
    let mut jobs = list_jobs(store).await?;

    // 2. Jobs whose owner is gone.
    for j in &mut jobs {
        let job = j.job();
        let orphan = job.state.is_owned()
            && job
                .owner
                .as_ref()
                .is_none_or(|o| !live.contains_key(o.worker_id.as_str()));
        if !orphan {
            continue;
        }
        let id = job.job_id;
        let mut next = j.record.clone();
        let body = &mut next.body;
        let owner = body.owner.take().map(|o| o.worker_id).unwrap_or_default();
        if body.attempts >= settings.job_max_attempts {
            body.state = JobState::Failed;
            body.finished_ms = Some(now);
            body.error = Some(format!(
                "owner lost {} times (control.job_max_attempts)",
                body.attempts
            ));
        } else {
            body.state = JobState::Queued;
            let wait = backoff(body.attempts.max(1));
            body.not_before_ms = now + u64::try_from(wait.as_millis()).unwrap_or(u64::MAX);
        }
        let key = job_key(store, id)?;
        let r = l
            .commit_if(
                vec![Cmp::ModRev(key.clone(), j.mod_revision)],
                vec![put_job(key, &next)?],
            )
            .await?;
        if r.succeeded {
            tracing::warn!(job = %id, worker = %owner, state = %next.body.state, "job owner lost");
            if next.body.state == JobState::Failed {
                report.failed.push(id);
            } else {
                report.requeued.push(id);
            }
            j.record = next;
            j.mod_revision = r.revision;
        }
    }

    // 2b. Suspension: a suspended tenant's paused kinds
    //     wait. Running and assigned ones are cancelled (their workers stop
    //     at the next progress write) and their loops submit them again;
    //     queued ones stay queued; a due schedule run is skipped.
    let paused_kind = |j: &Job| PAUSED_WHILE_SUSPENDED.contains(&j.kind.as_str());
    let suspended = suspended_tenants(
        &*cx.global,
        jobs.iter()
            .map(StoredJob::job)
            .filter(|j| paused_kind(j) && !j.state.is_final())
            .filter_map(tenant_of),
    )
    .await;
    if !suspended.is_empty() {
        for j in &jobs {
            let job = j.job();
            if !paused_kind(job)
                || job.state.is_final()
                || !tenant_of(job).is_some_and(|t| suspended.contains(&t))
            {
                continue;
            }
            let skip_run = job.kind == sprig_logic::schedule::JOB_KIND;
            if (job.state != JobState::Queued || skip_run)
                && cancel(l, cx.clock.as_ref(), job.job_id).await?
            {
                tracing::info!(job = %job.job_id, kind = %job.kind, tenant = ?tenant_of(job),
                    "job of a suspended tenant cancelled");
                report.cancelled_suspended.push(job.job_id);
            }
        }
    }

    // What the jobs in flight hold, and each tenant's starts in the last hour.
    let mut held = Held::default();
    let mut starts: HashMap<TenantId, u32> = HashMap::new();
    let hour_ago = now.saturating_sub(3_600_000);
    for j in &jobs {
        held.take(j.job());
        let job = j.job();
        if let (Some(t), Some(s)) = (job.tenant_id, job.started_ms)
            && s > hour_ago
            && !job.by_operator()
        {
            *starts.entry(t).or_default() += 1;
        }
    }

    // 3. Queued jobs, by priority then age.
    let mut queued: Vec<usize> = jobs
        .iter()
        .enumerate()
        .filter(|(_, j)| j.job().state == JobState::Queued)
        .map(|(i, _)| i)
        .collect();
    queued.sort_by(|&a, &b| {
        let (a, b) = (jobs[a].job(), jobs[b].job());
        b.priority.cmp(&a.priority).then(a.job_id.cmp(&b.job_id))
    });
    for i in queued {
        let job = jobs[i].job().clone();
        if paused_kind(&job) && tenant_of(&job).is_some_and(|t| suspended.contains(&t)) {
            // Cancelled in 2b (a due schedule run): not held.
            if !report.cancelled_suspended.contains(&job.job_id) {
                report.held_suspended.push(job.job_id);
            }
            continue;
        }
        if job.not_before_ms > now {
            report.next_not_before_ms = Some(
                report
                    .next_not_before_ms
                    .map_or(job.not_before_ms, |t| t.min(job.not_before_ms)),
            );
            continue;
        }
        // a. Slots on every cluster the job touches.
        if let Some(limit) = slot_limit(cx, job.class)
            && job.clusters.iter().any(|c| {
                held.slots
                    .get(&(job.class, c.clone()))
                    .copied()
                    .unwrap_or(0)
                    >= limit
            })
        {
            continue;
        }
        // b. Per-tenant exclusion.
        if let Some(t) = job.tenant_id {
            let exclusive = EXCLUSIVE_KINDS.contains(&job.kind.as_str());
            if held.tenant_exclusive.contains(&t) || (exclusive && held.tenant_active.contains(&t))
            {
                continue;
            }
            let classes = held.tenant_classes.get(&t);
            let has = |c| classes.is_some_and(|s| s.contains(&c));
            if (job.class == JobClass::Build && has(JobClass::Copy))
                || (job.class == JobClass::Copy && !job.by_operator() && has(JobClass::Build))
            {
                continue;
            }
            // c. The per-tenant rate, for automatic jobs not yet started.
            if !job.by_operator()
                && job.started_ms.is_none()
                && starts.get(&t).copied().unwrap_or(0) >= settings.tenant_jobs_per_hour
            {
                tracing::info!(job = %job.job_id, tenant = %t, "blocked: control.tenant_jobs_per_hour");
                report.rate_limited.push(job.job_id);
                continue;
            }
        }
        // d. A live worker with a free slot of the class: same zone first,
        //    then fewest jobs, then lowest id.
        let pick = workers
            .iter()
            .filter(|w| {
                let cap = w.registration.slots.get(&job.class).copied().unwrap_or(0);
                let used = held
                    .per_worker_class
                    .get(&(w.id.clone(), job.class))
                    .copied()
                    .unwrap_or(0);
                used < cap
            })
            .min_by_key(|w| {
                // A worker already holding a job of the same spread key
                // last (search copies of one index).
                let crowded = spread_of(&job)
                    .is_some_and(|k| held.spread.contains(&(w.id.clone(), k.to_owned())));
                let other_zone = job.zone.as_ref().is_some_and(|z| *z != w.registration.zone);
                let load = held.per_worker.get(&w.id).copied().unwrap_or(0);
                (crowded, other_zone, load, w.id.clone())
            });
        let Some(w) = pick else { continue };
        // e. Assign, guarded by the job and the worker's registration.
        let mut next = jobs[i].record.clone();
        let body = &mut next.body;
        body.attempts += 1;
        body.state = JobState::Assigned;
        body.owner = Some(Owner {
            worker_id: w.id.clone(),
            attempt: body.attempts,
        });
        body.started_ms.get_or_insert(now);
        let key = job_key(store, job.job_id)?;
        let wkey = store
            .tier()
            .key(&format!("{}{}", RegistrationKind::Worker.prefix(), w.id))?;
        let r = l
            .commit_if(
                vec![
                    Cmp::ModRev(key.clone(), jobs[i].mod_revision),
                    Cmp::CreateRev(wkey, w.create_revision),
                ],
                vec![put_job(key, &next)?],
            )
            .await?;
        if !r.succeeded {
            continue;
        }
        tracing::info!(job = %job.job_id, kind = %job.kind, worker = %w.id, attempt = next.body.attempts, "job assigned");
        if job.started_ms.is_none()
            && !job.by_operator()
            && let Some(t) = job.tenant_id
        {
            *starts.entry(t).or_default() += 1;
        }
        held.take(&next.body);
        report.assigned.push((job.job_id, w.id.clone()));
        jobs[i].record = next;
        jobs[i].mod_revision = r.revision;
    }

    publish_job_metrics(&jobs, &starts);
    Ok(report)
}

fn publish_job_metrics(jobs: &[StoredJob], starts: &HashMap<TenantId, u32>) {
    let mut counts: BTreeMap<(String, JobState), u32> = BTreeMap::new();
    let kinds: HashSet<&str> = jobs.iter().map(|j| j.job().kind.as_str()).collect();
    for kind in kinds {
        for state in JobState::ALL {
            counts.insert((kind.to_owned(), state), 0);
        }
    }
    for j in jobs {
        *counts
            .entry((j.job().kind.clone(), j.job().state))
            .or_default() += 1;
    }
    for ((kind, state), n) in counts {
        metrics::gauge!("sprig_controller_jobs", "kind" => kind, "state" => state.as_str())
            .set(f64::from(n));
    }
    let max = starts.values().copied().max().unwrap_or(0);
    metrics::gauge!("sprig_controller_tenant_jobs_per_hour_max").set(f64::from(max));
}

// ---- the gc loop ------------------------------------------------------------

/// The `gc` loop's job part (DC leader): deletes finished jobs 7 days after
/// they end. Dropping retired source schemas is the mover's part.
#[derive(Debug, Clone, Copy, Default)]
pub struct JobGc;

#[async_trait::async_trait]
impl Reconciler for JobGc {
    type Key = JobId;

    fn name(&self) -> &'static str {
        "gc"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn watches(&self) -> Vec<WatchSpec> {
        vec![WatchSpec::dc("jobs/")]
    }

    fn keys_for(&self, ev: &MetaEvent) -> Vec<JobId> {
        ev.kv
            .as_ref()
            .and_then(|kv| Record::<Job>::decode(&kv.value).ok())
            .filter(|r| r.body.state.is_final())
            .map(|r| vec![r.body.job_id])
            .unwrap_or_default()
    }

    async fn all_keys(&self, cx: &LoopCx) -> Result<Vec<JobId>> {
        Ok(list_jobs(&*cx.dc)
            .await?
            .iter()
            .filter(|j| j.job().state.is_final())
            .map(|j| j.job().job_id)
            .collect())
    }

    async fn reconcile(&self, cx: &LoopCx, id: &JobId) -> Result<Outcome> {
        let l = cx.leader()?;
        let Some(j) = get_job(&*cx.dc, *id).await? else {
            return Ok(Outcome::Done);
        };
        let (true, Some(finished)) = (j.job().state.is_final(), j.job().finished_ms) else {
            return Ok(Outcome::Done);
        };
        let retention = u64::try_from(JOB_RETENTION.as_millis()).unwrap_or(u64::MAX);
        let age = unix_ms(cx.clock.wall()).saturating_sub(finished);
        if age < retention {
            return Ok(Outcome::RequeueAfter(Duration::from_millis(
                retention - age,
            )));
        }
        let key = job_key(&*cx.dc, *id)?;
        let r = l
            .commit_if(
                vec![Cmp::ModRev(key.clone(), j.mod_revision)],
                vec![Op::Delete(key)],
            )
            .await?;
        if r.succeeded {
            tracing::info!(job = %id, "finished job deleted");
            Ok(Outcome::Done)
        } else {
            // Changed meanwhile: look again.
            Ok(Outcome::RequeueAfter(Duration::ZERO))
        }
    }
}

/// Formats a time as RFC 3339 UTC with second precision.
#[allow(clippy::many_single_char_names)] // the algorithm's usual names
pub(crate) fn rfc3339(t: SystemTime) -> String {
    let secs = t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
    let (days, rem) = (secs / 86_400, secs % 86_400);
    let (h, m, s) = (rem / 3600, rem % 3600 / 60, rem % 60);
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + u64::from(mo <= 2);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}

#[cfg(test)]
mod tests;
