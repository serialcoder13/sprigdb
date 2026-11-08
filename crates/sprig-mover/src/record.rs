//! Job records in the DC tier, claims, leases, tokens, shard locks and
//! copy slots (chapter 10, "Job record", "Jobs and workers"; build step 3).
//!
//! ```text
//! /sprig/dc/<dc>/mover/jobs/<job_id>                 the record (JSON, below)
//! /sprig/dc/<dc>/mover/owners/<job_id>               the owning worker, on its lease
//! /sprig/dc/<dc>/mover/shard-locks/<tenant>/<shard>  the job id; one job per shard
//! /sprig/dc/<dc>/mover/slots/copy/<cluster>/<n>      the job id, on the worker's lease
//! ```
//!
//! **Decision, against the book.** Chapter 10 puts records at `jobs/`,
//! where chapter 23's job scheduler (being built alongside) keeps its own,
//! differently shaped records and dispatches them. Until mover jobs run
//! through that scheduler, they live under `mover/` so neither side
//! misreads the other's records.
//!
//! A submit creates the record and the shard lock in one transaction, so
//! two jobs never hold one shard. The lock lives as long as the job, not a
//! worker: a crashed worker's job keeps its shard until a successor ends
//! it. A worker claims a runnable job whose owner key is missing (its
//! owner's lease ended) with one transaction that creates the owner key on
//! its lease and raises the record's `token`; the token is then raised on
//! every target position row, so a stale worker's target writes match no
//! row. Every later record write by the owner compares the owner key's
//! create revision, so a worker that lost its lease can't write either.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sprig_common::ids::{ClusterId, JobId, TenantId};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{
    Cmp, Consistency, Key, KeyValue, LeaseId, ListOpts, MetaStore, Op, Record, Revision, Txn,
    Versioned,
};

use crate::api::{
    CutoverMode, JobFilter, JobKind, JobSpec, JobState, JobStatus, Mover, Progress, Routing,
};

/// Where a cutover is, so a successor knows what to undo or finish.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CutoverStep {
    /// The map is held (or about to be).
    Holding,
    /// The sources are being fenced; receipts follow.
    Fencing,
    /// Fenced; catching up to `G` and finalizing the targets.
    Fenced,
    /// The commit was sent; its outcome decides.
    Committing,
}

/// The job record (chapter 10, "Job record").
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobRecord {
    /// The job.
    pub job_id: JobId,
    /// What it does.
    pub kind: JobKind,
    /// The tenant.
    pub tenant_id: TenantId,
    /// What was asked.
    pub spec: JobSpec,
    /// Where it is.
    pub state: JobState,
    /// The state a paused job resumes into.
    #[serde(default)]
    pub paused_from: Option<JobState>,
    /// During `cutting_over`.
    #[serde(default)]
    pub cutover_step: Option<CutoverStep>,
    /// Fence receipts of the current cutover attempt.
    #[serde(default)]
    pub receipts: Vec<serde_json::Value>,
    /// The fencing token of the current (or last) owner.
    #[serde(default)]
    pub token: u64,
    /// The current owner's worker id.
    #[serde(default)]
    pub owner: Option<String>,
    /// Counters.
    #[serde(default)]
    pub progress: Progress,
    /// The last error.
    #[serde(default)]
    pub error: Option<String>,
    /// Why cancellation was requested.
    #[serde(default)]
    pub cancel: Option<String>,
    /// A `manual` job may cut over.
    #[serde(default)]
    pub go: bool,
    /// Verification passed (an aborted cutover doesn't verify again).
    #[serde(default)]
    pub verified: bool,
    /// The epoch after the commit.
    #[serde(default)]
    pub epoch: Option<u64>,
    /// No cutover before this time (ms since the epoch): the backoff.
    #[serde(default)]
    pub not_before_ms: u64,
    /// When a retained source may be dropped.
    #[serde(default)]
    pub retain_until_ms: Option<u64>,
    /// Chapter 11's decision row.
    #[serde(default)]
    pub decision_id: Option<u64>,
    /// Created, ms since the epoch.
    pub created_ms: u64,
    /// Last written, ms since the epoch.
    pub updated_ms: u64,
}

impl Versioned for JobRecord {
    const V: u32 = 1;
}

impl JobRecord {
    /// The status callers see.
    #[must_use]
    pub fn status(&self) -> JobStatus {
        JobStatus {
            id: self.job_id,
            kind: self.kind,
            tenant: self.tenant_id,
            state: self.state,
            source: self.spec.source_cluster().cloned(),
            target: self.spec.target_cluster().cloned(),
            progress: self.progress.clone(),
            error: self.error.clone(),
            cancel_requested: self.cancel.is_some(),
            decision_id: self.decision_id,
            epoch: self.epoch,
            created_ms: self.created_ms,
            updated_ms: self.updated_ms,
        }
    }

    /// The shard lock this job holds.
    #[must_use]
    pub fn lock_rel(&self) -> String {
        lock_rel(
            self.tenant_id,
            self.spec.sources.first().map_or(0, |s| s.shard),
        )
    }
}

/// Milliseconds since the Unix epoch of `t`.
#[must_use]
pub fn wall_ms(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn job_rel(id: &JobId) -> String {
    format!("mover/jobs/{}", id.0)
}

fn owner_rel(id: &JobId) -> String {
    format!("mover/owners/{}", id.0)
}

fn lock_rel(tenant: TenantId, shard: u32) -> String {
    format!("mover/shard-locks/{tenant}/{shard}")
}

fn slot_rel(cluster: &ClusterId, n: u32) -> String {
    format!("mover/slots/copy/{cluster}/{n}")
}

fn not_found(id: &JobId) -> SprigError {
    SprigError::new(SprigCode::JobNotFound, format!("no job {}", id.0))
}

fn lease_lost(id: &JobId) -> SprigError {
    SprigError::new(
        SprigCode::JobLeaseLost,
        format!("this worker no longer owns job {}", id.0),
    )
}

/// The job store of one DC tier.
#[derive(Clone)]
pub struct JobStore {
    dc: Arc<dyn MetaStore>,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for JobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobStore")
            .field("tier", self.dc.tier())
            .finish_non_exhaustive()
    }
}

impl JobStore {
    /// Jobs in `dc` (the DC tier of the source clusters).
    #[must_use]
    pub fn new(dc: Arc<dyn MetaStore>, clock: Arc<dyn Clock>) -> Self {
        Self { dc, clock }
    }

    /// The store.
    #[must_use]
    pub fn meta(&self) -> &Arc<dyn MetaStore> {
        &self.dc
    }

    /// The clock.
    #[must_use]
    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    fn key(&self, rel: &str) -> Result<Key> {
        self.dc.tier().key(rel)
    }

    /// Now, in ms since the epoch.
    #[must_use]
    pub fn now_ms(&self) -> u64 {
        wall_ms(self.clock.wall())
    }

    /// A record and its `mod_revision`.
    ///
    /// # Errors
    /// 39130 if missing; the store's errors.
    pub async fn get(&self, id: &JobId) -> Result<(JobRecord, Revision)> {
        let kv = self
            .dc
            .get(&self.key(&job_rel(id))?, Consistency::Linearizable)
            .await?
            .ok_or_else(|| not_found(id))?;
        Ok((
            Record::<JobRecord>::decode(&kv.value)?.body,
            kv.mod_revision,
        ))
    }

    /// Every record, oldest first.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn all(&self) -> Result<Vec<(JobRecord, Revision)>> {
        let prefix = self.key("mover/jobs/")?;
        let mut out = Vec::new();
        let mut opts = ListOpts::default();
        loop {
            let page = self.dc.list(&prefix, opts.clone()).await?;
            for kv in &page.kvs {
                match Record::<JobRecord>::decode(&kv.value) {
                    Ok(r) => out.push((r.body, kv.mod_revision)),
                    Err(e) => {
                        tracing::warn!(key = %kv.key.as_str(), error = %e, "unreadable job record");
                    }
                }
            }
            if !page.more {
                break;
            }
            opts.at = Some(page.revision);
            opts.after = page.kvs.last().map(|kv| kv.key.clone());
        }
        Ok(out)
    }

    /// The owner key of a job, if a live worker owns it.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn owner(&self, id: &JobId) -> Result<Option<KeyValue>> {
        self.dc
            .get(&self.key(&owner_rel(id))?, Consistency::Linearizable)
            .await
    }

    /// Creates a job and its shard lock.
    ///
    /// # Errors
    /// 39131 if the shard is locked; `Internal` for a spec R0 can't run.
    pub async fn create(&self, spec: JobSpec) -> Result<JobRecord> {
        check_spec(&spec)?;
        let now = self.now_ms();
        let rec = JobRecord {
            job_id: JobId::new_v7(),
            kind: spec.kind,
            tenant_id: spec.tenant,
            decision_id: spec.decision_id,
            spec,
            state: JobState::Planned,
            paused_from: None,
            cutover_step: None,
            receipts: Vec::new(),
            token: 0,
            owner: None,
            progress: Progress::default(),
            error: None,
            cancel: None,
            go: false,
            verified: false,
            epoch: None,
            not_before_ms: 0,
            retain_until_ms: None,
            created_ms: now,
            updated_ms: now,
        };
        let lock = self.key(&rec.lock_rel())?;
        let r = self
            .dc
            .txn(Txn {
                when: vec![Cmp::Missing(lock.clone())],
                then: vec![
                    Op::Put {
                        key: lock.clone(),
                        value: rec.job_id.0.to_string().into_bytes(),
                        lease: None,
                    },
                    Op::Put {
                        key: self.key(&job_rel(&rec.job_id))?,
                        value: Record::new(rec.clone()).encode()?,
                        lease: None,
                    },
                ],
                otherwise: vec![Op::Get(lock)],
            })
            .await?;
        if r.succeeded {
            tracing::info!(job = %rec.job_id.0, kind = rec.kind.as_str(), tenant = %rec.tenant_id, "job created");
            return Ok(rec);
        }
        let holder = match r.results.first() {
            Some(sprig_meta::OpResult::Get(Some(kv))) => {
                String::from_utf8_lossy(&kv.value).into_owned()
            }
            _ => "?".to_owned(),
        };
        Err(SprigError::new(
            SprigCode::ShardLocked,
            format!("tenant {} is locked by job {holder}", rec.tenant_id),
        ))
    }

    /// Writes `rec` if its record is still at `rev` (writers without an
    /// owner: cancel, manual cutover). Returns the new revision, or `None`
    /// if it changed.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn put_if(&self, rec: &JobRecord, rev: Revision) -> Result<Option<Revision>> {
        let key = self.key(&job_rel(&rec.job_id))?;
        let r = self
            .dc
            .txn(Txn {
                when: vec![Cmp::ModRev(key.clone(), rev)],
                then: vec![Op::Put {
                    key,
                    value: Record::new(rec.clone()).encode()?,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        Ok(r.succeeded.then_some(r.revision))
    }

    /// Claims an unowned runnable job for `worker` on `lease`: creates the
    /// owner key and raises the token. Returns the claim, or `None` if the
    /// job changed or someone else owns it.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn claim(
        &self,
        rec: &JobRecord,
        rev: Revision,
        worker: &str,
        lease: LeaseId,
    ) -> Result<Option<Claim>> {
        let jkey = self.key(&job_rel(&rec.job_id))?;
        let okey = self.key(&owner_rel(&rec.job_id))?;
        let mut next = rec.clone();
        next.token += 1;
        next.owner = Some(worker.to_owned());
        next.updated_ms = self.now_ms();
        let r = self
            .dc
            .txn(Txn {
                when: vec![Cmp::ModRev(jkey.clone(), rev), Cmp::Missing(okey.clone())],
                then: vec![
                    Op::Put {
                        key: okey.clone(),
                        value: worker.as_bytes().to_vec(),
                        lease: Some(lease),
                    },
                    Op::Put {
                        key: jkey,
                        value: Record::new(next.clone()).encode()?,
                        lease: None,
                    },
                ],
                otherwise: Vec::new(),
            })
            .await?;
        if !r.succeeded {
            return Ok(None);
        }
        let kv = self
            .dc
            .get(&okey, Consistency::Linearizable)
            .await?
            .ok_or_else(|| lease_lost(&rec.job_id))?;
        Ok(Some(Claim {
            store: self.clone(),
            session: None,
            owner_key: okey,
            owner_rev: kv.create_revision,
            lease,
            rec: next,
        }))
    }

    /// Deletes the shard lock and the owner key of a finished job.
    fn unlock_ops(&self, rec: &JobRecord) -> Result<Vec<Op>> {
        Ok(vec![
            Op::Delete(self.key(&rec.lock_rel())?),
            Op::Delete(self.key(&owner_rel(&rec.job_id))?),
        ])
    }

    /// Takes a free copy slot on `cluster` for `job` under `lease`. Returns
    /// whether it got one (or already holds one).
    ///
    /// # Errors
    /// The store's errors.
    pub async fn take_slot(
        &self,
        cluster: &ClusterId,
        job: &JobId,
        max: u32,
        lease: LeaseId,
    ) -> Result<bool> {
        let me = job.0.to_string();
        for n in 0..max.max(1) {
            let key = self.key(&slot_rel(cluster, n))?;
            let r = self
                .dc
                .txn(Txn {
                    when: vec![Cmp::Missing(key.clone())],
                    then: vec![Op::Put {
                        key: key.clone(),
                        value: me.clone().into_bytes(),
                        lease: Some(lease),
                    }],
                    otherwise: vec![Op::Get(key)],
                })
                .await?;
            if r.succeeded {
                return Ok(true);
            }
            if let Some(sprig_meta::OpResult::Get(Some(kv))) = r.results.first()
                && kv.value == me.as_bytes()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Frees `job`'s copy slots on `cluster`.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn free_slots(&self, cluster: &ClusterId, job: &JobId, max: u32) -> Result<()> {
        let me = job.0.to_string();
        for n in 0..max.max(1) {
            let key = self.key(&slot_rel(cluster, n))?;
            if let Some(kv) = self.dc.get(&key, Consistency::Linearizable).await?
                && kv.value == me.as_bytes()
            {
                self.dc
                    .txn(Txn {
                        when: vec![Cmp::ModRev(key.clone(), kv.mod_revision)],
                        then: vec![Op::Delete(key)],
                        otherwise: Vec::new(),
                    })
                    .await?;
            }
        }
        Ok(())
    }
}

/// A worker's ownership of one job: every write is guarded by the owner
/// key it created.
#[derive(Clone)]
pub struct Claim {
    store: JobStore,
    session: Option<sprig_meta::Session>,
    owner_key: Key,
    owner_rev: Revision,
    lease: LeaseId,
    /// The record as last written by this owner.
    pub rec: JobRecord,
}

impl std::fmt::Debug for Claim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Claim")
            .field("job", &self.rec.job_id)
            .field("token", &self.rec.token)
            .finish_non_exhaustive()
    }
}

impl Claim {
    /// Ties the claim to the worker's session, so [`Self::check_lease`]
    /// knows when the lease is lost (by its conservative local deadline,
    /// before etcd can give the job to another worker).
    #[must_use]
    pub fn with_session(mut self, session: sprig_meta::Session) -> Self {
        self.session = Some(session);
        self
    }

    /// Fails with 39133 once the worker's lease is lost. The engine calls
    /// it before every step outside MariaDB's token check: hold, fence,
    /// unfence, commit.
    ///
    /// # Errors
    /// 39133.
    pub fn check_lease(&self) -> Result<()> {
        if self
            .session
            .as_ref()
            .is_some_and(sprig_meta::Session::is_lost)
        {
            return Err(lease_lost(&self.rec.job_id));
        }
        Ok(())
    }

    /// The fencing token.
    #[must_use]
    pub fn token(&self) -> u64 {
        self.rec.token
    }

    /// The worker's lease.
    #[must_use]
    pub fn lease(&self) -> LeaseId {
        self.lease
    }

    /// The store.
    #[must_use]
    pub fn store(&self) -> &JobStore {
        &self.store
    }

    /// Writes the record after `f` changes it, if this worker still owns
    /// the job. Fields only operators write (`cancel`, `go`) are merged from
    /// the stored record first.
    ///
    /// # Errors
    /// 39133 if the ownership is gone; the store's errors.
    pub async fn save(&mut self, f: impl FnOnce(&mut JobRecord)) -> Result<()> {
        let mut next = self.rec.clone();
        f(&mut next);
        for _ in 0..4 {
            let (stored, rev) = self.store.get(&next.job_id).await?;
            next.cancel = stored.cancel.clone();
            next.go = stored.go;
            next.updated_ms = self.store.now_ms();
            let jkey = self.store.key(&job_rel(&next.job_id))?;
            let r = self
                .store
                .dc
                .txn(Txn {
                    when: vec![
                        Cmp::CreateRev(self.owner_key.clone(), self.owner_rev),
                        Cmp::ModRev(jkey.clone(), rev),
                    ],
                    then: vec![Op::Put {
                        key: jkey,
                        value: Record::new(next.clone()).encode()?,
                        lease: None,
                    }],
                    otherwise: vec![Op::Get(self.owner_key.clone())],
                })
                .await?;
            if r.succeeded {
                self.rec = next;
                return Ok(());
            }
            match r.results.first() {
                Some(sprig_meta::OpResult::Get(Some(kv)))
                    if kv.create_revision == self.owner_rev =>
                {
                    // Only the record changed (an operator wrote it): retry.
                }
                _ => return Err(lease_lost(&next.job_id)),
            }
        }
        Err(lease_lost(&next.job_id))
    }

    /// Re-reads operator fields (`cancel`, `go`) into the record.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn refresh(&mut self) -> Result<()> {
        let (stored, _) = self.store.get(&self.rec.job_id).await?;
        self.rec.cancel = stored.cancel;
        self.rec.go = stored.go;
        Ok(())
    }

    /// Ends the job in `state` (`Done` or `RolledBack`): writes the record
    /// and deletes the shard lock and owner key in one transaction.
    ///
    /// # Errors
    /// 39133 if the ownership is gone; the store's errors.
    pub async fn finish(&mut self, state: JobState, error: Option<String>) -> Result<()> {
        let mut next = self.rec.clone();
        next.state = state;
        next.cutover_step = None;
        if error.is_some() {
            next.error = error;
        }
        next.updated_ms = self.store.now_ms();
        let jkey = self.store.key(&job_rel(&next.job_id))?;
        let mut then = vec![Op::Put {
            key: jkey,
            value: Record::new(next.clone()).encode()?,
            lease: None,
        }];
        then.extend(self.store.unlock_ops(&next)?);
        let r = self
            .store
            .dc
            .txn(Txn {
                when: vec![Cmp::CreateRev(self.owner_key.clone(), self.owner_rev)],
                then,
                otherwise: Vec::new(),
            })
            .await?;
        if !r.succeeded {
            return Err(lease_lost(&next.job_id));
        }
        self.rec = next;
        Ok(())
    }

    /// Gives the job back (the worker is stopping): deletes the owner key
    /// so another worker can claim it at once.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn release(&self) -> Result<()> {
        self.store
            .dc
            .txn(Txn {
                when: vec![Cmp::CreateRev(self.owner_key.clone(), self.owner_rev)],
                then: vec![Op::Delete(self.owner_key.clone())],
                otherwise: Vec::new(),
            })
            .await?;
        Ok(())
    }
}

/// Checks a spec against what this release runs.
fn check_spec(spec: &JobSpec) -> Result<()> {
    let bad = |m: &str| Err(SprigError::internal(format!("unsupported job: {m}")));
    match spec.kind {
        JobKind::MoveShard | JobKind::CopySwapDdl => {}
        k => return bad(&format!("{} arrives in a later release", k.as_str())),
    }
    if spec.sources.len() != 1 || spec.targets.len() != 1 {
        return bad("R0 jobs have one source and one target");
    }
    if spec.routing != Routing::Whole {
        return bad("R0 jobs route whole shards");
    }
    if matches!(
        spec.cutover,
        CutoverMode::Group { .. } | CutoverMode::Continuous
    ) {
        return bad("group and continuous cutovers arrive in R1");
    }
    let (s, t) = (&spec.sources[0], &spec.targets[0]);
    match spec.kind {
        JobKind::MoveShard if s.cluster == t.cluster => bad("a move needs another cluster"),
        JobKind::CopySwapDdl
            if s.cluster != t.cluster
                || t.ddl.is_none()
                || s.tables.as_ref().map(Vec::len) != Some(1) =>
        {
            bad("a copy-and-swap names one table on one cluster and its DDL")
        }
        _ => Ok(()),
    }
}

/// [`Mover`] on the DC tier's job records: what the balancer and the Admin
/// API use. Workers ([`crate::worker`]) run the jobs.
#[derive(Clone, Debug)]
pub struct MetaMover {
    store: JobStore,
}

impl MetaMover {
    /// On `store`.
    #[must_use]
    pub fn new(store: JobStore) -> Self {
        Self { store }
    }

    /// The job store.
    #[must_use]
    pub fn store(&self) -> &JobStore {
        &self.store
    }

    async fn edit(
        &self,
        id: &JobId,
        mut f: impl FnMut(&mut JobRecord) -> Result<bool> + Send,
    ) -> Result<()> {
        for _ in 0..8 {
            let (mut rec, rev) = self.store.get(id).await?;
            if !f(&mut rec)? {
                return Ok(());
            }
            rec.updated_ms = self.store.now_ms();
            if self.store.put_if(&rec, rev).await?.is_some() {
                return Ok(());
            }
        }
        Err(SprigError::new(
            SprigCode::CatalogConflict,
            format!("job {} kept changing", id.0),
        ))
    }
}

#[async_trait::async_trait]
impl Mover for MetaMover {
    async fn submit(&self, spec: JobSpec) -> Result<JobId> {
        Ok(self.store.create(spec).await?.job_id)
    }

    async fn status(&self, id: &JobId) -> Result<JobStatus> {
        Ok(self.store.get(id).await?.0.status())
    }

    async fn cancel(&self, id: &JobId, reason: &str) -> Result<()> {
        let (rec, rev) = self.store.get(id).await?;
        if rec.state.is_committed() {
            return Err(SprigError::new(
                SprigCode::JobStateInvalid,
                format!(
                    "job {} committed its cutover ({}); move the tenant back with a new job",
                    id.0, rec.state
                ),
            ));
        }
        if rec.state.is_terminal() {
            return Ok(());
        }
        // A job no worker started yet ends here.
        if rec.state == JobState::Planned && self.store.owner(id).await?.is_none() {
            let mut next = rec.clone();
            next.state = JobState::RolledBack;
            next.cancel = Some(reason.to_owned());
            next.updated_ms = self.store.now_ms();
            let jkey = self.store.key(&job_rel(id))?;
            let mut then = vec![Op::Put {
                key: jkey.clone(),
                value: Record::new(next.clone()).encode()?,
                lease: None,
            }];
            then.extend(self.store.unlock_ops(&next)?);
            let r = self
                .store
                .dc
                .txn(Txn {
                    when: vec![
                        Cmp::ModRev(jkey, rev),
                        Cmp::Missing(self.store.key(&owner_rel(id))?),
                    ],
                    then,
                    otherwise: Vec::new(),
                })
                .await?;
            if r.succeeded {
                return Ok(());
            }
        }
        let reason = reason.to_owned();
        self.edit(id, |r| {
            if r.state.is_committed() {
                return Err(SprigError::new(
                    SprigCode::JobStateInvalid,
                    format!("job {} committed its cutover", r.job_id.0),
                ));
            }
            if r.cancel.is_some() || r.state.is_terminal() {
                return Ok(false);
            }
            r.cancel = Some(reason.clone());
            Ok(true)
        })
        .await
    }

    async fn list(&self, filter: &JobFilter) -> Result<Vec<JobStatus>> {
        let mut out: Vec<JobStatus> = self
            .store
            .all()
            .await?
            .into_iter()
            .map(|(r, _)| r.status())
            .filter(|s| filter.tenant.is_none_or(|t| t == s.tenant))
            .filter(|s| !filter.active_only || !s.state.is_terminal())
            .collect();
        out.sort_by(|a, b| b.id.cmp(&a.id));
        Ok(out)
    }

    async fn cut_over(&self, id: &JobId) -> Result<()> {
        self.edit(id, |r| {
            if r.spec.cutover != CutoverMode::Manual {
                return Err(SprigError::new(
                    SprigCode::JobStateInvalid,
                    format!("job {} cuts over by itself", r.job_id.0),
                ));
            }
            if r.go {
                return Ok(false);
            }
            r.go = true;
            Ok(true)
        })
        .await
    }
}

/// How long a worker's lease is, from settings.
#[must_use]
pub fn lease_ttl(job_lease: Duration) -> Duration {
    job_lease.max(Duration::from_secs(2))
}

#[cfg(test)]
mod tests;
