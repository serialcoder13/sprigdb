//! Workload capture in the gateway: the real [`WorkloadHook`].
//!
//! [`GatewayWorkload`] is built once in [`crate::run`] with the recorder,
//! the fingerprinter, the sample outbox and the keyring, and its flusher
//! runs beside it. Every session the hook wraps gets a [`SessionCapture`]
//! ([`WorkloadHook::capture`]), which the executor uses per statement:
//!
//! ```text
//! tenant statement (logical text):
//!   fp      = fingerprinter.sql(tenant, text, ctx, catalog resolver)
//!   pending = recorder.plan(tenant, fp)          probe? sample?
//!   [probe] backend session reads Handler_read_* before and after
//!   statement runs, result relayed
//!   capture.finish: recorder.record(tenant, fp, summary)
//!                   transaction footprint (TxnTracker)
//!                   [sample] seal literals + context, offer to the reservoir
//! ```
//!
//! The request-level `begin`/`end` of the hook stay as cheap as
//! [`crate::pipeline::NoWorkload`]'s: everything a statement needs happens
//! where the executor knows the statement (its logical text, the tenant's
//! catalog, its backend connection and rows).
//!
//! Only tenant sessions are captured: accounts-file logins have no
//! tenant catalog to write to. Statements the gateway answers itself (DDL
//! through the catalog, settings, local answers) are not fingerprinted
//! yet *(choice: they never reach MariaDB, so they say nothing about
//! indexes)*. Statements of a client multi-statement packet are counted
//! but never probed.
//!
//! **Gateway id** *(choice)*: `workload_stats.gateway_id` is
//! `xxh3_64(node id ‖ process start time)` cut to 32 bits, so a restarted
//! gateway writes new rows instead of replacing its predecessor's counts
//! for the same bucket; the rollup merges them.

use std::sync::Arc;
use std::time::{Duration, Instant, UNIX_EPOCH};

use ring::rand::SystemRandom;
use sprig_catalog::{Namespace, TenantCatalog};
use sprig_common::ids::{ConnId, DbId, GroupId, ObjectId, TenantId};
use sprig_common::time::Clock;
use sprig_sql::names::{NameMap, Purpose};
use sprig_workload::fingerprint::{Fingerprint, SqlCtx, SqlFingerprinter};
use sprig_workload::keyring::TenantKeyring;
use sprig_workload::recorder::{ExecSummary, ProbePlan, Recorder};
use sprig_workload::samples::{
    CaptureMeta, SampleContext, SampleOutbox, SamplePayload, cap_lists, seal,
};
use sprig_workload::shape::{PhysicalTable, ResolvedTable, TableResolver};
use sprig_workload::txn::{GroupRef, StatementWrites, TxnTracker};
use sprig_workload::window::{SampleValue, SampledLiterals};

use crate::pipeline::{Request, WorkloadHook, WorkloadOutcome, WorkloadToken};
use crate::session::Session;

tokio::task_local! {
    /// The workload caller of statements run in this task: `caller =
    /// studio` while the HTTP engine runs a Studio session's statement
    /// (Studio's runs are kept apart and count
    /// toward automatic indexing only with `auto_index.include_studio`).
    pub static CALLER: Option<sprig_workload::fingerprint::Caller>;
}

/// A front end's own request (a MongoDB command, `Fingerprinter::mongo`)
/// while its statements run in this task: whether they are probed
/// whatever their own fingerprints' plans say, and the rows examined their
/// probes counted. [`SessionCapture::start`] and
/// [`SessionCapture::finish`] read and add to it.
#[derive(Debug, Default)]
pub struct FrontRun {
    /// Probe every statement of the request (its fingerprint is watched).
    pub probe: bool,
    /// Rows examined, summed over the probed statements.
    pub examined: std::sync::atomic::AtomicU64,
    /// Statements that were probed.
    pub probed: std::sync::atomic::AtomicU32,
}

tokio::task_local! {
    /// The front-end request whose statements run in this task.
    pub static FRONT_RUN: Arc<FrontRun>;
}

/// What a front end that fingerprints its own requests gets from its SQL
/// session ([`crate::pipeline::SessionExec::workload`]).
#[derive(Debug, Clone)]
pub struct FrontCapture {
    /// The gateway's capture.
    pub workload: GatewayWorkload,
    /// The tenant captures parameter samples (`workload.capture_samples`).
    pub samples: bool,
    /// The request's table in the tenant catalog, when it resolves.
    pub table: Option<ResolvedTable>,
}

/// The workload caller of the running task ([`CALLER`]), if one is set.
#[must_use]
pub fn task_caller() -> Option<sprig_workload::fingerprint::Caller> {
    CALLER.try_with(|c| *c).ok().flatten()
}

/// What the capture path reads besides the recorder's settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CaptureSettings {
    /// `workload.max_text_bytes` (4096).
    pub max_text_bytes: usize,
    /// `workload.max_in_list_values` (1000).
    pub max_in_list_values: usize,
    /// `workload.sample_retention_days` (7).
    pub sample_retention_days: u32,
}

impl Default for CaptureSettings {
    fn default() -> Self {
        Self {
            max_text_bytes: 4096,
            max_in_list_values: 1000,
            sample_retention_days: 7,
        }
    }
}

struct Inner {
    recorder: Arc<Recorder>,
    fingerprinter: SqlFingerprinter,
    outbox: Arc<SampleOutbox>,
    keyring: Arc<dyn TenantKeyring>,
    rng: SystemRandom,
    gateway_id: u32,
    clock: Arc<dyn Clock>,
    /// Live: replaced when a `workload.*` key changes.
    settings: arc_swap::ArcSwap<CaptureSettings>,
    /// Weights of writes inside compound statements not yet counted as a
    /// run, by tenant and fingerprint.
    compound_weights: std::sync::Mutex<std::collections::HashMap<(TenantId, u64), f64>>,
}

/// Inner-write weights kept at most (the map is emptied above it).
const MAX_INNER_WEIGHTS: usize = 100_000;

/// The gateway's workload capture (module docs). Cheap to clone.
#[derive(Clone)]
pub struct GatewayWorkload(Arc<Inner>);

impl std::fmt::Debug for GatewayWorkload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayWorkload")
            .field("gateway_id", &self.0.gateway_id)
            .finish_non_exhaustive()
    }
}

/// The gateway id of this process (module docs).
#[must_use]
pub fn gateway_id(node_id: &str, started: std::time::SystemTime) -> u32 {
    use std::hash::Hasher;
    let mut h = twox_hash::XxHash3_64::with_seed(0);
    h.write(node_id.as_bytes());
    h.write(
        &started
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
            .to_le_bytes(),
    );
    // Zero is the rollup's merged row.
    u32::try_from(h.finish() & 0xffff_ffff).unwrap_or(1).max(1)
}

impl GatewayWorkload {
    /// The capture of one gateway.
    #[must_use]
    pub fn new(
        recorder: Arc<Recorder>,
        outbox: Arc<SampleOutbox>,
        keyring: Arc<dyn TenantKeyring>,
        gateway_id: u32,
        clock: Arc<dyn Clock>,
        settings: CaptureSettings,
    ) -> Self {
        Self(Arc::new(Inner {
            recorder,
            fingerprinter: SqlFingerprinter::default(),
            outbox,
            keyring,
            rng: SystemRandom::new(),
            gateway_id,
            clock,
            settings: arc_swap::ArcSwap::from_pointee(settings),
            compound_weights: std::sync::Mutex::new(std::collections::HashMap::new()),
        }))
    }

    /// The recorder.
    #[must_use]
    pub fn recorder(&self) -> &Arc<Recorder> {
        &self.0.recorder
    }

    /// The fingerprinter (its cache is per gateway).
    #[must_use]
    pub fn fingerprinter(&self) -> &SqlFingerprinter {
        &self.0.fingerprinter
    }

    /// `workload.max_text_bytes`.
    #[must_use]
    pub fn max_text_bytes(&self) -> usize {
        self.0.settings.load().max_text_bytes
    }

    /// The capture settings now.
    #[must_use]
    pub fn capture_settings(&self) -> CaptureSettings {
        **self.0.settings.load()
    }

    /// Replaces the capture settings and the recorder's and sample
    /// outbox's (the `workload.*` keys apply live).
    pub fn reconfigure(
        &self,
        recorder: sprig_workload::recorder::RecorderConfig,
        capture: CaptureSettings,
    ) {
        self.0.outbox.set_per_bucket(recorder.samples_per_bucket);
        self.0.recorder.set_config(recorder);
        if **self.0.settings.load() != capture {
            self.0.settings.store(Arc::new(capture));
        }
    }

    /// This process's gateway id.
    #[must_use]
    pub fn gateway_id(&self) -> u32 {
        self.0.gateway_id
    }

    /// A front end's own request is about to run (`Fingerprinter::mongo`):
    /// the recorder's probe and sample plan for its fingerprint.
    #[must_use]
    pub fn front_plan(&self, tenant: TenantId, fp: &Fingerprint) -> ProbePlan {
        self.0.recorder.plan(tenant, fp)
    }

    /// Records a front end's own request (`Fingerprinter::mongo`) and,
    /// when `sample` is set, seals its values as a parameter sample, as
    /// [`SessionCapture::finish`] does for SQL.
    pub fn front_record(
        &self,
        tenant: TenantId,
        fp: &Fingerprint,
        ex: &ExecSummary,
        sample: Option<SamplePayload>,
    ) {
        let w = &self.0;
        w.recorder.record(tenant, fp, ex);
        let Some(payload) = sample.filter(|_| !ex.error && !fp.sensitive) else {
            return;
        };
        let wall = w.clock.wall();
        let meta = CaptureMeta {
            captured_at: wall,
            latency_us: ex.latency_us,
            rows_examined: ex.examined,
            rows_returned: ex.rows_returned.max(ex.rows_affected),
            expires_at: wall
                + Duration::from_secs(u64::from(w.settings.load().sample_retention_days) * 86_400),
        };
        match seal(&*w.keyring, &w.rng, tenant, fp.id, &payload, meta) {
            Ok(sealed) => {
                let secs = wall.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
                let bucket = u64::from(w.recorder.config().bucket_minutes.max(1)) * 60;
                if w.outbox.offer(tenant, secs - secs % bucket, sealed) {
                    metrics::counter!("sprig_workload_samples_kept_total").increment(1);
                }
            }
            Err(e) => {
                metrics::counter!("sprig_workload_samples_skipped_total").increment(1);
                tracing::trace!(error = %e, "workload sample not sealed");
            }
        }
    }

    /// `xxh3_64(gateway id, connection id)`: the session of a statement,
    /// for `scan_sessions`.
    #[must_use]
    pub fn session_hash(&self, conn: ConnId) -> u64 {
        use std::hash::Hasher;
        let mut h = twox_hash::XxHash3_64::with_seed(0);
        h.write(&self.0.gateway_id.to_le_bytes());
        h.write(&conn.0.to_le_bytes());
        h.finish()
    }
}

impl WorkloadHook for GatewayWorkload {
    fn begin(&self, _session: &Session, req: &Request) -> WorkloadToken {
        WorkloadToken {
            started: Instant::now(),
            request: req.name(),
        }
    }

    fn end(&self, _token: WorkloadToken, _outcome: &WorkloadOutcome<'_>) {}

    fn capture(&self) -> Option<SessionCapture> {
        Some(SessionCapture {
            wl: self.clone(),
            txn: TxnTracker::new(),
        })
    }
}

/// One statement between [`SessionCapture::start`] and
/// [`SessionCapture::finish`].
#[derive(Debug)]
pub struct Pending {
    fp: Fingerprint,
    plan: ProbePlan,
    started: Instant,
}

impl Pending {
    /// Probe the statement's rows examined.
    #[must_use]
    pub fn probe(&self) -> bool {
        self.plan.examined
    }

    /// Capture its parameters.
    #[must_use]
    pub fn wants_sample(&self) -> bool {
        self.plan.capture_params
    }

    /// Its fingerprint.
    #[must_use]
    pub fn fingerprint(&self) -> &Fingerprint {
        &self.fp
    }
}

/// A sample's inputs: the text the fingerprint's literal spans refer to,
/// the bound values of its placeholders and the session context.
#[derive(Debug)]
pub struct SampleInput<'a> {
    /// The statement text (logical names).
    pub text: &'a [u8],
    /// `COM_STMT_EXECUTE` values, rendered as literals; empty for text.
    pub params: &'a [SampleValue],
    /// The session context.
    pub context: SampleContext,
}

/// How a statement ended, for [`SessionCapture::finish`].
#[derive(Debug, Default)]
pub struct Done<'a> {
    /// It ended in an error.
    pub error: bool,
    /// Rows sent.
    pub rows_returned: u64,
    /// Rows changed.
    pub rows_affected: u64,
    /// Rows examined, when a probe ran.
    pub examined: Option<u64>,
    /// The session is in a transaction after it.
    pub in_transaction: bool,
    /// The table groups it wrote.
    pub groups: &'a [GroupRef],
    /// The tables it wrote.
    pub tables: &'a [ObjectId],
    /// Its sample, when [`Pending::wants_sample`] and the tenant captures
    /// samples.
    pub sample: Option<SampleInput<'a>>,
}

/// One session's capture: the gateway's capture plus the session's open
/// transaction.
#[derive(Debug)]
pub struct SessionCapture {
    wl: GatewayWorkload,
    txn: TxnTracker,
}

fn is_rollback(fp: &Fingerprint) -> bool {
    let t = fp.text.trim_start();
    t.starts_with("ROLLBACK") && !t.starts_with("ROLLBACK TO")
}

impl SessionCapture {
    /// The gateway's capture.
    #[must_use]
    pub fn workload(&self) -> &GatewayWorkload {
        &self.wl
    }

    /// Fingerprints a SQL statement of `tenant` (logical text).
    #[must_use]
    pub fn fingerprint(
        &self,
        tenant: TenantId,
        sql: &[u8],
        ctx: &SqlCtx<'_>,
        resolver: &dyn TableResolver,
    ) -> Fingerprint {
        self.wl.0.fingerprinter.sql(tenant, sql, ctx, resolver)
    }

    /// A statement is about to run: asks the recorder for a probe plan.
    /// `multi`: it came in a client multi-statement packet (never probed).
    #[must_use]
    pub fn start(&self, tenant: TenantId, fp: Fingerprint, multi: bool) -> Pending {
        let mut plan = self.wl.0.recorder.plan(tenant, &fp);
        if FRONT_RUN.try_with(|r| r.probe).unwrap_or(false) {
            plan.examined = true;
        }
        if multi {
            plan.examined = false;
        }
        Pending {
            fp,
            plan,
            started: Instant::now(),
        }
    }

    /// The statement ended: records it, follows the transaction, and seals
    /// its sample.
    pub fn finish(&mut self, tenant: TenantId, conn: ConnId, p: &Pending, done: Done<'_>) {
        let w = &self.wl.0;
        let latency = p.started.elapsed();
        let latency_us = u64::try_from(latency.as_micros()).unwrap_or(u64::MAX);
        let session = self.wl.session_hash(conn);
        let ex = ExecSummary {
            latency_us,
            error: done.error,
            rows_returned: done.rows_returned,
            rows_affected: done.rows_affected,
            shards: 1,
            clusters: 1,
            scatter: false,
            in_list_max: 0,
            examined: done.examined,
            session,
        };
        w.recorder.record(tenant, &p.fp, &ex);
        if let Some(x) = done.examined {
            let _ = FRONT_RUN.try_with(|r| {
                r.examined
                    .fetch_add(x, std::sync::atomic::Ordering::Relaxed);
                r.probed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            });
        }

        // Transaction footprints, from the
        // backend's transaction state after each statement.
        let now = Instant::now();
        let was_open = self.txn.is_open();
        if !was_open && done.in_transaction {
            self.txn.begin();
        }
        if self.txn.is_open() && !done.error {
            self.txn.statement(
                &p.fp,
                now,
                &StatementWrites {
                    groups: done.groups,
                    tables: done.tables,
                    shards: 1,
                    clusters: &[],
                    rows_affected: done.rows_affected,
                },
            );
        }
        if was_open && !done.in_transaction {
            if is_rollback(&p.fp) {
                self.txn.rollback();
            } else if let Some(f) = self.txn.commit(now)
                && let Some(tfp) = f.fingerprint(p.fp.front_end)
            {
                w.recorder.record(tenant, &tfp, &f.exec_summary(session));
            }
        }

        if let Some(s) = done.sample.filter(|_| p.plan.capture_params && !done.error) {
            let mut literals = SampledLiterals::from_execution(s.text, &p.fp, s.params);
            cap_lists(&mut literals, w.settings.load().max_in_list_values);
            let payload = SamplePayload {
                literals,
                context: s.context,
            };
            let wall = w.clock.wall();
            let meta = CaptureMeta {
                captured_at: wall,
                latency_us,
                rows_examined: done.examined,
                rows_returned: done.rows_returned.max(done.rows_affected),
                expires_at: wall
                    + Duration::from_secs(
                        u64::from(w.settings.load().sample_retention_days) * 86_400,
                    ),
            };
            match seal(&*w.keyring, &w.rng, tenant, p.fp.id, &payload, meta) {
                Ok(sealed) => {
                    let secs = wall.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
                    let bucket = u64::from(w.recorder.config().bucket_minutes.max(1)) * 60;
                    if w.outbox.offer(tenant, secs - secs % bucket, sealed) {
                        metrics::counter!("sprig_workload_samples_kept_total").increment(1);
                    }
                }
                Err(e) => {
                    // No key: samples aren't captured; statistics go on.
                    metrics::counter!("sprig_workload_samples_skipped_total").increment(1);
                    tracing::trace!(error = %e, "workload sample not sealed");
                }
            }
        }
    }

    /// One write statement inside a compound statement that ran (such as a
    /// Redis guarded batch), recorded as if
    /// it had been sent alone: a run each time its `weight`s (the share of
    /// the compound's runs it is expected to run in,
    /// `sprig_workload::fingerprint::compound`) add up to a whole one; one
    /// row, no time of its own (the compound has the time), no sample and
    /// no transaction footprint. What it adds is the write load the
    /// automatic indexing cost model reads.
    pub fn record_inner(&self, tenant: TenantId, conn: ConnId, fp: &Fingerprint, weight: f64) {
        let runs = {
            let mut m = self
                .wl
                .0
                .compound_weights
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if m.len() > MAX_INNER_WEIGHTS {
                m.clear();
            }
            let acc = m.entry((tenant, fp.id.0)).or_insert(0.0);
            *acc += weight.clamp(0.0, 1.0);
            let whole = acc.floor();
            *acc -= whole;
            whole
        };
        if runs < 1.0 {
            return;
        }
        let ex = ExecSummary {
            latency_us: 0,
            error: false,
            rows_returned: 0,
            rows_affected: 1,
            shards: 1,
            clusters: 1,
            scatter: false,
            in_list_max: 0,
            examined: None,
            session: self.wl.session_hash(conn),
        };
        self.wl.0.recorder.record(tenant, fp, &ex);
    }

    /// The session ended or reset: an open transaction records nothing.
    pub fn reset(&mut self) {
        self.txn.rollback();
    }
}

/// [`TableResolver`] over a tenant's catalog snapshot and name map.
pub struct CatalogResolver<'a> {
    cat: Option<&'a TenantCatalog>,
    map: &'a dyn NameMap,
    lower: bool,
}

impl std::fmt::Debug for CatalogResolver<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogResolver").finish_non_exhaustive()
    }
}

impl<'a> CatalogResolver<'a> {
    /// A resolver over `cat` (none: nothing resolves) and `map`;
    /// `lower`: `names.lower_case_table_names = 1`.
    #[must_use]
    pub fn new(cat: Option<&'a TenantCatalog>, map: &'a dyn NameMap, lower: bool) -> Self {
        Self { cat, map, lower }
    }

    fn key(&self, s: &str) -> String {
        if self.lower {
            s.to_lowercase()
        } else {
            s.to_owned()
        }
    }

    /// The table group of a database: its default group.
    #[must_use]
    pub fn group_of(&self, db: DbId) -> Option<GroupId> {
        self.cat?.databases.get(db).map(|d| d.default_group_id)
    }
}

impl TableResolver for CatalogResolver<'_> {
    fn resolve(&self, db: Option<&str>, table: &str) -> Option<ResolvedTable> {
        let cat = self.cat?;
        let db = db?;
        let row = cat.database(&self.key(db))?;
        let obj = cat.object(row.db_id, Namespace::Relation, &self.key(table))?;
        let physical = self
            .map
            .physical(&self.key(db), Purpose::Session)
            .ok()
            .and_then(|s| s.single())
            .map(|schema| PhysicalTable {
                schema: schema.to_string(),
                table: table.to_owned(),
            });
        Some(ResolvedTable {
            object_id: obj.object_id,
            db_id: row.db_id,
            physical,
        })
    }

    fn database(&self, db: &str, _table: &str) -> Option<DbId> {
        Some(self.cat?.database(&self.key(db))?.db_id)
    }
}

/// The groups and tables a statement writes, by its shape (each
/// database's default group).
#[must_use]
pub fn writes_of(fp: &Fingerprint, r: &CatalogResolver<'_>) -> (Vec<GroupRef>, Vec<ObjectId>) {
    let Some(w) = &fp.shape.writes else {
        return (Vec::new(), Vec::new());
    };
    let mut groups = Vec::new();
    let mut tables = Vec::new();
    for t in w.targets.iter().filter_map(|a| fp.shape.table(a)) {
        if let Some(o) = t.object_id {
            tables.push(o);
        }
        if let Some(db) = t.db_id
            && let Some(group) = r.group_of(db)
        {
            let g = GroupRef { db, group };
            if !groups.contains(&g) {
                groups.push(g);
            }
        }
    }
    (groups, tables)
}

/// A bound parameter as a sample value: its SQL literal text, escaped for
/// the session's `sql_mode`. With `NO_BACKSLASH_ESCAPES` a backslash is an
/// ordinary character, so it is written once (doubling it would change the
/// value when the sample is replayed under the same `sql_mode`); a NUL byte
/// can't be written inside quotes then, so such a string is sent as hex.
#[must_use]
pub fn sample_value(v: &sprig_proto_mysql::Value, no_backslash_escapes: bool) -> SampleValue {
    use sprig_proto_mysql::Value as V;
    use sprig_workload::fingerprint::LiteralKind as K;
    let (kind, text) = match v {
        V::Null => (K::Param, b"NULL".to_vec()),
        V::Int(i) => (K::Number, i.to_string().into_bytes()),
        V::UInt(u) => (K::Number, u.to_string().into_bytes()),
        V::Float(f) => (K::Number, f.to_string().into_bytes()),
        V::Double(d) => (K::Number, d.to_string().into_bytes()),
        V::Bytes(b) if no_backslash_escapes && b.contains(&0) => {
            let mut t = Vec::with_capacity(2 * b.len() + 3);
            t.extend_from_slice(b"X'");
            for &c in b {
                t.extend_from_slice(format!("{c:02X}").as_bytes());
            }
            t.push(b'\'');
            (K::String, t)
        }
        V::Bytes(b) => {
            let mut t = Vec::with_capacity(b.len() + 2);
            t.push(b'\'');
            for &c in b {
                match c {
                    b'\'' => t.extend_from_slice(b"''"),
                    b'\\' if !no_backslash_escapes => t.extend_from_slice(b"\\\\"),
                    0 => t.extend_from_slice(b"\\0"),
                    _ => t.push(c),
                }
            }
            t.push(b'\'');
            (K::String, t)
        }
        V::DateTime(d) => (
            K::String,
            format!(
                "'{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}'",
                d.year, d.month, d.day, d.hour, d.minute, d.second, d.micro
            )
            .into_bytes(),
        ),
        V::Time(t) => {
            let hours = u64::from(t.days) * 24 + u64::from(t.hour);
            (
                K::String,
                format!(
                    "'{}{hours:02}:{:02}:{:02}.{:06}'",
                    if t.negative { "-" } else { "" },
                    t.minute,
                    t.second,
                    t.micro
                )
                .into_bytes(),
            )
        }
    };
    SampleValue { kind, text }
}

/// What [`start`] returns: the hook for the front ends, and the flusher's
/// task (it flushes once more at shutdown).
pub type Started = (Arc<dyn WorkloadHook>, Option<tokio::task::JoinHandle<()>>);

/// Builds the gateway's workload capture from the process
/// configuration and the live settings, and starts its flusher. Tenant
/// sessions only: without tenant services (accounts-file logins), or with
/// `[gateway] workload_capture_off`, the hook records nothing.
///
/// # Errors
/// 39998 for a sample key file that can't be read.
pub fn start(
    config: &sprig_common::config::ProcessConfig,
    live: &sprig_settings::LiveSettings,
    tenants: Option<&Arc<crate::tenant::TenantServices>>,
    clock: Arc<dyn Clock>,
    shutdown: sprig_common::CancellationToken,
) -> sprig_common::Result<Started> {
    use sprig_settings::SettingCtx;
    use sprig_workload::settings as ws;
    let off: Arc<dyn WorkloadHook> = Arc::new(crate::pipeline::NoWorkload);
    if config.gateway.workload_capture_off {
        tracing::info!("workload capture is off ([gateway] workload_capture_off)");
        return Ok((off, None));
    }
    let Some(tenants) = tenants else {
        return Ok((off, None));
    };
    let keyring: Arc<dyn TenantKeyring> = if let Some(p) = &config.gateway.workload_key_file {
        let k = sprig_workload::keyring::MasterKeyring::from_file(p)?;
        tracing::info!(versions = ?k.versions(), "workload sample keys loaded");
        Arc::new(k)
    } else {
        tracing::warn!(
            "no [gateway] workload_key_file: parameter samples are not captured (statistics are)"
        );
        Arc::new(sprig_workload::keyring::NoKeys)
    };
    let dc = config.node.dc.clone();
    let (cfg, capture) = settings_now(live, &dc);
    let started = clock.wall();
    let gw = gateway_id(&config.node.id(), started);
    let salt = u64::from(gw) << 32
        ^ started
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
    let samples_per_bucket = cfg.samples_per_bucket;
    let recorder = Arc::new(Recorder::new(cfg, Arc::clone(&clock)));
    let outbox = Arc::new(SampleOutbox::new(samples_per_bucket, salt));
    let wl = GatewayWorkload::new(
        Arc::clone(&recorder),
        Arc::clone(&outbox),
        keyring,
        gw,
        Arc::clone(&clock),
        capture,
    );
    let locator: Arc<dyn sprig_catalog::CatalogLocator> = tenants.locator.clone();
    let target = Arc::new(sprig_workload::flush::CatalogTarget::new(
        locator,
        format!("gateway:{}", config.node.id()),
    ));
    let flive = live.clone();
    let fdc = dc.clone();
    let settings = Arc::new(move || sprig_workload::flush::FlushSettings {
        flush_seconds: flive.get(&ws::FLUSH_SECONDS, &SettingCtx::process(&fdc)),
        rollup_delay_minutes: flive.get(&ws::ROLLUP_DELAY_MINUTES, &SettingCtx::process(&fdc)),
    });
    let flusher = Arc::new(sprig_workload::flush::Flusher::new(
        recorder, outbox, target, gw, clock, settings,
    ));
    tracing::info!(gateway_id = gw, "workload capture on");
    // The recorder's keys apply live: a change to any `workload.*` key
    // rebuilds the recorder's and the capture's settings.
    let mut changes = live.subscribe(&["workload.*"]);
    let (wlive, wl2, wstop) = (live.clone(), wl.clone(), shutdown.clone());
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = wstop.cancelled() => return,
                r = changes.changed() => {
                    if r.is_err() {
                        return;
                    }
                    let (cfg, capture) = settings_now(&wlive, &dc);
                    wl2.reconfigure(cfg, capture);
                    tracing::info!("workload capture settings changed");
                }
            }
        }
    });
    let task = tokio::spawn(flusher.run(shutdown));
    Ok((Arc::new(wl), Some(task)))
}

/// The recorder's and the capture's settings at this DC's process scopes
/// now.
fn settings_now(
    live: &sprig_settings::LiveSettings,
    dc: &sprig_common::ids::DcId,
) -> (sprig_workload::recorder::RecorderConfig, CaptureSettings) {
    use sprig_settings::SettingCtx;
    use sprig_workload::settings as ws;
    let snap = live.snapshot();
    let ctx = SettingCtx::process(dc);
    let get_u32 = |s: &sprig_settings::Setting<u32>| snap.get(s, &ctx);
    let cfg = ws::recorder_config(get_u32, |s| snap.get(s, &ctx), |s| snap.get(s, &ctx));
    let capture = CaptureSettings {
        max_text_bytes: usize::try_from(get_u32(&ws::MAX_TEXT_BYTES)).unwrap_or(4096),
        max_in_list_values: usize::try_from(get_u32(&ws::MAX_IN_LIST_VALUES)).unwrap_or(1000),
        sample_retention_days: get_u32(&ws::SAMPLE_RETENTION_DAYS),
    };
    (cfg, capture)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_proto_mysql::Value;

    #[test]
    fn bound_values_render_as_literals() {
        let t = |v: Value| String::from_utf8(sample_value(&v, false).text).unwrap();
        assert_eq!(t(Value::Int(-5)), "-5");
        assert_eq!(t(Value::Null), "NULL");
        assert_eq!(
            t(Value::Bytes(bytes::Bytes::from_static(b"it's \\ x"))),
            "'it''s \\\\ x'"
        );
        assert_eq!(
            sample_value(&Value::Bytes(bytes::Bytes::from_static(b"x")), false)
                .string_value(false)
                .unwrap(),
            b"x".to_vec()
        );
        // NO_BACKSLASH_ESCAPES: the backslash is written once and reads back
        // as one; a NUL goes as hex.
        let nbe = sample_value(&Value::Bytes(bytes::Bytes::from_static(b"a\\b'c")), true);
        assert_eq!(String::from_utf8(nbe.text.clone()).unwrap(), "'a\\b''c'");
        assert_eq!(nbe.string_value(true).unwrap(), b"a\\b'c".to_vec());
        let nul = sample_value(&Value::Bytes(bytes::Bytes::from_static(b"a\0b")), true);
        assert_eq!(String::from_utf8(nul.text).unwrap(), "X'610062'");
    }

    #[test]
    fn gateway_ids_differ_per_process_and_are_never_zero() {
        let t0 = UNIX_EPOCH + Duration::from_secs(1);
        let t1 = UNIX_EPOCH + Duration::from_secs(2);
        assert_ne!(gateway_id("gw-1", t0), gateway_id("gw-1", t1));
        assert_ne!(gateway_id("gw-1", t0), 0);
    }
}
