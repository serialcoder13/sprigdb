//! Workload capture in the gateway (chapter 12; chapter 04, "Dispatch into
//! the layers"; milestone M0.4): the real [`WorkloadHook`].
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
//! Only tenant sessions are captured: accounts-file logins (M0.2) have no
//! tenant catalog to write to. Statements the gateway answers itself (DDL
//! through the catalog, settings, local answers) are not fingerprinted
//! yet *(choice: they never reach MariaDB, so they say nothing about
//! indexes)*. Statements of a client multi-statement packet are counted
//! but never probed (chapter 12).
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
    settings: CaptureSettings,
}

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
            settings,
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
        self.0.settings.max_text_bytes
    }

    /// This process's gateway id.
    #[must_use]
    pub fn gateway_id(&self) -> u32 {
        self.0.gateway_id
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

        // Transaction footprints (chapter 12, build step 7), from the
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
            cap_lists(&mut literals, w.settings.max_in_list_values);
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
                    + Duration::from_secs(u64::from(w.settings.sample_retention_days) * 86_400),
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

    /// The table group of a database (R0: its default group).
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
}

/// The groups and tables a statement writes, by its shape (R0: each
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

/// Builds the gateway's workload capture (chapter 12) from the process
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
    let get_u32 = |s: &sprig_settings::Setting<u32>| live.get(s, &SettingCtx::process(&dc));
    let cfg = ws::recorder_config(
        get_u32,
        |s| live.get(s, &SettingCtx::process(&dc)),
        |s| live.get(s, &SettingCtx::process(&dc)),
    );
    let capture = CaptureSettings {
        max_text_bytes: usize::try_from(get_u32(&ws::MAX_TEXT_BYTES)).unwrap_or(4096),
        max_in_list_values: usize::try_from(get_u32(&ws::MAX_IN_LIST_VALUES)).unwrap_or(1000),
        sample_retention_days: get_u32(&ws::SAMPLE_RETENTION_DAYS),
    };
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
    let task = tokio::spawn(flusher.run(shutdown));
    Ok((Arc::new(wl), Some(task)))
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
