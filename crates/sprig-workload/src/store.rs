//! The workload tables of the tenant catalog (chapter 12, "Data structures
//! and storage"; Appendix C; build step 4), as the `workload` migration
//! module, with the writes the flusher makes, the rollup and retention
//! job, and the reader chapter 13 uses.
//!
//! The module registers with `sprig-catalog`'s migrator
//! ([`WORKLOAD_MODULE`]); tenant bootstrap migrates it with the core
//! catalog, and the flusher and the rollup migrate it once per tenant
//! before their first write (idempotent), for tenants bootstrapped before
//! it existed. Like `autoindex`, the tables are read on demand and never
//! bump a catalog version: writes are plain statements.
//!
//! **Times.** `bucket_start` holds UTC seconds aligned to the bucket and
//! travels as seconds since the epoch; the `DATETIME(6)` columns travel as
//! microseconds (`TIMESTAMPADD` and `TIMESTAMPDIFF` against
//! `'1970-01-01'`), so the session time zone never matters.
//!
//! **Rollup** ([`WorkloadTables::rollup`]), `workload.rollup_delay_minutes`
//! after a bucket closes: each fingerprint's rows of that bucket (every
//! gateway and part) become one row with `gateway_id = 0, part = 0`;
//! counters summed, maxima kept, histograms and session sets merged; the
//! merge and the deletes are one transaction per batch of 500, so a rerun
//! is idempotent.
//!
//! **Day rows** *(choice, beyond the chapter)*: rolled-up hours older than
//! [`DAY_AFTER_DAYS`] (7) are merged into one row per UTC day, stored with
//! `bucket_start` at midnight, `gateway_id = 0` and `part =`
//! [`DAY_PART`] (65535), so a 30-day window reads at most 7 × 24 + 23
//! rows per fingerprint. Readers sum whatever rows fall in the window; a
//! window that starts inside a day older than 7 days gets the whole day.
//!
//! **Retention** ([`WorkloadTables::retain`]): stats older than
//! `workload.retention_days`, expired samples, samples past
//! `workload.max_samples_per_fingerprint` per fingerprint (oldest first),
//! and fingerprints not seen within the retention.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row, TxOpts, Value};
use sprig_catalog::{Migration, Module, Phase};
use sprig_common::ids::{DbId, TenantId};
use sprig_common::names::{SchemaName, quote};
use sprig_common::{Result, SprigCode, SprigError};

use crate::fingerprint::{Caller, FingerprintId, FpKind, kind_of_byte};
use crate::histogram::Histogram;
use crate::recorder::{FlushRow, NewFingerprint, SessionSet, Stats};
use crate::samples::SealedSample;
use crate::shape::{SHAPE_VERSION, Shape};
use crate::window::FpWindowStats;

/// The module name in the catalog's format records.
pub const MODULE: &str = "workload";

/// `workload_fingerprints` (chapter 12, Appendix C).
pub const DDL_FINGERPRINTS: &str = "CREATE TABLE IF NOT EXISTS workload_fingerprints (
  fingerprint_id  BIGINT UNSIGNED NOT NULL,
  kind            ENUM('sql','mongo','redis','kv','vector','search','analytics','api',
                       'logic','txn') NOT NULL,
  front_end       ENUM('mysql','mariadb','mongo','redis','http','grpc','logic',
                       'internal') NOT NULL,
  caller_fn_id    BIGINT UNSIGNED NULL,
  db_id           INT UNSIGNED    NOT NULL,
  stmt_type       VARCHAR(16)     NOT NULL,
  normalized_text MEDIUMTEXT      NOT NULL,
  shape           JSON            NOT NULL,
  object_ids      JSON            NOT NULL,
  shape_version   SMALLINT UNSIGNED NOT NULL,
  flags           SET('truncated','collision','other','unparsed','static') NOT NULL
                  DEFAULT '',
  first_seen      DATETIME(6)     NOT NULL,
  last_seen       DATETIME(6)     NOT NULL,
  PRIMARY KEY (fingerprint_id),
  KEY by_last_seen (last_seen)
) ENGINE=InnoDB";

/// `workload_stats` (chapter 12, Appendix C).
pub const DDL_STATS: &str = "CREATE TABLE IF NOT EXISTS workload_stats (
  fingerprint_id   BIGINT UNSIGNED  NOT NULL,
  bucket_start     DATETIME         NOT NULL,
  gateway_id       INT UNSIGNED     NOT NULL,
  part             SMALLINT UNSIGNED NOT NULL,
  exec_count       BIGINT UNSIGNED  NOT NULL,
  error_count      BIGINT UNSIGNED  NOT NULL,
  total_us         BIGINT UNSIGNED  NOT NULL,
  max_us           BIGINT UNSIGNED  NOT NULL,
  latency_hist     VARBINARY(512)   NOT NULL,
  rows_returned    BIGINT UNSIGNED  NOT NULL,
  rows_affected    BIGINT UNSIGNED  NOT NULL,
  examined_samples BIGINT UNSIGNED  NOT NULL,
  rows_examined    BIGINT UNSIGNED  NOT NULL,
  returned_sampled BIGINT UNSIGNED  NOT NULL,
  shards_sum       BIGINT UNSIGNED  NOT NULL,
  shards_max       INT UNSIGNED     NOT NULL,
  scatter_count    BIGINT UNSIGNED  NOT NULL,
  cross_cluster    BIGINT UNSIGNED  NOT NULL,
  in_list_max      INT UNSIGNED     NOT NULL,
  scan_runs        BIGINT UNSIGNED  NOT NULL,
  scan_sessions    VARBINARY(64)    NOT NULL,
  PRIMARY KEY (fingerprint_id, bucket_start, gateway_id, part),
  KEY by_bucket (bucket_start)
) ENGINE=InnoDB";

/// `workload_samples` (chapter 12; not in Appendix C yet).
pub const DDL_SAMPLES: &str = "CREATE TABLE IF NOT EXISTS workload_samples (
  fingerprint_id BIGINT UNSIGNED NOT NULL,
  sample_id      BIGINT UNSIGNED NOT NULL,
  captured_at    DATETIME(6)     NOT NULL,
  latency_us     BIGINT UNSIGNED NOT NULL,
  rows_examined  BIGINT UNSIGNED NULL,
  rows_returned  BIGINT UNSIGNED NOT NULL,
  key_version    INT UNSIGNED    NOT NULL,
  nonce          BINARY(12)      NOT NULL,
  payload        MEDIUMBLOB      NOT NULL,
  expires_at     DATETIME        NOT NULL,
  PRIMARY KEY (fingerprint_id, sample_id),
  KEY by_expiry (expires_at)
) ENGINE=InnoDB";

/// The module's migrations.
pub static WORKLOAD_MIGRATIONS: &[Migration] = &[Migration {
    format: 1,
    phase: Phase::Expand,
    name: "workload_fingerprints, workload_stats and workload_samples",
    statements: &[DDL_FINGERPRINTS, DDL_STATS, DDL_SAMPLES],
}];

/// The module, for [`Migrator::new`](sprig_catalog::Migrator::new) and
/// chapter 08's `BootstrapHooks.modules`.
pub const WORKLOAD_MODULE: Module = Module {
    name: MODULE,
    migrations: WORKLOAD_MIGRATIONS,
};

/// Rows per multi-row statement (chapter 12: "batched 500 per statement").
pub const BATCH: usize = 500;

/// `part` of a day row (module docs).
pub const DAY_PART: u16 = u16::MAX;

/// Rolled-up hours older than this many days are merged into day rows.
pub const DAY_AFTER_DAYS: u64 = 7;

const DAY: u64 = 86_400;

/// A time parameter in microseconds since the epoch.
const AT_US: &str = "TIMESTAMPADD(MICROSECOND, ?, '1970-01-01')";
/// A time parameter in seconds since the epoch.
const AT_S: &str = "TIMESTAMPADD(SECOND, ?, '1970-01-01')";

pub(crate) fn map_err(e: mysql_async::Error) -> SprigError {
    match &e {
        mysql_async::Error::Server(s) => match s.code {
            1205 | 1213 => SprigError::new(SprigCode::CatalogConflict, "catalog row busy; retry"),
            code => {
                let mut state = *b"HY000";
                if s.state.len() == 5 {
                    state.copy_from_slice(s.state.as_bytes());
                }
                SprigError::backend(code, state, format!("statement failed ({code})"))
            }
        }
        .with_source(e),
        _ => SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e),
    }
}

fn micros(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_micros()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

fn from_micros(us: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_micros(u64::try_from(us).unwrap_or(0))
}

fn secs(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

fn take<T: mysql_async::prelude::FromValue>(row: &mut Row, idx: usize, name: &str) -> Result<T> {
    match row.take_opt::<T, usize>(idx) {
        Some(Ok(v)) => Ok(v),
        Some(Err(e)) => Err(SprigError::internal(format!(
            "workload column {name}: unexpected value: {e}"
        ))),
        None => Err(SprigError::internal(format!(
            "workload column {name} missing"
        ))),
    }
}

/// Runs one statement and returns the rows it changed.
async fn exec_count<Q, P>(q: &mut Q, sql: String, params: P) -> Result<u64>
where
    Q: Queryable + Send,
    P: Into<mysql_async::Params> + Send,
{
    let r = q.exec_iter(sql, params).await.map_err(map_err)?;
    let n = r.affected_rows();
    r.drop_result().await.map_err(map_err)?;
    Ok(n)
}

/// The statement type as `workload_fingerprints.stmt_type` stores it.
fn stmt_type(shape: &Shape) -> String {
    serde_json::to_value(shape.stmt_type)
        .ok()
        .and_then(|v| v.as_str().map(str::to_ascii_uppercase))
        .unwrap_or_else(|| "OTHER".to_owned())
}

/// One `workload_fingerprints` row, as read back.
#[derive(Debug, Clone)]
pub struct FingerprintRow {
    /// The id.
    pub id: FingerprintId,
    /// The kind.
    pub kind: FpKind,
    /// The logic function, if any.
    pub caller_fn_id: Option<u64>,
    /// The logical database part of the id.
    pub db_id: DbId,
    /// `SELECT`, `UPDATE`, ...
    pub stmt_type: String,
    /// The normalized text.
    pub text: Arc<str>,
    /// The shape.
    pub shape: Arc<Shape>,
    /// Flags (`truncated`, `collision`, ...), as stored.
    pub flags: String,
    /// First seen.
    pub first_seen: SystemTime,
    /// Last seen.
    pub last_seen: SystemTime,
}

/// What a rollup or retention run did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MaintenanceReport {
    /// (fingerprint, bucket) pairs merged into one row.
    pub merged: u64,
    /// Partial rows deleted by the merge.
    pub merged_rows: u64,
    /// (fingerprint, day) pairs merged into day rows.
    pub days: u64,
    /// Stats rows past retention deleted.
    pub expired_stats: u64,
    /// Samples deleted (expired or over the cap).
    pub expired_samples: u64,
    /// Fingerprints not seen within the retention, deleted.
    pub expired_fingerprints: u64,
}

/// The retention settings of one run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    /// `workload.bucket_minutes`.
    pub bucket_minutes: u32,
    /// `workload.rollup_delay_minutes`.
    pub rollup_delay_minutes: u32,
    /// `workload.retention_days`.
    pub retention_days: u32,
    /// `workload.max_samples_per_fingerprint`.
    pub max_samples_per_fingerprint: u32,
}

impl Default for Retention {
    fn default() -> Self {
        Self {
            bucket_minutes: 60,
            rollup_delay_minutes: 10,
            retention_days: 30,
            max_samples_per_fingerprint: 50,
        }
    }
}

/// The workload tables of one catalog schema.
#[derive(Debug, Clone)]
pub struct WorkloadTables {
    schema: String,
    fps: String,
    stats: String,
    samples: String,
}

/// The stats columns after the key, in table order.
/// `insert_fingerprints`' update of a row recorded with an older shape
/// version (decision X34). MariaDB assigns left to right, so
/// `shape_version` goes last: the conditions before it read the stored
/// version.
const REWRITE_OLD_SHAPE: &str = "\
    stmt_type = IF(shape_version < VALUES(shape_version), VALUES(stmt_type), stmt_type), \
    shape = IF(shape_version < VALUES(shape_version), VALUES(shape), shape), \
    object_ids = IF(shape_version < VALUES(shape_version), VALUES(object_ids), object_ids), \
    shape_version = GREATEST(shape_version, VALUES(shape_version))";

const STAT_COLS: &str = "exec_count, error_count, total_us, max_us, latency_hist, \
    rows_returned, rows_affected, examined_samples, rows_examined, returned_sampled, \
    shards_sum, shards_max, scatter_count, cross_cluster, in_list_max, scan_runs, scan_sessions";

fn stat_values(s: &Stats, out: &mut Vec<Value>) {
    out.extend([
        Value::UInt(s.exec_count),
        Value::UInt(s.error_count),
        Value::UInt(s.total_us),
        Value::UInt(s.max_us),
        Value::Bytes(s.latency_hist.encode()),
        Value::UInt(s.rows_returned),
        Value::UInt(s.rows_affected),
        Value::UInt(s.examined_samples),
        Value::UInt(s.rows_examined),
        Value::UInt(s.returned_sampled),
        Value::UInt(s.shards_sum),
        Value::UInt(u64::from(s.shards_max)),
        Value::UInt(s.scatter_count),
        Value::UInt(s.cross_cluster),
        Value::UInt(u64::from(s.in_list_max)),
        Value::UInt(s.scan_runs),
        Value::Bytes(s.scan_sessions.encode()),
    ]);
}

/// Reads the stats columns starting at `at`.
fn read_stats(row: &mut Row, at: usize) -> Result<Stats> {
    let hist: Vec<u8> = take(row, at + 4, "latency_hist")?;
    let sessions: Vec<u8> = take(row, at + 16, "scan_sessions")?;
    Ok(Stats {
        exec_count: take(row, at, "exec_count")?,
        error_count: take(row, at + 1, "error_count")?,
        total_us: take(row, at + 2, "total_us")?,
        max_us: take(row, at + 3, "max_us")?,
        latency_hist: Histogram::decode(&hist)
            .ok_or_else(|| SprigError::internal("workload_stats.latency_hist is malformed"))?,
        rows_returned: take(row, at + 5, "rows_returned")?,
        rows_affected: take(row, at + 6, "rows_affected")?,
        examined_samples: take(row, at + 7, "examined_samples")?,
        rows_examined: take(row, at + 8, "rows_examined")?,
        returned_sampled: take(row, at + 9, "returned_sampled")?,
        shards_sum: take(row, at + 10, "shards_sum")?,
        shards_max: take(row, at + 11, "shards_max")?,
        scatter_count: take(row, at + 12, "scatter_count")?,
        cross_cluster: take(row, at + 13, "cross_cluster")?,
        in_list_max: take(row, at + 14, "in_list_max")?,
        scan_runs: take(row, at + 15, "scan_runs")?,
        scan_sessions: SessionSet::decode(&sessions),
    })
}

impl WorkloadTables {
    /// The tables of catalog schema `schema`.
    #[must_use]
    pub fn new(schema: impl Into<String>) -> Self {
        let schema = schema.into();
        let q = quote(&schema);
        Self {
            fps: format!("{q}.workload_fingerprints"),
            stats: format!("{q}.workload_stats"),
            samples: format!("{q}.workload_samples"),
            schema,
        }
    }

    /// The tables of `tenant`'s catalog, `t<tenant>_sprig`.
    #[must_use]
    pub fn for_tenant(tenant: TenantId) -> Self {
        Self::new(SchemaName::catalog(tenant).to_string())
    }

    /// The catalog schema.
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    // --- the flusher's writes --------------------------------------------

    /// First sightings: `INSERT IGNORE INTO workload_fingerprints`, 500
    /// rows per statement. A row already there whose `shape_version` is
    /// older than [`SHAPE_VERSION`] gets this build's shape, `object_ids`
    /// and `stmt_type` (decision X34: a fingerprint first recorded before
    /// R6 round 6 gains its `JSON_PATH` predicates' `func` and `cmp`, so
    /// automatic indexing can propose field columns for it). Rows of the
    /// same or a newer version, and `first_seen`, `last_seen` and `flags`,
    /// are left alone. Returns the affected-row count: 1 per row inserted,
    /// 2 per row rewritten.
    ///
    /// # Errors
    /// The catalog's errors.
    pub async fn insert_fingerprints<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        rows: &[NewFingerprint],
    ) -> Result<u64> {
        let mut n = 0;
        for chunk in rows.chunks(BATCH) {
            let sql = format!(
                "INSERT IGNORE INTO {} (fingerprint_id, kind, front_end, caller_fn_id, db_id, \
                 stmt_type, normalized_text, shape, object_ids, shape_version, flags, first_seen, \
                 last_seen) VALUES {} ON DUPLICATE KEY UPDATE {REWRITE_OLD_SHAPE}",
                self.fps,
                vec![format!("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, {AT_US}, {AT_US})"); chunk.len()]
                    .join(", ")
            );
            let mut params = Vec::with_capacity(chunk.len() * 13);
            for r in chunk {
                let m = &r.meta;
                let objects: Vec<u64> = m.shape.object_ids().iter().map(|o| o.0).collect();
                params.extend([
                    Value::UInt(m.id.0),
                    Value::from(m.kind.as_str()),
                    Value::from(m.front_end.as_str()),
                    m.caller.map_or(Value::NULL, |c| Value::UInt(c.function_id)),
                    Value::UInt(u64::from(m.db_id.0)),
                    Value::from(stmt_type(&m.shape)),
                    Value::from(&*m.text),
                    Value::from(m.shape.to_json()),
                    Value::from(serde_json::to_string(&objects).unwrap_or_else(|_| "[]".into())),
                    Value::UInt(u64::from(SHAPE_VERSION)),
                    Value::from(m.flags.to_set()),
                    Value::Int(micros(r.first_seen)),
                    Value::Int(micros(r.first_seen)),
                ]);
            }
            n += exec_count(q, sql, params).await?;
        }
        Ok(n)
    }

    /// One gateway's cumulative values: `REPLACE` under its `(gateway_id,
    /// part)` key, 500 rows per statement, then `last_seen` of the
    /// fingerprints written. Idempotent.
    ///
    /// # Errors
    /// The catalog's errors.
    pub async fn replace_stats<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        gateway_id: u32,
        rows: &[FlushRow],
    ) -> Result<()> {
        for chunk in rows.chunks(BATCH) {
            let sql = format!(
                "REPLACE INTO {} (fingerprint_id, bucket_start, gateway_id, part, {STAT_COLS}) \
                 VALUES {}",
                self.stats,
                vec![
                    format!("(?, {AT_S}, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
                    chunk.len()
                ]
                .join(", ")
            );
            let mut params = Vec::with_capacity(chunk.len() * 21);
            for r in chunk {
                params.extend([
                    Value::UInt(r.fingerprint.0),
                    Value::UInt(r.bucket_start),
                    Value::UInt(u64::from(gateway_id)),
                    Value::UInt(u64::from(r.part)),
                ]);
                stat_values(&r.stats, &mut params);
            }
            q.exec_drop(sql, params).await.map_err(map_err)?;
            self.touch(q, chunk).await?;
        }
        Ok(())
    }

    /// `last_seen` of the fingerprints of `rows`, to the latest of them.
    async fn touch<Q: Queryable + Send>(&self, q: &mut Q, rows: &[FlushRow]) -> Result<()> {
        let mut latest: BTreeMap<u64, SystemTime> = BTreeMap::new();
        for r in rows {
            let e = latest.entry(r.fingerprint.0).or_insert(r.last_seen);
            *e = (*e).max(r.last_seen);
        }
        if latest.is_empty() {
            return Ok(());
        }
        // One statement: CASE over the ids.
        let mut sql = format!(
            "UPDATE {} SET last_seen = GREATEST(last_seen, CASE fingerprint_id",
            self.fps
        );
        let mut params = Vec::with_capacity(latest.len() * 3);
        for (id, t) in &latest {
            sql.push_str(" WHEN ? THEN ");
            sql.push_str(AT_US);
            params.push(Value::UInt(*id));
            params.push(Value::Int(micros(*t)));
        }
        sql.push_str(" ELSE last_seen END) WHERE fingerprint_id IN (");
        sql.push_str(&vec!["?"; latest.len()].join(", "));
        sql.push(')');
        params.extend(latest.keys().map(|id| Value::UInt(*id)));
        q.exec_drop(sql, params).await.map_err(map_err)
    }

    /// Sealed samples: `REPLACE` by `(fingerprint_id, sample_id)`, so a
    /// reservoir slot that changed overwrites its row.
    ///
    /// # Errors
    /// The catalog's errors.
    pub async fn replace_samples<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        rows: &[SealedSample],
    ) -> Result<()> {
        for chunk in rows.chunks(BATCH / 5) {
            let sql = format!(
                "REPLACE INTO {} (fingerprint_id, sample_id, captured_at, latency_us, \
                 rows_examined, rows_returned, key_version, nonce, payload, expires_at) VALUES {}",
                self.samples,
                vec![format!("(?, ?, {AT_US}, ?, ?, ?, ?, ?, ?, {AT_S})"); chunk.len()].join(", ")
            );
            let mut params = Vec::with_capacity(chunk.len() * 10);
            for s in chunk {
                params.extend([
                    Value::UInt(s.fingerprint.0),
                    Value::UInt(s.sample_id),
                    Value::Int(micros(s.captured_at)),
                    Value::UInt(s.latency_us),
                    s.rows_examined.map_or(Value::NULL, Value::UInt),
                    Value::UInt(s.rows_returned),
                    Value::UInt(u64::from(s.key_version)),
                    Value::Bytes(s.nonce.to_vec()),
                    Value::Bytes(s.payload.clone()),
                    Value::UInt(secs(s.expires_at)),
                ]);
            }
            q.exec_drop(sql, params).await.map_err(map_err)?;
        }
        Ok(())
    }

    // --- readers ------------------------------------------------------------

    /// The fingerprints with these ids (missing ones are left out).
    ///
    /// # Errors
    /// The catalog's errors; 39999 for a malformed row.
    pub async fn fingerprints<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        ids: &[FingerprintId],
    ) -> Result<Vec<FingerprintRow>> {
        let mut out = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(1000) {
            let sql = format!(
                "SELECT fingerprint_id, kind + 0, caller_fn_id, db_id, stmt_type, \
                 normalized_text, shape, flags, \
                 TIMESTAMPDIFF(MICROSECOND, '1970-01-01', first_seen), \
                 TIMESTAMPDIFF(MICROSECOND, '1970-01-01', last_seen) \
                 FROM {} WHERE fingerprint_id IN ({})",
                self.fps,
                vec!["?"; chunk.len()].join(", ")
            );
            let params: Vec<Value> = chunk.iter().map(|i| Value::UInt(i.0)).collect();
            let rows: Vec<Row> = q.exec(sql, params).await.map_err(map_err)?;
            for mut r in rows {
                let kind: u8 = take(&mut r, 1, "kind")?;
                let shape: String = take(&mut r, 6, "shape")?;
                let text: String = take(&mut r, 5, "normalized_text")?;
                out.push(FingerprintRow {
                    id: sprig_common::ids::Fingerprint(take(&mut r, 0, "fingerprint_id")?),
                    kind: kind_of_byte(kind)
                        .ok_or_else(|| SprigError::internal("workload_fingerprints.kind"))?,
                    caller_fn_id: take(&mut r, 2, "caller_fn_id")?,
                    db_id: DbId(take(&mut r, 3, "db_id")?),
                    stmt_type: take(&mut r, 4, "stmt_type")?,
                    text: Arc::from(text),
                    shape: Arc::new(Shape::from_json(&shape).map_err(|e| {
                        SprigError::internal(format!("workload_fingerprints.shape: {e}"))
                    })?),
                    flags: take(&mut r, 7, "flags")?,
                    first_seen: from_micros(take(&mut r, 8, "first_seen")?),
                    last_seen: from_micros(take(&mut r, 9, "last_seen")?),
                });
            }
        }
        Ok(out)
    }

    /// Every stats row of buckets starting in `[from, to)` (seconds since
    /// the epoch), merged per fingerprint.
    ///
    /// # Errors
    /// The catalog's errors; 39999 for a malformed row.
    pub async fn merged_stats<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        from: u64,
        to: u64,
    ) -> Result<BTreeMap<u64, Stats>> {
        let sql = format!(
            "SELECT fingerprint_id, {STAT_COLS} FROM {} \
             WHERE bucket_start >= {AT_S} AND bucket_start < {AT_S}",
            self.stats
        );
        let rows: Vec<Row> = q.exec(sql, (from, to)).await.map_err(map_err)?;
        let mut out: BTreeMap<u64, Stats> = BTreeMap::new();
        for mut r in rows {
            let id: u64 = take(&mut r, 0, "fingerprint_id")?;
            let s = read_stats(&mut r, 1)?;
            out.entry(id).or_default().merge(&s);
        }
        Ok(out)
    }

    /// One fingerprint's stats per bucket in `[from, to)` (seconds since the
    /// epoch), merged over gateways and parts, oldest first: Studio's
    /// latency series (chapter 44, `Workload.Series`).
    ///
    /// # Errors
    /// The catalog's errors; 39999 for a malformed row.
    pub async fn series<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        fingerprint: FingerprintId,
        from: u64,
        to: u64,
    ) -> Result<Vec<(u64, Stats)>> {
        let sql = format!(
            "SELECT TIMESTAMPDIFF(SECOND, '1970-01-01', bucket_start), {STAT_COLS} FROM {} \
             WHERE fingerprint_id = ? AND bucket_start >= {AT_S} AND bucket_start < {AT_S}",
            self.stats
        );
        let rows: Vec<Row> = q
            .exec(sql, (fingerprint.0, from, to))
            .await
            .map_err(map_err)?;
        let mut out: BTreeMap<u64, Stats> = BTreeMap::new();
        for mut r in rows {
            let at: i64 = take(&mut r, 0, "bucket_start")?;
            let s = read_stats(&mut r, 1)?;
            out.entry(u64::try_from(at).unwrap_or(0))
                .or_default()
                .merge(&s);
        }
        Ok(out.into_iter().collect())
    }

    /// Chapter 12's `WorkloadStore::window` over one catalog: every
    /// fingerprint with runs in buckets starting in `[from, to)`, its
    /// counters merged (sums, maxima, histograms and session sets), with
    /// its text and shape. Samples are not included (see
    /// [`WorkloadTables::samples`]).
    ///
    /// # Errors
    /// The catalog's errors; 39999 for a malformed row.
    pub async fn window<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        tenant: TenantId,
        from: SystemTime,
        to: SystemTime,
    ) -> Result<Vec<FpWindowStats>> {
        let merged = self.merged_stats(q, secs(from), secs(to)).await?;
        let ids: Vec<FingerprintId> = merged
            .keys()
            .map(|i| sprig_common::ids::Fingerprint(*i))
            .collect();
        let metas: HashMap<u64, FingerprintRow> = self
            .fingerprints(q, &ids)
            .await?
            .into_iter()
            .map(|f| (f.id.0, f))
            .collect();
        Ok(merged
            .into_iter()
            .filter_map(|(id, stats)| {
                // Stats of a fingerprint row deleted by retention: skipped.
                let f = metas.get(&id)?;
                Some(FpWindowStats {
                    tenant,
                    id: f.id,
                    kind: f.kind,
                    text: Arc::clone(&f.text),
                    shape: Arc::clone(&f.shape),
                    caller: f.caller_fn_id.map(|function_id| Caller {
                        function_id,
                        version: 0,
                    }),
                    stats,
                    samples: Vec::new(),
                })
            })
            .collect())
    }

    /// Up to `max` sealed samples of a fingerprint, newest first, not
    /// expired.
    ///
    /// # Errors
    /// The catalog's errors.
    pub async fn samples<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        fp: FingerprintId,
        max: usize,
    ) -> Result<Vec<SealedSample>> {
        let sql = format!(
            "SELECT sample_id, TIMESTAMPDIFF(MICROSECOND, '1970-01-01', captured_at), latency_us, \
             rows_examined, rows_returned, key_version, nonce, payload, \
             TIMESTAMPDIFF(SECOND, '1970-01-01', expires_at) \
             FROM {} WHERE fingerprint_id = ? AND expires_at > UTC_TIMESTAMP() \
             ORDER BY captured_at DESC LIMIT ?",
            self.samples
        );
        let rows: Vec<Row> = q
            .exec(sql, (fp.0, u64::try_from(max).unwrap_or(u64::MAX)))
            .await
            .map_err(map_err)?;
        let mut out = Vec::with_capacity(rows.len());
        for mut r in rows {
            let nonce: Vec<u8> = take(&mut r, 6, "nonce")?;
            let nonce: [u8; 12] = nonce
                .try_into()
                .map_err(|_| SprigError::internal("workload_samples.nonce"))?;
            let exp: i64 = take(&mut r, 8, "expires_at")?;
            out.push(SealedSample {
                fingerprint: fp,
                sample_id: take(&mut r, 0, "sample_id")?,
                captured_at: from_micros(take(&mut r, 1, "captured_at")?),
                latency_us: take(&mut r, 2, "latency_us")?,
                rows_examined: take(&mut r, 3, "rows_examined")?,
                rows_returned: take(&mut r, 4, "rows_returned")?,
                key_version: take(&mut r, 5, "key_version")?,
                nonce,
                payload: take(&mut r, 7, "payload")?,
                expires_at: UNIX_EPOCH + Duration::from_secs(u64::try_from(exp).unwrap_or(0)),
            });
        }
        Ok(out)
    }

    /// The replay harness's decrypt API: up to `max` samples of `fp`,
    /// opened with `keyring` in memory. Samples that don't open (a key
    /// version retired, a damaged row) are skipped and counted in
    /// `sprig_workload_samples_unopened_total`; their values never leave
    /// this call.
    ///
    /// # Errors
    /// The catalog's errors.
    pub async fn open_samples<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        keyring: &dyn crate::keyring::TenantKeyring,
        tenant: TenantId,
        fp: FingerprintId,
        max: usize,
    ) -> Result<Vec<crate::samples::OpenedSample>> {
        let sealed = self.samples(q, fp, max).await?;
        let mut out = Vec::with_capacity(sealed.len());
        for s in &sealed {
            match crate::samples::open(keyring, tenant, s) {
                Ok(o) => out.push(o),
                Err(e) => {
                    metrics::counter!("sprig_workload_samples_unopened_total").increment(1);
                    tracing::debug!(fingerprint = %fp, sample = s.sample_id, error = %e,
                        "a workload sample didn't open; skipped");
                }
            }
        }
        Ok(out)
    }

    // --- rollup and retention ---------------------------------------------

    /// Merges the partial rows of closed buckets (module docs), in batches
    /// of 500 (fingerprint, bucket) pairs, one transaction each. `now` is
    /// seconds since the epoch.
    ///
    /// # Errors
    /// The catalog's errors; a failed batch is rolled back and the rerun
    /// picks it up again.
    pub async fn rollup(
        &self,
        conn: &mut Conn,
        now: u64,
        r: &Retention,
    ) -> Result<MaintenanceReport> {
        let bucket = u64::from(r.bucket_minutes.max(1)) * 60;
        let delay = u64::from(r.rollup_delay_minutes) * 60;
        // A bucket starting before the cutoff has ended at least `delay` ago.
        let cutoff = now.saturating_sub(bucket + delay);
        let mut rep = MaintenanceReport::default();
        loop {
            let (pairs, deleted) = self.rollup_batch(conn, cutoff).await?;
            rep.merged += pairs;
            rep.merged_rows += deleted;
            if pairs < BATCH as u64 {
                break;
            }
        }
        let day_cutoff = now.saturating_sub(DAY_AFTER_DAYS * DAY);
        let day_cutoff = day_cutoff - day_cutoff % DAY;
        loop {
            let pairs = self.day_batch(conn, day_cutoff).await?;
            rep.days += pairs;
            if pairs < BATCH as u64 {
                break;
            }
        }
        Ok(rep)
    }

    async fn rollup_batch(&self, conn: &mut Conn, cutoff: u64) -> Result<(u64, u64)> {
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .await
            .map_err(map_err)?;
        let keys: Vec<(u64, i64)> = tx
            .exec(
                format!(
                    "SELECT DISTINCT fingerprint_id, TIMESTAMPDIFF(SECOND, '1970-01-01', \
                     bucket_start) FROM {} WHERE bucket_start < {AT_S} AND part <> {DAY_PART} \
                     AND (gateway_id <> 0 OR part <> 0) ORDER BY 2, 1 LIMIT {BATCH}",
                    self.stats
                ),
                (cutoff,),
            )
            .await
            .map_err(map_err)?;
        if keys.is_empty() {
            tx.rollback().await.map_err(map_err)?;
            return Ok((0, 0));
        }
        let (merged, deleted) = self.merge_keys(&mut tx, &keys, None).await?;
        tx.commit().await.map_err(map_err)?;
        Ok((merged, deleted))
    }

    /// Merges the rows of each (fingerprint, bucket) key into one row:
    /// into `(bucket, 0, 0)`, or with `day`, into `(day start, 0,
    /// DAY_PART)`. Returns (keys merged, rows deleted).
    async fn merge_keys<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        keys: &[(u64, i64)],
        day: Option<()>,
    ) -> Result<(u64, u64)> {
        let cond = vec![
            "(fingerprint_id = ? AND bucket_start = TIMESTAMPADD(SECOND, ?, '1970-01-01'))";
            keys.len()
        ]
        .join(" OR ");
        let mut kp: Vec<Value> = Vec::with_capacity(keys.len() * 2);
        for (fp, b) in keys {
            kp.push(Value::UInt(*fp));
            kp.push(Value::Int(*b));
        }
        // Hour rollup: every partial row and an earlier merged row, never a
        // day row. Day merge: only merged hour rows.
        let only = match day {
            None => format!("part <> {DAY_PART}"),
            Some(()) => "gateway_id = 0 AND part = 0".to_owned(),
        };
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT fingerprint_id, TIMESTAMPDIFF(SECOND, '1970-01-01', bucket_start), \
                     gateway_id, part, {STAT_COLS} FROM {} WHERE ({cond}) AND {only} FOR UPDATE",
                    self.stats
                ),
                kp.clone(),
            )
            .await
            .map_err(map_err)?;
        // Target key → merged stats.
        let mut merged: BTreeMap<(u64, i64), Stats> = BTreeMap::new();
        let mut n_rows = 0u64;
        for mut r in rows {
            let fp: u64 = take(&mut r, 0, "fingerprint_id")?;
            let b: i64 = take(&mut r, 1, "bucket_start")?;
            let s = read_stats(&mut r, 4)?;
            let target = match day {
                Some(()) => b - b.rem_euclid(DAY.cast_signed()),
                None => b,
            };
            merged.entry((fp, target)).or_default().merge(&s);
            n_rows += 1;
        }
        let part = if day.is_some() { DAY_PART } else { 0 };
        q.exec_drop(
            format!("DELETE FROM {} WHERE ({cond}) AND {only}", self.stats),
            kp,
        )
        .await
        .map_err(map_err)?;
        if day.is_some() {
            // Day rows already there (an earlier run) join the merge.
            let days: Vec<(u64, i64)> = merged.keys().copied().collect();
            let dcond = vec!["(fingerprint_id = ? AND bucket_start = TIMESTAMPADD(SECOND, ?, '1970-01-01') AND gateway_id = 0 AND part = ?)"; days.len()].join(" OR ");
            let mut dp = Vec::with_capacity(days.len() * 3);
            for (fp, b) in &days {
                dp.extend([
                    Value::UInt(*fp),
                    Value::Int(*b),
                    Value::UInt(u64::from(DAY_PART)),
                ]);
            }
            let old: Vec<Row> = q
                .exec(
                    format!(
                        "SELECT fingerprint_id, TIMESTAMPDIFF(SECOND, '1970-01-01', bucket_start), \
                         {STAT_COLS} FROM {} WHERE {dcond} FOR UPDATE",
                        self.stats
                    ),
                    dp,
                )
                .await
                .map_err(map_err)?;
            for mut r in old {
                let fp: u64 = take(&mut r, 0, "fingerprint_id")?;
                let b: i64 = take(&mut r, 1, "bucket_start")?;
                let s = read_stats(&mut r, 2)?;
                merged.entry((fp, b)).or_default().merge(&s);
            }
        }
        let list: Vec<((u64, i64), Stats)> = merged.into_iter().collect();
        for chunk in list.chunks(BATCH) {
            let sql = format!(
                "REPLACE INTO {} (fingerprint_id, bucket_start, gateway_id, part, {STAT_COLS}) \
                 VALUES {}",
                self.stats,
                vec![
                    format!(
                        "(?, {AT_S}, 0, {part}, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    );
                    chunk.len()
                ]
                .join(", ")
            );
            let mut params = Vec::with_capacity(chunk.len() * 19);
            for ((fp, b), s) in chunk {
                params.push(Value::UInt(*fp));
                params.push(Value::Int(*b));
                stat_values(s, &mut params);
            }
            q.exec_drop(sql, params).await.map_err(map_err)?;
        }
        Ok((keys.len() as u64, n_rows))
    }

    async fn day_batch(&self, conn: &mut Conn, day_cutoff: u64) -> Result<u64> {
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .await
            .map_err(map_err)?;
        let keys: Vec<(u64, i64)> = tx
            .exec(
                format!(
                    "SELECT fingerprint_id, TIMESTAMPDIFF(SECOND, '1970-01-01', bucket_start) \
                     FROM {} WHERE bucket_start < {AT_S} AND gateway_id = 0 AND part = 0 \
                     ORDER BY 2, 1 LIMIT {BATCH}",
                    self.stats
                ),
                (day_cutoff,),
            )
            .await
            .map_err(map_err)?;
        if keys.is_empty() {
            tx.rollback().await.map_err(map_err)?;
            return Ok(0);
        }
        // Delete exactly these hour rows (the merge's DELETE matches any
        // gateway and part of the key; hours older than the cutoff are
        // already rolled up, so only (0, 0) rows remain there).
        let (n, _) = self.merge_keys(&mut tx, &keys, Some(())).await?;
        tx.commit().await.map_err(map_err)?;
        Ok(n)
    }

    /// Deletes what retention says (module docs). `now` is seconds since
    /// the epoch.
    ///
    /// # Errors
    /// The catalog's errors.
    pub async fn retain<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        now: u64,
        r: &Retention,
    ) -> Result<MaintenanceReport> {
        let mut rep = MaintenanceReport::default();
        let horizon = now.saturating_sub(u64::from(r.retention_days) * DAY);
        loop {
            let n = exec_count(
                q,
                format!(
                    "DELETE FROM {} WHERE bucket_start < {AT_S} LIMIT 10000",
                    self.stats
                ),
                (horizon,),
            )
            .await?;
            rep.expired_stats += n;
            if n < 10_000 {
                break;
            }
        }
        rep.expired_samples += exec_count(
            q,
            format!("DELETE FROM {} WHERE expires_at < {AT_S}", self.samples),
            (now,),
        )
        .await?;
        rep.expired_samples += exec_count(
            q,
            format!(
                "DELETE s FROM {t} s JOIN (SELECT fingerprint_id, sample_id FROM (SELECT \
                 fingerprint_id, sample_id, ROW_NUMBER() OVER (PARTITION BY fingerprint_id \
                 ORDER BY captured_at DESC, sample_id) AS rn FROM {t}) x WHERE rn > ?) d \
                 USING (fingerprint_id, sample_id)",
                t = self.samples
            ),
            (u64::from(r.max_samples_per_fingerprint),),
        )
        .await?;
        rep.expired_fingerprints += exec_count(
            q,
            format!("DELETE FROM {} WHERE last_seen < {AT_S}", self.fps),
            (horizon,),
        )
        .await?;
        Ok(rep)
    }
}

mod catalog;

pub use catalog::{CatalogWorkloadStore, WorkloadStore, maintain};

#[cfg(test)]
mod tests;
