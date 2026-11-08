//! The decision log (chapter 11, "The decision log"; build step 1): the
//! `decisions` table in each tenant's catalog schema, the [`DecisionLog`]
//! interface every component writes through, the retention job, and the
//! reads behind `sprigctl explain decision`.
//!
//! The table is the `decisions` migration module ([`DECISIONS_MODULE`]),
//! registered with `sprig-catalog`'s migrator by whoever bootstraps tenant
//! catalogs, as `sprig-advisor` does for its tables. Decisions are not
//! versioned catalog state (chapter 08 reads them on demand), so writes are
//! plain statements.
//!
//! Implementations:
//!
//! - [`CatalogDecisionLog`] writes to the tenant's catalog through a
//!   [`CatalogLocator`], buffering up to [`BUFFER_LIMIT`] decisions per
//!   process while a catalog is unreachable (chapter 11, "Failure handling");
//! - [`MemDecisionLog`] keeps them in memory (tests and the fleet
//!   simulator).
//!
//! **Times** travel as microseconds since the epoch, as in the other
//! catalog modules, so the session time zone never matters.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use mysql_async::prelude::Queryable;
use mysql_async::{Row, Value};
use serde::{Deserialize, Serialize};
use sprig_catalog::{CatalogLocator, Migration, Module, Phase};
use sprig_common::ids::TenantId;
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};

/// The module name in the catalog's format records.
pub const MODULE: &str = "decisions";

/// `decisions` (chapter 11, Appendix C).
pub const DDL_DECISIONS: &str = "CREATE TABLE IF NOT EXISTS decisions (
  decision_id   BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  decided_at    DATETIME(6)     NOT NULL,
  component     VARCHAR(24)     NOT NULL,
  kind          VARCHAR(48)     NOT NULL,
  subject_type  VARCHAR(24)     NOT NULL,
  subject_id    VARCHAR(64)     NOT NULL,
  mode          ENUM('applied','recommended','dry_run','operator') NOT NULL,
  outcome       ENUM('proposed','scheduled','running','succeeded','failed',
                     'rejected','cancelled','superseded') NOT NULL,
  reason_code   VARCHAR(64)     NOT NULL,
  summary       VARCHAR(512)    NOT NULL,
  evidence      JSON            NOT NULL,
  alternatives  JSON            NULL,
  job_id        BINARY(16)      NULL,
  parent_id     BIGINT UNSIGNED NULL,
  actor         VARCHAR(128)    NOT NULL,
  dc_id         VARCHAR(32)     NOT NULL,
  updated_at    DATETIME(6)     NOT NULL,
  PRIMARY KEY (decision_id),
  KEY by_subject (subject_type, subject_id, decided_at),
  KEY by_kind (component, kind, decided_at),
  KEY by_time (decided_at)
) ENGINE=InnoDB";

/// The module's migrations.
pub static DECISIONS_MIGRATIONS: &[Migration] = &[Migration {
    format: 1,
    phase: Phase::Expand,
    name: "decisions",
    statements: &[DDL_DECISIONS],
}];

/// The module, for [`Migrator::new`](sprig_catalog::Migrator::new) and
/// chapter 08's `BootstrapHooks.modules`.
pub const DECISIONS_MODULE: Module = Module {
    name: MODULE,
    migrations: DECISIONS_MIGRATIONS,
};

/// Decisions held per process while catalogs are unreachable.
pub const BUFFER_LIMIT: usize = 1000;

/// Rows the retention job deletes per statement.
pub const PURGE_BATCH: u32 = 1000;

macro_rules! text_enum {
    ($(#[$doc:meta])* $name:ident { $($(#[$vdoc:meta])* $variant:ident = $text:literal,)* }) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum $name { $($(#[$vdoc])* $variant,)* }

        impl $name {
            /// The SQL text.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self { $($name::$variant => $text,)* }
            }

            /// Parses the SQL text.
            ///
            /// # Errors
            /// `Internal` for any other text.
            pub fn parse(s: &str) -> Result<Self> {
                match s {
                    $($text => Ok($name::$variant),)*
                    _ => Err(SprigError::internal(format!(
                        concat!("decisions: bad ", stringify!($name), " {:?}"), s
                    ))),
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

text_enum! {
    /// `decisions.mode`.
    Mode {
        /// Done automatically.
        Applied = "applied",
        /// Recommended; nothing runs.
        Recommended = "recommended",
        /// `dry_run`: computed and logged only.
        DryRun = "dry_run",
        /// An operator's command.
        Operator = "operator",
    }
}

text_enum! {
    /// `decisions.outcome`.
    Outcome {
        /// Decided, not yet scheduled.
        Proposed = "proposed",
        /// A job exists.
        Scheduled = "scheduled",
        /// The job runs.
        Running = "running",
        /// Done.
        Succeeded = "succeeded",
        /// The job failed.
        Failed = "failed",
        /// Refused by a guardrail or constraint.
        Rejected = "rejected",
        /// Cancelled before running.
        Cancelled = "cancelled",
        /// A later decision replaced it.
        Superseded = "superseded",
    }
}

impl Outcome {
    /// Whether nothing more will happen to the decision.
    #[must_use]
    pub const fn is_final(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Rejected | Self::Cancelled | Self::Superseded
        )
    }
}

/// A decision to record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewDecision {
    /// When, microseconds since the epoch.
    pub decided_at_us: i64,
    /// `balancer`, `mover`, `advisor`, `autoindex`, `catalog`, ...
    pub component: String,
    /// `shard.place`, `shard.move`, `index.create`, ...
    pub kind: String,
    /// `shard`, `group`, `object`, `index`, `fingerprint`, `cluster`.
    pub subject_type: String,
    /// The subject's id, such as `t42/g1/s3`.
    pub subject_id: String,
    /// How it was applied.
    pub mode: Mode,
    /// Where it stands.
    pub outcome: Outcome,
    /// `cluster_hot`, `spread`, `residency_change`, ...
    pub reason_code: String,
    /// One line for people (cut to 512 characters).
    pub summary: String,
    /// Inputs, scores, thresholds, windows: identifiers and numbers only.
    pub evidence: serde_json::Value,
    /// Options considered, with scores.
    pub alternatives: Option<serde_json::Value>,
    /// The job, a UUIDv7.
    pub job_id: Option<[u8; 16]>,
    /// A parent decision.
    pub parent_id: Option<u64>,
    /// Controller id, or operator principal.
    pub actor: String,
    /// The DC that decided.
    pub dc_id: String,
}

/// A stored decision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionRow {
    /// Its id.
    pub decision_id: u64,
    /// The decision.
    #[serde(flatten)]
    pub d: NewDecision,
    /// Last change, microseconds since the epoch.
    pub updated_at_us: i64,
}

/// Where decisions go (chapter 11's interface).
#[async_trait::async_trait]
pub trait DecisionLog: Send + Sync {
    /// Records `d` in `tenant`'s log; returns its id (0 when buffered).
    ///
    /// # Errors
    /// The log's.
    async fn record(&self, tenant: TenantId, d: &NewDecision) -> Result<u64>;
    /// Changes a decision's outcome.
    ///
    /// # Errors
    /// The log's.
    async fn update_outcome(&self, tenant: TenantId, id: u64, o: Outcome, at_us: i64)
    -> Result<()>;
}

/// SQL access to one catalog's `decisions` table.
#[derive(Debug, Clone)]
pub struct DecisionStore {
    schema: String,
}

const COLUMNS: &str = "decision_id, TIMESTAMPDIFF(MICROSECOND, '1970-01-01', decided_at), \
    component, kind, subject_type, subject_id, mode, outcome, reason_code, summary, \
    evidence, alternatives, job_id, parent_id, actor, dc_id, \
    TIMESTAMPDIFF(MICROSECOND, '1970-01-01', updated_at)";

const AT: &str = "TIMESTAMPADD(MICROSECOND, ?, '1970-01-01')";

impl DecisionStore {
    /// The store in catalog schema `schema`.
    #[must_use]
    pub fn new(schema: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
        }
    }

    /// The store in `tenant`'s catalog, `t<tenant>_sprig`.
    #[must_use]
    pub fn for_tenant(tenant: TenantId) -> Self {
        Self::new(format!("t{tenant}_sprig"))
    }

    fn table(&self) -> String {
        format!("{}.decisions", quote(&self.schema))
    }

    /// Inserts `d`; returns its id.
    ///
    /// # Errors
    /// The driver's, mapped.
    pub async fn insert<Q: Queryable + Send>(&self, q: &mut Q, d: &NewDecision) -> Result<u64> {
        let sql = format!(
            "INSERT INTO {} (decided_at, component, kind, subject_type, subject_id, mode, \
             outcome, reason_code, summary, evidence, alternatives, job_id, parent_id, actor, \
             dc_id, updated_at) VALUES ({AT}, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, {AT})",
            self.table()
        );
        let params: Vec<Value> = vec![
            d.decided_at_us.into(),
            cut(&d.component, 24).into(),
            cut(&d.kind, 48).into(),
            cut(&d.subject_type, 24).into(),
            cut(&d.subject_id, 64).into(),
            d.mode.as_str().into(),
            d.outcome.as_str().into(),
            cut(&d.reason_code, 64).into(),
            cut(&d.summary, 512).into(),
            json(&d.evidence)?.into(),
            d.alternatives.as_ref().map(json).transpose()?.into(),
            d.job_id.map(|j| j.to_vec()).into(),
            d.parent_id.into(),
            cut(&d.actor, 128).into(),
            cut(&d.dc_id, 32).into(),
            d.decided_at_us.into(),
        ];
        q.exec_drop(sql, params).await.map_err(map_err)?;
        let id: Option<u64> = q
            .query_first("SELECT LAST_INSERT_ID()")
            .await
            .map_err(map_err)?;
        id.ok_or_else(|| SprigError::internal("LAST_INSERT_ID() returned nothing"))
    }

    /// Sets a decision's outcome (and its job, when given).
    ///
    /// # Errors
    /// The driver's, mapped.
    pub async fn set_outcome<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        id: u64,
        o: Outcome,
        job_id: Option<[u8; 16]>,
        at_us: i64,
    ) -> Result<()> {
        let sql = format!(
            "UPDATE {} SET outcome = ?, job_id = COALESCE(?, job_id), updated_at = {AT} \
             WHERE decision_id = ?",
            self.table()
        );
        q.exec_drop(
            sql,
            (o.as_str(), job_id.map(|j| j.to_vec()), at_us, id),
        )
        .await
        .map_err(map_err)
    }

    /// One decision.
    ///
    /// # Errors
    /// The driver's, mapped.
    pub async fn get<Q: Queryable + Send>(&self, q: &mut Q, id: u64) -> Result<Option<DecisionRow>> {
        let sql = format!("SELECT {COLUMNS} FROM {} WHERE decision_id = ?", self.table());
        let row: Option<Row> = q.exec_first(sql, (id,)).await.map_err(map_err)?;
        row.map(decode).transpose()
    }

    /// The newest decisions about one subject, newest first.
    ///
    /// # Errors
    /// The driver's, mapped.
    pub async fn by_subject<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        subject_type: &str,
        subject_id: &str,
        limit: u32,
    ) -> Result<Vec<DecisionRow>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM {} WHERE subject_type = ? AND subject_id = ? \
             ORDER BY decided_at DESC, decision_id DESC LIMIT ?",
            self.table()
        );
        let rows: Vec<Row> = q
            .exec(sql, (subject_type, subject_id, limit))
            .await
            .map_err(map_err)?;
        rows.into_iter().map(decode).collect()
    }

    /// The newest decisions of a component and kind prefix (`shard.` for
    /// every shard decision), newest first.
    ///
    /// # Errors
    /// The driver's, mapped.
    pub async fn recent<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        component: &str,
        since_us: i64,
        limit: u32,
    ) -> Result<Vec<DecisionRow>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM {} WHERE component = ? AND decided_at >= {AT} \
             ORDER BY decided_at DESC, decision_id DESC LIMIT ?",
            self.table()
        );
        let rows: Vec<Row> = q
            .exec(sql, (component, since_us, limit))
            .await
            .map_err(map_err)?;
        rows.into_iter().map(decode).collect()
    }

    /// The retention job: deletes rows decided before `before_us`, in
    /// batches of [`PURGE_BATCH`] (index `by_time`); returns how many.
    ///
    /// # Errors
    /// The driver's, mapped.
    pub async fn purge_before<Q: Queryable + Send>(&self, q: &mut Q, before_us: i64) -> Result<u64> {
        let sql = format!(
            "DELETE FROM {} WHERE decided_at < {AT} ORDER BY decided_at LIMIT {PURGE_BATCH}",
            self.table()
        );
        let mut total = 0u64;
        loop {
            q.exec_drop(&sql, (before_us,)).await.map_err(map_err)?;
            let n: u64 = q
                .query_first::<i64, _>("SELECT ROW_COUNT()")
                .await
                .map_err(map_err)?
                .and_then(|n| u64::try_from(n).ok())
                .unwrap_or(0);
            total += n;
            if n < u64::from(PURGE_BATCH) {
                return Ok(total);
            }
        }
    }
}

fn cut(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_owned();
    }
    s.chars().take(max).collect()
}

fn json(v: &serde_json::Value) -> Result<String> {
    serde_json::to_string(v).map_err(|e| SprigError::internal(format!("JSON encoding: {e}")))
}

fn map_err(e: mysql_async::Error) -> SprigError {
    match &e {
        mysql_async::Error::Server(s) => {
            let code = s.code;
            SprigError::backend(code, *b"HY000", format!("decisions: statement failed ({code})"))
                .with_source(e)
        }
        _ => SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e),
    }
}

fn take<T: mysql_async::prelude::FromValue>(row: &mut Row, i: usize, name: &str) -> Result<T> {
    match row.take_opt::<T, usize>(i) {
        Some(Ok(v)) => Ok(v),
        Some(Err(e)) => Err(SprigError::internal(format!("decisions.{name}: {e}"))),
        None => Err(SprigError::internal(format!("decisions.{name} missing"))),
    }
}

fn parse_json(raw: Option<Vec<u8>>, name: &str) -> Result<Option<serde_json::Value>> {
    raw.map(|b| {
        serde_json::from_slice(&b)
            .map_err(|e| SprigError::internal(format!("decisions.{name}: bad JSON: {e}")))
    })
    .transpose()
}

fn decode(mut r: Row) -> Result<DecisionRow> {
    let job: Option<Vec<u8>> = take(&mut r, 12, "job_id")?;
    let mode: String = take(&mut r, 6, "mode")?;
    let outcome: String = take(&mut r, 7, "outcome")?;
    Ok(DecisionRow {
        decision_id: take(&mut r, 0, "decision_id")?,
        d: NewDecision {
            decided_at_us: take(&mut r, 1, "decided_at")?,
            component: take(&mut r, 2, "component")?,
            kind: take(&mut r, 3, "kind")?,
            subject_type: take(&mut r, 4, "subject_type")?,
            subject_id: take(&mut r, 5, "subject_id")?,
            mode: Mode::parse(&mode)?,
            outcome: Outcome::parse(&outcome)?,
            reason_code: take(&mut r, 8, "reason_code")?,
            summary: take(&mut r, 9, "summary")?,
            evidence: parse_json(take(&mut r, 10, "evidence")?, "evidence")?
                .unwrap_or(serde_json::Value::Null),
            alternatives: parse_json(take(&mut r, 11, "alternatives")?, "alternatives")?,
            job_id: job.and_then(|j| <[u8; 16]>::try_from(j.as_slice()).ok()),
            parent_id: take(&mut r, 13, "parent_id")?,
            actor: take(&mut r, 14, "actor")?,
            dc_id: take(&mut r, 15, "dc_id")?,
        },
        updated_at_us: take(&mut r, 16, "updated_at")?,
    })
}

/// Decisions in memory, per tenant (tests and the simulator).
#[derive(Debug, Default)]
pub struct MemDecisionLog {
    rows: Mutex<BTreeMap<TenantId, Vec<DecisionRow>>>,
}

impl MemDecisionLog {
    /// An empty log.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every decision of `tenant`, oldest first.
    #[must_use]
    pub fn of(&self, tenant: TenantId) -> Vec<DecisionRow> {
        self.rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&tenant)
            .cloned()
            .unwrap_or_default()
    }

    /// Every decision, by tenant.
    #[must_use]
    pub fn all(&self) -> Vec<(TenantId, DecisionRow)> {
        self.rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .flat_map(|(t, v)| v.iter().map(move |r| (*t, r.clone())))
            .collect()
    }

    /// Deletes rows decided before `before_us`; returns how many.
    pub fn purge_before(&self, before_us: i64) -> usize {
        let mut g = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        let mut n = 0;
        for v in g.values_mut() {
            let len = v.len();
            v.retain(|r| r.d.decided_at_us >= before_us);
            n += len - v.len();
        }
        n
    }
}

#[async_trait::async_trait]
impl DecisionLog for MemDecisionLog {
    async fn record(&self, tenant: TenantId, d: &NewDecision) -> Result<u64> {
        let mut g = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        let id = g.values().map(Vec::len).sum::<usize>() as u64 + 1;
        g.entry(tenant).or_default().push(DecisionRow {
            decision_id: id,
            d: d.clone(),
            updated_at_us: d.decided_at_us,
        });
        Ok(id)
    }

    async fn update_outcome(
        &self,
        tenant: TenantId,
        id: u64,
        o: Outcome,
        at_us: i64,
    ) -> Result<()> {
        let mut g = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(r) = g
            .get_mut(&tenant)
            .and_then(|v| v.iter_mut().find(|r| r.decision_id == id))
        {
            r.d.outcome = o;
            r.updated_at_us = at_us;
        }
        Ok(())
    }
}

enum Pending {
    Record(TenantId, NewDecision),
}

/// Writes decisions to the tenant's catalog; while a catalog is
/// unreachable, keeps up to [`BUFFER_LIMIT`] of them and retries on the
/// next write or [`flush`](Self::flush).
pub struct CatalogDecisionLog {
    locator: Arc<dyn CatalogLocator>,
    buffer: Mutex<VecDeque<Pending>>,
}

impl fmt::Debug for CatalogDecisionLog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatalogDecisionLog").finish_non_exhaustive()
    }
}

impl CatalogDecisionLog {
    /// A log over `locator`.
    #[must_use]
    pub fn new(locator: Arc<dyn CatalogLocator>) -> Self {
        Self {
            locator,
            buffer: Mutex::new(VecDeque::new()),
        }
    }

    /// Decisions waiting for their catalog.
    #[must_use]
    pub fn buffered(&self) -> usize {
        self.buffer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    async fn write(&self, tenant: TenantId, d: &NewDecision) -> Result<u64> {
        let ep = self.locator.writer(tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(map_err)?;
        DecisionStore::new(ep.store.schema()).insert(&mut conn, d).await
    }

    /// Retries buffered decisions; returns how many were written.
    pub async fn flush(&self) -> usize {
        let pending: Vec<Pending> = self
            .buffer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain(..)
            .collect();
        let mut written = 0;
        let mut back = VecDeque::new();
        for p in pending {
            let Pending::Record(t, d) = p;
            if self.write(t, &d).await.is_ok() {
                written += 1;
            } else {
                back.push_back(Pending::Record(t, d));
            }
        }
        if !back.is_empty() {
            let mut g = self.buffer.lock().unwrap_or_else(PoisonError::into_inner);
            for p in back.into_iter().rev() {
                g.push_front(p);
            }
            while g.len() > BUFFER_LIMIT {
                g.pop_front();
            }
        }
        written
    }

    /// The decision with `id` in `tenant`'s log.
    ///
    /// # Errors
    /// The catalog's.
    pub async fn get(&self, tenant: TenantId, id: u64) -> Result<Option<DecisionRow>> {
        let ep = self.locator.reader(tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(map_err)?;
        DecisionStore::new(ep.store.schema()).get(&mut conn, id).await
    }

    /// The retention job for one tenant: deletes rows older than
    /// `retention_days` before `now_us`.
    ///
    /// # Errors
    /// The catalog's.
    pub async fn purge(&self, tenant: TenantId, retention_days: u32, now_us: i64) -> Result<u64> {
        let ep = self.locator.writer(tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(map_err)?;
        let before = now_us - i64::from(retention_days) * 86_400 * 1_000_000;
        DecisionStore::new(ep.store.schema())
            .purge_before(&mut conn, before)
            .await
    }
}

#[async_trait::async_trait]
impl DecisionLog for CatalogDecisionLog {
    async fn record(&self, tenant: TenantId, d: &NewDecision) -> Result<u64> {
        if self.buffered() > 0 {
            self.flush().await;
        }
        match self.write(tenant, d).await {
            Ok(id) => Ok(id),
            Err(e) if e.code() == Some(SprigCode::CatalogUnavailable) || e.retryable => {
                tracing::warn!(tenant = %tenant, kind = %d.kind, error = %e,
                    "decision buffered: catalog unreachable");
                let mut g = self.buffer.lock().unwrap_or_else(PoisonError::into_inner);
                g.push_back(Pending::Record(tenant, d.clone()));
                while g.len() > BUFFER_LIMIT {
                    g.pop_front();
                }
                Ok(0)
            }
            Err(e) => Err(e),
        }
    }

    async fn update_outcome(
        &self,
        tenant: TenantId,
        id: u64,
        o: Outcome,
        at_us: i64,
    ) -> Result<()> {
        if id == 0 {
            return Ok(());
        }
        let ep = self.locator.writer(tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(map_err)?;
        DecisionStore::new(ep.store.schema())
            .set_outcome(&mut conn, id, o, None, at_us)
            .await
    }
}

#[cfg(test)]
mod tests;
