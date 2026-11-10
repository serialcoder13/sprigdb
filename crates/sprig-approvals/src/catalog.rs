//! The tenant catalog's tables (chapter 52, "Catalog tables"): the
//! `approvals` migration module and [`CatalogRecStore`].
//!
//! Like the decision log, these tables are not versioned catalog state:
//! they are read on demand and written in plain transactions, so a
//! recommendation never invalidates a gateway's catalog cache. Catalogs
//! bootstrapped before the module existed are migrated on first use.
//!
//! A row keeps the whole item in `body` (JSON); the other columns are
//! projections for filtering, the due index and the one-live-item-per-
//! fingerprint unique key. Times are microseconds since the epoch.

use std::sync::Arc;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row, TxOpts};
use sprig_catalog::migrate::MigrateOpts;
use sprig_catalog::{CatalogLocator, Migration, Migrator, Module, Phase};
use sprig_common::ids::TenantId;
use sprig_common::names::quote;
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};

use crate::machine::{self, Verdict, VerdictPlan};
use crate::model::{Micros, Proposal, RecId, Recommendation, State};
use crate::policy::Policy;
use crate::store::{DUE_APPLYING_TIMEOUT, Filter, Raised, RecStore};

/// The module name in the catalog's format records.
pub const MODULE: &str = "approvals";

/// `recommendations` (chapter 52).
pub const DDL_RECOMMENDATIONS: &str = "CREATE TABLE IF NOT EXISTS recommendations (
  rec_id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  kind           VARCHAR(32)     NOT NULL,
  level          ENUM('table','database','tenant','dc','platform') NOT NULL,
  state          ENUM('open','approved','applying','applied','failed','ignored',
                      'snoozed','superseded','expired') NOT NULL,
  advisory       BOOLEAN         NOT NULL,
  db_id          INT UNSIGNED    NULL,
  object_id      BIGINT UNSIGNED NULL,
  target         VARCHAR(255)    NOT NULL,
  fingerprint    VARCHAR(32)     NOT NULL,
  notify_level   VARCHAR(40)     NOT NULL,
  due_at_us      BIGINT          NULL,
  created_at_us  BIGINT          NOT NULL,
  updated_at_us  BIGINT          NOT NULL,
  version        BIGINT UNSIGNED NOT NULL,
  body           JSON            NOT NULL,
  live_fp VARCHAR(32) AS (IF(state = 'open' OR state = 'approved' OR state = 'applying'
                          OR state = 'snoozed', fingerprint, NULL)) PERSISTENT,
  PRIMARY KEY (rec_id),
  UNIQUE KEY uk_live_fp (live_fp),
  KEY by_fp (fingerprint, updated_at_us),
  KEY by_target (kind, target),
  KEY by_state (state, updated_at_us),
  KEY by_due (due_at_us),
  KEY by_object (db_id, object_id, state)
) ENGINE=InnoDB";

/// `approval_subscriptions` (chapter 52): email preferences.
pub const DDL_SUBSCRIPTIONS: &str = "CREATE TABLE IF NOT EXISTS approval_subscriptions (
  user_name    VARCHAR(128)    NOT NULL,
  email        VARCHAR(254)    NOT NULL,
  delivery     ENUM('immediate','digest','off') NOT NULL,
  kinds        VARCHAR(255)    NOT NULL DEFAULT '*',
  updated_at_us BIGINT         NOT NULL,
  last_digest_us BIGINT        NULL,
  PRIMARY KEY (user_name)
) ENGINE=InnoDB";

/// The module's migrations.
pub static APPROVALS_MIGRATIONS: &[Migration] = &[Migration {
    format: 1,
    phase: Phase::Expand,
    name: "recommendations and approval_subscriptions",
    statements: &[DDL_RECOMMENDATIONS, DDL_SUBSCRIPTIONS],
}];

/// The module, for [`Migrator::new`] and chapter 08's bootstrap hooks.
pub const APPROVALS_MODULE: Module = Module {
    name: MODULE,
    migrations: APPROVALS_MIGRATIONS,
};

pub(crate) fn map_err(e: mysql_async::Error) -> SprigError {
    match &e {
        mysql_async::Error::Server(s) => {
            let code = s.code;
            SprigError::backend(
                code,
                *b"HY000",
                format!("recommendations: statement failed ({code}): {}", s.message),
            )
            .with_source(e)
        }
        _ => SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e),
    }
}

fn errno(e: &SprigError) -> Option<u16> {
    match e.kind {
        ErrorKind::Backend { errno, .. } => Some(errno),
        _ => None,
    }
}

const COLS: &str = "rec_id, body";

/// SQL access to one catalog's tables.
#[derive(Debug, Clone)]
pub struct SqlRecs {
    tenant: TenantId,
    table: String,
    subs: String,
}

impl SqlRecs {
    /// The tables in catalog `schema` of `tenant`.
    #[must_use]
    pub fn new(tenant: TenantId, schema: &str) -> Self {
        Self {
            tenant,
            table: format!("{}.recommendations", quote(schema)),
            subs: format!("{}.approval_subscriptions", quote(schema)),
        }
    }

    fn decode(&self, mut row: Row) -> Result<Recommendation> {
        let id: u64 = row
            .take(0)
            .ok_or_else(|| SprigError::internal("recommendations: no rec_id"))?;
        let body: String = row
            .take(1)
            .ok_or_else(|| SprigError::internal("recommendations: no body"))?;
        let mut r: Recommendation = serde_json::from_str(&body)
            .map_err(|e| SprigError::internal(format!("recommendations: bad body of {id}: {e}")))?;
        r.id = RecId::Tenant(self.tenant, id);
        Ok(r)
    }

    fn params(r: &Recommendation) -> Result<Vec<mysql_async::Value>> {
        let body = serde_json::to_string(r)
            .map_err(|e| SprigError::internal(format!("recommendations: encode: {e}")))?;
        Ok(vec![
            r.kind.as_str().into(),
            r.level.as_str().into(),
            r.state.as_str().into(),
            r.advisory.into(),
            r.subject.db.map(|d| d.0).into(),
            r.subject.object.map(|o| o.0).into(),
            r.target.clone().into(),
            r.fingerprint.clone().into(),
            r.notify_level.to_string().into(),
            machine::next_due(r, DUE_APPLYING_TIMEOUT).into(),
            r.created_at_us.into(),
            r.updated_at_us.into(),
            r.version.into(),
            body.into(),
        ])
    }

    /// Inserts a new item; returns its number.
    ///
    /// # Errors
    /// MariaDB's (1062 when a live item with the fingerprint exists).
    pub async fn insert<Q: Queryable + Send>(&self, q: &mut Q, r: &Recommendation) -> Result<u64> {
        let sql = format!(
            "INSERT INTO {} (kind, level, state, advisory, db_id, object_id, target, fingerprint, \
             notify_level, due_at_us, created_at_us, updated_at_us, version, body) \
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            self.table
        );
        let res = q.exec_iter(sql, Self::params(r)?).await.map_err(map_err)?;
        let id = res.last_insert_id().unwrap_or(0);
        res.drop_result().await.map_err(map_err)?;
        Ok(id)
    }

    /// Writes `r` if its stored version is `expected`.
    ///
    /// # Errors
    /// MariaDB's.
    pub async fn update<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        r: &Recommendation,
        expected: u64,
    ) -> Result<bool> {
        let sql = format!(
            "UPDATE {} SET kind=?, level=?, state=?, advisory=?, db_id=?, object_id=?, target=?, \
             fingerprint=?, notify_level=?, due_at_us=?, created_at_us=?, updated_at_us=?, \
             version=?, body=? WHERE rec_id=? AND version=?",
            self.table
        );
        let mut p = Self::params(r)?;
        p.push(r.id.number().into());
        p.push(expected.into());
        let res = q.exec_iter(sql, p).await.map_err(map_err)?;
        let n = res.affected_rows();
        res.drop_result().await.map_err(map_err)?;
        Ok(n == 1)
    }

    /// One item.
    ///
    /// # Errors
    /// MariaDB's.
    pub async fn get<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        n: u64,
    ) -> Result<Option<Recommendation>> {
        let sql = format!("SELECT {COLS} FROM {} WHERE rec_id = ?", self.table);
        let row: Option<Row> = q.exec_first(sql, (n,)).await.map_err(map_err)?;
        row.map(|r| self.decode(r)).transpose()
    }

    /// Items passing `f`, newest first.
    ///
    /// # Errors
    /// MariaDB's.
    pub async fn list<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        f: &Filter,
    ) -> Result<Vec<Recommendation>> {
        let mut wh = Vec::new();
        let mut args: Vec<mysql_async::Value> = Vec::new();
        let states: Vec<State> = if !f.states.is_empty() {
            f.states.clone()
        } else if f.all {
            Vec::new()
        } else {
            State::ALL.iter().copied().filter(|s| s.is_live()).collect()
        };
        if !states.is_empty() {
            wh.push(format!("state IN ({})", vec!["?"; states.len()].join(",")));
            args.extend(states.iter().map(|s| s.as_str().into()));
        }
        if let Some(l) = f.level {
            wh.push("level = ?".into());
            args.push(l.as_str().into());
        }
        if let Some(d) = f.db {
            wh.push("db_id = ?".into());
            args.push(d.0.into());
        }
        if let Some(o) = f.object {
            wh.push("object_id = ?".into());
            args.push(o.0.into());
        }
        if !f.kinds.is_empty() {
            let mut ors = Vec::new();
            for k in &f.kinds {
                ors.push("kind = ? OR kind LIKE ?".to_string());
                args.push(k.clone().into());
                args.push(format!("{k}.%").into());
            }
            wh.push(format!("({})", ors.join(" OR ")));
        }
        let wh = if wh.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", wh.join(" AND "))
        };
        let sql = format!(
            "SELECT {COLS} FROM {} {wh} ORDER BY rec_id DESC LIMIT {}",
            self.table,
            f.limit()
        );
        let rows: Vec<Row> = q.exec(sql, args).await.map_err(map_err)?;
        rows.into_iter().map(|r| self.decode(r)).collect()
    }

    /// Items due at `now`.
    ///
    /// # Errors
    /// MariaDB's.
    pub async fn due<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        now: Micros,
        limit: usize,
    ) -> Result<Vec<Recommendation>> {
        let sql = format!(
            "SELECT {COLS} FROM {} WHERE due_at_us <= ? ORDER BY due_at_us LIMIT {limit}",
            self.table
        );
        let rows: Vec<Row> = q.exec(sql, (now,)).await.map_err(map_err)?;
        rows.into_iter().map(|r| self.decode(r)).collect()
    }

    /// Deletes closed items changed before `before`.
    ///
    /// # Errors
    /// MariaDB's.
    pub async fn purge<Q: Queryable + Send>(&self, q: &mut Q, before: Micros) -> Result<u64> {
        let sql = format!(
            "DELETE FROM {} WHERE state IN ('applied','failed','ignored','superseded','expired') \
             AND updated_at_us < ? AND NOT (state = 'ignored' AND JSON_VALUE(body, '$.ignore.until_us') IS NULL) \
             LIMIT 1000",
            self.table
        );
        let res = q.exec_iter(sql, (before,)).await.map_err(map_err)?;
        let n = res.affected_rows();
        res.drop_result().await.map_err(map_err)?;
        Ok(n)
    }

    /// The raise, in one transaction on `conn` (chapter 52, "The gate").
    ///
    /// # Errors
    /// MariaDB's; 1062 or 1213 when another raise of the same advice won
    /// (the caller retries).
    pub async fn raise(
        &self,
        conn: &mut Conn,
        p: &Proposal,
        policy: &Policy,
        ancestry: &[TenantId],
        now: Micros,
    ) -> Result<Raised> {
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .await
            .map_err(map_err)?;
        let fp = p.fingerprint();
        let sql = format!(
            "SELECT {COLS} FROM {} WHERE kind = ? AND target = ? AND live_fp IS NOT NULL FOR UPDATE",
            self.table
        );
        let rows: Vec<Row> = tx
            .exec(sql, (p.kind.as_str(), &p.target))
            .await
            .map_err(map_err)?;
        let same = rows
            .into_iter()
            .map(|r| self.decode(r))
            .collect::<Result<Vec<_>>>()?;
        let ignored = if same.iter().any(|r| r.fingerprint == fp) {
            None
        } else {
            let sql = format!(
                "SELECT {COLS} FROM {} WHERE fingerprint = ? AND state = 'ignored' \
                 ORDER BY updated_at_us DESC LIMIT 1",
                self.table
            );
            let row: Option<Row> = tx.exec_first(sql, (&fp,)).await.map_err(map_err)?;
            row.map(|r| self.decode(r)).transpose()?
        };
        let plan = machine::raise(
            p,
            policy,
            &same,
            ignored.as_ref(),
            ancestry,
            RecId::Tenant(self.tenant, 0),
            now,
        );
        let mut stored = Vec::new();
        for w in &plan.writes {
            let mut w = w.clone();
            if w.id.number() == 0 {
                let n = self.insert(&mut tx, &w).await?;
                w.id = RecId::Tenant(self.tenant, n);
            } else {
                let expected = same.iter().find(|x| x.id == w.id).map_or(0, |x| x.version);
                if !self.update(&mut tx, &w, expected).await? {
                    return Err(SprigError::new(
                        SprigCode::CatalogConflict,
                        "recommendation changed during the raise",
                    ));
                }
            }
            stored.push(w);
        }
        tx.commit().await.map_err(map_err)?;
        let mut out = Raised::from_plan(&plan, &stored);
        if plan.verdict == VerdictPlan::Suppressed
            && let Some(i) = ignored
        {
            out.verdict = Verdict::Suppressed(i.id);
        }
        Ok(out)
    }

    /// Email subscriptions.
    ///
    /// # Errors
    /// MariaDB's.
    pub async fn subscriptions<Q: Queryable + Send>(&self, q: &mut Q) -> Result<Vec<Subscription>> {
        let sql = format!(
            "SELECT user_name, email, delivery, kinds, last_digest_us FROM {} ORDER BY user_name",
            self.subs
        );
        let rows: Vec<(String, String, String, String, Option<i64>)> =
            q.query(sql).await.map_err(map_err)?;
        Ok(rows
            .into_iter()
            .map(|(user, email, delivery, kinds, last)| Subscription {
                user,
                email,
                delivery,
                kinds,
                last_digest_us: last,
            })
            .collect())
    }

    /// Sets one user's subscription (`delivery = off` keeps the row).
    ///
    /// # Errors
    /// MariaDB's.
    pub async fn set_subscription<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        s: &Subscription,
        now: Micros,
    ) -> Result<()> {
        let sql = format!(
            "INSERT INTO {} (user_name, email, delivery, kinds, updated_at_us) VALUES (?,?,?,?,?) \
             ON DUPLICATE KEY UPDATE email = VALUES(email), delivery = VALUES(delivery), \
             kinds = VALUES(kinds), updated_at_us = VALUES(updated_at_us)",
            self.subs
        );
        q.exec_drop(sql, (&s.user, &s.email, &s.delivery, &s.kinds, now))
            .await
            .map_err(map_err)
    }

    /// Records a sent digest.
    ///
    /// # Errors
    /// MariaDB's.
    pub async fn digest_sent<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        user: &str,
        now: Micros,
    ) -> Result<()> {
        let sql = format!(
            "UPDATE {} SET last_digest_us = ? WHERE user_name = ?",
            self.subs
        );
        q.exec_drop(sql, (now, user)).await.map_err(map_err)
    }
}

/// One user's email preference.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Subscription {
    /// The tenant user (or an operator label).
    pub user: String,
    /// The address.
    pub email: String,
    /// `immediate`, `digest` or `off`.
    pub delivery: String,
    /// `*` or a comma list of kinds and families.
    #[serde(default = "star")]
    pub kinds: String,
    /// The last digest sent.
    #[serde(default)]
    pub last_digest_us: Option<i64>,
}

fn star() -> String {
    "*".into()
}

/// A tenant's recommendations in its catalog, reached through a
/// [`CatalogLocator`].
pub struct CatalogRecStore {
    locator: Arc<dyn CatalogLocator>,
    tenant: TenantId,
}

impl std::fmt::Debug for CatalogRecStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogRecStore")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl CatalogRecStore {
    /// `tenant`'s store.
    #[must_use]
    pub fn new(locator: Arc<dyn CatalogLocator>, tenant: TenantId) -> Self {
        Self { locator, tenant }
    }

    /// A writer connection and the tables, migrating the module on first
    /// use.
    ///
    /// # Errors
    /// The locator's and MariaDB's.
    pub async fn open(&self) -> Result<(Conn, SqlRecs)> {
        let ep = self.locator.writer(self.tenant).await?;
        let conn = ep.pool.get_conn().await.map_err(map_err)?;
        Ok((conn, SqlRecs::new(self.tenant, ep.store.schema())))
    }

    async fn migrate(&self, conn: &mut Conn) -> Result<()> {
        let ep = self.locator.writer(self.tenant).await?;
        Migrator::new(&[APPROVALS_MODULE])?
            .run(conn, ep.store.schema(), &MigrateOpts::default())
            .await?;
        Ok(())
    }
}

/// Runs `$body` with `$c` (a connection) and `$s` (the tables), migrating
/// the module and retrying once when the table is missing.
macro_rules! with_conn {
    ($self:ident, |$c:ident, $s:ident| $body:expr) => {{
        let (mut conn, $s) = $self.open().await?;
        let first = {
            let $c = &mut conn;
            $body
        };
        match first {
            Err(e) if errno(&e) == Some(1146) => {
                $self.migrate(&mut conn).await?;
                let $c = &mut conn;
                $body
            }
            r => r,
        }
    }};
}

#[async_trait::async_trait]
impl RecStore for CatalogRecStore {
    async fn raise(
        &self,
        p: &Proposal,
        policy: &Policy,
        ancestry: &[TenantId],
        now: Micros,
    ) -> Result<Raised> {
        let mut last = None;
        for _ in 0..3 {
            let r = with_conn!(self, |c, s| s.raise(c, p, policy, ancestry, now).await);
            match r {
                Err(e)
                    if matches!(errno(&e), Some(1062 | 1213 | 1205))
                        || e.code() == Some(SprigCode::CatalogConflict) =>
                {
                    last = Some(e);
                }
                r => return r,
            }
        }
        Err(last.unwrap_or_else(|| SprigError::internal("raise retries")))
    }

    async fn get(&self, n: u64) -> Result<Option<Recommendation>> {
        with_conn!(self, |c, s| s.get(c, n).await)
    }

    async fn list(&self, f: &Filter) -> Result<Vec<Recommendation>> {
        with_conn!(self, |c, s| s.list(c, f).await)
    }

    async fn put(&self, r: &Recommendation, expected: u64) -> Result<bool> {
        with_conn!(self, |c, s| s.update(c, r, expected).await)
    }

    async fn due(&self, now: Micros, limit: usize) -> Result<Vec<Recommendation>> {
        with_conn!(self, |c, s| s.due(c, now, limit).await)
    }

    async fn purge(&self, before: Micros) -> Result<u64> {
        with_conn!(self, |c, s| s.purge(c, before).await)
    }
}

#[cfg(test)]
mod live_tests;
