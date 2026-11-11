//! The MariaDB side of after-commit triggers: running a compiled version
//! on an event's shard with its marker, the catalog's dead letters and
//! log, the subscription's checkpoint, and dead-letter replays.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Pool};
use sprig_catalog::CatalogLocator;
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_feed::ResumeToken;
use sprig_feed::subs::{Subscription, Subscriptions};
use sprig_meta::MetaStore;

use super::{Applied, Apply, Outcomes, TriggerEvent, subscription_name};
use crate::catalog::LogicStore;
use crate::deploy::conn_err;

/// Pools of `sprig_ctl` connections to the cluster holding a tenant's
/// shards now.
#[async_trait]
pub trait ShardPools: Send + Sync {
    /// The pool for `tenant`'s cluster (R1 keeps a tenant's shards on one
    /// cluster at a time and moves them together).
    ///
    /// # Errors
    /// Unknown tenant or unreachable cluster (retryable).
    async fn pool(&self, tenant: TenantId) -> Result<Pool>;
}

/// The active version of a module and its parameter names, re-read from
/// the catalog at most every two seconds.
pub struct CatalogVersions {
    locator: Arc<dyn CatalogLocator>,
    tenant: TenantId,
    module_id: u64,
    cached: Mutex<Option<(Instant, u64, Vec<String>)>>,
}

impl CatalogVersions {
    /// Versions of `module_id` in `tenant`'s catalog.
    #[must_use]
    pub fn new(locator: Arc<dyn CatalogLocator>, tenant: TenantId, module_id: u64) -> Self {
        Self {
            locator,
            tenant,
            module_id,
            cached: Mutex::new(None),
        }
    }

    /// The active version and its parameters' names, in call order.
    ///
    /// # Errors
    /// 39860 without an active version; catalog errors.
    pub async fn active(&self) -> Result<(u64, Vec<String>)> {
        if let Some((at, v, p)) = &*self.cached.lock().unwrap_or_else(PoisonError::into_inner)
            && at.elapsed() < Duration::from_secs(2)
        {
            return Ok((*v, p.clone()));
        }
        let ep = self.locator.writer(self.tenant).await?;
        let mut c = ep.pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
        })?;
        let store = LogicStore::new(ep.store.schema());
        let m = store
            .module_by_id(&mut c, self.module_id)
            .await?
            .filter(|m| m.state == "active")
            .ok_or_else(|| {
                SprigError::new(
                    SprigCode::LogicNotFound,
                    format!("module {} is gone", self.module_id),
                )
            })?;
        let v = m.active_version.ok_or_else(|| {
            SprigError::new(
                SprigCode::LogicNotFound,
                format!("module {} has no active version", m.name),
            )
        })?;
        let (sig, _) = store.version_signature(&mut c, self.module_id, v).await?;
        let params: Vec<String> = sig["params"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|p| p["name"].as_str().map(str::to_owned))
            .collect();
        *self.cached.lock().unwrap_or_else(PoisonError::into_inner) =
            Some((Instant::now(), v, params.clone()));
        Ok((v, params))
    }

    /// The last version read (0 before the first read).
    #[must_use]
    pub fn last(&self) -> u64 {
        self.cached
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map_or(0, |(_, v, _)| *v)
    }
}

/// Runs a compiled trigger version on its event's shard (see the parent
/// module): marker and procedure in one transaction, as `sprig_ctl`; the
/// entry procedure is `SQL SECURITY DEFINER`, so it runs with the
/// binding's definer's rights.
pub struct MariaApply {
    /// The tenant.
    pub tenant: TenantId,
    /// The binding.
    pub binding_id: u64,
    /// The module.
    pub module_id: u64,
    /// Where shards are.
    pub pools: Arc<dyn ShardPools>,
    /// The active version.
    pub versions: Arc<CatalogVersions>,
    /// `logic.trigger_budget_ms`.
    pub budget: Duration,
    /// Chapter 29 (RL37): asked before each attempt; an error (retryable)
    /// makes the event wait, neither run nor dead-lettered.
    pub guard: Option<Arc<dyn RunGuard>>,
}

/// Whether a binding's code may run now (chapter 29, RL37: row-level
/// security on the trigger's table mustn't bind its definer, whose code
/// gets every changed row's images).
#[async_trait]
pub trait RunGuard: Send + Sync {
    /// `Ok` to run.
    ///
    /// # Errors
    /// Why not, retryable: the lane retries with backoff.
    async fn check(&self) -> Result<()>;

    /// The definer's `@sprig_rls` context (RL42), set on the shard
    /// connection while the code runs, so MariaDB's policy views and write
    /// triggers apply the definer's policies instead of treating the run as
    /// internal; `None` for none.
    ///
    /// # Errors
    /// Why it can't be built (retryable).
    async fn context(&self) -> Result<Option<String>> {
        Ok(None)
    }
}

/// An error's number.
fn errno(e: &SprigError) -> u16 {
    match e.kind {
        sprig_common::ErrorKind::Backend { errno, .. }
        | sprig_common::ErrorKind::Standard { errno, .. } => errno,
        sprig_common::ErrorKind::Sprig(c) => c.number(),
    }
}

/// Errors a later attempt may not see: lost connections, lock waits,
/// deadlocks, a read-only (held or fenced) server, a missing table during
/// a move (the marker table renamed by the cutover's fence).
fn transient(e: &SprigError) -> bool {
    e.retryable
        || matches!(errno(e), 1205 | 1213 | 1290 | 1836 | 2006 | 2013 | 1927 | 39030)
        // The marker table is renamed while a move holds the shard.
        || (errno(e) == 1146 && e.message.contains("__sprig_ldone"))
}

impl MariaApply {
    async fn attempt(
        &self,
        c: &mut Conn,
        ev: &TriggerEvent,
        version: u64,
        params: &[String],
    ) -> Result<Applied> {
        let schema = quote(&ev.schema(self.tenant));
        c.query_drop("START TRANSACTION").await.map_err(conn_err)?;
        c.exec_drop(
            format!(
                "INSERT IGNORE INTO {schema}.`__sprig_ldone` (binding_id, event_key, done_at) \
                 VALUES (?, ?, UTC_TIMESTAMP(6))"
            ),
            (self.binding_id, ev.key.as_bytes().to_vec()),
        )
        .await
        .map_err(conn_err)?;
        if c.affected_rows() == 0 {
            c.query_drop("ROLLBACK").await.map_err(conn_err)?;
            return Ok(Applied::AlreadyDone);
        }
        let mut args = ev.args(params);
        args.push("@__sprig_e".into());
        args.push("@__sprig_r".into());
        let secs = self.budget.as_secs_f64().max(0.001);
        c.query_drop(format!(
            "SET STATEMENT max_statement_time = {secs} FOR CALL {schema}.`__sprig_l{}_v{version}`({})",
            self.module_id,
            args.join(", ")
        ))
        .await
        .map_err(conn_err)?;
        let (event, result): (Option<String>, Option<String>) = c
            .query_first("SELECT @__sprig_e, @__sprig_r")
            .await
            .map_err(conn_err)?
            .unwrap_or((None, None));
        if matches!(event.as_deref(), Some("error" | "reject")) {
            let msg = result
                .as_deref()
                .and_then(|r| serde_json::from_str::<serde_json::Value>(r).ok())
                .and_then(|v| v["message"].as_str().map(str::to_owned))
                .unwrap_or_else(|| "the trigger raised its error event".to_owned());
            return Err(SprigError::new(SprigCode::LogicUserError, msg));
        }
        c.query_drop("COMMIT").await.map_err(conn_err)?;
        Ok(Applied::Ran(version))
    }
}

#[async_trait]
impl Apply for MariaApply {
    async fn apply(&self, ev: &TriggerEvent) -> Result<Applied> {
        // The version first: reading it takes a catalog connection, which
        // must never be awaited while this attempt holds a shard's.
        let (version, params) = self.versions.active().await?;
        let retry = |mut e: SprigError| {
            e.retryable = true;
            e
        };
        let ctx = match &self.guard {
            Some(g) => {
                g.check().await.map_err(retry)?;
                g.context().await.map_err(retry)?
            }
            None => None,
        };
        let pool = self.pools.pool(self.tenant).await?;
        let mut c = pool.get_conn().await.map_err(|e| {
            SprigError::new(
                SprigCode::BackendUnavailable,
                "the tenant's cluster is unreachable",
            )
            .with_source(e)
        })?;
        if let Some(x) = &ctx {
            c.exec_drop("SET @sprig_rls = ?", (x.as_str(),))
                .await
                .map_err(conn_err)?;
        }
        let r = self.attempt(&mut c, ev, version, &params).await;
        // The pooled sprig_ctl connection must not keep the definer's
        // context: other internal work on it would be checked as the
        // definer. A connection that can't be reset is closed.
        if ctx.is_some() {
            if r.is_err() {
                let _ = c.query_drop("ROLLBACK").await;
            }
            if c.query_drop("SET @sprig_rls = NULL").await.is_err() {
                let _ = c.disconnect().await;
                return r.map_err(|mut e| {
                    if transient(&e) {
                        e.retryable = true;
                    }
                    e
                });
            }
        }
        match r {
            Ok(a) => Ok(a),
            Err(mut e) => {
                let _ = c.query_drop("ROLLBACK").await;
                if errno(&e) == 1969 {
                    e = SprigError::new(
                        SprigCode::LogicBudgetExceeded,
                        "logic.trigger_budget_ms: the trigger ran out of time",
                    );
                }
                if transient(&e) {
                    e.retryable = true;
                }
                Err(e)
            }
        }
    }

    fn version(&self) -> u64 {
        self.versions.last()
    }
}

/// Dead letters and logs in the tenant's catalog, and the checkpoint in
/// the subscription.
pub struct CatalogOutcomes {
    /// The tenant's catalog.
    pub locator: Arc<dyn CatalogLocator>,
    /// The tenant.
    pub tenant: TenantId,
    /// The binding.
    pub binding_id: u64,
    /// The module (for log lines).
    pub module_id: u64,
    /// The subscriptions (the tenant's home DC tier).
    pub subs: Subscriptions,
}

impl CatalogOutcomes {
    async fn catalog(&self) -> Result<(Conn, LogicStore)> {
        let ep = self.locator.writer(self.tenant).await?;
        let c = ep.pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
        })?;
        Ok((c, LogicStore::new(ep.store.schema())))
    }
}

#[async_trait]
impl Outcomes for CatalogOutcomes {
    async fn dead_letter(
        &self,
        ev: &TriggerEvent,
        version: u64,
        attempts: u32,
        err: &SprigError,
    ) -> Result<()> {
        let (mut c, store) = self.catalog().await?;
        let token = ev
            .token
            .as_ref()
            .map(ResumeToken::encode)
            .unwrap_or_default();
        store
            .put_dead_letter(
                &mut c,
                self.binding_id,
                version,
                &ev.key,
                &token,
                &ev.to_json(),
                attempts,
                u32::from(errno(err)),
                &err.message,
            )
            .await
    }

    async fn ack(&self, token: &ResumeToken, events: u64) -> Result<()> {
        self.subs
            .ack(
                self.tenant,
                &subscription_name(self.binding_id),
                token,
                events,
                now_ms(),
            )
            .await
    }

    async fn log(&self, level: &str, message: &str) {
        match self.catalog().await {
            Ok((mut c, store)) => {
                if let Err(e) = store
                    .log(&mut c, self.module_id, None, level, "trigger", message)
                    .await
                {
                    tracing::warn!(error = %e, "can't write logic_log");
                }
            }
            Err(e) => tracing::warn!(error = %e, "can't write logic_log"),
        }
    }
}

/// Unix ms now.
#[must_use]
pub fn now_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

/// Registers a binding's subscription with its first checkpoint at
/// `pos` on `cluster` (`@@gtid_binlog_pos` read at deploy), so the job
/// starts with the first change committed after the deploy, whenever it
/// first runs. A subscription that exists is left as it is (a new
/// version of the trigger continues where the old one was).
///
/// # Errors
/// The store's errors; 39801 for a bad position.
pub async fn register(
    dc: Arc<dyn MetaStore>,
    tenant: TenantId,
    binding_id: u64,
    module_id: u64,
    cluster: ClusterId,
    pos: &str,
) -> Result<bool> {
    let subs = Subscriptions::new(Arc::clone(&dc));
    let name = subscription_name(binding_id);
    if subs.get(tenant, &name).await.is_ok() {
        return Ok(false);
    }
    let history = sprig_feed::ownership::history(&dc, tenant, 0).await?;
    let segment = history.last().map_or(0, |h| h.map_version);
    let token = ResumeToken {
        tenant,
        clamp: 0,
        cluster,
        segment,
        pos: pos.parse()?,
        until: None,
        progress: true,
    };
    match subs
        .register(&Subscription {
            tenant,
            name: name.clone(),
            kind: "logic".into(),
            ordering: "merged".into(),
            owner_ref: Some(format!("module:{module_id}")),
            state: "active".into(),
            created_at_ms: now_ms(),
        })
        .await
    {
        Ok(()) => {}
        Err(e) if e.code() == Some(SprigCode::FeedSubscriptionExists) => return Ok(false),
        Err(e) => return Err(e),
    }
    subs.ack(tenant, &name, &token, 0, now_ms()).await?;
    Ok(true)
}

/// Runs the binding's dead letters marked `replaying`, oldest first, with
/// the active version: `replayed` when it ran (or a marker says it had),
/// back to `dead` with one more attempt when it failed. Returns how many
/// were replayed.
///
/// # Errors
/// Catalog errors.
pub async fn replay(
    locator: &dyn CatalogLocator,
    tenant: TenantId,
    binding_id: u64,
    module_id: u64,
    apply: &dyn Apply,
) -> Result<u64> {
    let ep = locator.writer(tenant).await?;
    let mut c = ep.pool.get_conn().await.map_err(|e| {
        SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
    })?;
    let store = LogicStore::new(ep.store.schema());
    let rows = store
        .dead_letters(&mut c, Some(binding_id), Some("replaying"), 100)
        .await?;
    let mut n = 0;
    for r in rows {
        let ev = TriggerEvent::from_json(&r.event)?;
        match apply.apply(&ev).await {
            Ok(_) => {
                store
                    .set_dead_letters(
                        &mut c,
                        Some(binding_id),
                        &[r.dl_id],
                        "replaying",
                        "replayed",
                    )
                    .await?;
                let _ = store
                    .log(
                        &mut c,
                        module_id,
                        Some(apply.version()),
                        "info",
                        "trigger",
                        &format!("dead letter {} (event {}) replayed", r.dl_id, r.event_key),
                    )
                    .await;
                n += 1;
            }
            Err(e) => {
                store
                    .put_dead_letter(
                        &mut c,
                        binding_id,
                        apply.version(),
                        &r.event_key,
                        &[],
                        &r.event,
                        1,
                        u32::from(errno(&e)),
                        &e.message,
                    )
                    .await?;
                // Keep key order: stop at the first failure.
                break;
            }
        }
    }
    Ok(n)
}

/// Deletes markers of `binding_id` older than `hours` from one shard
/// schema, at most 10,000 at a time.
///
/// # Errors
/// The server's error.
pub async fn purge_markers(c: &mut Conn, schema: &str, binding_id: u64, hours: u64) -> Result<u64> {
    c.exec_drop(
        format!(
            "DELETE FROM {}.`__sprig_ldone` WHERE binding_id = ? AND done_at < UTC_TIMESTAMP(6) - INTERVAL ? HOUR LIMIT 10000",
            quote(schema)
        ),
        (binding_id, hours),
    )
    .await
    .map_err(conn_err)?;
    Ok(c.affected_rows())
}
