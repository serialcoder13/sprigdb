//! `kv_sweep` (chapter 33, "Expiry and garbage"; chapter 40, "Expiry: lazy
//! and active"; R6): removes expired key rows and their elements, and old
//! generations' elements, on every cluster of the DC as `sprig_ctl`.
//!
//! ```text
//! per cluster, in its own task, until cancelled:
//!   namespaces = sprig_kv::sweep::discover()
//!   each namespace: expired keys, batch by batch (SKIP LOCKED passes clients' keys)
//!   every orphan_scan_hours (0: every pass): orphaned element generations
//!   sleep until the next expiry, at most purge_interval_seconds
//! ```
//!
//! The controller's `kv_sweep` loop keeps one such job in the DC. The
//! sweeper is idempotent: a second copy after a lost lease deletes nothing
//! twice.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use serde_json::{Value, json};
use sprig_controller::catalogs::ClusterPools;
use sprig_controller::dispatch::JobClass;
use sprig_kv::sweep::{self, SweepOpts, Swept};
use sprig_kv::{Row, SqlError, SqlRunner};

use crate::runtime::{JobCtx, JobHandler};

/// The job's kind.
pub const JOB_KIND: &str = "kv_sweep";

/// A `sprig_ctl` connection as the sweeper's SQL.
struct Ctl(mysql_async::Conn);

fn sql_err(e: &mysql_async::Error) -> SqlError {
    match e {
        mysql_async::Error::Server(s) => SqlError {
            errno: Some(s.code),
            message: s.message.clone(),
        },
        other => SqlError {
            errno: None,
            message: other.to_string(),
        },
    }
}

#[async_trait]
impl SqlRunner for Ctl {
    async fn query(&mut self, sql: &str) -> Result<Vec<Row>, SqlError> {
        let rows: Vec<mysql_async::Row> = self.0.query(sql).await.map_err(|e| sql_err(&e))?;
        Ok(rows
            .into_iter()
            .map(|r| {
                r.unwrap()
                    .into_iter()
                    .map(|v| match v {
                        mysql_async::Value::NULL => None,
                        mysql_async::Value::Bytes(b) => Some(b),
                        other => Some(other.as_sql(true).into_bytes()),
                    })
                    .collect()
            })
            .collect())
    }

    async fn exec(&mut self, sql: &str) -> Result<u64, SqlError> {
        self.0.query_drop(sql).await.map_err(|e| sql_err(&e))?;
        Ok(self.0.affected_rows())
    }
}

/// The sweeper's settings: from the worker's live settings at each pass
/// when it has them, else from the job's spec (what the controller
/// resolved when it submitted the job).
#[derive(Debug, Clone, Copy)]
struct Opts {
    interval: Duration,
    orphan_every: Duration,
    sweep: SweepOpts,
}

impl Opts {
    /// The `kv.*` keys as they resolve now (dc, global).
    fn now(spec: &Value, settings: Option<&crate::settings::WorkerSettings>) -> Self {
        use sprig_controller::kv_sweep_loop as k;
        let Some(s) = settings else {
            return Self::of(spec);
        };
        Self {
            interval: Duration::from_secs(s.get(&k::PURGE_INTERVAL_SECONDS).max(1)),
            orphan_every: Duration::from_secs(s.get(&k::ORPHAN_SCAN_HOURS) * 3600),
            sweep: SweepOpts {
                batch_rows: s.get(&k::PURGE_BATCH_ROWS).max(1),
                inline_max: s.get(&k::INLINE_DELETE_MAX_ELEMENTS),
            },
        }
    }

    fn of(spec: &Value) -> Self {
        let n = |k: &str, d: u64| spec[k].as_u64().unwrap_or(d);
        Self {
            interval: Duration::from_secs(n("purge_interval_seconds", 10).max(1)),
            orphan_every: Duration::from_secs(n("orphan_scan_hours", 24) * 3600),
            sweep: SweepOpts {
                batch_rows: n("purge_batch_rows", 500).max(1),
                inline_max: n("inline_delete_max_elements", 128),
            },
        }
    }
}

/// See the module docs.
#[derive(Clone)]
pub struct KvSweep {
    /// `sprig_ctl` pools per cluster.
    pub pools: Arc<dyn ClusterPools>,
    /// The worker's live settings; `None` reads the job's spec.
    pub settings: Option<crate::settings::WorkerSettings>,
}

impl std::fmt::Debug for KvSweep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KvSweep").finish_non_exhaustive()
    }
}

/// One pass over a cluster; the time until its next expiry.
async fn pass(
    sql: &mut dyn SqlRunner,
    o: Opts,
    orphans: bool,
    total: &mut Swept,
    orphan_rows: &mut u64,
) -> Result<Option<Duration>, SqlError> {
    let mut next: Option<Duration> = None;
    for ns in sweep::discover(sql).await? {
        let s = sweep::expired(sql, &ns, o.sweep).await?;
        if s.keys > 0 || s.elements > 0 {
            metrics::counter!("sprig_kv_purged_keys_total").increment(s.keys);
            tracing::debug!(schema = %ns.schema, table = %ns.table, keys = s.keys, elements = s.elements, "expired keys swept");
        }
        *total += s;
        if orphans {
            loop {
                let (rows, gens) = sweep::orphans(sql, &ns, o.sweep).await?;
                *orphan_rows += rows;
                metrics::counter!("sprig_kv_orphan_elements_deleted_total").increment(rows);
                if gens < o.sweep.batch_rows {
                    break;
                }
            }
        }
        if let Some(ms) = sweep::next_expiry_ms(sql, &ns).await? {
            let d = Duration::from_millis(u64::try_from(ms).unwrap_or(0));
            next = Some(next.map_or(d, |n| n.min(d)));
        }
    }
    Ok(next)
}

#[async_trait]
impl JobHandler for KvSweep {
    fn kind(&self) -> &'static str {
        JOB_KIND
    }

    fn class(&self) -> JobClass {
        JobClass::Consumer
    }

    async fn run(&self, cx: &JobCtx) -> Result<Value, String> {
        let clusters = self.pools.sweepable().await.map_err(|e| e.to_string())?;
        let mut tasks = Vec::new();
        for cluster in clusters {
            let pools = Arc::clone(&self.pools);
            let cancel = cx.cancel.clone();
            let (spec, settings) = (cx.spec.clone(), self.settings.clone());
            tasks.push(tokio::spawn(async move {
                let mut total = Swept::default();
                let mut orphan_rows = 0u64;
                let mut last_orphans: Option<Instant> = None;
                while !cancel.is_cancelled() {
                    // `kv.*` as they resolve now, at every pass.
                    let o = Opts::now(&spec, settings.as_ref());
                    let wait = match pools.writer(&cluster).await {
                        Ok(pool) => match pool.get_conn().await {
                            Ok(conn) => {
                                let mut sql = Ctl(conn);
                                let orphans =
                                    last_orphans.is_none_or(|t| t.elapsed() >= o.orphan_every);
                                match pass(&mut sql, o, orphans, &mut total, &mut orphan_rows).await
                                {
                                    Ok(next) => {
                                        if orphans {
                                            last_orphans = Some(Instant::now());
                                        }
                                        next.map_or(o.interval, |n| n.min(o.interval))
                                    }
                                    Err(e) => {
                                        tracing::warn!(%cluster, error = %e, "key-value sweep failed");
                                        o.interval
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::debug!(%cluster, error = %e, "sweeper: no connection");
                                o.interval
                            }
                        },
                        Err(e) => {
                            tracing::debug!(%cluster, error = %e, "sweeper: no writer");
                            o.interval
                        }
                    };
                    // A key due now is swept at once; never spin faster
                    // than every 50 ms.
                    let wait = wait.max(Duration::from_millis(50));
                    tokio::select! {
                        () = cancel.cancelled() => {}
                        () = tokio::time::sleep(wait) => {}
                    }
                }
                (cluster, total, orphan_rows)
            }));
        }
        let mut out = Vec::new();
        for t in tasks {
            if let Ok((c, s, orphans)) = t.await {
                out.push(json!({"cluster": c.to_string(), "keys": s.keys, "elements": s.elements, "orphan_elements": orphans}));
            }
        }
        Ok(json!({"end": "cancelled", "clusters": out}))
    }
}
