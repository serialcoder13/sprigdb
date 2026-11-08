//! Load collection (chapter 11, "Load signals"; build step 2).
//!
//! - **Gateway reports.** Each gateway counts, per shard, statements, busy
//!   microseconds (statement latency), rows written and rows read, and
//!   sends the cumulative counters every `balancer.load_report_seconds`
//!   as a [`LoadReport`]. [`LoadAccumulator`] is the gateway side;
//!   [`LoadCollector`] the controller side, which turns counter deltas into
//!   rates and keeps an EWMA per shard over `balancer.window_minutes`.
//! - **Storage scans.** Shard sizes come from `mysql.innodb_table_stats`
//!   pages times `innodb_page_size`, one schema per query
//!   (`database_name = ?`), never `information_schema.TABLES`, which opens
//!   every table ([`scan_schema`]).
//!
//! The EWMA weight for a sample `dt` after the previous one is
//! `1 - exp(-dt / window)`, so irregular report times weigh correctly.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use mysql_async::prelude::Queryable;
use serde::{Deserialize, Serialize};
use sprig_common::ids::ClusterId;
use sprig_common::{Result, SprigCode, SprigError};

use crate::model::{LoadVector, ShardKey};

/// One gateway's cumulative counters for one shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardLoad {
    /// The shard.
    pub shard: ShardKey,
    /// The cluster the statements ran on.
    pub cluster: ClusterId,
    /// Statements.
    pub statements: u64,
    /// Statement latency summed, microseconds.
    pub busy_us: u64,
    /// Rows written.
    pub rows_written: u64,
    /// Rows read.
    pub rows_read: u64,
    /// Backend connections open to the shard now (a gauge).
    pub connections: u32,
}

/// One report from one gateway.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadReport {
    /// The gateway (its node id).
    pub gateway: String,
    /// When, milliseconds since the epoch.
    pub at_ms: u64,
    /// Every shard it served since it started.
    pub shards: Vec<ShardLoad>,
}

/// The gateway side: counters per shard, read every report period.
#[derive(Debug, Default)]
pub struct LoadAccumulator {
    inner: Mutex<BTreeMap<ShardKey, ShardLoad>>,
}

impl LoadAccumulator {
    /// An empty accumulator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Counts one statement.
    pub fn record(
        &self,
        shard: ShardKey,
        cluster: &ClusterId,
        busy: Duration,
        rows_written: u64,
        rows_read: u64,
    ) {
        let mut g = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let e = g.entry(shard).or_insert_with(|| ShardLoad {
            shard,
            cluster: cluster.clone(),
            statements: 0,
            busy_us: 0,
            rows_written: 0,
            rows_read: 0,
            connections: 0,
        });
        if &e.cluster != cluster {
            // The shard moved: counters continue, the cluster follows.
            e.cluster = cluster.clone();
        }
        e.statements += 1;
        e.busy_us += u64::try_from(busy.as_micros()).unwrap_or(u64::MAX);
        e.rows_written += rows_written;
        e.rows_read += rows_read;
    }

    /// Sets the open-connection gauge of a shard.
    pub fn connections(&self, shard: ShardKey, n: u32) {
        let mut g = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(e) = g.get_mut(&shard) {
            e.connections = n;
        }
    }

    /// The report to send now.
    #[must_use]
    pub fn report(&self, gateway: &str, at_ms: u64) -> LoadReport {
        let g = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        LoadReport {
            gateway: gateway.to_owned(),
            at_ms,
            shards: g.values().cloned().collect(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct Rate {
    busy_ms_per_sec: f64,
    write_rows_per_sec: f64,
    connections: f64,
}

#[derive(Debug, Clone)]
struct Last {
    counters: ShardLoad,
    at_ms: u64,
    rate: Rate,
}

/// The controller side: rates from counter deltas, an EWMA per shard.
#[derive(Debug)]
pub struct LoadCollector {
    window: Duration,
    last: HashMap<(String, ShardKey), Last>,
    ewma: HashMap<ShardKey, (LoadVector, u64)>,
    gateways: HashMap<String, u64>,
    cluster_of: HashMap<ShardKey, ClusterId>,
}

impl LoadCollector {
    /// A collector averaging over `window`.
    #[must_use]
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            last: HashMap::new(),
            ewma: HashMap::new(),
            gateways: HashMap::new(),
            cluster_of: HashMap::new(),
        }
    }

    /// Changes the window (`balancer.window_minutes` applies live).
    pub fn set_window(&mut self, w: Duration) {
        self.window = w;
    }

    /// Takes one report.
    #[allow(clippy::cast_precision_loss)]
    pub fn ingest(&mut self, r: &LoadReport) {
        self.gateways.insert(r.gateway.clone(), r.at_ms);
        for s in &r.shards {
            let key = (r.gateway.clone(), s.shard);
            self.cluster_of.insert(s.shard, s.cluster.clone());
            let rate = match self.last.get(&key) {
                Some(prev) if r.at_ms > prev.at_ms => {
                    let dt = (r.at_ms - prev.at_ms) as f64 / 1000.0;
                    // A restarted gateway's counters start again: no rate.
                    if s.busy_us < prev.counters.busy_us || s.rows_written < prev.counters.rows_written {
                        Rate {
                            connections: f64::from(s.connections),
                            ..Rate::default()
                        }
                    } else {
                        Rate {
                            busy_ms_per_sec: (s.busy_us - prev.counters.busy_us) as f64 / 1000.0 / dt,
                            write_rows_per_sec: (s.rows_written - prev.counters.rows_written) as f64
                                / dt,
                            connections: f64::from(s.connections),
                        }
                    }
                }
                Some(prev) => prev.rate,
                None => Rate {
                    connections: f64::from(s.connections),
                    ..Rate::default()
                },
            };
            let first = !self.last.contains_key(&key);
            self.last.insert(
                key,
                Last {
                    counters: s.clone(),
                    at_ms: r.at_ms,
                    rate,
                },
            );
            if !first {
                self.update_ewma(s.shard, r.at_ms);
            }
        }
    }

    /// Sums the latest rate of every gateway for `shard` and folds it in.
    #[allow(clippy::cast_precision_loss)]
    fn update_ewma(&mut self, shard: ShardKey, at_ms: u64) {
        let stale_ms = u64::try_from(self.window.as_millis()).unwrap_or(u64::MAX);
        let mut sum = Rate::default();
        for ((_, k), l) in &self.last {
            if *k == shard && l.at_ms + stale_ms >= at_ms {
                sum.busy_ms_per_sec += l.rate.busy_ms_per_sec;
                sum.write_rows_per_sec += l.rate.write_rows_per_sec;
                sum.connections += l.rate.connections;
            }
        }
        let sample = LoadVector {
            busy_ms_per_sec: sum.busy_ms_per_sec,
            write_rows_per_sec: sum.write_rows_per_sec,
            connections: sum.connections,
            ..LoadVector::default()
        };
        let window = self.window.as_secs_f64().max(1.0);
        let e = self.ewma.entry(shard).or_insert((sample, at_ms));
        if at_ms > e.1 {
            let dt = (at_ms - e.1) as f64 / 1000.0;
            let alpha = 1.0 - (-dt / window).exp();
            e.0 = e.0 + (sample - e.0).scale(alpha);
            e.1 = at_ms;
        }
    }

    /// A shard's averaged busy time, writes and connections (storage,
    /// schemas and tables come from scans and catalogs).
    #[must_use]
    pub fn shard_load(&self, shard: &ShardKey) -> LoadVector {
        self.ewma.get(shard).map(|e| e.0).unwrap_or_default()
    }

    /// Every shard with a load, and the cluster it was last reported on.
    #[must_use]
    pub fn shards(&self) -> Vec<(ShardKey, ClusterId, LoadVector)> {
        let mut v: Vec<_> = self
            .ewma
            .iter()
            .filter_map(|(k, e)| self.cluster_of.get(k).map(|c| (*k, c.clone(), e.0)))
            .collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }

    /// Gateways that reported within two periods of `now_ms`, over
    /// `expected` (chapter 11: no plan if under half arrived).
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn coverage(&self, now_ms: u64, period: Duration, expected: usize) -> f64 {
        if expected == 0 {
            return 1.0;
        }
        let horizon = u64::try_from(period.as_millis()).unwrap_or(u64::MAX).saturating_mul(2);
        let fresh = self
            .gateways
            .values()
            .filter(|t| **t + horizon >= now_ms)
            .count();
        fresh as f64 / expected as f64
    }

    /// Forgets gateways and shards not heard from since `before_ms`.
    pub fn forget_before(&mut self, before_ms: u64) {
        self.gateways.retain(|_, t| *t >= before_ms);
        self.last.retain(|_, l| l.at_ms >= before_ms);
        self.ewma.retain(|_, e| e.1 >= before_ms);
    }
}

/// A shard schema's size from `InnoDB`'s persistent statistics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaSize {
    /// Tables with statistics.
    pub tables: u64,
    /// Clustered and secondary index pages times the page size.
    pub bytes: u64,
}

/// The storage scan, one schema per statement.
pub const STORAGE_SQL: &str = "SELECT COUNT(*), \
    CAST(COALESCE(SUM(clustered_index_size + sum_of_other_index_sizes), 0) AS UNSIGNED) \
    FROM mysql.innodb_table_stats WHERE database_name = ?";

/// `@@innodb_page_size`.
///
/// # Errors
/// The driver's, as 39070.
pub async fn page_size<Q: Queryable + Send>(q: &mut Q) -> Result<u64> {
    let v: Option<u64> = q
        .query_first("SELECT @@innodb_page_size")
        .await
        .map_err(scan_err)?;
    Ok(v.unwrap_or(16_384))
}

/// The size of `schema` (statistics are refreshed by `InnoDB` in the
/// background, so a new table may show 0 pages for a while).
///
/// # Errors
/// The driver's, as 39070.
pub async fn scan_schema<Q: Queryable + Send>(q: &mut Q, schema: &str, page: u64) -> Result<SchemaSize> {
    let row: Option<(u64, u64)> = q.exec_first(STORAGE_SQL, (schema,)).await.map_err(scan_err)?;
    let (tables, pages) = row.unwrap_or((0, 0));
    Ok(SchemaSize {
        tables,
        bytes: pages.saturating_mul(page),
    })
}

fn scan_err(e: mysql_async::Error) -> SprigError {
    SprigError::new(SprigCode::CatalogUnavailable, "storage scan failed").with_source(e)
}

#[cfg(test)]
mod tests;
