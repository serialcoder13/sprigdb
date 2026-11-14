//! `AUTO_INCREMENT` ID blocks in the gateway: per tenant, one
//! `sprig_shard::ids::BlockAllocator` over the tenant catalog's
//! `sequences` rows, shared by the gateway's sessions.
//!
//! ```text
//! INSERT into a sharded table with an AUTO_INCREMENT column → Plan::NeedIds
//!   the table's sequences row (seq_id = its object id) missing → create it:
//!     next_value = the highest of MAX(column) + 1 and the AUTO_INCREMENT
//!     counter over the group's shards (a duplicate create means another
//!     gateway made it first)
//!   take(rows) from the block (consecutive, in the session's
//!     auto_increment_increment and _offset series); raise past the highest
//!     explicit value
//!   IdFill::finish → the plan, run through the router's executor; its OK
//!     packet and the session's LAST_INSERT_ID() get the first value
//! ```

use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use mysql_async::prelude::Queryable;
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};
use sprig_router::plan::IdFill;
use sprig_shard::ids::{
    AutoIncSession, BLOCK_LEASE, BlockAllocator, BlockConfig, CatalogSequences, Explicit,
    IdAllocator, IdRun, Ordering, SeqId, SequenceSource,
};

use crate::tenant::locator::GatewayLocator;

fn unavailable(e: &SprigError) -> SprigError {
    SprigError::new(
        SprigCode::IdBlockUnavailable,
        format!("the catalog can't hand out an ID block: {e}"),
    )
}

/// The tenant catalog's `sequences`, through the catalog cluster's pool of
/// the moment (a failover replaces the pool).
pub(super) struct GwSequences {
    locator: Arc<GatewayLocator>,
    tenant: TenantId,
    /// This gateway process (its coordinator id): the holder of the
    /// blocks it fetches.
    holder: String,
}

impl GwSequences {
    async fn catalog(&self) -> Result<CatalogSequences> {
        let cluster = match self.locator.cached_cluster(self.tenant) {
            Some(c) => c,
            None => self
                .locator
                .cluster(self.tenant)
                .await
                .map_err(|e| unavailable(&e))?,
        };
        let pool = self
            .locator
            .ctl_pool(&cluster)
            .await
            .map_err(|e| unavailable(&e))?;
        Ok(
            CatalogSequences::new(pool, SchemaName::catalog(self.tenant).to_string())
                .held_by(self.holder.clone(), BLOCK_LEASE),
        )
    }
}

#[async_trait::async_trait]
impl SequenceSource for GwSequences {
    async fn fetch(&self, seq: SeqId, size: u128) -> Result<(i128, i128)> {
        let started = Instant::now();
        let r = self.catalog().await?.fetch(seq, size).await;
        metrics::histogram!("sprig_ids_fetch_duration_seconds")
            .record(started.elapsed().as_secs_f64());
        if r.is_ok() {
            metrics::counter!("sprig_ids_blocks_fetched_total", "kind" => "auto_increment")
                .increment(1);
        }
        r
    }

    async fn raise(&self, seq: SeqId, v: i128) -> Result<()> {
        self.catalog().await?.raise(seq, v).await
    }

    async fn ordering(&self, seq: SeqId) -> Result<Ordering> {
        self.catalog().await?.ordering(seq).await
    }

    fn lease(&self) -> Option<std::time::Duration> {
        Some(BLOCK_LEASE)
    }

    async fn explicit(&self, seq: SeqId, v: i128) -> Result<Explicit> {
        let r = self.catalog().await?.explicit(seq, v).await;
        if matches!(r, Ok(Explicit::Reserved { .. })) {
            metrics::counter!("sprig_ids_reserved_refusals_total").increment(1);
        }
        r
    }
}

/// One tenant's ID blocks in this gateway.
pub(super) struct TenantIds {
    alloc: BlockAllocator<GwSequences>,
    /// Sequences whose row is known to exist.
    ready: Mutex<HashSet<u64>>,
    /// `sequence_blocks` is known to exist.
    tables: std::sync::atomic::AtomicBool,
    /// One creation at a time in this gateway.
    creating: tokio::sync::Mutex<()>,
}

impl TenantIds {
    pub(super) fn new(locator: Arc<GatewayLocator>, tenant: TenantId, holder: String) -> Self {
        Self {
            alloc: BlockAllocator::new(
                GwSequences {
                    locator,
                    tenant,
                    holder,
                },
                BlockConfig::default(),
            ),
            ready: Mutex::new(HashSet::new()),
            tables: std::sync::atomic::AtomicBool::new(false),
            creating: tokio::sync::Mutex::new(()),
        }
    }

    /// The statement's generated values (`None` when no row takes one),
    /// after the sequence past its highest explicit value.
    ///
    /// # Errors
    /// 39460 when the catalog can't hand out or raise a block; 1467 when
    /// the sequence is exhausted.
    pub(super) async fn take(&self, f: &IdFill, s: &AutoIncSession) -> Result<Option<IdRun>> {
        self.ensure(f).await?;
        let seq = SeqId(f.seq);
        let run = if f.rows.is_empty() {
            None
        } else {
            Some(self.alloc.take(seq, f.rows.len() as u64, s).await?)
        };
        if let Some(v) = f.explicit_max {
            self.alloc.explicit(seq, v).await?;
        }
        Ok(run)
    }

    /// Makes sure the sequence's row exists (created at first use, from the
    /// shards' current values).
    async fn ensure(&self, f: &IdFill) -> Result<()> {
        let known = |s: &Self| {
            s.ready
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .contains(&f.seq)
        };
        if known(self) {
            return Ok(());
        }
        let _one = self.creating.lock().await;
        if known(self) {
            return Ok(());
        }
        let src = &self.alloc.source();
        let cat = src.catalog().await?;
        if !self.tables.load(std::sync::atomic::Ordering::Relaxed) {
            // A catalog migrated before the leased blocks' table.
            cat.ensure_tables().await.map_err(|e| unavailable(&e))?;
            self.tables
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        if !cat.exists(SeqId(f.seq)).await? {
            let next = first_value(src, f).await?;
            match cat
                .create(SeqId(f.seq), f.seq, Some(&f.column), next, Ordering::Blocks)
                .await
            {
                Ok(()) => {
                    tracing::info!(
                        tenant = %src.tenant,
                        table = %f.table,
                        column = %f.column,
                        next,
                        "ID-block sequence created"
                    );
                }
                // Another gateway created it first.
                Err(e) if matches!(e.kind, ErrorKind::Backend { errno: 1062, .. }) => {}
                Err(e) => return Err(unavailable(&e)),
            }
        }
        self.ready
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(f.seq);
        Ok(())
    }
}

/// The first value a new sequence hands out: above every value on every
/// shard of the table, and above each shard's `AUTO_INCREMENT` counter
/// (MariaDB never reuses a value it handed out, even for a deleted row).
async fn first_value(src: &GwSequences, f: &IdFill) -> Result<i128> {
    let q = |s: &str| format!("`{}`", s.replace('`', "``"));
    let mut next: i128 = 1;
    for sh in &f.group.shards {
        let cluster = ClusterId::new(&sh.cluster)
            .map_err(|e| SprigError::internal(format!("cluster id {}: {e}", sh.cluster)))?;
        let pool = src
            .locator
            .ctl_pool(&cluster)
            .await
            .map_err(|e| unavailable(&e))?;
        let mut c = pool
            .get_conn()
            .await
            .map_err(|e| unavailable(&SprigError::internal(e.to_string())))?;
        let sql = format!(
            "SELECT CAST(GREATEST(COALESCE((SELECT MAX({col}) FROM {schema}.{table}), 0) + 1, \
                    COALESCE((SELECT AUTO_INCREMENT FROM information_schema.TABLES \
                              WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?), 1)) AS CHAR)",
            col = q(&f.column),
            schema = q(&sh.schema),
            table = q(&f.table),
        );
        let v: Option<String> = c
            .exec_first(sql, (sh.schema.as_str(), f.table.as_str()))
            .await
            .map_err(|e| unavailable(&SprigError::internal(e.to_string())))?;
        let v: i128 = v
            .as_deref()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| SprigError::internal("a shard's AUTO_INCREMENT value didn't parse"))?;
        next = next.max(v);
    }
    Ok(next)
}
