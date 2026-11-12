//! Sinks over a change topic whose owner a table's row-level security
//! binds (chapter 29, "Change feed consumers"; RLS round 5, decision RL44).
//!
//! ```text
//! each batch the stream source reads:
//!   the owner isn't bound (SinkEnv::row_policies_bind): delivered as read
//!   bound: the topic's SQL read over the batch's offsets, per partition, in
//!     one read-only transaction of a session of the owner on a gateway
//!     (SinkSql Begin, Read..., End): the records it returns are delivered
//!     with its values (an update whose row became visible or hidden reads
//!     as an insert or a delete); the others are left out
//! ```
//!
//! Records left out are never delivered or dead-lettered; their offsets are
//! settled with the batch's next delivered record. The check and the read
//! happen at every batch, so a policy, role or grant change applies from
//! the next batch.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};

use crate::job::SinkEnv;
use crate::model::{Position, SinkRecord};
use crate::peer::{RunAs, SinkPeer, SqlOp, SqlResult};
use crate::runtime::SourceReader;

/// A stream source filtered for its sink's owner.
pub struct FilteredChanges {
    /// The stream source.
    pub inner: Box<dyn SourceReader>,
    /// For the owner check.
    pub env: Arc<dyn SinkEnv>,
    /// The tenant.
    pub tenant: TenantId,
    /// The sink's owner.
    pub owner: u32,
    /// The change topic's table.
    pub object: u64,
    /// `` `db`.`t.changes` ``.
    pub topic: String,
    /// The owner, for the gateway's session.
    pub run_as: Option<RunAs>,
    /// The gateways.
    pub peer: Option<Arc<dyn SinkPeer>>,
    /// Picks the gateway (the sink's id).
    pub seed: u64,
}

impl std::fmt::Debug for FilteredChanges {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilteredChanges")
            .field("topic", &self.topic)
            .finish_non_exhaustive()
    }
}

fn unsupported(why: &str) -> SprigError {
    let mut e = SprigError::new(
        SprigCode::RlsUnsupported,
        format!(
            "ER_SPRIG_RLS_UNSUPPORTED: the sink's owner is bound by row-level security and {why}"
        ),
    );
    e.retryable = true;
    e
}

/// The read of one partition's offsets `from..=to` of `topic`.
#[must_use]
pub fn read_sql(topic: &str, partition: u32, from: u64, to: u64) -> String {
    format!(
        "SELECT offset_no, `value` FROM {topic} WHERE partition_no = {partition} AND offset_no \
         BETWEEN {from} AND {to}"
    )
}

/// `records` kept and rewritten by `seen` ((partition, offset) → value):
/// the others are left out.
#[must_use]
#[allow(clippy::implicit_hasher)] // the filter's own map
pub fn apply(
    records: Vec<SinkRecord>,
    seen: &HashMap<(u32, u64), Option<Vec<u8>>>,
) -> Vec<SinkRecord> {
    records
        .into_iter()
        .filter_map(|mut r| {
            let Position::Stream {
                partition, offset, ..
            } = r.pos
            else {
                return None;
            };
            let v = seen.get(&(partition, offset))?;
            r.value.clone_from(v);
            Some(r)
        })
        .collect()
}

/// `` `db`.`name` `` of stream `stream` from the tenant's catalog
/// (`catalog`), for its SQL read.
///
/// # Errors
/// The catalog's; 39221-style source lost when the stream is gone.
pub async fn topic_sql(pool: &mysql_async::Pool, catalog: &str, stream: u64) -> Result<String> {
    topic_of(pool, catalog, stream).await.map(|(t, _)| t)
}

/// [`topic_sql`] and the change topic's table (`None` for an event
/// stream).
///
/// # Errors
/// As [`topic_sql`].
pub async fn topic_of(
    pool: &mysql_async::Pool,
    catalog: &str,
    stream: u64,
) -> Result<(String, Option<u64>)> {
    use mysql_async::prelude::Queryable;
    let mut c = pool.get_conn().await.map_err(|e| {
        SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e)
    })?;
    let def = sprig_streams::StreamsStore::new(catalog)
        .get(&mut c, sprig_streams::StreamId(stream))
        .await?
        .ok_or_else(|| {
            SprigError::new(
                SprigCode::SinkSourceLost,
                format!("stream {stream} is gone"),
            )
        })?;
    let db: Option<String> = c
        .exec_first(
            format!(
                "SELECT name FROM {}.`databases` WHERE db_id = ?",
                sprig_common::names::quote(catalog)
            ),
            (def.db.0,),
        )
        .await
        .map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e)
        })?;
    let db = db.ok_or_else(|| {
        SprigError::new(SprigCode::SinkSourceLost, "the stream's database is gone")
    })?;
    Ok((
        format!(
            "{}.{}",
            sprig_common::names::quote(&db),
            sprig_common::names::quote(&def.name)
        ),
        def.source_object.map(|o| o.0),
    ))
}

/// The owner's view of `records` through `topic`'s SQL read.
async fn filter(
    peer: Option<Arc<dyn SinkPeer>>,
    run_as: Option<RunAs>,
    seed: u64,
    topic: String,
    records: Vec<SinkRecord>,
) -> Result<Vec<SinkRecord>> {
    {
        let (Some(peer), Some(run_as)) = (&peer, &run_as) else {
            return Err(unsupported(
                "this worker has no gateway to filter its records",
            ));
        };
        let mut ranges: BTreeMap<u32, (u64, u64)> = BTreeMap::new();
        for r in &records {
            if let Position::Stream {
                partition, offset, ..
            } = r.pos
            {
                let e = ranges.entry(partition).or_insert((offset, offset));
                e.0 = e.0.min(offset);
                e.1 = e.1.max(offset);
            }
        }
        let begin = peer.sql(seed, &run_as.request(SqlOp::Begin)).await?.ok()?;
        let handle = begin
            .handle
            .ok_or_else(|| SprigError::internal("SinkSql Begin answered no handle"))?;
        let mut shown: HashMap<(u32, u64), Option<Vec<u8>>> = HashMap::new();
        let mut failed = None;
        for (p, (from, to)) in &ranges {
            let req = run_as.request(SqlOp::Read {
                handle: handle.clone(),
                sql: read_sql(&topic, *p, *from, *to),
            });
            match peer.sql(seed, &req).await.and_then(|r| r.ok()) {
                Ok(reply) => {
                    for res in &reply.results {
                        for row in &res.rows {
                            let Some(o) =
                                SqlResult::text(row, 0).and_then(|t| t.parse::<u64>().ok())
                            else {
                                continue;
                            };
                            shown.insert((*p, o), SqlResult::bytes(row, 1));
                        }
                    }
                }
                Err(e) => {
                    failed = Some(e);
                    break;
                }
            }
        }
        let _ = peer.sql(seed, &run_as.request(SqlOp::End { handle })).await;
        if let Some(e) = failed {
            return Err(e);
        }
        let n = records.len();
        let out = apply(records, &shown);
        metrics::counter!("sprig_rls_feed_events_filtered_total", "protocol" => "sink")
            .increment((n - out.len()) as u64);
        Ok(out)
    }
}

#[async_trait]
impl SourceReader for FilteredChanges {
    async fn next_batch(
        &mut self,
        max_records: usize,
        max_bytes: u64,
        wait: Duration,
    ) -> Result<Vec<SinkRecord>> {
        let b = self.inner.next_batch(max_records, max_bytes, wait).await?;
        if b.is_empty()
            || !self
                .env
                .row_policies_bind(self.tenant, self.owner, &[self.object])
                .await?
        {
            return Ok(b);
        }
        filter(
            self.peer.clone(),
            self.run_as.clone(),
            self.seed,
            self.topic.clone(),
            b,
        )
        .await
    }

    async fn settle(&mut self, done: &[Position]) -> Result<()> {
        self.inner.settle(done).await
    }

    async fn reject(&mut self, rec: &SinkRecord, reason: &str) -> Result<bool> {
        self.inner.reject(rec, reason).await
    }

    async fn seek_after(&mut self, pos: &Position) -> Result<()> {
        self.inner.seek_after(pos).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(p: u32, o: u64) -> SinkRecord {
        SinkRecord {
            key: None,
            value: Some(b"raw".to_vec()),
            headers: Vec::new(),
            op: None,
            before: None,
            after: None,
            ts_ms: 0,
            idem_key: format!("{p}/{o}"),
            pos: Position::Stream {
                stream: 1,
                partition: p,
                offset: o,
                epoch: 0,
            },
        }
    }

    #[test]
    fn records_the_read_left_out_are_dropped() {
        let seen = HashMap::from([
            ((0, 1), Some(b"one".to_vec())),
            ((1, 7), Some(b"seven".to_vec())),
        ]);
        let out = apply(vec![rec(0, 1), rec(0, 2), rec(1, 7)], &seen);
        assert_eq!(
            out.iter()
                .map(|r| r.value.clone().unwrap_or_default())
                .collect::<Vec<_>>(),
            vec![b"one".to_vec(), b"seven".to_vec()]
        );
        assert_eq!(
            read_sql("`d`.`t.changes`", 3, 5, 9),
            "SELECT offset_no, `value` FROM `d`.`t.changes` WHERE partition_no = 3 AND offset_no BETWEEN 5 AND 9"
        );
    }
}
