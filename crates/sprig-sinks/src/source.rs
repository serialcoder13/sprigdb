//! Source readers (chapter 51, build step 6).
//!
//! - [`StreamSource`]: a stream or change topic, read from its log with
//!   `sprig-streams`' `LogReader`, positions in the consumer group
//!   `__sprig_sink_<id>` (`stream_offsets`), so Kafka tools and Studio show
//!   a sink's lag like any group's. A partition without a committed offset
//!   starts at `START AT` and commits that position at once, so a restart
//!   neither skips nor repeats records published meanwhile.
//! - Dead letters marked for replay are fed back first, with their
//!   original idempotency keys; they settle on their own (they are below
//!   the group's offsets).
//!
//! Not built: queue sources in workers (queue owners live in gateways
//! until the relay between them exists; the runtime's
//! `SourceReader::reject` is ready for them) and change-feed sources
//! (`CHANGES OF`; a change topic `t.changes` read as a stream gives the
//! same records).

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use mysql_async::Pool;
use sprig_common::{Result, SprigCode};
use sprig_streams::read::{LogReader, ReadLimits};
use sprig_streams::store::{CommittedOffset, StreamsStore};
use sprig_streams::{Offset, PartitionNo, StreamId};

use crate::model::{Position, SinkId, SinkRecord, StartAt, err, stream_key};
use crate::runtime::SourceReader;
use crate::store::SinksStore;

fn backend(e: mysql_async::Error) -> sprig_common::SprigError {
    let mut x =
        sprig_common::SprigError::new(SprigCode::CatalogUnavailable, "sink source unavailable")
            .with_source(e);
    x.retryable = true;
    x
}

/// A stream source for one task.
pub struct StreamSource {
    shard: Pool,
    catalog: Pool,
    catalog_schema: String,
    log: LogReader,
    /// The member whose instance `stream` is (a fan-in sink, R9); 0 for
    /// the sink's own tenant's stream.
    instance: u64,
    stream: StreamId,
    partitions: Vec<u32>,
    group: String,
    at: HashMap<u32, u64>,
    turn: usize,
    sink: SinkId,
    last_replay_check: Option<tokio::time::Instant>,
}

impl std::fmt::Debug for StreamSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamSource")
            .field("stream", &self.stream.0)
            .field("partitions", &self.partitions)
            .field("at", &self.at)
            .finish_non_exhaustive()
    }
}

impl StreamSource {
    /// Opens the source at its committed offsets, or at `start` for
    /// partitions without one (committing that position).
    ///
    /// # Errors
    /// The catalog's or the shard's.
    #[allow(clippy::too_many_arguments)]
    pub async fn open(
        shard: Pool,
        catalog: Pool,
        catalog_schema: String,
        log_schema: String,
        stream: StreamId,
        partitions: Vec<u32>,
        group: String,
        start: StartAt,
        sink: SinkId,
    ) -> Result<Self> {
        Self::open_instance(
            shard,
            catalog,
            catalog_schema,
            log_schema,
            0,
            stream,
            partitions,
            group,
            start,
            sink,
        )
        .await
    }

    /// [`Self::open`] over member `instance`'s instance `stream` (a fan-in
    /// sink, chapter 51): `shard` is the member's cluster, the positions
    /// are the sink's, in its own catalog, with `instance_tenant_id`.
    ///
    /// # Errors
    /// The catalog's or the shard's.
    #[allow(clippy::too_many_arguments)]
    pub async fn open_instance(
        shard: Pool,
        catalog: Pool,
        catalog_schema: String,
        log_schema: String,
        instance: u64,
        stream: StreamId,
        partitions: Vec<u32>,
        group: String,
        start: StartAt,
        sink: SinkId,
    ) -> Result<Self> {
        let log = LogReader { schema: log_schema };
        let store = StreamsStore::new(catalog_schema.clone());
        let mut c = catalog.get_conn().await.map_err(backend)?;
        let committed = store.group_offsets(&mut c, &group).await?;
        let mut sc = shard.get_conn().await.map_err(backend)?;
        let mut at = HashMap::new();
        let mut fresh = Vec::new();
        for p in &partitions {
            if let Some(o) = committed
                .iter()
                .find(|o| {
                    o.instance_tenant == instance && o.stream == stream && o.partition.0 == *p
                })
            {
                at.insert(*p, o.offset);
                continue;
            }
            let b = log.bounds(&mut sc, stream, PartitionNo(*p)).await?;
            let o = match start {
                StartAt::Earliest | StartAt::Snapshot => b.log_start.0,
                StartAt::Latest => b.high_watermark.0,
                StartAt::Timestamp { ms } => log
                    .offset_for_time(&mut sc, stream, PartitionNo(*p), Some(ms))
                    .await?
                    .map_or(b.high_watermark.0, |(o, _)| o.0),
            };
            at.insert(*p, o);
            fresh.push((*p, o));
        }
        for (p, o) in fresh {
            store
                .commit_offset(
                    &mut c,
                    &CommittedOffset {
                        group: group.clone(),
                        instance_tenant: instance,
                        stream,
                        partition: PartitionNo(p),
                        offset: o,
                        leader_epoch: None,
                        metadata: Some("sprig sink: start".into()),
                    },
                )
                .await?;
        }
        Ok(Self {
            shard,
            catalog,
            catalog_schema,
            log,
            instance,
            stream,
            partitions,
            group,
            at,
            turn: 0,
            sink,
            last_replay_check: None,
        })
    }

    async fn replays(&mut self) -> Result<Vec<SinkRecord>> {
        let now = tokio::time::Instant::now();
        if self
            .last_replay_check
            .is_some_and(|t| now.duration_since(t) < Duration::from_secs(3))
        {
            return Ok(Vec::new());
        }
        self.last_replay_check = Some(now);
        let store = SinksStore::new(self.catalog_schema.clone());
        let mut c = self.catalog.get_conn().await.map_err(backend)?;
        let mut v = store.take_replays(&mut c, self.sink, 500).await?;
        for r in &mut v {
            // Replays settle on their own: they are below the offsets.
            r.pos = Position::Queue {
                queue: 0,
                msg_id: format!("replay:{}", r.idem_key),
                delivery: String::new(),
            };
        }
        Ok(v)
    }
}

#[async_trait]
impl SourceReader for StreamSource {
    async fn next_batch(
        &mut self,
        max_records: usize,
        max_bytes: u64,
        wait: Duration,
    ) -> Result<Vec<SinkRecord>> {
        let mut out = self.replays().await?;
        if self.partitions.is_empty() {
            tokio::time::sleep(wait).await;
            return Ok(out);
        }
        let mut c = self.shard.get_conn().await.map_err(backend)?;
        let n = self.partitions.len();
        for i in 0..n {
            if out.len() >= max_records {
                break;
            }
            let p = self.partitions[(self.turn + i) % n];
            let from = self.at.get(&p).copied().unwrap_or(0);
            let r = self
                .log
                .read(
                    &mut c,
                    self.stream,
                    PartitionNo(p),
                    Offset(from),
                    ReadLimits {
                        max_records: u32::try_from(max_records - out.len()).unwrap_or(u32::MAX),
                        max_bytes,
                    },
                )
                .await;
            let batch = match r {
                Ok(b) => b,
                Err(e)
                    if e.kind
                        == sprig_common::ErrorKind::Sprig(SprigCode::StreamOffsetOutOfRange) =>
                {
                    let b = self.log.bounds(&mut c, self.stream, PartitionNo(p)).await?;
                    if from < b.log_start.0 {
                        return Err(err(
                            SprigCode::SinkSourceLost,
                            format!(
                                "partition {p}: position {from} is below the stream's log start {}",
                                b.log_start.0
                            ),
                        ));
                    }
                    // Above the high watermark (a reset log): start over at it.
                    self.at.insert(p, b.high_watermark.0);
                    continue;
                }
                Err(e) => return Err(e),
            };
            for rec in batch.records {
                self.at.insert(p, rec.offset.0 + 1);
                out.push(SinkRecord {
                    idem_key: stream_key(self.stream.0, p, rec.offset.0, rec.leader_epoch.0),
                    pos: Position::Stream {
                        stream: self.stream.0,
                        partition: p,
                        offset: rec.offset.0,
                        epoch: rec.leader_epoch.0,
                    },
                    key: rec.key,
                    value: rec.value,
                    headers: rec
                        .headers
                        .into_iter()
                        .map(|(k, v)| (k, v.unwrap_or_default()))
                        .collect(),
                    op: None,
                    before: None,
                    after: None,
                    ts_ms: rec.ts_ms,
                });
            }
        }
        self.turn = self.turn.wrapping_add(1);
        drop(c);
        if out.is_empty() {
            tokio::time::sleep(wait.min(Duration::from_millis(100))).await;
        }
        Ok(out)
    }

    async fn settle(&mut self, done: &[Position]) -> Result<()> {
        let mut by_part: HashMap<u32, (u64, u32)> = HashMap::new();
        for p in done {
            if let Position::Stream {
                stream,
                partition,
                offset,
                epoch,
            } = p
                && *stream == self.stream.0
            {
                let e = by_part.entry(*partition).or_insert((0, *epoch));
                if *offset + 1 > e.0 {
                    *e = (*offset + 1, *epoch);
                }
            }
        }
        if by_part.is_empty() {
            return Ok(());
        }
        let store = StreamsStore::new(self.catalog_schema.clone());
        let mut c = self.catalog.get_conn().await.map_err(backend)?;
        for (p, (o, e)) in by_part {
            store
                .commit_offset(
                    &mut c,
                    &CommittedOffset {
                        group: self.group.clone(),
                        instance_tenant: self.instance,
                        stream: self.stream,
                        partition: PartitionNo(p),
                        offset: o,
                        leader_epoch: Some(e),
                        metadata: None,
                    },
                )
                .await?;
        }
        Ok(())
    }

    async fn seek_after(&mut self, pos: &Position) -> Result<()> {
        if let Position::Stream {
            stream,
            partition,
            offset,
            ..
        } = pos
            && *stream == self.stream.0
            && self.partitions.contains(partition)
        {
            self.at.insert(*partition, offset + 1);
        }
        Ok(())
    }
}
