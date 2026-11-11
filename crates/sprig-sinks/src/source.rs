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

/// Catalog schemas whose streams module this process migrated.
fn migrated() -> &'static std::sync::Mutex<std::collections::HashSet<String>> {
    static M: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<String>>> =
        std::sync::OnceLock::new();
    M.get_or_init(Default::default)
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
        // `stream_offsets.instance_tenant_id` (R9) before the first read.
        if !migrated()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&catalog_schema)
        {
            sprig_streams::store::migrate(&mut c, &catalog_schema).await?;
            migrated()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(catalog_schema.clone());
        }
        let committed = store.group_offsets(&mut c, &group).await?;
        let mut sc = shard.get_conn().await.map_err(backend)?;
        let mut at = HashMap::new();
        let mut fresh = Vec::new();
        for p in &partitions {
            if let Some(o) = committed.iter().find(|o| {
                o.instance_tenant == instance && o.stream == stream && o.partition.0 == *p
            }) {
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

// ----------------------------------------------------------------------
// Fan-in sources (chapter 51, "Fan-in sinks").
// ----------------------------------------------------------------------

/// The header naming a record's member.
pub const TENANT_HEADER: &str = "x-sprig-tenant";

/// The stream id a fan-in task's positions carry for member `member`'s
/// instance stream `stream` (as the Kafka fan-in topics pack them: bit
/// 62, the member's id shifted by 24, the stream id), so positions of
/// two members never collide. Members below 2^38, streams below 2^24.
#[must_use]
pub fn fanin_stream_id(member: u64, stream: u64) -> Option<u64> {
    (member < (1 << 38) && stream < (1 << 24)).then_some((1 << 62) | (member << 24) | stream)
}

/// The member a fan-in record came from (its position's packed stream
/// id); `None` for a record of the sink's own tenant.
#[must_use]
pub fn record_member(r: &crate::model::SinkRecord) -> Option<u64> {
    match &r.pos {
        crate::model::Position::Stream { stream, .. } if stream & (1 << 62) != 0 => {
            Some((stream >> 24) & ((1 << 38) - 1))
        }
        _ => None,
    }
}

/// One member's instance, as a fan-in task reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FanInMember {
    /// The member.
    pub tenant: u64,
    /// Its name (the `x-sprig-tenant` tag).
    pub name: String,
    /// Its instance's stream id.
    pub stream: u64,
}

/// Opens one member's instance for a fan-in task: the member, its
/// partitions this task reads.
#[async_trait]
pub trait FanInOpener: Send + Sync {
    /// The members whose instances this task reads now, and for each the
    /// partitions.
    async fn members(&self) -> Result<Vec<(FanInMember, Vec<u32>)>>;
    /// A source over `m`'s instance (positions in the sink's catalog).
    async fn open(&self, m: &FanInMember, partitions: Vec<u32>) -> Result<Box<dyn SourceReader>>;
}

struct Child {
    member: FanInMember,
    packed: u64,
    src: Box<dyn SourceReader>,
    /// Nothing was there at the last read: not asked again before this.
    idle_until: Option<tokio::time::Instant>,
    idle_ms: u64,
}

/// A fan-in task's source: every member instance it reads, in turn; new
/// instances join at each refresh. Records are tagged with their member
/// (`x-sprig-tenant`), their idempotency keys start with the member's id,
/// and their positions carry [`fanin_stream_id`].
pub struct FanInSource {
    opener: Box<dyn FanInOpener>,
    /// `CHANGES OF`: the tables' change topics of the sink's own tenant;
    /// values are change envelopes, read into `op`, `before` and `after`,
    /// and nothing is tagged with a tenant.
    changes: bool,
    children: Vec<Child>,
    refresh: Duration,
    next_refresh: tokio::time::Instant,
    turn: usize,
}

impl std::fmt::Debug for FanInSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FanInSource")
            .field("members", &self.children.len())
            .finish_non_exhaustive()
    }
}

impl FanInSource {
    /// A source over the members `opener` lists, refreshed every
    /// `refresh` (`sink.fanin_refresh_seconds`).
    ///
    /// # Errors
    /// The first listing's.
    pub async fn open(opener: Box<dyn FanInOpener>, refresh: Duration) -> Result<Self> {
        let mut s = Self {
            opener,
            changes: false,
            children: Vec::new(),
            refresh,
            next_refresh: tokio::time::Instant::now(),
            turn: 0,
        };
        s.refresh_members().await?;
        Ok(s)
    }

    /// A source over the sink's own tables' change topics (`CHANGES OF`):
    /// the same reading, with each record's change envelope read into
    /// `op`, `before` and `after`.
    ///
    /// # Errors
    /// The first listing's.
    pub async fn open_changes(opener: Box<dyn FanInOpener>) -> Result<Self> {
        let mut s = Self {
            opener,
            changes: true,
            children: Vec::new(),
            refresh: Duration::from_secs(3600),
            next_refresh: tokio::time::Instant::now(),
            turn: 0,
        };
        s.refresh_members().await?;
        Ok(s)
    }

    /// The members read now.
    #[must_use]
    pub fn members(&self) -> Vec<u64> {
        self.children.iter().map(|c| c.member.tenant).collect()
    }

    async fn refresh_members(&mut self) -> Result<()> {
        self.next_refresh = tokio::time::Instant::now() + self.refresh;
        let list = self.opener.members().await?;
        for (m, parts) in list {
            if self
                .children
                .iter()
                .any(|c| c.member.tenant == m.tenant && c.member.stream == m.stream)
            {
                continue;
            }
            let Some(packed) = fanin_stream_id(m.tenant, m.stream) else {
                tracing::warn!(
                    member = m.tenant,
                    stream = m.stream,
                    "fan-in sink: member or stream id too large to read"
                );
                continue;
            };
            match self.opener.open(&m, parts).await {
                Ok(src) => {
                    tracing::debug!(member = m.tenant, "fan-in sink: member joined");
                    metrics::counter!("sprig_sink_fanin_members_joined_total").increment(1);
                    self.children.push(Child {
                        member: m,
                        packed,
                        src,
                        idle_until: None,
                        idle_ms: 0,
                    });
                }
                Err(e) => {
                    tracing::debug!(member = m.tenant, error = %e, "fan-in sink: member not opened yet");
                }
            }
        }
        Ok(())
    }

    fn child_of(&mut self, packed: u64) -> Option<&mut Child> {
        self.children.iter_mut().find(|c| c.packed == packed)
    }
}

/// A change topic's record as a change record (`CHANGES OF`).
fn change(mut r: SinkRecord, packed: u64) -> SinkRecord {
    if let Position::Stream { stream, .. } = &mut r.pos {
        *stream = packed;
    }
    if let Some(v) = r
        .value
        .as_deref()
        .and_then(|b| serde_json::from_slice::<serde_json::Value>(b).ok())
    {
        r.op = v
            .get("op")
            .and_then(serde_json::Value::as_str)
            .and_then(crate::model::ChangeOp::parse);
        r.before = v.get("before").filter(|x| !x.is_null()).cloned();
        r.after = v.get("after").filter(|x| !x.is_null()).cloned();
    }
    r
}

fn tag(mut r: SinkRecord, c: &Child) -> SinkRecord {
    if let Position::Stream { stream, .. } = &mut r.pos {
        *stream = c.packed;
        r.idem_key = format!("{}:{}", c.member.tenant, r.idem_key);
    }
    if !r.headers.iter().any(|(k, _)| k == TENANT_HEADER) {
        r.headers
            .push((TENANT_HEADER.to_owned(), c.member.name.clone().into_bytes()));
    }
    r
}

#[async_trait]
impl SourceReader for FanInSource {
    async fn next_batch(
        &mut self,
        max_records: usize,
        max_bytes: u64,
        wait: Duration,
    ) -> Result<Vec<SinkRecord>> {
        if tokio::time::Instant::now() >= self.next_refresh
            && let Err(e) = self.refresh_members().await
        {
            tracing::debug!(error = %e, "fan-in sink: refresh failed");
        }
        let mut out = Vec::new();
        let n = self.children.len();
        let now = tokio::time::Instant::now();
        for i in 0..n {
            if out.len() >= max_records {
                break;
            }
            let k = (self.turn + i) % n;
            let c = &mut self.children[k];
            if c.idle_until.is_some_and(|t| now < t) {
                continue;
            }
            let got = c
                .src
                .next_batch(max_records - out.len(), max_bytes, Duration::ZERO)
                .await?;
            if got.is_empty() {
                // Idle members are asked less often, up to once a second.
                c.idle_ms = (c.idle_ms * 2).clamp(50, 1000);
                c.idle_until = Some(now + Duration::from_millis(c.idle_ms));
            } else {
                c.idle_ms = 0;
                c.idle_until = None;
            }
            let c = &self.children[k];
            if self.changes {
                out.extend(got.into_iter().map(|r| change(r, c.packed)));
            } else {
                out.extend(got.into_iter().map(|r| tag(r, c)));
            }
        }
        self.turn = self.turn.wrapping_add(1);
        if out.is_empty() {
            tokio::time::sleep(wait.min(Duration::from_millis(100))).await;
        }
        Ok(out)
    }

    async fn settle(&mut self, done: &[Position]) -> Result<()> {
        let mut by: HashMap<u64, Vec<Position>> = HashMap::new();
        for p in done {
            if let Position::Stream { stream, .. } = p {
                by.entry(*stream).or_default().push(p.clone());
            }
        }
        for (packed, list) in by {
            let Some(c) = self.child_of(packed) else {
                continue;
            };
            let real = c.member.stream;
            let list: Vec<Position> = list
                .into_iter()
                .map(|mut p| {
                    if let Position::Stream { stream, .. } = &mut p {
                        *stream = real;
                    }
                    p
                })
                .collect();
            c.src.settle(&list).await?;
        }
        Ok(())
    }

    async fn seek_after(&mut self, pos: &Position) -> Result<()> {
        if let Position::Stream { stream, .. } = pos
            && let Some(c) = self.child_of(*stream)
        {
            let mut p = pos.clone();
            if let Position::Stream { stream, .. } = &mut p {
                *stream = c.member.stream;
            }
            c.src.seek_after(&p).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod fan_in_tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// One member's records, and what was settled.
    struct Fake {
        recs: Vec<SinkRecord>,
        settled: Arc<Mutex<Vec<(u64, Position)>>>,
        member: u64,
    }

    #[async_trait]
    impl SourceReader for Fake {
        async fn next_batch(
            &mut self,
            max: usize,
            _b: u64,
            _w: Duration,
        ) -> Result<Vec<SinkRecord>> {
            let n = max.min(self.recs.len());
            Ok(self.recs.drain(..n).collect())
        }
        async fn settle(&mut self, done: &[Position]) -> Result<()> {
            let mut s = self.settled.lock().unwrap();
            for p in done {
                s.push((self.member, p.clone()));
            }
            Ok(())
        }
        async fn seek_after(&mut self, _p: &Position) -> Result<()> {
            Ok(())
        }
    }

    struct Opener {
        members: Vec<(u64, &'static str)>,
        settled: Arc<Mutex<Vec<(u64, Position)>>>,
    }

    fn rec(stream: u64, offset: u64, header: Option<&str>) -> SinkRecord {
        SinkRecord {
            key: None,
            value: Some(b"v".to_vec()),
            headers: header
                .map(|h| vec![(TENANT_HEADER.to_owned(), h.as_bytes().to_vec())])
                .unwrap_or_default(),
            op: None,
            before: None,
            after: None,
            ts_ms: 0,
            idem_key: stream_key(stream, 0, offset, 1),
            pos: Position::Stream {
                stream,
                partition: 0,
                offset,
                epoch: 1,
            },
        }
    }

    #[async_trait]
    impl FanInOpener for Opener {
        async fn members(&self) -> Result<Vec<(FanInMember, Vec<u32>)>> {
            Ok(self
                .members
                .iter()
                .map(|(t, n)| {
                    (
                        FanInMember {
                            tenant: *t,
                            name: (*n).to_owned(),
                            stream: 7,
                        },
                        vec![0],
                    )
                })
                .collect())
        }
        async fn open(&self, m: &FanInMember, _p: Vec<u32>) -> Result<Box<dyn SourceReader>> {
            // Both members' instances have stream id 7: positions must not
            // collide.
            Ok(Box::new(Fake {
                recs: vec![rec(7, 0, None), rec(7, 1, Some("forged-stays"))],
                settled: Arc::clone(&self.settled),
                member: m.tenant,
            }))
        }
    }

    #[tokio::test]
    async fn tags_remaps_and_settles_per_member() {
        let settled = Arc::new(Mutex::new(Vec::new()));
        let mut s = FanInSource::open(
            Box::new(Opener {
                members: vec![(11, "acme"), (12, "globex")],
                settled: Arc::clone(&settled),
            }),
            Duration::from_secs(60),
        )
        .await
        .unwrap();
        assert_eq!(s.members(), vec![11, 12]);
        let got = s.next_batch(100, 1 << 20, Duration::ZERO).await.unwrap();
        assert_eq!(got.len(), 4);
        // Tagged with their member, keys prefixed, positions packed.
        let a: Vec<&SinkRecord> = got
            .iter()
            .filter(|r| r.idem_key.starts_with("11:"))
            .collect();
        assert_eq!(a.len(), 2);
        let tag = |r: &SinkRecord| {
            r.headers
                .iter()
                .find(|(k, _)| k == TENANT_HEADER)
                .map(|(_, v)| String::from_utf8_lossy(v).into_owned())
        };
        assert_eq!(tag(a[0]).as_deref(), Some("acme"));
        assert_eq!(a[0].to_json()["tenant"], "acme");
        // A header the record already had is kept (inherited instances are
        // stamped by the server).
        assert_eq!(tag(a[1]).as_deref(), Some("forged-stays"));
        let pa = fanin_stream_id(11, 7).unwrap();
        let pb = fanin_stream_id(12, 7).unwrap();
        assert_ne!(pa, pb);
        assert!(matches!(a[0].pos, Position::Stream { stream, .. } if stream == pa));
        // Settling goes back to each member with its own stream id.
        let done: Vec<Position> = got.iter().map(|r| r.pos.clone()).collect();
        s.settle(&done).await.unwrap();
        let st = settled.lock().unwrap();
        assert_eq!(st.len(), 4);
        assert!(
            st.iter()
                .all(|(_, p)| matches!(p, Position::Stream { stream: 7, .. }))
        );
        assert_eq!(st.iter().filter(|(m, _)| *m == 11).count(), 2);
    }

    #[test]
    fn packing_limits() {
        assert!(fanin_stream_id(1 << 38, 1).is_none());
        assert!(fanin_stream_id(1, 1 << 24).is_none());
        assert_eq!(fanin_stream_id(3, 5), Some((1 << 62) | (3 << 24) | 5));
        let packed = fanin_stream_id(252_530_600_004, 7).unwrap();
        assert_eq!((packed >> 24) & ((1 << 38) - 1), 252_530_600_004);
    }
}
