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
//! - [`QueueSource`] (R9, r9queue): a durable queue, consumed through a
//!   gateway's queue engine as the sink's owner ([`crate::peer`]):
//!   `sprig.mq_get` holds each message for `sink.queue_lease_ms`, a
//!   delivered record is acknowledged (`mq_ack`), and a record that fails
//!   for good is rejected without requeue (`mq_nack`) when the sink has no
//!   `DEAD LETTER TO`, so the queue's own dead-lettering applies. Its
//!   idempotency key is `q/<queue id>/<message id>`.
//!
//! `CHANGES OF` reads the tables' change topics (`t.changes`) as streams.

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

/// Marks a fan-in task's position stream ids: bit 62 plus the member
/// instance's slot in the task (R9, r9queue). Slots are numbered as
/// members join and are never reused within a task, so positions of two
/// members never collide, for any 64-bit tenant and stream ids. (r9final
/// packed the member's id and the stream id into it, which capped tenant
/// ids at 2^38.) Positions of fan-in tasks live only in the task: a
/// member instance's offsets are stored with its own stream id and
/// `instance_tenant_id`.
pub const FANIN_SLOT_FLAG: u64 = 1 << 62;

/// The member a fan-in record came from: the `<member>:` its idempotency
/// key starts with (keys of other sources start with `s/`, `q/` or `c/`,
/// and replayed dead letters keep their keys); `None` for a record of the
/// sink's own tenant.
#[must_use]
pub fn record_member(r: &crate::model::SinkRecord) -> Option<u64> {
    let (head, _) = r.idem_key.split_once(':')?;
    if head.is_empty() || !head.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    head.parse().ok()
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
/// (`x-sprig-tenant`), their idempotency keys start with the member's id
/// (`<member>:`), and their positions carry the member's slot
/// ([`FANIN_SLOT_FLAG`]).
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
    /// The next member instance's slot ([`FANIN_SLOT_FLAG`]).
    next_slot: u64,
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
            next_slot: 0,
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
            next_slot: 0,
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
            let packed = FANIN_SLOT_FLAG | self.next_slot;
            match self.opener.open(&m, parts).await {
                Ok(src) => {
                    tracing::debug!(member = m.tenant, "fan-in sink: member joined");
                    metrics::counter!("sprig_sink_fanin_members_joined_total").increment(1);
                    self.next_slot += 1;
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

/// A position with its member slot replaced by the instance's own id.
fn unslot(mut p: Position, real: u64) -> Position {
    match &mut p {
        Position::Stream { stream, .. } => *stream = real,
        Position::Queue { queue, .. } => *queue = real,
        Position::Change { .. } => {}
    }
    p
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
    match &mut r.pos {
        Position::Stream { stream, .. } => {
            *stream = c.packed;
            r.idem_key = format!("{}:{}", c.member.tenant, r.idem_key);
        }
        Position::Queue { queue, msg_id, .. } if !msg_id.starts_with("replay:") => {
            *queue = c.packed;
            r.idem_key = format!("{}:{}", c.member.tenant, r.idem_key);
        }
        _ => {}
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
            match p {
                Position::Stream { stream: slot, .. } | Position::Queue { queue: slot, .. } => {
                    by.entry(*slot).or_default().push(p.clone());
                }
                Position::Change { .. } => {}
            }
        }
        for (packed, list) in by {
            let Some(c) = self.child_of(packed) else {
                continue;
            };
            let real = c.member.stream;
            let list: Vec<Position> = list.into_iter().map(|p| unslot(p, real)).collect();
            c.src.settle(&list).await?;
        }
        Ok(())
    }

    async fn reject(&mut self, rec: &SinkRecord, reason: &str) -> Result<bool> {
        let slot = match &rec.pos {
            Position::Stream { stream, .. } => *stream,
            Position::Queue { queue, .. } => *queue,
            Position::Change { .. } => return Ok(false),
        };
        let Some(c) = self.child_of(slot) else {
            return Ok(false);
        };
        let mut r = rec.clone();
        r.pos = unslot(r.pos, c.member.stream);
        c.src.reject(&r, reason).await
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

// ----------------------------------------------------------------------
// Queue sources (chapter 51; R9, r9queue).
// ----------------------------------------------------------------------

/// The queue id and message id (hex) a delivery token names, read
/// without checking its MAC (the gateway checks it at settle time).
/// Chapter 50's layout: `q1.` and base64url of tenant (8 bytes), database
/// (4), queue (8), lane (2), class (1), sequence (8), message id (16),
/// owner epoch (8), then 16 bytes of MAC.
#[must_use]
pub fn token_parts(token: &str) -> Option<(u64, String)> {
    use base64::Engine as _;
    let b = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(token.strip_prefix("q1.")?)
        .ok()?;
    if b.len() < 55 {
        return None;
    }
    let queue = u64::from_be_bytes(b[12..20].try_into().ok()?);
    let mut hex = String::with_capacity(32);
    for x in &b[31..47] {
        hex.push_str(&format!("{x:02x}"));
    }
    Some((queue, hex))
}

/// Where a queue source reads: the sink's own queue, or a member's
/// instance of a queue definition (fan-in).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueAt {
    /// The sink's tenant's queue `id`, in database (vhost) `db`.
    Own {
        /// The queue id.
        id: u64,
        /// The vhost's database.
        db: String,
    },
    /// Member `member`'s instance of definition `def`, in database `db`.
    Member {
        /// The definition (declarer, object).
        def: (u64, u64),
        /// The member.
        member: u64,
        /// The instance's database.
        db: String,
    },
}

/// A queue source for one task (see the module docs).
pub struct QueueSource {
    peer: std::sync::Arc<dyn crate::peer::SinkPeer>,
    run_as: crate::peer::RunAs,
    at: QueueAt,
    queue: String,
    lease_ms: u64,
    seed: u64,
    idle_ms: u64,
}

impl std::fmt::Debug for QueueSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueSource")
            .field("queue", &self.queue)
            .field("at", &self.at)
            .finish_non_exhaustive()
    }
}

impl QueueSource {
    /// A source over `queue` at `at`, read as `run_as` through `peer`.
    #[must_use]
    pub fn new(
        peer: std::sync::Arc<dyn crate::peer::SinkPeer>,
        run_as: crate::peer::RunAs,
        at: QueueAt,
        queue: String,
        lease_ms: u64,
        seed: u64,
    ) -> Self {
        Self {
            peer,
            run_as,
            at,
            queue,
            lease_ms,
            seed,
            idle_ms: 0,
        }
    }

    async fn run(&self, op: crate::peer::SqlOp) -> Result<crate::peer::SqlReply> {
        self.peer
            .sql(self.seed, &self.run_as.request(op))
            .await?
            .ok()
    }

    /// Settles tokens: acknowledged (`ack`) or rejected without requeue.
    async fn settle_tokens(&self, tokens: Vec<String>, ack: bool) -> Result<()> {
        use crate::peer::{SqlOp, quote};
        for chunk in tokens.chunks(200) {
            let op = match &self.at {
                QueueAt::Own { .. } => SqlOp::Queue {
                    statements: chunk
                        .iter()
                        .map(|t| {
                            if ack {
                                format!("CALL sprig.mq_ack({})", quote(t))
                            } else {
                                format!("CALL sprig.mq_nack({}, 0)", quote(t))
                            }
                        })
                        .collect(),
                },
                QueueAt::Member { .. } => SqlOp::FanInSettle {
                    tokens: chunk.to_vec(),
                    ack,
                },
            };
            let r = self.run(op).await?;
            for (t, res) in chunk.iter().zip(&r.results) {
                if let Some(e) = &res.error {
                    // Already settled: its lease passed and the queue gave
                    // it out again (at least once); anything else is
                    // logged, the message returns after its lease.
                    tracing::debug!(token = %t, code = e.code, error = %e.message, "queue sink: a settle failed");
                    metrics::counter!("sprig_sink_queue_settle_failures_total").increment(1);
                }
            }
        }
        Ok(())
    }
}

/// A queue message's record: the body as the value, the routing key as
/// the key (lanes keep one routing key's messages in order), application
/// headers and `amqp.*` fields as headers.
fn queue_record(
    at: &QueueAt,
    res: &crate::peer::SqlResult,
    row: &[Option<String>],
) -> Option<SinkRecord> {
    use crate::peer::SqlResult as R;
    let col = |n: &str| res.col(n);
    let token = R::text(row, col("delivery_token")?)?;
    let (qid, msg) = token_parts(&token)?;
    let queue = match at {
        QueueAt::Own { id, .. } => *id,
        QueueAt::Member { .. } => qid,
    };
    let body = col("body").and_then(|i| R::bytes(row, i));
    let props: serde_json::Value = col("properties")
        .and_then(|i| R::text(row, i))
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_default();
    let rk = col("routing_key")
        .and_then(|i| R::text(row, i))
        .unwrap_or_default();
    let mut headers: Vec<(String, Vec<u8>)> = Vec::new();
    if let Some(h) = props.get("headers").and_then(serde_json::Value::as_object) {
        for (k, v) in h {
            // The delivery token settles the message: never sent on.
            if k == "x-sprig-delivery" {
                continue;
            }
            let t = match v {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            headers.push((k.clone(), t.into_bytes()));
        }
    }
    let mut amqp = |k: &str, v: Option<String>| {
        if let Some(v) = v {
            headers.push((format!("amqp.{k}"), v.into_bytes()));
        }
    };
    amqp("routing_key", Some(rk.clone()));
    amqp("exchange", col("exchange").and_then(|i| R::text(row, i)));
    amqp(
        "delivery_count",
        col("delivery_count").and_then(|i| R::text(row, i)),
    );
    for k in ["message_id", "content_type", "correlation_id", "type"] {
        amqp(
            k,
            props
                .get(k)
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
        );
    }
    let ts_ms = props
        .get("timestamp")
        .and_then(serde_json::Value::as_i64)
        .map_or_else(
            || {
                i64::try_from(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |d| d.as_millis()),
                )
                .unwrap_or(0)
            },
            |s| s.saturating_mul(1000),
        );
    Some(SinkRecord {
        key: (!rk.is_empty()).then(|| rk.into_bytes()),
        value: body,
        headers,
        op: None,
        before: None,
        after: None,
        ts_ms,
        idem_key: format!("q/{queue}/{msg}"),
        pos: Position::Queue {
            queue,
            msg_id: msg,
            delivery: token,
        },
    })
}

#[async_trait]
impl SourceReader for QueueSource {
    async fn next_batch(
        &mut self,
        max_records: usize,
        _max_bytes: u64,
        wait: Duration,
    ) -> Result<Vec<SinkRecord>> {
        use crate::peer::{SqlOp, quote};
        let n = u32::try_from(max_records.clamp(1, 1000)).unwrap_or(1000);
        let op = match &self.at {
            QueueAt::Own { db, .. } => SqlOp::Queue {
                statements: vec![format!(
                    "CALL sprig.mq_get({}, {n}, {}, {})",
                    quote(&self.queue),
                    self.lease_ms,
                    quote(db)
                )],
            },
            QueueAt::Member { def, member, db } => SqlOp::FanInGet {
                def_tenant: def.0,
                def_object: def.1,
                member: *member,
                db: db.clone(),
                queue: self.queue.clone(),
                max: n,
                lease_ms: self.lease_ms,
            },
        };
        let r = self.run(op).await?;
        let res = r.results.into_iter().next().unwrap_or_default();
        if let Some(e) = &res.error {
            return Err(e.error());
        }
        let out: Vec<SinkRecord> = res
            .rows
            .iter()
            .filter_map(|row| queue_record(&self.at, &res, row))
            .collect();
        if out.is_empty() {
            // Nothing waiting: ask again after 100 ms, doubling to 1 s.
            self.idle_ms = (self.idle_ms * 2).clamp(100, 1000);
            tokio::time::sleep(wait.min(Duration::from_millis(self.idle_ms))).await;
        } else {
            self.idle_ms = 0;
        }
        Ok(out)
    }

    async fn settle(&mut self, done: &[Position]) -> Result<()> {
        let tokens: Vec<String> = done
            .iter()
            .filter_map(|p| match p {
                Position::Queue {
                    msg_id, delivery, ..
                } if !msg_id.starts_with("replay:") && !delivery.is_empty() => {
                    Some(delivery.clone())
                }
                _ => None,
            })
            .collect();
        if tokens.is_empty() {
            return Ok(());
        }
        self.settle_tokens(tokens, true).await
    }

    async fn reject(&mut self, rec: &SinkRecord, reason: &str) -> Result<bool> {
        let Position::Queue {
            msg_id, delivery, ..
        } = &rec.pos
        else {
            return Ok(false);
        };
        if msg_id.starts_with("replay:") || delivery.is_empty() {
            return Ok(false);
        }
        tracing::info!(queue = %self.queue, message = %msg_id, reason, "queue sink: rejected to the queue's dead-lettering");
        self.settle_tokens(vec![delivery.clone()], false).await?;
        Ok(true)
    }

    async fn seek_after(&mut self, _pos: &Position) -> Result<()> {
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
        let pa = FANIN_SLOT_FLAG;
        let pb = FANIN_SLOT_FLAG | 1;
        assert_eq!(record_member(a[0]), Some(11));
        assert!(
            got.iter()
                .any(|r| matches!(r.pos, Position::Stream { stream, .. } if stream == pb))
        );
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

    #[tokio::test]
    async fn members_beyond_2_38_and_own_changes_untagged() {
        // R9 (r9queue): any 64-bit member id.
        let big = (1u64 << 40) + 3;
        let settled = Arc::new(Mutex::new(Vec::new()));
        let mut s = FanInSource::open(
            Box::new(Opener {
                members: vec![(big, "big"), (u64::MAX - 1, "max")],
                settled: Arc::clone(&settled),
            }),
            Duration::from_secs(60),
        )
        .await
        .unwrap();
        assert_eq!(s.members(), vec![big, u64::MAX - 1]);
        let got = s.next_batch(100, 1 << 20, Duration::ZERO).await.unwrap();
        assert_eq!(got.len(), 4);
        let mut members: Vec<u64> = got.iter().filter_map(record_member).collect();
        members.dedup();
        assert_eq!(members, vec![big, u64::MAX - 1]);
        let done: Vec<Position> = got.iter().map(|r| r.pos.clone()).collect();
        s.settle(&done).await.unwrap();
        let st = settled.lock().unwrap();
        assert_eq!(st.iter().filter(|(m, _)| *m == big).count(), 2);
        assert!(
            st.iter()
                .all(|(_, p)| matches!(p, Position::Stream { stream: 7, .. }))
        );
        // Records of other sources name no member.
        assert_eq!(record_member(&rec(7, 0, None)), None);
        let mut r = rec(7, 0, None);
        r.idem_key = "q/412/01JC7A3M4Z".into();
        assert_eq!(record_member(&r), None);
        r.idem_key = ":x".into();
        assert_eq!(record_member(&r), None);
    }

    #[tokio::test]
    async fn change_topics_are_not_members() {
        let settled = Arc::new(Mutex::new(Vec::new()));
        let mut s = FanInSource::open_changes(Box::new(Opener {
            members: vec![(26_306_000_001, "")],
            settled: Arc::clone(&settled),
        }))
        .await
        .unwrap();
        let got = s.next_batch(100, 1 << 20, Duration::ZERO).await.unwrap();
        assert_eq!(got.len(), 2);
        assert!(got.iter().all(|r| record_member(r).is_none()));
    }
}
