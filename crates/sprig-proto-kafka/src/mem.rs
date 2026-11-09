//! In-memory implementations of the broker's traits: for tests, and as the
//! gateway's **interim adapter** for topics until `sprig-streams`' catalog
//! API (chapter 45 build step 2) lands.
//!
//! [`MemTopics`] keeps topics per tenant in this process only: they are not
//! streams yet (nothing is created in MariaDB), other gateways don't see
//! them, and they are gone after a restart. [`MemGroupStore`] keeps groups
//! and offsets the same way; the gateway writes the real `stream_groups`
//! and `stream_offsets` tables instead. [`MemLeases`] grants coordinator
//! leases from a table shared by the brokers of one test.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use uuid::Uuid;

use crate::group::{
    CommittedOffset, GroupKey, GroupState, GroupStore, LeaseState, Leases, StoreError, StoredGroup,
};
use crate::protocol::err;
use crate::topics::{
    NewTopic, Principal, TopicConfigs, TopicDefaults, TopicError, TopicInfo, TopicKind, Topics,
};

fn lock<T>(m: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Topics in memory, per tenant (interim adapter; see the module docs).
#[derive(Debug)]
pub struct MemTopics {
    topics: std::sync::Mutex<HashMap<u64, BTreeMap<String, TopicInfo>>>,
    next_id: AtomicU64,
    defaults: TopicDefaults,
}

impl Default for MemTopics {
    fn default() -> Self {
        Self::new(TopicDefaults::default())
    }
}

impl MemTopics {
    /// An empty registry whose new topics inherit `defaults`.
    #[must_use]
    pub fn new(defaults: TopicDefaults) -> Self {
        Self {
            topics: std::sync::Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            defaults,
        }
    }

    /// Adds a change topic (tests: change topics are made by `ALTER TABLE
    /// ... SPRIG SET (stream.change_topic = on)`, not by Kafka clients).
    pub fn add_change_topic(&self, tenant: u64, name: &str, partitions: i32) -> TopicInfo {
        let t = self.info(
            name,
            partitions,
            TopicKind::Changes,
            TopicConfigs::default(),
        );
        lock(&self.topics)
            .entry(tenant)
            .or_default()
            .insert(name.to_owned(), t.clone());
        t
    }

    fn info(
        &self,
        name: &str,
        partitions: i32,
        kind: TopicKind,
        configs: TopicConfigs,
    ) -> TopicInfo {
        TopicInfo {
            name: name.to_owned(),
            stream_id: self.next_id.fetch_add(1, Ordering::Relaxed),
            topic_id: Uuid::now_v7(),
            partitions,
            kind,
            configs,
            defaults: self.defaults,
        }
    }

    fn unknown(name: &str) -> TopicError {
        TopicError::new(
            err::UNKNOWN_TOPIC_OR_PARTITION,
            format!("39940 ER_SPRIG_STREAM_NOT_FOUND: no stream {name}"),
        )
    }
}

/// A topic name must name a database: `<database>.<stream>`.
///
/// # Errors
/// `INVALID_TOPIC_EXCEPTION` without a database part.
pub fn split_topic(name: &str) -> Result<(&str, &str), TopicError> {
    match name.split_once('.') {
        Some((db, s)) if !db.is_empty() && !s.is_empty() => Ok((db, s)),
        _ => Err(TopicError::new(
            err::INVALID_TOPIC_EXCEPTION,
            format!("topic {name}: a topic is named <database>.<stream>"),
        )),
    }
}

#[async_trait]
impl Topics for MemTopics {
    async fn list(&self, p: &Principal) -> Result<Vec<TopicInfo>, TopicError> {
        Ok(lock(&self.topics)
            .get(&p.tenant_id)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default())
    }

    async fn get(&self, p: &Principal, name: &str) -> Result<Option<TopicInfo>, TopicError> {
        Ok(lock(&self.topics)
            .get(&p.tenant_id)
            .and_then(|m| m.get(name).cloned()))
    }

    async fn defaults(&self, _p: &Principal, _name: &str) -> Result<TopicDefaults, TopicError> {
        Ok(self.defaults)
    }

    async fn create(
        &self,
        p: &Principal,
        t: NewTopic,
        validate_only: bool,
    ) -> Result<TopicInfo, TopicError> {
        split_topic(&t.name)?;
        let mut all = lock(&self.topics);
        let m = all.entry(p.tenant_id).or_default();
        if m.contains_key(&t.name) {
            return Err(TopicError::new(
                err::TOPIC_ALREADY_EXISTS,
                format!("39942 ER_SPRIG_STREAM_EXISTS: {} already exists", t.name),
            ));
        }
        let info = self.info(&t.name, t.partitions, TopicKind::Events, t.configs);
        if !validate_only {
            m.insert(t.name, info.clone());
        }
        Ok(info)
    }

    async fn delete(&self, p: &Principal, name: &str) -> Result<TopicInfo, TopicError> {
        let mut all = lock(&self.topics);
        let m = all.entry(p.tenant_id).or_default();
        match m.get(name) {
            None => Err(Self::unknown(name)),
            Some(t) if t.kind == TopicKind::Changes => Err(TopicError::new(
                err::POLICY_VIOLATION,
                format!("{name} is a change topic: turn stream.change_topic off on its object"),
            )),
            Some(_) => m.remove(name).ok_or_else(|| Self::unknown(name)),
        }
    }

    async fn grow(
        &self,
        p: &Principal,
        name: &str,
        partitions: i32,
        validate_only: bool,
    ) -> Result<(), TopicError> {
        let mut all = lock(&self.topics);
        let t = all
            .entry(p.tenant_id)
            .or_default()
            .get_mut(name)
            .ok_or_else(|| Self::unknown(name))?;
        if partitions < t.partitions {
            return Err(TopicError::new(
                err::INVALID_PARTITIONS,
                format!(
                    "39943 ER_SPRIG_STREAM_PARTITIONS_DECREASE: {name} has {} partitions; partitions can only grow",
                    t.partitions
                ),
            ));
        }
        if partitions == t.partitions {
            return Err(TopicError::new(
                err::INVALID_PARTITIONS,
                format!("{name} already has {partitions} partitions"),
            ));
        }
        if !validate_only {
            t.partitions = partitions;
        }
        Ok(())
    }

    async fn set_configs(
        &self,
        p: &Principal,
        name: &str,
        c: TopicConfigs,
        validate_only: bool,
    ) -> Result<(), TopicError> {
        let mut all = lock(&self.topics);
        let t = all
            .entry(p.tenant_id)
            .or_default()
            .get_mut(name)
            .ok_or_else(|| Self::unknown(name))?;
        if !validate_only {
            t.configs = c;
        }
        Ok(())
    }
}

/// Groups and offsets in memory.
#[derive(Debug, Default)]
pub struct MemGroupStore {
    groups: std::sync::Mutex<HashMap<(u64, String), (StoredGroup, GroupState)>>,
    offsets: std::sync::Mutex<HashMap<(u64, String), Offsets>>,
}

/// One group's offsets by (stream, partition).
type Offsets = BTreeMap<(u64, i32), CommittedOffset>;

#[async_trait]
impl GroupStore for MemGroupStore {
    async fn commit(
        &self,
        tenant: u64,
        group: &str,
        offsets: &[CommittedOffset],
    ) -> Result<(), StoreError> {
        let mut all = lock(&self.offsets);
        let m = all.entry((tenant, group.to_owned())).or_default();
        for o in offsets {
            m.insert((o.stream_id, o.partition), o.clone());
        }
        Ok(())
    }

    async fn fetch(
        &self,
        tenant: u64,
        group: &str,
        only: Option<&[(u64, i32)]>,
    ) -> Result<Vec<CommittedOffset>, StoreError> {
        let all = lock(&self.offsets);
        let Some(m) = all.get(&(tenant, group.to_owned())) else {
            return Ok(Vec::new());
        };
        Ok(match only {
            None => m.values().cloned().collect(),
            Some(keys) => keys.iter().filter_map(|k| m.get(k).cloned()).collect(),
        })
    }

    async fn save_group(
        &self,
        tenant: u64,
        g: &StoredGroup,
        state: GroupState,
    ) -> Result<(), StoreError> {
        lock(&self.groups).insert((tenant, g.group.clone()), (g.clone(), state));
        Ok(())
    }

    async fn load_group(
        &self,
        tenant: u64,
        group: &str,
    ) -> Result<Option<StoredGroup>, StoreError> {
        Ok(lock(&self.groups)
            .get(&(tenant, group.to_owned()))
            .map(|(g, _)| g.clone()))
    }

    async fn list_groups(&self, tenant: u64) -> Result<Vec<StoredGroup>, StoreError> {
        let mut v: Vec<StoredGroup> = lock(&self.groups)
            .iter()
            .filter(|((t, _), _)| *t == tenant)
            .map(|(_, (g, _))| g.clone())
            .collect();
        v.sort_by(|a, b| a.group.cmp(&b.group));
        Ok(v)
    }

    async fn delete_group(&self, tenant: u64, group: &str) -> Result<(), StoreError> {
        lock(&self.groups).remove(&(tenant, group.to_owned()));
        lock(&self.offsets).remove(&(tenant, group.to_owned()));
        Ok(())
    }
}

/// A lease table shared by the brokers of one process (tests).
#[derive(Debug, Default)]
pub struct MemLeaseTable {
    held: std::sync::Mutex<HashMap<GroupKey, (i32, u64)>>,
    epoch: AtomicU64,
}

impl MemLeaseTable {
    /// Drops every lease `node` holds, as an expired etcd lease would.
    pub fn expire(&self, node: i32) {
        lock(&self.held).retain(|_, (n, _)| *n != node);
    }
}

/// Coordinator leases of one broker over a [`MemLeaseTable`].
#[derive(Debug, Clone)]
pub struct MemLeases {
    /// The shared table.
    pub table: Arc<MemLeaseTable>,
    /// This broker's id.
    pub node: i32,
}

impl MemLeases {
    /// A broker's leases over a table of its own (a single broker).
    #[must_use]
    pub fn single(node: i32) -> Self {
        Self {
            table: Arc::new(MemLeaseTable::default()),
            node,
        }
    }
}

#[async_trait]
impl Leases for MemLeases {
    async fn acquire(&self, key: &GroupKey) -> Result<LeaseState, StoreError> {
        let mut held = lock(&self.table.held);
        match held.get(key) {
            Some((n, e)) if *n == self.node => Ok(LeaseState::Held { epoch: *e }),
            Some((n, _)) => Ok(LeaseState::Elsewhere(*n)),
            None => {
                let e = self.table.epoch.fetch_add(1, Ordering::Relaxed) + 1;
                held.insert(key.clone(), (self.node, e));
                Ok(LeaseState::Held { epoch: e })
            }
        }
    }

    async fn holder(&self, key: &GroupKey) -> Result<Option<i32>, StoreError> {
        Ok(lock(&self.table.held).get(key).map(|(n, _)| *n))
    }

    async fn confirm(&self, key: &GroupKey, epoch: u64) -> bool {
        lock(&self.table.held).get(key) == Some(&(self.node, epoch))
    }

    async fn release(&self, key: &GroupKey) {
        let mut held = lock(&self.table.held);
        if held.get(key).is_some_and(|(n, _)| *n == self.node) {
            held.remove(key);
        }
    }
}

/// Streams' records in memory, for the data plane's tests: one log per
/// (tenant, stream, partition), appended at once (no sequencing delay
/// unless [`MemData::hold`] holds it), with Kafka's idempotent producer
/// rules (per producer and partition: the epoch, the last sequence, and
/// the last five batches for retries).
#[derive(Debug)]
pub struct MemData {
    topics: Arc<MemTopics>,
    state: std::sync::Mutex<MemDataState>,
    changed: tokio::sync::Notify,
    limits: crate::broker::data::Limits,
}

#[derive(Debug, Default)]
struct MemDataState {
    logs: HashMap<(u64, u64, i32), MemPartition>,
    /// Appends wait until released (tests of the sequencing timeout).
    held: bool,
}

#[derive(Debug, Default)]
struct MemPartition {
    records: Vec<crate::broker::data::StoredRecord>,
    log_start: i64,
    epoch: i32,
    /// Records appended but not yet given offsets (held).
    pending: Vec<([u8; 16], Vec<crate::broker::data::ProduceRecord>)>,
    /// Batch first event id -> base offset.
    offsets: HashMap<[u8; 16], i64>,
    producers: HashMap<i64, MemProducer>,
    next_event: u64,
}

#[derive(Debug, Default)]
struct MemProducer {
    epoch: i16,
    last_seq: i32,
    /// (base sequence, last sequence, first event).
    recent: std::collections::VecDeque<(i32, i32, [u8; 16])>,
}

impl MemData {
    /// Records of `topics`' streams.
    #[must_use]
    pub fn new(topics: Arc<MemTopics>) -> Self {
        Self {
            topics,
            state: std::sync::Mutex::new(MemDataState::default()),
            changed: tokio::sync::Notify::new(),
            limits: crate::broker::data::Limits {
                produce_wait: std::time::Duration::from_millis(500),
                fetch_max_wait: std::time::Duration::from_millis(500),
            },
        }
    }

    /// Holds new appends unsequenced (`true`) or sequences them
    /// (`false`).
    pub fn hold(&self, held: bool) {
        let mut st = lock(&self.state);
        st.held = held;
        if !held {
            for part in st.logs.values_mut() {
                for (id, recs) in std::mem::take(&mut part.pending) {
                    Self::sequence(part, id, recs);
                }
            }
        }
        drop(st);
        self.changed.notify_waiters();
    }

    /// Raises the leader epoch of every partition of `stream` (a failover)
    /// and drops records from `truncate_from` on (a lost tail).
    pub fn failover(&self, tenant: u64, stream: u64, truncate_from: Option<i64>) {
        let mut st = lock(&self.state);
        for ((t, s, _), part) in &mut st.logs {
            if *t == tenant && *s == stream {
                part.epoch += 1;
                if let Some(o) = truncate_from {
                    part.records.retain(|r| r.offset < o);
                }
            }
        }
    }

    /// Drops records below `offset` (retention).
    pub fn retain_from(&self, tenant: u64, stream: u64, partition: i32, offset: i64) {
        let mut st = lock(&self.state);
        if let Some(part) = st.logs.get_mut(&(tenant, stream, partition)) {
            part.records.retain(|r| r.offset >= offset);
            part.log_start = part.log_start.max(offset);
        }
    }

    fn sequence(
        part: &mut MemPartition,
        id: [u8; 16],
        recs: Vec<crate::broker::data::ProduceRecord>,
    ) {
        let base = part.records.last().map_or(part.log_start, |r| r.offset + 1);
        part.offsets.insert(id, base);
        for (i, r) in recs.into_iter().enumerate() {
            part.records.push(crate::broker::data::StoredRecord {
                offset: base + i64::try_from(i).unwrap_or(0),
                timestamp_ms: r.timestamp_ms,
                key: r.key,
                value: r.value,
                headers: r.headers,
                leader_epoch: part.epoch,
            });
        }
    }

    fn hw(part: &MemPartition) -> i64 {
        part.records.last().map_or(part.log_start, |r| r.offset + 1)
    }

    async fn find(
        &self,
        p: &Principal,
        name: &str,
        access: crate::broker::data::Access,
    ) -> Result<crate::broker::data::DataTopic, TopicError> {
        let t = self
            .topics
            .get(p, name)
            .await?
            .ok_or_else(|| MemTopics::unknown(name))?;
        Self::data_topic(&t, access)
    }

    fn data_topic(
        t: &TopicInfo,
        access: crate::broker::data::Access,
    ) -> Result<crate::broker::data::DataTopic, TopicError> {
        if access == crate::broker::data::Access::Write && t.kind == TopicKind::Changes {
            return Err(TopicError::new(
                err::INVALID_TOPIC_EXCEPTION,
                format!(
                    "39941 ER_SPRIG_STREAM_APPEND_ONLY: {} is a change topic",
                    t.name
                ),
            ));
        }
        let eff = t.configs.cleanup.unwrap_or(t.defaults.cleanup);
        Ok(crate::broker::data::DataTopic {
            name: t.name.clone(),
            stream_id: t.stream_id,
            topic_id: t.topic_id,
            partitions: t.partitions,
            kind: t.kind,
            compact: eff == crate::topics::Cleanup::Compact,
            max_message_bytes: usize::try_from(
                t.configs
                    .max_message_bytes
                    .unwrap_or(t.defaults.max_message_bytes),
            )
            .unwrap_or(0),
        })
    }
}

#[async_trait]
impl crate::broker::data::StreamData for MemData {
    async fn topic(
        &self,
        p: &Principal,
        name: &str,
        access: crate::broker::data::Access,
    ) -> Result<crate::broker::data::DataTopic, TopicError> {
        self.find(p, name, access).await
    }

    async fn topic_by_id(
        &self,
        p: &Principal,
        id: Uuid,
        access: crate::broker::data::Access,
    ) -> Result<crate::broker::data::DataTopic, TopicError> {
        let all = self.topics.list(p).await?;
        let t = all.iter().find(|t| t.topic_id == id).ok_or_else(|| {
            TopicError::new(err::UNKNOWN_TOPIC_ID, format!("no topic with id {id}"))
        })?;
        Self::data_topic(t, access)
    }

    async fn append(
        &self,
        p: &Principal,
        t: &crate::broker::data::DataTopic,
        partition: i32,
        records: Vec<crate::broker::data::ProduceRecord>,
        producer: Option<crate::broker::data::ProducerBatch>,
    ) -> Result<crate::broker::data::AppendTicket, TopicError> {
        let mut st = lock(&self.state);
        let held = st.held;
        let part = st
            .logs
            .entry((p.tenant_id, t.stream_id, partition))
            .or_default();
        part.next_event += 1;
        let mut id = [0u8; 16];
        id[8..].copy_from_slice(&part.next_event.to_be_bytes());
        let count = records.len();
        if let Some(b) = producer {
            let last = b.base_sequence + b.count - 1;
            let e = part.producers.entry(b.producer_id).or_insert(MemProducer {
                epoch: b.epoch,
                last_seq: -1,
                recent: std::collections::VecDeque::new(),
            });
            if b.epoch < e.epoch {
                return Err(TopicError::new(
                    err::INVALID_PRODUCER_EPOCH,
                    format!("producer epoch {} is older than {}", b.epoch, e.epoch),
                ));
            }
            if b.epoch > e.epoch {
                if b.base_sequence != 0 {
                    return Err(TopicError::new(
                        err::OUT_OF_ORDER_SEQUENCE_NUMBER,
                        "a new epoch starts at sequence 0",
                    ));
                }
                *e = MemProducer {
                    epoch: b.epoch,
                    last_seq: -1,
                    recent: std::collections::VecDeque::new(),
                };
            }
            if let Some((_, _, first)) = e
                .recent
                .iter()
                .find(|(bs, ls, _)| *bs == b.base_sequence && *ls == last)
            {
                let first = *first;
                return Ok(crate::broker::data::AppendTicket {
                    partition,
                    first_event: first,
                    count,
                    base_offset: part.offsets.get(&first).copied(),
                    duplicate: true,
                });
            }
            if e.last_seq >= 0 && b.base_sequence != e.last_seq.wrapping_add(1) {
                return Err(TopicError::new(
                    err::OUT_OF_ORDER_SEQUENCE_NUMBER,
                    format!(
                        "sequence {} after {} for producer {}",
                        b.base_sequence, e.last_seq, b.producer_id
                    ),
                ));
            }
            e.last_seq = last;
            e.recent.push_back((b.base_sequence, last, id));
            if e.recent.len() > 5 {
                e.recent.pop_front();
            }
        }
        if held {
            part.pending.push((id, records));
        } else {
            Self::sequence(part, id, records);
        }
        drop(st);
        self.changed.notify_waiters();
        Ok(crate::broker::data::AppendTicket {
            partition,
            first_event: id,
            count,
            base_offset: None,
            duplicate: false,
        })
    }

    async fn sequenced(
        &self,
        p: &Principal,
        t: &crate::broker::data::DataTopic,
        ticket: &crate::broker::data::AppendTicket,
        deadline: tokio::time::Instant,
    ) -> Result<i64, TopicError> {
        loop {
            let notified = self.changed.notified();
            {
                let st = lock(&self.state);
                if let Some(o) = st
                    .logs
                    .get(&(p.tenant_id, t.stream_id, ticket.partition))
                    .and_then(|part| part.offsets.get(&ticket.first_event))
                {
                    return Ok(*o);
                }
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Err(TopicError::new(
                    err::REQUEST_TIMED_OUT,
                    "39946 ER_SPRIG_STREAM_SEQUENCING_TIMEOUT: the records are committed but have \
                     no offset yet; retry",
                ));
            }
        }
    }

    async fn bounds(
        &self,
        p: &Principal,
        t: &crate::broker::data::DataTopic,
        partition: i32,
    ) -> Result<crate::broker::data::Bounds, TopicError> {
        let st = lock(&self.state);
        Ok(st.logs.get(&(p.tenant_id, t.stream_id, partition)).map_or(
            crate::broker::data::Bounds {
                log_start: 0,
                high_watermark: 0,
                leader_epoch: 0,
            },
            |part| crate::broker::data::Bounds {
                log_start: part.log_start,
                high_watermark: Self::hw(part),
                leader_epoch: part.epoch,
            },
        ))
    }

    async fn read(
        &self,
        p: &Principal,
        t: &crate::broker::data::DataTopic,
        partition: i32,
        offset: i64,
        max_bytes: usize,
    ) -> Result<
        (
            crate::broker::data::Bounds,
            Vec<crate::broker::data::StoredRecord>,
        ),
        TopicError,
    > {
        let b = self.bounds(p, t, partition).await?;
        if offset < b.log_start || offset > b.high_watermark {
            return Err(TopicError::new(
                err::OFFSET_OUT_OF_RANGE,
                format!(
                    "39947 ER_SPRIG_STREAM_OFFSET_OUT_OF_RANGE: offset {offset} is outside [{}, {}]",
                    b.log_start, b.high_watermark
                ),
            ));
        }
        let st = lock(&self.state);
        let mut out = Vec::new();
        let mut bytes = 0;
        if let Some(part) = st.logs.get(&(p.tenant_id, t.stream_id, partition)) {
            for r in part.records.iter().filter(|r| r.offset >= offset) {
                let n = r.key.as_ref().map_or(0, Vec::len) + r.value.as_ref().map_or(0, Vec::len);
                if !out.is_empty() && bytes + n > max_bytes {
                    break;
                }
                bytes += n;
                out.push(r.clone());
            }
        }
        Ok((b, out))
    }

    async fn wait(
        &self,
        p: &Principal,
        beyond: &[(u64, i32, i64)],
        deadline: tokio::time::Instant,
    ) {
        loop {
            let notified = self.changed.notified();
            {
                let st = lock(&self.state);
                let moved = beyond.iter().any(|(s, part, o)| {
                    st.logs
                        .get(&(p.tenant_id, *s, *part))
                        .is_some_and(|x| Self::hw(x) > *o)
                });
                if moved {
                    return;
                }
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return;
            }
        }
    }

    async fn offset_for_time(
        &self,
        p: &Principal,
        t: &crate::broker::data::DataTopic,
        partition: i32,
        ts: i64,
    ) -> Result<Option<(i64, i64)>, TopicError> {
        let st = lock(&self.state);
        let Some(part) = st.logs.get(&(p.tenant_id, t.stream_id, partition)) else {
            return Ok(None);
        };
        if ts == -3 {
            return Ok(part
                .records
                .iter()
                .max_by_key(|r| (r.timestamp_ms, -r.offset))
                .map(|r| (r.offset, r.timestamp_ms)));
        }
        Ok(part
            .records
            .iter()
            .find(|r| r.timestamp_ms >= ts)
            .map(|r| (r.offset, r.timestamp_ms)))
    }

    async fn epoch_end(
        &self,
        p: &Principal,
        t: &crate::broker::data::DataTopic,
        partition: i32,
        epoch: i32,
    ) -> Result<(i32, i64), TopicError> {
        let st = lock(&self.state);
        let Some(part) = st.logs.get(&(p.tenant_id, t.stream_id, partition)) else {
            return Ok((epoch, 0));
        };
        if epoch >= part.epoch {
            return Ok((part.epoch, Self::hw(part)));
        }
        let end = part
            .records
            .iter()
            .find(|r| r.leader_epoch > epoch)
            .map_or(Self::hw(part), |r| r.offset);
        let e = part
            .records
            .iter()
            .filter(|r| r.leader_epoch <= epoch)
            .map(|r| r.leader_epoch)
            .max()
            .unwrap_or(epoch);
        Ok((e, end))
    }

    async fn leader_epoch(&self, p: &Principal, stream_id: u64, partition: i32) -> i32 {
        lock(&self.state)
            .logs
            .get(&(p.tenant_id, stream_id, partition))
            .map_or(0, |x| x.epoch)
    }

    async fn limits(&self, _p: &Principal) -> crate::broker::data::Limits {
        self.limits
    }
}
