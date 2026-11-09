//! The data plane (chapter 45 build steps 10 and 11): Produce, Fetch,
//! ListOffsets and OffsetForLeaderEpoch, over the [`StreamData`] trait the
//! gateway implements on `sprig-streams` (and [`crate::mem::MemData`] in
//! memory, for tests).
//!
//! **Produce** (v3–9) runs in two phases. The first runs in the
//! connection's order, before the next request is read: the batch is
//! decoded and checked (CRC, one batch per partition, consecutive offset
//! deltas, no control or transactional batches, record sizes against
//! `stream.max_message_bytes`, keys on compacted streams) and appended to
//! the stream's outbox in one transaction with the idempotent producer's
//! sequence check ([`StreamData::append`]). The second, waiting until the
//! sequencer has given the records their offsets (at most
//! `stream.produce_wait_ms`), runs concurrently with the connection's next
//! requests, so a producer with several batches in flight isn't held to
//! one sequencing wait at a time; responses still go out in request order.
//! `acks=0` closes the connection (39950): SprigDB always acknowledges
//! after commit, and an `acks=0` producer reads no response.
//!
//! **Fetch** (v4–13) reads each partition from its offset and waits for
//! new records (the high-watermark signal) while fewer than `min_bytes`
//! were found, at most `max_wait_ms` capped by `kafka.fetch_max_wait_ms`.
//! `read_committed` and `read_uncommitted` read the same records: the log
//! holds only committed records, so the last stable offset is the high
//! watermark and no transaction was ever aborted. Fetch sessions (KIP-227)
//! are not kept: every response says session 0, and clients send full
//! fetches.
//!
//! **Leader epochs.** A partition's leader epoch is its log shard's
//! sequencer epoch, raised by every new sequencer. A request with an older
//! epoch than the current one gets `FENCED_LEADER_EPOCH` (the client
//! refreshes its metadata), a newer one `UNKNOWN_LEADER_EPOCH`.
//! OffsetForLeaderEpoch answers where an epoch's records end, which is how
//! clients detect a log that lost its tail in a failover (39948).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::time::Instant;
use uuid::Uuid;

use super::{DataPlane, DataReply, Session};
use crate::messages::{
    FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse,
    OffsetForLeaderEpochRequest, OffsetForLeaderEpochResponse, ProduceRequest, ProduceResponse,
    fetch_response, list_offsets_response, offset_for_leader_epoch_response, produce_response,
};
use crate::protocol::{ApiRange, DATA_PLANE, RequestHeader, api, err};
use crate::records::{ATTR_LOG_APPEND_TIME, Header, Record, RecordBatch};
use crate::topics::{Principal, TopicError, TopicKind};
use crate::wire::{CodecError, Message};

/// Records longer than this key are refused (the outbox's `k` column).
pub const MAX_KEY_BYTES: usize = 4096;
/// The most a produced batch may inflate to.
pub const MAX_UNCOMPRESSED_BATCH: usize = 64 * 1024 * 1024;

/// A topic as the data plane needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataTopic {
    /// `<database>.<stream>`.
    pub name: String,
    /// The stream.
    pub stream_id: u64,
    /// The topic id (Fetch v13).
    pub topic_id: Uuid,
    /// Partitions.
    pub partitions: i32,
    /// Event stream or change topic.
    pub kind: TopicKind,
    /// `cleanup.policy=compact`: records need keys.
    pub compact: bool,
    /// `stream.max_message_bytes`.
    pub max_message_bytes: usize,
}

/// What a request does to a topic (chapter 29: `INSERT` to produce,
/// `SELECT` to read).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// Fetch, ListOffsets, OffsetForLeaderEpoch.
    Read,
    /// Produce.
    Write,
}

/// An idempotent producer's batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerBatch {
    /// From InitProducerId.
    pub producer_id: i64,
    /// The producer's epoch.
    pub epoch: i16,
    /// The first record's sequence number.
    pub base_sequence: i32,
    /// Records in the batch.
    pub count: i32,
}

/// A record to append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceRecord {
    /// The key.
    pub key: Option<Vec<u8>>,
    /// The value (`None`: a tombstone).
    pub value: Option<Vec<u8>>,
    /// Headers, in order.
    pub headers: Vec<Header>,
    /// The producer's timestamp (ms since the Unix epoch).
    pub timestamp_ms: i64,
}

/// What phase one of a produce left for phase two.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendTicket {
    /// The partition.
    pub partition: i32,
    /// The outbox event id of the batch's first record.
    pub first_event: [u8; 16],
    /// Records in the batch.
    pub count: usize,
    /// The base offset, when already known (a duplicate already
    /// sequenced).
    pub base_offset: Option<i64>,
    /// A retried batch: nothing was appended.
    pub duplicate: bool,
}

/// A partition's bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bounds {
    /// The first retained offset.
    pub log_start: i64,
    /// The next offset.
    pub high_watermark: i64,
    /// The current leader epoch.
    pub leader_epoch: i32,
}

/// A record read from the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRecord {
    /// Its offset.
    pub offset: i64,
    /// Its timestamp.
    pub timestamp_ms: i64,
    /// The key.
    pub key: Option<Vec<u8>>,
    /// The value.
    pub value: Option<Vec<u8>>,
    /// Headers.
    pub headers: Vec<Header>,
    /// The epoch that appended it.
    pub leader_epoch: i32,
}

/// Live limits for a principal's requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// `stream.produce_wait_ms`.
    pub produce_wait: Duration,
    /// `kafka.fetch_max_wait_ms`.
    pub fetch_max_wait: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            produce_wait: Duration::from_millis(2000),
            fetch_max_wait: Duration::from_millis(500),
        }
    }
}

/// Streams as the data plane reads and writes them. Every call carries
/// the principal; errors are Kafka codes with SprigDB's message.
#[async_trait]
pub trait StreamData: Send + Sync + 'static {
    /// A topic by name, checked for `access` (`TOPIC_AUTHORIZATION_FAILED`,
    /// `UNKNOWN_TOPIC_OR_PARTITION`; a change topic's rows filtered by
    /// row-level security refuse reads).
    async fn topic(
        &self,
        p: &Principal,
        name: &str,
        access: Access,
    ) -> Result<DataTopic, TopicError>;
    /// A topic by id (Fetch v13; `UNKNOWN_TOPIC_ID`).
    async fn topic_by_id(
        &self,
        p: &Principal,
        id: Uuid,
        access: Access,
    ) -> Result<DataTopic, TopicError>;
    /// Phase one of a produce: the records into the outbox, in one
    /// transaction with the producer's sequence check.
    async fn append(
        &self,
        p: &Principal,
        t: &DataTopic,
        partition: i32,
        records: Vec<ProduceRecord>,
        producer: Option<ProducerBatch>,
    ) -> Result<AppendTicket, TopicError>;
    /// Phase two: the base offset once sequenced; `REQUEST_TIMED_OUT`
    /// (39946) at `deadline`.
    async fn sequenced(
        &self,
        p: &Principal,
        t: &DataTopic,
        ticket: &AppendTicket,
        deadline: Instant,
    ) -> Result<i64, TopicError>;
    /// A partition's bounds and leader epoch.
    async fn bounds(
        &self,
        p: &Principal,
        t: &DataTopic,
        partition: i32,
    ) -> Result<Bounds, TopicError>;
    /// Records from `offset`, at most `max_bytes` (the first record
    /// whatever its size). `OFFSET_OUT_OF_RANGE` outside `[log start, high
    /// watermark]`.
    async fn read(
        &self,
        p: &Principal,
        t: &DataTopic,
        partition: i32,
        offset: i64,
        max_bytes: usize,
    ) -> Result<(Bounds, Vec<StoredRecord>), TopicError>;
    /// Waits until one of `beyond`'s partitions (stream, partition,
    /// offset) has a high watermark above its offset, or `deadline`.
    async fn wait(&self, p: &Principal, beyond: &[(u64, i32, i64)], deadline: Instant);
    /// The first record at or after `ts` (ms): its offset and timestamp.
    /// `ts = -3` is the record with the largest timestamp.
    async fn offset_for_time(
        &self,
        p: &Principal,
        t: &DataTopic,
        partition: i32,
        ts: i64,
    ) -> Result<Option<(i64, i64)>, TopicError>;
    /// The largest epoch at most `epoch` and the offset where its records
    /// end.
    async fn epoch_end(
        &self,
        p: &Principal,
        t: &DataTopic,
        partition: i32,
        epoch: i32,
    ) -> Result<(i32, i64), TopicError>;
    /// The current leader epoch (Metadata v7+; 0 when unknown).
    async fn leader_epoch(&self, p: &Principal, stream_id: u64, partition: i32) -> i32;
    /// Limits for `p`.
    async fn limits(&self, p: &Principal) -> Limits;
}

/// The data plane over a [`StreamData`].
pub struct DataService {
    store: Arc<dyn StreamData>,
}

impl std::fmt::Debug for DataService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataService").finish_non_exhaustive()
    }
}

impl DataService {
    /// Over `store`.
    #[must_use]
    pub fn new(store: Arc<dyn StreamData>) -> Self {
        Self { store }
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

fn topic_err(code: i16, message: impl Into<String>) -> TopicError {
    TopicError::new(code, message)
}

/// Checks a produced `records` field and turns it into records to append.
///
/// # Errors
/// The Kafka code and message for the partition.
#[allow(clippy::too_many_lines)] // one check after another, in Kafka's order
pub fn check_batch(
    t: &DataTopic,
    records: Option<&[u8]>,
) -> Result<(Vec<ProduceRecord>, Option<ProducerBatch>), TopicError> {
    use crate::wire::CodecError as E;
    let bytes = records.ok_or_else(|| topic_err(err::CORRUPT_MESSAGE, "no records"))?;
    let batches =
        RecordBatch::decode_all(bytes, MAX_UNCOMPRESSED_BATCH, false).map_err(|e| match e {
            E::UnsupportedCompression(c) => topic_err(
                err::UNSUPPORTED_COMPRESSION_TYPE,
                format!("compression codec {c} is not supported"),
            ),
            other => topic_err(
                err::CORRUPT_MESSAGE,
                format!("corrupt record batch: {other}"),
            ),
        })?;
    let [batch] = batches.as_slice() else {
        return Err(topic_err(
            err::INVALID_RECORD,
            format!(
                "a produce carries exactly one record batch per partition, not {}",
                batches.len()
            ),
        ));
    };
    if batch.is_control() {
        return Err(topic_err(
            err::INVALID_RECORD,
            "control batches are not accepted",
        ));
    }
    if batch.is_transactional() {
        return Err(topic_err(
            err::INVALID_TXN_STATE,
            "39951 ER_SPRIG_KAFKA_TRANSACTIONS_UNSUPPORTED: transactional batches are not \
             supported; publish through SQL or logic inside a database transaction",
        ));
    }
    if batch.records.is_empty() {
        return Err(topic_err(err::INVALID_RECORD, "an empty record batch"));
    }
    let n = batch.records.len();
    if usize::try_from(batch.last_offset_delta).ok() != Some(n - 1)
        || batch
            .records
            .iter()
            .enumerate()
            .any(|(i, r)| usize::try_from(r.offset_delta).ok() != Some(i))
    {
        return Err(topic_err(
            err::INVALID_RECORD,
            "record offset deltas must be consecutive from 0",
        ));
    }
    let append_time = batch.attributes & ATTR_LOG_APPEND_TIME != 0 || batch.base_timestamp < 0;
    let now = now_ms();
    let mut out = Vec::with_capacity(n);
    for r in &batch.records {
        let key_len = r.key.as_ref().map_or(0, Vec::len);
        if key_len > MAX_KEY_BYTES {
            return Err(topic_err(
                err::MESSAGE_TOO_LARGE,
                format!(
                    "39944 ER_SPRIG_STREAM_RECORD_TOO_LARGE: a key of {key_len} bytes is above \
                     {MAX_KEY_BYTES}"
                ),
            ));
        }
        let size = key_len
            + r.value.as_ref().map_or(0, Vec::len)
            + r.headers
                .iter()
                .map(|h| h.key.len() + h.value.as_ref().map_or(0, Vec::len))
                .sum::<usize>();
        if size > t.max_message_bytes {
            return Err(topic_err(
                err::MESSAGE_TOO_LARGE,
                format!(
                    "39944 ER_SPRIG_STREAM_RECORD_TOO_LARGE: a record of {size} bytes is above \
                     stream.max_message_bytes ({})",
                    t.max_message_bytes
                ),
            ));
        }
        if t.compact && r.key.is_none() {
            return Err(topic_err(
                err::INVALID_RECORD,
                "39955 ER_SPRIG_STREAM_COMPACT_NEEDS_KEY: a record without a key on a compacted \
                 stream",
            ));
        }
        out.push(ProduceRecord {
            key: r.key.clone(),
            value: r.value.clone(),
            headers: r.headers.clone(),
            timestamp_ms: if append_time {
                now
            } else {
                batch.base_timestamp.saturating_add(r.timestamp_delta)
            },
        });
    }
    let producer = if batch.producer_id >= 0 {
        if batch.base_sequence < 0 || batch.producer_epoch < 0 {
            return Err(topic_err(
                err::INVALID_RECORD,
                "an idempotent batch needs a producer epoch and a base sequence",
            ));
        }
        Some(ProducerBatch {
            producer_id: batch.producer_id,
            epoch: batch.producer_epoch,
            base_sequence: batch.base_sequence,
            count: i32::try_from(n).unwrap_or(i32::MAX),
        })
    } else {
        None
    };
    Ok((out, producer))
}

/// Encodes records read from the log as record batches: one batch per run
/// of records with the same leader epoch, uncompressed. Offsets may have
/// gaps (compaction, row-level filtering): the offset deltas keep them.
///
/// # Errors
/// `Invalid` for more than `i32::MAX` records or bytes.
pub fn encode_batches(records: &[StoredRecord]) -> Result<Vec<u8>, CodecError> {
    let mut w = crate::wire::Writer::new();
    let mut i = 0;
    while i < records.len() {
        let epoch = records[i].leader_epoch;
        let mut j = i;
        while j < records.len() && records[j].leader_epoch == epoch && j - i < 10_000 {
            j += 1;
        }
        let run = &records[i..j];
        let base = run[0].offset;
        let base_ts = run.iter().map(|r| r.timestamp_ms).min().unwrap_or(0);
        let max_ts = run.iter().map(|r| r.timestamp_ms).max().unwrap_or(0);
        let delta =
            |o: i64| i32::try_from(o - base).map_err(|_| CodecError::Invalid("offset delta"));
        let batch = RecordBatch {
            base_offset: base,
            partition_leader_epoch: epoch,
            attributes: 0,
            last_offset_delta: delta(run[run.len() - 1].offset)?,
            base_timestamp: base_ts,
            max_timestamp: max_ts,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: run
                .iter()
                .map(|r| {
                    Ok(Record {
                        attributes: 0,
                        timestamp_delta: r.timestamp_ms - base_ts,
                        offset_delta: delta(r.offset)?,
                        key: r.key.clone(),
                        value: r.value.clone(),
                        headers: r.headers.clone(),
                    })
                })
                .collect::<Result<Vec<_>, CodecError>>()?,
        };
        batch.encode(&mut w)?;
        i = j;
    }
    Ok(w.into_inner())
}

/// The epoch check of Fetch, ListOffsets and OffsetForLeaderEpoch:
/// `current` is what the client last saw (-1: no check).
fn epoch_check(current: i32, b: &Bounds) -> i16 {
    if current < 0 || current == b.leader_epoch {
        err::NONE
    } else if current < b.leader_epoch {
        err::FENCED_LEADER_EPOCH
    } else {
        err::UNKNOWN_LEADER_EPOCH
    }
}

fn record_bytes(r: &StoredRecord) -> usize {
    r.key.as_ref().map_or(0, Vec::len)
        + r.value.as_ref().map_or(0, Vec::len)
        + r.headers
            .iter()
            .map(|h| h.key.len() + h.value.as_ref().map_or(0, Vec::len))
            .sum::<usize>()
        + 20
}

#[async_trait]
impl DataPlane for DataService {
    fn apis(&self) -> Vec<ApiRange> {
        DATA_PLANE.to_vec()
    }

    async fn handle(
        &self,
        s: &Session,
        h: &RequestHeader,
        body: &[u8],
    ) -> Result<DataReply, CodecError> {
        let v = h.api_version;
        match h.api_key {
            api::PRODUCE => {
                self.produce(s, ProduceRequest::from_bytes(body, v)?, v)
                    .await
            }
            api::FETCH => self
                .fetch(s, FetchRequest::from_bytes(body, v)?, v)
                .await
                .map(DataReply::Now),
            api::LIST_OFFSETS => self
                .list_offsets(s, ListOffsetsRequest::from_bytes(body, v)?, v)
                .await
                .map(DataReply::Now),
            api::OFFSET_FOR_LEADER_EPOCH => self
                .offset_for_leader_epoch(s, OffsetForLeaderEpochRequest::from_bytes(body, v)?, v)
                .await
                .map(DataReply::Now),
            _ => Err(CodecError::Invalid("not a data plane API")),
        }
    }

    async fn leader_epoch(&self, p: &Principal, stream_id: u64, partition: i32) -> i32 {
        self.store.leader_epoch(p, stream_id, partition).await
    }
}

/// One partition of a produce after phase one.
enum Phase1 {
    Done(produce_response::PartitionProduceResponse),
    Wait(i32, AppendTicket),
}

fn produce_error(index: i32, e: &TopicError) -> produce_response::PartitionProduceResponse {
    produce_response::PartitionProduceResponse {
        index,
        error_code: e.code,
        base_offset: -1,
        error_message: Some(e.message.clone()),
        ..Default::default()
    }
}

impl DataService {
    #[allow(clippy::too_many_lines)] // phase one, then phase two's future
    async fn produce(
        &self,
        s: &Session,
        q: ProduceRequest,
        v: i16,
    ) -> Result<DataReply, CodecError> {
        if q.acks == 0 {
            tracing::info!(tenant = %s.principal.tenant, user = %s.principal.user, client = %s.client_id, "kafka produce with acks=0 refused (39950)");
            return Err(CodecError::Invalid(
                "39950 ER_SPRIG_KAFKA_ACKS_ZERO: acks=0 is refused; SprigDB acknowledges after commit (use acks=1 or all)",
            ));
        }
        let refuse_all: Option<TopicError> = if q.transactional_id.is_some() {
            Some(topic_err(
                err::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
                "39951 ER_SPRIG_KAFKA_TRANSACTIONS_UNSUPPORTED: transactional.id is not \
                 supported; publish through SQL or logic inside a database transaction",
            ))
        } else if q.acks != 1 && q.acks != -1 {
            Some(topic_err(
                err::INVALID_REQUIRED_ACKS,
                format!("acks={} is not 1 or -1 (all)", q.acks),
            ))
        } else {
            None
        };
        let p = &s.principal;
        let mut topics: Vec<(String, Option<DataTopic>, Vec<Phase1>)> = Vec::new();
        for td in q.topic_data {
            let resolved = match &refuse_all {
                Some(e) => Err(e.clone()),
                None => self.store.topic(p, &td.name, Access::Write).await,
            };
            let mut parts = Vec::with_capacity(td.partition_data.len());
            for pd in td.partition_data {
                let r = match &resolved {
                    Err(e) => Err(e.clone()),
                    Ok(t) if pd.index < 0 || pd.index >= t.partitions => Err(topic_err(
                        err::UNKNOWN_TOPIC_OR_PARTITION,
                        format!("{} has no partition {}", t.name, pd.index),
                    )),
                    Ok(t) => match check_batch(t, pd.records.as_deref()) {
                        Err(e) => Err(e),
                        Ok((records, producer)) => {
                            self.store.append(p, t, pd.index, records, producer).await
                        }
                    },
                };
                parts.push(match r {
                    Ok(ticket) => Phase1::Wait(pd.index, ticket),
                    Err(e) => {
                        tracing::debug!(topic = %td.name, partition = pd.index, code = e.code, error = %e.message, "kafka produce refused");
                        Phase1::Done(produce_error(pd.index, &e))
                    }
                });
            }
            topics.push((td.name, resolved.ok(), parts));
        }
        let limits = self.store.limits(p).await;
        let wait = if q.timeout_ms > 0 {
            limits.produce_wait.min(Duration::from_millis(
                u64::try_from(q.timeout_ms).unwrap_or(0),
            ))
        } else {
            limits.produce_wait
        };
        let store = Arc::clone(&self.store);
        let principal = p.clone();
        let finish = async move {
            let deadline = Instant::now() + wait;
            let started = std::time::Instant::now();
            let mut resp = ProduceResponse::default();
            for (name, t, parts) in topics {
                let mut out = produce_response::TopicProduceResponse {
                    name,
                    ..Default::default()
                };
                for ph in parts {
                    out.partition_responses.push(match (ph, &t) {
                        (Phase1::Done(r), _) => r,
                        (Phase1::Wait(index, ticket), Some(t)) => {
                            match store.sequenced(&principal, t, &ticket, deadline).await {
                                Ok(base) => produce_response::PartitionProduceResponse {
                                    index,
                                    error_code: err::NONE,
                                    base_offset: base,
                                    ..Default::default()
                                },
                                Err(e) => produce_error(index, &e),
                            }
                        }
                        (Phase1::Wait(index, _), None) => produce_error(
                            index,
                            &topic_err(err::UNKNOWN_SERVER_ERROR, "topic lost"),
                        ),
                    });
                }
                resp.responses.push(out);
            }
            metrics::histogram!("sprig_kafka_produce_wait_seconds")
                .record(started.elapsed().as_secs_f64());
            resp.to_bytes(v)
        };
        Ok(DataReply::Later(Box::pin(finish)))
    }

    #[allow(clippy::too_many_lines)]
    async fn fetch(&self, s: &Session, q: FetchRequest, v: i16) -> Result<Vec<u8>, CodecError> {
        let p = &s.principal;
        let limits = self.store.limits(p).await;
        let max_wait = Duration::from_millis(u64::try_from(q.max_wait_ms.max(0)).unwrap_or(0))
            .min(limits.fetch_max_wait);
        let deadline = Instant::now() + max_wait;
        let max_bytes = usize::try_from(q.max_bytes.max(1)).unwrap_or(usize::MAX);
        let min_bytes = usize::try_from(q.min_bytes.max(0)).unwrap_or(0);
        // Resolve topics once.
        let mut topics: Vec<(
            Result<DataTopic, TopicError>,
            &crate::messages::fetch_request::FetchTopic,
        )> = Vec::with_capacity(q.topics.len());
        for ft in &q.topics {
            let t = if v >= 13 {
                self.store.topic_by_id(p, ft.topic_id, Access::Read).await
            } else {
                self.store.topic(p, &ft.topic, Access::Read).await
            };
            topics.push((t, ft));
        }
        loop {
            let mut resp = FetchResponse {
                error_code: err::NONE,
                session_id: 0,
                ..Default::default()
            };
            let mut total = 0usize;
            let mut errors = false;
            let mut beyond = Vec::new();
            for (t, ft) in &topics {
                let mut out = fetch_response::FetchableTopicResponse {
                    topic: ft.topic.clone(),
                    topic_id: ft.topic_id,
                    ..Default::default()
                };
                for fp in &ft.partitions {
                    let mut pr = fetch_response::PartitionData {
                        partition_index: fp.partition,
                        error_code: err::NONE,
                        high_watermark: -1,
                        last_stable_offset: -1,
                        log_start_offset: -1,
                        aborted_transactions: (q.isolation_level == 1).then(Vec::new),
                        preferred_read_replica: -1,
                        records: Some(Vec::new()),
                        ..Default::default()
                    };
                    let t = match t {
                        Err(e) => {
                            pr.error_code = if v >= 13 && e.code == err::UNKNOWN_TOPIC_OR_PARTITION
                            {
                                err::UNKNOWN_TOPIC_ID
                            } else {
                                e.code
                            };
                            errors = true;
                            out.partitions.push(pr);
                            continue;
                        }
                        Ok(t) => t,
                    };
                    if fp.partition < 0 || fp.partition >= t.partitions {
                        pr.error_code = err::UNKNOWN_TOPIC_OR_PARTITION;
                        errors = true;
                        out.partitions.push(pr);
                        continue;
                    }
                    let room = usize::try_from(fp.partition_max_bytes.max(0))
                        .unwrap_or(0)
                        .min(max_bytes.saturating_sub(total));
                    // Past the request's byte budget: no records, but the
                    // bounds (one more read would answer them; skip).
                    let budget = if total == 0 { room.max(1) } else { room };
                    let r = if budget == 0 {
                        self.store
                            .bounds(p, t, fp.partition)
                            .await
                            .map(|b| (b, Vec::new()))
                    } else {
                        self.store
                            .read(p, t, fp.partition, fp.fetch_offset, budget)
                            .await
                    };
                    match r {
                        Err(e) => {
                            pr.error_code = e.code;
                            if e.code == err::OFFSET_OUT_OF_RANGE
                                && let Ok(b) = self.store.bounds(p, t, fp.partition).await
                            {
                                pr.high_watermark = b.high_watermark;
                                pr.last_stable_offset = b.high_watermark;
                                pr.log_start_offset = b.log_start;
                            }
                            errors = true;
                        }
                        Ok((b, records)) => {
                            let code = epoch_check(fp.current_leader_epoch, &b);
                            pr.high_watermark = b.high_watermark;
                            pr.last_stable_offset = b.high_watermark;
                            pr.log_start_offset = b.log_start;
                            if code == err::NONE {
                                total += records.iter().map(record_bytes).sum::<usize>();
                                let next = records.last().map_or(fp.fetch_offset, |r| r.offset + 1);
                                beyond.push((t.stream_id, fp.partition, next));
                                pr.records = Some(encode_batches(&records)?);
                            } else {
                                pr.error_code = code;
                                errors = true;
                            }
                        }
                    }
                    out.partitions.push(pr);
                }
                resp.responses.push(out);
            }
            if errors
                || total >= min_bytes.max(1)
                || beyond.is_empty()
                || Instant::now() >= deadline
            {
                if total > 0 {
                    metrics::counter!("sprig_kafka_fetch_bytes_total")
                        .increment(u64::try_from(total).unwrap_or(0));
                }
                return resp.to_bytes(v);
            }
            self.store.wait(p, &beyond, deadline).await;
        }
    }

    async fn list_offsets(
        &self,
        s: &Session,
        q: ListOffsetsRequest,
        v: i16,
    ) -> Result<Vec<u8>, CodecError> {
        let p = &s.principal;
        let mut resp = ListOffsetsResponse::default();
        for lt in &q.topics {
            let t = self.store.topic(p, &lt.name, Access::Read).await;
            let mut out = list_offsets_response::ListOffsetsTopicResponse {
                name: lt.name.clone(),
                ..Default::default()
            };
            for lp in &lt.partitions {
                let mut pr = list_offsets_response::ListOffsetsPartitionResponse {
                    partition_index: lp.partition_index,
                    timestamp: -1,
                    offset: -1,
                    leader_epoch: -1,
                    ..Default::default()
                };
                let r: Result<(), TopicError> = async {
                    let t = t.as_ref().map_err(Clone::clone)?;
                    if lp.partition_index < 0 || lp.partition_index >= t.partitions {
                        return Err(topic_err(
                            err::UNKNOWN_TOPIC_OR_PARTITION,
                            "no such partition",
                        ));
                    }
                    let b = self.store.bounds(p, t, lp.partition_index).await?;
                    let code = epoch_check(lp.current_leader_epoch, &b);
                    if code != err::NONE {
                        return Err(topic_err(code, "leader epoch"));
                    }
                    pr.leader_epoch = b.leader_epoch;
                    match lp.timestamp {
                        -2 => pr.offset = b.log_start,
                        -1 => pr.offset = b.high_watermark,
                        ts if ts >= 0 || ts == -3 => {
                            if let Some((o, at)) = self
                                .store
                                .offset_for_time(p, t, lp.partition_index, ts)
                                .await?
                            {
                                pr.offset = o;
                                pr.timestamp = at;
                            } else if ts == -3 {
                                pr.offset = b.high_watermark;
                            }
                        }
                        _ => {
                            return Err(topic_err(
                                err::INVALID_REQUEST,
                                format!("timestamp {} is not a ListOffsets spec", lp.timestamp),
                            ));
                        }
                    }
                    Ok(())
                }
                .await;
                if let Err(e) = r {
                    pr.error_code = e.code;
                }
                out.partitions.push(pr);
            }
            resp.topics.push(out);
        }
        resp.to_bytes(v)
    }

    async fn offset_for_leader_epoch(
        &self,
        s: &Session,
        q: OffsetForLeaderEpochRequest,
        v: i16,
    ) -> Result<Vec<u8>, CodecError> {
        let p = &s.principal;
        let mut resp = OffsetForLeaderEpochResponse::default();
        for ot in &q.topics {
            let t = self.store.topic(p, &ot.topic, Access::Read).await;
            let mut out = offset_for_leader_epoch_response::OffsetForLeaderTopicResult {
                topic: ot.topic.clone(),
                ..Default::default()
            };
            for op in &ot.partitions {
                let mut pr = offset_for_leader_epoch_response::EpochEndOffset {
                    partition: op.partition,
                    leader_epoch: -1,
                    end_offset: -1,
                    ..Default::default()
                };
                let r: Result<(i32, i64), TopicError> = async {
                    let t = t.as_ref().map_err(Clone::clone)?;
                    if op.partition < 0 || op.partition >= t.partitions {
                        return Err(topic_err(err::UNKNOWN_TOPIC_OR_PARTITION, "no such partition"));
                    }
                    let b = self.store.bounds(p, t, op.partition).await?;
                    let code = epoch_check(op.current_leader_epoch, &b);
                    if code != err::NONE {
                        return Err(topic_err(code, "leader epoch"));
                    }
                    let (e, end) = self
                        .store
                        .epoch_end(p, t, op.partition, op.leader_epoch)
                        .await?;
                    if end < b.high_watermark && op.leader_epoch < b.leader_epoch {
                        tracing::debug!(topic = %t.name, partition = op.partition, epoch = op.leader_epoch, end, "kafka OffsetForLeaderEpoch: an older epoch ends before the high watermark");
                    }
                    Ok((e, end))
                }
                .await;
                match r {
                    Ok((e, end)) => {
                        pr.leader_epoch = e;
                        pr.end_offset = end;
                    }
                    Err(e) => pr.error_code = e.code,
                }
                out.partitions.push(pr);
            }
            resp.topics.push(out);
        }
        resp.to_bytes(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::Compression;

    fn topic(compact: bool) -> DataTopic {
        DataTopic {
            name: "shop.e".into(),
            stream_id: 1,
            topic_id: Uuid::nil(),
            partitions: 2,
            kind: TopicKind::Events,
            compact,
            max_message_bytes: 100,
        }
    }

    fn batch(keys: &[Option<&[u8]>], attributes: i16) -> RecordBatch {
        RecordBatch {
            attributes,
            last_offset_delta: i32::try_from(keys.len()).unwrap() - 1,
            base_timestamp: 1000,
            max_timestamp: 1000 + i64::try_from(keys.len()).unwrap(),
            producer_id: 7,
            producer_epoch: 0,
            base_sequence: 5,
            records: keys
                .iter()
                .enumerate()
                .map(|(i, k)| Record {
                    timestamp_delta: i64::try_from(i).unwrap(),
                    offset_delta: i32::try_from(i).unwrap(),
                    key: k.map(<[u8]>::to_vec),
                    value: Some(b"v".to_vec()),
                    ..Record::default()
                })
                .collect(),
            ..RecordBatch::default()
        }
    }

    #[test]
    fn batches_are_checked() {
        let t = topic(false);
        for c in [
            Compression::None,
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ] {
            let b = batch(&[Some(b"a"), None], i16::from(c.id()))
                .to_bytes()
                .unwrap();
            let (recs, prod) = check_batch(&t, Some(&b)).unwrap();
            assert_eq!(recs.len(), 2, "{c:?}");
            assert_eq!(recs[1].timestamp_ms, 1001);
            assert_eq!(
                prod,
                Some(ProducerBatch {
                    producer_id: 7,
                    epoch: 0,
                    base_sequence: 5,
                    count: 2
                })
            );
        }
        // Two batches.
        let one = batch(&[Some(b"a")], 0).to_bytes().unwrap();
        let two = [one.clone(), one.clone()].concat();
        assert_eq!(
            check_batch(&t, Some(&two)).unwrap_err().code,
            err::INVALID_RECORD
        );
        // A broken CRC.
        let mut bad = one.clone();
        let n = bad.len();
        bad[n - 1] ^= 1;
        assert_eq!(
            check_batch(&t, Some(&bad)).unwrap_err().code,
            err::CORRUPT_MESSAGE
        );
        // Gaps in offset deltas.
        let mut g = batch(&[Some(b"a"), Some(b"b")], 0);
        g.records[1].offset_delta = 2;
        g.last_offset_delta = 2;
        assert_eq!(
            check_batch(&t, Some(&g.to_bytes().unwrap()))
                .unwrap_err()
                .code,
            err::INVALID_RECORD
        );
        // Too large, keyless on a compacted stream, transactional.
        let mut big = batch(&[Some(b"a")], 0);
        big.records[0].value = Some(vec![0; 200]);
        let e = check_batch(&t, Some(&big.to_bytes().unwrap())).unwrap_err();
        assert_eq!(e.code, err::MESSAGE_TOO_LARGE);
        assert!(e.message.starts_with("39944"));
        let e =
            check_batch(&topic(true), Some(&batch(&[None], 0).to_bytes().unwrap())).unwrap_err();
        assert_eq!(e.code, err::INVALID_RECORD);
        assert!(e.message.starts_with("39955"));
        let e = check_batch(
            &t,
            Some(
                &batch(&[Some(b"a")], crate::records::ATTR_TRANSACTIONAL)
                    .to_bytes()
                    .unwrap(),
            ),
        )
        .unwrap_err();
        assert_eq!(e.code, err::INVALID_TXN_STATE);
        // Non-idempotent.
        let mut plain = batch(&[Some(b"a")], 0);
        plain.producer_id = -1;
        plain.producer_epoch = -1;
        plain.base_sequence = -1;
        assert_eq!(
            check_batch(&t, Some(&plain.to_bytes().unwrap())).unwrap().1,
            None
        );
    }

    #[test]
    fn stored_records_become_batches_by_epoch() {
        let rec = |offset, epoch| StoredRecord {
            offset,
            timestamp_ms: 2000 - offset,
            key: Some(b"k".to_vec()),
            value: None,
            headers: vec![Header {
                key: "h".into(),
                value: None,
            }],
            leader_epoch: epoch,
        };
        // A gap (offset 3 compacted away) and an epoch change.
        let recs = vec![rec(1, 4), rec(2, 4), rec(4, 4), rec(5, 6)];
        let bytes = encode_batches(&recs).unwrap();
        let back = RecordBatch::decode_all(&bytes, 1 << 20, false).unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(back[0].base_offset, 1);
        assert_eq!(back[0].last_offset_delta, 3);
        assert_eq!(back[0].partition_leader_epoch, 4);
        assert_eq!(
            back[0]
                .records
                .iter()
                .map(|r| r.offset_delta)
                .collect::<Vec<_>>(),
            vec![0, 1, 3]
        );
        assert_eq!(back[0].base_timestamp, 1996);
        assert_eq!(back[0].records[0].timestamp_delta, 3);
        assert_eq!(back[1].base_offset, 5);
        assert_eq!(back[1].partition_leader_epoch, 6);
        assert!(encode_batches(&[]).unwrap().is_empty());
    }

    #[test]
    fn epochs() {
        let b = Bounds {
            log_start: 0,
            high_watermark: 10,
            leader_epoch: 3,
        };
        assert_eq!(epoch_check(-1, &b), err::NONE);
        assert_eq!(epoch_check(3, &b), err::NONE);
        assert_eq!(epoch_check(2, &b), err::FENCED_LEADER_EPOCH);
        assert_eq!(epoch_check(4, &b), err::UNKNOWN_LEADER_EPOCH);
    }
}
