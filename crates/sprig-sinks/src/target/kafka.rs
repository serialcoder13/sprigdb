//! The Kafka target (chapter 51, "Kafka target"; build step 10): a
//! producer on `sprig-proto-kafka`'s codec, written in-house.
//!
//! ```text
//! connect(broker): the address checked by the destination rules
//!   (allowlist, residency; the broker's advertised address too), TLS for
//!   ssl and sasl_ssl, ApiVersions, then SASL PLAIN or SCRAM-SHA-256/512
//! at least once (the idempotent producer):
//!   InitProducerId(no transactional id); acks = all; one Produce per
//!   leader with every partition of the batch, sequence numbers per
//!   partition. A broker's definite refusal keeps the partition's next
//!   sequence; a lost answer (connection error, timeout) starts a new
//!   producer id, so a resend can't collide with a batch the broker may
//!   hold (duplicates are possible then; idempotency keys tell them apart)
//! exactly once (the transactional producer):
//!   open: tid := "sprig-" ‖ tenant ‖ "-" ‖ sink ‖ "-" ‖ task;
//!         InitProducerId(tid) fences older producers and aborts their
//!         open transaction; the offsets topic is read (read_committed)
//!         for the task's keys "<tenant>/<sink>/<task>/<unit>"
//!   deliver: [after any error: re-read the batch's units, skip records at
//!         or below them] AddPartitionsToTxn(data ∪ offsets partitions),
//!         Produce(data), Produce(offsets: encoded position ‖ epoch per
//!         unit), EndTxn(commit); an error re-initializes the producer,
//!         which aborts what is open
//! ```
//!
//! Requests on one producer are serialized (one in flight): the runtime
//! runs lanes in parallel, and keeping one request in flight per producer
//! is what keeps sequence numbers simple and gapless. `AddPartitionsToTxn`
//! and `EndTxn` are framed here (the codec has no definitions for them yet),
//! at version 3, the client versions of Kafka 3.x; KIP-890's client
//! versions (`EndTxn` 5, transactions without `AddPartitionsToTxn`) are for
//! Kafka 4.x brokers, which still accept these.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_kafka::client::Client;
use sprig_proto_kafka::compression::Compression;
use sprig_proto_kafka::messages::create_topics_request::{CreatableTopic, CreatableTopicConfig};
use sprig_proto_kafka::messages::fetch_request::{FetchPartition, FetchTopic};
use sprig_proto_kafka::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use sprig_proto_kafka::messages::metadata_request::MetadataRequestTopic;
use sprig_proto_kafka::messages::produce_request::{PartitionProduceData, TopicProduceData};
use sprig_proto_kafka::messages::{
    ApiVersionsRequest, ApiVersionsResponse, CreateTopicsRequest, CreateTopicsResponse,
    FetchRequest, FetchResponse, FindCoordinatorRequest, FindCoordinatorResponse,
    InitProducerIdRequest, InitProducerIdResponse, ListOffsetsRequest, ListOffsetsResponse,
    MetadataRequest, MetadataResponse, ProduceRequest, ProduceResponse,
};
use sprig_proto_kafka::records::{ATTR_TRANSACTIONAL, Header, Record, RecordBatch};
use sprig_proto_kafka::sasl::ScramMechanism;
use sprig_proto_kafka::wire::{Reader, Writer};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Mutex;
use zeroize::Zeroizing;

use super::{Conn, TargetCtx};
use crate::model::{
    Batch, BatchOutcome, ChangeOp, ConnKind, DestPosition, Guarantee, Outcome, Position,
    SinkRecord, TargetSpec, TaskEpoch, bad_option, delivery_failed, err,
};
use crate::runtime::{Target, TaskInfo};

/// Kafka error codes the producer reads.
mod code {
    pub(super) const NONE: i16 = 0;
    pub(super) const CORRUPT_MESSAGE: i16 = 2;
    pub(super) const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
    pub(super) const LEADER_NOT_AVAILABLE: i16 = 5;
    pub(super) const NOT_LEADER_OR_FOLLOWER: i16 = 6;
    pub(super) const MESSAGE_TOO_LARGE: i16 = 10;
    pub(super) const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
    pub(super) const COORDINATOR_NOT_AVAILABLE: i16 = 15;
    pub(super) const NOT_COORDINATOR: i16 = 16;
    pub(super) const RECORD_LIST_TOO_LARGE: i16 = 18;
    pub(super) const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
    pub(super) const INVALID_TIMESTAMP: i16 = 32;
    pub(super) const TOPIC_ALREADY_EXISTS: i16 = 36;
    pub(super) const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
    pub(super) const DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
    pub(super) const INVALID_PRODUCER_EPOCH: i16 = 47;
    pub(super) const CONCURRENT_TRANSACTIONS: i16 = 51;
    pub(super) const UNKNOWN_PRODUCER_ID: i16 = 59;
    pub(super) const INVALID_RECORD: i16 = 87;
    pub(super) const PRODUCER_FENCED: i16 = 90;
}

const API_ADD_PARTITIONS_TO_TXN: i16 = 24;
const API_END_TXN: i16 = 26;

/// Kafka's `Utils.murmur2` (seed `0x9747b28c`), bit for bit: the default
/// partitioner's hash, so a key lands on the partition Java producers and
/// chapter 45's streams choose.
#[must_use]
#[allow(
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation
)]
pub fn murmur2(data: &[u8]) -> i32 {
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;
    let len = data.len();
    let mut h: u32 = 0x9747_b28c ^ (len as u32);
    let mut chunks = data.chunks_exact(4);
    for c in &mut chunks {
        let mut k = u32::from_le_bytes([c[0], c[1], c[2], c[3]]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    let rest = chunks.remainder();
    if rest.len() >= 3 {
        h ^= u32::from(rest[2]) << 16;
    }
    if rest.len() >= 2 {
        h ^= u32::from(rest[1]) << 8;
    }
    if !rest.is_empty() {
        h ^= u32::from(rest[0]);
        h = h.wrapping_mul(M);
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h as i32
}

/// `toPositive(murmur2(key)) % partitions`.
#[must_use]
#[allow(clippy::cast_sign_loss, clippy::cast_possible_wrap)]
pub fn partition_for_key(key: &[u8], partitions: i32) -> i32 {
    let h = (murmur2(key) & 0x7fff_ffff) as u32;
    (h % partitions.max(1) as u32) as i32
}

/// `partitioner`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Partitioner {
    /// Kafka's default, on the record key.
    Murmur2,
    /// The source partition, modulo the topic's partitions.
    Source,
    /// One partition after another.
    RoundRobin,
}

/// `value_format`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueFormat {
    /// The value as JSON (change records as an envelope).
    Json,
    /// The value's own bytes.
    Raw,
    /// `CloudEvents`' Kafka binding, binary mode: the data in the value,
    /// attributes in `ce_` headers.
    CloudEvents,
}

/// `security`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Security {
    /// No TLS, no SASL.
    Plaintext,
    /// TLS.
    Ssl,
    /// SASL without TLS.
    SaslPlaintext,
    /// SASL over TLS.
    SaslSsl,
}

impl Security {
    fn tls(self) -> bool {
        matches!(self, Security::Ssl | Security::SaslSsl)
    }
    fn sasl(self) -> bool {
        matches!(self, Security::SaslPlaintext | Security::SaslSsl)
    }
}

/// A connection's settings, checked.
#[derive(Clone)]
struct ConnCfg {
    conn: Conn,
    bootstrap: Vec<(String, u16)>,
    security: Security,
    mechanism: String,
    user: Option<Zeroizing<String>>,
    password: Option<Zeroizing<String>>,
    tls: Option<Arc<rustls::ClientConfig>>,
    timeout: Duration,
}

/// A record ready for a batch: key, value, headers, timestamp.
type Rec = (Option<Vec<u8>>, Option<Vec<u8>>, Vec<Header>, i64);

/// Errors per partition: topic, partition, code.
type PartErrors = Vec<(String, i32, i16)>;

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

type Kc = Client<Box<dyn Io>>;

fn kafka_failed(msg: impl Into<String>) -> SprigError {
    delivery_failed(format!("kafka: {}", msg.into()))
}

fn fenced(msg: impl Into<String>) -> SprigError {
    err(SprigCode::SinkFenced, format!("kafka: {}", msg.into()))
}

#[allow(clippy::needless_pass_by_value)] // map_err's shape
fn client_err(e: sprig_proto_kafka::client::ClientError) -> SprigError {
    kafka_failed(e.to_string())
}

impl ConnCfg {
    fn new(conn: &Conn) -> Result<Self> {
        let bootstrap = conn
            .need("bootstrap")?
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| {
                let (h, p) = s
                    .rsplit_once(':')
                    .ok_or_else(|| bad_option(format!("bootstrap entry '{s}' needs host:port")))?;
                let p: u16 = p
                    .parse()
                    .map_err(|_| bad_option(format!("bootstrap entry '{s}': bad port")))?;
                Ok((
                    h.trim_start_matches('[').trim_end_matches(']').to_owned(),
                    p,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        if bootstrap.is_empty() {
            return Err(bad_option("bootstrap is empty"));
        }
        let security = match conn
            .opt("security")
            .unwrap_or_else(|| "plaintext".into())
            .to_ascii_lowercase()
            .as_str()
        {
            "plaintext" => Security::Plaintext,
            "ssl" => Security::Ssl,
            "sasl_plaintext" => Security::SaslPlaintext,
            "sasl_ssl" => Security::SaslSsl,
            other => return Err(bad_option(format!("security '{other}'"))),
        };
        let mechanism = conn
            .opt("sasl_mechanism")
            .unwrap_or_else(|| "PLAIN".into())
            .to_ascii_uppercase();
        if security.sasl()
            && !matches!(
                mechanism.as_str(),
                "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512"
            )
        {
            return Err(bad_option(format!("sasl_mechanism '{mechanism}'")));
        }
        let (user, password) = if security.sasl() {
            (
                Some(conn.secret("username")?),
                Some(conn.secret("password")?),
            )
        } else {
            (None, None)
        };
        let tls = if security.tls() {
            Some(super::tls_config(conn, &[])?)
        } else {
            None
        };
        let timeout = Duration::from_millis(conn.opt_u64("request_timeout_ms")?.unwrap_or(30_000));
        Ok(Self {
            conn: conn.clone(),
            bootstrap,
            security,
            mechanism,
            user,
            password,
            tls,
            timeout,
        })
    }

    /// Opens a connection to `host:port`: the checked address, TLS,
    /// `ApiVersions` and SASL. Returns the broker's API version ranges.
    async fn connect(&self, host: &str, port: u16) -> Result<(Kc, HashMap<i16, (i16, i16)>)> {
        let addr = self
            .conn
            .dest
            .resolve(host, port, Some(ConnKind::Kafka), Some(&self.conn.options))
            .await?;
        let tcp = tokio::time::timeout(self.timeout, tokio::net::TcpStream::connect(addr))
            .await
            .map_err(|_| kafka_failed(format!("connect to {host}:{port} timed out")))?
            .map_err(|e| kafka_failed(format!("connect to {host}:{port}: {e}")))?;
        let _ = tcp.set_nodelay(true);
        let io: Box<dyn Io> = if let Some(cfg) = &self.tls {
            let name = rustls::pki_types::ServerName::try_from(host.to_owned())
                .map_err(|e| bad_option(format!("broker name {host}: {e}")))?;
            let s = tokio_rustls::TlsConnector::from(Arc::clone(cfg))
                .connect(name, tcp)
                .await
                .map_err(|e| kafka_failed(format!("TLS to {host}:{port}: {e}")))?;
            Box::new(s)
        } else {
            Box::new(tcp)
        };
        let mut c: Kc = Client::new(io, "sprigdb-sink");
        let t = self.timeout;
        let r: ApiVersionsResponse = with_timeout(
            t,
            c.call(
                3,
                &ApiVersionsRequest {
                    client_software_name: "sprigdb-sink".into(),
                    client_software_version: env!("CARGO_PKG_VERSION").into(),
                    ..ApiVersionsRequest::default()
                },
            ),
        )
        .await?;
        if r.error_code != 0 {
            return Err(kafka_failed(format!("ApiVersions error {}", r.error_code)));
        }
        let versions = r
            .api_keys
            .iter()
            .map(|k| (k.api_key, (k.min_version, k.max_version)))
            .collect();
        if self.security.sasl() {
            let (Some(u), Some(p)) = (&self.user, &self.password) else {
                return Err(err(
                    SprigCode::SinkCredentialsInvalid,
                    "SASL needs username and password",
                ));
            };
            let r = match self.mechanism.as_str() {
                "SCRAM-SHA-256" => {
                    with_timeout(t, c.sasl_scram(ScramMechanism::Sha256, u, p)).await
                }
                "SCRAM-SHA-512" => {
                    with_timeout(t, c.sasl_scram(ScramMechanism::Sha512, u, p)).await
                }
                _ => with_timeout(t, c.sasl_plain(u, p)).await,
            };
            r.map_err(|e| {
                kafka_failed(format!(
                    "SASL {} login refused: {}",
                    self.mechanism, e.message
                ))
            })?;
        }
        Ok((c, versions))
    }
}

async fn with_timeout<T>(
    t: Duration,
    f: impl std::future::Future<Output = std::result::Result<T, sprig_proto_kafka::client::ClientError>>,
) -> Result<T> {
    match tokio::time::timeout(t, f).await {
        Ok(r) => r.map_err(client_err),
        Err(_) => Err(kafka_failed(format!(
            "no answer within {} ms",
            t.as_millis()
        ))),
    }
}

/// The version of `api` to use: our highest within the broker's range.
fn pick(versions: &HashMap<i16, (i16, i16)>, api: i16, ours: i16, min: i16) -> Result<i16> {
    let (lo, hi) = versions
        .get(&api)
        .copied()
        .ok_or_else(|| kafka_failed(format!("the broker doesn't serve API {api}")))?;
    let v = ours.min(hi);
    if v < lo.max(min) {
        return Err(kafka_failed(format!(
            "API {api}: the broker serves {lo}..{hi}, this client {min}..{ours}"
        )));
    }
    Ok(v)
}

/// A transaction API frame, version 3 (flexible): header v2.
fn txn_frame(
    api: i16,
    corr: i32,
    body: impl FnOnce(&mut Writer) -> sprig_proto_kafka::wire::Result<()>,
) -> Result<Vec<u8>> {
    let mut w = Writer::from_vec(vec![0; 4]);
    w.i16(api);
    w.i16(3);
    w.i32(corr);
    w.nullable_string(Some("sprigdb-sink"), false)
        .map_err(codec)?;
    w.uvarint(0);
    body(&mut w).map_err(codec)?;
    let size = i32::try_from(w.len() - 4).map_err(|_| kafka_failed("frame too large"))?;
    w.as_mut_slice()[..4].copy_from_slice(&size.to_be_bytes());
    Ok(w.into_inner())
}

#[allow(clippy::needless_pass_by_value)] // map_err's shape
fn codec(e: sprig_proto_kafka::wire::CodecError) -> SprigError {
    kafka_failed(format!("codec: {e}"))
}

/// `AddPartitionsToTxn` v3's request.
///
/// # Errors
/// A codec error.
pub fn add_partitions_frame(
    corr: i32,
    tid: &str,
    pid: i64,
    epoch: i16,
    topics: &[(String, Vec<i32>)],
) -> Result<Vec<u8>> {
    txn_frame(API_ADD_PARTITIONS_TO_TXN, corr, |w| {
        w.string(tid, true)?;
        w.i64(pid);
        w.i16(epoch);
        w.array(topics, true, |w, (t, ps)| {
            w.string(t, true)?;
            w.array(ps, true, |w, p| {
                w.i32(*p);
                Ok(())
            })?;
            w.uvarint(0);
            Ok(())
        })?;
        w.uvarint(0);
        Ok(())
    })
}

/// `EndTxn` v3's request.
///
/// # Errors
/// A codec error.
pub fn end_txn_frame(corr: i32, tid: &str, pid: i64, epoch: i16, commit: bool) -> Result<Vec<u8>> {
    txn_frame(API_END_TXN, corr, |w| {
        w.string(tid, true)?;
        w.i64(pid);
        w.i16(epoch);
        w.bool(commit);
        w.uvarint(0);
        Ok(())
    })
}

/// Reads a v3 response header (correlation id, tagged fields).
fn txn_response(frame: &[u8], corr: i32) -> Result<Reader<'_>> {
    let mut r = Reader::new(frame);
    let c = r.i32().map_err(codec)?;
    if c != corr {
        return Err(kafka_failed(format!("correlation id {c}, expected {corr}")));
    }
    r.tagged_fields(|_, _| Ok(false)).map_err(codec)?;
    Ok(r)
}

/// `AddPartitionsToTxn` v3's response: throttle and errors per partition.
///
/// # Errors
/// A codec error.
pub fn parse_add_partitions(frame: &[u8], corr: i32) -> Result<(i32, PartErrors)> {
    let mut r = txn_response(frame, corr)?;
    let throttle = r.i32().map_err(codec)?;
    let mut out = Vec::new();
    let topics = r
        .array(true, |r| {
            let name = r.string(true)?;
            let parts = r.array(true, |r| {
                let p = r.i32()?;
                let e = r.i16()?;
                r.tagged_fields(|_, _| Ok(false))?;
                Ok((p, e))
            })?;
            r.tagged_fields(|_, _| Ok(false))?;
            Ok((name, parts))
        })
        .map_err(codec)?;
    for (t, ps) in topics {
        for (p, e) in ps {
            out.push((t.clone(), p, e));
        }
    }
    Ok((throttle, out))
}

/// `EndTxn` v3's response: throttle and error code.
///
/// # Errors
/// A codec error.
pub fn parse_end_txn(frame: &[u8], corr: i32) -> Result<(i32, i16)> {
    let mut r = txn_response(frame, corr)?;
    let throttle = r.i32().map_err(codec)?;
    let e = r.i16().map_err(codec)?;
    Ok((throttle, e))
}

/// The broker side of a producer: connections, metadata, ids, sequences.
struct State {
    versions: HashMap<i16, (i16, i16)>,
    brokers: HashMap<i32, (String, u16)>,
    conns: HashMap<i32, Kc>,
    /// Topic → partition → leader.
    leaders: HashMap<String, Vec<i32>>,
    pid: i64,
    epoch: i16,
    seqs: HashMap<(String, i32), i32>,
    rr: u32,
    coordinator: Option<i32>,
    corr: i32,
    /// An error happened since the last delivery (exactly once: re-read
    /// the stored positions before resending).
    recheck: bool,
    /// Per offsets-topic key, what this producer committed last.
    committed: HashMap<String, Position>,
}

impl State {
    fn new() -> Self {
        Self {
            versions: HashMap::new(),
            brokers: HashMap::new(),
            conns: HashMap::new(),
            leaders: HashMap::new(),
            pid: -1,
            epoch: -1,
            seqs: HashMap::new(),
            rr: 0,
            coordinator: None,
            corr: 1_000_000,
            recheck: false,
            committed: HashMap::new(),
        }
    }

    fn next_corr(&mut self) -> i32 {
        self.corr = self.corr.wrapping_add(1).max(1_000_000);
        self.corr
    }
}

/// Exactly-once settings.
#[derive(Debug, Clone)]
struct Eo {
    tid: String,
    offsets_topic: String,
    key_prefix: String,
    txn_timeout_ms: i32,
}

/// The Kafka target.
pub struct KafkaTarget {
    cfg: ConnCfg,
    topic: String,
    key_expr: Option<String>,
    value_format: ValueFormat,
    partitioner: Partitioner,
    compression: Compression,
    ce_source: String,
    source_name: String,
    eo: Option<Eo>,
    st: Mutex<State>,
}

impl std::fmt::Debug for KafkaTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaTarget")
            .field("topic", &self.topic)
            .field("exactly_once", &self.eo.is_some())
            .finish_non_exhaustive()
    }
}

/// Builds the Kafka target of a sink.
///
/// # Errors
/// 39218 for bad options; 39231 for an unknown format or compression;
/// 39216 for missing credentials.
pub fn build(ctx: &TargetCtx) -> Result<Box<dyn Target>> {
    Ok(Box::new(KafkaTarget::new(ctx)?))
}

impl KafkaTarget {
    /// See [`build`].
    ///
    /// # Errors
    /// See [`build`].
    pub fn new(ctx: &TargetCtx) -> Result<Self> {
        let conn = ctx.conn()?;
        let cfg = ConnCfg::new(conn)?;
        let TargetSpec::Kafka { topic, key } = &ctx.sink.target else {
            return Err(bad_option("not a Kafka sink"));
        };
        if topic.is_empty() {
            return Err(bad_option("TOPIC is empty"));
        }
        let value_format = match ctx.opts.text("value_format", "json").as_str() {
            "json" => ValueFormat::Json,
            "raw" => ValueFormat::Raw,
            "cloudevents" => ValueFormat::CloudEvents,
            other => {
                return Err(err(
                    SprigCode::SinkFormatUnsupported,
                    format!("value_format '{other}': json, raw or cloudevents"),
                ));
            }
        };
        let partitioner = match ctx.opts.text("partitioner", "murmur2").as_str() {
            "murmur2" => Partitioner::Murmur2,
            "source" => Partitioner::Source,
            "round_robin" => Partitioner::RoundRobin,
            other => return Err(bad_option(format!("partitioner '{other}'"))),
        };
        let compression = match ctx.opts.text("compression", "zstd").as_str() {
            "none" => Compression::None,
            "gzip" => Compression::Gzip,
            "zstd" => Compression::Zstd,
            "snappy" => Compression::Snappy,
            "lz4" => Compression::Lz4,
            other => {
                return Err(err(
                    SprigCode::SinkFormatUnsupported,
                    format!("compression '{other}': none, gzip, zstd, snappy or lz4"),
                ));
            }
        };
        let key_expr = ctx
            .opts
            .get("key")
            .map(str::to_owned)
            .or_else(|| key.clone())
            .filter(|k| !k.is_empty() && k != "key");
        if let Some(k) = &key_expr {
            json_path_of(k)?;
        }
        let eo = (ctx.sink.guarantee == Guarantee::ExactlyOnce).then(|| Eo {
            tid: format!(
                "sprig-{}-{}-{}",
                ctx.task.tenant, ctx.task.sink, ctx.task.task_no
            ),
            offsets_topic: ctx
                .opts
                .get("kafka.offsets_topic")
                .or_else(|| conn.options.get("offsets_topic").and_then(Value::as_str))
                .unwrap_or("__sprig_sink_offsets")
                .to_owned(),
            key_prefix: format!("{}/", ctx.task.sink_key()),
            txn_timeout_ms: i32::try_from(
                ctx.opts
                    .u64("kafka.transaction_timeout_ms", 60_000)
                    .unwrap_or(60_000),
            )
            .unwrap_or(60_000),
        });
        Ok(Self {
            cfg,
            topic: topic.clone(),
            key_expr,
            value_format,
            partitioner,
            compression,
            ce_source: ctx.ce_source(),
            source_name: ctx.sink.source.name(),
            eo,
            st: Mutex::new(State::new()),
        })
    }

    // ----- connections, metadata, producer ids -----

    async fn bootstrap(&self, st: &mut State) -> Result<()> {
        if !st.brokers.is_empty() {
            return Ok(());
        }
        let mut last = None;
        for (h, p) in &self.cfg.bootstrap {
            match self.cfg.connect(h, *p).await {
                Ok((c, v)) => {
                    st.versions = v;
                    // A metadata call names the brokers; the bootstrap
                    // connection is kept as node -1 until then.
                    st.conns.insert(-1, c);
                    return self.refresh(st, &[]).await;
                }
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or_else(|| kafka_failed("no bootstrap broker")))
    }

    /// Metadata for `topics` (and the brokers).
    async fn refresh(&self, st: &mut State, extra: &[&str]) -> Result<()> {
        let mut names: Vec<&str> = vec![self.topic.as_str()];
        if let Some(eo) = &self.eo {
            names.push(eo.offsets_topic.as_str());
        }
        names.extend_from_slice(extra);
        let v = pick(&st.versions, 3, 12, 4)?;
        let req = MetadataRequest {
            topics: Some(
                names
                    .iter()
                    .map(|n| MetadataRequestTopic {
                        name: Some((*n).to_owned()),
                        ..MetadataRequestTopic::default()
                    })
                    .collect(),
            ),
            allow_auto_topic_creation: true,
            ..MetadataRequest::default()
        };
        let node = self.any_node(st).await?;
        let t = self.cfg.timeout;
        let r: Result<MetadataResponse> = {
            let c = st
                .conns
                .get_mut(&node)
                .ok_or_else(|| kafka_failed("no connection"))?;
            with_timeout(t, c.call(v, &req)).await
        };
        let r = match r {
            Ok(r) => r,
            Err(e) => {
                st.conns.remove(&node);
                return Err(e);
            }
        };
        st.brokers = r
            .brokers
            .iter()
            .map(|b| {
                (
                    b.node_id,
                    (b.host.clone(), u16::try_from(b.port).unwrap_or(9092)),
                )
            })
            .collect();
        for tp in r.topics {
            let Some(name) = tp.name else { continue };
            if tp.error_code != code::NONE || tp.partitions.is_empty() {
                st.leaders.remove(&name);
                continue;
            }
            let n = tp
                .partitions
                .iter()
                .map(|p| p.partition_index)
                .max()
                .unwrap_or(-1)
                + 1;
            let mut l = vec![-1; usize::try_from(n).unwrap_or(0)];
            for p in tp.partitions {
                if let Ok(i) = usize::try_from(p.partition_index) {
                    l[i] = p.leader_id;
                }
            }
            st.leaders.insert(name, l);
        }
        Ok(())
    }

    async fn any_node(&self, st: &mut State) -> Result<i32> {
        if let Some(n) = st.conns.keys().next().copied() {
            return Ok(n);
        }
        let ids: Vec<i32> = st.brokers.keys().copied().collect();
        for id in ids {
            if self.node(st, id).await.is_ok() {
                return Ok(id);
            }
        }
        st.brokers.clear();
        Box::pin(self.bootstrap(st)).await?;
        st.conns
            .keys()
            .next()
            .copied()
            .ok_or_else(|| kafka_failed("no broker reachable"))
    }

    /// A connection to broker `id`.
    async fn node(&self, st: &mut State, id: i32) -> Result<()> {
        if st.conns.contains_key(&id) {
            return Ok(());
        }
        let (h, p) = st
            .brokers
            .get(&id)
            .cloned()
            .ok_or_else(|| kafka_failed(format!("unknown broker {id}")))?;
        let (c, v) = self.cfg.connect(&h, p).await?;
        if st.versions.is_empty() {
            st.versions = v;
        }
        st.conns.insert(id, c);
        Ok(())
    }

    async fn call<Q, R>(&self, st: &mut State, node: i32, version: i16, q: &Q) -> Result<R>
    where
        Q: sprig_proto_kafka::wire::Message + Sync,
        R: sprig_proto_kafka::wire::Message,
    {
        self.node(st, node).await?;
        let t = self.cfg.timeout;
        let c = st
            .conns
            .get_mut(&node)
            .ok_or_else(|| kafka_failed("no connection"))?;
        let r = with_timeout(t, c.call(version, q)).await;
        if r.is_err() {
            st.conns.remove(&node);
        }
        r
    }

    async fn raw(&self, st: &mut State, node: i32, frame: &[u8]) -> Result<Vec<u8>> {
        self.node(st, node).await?;
        let t = self.cfg.timeout;
        let c = st
            .conns
            .get_mut(&node)
            .ok_or_else(|| kafka_failed("no connection"))?;
        let r = with_timeout(t, c.raw(frame)).await;
        if r.is_err() {
            st.conns.remove(&node);
        }
        r
    }

    async fn coordinator(&self, st: &mut State, tid: &str) -> Result<i32> {
        if let Some(c) = st.coordinator {
            return Ok(c);
        }
        let v = pick(&st.versions, 10, 3, 1)?;
        let node = self.any_node(st).await?;
        let r: FindCoordinatorResponse = self
            .call(
                st,
                node,
                v,
                &FindCoordinatorRequest {
                    key: tid.to_owned(),
                    key_type: 1,
                    ..FindCoordinatorRequest::default()
                },
            )
            .await?;
        if r.error_code != code::NONE {
            return Err(kafka_failed(format!(
                "FindCoordinator error {}: {}",
                r.error_code,
                r.error_message.unwrap_or_default()
            )));
        }
        st.brokers
            .entry(r.node_id)
            .or_insert((r.host.clone(), u16::try_from(r.port).unwrap_or(9092)));
        st.coordinator = Some(r.node_id);
        Ok(r.node_id)
    }

    /// `InitProducerId`: a new producer id (at least once), or the
    /// transactional id's next epoch (exactly once: fences older producers
    /// and aborts their open transaction).
    async fn init_producer(&self, st: &mut State) -> Result<()> {
        st.seqs.clear();
        let v = pick(&st.versions, 22, 4, 0)?;
        let mut tries = 0u32;
        loop {
            tries += 1;
            let (node, tid, timeout) = match &self.eo {
                Some(eo) => (
                    self.coordinator(st, &eo.tid).await?,
                    Some(eo.tid.clone()),
                    eo.txn_timeout_ms,
                ),
                None => (self.any_node(st).await?, None, 60_000),
            };
            let r: InitProducerIdResponse = self
                .call(
                    st,
                    node,
                    v,
                    &InitProducerIdRequest {
                        transactional_id: tid,
                        transaction_timeout_ms: timeout,
                        producer_id: -1,
                        producer_epoch: -1,
                        ..InitProducerIdRequest::default()
                    },
                )
                .await?;
            match r.error_code {
                code::NONE => {
                    st.pid = r.producer_id;
                    st.epoch = r.producer_epoch;
                    return Ok(());
                }
                code::CONCURRENT_TRANSACTIONS | code::COORDINATOR_LOAD_IN_PROGRESS
                    if tries < 50 =>
                {
                    // The fenced transaction is still being aborted.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                code::NOT_COORDINATOR | code::COORDINATOR_NOT_AVAILABLE if tries < 50 => {
                    st.coordinator = None;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                e => {
                    st.coordinator = None;
                    return Err(kafka_failed(format!("InitProducerId error {e}")));
                }
            }
        }
    }

    async fn ensure_ready(&self, st: &mut State) -> Result<()> {
        self.bootstrap(st).await?;
        if !st.leaders.contains_key(&self.topic) {
            self.refresh(st, &[]).await?;
        }
        if !st.leaders.contains_key(&self.topic) {
            return Err(kafka_failed(format!(
                "topic {} isn't known to the cluster (or not authorized)",
                self.topic
            )));
        }
        if st.pid < 0 {
            self.init_producer(st).await?;
        }
        Ok(())
    }

    // ----- records -----

    /// The record key: the `KEY` expression (a JSON path into the value),
    /// or the source key.
    fn key_of(&self, r: &SinkRecord) -> Option<Vec<u8>> {
        match &self.key_expr {
            None => r.key.clone(),
            Some(expr) => {
                let path = json_path_of(expr).ok()?;
                let mut v = &r.value_json();
                for p in &path {
                    v = match v {
                        Value::Object(m) => m.get(p)?,
                        Value::Array(a) => a.get(p.parse::<usize>().ok()?)?,
                        _ => return None,
                    };
                }
                match v {
                    Value::Null => None,
                    Value::String(s) => Some(s.as_bytes().to_vec()),
                    other => Some(other.to_string().into_bytes()),
                }
            }
        }
    }

    /// The Kafka record of a sink record: key, value and headers.
    #[must_use]
    pub fn encode_record(&self, r: &SinkRecord) -> (Option<Vec<u8>>, Option<Vec<u8>>, Vec<Header>) {
        let key = self.key_of(r);
        let value = match self.value_format {
            ValueFormat::Raw if r.op.is_none() => r.value.clone(),
            ValueFormat::Raw | ValueFormat::Json | ValueFormat::CloudEvents => {
                Some(json_value(r).to_string().into_bytes())
            }
        };
        let mut headers: Vec<Header> = r
            .headers
            .iter()
            .map(|(k, v)| Header {
                key: k.clone(),
                value: Some(v.clone()),
            })
            .collect();
        let h = |k: &str, v: String| Header {
            key: k.to_owned(),
            value: Some(v.into_bytes()),
        };
        headers.push(h("sprig.idempotency.key", r.idem_key.clone()));
        headers.push(h("sprig.source", self.source_name.clone()));
        if self.value_format == ValueFormat::CloudEvents {
            headers.push(h("ce_specversion", "1.0".into()));
            headers.push(h("ce_id", r.idem_key.clone()));
            headers.push(h("ce_source", self.ce_source.clone()));
            headers.push(h("ce_type", ce_type(r)));
            headers.push(h("ce_time", rfc3339(r.ts_ms)));
            if let Some(k) = r.key_text() {
                headers.push(h("ce_subject", k));
            }
            headers.push(h("content-type", "application/json".into()));
        }
        (key, value, headers)
    }

    fn partition(&self, st: &mut State, r: &SinkRecord, key: Option<&[u8]>, n: i32) -> i32 {
        match self.partitioner {
            Partitioner::Murmur2 => match key {
                Some(k) => partition_for_key(k, n),
                None => partition_for_key(r.idem_key.as_bytes(), n),
            },
            Partitioner::Source => {
                let p = match &r.pos {
                    Position::Stream { partition, .. } => i64::from(*partition),
                    Position::Change { shard, .. } => i64::from(*shard),
                    Position::Queue { .. } => {
                        i64::from(partition_for_key(r.idem_key.as_bytes(), n))
                    }
                };
                i32::try_from(p % i64::from(n.max(1))).unwrap_or(0)
            }
            Partitioner::RoundRobin => {
                st.rr = st.rr.wrapping_add(1);
                i32::try_from(st.rr % u32::try_from(n.max(1)).unwrap_or(1)).unwrap_or(0)
            }
        }
    }

    /// One batch per partition, in record order.
    fn record_batch(
        &self,
        st: &State,
        topic: &str,
        partition: i32,
        recs: &[Rec],
        transactional: bool,
    ) -> Result<(Vec<u8>, i32)> {
        let base_ts = recs.first().map_or(0, |r| r.3);
        let max_ts = recs.iter().map(|r| r.3).max().unwrap_or(base_ts);
        let seq = st
            .seqs
            .get(&(topic.to_owned(), partition))
            .copied()
            .unwrap_or(0);
        let mut attributes = i16::from(self.compression.id());
        if transactional {
            attributes |= ATTR_TRANSACTIONAL;
        }
        let n = i32::try_from(recs.len()).map_err(|_| kafka_failed("batch too large"))?;
        let b = RecordBatch {
            base_offset: 0,
            partition_leader_epoch: -1,
            attributes,
            last_offset_delta: n - 1,
            base_timestamp: base_ts,
            max_timestamp: max_ts,
            producer_id: st.pid,
            producer_epoch: st.epoch,
            base_sequence: seq,
            records: recs
                .iter()
                .enumerate()
                .map(|(i, (k, v, h, ts))| Record {
                    attributes: 0,
                    timestamp_delta: ts - base_ts,
                    offset_delta: i32::try_from(i).unwrap_or(i32::MAX),
                    key: k.clone(),
                    value: v.clone(),
                    headers: h.clone(),
                })
                .collect(),
        };
        Ok((b.to_bytes().map_err(codec)?, n))
    }

    /// Sends `parts` (topic, partition → batch bytes and record count) to
    /// their leaders; returns the error code per partition and the largest
    /// throttle time. A connection failure is `Err` (the answer is lost).
    async fn produce(
        &self,
        st: &mut State,
        parts: &BTreeMap<(String, i32), (Vec<u8>, i32)>,
    ) -> Result<(HashMap<(String, i32), i16>, i32)> {
        #[allow(clippy::type_complexity)]
        let mut by_leader: BTreeMap<i32, Vec<(&(String, i32), &Vec<u8>)>> = BTreeMap::new();
        for (tp, (bytes, _)) in parts {
            let leader = st
                .leaders
                .get(&tp.0)
                .and_then(|l| l.get(usize::try_from(tp.1).ok()?))
                .copied()
                .filter(|l| *l >= 0)
                .ok_or_else(|| kafka_failed(format!("no leader for {}-{}", tp.0, tp.1)))?;
            by_leader.entry(leader).or_default().push((tp, bytes));
        }
        let v = pick(&st.versions, 0, 9, 3)?;
        let mut codes = HashMap::new();
        let mut throttle = 0;
        for (leader, items) in by_leader {
            let mut topics: BTreeMap<&str, Vec<PartitionProduceData>> = BTreeMap::new();
            for ((t, p), bytes) in items {
                topics
                    .entry(t.as_str())
                    .or_default()
                    .push(PartitionProduceData {
                        index: *p,
                        records: Some(bytes.clone()),
                        ..PartitionProduceData::default()
                    });
            }
            let req = ProduceRequest {
                transactional_id: self.eo.as_ref().map(|e| e.tid.clone()),
                acks: -1,
                timeout_ms: i32::try_from(self.cfg.timeout.as_millis()).unwrap_or(30_000),
                topic_data: topics
                    .into_iter()
                    .map(|(name, partition_data)| TopicProduceData {
                        name: name.to_owned(),
                        partition_data,
                        ..TopicProduceData::default()
                    })
                    .collect(),
                ..ProduceRequest::default()
            };
            let r: ProduceResponse = self.call(st, leader, v, &req).await?;
            throttle = throttle.max(r.throttle_time_ms);
            for t in r.responses {
                for p in t.partition_responses {
                    codes.insert((t.name.clone(), p.index), p.error_code);
                }
            }
        }
        Ok((codes, throttle))
    }

    // ----- at least once -----

    async fn deliver_alo(&self, b: &Batch) -> Result<BatchOutcome> {
        let mut st = self.st.lock().await;
        let st = &mut *st;
        self.ensure_ready(st).await?;
        let n = i32::try_from(st.leaders.get(&self.topic).map_or(0, Vec::len)).unwrap_or(0);
        let mut by_part: BTreeMap<i32, Vec<usize>> = BTreeMap::new();
        let mut encoded = Vec::with_capacity(b.records.len());
        for (i, r) in b.records.iter().enumerate() {
            let (k, v, h) = self.encode_record(r);
            let p = self.partition(st, r, k.as_deref(), n);
            by_part.entry(p).or_default().push(i);
            encoded.push((k, v, h, r.ts_ms));
        }
        let mut parts = BTreeMap::new();
        for (p, idx) in &by_part {
            let recs: Vec<_> = idx.iter().map(|i| encoded[*i].clone()).collect();
            parts.insert(
                (self.topic.clone(), *p),
                self.record_batch(st, &self.topic, *p, &recs, false)?,
            );
        }
        let (codes, throttle) = match self.produce(st, &parts).await {
            Ok(x) => x,
            Err(e) => {
                // The answer is lost: the broker may hold the batch. A new
                // producer id keeps a later batch from reusing its
                // sequence numbers.
                st.pid = -1;
                return Err(e);
            }
        };
        let hint = if throttle > 0 {
            format!(" retry_after_ms={throttle}")
        } else {
            String::new()
        };
        let mut outs = vec![Outcome::Ok; b.records.len()];
        let mut dest_retry: Option<String> = None;
        let mut too_large = false;
        let mut reset = false;
        for ((t, p), (_, count)) in &parts {
            let code = codes.get(&(t.clone(), *p)).copied().unwrap_or(code::NONE);
            match code {
                code::NONE | code::DUPLICATE_SEQUENCE_NUMBER => {
                    *st.seqs.entry((t.clone(), *p)).or_insert(0) += *count;
                }
                code::MESSAGE_TOO_LARGE | code::RECORD_LIST_TOO_LARGE => too_large = true,
                code::CORRUPT_MESSAGE | code::INVALID_RECORD | code::INVALID_TIMESTAMP => {
                    for i in &by_part[p] {
                        outs[*i] = Outcome::Fail {
                            reason: format!(
                                "ER_SPRIG_SINK_DELIVERY_FAILED: kafka refused the record (error {code})"
                            ),
                        };
                    }
                }
                code::OUT_OF_ORDER_SEQUENCE_NUMBER
                | code::UNKNOWN_PRODUCER_ID
                | code::INVALID_PRODUCER_EPOCH => {
                    reset = true;
                    dest_retry = Some(format!("producer state lost (error {code})"));
                }
                code::NOT_LEADER_OR_FOLLOWER
                | code::LEADER_NOT_AVAILABLE
                | code::UNKNOWN_TOPIC_OR_PARTITION => {
                    st.leaders.remove(&self.topic);
                    dest_retry = Some(format!("leader moved (error {code}) on {t}-{p}"));
                }
                code::TOPIC_AUTHORIZATION_FAILED => {
                    dest_retry = Some(format!("not authorized to write {t} (error 29)"));
                }
                other => dest_retry = Some(format!("error {other} on {t}-{p}")),
            }
        }
        if reset {
            st.pid = -1;
        }
        if too_large {
            // Partitions written before stay written; their records come
            // again in the halves, as duplicates with equal keys.
            return Ok(BatchOutcome::TooLarge);
        }
        if let Some(why) = dest_retry {
            // Partitions that succeeded advanced their sequences; their
            // records are resent with the rest (at least once).
            return Err(kafka_failed(format!("{why}{hint}")));
        }
        if outs.iter().all(|o| *o == Outcome::Ok) {
            Ok(BatchOutcome::All(Outcome::Ok))
        } else {
            Ok(BatchOutcome::Each(outs))
        }
    }

    // ----- exactly once -----

    fn unit_key(eo: &Eo, p: &Position) -> String {
        let unit = p.unit_seq().map_or(0, |u| u.0);
        format!("{}{unit}", eo.key_prefix)
    }

    /// Creates the offsets topic (compacted, one partition) if it is
    /// missing and the credentials allow it.
    async fn ensure_offsets_topic(&self, st: &mut State, eo: &Eo) -> Result<()> {
        self.bootstrap(st).await?;
        if st.leaders.contains_key(&eo.offsets_topic) {
            return Ok(());
        }
        let v = pick(&st.versions, 19, 5, 2)?;
        let node = self.any_node(st).await?;
        let r: CreateTopicsResponse = self
            .call(
                st,
                node,
                v,
                &CreateTopicsRequest {
                    topics: vec![CreatableTopic {
                        name: eo.offsets_topic.clone(),
                        num_partitions: 1,
                        replication_factor: -1,
                        configs: vec![CreatableTopicConfig {
                            name: "cleanup.policy".into(),
                            value: Some("compact".into()),
                            ..CreatableTopicConfig::default()
                        }],
                        ..CreatableTopic::default()
                    }],
                    timeout_ms: 30_000,
                    validate_only: false,
                    ..CreateTopicsRequest::default()
                },
            )
            .await?;
        for t in &r.topics {
            if t.error_code != code::NONE && t.error_code != code::TOPIC_ALREADY_EXISTS {
                tracing::warn!(topic = %eo.offsets_topic, code = t.error_code, "offsets topic not created; it must exist");
            }
        }
        for _ in 0..50 {
            self.refresh(st, &[]).await?;
            if st.leaders.contains_key(&eo.offsets_topic) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(kafka_failed(format!(
            "the offsets topic {} doesn't exist and couldn't be created",
            eo.offsets_topic
        )))
    }

    /// The committed positions of this task's keys, from the offsets topic.
    async fn read_positions(
        &self,
        st: &mut State,
        eo: &Eo,
    ) -> Result<HashMap<String, (Position, u64)>> {
        let parts = st.leaders.get(&eo.offsets_topic).map_or(0, Vec::len);
        let mut out = HashMap::new();
        for p in 0..parts {
            let p = i32::try_from(p).unwrap_or(0);
            for rec in self.read_committed_in(st, &eo.offsets_topic, p).await? {
                let Some(k) = rec.key.as_deref().and_then(|k| std::str::from_utf8(k).ok()) else {
                    continue;
                };
                if !k.starts_with(&eo.key_prefix) {
                    continue;
                }
                match rec.value.as_deref().and_then(decode_offset_value) {
                    Some(v) => {
                        out.insert(k.to_owned(), v);
                    }
                    None => {
                        out.remove(k);
                    }
                }
            }
        }
        Ok(out)
    }

    /// Every committed record of `topic`-`partition`, in offset order
    /// (`read_committed`: aborted transactions and control records left out).
    #[allow(clippy::too_many_lines)] // the read_committed algorithm, whole
    async fn read_committed_in(
        &self,
        st: &mut State,
        topic: &str,
        partition: i32,
    ) -> Result<Vec<FetchedRecord>> {
        let leader = st
            .leaders
            .get(topic)
            .and_then(|l| l.get(usize::try_from(partition).ok()?))
            .copied()
            .filter(|l| *l >= 0)
            .ok_or_else(|| kafka_failed(format!("no leader for {topic}-{partition}")))?;
        let lv = pick(&st.versions, 2, 7, 2)?;
        let list = |ts: i64| ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 1,
            topics: vec![ListOffsetsTopic {
                name: topic.to_owned(),
                partitions: vec![ListOffsetsPartition {
                    partition_index: partition,
                    current_leader_epoch: -1,
                    timestamp: ts,
                    ..ListOffsetsPartition::default()
                }],
                ..ListOffsetsTopic::default()
            }],
            ..ListOffsetsRequest::default()
        };
        let mut bounds = [0i64; 2];
        for (i, ts) in [-2i64, -1].into_iter().enumerate() {
            let r: ListOffsetsResponse = self.call(st, leader, lv, &list(ts)).await?;
            let p = r
                .topics
                .first()
                .and_then(|t| t.partitions.first())
                .ok_or_else(|| kafka_failed("empty ListOffsets answer"))?;
            if p.error_code != code::NONE {
                return Err(kafka_failed(format!("ListOffsets error {}", p.error_code)));
            }
            bounds[i] = p.offset;
        }
        let (mut at, end) = (bounds[0], bounds[1]);
        let fv = pick(&st.versions, 1, 12, 4)?;
        let mut out = Vec::new();
        let mut aborted_pids: HashSet<i64> = HashSet::new();
        while at < end {
            let req = FetchRequest {
                replica_id: -1,
                max_wait_ms: 100,
                min_bytes: 1,
                max_bytes: 16 << 20,
                isolation_level: 1,
                session_id: 0,
                session_epoch: -1,
                topics: vec![FetchTopic {
                    topic: topic.to_owned(),
                    partitions: vec![FetchPartition {
                        partition,
                        current_leader_epoch: -1,
                        fetch_offset: at,
                        last_fetched_epoch: -1,
                        log_start_offset: -1,
                        partition_max_bytes: 8 << 20,
                        ..FetchPartition::default()
                    }],
                    ..FetchTopic::default()
                }],
                ..FetchRequest::default()
            };
            let r: FetchResponse = self.call(st, leader, fv, &req).await?;
            let pd = r
                .responses
                .into_iter()
                .next()
                .and_then(|t| t.partitions.into_iter().next())
                .ok_or_else(|| kafka_failed("empty Fetch answer"))?;
            if pd.error_code != code::NONE {
                return Err(kafka_failed(format!("Fetch error {}", pd.error_code)));
            }
            let mut aborted: Vec<(i64, i64)> = pd
                .aborted_transactions
                .unwrap_or_default()
                .iter()
                .map(|a| (a.first_offset, a.producer_id))
                .collect();
            aborted.sort_unstable();
            let batches =
                RecordBatch::decode_all(pd.records.as_deref().unwrap_or(&[]), 64 << 20, true)
                    .map_err(codec)?;
            if batches.is_empty() {
                break;
            }
            let mut ab = aborted.into_iter().peekable();
            for b in batches {
                while let Some((first, pid)) = ab.peek().copied() {
                    if first > b.last_offset() {
                        break;
                    }
                    aborted_pids.insert(pid);
                    ab.next();
                }
                at = at.max(b.last_offset() + 1);
                if b.is_control() {
                    // ABORT (0) or COMMIT (1) marker ends the transaction.
                    aborted_pids.remove(&b.producer_id);
                    continue;
                }
                if b.is_transactional() && aborted_pids.contains(&b.producer_id) {
                    continue;
                }
                for (i, r) in b.records.into_iter().enumerate() {
                    let offset = b.base_offset + i64::from(r.offset_delta);
                    let _ = i;
                    if offset < bounds[0] || offset >= end {
                        continue;
                    }
                    out.push(FetchedRecord {
                        offset,
                        key: r.key,
                        value: r.value,
                        headers: r
                            .headers
                            .into_iter()
                            .map(|h| (h.key, h.value.unwrap_or_default()))
                            .collect(),
                    });
                }
            }
        }
        Ok(out)
    }

    async fn txn_call(
        &self,
        st: &mut State,
        frame_of: impl Fn(i32) -> Result<Vec<u8>>,
    ) -> Result<(i32, Vec<u8>)> {
        let eo = self
            .eo
            .as_ref()
            .ok_or_else(|| kafka_failed("not transactional"))?;
        let node = self.coordinator(st, &eo.tid).await?;
        let corr = st.next_corr();
        let frame = frame_of(corr)?;
        let resp = self.raw(st, node, &frame).await?;
        Ok((corr, resp))
    }

    async fn add_partitions(
        &self,
        st: &mut State,
        eo: &Eo,
        topics: &[(String, Vec<i32>)],
    ) -> Result<()> {
        for tries in 0..50 {
            let (pid, epoch) = (st.pid, st.epoch);
            let (corr, resp) = self
                .txn_call(st, |c| add_partitions_frame(c, &eo.tid, pid, epoch, topics))
                .await?;
            let (_, errs) = parse_add_partitions(&resp, corr)?;
            let worst = errs.iter().map(|e| e.2).find(|e| *e != code::NONE);
            match worst {
                None => return Ok(()),
                Some(code::CONCURRENT_TRANSACTIONS | code::COORDINATOR_LOAD_IN_PROGRESS)
                    if tries < 49 =>
                {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Some(code::NOT_COORDINATOR | code::COORDINATOR_NOT_AVAILABLE) if tries < 49 => {
                    st.coordinator = None;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Some(code::PRODUCER_FENCED | code::INVALID_PRODUCER_EPOCH) => {
                    return Err(fenced("a newer producer holds the transactional id"));
                }
                Some(e) => return Err(kafka_failed(format!("AddPartitionsToTxn error {e}"))),
            }
        }
        Err(kafka_failed("AddPartitionsToTxn kept failing"))
    }

    async fn end_txn(&self, st: &mut State, eo: &Eo, commit: bool) -> Result<()> {
        for tries in 0..50 {
            let (pid, epoch) = (st.pid, st.epoch);
            let (corr, resp) = self
                .txn_call(st, |c| end_txn_frame(c, &eo.tid, pid, epoch, commit))
                .await?;
            let (_, e) = parse_end_txn(&resp, corr)?;
            match e {
                code::NONE => return Ok(()),
                code::CONCURRENT_TRANSACTIONS | code::COORDINATOR_LOAD_IN_PROGRESS
                    if tries < 49 =>
                {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                code::NOT_COORDINATOR | code::COORDINATOR_NOT_AVAILABLE if tries < 49 => {
                    st.coordinator = None;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                code::PRODUCER_FENCED | code::INVALID_PRODUCER_EPOCH => {
                    return Err(fenced("a newer producer holds the transactional id"));
                }
                e => return Err(kafka_failed(format!("EndTxn error {e}"))),
            }
        }
        Err(kafka_failed("EndTxn kept failing"))
    }

    /// Writes the batch and its positions in one transaction, without
    /// committing it (tests drop the producer here, as a crash would).
    #[allow(clippy::too_many_lines)] // one transaction, step by step
    async fn write_txn(&self, st: &mut State, eo: &Eo, b: &Batch) -> Result<Option<BatchOutcome>> {
        self.ensure_ready(st).await?;
        // After an error the last transaction may have committed: skip
        // what the destination already holds.
        let mut recs: Vec<&SinkRecord> = b.records.iter().collect();
        if st.recheck {
            let stored = self.read_positions(st, eo).await?;
            st.committed = stored.into_iter().map(|(k, v)| (k, v.0)).collect();
            st.recheck = false;
        }
        recs.retain(|r| {
            let k = Self::unit_key(eo, &r.pos);
            match (
                st.committed.get(&k).and_then(Position::unit_seq),
                r.pos.unit_seq(),
            ) {
                (Some((_, done)), Some((_, s))) => s > done,
                _ => true,
            }
        });
        if recs.is_empty() {
            return Ok(Some(BatchOutcome::All(Outcome::Ok)));
        }
        let n = i32::try_from(st.leaders.get(&self.topic).map_or(0, Vec::len)).unwrap_or(0);
        let mut by_part: BTreeMap<i32, Vec<Rec>> = BTreeMap::new();
        for r in &recs {
            let (k, v, h) = self.encode_record(r);
            let p = self.partition(st, r, k.as_deref(), n);
            by_part.entry(p).or_default().push((k, v, h, r.ts_ms));
        }
        // The positions, one record per unit, keyed for compaction.
        let on = i32::try_from(st.leaders.get(&eo.offsets_topic).map_or(1, Vec::len)).unwrap_or(1);
        let mut off_parts: BTreeMap<i32, Vec<Rec>> = BTreeMap::new();
        let ends: Vec<&Position> = b
            .ends
            .iter()
            .filter(|e| {
                recs.iter()
                    .any(|r| r.pos.unit_seq().map(|u| u.0) == e.unit_seq().map(|u| u.0))
            })
            .collect();
        let now = recs.last().map_or(0, |r| r.ts_ms);
        for e in &ends {
            let k = Self::unit_key(eo, e);
            let p = partition_for_key(k.as_bytes(), on);
            off_parts.entry(p).or_default().push((
                Some(k.into_bytes()),
                Some(encode_offset_value(e, st.epoch)),
                Vec::new(),
                now,
            ));
        }
        let mut topics: Vec<(String, Vec<i32>)> =
            vec![(self.topic.clone(), by_part.keys().copied().collect())];
        if !off_parts.is_empty() {
            topics.push((
                eo.offsets_topic.clone(),
                off_parts.keys().copied().collect(),
            ));
        }
        self.add_partitions(st, eo, &topics).await?;
        let mut parts = BTreeMap::new();
        for (p, rs) in &by_part {
            parts.insert(
                (self.topic.clone(), *p),
                self.record_batch(st, &self.topic, *p, rs, true)?,
            );
        }
        for (p, rs) in &off_parts {
            parts.insert(
                (eo.offsets_topic.clone(), *p),
                self.record_batch(st, &eo.offsets_topic, *p, rs, true)?,
            );
        }
        let (codes, throttle) = self.produce(st, &parts).await?;
        for ((t, p), (_, count)) in &parts {
            match codes.get(&(t.clone(), *p)).copied().unwrap_or(code::NONE) {
                code::NONE | code::DUPLICATE_SEQUENCE_NUMBER => {
                    *st.seqs.entry((t.clone(), *p)).or_insert(0) += *count;
                }
                code::MESSAGE_TOO_LARGE | code::RECORD_LIST_TOO_LARGE => {
                    return Ok(Some(BatchOutcome::TooLarge));
                }
                code::PRODUCER_FENCED | code::INVALID_PRODUCER_EPOCH => {
                    return Err(fenced("a newer producer holds the transactional id"));
                }
                code::CORRUPT_MESSAGE | code::INVALID_RECORD | code::INVALID_TIMESTAMP
                    if b.records.len() > 1 =>
                {
                    // Narrowed by halving: a transaction is all or nothing.
                    return Ok(Some(BatchOutcome::All(Outcome::Fail {
                        reason: "kafka refused a record".into(),
                    })));
                }
                code::CORRUPT_MESSAGE | code::INVALID_RECORD | code::INVALID_TIMESTAMP => {
                    return Ok(Some(BatchOutcome::All(Outcome::Fail {
                        reason: "ER_SPRIG_SINK_DELIVERY_FAILED: kafka refused the record".into(),
                    })));
                }
                c => {
                    let hint = if throttle > 0 {
                        format!(" retry_after_ms={throttle}")
                    } else {
                        String::new()
                    };
                    if matches!(
                        c,
                        code::NOT_LEADER_OR_FOLLOWER
                            | code::LEADER_NOT_AVAILABLE
                            | code::UNKNOWN_TOPIC_OR_PARTITION
                    ) {
                        st.leaders.remove(t);
                    }
                    return Err(kafka_failed(format!("Produce error {c} on {t}-{p}{hint}")));
                }
            }
        }
        for e in ends {
            st.committed.insert(Self::unit_key(eo, e), e.clone());
        }
        Ok(None)
    }

    async fn deliver_eo(&self, eo: &Eo, b: &Batch) -> Result<BatchOutcome> {
        let mut g = self.st.lock().await;
        let st = &mut *g;
        let r: Result<BatchOutcome> = async {
            if let Some(o) = self.write_txn(st, eo, b).await? {
                // Nothing written (or refused): end the transaction if one
                // is open.
                if !matches!(o, BatchOutcome::All(Outcome::Ok)) {
                    st.recheck = true;
                    self.end_txn(st, eo, false).await.ok();
                }
                return Ok(o);
            }
            self.end_txn(st, eo, true).await?;
            Ok(BatchOutcome::All(Outcome::Ok))
        }
        .await;
        if let Err(e) = &r {
            st.recheck = true;
            if e.kind != sprig_common::ErrorKind::Sprig(SprigCode::SinkFenced) {
                // A new epoch of the same transactional id aborts whatever
                // is open; the next attempt rechecks what committed.
                st.pid = -1;
                st.conns.clear();
                st.coordinator = None;
                st.committed.clear();
            }
        }
        r
    }

    /// Test hook: opens a transaction and writes `b` without committing it,
    /// as a producer that crashes before `EndTxn` does.
    ///
    /// # Errors
    /// The broker's.
    #[doc(hidden)]
    pub async fn write_uncommitted(&self, b: &Batch) -> Result<()> {
        let eo = self
            .eo
            .clone()
            .ok_or_else(|| kafka_failed("not transactional"))?;
        let mut g = self.st.lock().await;
        self.write_txn(&mut g, &eo, b).await.map(|_| ())
    }

    /// Reads every committed record of `topic` (`read_committed`), by
    /// partition, in offset order: what a `read_committed` consumer sees.
    ///
    /// # Errors
    /// The broker's.
    pub async fn read_committed(&self, topic: &str) -> Result<BTreeMap<i32, Vec<FetchedRecord>>> {
        let mut g = self.st.lock().await;
        let st = &mut *g;
        self.bootstrap(st).await?;
        self.refresh(st, &[topic]).await?;
        let n = st.leaders.get(topic).map_or(0, Vec::len);
        let mut out = BTreeMap::new();
        for p in 0..n {
            let p = i32::try_from(p).unwrap_or(0);
            out.insert(p, self.read_committed_in(st, topic, p).await?);
        }
        Ok(out)
    }
}

/// A record read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedRecord {
    /// Its offset.
    pub offset: i64,
    /// Its key.
    pub key: Option<Vec<u8>>,
    /// Its value.
    pub value: Option<Vec<u8>>,
    /// Its headers.
    pub headers: Vec<(String, Vec<u8>)>,
}

impl FetchedRecord {
    /// A header's value.
    #[must_use]
    pub fn header(&self, k: &str) -> Option<&[u8]> {
        self.headers
            .iter()
            .rev()
            .find(|h| h.0 == k)
            .map(|h| h.1.as_slice())
    }
}

/// An offsets-topic value: the encoded position, then the task epoch (8
/// bytes, big-endian).
#[must_use]
pub fn encode_offset_value(p: &Position, epoch: i16) -> Vec<u8> {
    let mut v = p.encode();
    v.extend_from_slice(&u64::try_from(epoch.max(0)).unwrap_or(0).to_be_bytes());
    v
}

/// Decodes [`encode_offset_value`].
#[must_use]
pub fn decode_offset_value(v: &[u8]) -> Option<(Position, u64)> {
    if v.len() < 9 {
        return None;
    }
    let (p, e) = v.split_at(v.len() - 8);
    Some((Position::decode(p)?, u64::from_be_bytes(e.try_into().ok()?)))
}

#[async_trait]
impl Target for KafkaTarget {
    fn kind(&self) -> &'static str {
        "kafka"
    }

    async fn open(&mut self, _task: &TaskInfo) -> Result<Vec<DestPosition>> {
        let Some(eo) = self.eo.clone() else {
            return Ok(Vec::new());
        };
        let mut g = self.st.lock().await;
        let st = &mut *g;
        self.ensure_offsets_topic(st, &eo).await?;
        // A new epoch of the transactional id: older producers are fenced
        // and their open transaction aborted.
        st.pid = -1;
        self.ensure_ready(st).await?;
        let stored = self.read_positions(st, &eo).await?;
        st.committed = stored
            .iter()
            .map(|(k, v)| (k.clone(), v.0.clone()))
            .collect();
        st.recheck = false;
        Ok(stored
            .into_values()
            .map(|(pos, epoch)| DestPosition {
                pos,
                epoch: TaskEpoch(epoch),
            })
            .collect())
    }

    async fn deliver(&self, b: &Batch) -> Result<BatchOutcome> {
        match &self.eo {
            Some(eo) => self.deliver_eo(eo, b).await,
            None => self.deliver_alo(b).await,
        }
    }

    fn exactly_once(&self) -> bool {
        self.eo.is_some()
    }
}

/// A `KEY` expression's JSON path: `$.a.b`, `value->>'$.a.b'` or
/// `value->'$.a'`.
///
/// # Errors
/// 39218 for anything else.
pub fn json_path_of(expr: &str) -> Result<Vec<String>> {
    let e = expr.trim();
    let path = if let Some(rest) = e.strip_prefix("value") {
        let rest = rest.trim_start();
        let rest = rest
            .strip_prefix("->>")
            .or_else(|| rest.strip_prefix("->"))
            .ok_or_else(|| bad_option(format!("KEY '{expr}': value->>'$.path' or $.path")))?;
        rest.trim().trim_matches('\'').to_owned()
    } else {
        e.to_owned()
    };
    let rest = path
        .strip_prefix('$')
        .ok_or_else(|| bad_option(format!("KEY '{expr}': a JSON path starts with $")))?;
    let parts: Vec<String> = rest
        .split('.')
        .filter(|s| !s.is_empty())
        .map(|s| s.trim_matches('"').to_owned())
        .collect();
    if parts.is_empty() {
        return Err(bad_option(format!("KEY '{expr}' names no field")));
    }
    Ok(parts)
}

/// The JSON value a record is sent as: change records as an envelope.
#[must_use]
pub fn json_value(r: &SinkRecord) -> Value {
    match r.op {
        Some(op) => json!({
            "op": op.letter(),
            "before": r.before.clone().unwrap_or(Value::Null),
            "after": r.after.clone().unwrap_or(Value::Null),
            "ts_ms": r.ts_ms,
        }),
        None => r.value_json(),
    }
}

/// `ce_type`: `sprig.change.<op>`, the record's `type` header, or
/// `sprig.event`.
#[must_use]
pub fn ce_type(r: &SinkRecord) -> String {
    if let Some(op) = r.op {
        let name = match op {
            ChangeOp::Create => "create",
            ChangeOp::Update => "update",
            ChangeOp::Delete => "delete",
            ChangeOp::Read => "read",
        };
        return format!("sprig.change.{name}");
    }
    r.headers
        .iter()
        .find(|h| h.0 == "type")
        .and_then(|h| std::str::from_utf8(&h.1).ok())
        .map_or_else(|| "sprig.event".to_owned(), str::to_owned)
}

/// RFC 3339 UTC time with milliseconds.
#[must_use]
pub fn rfc3339(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let milli = ms.rem_euclid(1000);
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{milli:03}Z",
        sod / 3600,
        (sod / 60) % 60,
        sod % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::stream_key;

    #[test]
    fn murmur2_matches_kafka() {
        let cases: &[(&[u8], i32)] = &[
            (b"21", -973_932_308),
            (b"foobar", -790_332_482),
            (b"a-little-bit-long-string", -985_981_536),
            (b"a-little-bit-longer-string", -1_486_304_829),
            (
                b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8",
                -58_897_971,
            ),
            (b"abc", 479_470_107),
        ];
        for (input, want) in cases {
            assert_eq!(murmur2(input), *want);
        }
        let p = partition_for_key(b"7001", 12);
        assert!((0..12).contains(&p));
        assert_eq!(partition_for_key(b"x", 0), 0);
    }

    #[test]
    fn transaction_frames() {
        let f = end_txn_frame(7, "t", 5, 2, true).unwrap();
        // size, key 26, version 3, corr 7, client id, empty tags, then
        // compact "t", pid, epoch, committed, empty tags.
        let mut want = vec![];
        want.extend_from_slice(&26i16.to_be_bytes());
        want.extend_from_slice(&3i16.to_be_bytes());
        want.extend_from_slice(&7i32.to_be_bytes());
        want.extend_from_slice(&12i16.to_be_bytes());
        want.extend_from_slice(b"sprigdb-sink");
        want.push(0);
        want.extend_from_slice(&[2, b't']);
        want.extend_from_slice(&5i64.to_be_bytes());
        want.extend_from_slice(&2i16.to_be_bytes());
        want.push(1);
        want.push(0);
        assert_eq!(&f[4..], &want[..]);
        assert_eq!(
            usize::try_from(i32::from_be_bytes(f[..4].try_into().unwrap())).unwrap(),
            want.len()
        );
        let f = add_partitions_frame(9, "t", 5, 2, &[("orders".into(), vec![0, 3])]).unwrap();
        assert!(f.ends_with(&[0, 0, 0, 0, 0, 0, 0, 3, 0, 0]), "{f:?}");
        // Responses.
        let mut r = vec![];
        r.extend_from_slice(&9i32.to_be_bytes());
        r.push(0); // tags
        r.extend_from_slice(&15i32.to_be_bytes()); // throttle
        r.push(2); // one topic
        r.push(7);
        r.extend_from_slice(b"orders");
        r.push(2); // one partition
        r.extend_from_slice(&3i32.to_be_bytes());
        r.extend_from_slice(&51i16.to_be_bytes());
        r.push(0);
        r.push(0);
        r.push(0);
        assert_eq!(
            parse_add_partitions(&r, 9).unwrap(),
            (15, vec![("orders".into(), 3, 51)])
        );
        let mut e = vec![];
        e.extend_from_slice(&4i32.to_be_bytes());
        e.push(0);
        e.extend_from_slice(&0i32.to_be_bytes());
        e.extend_from_slice(&90i16.to_be_bytes());
        e.push(0);
        assert_eq!(parse_end_txn(&e, 4).unwrap(), (0, 90));
        assert!(parse_end_txn(&e, 5).is_err());
    }

    #[test]
    fn offsets_values_round_trip() {
        let p = Position::Stream {
            stream: 1,
            partition: 2,
            offset: 300,
            epoch: 4,
        };
        let v = encode_offset_value(&p, 7);
        assert_eq!(decode_offset_value(&v), Some((p, 7)));
        assert_eq!(decode_offset_value(b"x"), None);
    }

    #[test]
    fn keys_formats_and_times() {
        assert_eq!(
            json_path_of("$.customer.id").unwrap(),
            vec!["customer", "id"]
        );
        assert_eq!(json_path_of("value->>'$.id'").unwrap(), vec!["id"]);
        assert!(json_path_of("id").is_err());
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(rfc3339(1_762_682_400_123), "2025-11-09T10:00:00.123Z");
        let mut r = SinkRecord {
            key: Some(b"k".to_vec()),
            value: Some(br#"{"a":1}"#.to_vec()),
            headers: vec![("type".into(), b"order.paid".to_vec())],
            op: None,
            before: None,
            after: None,
            ts_ms: 0,
            idem_key: stream_key(1, 0, 5, 0),
            pos: Position::Stream {
                stream: 1,
                partition: 0,
                offset: 5,
                epoch: 0,
            },
        };
        assert_eq!(ce_type(&r), "order.paid");
        assert_eq!(json_value(&r), json!({"a": 1}));
        r.op = Some(ChangeOp::Update);
        r.after = Some(json!({"id": 7}));
        assert_eq!(ce_type(&r), "sprig.change.update");
        assert_eq!(json_value(&r)["after"]["id"], 7);
    }

    #[test]
    fn record_batches_round_trip_with_every_codec() {
        for c in [
            Compression::None,
            Compression::Gzip,
            Compression::Zstd,
            Compression::Snappy,
            Compression::Lz4,
        ] {
            let b = RecordBatch {
                attributes: i16::from(c.id()) | ATTR_TRANSACTIONAL,
                last_offset_delta: 1,
                base_timestamp: 10,
                max_timestamp: 12,
                producer_id: 5,
                producer_epoch: 1,
                base_sequence: 40,
                records: vec![
                    Record {
                        offset_delta: 0,
                        key: Some(b"k".to_vec()),
                        value: Some(b"v1".to_vec()),
                        headers: vec![Header {
                            key: "sprig.idempotency.key".into(),
                            value: Some(b"s/1/0/0/0".to_vec()),
                        }],
                        ..Record::default()
                    },
                    Record {
                        offset_delta: 1,
                        timestamp_delta: 2,
                        value: Some(b"v2".to_vec()),
                        ..Record::default()
                    },
                ],
                ..RecordBatch::default()
            };
            let bytes = b.to_bytes().unwrap();
            let back = RecordBatch::decode_all(&bytes, 1 << 20, false).unwrap();
            assert_eq!(back, vec![b], "{c:?}");
        }
    }
}
