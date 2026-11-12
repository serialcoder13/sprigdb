//! The sink model (chapter 51, "Interfaces" and "Data structures"):
//! connections, sinks, tasks, and the records, batches and outcomes the
//! runtime moves between sources and targets.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sprig_common::{Result, SprigCode, SprigError};

/// A sink's id (from the object counter, as streams' are).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SinkId(pub u64);

/// A connection's id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ConnectionId(pub u64);

/// A task's number within its sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TaskNo(pub u32);

/// A task's epoch: bumped on every claim, so a stale worker is fenced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TaskEpoch(pub u64);

macro_rules! text_enum {
    ($(#[$doc:meta])* $name:ident { $($(#[$vdoc:meta])* $v:ident = $s:literal),* $(,)? }) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum $name { $($(#[$vdoc])* $v,)* }

        impl $name {
            /// Every value.
            pub const ALL: &'static [$name] = &[$($name::$v),*];

            /// The catalog's text.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self { $($name::$v => $s,)* }
            }

            /// Parses the catalog's text (any case).
            #[must_use]
            pub fn parse(s: &str) -> Option<Self> {
                let s = s.to_ascii_lowercase();
                match s.as_str() { $($s => Some($name::$v),)* _ => None }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

text_enum!(
    /// A connection's type (`sink_connections.kind`).
    ConnKind {
        /// HTTP endpoints.
        Webhook = "webhook",
        /// An outside Kafka cluster.
        Kafka = "kafka",
        /// S3 or an S3-compatible store.
        S3 = "s3",
        /// MySQL, MariaDB and MySQL-protocol servers.
        Mysql = "mysql",
        /// PostgreSQL and PostgreSQL-protocol servers.
        Postgresql = "postgresql",
    }
);

text_enum!(
    /// A sink's target (`sinks.target_kind`).
    TargetKind {
        /// `TO WEBHOOK`.
        Webhook = "webhook",
        /// `TO KAFKA`.
        Kafka = "kafka",
        /// `TO S3`.
        S3 = "s3",
        /// `TO DATABASE`.
        Database = "database",
        /// `TO SCRIPT`.
        Script = "script",
    }
);

text_enum!(
    /// A sink's source (`sinks.source_kind`).
    SourceKind {
        /// An event stream or change topic.
        Stream = "stream",
        /// A queue.
        Queue = "queue",
        /// The change feed of tables.
        Changes = "changes",
    }
);

text_enum!(
    /// `GUARANTEE`.
    Guarantee {
        /// The default.
        AtLeastOnce = "at_least_once",
        /// Where the destination allows it.
        ExactlyOnce = "exactly_once",
    }
);

text_enum!(
    /// `sinks.state` (chapter 51, "State machines").
    SinkState {
        /// Being created.
        Creating = "creating",
        /// Delivering.
        Running = "running",
        /// Paused by a user, or by failures (`state_reason` says which).
        Paused = "paused",
        /// Some batches retry for destination-wide reasons.
        Failing = "failing",
        /// Stopped for good until `RESET TO` (source lost).
        Failed = "failed",
        /// Being dropped.
        Dropping = "dropping",
    }
);

text_enum!(
    /// `sink_tasks.state`.
    TaskState {
        /// Not claimed.
        Idle = "idle",
        /// Delivering.
        Running = "running",
        /// Backing off.
        Retrying = "retrying",
        /// Paused with its sink.
        Paused = "paused",
        /// Failed with its sink.
        Failed = "failed",
    }
);

text_enum!(
    /// A script sink's language.
    ScriptLanguage {
        /// QuickJS.
        Javascript = "javascript",
        /// The native KIRun runtime.
        Kirun = "kirun",
        /// The shared Lua 5.1 runtime.
        Lua = "lua",
    }
);

impl TargetKind {
    /// The connection kinds a target accepts.
    #[must_use]
    pub fn accepts(self, c: ConnKind) -> bool {
        match self {
            TargetKind::Webhook => c == ConnKind::Webhook,
            TargetKind::Kafka => c == ConnKind::Kafka,
            TargetKind::S3 => c == ConnKind::S3,
            TargetKind::Database => matches!(c, ConnKind::Mysql | ConnKind::Postgresql),
            TargetKind::Script => true,
        }
    }
}

/// `START AT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "at", rename_all = "snake_case")]
pub enum StartAt {
    /// The source's end when the sink starts (the default).
    #[default]
    Latest,
    /// The source's beginning.
    Earliest,
    /// Every current row first (change sources).
    Snapshot,
    /// The first record at or after this time (ms since the epoch).
    Timestamp {
        /// Milliseconds since the Unix epoch.
        ms: i64,
    },
}

/// One credential's metadata: never its value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretMeta {
    /// `password`, `token`, ...
    pub name: String,
    /// Its version.
    pub version: u32,
    /// Who set it (user id).
    pub set_by: u32,
    /// When, as `YYYY-MM-DD HH:MM:SS.fff` UTC.
    pub set_at: String,
}

/// A connection (`sink_connections`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectionDef {
    /// Its id.
    pub id: ConnectionId,
    /// Its logical database.
    pub db_id: u32,
    /// Its name.
    pub name: String,
    /// Its type.
    pub kind: ConnKind,
    /// `WITH (...)`: endpoints, TLS, auth; never secrets.
    pub options: Map<String, Value>,
    /// `host:port` of every destination it names, checked.
    pub hosts: Vec<String>,
    /// The tags of the allowlist entry its hosts matched.
    pub residency_tags: Option<Map<String, Value>>,
    /// Its owner.
    pub owner_user_id: u32,
    /// Its current credentials (metadata only).
    pub secrets: Vec<SecretMeta>,
}

/// Where a sink's target connection comes from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ConnRef {
    /// A connection of the sink's tenant.
    Tenant {
        /// Its id.
        id: ConnectionId,
        /// Its name, as written.
        name: String,
    },
    /// A platform connection granted to the tenant.
    Platform {
        /// Its name.
        name: String,
    },
}

impl ConnRef {
    /// The name.
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            ConnRef::Tenant { name, .. } | ConnRef::Platform { name } => name,
        }
    }
}

/// A sink's source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SourceSpec {
    /// A stream or change topic.
    Stream {
        /// The stream's id.
        id: u64,
        /// Its name.
        name: String,
        /// Its partitions when the sink was created or last altered.
        partitions: u32,
    },
    /// A queue of the database's virtual host.
    Queue {
        /// The queue's id.
        id: u64,
        /// Its name.
        name: String,
    },
    /// Tables' committed changes.
    Changes {
        /// `(object id, name)` per table.
        objects: Vec<(u64, String)>,
    },
    /// `FROM STREAM s FAN IN [UNDER t]`: every member's instance of an
    /// inherited stream definition below the sink's tenant (chapter 51,
    /// "Fan-in sinks").
    FanIn {
        /// The declaring tenant.
        def_tenant: u64,
        /// The definition's object id there.
        def_object: u64,
        /// The database name of the instances.
        db: String,
        /// The stream's name.
        name: String,
        /// Partitions of each instance.
        partitions: u32,
        /// `UNDER t`: only that tenant's subtree.
        under: Option<u64>,
    },
    /// `FROM QUEUE q FAN IN [UNDER t]`: every member's instance of an
    /// inherited queue definition below the sink's tenant (chapter 51,
    /// "Fan-in sinks"; R9, r9queue).
    FanInQueue {
        /// The declaring tenant.
        def_tenant: u64,
        /// The definition's object id there.
        def_object: u64,
        /// The database (vhost) of the instances.
        db: String,
        /// The queue's name.
        name: String,
        /// `UNDER t`: only that tenant's subtree.
        under: Option<u64>,
    },
}

impl SourceSpec {
    /// The kind.
    #[must_use]
    pub fn kind(&self) -> SourceKind {
        match self {
            SourceSpec::Stream { .. } | SourceSpec::FanIn { .. } => SourceKind::Stream,
            SourceSpec::Queue { .. } | SourceSpec::FanInQueue { .. } => SourceKind::Queue,
            SourceSpec::Changes { .. } => SourceKind::Changes,
        }
    }

    /// The ids, as stored in `sinks.source_ids`.
    #[must_use]
    pub fn ids(&self) -> Vec<u64> {
        match self {
            SourceSpec::Stream { id, .. } | SourceSpec::Queue { id, .. } => vec![*id],
            SourceSpec::Changes { objects } => objects.iter().map(|o| o.0).collect(),
            SourceSpec::FanIn { def_object, .. } | SourceSpec::FanInQueue { def_object, .. } => {
                vec![*def_object]
            }
        }
    }

    /// The name(s), for `SHOW` and the `{source}` template variable.
    #[must_use]
    pub fn name(&self) -> String {
        match self {
            SourceSpec::Stream { name, .. }
            | SourceSpec::Queue { name, .. }
            | SourceSpec::FanIn { name, .. }
            | SourceSpec::FanInQueue { name, .. } => name.clone(),
            SourceSpec::Changes { objects } => objects
                .iter()
                .map(|o| o.1.as_str())
                .collect::<Vec<_>>()
                .join(", "),
        }
    }
}

/// A sink's target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TargetSpec {
    /// `TO WEBHOOK CONNECTION c [PATH '/p']`.
    Webhook {
        /// Appended to the connection's `url`.
        path: Option<String>,
    },
    /// `TO KAFKA CONNECTION c TOPIC 't' [KEY 'expr']`.
    Kafka {
        /// The destination topic.
        topic: String,
        /// The key expression (a JSON path into the value, or `key`).
        key: Option<String>,
    },
    /// `TO S3 CONNECTION c PREFIX 'template'`.
    S3 {
        /// The object name prefix template.
        prefix: String,
    },
    /// `TO DATABASE CONNECTION c TABLE 'name or template'`.
    Database {
        /// The destination table.
        table: String,
    },
    /// `TO SCRIPT ...`.
    Script {
        /// The language.
        language: ScriptLanguage,
        /// The inline body (`AS $$ ... $$`).
        source: Option<String>,
        /// A deployed function (`TO SCRIPT FUNCTION f`).
        function: Option<String>,
    },
}

impl TargetSpec {
    /// The kind.
    #[must_use]
    pub fn kind(&self) -> TargetKind {
        match self {
            TargetSpec::Webhook { .. } => TargetKind::Webhook,
            TargetSpec::Kafka { .. } => TargetKind::Kafka,
            TargetSpec::S3 { .. } => TargetKind::S3,
            TargetSpec::Database { .. } => TargetKind::Database,
            TargetSpec::Script { .. } => TargetKind::Script,
        }
    }
}

/// `DEAD LETTER TO`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DeadLetter {
    /// A stream (by id; the default is `<sink>_dead`).
    Stream {
        /// The stream's id.
        id: u64,
        /// Its name.
        name: String,
    },
    /// A queue.
    Queue {
        /// Its name.
        name: String,
    },
    /// An exchange.
    Exchange {
        /// Its name.
        name: String,
    },
    /// A queue source's own dead-letter exchange (nack without requeue).
    SourceQueue,
}

/// `COLUMNS (...)` or `EXCEPT COLUMNS (...)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum Projection {
    /// Only these columns.
    Only {
        /// Column names.
        columns: Vec<String>,
    },
    /// Every column but these.
    Except {
        /// Column names.
        columns: Vec<String>,
    },
}

/// A sink (`sinks`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SinkDef {
    /// Its id.
    pub id: SinkId,
    /// Its logical database.
    pub db_id: u32,
    /// Its name.
    pub name: String,
    /// `FROM ...`.
    pub source: SourceSpec,
    /// `WHERE ...`, as written.
    pub filter_sql: Option<String>,
    /// `COLUMNS` or `EXCEPT COLUMNS`.
    pub projection: Option<Projection>,
    /// `TRANSFORM f`.
    pub transform: Option<String>,
    /// `TO ...`.
    pub target: TargetSpec,
    /// The target's connection, then `USING CONNECTIONS` of scripts.
    pub connections: Vec<ConnRef>,
    /// `GUARANTEE`.
    pub guarantee: Guarantee,
    /// `DEAD LETTER TO`.
    pub dead_letter: Option<DeadLetter>,
    /// `START AT`.
    pub start_at: StartAt,
    /// The run-as user.
    pub owner_user_id: u32,
    /// The state.
    pub state: SinkState,
    /// Why it is paused, failing or failed.
    pub state_reason: Option<String>,
    /// Bumps on every `ALTER`.
    pub definition_ver: u64,
    /// `WITH (...)`: `sink.*` settings at the sink's scope and target
    /// options, normalized (keys without `sink.`).
    pub options: BTreeMap<String, String>,
}

impl SinkDef {
    /// The target's own connection, if any.
    #[must_use]
    pub fn target_connection(&self) -> Option<&ConnRef> {
        match self.target {
            TargetSpec::Script { .. } => None,
            _ => self.connections.first(),
        }
    }

    /// The consumer group of a stream source (chapter 51, "Where positions
    /// live").
    #[must_use]
    pub fn group(&self) -> String {
        format!("__sprig_sink_{}", self.id.0)
    }

    /// The paused state's reason says a user paused it (never probes).
    #[must_use]
    pub fn paused_by_user(&self) -> bool {
        self.state == SinkState::Paused
            && self
                .state_reason
                .as_deref()
                .is_some_and(|r| r.starts_with("user"))
    }
}

/// A sink task (`sink_tasks`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskRow {
    /// Its sink.
    pub sink: SinkId,
    /// Its number.
    pub task_no: TaskNo,
    /// What it reads: `{"partitions":[0,1]}`, `{"lane":0}`, ...
    pub source_part: Value,
    /// Bumped on every claim.
    pub epoch: TaskEpoch,
    /// The worker that holds it.
    pub worker_id: Option<String>,
    /// An S3 file being written (exactly once).
    pub pending: Option<Value>,
    /// Its state.
    pub state: TaskState,
    /// Consecutive failed attempts.
    pub attempts: u32,
    /// The last error.
    pub last_error: Option<String>,
    /// Records delivered (written at most every 10 s).
    pub delivered: u64,
}

/// Where a record came from (chapter 51's `Position`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "src", rename_all = "snake_case")]
pub enum Position {
    /// A stream record.
    Stream {
        /// The stream.
        stream: u64,
        /// Its partition.
        partition: u32,
        /// Its offset.
        offset: u64,
        /// The leader epoch that appended it.
        epoch: u32,
    },
    /// A queue message.
    Queue {
        /// The queue.
        queue: u64,
        /// Its message id.
        msg_id: String,
        /// The delivery token that settles it.
        delivery: String,
    },
    /// A change event.
    Change {
        /// Chapter 38's event id (cluster, GTID, row).
        event: String,
        /// The feed token to resume after it.
        token: String,
        /// The shard it came from.
        shard: u32,
        /// Its order within the shard, for settling.
        seq: u64,
    },
}

impl Position {
    /// The unit positions settle in order within (a partition, a shard),
    /// and the record's order in it; `None` for queue messages, which
    /// settle one by one.
    #[must_use]
    pub fn unit_seq(&self) -> Option<(u64, u64)> {
        match self {
            Position::Stream {
                stream,
                partition,
                offset,
                ..
            } => {
                // A fan-in task reads many member instances, each in its
                // slot (bit 62 and a counter): their partitions are units
                // of their own. Keyed by the partition alone, member A's
                // offset 7 and member B's offset 7 of partition 0 were one
                // entry, so settling B's could commit A's partition past
                // records A hadn't delivered, lost after a crash (R9,
                // r9tests). A plain stream task keeps the partition alone
                // (destination offset rows are keyed by it).
                let unit = if stream & (1 << 62) == 0 {
                    u64::from(*partition)
                } else {
                    (1 << 63) | ((stream & ((1 << 38) - 1)) << 24) | u64::from(*partition)
                };
                Some((unit, *offset))
            }
            Position::Change { shard, seq, .. } => Some((u64::from(*shard), *seq)),
            Position::Queue { .. } => None,
        }
    }

    /// A short, stable text for dead-letter headers and object names.
    #[must_use]
    pub fn text(&self) -> String {
        match self {
            Position::Stream {
                partition,
                offset,
                epoch,
                ..
            } => format!("{partition}/{offset}/{epoch}"),
            Position::Queue { msg_id, .. } => msg_id.clone(),
            Position::Change { event, .. } => event.clone(),
        }
    }

    /// The encoded form kept in destination offset tables.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Decodes [`Position::encode`].
    #[must_use]
    pub fn decode(b: &[u8]) -> Option<Self> {
        serde_json::from_slice(b).ok()
    }
}

/// A change record's operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ChangeOp {
    /// `c`: insert.
    Create,
    /// `u`: update.
    Update,
    /// `d`: delete.
    Delete,
    /// `r`: a snapshot row.
    Read,
}

impl ChangeOp {
    /// The one-letter form.
    #[must_use]
    pub const fn letter(self) -> &'static str {
        match self {
            ChangeOp::Create => "c",
            ChangeOp::Update => "u",
            ChangeOp::Delete => "d",
            ChangeOp::Read => "r",
        }
    }

    /// Parses the one-letter form.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "c" => Some(ChangeOp::Create),
            "u" => Some(ChangeOp::Update),
            "d" => Some(ChangeOp::Delete),
            "r" => Some(ChangeOp::Read),
            _ => None,
        }
    }
}

/// One record on its way out (chapter 51's `SinkRecord`).
#[derive(Debug, Clone, PartialEq)]
pub struct SinkRecord {
    /// The record key.
    pub key: Option<Vec<u8>>,
    /// The value as bytes (stream records, queue bodies).
    pub value: Option<Vec<u8>>,
    /// Headers.
    pub headers: Vec<(String, Vec<u8>)>,
    /// For change records: the operation.
    pub op: Option<ChangeOp>,
    /// For change records: the row before.
    pub before: Option<Value>,
    /// For change records: the row after.
    pub after: Option<Value>,
    /// The record's own time (event time), ms since the epoch.
    pub ts_ms: i64,
    /// Stable across redeliveries.
    pub idem_key: String,
    /// Where it came from.
    pub pos: Position,
}

impl SinkRecord {
    /// The value as JSON: parsed when it is JSON text, a string when it
    /// is other UTF-8, `{"base64": ...}` otherwise, `null` when absent.
    #[must_use]
    pub fn value_json(&self) -> Value {
        if let Some(after) = &self.after {
            return after.clone();
        }
        bytes_json(self.value.as_deref())
    }

    /// The key as text (lossy), if any.
    #[must_use]
    pub fn key_text(&self) -> Option<String> {
        self.key
            .as_ref()
            .map(|k| String::from_utf8_lossy(k).into_owned())
    }

    /// The headers as a JSON object (UTF-8 values as strings).
    #[must_use]
    pub fn headers_json(&self) -> Value {
        let mut m = Map::new();
        for (k, v) in &self.headers {
            m.insert(k.clone(), bytes_json_text(v));
        }
        Value::Object(m)
    }

    /// Bytes the record holds, for batch and in-flight limits.
    #[must_use]
    pub fn size(&self) -> u64 {
        let mut n = self.key.as_ref().map_or(0, Vec::len)
            + self.value.as_ref().map_or(0, Vec::len)
            + self.idem_key.len()
            + 64;
        for (k, v) in &self.headers {
            n += k.len() + v.len();
        }
        if let Some(a) = &self.after {
            n += a.to_string().len();
        }
        if let Some(b) = &self.before {
            n += b.to_string().len();
        }
        n as u64
    }

    /// The JSON form scripts and the `json` webhook format see.
    #[must_use]
    pub fn to_json(&self) -> Value {
        let mut m = Map::new();
        m.insert(
            "key".into(),
            self.key.as_deref().map_or(Value::Null, bytes_json_text),
        );
        m.insert("value".into(), self.value_json());
        m.insert("headers".into(), self.headers_json());
        if let Some(op) = self.op {
            m.insert("op".into(), json!(op.letter()));
            m.insert("before".into(), self.before.clone().unwrap_or(Value::Null));
            m.insert("after".into(), self.after.clone().unwrap_or(Value::Null));
        }
        m.insert("ts".into(), json!(self.ts_ms));
        // A member's record (fan-in sinks, inherited streams): its tenant.
        if let Some((_, t)) = self.headers.iter().find(|(k, _)| k == "x-sprig-tenant") {
            m.insert("tenant".into(), json!(String::from_utf8_lossy(t)));
        }
        m.insert("idempotencyKey".into(), json!(self.idem_key));
        m.insert("position".into(), json!(self.pos.text()));
        Value::Object(m)
    }
}

/// Bytes as JSON: a JSON document parsed, other UTF-8 as a string,
/// anything else `{"base64": ...}`.
#[must_use]
pub fn bytes_json(b: Option<&[u8]>) -> Value {
    let Some(b) = b else {
        return Value::Null;
    };
    if let Ok(v) = serde_json::from_slice::<Value>(b) {
        return v;
    }
    bytes_json_text(b)
}

/// Bytes as a JSON string when UTF-8, else `{"base64": ...}`.
#[must_use]
pub fn bytes_json_text(b: &[u8]) -> Value {
    if let Ok(s) = std::str::from_utf8(b) {
        Value::String(s.to_owned())
    } else {
        use base64::Engine as _;
        json!({"base64": base64::engine::general_purpose::STANDARD.encode(b)})
    }
}

/// The idempotency key of a stream record (chapter 51, "Idempotency
/// keys").
#[must_use]
pub fn stream_key(stream: u64, partition: u32, offset: u64, epoch: u32) -> String {
    format!("s/{stream}/{partition}/{offset}/{epoch}")
}

/// The idempotency key of a queue message.
#[must_use]
pub fn queue_key(queue: u64, msg_id: &str) -> String {
    format!("q/{queue}/{msg_id}")
}

/// The idempotency key of a change event.
#[must_use]
pub fn change_key(cluster: &str, gtid: &str, row: u32) -> String {
    format!("c/{cluster}/{gtid}/{row}")
}

/// A batch's id: the first 16 bytes of SHA-256 over its record keys, in
/// hex (stable, since a batch's contents are fixed once cut).
#[must_use]
pub fn batch_id(records: &[SinkRecord]) -> String {
    let mut h = ring::digest::Context::new(&ring::digest::SHA256);
    for r in records {
        h.update(r.idem_key.as_bytes());
        h.update(b"\n");
    }
    hex(&h.finish().as_ref()[..16])
}

/// Lower-case hex.
#[must_use]
pub fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s
}

/// One batch on its way to a target.
#[derive(Debug, Clone, PartialEq)]
pub struct Batch {
    /// Its id ([`batch_id`]).
    pub id: String,
    /// The records, in source order within each key.
    pub records: Vec<SinkRecord>,
    /// Attempts so far (1 on the first send).
    pub attempt: u32,
    /// The lane.
    pub lane: u16,
    /// Exactly-once targets: the positions to make durable with the data,
    /// the batch's last record of each partition or shard (tasks under
    /// `EXACTLY ONCE` have one lane, so each is a prefix of its unit). A
    /// target skips records at or below the position it already holds for
    /// their unit, so a resent batch whose answer was lost applies nothing
    /// twice.
    pub ends: Vec<Position>,
}

/// The last position of each unit among `records` (in unit order).
#[must_use]
pub fn unit_ends(records: &[SinkRecord]) -> Vec<Position> {
    let mut m: std::collections::BTreeMap<u64, &Position> = std::collections::BTreeMap::new();
    for r in records {
        if let Some((u, _)) = r.pos.unit_seq() {
            m.insert(u, &r.pos);
        }
    }
    m.into_values().cloned().collect()
}

impl Batch {
    /// A batch of `records` (its id computed).
    #[must_use]
    pub fn new(records: Vec<SinkRecord>, lane: u16) -> Self {
        Self {
            id: batch_id(&records),
            records,
            attempt: 1,
            lane,
            ends: Vec::new(),
        }
    }

    /// Bytes of its records.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.records.iter().map(SinkRecord::size).sum()
    }
}

/// The outcome of one record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Delivered.
    Ok,
    /// Try again, after at least `after` when the destination said so.
    Retry {
        /// The destination's hint (`Retry-After`, ...).
        after: Option<Duration>,
        /// Why.
        reason: String,
    },
    /// Failed for good: dead-letter it.
    Fail {
        /// Why.
        reason: String,
    },
}

/// What a target says about one delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchOutcome {
    /// One outcome for the whole batch. `Retry` is a destination-wide
    /// failure (it counts towards pausing); `Fail` of a batch of several
    /// records is narrowed by halving until the failing record is alone.
    All(Outcome),
    /// One outcome per record, in order. `Retry` here is a one-record
    /// retry, up to `sink.record_max_attempts`.
    Each(Vec<Outcome>),
    /// The batch is too large for the destination (HTTP 413): halve it
    /// without counting a failure; a single record fails (39225).
    TooLarge,
}

/// The position an exactly-once target keeps with its data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DestPosition {
    /// The last record made durable.
    pub pos: Position,
    /// The task epoch that wrote it.
    pub epoch: TaskEpoch,
}

/// A sink-layer error.
#[must_use]
pub fn err(code: SprigCode, msg: impl Into<String>) -> SprigError {
    let name = code.name();
    let msg = msg.into();
    SprigError::new(code, format!("{name}: {msg}"))
}

/// 39218 for an option.
#[must_use]
pub fn bad_option(msg: impl Into<String>) -> SprigError {
    err(SprigCode::SinkOptionInvalid, msg)
}

/// A retryable delivery failure (39221).
#[must_use]
pub fn delivery_failed(msg: impl Into<String>) -> SprigError {
    let mut e = err(SprigCode::SinkDeliveryFailed, msg);
    e.retryable = true;
    e
}

/// Reads a `WITH` option as text.
#[must_use]
pub fn opt<'a>(options: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    options.get(key).and_then(Value::as_str)
}

/// Reads a `WITH` option as a number (text or number).
///
/// # Errors
/// 39218 when present but not a number.
pub fn opt_u64(options: &Map<String, Value>, key: &str) -> Result<Option<u64>> {
    match options.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => n
            .as_u64()
            .map(Some)
            .ok_or_else(|| bad_option(format!("{key} must be a whole number"))),
        Some(Value::String(s)) => s
            .trim()
            .parse()
            .map(Some)
            .map_err(|_| bad_option(format!("{key} must be a whole number, not '{s}'"))),
        Some(v) => Err(bad_option(format!("{key} must be a number, not {v}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(k: &str) -> SinkRecord {
        SinkRecord {
            key: Some(k.as_bytes().to_vec()),
            value: Some(br#"{"a":1}"#.to_vec()),
            headers: vec![("h".into(), b"v".to_vec()), ("b".into(), vec![0xff])],
            op: None,
            before: None,
            after: None,
            ts_ms: 1,
            idem_key: stream_key(88, 5, 1205, 3),
            pos: Position::Stream {
                stream: 88,
                partition: 5,
                offset: 1205,
                epoch: 3,
            },
        }
    }

    #[test]
    fn keys_and_batch_ids_are_stable() {
        assert_eq!(stream_key(88, 5, 1205, 3), "s/88/5/1205/3");
        assert_eq!(queue_key(412, "01JC"), "q/412/01JC");
        assert_eq!(change_key("7", "0-11-90412", 2), "c/7/0-11-90412/2");
        let a = Batch::new(vec![rec("x"), rec("y")], 0);
        let b = Batch::new(vec![rec("x"), rec("y")], 3);
        assert_eq!(a.id, b.id);
        assert_eq!(a.id.len(), 32);
        let mut r = rec("x");
        r.idem_key = "other".into();
        assert_ne!(Batch::new(vec![r], 0).id, a.id);
    }

    #[test]
    fn json_forms() {
        let r = rec("k");
        let j = r.to_json();
        assert_eq!(j["value"]["a"], 1);
        assert_eq!(j["headers"]["h"], "v");
        assert!(j["headers"]["b"]["base64"].is_string());
        assert_eq!(j["idempotencyKey"], "s/88/5/1205/3");
        assert_eq!(bytes_json(Some(b"plain")), json!("plain"));
        let p = r.pos.encode();
        assert_eq!(Position::decode(&p), Some(r.pos.clone()));
        assert_eq!(r.pos.unit_seq(), Some((5, 1205)));
    }

    #[test]
    fn enums_parse() {
        for k in ConnKind::ALL {
            assert_eq!(ConnKind::parse(k.as_str()), Some(*k));
        }
        assert_eq!(TargetKind::parse("DATABASE"), Some(TargetKind::Database));
        assert!(TargetKind::Database.accepts(ConnKind::Postgresql));
        assert!(!TargetKind::Kafka.accepts(ConnKind::S3));
    }
}
