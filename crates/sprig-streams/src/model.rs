//! Streams as the catalog and the sequencer see them (chapter 45,
//! "Interfaces" and "Data structures and storage").
//!
//! **Physical names (as built).** A stream's tables are named by its id,
//! not its name: the outbox `__sprig_eo_<stream_id>`, the log
//! `__sprig_sl_<stream_id>`. Stream names follow Kafka's topic rules (up
//! to 249 characters, dots and dashes allowed), which MariaDB's 64-character
//! identifiers can't hold, and change topics are named `<object>.changes`.
//! Ids never change, so a rename later needs no table rename.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use sprig_catalog::cache::ExtState;
use sprig_common::ids::{DbId, GroupId, ObjectId, ShardId, TenantId};
use sprig_common::names::SchemaName;

/// `streams.stream_id`: an id from the tenant's object counter (chapter
/// 08), so it can become `objects.object_id` once streams are catalog
/// objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StreamId(pub u64);

/// A partition of a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionNo(pub u32);

/// An offset in a partition: the record's position, from 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Offset(pub u64);

/// The sequencer's leader epoch: raised by every new sequencer of a log
/// shard, and stored with each record it appends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct LeaderEpoch(pub u32);

/// `streams.kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamKind {
    /// Application events, published through the outbox.
    Events,
    /// A change topic of an object, from the change feed.
    Changes,
}

impl StreamKind {
    /// The catalog's text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            StreamKind::Events => "events",
            StreamKind::Changes => "changes",
        }
    }

    /// From the catalog's text.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "events" => Some(StreamKind::Events),
            "changes" => Some(StreamKind::Changes),
            _ => None,
        }
    }
}

/// `streams.cleanup`: Kafka's `cleanup.policy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Cleanup {
    /// Retention by time and size.
    Delete,
    /// Compaction by key (and retention).
    Compact,
}

impl Cleanup {
    /// The catalog's text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Cleanup::Delete => "delete",
            Cleanup::Compact => "compact",
        }
    }

    /// From the catalog's text.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "delete" => Some(Cleanup::Delete),
            "compact" => Some(Cleanup::Compact),
            _ => None,
        }
    }
}

/// `streams.envelope`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Envelope {
    /// Chapter 45's change envelope (change topics).
    ChangeJson,
    /// The value as published (event streams).
    Raw,
}

impl Envelope {
    /// The catalog's text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Envelope::ChangeJson => "change_json",
            Envelope::Raw => "raw",
        }
    }

    /// From the catalog's text.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "change_json" => Some(Envelope::ChangeJson),
            "raw" => Some(Envelope::Raw),
            _ => None,
        }
    }
}

/// `streams.state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamState {
    /// Tables being created.
    Creating,
    /// In use.
    Active,
    /// Gone from clients; tables and offsets being removed.
    Dropping,
}

impl StreamState {
    /// The catalog's text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            StreamState::Creating => "creating",
            StreamState::Active => "active",
            StreamState::Dropping => "dropping",
        }
    }

    /// From the catalog's text.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "creating" => Some(StreamState::Creating),
            "active" => Some(StreamState::Active),
            "dropping" => Some(StreamState::Dropping),
            _ => None,
        }
    }
}

/// One shard schema of a group, by id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardSlot {
    /// The table group.
    pub group: GroupId,
    /// The shard.
    pub shard: ShardId,
}

impl ShardSlot {
    /// The physical schema of this slot in `db` of `tenant`.
    #[must_use]
    pub fn schema(&self, tenant: TenantId, db: DbId) -> SchemaName {
        SchemaName::shard(tenant, db, self.group, self.shard)
    }
}

/// A log shard: a shard schema of the stream's log group and the
/// partitions it holds (chapter 45: the log group is sharded by partition,
/// so each partition lives whole on one shard).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogShard {
    /// Where.
    pub slot: ShardSlot,
    /// The partitions it holds; empty means every partition.
    pub partitions: Vec<PartitionNo>,
}

impl LogShard {
    /// Whether partition `p` lives here.
    #[must_use]
    pub fn holds(&self, p: PartitionNo) -> bool {
        self.partitions.is_empty() || self.partitions.contains(&p)
    }
}

/// One row of `streams`, with what the gateway and the sequencer need.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamDef {
    /// The id.
    pub id: StreamId,
    /// The logical database.
    pub db: DbId,
    /// The name (a Kafka topic name).
    pub name: String,
    /// Events or changes.
    pub kind: StreamKind,
    /// For a change topic: the watched object.
    pub source_object: Option<ObjectId>,
    /// For a change topic: the watched table's name (logical).
    pub source_name: Option<String>,
    /// Partitions; they only grow.
    pub partitions: u32,
    /// The event key's columns (documentation, and the attached group's
    /// key check).
    pub key_spec: Vec<String>,
    /// The outbox's table group, when `ATTACHED TO GROUP OF` was given.
    pub attached_group: Option<GroupId>,
    /// The stream log's table group.
    pub log_group: GroupId,
    /// Kafka's `cleanup.policy`.
    pub cleanup: Cleanup,
    /// The record envelope.
    pub envelope: Envelope,
    /// The life cycle.
    pub state: StreamState,
    /// Where the outbox lives (event streams; empty for change topics).
    pub outbox: Vec<ShardSlot>,
    /// Where the log lives.
    pub log: Vec<LogShard>,
    /// When created (ms since the Unix epoch).
    pub created_at_ms: u64,
}

impl StreamDef {
    /// The outbox table's name (chapter 45: `__sprig_eo_<stream>`; by id,
    /// see the module docs).
    #[must_use]
    pub fn outbox_table(&self) -> String {
        outbox_table(self.id)
    }

    /// The log table's name.
    #[must_use]
    pub fn log_table(&self) -> String {
        log_table(self.id)
    }

    /// The log shard holding partition `p`.
    #[must_use]
    pub fn log_shard_of(&self, p: PartitionNo) -> Option<&LogShard> {
        self.log.iter().find(|l| l.holds(p))
    }

    /// The partitions of log shard `slot`.
    #[must_use]
    pub fn partitions_of(&self, slot: &ShardSlot) -> Vec<PartitionNo> {
        let Some(l) = self.log.iter().find(|l| &l.slot == slot) else {
            return Vec::new();
        };
        (0..self.partitions)
            .map(PartitionNo)
            .filter(|p| l.holds(*p))
            .collect()
    }
}

/// `__sprig_eo_<id>`.
#[must_use]
pub fn outbox_table(id: StreamId) -> String {
    format!("__sprig_eo_{}", id.0)
}

/// `__sprig_sl_<id>`.
#[must_use]
pub fn log_table(id: StreamId) -> String {
    format!("__sprig_sl_{}", id.0)
}

/// The checkpoint table, one per log shard schema.
pub const CKPT_TABLE: &str = "__sprig_sl_ckpt";

/// The checkpoint row holding a log shard's sequencer state (leader epoch
/// and feed position) rather than one partition's offsets.
pub const SHARD_ROW: u32 = u32::MAX;

/// The change topic of object `object`: `<object>.changes`.
#[must_use]
pub fn change_topic_name(object: &str) -> String {
    format!("{object}.changes")
}

/// Whether `name` is a valid stream name: Kafka's topic rules (1 to 249
/// characters of `[a-zA-Z0-9._-]`, not `.` or `..`).
#[must_use]
pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 249
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// A record as read from the log (chapter 45's `Record`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Its partition.
    pub partition: PartitionNo,
    /// Its offset.
    pub offset: Offset,
    /// Its timestamp (ms since the Unix epoch).
    pub ts_ms: i64,
    /// The key.
    pub key: Option<Vec<u8>>,
    /// The value; `None` is a tombstone.
    pub value: Option<Vec<u8>>,
    /// Headers.
    pub headers: Vec<(String, Vec<u8>)>,
    /// The leader epoch that appended it.
    pub leader_epoch: LeaderEpoch,
}

/// Where a record came from: its idempotency key in the log
/// (`source_event`, `uk_source`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Source {
    /// An outbox row: its `event_id` (UUIDv7).
    Outbox([u8; 16]),
    /// A change event: its feed `EventId`, encoded.
    Change(Vec<u8>),
}

impl Source {
    /// The bytes stored in `source_event`: `e` + the event id, or `c` +
    /// the encoded feed event id.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Source::Outbox(id) => {
                let mut v = Vec::with_capacity(17);
                v.push(b'e');
                v.extend_from_slice(id);
                v
            }
            Source::Change(b) => {
                let mut v = Vec::with_capacity(b.len() + 1);
                v.push(b'c');
                v.extend_from_slice(b);
                v
            }
        }
    }

    /// The feed's event id as `source_event` bytes (at most 64).
    #[must_use]
    pub fn of_event(id: &sprig_feed::EventId) -> Self {
        let c = id.cluster.to_string();
        let mut v = Vec::with_capacity(c.len() + 21);
        v.push(u8::try_from(c.len().min(40)).unwrap_or(40));
        v.extend_from_slice(&c.as_bytes()[..c.len().min(40)]);
        v.extend_from_slice(&id.gtid.domain.to_be_bytes());
        v.extend_from_slice(&id.gtid.server.to_be_bytes());
        v.extend_from_slice(&id.gtid.seq.to_be_bytes());
        v.extend_from_slice(&id.row.to_be_bytes());
        Source::Change(v)
    }
}

/// A record before sequencing: what the sequencer appends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewRecord {
    /// The partition (the outbox's, a producer's explicit one, or the
    /// partitioner's).
    pub partition: PartitionNo,
    /// Timestamp (ms since the Unix epoch).
    pub ts_ms: i64,
    /// The key.
    pub key: Option<Vec<u8>>,
    /// The value; `None` is a tombstone.
    pub value: Option<Vec<u8>>,
    /// Headers as stored (a JSON object), if any.
    pub headers: Option<String>,
    /// The idempotency key.
    pub source: Source,
}

/// The `streams` module in a catalog snapshot: the tenant's live streams,
/// by database and name.
#[derive(Debug, Clone, Default)]
pub struct StreamsState {
    /// The module's tables exist.
    pub migrated: bool,
    by_db: HashMap<DbId, BTreeMap<String, Arc<StreamDef>>>,
}

impl StreamsState {
    /// A state with `streams` (the module migrated).
    #[must_use]
    pub fn new(streams: Vec<StreamDef>) -> Self {
        let mut by_db: HashMap<DbId, BTreeMap<String, Arc<StreamDef>>> = HashMap::new();
        for s in streams {
            by_db
                .entry(s.db)
                .or_default()
                .insert(s.name.clone(), Arc::new(s));
        }
        Self {
            migrated: true,
            by_db,
        }
    }

    /// Whether there is no stream at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_db.values().all(BTreeMap::is_empty)
    }

    /// Stream `name` of `db` (exact name; with `lctn`, case-insensitive),
    /// in any state.
    #[must_use]
    pub fn get(&self, db: DbId, name: &str, lctn: bool) -> Option<&Arc<StreamDef>> {
        let m = self.by_db.get(&db)?;
        if let Some(s) = m.get(name) {
            return Some(s);
        }
        if lctn {
            return m.values().find(|s| s.name.eq_ignore_ascii_case(name));
        }
        None
    }

    /// The streams of `db`, by name.
    pub fn of_db(&self, db: DbId) -> impl Iterator<Item = &Arc<StreamDef>> {
        self.by_db.get(&db).into_iter().flat_map(BTreeMap::values)
    }

    /// Every stream.
    pub fn all(&self) -> impl Iterator<Item = &Arc<StreamDef>> {
        self.by_db.values().flat_map(BTreeMap::values)
    }

    /// The change topic of object `object`, if any.
    #[must_use]
    pub fn change_topic_of(&self, db: DbId, object: ObjectId) -> Option<&Arc<StreamDef>> {
        self.of_db(db)
            .find(|s| s.kind == StreamKind::Changes && s.source_object == Some(object))
    }

    /// Whether `word` (an identifier as written) names a stream of any
    /// database: the gateway's quick test before it looks closer.
    #[must_use]
    pub fn mentions(&self, word: &str, lctn: bool) -> bool {
        self.all().any(|s| {
            if lctn {
                s.name.eq_ignore_ascii_case(word)
            } else {
                s.name == word
            }
        })
    }
}

impl ExtState for StreamsState {
    fn bytes(&self) -> usize {
        self.all().map(|s| 256 + s.name.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert!(valid_name("order_events"));
        assert!(valid_name("orders.changes"));
        assert!(valid_name("a-b.c_9"));
        assert!(!valid_name(".."));
        assert!(!valid_name("a b"));
        assert!(!valid_name(&"x".repeat(250)));
        assert_eq!(outbox_table(StreamId(41)), "__sprig_eo_41");
        assert_eq!(change_topic_name("orders"), "orders.changes");
        let e = Source::Outbox([7; 16]).to_bytes();
        assert_eq!(e.len(), 17);
        assert_eq!(e[0], b'e');
    }
}
