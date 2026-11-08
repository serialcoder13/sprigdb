//! Change events and feed items (chapter 38, "Interfaces").
//!
//! Events name what clients see: the tenant, the logical database and
//! table names and ids, the shard ids. They never carry physical schema
//! names: DDL text is rewritten to logical names, or left out when it
//! can't be.

use sprig_binlog::{Gtid, Value};
use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId, TenantId};

use crate::token::ResumeToken;

/// Idempotency key: unique and stable for one row change in one cluster's
/// history. Consumers dedupe by it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EventId {
    /// The cluster whose binlog holds the change.
    pub cluster: ClusterId,
    /// The transaction.
    pub gtid: Gtid,
    /// The row's index in the transaction (0 for a DDL event).
    pub row: u32,
}

/// What a DDL statement did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DdlKind {
    /// `CREATE`.
    Create,
    /// `ALTER`.
    Alter,
    /// `DROP`.
    Drop,
    /// `RENAME`.
    Rename,
    /// Anything else.
    Other,
}

/// The operation of an event (R1: tables only).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Op {
    /// A row was inserted.
    Insert,
    /// A row was updated.
    Update,
    /// A row was deleted.
    Delete,
    /// A DDL statement ran on the table's shard.
    Ddl(DdlKind),
    /// `TRUNCATE TABLE`: every row of the table in this shard is gone.
    Truncate,
}

/// A row image or key: (column name, value), in table order, without
/// internal columns.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Image(pub Vec<(String, Value)>);

impl Image {
    /// The value of a column.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.0.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    }
}

/// Where an event's row lives, by id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShardRef {
    /// The table group.
    pub group: GroupId,
    /// The shard (0 for a reference table).
    pub shard: ShardId,
}

/// One committed change.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangeEvent {
    /// The idempotency key.
    pub id: EventId,
    /// Resume after this event.
    pub token: ResumeToken,
    /// The tenant.
    pub tenant: TenantId,
    /// The logical database's id.
    pub db: DbId,
    /// The logical database's name.
    pub db_name: String,
    /// `objects.object_id` of the table (0 if the catalog has no row).
    pub object: u64,
    /// The table's logical name.
    pub table: String,
    /// The shard.
    pub shard: ShardRef,
    /// The primary key (empty for DDL and truncate).
    pub key: Image,
    /// What happened.
    pub op: Op,
    /// The full row before the change.
    pub before: Option<Image>,
    /// The full row after the change.
    pub after: Option<Image>,
    /// For updates: the columns whose value changed.
    pub delta: Option<Vec<String>>,
    /// The merge clamp of the transaction (also in the token).
    pub hlc: u64,
    /// The transaction.
    pub txn: Gtid,
    /// For DDL: the statement with logical names, when it could be
    /// rewritten.
    pub ddl: Option<String>,
}

/// One item of a feed.
#[derive(Debug, Clone, PartialEq)]
pub enum FeedItem {
    /// A change.
    Event(Box<ChangeEvent>),
    /// The end of a transaction whose events came just before.
    TxnEnd {
        /// Resume after the whole transaction.
        token: ResumeToken,
    },
    /// The feed advanced without events (idle, or filtered traffic).
    Progress {
        /// Resume here.
        token: ResumeToken,
    },
}

impl FeedItem {
    /// The item's resume token.
    #[must_use]
    pub fn token(&self) -> &ResumeToken {
        match self {
            FeedItem::Event(e) => &e.token,
            FeedItem::TxnEnd { token } | FeedItem::Progress { token } => token,
        }
    }
}
