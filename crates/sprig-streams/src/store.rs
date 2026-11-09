//! The `streams` catalog module (chapter 45, "Catalog tables"; build step
//! 2): `streams`, `stream_groups` and `stream_offsets` in `t<tenant>_sprig`,
//! their migration, the snapshot loader ([`StreamsExtension`]) and the
//! writes stream DDL makes.
//!
//! Every stream DDL commits its row in one versioned catalog change, and
//! every gateway's snapshot reloads `streams` whole (it is small). A
//! catalog bootstrapped before the module has no tables; the first stream
//! DDL migrates it ([`migrate`]).
//!
//! **As built, against the book:** `streams` has three more columns:
//! `source_name` (a change topic's table, for its envelope),
//! `outbox_shards` and `log_shards` (the shard schemas of the outbox and the
//! log, as `[{"group":g,"shard":s,"partitions":[...]}]`), and
//! `updated_at`. Streams have no `objects` row yet (the `objects.kind`
//! enum has no `stream`); their ids come from the object counter.

use std::sync::Arc;

use mysql_async::Row;
use mysql_async::prelude::Queryable;
use serde_json::{Value, json};
use sprig_catalog::cache::{ExtState, SnapshotTx};
use sprig_catalog::migrate::{MigrateOpts, Migration, Module, Phase};
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogExtension, Migrator};
use sprig_common::ids::{DbId, GroupId, ObjectId, ShardId};
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};

use crate::model::{
    Cleanup, Envelope, LogShard, PartitionNo, ShardSlot, StreamDef, StreamId, StreamKind,
    StreamState, StreamsState,
};

/// The module name.
pub const MODULE: &str = "streams";

/// `streams` (chapter 45, with the columns of the module docs).
pub const DDL_STREAMS: &str = "CREATE TABLE IF NOT EXISTS streams (
  stream_id         BIGINT UNSIGNED NOT NULL,
  db_id             INT UNSIGNED NOT NULL,
  name              VARCHAR(249) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  kind              ENUM('events','changes') NOT NULL,
  source_object_id  BIGINT UNSIGNED NULL,
  source_name       VARCHAR(64) NULL,
  partitions        INT UNSIGNED NOT NULL,
  key_spec          JSON NULL,
  attached_group_id INT UNSIGNED NULL,
  log_group_id      INT UNSIGNED NOT NULL,
  cleanup           ENUM('delete','compact') NOT NULL DEFAULT 'delete',
  envelope          ENUM('change_json','raw') NOT NULL,
  state             ENUM('creating','active','dropping') NOT NULL,
  outbox_shards     JSON NOT NULL,
  log_shards        JSON NOT NULL,
  created_at        DATETIME(3) NOT NULL,
  updated_at        DATETIME(3) NOT NULL,
  PRIMARY KEY (stream_id),
  UNIQUE KEY uk_stream_name (db_id, name)
) ENGINE=InnoDB";

/// `stream_groups` (chapter 45): the durable part of consumer groups.
pub const DDL_STREAM_GROUPS: &str = "CREATE TABLE IF NOT EXISTS stream_groups (
  group_name        VARCHAR(255) NOT NULL,
  protocol_type     VARCHAR(64) NOT NULL,
  generation        INT UNSIGNED NOT NULL DEFAULT 0,
  state             ENUM('empty','stable','dead') NOT NULL DEFAULT 'empty',
  updated_at        DATETIME(3) NOT NULL,
  PRIMARY KEY (group_name)
) ENGINE=InnoDB";

/// `stream_offsets` (chapter 45): committed offsets.
pub const DDL_STREAM_OFFSETS: &str = "CREATE TABLE IF NOT EXISTS stream_offsets (
  group_name        VARCHAR(255) NOT NULL,
  stream_id         BIGINT UNSIGNED NOT NULL,
  partition_no      INT UNSIGNED NOT NULL,
  offset_no         BIGINT UNSIGNED NOT NULL,
  leader_epoch      INT UNSIGNED NULL,
  metadata          VARCHAR(1024) NULL,
  committed_at      DATETIME(3) NOT NULL,
  PRIMARY KEY (group_name, stream_id, partition_no)
) ENGINE=InnoDB";

/// The module's migrations.
pub static STREAMS_MIGRATIONS: &[Migration] = &[Migration {
    format: 1,
    phase: Phase::Expand,
    name: "streams, stream_groups and stream_offsets",
    statements: &[DDL_STREAMS, DDL_STREAM_GROUPS, DDL_STREAM_OFFSETS],
}];

/// The `streams` module, for [`Migrator::new`].
pub const STREAMS_MODULE: Module = Module {
    name: MODULE,
    migrations: STREAMS_MIGRATIONS,
};

/// Applies the module's migrations to catalog `schema` (idempotent, under
/// the migrator's lock).
///
/// # Errors
/// The migrator's.
pub async fn migrate(c: &mut mysql_async::Conn, schema: &str) -> Result<()> {
    Migrator::new(&[STREAMS_MODULE])?
        .run(c, schema, &MigrateOpts::default())
        .await?;
    Ok(())
}

pub(crate) fn map_err(e: mysql_async::Error) -> SprigError {
    match &e {
        mysql_async::Error::Server(s) => match s.code {
            1205 | 1213 => SprigError::new(SprigCode::CatalogConflict, "catalog row busy; retry"),
            1062 => SprigError::new(SprigCode::StreamExists, "a stream with this name exists"),
            code => {
                let mut state = *b"HY000";
                if s.state.len() == 5 {
                    state.copy_from_slice(s.state.as_bytes());
                }
                SprigError::backend(code, state, format!("catalog statement failed ({code})"))
            }
        }
        .with_source(e),
        _ => SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e),
    }
}

fn bad(what: &str) -> SprigError {
    SprigError::internal(format!("streams catalog: {what}"))
}

fn col<T: mysql_async::prelude::FromValue>(row: &Row, i: usize) -> Result<T> {
    row.get_opt::<T, usize>(i)
        .and_then(std::result::Result::ok)
        .ok_or_else(|| bad(&format!("bad column {i}")))
}

/// `[{"group":g,"shard":s}]`.
#[must_use]
pub fn slots_json(slots: &[ShardSlot]) -> Value {
    Value::Array(
        slots
            .iter()
            .map(|s| json!({"group": s.group.0, "shard": s.shard.0}))
            .collect(),
    )
}

/// `[{"group":g,"shard":s,"partitions":[...]}]` (empty: all).
#[must_use]
pub fn log_json(log: &[LogShard]) -> Value {
    Value::Array(
        log.iter()
            .map(|l| {
                json!({
                    "group": l.slot.group.0,
                    "shard": l.slot.shard.0,
                    "partitions": l.partitions.iter().map(|p| p.0).collect::<Vec<_>>(),
                })
            })
            .collect(),
    )
}

fn slot_of(v: &Value) -> Option<ShardSlot> {
    Some(ShardSlot {
        group: GroupId(u32::try_from(v["group"].as_u64()?).ok()?),
        shard: ShardId(u32::try_from(v["shard"].as_u64()?).ok()?),
    })
}

fn parse_slots(text: &str) -> Result<Vec<ShardSlot>> {
    let v: Value = serde_json::from_str(text).map_err(|_| bad("outbox_shards"))?;
    v.as_array()
        .ok_or_else(|| bad("outbox_shards"))?
        .iter()
        .map(|s| slot_of(s).ok_or_else(|| bad("outbox_shards")))
        .collect()
}

fn parse_log(text: &str) -> Result<Vec<LogShard>> {
    let v: Value = serde_json::from_str(text).map_err(|_| bad("log_shards"))?;
    v.as_array()
        .ok_or_else(|| bad("log_shards"))?
        .iter()
        .map(|s| {
            let slot = slot_of(s).ok_or_else(|| bad("log_shards"))?;
            let partitions = s["partitions"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_u64)
                        .filter_map(|p| u32::try_from(p).ok())
                        .map(PartitionNo)
                        .collect()
                })
                .unwrap_or_default();
            Ok(LogShard { slot, partitions })
        })
        .collect()
}

const COLUMNS: &str = "stream_id, db_id, name, kind, source_object_id, source_name, partitions, \
     CAST(key_spec AS CHAR), attached_group_id, log_group_id, cleanup, envelope, state, \
     CAST(outbox_shards AS CHAR), CAST(log_shards AS CHAR), \
     CAST(UNIX_TIMESTAMP(created_at) * 1000 AS UNSIGNED)";

fn def_of(r: &Row) -> Result<StreamDef> {
    let kind: String = col(r, 3)?;
    let key_spec: Option<String> = col(r, 7)?;
    let cleanup: String = col(r, 10)?;
    let envelope: String = col(r, 11)?;
    let state: String = col(r, 12)?;
    let outbox: String = col(r, 13)?;
    let log: String = col(r, 14)?;
    Ok(StreamDef {
        id: StreamId(col(r, 0)?),
        db: DbId(col(r, 1)?),
        name: col(r, 2)?,
        kind: StreamKind::parse(&kind).ok_or_else(|| bad("kind"))?,
        source_object: col::<Option<u64>>(r, 4)?.map(ObjectId),
        source_name: col(r, 5)?,
        partitions: col(r, 6)?,
        key_spec: match key_spec {
            Some(j) => serde_json::from_str(&j).map_err(|_| bad("key_spec"))?,
            None => Vec::new(),
        },
        attached_group: col::<Option<u32>>(r, 8)?.map(GroupId),
        log_group: GroupId(col(r, 9)?),
        cleanup: Cleanup::parse(&cleanup).ok_or_else(|| bad("cleanup"))?,
        envelope: Envelope::parse(&envelope).ok_or_else(|| bad("envelope"))?,
        state: StreamState::parse(&state).ok_or_else(|| bad("state"))?,
        outbox: parse_slots(&outbox)?,
        log: parse_log(&log)?,
        created_at_ms: col::<Option<u64>>(r, 15)?.unwrap_or(0),
    })
}

/// A committed offset (`stream_offsets`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedOffset {
    /// The consumer group.
    pub group: String,
    /// The stream.
    pub stream: StreamId,
    /// The partition.
    pub partition: PartitionNo,
    /// The next record to read.
    pub offset: u64,
    /// The leader epoch the consumer saw.
    pub leader_epoch: Option<u32>,
    /// Client metadata.
    pub metadata: Option<String>,
}

/// Reads and writes the module's tables in one catalog schema.
#[derive(Debug, Clone)]
pub struct StreamsStore {
    schema: String,
}

impl StreamsStore {
    /// The tables of catalog schema `schema`.
    #[must_use]
    pub fn new(schema: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
        }
    }

    fn t(&self, table: &str) -> String {
        format!("{}.{table}", quote(&self.schema))
    }

    /// Whether the module's tables exist.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn migrated<Q: Queryable + Send>(&self, q: &mut Q) -> Result<bool> {
        let n: Option<u64> = q
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'stream_offsets'",
                (self.schema.as_str(),),
            )
            .await
            .map_err(map_err)?;
        Ok(n.unwrap_or(0) > 0)
    }

    /// Every stream not being dropped.
    ///
    /// # Errors
    /// 39070, a backend error, or `Internal` for a malformed row.
    pub async fn load<Q: Queryable + Send>(&self, q: &mut Q) -> Result<StreamsState> {
        let rows: Vec<Row> = q
            .query(format!(
                "SELECT {COLUMNS} FROM {} WHERE state <> 'dropping' ORDER BY stream_id",
                self.t("streams")
            ))
            .await
            .map_err(map_err)?;
        let defs = rows.iter().map(def_of).collect::<Result<Vec<_>>>()?;
        Ok(StreamsState::new(defs))
    }

    /// One stream by id, in any state.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn get<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        id: StreamId,
    ) -> Result<Option<StreamDef>> {
        let row: Option<Row> = q
            .exec_first(
                format!(
                    "SELECT {COLUMNS} FROM {} WHERE stream_id = ?",
                    self.t("streams")
                ),
                (id.0,),
            )
            .await
            .map_err(map_err)?;
        row.as_ref().map(def_of).transpose()
    }

    /// Inserts a stream (its name must be free: 39942 otherwise).
    ///
    /// # Errors
    /// 39942, 39070 or a backend error.
    pub async fn insert<Q: Queryable + Send>(&self, q: &mut Q, s: &StreamDef) -> Result<()> {
        q.exec_drop(
            format!(
                "INSERT INTO {} (stream_id, db_id, name, kind, source_object_id, source_name, \
                 partitions, key_spec, attached_group_id, log_group_id, cleanup, envelope, state, \
                 outbox_shards, log_shards, created_at, updated_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))",
                self.t("streams")
            ),
            mysql_async::Params::Positional(vec![
                s.id.0.into(),
                s.db.0.into(),
                s.name.as_str().into(),
                s.kind.as_str().into(),
                s.source_object.map(|o| o.0).into(),
                s.source_name.as_deref().into(),
                s.partitions.into(),
                serde_json::to_string(&s.key_spec).unwrap_or_default().into(),
                s.attached_group.map(|g| g.0).into(),
                s.log_group.0.into(),
                s.cleanup.as_str().into(),
                s.envelope.as_str().into(),
                s.state.as_str().into(),
                slots_json(&s.outbox).to_string().into(),
                log_json(&s.log).to_string().into(),
            ]),
        )
        .await
        .map_err(map_err)
    }

    /// Changes a stream's partitions, cleanup policy and state.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn update<Q: Queryable + Send>(&self, q: &mut Q, s: &StreamDef) -> Result<()> {
        q.exec_drop(
            format!(
                "UPDATE {} SET partitions = ?, cleanup = ?, state = ?, log_shards = ?, \
                 updated_at = UTC_TIMESTAMP(3) WHERE stream_id = ?",
                self.t("streams")
            ),
            (
                s.partitions,
                s.cleanup.as_str(),
                s.state.as_str(),
                log_json(&s.log).to_string(),
                s.id.0,
            ),
        )
        .await
        .map_err(map_err)
    }

    /// Removes a stream's row and its committed offsets.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn delete<Q: Queryable + Send>(&self, q: &mut Q, id: StreamId) -> Result<()> {
        q.exec_drop(
            format!(
                "DELETE FROM {} WHERE stream_id = ?",
                self.t("stream_offsets")
            ),
            (id.0,),
        )
        .await
        .map_err(map_err)?;
        q.exec_drop(
            format!("DELETE FROM {} WHERE stream_id = ?", self.t("streams")),
            (id.0,),
        )
        .await
        .map_err(map_err)
    }

    /// Commits a consumer group's offset (`sprig.commit_offset` and Kafka's
    /// `OffsetCommit` will call this, inside the caller's transaction when
    /// there is one).
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn commit_offset<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        o: &CommittedOffset,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "INSERT INTO {} (group_name, stream_id, partition_no, offset_no, leader_epoch, \
                 metadata, committed_at) VALUES (?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3)) \
                 ON DUPLICATE KEY UPDATE offset_no = VALUES(offset_no), \
                 leader_epoch = VALUES(leader_epoch), metadata = VALUES(metadata), \
                 committed_at = VALUES(committed_at)",
                self.t("stream_offsets")
            ),
            (
                o.group.as_str(),
                o.stream.0,
                o.partition.0,
                o.offset,
                o.leader_epoch,
                o.metadata.as_deref(),
            ),
        )
        .await
        .map_err(map_err)
    }

    /// A group's committed offsets on a stream.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn offsets<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        group: &str,
        stream: StreamId,
    ) -> Result<Vec<CommittedOffset>> {
        let rows: Vec<(u32, u64, Option<u32>, Option<String>)> = q
            .exec(
                format!(
                    "SELECT partition_no, offset_no, leader_epoch, metadata FROM {} \
                     WHERE group_name = ? AND stream_id = ? ORDER BY partition_no",
                    self.t("stream_offsets")
                ),
                (group, stream.0),
            )
            .await
            .map_err(map_err)?;
        Ok(rows
            .into_iter()
            .map(|(p, o, e, m)| CommittedOffset {
                group: group.to_owned(),
                stream,
                partition: PartitionNo(p),
                offset: o,
                leader_epoch: e,
                metadata: m,
            })
            .collect())
    }
}

/// The module in the catalog cache: [`StreamsState`] in every snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub struct StreamsExtension;

#[async_trait::async_trait]
impl CatalogExtension for StreamsExtension {
    fn module(&self) -> &'static str {
        MODULE
    }

    fn migrations(&self) -> &'static [Migration] {
        STREAMS_MIGRATIONS
    }

    fn versioned(&self) -> Option<VersionKind> {
        Some(VersionKind::Catalog)
    }

    async fn load(
        &self,
        tx: &mut SnapshotTx<'_>,
        _since: Option<u64>,
        _prev: Option<&Arc<dyn ExtState>>,
    ) -> Result<Arc<dyn ExtState>> {
        let store = StreamsStore::new(tx.schema());
        let conn = tx.conn();
        if !store.migrated(conn).await? {
            return Ok(Arc::new(StreamsState::default()));
        }
        Ok(Arc::new(store.load(conn).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn module_is_well_formed() {
        Migrator::new(&[STREAMS_MODULE]).unwrap();
        assert_eq!(STREAMS_MODULE.newest(), 1);
        for ddl in STREAMS_MIGRATIONS[0].statements {
            assert!(ddl.starts_with("CREATE TABLE IF NOT EXISTS "));
        }
    }

    #[test]
    fn shard_json_round_trips() {
        let log = vec![LogShard {
            slot: ShardSlot {
                group: GroupId(1),
                shard: ShardId(2),
            },
            partitions: vec![PartitionNo(0), PartitionNo(3)],
        }];
        assert_eq!(parse_log(&log_json(&log).to_string()).unwrap(), log);
        let slots = vec![log[0].slot];
        assert_eq!(parse_slots(&slots_json(&slots).to_string()).unwrap(), slots);
    }
}
