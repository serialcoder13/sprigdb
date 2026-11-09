//! Publishing from front ends other than SQL text (chapter 45, "Logic and
//! other protocols"): the interface `db.publish` (chapter 41), MongoDB's
//! `insertOne` and `insertMany` on a stream (chapter 30) and Kafka's
//! produce plug into.
//!
//! **As built:** only the statement every one of them sends is here
//! ([`outbox_insert`]); a [`Publisher`] runs it inside the caller's
//! transaction. No front end calls it yet: the logic runtime's engine tier
//! can publish with `INSERT INTO <stream>` through `Sprig.Db.Exec`, which
//! the gateway's SQL path already rewrites; the MongoDB front end (R5) and
//! compiled routines don't exist for streams yet.

use sprig_common::Result;
use sprig_common::names::quote;

use crate::ddl::hex_literal;
use crate::model::{StreamDef, StreamKind};

/// One record to publish.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Publish {
    /// The key.
    pub key: Option<Vec<u8>>,
    /// The value (`None`: a tombstone).
    pub value: Option<Vec<u8>>,
    /// Headers as a JSON object.
    pub headers: Option<String>,
    /// A partition the producer chose.
    pub partition: Option<u32>,
}

/// The outbox insert of `records` into stream `def` (in logical database
/// `db` when given), for a session's own transaction. `None` for a change
/// topic, which only SprigDB writes (39941).
#[must_use]
pub fn outbox_insert(def: &StreamDef, db: Option<&str>, records: &[Publish]) -> Option<String> {
    if def.kind == StreamKind::Changes || records.is_empty() {
        return None;
    }
    let table = match db {
        Some(d) => format!("{}.{}", quote(d), quote(&def.outbox_table())),
        None => quote(&def.outbox_table()),
    };
    let lit = |b: &Option<Vec<u8>>| b.as_deref().map_or_else(|| "NULL".to_owned(), hex_literal);
    let rows: Vec<String> = records
        .iter()
        .map(|r| {
            format!(
                "({}, {}, {}, {})",
                lit(&r.key),
                lit(&r.value),
                r.headers.as_deref().map_or_else(
                    || "NULL".to_owned(),
                    |h| format!("CONVERT({} USING utf8mb4)", hex_literal(h.as_bytes()))
                ),
                r.partition.map_or_else(
                    || "NULL".to_owned(),
                    |p| (p % def.partitions.max(1)).to_string()
                )
            )
        })
        .collect();
    Some(format!(
        "INSERT INTO {table} (k, v, headers, partition_no) VALUES {}",
        rows.join(", ")
    ))
}

/// Publishes inside the caller's transaction (the interface for
/// `db.publish`, MongoDB inserts on a stream, and Kafka produce).
#[async_trait::async_trait]
pub trait Publisher: Send {
    /// Publishes `records` to stream `name` of database `db`; they exist
    /// only if the caller's transaction commits.
    ///
    /// # Errors
    /// 39940 for an unknown stream, 39941 for a change topic, 39944 above
    /// `stream.max_message_bytes`, and the session's errors.
    async fn publish(&mut self, db: &str, name: &str, records: &[Publish]) -> Result<u64>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Cleanup, Envelope, StreamId, StreamState};
    use sprig_common::ids::{DbId, GroupId};

    #[test]
    fn statement() {
        let def = StreamDef {
            id: StreamId(41),
            db: DbId(1),
            name: "e".into(),
            kind: StreamKind::Events,
            source_object: None,
            source_name: None,
            partitions: 4,
            key_spec: Vec::new(),
            attached_group: None,
            log_group: GroupId(1),
            cleanup: Cleanup::Delete,
            envelope: Envelope::Raw,
            state: StreamState::Active,
            outbox: Vec::new(),
            log: Vec::new(),
            created_at_ms: 0,
        };
        let sql = outbox_insert(
            &def,
            Some("shop"),
            &[Publish {
                key: Some(b"k".to_vec()),
                value: None,
                headers: None,
                partition: Some(6),
            }],
        )
        .unwrap();
        assert_eq!(
            sql,
            "INSERT INTO `shop`.`__sprig_eo_41` (k, v, headers, partition_no) VALUES (X'6B', NULL, NULL, 2)"
        );
    }
}
