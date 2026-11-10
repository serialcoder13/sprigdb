//! Retryable-write records (chapter 30, "Retryable writes, exactly once").
//!
//! Each logical database that holds collections keeps one hidden table,
//! `__sprig_doc_sessions`, next to its collections, so a record commits
//! in the same MariaDB transaction as the writes it describes. One row per
//! logical session and user: the newest `txnNumber`, and the reply of each
//! statement of that `txnNumber` that took effect, keyed by its statement
//! id (its position in the command). A retried write reads the row
//! `FOR UPDATE`, answers the statements already done from it, and runs
//! only the others.
//!
//! The name starts `__sprig_doc_` (not chapter 30's
//! `__sprig_mongo_sessions`) because chapter 08's DDL lets only the
//! document engine's `__sprig_doc_` names through (`decisions/r5-interim.md`,
//! D12).

use std::collections::BTreeMap;

use bson::{Bson, Document};
use sprig_proto_mongo::errors::{MongoError, code};

use crate::sql::{bin_lit, ident};

/// The table, in each logical database.
pub const SESSIONS_TABLE: &str = "__sprig_doc_sessions";

/// `CREATE TABLE` of the records table.
#[must_use]
pub fn table_sql(db: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (
  lsid        VARBINARY(64) NOT NULL,
  uid         BINARY(32) NOT NULL,
  txn_number  BIGINT NOT NULL,
  results     LONGBLOB NOT NULL,
  txn_state   ENUM('retryable','committed') NOT NULL,
  updated_at  DATETIME(3) NOT NULL,
  PRIMARY KEY (lsid, uid),
  KEY k_updated (updated_at)
) ENGINE=InnoDB",
        ident(db),
        ident(SESSIONS_TABLE)
    )
}

/// The row of `(lsid, uid)`, locked for the transaction; with
/// `Record::aborted` and `Record::committed` it also settles a
/// transaction's outcome (chapter 30, "Sessions and transactions").
#[must_use]
pub fn select_sql(db: &str, lsid: &[u8], uid: &[u8; 32]) -> String {
    format!(
        "SELECT txn_number, HEX(results), txn_state FROM {}.{} WHERE lsid = {} AND uid = {} FOR UPDATE",
        ident(db),
        ident(SESSIONS_TABLE),
        bin_lit(lsid),
        bin_lit(uid)
    )
}

/// Writes the row: `txn_number` and the statements done so far.
#[must_use]
pub fn upsert_sql(
    db: &str,
    lsid: &[u8],
    uid: &[u8; 32],
    txn_number: i64,
    results: &[u8],
    committed: bool,
) -> String {
    let state = if committed { "committed" } else { "retryable" };
    format!(
        "INSERT INTO {}.{} (lsid, uid, txn_number, results, txn_state, updated_at) \
         VALUES ({}, {}, {txn_number}, {}, '{state}', NOW(3)) ON DUPLICATE KEY UPDATE \
         txn_number = VALUES(txn_number), results = VALUES(results), \
         txn_state = VALUES(txn_state), updated_at = VALUES(updated_at)",
        ident(db),
        ident(SESSIONS_TABLE),
        bin_lit(lsid),
        bin_lit(uid),
        bin_lit(results)
    )
}

/// Deletes up to `limit` rows older than `minutes`.
#[must_use]
pub fn purge_sql(db: &str, minutes: u64, limit: u64) -> String {
    format!(
        "DELETE FROM {}.{} WHERE updated_at < NOW(3) - INTERVAL {minutes} MINUTE LIMIT {limit}",
        ident(db),
        ident(SESSIONS_TABLE)
    )
}

/// One session row as read.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Record {
    /// The newest `txnNumber`.
    pub txn_number: i64,
    /// The replies of its statements that took effect, by statement id.
    pub done: StmtResults,
    /// It records a committed transaction rather than retryable writes.
    pub committed: bool,
    /// It fences an abandoned transaction of `txn_number` (written by the
    /// gateway that answered its `commitTransaction` or
    /// `abortTransaction` from a recovery token): that transaction can't
    /// commit any more.
    pub aborted: bool,
}

/// The `results` of a fence ([`Record::aborted`]).
const ABORTED: &str = "$aborted";

/// `results` for a fence.
#[must_use]
pub fn aborted_results() -> Vec<u8> {
    let mut d = Document::new();
    d.insert(ABORTED, true);
    bson::to_vec(&d).unwrap_or_default()
}

/// Statement replies by statement id.
pub type StmtResults = BTreeMap<i32, Document>;

fn bad_row(what: &str) -> MongoError {
    MongoError::new(
        code::INTERNAL_ERROR,
        format!("unreadable retryable-write record: {what}"),
    )
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

/// Encodes the statements done as BSON: `{"<stmtId>": reply, ...}`.
///
/// # Errors
/// 10334 past 16 MiB of statement replies.
pub fn encode(done: &StmtResults) -> Result<Vec<u8>, MongoError> {
    let mut d = Document::new();
    for (k, v) in done {
        d.insert(k.to_string(), Bson::Document(v.clone()));
    }
    bson::to_vec(&d).map_err(|e| MongoError::new(code::BSON_OBJECT_TOO_LARGE, e.to_string()))
}

/// Decodes a row of [`select_sql`].
///
/// # Errors
/// 1 for a row this module didn't write.
pub fn decode(row: &[Option<Vec<u8>>]) -> Result<Record, MongoError> {
    let txt = |i: usize| -> Result<String, MongoError> {
        row.get(i)
            .and_then(|c| c.as_ref())
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .ok_or_else(|| bad_row("missing column"))
    };
    let txn_number = txt(0)?.parse::<i64>().map_err(|_| bad_row("txn_number"))?;
    let bytes = hex_decode(&txt(1)?).ok_or_else(|| bad_row("results"))?;
    let d = if bytes.is_empty() {
        Document::new()
    } else {
        bson::from_slice::<Document>(&bytes).map_err(|_| bad_row("results"))?
    };
    let mut done = StmtResults::new();
    let aborted = d.contains_key(ABORTED);
    for (k, v) in d {
        if k.starts_with('$') {
            continue;
        }
        let id = k.parse::<i32>().map_err(|_| bad_row("statement id"))?;
        if let Bson::Document(r) = v {
            done.insert(id, r);
        }
    }
    Ok(Record {
        txn_number,
        done,
        committed: txt(2)? == "committed",
        aborted,
    })
}

#[cfg(test)]
mod tests {
    use bson::doc;

    use super::*;

    fn hex(b: &[u8]) -> Vec<u8> {
        b.iter()
            .flat_map(|x| {
                let h = b"0123456789ABCDEF";
                [h[usize::from(x >> 4)], h[usize::from(x & 15)]]
            })
            .collect()
    }

    #[test]
    fn round_trip() {
        let mut done = StmtResults::new();
        done.insert(0, doc! {"n": 1});
        done.insert(7, doc! {"n": 1, "m": 0, "u": bson::oid::ObjectId::new()});
        let b = encode(&done).unwrap();
        let row = vec![
            Some(b"42".to_vec()),
            Some(hex(&b)),
            Some(b"retryable".to_vec()),
        ];
        let r = decode(&row).unwrap();
        assert_eq!(r.txn_number, 42);
        assert_eq!(r.done, done);
        assert!(!r.committed);
        let empty = vec![
            Some(b"3".to_vec()),
            Some(hex(&encode(&StmtResults::new()).unwrap())),
            Some(b"committed".to_vec()),
        ];
        let r = decode(&empty).unwrap();
        assert!(r.done.is_empty() && r.committed && !r.aborted);
        let fence = vec![
            Some(b"4".to_vec()),
            Some(hex(&aborted_results())),
            Some(b"retryable".to_vec()),
        ];
        let r = decode(&fence).unwrap();
        assert!(r.aborted && r.done.is_empty() && !r.committed);
        assert!(decode(&[Some(b"x".to_vec()), None, None]).is_err());
    }

    #[test]
    fn statements() {
        let s = upsert_sql("shop", &[1, 2], &[0xAB; 32], 9, &[5], false);
        assert!(s.contains("`shop`.`__sprig_doc_sessions`"));
        assert!(s.contains("X'0102'") && s.contains("'retryable'"));
        assert!(select_sql("shop", &[1], &[0; 32]).ends_with("FOR UPDATE"));
        assert!(purge_sql("shop", 35, 1000).contains("INTERVAL 35 MINUTE LIMIT 1000"));
    }
}
