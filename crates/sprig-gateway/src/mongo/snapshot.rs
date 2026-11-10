//! Snapshot reads outside transactions (chapter 30, "Read preference,
//! write concern, read concern"; R5 round 4).
//!
//! A read with `readConcern: {level: "snapshot"}` outside a transaction
//! (a driver's snapshot session) sees one point in time across commands.
//! SprigDB keeps that point as an open MariaDB read-only transaction
//! started `WITH CONSISTENT SNAPSHOT` on an executor session of its own:
//!
//! - the session's first snapshot read (no `atClusterTime`) opens one and
//!   names it by this gateway's cluster time, which the reply carries as
//!   `atClusterTime` (in the cursor for `find` and `aggregate`, at the top
//!   for `distinct`), as `mongos` does;
//! - later reads send that `atClusterTime` back and run on the same
//!   transaction, so writes committed since are invisible to them; another
//!   session given the same time (`snapshotTime`) shares it;
//! - a time this gateway doesn't hold (another gateway's, or one reaped)
//!   is 239 `SnapshotTooOld`, which drivers report as they would for a
//!   time past MongoDB's history window.
//!
//! A snapshot is reaped after [`IDLE`] without a read, and a tenant holds
//! at most [`PER_TENANT`] at once on a gateway (the oldest is closed).
//! Only `find`, `aggregate` (without `$out`/`$merge`), `distinct` and
//! `count` read at a snapshot, as in MongoDB 8.0; other commands keep
//! refusing it with 72. A tenant spread over several clusters gets one
//! point per cluster only (the executor session's own), not a time
//! across them (chapter 18's read time).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bson::{Bson, Document, Timestamp};
use sprig_proto_mongo::errors::MongoError;

use super::cursors::Owner;
use super::sql::SqlSession;

/// Idle time after which a snapshot closes (MongoDB keeps 5 minutes of
/// history by default, `minSnapshotHistoryWindowInSeconds`).
pub const IDLE: Duration = Duration::from_secs(300);

/// Open snapshots per tenant per gateway.
pub const PER_TENANT: usize = 64;

/// 239 `SnapshotTooOld`.
pub const SNAPSHOT_TOO_OLD: i32 = 239;

/// One open snapshot.
#[derive(Debug)]
pub struct Snapshot {
    pub(crate) sql: Option<SqlSession>,
    pub(crate) at: Timestamp,
    owner: Owner,
    used: Instant,
}

type Entry = Arc<tokio::sync::Mutex<Snapshot>>;

/// The gateway's open snapshots, by tenant and time.
#[derive(Debug, Default)]
pub struct Snapshots {
    map: tokio::sync::Mutex<HashMap<(u64, u64), Entry>>,
}

fn key(t: Timestamp) -> u64 {
    (u64::from(t.time) << 32) | u64::from(t.increment)
}

/// Whether `name` with `body` reads at a snapshot when asked to.
#[must_use]
pub fn reads(name: &str, body: &Document) -> bool {
    match name {
        "find" | "distinct" | "count" => true,
        "aggregate" => !body.get_array("pipeline").is_ok_and(|p| {
            p.iter().any(|s| {
                s.as_document()
                    .is_some_and(|d| d.contains_key("$out") || d.contains_key("$merge"))
            })
        }),
        _ => false,
    }
}

/// The `atClusterTime` a command asks for, if any.
#[must_use]
pub fn at_of(body: &Document) -> Option<Timestamp> {
    match body.get_document("readConcern").ok()?.get("atClusterTime")? {
        Bson::Timestamp(t) => Some(*t),
        _ => None,
    }
}

impl Snapshots {
    /// The snapshot at `at` for `owner`'s tenant (239 when this gateway
    /// doesn't hold it).
    ///
    /// # Errors
    /// 239; 13 when the snapshot belongs to another user.
    pub async fn get(&self, owner: &Owner, at: Timestamp) -> Result<Entry, MongoError> {
        let e = self
            .map
            .lock()
            .await
            .get(&(owner.tenant, key(at)))
            .cloned()
            .ok_or_else(|| {
                MongoError::new(
                    SNAPSHOT_TOO_OLD,
                    format!(
                        "Read timestamp {at} is older than the oldest available timestamp on this gateway (snapshots are kept {} s)",
                        IDLE.as_secs()
                    ),
                )
            })?;
        {
            let mut g = e.lock().await;
            if g.owner != *owner {
                return Err(MongoError::new(
                    13,
                    "this snapshot belongs to another user",
                ));
            }
            g.used = Instant::now();
        }
        Ok(e)
    }

    /// Keeps `sql` (in a consistent-snapshot transaction) as the snapshot
    /// at `at`; closes the tenant's oldest past [`PER_TENANT`].
    pub async fn put(&self, owner: Owner, at: Timestamp, sql: SqlSession) -> Entry {
        let tenant = owner.tenant;
        let e = Arc::new(tokio::sync::Mutex::new(Snapshot {
            sql: Some(sql),
            at,
            owner,
            used: Instant::now(),
        }));
        let evicted = {
            let mut m = self.map.lock().await;
            m.insert((tenant, key(at)), Arc::clone(&e));
            let mut mine: Vec<(u64, u64)> = m.keys().filter(|k| k.0 == tenant).copied().collect();
            let mut out = Vec::new();
            if mine.len() > PER_TENANT {
                mine.sort_unstable();
                for k in &mine[..mine.len() - PER_TENANT] {
                    if let Some(x) = m.remove(k) {
                        out.push(x);
                    }
                }
            }
            out
        };
        for x in evicted {
            close(&x).await;
        }
        e
    }

    /// Closes snapshots idle past [`IDLE`].
    pub async fn reap(&self) {
        let old: Vec<Entry> = {
            let mut m = self.map.lock().await;
            let mut out = Vec::new();
            let keys: Vec<(u64, u64)> = m.keys().copied().collect();
            for k in keys {
                let idle = m
                    .get(&k)
                    .and_then(|e| e.try_lock().ok().map(|g| g.used.elapsed() > IDLE))
                    .unwrap_or(false);
                if idle && let Some(e) = m.remove(&k) {
                    out.push(e);
                }
            }
            out
        };
        for e in old {
            close(&e).await;
        }
    }
}

async fn close(e: &Entry) {
    let mut g = e.lock().await;
    if let Some(mut s) = g.sql.take() {
        let _ = sprig_doc::engine::SqlRunner::exec(&mut s, "ROLLBACK").await;
        s.close().await;
    }
}

/// Puts `at` into a read's reply: in the cursor for `find` and
/// `aggregate`, at the top level otherwise.
pub fn stamp(reply: &mut Document, at: Timestamp) {
    if let Ok(c) = reply.get_document_mut("cursor") {
        c.insert("atClusterTime", at);
    } else {
        reply.insert("atClusterTime", at);
    }
}
