//! Redis keyspace notifications from the change feed (chapter 40, "Pub/sub
//! and keyspace notifications"; RLS round 5, decision RL51).
//!
//! ```text
//! a connection subscribes to a pattern or a __key... channel (Session):
//!   watch(tenant): one task per tenant on this gateway, unless running
//! the task, while Hub::keyspace_interest(tenant) holds (checked every 5 s
//! and at each transaction's end):
//!   tail the tenant's feed from now (chapter 38's TenantTail)
//!   keep the row changes of the Redis databases' tables
//!   (<redis.logical_database>.db<n>, __sprig_ke_db<n>, __sprig_kz_db<n>)
//!   whose statement isn't marked o=k (a Redis command this gateway family
//!   knows sent those itself)
//!   at each transaction's end, per key in first-change order: its events
//!   (below), published under this gateway's flags for the tenant to this
//!   gateway's own subscribers (each gateway tails for its own), each
//!   checked per subscriber when delivered (RL46: SELECT on the key table
//!   and its key policies)
//! events of one key's changes in one transaction:
//!   key row deleted: expired (x) when its expiry had passed at the commit,
//!                    else del (g)
//!   key row inserted: new (n), then its type's write event
//!   key row updated, only the expiry changed: expire, or persist when it
//!                    was cleared (g)
//!   else by the key's type: string set ($); hash hset or, only element
//!   deletes, hdel (h); list lpush or rpush by which end grew, lpop or rpop
//!   by which shrank, lset (l); set sadd or srem (s); zset zadd or zrem (z)
//! ```
//!
//! Names are read from rows, not commands: `INCRBY` through SQL is `set`,
//! a list's `LREM` through a script `lpop` or `rpop`. Feed lag applies.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use sprig_binlog::{OpHint, Value as BValue};
use sprig_common::ids::TenantId;
use sprig_feed::{ChangeEvent, FeedItem, Op, StartAt, TailOptions, TenantTail};

/// How often a task with no events checks it still has subscribers.
const CHECK: Duration = Duration::from_secs(5);

/// The gateway's [`sprig_redis::keyspace::KeyspaceFeed`].
pub(crate) struct FeedNotifier {
    env: std::sync::Weak<sprig_redis::Env>,
    svc: Arc<crate::tenant::TenantServices>,
    running: Arc<Mutex<HashSet<String>>>,
}

impl std::fmt::Debug for FeedNotifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FeedNotifier").finish_non_exhaustive()
    }
}

impl FeedNotifier {
    /// Installs the notifier on `env`.
    pub(crate) fn install(env: &Arc<sprig_redis::Env>, svc: Arc<crate::tenant::TenantServices>) {
        env.set_keyspace_feed(Arc::new(Self {
            env: Arc::downgrade(env),
            svc,
            running: Arc::default(),
        }));
    }
}

impl sprig_redis::keyspace::KeyspaceFeed for FeedNotifier {
    fn watch(&self, scope: &str) {
        let Ok(id) = scope.parse::<u64>() else {
            return;
        };
        if !self
            .running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(scope.to_owned())
        {
            return;
        }
        let (env, svc, running, scope) = (
            self.env.clone(),
            Arc::clone(&self.svc),
            Arc::clone(&self.running),
            scope.to_owned(),
        );
        tokio::spawn(async move {
            let r = run(&env, &svc, TenantId(id), &scope).await;
            if let Err(e) = r {
                tracing::warn!(tenant = id, error = %e, "keyspace notifications from the feed stopped");
            }
            running
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&scope);
            // A subscriber that arrived while it stopped starts it again.
            if let Some(env) = env.upgrade()
                && env.hub.keyspace_interest(&scope)
                && let Some(f) = env.keyspace_feed()
            {
                tokio::time::sleep(Duration::from_secs(1)).await;
                f.watch(&scope);
            }
        });
    }
}

/// One key's changes in a transaction.
#[derive(Debug, Default)]
struct KeyChanges {
    db: u32,
    key: Vec<u8>,
    /// The key row's change.
    row: Option<(Op, Option<sprig_feed::Image>, Option<sprig_feed::Image>)>,
    /// Element rows inserted, updated, deleted.
    added: usize,
    changed: usize,
    removed: usize,
    /// The commit time, seconds.
    secs: u64,
}

/// Which Redis table `table` of the logical database is, and its index:
/// (index, element table).
fn redis_table(table: &str) -> Option<(u32, bool)> {
    let (rest, elem) = if let Some(r) = table
        .strip_prefix("__sprig_ke_")
        .or_else(|| table.strip_prefix("__sprig_kz_"))
    {
        (r, true)
    } else {
        (table, false)
    };
    let n = rest.strip_prefix("db")?.parse::<u32>().ok()?;
    Some((n, elem))
}

fn col<'a>(img: Option<&'a sprig_feed::Image>, name: &str) -> Option<&'a BValue> {
    img?.0
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(name))
        .map(|(_, v)| v)
}

fn bytes(v: Option<&BValue>) -> Option<Vec<u8>> {
    match v? {
        BValue::String(b) | BValue::Bytes(b) => Some(b.clone()),
        _ => None,
    }
}

fn int(v: Option<&BValue>) -> i64 {
    match v {
        Some(BValue::Int(i)) => *i,
        Some(BValue::UInt(u)) => i64::try_from(*u).unwrap_or(i64::MAX),
        _ => 0,
    }
}

/// Seconds since the epoch of a UTC `DATETIME`.
fn epoch_secs(t: &sprig_binlog::DateTime) -> i64 {
    // Days from the civil date (Howard Hinnant's algorithm).
    let (y, m, d) = (
        i64::from(t.date.year),
        i64::from(t.date.month),
        i64::from(t.date.day),
    );
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    days * 86_400 + i64::from(t.hour) * 3600 + i64::from(t.minute) * 60 + i64::from(t.second)
}

/// The expiry of a key row image, seconds; `None` without one.
fn expiry(img: Option<&sprig_feed::Image>) -> Option<i64> {
    match col(img, "expires_at")? {
        BValue::DateTime(t) => Some(epoch_secs(t)),
        _ => None,
    }
}

/// The key type of a key row image (the `type` enum's member index).
fn key_type(img: Option<&sprig_feed::Image>) -> &'static str {
    match col(img, "type") {
        Some(BValue::Enum(2)) => "hash",
        Some(BValue::Enum(3)) => "list",
        Some(BValue::Enum(4)) => "set",
        Some(BValue::Enum(5)) => "zset",
        Some(BValue::String(s) | BValue::Bytes(s)) => match s.as_slice() {
            b"hash" => "hash",
            b"list" => "list",
            b"set" => "set",
            b"zset" => "zset",
            _ => "string",
        },
        _ => "string",
    }
}

/// The class bits (`keyspace` flags) of the events below.
fn class(ev: &str) -> u16 {
    let f = |s: &[u8]| sprig_redis::keyspace::parse_flags(s).unwrap_or(0);
    match ev {
        "expired" => f(b"x"),
        "new" => f(b"n"),
        "set" => f(b"$"),
        "hset" | "hdel" => f(b"h"),
        "lpush" | "rpush" | "lpop" | "rpop" | "lset" => f(b"l"),
        "sadd" | "srem" => f(b"s"),
        "zadd" | "zrem" => f(b"z"),
        _ => f(b"g"),
    }
}

/// The events of one key's changes (see the module docs).
fn key_events(c: &KeyChanges) -> Vec<&'static str> {
    let mut out = Vec::new();
    let (before, after) = match &c.row {
        Some((Op::Delete, b, _)) => {
            let expired =
                expiry(b.as_ref()).is_some_and(|e| e <= i64::try_from(c.secs).unwrap_or(i64::MAX));
            out.push(if expired { "expired" } else { "del" });
            return out;
        }
        Some((Op::Insert, _, a)) => {
            out.push("new");
            (None, a.as_ref())
        }
        Some((_, b, a)) => (b.as_ref(), a.as_ref()),
        None => (None, None),
    };
    let elements = c.added + c.changed + c.removed;
    // Only the expiry changed: expire or persist.
    if before.is_some()
        && elements == 0
        && col(before, "v") == col(after, "v")
        && expiry(before) != expiry(after)
    {
        out.push(if expiry(after).is_some() {
            "expire"
        } else {
            "persist"
        });
        return out;
    }
    let ty = if after.is_some() {
        key_type(after)
    } else if c.row.is_none() && elements > 0 {
        // Element rows alone: a hash's, a list's or a set's (the sorted-set
        // table's rows are counted the same way; told apart by the table).
        "hash"
    } else {
        "string"
    };
    let ev = match ty {
        "hash" if c.added + c.changed == 0 && c.removed > 0 => "hdel",
        "hash" => "hset",
        "set" if c.added == 0 && c.removed > 0 => "srem",
        "set" => "sadd",
        "zset" if c.added + c.changed == 0 && c.removed > 0 => "zrem",
        "zset" => "zadd",
        "list" => {
            let (lh0, lt0) = (int(col(before, "lh")), int(col(before, "lt")));
            let (lh1, lt1) = (int(col(after, "lh")), int(col(after, "lt")));
            if before.is_none() {
                if lh1 < 0 { "lpush" } else { "rpush" }
            } else if lh1 < lh0 {
                "lpush"
            } else if lt1 > lt0 {
                "rpush"
            } else if lh1 > lh0 {
                "lpop"
            } else if lt1 < lt0 {
                "rpop"
            } else {
                "lset"
            }
        }
        _ => "set",
    };
    out.push(ev);
    out
}

/// Records event `e` (of the Redis tables) in `keys`.
fn record(keys: &mut Vec<KeyChanges>, e: &ChangeEvent, db: u32, elem: bool, zset: bool) {
    let img = e.after.as_ref().or(e.before.as_ref());
    let Some(key) = bytes(col(img, "k")) else {
        return;
    };
    let i = if let Some(i) = keys.iter().position(|c| c.db == db && c.key == key) {
        i
    } else {
        keys.push(KeyChanges {
            db,
            key,
            secs: e.hlc >> 32,
            ..KeyChanges::default()
        });
        keys.len() - 1
    };
    let c = &mut keys[i];
    if elem {
        match e.op {
            Op::Insert => c.added += 1,
            Op::Update => c.changed += 1,
            Op::Delete => c.removed += 1,
            _ => {}
        }
        // Rows of a sorted set without its key row: tell zadd from hset.
        if zset && c.row.is_none() {
            c.row = Some((
                Op::Update,
                None,
                Some(sprig_feed::Image(vec![("type".into(), BValue::Enum(5))])),
            ));
        }
    } else {
        // The first change keeps its before image; the last its after.
        let before = match &c.row {
            Some((_, b, _)) if c.row.as_ref().is_some_and(|(o, _, _)| *o != Op::Insert) => {
                b.clone()
            }
            _ => e.before.clone(),
        };
        let op = match (&c.row, e.op) {
            (Some((Op::Insert, _, _)), Op::Delete) => {
                // Created and deleted in one transaction: a del.
                Op::Delete
            }
            (Some((Op::Insert, _, _)), _) => Op::Insert,
            (_, op) => op,
        };
        c.row = Some((op, before, e.after.clone()));
    }
}

/// The tail as the loop reads it: in a task of its own (decision r9-131),
/// so the subscriber check's wait never cancels a read half-way.
type Feed = sprig_feed::pump::Pump<FeedItem>;

fn feed_of(tail: TenantTail) -> Feed {
    sprig_feed::pump::Pump::start(tail, Feed::AHEAD, "keyspace notifications")
}

/// The next feed item, or `None` after `wait` without one (the loop then
/// checks for subscribers). Cancel-safe.
async fn read(feed: &mut Feed, wait: Duration) -> sprig_common::Result<Option<FeedItem>> {
    match tokio::time::timeout(wait, feed.next()).await {
        Ok(i) => i.map(Some),
        Err(_) => Ok(None),
    }
}

async fn run(
    weak: &std::sync::Weak<sprig_redis::Env>,
    svc: &crate::tenant::TenantServices,
    tenant: TenantId,
    scope: &str,
) -> sprig_common::Result<()> {
    let (clusters, placement, names) = crate::http::changes::feed(svc)?;
    let tail = TenantTail::start(
        tenant,
        StartAt::Now,
        clusters,
        placement,
        names,
        TailOptions {
            progress_every: Duration::from_millis(500),
            ..TailOptions::default()
        },
    )
    .await?;
    let mut feed = feed_of(tail);
    tracing::info!(tenant = %tenant, "keyspace notifications from the feed started");
    let mut keys: Vec<KeyChanges> = Vec::new();
    loop {
        let Some(env) = weak.upgrade() else {
            return Ok(());
        };
        if !env.hub.keyspace_interest(scope) {
            tracing::info!(tenant = %tenant, "keyspace notifications from the feed stopped: no subscriber");
            return Ok(());
        }
        let database = env.config().logical_database;
        drop(env);
        let Some(item) = read(&mut feed, CHECK).await? else {
            continue;
        };
        match item {
            FeedItem::Event(e) => {
                if e.hint == Some(OpHint::Notified) || !e.db_name.eq_ignore_ascii_case(&database) {
                    continue;
                }
                let Some((db, elem)) = redis_table(&e.table) else {
                    continue;
                };
                if matches!(e.op, Op::Insert | Op::Update | Op::Delete) {
                    record(&mut keys, &e, db, elem, e.table.starts_with("__sprig_kz_"));
                }
            }
            FeedItem::TxnEnd { .. } => {
                let Some(env) = weak.upgrade() else {
                    return Ok(());
                };
                let flags =
                    sprig_redis::keyspace::parse_flags(env.keyspace_flags(scope).as_bytes())
                        .unwrap_or(0);
                for c in keys.drain(..) {
                    if flags == 0 {
                        continue;
                    }
                    let evs: Vec<(&'static str, u16, Bytes)> = key_events(&c)
                        .into_iter()
                        .map(|ev| (ev, class(ev), Bytes::from(c.key.clone())))
                        .collect();
                    for (channel, data) in sprig_redis::keyspace::publishes(flags, c.db, &evs) {
                        env.hub.publish(scope, &channel, &data, false);
                    }
                    metrics::counter!("sprig_redis_keyspace_events_total")
                        .increment(evs.len() as u64);
                }
            }
            FeedItem::Progress { .. } => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn img(cols: &[(&str, BValue)]) -> sprig_feed::Image {
        sprig_feed::Image(
            cols.iter()
                .map(|(n, v)| ((*n).to_owned(), v.clone()))
                .collect(),
        )
    }

    fn dt(y: u16, mo: u8, d: u8, h: u8) -> BValue {
        BValue::DateTime(sprig_binlog::DateTime {
            date: sprig_binlog::Date {
                year: y,
                month: mo,
                day: d,
            },
            hour: h,
            minute: 0,
            second: 0,
            micros: 0,
            fsp: 3,
        })
    }

    /// Decision r9-131: the subscriber check's wait (5 s) fires while the
    /// tail waits on a new table's catalog lookup (6 s here): no change
    /// may be lost, so no notification is.
    #[tokio::test(start_paused = true)]
    async fn the_subscriber_check_never_drops_a_change() {
        use sprig_feed::testing::{SlowFeed, keys_once};
        let feed = SlowFeed::new(5, Duration::from_secs(6));
        let mut f = feed_of(feed.tail().await);
        let mut items = Vec::new();
        let end = tokio::time::Instant::now() + Duration::from_secs(120);
        while tokio::time::Instant::now() < end && keys_once(&items).len() < 5 {
            if let Some(i) = read(&mut f, CHECK).await.unwrap() {
                items.push(i);
            }
        }
        assert_eq!(keys_once(&items), (1..=5).collect::<Vec<_>>());
    }

    #[test]
    fn tables_and_times() {
        assert_eq!(redis_table("db0"), Some((0, false)));
        assert_eq!(redis_table("__sprig_ke_db3"), Some((3, true)));
        assert_eq!(redis_table("__sprig_kz_db15"), Some((15, true)));
        assert_eq!(redis_table("orders"), None);
        let BValue::DateTime(t) = dt(2026, 11, 12, 0) else {
            unreachable!()
        };
        assert_eq!(epoch_secs(&t), 1_794_441_600);
    }

    #[test]
    fn events_from_rows() {
        let k = ("k", BValue::Bytes(b"a".to_vec()));
        // A new string key.
        let c = KeyChanges {
            key: b"a".to_vec(),
            row: Some((
                Op::Insert,
                None,
                Some(img(&[k.clone(), ("type", BValue::Enum(1))])),
            )),
            ..KeyChanges::default()
        };
        assert_eq!(key_events(&c), vec!["new", "set"]);
        // A key deleted after its expiry, and one deleted before it.
        let gone = |secs| KeyChanges {
            row: Some((
                Op::Delete,
                Some(img(&[k.clone(), ("expires_at", dt(2026, 11, 12, 0))])),
                None,
            )),
            secs,
            ..KeyChanges::default()
        };
        assert_eq!(key_events(&gone(1_794_441_601)), vec!["expired"]);
        assert_eq!(key_events(&gone(1_794_441_000)), vec!["del"]);
        // Only the expiry set.
        let c = KeyChanges {
            row: Some((
                Op::Update,
                Some(img(&[("v", BValue::Bytes(b"x".to_vec()))])),
                Some(img(&[
                    ("v", BValue::Bytes(b"x".to_vec())),
                    ("expires_at", dt(2030, 1, 1, 0)),
                ])),
            )),
            ..KeyChanges::default()
        };
        assert_eq!(key_events(&c), vec!["expire"]);
        // A hash field deleted; a list grown at the tail.
        let c = KeyChanges {
            row: Some((
                Op::Update,
                Some(img(&[("type", BValue::Enum(2))])),
                Some(img(&[("type", BValue::Enum(2))])),
            )),
            removed: 1,
            ..KeyChanges::default()
        };
        assert_eq!(key_events(&c), vec!["hdel"]);
        let c = KeyChanges {
            row: Some((
                Op::Update,
                Some(img(&[
                    ("type", BValue::Enum(3)),
                    ("lh", BValue::Int(0)),
                    ("lt", BValue::Int(2)),
                ])),
                Some(img(&[
                    ("type", BValue::Enum(3)),
                    ("lh", BValue::Int(0)),
                    ("lt", BValue::Int(3)),
                ])),
            )),
            added: 1,
            ..KeyChanges::default()
        };
        assert_eq!(key_events(&c), vec!["rpush"]);
    }
}
