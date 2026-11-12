//! Smaller MongoDB 8.1–9.0 reporting items (M4, decision S116):
//! `connectionStatus.authInfo.UUID` (8.1), `serverStatus` with `none: 1`
//! (8.3) and its change-stream metrics (9.0), and change-stream cursors
//! with their position for `currentOp`/`$currentOp` (9.0's
//! `cursor.changeStreams.optime`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::Instant;

use bson::{Binary, Bson, DateTime, Document, Timestamp, doc};

use super::conn::Conn;

/// Change-stream cursors opened on this gateway.
static OPENED: AtomicU64 = AtomicU64::new(0);
/// Change-stream cursors closed.
static CLOSED: AtomicU64 = AtomicU64::new(0);
/// Opened per scope: cluster, database, collection.
static SCOPE: [AtomicU64; 3] = [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];

/// The exponential moving average of closed streams' lifespans, in µs
/// (MongoDB 9.0's `cursor.lifespan.average`; `None` before the first).
static LIFESPAN_AVERAGE: Mutex<Option<f64>> = Mutex::new(None);
/// The weight of the newest lifespan in [`LIFESPAN_AVERAGE`].
const LIFESPAN_ALPHA: f64 = 0.2;

/// MongoDB 9.0's `metrics.changeStreams` option and error counters
/// (bugfix-60), by path below `metrics.changeStreams`.
fn counters() -> std::sync::MutexGuard<'static, HashMap<String, u64>> {
    static M: std::sync::OnceLock<Mutex<HashMap<String, u64>>> = std::sync::OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

fn bump(path: &str) {
    *counters().entry(path.to_owned()).or_default() += 1;
}

/// `option.cursor.batchSize`'s bucket bounds.
const BATCH_BOUNDS: [i64; 5] = [1, 10, 100, 1000, 10000];
/// `option.cursor.maxTimeMS`'s bucket bounds.
const MAX_TIME_BOUNDS: [i64; 13] = [
    100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100_000, 200_000, 500_000, 1_000_000,
];

/// The bucket labels of a histogram with `bounds`: `(-inf, b0)`,
/// `[b0, b1)`, ..., `[bn, inf)`.
fn buckets(bounds: &[i64]) -> Vec<String> {
    let mut out = vec![format!("(-inf, {})", bounds[0])];
    for w in bounds.windows(2) {
        out.push(format!("[{}, {})", w[0], w[1]));
    }
    out.push(format!("[{}, inf)", bounds[bounds.len() - 1]));
    out
}

fn observe(histogram: &str, bounds: &[i64], v: i64) {
    let i = bounds.iter().take_while(|b| v >= **b).count();
    bump(&format!("option.cursor.{histogram}.{}", buckets(bounds)[i]));
}

fn num_of(v: Option<&Bson>) -> Option<i64> {
    match v? {
        Bson::Int32(n) => Some(i64::from(*n)),
        Bson::Int64(n) => Some(*n),
        #[allow(clippy::cast_possible_truncation)]
        Bson::Double(f) => Some(*f as i64),
        _ => None,
    }
}

/// A change stream opened with `$changeStream` options `stage` by the
/// `aggregate` command `body`: its option counters and the cursor
/// option histograms (an `aggregate` without `batchSize` counts the
/// default first batch, 101, as MongoDB backfills one).
pub(crate) fn stream_options(stage: &Document, body: &Document) {
    for (k, v) in stage {
        match (k.as_str(), v) {
            ("fullDocument", Bson::String(m))
                if matches!(m.as_str(), "required" | "updateLookup" | "whenAvailable") =>
            {
                bump(&format!("option.fullDocument.{m}"));
            }
            ("fullDocumentBeforeChange", Bson::String(m))
                if matches!(m.as_str(), "required" | "whenAvailable") =>
            {
                bump(&format!("option.fullDocumentBeforeChange.{m}"));
            }
            ("resumeAfter" | "startAfter" | "startAtOperationTime", _) => {
                bump(&format!("option.{k}"));
            }
            (
                "showExpandedEvents"
                | "showMigrationEvents"
                | "showSystemEvents"
                | "showRawUpdateDescription"
                | "ignoreRemovedShards"
                | "matchCollectionUUIDForUpdateLookup",
                Bson::Boolean(true),
            ) => bump(&format!("option.{k}")),
            _ => {}
        }
    }
    let batch = body
        .get_document("cursor")
        .ok()
        .and_then(|c| num_of(c.get("batchSize")))
        .unwrap_or(101);
    observe("batchSize", &BATCH_BOUNDS, batch);
    if let Some(ms) = num_of(body.get("maxTimeMS")) {
        observe("maxTimeMS", &MAX_TIME_BOUNDS, ms);
    }
}

/// A `getMore` on a change stream: the cursor option histograms, for
/// the options it sets.
pub(crate) fn stream_get_more(body: &Document) {
    if let Some(n) = num_of(body.get("batchSize")) {
        observe("batchSize", &BATCH_BOUNDS, n);
    }
    if let Some(ms) = num_of(body.get("maxTimeMS")) {
        observe("maxTimeMS", &MAX_TIME_BOUNDS, ms);
    }
}

/// An error a change stream's cursor met (opening, resuming or reading),
/// counted as MongoDB 9.0 sorts them.
pub(crate) fn stream_error(e: &sprig_proto_mongo::errors::MongoError) {
    // MongoDB's resumable change-stream errors (network, step-down and
    // shutdown codes), besides the label.
    const RETRIABLE: [i32; 16] = [
        6, 7, 63, 89, 91, 133, 150, 189, 234, 262, 9001, 10107, 11600, 13388, 13435, 13436,
    ];
    let path = match e.code {
        10334 => "error.nonRetriable.bsonObjectTooLarge",
        280 => "error.nonRetriable.changeStreamFatalError",
        286 => "error.nonRetriable.changeStreamHistoryLost",
        11602 => "error.retriable.interruptedDueToReplStateChange",
        c if RETRIABLE.contains(&c)
            || e.labels.contains(&"ResumableChangeStreamError") =>
        {
            "error.retriable.other"
        }
        _ => "error.nonRetriable.other",
    };
    bump(path);
}

/// The option and error counters as `serverStatus` shows them.
fn option_and_error_metrics() -> (Document, Document) {
    let m = counters().clone();
    let get = |p: &str| n(m.get(p).copied().unwrap_or(0));
    let histogram = |name: &str, bounds: &[i64]| {
        let mut d = Document::new();
        let mut total = 0;
        for b in buckets(bounds) {
            let c = m
                .get(&format!("option.cursor.{name}.{b}"))
                .copied()
                .unwrap_or(0);
            total += c;
            d.insert(b, doc! {"count": n(c)});
        }
        d.insert("totalCount", n(total));
        d
    };
    let option = doc! {
        "fullDocument": {
            "required": get("option.fullDocument.required"),
            "updateLookup": get("option.fullDocument.updateLookup"),
            "whenAvailable": get("option.fullDocument.whenAvailable"),
        },
        "fullDocumentBeforeChange": {
            "required": get("option.fullDocumentBeforeChange.required"),
            "whenAvailable": get("option.fullDocumentBeforeChange.whenAvailable"),
        },
        "ignoreRemovedShards": get("option.ignoreRemovedShards"),
        "matchCollectionUUIDForUpdateLookup": get("option.matchCollectionUUIDForUpdateLookup"),
        "resumeAfter": get("option.resumeAfter"),
        "showExpandedEvents": get("option.showExpandedEvents"),
        "showMigrationEvents": get("option.showMigrationEvents"),
        "showRawUpdateDescription": get("option.showRawUpdateDescription"),
        "showSystemEvents": get("option.showSystemEvents"),
        "startAfter": get("option.startAfter"),
        "startAtOperationTime": get("option.startAtOperationTime"),
        "cursor": {
            "batchSize": histogram("batchSize", &BATCH_BOUNDS),
            "maxTimeMS": histogram("maxTimeMS", &MAX_TIME_BOUNDS),
        },
    };
    let error = doc! {
        "nonRetriable": {
            "bsonObjectTooLarge": get("error.nonRetriable.bsonObjectTooLarge"),
            "changeStreamFatalError": get("error.nonRetriable.changeStreamFatalError"),
            "changeStreamHistoryLost": get("error.nonRetriable.changeStreamHistoryLost"),
            "other": get("error.nonRetriable.other"),
        },
        "retriable": {
            "interruptedDueToReplStateChange": get("error.retriable.interruptedDueToReplStateChange"),
            "other": get("error.retriable.other"),
        },
    };
    (option, error)
}

/// The open change-stream cursors and when each opened.
fn open_at() -> std::sync::MutexGuard<'static, HashMap<i64, Instant>> {
    static M: std::sync::OnceLock<Mutex<HashMap<i64, Instant>>> = std::sync::OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// The scope of a change stream on `ns` (`db.coll`, `db.$cmd...` for a
/// database, `admin.$cmd...` for the cluster): 0 cluster, 1 database, 2
/// collection.
pub(crate) fn stream_scope(ns: &str) -> usize {
    let (db, coll) = ns.split_once('.').unwrap_or((ns, ""));
    if coll.is_empty() || coll.starts_with("$cmd") {
        usize::from(db != "admin")
    } else {
        2
    }
}

/// A change-stream cursor `id` opened, of [`stream_scope`] `scope`.
pub(crate) fn stream_opened(id: i64, scope: usize) {
    OPENED.fetch_add(1, Ordering::Relaxed);
    SCOPE[scope.min(2)].fetch_add(1, Ordering::Relaxed);
    open_at().insert(id, Instant::now());
}

/// Cursor `id` closed (any cursor; only change streams are tracked).
pub(crate) fn cursor_closed(id: i64) {
    if let Some(t) = open_at().remove(&id) {
        close(t);
    }
}

fn close(t: Instant) {
    let micros = u64::try_from(t.elapsed().as_micros()).unwrap_or(u64::MAX);
    CLOSED.fetch_add(1, Ordering::Relaxed);
    #[allow(clippy::cast_precision_loss)]
    let x = micros as f64;
    let mut avg = LIFESPAN_AVERAGE
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    *avg = Some(avg.map_or(x, |a| a + LIFESPAN_ALPHA * (x - a)));
}

/// Streams the table no longer holds (timed out, their session killed)
/// count as closed now.
fn sweep(c: &Conn) -> usize {
    let live: std::collections::HashSet<i64> = c.shared.cursors.ids().into_iter().collect();
    let mut m = open_at();
    let gone: Vec<i64> = m.keys().filter(|id| !live.contains(id)).copied().collect();
    for id in gone {
        if let Some(t) = m.remove(&id) {
            close(t);
        }
    }
    m.len()
}

fn n(v: u64) -> Bson {
    Bson::Int64(i64::try_from(v).unwrap_or(i64::MAX))
}

/// `metrics.changeStreams` (MongoDB 9.0's names, the ones SprigDB
/// counts).
fn change_stream_metrics(c: &Conn) -> Document {
    let open = sweep(c);
    let pinned = c
        .shared
        .cursors
        .entries()
        .into_iter()
        .filter(|(_, cur)| cur.try_lock().is_err())
        .count();
    let closed = CLOSED.load(Ordering::Relaxed);
    // MongoDB 9.0's names (bugfix-60): `average` is an exponential moving
    // average in µs, `totalCount` the streams closed.
    let average = LIFESPAN_AVERAGE
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .unwrap_or(0.0);
    let (option, error) = option_and_error_metrics();
    doc! {
        "cursor": {
            "open": {"total": n(open as u64), "pinned": n(pinned.min(open) as u64)},
            "totalOpened": n(OPENED.load(Ordering::Relaxed)),
            "lifespan": {"average": average, "totalCount": n(closed)},
        },
        "option": option,
        "error": error,
        "scope": {
            "cluster": n(SCOPE[0].load(Ordering::Relaxed)),
            "db": n(SCOPE[1].load(Ordering::Relaxed)),
            "collection": n(SCOPE[2].load(Ordering::Relaxed)),
        },
    }
}

/// `serverStatus`: the top-level fields always; each section unless
/// `{<section>: 0}`, or, with `none: 1` (MongoDB 8.3), only the sections
/// asked for by name.
pub(crate) fn server_status(c: &Conn, body: &Document) -> Document {
    let none = body.get("none").is_some_and(truthy);
    let want = |s: &str| match body.get(s) {
        Some(v) => truthy(v),
        None => !none,
    };
    let up = c.shared.started.elapsed();
    let mut d = doc! {
        "host": c.shared.d.gateway_id.clone(),
        "version": c.shared.settings().server_version,
        "process": "mongos",
        "pid": i64::from(std::process::id()),
        "uptime": up.as_secs_f64(),
        "uptimeMillis": i64::try_from(up.as_millis()).unwrap_or(i64::MAX),
        "uptimeEstimate": i64::try_from(up.as_secs()).unwrap_or(i64::MAX),
        "localTime": DateTime::now(),
    };
    if want("connections") {
        let current = i32::try_from(c.shared.d.sessions.list().len()).unwrap_or(i32::MAX);
        d.insert("connections", doc! {"current": current});
    }
    if want("metrics") {
        d.insert(
            "metrics",
            doc! {"changeStreams": change_stream_metrics(c), "cursor": {"open": {"total": n(c.shared.cursors.len() as u64)}}},
        );
    }
    d.insert("ok", 1.0);
    d
}

/// A `serverStatus` section flag: `1`, `true`, ...
pub(crate) fn truthy(v: &Bson) -> bool {
    sprig_doc::filter::truthy(v)
}

/// The connection's UUID (`connectionStatus.authInfo.UUID`, MongoDB 8.1):
/// stable for the connection, from the process id and connection number.
fn conn_uuid(c: &Conn) -> Binary {
    let mut seed = c.shared.process_id.bytes().to_vec();
    seed.extend_from_slice(c.shared.d.gateway_id.as_bytes());
    seed.extend_from_slice(&c.id.to_le_bytes());
    let hi = sprig_doc::meta::fnv64(&seed);
    seed.reverse();
    let lo = sprig_doc::meta::fnv64(&seed);
    let mut b = [0u8; 16];
    b[..8].copy_from_slice(&hi.to_be_bytes());
    b[8..].copy_from_slice(&lo.to_be_bytes());
    // Version 8 (custom), RFC 9562 variant.
    b[6] = (b[6] & 0x0F) | 0x80;
    b[8] = (b[8] & 0x3F) | 0x80;
    Binary {
        subtype: bson::spec::BinarySubtype::Uuid,
        bytes: b.to_vec(),
    }
}

/// `connectionStatus`.
pub(crate) fn connection_status(c: &Conn) -> Document {
    let p = c.principal.as_ref();
    let users: Vec<Document> = p
        .map(|p| {
            doc! {"user": format!("{}.{}", p.tenant_name.clone().unwrap_or_default(), p.user), "db": "admin"}
        })
        .into_iter()
        .collect();
    doc! {
        "authInfo": {"authenticatedUsers": users, "authenticatedUserRoles": [], "UUID": conn_uuid(c)},
        "ok": 1.0,
    }
}

/// The caller's change-stream cursors as `currentOp` entries
/// (`idleCursor`), each with `cursor.changeStreams.optime`: the stream's
/// position, the cluster time of its last resume token. For
/// `$currentOp` with `idleCursors: true`.
pub(crate) fn change_stream_ops(c: &Conn) -> Vec<Document> {
    let who = super::commands::owner(c);
    let mut out = Vec::new();
    let opened = open_at().clone();
    for (id, cur) in c.shared.cursors.entries() {
        let Ok(g) = cur.try_lock() else { continue };
        let Some(cur) = g.as_ref() else { continue };
        if cur.owner != who {
            continue;
        }
        let Some(st) = cur.stream.as_ref() else {
            continue;
        };
        let clamp = sprig_feed::ResumeToken::from_hex(&st.token).map_or(0, |t| t.clamp);
        let ts = Timestamp {
            time: u32::try_from(clamp >> 32).unwrap_or(u32::MAX),
            increment: u32::try_from(clamp & 0xFFFF_FFFF).unwrap_or(0),
        };
        let mut cd = doc! {
            "cursorId": id,
            "tailable": true,
            "awaitData": true,
            "noCursorTimeout": cur.no_timeout,
            // No position before the stream's first event or progress
            // token (its token then has no cluster time).
            "changeStreams": if clamp == 0 { doc! {} } else { doc! {"optime": {"ts": ts, "t": -1i64}} },
        };
        if let Some(t) = opened.get(&id) {
            let ago = i64::try_from(t.elapsed().as_millis()).unwrap_or(0);
            cd.insert(
                "createdDate",
                DateTime::from_millis(DateTime::now().timestamp_millis() - ago),
            );
        }
        let mut d =
            doc! {"type": "idleCursor", "host": "sprigdb", "ns": cur.ns.clone(), "cursor": cd};
        if let Some(l) = &cur.lsid {
            d.insert(
                "lsid",
                doc! {"id": Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: l.clone() }},
            );
        }
        out.push(d);
    }
    out
}

#[cfg(test)]
mod tests {
    use sprig_proto_mongo::errors::MongoError;

    use super::*;

    /// bugfix-60 (S116's leftover): the option counters and histograms, and
    /// the error counters, under MongoDB 9.0's names.
    #[test]
    fn change_stream_option_and_error_counters() {
        stream_options(
            &doc! {"fullDocument": "updateLookup", "fullDocumentBeforeChange": "off", "resumeAfter": {"_data": "00"}, "showExpandedEvents": true},
            &doc! {"aggregate": "c", "cursor": {}, "maxTimeMS": 250},
        );
        stream_options(
            &doc! {"fullDocument": "default", "showExpandedEvents": false},
            &doc! {"aggregate": "c", "cursor": {"batchSize": 5}},
        );
        stream_get_more(&doc! {"getMore": 1i64, "collection": "c", "maxTimeMS": 1000});
        stream_error(&MongoError::new(286, "history lost"));
        stream_error(&MongoError::new(280, "pre-image required"));
        stream_error(&MongoError::new(234, "rls changed").label("ResumableChangeStreamError"));
        stream_error(&MongoError::new(2, "bad value"));
        let (o, e) = option_and_error_metrics();
        let at = |d: &Document, p: &str| {
            let mut cur = d.clone();
            let parts: Vec<&str> = p.split('/').collect();
            for k in &parts[..parts.len() - 1] {
                cur = cur.get_document(k).unwrap().clone();
            }
            cur.get_i64(parts[parts.len() - 1]).unwrap()
        };
        assert_eq!(at(&o, "fullDocument/updateLookup"), 1);
        assert_eq!(at(&o, "fullDocument/required"), 0);
        assert_eq!(at(&o, "fullDocumentBeforeChange/whenAvailable"), 0);
        assert_eq!(at(&o, "resumeAfter"), 1);
        assert_eq!(at(&o, "showExpandedEvents"), 1);
        assert_eq!(at(&o, "startAfter"), 0);
        assert_eq!(at(&o, "cursor/batchSize/[100, 1000)/count"), 1);
        assert_eq!(at(&o, "cursor/batchSize/[1, 10)/count"), 1);
        assert_eq!(at(&o, "cursor/batchSize/totalCount"), 2);
        assert_eq!(at(&o, "cursor/maxTimeMS/[200, 500)/count"), 1);
        assert_eq!(at(&o, "cursor/maxTimeMS/[1000, 2000)/count"), 1);
        assert_eq!(at(&o, "cursor/maxTimeMS/totalCount"), 2);
        assert_eq!(
            o.get_document("cursor")
                .unwrap()
                .get_document("maxTimeMS")
                .unwrap()
                .len(),
            15
        );
        assert_eq!(at(&e, "nonRetriable/changeStreamHistoryLost"), 1);
        assert_eq!(at(&e, "nonRetriable/changeStreamFatalError"), 1);
        assert_eq!(at(&e, "nonRetriable/other"), 1);
        assert_eq!(at(&e, "nonRetriable/bsonObjectTooLarge"), 0);
        assert_eq!(at(&e, "retriable/other"), 1);
        assert_eq!(at(&e, "retriable/interruptedDueToReplStateChange"), 0);
        // The lifespan average is exponential: the newest weighs 0.2.
        close(Instant::now());
        assert!(LIFESPAN_AVERAGE.lock().unwrap().is_some());
    }
}
