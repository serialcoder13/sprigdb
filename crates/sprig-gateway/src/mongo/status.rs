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
/// Change-stream cursors closed, and their summed lifespans.
static CLOSED: AtomicU64 = AtomicU64::new(0);
static LIFESPAN_MICROS: AtomicU64 = AtomicU64::new(0);
/// Opened per scope: cluster, database, collection.
static SCOPE: [AtomicU64; 3] = [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];

/// The open change-stream cursors and when each opened.
fn open_at() -> std::sync::MutexGuard<'static, HashMap<i64, Instant>> {
    static M: std::sync::OnceLock<Mutex<HashMap<i64, Instant>>> = std::sync::OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// A change-stream cursor `id` opened on `ns` (`db.coll`, `db.$cmd...`
/// for a database, `admin.$cmd...` for the cluster).
pub(crate) fn stream_opened(id: i64, ns: &str) {
    OPENED.fetch_add(1, Ordering::Relaxed);
    let (db, coll) = ns.split_once('.').unwrap_or((ns, ""));
    let scope = if coll.is_empty() || coll.starts_with("$cmd") {
        usize::from(db != "admin")
    } else {
        2
    };
    SCOPE[scope].fetch_add(1, Ordering::Relaxed);
    open_at().insert(id, Instant::now());
}

/// Cursor `id` closed (any cursor; only change streams are tracked).
pub(crate) fn cursor_closed(id: i64) {
    if let Some(t) = open_at().remove(&id) {
        close(t);
    }
}

fn close(t: Instant) {
    CLOSED.fetch_add(1, Ordering::Relaxed);
    LIFESPAN_MICROS.fetch_add(
        u64::try_from(t.elapsed().as_micros()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
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
    let total = LIFESPAN_MICROS.load(Ordering::Relaxed);
    doc! {
        "cursor": {
            "open": {"total": n(open as u64), "pinned": n(pinned.min(open) as u64)},
            "totalOpened": n(OPENED.load(Ordering::Relaxed)),
            "lifespan": {"count": n(closed), "averageMicros": n(total.checked_div(closed).unwrap_or(0))},
        },
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
