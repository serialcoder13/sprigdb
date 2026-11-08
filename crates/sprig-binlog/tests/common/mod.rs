//! Helpers for the Compose integration tests.
//!
//! Servers come from the `SPRIG_TEST_*` variables of `sprig_testkit::skip`
//! (`SPRIG_TEST_MARIADB_SINGLE`, `..._REPLICATION`, `..._GALERA`), plus one
//! of this crate's own: `SPRIG_TEST_BINLOG_SCRATCH`, comma-separated URLs of
//! throwaway servers on which tests may change global variables
//! (`log_bin_compress`, `binlog_row_event_fragment_threshold`). Never point
//! it at a shared server. Each test skips when its variable is unset, and
//! fails instead under `SPRIG_TEST_STRICT=1`.
//!
//! Tests use their own schema, `binlog_t_<random>`, and drop it at the end.

#![allow(dead_code, unreachable_pub, clippy::missing_panics_doc)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mysql_async::prelude::Queryable;
use sprig_binlog::{BinlogSource, BinlogStream, Event, GtidPos, StreamOptions};

pub const SINGLE: &str = "SPRIG_TEST_MARIADB_SINGLE";
pub const REPLICATION: &str = "SPRIG_TEST_MARIADB_REPLICATION";
pub const GALERA: &str = "SPRIG_TEST_MARIADB_GALERA";
pub const SCRATCH: &str = "SPRIG_TEST_BINLOG_SCRATCH";

/// The URLs in `var`, or `None` (after reporting a skip) when unset.
pub fn urls(var: &str, test: &str) -> Option<Vec<String>> {
    if let Some(v) = std::env::var(var).ok().filter(|v| !v.trim().is_empty()) {
        Some(
            v.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect(),
        )
    } else {
        let strict = std::env::var("SPRIG_TEST_STRICT")
            .is_ok_and(|s| matches!(s.trim(), "1" | "true" | "yes" | "on"));
        assert!(
            !strict,
            "{test}: {var} is not set and SPRIG_TEST_STRICT is on"
        );
        eprintln!("skipped {test}: set {var} to run it");
        None
    }
}

/// Upper-case hexadecimal, as `HEX()` returns it.
pub fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    b.iter().fold(String::new(), |mut s, x| {
        let _ = write!(s, "{x:02X}");
        s
    })
}

/// A unique schema name, `binlog_t_<random>`.
pub fn schema_name() -> String {
    static N: AtomicU32 = AtomicU32::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!(
        "binlog_t_{:x}{:x}{}",
        std::process::id(),
        nanos,
        N.fetch_add(1, Ordering::Relaxed)
    )
}

/// A binlog `server_id` unique to this test process and call, far from
/// real servers' ids and from `mover.server_id_base`.
pub fn server_id() -> u32 {
    static N: AtomicU32 = AtomicU32::new(0);
    4_100_000_000 + (std::process::id() % 100_000) * 100 + N.fetch_add(1, Ordering::Relaxed) % 100
}

/// A SQL connection with `time_zone = '+00:00'`.
pub async fn sql(url: &str) -> mysql_async::Conn {
    let mut c = mysql_async::Conn::from_url(url).await.expect("connect");
    c.query_drop("SET time_zone = '+00:00'").await.unwrap();
    c
}

pub async fn gtid_pos(c: &mut mysql_async::Conn) -> GtidPos {
    let p: Option<String> = c.query_first("SELECT @@gtid_binlog_pos").await.unwrap();
    p.unwrap_or_default().parse().unwrap()
}

pub fn source(url: &str) -> BinlogSource {
    BinlogSource::from_url(url).unwrap()
}

/// Options filtered to one schema.
pub fn opts(schema: &str) -> StreamOptions {
    let mut o = StreamOptions::new(server_id());
    let s = schema.to_owned();
    o.filter = Some(Arc::new(move |db: &str, _: &str| db == s));
    o
}

/// Reads events from `stream` until its position reaches `target`.
pub async fn read_until(stream: &mut BinlogStream, target: &GtidPos) -> Vec<Event> {
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !stream.position().reached(target) {
        let ev = tokio::time::timeout_at(deadline, stream.next())
            .await
            .unwrap_or_else(|_| panic!("timed out at {} waiting for {target}", stream.position()))
            .unwrap();
        out.push(ev);
    }
    out
}

/// Like [`read_until`], with each event's type code.
pub async fn read_typed(stream: &mut BinlogStream, target: &GtidPos) -> Vec<(u8, Event)> {
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !stream.position().reached(target) {
        let ev = tokio::time::timeout_at(deadline, stream.next())
            .await
            .unwrap_or_else(|_| panic!("timed out at {} waiting for {target}", stream.position()))
            .unwrap();
        out.push((stream.last_header().type_code, ev));
    }
    out
}

/// Opens a stream after `from` and reads until the server's current position.
pub async fn read_range(url: &str, from: &GtidPos, to: &GtidPos, o: &StreamOptions) -> Vec<Event> {
    let mut s = BinlogStream::connect(&source(url), from, o).await.unwrap();
    read_until(&mut s, to).await
}

pub async fn drop_schema(url: &str, schema: &str) {
    let mut c = sql(url).await;
    c.query_drop(format!("DROP DATABASE IF EXISTS {schema}"))
        .await
        .unwrap();
}
