//! Change feeds under timer-heavy clients: no change may be lost when a consumer's timer fires while the
//! gateway's tail waits on a catalog lookup.
//!
//! - **HTTP (Server-Sent Events):** a stream, then many new tables, each
//!   getting its first row in a burst. A new table's first row needs a
//!   catalog lookup, and the stream's 20 ms batch window (and heartbeat:
//!   set `api.stream.heartbeat_seconds` to 1 at dc scope, the gateway
//!   reads it only there) fire during those lookups. Every table's insert
//!   must arrive, once. Before the fix, 18 of 200 were lost.
//! - **MongoDB:** a database-wide change stream polled with a 5 ms
//!   `maxAwaitTimeMS` and batches of 1, while documents go into many new
//!   collections. Every insert must arrive, once.
//! - **Redis keyspace notifications from the feed:** SQL writes to keys
//!   (not Redis commands, so the notifications come from the feed),
//!   spread over 12 s so the feed loop's 5 s subscriber checks fire
//!   between them. Every key's notification must arrive, once.
//!
//! Needs a gateway with MySQL, HTTP, MongoDB and Redis listeners, and a
//! tenant user who may create databases:
//!
//! ```sh
//! export SPRIG_TEST_FC_SQL=mysql://acme.app:sprig-dev-app@127.0.0.1:34306
//! export SPRIG_TEST_FC_HTTP=127.0.0.1:34080
//! export SPRIG_TEST_FC_LOGIN=acme.app:sprig-dev-app
//! export SPRIG_TEST_FC_MONGO='mongodb://acme.app:sprig-dev-app@127.0.0.1:34717/?authSource=admin&directConnection=true'
//! export SPRIG_TEST_FC_REDIS=127.0.0.1:34638
//! export SPRIG_TEST_FC_TABLES=200   # new tables per run (default 200)
//! cargo test -p sprig-gateway --test feed_cancel_live -- --nocapture --test-threads 1
//! ```
//!
//! Works in the databases `fc_http` and `fc_mongo` and the Redis keys
//! `fc:*` of the tenant's `redis` database, dropped afterwards. Each test is
//! skipped without its variables.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    missing_docs
)]

use std::collections::BTreeMap;
use std::time::Duration;

use base64::Engine as _;
use futures::StreamExt as _;
use mongodb::bson::{Document, doc};
use mysql_async::prelude::Queryable;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

fn tables() -> usize {
    var("SPRIG_TEST_FC_TABLES").map_or(200, |v| v.parse().unwrap())
}

async fn sql(url: &str) -> mysql_async::Conn {
    let opts = mysql_async::Opts::from_url(url).unwrap();
    let opts = mysql_async::OptsBuilder::from_opts(opts)
        .prefer_socket(false)
        .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true));
    mysql_async::Conn::new(opts).await.unwrap()
}

async fn must(c: &mut mysql_async::Conn, s: &str) {
    if let Err(e) = c.query_drop(s).await {
        panic!("{s}: {e}");
    }
}

/// How many times each name came; the names that didn't come or came
/// more than once.
fn tally(got: &[String], want: &[String]) -> (Vec<String>, Vec<String>) {
    let mut n: BTreeMap<&str, usize> = BTreeMap::new();
    for g in got {
        *n.entry(g.as_str()).or_default() += 1;
    }
    let missing = want
        .iter()
        .filter(|w| !n.contains_key(w.as_str()))
        .cloned()
        .collect();
    let twice = n
        .iter()
        .filter(|(_, c)| **c > 1)
        .map(|(k, _)| (*k).to_owned())
        .collect();
    (missing, twice)
}

/// The `data:` JSON of each SSE event in `text`.
fn sse_events(text: &str) -> Vec<serde_json::Value> {
    text.split("\n\n")
        .filter_map(|block| {
            block
                .lines()
                .find_map(|l| l.strip_prefix("data: "))
                .and_then(|d| serde_json::from_str(d.trim()).ok())
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_stream_keeps_every_new_tables_first_row() {
    let (Some(sql_url), Some(http), Some(login)) = (
        var("SPRIG_TEST_FC_SQL"),
        var("SPRIG_TEST_FC_HTTP"),
        var("SPRIG_TEST_FC_LOGIN"),
    ) else {
        eprintln!("skipped: set SPRIG_TEST_FC_SQL, SPRIG_TEST_FC_HTTP and SPRIG_TEST_FC_LOGIN");
        return;
    };
    let n = tables();
    let mut c = sql(&sql_url).await;
    must(&mut c, "DROP DATABASE IF EXISTS fc_http").await;
    must(&mut c, "CREATE DATABASE fc_http").await;
    for i in 0..n {
        must(
            &mut c,
            &format!("CREATE TABLE fc_http.n{i} (id INT PRIMARY KEY, v INT)"),
        )
        .await;
    }
    // The stream, from now.
    let mut s = tokio::net::TcpStream::connect(&http).await.unwrap();
    let auth = base64::engine::general_purpose::STANDARD.encode(&login);
    let req = format!(
        "GET /v1/databases/fc_http/changes HTTP/1.1\r\nhost: {http}\r\nauthorization: Basic {auth}\r\naccept: text/event-stream\r\n\r\n"
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let reader = tokio::spawn(async move {
        let mut buf = Vec::new();
        let mut chunk = vec![0u8; 65536];
        let end = tokio::time::Instant::now() + Duration::from_secs(90);
        loop {
            match tokio::time::timeout_at(end, s.read(&mut chunk)).await {
                Ok(Ok(k)) if k > 0 => buf.extend_from_slice(&chunk[..k]),
                _ => return buf,
            }
            let t = String::from_utf8_lossy(&buf);
            if t.matches("\"op\":\"insert\"").count() >= n {
                // A little longer, for duplicates.
                let more = tokio::time::Instant::now() + Duration::from_secs(3);
                while let Ok(Ok(k)) = tokio::time::timeout_at(more, s.read(&mut chunk)).await {
                    if k == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..k]);
                }
                return buf;
            }
        }
    });
    tokio::time::sleep(Duration::from_secs(2)).await;
    // Each new table's first row, one transaction each, in a burst from
    // four sessions.
    let mut writers = Vec::new();
    for w in 0..4 {
        let url = sql_url.clone();
        writers.push(tokio::spawn(async move {
            let mut c = sql(&url).await;
            for i in (w..n).step_by(4) {
                must(
                    &mut c,
                    &format!("INSERT INTO fc_http.n{i} VALUES ({i}, {i})"),
                )
                .await;
            }
        }));
    }
    for w in writers {
        w.await.unwrap();
    }
    let buf = reader.await.unwrap();
    let text = String::from_utf8_lossy(&buf).into_owned();
    assert!(
        text.starts_with("HTTP/1.1 200"),
        "{}",
        &text[..text.len().min(400)]
    );
    let evs = sse_events(&text);
    let heartbeats = evs.iter().filter(|e| e["op"] == "heartbeat").count();
    let got: Vec<String> = evs
        .iter()
        .filter(|e| e["op"] == "insert")
        .filter_map(|e| e["object"].as_str().map(str::to_owned))
        .collect();
    let want: Vec<String> = (0..n).map(|i| format!("n{i}")).collect();
    let (missing, twice) = tally(&got, &want);
    eprintln!(
        "http: {} of {n} inserts, {} missing, {} twice, {heartbeats} heartbeats",
        got.len(),
        missing.len(),
        twice.len()
    );
    must(&mut c, "DROP DATABASE IF EXISTS fc_http").await;
    assert!(missing.is_empty(), "missing: {missing:?}");
    assert!(twice.is_empty(), "twice: {twice:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mongo_stream_keeps_every_new_collections_first_document() {
    let Some(uri) = var("SPRIG_TEST_FC_MONGO") else {
        eprintln!("skipped: set SPRIG_TEST_FC_MONGO");
        return;
    };
    let n = tables();
    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let db = client.database("fc_mongo");
    let _ = db.drop().await;
    db.collection::<Document>("seed")
        .insert_one(doc! {"_id": 0})
        .await
        .unwrap();
    let mut st = db
        .watch()
        .max_await_time(Duration::from_millis(5))
        .batch_size(1)
        .await
        .unwrap();
    let writer = {
        let db = db.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            for i in 0..n {
                db.collection::<Document>(&format!("c{i}"))
                    .insert_one(doc! {"_id": i64::try_from(i).unwrap()})
                    .await
                    .unwrap();
            }
        })
    };
    let mut got = Vec::new();
    let end = tokio::time::Instant::now() + Duration::from_secs(90);
    let mut done_at = None;
    loop {
        let stop = done_at.unwrap_or(end);
        match tokio::time::timeout_at(stop, st.next()).await {
            Ok(Some(Ok(e))) => {
                if e.operation_type == mongodb::change_stream::event::OperationType::Insert
                    && let Some(ns) = &e.ns
                    && let Some(coll) = &ns.coll
                {
                    got.push(coll.clone());
                    if got.len() >= n && done_at.is_none() {
                        done_at = Some(tokio::time::Instant::now() + Duration::from_secs(3));
                    }
                }
            }
            Ok(Some(Err(e))) => panic!("change stream failed: {e}"),
            Ok(None) | Err(_) => break,
        }
    }
    writer.await.unwrap();
    let want: Vec<String> = (0..n).map(|i| format!("c{i}")).collect();
    let (missing, twice) = tally(&got, &want);
    eprintln!(
        "mongo: {} of {n} inserts, {} missing, {} twice",
        got.len(),
        missing.len(),
        twice.len()
    );
    let _ = db.drop().await;
    assert!(missing.is_empty(), "missing: {missing:?}");
    assert!(twice.is_empty(), "twice: {twice:?}");
}

/// The keys of the `pmessage` frames in `text` (each frame has the
/// pattern line, then the channel line, then the event).
fn channels(text: &str) -> Vec<String> {
    text.lines()
        .filter_map(|l| l.strip_prefix("__keyspace@0__:"))
        .filter(|k| k.starts_with("fc:") && *k != "fc:*")
        .map(str::to_owned)
        .collect()
}

fn resp(args: &[&str]) -> Vec<u8> {
    let mut v = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        v.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
    }
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redis_keyspace_notifications_from_the_feed_keep_every_change() {
    let (Some(sql_url), Some(redis), Some(login)) = (
        var("SPRIG_TEST_FC_SQL"),
        var("SPRIG_TEST_FC_REDIS"),
        var("SPRIG_TEST_FC_LOGIN"),
    ) else {
        eprintln!("skipped: set SPRIG_TEST_FC_SQL, SPRIG_TEST_FC_REDIS and SPRIG_TEST_FC_LOGIN");
        return;
    };
    let (user, pw) = login.split_once(':').unwrap();
    let keys = 60;
    let mut c = sql(&sql_url).await;
    // Notifications on, and a subscriber.
    let mut admin = tokio::net::TcpStream::connect(&redis).await.unwrap();
    let mut sink = vec![0u8; 4096];
    for cmd in [
        resp(&["AUTH", user, pw]),
        resp(&["CONFIG", "SET", "notify-keyspace-events", "KEA"]),
        // The tenant's Redis database exists after its first write.
        resp(&["SET", "fcinit", "x"]),
    ] {
        admin.write_all(&cmd).await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), admin.read(&mut sink)).await;
    }
    let _ = c
        .query_drop("DELETE FROM redis.db0 WHERE k LIKE 'fc:%'")
        .await;
    let mut sub = tokio::net::TcpStream::connect(&redis).await.unwrap();
    sub.write_all(&resp(&["AUTH", user, pw])).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), sub.read(&mut sink)).await;
    sub.write_all(&resp(&["PSUBSCRIBE", "__keyspace@0__:fc:*"]))
        .await
        .unwrap();
    let reader = tokio::spawn(async move {
        let mut buf = Vec::new();
        let mut chunk = vec![0u8; 65536];
        let mut end = tokio::time::Instant::now() + Duration::from_secs(40);
        let mut all = false;
        while let Ok(Ok(k)) = tokio::time::timeout_at(end, sub.read(&mut chunk)).await {
            if k == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..k]);
            if !all && channels(&String::from_utf8_lossy(&buf)).len() >= keys {
                // A little longer, for duplicates.
                all = true;
                end = tokio::time::Instant::now() + Duration::from_secs(3);
            }
        }
        buf
    });
    tokio::time::sleep(Duration::from_secs(2)).await;
    // SQL writes, so the notifications come from the feed: one every
    // 200 ms, across two of the loop's 5 s subscriber checks.
    for i in 0..keys {
        must(
            &mut c,
            &format!(
                "INSERT INTO redis.db0 (k, type, v, g, _v) VALUES ('fc:{i}', 'string', 'x', 1, 1)"
            ),
        )
        .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let buf = reader.await.unwrap();
    let text = String::from_utf8_lossy(&buf).into_owned();
    let got = channels(&text);
    let want: Vec<String> = (0..keys).map(|i| format!("fc:{i}")).collect();
    let (missing, twice) = tally(&got, &want);
    eprintln!(
        "redis: {} of {keys} notifications, {} missing, {} twice",
        got.len(),
        missing.len(),
        twice.len()
    );
    let _ = c
        .query_drop("DELETE FROM redis.db0 WHERE k LIKE 'fc:%' OR k = 'fcinit'")
        .await;
    admin
        .write_all(&resp(&["CONFIG", "SET", "notify-keyspace-events", ""]))
        .await
        .unwrap();
    assert!(missing.is_empty(), "missing: {missing:?}");
    assert!(twice.is_empty(), "twice: {twice:?}");
}
