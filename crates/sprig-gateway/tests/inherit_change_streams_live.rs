//! Change streams over inheritable tables (`ViewMapper`): each subscriber gets the records of its own space's
//! merged view, never layer rows. Tenants `a > b`; in `a`, the root space
//! and a space `s1` (the home of user `u1`); `docs` has a record hierarchy
//! and is shared to `s1` and to `b`.
//!
//! - A root insert reaches the root's, `s1`'s and `b`'s streams (`b`'s from
//!   `a`'s feed, which its stream tails too).
//! - `s1`'s override is an update in `s1` only; a root change to the
//!   overridden field is suppressed in `s1`, one to another field isn't.
//! - `s1`'s own row is an insert in `s1` only; its delete a delete there.
//! - A subtree move is one update of the moved row: the rows below, whose
//!   paths changed, send nothing.
//! - `b`'s override is an update in `b` only.
//! - No event's key carries `_sprig_space`.
//!
//! ```sh
//! export SPRIG_TEST_ICS_SQL=127.0.0.1:36306
//! export SPRIG_TEST_ICS_HTTP=127.0.0.1:36080
//! export SPRIG_TEST_ICS_TENANTS=r6t3a,r6t3b
//! cargo test -p sprig-gateway --test inherit_change_streams_live -- --nocapture
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

use std::time::Duration;

use base64::Engine as _;
use mysql_async::prelude::Queryable;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PW: &str = "sprig-dev-app";

async fn sql(addr: &str, tenant: &str, user: &str) -> mysql_async::Conn {
    let (host, port) = addr.split_once(':').unwrap();
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{tenant}.{user}")))
        .pass(Some(PW))
        .prefer_socket(false)
        .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true));
    mysql_async::Conn::new(opts).await.unwrap()
}

async fn must(c: &mut mysql_async::Conn, s: &str) {
    if let Err(e) = c.query_drop(s).await {
        panic!("{s}: {e}");
    }
}

/// An NDJSON stream of the changes route, the connection kept.
async fn open(addr: &str, tenant: &str, user: &str, db: &str) -> tokio::net::TcpStream {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let auth = base64::engine::general_purpose::STANDARD.encode(format!("{tenant}.{user}:{PW}"));
    let req = format!(
        "GET /v1/databases/{db}/changes?objects=docs HTTP/1.1\r\nhost: {addr}\r\nauthorization: \
         Basic {auth}\r\n\r\n"
    );
    s.write_all(req.as_bytes()).await.unwrap();
    s
}

/// What a stream got so far, heartbeats left out.
async fn drain(s: &mut tokio::net::TcpStream, secs: u64) -> Vec<serde_json::Value> {
    let end = tokio::time::Instant::now() + Duration::from_secs(secs);
    let mut buf = Vec::new();
    let mut chunk = vec![0u8; 65536];
    loop {
        match tokio::time::timeout_at(end, s.read(&mut chunk)).await {
            Ok(Ok(n)) if n > 0 => buf.extend_from_slice(&chunk[..n]),
            _ => break,
        }
    }
    let text = String::from_utf8_lossy(&buf).into_owned();
    text.lines()
        .filter_map(|l| {
            let l = l.trim();
            // Chunked framing: the JSON lines are what parses.
            serde_json::from_str::<serde_json::Value>(l).ok()
        })
        .filter(|v| v.is_object() && v["op"] != "heartbeat")
        .collect()
}

fn summary(evs: &[serde_json::Value]) -> Vec<String> {
    evs.iter()
        .map(|e| {
            let j = |k: &str| -> serde_json::Value {
                serde_json::from_str(e[k].as_str().unwrap_or("null")).unwrap_or_default()
            };
            let key = j("key");
            assert!(
                key.get("_sprig_space").is_none() && key.get("_sprig_share").is_none(),
                "a layer column in the key: {key}"
            );
            let doc = if e["op"] == "update" {
                j("update")
            } else {
                j("after")
            };
            // The record is read when the event is mapped:
            // events of one batch carry its state at the batch's end, so
            // only the fields named are compared.
            let mut fields: Vec<String> = doc
                .as_object()
                .map(|m| m.keys().filter(|k| *k != "id").cloned().collect())
                .unwrap_or_default();
            fields.sort();
            format!(
                "{} {} {}",
                e["op"].as_str().unwrap_or(""),
                key["id"],
                fields.join(",")
            )
            .trim_end()
            .to_owned()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn change_streams_carry_merged_records() {
    let (Ok(sql_addr), Ok(http_addr), Ok(tenants)) = (
        std::env::var("SPRIG_TEST_ICS_SQL"),
        std::env::var("SPRIG_TEST_ICS_HTTP"),
        std::env::var("SPRIG_TEST_ICS_TENANTS"),
    ) else {
        eprintln!(
            "skipped: set SPRIG_TEST_ICS_SQL, SPRIG_TEST_ICS_HTTP and SPRIG_TEST_ICS_TENANTS"
        );
        return;
    };
    let t: Vec<&str> = tenants.split(',').map(str::trim).collect();
    let (a, b) = (t[0], t[1]);
    let pid = std::process::id() % 10_000;
    let db = format!("ics_{pid}");
    let s1 = format!("ics_{pid}_s1");
    let u1 = format!("ics{pid}u1");
    let mut root = sql(&sql_addr, a, "app").await;
    let _ = root
        .query_drop(format!("DROP SPACE IF EXISTS `{s1}` CASCADE"))
        .await;
    let _ = root
        .query_drop(format!("DROP USER IF EXISTS '{u1}'@'%'"))
        .await;
    must(&mut root, &format!("CREATE DATABASE {db}")).await;
    must(&mut root, &format!("USE {db}")).await;
    must(
        &mut root,
        "CREATE TABLE docs (id INT PRIMARY KEY, parent_id INT NULL, body VARCHAR(40) NOT NULL \
         DEFAULT '', n INT NOT NULL DEFAULT 0)",
    )
    .await;
    must(&mut root, "ALTER TABLE docs ADD HIERARCHY (parent_id)").await;
    must(&mut root, "ALTER TABLE docs SPRIG INHERITABLE KEY (id)").await;
    must(&mut root, &format!("CREATE SPACE `{s1}`")).await;
    must(
        &mut root,
        &format!("CREATE USER '{u1}'@'%' IDENTIFIED BY '{PW}'"),
    )
    .await;
    must(
        &mut root,
        &format!("GRANT SELECT, INSERT, UPDATE, DELETE ON {db}.* TO '{u1}'@'%'"),
    )
    .await;
    // A change stream needs WATCH too.
    must(&mut root, &format!("GRANT WATCH ON {db}.* TO '{u1}'@'%'")).await;
    must(
        &mut root,
        &format!("ALTER SPACE `{s1}` SET HOME FOR '{u1}'"),
    )
    .await;
    must(
        &mut root,
        &format!("CREATE SHARE ics_{pid}_s ON docs TO SPACE `{s1}`"),
    )
    .await;
    must(
        &mut root,
        &format!("CREATE SHARE ics_{pid}_b ON docs TO SPACE `{b}`"),
    )
    .await;
    let mut space = sql(&sql_addr, a, "app").await;
    must(&mut space, &format!("USE {db}")).await;
    must(&mut space, &format!("USE SPACE `{s1}`")).await;
    let mut child = sql(&sql_addr, b, "app").await;
    must(&mut child, &format!("USE {db}")).await;

    let mut s_root = open(&http_addr, a, "app", &db).await;
    let mut s_s1 = open(&http_addr, a, &u1, &db).await;
    let mut s_b = open(&http_addr, b, "app", &db).await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    must(
        &mut root,
        "INSERT INTO docs (id, parent_id, body) VALUES (1, NULL, 'one'), (2, 1, 'two')",
    )
    .await;
    must(&mut space, "UPDATE docs SET body = 's1' WHERE id = 2").await;
    must(&mut root, "UPDATE docs SET body = 'root2' WHERE id = 2").await;
    must(&mut root, "UPDATE docs SET n = 5 WHERE id = 2").await;
    must(
        &mut space,
        "INSERT INTO docs (id, parent_id, body) VALUES (100, 1, 'own')",
    )
    .await;
    must(
        &mut root,
        "INSERT INTO docs (id, parent_id, body) VALUES (3, 1, 'three')",
    )
    .await;
    must(
        &mut root,
        "INSERT INTO docs (id, parent_id, body) VALUES (4, 3, 'four')",
    )
    .await;
    must(&mut root, "UPDATE docs SET parent_id = 2 WHERE id = 3").await;
    must(&mut child, "UPDATE docs SET body = 'b' WHERE id = 1").await;
    must(&mut space, "DELETE FROM docs WHERE id = 100").await;

    let (r, s, c) = tokio::join!(
        drain(&mut s_root, 8),
        drain(&mut s_s1, 8),
        drain(&mut s_b, 8)
    );
    let (r, s, c) = (summary(&r), summary(&s), summary(&c));
    eprintln!("root: {r:#?}\ns1: {s:#?}\nb: {c:#?}");
    let want_root = vec![
        "insert 1 body,n,parent_id",
        "insert 2 body,n,parent_id",
        "update 2 body",
        "update 2 n",
        "insert 3 body,n,parent_id",
        "insert 4 body,n,parent_id",
        "update 3 parent_id",
    ];
    assert_eq!(r, want_root, "the root's stream");
    let want_s1 = [
        "insert 1 body,n,parent_id",
        "insert 2 body,n,parent_id",
        "update 2 body,n,parent_id",
        "update 2 n",
        "insert 100 body,n,parent_id",
        "insert 3 body,n,parent_id",
        "insert 4 body,n,parent_id",
        "update 3 parent_id",
        "delete 100",
    ];
    assert_eq!(s, want_s1, "s1's stream");
    let want_b = [
        "insert 1 body,n,parent_id",
        "insert 2 body,n,parent_id",
        "update 2 body",
        "update 2 n",
        "insert 3 body,n,parent_id",
        "insert 4 body,n,parent_id",
        "update 3 parent_id",
        "update 1 body,n,parent_id",
    ];
    // b's stream merges a's feed and its own: compared as sets of events,
    // a's in a's order.
    let mut cs = c.clone();
    cs.sort();
    let mut wb = want_b.iter().map(ToString::to_string).collect::<Vec<_>>();
    wb.sort();
    assert_eq!(cs, wb, "b's stream");
    let from_a: Vec<&String> = c.iter().filter(|x| !x.starts_with("update 1 ")).collect();
    assert_eq!(
        from_a,
        want_root.iter().collect::<Vec<_>>(),
        "a's events in b's stream"
    );

    // Cleanup: b's override, the shares, the space, the user, the database.
    must(&mut child, "SPRIG RESET docs").await;
    must(&mut root, &format!("DROP SHARE ics_{pid}_b")).await;
    must(&mut root, &format!("DROP SPACE `{s1}` CASCADE")).await;
    must(&mut root, &format!("DROP USER '{u1}'@'%'")).await;
    let _ = root.query_drop(format!("DROP DATABASE {db}")).await;
    let _ = child.query_drop(format!("DROP DATABASE {db}")).await;
}
