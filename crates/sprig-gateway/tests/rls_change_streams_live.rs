//! Change streams filtered per event for users a table's row-level
//! security binds:
//! `Changes.Watch` over gRPC, Server-Sent Events and WebSocket, and Redis
//! keyspace notifications. Each stream is opened as a bound user (alice)
//! while another user's rows (bob's) change: alice gets her own changes, a
//! row that becomes hers as an insert, one that stops being hers as a
//! delete, and nothing of bob's; a policy change applies from the next
//! batch; a resumed stream starts after its token; a user without SELECT
//! is refused. Keyspace notifications also come from the change feed for
//! writes that aren't the gateway's own Redis commands: scripts, SQL and
//! expiries.
//!
//! ```sh
//! export SPRIG_TEST_RLS_SQL=127.0.0.1:39306
//! export SPRIG_TEST_RLS_HTTP=127.0.0.1:39080
//! export SPRIG_TEST_RLS_REDIS=127.0.0.1:39380
//! export SPRIG_TEST_RLS_TENANT=acme
//! cargo test -p sprig-gateway --test rls_change_streams_live -- --nocapture
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines
)]

use std::time::Duration;

use base64::Engine as _;
use mysql_async::prelude::Queryable;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PW: &str = "Rlsc-suite-pw-1";
const DB: &str = "rlsc_suite";

struct Checks {
    n: usize,
    failed: Vec<String>,
}

impl Checks {
    fn ok(&mut self, what: &str, cond: bool, detail: impl FnOnce() -> String) {
        self.n += 1;
        if cond {
            eprintln!("ok   {what}");
        } else {
            let d = detail();
            eprintln!("FAIL {what}: {d}");
            self.failed.push(format!("{what}: {d}"));
        }
    }
}

fn tenant() -> String {
    std::env::var("SPRIG_TEST_RLS_TENANT").unwrap_or_else(|_| "acme".into())
}

async fn sql(addr: &str, user: &str, pw: &str) -> mysql_async::Conn {
    let (host, port) = addr.split_once(':').unwrap();
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{}.{user}", tenant())))
        .pass(Some(pw))
        .prefer_socket(false)
        .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true));
    mysql_async::Conn::new(opts).await.unwrap()
}

async fn must(c: &mut mysql_async::Conn, s: &str) {
    if let Err(e) = c.query_drop(s).await {
        panic!("{s}: {e}");
    }
}

fn basic(user: &str, pw: &str) -> String {
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{}.{user}:{pw}", tenant()))
    )
}

/// Reads from `s` for `secs`, appending to `buf`.
async fn read_for(s: &mut tokio::net::TcpStream, buf: &mut Vec<u8>, secs: u64) {
    let end = tokio::time::Instant::now() + Duration::from_secs(secs);
    let mut chunk = vec![0u8; 65536];
    loop {
        match tokio::time::timeout_at(end, s.read(&mut chunk)).await {
            Ok(Ok(n)) if n > 0 => buf.extend_from_slice(&chunk[..n]),
            _ => return,
        }
    }
}

/// An HTTP/1.1 GET of the changes route as `user`, the connection kept to
/// read the stream; `extra` are more header lines.
async fn open_http(
    addr: &str,
    user: &str,
    pw: &str,
    query: &str,
    extra: &str,
) -> tokio::net::TcpStream {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "GET /v1/databases/{DB}/changes{query} HTTP/1.1\r\nhost: {addr}\r\nauthorization: {}\r\n{extra}\r\n",
        basic(user, pw)
    );
    s.write_all(req.as_bytes()).await.unwrap();
    s
}

/// The `data:` JSON of each SSE event in `text`, with its `id`.
fn sse_events(text: &str) -> Vec<(String, serde_json::Value)> {
    let mut out = Vec::new();
    for block in text.split("\n\n") {
        let mut id = String::new();
        let mut data = None;
        for line in block.lines() {
            if let Some(v) = line.strip_prefix("id: ") {
                v.trim().clone_into(&mut id);
            }
            if let Some(v) = line.strip_prefix("data: ") {
                data = serde_json::from_str(v.trim()).ok();
            }
        }
        if let Some(d) = data {
            out.push((id, d));
        }
    }
    out
}

/// Text frames of a server's WebSocket stream in `b` (after the 101).
fn ws_texts(b: &[u8]) -> Vec<String> {
    let mut out = Vec::new();
    let Some(start) = b.windows(4).position(|w| w == b"\r\n\r\n") else {
        return out;
    };
    let mut i = start + 4;
    while i + 2 <= b.len() {
        let op = b[i] & 0x0F;
        let mut n = usize::from(b[i + 1] & 0x7F);
        let mut at = i + 2;
        if n == 126 {
            if at + 2 > b.len() {
                break;
            }
            n = usize::from(u16::from_be_bytes([b[at], b[at + 1]]));
            at += 2;
        } else if n == 127 {
            if at + 8 > b.len() {
                break;
            }
            let mut x = [0u8; 8];
            x.copy_from_slice(&b[at..at + 8]);
            n = usize::try_from(u64::from_be_bytes(x)).unwrap();
            at += 8;
        }
        if at + n > b.len() {
            break;
        }
        if op == 1 {
            out.push(String::from_utf8_lossy(&b[at..at + n]).into_owned());
        }
        i = at + n;
    }
    out
}

/// What a stream got: (op, key or after text) of each change event.
fn summary(evs: &[serde_json::Value]) -> Vec<String> {
    evs.iter()
        .filter(|e| e["op"] != "heartbeat")
        .map(|e| {
            let j = |k: &str| -> serde_json::Value {
                serde_json::from_str(e[k].as_str().unwrap_or("null")).unwrap_or_default()
            };
            format!(
                "{} {} {}",
                e["op"].as_str().unwrap_or(""),
                j("key")["id"],
                j("after")["body"].as_str().unwrap_or("-")
            )
        })
        .collect()
}

/// RESP: one command.
fn resp(args: &[&str]) -> Vec<u8> {
    let mut v = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        v.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
    }
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn change_streams_filtered_per_event() {
    let (Ok(sql_addr), Ok(http_addr), Ok(redis_addr)) = (
        std::env::var("SPRIG_TEST_RLS_SQL"),
        std::env::var("SPRIG_TEST_RLS_HTTP"),
        std::env::var("SPRIG_TEST_RLS_REDIS"),
    ) else {
        eprintln!("skipped: set SPRIG_TEST_RLS_SQL, SPRIG_TEST_RLS_HTTP and SPRIG_TEST_RLS_REDIS");
        return;
    };
    let mut ck = Checks {
        n: 0,
        failed: Vec::new(),
    };
    let mut admin = sql(&sql_addr, "app", "sprig-dev-app").await;
    let _ = admin
        .query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
        .await;
    for u in ["rlsc_alice", "rlsc_bob", "rlsc_none"] {
        let _ = admin
            .query_drop(format!("DROP USER IF EXISTS '{u}'@'%'"))
            .await;
    }
    must(&mut admin, &format!("CREATE DATABASE {DB}")).await;
    must(
        &mut admin,
        &format!(
            "CREATE TABLE {DB}.docs (id INT PRIMARY KEY, owner VARCHAR(64), body VARCHAR(64))"
        ),
    )
    .await;
    must(
        &mut admin,
        &format!("CREATE TABLE {DB}.other (id INT PRIMARY KEY, v VARCHAR(64))"),
    )
    .await;
    must(
        &mut admin,
        &format!("ALTER TABLE {DB}.docs ENABLE ROW LEVEL SECURITY"),
    )
    .await;
    must(
        &mut admin,
        &format!("CREATE POLICY own ON {DB}.docs USING (owner = CURRENT_USER())"),
    )
    .await;
    for u in ["rlsc_alice", "rlsc_bob", "rlsc_none"] {
        must(
            &mut admin,
            &format!("CREATE USER '{u}'@'%' IDENTIFIED BY '{PW}'"),
        )
        .await;
    }
    for u in ["rlsc_alice", "rlsc_bob"] {
        must(
            &mut admin,
            &format!("GRANT SELECT ON {DB}.docs TO '{u}'@'%'"),
        )
        .await;
    }
    // rlsc_none may read `other` only.
    must(
        &mut admin,
        &format!("GRANT SELECT ON {DB}.other TO 'rlsc_none'@'%'"),
    )
    .await;

    // A user without SELECT on docs is refused.
    let mut s = open_http(
        &http_addr,
        "rlsc_none",
        PW,
        "?objects=docs",
        "accept: text/event-stream\r\n",
    )
    .await;
    let mut buf = Vec::new();
    read_for(&mut s, &mut buf, 3).await;
    let t = String::from_utf8_lossy(&buf).into_owned();
    ck.ok(
        "a user without SELECT is refused",
        t.starts_with("HTTP/1.1 4") && t.contains("1142"),
        || t.chars().take(300).collect(),
    );

    // Open alice's three streams: SSE, WebSocket, gRPC.
    let mut sse = open_http(
        &http_addr,
        "rlsc_alice",
        PW,
        "?objects=docs",
        "accept: text/event-stream\r\n",
    )
    .await;
    // The control: the administrator (not bound) sees bob's rows.
    let mut sse_admin = open_http(
        &http_addr,
        "app",
        "sprig-dev-app",
        "?objects=docs",
        "accept: text/event-stream\r\n",
    )
    .await;
    let mut ws = open_http(
        &http_addr,
        "rlsc_alice",
        PW,
        "?objects=docs",
        "connection: Upgrade\r\nupgrade: websocket\r\nsec-websocket-version: 13\r\nsec-websocket-key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
    )
    .await;
    let channel = tonic::transport::Channel::from_shared(format!("http://{http_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut grpc = sprig_api::proto::v1::changes_client::ChangesClient::new(channel);
    let mut req = tonic::Request::new(sprig_api::proto::v1::WatchRequest {
        database: DB.into(),
        objects: vec!["docs".into()],
        ..Default::default()
    });
    req.metadata_mut()
        .insert("authorization", basic("rlsc_alice", PW).parse().unwrap());
    let mut gstream = grpc.watch(req).await.unwrap().into_inner();
    let grpc_events = std::sync::Arc::new(std::sync::Mutex::new(Vec::<serde_json::Value>::new()));
    let ge = grpc_events.clone();
    tokio::spawn(async move {
        while let Ok(Some(e)) = gstream.message().await {
            ge.lock().unwrap().push(serde_json::json!({
                "op": e.op, "key": e.key, "after": e.after, "resume_token": e.resume_token,
            }));
        }
    });
    // Keys an earlier run left would make the writes below no-ops (an
    // unchanged row reaches no binlog); cleared before anyone subscribes.
    {
        let mut ar = tokio::net::TcpStream::connect(&redis_addr).await.unwrap();
        let mut b = Vec::new();
        ar.write_all(&resp(&[
            "AUTH",
            &format!("{}.app", tenant()),
            "sprig-dev-app",
        ]))
        .await
        .unwrap();
        ar.write_all(&resp(&[
            "DEL",
            "rlsc_alice:k1",
            "rlsc_alice:h",
            "rlsc_alice:lua",
            "rlsc_bob:lua",
            "rlsc_alice:exp",
            "rlsc_bob:secret",
        ]))
        .await
        .unwrap();
        read_for(&mut ar, &mut b, 1).await;
    }
    let _ = admin
        .query_drop("DELETE FROM redis.db0 WHERE k IN ('rlsc_alice:sql', 'rlsc_bob:sql')")
        .await;
    // Redis: alice's keyspace notifications.
    let mut rd = tokio::net::TcpStream::connect(&redis_addr).await.unwrap();
    rd.write_all(&resp(&["AUTH", &format!("{}.rlsc_alice", tenant()), PW]))
        .await
        .unwrap();
    let mut rbuf = Vec::new();
    read_for(&mut rd, &mut rbuf, 1).await;
    rd.write_all(&resp(&["PSUBSCRIBE", "__key*__:*"]))
        .await
        .unwrap();
    let mut rd_admin = tokio::net::TcpStream::connect(&redis_addr).await.unwrap();
    let mut admin_rbuf = Vec::new();
    rd_admin
        .write_all(&resp(&[
            "AUTH",
            &format!("{}.app", tenant()),
            "sprig-dev-app",
        ]))
        .await
        .unwrap();
    read_for(&mut rd_admin, &mut admin_rbuf, 1).await;
    rd_admin
        .write_all(&resp(&["PSUBSCRIBE", "__key*__:*"]))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // The changes, by a user the policies don't bind (the administrator).
    for s in [
        format!("INSERT INTO {DB}.docs VALUES (1, 'rlsc_alice@%', 'alice-own')"),
        format!("INSERT INTO {DB}.docs VALUES (2, 'rlsc_bob@%', 'bob-secret-2')"),
        format!(
            "UPDATE {DB}.docs SET owner = 'rlsc_alice@%', body = 'given-to-alice' WHERE id = 2"
        ),
        format!(
            "UPDATE {DB}.docs SET owner = 'rlsc_bob@%', body = 'taken-from-alice' WHERE id = 1"
        ),
        format!("UPDATE {DB}.docs SET body = 'bob-secret-update' WHERE id = 1"),
        format!("INSERT INTO {DB}.other VALUES (1, 'not-watched')"),
    ] {
        must(&mut admin, &s).await;
    }
    // Redis keys: the key table's policy shows each user its own prefix.
    {
        let mut ar = tokio::net::TcpStream::connect(&redis_addr).await.unwrap();
        let mut b = Vec::new();
        ar.write_all(&resp(&[
            "AUTH",
            &format!("{}.app", tenant()),
            "sprig-dev-app",
        ]))
        .await
        .unwrap();
        read_for(&mut ar, &mut b, 1).await;
        ar.write_all(&resp(&["SET", "rlsc_init", "x"]))
            .await
            .unwrap();
        read_for(&mut ar, &mut b, 1).await;
        drop(ar);
    }
    let _ = admin
        .query_drop("DROP POLICY IF EXISTS rlsc_own ON redis.db0")
        .await;
    must(
        &mut admin,
        "CREATE POLICY rlsc_own ON redis.db0 USING (k LIKE CONCAT(SUBSTRING_INDEX(CURRENT_USER(), '@', 1), ':%'))",
    )
    .await;
    must(
        &mut admin,
        "ALTER TABLE redis.db0 ENABLE ROW LEVEL SECURITY",
    )
    .await;
    must(&mut admin, "GRANT SELECT ON redis.* TO 'rlsc_alice'@'%'").await;
    {
        let mut ar = tokio::net::TcpStream::connect(&redis_addr).await.unwrap();
        let mut b = Vec::new();
        ar.write_all(&resp(&[
            "AUTH",
            &format!("{}.app", tenant()),
            "sprig-dev-app",
        ]))
        .await
        .unwrap();
        read_for(&mut ar, &mut b, 1).await;
        for c in [
            resp(&["CONFIG", "SET", "notify-keyspace-events", "KEA"]),
            resp(&["SET", "rlsc_alice:k1", "v"]),
            resp(&["SET", "rlsc_bob:secret", "v"]),
            resp(&["HSET", "rlsc_alice:h", "f", "v"]),
            resp(&["DEL", "rlsc_bob:secret"]),
        ] {
            ar.write_all(&c).await.unwrap();
            read_for(&mut ar, &mut b, 1).await;
        }
        let t = String::from_utf8_lossy(&b).into_owned();
        ck.ok("redis setup", !t.contains("-ERR"), || t.clone());
        // Writes that aren't Redis commands this gateway knows notify
        // from the change feed: a script's, SQL's, and a key's expiry.
        let mut b = Vec::new();
        for c in [
            resp(&[
                "EVAL",
                "redis.call('SET', KEYS[1], 'x'); redis.call('SET', KEYS[2], 'y'); return 1",
                "2",
                "rlsc_alice:lua",
                "rlsc_bob:lua",
            ]),
            resp(&["SET", "rlsc_alice:exp", "v", "PX", "300"]),
        ] {
            ar.write_all(&c).await.unwrap();
            read_for(&mut ar, &mut b, 1).await;
        }
        let t = String::from_utf8_lossy(&b).into_owned();
        ck.ok("redis script and expiry setup", !t.contains("-ERR"), || {
            t.clone()
        });
    }
    for k in ["rlsc_alice:sql", "rlsc_bob:sql"] {
        must(
            &mut admin,
            &format!(
                "INSERT INTO redis.db0 (k, type, v, g, _v) VALUES ('{k}', 'string', 'from-sql', 1, 1)"
            ),
        )
        .await;
    }

    // What each stream got.
    let mut sbuf = Vec::new();
    read_for(&mut sse, &mut sbuf, 8).await;
    let st = String::from_utf8_lossy(&sbuf).into_owned();
    let sse_evs = sse_events(&st);
    let mut wbuf = Vec::new();
    read_for(&mut ws, &mut wbuf, 2).await;
    let ws_evs: Vec<serde_json::Value> = ws_texts(&wbuf)
        .iter()
        .filter_map(|t| serde_json::from_str(t).ok())
        .collect();
    let g_evs = grpc_events.lock().unwrap().clone();
    let want = [
        "insert 1 alice-own",
        "insert 2 given-to-alice",
        "delete 1 -",
    ];
    for (name, evs, raw) in [
        (
            "SSE",
            sse_evs.iter().map(|(_, e)| e.clone()).collect::<Vec<_>>(),
            st.clone(),
        ),
        (
            "WebSocket",
            ws_evs.clone(),
            String::from_utf8_lossy(&wbuf).into_owned(),
        ),
        ("gRPC", g_evs.clone(), format!("{g_evs:?}")),
    ] {
        let got = summary(&evs);
        ck.ok(
            &format!("{name}: alice's changes, visibility changes as insert and delete"),
            got == want,
            || format!("{got:#?}"),
        );
        ck.ok(
            &format!("LEAK: {name}: bob's rows"),
            !raw.contains("bob-secret") && !raw.contains("taken-from-alice"),
            || raw.chars().take(2000).collect(),
        );
        ck.ok(
            &format!("{name}: other tables not watched"),
            !raw.contains("not-watched"),
            String::new,
        );
    }
    let mut abuf = Vec::new();
    read_for(&mut sse_admin, &mut abuf, 1).await;
    let at = String::from_utf8_lossy(&abuf).into_owned();
    ck.ok(
        "control: the administrator's stream has bob's rows",
        at.contains("bob-secret-2") && at.contains("taken-from-alice"),
        || at.chars().take(1500).collect(),
    );
    drop(sse_admin);
    ck.ok(
        "WebSocket upgraded",
        String::from_utf8_lossy(&wbuf).starts_with("HTTP/1.1 101"),
        || String::from_utf8_lossy(&wbuf).chars().take(200).collect(),
    );

    // A policy change applies from the next batch: a restrictive policy
    // hides alice's rows from her.
    must(
        &mut admin,
        &format!("CREATE POLICY none_after ON {DB}.docs AS RESTRICTIVE USING (id < 100)"),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    must(
        &mut admin,
        &format!(
            "INSERT INTO {DB}.docs VALUES (100, 'rlsc_alice@%', 'alice-hidden-by-new-policy')"
        ),
    )
    .await;
    must(
        &mut admin,
        &format!("INSERT INTO {DB}.docs VALUES (3, 'rlsc_alice@%', 'alice-after-policy')"),
    )
    .await;
    let mut more = Vec::new();
    read_for(&mut sse, &mut more, 6).await;
    let mt = String::from_utf8_lossy(&more).into_owned();
    ck.ok(
        "SSE: a new restrictive policy applies at once",
        mt.contains("alice-after-policy") && !mt.contains("alice-hidden-by-new-policy"),
        || mt.chars().take(1500).collect(),
    );
    drop(sse);
    drop(ws);

    // Resume: an SSE stream opened with Last-Event-ID after alice's first
    // event gets the rest, not the first.
    if let Some((first_id, _)) = sse_evs.iter().find(|(_, e)| e["op"] != "heartbeat") {
        let mut r = open_http(
            &http_addr,
            "rlsc_alice",
            PW,
            "?objects=docs",
            &format!("accept: text/event-stream\r\nlast-event-id: {first_id}\r\n"),
        )
        .await;
        let mut rb = Vec::new();
        read_for(&mut r, &mut rb, 8).await;
        let rt = String::from_utf8_lossy(&rb).into_owned();
        let got = summary(
            &sse_events(&rt)
                .into_iter()
                .map(|(_, e)| e)
                .collect::<Vec<_>>(),
        );
        ck.ok(
            "SSE resumes after Last-Event-ID",
            got.first().is_some_and(|g| g.contains("given-to-alice"))
                && !got.iter().any(|g| g.contains("alice-own"))
                && !rt.contains("bob-secret"),
            || format!("{got:#?}"),
        );
    } else {
        ck.ok("SSE resumes after Last-Event-ID", false, || {
            "no first event".into()
        });
    }

    // Redis keyspace notifications (the expiry's comes from the worker's
    // sweeper, within seconds).
    let end = tokio::time::Instant::now() + Duration::from_secs(40);
    while tokio::time::Instant::now() < end
        && !String::from_utf8_lossy(&rbuf).contains("__keyevent@0__:expired")
    {
        read_for(&mut rd, &mut rbuf, 2).await;
    }
    read_for(&mut rd, &mut rbuf, 2).await;
    let rt = String::from_utf8_lossy(&rbuf).into_owned();
    ck.ok(
        "Redis: alice's keys' notifications",
        rt.contains("__keyspace@0__:rlsc_alice:k1")
            && rt.contains("__keyevent@0__:set")
            && rt.contains("__keyspace@0__:rlsc_alice:h")
            && rt.contains("hset"),
        || rt.clone(),
    );
    read_for(&mut rd_admin, &mut admin_rbuf, 1).await;
    let rat = String::from_utf8_lossy(&admin_rbuf).into_owned();
    ck.ok(
        "control: the administrator's notifications name bob's key",
        rat.contains("__keyspace@0__:rlsc_bob:secret") && rat.contains("del"),
        || rat.clone(),
    );
    ck.ok(
        "LEAK: Redis: bob's key in a notification",
        !rt.contains("rlsc_bob"),
        || rt.clone(),
    );
    // Notifications from the feed.
    ck.ok(
        "Redis: a script's write notifies (from the feed)",
        rt.contains("__keyspace@0__:rlsc_alice:lua"),
        || rt.clone(),
    );
    ck.ok(
        "Redis: a key written through SQL notifies",
        rt.contains("__keyspace@0__:rlsc_alice:sql") && rt.contains("__keyevent@0__:set"),
        || rt.clone(),
    );
    ck.ok(
        "Redis: an expired key notifies",
        rt.contains("__keyspace@0__:rlsc_alice:exp") && rt.contains("__keyevent@0__:expired"),
        || rt.clone(),
    );
    ck.ok(
        "Redis: a command's notification isn't repeated from the feed",
        rt.matches("__keyspace@0__:rlsc_alice:k1").count() == 1,
        || rt.clone(),
    );

    // Cleanup.
    {
        let mut ar = tokio::net::TcpStream::connect(&redis_addr).await.unwrap();
        let mut b = Vec::new();
        ar.write_all(&resp(&[
            "AUTH",
            &format!("{}.app", tenant()),
            "sprig-dev-app",
        ]))
        .await
        .unwrap();
        for c in [
            resp(&["CONFIG", "SET", "notify-keyspace-events", ""]),
            resp(&[
                "DEL",
                "rlsc_alice:k1",
                "rlsc_alice:h",
                "rlsc_init",
                "rlsc_alice:lua",
                "rlsc_bob:lua",
                "rlsc_alice:sql",
                "rlsc_bob:sql",
                "rlsc_alice:exp",
            ]),
        ] {
            ar.write_all(&c).await.unwrap();
            read_for(&mut ar, &mut b, 1).await;
        }
    }
    for s in [
        "DELETE FROM redis.db0 WHERE k IN ('rlsc_alice:sql', 'rlsc_bob:sql')".to_owned(),
        "DROP POLICY IF EXISTS rlsc_own ON redis.db0".to_owned(),
        "ALTER TABLE redis.db0 DISABLE ROW LEVEL SECURITY".to_owned(),
        format!("DROP DATABASE IF EXISTS {DB}"),
        "DROP USER IF EXISTS 'rlsc_alice'@'%'".to_owned(),
        "DROP USER IF EXISTS 'rlsc_bob'@'%'".to_owned(),
        "DROP USER IF EXISTS 'rlsc_none'@'%'".to_owned(),
    ] {
        let _ = admin.query_drop(s).await;
    }
    eprintln!(
        "change streams filtered per event: {} checks, {} failed",
        ck.n,
        ck.failed.len()
    );
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}

/// `sprigctl settings set <key> <value> --scope global` through the
/// command in `SPRIG_TEST_RLS_CTL` (a private deployment's wrapper).
fn global_setting(ctl: &str, key: &str, value: Option<&str>) {
    let mut c = std::process::Command::new(ctl);
    match value {
        Some(v) => c.args([
            "settings", "set", key, v, "--scope", "global", "--reason", "rls test",
        ]),
        None => c.args([
            "settings", "reset", key, "--scope", "global", "--reason", "rls test",
        ]),
    };
    let out = c.output().unwrap();
    assert!(
        out.status.success() || value.is_none(),
        "{key}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// A GET of `/v1/databases/rlsl_suite/changes<query>` as the tenant's
/// administrator, NDJSON.
async fn open_limits(addr: &str, query: &str) -> tokio::net::TcpStream {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "GET /v1/databases/rlsl_suite/changes{query} HTTP/1.1\r\nhost: {addr}\r\nauthorization: {}\r\n\r\n",
        basic("app", "sprig-dev-app")
    );
    s.write_all(req.as_bytes()).await.unwrap();
    s
}

/// The limits on `Changes.Watch`: the open
/// streams per tenant on a gateway (39780), the slow-consumer end (39790);
/// `start_at_hlc` stays refused (39802).
#[allow(clippy::many_single_char_names)] // streams a to d, h
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn change_stream_limits() {
    let (Ok(sql_addr), Ok(http_addr), Ok(ctl)) = (
        std::env::var("SPRIG_TEST_RLS_SQL"),
        std::env::var("SPRIG_TEST_RLS_HTTP"),
        std::env::var("SPRIG_TEST_RLS_CTL"),
    ) else {
        eprintln!("skipped: set SPRIG_TEST_RLS_SQL, SPRIG_TEST_RLS_HTTP and SPRIG_TEST_RLS_CTL");
        return;
    };
    let mut ck = Checks {
        n: 0,
        failed: Vec::new(),
    };
    let mut admin = sql(&sql_addr, "app", "sprig-dev-app").await;
    let _ = admin.query_drop("DROP DATABASE IF EXISTS rlsl_suite").await;
    must(&mut admin, "CREATE DATABASE rlsl_suite").await;
    must(
        &mut admin,
        "CREATE TABLE rlsl_suite.big (id INT PRIMARY KEY, body LONGTEXT)",
    )
    .await;
    // Cleared at the end; a run that panicked can leave them set.
    global_setting(&ctl, "api.stream.max_per_tenant", Some("2"));
    global_setting(&ctl, "api.stream.max_buffered_events", Some("16"));
    tokio::time::sleep(Duration::from_secs(3)).await;

    // 1. Two streams open; the third is refused (39780); closing one frees
    // its slot.
    let mut a = open_limits(&http_addr, "?objects=big").await;
    let mut b = open_limits(&http_addr, "?objects=big").await;
    let (mut ab, mut bb) = (Vec::new(), Vec::new());
    read_for(&mut a, &mut ab, 1).await;
    read_for(&mut b, &mut bb, 1).await;
    let mut c = open_limits(&http_addr, "?objects=big").await;
    let mut cb = Vec::new();
    read_for(&mut c, &mut cb, 2).await;
    let ct = String::from_utf8_lossy(&cb).into_owned();
    ck.ok(
        "the tenant's third stream is refused (api.stream.max_per_tenant)",
        String::from_utf8_lossy(&ab).starts_with("HTTP/1.1 200")
            && String::from_utf8_lossy(&bb).starts_with("HTTP/1.1 200")
            && !ct.starts_with("HTTP/1.1 200")
            && ct.contains("39780"),
        || ct.clone(),
    );
    drop(a);
    drop(b);
    drop(c);
    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut d = open_limits(&http_addr, "?objects=big").await;
    let mut db = Vec::new();
    read_for(&mut d, &mut db, 1).await;
    ck.ok(
        "a closed stream frees its slot",
        String::from_utf8_lossy(&db).starts_with("HTTP/1.1 200"),
        || String::from_utf8_lossy(&db).into_owned(),
    );

    // 2. A client that doesn't read falls behind and its stream ends with
    // 39790 (after at most api.stream.max_buffered_events events).
    for chunk in 0..6 {
        let rows: Vec<String> = (1..=500)
            .map(|i| format!("({}, REPEAT('x', 4000))", chunk * 500 + i))
            .collect();
        must(
            &mut admin,
            &format!("INSERT INTO rlsl_suite.big VALUES {}", rows.join(", ")),
        )
        .await;
    }
    tokio::time::sleep(Duration::from_secs(8)).await;
    let mut slow = Vec::new();
    let end = tokio::time::Instant::now() + Duration::from_secs(60);
    while tokio::time::Instant::now() < end && !String::from_utf8_lossy(&slow).contains("39790") {
        read_for(&mut d, &mut slow, 2).await;
    }
    let st = String::from_utf8_lossy(&slow).into_owned();
    let events = st.matches("\"op\":\"insert\"").count();
    ck.ok(
        "a slow client's stream ends with 39790",
        st.contains("39790") && events < 3000,
        || {
            format!(
                "{events} events; tail {}",
                &st[st.len().saturating_sub(300)..]
            )
        },
    );
    drop(d);

    // 3. start_at_hlc is refused (39802): the feed can't position by time.
    global_setting(&ctl, "api.stream.max_buffered_events", None);
    let mut h = open_limits(
        &http_addr,
        &format!("?objects=big&start_at_hlc={}", 1u64 << 32),
    )
    .await;
    let mut hb = Vec::new();
    read_for(&mut h, &mut hb, 3).await;
    let ht = String::from_utf8_lossy(&hb).into_owned();
    ck.ok(
        "start_at_hlc is refused (39802)",
        !ht.starts_with("HTTP/1.1 200") && ht.contains("39802"),
        || ht.chars().take(600).collect(),
    );
    drop(h);

    global_setting(&ctl, "api.stream.max_per_tenant", None);
    let _ = admin.query_drop("DROP DATABASE IF EXISTS rlsl_suite").await;
    eprintln!(
        "change stream limits: {} checks, {} failed",
        ck.n,
        ck.failed.len()
    );
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
