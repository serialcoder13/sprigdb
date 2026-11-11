//! Row-level security on every path that streams a table's rows out of
//! the gateway's session rewrite (chapter 29, "Change feed consumers"; RLS
//! round 4, decisions RL36 to RL39): change topics read in SQL, views over
//! them, triggers on them, sinks from them, after-commit triggers on the
//! table, and the dead letters those keep. Each path is checked twice:
//! a user the table's policies bind can't set it up, and one that was set
//! up by a user the policies didn't bind stops delivering when they come
//! to bind that user (`FORCE ROW LEVEL SECURITY` binds the table's
//! managers), and goes on when they don't any more.
//!
//! ```sh
//! export SPRIG_TEST_RLS_SQL=127.0.0.1:37306
//! export SPRIG_TEST_RLS_HTTP=127.0.0.1:37080
//! export SPRIG_TEST_RLS_HOOK_PORT=37881   # on sink.destination_allowlist
//! export SPRIG_TEST_RLS_TENANT=acme
//! # tenant settings: logic.trigger_max_attempts 2,
//! # logic.trigger_retry_initial_ms 100, logic.trigger_retry_max_ms 2000
//! cargo test -p sprig-gateway --test rls_streams_live -- --nocapture
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines
)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use mysql_async::prelude::Queryable;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PW: &str = "Rlss-suite-pw-1";
const DB: &str = "rlss_suite";

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
        .db_name(Some(DB))
        .prefer_socket(false)
        .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true));
    mysql_async::Conn::new(opts).await.unwrap()
}

async fn admin_conn(addr: &str) -> mysql_async::Conn {
    let (host, port) = addr.split_once(':').unwrap();
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{}.app", tenant())))
        .pass(Some("sprig-dev-app"))
        .prefer_socket(false)
        .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true));
    mysql_async::Conn::new(opts).await.unwrap()
}

/// A statement's error number and text, or `Ok`.
async fn run(c: &mut mysql_async::Conn, s: &str) -> Result<(), (u16, String)> {
    c.query_drop(s).await.map_err(|e| match e {
        mysql_async::Error::Server(x) => (x.code, x.message),
        other => (0, other.to_string()),
    })
}

async fn must(c: &mut mysql_async::Conn, s: &str) {
    if let Err(e) = run(c, s).await {
        panic!("{s}: {e:?}");
    }
}

async fn texts(c: &mut mysql_async::Conn, s: &str) -> Vec<String> {
    c.query::<Option<String>, _>(s)
        .await
        .unwrap_or_default()
        .into_iter()
        .flatten()
        .collect()
}

/// Waits up to `secs` for `table`'s `v` column to hold a value containing
/// `needle`; whether it did.
async fn arrives(c: &mut mysql_async::Conn, table: &str, needle: &str, secs: u64) -> bool {
    let end = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let v = texts(c, &format!("SELECT v FROM {DB}.{table}")).await;
        if v.iter().any(|x| x.contains(needle)) {
            return true;
        }
        if tokio::time::Instant::now() > end {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

type Received = Arc<Mutex<Vec<String>>>;

/// A webhook receiver on `port`: every POST body, answered 200.
async fn hook_receiver(port: u16) -> Received {
    let got: Received = Arc::default();
    let l = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .unwrap();
    let g = got.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = l.accept().await else {
                return;
            };
            let g = g.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = vec![0u8; 65536];
                loop {
                    let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
                        match s.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                        continue;
                    };
                    let head = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
                    let len: usize = head
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length:"))
                        .and_then(|v| v.trim().parse().ok())
                        .unwrap_or(0);
                    while buf.len() < end + 4 + len {
                        match s.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let body = String::from_utf8_lossy(&buf[end + 4..end + 4 + len]).into_owned();
                    buf.drain(..end + 4 + len);
                    g.lock().unwrap().push(body);
                    if s.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n{}")
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            });
        }
    });
    got
}

async fn hook_has(got: &Received, needle: &str, secs: u64) -> bool {
    let end = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        if got.lock().unwrap().iter().any(|b| b.contains(needle)) {
            return true;
        }
        if tokio::time::Instant::now() > end {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// `GET path` on the HTTP listener as `user` (Basic): status and body.
async fn get(addr: &str, user: &str, pw: &str, path: &str) -> (u16, String) {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let auth =
        base64::engine::general_purpose::STANDARD.encode(format!("{}.{user}:{pw}", tenant()));
    let req = format!(
        "GET {path} HTTP/1.1\r\nhost: {addr}\r\nauthorization: Basic {auth}\r\nconnection: close\r\n\r\n"
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut out = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(20), s.read_to_end(&mut out)).await;
    let text = String::from_utf8_lossy(&out).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_owned())
        .unwrap_or_default();
    (status, body)
}

const TRIGGER_JS: &str = r"CREATE TRIGGER {DB}.{NAME} ON STREAM {DB}.`docs.changes` LANGUAGE javascript AS $$
export default function (ctx) {
  if (ctx.record.value.includes('poison')) throw new Error('poisoned record');
  db.exec('INSERT INTO {DB}.{LEAK} (v) VALUES (?)', [ctx.record.value]);
}
$$";

fn trigger_js(name: &str, leak: &str) -> String {
    TRIGGER_JS
        .replace("{DB}", DB)
        .replace("{NAME}", name)
        .replace("{LEAK}", leak)
}

fn after_commit(name: &str, leak: &str) -> String {
    format!(
        r#"CREATE TRIGGER {DB}.{name} AFTER COMMIT ON {DB}.docs FOR INSERT LANGUAGE kirun AS '{{"namespace":"App","name":"Ac","parameters":{{"body":{{"parameterName":"body","schema":{{"type":["STRING"]}}}}}},"steps":{{"ins":{{"statementName":"ins","namespace":"Sprig.Db","name":"Exec","parameterMap":{{"sql":{{"s":{{"key":"s","type":"VALUE","value":"INSERT INTO {leak} (v) VALUES (?)"}}}},"args":{{"a":{{"key":"a","type":"EXPRESSION","order":1,"expression":"Arguments.body"}}}}}}}}}}}}'"#
    )
}

/// Whether `e` is a refusal: 39366, 1142, 1044 or a sink privilege (39212).
fn refused(e: &Result<(), (u16, String)>) -> bool {
    matches!(e, Err((39366 | 1142 | 1044 | 39212, _)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn row_level_security_on_streamed_rows() {
    let (Ok(sql_addr), Ok(http_addr), Ok(port)) = (
        std::env::var("SPRIG_TEST_RLS_SQL"),
        std::env::var("SPRIG_TEST_RLS_HTTP"),
        std::env::var("SPRIG_TEST_RLS_HOOK_PORT"),
    ) else {
        eprintln!(
            "skipped: set SPRIG_TEST_RLS_SQL, SPRIG_TEST_RLS_HTTP and SPRIG_TEST_RLS_HOOK_PORT"
        );
        return;
    };
    let port: u16 = port.parse().unwrap();
    let got = hook_receiver(port).await;
    let mut ck = Checks {
        n: 0,
        failed: Vec::new(),
    };
    // Setup, as the tenant's administrator.
    let mut admin = admin_conn(&sql_addr).await;
    for s in [
        format!("DROP SINK IF EXISTS {DB}.s_alice"),
        format!("DROP SINK IF EXISTS {DB}.s_alice2"),
        format!("DROP SINK IF EXISTS {DB}.s_mgr"),
    ] {
        let _ = run(&mut admin, &s).await;
    }
    // DROP DATABASE leaves a database's triggers and sinks behind: drop
    // them by name first.
    for t in ["t_mgr", "a_mgr", "t_alice", "a_alice"] {
        let _ = run(&mut admin, &format!("DROP TRIGGER {DB}.{t}")).await;
    }
    let _ = run(&mut admin, &format!("DROP DATABASE IF EXISTS {DB}")).await;
    for u in ["rlss_alice", "rlss_mgr"] {
        let _ = run(&mut admin, &format!("DROP USER IF EXISTS '{u}'@'%'")).await;
    }
    must(&mut admin, &format!("CREATE DATABASE {DB}")).await;
    must(
        &mut admin,
        &format!(
            "CREATE TABLE {DB}.docs (id INT PRIMARY KEY, owner VARCHAR(64), body VARCHAR(64))"
        ),
    )
    .await;
    for t in ["leak_alice", "leak_t", "leak_a", "leak_v"] {
        must(
            &mut admin,
            &format!("CREATE TABLE {DB}.{t} (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)"),
        )
        .await;
    }
    must(
        &mut admin,
        &format!("ALTER TABLE {DB}.docs SPRIG SET (stream.change_topic = on)"),
    )
    .await;
    must(
        &mut admin,
        &format!("ALTER TABLE {DB}.docs ENABLE ROW LEVEL SECURITY"),
    )
    .await;
    must(
        &mut admin,
        &format!("CREATE POLICY own ON {DB}.docs FOR ALL TO PUBLIC USING (owner = CURRENT_USER())"),
    )
    .await;
    for u in ["rlss_alice", "rlss_mgr"] {
        must(
            &mut admin,
            &format!("CREATE USER '{u}'@'%' IDENTIFIED BY '{PW}'"),
        )
        .await;
        must(
            &mut admin,
            &format!("GRANT SPRIG_SINK_ADMIN ON *.* TO '{u}'@'%'"),
        )
        .await;
    }
    // alice: reads and writes, code and triggers, no ALTER: the policies
    // bind her. mgr: ALL, so a manager the policies bind only when forced.
    must(
        &mut admin,
        &format!(
            "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, CREATE VIEW, CREATE ROUTINE, \
             ALTER ROUTINE, TRIGGER, EXECUTE ON {DB}.* TO 'rlss_alice'@'%'"
        ),
    )
    .await;
    must(
        &mut admin,
        &format!("GRANT ALL ON {DB}.* TO 'rlss_mgr'@'%'"),
    )
    .await;
    must(&mut admin, "GRANT SPRIG_SINK_ADMIN ON *.* TO 'app'@'%'").await;
    must(
        &mut admin,
        &format!(
            "CREATE CONNECTION {DB}.hook TYPE WEBHOOK WITH (url = 'http://127.0.0.1:{port}', \
             format = 'ndjson', signature = 'none')"
        ),
    )
    .await;
    for u in ["rlss_alice", "rlss_mgr"] {
        must(
            &mut admin,
            &format!("GRANT USAGE ON CONNECTION {DB}.hook TO '{u}'@'%'"),
        )
        .await;
    }
    must(
        &mut admin,
        &format!(
            "INSERT INTO {DB}.docs VALUES (1, 'rlss_alice@%', 'alice-row'), \
             (2, 'rlss_bob@%', 'bob-secret-0')"
        ),
    )
    .await;
    let mut alice = sql(&sql_addr, "rlss_alice", PW).await;
    let mut mgr = sql(&sql_addr, "rlss_mgr", PW).await;
    let rows = texts(&mut alice, "SELECT body FROM docs").await;
    ck.ok(
        "alice's own rows",
        rows == vec!["alice-row".to_owned()],
        || format!("{rows:?}"),
    );

    // 1. SQL reads of the change topic: refused to alice, read by mgr.
    let e = run(&mut alice, "SELECT * FROM `docs.changes`").await;
    ck.ok(
        "SQL change topic refused to a bound user",
        matches!(e, Err((39366, _))),
        || format!("{e:?}"),
    );
    // The topic's sequencer copies changes into it within seconds.
    let mut v = Vec::new();
    for _ in 0..40 {
        v = texts(&mut mgr, "SELECT CAST(value AS CHAR) FROM `docs.changes`").await;
        if v.iter().any(|x| x.contains("bob-secret-0")) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    ck.ok(
        "SQL change topic read by a manager",
        v.iter().any(|x| x.contains("bob-secret-0")),
        || format!("{v:?}"),
    );

    // 2. A view over the change topic (RL38): refused even to its creator
    // the policies don't bind, since its readers would read the log with
    // the creator's rights.
    let e = run(
        &mut admin,
        &format!("CREATE VIEW {DB}.vch AS SELECT * FROM {DB}.`docs.changes`"),
    )
    .await;
    ck.ok(
        "view over a change topic refused",
        matches!(e, Err((39366, _))),
        || format!("{e:?}"),
    );
    if e.is_ok() {
        tokio::time::sleep(Duration::from_secs(3)).await;
        let v = texts(&mut alice, "SELECT CAST(value AS CHAR) FROM vch").await;
        ck.ok(
            "LEAK: alice reads bob's changes through the view",
            !v.iter().any(|x| x.contains("bob-secret")),
            || format!("{v:?}"),
        );
    }

    // 3. Setting a path up as alice (RL36, RL37): every one refused.
    let e = run(&mut alice, &trigger_js("t_alice", "leak_alice")).await;
    ck.ok(
        "stream trigger on the change topic refused",
        refused(&e),
        || format!("{e:?}"),
    );
    let e = run(
        &mut alice,
        &format!("CREATE SINK {DB}.s_alice FROM STREAM {DB}.`docs.changes` TO WEBHOOK CONNECTION hook WITH (batch_max_wait_ms = 100)"),
    )
    .await;
    ck.ok("sink FROM STREAM t.changes refused", refused(&e), || {
        format!("{e:?}")
    });
    let e = run(
        &mut alice,
        &format!("CREATE SINK {DB}.s_alice2 FROM CHANGES OF {DB}.docs TO WEBHOOK CONNECTION hook WITH (batch_max_wait_ms = 100)"),
    )
    .await;
    ck.ok("sink FROM CHANGES OF refused", refused(&e), || {
        format!("{e:?}")
    });
    let e = run(&mut alice, &after_commit("a_alice", "leak_alice")).await;
    ck.ok("after-commit trigger refused", refused(&e), || {
        format!("{e:?}")
    });
    let e = run(
        &mut alice,
        &format!("CREATE TRIGGER {DB}.n_alice AFTER INSERT ON {DB}.docs FOR EACH ROW INSERT INTO {DB}.leak_alice (v) VALUES (NEW.body)"),
    )
    .await;
    ck.ok("MariaDB trigger on the table refused", refused(&e), || {
        format!("{e:?}")
    });
    // Whatever got through would now see bob's next row.
    must(
        &mut admin,
        &format!("INSERT INTO {DB}.docs VALUES (3, 'rlss_bob@%', 'bob-secret-alice')"),
    )
    .await;
    let leaked_t = arrives(&mut admin, "leak_alice", "bob-secret-alice", 10).await;
    ck.ok("LEAK: alice's code got bob's row", !leaked_t, String::new);
    let leaked_h = hook_has(&got, "bob-secret-alice", 2).await;
    ck.ok(
        "LEAK: alice's sink exported bob's row",
        !leaked_h,
        String::new,
    );

    // 4. The same paths set up by mgr (a manager: not bound) deliver ...
    must(&mut mgr, &trigger_js("t_mgr", "leak_t")).await;
    must(&mut mgr, &after_commit("a_mgr", "leak_a")).await;
    must(
        &mut mgr,
        &format!("CREATE SINK {DB}.s_mgr FROM STREAM {DB}.`docs.changes` TO WEBHOOK CONNECTION hook WITH (batch_max_wait_ms = 100)"),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    must(
        &mut admin,
        &format!("INSERT INTO {DB}.docs VALUES (4, 'rlss_bob@%', 'bob-before-force')"),
    )
    .await;
    ck.ok(
        "manager's stream trigger delivers",
        arrives(&mut admin, "leak_t", "bob-before-force", 60).await,
        String::new,
    );
    ck.ok(
        "manager's after-commit trigger delivers",
        arrives(&mut admin, "leak_a", "bob-before-force", 60).await,
        String::new,
    );
    ck.ok(
        "manager's sink delivers",
        hook_has(&got, "bob-before-force", 60).await,
        String::new,
    );
    // ... and stop once FORCE makes the policies bind mgr.
    must(
        &mut admin,
        &format!("ALTER TABLE {DB}.docs FORCE ROW LEVEL SECURITY"),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    must(
        &mut admin,
        &format!("INSERT INTO {DB}.docs VALUES (5, 'rlss_bob@%', 'bob-while-forced')"),
    )
    .await;
    let t = arrives(&mut admin, "leak_t", "bob-while-forced", 12).await;
    ck.ok(
        "LEAK: forced: stream trigger still delivers",
        !t,
        String::new,
    );
    let a = arrives(&mut admin, "leak_a", "bob-while-forced", 2).await;
    ck.ok(
        "LEAK: forced: after-commit trigger still delivers",
        !a,
        String::new,
    );
    let h = hook_has(&got, "bob-while-forced", 2).await;
    ck.ok("LEAK: forced: sink still exports", !h, String::new);
    let st = texts(
        &mut admin,
        &format!("SELECT state FROM information_schema.SPRIG_SINKS WHERE sink_schema = '{DB}' AND sink_name = 's_mgr'"),
    )
    .await;
    eprintln!("s_mgr while forced: {st:?}");
    // NO FORCE frees mgr: the triggers go on with the rows they held back
    // (nothing was dropped or dead-lettered), the sink after RESUME.
    must(
        &mut admin,
        &format!("ALTER TABLE {DB}.docs NO FORCE ROW LEVEL SECURITY"),
    )
    .await;
    ck.ok(
        "unforced: stream trigger catches up",
        arrives(&mut admin, "leak_t", "bob-while-forced", 60).await,
        String::new,
    );
    ck.ok(
        "unforced: after-commit trigger catches up",
        arrives(&mut admin, "leak_a", "bob-while-forced", 60).await,
        String::new,
    );
    let _ = run(&mut mgr, &format!("ALTER SINK {DB}.s_mgr RESUME")).await;
    ck.ok(
        "unforced: sink resumes",
        hook_has(&got, "bob-while-forced", 60).await,
        String::new,
    );

    // 5. Dead letters (RL39): a record the stream trigger fails on for good
    // keeps the change record; alice (bound) gets it without the event,
    // the administrator (a manager) with it.
    must(
        &mut admin,
        &format!("INSERT INTO {DB}.docs VALUES (6, 'rlss_bob@%', 'bob-poison')"),
    )
    .await;
    let path = format!("/v1/databases/{DB}/logic/trigger/t_mgr/deadLetters");
    let end = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut admin_view = (0, String::new());
    while tokio::time::Instant::now() < end {
        admin_view = get(&http_addr, "app", "sprig-dev-app", &path).await;
        if admin_view.1.contains("bob-poison") {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    ck.ok(
        "dead letter shows its event to a manager",
        admin_view.0 == 200 && admin_view.1.contains("bob-poison"),
        || format!("{admin_view:?}"),
    );
    let alice_view = get(&http_addr, "rlss_alice", PW, &path).await;
    ck.ok(
        "LEAK: dead letter shows bob's row to alice",
        alice_view.0 == 200
            && !alice_view.1.contains("bob-poison")
            && alice_view.1.contains("dead_letters"),
        || format!("{alice_view:?}"),
    );

    // Cleanup.
    for s in [
        format!("DROP TRIGGER {DB}.t_mgr"),
        format!("DROP TRIGGER {DB}.a_mgr"),
        format!("DROP TRIGGER {DB}.t_alice"),
        format!("DROP TRIGGER {DB}.a_alice"),
        format!("DROP SINK IF EXISTS {DB}.s_mgr"),
        format!("DROP SINK IF EXISTS {DB}.s_alice"),
        format!("DROP SINK IF EXISTS {DB}.s_alice2"),
        format!("DROP DATABASE IF EXISTS {DB}"),
        "DROP USER IF EXISTS 'rlss_alice'@'%'".to_owned(),
        "DROP USER IF EXISTS 'rlss_mgr'@'%'".to_owned(),
    ] {
        let _ = run(&mut admin, &s).await;
    }
    eprintln!(
        "row-level security on streamed rows: {} checks, {} failed",
        ck.n,
        ck.failed.len()
    );
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
