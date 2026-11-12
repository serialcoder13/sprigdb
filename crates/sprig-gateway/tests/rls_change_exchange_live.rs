//! Change exchanges filtered per change (chapter 50, "Change feed to an
//! exchange"; chapter 29, RLS round 5, decision RL52):
//! `ALTER TABLE t SPRIG SET (queue.change_exchange = 'x')` publishes each
//! committed change of `t` to exchange `x`, as the user who set it, with
//! routing key `<db>.<t>.<op>` and the change envelope as the body. A user
//! the table's policies bind (here through `FORCE ROW LEVEL SECURITY`) gets
//! only the changes of rows they show: its own rows, a row given to it as
//! an insert (`c`), one taken from it as a delete (`d`), nothing of
//! another user's. A table without policies publishes every change; a
//! `RESET` stops publishing.
//!
//! ```sh
//! export SPRIG_TEST_RLS_SQL=127.0.0.1:36306
//! export SPRIG_TEST_RLS_AMQP=127.0.0.1:36682
//! export SPRIG_TEST_RLS_TENANT=acme
//! cargo test -p sprig-gateway --test rls_change_exchange_live -- --nocapture
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines
)]

use std::time::Duration;

use lapin::options::{
    BasicGetOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
    QueuePurgeOptions,
};
use lapin::types::FieldTable;
use lapin::{Connection, ConnectionProperties, ExchangeKind};
use mysql_async::prelude::Queryable;

const PW: &str = "Rlsx-suite-pw-1";
const DB: &str = "rlsx_suite";

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

async fn run(c: &mut mysql_async::Conn, s: &str) -> Result<(), String> {
    c.query_drop(s).await.map_err(|e| e.to_string())
}

async fn must(c: &mut mysql_async::Conn, s: &str) {
    if let Err(e) = run(c, s).await {
        panic!("{s}: {e}");
    }
}

/// Every message on `queue` until it stays empty for `quiet`, as
/// (routing key, body).
async fn drain(ch: &lapin::Channel, queue: &str, quiet: Duration) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut last = tokio::time::Instant::now();
    loop {
        if let Ok(Some(m)) = ch
            .basic_get(queue.into(), BasicGetOptions { no_ack: true })
            .await
        {
            out.push((
                m.delivery.routing_key.to_string(),
                String::from_utf8_lossy(&m.delivery.data).into_owned(),
            ));
            last = tokio::time::Instant::now();
        } else {
            if last.elapsed() > quiet {
                return out;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }
}

/// What a message shows: `<op letter>:<body column of its image>`.
fn shown(body: &str) -> String {
    let v: serde_json::Value = serde_json::from_str(body).unwrap_or_default();
    let img = if v["after"].is_null() {
        &v["before"]
    } else {
        &v["after"]
    };
    format!(
        "{}:{}",
        v["op"].as_str().unwrap_or("?"),
        img["body"].as_str().unwrap_or("?")
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn change_exchanges_filtered_per_change() {
    let (Ok(sql_addr), Ok(amqp)) = (
        std::env::var("SPRIG_TEST_RLS_SQL"),
        std::env::var("SPRIG_TEST_RLS_AMQP"),
    ) else {
        eprintln!("skipped: set SPRIG_TEST_RLS_SQL and SPRIG_TEST_RLS_AMQP");
        return;
    };
    let mut ck = Checks {
        n: 0,
        failed: Vec::new(),
    };
    let mut admin = sql(&sql_addr, "app", "sprig-dev-app").await;
    for s in [
        format!("ALTER TABLE {DB}.docs SPRIG RESET (queue.change_exchange)"),
        format!("ALTER TABLE {DB}.notes SPRIG RESET (queue.change_exchange)"),
        format!("DROP DATABASE IF EXISTS {DB}"),
        "DROP USER IF EXISTS 'rlsx_alice'@'%'".to_owned(),
        "DROP USER IF EXISTS 'rlsx_writer'@'%'".to_owned(),
    ] {
        let _ = run(&mut admin, &s).await;
    }
    for s in [
        format!("CREATE DATABASE {DB}"),
        format!("CREATE TABLE {DB}.docs (id INT PRIMARY KEY, owner VARCHAR(64), body VARCHAR(64))"),
        format!("CREATE TABLE {DB}.notes (id INT PRIMARY KEY, body VARCHAR(64))"),
        format!("ALTER TABLE {DB}.docs ENABLE ROW LEVEL SECURITY"),
        format!("ALTER TABLE {DB}.docs FORCE ROW LEVEL SECURITY"),
        format!("CREATE POLICY own ON {DB}.docs FOR ALL TO PUBLIC USING (owner = CURRENT_USER())"),
        format!("CREATE USER 'rlsx_alice'@'%' IDENTIFIED BY '{PW}'"),
        // alice manages the tables (ALTER, to set the exchange), so only
        // FORCE makes docs' policies bind her.
        format!("GRANT ALL ON {DB}.* TO 'rlsx_alice'@'%'"),
        format!("CREATE USER 'rlsx_writer'@'%' IDENTIFIED BY '{PW}'"),
        format!("GRANT INSERT, UPDATE, SELECT ON {DB}.* TO 'rlsx_writer'@'%'"),
        "GRANT BYPASS RLS ON *.* TO 'rlsx_writer'@'%'".to_owned(),
    ] {
        must(&mut admin, &s).await;
    }
    // The exchanges and a queue bound to each, in the database's vhost.
    let conn = Connection::connect(
        &format!("amqp://{}.app:sprig-dev-app@{amqp}/{DB}", tenant()),
        ConnectionProperties::default(),
    )
    .await
    .unwrap();
    let ch = conn.create_channel().await.unwrap();
    for (x, q) in [("docs.x", "docs_q"), ("notes.x", "notes_q")] {
        ch.exchange_declare(
            x.into(),
            ExchangeKind::Topic,
            ExchangeDeclareOptions {
                durable: true,
                ..ExchangeDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
        ch.queue_declare(
            q.into(),
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
        ch.queue_bind(
            q.into(),
            x.into(),
            "#".into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
        let _ = ch.queue_purge(q.into(), QueuePurgeOptions::default()).await;
    }
    let mut alice = sql(&sql_addr, "rlsx_alice", PW).await;
    let mut writer = sql(&sql_addr, "rlsx_writer", PW).await;
    for t in ["docs", "notes"] {
        let e = run(
            &mut alice,
            &format!("ALTER TABLE {DB}.{t} SPRIG SET (queue.change_exchange = '{t}.x')"),
        )
        .await;
        ck.ok(&format!("change exchange set on {t}"), e.is_ok(), || {
            format!("{e:?}")
        });
    }
    // The forwarders start within seconds (the controller's loop, then a
    // worker job); changes from the setting on are published.
    tokio::time::sleep(Duration::from_secs(4)).await;
    for s in [
        format!(
            "INSERT INTO {DB}.docs VALUES (1, 'rlsx_alice@%', 'alice-row'), (2, 'rlsx_bob@%', 'bob-secret')"
        ),
        format!(
            "UPDATE {DB}.docs SET owner = 'rlsx_alice@%', body = 'given-to-alice' WHERE id = 2"
        ),
        format!("UPDATE {DB}.docs SET owner = 'rlsx_bob@%', body = 'bob-took-it' WHERE id = 1"),
        format!("INSERT INTO {DB}.docs VALUES (3, 'rlsx_bob@%', 'bob-secret-3')"),
        format!("INSERT INTO {DB}.notes VALUES (1, 'note-one')"),
    ] {
        must(&mut writer, &s).await;
    }
    let mut got = Vec::new();
    let end = tokio::time::Instant::now() + Duration::from_secs(60);
    while tokio::time::Instant::now() < end && got.len() < 3 {
        got.extend(drain(&ch, "docs_q", Duration::from_secs(2)).await);
    }
    got.extend(drain(&ch, "docs_q", Duration::from_secs(3)).await);
    let seen: Vec<String> = got.iter().map(|(_, b)| shown(b)).collect();
    let keys: Vec<&str> = got.iter().map(|(k, _)| k.as_str()).collect();
    ck.ok(
        "a bound setter's exchange gets its rows, visibility changes as c and d",
        seen == vec!["c:alice-row", "c:given-to-alice", "d:alice-row"],
        || format!("{seen:?}"),
    );
    ck.ok(
        "routing keys are <db>.<table>.<op>",
        keys == vec![
            "rlsx_suite.docs.c",
            "rlsx_suite.docs.c",
            "rlsx_suite.docs.d",
        ],
        || format!("{keys:?}"),
    );
    ck.ok(
        "LEAK: another user's row in the exchange",
        !got.iter().any(|(_, b)| b.contains("bob-")),
        || format!("{got:?}"),
    );
    let notes = drain(&ch, "notes_q", Duration::from_secs(3)).await;
    ck.ok(
        "a table without policies publishes every change",
        notes.len() == 1 && notes[0].0 == "rlsx_suite.notes.c" && notes[0].1.contains("note-one"),
        || format!("{notes:?}"),
    );
    // RESET stops publishing.
    must(
        &mut alice,
        &format!("ALTER TABLE {DB}.notes SPRIG RESET (queue.change_exchange)"),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(7)).await;
    must(
        &mut writer,
        &format!("INSERT INTO {DB}.notes VALUES (2, 'after-reset')"),
    )
    .await;
    let after = drain(&ch, "notes_q", Duration::from_secs(4)).await;
    ck.ok("RESET stops publishing", after.is_empty(), || {
        format!("{after:?}")
    });

    // Cleanup.
    let _ = conn.close(0, "done".into()).await;
    for s in [
        format!("ALTER TABLE {DB}.docs SPRIG RESET (queue.change_exchange)"),
        format!("DROP DATABASE IF EXISTS {DB}"),
        "DROP USER IF EXISTS 'rlsx_alice'@'%'".to_owned(),
        "DROP USER IF EXISTS 'rlsx_writer'@'%'".to_owned(),
    ] {
        let _ = run(&mut admin, &s).await;
    }
    eprintln!(
        "change exchanges filtered per change: {} checks, {} failed",
        ck.n,
        ck.failed.len()
    );
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
