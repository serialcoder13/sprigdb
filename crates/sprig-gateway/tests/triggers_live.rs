//! Triggers on streams and queues through a live private deployment
//! (chapter 41, "Triggers on streams and queues"; chapter 45, "Exactly-once
//! consume and write"; R9, r9triggers).
//!
//! ```sh
//! export SPRIG_TEST_TRIGGERS_GATEWAY=127.0.0.1:26306        # MySQL wire
//! export SPRIG_TEST_TRIGGERS_AMQP=127.0.0.1:26672           # AMQP 0-9-1
//! export SPRIG_TEST_TRIGGERS_ROOT=mysql://root:sprig-dev-root@127.0.0.1:13301
//! export SPRIG_TEST_TRIGGERS_TENANT=<acme's tenant id>
//! export SPRIG_TEST_TRIGGERS_WORKER_PIDFILE=<state>/worker.pid
//! export SPRIG_TEST_TRIGGERS_WORKER_START='<command that starts a worker>'
//! cargo test -p sprig-gateway --test triggers_live -- --nocapture --test-threads 1
//! ```
//!
//! The worker must run with `SPRIG_LOGIC_TRIGGER_MAX_ATTEMPTS=3`. Works in
//! the databases `r9t_sec`, `r9t_app`, `r9t_q` and `r9t_many` of the
//! tenant `acme` (user `acme.app`), dropped first and at the end.
//!
//! - **The system-tenant example of chapter 45:** three triggers on one
//!   stream (`sync_users` in JavaScript, `sync_users_appb` in Lua, `audit_k`
//!   in KIRun) each run every event exactly once, while the worker running
//!   them is killed with `kill -9` and started again.
//! - **A queue trigger's failing message** becomes a dead letter after 3
//!   attempts; the others run once.
//! - **`db.publish` and `db.mq.publish` inside a rolled-back transaction**
//!   deliver nothing; committed, they deliver once. `sprig.commit_offset`
//!   inside a transaction commits or rolls back with it.
//! - **Twelve streams on one worker** are all sequenced.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    missing_docs
)]

use std::time::{Duration, Instant};

use lapin::options::{BasicPublishOptions, QueueDeclareOptions};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Connection, ConnectionProperties};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use sprig_testkit::skip;

const GATEWAY: &str = "SPRIG_TEST_TRIGGERS_GATEWAY";
const USER: &str = "acme.app";
const PASSWORD: &str = "sprig-dev-app";

struct Env {
    mysql: String,
    amqp: Option<String>,
    root: Option<String>,
    tenant: Option<u64>,
    pidfile: Option<String>,
    start: Option<String>,
}

fn env(test: &str) -> Option<Env> {
    let Some(v) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, test);
        return None;
    };
    let get = |k: &str| std::env::var(k).ok().filter(|s| !s.is_empty());
    Some(Env {
        mysql: v,
        amqp: get("SPRIG_TEST_TRIGGERS_AMQP"),
        root: get("SPRIG_TEST_TRIGGERS_ROOT"),
        tenant: get("SPRIG_TEST_TRIGGERS_TENANT").and_then(|t| t.parse().ok()),
        pidfile: get("SPRIG_TEST_TRIGGERS_WORKER_PIDFILE"),
        start: get("SPRIG_TEST_TRIGGERS_WORKER_START"),
    })
}

async fn conn(addr: &str) -> Conn {
    let (host, port) = addr.rsplit_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(USER))
        .pass(Some(PASSWORD))
        .ssl_opts(Some(
            SslOpts::default().with_danger_accept_invalid_certs(true),
        ));
    Conn::new(opts).await.unwrap()
}

/// A statement's rows as text, or its error number and message.
async fn sql(c: &mut Conn, q: &str) -> Result<Vec<Vec<Option<String>>>, (u16, String)> {
    match c.query::<mysql_async::Row, _>(q).await {
        Ok(rows) => Ok(rows
            .into_iter()
            .map(|r| {
                r.unwrap()
                    .into_iter()
                    .map(|v| match v {
                        mysql_async::Value::NULL => None,
                        mysql_async::Value::Bytes(b) => Some(String::from_utf8_lossy(&b).into()),
                        other => Some(other.as_sql(true)),
                    })
                    .collect()
            })
            .collect()),
        Err(mysql_async::Error::Server(e)) => Err((e.code, e.message)),
        Err(e) => panic!("{q}: {e}"),
    }
}

async fn ok(c: &mut Conn, q: &str) -> Vec<Vec<Option<String>>> {
    sql(c, q).await.unwrap_or_else(|e| panic!("{q}: {e:?}"))
}

async fn one(c: &mut Conn, q: &str) -> String {
    ok(c, q).await[0][0].clone().unwrap_or_default()
}

async fn count(c: &mut Conn, q: &str) -> u64 {
    one(c, q).await.parse().unwrap_or(0)
}

/// Waits until `q` (a count) reaches `n`, up to `secs`.
async fn wait_count(c: &mut Conn, q: &str, n: u64, secs: u64) -> u64 {
    let end = Instant::now() + Duration::from_secs(secs);
    loop {
        let got = count(c, q).await;
        if got >= n || Instant::now() > end {
            return got;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn reset(c: &mut Conn) {
    for db in ["r9t_sec", "r9t_app", "r9t_q", "r9t_many"] {
        let _ = sql(c, &format!("DROP DATABASE IF EXISTS {db}")).await;
    }
}

const SYNC_USERS_JS: &str = r"
CREATE TRIGGER r9t_app.sync_users ON STREAM r9t_sec.user_events LANGUAGE javascript AS $$
export default function (ctx) {
  const e = JSON.parse(ctx.record.value);
  db.exec('INSERT INTO r9t_app.hits_a (p, o, user_id) VALUES (?, ?, ?)',
          [ctx.record.partition, ctx.record.offset, e.user_id]);
  if (e.type === 'user.added')
    db.exec('INSERT INTO r9t_app.users_a (id, email) VALUES (?, ?) ' +
            'ON DUPLICATE KEY UPDATE email = VALUES(email)', [e.user_id, e.email]);
}
$$";

const SYNC_USERS_LUA: &str = r"
CREATE TRIGGER r9t_app.sync_users_appb ON STREAM r9t_sec.user_events LANGUAGE lua AS $$
local e = cjson.decode(ctx.record.value)
db.exec('INSERT INTO r9t_app.hits_b (p, o, user_id) VALUES (?, ?, ?)',
        ctx.record.partition, ctx.record.offset, e.user_id)
$$";

const AUDIT_KIRUN: &str = r#"
CREATE TRIGGER r9t_app.audit_k ON STREAM r9t_sec.user_events LANGUAGE kirun AS '
{"namespace": "App", "name": "Audit",
 "parameters": {
   "partition": {"parameterName": "partition", "schema": {"type": ["INTEGER"]}},
   "offset": {"parameterName": "offset", "schema": {"type": ["LONG"]}}},
 "steps": {
  "ins": {"statementName": "ins", "namespace": "Sprig.Db", "name": "Exec",
    "parameterMap": {
      "sql": {"s": {"key": "s", "type": "VALUE", "value": "INSERT INTO r9t_app.hits_k (p, o) VALUES (?, ?)"}},
      "args": {"a": {"key": "a", "type": "EXPRESSION", "order": 1, "expression": "Arguments.partition"},
               "b": {"key": "b", "type": "EXPRESSION", "order": 2, "expression": "Arguments.offset"}}}}}}'"#;

async fn publish_users(c: &mut Conn, from: u64, to: u64) {
    let mut vals = Vec::new();
    for i in from..to {
        vals.push(format!(
            "({i}, JSON_OBJECT('type', 'user.added', 'user_id', {i}, 'email', 'u{i}@x.example'))"
        ));
        if vals.len() == 25 {
            ok(
                c,
                &format!(
                    "INSERT INTO r9t_sec.user_events (key, value) VALUES {}",
                    vals.join(", ")
                ),
            )
            .await;
            vals.clear();
        }
    }
    if !vals.is_empty() {
        ok(
            c,
            &format!(
                "INSERT INTO r9t_sec.user_events (key, value) VALUES {}",
                vals.join(", ")
            ),
        )
        .await;
    }
}

fn kill_worker(e: &Env) -> bool {
    let (Some(pidfile), Some(start)) = (&e.pidfile, &e.start) else {
        eprintln!("no SPRIG_TEST_TRIGGERS_WORKER_PIDFILE/START: the worker isn't killed");
        return false;
    };
    let pid = std::fs::read_to_string(pidfile).unwrap();
    let pid = pid.trim();
    let st = std::process::Command::new("kill")
        .args(["-9", pid])
        .status()
        .unwrap();
    assert!(st.success(), "kill -9 {pid}");
    eprintln!("worker {pid} killed with -9");
    std::thread::sleep(Duration::from_secs(2));
    let st = std::process::Command::new("sh")
        .args(["-c", start])
        .status()
        .unwrap();
    assert!(st.success(), "worker start");
    eprintln!("worker started again");
    true
}

#[tokio::test]
async fn stream_triggers_each_run_every_event_once_across_a_worker_kill() {
    let Some(e) = env("stream_triggers_each_run_every_event_once_across_a_worker_kill") else {
        return;
    };
    let mut c = conn(&e.mysql).await;
    reset(&mut c).await;
    ok(&mut c, "CREATE DATABASE r9t_sec").await;
    ok(&mut c, "CREATE DATABASE r9t_app").await;
    for t in ["hits_a", "hits_b"] {
        ok(
            &mut c,
            &format!(
                "CREATE TABLE r9t_app.{t} (id BIGINT AUTO_INCREMENT PRIMARY KEY, p INT, o BIGINT, user_id BIGINT)"
            ),
        )
        .await;
    }
    ok(
        &mut c,
        "CREATE TABLE r9t_app.hits_k (id BIGINT AUTO_INCREMENT PRIMARY KEY, p INT, o BIGINT)",
    )
    .await;
    ok(
        &mut c,
        "CREATE TABLE r9t_app.users_a (id BIGINT PRIMARY KEY, email VARCHAR(100))",
    )
    .await;
    ok(
        &mut c,
        "CREATE STREAM r9t_sec.user_events KEY (user_id) PARTITIONS 4",
    )
    .await;
    for t in [SYNC_USERS_JS, SYNC_USERS_LUA, AUDIT_KIRUN] {
        ok(&mut c, t).await;
        let w = ok(&mut c, "SHOW WARNINGS").await;
        eprintln!("{:?}", w.iter().map(|r| r[2].clone()).collect::<Vec<_>>());
    }
    // Let the jobs start (the controller's loop runs every second).
    tokio::time::sleep(Duration::from_secs(4)).await;
    let n: u64 = 400;
    let started = Instant::now();
    publish_users(&mut c, 0, n / 2).await;
    // Mid-way: the worker dies without warning and comes back.
    let killed = wait_count(&mut c, "SELECT COUNT(*) FROM r9t_app.hits_a", 40, 60).await >= 40
        && kill_worker(&e);
    publish_users(&mut c, n / 2, n).await;
    for t in ["hits_a", "hits_b", "hits_k"] {
        let got = wait_count(&mut c, &format!("SELECT COUNT(*) FROM r9t_app.{t}"), n, 180).await;
        eprintln!(
            "{t}: {got} rows after {:.1} s",
            started.elapsed().as_secs_f64()
        );
    }
    // A moment more: a duplicate would show now.
    tokio::time::sleep(Duration::from_secs(3)).await;
    for t in ["hits_a", "hits_b", "hits_k"] {
        let rows = count(&mut c, &format!("SELECT COUNT(*) FROM r9t_app.{t}")).await;
        let distinct = count(
            &mut c,
            &format!("SELECT COUNT(DISTINCT p, o) FROM r9t_app.{t}"),
        )
        .await;
        assert_eq!((rows, distinct), (n, n), "{t}: every event once");
    }
    // Order inside each partition: offsets grow with the insert order.
    let disorder = count(
        &mut c,
        "SELECT COUNT(*) FROM r9t_app.hits_a a JOIN r9t_app.hits_a b \
         ON a.p = b.p AND a.id < b.id AND a.o > b.o",
    )
    .await;
    assert_eq!(disorder, 0, "each partition's records ran in offset order");
    assert_eq!(
        count(&mut c, "SELECT COUNT(*) FROM r9t_app.users_a").await,
        n,
        "sync_users upserted every user"
    );
    eprintln!("worker killed mid-way: {killed}");
}

#[tokio::test]
async fn a_queue_triggers_failing_message_becomes_a_dead_letter() {
    let Some(e) = env("a_queue_triggers_failing_message_becomes_a_dead_letter") else {
        return;
    };
    let Some(amqp) = e.amqp.clone() else {
        eprintln!("no SPRIG_TEST_TRIGGERS_AMQP: skipped");
        return;
    };
    let mut c = conn(&e.mysql).await;
    let _ = sql(&mut c, "DROP DATABASE IF EXISTS r9t_q").await;
    ok(&mut c, "CREATE DATABASE r9t_q").await;
    ok(
        &mut c,
        "CREATE TABLE r9t_q.done (id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)",
    )
    .await;
    let a = Connection::connect(
        &format!("amqp://{USER}:{PASSWORD}@{amqp}/r9t_q"),
        ConnectionProperties::default(),
    )
    .await
    .unwrap();
    let ch = a.create_channel().await.unwrap();
    ch.queue_declare(
        "jobs".into(),
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
    ok(
        &mut c,
        r"CREATE TRIGGER r9t_q.work ON QUEUE r9t_q.jobs LANGUAGE javascript AS $$
export default function (ctx) {
  const b = JSON.parse(ctx.message.body);
  if (b.fail) throw new Error('job ' + b.n + ' always fails');
  db.exec('INSERT INTO r9t_q.done (n) VALUES (?)', [b.n]);
}
$$",
    )
    .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    for i in 0..20 {
        let body = if i == 7 {
            format!("{{\"n\": {i}, \"fail\": true}}")
        } else {
            format!("{{\"n\": {i}}}")
        };
        ch.basic_publish(
            "".into(),
            "jobs".into(),
            BasicPublishOptions::default(),
            body.as_bytes(),
            BasicProperties::default().with_delivery_mode(2),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    }
    let got = wait_count(&mut c, "SELECT COUNT(*) FROM r9t_q.done", 19, 90).await;
    assert_eq!(got, 19, "19 messages ran");
    // The failing one: 3 attempts, then a dead letter.
    if let (Some(root), Some(t)) = (&e.root, e.tenant) {
        let mut r = Conn::from_url(root).await.unwrap();
        let q = format!(
            "SELECT COUNT(*), MAX(attempts), MAX(error_code) FROM t{t}_sprig.logic_dead_letters d \
             JOIN t{t}_sprig.logic_message_sources s ON s.binding_id = d.binding_id \
             WHERE s.binding_id = (SELECT MAX(binding_id) FROM t{t}_sprig.logic_message_sources \
             WHERE name = 'jobs') AND d.event_key LIKE 'm%'"
        );
        let end = Instant::now() + Duration::from_secs(60);
        let row = loop {
            let row: (u64, Option<u32>, Option<u32>) = r.query_first(&q).await.unwrap().unwrap();
            if row.0 > 0 || Instant::now() > end {
                break row;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        assert_eq!(
            row,
            (1, Some(3), Some(39865)),
            "one dead letter, 3 attempts"
        );
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        count(&mut c, "SELECT COUNT(*) FROM r9t_q.done").await,
        19,
        "no message ran twice, the dead one never"
    );
    assert_eq!(
        count(&mut c, "SELECT COUNT(DISTINCT n) FROM r9t_q.done").await,
        19
    );
    a.close(200, "done".into()).await.unwrap();
}

#[tokio::test]
async fn publishing_from_logic_follows_the_transaction() {
    let Some(e) = env("publishing_from_logic_follows_the_transaction") else {
        return;
    };
    let mut c = conn(&e.mysql).await;
    // The stream of the first test, or a fresh one.
    let _ = sql(&mut c, "CREATE DATABASE IF NOT EXISTS r9t_sec").await;
    let _ = sql(&mut c, "DROP STREAM r9t_sec.rb_events").await;
    ok(&mut c, "CREATE STREAM r9t_sec.rb_events PARTITIONS 2").await;
    ok(
        &mut c,
        r"CREATE OR REPLACE PROCEDURE r9t_sec.pub_evt(n BIGINT) LANGUAGE javascript AS $$
db.publish('r9t_sec.rb_events', n, {type: 'rb.test', user_id: n});
$$",
    )
    .await;
    // Rolled back: no record.
    ok(&mut c, "START TRANSACTION").await;
    ok(&mut c, "CALL r9t_sec.pub_evt(900001)").await;
    ok(&mut c, "ROLLBACK").await;
    // Committed: one record.
    ok(&mut c, "START TRANSACTION").await;
    ok(&mut c, "CALL r9t_sec.pub_evt(900002)").await;
    ok(&mut c, "COMMIT").await;
    let got = wait_count(
        &mut c,
        "SELECT COUNT(*) FROM r9t_sec.rb_events WHERE `key` = '900002'",
        1,
        30,
    )
    .await;
    assert_eq!(got, 1, "the committed publish was sequenced once");
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        count(
            &mut c,
            "SELECT COUNT(*) FROM r9t_sec.rb_events WHERE `key` = '900001'"
        )
        .await,
        0,
        "the rolled-back publish never appeared"
    );

    // sprig.commit_offset inside a transaction commits with it.
    if let (Some(root), Some(t)) = (&e.root, e.tenant) {
        let mut r = Conn::from_url(root).await.unwrap();
        let read = |t: u64| {
            format!(
                "SELECT offset_no FROM t{t}_sprig.stream_offsets WHERE group_name = 'r9t_billing' AND partition_no = 1"
            )
        };
        let _ = r
            .query_drop(format!(
                "DELETE FROM t{t}_sprig.stream_offsets WHERE group_name = 'r9t_billing'"
            ))
            .await;
        ok(&mut c, "DROP TABLE IF EXISTS r9t_sec.invoices").await;
        ok(&mut c, "CREATE TABLE r9t_sec.invoices (id INT PRIMARY KEY)").await;
        ok(&mut c, "START TRANSACTION").await;
        ok(&mut c, "INSERT INTO r9t_sec.invoices VALUES (1)").await;
        ok(
            &mut c,
            "CALL sprig.commit_offset('r9t_billing', 'r9t_sec.rb_events', 1, 17)",
        )
        .await;
        ok(&mut c, "ROLLBACK").await;
        let after_rollback: Option<u64> = r.query_first(read(t)).await.unwrap();
        assert_eq!(
            after_rollback, None,
            "a rolled-back commit_offset left nothing"
        );
        ok(&mut c, "START TRANSACTION").await;
        ok(&mut c, "INSERT INTO r9t_sec.invoices VALUES (2)").await;
        ok(
            &mut c,
            "CALL sprig.commit_offset('r9t_billing', 'r9t_sec.rb_events', 1, 18)",
        )
        .await;
        ok(&mut c, "COMMIT").await;
        let after_commit: Option<u64> = r.query_first(read(t)).await.unwrap();
        assert_eq!(
            after_commit,
            Some(18),
            "a committed commit_offset is stored"
        );
        // Outside a transaction: at once.
        ok(
            &mut c,
            "CALL sprig.commit_offset('r9t_billing', 'r9t_sec.rb_events', 1, 19)",
        )
        .await;
        let now: Option<u64> = r.query_first(read(t)).await.unwrap();
        assert_eq!(now, Some(19));
        assert_eq!(
            count(&mut c, "SELECT COUNT(*) FROM r9t_sec.invoices").await,
            1
        );
    }

    // db.mq.publish inside a rolled-back transaction delivers nothing;
    // db.mq.get and db.mq.ack consume inside the caller's transaction.
    // The SQL and logic calls use vhost `/`, the database `mq` (here the
    // private tenant's own).
    if let Some(amqp) = &e.amqp {
        ok(&mut c, "CREATE DATABASE IF NOT EXISTS mq").await;
        let a = Connection::connect(
            &format!("amqp://{USER}:{PASSWORD}@{amqp}/%2f"),
            ConnectionProperties::default(),
        )
        .await
        .unwrap();
        let ch = a.create_channel().await.unwrap();
        ch.queue_declare(
            "r9t_rb".into(),
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
        ok(&mut c, "CREATE DATABASE IF NOT EXISTS r9t_q").await;
        ok(
            &mut c,
            r"CREATE OR REPLACE PROCEDURE r9t_q.enqueue(n BIGINT) LANGUAGE lua AS $$
db.mq.publish('', 'r9t_rb', cjson.encode({n = n}))
$$",
        )
        .await;
        ok(
            &mut c,
            r"CREATE OR REPLACE PROCEDURE r9t_q.take() LANGUAGE javascript AS $$
const m = db.mq.get('r9t_rb', {max: 5, leaseMs: 5000});
for (const x of m) db.mq.ack(x.deliveryToken);
return { got: m.map((x) => JSON.parse(x.body).n).join(',') };
$$",
        )
        .await;
        ok(&mut c, "START TRANSACTION").await;
        ok(&mut c, "CALL r9t_q.enqueue(5001)").await;
        ok(&mut c, "ROLLBACK").await;
        ok(&mut c, "START TRANSACTION").await;
        ok(&mut c, "CALL r9t_q.enqueue(5002)").await;
        ok(&mut c, "COMMIT").await;
        // The forwarder moves committed outbox rows within ~200 ms.
        tokio::time::sleep(Duration::from_secs(2)).await;
        ok(&mut c, "START TRANSACTION").await;
        let got = ok(&mut c, "CALL r9t_q.take()").await;
        ok(&mut c, "COMMIT").await;
        let got = got[0]
            .iter()
            .flatten()
            .cloned()
            .collect::<Vec<_>>()
            .join("|");
        assert!(
            got.ends_with("5002"),
            "only the committed publish arrived: {got}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
        let left = ok(&mut c, "CALL sprig.mq_get('r9t_rb', 10, 1000)").await;
        assert!(
            left.is_empty(),
            "db.mq.ack committed: nothing left ({left:?})"
        );
        a.close(200, "done".into()).await.unwrap();
    }
}

#[tokio::test]
async fn twelve_streams_on_one_worker_are_all_sequenced() {
    let Some(e) = env("twelve_streams_on_one_worker_are_all_sequenced") else {
        return;
    };
    let mut c = conn(&e.mysql).await;
    let _ = sql(&mut c, "DROP DATABASE IF EXISTS r9t_many").await;
    ok(&mut c, "CREATE DATABASE r9t_many").await;
    for round in 0..2 {
        let started = Instant::now();
        for i in 0..12 {
            ok(
                &mut c,
                &format!("CREATE STREAM r9t_many.s{round}_{i} PARTITIONS 1"),
            )
            .await;
            ok(
                &mut c,
                &format!("INSERT INTO r9t_many.s{round}_{i} (key, value) VALUES ('k', 'v{i}')"),
            )
            .await;
        }
        let mut seq = 0;
        let end = Instant::now() + Duration::from_secs(60);
        while Instant::now() < end {
            seq = 0;
            for i in 0..12 {
                if count(
                    &mut c,
                    &format!("SELECT COUNT(*) FROM r9t_many.s{round}_{i}"),
                )
                .await
                    == 1
                {
                    seq += 1;
                }
            }
            if seq == 12 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        eprintln!(
            "round {round}: {seq} of 12 streams sequenced in {:.1} s",
            started.elapsed().as_secs_f64()
        );
        assert_eq!(seq, 12, "every stream got a sequencer");
        // Dropped streams give their slots back.
        for i in 0..12 {
            ok(&mut c, &format!("DROP STREAM r9t_many.s{round}_{i}")).await;
        }
    }
    let _ = sql(&mut c, "DROP DATABASE IF EXISTS r9t_many").await;
}
