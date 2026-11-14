//! Live tests through a gateway:
//! messages in MariaDB, SQL in the same gateway.
//!
//! ```text
//! SPRIG_AMQP_ADDR=127.0.0.1:36672 SPRIG_AMQP_USER=acme.app SPRIG_AMQP_PASSWORD=sprig-dev-app \
//! SPRIG_MYSQL_URL=mysql://acme.app:sprig-dev-app@127.0.0.1:36306/mq \
//! SPRIG_GATEWAY_RESTART='deploy/compose/gateway-dev.sh restart' \
//! cargo test -p sprig-queues --test live -- --nocapture --test-threads 1
//! ```
//!
//! Without `SPRIG_AMQP_ADDR` every test returns at once.
//!
//! - **Across a gateway restart:** published with confirms, some
//!   consumed and acknowledged, some delivered and left unacknowledged,
//!   the gateway restarted: every message arrives exactly once more
//!   except the acknowledged ones, and the unacknowledged ones come back
//!   with `redelivered`.
//! - **Publish inside a transaction:** a rolled-back transaction's message
//!   is never delivered; a committed one's is, once.
//! - **Acknowledge inside a transaction:** the delete of the message and
//!   the consumer's insert commit together; a rolled-back acknowledgement
//!   leaves the message with its consumer; a second acknowledgement of the
//!   same delivery is 39258.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::similar_names,
    clippy::many_single_char_names,
    clippy::doc_markdown,
    clippy::match_same_arms,
    clippy::cast_possible_truncation,
    missing_docs
)]

use std::collections::HashMap;
use std::time::Duration;

use futures::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicGetOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions, QueueDeclareOptions, QueueDeleteOptions,
};
use lapin::types::{AMQPValue, FieldTable};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
use mysql_async::prelude::Queryable;

fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

fn uniq(p: &str) -> String {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros();
    format!("r9queues_{p}_{t}")
}

async fn amqp() -> Option<Connection> {
    let addr = var("SPRIG_AMQP_ADDR")?;
    let (u, p) = (var("SPRIG_AMQP_USER")?, var("SPRIG_AMQP_PASSWORD")?);
    for _ in 0..60 {
        if let Ok(c) = Connection::connect(
            &format!("amqp://{u}:{p}@{addr}/%2f"),
            ConnectionProperties::default(),
        )
        .await
        {
            return Some(c);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("can't connect to {addr}");
}

async fn sql() -> Option<mysql_async::Conn> {
    let url = var("SPRIG_MYSQL_URL")?;
    Some(
        mysql_async::Conn::new(mysql_async::Opts::from_url(&url).unwrap())
            .await
            .unwrap(),
    )
}

async fn declare(ch: &Channel, q: &str) {
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
}

async fn publish(ch: &Channel, q: &str, body: &str) {
    let c = ch
        .basic_publish(
            "".into(),
            q.into(),
            BasicPublishOptions::default(),
            body.as_bytes(),
            BasicProperties::default().with_delivery_mode(2),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    assert!(c.is_ack(), "{c:?}");
}

/// Gets up to `n` messages within `wait`. Errors (an owner still being
/// taken over after a crash answers 39260 and closes the channel) open a
/// new channel and go on.
async fn drain(ch: &Channel, q: &str, n: usize, wait: Duration) -> Vec<(String, bool)> {
    let mut out = Vec::new();
    let end = tokio::time::Instant::now() + wait;
    let mut retry: Option<Channel> = None;
    while out.len() < n && tokio::time::Instant::now() < end {
        let c = retry.as_ref().unwrap_or(ch);
        match c.basic_get(q.into(), BasicGetOptions::default()).await {
            Ok(Some(m)) => {
                out.push((String::from_utf8(m.data.clone()).unwrap(), m.redelivered));
                m.ack(BasicAckOptions::default()).await.unwrap();
            }
            Ok(None) => tokio::time::sleep(Duration::from_millis(100)).await,
            Err(e) => {
                eprintln!("get: {e}; retrying on a new channel");
                tokio::time::sleep(Duration::from_millis(500)).await;
                let conn = amqp().await.unwrap();
                retry = Some(conn.create_channel().await.unwrap());
                std::mem::forget(conn);
            }
        }
    }
    out
}

fn token(d: &lapin::message::BasicGetMessage) -> String {
    let h = d.properties.headers().as_ref().unwrap();
    match h.inner().get("x-sprig-delivery") {
        Some(AMQPValue::LongString(s)) => s.to_string(),
        other => panic!("no x-sprig-delivery: {other:?}"),
    }
}

#[tokio::test]
async fn survives_a_gateway_restart() {
    let (Some(conn), Some(restart)) = (amqp().await, var("SPRIG_GATEWAY_RESTART")) else {
        return;
    };
    let q = uniq("restart");
    let ch = conn.create_channel().await.unwrap();
    declare(&ch, &q).await;
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    let total = 200;
    for i in 0..total {
        publish(&ch, &q, &format!("m{i:03}")).await;
    }
    // 50 consumed and acknowledged; 20 delivered and left unacknowledged.
    let acked = drain(&ch, &q, 50, Duration::from_secs(20)).await;
    assert_eq!(acked.len(), 50);
    let c2 = conn.create_channel().await.unwrap();
    c2.basic_qos(20, BasicQosOptions::default()).await.unwrap();
    let mut consumer = c2
        .basic_consume(
            q.as_str().into(),
            "held".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let mut held = Vec::new();
    while held.len() < 20 {
        let d = tokio::time::timeout(Duration::from_secs(10), consumer.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        held.push(String::from_utf8(d.data.clone()).unwrap());
    }
    // The gateway goes down with them unacknowledged.
    let st = std::process::Command::new("bash")
        .arg("-c")
        .arg(&restart)
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
        .status()
        .unwrap();
    assert!(st.success(), "restart command failed");
    drop(consumer);
    let conn = amqp().await.unwrap();
    let ch = conn.create_channel().await.unwrap();
    let rest = drain(&ch, &q, total, Duration::from_secs(60)).await;
    let mut count: HashMap<String, usize> = HashMap::new();
    for (b, _) in acked.iter().chain(rest.iter()) {
        *count.entry(b.clone()).or_default() += 1;
    }
    for i in 0..total {
        let b = format!("m{i:03}");
        assert_eq!(
            count.get(&b),
            Some(&1),
            "{b} after the restart: {:?}",
            count.get(&b)
        );
    }
    for (b, redelivered) in &rest {
        if held.contains(b) {
            assert!(
                redelivered,
                "{b} was delivered before the restart: redelivered"
            );
        }
    }
    let conn2 = amqp().await.unwrap();
    let ch = conn2.create_channel().await.unwrap();
    let n = ch
        .queue_delete(q.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
    assert_eq!(n, 0);
    eprintln!(
        "restart: {total} confirmed, 50 acked before, 20 held across it (all redelivered), {} after",
        rest.len()
    );
}

#[tokio::test]
async fn transactional_publish_commits_or_rolls_back_with_the_data() {
    let (Some(conn), Some(mut db)) = (amqp().await, sql().await) else {
        return;
    };
    let q = uniq("txpub");
    let t = uniq("orders");
    let ch = conn.create_channel().await.unwrap();
    declare(&ch, &q).await;
    db.query_drop(format!(
        "CREATE TABLE {t} (id INT PRIMARY KEY, status VARCHAR(20))"
    ))
    .await
    .unwrap();
    db.query_drop(format!("INSERT INTO {t} VALUES (7001, 'new')"))
        .await
        .unwrap();
    // Rolled back: the update and the message both vanish.
    db.query_drop("START TRANSACTION").await.unwrap();
    db.query_drop(format!("UPDATE {t} SET status = 'paid' WHERE id = 7001"))
        .await
        .unwrap();
    let routed: Option<u32> = db
        .query_first(format!(
            "CALL sprig.mq_publish('', '{q}', 'rolled-back', JSON_OBJECT('message_id', 'm-rb'))"
        ))
        .await
        .unwrap();
    assert_eq!(routed, Some(1));
    db.query_drop("ROLLBACK").await.unwrap();
    // Committed: both stay.
    db.query_drop("START TRANSACTION").await.unwrap();
    db.query_drop(format!("UPDATE {t} SET status = 'paid' WHERE id = 7001"))
        .await
        .unwrap();
    let _: Option<u32> = db
        .query_first(format!(
            "CALL sprig.mq_publish('', '{q}', JSON_OBJECT('order_id', 7001), JSON_OBJECT('content_type', 'application/json', 'message_id', 'm-commit'))"
        ))
        .await
        .unwrap();
    db.query_drop("COMMIT").await.unwrap();
    let status: Option<String> = db
        .query_first(format!("SELECT status FROM {t} WHERE id = 7001"))
        .await
        .unwrap();
    assert_eq!(status.as_deref(), Some("paid"));
    let got = drain(&ch, &q, 2, Duration::from_secs(5)).await;
    assert_eq!(got.len(), 1, "only the committed message: {got:?}");
    let v: serde_json::Value = serde_json::from_str(&got[0].0).unwrap();
    assert_eq!(v["order_id"], 7001);
    // Outside a transaction: at once.
    let routed: Option<u32> = db
        .query_first(format!("CALL sprig.mq_publish('', '{q}', 'now', NULL)"))
        .await
        .unwrap();
    assert_eq!(routed, Some(1));
    let got = drain(&ch, &q, 1, Duration::from_secs(5)).await;
    assert_eq!(got[0].0, "now");
    // Mandatory with no queue: 39255.
    let e = db
        .query_drop("CALL sprig.mq_publish('', 'r9queues-no-such-queue', 'x', NULL, 1)")
        .await
        .unwrap_err();
    assert!(
        e.to_string().contains("39255") || e.to_string().contains("UNROUTABLE"),
        "{e}"
    );
    db.query_drop(format!("DROP TABLE {t}")).await.unwrap();
    ch.queue_delete(q.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
    eprintln!("transactional publish: rolled back = not delivered; committed = delivered once");
}

#[tokio::test]
async fn ack_inside_a_transaction_commits_with_the_consumers_writes() {
    let (Some(conn), Some(mut db)) = (amqp().await, sql().await) else {
        return;
    };
    let q = uniq("txack");
    let t = uniq("invoices");
    let ch = conn.create_channel().await.unwrap();
    declare(&ch, &q).await;
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    publish(&ch, &q, "job-1").await;
    publish(&ch, &q, "job-2").await;
    db.query_drop(format!("CREATE TABLE {t} (job VARCHAR(20) PRIMARY KEY)"))
        .await
        .unwrap();
    // A consumer connection that takes both and never acknowledges.
    let consumer_conn = amqp().await.unwrap();
    let cc = consumer_conn.create_channel().await.unwrap();
    let j1 = cc
        .basic_get(q.as_str().into(), BasicGetOptions::default())
        .await
        .unwrap()
        .unwrap();
    let j2 = cc
        .basic_get(q.as_str().into(), BasicGetOptions::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((&*j1.data, &*j2.data), (&b"job-1"[..], &b"job-2"[..]));
    // job-1: the insert and the acknowledgement commit together.
    db.query_drop("START TRANSACTION").await.unwrap();
    db.query_drop(format!("INSERT INTO {t} VALUES ('job-1')"))
        .await
        .unwrap();
    db.query_drop(format!("CALL sprig.mq_ack('{}')", token(&j1)))
        .await
        .unwrap();
    db.query_drop("COMMIT").await.unwrap();
    // A second acknowledgement of the same delivery: 39258, rolled back.
    db.query_drop("START TRANSACTION").await.unwrap();
    db.query_drop(format!("INSERT INTO {t} VALUES ('dup')"))
        .await
        .unwrap();
    let e = db
        .query_drop(format!("CALL sprig.mq_ack('{}')", token(&j1)))
        .await
        .unwrap_err();
    assert!(
        e.to_string().contains("39258") || e.to_string().contains("ALREADY_SETTLED"),
        "{e}"
    );
    let _ = db.query_drop("ROLLBACK").await;
    // job-2: the acknowledgement rolls back with the insert.
    db.query_drop("START TRANSACTION").await.unwrap();
    db.query_drop(format!("INSERT INTO {t} VALUES ('job-2')"))
        .await
        .unwrap();
    db.query_drop(format!("CALL sprig.mq_ack('{}')", token(&j2)))
        .await
        .unwrap();
    db.query_drop("ROLLBACK").await.unwrap();
    // The consumer dies without acknowledging anything.
    consumer_conn.close(200, "crash".into()).await.unwrap();
    // job-1 is gone for good; job-2 comes back, redelivered (after the
    // owner checked the held rows).
    let got = drain(&ch, &q, 2, Duration::from_secs(15)).await;
    assert_eq!(got, vec![("job-2".to_owned(), true)], "{got:?}");
    let rows: Vec<String> = db
        .query(format!("SELECT job FROM {t} ORDER BY job"))
        .await
        .unwrap();
    assert_eq!(rows, ["job-1"]);
    // mq_get from SQL, acknowledged in its own transaction.
    publish(&ch, &q, "job-3").await;
    let r: Vec<(String, String)> = db
        .query(format!("CALL sprig.mq_get('{q}', 10, 30000)"))
        .await
        .unwrap()
        .into_iter()
        .map(|row: mysql_async::Row| {
            let (tok, body): (String, String) = (row.get(0).unwrap(), row.get(1).unwrap());
            (tok, body)
        })
        .collect();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].1, "job-3");
    db.query_drop("START TRANSACTION").await.unwrap();
    db.query_drop(format!("INSERT INTO {t} VALUES ('job-3')"))
        .await
        .unwrap();
    db.query_drop(format!("CALL sprig.mq_ack('{}')", r[0].0))
        .await
        .unwrap();
    db.query_drop("COMMIT").await.unwrap();
    assert!(drain(&ch, &q, 1, Duration::from_secs(2)).await.is_empty());
    db.query_drop(format!("DROP TABLE {t}")).await.unwrap();
    ch.queue_delete(q.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
    eprintln!(
        "ack in a transaction: committed once, rolled back = redelivered, double ack = 39258"
    );
}
