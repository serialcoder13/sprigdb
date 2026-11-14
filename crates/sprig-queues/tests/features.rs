//! The engine's RabbitMQ features through `lapin`:
//! delayed messages, queue TTL, consumer timeouts, single active consumer,
//! consumer priorities, `reject-publish`, quorum delivery limits, and
//! privileges. In process by default; against a gateway with
//! `SPRIG_AMQP_ADDR` (the permission test needs the in-process engine's
//! read-only user and is skipped there).

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

mod common;

use std::time::{Duration, Instant};

use common::{lapin, lapin_to, server, uniq};
use futures::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicGetOptions, BasicPublishOptions, BasicQosOptions,
    BasicRejectOptions, ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions,
    QueueDeclareOptions, QueueDeleteOptions,
};
use lapin::types::{AMQPValue, FieldTable, ShortString};
use lapin::{BasicProperties, Channel, ExchangeKind};

fn args(kv: &[(&str, AMQPValue)]) -> FieldTable {
    let mut t = FieldTable::default();
    for (k, v) in kv {
        t.insert(ShortString::from(*k), v.clone());
    }
    t
}

fn s(v: &str) -> AMQPValue {
    AMQPValue::LongString(v.into())
}

async fn queue(ch: &Channel, q: &str, a: FieldTable) {
    ch.queue_declare(
        q.into(),
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        a,
    )
    .await
    .unwrap();
}

async fn send(ch: &Channel, x: &str, key: &str, body: &str, p: BasicProperties) -> bool {
    ch.basic_publish(
        x.into(),
        key.into(),
        BasicPublishOptions::default(),
        body.as_bytes(),
        p,
    )
    .await
    .unwrap()
    .await
    .unwrap()
    .is_ack()
}

async fn get(ch: &Channel, q: &str) -> Option<lapin::message::BasicGetMessage> {
    ch.basic_get(q.into(), BasicGetOptions::default())
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_messages_arrive_when_due() {
    let srv = server().await;
    let conn = lapin(&srv).await;
    let ch = conn.create_channel().await.unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    let x = uniq("delayed");
    let q = uniq("later");
    ch.exchange_declare(
        x.as_str().into(),
        ExchangeKind::Custom("x-delayed-message".into()),
        ExchangeDeclareOptions {
            durable: true,
            ..ExchangeDeclareOptions::default()
        },
        args(&[("x-delayed-type", s("direct"))]),
    )
    .await
    .unwrap();
    queue(&ch, &q, FieldTable::default()).await;
    ch.queue_bind(
        q.as_str().into(),
        x.as_str().into(),
        "k".into(),
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    let start = Instant::now();
    let delayed =
        BasicProperties::default().with_headers(args(&[("x-delay", AMQPValue::LongInt(600))]));
    assert!(send(&ch, &x, "k", "later", delayed).await);
    assert!(send(&ch, &x, "k", "now", BasicProperties::default()).await);
    let first = get(&ch, &q).await.expect("the undelayed message at once");
    assert_eq!(&*first.data, b"now");
    first.ack(BasicAckOptions::default()).await.unwrap();
    assert!(get(&ch, &q).await.is_none(), "not before it is due");
    let mut m = None;
    while start.elapsed() < Duration::from_secs(10) {
        if let Some(x) = get(&ch, &q).await {
            m = Some(x);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let m = m.expect("the delayed message arrives");
    let waited = start.elapsed();
    assert!(waited >= Duration::from_millis(600), "{waited:?}");
    assert_eq!(&*m.data, b"later");
    m.ack(BasicAckOptions::default()).await.unwrap();
    eprintln!("delayed 600 ms: arrived after {waited:?}");
    ch.queue_delete(q.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unused_queues_expire() {
    let srv = server().await;
    let conn = lapin(&srv).await;
    let ch = conn.create_channel().await.unwrap();
    let q = uniq("expires");
    queue(&ch, &q, args(&[("x-expires", AMQPValue::LongInt(400))])).await;
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let e = ch
        .queue_declare(
            q.as_str().into(),
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap_err();
    assert!(
        e.to_string().contains("NOT_FOUND") || e.to_string().contains("404"),
        "{e}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_timeout_cancels_and_requeues() {
    let srv = server().await;
    let conn = lapin(&srv).await;
    let ch = conn.create_channel().await.unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    let q = uniq("timeout");
    queue(
        &ch,
        &q,
        args(&[("x-consumer-timeout", AMQPValue::LongInt(500))]),
    )
    .await;
    assert!(send(&ch, "", &q, "slow", BasicProperties::default()).await);
    let c = conn.create_channel().await.unwrap();
    let mut consumer = c
        .basic_consume(
            q.as_str().into(),
            "slowpoke".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let d = tokio::time::timeout(Duration::from_secs(5), consumer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(&*d.data, b"slow");
    // Never acknowledged: the server cancels the consumer.
    let end = tokio::time::timeout(Duration::from_secs(5), consumer.next())
        .await
        .expect("the consumer is cancelled");
    assert!(end.is_none(), "{end:?}");
    let back = get(&ch, &q).await.expect("requeued");
    assert!(back.redelivered);
    back.ack(BasicAckOptions::default()).await.unwrap();
    ch.queue_delete(q.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_active_consumer_and_consumer_priorities() {
    let srv = server().await;
    let conn = lapin(&srv).await;
    let ch = conn.create_channel().await.unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    // Single active consumer: the second takes over when the first goes.
    let q = uniq("sac");
    queue(
        &ch,
        &q,
        args(&[("x-single-active-consumer", AMQPValue::Boolean(true))]),
    )
    .await;
    let c1 = conn.create_channel().await.unwrap();
    let c2 = conn.create_channel().await.unwrap();
    let mut first = c1
        .basic_consume(
            q.as_str().into(),
            "first".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let mut second = c2
        .basic_consume(
            q.as_str().into(),
            "second".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    for i in 0..3 {
        assert!(send(&ch, "", &q, &format!("s{i}"), BasicProperties::default()).await);
    }
    for i in 0..3 {
        let d = tokio::time::timeout(Duration::from_secs(5), first.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(d.data, format!("s{i}").into_bytes());
        if i < 2 {
            d.ack(BasicAckOptions::default()).await.unwrap();
        }
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(300), second.next())
            .await
            .is_err(),
        "the second consumer stays idle"
    );
    // The first goes away with s2 unacknowledged: the second gets it.
    c1.close(200, "bye".into()).await.unwrap();
    let d = tokio::time::timeout(Duration::from_secs(5), second.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(&*d.data, b"s2");
    assert!(d.redelivered);
    d.ack(BasicAckOptions::default()).await.unwrap();
    c2.close(200, "bye".into()).await.unwrap();
    // Consumer priorities: the higher one gets everything while it has
    // credit.
    let q = uniq("prio");
    queue(&ch, &q, FieldTable::default()).await;
    let lo = conn.create_channel().await.unwrap();
    let hi = conn.create_channel().await.unwrap();
    hi.basic_qos(100, BasicQosOptions::default()).await.unwrap();
    let mut low = lo
        .basic_consume(
            q.as_str().into(),
            "low".into(),
            BasicConsumeOptions::default(),
            args(&[("x-priority", AMQPValue::LongInt(1))]),
        )
        .await
        .unwrap();
    let mut high = hi
        .basic_consume(
            q.as_str().into(),
            "high".into(),
            BasicConsumeOptions::default(),
            args(&[("x-priority", AMQPValue::LongInt(10))]),
        )
        .await
        .unwrap();
    for i in 0..10 {
        assert!(send(&ch, "", &q, &format!("p{i}"), BasicProperties::default()).await);
    }
    for _ in 0..10 {
        let d = tokio::time::timeout(Duration::from_secs(5), high.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        d.ack(BasicAckOptions::default()).await.unwrap();
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(300), low.next())
            .await
            .is_err()
    );
    lo.close(200, "bye".into()).await.unwrap();
    hi.close(200, "bye".into()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reject_publish_and_quorum_delivery_limit() {
    let srv = server().await;
    let conn = lapin(&srv).await;
    let ch = conn.create_channel().await.unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    // reject-publish: the third message is nacked.
    let q = uniq("full");
    queue(
        &ch,
        &q,
        args(&[
            ("x-max-length", AMQPValue::LongInt(2)),
            ("x-overflow", s("reject-publish")),
        ]),
    )
    .await;
    assert!(send(&ch, "", &q, "a", BasicProperties::default()).await);
    assert!(send(&ch, "", &q, "b", BasicProperties::default()).await);
    assert!(
        !send(&ch, "", &q, "c", BasicProperties::default()).await,
        "nacked"
    );
    ch.queue_delete(q.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
    // A quorum queue with a delivery limit of 1 dead-letters the message
    // on its second failed delivery.
    let q = uniq("quorum");
    let dlq = uniq("poison");
    queue(&ch, &dlq, FieldTable::default()).await;
    queue(
        &ch,
        &q,
        args(&[
            ("x-queue-type", s("quorum")),
            ("x-delivery-limit", AMQPValue::LongInt(1)),
            ("x-dead-letter-exchange", s("")),
            ("x-dead-letter-routing-key", s(&dlq)),
        ]),
    )
    .await;
    assert!(send(&ch, "", &q, "poison", BasicProperties::default()).await);
    for attempt in 0..2 {
        let m = get(&ch, &q).await.expect("delivered");
        if attempt == 1 {
            let count = m
                .properties
                .headers()
                .as_ref()
                .and_then(|h| h.inner().get("x-delivery-count").cloned());
            assert!(count.is_some(), "quorum queues count failed deliveries");
        }
        m.reject(BasicRejectOptions { requeue: true })
            .await
            .unwrap();
    }
    let mut dead = None;
    for _ in 0..50 {
        dead = get(&ch, &dlq).await;
        if dead.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let dead = dead.expect("dead-lettered at the limit");
    assert_eq!(&*dead.data, b"poison");
    dead.ack(BasicAckOptions::default()).await.unwrap();
    assert!(get(&ch, &q).await.is_none());
    for x in [&q, &dlq] {
        ch.queue_delete(x.as_str().into(), QueueDeleteOptions::default())
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn privileges_are_checked() {
    let srv = server().await;
    if srv.catalog.is_none() {
        return; // the gateway's grants: see the live notes
    }
    let ro = lapin_to(&srv, "acme.ro", "secret").await;
    let ch = ro.create_channel().await.unwrap();
    let e = ch
        .queue_declare(
            uniq("ro").as_str().into(),
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap_err();
    assert!(
        e.to_string().contains("ACCESS_REFUSED") || e.to_string().contains("403"),
        "{e}"
    );
    // Reading is allowed: a queue the app user made.
    let app = lapin(&srv).await;
    let a = app.create_channel().await.unwrap();
    let q = uniq("shared");
    queue(&a, &q, FieldTable::default()).await;
    let ch = ro.create_channel().await.unwrap();
    assert!(get(&ch, &q).await.is_none());
    // Publishing is not.
    ch.basic_publish(
        "".into(),
        q.as_str().into(),
        BasicPublishOptions::default(),
        b"x",
        BasicProperties::default(),
    )
    .await
    .unwrap();
    let e = ch
        .basic_get(q.as_str().into(), BasicGetOptions::default())
        .await
        .unwrap_err();
    assert!(
        e.to_string().contains("ACCESS_REFUSED") || e.to_string().contains("403"),
        "{e}"
    );
}

/// Live only: privilege grants on a gateway. `SPRIG_AMQP_RO_USER` holds
/// `SELECT, DELETE ON mq.*` only; `SPRIG_AMQP_NONE_USER` nothing (both
/// with `SPRIG_AMQP_RO_PASSWORD`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn privileges_on_a_gateway() {
    let (Some(ro_user), Some(none_user), Some(pass)) = (
        common::var("SPRIG_AMQP_RO_USER"),
        common::var("SPRIG_AMQP_NONE_USER"),
        common::var("SPRIG_AMQP_RO_PASSWORD"),
    ) else {
        return;
    };
    let srv = server().await;
    // No right on the vhost's database: the vhost can't be opened.
    let none = lapin::Connection::connect(
        &format!("amqp://{none_user}:{pass}@{}/%2f", srv.addr),
        lapin::ConnectionProperties::default(),
    )
    .await;
    assert!(none.is_err(), "a user without rights opened the vhost");
    let app = lapin(&srv).await;
    let a = app.create_channel().await.unwrap();
    a.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    let q = uniq("granted");
    queue(&a, &q, FieldTable::default()).await;
    assert!(send(&a, "", &q, "for-ro", BasicProperties::default()).await);
    let ro = lapin_to(&srv, &ro_user, &pass).await;
    // Read (SELECT and DELETE on the database): get and ack work.
    let ch = ro.create_channel().await.unwrap();
    let m = get(&ch, &q).await.expect("readable");
    assert_eq!(&*m.data, b"for-ro");
    m.ack(BasicAckOptions::default()).await.unwrap();
    // Configure (CREATE): refused.
    let e = ch
        .queue_declare(
            uniq("ro-made").as_str().into(),
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap_err();
    assert!(
        e.to_string().contains("ACCESS_REFUSED") || e.to_string().contains("403"),
        "{e}"
    );
    // Write (INSERT): refused.
    let ch = ro.create_channel().await.unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    let r = ch
        .basic_publish(
            "".into(),
            q.as_str().into(),
            BasicPublishOptions::default(),
            b"x",
            BasicProperties::default(),
        )
        .await;
    let refused = match r {
        Err(e) => e.to_string(),
        Ok(c) => match c.await {
            Err(e) => e.to_string(),
            Ok(c) => format!("{c:?}"),
        },
    };
    assert!(
        refused.contains("ACCESS_REFUSED") || refused.contains("403"),
        "{refused}"
    );
    a.queue_delete(q.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
    eprintln!("grants: no rights = no vhost; SELECT+DELETE = consume only");
}
