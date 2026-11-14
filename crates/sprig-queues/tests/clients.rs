//! Independent clients against the queue engine (test-only
//! crates): `lapin` speaks AMQP 0-9-1, `fe2o3-amqp`
//! AMQP 1.0. Neither shares code with `sprig-proto-amqp`, so a misreading
//! of the specs on both sides of the in-house client can't hide here.
//!
//! By default the server runs in process over the engine with the
//! in-memory catalog; with `SPRIG_AMQP_ADDR` (and `SPRIG_AMQP_USER`,
//! `SPRIG_AMQP_PASSWORD`) the same tests run against a gateway, whose
//! engine stores messages in MariaDB.

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

use std::time::Duration;

use common::{lapin_to, uniq};
use futures::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicGetOptions, BasicNackOptions, BasicPublishOptions,
    BasicQosOptions, ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions,
    QueueDeclareOptions, QueueDeleteOptions,
};
use lapin::types::{AMQPValue, FieldTable, ShortString};
use lapin::{BasicProperties, Connection, ExchangeKind};

async fn lapin_conn() -> Connection {
    common::lapin(&common::server().await).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lapin_work_queue_confirms_acks_and_redelivery() {
    let conn = lapin_conn().await;
    let ch = conn.create_channel().await.unwrap();
    let q = uniq("work");
    ch.queue_declare(
        q.as_str().into(),
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    for i in 0..50u32 {
        let c = ch
            .basic_publish(
                "".into(),
                q.as_str().into(),
                BasicPublishOptions::default(),
                format!("job-{i}").as_bytes(),
                BasicProperties::default().with_delivery_mode(2),
            )
            .await
            .unwrap()
            .await
            .unwrap();
        assert!(c.is_ack(), "publish {i} not confirmed: {c:?}");
    }
    // A get, nacked with requeue: comes back redelivered, first.
    let g = ch
        .basic_get(q.as_str().into(), BasicGetOptions::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&*g.data, b"job-0");
    assert!(!g.redelivered);
    g.nack(BasicNackOptions {
        requeue: true,
        ..BasicNackOptions::default()
    })
    .await
    .unwrap();
    let c2 = conn.create_channel().await.unwrap();
    c2.basic_qos(10, BasicQosOptions::default()).await.unwrap();
    let mut consumer = c2
        .basic_consume(
            q.as_str().into(),
            "c1".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let mut seen = Vec::new();
    while seen.len() < 50 {
        let d = tokio::time::timeout(Duration::from_secs(10), consumer.next())
            .await
            .expect("delivery in time")
            .unwrap()
            .unwrap();
        if seen.is_empty() {
            assert_eq!(&*d.data, b"job-0");
            assert!(d.redelivered, "the requeued job comes back redelivered");
        }
        seen.push(String::from_utf8(d.data.clone()).unwrap());
        d.ack(BasicAckOptions::default()).await.unwrap();
    }
    let want: Vec<String> = (0..50).map(|i| format!("job-{i}")).collect();
    assert_eq!(seen, want, "FIFO order");
    c2.close(200, "bye".into()).await.unwrap();
    let n = ch
        .queue_delete(q.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
    assert_eq!(n, 0, "every message acknowledged");
    conn.close(200, "bye".into()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lapin_topic_routing_and_dead_letters() {
    let conn = lapin_conn().await;
    let ch = conn.create_channel().await.unwrap();
    let x = uniq("orders");
    let dlx = uniq("dlx");
    let billing = uniq("billing");
    let dead = uniq("dead");
    ch.exchange_declare(
        x.as_str().into(),
        ExchangeKind::Topic,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.exchange_declare(
        dlx.as_str().into(),
        ExchangeKind::Fanout,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    let mut args = FieldTable::default();
    args.insert(
        ShortString::from("x-dead-letter-exchange"),
        AMQPValue::LongString(dlx.as_str().into()),
    );
    ch.queue_declare(
        billing.as_str().into(),
        QueueDeclareOptions::default(),
        args,
    )
    .await
    .unwrap();
    ch.queue_declare(
        dead.as_str().into(),
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_bind(
        billing.as_str().into(),
        x.as_str().into(),
        "order.paid.*".into(),
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_bind(
        dead.as_str().into(),
        dlx.as_str().into(),
        "".into(),
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    for key in ["order.paid.eu", "order.shipped.eu", "order.paid.us"] {
        ch.basic_publish(
            x.as_str().into(),
            key.into(),
            BasicPublishOptions::default(),
            key.as_bytes(),
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    }
    // Two routed; reject one without requeue: it is dead-lettered.
    let a = ch
        .basic_get(billing.as_str().into(), BasicGetOptions::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&*a.data, b"order.paid.eu");
    a.nack(BasicNackOptions {
        requeue: false,
        ..BasicNackOptions::default()
    })
    .await
    .unwrap();
    let b = ch
        .basic_get(billing.as_str().into(), BasicGetOptions::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&*b.data, b"order.paid.us");
    b.ack(BasicAckOptions::default()).await.unwrap();
    let mut d = None;
    for _ in 0..50 {
        d = ch
            .basic_get(dead.as_str().into(), BasicGetOptions::default())
            .await
            .unwrap();
        if d.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let d = d.expect("the dead letter arrives");
    assert_eq!(&*d.data, b"order.paid.eu");
    let deaths = d
        .properties
        .headers()
        .as_ref()
        .and_then(|h| h.inner().get("x-death").cloned())
        .expect("x-death header");
    let AMQPValue::FieldArray(a) = deaths else {
        panic!("x-death is an array")
    };
    let AMQPValue::FieldTable(first) = &a.as_slice()[0] else {
        panic!("x-death entries are tables")
    };
    assert_eq!(
        first.inner().get("reason"),
        Some(&AMQPValue::LongString("rejected".into()))
    );
    d.ack(BasicAckOptions::default()).await.unwrap();
    for q in [&billing, &dead] {
        ch.queue_delete(q.as_str().into(), QueueDeleteOptions::default())
            .await
            .unwrap();
    }
    for e in [&x, &dlx] {
        ch.exchange_delete(
            e.as_str().into(),
            lapin::options::ExchangeDeleteOptions::default(),
        )
        .await
        .unwrap();
    }
    conn.close(200, "bye".into()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fe2o3_amqp10_send_receive_release() {
    use fe2o3_amqp::{Connection as Conn10, Receiver, Sender, Session};
    let srv = common::server().await;
    let (addr, user, pass) = (srv.addr.clone(), srv.user.clone(), srv.pass.clone());
    // A queue to use, declared over 0-9-1.
    let q = uniq("amqp10");
    let conn = lapin_to(&srv, &user, &pass).await;
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare(
        q.as_str().into(),
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
    let mut c = Conn10::open(
        "r9queues-fe2o3",
        format!("amqp://{user}:{pass}@{addr}").as_str(),
    )
    .await
    .unwrap();
    let mut s = Session::begin(&mut c).await.unwrap();
    let addr10 = format!("/queues/{q}");
    let mut sender = Sender::attach(&mut s, "r9queues-sender", addr10.as_str())
        .await
        .unwrap();
    for i in 0..5 {
        let outcome = sender.send(format!("m{i}")).await.unwrap();
        assert!(outcome.is_accepted(), "{outcome:?}");
    }
    let mut receiver = Receiver::attach(&mut s, "r9queues-receiver", addr10.as_str())
        .await
        .unwrap();
    let d: fe2o3_amqp::link::delivery::Delivery<String> = receiver.recv().await.unwrap();
    assert_eq!(d.body(), "m0");
    receiver.release(&d).await.unwrap();
    let mut got = Vec::new();
    while got.len() < 5 {
        let d: fe2o3_amqp::link::delivery::Delivery<String> =
            tokio::time::timeout(Duration::from_secs(10), receiver.recv())
                .await
                .expect("delivery in time")
                .unwrap();
        got.push(d.body().clone());
        receiver.accept(&d).await.unwrap();
    }
    // The released m0 comes back after what the link's credit had already
    // taken (m1 to m4 were in flight), as with RabbitMQ.
    got.sort();
    assert_eq!(got, ["m0", "m1", "m2", "m3", "m4"]);
    receiver.close().await.unwrap();
    sender.close().await.unwrap();
    s.end().await.unwrap();
    c.close().await.unwrap();
    let n = ch
        .queue_delete(q.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
    assert_eq!(n, 0);
    conn.close(200, "bye".into()).await.unwrap();
}
