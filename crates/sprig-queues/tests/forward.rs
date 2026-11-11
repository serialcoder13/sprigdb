//! The feed-driven forwarder's engine side (chapter 50, "Publishing inside
//! a database transaction"; R9, r9fwd), in process:
//!
//! - `SqlVhost::forward_outbox` enqueues outbox rows in the order it is
//!   given (the feed's commit order), not the table's (insert order);
//! - the forward ledger keeps a replayed forward a no-op after the
//!   message was consumed (a forwarder that crashed between the enqueue
//!   and the delete);
//! - with `queue.forwarder = feed` the polling forwarder leaves young
//!   transactional publishes to the feed and takes them only after
//!   `queue.forward_fallback_ms`; with `poll` it takes them at once.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    missing_docs
)]

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{lapin, server_with, uniq};
use lapin::options::{BasicAckOptions, BasicGetOptions, QueueDeclareOptions};
use lapin::types::FieldTable;
use sprig_proto_amqp::auth::Principal;
use sprig_queues::QueueEngine;
use sprig_queues::settings::QueueSettings;
use sprig_queues::sql::{SqlVhost, TxnWrite, message};
use sprig_queues::store::{MsgId, OutboxRow};

fn principal() -> Arc<Principal> {
    Arc::new(Principal {
        tenant_id: 42,
        tenant: "acme".into(),
        user: "app".into(),
        user_id: Some(1),
        handle: None,
    })
}

async fn vhost(e: &Arc<QueueEngine>) -> SqlVhost {
    e.sql_vhost(principal(), "/", 7).await.unwrap()
}

/// An outbox row as a transactional publish of `body` to queue `q` makes.
async fn row(v: &SqlVhost, q: &str, body: &str) -> OutboxRow {
    let m = message("", q, body.as_bytes().to_vec(), None).unwrap();
    match v.publish_in_txn(m, true).await.unwrap() {
        TxnWrite::Outbox(r, n) => {
            assert_eq!(n, 1);
            r
        }
        other => panic!("not an outbox row: {other:?}"),
    }
}

async fn drain(ch: &lapin::Channel, q: &str) -> Vec<String> {
    let mut out = Vec::new();
    while let Some(m) = ch
        .basic_get(q.into(), BasicGetOptions::default())
        .await
        .unwrap()
    {
        out.push(String::from_utf8_lossy(&m.delivery.data).into_owned());
        m.delivery.ack(BasicAckOptions::default()).await.unwrap();
    }
    out
}

async fn declare(ch: &lapin::Channel, q: &str) {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwards_in_the_order_given_and_never_twice() {
    let s = server_with(QueueSettings {
        forward_feed: true,
        forward_fallback_ms: 600_000,
        ..QueueSettings::default()
    })
    .await;
    let e = Arc::clone(s.engine.as_ref().unwrap());
    let conn = lapin(&s).await;
    let ch = conn.create_channel().await.unwrap();
    let q = uniq("fwd-order");
    declare(&ch, &q).await;
    let v = vhost(&e).await;
    v.ensure_store().await.unwrap();
    let store = s.catalog.as_ref().unwrap().store_of(42, "mq").unwrap();
    // 40 rows inserted in order 0..40 (their ids rise with it); the
    // "commit order" is a shuffle of them.
    let mut rows = Vec::new();
    for i in 0..40 {
        rows.push(row(&v, &q, &format!("m{i}")).await);
    }
    store.put_outbox(rows.clone());
    let mut order: Vec<usize> = (0..40).collect();
    // A fixed shuffle: odd ones first, backwards, then the even ones.
    order.sort_by_key(|i| (i % 2 == 0, std::cmp::Reverse(*i)));
    let ids: Vec<MsgId> = order.iter().map(|i| rows[*i].out_id).collect();
    let (fwd, gone) = v.forward_outbox(&ids[..25]).await.unwrap();
    assert_eq!((fwd, gone), (25, 0));
    let (fwd, gone) = v.forward_outbox(&ids[25..]).await.unwrap();
    assert_eq!((fwd, gone), (15, 0));
    assert_eq!(store.outbox_len(), 0, "forwarded rows are deleted");
    let got = drain(&ch, &q).await;
    let want: Vec<String> = order.iter().map(|i| format!("m{i}")).collect();
    assert_eq!(got, want, "enqueued in the order given (commit order)");

    // A forwarder that crashed after its enqueue and before its delete:
    // the row is back, its message consumed meanwhile. Forwarding it again
    // adds nothing (the forward ledger), and the row is deleted.
    store.put_outbox(vec![rows[3].clone(), rows[4].clone()]);
    let (fwd, gone) = v
        .forward_outbox(&[rows[3].out_id, rows[4].out_id, [9; 16]])
        .await
        .unwrap();
    assert_eq!((fwd, gone), (2, 1), "two still there, one unknown");
    assert_eq!(store.outbox_len(), 0);
    assert!(drain(&ch, &q).await.is_empty(), "no message twice");

    // The ledger keeps rows while their outbox row exists or they are
    // young; a purge before now + 1 s takes the 40 forwarded ones.
    let n = sprig_queues::store::MsgStore::purge_ledger(&*store, i64::MAX)
        .await
        .unwrap();
    assert_eq!(n, 40);
    conn.close(0, "done".into()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_poller_leaves_young_publishes_to_the_feed() {
    let s = server_with(QueueSettings {
        forward_feed: true,
        forward_fallback_ms: 1_200,
        ..QueueSettings::default()
    })
    .await;
    let e = Arc::clone(s.engine.as_ref().unwrap());
    let conn = lapin(&s).await;
    let ch = conn.create_channel().await.unwrap();
    let q = uniq("fwd-fallback");
    declare(&ch, &q).await;
    let v = vhost(&e).await;
    v.ensure_store().await.unwrap();
    let store = s.catalog.as_ref().unwrap().store_of(42, "mq").unwrap();
    let start = Instant::now();
    store.put_outbox(vec![row(&v, &q, "late").await]);
    v.committed();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(store.outbox_len(), 1, "a young publish waits for the feed");
    // The fallback: forwarded by the poller once older than 1.2 s.
    let mut got = Vec::new();
    while got.is_empty() && start.elapsed() < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        got = drain(&ch, &q).await;
    }
    assert_eq!(got, ["late"]);
    let waited = start.elapsed();
    assert!(
        waited >= Duration::from_millis(1_200),
        "fallback after the delay, not before ({waited:?})"
    );
    conn.close(0, "done".into()).await.unwrap();

    // `poll`: the old behaviour, at once.
    let s = server_with(QueueSettings {
        forward_feed: false,
        ..QueueSettings::default()
    })
    .await;
    let e = Arc::clone(s.engine.as_ref().unwrap());
    let conn = lapin(&s).await;
    let ch = conn.create_channel().await.unwrap();
    declare(&ch, &q).await;
    let v = vhost(&e).await;
    v.ensure_store().await.unwrap();
    let store = s.catalog.as_ref().unwrap().store_of(42, "mq").unwrap();
    let start = Instant::now();
    store.put_outbox(vec![row(&v, &q, "now").await]);
    v.committed();
    let mut got = Vec::new();
    while got.is_empty() && start.elapsed() < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(20)).await;
        got = drain(&ch, &q).await;
    }
    assert_eq!(got, ["now"]);
    assert!(start.elapsed() < Duration::from_millis(1_000));
    conn.close(0, "done".into()).await.unwrap();
}
