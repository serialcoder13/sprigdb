//! `CALL sprig.mq_info(NULL [, vhost])` (ST66): every durable queue of a
//! vhost the user may read, in name order, with ready, unacknowledged and
//! consumers, in one call, as the single-queue form counts them.

#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use std::sync::Arc;

use common::{lapin, server, uniq};
use lapin::BasicProperties;
use lapin::options::{BasicGetOptions, BasicPublishOptions, QueueDeclareOptions};
use lapin::types::FieldTable;
use sprig_proto_amqp::auth::Principal;

fn principal() -> Arc<Principal> {
    Arc::new(Principal {
        tenant_id: 42,
        tenant: "acme".into(),
        user: "app".into(),
        user_id: Some(1),
        handle: None,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_queue_in_one_call() {
    let s = server().await;
    let Some(e) = s.engine.clone() else {
        return; // against a gateway: the e2e suite covers it
    };
    let conn = lapin(&s).await;
    let ch = conn.create_channel().await.unwrap();
    let base = uniq("info");
    let names = [
        format!("{base}-a"),
        format!("{base}-b"),
        format!("{base}-c"),
    ];
    for q in &names {
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
    }
    // a: 3 messages, one taken and not acknowledged; b: 1; c: none.
    for (q, n) in [(&names[0], 3), (&names[1], 1)] {
        for i in 0..n {
            ch.basic_publish(
                "".into(),
                q.as_str().into(),
                BasicPublishOptions::default(),
                format!("m{i}").as_bytes(),
                BasicProperties::default(),
            )
            .await
            .unwrap()
            .await
            .unwrap();
        }
    }
    let held = ch
        .basic_get(names[0].as_str().into(), BasicGetOptions::default())
        .await
        .unwrap();
    assert!(held.is_some());
    let v = e.sql_vhost(principal(), "/", 9).await.unwrap();
    let all = v.info_all().await.unwrap();
    let mine: Vec<_> = all
        .iter()
        .filter(|(n, _)| n.starts_with(&base))
        .map(|(n, i)| (n.clone(), i.ready, i.unacked, i.consumers))
        .collect();
    assert_eq!(
        mine,
        vec![
            (names[0].clone(), 2, 1, 0),
            (names[1].clone(), 1, 0, 0),
            (names[2].clone(), 0, 0, 0),
        ],
        "name order, the same counts as mq_info of each"
    );
    // The same as the single-queue form.
    for (n, i) in all.iter().filter(|(n, _)| n.starts_with(&base)) {
        assert_eq!(v.info(n).await.unwrap(), *i);
    }
    drop(held);
    conn.close(200, "bye".into()).await.unwrap();
}

/// bugfix-66 (R9 item 123): when the queue's owner can't answer (here
/// another gateway holds its lease and there's no relay between them),
/// `mq_info` reported "0 ready, 1 unacknowledged, 0 consumers" for a
/// message nobody had taken. Now it asks again, then counts the stored
/// messages as ready.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn info_when_the_owner_cannot_answer() {
    use sprig_queues::mem::MemLeases;
    if common::var("SPRIG_AMQP_ADDR").is_some() {
        return; // in process only
    }
    let catalog = common::mem_catalog();
    let table = MemLeases::new();
    let a = common::server_on(Arc::clone(&catalog), Arc::new(table.for_gateway("gw-a")), 1).await;
    let b = common::server_on(Arc::clone(&catalog), Arc::new(table.for_gateway("gw-b")), 2).await;
    let conn = lapin(&a).await;
    let ch = conn.create_channel().await.unwrap();
    let q = uniq("noanswer");
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
    ch.confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await
        .unwrap();
    ch.basic_publish(
        "".into(),
        q.as_str().into(),
        BasicPublishOptions::default(),
        b"one",
        BasicProperties::default(),
    )
    .await
    .unwrap()
    .await
    .unwrap();
    // On a: its owner answers.
    let va = a
        .engine
        .clone()
        .unwrap()
        .sql_vhost(principal(), "/", 9)
        .await
        .unwrap();
    let i = va.info(&q).await.unwrap();
    assert_eq!((i.ready, i.unacked, i.consumers), (1, 0, 0), "{i:?}");
    // On b: gw-a holds the owner's lease and b can't reach it.
    let vb = b
        .engine
        .clone()
        .unwrap()
        .sql_vhost(principal(), "/", 9)
        .await
        .unwrap();
    let i = vb.info(&q).await.unwrap();
    eprintln!("mq_info on the gateway whose owner can't answer: {i:?}");
    assert_eq!(
        (i.ready, i.unacked, i.consumers),
        (1, 0, 0),
        "a message nobody took isn't unacknowledged: {i:?}"
    );
    conn.close(200, "bye".into()).await.unwrap();
}
