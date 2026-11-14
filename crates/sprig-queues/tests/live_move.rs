//! A queue's tenant moves between clusters under publish and consume load
//! (a shard move and the mover's cutover). The mover renames the source tables at its fence, so the
//! queue owner's next write fails, its store is marked stale, and the
//! next owner opens the store where the tenant's map points once the
//! hold is released.
//!
//! ```text
//! SPRIG_TEST_QUEUE_MOVE=1 SPRIG_ETCD=http://127.0.0.1:43379 \
//! SPRIG_AMQP_ADDR=127.0.0.1:43672 SPRIG_AMQP_ADDR2=127.0.0.1:43682 \
//! SPRIG_AMQP_USER=acme.app SPRIG_AMQP_PASSWORD=sprig-dev-app \
//! SPRIG_MOVE_TENANT=9430600000 SPRIG_MOVE_ROUTE=dc-a-c1,dc-a-c2,dc-a-c1 \
//! cargo test --release -p sprig-queues --test live_move -- --nocapture
//! ```
//!
//! A publisher on one gateway republishes what isn't confirmed (a new
//! message id per attempt); a consumer on the other acknowledges. After
//! the moves: every confirmed message was delivered, and no message id was
//! delivered twice without `redelivered`.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::similar_names,
    clippy::many_single_char_names,
    clippy::doc_markdown,
    clippy::cast_possible_truncation,
    missing_docs
)]

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions, QueueDeclareOptions, QueueDeleteOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Connection, ConnectionProperties};
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::CancellationToken;
use sprig_common::ids::{ClusterId, DcId, TenantId};
use sprig_common::time::SystemClock;
use sprig_meta::{EtcdConfig, EtcdStore, MetaStore, Tier};
use sprig_mover::mysql::MysqlEnvFactory;
use sprig_mover::{
    JobRuntime, JobSpec, JobState, JobStore, MetaMover, Mover, MoverSettings, WorkerOpts,
};
use sprig_shard::TenantMaps;

fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

async fn connect(addr: &str) -> Connection {
    let (u, p) = (
        var("SPRIG_AMQP_USER").unwrap(),
        var("SPRIG_AMQP_PASSWORD").unwrap(),
    );
    for _ in 0..240 {
        if let Ok(c) = Connection::connect(
            &format!("amqp://{u}:{p}@{addr}/%2f"),
            ConnectionProperties::default(),
        )
        .await
        {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("can't connect to {addr}");
}

#[derive(Default)]
struct Seen {
    by_id: HashMap<String, Vec<bool>>,
    bodies: HashSet<String>,
    deliveries: u64,
    redelivered: u64,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_queue_moves_with_its_tenant_under_load() {
    if var("SPRIG_TEST_QUEUE_MOVE").is_none() {
        eprintln!("skipped: set SPRIG_TEST_QUEUE_MOVE=1 (see the module docs)");
        return;
    }
    let etcd = var("SPRIG_ETCD").unwrap();
    let (a, b) = (
        var("SPRIG_AMQP_ADDR").unwrap(),
        var("SPRIG_AMQP_ADDR2").unwrap(),
    );
    let tenant = TenantId(var("SPRIG_MOVE_TENANT").unwrap().parse().unwrap());
    let route: Vec<String> = var("SPRIG_MOVE_ROUTE")
        .unwrap_or_else(|| "dc-a-c1,dc-a-c2,dc-a-c1".into())
        .split(',')
        .map(str::to_owned)
        .collect();
    let cfg = EtcdConfig::new(vec![etcd]);
    let dc: Arc<dyn MetaStore> = Arc::new(
        EtcdStore::connect(Tier::Dc(DcId::new("dc-a").unwrap()), &cfg)
            .await
            .unwrap(),
    );
    let global: Arc<dyn MetaStore> =
        Arc::new(EtcdStore::connect(Tier::Global, &cfg).await.unwrap());
    let maps = TenantMaps::new(dc.clone());
    let settings = MoverSettings {
        copy_chunk_rows: 1000,
        ready_sustain: Duration::from_secs(3),
        cutover_retry: Duration::from_secs(2),
        source_retain: Duration::from_secs(3),
        ..MoverSettings::default()
    };
    let registry = ClusterRegistry::new(global, dc.clone()).unwrap();
    let factory = Arc::new(MysqlEnvFactory::new(registry, maps, settings));
    let store = JobStore::new(dc.clone(), Arc::new(SystemClock));
    let mut opts = WorkerOpts::new("r9queues2-move-test");
    opts.scan_interval = Duration::from_millis(200);
    let stop_worker = CancellationToken::new();
    let worker =
        tokio::spawn(JobRuntime::new(store.clone(), factory, opts).run(stop_worker.clone()));
    let mover = MetaMover::new(store);

    let q = format!(
        "r9queues2_move_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    {
        let c = connect(&a).await;
        let ch = c.create_channel().await.unwrap();
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
        let _ = c.close(200, "bye".into()).await;
    }
    let stop = CancellationToken::new();
    let confirmed = Arc::new(Mutex::new(HashSet::<u64>::new()));
    let nacks = Arc::new(Mutex::new(0u64));
    // The publisher, on gateway 1.
    let publisher = {
        let (q, a, stop) = (q.clone(), a.clone(), stop.clone());
        let (confirmed, nacks) = (Arc::clone(&confirmed), Arc::clone(&nacks));
        tokio::spawn(async move {
            let mut conn = connect(&a).await;
            let mut ch = conn.create_channel().await.unwrap();
            ch.confirm_select(ConfirmSelectOptions::default())
                .await
                .unwrap();
            let (mut n, mut attempt) = (0u64, 0u64);
            while !stop.is_cancelled() {
                attempt += 1;
                let ok = match ch
                    .basic_publish(
                        "".into(),
                        q.as_str().into(),
                        BasicPublishOptions::default(),
                        n.to_string().as_bytes(),
                        BasicProperties::default()
                            .with_delivery_mode(2)
                            .with_message_id(format!("{n}.{attempt}").into()),
                    )
                    .await
                {
                    Ok(c) => matches!(c.await, Ok(c) if c.is_ack()),
                    Err(_) => false,
                };
                if ok {
                    confirmed.lock().unwrap().insert(n);
                    n += 1;
                    continue;
                }
                *nacks.lock().unwrap() += 1;
                tokio::time::sleep(Duration::from_millis(100)).await;
                if !ch.status().connected() {
                    if !conn.status().connected() {
                        conn = connect(&a).await;
                    }
                    if let Ok(c) = conn.create_channel().await {
                        ch = c;
                        let _ = ch.confirm_select(ConfirmSelectOptions::default()).await;
                    }
                }
            }
            let _ = conn.close(200, "bye".into()).await;
        })
    };
    // The consumer, on gateway 2.
    let seen = Arc::new(Mutex::new(Seen::default()));
    let consumer = {
        let (q, b, stop, seen) = (q.clone(), b.clone(), stop.clone(), Arc::clone(&seen));
        tokio::spawn(async move {
            'outer: while !stop.is_cancelled() {
                let conn = connect(&b).await;
                let Ok(ch) = conn.create_channel().await else {
                    continue;
                };
                let _ = ch.basic_qos(50, BasicQosOptions::default()).await;
                let Ok(mut cons) = ch
                    .basic_consume(
                        q.as_str().into(),
                        "c".into(),
                        BasicConsumeOptions::default(),
                        FieldTable::default(),
                    )
                    .await
                else {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                };
                loop {
                    let next = tokio::select! {
                        () = stop.cancelled() => break 'outer,
                        n = tokio::time::timeout(Duration::from_secs(5), cons.next()) => n,
                    };
                    match next {
                        Ok(Some(Ok(d))) => {
                            {
                                let mut s = seen.lock().unwrap();
                                s.deliveries += 1;
                                if d.redelivered {
                                    s.redelivered += 1;
                                }
                                let mid = d
                                    .properties
                                    .message_id()
                                    .as_ref()
                                    .map(ToString::to_string)
                                    .unwrap_or_default();
                                s.by_id.entry(mid).or_default().push(d.redelivered);
                                s.bodies
                                    .insert(String::from_utf8_lossy(&d.data).into_owned());
                            }
                            if d.ack(BasicAckOptions::default()).await.is_err() {
                                break;
                            }
                        }
                        Ok(Some(Err(_)) | None) => break,
                        Err(_) => {}
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
    };
    tokio::time::sleep(Duration::from_secs(5)).await;
    let mut report = Vec::new();
    for pair in route.windows(2) {
        let (from, to) = (&pair[0], &pair[1]);
        let c0 = confirmed.lock().unwrap().len();
        let t0 = Instant::now();
        let id = mover
            .submit(JobSpec::move_tenant(
                tenant,
                ClusterId::new(from).unwrap(),
                ClusterId::new(to).unwrap(),
            ))
            .await
            .unwrap();
        let st = loop {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let Ok(st) = mover.status(&id).await else {
                continue;
            };
            if st.state.is_terminal() || st.state == JobState::Paused {
                break st;
            }
            assert!(
                t0.elapsed() < Duration::from_secs(600),
                "{from} → {to} stuck: {st:?}"
            );
        };
        assert_eq!(st.state, JobState::Done, "{from} → {to}: {st:?}");
        let line = format!(
            "{from} → {to}: {:.1} s, holds {:?} ms, {} messages confirmed meanwhile, {} nacks so far",
            t0.elapsed().as_secs_f64(),
            st.progress.holds_ms,
            confirmed.lock().unwrap().len() - c0,
            nacks.lock().unwrap()
        );
        eprintln!("{line}");
        report.push(line);
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    // Stop publishing; give the consumer time to drain.
    let total = {
        stop.cancel();
        let _ = publisher.await;
        confirmed.lock().unwrap().clone()
    };
    // A fresh consumer drains what is left.
    let deadline = Instant::now() + Duration::from_secs(120);
    let _ = consumer.await;
    {
        let conn = connect(&b).await;
        let ch = conn.create_channel().await.unwrap();
        ch.basic_qos(200, BasicQosOptions::default()).await.unwrap();
        let mut cons = ch
            .basic_consume(
                q.as_str().into(),
                "drain".into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        while Instant::now() < deadline {
            let missing = {
                let s = seen.lock().unwrap();
                total.iter().any(|n| !s.bodies.contains(&n.to_string()))
            };
            if !missing {
                break;
            }
            if let Ok(Some(Ok(d))) = tokio::time::timeout(Duration::from_secs(5), cons.next()).await
            {
                {
                    let mut s = seen.lock().unwrap();
                    s.deliveries += 1;
                    if d.redelivered {
                        s.redelivered += 1;
                    }
                    let mid = d
                        .properties
                        .message_id()
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_default();
                    s.by_id.entry(mid).or_default().push(d.redelivered);
                    s.bodies
                        .insert(String::from_utf8_lossy(&d.data).into_owned());
                }
                d.ack(BasicAckOptions::default()).await.unwrap();
            }
        }
        let _ = ch
            .queue_delete(q.as_str().into(), QueueDeleteOptions::default())
            .await;
        let _ = conn.close(200, "bye".into()).await;
    }
    stop_worker.cancel();
    let _ = worker.await;
    let s = seen.lock().unwrap();
    let missing: Vec<u64> = total
        .iter()
        .filter(|n| !s.bodies.contains(&n.to_string()))
        .copied()
        .collect();
    eprintln!(
        "{} confirmed, {} deliveries ({} redelivered), {} nacks (republished)",
        total.len(),
        s.deliveries,
        s.redelivered,
        nacks.lock().unwrap()
    );
    for l in &report {
        eprintln!("  {l}");
    }
    assert!(missing.is_empty(), "confirmed and lost: {missing:?}");
    for (mid, flags) in &s.by_id {
        let fresh = flags.iter().filter(|r| !**r).count();
        assert!(fresh <= 1, "{mid} delivered fresh {fresh} times: {flags:?}");
    }
}
