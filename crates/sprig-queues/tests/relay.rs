//! Queues across gateways in one process (chapter 50, "The queue owner";
//! the `QueuePeer` relay): several engines share the in-memory catalog,
//! leases, temporary-queue registry and placement, and reach each other's
//! owners through [`MemPeers`]. Real AMQP clients (`lapin`) connect to
//! each gateway's front end.
//!
//! - publish on one gateway, consume, get, purge and delete on another;
//! - clients on gateways that don't own the queue while its owner is
//!   killed mid-stream: nothing confirmed is lost, nothing is delivered
//!   twice without `redelivered`;
//! - direct reply-to and exclusive reply queues across gateways;
//! - the owner follows its consumers to their gateway, without
//!   redelivering what they hold.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::similar_names,
    clippy::many_single_char_names,
    clippy::doc_markdown,
    clippy::match_same_arms,
    clippy::cast_possible_truncation,
    unreachable_pub,
    missing_docs
)]

mod common;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicGetOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions, QueueDeclareOptions, QueueDeleteOptions, QueuePurgeOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
use sprig_common::CancellationToken;
use sprig_proto_amqp::auth::{AuthFailure, ConnInfo, Credentials, Identity, Principal};
use sprig_proto_amqp::message::Message;
use sprig_proto_amqp::reply_to::{DirectReplyTo, ReplyRelay};
use sprig_proto_amqp::server::{Config, Services, serve};
use sprig_queues::engine::{EngineDeps, QueueEngine};
use sprig_queues::mem::{MemCatalog, MemLeases, MemTemps};
use sprig_queues::owner::SystemClock;
use sprig_queues::relay::{MemPeers, MemPlacement};
use sprig_queues::settings::QueueSettings;
use tokio::net::TcpListener;

struct TestIdentity;

#[async_trait]
impl Identity for TestIdentity {
    async fn login(&self, _c: &ConnInfo, creds: Credentials) -> Result<Principal, AuthFailure> {
        let Credentials::Password { user, password, .. } = creds else {
            return Err(AuthFailure("no".into()));
        };
        if user != "acme.app" || password != b"secret" {
            return Err(AuthFailure("bad credentials".into()));
        }
        Ok(Principal {
            tenant_id: 42,
            tenant: "acme".into(),
            user: "app".into(),
            user_id: Some(1),
            handle: None,
        })
    }
}

#[derive(Default)]
struct Hop(Mutex<HashMap<u32, Arc<DirectReplyTo>>>);

impl ReplyRelay for Hop {
    fn relay(&self, slot: u32, name: &str, message: Message) {
        let to = self.0.lock().unwrap().get(&slot).cloned();
        if let Some(r) = to {
            // The relaying gateway checked the replier's tenant already.
            let _ = r.deliver_relayed(name, message);
        }
    }
}

/// The shared footing of the simulated gateways.
struct Net {
    catalog: Arc<MemCatalog>,
    leases: MemLeases,
    peers: MemPeers,
    placement: MemPlacement,
    temps: MemTemps,
    hop: Arc<Hop>,
}

struct Gw {
    id: String,
    addr: String,
    engine: Arc<QueueEngine>,
    close: CancellationToken,
    accept: tokio::task::JoinHandle<()>,
}

impl Net {
    fn new(settings: QueueSettings) -> Self {
        let catalog = Arc::new(MemCatalog::new(
            Arc::new(|_, v| Ok(if v == "/" { "mq".into() } else { v.to_owned() })),
            Arc::new(|_, _, _| true),
        ));
        catalog.set_settings(settings);
        let peers = MemPeers::new();
        Self {
            catalog,
            leases: MemLeases::new(),
            placement: MemPlacement::new(peers.clone()),
            peers,
            temps: MemTemps::new(),
            hop: Arc::new(Hop::default()),
        }
    }

    async fn gateway(&self, slot: u32) -> Gw {
        let id = format!("gw-{slot}");
        let engine = Arc::new(QueueEngine::new(EngineDeps {
            catalog: Arc::clone(&self.catalog) as Arc<dyn sprig_queues::engine::MqCatalog>,
            leases: Arc::new(self.leases.for_gateway(&id)),
            temps: Arc::new(self.temps.for_gateway(&id)),
            clock: Arc::new(SystemClock),
            slot,
            peers: Some(Arc::new(self.peers.clone())),
            placement: Some(Arc::new(self.placement.clone())),
        }));
        self.peers.register(&id, &engine);
        let replies = DirectReplyTo::with_key(slot, b"shared reply key");
        replies.set_relay(Arc::clone(&self.hop) as Arc<dyn ReplyRelay>);
        self.hop
            .0
            .lock()
            .unwrap()
            .insert(slot, Arc::clone(&replies));
        let svc = Services {
            identity: Arc::new(TestIdentity),
            backend: Arc::clone(&engine) as Arc<dyn sprig_proto_amqp::backend::QueueBackend>,
            replies,
        };
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        let close = CancellationToken::new();
        let close2 = close.clone();
        let accept = tokio::spawn(async move {
            loop {
                let (s, peer) = l.accept().await.unwrap();
                let svc = svc.clone();
                let close = close2.clone();
                tokio::spawn(async move {
                    let info = ConnInfo {
                        peer,
                        tls: false,
                        sni: None,
                        cert_login: None,
                    };
                    let _ = serve(s, info, &svc, Config::default(), close).await;
                });
            }
        });
        Gw {
            id,
            addr,
            engine,
            close,
            accept,
        }
    }

    /// `kill -9` of a gateway: its streams break at once, its connections
    /// close, its leases expire, its temporary queues go.
    fn kill(&self, g: &Gw) {
        self.peers.cut(&g.id);
        g.close.cancel();
        g.accept.abort();
        g.engine.shutdown();
        self.leases.expire(&g.id);
        self.temps.drop_gateway(&g.id);
    }

    fn holder(&self, queue_id: u64) -> Option<String> {
        self.leases.holder(42, queue_id)
    }
}

fn quick() -> QueueSettings {
    QueueSettings {
        takeover_grace_ms: 300,
        ..QueueSettings::default()
    }
}

async fn connect(g: &Gw) -> Connection {
    Connection::connect(
        &format!("amqp://acme.app:secret@{}/%2f", g.addr),
        ConnectionProperties::default(),
    )
    .await
    .unwrap()
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

/// The queue's id, from the catalog.
async fn queue_id(net: &Net, q: &str) -> u64 {
    use sprig_queues::engine::MqCatalog;
    let p = Principal {
        tenant_id: 42,
        tenant: "acme".into(),
        user: "app".into(),
        user_id: Some(1),
        handle: None,
    };
    let v = net.catalog.open(&p, "/").await.unwrap();
    net.catalog.topology(&v).queues.get(q).unwrap().id
}

async fn publish_confirmed(ch: &Channel, q: &str, body: &str, id: &str) -> bool {
    match ch
        .basic_publish(
            "".into(),
            q.into(),
            BasicPublishOptions::default(),
            body.as_bytes(),
            BasicProperties::default()
                .with_delivery_mode(2)
                .with_message_id(id.into()),
        )
        .await
    {
        Ok(c) => matches!(c.await, Ok(c) if c.is_ack()),
        Err(_) => false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_on_one_gateway_consume_on_another() {
    let net = Net::new(quick());
    let a = net.gateway(1).await;
    let b = net.gateway(2).await;
    let ca = connect(&a).await;
    let cb = connect(&b).await;
    let pa = ca.create_channel().await.unwrap();
    let q = common::uniq("relay");
    declare(&pa, &q).await;
    pa.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    for i in 0..300 {
        assert!(publish_confirmed(&pa, &q, &format!("m-{i}"), &format!("m-{i}")).await);
    }
    let id = queue_id(&net, &q).await;
    let owner = net.holder(id).unwrap();
    assert!(owner == a.id || owner == b.id, "{owner}");
    // A get and a passive declare through B see the queue.
    let cbh = cb.create_channel().await.unwrap();
    let n = cbh
        .queue_declare(
            q.as_str().into(),
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    assert_eq!(n.message_count(), 300);
    let g = cbh
        .basic_get(q.as_str().into(), BasicGetOptions::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&*g.data, b"m-0");
    g.ack(BasicAckOptions::default()).await.unwrap();
    // Consume the rest on B, in order.
    cbh.basic_qos(50, BasicQosOptions::default()).await.unwrap();
    let mut consumer = cbh
        .basic_consume(
            q.as_str().into(),
            "c1".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    for i in 1..300 {
        let d = tokio::time::timeout(Duration::from_secs(10), consumer.next())
            .await
            .expect("delivery in time")
            .unwrap()
            .unwrap();
        assert_eq!(String::from_utf8_lossy(&d.data), format!("m-{i}"));
        assert!(!d.redelivered);
        d.ack(BasicAckOptions::default()).await.unwrap();
    }
    // Publishing on B and getting on A crosses the other way.
    let pb = cb.create_channel().await.unwrap();
    pb.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    let q2 = common::uniq("relay2");
    declare(&pb, &q2).await;
    for i in 0..20 {
        assert!(publish_confirmed(&pb, &q2, &format!("x-{i}"), &format!("x-{i}")).await);
    }
    let purged = pa
        .queue_purge(q2.as_str().into(), QueuePurgeOptions::default())
        .await
        .unwrap();
    assert_eq!(purged, 20);
    let n = pa
        .queue_delete(q2.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
    assert_eq!(n, 0);
    let gone = pb
        .queue_declare(
            q2.as_str().into(),
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await;
    assert!(gone.is_err(), "deleted through A, gone for B");
    let _ = ca.close(200, "bye".into()).await;
    let _ = cb.close(200, "bye".into()).await;
}

/// Publishes `n` numbered messages on `ch`, republishing any that aren't
/// confirmed (a new message id each attempt), until every number is
/// confirmed once. Returns the confirmed numbers.
async fn publish_all(
    conn_of: impl Fn() -> futures::future::BoxFuture<'static, Connection>,
    q: String,
    n: u32,
) -> HashSet<u32> {
    let mut confirmed = HashSet::new();
    let mut attempt = 0u32;
    let mut conn = conn_of().await;
    let mut ch = conn.create_channel().await.unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    let mut i = 0;
    while i < n {
        attempt += 1;
        if publish_confirmed(&ch, &q, &i.to_string(), &format!("{i}.{attempt}")).await {
            confirmed.insert(i);
            i += 1;
        } else {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if !ch.status().connected() {
                conn = conn_of().await;
                ch = conn.create_channel().await.unwrap();
                ch.confirm_select(ConfirmSelectOptions::default())
                    .await
                    .unwrap();
            }
        }
    }
    let _ = conn.close(200, "bye".into()).await;
    confirmed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owner_killed_mid_stream_clients_elsewhere_see_no_loss() {
    let net = Net::new(quick());
    let gws = [
        net.gateway(1).await,
        net.gateway(2).await,
        net.gateway(3).await,
    ];
    let q = common::uniq("kill");
    {
        let c = connect(&gws[0]).await;
        let ch = c.create_channel().await.unwrap();
        declare(&ch, &q).await;
        ch.confirm_select(ConfirmSelectOptions::default())
            .await
            .unwrap();
        assert!(publish_confirmed(&ch, &q, "warmup", "warmup").await);
        let _ = c.close(200, "bye".into()).await;
    }
    let id = queue_id(&net, &q).await;
    let owner = net.holder(id).unwrap();
    let others: Vec<&Gw> = gws.iter().filter(|g| g.id != owner).collect();
    let owner_gw = gws.iter().find(|g| g.id == owner).unwrap();
    let (pub_gw, con_gw) = (others[0], others[1]);
    // The consumer on one non-owner gateway, the publisher on the other.
    let cc = connect(con_gw).await;
    let cch = cc.create_channel().await.unwrap();
    cch.basic_qos(20, BasicQosOptions::default()).await.unwrap();
    let mut consumer = cch
        .basic_consume(
            q.as_str().into(),
            "c".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let total = 1500u32;
    let addr = pub_gw.addr.clone();
    let qq = q.clone();
    let publisher = tokio::spawn(async move {
        publish_all(
            move || {
                let addr = addr.clone();
                Box::pin(async move {
                    Connection::connect(
                        &format!("amqp://acme.app:secret@{addr}/%2f"),
                        ConnectionProperties::default(),
                    )
                    .await
                    .unwrap()
                })
            },
            qq,
            total,
        )
        .await
    });
    let mut seen: HashMap<String, Vec<bool>> = HashMap::new();
    let mut numbers: HashSet<u32> = HashSet::new();
    let mut killed = false;
    let mut held = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    while numbers.len() < total as usize + 1 && tokio::time::Instant::now() < deadline {
        let Ok(Some(Ok(d))) = tokio::time::timeout(Duration::from_secs(5), consumer.next()).await
        else {
            continue;
        };
        let body = String::from_utf8(d.data.clone()).unwrap();
        let mid = d
            .properties
            .message_id()
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        seen.entry(mid).or_default().push(d.redelivered);
        if let Ok(n) = body.parse::<u32>() {
            numbers.insert(n);
        } else {
            numbers.insert(u32::MAX);
        }
        // Leave some unacknowledged across the kill, and acknowledge them
        // once the next owner took over (they stay delivered to this
        // consumer through the takeover grace).
        if !killed && held.len() < 8 && numbers.len().is_multiple_of(5) {
            held.push(d);
        } else {
            d.ack(BasicAckOptions::default()).await.unwrap();
        }
        if !killed && numbers.len() > total as usize / 3 {
            killed = true;
            net.kill(owner_gw);
        }
        if killed && !held.is_empty() && net.holder(id).is_some_and(|h| h != owner) {
            for h in held.drain(..) {
                let h: lapin::message::Delivery = h;
                h.ack(BasicAckOptions::default()).await.unwrap();
            }
        }
    }
    let confirmed = publisher.await.unwrap();
    assert!(killed);
    let missing: Vec<u32> = confirmed
        .iter()
        .filter(|n| !numbers.contains(n))
        .copied()
        .collect();
    assert!(
        missing.is_empty(),
        "confirmed but never delivered: {missing:?}"
    );
    // No message (by id) delivered twice without `redelivered` on the
    // later deliveries.
    for (mid, flags) in &seen {
        let fresh = flags.iter().filter(|r| !**r).count();
        assert!(fresh <= 1, "{mid} delivered fresh {fresh} times: {flags:?}");
    }
    let new_owner = net.holder(id).unwrap();
    assert_ne!(new_owner, owner);
    let _ = cc.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn direct_reply_to_and_reply_queues_across_gateways() {
    let net = Net::new(quick());
    let a = net.gateway(1).await;
    let b = net.gateway(2).await;
    let ca = connect(&a).await;
    let cb = connect(&b).await;
    let rpc = common::uniq("rpc");
    let req = ca.create_channel().await.unwrap();
    declare(&req, &rpc).await;
    // The responder on B answers every request to its reply-to.
    let resp = cb.create_channel().await.unwrap();
    let mut requests = resp
        .basic_consume(
            rpc.as_str().into(),
            "server".into(),
            BasicConsumeOptions {
                no_ack: true,
                ..BasicConsumeOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    let resp2 = resp.clone();
    tokio::spawn(async move {
        while let Some(Ok(d)) = requests.next().await {
            let to = d.properties.reply_to().clone().unwrap();
            let mut body = b"re:".to_vec();
            body.extend_from_slice(&d.data);
            let _ = resp2
                .basic_publish(
                    "".into(),
                    to.as_str().into(),
                    BasicPublishOptions::default(),
                    &body,
                    BasicProperties::default(),
                )
                .await;
        }
    });
    // Direct reply-to: the requester on A.
    let mut replies = req
        .basic_consume(
            "amq.rabbitmq.reply-to".into(),
            "rr".into(),
            BasicConsumeOptions {
                no_ack: true,
                ..BasicConsumeOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    for i in 0..5 {
        req.basic_publish(
            "".into(),
            rpc.as_str().into(),
            BasicPublishOptions::default(),
            format!("q{i}").as_bytes(),
            BasicProperties::default().with_reply_to("amq.rabbitmq.reply-to".into()),
        )
        .await
        .unwrap();
        let d = tokio::time::timeout(Duration::from_secs(10), replies.next())
            .await
            .expect("direct reply in time")
            .unwrap()
            .unwrap();
        assert_eq!(String::from_utf8_lossy(&d.data), format!("re:q{i}"));
    }
    // An exclusive server-named reply queue on A, answered from B.
    let rq = req
        .queue_declare(
            "".into(),
            QueueDeclareOptions {
                exclusive: true,
                auto_delete: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    let rname = rq.name().to_string();
    let mut mine = req
        .basic_consume(
            rname.as_str().into(),
            "mine".into(),
            BasicConsumeOptions {
                no_ack: true,
                ..BasicConsumeOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    for i in 0..5 {
        req.basic_publish(
            "".into(),
            rpc.as_str().into(),
            BasicPublishOptions::default(),
            format!("t{i}").as_bytes(),
            BasicProperties::default().with_reply_to(rname.as_str().into()),
        )
        .await
        .unwrap();
        let d = tokio::time::timeout(Duration::from_secs(10), mine.next())
            .await
            .expect("queue reply in time")
            .unwrap()
            .unwrap();
        assert_eq!(String::from_utf8_lossy(&d.data), format!("re:t{i}"));
    }
    // B can't consume A's exclusive queue (405).
    let other = cb.create_channel().await.unwrap();
    let r = other
        .basic_consume(
            rname.as_str().into(),
            "thief".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await;
    assert!(r.is_err(), "another connection's exclusive queue is locked");
    let _ = ca.close(200, "bye".into()).await;
    let _ = cb.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_owner_follows_its_consumers_without_redelivering() {
    let net = Net::new(QueueSettings {
        takeover_grace_ms: 500,
        owner_move_after_ms: 1_000,
        owner_move_cooldown_ms: 600_000,
        ..QueueSettings::default()
    });
    let a = net.gateway(1).await;
    let b = net.gateway(2).await;
    let q = common::uniq("follow");
    let ca = connect(&a).await;
    let pa = ca.create_channel().await.unwrap();
    declare(&pa, &q).await;
    pa.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    for i in 0..100 {
        assert!(publish_confirmed(&pa, &q, &format!("m-{i}"), &format!("m-{i}")).await);
    }
    let id = queue_id(&net, &q).await;
    let owner = net.holder(id).unwrap();
    // The consumers on the other gateway.
    let there = if owner == a.id { &b } else { &a };
    let cc = connect(there).await;
    let ch = cc.create_channel().await.unwrap();
    ch.basic_qos(10, BasicQosOptions::default()).await.unwrap();
    let mut consumer = ch
        .basic_consume(
            q.as_str().into(),
            "c".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let mut got = Vec::new();
    let mut held = Vec::new();
    let end = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut moved = false;
    while got.len() < 100 && tokio::time::Instant::now() < end {
        if !moved && net.holder(id).as_deref() == Some(there.id.as_str()) {
            moved = true;
            // What was delivered by the old owner is acknowledged now,
            // through the new one.
            for d in held.drain(..) {
                let d: lapin::message::Delivery = d;
                d.ack(BasicAckOptions::default()).await.unwrap();
            }
        }
        let Ok(Some(Ok(d))) =
            tokio::time::timeout(Duration::from_millis(300), consumer.next()).await
        else {
            continue;
        };
        assert!(
            !d.redelivered,
            "{} came back redelivered",
            String::from_utf8_lossy(&d.data)
        );
        got.push(String::from_utf8(d.data.clone()).unwrap());
        // Hold the first ten until the owner moved; ack the rest slowly so
        // the move happens mid-stream.
        if got.len() <= 10 && !moved {
            held.push(d);
        } else {
            d.ack(BasicAckOptions::default()).await.unwrap();
            if !moved {
                tokio::time::sleep(Duration::from_millis(40)).await;
            }
        }
    }
    assert!(moved, "the owner moved to the consumers' gateway");
    let want: Vec<String> = (0..100).map(|i| format!("m-{i}")).collect();
    assert_eq!(got, want, "every message once, in order");
    let n = pa
        .queue_delete(q.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
    assert_eq!(n, 0, "nothing left");
    let _ = ca.close(200, "bye".into()).await;
    let _ = cc.close(200, "bye".into()).await;
}

/// R9 (r9tests): messages taken by `mq_get` under a lease tag go back to
/// the queue at once when `mq_release` names the tag, through any gateway:
/// gets through B (relayed to the owner), the release through C. Messages
/// held by a session without a tag, or under another tag, stay held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lease_tags_are_released_through_any_gateway() {
    let net = Net::new(quick());
    let a = net.gateway(1).await;
    let b = net.gateway(2).await;
    let c = net.gateway(3).await;
    let ca = connect(&a).await;
    let pa = ca.create_channel().await.unwrap();
    let q = common::uniq("tagrel");
    declare(&pa, &q).await;
    pa.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    for i in 0..10 {
        assert!(publish_confirmed(&pa, &q, &format!("m-{i}"), &format!("m-{i}")).await);
    }
    let p = Arc::new(Principal {
        tenant_id: 42,
        tenant: "acme".into(),
        user: "app".into(),
        user_id: Some(1),
        handle: None,
    });
    let body = |d: &sprig_proto_amqp::message::Delivery| {
        String::from_utf8_lossy(&sprig_queues::sql::body_bytes(d)).into_owned()
    };
    // The dead task's gets: four under its tag, ten-minute leases. Two more
    // by a plain session, two under another tag.
    let vb = b.engine.sql_vhost(Arc::clone(&p), "/", 7).await.unwrap();
    let dead = vb
        .get_tagged(&q, 4, 600_000, Some("s9/0/3"))
        .await
        .unwrap();
    assert_eq!(dead.iter().map(body).collect::<Vec<_>>(), ["m-0", "m-1", "m-2", "m-3"]);
    let plain = vb.get(&q, 2, 600_000).await.unwrap();
    assert_eq!(plain.len(), 2);
    let other = vb
        .get_tagged(&q, 2, 600_000, Some("s9/1/1"))
        .await
        .unwrap();
    assert_eq!(other.len(), 2);
    // The new task, on another gateway and session, releases the tag.
    let vc = c.engine.sql_vhost(Arc::clone(&p), "/", 8).await.unwrap();
    vc.release_tag(&q, "s9/0/3").await.unwrap();
    let mut again = Vec::new();
    for _ in 0..50 {
        again.extend(vc.get(&q, 10, 600_000).await.unwrap());
        if again.len() >= 6 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut got: Vec<String> = again.iter().map(body).collect();
    got.sort();
    assert_eq!(
        got,
        ["m-0", "m-1", "m-2", "m-3", "m-8", "m-9"],
        "the tag's four came back at once; the plain and other-tag ones stayed held"
    );
    for d in again.iter().filter(|d| body(d) < "m-4".to_owned()) {
        assert!(d.redelivered, "{} is a redelivery", body(d));
        assert_eq!(d.delivery_count, 1, "a release counts as a failed delivery");
    }
    // Releasing an unknown tag, or the same tag twice, changes nothing.
    vc.release_tag(&q, "s9/0/3").await.unwrap();
    vc.release_tag(&q, "nobody").await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(vc.get(&q, 10, 600_000).await.unwrap().is_empty());
    let _ = ca.close(200, "bye".into()).await;
}
