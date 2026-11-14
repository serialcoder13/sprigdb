//! A randomized simulation of queues across gateways: crash owners
//! and consumer gateways at every step. Three engines share the in-memory catalog, leases, registry and
//! store, and reach each other through the relay. At random steps clients
//! publish (with confirms) through any gateway, consumers on any gateway
//! take, acknowledge, requeue or hold deliveries, a gateway is killed
//! (its streams break, its leases expire, its clients vanish) or comes
//! back, and consumers move between gateways, so owners move to follow
//! them (`queue.owner_move_after_seconds`, shortened).
//!
//! Checked after every gateway is back and a last consumer drained the
//! queue:
//! - every confirmed message was delivered (none lost);
//! - no message was delivered twice without `redelivered` on the later
//!   deliveries.
//!
//! `SIM_SEEDS` (default 8) and `SIM_STEPS` (default 200) size the run.

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
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use sprig_proto_amqp::auth::Principal;
use sprig_proto_amqp::backend::{
    ConnKey, ConsumeSpec, ConsumerId, Event, OpCtx, QueueBackend, QueueSpec, Settlement, Vhost,
};
use sprig_proto_amqp::message::DeliveryId;
use sprig_proto_amqp::v091::codec::FieldTable;
use sprig_queues::engine::{EngineDeps, QueueEngine};
use sprig_queues::mem::{MemCatalog, MemLeases, MemTemps};
use sprig_queues::owner::SystemClock;
use sprig_queues::relay::{MemPeers, MemPlacement};
use sprig_queues::settings::QueueSettings;
use tokio::sync::mpsc;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

struct Net {
    catalog: Arc<MemCatalog>,
    leases: MemLeases,
    peers: MemPeers,
    placement: MemPlacement,
    temps: MemTemps,
}

impl Net {
    fn engine(&self, i: usize) -> Arc<QueueEngine> {
        let id = format!("gw-{i}");
        let e = Arc::new(QueueEngine::new(EngineDeps {
            catalog: Arc::clone(&self.catalog) as Arc<dyn sprig_queues::engine::MqCatalog>,
            leases: Arc::new(self.leases.for_gateway(&id)),
            temps: Arc::new(self.temps.for_gateway(&id)),
            clock: Arc::new(SystemClock),
            slot: u32::try_from(i).unwrap(),
            peers: Some(Arc::new(self.peers.clone())),
            placement: Some(Arc::new(self.placement.clone())),
        }));
        self.peers.register(&id, &e);
        e
    }

    fn kill(&self, i: usize, e: &QueueEngine) {
        let id = format!("gw-{i}");
        self.peers.cut(&id);
        e.shutdown();
        self.leases.expire(&id);
        self.temps.drop_gateway(&id);
    }
}

fn principal() -> Arc<Principal> {
    Arc::new(Principal {
        tenant_id: 42,
        tenant: "acme".into(),
        user: "app".into(),
        user_id: Some(1),
        handle: None,
    })
}

struct Client {
    gw: usize,
    v: Arc<dyn Vhost>,
    cx: OpCtx,
    rx: mpsc::UnboundedReceiver<Event>,
    consumer: Option<ConsumerId>,
    held: Vec<DeliveryId>,
}

async fn client(e: &QueueEngine, gw: usize) -> Client {
    let conn = ConnKey::next();
    let (tx, rx) = mpsc::unbounded_channel();
    let v = e.open_vhost(&principal(), "/", conn, tx).await.unwrap();
    Client {
        gw,
        v,
        cx: OpCtx {
            principal: principal(),
            conn,
            channel: 1,
        },
        rx,
        consumer: None,
        held: Vec::new(),
    }
}

async fn consume(c: &mut Client, q: &str, prefetch: u32) -> bool {
    let id = ConsumerId::next();
    let r =
        c.v.consume(
            &c.cx,
            ConsumeSpec {
                consumer: id,
                queue: q.into(),
                tag: format!("c{}", id.0),
                no_ack: false,
                exclusive: false,
                priority: 0,
                arguments: FieldTable::new(),
            },
        )
        .await;
    if r.is_err() {
        return false;
    }
    c.v.credit(id, prefetch, false);
    c.consumer = Some(id);
    true
}

fn message(n: u64) -> sprig_proto_amqp::message::Message {
    sprig_queues::msg::rebuild(
        0,
        String::new(),
        "simq".into(),
        Bytes::from_static(&[0, 0]),
        Bytes::from(n.to_string()),
    )
    .unwrap()
}

#[derive(Default)]
struct Seen {
    fresh: HashMap<u64, u32>,
    any: HashSet<u64>,
    deliveries: u64,
    redelivered: u64,
}

/// Takes what arrived for a client's consumer and settles some of it.
fn pump(c: &mut Client, rng: &mut Rng, seen: &mut Seen) {
    let mut settle = Vec::new();
    let mut credit = 0u32;
    while let Ok(ev) = c.rx.try_recv() {
        match ev {
            Event::Deliver { delivery: d, .. } => {
                let n: u64 = String::from_utf8_lossy(&d.message.body).parse().unwrap();
                seen.deliveries += 1;
                seen.any.insert(n);
                if d.redelivered {
                    seen.redelivered += 1;
                } else {
                    *seen.fresh.entry(n).or_default() += 1;
                }
                match rng.below(10) {
                    0 => settle.push((d.id, Settlement::Requeue { failed: false })),
                    1 if c.held.len() < 5 => c.held.push(d.id),
                    _ => settle.push((d.id, Settlement::Ack)),
                }
            }
            Event::Cancelled { consumer, .. } if Some(consumer) == c.consumer => {
                c.consumer = None;
            }
            _ => {}
        }
    }
    credit += u32::try_from(settle.len()).unwrap();
    if !settle.is_empty() {
        c.v.settle(&c.cx, settle);
    }
    if let Some(id) = c.consumer
        && credit > 0
    {
        c.v.credit(id, credit, false);
    }
}

struct Outcome {
    confirmed: usize,
    deliveries: u64,
    redelivered: u64,
    kills: u32,
    moves: u64,
}

async fn simulate(seed: u64, steps: u32) -> Outcome {
    let catalog = Arc::new(MemCatalog::new(
        Arc::new(|_, _| Ok("mq".into())),
        Arc::new(|_, _, _| true),
    ));
    catalog.set_settings(QueueSettings {
        takeover_grace_ms: 150,
        owner_move_after_ms: 400,
        owner_move_cooldown_ms: 1_500,
        batch_rows: 50,
        ..QueueSettings::default()
    });
    let peers = MemPeers::new();
    let net = Net {
        catalog,
        leases: MemLeases::new(),
        placement: MemPlacement::new(peers.clone()),
        peers,
        temps: MemTemps::new(),
    };
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let mut engines: Vec<Option<Arc<QueueEngine>>> = (0..3).map(|i| Some(net.engine(i))).collect();
    let q = "simq".to_owned();
    {
        let c = client(engines[0].as_ref().unwrap(), 0).await;
        c.v.declare_queue(
            &c.cx,
            QueueSpec {
                name: q.clone(),
                durable: true,
                ..QueueSpec::default()
            },
            false,
        )
        .await
        .unwrap();
    }
    let qid = {
        use sprig_queues::engine::MqCatalog;
        let v = net.catalog.open(&principal(), "/").await.unwrap();
        net.catalog.topology(&v).queues.get(&q).unwrap().id
    };
    let mut owner_changes = 0u64;
    let mut last_owner: Option<String> = None;
    let mut consumers: Vec<Client> = Vec::new();
    for g in [1usize, 2] {
        let mut c = client(engines[g].as_ref().unwrap(), g).await;
        if consume(&mut c, &q, 10).await {
            consumers.push(c);
        }
    }
    let mut confirms = tokio::task::JoinSet::new();
    let mut next = 0u64;
    let mut seen = Seen::default();
    let mut kills = 0u32;
    for _ in 0..steps {
        let live: Vec<usize> = (0..3).filter(|i| engines[*i].is_some()).collect();
        match rng.below(100) {
            0..40 => {
                let g = live[rng.below(live.len() as u64) as usize];
                let c = client(engines[g].as_ref().unwrap(), g).await;
                for _ in 0..=rng.below(5) {
                    let n = next;
                    next += 1;
                    if let Ok(p) = c.v.publish(&c.cx, message(n), false) {
                        confirms.spawn(async move {
                            matches!(
                                tokio::time::timeout(Duration::from_secs(5), p.confirm).await,
                                Ok(Ok(()))
                            )
                            .then_some(n)
                        });
                    }
                }
            }
            40..78 => {
                for c in &mut consumers {
                    pump(c, &mut rng, &mut seen);
                }
            }
            78..86 => {
                for c in &mut consumers {
                    let held = std::mem::take(&mut c.held);
                    if !held.is_empty() {
                        let n = u32::try_from(held.len()).unwrap();
                        c.v.settle(
                            &c.cx,
                            held.into_iter().map(|id| (id, Settlement::Ack)).collect(),
                        );
                        if let Some(id) = c.consumer {
                            c.v.credit(id, n, false);
                        }
                    }
                }
            }
            86..89 if live.len() > 1 => {
                let g = live[rng.below(live.len() as u64) as usize];
                net.kill(g, engines[g].as_ref().unwrap());
                engines[g] = None;
                kills += 1;
                // Its clients are gone with it.
                consumers.retain(|c| c.gw != g);
            }
            89..93 => {
                if let Some(g) = (0..3).find(|i| engines[*i].is_none()) {
                    engines[g] = Some(net.engine(g));
                }
            }
            93..100 => {
                // A consumer moves: closes here, opens on another gateway.
                if !consumers.is_empty() {
                    let i = rng.below(consumers.len() as u64) as usize;
                    let c = consumers.swap_remove(i);
                    if engines[c.gw].is_some() {
                        c.v.connection_closed(c.cx.conn).await;
                    }
                }
                let g = live[rng.below(live.len() as u64) as usize];
                if let Some(e) = &engines[g] {
                    let mut c = client(e, g).await;
                    if consume(&mut c, &q, 10).await {
                        consumers.push(c);
                    }
                }
            }
            _ => {}
        }
        // Consumers whose consumer ended (a lost owner they could not
        // re-attach to) consume again.
        for c in &mut consumers {
            if c.consumer.is_none() && engines[c.gw].is_some() {
                let _ = consume(c, &q, 10).await;
            }
        }
        tokio::time::sleep(Duration::from_millis(5 + rng.below(20))).await;
        let h = net.leases.holder(42, qid);
        if h.is_some() && h != last_owner {
            if last_owner.is_some() {
                owner_changes += 1;
            }
            last_owner = h;
        }
    }
    // Everything back; a last consumer drains the queue.
    for (i, e) in engines.iter_mut().enumerate() {
        if e.is_none() {
            *e = Some(net.engine(i));
        }
    }
    for c in &mut consumers {
        pump(c, &mut rng, &mut seen);
        if engines[c.gw].is_some() {
            c.v.connection_closed(c.cx.conn).await;
        }
    }
    consumers.clear();
    let mut confirmed = HashSet::new();
    while let Some(r) = confirms.join_next().await {
        if let Ok(Some(n)) = r {
            confirmed.insert(n);
        }
    }
    let g = rng.below(3) as usize;
    let mut last = client(engines[g].as_ref().unwrap(), g).await;
    let mut quiet = 0;
    let mut tries = 0;
    while quiet < 30 {
        if last.consumer.is_none() && !consume(&mut last, &q, 50).await {
            tries += 1;
            assert!(tries < 100, "seed {seed}: the last consumer can't attach");
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        let before = seen.deliveries;
        // Only acknowledgements now.
        let mut settle = Vec::new();
        while let Ok(ev) = last.rx.try_recv() {
            match ev {
                Event::Deliver { delivery: d, .. } => {
                    let n: u64 = String::from_utf8_lossy(&d.message.body).parse().unwrap();
                    seen.deliveries += 1;
                    seen.any.insert(n);
                    if d.redelivered {
                        seen.redelivered += 1;
                    } else {
                        *seen.fresh.entry(n).or_default() += 1;
                    }
                    settle.push((d.id, Settlement::Ack));
                }
                Event::Cancelled { .. } => last.consumer = None,
                _ => {}
            }
        }
        let n = u32::try_from(settle.len()).unwrap();
        if n > 0 {
            last.v.settle(&last.cx, settle);
            if let Some(id) = last.consumer {
                last.v.credit(id, n, false);
            }
        }
        quiet = if seen.deliveries == before {
            quiet + 1
        } else {
            0
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let lost: Vec<u64> = confirmed
        .iter()
        .filter(|n| !seen.any.contains(n))
        .copied()
        .collect();
    assert!(lost.is_empty(), "seed {seed}: confirmed and lost: {lost:?}");
    for (n, fresh) in &seen.fresh {
        assert!(
            *fresh <= 1,
            "seed {seed}: message {n} delivered {fresh} times without redelivered"
        );
    }
    for e in engines.iter().flatten() {
        e.shutdown();
    }
    Outcome {
        confirmed: confirmed.len(),
        deliveries: seen.deliveries,
        redelivered: seen.redelivered,
        kills,
        moves: owner_changes,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queues_survive_gateway_kills_and_owner_moves() {
    let seeds: u64 = std::env::var("SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let steps: u32 = std::env::var("SIM_STEPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);
    let (mut confirmed, mut deliveries, mut redelivered, mut kills, mut moves) = (0, 0, 0, 0, 0);
    for seed in 1..=seeds {
        let o = simulate(seed, steps).await;
        confirmed += o.confirmed;
        deliveries += o.deliveries;
        redelivered += o.redelivered;
        kills += o.kills;
        moves += o.moves;
    }
    eprintln!(
        "relay simulation: {seeds} seeds x {steps} steps: {confirmed} confirmed, {deliveries} deliveries ({redelivered} redelivered), {kills} gateway kills, {moves} owner changes"
    );
}
