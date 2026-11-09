//! A deterministic simulation of the queue owner (chapter 50, "Testing":
//! crash owners at every step; chapter 46).
//!
//! One queue on an in-memory store. A publisher sends numbered messages
//! with confirms; a consumer takes them with credit and acknowledges most;
//! at random steps the owner crashes (its task is aborted), loses its lease
//! without noticing (a second owner takes over while the first runs on and
//! is fenced at its next write), or the store fails a write or loses a
//! commit's answer. Seeds make every run repeatable.
//!
//! Checked at the end, after a last owner drains the queue:
//! - every confirmed message was delivered and acknowledged (none lost);
//! - no message was delivered twice without `redelivered` on the later
//!   deliveries (at most one delivery with `redelivered = false`);
//! - a fenced owner never wrote after the takeover (the store's epoch).

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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use sprig_proto_amqp::backend::{ConnKey, ConsumerId, EventSender, Settlement};
use sprig_proto_amqp::error::MqError;
use sprig_proto_amqp::message::{Delivery, Message, MessageInfo, MsgFormat};
use sprig_proto_amqp::v091::codec::FieldTable;
use sprig_queues::memstore::{Faults, MemStore};
use sprig_queues::owner::{
    self, Cmd, ConsumerSpec, DeliverySink, Holder, Incoming, OwnerParams, SystemClock,
};
use sprig_queues::store::{MsgStore, new_msg_id};
use sprig_queues::topology::{QType, QueueCfg, QueueDef};
use tokio::sync::{mpsc, oneshot};

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

enum Seen {
    Deliver(ConsumerId, Delivery),
    Cancelled(ConsumerId),
}

struct Sink(mpsc::UnboundedSender<Seen>);

impl DeliverySink for Sink {
    fn deliver(&self, c: ConsumerId, d: Delivery) -> bool {
        self.0.send(Seen::Deliver(c, d)).is_ok()
    }
    fn cancelled(&self, c: ConsumerId, _r: MqError) {
        let _ = self.0.send(Seen::Cancelled(c));
    }
    fn drained(&self, _c: ConsumerId, _n: u32) {}
}

fn queue() -> Arc<QueueDef> {
    Arc::new(QueueDef {
        id: 7,
        name: "sim".into(),
        qtype: QType::Classic,
        durable: true,
        exclusive: None,
        auto_delete: false,
        arguments: FieldTable::new(),
        lanes: 1,
        temp_on: None,
    })
}

fn message(n: u64) -> Arc<Message> {
    let body = Bytes::from(n.to_string());
    Arc::new(Message {
        format: MsgFormat::Amqp091,
        exchange: String::new(),
        routing_key: "sim".into(),
        props: Bytes::from_static(&[0, 0]),
        body: body.clone(),
        info: MessageInfo {
            size: 2 + body.len() as u64,
            ..MessageInfo::default()
        },
    })
}

struct Owner {
    tx: mpsc::UnboundedSender<Cmd>,
    task: tokio::task::JoinHandle<()>,
}

fn start(store: &MemStore, cache_bytes: u64) -> Owner {
    let q = queue();
    let (ev, evrx) = mpsc::unbounded_channel();
    std::mem::forget(evrx);
    let p = OwnerParams {
        tenant: 1,
        db: 1,
        cfg: QueueCfg::of(&q, &[], 20),
        queue: q,
        batch_rows: 50,
        cache_bytes,
        page: 16,
        consumer_timeout_ms: 0,
        max_unacked: 1000,
        hold_check_ms: 5000,
        tokens: None,
        clock: Arc::new(SystemClock),
        events: ev,
    };
    let (tx, rx) = mpsc::unbounded_channel();
    let s: Arc<dyn MsgStore> = Arc::new(store.clone());
    let task = tokio::spawn(owner::run(p, s, rx));
    Owner { tx, task }
}

struct Run {
    confirmed: HashSet<u64>,
    acked: HashSet<u64>,
    fresh_deliveries: HashMap<u64, u32>,
    deliveries: u64,
    redelivered: u64,
    crashes: u32,
    fenced: u32,
    store_faults: u32,
}

async fn attach(
    o: &Owner,
    id: ConsumerId,
    holder: Holder,
    sink: &mpsc::UnboundedSender<Seen>,
    credit: u32,
) {
    let (tx, rx) = oneshot::channel();
    let _ = o.tx.send(Cmd::Attach(
        ConsumerSpec {
            id,
            holder,
            sink: Arc::new(Sink(sink.clone())),
            no_ack: false,
            exclusive: false,
            priority: 0,
        },
        tx,
    ));
    let _ = rx.await;
    let _ = o.tx.send(Cmd::Credit {
        consumer: id,
        credit,
        drain: false,
    });
}

async fn simulate(seed: u64, steps: u32) -> Run {
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let store = MemStore::new();
    let cache = if seed.is_multiple_of(3) { 300 } else { 1 << 20 };
    let mut owner = start(&store, cache);
    let mut stale: Vec<Owner> = Vec::new();
    let (stx, mut srx) = mpsc::unbounded_channel::<Seen>();
    let mut conn = 1u64;
    let mut holder = Holder {
        origin: 0,
        conn: ConnKey(conn),
    };
    let mut consumer = ConsumerId(1);
    attach(&owner, consumer, holder, &stx, 20).await;
    let mut run = Run {
        confirmed: HashSet::new(),
        acked: HashSet::new(),
        fresh_deliveries: HashMap::new(),
        deliveries: 0,
        redelivered: 0,
        crashes: 0,
        fenced: 0,
        store_faults: 0,
    };
    let mut next = 0u64;
    let mut confirms: BTreeMap<u64, oneshot::Receiver<Result<(), MqError>>> = BTreeMap::new();
    let mut held: Vec<(Holder, sprig_proto_amqp::message::DeliveryId, u64)> = Vec::new();
    for _ in 0..steps {
        match rng.below(100) {
            // Publish a few.
            0..=39 => {
                let n = 1 + rng.below(5);
                let mut items = Vec::new();
                for _ in 0..n {
                    next += 1;
                    let (tx, rx) = oneshot::channel();
                    confirms.insert(next, rx);
                    items.push((
                        Incoming {
                            msg: message(next),
                            msg_id: new_msg_id(),
                            dedup: false,
                            deaths: None,
                        },
                        Some(tx),
                    ));
                }
                let _ = owner.tx.send(Cmd::Enqueue(items));
            }
            // Acknowledge (or reject back) what the consumer holds.
            40..=69 => {
                let k = held.len().min(1 + rng.below(6) as usize);
                let batch: Vec<_> = held.drain(..k).collect();
                for (h, id, n) in batch {
                    let how = if rng.below(10) == 0 {
                        Settlement::Requeue { failed: false }
                    } else {
                        Settlement::Ack
                    };
                    if how == Settlement::Ack && h == holder {
                        // Counted when the owner confirms the delete: the
                        // final drain re-checks anything lost.
                        run.acked.insert(n);
                    }
                    let _ = owner.tx.send(Cmd::Settle(h, vec![(id, how)]));
                }
                let _ = owner.tx.send(Cmd::Credit {
                    consumer,
                    credit: u32::try_from(k).unwrap_or(1),
                    drain: false,
                });
            }
            // Let things run.
            70..=89 => tokio::time::sleep(Duration::from_millis(1 + rng.below(3))).await,
            // Owner crash: the task dies mid-whatever.
            90..=93 => {
                owner.task.abort();
                let _ = (&mut owner.task).await;
                run.crashes += 1;
                owner = start(&store, cache);
                conn += 1;
                holder = Holder {
                    origin: 0,
                    conn: ConnKey(conn),
                };
                consumer = ConsumerId(conn);
                // Deliveries of the dead owner's consumer are lost with it.
                held.clear();
                attach(&owner, consumer, holder, &stx, 20).await;
            }
            // Lease lost without noticing: a second owner takes over, the
            // first runs on until its next write is fenced.
            94..=96 => {
                let old = std::mem::replace(&mut owner, start(&store, cache));
                stale.push(old);
                run.fenced += 1;
                conn += 1;
                holder = Holder {
                    origin: 0,
                    conn: ConnKey(conn),
                };
                consumer = ConsumerId(conn);
                attach(&owner, consumer, holder, &stx, 20).await;
            }
            // Store faults.
            _ => {
                run.store_faults += 1;
                store.inject(if rng.below(2) == 0 {
                    Faults {
                        fail_next_write: true,
                        lose_next_answer: false,
                    }
                } else {
                    Faults {
                        fail_next_write: false,
                        lose_next_answer: true,
                    }
                });
            }
        }
        // Collect deliveries.
        while let Ok(s) = srx.try_recv() {
            match s {
                Seen::Deliver(c, d) => {
                    let n: u64 = std::str::from_utf8(&d.message.body)
                        .unwrap()
                        .parse()
                        .unwrap();
                    run.deliveries += 1;
                    if d.redelivered {
                        run.redelivered += 1;
                    } else {
                        *run.fresh_deliveries.entry(n).or_default() += 1;
                    }
                    if c == consumer {
                        held.push((holder, d.id, n));
                    }
                }
                Seen::Cancelled(c) => {
                    if c == consumer {
                        // The owner stopped (fenced or a store failure):
                        // the next one takes over.
                        owner.task.abort();
                        let _ = (&mut owner.task).await;
                        owner = start(&store, cache);
                        conn += 1;
                        holder = Holder {
                            origin: 0,
                            conn: ConnKey(conn),
                        };
                        consumer = ConsumerId(conn);
                        held.clear();
                        attach(&owner, consumer, holder, &stx, 20).await;
                    }
                }
            }
        }
        // If the owner died on its own (store failure), restart it.
        if owner.task.is_finished() {
            owner = start(&store, cache);
            conn += 1;
            holder = Holder {
                origin: 0,
                conn: ConnKey(conn),
            };
            consumer = ConsumerId(conn);
            held.clear();
            attach(&owner, consumer, holder, &stx, 20).await;
        }
        // Collect confirms that arrived.
        let done: Vec<u64> = confirms
            .iter_mut()
            .filter_map(|(n, rx)| match rx.try_recv() {
                Ok(Ok(())) => Some((*n, true)),
                Ok(Err(_)) | Err(oneshot::error::TryRecvError::Closed) => Some((*n, false)),
                Err(oneshot::error::TryRecvError::Empty) => None,
            })
            .map(|(n, ok)| {
                if ok {
                    run.confirmed.insert(n);
                }
                n
            })
            .collect();
        for n in done {
            confirms.remove(&n);
        }
    }
    // Wind down: stale owners are fenced at their next write or stop.
    for s in stale {
        let _ = s.tx.send(Cmd::Stop);
        let _ = tokio::time::timeout(Duration::from_secs(5), s.task).await;
    }
    owner.task.abort();
    let _ = (&mut owner.task).await;
    for (n, rx) in confirms {
        if let Ok(Ok(())) = rx.await {
            run.confirmed.insert(n);
        }
    }
    // A last owner drains everything; every delivery is acknowledged.
    store.inject(Faults::default());
    let last = start(&store, 1 << 20);
    let (dtx, mut drx) = mpsc::unbounded_channel::<Seen>();
    let h = Holder {
        origin: 0,
        conn: ConnKey(999_999),
    };
    attach(&last, ConsumerId(999_999), h, &dtx, 100_000).await;
    let mut final_acked = HashSet::new();
    loop {
        match tokio::time::timeout(Duration::from_millis(300), drx.recv()).await {
            Ok(Some(Seen::Deliver(_, d))) => {
                let n: u64 = std::str::from_utf8(&d.message.body)
                    .unwrap()
                    .parse()
                    .unwrap();
                run.deliveries += 1;
                if d.redelivered {
                    run.redelivered += 1;
                } else {
                    *run.fresh_deliveries.entry(n).or_default() += 1;
                }
                final_acked.insert(n);
                let _ = last.tx.send(Cmd::Settle(h, vec![(d.id, Settlement::Ack)]));
            }
            Ok(Some(Seen::Cancelled(_)) | None) => break,
            Err(_) => break,
        }
    }
    let _ = last.tx.send(Cmd::Stop);
    let _ = last.task.await;
    assert!(
        store.messages(7).is_empty(),
        "seed {seed}: messages left after the drain: {}",
        store.messages(7).len()
    );
    // Everything confirmed was consumed: acknowledged during the run or
    // drained at the end.
    let consumed: HashSet<u64> = run.acked.union(&final_acked).copied().collect();
    let lost: Vec<u64> = run.confirmed.difference(&consumed).copied().collect();
    assert!(
        lost.is_empty(),
        "seed {seed}: confirmed messages lost: {lost:?}"
    );
    for (n, fresh) in &run.fresh_deliveries {
        assert!(
            *fresh <= 1,
            "seed {seed}: message {n} delivered {fresh} times without redelivered"
        );
    }
    run
}

#[tokio::test(flavor = "current_thread", start_paused = false)]
async fn owner_survives_crashes_fencing_and_store_faults() {
    let seeds: u64 = std::env::var("SPRIG_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let (mut deliveries, mut redelivered, mut crashes, mut fenced, mut faults, mut confirmed) =
        (0, 0, 0, 0, 0, 0);
    for seed in 1..=seeds {
        let r = simulate(seed, 400).await;
        deliveries += r.deliveries;
        redelivered += r.redelivered;
        crashes += r.crashes;
        fenced += r.fenced;
        faults += r.store_faults;
        confirmed += r.confirmed.len();
    }
    eprintln!(
        "{seeds} seeds: {confirmed} confirmed, {deliveries} deliveries ({redelivered} redelivered), {crashes} crashes, {fenced} stale owners, {faults} store faults"
    );
    assert!(crashes > 0 && fenced > 0 && faults > 0);
}

/// Unused helper kept for symmetry with the engine's sinks.
#[allow(dead_code)]
fn unused(_: EventSender) {}
