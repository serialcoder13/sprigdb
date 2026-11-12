//! The queue relay between gateways (chapter 50, "The queue owner":
//! `sprig.internal.v1.QueuePeer` on 7403).
//!
//! A gateway that doesn't own a queue runs a **remote slot** for it: the
//! same command channel as a local owner, whose commands go over one
//! ordered stream to the owner's gateway ([`Up`] frames) and whose
//! answers, confirms, deliveries and cancels come back ([`Down`] frames).
//! The owner's gateway **serves** the stream ([`QueueEngine::serve_relay`])
//! by feeding the frames to its local owner, with the stream as the
//! consumers' delivery sink. One stream per queue and gateway carries
//! both of the chapter's flows, `Enqueue` (messages in, confirms out) and
//! `Attach` (credit and settlements in, deliveries out), plus gets and the
//! queue operations, so one channel's order holds across all of them (a
//! `basic.get` sees the channel's earlier publish).
//!
//! The transport is a trait ([`PeerTransport`]): the gateway's is gRPC
//! over the peer service with mutual TLS, the tests' is [`MemPeers`]
//! (engines in one process).
//!
//! When the stream ends (the owner moved, stopped or died), commands in
//! flight are answered with 39260, the slot's consumers are told their
//! owner moved (they re-attach, reporting the deliveries they still
//! hold), and the slot looks the owner up again.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use sprig_proto_amqp::backend::{ConnKey, ConsumerId, Settlement};
use sprig_proto_amqp::error::{MqCode, MqError};
use sprig_proto_amqp::message::{Delivery, DeliveryId, Message};
use sprig_proto_amqp::v091::codec::FieldTable;
use tokio::sync::{mpsc, oneshot};

use crate::engine::{QueueEngine, VhostRef, lock};
use crate::msg;
use crate::owner::{ANY_HOLDER, Cmd, ConsumerSpec, DeliverySink, Holder, Incoming};
use crate::store::MsgId;

/// The queue a relay stream is for, and who opens it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueRef {
    /// The tenant.
    pub tenant: u64,
    /// The vhost's database.
    pub db_id: u32,
    /// Its name.
    pub db: String,
    /// The vhost name.
    pub vhost: String,
    /// The queue.
    pub queue_id: u64,
    /// Its name.
    pub queue: String,
    /// The gateway opening the stream.
    pub from: String,
}

/// A message on the wire: its stored parts (chapter 50, "Message
/// encoding"); the receiver parses what the engine reads again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireMsg {
    /// 0 = AMQP 0-9-1, 1 = AMQP 1.0.
    pub fmt: u8,
    /// Published to.
    pub exchange: String,
    /// Published with.
    pub routing_key: String,
    /// Properties in the source encoding.
    pub props: Bytes,
    /// The body.
    pub body: Bytes,
    /// The engine dropped the message's TTL (a dead letter).
    pub no_ttl: bool,
}

impl WireMsg {
    /// The wire form of a message.
    #[must_use]
    pub fn of(m: &Message) -> Self {
        Self {
            fmt: msg::fmt_code(m.format),
            exchange: m.exchange.clone(),
            routing_key: m.routing_key.clone(),
            props: m.props.clone(),
            body: m.body.clone(),
            no_ttl: m.info.ttl_ms.is_none(),
        }
    }

    /// The message again.
    ///
    /// # Errors
    /// 406 for properties that don't parse.
    pub fn message(&self) -> Result<Message, MqError> {
        let mut m = msg::rebuild(
            self.fmt,
            self.exchange.clone(),
            self.routing_key.clone(),
            self.props.clone(),
            self.body.clone(),
        )?;
        if self.no_ttl {
            m.info.ttl_ms = None;
        }
        Ok(m)
    }
}

/// A delivery on the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireDelivery {
    /// What to settle.
    pub id: DeliveryId,
    /// The queue name.
    pub queue: String,
    /// The message.
    pub msg: WireMsg,
    /// Delivered before.
    pub redelivered: bool,
    /// Failed deliveries so far.
    pub delivery_count: u32,
    /// Delivery-time headers, AMQP-encoded table entries.
    pub annotations: Bytes,
}

impl WireDelivery {
    /// The wire form of a delivery.
    #[must_use]
    pub fn of(d: &Delivery) -> Self {
        let mut a = BytesMut::new();
        d.annotations.encode_entries(&mut a);
        Self {
            id: d.id,
            queue: d.queue.clone(),
            msg: WireMsg::of(&d.message),
            redelivered: d.redelivered,
            delivery_count: d.delivery_count,
            annotations: a.freeze(),
        }
    }

    /// The delivery again.
    ///
    /// # Errors
    /// 406 for a message or annotations that don't parse.
    pub fn delivery(&self) -> Result<Delivery, MqError> {
        let annotations = if self.annotations.is_empty() {
            FieldTable::new()
        } else {
            FieldTable::decode_entries(&self.annotations, 0)?
        };
        Ok(Delivery {
            id: self.id,
            queue: self.queue.clone(),
            message: Arc::new(self.msg.message()?),
            redelivered: self.redelivered,
            delivery_count: self.delivery_count,
            annotations,
        })
    }
}

/// From the gateway with clients to the owner's gateway.
#[derive(Debug, Clone, PartialEq)]
pub enum Up {
    /// A message to enqueue; `confirm`: answer `req` after the commit.
    Enqueue {
        /// Answer id.
        req: u64,
        /// The message.
        msg: WireMsg,
        /// Its id.
        msg_id: MsgId,
        /// Check `uk_msg` first (an outbox forward).
        dedup: bool,
        /// `x-death`, encoded.
        deaths: Option<Bytes>,
        /// The publisher wants the confirm.
        confirm: bool,
    },
    /// A consumer.
    Attach {
        /// Answer id.
        req: u64,
        /// The consumer.
        consumer: u64,
        /// Its connection on the sending gateway.
        conn: u64,
        /// Settled on delivery.
        no_ack: bool,
        /// Exclusive.
        exclusive: bool,
        /// `x-priority`.
        priority: i32,
        /// Deliveries it still holds from an earlier owner.
        held: Vec<DeliveryId>,
    },
    /// Credit.
    Credit {
        /// The consumer.
        consumer: u64,
        /// More deliveries.
        credit: u32,
        /// Give up the rest when empty.
        drain: bool,
    },
    /// End a consumer.
    Cancel {
        /// The consumer.
        consumer: u64,
    },
    /// Settlements of a connection's deliveries.
    Settle {
        /// The connection.
        conn: u64,
        /// By delivery token (SQL's `mq_nack`): whoever holds it.
        any: bool,
        /// The settlements.
        items: Vec<(DeliveryId, Settlement)>,
    },
    /// `basic.get` (`lease_ms`: SQL's `mq_get`).
    Get {
        /// Answer id.
        req: u64,
        /// The connection.
        conn: u64,
        /// Settle at once.
        no_ack: bool,
        /// Back unless settled within this.
        lease_ms: Option<u64>,
    },
    /// `queue.purge`.
    Purge {
        /// Answer id.
        req: u64,
    },
    /// `queue.delete`.
    Delete {
        /// Answer id.
        req: u64,
        /// Only without consumers.
        if_unused: bool,
        /// Only when empty.
        if_empty: bool,
    },
    /// Ready messages and consumers.
    Info {
        /// Answer id.
        req: u64,
    },
    /// A connection is gone.
    ConnLost {
        /// The connection.
        conn: u64,
    },
    /// An acknowledgement inside a transaction starts.
    Hold {
        /// Answer id.
        req: u64,
        /// The delivery.
        id: DeliveryId,
        /// Its message id.
        msg_id: MsgId,
    },
    /// The transaction ended.
    Release {
        /// The delivery.
        id: DeliveryId,
        /// Committed.
        committed: bool,
    },
    /// The transaction ended; check the row.
    Verify {
        /// The delivery.
        id: DeliveryId,
    },
}

/// An answer to an [`Up`] frame with a `req`.
#[derive(Debug, Clone, PartialEq)]
pub enum Answer {
    /// Done (confirm, attach, hold).
    Done,
    /// A get: the delivery and the messages left.
    Got(Option<Box<(WireDelivery, u32)>>),
    /// Purged or deleted messages.
    Count(u32),
    /// Ready messages and consumers.
    Info(u32, u32),
}

/// From the owner's gateway back.
#[derive(Debug, Clone, PartialEq)]
pub enum Down {
    /// An answer.
    Reply {
        /// The [`Up`] frame's `req`.
        req: u64,
        /// What it gave.
        result: Result<Answer, MqError>,
    },
    /// A delivery to a consumer.
    Deliver {
        /// The consumer.
        consumer: u64,
        /// The delivery.
        d: WireDelivery,
    },
    /// The consumer ended on the owner's side.
    Cancelled {
        /// The consumer.
        consumer: u64,
        /// Why.
        reason: MqError,
    },
    /// Drained credit.
    Drained {
        /// The consumer.
        consumer: u64,
        /// Credit given up.
        credit: u32,
    },
    /// The owner is no longer here: the stream ends.
    Closed(MqError),
}

/// One relay stream, seen from the gateway that opened it.
#[derive(Debug)]
pub struct Link {
    /// Frames to the owner.
    pub up: mpsc::UnboundedSender<Up>,
    /// Frames from it; `None` when the stream ended.
    pub down: mpsc::UnboundedReceiver<Down>,
}

/// How a gateway reaches another one's queue owners.
#[async_trait]
pub trait PeerTransport: Send + Sync + 'static {
    /// Opens the relay stream of queue `q` to gateway `to`.
    ///
    /// # Errors
    /// 39260 when the gateway can't be reached.
    async fn open(&self, to: &str, q: QueueRef) -> Result<Link, MqError>;
}

/// Who may own a tenant's queues (chapter 50: rendezvous hashing over the
/// home data center's gateways; moves toward consumers).
#[async_trait]
pub trait Placement: Send + Sync + 'static {
    /// The gateways of the tenant's home data center that may own its
    /// queues (ready, serving the peer service). Empty: no placement; the
    /// first gateway that needs a queue owns it.
    async fn candidates(&self, tenant: u64) -> Vec<String>;
    /// The owner a move chose for `object`, and when (epoch ms).
    async fn preferred(&self, tenant: u64, object: u64) -> Option<(String, i64)>;
    /// Records a move of `object` to `gateway`.
    async fn prefer(&self, tenant: u64, object: u64, gateway: &str, at_ms: i64);
}

fn mix(mut h: u64) -> u64 {
    // splitmix64's finalizer.
    h = (h ^ (h >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h = (h ^ (h >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^ (h >> 31)
}

/// The rendezvous (highest random weight) score of a gateway for an
/// object: FNV-1a of the gateway id, mixed with the tenant and object.
/// Stable across processes and builds.
#[must_use]
pub fn score(gateway: &str, tenant: u64, object: u64) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in gateway.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    mix(h ^ mix(tenant ^ mix(object)))
}

/// The rendezvous choice among `candidates` (ties by name).
#[must_use]
pub fn rendezvous(candidates: &[String], tenant: u64, object: u64) -> Option<&String> {
    candidates.iter().max_by(|a, b| {
        score(a, tenant, object)
            .cmp(&score(b, tenant, object))
            .then_with(|| b.cmp(a))
    })
}

fn next_id() -> u64 {
    static N: AtomicU64 = AtomicU64::new(1);
    N.fetch_add(1, Ordering::Relaxed)
}

fn moved(text: impl Into<String>) -> MqError {
    MqError::new(MqCode::OwnerMoved, text)
}

// ----------------------------------------------------------------------
// The remote slot: commands to frames.
// ----------------------------------------------------------------------

type Reply<T> = oneshot::Sender<Result<T, MqError>>;

enum Pending {
    Unit(Reply<()>),
    Attach(Reply<()>, ConsumerId),
    Get(Reply<Option<(Delivery, u32)>>),
    Count(Reply<u32>),
    Info(oneshot::Sender<(u32, u32)>),
}

/// How a remote slot's stream ended.
#[derive(Debug)]
pub(crate) enum RemoteEnd {
    /// The stream closed or the owner said so: look the owner up again.
    Lost(MqError),
    /// `Cmd::Stop`, or the engine dropped the slot.
    Stopped,
}

struct Remote {
    up: mpsc::UnboundedSender<Up>,
    pending: HashMap<u64, Pending>,
    consumers: HashMap<ConsumerId, (Arc<dyn DeliverySink>, ConnKey)>,
}

impl Remote {
    fn send(&self, f: Up) -> bool {
        self.up.send(f).is_ok()
    }

    /// One command; `false` to stop.
    fn command(&mut self, c: Cmd) -> bool {
        match c {
            Cmd::Enqueue(items) => {
                for (inc, reply) in items {
                    let req = next_id();
                    let confirm = reply.is_some();
                    if let Some(r) = reply {
                        self.pending.insert(req, Pending::Unit(r));
                    }
                    let _ = self.send(Up::Enqueue {
                        req,
                        msg: WireMsg::of(&inc.msg),
                        msg_id: inc.msg_id,
                        dedup: inc.dedup,
                        deaths: inc.deaths.as_ref().map(msg::encode_deaths),
                        confirm,
                    });
                }
            }
            Cmd::Attach(spec, reply) => {
                let req = next_id();
                self.consumers
                    .insert(spec.id, (Arc::clone(&spec.sink), spec.holder.conn));
                self.pending.insert(req, Pending::Attach(reply, spec.id));
                let _ = self.send(Up::Attach {
                    req,
                    consumer: spec.id.0,
                    conn: spec.holder.conn.0,
                    no_ack: spec.no_ack,
                    exclusive: spec.exclusive,
                    priority: spec.priority,
                    held: spec.held,
                });
            }
            Cmd::Credit {
                consumer,
                credit,
                drain,
            } => {
                let _ = self.send(Up::Credit {
                    consumer: consumer.0,
                    credit,
                    drain,
                });
            }
            Cmd::Cancel(id) => {
                self.consumers.remove(&id);
                let _ = self.send(Up::Cancel { consumer: id.0 });
            }
            Cmd::Settle(h, items) => {
                let _ = self.send(Up::Settle {
                    conn: h.conn.0,
                    any: h.origin == ANY_HOLDER,
                    items,
                });
            }
            Cmd::Get {
                holder,
                no_ack,
                lease_ms,
                reply,
            } => {
                let req = next_id();
                self.pending.insert(req, Pending::Get(reply));
                let _ = self.send(Up::Get {
                    req,
                    conn: holder.conn.0,
                    no_ack,
                    lease_ms,
                });
            }
            Cmd::Purge(reply) => {
                let req = next_id();
                self.pending.insert(req, Pending::Count(reply));
                let _ = self.send(Up::Purge { req });
            }
            Cmd::Delete {
                if_unused,
                if_empty,
                reply,
            } => {
                let req = next_id();
                self.pending.insert(req, Pending::Count(reply));
                let _ = self.send(Up::Delete {
                    req,
                    if_unused,
                    if_empty,
                });
            }
            Cmd::Info(reply) => {
                let req = next_id();
                self.pending.insert(req, Pending::Info(reply));
                let _ = self.send(Up::Info { req });
            }
            Cmd::ConnLost(h) => {
                self.consumers.retain(|_, (_, c)| *c != h.conn);
                let _ = self.send(Up::ConnLost { conn: h.conn.0 });
            }
            Cmd::Hold { id, msg_id, reply } => {
                let req = next_id();
                self.pending.insert(req, Pending::Unit(reply));
                let _ = self.send(Up::Hold { req, id, msg_id });
            }
            Cmd::Release { id, committed } => {
                let _ = self.send(Up::Release { id, committed });
            }
            Cmd::Verify { id } => {
                let _ = self.send(Up::Verify { id });
            }
            // The owner reads its own catalog; a hand-off is the owner's.
            Cmd::Reconfigure(_) | Cmd::HandOff(_) => {}
            Cmd::Stop => return false,
        }
        true
    }

    /// One frame from the owner; `Some` when the stream ends.
    fn frame(&mut self, f: Down) -> Option<MqError> {
        match f {
            Down::Reply { req, result } => match self.pending.remove(&req) {
                Some(Pending::Unit(r)) => {
                    let _ = r.send(result.map(|_| ()));
                }
                Some(Pending::Attach(r, id)) => {
                    if result.is_err() {
                        self.consumers.remove(&id);
                    }
                    let _ = r.send(result.map(|_| ()));
                }
                Some(Pending::Get(r)) => {
                    let v = result.and_then(|a| match a {
                        Answer::Got(Some(b)) => Ok(Some((b.0.delivery()?, b.1))),
                        _ => Ok(None),
                    });
                    let _ = r.send(v);
                }
                Some(Pending::Count(r)) => {
                    let _ = r.send(result.map(|a| match a {
                        Answer::Count(n) => n,
                        _ => 0,
                    }));
                }
                Some(Pending::Info(r)) => {
                    // bugfix-66: no answer is no answer (dropping the
                    // sender), never a made-up "0 ready, 0 consumers".
                    if let Ok(Answer::Info(m, c)) = result {
                        let _ = r.send((m, c));
                    }
                }
                None => {}
            },
            Down::Deliver { consumer, d } => {
                let id = ConsumerId(consumer);
                let did = d.id;
                let taken = match (self.consumers.get(&id), d.delivery()) {
                    (Some((sink, _)), Ok(d)) => sink.deliver(id, d),
                    (_, Err(e)) => {
                        tracing::warn!(error = %e, "unreadable relayed delivery");
                        false
                    }
                    (None, _) => false,
                };
                if !taken {
                    // Nobody here takes it (the consumer or its
                    // connection is gone): back to the queue at once, and
                    // the consumer ends at the owner.
                    let conn = self.consumers.remove(&id).map_or(0, |(_, c)| c.0);
                    let _ = self.send(Up::Cancel { consumer });
                    let _ = self.send(Up::Settle {
                        conn,
                        any: conn == 0,
                        items: vec![(did, Settlement::Requeue { failed: false })],
                    });
                }
            }
            Down::Cancelled { consumer, reason } => {
                let id = ConsumerId(consumer);
                if let Some((sink, _)) = self.consumers.remove(&id) {
                    sink.cancelled(id, reason);
                }
            }
            Down::Drained { consumer, credit } => {
                let id = ConsumerId(consumer);
                if let Some((sink, _)) = self.consumers.get(&id) {
                    sink.drained(id, credit);
                }
            }
            Down::Closed(e) => return Some(e),
        }
        None
    }

    /// The stream ended: answers in flight fail, consumers re-attach.
    fn lost(&mut self, e: &MqError) {
        let err = moved(format!("the queue's owner gateway went away: {}", e.text));
        for (_, p) in self.pending.drain() {
            match p {
                Pending::Unit(r) | Pending::Attach(r, _) => {
                    let _ = r.send(Err(err.clone()));
                }
                Pending::Get(r) => {
                    let _ = r.send(Err(err.clone()));
                }
                Pending::Count(r) => {
                    let _ = r.send(Err(err.clone()));
                }
                // bugfix-66: dropped, unanswered.
                Pending::Info(r) => drop(r),
            }
        }
        for (id, (sink, _)) in self.consumers.drain() {
            sink.cancelled(id, err.clone());
        }
    }
}

/// Runs a remote slot over `link` until the stream ends or the slot stops.
pub(crate) async fn remote(link: Link, rx: &mut mpsc::UnboundedReceiver<Cmd>) -> RemoteEnd {
    let Link { up, mut down } = link;
    let mut r = Remote {
        up,
        pending: HashMap::new(),
        consumers: HashMap::new(),
    };
    loop {
        tokio::select! {
            c = rx.recv() => match c {
                None => {
                    r.lost(&moved("the slot was dropped"));
                    return RemoteEnd::Stopped;
                }
                Some(c) => {
                    if !r.command(c) {
                        r.lost(&moved("stopped"));
                        return RemoteEnd::Stopped;
                    }
                }
            },
            f = down.recv() => {
                let end = match f {
                    None => Some(moved("the relay stream ended")),
                    Some(f) => r.frame(f),
                };
                if let Some(e) = end {
                    r.lost(&e);
                    return RemoteEnd::Lost(e);
                }
            }
        }
    }
}

// ----------------------------------------------------------------------
// The owner's side: frames to commands.
// ----------------------------------------------------------------------

/// Delivers to a consumer behind a relay stream.
struct StreamSink {
    down: mpsc::UnboundedSender<Down>,
}

impl DeliverySink for StreamSink {
    fn deliver(&self, consumer: ConsumerId, d: Delivery) -> bool {
        self.down
            .send(Down::Deliver {
                consumer: consumer.0,
                d: WireDelivery::of(&d),
            })
            .is_ok()
    }
    fn cancelled(&self, consumer: ConsumerId, reason: MqError) {
        let _ = self.down.send(Down::Cancelled {
            consumer: consumer.0,
            reason,
        });
    }
    fn drained(&self, consumer: ConsumerId, credit: u32) {
        let _ = self.down.send(Down::Drained {
            consumer: consumer.0,
            credit,
        });
    }
}

fn reply_of<T: Send + 'static>(
    req: u64,
    rx: oneshot::Receiver<Result<T, MqError>>,
    f: impl FnOnce(T) -> Answer + Send + 'static,
) -> BoxFuture<'static, Down> {
    Box::pin(async move {
        let result = match rx.await {
            Ok(r) => r.map(f),
            Err(_) => Err(moved("owner stopped")),
        };
        Down::Reply { req, result }
    })
}

impl QueueEngine {
    /// Serves one relay stream on the owner's gateway: frames from `up`
    /// become commands to this gateway's owner of `q` (taken over if
    /// needed); answers, deliveries and cancels go to `down`. Ends when
    /// the stream ends (the opener's connections there are then lost:
    /// their deliveries return) or the owner leaves this gateway
    /// ([`Down::Closed`]).
    pub async fn serve_relay(
        &self,
        q: QueueRef,
        mut up: mpsc::UnboundedReceiver<Up>,
        down: mpsc::UnboundedSender<Down>,
    ) {
        let close = |e: MqError| {
            let _ = down.send(Down::Closed(e));
        };
        let r = VhostRef {
            tenant: q.tenant,
            db_id: q.db_id,
            db: q.db.clone(),
            name: q.vhost.clone(),
        };
        let v = self.vshared(&r);
        let mut def = v.queue_by_id(q.queue_id);
        if def.is_none() {
            if let Err(e) = self.inner.deps.catalog.refresh(&r).await {
                close(e);
                return;
            }
            def = v.queue_by_id(q.queue_id);
        }
        let Some(def) = def else {
            close(MqError::not_found(format!(
                "no queue '{}' in vhost '{}'",
                q.queue, q.vhost
            )));
            return;
        };
        let (h, done) = match v.serve_slot(&def).await {
            Ok(x) => x,
            Err(e) => {
                close(e);
                return;
            }
        };
        let link = next_id();
        let from: Arc<str> = Arc::from(q.from.as_str());
        let holder = |conn: u64| Holder {
            origin: link,
            conn: ConnKey(conn),
        };
        let mut conns: HashSet<u64> = HashSet::new();
        let mut waits: FuturesUnordered<BoxFuture<'static, Down>> = FuturesUnordered::new();
        let temp_here = def.temp_on.is_some();
        let sink: Arc<dyn DeliverySink> = Arc::new(StreamSink { down: down.clone() });
        loop {
            tokio::select! {
                f = up.recv() => {
                    let Some(f) = f else { break };
                    let cmd = match f {
                        Up::Enqueue { req, msg, msg_id, dedup, deaths, confirm } => {
                            let m = match msg.message() {
                                Ok(m) => Arc::new(m),
                                Err(e) => {
                                    if confirm {
                                        let _ = down.send(Down::Reply { req, result: Err(e) });
                                    }
                                    continue;
                                }
                            };
                            let reply = confirm.then(|| {
                                let (tx, rx) = oneshot::channel();
                                waits.push(reply_of(req, rx, |()| Answer::Done));
                                tx
                            });
                            Cmd::Enqueue(vec![(
                                Incoming {
                                    msg: m,
                                    msg_id,
                                    dedup,
                                    deaths: deaths.as_deref().and_then(msg::decode_deaths),
                                },
                                reply,
                            )])
                        }
                        Up::Attach { req, consumer, conn, no_ack, exclusive, priority, held } => {
                            conns.insert(conn);
                            let (tx, rx) = oneshot::channel();
                            waits.push(reply_of(req, rx, |()| Answer::Done));
                            Cmd::Attach(
                                ConsumerSpec {
                                    id: ConsumerId(consumer),
                                    holder: holder(conn),
                                    sink: Arc::clone(&sink),
                                    no_ack,
                                    exclusive,
                                    priority,
                                    held,
                                    from: Some(Arc::clone(&from)),
                                },
                                tx,
                            )
                        }
                        Up::Credit { consumer, credit, drain } => Cmd::Credit {
                            consumer: ConsumerId(consumer),
                            credit,
                            drain,
                        },
                        Up::Cancel { consumer } => Cmd::Cancel(ConsumerId(consumer)),
                        Up::Settle { conn, any, items } => {
                            let mut h = holder(conn);
                            if any {
                                h.origin = ANY_HOLDER;
                            }
                            Cmd::Settle(h, items)
                        }
                        Up::Get { req, conn, no_ack, lease_ms } => {
                            conns.insert(conn);
                            let (tx, rx) = oneshot::channel();
                            waits.push(reply_of(req, rx, |g: Option<(Delivery, u32)>| {
                                Answer::Got(g.map(|(d, n)| Box::new((WireDelivery::of(&d), n))))
                            }));
                            Cmd::Get { holder: holder(conn), no_ack, lease_ms, reply: tx }
                        }
                        Up::Purge { req } => {
                            let (tx, rx) = oneshot::channel();
                            waits.push(reply_of(req, rx, Answer::Count));
                            Cmd::Purge(tx)
                        }
                        Up::Delete { req, if_unused, if_empty } => {
                            let (tx, rx) = oneshot::channel();
                            let (v2, d2) = (Arc::clone(&v), Arc::clone(&def));
                            waits.push(Box::pin(async move {
                                let result = match rx.await {
                                    Ok(Ok(n)) => {
                                        // A temporary queue lives in this
                                        // gateway's model: it goes here.
                                        lock(&v2.owners).remove(&d2.id);
                                        if temp_here {
                                            v2.forget_queue(&d2).await;
                                        }
                                        Ok(Answer::Count(n))
                                    }
                                    Ok(Err(e)) => Err(e),
                                    Err(_) => Err(moved("owner stopped")),
                                };
                                Down::Reply { req, result }
                            }));
                            Cmd::Delete { if_unused, if_empty, reply: tx }
                        }
                        Up::Info { req } => {
                            let (tx, rx) = oneshot::channel();
                            waits.push(Box::pin(async move {
                                // bugfix-66: an owner that didn't answer
                                // is an error, not "0 ready".
                                let result = match rx.await {
                                    Ok((m, c)) => Ok(Answer::Info(m, c)),
                                    Err(_) => Err(moved("owner stopped")),
                                };
                                Down::Reply { req, result }
                            }));
                            Cmd::Info(tx)
                        }
                        Up::ConnLost { conn } => {
                            conns.remove(&conn);
                            Cmd::ConnLost(holder(conn))
                        }
                        Up::Hold { req, id, msg_id } => {
                            let (tx, rx) = oneshot::channel();
                            waits.push(reply_of(req, rx, |()| Answer::Done));
                            Cmd::Hold { id, msg_id, reply: tx }
                        }
                        Up::Release { id, committed } => Cmd::Release { id, committed },
                        Up::Verify { id } => Cmd::Verify { id },
                    };
                    if !h.send(cmd) {
                        close(moved(format!("queue '{}': its owner stopped here", q.queue)));
                        break;
                    }
                }
                Some(d) = waits.next(), if !waits.is_empty() => {
                    let _ = down.send(d);
                }
                () = done.cancelled() => {
                    // Answers already decided go out first.
                    while let Some(d) = waits.next().await {
                        let _ = down.send(d);
                    }
                    close(moved(format!("queue '{}': the owner left this gateway", q.queue)));
                    break;
                }
            }
        }
        for c in conns {
            let _ = h.send(Cmd::ConnLost(holder(c)));
        }
    }
}

// ----------------------------------------------------------------------
// In-memory transport and placement (tests, the simulation).
// ----------------------------------------------------------------------

/// Engines in one process reaching each other by gateway id.
#[derive(Clone, Default)]
pub struct MemPeers {
    engines: Arc<Mutex<HashMap<String, std::sync::Weak<crate::engine::Inner>>>>,
    down: Arc<Mutex<HashSet<String>>>,
    links: Arc<Mutex<Vec<(String, mpsc::UnboundedSender<Down>)>>>,
}

impl std::fmt::Debug for MemPeers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemPeers").finish_non_exhaustive()
    }
}

impl MemPeers {
    /// No engines yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Makes `engine` reachable as gateway `id`.
    pub fn register(&self, id: &str, engine: &QueueEngine) {
        lock(&self.engines).insert(id.to_owned(), Arc::downgrade(&engine.inner));
        lock(&self.down).remove(id);
    }

    /// Gateway `id` becomes unreachable, and every stream to it breaks
    /// (a killed process or a cut network).
    pub fn cut(&self, id: &str) {
        lock(&self.down).insert(id.to_owned());
        lock(&self.links).retain(|(to, _)| to != id);
    }

    /// The gateways reachable now.
    #[must_use]
    pub fn live(&self) -> Vec<String> {
        let down = lock(&self.down).clone();
        let mut v: Vec<String> = lock(&self.engines)
            .iter()
            .filter(|(k, e)| !down.contains(*k) && e.strong_count() > 0)
            .map(|(k, _)| k.clone())
            .collect();
        v.sort();
        v
    }
}

#[async_trait]
impl PeerTransport for MemPeers {
    async fn open(&self, to: &str, q: QueueRef) -> Result<Link, MqError> {
        if lock(&self.down).contains(to) {
            return Err(moved(format!("gateway {to} can't be reached")));
        }
        let Some(inner) = lock(&self.engines)
            .get(to)
            .and_then(std::sync::Weak::upgrade)
        else {
            return Err(moved(format!("gateway {to} can't be reached")));
        };
        let engine = QueueEngine { inner };
        let (up_tx, up_rx) = mpsc::unbounded_channel();
        let (down_tx, down_rx) = mpsc::unbounded_channel();
        // The opener's end of `down` is relayed through a forwarder whose
        // sender `cut` can drop, so a cut breaks the stream at once.
        let (fwd_tx, fwd_rx) = mpsc::unbounded_channel::<Down>();
        lock(&self.links).push((to.to_owned(), fwd_tx.clone()));
        let (tx_out, rx_out) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut down_rx = down_rx;
            let mut fwd_rx = fwd_rx;
            drop(fwd_tx);
            loop {
                tokio::select! {
                    d = down_rx.recv() => match d {
                        Some(d) => { if tx_out.send(d).is_err() { break; } }
                        None => break,
                    },
                    d = fwd_rx.recv() => if d.is_none() { break },
                }
            }
        });
        let peers = self.clone();
        let to2 = to.to_owned();
        // Frames up are dropped once the gateway is cut.
        let (up_out, mut up_in) = mpsc::unbounded_channel::<Up>();
        tokio::spawn(async move {
            while let Some(f) = up_in.recv().await {
                if lock(&peers.down).contains(&to2) || up_tx.send(f).is_err() {
                    break;
                }
            }
        });
        tokio::spawn(async move { engine.serve_relay(q, up_rx, down_tx).await });
        Ok(Link {
            up: up_out,
            down: rx_out,
        })
    }
}

/// [`Placement`] over [`MemPeers`]' live gateways.
#[derive(Clone, Default)]
pub struct MemPlacement {
    peers: MemPeers,
    preferred: Arc<Mutex<HashMap<(u64, u64), (String, i64)>>>,
}

impl std::fmt::Debug for MemPlacement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemPlacement").finish_non_exhaustive()
    }
}

impl MemPlacement {
    /// Placement over `peers`.
    #[must_use]
    pub fn new(peers: MemPeers) -> Self {
        Self {
            peers,
            preferred: Arc::default(),
        }
    }
}

#[async_trait]
impl Placement for MemPlacement {
    async fn candidates(&self, _tenant: u64) -> Vec<String> {
        self.peers.live()
    }
    async fn preferred(&self, tenant: u64, object: u64) -> Option<(String, i64)> {
        lock(&self.preferred).get(&(tenant, object)).cloned()
    }
    async fn prefer(&self, tenant: u64, object: u64, gateway: &str, at_ms: i64) {
        lock(&self.preferred).insert((tenant, object), (gateway.to_owned(), at_ms));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rendezvous_is_stable_and_spreads() {
        let gws: Vec<String> = (1..=4).map(|i| format!("gw-{i}")).collect();
        let mut count = HashMap::new();
        for obj in 0..4000u64 {
            let w = rendezvous(&gws, 7, obj).cloned().unwrap_or_default();
            assert_eq!(rendezvous(&gws, 7, obj), Some(&w));
            *count.entry(w).or_insert(0u32) += 1;
        }
        for g in &gws {
            let n = count.get(g).copied().unwrap_or(0);
            assert!((800..1200).contains(&n), "{g}: {n}");
        }
        // Removing a gateway moves only its own objects.
        let three: Vec<String> = gws[..3].to_vec();
        for obj in 0..4000u64 {
            let before = rendezvous(&gws, 7, obj).cloned().unwrap_or_default();
            let after = rendezvous(&three, 7, obj).cloned().unwrap_or_default();
            if before != "gw-4" {
                assert_eq!(before, after);
            }
        }
    }

    #[test]
    fn messages_and_deliveries_round_trip() {
        use sprig_proto_amqp::v091::codec::FieldValue;
        use sprig_proto_amqp::v091::props::BasicProperties;
        let p = BasicProperties {
            priority: Some(3),
            expiration: Some("500".into()),
            ..BasicProperties::default()
        };
        let mut b = BytesMut::new();
        p.encode(&mut b);
        let m = msg::rebuild(
            0,
            "x".into(),
            "k".into(),
            b.freeze(),
            Bytes::from_static(b"hi"),
        )
        .unwrap_or_else(|_| unreachable!());
        let w = WireMsg::of(&m);
        assert_eq!(w.message().ok().as_ref(), Some(&m));
        let mut dead = m.clone();
        dead.info.ttl_ms = None;
        assert_eq!(WireMsg::of(&dead).message().ok(), Some(dead));
        let d = Delivery {
            id: DeliveryId {
                queue: 9,
                lane: 0,
                pclass: 2,
                seq: 77,
            },
            queue: "q".into(),
            message: Arc::new(m),
            redelivered: true,
            delivery_count: 3,
            annotations: FieldTable::from([("x-sprig-delivery", FieldValue::str("q1.x"))]),
        };
        assert_eq!(WireDelivery::of(&d).delivery().ok(), Some(d));
    }
}
