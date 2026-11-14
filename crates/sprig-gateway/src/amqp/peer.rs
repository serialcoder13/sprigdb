//! The queue relay between gateways, toward each queue's owner:
//! `sprig.internal.v1.QueuePeer` on the peer service (7403, mutual TLS
//! from the internal CA; plaintext on loopback in development, like every
//! peer call).
//!
//! - [`QueuePeerService`]: this gateway's side of other gateways' relay
//!   streams (its queue owners serve them) and of their direct reply-to
//!   replies.
//! - [`GatewayRelay`]: `sprig-queues`' [`PeerTransport`] (a stream per
//!   queue to the owner's gateway, found in the registry) and
//!   `sprig-proto-amqp`'s [`ReplyRelay`] (a reply to the requester's
//!   gateway, found by its slot).
//! - [`GatewayPlacement`]: owner candidates are the ready gateways of the
//!   tenant's home DC that serve the peer service; a move's choice is kept
//!   in `/sprig/dc/<home>/queue-placement/<tenant>/<object>`.
//! - [`EtcdTemps`]: the temporary-queue registry,
//!   `/sprig/dc/<dc>/queue-temps/<tenant>/<db>/<gateway>` on this
//!   gateway's lease, watched by every gateway.
//! - [`reply_key`]: the direct reply-to key the DC's gateways share
//!   (`/sprig/dc/<dc>/amqp-reply-key`), so any of them checks any name.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use sprig_api::proto::internal::queue_peer_client::QueuePeerClient;
use sprig_api::proto::internal::queue_peer_server::{QueuePeer, QueuePeerServer};
use sprig_api::proto::internal::{self as pb, queue_down, queue_up};
use sprig_common::CancellationToken;
use sprig_common::ids::{DcId, TenantId};
use sprig_meta::{
    Cmp, Consistency, Key, LeaseId, ListOpts, MetaStore, Op, Tier, TierView, Txn, WatchEvent,
};
use sprig_proto_amqp::backend::{DeadLetterReason, Settlement};
use sprig_proto_amqp::error::{MqCode, MqError};
use sprig_proto_amqp::message::{DeliveryId, Message};
use sprig_proto_amqp::reply_to::{DirectReplyTo, ReplyRelay};
use sprig_queues::QueueEngine;
use sprig_queues::engine::{TempRegistry, VhostRef};
use sprig_queues::relay::{
    Answer, Down, Link, PeerTransport, Placement, QueueRef, Up, WireDelivery, WireMsg,
};
use sprig_queues::topology::TempSet;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::peer::PeerTls;
use crate::registry::{self, GatewayState};

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn unreachable_owner(to: &str, e: impl std::fmt::Display) -> MqError {
    MqError::new(
        MqCode::OwnerMoved,
        format!("the queue's owner gateway {to} can't be reached: {e}"),
    )
}

// ----------------------------------------------------------------------
// Frames: sprig-queues' relay frames and the protobuf messages.
// ----------------------------------------------------------------------

fn msg_pb(w: &WireMsg) -> pb::QueueMessage {
    pb::QueueMessage {
        fmt: u32::from(w.fmt),
        exchange: w.exchange.clone(),
        routing_key: w.routing_key.clone(),
        props: w.props.to_vec(),
        body: w.body.to_vec(),
        no_ttl: w.no_ttl,
    }
}

fn msg_of(m: pb::QueueMessage) -> WireMsg {
    WireMsg {
        fmt: u8::try_from(m.fmt).unwrap_or(0),
        exchange: m.exchange,
        routing_key: m.routing_key,
        props: Bytes::from(m.props),
        body: Bytes::from(m.body),
        no_ttl: m.no_ttl,
    }
}

fn id_pb(d: DeliveryId) -> pb::QueueDeliveryId {
    pb::QueueDeliveryId {
        queue: d.queue,
        lane: u32::from(d.lane),
        pclass: u32::from(d.pclass),
        seq: d.seq,
    }
}

fn id_of(d: Option<pb::QueueDeliveryId>) -> DeliveryId {
    let d = d.unwrap_or_default();
    DeliveryId {
        queue: d.queue,
        lane: u16::try_from(d.lane).unwrap_or(0),
        pclass: u8::try_from(d.pclass).unwrap_or(0),
        seq: d.seq,
    }
}

fn msg_id_of(b: &[u8]) -> [u8; 16] {
    <[u8; 16]>::try_from(b).unwrap_or([0; 16])
}

fn err_pb(e: &MqError) -> pb::QueueError {
    pb::QueueError {
        code: e.code.number(),
        reply: u32::from(e.reply),
        text: e.text.clone(),
    }
}

fn err_of(e: Option<pb::QueueError>) -> MqError {
    let e = e.unwrap_or_default();
    let code = MqCode::from_number(e.code).unwrap_or(MqCode::OwnerMoved);
    MqError::new(code, e.text).with_reply(u16::try_from(e.reply).unwrap_or(0))
}

fn how_pb(s: Settlement) -> u32 {
    match s {
        Settlement::Ack => 0,
        Settlement::Requeue { failed: false } => 1,
        Settlement::Requeue { failed: true } => 2,
        Settlement::DeadLetter(r) => {
            10 + match r {
                DeadLetterReason::Rejected => 0,
                DeadLetterReason::Expired => 1,
                DeadLetterReason::Maxlen => 2,
                DeadLetterReason::DeliveryLimit => 3,
            }
        }
    }
}

fn how_of(n: u32) -> Settlement {
    match n {
        0 => Settlement::Ack,
        1 => Settlement::Requeue { failed: false },
        2 => Settlement::Requeue { failed: true },
        11 => Settlement::DeadLetter(DeadLetterReason::Expired),
        12 => Settlement::DeadLetter(DeadLetterReason::Maxlen),
        13 => Settlement::DeadLetter(DeadLetterReason::DeliveryLimit),
        _ => Settlement::DeadLetter(DeadLetterReason::Rejected),
    }
}

fn delivery_pb(d: &WireDelivery) -> pb::QueueDelivery {
    pb::QueueDelivery {
        id: Some(id_pb(d.id)),
        queue: d.queue.clone(),
        message: Some(msg_pb(&d.msg)),
        redelivered: d.redelivered,
        delivery_count: d.delivery_count,
        annotations: d.annotations.to_vec(),
    }
}

fn delivery_of(d: pb::QueueDelivery) -> WireDelivery {
    WireDelivery {
        id: id_of(d.id),
        queue: d.queue,
        msg: msg_of(d.message.unwrap_or_default()),
        redelivered: d.redelivered,
        delivery_count: d.delivery_count,
        annotations: Bytes::from(d.annotations),
    }
}

/// An [`Up`] frame as protobuf.
#[must_use]
#[allow(clippy::too_many_lines)] // one arm per frame
pub fn up_pb(u: Up) -> pb::QueueUp {
    use queue_up::Frame;
    let frame = match u {
        Up::Enqueue {
            req,
            msg,
            msg_id,
            dedup,
            deaths,
            confirm,
        } => Frame::Enqueue(pb::QueueEnqueue {
            req,
            message: Some(msg_pb(&msg)),
            msg_id: msg_id.to_vec(),
            dedup,
            deaths: deaths.map(|d| d.to_vec()).unwrap_or_default(),
            confirm,
        }),
        Up::Attach {
            req,
            consumer,
            conn,
            no_ack,
            exclusive,
            priority,
            held,
        } => Frame::Attach(pb::QueueAttach {
            req,
            consumer,
            conn,
            no_ack,
            exclusive,
            priority,
            held: held.into_iter().map(id_pb).collect(),
        }),
        Up::Credit {
            consumer,
            credit,
            drain,
        } => Frame::Credit(pb::QueueCredit {
            consumer,
            credit,
            drain,
        }),
        Up::Cancel { consumer } => Frame::Cancel(consumer),
        Up::Settle { conn, any, items } => Frame::Settle(pb::QueueSettle {
            conn,
            any,
            items: items
                .into_iter()
                .map(|(id, how)| pb::QueueSettlement {
                    id: Some(id_pb(id)),
                    how: how_pb(how),
                })
                .collect(),
        }),
        Up::Get {
            req,
            conn,
            no_ack,
            lease_ms,
        } => Frame::Get(pb::QueueGet {
            req,
            conn,
            no_ack,
            has_lease: lease_ms.is_some(),
            lease_ms: lease_ms.unwrap_or(0),
        }),
        Up::Purge { req } => Frame::Op(pb::QueueOp {
            req,
            op: 1,
            ..pb::QueueOp::default()
        }),
        Up::Delete {
            req,
            if_unused,
            if_empty,
        } => Frame::Op(pb::QueueOp {
            req,
            op: 2,
            if_unused,
            if_empty,
        }),
        Up::Info { req } => Frame::Op(pb::QueueOp {
            req,
            op: 3,
            ..pb::QueueOp::default()
        }),
        Up::ConnLost { conn } => Frame::ConnLost(conn),
        Up::Hold { req, id, msg_id } => Frame::Hold(pb::QueueHold {
            req,
            id: Some(id_pb(id)),
            msg_id: msg_id.to_vec(),
        }),
        Up::Release { id, committed } => Frame::Release(pb::QueueRelease {
            id: Some(id_pb(id)),
            committed,
            verify: false,
        }),
        Up::Verify { id } => Frame::Release(pb::QueueRelease {
            id: Some(id_pb(id)),
            committed: false,
            verify: true,
        }),
    };
    pb::QueueUp { frame: Some(frame) }
}

/// A protobuf frame as an [`Up`] frame (`None`: `open` or empty).
#[must_use]
pub fn up_of(u: pb::QueueUp) -> Option<Up> {
    use queue_up::Frame;
    Some(match u.frame? {
        Frame::Open(_) => return None,
        Frame::Enqueue(e) => Up::Enqueue {
            req: e.req,
            msg: msg_of(e.message.unwrap_or_default()),
            msg_id: msg_id_of(&e.msg_id),
            dedup: e.dedup,
            deaths: (!e.deaths.is_empty()).then(|| Bytes::from(e.deaths)),
            confirm: e.confirm,
        },
        Frame::Attach(a) => Up::Attach {
            req: a.req,
            consumer: a.consumer,
            conn: a.conn,
            no_ack: a.no_ack,
            exclusive: a.exclusive,
            priority: a.priority,
            held: a.held.into_iter().map(|d| id_of(Some(d))).collect(),
        },
        Frame::Credit(c) => Up::Credit {
            consumer: c.consumer,
            credit: c.credit,
            drain: c.drain,
        },
        Frame::Cancel(consumer) => Up::Cancel { consumer },
        Frame::Settle(s) => Up::Settle {
            conn: s.conn,
            any: s.any,
            items: s
                .items
                .into_iter()
                .map(|i| (id_of(i.id), how_of(i.how)))
                .collect(),
        },
        Frame::Get(g) => Up::Get {
            req: g.req,
            conn: g.conn,
            no_ack: g.no_ack,
            lease_ms: g.has_lease.then_some(g.lease_ms),
        },
        Frame::Op(o) => match o.op {
            1 => Up::Purge { req: o.req },
            2 => Up::Delete {
                req: o.req,
                if_unused: o.if_unused,
                if_empty: o.if_empty,
            },
            _ => Up::Info { req: o.req },
        },
        Frame::ConnLost(conn) => Up::ConnLost { conn },
        Frame::Hold(h) => Up::Hold {
            req: h.req,
            id: id_of(h.id),
            msg_id: msg_id_of(&h.msg_id),
        },
        Frame::Release(r) if r.verify => Up::Verify { id: id_of(r.id) },
        Frame::Release(r) => Up::Release {
            id: id_of(r.id),
            committed: r.committed,
        },
    })
}

/// A [`Down`] frame as protobuf.
#[must_use]
pub fn down_pb(d: Down) -> pb::QueueDown {
    use queue_down::Frame;
    let frame = match d {
        Down::Reply { req, result } => {
            let mut r = pb::QueueReply {
                req,
                ..pb::QueueReply::default()
            };
            match result {
                Err(e) => r.error = Some(err_pb(&e)),
                Ok(Answer::Done) => r.kind = 0,
                Ok(Answer::Got(g)) => {
                    r.kind = 1;
                    if let Some(b) = g {
                        r.delivery = Some(delivery_pb(&b.0));
                        r.count = b.1;
                    }
                }
                Ok(Answer::Count(n)) => {
                    r.kind = 2;
                    r.count = n;
                }
                Ok(Answer::Info(m, c)) => {
                    r.kind = 3;
                    r.count = m;
                    r.consumers = c;
                }
            }
            Frame::Reply(r)
        }
        Down::Deliver { consumer, d } => Frame::Deliver(pb::QueueDeliver {
            consumer,
            delivery: Some(delivery_pb(&d)),
        }),
        Down::Cancelled { consumer, reason } => Frame::Cancelled(pb::QueueCancelled {
            consumer,
            reason: Some(err_pb(&reason)),
        }),
        Down::Drained { consumer, credit } => Frame::Drained(pb::QueueDrained { consumer, credit }),
        Down::Closed(e) => Frame::Closed(err_pb(&e)),
    };
    pb::QueueDown { frame: Some(frame) }
}

/// A protobuf frame as a [`Down`] frame.
#[must_use]
pub fn down_of(d: pb::QueueDown) -> Option<Down> {
    use queue_down::Frame;
    Some(match d.frame? {
        Frame::Reply(r) => Down::Reply {
            req: r.req,
            result: match r.error {
                Some(e) => Err(err_of(Some(e))),
                None => Ok(match r.kind {
                    1 => Answer::Got(r.delivery.map(|d| Box::new((delivery_of(d), r.count)))),
                    2 => Answer::Count(r.count),
                    3 => Answer::Info(r.count, r.consumers),
                    _ => Answer::Done,
                }),
            },
        },
        Frame::Deliver(d) => Down::Deliver {
            consumer: d.consumer,
            d: delivery_of(d.delivery.unwrap_or_default()),
        },
        Frame::Cancelled(c) => Down::Cancelled {
            consumer: c.consumer,
            reason: err_of(c.reason),
        },
        Frame::Drained(d) => Down::Drained {
            consumer: d.consumer,
            credit: d.credit,
        },
        Frame::Closed(e) => Down::Closed(err_of(Some(e))),
    })
}

fn open_pb(q: &QueueRef) -> pb::QueueUp {
    pb::QueueUp {
        frame: Some(queue_up::Frame::Open(pb::QueueOpen {
            tenant_id: q.tenant,
            db_id: q.db_id,
            db: q.db.clone(),
            vhost: q.vhost.clone(),
            queue_id: q.queue_id,
            queue: q.queue.clone(),
            from_gateway: q.from.clone(),
        })),
    }
}

// ----------------------------------------------------------------------
// The service.
// ----------------------------------------------------------------------

type DownStream = Pin<Box<dyn Stream<Item = Result<pb::QueueDown, tonic::Status>> + Send>>;

/// This gateway's side of the queue relay.
#[derive(Clone)]
pub struct QueuePeerService {
    engine: Arc<QueueEngine>,
    replies: Arc<DirectReplyTo>,
    slot: u32,
}

impl std::fmt::Debug for QueuePeerService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueuePeerService")
            .field("slot", &self.slot)
            .finish_non_exhaustive()
    }
}

impl QueuePeerService {
    /// The service over this gateway's engine and direct reply-to
    /// requesters.
    #[must_use]
    pub fn new(engine: Arc<QueueEngine>, replies: Arc<DirectReplyTo>, slot: u32) -> Self {
        Self {
            engine,
            replies,
            slot,
        }
    }

    /// The tonic service.
    #[must_use]
    pub fn server(self) -> QueuePeerServer<Self> {
        QueuePeerServer::new(self)
            .max_decoding_message_size(64 << 20)
            .max_encoding_message_size(64 << 20)
    }
}

#[async_trait]
impl QueuePeer for QueuePeerService {
    type RelayStream = DownStream;

    async fn relay(
        &self,
        req: tonic::Request<tonic::Streaming<pb::QueueUp>>,
    ) -> Result<tonic::Response<Self::RelayStream>, tonic::Status> {
        let mut inbound = req.into_inner();
        let first = inbound
            .message()
            .await?
            .and_then(|f| f.frame)
            .ok_or_else(|| tonic::Status::invalid_argument("no open frame"))?;
        let queue_up::Frame::Open(o) = first else {
            return Err(tonic::Status::invalid_argument(
                "the first frame must be open",
            ));
        };
        let q = QueueRef {
            tenant: o.tenant_id,
            db_id: o.db_id,
            db: o.db,
            vhost: o.vhost,
            queue_id: o.queue_id,
            queue: o.queue,
            from: o.from_gateway,
        };
        let (up_tx, up_rx) = tokio::sync::mpsc::unbounded_channel();
        let (down_tx, down_rx) = tokio::sync::mpsc::unbounded_channel::<Down>();
        tokio::spawn(async move {
            while let Ok(Some(f)) = inbound.message().await {
                if let Some(u) = up_of(f)
                    && up_tx.send(u).is_err()
                {
                    break;
                }
            }
        });
        let engine = Arc::clone(&self.engine);
        tokio::spawn(async move { engine.serve_relay(q, up_rx, down_tx).await });
        let mut down_rx = down_rx;
        let out = futures::stream::poll_fn(move |cx| {
            down_rx.poll_recv(cx).map(|o| o.map(|d| Ok(down_pb(d))))
        });
        Ok(tonic::Response::new(Box::pin(out)))
    }

    async fn reply(
        &self,
        req: tonic::Request<pb::QueueReplyRequest>,
    ) -> Result<tonic::Response<pb::QueueReplyAck>, tonic::Status> {
        let r = req.into_inner();
        if r.slot != self.slot {
            return Ok(tonic::Response::new(pb::QueueReplyAck { delivered: false }));
        }
        let m = msg_of(r.message.unwrap_or_default())
            .message()
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        // The sending gateway checked the replier's tenant against the
        // name (`DirectReplyTo::deliver`).
        let delivered = self.replies.deliver_relayed(&r.name, m) == Some(true);
        Ok(tonic::Response::new(pb::QueueReplyAck { delivered }))
    }
}

// ----------------------------------------------------------------------
// The client side.
// ----------------------------------------------------------------------

struct RelayInner {
    store: Arc<dyn MetaStore>,
    tls: Option<PeerTls>,
    channels: Mutex<HashMap<String, Channel>>,
}

/// Other gateways' queue owners and direct reply-to requesters, over the
/// peer service.
#[derive(Clone)]
pub struct GatewayRelay {
    inner: Arc<RelayInner>,
}

impl std::fmt::Debug for GatewayRelay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayRelay")
            .field("mtls", &self.inner.tls.is_some())
            .finish_non_exhaustive()
    }
}

impl GatewayRelay {
    /// Gateways found in `store` (this DC's tier), reached with `tls`
    /// (`None`: plaintext, development on loopback).
    #[must_use]
    pub fn new(store: Arc<dyn MetaStore>, tls: Option<PeerTls>) -> Self {
        Self {
            inner: Arc::new(RelayInner {
                store,
                tls,
                channels: Mutex::new(HashMap::new()),
            }),
        }
    }
}

impl RelayInner {
    /// A channel to gateway `rec` for long-lived streams: no request
    /// timeout, HTTP/2 keepalive to notice a dead peer.
    fn channel(&self, rec: &registry::GatewayRecord) -> Result<Channel, String> {
        let port = rec
            .listeners
            .get("peer")
            .ok_or_else(|| format!("gateway {} serves no peer service", rec.gateway_id))?;
        let scheme = if self.tls.is_some() { "https" } else { "http" };
        let uri = format!("{scheme}://{}:{port}", rec.addr);
        if let Some(c) = lock(&self.channels).get(&uri) {
            return Ok(c.clone());
        }
        let mut ep = Endpoint::from_shared(uri.clone())
            .map_err(|e| e.to_string())?
            .connect_timeout(Duration::from_secs(2))
            .http2_keep_alive_interval(Duration::from_secs(5))
            .keep_alive_timeout(Duration::from_secs(5))
            .keep_alive_while_idle(true)
            .tcp_nodelay(true);
        if let Some(t) = &self.tls {
            let cfg = ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(&t.ca_pem))
                .identity(Identity::from_pem(&t.cert_pem, &t.key_pem))
                .domain_name(rec.addr.clone());
            ep = ep.tls_config(cfg).map_err(|e| e.to_string())?;
        }
        let ch = ep.connect_lazy();
        lock(&self.channels).insert(uri, ch.clone());
        Ok(ch)
    }

    fn client(&self, rec: &registry::GatewayRecord) -> Result<QueuePeerClient<Channel>, String> {
        Ok(QueuePeerClient::new(self.channel(rec)?)
            .max_decoding_message_size(64 << 20)
            .max_encoding_message_size(64 << 20))
    }
}

#[async_trait]
impl PeerTransport for GatewayRelay {
    async fn open(&self, to: &str, q: QueueRef) -> Result<Link, MqError> {
        let rec = registry::get(self.inner.store.as_ref(), to)
            .await
            .map_err(|e| unreachable_owner(to, e))?
            .ok_or_else(|| unreachable_owner(to, "not registered"))?;
        let mut client = self
            .inner
            .client(&rec)
            .map_err(|e| unreachable_owner(to, e))?;
        let (up_tx, up_rx) = tokio::sync::mpsc::unbounded_channel::<Up>();
        let mut up_rx = up_rx;
        let outbound = futures::stream::once(std::future::ready(open_pb(&q))).chain(
            futures::stream::poll_fn(move |cx| up_rx.poll_recv(cx).map(|o| o.map(up_pb))),
        );
        let resp = tokio::time::timeout(Duration::from_secs(5), client.relay(outbound))
            .await
            .map_err(|_| unreachable_owner(to, "timed out"))?
            .map_err(|e| unreachable_owner(to, e.message()))?;
        let mut inbound = resp.into_inner();
        let (down_tx, down_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                match inbound.message().await {
                    Ok(Some(f)) => {
                        if let Some(d) = down_of(f)
                            && down_tx.send(d).is_err()
                        {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        tracing::debug!(error = %e, "queue relay stream broke");
                        break;
                    }
                }
            }
        });
        metrics::counter!("sprig_queue_relay_streams_total").increment(1);
        Ok(Link {
            up: up_tx,
            down: down_rx,
        })
    }
}

impl ReplyRelay for GatewayRelay {
    fn relay(&self, slot: u32, name: &str, message: Message) {
        let inner = Arc::clone(&self.inner);
        let name = name.to_owned();
        tokio::spawn(async move {
            let Ok(slot16) = u16::try_from(slot) else {
                return;
            };
            let Ok(Some(rec)) = registry::slot_holder(inner.store.as_ref(), slot16).await else {
                return;
            };
            let Ok(mut c) = inner.client(&rec) else {
                return;
            };
            let req = pb::QueueReplyRequest {
                slot,
                name,
                message: Some(msg_pb(&WireMsg::of(&message))),
            };
            if let Err(e) = tokio::time::timeout(Duration::from_secs(5), c.reply(req)).await {
                tracing::debug!(error = %e, "direct reply-to relay timed out");
            }
        });
    }
}

// ----------------------------------------------------------------------
// Placement.
// ----------------------------------------------------------------------

#[derive(serde::Serialize, serde::Deserialize)]
struct Preferred {
    v: u32,
    gateway: String,
    at_ms: i64,
}

/// Where a tenant's DC is.
pub type HomeOf = Arc<dyn Fn(u64) -> DcId + Send + Sync>;

/// Owner candidates from the registry of the tenant's home DC.
pub struct GatewayPlacement {
    store: Arc<dyn MetaStore>,
    home: HomeOf,
    cache: Mutex<HashMap<DcId, (Instant, Vec<String>)>>,
}

impl std::fmt::Debug for GatewayPlacement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayPlacement").finish_non_exhaustive()
    }
}

impl GatewayPlacement {
    /// Placement over `store` (any tier: single-etcd mode reads every
    /// DC's tier through it); `home` names a tenant's home DC.
    #[must_use]
    pub fn new(store: Arc<dyn MetaStore>, home: HomeOf) -> Self {
        Self {
            store,
            home,
            cache: Mutex::new(HashMap::new()),
        }
    }

    fn tier(&self, tenant: u64) -> Arc<dyn MetaStore> {
        TierView::over(&self.store, Tier::Dc((self.home)(tenant)))
    }

    fn key(store: &dyn MetaStore, tenant: u64, object: u64) -> Option<Key> {
        store
            .tier()
            .key(&format!("queue-placement/{tenant}/{object}"))
            .ok()
    }
}

#[async_trait]
impl Placement for GatewayPlacement {
    async fn candidates(&self, tenant: u64) -> Vec<String> {
        let dc = (self.home)(tenant);
        if let Some((at, v)) = lock(&self.cache).get(&dc)
            && at.elapsed() < Duration::from_secs(1)
        {
            return v.clone();
        }
        let tier = TierView::over(&self.store, Tier::Dc(dc.clone()));
        let mut v: Vec<String> = match registry::list(&*tier).await {
            Ok(list) => list
                .into_iter()
                .filter(|r| r.state == GatewayState::Ready && r.listeners.contains_key("peer"))
                .map(|r| r.gateway_id)
                .collect(),
            Err(e) => {
                tracing::debug!(error = %e, "queue placement: registry unreadable");
                return lock(&self.cache)
                    .get(&dc)
                    .map(|(_, v)| v.clone())
                    .unwrap_or_default();
            }
        };
        v.sort();
        lock(&self.cache).insert(dc, (Instant::now(), v.clone()));
        v
    }

    async fn preferred(&self, tenant: u64, object: u64) -> Option<(String, i64)> {
        let store = self.tier(tenant);
        let key = Self::key(store.as_ref(), tenant, object)?;
        let kv = store.get(&key, Consistency::Serializable).await.ok()??;
        let p: Preferred = serde_json::from_slice(&kv.value).ok()?;
        Some((p.gateway, p.at_ms))
    }

    async fn prefer(&self, tenant: u64, object: u64, gateway: &str, at_ms: i64) {
        let store = self.tier(tenant);
        let Some(key) = Self::key(store.as_ref(), tenant, object) else {
            return;
        };
        let value = serde_json::to_vec(&Preferred {
            v: 1,
            gateway: gateway.to_owned(),
            at_ms,
        })
        .unwrap_or_default();
        let _ = store
            .txn(Txn {
                when: Vec::new(),
                then: vec![Op::Put {
                    key,
                    value,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await;
    }
}

// ----------------------------------------------------------------------
// The temporary-queue registry.
// ----------------------------------------------------------------------

type SetKey = (u64, u32);

#[derive(Default)]
struct TempsState {
    /// Every gateway's sets, by (tenant, db) then gateway.
    all: HashMap<SetKey, HashMap<String, Arc<TempSet>>>,
    /// Others' sets merged, by (tenant, db).
    merged: HashMap<SetKey, Arc<TempSet>>,
    /// This gateway's published sets (republished after a lost lease).
    mine: HashMap<SetKey, Vec<u8>>,
    lease: Option<LeaseId>,
}

/// Temporary queues in this DC's etcd tier, watched.
pub struct EtcdTemps {
    store: Arc<dyn MetaStore>,
    me: String,
    state: Mutex<TempsState>,
    write: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for EtcdTemps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EtcdTemps")
            .field("me", &self.me)
            .finish_non_exhaustive()
    }
}

const TEMPS_TTL: Duration = Duration::from_secs(10);

impl EtcdTemps {
    /// The registry of gateway `me` in `store` (the DC tier); call
    /// [`EtcdTemps::run`] to follow other gateways'.
    #[must_use]
    pub fn new(store: Arc<dyn MetaStore>, me: String) -> Self {
        Self {
            store,
            me,
            state: Mutex::new(TempsState::default()),
            write: tokio::sync::Mutex::new(()),
        }
    }

    fn prefix(&self) -> Option<Key> {
        self.store.tier().key("queue-temps").ok()
    }

    fn key_of(&self, k: SetKey) -> Option<Key> {
        self.store
            .tier()
            .key(&format!("queue-temps/{}/{}/{}", k.0, k.1, self.me))
            .ok()
    }

    /// `(tenant, db, gateway)` of a key.
    fn parse(key: &Key) -> Option<(SetKey, String)> {
        let s = key.as_str();
        let rest = &s[s.find("queue-temps/")? + "queue-temps/".len()..];
        let mut it = rest.splitn(3, '/');
        let t = it.next()?.parse().ok()?;
        let d = it.next()?.parse().ok()?;
        Some(((t, d), it.next()?.to_owned()))
    }

    fn apply(&self, key: &Key, value: Option<&[u8]>) {
        let Some((k, gw)) = Self::parse(key) else {
            return;
        };
        let mut st = lock(&self.state);
        let entry = st.all.entry(k).or_default();
        match value.and_then(TempSet::decode) {
            Some(set) => {
                entry.insert(gw, Arc::new(set));
            }
            None => {
                entry.remove(&gw);
            }
        }
        st.merged.remove(&k);
    }

    async fn lease(&self) -> Option<LeaseId> {
        if let Some(l) = lock(&self.state).lease {
            return Some(l);
        }
        let l = self.store.grant_lease(TEMPS_TTL).await.ok()?;
        lock(&self.state).lease = Some(l);
        Some(l)
    }

    /// Writes this gateway's set of `k` (`None`: none left, the key goes).
    async fn put(&self, k: SetKey, value: Option<Vec<u8>>) {
        let _w = self.write.lock().await;
        let Some(key) = self.key_of(k) else { return };
        let op = match value {
            None => Op::Delete(key),
            Some(value) => {
                let Some(lease) = self.lease().await else {
                    return;
                };
                Op::Put {
                    key,
                    value,
                    lease: Some(lease),
                }
            }
        };
        if let Err(e) = self
            .store
            .txn(Txn {
                when: Vec::<Cmp>::new(),
                then: vec![op],
                otherwise: Vec::new(),
            })
            .await
        {
            tracing::debug!(error = %e, "temporary queue registry write failed");
        }
    }

    /// Follows the registry and keeps this gateway's lease alive until
    /// `stop` (the lease is revoked then, so other gateways drop this
    /// one's queues at once).
    pub async fn run(self: Arc<Self>, stop: CancellationToken) {
        let Some(prefix) = self.prefix() else { return };
        let keeper = {
            let me = Arc::clone(&self);
            let stop = stop.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        () = stop.cancelled() => break,
                        () = tokio::time::sleep(Duration::from_secs(3)) => {}
                    }
                    let l = lock(&me.state).lease;
                    let Some(l) = l else { continue };
                    if !matches!(me.store.keep_alive(l).await, Ok(Some(_))) {
                        // Lost: a new lease, and this gateway's sets again.
                        let mine = {
                            let mut st = lock(&me.state);
                            st.lease = None;
                            st.mine.clone()
                        };
                        for (k, v) in mine {
                            me.put(k, Some(v)).await;
                        }
                    }
                }
                let l = lock(&me.state).lease.take();
                if let Some(l) = l {
                    let _ = me.store.revoke_lease(l).await;
                }
            })
        };
        'outer: loop {
            if stop.is_cancelled() {
                break;
            }
            // List, then watch from the listing's revision.
            let page = match self.store.list(&prefix, ListOpts::default()).await {
                Ok(p) => p,
                Err(e) => {
                    tracing::debug!(error = %e, "temporary queue registry unreadable");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            {
                let mut st = lock(&self.state);
                st.all.clear();
                st.merged.clear();
            }
            for kv in &page.kvs {
                self.apply(&kv.key, Some(&kv.value));
            }
            let Ok(mut w) = self.store.watch(&prefix, page.revision + 1).await else {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            };
            loop {
                let ev = tokio::select! {
                    () = stop.cancelled() => break 'outer,
                    ev = w.next() => ev,
                };
                match ev {
                    Some(Ok(WatchEvent::Put(kv))) => self.apply(&kv.key, Some(&kv.value)),
                    Some(Ok(WatchEvent::Delete { key, .. })) => self.apply(&key, None),
                    Some(Ok(WatchEvent::Progress(_))) => {}
                    // Compacted, broken or ended: list again.
                    _ => continue 'outer,
                }
            }
        }
        let _ = keeper.await;
    }
}

#[async_trait]
impl TempRegistry for EtcdTemps {
    async fn publish(&self, v: &VhostRef, set: &TempSet) {
        let k = (v.tenant, v.db_id);
        if set.queues.is_empty() && set.bindings.is_empty() {
            lock(&self.state).mine.remove(&k);
            self.put(k, None).await;
            return;
        }
        let value = set.encode();
        lock(&self.state).mine.insert(k, value.clone());
        self.put(k, Some(value)).await;
    }

    fn others(&self, v: &VhostRef) -> Option<Arc<TempSet>> {
        let k = (v.tenant, v.db_id);
        let mut st = lock(&self.state);
        if let Some(m) = st.merged.get(&k) {
            return Some(Arc::clone(m));
        }
        let mut out = TempSet::default();
        if let Some(by_gw) = st.all.get(&k) {
            for (gw, s) in by_gw {
                if *gw == self.me {
                    continue;
                }
                out.queues
                    .extend(s.queues.iter().map(|(n, q)| (n.clone(), Arc::clone(q))));
                out.bindings.extend(s.bindings.iter().cloned());
            }
        }
        let out = Arc::new(out);
        st.merged.insert(k, Arc::clone(&out));
        Some(out)
    }
}

// ----------------------------------------------------------------------
// The shared direct reply-to key.
// ----------------------------------------------------------------------

/// The direct reply-to key of this DC's gateways: read, or made by the
/// first gateway (32 random bytes, kept in etcd), so every gateway checks
/// every other's reply-to names. `None` when etcd can't be read: the
/// gateway then uses a key of its own (names stay local to it).
pub async fn reply_key(store: &dyn MetaStore) -> Option<Vec<u8>> {
    use ring::rand::SecureRandom;
    let key = store.tier().key("amqp-reply-key").ok()?;
    let mut fresh = [0u8; 32];
    ring::rand::SystemRandom::new().fill(&mut fresh).ok()?;
    let r = store
        .txn(Txn {
            when: vec![Cmp::Missing(key.clone())],
            then: vec![Op::Put {
                key: key.clone(),
                value: fresh.to_vec(),
                lease: None,
            }],
            otherwise: vec![Op::Get(key.clone())],
        })
        .await
        .ok()?;
    if r.succeeded {
        return Some(fresh.to_vec());
    }
    let kv = store.get(&key, Consistency::Linearizable).await.ok()??;
    (kv.value.len() == 32).then_some(kv.value)
}

/// The tenant's home DC, from the tenant locator.
#[must_use]
pub fn home_of(locator: Arc<crate::tenant::locator::GatewayLocator>) -> HomeOf {
    Arc::new(move |t| locator.home_of(TenantId(t)))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::too_many_lines)]

    use super::*;

    #[test]
    fn frames_round_trip_through_protobuf() {
        let id = DeliveryId {
            queue: 7,
            lane: 0,
            pclass: 3,
            seq: 99,
        };
        let w = WireMsg {
            fmt: 0,
            exchange: "x".into(),
            routing_key: "k".into(),
            props: Bytes::from_static(&[0, 0]),
            body: Bytes::from_static(b"hi"),
            no_ttl: true,
        };
        let ups = vec![
            Up::Enqueue {
                req: 1,
                msg: w.clone(),
                msg_id: [5; 16],
                dedup: true,
                deaths: Some(Bytes::from_static(b"d")),
                confirm: true,
            },
            Up::Attach {
                req: 2,
                consumer: 3,
                conn: 4,
                no_ack: false,
                exclusive: true,
                priority: -2,
                held: vec![id],
            },
            Up::Credit {
                consumer: 3,
                credit: 10,
                drain: true,
            },
            Up::Cancel { consumer: 3 },
            Up::Settle {
                conn: 4,
                any: true,
                items: vec![
                    (id, Settlement::Ack),
                    (id, Settlement::Requeue { failed: true }),
                    (id, Settlement::DeadLetter(DeadLetterReason::Maxlen)),
                ],
            },
            Up::Get {
                req: 5,
                conn: 4,
                no_ack: false,
                lease_ms: Some(30_000),
            },
            Up::Purge { req: 6 },
            Up::Delete {
                req: 7,
                if_unused: true,
                if_empty: false,
            },
            Up::Info { req: 8 },
            Up::ConnLost { conn: 4 },
            Up::Hold {
                req: 9,
                id,
                msg_id: [1; 16],
            },
            Up::Release {
                id,
                committed: true,
            },
            Up::Verify { id },
        ];
        for u in ups {
            assert_eq!(up_of(up_pb(u.clone())), Some(u));
        }
        let d = WireDelivery {
            id,
            queue: "q".into(),
            msg: w,
            redelivered: true,
            delivery_count: 2,
            annotations: Bytes::from_static(b"a"),
        };
        let downs = vec![
            Down::Reply {
                req: 1,
                result: Ok(Answer::Done),
            },
            Down::Reply {
                req: 2,
                result: Ok(Answer::Got(Some(Box::new((d.clone(), 4))))),
            },
            Down::Reply {
                req: 3,
                result: Ok(Answer::Got(None)),
            },
            Down::Reply {
                req: 4,
                result: Ok(Answer::Count(9)),
            },
            Down::Reply {
                req: 5,
                result: Ok(Answer::Info(3, 1)),
            },
            Down::Reply {
                req: 6,
                result: Err(MqError::new(MqCode::QueueFull, "full")),
            },
            Down::Deliver { consumer: 3, d },
            Down::Cancelled {
                consumer: 3,
                reason: MqError::not_found("gone"),
            },
            Down::Drained {
                consumer: 3,
                credit: 5,
            },
            Down::Closed(MqError::new(MqCode::OwnerMoved, "moved")),
        ];
        for x in downs {
            assert_eq!(down_of(down_pb(x.clone())), Some(x));
        }
    }

    /// Two engines relaying over real gRPC on loopback (the peer service,
    /// plaintext as in development): publish on one, consume on the
    /// other; an exclusive queue of one is reached from the other through
    /// the etcd registry; a direct reply-to reply crosses too.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn queues_relay_over_the_peer_service() {
        use sprig_common::time::SystemClock;
        use sprig_meta::{MemStore, Record};
        use sprig_proto_amqp::auth::Principal;
        use sprig_proto_amqp::backend::{
            ConnKey, ConsumeSpec, ConsumerId, Event, OpCtx, QueueBackend, QueueSpec,
        };
        use sprig_proto_amqp::v091::codec::FieldTable;
        use sprig_queues::engine::{EngineDeps, Leases};
        use sprig_queues::mem::{MemCatalog, MemLeases};

        use crate::peer::PeerHost;
        use crate::session::SessionTable;

        struct Host {
            sessions: Arc<SessionTable>,
            id: String,
            q: QueuePeerService,
        }
        #[async_trait]
        impl PeerHost for Host {
            fn sessions(&self) -> &Arc<SessionTable> {
                &self.sessions
            }
            fn gateway_id(&self) -> String {
                self.id.clone()
            }
            fn drain(&self, _reason: &str) -> bool {
                false
            }
            async fn forwarded(&self, _req: pb::ForwardRequest) -> pb::ForwardReply {
                pb::ForwardReply::default()
            }
            fn queue_peer(&self) -> Option<QueuePeerService> {
                Some(self.q.clone())
            }
        }
        let dc = DcId::new("dc-a").unwrap();
        let mem: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(dc.clone())));
        let catalog = Arc::new(MemCatalog::new(
            Arc::new(|_, _| Ok("mq".into())),
            Arc::new(|_, _, _| true),
        ));
        let leases = MemLeases::new();
        let stop = CancellationToken::new();
        let key = reply_key(mem.as_ref()).await.unwrap();
        assert_eq!(
            reply_key(mem.as_ref()).await.unwrap(),
            key,
            "one key per DC"
        );
        let mut engines = Vec::new();
        let mut replies = Vec::new();
        for i in 1..=2u16 {
            let id = format!("gw-{i}");
            let relay = GatewayRelay::new(Arc::clone(&mem), None);
            let temps = Arc::new(EtcdTemps::new(Arc::clone(&mem), id.clone()));
            tokio::spawn(Arc::clone(&temps).run(stop.clone()));
            let dc2 = dc.clone();
            let engine = Arc::new(QueueEngine::new(EngineDeps {
                catalog: Arc::clone(&catalog) as Arc<dyn sprig_queues::engine::MqCatalog>,
                leases: Arc::new(leases.for_gateway(&id)),
                temps,
                clock: Arc::new(sprig_queues::owner::SystemClock),
                slot: u32::from(i),
                peers: Some(Arc::new(relay.clone())),
                placement: Some(Arc::new(GatewayPlacement::new(
                    Arc::clone(&mem),
                    Arc::new(move |_| dc2.clone()),
                ))),
            }));
            let r = DirectReplyTo::with_key(u32::from(i), &key);
            r.set_relay(Arc::new(relay));
            let host = Arc::new(Host {
                sessions: Arc::new(SessionTable::new(Arc::new(SystemClock))),
                id: id.clone(),
                q: QueuePeerService::new(Arc::clone(&engine), Arc::clone(&r), u32::from(i)),
            });
            let (addr, _task) = crate::peer::serve(
                "127.0.0.1:0".parse().unwrap(),
                None,
                host as Arc<dyn PeerHost>,
                stop.clone(),
            )
            .await
            .unwrap()
            .unwrap();
            let rec = registry::GatewayRecord {
                gateway_id: id.clone(),
                slot: i,
                version: "test".into(),
                addr: "127.0.0.1".into(),
                listeners: [("peer".to_owned(), addr.port())].into(),
                state: GatewayState::Ready,
                catalog_formats: Vec::new(),
                authorizers: Vec::new(),
                load: registry::Load::default(),
                kafka_host: None,
            };
            let slot = registry::SlotRecord {
                gateway_id: id.clone(),
            };
            mem.txn(Txn {
                when: Vec::new(),
                then: vec![
                    Op::Put {
                        key: mem.tier().key(&format!("gateways/{id}")).unwrap(),
                        value: Record::new(rec).encode().unwrap(),
                        lease: None,
                    },
                    Op::Put {
                        key: mem.tier().key(&format!("gateway-slots/{i}")).unwrap(),
                        value: Record::new(slot).encode().unwrap(),
                        lease: None,
                    },
                ],
                otherwise: Vec::new(),
            })
            .await
            .unwrap();
            engines.push(engine);
            replies.push(r);
        }
        let p = Arc::new(Principal {
            tenant_id: 42,
            tenant: "acme".into(),
            user: "app".into(),
            user_id: Some(1),
            handle: None,
        });
        let open = |e: &Arc<QueueEngine>| {
            let e = Arc::clone(e);
            let p = Arc::clone(&p);
            async move {
                let conn = ConnKey::next();
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                let v = e.open_vhost(&p, "/", conn, tx).await.unwrap();
                let cx = OpCtx {
                    principal: p,
                    conn,
                    channel: 1,
                };
                (v, cx, rx)
            }
        };
        let (va, cxa, mut rxa) = open(&engines[0]).await;
        let (vb, cxb, mut rxb) = open(&engines[1]).await;
        va.declare_queue(
            &cxa,
            QueueSpec {
                name: "work".into(),
                durable: true,
                ..QueueSpec::default()
            },
            false,
        )
        .await
        .unwrap();
        let body = |n: u32| {
            sprig_queues::msg::rebuild(
                0,
                String::new(),
                "work".into(),
                Bytes::from_static(&[0, 0]),
                Bytes::from(n.to_string()),
            )
            .unwrap()
        };
        for n in 0..50 {
            let pubd = va.publish(&cxa, body(n), false).unwrap();
            assert_eq!(pubd.routed, 1);
            pubd.confirm.await.unwrap();
        }
        let c = ConsumerId::next();
        vb.consume(
            &cxb,
            ConsumeSpec {
                consumer: c,
                queue: "work".into(),
                tag: "c".into(),
                no_ack: false,
                exclusive: false,
                priority: 0,
                arguments: FieldTable::new(),
            },
        )
        .await
        .unwrap();
        vb.credit(c, 100, false);
        let mut got = Vec::new();
        while got.len() < 50 {
            let ev = tokio::time::timeout(Duration::from_secs(10), rxb.recv())
                .await
                .expect("a delivery in time")
                .unwrap();
            if let Event::Deliver { delivery: d, .. } = ev {
                got.push(String::from_utf8_lossy(&d.message.body).into_owned());
                vb.settle(&cxb, vec![(d.id, Settlement::Ack)]);
            }
        }
        let want: Vec<String> = (0..50).map(|n| n.to_string()).collect();
        assert_eq!(got, want);
        let owner = leases.holder(42, 1001).unwrap_or_default();
        assert!(owner == "gw-1" || owner == "gw-2", "{owner:?}");
        let _ = leases.for_gateway("x").me();
        // An exclusive server-named queue on A, published to from B.
        let d = va
            .declare_queue(
                &cxa,
                QueueSpec {
                    name: String::new(),
                    exclusive: true,
                    ..QueueSpec::default()
                },
                false,
            )
            .await
            .unwrap();
        let ca = ConsumerId::next();
        va.consume(
            &cxa,
            ConsumeSpec {
                consumer: ca,
                queue: d.name.clone(),
                tag: "r".into(),
                no_ack: true,
                exclusive: false,
                priority: 0,
                arguments: FieldTable::new(),
            },
        )
        .await
        .unwrap();
        va.credit(ca, 10, false);
        let reply = |rk: &str| {
            sprig_queues::msg::rebuild(
                0,
                String::new(),
                rk.to_owned(),
                Bytes::from_static(&[0, 0]),
                Bytes::from_static(b"answer"),
            )
            .unwrap()
        };
        let mut routed = 0;
        for _ in 0..50 {
            let pubd = vb.publish(&cxb, reply(&d.name), false).unwrap();
            routed = pubd.routed;
            if routed == 1 {
                pubd.confirm.await.unwrap();
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(routed, 1, "B routes to A's temporary queue");
        loop {
            let ev = tokio::time::timeout(Duration::from_secs(10), rxa.recv())
                .await
                .expect("the reply in time")
                .unwrap();
            if let Event::Deliver { delivery: dd, .. } = ev {
                assert_eq!(&dd.message.body[..], b"answer");
                break;
            }
        }
        // Direct reply-to: a requester on A, answered through B.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let rc = ConsumerId::next();
        let name = replies[0].register(ConnKey::next(), 1, rc, tx, 7);
        assert!(replies[1].alive(&name, 7));
        // Another tenant's connection can't reply to it.
        assert_eq!(replies[1].deliver(&name, reply(&name), 8), None);
        assert_eq!(replies[1].deliver(&name, reply(&name), 7), Some(true));
        let ev = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("the direct reply in time")
            .unwrap();
        assert!(matches!(ev, Event::Deliver { consumer, .. } if consumer == rc));
        stop.cancel();
        for e in &engines {
            e.shutdown();
        }
    }
}
