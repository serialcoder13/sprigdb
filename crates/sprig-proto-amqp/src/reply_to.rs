//! Direct reply-to for RPC: the
//! `amq.rabbitmq.reply-to` pseudo-queue, which stores nothing.
//!
//! A requester consumes `amq.rabbitmq.reply-to` (0-9-1, `no_ack`) or
//! attaches a volatile receiving link (1.0); its published `reply_to` is
//! rewritten to `amq.rabbitmq.reply-to.<token>`, where the token is
//! base64url of `(gateway, connection, channel, nonce, tenant)` and an HMAC
//! under this gateway's key, so responders can't forge names that reach
//! another connection. A reply published to the default exchange with that
//! name as the routing key goes straight to the requester's connection,
//! only from a connection of the requester's own tenant: the key is shared
//! by the data center's gateways, so a name that leaked to another tenant
//! would otherwise carry that tenant's message into the requester's
//! connection.
//!
//! Replies for a requester on another gateway go over the peer service
//! (7403): gateways that share the key ([`DirectReplyTo::with_key`]) check
//! each other's names, and a [`ReplyRelay`] carries the reply to the
//! requester's gateway. Without a relay such a reply is dropped like one
//! for a requester that is gone (both count as routed, as in RabbitMQ).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::hmac;
use ring::rand::{SecureRandom, SystemRandom};
use subtle::ConstantTimeEq;

use crate::backend::{ConnKey, ConsumerId, Event, EventSender};
use crate::message::{Delivery, DeliveryId, Message};
use crate::v091::codec::FieldTable;

/// The pseudo-queue name.
pub const PSEUDO_QUEUE: &str = "amq.rabbitmq.reply-to";
/// The prefix of rewritten names.
pub const PREFIX: &str = "amq.rabbitmq.reply-to.";

const MAC_LEN: usize = 16;
/// Gateway (4), connection (8), channel (2), nonce (8), tenant (8).
const BODY_LEN: usize = 30;

struct Entry {
    events: EventSender,
    consumer: ConsumerId,
    nonce: u64,
    tenant: u64,
}

/// Carries replies to requesters on other gateways (the peer service).
pub trait ReplyRelay: Send + Sync + 'static {
    /// Sends `message` to the requester named `name` on gateway `slot`;
    /// fire and forget (a requester that is gone drops it).
    fn relay(&self, slot: u32, name: &str, message: Message);
}

/// This gateway's direct reply-to requesters.
pub struct DirectReplyTo {
    key: hmac::Key,
    gateway: u32,
    entries: Mutex<HashMap<(ConnKey, u16), Entry>>,
    relay: std::sync::OnceLock<Arc<dyn ReplyRelay>>,
}

impl std::fmt::Debug for DirectReplyTo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectReplyTo")
            .field("gateway", &self.gateway)
            .finish_non_exhaustive()
    }
}

/// A decoded reply-to name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplyAddr {
    /// The gateway the requester is on.
    pub gateway: u32,
    /// Its connection.
    pub conn: ConnKey,
    /// Its channel (0-9-1) or link handle slot (1.0).
    pub channel: u16,
    nonce: u64,
    /// The requester's tenant (0 for a connection without one).
    pub tenant: u64,
}

impl DirectReplyTo {
    /// A registry for gateway number `gateway` with a random key.
    #[must_use]
    pub fn new(gateway: u32) -> Arc<Self> {
        let rng = SystemRandom::new();
        let mut k = [0u8; 32];
        let _ = rng.fill(&mut k);
        Self::with_key(gateway, &k)
    }

    /// A registry for gateway number `gateway` with a key shared by the
    /// gateways that relay replies to each other.
    #[must_use]
    pub fn with_key(gateway: u32, key: &[u8]) -> Arc<Self> {
        Arc::new(Self {
            key: hmac::Key::new(hmac::HMAC_SHA256, key),
            gateway,
            entries: Mutex::new(HashMap::new()),
            relay: std::sync::OnceLock::new(),
        })
    }

    /// Replies to other gateways' requesters go through `r` (set once).
    pub fn set_relay(&self, r: Arc<dyn ReplyRelay>) {
        let _ = self.relay.set(r);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<(ConnKey, u16), Entry>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Registers a requester of tenant `tenant`; returns its reply-to
    /// name.
    pub fn register(
        &self,
        conn: ConnKey,
        channel: u16,
        consumer: ConsumerId,
        events: EventSender,
        tenant: u64,
    ) -> String {
        let mut n = [0u8; 8];
        let _ = SystemRandom::new().fill(&mut n);
        let nonce = u64::from_be_bytes(n);
        self.lock().insert(
            (conn, channel),
            Entry {
                events,
                consumer,
                nonce,
                tenant,
            },
        );
        self.name(conn, channel, nonce, tenant)
    }

    /// The requester registered on `(conn, channel)`, and its name.
    #[must_use]
    pub fn lookup(&self, conn: ConnKey, channel: u16) -> Option<(ConsumerId, String)> {
        let g = self.lock();
        let e = g.get(&(conn, channel))?;
        Some((e.consumer, self.name(conn, channel, e.nonce, e.tenant)))
    }

    /// Removes a requester.
    pub fn unregister(&self, conn: ConnKey, channel: u16) {
        self.lock().remove(&(conn, channel));
    }

    /// Removes every requester of a connection.
    pub fn unregister_conn(&self, conn: ConnKey) {
        self.lock().retain(|(c, _), _| *c != conn);
    }

    fn name(&self, conn: ConnKey, channel: u16, nonce: u64, tenant: u64) -> String {
        let mut raw = Vec::with_capacity(BODY_LEN + MAC_LEN);
        raw.extend_from_slice(&self.gateway.to_be_bytes());
        raw.extend_from_slice(&conn.0.to_be_bytes());
        raw.extend_from_slice(&channel.to_be_bytes());
        raw.extend_from_slice(&nonce.to_be_bytes());
        raw.extend_from_slice(&tenant.to_be_bytes());
        let tag = hmac::sign(&self.key, &raw);
        raw.extend_from_slice(&tag.as_ref()[..MAC_LEN]);
        format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode(raw))
    }

    /// Decodes and checks a reply-to name; `None` for a malformed or
    /// forged one.
    #[must_use]
    pub fn decode(&self, name: &str) -> Option<ReplyAddr> {
        let raw = URL_SAFE_NO_PAD.decode(name.strip_prefix(PREFIX)?).ok()?;
        if raw.len() != BODY_LEN + MAC_LEN {
            return None;
        }
        let (body, mac) = raw.split_at(BODY_LEN);
        // The MAC is truncated: compare its prefix in constant time.
        let full = hmac::sign(&self.key, body);
        if !bool::from(full.as_ref()[..MAC_LEN].ct_eq(mac)) {
            return None;
        }
        let u32_at =
            |i: usize| u32::from_be_bytes([body[i], body[i + 1], body[i + 2], body[i + 3]]);
        let u64_at = |i: usize| {
            let mut a = [0u8; 8];
            a.copy_from_slice(&body[i..i + 8]);
            u64::from_be_bytes(a)
        };
        Some(ReplyAddr {
            gateway: u32_at(0),
            conn: ConnKey(u64_at(4)),
            channel: u16::from_be_bytes([body[12], body[13]]),
            nonce: u64_at(14),
            tenant: u64_at(22),
        })
    }

    /// Decodes a name a connection of `tenant` may use: `None` for a
    /// malformed or forged one, or one of another tenant's requester.
    #[must_use]
    pub fn decode_for(&self, name: &str, tenant: u64) -> Option<ReplyAddr> {
        self.decode(name).filter(|a| a.tenant == tenant)
    }

    /// Whether `name` reaches a live requester of `tenant` (a passive
    /// `queue.declare` of it).
    #[must_use]
    pub fn alive(&self, name: &str, tenant: u64) -> bool {
        self.decode_for(name, tenant).is_some_and(|a| {
            // Another gateway's requester: its name is genuine (the shared
            // key), and only its gateway knows whether it is still there.
            (a.gateway != self.gateway && self.relay.get().is_some())
                || a.gateway == self.gateway
                    && self
                        .lock()
                        .get(&(a.conn, a.channel))
                        .is_some_and(|e| e.nonce == a.nonce)
        })
    }

    /// Delivers a reply from a connection of `tenant`. `None` for a
    /// malformed or forged name, or another tenant's requester (the caller
    /// treats it as unroutable); `Some(false)` when the requester is gone
    /// (the reply is dropped, and still counts as routed).
    #[must_use]
    pub fn deliver(&self, name: &str, message: Message, tenant: u64) -> Option<bool> {
        let a = self.decode_for(name, tenant)?;
        Some(self.deliver_to(name, a, message))
    }

    /// Delivers a reply another gateway relayed: that gateway checked the
    /// replier's tenant against the name already.
    #[must_use]
    pub fn deliver_relayed(&self, name: &str, message: Message) -> Option<bool> {
        let a = self.decode(name)?;
        Some(self.deliver_to(name, a, message))
    }

    fn deliver_to(&self, name: &str, a: ReplyAddr, message: Message) -> bool {
        if a.gateway != self.gateway {
            return match self.relay.get() {
                Some(r) => {
                    r.relay(a.gateway, name, message);
                    true
                }
                None => false,
            };
        }
        let g = self.lock();
        let Some(e) = g.get(&(a.conn, a.channel)).filter(|e| e.nonce == a.nonce) else {
            return false;
        };
        let d = Delivery {
            id: DeliveryId::default(),
            queue: name.to_owned(),
            message: Arc::new(message),
            redelivered: false,
            delivery_count: 0,
            annotations: FieldTable::new(),
        };
        e.events
            .send(Event::Deliver {
                consumer: e.consumer,
                delivery: d,
            })
            .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tokio::sync::mpsc;

    use super::*;
    use crate::message::{MessageInfo, MsgFormat};

    fn msg() -> Message {
        Message {
            format: MsgFormat::Amqp091,
            exchange: String::new(),
            routing_key: "r".into(),
            props: Bytes::from_static(&[0, 0]),
            body: Bytes::from_static(b"hi"),
            info: MessageInfo::default(),
        }
    }

    #[test]
    fn names_round_trip_and_resist_forgery() {
        let r = DirectReplyTo::new(7);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let c = ConsumerId::next();
        let name = r.register(ConnKey(42), 3, c, tx, 900);
        assert!(name.starts_with(PREFIX));
        assert!(r.alive(&name, 900));
        let a = r.decode(&name).unwrap();
        assert_eq!(
            (a.gateway, a.conn, a.channel, a.tenant),
            (7, ConnKey(42), 3, 900)
        );
        assert_eq!(r.deliver(&name, msg(), 900), Some(true));
        assert!(matches!(rx.try_recv(), Ok(Event::Deliver { consumer, .. }) if consumer == c));
        // A flipped character fails the MAC.
        let mut forged = name.clone().into_bytes();
        let last = forged.len() - 3;
        forged[last] = if forged[last] == b'A' { b'B' } else { b'A' };
        let forged = String::from_utf8(forged).unwrap();
        assert_eq!(r.deliver(&forged, msg(), 900), None);
        // Another gateway's key.
        let other = DirectReplyTo::new(7);
        assert!(other.decode(&name).is_none());
        // Gone requester: dropped, not an error.
        r.unregister(ConnKey(42), 3);
        assert!(!r.alive(&name, 900));
        assert_eq!(r.deliver(&name, msg(), 900), Some(false));
    }

    #[test]
    fn another_tenant_can_not_reply() {
        let r = DirectReplyTo::with_key(1, b"shared-in-the-dc");
        let (tx, mut rx) = mpsc::unbounded_channel();
        let c = ConsumerId::next();
        let name = r.register(ConnKey(9), 1, c, tx, 2_430_600_001);
        // A connection of another tenant: unroutable, never delivered.
        assert_eq!(r.deliver(&name, msg(), 2_430_600_002), None);
        assert!(!r.alive(&name, 2_430_600_002));
        assert!(rx.try_recv().is_err());
        // Another gateway of the DC checks it the same way.
        let b = DirectReplyTo::with_key(2, b"shared-in-the-dc");
        assert!(b.decode_for(&name, 2_430_600_002).is_none());
        assert!(b.decode_for(&name, 2_430_600_001).is_some());
        // The requester's own tenant.
        assert_eq!(r.deliver(&name, msg(), 2_430_600_001), Some(true));
        assert!(matches!(rx.try_recv(), Ok(Event::Deliver { consumer, .. }) if consumer == c));
    }

    #[test]
    fn replies_cross_gateways_with_a_shared_key() {
        struct Hop(Arc<DirectReplyTo>);
        impl ReplyRelay for Hop {
            fn relay(&self, slot: u32, name: &str, message: Message) {
                assert_eq!(slot, 1);
                let _ = self.0.deliver_relayed(name, message);
            }
        }
        let a = DirectReplyTo::with_key(1, b"shared");
        let b = DirectReplyTo::with_key(2, b"shared");
        let (tx, mut rx) = mpsc::unbounded_channel();
        let c = ConsumerId::next();
        let name = a.register(ConnKey(5), 1, c, tx, 0);
        // Without a relay, another gateway's requester is dropped.
        assert_eq!(b.deliver(&name, msg(), 0), Some(false));
        b.set_relay(Arc::new(Hop(Arc::clone(&a))));
        assert!(b.alive(&name, 0));
        assert_eq!(b.deliver(&name, msg(), 0), Some(true));
        assert!(matches!(rx.try_recv(), Ok(Event::Deliver { consumer, .. }) if consumer == c));
        // Another key can't check the name: unroutable.
        let other = DirectReplyTo::with_key(3, b"other");
        assert_eq!(other.deliver(&name, msg(), 0), None);
    }
}
