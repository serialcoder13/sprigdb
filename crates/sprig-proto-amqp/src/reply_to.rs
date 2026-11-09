//! Direct reply-to (chapter 50, "RPC and direct reply-to"): the
//! `amq.rabbitmq.reply-to` pseudo-queue, which stores nothing.
//!
//! A requester consumes `amq.rabbitmq.reply-to` (0-9-1, `no_ack`) or
//! attaches a volatile receiving link (1.0); its published `reply_to` is
//! rewritten to `amq.rabbitmq.reply-to.<token>`, where the token is
//! base64url of `(gateway, connection, channel, nonce)` and an HMAC under
//! this gateway's key, so responders can't forge names that reach another
//! connection. A reply published to the default exchange with that name
//! as the routing key goes straight to the requester's connection.
//!
//! Replies for a requester on another gateway go over the peer service
//! (7403) in chapter 50's design; that relay is not built yet, so such a
//! reply is dropped like one for a requester that is gone (both count as
//! routed, as in RabbitMQ).

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

struct Entry {
    events: EventSender,
    consumer: ConsumerId,
    nonce: u64,
}

/// This gateway's direct reply-to requesters.
pub struct DirectReplyTo {
    key: hmac::Key,
    gateway: u32,
    entries: Mutex<HashMap<(ConnKey, u16), Entry>>,
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
}

impl DirectReplyTo {
    /// A registry for gateway number `gateway` with a random key.
    #[must_use]
    pub fn new(gateway: u32) -> Arc<Self> {
        let rng = SystemRandom::new();
        let mut k = [0u8; 32];
        let _ = rng.fill(&mut k);
        Arc::new(Self {
            key: hmac::Key::new(hmac::HMAC_SHA256, &k),
            gateway,
            entries: Mutex::new(HashMap::new()),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<(ConnKey, u16), Entry>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Registers a requester; returns its reply-to name.
    pub fn register(
        &self,
        conn: ConnKey,
        channel: u16,
        consumer: ConsumerId,
        events: EventSender,
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
            },
        );
        self.name(conn, channel, nonce)
    }

    /// The requester registered on `(conn, channel)`, and its name.
    #[must_use]
    pub fn lookup(&self, conn: ConnKey, channel: u16) -> Option<(ConsumerId, String)> {
        let g = self.lock();
        let e = g.get(&(conn, channel))?;
        Some((e.consumer, self.name(conn, channel, e.nonce)))
    }

    /// Removes a requester.
    pub fn unregister(&self, conn: ConnKey, channel: u16) {
        self.lock().remove(&(conn, channel));
    }

    /// Removes every requester of a connection.
    pub fn unregister_conn(&self, conn: ConnKey) {
        self.lock().retain(|(c, _), _| *c != conn);
    }

    fn name(&self, conn: ConnKey, channel: u16, nonce: u64) -> String {
        let mut raw = Vec::with_capacity(22 + MAC_LEN);
        raw.extend_from_slice(&self.gateway.to_be_bytes());
        raw.extend_from_slice(&conn.0.to_be_bytes());
        raw.extend_from_slice(&channel.to_be_bytes());
        raw.extend_from_slice(&nonce.to_be_bytes());
        let tag = hmac::sign(&self.key, &raw);
        raw.extend_from_slice(&tag.as_ref()[..MAC_LEN]);
        format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode(raw))
    }

    /// Decodes and checks a reply-to name; `None` for a malformed or
    /// forged one.
    #[must_use]
    pub fn decode(&self, name: &str) -> Option<ReplyAddr> {
        let raw = URL_SAFE_NO_PAD.decode(name.strip_prefix(PREFIX)?).ok()?;
        if raw.len() != 22 + MAC_LEN {
            return None;
        }
        let (body, mac) = raw.split_at(22);
        // The MAC is truncated: compare its prefix in constant time.
        let full = hmac::sign(&self.key, body);
        if !bool::from(full.as_ref()[..MAC_LEN].ct_eq(mac)) {
            return None;
        }
        let u32_at = |i: usize| u32::from_be_bytes([body[i], body[i + 1], body[i + 2], body[i + 3]]);
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
        })
    }

    /// Whether `name` reaches a live requester (a passive
    /// `queue.declare` of it).
    #[must_use]
    pub fn alive(&self, name: &str) -> bool {
        self.decode(name).is_some_and(|a| {
            a.gateway == self.gateway
                && self
                    .lock()
                    .get(&(a.conn, a.channel))
                    .is_some_and(|e| e.nonce == a.nonce)
        })
    }

    /// Delivers a reply. `Err` for a malformed or forged name (the caller
    /// refuses the publish); `Ok(false)` when the requester is gone (the
    /// reply is dropped, and still counts as routed).
    ///
    /// # Errors
    /// The name isn't one this gateway's key signed.
    pub fn deliver(&self, name: &str, message: Message) -> Result<bool, ()> {
        let a = self.decode(name).ok_or(())?;
        if a.gateway != self.gateway {
            return Ok(false);
        }
        let g = self.lock();
        let Some(e) = g.get(&(a.conn, a.channel)).filter(|e| e.nonce == a.nonce) else {
            return Ok(false);
        };
        let d = Delivery {
            id: DeliveryId::default(),
            queue: name.to_owned(),
            message: Arc::new(message),
            redelivered: false,
            delivery_count: 0,
            annotations: FieldTable::new(),
        };
        Ok(e
            .events
            .send(Event::Deliver {
                consumer: e.consumer,
                delivery: d,
            })
            .is_ok())
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
        let name = r.register(ConnKey(42), 3, c, tx);
        assert!(name.starts_with(PREFIX));
        assert!(r.alive(&name));
        let a = r.decode(&name).unwrap();
        assert_eq!((a.gateway, a.conn, a.channel), (7, ConnKey(42), 3));
        assert_eq!(r.deliver(&name, msg()), Ok(true));
        assert!(matches!(rx.try_recv(), Ok(Event::Deliver { consumer, .. }) if consumer == c));
        // A flipped character fails the MAC.
        let mut forged = name.clone().into_bytes();
        let last = forged.len() - 3;
        forged[last] = if forged[last] == b'A' { b'B' } else { b'A' };
        let forged = String::from_utf8(forged).unwrap();
        assert_eq!(r.deliver(&forged, msg()), Err(()));
        // Another gateway's key.
        let other = DirectReplyTo::new(7);
        assert!(other.decode(&name).is_none());
        // Gone requester: dropped, not an error.
        r.unregister(ConnKey(42), 3);
        assert!(!r.alive(&name));
        assert_eq!(r.deliver(&name, msg()), Ok(false));
    }
}
