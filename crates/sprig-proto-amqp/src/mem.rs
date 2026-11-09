//! An in-memory queue engine behind [`QueueBackend`]: enough of chapter
//! 50's semantics to test both front ends end to end, and to serve the
//! gateway's AMQP listeners until `sprig-queues` (the MariaDB-backed
//! engine, build steps 2–10) replaces it. Nothing survives a restart.
//!
//! Served: the built-in exchanges and the four exchange types (topic
//! patterns with `*` and `#`, headers with `x-match` `all`, `any`,
//! `all-with-x`, `any-with-x`), exchange-to-exchange bindings, alternate
//! exchanges, internal exchanges, declare equivalence and RabbitMQ's
//! reply codes, server-named and exclusive and auto-delete queues,
//! credit-based dispatch with consumer priorities, single active
//! consumer and exclusive consumers, requeue at the original position,
//! `x-delivery-count` and `x-delivery-limit`, message and per-message
//! TTL at the head, `x-max-length` with `drop-head` and `reject-publish`,
//! `x-max-priority`, dead-lettering with `x-death`, purge, `basic.get`,
//! transactions applied as one step, and `connection.blocked` for tests.
//!
//! Not served: delayed messages (an `x-delayed-message` exchange routes
//! at once by its `x-delayed-type`), lanes, queue TTL (`x-expires`),
//! streams (`x-queue-type = stream` is refused with 540).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use crate::auth::Principal;
use crate::backend::{
    BindingSpec, ConnKey, ConsumeSpec, ConsumerId, DeadLetterReason, DestKind, Event, EventSender,
    ExchangeSpec, OpCtx, Published, QueueBackend, QueueDeclared, QueueSpec, Settlement, TxBatch,
    Vhost,
};
use crate::error::{MqCode, MqError, reply};
use crate::message::{Delivery, DeliveryId, Message};
use crate::v091::codec::{FieldTable, FieldValue};

/// A RabbitMQ permission kind (chapter 50, "Security notes").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Perm {
    /// Declare and delete.
    Configure,
    /// Publish, bind as destination.
    Write,
    /// Consume, get, purge, bind as source.
    Read,
}

/// Decides permissions: `(user, permission, object name) -> allowed`.
pub type Authorizer = Arc<dyn Fn(&Principal, Perm, &str) -> bool + Send + Sync>;

/// Maps a vhost name to the tenant's database: `Err` for an unknown
/// vhost or no access.
pub type VhostMap = Arc<dyn Fn(&Principal, &str) -> Result<String, MqError> + Send + Sync>;

/// The in-memory engine.
pub struct MemBackend {
    vhosts: Mutex<HashMap<(u64, String), Arc<MemVhost>>>,
    map: VhostMap,
    authz: Authorizer,
}

impl std::fmt::Debug for MemBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemBackend").finish_non_exhaustive()
    }
}

impl Default for MemBackend {
    fn default() -> Self {
        Self::new(
            Arc::new(|_, v| Ok(default_database(v, "mq"))),
            Arc::new(|_, _, _| true),
        )
    }
}

/// Chapter 50's vhost mapping without `mq_vhosts`: `/` is
/// `default_db` (`amqp.default_vhost_database`), any other vhost the
/// database of the same name.
#[must_use]
pub fn default_database(vhost: &str, default_db: &str) -> String {
    if vhost == "/" || vhost.is_empty() {
        default_db.to_owned()
    } else {
        vhost.to_owned()
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl MemBackend {
    /// An engine with a vhost mapping and an authorizer.
    #[must_use]
    pub fn new(map: VhostMap, authz: Authorizer) -> Self {
        Self {
            vhosts: Mutex::new(HashMap::new()),
            map,
            authz,
        }
    }

    /// Sends `connection.blocked` (or unblocked) to every connection of
    /// every vhost: the tests' stand-in for quotas and back-pressure.
    pub fn set_blocked(&self, blocked: bool) {
        for v in lock(&self.vhosts).values() {
            let st = lock(&v.state);
            for tx in st.conns.values() {
                let _ = tx.send(if blocked {
                    Event::Blocked("low on disk".into())
                } else {
                    Event::Unblocked
                });
            }
        }
    }
}

#[async_trait]
impl QueueBackend for MemBackend {
    async fn open_vhost(
        &self,
        principal: &Principal,
        vhost: &str,
        conn: ConnKey,
        events: EventSender,
    ) -> Result<Arc<dyn Vhost>, MqError> {
        let db = (self.map)(principal, vhost)?;
        let v = {
            let mut g = lock(&self.vhosts);
            Arc::clone(
                g.entry((principal.tenant_id, db.clone()))
                    .or_insert_with(|| {
                        Arc::new(MemVhost {
                            name: vhost.to_owned(),
                            state: Mutex::new(State::new(vhost)),
                            authz: Arc::clone(&self.authz),
                        })
                    }),
            )
        };
        lock(&v.state).conns.insert(conn, events);
        Ok(v)
    }
}

struct Exchange {
    kind: String,
    durable: bool,
    auto_delete: bool,
    internal: bool,
    arguments: FieldTable,
    builtin: bool,
}

#[derive(Clone)]
struct Entry {
    msg: Arc<Message>,
    seq: u64,
    pclass: u8,
    redelivered: bool,
    delivery_count: u32,
    expires_at: Option<Instant>,
    deaths: Option<FieldValue>,
}

struct Queue {
    id: u64,
    durable: bool,
    exclusive: Option<ConnKey>,
    auto_delete: bool,
    arguments: FieldTable,
    quorum: bool,
    ready: BTreeMap<(u8, u64), Entry>,
    unacked: HashMap<u64, (Entry, Option<ConsumerId>)>,
    next_seq: u64,
    consumers: Vec<ConsumerId>,
    rr: usize,
    had_consumer: bool,
}

impl Queue {
    fn arg_i64(&self, k: &str) -> Option<i64> {
        self.arguments.get(k).and_then(FieldValue::as_i64)
    }
    fn arg_str(&self, k: &str) -> Option<String> {
        self.arguments
            .get(k)
            .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned))
    }
    fn max_priority(&self) -> Option<u8> {
        if self.quorum {
            return Some(31);
        }
        self.arg_i64("x-max-priority")
            .and_then(|p| u8::try_from(p.clamp(1, 255)).ok())
    }
    fn pclass(&self, m: &Message) -> u8 {
        match self.max_priority() {
            None => 0,
            Some(max) => {
                let p = m.info.priority.unwrap_or(if self.quorum { 4 } else { 0 });
                max - p.min(max)
            }
        }
    }
}

struct Consumer {
    conn: ConnKey,
    queue: String,
    no_ack: bool,
    exclusive: bool,
    priority: i32,
    credit: u32,
    drain: bool,
}

struct Binding {
    source: String,
    dest: String,
    kind: DestKind,
    key: String,
    arguments: FieldTable,
}

struct State {
    vhost: String,
    exchanges: HashMap<String, Exchange>,
    queues: HashMap<String, Queue>,
    queue_ids: HashMap<u64, String>,
    bindings: Vec<Binding>,
    consumers: HashMap<ConsumerId, Consumer>,
    conns: HashMap<ConnKey, EventSender>,
    next_queue_id: u64,
}

const KINDS: [&str; 5] = ["direct", "fanout", "topic", "headers", "x-delayed-message"];

impl State {
    fn new(vhost: &str) -> Self {
        let mut exchanges = HashMap::new();
        for (name, kind) in [
            ("", "direct"),
            ("amq.direct", "direct"),
            ("amq.fanout", "fanout"),
            ("amq.topic", "topic"),
            ("amq.headers", "headers"),
            ("amq.match", "headers"),
        ] {
            exchanges.insert(
                name.to_owned(),
                Exchange {
                    kind: kind.to_owned(),
                    durable: true,
                    auto_delete: false,
                    internal: false,
                    arguments: FieldTable::new(),
                    builtin: true,
                },
            );
        }
        Self {
            vhost: vhost.to_owned(),
            exchanges,
            queues: HashMap::new(),
            queue_ids: HashMap::new(),
            bindings: Vec::new(),
            consumers: HashMap::new(),
            conns: HashMap::new(),
            next_queue_id: 1,
        }
    }

    fn no_exchange(&self, name: &str) -> MqError {
        MqError::not_found(format!("no exchange '{name}' in vhost '{}'", self.vhost))
    }

    fn no_queue(&self, name: &str) -> MqError {
        MqError::not_found(format!("no queue '{name}' in vhost '{}'", self.vhost))
    }

    fn locked(&self, name: &str) -> MqError {
        MqError::locked(format!(
            "cannot obtain exclusive access to locked queue '{name}' in vhost '{}'. It could be originally declared on another connection or the exclusive property value does not match that of the original declaration.",
            self.vhost
        ))
    }

    /// The queue, checked for exclusive ownership.
    fn queue_for(&self, name: &str, conn: ConnKey) -> Result<&Queue, MqError> {
        let q = self.queues.get(name).ok_or_else(|| self.no_queue(name))?;
        if q.exclusive.is_some_and(|o| o != conn) {
            return Err(self.locked(name));
        }
        Ok(q)
    }

    /// Routes a message (chapter 50, "Routing"): the queues reached.
    fn route(&self, exchange: &str, key: &str, headers: &FieldTable) -> Vec<String> {
        let mut out = Vec::new();
        let mut seen_q = HashSet::new();
        let mut visited = HashSet::new();
        self.route_into(exchange, key, headers, &mut visited, &mut seen_q, &mut out);
        if out.is_empty()
            && let Some(alt) = self
                .exchanges
                .get(exchange)
                .and_then(|e| e.arguments.get("alternate-exchange"))
                .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned))
        {
            let mut visited = HashSet::new();
            self.route_into(&alt, key, headers, &mut visited, &mut seen_q, &mut out);
        }
        out
    }

    fn route_into(
        &self,
        exchange: &str,
        key: &str,
        headers: &FieldTable,
        visited: &mut HashSet<String>,
        seen_q: &mut HashSet<String>,
        out: &mut Vec<String>,
    ) {
        if !visited.insert(exchange.to_owned()) {
            return;
        }
        let Some(x) = self.exchanges.get(exchange) else {
            return;
        };
        if exchange.is_empty() {
            if self.queues.contains_key(key) && seen_q.insert(key.to_owned()) {
                out.push(key.to_owned());
            }
            return;
        }
        let kind = if x.kind == "x-delayed-message" {
            x.arguments
                .get("x-delayed-type")
                .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned))
                .unwrap_or_else(|| "direct".into())
        } else {
            x.kind.clone()
        };
        for b in self.bindings.iter().filter(|b| b.source == exchange) {
            let hit = match kind.as_str() {
                "fanout" => true,
                "topic" => topic_match(&b.key, key),
                "headers" => headers_match(&b.arguments, headers),
                _ => b.key == key,
            };
            if !hit {
                continue;
            }
            match b.kind {
                DestKind::Queue => {
                    if seen_q.insert(b.dest.clone()) {
                        out.push(b.dest.clone());
                    }
                }
                DestKind::Exchange => self.route_into(&b.dest, key, headers, visited, seen_q, out),
            }
        }
    }

    /// Puts a message on a queue; `Err` for `reject-publish` overflow.
    fn enqueue(
        &mut self,
        qname: &str,
        msg: Arc<Message>,
        deaths: Option<FieldValue>,
    ) -> Result<(), MqError> {
        let Some(q) = self.queues.get_mut(qname) else {
            return Ok(());
        };
        let max_len = q
            .arg_i64("x-max-length")
            .and_then(|n| usize::try_from(n).ok());
        let max_bytes = q
            .arg_i64("x-max-length-bytes")
            .and_then(|n| u64::try_from(n).ok());
        let overflow = q
            .arg_str("x-overflow")
            .unwrap_or_else(|| "drop-head".into());
        let bytes: u64 = q.ready.values().map(|e| e.msg.info.size).sum();
        let full = max_len.is_some_and(|n| q.ready.len() >= n)
            || max_bytes.is_some_and(|n| bytes + msg.info.size > n);
        let mut dropped = Vec::new();
        if full {
            if overflow.starts_with("reject-publish") {
                return Err(MqError::new(
                    MqCode::QueueFull,
                    format!("queue '{qname}' in vhost '{}' is full", self.vhost),
                ));
            }
            // drop-head: make room.
            while max_len.is_some_and(|n| q.ready.len() >= n.max(1)) || {
                let b: u64 = q.ready.values().map(|e| e.msg.info.size).sum();
                max_bytes.is_some_and(|n| b + msg.info.size > n) && !q.ready.is_empty()
            } {
                let Some((&k, _)) = q.ready.iter().next() else {
                    break;
                };
                if let Some(e) = q.ready.remove(&k) {
                    dropped.push(e);
                }
                if max_len == Some(0) {
                    break;
                }
            }
            if max_len == Some(0) {
                // Nothing fits: the new message is the dropped head.
                dropped.push(Entry {
                    msg: Arc::clone(&msg),
                    seq: 0,
                    pclass: 0,
                    redelivered: false,
                    delivery_count: 0,
                    expires_at: None,
                    deaths,
                });
                for e in dropped {
                    self.dead_letter(qname, e, DeadLetterReason::Maxlen);
                }
                return Ok(());
            }
        }
        let q = self.queues.get_mut(qname).unwrap_or_else(|| unreachable!());
        let ttl = [
            q.arg_i64("x-message-ttl")
                .and_then(|t| u64::try_from(t).ok()),
            msg.info.ttl_ms,
        ]
        .into_iter()
        .flatten()
        .min();
        let seq = q.next_seq;
        q.next_seq += 1;
        let pclass = q.pclass(&msg);
        q.ready.insert(
            (pclass, seq),
            Entry {
                msg,
                seq,
                pclass,
                redelivered: false,
                delivery_count: 0,
                expires_at: ttl.map(|t| Instant::now() + std::time::Duration::from_millis(t)),
                deaths,
            },
        );
        for e in dropped {
            self.dead_letter(qname, e, DeadLetterReason::Maxlen);
        }
        Ok(())
    }

    /// Dead-letters an entry (chapter 50, "Dead-lettering").
    fn dead_letter(&mut self, qname: &str, e: Entry, reason: DeadLetterReason) {
        let Some(q) = self.queues.get(qname) else {
            return;
        };
        let Some(dlx) = q.arg_str("x-dead-letter-exchange") else {
            return;
        };
        let key = q
            .arg_str("x-dead-letter-routing-key")
            .unwrap_or_else(|| e.msg.routing_key.clone());
        // x-death: one entry per (queue, reason), newest first.
        let mut deaths: Vec<FieldValue> = match e.deaths {
            Some(FieldValue::Array(a)) => a,
            _ => Vec::new(),
        };
        let mut count = 1i64;
        deaths.retain(|d| {
            let FieldValue::Table(t) = d else { return true };
            let same = t
                .get("queue")
                .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned))
                .as_deref()
                == Some(qname)
                && t.get("reason")
                    .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned))
                    .as_deref()
                    == Some(reason.as_str());
            if same {
                count += t.get("count").and_then(FieldValue::as_i64).unwrap_or(0);
            }
            !same
        });
        // A cycle back to the same queue for a reason other than rejected
        // is dropped (RabbitMQ's rule, 39277).
        if reason != DeadLetterReason::Rejected && count > 1 {
            tracing::debug!(
                queue = qname,
                code = MqCode::DeadLetterCycle.number(),
                "dead-letter cycle: message dropped"
            );
            return;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        deaths.insert(
            0,
            FieldValue::Table(FieldTable::from([
                ("count", FieldValue::I64(count)),
                ("reason", FieldValue::str(reason.as_str())),
                ("queue", FieldValue::str(qname)),
                ("time", FieldValue::Timestamp(now)),
                ("exchange", FieldValue::str(&e.msg.exchange)),
                (
                    "routing-keys",
                    FieldValue::Array(vec![FieldValue::str(&e.msg.routing_key)]),
                ),
            ])),
        );
        let mut m = (*e.msg).clone();
        m.info.ttl_ms = None;
        let headers = m.info.headers.clone();
        let msg = Arc::new(m);
        for target in self.route(&dlx, &key, &headers) {
            let _ = self.enqueue(
                &target,
                Arc::clone(&msg),
                Some(FieldValue::Array(deaths.clone())),
            );
            self.dispatch(&target);
        }
    }

    fn deliver_entry(qname: &str, q: &Queue, e: &Entry) -> Delivery {
        let mut annotations = FieldTable::new();
        if let Some(d) = &e.deaths {
            annotations.insert("x-death", d.clone());
        }
        if q.quorum && e.delivery_count > 0 {
            annotations.insert(
                "x-delivery-count",
                FieldValue::I64(i64::from(e.delivery_count)),
            );
        }
        Delivery {
            id: DeliveryId {
                queue: q.id,
                lane: 0,
                pclass: e.pclass,
                seq: e.seq,
            },
            queue: qname.to_owned(),
            message: Arc::clone(&e.msg),
            redelivered: e.redelivered,
            delivery_count: e.delivery_count,
            annotations,
        }
    }

    /// Pops the next live message, expiring heads on the way.
    fn pop_ready(&mut self, qname: &str) -> Option<Entry> {
        loop {
            let q = self.queues.get_mut(qname)?;
            let (&k, _) = q.ready.iter().next()?;
            let e = q.ready.remove(&k)?;
            if e.expires_at.is_some_and(|t| t <= Instant::now()) {
                self.dead_letter(qname, e, DeadLetterReason::Expired);
                continue;
            }
            return Some(e);
        }
    }

    /// Hands ready messages to consumers with credit (chapter 50,
    /// "Dispatch, credit and consumers").
    fn dispatch(&mut self, qname: &str) {
        loop {
            let Some(q) = self.queues.get(qname) else {
                return;
            };
            if q.ready.is_empty() {
                break;
            }
            let sac = q.arguments.flag("x-single-active-consumer");
            let candidates: Vec<ConsumerId> = if sac {
                q.consumers.first().copied().into_iter().collect()
            } else {
                q.consumers.clone()
            };
            let best = candidates
                .iter()
                .filter_map(|c| {
                    self.consumers
                        .get(c)
                        .filter(|c| c.credit > 0)
                        .map(|c| c.priority)
                })
                .max();
            let Some(best) = best else { break };
            let n = candidates.len();
            let start = q.rr;
            let mut chosen = None;
            for i in 0..n {
                let c = candidates[(start + i) % n];
                if self
                    .consumers
                    .get(&c)
                    .is_some_and(|x| x.credit > 0 && x.priority == best)
                {
                    chosen = Some((c, (start + i + 1) % n.max(1)));
                    break;
                }
            }
            let Some((cid, next_rr)) = chosen else { break };
            let Some(e) = self.pop_ready(qname) else {
                break;
            };
            let q = self.queues.get_mut(qname).unwrap_or_else(|| unreachable!());
            q.rr = next_rr;
            let c = self
                .consumers
                .get_mut(&cid)
                .unwrap_or_else(|| unreachable!());
            c.credit -= 1;
            let (conn, no_ack) = (c.conn, c.no_ack);
            let q = self.queues.get(qname).unwrap_or_else(|| unreachable!());
            let d = Self::deliver_entry(qname, q, &e);
            if !no_ack {
                let q = self.queues.get_mut(qname).unwrap_or_else(|| unreachable!());
                q.unacked.insert(e.seq, (e, Some(cid)));
            }
            if let Some(tx) = self.conns.get(&conn) {
                let _ = tx.send(Event::Deliver {
                    consumer: cid,
                    delivery: d,
                });
            }
        }
        self.finish_drains(qname);
    }

    /// Consumers draining an empty queue give up their credit.
    fn finish_drains(&mut self, qname: &str) {
        let empty = self.queues.get(qname).is_none_or(|q| q.ready.is_empty());
        if !empty {
            return;
        }
        let ids: Vec<ConsumerId> = self
            .consumers
            .iter()
            .filter(|(_, c)| c.queue == qname && c.drain)
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            let Some(c) = self.consumers.get_mut(&id) else {
                continue;
            };
            let credit = std::mem::take(&mut c.credit);
            c.drain = false;
            if let Some(tx) = self.conns.get(&c.conn) {
                let _ = tx.send(Event::Drained {
                    consumer: id,
                    credit,
                });
            }
        }
    }

    fn settle_one(&mut self, id: DeliveryId, s: Settlement) -> Option<String> {
        let qname = self.queue_ids.get(&id.queue)?.clone();
        let q = self.queues.get_mut(&qname)?;
        let (mut e, _) = q.unacked.remove(&id.seq)?;
        match s {
            Settlement::Ack => {}
            Settlement::Requeue { failed } => {
                if failed {
                    e.delivery_count += 1;
                }
                let limit =
                    q.arg_i64("x-delivery-limit")
                        .or(if q.quorum { Some(20) } else { None });
                if limit.is_some_and(|l| l >= 0 && i64::from(e.delivery_count) > l) {
                    self.dead_letter(&qname, e, DeadLetterReason::DeliveryLimit);
                } else {
                    e.redelivered = true;
                    q.ready.insert((e.pclass, e.seq), e);
                }
            }
            Settlement::DeadLetter(r) => self.dead_letter(&qname, e, r),
        }
        Some(qname)
    }

    fn delete_queue(&mut self, name: &str) -> u32 {
        let Some(q) = self.queues.remove(name) else {
            return 0;
        };
        self.queue_ids.remove(&q.id);
        self.bindings
            .retain(|b| !(b.kind == DestKind::Queue && b.dest == name));
        for cid in &q.consumers {
            if let Some(c) = self.consumers.remove(cid)
                && let Some(tx) = self.conns.get(&c.conn)
            {
                let _ = tx.send(Event::Cancelled {
                    consumer: *cid,
                    reason: MqError::not_found(format!(
                        "queue '{name}' in vhost '{}' was deleted",
                        self.vhost
                    )),
                });
            }
        }
        u32::try_from(q.ready.len()).unwrap_or(u32::MAX)
    }

    /// Removes a consumer; an auto-delete queue goes with its last one.
    fn remove_consumer(&mut self, id: ConsumerId) {
        let Some(c) = self.consumers.remove(&id) else {
            return;
        };
        let Some(q) = self.queues.get_mut(&c.queue) else {
            return;
        };
        q.consumers.retain(|x| *x != id);
        if q.rr >= q.consumers.len() {
            q.rr = 0;
        }
        if q.auto_delete && q.had_consumer && q.consumers.is_empty() {
            let name = c.queue.clone();
            self.delete_queue(&name);
        } else {
            let name = c.queue.clone();
            self.dispatch(&name);
        }
    }

    fn publish_one(&mut self, m: Message) -> Result<u32, MqError> {
        let Some(x) = self.exchanges.get(&m.exchange) else {
            return Err(self.no_exchange(&m.exchange));
        };
        if x.internal {
            return Err(MqError::access_refused(format!(
                "cannot publish to internal exchange '{}' in vhost '{}'",
                m.exchange, self.vhost
            )));
        }
        let targets = self.route(&m.exchange, &m.routing_key, &m.info.headers);
        let msg = Arc::new(m);
        let mut first_err = None;
        for t in &targets {
            if let Err(e) = self.enqueue(t, Arc::clone(&msg), None) {
                first_err.get_or_insert(e);
            }
            self.dispatch(t);
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(u32::try_from(targets.len()).unwrap_or(u32::MAX)),
        }
    }
}

/// Topic matching: `*` is one word, `#` zero or more.
#[must_use]
pub fn topic_match(pattern: &str, key: &str) -> bool {
    fn m(p: &[&str], k: &[&str]) -> bool {
        match p.split_first() {
            None => k.is_empty(),
            Some((&"#", rest)) => (0..=k.len()).any(|i| m(rest, &k[i..])),
            Some((&w, rest)) => k
                .split_first()
                .is_some_and(|(&kw, krest)| (w == "*" || w == kw) && m(rest, krest)),
        }
    }
    let p: Vec<&str> = pattern.split('.').collect();
    let k: Vec<&str> = if key.is_empty() {
        Vec::new()
    } else {
        key.split('.').collect()
    };
    let p: Vec<&str> = if pattern.is_empty() { Vec::new() } else { p };
    m(&p, &k)
}

/// Headers matching with `x-match` (`all` by default).
#[must_use]
pub fn headers_match(binding: &FieldTable, headers: &FieldTable) -> bool {
    let mode = binding
        .get("x-match")
        .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned))
        .unwrap_or_else(|| "all".into());
    let with_x = mode.ends_with("-with-x");
    let any = mode.starts_with("any");
    let mut checked = 0;
    for (k, want) in &binding.0 {
        if k == "x-match" || (k.starts_with("x-") && !with_x) {
            continue;
        }
        checked += 1;
        let hit = match headers.get(k) {
            None => false,
            Some(have) => matches!(want, FieldValue::Void) || values_equal(want, have),
        };
        if any && hit {
            return true;
        }
        if !any && !hit {
            return false;
        }
    }
    let _ = checked;
    !any
}

fn values_equal(a: &FieldValue, b: &FieldValue) -> bool {
    match (a.as_i64(), b.as_i64()) {
        (Some(x), Some(y)) => x == y,
        _ => match (a.as_str(), b.as_str()) {
            (Some(x), Some(y)) => x == y,
            _ => a == b,
        },
    }
}

/// One vhost of the in-memory engine.
pub struct MemVhost {
    name: String,
    state: Mutex<State>,
    authz: Authorizer,
}

impl MemVhost {
    fn check(&self, cx: &OpCtx, p: Perm, kind: &str, name: &str) -> Result<(), MqError> {
        if (self.authz)(&cx.principal, p, name) {
            return Ok(());
        }
        let what = match p {
            Perm::Configure => "configure",
            Perm::Write => "write",
            Perm::Read => "read",
        };
        Err(MqError::access_refused(format!(
            "access to {kind} '{name}' in vhost '{}' refused for user '{}' ({what})",
            self.name, cx.principal.user
        )))
    }

    fn st(&self) -> MutexGuard<'_, State> {
        lock(&self.state)
    }
}

fn inequivalent(what: &str, arg: &str, name: &str, vhost: &str, got: &str, cur: &str) -> MqError {
    MqError::precondition(format!(
        "inequivalent arg '{arg}' for {what} '{name}' in vhost '{vhost}': received '{got}' but current is '{cur}'"
    ))
}

/// Queue arguments whose difference makes a redeclare inequivalent.
const QUEUE_ARGS: [&str; 9] = [
    "x-queue-type",
    "x-message-ttl",
    "x-max-length",
    "x-max-length-bytes",
    "x-overflow",
    "x-dead-letter-exchange",
    "x-dead-letter-routing-key",
    "x-max-priority",
    "x-single-active-consumer",
];

fn arg_text(t: &FieldTable, k: &str) -> String {
    match t.get(k) {
        None => "none".into(),
        Some(v) => v
            .as_str()
            .map(std::borrow::Cow::into_owned)
            .or_else(|| v.as_i64().map(|n| n.to_string()))
            .or_else(|| v.as_bool().map(|b| b.to_string()))
            .unwrap_or_else(|| format!("{v:?}")),
    }
}

#[async_trait]
impl Vhost for MemVhost {
    fn name(&self) -> &str {
        &self.name
    }

    async fn declare_exchange(
        &self,
        cx: &OpCtx,
        spec: ExchangeSpec,
        passive: bool,
    ) -> Result<(), MqError> {
        let st = &mut *self.st();
        if spec.name.is_empty() {
            if passive {
                return Ok(());
            }
            return Err(MqError::access_refused(
                "operation not permitted on the default exchange",
            ));
        }
        if passive {
            return if st.exchanges.contains_key(&spec.name) {
                Ok(())
            } else {
                Err(st.no_exchange(&spec.name))
            };
        }
        if !KINDS.contains(&spec.kind.as_str()) {
            return Err(MqError::frame(
                reply::COMMAND_INVALID,
                format!("invalid exchange type '{}'", spec.kind),
            ));
        }
        if let Some(x) = st.exchanges.get(&spec.name) {
            let v = st.vhost.clone();
            let name = &spec.name;
            if x.kind != spec.kind {
                return Err(inequivalent(
                    "exchange", "type", name, &v, &spec.kind, &x.kind,
                ));
            }
            if x.durable != spec.durable {
                return Err(inequivalent(
                    "exchange",
                    "durable",
                    name,
                    &v,
                    &spec.durable.to_string(),
                    &x.durable.to_string(),
                ));
            }
            if x.auto_delete != spec.auto_delete {
                return Err(inequivalent(
                    "exchange",
                    "auto_delete",
                    name,
                    &v,
                    &spec.auto_delete.to_string(),
                    &x.auto_delete.to_string(),
                ));
            }
            if x.internal != spec.internal {
                return Err(inequivalent(
                    "exchange",
                    "internal",
                    name,
                    &v,
                    &spec.internal.to_string(),
                    &x.internal.to_string(),
                ));
            }
            for k in ["alternate-exchange", "x-delayed-type"] {
                if !x.builtin && arg_text(&x.arguments, k) != arg_text(&spec.arguments, k) {
                    return Err(inequivalent(
                        "exchange",
                        k,
                        name,
                        &v,
                        &arg_text(&spec.arguments, k),
                        &arg_text(&x.arguments, k),
                    ));
                }
            }
            return Ok(());
        }
        if spec.name.starts_with("amq.") {
            return Err(MqError::access_refused(format!(
                "exchange name '{}' contains reserved prefix 'amq.*'",
                spec.name
            )));
        }
        self.check(cx, Perm::Configure, "exchange", &spec.name)?;
        st.exchanges.insert(
            spec.name,
            Exchange {
                kind: spec.kind,
                durable: spec.durable,
                auto_delete: spec.auto_delete,
                internal: spec.internal,
                arguments: spec.arguments,
                builtin: false,
            },
        );
        Ok(())
    }

    async fn delete_exchange(
        &self,
        cx: &OpCtx,
        name: &str,
        if_unused: bool,
    ) -> Result<(), MqError> {
        let st = &mut *self.st();
        if name.is_empty()
            || st.exchanges.get(name).is_some_and(|x| x.builtin)
            || name.starts_with("amq.")
        {
            return Err(MqError::access_refused(format!(
                "operation not permitted on the default exchange or '{name}'"
            )));
        }
        if !st.exchanges.contains_key(name) {
            return Ok(());
        }
        self.check(cx, Perm::Configure, "exchange", name)?;
        if if_unused && st.bindings.iter().any(|b| b.source == name) {
            return Err(MqError::precondition(format!(
                "exchange '{name}' in vhost '{}' in use",
                st.vhost
            )));
        }
        st.exchanges.remove(name);
        st.bindings
            .retain(|b| b.source != name && !(b.kind == DestKind::Exchange && b.dest == name));
        Ok(())
    }

    async fn declare_queue(
        &self,
        cx: &OpCtx,
        mut spec: QueueSpec,
        passive: bool,
    ) -> Result<QueueDeclared, MqError> {
        let st = &mut *self.st();
        let declared = |q: &Queue, name: &str| QueueDeclared {
            name: name.to_owned(),
            message_count: u32::try_from(q.ready.len()).unwrap_or(u32::MAX),
            consumer_count: u32::try_from(q.consumers.len()).unwrap_or(u32::MAX),
        };
        if passive {
            let q = st.queue_for(&spec.name, cx.conn)?;
            return Ok(declared(q, &spec.name));
        }
        if spec.name.is_empty() {
            spec.name = format!("amq.gen-{}", crate::v091::server::random_suffix());
        } else if spec.name.starts_with("amq.") && !st.queues.contains_key(&spec.name) {
            return Err(MqError::access_refused(format!(
                "queue name '{}' contains reserved prefix 'amq.*'",
                spec.name
            )));
        }
        let qtype = spec
            .arguments
            .get("x-queue-type")
            .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned))
            .unwrap_or_else(|| "classic".into());
        match qtype.as_str() {
            "classic" => {}
            "quorum" => {
                if spec.exclusive {
                    return Err(MqError::precondition(format!(
                        "invalid property 'exclusive' for queue '{}'",
                        spec.name
                    )));
                }
                if !spec.durable {
                    return Err(MqError::precondition(format!(
                        "invalid property 'non-durable' for queue '{}'",
                        spec.name
                    )));
                }
            }
            "stream" => {
                return Err(MqError::not_implemented(
                    "stream queues are not served by the in-memory engine",
                ));
            }
            other => {
                return Err(MqError::precondition(format!(
                    "unknown queue type '{other}'"
                )));
            }
        }
        if let Some(q) = st.queues.get(&spec.name) {
            if q.exclusive.is_some_and(|o| o != cx.conn) || q.exclusive.is_some() != spec.exclusive
            {
                return Err(st.locked(&spec.name));
            }
            let v = st.vhost.clone();
            if q.durable != spec.durable {
                return Err(inequivalent(
                    "queue",
                    "durable",
                    &spec.name,
                    &v,
                    &spec.durable.to_string(),
                    &q.durable.to_string(),
                ));
            }
            if q.auto_delete != spec.auto_delete {
                return Err(inequivalent(
                    "queue",
                    "auto_delete",
                    &spec.name,
                    &v,
                    &spec.auto_delete.to_string(),
                    &q.auto_delete.to_string(),
                ));
            }
            for k in QUEUE_ARGS {
                let (a, b) = (arg_text(&spec.arguments, k), arg_text(&q.arguments, k));
                let default_classic = k == "x-queue-type"
                    && (a == "classic" || a == "none")
                    && (b == "classic" || b == "none");
                if a != b && !default_classic {
                    return Err(inequivalent("queue", k, &spec.name, &v, &a, &b));
                }
            }
            return Ok(declared(q, &spec.name));
        }
        self.check(cx, Perm::Configure, "queue", &spec.name)?;
        let id = st.next_queue_id;
        st.next_queue_id += 1;
        st.queue_ids.insert(id, spec.name.clone());
        let q = Queue {
            id,
            durable: spec.durable,
            exclusive: spec.exclusive.then_some(cx.conn),
            auto_delete: spec.auto_delete,
            quorum: qtype == "quorum",
            arguments: spec.arguments,
            ready: BTreeMap::new(),
            unacked: HashMap::new(),
            next_seq: 1,
            consumers: Vec::new(),
            rr: 0,
            had_consumer: false,
        };
        let out = declared(&q, &spec.name);
        st.queues.insert(spec.name, q);
        Ok(out)
    }

    async fn delete_queue(
        &self,
        cx: &OpCtx,
        name: &str,
        if_unused: bool,
        if_empty: bool,
    ) -> Result<u32, MqError> {
        let st = &mut *self.st();
        let Some(q) = st.queues.get(name) else {
            return Ok(0);
        };
        if q.exclusive.is_some_and(|o| o != cx.conn) {
            return Err(st.locked(name));
        }
        self.check(cx, Perm::Configure, "queue", name)?;
        if if_unused && !q.consumers.is_empty() {
            return Err(MqError::precondition(format!(
                "queue '{name}' in vhost '{}' in use",
                st.vhost
            )));
        }
        if if_empty && !q.ready.is_empty() {
            return Err(MqError::precondition(format!(
                "queue '{name}' in vhost '{}' is not empty",
                st.vhost
            )));
        }
        Ok(st.delete_queue(name))
    }

    async fn purge_queue(&self, cx: &OpCtx, name: &str) -> Result<u32, MqError> {
        let st = &mut *self.st();
        st.queue_for(name, cx.conn)?;
        self.check(cx, Perm::Read, "queue", name)?;
        let q = st.queues.get_mut(name).unwrap_or_else(|| unreachable!());
        let n = q.ready.len();
        q.ready.clear();
        Ok(u32::try_from(n).unwrap_or(u32::MAX))
    }

    async fn bind(&self, cx: &OpCtx, b: BindingSpec) -> Result<(), MqError> {
        let st = &mut *self.st();
        if b.source.is_empty() || (b.dest_kind == DestKind::Exchange && b.destination.is_empty()) {
            return Err(MqError::access_refused(
                "operation not permitted on the default exchange",
            ));
        }
        if !st.exchanges.contains_key(&b.source) {
            return Err(st.no_exchange(&b.source));
        }
        match b.dest_kind {
            DestKind::Queue => {
                st.queue_for(&b.destination, cx.conn)?;
            }
            DestKind::Exchange => {
                if !st.exchanges.contains_key(&b.destination) {
                    return Err(st.no_exchange(&b.destination));
                }
            }
        }
        let kind = if b.dest_kind == DestKind::Queue {
            "queue"
        } else {
            "exchange"
        };
        self.check(cx, Perm::Write, kind, &b.destination)?;
        self.check(cx, Perm::Read, "exchange", &b.source)?;
        let exists = st.bindings.iter().any(|x| {
            x.source == b.source
                && x.dest == b.destination
                && x.kind == b.dest_kind
                && x.key == b.key
                && x.arguments == b.arguments
        });
        if !exists {
            st.bindings.push(Binding {
                source: b.source,
                dest: b.destination,
                kind: b.dest_kind,
                key: b.key,
                arguments: b.arguments,
            });
        }
        Ok(())
    }

    async fn unbind(&self, cx: &OpCtx, b: BindingSpec) -> Result<(), MqError> {
        let st = &mut *self.st();
        if b.dest_kind == DestKind::Queue
            && let Some(q) = st.queues.get(&b.destination)
            && q.exclusive.is_some_and(|o| o != cx.conn)
        {
            return Err(st.locked(&b.destination));
        }
        let kind = if b.dest_kind == DestKind::Queue {
            "queue"
        } else {
            "exchange"
        };
        self.check(cx, Perm::Write, kind, &b.destination)?;
        st.bindings.retain(|x| {
            !(x.source == b.source
                && x.dest == b.destination
                && x.kind == b.dest_kind
                && x.key == b.key
                && x.arguments == b.arguments)
        });
        // An auto-delete exchange goes with its last binding.
        let auto = st
            .exchanges
            .get(&b.source)
            .is_some_and(|x| x.auto_delete && !x.builtin);
        if auto && !st.bindings.iter().any(|x| x.source == b.source) {
            st.exchanges.remove(&b.source);
        }
        Ok(())
    }

    fn publish(&self, cx: &OpCtx, m: Message, _mandatory: bool) -> Result<Published, MqError> {
        self.check(cx, Perm::Write, "exchange", &m.exchange)?;
        let st = &mut *self.st();
        match st.publish_one(m) {
            Ok(n) => Ok(Published::done(n)),
            Err(e) if e.code == MqCode::QueueFull => Ok(Published {
                routed: 1,
                confirm: Box::pin(async move { Err(e) }),
            }),
            Err(e) => Err(e),
        }
    }

    async fn consume(&self, cx: &OpCtx, spec: ConsumeSpec) -> Result<(), MqError> {
        let st = &mut *self.st();
        let q = st.queue_for(&spec.queue, cx.conn)?;
        self.check(cx, Perm::Read, "queue", &spec.queue)?;
        let exclusive_held = q
            .consumers
            .iter()
            .any(|c| st.consumers.get(c).is_some_and(|c| c.exclusive));
        if exclusive_held || (spec.exclusive && !q.consumers.is_empty()) {
            return Err(MqError::access_refused(format!(
                "queue '{}' in vhost '{}' in exclusive use",
                spec.queue, st.vhost
            )));
        }
        st.consumers.insert(
            spec.consumer,
            Consumer {
                conn: cx.conn,
                queue: spec.queue.clone(),
                no_ack: spec.no_ack,
                exclusive: spec.exclusive,
                priority: spec.priority,
                credit: 0,
                drain: false,
            },
        );
        let q = st
            .queues
            .get_mut(&spec.queue)
            .unwrap_or_else(|| unreachable!());
        q.consumers.push(spec.consumer);
        q.had_consumer = true;
        Ok(())
    }

    async fn cancel(&self, _cx: &OpCtx, consumer: ConsumerId) {
        self.st().remove_consumer(consumer);
    }

    fn credit(&self, consumer: ConsumerId, credit: u32, drain: bool) {
        let st = &mut *self.st();
        let Some(c) = st.consumers.get_mut(&consumer) else {
            return;
        };
        c.credit = c.credit.saturating_add(credit);
        c.drain = drain;
        let q = c.queue.clone();
        st.dispatch(&q);
    }

    async fn get(
        &self,
        cx: &OpCtx,
        queue: &str,
        no_ack: bool,
    ) -> Result<Option<(Delivery, u32)>, MqError> {
        let st = &mut *self.st();
        st.queue_for(queue, cx.conn)?;
        self.check(cx, Perm::Read, "queue", queue)?;
        let Some(e) = st.pop_ready(queue) else {
            return Ok(None);
        };
        let q = st.queues.get(queue).unwrap_or_else(|| unreachable!());
        let d = State::deliver_entry(queue, q, &e);
        let q = st.queues.get_mut(queue).unwrap_or_else(|| unreachable!());
        let remaining = u32::try_from(q.ready.len()).unwrap_or(u32::MAX);
        if !no_ack {
            q.unacked.insert(e.seq, (e, None));
        }
        Ok(Some((d, remaining)))
    }

    fn settle(&self, _cx: &OpCtx, s: Vec<(DeliveryId, Settlement)>) {
        let st = &mut *self.st();
        let mut touched = HashSet::new();
        for (id, how) in s {
            if let Some(q) = st.settle_one(id, how) {
                touched.insert(q);
            }
        }
        for q in touched {
            st.dispatch(&q);
        }
    }

    async fn commit_tx(&self, cx: &OpCtx, tx: TxBatch) -> Result<Vec<u32>, MqError> {
        for (m, _) in &tx.publishes {
            self.check(cx, Perm::Write, "exchange", &m.exchange)?;
        }
        let st = &mut *self.st();
        // Check every exchange first, so nothing applies on failure.
        for (m, _) in &tx.publishes {
            if !st.exchanges.contains_key(&m.exchange) {
                return Err(st.no_exchange(&m.exchange));
            }
        }
        let mut counts = Vec::new();
        for (m, _) in tx.publishes {
            counts.push(st.publish_one(m).unwrap_or(0));
        }
        let mut touched = HashSet::new();
        for (id, how) in tx.settlements {
            if let Some(q) = st.settle_one(id, how) {
                touched.insert(q);
            }
        }
        for q in touched {
            st.dispatch(&q);
        }
        Ok(counts)
    }

    async fn connection_closed(&self, conn: ConnKey) {
        let st = &mut *self.st();
        st.conns.remove(&conn);
        let mine: Vec<ConsumerId> = st
            .consumers
            .iter()
            .filter(|(_, c)| c.conn == conn)
            .map(|(id, _)| *id)
            .collect();
        // A safety net: deliveries still out with this connection's
        // consumers go back (the front ends normally settled them).
        let set: HashSet<ConsumerId> = mine.iter().copied().collect();
        let mut touched = Vec::new();
        for (name, q) in &mut st.queues {
            let lost: Vec<u64> = q
                .unacked
                .iter()
                .filter(|(_, (_, c))| c.is_some_and(|c| set.contains(&c)))
                .map(|(k, _)| *k)
                .collect();
            for k in lost {
                if let Some((mut e, _)) = q.unacked.remove(&k) {
                    e.redelivered = true;
                    e.delivery_count += 1;
                    q.ready.insert((e.pclass, e.seq), e);
                }
            }
            touched.push(name.clone());
        }
        for id in mine {
            st.remove_consumer(id);
        }
        let exclusive: Vec<String> = st
            .queues
            .iter()
            .filter(|(_, q)| q.exclusive == Some(conn))
            .map(|(n, _)| n.clone())
            .collect();
        for q in exclusive {
            st.delete_queue(&q);
        }
        for q in touched {
            st.dispatch(&q);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topics() {
        assert!(topic_match("order.paid.*", "order.paid.eu"));
        assert!(!topic_match("order.paid.*", "order.paid"));
        assert!(!topic_match("order.paid.*", "order.paid.eu.x"));
        assert!(topic_match("order.#", "order"));
        assert!(topic_match("order.#", "order.a.b"));
        assert!(topic_match("#", ""));
        assert!(topic_match("#", "a.b"));
        assert!(topic_match("*.b.#", "a.b"));
        assert!(topic_match("a.#.z", "a.z"));
        assert!(topic_match("a.#.z", "a.x.y.z"));
        assert!(!topic_match("a.#.z", "a.x.y"));
        assert!(topic_match("", ""));
        assert!(!topic_match("*", ""));
    }

    #[test]
    fn headers() {
        let h = FieldTable::from([
            ("format", FieldValue::str("pdf")),
            ("type", FieldValue::str("report")),
        ]);
        let all = FieldTable::from([
            ("x-match", FieldValue::str("all")),
            ("format", FieldValue::str("pdf")),
            ("type", FieldValue::str("report")),
        ]);
        assert!(headers_match(&all, &h));
        let any = FieldTable::from([
            ("x-match", FieldValue::str("any")),
            ("format", FieldValue::str("zip")),
            ("type", FieldValue::str("report")),
        ]);
        assert!(headers_match(&any, &h));
        let none = FieldTable::from([
            ("x-match", FieldValue::str("any")),
            ("format", FieldValue::str("zip")),
        ]);
        assert!(!headers_match(&none, &h));
        let int = FieldTable::from([("n", FieldValue::I32(1))]);
        assert!(headers_match(
            &int,
            &FieldTable::from([("n", FieldValue::I64(1))])
        ));
        let with_x = FieldTable::from([
            ("x-match", FieldValue::str("all-with-x")),
            ("x-k", FieldValue::str("v")),
        ]);
        assert!(!headers_match(&with_x, &h));
        assert!(headers_match(
            &with_x,
            &FieldTable::from([("x-k", FieldValue::str("v"))])
        ));
    }
}
