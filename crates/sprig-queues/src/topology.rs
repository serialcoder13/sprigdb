//! The messaging model of one virtual host and routing (chapter 50,
//! "The model, mapped" and "Routing"; build steps 2 and 3).
//!
//! A [`Topology`] is the durable part, read from the catalog snapshot
//! (`mq_exchanges`, `mq_queues`, `mq_bindings`, `mq_policies`,
//! `mq_permissions`). Temporary queues (exclusive, auto-delete and
//! server-named) and their bindings are not in the catalog (the lead's
//! choice, 9 November 2026): they are a [`TempSet`] kept by the gateway
//! that declared them and shared with the other gateways through etcd.
//! Routing reads both, in memory, so `basic.publish` never waits.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use sprig_proto_amqp::backend::DestKind;
use sprig_proto_amqp::mem::{headers_match, topic_match};
use sprig_proto_amqp::v091::codec::{FieldTable, FieldValue};

/// The built-in exchanges every vhost has without rows.
pub const BUILTIN_EXCHANGES: [(&str, ExKind); 6] = [
    ("", ExKind::Direct),
    ("amq.direct", ExKind::Direct),
    ("amq.fanout", ExKind::Fanout),
    ("amq.topic", ExKind::Topic),
    ("amq.headers", ExKind::Headers),
    ("amq.match", ExKind::Headers),
];

/// An exchange type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExKind {
    /// `direct`.
    Direct,
    /// `fanout`.
    Fanout,
    /// `topic`.
    Topic,
    /// `headers`.
    Headers,
    /// `x-delayed-message`: held until due, then routed as its
    /// `x-delayed-type`.
    Delayed,
}

impl ExKind {
    /// Parses an `exchange.declare` type.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "direct" => ExKind::Direct,
            "fanout" => ExKind::Fanout,
            "topic" => ExKind::Topic,
            "headers" => ExKind::Headers,
            "x-delayed-message" => ExKind::Delayed,
            _ => return None,
        })
    }

    /// The type's name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            ExKind::Direct => "direct",
            ExKind::Fanout => "fanout",
            ExKind::Topic => "topic",
            ExKind::Headers => "headers",
            ExKind::Delayed => "x-delayed-message",
        }
    }
}

/// An exchange.
#[derive(Debug, Clone, PartialEq)]
pub struct ExchangeDef {
    /// `objects` counter id (0 for built-ins).
    pub id: u64,
    /// The name.
    pub name: String,
    /// The type.
    pub kind: ExKind,
    /// Durable.
    pub durable: bool,
    /// Deleted with its last binding.
    pub auto_delete: bool,
    /// Not published to by clients.
    pub internal: bool,
    /// `alternate-exchange`, `x-delayed-type`, ...
    pub arguments: FieldTable,
    /// One of [`BUILTIN_EXCHANGES`].
    pub builtin: bool,
}

impl ExchangeDef {
    /// The type messages are routed by: a delayed exchange's
    /// `x-delayed-type` (default `direct`).
    #[must_use]
    pub fn route_kind(&self) -> ExKind {
        if self.kind == ExKind::Delayed {
            self.arguments
                .get("x-delayed-type")
                .and_then(|v| v.as_str().and_then(|s| ExKind::parse(&s)))
                .filter(|k| *k != ExKind::Delayed)
                .unwrap_or(ExKind::Direct)
        } else {
            self.kind
        }
    }

    /// The alternate exchange.
    #[must_use]
    pub fn alternate(&self) -> Option<String> {
        self.arguments
            .get("alternate-exchange")
            .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned))
    }
}

/// A queue type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QType {
    /// `classic`.
    Classic,
    /// `quorum`.
    Quorum,
    /// `stream` (chapter 45; not served by this engine yet).
    Stream,
}

impl QType {
    /// The stored name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            QType::Classic => "classic",
            QType::Quorum => "quorum",
            QType::Stream => "stream",
        }
    }

    /// Parses a stored or declared name.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "classic" => QType::Classic,
            "quorum" => QType::Quorum,
            "stream" => QType::Stream,
            _ => return None,
        })
    }
}

/// Who owns an exclusive queue: a connection of a gateway.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExclusiveOwner {
    /// The gateway id.
    pub gateway: String,
    /// The connection.
    pub conn: u64,
}

/// A queue.
#[derive(Debug, Clone, PartialEq)]
pub struct QueueDef {
    /// The queue id (`DeliveryId::queue`): the object counter for durable
    /// queues, a gateway-local id for temporary ones.
    pub id: u64,
    /// The name.
    pub name: String,
    /// The type.
    pub qtype: QType,
    /// Durable.
    pub durable: bool,
    /// Owned by one connection.
    pub exclusive: Option<ExclusiveOwner>,
    /// Deleted with its last consumer.
    pub auto_delete: bool,
    /// `x-*` arguments as declared.
    pub arguments: FieldTable,
    /// `x-sprig-lanes` (stored; served as one lane, see the crate docs).
    pub lanes: u16,
    /// Temporary: kept in the declaring gateway's memory (`Some(gateway)`).
    pub temp_on: Option<String>,
}

/// A binding.
#[derive(Debug, Clone, PartialEq)]
pub struct BindingDef {
    /// The source exchange.
    pub source: String,
    /// The destination.
    pub dest: String,
    /// Queue or exchange.
    pub dest_kind: DestKind,
    /// The binding key.
    pub key: String,
    /// Arguments (headers exchanges).
    pub arguments: FieldTable,
}

impl BindingDef {
    /// Same source, destination, key and arguments.
    #[must_use]
    pub fn same(&self, o: &BindingDef) -> bool {
        self.source == o.source
            && self.dest == o.dest
            && self.dest_kind == o.dest_kind
            && self.key == o.key
            && self.arguments == o.arguments
    }
}

/// What a policy applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyTo {
    /// Every queue.
    Queues,
    /// Classic queues.
    ClassicQueues,
    /// Quorum queues.
    QuorumQueues,
    /// Streams.
    Streams,
    /// Exchanges.
    Exchanges,
    /// Queues and exchanges.
    All,
}

impl ApplyTo {
    /// Parses a stored name.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "queues" => ApplyTo::Queues,
            "classic_queues" => ApplyTo::ClassicQueues,
            "quorum_queues" => ApplyTo::QuorumQueues,
            "streams" => ApplyTo::Streams,
            "exchanges" => ApplyTo::Exchanges,
            "all" => ApplyTo::All,
            _ => return None,
        })
    }

    fn queues(self, t: QType) -> bool {
        match self {
            ApplyTo::Queues | ApplyTo::All => t != QType::Stream,
            ApplyTo::ClassicQueues => t == QType::Classic,
            ApplyTo::QuorumQueues => t == QType::Quorum,
            ApplyTo::Streams => t == QType::Stream,
            ApplyTo::Exchanges => false,
        }
    }
}

/// A policy (`mq_policies`).
#[derive(Debug, Clone)]
pub struct PolicyDef {
    /// The name.
    pub name: String,
    /// An operator policy (its limits cap the queue's own).
    pub operator: bool,
    /// Matches names.
    pub pattern: regex::Regex,
    /// What it applies to.
    pub apply_to: ApplyTo,
    /// Higher wins.
    pub priority: i32,
    /// RabbitMQ's keys without `x-`: `message-ttl`, `max-length`, ...
    pub definition: serde_json::Map<String, serde_json::Value>,
}

/// A RabbitMQ-style permission rule (`mq_permissions`).
#[derive(Debug, Clone)]
pub struct PermissionDef {
    /// The user.
    pub user_id: u64,
    /// Declare and delete.
    pub configure: regex::Regex,
    /// Publish, bind as destination.
    pub write: regex::Regex,
    /// Consume, get, purge, bind as source.
    pub read: regex::Regex,
}

/// The durable messaging model of one vhost, at one catalog version.
#[derive(Debug, Clone, Default)]
pub struct Topology {
    /// Catalog version it was read at.
    pub version: u64,
    /// Exchanges by name (built-ins included).
    pub exchanges: HashMap<String, Arc<ExchangeDef>>,
    /// Durable queues by name.
    pub queues: HashMap<String, Arc<QueueDef>>,
    /// Bindings.
    pub bindings: Vec<BindingDef>,
    /// Policies, highest priority first.
    pub policies: Vec<PolicyDef>,
    /// Permission rules.
    pub permissions: Vec<PermissionDef>,
    /// `mq_vhosts.limits`: `max-queues`.
    pub max_queues: Option<u64>,
}

impl Topology {
    /// A vhost with only the built-in exchanges.
    #[must_use]
    pub fn new() -> Self {
        let mut t = Self::default();
        t.add_builtins();
        t
    }

    /// Adds the built-in exchanges.
    pub fn add_builtins(&mut self) {
        for (name, kind) in BUILTIN_EXCHANGES {
            self.exchanges.insert(
                name.to_owned(),
                Arc::new(ExchangeDef {
                    id: 0,
                    name: name.to_owned(),
                    kind,
                    durable: true,
                    auto_delete: false,
                    internal: false,
                    arguments: FieldTable::new(),
                    builtin: true,
                }),
            );
        }
    }

    /// The exchange with id `id`.
    #[must_use]
    pub fn exchange_by_id(&self, id: u64) -> Option<&Arc<ExchangeDef>> {
        self.exchanges.values().find(|e| e.id == id && id != 0)
    }
}

/// Temporary queues and their bindings, of every gateway, for one vhost.
#[derive(Debug, Clone, Default)]
pub struct TempSet {
    /// Queues by name.
    pub queues: HashMap<String, Arc<QueueDef>>,
    /// Bindings whose destination is one of them.
    pub bindings: Vec<BindingDef>,
}

/// What routing sees: the durable model and the temporary queues.
#[derive(Debug, Clone, Copy)]
pub struct View<'a> {
    /// Durable.
    pub topo: &'a Topology,
    /// Temporary.
    pub temp: &'a TempSet,
}

impl<'a> View<'a> {
    /// A queue by name.
    #[must_use]
    pub fn queue(&self, name: &str) -> Option<&'a Arc<QueueDef>> {
        self.temp
            .queues
            .get(name)
            .or_else(|| self.topo.queues.get(name))
    }

    /// An exchange by name.
    #[must_use]
    pub fn exchange(&self, name: &str) -> Option<&'a Arc<ExchangeDef>> {
        self.topo.exchanges.get(name)
    }

    /// Every binding.
    pub fn bindings(&self) -> impl Iterator<Item = &'a BindingDef> {
        self.topo.bindings.iter().chain(self.temp.bindings.iter())
    }

    /// Routes a message (chapter 50, "Routing"): the queues reached, each
    /// once. `kind_override` routes `exchange` as another type (a delayed
    /// exchange when its message is due).
    #[must_use]
    pub fn route(&self, exchange: &str, key: &str, headers: &FieldTable) -> Vec<Arc<QueueDef>> {
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        let mut visited = HashSet::new();
        self.route_into(exchange, key, headers, &mut visited, &mut seen, &mut out);
        if out.is_empty()
            && let Some(alt) = self.exchange(exchange).and_then(|e| e.alternate())
        {
            let mut visited = HashSet::new();
            self.route_into(&alt, key, headers, &mut visited, &mut seen, &mut out);
        }
        out
    }

    fn route_into(
        &self,
        exchange: &str,
        key: &str,
        headers: &FieldTable,
        visited: &mut HashSet<String>,
        seen: &mut HashSet<u64>,
        out: &mut Vec<Arc<QueueDef>>,
    ) {
        if !visited.insert(exchange.to_owned()) {
            return;
        }
        let Some(x) = self.exchange(exchange) else {
            return;
        };
        if exchange.is_empty() {
            if let Some(q) = self.queue(key)
                && seen.insert(q.id)
            {
                out.push(Arc::clone(q));
            }
            return;
        }
        let kind = x.route_kind();
        for b in self.bindings().filter(|b| b.source == exchange) {
            let hit = match kind {
                ExKind::Fanout => true,
                ExKind::Topic => topic_match(&b.key, key),
                ExKind::Headers => headers_match(&b.arguments, headers),
                ExKind::Direct | ExKind::Delayed => b.key == key,
            };
            if !hit {
                continue;
            }
            match b.dest_kind {
                DestKind::Queue => {
                    if let Some(q) = self.queue(&b.dest)
                        && seen.insert(q.id)
                    {
                        out.push(Arc::clone(q));
                    }
                }
                DestKind::Exchange => {
                    self.route_into(&b.dest, key, headers, visited, seen, out);
                }
            }
        }
    }
}

/// `x-overflow`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Overflow {
    /// Drop (or dead-letter) the oldest.
    DropHead,
    /// Refuse the new message.
    RejectPublish,
    /// Refuse and dead-letter it (classic only).
    RejectPublishDlx,
}

/// A queue's effective behaviour: its arguments with policies applied.
#[derive(Debug, Clone, PartialEq)]
pub struct QueueCfg {
    /// Quorum rules.
    pub quorum: bool,
    /// Priority classes (`x-max-priority`; quorum: 31).
    pub max_priority: Option<u8>,
    /// `x-message-ttl`.
    pub message_ttl_ms: Option<u64>,
    /// `x-expires`.
    pub expires_ms: Option<u64>,
    /// `x-max-length`.
    pub max_length: Option<u64>,
    /// `x-max-length-bytes`.
    pub max_length_bytes: Option<u64>,
    /// `x-overflow`.
    pub overflow: Overflow,
    /// `x-dead-letter-exchange`.
    pub dlx: Option<String>,
    /// `x-dead-letter-routing-key`.
    pub dlrk: Option<String>,
    /// `x-delivery-limit` (quorum: default 20).
    pub delivery_limit: Option<i64>,
    /// `x-single-active-consumer`.
    pub single_active: bool,
    /// `x-consumer-timeout`.
    pub consumer_timeout_ms: Option<u64>,
}

fn arg_i64(t: &FieldTable, k: &str) -> Option<i64> {
    t.get(k).and_then(FieldValue::as_i64)
}

fn arg_str(t: &FieldTable, k: &str) -> Option<String> {
    t.get(k)
        .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned))
}

impl QueueCfg {
    /// The behaviour of `q` under `policies` (highest priority first): the
    /// queue's own arguments win over a policy, as in RabbitMQ; an
    /// operator policy's numeric limits cap them.
    #[must_use]
    pub fn of(q: &QueueDef, policies: &[PolicyDef], delivery_limit_default: i64) -> Self {
        let a = &q.arguments;
        let user = policies
            .iter()
            .find(|p| !p.operator && p.apply_to.queues(q.qtype) && p.pattern.is_match(&q.name));
        let op = policies
            .iter()
            .find(|p| p.operator && p.apply_to.queues(q.qtype) && p.pattern.is_match(&q.name));
        let pol_i = |p: Option<&PolicyDef>, k: &str| {
            p.and_then(|p| p.definition.get(k))
                .and_then(serde_json::Value::as_i64)
        };
        let pol_s = |p: Option<&PolicyDef>, k: &str| {
            p.and_then(|p| p.definition.get(k))
                .and_then(|v| v.as_str().map(str::to_owned))
        };
        let num = |arg: &str, pk: &str| {
            let own = arg_i64(a, arg).or_else(|| pol_i(user, pk));
            match (own, pol_i(op, pk)) {
                (Some(x), Some(cap)) => Some(x.min(cap)),
                (x, cap) => x.or(cap),
            }
            .and_then(|v| u64::try_from(v).ok())
        };
        let quorum = q.qtype == QType::Quorum;
        let max_priority = if quorum {
            Some(31)
        } else {
            arg_i64(a, "x-max-priority")
                .or_else(|| pol_i(user, "max-priority"))
                .and_then(|p| u8::try_from(p.clamp(1, 255)).ok())
        };
        let overflow = match arg_str(a, "x-overflow")
            .or_else(|| pol_s(user, "overflow"))
            .as_deref()
        {
            Some("reject-publish-dlx") if !quorum => Overflow::RejectPublishDlx,
            Some("reject-publish" | "reject-publish-dlx") => Overflow::RejectPublish,
            _ => Overflow::DropHead,
        };
        let delivery_limit = arg_i64(a, "x-delivery-limit")
            .or_else(|| pol_i(user, "delivery-limit"))
            .or(if quorum {
                Some(delivery_limit_default)
            } else {
                None
            });
        Self {
            quorum,
            max_priority,
            message_ttl_ms: num("x-message-ttl", "message-ttl"),
            expires_ms: num("x-expires", "expires"),
            max_length: num("x-max-length", "max-length"),
            max_length_bytes: num("x-max-length-bytes", "max-length-bytes"),
            overflow,
            dlx: arg_str(a, "x-dead-letter-exchange")
                .or_else(|| pol_s(user, "dead-letter-exchange")),
            dlrk: arg_str(a, "x-dead-letter-routing-key")
                .or_else(|| pol_s(user, "dead-letter-routing-key")),
            delivery_limit,
            single_active: a.flag("x-single-active-consumer"),
            consumer_timeout_ms: num("x-consumer-timeout", "consumer-timeout"),
        }
    }

    /// The priority class of a message (0 is served first).
    #[must_use]
    pub fn pclass(&self, priority: Option<u8>) -> u8 {
        match self.max_priority {
            None => 0,
            Some(max) => {
                let p = priority.unwrap_or(if self.quorum { 4 } else { 0 });
                max - p.min(max)
            }
        }
    }
}

/// Queue arguments whose difference makes a redeclare inequivalent.
pub const EQUIVALENCE_ARGS: [&str; 9] = [
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

/// An argument as RabbitMQ prints it in an inequivalence error.
#[must_use]
pub fn arg_text(t: &FieldTable, k: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn q(name: &str, id: u64) -> Arc<QueueDef> {
        Arc::new(QueueDef {
            id,
            name: name.into(),
            qtype: QType::Classic,
            durable: true,
            exclusive: None,
            auto_delete: false,
            arguments: FieldTable::new(),
            lanes: 1,
            temp_on: None,
        })
    }

    fn x(name: &str, kind: ExKind, args: FieldTable) -> Arc<ExchangeDef> {
        Arc::new(ExchangeDef {
            id: 9,
            name: name.into(),
            kind,
            durable: true,
            auto_delete: false,
            internal: false,
            arguments: args,
            builtin: false,
        })
    }

    fn b(s: &str, d: &str, k: DestKind, key: &str) -> BindingDef {
        BindingDef {
            source: s.into(),
            dest: d.into(),
            dest_kind: k,
            key: key.into(),
            arguments: FieldTable::new(),
        }
    }

    #[test]
    fn routes_every_way() {
        let mut t = Topology::new();
        t.queues.insert("a".into(), q("a", 1));
        t.queues.insert("b".into(), q("b", 2));
        t.exchanges
            .insert("t".into(), x("t", ExKind::Topic, FieldTable::new()));
        t.exchanges.insert(
            "f".into(),
            x(
                "f",
                ExKind::Fanout,
                FieldTable::from([("alternate-exchange", FieldValue::str("t"))]),
            ),
        );
        t.exchanges.insert(
            "d".into(),
            x(
                "d",
                ExKind::Delayed,
                FieldTable::from([("x-delayed-type", FieldValue::str("topic"))]),
            ),
        );
        t.bindings.push(b("t", "a", DestKind::Queue, "o.*"));
        t.bindings.push(b("t", "f", DestKind::Exchange, "o.#"));
        t.bindings.push(b("f", "b", DestKind::Queue, ""));
        t.bindings.push(b("f", "t", DestKind::Exchange, ""));
        t.bindings.push(b("d", "a", DestKind::Queue, "z.*"));
        let temp = TempSet::default();
        let v = View {
            topo: &t,
            temp: &temp,
        };
        let names = |r: Vec<Arc<QueueDef>>| r.iter().map(|q| q.name.clone()).collect::<Vec<_>>();
        assert_eq!(names(v.route("t", "o.x", &FieldTable::new())), ["a", "b"]);
        assert_eq!(names(v.route("", "b", &FieldTable::new())), ["b"]);
        assert!(v.route("", "zz", &FieldTable::new()).is_empty());
        assert_eq!(names(v.route("d", "z.k", &FieldTable::new())), ["a"]);
        // Fanout with no queues: none bound here, so the alternate.
        t.bindings.retain(|x| x.source != "f");
        let v = View {
            topo: &t,
            temp: &temp,
        };
        assert_eq!(names(v.route("f", "o.q", &FieldTable::new())), ["a"]);
    }

    #[test]
    fn policies_and_pclass() {
        let mut qd = (*q("jobs", 1)).clone();
        qd.arguments.insert("x-max-length", FieldValue::I32(10));
        let pol = |op: bool, def: serde_json::Value| PolicyDef {
            name: "p".into(),
            operator: op,
            pattern: regex::Regex::new("^jo").unwrap_or_else(|_| unreachable!()),
            apply_to: ApplyTo::Queues,
            priority: 0,
            definition: def.as_object().cloned().unwrap_or_default(),
        };
        let c = QueueCfg::of(
            &qd,
            &[
                pol(
                    false,
                    serde_json::json!({"max-length": 50, "message-ttl": 7}),
                ),
                pol(true, serde_json::json!({"max-length": 5})),
            ],
            20,
        );
        assert_eq!(c.max_length, Some(5));
        assert_eq!(c.message_ttl_ms, Some(7));
        assert_eq!(c.pclass(Some(3)), 0);
        qd.arguments.insert("x-max-priority", FieldValue::I32(5));
        let c = QueueCfg::of(&qd, &[], 20);
        assert_eq!(
            (c.pclass(Some(5)), c.pclass(Some(0)), c.pclass(None)),
            (0, 5, 5)
        );
    }
}
