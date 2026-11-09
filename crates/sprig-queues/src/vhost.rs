//! One connection's view of a vhost: the [`Vhost`] methods (chapter 50,
//! declare semantics, publishing, consuming, settling). Replies and texts
//! follow RabbitMQ's, as `sprig-proto-amqp`'s in-memory engine does, so
//! the same protocol tests pass against both.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures::future::join_all;
use sprig_proto_amqp::backend::{
    BindingSpec, ConnKey, ConsumeSpec, ConsumerId, DestKind, ExchangeSpec, OpCtx, Published,
    QueueDeclared, QueueSpec, Settlement, TxBatch, Vhost,
};
use sprig_proto_amqp::error::{MqCode, MqError, reply};
use sprig_proto_amqp::mem::Perm;
use sprig_proto_amqp::message::{Delivery, DeliveryId, Message};
use tokio::sync::oneshot;

use crate::engine::{ConsumerRec, LocalSink, TopoChange, VShared, lock};
use crate::msg;
use crate::owner::{Cmd, ConsumerSpec, Holder, Incoming};
use crate::store::{DelayedRow, new_msg_id};
use crate::topology::{
    BindingDef, EQUIVALENCE_ARGS, ExKind, ExchangeDef, ExclusiveOwner, QType, QueueDef, arg_text,
};

/// A connection's vhost.
pub struct ConnVhost {
    v: Arc<VShared>,
    conn: ConnKey,
}

impl std::fmt::Debug for ConnVhost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnVhost")
            .field("vhost", &self.v.r.name)
            .finish_non_exhaustive()
    }
}

fn inequivalent(what: &str, arg: &str, name: &str, vhost: &str, got: &str, cur: &str) -> MqError {
    MqError::precondition(format!(
        "inequivalent arg '{arg}' for {what} '{name}' in vhost '{vhost}': received '{got}' but current is '{cur}'"
    ))
}

fn server_name() -> String {
    let mut b = [0u8; 16];
    let _ = ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut b);
    format!("amq.gen-{}", URL_SAFE_NO_PAD.encode(b))
}

impl ConnVhost {
    pub(crate) fn new(v: Arc<VShared>, conn: ConnKey) -> Self {
        Self { v, conn }
    }

    fn holder(&self) -> Holder {
        Holder {
            origin: 0,
            conn: self.conn,
        }
    }

    fn engine(&self) -> Result<Arc<crate::engine::Inner>, MqError> {
        self.v
            .engine
            .upgrade()
            .ok_or_else(|| MqError::new(MqCode::OwnerMoved, "the queue engine stopped"))
    }

    fn check(&self, cx: &OpCtx, p: Perm, kind: &str, name: &str) -> Result<(), MqError> {
        let e = self.engine()?;
        if e.deps
            .catalog
            .allowed(&cx.principal, &self.v.r, p, kind, name)
        {
            return Ok(());
        }
        let what = match p {
            Perm::Configure => "configure",
            Perm::Write => "write",
            Perm::Read => "read",
        };
        Err(MqError::access_refused(format!(
            "access to {kind} '{name}' in vhost '{}' refused for user '{}' ({what})",
            self.v.r.name, cx.principal.user
        )))
    }

    fn no_exchange(&self, name: &str) -> MqError {
        MqError::not_found(format!("no exchange '{name}' in vhost '{}'", self.v.r.name))
    }

    fn no_queue(&self, name: &str) -> MqError {
        MqError::not_found(format!("no queue '{name}' in vhost '{}'", self.v.r.name))
    }

    fn locked(&self, name: &str) -> MqError {
        MqError::locked(format!(
            "cannot obtain exclusive access to locked queue '{name}' in vhost '{}'. It could be originally declared on another connection or the exclusive property value does not match that of the original declaration.",
            self.v.r.name
        ))
    }

    fn mine(&self, q: &QueueDef) -> bool {
        match &q.exclusive {
            None => true,
            Some(o) => {
                self.engine().is_ok_and(|e| e.deps.leases.me() == o.gateway)
                    && o.conn == self.conn.0
            }
        }
    }

    /// The queue, refreshed from the catalog when missing, checked for
    /// exclusive ownership.
    async fn queue_for(&self, name: &str) -> Result<Arc<QueueDef>, MqError> {
        let q = if let Some(q) = self.v.queue(name) {
            q
        } else {
            self.engine()?.deps.catalog.refresh(&self.v.r).await?;
            self.v.queue(name).ok_or_else(|| self.no_queue(name))?
        };
        if !self.mine(&q) {
            return Err(self.locked(name));
        }
        Ok(q)
    }

    async fn exchange(&self, name: &str) -> Result<Option<Arc<ExchangeDef>>, MqError> {
        let e = self.engine()?;
        if let Some(x) = e.deps.catalog.topology(&self.v.r).exchanges.get(name) {
            return Ok(Some(Arc::clone(x)));
        }
        let t = e.deps.catalog.refresh(&self.v.r).await?;
        Ok(t.exchanges.get(name).cloned())
    }

    async fn info(&self, q: &Arc<QueueDef>) -> (u32, u32) {
        let h = self.v.slot(q);
        let (tx, rx) = oneshot::channel();
        if !h.send(Cmd::Info(tx)) {
            return (0, 0);
        }
        tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or((0, 0))
    }

    async fn publish_temp(&self) {
        if let Ok(e) = self.engine() {
            let set = lock(&self.v.temp).clone();
            e.deps.temps.publish(&self.v.r, &set).await;
        }
    }

    /// Routes and enqueues one message; the confirm resolves after every
    /// destination committed.
    fn route_and_enqueue(&self, m: Message) -> Result<Published, MqError> {
        let e = self.engine()?;
        let topo = e.deps.catalog.topology(&self.v.r);
        let Some(x) = topo.exchanges.get(&m.exchange).cloned() else {
            return Err(self.no_exchange(&m.exchange));
        };
        if x.internal {
            return Err(MqError::access_refused(format!(
                "cannot publish to internal exchange '{}' in vhost '{}'",
                m.exchange, self.v.r.name
            )));
        }
        let s = self.v.settings();
        self.quota(m.info.size, s.quota_bytes_per_sec);
        if x.kind == ExKind::Delayed
            && let Some(d) = msg::delay_ms(&m)
        {
            if u64::try_from(d).unwrap_or(u64::MAX) > s.delay_max_ms {
                return Err(MqError::new(
                    MqCode::DelayOutOfRange,
                    format!(
                        "x-delay {d} is above queue.delay_max_ms ({})",
                        s.delay_max_ms
                    ),
                )
                .with_reply(406));
            }
            let row = DelayedRow {
                exchange_id: x.id,
                exchange: x.name.clone(),
                due_ms: e.deps.clock.now_ms().saturating_add(d),
                msg_id: new_msg_id(),
                routing_key: m.routing_key.clone(),
                fmt: msg::fmt_code(m.format),
                props: m.props.clone(),
                body: m.body.clone(),
            };
            let cat = Arc::clone(&e.deps.catalog);
            let r = self.v.r.clone();
            let wake = Arc::clone(&self.v.wake);
            metrics::counter!("sprig_queue_published_total", "path" => "delayed").increment(1);
            return Ok(Published {
                routed: 1,
                confirm: Box::pin(async move {
                    let store = cat.store(&r).await?;
                    store
                        .delay(&row)
                        .await
                        .map_err(|e| MqError::new(MqCode::OwnerMoved, e.to_string()))?;
                    wake.notify_one();
                    Ok(())
                }),
            });
        }
        let targets = self
            .v
            .with_view(|v| v.route(&m.exchange, &m.routing_key, &m.info.headers))
            .unwrap_or_default();
        if targets.is_empty() {
            return Ok(Published::done(0));
        }
        let msg = Arc::new(m);
        let mut waits = Vec::with_capacity(targets.len());
        for q in &targets {
            let (tx, rx) = oneshot::channel();
            let h = self.v.slot(q);
            let _ = h.send(Cmd::Enqueue(vec![(
                Incoming {
                    msg: Arc::clone(&msg),
                    msg_id: new_msg_id(),
                    dedup: false,
                    deaths: None,
                },
                Some(tx),
            )]));
            waits.push(rx);
        }
        let timeout = Duration::from_millis(s.publish_timeout_ms);
        Ok(Published {
            routed: u32::try_from(targets.len()).unwrap_or(u32::MAX),
            confirm: Box::pin(async move {
                let all = join_all(waits);
                match tokio::time::timeout(timeout, all).await {
                    Err(_) => Err(MqError::new(
                        MqCode::ConfirmTimeout,
                        "not committed within amqp.publish_timeout_ms",
                    )),
                    Ok(rs) => {
                        for r in rs {
                            match r {
                                Ok(Ok(())) => {}
                                Ok(Err(e)) => return Err(e),
                                Err(_) => {
                                    return Err(MqError::new(MqCode::OwnerMoved, "owner stopped"));
                                }
                            }
                        }
                        Ok(())
                    }
                }
            }),
        })
    }

    /// The tenant's publish quota (`amqp.quota_publish_bytes_per_sec`):
    /// over it, the vhost's connections get `connection.blocked` until the
    /// next second.
    fn quota(&self, bytes: u64, per_sec: u64) {
        if per_sec == 0 {
            return;
        }
        let now = self
            .engine()
            .map(|e| e.deps.clock.now_ms() / 1000)
            .unwrap_or(0);
        let over = {
            let mut q = lock(&self.v.quota);
            if q.0 != now {
                *q = (now, 0);
            }
            q.1 += bytes;
            q.1 > per_sec
        };
        if over && !self.v.blocked.swap(true, Ordering::Relaxed) {
            for tx in lock(&self.v.conns).values() {
                let _ = tx.send(sprig_proto_amqp::backend::Event::Blocked(format!(
                    "{} {}: publish quota",
                    MqCode::Quota.number(),
                    MqCode::Quota.name()
                )));
            }
            let v = Arc::clone(&self.v);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                v.blocked.store(false, Ordering::Relaxed);
                for tx in lock(&v.conns).values() {
                    let _ = tx.send(sprig_proto_amqp::backend::Event::Unblocked);
                }
            });
        }
    }
}

#[async_trait]
impl Vhost for ConnVhost {
    fn name(&self) -> &str {
        &self.v.r.name
    }

    async fn declare_exchange(
        &self,
        cx: &OpCtx,
        spec: ExchangeSpec,
        passive: bool,
    ) -> Result<(), MqError> {
        if spec.name.is_empty() {
            if passive {
                return Ok(());
            }
            return Err(MqError::access_refused(
                "operation not permitted on the default exchange",
            ));
        }
        let cur = self.exchange(&spec.name).await?;
        if passive {
            return cur.map(|_| ()).ok_or_else(|| self.no_exchange(&spec.name));
        }
        let Some(kind) = ExKind::parse(&spec.kind) else {
            return Err(MqError::frame(
                reply::COMMAND_INVALID,
                format!("invalid exchange type '{}'", spec.kind),
            ));
        };
        let vname = self.v.r.name.clone();
        let check = |x: &ExchangeDef| -> Result<(), MqError> {
            let name = &spec.name;
            if x.kind != kind {
                return Err(inequivalent(
                    "exchange",
                    "type",
                    name,
                    &vname,
                    &spec.kind,
                    x.kind.as_str(),
                ));
            }
            for (arg, a, b) in [
                ("durable", spec.durable, x.durable),
                ("auto_delete", spec.auto_delete, x.auto_delete),
                ("internal", spec.internal, x.internal),
            ] {
                if a != b {
                    return Err(inequivalent(
                        "exchange",
                        arg,
                        name,
                        &vname,
                        &a.to_string(),
                        &b.to_string(),
                    ));
                }
            }
            for k in ["alternate-exchange", "x-delayed-type"] {
                if !x.builtin && arg_text(&x.arguments, k) != arg_text(&spec.arguments, k) {
                    return Err(inequivalent(
                        "exchange",
                        k,
                        name,
                        &vname,
                        &arg_text(&spec.arguments, k),
                        &arg_text(&x.arguments, k),
                    ));
                }
            }
            Ok(())
        };
        if let Some(x) = cur {
            return check(&x);
        }
        if spec.name.starts_with("amq.") {
            return Err(MqError::access_refused(format!(
                "exchange name '{}' contains reserved prefix 'amq.*'",
                spec.name
            )));
        }
        self.check(cx, Perm::Configure, "exchange", &spec.name)?;
        let t = self
            .engine()?
            .deps
            .catalog
            .change(
                &self.v.r,
                TopoChange::AddExchange(ExchangeDef {
                    id: 0,
                    name: spec.name.clone(),
                    kind,
                    durable: spec.durable,
                    auto_delete: spec.auto_delete,
                    internal: spec.internal,
                    arguments: spec.arguments.clone(),
                    builtin: false,
                }),
            )
            .await?;
        // Declared at the same time elsewhere: the first one wins.
        match t.exchanges.get(&spec.name) {
            Some(x) => check(x),
            None => Ok(()),
        }
    }

    async fn delete_exchange(
        &self,
        cx: &OpCtx,
        name: &str,
        if_unused: bool,
    ) -> Result<(), MqError> {
        let cur = self.exchange(name).await?;
        if name.is_empty() || cur.as_ref().is_some_and(|x| x.builtin) || name.starts_with("amq.") {
            return Err(MqError::access_refused(format!(
                "operation not permitted on the default exchange or '{name}'"
            )));
        }
        if cur.is_none() {
            return Ok(());
        }
        self.check(cx, Perm::Configure, "exchange", name)?;
        let bound = self
            .v
            .with_view(|v| v.bindings().any(|b| b.source == name))
            .unwrap_or(false);
        if if_unused && bound {
            return Err(MqError::precondition(format!(
                "exchange '{name}' in vhost '{}' in use",
                self.v.r.name
            )));
        }
        {
            let mut t = lock(&self.v.temp);
            t.bindings.retain(|b| {
                b.source != name && !(b.dest_kind == DestKind::Exchange && b.dest == name)
            });
        }
        self.publish_temp().await;
        self.engine()?
            .deps
            .catalog
            .change(&self.v.r, TopoChange::DelExchange(name.to_owned()))
            .await?;
        Ok(())
    }

    async fn declare_queue(
        &self,
        cx: &OpCtx,
        mut spec: QueueSpec,
        passive: bool,
    ) -> Result<QueueDeclared, MqError> {
        if passive {
            let q = self.queue_for(&spec.name).await?;
            let (m, c) = self.info(&q).await;
            return Ok(QueueDeclared {
                name: spec.name,
                message_count: m,
                consumer_count: c,
            });
        }
        let server_named = spec.name.is_empty();
        if server_named {
            spec.name = server_name();
        }
        let existing = if let Some(q) = self.v.queue(&spec.name) {
            Some(q)
        } else {
            self.engine()?.deps.catalog.refresh(&self.v.r).await?;
            self.v.queue(&spec.name)
        };
        if !server_named && spec.name.starts_with("amq.") && existing.is_none() {
            return Err(MqError::access_refused(format!(
                "queue name '{}' contains reserved prefix 'amq.*'",
                spec.name
            )));
        }
        let settings = self.v.settings();
        let qtype_s = spec
            .arguments
            .get("x-queue-type")
            .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned))
            .unwrap_or_else(|| {
                if settings.default_quorum && spec.durable && !spec.exclusive {
                    "quorum".into()
                } else {
                    "classic".into()
                }
            });
        let qtype = match QType::parse(&qtype_s) {
            Some(QType::Classic) => QType::Classic,
            Some(QType::Quorum) => {
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
                QType::Quorum
            }
            Some(QType::Stream) => {
                return Err(MqError::not_implemented(
                    "stream queues are not served by the queue engine yet (chapter 50 step 17)",
                ));
            }
            None => {
                return Err(MqError::precondition(format!(
                    "unknown queue type '{qtype_s}'"
                )));
            }
        };
        if let Some(q) = existing {
            if !self.mine(&q) || q.exclusive.is_some() != spec.exclusive {
                return Err(self.locked(&spec.name));
            }
            let v = self.v.r.name.clone();
            for (arg, a, b) in [
                ("durable", spec.durable, q.durable),
                ("auto_delete", spec.auto_delete, q.auto_delete),
            ] {
                if a != b {
                    return Err(inequivalent(
                        "queue",
                        arg,
                        &spec.name,
                        &v,
                        &a.to_string(),
                        &b.to_string(),
                    ));
                }
            }
            for k in EQUIVALENCE_ARGS {
                let (a, b) = (arg_text(&spec.arguments, k), arg_text(&q.arguments, k));
                let default_classic = k == "x-queue-type"
                    && (a == "classic" || a == "none")
                    && (b == "classic" || b == "none");
                if a != b && !default_classic {
                    return Err(inequivalent("queue", k, &spec.name, &v, &a, &b));
                }
            }
            let (m, c) = self.info(&q).await;
            return Ok(QueueDeclared {
                name: spec.name,
                message_count: m,
                consumer_count: c,
            });
        }
        self.check(cx, Perm::Configure, "queue", &spec.name)?;
        let e = self.engine()?;
        let topo = e.deps.catalog.topology(&self.v.r);
        if let Some(max) = topo.max_queues {
            let n = topo.queues.len() as u64 + lock(&self.v.temp).queues.len() as u64;
            if n >= max {
                return Err(MqError::new(
                    MqCode::VhostLimit,
                    format!(
                        "cannot declare queue '{}': queue limit in vhost '{}' ({max}) is reached",
                        spec.name, self.v.r.name
                    ),
                )
                .with_reply(406));
            }
        }
        let lanes = spec
            .arguments
            .get("x-sprig-lanes")
            .and_then(sprig_proto_amqp::v091::codec::FieldValue::as_i64)
            .and_then(|n| u16::try_from(n.clamp(1, 1024)).ok())
            .unwrap_or(1);
        let temp = spec.exclusive || spec.auto_delete || server_named;
        let mut def = QueueDef {
            id: 0,
            name: spec.name.clone(),
            qtype,
            durable: spec.durable,
            exclusive: spec.exclusive.then(|| ExclusiveOwner {
                gateway: e.deps.leases.me().to_owned(),
                conn: self.conn.0,
            }),
            auto_delete: spec.auto_delete,
            arguments: spec.arguments,
            lanes,
            temp_on: None,
        };
        if temp {
            def.id = e.temp_id();
            def.temp_on = Some(e.deps.leases.me().to_owned());
            lock(&self.v.temp)
                .queues
                .insert(def.name.clone(), Arc::new(def));
            self.publish_temp().await;
        } else {
            let t = e
                .deps
                .catalog
                .change(&self.v.r, TopoChange::AddQueue(def))
                .await?;
            if let Some(q) = t.queues.get(&spec.name) {
                let (m, c) = self.info(q).await;
                return Ok(QueueDeclared {
                    name: spec.name,
                    message_count: m,
                    consumer_count: c,
                });
            }
        }
        Ok(QueueDeclared {
            name: spec.name,
            message_count: 0,
            consumer_count: 0,
        })
    }

    async fn delete_queue(
        &self,
        cx: &OpCtx,
        name: &str,
        if_unused: bool,
        if_empty: bool,
    ) -> Result<u32, MqError> {
        let q = match self.queue_for(name).await {
            Ok(q) => q,
            Err(e) if e.code == MqCode::NotFound => return Ok(0),
            Err(e) => return Err(e),
        };
        self.check(cx, Perm::Configure, "queue", name)?;
        let h = self.v.slot(&q);
        let (tx, rx) = oneshot::channel();
        if !h.send(Cmd::Delete {
            if_unused,
            if_empty,
            reply: tx,
        }) {
            return Err(MqError::new(MqCode::OwnerMoved, "owner stopped"));
        }
        let n = rx
            .await
            .map_err(|_| MqError::new(MqCode::OwnerMoved, "owner stopped"))??;
        lock(&self.v.owners).remove(&q.id);
        self.v.forget_queue(&q).await;
        Ok(n)
    }

    async fn purge_queue(&self, cx: &OpCtx, name: &str) -> Result<u32, MqError> {
        let q = self.queue_for(name).await?;
        self.check(cx, Perm::Read, "queue", name)?;
        let (tx, rx) = oneshot::channel();
        if !self.v.slot(&q).send(Cmd::Purge(tx)) {
            return Err(MqError::new(MqCode::OwnerMoved, "owner stopped"));
        }
        rx.await
            .map_err(|_| MqError::new(MqCode::OwnerMoved, "owner stopped"))?
    }

    async fn bind(&self, cx: &OpCtx, b: BindingSpec) -> Result<(), MqError> {
        if b.source.is_empty() || (b.dest_kind == DestKind::Exchange && b.destination.is_empty()) {
            return Err(MqError::access_refused(
                "operation not permitted on the default exchange",
            ));
        }
        if self.exchange(&b.source).await?.is_none() {
            return Err(self.no_exchange(&b.source));
        }
        let temp_dest = match b.dest_kind {
            DestKind::Queue => self.queue_for(&b.destination).await?.temp_on.is_some(),
            DestKind::Exchange => {
                if self.exchange(&b.destination).await?.is_none() {
                    return Err(self.no_exchange(&b.destination));
                }
                false
            }
        };
        let kind = if b.dest_kind == DestKind::Queue {
            "queue"
        } else {
            "exchange"
        };
        self.check(cx, Perm::Write, kind, &b.destination)?;
        self.check(cx, Perm::Read, "exchange", &b.source)?;
        let def = BindingDef {
            source: b.source,
            dest: b.destination,
            dest_kind: b.dest_kind,
            key: b.key,
            arguments: b.arguments,
        };
        if temp_dest {
            {
                let mut t = lock(&self.v.temp);
                if !t.bindings.iter().any(|x| x.same(&def)) {
                    t.bindings.push(def);
                }
            }
            self.publish_temp().await;
            return Ok(());
        }
        let e = self.engine()?;
        if e.deps
            .catalog
            .topology(&self.v.r)
            .bindings
            .iter()
            .any(|x| x.same(&def))
        {
            return Ok(());
        }
        e.deps
            .catalog
            .change(&self.v.r, TopoChange::Bind(def))
            .await?;
        Ok(())
    }

    async fn unbind(&self, cx: &OpCtx, b: BindingSpec) -> Result<(), MqError> {
        if b.dest_kind == DestKind::Queue
            && let Some(q) = self.v.queue(&b.destination)
            && !self.mine(&q)
        {
            return Err(self.locked(&b.destination));
        }
        let kind = if b.dest_kind == DestKind::Queue {
            "queue"
        } else {
            "exchange"
        };
        self.check(cx, Perm::Write, kind, &b.destination)?;
        let def = BindingDef {
            source: b.source.clone(),
            dest: b.destination,
            dest_kind: b.dest_kind,
            key: b.key,
            arguments: b.arguments,
        };
        let had_temp = {
            let mut t = lock(&self.v.temp);
            let n = t.bindings.len();
            t.bindings.retain(|x| !x.same(&def));
            n != t.bindings.len()
        };
        let e = self.engine()?;
        if had_temp {
            self.publish_temp().await;
        } else if e
            .deps
            .catalog
            .topology(&self.v.r)
            .bindings
            .iter()
            .any(|x| x.same(&def))
        {
            e.deps
                .catalog
                .change(&self.v.r, TopoChange::Unbind(def))
                .await?;
        }
        // An auto-delete exchange goes with its last binding.
        let auto = e
            .deps
            .catalog
            .topology(&self.v.r)
            .exchanges
            .get(&b.source)
            .is_some_and(|x| x.auto_delete && !x.builtin);
        let bound = self
            .v
            .with_view(|v| v.bindings().any(|x| x.source == b.source))
            .unwrap_or(true);
        if auto && !bound {
            e.deps
                .catalog
                .change(&self.v.r, TopoChange::DelExchange(b.source))
                .await?;
        }
        Ok(())
    }

    fn publish(&self, cx: &OpCtx, m: Message, _mandatory: bool) -> Result<Published, MqError> {
        self.check(cx, Perm::Write, "exchange", &m.exchange)?;
        self.route_and_enqueue(m)
    }

    async fn consume(&self, cx: &OpCtx, spec: ConsumeSpec) -> Result<(), MqError> {
        let q = self.queue_for(&spec.queue).await?;
        self.check(cx, Perm::Read, "queue", &spec.queue)?;
        let Some(events) = lock(&self.v.conns).get(&self.conn).cloned() else {
            return Err(MqError::new(MqCode::OwnerMoved, "connection closed"));
        };
        let rec = Arc::new(ConsumerRec {
            conn: self.conn,
            queue: spec.queue.clone(),
            queue_id: AtomicU64::new(q.id),
            no_ack: spec.no_ack,
            exclusive: spec.exclusive,
            priority: spec.priority,
            credit_out: AtomicU32::new(0),
            held: std::sync::Mutex::default(),
        });
        let cs = ConsumerSpec {
            id: spec.consumer,
            holder: self.holder(),
            sink: Arc::new(LocalSink {
                events,
                rec: Arc::clone(&rec),
                vhost: Arc::downgrade(&self.v),
            }),
            no_ack: spec.no_ack,
            exclusive: spec.exclusive,
            priority: spec.priority,
            held: Vec::new(),
            from: None,
        };
        lock(&self.v.consumers).insert(spec.consumer, rec);
        let (tx, rx) = oneshot::channel();
        let ok = self.v.slot(&q).send(Cmd::Attach(cs, tx));
        let r = if ok {
            rx.await
                .unwrap_or_else(|_| Err(MqError::new(MqCode::OwnerMoved, "owner stopped")))
        } else {
            Err(MqError::new(MqCode::OwnerMoved, "owner stopped"))
        };
        if r.is_err() {
            lock(&self.v.consumers).remove(&spec.consumer);
        }
        r
    }

    async fn cancel(&self, _cx: &OpCtx, consumer: ConsumerId) {
        let Some(rec) = lock(&self.v.consumers).remove(&consumer) else {
            return;
        };
        if let Some(h) = self.v.slot_of(rec.queue_id.load(Ordering::Relaxed)) {
            let _ = h.send(Cmd::Cancel(consumer));
        }
    }

    fn credit(&self, consumer: ConsumerId, credit: u32, drain: bool) {
        let Some(rec) = lock(&self.v.consumers).get(&consumer).cloned() else {
            return;
        };
        rec.credit_out.fetch_add(credit, Ordering::Relaxed);
        if let Some(h) = self.v.slot_of(rec.queue_id.load(Ordering::Relaxed)) {
            let _ = h.send(Cmd::Credit {
                consumer,
                credit,
                drain,
            });
        }
    }

    async fn get(
        &self,
        cx: &OpCtx,
        queue: &str,
        no_ack: bool,
    ) -> Result<Option<(Delivery, u32)>, MqError> {
        let q = self.queue_for(queue).await?;
        self.check(cx, Perm::Read, "queue", queue)?;
        let (tx, rx) = oneshot::channel();
        if !self.v.slot(&q).send(Cmd::Get {
            holder: self.holder(),
            no_ack,
            lease_ms: None,
            reply: tx,
        }) {
            return Err(MqError::new(MqCode::OwnerMoved, "owner stopped"));
        }
        rx.await
            .map_err(|_| MqError::new(MqCode::OwnerMoved, "owner stopped"))?
    }

    fn settle(&self, _cx: &OpCtx, s: Vec<(DeliveryId, Settlement)>) {
        let mut by_q: HashMap<u64, Vec<(DeliveryId, Settlement)>> = HashMap::new();
        for (id, how) in s {
            if id.queue == 0 && id.seq == 0 {
                continue; // direct reply-to: nothing to settle
            }
            by_q.entry(id.queue).or_default().push((id, how));
        }
        // The consumers no longer hold these (re-attachment reports).
        for r in lock(&self.v.consumers).values() {
            if r.conn != self.conn || r.no_ack {
                continue;
            }
            if let Some(items) = by_q.get(&r.queue_id.load(Ordering::Relaxed)) {
                let mut held = lock(&r.held);
                for (id, _) in items {
                    held.remove(id);
                }
            }
        }
        for (q, items) in by_q {
            // No owner: the delivery comes back with the next one.
            if let Some(h) = self.v.slot_of(q) {
                let _ = h.send(Cmd::Settle(self.holder(), items));
            }
        }
    }

    async fn commit_tx(&self, cx: &OpCtx, tx: TxBatch) -> Result<Vec<u32>, MqError> {
        // Check everything first, so nothing applies on failure.
        for (m, _) in &tx.publishes {
            self.check(cx, Perm::Write, "exchange", &m.exchange)?;
            if self.exchange(&m.exchange).await?.is_none() {
                return Err(self.no_exchange(&m.exchange));
            }
        }
        let mut counts = Vec::with_capacity(tx.publishes.len());
        let mut confirms = Vec::new();
        for (m, _) in tx.publishes {
            match self.route_and_enqueue(m) {
                Ok(p) => {
                    counts.push(p.routed);
                    confirms.push(p.confirm);
                }
                Err(_) => counts.push(0),
            }
        }
        for c in confirms {
            match c.await {
                Ok(()) => {}
                Err(e) if e.code == MqCode::QueueFull => {}
                Err(e) => return Err(e),
            }
        }
        self.settle(cx, tx.settlements);
        Ok(counts)
    }

    async fn connection_closed(&self, conn: ConnKey) {
        lock(&self.v.conns).remove(&conn);
        lock(&self.v.consumers).retain(|_, r| r.conn != conn);
        let holder = Holder { origin: 0, conn };
        let owners: Vec<_> = lock(&self.v.owners).values().cloned().collect();
        for h in owners {
            let _ = h.send(Cmd::ConnLost(holder));
        }
        let me = self
            .engine()
            .map(|e| e.deps.leases.me().to_owned())
            .unwrap_or_default();
        let exclusive: Vec<Arc<QueueDef>> = lock(&self.v.temp)
            .queues
            .values()
            .filter(|q| {
                q.exclusive
                    .as_ref()
                    .is_some_and(|o| o.conn == conn.0 && o.gateway == me)
            })
            .cloned()
            .collect();
        for q in exclusive {
            self.v.drop_queue(&q).await;
        }
    }
}
