//! Queues from SQL (chapter 50, "SQL, logic, MongoDB and HTTP";
//! build steps 9 and 10): `CALL sprig.mq_publish(...)`, `mq_get`,
//! `mq_ack` and `mq_nack`.
//!
//! The gateway evaluates the arguments in the session (so any expression
//! works) and calls the engine here:
//!
//! - **Publishing outside a transaction** routes and enqueues at once and
//!   returns the number of queues reached, after the owners committed.
//! - **Publishing inside a transaction** routes at once (so `mandatory` and
//!   the count are known) and gives the gateway an outbox row
//!   (`__sprig_qo`, kind `publish`) to insert in the client's own
//!   transaction: the message exists only if the transaction commits. The
//!   forwarder enqueues committed rows with the row's id as the message
//!   id, so a replay is a no-op (`uk_msg`). A delayed exchange's message
//!   becomes a `__sprig_qd` row the same way.
//! - **Acknowledging** takes a delivery token (`x-sprig-delivery`, or the
//!   first column of `mq_get`): the owner holds the delivery (no timeout,
//!   no redelivery), the gateway deletes the message row in the client's
//!   transaction, and the owner learns the outcome when the client says
//!   (outside a transaction: at once) or, at the latest, by reading the row
//!   under a lock after `hold_check_ms`. MariaDB's row lock decides, never
//!   timing: a delete that finds no row is 39258 and the transaction rolls
//!   back.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use serde_json::{Value, json};
use sprig_proto_amqp::auth::Principal;
use sprig_proto_amqp::backend::{ConnKey, OpCtx, Settlement, Vhost};
use sprig_proto_amqp::convert;
use sprig_proto_amqp::error::{MqCode, MqError};
use sprig_proto_amqp::mem::Perm;
use sprig_proto_amqp::message::{Delivery, Message};
use sprig_proto_amqp::v091::props::BasicProperties;
use tokio::sync::oneshot;

use crate::catalog::{json_table, table_json};
use crate::engine::{QueueEngine, VShared, VhostRef};
use crate::msg;
use crate::owner::{ANY_HOLDER, Cmd, Holder};
use crate::store::{DelayedRow, OutDest, OutKind, OutboxRow, new_msg_id};
use crate::token::{self, TokenData};
use crate::topology::ExKind;
use crate::vhost::ConnVhost;

/// Which call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MqCall {
    /// `sprig.mq_publish(exchange, routing_key, body, properties [, mandatory [, vhost]])`.
    Publish,
    /// `sprig.mq_get(queue [, max [, lease_ms [, vhost [, lease_tag]]]])`.
    /// With a lease tag (R9, r9tests), the messages are held under the
    /// tag rather than the session, so `mq_release` can give them back.
    Get,
    /// `sprig.mq_ack(token)`.
    Ack,
    /// `sprig.mq_nack(token, requeue)`.
    Nack,
    /// `sprig.mq_info(queue [, vhost])`: ready, consumers, unacked (ST50);
    /// with no queue (`mq_info()`, `mq_info(NULL, vhost)`), one row per
    /// queue the user may read (ST66).
    Info,
    /// `sprig.mq_browse(queue [, max [, vhost]])`: messages, not consumed.
    Browse,
    /// `sprig.mq_purge(queue [, vhost])`: ready messages removed.
    Purge,
    /// `sprig.mq_republish(queue, exchange [, routing_key [, max [, vhost]]])`:
    /// moves up to `max` messages (a dead-letter queue's, say) to an
    /// exchange, with their own routing key when `routing_key` is NULL.
    Republish,
    /// `sprig.mq_release(queue, lease_tag [, vhost])` (R9, r9tests): every
    /// message of `queue` held under `lease_tag` (by `mq_get`'s fifth
    /// argument) goes back to the queue at once, as when an AMQP
    /// connection closes. A queue sink's new task gives back a dead
    /// task's messages this way instead of waiting out their lease.
    Release,
}

/// A parsed `CALL sprig.mq_*(...)`: the call and its argument texts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Call {
    /// Which.
    pub call: MqCall,
    /// Each argument's SQL text, as written.
    pub args: Vec<String>,
}

/// Parses `CALL sprig.mq_<name>(<args>)`; `None` for anything else.
#[must_use]
pub fn parse_call(sql: &str) -> Option<Call> {
    let s = sql.trim().trim_end_matches(';').trim();
    let rest = strip_word(s, "call")?;
    let rest = rest.trim_start();
    let rest = rest
        .strip_prefix("`sprig`")
        .or_else(|| strip_word(rest, "sprig"))?;
    let rest = rest.trim_start().strip_prefix('.')?.trim_start();
    let (name, rest) = rest.split_at(rest.find(|c: char| c == '(' || c.is_whitespace())?);
    let call = match name.trim_matches('`').to_ascii_lowercase().as_str() {
        "mq_publish" => MqCall::Publish,
        "mq_get" => MqCall::Get,
        "mq_ack" => MqCall::Ack,
        "mq_nack" => MqCall::Nack,
        "mq_info" => MqCall::Info,
        "mq_browse" => MqCall::Browse,
        "mq_purge" => MqCall::Purge,
        "mq_republish" => MqCall::Republish,
        "mq_release" => MqCall::Release,
        _ => return None,
    };
    let inner = rest.trim().strip_prefix('(')?.strip_suffix(')')?;
    Some(Call {
        call,
        args: split_args(inner)?,
    })
}

fn strip_word<'a>(s: &'a str, w: &str) -> Option<&'a str> {
    let head = s.get(..w.len())?;
    if !head.eq_ignore_ascii_case(w) {
        return None;
    }
    let rest = &s[w.len()..];
    if rest.starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_') {
        return None;
    }
    Some(rest)
}

/// Splits arguments at top-level commas (outside quotes and parentheses).
fn split_args(s: &str) -> Option<Vec<String>> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut depth = 0i32;
    let mut quote: Option<char> = None;
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if let Some(q) = quote {
            cur.push(c);
            if c == '\\' {
                if let Some(n) = chars.next() {
                    cur.push(n);
                }
            } else if c == q {
                if chars.peek() == Some(&q) {
                    cur.push(chars.next()?);
                } else {
                    quote = None;
                }
            }
            continue;
        }
        match c {
            '\'' | '"' | '`' => {
                quote = Some(c);
                cur.push(c);
            }
            '(' => {
                depth += 1;
                cur.push(c);
            }
            ')' => {
                depth -= 1;
                cur.push(c);
            }
            ',' if depth == 0 => out.push(std::mem::take(&mut cur).trim().to_owned()),
            _ => cur.push(c),
        }
    }
    if quote.is_some() || depth != 0 {
        return None;
    }
    if !cur.trim().is_empty() || !out.is_empty() {
        out.push(cur.trim().to_owned());
    }
    Some(out)
}

/// A message published from SQL: a 0-9-1 message whose properties come
/// from a JSON object (`content_type`, `content_encoding`, `headers`,
/// `delivery_mode`, `priority`, `correlation_id`, `reply_to`,
/// `expiration`, `message_id`, `timestamp`, `type`, `user_id`, `app_id`).
/// Messages are persistent unless `delivery_mode` says 1.
///
/// # Errors
/// 406 for a malformed properties object.
pub fn message(
    exchange: &str,
    routing_key: &str,
    body: Vec<u8>,
    props: Option<&str>,
) -> Result<Message, MqError> {
    let v: Value = match props {
        None => Value::Null,
        Some(s) if s.trim().is_empty() => Value::Null,
        Some(s) => serde_json::from_str(s)
            .map_err(|e| MqError::precondition(format!("properties are not a JSON object: {e}")))?,
    };
    if !v.is_null() && !v.is_object() {
        return Err(MqError::precondition("properties are not a JSON object"));
    }
    let s = |k: &str| {
        v.get(k).and_then(|x| match x {
            Value::String(s) => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            _ => None,
        })
    };
    let n = |k: &str| v.get(k).and_then(Value::as_u64);
    let p = BasicProperties {
        content_type: s("content_type"),
        content_encoding: s("content_encoding"),
        headers: v.get("headers").filter(|h| h.is_object()).map(json_table),
        delivery_mode: Some(
            n("delivery_mode")
                .and_then(|d| u8::try_from(d).ok())
                .unwrap_or(2),
        ),
        priority: n("priority").and_then(|d| u8::try_from(d).ok()),
        correlation_id: s("correlation_id"),
        reply_to: s("reply_to"),
        expiration: s("expiration"),
        message_id: s("message_id"),
        timestamp: n("timestamp"),
        kind: s("type"),
        user_id: s("user_id"),
        app_id: s("app_id"),
        cluster_id: None,
    };
    let mut raw = BytesMut::new();
    p.encode(&mut raw);
    convert::from_091(
        exchange.to_owned(),
        routing_key.to_owned(),
        raw.freeze(),
        &p,
        Bytes::from(body),
    )
}

/// A delivery's properties as the JSON `mq_get` returns.
#[must_use]
pub fn properties_json(d: &Delivery) -> String {
    let Ok((raw, _)) = convert::to_091(d) else {
        return "{}".into();
    };
    let Ok(p) = BasicProperties::decode(&raw) else {
        return "{}".into();
    };
    let mut o = serde_json::Map::new();
    let mut put = |k: &str, v: Option<Value>| {
        if let Some(v) = v {
            o.insert(k.to_owned(), v);
        }
    };
    put("content_type", p.content_type.map(Value::String));
    put("content_encoding", p.content_encoding.map(Value::String));
    put("headers", p.headers.as_ref().map(table_json));
    put("delivery_mode", p.delivery_mode.map(|d| json!(d)));
    put("priority", p.priority.map(|d| json!(d)));
    put("correlation_id", p.correlation_id.map(Value::String));
    put("reply_to", p.reply_to.map(Value::String));
    put("expiration", p.expiration.map(Value::String));
    put("message_id", p.message_id.map(Value::String));
    put("timestamp", p.timestamp.map(|d| json!(d)));
    put("type", p.kind.map(Value::String));
    put("user_id", p.user_id.map(Value::String));
    put("app_id", p.app_id.map(Value::String));
    Value::Object(o).to_string()
}

/// The body of a delivery as `mq_get` returns it (0-9-1 body bytes).
#[must_use]
pub fn body_bytes(d: &Delivery) -> Bytes {
    convert::to_091(d).map(|(_, b)| b).unwrap_or_default()
}

/// `mq_info`'s answer (R9, ST50).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueueInfo {
    /// Ready messages.
    pub ready: u64,
    /// Consumers attached.
    pub consumers: u64,
    /// Delivered, not yet acknowledged.
    pub unacked: u64,
}

/// What a publish inside a transaction writes in the client's
/// transaction.
#[derive(Debug, Clone)]
pub enum TxnWrite {
    /// An outbox row, and how many queues it reaches.
    Outbox(OutboxRow, u32),
    /// A delayed message.
    Delayed(DelayedRow),
    /// Nothing: no queue (and not mandatory).
    Nothing,
}

/// A SQL session's use of one vhost.
pub struct SqlVhost {
    v: Arc<VShared>,
    cv: ConnVhost,
    cx: OpCtx,
}

impl std::fmt::Debug for SqlVhost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlVhost")
            .field("vhost", &self.v.r)
            .finish_non_exhaustive()
    }
}

/// SQL sessions' connection keys: this bit and the session id, apart from
/// AMQP connections' process counter.
pub const SQL_CONN_BIT: u64 = 1 << 62;

/// Lease tags' connection keys (R9, r9tests): this bit and a hash of the
/// tag. Unlike a session's or an AMQP connection's key, a tag's key means
/// the same on every gateway, so the owner matches it whatever link the
/// get came over ([`crate::owner`]'s `conn_lost`).
pub const LEASE_TAG_BIT: u64 = 1 << 61;

/// The connection key messages taken with lease tag `tag` are held
/// under: [`LEASE_TAG_BIT`] and the tag's 64-bit FNV-1a hash (stable
/// across builds and gateways), cut to the bits below it.
#[must_use]
pub fn lease_tag_conn(tag: &str) -> ConnKey {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in tag.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    ConnKey(LEASE_TAG_BIT | (h & (LEASE_TAG_BIT - 1)))
}

fn unroutable(exchange: &str, key: &str) -> MqError {
    MqError::new(
        MqCode::Unroutable,
        format!("message to exchange '{exchange}' with routing key '{key}' reached no queue"),
    )
    .with_reply(312)
}

impl QueueEngine {
    /// Opens database `db` as a vhost for a SQL session (`session` is its
    /// connection id), checking the user may use it.
    ///
    /// # Errors
    /// 39250, 39253, or the catalog's.
    pub async fn sql_vhost(
        &self,
        principal: Arc<Principal>,
        db: &str,
        session: u64,
    ) -> Result<SqlVhost, MqError> {
        let r = self.inner.deps.catalog.open(&principal, db).await?;
        self.inner.deps.catalog.refresh(&r).await?;
        let v = self.vshared(&r);
        let conn = ConnKey(SQL_CONN_BIT | session);
        Ok(SqlVhost {
            cv: ConnVhost::new(Arc::clone(&v), conn),
            v,
            cx: OpCtx {
                principal,
                conn,
                channel: 0,
            },
        })
    }

    /// An acknowledgement inside session `session`'s open transaction:
    /// its owner is told how the transaction ended
    /// ([`QueueEngine::txn_ended`]), and checks the row itself after
    /// `hold_check_ms` if nobody says.
    pub fn note_txn_hold(&self, session: u64, t: TokenData) {
        let now = self.inner.deps.clock.now_ms();
        let mut g = crate::engine::lock(&self.inner.txn_holds);
        // Sessions that ended without a word (their owners checked the
        // rows themselves) go after ten minutes.
        g.retain(|_, v| v.iter().any(|(at, _)| now - at < 600_000));
        g.entry(session).or_default().push((now, t));
    }

    /// Whether session `session` has acknowledgements waiting for its
    /// transaction to end (checked after each statement; cheap).
    #[must_use]
    pub fn has_txn_holds(&self, session: u64) -> bool {
        crate::engine::lock(&self.inner.txn_holds).contains_key(&session)
    }

    /// The session's transaction ended (the session's commit hook):
    /// `Some(true)` a `COMMIT` that succeeded, `Some(false)` a `ROLLBACK`,
    /// `None` when the session can't tell (an implicit commit, a failed
    /// `COMMIT`): the owner then reads the row now. Either way the owner
    /// learns at once instead of after `hold_check_ms`.
    pub fn txn_ended(&self, session: u64, committed: Option<bool>) {
        let Some(holds) = crate::engine::lock(&self.inner.txn_holds).remove(&session) else {
            return;
        };
        for (_, t) in holds {
            let Some(v) = self.vhost_of(t.tenant, t.db) else {
                continue;
            };
            if let Some(h) = v.slot_of(t.id.queue) {
                let _ = h.send(match committed {
                    Some(c) => Cmd::Release {
                        id: t.id,
                        committed: c,
                    },
                    None => Cmd::Verify { id: t.id },
                });
            }
        }
    }

    /// Checks a delivery token.
    ///
    /// # Errors
    /// 39259 for a malformed or forged token.
    pub fn verify_token(&self, t: &str) -> Result<TokenData, MqError> {
        let bad = || {
            MqError::new(
                MqCode::DeliveryTokenInvalid,
                "malformed or forged delivery token",
            )
        };
        let tenant = token::tenant_of(t).ok_or_else(bad)?;
        let key = self.inner.deps.catalog.token_key(tenant).ok_or_else(bad)?;
        key.verify(t).ok_or_else(bad)
    }
}

impl SqlVhost {
    /// The vhost.
    #[must_use]
    pub fn vhost(&self) -> &VhostRef {
        &self.v.r
    }

    /// Makes sure the vhost's shard tables exist (before the session
    /// writes them).
    ///
    /// # Errors
    /// The store's.
    pub async fn ensure_store(&self) -> Result<(), MqError> {
        let e = self
            .v
            .engine
            .upgrade()
            .ok_or_else(|| MqError::new(MqCode::OwnerMoved, "engine stopped"))?;
        e.deps.catalog.store(&self.v.r).await.map(|_| ())
    }

    /// Publishes now (outside a transaction): the number of queues
    /// reached, after every one committed.
    ///
    /// # Errors
    /// 39250 (no exchange), 39253, 39255 (mandatory, no queue), 39256
    /// (full), 39261.
    pub async fn publish_now(&self, m: Message, mandatory: bool) -> Result<u32, MqError> {
        let (exchange, key) = (m.exchange.clone(), m.routing_key.clone());
        let p = self.cv.publish(&self.cx, m, mandatory)?;
        if p.routed == 0 && mandatory {
            return Err(unroutable(&exchange, &key));
        }
        p.confirm.await?;
        Ok(p.routed)
    }

    /// Routes a publish inside a transaction: what the session writes.
    ///
    /// # Errors
    /// 39250, 39253, 39255, 39264.
    pub async fn publish_in_txn(&self, m: Message, mandatory: bool) -> Result<TxnWrite, MqError> {
        let e = self
            .v
            .engine
            .upgrade()
            .ok_or_else(|| MqError::new(MqCode::OwnerMoved, "engine stopped"))?;
        if !e.deps.catalog.allowed(
            &self.cx.principal,
            &self.v.r,
            Perm::Write,
            "exchange",
            &m.exchange,
        ) {
            return Err(MqError::access_refused(format!(
                "access to exchange '{}' in vhost '{}' refused for user '{}' (write)",
                m.exchange, self.v.r.name, self.cx.principal.user
            )));
        }
        let topo = e.deps.catalog.refresh(&self.v.r).await?;
        let Some(x) = topo.exchanges.get(&m.exchange).cloned() else {
            return Err(MqError::not_found(format!(
                "no exchange '{}' in vhost '{}'",
                m.exchange, self.v.r.name
            )));
        };
        if x.internal {
            return Err(MqError::access_refused(format!(
                "cannot publish to internal exchange '{}'",
                m.exchange
            )));
        }
        let fmt = msg::fmt_code(m.format);
        if x.kind == ExKind::Delayed
            && let Some(d) = msg::delay_ms(&m)
        {
            let s = e.deps.catalog.settings(&self.v.r);
            if u64::try_from(d).unwrap_or(u64::MAX) > s.delay_max_ms {
                return Err(MqError::new(
                    MqCode::DelayOutOfRange,
                    format!(
                        "x-delay {d} is above queue.delay_max_ms ({})",
                        s.delay_max_ms
                    ),
                ));
            }
            return Ok(TxnWrite::Delayed(DelayedRow {
                exchange_id: x.id,
                exchange: x.name.clone(),
                due_ms: e.deps.clock.now_ms().saturating_add(d),
                msg_id: new_msg_id(),
                routing_key: m.routing_key.clone(),
                fmt,
                props: m.props.clone(),
                body: m.body.clone(),
            }));
        }
        let targets = self
            .v
            .with_view(|v| v.route(&m.exchange, &m.routing_key, &m.info.headers))
            .unwrap_or_default();
        if targets.is_empty() {
            if mandatory {
                return Err(unroutable(&m.exchange, &m.routing_key));
            }
            return Ok(TxnWrite::Nothing);
        }
        let n = u32::try_from(targets.len()).unwrap_or(u32::MAX);
        metrics::counter!("sprig_queue_published_total", "path" => "outbox").increment(1);
        Ok(TxnWrite::Outbox(
            OutboxRow {
                out_id: new_msg_id(),
                kind: OutKind::Publish,
                dest: OutDest::Queues(targets.iter().map(|q| q.id).collect()),
                msg_id: new_msg_id(),
                fmt,
                exchange: m.exchange.clone(),
                routing_key: m.routing_key.clone(),
                props: m.props.clone(),
                body: m.body.clone(),
                deaths: None,
            },
            n,
        ))
    }

    /// The transaction that wrote outbox rows committed: forward them now.
    pub fn committed(&self) {
        self.v.wake.notify_one();
    }

    /// The feed-driven forwarder's step (R9, r9fwd): forwards the outbox
    /// rows of `ids` still there, in the order of `ids` (commit order),
    /// waiting for every destination's confirm, then deletes them. Rows
    /// already gone (the polling forwarder's fallback, an earlier attempt)
    /// are skipped; a row forwarded twice is a no-op (the forward ledger).
    /// Returns (forwarded, already gone).
    ///
    /// # Errors
    /// The store's (39260 `OWNER_MOVED` for a store or owner that can't be
    /// reached); nothing is deleted then, and the caller tries again.
    pub async fn forward_outbox(
        &self,
        ids: &[crate::store::MsgId],
    ) -> Result<(usize, usize), MqError> {
        let e = self
            .v
            .engine
            .upgrade()
            .ok_or_else(|| MqError::new(MqCode::OwnerMoved, "engine stopped"))?;
        let store = e.deps.catalog.store(&self.v.r).await?;
        let rows = store
            .outbox_rows(ids)
            .await
            .map_err(|err| MqError::new(MqCode::OwnerMoved, err.to_string()))?;
        let gone = ids.len() - rows.len();
        if rows.is_empty() {
            return Ok((0, gone));
        }
        // Rows routed now (an exchange destination) need the latest
        // bindings; the rest were routed at publish.
        if rows
            .iter()
            .any(|r| matches!(r.dest, crate::store::OutDest::Exchange { .. }))
        {
            e.deps.catalog.refresh(&self.v.r).await?;
        }
        drop(e);
        self.v.forward(&rows).await?;
        let done: Vec<_> = rows.iter().map(|r| r.out_id).collect();
        store
            .outbox_done(&done)
            .await
            .map_err(|err| MqError::new(MqCode::OwnerMoved, err.to_string()))?;
        metrics::counter!("sprig_queue_published_total", "path" => "outbox_feed")
            .increment(done.len() as u64);
        Ok((done.len(), gone))
    }

    /// `mq_get`: up to `max` messages, each held for `lease_ms` unless
    /// acknowledged.
    ///
    /// # Errors
    /// 39250, 39253, 39252.
    pub async fn get(
        &self,
        queue: &str,
        max: u32,
        lease_ms: u64,
    ) -> Result<Vec<Delivery>, MqError> {
        self.get_tagged(queue, max, lease_ms, None).await
    }

    /// [`Self::get`] with an optional lease tag (R9, r9tests): the
    /// messages are held under the tag's key ([`lease_tag_conn`]), not the
    /// session's, so [`Self::release_tag`] can return them later from any
    /// session on any gateway.
    ///
    /// # Errors
    /// 39250, 39253, 39252.
    pub async fn get_tagged(
        &self,
        queue: &str,
        max: u32,
        lease_ms: u64,
        tag: Option<&str>,
    ) -> Result<Vec<Delivery>, MqError> {
        let conn = tag
            .filter(|t| !t.is_empty())
            .map_or(self.cx.conn, lease_tag_conn);
        // Through the vhost first, for the checks; then the owner with the
        // lease.
        let q = self.v.queue(queue).ok_or_else(|| {
            MqError::not_found(format!("no queue '{queue}' in vhost '{}'", self.v.r.name))
        })?;
        let e = self
            .v
            .engine
            .upgrade()
            .ok_or_else(|| MqError::new(MqCode::OwnerMoved, "engine stopped"))?;
        if !e
            .deps
            .catalog
            .allowed(&self.cx.principal, &self.v.r, Perm::Read, "queue", queue)
        {
            return Err(MqError::access_refused(format!(
                "access to queue '{queue}' in vhost '{}' refused for user '{}' (read)",
                self.v.r.name, self.cx.principal.user
            )));
        }
        let h = self.v.slot(&q);
        let mut out = Vec::new();
        for _ in 0..max.max(1) {
            let (tx, rx) = oneshot::channel();
            if !h.send(Cmd::Get {
                holder: Holder { origin: 0, conn },
                no_ack: false,
                lease_ms: Some(lease_ms.max(1)),
                reply: tx,
            }) {
                break;
            }
            match rx
                .await
                .map_err(|_| MqError::new(MqCode::OwnerMoved, "owner stopped"))??
            {
                Some((d, _)) => out.push(d),
                None => break,
            }
        }
        Ok(out)
    }

    /// The queue, checked for `perm` (refreshing the topology once when
    /// it is missing).
    async fn checked_queue(
        &self,
        queue: &str,
        perm: Perm,
    ) -> Result<(Arc<crate::engine::Inner>, Arc<crate::topology::QueueDef>), MqError> {
        let e = self
            .v
            .engine
            .upgrade()
            .ok_or_else(|| MqError::new(MqCode::OwnerMoved, "engine stopped"))?;
        let q = if let Some(q) = self.v.queue(queue) {
            q
        } else {
            e.deps.catalog.refresh(&self.v.r).await?;
            self.v.queue(queue).ok_or_else(|| {
                MqError::not_found(format!("no queue '{queue}' in vhost '{}'", self.v.r.name))
            })?
        };
        if !e
            .deps
            .catalog
            .allowed(&self.cx.principal, &self.v.r, perm, "queue", queue)
        {
            return Err(MqError::access_refused(format!(
                "access to queue '{queue}' in vhost '{}' refused for user '{}'",
                self.v.r.name, self.cx.principal.user
            )));
        }
        Ok((e, q))
    }

    /// `mq_info` (R9, ST50): ready messages, consumers, and messages
    /// delivered but not yet acknowledged (stored minus ready, read a
    /// moment apart).
    ///
    /// bugfix-66 (R9 item 123): when the queue's owner can't answer (its
    /// slot failed: an owner lease it couldn't take or reach, an owner that
    /// keeps moving, a relay that broke), the owner is asked once more,
    /// then the answer comes from the store: every stored message ready,
    /// no consumers (an owner nobody can reach serves none here), with a
    /// warning. Before, such a failure read as "0 ready, 0 consumers", so
    /// every stored message showed as unacknowledged.
    ///
    /// # Errors
    /// 39250 (no queue), 39252 (refused), the store's.
    pub async fn info(&self, queue: &str) -> Result<QueueInfo, MqError> {
        let (e, q) = self.checked_queue(queue, Perm::Read).await?;
        let mut answer = self.owner_info(&q).await;
        if answer.is_none() {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            answer = self.owner_info(&q).await;
        }
        let store = e.deps.catalog.store(&self.v.r).await?;
        let stored = store
            .stored(q.id)
            .await
            .map_err(|err| MqError::new(MqCode::OwnerMoved, err.to_string()))?;
        Ok(match answer {
            Some((ready, consumers)) => QueueInfo {
                ready: u64::from(ready),
                consumers: u64::from(consumers),
                unacked: stored.saturating_sub(u64::from(ready)),
            },
            None => {
                tracing::warn!(queue = %q.name, vhost = %self.v.r.name, stored,
                    "mq_info: the queue's owner didn't answer; counting every stored message as ready");
                metrics::counter!("sprig_queue_info_unanswered_total").increment(1);
                QueueInfo {
                    ready: stored,
                    consumers: 0,
                    unacked: 0,
                }
            }
        })
    }

    /// The owner's ready count and consumers, or `None` when it couldn't
    /// answer within 10 s.
    async fn owner_info(&self, q: &Arc<crate::topology::QueueDef>) -> Option<(u32, u32)> {
        let (tx, rx) = oneshot::channel();
        if !self.v.slot(q).send(Cmd::Info(tx)) {
            return None;
        }
        tokio::time::timeout(std::time::Duration::from_secs(10), rx)
            .await
            .ok()
            .and_then(Result::ok)
    }

    /// `mq_info` for every durable queue of the vhost the user may read
    /// (`CALL sprig.mq_info(NULL [, vhost])`; ST66), in name order: one call
    /// answers a page of queues. Queues the user may not read are left out,
    /// and so is a queue dropped while the call runs.
    ///
    /// # Errors
    /// 39261 (engine stopped), the catalog's, the store's.
    pub async fn info_all(&self) -> Result<Vec<(String, QueueInfo)>, MqError> {
        use futures::StreamExt as _;
        let e = self
            .v
            .engine
            .upgrade()
            .ok_or_else(|| MqError::new(MqCode::OwnerMoved, "engine stopped"))?;
        let topo = e.deps.catalog.refresh(&self.v.r).await?;
        let mut names: Vec<String> = topo
            .queues
            .keys()
            .filter(|n| {
                e.deps
                    .catalog
                    .allowed(&self.cx.principal, &self.v.r, Perm::Read, "queue", n)
            })
            .cloned()
            .collect();
        drop(e);
        names.sort();
        let answers: Vec<_> = futures::stream::iter(names)
            .map(|n| async move {
                let i = self.info(&n).await;
                (n, i)
            })
            .buffered(8)
            .collect()
            .await;
        let mut out = Vec::with_capacity(answers.len());
        for (n, i) in answers {
            match i {
                Ok(i) => out.push((n, i)),
                Err(err) if matches!(err.code, MqCode::NotFound | MqCode::AccessRefused) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(out)
    }

    /// Whether the session's user may consume the queue `t` names (the
    /// right `mq_get` checks: read, which for a queue is `SELECT` and
    /// `DELETE` on it or its database). R9 (r9tests): an acknowledgement
    /// by a user granted only the queue is settled by its owner.
    #[must_use]
    pub fn may_consume(&self, t: &TokenData) -> bool {
        let (Some(q), Some(e)) = (self.v.queue_by_id(t.id.queue), self.v.engine.upgrade()) else {
            return false;
        };
        e.deps
            .catalog
            .allowed(&self.cx.principal, &self.v.r, Perm::Read, "queue", &q.name)
    }

    /// `mq_release` (R9, r9tests): every message of `queue` held under
    /// lease tag `tag` goes back to the queue now, its delivery count
    /// raised as when its lease ends. Checked as a read of the queue (the
    /// right `mq_get` needs). The owner applies it after the commands
    /// before it; the caller sees the result through `mq_info`.
    ///
    /// # Errors
    /// 39250, 39252, 39261 (owner stopped).
    pub async fn release_tag(&self, queue: &str, tag: &str) -> Result<(), MqError> {
        let (_, q) = self.checked_queue(queue, Perm::Read).await?;
        if tag.is_empty() {
            return Ok(());
        }
        if !self.v.slot(&q).send(Cmd::ConnLost(Holder {
            origin: 0,
            conn: lease_tag_conn(tag),
        })) {
            return Err(MqError::new(MqCode::OwnerMoved, "owner stopped"));
        }
        metrics::counter!("sprig_queue_lease_tag_releases_total").increment(1);
        Ok(())
    }

    /// `mq_purge` (R9, ST50): removes the ready messages; delivered ones
    /// stay until settled (RabbitMQ's `queue.purge`).
    ///
    /// # Errors
    /// 39250, 39252, 39261 (owner moved).
    pub async fn purge(&self, queue: &str) -> Result<u32, MqError> {
        self.checked_queue(queue, Perm::Read).await?;
        self.cv.purge_queue(&self.cx, queue).await
    }

    /// `mq_browse` (R9, ST50): up to `max` stored messages of `queue`, in
    /// delivery order, without delivering or locking them (those already
    /// delivered and not yet acknowledged are included; a browse is a
    /// read of the store, not of the owner's memory).
    ///
    /// # Errors
    /// 39250, 39252, the store's.
    pub async fn browse(&self, queue: &str, max: usize) -> Result<Vec<Delivery>, MqError> {
        let (e, q) = self.checked_queue(queue, Perm::Read).await?;
        let store = e.deps.catalog.store(&self.v.r).await?;
        let rows = store
            .load(q.id, 0, None, max.clamp(1, 1_000))
            .await
            .map_err(|err| MqError::new(MqCode::OwnerMoved, err.to_string()))?;
        Ok(rows
            .into_iter()
            .filter_map(|m| {
                let id = sprig_proto_amqp::message::DeliveryId {
                    queue: q.id,
                    lane: m.lane,
                    pclass: m.pclass,
                    seq: m.seq,
                };
                let msg = msg::rebuild(m.fmt, m.exchange, m.routing_key, m.props, m.body).ok()?;
                Some(Delivery {
                    id,
                    queue: q.name.clone(),
                    message: Arc::new(msg),
                    redelivered: m.delivery_count > 0,
                    delivery_count: m.delivery_count,
                    annotations: sprig_proto_amqp::v091::codec::FieldTable::default(),
                })
            })
            .collect())
    }

    /// Starts an acknowledgement inside a transaction: the owner holds the
    /// delivery.
    ///
    /// # Errors
    /// 39258 when it is no longer delivered.
    pub async fn hold(&self, t: &TokenData) -> Result<(), MqError> {
        let Some(q) = self.v.queue_by_id(t.id.queue) else {
            return Err(MqError::new(
                MqCode::AlreadySettled,
                "the queue no longer exists",
            ));
        };
        let (tx, rx) = oneshot::channel();
        if !self.v.slot(&q).send(Cmd::Hold {
            id: t.id,
            msg_id: t.msg_id,
            reply: tx,
        }) {
            return Err(MqError::new(MqCode::OwnerMoved, "owner stopped"));
        }
        rx.await
            .map_err(|_| MqError::new(MqCode::OwnerMoved, "owner stopped"))?
    }

    /// The transaction holding `t` ended.
    pub fn release(&self, t: &TokenData, committed: bool) {
        if let Some(h) = self.v.slot_of(t.id.queue) {
            let _ = h.send(Cmd::Release {
                id: t.id,
                committed,
            });
        }
    }

    /// Acknowledges a delivery with its owner, outside any transaction
    /// (the owner deletes the message): a fan-in queue sink's settle of a
    /// member's message (chapter 51, R9, r9queue).
    pub fn ack(&self, t: &TokenData) {
        if let Some(h) = self.v.slot_of(t.id.queue) {
            let _ = h.send(Cmd::Settle(
                Holder {
                    origin: ANY_HOLDER,
                    conn: self.cx.conn,
                },
                vec![(t.id, Settlement::Ack)],
            ));
        }
    }

    /// `mq_nack(token, requeue)`.
    pub fn nack(&self, t: &TokenData, requeue: bool) {
        if let Some(h) = self.v.slot_of(t.id.queue) {
            let how = if requeue {
                Settlement::Requeue { failed: false }
            } else {
                Settlement::DeadLetter(sprig_proto_amqp::backend::DeadLetterReason::Rejected)
            };
            let _ = h.send(Cmd::Settle(
                Holder {
                    origin: ANY_HOLDER,
                    conn: self.cx.conn,
                },
                vec![(t.id, how)],
            ));
        }
    }

    /// Returns a delivery as a failed one (`Requeue { failed: true }`): it
    /// goes back to its position and its `x-delivery-count` rises, so the
    /// queue's delivery limit and a trigger's `logic.trigger_max_attempts`
    /// (chapter 41, R9) see the failure. `mq_nack` with requeue doesn't
    /// count, as RabbitMQ's `nack` doesn't.
    pub fn fail(&self, t: &TokenData) {
        if let Some(h) = self.v.slot_of(t.id.queue) {
            let _ = h.send(Cmd::Settle(
                Holder {
                    origin: ANY_HOLDER,
                    conn: self.cx.conn,
                },
                vec![(t.id, Settlement::Requeue { failed: true })],
            ));
        }
    }

    /// The vhost's database name (for the session's statements).
    #[must_use]
    pub fn db(&self) -> &str {
        &self.v.r.db
    }

    /// The vhost (for `Vhost` calls).
    #[must_use]
    pub fn conn_vhost(&self) -> &dyn Vhost {
        &self.cv
    }
}

/// `X'...'` of bytes.
#[must_use]
pub fn hex_literal(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(3 + b.len() * 2);
    s.push_str("X'");
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s.push('\'');
    s
}

/// The insert of an outbox row into `db`'s `__sprig_qo`, as a statement
/// of the client's session (logical names, internal mode).
#[must_use]
pub fn outbox_sql(db: &str, r: &OutboxRow) -> String {
    let dest = serde_json::to_string(&r.dest).unwrap_or_default();
    format!(
        "INSERT INTO `{}`.`__sprig_qo` (out_id, kind, dest, msg_id, fmt, exchange, routing_key, props, body, deaths) \
         VALUES ({}, '{}', {}, {}, {}, {}, {}, {}, {}, NULL)",
        db.replace('`', "``"),
        hex_literal(&r.out_id),
        r.kind.as_str(),
        hex_literal(dest.as_bytes()),
        hex_literal(&r.msg_id),
        r.fmt,
        hex_literal(r.exchange.as_bytes()),
        hex_literal(r.routing_key.as_bytes()),
        hex_literal(&r.props),
        hex_literal(&r.body),
    )
}

/// The insert of a delayed message into `db`'s `__sprig_qd`.
#[must_use]
pub fn delayed_sql(db: &str, r: &DelayedRow) -> String {
    format!(
        "INSERT INTO `{}`.`__sprig_qd` (exchange_id, exchange, due_ms, msg_id, routing_key, fmt, props, body) \
         VALUES ({}, {}, {}, {}, {}, {}, {}, {})",
        db.replace('`', "``"),
        r.exchange_id,
        hex_literal(r.exchange.as_bytes()),
        r.due_ms,
        hex_literal(&r.msg_id),
        hex_literal(r.routing_key.as_bytes()),
        r.fmt,
        hex_literal(&r.props),
        hex_literal(&r.body),
    )
}

/// The delete an acknowledgement makes in the client's transaction.
#[must_use]
pub fn ack_sql(db: &str, t: &TokenData) -> String {
    format!(
        "DELETE FROM `{}`.`__sprig_qm` WHERE queue_id = {} AND lane = {} AND pclass = {} AND seq = {} AND msg_id = {}",
        db.replace('`', "``"),
        t.id.queue,
        t.id.lane,
        t.id.pclass,
        t.id.seq,
        hex_literal(&t.msg_id)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_calls() {
        let c = parse_call(
            "CALL sprig.mq_publish('orders', 'order.paid.eu', JSON_OBJECT('id', 7, 'x', 'a,b'), JSON_OBJECT('message_id','m1'));",
        )
        .unwrap_or_else(|| unreachable!());
        assert_eq!(c.call, MqCall::Publish);
        assert_eq!(c.args.len(), 4);
        assert_eq!(c.args[2], "JSON_OBJECT('id', 7, 'x', 'a,b')");
        let c = parse_call("call `sprig`.mq_ack('q1.AA')").unwrap_or_else(|| unreachable!());
        assert_eq!((c.call, c.args), (MqCall::Ack, vec!["'q1.AA'".to_owned()]));
        assert!(parse_call("CALL sprig.other(1)").is_none());
        assert!(parse_call("CALL sprigx.mq_ack(1)").is_none());
        assert!(parse_call("SELECT 1").is_none());
        let c = parse_call("CALL sprig.mq_get('q', 10, 30000)").unwrap_or_else(|| unreachable!());
        assert_eq!(c.args, ["'q'", "10", "30000"]);
    }

    #[test]
    fn parses_studio_calls() {
        // R9 (r9cluster, ST50): queue pages' calls.
        let p = |s: &str| parse_call(s).map(|c| (c.call, c.args.len()));
        assert_eq!(p("CALL sprig.mq_info('q')"), Some((MqCall::Info, 1)));
        // ST66: every queue of a vhost in one call.
        assert_eq!(p("CALL sprig.mq_info()"), Some((MqCall::Info, 0)));
        assert_eq!(
            p("CALL sprig.mq_info(NULL, 'shop')"),
            Some((MqCall::Info, 2))
        );
        assert_eq!(
            p("CALL sprig.mq_browse('q', 50, 'mq')"),
            Some((MqCall::Browse, 3))
        );
        assert_eq!(p("call sprig.mq_purge('q')"), Some((MqCall::Purge, 1)));
        assert_eq!(
            p("CALL sprig.mq_republish('dlq', 'orders', NULL, 10)"),
            Some((MqCall::Republish, 4))
        );
    }

    #[test]
    fn sql_messages() {
        let m = message(
            "x",
            "k",
            b"{\"a\":1}".to_vec(),
            Some(r#"{"content_type":"application/json","message_id":"o1","priority":3,"headers":{"source":"checkout"}}"#),
        )
        .unwrap_or_else(|_| unreachable!());
        assert_eq!(m.info.message_id.as_deref(), Some("o1"));
        assert_eq!(m.info.priority, Some(3));
        assert!(m.info.durable);
        assert!(m.info.headers.get("source").is_some());
        assert!(message("x", "k", vec![], Some("[1]")).is_err());
        assert_eq!(hex_literal(&[0, 255]), "X'00ff'");
    }
}
