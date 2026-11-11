//! Queues in a tenant session (chapter 50, "SQL, logic, MongoDB and
//! HTTP"; build steps 9 and 10): `CALL sprig.mq_publish(...)`, `mq_get`,
//! `mq_ack` and `mq_nack`, run by [`sprig_queues::sql`] over the
//! gateway's queue engine.
//!
//! The arguments are evaluated by the session itself (`SELECT <args>`), so
//! any expression works (`JSON_OBJECT(...)`, columns of a subquery) under
//! the user's own rights.
//!
//! - **`mq_publish` in a transaction** inserts one `__sprig_qo` row (or a
//!   `__sprig_qd` row for a delayed exchange) through this session, in its
//!   transaction (internal mode, logical names): the message exists only
//!   if the transaction commits. Outside a transaction it publishes at
//!   once and answers after the queues committed.
//! - **`mq_ack`** asks the owner to hold the delivery, then deletes the
//!   message row through this session. Zero rows: 39258, and the
//!   transaction is rolled back. Outside a transaction the owner is told
//!   at once; inside one, the session's commit hook tells it when the
//!   transaction ends ([`PassSession::mq_after_request`]), and the owner
//!   checks the row under a lock after `hold_check_ms` (5 s) if nobody
//!   does (a session that disconnected).
//! - **The vhost** is an optional last argument of `mq_publish` and
//!   `mq_get` (NULL or absent: `/`); a delivery token names its own.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_amqp::error::{MqCode, MqError};
use sprig_proto_mysql::command::QueryAttrs;
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::{ColumnDef, StatusFlags};
use sprig_queues::sql::{self as mq, MqCall, TxnWrite};

use super::PassSession;
use super::tenant::Step;
use crate::pipeline::{EndOfRows, ExecCtx, ResultSink, RowFormat, RowValues, Wire};

/// A chapter 50 error as a SQL one: the number, the name, the text.
pub(super) fn sql_error(e: &MqError) -> SprigError {
    let state = if e.code == MqCode::OwnerMoved {
        *b"40001"
    } else {
        *b"HY000"
    };
    SprigError::standard(
        u16::try_from(e.code.number()).unwrap_or(39250),
        state,
        format!("{}: {}", e.code.name(), e.text),
    )
}

fn arg_error(what: &str) -> SprigError {
    SprigError::standard(1318, *b"42000", format!("Incorrect arguments to {what}"))
}

/// Keeps one internal statement's result.
struct Collect {
    wire: Wire,
    rows: Vec<Vec<Option<Vec<u8>>>>,
    cols: usize,
    affected: Option<u64>,
    error: Option<SprigError>,
    done: bool,
}

#[async_trait]
impl ResultSink for Collect {
    fn wire(&self) -> Wire {
        self.wire
    }
    async fn begin_rows(&mut self, columns: &[ColumnDef], _format: RowFormat) -> Result<()> {
        self.cols = columns.len();
        Ok(())
    }
    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        match values {
            RowValues::Text(v) => self
                .rows
                .push(v.iter().map(|c| c.map(<[u8]>::to_vec)).collect()),
            RowValues::Binary(_) => return Err(SprigError::internal("a binary row in an mq call")),
        }
        Ok(())
    }
    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        let r = sprig_proto_mysql::resultset::decode_text_row(&payload, self.cols)?
            .into_iter()
            .map(|c| c.map(<[u8]>::to_vec))
            .collect();
        self.rows.push(r);
        Ok(())
    }
    async fn end_rows(&mut self, _end: EndOfRows) -> Result<()> {
        self.done = true;
        Ok(())
    }
    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        self.affected = Some(ok.affected_rows);
        self.done = true;
        Ok(())
    }
    async fn error(&mut self, err: &SprigError) -> Result<()> {
        self.error = Some(SprigError {
            kind: err.kind,
            message: err.message.clone(),
            retryable: err.retryable,
            source: None,
        });
        self.done = true;
        Ok(())
    }
    async fn column_list(&mut self, _columns: &[ColumnDef], _end: EndOfRows) -> Result<()> {
        Ok(())
    }
    async fn local_infile_request(&mut self, _file_name: &[u8]) -> Result<()> {
        Err(SprigError::new(
            SprigCode::TypeUnsupported,
            "LOAD DATA LOCAL INFILE can't run inside sprig.mq_*",
        ))
    }
    async fn local_infile_read(&mut self) -> Result<Bytes> {
        Ok(Bytes::new())
    }
    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    fn finished(&self) -> bool {
        self.done
    }
}

enum Reply {
    Rows(Vec<&'static str>, Vec<Vec<Option<Vec<u8>>>>),
    Ok(u64),
}

fn contains_mq(text: &[u8]) -> bool {
    text.windows(3).any(|w| {
        w[0].eq_ignore_ascii_case(&b'm') && w[1].eq_ignore_ascii_case(&b'q') && w[2] == b'_'
    })
}

impl PassSession {
    /// Runs `sql` through this session (logical names, its transaction);
    /// `internal` allows the `__sprig_q*` tables.
    async fn mq_run(&mut self, cx: &mut ExecCtx<'_>, sql: &str, internal: bool) -> Result<Collect> {
        let mut sink = Collect {
            wire: Wire {
                persona: cx.proto.persona,
                caps: cx.proto.caps,
            },
            rows: Vec::new(),
            cols: 0,
            affected: None,
            error: None,
            done: false,
        };
        if internal && let Some(ts) = self.tenant.as_deref_mut() {
            ts.types.internal = true;
        }
        let text = Bytes::copy_from_slice(sql.as_bytes());
        let r = Box::pin(self.tenant_text(
            cx,
            &text,
            &QueryAttrs::default(),
            false,
            RowFormat::Text,
            &mut sink,
        ))
        .await;
        if internal && let Some(ts) = self.tenant.as_deref_mut() {
            ts.types.internal = false;
        }
        r?;
        if let Some(e) = sink.error.take() {
            return Err(e);
        }
        Ok(sink)
    }

    fn mq_in_transaction(&self) -> bool {
        let s = self.status();
        s.contains(StatusFlags::IN_TRANS) || !s.contains(StatusFlags::AUTOCOMMIT)
    }

    /// The session's commit hook for acknowledgements inside a
    /// transaction (chapter 50, "Acknowledging inside a database
    /// transaction", step 5): after every request, if this session holds
    /// deliveries for its transaction and the transaction has ended, the
    /// owners learn it now: not committed for a `ROLLBACK`; for anything
    /// else (`COMMIT`, an implicit commit) they read the row at once,
    /// which no longer waits on a lock. A `COMMIT` is not taken at its
    /// word because a failed one reaches the client as a relayed error
    /// packet, not as an error here. `word`: what [`txn_word`] made of
    /// the request.
    pub(super) fn mq_after_request(&self, session: u64, word: Option<bool>, ok: bool) {
        let Some(ts) = self.tenant.as_deref() else {
            return;
        };
        let Some(engine) = ts.services().mq.get() else {
            return;
        };
        if !engine.has_txn_holds(session) || self.mq_in_transaction() {
            return;
        }
        engine.txn_ended(session, if ok { word } else { None });
    }

    /// Chapter 50's step of [`PassSession::tenant_text`]: `None` when the
    /// statement isn't a `sprig.mq_*` call.
    pub(super) async fn tenant_queues(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let Some(ts) = self.tenant.as_deref() else {
            return Ok(None);
        };
        if ts.types.internal || !contains_mq(text) {
            return Ok(None);
        }
        let Ok(sql) = std::str::from_utf8(text) else {
            return Ok(None);
        };
        let Some(call) = mq::parse_call(sql) else {
            return Ok(None);
        };
        let r = self.mq_call(cx, call).await;
        let mut status = self.status();
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        match r {
            Err(e) => Ok(Some(self.fail(out, &e).await?)),
            Ok(Reply::Ok(n)) => {
                let mut ok = OkPacket::new(status);
                ok.affected_rows = n;
                out.ok(ok).await?;
                Ok(Some(Step {
                    errored: false,
                    status: Some(status),
                }))
            }
            Ok(Reply::Rows(names, rows)) => {
                let rows = sprig_types::Rows {
                    columns: names
                        .iter()
                        .map(|n| crate::pipeline::text_column(n, cx.proto.collation))
                        .collect(),
                    rows,
                };
                super::types::send_rows(out, &rows, format, status).await?;
                Ok(Some(Step {
                    errored: false,
                    status: Some(status),
                }))
            }
        }
    }

    #[allow(clippy::too_many_lines)] // one arm per call, each short
    #[allow(clippy::many_single_char_names)]
    async fn mq_call(&mut self, cx: &mut ExecCtx<'_>, call: mq::Call) -> Result<Reply> {
        let (engine, principal, default_db) = {
            let ts = self
                .tenant
                .as_deref()
                .ok_or_else(|| SprigError::internal("no tenant"))?;
            let engine = ts.services().mq.get().cloned().ok_or_else(|| {
                sql_error(&MqError::not_implemented(
                    "the queue engine isn't running on this gateway (catalog logins and the AMQP front end)",
                ))
            })?;
            let p = &ts.principal;
            let principal = Arc::new(sprig_proto_amqp::auth::Principal {
                tenant_id: ts.tenant.0,
                tenant: p.tenant_name.clone().unwrap_or_default(),
                user: p.user.clone(),
                user_id: p.user_id.map(|u| u64::from(u.0)),
                handle: Some(Arc::new(p.clone())),
            });
            (engine, principal, "/".to_owned())
        };
        let n = call.args.len();
        let (lo, hi, name) = match call.call {
            MqCall::Publish => (4, 6, "sprig.mq_publish"),
            MqCall::Get => (1, 5, "sprig.mq_get"),
            MqCall::Release => (2, 3, "sprig.mq_release"),
            MqCall::Ack => (1, 1, "sprig.mq_ack"),
            MqCall::Nack => (1, 2, "sprig.mq_nack"),
            MqCall::Info => (1, 2, "sprig.mq_info"),
            MqCall::Browse => (1, 3, "sprig.mq_browse"),
            MqCall::Purge => (1, 2, "sprig.mq_purge"),
            MqCall::Republish => (2, 5, "sprig.mq_republish"),
        };
        if n < lo || n > hi {
            return Err(arg_error(name));
        }
        // The arguments, evaluated by the session under the user's rights.
        let vals = {
            let c = self
                .mq_run(cx, &format!("SELECT {}", call.args.join(", ")), false)
                .await?;
            c.rows.into_iter().next().ok_or_else(|| arg_error(name))?
        };
        let text = |i: usize| -> Option<String> {
            vals.get(i)
                .cloned()
                .flatten()
                .map(|b| String::from_utf8_lossy(&b).into_owned())
        };
        let num = |i: usize, d: u64| {
            text(i)
                .and_then(|s| s.trim().parse::<u64>().ok())
                .unwrap_or(d)
        };
        let session = u64::from(cx.session.id.0);
        // The optional vhost argument (last of mq_publish and mq_get);
        // NULL or absent is `/`.
        let vhost = |i: usize| {
            text(i)
                .filter(|v| !v.is_empty())
                .unwrap_or(default_db.clone())
        };
        match call.call {
            MqCall::Publish => {
                let exchange = text(0).unwrap_or_default();
                let key = text(1).unwrap_or_default();
                let body = vals.get(2).cloned().flatten().unwrap_or_default();
                let mandatory = num(4, 0) != 0;
                let mut m = mq::message(&exchange, &key, body, text(3).as_deref())
                    .map_err(|e| sql_error(&e))?;
                // Chapter 42: an inherited exchange or queue (a targeted
                // publish into a member's instance, or the headers stamped
                // on one into this tenant's own).
                if let Some(routed) = self
                    .inherit_mq_publish(cx, &mut m, mandatory, &vhost(5))
                    .await?
                {
                    return Ok(Reply::Rows(
                        vec!["routed"],
                        vec![vec![Some(routed.to_string().into_bytes())]],
                    ));
                }
                let v = engine
                    .sql_vhost(principal, &vhost(5), session)
                    .await
                    .map_err(|e| sql_error(&e))?;
                if !self.mq_in_transaction() {
                    let routed = v
                        .publish_now(m, mandatory)
                        .await
                        .map_err(|e| sql_error(&e))?;
                    return Ok(Reply::Rows(
                        vec!["routed"],
                        vec![vec![Some(routed.to_string().into_bytes())]],
                    ));
                }
                v.ensure_store().await.map_err(|e| sql_error(&e))?;
                let db = v.db().to_owned();
                let routed = match v
                    .publish_in_txn(m, mandatory)
                    .await
                    .map_err(|e| sql_error(&e))?
                {
                    TxnWrite::Outbox(row, n) => {
                        self.mq_run(cx, &mq::outbox_sql(&db, &row), true).await?;
                        n
                    }
                    TxnWrite::Delayed(row) => {
                        self.mq_run(cx, &mq::delayed_sql(&db, &row), true).await?;
                        1
                    }
                    TxnWrite::Nothing => 0,
                };
                Ok(Reply::Rows(
                    vec!["routed"],
                    vec![vec![Some(routed.to_string().into_bytes())]],
                ))
            }
            MqCall::Get => {
                let queue = text(0).unwrap_or_default();
                let max = u32::try_from(num(1, 1).clamp(1, 1000)).unwrap_or(1);
                let lease = num(2, 30_000);
                let v = engine
                    .sql_vhost(principal, &vhost(3), session)
                    .await
                    .map_err(|e| sql_error(&e))?;
                // R9 (r9tests): an optional lease tag, so `mq_release` can
                // give the messages back (a queue sink's dead task's).
                let tag = text(4);
                let ds = v
                    .get_tagged(&queue, max, lease, tag.as_deref())
                    .await
                    .map_err(|e| sql_error(&e))?;
                let rows = ds
                    .iter()
                    .map(|d| {
                        let token = d
                            .annotations
                            .get("x-sprig-delivery")
                            .and_then(|t| t.as_str().map(std::borrow::Cow::into_owned));
                        vec![
                            token.map(String::into_bytes),
                            Some(mq::body_bytes(d).to_vec()),
                            Some(mq::properties_json(d).into_bytes()),
                            Some(d.message.routing_key.clone().into_bytes()),
                            Some(d.message.exchange.clone().into_bytes()),
                            Some(if d.redelivered {
                                b"1".to_vec()
                            } else {
                                b"0".to_vec()
                            }),
                            Some(d.delivery_count.to_string().into_bytes()),
                        ]
                    })
                    .collect();
                Ok(Reply::Rows(
                    vec![
                        "delivery_token",
                        "body",
                        "properties",
                        "routing_key",
                        "exchange",
                        "redelivered",
                        "delivery_count",
                    ],
                    rows,
                ))
            }
            // R9 (r9tests): a lease tag's messages back to the queue now.
            MqCall::Release => {
                let v = engine
                    .sql_vhost(principal, &vhost(2), session)
                    .await
                    .map_err(|e| sql_error(&e))?;
                v.release_tag(&text(0).unwrap_or_default(), &text(1).unwrap_or_default())
                    .await
                    .map_err(|e| sql_error(&e))?;
                Ok(Reply::Ok(0))
            }
            // R9 (r9cluster), for Studio's queue pages (ST50).
            MqCall::Info => {
                let v = engine
                    .sql_vhost(principal, &vhost(1), session)
                    .await
                    .map_err(|e| sql_error(&e))?;
                let i = v
                    .info(&text(0).unwrap_or_default())
                    .await
                    .map_err(|e| sql_error(&e))?;
                let n = |x: u64| Some(x.to_string().into_bytes());
                Ok(Reply::Rows(
                    vec!["ready", "unacked", "consumers"],
                    vec![vec![n(i.ready), n(i.unacked), n(i.consumers)]],
                ))
            }
            MqCall::Purge => {
                let v = engine
                    .sql_vhost(principal, &vhost(1), session)
                    .await
                    .map_err(|e| sql_error(&e))?;
                let purged = v
                    .purge(&text(0).unwrap_or_default())
                    .await
                    .map_err(|e| sql_error(&e))?;
                Ok(Reply::Rows(
                    vec!["purged"],
                    vec![vec![Some(purged.to_string().into_bytes())]],
                ))
            }
            MqCall::Browse => {
                let v = engine
                    .sql_vhost(principal, &vhost(2), session)
                    .await
                    .map_err(|e| sql_error(&e))?;
                let max = usize::try_from(num(1, 100).clamp(1, 1000)).unwrap_or(100);
                let ds = v
                    .browse(&text(0).unwrap_or_default(), max)
                    .await
                    .map_err(|e| sql_error(&e))?;
                let rows = ds
                    .iter()
                    .map(|d| {
                        vec![
                            Some(d.id.seq.to_string().into_bytes()),
                            Some(mq::body_bytes(d).to_vec()),
                            Some(mq::properties_json(d).into_bytes()),
                            Some(d.message.routing_key.clone().into_bytes()),
                            Some(d.message.exchange.clone().into_bytes()),
                            Some(d.delivery_count.to_string().into_bytes()),
                        ]
                    })
                    .collect();
                Ok(Reply::Rows(
                    vec![
                        "seq",
                        "body",
                        "properties",
                        "routing_key",
                        "exchange",
                        "delivery_count",
                    ],
                    rows,
                ))
            }
            MqCall::Republish => {
                // Moves messages: take each with a lease, publish it to
                // the exchange (confirmed), then acknowledge it. A crash
                // between the two leaves a copy in both places (at least
                // once), as RabbitMQ's shovel "move messages" does.
                let queue = text(0).unwrap_or_default();
                let exchange = text(1).unwrap_or_default();
                let key = text(2);
                let max = u32::try_from(num(3, 100).clamp(1, 10_000)).unwrap_or(100);
                let v = engine
                    .sql_vhost(principal, &vhost(4), session)
                    .await
                    .map_err(|e| sql_error(&e))?;
                let mut moved = 0u64;
                while u64::from(max) > moved {
                    let batch = u32::try_from((u64::from(max) - moved).min(100)).unwrap_or(100);
                    let ds = v
                        .get(&queue, batch, 60_000)
                        .await
                        .map_err(|e| sql_error(&e))?;
                    if ds.is_empty() {
                        break;
                    }
                    for (i, d) in ds.iter().enumerate() {
                        let token = d
                            .annotations
                            .get("x-sprig-delivery")
                            .and_then(|t| t.as_str().map(std::borrow::Cow::into_owned))
                            .unwrap_or_default();
                        let t = engine.verify_token(&token).map_err(|e| sql_error(&e))?;
                        let rk = key.clone().unwrap_or_else(|| d.message.routing_key.clone());
                        let m = mq::message(
                            &exchange,
                            &rk,
                            mq::body_bytes(d).to_vec(),
                            Some(&mq::properties_json(d)),
                        )
                        .map_err(|e| sql_error(&e))?;
                        match v.publish_now(m, true).await {
                            Ok(_) => {
                                v.ack(&t);
                                moved += 1;
                            }
                            Err(e) => {
                                // This one and the rest of the batch go back.
                                for r in &ds[i..] {
                                    if let Some(tok) = r
                                        .annotations
                                        .get("x-sprig-delivery")
                                        .and_then(|t| t.as_str().map(std::borrow::Cow::into_owned))
                                        && let Ok(t) = engine.verify_token(&tok)
                                    {
                                        v.nack(&t, true);
                                    }
                                }
                                return Err(sql_error(&e));
                            }
                        }
                    }
                }
                Ok(Reply::Rows(
                    vec!["moved"],
                    vec![vec![Some(moved.to_string().into_bytes())]],
                ))
            }
            MqCall::Ack | MqCall::Nack => {
                let token = text(0).unwrap_or_default();
                let t = engine.verify_token(&token).map_err(|e| sql_error(&e))?;
                if t.tenant != principal.tenant_id {
                    return Err(sql_error(&MqError::new(
                        MqCode::DeliveryTokenInvalid,
                        "the delivery token is another tenant's",
                    )));
                }
                // The token names the vhost's database.
                let db = {
                    let ts = self
                        .tenant
                        .as_deref()
                        .ok_or_else(|| SprigError::internal("no tenant"))?;
                    ts.catalog()
                        .and_then(|c| {
                            c.databases
                                .get(sprig_common::ids::DbId(t.db))
                                .map(|d| d.name.clone())
                        })
                        .ok_or_else(|| {
                            sql_error(&MqError::not_found("the token's vhost no longer exists"))
                        })?
                };
                let v = engine
                    .sql_vhost(principal, &db, session)
                    .await
                    .map_err(|e| sql_error(&e))?;
                if call.call == MqCall::Nack {
                    v.nack(&t, num(1, 1) != 0);
                    return Ok(Reply::Ok(1));
                }
                v.hold(&t).await.map_err(|e| sql_error(&e))?;
                let in_txn = self.mq_in_transaction();
                let deleted = match self.mq_run(cx, &mq::ack_sql(&db, &t), true).await {
                    Ok(c) => c.affected.unwrap_or(0),
                    // R9 (r9tests): a user granted only the queue (not
                    // DELETE on its database's tables), such as an
                    // inherited queue sink's sprig_inherited, may consume
                    // it (the engine checked the queue's rights at the
                    // get). Outside a transaction the owner deletes the
                    // message itself, as for an AMQP acknowledgement.
                    Err(e)
                        if !in_txn
                            && matches!(e.protocol().mysql_errno, 1142 | 1143 | 1044)
                            && v.may_consume(&t) =>
                    {
                        v.release(&t, false);
                        v.ack(&t);
                        metrics::counter!("sprig_queue_acked_by_owner_total").increment(1);
                        return Ok(Reply::Ok(1));
                    }
                    Err(e) => {
                        v.release(&t, false);
                        return Err(e);
                    }
                };
                if deleted == 0 {
                    v.release(&t, false);
                    if in_txn {
                        let _ = self.mq_run(cx, "ROLLBACK", false).await;
                    }
                    return Err(sql_error(&MqError::new(
                        MqCode::AlreadySettled,
                        "the message was already settled (acknowledged, or returned after its lease); the transaction is rolled back",
                    )));
                }
                if in_txn {
                    // The session's commit hook tells the owner how the
                    // transaction ended ([`PassSession::mq_after_request`]).
                    engine.note_txn_hold(session, t);
                } else {
                    v.release(&t, true);
                }
                Ok(Reply::Ok(1))
            }
        }
    }
}

/// `Some(false)` for a lone `ROLLBACK` (text protocol), else `None`: the
/// commit hook's reading of a request.
pub(super) fn txn_word(req: &crate::pipeline::Request) -> Option<bool> {
    let crate::pipeline::Request::Query { sql, .. } = req else {
        return None;
    };
    let s = std::str::from_utf8(sql)
        .ok()?
        .trim()
        .trim_end_matches(';')
        .trim();
    let w = s.to_ascii_lowercase();
    let w = w.strip_suffix(" work").unwrap_or(&w);
    (w == "rollback").then_some(false)
}
