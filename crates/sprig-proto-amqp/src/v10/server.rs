//! The AMQP 1.0 server front end (chapter 50 step 14): the SASL layer,
//! `open`, sessions with their windows, links with credit, drain and
//! settlement modes, transfers in both directions, dispositions and
//! outcomes, RabbitMQ 4's address format v2, the anonymous terminus, and
//! direct reply-to over volatile links.
//!
//! One task serves a connection, like the 0-9-1 server. A session uses the
//! client's channel number for both directions, and a link the client's
//! handle: both are ours to choose, and choosing the same keeps the maps
//! simple. Session windows count transfer frames; delivery ids are a
//! separate per-session counter.
//!
//! Not served, as in RabbitMQ 4: transactions (a coordinator target),
//! link recovery (resuming unsettled state), and `rcv-settle-mode =
//! second`. The HTTP-over-AMQP management node (`/management`, step 15)
//! is not built yet; attaching to it is refused with `amqp:not-found`.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use sprig_common::CancellationToken;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::message::{Address, Sections, percent_encode};
use super::performative::{
    AmqpError, DeliveryState, FRAME_AMQP, FRAME_SASL, Frame, MIN_MAX_FRAME_SIZE, Performative,
    Source, Target, put_frame, put_heartbeat, role, sasl_code, snd_settle,
};
use super::types::Value;
use crate::auth::{self, ConnInfo, Principal};
use crate::backend::{
    ConnKey, ConsumeSpec, ConsumerId, DeadLetterReason, Event, ExchangeSpec, OpCtx, QueueSpec,
    Settlement, Vhost,
};
use crate::convert;
use crate::error::{MqCode, MqError, condition};
use crate::message::{Delivery, DeliveryId};
use crate::reply_to::PREFIX as REPLY_PREFIX;
use crate::server::{Config, Services, io_err};
use crate::sniff::{AMQP_10, AMQP_10_SASL};
use crate::v091::codec::FieldValue;

/// Transfer frames a session accepts before it widens the window again.
const SESSION_WINDOW: u32 = 8192;
/// How long a closing connection waits for the client's `close`.
const CLOSE_WAIT: Duration = Duration::from_secs(3);
/// The capability of a volatile (direct reply-to) source.
const VOLATILE: &str = "rabbitmq:volatile-queue";

fn amqp_error(e: &MqError) -> AmqpError {
    AmqpError::new(e.code.default_condition(), e.description())
}

async fn write<W: AsyncWrite + Unpin>(w: &mut W, out: &mut BytesMut) -> Result<(), MqError> {
    if !out.is_empty() {
        w.write_all(out).await.map_err(|e| io_err(&e))?;
        w.flush().await.map_err(|e| io_err(&e))?;
        out.clear();
    }
    Ok(())
}

/// Reads the next frame before the deadline.
async fn next_frame<R: AsyncRead + Unpin>(
    rd: &mut R,
    inb: &mut BytesMut,
    max: u32,
    deadline: Instant,
) -> Result<Frame, MqError> {
    loop {
        match Frame::parse(inb, max) {
            Ok(Some(f)) => return Ok(f),
            Ok(None) => {}
            Err(e) => return Err(MqError::new(MqCode::FrameError, e.0)),
        }
        match tokio::time::timeout_at(deadline, rd.read_buf(inb)).await {
            Ok(Ok(0)) => {
                return Err(MqError::new(MqCode::FrameError, "client closed").with_reply(320));
            }
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(io_err(&e)),
            Err(_) => {
                return Err(MqError::new(MqCode::FrameError, "handshake timeout").with_reply(320));
            }
        }
    }
}

fn perf(f: &Frame) -> Result<Performative, MqError> {
    let body = f.body.as_ref().ok_or_else(|| {
        MqError::new(
            MqCode::FrameError,
            "empty frame where a performative was expected",
        )
    })?;
    Performative::from_value(body).map_err(|e| MqError::new(MqCode::FrameError, e.0))
}

/// Runs a 1.0 connection whose `AMQP 3 1 0 0` header was read.
#[allow(clippy::too_many_lines)]
pub(crate) async fn run<S>(
    io: S,
    info: ConnInfo,
    svc: &Services,
    cfg: Config,
    close: CancellationToken,
) -> Result<(), MqError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let (mut rd, mut wr) = tokio::io::split(io);
    let mut inb = BytesMut::with_capacity(16 * 1024);
    let mut out = BytesMut::new();
    let deadline = Instant::now() + cfg.handshake_timeout;
    // SASL layer.
    out.extend_from_slice(&AMQP_10_SASL);
    let offered = auth::offered(&cfg.mechanisms, &info, true);
    put_frame(
        &mut out,
        FRAME_SASL,
        0,
        &Performative::SaslMechanisms {
            mechanisms: offered.clone(),
        }
        .to_value(),
        &[],
    );
    write(&mut wr, &mut out).await?;
    let f = next_frame(&mut rd, &mut inb, MIN_MAX_FRAME_SIZE * 128, deadline).await?;
    let Performative::SaslInit {
        mechanism,
        initial_response,
        ..
    } = perf(&f)?
    else {
        return Err(MqError::new(MqCode::SaslFailed, "expected sasl-init"));
    };
    let response = initial_response.unwrap_or_default();
    let creds = auth::credentials(&mechanism, &response, &offered, &info);
    let principal = match creds {
        Some(c) => svc.identity.login(&info, c).await.map_err(|e| e.0),
        None => Err(format!(
            "mechanism {mechanism} not offered or malformed response"
        )),
    };
    let code = if principal.is_ok() {
        sasl_code::OK
    } else {
        sasl_code::AUTH
    };
    put_frame(
        &mut out,
        FRAME_SASL,
        0,
        &Performative::SaslOutcome {
            code,
            additional_data: None,
        }
        .to_value(),
        &[],
    );
    write(&mut wr, &mut out).await?;
    let principal = match principal {
        Ok(p) => Arc::new(p),
        Err(why) => {
            metrics::counter!("sprig_amqp_auth_failures_total", "mechanism" => mechanism.to_ascii_uppercase())
                .increment(1);
            tracing::info!(peer = %info.peer, reason = %why, "AMQP 1.0 login refused");
            let _ = wr.shutdown().await;
            return Err(MqError::new(MqCode::SaslFailed, why));
        }
    };
    // The AMQP layer's header.
    while inb.len() < 8 {
        match tokio::time::timeout_at(deadline, rd.read_buf(&mut inb)).await {
            Ok(Ok(n)) if n > 0 => {}
            Ok(Err(e)) => return Err(io_err(&e)),
            _ => {
                return Err(MqError::new(
                    MqCode::ProtocolHeader,
                    "no AMQP header after SASL",
                ));
            }
        }
    }
    let header = inb.split_to(8);
    if header[..] != AMQP_10 {
        out.extend_from_slice(&AMQP_10);
        let _ = write(&mut wr, &mut out).await;
        return Err(MqError::new(
            MqCode::ProtocolHeader,
            "expected AMQP 0 1 0 0 after SASL",
        ));
    }
    out.extend_from_slice(&AMQP_10);
    write(&mut wr, &mut out).await?;
    let f = next_frame(
        &mut rd,
        &mut inb,
        cfg.frame_max.max(MIN_MAX_FRAME_SIZE),
        deadline,
    )
    .await?;
    let Performative::Open {
        hostname,
        max_frame_size,
        idle_timeout,
        ..
    } = perf(&f)?
    else {
        return Err(MqError::new(MqCode::FrameError, "expected open"));
    };
    let vhost_name = hostname
        .as_deref()
        .and_then(|h| h.strip_prefix("vhost:"))
        .map_or_else(|| "/".to_owned(), str::to_owned);
    let our_idle = u32::from(cfg.heartbeat) * 1000;
    let our_open = Performative::Open {
        container_id: cfg.node.clone(),
        hostname: None,
        max_frame_size: cfg.frame_max,
        channel_max: cfg.channel_max,
        idle_timeout: (our_idle > 0).then_some(our_idle),
        offered_capabilities: vec!["ANONYMOUS-RELAY".into()],
        desired_capabilities: vec![],
        properties: Some(Value::Map(vec![
            (Value::sym("product"), Value::str(&cfg.product)),
            (Value::sym("version"), Value::str(&cfg.version)),
            (Value::sym("platform"), Value::str("Rust")),
        ])),
    };
    put_frame(&mut out, FRAME_AMQP, 0, &our_open.to_value(), &[]);
    let key = ConnKey::next();
    let (events_tx, events) = mpsc::unbounded_channel();
    let vhost = match svc
        .backend
        .open_vhost(&principal, &vhost_name, key, events_tx.clone())
        .await
    {
        Ok(v) => v,
        Err(e) => {
            put_frame(
                &mut out,
                FRAME_AMQP,
                0,
                &Performative::Close {
                    error: Some(amqp_error(&e)),
                }
                .to_value(),
                &[],
            );
            let _ = write(&mut wr, &mut out).await;
            let _ = wr.shutdown().await;
            return Err(e);
        }
    };
    write(&mut wr, &mut out).await?;
    let mut conn = Conn {
        svc,
        cfg: cfg.clone(),
        key,
        vhost,
        principal,
        info,
        max_out: cfg.frame_max.min(max_frame_size).max(MIN_MAX_FRAME_SIZE),
        sessions: HashMap::new(),
        consumers: HashMap::new(),
        confirms: FuturesUnordered::new(),
        out,
        events_tx,
        closing: None,
        finished: false,
        epochs: 0,
        reply_slots: 0,
    };
    let peer_idle = idle_timeout
        .filter(|t| *t > 0)
        .map(|t| Duration::from_millis(u64::from(t)));
    let r = conn
        .serve(&mut rd, &mut wr, inb, events, close, peer_idle, our_idle)
        .await;
    conn.teardown().await;
    let _ = wr.shutdown().await;
    r
}

struct ConfirmDone {
    ch: u16,
    epoch: u64,
    handle: u32,
    delivery_id: u32,
    result: Result<(), MqError>,
}

struct InLink {
    /// `None`: the anonymous terminus (`to` per message).
    target: Option<Address>,
    /// Credit the client has left.
    credit: u32,
    delivery_count: u32,
    /// Credit to give back with the next flow.
    returned: u32,
    /// A multi-frame transfer in progress: delivery id, settled, payload.
    partial: Option<(u32, bool, BytesMut)>,
}

struct OutLink {
    consumer: ConsumerId,
    settled: bool,
    delivery_count: u32,
    /// Link credit the client gave and we haven't used.
    credit: u32,
    /// Credit granted to the engine and not used yet.
    engine_credit: u32,
    /// A direct reply-to link: its registry slot.
    reply_slot: Option<u16>,
}

enum Link {
    In(InLink),
    Out(OutLink),
}

struct Session {
    epoch: u64,
    links: HashMap<u32, Link>,
    /// Incoming transfer frames.
    next_incoming_id: u32,
    in_window: u32,
    /// Outgoing transfer frames.
    next_outgoing_id: u32,
    remote_next_incoming_id: u32,
    remote_incoming_window: u32,
    next_delivery_id: u32,
    /// Our unsettled deliveries: delivery id → link and message.
    unsettled: BTreeMap<u32, (u32, DeliveryId)>,
    /// Deliveries waiting for the client's incoming window.
    pending: VecDeque<(u32, Delivery)>,
    /// `end` sent, waiting for the client's.
    ending: bool,
}

struct Conn<'s> {
    svc: &'s Services,
    cfg: Config,
    key: ConnKey,
    vhost: Arc<dyn Vhost>,
    principal: Arc<Principal>,
    info: ConnInfo,
    max_out: u32,
    sessions: HashMap<u16, Session>,
    consumers: HashMap<ConsumerId, (u16, u32)>,
    confirms: FuturesUnordered<BoxFuture<'static, ConfirmDone>>,
    out: BytesMut,
    events_tx: crate::backend::EventSender,
    closing: Option<Instant>,
    finished: bool,
    epochs: u64,
    reply_slots: u16,
}

impl Conn<'_> {
    fn cx(&self, ch: u16) -> OpCtx {
        OpCtx {
            principal: Arc::clone(&self.principal),
            conn: self.key,
            channel: ch,
        }
    }

    fn send(&mut self, ch: u16, p: &Performative) {
        put_frame(&mut self.out, FRAME_AMQP, ch, &p.to_value(), &[]);
    }

    #[allow(clippy::too_many_arguments)]
    async fn serve<R, W>(
        &mut self,
        rd: &mut R,
        wr: &mut W,
        mut inb: BytesMut,
        mut events: mpsc::UnboundedReceiver<Event>,
        close: CancellationToken,
        peer_idle: Option<Duration>,
        our_idle_ms: u32,
    ) -> Result<(), MqError>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        let send_every = peer_idle.map(|d| d / 2);
        let recv_limit =
            (our_idle_ms > 0).then(|| Duration::from_millis(u64::from(our_idle_ms)) * 2);
        let tick = send_every
            .into_iter()
            .chain(recv_limit.map(|d| d / 4))
            .min()
            .unwrap_or(Duration::from_secs(3600))
            .max(Duration::from_millis(100));
        let mut ticker = tokio::time::interval(tick);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_recv = Instant::now();
        let mut last_send = Instant::now();
        loop {
            if !self.out.is_empty() {
                write(wr, &mut self.out).await?;
                last_send = Instant::now();
            }
            if self.finished {
                return Ok(());
            }
            let close_at = self.closing;
            tokio::select! {
                () = close.cancelled(), if self.closing.is_none() => {
                    self.close_connection(AmqpError::new(condition::CONNECTION_FORCED, "the gateway is shutting down"));
                }
                () = async { tokio::time::sleep_until(close_at.unwrap_or_else(Instant::now)).await }, if close_at.is_some() => {
                    return Ok(());
                }
                r = rd.read_buf(&mut inb) => {
                    match r {
                        Ok(0) => return Ok(()),
                        Ok(_) => {}
                        Err(e) => return Err(io_err(&e)),
                    }
                    last_recv = Instant::now();
                    loop {
                        let f = match Frame::parse(&mut inb, self.cfg.frame_max.max(MIN_MAX_FRAME_SIZE)) {
                            Ok(Some(f)) => f,
                            Ok(None) => break,
                            Err(e) => {
                                self.close_connection(AmqpError::new(condition::FRAMING_ERROR, e.0));
                                inb.clear();
                                break;
                            }
                        };
                        if let Err(e) = self.on_frame(f).await {
                            self.close_connection(e);
                        }
                        if self.finished {
                            break;
                        }
                    }
                }
                Some(ev) = events.recv() => self.on_event(ev).await,
                Some(c) = self.confirms.next(), if !self.confirms.is_empty() => self.on_confirm(c),
                _ = ticker.tick() => {
                    if let Some(limit) = recv_limit
                        && last_recv.elapsed() > limit && self.closing.is_none() {
                            self.close_connection(AmqpError::new(
                                condition::RESOURCE_LIMIT_EXCEEDED,
                                "local-idle-timeout expired",
                            ));
                        }
                    if let Some(every) = send_every
                        && last_send.elapsed() >= every {
                            put_heartbeat(&mut self.out);
                        }
                }
            }
        }
    }

    fn close_connection(&mut self, e: AmqpError) {
        if self.closing.is_some() {
            return;
        }
        tracing::debug!(peer = %self.info.peer, condition = %e.condition, description = ?e.description, "AMQP 1.0 connection error");
        self.send(0, &Performative::Close { error: Some(e) });
        self.closing = Some(Instant::now() + CLOSE_WAIT);
    }

    /// One frame; `Err` closes the connection.
    async fn on_frame(&mut self, f: Frame) -> Result<(), AmqpError> {
        if f.kind != FRAME_AMQP {
            return Err(AmqpError::new(
                condition::FRAMING_ERROR,
                "SASL frame after SASL",
            ));
        }
        if f.body.is_none() {
            return Ok(()); // heartbeat
        }
        let p = Performative::from_value(f.body.as_ref().unwrap_or(&Value::Null))
            .map_err(|e| AmqpError::new(condition::DECODE_ERROR, e.0))?;
        if self.closing.is_some() {
            if matches!(p, Performative::Close { .. }) {
                self.finished = true;
            }
            return Ok(());
        }
        let ch = f.channel;
        match p {
            Performative::Close { error } => {
                if let Some(e) = error {
                    tracing::debug!(peer = %self.info.peer, condition = %e.condition, "client closed with an error");
                }
                self.send(0, &Performative::Close { error: None });
                self.finished = true;
                Ok(())
            }
            Performative::Open { .. } => Err(AmqpError::new(condition::NOT_ALLOWED, "second open")),
            Performative::Begin {
                remote_channel,
                next_outgoing_id,
                incoming_window,
                handle_max,
                ..
            } => {
                if remote_channel.is_some() {
                    return Err(AmqpError::new(
                        condition::NOT_ALLOWED,
                        "begin with remote-channel from a client",
                    ));
                }
                if self.sessions.contains_key(&ch) {
                    return Err(AmqpError::new(
                        condition::NOT_ALLOWED,
                        format!("channel {ch} already has a session"),
                    ));
                }
                if ch > self.cfg.channel_max {
                    return Err(AmqpError::new(
                        condition::RESOURCE_LIMIT_EXCEEDED,
                        format!("channel {ch} above channel-max"),
                    ));
                }
                self.epochs += 1;
                self.sessions.insert(
                    ch,
                    Session {
                        epoch: self.epochs,
                        links: HashMap::new(),
                        next_incoming_id: next_outgoing_id,
                        in_window: SESSION_WINDOW,
                        next_outgoing_id: 0,
                        remote_next_incoming_id: 0,
                        remote_incoming_window: incoming_window,
                        next_delivery_id: 0,
                        unsettled: BTreeMap::new(),
                        pending: VecDeque::new(),
                        ending: false,
                    },
                );
                self.send(
                    ch,
                    &Performative::Begin {
                        remote_channel: Some(ch),
                        next_outgoing_id: 0,
                        incoming_window: SESSION_WINDOW,
                        outgoing_window: u32::MAX,
                        handle_max: handle_max.min(65_535),
                        offered_capabilities: vec![],
                        desired_capabilities: vec![],
                        properties: None,
                    },
                );
                Ok(())
            }
            Performative::End { .. } => {
                let ending = self.sessions.get(&ch).is_some_and(|s| s.ending);
                self.end_session(ch).await;
                if !ending {
                    self.send(ch, &Performative::End { error: None });
                }
                Ok(())
            }
            other => {
                let Some(s) = self.sessions.get(&ch) else {
                    return Err(AmqpError::new(
                        condition::NOT_ALLOWED,
                        format!("{} on channel {ch} without a session", other.name()),
                    ));
                };
                if s.ending {
                    return Ok(());
                }
                self.on_session(ch, other, f.payload).await
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn on_session(
        &mut self,
        ch: u16,
        p: Performative,
        payload: Bytes,
    ) -> Result<(), AmqpError> {
        match p {
            Performative::Attach {
                name,
                handle,
                role: their_role,
                snd_settle_mode,
                rcv_settle_mode,
                source,
                target,
                coordinator,
                initial_delivery_count,
                properties,
                ..
            } => {
                if self.session(ch).links.contains_key(&handle) {
                    return Err(AmqpError::new(
                        condition::HANDLE_IN_USE,
                        format!("handle {handle} in use"),
                    ));
                }
                let echo = (source.clone(), target.clone());
                let refused = if coordinator {
                    Some(AmqpError::new(
                        condition::NOT_IMPLEMENTED,
                        MqError::new(MqCode::Amqp10Unsupported, "transactions are not supported")
                            .description(),
                    ))
                } else if rcv_settle_mode == 1 {
                    Some(AmqpError::new(
                        condition::NOT_IMPLEMENTED,
                        MqError::new(
                            MqCode::Amqp10Unsupported,
                            "rcv-settle-mode second is not supported",
                        )
                        .description(),
                    ))
                } else {
                    None
                };
                if let Some(e) = refused {
                    self.refuse_attach(ch, &name, handle, !their_role, echo, e);
                    return Ok(());
                }
                if their_role == role::SENDER {
                    self.attach_in(
                        ch,
                        name,
                        handle,
                        snd_settle_mode,
                        source,
                        target,
                        initial_delivery_count,
                    )
                    .await;
                } else {
                    let fan_in = fan_in_property(properties.as_ref());
                    self.attach_out(ch, name, handle, snd_settle_mode, source, target, fan_in)
                        .await;
                }
                Ok(())
            }
            Performative::Flow {
                next_incoming_id,
                incoming_window,
                handle,
                delivery_count,
                link_credit,
                drain,
                echo,
                ..
            } => {
                {
                    let s = self.session(ch);
                    s.remote_next_incoming_id = next_incoming_id.unwrap_or(0);
                    s.remote_incoming_window = incoming_window;
                }
                if let Some(h) = handle {
                    self.link_flow(ch, h, delivery_count, link_credit, drain, echo);
                } else if echo {
                    self.session_flow(ch);
                }
                self.send_pending(ch);
                Ok(())
            }
            Performative::Transfer {
                handle,
                delivery_id,
                settled,
                more,
                aborted,
                ..
            } => self.on_transfer(ch, handle, delivery_id, settled, more, aborted, &payload),
            Performative::Disposition {
                role: r,
                first,
                last,
                settled,
                state,
                ..
            } => {
                if r == role::RECEIVER {
                    self.on_disposition(ch, first, last.unwrap_or(first), settled, state);
                }
                Ok(())
            }
            Performative::Detach { handle, closed, .. } => {
                self.drop_link(ch, handle).await;
                self.send(
                    ch,
                    &Performative::Detach {
                        handle,
                        closed,
                        error: None,
                    },
                );
                Ok(())
            }
            other => Err(AmqpError::new(
                condition::NOT_ALLOWED,
                format!("unexpected {}", other.name()),
            )),
        }
    }

    fn session(&mut self, ch: u16) -> &mut Session {
        self.sessions
            .get_mut(&ch)
            .unwrap_or_else(|| unreachable!("checked by on_frame"))
    }

    /// Answers an attach with a null terminus, then detaches with `e`.
    fn refuse_attach(
        &mut self,
        ch: u16,
        name: &str,
        handle: u32,
        our_role: bool,
        (source, target): (Option<Source>, Option<Target>),
        e: AmqpError,
    ) {
        let (source, target) = if our_role == role::RECEIVER {
            (source, None)
        } else {
            (None, target)
        };
        self.send(
            ch,
            &Performative::Attach {
                name: name.to_owned(),
                handle,
                role: our_role,
                snd_settle_mode: snd_settle::MIXED,
                rcv_settle_mode: 0,
                source,
                target,
                coordinator: false,
                unsettled: None,
                incomplete_unsettled: false,
                initial_delivery_count: (our_role == role::SENDER).then_some(0),
                max_message_size: None,
                offered_capabilities: vec![],
                desired_capabilities: vec![],
                properties: None,
            },
        );
        self.send(
            ch,
            &Performative::Detach {
                handle,
                closed: true,
                error: Some(e),
            },
        );
    }

    /// The client sends to us: a publishing link.
    #[allow(clippy::too_many_arguments)]
    async fn attach_in(
        &mut self,
        ch: u16,
        name: String,
        handle: u32,
        snd_settle_mode: u8,
        source: Option<Source>,
        target: Option<Target>,
        initial_delivery_count: Option<u32>,
    ) {
        let addr = target.as_ref().and_then(|t| t.address.clone());
        let parsed = match addr.as_deref() {
            None => Ok(None),
            Some(a) => Address::parse(a, self.cfg.legacy_addresses).map(Some),
        };
        let cx = self.cx(ch);
        let checked = match parsed {
            Err(why) => Err(AmqpError::new(
                condition::INVALID_FIELD,
                MqError::new(MqCode::LinkRefused, why).description(),
            )),
            Ok(None) => Ok(None),
            Ok(Some(a)) => {
                let exists = match &a {
                    Address::Exchange { exchange, .. } => {
                        self.vhost
                            .declare_exchange(
                                &cx,
                                ExchangeSpec {
                                    name: exchange.clone(),
                                    ..ExchangeSpec::default()
                                },
                                true,
                            )
                            .await
                    }
                    Address::Queue(q) if q.starts_with(REPLY_PREFIX) => Ok(()),
                    Address::Queue(q) => self
                        .vhost
                        .declare_queue(
                            &cx,
                            QueueSpec {
                                name: q.clone(),
                                ..QueueSpec::default()
                            },
                            true,
                        )
                        .await
                        .map(|_| ()),
                };
                exists.map(|()| Some(a)).map_err(|e| amqp_error(&e))
            }
        };
        let target_addr = match checked {
            Ok(a) => a,
            Err(e) => {
                self.refuse_attach(ch, &name, handle, role::RECEIVER, (source, target), e);
                return;
            }
        };
        let credit = self.cfg.link_credit.max(1);
        let dc = initial_delivery_count.unwrap_or(0);
        self.session(ch).links.insert(
            handle,
            Link::In(InLink {
                target: target_addr,
                credit,
                delivery_count: dc,
                returned: 0,
                partial: None,
            }),
        );
        self.send(
            ch,
            &Performative::Attach {
                name,
                handle,
                role: role::RECEIVER,
                snd_settle_mode,
                rcv_settle_mode: 0,
                source,
                target,
                coordinator: false,
                unsettled: None,
                incomplete_unsettled: false,
                initial_delivery_count: None,
                max_message_size: Some(self.cfg.max_message_bytes),
                offered_capabilities: vec![],
                desired_capabilities: vec![],
                properties: None,
            },
        );
        self.send_link_flow(ch, handle);
    }

    /// The client receives from us: a consuming link. `fan_in`: the link
    /// property `x-sprig-fan-in` was true (chapter 50, "Fan-in consumers").
    #[allow(clippy::too_many_arguments)]
    async fn attach_out(
        &mut self,
        ch: u16,
        name: String,
        handle: u32,
        snd_settle_mode: u8,
        source: Option<Source>,
        target: Option<Target>,
        fan_in: bool,
    ) {
        let settled = snd_settle_mode == snd_settle::SETTLED;
        let mut src = source.clone().unwrap_or_default();
        let volatile = src.address.is_none()
            && (src.dynamic || src.capabilities.iter().any(|c| c == VOLATILE));
        let consumer = ConsumerId::next();
        let mut reply_slot = None;
        let refusal = if volatile {
            if settled {
                self.reply_slots = self.reply_slots.wrapping_add(1);
                let slot = self.reply_slots;
                let reply_name = self.svc.replies.register(
                    self.key,
                    slot,
                    consumer,
                    self.events_tx.clone(),
                    self.principal.tenant_id,
                );
                src.address = Some(format!("/queues/{}", percent_encode(&reply_name)));
                reply_slot = Some(slot);
                None
            } else {
                Some(AmqpError::new(
                    condition::NOT_ALLOWED,
                    MqError::new(
                        MqCode::DirectReplyTo,
                        "a volatile reply link must be pre-settled",
                    )
                    .description(),
                ))
            }
        } else {
            match src
                .address
                .as_deref()
                .map(|a| Address::parse(a, self.cfg.legacy_addresses))
            {
                Some(Ok(Address::Queue(q))) => {
                    let mut arguments = crate::v091::codec::FieldTable::new();
                    if let Some(off) = stream_offset(src.filter.as_ref()) {
                        arguments.insert("x-stream-offset", off);
                    }
                    if fan_in {
                        arguments.insert(
                            "x-sprig-fan-in",
                            crate::v091::codec::FieldValue::Bool(true),
                        );
                    }
                    match self
                        .vhost
                        .consume(
                            &self.cx(ch),
                            ConsumeSpec {
                                consumer,
                                queue: q,
                                tag: name.clone(),
                                no_ack: settled,
                                exclusive: false,
                                priority: 0,
                                arguments,
                            },
                        )
                        .await
                    {
                        Ok(()) => None,
                        Err(e) => Some(amqp_error(&e)),
                    }
                }
                Some(Ok(Address::Exchange { .. })) => Some(AmqpError::new(
                    condition::INVALID_FIELD,
                    MqError::new(MqCode::LinkRefused, "a source must be /queues/:queue")
                        .description(),
                )),
                Some(Err(why)) => Some(AmqpError::new(
                    condition::INVALID_FIELD,
                    MqError::new(MqCode::LinkRefused, why).description(),
                )),
                None => Some(AmqpError::new(
                    condition::INVALID_FIELD,
                    MqError::new(MqCode::LinkRefused, "a source address is required").description(),
                )),
            }
        };
        if let Some(e) = refusal {
            self.refuse_attach(ch, &name, handle, role::SENDER, (source, target), e);
            return;
        }
        self.session(ch).links.insert(
            handle,
            Link::Out(OutLink {
                consumer,
                settled,
                delivery_count: 0,
                credit: 0,
                engine_credit: 0,
                reply_slot,
            }),
        );
        self.consumers.insert(consumer, (ch, handle));
        self.send(
            ch,
            &Performative::Attach {
                name,
                handle,
                role: role::SENDER,
                snd_settle_mode: if settled {
                    snd_settle::SETTLED
                } else {
                    snd_settle::UNSETTLED
                },
                rcv_settle_mode: 0,
                source: Some(src),
                target,
                coordinator: false,
                unsettled: None,
                incomplete_unsettled: false,
                initial_delivery_count: Some(0),
                max_message_size: None,
                offered_capabilities: vec![],
                desired_capabilities: vec![],
                properties: None,
            },
        );
    }

    fn send_link_flow(&mut self, ch: u16, handle: u32) {
        let s = self.session(ch);
        let (dc, credit, drain) = match s.links.get(&handle) {
            Some(Link::In(l)) => (l.delivery_count, l.credit, false),
            Some(Link::Out(l)) => (l.delivery_count, l.credit, false),
            None => return,
        };
        let p = Performative::Flow {
            next_incoming_id: Some(s.next_incoming_id),
            incoming_window: s.in_window,
            next_outgoing_id: s.next_outgoing_id,
            outgoing_window: u32::MAX,
            handle: Some(handle),
            delivery_count: Some(dc),
            link_credit: Some(credit),
            available: None,
            drain,
            echo: false,
            properties: None,
        };
        self.send(ch, &p);
    }

    fn session_flow(&mut self, ch: u16) {
        let s = self.session(ch);
        let p = Performative::Flow {
            next_incoming_id: Some(s.next_incoming_id),
            incoming_window: s.in_window,
            next_outgoing_id: s.next_outgoing_id,
            outgoing_window: u32::MAX,
            handle: None,
            delivery_count: None,
            link_credit: None,
            available: None,
            drain: false,
            echo: false,
            properties: None,
        };
        self.send(ch, &p);
    }

    fn link_flow(
        &mut self,
        ch: u16,
        handle: u32,
        delivery_count: Option<u32>,
        link_credit: Option<u32>,
        drain: bool,
        echo: bool,
    ) {
        let vhost = Arc::clone(&self.vhost);
        let s = self.session(ch);
        match s.links.get_mut(&handle) {
            Some(Link::Out(l)) => {
                if let Some(lc) = link_credit {
                    // credit = delivery-count(rcv) + link-credit(rcv) - delivery-count(snd)
                    let dc_rcv = delivery_count.unwrap_or(0);
                    let credit = dc_rcv.wrapping_add(lc).wrapping_sub(l.delivery_count);
                    let credit = if credit > lc.saturating_add(1 << 20) {
                        0
                    } else {
                        credit
                    };
                    l.credit = credit;
                    if credit > l.engine_credit {
                        vhost.credit(l.consumer, credit - l.engine_credit, drain);
                        l.engine_credit = credit;
                    } else if drain {
                        vhost.credit(l.consumer, 0, true);
                    }
                }
                if echo {
                    self.send_link_flow(ch, handle);
                }
            }
            Some(Link::In(_)) => {
                if echo {
                    self.send_link_flow(ch, handle);
                }
            }
            None => {}
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_transfer(
        &mut self,
        ch: u16,
        handle: u32,
        delivery_id: Option<u32>,
        settled: Option<bool>,
        more: bool,
        aborted: bool,
        payload: &Bytes,
    ) -> Result<(), AmqpError> {
        let max = self.cfg.max_message_bytes;
        let s = self.session(ch);
        s.next_incoming_id = s.next_incoming_id.wrapping_add(1);
        s.in_window = s.in_window.saturating_sub(1);
        let widen = s.in_window < SESSION_WINDOW / 2;
        if widen {
            s.in_window = SESSION_WINDOW;
        }
        let Some(Link::In(l)) = s.links.get_mut(&handle) else {
            return Err(AmqpError::new(
                condition::UNATTACHED_HANDLE,
                format!("transfer on handle {handle}, which is not a receiving link"),
            ));
        };
        let (id, pre_settled, mut buf) = if let Some(p) = l.partial.take() {
            p
        } else {
            let Some(id) = delivery_id else {
                return Err(AmqpError::new(
                    condition::INVALID_FIELD,
                    "first transfer without delivery-id",
                ));
            };
            if l.credit == 0 {
                return Err(AmqpError::new(
                    condition::TRANSFER_LIMIT_EXCEEDED,
                    "transfer without link credit",
                ));
            }
            l.credit -= 1;
            l.delivery_count = l.delivery_count.wrapping_add(1);
            (id, settled.unwrap_or(false), BytesMut::new())
        };
        let pre_settled = pre_settled || settled.unwrap_or(false);
        if aborted {
            l.returned += 1;
            if widen {
                self.session_flow(ch);
            }
            return Ok(());
        }
        buf.extend_from_slice(payload);
        if buf.len() as u64 > max {
            let e = MqError::new(
                MqCode::MessageTooLarge,
                format!("message size exceeds the configured max size {max}"),
            );
            l.returned += 1;
            self.disposition_in(ch, id, DeliveryState::Rejected(Some(amqp_error(&e))));
            // Swallow the rest of the delivery.
            if more && let Some(Link::In(l)) = self.session(ch).links.get_mut(&handle) {
                l.partial = Some((id, true, BytesMut::new()));
            }
            return Ok(());
        }
        if more {
            l.partial = Some((id, pre_settled, buf));
            if widen {
                self.session_flow(ch);
            }
            return Ok(());
        }
        let target = l.target.clone();
        if widen {
            self.session_flow(ch);
        }
        self.publish(ch, handle, id, pre_settled, target, &buf.freeze());
        Ok(())
    }

    /// A whole message from a publishing link.
    fn publish(
        &mut self,
        ch: u16,
        handle: u32,
        id: u32,
        settled: bool,
        target: Option<Address>,
        payload: &Bytes,
    ) {
        let outcome = self.route_and_publish(ch, target, payload);
        match outcome {
            Ok(Some(confirm)) => {
                let epoch = self.session(ch).epoch;
                if settled {
                    self.return_credit(ch, handle, 1);
                    // Pre-settled: nobody waits, but the engine still commits.
                    tokio::spawn(async move {
                        let _ = confirm.await;
                    });
                } else {
                    self.confirms.push(Box::pin(async move {
                        ConfirmDone {
                            ch,
                            epoch,
                            handle,
                            delivery_id: id,
                            result: confirm.await,
                        }
                    }));
                }
            }
            Ok(None) => {
                // Unroutable: released.
                self.return_credit(ch, handle, 1);
                if !settled {
                    self.disposition_in(ch, id, DeliveryState::Released);
                }
            }
            Err(e) => {
                self.return_credit(ch, handle, 1);
                if !settled {
                    self.disposition_in(ch, id, DeliveryState::Rejected(Some(e)));
                }
            }
        }
    }

    fn route_and_publish(
        &mut self,
        ch: u16,
        target: Option<Address>,
        payload: &Bytes,
    ) -> Result<Option<BoxFuture<'static, Result<(), MqError>>>, AmqpError> {
        let s =
            Sections::parse(payload).map_err(|e| AmqpError::new(condition::DECODE_ERROR, e.0))?;
        let addr = if let Some(a) = target {
            a
        } else {
            let to = s.properties.to.as_deref().ok_or_else(|| {
                AmqpError::new(
                    condition::PRECONDITION_FAILED,
                    "the anonymous terminus needs the 'to' property",
                )
            })?;
            Address::parse(to, self.cfg.legacy_addresses).map_err(|why| {
                AmqpError::new(
                    condition::PRECONDITION_FAILED,
                    MqError::new(MqCode::LinkRefused, why).description(),
                )
            })?
        };
        let (exchange, key) = addr.route();
        let msg = convert::from_10(exchange.to_owned(), key.to_owned(), &s);
        if exchange.is_empty() && key.starts_with(REPLY_PREFIX) {
            let _ = self.svc.replies.deliver(key, msg, self.principal.tenant_id);
            return Ok(Some(Box::pin(async { Ok(()) })));
        }
        let p = self
            .vhost
            .publish(&self.cx(ch), msg, false)
            .map_err(|e| amqp_error(&e))?;
        if p.routed == 0 {
            return Ok(None);
        }
        Ok(Some(p.confirm))
    }

    fn return_credit(&mut self, ch: u16, handle: u32, n: u32) {
        let target = self.cfg.link_credit.max(1);
        let Some(s) = self.sessions.get_mut(&ch) else {
            return;
        };
        let Some(Link::In(l)) = s.links.get_mut(&handle) else {
            return;
        };
        l.returned += n;
        if l.returned >= (target / 2).max(1) {
            l.credit = l.credit.saturating_add(l.returned).min(target);
            l.returned = 0;
            self.send_link_flow(ch, handle);
        }
    }

    fn disposition_in(&mut self, ch: u16, id: u32, state: DeliveryState) {
        self.send(
            ch,
            &Performative::Disposition {
                role: role::RECEIVER,
                first: id,
                last: None,
                settled: true,
                state: Some(state),
                batchable: false,
            },
        );
    }

    fn on_confirm(&mut self, c: ConfirmDone) {
        if self
            .sessions
            .get(&c.ch)
            .is_none_or(|s| s.epoch != c.epoch || s.ending)
        {
            return;
        }
        let state = match c.result {
            Ok(()) => DeliveryState::Accepted,
            Err(e) => {
                let mut err = amqp_error(&e);
                if e.code == MqCode::QueueFull {
                    err.condition = condition::RESOURCE_LIMIT_EXCEEDED.into();
                }
                DeliveryState::Rejected(Some(err))
            }
        };
        self.disposition_in(c.ch, c.delivery_id, state);
        self.return_credit(c.ch, c.handle, 1);
    }

    /// The client settles our deliveries.
    fn on_disposition(
        &mut self,
        ch: u16,
        first: u32,
        last: u32,
        settled: bool,
        state: Option<DeliveryState>,
    ) {
        let settlement = match &state {
            Some(DeliveryState::Accepted) => Some(Settlement::Ack),
            Some(DeliveryState::Rejected(_)) => {
                Some(Settlement::DeadLetter(DeadLetterReason::Rejected))
            }
            Some(DeliveryState::Released) => Some(Settlement::Requeue { failed: false }),
            Some(DeliveryState::Modified {
                delivery_failed,
                undeliverable_here,
                ..
            }) => Some(if *undeliverable_here {
                Settlement::DeadLetter(DeadLetterReason::Rejected)
            } else {
                Settlement::Requeue {
                    failed: *delivery_failed,
                }
            }),
            // Settled with no outcome: the source's default, released.
            None if settled => Some(Settlement::Requeue { failed: false }),
            _ => None,
        };
        let Some(settlement) = settlement else { return };
        let cx = self.cx(ch);
        let s = self.session(ch);
        let ids: Vec<u32> = if last >= first {
            s.unsettled.range(first..=last).map(|(k, _)| *k).collect()
        } else {
            // The range wrapped around the serial number space.
            s.unsettled
                .keys()
                .copied()
                .filter(|k| *k >= first || *k <= last)
                .collect()
        };
        let batch: Vec<(DeliveryId, Settlement)> = ids
            .iter()
            .filter_map(|k| s.unsettled.remove(k).map(|(_, d)| (d, settlement)))
            .collect();
        if !batch.is_empty() {
            self.vhost.settle(&cx, batch);
        }
        if !settled {
            self.send(
                ch,
                &Performative::Disposition {
                    role: role::SENDER,
                    first,
                    last: Some(last),
                    settled: true,
                    state,
                    batchable: false,
                },
            );
        }
    }

    async fn on_event(&mut self, ev: Event) {
        match ev {
            Event::Deliver { consumer, delivery } => {
                let Some(&(ch, handle)) = self.consumers.get(&consumer) else {
                    self.requeue(&delivery);
                    return;
                };
                self.deliver(ch, handle, delivery);
            }
            Event::Cancelled { consumer, reason } => {
                let Some((ch, handle)) = self.consumers.remove(&consumer) else {
                    return;
                };
                self.drop_link(ch, handle).await;
                let mut e = amqp_error(&reason);
                if reason.code == MqCode::NotFound {
                    e.condition = condition::RESOURCE_DELETED.into();
                }
                self.send(
                    ch,
                    &Performative::Detach {
                        handle,
                        closed: true,
                        error: Some(e),
                    },
                );
            }
            Event::Drained { consumer, credit } => {
                let Some(&(ch, handle)) = self.consumers.get(&consumer) else {
                    return;
                };
                let Some(Link::Out(l)) = self.session(ch).links.get_mut(&handle) else {
                    return;
                };
                l.delivery_count = l
                    .delivery_count
                    .wrapping_add(l.credit.max(credit).min(l.credit));
                l.credit = 0;
                l.engine_credit = 0;
                let dc = l.delivery_count;
                let s = self.session(ch);
                let p = Performative::Flow {
                    next_incoming_id: Some(s.next_incoming_id),
                    incoming_window: s.in_window,
                    next_outgoing_id: s.next_outgoing_id,
                    outgoing_window: u32::MAX,
                    handle: Some(handle),
                    delivery_count: Some(dc),
                    link_credit: Some(0),
                    available: Some(0),
                    drain: true,
                    echo: false,
                    properties: None,
                };
                self.send(ch, &p);
            }
            // 1.0 has no blocked notice: credit is the back-pressure.
            Event::Blocked(_) | Event::Unblocked => {}
        }
    }

    fn requeue(&self, d: &Delivery) {
        if d.id != DeliveryId::default() {
            self.vhost.settle(
                &self.cx(0),
                vec![(d.id, Settlement::Requeue { failed: false })],
            );
        }
    }

    fn deliver(&mut self, ch: u16, handle: u32, d: Delivery) {
        let Some(s) = self.sessions.get_mut(&ch) else {
            self.requeue(&d);
            return;
        };
        let Some(Link::Out(l)) = s.links.get_mut(&handle) else {
            self.requeue(&d);
            return;
        };
        let reply = l.reply_slot.is_some();
        if l.credit == 0 {
            // Replies without credit are dropped, as in RabbitMQ.
            if !reply {
                self.requeue(&d);
            }
            return;
        }
        l.credit -= 1;
        l.engine_credit = l.engine_credit.saturating_sub(1);
        l.delivery_count = l.delivery_count.wrapping_add(1);
        if s.pending.is_empty() && self.window_open(ch) {
            self.send_transfer(ch, handle, &d);
        } else {
            self.session(ch).pending.push_back((handle, d));
        }
    }

    fn window_open(&mut self, ch: u16) -> bool {
        let s = self.session(ch);
        let limit = s
            .remote_next_incoming_id
            .wrapping_add(s.remote_incoming_window);
        // Serial-number comparison: next_outgoing_id < limit.
        limit.wrapping_sub(s.next_outgoing_id).cast_signed() > 0
    }

    fn send_pending(&mut self, ch: u16) {
        while self.window_open(ch) {
            let Some((handle, d)) = self.session(ch).pending.pop_front() else {
                break;
            };
            self.send_transfer(ch, handle, &d);
        }
    }

    fn send_transfer(&mut self, ch: u16, handle: u32, d: &Delivery) {
        let payload = match convert::to_10(d) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, queue = %d.queue, "undeliverable stored message dead-lettered");
                self.vhost.settle(
                    &self.cx(ch),
                    vec![(d.id, Settlement::DeadLetter(DeadLetterReason::Rejected))],
                );
                return;
            }
        };
        let max_out = self.max_out as usize;
        let s = self.session(ch);
        let settled = matches!(s.links.get(&handle), Some(Link::Out(l)) if l.settled);
        let id = s.next_delivery_id;
        s.next_delivery_id = s.next_delivery_id.wrapping_add(1);
        if !settled {
            s.unsettled.insert(id, (handle, d.id));
        }
        // Frame overhead: 8-byte header plus a transfer of at most ~40
        // bytes; leave room.
        let room = max_out.saturating_sub(64).max(64);
        let chunks: Vec<&[u8]> = if payload.is_empty() {
            vec![&[]]
        } else {
            payload.chunks(room).collect()
        };
        let n = chunks.len();
        for (i, chunk) in chunks.into_iter().enumerate() {
            let first = i == 0;
            let t = Performative::Transfer {
                handle,
                delivery_id: first.then_some(id),
                delivery_tag: first.then(|| Bytes::copy_from_slice(&id.to_be_bytes())),
                message_format: first.then_some(0),
                settled: first.then_some(settled),
                more: i + 1 < n,
                rcv_settle_mode: None,
                state: None,
                resume: false,
                aborted: false,
                batchable: false,
            };
            put_frame(&mut self.out, FRAME_AMQP, ch, &t.to_value(), chunk);
            let s = self.session(ch);
            s.next_outgoing_id = s.next_outgoing_id.wrapping_add(1);
        }
    }

    /// Ends a link: its consumer ends and its unsettled deliveries are
    /// requeued.
    async fn drop_link(&mut self, ch: u16, handle: u32) {
        let cx = self.cx(ch);
        let Some(s) = self.sessions.get_mut(&ch) else {
            return;
        };
        let Some(link) = s.links.remove(&handle) else {
            return;
        };
        if let Link::Out(l) = link {
            let ids: Vec<u32> = s
                .unsettled
                .iter()
                .filter(|(_, (h, _))| *h == handle)
                .map(|(k, _)| *k)
                .collect();
            let mut batch: Vec<(DeliveryId, Settlement)> = ids
                .iter()
                .filter_map(|k| {
                    s.unsettled
                        .remove(k)
                        .map(|(_, d)| (d, Settlement::Requeue { failed: false }))
                })
                .collect();
            let pending: VecDeque<(u32, Delivery)> = std::mem::take(&mut s.pending);
            for (h, d) in pending {
                if h == handle {
                    batch.push((d.id, Settlement::Requeue { failed: false }));
                } else {
                    s.pending.push_back((h, d));
                }
            }
            self.consumers.remove(&l.consumer);
            if let Some(slot) = l.reply_slot {
                self.svc.replies.unregister(self.key, slot);
            } else {
                self.vhost.cancel(&cx, l.consumer).await;
            }
            batch.retain(|(id, _)| *id != DeliveryId::default());
            if !batch.is_empty() {
                self.vhost.settle(&cx, batch);
            }
        }
    }

    async fn end_session(&mut self, ch: u16) {
        let handles: Vec<u32> = self
            .sessions
            .get(&ch)
            .map(|s| s.links.keys().copied().collect())
            .unwrap_or_default();
        for h in handles {
            self.drop_link(ch, h).await;
        }
        self.sessions.remove(&ch);
    }

    async fn teardown(&mut self) {
        let chans: Vec<u16> = self.sessions.keys().copied().collect();
        for ch in chans {
            self.end_session(ch).await;
        }
        self.svc.replies.unregister_conn(self.key);
        self.vhost.connection_closed(self.key).await;
    }
}

/// Whether a link's properties hold `x-sprig-fan-in` = true (a boolean,
/// or the string `true`).
fn fan_in_property(p: Option<&Value>) -> bool {
    let Some(Value::Map(m)) = p else {
        return false;
    };
    m.iter().any(|(k, v)| {
        matches!(k, Value::Symbol(s) | Value::String(s) if s == "x-sprig-fan-in")
            && match v {
                Value::Bool(b) => *b,
                Value::String(s) | Value::Symbol(s) => s.eq_ignore_ascii_case("true"),
                _ => false,
            }
    })
}

/// `rabbitmq:stream-offset-spec` from a source filter, as the 0-9-1
/// `x-stream-offset` argument.
fn stream_offset(filter: Option<&Value>) -> Option<FieldValue> {
    let Some(Value::Map(m)) = filter else {
        return None;
    };
    m.iter().find_map(|(k, v)| {
        if k.as_str() != Some("rabbitmq:stream-offset-spec") {
            return None;
        }
        let inner = match v {
            Value::Described(_, inner) => &**inner,
            other => other,
        };
        convert::to_field(inner)
    })
}
