//! The AMQP 0-9-1 server front end: the connection
//! handshake (`start`, SASL, `tune`, `open`), channels, every method of
//! `exchange`, `queue`, `basic`, `confirm` and `tx`, content assembly,
//! heartbeats, and RabbitMQ's reply codes and extensions.
//!
//! One task serves a connection. It reads frames, handles each in order
//! (awaiting the engine for declarations), and between frames writes
//! deliveries, confirms and notices that arrive from the engine. Errors
//! that the spec calls soft close the channel (`channel.close`); hard
//! ones close the connection (`connection.close`).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use sprig_common::CancellationToken;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::codec::{FieldTable, FieldValue};
use super::frame::{
    ContentHeader, FRAME_BODY, FRAME_HEADER, FRAME_HEARTBEAT, FRAME_METHOD, FRAME_MIN_SIZE, Frame,
    put_content, put_heartbeat, put_method,
};
use super::method::{
    BasicAck, BasicCancel, BasicCancelOk, BasicConsume, BasicConsumeOk, BasicDeliver,
    BasicGetEmpty, BasicGetOk, BasicNack, BasicPublish, BasicQosOk, BasicRecoverOk, BasicReturn,
    ChannelClose, ChannelCloseOk, ChannelFlowOk, ChannelOpenOk, ConfirmSelectOk, ConnectionBlocked,
    ConnectionClose, ConnectionCloseOk, ConnectionOpenOk, ConnectionStart, ConnectionTune,
    ConnectionUnblocked, ConnectionUpdateSecretOk, ExchangeBindOk, ExchangeDeclareOk,
    ExchangeDeleteOk, ExchangeUnbindOk, Method, MethodError, QueueBindOk, QueueDeclareOk,
    QueueDeleteOk, QueuePurgeOk, QueueUnbindOk, TxCommitOk, TxRollbackOk, TxSelectOk,
};
use crate::auth::{self, ConnInfo, Credentials, Principal};
use crate::backend::{
    BindingSpec, ConnKey, ConsumeSpec, ConsumerId, DeadLetterReason, DestKind, Event, ExchangeSpec,
    OpCtx, QueueSpec, Settlement, TxBatch, Vhost,
};
use crate::convert;
use crate::error::{MqCode, MqError, reply};
use crate::message::{Delivery, DeliveryId, Message};
use crate::reply_to::{PREFIX as REPLY_PREFIX, PSEUDO_QUEUE};
use crate::server::{Config, Services, io_err};

/// How long a closing connection waits for `connection.close-ok`.
const CLOSE_WAIT: Duration = Duration::from_secs(3);

/// The capabilities a client announced in `client_properties`.
#[derive(Debug, Clone, Copy, Default)]
struct ClientCaps {
    cancel_notify: bool,
    auth_failure_close: bool,
    blocked: bool,
}

impl ClientCaps {
    fn of(props: &FieldTable) -> Self {
        let Some(FieldValue::Table(c)) = props.get("capabilities") else {
            return Self::default();
        };
        Self {
            cancel_notify: c.flag("consumer_cancel_notify"),
            auth_failure_close: c.flag("authentication_failure_close"),
            blocked: c.flag("connection.blocked"),
        }
    }
}

fn server_properties(cfg: &Config) -> FieldTable {
    let t = |b: bool| FieldValue::Bool(b);
    let caps = FieldTable::from([
        ("publisher_confirms", t(true)),
        ("exchange_exchange_bindings", t(true)),
        ("basic.nack", t(true)),
        ("consumer_cancel_notify", t(true)),
        ("connection.blocked", t(true)),
        ("consumer_priorities", t(true)),
        ("authentication_failure_close", t(true)),
        ("per_consumer_qos", t(true)),
        ("direct_reply_to", t(true)),
    ]);
    FieldTable::from([
        ("capabilities", FieldValue::Table(caps)),
        ("cluster_name", FieldValue::str(&cfg.node)),
        ("copyright", FieldValue::str("SprigDB contributors")),
        (
            "information",
            FieldValue::str("SprigDB: RabbitMQ-compatible queues on MariaDB. Apache-2.0."),
        ),
        ("platform", FieldValue::str("Rust")),
        ("product", FieldValue::str(&cfg.product)),
        ("version", FieldValue::str(&cfg.version)),
    ])
}

/// A random name suffix: 22 base64url characters, as RabbitMQ's.
pub(crate) fn random_suffix() -> String {
    let mut b = [0u8; 16];
    let _ = ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut b);
    URL_SAFE_NO_PAD.encode(b)
}

async fn flush<W: AsyncWrite + Unpin>(w: &mut W, out: &mut BytesMut) -> Result<(), MqError> {
    if !out.is_empty() {
        w.write_all(out).await.map_err(|e| io_err(&e))?;
        w.flush().await.map_err(|e| io_err(&e))?;
        out.clear();
    }
    Ok(())
}

/// Reads the next method frame on channel 0 during the handshake.
async fn expect<R: AsyncRead + Unpin>(
    rd: &mut R,
    inb: &mut BytesMut,
    frame_max: u32,
    deadline: Instant,
) -> Result<Method, MqError> {
    loop {
        if let Some(f) = Frame::parse(inb, frame_max)? {
            match f.kind {
                FRAME_HEARTBEAT => {}
                FRAME_METHOD if f.channel == 0 => {
                    return Method::decode(&f.payload).map_err(method_error);
                }
                _ => {
                    return Err(MqError::frame(
                        reply::UNEXPECTED_FRAME,
                        "expected a connection method on channel 0",
                    ));
                }
            }
            continue;
        }
        match tokio::time::timeout_at(deadline, rd.read_buf(inb)).await {
            Ok(Ok(0)) => {
                return Err(MqError::frame(
                    reply::CONNECTION_FORCED,
                    "client closed during handshake",
                ));
            }
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(io_err(&e)),
            Err(_) => {
                return Err(MqError::frame(
                    reply::CONNECTION_FORCED,
                    "handshake timeout",
                ));
            }
        }
    }
}

fn method_error(e: MethodError) -> MqError {
    match e {
        MethodError::Unknown(c, m) => MqError::not_implemented(format!("unknown method {c}.{m}")),
        MethodError::Syntax(e) => e.into(),
    }
}

/// Sends `connection.close` with `e` and waits briefly for close-ok.
async fn refuse<R, W>(rd: &mut R, wr: &mut W, inb: &mut BytesMut, e: &MqError)
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut out = BytesMut::new();
    put_method(
        &mut out,
        0,
        &ConnectionClose {
            reply_code: e.reply,
            reply_text: e.reply_text(),
            class_id: 0,
            method_id: 0,
        }
        .into(),
    );
    if flush(wr, &mut out).await.is_ok() {
        let _ = expect(rd, inb, 0, Instant::now() + Duration::from_secs(1)).await;
    }
    let _ = wr.shutdown().await;
}

/// Runs a 0-9-1 connection whose protocol header was read.
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
    let offered = auth::offered(&cfg.mechanisms, &info, false);
    put_method(
        &mut out,
        0,
        &ConnectionStart {
            version_major: 0,
            version_minor: 9,
            server_properties: server_properties(&cfg),
            mechanisms: Bytes::from(offered.join(" ")),
            locales: Bytes::from_static(b"en_US"),
        }
        .into(),
    );
    flush(&mut wr, &mut out).await?;
    let hs_max = cfg.frame_max.max(FRAME_MIN_SIZE);
    let Method::ConnectionStartOk(start_ok) = expect(&mut rd, &mut inb, hs_max, deadline).await?
    else {
        return Err(MqError::frame(
            reply::COMMAND_INVALID,
            "expected connection.start-ok",
        ));
    };
    let caps = ClientCaps::of(&start_ok.client_properties);
    let creds = auth::credentials(&start_ok.mechanism, &start_ok.response, &offered, &info);
    let login_user = match &creds {
        Some(Credentials::Password { user, .. }) => user.clone(),
        Some(Credentials::External { login }) => login.clone(),
        None => String::new(),
    };
    let principal = match creds {
        Some(c) => svc.identity.login(&info, c).await.map_err(|e| e.0),
        None => Err(format!(
            "mechanism {} not offered or malformed response",
            start_ok.mechanism
        )),
    };
    let principal = match principal {
        Ok(p) => Arc::new(p),
        Err(why) => {
            metrics::counter!("sprig_amqp_auth_failures_total", "mechanism" => start_ok.mechanism.to_ascii_uppercase())
                .increment(1);
            tracing::info!(peer = %info.peer, user = %login_user, reason = %why, "AMQP login refused");
            let e = MqError::new(
                MqCode::SaslFailed,
                format!(
                    "Login was refused using authentication mechanism {}. For details see the broker logfile.",
                    start_ok.mechanism
                ),
            );
            if caps.auth_failure_close {
                refuse(&mut rd, &mut wr, &mut inb, &e).await;
            } else {
                let _ = wr.shutdown().await;
            }
            return Err(e);
        }
    };
    put_method(
        &mut out,
        0,
        &ConnectionTune {
            channel_max: cfg.channel_max,
            frame_max: cfg.frame_max,
            heartbeat: cfg.heartbeat,
        }
        .into(),
    );
    flush(&mut wr, &mut out).await?;
    let Method::ConnectionTuneOk(tune) = expect(&mut rd, &mut inb, hs_max, deadline).await? else {
        return Err(MqError::frame(
            reply::COMMAND_INVALID,
            "expected connection.tune-ok",
        ));
    };
    let frame_max = if tune.frame_max == 0 {
        cfg.frame_max
    } else {
        tune.frame_max
    };
    let channel_max = if tune.channel_max == 0 {
        cfg.channel_max
    } else {
        tune.channel_max
    };
    let negotiation = if frame_max > cfg.frame_max && cfg.frame_max != 0 {
        Some(format!(
            "negotiated frame_max = {frame_max} is higher than the maximum allowed value ({})",
            cfg.frame_max
        ))
    } else if frame_max < FRAME_MIN_SIZE {
        Some(format!("frame_max={frame_max} < {FRAME_MIN_SIZE} min size"))
    } else if channel_max > cfg.channel_max && cfg.channel_max != 0 {
        Some(format!(
            "negotiated channel_max = {channel_max} is higher than the maximum allowed value ({})",
            cfg.channel_max
        ))
    } else {
        None
    };
    if let Some(text) = negotiation {
        let e = MqError::new(MqCode::ChannelLimit, text).with_reply(reply::NOT_ALLOWED);
        refuse(&mut rd, &mut wr, &mut inb, &e).await;
        return Err(e);
    }
    let Method::ConnectionOpen(open) = expect(&mut rd, &mut inb, frame_max, deadline).await? else {
        return Err(MqError::frame(
            reply::COMMAND_INVALID,
            "expected connection.open",
        ));
    };
    let vhost_name = if open.virtual_host.is_empty() {
        "/".to_owned()
    } else {
        open.virtual_host
    };
    let key = ConnKey::next();
    let (events_tx, events) = mpsc::unbounded_channel();
    let vhost = match svc
        .backend
        .open_vhost(&principal, &vhost_name, key, events_tx.clone())
        .await
    {
        Ok(v) => v,
        Err(e) => {
            let e = e.with_reply(reply::NOT_ALLOWED);
            refuse(&mut rd, &mut wr, &mut inb, &e).await;
            return Err(e);
        }
    };
    put_method(&mut out, 0, &ConnectionOpenOk::default().into());
    flush(&mut wr, &mut out).await?;
    tracing::debug!(peer = %info.peer, user = %principal.user, vhost = %vhost_name, "AMQP 0-9-1 connection open");
    let mut conn = Conn {
        svc,
        cfg,
        key,
        vhost,
        principal,
        login_user,
        info,
        frame_max,
        channel_max,
        heartbeat: tune.heartbeat,
        caps,
        channels: HashMap::new(),
        consumers: HashMap::new(),
        confirms: FuturesUnordered::new(),
        out,
        events_tx,
        closing: None,
        finished: false,
        epochs: 0,
    };
    let r = conn.serve(&mut rd, &mut wr, inb, events, close).await;
    conn.teardown().await;
    let _ = wr.shutdown().await;
    r
}

struct ConfirmDone {
    ch: u16,
    epoch: u64,
    seq: u64,
    result: Result<(), MqError>,
}

#[derive(Debug, Clone, Copy)]
struct Unacked {
    id: DeliveryId,
    consumer: Option<ConsumerId>,
}

#[derive(Debug)]
struct ConsumerState {
    id: ConsumerId,
    no_ack: bool,
    /// Per-consumer prefetch (0: unlimited).
    limit: u32,
    /// Credit granted to the engine and not used yet.
    credit_out: u32,
    unacked: u32,
    /// The direct reply-to consumer: not an engine consumer.
    reply: bool,
}

#[derive(Debug, Default)]
enum Content {
    #[default]
    None,
    Header(BasicPublish),
    Body(BasicPublish, Box<ContentHeader>, BytesMut),
}

#[derive(Debug, Default)]
struct TxBuf {
    publishes: Vec<(Message, bool)>,
    settlements: Vec<(u64, Unacked, Settlement)>,
}

#[derive(Debug, Default)]
struct Channel {
    epoch: u64,
    closing: bool,
    content: Content,
    confirm: bool,
    publish_seq: u64,
    tx: Option<TxBuf>,
    next_tag: u64,
    unacked: BTreeMap<u64, Unacked>,
    consumers: HashMap<String, ConsumerState>,
    /// `basic.qos global = false`: the prefetch of later consumers.
    prefetch: u32,
    /// `basic.qos global = true`: the channel's limit.
    global: u32,
    last_queue: Option<String>,
    /// Deliveries held back by the channel's global prefetch.
    held: std::collections::VecDeque<(String, ConsumerId, Delivery)>,
}

struct Conn<'s> {
    svc: &'s Services,
    cfg: Config,
    key: ConnKey,
    vhost: Arc<dyn Vhost>,
    principal: Arc<Principal>,
    login_user: String,
    info: ConnInfo,
    frame_max: u32,
    channel_max: u16,
    heartbeat: u16,
    caps: ClientCaps,
    channels: HashMap<u16, Channel>,
    consumers: HashMap<ConsumerId, (u16, String)>,
    confirms: FuturesUnordered<BoxFuture<'static, ConfirmDone>>,
    out: BytesMut,
    events_tx: crate::backend::EventSender,
    /// When the connection.close we sent stops waiting for close-ok.
    closing: Option<Instant>,
    finished: bool,
    epochs: u64,
}

type HResult = Result<(), MqError>;

impl Conn<'_> {
    fn cx(&self, ch: u16) -> OpCtx {
        OpCtx {
            principal: Arc::clone(&self.principal),
            conn: self.key,
            channel: ch,
        }
    }

    fn send(&mut self, ch: u16, m: impl Into<Method>) {
        put_method(&mut self.out, ch, &m.into());
    }

    async fn serve<R, W>(
        &mut self,
        rd: &mut ReadHalf<R>,
        wr: &mut WriteHalf<W>,
        mut inb: BytesMut,
        mut events: mpsc::UnboundedReceiver<Event>,
        close: CancellationToken,
    ) -> Result<(), MqError>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
        ReadHalf<R>: AsyncRead + Unpin,
        WriteHalf<W>: AsyncWrite + Unpin,
    {
        let hb = Duration::from_secs(u64::from(self.heartbeat));
        let mut ticker = tokio::time::interval(if self.heartbeat > 0 {
            hb / 2
        } else {
            Duration::from_secs(3600)
        });
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_recv = Instant::now();
        let mut last_send = Instant::now();
        loop {
            if !self.out.is_empty() {
                flush(wr, &mut self.out).await?;
                last_send = Instant::now();
            }
            if self.finished {
                return Ok(());
            }
            let close_at = self.closing;
            tokio::select! {
                () = close.cancelled(), if self.closing.is_none() => {
                    self.close_connection(&MqError::new(
                        MqCode::FrameError,
                        "broker forced connection closure with reason 'shutdown'",
                    ).with_reply(reply::CONNECTION_FORCED), 0, 0);
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
                        let f = match Frame::parse(&mut inb, self.frame_max) {
                            Ok(Some(f)) => f,
                            Ok(None) => break,
                            Err(e) => {
                                self.close_connection(&e, 0, 0);
                                inb.clear();
                                break;
                            }
                        };
                        if let Err(e) = self.on_frame(f).await {
                            let (c, m) = e.1;
                            self.close_connection(&e.0, c, m);
                        }
                        if self.finished {
                            break;
                        }
                    }
                }
                Some(ev) = events.recv() => self.on_event(ev).await,
                Some(c) = self.confirms.next(), if !self.confirms.is_empty() => self.on_confirm(c),
                _ = ticker.tick(), if self.heartbeat > 0 => {
                    if last_recv.elapsed() > hb * 2 {
                        tracing::info!(peer = %self.info.peer, "AMQP heartbeat timeout: no frames for two intervals");
                        return Err(MqError::frame(reply::CONNECTION_FORCED, "missed heartbeats from client"));
                    }
                    if last_send.elapsed() >= hb / 2 {
                        put_heartbeat(&mut self.out);
                    }
                }
            }
        }
    }

    /// Sends `connection.close` and waits for close-ok.
    fn close_connection(&mut self, e: &MqError, class_id: u16, method_id: u16) {
        if self.closing.is_some() {
            return;
        }
        tracing::debug!(peer = %self.info.peer, error = %e, "AMQP connection error");
        self.send(
            0,
            ConnectionClose {
                reply_code: e.reply,
                reply_text: e.reply_text(),
                class_id,
                method_id,
            },
        );
        self.closing = Some(Instant::now() + CLOSE_WAIT);
    }

    /// Handles one frame; `Err` closes the connection with the error and
    /// the offending method's ids.
    async fn on_frame(&mut self, f: Frame) -> Result<(), (MqError, (u16, u16))> {
        match f.kind {
            FRAME_HEARTBEAT if f.channel == 0 => Ok(()),
            FRAME_HEARTBEAT => Err((
                MqError::frame(
                    reply::FRAME_ERROR,
                    format!("heartbeat frame on channel {}", f.channel),
                ),
                (0, 0),
            )),
            FRAME_METHOD => {
                let m = Method::decode(&f.payload).map_err(|e| (method_error(e), (0, 0)))?;
                let ids = m.id();
                if self.closing.is_some() {
                    match m {
                        Method::ConnectionCloseOk(_) => self.finished = true,
                        Method::ConnectionClose(_) => {
                            self.send(0, ConnectionCloseOk {});
                            self.finished = true;
                        }
                        _ => {}
                    }
                    return Ok(());
                }
                if f.channel == 0 {
                    self.on_connection_method(m).await.map_err(|e| (e, ids))
                } else {
                    self.on_channel_method(f.channel, m)
                        .await
                        .map_err(|e| (e, ids))
                }
            }
            FRAME_HEADER | FRAME_BODY => {
                if self.closing.is_some() {
                    return Ok(());
                }
                self.on_content(f).await.map_err(|e| (e, (60, 40)))
            }
            other => Err((
                MqError::frame(reply::FRAME_ERROR, format!("unknown frame type {other}")),
                (0, 0),
            )),
        }
    }

    async fn on_connection_method(&mut self, m: Method) -> HResult {
        match m {
            Method::ConnectionClose(c) => {
                tracing::debug!(peer = %self.info.peer, code = c.reply_code, text = %c.reply_text, "client closed the connection");
                self.send(0, ConnectionCloseOk {});
                self.finished = true;
                Ok(())
            }
            Method::ConnectionCloseOk(_)
            | Method::ConnectionBlocked(_)
            | Method::ConnectionUnblocked(_) => Ok(()),
            Method::ConnectionUpdateSecret(u) => {
                let creds = Credentials::Password {
                    mechanism: "PLAIN",
                    user: self.login_user.clone(),
                    password: u.new_secret.to_vec(),
                };
                match self.svc.identity.login(&self.info, creds).await {
                    Ok(p)
                        if p.user == self.principal.user
                            && p.tenant_id == self.principal.tenant_id =>
                    {
                        self.principal = Arc::new(p);
                        self.send(0, ConnectionUpdateSecretOk {});
                        Ok(())
                    }
                    _ => Err(MqError::access_refused("new secret was refused")
                        .with_reply(reply::NOT_ALLOWED)),
                }
            }
            other => Err(MqError::frame(
                reply::COMMAND_INVALID,
                format!("{} is not allowed on channel 0", other.name()),
            )),
        }
    }

    async fn on_channel_method(&mut self, ch: u16, m: Method) -> HResult {
        if self.channel_max != 0 && ch > self.channel_max {
            return Err(MqError::new(
                MqCode::ChannelLimit,
                format!(
                    "channel {ch} is above the negotiated channel_max {}",
                    self.channel_max
                ),
            ));
        }
        if m.id().0 == 10 || !m.client_may_send() {
            return Err(MqError::frame(
                reply::COMMAND_INVALID,
                format!("{} is not allowed here", m.name()),
            ));
        }
        let Some(c) = self.channels.get(&ch) else {
            return match m {
                Method::ChannelOpen(_) => {
                    self.epochs += 1;
                    self.channels.insert(
                        ch,
                        Channel {
                            epoch: self.epochs,
                            ..Channel::default()
                        },
                    );
                    metrics::gauge!("sprig_amqp_channels").increment(1.0);
                    self.send(ch, ChannelOpenOk::default());
                    Ok(())
                }
                Method::ChannelCloseOk(_) => Ok(()),
                _ => Err(MqError::frame(
                    reply::CHANNEL_ERROR,
                    format!("expected 'channel.open' on channel {ch}"),
                )),
            };
        };
        if c.closing {
            match m {
                Method::ChannelCloseOk(_) => self.remove_channel(ch).await,
                Method::ChannelClose(_) => {
                    self.send(ch, ChannelCloseOk {});
                    self.remove_channel(ch).await;
                }
                _ => {}
            }
            return Ok(());
        }
        if matches!(m, Method::ChannelOpen(_)) {
            return Err(MqError::frame(
                reply::CHANNEL_ERROR,
                format!("second 'channel.open' seen on channel {ch}"),
            ));
        }
        if !matches!(c.content, Content::None) {
            return Err(MqError::frame(
                reply::UNEXPECTED_FRAME,
                format!(
                    "expected content header for class 60, got non content header frame instead ({})",
                    m.name()
                ),
            ));
        }
        let ids = m.id();
        match self.on_method(ch, m).await {
            Ok(()) => Ok(()),
            Err(e) if e.is_hard() => Err(e),
            Err(e) => {
                self.close_channel(ch, &e, ids).await;
                Ok(())
            }
        }
    }

    /// Closes a channel from the server's side.
    async fn close_channel(&mut self, ch: u16, e: &MqError, (class_id, method_id): (u16, u16)) {
        tracing::debug!(peer = %self.info.peer, channel = ch, error = %e, "AMQP channel error");
        self.send(
            ch,
            ChannelClose {
                reply_code: e.reply,
                reply_text: e.reply_text(),
                class_id,
                method_id,
            },
        );
        self.release_channel(ch).await;
        if let Some(c) = self.channels.get_mut(&ch) {
            c.closing = true;
            c.content = Content::None;
        }
    }

    /// Ends a channel's consumers and requeues its unacknowledged
    /// deliveries; the channel itself stays until close-ok.
    async fn release_channel(&mut self, ch: u16) {
        let cx = self.cx(ch);
        let Some(c) = self.channels.get_mut(&ch) else {
            return;
        };
        let consumers: Vec<ConsumerState> = c.consumers.drain().map(|(_, s)| s).collect();
        let mut settle: Vec<(DeliveryId, Settlement)> = c
            .unacked
            .values()
            .filter(|u| u.id != DeliveryId::default() || u.consumer.is_some())
            .map(|u| (u.id, Settlement::Requeue { failed: true }))
            .collect();
        c.unacked.clear();
        settle.extend(
            c.held
                .drain(..)
                .map(|(_, _, d)| (d.id, Settlement::Requeue { failed: false })),
        );
        if let Some(tx) = c.tx.as_mut() {
            settle.extend(
                tx.settlements
                    .drain(..)
                    .map(|(_, u, _)| (u.id, Settlement::Requeue { failed: true })),
            );
            tx.publishes.clear();
        }
        for s in consumers {
            self.consumers.remove(&s.id);
            if s.reply {
                self.svc.replies.unregister(self.key, ch);
            } else {
                // Cancel before requeueing, so the requeued messages
                // don't go back to these consumers.
                self.vhost.cancel(&cx, s.id).await;
            }
        }
        settle.retain(|(id, _)| *id != DeliveryId::default());
        if !settle.is_empty() {
            self.vhost.settle(&cx, settle);
        }
    }

    async fn remove_channel(&mut self, ch: u16) {
        self.release_channel(ch).await;
        if self.channels.remove(&ch).is_some() {
            metrics::gauge!("sprig_amqp_channels").decrement(1.0);
        }
    }

    /// The queue a method names; empty means the channel's last declared
    /// queue.
    fn queue_name(&self, ch: u16, q: String) -> Result<String, MqError> {
        if !q.is_empty() {
            return Ok(q);
        }
        self.channels
            .get(&ch)
            .and_then(|c| c.last_queue.clone())
            .ok_or_else(|| MqError::not_found("no previously declared queue"))
    }

    fn chan(&mut self, ch: u16) -> &mut Channel {
        self.channels.entry(ch).or_default()
    }

    #[allow(clippy::too_many_lines)]
    async fn on_method(&mut self, ch: u16, m: Method) -> HResult {
        let cx = self.cx(ch);
        match m {
            Method::ChannelClose(_) => {
                self.send(ch, ChannelCloseOk {});
                self.remove_channel(ch).await;
            }
            Method::ChannelFlow(f) => {
                if !f.active {
                    return Err(MqError::not_implemented("active=false"));
                }
                self.send(ch, ChannelFlowOk { active: true });
            }
            Method::ChannelFlowOk(_) | Method::ChannelCloseOk(_) => {}
            Method::ExchangeDeclare(d) => {
                let no_wait = d.no_wait;
                self.vhost
                    .declare_exchange(
                        &cx,
                        ExchangeSpec {
                            name: d.exchange,
                            kind: d.kind,
                            durable: d.durable,
                            auto_delete: d.auto_delete,
                            internal: d.internal,
                            arguments: d.arguments,
                        },
                        d.passive,
                    )
                    .await?;
                if !no_wait {
                    self.send(ch, ExchangeDeclareOk {});
                }
            }
            Method::ExchangeDelete(d) => {
                self.vhost
                    .delete_exchange(&cx, &d.exchange, d.if_unused)
                    .await?;
                if !d.no_wait {
                    self.send(ch, ExchangeDeleteOk {});
                }
            }
            Method::ExchangeBind(b) => {
                self.vhost
                    .bind(
                        &cx,
                        BindingSpec {
                            source: b.source,
                            destination: b.destination,
                            dest_kind: DestKind::Exchange,
                            key: b.routing_key,
                            arguments: b.arguments,
                        },
                    )
                    .await?;
                if !b.no_wait {
                    self.send(ch, ExchangeBindOk {});
                }
            }
            Method::ExchangeUnbind(b) => {
                self.vhost
                    .unbind(
                        &cx,
                        BindingSpec {
                            source: b.source,
                            destination: b.destination,
                            dest_kind: DestKind::Exchange,
                            key: b.routing_key,
                            arguments: b.arguments,
                        },
                    )
                    .await?;
                if !b.no_wait {
                    self.send(ch, ExchangeUnbindOk {});
                }
            }
            Method::QueueDeclare(d) => {
                let declared = if d.queue == PSEUDO_QUEUE && d.passive {
                    crate::backend::QueueDeclared {
                        name: d.queue.clone(),
                        message_count: 0,
                        consumer_count: 0,
                    }
                } else if d.queue.starts_with(REPLY_PREFIX) {
                    if !d.passive || !self.svc.replies.alive(&d.queue, self.principal.tenant_id) {
                        return Err(MqError::not_found(format!(
                            "no queue '{}' in vhost '{}'",
                            d.queue,
                            self.vhost.name()
                        )));
                    }
                    crate::backend::QueueDeclared {
                        name: d.queue.clone(),
                        message_count: 0,
                        consumer_count: 1,
                    }
                } else {
                    let name = if d.queue.is_empty() && d.passive {
                        self.queue_name(ch, String::new())?
                    } else {
                        d.queue
                    };
                    self.vhost
                        .declare_queue(
                            &cx,
                            QueueSpec {
                                name,
                                durable: d.durable,
                                exclusive: d.exclusive,
                                auto_delete: d.auto_delete,
                                arguments: d.arguments,
                            },
                            d.passive,
                        )
                        .await?
                };
                self.chan(ch).last_queue = Some(declared.name.clone());
                if !d.no_wait {
                    self.send(
                        ch,
                        QueueDeclareOk {
                            queue: declared.name,
                            message_count: declared.message_count,
                            consumer_count: declared.consumer_count,
                        },
                    );
                }
            }
            Method::QueueBind(b) => {
                let q = self.queue_name(ch, b.queue)?;
                self.vhost
                    .bind(
                        &cx,
                        BindingSpec {
                            source: b.exchange,
                            destination: q,
                            dest_kind: DestKind::Queue,
                            key: b.routing_key,
                            arguments: b.arguments,
                        },
                    )
                    .await?;
                if !b.no_wait {
                    self.send(ch, QueueBindOk {});
                }
            }
            Method::QueueUnbind(b) => {
                let q = self.queue_name(ch, b.queue)?;
                self.vhost
                    .unbind(
                        &cx,
                        BindingSpec {
                            source: b.exchange,
                            destination: q,
                            dest_kind: DestKind::Queue,
                            key: b.routing_key,
                            arguments: b.arguments,
                        },
                    )
                    .await?;
                self.send(ch, QueueUnbindOk {});
            }
            Method::QueuePurge(p) => {
                let q = self.queue_name(ch, p.queue)?;
                let n = self.vhost.purge_queue(&cx, &q).await?;
                if !p.no_wait {
                    self.send(ch, QueuePurgeOk { message_count: n });
                }
            }
            Method::QueueDelete(d) => {
                let q = self.queue_name(ch, d.queue)?;
                let n = self
                    .vhost
                    .delete_queue(&cx, &q, d.if_unused, d.if_empty)
                    .await?;
                if !d.no_wait {
                    self.send(ch, QueueDeleteOk { message_count: n });
                }
            }
            Method::BasicQos(q) => {
                if q.prefetch_size != 0 {
                    return Err(MqError::not_implemented(format!(
                        "prefetch_size!=0 ({})",
                        q.prefetch_size
                    )));
                }
                let c = self.chan(ch);
                if q.global {
                    c.global = u32::from(q.prefetch_count);
                } else {
                    c.prefetch = u32::from(q.prefetch_count);
                }
                self.send(ch, BasicQosOk {});
                self.regrant(ch);
            }
            Method::BasicConsume(c) => self.consume(ch, c).await?,
            Method::BasicCancel(c) => {
                let state = self.chan(ch).consumers.remove(&c.consumer_tag);
                if let Some(s) = state {
                    self.consumers.remove(&s.id);
                    if s.reply {
                        self.svc.replies.unregister(self.key, ch);
                    } else {
                        self.vhost.cancel(&cx, s.id).await;
                    }
                }
                if !c.no_wait {
                    self.send(
                        ch,
                        BasicCancelOk {
                            consumer_tag: c.consumer_tag,
                        },
                    );
                }
            }
            Method::BasicPublish(p) => {
                if p.immediate {
                    return Err(MqError::not_implemented("immediate=true"));
                }
                self.chan(ch).content = Content::Header(p);
            }
            Method::BasicGet(g) => {
                let q = self.queue_name(ch, g.queue)?;
                match self.vhost.get(&cx, &q, g.no_ack).await? {
                    None => self.send(ch, BasicGetEmpty::default()),
                    Some((d, remaining)) => {
                        let (props, body) = convert::to_091(&d)?;
                        let c = self.chan(ch);
                        c.next_tag += 1;
                        let tag = c.next_tag;
                        if !g.no_ack {
                            c.unacked.insert(
                                tag,
                                Unacked {
                                    id: d.id,
                                    consumer: None,
                                },
                            );
                        }
                        let m: Method = BasicGetOk {
                            delivery_tag: tag,
                            redelivered: d.redelivered,
                            exchange: d.message.exchange.clone(),
                            routing_key: d.message.routing_key.clone(),
                            message_count: remaining,
                        }
                        .into();
                        put_content(&mut self.out, ch, &m, &props, &body, self.frame_max);
                    }
                }
            }
            Method::BasicAck(a) => {
                let taken = self.take_tags(ch, a.delivery_tag, a.multiple)?;
                self.settle(ch, taken, Settlement::Ack);
            }
            Method::BasicNack(n) => {
                let taken = self.take_tags(ch, n.delivery_tag, n.multiple)?;
                let s = if n.requeue {
                    Settlement::Requeue { failed: false }
                } else {
                    Settlement::DeadLetter(DeadLetterReason::Rejected)
                };
                self.settle(ch, taken, s);
            }
            Method::BasicReject(r) => {
                let taken = self.take_tags(ch, r.delivery_tag, false)?;
                let s = if r.requeue {
                    Settlement::Requeue { failed: true }
                } else {
                    Settlement::DeadLetter(DeadLetterReason::Rejected)
                };
                self.settle(ch, taken, s);
            }
            Method::BasicRecover(r) => {
                if !r.requeue {
                    return Err(MqError::not_implemented("requeue=false"));
                }
                let taken = self.take_tags(ch, 0, true)?;
                self.settle(ch, taken, Settlement::Requeue { failed: false });
                self.send(ch, BasicRecoverOk {});
            }
            Method::BasicRecoverAsync(r) => {
                if !r.requeue {
                    return Err(MqError::not_implemented("requeue=false"));
                }
                let taken = self.take_tags(ch, 0, true)?;
                self.settle(ch, taken, Settlement::Requeue { failed: false });
            }
            Method::ConfirmSelect(s) => {
                let c = self.chan(ch);
                if c.tx.is_some() {
                    return Err(MqError::precondition(
                        "cannot switch from tx to confirm mode",
                    ));
                }
                c.confirm = true;
                if !s.no_wait {
                    self.send(ch, ConfirmSelectOk {});
                }
            }
            Method::TxSelect(_) => {
                let c = self.chan(ch);
                if c.confirm {
                    return Err(MqError::precondition(
                        "cannot switch from confirm to tx mode",
                    ));
                }
                if c.tx.is_none() {
                    c.tx = Some(TxBuf::default());
                }
                self.send(ch, TxSelectOk {});
            }
            Method::TxCommit(_) => self.tx_commit(ch).await?,
            Method::TxRollback(_) => {
                let c = self.chan(ch);
                let Some(tx) = c.tx.as_mut() else {
                    return Err(MqError::precondition("channel is not transactional"));
                };
                let undo: Vec<(u64, Unacked)> =
                    tx.settlements.drain(..).map(|(t, u, _)| (t, u)).collect();
                tx.publishes.clear();
                for (t, u) in undo {
                    c.unacked.insert(t, u);
                    if let Some(cid) = u.consumer
                        && let Some(s) = c.consumers.values_mut().find(|s| s.id == cid)
                    {
                        s.unacked += 1;
                    }
                }
                self.send(ch, TxRollbackOk {});
            }
            other => {
                return Err(MqError::frame(
                    reply::COMMAND_INVALID,
                    format!("{} is not allowed here", other.name()),
                ));
            }
        }
        Ok(())
    }

    async fn consume(&mut self, ch: u16, c: BasicConsume) -> HResult {
        let queue = self.queue_name(ch, c.queue)?;
        let tag = if c.consumer_tag.is_empty() {
            format!("amq.ctag-{}", random_suffix())
        } else {
            c.consumer_tag
        };
        if self.chan(ch).consumers.contains_key(&tag) {
            return Err(MqError::frame(
                reply::NOT_ALLOWED,
                format!("attempt to reuse consumer tag '{tag}'"),
            ));
        }
        let id = ConsumerId::next();
        let queue_name = queue.clone();
        if queue == PSEUDO_QUEUE {
            if !c.no_ack {
                return Err(MqError::new(
                    MqCode::DirectReplyTo,
                    "reply consumer cannot acknowledge",
                ));
            }
            if self.chan(ch).consumers.values().any(|s| s.reply) {
                return Err(MqError::new(
                    MqCode::DirectReplyTo,
                    "reply consumer already set",
                ));
            }
            self.svc.replies.register(
                self.key,
                ch,
                id,
                self.events_tx.clone(),
                self.principal.tenant_id,
            );
        } else {
            let priority = c
                .arguments
                .get("x-priority")
                .and_then(FieldValue::as_i64)
                .and_then(|p| i32::try_from(p).ok())
                .unwrap_or(0);
            self.vhost
                .consume(
                    &self.cx(ch),
                    ConsumeSpec {
                        consumer: id,
                        queue,
                        tag: tag.clone(),
                        no_ack: c.no_ack,
                        exclusive: c.exclusive,
                        priority,
                        arguments: c.arguments,
                    },
                )
                .await?;
        }
        let limit = self.chan(ch).prefetch;
        self.chan(ch).consumers.insert(
            tag.clone(),
            ConsumerState {
                id,
                no_ack: c.no_ack,
                limit,
                credit_out: 0,
                unacked: 0,
                reply: queue_name == PSEUDO_QUEUE,
            },
        );
        self.consumers.insert(id, (ch, tag.clone()));
        if !c.no_wait {
            self.send(ch, BasicConsumeOk { consumer_tag: tag });
        }
        self.regrant(ch);
        Ok(())
    }

    /// Content frames for a pending `basic.publish`.
    async fn on_content(&mut self, f: Frame) -> HResult {
        let ch = f.channel;
        let max = self.cfg.max_message_bytes;
        let Some(c) = self.channels.get_mut(&ch) else {
            return Err(MqError::frame(
                reply::CHANNEL_ERROR,
                format!("content frame on unopened channel {ch}"),
            ));
        };
        if c.closing {
            return Ok(());
        }
        let content = std::mem::take(&mut c.content);
        let done = match (content, f.kind) {
            (Content::Header(p), FRAME_HEADER) => {
                let h = ContentHeader::decode(&f.payload)?;
                if h.class_id != 60 {
                    return Err(MqError::frame(
                        reply::UNEXPECTED_FRAME,
                        format!(
                            "content header for class {} after basic.publish",
                            h.class_id
                        ),
                    ));
                }
                if h.body_size > max {
                    let e = MqError::new(
                        MqCode::MessageTooLarge,
                        format!(
                            "message size {} is larger than configured max size {max}",
                            h.body_size
                        ),
                    );
                    self.close_channel(ch, &e, (60, 40)).await;
                    return Ok(());
                }
                if h.body_size == 0 {
                    Some((p, h, Bytes::new()))
                } else {
                    let cap = usize::try_from(h.body_size)
                        .unwrap_or(usize::MAX)
                        .min(1 << 20);
                    c.content = Content::Body(p, Box::new(h), BytesMut::with_capacity(cap));
                    None
                }
            }
            (Content::Body(p, h, mut body), FRAME_BODY) => {
                body.extend_from_slice(&f.payload);
                let have = body.len() as u64;
                if have > h.body_size {
                    return Err(MqError::frame(
                        reply::FRAME_ERROR,
                        format!("body of {have} bytes exceeds the announced {}", h.body_size),
                    ));
                }
                if have == h.body_size {
                    Some((p, *h, body.freeze()))
                } else {
                    c.content = Content::Body(p, h, body);
                    None
                }
            }
            (_, kind) => {
                return Err(MqError::frame(
                    reply::UNEXPECTED_FRAME,
                    format!("unexpected content frame of type {kind} on channel {ch}"),
                ));
            }
        };
        if let Some((p, h, body)) = done {
            match self.publish(ch, &p, &h, body) {
                Ok(()) => {}
                Err(e) if e.is_hard() => return Err(e),
                Err(e) => self.close_channel(ch, &e, (60, 40)).await,
            }
        }
        Ok(())
    }

    fn publish(&mut self, ch: u16, p: &BasicPublish, h: &ContentHeader, body: Bytes) -> HResult {
        let mut msg = convert::from_091(
            p.exchange.clone(),
            p.routing_key.clone(),
            h.raw_props.clone(),
            &h.props,
            body,
        )?;
        if h.props.reply_to.as_deref() == Some(PSEUDO_QUEUE) {
            let Some((_, name)) = self.svc.replies.lookup(self.key, ch) else {
                return Err(MqError::new(
                    MqCode::DirectReplyTo,
                    "fast reply consumer does not exist",
                ));
            };
            msg = convert::with_reply_to(&msg, &name)?;
        }
        let cx = self.cx(ch);
        let c = self.chan(ch);
        if let Some(tx) = c.tx.as_mut() {
            tx.publishes.push((msg, p.mandatory));
            return Ok(());
        }
        let published = if msg.exchange.is_empty() && msg.routing_key.starts_with(REPLY_PREFIX) {
            let rk = msg.routing_key.clone();
            match self
                .svc
                .replies
                .deliver(&rk, msg.clone(), self.principal.tenant_id)
            {
                Some(_) => crate::backend::Published::done(1),
                None => crate::backend::Published::done(0),
            }
        } else {
            self.vhost.publish(&cx, msg.clone(), p.mandatory)?
        };
        if published.routed == 0 && p.mandatory {
            self.send_return(ch, &msg)?;
        }
        let c = self.chan(ch);
        if c.confirm {
            c.publish_seq += 1;
            let (seq, epoch) = (c.publish_seq, c.epoch);
            let confirm = published.confirm;
            self.confirms.push(Box::pin(async move {
                ConfirmDone {
                    ch,
                    epoch,
                    seq,
                    result: confirm.await,
                }
            }));
        }
        Ok(())
    }

    fn send_return(&mut self, ch: u16, msg: &Message) -> HResult {
        let d = Delivery {
            id: DeliveryId::default(),
            queue: String::new(),
            message: Arc::new(msg.clone()),
            redelivered: false,
            delivery_count: 0,
            annotations: FieldTable::new(),
        };
        let (props, body) = convert::to_091(&d)?;
        let m: Method = BasicReturn {
            reply_code: reply::NO_ROUTE,
            reply_text: "NO_ROUTE".into(),
            exchange: msg.exchange.clone(),
            routing_key: msg.routing_key.clone(),
        }
        .into();
        put_content(&mut self.out, ch, &m, &props, &body, self.frame_max);
        Ok(())
    }

    /// Removes acknowledged tags from the channel.
    fn take_tags(
        &mut self,
        ch: u16,
        tag: u64,
        multiple: bool,
    ) -> Result<Vec<(u64, Unacked)>, MqError> {
        let c = self.chan(ch);
        let taken: Vec<(u64, Unacked)> = if multiple {
            let keys: Vec<u64> = c
                .unacked
                .keys()
                .copied()
                .filter(|t| tag == 0 || *t <= tag)
                .collect();
            keys.into_iter()
                .filter_map(|t| c.unacked.remove(&t).map(|u| (t, u)))
                .collect()
        } else {
            c.unacked
                .remove(&tag)
                .map(|u| vec![(tag, u)])
                .unwrap_or_default()
        };
        if taken.is_empty() && (tag != 0 || !multiple) {
            return Err(MqError::precondition(format!("unknown delivery tag {tag}")));
        }
        for (_, u) in &taken {
            if let Some(cid) = u.consumer
                && let Some(s) = c.consumers.values_mut().find(|s| s.id == cid)
            {
                s.unacked = s.unacked.saturating_sub(1);
            }
        }
        Ok(taken)
    }

    fn settle(&mut self, ch: u16, taken: Vec<(u64, Unacked)>, s: Settlement) {
        let cx = self.cx(ch);
        let c = self.chan(ch);
        if let Some(tx) = c.tx.as_mut() {
            tx.settlements
                .extend(taken.into_iter().map(|(t, u)| (t, u, s)));
            return;
        }
        let batch: Vec<(DeliveryId, Settlement)> =
            taken.into_iter().map(|(_, u)| (u.id, s)).collect();
        if !batch.is_empty() {
            self.vhost.settle(&cx, batch);
        }
        self.release_held(ch);
        self.regrant(ch);
    }

    /// Delivers held messages while the channel's window has room.
    fn release_held(&mut self, ch: u16) {
        loop {
            let Some(c) = self.channels.get_mut(&ch) else {
                return;
            };
            if c.global > 0 && c.unacked.len() >= c.global as usize {
                return;
            }
            let Some((tag, consumer, d)) = c.held.pop_front() else {
                return;
            };
            self.write_delivery(ch, tag, consumer, &d);
        }
    }

    /// Grants engine credit so each consumer's unacknowledged deliveries
    /// plus outstanding credit stay within its prefetch and the channel's
    /// global limit.
    fn regrant(&mut self, ch: u16) {
        let max = self.cfg.max_unacked_per_consumer.max(1);
        let vhost = Arc::clone(&self.vhost);
        let Some(c) = self.channels.get_mut(&ch) else {
            return;
        };
        if c.closing {
            return;
        }
        let mut tags: Vec<&String> = c.consumers.keys().collect();
        tags.sort();
        let tags: Vec<String> = tags.into_iter().cloned().collect();
        for t in tags {
            let Some(s) = c.consumers.get_mut(&t) else {
                continue;
            };
            if s.reply {
                continue;
            }
            let mut cap = if s.no_ack || s.limit == 0 {
                max
            } else {
                s.limit.min(max)
            };
            if !s.no_ack && c.global > 0 {
                // Each consumer may hold up to the channel's window; the
                // window itself is enforced at delivery (held messages
                // wait for acks), so idle credit on one queue can't
                // starve another.
                cap = cap.min(c.global);
            }
            let held = u32::try_from(c.held.iter().filter(|(_, id, _)| *id == s.id).count())
                .unwrap_or(u32::MAX);
            let have = if s.no_ack {
                s.credit_out
            } else {
                s.unacked + s.credit_out + held
            };
            let want = cap.saturating_sub(have);
            if want == 0 {
                continue;
            }
            if want >= (cap / 4).max(1) || s.credit_out == 0 {
                vhost.credit(s.id, want, false);
                s.credit_out += want;
            }
        }
    }

    async fn tx_commit(&mut self, ch: u16) -> HResult {
        let cx = self.cx(ch);
        let c = self.chan(ch);
        let Some(tx) = c.tx.as_mut() else {
            return Err(MqError::precondition("channel is not transactional"));
        };
        let publishes = std::mem::take(&mut tx.publishes);
        let settlements = std::mem::take(&mut tx.settlements);
        let batch = TxBatch {
            publishes: publishes.clone(),
            settlements: settlements.iter().map(|(_, u, s)| (u.id, *s)).collect(),
        };
        let routed = self.vhost.commit_tx(&cx, batch).await?;
        for ((m, mandatory), n) in publishes.iter().zip(routed) {
            if n == 0 && *mandatory {
                self.send_return(ch, m)?;
            }
        }
        self.send(ch, TxCommitOk {});
        self.regrant(ch);
        Ok(())
    }

    fn on_confirm(&mut self, c: ConfirmDone) {
        let Some(chan) = self.channels.get(&c.ch) else {
            return;
        };
        if chan.epoch != c.epoch || chan.closing {
            return;
        }
        match c.result {
            Ok(()) => self.send(
                c.ch,
                BasicAck {
                    delivery_tag: c.seq,
                    multiple: false,
                },
            ),
            Err(e) => {
                tracing::debug!(error = %e, "publish nacked");
                self.send(
                    c.ch,
                    BasicNack {
                        delivery_tag: c.seq,
                        multiple: false,
                        requeue: false,
                    },
                );
            }
        }
    }

    async fn on_event(&mut self, ev: Event) {
        match ev {
            Event::Deliver { consumer, delivery } => self.on_deliver(consumer, delivery),
            Event::Cancelled { consumer, reason } => {
                let Some((ch, tag)) = self.consumers.remove(&consumer) else {
                    return;
                };
                if let Some(c) = self.channels.get_mut(&ch) {
                    c.consumers.remove(&tag);
                }
                if self.closing.is_some() {
                    return;
                }
                if self.caps.cancel_notify {
                    self.send(
                        ch,
                        BasicCancel {
                            consumer_tag: tag,
                            no_wait: true,
                        },
                    );
                } else {
                    self.close_channel(ch, &reason, (60, 20)).await;
                }
            }
            Event::Drained { .. } => {}
            Event::Blocked(reason) => {
                if self.caps.blocked {
                    self.send(0, ConnectionBlocked { reason });
                }
            }
            Event::Unblocked => {
                if self.caps.blocked {
                    self.send(0, ConnectionUnblocked {});
                }
            }
        }
    }

    fn on_deliver(&mut self, consumer: ConsumerId, d: Delivery) {
        let requeue = |s: &Self, d: &Delivery| {
            if d.id != DeliveryId::default() {
                s.vhost.settle(
                    &s.cx(0),
                    vec![(d.id, Settlement::Requeue { failed: false })],
                );
            }
        };
        let Some((ch, tag)) = self.consumers.get(&consumer).cloned() else {
            requeue(self, &d);
            return;
        };
        if self.closing.is_some() || self.channels.get(&ch).is_none_or(|c| c.closing) {
            requeue(self, &d);
            return;
        }
        let (props, body) = match convert::to_091(&d) {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!(error = %e, queue = %d.queue, "undeliverable stored message dead-lettered");
                self.vhost.settle(
                    &self.cx(ch),
                    vec![(d.id, Settlement::DeadLetter(DeadLetterReason::Rejected))],
                );
                return;
            }
        };
        let c = self.chan(ch);
        let mut manual = false;
        if let Some(s) = c.consumers.get_mut(&tag) {
            s.credit_out = s.credit_out.saturating_sub(1);
            manual = !s.no_ack;
        }
        if manual && c.global > 0 && (c.unacked.len() >= c.global as usize || !c.held.is_empty()) {
            c.held.push_back((tag, consumer, d));
            return;
        }
        self.write_delivery_content(ch, tag, consumer, &d, &props, &body);
        self.regrant(ch);
    }

    fn write_delivery(&mut self, ch: u16, tag: String, consumer: ConsumerId, d: &Delivery) {
        match convert::to_091(d) {
            Ok((props, body)) => self.write_delivery_content(ch, tag, consumer, d, &props, &body),
            Err(e) => {
                tracing::warn!(error = %e, queue = %d.queue, "undeliverable stored message dead-lettered");
                self.vhost.settle(
                    &self.cx(ch),
                    vec![(d.id, Settlement::DeadLetter(DeadLetterReason::Rejected))],
                );
            }
        }
    }

    fn write_delivery_content(
        &mut self,
        ch: u16,
        tag: String,
        consumer: ConsumerId,
        d: &Delivery,
        props: &[u8],
        body: &[u8],
    ) {
        let c = self.chan(ch);
        c.next_tag += 1;
        let delivery_tag = c.next_tag;
        if let Some(s) = c.consumers.get_mut(&tag)
            && !s.no_ack
        {
            s.unacked += 1;
            c.unacked.insert(
                delivery_tag,
                Unacked {
                    id: d.id,
                    consumer: Some(consumer),
                },
            );
        }
        let m: Method = BasicDeliver {
            consumer_tag: tag,
            delivery_tag,
            redelivered: d.redelivered,
            exchange: d.message.exchange.clone(),
            routing_key: d.message.routing_key.clone(),
        }
        .into();
        put_content(&mut self.out, ch, &m, props, body, self.frame_max);
    }

    /// The connection ended: every channel's consumers end and their
    /// deliveries are requeued; exclusive queues go.
    async fn teardown(&mut self) {
        let chans: Vec<u16> = self.channels.keys().copied().collect();
        for ch in chans {
            self.remove_channel(ch).await;
        }
        self.svc.replies.unregister_conn(self.key);
        self.vhost.connection_closed(self.key).await;
    }
}
