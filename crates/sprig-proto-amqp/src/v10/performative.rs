//! AMQP 1.0 composite types: the transport performatives (part 2), the
//! delivery states and terminus types (part 3), the SASL frames (part 5),
//! and framing (part 2.3).

use bytes::{BufMut, Bytes, BytesMut};

use super::types::{DResult, DecodeError, Value};

/// Frame type: AMQP.
pub const FRAME_AMQP: u8 = 0;
/// Frame type: SASL.
pub const FRAME_SASL: u8 = 1;
/// The smallest `max-frame-size` (the spec's `MIN-MAX-FRAME-SIZE`).
pub const MIN_MAX_FRAME_SIZE: u32 = 512;

/// One frame. An AMQP frame with no body is a heartbeat.
#[derive(Debug, Clone, PartialEq)]
pub struct Frame {
    /// [`FRAME_AMQP`] or [`FRAME_SASL`].
    pub kind: u8,
    /// The channel (AMQP frames).
    pub channel: u16,
    /// The performative, `None` for a heartbeat.
    pub body: Option<Value>,
    /// Bytes after the performative (a transfer's message data).
    pub payload: Bytes,
}

/// A framing error (`amqp:connection:framing-error`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct FramingError(pub String);

impl Frame {
    /// Takes one whole frame from the front of `buf`. `max` bounds the
    /// frame size (0: none).
    ///
    /// # Errors
    /// A frame larger than `max`, a bad data offset, or a malformed
    /// performative.
    pub fn parse(buf: &mut BytesMut, max: u32) -> Result<Option<Frame>, FramingError> {
        if buf.len() < 8 {
            return Ok(None);
        }
        let size = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if size < 8 {
            return Err(FramingError(format!("frame size {size} below 8")));
        }
        if max != 0 && size > max as usize {
            return Err(FramingError(format!(
                "frame size {size} exceeds max-frame-size {max}"
            )));
        }
        let doff = buf[4] as usize * 4;
        if doff < 8 || doff > size {
            return Err(FramingError(format!("bad data offset {}", buf[4])));
        }
        if buf.len() < size {
            buf.reserve(size - buf.len());
            return Ok(None);
        }
        let raw = buf.split_to(size).freeze();
        let kind = raw[5];
        let channel = u16::from_be_bytes([raw[6], raw[7]]);
        let rest = raw.slice(doff..);
        if rest.is_empty() {
            return Ok(Some(Frame {
                kind,
                channel,
                body: None,
                payload: Bytes::new(),
            }));
        }
        let (body, n) = Value::decode(&rest).map_err(|e| FramingError(e.0))?;
        Ok(Some(Frame {
            kind,
            channel,
            body: Some(body),
            payload: rest.slice(n..),
        }))
    }
}

/// Writes a frame.
#[allow(clippy::cast_possible_truncation)]
pub fn put_frame(buf: &mut BytesMut, kind: u8, channel: u16, body: &Value, payload: &[u8]) {
    let at = buf.len();
    buf.put_u32(0);
    buf.put_u8(2);
    buf.put_u8(kind);
    buf.put_u16(channel);
    body.encode(buf);
    buf.put_slice(payload);
    let n = (buf.len() - at) as u32;
    buf[at..at + 4].copy_from_slice(&n.to_be_bytes());
}

/// Writes a heartbeat (an empty AMQP frame).
pub fn put_heartbeat(buf: &mut BytesMut) {
    buf.put_slice(&[0, 0, 0, 8, 2, 0, 0, 0]);
}

/// The fields of a composite, with typed accessors that treat a missing
/// field as null.
struct Fields<'a>(&'a [Value]);

impl Fields<'_> {
    fn get(&self, i: usize) -> &Value {
        self.0.get(i).unwrap_or(&Value::Null)
    }
    fn opt_str(&self, i: usize) -> DResult<Option<String>> {
        match self.get(i) {
            Value::Null => Ok(None),
            v => v
                .as_str()
                .map(|s| Some(s.to_owned()))
                .ok_or_else(|| bad(i, "a string")),
        }
    }
    fn str(&self, i: usize) -> DResult<String> {
        self.opt_str(i)?.ok_or_else(|| bad(i, "a mandatory string"))
    }
    fn opt_u32(&self, i: usize) -> DResult<Option<u32>> {
        match self.get(i) {
            Value::Null => Ok(None),
            v => v
                .as_u64()
                .and_then(|x| u32::try_from(x).ok())
                .map(Some)
                .ok_or_else(|| bad(i, "a uint")),
        }
    }
    fn u32(&self, i: usize) -> DResult<u32> {
        self.opt_u32(i)?.ok_or_else(|| bad(i, "a mandatory uint"))
    }
    fn opt_u64(&self, i: usize) -> DResult<Option<u64>> {
        match self.get(i) {
            Value::Null => Ok(None),
            v => v.as_u64().map(Some).ok_or_else(|| bad(i, "a ulong")),
        }
    }
    fn opt_bool(&self, i: usize) -> DResult<Option<bool>> {
        match self.get(i) {
            Value::Null => Ok(None),
            v => v.as_bool().map(Some).ok_or_else(|| bad(i, "a boolean")),
        }
    }
    fn bool(&self, i: usize, default: bool) -> DResult<bool> {
        Ok(self.opt_bool(i)?.unwrap_or(default))
    }
    fn opt_bin(&self, i: usize) -> DResult<Option<Bytes>> {
        match self.get(i) {
            Value::Null => Ok(None),
            Value::Binary(b) => Ok(Some(b.clone())),
            _ => Err(bad(i, "binary")),
        }
    }
    /// A `multiple` symbol field: one symbol or an array of them.
    fn symbols(&self, i: usize) -> Vec<String> {
        match self.get(i) {
            Value::Symbol(s) => vec![s.clone()],
            Value::Array(a) => a.iter().filter_map(|v| v.as_str().map(str::to_owned)).collect(),
            _ => Vec::new(),
        }
    }
    fn opt(&self, i: usize) -> Option<Value> {
        match self.get(i) {
            Value::Null => None,
            v => Some(v.clone()),
        }
    }
}

fn bad(i: usize, what: &str) -> DecodeError {
    DecodeError(format!("field {i} is not {what}"))
}

fn opt<T>(v: Option<T>, f: impl FnOnce(T) -> Value) -> Value {
    v.map_or(Value::Null, f)
}

fn syms(v: &[String]) -> Value {
    if v.is_empty() {
        Value::Null
    } else {
        Value::Array(v.iter().map(|s| Value::sym(s)).collect())
    }
}

/// An `error`.
#[derive(Debug, Clone, PartialEq)]
pub struct AmqpError {
    /// The condition symbol.
    pub condition: String,
    /// Text.
    pub description: Option<String>,
    /// More information.
    pub info: Option<Value>,
}

impl AmqpError {
    /// An error with a description.
    pub fn new(condition: &str, description: impl Into<String>) -> Self {
        Self {
            condition: condition.to_owned(),
            description: Some(description.into()),
            info: None,
        }
    }

    fn to_value(&self) -> Value {
        Value::composite(
            0x1d,
            vec![
                Value::sym(&self.condition),
                opt(self.description.clone(), Value::String),
                self.info.clone().unwrap_or(Value::Null),
            ],
        )
    }

    fn from_value(v: &Value) -> DResult<Option<Self>> {
        if v.is_null() {
            return Ok(None);
        }
        let f = Fields(v.fields().ok_or_else(|| DecodeError("error is not a list".into()))?);
        Ok(Some(Self {
            condition: f.str(0)?,
            description: f.opt_str(1)?,
            info: f.opt(2),
        }))
    }
}

/// A delivery state or outcome.
#[derive(Debug, Clone, PartialEq)]
pub enum DeliveryState {
    /// `received`.
    Received {
        /// `section-number`.
        section_number: u32,
        /// `section-offset`.
        section_offset: u64,
    },
    /// `accepted`.
    Accepted,
    /// `rejected`.
    Rejected(Option<AmqpError>),
    /// `released`.
    Released,
    /// `modified`.
    Modified {
        /// `delivery-failed`.
        delivery_failed: bool,
        /// `undeliverable-here`.
        undeliverable_here: bool,
        /// `message-annotations`.
        annotations: Option<Value>,
    },
    /// A state this crate doesn't know (transactional states).
    Other(u64),
}

impl DeliveryState {
    /// As a value.
    #[must_use]
    pub fn to_value(&self) -> Value {
        match self {
            DeliveryState::Received {
                section_number,
                section_offset,
            } => Value::composite(
                0x23,
                vec![Value::Uint(*section_number), Value::Ulong(*section_offset)],
            ),
            DeliveryState::Accepted => Value::composite(0x24, vec![]),
            DeliveryState::Rejected(e) => Value::composite(
                0x25,
                vec![e.as_ref().map_or(Value::Null, AmqpError::to_value)],
            ),
            DeliveryState::Released => Value::composite(0x26, vec![]),
            DeliveryState::Modified {
                delivery_failed,
                undeliverable_here,
                annotations,
            } => Value::composite(
                0x27,
                vec![
                    Value::Bool(*delivery_failed),
                    Value::Bool(*undeliverable_here),
                    annotations.clone().unwrap_or(Value::Null),
                ],
            ),
            DeliveryState::Other(c) => Value::composite(*c, vec![]),
        }
    }

    /// From a value; `None` for null.
    ///
    /// # Errors
    /// Not a delivery state.
    pub fn from_value(v: &Value) -> DResult<Option<Self>> {
        if v.is_null() {
            return Ok(None);
        }
        let code = v
            .descriptor()
            .ok_or_else(|| DecodeError("delivery state has no descriptor".into()))?;
        let f = Fields(v.fields().unwrap_or(&[]));
        Ok(Some(match code {
            0x23 => DeliveryState::Received {
                section_number: f.opt_u32(0)?.unwrap_or(0),
                section_offset: f.opt_u64(1)?.unwrap_or(0),
            },
            0x24 => DeliveryState::Accepted,
            0x25 => DeliveryState::Rejected(AmqpError::from_value(f.get(0))?),
            0x26 => DeliveryState::Released,
            0x27 => DeliveryState::Modified {
                delivery_failed: f.bool(0, false)?,
                undeliverable_here: f.bool(1, false)?,
                annotations: f.opt(2),
            },
            other => DeliveryState::Other(other),
        }))
    }
}

/// A `source` terminus.
#[derive(Debug, Clone, Default, PartialEq)]
#[allow(missing_docs)]
pub struct Source {
    pub address: Option<String>,
    pub durable: u32,
    pub expiry_policy: Option<String>,
    pub timeout: u32,
    pub dynamic: bool,
    pub dynamic_node_properties: Option<Value>,
    pub distribution_mode: Option<String>,
    pub filter: Option<Value>,
    pub default_outcome: Option<Value>,
    pub outcomes: Vec<String>,
    pub capabilities: Vec<String>,
}

impl Source {
    /// As a value.
    #[must_use]
    pub fn to_value(&self) -> Value {
        Value::composite(
            0x28,
            vec![
                opt(self.address.clone(), Value::String),
                Value::Uint(self.durable),
                opt(self.expiry_policy.clone(), Value::Symbol),
                Value::Uint(self.timeout),
                Value::Bool(self.dynamic),
                self.dynamic_node_properties.clone().unwrap_or(Value::Null),
                opt(self.distribution_mode.clone(), Value::Symbol),
                self.filter.clone().unwrap_or(Value::Null),
                self.default_outcome.clone().unwrap_or(Value::Null),
                syms(&self.outcomes),
                syms(&self.capabilities),
            ],
        )
    }

    fn from_value(v: &Value) -> DResult<Option<Self>> {
        if v.is_null() {
            return Ok(None);
        }
        let f = Fields(v.fields().ok_or_else(|| DecodeError("source is not a list".into()))?);
        Ok(Some(Self {
            address: f.opt_str(0)?,
            durable: f.opt_u32(1)?.unwrap_or(0),
            expiry_policy: f.opt_str(2)?,
            timeout: f.opt_u32(3)?.unwrap_or(0),
            dynamic: f.bool(4, false)?,
            dynamic_node_properties: f.opt(5),
            distribution_mode: f.opt_str(6)?,
            filter: f.opt(7),
            default_outcome: f.opt(8),
            outcomes: f.symbols(9),
            capabilities: f.symbols(10),
        }))
    }
}

/// A `target` terminus.
#[derive(Debug, Clone, Default, PartialEq)]
#[allow(missing_docs)]
pub struct Target {
    pub address: Option<String>,
    pub durable: u32,
    pub expiry_policy: Option<String>,
    pub timeout: u32,
    pub dynamic: bool,
    pub dynamic_node_properties: Option<Value>,
    pub capabilities: Vec<String>,
}

impl Target {
    /// As a value.
    #[must_use]
    pub fn to_value(&self) -> Value {
        Value::composite(
            0x29,
            vec![
                opt(self.address.clone(), Value::String),
                Value::Uint(self.durable),
                opt(self.expiry_policy.clone(), Value::Symbol),
                Value::Uint(self.timeout),
                Value::Bool(self.dynamic),
                self.dynamic_node_properties.clone().unwrap_or(Value::Null),
                syms(&self.capabilities),
            ],
        )
    }

    fn from_value(v: &Value) -> DResult<Option<Self>> {
        if v.is_null() {
            return Ok(None);
        }
        // A coordinator target (transactions) has descriptor 0x30.
        if v.descriptor() == Some(0x30) {
            return Err(DecodeError("coordinator".into()));
        }
        let f = Fields(v.fields().ok_or_else(|| DecodeError("target is not a list".into()))?);
        Ok(Some(Self {
            address: f.opt_str(0)?,
            durable: f.opt_u32(1)?.unwrap_or(0),
            expiry_policy: f.opt_str(2)?,
            timeout: f.opt_u32(3)?.unwrap_or(0),
            dynamic: f.bool(4, false)?,
            dynamic_node_properties: f.opt(5),
            capabilities: f.symbols(6),
        }))
    }
}

/// Link roles.
pub mod role {
    /// The sender.
    pub const SENDER: bool = false;
    /// The receiver.
    pub const RECEIVER: bool = true;
}

/// `snd-settle-mode` values.
pub mod snd_settle {
    /// Unsettled.
    pub const UNSETTLED: u8 = 0;
    /// Settled (pre-settled, at most once).
    pub const SETTLED: u8 = 1;
    /// Mixed.
    pub const MIXED: u8 = 2;
}

/// A transport or SASL performative.
#[derive(Debug, Clone, PartialEq)]
#[allow(missing_docs)]
pub enum Performative {
    Open {
        container_id: String,
        hostname: Option<String>,
        max_frame_size: u32,
        channel_max: u16,
        idle_timeout: Option<u32>,
        offered_capabilities: Vec<String>,
        desired_capabilities: Vec<String>,
        properties: Option<Value>,
    },
    Begin {
        remote_channel: Option<u16>,
        next_outgoing_id: u32,
        incoming_window: u32,
        outgoing_window: u32,
        handle_max: u32,
        offered_capabilities: Vec<String>,
        desired_capabilities: Vec<String>,
        properties: Option<Value>,
    },
    Attach {
        name: String,
        handle: u32,
        role: bool,
        snd_settle_mode: u8,
        rcv_settle_mode: u8,
        source: Option<Source>,
        target: Option<Target>,
        /// A coordinator target (transactions) was asked for.
        coordinator: bool,
        unsettled: Option<Value>,
        incomplete_unsettled: bool,
        initial_delivery_count: Option<u32>,
        max_message_size: Option<u64>,
        offered_capabilities: Vec<String>,
        desired_capabilities: Vec<String>,
        properties: Option<Value>,
    },
    Flow {
        next_incoming_id: Option<u32>,
        incoming_window: u32,
        next_outgoing_id: u32,
        outgoing_window: u32,
        handle: Option<u32>,
        delivery_count: Option<u32>,
        link_credit: Option<u32>,
        available: Option<u32>,
        drain: bool,
        echo: bool,
        properties: Option<Value>,
    },
    Transfer {
        handle: u32,
        delivery_id: Option<u32>,
        delivery_tag: Option<Bytes>,
        message_format: Option<u32>,
        settled: Option<bool>,
        more: bool,
        rcv_settle_mode: Option<u8>,
        state: Option<DeliveryState>,
        resume: bool,
        aborted: bool,
        batchable: bool,
    },
    Disposition {
        role: bool,
        first: u32,
        last: Option<u32>,
        settled: bool,
        state: Option<DeliveryState>,
        batchable: bool,
    },
    Detach {
        handle: u32,
        closed: bool,
        error: Option<AmqpError>,
    },
    End {
        error: Option<AmqpError>,
    },
    Close {
        error: Option<AmqpError>,
    },
    SaslMechanisms {
        mechanisms: Vec<String>,
    },
    SaslInit {
        mechanism: String,
        initial_response: Option<Bytes>,
        hostname: Option<String>,
    },
    SaslChallenge {
        challenge: Bytes,
    },
    SaslResponse {
        response: Bytes,
    },
    SaslOutcome {
        code: u8,
        additional_data: Option<Bytes>,
    },
}

/// SASL outcome codes.
pub mod sasl_code {
    /// Authenticated.
    pub const OK: u8 = 0;
    /// Bad credentials.
    pub const AUTH: u8 = 1;
    /// A system error.
    pub const SYS: u8 = 2;
    /// A permanent system error.
    pub const SYS_PERM: u8 = 3;
    /// A transient system error.
    pub const SYS_TEMP: u8 = 4;
}

impl Performative {
    /// The spec name.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Performative::Open { .. } => "open",
            Performative::Begin { .. } => "begin",
            Performative::Attach { .. } => "attach",
            Performative::Flow { .. } => "flow",
            Performative::Transfer { .. } => "transfer",
            Performative::Disposition { .. } => "disposition",
            Performative::Detach { .. } => "detach",
            Performative::End { .. } => "end",
            Performative::Close { .. } => "close",
            Performative::SaslMechanisms { .. } => "sasl-mechanisms",
            Performative::SaslInit { .. } => "sasl-init",
            Performative::SaslChallenge { .. } => "sasl-challenge",
            Performative::SaslResponse { .. } => "sasl-response",
            Performative::SaslOutcome { .. } => "sasl-outcome",
        }
    }

    /// As a value.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn to_value(&self) -> Value {
        let err = |e: &Option<AmqpError>| e.as_ref().map_or(Value::Null, AmqpError::to_value);
        match self {
            Performative::Open {
                container_id,
                hostname,
                max_frame_size,
                channel_max,
                idle_timeout,
                offered_capabilities,
                desired_capabilities,
                properties,
            } => Value::composite(
                0x10,
                vec![
                    Value::str(container_id),
                    opt(hostname.clone(), Value::String),
                    Value::Uint(*max_frame_size),
                    Value::Ushort(*channel_max),
                    opt(*idle_timeout, Value::Uint),
                    Value::Null,
                    Value::Null,
                    syms(offered_capabilities),
                    syms(desired_capabilities),
                    properties.clone().unwrap_or(Value::Null),
                ],
            ),
            Performative::Begin {
                remote_channel,
                next_outgoing_id,
                incoming_window,
                outgoing_window,
                handle_max,
                offered_capabilities,
                desired_capabilities,
                properties,
            } => Value::composite(
                0x11,
                vec![
                    opt(*remote_channel, Value::Ushort),
                    Value::Uint(*next_outgoing_id),
                    Value::Uint(*incoming_window),
                    Value::Uint(*outgoing_window),
                    Value::Uint(*handle_max),
                    syms(offered_capabilities),
                    syms(desired_capabilities),
                    properties.clone().unwrap_or(Value::Null),
                ],
            ),
            Performative::Attach {
                name,
                handle,
                role,
                snd_settle_mode,
                rcv_settle_mode,
                source,
                target,
                coordinator: _,
                unsettled,
                incomplete_unsettled,
                initial_delivery_count,
                max_message_size,
                offered_capabilities,
                desired_capabilities,
                properties,
            } => Value::composite(
                0x12,
                vec![
                    Value::str(name),
                    Value::Uint(*handle),
                    Value::Bool(*role),
                    Value::Ubyte(*snd_settle_mode),
                    Value::Ubyte(*rcv_settle_mode),
                    source.as_ref().map_or(Value::Null, Source::to_value),
                    target.as_ref().map_or(Value::Null, Target::to_value),
                    unsettled.clone().unwrap_or(Value::Null),
                    Value::Bool(*incomplete_unsettled),
                    opt(*initial_delivery_count, Value::Uint),
                    opt(*max_message_size, Value::Ulong),
                    syms(offered_capabilities),
                    syms(desired_capabilities),
                    properties.clone().unwrap_or(Value::Null),
                ],
            ),
            Performative::Flow {
                next_incoming_id,
                incoming_window,
                next_outgoing_id,
                outgoing_window,
                handle,
                delivery_count,
                link_credit,
                available,
                drain,
                echo,
                properties,
            } => Value::composite(
                0x13,
                vec![
                    opt(*next_incoming_id, Value::Uint),
                    Value::Uint(*incoming_window),
                    Value::Uint(*next_outgoing_id),
                    Value::Uint(*outgoing_window),
                    opt(*handle, Value::Uint),
                    opt(*delivery_count, Value::Uint),
                    opt(*link_credit, Value::Uint),
                    opt(*available, Value::Uint),
                    Value::Bool(*drain),
                    Value::Bool(*echo),
                    properties.clone().unwrap_or(Value::Null),
                ],
            ),
            Performative::Transfer {
                handle,
                delivery_id,
                delivery_tag,
                message_format,
                settled,
                more,
                rcv_settle_mode,
                state,
                resume,
                aborted,
                batchable,
            } => Value::composite(
                0x14,
                vec![
                    Value::Uint(*handle),
                    opt(*delivery_id, Value::Uint),
                    opt(delivery_tag.clone(), Value::Binary),
                    opt(*message_format, Value::Uint),
                    opt(*settled, Value::Bool),
                    Value::Bool(*more),
                    opt(*rcv_settle_mode, Value::Ubyte),
                    state.as_ref().map_or(Value::Null, DeliveryState::to_value),
                    Value::Bool(*resume),
                    Value::Bool(*aborted),
                    Value::Bool(*batchable),
                ],
            ),
            Performative::Disposition {
                role,
                first,
                last,
                settled,
                state,
                batchable,
            } => Value::composite(
                0x15,
                vec![
                    Value::Bool(*role),
                    Value::Uint(*first),
                    opt(*last, Value::Uint),
                    Value::Bool(*settled),
                    state.as_ref().map_or(Value::Null, DeliveryState::to_value),
                    Value::Bool(*batchable),
                ],
            ),
            Performative::Detach {
                handle,
                closed,
                error,
            } => Value::composite(0x16, vec![Value::Uint(*handle), Value::Bool(*closed), err(error)]),
            Performative::End { error } => Value::composite(0x17, vec![err(error)]),
            Performative::Close { error } => Value::composite(0x18, vec![err(error)]),
            Performative::SaslMechanisms { mechanisms } => Value::composite(
                0x40,
                vec![Value::Array(mechanisms.iter().map(|m| Value::sym(m)).collect())],
            ),
            Performative::SaslInit {
                mechanism,
                initial_response,
                hostname,
            } => Value::composite(
                0x41,
                vec![
                    Value::sym(mechanism),
                    opt(initial_response.clone(), Value::Binary),
                    opt(hostname.clone(), Value::String),
                ],
            ),
            Performative::SaslChallenge { challenge } => {
                Value::composite(0x42, vec![Value::Binary(challenge.clone())])
            }
            Performative::SaslResponse { response } => {
                Value::composite(0x43, vec![Value::Binary(response.clone())])
            }
            Performative::SaslOutcome {
                code,
                additional_data,
            } => Value::composite(
                0x44,
                vec![Value::Ubyte(*code), opt(additional_data.clone(), Value::Binary)],
            ),
        }
    }

    /// From a frame body.
    ///
    /// # Errors
    /// Not a performative, or malformed fields.
    #[allow(clippy::too_many_lines)]
    pub fn from_value(v: &Value) -> DResult<Self> {
        let code = v
            .descriptor()
            .ok_or_else(|| DecodeError("frame body is not a described type".into()))?;
        let f = Fields(
            v.fields()
                .ok_or_else(|| DecodeError("performative is not a list".into()))?,
        );
        Ok(match code {
            0x10 => Performative::Open {
                container_id: f.str(0)?,
                hostname: f.opt_str(1)?,
                max_frame_size: f.opt_u32(2)?.unwrap_or(u32::MAX),
                channel_max: f
                    .opt_u32(3)?
                    .map_or(u16::MAX, |c| u16::try_from(c).unwrap_or(u16::MAX)),
                idle_timeout: f.opt_u32(4)?,
                offered_capabilities: f.symbols(7),
                desired_capabilities: f.symbols(8),
                properties: f.opt(9),
            },
            0x11 => Performative::Begin {
                remote_channel: f.opt_u32(0)?.map(|c| u16::try_from(c).unwrap_or(u16::MAX)),
                next_outgoing_id: f.u32(1)?,
                incoming_window: f.u32(2)?,
                outgoing_window: f.u32(3)?,
                handle_max: f.opt_u32(4)?.unwrap_or(u32::MAX),
                offered_capabilities: f.symbols(5),
                desired_capabilities: f.symbols(6),
                properties: f.opt(7),
            },
            0x12 => {
                let (target, coordinator) = match Target::from_value(f.get(6)) {
                    Ok(t) => (t, false),
                    Err(e) if e.0 == "coordinator" => (None, true),
                    Err(e) => return Err(e),
                };
                Performative::Attach {
                    name: f.str(0)?,
                    handle: f.u32(1)?,
                    role: f.opt_bool(2)?.ok_or_else(|| bad(2, "a mandatory boolean"))?,
                    snd_settle_mode: f
                        .opt_u32(3)?
                        .map_or(snd_settle::MIXED, |m| u8::try_from(m).unwrap_or(u8::MAX)),
                    rcv_settle_mode: f
                        .opt_u32(4)?
                        .map_or(0, |m| u8::try_from(m).unwrap_or(u8::MAX)),
                    source: Source::from_value(f.get(5))?,
                    target,
                    coordinator,
                    unsettled: f.opt(7),
                    incomplete_unsettled: f.bool(8, false)?,
                    initial_delivery_count: f.opt_u32(9)?,
                    max_message_size: f.opt_u64(10)?,
                    offered_capabilities: f.symbols(11),
                    desired_capabilities: f.symbols(12),
                    properties: f.opt(13),
                }
            }
            0x13 => Performative::Flow {
                next_incoming_id: f.opt_u32(0)?,
                incoming_window: f.u32(1)?,
                next_outgoing_id: f.u32(2)?,
                outgoing_window: f.u32(3)?,
                handle: f.opt_u32(4)?,
                delivery_count: f.opt_u32(5)?,
                link_credit: f.opt_u32(6)?,
                available: f.opt_u32(7)?,
                drain: f.bool(8, false)?,
                echo: f.bool(9, false)?,
                properties: f.opt(10),
            },
            0x14 => Performative::Transfer {
                handle: f.u32(0)?,
                delivery_id: f.opt_u32(1)?,
                delivery_tag: f.opt_bin(2)?,
                message_format: f.opt_u32(3)?,
                settled: f.opt_bool(4)?,
                more: f.bool(5, false)?,
                rcv_settle_mode: f.opt_u32(6)?.map(|m| u8::try_from(m).unwrap_or(u8::MAX)),
                state: DeliveryState::from_value(f.get(7))?,
                resume: f.bool(8, false)?,
                aborted: f.bool(9, false)?,
                batchable: f.bool(10, false)?,
            },
            0x15 => Performative::Disposition {
                role: f.opt_bool(0)?.ok_or_else(|| bad(0, "a mandatory boolean"))?,
                first: f.u32(1)?,
                last: f.opt_u32(2)?,
                settled: f.bool(3, false)?,
                state: DeliveryState::from_value(f.get(4))?,
                batchable: f.bool(5, false)?,
            },
            0x16 => Performative::Detach {
                handle: f.u32(0)?,
                closed: f.bool(1, false)?,
                error: AmqpError::from_value(f.get(2))?,
            },
            0x17 => Performative::End {
                error: AmqpError::from_value(f.get(0))?,
            },
            0x18 => Performative::Close {
                error: AmqpError::from_value(f.get(0))?,
            },
            0x40 => Performative::SaslMechanisms {
                mechanisms: f.symbols(0),
            },
            0x41 => Performative::SaslInit {
                mechanism: f.str(0)?,
                initial_response: f.opt_bin(1)?,
                hostname: f.opt_str(2)?,
            },
            0x42 => Performative::SaslChallenge {
                challenge: f.opt_bin(0)?.unwrap_or_default(),
            },
            0x43 => Performative::SaslResponse {
                response: f.opt_bin(0)?.unwrap_or_default(),
            },
            0x44 => Performative::SaslOutcome {
                code: f
                    .opt_u32(0)?
                    .map_or(sasl_code::SYS, |c| u8::try_from(c).unwrap_or(sasl_code::SYS)),
                additional_data: f.opt_bin(1)?,
            },
            other => return Err(DecodeError(format!("unknown performative {other:#x}"))),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt(p: &Performative) {
        let v = p.to_value();
        let b = v.to_bytes();
        let back = Performative::from_value(&Value::decode_all(&b).unwrap()).unwrap();
        assert_eq!(&back, p);
    }

    #[test]
    fn performatives_round_trip() {
        rt(&Performative::Open {
            container_id: "c".into(),
            hostname: Some("vhost:/".into()),
            max_frame_size: 131_072,
            channel_max: 65535,
            idle_timeout: Some(60_000),
            offered_capabilities: vec!["ANONYMOUS-RELAY".into()],
            desired_capabilities: vec![],
            properties: Some(Value::Map(vec![(Value::sym("product"), Value::str("SprigDB"))])),
        });
        rt(&Performative::Begin {
            remote_channel: Some(0),
            next_outgoing_id: 1,
            incoming_window: 2,
            outgoing_window: 3,
            handle_max: 4,
            offered_capabilities: vec![],
            desired_capabilities: vec![],
            properties: None,
        });
        rt(&Performative::Attach {
            name: "l".into(),
            handle: 0,
            role: role::RECEIVER,
            snd_settle_mode: snd_settle::UNSETTLED,
            rcv_settle_mode: 0,
            source: Some(Source {
                address: Some("/queues/q".into()),
                capabilities: vec!["rabbitmq:volatile-queue".into()],
                ..Default::default()
            }),
            target: Some(Target::default()),
            coordinator: false,
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: Some(0),
            max_message_size: Some(1 << 20),
            offered_capabilities: vec![],
            desired_capabilities: vec![],
            properties: None,
        });
        rt(&Performative::Flow {
            next_incoming_id: Some(1),
            incoming_window: 100,
            next_outgoing_id: 2,
            outgoing_window: 100,
            handle: Some(0),
            delivery_count: Some(5),
            link_credit: Some(10),
            available: None,
            drain: true,
            echo: false,
            properties: None,
        });
        rt(&Performative::Transfer {
            handle: 0,
            delivery_id: Some(9),
            delivery_tag: Some(Bytes::from_static(b"t")),
            message_format: Some(0),
            settled: Some(false),
            more: true,
            rcv_settle_mode: None,
            state: None,
            resume: false,
            aborted: false,
            batchable: false,
        });
        for state in [
            DeliveryState::Accepted,
            DeliveryState::Released,
            DeliveryState::Rejected(Some(AmqpError::new("amqp:not-found", "x"))),
            DeliveryState::Modified {
                delivery_failed: true,
                undeliverable_here: false,
                annotations: None,
            },
            DeliveryState::Received {
                section_number: 1,
                section_offset: 2,
            },
        ] {
            rt(&Performative::Disposition {
                role: role::RECEIVER,
                first: 1,
                last: Some(3),
                settled: true,
                state: Some(state),
                batchable: false,
            });
        }
        rt(&Performative::Detach {
            handle: 1,
            closed: true,
            error: Some(AmqpError::new("amqp:not-found", "no queue")),
        });
        rt(&Performative::End { error: None });
        rt(&Performative::Close { error: None });
        rt(&Performative::SaslMechanisms {
            mechanisms: vec!["PLAIN".into()],
        });
        rt(&Performative::SaslInit {
            mechanism: "PLAIN".into(),
            initial_response: Some(Bytes::from_static(b"\0u\0p")),
            hostname: Some("h".into()),
        });
        rt(&Performative::SaslOutcome {
            code: 0,
            additional_data: None,
        });
    }

    #[test]
    fn golden_frames() {
        let mut b = BytesMut::new();
        put_heartbeat(&mut b);
        let f = Frame::parse(&mut b, 512).unwrap().unwrap();
        assert!(f.body.is_none());
        // close() on channel 0: 00 00 00 0c 02 00 00 00 | 00 53 18 45.
        let mut b = BytesMut::new();
        put_frame(&mut b, FRAME_AMQP, 0, &Performative::Close { error: None }.to_value(), &[]);
        assert_eq!(&b[..], &[0, 0, 0, 12, 2, 0, 0, 0, 0x00, 0x53, 0x18, 0x45]);
        let f = Frame::parse(&mut b, 0).unwrap().unwrap();
        assert_eq!(
            Performative::from_value(f.body.as_ref().unwrap()).unwrap(),
            Performative::Close { error: None }
        );
        // An extended header (doff 3) is skipped.
        let mut b = BytesMut::from(&[0u8, 0, 0, 16, 3, 0, 0, 1, 9, 9, 9, 9, 0x00, 0x53, 0x18, 0x45][..]);
        let f = Frame::parse(&mut b, 0).unwrap().unwrap();
        assert_eq!(f.channel, 1);
        // Bad sizes.
        assert!(Frame::parse(&mut BytesMut::from(&[0u8, 0, 0, 4, 2, 0, 0, 0][..]), 0).is_err());
        assert!(Frame::parse(&mut BytesMut::from(&[0u8, 0, 4, 0, 2, 0, 0, 0][..]), 512).is_err());
        assert!(Frame::parse(&mut BytesMut::from(&[0u8, 0, 0, 8, 1, 0, 0, 0][..]), 0).is_err());
    }
}
