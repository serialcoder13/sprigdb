//! AMQP 1.0 messages (part 3.2): the sections of an annotated message, and
//! RabbitMQ 4's address format v2.
//!
//! A message is a sequence of described sections: `header`,
//! `delivery-annotations`, `message-annotations` (the annotations), then
//! the bare message — `properties`, `application-properties`, the body
//! (`data`*, `amqp-sequence`* or one `amqp-value`) — and `footer`.
//! [`Sections::parse`] records where the bare message starts, so it can be
//! stored and delivered byte for byte.

use std::fmt::Write as _;

use bytes::{Bytes, BytesMut};

use super::types::{DResult, DecodeError, Value};

/// Section descriptors.
pub mod section {
    /// `header`.
    pub const HEADER: u64 = 0x70;
    /// `delivery-annotations`.
    pub const DELIVERY_ANNOTATIONS: u64 = 0x71;
    /// `message-annotations`.
    pub const MESSAGE_ANNOTATIONS: u64 = 0x72;
    /// `properties`.
    pub const PROPERTIES: u64 = 0x73;
    /// `application-properties`.
    pub const APPLICATION_PROPERTIES: u64 = 0x74;
    /// `data`.
    pub const DATA: u64 = 0x75;
    /// `amqp-sequence`.
    pub const AMQP_SEQUENCE: u64 = 0x76;
    /// `amqp-value`.
    pub const AMQP_VALUE: u64 = 0x77;
    /// `footer`.
    pub const FOOTER: u64 = 0x78;
}

/// The `header` section.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    /// `durable`.
    pub durable: bool,
    /// `priority` (default 4).
    pub priority: u8,
    /// `ttl` in milliseconds.
    pub ttl: Option<u32>,
    /// `first-acquirer`.
    pub first_acquirer: bool,
    /// `delivery-count`.
    pub delivery_count: u32,
}

impl Default for Header {
    fn default() -> Self {
        Self {
            durable: false,
            priority: 4,
            ttl: None,
            first_acquirer: false,
            delivery_count: 0,
        }
    }
}

impl Header {
    /// As a section.
    #[must_use]
    pub fn to_value(&self) -> Value {
        Value::composite(
            section::HEADER,
            vec![
                Value::Bool(self.durable),
                Value::Ubyte(self.priority),
                self.ttl.map_or(Value::Null, Value::Uint),
                Value::Bool(self.first_acquirer),
                Value::Uint(self.delivery_count),
            ],
        )
    }
}

/// The `properties` section.
#[derive(Debug, Clone, Default, PartialEq)]
#[allow(missing_docs)]
pub struct Properties {
    /// ulong, uuid, binary or string.
    pub message_id: Option<Value>,
    pub user_id: Option<Bytes>,
    pub to: Option<String>,
    pub subject: Option<String>,
    pub reply_to: Option<String>,
    /// ulong, uuid, binary or string.
    pub correlation_id: Option<Value>,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub absolute_expiry_time: Option<i64>,
    pub creation_time: Option<i64>,
    pub group_id: Option<String>,
    pub group_sequence: Option<u32>,
    pub reply_to_group_id: Option<String>,
}

impl Properties {
    /// As a section.
    #[must_use]
    pub fn to_value(&self) -> Value {
        let s = |v: &Option<String>| v.clone().map_or(Value::Null, Value::String);
        let y = |v: &Option<String>| v.clone().map_or(Value::Null, Value::Symbol);
        let t = |v: Option<i64>| v.map_or(Value::Null, Value::Timestamp);
        Value::composite(
            section::PROPERTIES,
            vec![
                self.message_id.clone().unwrap_or(Value::Null),
                self.user_id.clone().map_or(Value::Null, Value::Binary),
                s(&self.to),
                s(&self.subject),
                s(&self.reply_to),
                self.correlation_id.clone().unwrap_or(Value::Null),
                y(&self.content_type),
                y(&self.content_encoding),
                t(self.absolute_expiry_time),
                t(self.creation_time),
                s(&self.group_id),
                self.group_sequence.map_or(Value::Null, Value::Uint),
                s(&self.reply_to_group_id),
            ],
        )
    }

    fn from_fields(f: &[Value]) -> Self {
        let get = |i: usize| f.get(i).filter(|v| !v.is_null());
        let s = |i: usize| get(i).and_then(Value::as_str).map(str::to_owned);
        let t = |i: usize| match get(i) {
            Some(Value::Timestamp(t)) => Some(*t),
            _ => None,
        };
        Self {
            message_id: get(0).cloned(),
            user_id: match get(1) {
                Some(Value::Binary(b)) => Some(b.clone()),
                _ => None,
            },
            to: s(2),
            subject: s(3),
            reply_to: s(4),
            correlation_id: get(5).cloned(),
            content_type: s(6),
            content_encoding: s(7),
            absolute_expiry_time: t(8),
            creation_time: t(9),
            group_id: s(10),
            group_sequence: get(11)
                .and_then(Value::as_u64)
                .and_then(|v| u32::try_from(v).ok()),
            reply_to_group_id: s(12),
        }
    }
}

/// The body.
#[derive(Debug, Clone, PartialEq)]
pub enum Body {
    /// One or more `data` sections.
    Data(Vec<Bytes>),
    /// One or more `amqp-sequence` sections.
    Sequence(Vec<Vec<Value>>),
    /// One `amqp-value`.
    Value(Value),
    /// No body section.
    Empty,
}

/// A parsed message.
#[derive(Debug, Clone, PartialEq)]
pub struct Sections {
    /// `header`.
    pub header: Option<Header>,
    /// `message-annotations` entries.
    pub message_annotations: Vec<(Value, Value)>,
    /// `properties`.
    pub properties: Properties,
    /// `application-properties` entries.
    pub application_properties: Vec<(String, Value)>,
    /// The body.
    pub body: Body,
    /// The bare message (and footer), byte for byte.
    pub bare: Bytes,
    /// The encoded body sections, byte for byte.
    pub body_raw: Bytes,
}

fn map_entries(v: &Value) -> DResult<Vec<(Value, Value)>> {
    match v {
        Value::Map(m) => Ok(m.clone()),
        Value::Null => Ok(Vec::new()),
        _ => Err(DecodeError("annotations are not a map".into())),
    }
}

impl Sections {
    /// Parses a message payload (all of a delivery's transfer frames).
    ///
    /// # Errors
    /// Malformed sections or sections out of order.
    pub fn parse(payload: &Bytes) -> DResult<Self> {
        let mut pos = 0;
        let mut header = None;
        let mut message_annotations = Vec::new();
        let mut properties = Properties::default();
        let mut application_properties = Vec::new();
        let mut body = Body::Empty;
        let mut bare_start = None;
        let mut body_range: Option<(usize, usize)> = None;
        let mut last = 0u64;
        while pos < payload.len() {
            let (v, n) = Value::decode(&payload[pos..])?;
            let code = v
                .descriptor()
                .ok_or_else(|| DecodeError("message section has no descriptor".into()))?;
            if !(section::HEADER..=section::FOOTER).contains(&code) {
                return Err(DecodeError(format!("unknown message section {code:#x}")));
            }
            // Sections come in order; data and sequence sections repeat.
            let repeats = code == last && matches!(code, section::DATA | section::AMQP_SEQUENCE);
            if code < last || (code == last && !repeats) {
                return Err(DecodeError("message sections out of order".into()));
            }
            if code >= section::PROPERTIES && bare_start.is_none() {
                bare_start = Some(pos);
            }
            let inner = match &v {
                Value::Described(_, inner) => &**inner,
                _ => &Value::Null,
            };
            match code {
                section::HEADER => {
                    let f = v.fields().unwrap_or(&[]);
                    let get = |i: usize| f.get(i).filter(|x| !x.is_null());
                    header = Some(Header {
                        durable: get(0).and_then(Value::as_bool).unwrap_or(false),
                        priority: get(1)
                            .and_then(Value::as_u64)
                            .and_then(|p| u8::try_from(p).ok())
                            .unwrap_or(4),
                        ttl: get(2)
                            .and_then(Value::as_u64)
                            .and_then(|t| u32::try_from(t).ok()),
                        first_acquirer: get(3).and_then(Value::as_bool).unwrap_or(false),
                        delivery_count: get(4)
                            .and_then(Value::as_u64)
                            .and_then(|c| u32::try_from(c).ok())
                            .unwrap_or(0),
                    });
                }
                section::MESSAGE_ANNOTATIONS => message_annotations = map_entries(inner)?,
                section::PROPERTIES => {
                    properties = Properties::from_fields(v.fields().unwrap_or(&[]));
                }
                section::APPLICATION_PROPERTIES => {
                    for (k, val) in map_entries(inner)? {
                        let Some(k) = k.as_str() else {
                            return Err(DecodeError(
                                "application-properties keys must be strings".into(),
                            ));
                        };
                        application_properties.push((k.to_owned(), val));
                    }
                }
                section::DATA | section::AMQP_SEQUENCE | section::AMQP_VALUE => {
                    if matches!(body, Body::Value(_))
                        || (code == section::AMQP_VALUE && !matches!(body, Body::Empty))
                        || (code == section::DATA && matches!(body, Body::Sequence(_)))
                        || (code == section::AMQP_SEQUENCE && matches!(body, Body::Data(_)))
                    {
                        return Err(DecodeError("mixed body sections".into()));
                    }
                    body_range = Some((body_range.map_or(pos, |r| r.0), pos + n));
                    match (code, &mut body, inner) {
                        (section::DATA, Body::Data(d), Value::Binary(b)) => d.push(b.clone()),
                        (section::DATA, _, Value::Binary(b)) => body = Body::Data(vec![b.clone()]),
                        (section::AMQP_SEQUENCE, Body::Sequence(s), Value::List(l)) => {
                            s.push(l.clone());
                        }
                        (section::AMQP_SEQUENCE, _, Value::List(l)) => {
                            body = Body::Sequence(vec![l.clone()]);
                        }
                        (section::AMQP_VALUE, _, v) => body = Body::Value(v.clone()),
                        _ => return Err(DecodeError("malformed body section".into())),
                    }
                }
                _ => {}
            }
            last = code;
            pos += n;
        }
        let bare = bare_start.map_or_else(Bytes::new, |s| payload.slice(s..));
        let body_raw = body_range.map_or_else(Bytes::new, |(a, b)| payload.slice(a..b));
        Ok(Self {
            header,
            message_annotations,
            properties,
            application_properties,
            body,
            bare,
            body_raw,
        })
    }

    /// A message annotation by symbol key.
    #[must_use]
    pub fn annotation(&self, key: &str) -> Option<&Value> {
        self.message_annotations
            .iter()
            .find(|(k, _)| k.as_str() == Some(key))
            .map(|(_, v)| v)
    }
}

/// Encodes a `message-annotations` section.
#[must_use]
pub fn annotations_section(entries: Vec<(Value, Value)>) -> Value {
    Value::Described(
        Box::new(Value::Ulong(section::MESSAGE_ANNOTATIONS)),
        Box::new(Value::Map(entries)),
    )
}

/// Encodes an `application-properties` section.
#[must_use]
pub fn application_properties_section(entries: Vec<(String, Value)>) -> Value {
    Value::Described(
        Box::new(Value::Ulong(section::APPLICATION_PROPERTIES)),
        Box::new(Value::Map(
            entries
                .into_iter()
                .map(|(k, v)| (Value::String(k), v))
                .collect(),
        )),
    )
}

/// Encodes a `data` section.
#[must_use]
pub fn data_section(b: Bytes) -> Value {
    Value::Described(
        Box::new(Value::Ulong(section::DATA)),
        Box::new(Value::Binary(b)),
    )
}

/// Encodes an `amqp-value` section.
#[must_use]
pub fn value_section(v: Value) -> Value {
    Value::Described(Box::new(Value::Ulong(section::AMQP_VALUE)), Box::new(v))
}

/// Concatenates sections.
#[must_use]
pub fn encode_sections(parts: &[Value]) -> Bytes {
    let mut b = BytesMut::new();
    for p in parts {
        p.encode(&mut b);
    }
    b.freeze()
}

/// A v2 address (or a legacy one, when allowed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Address {
    /// `/exchanges/:e/:key` or `/exchanges/:e` (key empty).
    Exchange {
        /// The exchange.
        exchange: String,
        /// The routing key.
        key: String,
    },
    /// `/queues/:q`: the default exchange with the queue name as key.
    Queue(String),
}

impl Address {
    /// Parses an address. With `legacy` (`amqp10.legacy_addresses`), bare
    /// queue names and the v1 forms `/amq/queue/:q`, `/queue/:q`,
    /// `/exchange/:e/:key` and `/topic/:key` are accepted too.
    ///
    /// # Errors
    /// The reason it is refused (39272).
    pub fn parse(addr: &str, legacy: bool) -> Result<Address, String> {
        let segs: Vec<&str> = addr.split('/').collect();
        let dec =
            |s: &str| percent_decode(s).ok_or_else(|| format!("bad percent-encoding in '{addr}'"));
        match segs.as_slice() {
            ["", "exchanges", e] | ["", "exchanges", e, _] => {
                let exchange = dec(e)?;
                if exchange.is_empty() || exchange == "amq.default" {
                    return Err("the default exchange is addressed as /queues/:queue".into());
                }
                let key = match segs.get(3) {
                    Some(k) => dec(k)?,
                    None => String::new(),
                };
                Ok(Address::Exchange { exchange, key })
            }
            ["", "queues", q] => {
                let q = dec(q)?;
                if q.is_empty() {
                    return Err("empty queue name".into());
                }
                Ok(Address::Queue(q))
            }
            _ if legacy => match segs.as_slice() {
                ["", "amq", "queue", q] | ["", "queue", q] if !q.is_empty() => {
                    Ok(Address::Queue((*q).to_owned()))
                }
                ["", "exchange", e, k] if !e.is_empty() => Ok(Address::Exchange {
                    exchange: (*e).to_owned(),
                    key: (*k).to_owned(),
                }),
                ["", "exchange", e] if !e.is_empty() => Ok(Address::Exchange {
                    exchange: (*e).to_owned(),
                    key: String::new(),
                }),
                ["", "topic", k] => Ok(Address::Exchange {
                    exchange: "amq.topic".into(),
                    key: (*k).to_owned(),
                }),
                [q] if !q.is_empty() => Ok(Address::Queue((*q).to_owned())),
                _ => Err(format!("unsupported address '{addr}'")),
            },
            _ => Err(format!(
                "address '{addr}' is not of the form /exchanges/:exchange/:key, /exchanges/:exchange or /queues/:queue"
            )),
        }
    }

    /// The exchange and routing key a publish to it uses.
    #[must_use]
    pub fn route(&self) -> (&str, &str) {
        match self {
            Address::Exchange { exchange, key } => (exchange, key),
            Address::Queue(q) => ("", q),
        }
    }
}

/// `/queues/:q`, percent-encoded.
#[must_use]
pub fn queue_address(q: &str) -> String {
    format!("/queues/{}", percent_encode(q))
}

/// RFC 3986 percent-encoding of everything but unreserved characters.
#[must_use]
pub fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

/// Decodes percent-encoding; `None` for a bad escape or non-UTF-8 result.
#[must_use]
pub fn percent_decode(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            let h = std::str::from_utf8(b.get(i + 1..i + 3)?).ok()?;
            out.push(u8::from_str_radix(h, 16).ok()?);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses() {
        let p = |s: &str| Address::parse(s, false);
        assert_eq!(
            p("/exchanges/amq.direct/my-routing_key%2F123"),
            Ok(Address::Exchange {
                exchange: "amq.direct".into(),
                key: "my-routing_key/123".into()
            })
        );
        assert_eq!(
            p("/exchanges/logs"),
            Ok(Address::Exchange {
                exchange: "logs".into(),
                key: String::new()
            })
        );
        assert_eq!(p("/queues/a%20b"), Ok(Address::Queue("a b".into())));
        assert!(p("/exchanges/").is_err());
        assert!(p("/exchanges/amq.default/k").is_err());
        assert!(p("/exchanges/e/a/b").is_err());
        assert!(p("/queues/").is_err());
        assert!(p("orders").is_err());
        assert!(p("/queues/%zz").is_err());
        assert_eq!(
            Address::parse("orders", true),
            Ok(Address::Queue("orders".into()))
        );
        assert_eq!(
            Address::parse("/amq/queue/orders", true),
            Ok(Address::Queue("orders".into()))
        );
        assert_eq!(queue_address("a/b c"), "/queues/a%2Fb%20c");
        assert_eq!(percent_decode("a%2Fb%20c").unwrap(), "a/b c");
    }

    #[test]
    fn sections_parse_and_keep_bare_bytes() {
        let header = Header {
            durable: true,
            priority: 7,
            ttl: Some(1000),
            ..Default::default()
        };
        let props = Properties {
            message_id: Some(Value::str("m1")),
            to: Some("/queues/q".into()),
            reply_to: Some("/queues/r".into()),
            content_type: Some("text/plain".into()),
            ..Default::default()
        };
        let parts = vec![
            header.to_value(),
            Value::Described(
                Box::new(Value::Ulong(section::DELIVERY_ANNOTATIONS)),
                Box::new(Value::Map(vec![])),
            ),
            annotations_section(vec![(Value::sym("x-opt-a"), Value::Int(1))]),
            props.to_value(),
            application_properties_section(vec![("k".into(), Value::str("v"))]),
            data_section(Bytes::from_static(b"one")),
            data_section(Bytes::from_static(b"two")),
        ];
        let payload = encode_sections(&parts);
        let s = Sections::parse(&payload).unwrap();
        assert_eq!(s.header.as_ref().unwrap(), &header);
        assert_eq!(s.properties, props);
        assert_eq!(
            s.application_properties,
            vec![("k".into(), Value::str("v"))]
        );
        assert_eq!(
            s.body,
            Body::Data(vec![Bytes::from_static(b"one"), Bytes::from_static(b"two")])
        );
        assert_eq!(s.annotation("x-opt-a"), Some(&Value::Int(1)));
        assert_eq!(s.bare, encode_sections(&parts[3..]));
        assert_eq!(s.body_raw, encode_sections(&parts[5..]));
        // Out of order.
        let bad = encode_sections(&[props.to_value(), header.to_value()]);
        assert!(Sections::parse(&bad).is_err());
        // Mixed body.
        let bad = encode_sections(&[data_section(Bytes::new()), value_section(Value::Null)]);
        assert!(Sections::parse(&bad).is_err());
        // Not a section.
        assert!(Sections::parse(&Bytes::from_static(&[0x40])).is_err());
    }
}
