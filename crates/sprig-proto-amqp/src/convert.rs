//! Messages in and out of both protocols, and conversion between them
//! (chapter 50, "Message encoding"), following RabbitMQ 4's documented
//! mapping (rabbitmq.com/docs/conversions).
//!
//! A message keeps the encoding it was published in. Conversion happens
//! only when a consumer speaks the other protocol, and delivery-time
//! annotations (`x-delivery-count`, `x-sprig-delivery`, `x-death`, ...)
//! are added on the way out, never stored.
//!
//! 1.0 to 0-9-1:
//!
//! | 1.0 | 0-9-1 |
//! |---|---|
//! | `header.durable` | `delivery_mode` 2 (true) or 1 |
//! | `header.priority` | `priority` |
//! | `header.ttl` | `expiration` (milliseconds as text) |
//! | `message-id`, `correlation-id`: string under 256 bytes, ulong, uuid | `message_id`, `correlation_id` (text; uuid as `urn:uuid:...`) |
//! | the same, longer string or binary | header `x-message-id`, `x-correlation-id` |
//! | `user-id` (UTF-8, up to 255 bytes) | `user_id` |
//! | `reply-to` | `reply_to`, `/queues/:q` becoming `q` |
//! | `content-type`, `content-encoding` | `content_type`, `content_encoding` |
//! | `creation-time` | `timestamp` (seconds) |
//! | `group-id` | `app_id` |
//! | `application-properties` | `headers` |
//! | message annotations `x-*` (`x-cc` as `CC`) | `headers` |
//! | one `data` section | the body |
//! | several `data`, `amqp-sequence`, `amqp-value` | the encoded body sections, `type = amqp-1.0` |
//!
//! 0-9-1 to 1.0 is the reverse; `type` becomes the annotation
//! `x-basic-type`, header values that are tables or arrays are dropped
//! (RabbitMQ's rule), header keys starting `x-` become message
//! annotations, and the body becomes one `data` section — unless `type`
//! is `amqp-1.0`, when the body is the 1.0 sections it came from.

use std::sync::Arc;

use bytes::Bytes;

use crate::error::MqError;
use crate::message::{Delivery, Message, MessageInfo, MsgFormat};
use crate::v091::codec::{FieldTable, FieldValue};
use crate::v091::props::BasicProperties;
use crate::v10::message::{
    Body, Header, Properties, Sections, annotations_section, application_properties_section,
    data_section, encode_sections, percent_decode, queue_address,
};
use crate::v10::types::Value;

/// The `type` RabbitMQ sets on a 0-9-1 message whose body is 1.0
/// sections.
pub const AMQP10_TYPE: &str = "amqp-1.0";

/// A message published over 0-9-1.
///
/// # Errors
/// 406 for a bad `expiration`.
pub fn from_091(
    exchange: String,
    routing_key: String,
    raw_props: Bytes,
    props: &BasicProperties,
    body: Bytes,
) -> Result<Message, MqError> {
    let info = MessageInfo {
        headers: props.headers.clone().unwrap_or_default(),
        priority: props.priority,
        ttl_ms: props.ttl_ms()?,
        durable: props.delivery_mode == Some(2),
        message_id: props.message_id.clone(),
        reply_to: props.reply_to.clone(),
        size: (raw_props.len() + body.len()) as u64,
    };
    Ok(Message {
        format: MsgFormat::Amqp091,
        exchange,
        routing_key,
        props: raw_props,
        body,
        info,
    })
}

/// A message published over 1.0, whose payload parsed into `s`.
#[must_use]
pub fn from_10(exchange: String, routing_key: String, s: &Sections) -> Message {
    let header = s.header.clone().unwrap_or_default();
    let mut parts = Vec::new();
    if s.header.is_some() {
        parts.push(header.to_value());
    }
    if !s.message_annotations.is_empty() {
        parts.push(annotations_section(s.message_annotations.clone()));
    }
    let props = encode_sections(&parts);
    let mut headers = FieldTable::new();
    for (k, v) in &s.application_properties {
        if let Some(f) = to_field(v) {
            headers.insert(k.clone(), f);
        }
    }
    let info = MessageInfo {
        headers,
        priority: s.header.as_ref().map(|h| h.priority),
        ttl_ms: header.ttl.map(u64::from),
        durable: header.durable,
        message_id: s.properties.message_id.as_ref().and_then(id_text),
        reply_to: s.properties.reply_to.clone(),
        size: (props.len() + s.bare.len()) as u64,
    };
    Message {
        format: MsgFormat::Amqp10,
        exchange,
        routing_key,
        props,
        body: s.bare.clone(),
        info,
    }
}

/// The text of a message or correlation id.
fn id_text(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Ulong(n) => Some(n.to_string()),
        Value::Uuid(u) => Some(format!("urn:uuid:{}", uuid_text(u))),
        _ => None,
    }
}

fn uuid_text(u: &[u8; 16]) -> String {
    let h: String = u.iter().map(|b| format!("{b:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &h[..8], &h[8..12], &h[12..16], &h[16..20], &h[20..])
}

fn parse_urn_uuid(s: &str) -> Option<[u8; 16]> {
    let hex: String = s.strip_prefix("urn:uuid:")?.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, o) in out.iter_mut().enumerate() {
        *o = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// A 1.0 value as a 0-9-1 field value (RabbitMQ's table); `None` for
/// values with no counterpart (decimals, NaN and infinite floats).
#[must_use]
pub fn to_field(v: &Value) -> Option<FieldValue> {
    Some(match v {
        Value::Null => FieldValue::Void,
        Value::Bool(b) => FieldValue::Bool(*b),
        Value::Ubyte(x) => FieldValue::U8(*x),
        Value::Ushort(x) => FieldValue::U16(*x),
        Value::Uint(x) => FieldValue::U32(*x),
        Value::Ulong(x) => FieldValue::I64(x.cast_signed()),
        Value::Byte(x) => FieldValue::I8(*x),
        Value::Short(x) => FieldValue::I16(*x),
        Value::Int(x) => FieldValue::I32(*x),
        Value::Long(x) => FieldValue::I64(*x),
        Value::Float(x) if x.is_finite() => FieldValue::F32(*x),
        Value::Double(x) if x.is_finite() => FieldValue::F64(*x),
        Value::Char(c) => FieldValue::str(&c.to_string()),
        Value::Timestamp(ms) => FieldValue::Timestamp(u64::try_from(ms / 1000).unwrap_or(0)),
        Value::Uuid(u) => FieldValue::str(&format!("urn:uuid:{}", uuid_text(u))),
        Value::Binary(b) => FieldValue::Bytes(b.clone()),
        Value::String(s) | Value::Symbol(s) => FieldValue::str(s),
        Value::List(l) | Value::Array(l) => FieldValue::Array(l.iter().filter_map(to_field).collect()),
        Value::Map(m) => FieldValue::Table(FieldTable(
            m.iter()
                .filter_map(|(k, v)| Some((k.as_str()?.to_owned(), to_field(v)?)))
                .collect(),
        )),
        Value::Described(_, inner) => return to_field(inner),
        _ => return None,
    })
}

/// A 0-9-1 field value as a 1.0 value; `None` for decimals.
#[must_use]
pub fn to_value(f: &FieldValue) -> Option<Value> {
    Some(match f {
        FieldValue::Void => Value::Null,
        FieldValue::Bool(b) => Value::Bool(*b),
        FieldValue::I8(x) => Value::Byte(*x),
        FieldValue::U8(x) => Value::Ubyte(*x),
        FieldValue::I16(x) => Value::Short(*x),
        FieldValue::U16(x) => Value::Ushort(*x),
        FieldValue::I32(x) => Value::Int(*x),
        FieldValue::U32(x) => Value::Uint(*x),
        FieldValue::I64(x) => Value::Long(*x),
        FieldValue::F32(x) => Value::Float(*x),
        FieldValue::F64(x) => Value::Double(*x),
        FieldValue::LongStr(b) => match std::str::from_utf8(b) {
            Ok(s) => Value::str(s),
            Err(_) => Value::Binary(b.clone()),
        },
        FieldValue::Bytes(b) => Value::Binary(b.clone()),
        FieldValue::Timestamp(s) => Value::Timestamp(i64::try_from(s.saturating_mul(1000)).unwrap_or(i64::MAX)),
        FieldValue::Array(a) => Value::List(a.iter().filter_map(to_value).collect()),
        FieldValue::Table(t) => Value::Map(
            t.0.iter()
                .filter_map(|(k, v)| {
                    let key = if k.starts_with("x-") { Value::sym(k) } else { Value::str(k) };
                    Some((key, to_value(v)?))
                })
                .collect(),
        ),
        FieldValue::Decimal(..) => return None,
    })
}

/// A delivery as 0-9-1 content: the property section and the body.
///
/// # Errors
/// A stored message that no longer parses (internal error).
pub fn to_091(d: &Delivery) -> Result<(Bytes, Bytes), MqError> {
    let m = &d.message;
    match m.format {
        MsgFormat::Amqp091 => {
            if d.annotations.is_empty() {
                return Ok((m.props.clone(), m.body.clone()));
            }
            let mut p = BasicProperties::decode(&m.props)?;
            let mut h = p.headers.take().unwrap_or_default();
            for (k, v) in &d.annotations.0 {
                h.insert(k.clone(), v.clone());
            }
            p.headers = Some(h);
            Ok((p.to_bytes(), m.body.clone()))
        }
        MsgFormat::Amqp10 => {
            let mut payload = m.props.to_vec();
            payload.extend_from_slice(&m.body);
            let s = Sections::parse(&Bytes::from(payload))
                .map_err(|e| MqError::new(crate::error::MqCode::FrameError, e.0).with_reply(541))?;
            let (mut p, body) = props_10_to_091(&s);
            if !d.annotations.is_empty() {
                let mut h = p.headers.take().unwrap_or_default();
                for (k, v) in &d.annotations.0 {
                    h.insert(k.clone(), v.clone());
                }
                p.headers = Some(h);
            }
            Ok((p.to_bytes(), body))
        }
    }
}

/// 1.0 sections as 0-9-1 properties and body.
#[must_use]
pub fn props_10_to_091(s: &Sections) -> (BasicProperties, Bytes) {
    let mut p = BasicProperties::default();
    let mut headers = FieldTable::new();
    if let Some(h) = &s.header {
        p.delivery_mode = Some(if h.durable { 2 } else { 1 });
        p.priority = Some(h.priority);
        p.expiration = h.ttl.map(|t| t.to_string());
    }
    let pr = &s.properties;
    let id = |v: &Option<Value>, header: &str, headers: &mut FieldTable| -> Option<String> {
        match v.as_ref()? {
            Value::String(s) if s.len() < 256 => Some(s.clone()),
            Value::String(s) => {
                headers.insert(header, FieldValue::str(s));
                None
            }
            Value::Binary(b) => {
                headers.insert(header, FieldValue::Bytes(b.clone()));
                None
            }
            other => id_text(other),
        }
    };
    p.message_id = id(&pr.message_id, "x-message-id", &mut headers);
    p.correlation_id = id(&pr.correlation_id, "x-correlation-id", &mut headers);
    p.user_id = pr
        .user_id
        .as_ref()
        .and_then(|b| std::str::from_utf8(b).ok().filter(|s| s.len() <= 255).map(str::to_owned));
    p.reply_to = pr.reply_to.as_ref().map(|r| {
        r.strip_prefix("/queues/")
            .and_then(percent_decode)
            .unwrap_or_else(|| r.clone())
    });
    p.content_type.clone_from(&pr.content_type);
    p.content_encoding.clone_from(&pr.content_encoding);
    p.timestamp = pr.creation_time.and_then(|t| u64::try_from(t / 1000).ok());
    p.app_id.clone_from(&pr.group_id);
    for (k, v) in &s.application_properties {
        if let Some(f) = to_field(v) {
            headers.insert(k.clone(), f);
        }
    }
    for (k, v) in &s.message_annotations {
        let Some(k) = k.as_str() else { continue };
        if k == "x-basic-type" {
            p.kind = v.as_str().map(str::to_owned);
        } else if k == "x-cc" {
            if let Some(f) = to_field(v) {
                headers.insert("CC", f);
            }
        } else if k.starts_with("x-") && k != "x-exchange" && k != "x-routing-key" {
            if let Some(f) = to_field(v) {
                headers.insert(k, f);
            }
        }
    }
    if !headers.is_empty() {
        p.headers = Some(headers);
    }
    let body = match &s.body {
        Body::Data(d) if d.len() == 1 => d[0].clone(),
        Body::Empty => Bytes::new(),
        _ => {
            p.kind = Some(AMQP10_TYPE.into());
            s.body_raw.clone()
        }
    };
    (p, body)
}

/// A delivery as a 1.0 message payload: `header` (with the delivery
/// count), `message-annotations` (with `x-exchange`, `x-routing-key` and
/// the delivery annotations), then the bare message.
///
/// # Errors
/// A stored message that no longer parses (internal error).
pub fn to_10(d: &Delivery) -> Result<Bytes, MqError> {
    let m = &d.message;
    let (mut header, mut annotations, bare) = match m.format {
        MsgFormat::Amqp10 => {
            let s = if m.props.is_empty() {
                None
            } else {
                Some(
                    Sections::parse(&m.props)
                        .map_err(|e| MqError::new(crate::error::MqCode::FrameError, e.0).with_reply(541))?,
                )
            };
            let header = s.as_ref().and_then(|s| s.header.clone());
            let ann = s.map(|s| s.message_annotations).unwrap_or_default();
            (header, ann, m.body.clone())
        }
        MsgFormat::Amqp091 => {
            let p = BasicProperties::decode(&m.props)?;
            let (h, a, bare) = props_091_to_10(&p, &m.body);
            (h, a, bare)
        }
    };
    if d.delivery_count > 0 || header.is_some() {
        let h = header.get_or_insert_with(Header::default);
        h.delivery_count = d.delivery_count;
        h.first_acquirer = false;
    }
    annotations.retain(|(k, _)| !matches!(k.as_str(), Some("x-exchange" | "x-routing-key")));
    annotations.push((Value::sym("x-exchange"), Value::str(&m.exchange)));
    annotations.push((Value::sym("x-routing-key"), Value::str(&m.routing_key)));
    for (k, v) in &d.annotations.0 {
        if let Some(v) = to_value(v) {
            annotations.push((Value::sym(k), v));
        }
    }
    let mut parts = Vec::new();
    if let Some(h) = header {
        parts.push(h.to_value());
    }
    parts.push(annotations_section(annotations));
    let mut out = encode_sections(&parts).to_vec();
    out.extend_from_slice(&bare);
    Ok(Bytes::from(out))
}

/// 0-9-1 properties and body as a 1.0 header, message annotations and
/// bare message.
#[must_use]
pub fn props_091_to_10(
    p: &BasicProperties,
    body: &Bytes,
) -> (Option<Header>, Vec<(Value, Value)>, Bytes) {
    let header = (p.delivery_mode.is_some() || p.priority.is_some() || p.expiration.is_some())
        .then(|| Header {
            durable: p.delivery_mode == Some(2),
            priority: p.priority.unwrap_or(4),
            ttl: p.expiration.as_ref().and_then(|e| e.parse().ok()),
            ..Header::default()
        });
    let mut annotations = Vec::new();
    let mut app = Vec::new();
    let mut message_id = p.message_id.as_ref().map(|s| id_value(s));
    let mut correlation_id = p.correlation_id.as_ref().map(|s| id_value(s));
    if let Some(h) = &p.headers {
        for (k, v) in &h.0 {
            match k.as_str() {
                "x-message-id" if message_id.is_none() => message_id = to_value(v),
                "x-correlation-id" if correlation_id.is_none() => correlation_id = to_value(v),
                _ if k.starts_with("x-") => {
                    if let Some(v) = to_value(v) {
                        annotations.push((Value::sym(k), v));
                    }
                }
                _ => {
                    if !matches!(v, FieldValue::Table(_) | FieldValue::Array(_)) {
                        if let Some(v) = to_value(v) {
                            app.push((k.clone(), v));
                        }
                    }
                }
            }
        }
    }
    let amqp10_body = p.kind.as_deref() == Some(AMQP10_TYPE);
    if let Some(t) = p.kind.as_ref().filter(|_| !amqp10_body) {
        annotations.push((Value::sym("x-basic-type"), Value::str(t)));
    }
    let props = Properties {
        message_id,
        user_id: p.user_id.as_ref().map(|u| Bytes::copy_from_slice(u.as_bytes())),
        reply_to: p.reply_to.as_ref().map(|r| {
            if r.starts_with('/') {
                r.clone()
            } else {
                queue_address(r)
            }
        }),
        correlation_id,
        content_type: p.content_type.clone(),
        content_encoding: p.content_encoding.clone(),
        creation_time: p
            .timestamp
            .and_then(|t| i64::try_from(t.saturating_mul(1000)).ok()),
        group_id: p.app_id.clone(),
        ..Properties::default()
    };
    let mut parts = vec![props.to_value()];
    if !app.is_empty() {
        parts.push(application_properties_section(app));
    }
    let mut bare = encode_sections(&parts).to_vec();
    if amqp10_body && Sections::parse(body).is_ok() {
        bare.extend_from_slice(body);
    } else {
        bare.extend_from_slice(&encode_sections(&[data_section(body.clone())]));
    }
    (header, annotations, Bytes::from(bare))
}

fn id_value(s: &str) -> Value {
    parse_urn_uuid(s).map_or_else(|| Value::str(s), Value::Uuid)
}

/// The message with a new `reply_to` (direct reply-to rewriting).
///
/// # Errors
/// A message whose properties don't parse.
pub fn with_reply_to(m: &Message, reply_to: &str) -> Result<Message, MqError> {
    let mut out = m.clone();
    match m.format {
        MsgFormat::Amqp091 => {
            let mut p = BasicProperties::decode(&m.props)?;
            p.reply_to = Some(reply_to.to_owned());
            out.props = p.to_bytes();
        }
        MsgFormat::Amqp10 => {
            let s = Sections::parse(&m.body).map_err(|e| MqError::precondition(e.0))?;
            let mut props = s.properties.clone();
            props.reply_to = Some(queue_address(reply_to));
            // Replace the properties section, keep the rest of the bare
            // message as it was.
            let mut rest = m.body.clone();
            if let Ok((v, n)) = Value::decode(&m.body) {
                if v.descriptor() == Some(crate::v10::message::section::PROPERTIES) {
                    rest = m.body.slice(n..);
                }
            }
            let mut b = encode_sections(&[props.to_value()]).to_vec();
            b.extend_from_slice(&rest);
            out.body = Bytes::from(b);
        }
    }
    out.info.reply_to = Some(reply_to.to_owned());
    Ok(out)
}

/// Shares a message between the queues it was routed to.
#[must_use]
pub fn shared(m: Message) -> Arc<Message> {
    Arc::new(m)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::DeliveryId;

    fn delivery(m: Message, count: u32) -> Delivery {
        Delivery {
            id: DeliveryId::default(),
            queue: "q".into(),
            message: Arc::new(m),
            redelivered: count > 0,
            delivery_count: count,
            annotations: FieldTable::new(),
        }
    }

    #[test]
    fn amqp091_to_10_and_back() {
        let p = BasicProperties {
            content_type: Some("application/json".into()),
            headers: Some(FieldTable::from([
                ("tenant", FieldValue::str("acme")),
                ("x-retries", FieldValue::I32(2)),
                ("nested", FieldValue::Table(FieldTable::new())),
            ])),
            delivery_mode: Some(2),
            priority: Some(5),
            correlation_id: Some("urn:uuid:550e8400-e29b-41d4-a716-446655440000".into()),
            reply_to: Some("replies".into()),
            expiration: Some("60000".into()),
            message_id: Some("m-1".into()),
            timestamp: Some(1_700_000_000),
            kind: Some("order".into()),
            app_id: Some("shop".into()),
            ..Default::default()
        };
        let raw = p.to_bytes();
        let m = from_091("ex".into(), "rk".into(), raw.clone(), &p, Bytes::from_static(b"{}")).unwrap();
        assert_eq!(m.info.ttl_ms, Some(60_000));
        assert!(m.info.durable);
        let payload = to_10(&delivery(m.clone(), 1)).unwrap();
        let s = Sections::parse(&payload).unwrap();
        let h = s.header.clone().unwrap();
        assert!(h.durable);
        assert_eq!((h.priority, h.ttl, h.delivery_count), (5, Some(60_000), 1));
        assert_eq!(s.properties.reply_to.as_deref(), Some("/queues/replies"));
        assert_eq!(s.properties.message_id, Some(Value::str("m-1")));
        assert!(matches!(s.properties.correlation_id, Some(Value::Uuid(_))));
        assert_eq!(s.properties.creation_time, Some(1_700_000_000_000));
        assert_eq!(s.properties.group_id.as_deref(), Some("shop"));
        assert_eq!(s.application_properties, vec![("tenant".into(), Value::str("acme"))]);
        assert_eq!(s.annotation("x-retries"), Some(&Value::Int(2)));
        assert_eq!(s.annotation("x-basic-type"), Some(&Value::str("order")));
        assert_eq!(s.annotation("x-exchange"), Some(&Value::str("ex")));
        assert_eq!(s.annotation("x-routing-key"), Some(&Value::str("rk")));
        assert_eq!(s.body, Body::Data(vec![Bytes::from_static(b"{}")]));
        // And back to 0-9-1, through a 1.0-format message.
        let m10 = from_10("ex".into(), "rk".into(), &s);
        let (props, body) = to_091(&delivery(m10, 0)).unwrap();
        let back = BasicProperties::decode(&props).unwrap();
        assert_eq!(&body[..], b"{}");
        assert_eq!(back.content_type, p.content_type);
        assert_eq!(back.delivery_mode, Some(2));
        assert_eq!(back.reply_to.as_deref(), Some("replies"));
        assert_eq!(back.correlation_id, p.correlation_id);
        assert_eq!(back.timestamp, p.timestamp);
        assert_eq!(back.kind.as_deref(), Some("order"));
        let hb = back.headers.unwrap();
        assert_eq!(hb.get("tenant"), Some(&FieldValue::str("acme")));
        assert_eq!(hb.get("x-retries"), Some(&FieldValue::I32(2)));
        assert!(hb.get("nested").is_none());
    }

    #[test]
    fn amqp10_value_bodies_get_the_amqp10_type() {
        let payload = encode_sections(&[
            Properties {
                message_id: Some(Value::Ulong(42)),
                ..Default::default()
            }
            .to_value(),
            crate::v10::message::value_section(Value::str("hello")),
        ]);
        let s = Sections::parse(&payload).unwrap();
        let m = from_10(String::new(), "q".into(), &s);
        assert_eq!(m.body, payload, "bare message kept byte for byte");
        let (props, body) = to_091(&delivery(m.clone(), 0)).unwrap();
        let p = BasicProperties::decode(&props).unwrap();
        assert_eq!(p.kind.as_deref(), Some(AMQP10_TYPE));
        assert_eq!(p.message_id.as_deref(), Some("42"));
        // The body is the encoded amqp-value section, and goes back to 1.0
        // as it was.
        let m091 = from_091(String::new(), "q".into(), props, &p, body).unwrap();
        let again = Sections::parse(&to_10(&delivery(m091, 0)).unwrap()).unwrap();
        assert_eq!(again.body, Body::Value(Value::str("hello")));
        // A 1.0 consumer of a 1.0 message gets the bare bytes unchanged.
        let out = to_10(&delivery(m, 0)).unwrap();
        assert!(out.ends_with(&payload));
    }

    #[test]
    fn annotations_are_added_on_delivery_only() {
        let p = BasicProperties::default();
        let m = from_091(String::new(), "q".into(), p.to_bytes(), &p, Bytes::new()).unwrap();
        let mut d = delivery(m.clone(), 0);
        d.annotations.insert("x-delivery-count", FieldValue::I64(3));
        let (props, _) = to_091(&d).unwrap();
        let back = BasicProperties::decode(&props).unwrap();
        assert_eq!(back.headers.unwrap().get("x-delivery-count"), Some(&FieldValue::I64(3)));
        assert_eq!(d.message.props, m.props);
    }

    #[test]
    fn reply_to_rewrite() {
        let p = BasicProperties {
            reply_to: Some("amq.rabbitmq.reply-to".into()),
            ..Default::default()
        };
        let m = from_091(String::new(), "rpc".into(), p.to_bytes(), &p, Bytes::new()).unwrap();
        let r = with_reply_to(&m, "amq.rabbitmq.reply-to.abc").unwrap();
        assert_eq!(
            BasicProperties::decode(&r.props).unwrap().reply_to.as_deref(),
            Some("amq.rabbitmq.reply-to.abc")
        );
        let payload = encode_sections(&[
            Properties::default().to_value(),
            data_section(Bytes::from_static(b"x")),
        ]);
        let m10 = from_10(String::new(), "rpc".into(), &Sections::parse(&payload).unwrap());
        let r = with_reply_to(&m10, "amq.rabbitmq.reply-to.abc").unwrap();
        let s = Sections::parse(&r.body).unwrap();
        assert_eq!(s.properties.reply_to.as_deref(), Some("/queues/amq.rabbitmq.reply-to.abc"));
        assert_eq!(s.body, Body::Data(vec![Bytes::from_static(b"x")]));
    }
}
