//! Messages between the front ends' form ([`Message`]) and rows
//! ([`StoredMsg`], [`OutboxRow`]): a message keeps its source encoding
//! (chapter 50, "Message encoding"); what the engine reads from it
//! (headers, priority, TTL) is parsed again when a row is loaded.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use sprig_proto_amqp::convert;
use sprig_proto_amqp::error::MqError;
use sprig_proto_amqp::message::{Message, MsgFormat};
use sprig_proto_amqp::v10::message::Sections;
use sprig_proto_amqp::v091::codec::{FieldTable, FieldValue};
use sprig_proto_amqp::v091::props::BasicProperties;

/// `__sprig_qm.fmt` of a format.
#[must_use]
pub const fn fmt_code(f: MsgFormat) -> u8 {
    match f {
        MsgFormat::Amqp091 => 0,
        MsgFormat::Amqp10 => 1,
    }
}

/// Rebuilds a message from its stored parts.
///
/// # Errors
/// 406 for properties that no longer parse (never for rows this engine
/// wrote).
pub fn rebuild(
    fmt: u8,
    exchange: String,
    routing_key: String,
    props: Bytes,
    body: Bytes,
) -> Result<Message, MqError> {
    if fmt == 1 {
        let mut all = BytesMut::with_capacity(props.len() + body.len());
        all.extend_from_slice(&props);
        all.extend_from_slice(&body);
        let s = Sections::parse(&all.freeze())
            .map_err(|e| MqError::precondition(format!("stored 1.0 message: {}", e.0)))?;
        Ok(convert::from_10(exchange, routing_key, &s))
    } else {
        let p = BasicProperties::decode(&props)?;
        convert::from_091(exchange, routing_key, props, &p, body)
    }
}

/// Encodes an `x-death` array.
#[must_use]
pub fn encode_deaths(d: &FieldValue) -> Bytes {
    let mut b = BytesMut::new();
    FieldTable::from([("x-death", d.clone())]).encode_entries(&mut b);
    b.freeze()
}

/// Decodes an `x-death` array.
#[must_use]
pub fn decode_deaths(b: &[u8]) -> Option<FieldValue> {
    FieldTable::decode_entries(b, 0)
        .ok()
        .and_then(|t| t.get("x-death").cloned())
}

/// A message whose TTL and `expiration` are cleared (a dead letter, so it
/// doesn't expire again at once): the stored properties are kept, only
/// the engine's reading of them changes.
#[must_use]
pub fn without_ttl(m: &Arc<Message>) -> Arc<Message> {
    if m.info.ttl_ms.is_none() {
        return Arc::clone(m);
    }
    let mut c = (**m).clone();
    c.info.ttl_ms = None;
    Arc::new(c)
}

/// The `x-delay` header in milliseconds (`None`: route at once).
#[must_use]
pub fn delay_ms(m: &Message) -> Option<i64> {
    m.info
        .headers
        .get("x-delay")
        .and_then(FieldValue::as_i64)
        .filter(|d| *d > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rebuilds_091_and_deaths() {
        let p = BasicProperties {
            priority: Some(3),
            expiration: Some("500".into()),
            headers: Some(FieldTable::from([("x-delay", FieldValue::I32(20))])),
            ..BasicProperties::default()
        };
        let mut b = BytesMut::new();
        p.encode(&mut b);
        let raw = b.freeze();
        let m = rebuild(0, "x".into(), "k".into(), raw, Bytes::from_static(b"b"))
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(m.info.priority, Some(3));
        assert_eq!(m.info.ttl_ms, Some(500));
        assert_eq!(delay_ms(&m), Some(20));
        let d = FieldValue::Array(vec![FieldValue::Table(FieldTable::from([(
            "count",
            FieldValue::I64(2),
        )]))]);
        assert_eq!(decode_deaths(&encode_deaths(&d)), Some(d));
    }
}
