//! The `basic` class's content properties: 14 optional fields announced by
//! flag bits 15 down to 2 of the property flags (bit 0 continues the flags
//! into another short, which `basic` never needs).

use bytes::{Bytes, BytesMut};

use super::codec::{Dec, Enc, FieldTable};
use crate::error::{MqError, reply};

/// The `basic` content properties.
#[derive(Debug, Clone, Default, PartialEq)]
#[allow(missing_docs)]
pub struct BasicProperties {
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub headers: Option<FieldTable>,
    pub delivery_mode: Option<u8>,
    pub priority: Option<u8>,
    pub correlation_id: Option<String>,
    pub reply_to: Option<String>,
    pub expiration: Option<String>,
    pub message_id: Option<String>,
    /// Seconds since the epoch.
    pub timestamp: Option<u64>,
    pub kind: Option<String>,
    pub user_id: Option<String>,
    pub app_id: Option<String>,
    pub cluster_id: Option<String>,
}

/// Decodes a short string property leniently: properties are kept as sent
/// and only read for routing and conversion, so invalid UTF-8 is replaced
/// rather than refused.
fn lossy(d: &mut Dec<'_>) -> Result<String, MqError> {
    Ok(String::from_utf8_lossy(d.shortstr_bytes()?).into_owned())
}

impl BasicProperties {
    /// Encodes the flags and the present properties.
    pub fn encode(&self, buf: &mut BytesMut) {
        let mut flags: u16 = 0;
        let bits = [
            self.content_type.is_some(),
            self.content_encoding.is_some(),
            self.headers.is_some(),
            self.delivery_mode.is_some(),
            self.priority.is_some(),
            self.correlation_id.is_some(),
            self.reply_to.is_some(),
            self.expiration.is_some(),
            self.message_id.is_some(),
            self.timestamp.is_some(),
            self.kind.is_some(),
            self.user_id.is_some(),
            self.app_id.is_some(),
            self.cluster_id.is_some(),
        ];
        for (i, b) in bits.iter().enumerate() {
            if *b {
                flags |= 1 << (15 - i);
            }
        }
        let mut e = Enc::new(buf);
        e.u16(flags);
        let s = |e: &mut Enc<'_>, v: &Option<String>| {
            if let Some(v) = v {
                e.shortstr(v);
            }
        };
        s(&mut e, &self.content_type);
        s(&mut e, &self.content_encoding);
        if let Some(h) = &self.headers {
            e.table(h);
        }
        if let Some(v) = self.delivery_mode {
            e.u8(v);
        }
        if let Some(v) = self.priority {
            e.u8(v);
        }
        s(&mut e, &self.correlation_id);
        s(&mut e, &self.reply_to);
        s(&mut e, &self.expiration);
        s(&mut e, &self.message_id);
        if let Some(v) = self.timestamp {
            e.u64(v);
        }
        s(&mut e, &self.kind);
        s(&mut e, &self.user_id);
        s(&mut e, &self.app_id);
        s(&mut e, &self.cluster_id);
    }

    /// The encoding as bytes.
    #[must_use]
    pub fn to_bytes(&self) -> Bytes {
        let mut b = BytesMut::new();
        self.encode(&mut b);
        b.freeze()
    }

    /// Decodes the flags and properties.
    ///
    /// # Errors
    /// 502 for malformed properties.
    pub fn decode(raw: &[u8]) -> Result<Self, MqError> {
        let mut d = Dec::new(raw);
        let mut flags = d.u16()?;
        let first = flags;
        // Further flag words (bit 0 set) carry nothing for `basic`.
        while flags & 1 != 0 {
            flags = d.u16()?;
        }
        let has = |bit: u16| first & (1 << bit) != 0;
        let mut p = BasicProperties::default();
        if has(15) {
            p.content_type = Some(lossy(&mut d)?);
        }
        if has(14) {
            p.content_encoding = Some(lossy(&mut d)?);
        }
        if has(13) {
            p.headers = Some(d.table()?);
        }
        if has(12) {
            p.delivery_mode = Some(d.u8()?);
        }
        if has(11) {
            p.priority = Some(d.u8()?);
        }
        if has(10) {
            p.correlation_id = Some(lossy(&mut d)?);
        }
        if has(9) {
            p.reply_to = Some(lossy(&mut d)?);
        }
        if has(8) {
            p.expiration = Some(lossy(&mut d)?);
        }
        if has(7) {
            p.message_id = Some(lossy(&mut d)?);
        }
        if has(6) {
            p.timestamp = Some(d.u64()?);
        }
        if has(5) {
            p.kind = Some(lossy(&mut d)?);
        }
        if has(4) {
            p.user_id = Some(lossy(&mut d)?);
        }
        if has(3) {
            p.app_id = Some(lossy(&mut d)?);
        }
        if has(2) {
            p.cluster_id = Some(lossy(&mut d)?);
        }
        if d.remaining() != 0 {
            return Err(MqError::frame(
                reply::SYNTAX_ERROR,
                "trailing bytes after content properties",
            ));
        }
        Ok(p)
    }

    /// The per-message TTL in milliseconds, from `expiration`.
    ///
    /// # Errors
    /// 406 for an `expiration` that isn't a non-negative integer, as
    /// RabbitMQ refuses it.
    pub fn ttl_ms(&self) -> Result<Option<u64>, MqError> {
        match &self.expiration {
            None => Ok(None),
            Some(s) => s.parse::<u64>().map(Some).map_err(|_| {
                MqError::precondition(format!("invalid expiration '{s}'"))
                    .with_reply(reply::PRECONDITION_FAILED)
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v091::codec::FieldValue;

    #[test]
    fn golden_content_type_and_delivery_mode() {
        let p = BasicProperties {
            content_type: Some("text/plain".into()),
            delivery_mode: Some(2),
            ..Default::default()
        };
        let b = p.to_bytes();
        let mut want = vec![0b1001_0000, 0, 10];
        want.extend_from_slice(b"text/plain");
        want.push(2);
        assert_eq!(&b[..], &want[..]);
        assert_eq!(BasicProperties::decode(&b).unwrap(), p);
    }

    #[test]
    fn all_fields_round_trip() {
        let p = BasicProperties {
            content_type: Some("a".into()),
            content_encoding: Some("b".into()),
            headers: Some(FieldTable::from([("h", FieldValue::I32(1))])),
            delivery_mode: Some(1),
            priority: Some(9),
            correlation_id: Some("c".into()),
            reply_to: Some("r".into()),
            expiration: Some("60000".into()),
            message_id: Some("m".into()),
            timestamp: Some(1_700_000_000),
            kind: Some("t".into()),
            user_id: Some("u".into()),
            app_id: Some("app".into()),
            cluster_id: Some("cl".into()),
        };
        let b = p.to_bytes();
        assert_eq!(&b[..2], &[0xff, 0xfc]);
        assert_eq!(BasicProperties::decode(&b).unwrap(), p);
        assert_eq!(p.ttl_ms().unwrap(), Some(60_000));
        let bad = BasicProperties {
            expiration: Some("-1".into()),
            ..Default::default()
        };
        assert_eq!(bad.ttl_ms().unwrap_err().reply, 406);
    }
}
