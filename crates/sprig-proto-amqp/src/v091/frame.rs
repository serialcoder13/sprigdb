//! AMQP 0-9-1 framing: `type:u8 channel:u16 size:u32 payload frame-end`,
//! with frame-end `0xCE`.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::method::Method;
use super::props::BasicProperties;
use crate::error::{MqError, reply};

/// The 0-9-1 protocol header.
pub const PROTOCOL_HEADER: [u8; 8] = *b"AMQP\x00\x00\x09\x01";
/// Frame type: method.
pub const FRAME_METHOD: u8 = 1;
/// Frame type: content header.
pub const FRAME_HEADER: u8 = 2;
/// Frame type: content body.
pub const FRAME_BODY: u8 = 3;
/// Frame type: heartbeat.
pub const FRAME_HEARTBEAT: u8 = 8;
/// The frame-end octet.
pub const FRAME_END: u8 = 0xCE;
/// Frame header plus frame-end.
pub const FRAME_OVERHEAD: usize = 8;
/// The smallest `frame_max` a peer may negotiate (the spec's
/// `frame-min-size`).
pub const FRAME_MIN_SIZE: u32 = 4096;

/// One frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// The frame type.
    pub kind: u8,
    /// The channel.
    pub channel: u16,
    /// The payload.
    pub payload: Bytes,
}

impl Frame {
    /// Takes one whole frame from the front of `buf`, if it holds one.
    /// `frame_max` bounds the whole frame (header, payload and frame-end),
    /// as the spec counts it; 0 means no limit.
    ///
    /// # Errors
    /// 501 `frame-error` for a frame above `frame_max` or a bad frame-end.
    pub fn parse(buf: &mut BytesMut, frame_max: u32) -> Result<Option<Frame>, MqError> {
        if buf.len() < 7 {
            return Ok(None);
        }
        let size = u32::from_be_bytes([buf[3], buf[4], buf[5], buf[6]]) as usize;
        if frame_max != 0 && size + FRAME_OVERHEAD > frame_max as usize {
            return Err(MqError::frame(
                reply::FRAME_ERROR,
                format!(
                    "frame size {} exceeds frame_max {frame_max}",
                    size + FRAME_OVERHEAD
                ),
            ));
        }
        if buf.len() < size + FRAME_OVERHEAD {
            buf.reserve(size + FRAME_OVERHEAD - buf.len());
            return Ok(None);
        }
        if buf[7 + size] != FRAME_END {
            return Err(MqError::frame(
                reply::FRAME_ERROR,
                format!("bad frame-end octet {:#04x}", buf[7 + size]),
            ));
        }
        let kind = buf[0];
        let channel = u16::from_be_bytes([buf[1], buf[2]]);
        buf.advance(7);
        let payload = buf.split_to(size).freeze();
        buf.advance(1);
        Ok(Some(Frame {
            kind,
            channel,
            payload,
        }))
    }

    /// Encodes the frame.
    pub fn encode(&self, buf: &mut BytesMut) {
        put_frame(buf, self.kind, self.channel, |b| b.put_slice(&self.payload));
    }
}

/// Writes a frame whose payload `f` writes.
pub fn put_frame(buf: &mut BytesMut, kind: u8, channel: u16, f: impl FnOnce(&mut BytesMut)) {
    buf.put_u8(kind);
    buf.put_u16(channel);
    let at = buf.len();
    buf.put_u32(0);
    f(buf);
    #[allow(clippy::cast_possible_truncation)]
    let n = (buf.len() - at - 4) as u32;
    buf[at..at + 4].copy_from_slice(&n.to_be_bytes());
    buf.put_u8(FRAME_END);
}

/// Writes a method frame.
pub fn put_method(buf: &mut BytesMut, channel: u16, m: &Method) {
    put_frame(buf, FRAME_METHOD, channel, |b| m.encode(b));
}

/// Writes a heartbeat frame.
pub fn put_heartbeat(buf: &mut BytesMut) {
    put_frame(buf, FRAME_HEARTBEAT, 0, |_| {});
}

/// Writes a content header (class `basic`) whose property section is
/// `props` (flags and list, as [`BasicProperties::encode`] writes them or
/// as a publisher sent them).
pub fn put_content_header(buf: &mut BytesMut, channel: u16, body_size: u64, props: &[u8]) {
    put_frame(buf, FRAME_HEADER, channel, |b| {
        b.put_u16(60);
        b.put_u16(0);
        b.put_u64(body_size);
        b.put_slice(props);
    });
}

/// Writes a method with content: the method, the header, and body frames
/// of at most `frame_max - 8` bytes each.
pub fn put_content(
    buf: &mut BytesMut,
    channel: u16,
    m: &Method,
    props: &[u8],
    body: &[u8],
    frame_max: u32,
) {
    put_method(buf, channel, m);
    put_content_header(buf, channel, body.len() as u64, props);
    let max = if frame_max == 0 {
        usize::MAX
    } else {
        (frame_max as usize).saturating_sub(FRAME_OVERHEAD).max(1)
    };
    for chunk in body.chunks(max) {
        put_frame(buf, FRAME_BODY, channel, |b| b.put_slice(chunk));
    }
}

/// A decoded content header.
#[derive(Debug, Clone, PartialEq)]
pub struct ContentHeader {
    /// The class (60 for `basic`).
    pub class_id: u16,
    /// The body size.
    pub body_size: u64,
    /// The raw property section (flags and list), kept byte for byte.
    pub raw_props: Bytes,
    /// The decoded properties.
    pub props: BasicProperties,
}

impl ContentHeader {
    /// Decodes a content header frame's payload.
    ///
    /// # Errors
    /// 502 for a malformed header.
    pub fn decode(payload: &Bytes) -> Result<Self, MqError> {
        if payload.len() < 14 {
            return Err(MqError::frame(
                reply::SYNTAX_ERROR,
                "truncated content header",
            ));
        }
        let class_id = u16::from_be_bytes([payload[0], payload[1]]);
        let mut a = [0u8; 8];
        a.copy_from_slice(&payload[4..12]);
        let body_size = u64::from_be_bytes(a);
        let raw_props = payload.slice(12..);
        let props = BasicProperties::decode(&raw_props)?;
        Ok(Self {
            class_id,
            body_size,
            raw_props,
            props,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v091::method::*;

    #[test]
    fn heartbeat_golden() {
        let mut b = BytesMut::new();
        put_heartbeat(&mut b);
        assert_eq!(&b[..], &[8, 0, 0, 0, 0, 0, 0, 0xCE]);
    }

    #[test]
    fn tune_ok_golden() {
        // connection.tune-ok channel_max=2047 frame_max=131072 heartbeat=60.
        let mut b = BytesMut::new();
        put_method(
            &mut b,
            0,
            &ConnectionTuneOk {
                channel_max: 2047,
                frame_max: 131_072,
                heartbeat: 60,
            }
            .into(),
        );
        let want: &[u8] = &[
            1, 0, 0, 0, 0, 0, 12, // method frame, channel 0, size 12
            0, 10, 0, 31, // connection.tune-ok
            0x07, 0xff, 0, 2, 0, 0, 0, 60, 0xCE,
        ];
        assert_eq!(&b[..], want);
        let f = Frame::parse(&mut b, 0).unwrap().unwrap();
        assert_eq!((f.kind, f.channel), (1, 0));
        assert!(b.is_empty());
        assert_eq!(
            Method::decode(&f.payload).unwrap(),
            Method::ConnectionTuneOk(ConnectionTuneOk {
                channel_max: 2047,
                frame_max: 131_072,
                heartbeat: 60
            })
        );
    }

    #[test]
    fn partial_and_bad_frames() {
        let mut b = BytesMut::from(&[1u8, 0, 1, 0, 0, 0, 4, 0, 20][..]);
        assert_eq!(Frame::parse(&mut b, 0).unwrap(), None);
        b.extend_from_slice(&[0, 10, 0xCE]);
        let f = Frame::parse(&mut b, 0).unwrap().unwrap();
        assert_eq!(f.channel, 1);
        let mut bad = BytesMut::from(&[1u8, 0, 1, 0, 0, 0, 0, 0xAB][..]);
        assert_eq!(Frame::parse(&mut bad, 0).unwrap_err().reply, 501);
        let mut big = BytesMut::from(&[3u8, 0, 1, 0, 0, 0x20, 0][..]);
        assert_eq!(Frame::parse(&mut big, 4096).unwrap_err().reply, 501);
    }

    #[test]
    fn content_splits_bodies() {
        let mut b = BytesMut::new();
        let body = vec![7u8; 10_000];
        put_content(
            &mut b,
            3,
            &BasicPublish {
                exchange: "x".into(),
                routing_key: "k".into(),
                ..Default::default()
            }
            .into(),
            &[0, 0],
            &body,
            4096,
        );
        let mut frames = Vec::new();
        while let Some(f) = Frame::parse(&mut b, 4096).unwrap() {
            frames.push(f);
        }
        assert_eq!(frames.len(), 2 + 3);
        assert!(frames[2..].iter().all(|f| f.payload.len() <= 4088));
        let h = ContentHeader::decode(&frames[1].payload).unwrap();
        assert_eq!((h.class_id, h.body_size), (60, 10_000));
    }

    #[test]
    fn publish_golden() {
        // basic.publish exchange="" routing_key="q" mandatory=true.
        let m: Method = BasicPublish {
            ticket: 0,
            exchange: String::new(),
            routing_key: "q".into(),
            mandatory: true,
            immediate: false,
        }
        .into();
        let mut b = BytesMut::new();
        m.encode(&mut b);
        assert_eq!(&b[..], &[0, 60, 0, 40, 0, 0, 0, 1, b'q', 0b01]);
    }
}
