//! The protocol sniffer on 5672 and 5671 (chapter 04, "Protocol detection
//! on the AMQP ports"; chapter 50 step 23): the client's 8-byte protocol
//! header picks the front end.
//!
//! | Header | Front end |
//! |---|---|
//! | `AMQP 0 0 9 1` | AMQP 0-9-1 |
//! | `AMQP 3 1 0 0` | AMQP 1.0, SASL layer first |
//! | `AMQP 0 1 0 0` | 1.0 without SASL: answered `AMQP 3 1 0 0`, closed |
//! | `AMQP 2 1 0 0` | 1.0's TLS upgrade: answered `AMQP 3 1 0 0`, closed |
//! | anything else | answered `AMQP 0 0 9 1`, closed (39270) |
//!
//! An MQTT front end would be added here by recognizing `CONNECT` (first
//! byte `0x10`) on its own ports.

/// The 0-9-1 header.
pub const AMQP_091: [u8; 8] = *b"AMQP\x00\x00\x09\x01";
/// The 1.0 SASL layer header.
pub const AMQP_10_SASL: [u8; 8] = *b"AMQP\x03\x01\x00\x00";
/// The 1.0 AMQP layer header.
pub const AMQP_10: [u8; 8] = *b"AMQP\x00\x01\x00\x00";
/// The 1.0 TLS layer header.
pub const AMQP_10_TLS: [u8; 8] = *b"AMQP\x02\x01\x00\x00";

/// What a protocol header asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Detected {
    /// AMQP 0-9-1.
    Amqp091,
    /// AMQP 1.0 with SASL.
    Amqp10Sasl,
    /// AMQP 1.0 without SASL (refused: SASL is required).
    Amqp10NoSasl,
    /// AMQP 1.0's in-protocol TLS (refused, as in RabbitMQ).
    Amqp10Tls,
    /// Anything else.
    Unknown,
}

impl Detected {
    /// Classifies a header.
    #[must_use]
    pub fn of(h: &[u8; 8]) -> Self {
        match *h {
            AMQP_091 => Detected::Amqp091,
            AMQP_10_SASL => Detected::Amqp10Sasl,
            AMQP_10 => Detected::Amqp10NoSasl,
            AMQP_10_TLS => Detected::Amqp10Tls,
            _ => Detected::Unknown,
        }
    }

    /// The header to answer with before closing, for refused headers.
    #[must_use]
    pub fn refusal(self) -> Option<[u8; 8]> {
        match self {
            Detected::Amqp091 | Detected::Amqp10Sasl => None,
            Detected::Amqp10NoSasl | Detected::Amqp10Tls => Some(AMQP_10_SASL),
            Detected::Unknown => Some(AMQP_091),
        }
    }

    /// The metric label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Detected::Amqp091 => "0-9-1",
            Detected::Amqp10Sasl | Detected::Amqp10NoSasl | Detected::Amqp10Tls => "1.0",
            Detected::Unknown => "unknown",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table() {
        assert_eq!(Detected::of(b"AMQP\x00\x00\x09\x01"), Detected::Amqp091);
        assert_eq!(Detected::of(b"AMQP\x03\x01\x00\x00"), Detected::Amqp10Sasl);
        assert_eq!(
            Detected::of(b"AMQP\x00\x01\x00\x00"),
            Detected::Amqp10NoSasl
        );
        assert_eq!(Detected::of(b"AMQP\x02\x01\x00\x00"), Detected::Amqp10Tls);
        // 0-9 and 0-8 headers, HTTP, TLS hellos: unknown.
        assert_eq!(Detected::of(b"AMQP\x01\x01\x00\x09"), Detected::Unknown);
        assert_eq!(Detected::of(b"GET / HT"), Detected::Unknown);
        assert_eq!(
            Detected::of(&[0x16, 3, 1, 0, 0, 0, 0, 0]),
            Detected::Unknown
        );
        assert_eq!(Detected::Unknown.refusal(), Some(AMQP_091));
        assert_eq!(Detected::Amqp10NoSasl.refusal(), Some(AMQP_10_SASL));
        assert_eq!(Detected::Amqp10Tls.refusal(), Some(AMQP_10_SASL));
        assert_eq!(Detected::Amqp091.refusal(), None);
    }
}
