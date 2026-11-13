//! The queue and AMQP error numbers (39250–39289) and how they reach AMQP
//! clients: a 0-9-1 reply code, or a 1.0 condition symbol, with SprigDB's
//! number and name in the text.

use std::fmt;

use sprig_common::SprigCode;

/// AMQP 0-9-1 reply codes (the spec's constants, as RabbitMQ uses them).
pub mod reply {
    /// `reply-success`.
    pub const SUCCESS: u16 = 200;
    /// `content-too-large`.
    pub const CONTENT_TOO_LARGE: u16 = 311;
    /// `no-route` (basic.return).
    pub const NO_ROUTE: u16 = 312;
    /// `no-consumers`.
    pub const NO_CONSUMERS: u16 = 313;
    /// `connection-forced`.
    pub const CONNECTION_FORCED: u16 = 320;
    /// `invalid-path`.
    pub const INVALID_PATH: u16 = 402;
    /// `access-refused`.
    pub const ACCESS_REFUSED: u16 = 403;
    /// `not-found`.
    pub const NOT_FOUND: u16 = 404;
    /// `resource-locked`.
    pub const RESOURCE_LOCKED: u16 = 405;
    /// `precondition-failed`.
    pub const PRECONDITION_FAILED: u16 = 406;
    /// `frame-error`.
    pub const FRAME_ERROR: u16 = 501;
    /// `syntax-error`.
    pub const SYNTAX_ERROR: u16 = 502;
    /// `command-invalid`.
    pub const COMMAND_INVALID: u16 = 503;
    /// `channel-error`.
    pub const CHANNEL_ERROR: u16 = 504;
    /// `unexpected-frame`.
    pub const UNEXPECTED_FRAME: u16 = 505;
    /// `resource-error`.
    pub const RESOURCE_ERROR: u16 = 506;
    /// `not-allowed`.
    pub const NOT_ALLOWED: u16 = 530;
    /// `not-implemented`.
    pub const NOT_IMPLEMENTED: u16 = 540;
    /// `internal-error`.
    pub const INTERNAL_ERROR: u16 = 541;

    /// Whether a reply code closes the connection (a "hard" error) rather
    /// than the channel.
    #[must_use]
    pub const fn is_hard(code: u16) -> bool {
        matches!(code, 320 | 402 | 501..=506 | 530 | 540 | 541)
    }

    /// The spec's name, as RabbitMQ prefixes reply texts with it.
    #[must_use]
    pub const fn name(code: u16) -> &'static str {
        match code {
            200 => "OK",
            311 => "CONTENT_TOO_LARGE",
            312 => "NO_ROUTE",
            313 => "NO_CONSUMERS",
            320 => "CONNECTION_FORCED",
            402 => "INVALID_PATH",
            403 => "ACCESS_REFUSED",
            404 => "NOT_FOUND",
            405 => "RESOURCE_LOCKED",
            406 => "PRECONDITION_FAILED",
            501 => "FRAME_ERROR",
            502 => "SYNTAX_ERROR",
            503 => "COMMAND_INVALID",
            504 => "CHANNEL_ERROR",
            505 => "UNEXPECTED_FRAME",
            506 => "RESOURCE_ERROR",
            530 => "NOT_ALLOWED",
            540 => "NOT_IMPLEMENTED",
            _ => "INTERNAL_ERROR",
        }
    }
}

/// AMQP 1.0 error conditions SprigDB sends.
pub mod condition {
    /// `amqp:internal-error`.
    pub const INTERNAL_ERROR: &str = "amqp:internal-error";
    /// `amqp:not-found`.
    pub const NOT_FOUND: &str = "amqp:not-found";
    /// `amqp:unauthorized-access`.
    pub const UNAUTHORIZED_ACCESS: &str = "amqp:unauthorized-access";
    /// `amqp:decode-error`.
    pub const DECODE_ERROR: &str = "amqp:decode-error";
    /// `amqp:resource-limit-exceeded`.
    pub const RESOURCE_LIMIT_EXCEEDED: &str = "amqp:resource-limit-exceeded";
    /// `amqp:not-allowed`.
    pub const NOT_ALLOWED: &str = "amqp:not-allowed";
    /// `amqp:invalid-field`.
    pub const INVALID_FIELD: &str = "amqp:invalid-field";
    /// `amqp:not-implemented`.
    pub const NOT_IMPLEMENTED: &str = "amqp:not-implemented";
    /// `amqp:resource-locked`.
    pub const RESOURCE_LOCKED: &str = "amqp:resource-locked";
    /// `amqp:precondition-failed`.
    pub const PRECONDITION_FAILED: &str = "amqp:precondition-failed";
    /// `amqp:resource-deleted`.
    pub const RESOURCE_DELETED: &str = "amqp:resource-deleted";
    /// `amqp:frame-size-too-small`.
    pub const FRAME_SIZE_TOO_SMALL: &str = "amqp:frame-size-too-small";
    /// `amqp:connection:forced`.
    pub const CONNECTION_FORCED: &str = "amqp:connection:forced";
    /// `amqp:connection:framing-error`.
    pub const FRAMING_ERROR: &str = "amqp:connection:framing-error";
    /// `amqp:session:window-violation`.
    pub const WINDOW_VIOLATION: &str = "amqp:session:window-violation";
    /// `amqp:session:unattached-handle`.
    pub const UNATTACHED_HANDLE: &str = "amqp:session:unattached-handle";
    /// `amqp:session:handle-in-use`.
    pub const HANDLE_IN_USE: &str = "amqp:session:handle-in-use";
    /// `amqp:link:detach-forced`.
    pub const DETACH_FORCED: &str = "amqp:link:detach-forced";
    /// `amqp:link:message-size-exceeded`.
    pub const MESSAGE_SIZE_EXCEEDED: &str = "amqp:link:message-size-exceeded";
    /// `amqp:link:transfer-limit-exceeded`.
    pub const TRANSFER_LIMIT_EXCEEDED: &str = "amqp:link:transfer-limit-exceeded";
}

/// Chapter 50's error numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
pub enum MqCode {
    NotFound,
    Precondition,
    ResourceLocked,
    AccessRefused,
    NotImplemented,
    Unroutable,
    QueueFull,
    MessageTooLarge,
    AlreadySettled,
    DeliveryTokenInvalid,
    OwnerMoved,
    ConfirmTimeout,
    ConsumerTimeout,
    TxnNeedsXa,
    DelayOutOfRange,
    Residency,
    Quota,
    VhostLimit,
    FrameError,
    SaslFailed,
    ProtocolHeader,
    ChannelLimit,
    LinkRefused,
    Amqp10Unsupported,
    StreamOffset,
    DirectReplyTo,
    MgmtUnsupported,
    DeadLetterCycle,
}

impl MqCode {
    /// Every code, in number order.
    pub const ALL: [MqCode; 28] = [
        MqCode::NotFound,
        MqCode::Precondition,
        MqCode::ResourceLocked,
        MqCode::AccessRefused,
        MqCode::NotImplemented,
        MqCode::Unroutable,
        MqCode::QueueFull,
        MqCode::MessageTooLarge,
        MqCode::AlreadySettled,
        MqCode::DeliveryTokenInvalid,
        MqCode::OwnerMoved,
        MqCode::ConfirmTimeout,
        MqCode::ConsumerTimeout,
        MqCode::TxnNeedsXa,
        MqCode::DelayOutOfRange,
        MqCode::Residency,
        MqCode::Quota,
        MqCode::VhostLimit,
        MqCode::FrameError,
        MqCode::SaslFailed,
        MqCode::ProtocolHeader,
        MqCode::ChannelLimit,
        MqCode::LinkRefused,
        MqCode::Amqp10Unsupported,
        MqCode::StreamOffset,
        MqCode::DirectReplyTo,
        MqCode::MgmtUnsupported,
        MqCode::DeadLetterCycle,
    ];

    /// The code of a SprigDB error number.
    #[must_use]
    pub fn from_number(n: u32) -> Option<Self> {
        Self::ALL.into_iter().find(|c| c.number() == n)
    }

    /// The registry's code (`SprigCode`), which holds the number and name.
    #[must_use]
    pub const fn sprig(self) -> SprigCode {
        match self {
            MqCode::NotFound => SprigCode::MqNotFound,
            MqCode::Precondition => SprigCode::MqPrecondition,
            MqCode::ResourceLocked => SprigCode::MqResourceLocked,
            MqCode::AccessRefused => SprigCode::MqAccessRefused,
            MqCode::NotImplemented => SprigCode::MqNotImplemented,
            MqCode::Unroutable => SprigCode::MqUnroutable,
            MqCode::QueueFull => SprigCode::MqQueueFull,
            MqCode::MessageTooLarge => SprigCode::MqMessageTooLarge,
            MqCode::AlreadySettled => SprigCode::MqAlreadySettled,
            MqCode::DeliveryTokenInvalid => SprigCode::MqDeliveryTokenInvalid,
            MqCode::OwnerMoved => SprigCode::MqOwnerMoved,
            MqCode::ConfirmTimeout => SprigCode::MqConfirmTimeout,
            MqCode::ConsumerTimeout => SprigCode::MqConsumerTimeout,
            MqCode::TxnNeedsXa => SprigCode::MqTxnNeedsXa,
            MqCode::DelayOutOfRange => SprigCode::MqDelayOutOfRange,
            MqCode::Residency => SprigCode::MqResidency,
            MqCode::Quota => SprigCode::MqQuota,
            MqCode::VhostLimit => SprigCode::MqVhostLimit,
            MqCode::FrameError => SprigCode::AmqpFrameError,
            MqCode::SaslFailed => SprigCode::AmqpSaslFailed,
            MqCode::ProtocolHeader => SprigCode::AmqpProtocolHeader,
            MqCode::ChannelLimit => SprigCode::AmqpChannelLimit,
            MqCode::LinkRefused => SprigCode::Amqp10LinkRefused,
            MqCode::Amqp10Unsupported => SprigCode::Amqp10Unsupported,
            MqCode::StreamOffset => SprigCode::MqStreamOffset,
            MqCode::DirectReplyTo => SprigCode::MqDirectReplyTo,
            MqCode::MgmtUnsupported => SprigCode::MqMgmtUnsupported,
            MqCode::DeadLetterCycle => SprigCode::MqDeadLetterCycle,
        }
    }

    /// The SprigDB error number.
    #[must_use]
    pub const fn number(self) -> u32 {
        self.sprig().number() as u32
    }

    /// The `ER_SPRIG_*` name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        self.sprig().name()
    }

    /// The 0-9-1 reply code an error of this number gets by default.
    #[must_use]
    pub const fn default_reply(self) -> u16 {
        match self {
            MqCode::NotFound => reply::NOT_FOUND,
            MqCode::ResourceLocked => reply::RESOURCE_LOCKED,
            MqCode::AccessRefused | MqCode::SaslFailed | MqCode::Residency => reply::ACCESS_REFUSED,
            MqCode::NotImplemented | MqCode::Amqp10Unsupported | MqCode::MgmtUnsupported => {
                reply::NOT_IMPLEMENTED
            }
            MqCode::Unroutable => reply::NO_ROUTE,
            MqCode::FrameError | MqCode::ProtocolHeader => reply::FRAME_ERROR,
            MqCode::ChannelLimit => reply::NOT_ALLOWED,
            MqCode::OwnerMoved | MqCode::TxnNeedsXa | MqCode::DeadLetterCycle => {
                reply::INTERNAL_ERROR
            }
            _ => reply::PRECONDITION_FAILED,
        }
    }

    /// The AMQP 1.0 condition an error of this number gets by default.
    #[must_use]
    pub const fn default_condition(self) -> &'static str {
        match self {
            MqCode::NotFound | MqCode::LinkRefused => condition::NOT_FOUND,
            MqCode::AccessRefused | MqCode::SaslFailed | MqCode::Residency => {
                condition::UNAUTHORIZED_ACCESS
            }
            MqCode::NotImplemented | MqCode::Amqp10Unsupported => condition::NOT_IMPLEMENTED,
            MqCode::ResourceLocked => condition::RESOURCE_LOCKED,
            MqCode::MessageTooLarge => condition::MESSAGE_SIZE_EXCEEDED,
            MqCode::FrameError | MqCode::ProtocolHeader => condition::FRAMING_ERROR,
            MqCode::QueueFull | MqCode::Quota | MqCode::VhostLimit | MqCode::ChannelLimit => {
                condition::RESOURCE_LIMIT_EXCEEDED
            }
            MqCode::OwnerMoved | MqCode::TxnNeedsXa | MqCode::DeadLetterCycle => {
                condition::INTERNAL_ERROR
            }
            _ => condition::PRECONDITION_FAILED,
        }
    }
}

/// An error of the queue model or of a protocol front end.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub struct MqError {
    /// SprigDB's number.
    pub code: MqCode,
    /// The 0-9-1 reply code.
    pub reply: u16,
    /// The text, without the reply-code name or SprigDB's number.
    pub text: String,
}

impl fmt::Display for MqError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} - {} ({} {})",
            reply::name(self.reply),
            self.text,
            self.code.number(),
            self.code.name()
        )
    }
}

impl MqError {
    /// An error with the code's default reply code.
    pub fn new(code: MqCode, text: impl Into<String>) -> Self {
        Self {
            code,
            reply: code.default_reply(),
            text: text.into(),
        }
    }

    /// The same error with another 0-9-1 reply code.
    #[must_use]
    pub fn with_reply(mut self, reply: u16) -> Self {
        self.reply = reply;
        self
    }

    /// 404 for an unknown object.
    pub fn not_found(text: impl Into<String>) -> Self {
        Self::new(MqCode::NotFound, text)
    }

    /// 406.
    pub fn precondition(text: impl Into<String>) -> Self {
        Self::new(MqCode::Precondition, text)
    }

    /// 403.
    pub fn access_refused(text: impl Into<String>) -> Self {
        Self::new(MqCode::AccessRefused, text)
    }

    /// 405.
    pub fn locked(text: impl Into<String>) -> Self {
        Self::new(MqCode::ResourceLocked, text)
    }

    /// 540.
    pub fn not_implemented(text: impl Into<String>) -> Self {
        Self::new(MqCode::NotImplemented, text)
    }

    /// A framing or command error with a given hard reply code (501, 502,
    /// 503, 504, 505).
    pub fn frame(reply: u16, text: impl Into<String>) -> Self {
        Self::new(MqCode::FrameError, text).with_reply(reply)
    }

    /// The reply text a client sees: RabbitMQ's `NAME - text` form plus
    /// SprigDB's number and name, cut to fit a short string.
    #[must_use]
    pub fn reply_text(&self) -> String {
        truncate(self.to_string(), 255)
    }

    /// The 1.0 description: the same text.
    #[must_use]
    pub fn description(&self) -> String {
        self.to_string()
    }

    /// Whether the 0-9-1 reply code closes the connection.
    #[must_use]
    pub fn is_hard(&self) -> bool {
        reply::is_hard(self.reply)
    }
}

/// Cuts `s` to at most `max` bytes on a character boundary.
#[must_use]
pub fn truncate(mut s: String, max: usize) -> String {
    if s.len() > max {
        let mut end = max;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        s.truncate(end);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_and_text() {
        assert_eq!(MqCode::NotFound.number(), 39250);
        assert_eq!(MqCode::DeadLetterCycle.number(), 39277);
        assert_eq!(MqCode::ProtocolHeader.number(), 39270);
        let e = MqError::not_found("no queue 'q' in vhost '/'");
        assert_eq!(
            e.reply_text(),
            "NOT_FOUND - no queue 'q' in vhost '/' (39250 ER_SPRIG_MQ_NOT_FOUND)"
        );
        assert!(!e.is_hard());
        assert!(MqError::frame(505, "x").is_hard());
        let long = MqError::precondition("é".repeat(300));
        assert!(long.reply_text().len() <= 255);
    }
}
