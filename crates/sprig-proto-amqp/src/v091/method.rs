//! Every AMQP 0-9-1 method of the classes RabbitMQ serves: `connection`,
//! `channel`, `exchange`, `queue`, `basic`, `confirm` and `tx`, with
//! RabbitMQ's extensions (`connection.blocked`, `connection.update-secret`,
//! `exchange.bind`, `basic.nack`, `confirm.select`).
//!
//! Fields follow the 0-9-1 XML spec in order; the deprecated `reserved`
//! fields (tickets, `insist`, `known-hosts`, `out-of-band`, `cluster-id`)
//! keep their place so the byte layout is the spec's.

use bytes::{BufMut, Bytes, BytesMut};

use super::codec::{Arg, DResult, Dec, DecodeError, Enc, FieldTable};

macro_rules! methods {
    ($(
        $(#[$doc:meta])*
        $name:ident ($class:literal, $method:literal, $text:literal) { $($field:ident : $ty:ty),* $(,)? }
    )*) => {
        $(
            $(#[$doc])*
            #[derive(Debug, Clone, Default, PartialEq)]
            #[allow(missing_docs)]
            pub struct $name { $(pub $field: $ty),* }

            impl $name {
                /// Class and method id.
                pub const ID: (u16, u16) = ($class, $method);

                #[allow(unused_variables)]
                fn put(&self, e: &mut Enc<'_>) {
                    $( Arg::put(&self.$field, e); )*
                }

                #[allow(unused_variables)]
                fn get(d: &mut Dec<'_>) -> DResult<Self> {
                    Ok(Self { $( $field: <$ty as Arg>::get(d)? ),* })
                }
            }

            impl From<$name> for Method {
                fn from(m: $name) -> Method {
                    Method::$name(m)
                }
            }
        )*

        /// A method frame's content.
        #[derive(Debug, Clone, PartialEq)]
        #[allow(missing_docs)]
        pub enum Method {
            $( $name($name), )*
        }

        impl Method {
            /// Class and method id.
            #[must_use]
            pub fn id(&self) -> (u16, u16) {
                match self { $( Method::$name(_) => $name::ID, )* }
            }

            /// The spec name, `class.method`.
            #[must_use]
            pub fn name(&self) -> &'static str {
                match self { $( Method::$name(_) => $text, )* }
            }

            /// Encodes the method frame payload: class id, method id,
            /// arguments.
            pub fn encode(&self, buf: &mut BytesMut) {
                let (c, m) = self.id();
                buf.put_u16(c);
                buf.put_u16(m);
                let mut e = Enc::new(buf);
                match self { $( Method::$name(x) => x.put(&mut e), )* }
            }

            /// Decodes a method frame payload.
            ///
            /// # Errors
            /// [`MethodError::Unknown`] for ids no method has (540), and
            /// [`MethodError::Syntax`] for malformed arguments (502).
            pub fn decode(payload: &[u8]) -> Result<Self, MethodError> {
                let mut d = Dec::new(payload);
                let c = d.u16().map_err(MethodError::Syntax)?;
                let m = d.u16().map_err(MethodError::Syntax)?;
                let out = match (c, m) {
                    $( ($class, $method) => Method::$name($name::get(&mut d).map_err(MethodError::Syntax)?), )*
                    _ => return Err(MethodError::Unknown(c, m)),
                };
                Ok(out)
            }
        }
    };
}

/// A method frame that can't be decoded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MethodError {
    /// No such class and method.
    #[error("unknown method {0}.{1}")]
    Unknown(u16, u16),
    /// Malformed arguments.
    #[error("{0}")]
    Syntax(DecodeError),
}

methods! {
    /// `connection.start`.
    ConnectionStart (10, 10, "connection.start") {
        version_major: u8, version_minor: u8, server_properties: FieldTable,
        mechanisms: Bytes, locales: Bytes,
    }
    /// `connection.start-ok`.
    ConnectionStartOk (10, 11, "connection.start-ok") {
        client_properties: FieldTable, mechanism: String, response: Bytes, locale: String,
    }
    /// `connection.secure`.
    ConnectionSecure (10, 20, "connection.secure") { challenge: Bytes }
    /// `connection.secure-ok`.
    ConnectionSecureOk (10, 21, "connection.secure-ok") { response: Bytes }
    /// `connection.tune`.
    ConnectionTune (10, 30, "connection.tune") { channel_max: u16, frame_max: u32, heartbeat: u16 }
    /// `connection.tune-ok`.
    ConnectionTuneOk (10, 31, "connection.tune-ok") { channel_max: u16, frame_max: u32, heartbeat: u16 }
    /// `connection.open`.
    ConnectionOpen (10, 40, "connection.open") { virtual_host: String, capabilities: String, insist: bool }
    /// `connection.open-ok`.
    ConnectionOpenOk (10, 41, "connection.open-ok") { known_hosts: String }
    /// `connection.close`.
    ConnectionClose (10, 50, "connection.close") {
        reply_code: u16, reply_text: String, class_id: u16, method_id: u16,
    }
    /// `connection.close-ok`.
    ConnectionCloseOk (10, 51, "connection.close-ok") {}
    /// `connection.blocked` (RabbitMQ).
    ConnectionBlocked (10, 60, "connection.blocked") { reason: String }
    /// `connection.unblocked` (RabbitMQ).
    ConnectionUnblocked (10, 61, "connection.unblocked") {}
    /// `connection.update-secret` (RabbitMQ).
    ConnectionUpdateSecret (10, 70, "connection.update-secret") { new_secret: Bytes, reason: String }
    /// `connection.update-secret-ok` (RabbitMQ).
    ConnectionUpdateSecretOk (10, 71, "connection.update-secret-ok") {}

    /// `channel.open`.
    ChannelOpen (20, 10, "channel.open") { out_of_band: String }
    /// `channel.open-ok`.
    ChannelOpenOk (20, 11, "channel.open-ok") { channel_id: Bytes }
    /// `channel.flow`.
    ChannelFlow (20, 20, "channel.flow") { active: bool }
    /// `channel.flow-ok`.
    ChannelFlowOk (20, 21, "channel.flow-ok") { active: bool }
    /// `channel.close`.
    ChannelClose (20, 40, "channel.close") {
        reply_code: u16, reply_text: String, class_id: u16, method_id: u16,
    }
    /// `channel.close-ok`.
    ChannelCloseOk (20, 41, "channel.close-ok") {}

    /// `exchange.declare`.
    ExchangeDeclare (40, 10, "exchange.declare") {
        ticket: u16, exchange: String, kind: String, passive: bool, durable: bool,
        auto_delete: bool, internal: bool, no_wait: bool, arguments: FieldTable,
    }
    /// `exchange.declare-ok`.
    ExchangeDeclareOk (40, 11, "exchange.declare-ok") {}
    /// `exchange.delete`.
    ExchangeDelete (40, 20, "exchange.delete") { ticket: u16, exchange: String, if_unused: bool, no_wait: bool }
    /// `exchange.delete-ok`.
    ExchangeDeleteOk (40, 21, "exchange.delete-ok") {}
    /// `exchange.bind` (RabbitMQ).
    ExchangeBind (40, 30, "exchange.bind") {
        ticket: u16, destination: String, source: String, routing_key: String,
        no_wait: bool, arguments: FieldTable,
    }
    /// `exchange.bind-ok` (RabbitMQ).
    ExchangeBindOk (40, 31, "exchange.bind-ok") {}
    /// `exchange.unbind` (RabbitMQ).
    ExchangeUnbind (40, 40, "exchange.unbind") {
        ticket: u16, destination: String, source: String, routing_key: String,
        no_wait: bool, arguments: FieldTable,
    }
    /// `exchange.unbind-ok` (RabbitMQ; method id 51, not 41).
    ExchangeUnbindOk (40, 51, "exchange.unbind-ok") {}

    /// `queue.declare`.
    QueueDeclare (50, 10, "queue.declare") {
        ticket: u16, queue: String, passive: bool, durable: bool, exclusive: bool,
        auto_delete: bool, no_wait: bool, arguments: FieldTable,
    }
    /// `queue.declare-ok`.
    QueueDeclareOk (50, 11, "queue.declare-ok") { queue: String, message_count: u32, consumer_count: u32 }
    /// `queue.bind`.
    QueueBind (50, 20, "queue.bind") {
        ticket: u16, queue: String, exchange: String, routing_key: String,
        no_wait: bool, arguments: FieldTable,
    }
    /// `queue.bind-ok`.
    QueueBindOk (50, 21, "queue.bind-ok") {}
    /// `queue.purge`.
    QueuePurge (50, 30, "queue.purge") { ticket: u16, queue: String, no_wait: bool }
    /// `queue.purge-ok`.
    QueuePurgeOk (50, 31, "queue.purge-ok") { message_count: u32 }
    /// `queue.delete`.
    QueueDelete (50, 40, "queue.delete") {
        ticket: u16, queue: String, if_unused: bool, if_empty: bool, no_wait: bool,
    }
    /// `queue.delete-ok`.
    QueueDeleteOk (50, 41, "queue.delete-ok") { message_count: u32 }
    /// `queue.unbind`.
    QueueUnbind (50, 50, "queue.unbind") {
        ticket: u16, queue: String, exchange: String, routing_key: String, arguments: FieldTable,
    }
    /// `queue.unbind-ok`.
    QueueUnbindOk (50, 51, "queue.unbind-ok") {}

    /// `basic.qos`.
    BasicQos (60, 10, "basic.qos") { prefetch_size: u32, prefetch_count: u16, global: bool }
    /// `basic.qos-ok`.
    BasicQosOk (60, 11, "basic.qos-ok") {}
    /// `basic.consume`.
    BasicConsume (60, 20, "basic.consume") {
        ticket: u16, queue: String, consumer_tag: String, no_local: bool, no_ack: bool,
        exclusive: bool, no_wait: bool, arguments: FieldTable,
    }
    /// `basic.consume-ok`.
    BasicConsumeOk (60, 21, "basic.consume-ok") { consumer_tag: String }
    /// `basic.cancel`.
    BasicCancel (60, 30, "basic.cancel") { consumer_tag: String, no_wait: bool }
    /// `basic.cancel-ok`.
    BasicCancelOk (60, 31, "basic.cancel-ok") { consumer_tag: String }
    /// `basic.publish`.
    BasicPublish (60, 40, "basic.publish") {
        ticket: u16, exchange: String, routing_key: String, mandatory: bool, immediate: bool,
    }
    /// `basic.return`.
    BasicReturn (60, 50, "basic.return") {
        reply_code: u16, reply_text: String, exchange: String, routing_key: String,
    }
    /// `basic.deliver`.
    BasicDeliver (60, 60, "basic.deliver") {
        consumer_tag: String, delivery_tag: u64, redelivered: bool, exchange: String,
        routing_key: String,
    }
    /// `basic.get`.
    BasicGet (60, 70, "basic.get") { ticket: u16, queue: String, no_ack: bool }
    /// `basic.get-ok`.
    BasicGetOk (60, 71, "basic.get-ok") {
        delivery_tag: u64, redelivered: bool, exchange: String, routing_key: String,
        message_count: u32,
    }
    /// `basic.get-empty`.
    BasicGetEmpty (60, 72, "basic.get-empty") { cluster_id: String }
    /// `basic.ack`.
    BasicAck (60, 80, "basic.ack") { delivery_tag: u64, multiple: bool }
    /// `basic.reject`.
    BasicReject (60, 90, "basic.reject") { delivery_tag: u64, requeue: bool }
    /// `basic.recover-async` (deprecated).
    BasicRecoverAsync (60, 100, "basic.recover-async") { requeue: bool }
    /// `basic.recover`.
    BasicRecover (60, 110, "basic.recover") { requeue: bool }
    /// `basic.recover-ok`.
    BasicRecoverOk (60, 111, "basic.recover-ok") {}
    /// `basic.nack` (RabbitMQ).
    BasicNack (60, 120, "basic.nack") { delivery_tag: u64, multiple: bool, requeue: bool }

    /// `confirm.select` (RabbitMQ).
    ConfirmSelect (85, 10, "confirm.select") { no_wait: bool }
    /// `confirm.select-ok` (RabbitMQ).
    ConfirmSelectOk (85, 11, "confirm.select-ok") {}

    /// `tx.select`.
    TxSelect (90, 10, "tx.select") {}
    /// `tx.select-ok`.
    TxSelectOk (90, 11, "tx.select-ok") {}
    /// `tx.commit`.
    TxCommit (90, 20, "tx.commit") {}
    /// `tx.commit-ok`.
    TxCommitOk (90, 21, "tx.commit-ok") {}
    /// `tx.rollback`.
    TxRollback (90, 30, "tx.rollback") {}
    /// `tx.rollback-ok`.
    TxRollbackOk (90, 31, "tx.rollback-ok") {}
}

impl Method {
    /// Whether a client may send it (the rest only servers send).
    #[must_use]
    pub fn client_may_send(&self) -> bool {
        !matches!(
            self,
            Method::ConnectionStart(_)
                | Method::ConnectionSecure(_)
                | Method::ConnectionTune(_)
                | Method::ConnectionOpenOk(_)
                | Method::ConnectionUpdateSecretOk(_)
                | Method::ChannelOpenOk(_)
                | Method::ExchangeDeclareOk(_)
                | Method::ExchangeDeleteOk(_)
                | Method::ExchangeBindOk(_)
                | Method::ExchangeUnbindOk(_)
                | Method::QueueDeclareOk(_)
                | Method::QueueBindOk(_)
                | Method::QueuePurgeOk(_)
                | Method::QueueDeleteOk(_)
                | Method::QueueUnbindOk(_)
                | Method::BasicQosOk(_)
                | Method::BasicConsumeOk(_)
                | Method::BasicReturn(_)
                | Method::BasicDeliver(_)
                | Method::BasicGetOk(_)
                | Method::BasicGetEmpty(_)
                | Method::BasicRecoverOk(_)
                | Method::ConfirmSelectOk(_)
                | Method::TxSelectOk(_)
                | Method::TxCommitOk(_)
                | Method::TxRollbackOk(_)
        )
    }

    /// Whether content (a header and body frames) follows the method.
    #[must_use]
    pub fn has_content(&self) -> bool {
        matches!(
            self,
            Method::BasicPublish(_)
                | Method::BasicReturn(_)
                | Method::BasicDeliver(_)
                | Method::BasicGetOk(_)
        )
    }
}
