//! `sprig-proto-kafka`: the Kafka wire protocol codec and broker front end.
//!
//! Layer: 1 foundations. Design: [chapter 45](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/45-event-streams.md) (Event streams) of the SprigDB low-level design, build steps 8, 9, 12 and 13.
//!
//! - [`wire`]: primitive types, [`wire::Reader`] and [`wire::Writer`], the
//!   [`wire::Message`] trait;
//! - [`messages`]: every served message, generated from Kafka's message
//!   definitions by `tools/gen_messages.py`;
//! - [`protocol`]: API keys, served versions, error codes, headers and
//!   frames;
//! - [`records`]: record batches v2 with CRC-32C ([`crc32c`]) and
//!   [`compression`] (gzip, zstd; snappy and LZ4 refused);
//! - [`sasl`]: SASL PLAIN and SCRAM-SHA-256/512 server conversations;
//! - [`broker`]: the broker front end over traits the gateway implements
//!   (identity, brokers, topics, offsets, coordinator leases), with the
//!   classic consumer group coordinator ([`group`]).

pub mod compression;
pub mod crc32c;
pub mod messages;
pub mod protocol;
pub mod records;
pub mod wire;
