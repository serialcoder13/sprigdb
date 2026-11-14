//! `sprig-proto-kafka`: the Kafka wire protocol codec and broker front end.
//!
//! Layer: 1 foundations.
//!
//! - [`wire`]: primitive types, [`wire::Reader`] and [`wire::Writer`], the
//!   [`wire::Message`] trait;
//! - [`messages`]: every served message, generated from Kafka's message
//!   definitions by `tools/gen_messages.py`;
//! - [`protocol`]: API keys, served versions, error codes, headers and
//!   frames;
//! - [`records`]: record batches v2 with CRC-32C ([`crc32c`]) and
//!   [`compression`] (gzip, snappy, LZ4, zstd);
//! - [`sasl`]: SASL PLAIN and SCRAM-SHA-256/512 server conversations;
//! - [`broker`]: the broker front end over traits the gateway implements
//!   (identity, brokers, topics, offsets, coordinator leases), with the
//!   classic consumer group coordinator ([`group`]) and topic
//!   administration over streams ([`topics`]);
//! - [`mem`]: in-memory implementations: tests, and the gateway's temporary
//!   topic adapter until `sprig-streams`' catalog API lands;
//! - [`client`]: a minimal client for tests and tools.

// Kafka API and config names (ListOffsets, retention.ms) fill the docs;
// handlers take the request, session and version as `q`, `s` and `v`.
#![allow(clippy::doc_markdown, clippy::many_single_char_names)]

pub mod broker;
pub mod client;
pub mod compression;
pub mod crc32c;
pub mod group;
pub mod mem;
pub mod messages;
pub mod protocol;
pub mod records;
pub mod sasl;
pub mod topics;
pub mod wire;
