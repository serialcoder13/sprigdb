//! `sprig-proto-amqp`: AMQP 0-9-1 and AMQP 1.0 codecs and front ends.
//!
//! Layer: 1 foundations. Design: [chapter 50](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/50-queues.md) (Message queues and AMQP) of the SprigDB low-level design, build steps 11–14 and 23.
//!
//! - [`sniff`]: the protocol header table shared by 5672 and 5671;
//! - [`v091`]: AMQP 0-9-1 frames, methods, field tables and the server;
//! - [`v10`]: AMQP 1.0 types, performatives, messages, addresses and the
//!   server;
//! - [`auth`]: the SASL layer (`PLAIN`, `AMQPLAIN`, `EXTERNAL`) over an
//!   [`auth::Identity`] the gateway implements;
//! - [`backend`]: the [`backend::QueueBackend`] boundary to the queue
//!   engine, and [`mem`], an in-memory engine for tests;
//! - [`convert`]: message conversion between the two protocols;
//! - [`reply_to`]: direct reply-to names;
//! - [`server`]: one connection, from the protocol header on.

// AMQP names (basic.publish, x-dead-letter-exchange) fill the docs;
// performatives and methods have many flags and long field lists, and
// protocol handlers are long matches over them.
#![allow(
    clippy::doc_markdown,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::struct_excessive_bools
)]

pub mod auth;
pub mod backend;
pub mod convert;
pub mod error;
pub mod mem;
pub mod message;
pub mod reply_to;
pub mod server;
pub mod sniff;
pub mod v091;
pub mod v10;
