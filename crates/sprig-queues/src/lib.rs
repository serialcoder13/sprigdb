//! `sprig-queues`: the queue engine behind AMQP (chapter 50, build steps
//! 2–10): exchanges, queues and bindings in the tenant catalog, messages
//! in MariaDB, one owner per queue.
//!
//! Layer: 2 data layer. Design: [chapter 50](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/50-queues.md) (Message queues and AMQP) of the SprigDB low-level design.
//!
//! - [`topology`]: the model and routing (every exchange type,
//!   exchange-to-exchange bindings, alternate exchanges, policies);
//! - [`store`]: what owners write ([`store::MsgStore`]), served by
//!   [`maria`] (the `__sprig_q*` shard tables) and [`memstore`];
//! - [`catalog`]: the `mq_*` catalog tables, their migration and the
//!   snapshot loader;
//! - [`owner`]: the queue owner: head cache, credit dispatch, unacked
//!   tracking, consumer timeouts, TTL, length limits, priorities,
//!   dead-lettering, batched writes fenced by the owner epoch;
//! - [`engine`] and [`vhost`]: [`engine::QueueEngine`], the
//!   `QueueBackend` the AMQP front ends run on; owner slots, leases, the
//!   outbox forwarder (dead letters, delayed messages, transactional
//!   publishes);
//! - [`token`]: delivery tokens for acknowledging inside a database
//!   transaction; [`sql`]: `sprig.mq_publish`, `mq_get`, `mq_ack` and
//!   `mq_nack` for the gateway's SQL path;
//! - [`mem`]: an in-memory catalog and leases (tests, simulation).

// AMQP names (basic.publish, x-dead-letter-exchange) fill the docs.
#![allow(
    clippy::doc_markdown,
    clippy::too_many_lines,
    clippy::module_name_repetitions,
    clippy::cast_precision_loss,
    clippy::struct_excessive_bools
)]

pub mod catalog;
pub mod engine;
pub mod maria;
pub mod mem;
pub mod memstore;
pub mod msg;
pub mod owner;
pub mod settings;
pub mod store;
pub mod token;
pub mod topology;
pub mod vhost;

pub use engine::{EngineDeps, QueueEngine};
