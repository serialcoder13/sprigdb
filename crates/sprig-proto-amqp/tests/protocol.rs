//! Protocol tests for both AMQP versions over TCP, with this crate's own
//! clients (no AMQP client library is installed on the build machines).
//!
//! By default each test starts the server in process on 127.0.0.1 with
//! the in-memory engine (`sprig-queues` runs the same suite, in
//! `protocol/suite.rs`, against the queue engine). With `SPRIG_AMQP_ADDR` set, the same tests run
//! against a gateway's `amqp` listener instead (the live test):
//!
//! ```text
//! SPRIG_AMQP_ADDR=127.0.0.1:35672 SPRIG_AMQP_USER=acme.app SPRIG_AMQP_PASSWORD=... \
//! [SPRIG_AMQP_TLS_ADDR=127.0.0.1:35671 SPRIG_AMQP_CA=deploy/compose/gateway-certs/ca.pem] \
//! cargo test -p sprig-proto-amqp --test protocol -- --nocapture
//! ```
//!
//! Tests that need the in-memory engine's test hooks (permissions,
//! `connection.blocked`, a small `max_message_bytes`) only run in process.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::similar_names,
    clippy::many_single_char_names,
    missing_docs
)]

use std::sync::Arc as EngineArc;

use sprig_proto_amqp::backend::QueueBackend;
use sprig_proto_amqp::mem::{Authorizer, MemBackend, VhostMap};

/// Sends `connection.blocked` (true) or unblocked (false).
type Blocker = EngineArc<dyn Fn(bool) + Send + Sync>;

/// The engine the suite runs against: `sprig-proto-amqp`'s in-memory one.
fn engine_under_test(map: VhostMap, authz: Authorizer) -> (EngineArc<dyn QueueBackend>, Blocker) {
    let mem = EngineArc::new(MemBackend::new(map, authz));
    let m = EngineArc::clone(&mem);
    (mem, EngineArc::new(move |b| m.set_blocked(b)))
}

include!("protocol/suite.rs");
