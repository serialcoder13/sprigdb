//! `sprig-proto-amqp`'s protocol suite (chapter 50 step 21) against the
//! queue engine: the AMQP front ends over [`QueueEngine`] with the
//! in-memory catalog, leases and message store. The suite is the same
//! file the in-memory engine runs (`sprig-proto-amqp/tests/protocol/
//! suite.rs`); with `SPRIG_AMQP_ADDR` set it runs against a gateway
//! instead, whose engine is this one over MariaDB.

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
use sprig_proto_amqp::mem::{Authorizer, VhostMap};
use sprig_queues::engine::{EngineDeps, NoTemps, QueueEngine};
use sprig_queues::mem::{MemCatalog, MemLeases};
use sprig_queues::owner::SystemClock;

/// Sends `connection.blocked` (true) or unblocked (false).
type Blocker = EngineArc<dyn Fn(bool) + Send + Sync>;

/// The queue engine over memory.
fn engine_under_test(map: VhostMap, authz: Authorizer) -> (EngineArc<dyn QueueBackend>, Blocker) {
    let catalog = EngineArc::new(MemCatalog::new(map, authz));
    let engine = EngineArc::new(QueueEngine::new(EngineDeps {
        catalog,
        leases: EngineArc::new(MemLeases::new().for_gateway("gw-test")),
        temps: EngineArc::new(NoTemps),
        clock: EngineArc::new(SystemClock),
        slot: 1,
    }));
    let e = EngineArc::clone(&engine);
    (engine, EngineArc::new(move |b| e.set_blocked(b)))
}

include!("../../sprig-proto-amqp/tests/protocol/suite.rs");
