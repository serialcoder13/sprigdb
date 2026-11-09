//! `sprig-proto-bolt`: the Bolt protocol and PackStream codec.
//!
//! Layer: 1 foundations. Design: [chapter 43](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/43-graphs.md) (Graphs) of the SprigDB low-level design.
//!
//! - [`packstream`]: PackStream v1 values (build step 16).
//! - [`message`]: the handshake, chunked framing and Bolt 5.0–5.4
//!   messages (step 17).
//! - [`server`]: the connection state machine over a [`server::BoltService`]
//!   (steps 17 and 18).
//! - [`structs`]: graph values as PackStream structures (step 19).
//! - [`client`]: a minimal client for tests and the differential suite.

pub mod client;
pub mod message;
pub mod packstream;
pub mod server;
pub mod structs;

pub use message::{BoltVersion, Request, Response};
pub use packstream::{PackError, PackValue};
