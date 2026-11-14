//! `sprig-proto-bolt`: the Bolt protocol and PackStream codec.
//!
//! Layer: 1 foundations.
//!
//! - [`packstream`]: PackStream v1 values.
//! - [`message`]: the handshake, chunked framing and Bolt 5.0–5.8
//!   messages.
//! - [`server`]: the connection state machine over a [`server::BoltService`].
//! - [`structs`]: graph values as PackStream structures.
//! - [`client`]: a minimal client for tests and the differential suite.

pub mod client;
pub mod message;
pub mod packstream;
pub mod server;
pub mod structs;

pub use message::{BoltVersion, Request, Response};
pub use packstream::{PackError, PackValue};
