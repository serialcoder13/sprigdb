//! `sprig-proto-mysql`: MySQL and MariaDB wire codec, server and client side, and the auth plugins.
//!
//! Layer: 1 foundations.
//!
//! Modules:
//!
//! - [`codec`]: [`PacketCodec`], the tokio-util framing codec: 16 MiB
//!   splitting and reassembly, sequence ids, the `max_packet` limit, and
//!   zlib or zstd compressed framing, switchable on a live codec;
//! - [`buf`]: [`Reader`] and [`Writer`] for the protocol's integer and
//!   string types;
//! - [`packets`]: [`Caps`], [`StatusFlags`], [`Persona`], OK (with the
//!   session-state tracker), ERR, EOF, `HandshakeV10`,
//!   `HandshakeResponse41`, `SSLRequest`, `AuthSwitchRequest` and `AuthMoreData`,
//!   each encoded and decoded;
//! - [`resultset`]: column definitions (with MariaDB extended metadata),
//!   text and binary rows, the binary [`Value`] encoding, `PREPARE_OK`, and
//!   classifiers for response and row-stream packets;
//! - [`command`]: every `COM_*` the gateway accepts, parsed without panics
//!   and encoded, including `COM_STMT_EXECUTE` parameters, query
//!   attributes and MariaDB's `COM_STMT_BULK_EXECUTE`;
//! - [`error`]: ERR packet builders and the persona error maps;
//! - [`handshake`] and [`auth`]: the server side of the connection phase and
//!   the server auth plugins;
//! - [`client`]: the client side, for backend connections.

// Every fallible function in this crate fails only with `ProtoError` on
// malformed or short input (or I/O in the codec), so per-function
// `# Errors` sections would all say the same thing.
#![allow(clippy::missing_errors_doc)]

pub mod auth;
pub mod buf;
pub mod client;
pub mod codec;
pub mod command;
pub mod error;
pub mod handshake;
pub mod packets;
pub mod resultset;
pub mod state;

mod proto_error;

#[cfg(test)]
mod fuzz_tests;

pub use buf::{Reader, Writer};
pub use codec::{Framing, Packet, PacketCodec};
pub use command::Command;
pub use error::ErrPacket;
pub use packets::{Caps, Persona, StatusFlags};
pub use proto_error::{ProtoError, ProtoResult};
pub use resultset::{ColumnDef, Value};
