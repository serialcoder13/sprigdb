//! `sprig-proto-mongo`: the `OP_MSG` codec, BSON helpers, SASL and SCRAM.
//!
//! Layer: 1 foundations.
//!
//! - [`codec`]: `OP_MSG`, `OP_QUERY`/`OP_REPLY` and `OP_COMPRESSED`
//!   (snappy, zlib, zstd), with the CRC-32C checksum ([`crc32c`]);
//! - [`scram`]: SCRAM-SHA-256 server (and a client for tests) over
//!   the login service's stored verifiers;
//! - [`errors`]: error codes, `codeName`s and the error reply.
//!
//! The front end that uses them is `sprig_gateway::mongo`.

// MongoDB command and field names fill the docs.
#![allow(clippy::doc_markdown)]

pub mod codec;
pub mod crc32c;
pub mod errors;
pub mod scram;

pub use bson;
pub use codec::{Compressor, DocSequence, Frame, MsgHeader, OpMsg, OpQuery, OpReply};
pub use errors::MongoError;
