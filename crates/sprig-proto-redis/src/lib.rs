//! `sprig-proto-redis`: the RESP2 and RESP3 codec.
//!
//! Layer: 1 protocols.
//!
//! - [`codec`]: request decoding (multibulk and inline, with Redis's
//!   limits and protocol errors) and reply encoding ([`Frame`], RESP3
//!   frames downgraded for RESP2 clients).
//! - [`num`]: Redis's number rules: `string2ll`, `strtod`-style doubles,
//!   and `d2string`, the text Redis 7.2 gives a double.

// Redis's number rules are casts by design: lengths and counts become RESP
// integers, scores move between doubles and 52-bit geohashes, and the
// casts are checked where a value could leave its range.
#![allow(
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::float_cmp
)]

pub mod codec;
pub mod num;

pub use codec::{Command, DecodeError, Decoder, Frame, Limits, NullShape, Proto, encode};
