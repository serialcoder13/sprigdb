//! Hashing for routing.
//!
//! `xxh3_64` with seed 0 over the normalized bytes. Its output has been
//! frozen since xxHash 0.8.0; `tests/vectors.txt` pins golden vectors.

use sprig_common::Result;

use crate::key::{KeySpec, KeyValue, RoutingHash};
use crate::normalize::{NormOutcome, Normalizer, StdNormalizer};

/// `xxh3_64(normalized, seed = 0)`.
#[must_use]
pub fn routing_hash(normalized: &[u8]) -> RoutingHash {
    RoutingHash(twox_hash::XxHash3_64::oneshot(normalized))
}

/// Normalizes `v` under `spec` and hashes it.
///
/// # Errors
/// What [`Normalizer::normalize`] returns.
pub fn hash_key(spec: &KeySpec, v: &[KeyValue<'_>]) -> Result<(RoutingHash, NormOutcome)> {
    let mut buf = Vec::with_capacity(32);
    let outcome = StdNormalizer.normalize(spec, v, &mut buf)?;
    Ok((routing_hash(&buf), outcome))
}
