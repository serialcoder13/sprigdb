//! `sprig-workload`: fingerprinting, statistics and the workload store.
//!
//! Layer: 2 operations. Design: [chapter 12](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/12-workload.md)
//! (Workload capture) of the SprigDB low-level design.
//!
//! Implemented so far (chapter 12 build steps 1, 2, 3 and 7, M0.4):
//!
//! - [`fingerprint`]: the SQL normalizer on `sprig-sql`'s persona-aware
//!   lexer, the `xxh3_64` digest, and [`fingerprint::SqlFingerprinter`],
//!   which caches each fingerprint's text and shape per gateway;
//! - [`shape`]: the shape types the advisor reads (a stable contract), the
//!   [`shape::TableResolver`] the gateway implements, and the extractor
//!   (sqlparser-rs, with a lexer fallback);
//! - [`histogram`]: the sparse log latency histogram;
//! - [`recorder`]: in-memory aggregation per (tenant, fingerprint), probe
//!   plans, memory caps and eviction;
//! - [`window`]: window statistics and parameter samples, as chapter 13
//!   reads them;
//! - [`txn`]: transaction footprints (the `txn` kind);
//! - [`store`]: the catalog tables (the `workload` migration module), the
//!   flusher's writes, rollup and retention, and the reader chapter 13
//!   uses ([`store::WorkloadStore`], step 4);
//! - [`flush`]: the per-gateway flusher, off the statement path (step 4);
//! - [`samples`] and [`keyring`]: sealed parameter samples (step 6);
//! - [`settings`]: the `workload.*` keys.
//!
//! The `Handler_read_*` probes (step 5) are in the gateway's backend
//! session (`sprig-gateway::backend`), which owns the connection.

pub mod fingerprint;
pub mod flush;
pub mod histogram;
pub mod keyring;
pub mod recorder;
pub mod samples;
pub mod settings;
pub mod shape;
pub mod store;
pub mod txn;
pub mod window;
