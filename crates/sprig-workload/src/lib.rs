//! `sprig-workload`: fingerprinting, statistics and the workload store.
//!
//! Layer: 2 operations.
//!
//! Implemented so far:
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
//! - [`window`]: window statistics and parameter samples, as automatic
//!   indexing reads them;
//! - [`txn`]: transaction footprints (the `txn` kind);
//! - [`store`]: the catalog tables (the `workload` migration module), the
//!   flusher's writes, rollup and retention, and the reader automatic
//!   indexing uses ([`store::WorkloadStore`]);
//! - [`flush`]: the per-gateway flusher, off the statement path;
//! - [`samples`] and [`keyring`]: sealed parameter samples;
//! - [`settings`]: the `workload.*` keys.
//!
//! The `Handler_read_*` probes are in the gateway's backend
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
