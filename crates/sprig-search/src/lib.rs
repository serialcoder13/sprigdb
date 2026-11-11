//! `sprig-search`: full-text search with Tantivy (chapter 39).
//!
//! Layer: 2 data layer. Design: [chapter 39](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/39-search.md) (Full-text search) of the SprigDB low-level design.
//!
//! - [`def`], index definitions (SQL, Atlas, MongoDB `text`);
//! - [`ddl`], `CREATE`, `ALTER`, `DROP` and `SHOW SEARCH INDEX`, search
//!   analyzers, and MySQL `FULLTEXT` DDL;
//! - [`analyze`], analyzers;
//! - [`schema`], Tantivy schemas, keys and extraction;
//! - [`partition`], one Tantivy index with its committed feed position;
//! - [`query`], the query AST and its parsers;
//! - [`build`], Tantivy queries from the AST;
//! - [`search`], an index's partitions, global BM25, merges and facets;
//! - [`highlight`], highlights from fetched text;
//! - [`sqlscan`], `MATCH ... AGAINST` and `sprig_search()` in SQL text;
//! - [`copy`], one copy of an index kept current from the change feed,
//!   on a worker with the search role or in a gateway;
//! - [`rpc`], the messages between gateways and copies;
//! - [`placement`], the DC-tier records of indexes and their copies on
//!   workers;
//! - [`settings`], the `search.*` keys.
//!
//! The library holds no connections of its own: its host (a worker's
//! `search_copy` job or a gateway) gives a copy the feed and the seed's
//! rows.

// Single-letter names follow Lucene and MySQL notation (q, t, f, d);
// the parsers are long tables.
#![allow(clippy::many_single_char_names, clippy::too_many_lines)]

pub mod analyze;
pub mod build;
pub mod copy;
pub mod ddl;
pub mod def;
pub mod highlight;
pub mod partition;
pub mod placement;
pub mod query;
pub mod rpc;
pub mod schema;
pub mod search;
pub mod settings;
pub mod sqlscan;

pub use def::{FieldDef, FieldKind, Flavor, IndexDef, Model, Source};
pub use query::SearchQuery;
pub use schema::{IndexSchema, Key, KeyPart, Record, Val};
pub use search::{Hit, Scoring, SearchIndex};
