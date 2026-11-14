//! `sprig-docs`: SprigDB's product documentation as data.
//!
//! - [`reference`](mod@reference): the generated reference pages (`docs/reference/*.md`),
//!   rendered from plain data so this crate depends on nothing in the
//!   workspace; `sprig-docs-gen` collects the data from the code (the
//!   settings registry, `SprigCode::ALL`, the `OpenAPI` document, the source
//!   tree's metric macros) and writes or checks the pages.
//! - [`metrics_scan`]: finds every `counter!`, `gauge!` and `histogram!`
//!   named `sprig_*` in the source, with its labels.
//! - The docs pack, which makes the pages searchable offline:
//!   - [`page`] reads `docs/` and each page's front matter;
//!   - [`chunk`] cuts a page into chunks and finds exact identifiers;
//!   - [`pack`] is the reproducible pack file, written and verified;
//!   - [`search`] is in-process BM25, cosine and reciprocal-rank fusion;
//!   - [`table`] is the pack as rows of the `__sprig_docs` system tables
//!     every cluster holds, and the digest their loader verifies;
//!   - [`settings`] holds the `docs.*` keys.
//!
//!   Embeddings come from `sprig-embed`; this crate has no model.

pub mod chunk;
pub mod metrics_scan;
pub mod pack;
pub mod page;
pub mod reference;
pub mod search;
pub mod settings;
pub mod table;
