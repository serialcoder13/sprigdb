//! `sprig-docs`: SprigDB's product documentation (chapter 55, "Product
//! documentation and the docs pack"; decisions AI2, AI3, AI16 of
//! `sprigdb-plan/decisions/ai-integration.md`).
//!
//! - [`reference`]: the generated reference pages (`docs/reference/*.md`),
//!   rendered from plain data so this crate depends on nothing in the
//!   workspace; `sprig-docs-gen` collects the data from the code (the
//!   settings registry, `SprigCode::ALL`, the `OpenAPI` document, the source
//!   tree's metric macros) and writes or checks the pages.
//! - [`metrics_scan`]: finds every `counter!`, `gauge!` and `histogram!`
//!   named `sprig_*` in the source, with its labels.
//!
//! The docs pack (chunking, embeddings, the pack file) comes next, in this
//! crate, so the gateway can read packs without linking the generator.

pub mod metrics_scan;
pub mod reference;
