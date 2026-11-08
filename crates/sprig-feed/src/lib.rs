//! `sprig-feed`: the change feed: change events decoded from the binlog,
//! merging, resume tokens and subscriptions.
//!
//! Layer: 2 operations. Design: [chapter 38](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/38-change-feed.md) (Change feed) of the SprigDB low-level design.
//!
//! R1 serves internal consumers (chapter 48, R1 order 8):
//!
//! - [`source`] and [`binlog`]: one cluster's committed transactions for one
//!   tenant, assembled from `sprig-binlog`'s dump (the mover's reader),
//!   with `ANNOTATE_ROWS` stamps, XA groups and applier traffic marked;
//! - [`decode`], [`names`]: row changes as logical, tenant-scoped events:
//!   logical database and table names and ids, never physical schema
//!   names, internal columns removed;
//! - [`tail`]: one tenant's ordered feed, following the tenant across moves
//!   through the handoff records ([`ownership`]) the mover writes with
//!   each cutover, resumable from any [`token`];
//! - [`merge`]: the k-way merge by `(clamp, cluster, row)` across clusters;
//! - [`subs`]: subscriptions and acknowledged positions;
//! - [`requirements`]: the binlog settings the feed needs (`feed_source`);
//! - [`hlc`]: stamps and clamps.
//!
//! Not yet (later R1 build steps and R5): the shared per-cluster reader
//! with its ring and spool, catch-up reader limits, the archive, snapshots,
//! per-shard subscriptions, `FeedReader/Tail` on 7401, `sprigctl feed`, and
//! the application transports.

pub mod binlog;
pub mod decode;
pub mod event;
pub mod hlc;
pub mod merge;
pub mod names;
pub mod ownership;
pub mod placement;
pub mod requirements;
pub mod source;
pub mod subs;
pub mod tail;
pub mod token;

pub use event::{ChangeEvent, DdlKind, EventId, FeedItem, Image, Op, ShardRef};
pub use ownership::Handoff;
pub use placement::{MetaPlacement, PlacementView};
pub use source::{Clusters, RawRow, RawTxn, SourceItem, TableShape, TxnSource};
pub use tail::{StartAt, TailOptions, TenantTail};
pub use token::ResumeToken;
