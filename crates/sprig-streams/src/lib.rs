//! `sprig-streams`: event streams: storage, retention, change topics,
//! consumer groups and offsets.
//!
//! Layer: 2 data layer.
//!
//! What exists (the core below the Kafka front end):
//!
//! - [`model`], [`store`]: streams in the catalog (`streams`,
//!   `stream_groups`, `stream_offsets`), loaded into every snapshot by
//!   [`StreamsExtension`];
//! - [`grammar`], [`sql`]: `CREATE | ALTER | DROP STREAM`, `SHOW STREAMS`,
//!   publishing with `INSERT INTO <stream>`, reading with `SELECT ... FROM
//!   <stream>`, and the append-only refusals, for the gateway;
//! - [`ddl`]: the outbox, the stream log with its day partitions, and the
//!   checkpoints;
//! - [`partition`]: Kafka's murmur2 partitioner;
//! - [`sequencer`]: offsets in commit order from the tenant's feed, with
//!   leader-epoch fencing, checkpoints, outbox cleanup and high
//!   watermarks, on a [`maria::MariaLog`] or, in tests and the
//!   simulation, a [`memlog::MemLog`];
//! - [`envelope`]: change-topic records for SQL tables;
//! - [`retention`]: day partitions ahead, time and size retention,
//!   compaction, and the outbox sweep;
//! - [`read`]: reading the log by partition and offset, the high
//!   watermark, and the long-poll wait, for the Kafka front end;
//! - [`offsets`]: committing an offset inside the caller's transaction
//!   (`sprig.commit_offset`, `db.commitOffset`, triggers on streams).
//!
//! Not built here: the Kafka codec and broker (`sprig-proto-kafka`), HTTP
//! and gRPC consumers, consumer-group coordination (only its tables),
//! snapshots of change topics, envelopes for collections, key-value,
//! Redis and graphs, log groups with more than one shard, and outboxes in
//! groups with more than one shard (39945).

// Statements read best as text built with format!, and the short
// pseudocode names (s, p, o, q) are kept.
#![allow(
    clippy::many_single_char_names,
    clippy::format_push_string,
    clippy::type_complexity
)]

pub mod ddl;
pub mod envelope;
pub mod grammar;
pub mod maria;
pub mod memlog;
pub mod model;
pub mod offsets;
pub mod partition;
pub mod publish;
pub mod read;
pub mod retention;
pub mod runtime;
pub mod sequencer;
pub mod settings;
pub mod sql;
pub mod store;

pub use model::{
    Cleanup, Envelope, LeaderEpoch, LogShard, NewRecord, Offset, PartitionNo, Record, ShardSlot,
    Source, StreamDef, StreamId, StreamKind, StreamState, StreamsState,
};
pub use sequencer::{RunEnd, Sequencer, Start};
pub use store::{MODULE, StreamsExtension, StreamsStore};
