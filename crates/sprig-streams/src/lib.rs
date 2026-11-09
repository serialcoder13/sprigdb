//! `sprig-streams`: event streams: storage, retention, change topics,
//! consumer groups and offsets.
//!
//! Layer: 2 data layer. Design: [chapter 45](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/45-event-streams.md) (Event streams) of the SprigDB low-level design.
//!
//! What exists (R9, build steps 1–7: the core below the Kafka front end):
//!
//! - [`model`], [`store`]: streams in the catalog (`streams`,
//!   `stream_groups`, `stream_offsets`), loaded into every snapshot by
//!   [`StreamsExtension`] (step 2);
//! - [`grammar`], [`sql`]: `CREATE | ALTER | DROP STREAM`, `SHOW STREAMS`,
//!   publishing with `INSERT INTO <stream>`, reading with `SELECT ... FROM
//!   <stream>`, and the append-only refusals, for the gateway (steps 2, 3);
//! - [`ddl`]: the outbox, the stream log with its day partitions, and the
//!   checkpoints (steps 3, 4);
//! - [`partition`]: Kafka's murmur2 partitioner;
//! - [`sequencer`]: offsets in commit order from the tenant's feed, with
//!   leader-epoch fencing, checkpoints, outbox cleanup and high watermarks
//!   (step 5), on a [`maria::MariaLog`] or, in tests and the simulation, a
//!   [`memlog::MemLog`];
//! - [`envelope`]: change-topic records for SQL tables (step 6);
//! - [`retention`]: day partitions ahead, time and size retention,
//!   compaction, and the outbox sweep (step 7);
//! - [`read`]: reading the log by partition and offset, the high
//!   watermark, and the long-poll wait, for the Kafka front end.
//!
//! Not built here: the Kafka codec and broker (`sprig-proto-kafka`, steps
//! 8–13), HTTP and gRPC consumers (14), consumer-group coordination (only
//! its tables), snapshots of change topics, envelopes for collections,
//! key-value, Redis and graphs, log groups with more than one shard, and
//! outboxes in groups with more than one shard (39945).

pub mod ddl;
pub mod envelope;
pub mod grammar;
pub mod maria;
pub mod memlog;
pub mod model;
pub mod partition;
pub mod read;
pub mod retention;
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
