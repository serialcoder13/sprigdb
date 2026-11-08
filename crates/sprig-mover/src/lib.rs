//! `sprig-mover`: copy, catch-up, apply and cutover.
//!
//! Layer: 2 operations. Design: [chapter 10](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/10-mover.md) (The mover) of the SprigDB low-level design.
//!
//! The mover copies a shard's schemas between clusters while clients keep
//! writing, then switches ownership with a write hold of at most
//! `mover.cutover_max_hold_ms`:
//!
//! 1. a chunked copy with DBLog watermarks ([`engine`]);
//! 2. catch-up from the binlog through one ordered, idempotent apply lane,
//!    whose every target transaction also commits the source position;
//! 3. verification by per-row digests, with range repair;
//! 4. cutover: hold the tenant's map, fence the source, apply up to the
//!    fence position, raise the epoch on the map, release.
//!
//! What is here (chapter 10 build steps 3 to 8, 10 and 11; steps 1 and 2
//! are `sprig-binlog`):
//!
//! - [`api`]: [`Mover`] (submit, status, cancel, list, manual cutover) and
//!   the job types, which the balancer (chapter 11) codes against;
//! - [`record`]: job records, claims, leases, tokens, shard locks and copy
//!   slots in the DC tier, and [`MetaMover`], the [`Mover`] on them;
//! - [`engine`]: one job's state machine;
//! - [`io`]: the seams the engine talks through, so the deterministic
//!   simulation (`sprig-testkit::sim::mover`) runs the same engine;
//! - [`mysql`]: the production source, stream and target on MariaDB;
//! - [`maps`]: the cutover's hold and commit on chapter 15's tenant map;
//! - [`worker`]: the job runtime a worker role runs;
//! - [`settings`]: the `mover.*` keys.
//!
//! R0 moves a tenant's whole placement unit (its catalog and every
//! database's shard schema, which R0 keeps on one cluster), keeping the
//! schema names; copy-and-swap is table scope. Split, merge, re-key and
//! reference copies are later releases.

pub mod api;
pub mod engine;
pub mod io;
pub mod maps;
pub mod model;
pub mod mysql;
pub mod record;
pub mod settings;
pub mod worker;

pub use api::{
    CutoverMode, DdlTransform, HashRange, JobFilter, JobKind, JobSpec, JobState, JobStatus,
    Mover, Progress, Routing, SourceSpec, TargetSpec, VerifyMode,
};
pub use io::{ChangeStream, CommitOutcome, JobEnv, MapIo, SourceIo, TargetIo};
pub use record::{Claim, JobRecord, JobStore, MetaMover};
pub use settings::MoverSettings;
pub use worker::{EnvFactory, JobRuntime, WorkerOpts};
