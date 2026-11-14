//! Split and merge under deterministic simulation:
//! a shard splits in two and the halves merge back under client load,
//! while workers crash at every job state, etcd partitions, streams drop,
//! target commits fail or lose their reply, fences fail or stall and
//! binlogs are purged; the invariants of `sprig_testkit::sim::split` hold
//! on every seed.
//!
//! `SPRIG_SIM_SEEDS=10000 cargo test -p sprig-testkit --test sim_split`
//! runs many seeds; CI runs the default.

use std::time::Duration;

use sprig_testkit::sim::Sim;
use sprig_testkit::sim::split::run_split_merge;

#[test]
fn a_shard_splits_and_merges_under_load_and_faults() {
    Sim::new("split")
        .seeds(100)
        .max_time(Duration::from_secs(7200))
        .run(|w| async move { run_split_merge(w).await.map_err(Into::into) });
}
