//! The mover under deterministic simulation (chapter 10, "Testing"; build
//! step 9): a tenant moves under load while workers crash at every job
//! state, etcd partitions, streams drop, target commits fail or lose their
//! reply, fences fail or stall, binlogs are purged and clients run DDL;
//! the seven invariants of `sprig_testkit::sim::mover` hold on every seed.
//!
//! `SPRIG_SIM_SEEDS=10000 cargo test -p sprig-testkit --test sim_mover`
//! runs the milestone's 10,000 seeds; CI runs the default.

use std::time::Duration;

use sprig_testkit::sim::Sim;
use sprig_testkit::sim::mover::run_move;

#[test]
fn a_tenant_moves_under_load_and_faults() {
    Sim::new("mover")
        .seeds(200)
        .max_time(Duration::from_secs(3600))
        .run(|w| async move { run_move(w).await.map_err(Into::into) });
}
