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
use sprig_testkit::sim::mover::{run_idle_move, run_move, run_move_triggers};

#[test]
fn a_tenant_moves_under_load_and_faults() {
    Sim::new("mover")
        .seeds(200)
        .max_time(Duration::from_secs(3600))
        .run(|w| async move { run_move(w).await.map_err(Into::into) });
}

/// An idle tenant (no client writes) moves, sometimes behind another
/// job's copy slots, and an operator's cancel in any state before the
/// commit rolls it back (the R0 fixes before the 72-hour soak).
#[test]
fn an_idle_tenant_moves_and_cancels_in_every_state() {
    Sim::new("mover-idle")
        .seeds(200)
        .max_time(Duration::from_secs(3600))
        .run(|w| async move { run_idle_move(w).await.map_err(Into::into) });
}

/// An after-commit trigger runs on the tenant's feed while the tenant
/// moves and the trigger's worker is killed at random: every client
/// change runs exactly once (chapter 41, R7 order 5).
#[test]
fn a_trigger_runs_each_change_once_across_moves_and_worker_kills() {
    Sim::new("mover-triggers")
        .seeds(100)
        .max_time(Duration::from_secs(3600))
        .run(|w| async move { run_move_triggers(w).await.map_err(Into::into) });
}
