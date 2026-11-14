//! The mover under deterministic simulation: a tenant moves under load while workers crash at every job
//! state, etcd partitions, streams drop, target commits fail or lose their
//! reply, fences fail or stall, binlogs are purged and clients run DDL;
//! the seven invariants of `sprig_testkit::sim::mover` hold on every seed.
//!
//! `SPRIG_SIM_SEEDS=10000 cargo test -p sprig-testkit --test sim_mover`
//! runs the milestone's 10,000 seeds; CI runs the default.

use std::time::Duration;

use sprig_testkit::sim::Sim;
use sprig_testkit::sim::mover::{
    run_idle_move, run_move, run_move_streams, run_move_streams_shared, run_move_triggers,
    run_move_triggers_shared, run_move_xa, run_move_xa_no_drain, xa,
};

#[test]
fn a_tenant_moves_under_load_and_faults() {
    Sim::new("mover")
        .seeds(200)
        .max_time(Duration::from_secs(3600))
        .run(|w| async move { run_move(w).await.map_err(Into::into) });
}

/// An idle tenant (no client writes) moves, sometimes behind another
/// job's copy slots, and an operator's cancel in any state before the
/// commit rolls it back.
#[test]
fn an_idle_tenant_moves_and_cancels_in_every_state() {
    Sim::new("mover-idle")
        .seeds(200)
        .max_time(Duration::from_secs(3600))
        .run(|w| async move { run_idle_move(w).await.map_err(Into::into) });
}

/// An after-commit trigger runs on the tenant's feed while the tenant
/// moves and the trigger's worker is killed at random: every client
/// change runs exactly once.
#[test]
fn a_trigger_runs_each_change_once_across_moves_and_worker_kills() {
    Sim::new("mover-triggers")
        .seeds(100)
        .max_time(Duration::from_secs(3600))
        .run(|w| async move { run_move_triggers(w).await.map_err(Into::into) });
}

/// The same, with the trigger reading through the shared per-cluster
/// reader (R9, r9feed2): detached by its tiny buffer, it comes back through
/// the ring, a replay or a catch-up dump, and follows the move. Every
/// client change still runs exactly once.
#[test]
fn a_trigger_on_the_shared_reader_runs_each_change_once() {
    Sim::new("mover-triggers-shared")
        .seeds(100)
        .max_time(Duration::from_secs(3600))
        .run(|w| async move { run_move_triggers_shared(w).await.map_err(Into::into) });
}

/// A change topic's sequencer runs on the tenant's feed while the tenant
/// moves; sequencers are killed, overlapped by a second one (a lost
/// lease) and lose append answers at random: each partition's log holds
/// every change of the table once, in commit order, at offsets without
/// gaps.
#[test]
fn a_stream_sequencer_keeps_every_change_once_in_order_across_moves() {
    Sim::new("mover-streams")
        .seeds(100)
        .max_time(Duration::from_secs(3600))
        .run(|w| async move { run_move_streams(w).await.map_err(Into::into) });
}

/// The same, with the sequencer reading through the shared per-cluster
/// reader: its subscription is detached when its tiny buffer
/// fills, comes back through the ring or a catch-up dump, and follows the
/// move to the target's hub. Nothing lost, duplicated or reordered.
#[test]
fn a_stream_sequencer_on_the_shared_reader_keeps_every_change_once_in_order() {
    Sim::new("mover-streams-shared")
        .seeds(100)
        .max_time(Duration::from_secs(3600))
        .run(|w| async move { run_move_streams_shared(w).await.map_err(Into::into) });
}

/// A coordinator prepares distributed-transaction branches on the source
/// while the tenant moves; some coordinators crash (branches in doubt) or
/// stall (a live connection owns the branch). The cutover finishes them
/// through the resolver before and after the fence (cutover steps 2b
/// and 3b); a drain that runs out of time aborts the attempt, which is
/// retried. Under both the rename fence and a kill fence, nothing is lost
/// or committed on the source after `G`.
#[test]
fn prepared_branches_are_drained_at_cutover() {
    Sim::new("mover-xa")
        .seeds(200)
        .max_time(Duration::from_secs(3600))
        .run(|w| async move { run_move_xa(w).await.map_err(Into::into) });
    let (prepared, committed, rolled_back, short, refused) = xa::totals();
    println!(
        "xa: {prepared} branches prepared; cutover drains committed {committed} and rolled back \
         {rolled_back}; {short} drains ran out of time (attempt aborted); {refused} rename fences \
         refused by a prepared branch"
    );
    assert!(committed + rolled_back > 0, "no cutover drained a branch");
    assert!(short > 0, "no drain ran out of time");
}

/// The same without the drain (the cutover before it): under a kill
/// fence a branch committed after `G` is lost, and the checks say so.
/// Run with `--ignored` to see the sim catch it.
#[test]
#[ignore = "shows the bug the drain fixes"]
#[should_panic(expected = "invariant")]
fn without_the_drain_a_late_commit_is_lost() {
    Sim::new("mover-xa-no-drain")
        .seeds(200)
        .max_time(Duration::from_secs(3600))
        .run(|w| async move { run_move_xa_no_drain(w).await.map_err(Into::into) });
}
