use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::*;
use crate::faults::LinkFault;

async fn ping_pong(world: World) -> SimResult {
    let (a, b) = (Node::from("a"), Node::from("b"));
    world.net().set_link(
        &a,
        &b,
        LinkFault {
            latency: Duration::from_millis(5),
            jitter: Duration::from_millis(20),
            loss: 0.1,
        },
    );
    let mut eb = world.net().endpoint(&b);
    let ea = world.net().endpoint(&a);
    for i in 0..50u8 {
        ea.send(&b, vec![i]);
    }
    let mut got = Vec::new();
    while let Ok(Some(env)) = tokio::time::timeout(Duration::from_secs(1), eb.recv()).await {
        got.push(env.payload[0]);
    }
    world.record(format!("received {got:?}"));
    if got.is_empty() {
        return Err("nothing arrived".into());
    }
    Ok(())
}

#[test]
fn a_seed_replays_exactly() {
    let sim = Sim::new("unit-replay");
    let body = |w: World, _p: FaultPlan| ping_pong(w);
    let a = sim.run_once(42, &FaultPlan::default(), &body);
    let b = sim.run_once(42, &FaultPlan::default(), &body);
    let c = sim.run_once(43, &FaultPlan::default(), &body);
    assert_eq!(a.result, Ok(()));
    assert_eq!(a.digest, b.digest);
    assert_eq!(a.trace, b.trace);
    // Jitter and loss differ by seed, so the delivery order does too.
    assert_ne!(a.digest, c.digest);
}

#[test]
fn many_seeds_pass() {
    Sim::new("unit-many").seeds(20).run(ping_pong);
}

#[test]
fn a_failure_names_its_seed_and_shrinks_the_plan() {
    let nodes: Vec<Node> = ["x", "y", "z"].into_iter().map(Node::from).collect();
    let culprit = |p: &FaultPlan| {
        p.faults
            .iter()
            .any(|f| matches!(&f.fault, FaultKind::Kill { node } if node.as_str() == "x"))
    };
    // Find a seed whose random plan contains the culprit.
    let seed = (0..1000)
        .find(|s| {
            culprit(&FaultPlan::random(
                &plan_rng(*s),
                &nodes,
                Duration::from_secs(10),
                12,
            ))
        })
        .unwrap();
    let sim = Sim::new("unit-shrink").seeds(1);
    let r = std::panic::catch_unwind(AssertUnwindSafe(|| {
        // Run just that seed through the public entry point.
        let body = |w: World, p: FaultPlan| async move {
            w.run_plan(&p).await;
            if culprit(&p) {
                return Err("x was killed".into());
            }
            Ok(())
        };
        let case = RegressionCase {
            suite: "unit-shrink".into(),
            seed,
            failure: String::new(),
            plan: FaultPlan::random(&plan_rng(seed), &nodes, Duration::from_secs(10), 12),
        };
        let out = sim.run_once(seed, &case.plan, &body);
        let msg = out.result.clone().unwrap_err();
        sim.report_failure(&case, &msg, &out, &body);
    }));
    let msg = panic_message(&*r.unwrap_err());
    assert!(msg.contains(&format!("SPRIG_SIM_SEED={seed}")), "{msg}");
    assert!(msg.contains("shrunk to 1"), "{msg}");
    assert!(msg.contains("x was killed"), "{msg}");
}

#[test]
fn stuck_runs_fail_on_virtual_time() {
    let sim = Sim::new("unit-stuck").max_time(Duration::from_secs(30));
    let out = sim.run_once(1, &FaultPlan::default(), &|_w: World, _p| async {
        std::future::pending::<()>().await;
        Ok(())
    });
    assert!(out.result.unwrap_err().contains("did not finish"));
}

#[test]
fn panics_in_spawned_tasks_fail_the_run() {
    let sim = Sim::new("unit-panic");
    let out = sim.run_once(1, &FaultPlan::default(), &|w: World, _p| async move {
        let _ = w
            .spawn("boom", async {
                panic!("invariant broken");
            })
            .await;
        Ok(())
    });
    assert!(out.result.unwrap_err().contains("invariant broken"));
}

#[test]
fn supervised_nodes_freeze_die_and_restart() {
    let sim = Sim::new("unit-nodes");
    let out = sim.run_once(7, &FaultPlan::default(), &|w: World, _p| async move {
        let n = Node::from("svc");
        let ticks = Arc::new(AtomicU64::new(0));
        let starts = Arc::new(AtomicU64::new(0));
        {
            let (ticks, starts) = (Arc::clone(&ticks), Arc::clone(&starts));
            w.spawn_node(&n, move |ctx| {
                let ticks = Arc::clone(&ticks);
                starts.fetch_add(1, Ordering::SeqCst);
                async move {
                    assert_eq!(ctx.node.as_str(), "svc");
                    loop {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        ticks.fetch_add(1, Ordering::SeqCst);
                    }
                }
            });
        }
        w.sleep(Duration::from_millis(1050)).await;
        assert_eq!(ticks.load(Ordering::SeqCst), 10);

        let f = w.faults().clone();
        let id = f.pause(&n).await?;
        w.sleep(Duration::from_secs(5)).await;
        assert_eq!(ticks.load(Ordering::SeqCst), 10, "paused node ran");
        f.heal(id).await?;
        w.sleep(Duration::from_millis(10)).await;
        // The overdue sleep fires once on resume.
        assert_eq!(ticks.load(Ordering::SeqCst), 11);

        f.kill(&n).await?;
        assert_eq!(w.net().state(&n), NodeState::Crashed);
        w.sleep(Duration::from_secs(3)).await;
        assert_eq!(w.net().state(&n), NodeState::Running);
        assert_eq!(starts.load(Ordering::SeqCst), 2);
        assert_eq!(w.net().incarnation(&n), 1);
        Ok(())
    });
    assert_eq!(out.result, Ok(()), "{}", out.trace.join("\n"));
}

#[test]
fn seed_variable_selects_one_seed() {
    // Only checks parsing; the variable itself isn't set in unit tests.
    let s = Sim::new("unit-seeds").seeds(3);
    if skip::var(SEED_VAR).is_none() && skip::var(SEEDS_VAR).is_none() {
        let list = s.seed_list();
        assert_eq!(list.len(), 3);
        assert_eq!(list[1], list[0].wrapping_add(1));
    }
}
