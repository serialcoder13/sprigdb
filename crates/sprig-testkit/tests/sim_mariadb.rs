//! The MariaDB model inside a simulation: writes to a semi-sync primary
//! under faults, then the primary is lost and a replica promoted
//! as failover does. Checked:
//!
//! 1. no phantom rows: the new primary holds only rows a client wrote;
//! 2. an acknowledged write missing after the failover is listed among
//!    the GTIDs the failover lost (the diverged-transaction report is
//!    complete);
//! 3. while semi-sync never fell back to asynchronous, no acknowledged
//!    write is lost.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sprig_testkit::faults::Node;
use sprig_testkit::sim::mariadb::{Gtid, Outcome, ServerConfig, SimMariaDb, Stmt};
use sprig_testkit::sim::{FaultKind, FaultPlan, Sim, SimResult, World};

const SCHEMA: &str = "app";

fn model(world: &World) -> SimMariaDb {
    SimMariaDb::new(world, |m| {
        let mut p = ServerConfig::new("p", 201, 2);
        p.semisync_master = true;
        m.add_server(p);
        m.add_replica(
            ServerConfig::new("r1", 202, 2),
            "p",
            Duration::from_millis(50),
        );
        m.add_replica(
            ServerConfig::new("r2", 203, 2),
            "p",
            Duration::from_millis(80),
        );
    })
}

#[derive(Default)]
struct Log {
    /// key -> GTID of acknowledged inserts (None if the GTID read failed).
    acked: BTreeMap<i64, Option<Gtid>>,
    attempted: BTreeSet<i64>,
    next_key: i64,
}

async fn writer(world: World, db: SimMariaDb, log: Arc<Mutex<Log>>, until: Duration) {
    let me = Node::from("app");
    let mut conn = None;
    while world.elapsed() < until {
        if conn.is_none() {
            conn = db.connect(&me, "p", None, SCHEMA).await.ok();
            if conn.is_none() {
                world.sleep(Duration::from_millis(200)).await;
                continue;
            }
        }
        let c = conn.as_ref().expect("connected");
        let k = {
            let mut l = log.lock().expect("log lock");
            l.next_key += 1;
            let k = l.next_key;
            l.attempted.insert(k);
            k
        };
        let o = c
            .exec(&Stmt::Insert {
                t: "t".into(),
                k,
                v: format!("v{k}"),
            })
            .await;
        match o {
            Outcome::Ok { .. } => {
                let g = c.exec(&Stmt::LastGtid).await;
                let gtid = g.scalar().and_then(|s| s.parse().ok());
                log.lock().expect("log lock").acked.insert(k, gtid);
            }
            Outcome::Err(_) => conn = None,
        }
        world
            .sleep(Duration::from_millis(world.rng().range(10..100)))
            .await;
    }
}

async fn scenario(world: World, plan: FaultPlan) -> SimResult {
    let db = model(&world);
    {
        let c = db
            .connect(&Node::from("setup"), "p", None, SCHEMA)
            .await
            .map_err(|e| format!("setup: {e}"))?;
        c.exec(&Stmt::CreateTable("t".into())).await;
    }
    let log = Arc::new(Mutex::new(Log::default()));
    // Phase 1: faults while writing.
    let w = world.spawn(
        "writer",
        writer(
            world.clone(),
            db.clone(),
            Arc::clone(&log),
            Duration::from_secs(60),
        ),
    );
    world.run_plan(&plan).await;
    world.heal_all().await;
    let _ = w.await;
    world.sleep(Duration::from_secs(2)).await;

    // Phase 2: the primary is cut off while the writer goes on: commits
    // wait for an ack, then semi-sync falls back and they are acked
    // unreplicated.
    let isolate = world
        .net()
        .partition(&[Node::from("p")], &[Node::from("r1"), Node::from("r2")]);
    let until = world.elapsed() + Duration::from_secs(25);
    writer(world.clone(), db.clone(), Arc::clone(&log), until).await;

    // Fail over to the replica with the most GTIDs.
    let (new, lost) = db.with_model(|m, now| {
        let best = ["r1", "r2"]
            .into_iter()
            .max_by_key(|r| m.gtids(r).len())
            .expect("replicas");
        m.promote(best, true, now);
        (best.to_owned(), m.lost_gtids("p", best))
    });
    world.net().heal(isolate);
    let rows: BTreeSet<i64> = db
        .with_model(|m, _| m.rows(&new, SCHEMA, "t"))
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    let log = log.lock().expect("log lock");
    if let Some(k) = rows.iter().find(|k| !log.attempted.contains(k)) {
        return Err(format!("phantom row {k} on {new}").into());
    }
    let lost_set: BTreeSet<Gtid> = lost.iter().copied().collect();
    let fell_back = world
        .trace()
        .lines()
        .iter()
        .any(|l| l.contains("semi-sync off"));
    let mut lost_acked = 0;
    for (k, g) in &log.acked {
        if rows.contains(k) {
            continue;
        }
        lost_acked += 1;
        match g {
            Some(g) if !lost_set.contains(g) => {
                return Err(format!("acked key {k} ({g}) lost but not reported").into());
            }
            _ => {}
        }
    }
    if lost_acked > 0 && !fell_back {
        return Err(format!("{lost_acked} acked writes lost with semi-sync on").into());
    }
    world.record(format!(
        "failover to {new}: {} acked, {lost_acked} lost, {} GTIDs diverged",
        log.acked.len(),
        lost.len()
    ));
    Ok(())
}

#[test]
fn failover_reports_every_lost_write() {
    let nodes: Vec<Node> = ["app", "p", "r1", "r2"]
        .into_iter()
        .map(Node::from)
        .collect();
    Sim::new("mariadb-failover").seeds(30).run_with_faults(
        |rng| {
            let mut p = FaultPlan::random(rng, &nodes, Duration::from_secs(50), 6);
            // The primary is lost on purpose in phase 2; here keep it up.
            p.faults
                .retain(|f| !matches!(&f.fault, FaultKind::Kill { node } if node.as_str() == "p"));
            p
        },
        scenario,
    );
}
