//! Leader election under faults, in simulation: only the token holder's
//! writes commit, and lease arithmetic holds under clock rate skew.
//!
//! Three controllers run sprig-meta's real `Election` against the
//! simulated etcd. Each, while it leads, commits a guarded counter
//! increment every 200 ms. A seeded plan partitions, pauses, kills and
//! skews them. Checked:
//!
//! 1. at every sample, at most one running controller believes it leads;
//! 2. guarded writes commit in token order, and each token belongs to one
//!    holder;
//! 3. after healing, a leader is elected and commits within 30 s.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sprig_common::time::Clock;
use sprig_meta::{Election, Elector, LeaderRecord, Op, SessionOpts, Tier};
use sprig_testkit::faults::Node;
use sprig_testkit::sim::{FaultPlan, NodeState, Sim, SimResult, World};

const NODES: [&str; 3] = ["c1", "c2", "c3"];

/// One successful guarded write: (store revision, token, holder).
type Commit = (u64, u64, String);

#[derive(Default)]
struct Shared {
    commits: Vec<Commit>,
    /// Who currently believes it leads: node -> leadership.
    leading: BTreeMap<String, sprig_meta::Leadership>,
}

fn controller(world: &World, node: &Node, shared: Arc<Mutex<Shared>>) {
    world.spawn_node(node, move |ctx| {
        let shared = Arc::clone(&shared);
        async move {
            let store = ctx.meta(Tier::Global);
            let clock: Arc<dyn Clock> = ctx.clock();
            let opts = SessionOpts::new(Duration::from_secs(10));
            let election = Election::new(Arc::clone(&store), clock, opts);
            let key = Tier::Global.key("leaders/controller").expect("key");
            let counter = Tier::Global.key("counter").expect("key");
            let me = LeaderRecord {
                holder: ctx.node.to_string(),
                dc: None,
                build: "sim".into(),
            };
            loop {
                let Ok(l) = election.campaign(&key, &me).await else {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                };
                ctx.world
                    .record(format!("{} leads with token {}", ctx.node, l.token));
                // The guard takes the entry out when the node dies: a
                // Leadership clone outliving the node would keep its
                // session's keep-alive task running, a zombie.
                let _guard = Believer::new(&shared, &ctx.node, &l);
                let mut n = 0u64;
                while l.is_leading() {
                    n += 1;
                    let op = Op::Put {
                        key: counter.clone(),
                        value: format!("{}:{n}", ctx.node).into_bytes(),
                        lease: None,
                    };
                    match l.commit(vec![op]).await {
                        Ok(r) => {
                            lock(&shared)
                                .commits
                                .push((r.revision, l.token, ctx.node.to_string()));
                        }
                        Err(e) if e.retryable => {}
                        Err(_) => break,
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                ctx.world.record(format!("{} stepped down", ctx.node));
            }
        }
    });
}

/// Registers a node as believing it leads, until dropped.
struct Believer {
    shared: Arc<Mutex<Shared>>,
    node: String,
}

impl Believer {
    fn new(shared: &Arc<Mutex<Shared>>, node: &Node, l: &sprig_meta::Leadership) -> Self {
        lock(shared).leading.insert(node.to_string(), l.clone());
        Self {
            shared: Arc::clone(shared),
            node: node.to_string(),
        }
    }
}

impl Drop for Believer {
    fn drop(&mut self) {
        lock(&self.shared).leading.remove(&self.node);
    }
}

fn lock(m: &Mutex<Shared>) -> std::sync::MutexGuard<'_, Shared> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

async fn scenario(world: World, plan: FaultPlan) -> SimResult {
    let shared = Arc::new(Mutex::new(Shared::default()));
    let nodes: Vec<Node> = NODES.into_iter().map(Node::from).collect();
    for n in &nodes {
        controller(&world, n, Arc::clone(&shared));
    }
    // Sample "who believes it leads" every 50 ms while the plan runs.
    let sampler = {
        let (world, shared, nodes) = (world.clone(), Arc::clone(&shared), nodes.clone());
        world.clone().spawn("sampler", async move {
            loop {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let s = lock(&shared);
                let believers: Vec<&String> = s
                    .leading
                    .iter()
                    .filter(|(n, l)| {
                        let node = Node::from(n.as_str());
                        nodes.contains(&node)
                            && world.net().state(&node) == NodeState::Running
                            && l.is_leading()
                    })
                    .map(|(n, _)| n)
                    .collect();
                if believers.len() > 1 {
                    world.fail(format!("two leaders at once: {believers:?}"));
                }
            }
        })
    };
    world.run_plan(&plan).await;
    world.heal_all().await;
    let healed_at = lock(&shared).commits.len();
    world.sleep(Duration::from_secs(30)).await;
    sampler.abort();

    let s = lock(&shared);
    if s.commits.len() == healed_at {
        return Err("no leader committed within 30 s of healing".into());
    }
    let mut by_rev = s.commits.clone();
    by_rev.sort();
    let mut owner: BTreeMap<u64, &str> = BTreeMap::new();
    for w in by_rev.windows(2) {
        if w[1].1 < w[0].1 {
            return Err(format!("token went back: {:?} then {:?}", w[0], w[1]).into());
        }
    }
    for (_, token, holder) in &by_rev {
        if let Some(prev) = owner.insert(*token, holder)
            && prev != holder
        {
            return Err(format!("token {token} used by {prev} and {holder}").into());
        }
    }
    world.record(format!("{} guarded commits checked", by_rev.len()));
    Ok(())
}

#[test]
fn one_leader_under_faults() {
    let nodes: Vec<Node> = NODES.into_iter().map(Node::from).collect();
    Sim::new("election").seeds(60).run_with_faults(
        |rng| FaultPlan::random(rng, &nodes, Duration::from_secs(120), 8),
        scenario,
    );
}
