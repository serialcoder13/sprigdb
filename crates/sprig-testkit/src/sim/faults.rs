//! Faults in a simulation: [`SimFaults`] implements [`Faults`] on the
//! simulated network and clocks, and a [`FaultPlan`] is a seeded schedule
//! of faults that the harness can shrink when a run fails.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sprig_common::Result;

use super::clock::SimClock;
use super::net::SimNet;
use super::rng::SimRng;
use crate::faults::{FaultId, Faults, LinkFault, Node};

/// The node clocks of a world, created on first use.
#[derive(Debug, Clone, Default)]
pub struct Clocks(Arc<Mutex<BTreeMap<Node, Arc<SimClock>>>>);

impl Clocks {
    /// `node`'s clock. Call inside the simulation's runtime.
    #[must_use]
    pub fn get(&self, node: &Node) -> Arc<SimClock> {
        let mut m = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(
            m.entry(node.clone())
                .or_insert_with(|| Arc::new(SimClock::new())),
        )
    }

    /// Every node that has a clock: every node the world has seen.
    #[must_use]
    pub fn nodes(&self) -> Vec<Node> {
        let m = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        m.keys().cloned().collect()
    }
}

#[derive(Debug, Clone)]
enum Held {
    Net,
    Pause(Node),
    Skew(Node),
}

/// [`Faults`] in a simulation.
#[derive(Debug, Clone)]
pub struct SimFaults {
    net: SimNet,
    clocks: Clocks,
    rng: SimRng,
    held: Arc<Mutex<BTreeMap<FaultId, Held>>>,
    restart_after: (Duration, Duration),
}

impl SimFaults {
    /// Faults on `net` and `clocks`; killed nodes restart after a seeded
    /// 0.1 to 2 s.
    #[must_use]
    pub fn new(net: SimNet, clocks: Clocks, rng: SimRng) -> Self {
        Self {
            net,
            clocks,
            rng,
            held: Arc::default(),
            restart_after: (Duration::from_millis(100), Duration::from_secs(2)),
        }
    }

    /// Sets the range a killed node's restart delay is drawn from.
    #[must_use]
    pub fn restart_after(mut self, lo: Duration, hi: Duration) -> Self {
        self.restart_after = (lo, hi);
        self
    }

    fn hold(&self, id: FaultId, h: Held) {
        self.held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id, h);
    }

    /// Crashes `n` without restarting it.
    pub fn crash(&self, n: &Node) {
        self.net.crash(n);
    }

    /// Restarts a crashed node.
    pub fn restart(&self, n: &Node) {
        self.net.restart(n);
    }
}

#[async_trait::async_trait]
impl Faults for SimFaults {
    async fn partition(&self, a: &[Node], b: &[Node]) -> Result<FaultId> {
        let id = self.net.partition(a, b);
        self.hold(id, Held::Net);
        Ok(id)
    }

    async fn link(&self, a: &Node, b: &Node, fault: LinkFault) -> Result<FaultId> {
        let id = self.net.degrade(a, b, fault);
        self.hold(id, Held::Net);
        Ok(id)
    }

    async fn kill(&self, n: &Node) -> Result<()> {
        self.net.crash(n);
        let delay = self
            .rng
            .duration(self.restart_after.0, self.restart_after.1);
        let (net, n) = (self.net.clone(), n.clone());
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            net.restart(&n);
        });
        Ok(())
    }

    async fn pause(&self, n: &Node) -> Result<FaultId> {
        let id = self.net.allocate_fault_id();
        self.net.pause(n);
        self.hold(id, Held::Pause(n.clone()));
        Ok(id)
    }

    async fn skew_clock(&self, n: &Node, offset_ms: i64, rate_ppm: i64) -> Result<FaultId> {
        let id = self.net.allocate_fault_id();
        let c = self.clocks.get(n);
        c.set_wall_offset_ms(offset_ms);
        c.set_rate_ppm(rate_ppm);
        self.net.trace().record(format!(
            "clock {id} {n} offset {offset_ms} ms rate {rate_ppm} ppm"
        ));
        self.hold(id, Held::Skew(n.clone()));
        Ok(id)
    }

    async fn heal(&self, id: FaultId) -> Result<()> {
        let h = self
            .held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&id);
        match h {
            Some(Held::Net) => {
                self.net.heal(id);
            }
            Some(Held::Pause(n)) => self.net.resume(&n),
            Some(Held::Skew(n)) => {
                self.clocks.get(&n).reset_skew();
                self.net.trace().record(format!("clock {id} {n} healed"));
            }
            None => {}
        }
        Ok(())
    }

    async fn heal_all(&self) -> Result<()> {
        let all: Vec<FaultId> = self
            .held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .copied()
            .collect();
        for id in all {
            self.heal(id).await?;
        }
        self.net.heal_all();
        Ok(())
    }
}

/// One fault of a [`FaultPlan`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FaultKind {
    /// Cut `a` from `b` for `for_ms`.
    Partition {
        /// One side.
        a: Vec<Node>,
        /// The other side.
        b: Vec<Node>,
        /// How long.
        for_ms: u64,
    },
    /// Degrade the link between `a` and `b` for `for_ms`.
    Link {
        /// One end.
        a: Node,
        /// The other end.
        b: Node,
        /// Added latency.
        latency_ms: u64,
        /// Added jitter.
        jitter_ms: u64,
        /// Loss probability.
        loss: f64,
        /// How long.
        for_ms: u64,
    },
    /// Kill the node; it restarts on its own.
    Kill {
        /// The node.
        node: Node,
    },
    /// Freeze the node for `for_ms`.
    Pause {
        /// The node.
        node: Node,
        /// How long.
        for_ms: u64,
    },
    /// Skew the node's clock for `for_ms`.
    Skew {
        /// The node.
        node: Node,
        /// Wall clock offset.
        offset_ms: i64,
        /// Rate error.
        rate_ppm: i64,
        /// How long.
        for_ms: u64,
    },
}

/// A fault at a point of virtual time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlannedFault {
    /// Milliseconds after the plan starts.
    pub at_ms: u64,
    /// The fault.
    #[serde(flatten)]
    pub fault: FaultKind,
}

/// A schedule of faults (chapter 46, `simulate` step 2).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FaultPlan {
    /// The faults, in time order.
    #[serde(default)]
    pub faults: Vec<PlannedFault>,
}

impl FaultPlan {
    /// `n` random faults among `nodes` within `horizon`: partitions that
    /// isolate one node or split the nodes in two, slow lossy links, kills,
    /// pauses and clock skews, each lasting up to a fifth of the horizon.
    #[must_use]
    pub fn random(rng: &SimRng, nodes: &[Node], horizon: Duration, n: usize) -> Self {
        let h = u64::try_from(horizon.as_millis())
            .unwrap_or(u64::MAX)
            .max(1);
        let mut faults = Vec::with_capacity(n);
        if nodes.is_empty() {
            return Self { faults };
        }
        for _ in 0..n {
            let at_ms = rng.below(h);
            let for_ms = rng.range(100..(h / 5).max(101));
            let node = rng.pick(nodes).clone();
            let fault = match rng.below(6) {
                0 | 1 if nodes.len() > 1 => {
                    let mut all = nodes.to_vec();
                    rng.shuffle(&mut all);
                    // Isolate one node, or split into two groups.
                    let cut = if rng.chance(0.5) {
                        1
                    } else {
                        1 + rng.index(all.len() - 1)
                    };
                    let b = all.split_off(cut);
                    FaultKind::Partition { a: all, b, for_ms }
                }
                2 if nodes.len() > 1 => {
                    let mut other = rng.pick(nodes).clone();
                    if other == node {
                        other = nodes[(nodes.iter().position(|x| *x == node).unwrap_or(0) + 1)
                            % nodes.len()]
                        .clone();
                    }
                    FaultKind::Link {
                        a: node,
                        b: other,
                        latency_ms: rng.range(5..500),
                        jitter_ms: rng.range(0..100),
                        loss: if rng.chance(0.3) { 0.2 } else { 0.0 },
                        for_ms,
                    }
                }
                3 => FaultKind::Kill { node },
                4 => FaultKind::Pause { node, for_ms },
                _ => FaultKind::Skew {
                    node,
                    offset_ms: i64::try_from(rng.range(0..4000)).unwrap_or(0) - 2000,
                    rate_ppm: i64::try_from(rng.range(0..2000)).unwrap_or(0) - 1000,
                    for_ms,
                },
            };
            faults.push(PlannedFault { at_ms, fault });
        }
        faults.sort_by_key(|f| f.at_ms);
        Self { faults }
    }

    /// Whether the plan has no faults.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.faults.is_empty()
    }

    /// Runs the plan on `faults`: each fault at its time, each heal after
    /// its duration. Resolves when the last fault has been injected and
    /// healed.
    pub async fn run(&self, faults: Arc<dyn Faults>) {
        let start = tokio::time::Instant::now();
        let mut heals = Vec::new();
        for pf in &self.faults {
            tokio::time::sleep_until(start + Duration::from_millis(pf.at_ms)).await;
            let r = match &pf.fault {
                FaultKind::Partition { a, b, for_ms } => {
                    faults.partition(a, b).await.map(|id| Some((id, *for_ms)))
                }
                FaultKind::Link {
                    a,
                    b,
                    latency_ms,
                    jitter_ms,
                    loss,
                    for_ms,
                } => faults
                    .link(
                        a,
                        b,
                        LinkFault {
                            latency: Duration::from_millis(*latency_ms),
                            jitter: Duration::from_millis(*jitter_ms),
                            loss: *loss,
                        },
                    )
                    .await
                    .map(|id| Some((id, *for_ms))),
                FaultKind::Kill { node } => faults.kill(node).await.map(|()| None),
                FaultKind::Pause { node, for_ms } => {
                    faults.pause(node).await.map(|id| Some((id, *for_ms)))
                }
                FaultKind::Skew {
                    node,
                    offset_ms,
                    rate_ppm,
                    for_ms,
                } => faults
                    .skew_clock(node, *offset_ms, *rate_ppm)
                    .await
                    .map(|id| Some((id, *for_ms))),
            };
            if let Ok(Some((id, for_ms))) = r {
                let f = Arc::clone(&faults);
                heals.push(tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(for_ms)).await;
                    let _ = f.heal(id).await;
                }));
            }
        }
        for h in heals {
            let _ = h.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::Trace;
    use crate::sim::net::NodeState;

    #[test]
    fn random_plans_are_seeded_and_sorted() {
        let nodes: Vec<Node> = ["a", "b", "c"].into_iter().map(Node::from).collect();
        let p1 = FaultPlan::random(&SimRng::new(9), &nodes, Duration::from_secs(60), 20);
        let p2 = FaultPlan::random(&SimRng::new(9), &nodes, Duration::from_secs(60), 20);
        assert_eq!(p1, p2);
        assert_eq!(p1.faults.len(), 20);
        assert!(p1.faults.windows(2).all(|w| w[0].at_ms <= w[1].at_ms));
        // Round-trips through TOML, as regression cases store it.
        let text = toml::to_string(&p1).unwrap();
        assert_eq!(toml::from_str::<FaultPlan>(&text).unwrap(), p1);
    }

    #[tokio::test(start_paused = true)]
    async fn plans_inject_and_heal() {
        let net = SimNet::new(SimRng::new(1), Trace::new());
        let f = Arc::new(SimFaults::new(
            net.clone(),
            Clocks::default(),
            SimRng::new(2),
        ));
        let plan = FaultPlan {
            faults: vec![
                PlannedFault {
                    at_ms: 100,
                    fault: FaultKind::Partition {
                        a: vec![Node::from("a")],
                        b: vec![Node::from("b")],
                        for_ms: 1000,
                    },
                },
                PlannedFault {
                    at_ms: 200,
                    fault: FaultKind::Pause {
                        node: Node::from("c"),
                        for_ms: 500,
                    },
                },
            ],
        };
        let (a, b, c) = (Node::from("a"), Node::from("b"), Node::from("c"));
        let run = tokio::spawn({
            let plan = plan.clone();
            let f: Arc<dyn Faults> = f.clone();
            async move { plan.run(f).await }
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!net.reachable(&a, &b));
        assert_eq!(net.state(&c), NodeState::Paused);
        run.await.unwrap();
        assert!(net.reachable(&a, &b));
        assert_eq!(net.state(&c), NodeState::Running);
    }
}
