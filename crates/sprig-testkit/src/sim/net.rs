//! The simulated network: nodes, links with seeded latency and loss,
//! partitions, and node states (running, paused, crashed).
//!
//! Nothing here opens a socket. Simulated components talk through seams:
//! [`SimNet::transit`] delays and possibly drops one message between two
//! nodes, [`Endpoint`]s carry byte messages between mailboxes, and
//! [`crate::sim::SimEtcd`] and the MariaDB model consult the same network,
//! so one partition cuts every kind of traffic between its sides.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::sync::{mpsc, watch};

use super::rng::SimRng;
use super::trace::Trace;
use crate::faults::{FaultId, LinkFault, Node};

/// Queue length of one mailbox; a message to a full mailbox is lost.
pub const MAILBOX: usize = 1024;

/// The default link inside a data center: 0.2 to 1 ms one way.
pub const DEFAULT_LINK: LinkFault = LinkFault {
    latency: Duration::from_micros(200),
    jitter: Duration::from_micros(800),
    loss: 0.0,
};

/// Why a message didn't arrive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetError {
    /// A partition separates the nodes, or the receiver is crashed.
    Unreachable,
    /// The link lost the message.
    Dropped,
}

impl std::fmt::Display for NetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            NetError::Unreachable => "unreachable",
            NetError::Dropped => "dropped",
        })
    }
}

impl std::error::Error for NetError {}

/// A node's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    /// Runs normally.
    Running,
    /// Frozen: runs no code, sends nothing; messages to it wait.
    Paused,
    /// Down: messages to it are lost. Restarts bump the incarnation.
    Crashed,
}

/// One message in a mailbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    /// The sender.
    pub from: Node,
    /// The bytes sent.
    pub payload: Vec<u8>,
}

#[derive(Debug, Default)]
struct NetState {
    default_link: Option<LinkFault>,
    links: BTreeMap<(Node, Node), LinkFault>,
    degraded: BTreeMap<FaultId, ((Node, Node), LinkFault)>,
    partitions: BTreeMap<FaultId, (BTreeSet<Node>, BTreeSet<Node>)>,
    states: BTreeMap<Node, NodeState>,
    incarnations: BTreeMap<Node, u64>,
    mailboxes: BTreeMap<Node, mpsc::Sender<Envelope>>,
    next_fault: u64,
}

fn pair(a: &Node, b: &Node) -> (Node, Node) {
    if a <= b {
        (a.clone(), b.clone())
    } else {
        (b.clone(), a.clone())
    }
}

impl NetState {
    fn state(&self, n: &Node) -> NodeState {
        self.states.get(n).copied().unwrap_or(NodeState::Running)
    }

    fn cut(&self, a: &Node, b: &Node) -> bool {
        self.partitions
            .values()
            .any(|(x, y)| (x.contains(a) && y.contains(b)) || (x.contains(b) && y.contains(a)))
    }

    fn link(&self, a: &Node, b: &Node) -> LinkFault {
        let key = pair(a, b);
        let base = self
            .links
            .get(&key)
            .copied()
            .unwrap_or(self.default_link.unwrap_or(DEFAULT_LINK));
        // Degradations add up on top of the base link.
        self.degraded
            .values()
            .filter(|(k, _)| *k == key)
            .fold(base, |acc, (_, f)| LinkFault {
                latency: acc.latency + f.latency,
                jitter: acc.jitter + f.jitter,
                loss: 1.0 - (1.0 - acc.loss) * (1.0 - f.loss),
            })
    }

    fn fault_id(&mut self) -> FaultId {
        self.next_fault += 1;
        FaultId(self.next_fault)
    }
}

/// The simulated network. Clones share one network.
#[derive(Debug, Clone)]
pub struct SimNet {
    state: Arc<Mutex<NetState>>,
    changed: Arc<watch::Sender<u64>>,
    rng: SimRng,
    trace: Trace,
}

impl SimNet {
    /// An empty network: every pair of nodes connected by [`DEFAULT_LINK`].
    #[must_use]
    pub fn new(rng: SimRng, trace: Trace) -> Self {
        Self {
            state: Arc::new(Mutex::new(NetState::default())),
            changed: Arc::new(watch::channel(0).0),
            rng,
            trace,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, NetState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn bump(&self) {
        self.changed.send_modify(|v| *v += 1);
    }

    /// A receiver that changes whenever partitions, links or node states
    /// change.
    #[must_use]
    pub fn changes(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    /// The trace this network records to.
    #[must_use]
    pub fn trace(&self) -> &Trace {
        &self.trace
    }

    /// Sets the link used between pairs without their own.
    pub fn set_default_link(&self, link: LinkFault) {
        self.lock().default_link = Some(link);
    }

    /// Sets the base link between `a` and `b`, such as an inter-DC latency.
    pub fn set_link(&self, a: &Node, b: &Node, link: LinkFault) {
        self.lock().links.insert(pair(a, b), link);
        self.bump();
    }

    /// The link between `a` and `b` with every degradation applied.
    #[must_use]
    pub fn link(&self, a: &Node, b: &Node) -> LinkFault {
        self.lock().link(a, b)
    }

    /// A fresh fault id, for faults kept outside the network (pauses,
    /// clock skew).
    #[must_use]
    pub fn allocate_fault_id(&self) -> FaultId {
        self.lock().fault_id()
    }

    /// Cuts `a` from `b`.
    #[allow(clippy::must_use_candidate)] // a permanent fault needs no id
    pub fn partition(&self, a: &[Node], b: &[Node]) -> FaultId {
        let mut s = self.lock();
        let id = s.fault_id();
        s.partitions.insert(
            id,
            (a.iter().cloned().collect(), b.iter().cloned().collect()),
        );
        drop(s);
        self.trace
            .record(format!("net {id} partition {a:?} | {b:?}"));
        self.bump();
        id
    }

    /// Degrades the link between `a` and `b`.
    #[allow(clippy::must_use_candidate)] // a permanent fault needs no id
    pub fn degrade(&self, a: &Node, b: &Node, fault: LinkFault) -> FaultId {
        let mut s = self.lock();
        let id = s.fault_id();
        s.degraded.insert(id, (pair(a, b), fault));
        drop(s);
        self.trace.record(format!(
            "net {id} degrade {a}-{b} +{:?}±{:?} loss {}",
            fault.latency, fault.jitter, fault.loss
        ));
        self.bump();
        id
    }

    /// Removes a partition or degradation; `false` if there was none.
    #[allow(clippy::must_use_candidate)] // healing twice is fine to ignore
    pub fn heal(&self, id: FaultId) -> bool {
        let mut s = self.lock();
        let found = s.partitions.remove(&id).is_some() || s.degraded.remove(&id).is_some();
        drop(s);
        if found {
            self.trace.record(format!("net {id} healed"));
            self.bump();
        }
        found
    }

    /// Removes every partition and degradation and resumes paused nodes.
    pub fn heal_all(&self) {
        let mut s = self.lock();
        s.partitions.clear();
        s.degraded.clear();
        for st in s.states.values_mut() {
            if *st == NodeState::Paused {
                *st = NodeState::Running;
            }
        }
        drop(s);
        self.trace.record("net heal all");
        self.bump();
    }

    /// The node's state.
    #[must_use]
    pub fn state(&self, n: &Node) -> NodeState {
        self.lock().state(n)
    }

    /// Nodes that are crashed now.
    #[must_use]
    pub fn crashed_nodes(&self) -> Vec<Node> {
        let s = self.lock();
        s.states
            .iter()
            .filter(|(_, st)| **st == NodeState::Crashed)
            .map(|(n, _)| n.clone())
            .collect()
    }

    /// The node's incarnation: 0 at first, plus one per restart.
    #[must_use]
    pub fn incarnation(&self, n: &Node) -> u64 {
        self.lock().incarnations.get(n).copied().unwrap_or(0)
    }

    fn set_state(&self, n: &Node, st: NodeState) {
        let mut s = self.lock();
        let old = s.state(n);
        if old == st {
            return;
        }
        if old == NodeState::Crashed && st == NodeState::Running {
            *s.incarnations.entry(n.clone()).or_default() += 1;
        }
        s.states.insert(n.clone(), st);
        drop(s);
        self.trace.record(format!("node {n} {st:?}"));
        self.bump();
    }

    /// Crashes `n`: its supervised task stops, messages to it are lost.
    pub fn crash(&self, n: &Node) {
        self.set_state(n, NodeState::Crashed);
    }

    /// Restarts a crashed node, or resumes a paused one.
    pub fn restart(&self, n: &Node) {
        self.set_state(n, NodeState::Running);
    }

    /// Freezes `n`.
    pub fn pause(&self, n: &Node) {
        if self.state(n) == NodeState::Running {
            self.set_state(n, NodeState::Paused);
        }
    }

    /// Resumes a paused node.
    pub fn resume(&self, n: &Node) {
        if self.state(n) == NodeState::Paused {
            self.set_state(n, NodeState::Running);
        }
    }

    /// Waits until `n` is running (not paused, not crashed).
    pub async fn wait_running(&self, n: &Node) {
        let mut rx = self.changes();
        while self.state(n) != NodeState::Running {
            if rx.changed().await.is_err() {
                return;
            }
        }
    }

    /// Whether a message from `a` would reach `b` now: no partition between
    /// them and neither crashed.
    #[must_use]
    pub fn reachable(&self, a: &Node, b: &Node) -> bool {
        let s = self.lock();
        a == b
            || (!s.cut(a, b)
                && s.state(a) != NodeState::Crashed
                && s.state(b) != NodeState::Crashed)
    }

    /// One message from `from` to `to`: waits while the sender is paused,
    /// then for the link's latency plus seeded jitter. Fails if the link
    /// loses the message or the nodes can't reach each other on arrival.
    ///
    /// # Errors
    /// [`NetError::Dropped`] or [`NetError::Unreachable`].
    pub async fn transit(&self, from: &Node, to: &Node) -> Result<(), NetError> {
        self.wait_running(from).await;
        if from == to {
            return Ok(());
        }
        let link = self.link(from, to);
        let delay = link.latency + self.rng.duration(Duration::ZERO, link.jitter);
        let lost = self.rng.chance(link.loss);
        tokio::time::sleep(delay).await;
        if !self.reachable(from, to) {
            return Err(NetError::Unreachable);
        }
        if lost {
            return Err(NetError::Dropped);
        }
        Ok(())
    }

    /// A mailbox for `node`, replacing any earlier one.
    #[must_use]
    pub fn endpoint(&self, node: &Node) -> Endpoint {
        let (tx, rx) = mpsc::channel(MAILBOX);
        self.lock().mailboxes.insert(node.clone(), tx);
        Endpoint {
            node: node.clone(),
            net: self.clone(),
            rx,
        }
    }

    /// Sends `payload` from `from` to `to`'s mailbox in the background; a
    /// lost message vanishes silently, as on a real network.
    pub fn send(&self, from: &Node, to: &Node, payload: Vec<u8>) {
        let net = self.clone();
        let (from, to) = (from.clone(), to.clone());
        tokio::spawn(async move {
            match net.transit(&from, &to).await {
                Ok(()) => {
                    let tx = net.lock().mailboxes.get(&to).cloned();
                    let delivered = tx.is_some_and(|tx| {
                        tx.try_send(Envelope {
                            from: from.clone(),
                            payload,
                        })
                        .is_ok()
                    });
                    net.trace.record(format!(
                        "msg {from}->{to} {}",
                        if delivered { "delivered" } else { "no mailbox" }
                    ));
                }
                Err(e) => net.trace.record(format!("msg {from}->{to} {e}")),
            }
        });
    }
}

/// A node's mailbox on the simulated network.
#[derive(Debug)]
pub struct Endpoint {
    node: Node,
    net: SimNet,
    rx: mpsc::Receiver<Envelope>,
}

impl Endpoint {
    /// The node this mailbox belongs to.
    #[must_use]
    pub fn node(&self) -> &Node {
        &self.node
    }

    /// Sends bytes to `to`.
    pub fn send(&self, to: &Node, payload: Vec<u8>) {
        self.net.send(&self.node, to, payload);
    }

    /// The next message; `None` once the network is gone.
    pub async fn recv(&mut self) -> Option<Envelope> {
        let env = self.rx.recv().await?;
        // A paused node reads nothing until it resumes.
        self.net.wait_running(&self.node).await;
        Some(env)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn net() -> SimNet {
        SimNet::new(SimRng::new(1), Trace::new())
    }

    #[tokio::test(start_paused = true)]
    async fn partitions_cut_both_ways_and_heal() {
        let n = net();
        let (a, b, c) = (Node::from("a"), Node::from("b"), Node::from("c"));
        let id = n.partition(std::slice::from_ref(&a), std::slice::from_ref(&b));
        assert!(!n.reachable(&a, &b) && !n.reachable(&b, &a));
        assert!(n.reachable(&a, &c));
        assert_eq!(n.transit(&a, &b).await, Err(NetError::Unreachable));
        assert!(n.heal(id));
        assert!(!n.heal(id));
        assert_eq!(n.transit(&a, &b).await, Ok(()));
    }

    #[tokio::test(start_paused = true)]
    async fn latency_is_virtual_and_bounded() {
        let n = net();
        let (a, b) = (Node::from("a"), Node::from("b"));
        n.set_link(&a, &b, LinkFault::latency(Duration::from_millis(30)));
        let t0 = tokio::time::Instant::now();
        n.transit(&a, &b).await.unwrap();
        assert_eq!(t0.elapsed(), Duration::from_millis(30));
        n.degrade(
            &a,
            &b,
            LinkFault {
                latency: Duration::from_millis(10),
                jitter: Duration::from_millis(5),
                loss: 0.0,
            },
        );
        let t1 = tokio::time::Instant::now();
        n.transit(&b, &a).await.unwrap();
        let d = t1.elapsed();
        assert!(
            d >= Duration::from_millis(40) && d <= Duration::from_millis(45),
            "{d:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn mailboxes_deliver_and_crashes_lose() {
        let n = net();
        let (a, b) = (Node::from("a"), Node::from("b"));
        let mut eb = n.endpoint(&b);
        let ea = n.endpoint(&a);
        ea.send(&b, b"hi".to_vec());
        let env = eb.recv().await.unwrap();
        assert_eq!((env.from, env.payload), (a.clone(), b"hi".to_vec()));
        n.crash(&b);
        assert_eq!(n.transit(&a, &b).await, Err(NetError::Unreachable));
        n.restart(&b);
        assert_eq!(n.incarnation(&b), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn paused_senders_wait() {
        let n = net();
        let (a, b) = (Node::from("a"), Node::from("b"));
        n.set_link(&a, &b, LinkFault::latency(Duration::from_millis(1)));
        n.pause(&a);
        let n2 = n.clone();
        let a2 = a.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            n2.resume(&a2);
        });
        let t0 = tokio::time::Instant::now();
        n.transit(&a, &b).await.unwrap();
        assert_eq!(t0.elapsed(), Duration::from_millis(3001));
    }
}
