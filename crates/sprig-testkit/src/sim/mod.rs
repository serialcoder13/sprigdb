//! Deterministic simulation (chapter 46, "Deterministic simulation"; build
//! step 2).
//!
//! # How it works
//!
//! A run is one seed. The harness builds a current-thread tokio runtime
//! with paused time, so tasks run one at a time in a fixed order and
//! virtual time jumps to the next timer whenever every task waits: an hour
//! of leases, timeouts and failovers takes milliseconds. Production code
//! needs no changes: it already reaches time through
//! [`sprig_common::time::Clock`] (here a [`SimClock`] per node) and
//! tokio's timer, etcd through [`sprig_meta::MetaStore`] (here
//! [`SimEtcd`], a `MemStore` behind the simulated network) and MariaDB
//! through a backend trait (here the [`mariadb`] model). Every other
//! source of variation (network latency and loss, fault times, restart
//! delays, workload choices) comes from one [`SimRng`] seeded by the run's
//! seed.
//!
//! ```no_run
//! use std::time::Duration;
//! use sprig_testkit::sim::{FaultPlan, Sim};
//! use sprig_testkit::faults::Node;
//!
//! #[test]
//! fn leader_is_unique() {
//!     Sim::new("election").seeds(50).run_with_faults(
//!         |rng| FaultPlan::random(rng, &[Node::from("c1"), Node::from("c2")],
//!                                 Duration::from_secs(60), 5),
//!         |world, plan| async move {
//!             // start nodes with world.spawn_node, run the plan, then
//!             world.run_plan(&plan).await;
//!             world.heal_all().await;
//!             // check invariants; return Err to fail the seed
//!             Ok(())
//!         },
//!     );
//! }
//! ```
//!
//! # Seeds, failures and replay
//!
//! `Sim::run*` runs the suite's regression cases (files
//! `tests/regressions/<suite>/*.toml` of the crate under test), then
//! `SPRIG_SIM_SEEDS` seeds (default: the suite's `seeds`, else 200,
//! `testkit.sim_seeds`) from a base that is random per process unless
//! `SPRIG_SIM_SEED_BASE` fixes it. `SPRIG_SIM_SEED=<n>` runs that one seed
//! only. A failing seed panics the test with the seed, the command to
//! replay it, the end of its trace, and (when the plan had faults) a
//! fault plan shrunk by delta debugging, written as a regression case to
//! `$SPRIG_TEST_LOG_DIR/regressions/<suite>/` (or the system temp dir)
//! next to the failing run's full trace.
//!
//! `SPRIG_SIM_TRACE=1` prints every run's trace (run with `--nocapture`).
//!
//! # Determinism
//!
//! The first seed of every run, and every failing seed, runs twice; a
//! different trace is a determinism bug (`simulate` step 5). tokio's
//! `select!` picks among ready branches at random: with
//! `RUSTFLAGS="--cfg tokio_unstable"` the harness seeds that choice too
//! and a trace mismatch fails the test; without it a mismatch is only
//! printed as a warning. Simulation code in this crate uses `biased`
//! selects.
//!
//! **Decision, against the book.** Chapter 46 picks madsim. Its
//! replacement crates trail the versions SprigDB pins (madsim-etcd-client
//! 0.7 wraps etcd-client 0.18, SprigDB uses 0.21), nothing simulates
//! `mysql_async`, and madsim needs `--cfg madsim` across the whole
//! dependency graph. Every component SprigDB simulates already sits behind
//! a seam (`Clock`, `MetaStore`, the backend trait), so this seeded tokio
//! runtime gives the same replayable runs without a second build of the
//! world.

pub mod clock;
pub mod faults;
pub mod mariadb;
pub mod meta;
pub mod mover;
pub mod net;
pub mod rng;
pub mod trace;

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures::FutureExt;
use serde::{Deserialize, Serialize};
use sprig_meta::Tier;

pub use clock::SimClock;
pub use faults::{Clocks, FaultKind, FaultPlan, PlannedFault, SimFaults};
pub use meta::SimEtcd;
pub use net::{Endpoint, NetError, NodeState, SimNet};
pub use rng::SimRng;
pub use trace::Trace;

use crate::faults::{Faults, Node};
use crate::skip;

/// Seed to replay: run only this seed.
pub const SEED_VAR: &str = "SPRIG_SIM_SEED";
/// Number of seeds per suite.
pub const SEEDS_VAR: &str = "SPRIG_SIM_SEEDS";
/// First seed of a run, instead of a random one.
pub const SEED_BASE_VAR: &str = "SPRIG_SIM_SEED_BASE";
/// Set to print every run's trace (with `--nocapture`).
pub const TRACE_VAR: &str = "SPRIG_SIM_TRACE";
/// `testkit.sim_seeds`: seeds per suite when neither the suite nor
/// `SPRIG_SIM_SEEDS` says.
pub const DEFAULT_SEEDS: u64 = 200;

/// What a simulation body returns: `Err` fails the seed.
pub type SimResult = std::result::Result<(), SimError>;
/// A simulation failure.
pub type SimError = Box<dyn std::error::Error + Send + Sync>;

/// The node etcd runs on in every world.
pub const ETCD_NODE: &str = "etcd";

/// Everything one run shares: seed, randomness, network, clocks, etcd,
/// faults and the trace. Cheap to clone.
#[derive(Clone)]
pub struct World {
    inner: Arc<WorldInner>,
}

struct WorldInner {
    seed: u64,
    rng: SimRng,
    net: SimNet,
    trace: Trace,
    clocks: Clocks,
    etcd: SimEtcd,
    faults: SimFaults,
    failures: Mutex<Vec<String>>,
}

impl std::fmt::Debug for World {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("World")
            .field("seed", &self.inner.seed)
            .finish_non_exhaustive()
    }
}

/// What a supervised node's factory gets each time the node (re)starts.
#[derive(Debug, Clone)]
pub struct NodeCtx {
    /// The node.
    pub node: Node,
    /// 0 at the first start, plus one per restart.
    pub incarnation: u64,
    /// The world.
    pub world: World,
}

impl NodeCtx {
    /// The node's clock.
    #[must_use]
    pub fn clock(&self) -> Arc<SimClock> {
        self.world.clock(&self.node)
    }

    /// A `MetaStore` client for this node.
    #[must_use]
    pub fn meta(&self, tier: Tier) -> Arc<dyn sprig_meta::MetaStore> {
        self.world.etcd().client(&self.node, tier)
    }
}

impl World {
    /// A world for `seed`. Call inside a runtime with paused time; the
    /// harness does this for you.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        let rng = SimRng::new(seed);
        let trace = Trace::new();
        let net = SimNet::new(rng.fork(), trace.clone());
        let clocks = Clocks::default();
        let etcd_node = Node::from(ETCD_NODE);
        let etcd = SimEtcd::new(etcd_node.clone(), net.clone(), clocks.get(&etcd_node));
        let faults = SimFaults::new(net.clone(), clocks.clone(), rng.fork());
        Self {
            inner: Arc::new(WorldInner {
                seed,
                rng,
                net,
                trace,
                clocks,
                etcd,
                faults,
                failures: Mutex::new(Vec::new()),
            }),
        }
    }

    /// The run's seed.
    #[must_use]
    pub fn seed(&self) -> u64 {
        self.inner.seed
    }

    /// The run's random numbers.
    #[must_use]
    pub fn rng(&self) -> &SimRng {
        &self.inner.rng
    }

    /// The network.
    #[must_use]
    pub fn net(&self) -> &SimNet {
        &self.inner.net
    }

    /// The trace.
    #[must_use]
    pub fn trace(&self) -> &Trace {
        &self.inner.trace
    }

    /// Records an event in the trace.
    pub fn record(&self, event: impl AsRef<str>) {
        self.inner.trace.record(event);
    }

    /// `node`'s clock.
    #[must_use]
    pub fn clock(&self, node: &Node) -> Arc<SimClock> {
        self.inner.clocks.get(node)
    }

    /// The node clocks.
    #[must_use]
    pub fn clocks(&self) -> &Clocks {
        &self.inner.clocks
    }

    /// The world's etcd.
    #[must_use]
    pub fn etcd(&self) -> &SimEtcd {
        &self.inner.etcd
    }

    /// The fault injector.
    #[must_use]
    pub fn faults(&self) -> &SimFaults {
        &self.inner.faults
    }

    /// Virtual time since the run started.
    #[must_use]
    pub fn elapsed(&self) -> Duration {
        self.inner.trace.elapsed()
    }

    /// Sleeps in virtual time.
    pub async fn sleep(&self, d: Duration) {
        tokio::time::sleep(d).await;
    }

    /// Yields to other tasks 0 to 3 times, at random: call at await points
    /// of interest to vary interleavings by seed.
    pub async fn chaos_yield(&self) {
        for _ in 0..self.inner.rng.below(4) {
            tokio::task::yield_now().await;
        }
    }

    /// Fails the run with `msg` once the body returns; for checks inside
    /// spawned tasks.
    pub fn fail(&self, msg: impl Into<String>) {
        let msg = msg.into();
        self.record(format!("FAIL {msg}"));
        self.inner
            .failures
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(msg);
    }

    /// Failures recorded so far.
    #[must_use]
    pub fn failures(&self) -> Vec<String> {
        self.inner
            .failures
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Spawns a task whose panic fails the run (tokio would otherwise
    /// swallow it).
    pub fn spawn<F>(&self, name: &str, fut: F) -> tokio::task::JoinHandle<Option<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let world = self.clone();
        let name = name.to_owned();
        tokio::spawn(async move {
            match AssertUnwindSafe(fut).catch_unwind().await {
                Ok(v) => Some(v),
                Err(p) => {
                    world.fail(format!("task {name} panicked: {}", panic_message(&*p)));
                    None
                }
            }
        })
    }

    /// Runs a process on `node` under a supervisor: the factory's future
    /// is frozen while the node is paused, dropped when the node crashes,
    /// and made anew (with the next incarnation) when it restarts. Spawn
    /// the node's own tasks so that dropping its future stops them (for
    /// example, own their `JoinHandle`s and abort on drop).
    pub fn spawn_node<F, Fut>(&self, node: &Node, factory: F)
    where
        F: Fn(NodeCtx) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let world = self.clone();
        let node = node.clone();
        let name = format!("node {node}");
        self.spawn(&name, async move {
            let net = world.net().clone();
            let mut changes = net.changes();
            loop {
                while net.state(&node) == NodeState::Crashed {
                    if changes.changed().await.is_err() {
                        return;
                    }
                }
                let incarnation = net.incarnation(&node);
                world.record(format!("node {node} start #{incarnation}"));
                let ctx = NodeCtx {
                    node: node.clone(),
                    incarnation,
                    world: world.clone(),
                };
                let run = Gated::new(Box::pin(factory(ctx)), net.clone(), node.clone());
                let crashed = {
                    let (net, node) = (net.clone(), node.clone());
                    async move {
                        let mut ch = net.changes();
                        while net.state(&node) != NodeState::Crashed {
                            if ch.changed().await.is_err() {
                                std::future::pending::<()>().await;
                            }
                        }
                    }
                };
                tokio::select! {
                    biased;
                    () = crashed => world.record(format!("node {node} killed")),
                    () = run => {
                        world.record(format!("node {node} exited"));
                        return;
                    }
                }
            }
        });
    }

    /// Runs `plan` against this world's faults; resolves when every fault
    /// has been injected and healed.
    pub async fn run_plan(&self, plan: &FaultPlan) {
        let f: Arc<dyn Faults> = Arc::new(self.inner.faults.clone());
        plan.run(f).await;
    }

    /// Heals every fault and restarts crashed nodes (`simulate` step 3).
    pub async fn heal_all(&self) {
        let _ = self.inner.faults.heal_all().await;
        let crashed = self.inner.net.crashed_nodes();
        for n in crashed {
            self.inner.net.restart(&n);
        }
    }
}

/// Freezes a future while its node is paused.
struct Gated<F> {
    inner: std::pin::Pin<Box<F>>,
    gate: Option<std::pin::Pin<Box<dyn Future<Output = ()> + Send>>>,
    net: SimNet,
    node: Node,
}

impl<F> Gated<F> {
    fn new(inner: std::pin::Pin<Box<F>>, net: SimNet, node: Node) -> Self {
        Self {
            inner,
            gate: None,
            net,
            node,
        }
    }
}

impl<F: Future<Output = ()>> Future for Gated<F> {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        loop {
            if let Some(g) = self.gate.as_mut() {
                if g.as_mut().poll(cx).is_pending() {
                    return std::task::Poll::Pending;
                }
                self.gate = None;
            }
            if self.net.state(&self.node) == NodeState::Paused {
                let (net, node) = (self.net.clone(), self.node.clone());
                self.gate = Some(Box::pin(async move { net.wait_running(&node).await }));
                continue;
            }
            return self.inner.as_mut().poll(cx);
        }
    }
}

fn panic_message(p: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic".to_owned()
    }
}

/// The result of one run.
#[derive(Debug, Clone)]
pub struct RunOutcome {
    /// The seed.
    pub seed: u64,
    /// `Err` with the failure message.
    pub result: Result<(), String>,
    /// The kept trace lines.
    pub trace: Vec<String>,
    /// Event count and hash of the whole trace.
    pub digest: (u64, u64),
    /// Virtual time the run took.
    pub virtual_time: Duration,
}

/// A stored failing case (chapter 46, "Data structures and storage").
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegressionCase {
    /// The suite.
    pub suite: String,
    /// The seed.
    pub seed: u64,
    /// What failed, for people.
    #[serde(default)]
    pub failure: String,
    /// The (shrunk) fault schedule.
    #[serde(default)]
    pub plan: FaultPlan,
}

/// A simulation suite: a name, how many seeds, and limits.
#[derive(Debug, Clone)]
pub struct Sim {
    suite: String,
    seeds: Option<u64>,
    max_time: Duration,
    shrink_runs: usize,
    check_determinism: bool,
}

impl Sim {
    /// A suite named `suite` (also its regression directory's name).
    #[must_use]
    pub fn new(suite: &str) -> Self {
        Self {
            suite: suite.to_owned(),
            seeds: None,
            max_time: Duration::from_secs(3600),
            shrink_runs: 200,
            check_determinism: true,
        }
    }

    /// Seeds per run unless `SPRIG_SIM_SEEDS` says otherwise.
    #[must_use]
    pub fn seeds(mut self, n: u64) -> Self {
        self.seeds = Some(n);
        self
    }

    /// Virtual time after which a run fails as stuck (default one hour).
    #[must_use]
    pub fn max_time(mut self, d: Duration) -> Self {
        self.max_time = d;
        self
    }

    /// At most this many reruns while shrinking a failing plan.
    #[must_use]
    pub fn shrink_runs(mut self, n: usize) -> Self {
        self.shrink_runs = n;
        self
    }

    /// Whether to rerun the first seed and compare traces (default on).
    #[must_use]
    pub fn check_determinism(mut self, on: bool) -> Self {
        self.check_determinism = on;
        self
    }

    /// The seeds this run will try, after regression cases.
    #[must_use]
    pub fn seed_list(&self) -> Vec<u64> {
        if let Some(s) = skip::var(SEED_VAR).and_then(|v| v.parse().ok()) {
            return vec![s];
        }
        let n = skip::var(SEEDS_VAR)
            .and_then(|v| v.parse().ok())
            .or(self.seeds)
            .unwrap_or(DEFAULT_SEEDS);
        let base = skip::var(SEED_BASE_VAR)
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(random_base);
        (0..n).map(|i| base.wrapping_add(i)).collect()
    }

    /// Runs `body` once per seed with no faults planned.
    ///
    /// # Panics
    /// When a seed fails, with how to replay it.
    pub fn run<F, Fut>(&self, body: F)
    where
        F: Fn(World) -> Fut,
        Fut: Future<Output = SimResult>,
    {
        self.run_with_faults(|_| FaultPlan::default(), |w, _| body(w));
    }

    /// Runs `body` once per seed with the fault plan `plan` makes from the
    /// seed. The body runs the plan (usually `world.run_plan(&plan)`
    /// alongside its workload), heals, and checks invariants.
    ///
    /// # Panics
    /// When a seed fails, after shrinking its plan.
    pub fn run_with_faults<G, F, Fut>(&self, plan: G, body: F)
    where
        G: Fn(&SimRng) -> FaultPlan,
        F: Fn(World, FaultPlan) -> Fut,
        Fut: Future<Output = SimResult>,
    {
        let replay_only = skip::var(SEED_VAR).is_some();
        let mut cases: Vec<RegressionCase> = if replay_only {
            Vec::new()
        } else {
            self.regression_cases()
        };
        let seeds = self.seed_list();
        if let (Some(first), Some(last)) = (seeds.first(), seeds.last()) {
            eprintln!(
                "sim {}: {} regression case(s), seeds {first}..={last}",
                self.suite,
                cases.len()
            );
        }
        for seed in &seeds {
            cases.push(RegressionCase {
                suite: self.suite.clone(),
                seed: *seed,
                failure: String::new(),
                plan: plan(&plan_rng(*seed)),
            });
        }
        let mut checked = false;
        let print = skip::flag(TRACE_VAR);
        for case in cases {
            let out = self.run_once(case.seed, &case.plan, &body);
            if print {
                eprintln!(
                    "sim {} seed {}: {:?}\n{}",
                    self.suite,
                    case.seed,
                    out.result,
                    out.trace.join("\n")
                );
            }
            if self.check_determinism && !checked {
                checked = true;
                self.compare_reruns(&out, &case.plan, &body);
            }
            if let Err(msg) = &out.result {
                self.report_failure(&case, msg, &out, &body);
            }
        }
    }

    fn compare_reruns<F, Fut>(&self, first: &RunOutcome, plan: &FaultPlan, body: &F) -> bool
    where
        F: Fn(World, FaultPlan) -> Fut,
        Fut: Future<Output = SimResult>,
    {
        let again = self.run_once(first.seed, plan, body);
        if again.digest == first.digest && again.result == first.result {
            return true;
        }
        let diff = trace::first_difference(&first.trace, &again.trace);
        let msg = format!(
            "sim {}: seed {} is not deterministic: traces differ at {diff:?}",
            self.suite, first.seed
        );
        #[cfg(tokio_unstable)]
        panic!("{msg}\n  replay: {SEED_VAR}={}", first.seed);
        #[cfg(not(tokio_unstable))]
        {
            eprintln!(
                "warning: {msg}\n  (built without --cfg tokio_unstable, tokio's select! order isn't seeded)"
            );
            false
        }
    }

    fn report_failure<F, Fut>(
        &self,
        case: &RegressionCase,
        msg: &str,
        out: &RunOutcome,
        body: &F,
    ) -> !
    where
        F: Fn(World, FaultPlan) -> Fut,
        Fut: Future<Output = SimResult>,
    {
        let deterministic = self.compare_reruns(out, &case.plan, body);
        let shrunk = if case.plan.is_empty() {
            case.plan.clone()
        } else {
            self.shrink(case.seed, &case.plan, body)
        };
        let reg = RegressionCase {
            suite: self.suite.clone(),
            seed: case.seed,
            failure: msg.to_owned(),
            plan: shrunk.clone(),
        };
        let written = write_regression(&reg, &out.trace);
        panic!(
            "simulation `{}` failed for seed {}{}: {msg}\n  replay: {SEED_VAR}={} cargo test <this test>\n  \
             fault plan: {} fault(s), shrunk to {}{}\n  last trace lines:\n{}",
            self.suite,
            case.seed,
            if deterministic {
                ""
            } else {
                " (NOT deterministic)"
            },
            case.seed,
            case.plan.faults.len(),
            shrunk.faults.len(),
            written.map_or_else(String::new, |p| format!(
                "\n  regression case: {}",
                p.display()
            )),
            indent(&out.trace, 40)
        );
    }

    /// Delta debugging (ddmin) over the plan's faults: the smallest plan
    /// found that still fails with this seed.
    fn shrink<F, Fut>(&self, seed: u64, plan: &FaultPlan, body: &F) -> FaultPlan
    where
        F: Fn(World, FaultPlan) -> Fut,
        Fut: Future<Output = SimResult>,
    {
        let mut runs = 0;
        let mut current = plan.faults.clone();
        let mut n = 2usize;
        let fails = |faults: &[PlannedFault], runs: &mut usize| {
            *runs += 1;
            let p = FaultPlan {
                faults: faults.to_vec(),
            };
            self.run_once(seed, &p, body).result.is_err()
        };
        while current.len() >= 2 && runs < self.shrink_runs {
            let chunk = current.len().div_ceil(n);
            let mut reduced = None;
            for i in 0..n {
                let lo = i * chunk;
                if lo >= current.len() {
                    break;
                }
                let hi = (lo + chunk).min(current.len());
                let complement: Vec<PlannedFault> = current[..lo]
                    .iter()
                    .chain(&current[hi..])
                    .cloned()
                    .collect();
                if fails(&complement, &mut runs) {
                    reduced = Some(complement);
                    break;
                }
                if runs >= self.shrink_runs {
                    break;
                }
            }
            if let Some(c) = reduced {
                current = c;
                n = (n - 1).max(2);
            } else {
                if n >= current.len() {
                    break;
                }
                n = (n * 2).min(current.len());
            }
        }
        if current.len() == 1 && fails(&[], &mut runs) {
            current.clear();
        }
        FaultPlan { faults: current }
    }

    /// Runs one seed with one plan and reports what happened. Never panics
    /// on a failing body.
    pub fn run_once<F, Fut>(&self, seed: u64, plan: &FaultPlan, body: &F) -> RunOutcome
    where
        F: Fn(World, FaultPlan) -> Fut,
        Fut: Future<Output = SimResult>,
    {
        let rt = runtime(seed);
        let max = self.max_time;
        let caught = std::panic::catch_unwind(AssertUnwindSafe(|| {
            rt.block_on(async {
                let world = World::new(seed);
                let w = world.clone();
                let r = tokio::time::timeout(max, body(w, plan.clone())).await;
                let result = match r {
                    Err(_) => Err(format!(
                        "did not finish within {max:?} of virtual time (stuck?)"
                    )),
                    Ok(Err(e)) => Err(e.to_string()),
                    Ok(Ok(())) => match world.failures().first() {
                        Some(f) => Err(f.clone()),
                        None => Ok(()),
                    },
                };
                (
                    result,
                    world.trace().lines(),
                    world.trace().digest(),
                    world.elapsed(),
                )
            })
        }));
        drop(rt);
        match caught {
            Ok((result, trace, digest, virtual_time)) => RunOutcome {
                seed,
                result,
                trace,
                digest,
                virtual_time,
            },
            Err(p) => RunOutcome {
                seed,
                result: Err(format!("panicked: {}", panic_message(&*p))),
                trace: Vec::new(),
                digest: (0, 0),
                virtual_time: Duration::ZERO,
            },
        }
    }

    /// The suite's stored regression cases, from
    /// `$CARGO_MANIFEST_DIR/tests/regressions/<suite>/*.toml`.
    ///
    /// # Panics
    /// If a case file can't be read or parsed: a broken regression case
    /// must not pass silently.
    #[must_use]
    pub fn regression_cases(&self) -> Vec<RegressionCase> {
        let Some(dir) = std::env::var_os("CARGO_MANIFEST_DIR") else {
            return Vec::new();
        };
        let dir = Path::new(&dir)
            .join("tests")
            .join("regressions")
            .join(&self.suite);
        let Ok(rd) = std::fs::read_dir(&dir) else {
            return Vec::new();
        };
        let mut files: Vec<PathBuf> = rd
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "toml"))
            .collect();
        files.sort();
        files
            .iter()
            .map(|p| {
                let text = std::fs::read_to_string(p)
                    .unwrap_or_else(|e| panic!("reading {}: {e}", p.display()));
                toml::from_str(&text).unwrap_or_else(|e| panic!("parsing {}: {e}", p.display()))
            })
            .collect()
    }
}

/// The runtime of one run: current-thread, paused time, and (with
/// `--cfg tokio_unstable`) tokio's own RNG seeded too.
fn runtime(seed: u64) -> tokio::runtime::Runtime {
    let mut b = tokio::runtime::Builder::new_current_thread();
    b.enable_time().start_paused(true);
    #[cfg(tokio_unstable)]
    b.rng_seed(tokio::runtime::RngSeed::from_bytes(&seed.to_le_bytes()));
    #[cfg(not(tokio_unstable))]
    let _ = seed;
    b.build().expect("building a current-thread runtime")
}

/// The plan generator's own stream, so plans don't depend on how many
/// numbers the world draws.
fn plan_rng(seed: u64) -> SimRng {
    SimRng::new(seed ^ 0x5157_504c_414e_0001)
}

fn random_base() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos()),
    );
    // Keep seeds short enough to type.
    h.finish() % 1_000_000_000
}

fn indent(lines: &[String], n: usize) -> String {
    let from = lines.len().saturating_sub(n);
    lines[from..]
        .iter()
        .map(|l| format!("    {l}"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Writes the case as `<seed>.toml` and the failing run's trace as
/// `<seed>.trace` under the log directory.
fn write_regression(case: &RegressionCase, trace: &[String]) -> Option<PathBuf> {
    let base = skip::var(skip::LOG_DIR)
        .map_or_else(|| std::env::temp_dir().join("sprig-testkit"), PathBuf::from);
    let dir = base.join("regressions").join(&case.suite);
    std::fs::create_dir_all(&dir).ok()?;
    let path = dir.join(format!("{}.toml", case.seed));
    let text = toml::to_string(case).ok()?;
    std::fs::write(&path, text).ok()?;
    let _ = std::fs::write(dir.join(format!("{}.trace", case.seed)), trace.join("\n"));
    Some(path)
}

#[cfg(test)]
mod tests;
