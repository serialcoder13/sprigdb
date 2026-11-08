//! The MariaDB model inside a simulation: [`SimMariaDb`] drives the
//! synchronous [`Model`] on tokio's paused clock and the simulated network.
//!
//! Servers are network nodes named after their [`ServerConfig::name`]
//! (`super::ServerConfig`): a partition between two of them stops
//! replication and acknowledgements, a partition between a client and a
//! server makes the client's statement fail with `2013` after the client
//! timeout, and killing a server's node crashes the server (restarting the
//! node restarts it).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::sync::Notify;

use super::model::{ConnId, Model, Step};
use super::{Outcome, Stmt, errno};
use crate::faults::Node;
use crate::sim::net::NodeState;
use crate::sim::{SimNet, Trace, World};

/// Client-side timeout of a statement whose request or reply is lost.
pub const CLIENT_TIMEOUT: Duration = Duration::from_secs(5);

struct Inner {
    model: Mutex<Model>,
    net: SimNet,
    trace: Trace,
    origin: tokio::time::Instant,
    kick: Notify,
    waiters: Mutex<BTreeMap<ConnId, Arc<Notify>>>,
}

/// The MariaDB model in a simulation. Cheap to clone.
#[derive(Clone)]
pub struct SimMariaDb {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for SimMariaDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimMariaDb").finish_non_exhaustive()
    }
}

impl SimMariaDb {
    /// A model built by `build` (add servers, replicas, Galera clusters),
    /// wired to the world's network for reachability and delays, with a
    /// driver task that runs timed events and follows node crashes.
    #[must_use]
    pub fn new(world: &World, build: impl FnOnce(&mut Model)) -> Self {
        let mut model = Model::new(world.rng().next_u64());
        let net = world.net().clone();
        let n1 = net.clone();
        model.set_reachability(Box::new(move |a, b| {
            n1.reachable(&Node::from(a), &Node::from(b))
        }));
        let n2 = net.clone();
        model.set_delay(Box::new(move |a, b| {
            n2.link(&Node::from(a), &Node::from(b)).latency
        }));
        build(&mut model);
        let inner = Arc::new(Inner {
            model: Mutex::new(model),
            net,
            trace: world.trace().clone(),
            origin: tokio::time::Instant::now(),
            kick: Notify::new(),
            waiters: Mutex::new(BTreeMap::new()),
        });
        let db = Self { inner };
        let driver = db.clone();
        world.spawn("mariadb model", async move { driver.drive().await });
        db
    }

    fn model(&self) -> MutexGuard<'_, Model> {
        self.inner
            .model
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn now(&self) -> Duration {
        tokio::time::Instant::now() - self.inner.origin
    }

    /// Runs `f` on the model at the current virtual time (for topology
    /// changes such as promotion, and for checks), then wakes waiters.
    pub fn with_model<R>(&self, f: impl FnOnce(&mut Model, Duration) -> R) -> R {
        let now = self.now();
        let r = {
            let mut m = self.model();
            m.advance(now);
            f(&mut m, now)
        };
        self.settle();
        self.inner.kick.notify_one();
        r
    }

    /// Flushes the model's log to the trace and wakes finished waiters.
    fn settle(&self) {
        let (log, done) = {
            let mut m = self.model();
            (m.take_log(), m.take_completed())
        };
        for l in log {
            self.inner.trace.record(format!("mariadb {l}"));
        }
        let w = self
            .inner
            .waiters
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        for c in done {
            if let Some(n) = w.get(&c) {
                n.notify_one();
            }
        }
    }

    async fn drive(&self) {
        let mut changes = self.inner.net.changes();
        loop {
            self.sync_nodes();
            let next = self.model().next_event_time();
            let sleep = async {
                match next {
                    Some(t) => tokio::time::sleep_until(self.inner.origin + t).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                biased;
                r = changes.changed() => {
                    if r.is_err() {
                        return;
                    }
                }
                () = self.inner.kick.notified() => {}
                () = sleep => {}
            }
            let now = self.now();
            self.model().advance(now);
            self.settle();
        }
    }

    /// Crashes and restarts servers whose nodes crashed or restarted.
    fn sync_nodes(&self) {
        let now = self.now();
        {
            let mut m = self.model();
            for s in m.servers() {
                let st = self.inner.net.state(&Node::from(s.as_str()));
                match (st, m.is_up(&s)) {
                    (NodeState::Crashed, true) => m.crash(&s, now),
                    (NodeState::Running | NodeState::Paused, false) => m.restart(&s, now),
                    _ => {}
                }
            }
        }
        self.settle();
    }

    /// Connects from the process on `from` to `server`.
    ///
    /// # Errors
    /// `2013` if the server is unreachable or down, `1045` for an unknown
    /// user.
    pub async fn connect(
        &self,
        from: &Node,
        server: &str,
        user: Option<&str>,
        schema: &str,
    ) -> Result<SimConn, u16> {
        let srv = Node::from(server);
        let net = &self.inner.net;
        if net.transit(from, &srv).await.is_err() || net.transit(&srv, from).await.is_err() {
            tokio::time::sleep(CLIENT_TIMEOUT).await;
            return Err(errno::SERVER_LOST);
        }
        self.sync_nodes();
        let now = self.now();
        let id = self.model().connect(server, user, schema, now)?;
        self.inner
            .waiters
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id, Arc::new(Notify::new()));
        Ok(SimConn {
            db: self.clone(),
            id,
            from: from.clone(),
            server: srv,
        })
    }
}

/// A client connection to a model server.
#[derive(Debug)]
pub struct SimConn {
    db: SimMariaDb,
    id: ConnId,
    from: Node,
    server: Node,
}

impl SimConn {
    /// The connection's id (for `KILL`).
    #[must_use]
    pub fn id(&self) -> ConnId {
        self.id
    }

    /// The server's node.
    #[must_use]
    pub fn server(&self) -> &Node {
        &self.server
    }

    /// Runs a statement: the request crosses the network, the server runs
    /// it (waiting for locks, acknowledgements or GTIDs in virtual time),
    /// the reply crosses back. A lost request or reply fails with `2013`
    /// after [`CLIENT_TIMEOUT`]; whether the statement ran is then unknown,
    /// as with a real client.
    pub async fn exec(&self, stmt: &Stmt) -> Outcome {
        let net = self.db.inner.net.clone();
        let started = tokio::time::Instant::now();
        let lost = || async {
            tokio::time::sleep(CLIENT_TIMEOUT.saturating_sub(started.elapsed())).await;
            Outcome::Err(errno::SERVER_LOST)
        };
        if net.transit(&self.from, &self.server).await.is_err() {
            return lost().await;
        }
        net.wait_running(&self.server).await;
        self.db.sync_nodes();
        let notify = self
            .db
            .inner
            .waiters
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&self.id)
            .cloned();
        let now = self.db.now();
        let step = self.db.model().exec(self.id, stmt, now);
        self.db.settle();
        self.db.inner.kick.notify_one();
        let outcome = match step {
            Step::Done(o) => o,
            Step::Blocked => loop {
                if let Some(o) = self.db.model().take_done(self.id) {
                    break o;
                }
                match &notify {
                    Some(n) => n.notified().await,
                    None => break Outcome::Err(errno::SERVER_LOST),
                }
            },
        };
        self.db
            .inner
            .trace
            .record(format!("mariadb {} {stmt:?} -> {outcome}", self.id));
        if net.transit(&self.server, &self.from).await.is_err() {
            return lost().await;
        }
        outcome
    }
}

impl Drop for SimConn {
    fn drop(&mut self) {
        let now = self.db.now();
        self.db.model().disconnect(self.id, now);
        self.db
            .inner
            .waiters
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.id);
        self.db.settle();
    }
}
