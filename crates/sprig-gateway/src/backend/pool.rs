//! Pools (chapter 06, "Pools and connections", "Acquire, sync and
//! release"; build step 2): one pool per [`PoolKey`], idle connections in
//! LIFO buckets keyed by the digest of their applied state, a limit per
//! pool and per node, lifetimes and idle timeouts. Connections return
//! without `COM_RESET_CONNECTION`: sync makes a reset unnecessary.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sprig_common::ids::{ClusterId, NodeId};
use tokio::sync::Notify;

use super::cluster::NodeKey;
use super::conn::BackendConn;
use super::state::Baseline;

/// The key of one pool (chapter 06, "Interfaces").
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PoolKey {
    /// The cluster.
    pub cluster: ClusterId,
    /// The node.
    pub node: NodeId,
    /// The backend account.
    pub account: String,
    /// `CLIENT_FOUND_ROWS` changes affected-row counts.
    pub found_rows: bool,
    /// MariaDB extended capability bits the connections negotiate (only
    /// for 3307 sessions whose client has them, chapter 05).
    pub mariadb_caps: u64,
}

impl PoolKey {
    /// The node, globally.
    #[must_use]
    pub fn node_key(&self) -> NodeKey {
        NodeKey {
            cluster: self.cluster.clone(),
            node: self.node.clone(),
        }
    }
}

/// What to do with a connection on release.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Release {
    /// Back to its digest bucket, if it is idle and healthy.
    Reuse,
    /// Close it.
    Discard,
}

/// Limits in force for a pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// `backend.max_conns_per_account_node`.
    pub per_pool: u32,
    /// `backend.max_conns_per_node`.
    pub per_node: u32,
    /// `backend.max_lifetime_seconds`.
    pub max_lifetime: Duration,
    /// `backend.idle_timeout_seconds`.
    pub idle_timeout: Duration,
}

/// Why an acquire failed.
#[derive(Debug)]
pub enum AcquireError<E> {
    /// No connection became free before the deadline (39031).
    Timeout,
    /// Opening a new connection failed.
    Connect(E),
}

/// Counts of one pool, for tests and metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolStats {
    /// Open connections: idle, leased and connecting.
    pub open: u32,
    /// Leased.
    pub leased: u32,
    /// Idle.
    pub idle: u32,
}

struct IdleConn {
    conn: BackendConn,
    seq: u64,
    since: Instant,
}

#[derive(Default)]
struct Pool {
    idle: HashMap<u64, Vec<IdleConn>>,
    open: u32,
    leased: u32,
    baseline: Option<Arc<Baseline>>,
}

impl Pool {
    fn idle_count(&self) -> u32 {
        u32::try_from(self.idle.values().map(Vec::len).sum::<usize>()).unwrap_or(u32::MAX)
    }

    /// The most recently released idle connection, from any bucket.
    fn pop_any(&mut self) -> Option<IdleConn> {
        let (&d, _) = self
            .idle
            .iter()
            .filter(|(_, v)| !v.is_empty())
            .max_by_key(|(_, v)| v.last().map_or(0, |c| c.seq))?;
        self.pop_bucket(d)
    }

    fn pop_bucket(&mut self, d: u64) -> Option<IdleConn> {
        let b = self.idle.get_mut(&d)?;
        let c = b.pop();
        if b.is_empty() {
            self.idle.remove(&d);
        }
        c
    }

    /// The idle connection idle the longest.
    fn pop_oldest(&mut self) -> Option<IdleConn> {
        let (&d, _) = self
            .idle
            .iter()
            .filter(|(_, v)| !v.is_empty())
            .min_by_key(|(_, v)| v.first().map_or(Instant::now(), |c| c.since))?;
        let b = self.idle.get_mut(&d)?;
        let c = if b.is_empty() {
            None
        } else {
            Some(b.remove(0))
        };
        if b.is_empty() {
            self.idle.remove(&d);
        }
        c
    }
}

#[derive(Default, Clone, Copy)]
struct NodeCount {
    open: u32,
    leased: u32,
}

#[derive(Default)]
struct Inner {
    pools: HashMap<PoolKey, Pool>,
    nodes: HashMap<NodeKey, NodeCount>,
    seq: u64,
    /// Accounts whose grants changed (chapter 29): connections opened
    /// before the instant are closed instead of reused.
    retired: HashMap<String, Instant>,
}

pub(crate) struct Shared {
    inner: Mutex<Inner>,
    freed: Notify,
}

impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Forgets one open connection of `key`.
    fn forget(&self, key: &PoolKey, leased: bool) {
        {
            let mut g = self.lock();
            if let Some(p) = g.pools.get_mut(key) {
                p.open = p.open.saturating_sub(1);
                if leased {
                    p.leased = p.leased.saturating_sub(1);
                }
            }
            if let Some(n) = g.nodes.get_mut(&key.node_key()) {
                n.open = n.open.saturating_sub(1);
                if leased {
                    n.leased = n.leased.saturating_sub(1);
                }
            }
        }
        self.freed.notify_waiters();
    }
}

/// Closes connections off the caller's path: `COM_QUIT`, then the socket.
pub(crate) fn close_all(conns: Vec<BackendConn>) {
    if conns.is_empty() {
        return;
    }
    if let Ok(h) = tokio::runtime::Handle::try_current() {
        h.spawn(async move {
            for mut c in conns {
                let _ = tokio::time::timeout(Duration::from_secs(1), c.client.quit()).await;
            }
        });
    }
}

/// A connection borrowed from a pool. Dropping a lease without
/// [`BackendPools::release`] closes the connection.
pub struct BackendLease {
    conn: Option<BackendConn>,
    key: PoolKey,
    shared: Arc<Shared>,
    /// Whether the connection was opened for this lease.
    pub fresh: bool,
}

impl std::fmt::Debug for BackendLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendLease")
            .field("key", &self.key)
            .field("conn", &self.conn)
            .finish_non_exhaustive()
    }
}

impl BackendLease {
    /// The connection.
    ///
    /// # Panics
    /// Never: a lease always holds its connection until released.
    #[allow(clippy::missing_panics_doc)]
    pub fn conn(&mut self) -> &mut BackendConn {
        self.conn.as_mut().expect("a lease holds its connection")
    }

    /// The connection, read-only.
    ///
    /// # Panics
    /// Never.
    #[allow(clippy::missing_panics_doc)]
    #[must_use]
    pub fn conn_ref(&self) -> &BackendConn {
        self.conn.as_ref().expect("a lease holds its connection")
    }

    /// The pool it came from.
    #[must_use]
    pub fn key(&self) -> &PoolKey {
        &self.key
    }
}

impl Drop for BackendLease {
    fn drop(&mut self) {
        if let Some(c) = self.conn.take() {
            self.shared.forget(&self.key, true);
            close_all(vec![c]);
        }
    }
}

/// Every pool of this gateway.
pub struct BackendPools {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for BackendPools {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendPools").finish_non_exhaustive()
    }
}

impl Default for BackendPools {
    fn default() -> Self {
        Self::new()
    }
}

impl BackendPools {
    /// No pools yet.
    #[must_use]
    pub fn new() -> Self {
        Self {
            shared: Arc::new(Shared {
                inner: Mutex::new(Inner::default()),
                freed: Notify::new(),
            }),
        }
    }

    /// Borrows a connection: an idle one from the bucket of
    /// `want_digest(baseline)`, else any idle one, else a new one from
    /// `connect` while below both limits, else waits for a release until
    /// `deadline`. Idle connections past `limits.max_lifetime` are closed
    /// on the way.
    ///
    /// # Errors
    /// [`AcquireError::Timeout`] at the deadline; [`AcquireError::Connect`]
    /// when opening a connection fails.
    #[allow(clippy::too_many_lines)] // one locked decision, then its outcome
    pub async fn acquire<E, F, Fut>(
        &self,
        key: &PoolKey,
        want_digest: impl Fn(&Baseline) -> u64,
        limits: Limits,
        deadline: tokio::time::Instant,
        mut connect: F,
    ) -> Result<BackendLease, AcquireError<E>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<BackendConn, E>>,
    {
        let node_key = key.node_key();
        loop {
            let freed = self.shared.freed.notified();
            let mut expired = Vec::new();
            let step = {
                let mut g = self.shared.lock();
                let inner = &mut *g;
                let pool = inner.pools.entry(key.clone()).or_default();
                let mut got = None;
                while got.is_none() {
                    let c = match pool.baseline.as_ref().map(|b| want_digest(b)) {
                        Some(d) => pool.pop_bucket(d).or_else(|| pool.pop_any()),
                        None => pool.pop_any(),
                    };
                    let Some(mut c) = c else { break };
                    // A connection the server closed while it sat idle
                    // (KILL CONNECTION, a restart) is never handed out: a
                    // statement sent on it would fail without reaching the
                    // server (chapter 06, "Failure handling").
                    if c.conn.age() > limits.max_lifetime
                        || !c.conn.client.is_idle()
                        || c.conn.client.closed_while_idle()
                    {
                        pool.open = pool.open.saturating_sub(1);
                        if let Some(n) = inner.nodes.get_mut(&node_key) {
                            n.open = n.open.saturating_sub(1);
                        }
                        expired.push(c.conn);
                    } else {
                        got = Some(c.conn);
                    }
                }
                let node = inner.nodes.entry(node_key.clone()).or_default();
                if let Some(conn) = got {
                    pool.leased += 1;
                    node.leased += 1;
                    Step::Got(Box::new(conn))
                } else if pool.open < limits.per_pool && node.open < limits.per_node {
                    pool.open += 1;
                    node.open += 1;
                    Step::Connect
                } else if pool.open < limits.per_pool {
                    // The node is full: close another pool's idle connection
                    // on this node, if there is one.
                    let victim = inner
                        .pools
                        .iter_mut()
                        .filter(|(k, p)| {
                            k.node_key() == node_key && *k != key && p.idle_count() > 0
                        })
                        .find_map(|(_, p)| {
                            p.pop_oldest().map(|c| {
                                p.open = p.open.saturating_sub(1);
                                c.conn
                            })
                        });
                    match victim {
                        Some(v) => {
                            expired.push(v);
                            if let Some(p) = inner.pools.get_mut(key) {
                                p.open += 1;
                            }
                            Step::Connect
                        }
                        None => Step::Wait,
                    }
                } else {
                    Step::Wait
                }
            };
            close_all(expired);
            match step {
                Step::Got(conn) => {
                    return Ok(BackendLease {
                        conn: Some(*conn),
                        key: key.clone(),
                        shared: Arc::clone(&self.shared),
                        fresh: false,
                    });
                }
                Step::Connect => match connect().await {
                    Ok(conn) => {
                        let mut g = self.shared.lock();
                        let inner = &mut *g;
                        let pool = inner.pools.entry(key.clone()).or_default();
                        if pool.baseline.is_none() {
                            pool.baseline = Some(Arc::clone(&conn.baseline));
                        }
                        pool.leased += 1;
                        inner.nodes.entry(node_key.clone()).or_default().leased += 1;
                        return Ok(BackendLease {
                            conn: Some(conn),
                            key: key.clone(),
                            shared: Arc::clone(&self.shared),
                            fresh: true,
                        });
                    }
                    Err(e) => {
                        self.shared.forget(key, false);
                        return Err(AcquireError::Connect(e));
                    }
                },
                Step::Wait => {
                    if tokio::time::timeout_at(deadline, freed).await.is_err() {
                        return Err(AcquireError::Timeout);
                    }
                }
            }
        }
    }

    /// Returns a lease. `Reuse` puts the connection back in its digest
    /// bucket when it is idle, outside a transaction, within its lifetime
    /// and within the (possibly lowered) limits; otherwise it is closed.
    pub fn release(&self, mut lease: BackendLease, how: Release, limits: Limits) {
        let Some(conn) = lease.conn.take() else {
            return;
        };
        let key = lease.key.clone();
        let node_key = key.node_key();
        let mut reusable = how == Release::Reuse
            && conn.client.is_idle()
            && !conn.client.in_transaction()
            && conn.age() < limits.max_lifetime;
        let mut close = None;
        {
            let mut g = self.shared.lock();
            if let Some(at) = g.retired.get(&key.account)
                && Instant::now()
                    .checked_sub(conn.age())
                    .is_none_or(|opened| opened < *at)
            {
                reusable = false;
            }
            let inner = &mut *g;
            inner.seq += 1;
            let seq = inner.seq;
            let node = inner.nodes.entry(node_key).or_default();
            node.leased = node.leased.saturating_sub(1);
            let node_over = node.open > limits.per_node;
            let pool = inner.pools.entry(key).or_default();
            pool.leased = pool.leased.saturating_sub(1);
            if reusable && pool.open <= limits.per_pool && !node_over {
                let d = conn.applied.digest(&conn.baseline);
                pool.idle.entry(d).or_default().push(IdleConn {
                    conn,
                    seq,
                    since: Instant::now(),
                });
            } else {
                pool.open = pool.open.saturating_sub(1);
                node.open = node.open.saturating_sub(1);
                close = Some(conn);
            }
        }
        self.shared.freed.notify_waiters();
        close_all(close.into_iter().collect());
    }

    /// Closes idle connections past their idle timeout or lifetime, and
    /// idle excess above lowered limits. `limits` gives each cluster's.
    pub fn reap(&self, limits: impl Fn(&ClusterId) -> Limits) {
        let mut closed = Vec::new();
        {
            let mut g = self.shared.lock();
            let inner = &mut *g;
            for (key, pool) in &mut inner.pools {
                let l = limits(&key.cluster);
                let node = inner.nodes.entry(key.node_key()).or_default();
                for bucket in pool.idle.values_mut() {
                    let mut keep = Vec::with_capacity(bucket.len());
                    for c in bucket.drain(..) {
                        if c.since.elapsed() > l.idle_timeout || c.conn.age() > l.max_lifetime {
                            pool.open = pool.open.saturating_sub(1);
                            node.open = node.open.saturating_sub(1);
                            closed.push(c.conn);
                        } else {
                            keep.push(c);
                        }
                    }
                    *bucket = keep;
                }
                pool.idle.retain(|_, b| !b.is_empty());
                while (pool.open > l.per_pool || node.open > l.per_node) && pool.idle_count() > 0 {
                    if let Some(c) = pool.pop_oldest() {
                        pool.open = pool.open.saturating_sub(1);
                        node.open = node.open.saturating_sub(1);
                        closed.push(c.conn);
                    }
                }
            }
        }
        if !closed.is_empty() {
            self.shared.freed.notify_waiters();
        }
        close_all(closed);
    }

    /// Retires the connections of backend `account` (chapter 29: its
    /// grants changed, and MariaDB keeps a connection's database
    /// privileges until its next `USE`): idle ones close now, leased ones
    /// on release.
    pub fn retire_account(&self, account: &str) {
        let mut closed = Vec::new();
        {
            let mut g = self.shared.lock();
            let inner = &mut *g;
            inner.retired.insert(account.to_owned(), Instant::now());
            for (key, pool) in &mut inner.pools {
                if key.account != account {
                    continue;
                }
                let node = inner.nodes.entry(key.node_key()).or_default();
                for (_, b) in pool.idle.drain() {
                    for c in b {
                        pool.open = pool.open.saturating_sub(1);
                        node.open = node.open.saturating_sub(1);
                        closed.push(c.conn);
                    }
                }
            }
        }
        self.shared.freed.notify_waiters();
        close_all(closed);
    }

    /// Closes every idle connection to `node` (it went Down).
    pub fn close_idle(&self, node: &NodeKey) {
        let mut closed = Vec::new();
        {
            let mut g = self.shared.lock();
            let inner = &mut *g;
            for (key, pool) in &mut inner.pools {
                if &key.node_key() != node {
                    continue;
                }
                for (_, b) in pool.idle.drain() {
                    for c in b {
                        pool.open = pool.open.saturating_sub(1);
                        if let Some(n) = inner.nodes.get_mut(node) {
                            n.open = n.open.saturating_sub(1);
                        }
                        closed.push(c.conn);
                    }
                }
            }
        }
        self.shared.freed.notify_waiters();
        close_all(closed);
    }

    /// The counts of one pool.
    #[must_use]
    pub fn stats(&self, key: &PoolKey) -> PoolStats {
        let g = self.shared.lock();
        g.pools
            .get(key)
            .map_or_else(PoolStats::default, |p| PoolStats {
                open: p.open,
                leased: p.leased,
                idle: p.idle_count(),
            })
    }

    /// Open and leased connections to a node.
    #[must_use]
    pub fn node_counts(&self, node: &NodeKey) -> (u32, u32) {
        let g = self.shared.lock();
        g.nodes.get(node).map_or((0, 0), |n| (n.open, n.leased))
    }

    /// Per-cluster totals `(idle, leased)`, for the pool gauge.
    #[must_use]
    pub fn totals(&self) -> HashMap<ClusterId, (u32, u32)> {
        let g = self.shared.lock();
        let mut out: HashMap<ClusterId, (u32, u32)> = HashMap::new();
        for (k, p) in &g.pools {
            let e = out.entry(k.cluster.clone()).or_default();
            e.0 += p.idle_count();
            e.1 += p.leased;
        }
        out
    }
}

enum Step {
    Got(Box<BackendConn>),
    Connect,
    Wait,
}
