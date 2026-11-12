//! Sprig 2PC's relay fence (chapter 18, "The relay fence"; decisions Q28
//! and Q36): the part of the writer node's agent that keeps a held
//! branch's rows out of reach until the branch is recovered.
//!
//! Every connection of a tenant with `sprig_2pc` to a Galera node passes
//! the node agent's relay (port 7404); the tenant's backend accounts
//! accept connections only from the node itself, so the relay is the only
//! way in. The relay passes TLS through unchanged; what it knows of a
//! connection is its tenant (the gateway's preamble) and its backend
//! thread id (the server's greeting, which is never encrypted).
//!
//! ```text
//! prepare (the coordinator, through the agent, for a held branch on thread X):
//!   refuse while the tenant is fenced or X's client is gone
//!   register X as holder of (schema, dtid); then insert the redo rows
//! X's client goes away (its gateway died or gave up) while X holds:
//!   keep X's backend open, so MariaDB keeps the branch and its locks
//!   fence the tenant: refuse its new connections
//!   end every other connection of the tenant (KILL, and wait until gone),
//!   then X: no waiter can take X's rows before the recovery below
//!   recover the tenant's redo rows at any age: decide by insert race at
//!   the home; commit: replay (as sprig_ctl); abort: drop
//!   reopen the tenant (an undecided branch keeps it fenced; retried)
//! the agent starts: recover every redo row on the node before admitting anyone
//! sweep: a holder registered longer than txn.resolver_min_age_seconds (a
//!   stalled or partitioned gateway), or whose backend thread is gone, is lost
//! ```
//!
//! [`RelayFence`] holds the registry and runs these steps; [`RelayLinks`]
//! is the relay itself (the agent's TCP relay, or the simulation's).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use sprig_common::CancellationToken;
use sprig_common::ids::{ClusterId, TenantId};
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::backend::{AnchorLocator, ClusterAdmin, DbError, DbResult};
use crate::redo::RedoStmt;
use crate::resolver::{RedoReport, Resolver};
use crate::settings::ResolverConfig;
use crate::xid::{Dtid, Gtrid};

/// Errno the relay answers a refused connection with (and the agent a
/// refused prepare): 39448 `ER_SPRIG_HELD_BRANCH_FENCED`.
pub const FENCED_ERRNO: u16 = 39448;

/// How long the fence waits for one ended connection to leave the server.
pub const END_WAIT: Duration = Duration::from_secs(5);

/// The relay the fence works through.
#[async_trait::async_trait]
pub trait RelayLinks: Send + Sync + 'static {
    /// Ends one relayed connection now: `KILL CONNECTION` of its backend
    /// thread (as `sprig_ctl`, on the node itself) and both sides closed.
    /// Returns once the server no longer runs the thread, or after
    /// [`END_WAIT`]; `false` if it may still run.
    async fn end(&self, thread: u64) -> bool;

    /// Whether the backend side of the relayed connection on `thread` is
    /// still open (the server didn't end it).
    fn backend_alive(&self, thread: u64) -> bool;
}

/// Whether a new connection of a tenant may pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// Relay it.
    Open,
    /// Refuse it with 39448: the agent is starting, or the tenant is
    /// fenced while a lost holder's branch is recovered.
    Fenced,
}

/// What the relay does with a connection whose client went away.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientGone {
    /// Close the backend side as usual.
    Close,
    /// Keep the backend side open: it holds a branch; the fence ends it.
    Keep,
}

#[derive(Debug, Clone)]
struct Hold {
    thread: u64,
    schema: String,
    dtid: Dtid,
    since: Instant,
    /// Its redo rows are being written.
    inserting: bool,
}

#[derive(Debug, Default)]
struct Tenant {
    links: BTreeSet<u64>,
    /// Links whose client is gone, kept open because they hold.
    lost: BTreeSet<u64>,
    holds: Vec<Hold>,
    fenced: bool,
}

#[derive(Debug, Default)]
struct State {
    ready: bool,
    tenants: BTreeMap<TenantId, Tenant>,
}

/// What the fence has done since it started.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FenceStats {
    /// Tenants fenced (a holder lost).
    pub fenced: u64,
    /// Connections ended by fences.
    pub ended: u64,
    /// Held branches replayed by fences and the start-up recovery.
    pub replayed: u64,
    /// Held branches dropped (aborted) by them.
    pub dropped: u64,
}

/// The relay fence of one node (one cluster's writer).
pub struct RelayFence<L: RelayLinks> {
    cluster: ClusterId,
    links: Arc<L>,
    admin: Arc<dyn ClusterAdmin>,
    resolver: Resolver,
    state: Mutex<State>,
    stats: Mutex<FenceStats>,
    wake: Notify,
}

impl<L: RelayLinks> std::fmt::Debug for RelayFence<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelayFence")
            .field("cluster", &self.cluster)
            .finish_non_exhaustive()
    }
}

fn fenced_error(what: &str) -> DbError {
    DbError::new(FENCED_ERRNO, what)
}

impl<L: RelayLinks> RelayFence<L> {
    /// A fence for `cluster`'s writer node, acting through `admin` (as
    /// `sprig_ctl`, on the node) and deciding at the homes `locator` finds.
    /// It admits nobody until [`Self::start`] has recovered the node.
    pub fn new(
        cluster: ClusterId,
        links: Arc<L>,
        admin: Arc<dyn ClusterAdmin>,
        locator: Arc<dyn AnchorLocator>,
        cfg: ResolverConfig,
    ) -> Self {
        let resolver = Resolver::new(Arc::clone(&admin), locator, cfg);
        Self {
            cluster,
            links,
            admin,
            resolver,
            state: Mutex::new(State::default()),
            stats: Mutex::new(FenceStats::default()),
            wake: Notify::new(),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// What the fence has done.
    #[must_use]
    pub fn stats(&self) -> FenceStats {
        self.stats
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn count(&self, r: &RedoReport) {
        let mut s = self.stats.lock().unwrap_or_else(PoisonError::into_inner);
        s.replayed += r.replayed as u64;
        s.dropped += r.dropped as u64;
    }

    /// Whether the start-up recovery is done.
    #[must_use]
    pub fn ready(&self) -> bool {
        self.state().ready
    }

    /// Whether `tenant` is fenced now.
    #[must_use]
    pub fn is_fenced(&self, tenant: TenantId) -> bool {
        self.state().tenants.get(&tenant).is_some_and(|t| t.fenced)
    }

    /// Start-up: recovers every held branch on the node at any age, then
    /// admits connections. Every holder lived in this process (the relay
    /// is the only way in), so none is alive. A tenant with a branch that
    /// can't be decided yet (its home unreachable) stays fenced and is
    /// retried by [`Self::run`]; the others open.
    ///
    /// # Errors
    /// When the node's redo rows can't be read: nothing opens; retry.
    pub async fn start(&self) -> sprig_common::Result<RedoReport> {
        let all = self.admin.redo_branches(&self.cluster).await.map_err(|e| {
            sprig_common::SprigError::new(
                sprig_common::SprigCode::ClusterUnavailable,
                format!("redo rows on {}: {e}", self.cluster),
            )
        })?;
        let tenants: BTreeSet<TenantId> = all.iter().map(|b| b.gtrid.tenant).collect();
        let mut total = RedoReport::default();
        let mut stuck = BTreeSet::new();
        for t in tenants {
            match self.resolver.recover_held(&self.cluster, Some(t)).await {
                Ok(r) => {
                    if r.unresolved > 0 {
                        stuck.insert(t);
                    }
                    self.count(&r);
                    total.listed += r.listed;
                    total.replayed += r.replayed;
                    total.dropped += r.dropped;
                    total.gone += r.gone;
                    total.hazards += r.hazards;
                    total.unresolved += r.unresolved;
                }
                Err(_) => {
                    stuck.insert(t);
                }
            }
        }
        let mut st = self.state();
        for t in stuck {
            st.tenants.entry(t).or_default().fenced = true;
        }
        st.ready = true;
        drop(st);
        self.wake.notify_one();
        tracing::info!(cluster = %self.cluster, replayed = total.replayed, dropped = total.dropped,
            unresolved = total.unresolved, "relay fence: start-up recovery done; admitting connections");
        Ok(total)
    }

    /// Whether a new connection of `tenant` may pass.
    #[must_use]
    pub fn admit(&self, tenant: TenantId) -> Admission {
        let st = self.state();
        if !st.ready || st.tenants.get(&tenant).is_some_and(|t| t.fenced) {
            Admission::Fenced
        } else {
            Admission::Open
        }
    }

    /// A connection of `tenant` passed: its backend thread is `thread`.
    /// `false` if the tenant was fenced meanwhile: end it.
    pub fn opened(&self, tenant: TenantId, thread: u64) -> bool {
        let mut st = self.state();
        if !st.ready {
            return false;
        }
        let t = st.tenants.entry(tenant).or_default();
        if t.fenced {
            return false;
        }
        t.links.insert(thread);
        true
    }

    /// The client side of `thread` went away. A holder's backend stays
    /// open ([`ClientGone::Keep`]) and its tenant is fenced.
    pub fn client_gone(&self, tenant: TenantId, thread: u64) -> ClientGone {
        let mut st = self.state();
        let Some(t) = st.tenants.get_mut(&tenant) else {
            return ClientGone::Close;
        };
        if t.holds.iter().any(|h| h.thread == thread) {
            t.lost.insert(thread);
            if !t.fenced {
                t.fenced = true;
                self.stats
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .fenced += 1;
                tracing::warn!(cluster = %self.cluster, %tenant, thread,
                    "relay fence: a held branch's gateway went away; fencing the tenant");
            }
            drop(st);
            self.wake.notify_one();
            ClientGone::Keep
        } else {
            t.links.remove(&thread);
            ClientGone::Close
        }
    }

    /// The backend side of `thread` closed (the server ended it). A holder
    /// lost this way fences its tenant too.
    pub fn backend_gone(&self, tenant: TenantId, thread: u64) {
        let mut st = self.state();
        let Some(t) = st.tenants.get_mut(&tenant) else {
            return;
        };
        t.links.remove(&thread);
        if t.holds.iter().any(|h| h.thread == thread) && !t.fenced {
            t.fenced = true;
            self.stats
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .fenced += 1;
            drop(st);
            tracing::warn!(cluster = %self.cluster, %tenant, thread,
                "relay fence: a held branch's connection ended on the server; fencing the tenant");
            self.wake.notify_one();
        }
    }

    /// A held branch prepares (the coordinator, through the agent): the
    /// holder is registered first, then its redo rows go in as `sprig_ctl`.
    ///
    /// # Errors
    /// 39448 while the tenant is fenced; a lost-connection error when the
    /// holder's client is already gone (the transaction then aborts); the
    /// insert's error.
    #[allow(clippy::too_many_arguments)]
    pub async fn prepare(
        &self,
        tenant: TenantId,
        thread: u64,
        schema: &str,
        dtid: Dtid,
        gtrid: &Gtrid,
        stmts: &[RedoStmt],
    ) -> DbResult<()> {
        {
            let mut st = self.state();
            if !st.ready {
                return Err(fenced_error("the relay fence is starting"));
            }
            let t = st.tenants.entry(tenant).or_default();
            if t.fenced {
                return Err(fenced_error("the tenant is fenced on this node"));
            }
            if !t.links.contains(&thread) || t.lost.contains(&thread) {
                return Err(DbError::lost("the held branch's relayed connection is gone"));
            }
            t.holds.push(Hold {
                thread,
                schema: schema.to_owned(),
                dtid,
                since: Instant::now(),
                inserting: true,
            });
        }
        let r = self
            .admin
            .insert_redo(&self.cluster, schema, dtid, gtrid, stmts)
            .await;
        {
            let mut st = self.state();
            // On failure it is unknown whether the rows went in: the hold
            // stays either way, so the holder's loss still recovers them
            // (the coordinator's abort removes it through `finished`).
            if let Some(t) = st.tenants.get_mut(&tenant) {
                for h in t
                    .holds
                    .iter_mut()
                    .filter(|h| h.thread == thread && h.dtid == dtid)
                {
                    h.inserting = false;
                }
            }
        }
        self.wake.notify_one();
        r
    }

    /// A held branch finished (committed with its marker, or rolled back):
    /// its redo rows go, and so does the hold.
    ///
    /// # Errors
    /// The delete's error (the hold stays; recovery cleans up later).
    pub async fn finished(&self, schema: &str, dtid: Dtid) -> DbResult<()> {
        self.admin.delete_redo(&self.cluster, schema, dtid).await?;
        let mut st = self.state();
        for t in st.tenants.values_mut() {
            t.holds.retain(|h| h.dtid != dtid);
        }
        Ok(())
    }

    /// The coordinator gave up on a held branch (its phase 2 failed): the
    /// holder counts as lost.
    pub fn abandon(&self, dtid: Dtid, thread: u64) {
        let mut st = self.state();
        let mut hit = false;
        for t in st.tenants.values_mut() {
            if t.holds.iter().any(|h| h.dtid == dtid && h.thread == thread) {
                t.lost.insert(thread);
                if !t.fenced {
                    t.fenced = true;
                    hit = true;
                }
            }
        }
        drop(st);
        if hit {
            self.stats
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .fenced += 1;
            self.wake.notify_one();
        }
    }

    /// Runs fences and the sweep until `cancel`: every fenced tenant is
    /// recovered and reopened; a holder registered longer than `min_age`,
    /// or whose backend is gone, fences its tenant.
    pub async fn run(&self, cancel: CancellationToken) {
        loop {
            self.sweep();
            let fenced: Vec<TenantId> = {
                let st = self.state();
                if st.ready {
                    st.tenants
                        .iter()
                        .filter(|(_, t)| t.fenced)
                        .map(|(k, _)| *k)
                        .collect()
                } else {
                    Vec::new()
                }
            };
            for t in fenced {
                self.fence_tenant(t).await;
            }
            let interval = self.resolver.config().interval.min(Duration::from_secs(1));
            tokio::select! {
                () = cancel.cancelled() => return,
                () = self.wake.notified() => {}
                () = tokio::time::sleep(interval) => {}
            }
        }
    }

    /// Lost holders: registered past `min_age` (a stalled or partitioned
    /// gateway), or whose backend thread is gone.
    fn sweep(&self) {
        let min_age = self.resolver.config().min_age;
        let mut st = self.state();
        let mut newly = 0u64;
        for (tenant, t) in &mut st.tenants {
            if t.fenced {
                continue;
            }
            let lost = t.holds.iter().find(|h| {
                !h.inserting
                    && (h.since.elapsed() >= min_age || !self.links.backend_alive(h.thread))
            });
            if let Some(h) = lost {
                tracing::warn!(cluster = %self.cluster, %tenant, thread = h.thread, dtid = %h.dtid, schema = %h.schema,
                    "relay fence: a holder is lost (too old, or its connection ended); fencing the tenant");
                t.fenced = true;
                newly += 1;
            }
        }
        drop(st);
        if newly > 0 {
            self.stats
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .fenced += newly;
        }
    }

    /// One fenced tenant: end its connections (holders last), recover its
    /// held branches, reopen. Returns whether it reopened.
    pub async fn fence_tenant(&self, tenant: TenantId) -> bool {
        let (others, holders) = {
            let st = self.state();
            let Some(t) = st.tenants.get(&tenant) else {
                return true;
            };
            if !t.fenced {
                return true;
            }
            let holders: BTreeSet<u64> = t.holds.iter().map(|h| h.thread).collect();
            let others: Vec<u64> = t
                .links
                .iter()
                .copied()
                .filter(|th| !holders.contains(th))
                .collect();
            (others, holders.into_iter().collect::<Vec<u64>>())
        };
        // Waiters first: once they are gone, no statement can take a
        // holder's rows when its locks go.
        let ended = futures::future::join_all(others.iter().map(|&th| self.links.end(th))).await;
        if ended.iter().any(|ok| !ok) {
            tracing::warn!(cluster = %self.cluster, %tenant,
                "relay fence: a connection didn't end in time; the tenant stays fenced");
            return false;
        }
        let h_ended = futures::future::join_all(holders.iter().map(|&th| self.links.end(th))).await;
        if h_ended.iter().any(|ok| !ok) {
            return false;
        }
        {
            let mut st = self.state();
            if let Some(t) = st.tenants.get_mut(&tenant) {
                for th in others.iter().chain(holders.iter()) {
                    t.links.remove(th);
                    t.lost.remove(th);
                }
            }
        }
        self.stats
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .ended += (others.len() + holders.len()) as u64;
        // A prepare still writing its rows finishes first, so recovery
        // sees them.
        let until = Instant::now() + END_WAIT;
        loop {
            let busy = self
                .state()
                .tenants
                .get(&tenant)
                .is_some_and(|t| t.holds.iter().any(|h| h.inserting));
            if !busy {
                break;
            }
            if Instant::now() >= until {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let r = match self.resolver.recover_held(&self.cluster, Some(tenant)).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(cluster = %self.cluster, %tenant, error = %e,
                    "relay fence: recovery failed; the tenant stays fenced");
                return false;
            }
        };
        self.count(&r);
        if r.unresolved > 0 {
            tracing::warn!(cluster = %self.cluster, %tenant, unresolved = r.unresolved,
                "relay fence: a held branch can't be decided yet; the tenant stays fenced");
            return false;
        }
        let mut st = self.state();
        if let Some(t) = st.tenants.get_mut(&tenant) {
            t.holds.retain(|h| !holders.contains(&h.thread));
            t.fenced = false;
        }
        drop(st);
        tracing::info!(cluster = %self.cluster, %tenant, replayed = r.replayed, dropped = r.dropped,
            "relay fence: held branches recovered; the tenant is open again");
        true
    }
}

#[cfg(test)]
mod tests;
