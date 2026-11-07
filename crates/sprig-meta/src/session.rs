//! Sessions: a lease kept alive in the background, with a conservative
//! local deadline (chapter 23, "Leases with a conservative deadline").
//!
//! ```text
//! keep(lease, ttl, period):          # period = ttl/3
//! 1. deadline = t0 + ttl             # t0 read before the grant request
//! 2. every period:
//!    a. ts = now()                   # read BEFORE sending
//!    b. r = keep_alive(lease), timeout = period
//!    c. if r == Some(granted): deadline = max(deadline, ts + granted)
//!    d. if r == None, or now() >= deadline - margin: mark lost; stop
//! ```
//!
//! Reading `ts` before sending means delays only move the deadline earlier.
//! etcd never expires a lease sooner than its TTL after the last keep-alive
//! it handled, so a process that stops at `deadline - margin` stops before
//! etcd can give its role away.
//!
//! Time comes from the injected [`Clock`]; waits use tokio's timer, so a
//! simulation drives both with tokio's paused clock (see
//! [`TokioClock`]).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime};

use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::{LeaseId, MetaStore};

/// Default margin before the deadline at which a session counts as lost.
pub const DEFAULT_MARGIN: Duration = Duration::from_secs(1);

/// How a session keeps its lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionOpts {
    /// The lease TTL (`control.leader_lease_seconds` for leaders).
    pub ttl: Duration,
    /// Stop this long before the deadline (1 s for sessions).
    pub margin: Duration,
    /// Keep-alive period; `ttl / 3` by default.
    pub period: Duration,
}

impl SessionOpts {
    /// `ttl` with the default margin and period.
    #[must_use]
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            margin: DEFAULT_MARGIN,
            period: ttl / 3,
        }
    }
}

/// A lease kept alive in the background. Cloning shares the session; the
/// keep-alive stops when the last clone drops (the lease then expires on its
/// TTL) or on [`Session::close`].
#[derive(Clone)]
pub struct Session {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("lease", &self.inner.state.lease)
            .field("lost", &self.is_lost())
            .finish_non_exhaustive()
    }
}

/// State shared with the keep-alive task.
struct State {
    lease: LeaseId,
    margin: Duration,
    clock: Arc<dyn Clock>,
    deadline: Mutex<Instant>,
    lost: watch::Sender<bool>,
    halted: AtomicBool,
}

struct Inner {
    state: Arc<State>,
    store: Arc<dyn MetaStore>,
    task: JoinHandle<()>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl State {
    fn deadline(&self) -> Instant {
        *self.deadline.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn extend(&self, to: Instant) {
        let mut d = self.deadline.lock().unwrap_or_else(PoisonError::into_inner);
        *d = (*d).max(to);
    }

    fn expired(&self) -> bool {
        self.clock.now() + self.margin >= self.deadline()
    }

    fn mark_lost(&self) {
        self.lost.send_replace(true);
    }
}

impl Session {
    /// Grants a lease and starts keeping it alive.
    ///
    /// # Errors
    /// The store's error if the grant fails; internal if `opts.ttl` is not
    /// longer than `opts.margin` or the period is zero.
    pub async fn open(
        store: Arc<dyn MetaStore>,
        clock: Arc<dyn Clock>,
        opts: SessionOpts,
    ) -> Result<Self> {
        if opts.ttl <= opts.margin || opts.period.is_zero() {
            return Err(SprigError::internal(format!(
                "session ttl {:?} must exceed margin {:?}, with a non-zero period",
                opts.ttl, opts.margin
            )));
        }
        let t0 = clock.now();
        let lease = store.grant_lease(opts.ttl).await?;
        let (lost, _) = watch::channel(false);
        let state = Arc::new(State {
            lease,
            margin: opts.margin,
            deadline: Mutex::new(t0 + opts.ttl),
            clock,
            lost,
            halted: AtomicBool::new(false),
        });
        let task = tokio::spawn(keep(Arc::clone(&store), Arc::clone(&state), opts.period));
        Ok(Self {
            inner: Arc::new(Inner { state, store, task }),
        })
    }

    /// The session's lease; bind keys to it so they vanish with the session.
    #[must_use]
    pub fn lease(&self) -> LeaseId {
        self.inner.state.lease
    }

    /// The store the session lives in.
    #[must_use]
    pub fn store(&self) -> &Arc<dyn MetaStore> {
        &self.inner.store
    }

    /// Before this instant (on the session's clock) the lease is certainly
    /// alive. Side effects outside etcd check it first.
    #[must_use]
    pub fn valid_until(&self) -> Instant {
        let s = &self.inner.state;
        s.deadline()
            .checked_sub(s.margin)
            .unwrap_or_else(|| s.deadline())
    }

    /// Whether the session is lost: the lease ended, its deadline minus the
    /// margin passed, or the session was closed.
    #[must_use]
    pub fn is_lost(&self) -> bool {
        let s = &self.inner.state;
        if *s.lost.borrow() {
            return true;
        }
        if s.expired() {
            s.mark_lost();
            return true;
        }
        false
    }

    /// Resolves once the session is lost. Loss is noticed by the keep-alive
    /// task, so this may resolve up to one period after [`Self::is_lost`]
    /// turns true.
    pub async fn lost(&self) {
        let mut rx = self.inner.state.lost.subscribe();
        // The sender lives in `self`, so the channel can't close here.
        let _ = rx.wait_for(|lost| *lost).await;
    }

    /// Stops the keep-alive and revokes the lease, deleting its keys.
    ///
    /// # Errors
    /// The store's error if the revoke fails; the lease then ends on its TTL.
    pub async fn close(&self) -> Result<()> {
        self.inner.task.abort();
        self.inner.state.mark_lost();
        self.inner.store.revoke_lease(self.lease()).await
    }

    /// Simulates a hung process: keep-alives stop being sent, but the local
    /// deadline is still checked, so the session is marked lost on time.
    #[cfg(any(test, feature = "conformance"))]
    #[doc(hidden)]
    pub fn halt_keep_alive(&self) {
        self.inner.state.halted.store(true, Ordering::SeqCst);
    }
}

/// The keep-alive loop.
async fn keep(store: Arc<dyn MetaStore>, s: Arc<State>, period: Duration) {
    loop {
        tokio::time::sleep(period).await;
        if *s.lost.borrow() {
            return;
        }
        if !s.halted.load(Ordering::SeqCst) {
            let ts = s.clock.now();
            match tokio::time::timeout(period, store.keep_alive(s.lease)).await {
                Ok(Ok(Some(granted))) => s.extend(ts + granted),
                Ok(Ok(None)) => {
                    tracing::warn!(lease = s.lease.0, "session lease ended");
                    s.mark_lost();
                    return;
                }
                Ok(Err(e)) => tracing::warn!(lease = s.lease.0, error = %e, "keep-alive failed"),
                Err(_) => tracing::warn!(lease = s.lease.0, "keep-alive timed out"),
            }
        }
        if s.expired() {
            tracing::warn!(lease = s.lease.0, "session deadline passed");
            s.mark_lost();
            return;
        }
    }
}

/// The error for work refused because a session or leadership was lost.
/// Not retryable: the caller must step down, not retry.
pub(crate) fn lost_error(what: &str) -> SprigError {
    let mut e = SprigError::new(SprigCode::MetaUnavailable, format!("{what} lost"));
    e.retryable = false;
    e
}

/// A [`Clock`] that follows tokio's timer, so a runtime with paused time
/// drives leases, sessions and their sleeps together.
#[derive(Debug, Clone, Copy)]
pub struct TokioClock {
    origin: tokio::time::Instant,
    wall_origin: SystemTime,
}

impl TokioClock {
    /// A clock starting now.
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: tokio::time::Instant::now(),
            wall_origin: SystemTime::now(),
        }
    }
}

impl Default for TokioClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for TokioClock {
    fn now(&self) -> Instant {
        tokio::time::Instant::now().into_std()
    }
    fn wall(&self) -> SystemTime {
        self.wall_origin + (tokio::time::Instant::now() - self.origin)
    }
}
