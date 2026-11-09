//! Permits: the DC lease's sub-leases (chapter 25, "DC leases, sub-leases
//! and self-fencing", steps 3-9).
//!
//! Every gateway, agent and worker of a DC holds a [`PermitHolder`] and
//! renews it every second from its DC's controllers (`Permits.Renew`). A
//! permit is valid until the holder's clock, read **before** the request
//! was sent, plus the `valid_ms` the controller granted; so delays only end
//! it earlier. While the permit is valid the holder may write; when it
//! lapses or is refused:
//!
//! - a gateway fails writes for tenants homed in its DC with 39501
//!   `ER_SPRIG_DC_FENCED`; reads continue;
//! - an agent on a writer sets `read_only = ON` and kills tenant sessions;
//! - a worker stops cutovers and promotions.
//!
//! The [`Watchdog`] is a plain OS thread: when the renew loop hasn't
//! ticked for `failover.watchdog_ms`, it fences directly (the holder's
//! [`PermitHolder::force_fence`] and the role's callback); still stuck
//! after twice that, it runs the abort callback (by default
//! `std::process::abort`).
//!
//! [`PermitHolder::run`] is a plain future, like `DcLease::run`: a paused
//! process stops renewing.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};

/// What a holder asks for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermitRequest {
    /// The holder's registration id.
    pub holder_id: String,
    /// `gateway`, `agent` or `worker`.
    pub role: String,
}

/// A permit, as `Permits.Renew` returns it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Permit {
    /// The DC's epoch under its current lease.
    pub dc_epoch: u64,
    /// How long the permit is valid, from the holder's send time.
    pub valid_ms: u32,
    /// The DC is fenced: no permit.
    pub fenced: bool,
    /// New client connections each gateway may admit per second while the
    /// DC returns from an outage; 0 means no limit (chapter 25, "Traffic
    /// returns in steps").
    #[serde(default)]
    pub admit_per_second: u32,
}

impl Permit {
    /// A valid permit for `d`.
    #[must_use]
    pub fn valid(dc_epoch: u64, d: Duration) -> Self {
        Self {
            dc_epoch,
            valid_ms: u32::try_from(d.as_millis()).unwrap_or(u32::MAX),
            fenced: false,
            admit_per_second: 0,
        }
    }

    /// A refusal.
    #[must_use]
    pub fn fenced(dc_epoch: u64) -> Self {
        Self {
            dc_epoch,
            valid_ms: 0,
            fenced: true,
            admit_per_second: 0,
        }
    }
}

/// Where permits come from: the DC's controllers.
#[async_trait::async_trait]
pub trait PermitSource: Send + Sync {
    /// One `Permits.Renew` call.
    async fn renew(&self, req: &PermitRequest) -> Result<Permit>;
}

#[derive(Debug, Default)]
struct HolderState {
    expiry: Option<Instant>,
    dc_epoch: u64,
    admit_per_second: u32,
}

/// One holder's permit (see the module docs).
pub struct PermitHolder {
    req: PermitRequest,
    clock: Arc<dyn Clock>,
    state: Mutex<HolderState>,
    forced: AtomicBool,
    ticks: AtomicU64,
}

impl std::fmt::Debug for PermitHolder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PermitHolder")
            .field("req", &self.req)
            .field("writable", &self.writable())
            .finish_non_exhaustive()
    }
}

/// 39501 `ER_SPRIG_DC_FENCED`.
#[must_use]
pub fn dc_fenced_error(dc: &str) -> SprigError {
    SprigError::new(
        SprigCode::DcFenced,
        format!("data center {dc} is fenced: it holds no valid DC lease; writes are refused"),
    )
}

impl PermitHolder {
    /// A holder with no permit yet.
    #[must_use]
    pub fn new(holder_id: impl Into<String>, role: &str, clock: Arc<dyn Clock>) -> Self {
        Self {
            req: PermitRequest {
                holder_id: holder_id.into(),
                role: role.to_owned(),
            },
            clock,
            state: Mutex::new(HolderState::default()),
            forced: AtomicBool::new(false),
            ticks: AtomicU64::new(0),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HolderState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether the holder may write now.
    #[must_use]
    pub fn writable(&self) -> bool {
        if self.forced.load(Ordering::SeqCst) {
            return false;
        }
        let now = self.clock.now();
        self.lock().expiry.is_some_and(|e| now < e)
    }

    /// When the permit ends, on the holder's clock.
    #[must_use]
    pub fn expiry(&self) -> Option<Instant> {
        self.lock().expiry
    }

    /// Time left on the permit (zero when fenced).
    #[must_use]
    pub fn remaining(&self) -> Duration {
        if self.forced.load(Ordering::SeqCst) {
            return Duration::ZERO;
        }
        let now = self.clock.now();
        self.lock()
            .expiry
            .map_or(Duration::ZERO, |e| e.saturating_duration_since(now))
    }

    /// The DC epoch of the last permit.
    #[must_use]
    pub fn dc_epoch(&self) -> u64 {
        self.lock().dc_epoch
    }

    /// The admission rate of the last permit (0: no limit).
    #[must_use]
    pub fn admit_per_second(&self) -> u32 {
        self.lock().admit_per_second
    }

    /// Renew rounds so far (the watchdog compares it).
    #[must_use]
    pub fn ticks(&self) -> u64 {
        self.ticks.load(Ordering::SeqCst)
    }

    /// Fences the holder until [`Self::clear_forced`]: the watchdog's
    /// direct fence.
    pub fn force_fence(&self) {
        self.forced.store(true, Ordering::SeqCst);
    }

    /// Clears a forced fence (the renew loop is ticking again).
    pub fn clear_forced(&self) {
        self.forced.store(false, Ordering::SeqCst);
    }

    /// Whether the watchdog fenced the holder.
    #[must_use]
    pub fn is_forced(&self) -> bool {
        self.forced.load(Ordering::SeqCst)
    }

    /// One renewal: asks each source in turn until one answers, each
    /// within `timeout`. Returns whether the holder is writable after it.
    pub async fn renew_once(&self, sources: &[Arc<dyn PermitSource>], timeout: Duration) -> bool {
        self.ticks.fetch_add(1, Ordering::SeqCst);
        for s in sources {
            // Read before sending: delays only end the permit earlier.
            let ts = self.clock.now();
            match tokio::time::timeout(timeout, s.renew(&self.req)).await {
                Ok(Ok(p)) => {
                    let mut st = self.lock();
                    st.dc_epoch = p.dc_epoch;
                    st.admit_per_second = p.admit_per_second;
                    if p.fenced {
                        st.expiry = None;
                    } else {
                        let to = ts + Duration::from_millis(u64::from(p.valid_ms));
                        // A later answer never shortens what an earlier
                        // one granted, but a refusal ends it at once.
                        st.expiry = Some(st.expiry.map_or(to, |e| e.max(to)));
                    }
                    break;
                }
                Ok(Err(e)) => tracing::debug!(error = %e, "permit renewal failed"),
                Err(_) => tracing::debug!("permit renewal timed out"),
            }
        }
        if self.forced.load(Ordering::SeqCst) && self.lock().expiry.is_some() {
            // The loop runs again: the watchdog's fence can lift.
            self.clear_forced();
        }
        self.writable()
    }

    /// Renews every `period` until dropped.
    pub async fn run(&self, sources: Vec<Arc<dyn PermitSource>>, period: Duration) {
        loop {
            self.renew_once(&sources, period).await;
            tokio::time::sleep(period).await;
        }
    }
}

/// A watchdog thread over a holder (see the module docs). Stops when
/// dropped.
#[derive(Debug)]
pub struct Watchdog {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

/// What the watchdog calls.
pub type WatchdogAction = Box<dyn Fn() + Send + Sync>;

impl Watchdog {
    /// Watches `ticks` (the renew loop's counter): no change for `stall`
    /// runs `fence`, no change for twice that runs `abort`. Real time: the
    /// watchdog is for real processes, never for simulations.
    #[must_use]
    pub fn spawn(
        ticks: Arc<dyn Fn() -> u64 + Send + Sync>,
        stall: Duration,
        fence: WatchdogAction,
        abort: WatchdogAction,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&stop);
        let thread = std::thread::Builder::new()
            .name("sprig-watchdog".into())
            .spawn(move || {
                let mut last = ticks();
                let mut since = Instant::now();
                let mut fenced = false;
                let nap = (stall / 4).max(Duration::from_millis(5));
                while !s.load(Ordering::SeqCst) {
                    let slept_from = Instant::now();
                    std::thread::sleep(nap);
                    let t = ticks();
                    if t != last {
                        last = t;
                        since = Instant::now();
                        fenced = false;
                        continue;
                    }
                    // The watchdog's own nap overran by more than `stall`:
                    // the whole process was paused (SIGSTOP, a frozen VM),
                    // not just the loop. Fence (any permit has expired
                    // meanwhile) and give the loop time to tick again
                    // before judging it stuck.
                    if slept_from.elapsed() >= nap + stall {
                        tracing::warn!(paused = ?slept_from.elapsed(), "process was paused: fencing");
                        fence();
                        fenced = true;
                        since = Instant::now();
                        continue;
                    }
                    let stuck = since.elapsed();
                    if stuck >= stall * 2 {
                        tracing::error!(?stuck, "permit loop stuck: aborting");
                        abort();
                        since = Instant::now();
                    } else if stuck >= stall && !fenced {
                        tracing::warn!(?stuck, "permit loop stalled: fencing");
                        fence();
                        fenced = true;
                    }
                }
            })
            .ok();
        Self { stop, thread }
    }

    /// A watchdog over a holder: fences it, then runs `abort`.
    #[must_use]
    pub fn over(holder: &Arc<PermitHolder>, stall: Duration, abort: WatchdogAction) -> Self {
        let (h1, h2) = (Arc::clone(holder), Arc::clone(holder));
        Self::spawn(
            Arc::new(move || h1.ticks()),
            stall,
            Box::new(move || h2.force_fence()),
            abort,
        )
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// A gateway's admission of new connections while its DC returns: a token
/// bucket refilled at the permit's `admit_per_second` (0: no limit);
/// refused clients get 39531 with a retry delay of 1-5 s, fully jittered
/// (chapter 25, "Traffic returns in steps").
#[derive(Debug, Clone)]
pub struct Admission {
    tokens: f64,
    last: Instant,
    seed: u64,
}

impl Admission {
    /// An empty bucket at `now` (a returning gateway starts with none).
    #[must_use]
    pub fn new(now: Instant, seed: u64) -> Self {
        Self {
            tokens: 0.0,
            last: now,
            seed: seed | 1,
        }
    }

    fn jitter(&mut self) -> Duration {
        // xorshift64: full jitter between 1 and 5 s, no shared RNG needed.
        self.seed ^= self.seed << 13;
        self.seed ^= self.seed >> 7;
        self.seed ^= self.seed << 17;
        Duration::from_millis(1000 + self.seed % 4001)
    }

    /// Admits one connection or returns 39531 with a retry delay.
    ///
    /// # Errors
    /// 39531 `ER_SPRIG_ADMISSION_BACKOFF` (SQLSTATE 08S01).
    pub fn admit(&mut self, now: Instant, rate: u32) -> Result<()> {
        self.admit_or_wait(now, rate).map_err(|wait| {
            SprigError::new(
                SprigCode::AdmissionBackoff,
                format!(
                    "this data center is returning from an outage and admits connections in steps; \
                     retry in {} ms",
                    wait.as_millis()
                ),
            )
        })
    }

    /// As [`Self::admit`], returning only the retry delay of a refusal.
    ///
    /// # Errors
    /// The delay after which the client should retry.
    pub fn admit_or_wait(&mut self, now: Instant, rate: u32) -> std::result::Result<(), Duration> {
        if rate == 0 {
            return Ok(());
        }
        let dt = now.saturating_duration_since(self.last).as_secs_f64();
        self.last = now;
        let cap = f64::from(rate);
        self.tokens = (self.tokens + dt * cap).min(cap);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            return Ok(());
        }
        Err(self.jitter())
    }
}

pub mod http;

#[cfg(test)]
mod tests;
