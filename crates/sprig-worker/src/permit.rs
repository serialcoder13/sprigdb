//! The worker's DC permit (chapter 25, "DC leases, sub-leases and
//! self-fencing" step 7: "worker: stops cutovers and promotions").
//!
//! The worker renews a permit from its DC's controllers like a gateway
//! (`sprig_meta::permit::http::PermitKeeper`, armed once a controller
//! serving permits is seen) and runs the mover's job runtime only while it
//! may write: when the permit lapses, the runtime is stopped, which aborts
//! every running job (a cutover cut short is undone by its hold deadline,
//! chapter 10), and it starts again once the permit is valid. A watchdog
//! thread fences the permit when the runtime stalls and aborts the process
//! when it stays stuck, as in the other roles.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use sprig_common::time::SystemClock;
use sprig_common::{CancellationToken, Result};
use sprig_meta::MetaStore;
use sprig_meta::permit::http::PermitKeeper;
use sprig_meta::permit::{PermitHolder, Watchdog};

/// How often the gate looks at the permit.
const CHECK: Duration = Duration::from_millis(200);

/// Starts the worker's permit keeper and its watchdog (`stall` =
/// `failover.watchdog_ms`); the keeper stops at `stop`.
pub fn keep(
    worker: &str,
    dc: Arc<dyn MetaStore>,
    stall: Duration,
    stop: CancellationToken,
) -> (Arc<PermitKeeper>, Watchdog) {
    let holder = Arc::new(PermitHolder::new(worker, "worker", Arc::new(SystemClock)));
    let keeper = Arc::new(PermitKeeper::new(Arc::clone(&holder)));
    drop(tokio::spawn(Arc::clone(&keeper).run(
        dc,
        Duration::from_secs(1),
        stop,
    )));
    let beat = keeper.heartbeat();
    let h2 = Arc::clone(&holder);
    let dog = Watchdog::spawn(
        Arc::new(move || beat.load(Ordering::SeqCst)),
        stall,
        Box::new(move || h2.force_fence()),
        Box::new(|| {
            tracing::error!("the worker's runtime is stuck: aborting (chapter 25 watchdog)");
            std::process::abort();
        }),
    );
    (keeper, dog)
}

/// Runs `start(token)` only while `may_write()` holds, until `shutdown`:
/// the token is cancelled when the permit lapses, and `start` is called
/// again once it is back. Returns the last run's result.
///
/// # Errors
/// The last run's error.
pub async fn gated<F, Fut>(
    may_write: impl Fn() -> bool + Send,
    shutdown: CancellationToken,
    mut start: F,
) -> Result<()>
where
    F: FnMut(CancellationToken) -> Fut,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let mut last = Ok(());
    while !shutdown.is_cancelled() {
        if !may_write() {
            tokio::select! {
                () = shutdown.cancelled() => break,
                () = tokio::time::sleep(CHECK) => {}
            }
            continue;
        }
        let run = shutdown.child_token();
        let mut task = tokio::spawn(start(run.clone()));
        let lapsed = loop {
            tokio::select! {
                r = &mut task => {
                    last = r.unwrap_or_else(|e| Err(sprig_common::SprigError::internal(e.to_string())));
                    break false;
                }
                () = tokio::time::sleep(CHECK) => {
                    if !may_write() {
                        break true;
                    }
                }
            }
        };
        if lapsed {
            tracing::warn!(
                "no valid DC permit: mover jobs stopped (cutovers and moves wait for the permit)"
            );
            run.cancel();
            last = task
                .await
                .unwrap_or_else(|e| Err(sprig_common::SprigError::internal(e.to_string())));
            tracing::info!("mover job runtime stopped; it starts again with a valid permit");
        } else if !shutdown.is_cancelled() {
            // The runtime ended by itself: don't spin.
            tokio::select! {
                () = shutdown.cancelled() => {}
                () = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    }
    last
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    use super::*;

    #[tokio::test]
    async fn the_runtime_stops_when_the_permit_lapses_and_starts_again() {
        let ok = Arc::new(AtomicBool::new(true));
        let starts = Arc::new(AtomicUsize::new(0));
        let stops = Arc::new(AtomicUsize::new(0));
        let shutdown = CancellationToken::new();
        let (o, s1, s2) = (Arc::clone(&ok), Arc::clone(&starts), Arc::clone(&stops));
        let sd = shutdown.clone();
        let gate = tokio::spawn(async move {
            gated(
                move || o.load(Ordering::SeqCst),
                sd,
                move |t: CancellationToken| {
                    s1.fetch_add(1, Ordering::SeqCst);
                    let s2 = Arc::clone(&s2);
                    async move {
                        t.cancelled().await;
                        s2.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                },
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        ok.store(false, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert_eq!(stops.load(Ordering::SeqCst), 1, "stopped without a permit");
        assert_eq!(starts.load(Ordering::SeqCst), 1, "not started again yet");
        ok.store(true, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert_eq!(starts.load(Ordering::SeqCst), 2, "started again");
        shutdown.cancel();
        gate.await.expect("join").expect("ok");
        assert_eq!(stops.load(Ordering::SeqCst), 2);
    }
}
