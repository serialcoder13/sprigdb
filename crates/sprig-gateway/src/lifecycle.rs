//! Drain.
//!
//! Drain starts on SIGTERM, on `sprigctl gateway drain` (the Admin API's
//! `Gateways.DrainGateway`, reaching this gateway directly or through the
//! peer service), or when the provisioner removes the gateway:
//!
//! ```text
//! 1. state = draining; /readyz = 503; wait gateway.drain_lb_delay_seconds.
//! 2. Close every listener.
//! 3. Close idle sessions with no pins; mark the rest close_at_safe_point.
//! 4. Wait for zero sessions or gateway.drain_timeout_seconds; close what remains.
//! 5. Flush the workload hook, end feed subscriptions, revoke the lease, exit 0.
//! ```
//!
//! [`Lifecycle`] carries the request and the flag the services check (a
//! draining gateway refuses `Transactions.Begin` with 39000 and reports
//! `NOT_SERVING` on `grpc.health.v1`); [`crate::run`] runs the steps.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use sprig_common::CancellationToken;
use sprig_settings::{Apply, HardBounds, Scopes, Setting};

/// `gateway.drain_lb_delay_seconds`.
pub static DRAIN_LB_DELAY_SECONDS: Setting<u64> = Setting {
    key: "gateway.drain_lb_delay_seconds",
    default: 10,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(0, 3600),
    apply: Apply::Live,
    operator_only: false,
    owner: 4,
    doc: "Readiness off to listeners closed, when the gateway drains",
};
sprig_settings::register!(DRAIN_LB_DELAY_SECONDS);

/// The drain request and state of one gateway.
#[derive(Debug, Clone)]
pub struct Lifecycle {
    draining: Arc<AtomicBool>,
    requested: CancellationToken,
}

impl Default for Lifecycle {
    fn default() -> Self {
        Self::new()
    }
}

impl Lifecycle {
    /// Not draining.
    #[must_use]
    pub fn new() -> Self {
        Self {
            draining: Arc::new(AtomicBool::new(false)),
            requested: CancellationToken::new(),
        }
    }

    /// Asks for a drain; `false` if one was already asked for.
    pub fn request_drain(&self, reason: &str) -> bool {
        if self.requested.is_cancelled() {
            return false;
        }
        tracing::info!(event = "gateway.drain_requested", reason, "drain requested");
        self.requested.cancel();
        true
    }

    /// Resolves when a drain is asked for.
    pub async fn requested(&self) {
        self.requested.cancelled().await;
    }

    /// Marks the gateway draining (step 1).
    pub fn mark_draining(&self) {
        self.draining.store(true, Ordering::SeqCst);
        // A SIGTERM drains as well as an admin request.
        self.requested.cancel();
    }

    /// Whether the gateway is draining.
    #[must_use]
    pub fn draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }

    /// The flag the HTTP services read.
    #[must_use]
    pub fn flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.draining)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_drain_is_requested_once() {
        let l = Lifecycle::new();
        assert!(!l.draining());
        assert!(l.request_drain("test"));
        assert!(!l.request_drain("again"));
        l.requested().await;
        l.mark_draining();
        assert!(l.draining());
        assert!(l.flag().load(Ordering::SeqCst));
    }
}
