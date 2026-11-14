//! Live listener changes from the settings watch, with no restart.
//!
//! [`follow`] waits on a [`LiveSettings`] subscription to the keys that
//! shape listeners ([`LISTENER_KEYS`]), re-resolves every listener from the
//! new snapshot, and hands the result to [`ListenerManager::apply`], which
//! binds a new socket before closing the old one and drains connections of
//! a stricter generation. A change that can't be applied (port in use, bad
//! certificate) keeps the running listener and raises `SettingNotApplied`
//! once per distinct failure.
//!
//! Certificate *contents* rotate without a settings change: the
//! [`CertStore`] poller swaps them in place. A changed certificate *path*
//! is a settings change and loads a new bundle here.

use std::collections::BTreeSet;
use std::sync::Arc;

use sprig_common::CancellationToken;
use sprig_common::ids::DcId;
use sprig_settings::{LiveSettings, SettingCtx};
use tokio::sync::watch;

use super::{ApplyReport, ListenerManager, resolve};
use crate::tls::CertStore;

/// The keys whose change re-resolves the listeners: every `listener.*` and
/// `tls.*` key, and the gateway keys a [`ListenerSpec`](super::ListenerSpec)
/// carries.
pub const LISTENER_KEYS: &[&str] = &[
    "listener.*",
    "tls.*",
    "gateway.proxy_protocol",
    "gateway.proxy_trusted_cidrs",
    "gateway.handshake_timeout_ms",
];

/// Resolves the listeners from the current snapshot of `live` and applies
/// them.
pub async fn apply_current(
    live: &LiveSettings,
    dc: &DcId,
    certs: &CertStore,
    manager: &ListenerManager,
) -> ApplyReport {
    let snapshot = live.snapshot();
    let ctx = SettingCtx::process(dc);
    let res = resolve(&snapshot, &ctx, certs);
    manager.apply(res).await
}

/// Re-applies the listeners after every change seen on `changes` (a
/// subscription to [`LISTENER_KEYS`], taken before the initial apply so no
/// change falls in between) until `stop` is cancelled. `seen` holds the
/// rejections already reported by the initial apply.
pub async fn follow(
    live: LiveSettings,
    dc: DcId,
    certs: Arc<CertStore>,
    manager: Arc<ListenerManager>,
    mut changes: watch::Receiver<u64>,
    mut seen: BTreeSet<(&'static str, String)>,
    stop: CancellationToken,
) {
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            r = changes.changed() => if r.is_err() { return },
        }
        let report = apply_current(&live, &dc, &certs, &manager).await;
        for name in &report.applied {
            metrics::counter!("sprig_meta_reconfigurations_total", "what" => "listener", "result" => "applied").increment(1);
            tracing::info!(
                event = "gateway.listener_reconfigured",
                listener = name,
                "listener settings applied live"
            );
        }
        let now = rejections(&report);
        for (name, reason) in now.difference(&seen) {
            metrics::counter!("sprig_meta_reconfigurations_total", "what" => "listener", "result" => "not_applied").increment(1);
            sprig_settings::not_applied("listener", &format!("listener {name}: {reason}"));
        }
        seen = now;
    }
}

/// The rejections of a report, for de-duplicating alerts.
#[must_use]
pub fn rejections(report: &ApplyReport) -> BTreeSet<(&'static str, String)> {
    report
        .rejected
        .iter()
        .map(|r| (r.name, r.reason.clone()))
        .collect()
}

#[cfg(test)]
mod tests;
