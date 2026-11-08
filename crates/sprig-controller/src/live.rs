//! The controller's live settings (chapter 23, "Settings: writes, bounds
//! and watches"): the `global`, `dc` and `cluster` scopes mirrored from
//! etcd by [`LiveSettings`], and the [`ControlSettings`] snapshot the
//! runtime reads, re-resolved after every change.
//!
//! Loops that work per cluster resolve their keys themselves from
//! [`LiveSettings::snapshot`] with the cluster in the context, so a
//! `cluster`-scope value applies to that cluster only.

use std::sync::Arc;

use sprig_common::ids::DcId;
use sprig_common::{CancellationToken, Result};
use sprig_settings::{LiveSettings, Registry, SettingsStores};
use tokio::sync::watch;

use crate::runtime::Stores;
use crate::settings::ControlSettings;

/// The controller's settings: the mirrors and the runtime's snapshot.
#[derive(Debug, Clone)]
pub struct ControllerSettings {
    /// Every mirrored row.
    pub live: LiveSettings,
    /// The runtime's keys for this DC; changes on every relevant change.
    pub control: watch::Receiver<Arc<ControlSettings>>,
}

/// Starts the mirrors over `stores`, waits for the first snapshot, and
/// follows changes until `shutdown`. `None` if shutdown came first.
///
/// # Errors
/// Internal if the setting registry is invalid (a bug) or a store isn't
/// the tier it should be.
pub async fn start(
    stores: &Stores,
    dc: &DcId,
    shutdown: &CancellationToken,
) -> Result<Option<ControllerSettings>> {
    let live = LiveSettings::start(
        Registry::global()?,
        SettingsStores::new(Arc::clone(&stores.global), Arc::clone(&stores.dc))?,
    )?;
    tokio::select! {
        () = shutdown.cancelled() => return Ok(None),
        () = live.ready() => {}
    }
    let first = ControlSettings::resolve(&live.snapshot(), dc);
    tracing::info!(?first, "settings mirrored");
    let (tx, control) = watch::channel(Arc::new(first));
    tokio::spawn(follow(live.clone(), dc.clone(), tx, shutdown.clone()));
    Ok(Some(ControllerSettings { live, control }))
}

/// Re-resolves [`ControlSettings`] after each change and publishes it when
/// it differs.
async fn follow(
    live: LiveSettings,
    dc: DcId,
    tx: watch::Sender<Arc<ControlSettings>>,
    shutdown: CancellationToken,
) {
    let mut changes = live.changes();
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            r = changes.changed() => if r.is_err() { return },
        }
        let next = ControlSettings::resolve(&live.snapshot(), &dc);
        tx.send_if_modified(|cur| {
            if **cur == next {
                return false;
            }
            tracing::info!(?next, "controller settings changed");
            *cur = Arc::new(next);
            true
        });
    }
}
