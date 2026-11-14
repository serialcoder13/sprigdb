//! Live listener changes for the MySQL family.
//!
//! - **Per greeting:** mode, certificate and `tls.min_version` come from
//!   the listener snapshot the connection was accepted under
//!   (`ClientConn::listener` and its acceptor), and the version strings are
//!   read from the live settings when the greeting is built, so every
//!   change reaches new connections only.
//! - **Stricter listeners:** when `listener.<name>.tls` tightens,
//!   `tls.min_version` rises, or the listener moves or is disabled,
//!   sessions accepted under the looser spec that the new spec no longer
//!   allows ([`Strictness::keeps`]: plaintext under `required`, TLS 1.2
//!   under a 1.3 minimum, any session of a moved or disabled listener) are
//!   marked `close_at_safe_point` (reason `ListenerRetired`) once
//!   `listener.drain_seconds` has passed. The front end then
//!   answers their next command with 39004 and closes; a session inside a
//!   transaction closes after it ends. A session that already meets the
//!   new spec (TLS, at a high enough version) stays open.
//!
//! [`ListenerHooks`] keeps the spec each open MySQL session was accepted
//! under and its transport (updated when the client upgrades to TLS), so
//! the comparison is exact whatever generations came between. The
//! listener manager closes the same sessions by force at twice the drain
//! time if they are still open (an idle one closes then, without a
//! packet).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use sprig_common::CancellationToken;
use sprig_common::ids::{ConnId, DcId};
use sprig_settings::{LiveSettings, SettingCtx};

use crate::frontend::ConnTransport;
pub use crate::listener::Strictness;
use crate::listener::settings::{DRAIN_SECONDS, LISTENERS, TLS_MIN_VERSION};
use crate::listener::{ListenerSpec, TlsMode, TlsVersion};
use crate::session::{CloseReason, SessionHandle};

struct Entry {
    handle: Weak<SessionHandle>,
    listener: &'static str,
    accepted: Strictness,
    transport: Arc<ConnTransport>,
}

#[derive(Default)]
struct Inner {
    sessions: Mutex<HashMap<ConnId, Entry>>,
}

/// The MySQL front ends' registry of open sessions and their listener
/// specs. Cheap to clone.
#[derive(Clone, Default)]
pub struct ListenerHooks {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for ListenerHooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListenerHooks")
            .field("sessions", &lock(&self.inner.sessions).len())
            .finish()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Removes a session from the registry when dropped.
#[derive(Debug)]
pub struct Registered {
    hooks: ListenerHooks,
    id: ConnId,
}

impl Drop for Registered {
    fn drop(&mut self) {
        lock(&self.hooks.inner.sessions).remove(&self.id);
    }
}

impl ListenerHooks {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records that `handle` was accepted under `spec` and runs over
    /// `transport`, until the guard drops.
    #[must_use]
    pub fn register(
        &self,
        handle: &Arc<SessionHandle>,
        spec: &ListenerSpec,
        transport: Arc<ConnTransport>,
    ) -> Registered {
        lock(&self.inner.sessions).insert(
            handle.id(),
            Entry {
                handle: Arc::downgrade(handle),
                listener: spec.name,
                accepted: Strictness::of(spec),
                transport,
            },
        );
        Registered {
            hooks: self.clone(),
            id: handle.id(),
        }
    }

    /// Listener `name` is now `now`: after `drain`, the sessions it is
    /// stricter for and no longer allows ([`Strictness::keeps`], with the
    /// session's transport at that time) are marked to close at their next
    /// safe point. Returns how many sessions it is stricter for (the
    /// candidates; those already on TLS that meets `now` stay open).
    #[allow(clippy::must_use_candidate)] // called for its effect; tests read the count
    pub fn listener_changed(&self, name: &'static str, now: Strictness, drain: Duration) -> usize {
        let candidates: Vec<(Weak<SessionHandle>, Strictness, Arc<ConnTransport>)> =
            lock(&self.inner.sessions)
                .values()
                .filter(|e| e.listener == name && now.stricter_than(&e.accepted))
                .map(|e| (e.handle.clone(), e.accepted, Arc::clone(&e.transport)))
                .collect();
        let n = candidates.len();
        if n > 0 {
            tokio::spawn(async move {
                tokio::time::sleep(drain).await;
                let mut closing = 0usize;
                for (h, accepted, transport) in &candidates {
                    if now.keeps(accepted, transport.tls()) {
                        continue;
                    }
                    if let Some(h) = h.upgrade() {
                        h.flags().close_at_safe_point(CloseReason::ListenerRetired);
                        closing += 1;
                    }
                }
                tracing::info!(
                    listener = name,
                    sessions = n,
                    closing,
                    drain_s = drain.as_secs(),
                    "listener made stricter: older sessions it no longer allows close at a safe point"
                );
            });
        }
        n
    }

    /// Follows the live settings: after every change to `listener.*` or
    /// `tls.*`, re-reads the `mysql` and `mariadb` listeners' strictness and
    /// calls [`ListenerHooks::listener_changed`] for each that changed,
    /// until `stop` is cancelled.
    #[must_use = "the follower stops when the handle's task is cancelled or `stop` fires"]
    pub fn spawn_follower(
        &self,
        live: LiveSettings,
        dc: DcId,
        stop: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let hooks = self.clone();
        let mut changes = live.subscribe(&["listener.*", "tls.*"]);
        tokio::spawn(async move {
            let mut last = read_strictness(&live, &dc);
            loop {
                tokio::select! {
                    () = stop.cancelled() => return,
                    r = changes.changed() => if r.is_err() { return },
                }
                let now = read_strictness(&live, &dc);
                let drain = {
                    let ctx = SettingCtx::process(&dc);
                    Duration::from_secs(live.get(&DRAIN_SECONDS, &ctx))
                };
                for (name, s) in &now {
                    if last.get(name) != Some(s) {
                        hooks.listener_changed(name, *s, drain);
                    }
                }
                last = now;
            }
        })
    }
}

/// The strictness of the MySQL-family listeners as the settings say now.
/// A listener whose settings don't parse is left out (the manager keeps
/// running the old one).
fn read_strictness(live: &LiveSettings, dc: &DcId) -> HashMap<&'static str, Strictness> {
    let snap = live.snapshot();
    let ctx = SettingCtx::process(dc);
    let min_tls = match snap.get(&TLS_MIN_VERSION, &ctx).as_ref() {
        "1.3" => TlsVersion::V1_3,
        _ => TlsVersion::V1_2,
    };
    let mut out = HashMap::new();
    for keys in LISTENERS
        .iter()
        .filter(|k| !k.protocol.client_speaks_first())
    {
        let Ok(bind) = snap.get(keys.bind, &ctx).parse::<SocketAddr>() else {
            continue;
        };
        let Ok(tls) = snap.get(keys.tls, &ctx).parse::<TlsMode>() else {
            continue;
        };
        out.insert(
            keys.name,
            Strictness {
                enabled: snap.get(keys.enabled, &ctx),
                bind,
                tls,
                min_tls,
            },
        );
    }
    out
}
