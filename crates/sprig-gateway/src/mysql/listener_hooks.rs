//! Live listener changes for the MySQL family (chapter 05, "Live listener
//! changes"; build step 12).
//!
//! - **Per greeting:** mode, certificate and `tls.min_version` come from
//!   the listener snapshot the connection was accepted under
//!   (`ClientConn::listener` and its acceptor), and the version strings are
//!   read from the live settings when the greeting is built, so every
//!   change reaches new connections only.
//! - **Stricter listeners:** when `listener.<name>.tls` tightens,
//!   `tls.min_version` rises, or the listener moves or is disabled,
//!   sessions accepted under the looser spec are marked
//!   `close_at_safe_point` (reason `ListenerRetired`) once
//!   `listener.drain_seconds` has passed (chapter 04). The front end then
//!   answers their next command with 39004 and closes; a session inside a
//!   transaction closes after it ends.
//!
//! [`ListenerHooks`] keeps the spec each open MySQL session was accepted
//! under, so the comparison is exact whatever generations came between.
//! The listener manager's own hard close of an older generation still
//! fires at the same deadline (chapter 04 build step 8 moves it later).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use sprig_common::CancellationToken;
use sprig_common::ids::{ConnId, DcId};
use sprig_settings::{LiveSettings, SettingCtx};

use crate::listener::settings::{DRAIN_SECONDS, LISTENERS, TLS_MIN_VERSION};
use crate::listener::{ListenerSpec, TlsMode, TlsVersion};
use crate::session::{CloseReason, SessionHandle};

/// The parts of a listener spec that decide whether a change is stricter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Strictness {
    /// `listener.<name>.enabled`.
    pub enabled: bool,
    /// `listener.<name>.bind`.
    pub bind: SocketAddr,
    /// `listener.<name>.tls`.
    pub tls: TlsMode,
    /// `tls.min_version`.
    pub min_tls: TlsVersion,
}

impl Strictness {
    /// The strictness of `spec`.
    #[must_use]
    pub fn of(spec: &ListenerSpec) -> Self {
        Self {
            enabled: spec.enabled,
            bind: spec.bind,
            tls: spec.tls,
            min_tls: spec.min_tls,
        }
    }

    /// Whether moving from `old` to `self` must retire `old`'s sessions
    /// (the same rule as the listener manager's).
    #[must_use]
    pub fn stricter_than(&self, old: &Self) -> bool {
        !self.enabled || self.bind != old.bind || self.tls > old.tls || self.min_tls > old.min_tls
    }
}

struct Entry {
    handle: Weak<SessionHandle>,
    listener: &'static str,
    accepted: Strictness,
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

    /// Records that `handle` was accepted under `spec`, until the guard
    /// drops.
    #[must_use]
    pub fn register(&self, handle: &Arc<SessionHandle>, spec: &ListenerSpec) -> Registered {
        lock(&self.inner.sessions).insert(
            handle.id(),
            Entry {
                handle: Arc::downgrade(handle),
                listener: spec.name,
                accepted: Strictness::of(spec),
            },
        );
        Registered {
            hooks: self.clone(),
            id: handle.id(),
        }
    }

    /// Listener `name` is now `now`: sessions it is stricter for are marked
    /// to close at their next safe point after `drain`. Returns how many
    /// were scheduled.
    pub fn listener_changed(&self, name: &'static str, now: Strictness, drain: Duration) -> usize {
        let doomed: Vec<Weak<SessionHandle>> = lock(&self.inner.sessions)
            .values()
            .filter(|e| e.listener == name && now.stricter_than(&e.accepted))
            .map(|e| e.handle.clone())
            .collect();
        let n = doomed.len();
        if n > 0 {
            tracing::info!(
                listener = name,
                sessions = n,
                drain_s = drain.as_secs(),
                "listener made stricter: older sessions close at a safe point after the drain time"
            );
            tokio::spawn(async move {
                tokio::time::sleep(drain).await;
                for h in doomed.iter().filter_map(Weak::upgrade) {
                    h.flags().close_at_safe_point(CloseReason::ListenerRetired);
                }
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
