//! The listener manager (chapter 04, "Listeners", "Accept and protocol
//! detection" and "Live listener changes"; build step 2).
//!
//! One listener per protocol, each with its own bind address, TLS mode and
//! certificate bundle, resolved from `listener.<name>.*` ([`resolve`]).
//! [`ListenerManager::apply`] makes the running listeners match a new set of
//! specs without a restart:
//!
//! - a changed spec bumps the listener's generation; new connections get the
//!   new spec, open ones keep the snapshot they were accepted with;
//! - a new bind address is bound first, and only then is the old socket
//!   closed; if the bind fails the old listener keeps running;
//! - a stricter change (TLS tightened, minimum version raised, listener
//!   moved or disabled) closes, once `listener.drain_seconds` has passed,
//!   the older generations' connections that the new spec no longer allows
//!   ([`Strictness::keeps`]): a plaintext connection when TLS becomes
//!   `required`, a TLS 1.2 one when the minimum rises to 1.3, every one
//!   when the listener moves or is disabled. A TLS 1.3 connection stays
//!   open through both TLS changes, and a looser change closes nothing. The
//!   close is graceful first (`close_soon`, a safe point) and forced at
//!   twice the drain time for front ends that have safe points.
//!
//! Each connection runs in its own task: the PROXY header from trusted
//! peers, then TLS by mode. Client-speaks-first protocols detect a TLS
//! `ClientHello` from the first byte; the MySQL family is told what to offer
//! in-protocol ([`InProtocolTls`](crate::frontend::InProtocolTls)).
//!
//! [`live::follow`] re-applies the listeners whenever their settings change
//! in etcd (chapter 23, "Live reconfiguration: no restarts").

mod conn;
pub mod live;
pub mod proxy;
pub mod settings;

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use sprig_common::CancellationToken;
use tokio::net::{TcpListener, TcpSocket};
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use tokio_util::task::TaskTracker;

pub use self::proxy::{Cidr, ProxyMode, ProxyPolicy};
pub use self::settings::{LISTENERS, ListenerKeys, Resolution, resolve};
use crate::frontend::{ConnTransport, Frontend, GatewayContext, Protocol, TlsInfo};
use crate::tls::{self, CertBundle};

/// A listener's TLS mode, ordered from loosest to strictest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TlsMode {
    /// Plaintext only.
    Off,
    /// TLS if the client asks for it.
    Optional,
    /// TLS or nothing.
    Required,
}

impl TlsMode {
    /// The setting value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            TlsMode::Off => "off",
            TlsMode::Optional => "optional",
            TlsMode::Required => "required",
        }
    }
}

impl FromStr for TlsMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "off" => Ok(TlsMode::Off),
            "optional" => Ok(TlsMode::Optional),
            "required" => Ok(TlsMode::Required),
            _ => Err(format!("TLS mode {s:?} is not required, optional or off")),
        }
    }
}

/// The lowest TLS version a listener accepts (`tls.min_version`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TlsVersion {
    /// TLS 1.2 and 1.3.
    V1_2,
    /// TLS 1.3 only.
    V1_3,
}

/// One listener, resolved from `listener.<name>.*` and the gateway keys it
/// depends on.
#[derive(Debug, Clone)]
pub struct ListenerSpec {
    /// `mysql`, `mariadb`, `http`, `https`, `mongo`, `redis`, `bolt`, `kafka`.
    pub name: &'static str,
    /// The protocol, which picks the front end.
    pub protocol: Protocol,
    /// Address and port.
    pub bind: SocketAddr,
    /// Serve this listener.
    pub enabled: bool,
    /// TLS mode.
    pub tls: TlsMode,
    /// The certificate bundle; needed unless `tls` is `off`.
    pub cert: Option<Arc<CertBundle>>,
    /// Lowest TLS version.
    pub min_tls: TlsVersion,
    /// PROXY protocol policy.
    pub proxy: ProxyPolicy,
    /// Longest pre-front-end phase: PROXY header, first byte, TLS handshake.
    pub handshake_timeout: Duration,
}

impl ListenerSpec {
    /// A plaintext, enabled spec with no PROXY handling and a 10 s
    /// handshake timeout; set the other fields as needed.
    #[must_use]
    pub fn new(name: &'static str, protocol: Protocol, bind: SocketAddr) -> Self {
        Self {
            name,
            protocol,
            bind,
            enabled: true,
            tls: TlsMode::Off,
            cert: None,
            min_tls: TlsVersion::V1_2,
            proxy: ProxyPolicy::OFF,
            handshake_timeout: Duration::from_secs(10),
        }
    }

    fn same_as(&self, o: &Self) -> bool {
        let same_cert = match (&self.cert, &o.cert) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        };
        same_cert
            && self.name == o.name
            && self.protocol == o.protocol
            && self.bind == o.bind
            && self.enabled == o.enabled
            && self.tls == o.tls
            && self.min_tls == o.min_tls
            && self.proxy == o.proxy
            && self.handshake_timeout == o.handshake_timeout
    }

    /// Whether moving from `old` to `self` is stricter: connections accepted
    /// under `old` that `self` no longer allows ([`Strictness::keeps`])
    /// must then close after `listener.drain_seconds`.
    fn stricter_than(&self, old: &Self) -> bool {
        Strictness::of(self).stricter_than(&Strictness::of(old))
    }

    /// Plaintext is accepted on an address outside private ranges.
    fn plaintext_public(&self) -> bool {
        self.enabled && self.tls != TlsMode::Required && !is_private(self.bind.ip())
    }
}

/// The parts of a listener spec that decide whether a change is stricter,
/// and which open connections it closes.
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

    /// Whether moving from `old` to `self` is stricter: TLS tightened,
    /// minimum version raised, listener moved or disabled. Only then can
    /// a connection accepted under `old` lose its place
    /// ([`Strictness::keeps`]).
    #[must_use]
    pub fn stricter_than(&self, old: &Self) -> bool {
        !self.enabled || self.bind != old.bind || self.tls > old.tls || self.min_tls > old.min_tls
    }

    /// Whether a connection accepted under `accepted` that runs over `tls`
    /// (`None`: plaintext) may stay open under `self`: the listener is
    /// still enabled at the same address and the connection already meets
    /// its TLS mode and minimum version. A TLS connection meets `required`;
    /// a plaintext one meets `optional` and `off` whatever the minimum
    /// version. A connection that meets the new spec gains nothing from
    /// being closed, and closing it costs its client a reconnect and, on
    /// HTTP/2, a `GOAWAY` race (chapter 04, "Live listener changes").
    #[must_use]
    pub fn keeps(&self, accepted: &Self, tls: Option<TlsVersion>) -> bool {
        self.enabled
            && self.bind == accepted.bind
            && match tls {
                None => self.tls != TlsMode::Required,
                Some(v) => v >= self.min_tls,
            }
    }
}

/// RFC 1918, loopback, link-local or ULA. The unspecified address binds
/// every interface, so it is not private.
fn is_private(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(a) => a.is_private() || a.is_loopback() || a.is_link_local(),
        IpAddr::V6(a) => {
            if let Some(v4) = a.to_ipv4_mapped() {
                return is_private(v4.into());
            }
            let first = a.segments()[0];
            a.is_loopback() || first & 0xfe00 == 0xfc00 || first & 0xffc0 == 0xfe80
        }
    }
}

/// A listener whose new spec was not applied; the running one is kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListenerRejected {
    /// The listener.
    pub name: &'static str,
    /// Why.
    pub reason: String,
}

/// What [`ListenerManager::apply`] did.
#[derive(Debug, Default)]
pub struct ApplyReport {
    /// Listeners whose new spec took effect.
    pub applied: Vec<&'static str>,
    /// Listeners left as they were, and why.
    pub rejected: Vec<ListenerRejected>,
}

/// The front ends by protocol.
#[derive(Default)]
pub struct Frontends(HashMap<Protocol, Arc<dyn Frontend>>);

impl fmt::Debug for Frontends {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.0.keys()).finish()
    }
}

impl Frontends {
    /// No front ends.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `fe` for its protocol, replacing any earlier one.
    #[must_use]
    pub fn with(mut self, fe: Arc<dyn Frontend>) -> Self {
        self.0.insert(fe.protocol(), fe);
        self
    }
}

/// One open connection of a [`ConnGroup`]: its transport and its own
/// close tokens (children of the group's).
struct ConnEntry {
    transport: Arc<ConnTransport>,
    soft: CancellationToken,
    close: CancellationToken,
}

/// The connections accepted under one generation of a listener.
#[derive(Clone)]
struct ConnGroup {
    /// The spec they were accepted under.
    spec: Arc<ListenerSpec>,
    tracker: TaskTracker,
    /// Cancelled to close every connection of the group (shutdown).
    close: CancellationToken,
    /// Cancelled to ask every connection of the group to close at its next
    /// safe point ([`ClientConn::close_soon`](crate::frontend::ClientConn)).
    soft: CancellationToken,
    /// The open connections, so a stricter change closes only those the new
    /// spec doesn't allow.
    conns: Arc<Mutex<HashMap<u64, ConnEntry>>>,
    next_id: Arc<AtomicU64>,
}

/// A connection's place in its group; dropping it removes the entry.
pub(crate) struct ConnSlot {
    conns: Arc<Mutex<HashMap<u64, ConnEntry>>>,
    id: u64,
    pub(crate) transport: Arc<ConnTransport>,
    pub(crate) soft: CancellationToken,
    pub(crate) close: CancellationToken,
}

impl Drop for ConnSlot {
    fn drop(&mut self) {
        lock(&self.conns).remove(&self.id);
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl ConnGroup {
    fn new(spec: Arc<ListenerSpec>) -> Self {
        Self {
            spec,
            tracker: TaskTracker::new(),
            close: CancellationToken::new(),
            soft: CancellationToken::new(),
            conns: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(AtomicU64::new(0)),
        }
    }

    fn finished(&self) -> bool {
        self.tracker.is_closed() && self.tracker.is_empty()
    }

    /// Registers a new connection of the group.
    fn join(&self) -> ConnSlot {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let transport = Arc::new(ConnTransport::default());
        let soft = self.soft.child_token();
        let close = self.close.child_token();
        lock(&self.conns).insert(
            id,
            ConnEntry {
                transport: Arc::clone(&transport),
                soft: soft.clone(),
                close: close.clone(),
            },
        );
        ConnSlot {
            conns: Arc::clone(&self.conns),
            id,
            transport,
            soft,
            close,
        }
    }

    /// The close tokens (soft, forced) of the open connections that `now`
    /// no longer allows.
    fn not_kept(&self, now: &Strictness) -> Vec<(CancellationToken, CancellationToken)> {
        let accepted = Strictness::of(&self.spec);
        lock(&self.conns)
            .values()
            .filter(|e| !now.keeps(&accepted, e.transport.tls()))
            .map(|e| (e.soft.clone(), e.close.clone()))
            .collect()
    }
}

/// What a listener's new connections get.
struct Active {
    spec: Arc<ListenerSpec>,
    generation: u64,
    acceptor: Option<TlsAcceptor>,
    group: ConnGroup,
}

/// A bound socket and its accept loop.
struct Bound {
    requested: SocketAddr,
    local: SocketAddr,
    stop: CancellationToken,
    task: JoinHandle<()>,
}

impl Bound {
    /// Stops accepting and waits until the socket is closed.
    async fn close(self) {
        self.stop.cancel();
        let _ = self.task.await;
    }
}

#[derive(Default)]
struct Slot {
    active: Arc<ArcSwapOption<Active>>,
    socket: Option<Bound>,
    generation: u64,
    /// Older generations with connections still open.
    old: Vec<ConnGroup>,
}

struct Shared {
    frontends: Frontends,
    ctx: Arc<GatewayContext>,
}

/// Runs the client listeners and applies changes to them live.
pub struct ListenerManager {
    shared: Arc<Shared>,
    slots: tokio::sync::Mutex<HashMap<&'static str, Slot>>,
    binds: HashMap<String, SocketAddr>,
}

impl fmt::Debug for ListenerManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ListenerManager")
            .field("frontends", &self.shared.frontends)
            .finish_non_exhaustive()
    }
}

impl ListenerManager {
    /// A manager with no listeners yet. A listener whose protocol has no
    /// front end in `frontends` is rejected when applied.
    #[must_use]
    pub fn new(frontends: Frontends, ctx: Arc<GatewayContext>) -> Self {
        Self {
            shared: Arc::new(Shared { frontends, ctx }),
            slots: tokio::sync::Mutex::new(HashMap::new()),
            binds: HashMap::new(),
        }
    }

    /// The same manager with this process's own addresses for some
    /// listeners (`[gateway] listen_override`): they replace the
    /// `listener.<name>.bind` setting, so a second gateway can run in the
    /// same DC (development and tests; production gateways share the DC's
    /// settings).
    #[must_use]
    pub fn with_bind_overrides(mut self, binds: HashMap<String, SocketAddr>) -> Self {
        self.binds = binds;
        self
    }

    /// Makes the listeners match `res`: listeners whose spec changed are
    /// re-bound or updated in place; listeners not in `res.specs` are left
    /// as they are.
    pub async fn apply(&self, res: Resolution) -> ApplyReport {
        let mut report = ApplyReport {
            applied: Vec::new(),
            rejected: res.rejected,
        };
        let mut slots = self.slots.lock().await;
        for mut spec in res.specs {
            let name = spec.name;
            if let Some(a) = self.binds.get(name) {
                spec.bind = *a;
            }
            let slot = slots.entry(name).or_default();
            match self.apply_one(slot, spec, res.drain).await {
                Ok(true) => report.applied.push(name),
                Ok(false) => {}
                Err(reason) => report.rejected.push(ListenerRejected { name, reason }),
            }
        }
        for r in &report.rejected {
            tracing::warn!(event = "gateway.listener_rejected", listener = r.name, reason = %r.reason, "listener change not applied; the running listener is kept");
        }
        report
    }

    /// Applies one spec. `Ok(false)`: nothing changed.
    async fn apply_one(
        &self,
        slot: &mut Slot,
        spec: ListenerSpec,
        drain: Duration,
    ) -> Result<bool, String> {
        let old = slot.active.load_full();
        if old.as_ref().is_some_and(|o| o.spec.same_as(&spec)) || (old.is_none() && !spec.enabled) {
            return Ok(false);
        }
        let name = spec.name;
        if !spec.enabled {
            if let Some(b) = slot.socket.take() {
                b.close().await;
            }
            slot.active.store(None);
            slot.generation += 1;
            if let Some(o) = old {
                retire(slot, name, o.group.clone(), &spec, drain);
            }
            publish(slot.generation, &spec);
            tracing::info!(
                event = "gateway.listener_applied",
                listener = name,
                generation = slot.generation,
                "listener disabled"
            );
            return Ok(true);
        }

        if !self.shared.frontends.0.contains_key(&spec.protocol) {
            return Err(format!("no {} front end in this build yet", spec.protocol));
        }
        let acceptor = match (&spec.cert, spec.tls) {
            (_, TlsMode::Off) => None,
            (None, _) => return Err("TLS mode needs a certificate bundle".into()),
            (Some(cert), _) => {
                let mut cfg =
                    tls::server_config(cert.clone(), spec.min_tls).map_err(|e| e.message)?;
                if spec.protocol == Protocol::Http {
                    // ALPN: h2 first, then HTTP/1.1 (chapter 37).
                    let c = Arc::make_mut(&mut cfg);
                    c.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
                }
                Some(TlsAcceptor::from(cfg))
            }
        };
        let listener = match &slot.socket {
            Some(b) if b.requested == spec.bind => None,
            current => {
                let same_port = current
                    .as_ref()
                    .is_some_and(|b| b.local.port() == spec.bind.port());
                let l = bind(spec.bind, same_port)
                    .and_then(|l| Ok((l.local_addr()?, l)))
                    .map_err(|e| format!("can't bind {}: {e}", spec.bind))?;
                Some(l)
            }
        };

        slot.generation += 1;
        let stricter = old.as_ref().is_some_and(|o| spec.stricter_than(&o.spec));
        let spec = Arc::new(spec);
        slot.active.store(Some(Arc::new(Active {
            spec: spec.clone(),
            generation: slot.generation,
            acceptor,
            group: ConnGroup::new(spec.clone()),
        })));
        if let Some((local, listener)) = listener {
            let bound = self.start_accepting(listener, spec.bind, local, slot.active.clone());
            if let Some(b) = slot.socket.replace(bound) {
                b.close().await;
            }
        }
        if let Some(o) = old {
            retire(slot, name, o.group.clone(), &spec, drain);
        }
        publish(slot.generation, &spec);
        let local = slot.socket.as_ref().map(|b| b.local);
        tracing::info!(
            event = "gateway.listener_applied",
            listener = name,
            generation = slot.generation,
            bind = ?local,
            tls = spec.tls.as_str(),
            stricter,
            "listener applied"
        );
        Ok(true)
    }

    fn start_accepting(
        &self,
        listener: TcpListener,
        requested: SocketAddr,
        local: SocketAddr,
        active: Arc<ArcSwapOption<Active>>,
    ) -> Bound {
        let stop = CancellationToken::new();
        let task = tokio::spawn(accept_loop(
            listener,
            active,
            self.shared.clone(),
            stop.clone(),
        ));
        Bound {
            requested,
            local,
            stop,
            task,
        }
    }

    /// The address listener `name` is bound to, if it is running.
    pub async fn local_addr(&self, name: &str) -> Option<SocketAddr> {
        let slots = self.slots.lock().await;
        slots.get(name)?.socket.as_ref().map(|b| b.local)
    }

    /// The current generation of listener `name` (0 if never applied).
    pub async fn generation(&self, name: &str) -> u64 {
        let slots = self.slots.lock().await;
        slots.get(name).map_or(0, |s| s.generation)
    }

    /// Closes every listener, then waits up to `timeout` for open
    /// connections to end before closing the rest.
    pub async fn shutdown(&self, timeout: Duration) {
        let mut groups = Vec::new();
        {
            let mut slots = self.slots.lock().await;
            for slot in slots.values_mut() {
                if let Some(b) = slot.socket.take() {
                    b.close().await;
                }
                if let Some(a) = slot.active.swap(None) {
                    a.group.tracker.close();
                    groups.push(a.group.clone());
                }
                groups.append(&mut slot.old);
            }
        }
        // Every connection is asked to close at its next safe point now;
        // the ones still open at `timeout` are closed by force.
        for g in &groups {
            g.soft.cancel();
        }
        let all_done = async {
            for g in &groups {
                g.tracker.wait().await;
            }
        };
        if tokio::time::timeout(timeout, all_done).await.is_err() {
            tracing::info!("closing the connections still open after the drain timeout");
            for g in &groups {
                g.close.cancel();
            }
            for g in &groups {
                g.tracker.wait().await;
            }
        }
    }
}

/// Stops `group` taking new connections and keeps it with the older
/// generations. In every older generation that `new` is stricter than, the
/// connections `new` no longer allows ([`Strictness::keeps`], checked when
/// `drain` has passed, so a MySQL session's in-protocol TLS counts) are
/// asked to close at their next safe point then, and closed by force at
/// twice `drain` (chapter 04: "for sessions accepted under the older
/// generation of this name"). The others stay open.
fn retire(
    slot: &mut Slot,
    name: &'static str,
    group: ConnGroup,
    new: &ListenerSpec,
    drain: Duration,
) {
    group.tracker.close();
    slot.old.retain(|g| !g.finished());
    slot.old.push(group);
    let now = Strictness::of(new);
    for g in &slot.old {
        if !new.stricter_than(&g.spec) {
            continue;
        }
        let g = g.clone();
        // Front ends that close at a safe point do so at `drain`; the forced
        // close waits another `drain` for sessions still mid-transaction.
        let force_after = if g.spec.protocol.closes_at_safe_points() {
            drain.saturating_mul(2)
        } else {
            drain
        };
        tokio::spawn(async move {
            let soft_after = drain.min(force_after);
            tokio::select! {
                () = g.tracker.wait() => return,
                () = tokio::time::sleep(soft_after) => {}
            }
            let doomed = g.not_kept(&now);
            tracing::info!(
                listener = name,
                open = g.tracker.len(),
                closing = doomed.len(),
                "listener made stricter: closing the older connections it no longer allows"
            );
            if doomed.is_empty() {
                return;
            }
            for (soft, _) in &doomed {
                soft.cancel();
            }
            tokio::select! {
                () = g.tracker.wait() => {}
                () = tokio::time::sleep(force_after.saturating_sub(soft_after)) => {
                    tracing::info!(listener = name, "drain time over: closing by force the connections of an older listener generation still open");
                    for (_, close) in &doomed {
                        close.cancel();
                    }
                }
            }
        });
    }
}

fn publish(generation: u64, spec: &ListenerSpec) {
    #[allow(clippy::cast_precision_loss)] // generations stay far below 2^53
    metrics::gauge!("sprig_gateway_listener_generation", "listener" => spec.name)
        .set(generation as f64);
    let public = spec.plaintext_public();
    metrics::gauge!("sprig_gateway_listener_plaintext_public", "listener" => spec.name)
        .set(if public { 1.0 } else { 0.0 });
    if public {
        tracing::warn!(event = "gateway.plaintext_public", listener = spec.name, bind = %spec.bind, tls = spec.tls.as_str(), "listener accepts plaintext on a non-private address");
    }
}

/// Binds a listening socket with `SO_REUSEADDR`, and `SO_REUSEPORT` when it
/// replaces a socket on the same port.
fn bind(addr: SocketAddr, reuse_port: bool) -> io::Result<TcpListener> {
    let sock = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    sock.set_reuseaddr(true)?;
    #[cfg(unix)]
    if reuse_port {
        sock.set_reuseport(true)?;
    }
    #[cfg(not(unix))]
    let _ = reuse_port;
    sock.bind(addr)?;
    sock.listen(1024)
}

/// Accepts until `stop` is cancelled, then drops (closes) the socket.
async fn accept_loop(
    listener: TcpListener,
    active: Arc<ArcSwapOption<Active>>,
    shared: Arc<Shared>,
    stop: CancellationToken,
) {
    loop {
        let accepted = tokio::select! {
            () = stop.cancelled() => return,
            r = listener.accept() => r,
        };
        match accepted {
            Ok((sock, peer)) => {
                // A retired listener: drop the socket.
                let Some(a) = active.load_full() else {
                    continue;
                };
                let Some(fe) = shared.frontends.0.get(&a.spec.protocol).cloned() else {
                    continue;
                };
                // Chapter 25: a returning DC admits connections in steps.
                if let Some(f) = crate::failover::get()
                    && let Err(wait) = f.admit(a.spec.protocol.as_str())
                {
                    metrics::counter!("sprig_gateway_connections_rejected_total", "listener" => a.spec.name, "reason" => "admission").increment(1);
                    tokio::spawn(conn::refuse_admission(
                        sock,
                        a.spec.protocol,
                        a.spec.tls,
                        wait,
                    ));
                    continue;
                }
                // gateway.max_connections (chapter 04 step 7).
                let mut permit = None;
                if let Some(l) = &shared.ctx.limits {
                    let Some(p) = l.try_connection() else {
                        metrics::counter!("sprig_gateway_connections_rejected_total", "listener" => a.spec.name, "reason" => "max_connections").increment(1);
                        tokio::spawn(conn::refuse_too_many(sock, a.spec.protocol, a.spec.tls));
                        continue;
                    };
                    permit = Some(p);
                }
                let tracker = a.group.tracker.clone();
                tracker.spawn(conn::run(sock, peer, a, fe, shared.ctx.clone(), permit));
            }
            Err(e) => {
                // EMFILE and friends: count, back off briefly (chapter 04).
                let name = active.load().as_ref().map_or("unknown", |a| a.spec.name);
                metrics::counter!("sprig_gateway_connections_rejected_total", "listener" => name, "reason" => "accept_error").increment(1);
                tracing::warn!(listener = name, error = %e, "accept failed");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// Counts a TLS handshake in `sprig_gateway_tls_handshakes_total`.
pub(crate) fn count_handshake(
    listener: &'static str,
    info: Option<&TlsInfo>,
    outcome: &'static str,
) {
    let version = info.map_or("none", |i| i.version);
    metrics::counter!("sprig_gateway_tls_handshakes_total", "listener" => listener, "version" => version, "outcome" => outcome).increment(1);
}

#[cfg(test)]
mod tests;
