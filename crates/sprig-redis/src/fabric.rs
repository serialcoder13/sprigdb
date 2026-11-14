//! Pub/sub and wake-ups across gateways: the hubs.
//!
//! ```text
//! hub(tenant, hub key) = rendezvous over the DC's ready gateways
//!   hub key: "c" for classic channels, patterns and blocked keys;
//!            "s<i>", i = CRC16 slot mod redis.sharded_pubsub_hubs, for sharded channels
//! SUBSCRIBE: register the interest with the hub, then reply (a later PUBLISH is
//!   delivered); every refresh, each gateway re-sends its whole interest set to
//!   the hub of each (tenant, hub key), so a new hub fills within one refresh and
//!   a dead gateway's entries expire after `ttl`
//! PUBLISH: to the hub, which delivers to every gateway with a matching
//!   interest (itself included) and returns the sum of their receivers
//! push to a list or sorted set: wake local waiters at once, then tell the hub,
//!   which wakes gateways with a waiter on the key; a hub that has no waiter of
//!   the tenant says so, and the gateway stops asking until a waiter appears
//!   (the hub tells every gateway when a tenant's first waiter registers)
//! ```
//!
//! A message in flight at a hub that dies is lost (at most once, as in
//! Redis). The transport ([`Transport`]) is the gateway's peer service
//! (`sprig.internal.v1.RedisPeer`); calls to this gateway itself never
//! leave the process, so one gateway alone costs nothing extra.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock, PoisonError, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;

use crate::env::Env;
use crate::glob;

/// One thing a gateway asks the hub to send it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Interest {
    /// `SUBSCRIBE channel`.
    Channel(Bytes),
    /// `PSUBSCRIBE pattern`.
    Pattern(Bytes),
    /// `SSUBSCRIBE channel`.
    Shard(Bytes),
    /// A client blocked on `(database, key)`.
    Wait(u32, Bytes),
}

impl Interest {
    /// The hub key: `c`, or `s<i>` for a sharded channel.
    #[must_use]
    pub fn hub_key(&self, sharded_hubs: u32) -> String {
        match self {
            Self::Shard(c) => shard_hub_key(c, sharded_hubs),
            _ => "c".to_owned(),
        }
    }
}

/// The hub key of a sharded channel: its CRC16 slot over the hubs.
#[must_use]
pub fn shard_hub_key(channel: &[u8], sharded_hubs: u32) -> String {
    let slot = u32::from(sprig_kv::layout::key_slot(channel));
    format!("s{}", slot % sharded_hubs.max(1))
}

/// Rendezvous hashing: the member with the highest hash of `key` and its
/// id. Every gateway with the same member list picks the same one.
#[must_use]
pub fn rendezvous<'a>(key: &str, members: &'a [String]) -> Option<&'a str> {
    members
        .iter()
        .max_by_key(|m| {
            let mut b = Vec::with_capacity(key.len() + m.len() + 1);
            b.extend_from_slice(key.as_bytes());
            b.push(0);
            b.extend_from_slice(m.as_bytes());
            (sprig_kv::layout::routing_hash_of(&b), (*m).clone())
        })
        .map(String::as_str)
}

/// A gateway's interests for one hub: replace (`full`) or change.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InterestMsg {
    /// The tenant.
    pub scope: String,
    /// The hub key.
    pub hub_key: String,
    /// The sending gateway.
    pub from: String,
    /// `add` is the whole set (a refresh).
    pub full: bool,
    /// Added interests.
    pub add: Vec<Interest>,
    /// Removed interests.
    pub remove: Vec<Interest>,
}

/// A message to publish, or to deliver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishMsg {
    /// The tenant.
    pub scope: String,
    /// The channel.
    pub channel: Bytes,
    /// The payload.
    pub data: Bytes,
    /// `SPUBLISH`.
    pub shard: bool,
}

/// Keys pushed to, or a tenant's first waiter.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WakeMsg {
    /// The tenant.
    pub scope: String,
    /// The Redis database.
    pub db: u32,
    /// The keys.
    pub keys: Vec<Bytes>,
    /// To the hub: forward to gateways with waiters. From the hub: wake
    /// local waiters.
    pub forward: bool,
    /// The gateway the push happened on (the hub skips it).
    pub from: String,
    /// From the hub: the tenant has a waiter now; ask again.
    pub appeared: bool,
}

/// `PUBSUB` across gateways.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Introspection {
    /// Channels with their subscriber counts.
    pub channels: BTreeMap<Bytes, i64>,
    /// Distinct patterns.
    pub patterns: BTreeSet<Bytes>,
}

impl Introspection {
    /// Adds another gateway's view.
    pub fn merge(&mut self, o: Self) {
        for (c, n) in o.channels {
            *self.channels.entry(c).or_insert(0) += n;
        }
        self.patterns.extend(o.patterns);
    }
}

/// How gateways reach each other (the peer service in the gateway).
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// This gateway's id.
    fn me(&self) -> String;
    /// The DC's ready gateways that serve the peer service, this one
    /// included, sorted.
    fn members(&self) -> Vec<String>;
    /// Sends interests to the hub `to`.
    async fn interest(&self, to: &str, m: InterestMsg) -> Result<(), String>;
    /// Publishes through the hub `to`; the receivers.
    async fn publish(&self, to: &str, m: PublishMsg) -> Result<i64, String>;
    /// Delivers to gateway `to`'s subscribers; the receivers.
    async fn deliver(&self, to: &str, m: PublishMsg) -> Result<i64, String>;
    /// A wake-up to the hub, or from it; whether the tenant has waiters.
    async fn wake(&self, to: &str, m: WakeMsg) -> Result<bool, String>;
    /// Gateway `to`'s channels and patterns of a tenant.
    async fn introspect(&self, to: &str, scope: &str, shard: bool)
    -> Result<Introspection, String>;
}

/// Timing of the hubs.
#[derive(Debug, Clone, Copy)]
pub struct MeshOpts {
    /// `redis.sharded_pubsub_hubs`.
    pub sharded_hubs: u32,
    /// Every this long, each gateway re-sends its interests.
    pub refresh: Duration,
    /// A gateway's interests expire this long after its last refresh.
    pub ttl: Duration,
    /// How long one peer call may take.
    pub call_timeout: Duration,
    /// A hub's "no waiters" answer is trusted this long.
    pub no_waiters_for: Duration,
}

impl Default for MeshOpts {
    fn default() -> Self {
        Self {
            sharded_hubs: 8,
            refresh: Duration::from_secs(2),
            ttl: Duration::from_secs(10),
            call_timeout: Duration::from_secs(2),
            no_waiters_for: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Default)]
struct GwEntry {
    exact: HashSet<Interest>,
    patterns: BTreeSet<Bytes>,
    seen: Option<Instant>,
}

impl GwEntry {
    fn add(&mut self, i: Interest) {
        match i {
            Interest::Pattern(p) => {
                self.patterns.insert(p);
            }
            other => {
                self.exact.insert(other);
            }
        }
    }

    fn remove(&mut self, i: &Interest) {
        match i {
            Interest::Pattern(p) => {
                self.patterns.remove(p);
            }
            other => {
                self.exact.remove(other);
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.exact.is_empty() && self.patterns.is_empty()
    }

    fn has_wait(&self) -> bool {
        self.exact.iter().any(|i| matches!(i, Interest::Wait(..)))
    }
}

/// What this gateway holds as a hub: per (tenant, hub key), each
/// gateway's interests.
#[derive(Debug, Default)]
pub struct HubTable {
    t: HashMap<(String, String), HashMap<String, GwEntry>>,
}

impl HubTable {
    /// Applies a gateway's interests; `true` when the tenant had no
    /// waiter before and has one now.
    pub fn apply(&mut self, m: &InterestMsg, now: Instant, ttl: Duration) -> bool {
        let key = (m.scope.clone(), m.hub_key.clone());
        let gws = self.t.entry(key.clone()).or_default();
        let had_wait = gws
            .values()
            .any(|e| e.seen.is_some_and(|s| now.duration_since(s) < ttl) && e.has_wait());
        let e = gws.entry(m.from.clone()).or_default();
        if m.full {
            e.exact.clear();
            e.patterns.clear();
        }
        for i in &m.remove {
            e.remove(i);
        }
        for i in &m.add {
            e.add(i.clone());
        }
        e.seen = Some(now);
        let has_wait = e.has_wait();
        if e.is_empty() {
            gws.remove(&m.from);
        }
        if gws.is_empty() {
            self.t.remove(&key);
        }
        !had_wait && has_wait
    }

    fn live(
        &mut self,
        scope: &str,
        hub_key: &str,
        now: Instant,
        ttl: Duration,
    ) -> Option<&HashMap<String, GwEntry>> {
        let key = (scope.to_owned(), hub_key.to_owned());
        let gws = self.t.get_mut(&key)?;
        gws.retain(|_, e| e.seen.is_some_and(|s| now.duration_since(s) < ttl));
        if gws.is_empty() {
            self.t.remove(&key);
            return None;
        }
        self.t.get(&key)
    }

    /// The gateways a message on `channel` goes to.
    pub fn targets(
        &mut self,
        scope: &str,
        hub_key: &str,
        channel: &Bytes,
        shard: bool,
        now: Instant,
        ttl: Duration,
    ) -> Vec<String> {
        let Some(gws) = self.live(scope, hub_key, now, ttl) else {
            return Vec::new();
        };
        let mut out: Vec<String> = gws
            .iter()
            .filter(|(_, e)| {
                if shard {
                    e.exact.contains(&Interest::Shard(channel.clone()))
                } else {
                    e.exact.contains(&Interest::Channel(channel.clone()))
                        || e.patterns.iter().any(|p| glob::matches(p, channel))
                }
            })
            .map(|(g, _)| g.clone())
            .collect();
        out.sort();
        out
    }

    /// The gateways with a waiter on one of `keys`, and whether the tenant
    /// has any waiter.
    pub fn waiters(
        &mut self,
        scope: &str,
        db: u32,
        keys: &[Bytes],
        now: Instant,
        ttl: Duration,
    ) -> (Vec<String>, bool) {
        let Some(gws) = self.live(scope, "c", now, ttl) else {
            return (Vec::new(), false);
        };
        let any = gws.values().any(GwEntry::has_wait);
        let mut out: Vec<String> = gws
            .iter()
            .filter(|(_, e)| {
                keys.iter()
                    .any(|k| e.exact.contains(&Interest::Wait(db, k.clone())))
            })
            .map(|(g, _)| g.clone())
            .collect();
        out.sort();
        (out, any)
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Pub/sub and wake-ups across the DC's gateways, over a [`Transport`].
pub struct Mesh {
    env: OnceLock<Weak<Env>>,
    transport: Arc<dyn Transport>,
    opts: MeshOpts,
    table: Mutex<HubTable>,
    /// This gateway's interests, with how many local subscriptions hold each.
    local: Mutex<HashMap<(String, Interest), usize>>,
    /// Tenants whose hub had no waiter, until when.
    no_waiters: Mutex<HashMap<String, Instant>>,
    /// The hub each (tenant, hub key) was last synced to.
    synced: Mutex<HashMap<(String, String), String>>,
    /// Gateways a call just failed to reach, until when: left out of the
    /// hub choice so a dead hub's tenants move at once, before its
    /// registration expires.
    suspects: Mutex<HashMap<String, Instant>>,
}

impl fmt::Debug for Mesh {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Mesh")
            .field("opts", &self.opts)
            .finish_non_exhaustive()
    }
}

impl Mesh {
    /// A mesh over `transport`.
    #[must_use]
    pub fn new(transport: Arc<dyn Transport>, opts: MeshOpts) -> Arc<Self> {
        Arc::new(Self {
            env: OnceLock::new(),
            transport,
            opts,
            table: Mutex::new(HubTable::default()),
            local: Mutex::new(HashMap::new()),
            no_waiters: Mutex::new(HashMap::new()),
            synced: Mutex::new(HashMap::new()),
            suspects: Mutex::new(HashMap::new()),
        })
    }

    /// Joins the mesh to the gateway's shared Redis state.
    pub fn attach(self: &Arc<Self>, env: &Arc<Env>) {
        let _ = self.env.set(Arc::downgrade(env));
        env.set_mesh(Arc::clone(self));
    }

    fn env(&self) -> Option<Arc<Env>> {
        self.env.get().and_then(Weak::upgrade)
    }

    /// The options.
    #[must_use]
    pub fn opts(&self) -> MeshOpts {
        self.opts
    }

    /// The members, without gateways a call just failed to reach.
    fn members(&self) -> Vec<String> {
        let all = self.transport.members();
        let now = Instant::now();
        let mut sus = lock(&self.suspects);
        sus.retain(|_, until| *until > now);
        let me = self.transport.me();
        all.into_iter()
            .filter(|g| *g == me || !sus.contains_key(g))
            .collect()
    }

    /// Leaves `gateway` out of hub choices for a while.
    fn suspect(&self, gateway: &str) {
        if gateway != self.transport.me() {
            lock(&self.suspects).insert(gateway.to_owned(), Instant::now() + self.opts.ttl);
        }
    }

    fn hub(&self, scope: &str, hub_key: &str) -> String {
        let members = self.members();
        rendezvous(&format!("{scope}/{hub_key}"), &members)
            .map_or_else(|| self.transport.me(), str::to_owned)
    }

    async fn timed<T>(
        &self,
        f: impl std::future::Future<Output = Result<T, String>>,
    ) -> Result<T, String> {
        tokio::time::timeout(self.opts.call_timeout, f)
            .await
            .unwrap_or_else(|_| Err("peer call timed out".into()))
    }

    async fn send_interest(&self, m: InterestMsg) -> Result<(), String> {
        let hub = self.hub(&m.scope, &m.hub_key);
        let me = self.transport.me();
        if hub == me {
            self.on_interest(&m);
            Ok(())
        } else {
            let r = self.timed(self.transport.interest(&hub, m)).await;
            if r.is_err() {
                self.suspect(&hub);
            }
            r
        }
    }

    /// Takes one more local hold of `i`; the first registers it with the
    /// hub before returning.
    ///
    /// # Errors
    /// The hub couldn't be reached (the hold stays; the next refresh sends it).
    pub async fn hold(&self, scope: &str, i: Interest) -> Result<(), String> {
        let first = {
            let mut l = lock(&self.local);
            let n = l.entry((scope.to_owned(), i.clone())).or_insert(0);
            *n += 1;
            *n == 1
        };
        if !first {
            return Ok(());
        }
        let hub_key = i.hub_key(self.opts.sharded_hubs);
        self.send_interest(InterestMsg {
            scope: scope.to_owned(),
            hub_key,
            from: self.transport.me(),
            full: false,
            add: vec![i],
            remove: Vec::new(),
        })
        .await
    }

    /// Drops a local hold of `i`; the last tells the hub, in the background.
    pub fn release(self: &Arc<Self>, scope: &str, i: Interest) {
        let last = {
            let mut l = lock(&self.local);
            let k = (scope.to_owned(), i.clone());
            match l.get_mut(&k) {
                Some(n) if *n > 1 => {
                    *n -= 1;
                    false
                }
                Some(_) => {
                    l.remove(&k);
                    true
                }
                None => false,
            }
        };
        if !last {
            return;
        }
        let me = Arc::clone(self);
        let scope = scope.to_owned();
        tokio::spawn(async move {
            let hub_key = i.hub_key(me.opts.sharded_hubs);
            let _ = me
                .send_interest(InterestMsg {
                    scope,
                    hub_key,
                    from: me.transport.me(),
                    full: false,
                    add: Vec::new(),
                    remove: vec![i],
                })
                .await;
        });
    }

    /// Re-sends every interest to its hub: each `refresh`, or at once when
    /// the hub of a (tenant, hub key) changed.
    pub async fn refresh(&self, force: bool) {
        let mut groups: HashMap<(String, String), Vec<Interest>> = HashMap::new();
        for (scope, i) in lock(&self.local).keys() {
            groups
                .entry((scope.clone(), i.hub_key(self.opts.sharded_hubs)))
                .or_default()
                .push(i.clone());
        }
        let me = self.transport.me();
        let mut seen = HashSet::new();
        for ((scope, hub_key), add) in groups {
            let hub = self.hub(&scope, &hub_key);
            let key = (scope.clone(), hub_key.clone());
            seen.insert(key.clone());
            let changed = lock(&self.synced).get(&key) != Some(&hub);
            if !force && !changed {
                continue;
            }
            let r = self
                .send_interest(InterestMsg {
                    scope,
                    hub_key,
                    from: me.clone(),
                    full: true,
                    add,
                    remove: Vec::new(),
                })
                .await;
            if r.is_ok() {
                lock(&self.synced).insert(key, hub);
            } else {
                lock(&self.synced).remove(&key);
            }
        }
        lock(&self.synced).retain(|k, _| seen.contains(k));
    }

    /// Runs [`Self::refresh`] until `stop`: a forced one every
    /// `opts.refresh`, and a check for changed hubs every quarter of it.
    pub async fn run(self: Arc<Self>, stop: impl std::future::Future<Output = ()> + Send) {
        tokio::pin!(stop);
        let tick = (self.opts.refresh / 4).max(Duration::from_millis(50));
        let mut last_full = Instant::now();
        loop {
            tokio::select! {
                () = &mut stop => return,
                () = tokio::time::sleep(tick) => {}
            }
            let force = last_full.elapsed() >= self.opts.refresh;
            if force {
                last_full = Instant::now();
            }
            self.refresh(force).await;
        }
    }

    /// `PUBLISH` / `SPUBLISH` through the hub.
    ///
    /// # Errors
    /// No hub reachable (39848).
    pub async fn publish(&self, m: PublishMsg) -> Result<i64, String> {
        let hub_key = if m.shard {
            shard_hub_key(&m.channel, self.opts.sharded_hubs)
        } else {
            "c".to_owned()
        };
        let mut last = String::new();
        for attempt in 0..2 {
            let hub = self.hub(&m.scope, &hub_key);
            if hub == self.transport.me() {
                return Ok(self.on_publish(m).await);
            }
            match self.timed(self.transport.publish(&hub, m.clone())).await {
                Ok(n) => return Ok(n),
                Err(e) => {
                    // The next try picks the next hub.
                    self.suspect(&hub);
                    last = e;
                }
            }
            if attempt == 0 {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        Err(last)
    }

    /// Tells the hub about pushes to `keys` (after commit), unless it said
    /// recently that the tenant has no waiter. Doesn't wait.
    pub fn wake(self: &Arc<Self>, scope: &str, db: u32, keys: Vec<Bytes>) {
        if keys.is_empty() {
            return;
        }
        if lock(&self.no_waiters)
            .get(scope)
            .is_some_and(|until| Instant::now() < *until)
        {
            return;
        }
        let me = Arc::clone(self);
        let m = WakeMsg {
            scope: scope.to_owned(),
            db,
            keys,
            forward: true,
            from: self.transport.me(),
            appeared: false,
        };
        tokio::spawn(async move {
            let hub = me.hub(&m.scope, "c");
            let scope = m.scope.clone();
            let any = if hub == me.transport.me() {
                Ok(me.on_wake(m).await)
            } else {
                let r = me.timed(me.transport.wake(&hub, m)).await;
                if r.is_err() {
                    me.suspect(&hub);
                }
                r
            };
            if let Ok(false) = any {
                lock(&me.no_waiters).insert(scope, Instant::now() + me.opts.no_waiters_for);
            }
        });
    }

    /// `PUBSUB CHANNELS`, `NUMSUB`, `NUMPAT` (and the `SHARD` forms) over
    /// every gateway; unreachable gateways are left out.
    pub async fn introspect(&self, scope: &str, shard: bool) -> Introspection {
        let me = self.transport.me();
        let mut out = Introspection::default();
        let calls = self.members().into_iter().map(|g| {
            let me = me.clone();
            async move {
                if g == me {
                    Ok(self.on_introspect(scope, shard))
                } else {
                    self.timed(self.transport.introspect(&g, scope, shard))
                        .await
                }
            }
        });
        for i in futures::future::join_all(calls).await.into_iter().flatten() {
            out.merge(i);
        }
        out
    }

    // ---- the receiving side (the peer service calls these) ----

    /// A gateway's interests arrived at this hub.
    pub fn on_interest(&self, m: &InterestMsg) {
        let appeared = lock(&self.table).apply(m, Instant::now(), self.opts.ttl);
        if appeared && let Ok(rt) = tokio::runtime::Handle::try_current() {
            // The tenant's first waiter: gateways that stopped asking
            // because no one waited start again.
            let transport = Arc::clone(&self.transport);
            let me = transport.me();
            let timeout = self.opts.call_timeout;
            let (scope, from) = (m.scope.clone(), m.from.clone());
            rt.spawn(async move {
                for g in transport.members() {
                    if g == from || g == me {
                        continue;
                    }
                    let msg = WakeMsg {
                        scope: scope.clone(),
                        appeared: true,
                        ..WakeMsg::default()
                    };
                    let _ = tokio::time::timeout(timeout, transport.wake(&g, msg)).await;
                }
            });
            if m.from != self.transport.me() {
                lock(&self.no_waiters).remove(&m.scope);
            }
        }
    }

    /// A publish arrived at this hub: deliver to every interested gateway.
    pub async fn on_publish(&self, m: PublishMsg) -> i64 {
        let hub_key = if m.shard {
            shard_hub_key(&m.channel, self.opts.sharded_hubs)
        } else {
            "c".to_owned()
        };
        let targets = lock(&self.table).targets(
            &m.scope,
            &hub_key,
            &m.channel,
            m.shard,
            Instant::now(),
            self.opts.ttl,
        );
        let me = self.transport.me();
        let calls = targets.into_iter().map(|g| {
            let m = m.clone();
            let me = me.clone();
            async move {
                if g == me {
                    Ok(self.on_deliver(&m))
                } else {
                    self.timed(self.transport.deliver(&g, m)).await
                }
            }
        });
        futures::future::join_all(calls)
            .await
            .into_iter()
            .map(|r| r.unwrap_or(0))
            .sum()
    }

    /// A message from the hub for this gateway's subscribers.
    pub fn on_deliver(&self, m: &PublishMsg) -> i64 {
        self.env()
            .map_or(0, |e| e.hub.publish(&m.scope, &m.channel, &m.data, m.shard))
    }

    /// A wake-up: at the hub (`forward`), passed to gateways with waiters;
    /// at a gateway, its local waiters woken. Returns whether the tenant
    /// has waiters (the hub's answer).
    pub async fn on_wake(&self, m: WakeMsg) -> bool {
        if m.appeared {
            lock(&self.no_waiters).remove(&m.scope);
            return true;
        }
        if !m.forward {
            if let Some(env) = self.env() {
                for k in &m.keys {
                    env.waits.wake(&m.scope, m.db, k);
                }
            }
            return true;
        }
        let (targets, any) =
            lock(&self.table).waiters(&m.scope, m.db, &m.keys, Instant::now(), self.opts.ttl);
        let me = self.transport.me();
        let calls = targets.into_iter().filter(|g| *g != m.from).map(|g| {
            let mut fwd = m.clone();
            fwd.forward = false;
            let me = me.clone();
            async move {
                if g == me {
                    Box::pin(self.on_wake(fwd)).await;
                    Ok(true)
                } else {
                    self.timed(self.transport.wake(&g, fwd)).await
                }
            }
        });
        futures::future::join_all(calls).await;
        any
    }

    /// This gateway's channels and patterns of a tenant.
    #[must_use]
    pub fn on_introspect(&self, scope: &str, shard: bool) -> Introspection {
        self.env()
            .map(|e| e.hub.snapshot(scope, shard))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests;
