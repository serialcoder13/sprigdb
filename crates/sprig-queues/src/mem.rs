//! In-memory [`MqCatalog`] and [`Leases`]: the engine in process, for the
//! protocol tests, the simulation and benchmarks. Several engines (one per
//! simulated gateway) can share one [`MemCatalog`] and one [`MemLeases`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sprig_common::CancellationToken;
use sprig_proto_amqp::auth::Principal;
use sprig_proto_amqp::error::MqError;
use sprig_proto_amqp::mem::Perm;

use crate::engine::{Acquired, Leases, MqCatalog, TempRegistry, TopoChange, VhostRef, lock};
use crate::memstore::MemStore;
use crate::settings::QueueSettings;
use crate::store::MsgStore;
use crate::token::TokenKey;
use crate::topology::{TempSet, Topology};

/// Maps a vhost name to a database name, or refuses it.
pub type VhostMap = Arc<dyn Fn(&Principal, &str) -> Result<String, MqError> + Send + Sync>;
/// Decides permissions.
pub type Authorizer = Arc<dyn Fn(&Principal, Perm, &str) -> bool + Send + Sync>;

struct Db {
    id: u32,
    topo: Arc<Topology>,
    store: Arc<MemStore>,
}

/// The in-memory catalog.
pub struct MemCatalog {
    map: VhostMap,
    authz: Authorizer,
    dbs: Mutex<HashMap<(u64, String), Db>>,
    next_id: Mutex<u64>,
    settings: Mutex<QueueSettings>,
    key: Arc<TokenKey>,
    /// What [`MqCatalog::feed_forwarder_runs`] answers (bugfix-42).
    feed_runs: std::sync::atomic::AtomicBool,
}

impl std::fmt::Debug for MemCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemCatalog").finish_non_exhaustive()
    }
}

impl MemCatalog {
    /// A catalog with a vhost mapping and an authorizer.
    #[must_use]
    pub fn new(map: VhostMap, authz: Authorizer) -> Self {
        Self {
            map,
            authz,
            dbs: Mutex::new(HashMap::new()),
            next_id: Mutex::new(1000),
            settings: Mutex::new(QueueSettings::default()),
            key: Arc::new(TokenKey::new(b"memory-catalog-token-key")),
            feed_runs: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Says whether the feed-driven forwarder runs (bugfix-42).
    pub fn set_feed_forwarder_runs(&self, runs: bool) {
        self.feed_runs
            .store(runs, std::sync::atomic::Ordering::SeqCst);
    }

    /// Changes the settings every vhost runs with.
    pub fn set_settings(&self, s: QueueSettings) {
        *lock(&self.settings) = s;
    }

    /// The store of a database (tests look at the rows).
    #[must_use]
    pub fn store_of(&self, tenant: u64, db: &str) -> Option<Arc<MemStore>> {
        lock(&self.dbs)
            .get(&(tenant, db.to_owned()))
            .map(|d| Arc::clone(&d.store))
    }

    fn next(&self) -> u64 {
        let mut n = lock(&self.next_id);
        *n += 1;
        *n
    }
}

#[async_trait]
impl MqCatalog for MemCatalog {
    async fn open(&self, p: &Principal, vhost: &str) -> Result<VhostRef, MqError> {
        let db = (self.map)(p, vhost)?;
        let mut g = lock(&self.dbs);
        let n = g.len();
        let d = g.entry((p.tenant_id, db.clone())).or_insert_with(|| Db {
            id: u32::try_from(n + 1).unwrap_or(u32::MAX),
            topo: Arc::new(Topology::new()),
            store: Arc::new(MemStore::new()),
        });
        Ok(VhostRef {
            tenant: p.tenant_id,
            db_id: d.id,
            db,
            name: vhost.to_owned(),
        })
    }

    fn topology(&self, v: &VhostRef) -> Arc<Topology> {
        lock(&self.dbs)
            .get(&(v.tenant, v.db.clone()))
            .map(|d| Arc::clone(&d.topo))
            .unwrap_or_default()
    }

    async fn refresh(&self, v: &VhostRef) -> Result<Arc<Topology>, MqError> {
        Ok(self.topology(v))
    }

    async fn change(&self, v: &VhostRef, c: TopoChange) -> Result<Arc<Topology>, MqError> {
        let id = self.next();
        let mut g = lock(&self.dbs);
        let Some(d) = g.get_mut(&(v.tenant, v.db.clone())) else {
            return Err(MqError::not_found(format!("vhost '{}'", v.name)));
        };
        let mut t = (*d.topo).clone();
        t.version += 1;
        match c {
            TopoChange::AddExchange(mut x) => {
                if !t.exchanges.contains_key(&x.name) {
                    x.id = id;
                    t.exchanges.insert(x.name.clone(), Arc::new(x));
                }
            }
            TopoChange::DelExchange(name) => {
                t.exchanges.remove(&name);
                t.bindings.retain(|b| {
                    b.source != name
                        && !(b.dest_kind == sprig_proto_amqp::backend::DestKind::Exchange
                            && b.dest == name)
                });
            }
            TopoChange::AddQueue(mut q) => {
                if !t.queues.contains_key(&q.name) {
                    q.id = id;
                    t.queues.insert(q.name.clone(), Arc::new(q));
                }
            }
            TopoChange::DelQueue(name) => {
                t.queues.remove(&name);
                t.bindings.retain(|b| {
                    !(b.dest_kind == sprig_proto_amqp::backend::DestKind::Queue && b.dest == name)
                });
            }
            TopoChange::Bind(b) => {
                if !t.bindings.iter().any(|x| x.same(&b)) {
                    t.bindings.push(b);
                }
            }
            TopoChange::Unbind(b) => t.bindings.retain(|x| !x.same(&b)),
        }
        d.topo = Arc::new(t);
        Ok(Arc::clone(&d.topo))
    }

    async fn store(&self, v: &VhostRef) -> Result<Arc<dyn MsgStore>, MqError> {
        lock(&self.dbs)
            .get(&(v.tenant, v.db.clone()))
            .map(|d| Arc::clone(&d.store) as Arc<dyn MsgStore>)
            .ok_or_else(|| MqError::not_found(format!("vhost '{}'", v.name)))
    }

    fn token_key(&self, _tenant: u64) -> Option<Arc<TokenKey>> {
        Some(Arc::clone(&self.key))
    }

    fn allowed(&self, p: &Principal, _v: &VhostRef, perm: Perm, _kind: &str, name: &str) -> bool {
        (self.authz)(p, perm, name)
    }

    fn settings(&self, _v: &VhostRef) -> QueueSettings {
        lock(&self.settings).clone()
    }

    async fn feed_forwarder_runs(&self, _v: &VhostRef) -> bool {
        self.feed_runs.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[derive(Default)]
struct LeaseTable {
    holders: HashMap<(u64, u64), (String, CancellationToken)>,
}

/// Leases in memory, shared by simulated gateways.
#[derive(Clone, Default)]
pub struct MemLeases {
    table: Arc<Mutex<LeaseTable>>,
}

impl std::fmt::Debug for MemLeases {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemLeases").finish_non_exhaustive()
    }
}

impl MemLeases {
    /// An empty table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// One gateway's view.
    #[must_use]
    pub fn for_gateway(&self, me: &str) -> GatewayLeases {
        GatewayLeases {
            table: Arc::clone(&self.table),
            me: me.to_owned(),
        }
    }

    /// Expires every lease of `gateway` (a lost etcd session).
    pub fn expire(&self, gateway: &str) {
        let mut t = lock(&self.table);
        t.holders.retain(|_, (g, tok)| {
            if g == gateway {
                tok.cancel();
                false
            } else {
                true
            }
        });
    }

    /// Expires every lease of `gateway` without telling it (it hasn't
    /// noticed yet: a stale owner).
    pub fn expire_silently(&self, gateway: &str) {
        lock(&self.table).holders.retain(|_, (g, _)| g != gateway);
    }

    /// Who holds a lease.
    #[must_use]
    pub fn holder(&self, tenant: u64, object: u64) -> Option<String> {
        lock(&self.table)
            .holders
            .get(&(tenant, object))
            .map(|(g, _)| g.clone())
    }
}

/// [`MemLeases`] as one gateway sees them.
#[derive(Clone)]
pub struct GatewayLeases {
    table: Arc<Mutex<LeaseTable>>,
    me: String,
}

impl std::fmt::Debug for GatewayLeases {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayLeases")
            .field("me", &self.me)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Leases for GatewayLeases {
    fn me(&self) -> &str {
        &self.me
    }

    async fn acquire(&self, tenant: u64, object: u64) -> Result<Acquired, MqError> {
        let mut t = lock(&self.table);
        match t.holders.get(&(tenant, object)) {
            Some((g, tok)) if *g == self.me => Ok(Acquired::Mine(tok.clone())),
            Some((g, _)) => Ok(Acquired::Held(g.clone())),
            None => {
                let tok = CancellationToken::new();
                t.holders
                    .insert((tenant, object), (self.me.clone(), tok.clone()));
                Ok(Acquired::Mine(tok))
            }
        }
    }

    async fn holder(&self, tenant: u64, object: u64) -> Result<Option<String>, MqError> {
        Ok(lock(&self.table)
            .holders
            .get(&(tenant, object))
            .map(|(g, _)| g.clone()))
    }

    async fn release(&self, tenant: u64, object: u64) {
        let mut t = lock(&self.table);
        if t.holders
            .get(&(tenant, object))
            .is_some_and(|(g, _)| *g == self.me)
        {
            t.holders.remove(&(tenant, object));
        }
    }
}

/// Temporary-queue registries of simulated gateways (one process).
#[derive(Clone, Default)]
pub struct MemTemps {
    sets: Arc<Mutex<HashMap<(u64, u32, String), TempSet>>>,
}

impl std::fmt::Debug for MemTemps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemTemps").finish_non_exhaustive()
    }
}

impl MemTemps {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// One gateway's view.
    #[must_use]
    pub fn for_gateway(&self, me: &str) -> GatewayTemps {
        GatewayTemps {
            sets: Arc::clone(&self.sets),
            me: me.to_owned(),
        }
    }

    /// A gateway is gone: its entries go (its etcd lease expired).
    pub fn drop_gateway(&self, gateway: &str) {
        lock(&self.sets).retain(|(_, _, g), _| g != gateway);
    }
}

/// [`MemTemps`] as one gateway sees it.
#[derive(Clone)]
pub struct GatewayTemps {
    sets: Arc<Mutex<HashMap<(u64, u32, String), TempSet>>>,
    me: String,
}

impl std::fmt::Debug for GatewayTemps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayTemps")
            .field("me", &self.me)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TempRegistry for GatewayTemps {
    async fn publish(&self, v: &VhostRef, set: &TempSet) {
        lock(&self.sets).insert((v.tenant, v.db_id, self.me.clone()), set.clone());
    }

    fn others(&self, v: &VhostRef) -> Option<Arc<TempSet>> {
        let g = lock(&self.sets);
        let mut out = TempSet::default();
        for ((t, d, gw), s) in g.iter() {
            if *t == v.tenant && *d == v.db_id && *gw != self.me {
                out.queues
                    .extend(s.queues.iter().map(|(k, q)| (k.clone(), Arc::clone(q))));
                out.bindings.extend(s.bindings.iter().cloned());
            }
        }
        Some(Arc::new(out))
    }
}
