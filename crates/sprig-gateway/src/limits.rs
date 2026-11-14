//! Limits and admission.
//!
//! Per gateway:
//!
//! - **Connections:** `gateway.max_connections` client connections, checked
//!   at accept ([`Limiter::try_connection`]; MySQL gets 1040 in place of the
//!   greeting, HTTP a 503); `limits.tenant_connections` sessions per tenant,
//!   checked after login ([`Limiter::attach`]; 39001).
//! - **Statements:** `limits.tenant_active_statements` and
//!   `limits.user_active_statements` running at once ([`Limited`], a wrapper
//!   around the executor, so the MySQL wire and HTTP share it):
//!
//! ```text
//! admit(session, cmd):
//!   if cmd ends a transaction or is a ping or close: free permit
//!   if tenant and user semaphores both try_acquire: return permit
//!   enqueue on the tenant's queue; high priority if session.state == InTransaction
//!   grant in queue order, high priority first, skipping users at their limit
//!   past limits.queue_timeout_ms: 39002
//! ```
//!
//! - **Memory:** what the gateway buffers is charged to three nested
//!   budgets ([`Limiter::charge`]): the gateway (`gateway.memory_limit_mb`,
//!   0 = 75% of the cgroup's limit, unlimited without one), the tenant
//!   (`limits.tenant_memory_mb`) and the user (a quarter of the tenant's). A
//!   charge succeeds only if all three have room; otherwise 39003.
//!
//! **As built.** The `limits.*` keys resolve for the session's tenant
//! (tenant, its ancestors, dc, global) from the tenant's cached catalog
//! settings, memoized per tenant until either the etcd snapshot or the
//! tenant's settings snapshot changes; a tenant whose catalog isn't cached
//! (and the limiter in tests) gets the dc and global values. The gateway
//! keys (`gateway.max_connections`, `gateway.memory_limit_mb`) stay at dc
//! and global. There is no deficit round robin across tenants: with a
//! separate cap per tenant there is no shared pool to divide, so each
//! tenant's queue is served on its own (round robin across tenants would
//! matter only with a gateway-wide statement cap). Memory is charged for HTTP results
//! the gateway buffers (`Sql.Execute`); the MySQL relay is bounded by
//! `gateway.relay_buffer_kb` per session and not charged yet.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sprig_common::ids::{DcId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_settings::{
    Apply, HardBounds, LiveSettings, Scopes, Setting, SettingCtx, SettingValue, TenantSettings,
};
use tokio::sync::oneshot;

use crate::pipeline::{
    Abandon, ExecCtx, ExecSummary, Executor, LoginInfo, Request, ResultSink, SessionExec,
    SharedExecutor,
};
use crate::session::SessionState;

macro_rules! key {
    ($NAME:ident, $key:literal, $default:expr, $scopes:expr, $op:expr, $min:expr, $max:expr, $doc:literal) => {
        #[doc = concat!("`", $key, "`.")]
        pub static $NAME: Setting<u64> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: HardBounds::range($min, $max),
            apply: Apply::Live,
            operator_only: $op,
            owner: 4,
            doc: $doc,
        };
        sprig_settings::register!($NAME);
    };
}

key!(
    MAX_CONNECTIONS,
    "gateway.max_connections",
    20_000,
    Scopes::PROCESS,
    false,
    1,
    10_000_000,
    "Client connections per gateway"
);
key!(
    MEMORY_LIMIT_MB,
    "gateway.memory_limit_mb",
    0,
    Scopes::PROCESS,
    false,
    0,
    10_000_000,
    "Gateway memory budget; 0 = 75% of the cgroup limit"
);
key!(
    TENANT_CONNECTIONS,
    "limits.tenant_connections",
    2000,
    Scopes::TREE,
    true,
    1,
    10_000_000,
    "Client connections per tenant per gateway"
);
key!(
    TENANT_ACTIVE_STATEMENTS,
    "limits.tenant_active_statements",
    256,
    Scopes::TREE,
    true,
    1,
    1_000_000,
    "Statements a tenant runs at once per gateway"
);
key!(
    USER_ACTIVE_STATEMENTS,
    "limits.user_active_statements",
    64,
    Scopes::TREE,
    false,
    1,
    1_000_000,
    "Statements a user runs at once per gateway"
);
key!(
    TENANT_MEMORY_MB,
    "limits.tenant_memory_mb",
    1024,
    Scopes::TREE,
    true,
    1,
    10_000_000,
    "Session memory per tenant per gateway"
);
key!(
    QUEUE_TIMEOUT_MS,
    "limits.queue_timeout_ms",
    5000,
    Scopes::TREE,
    false,
    1,
    3_600_000,
    "Longest admission wait"
);

/// The limits in force.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// `gateway.max_connections`.
    pub max_connections: usize,
    /// The gateway's memory budget in bytes; `None` for none.
    pub gateway_memory: Option<usize>,
    /// `limits.tenant_connections`.
    pub tenant_connections: usize,
    /// `limits.tenant_active_statements`.
    pub tenant_active: usize,
    /// `limits.user_active_statements`.
    pub user_active: usize,
    /// `limits.tenant_memory_mb` in bytes.
    pub tenant_memory: usize,
    /// `limits.queue_timeout_ms`.
    pub queue_timeout: Duration,
}

fn us(n: u64) -> usize {
    usize::try_from(n).unwrap_or(usize::MAX)
}

fn mb(n: u64) -> usize {
    us(n).saturating_mul(1 << 20)
}

/// 75% of the cgroup's memory limit, when the process has one: read once
/// per process (every statement asks for the limits twice, and reading
/// the file each time cost two `open` calls per statement).
fn cgroup_budget() -> Option<usize> {
    static BUDGET: std::sync::OnceLock<Option<usize>> = std::sync::OnceLock::new();
    *BUDGET.get_or_init(read_cgroup_budget)
}

fn read_cgroup_budget() -> Option<usize> {
    let raw = std::fs::read_to_string("/sys/fs/cgroup/memory.max")
        .or_else(|_| std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes"))
        .ok()?;
    let limit: usize = raw.trim().parse().ok()?;
    (limit < usize::MAX / 2).then(|| limit / 4 * 3)
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_connections: us(MAX_CONNECTIONS.default),
            gateway_memory: cgroup_budget(),
            tenant_connections: us(TENANT_CONNECTIONS.default),
            tenant_active: us(TENANT_ACTIVE_STATEMENTS.default),
            user_active: us(USER_ACTIVE_STATEMENTS.default),
            tenant_memory: mb(TENANT_MEMORY_MB.default),
            queue_timeout: Duration::from_millis(QUEUE_TIMEOUT_MS.default),
        }
    }
}

/// A tenant's settings snapshot as the catalog cache holds it now, without
/// loading (`None`: not cached here).
pub type TenantSettingsLookup = Arc<dyn Fn(TenantId) -> Option<Arc<TenantSettings>> + Send + Sync>;

/// Where the limits come from.
#[derive(Clone)]
pub enum LimitSource {
    /// Fixed values (tests).
    Fixed(Limits),
    /// The live settings at this DC's process scopes.
    Live {
        /// The mirror.
        live: LiveSettings,
        /// The DC.
        dc: DcId,
        /// The tenants' settings, for the tenant-scoped `limits.*` keys
        /// (`None`: dc and global only).
        tenants: Option<TenantSettingsLookup>,
    },
}

impl std::fmt::Debug for LimitSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fixed(l) => f.debug_tuple("Fixed").field(l).finish(),
            Self::Live { dc, .. } => f
                .debug_struct("Live")
                .field("dc", dc)
                .finish_non_exhaustive(),
        }
    }
}

impl LimitSource {
    fn current(&self) -> Limits {
        match self {
            Self::Fixed(l) => *l,
            Self::Live { live, .. } => self.current_in(&live.snapshot()),
        }
    }

    /// The limits in `snap` for `tenant`'s settings `ts` (fixed values
    /// ignore both): the `limits.*` keys at tenant scope, the gateway keys
    /// at dc and global.
    fn current_for(&self, snap: &sprig_settings::watch::Snapshot, ts: &TenantSettings) -> Limits {
        let Self::Live { dc, .. } = self else {
            return self.current_in(snap);
        };
        let base = self.current_in(snap);
        let ctx = ts.ctx(dc);
        let get = |k: &Setting<u64>| {
            ts.resolve(snap, k.key, &ctx)
                .ok()
                .and_then(|r| u64::from_value(&r.value))
                .unwrap_or(k.default)
        };
        Limits {
            tenant_connections: us(get(&TENANT_CONNECTIONS)),
            tenant_active: us(get(&TENANT_ACTIVE_STATEMENTS)),
            user_active: us(get(&USER_ACTIVE_STATEMENTS)),
            tenant_memory: mb(get(&TENANT_MEMORY_MB)),
            queue_timeout: Duration::from_millis(get(&QUEUE_TIMEOUT_MS)),
            ..base
        }
    }

    /// The limits in `snap` (fixed values ignore it).
    fn current_in(&self, snap: &sprig_settings::watch::Snapshot) -> Limits {
        match self {
            Self::Fixed(l) => *l,
            Self::Live { dc, .. } => {
                let ctx = SettingCtx::process(dc);
                let g = snap.get(&MEMORY_LIMIT_MB, &ctx);
                Limits {
                    max_connections: us(snap.get(&MAX_CONNECTIONS, &ctx)),
                    gateway_memory: if g == 0 { cgroup_budget() } else { Some(mb(g)) },
                    tenant_connections: us(snap.get(&TENANT_CONNECTIONS, &ctx)),
                    tenant_active: us(snap.get(&TENANT_ACTIVE_STATEMENTS, &ctx)),
                    user_active: us(snap.get(&USER_ACTIVE_STATEMENTS, &ctx)),
                    tenant_memory: mb(snap.get(&TENANT_MEMORY_MB, &ctx)),
                    queue_timeout: Duration::from_millis(snap.get(&QUEUE_TIMEOUT_MS, &ctx)),
                }
            }
        }
    }
}

type Key = Option<TenantId>;

/// A tenant's resolved limits, with the snapshots they came from.
type TenantMemo = (sprig_settings::watch::Snapshot, Arc<TenantSettings>, Limits);

/// Tenants whose limits are memoized at most; past it the memo starts over.
const TENANT_MEMO_MAX: usize = 4096;

struct Waiter {
    id: u64,
    user: String,
    high: bool,
    tx: oneshot::Sender<()>,
}

#[derive(Default)]
struct TenantState {
    conns: usize,
    active: usize,
    users: HashMap<String, usize>,
    mem: usize,
    user_mem: HashMap<String, usize>,
    queue: VecDeque<Waiter>,
}

impl TenantState {
    fn idle(&self) -> bool {
        self.conns == 0 && self.active == 0 && self.mem == 0 && self.queue.is_empty()
    }
}

#[derive(Default)]
struct State {
    tenants: HashMap<Key, TenantState>,
    next_waiter: u64,
}

/// The gateway's limits.
pub struct Limiter {
    source: LimitSource,
    /// The limits last resolved, with the settings snapshot they came from:
    /// every statement asks, and resolving seven keys each time was 4% of a
    /// point select's gateway CPU.
    memo: Mutex<Option<(sprig_settings::watch::Snapshot, Limits)>>,
    /// The same per tenant, for the tenant-scoped keys.
    tenant_memo: Mutex<HashMap<TenantId, TenantMemo>>,
    connections: AtomicUsize,
    memory: AtomicUsize,
    state: Mutex<State>,
}

impl std::fmt::Debug for Limiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Limiter")
            .field("connections", &self.connections.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// One client connection's place under `gateway.max_connections`.
#[derive(Debug)]
pub struct ConnPermit(Arc<Limiter>);

impl Drop for ConnPermit {
    fn drop(&mut self) {
        self.0.connections.fetch_sub(1, Ordering::SeqCst);
    }
}

/// One session's place under `limits.tenant_connections`.
#[derive(Debug)]
pub struct Attached {
    limiter: Arc<Limiter>,
    tenant: Key,
}

impl Drop for Attached {
    fn drop(&mut self) {
        let mut st = self.limiter.lock();
        if let Some(t) = st.tenants.get_mut(&self.tenant) {
            t.conns = t.conns.saturating_sub(1);
        }
        Limiter::forget_if_idle(&mut st, self.tenant);
    }
}

/// One running statement's admission permit.
#[derive(Debug)]
pub struct ActivePermit {
    limiter: Arc<Limiter>,
    tenant: Key,
    user: String,
}

impl Drop for ActivePermit {
    fn drop(&mut self) {
        self.limiter.release(self.tenant, &self.user);
    }
}

/// Bytes charged to the gateway, tenant and user budgets.
#[derive(Debug)]
pub struct MemCharge {
    limiter: Arc<Limiter>,
    tenant: Key,
    user: String,
    bytes: usize,
}

impl MemCharge {
    /// Adds `bytes` to the charge.
    ///
    /// # Errors
    /// 39003 if a budget has no room; the charge is unchanged.
    pub fn grow(&mut self, bytes: usize) -> Result<()> {
        self.limiter.take(self.tenant, &self.user, bytes)?;
        self.bytes += bytes;
        Ok(())
    }

    /// Bytes charged.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for MemCharge {
    fn drop(&mut self) {
        if self.bytes == 0 {
            return;
        }
        self.limiter.memory.fetch_sub(self.bytes, Ordering::SeqCst);
        let mut st = self.limiter.lock();
        if let Some(t) = st.tenants.get_mut(&self.tenant) {
            t.mem = t.mem.saturating_sub(self.bytes);
            if let Some(u) = t.user_mem.get_mut(&self.user) {
                *u = u.saturating_sub(self.bytes);
            }
        }
        Limiter::forget_if_idle(&mut st, self.tenant);
    }
}

fn admission_timeout(waited: Duration) -> SprigError {
    SprigError::new(
        SprigCode::AdmissionTimeout,
        format!(
            "the statement waited {} ms for an admission slot (limits.tenant_active_statements, limits.user_active_statements)",
            waited.as_millis()
        ),
    )
}

impl Limiter {
    /// A limiter reading `source`.
    #[must_use]
    pub fn new(source: LimitSource) -> Arc<Self> {
        Arc::new(Self {
            source,
            memo: Mutex::new(None),
            tenant_memo: Mutex::new(HashMap::new()),
            connections: AtomicUsize::new(0),
            memory: AtomicUsize::new(0),
            state: Mutex::new(State::default()),
        })
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn forget_if_idle(st: &mut State, tenant: Key) {
        if st.tenants.get(&tenant).is_some_and(TenantState::idle) {
            st.tenants.remove(&tenant);
        }
    }

    /// The limits in force now.
    #[must_use]
    pub fn limits(&self) -> Limits {
        let LimitSource::Live { live, .. } = &self.source else {
            return self.source.current();
        };
        let snap = live.snapshot();
        let mut memo = self.memo.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((s, l)) = memo.as_ref()
            && Arc::ptr_eq(s, &snap)
        {
            return *l;
        }
        let l = self.source.current_in(&snap);
        *memo = Some((snap, l));
        l
    }

    /// The limits in force now for `tenant`: its tenant-scoped values when
    /// its catalog is cached here, else [`limits`](Self::limits).
    #[must_use]
    pub fn limits_for(&self, tenant: Key) -> Limits {
        let (
            LimitSource::Live {
                live,
                tenants: Some(lookup),
                ..
            },
            Some(t),
        ) = (&self.source, tenant)
        else {
            return self.limits();
        };
        let Some(ts) = lookup(t) else {
            return self.limits();
        };
        let snap = live.snapshot();
        let mut memo = self
            .tenant_memo
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some((s, m, l)) = memo.get(&t)
            && Arc::ptr_eq(s, &snap)
            && Arc::ptr_eq(m, &ts)
        {
            return *l;
        }
        let l = self.source.current_for(&snap, &ts);
        if memo.len() >= TENANT_MEMO_MAX && !memo.contains_key(&t) {
            memo.clear();
        }
        memo.insert(t, (snap, ts, l));
        l
    }

    /// Client connections now.
    #[must_use]
    pub fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    /// A place for a new connection, or `None` at `gateway.max_connections`.
    #[must_use]
    pub fn try_connection(self: &Arc<Self>) -> Option<ConnPermit> {
        let max = self.limits().max_connections;
        let prev = self.connections.fetch_add(1, Ordering::SeqCst);
        if prev >= max {
            self.connections.fetch_sub(1, Ordering::SeqCst);
            return None;
        }
        Some(ConnPermit(Arc::clone(self)))
    }

    /// Attaches a logged-in session to its tenant.
    ///
    /// # Errors
    /// 39001 at `limits.tenant_connections`.
    pub fn attach(self: &Arc<Self>, tenant: Key) -> Result<Attached> {
        let max = self.limits_for(tenant).tenant_connections;
        let mut st = self.lock();
        let t = st.tenants.entry(tenant).or_default();
        if t.conns >= max {
            metrics::counter!("sprig_gateway_connections_rejected_total", "listener" => "any", "reason" => "tenant_limit").increment(1);
            return Err(SprigError::new(
                SprigCode::TenantConnectionLimit,
                format!(
                    "the tenant has {max} connections to this gateway (limits.tenant_connections)"
                ),
            ));
        }
        t.conns += 1;
        Ok(Attached {
            limiter: Arc::clone(self),
            tenant,
        })
    }

    /// Admits one statement of `user` of `tenant`; `high` for a session
    /// inside a transaction.
    ///
    /// # Errors
    /// 39002 past `limits.queue_timeout_ms`.
    pub async fn admit(
        self: &Arc<Self>,
        tenant: Key,
        user: &str,
        high: bool,
    ) -> Result<ActivePermit> {
        let l = self.limits_for(tenant);
        let (rx, id) = {
            let mut st = self.lock();
            let next = st.next_waiter;
            let t = st.tenants.entry(tenant).or_default();
            let ua = t.users.get(user).copied().unwrap_or(0);
            if t.queue.is_empty() && t.active < l.tenant_active && ua < l.user_active {
                t.active += 1;
                *t.users.entry(user.to_owned()).or_default() += 1;
                return Ok(self.permit(tenant, user));
            }
            let (tx, rx) = oneshot::channel();
            let w = Waiter {
                id: next,
                user: user.to_owned(),
                high,
                tx,
            };
            // High priority goes after the other high-priority waiters.
            let at = if high {
                t.queue
                    .iter()
                    .position(|w| !w.high)
                    .unwrap_or(t.queue.len())
            } else {
                t.queue.len()
            };
            t.queue.insert(at, w);
            st.next_waiter += 1;
            (rx, next)
        };
        let started = Instant::now();
        let r = tokio::time::timeout(l.queue_timeout, rx).await;
        metrics::histogram!("sprig_gateway_admission_wait_seconds")
            .record(started.elapsed().as_secs_f64());
        if let Ok(Ok(())) = r {
            return Ok(self.permit(tenant, user));
        }
        // Timed out: leave the queue, unless a grant raced the timeout, in
        // which case give the slot back.
        let mut st = self.lock();
        let granted = st.tenants.get_mut(&tenant).is_none_or(|t| {
            match t.queue.iter().position(|w| w.id == id) {
                Some(i) => {
                    t.queue.remove(i);
                    false
                }
                None => true,
            }
        });
        drop(st);
        if granted {
            self.release(tenant, user);
        }
        Err(admission_timeout(started.elapsed()))
    }

    fn permit(self: &Arc<Self>, tenant: Key, user: &str) -> ActivePermit {
        ActivePermit {
            limiter: Arc::clone(self),
            tenant,
            user: user.to_owned(),
        }
    }

    /// A statement ended: free its slot and grant waiters.
    fn release(&self, tenant: Key, user: &str) {
        let l = self.limits_for(tenant);
        let mut st = self.lock();
        if let Some(t) = st.tenants.get_mut(&tenant) {
            t.active = t.active.saturating_sub(1);
            if let Some(u) = t.users.get_mut(user) {
                *u = u.saturating_sub(1);
                if *u == 0 {
                    t.users.remove(user);
                }
            }
            // Grant in order, skipping users at their own limit.
            while t.active < l.tenant_active {
                let Some(i) = t
                    .queue
                    .iter()
                    .position(|w| t.users.get(&w.user).copied().unwrap_or(0) < l.user_active)
                else {
                    break;
                };
                let Some(w) = t.queue.remove(i) else { break };
                if w.tx.is_closed() {
                    continue;
                }
                t.active += 1;
                *t.users.entry(w.user.clone()).or_default() += 1;
                if w.tx.send(()).is_err() {
                    // The waiter left between the check and the send.
                    t.active -= 1;
                    if let Some(u) = t.users.get_mut(&w.user) {
                        *u -= 1;
                    }
                }
            }
        }
        Self::forget_if_idle(&mut st, tenant);
    }

    /// An empty charge for `user` of `tenant`, grown as bytes are buffered.
    #[must_use]
    pub fn charge(self: &Arc<Self>, tenant: Key, user: &str) -> MemCharge {
        MemCharge {
            limiter: Arc::clone(self),
            tenant,
            user: user.to_owned(),
            bytes: 0,
        }
    }

    fn take(&self, tenant: Key, user: &str, bytes: usize) -> Result<()> {
        let l = self.limits_for(tenant);
        let full = |what: &str| {
            SprigError::new(
                SprigCode::MemoryLimit,
                format!("the {what} memory budget on this gateway is spent"),
            )
        };
        let prev = self.memory.fetch_add(bytes, Ordering::SeqCst);
        if l.gateway_memory.is_some_and(|g| prev + bytes > g) {
            self.memory.fetch_sub(bytes, Ordering::SeqCst);
            return Err(full("gateway"));
        }
        let mut st = self.lock();
        let t = st.tenants.entry(tenant).or_default();
        let um = t.user_mem.get(user).copied().unwrap_or(0);
        let r = if t.mem + bytes > l.tenant_memory {
            Err(full("tenant"))
        } else if um + bytes > l.tenant_memory / 4 {
            Err(full("user"))
        } else {
            t.mem += bytes;
            *t.user_mem.entry(user.to_owned()).or_default() += bytes;
            Ok(())
        };
        if r.is_err() {
            self.memory.fetch_sub(bytes, Ordering::SeqCst);
            Self::forget_if_idle(&mut st, tenant);
        }
        r
    }

    /// Bytes charged to the gateway now.
    #[must_use]
    pub fn memory(&self) -> usize {
        self.memory.load(Ordering::SeqCst)
    }
}

/// Whether a request skips admission: it ends a transaction or answers
/// nothing that runs.
fn free_pass(req: &Request) -> bool {
    match req {
        Request::CloseStmt { .. } | Request::LongData { .. } | Request::ResetStmt { .. } => true,
        Request::Query { sql, .. } => {
            let t = sql.iter().skip_while(|b| b.is_ascii_whitespace()).take(8);
            let head: Vec<u8> = t.map(u8::to_ascii_uppercase).collect();
            head.starts_with(b"COMMIT") || head.starts_with(b"ROLLBACK")
        }
        _ => false,
    }
}

/// The executor behind admission control.
pub struct Limited {
    inner: SharedExecutor,
    limiter: Arc<Limiter>,
}

impl std::fmt::Debug for Limited {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Limited").finish_non_exhaustive()
    }
}

/// `exec` behind `limiter`'s admission.
#[must_use]
pub fn limited(exec: SharedExecutor, limiter: Arc<Limiter>) -> SharedExecutor {
    Arc::new(Limited {
        inner: exec,
        limiter,
    })
}

impl Executor for Limited {
    fn open(&self, login: &LoginInfo) -> Box<dyn SessionExec> {
        Box::new(LimitedSession {
            inner: self.inner.open(login),
            limiter: Arc::clone(&self.limiter),
            tenant: login.principal.tenant,
            user: login.principal.user.clone(),
        })
    }
}

struct LimitedSession {
    inner: Box<dyn SessionExec>,
    limiter: Arc<Limiter>,
    tenant: Key,
    user: String,
}

#[async_trait]
impl SessionExec for LimitedSession {
    async fn point_read(
        &mut self,
        cx: &mut ExecCtx<'_>,
        stmt: crate::pipeline::StmtRef,
        params: &sprig_proto_mysql::command::ExecuteParams,
    ) -> Result<crate::pipeline::PointRead> {
        let high = cx.session.state() == SessionState::InTransaction;
        let _permit = self.limiter.admit(self.tenant, &self.user, high).await?;
        self.inner.point_read(cx, stmt, params).await
    }

    fn holds_idle(&self) -> bool {
        self.inner.holds_idle()
    }

    fn release_idle(&mut self) {
        self.inner.release_idle();
    }

    async fn execute(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: Request,
        out: &mut dyn ResultSink,
    ) -> Result<ExecSummary> {
        let _permit = if free_pass(&req) {
            None
        } else {
            let high = cx.session.state() == SessionState::InTransaction;
            Some(self.limiter.admit(self.tenant, &self.user, high).await?)
        };
        self.inner.execute(cx, req, out).await
    }

    async fn init_db(&mut self, cx: &mut ExecCtx<'_>, schema: &str) -> Result<()> {
        self.inner.init_db(cx, schema).await
    }

    async fn reset(&mut self, cx: &mut ExecCtx<'_>) -> Result<()> {
        self.inner.reset(cx).await
    }

    async fn abandoned(&mut self, cx: &mut ExecCtx<'_>, why: Abandon) {
        self.inner.abandoned(cx, why).await;
    }

    async fn close(&mut self) {
        self.inner.close().await;
    }

    fn attach_capture(&mut self, capture: crate::workload::SessionCapture) {
        self.inner.attach_capture(capture);
    }

    fn workload(&self, db: &str, table: &str) -> Option<crate::workload::FrontCapture> {
        self.inner.workload(db, table)
    }

    fn idle_timeout(&self) -> Option<std::time::Duration> {
        self.inner.idle_timeout()
    }

    fn set_internal(&mut self, on: bool) {
        self.inner.set_internal(on);
    }

    fn row_policy_binds(&self, object: u64) -> Option<bool> {
        self.inner.row_policy_binds(object)
    }

    fn row_policy_docs(&self) -> Option<std::sync::Arc<crate::tenant::rls::DocViews>> {
        self.inner.row_policy_docs()
    }

    fn holds_tenant_privilege(&self, action: sprig_auth::authz::Action) -> Option<bool> {
        self.inner.holds_tenant_privilege(action)
    }

    fn set_claims(&mut self, claims: Option<std::sync::Arc<serde_json::Value>>) {
        self.inner.set_claims(claims);
    }
}

#[cfg(test)]
mod tests;
