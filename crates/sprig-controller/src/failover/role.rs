//! Chapter 25 in the controller role: what [`crate::serve`] starts.
//!
//! On every controller of a DC:
//!
//! - the DC lease ([`DcLease`]), renewed every TTL/4; the DC leader takes
//!   a new one when there is none;
//! - the internal services on `[controller] internal_addr`
//!   ([`super::service`]: `Permits.Renew`, `Peer.Ping`,
//!   `Observer.ObserveDc`), published in the controller's registration;
//! - the standby lags this DC holds ([`super::nodes::standby_lags`]),
//!   measured every second for its observer;
//! - the admission rate of the current traffic step, read from the DC
//!   tier's `recovery/traffic` record, which every controller's permits
//!   carry to the gateways.
//!
//! On the DC leader, loops:
//!
//! - `dc_observe`: publishes `dc-observe/<dc>` (controllers, writers and
//!   whether an agent fences each one) every 5 s;
//! - `recovery`: a returning DC fences and quarantines its moved tenants
//!   ([`ReturningDc`]), sets itself `active`, starts the traffic steps,
//!   and reports each fenced copy's diverged transactions into the
//!   failover record ([`super::nodes::report_diverged`]).
//!
//! On the global leader, the `dc_failover` loop runs
//! [`GlobalFailover::tick`] every 500 ms, writing through the leader's
//! token.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sprig_catalog::placement::{MapRole, ShardMap};
use sprig_common::ids::{DcId, TenantId};
use sprig_common::time::Clock;
use sprig_common::{CancellationToken, Result};
use sprig_meta::dclease::DcLease;
use sprig_meta::{
    Consistency, ListOpts, MetaStore, Op, Record, Revision, Tier, TierView, Txn, Versioned,
};
use sprig_settings::LiveSettings;
use tokio::sync::watch;

use super::client::{HttpDcProbe, HttpObserverClient};
use super::global::GlobalFailover;
use super::nodes::{ClusterHooks, GrantFence, report_diverged, standby_lags};
use super::observe::{ObserveTargets, WriterTarget, targets_key};
use super::promote::{DcTiers, Guard, Promotion};
use super::record::StandbyLag;
use super::recovery::{ReturnStep, ReturningDc};
use super::service::{Internal, router};
use super::settings::FailoverSettings;
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler, Status, Stores};

/// The one key of the single-unit loops here.
const UNIT: &str = "dc";
/// `dc-observe/<dc>` is written this often.
const PUBLISH_EVERY: Duration = Duration::from_secs(5);
/// The diverged-transaction reports are retried this often.
const REPORT_EVERY: Duration = Duration::from_secs(10);
/// One report may take this long.
const REPORT_TIMEOUT: Duration = Duration::from_secs(15);

/// `recovery/traffic` in the DC tier: when the stepped return of traffic
/// started, so every controller of the DC grants the same admission rate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrafficRecord {
    /// Unix milliseconds when the DC returned.
    pub started_ms: u64,
    /// `recovery.traffic_steps_pct`.
    pub steps_pct: Vec<u32>,
    /// `recovery.step_minutes`, in seconds.
    pub step_seconds: u64,
    /// `recovery.admission_connects_per_second`.
    pub base_per_second: u32,
}

impl Versioned for TrafficRecord {
    const V: u32 = 1;
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

impl TrafficRecord {
    /// The admission rate now: the step's, or 0 (no limit) once the last
    /// step is reached.
    #[must_use]
    pub fn admit_per_second(&self, now_ms: u64) -> u32 {
        if self.steps_pct.is_empty() {
            return 0;
        }
        let step_ms = self.step_seconds.max(1) * 1000;
        let idx = usize::try_from(now_ms.saturating_sub(self.started_ms) / step_ms)
            .unwrap_or(usize::MAX)
            .min(self.steps_pct.len() - 1);
        if idx + 1 == self.steps_pct.len() {
            return 0;
        }
        let first = u64::from(self.steps_pct[0].max(1));
        let rate = u64::from(self.base_per_second) * u64::from(self.steps_pct[idx]) / first;
        u32::try_from(rate).unwrap_or(u32::MAX).max(1)
    }
}

/// The DC tier's `recovery/traffic` key.
///
/// # Errors
/// Internal never in practice.
pub fn traffic_key(tier: &Tier) -> Result<sprig_meta::Key> {
    tier.key("recovery/traffic")
}

/// Chapter 25 in one controller (see the module docs).
pub struct FailoverRole {
    dc: DcId,
    node: String,
    stores: Stores,
    clock: Arc<dyn Clock>,
    live: Option<LiveSettings>,
    single_etcd: bool,
    lease: Arc<DcLease>,
    lags: Arc<RwLock<Vec<StandbyLag>>>,
    admit: Arc<std::sync::atomic::AtomicU32>,
    internal_url: RwLock<Option<String>>,
    global: Mutex<Option<(Revision, Arc<GlobalFailover>)>>,
    reported: Mutex<Option<Instant>>,
    /// The leader token under which `global-service` was published.
    published_global: Mutex<Option<Revision>>,
}

impl std::fmt::Debug for FailoverRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FailoverRole")
            .field("dc", &self.dc)
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl FailoverRole {
    /// The role of controller `node` in `dc`. `single_etcd`: one etcd
    /// serves every tier, so the global leader reaches every DC's tier
    /// (promotions write the standby DC's maps).
    #[must_use]
    pub fn new(
        dc: DcId,
        node: String,
        stores: Stores,
        clock: Arc<dyn Clock>,
        live: Option<LiveSettings>,
        single_etcd: bool,
    ) -> Arc<Self> {
        let settings = resolve(live.as_ref(), &dc);
        let lease = Arc::new(DcLease::new(
            dc.clone(),
            node.clone(),
            Arc::clone(&stores.global),
            Arc::clone(&clock),
            settings.lease_opts(),
        ));
        Arc::new(Self {
            dc,
            node,
            stores,
            clock,
            live,
            single_etcd,
            lease,
            lags: Arc::new(RwLock::new(Vec::new())),
            admit: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            internal_url: RwLock::new(None),
            global: Mutex::new(None),
            reported: Mutex::new(None),
            published_global: Mutex::new(None),
        })
    }

    /// The stores.
    #[must_use]
    pub fn stores(&self) -> &Stores {
        &self.stores
    }

    /// This controller's view of the DC lease.
    #[must_use]
    pub fn lease(&self) -> &Arc<DcLease> {
        &self.lease
    }

    /// The failover settings for `dc` now.
    #[must_use]
    pub fn settings(&self, dc: &DcId) -> FailoverSettings {
        resolve(self.live.as_ref(), dc)
    }

    /// What the internal routes answer from.
    #[must_use]
    pub fn internal(&self) -> Arc<Internal> {
        let lags = Arc::clone(&self.lags);
        let admit = Arc::clone(&self.admit);
        Arc::new(Internal {
            lease: Arc::clone(&self.lease),
            probe: Arc::new(HttpDcProbe::new(
                Arc::clone(&self.stores.global),
                Duration::from_secs(1),
            )),
            settings: self.settings(&self.dc),
            lags: Arc::new(move || lags.read().unwrap_or_else(PoisonError::into_inner).clone()),
            admit_per_second: Arc::new(move || admit.load(std::sync::atomic::Ordering::SeqCst)),
            fenced_writers: Arc::new(Vec::new),
        })
    }

    /// Serves the internal routes (and `extra`, chapter 23's relay) on
    /// `listener` until `stop`; remembers the advertised URL for
    /// `dc-observe/<dc>`.
    #[allow(clippy::needless_pass_by_value)] // the router is consumed by merge
    ///
    /// With `tls` (`[controller] internal_tls`; hardening round 3) it
    /// serves mutual TLS and the URL is `https://`.
    pub fn serve_internal(
        self: &Arc<Self>,
        listener: tokio::net::TcpListener,
        advertised: &str,
        extra: axum::Router,
        tls: Option<Arc<sprig_common::https::ServerConfig>>,
        stop: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let scheme = if tls.is_some() { "https" } else { "http" };
        *self
            .internal_url
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Some(format!("{scheme}://{advertised}"));
        let me = Arc::clone(self);
        let handle: super::two_tier::GlobalHandle =
            Arc::new(move || lock(&me.global).as_ref().map(|(_, g)| Arc::clone(g)));
        let app = router(self.internal())
            .merge(super::two_tier::router(handle))
            .merge(super::relay::router(Arc::clone(&self.stores.dc)))
            .merge(extra);
        tokio::spawn(async move {
            let r = match tls {
                None => {
                    axum::serve(listener, app)
                        .with_graceful_shutdown(async move { stop.cancelled().await })
                        .await
                }
                Some(cfg) => match sprig_common::https::TlsListener::new(listener, cfg) {
                    Ok(l) => {
                        axum::serve(l, app)
                            .with_graceful_shutdown(async move { stop.cancelled().await })
                            .await
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "internal services can't serve TLS");
                        return;
                    }
                },
            };
            if let Err(e) = r {
                tracing::warn!(error = %e, "internal services stopped");
            }
        })
    }

    /// The background tasks of every controller: the DC lease (taken by
    /// the DC leader, as `status` says), the standby lags, the admission
    /// rate. They end at `stop`.
    pub fn spawn(
        self: &Arc<Self>,
        status: &watch::Receiver<Status>,
        stop: &CancellationToken,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        let mut tasks = Vec::new();
        let (me, st, s) = (Arc::clone(self), status.clone(), stop.clone());
        tasks.push(tokio::spawn(async move {
            let lease = Arc::clone(&me.lease);
            let leading = move || st.borrow().dc_leader.is_some();
            tokio::select! {
                () = s.cancelled() => {}
                () = lease.run(leading) => {}
            }
        }));
        let (me, s) = (Arc::clone(self), stop.clone());
        tasks.push(tokio::spawn(async move {
            while !s.is_cancelled() {
                me.measure().await;
                tokio::select! {
                    () = s.cancelled() => {}
                    () = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }
        }));
        tasks
    }

    /// One measuring round: standby lags and the admission rate.
    async fn measure(&self) {
        match tokio::time::timeout(
            Duration::from_secs(5),
            standby_lags(&self.stores.global, &self.stores.dc),
        )
        .await
        {
            Ok(Ok(l)) => {
                *self.lags.write().unwrap_or_else(PoisonError::into_inner) = l;
            }
            Ok(Err(e)) => tracing::debug!(error = %e, "standby lags"),
            Err(_) => tracing::debug!("standby lags timed out"),
        }
        let rate = match traffic_key(self.stores.dc.tier()) {
            Ok(k) => match self.stores.dc.get(&k, Consistency::Linearizable).await {
                Ok(Some(kv)) => Record::<TrafficRecord>::decode(&kv.value)
                    .map_or(0, |r| r.body.admit_per_second(unix_ms())),
                Ok(None) => 0,
                // Unreachable: keep the last rate.
                Err(_) => self.admit.load(std::sync::atomic::Ordering::SeqCst),
            },
            Err(_) => 0,
        };
        self.admit.store(rate, std::sync::atomic::Ordering::SeqCst);
        #[allow(clippy::cast_precision_loss)] // a gauge
        metrics::gauge!("sprig_controller_dc_lease_remaining_seconds").set(
            self.lease.valid_until().map_or(0.0, |u| {
                u.saturating_duration_since(self.clock.now()).as_secs_f64()
            }),
        );
    }

    /// Every DC's tier: views of the one etcd in single-etcd mode; in two
    /// tiers, only this DC's (promotions into another DC then fail after
    /// the commit point and are retried, so two-tier deployments keep
    /// `failover.mode = manual` until the `tenant_promote` job exists).
    #[must_use]
    pub fn dc_tiers(&self) -> DcTiers {
        let dc = Arc::clone(&self.stores.dc);
        let me = self.dc.clone();
        let single = self.single_etcd;
        Arc::new(move |d: &DcId| {
            if *d == me || !single {
                if *d != me {
                    tracing::debug!(dc = %d, "another DC's tier isn't reachable in two-tier mode");
                }
                return Arc::clone(&dc);
            }
            TierView::over(&dc, Tier::Dc(d.clone()))
        })
    }

    /// Promotions writing global keys through `guard`.
    #[must_use]
    pub fn promotion(&self, guard: Guard) -> Promotion {
        let tiers = self.dc_tiers();
        Promotion {
            global: Arc::clone(&self.stores.global),
            guard,
            dc_tier: Arc::clone(&tiers),
            hooks: self.hooks(tiers),
        }
    }

    /// The data-path hooks; in two tiers, promotions into other DCs stop at
    /// the commit point and those DCs' `tenant_promote` loops finish them.
    fn hooks(&self, tiers: DcTiers) -> Arc<dyn super::promote::PromoteHooks> {
        let inner = Arc::new(ClusterHooks {
            global: Arc::clone(&self.stores.global),
            dc_tier: tiers,
        });
        if self.single_etcd {
            inner
        } else {
            Arc::new(super::promote::TwoTierHooks {
                inner,
                me: self.dc.clone(),
            })
        }
    }

    /// One pass of the `tenant_promote` loop (two tiers; see
    /// [`super::two_tier`]).
    async fn tenant_promote(&self) -> Result<usize> {
        let hooks = ClusterHooks {
            global: Arc::clone(&self.stores.global),
            dc_tier: self.dc_tiers(),
        };
        let client = super::two_tier::GlobalClient::new(
            Arc::clone(&self.stores.global),
            Duration::from_secs(10),
        );
        super::two_tier::tenant_promote(
            &*self.stores.global,
            &*self.stores.dc,
            &self.dc,
            &hooks,
            &client,
        )
        .await
    }

    /// Publishes this global leader's `Global` service once per term.
    async fn publish_global_service(&self, cx: &LoopCx) -> Result<()> {
        let l = cx.leader()?;
        if *lock(&self.published_global) == Some(l.token) {
            return Ok(());
        }
        let Some(url) = self
            .internal_url
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
        else {
            return Ok(());
        };
        super::two_tier::publish(l, &self.node, &url).await?;
        *lock(&self.published_global) = Some(l.token);
        Ok(())
    }

    /// `dc-observe/<dc>`: this DC's controllers' internal URLs and the
    /// nodes of its clusters, each marked self-fencing when an agent
    /// registered for it (`agents/<id>`).
    async fn targets(&self) -> Result<ObserveTargets> {
        let mut controllers: Vec<String> =
            sprig_meta::permit::http::internal_addrs(&*self.stores.dc)
                .await?
                .into_iter()
                .map(|a| {
                    if a.contains("://") {
                        a
                    } else {
                        format!("http://{a}")
                    }
                })
                .collect();
        if let Some(me) = self
            .internal_url
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            && !controllers.contains(&me)
        {
            controllers.push(me);
        }
        let agents = agent_nodes(&*self.stores.dc).await?;
        let reg = sprig_cluster::registry::ClusterRegistry::new(
            Arc::clone(&self.stores.global),
            Arc::clone(&self.stores.dc),
        )?;
        let mut writers = Vec::new();
        for c in reg.list().await? {
            let rec = c.body();
            for n in &rec.nodes {
                writers.push(WriterTarget {
                    cluster: rec.cluster_id.to_string(),
                    addr: n.addr.clone(),
                    self_fences: agents.contains(&(rec.cluster_id.to_string(), n.node.to_string())),
                });
            }
        }
        Ok(ObserveTargets {
            controllers,
            writers,
        })
    }

    async fn publish(&self) -> Result<()> {
        let t = self.targets().await?;
        self.stores
            .global
            .txn(Txn {
                when: Vec::new(),
                then: vec![Op::Put {
                    key: targets_key(&self.dc)?,
                    value: Record::new(t).encode()?,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        Ok(())
    }

    /// One pass of the returning DC's side (the `recovery` loop).
    async fn recover(&self) -> Result<()> {
        let ret = ReturningDc {
            dc: self.dc.clone(),
            global: Arc::clone(&self.stores.global),
            tier: Arc::clone(&self.stores.dc),
            fence: Arc::new(GrantFence {
                global: Arc::clone(&self.stores.global),
                tier: Arc::clone(&self.stores.dc),
            }),
        };
        if let ReturnStep::Unfenced { fenced } = ret.step().await? {
            let s = self.settings(&self.dc);
            let rec = TrafficRecord {
                started_ms: unix_ms(),
                steps_pct: s.traffic_steps_pct.clone(),
                step_seconds: s.step.as_secs(),
                base_per_second: s.admission_per_second,
            };
            self.stores
                .dc
                .txn(Txn {
                    when: Vec::new(),
                    then: vec![Op::Put {
                        key: traffic_key(self.stores.dc.tier())?,
                        value: Record::new(rec).encode()?,
                        lease: None,
                    }],
                    otherwise: Vec::new(),
                })
                .await?;
            tracing::warn!(dc = %self.dc, fenced = ?fenced, "returned: traffic returns in steps");
            *lock(&self.reported) = None;
        }
        // Diverged-transaction reports for every quarantined copy, until
        // each is in its failover record; only once the DC is active, and
        // bounded, so they never hold up a return (reading a binlog can
        // wait behind a commit stuck on a semi-sync acknowledgement).
        let due = lock(&self.reported).is_none_or(|t| t.elapsed() >= REPORT_EVERY);
        if due && self.lease.dc_state() == "active" {
            *lock(&self.reported) = Some(Instant::now());
            for t in quarantined(&*self.stores.dc).await? {
                let r = tokio::time::timeout(
                    REPORT_TIMEOUT,
                    report_diverged(&self.stores.global, &self.stores.dc, t),
                )
                .await;
                match r {
                    Ok(Ok(Some(n))) => {
                        tracing::info!(tenant = %t, diverged = n, "diverged-transaction report written");
                    }
                    Ok(Ok(None)) => {}
                    Ok(Err(e)) => {
                        tracing::warn!(tenant = %t, error = %e, "diverged-transaction report failed; retrying");
                    }
                    Err(_) => {
                        tracing::warn!(tenant = %t, "diverged-transaction report timed out; retrying");
                    }
                }
            }
        }
        Ok(())
    }

    fn global_failover(&self, cx: &LoopCx) -> Result<Arc<GlobalFailover>> {
        let l = cx.leader()?;
        let mut g = lock(&self.global);
        if let Some((token, f)) = g.as_ref()
            && *token == l.token
        {
            return Ok(Arc::clone(f));
        }
        let live = self.live.clone();
        let f = Arc::new(GlobalFailover::new(
            Arc::clone(&self.stores.global),
            Arc::clone(&self.clock),
            Arc::new(move |d: &DcId| resolve(live.as_ref(), d)),
            Arc::new(HttpObserverClient::new(
                Arc::clone(&self.stores.global),
                Duration::from_secs(10),
            )),
            self.promotion(Guard::Leader(l.clone())),
        ));
        *g = Some((l.token, Arc::clone(&f)));
        Ok(f)
    }

    /// The DC-leader and global-leader loops.
    #[must_use]
    pub fn loops(self: &Arc<Self>) -> Vec<crate::runtime::Loop> {
        vec![
            crate::runtime::Loop::new(Unit {
                role: Arc::clone(self),
                kind: Kind::Publish,
            }),
            crate::runtime::Loop::new(Unit {
                role: Arc::clone(self),
                kind: Kind::Recovery,
            }),
            crate::runtime::Loop::new(Unit {
                role: Arc::clone(self),
                kind: Kind::Global,
            }),
            crate::runtime::Loop::new(Unit {
                role: Arc::clone(self),
                kind: Kind::TenantPromote,
            }),
            // Failover inside a cluster (r2next).
            crate::runtime::Loop::new(super::cluster::ClusterFailover::new(
                self.dc.clone(),
                self.node.clone(),
                Arc::clone(&self.clock),
                self.live.clone(),
            )),
        ]
    }
}

fn resolve(live: Option<&LiveSettings>, dc: &DcId) -> FailoverSettings {
    live.map_or_else(FailoverSettings::default, |l| {
        FailoverSettings::resolve(&l.snapshot(), dc)
    })
}

/// `(cluster, node)` of every agent registered in this DC (`agents/<id>`).
///
/// # Errors
/// The store's errors.
pub async fn agent_nodes(dc: &dyn MetaStore) -> Result<BTreeSet<(String, String)>> {
    let page = dc
        .list(&dc.tier().key("agents/")?, ListOpts::default())
        .await?;
    Ok(page
        .kvs
        .iter()
        .filter_map(|kv| {
            let v: serde_json::Value = serde_json::from_slice(&kv.value).ok()?;
            Some((
                v.get("cluster")?.as_str()?.to_owned(),
                v.get("node")?.as_str()?.to_owned(),
            ))
        })
        .collect())
}

/// Tenants with a `quarantined` map in this tier.
async fn quarantined(tier: &dyn MetaStore) -> Result<Vec<TenantId>> {
    let page = tier
        .list(&tier.tier().key("shardmaps/")?, ListOpts::default())
        .await?;
    let mut out: Vec<TenantId> = page
        .kvs
        .iter()
        .filter_map(|kv| Record::<ShardMap>::decode(&kv.value).ok())
        .filter(|m| m.body.role == MapRole::Quarantined)
        .map(|m| m.body.tenant_id)
        .collect();
    out.sort_unstable_by_key(|t| t.0);
    out.dedup();
    Ok(out)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Publish,
    Recovery,
    Global,
    TenantPromote,
}

/// One of the single-unit loops (see the module docs).
struct Unit {
    role: Arc<FailoverRole>,
    kind: Kind,
}

#[async_trait::async_trait]
impl Reconciler for Unit {
    type Key = &'static str;

    fn name(&self) -> &'static str {
        match self.kind {
            Kind::Publish => "dc_observe",
            Kind::Recovery => "recovery",
            Kind::Global => "dc_failover",
            Kind::TenantPromote => "tenant_promote",
        }
    }

    fn tier(&self) -> LoopTier {
        match self.kind {
            Kind::Publish | Kind::Recovery | Kind::TenantPromote => LoopTier::DcLeader,
            Kind::Global => LoopTier::GlobalLeader,
        }
    }

    fn keys_for(&self, _ev: &MetaEvent) -> Vec<Self::Key> {
        vec![UNIT]
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<Self::Key>> {
        Ok(vec![UNIT])
    }

    async fn reconcile(&self, cx: &LoopCx, _key: &Self::Key) -> Result<Outcome> {
        match self.kind {
            Kind::Publish => {
                cx.leader()?;
                self.role.publish().await?;
                Ok(Outcome::RequeueAfter(PUBLISH_EVERY))
            }
            Kind::Recovery => {
                cx.leader()?;
                self.role.recover().await?;
                Ok(Outcome::RequeueAfter(Duration::from_millis(500)))
            }
            Kind::TenantPromote => {
                cx.leader()?;
                if self.role.single_etcd {
                    // The global leader writes every step itself.
                    return Ok(Outcome::RequeueAfter(Duration::from_secs(30)));
                }
                self.role.tenant_promote().await?;
                Ok(Outcome::RequeueAfter(Duration::from_millis(500)))
            }
            Kind::Global => {
                let g = self.role.global_failover(cx)?;
                if let Err(e) = self.role.publish_global_service(cx).await {
                    tracing::warn!(error = %e, "can't publish the Global service");
                }
                for o in g.tick().await? {
                    if let super::global::Outcome::Ended(f) = o {
                        tracing::warn!(failover = %f.failover_id, dc = %f.dc_id, state = ?f.state, votes = ?f.votes, "DC failover decision");
                    }
                }
                Ok(Outcome::RequeueAfter(Duration::from_millis(500)))
            }
        }
    }
}

/// Where the internal services listen and what they publish.
///
/// # Errors
/// 39998 if the address can't be bound.
pub async fn bind(
    addr: SocketAddr,
    advertise: Option<&str>,
) -> Result<(tokio::net::TcpListener, String)> {
    let l = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
        sprig_common::SprigError::new(
            sprig_common::SprigCode::ConfigInvalid,
            format!("[controller] internal_addr {addr} can't be bound: {e}"),
        )
    })?;
    let bound = l
        .local_addr()
        .map_err(|e| sprig_common::SprigError::internal(e.to_string()))?;
    let adv = advertise.map_or_else(|| crate::registration::advertised(bound), str::to_owned);
    Ok((l, adv))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_admission_rate_follows_the_steps_then_lifts() {
        let r = TrafficRecord {
            started_ms: 1_000_000,
            steps_pct: vec![10, 25, 50, 100],
            step_seconds: 60,
            base_per_second: 200,
        };
        assert_eq!(r.admit_per_second(1_000_000 + 1_000), 200);
        assert_eq!(r.admit_per_second(1_000_000 + 61_000), 500);
        assert_eq!(r.admit_per_second(1_000_000 + 121_000), 1000);
        assert_eq!(
            r.admit_per_second(1_000_000 + 181_000),
            0,
            "the last step lifts the limit"
        );
    }
}
