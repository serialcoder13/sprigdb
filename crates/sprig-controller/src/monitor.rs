//! The `registry` loop: probes every registered cluster and keeps its
//! capability profile and health current (chapter 09, "Probes",
//! "Publishing", "Health model"; chapter 23, "The reconciler pattern and
//! the loops").
//!
//! ```text
//! pass(cluster):
//! 1. read the record; retired or gone -> forget it
//! 2. resolve cluster.* for this cluster (cluster > dc > global)
//! 3. depth: deep at the first pass of a new profile, after a version,
//!    server_id or Galera membership change, daily, or on `probe --full`;
//!    full every cluster.probe_interval_seconds; light otherwise
//! 4. probe every node as sprig_ctl (a deep probe also reads the binlog
//!    as sprig_repl: the dump probe); derive the profile; assess health
//! 5. publish: downgrades at once, upgrades after
//!    cluster.capability_upgrade_sustain_seconds; health when it changes;
//!    the Galera writer set when it changes
//! 6. probing records left by a crashed registration, and rejected ones,
//!    pass the registration gates again
//! 7. again after cluster.health_interval_seconds
//! ```
//!
//! Every write is guarded by the DC leader's fencing token. The state kept
//! per cluster (publisher hysteresis, failure counts, the Galera writer
//! tracker) is in memory only; another controller that takes over starts
//! from the published record.
//!
//! Heartbeats are written once per pass, before probing, rather than every
//! `cluster.heartbeat_interval_ms`. Not yet: tuning (chapter 09 step 10).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sprig_cluster::driver::{BasicDriver, GaleraDriver, NodeConnector, TopologyDriver};
use sprig_cluster::health::{ClusterHealth, HealthReport, HeartbeatWriter};
use sprig_cluster::probe::{NodeProbe, ProbeDepth, Prober};
use sprig_cluster::profile::{
    CapabilityPublisher, Direction, FieldChange, full_probe_triggers, registration_gaps,
};
use sprig_cluster::record::{AdminState, NodeSpec, SecretRefs, TopologyKind, rfc3339};
use sprig_cluster::registry::{ClusterRegistry, StoredCluster};
use sprig_cluster::settings::ClusterSettings;
use sprig_cluster::sql::{NodeEndpoint, ProbeError, ProbeSession};
use sprig_common::Result;
use sprig_common::ids::ClusterId;
use sprig_common::time::Clock;
use sprig_meta::Cmp;
use sprig_provision::credentials::CredentialConnector;
use sprig_settings::{LiveSettings, SettingCtx};

use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler, WatchSpec};

/// Deep probes run at least this often.
pub const DEEP_PROBE_PERIOD: Duration = Duration::from_secs(24 * 3600);
/// How long a `probing` record is left to the registration that wrote it
/// before the loop finishes the registration itself.
pub const PROBING_GRACE: Duration = Duration::from_secs(30);

/// A driver, keeping the concrete type for its extra methods.
#[derive(Clone)]
enum Driver {
    Basic(Arc<BasicDriver>),
    Galera(Arc<GaleraDriver>),
}

impl Driver {
    fn new(kind: TopologyKind, connector: Arc<dyn NodeConnector>, s: ClusterSettings) -> Self {
        match kind {
            TopologyKind::Galera => Driver::Galera(Arc::new(GaleraDriver::new(connector, s))),
            other => Driver::Basic(Arc::new(BasicDriver::new(other, connector, s))),
        }
    }

    fn dyn_driver(&self) -> &dyn TopologyDriver {
        match self {
            Driver::Basic(d) => &**d,
            Driver::Galera(d) => &**d,
        }
    }

    fn report(&self) -> Option<HealthReport> {
        match self {
            Driver::Basic(d) => d.last_report(),
            Driver::Galera(d) => d.last_report(),
        }
    }
}

/// What the loop keeps about one cluster during a term.
#[derive(Default)]
struct ClusterState {
    driver: Option<(Driver, TopologyKind, ClusterSettings, String)>,
    publisher: Option<CapabilityPublisher>,
    last_probes: Vec<NodeProbe>,
    last_full: Option<Instant>,
    last_deep: Option<Instant>,
    need_deep: bool,
    probing_since: Option<Instant>,
    last_pass: Option<Instant>,
    heartbeat: Option<(sprig_common::ids::NodeId, HeartbeatWriter)>,
}

/// What one pass did.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PassReport {
    /// The record after the pass.
    #[serde(skip)]
    pub cluster: Option<StoredCluster>,
    /// How deep it probed; `None` when it only waited.
    pub depth: Option<ProbeDepth>,
    /// The health assessment.
    pub health: Option<HealthReport>,
    /// What the published profile changed, if it changed.
    pub changes: Vec<FieldChange>,
    /// Upgrades seen but held for the sustain period.
    pub held: Vec<String>,
    /// Why the next pass goes deep, if it does.
    pub deep_next: Vec<String>,
    /// When to look again.
    #[serde(skip)]
    pub next: Duration,
}

/// Probes clusters and publishes what it finds. Shared by the `registry`
/// loop and the Admin API's `probe` call.
pub struct ClusterMonitor {
    registry: ClusterRegistry,
    live: LiveSettings,
    clock: Arc<dyn Clock>,
    states: Mutex<HashMap<ClusterId, Arc<tokio::sync::Mutex<ClusterState>>>>,
}

impl std::fmt::Debug for ClusterMonitor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterMonitor")
            .field("registry", &self.registry)
            .finish_non_exhaustive()
    }
}

fn health_value(h: ClusterHealth) -> f64 {
    match h {
        ClusterHealth::Healthy => 0.0,
        ClusterHealth::Degraded => 1.0,
        ClusterHealth::NoWriter => 2.0,
        ClusterHealth::Unknown => 3.0,
    }
}

impl ClusterMonitor {
    /// A monitor over `registry` (unguarded: each pass adds its guard),
    /// resolving settings from `live`.
    #[must_use]
    pub fn new(registry: ClusterRegistry, live: LiveSettings, clock: Arc<dyn Clock>) -> Self {
        Self {
            registry,
            live,
            clock,
            states: Mutex::new(HashMap::new()),
        }
    }

    /// The registry, unguarded.
    #[must_use]
    pub fn registry(&self) -> &ClusterRegistry {
        &self.registry
    }

    /// The settings of `id`, resolved now.
    #[must_use]
    pub fn settings_for(&self, id: Option<&ClusterId>) -> ClusterSettings {
        let dc = self.registry.dc().clone();
        let ctx = SettingCtx {
            cluster: id,
            ..SettingCtx::process(&dc)
        };
        ClusterSettings::resolve(&self.live.snapshot(), &ctx)
    }

    fn states(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<ClusterId, Arc<tokio::sync::Mutex<ClusterState>>>> {
        self.states.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn cell(&self, id: &ClusterId) -> Arc<tokio::sync::Mutex<ClusterState>> {
        Arc::clone(self.states().entry(id.clone()).or_default())
    }

    /// Whether this term knows `id` (its passes are scheduled already).
    #[must_use]
    pub fn knows(&self, id: &ClusterId) -> bool {
        self.states().contains_key(id)
    }

    /// Drops everything kept about `id`.
    pub fn forget(&self, id: &ClusterId) {
        self.states().remove(id);
    }

    /// Drops everything: a new term starts from the records.
    pub fn reset(&self) {
        self.states().clear();
    }

    /// The last health assessment of `id` in this term.
    pub async fn last_report(&self, id: &ClusterId) -> Option<HealthReport> {
        let cell = self.states().get(id).cloned()?;
        let st = cell.lock().await;
        st.driver.as_ref().and_then(|(d, ..)| d.report())
    }

    /// Takes over a cluster a registration just published: its deep probes
    /// count, so the first pass is a light one.
    pub async fn adopt(&self, stored: &StoredCluster, probes: Vec<NodeProbe>) {
        let cell = self.cell(&stored.body().cluster_id);
        let mut st = cell.lock().await;
        let settings = self.settings_for(Some(&stored.body().cluster_id));
        let now = self.clock.now();
        st.publisher = Some(CapabilityPublisher::new(
            stored.body().profile.clone(),
            settings.capability_upgrade_sustain,
        ));
        st.last_probes = probes;
        st.last_full = Some(now);
        st.last_deep = Some(now);
        st.last_pass = Some(now);
    }

    /// One pass over `id`: probe at `force` depth or the scheduled one,
    /// publish, and say when to look again. Writes carry `guard`. `None` if
    /// the cluster is gone or retired.
    ///
    /// # Errors
    /// 39504 if the guard no longer holds; 39500 if etcd is unreachable.
    #[allow(clippy::too_many_lines)] // the pass, step by step as in the module docs
    pub async fn pass(
        &self,
        id: &ClusterId,
        force: Option<ProbeDepth>,
        guard: Vec<Cmp>,
    ) -> Result<Option<PassReport>> {
        let registry = self.registry.clone().with_guard(guard);
        let Some(stored) = registry.get(id).await? else {
            self.forget(id);
            return Ok(None);
        };
        let rec = stored.body().clone();
        if rec.admin_state == AdminState::Retired {
            self.forget(id);
            return Ok(None);
        }
        let settings = self.settings_for(Some(id));
        let cell = self.cell(id);
        let mut st = cell.lock().await;
        let now = self.clock.now();

        // Record events wake the loop too; a cluster probed less than
        // one interval ago waits for its turn.
        if force.is_none()
            && let Some(last) = st.last_pass
        {
            let since = now.saturating_duration_since(last);
            if since < settings.health_interval {
                return Ok(Some(PassReport {
                    cluster: Some(stored),
                    depth: None,
                    health: None,
                    changes: Vec::new(),
                    held: Vec::new(),
                    deep_next: Vec::new(),
                    next: settings.health_interval.saturating_sub(since),
                }));
            }
        }

        // A registration in flight owns a probing record for a while.
        if rec.admin_state == AdminState::Probing && force.is_none() {
            let since = *st.probing_since.get_or_insert(now);
            let waited = now.saturating_duration_since(since);
            if waited < PROBING_GRACE {
                return Ok(Some(PassReport {
                    cluster: Some(stored),
                    depth: None,
                    health: None,
                    changes: Vec::new(),
                    held: Vec::new(),
                    deep_next: Vec::new(),
                    next: PROBING_GRACE.saturating_sub(waited),
                }));
            }
        }

        // The driver follows the record's topology, secrets and settings.
        let secrets = format!("{}|{}", rec.secrets.ctl, rec.secrets.repl);
        let stale = st.driver.as_ref().is_none_or(|(_, kind, s, sec)| {
            *kind != rec.topology || *s != settings || *sec != secrets
        });
        if stale {
            let connector: Arc<dyn NodeConnector> =
                Arc::new(CredentialConnector::ctl(&rec.secrets));
            st.driver = Some((
                Driver::new(rec.topology, connector, settings.clone()),
                rec.topology,
                settings.clone(),
                secrets,
            ));
        }
        let Some((driver, ..)) = st.driver.clone() else {
            return Ok(None);
        };
        // A profile with no server facts came from a pass nothing answered.
        let has_facts = rec
            .profile
            .as_ref()
            .is_some_and(|p| p.server.version.is_some());
        // The publisher starts from the record, and restarts if someone
        // else published a newer profile.
        let published_version = st
            .publisher
            .as_ref()
            .and_then(|p| p.published().map(|p| p.version));
        if st.publisher.is_none()
            || (rec.profile_version > published_version.unwrap_or(0) && rec.profile.is_some())
        {
            st.publisher = Some(CapabilityPublisher::new(
                rec.profile.clone(),
                settings.capability_upgrade_sustain,
            ));
            if has_facts && st.last_deep.is_none() {
                // Deep facts were carried in the record; refresh them on
                // the daily schedule.
                st.last_deep = Some(now);
            }
        }
        if let Some(p) = st.publisher.as_mut() {
            p.set_sustain(settings.capability_upgrade_sustain);
        }

        let since = |t: Option<Instant>| t.map(|t| now.saturating_duration_since(t));
        let depth = force.unwrap_or_else(|| {
            if st.need_deep
                || !has_facts
                || since(st.last_deep).is_none_or(|d| d >= DEEP_PROBE_PERIOD)
            {
                ProbeDepth::Deep
            } else if matches!(rec.admin_state, AdminState::Probing | AdminState::Rejected)
                || since(st.last_full).is_none_or(|d| d >= settings.probe_interval)
            {
                ProbeDepth::Full
            } else {
                ProbeDepth::Light
            }
        });

        let d = driver.dyn_driver();
        d.observe(&rec);
        let nodes = d.discover(&rec).await?;
        let mut eps = Vec::new();
        // A deep probe also runs the binlog dump probe as sprig_repl, as
        // registration does (chapter 09, "Binlog dump"); without it a
        // registration-time "feed unsupported" would lapse into the
        // settings' verdict after the upgrade sustain period.
        let repl = repl_endpoints(id, &rec.secrets, &nodes, depth);
        let mut repl_eps = repl.as_ref().map(|_| Vec::new());
        let mut failed: Vec<(usize, NodeProbe)> = Vec::new();
        for (i, n) in nodes.iter().enumerate() {
            match CredentialConnector::ctl(&rec.secrets).endpoint(n) {
                Ok(ep) => {
                    // Keep sprig_repl's list in the order of `eps`.
                    if let (Some(r), Some(all)) = (repl_eps.as_mut(), repl.as_ref()) {
                        r.push(all[i].clone());
                    }
                    eps.push(ep);
                }
                Err(e) => failed.push((
                    i,
                    NodeProbe {
                        node: n.node.clone(),
                        depth,
                        at_unix_ms: 0,
                        took_ms: 0,
                        result: Err(ProbeError {
                            errno: None,
                            message: e.message,
                            timeout: false,
                        }),
                        active: None,
                    },
                )),
            }
        }
        if rec.topology != TopologyKind::Galera {
            beat(&mut st, &eps, settings.probe_timeout).await;
        }
        let started = Instant::now();
        let mut probes = Prober::with_clock(settings.probe_timeout, Arc::clone(&self.clock))
            .probe_cluster_with_repl(rec.topology, &eps, repl_eps.as_deref(), depth)
            .await;
        for (i, p) in failed {
            probes.insert(i.min(probes.len()), p);
        }
        let labels = [
            ("dc", self.registry.dc().to_string()),
            ("cluster", id.to_string()),
        ];
        metrics::histogram!("sprig_cluster_probe_duration_seconds", &labels)
            .record(started.elapsed().as_secs_f64());
        for p in probes.iter().filter(|p| p.result.is_err()) {
            metrics::counter!("sprig_cluster_probe_failures_total", "dc" => labels[0].1.clone(),
                "cluster" => labels[1].1.clone(), "node" => p.node.to_string())
            .increment(1);
        }

        let deep_next = full_probe_triggers(&st.last_probes, &probes);
        if !deep_next.is_empty() {
            tracing::info!(cluster = %id, triggers = ?deep_next, "a deep probe follows");
        }
        st.need_deep = !deep_next.is_empty();
        let candidate = d.derive(&probes, rec.declared.as_ref());
        let publisher = st
            .publisher
            .as_mut()
            .ok_or_else(|| sprig_common::SprigError::internal("no publisher"))?;
        let publication = publisher.observe(candidate, now);
        let held: Vec<String> = publisher
            .pending()
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        let current = publisher.published().cloned();
        let mut stored = stored;
        let mut changes = Vec::new();
        if let Some(p) = publication {
            for c in &p.changes {
                let direction = match c.direction {
                    Direction::Upgrade => "upgrade",
                    Direction::Downgrade => "downgrade",
                    Direction::Changed => "changed",
                };
                metrics::counter!("sprig_cluster_capability_changes_total",
                    "dc" => labels[0].1.clone(), "cluster" => labels[1].1.clone(),
                    "field" => c.field.clone(), "direction" => direction)
                .increment(1);
            }
            if !p.changes.is_empty() {
                tracing::info!(cluster = %id, version = p.profile.version,
                    changes = ?p.changes, "capability profile changed");
            }
            changes.clone_from(&p.changes);
            stored = registry.publish_profile(id, &p).await?;
        }
        let profile = current.unwrap_or_default();
        let health = d.health(&probes, &profile);
        let report = driver.report();
        metrics::gauge!("sprig_cluster_health_state", &labels).set(health_value(health));
        metrics::gauge!("sprig_cluster_semi_sync_status", &labels)
            .set(if profile.semi_sync.active { 1.0 } else { 0.0 });
        if let Driver::Galera(g) = &driver
            && let Some(ws) = g.plan_writer_set(now)
        {
            tracing::info!(cluster = %id, epoch = ws.writer_epoch, "publishing the Galera writer set");
            stored = registry
                .update(id, |r| {
                    r.galera = Some(ws.clone());
                    Ok(true)
                })
                .await?;
        }
        if stored.body().health.as_ref().map(|h| h.state) != Some(health) {
            stored = registry
                .publish_health(id, health, rfc3339(self.clock.wall()))
                .await?;
        }

        // Registration gates, for probing (crash recovery) and rejected
        // records; they need the grants, so a full probe at least.
        if matches!(
            stored.body().admin_state,
            AdminState::Probing | AdminState::Rejected
        ) && depth >= ProbeDepth::Full
        {
            let gates = registration_gaps(&probes, &profile, settings.lower_case_table_names);
            let (to, gaps) = if gates.blocking.is_empty() {
                (AdminState::Active, gates.advisories)
            } else {
                (AdminState::Rejected, gates.blocking)
            };
            if stored.body().admin_state != to || stored.body().gaps != gaps {
                tracing::info!(cluster = %id, state = %to, ?gaps, "registration gates");
                stored = registry.set_admin_state(id, to, gaps).await?;
            }
        }

        // A pass counts only if something answered it.
        if depth >= ProbeDepth::Full && probes.iter().any(|p| p.facts().is_some()) {
            st.last_full = Some(now);
        }
        if depth == ProbeDepth::Deep && probes.iter().any(|p| p.active.is_some()) {
            st.last_deep = Some(now);
        }
        st.last_probes = probes;
        st.probing_since = None;
        st.last_pass = Some(now);
        Ok(Some(PassReport {
            cluster: Some(stored),
            depth: Some(depth),
            health: report,
            changes,
            held,
            deep_next,
            next: settings.health_interval,
        }))
    }
}

/// Writes the writer's heartbeat before a pass, so replicas' lag is read
/// from a fresh row (chapter 09, "Internal schema"). The writer is the one
/// the last pass saw; nothing is written before a pass has seen replicas.
/// Every node's `sprig_repl` endpoint, in the order of `nodes`, for the
/// binlog dump probe of a deep probe; `None` at other depths, or (with a
/// warning) when the credential doesn't resolve: the dump probe then
/// doesn't run and the feed settings decide, as at registration.
fn repl_endpoints(
    cluster: &ClusterId,
    secrets: &SecretRefs,
    nodes: &[NodeSpec],
    depth: ProbeDepth,
) -> Option<Vec<NodeEndpoint>> {
    if depth != ProbeDepth::Deep {
        return None;
    }
    let repl = CredentialConnector::repl(secrets);
    match nodes.iter().map(|n| repl.endpoint(n)).collect() {
        Ok(eps) => Some(eps),
        Err(e) => {
            tracing::warn!(cluster = %cluster, error = %e.message,
                "sprig_repl credential doesn't resolve; the binlog dump probe is skipped");
            None
        }
    }
}

async fn beat(st: &mut ClusterState, eps: &[NodeEndpoint], timeout: Duration) {
    if !st.last_probes.iter().any(|p| {
        p.facts()
            .is_some_and(sprig_cluster::probe::NodeFacts::is_replica)
    }) {
        return;
    }
    let Some(writer) = st
        .last_probes
        .iter()
        .find(|p| p.facts().is_some_and(|f| !f.is_replica() && f.writable()))
        .map(|p| p.node.clone())
    else {
        return;
    };
    let Some(ep) = eps.iter().find(|e| e.node == writer) else {
        return;
    };
    if st.heartbeat.as_ref().is_none_or(|(n, _)| n != &writer) {
        st.heartbeat = Some((writer.clone(), HeartbeatWriter::new(writer.clone(), 0)));
    }
    let Some((_, hb)) = st.heartbeat.as_mut() else {
        return;
    };
    let r = match ProbeSession::connect(ep, timeout).await {
        Ok(mut s) => {
            let r = hb.beat(&mut s).await;
            s.close().await;
            r
        }
        Err(e) => Err(e),
    };
    if let Err(e) = r {
        tracing::debug!(node = %writer, error = %e, "heartbeat failed");
        st.heartbeat = None;
    }
}

/// The `registry` loop (chapter 23's loop table): one key per cluster of
/// this DC, on the DC leader.
#[derive(Debug, Clone)]
pub struct RegistryLoop {
    monitor: Arc<ClusterMonitor>,
}

impl RegistryLoop {
    /// The loop over `monitor`.
    #[must_use]
    pub fn new(monitor: Arc<ClusterMonitor>) -> Self {
        Self { monitor }
    }
}

/// The guard every leader write carries: its leader key's create revision.
///
/// # Errors
/// 39504 when `cx` doesn't lead.
pub fn leader_guard(cx: &LoopCx) -> Result<Vec<Cmp>> {
    let l = cx.leader()?;
    Ok(vec![Cmp::CreateRev(l.key().clone(), l.token)])
}

#[async_trait::async_trait]
impl Reconciler for RegistryLoop {
    type Key = ClusterId;

    fn name(&self) -> &'static str {
        "registry"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn watches(&self) -> Vec<WatchSpec> {
        vec![WatchSpec::dc("clusters/")]
    }

    /// Every changed cluster; a pass right after another one only
    /// reschedules itself, so the loop's own writes cost no probes.
    fn keys_for(&self, ev: &MetaEvent) -> Vec<ClusterId> {
        ev.path()
            .strip_prefix("clusters/")
            .and_then(|s| ClusterId::new(s).ok())
            .into_iter()
            .collect()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<ClusterId>> {
        Ok(self
            .monitor
            .registry()
            .list()
            .await?
            .into_iter()
            .map(|c| c.body().cluster_id.clone())
            .collect())
    }

    async fn reconcile(&self, cx: &LoopCx, key: &ClusterId) -> Result<Outcome> {
        let guard = leader_guard(cx)?;
        Ok(match self.monitor.pass(key, None, guard).await? {
            None => Outcome::Done,
            Some(r) => Outcome::RequeueAfter(r.next),
        })
    }

    fn workers(&self) -> usize {
        8
    }
}

#[cfg(test)]
pub(crate) mod tests;
