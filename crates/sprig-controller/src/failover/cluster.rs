//! The `cluster_failover` loop (chapter 25, "Primary + replicas, driven by
//! SprigDB", "Galera: writer nodes and lost quorum", "Damping failover
//! flapping"): the DC leader replaces a lost writer inside one cluster,
//! with `sprig_cluster::failover`'s steps.
//!
//! ```text
//! pass(c), every 500 ms per active primary_replica or galera cluster:
//! primary + replicas
//! 1. writer = the node leases/writer/c names (else the one recorded)
//! 2. probe it (connect, the semi-sync sample); answered -> healthy; the former
//!    writer of the last failover is rejoined or reported (below)
//! 3. failed failover.cluster_detect_seconds -> the damper: sources are the
//!    probes (refused, not timed out) and the agent (lease handed back or
//!    mysqld down); hard = lease gone, no live agent or mysqld down, and the
//!    probe failed
//! 4. the writer lease must be gone (its agent fenced and released it, or
//!    its session ended), then failover.clock_drift_margin_ms
//! 5. failover.mode manual -> blocked (39512)
//! 6. drain the relay logs of replicas with agents; choose; expected loss;
//!    RPO -> blocked (39512) when over
//! 7. writer_epoch + 1: claim leases/writer/c for the chosen node (created
//!    only when missing, under the controller's own short lease)
//! 8. promote it (replication reset; read_only stays on: its agent takes
//!    the claimed lease over and clears it); repoint the other replicas
//! 9. the agent took the lease over -> done: record the failover
//! returning former writer (after 9, once it answers):
//!    nothing diverged -> rejoin as a replica (read_only stays on);
//!    diverged -> report into the record, keep it out, alert
//! galera
//! 1. probe every node; GaleraQuorum::observe; writers are re-picked by the
//!    registry loop's writer set
//! 2. bootstrap the most advanced node after failover.galera_quorum_wait_seconds
//!    only when every other node is confirmed down (no agent registration
//!    and unreachable); otherwise blocked
//! ```
//!
//! The state survives a change of DC leader in
//! `/sprig/dc/<dc>/cluster-failover/<cluster>` (written under the
//! leader's token): the writer, `writer_epoch`, the last failover's time
//! (the damper's interval), an open promotion, and the last 10 failovers
//! with their RTO, loss estimate and the former writer's fate.
//!
//! **Where it differs from the book.** The writer lease is created by the
//! controller for the chosen node and taken over by that node's agent (the
//! book binds it to the agent's session directly, which another process
//! can't do). Gateways don't report connect failures yet, so the sources
//! are the probes and the agent. The RPO is the cluster's
//! `failover.rpo_seconds` (dc and global scopes); tenant-scoped RPOs live
//! in catalogs and aren't read here yet. A diverged former writer stays out
//! until an operator rebuilds it: `node_rebuild` isn't built.

// s, w, f, c, p: the settings, the watch, the facts, the cluster, the
// promotion, in every step.
#![allow(clippy::many_single_char_names)]

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sprig_cluster::failover::damper::{Damp, DownSources, FlapDamper};
use sprig_cluster::failover::galera::{GaleraAction, GaleraQuorum, bootstrap, probe_node};
use sprig_cluster::failover::primary_replica::{
    Choice, ReplSource, ReplicaState, binlog_pos, check_rpo, choose, diverged, diverged_events,
    drain_relay, expected_loss, promote_fenced, rejoin, repoint, report_address,
};
use sprig_cluster::failover::writer::{
    AgentRecord, CONTROLLER_PREFIX, MysqldState, WriterLease, agents_prefix, writer_lease_key,
};
use sprig_cluster::record::{AdminState, ClusterRecord, ProvisionerKind, TopologyKind, rfc3339};
use sprig_cluster::registry::ClusterRegistry;
use sprig_cluster::sql::{NodeEndpoint, ProbeSession};
use sprig_common::ids::{ClusterId, DcId};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigError};
use sprig_meta::session::SessionOpts;
use sprig_meta::{Cmp, Consistency, Key, ListOpts, MetaStore, Op, Record, Session, Versioned};
use sprig_settings::{LiveSettings, SettingCtx};

use super::nodes::ctl_endpoints;
use super::record::ErrantReport;
use super::recovery::errant_report;
use super::settings as fs;
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler, WatchSpec};

/// A pass's period while the writer answers.
const HEALTHY_EVERY: Duration = Duration::from_millis(1000);
/// A pass's period while something is wrong.
const BUSY_EVERY: Duration = Duration::from_millis(500);
/// One probe of the writer.
const PROBE_TIMEOUT: Duration = Duration::from_millis(1500);
/// Failovers kept in the record.
const HISTORY: usize = 10;
/// How often a diverged former writer's recovery jobs are looked at.
const RECOVERY_EVERY: Duration = Duration::from_secs(3);
/// The promotion's claim is re-sent this long after the agent should have
/// taken it over, with an alert.
const TAKEOVER_ALERT: Duration = Duration::from_secs(30);

/// `cluster-failover/<cluster>` in `tier`.
///
/// # Errors
/// Internal for an id that isn't a key segment.
pub fn record_key(tier: &sprig_meta::Tier, cluster: &ClusterId) -> Result<Key> {
    tier.key(&format!("cluster-failover/{cluster}"))
}

fn unix_ms(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

// ---------------------------------------------------------------------------
// The record
// ---------------------------------------------------------------------------

/// What happened to the former writer after a failover.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FormerWriter {
    /// Not back yet: it stays fenced (`read_only`, no writer lease).
    Fenced,
    /// Back with nothing diverged: a replica of the new writer now.
    Rejoined,
    /// Back with transactions nobody else has: reported, kept out until
    /// it is extracted and rebuilt.
    Diverged,
    /// A `node_rebuild` job is replacing its data from a replica.
    Rebuilding,
    /// Rebuilt from a replica and replicating from the writer (r2close).
    Rebuilt,
}

/// One failover in the record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailoverEvent {
    /// `promote` (primary + replicas) or `galera_bootstrap`.
    pub kind: String,
    /// When it finished, RFC 3339 UTC.
    pub at: String,
    /// The same, Unix milliseconds.
    pub at_unix_ms: u64,
    /// The former writer (empty for a bootstrap).
    pub from: String,
    /// The new writer, or the bootstrapped node.
    pub to: String,
    /// The writer epoch it set.
    pub writer_epoch: u64,
    /// From the first failed probe to the decision.
    pub detect_ms: u64,
    /// From the first failed probe to the new writer's lease (the RTO, as
    /// SprigDB sees it; writes resume once the agent clears `read_only`).
    pub rto_ms: u64,
    /// The expected loss (1 s with semi-sync on; else the lag).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loss_estimate_ms: Option<u64>,
    /// The sources that agreed the writer was down.
    #[serde(default)]
    pub sources: Vec<String>,
    /// The new writer's applied position when promoted.
    #[serde(default)]
    pub applied: String,
    /// The cluster's GTID domain.
    #[serde(default)]
    pub domain: u32,
    /// The former writer's fate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub former: Option<FormerWriter>,
    /// Its diverged transactions, when it had any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub errant: Option<ErrantReport>,
    /// The `errant_extract` job of a diverged former writer (r2close).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extract_job: Option<String>,
    /// Its `node_rebuild` job.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rebuild_job: Option<String>,
    /// Why the last extract or rebuild failed (an operator retries with
    /// `sprigctl cluster extract|rebuild`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_error: Option<String>,
}

/// A promotion between the lease claim and the agent's takeover.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenPromotion {
    /// The former writer.
    pub from: String,
    /// The chosen node.
    pub to: String,
    /// The writer epoch claimed.
    pub writer_epoch: u64,
    /// Its applied position.
    pub applied: String,
    /// The cluster's GTID domain.
    pub domain: u32,
    /// The other replicas, repointed at `to`.
    pub others: Vec<String>,
    /// Switch semi-sync's primary side on at `to`.
    pub semisync: bool,
    /// From the first failed probe to the decision.
    pub detect_ms: u64,
    /// When the writer first failed, Unix ms (for the RTO).
    pub lost_at_unix_ms: u64,
    /// The expected loss.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loss_estimate_ms: Option<u64>,
    /// The sources.
    #[serde(default)]
    pub sources: Vec<String>,
}

/// `/sprig/dc/<dc>/cluster-failover/<cluster>`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ClusterFailoverRecord {
    /// The cluster.
    pub cluster_id: String,
    /// `healthy`, `writer_lost`, `suppressed`, `promoting`, `quorum_lost`
    /// or `blocked`.
    pub state: String,
    /// Why (blocked, suppressed, waiting).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The writer the loop watches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer: Option<String>,
    /// Raised by every failover.
    #[serde(default)]
    pub writer_epoch: u64,
    /// The last automatic failover, Unix ms (the damper's interval).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failover_unix_ms: Option<u64>,
    /// An open promotion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promotion: Option<OpenPromotion>,
    /// The last failovers, oldest first.
    #[serde(default)]
    pub history: Vec<FailoverEvent>,
    /// Who decided the writer: `sprig failover`, `observed:
    /// mariadb-operator` or `observed: provider` (r2opfix).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

impl Versioned for ClusterFailoverRecord {
    const V: u32 = 1;
}

// ---------------------------------------------------------------------------
// The decision (pure, for tests)
// ---------------------------------------------------------------------------

/// What the probe of the writer saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Probe {
    /// It answered; the semi-sync sample.
    Up {
        /// `Rpl_semi_sync_master_status = ON`.
        semisync: bool,
    },
    /// Connection refused (or reset): mysqld isn't running.
    Refused,
    /// No answer in time: slow, paused, or the host is gone.
    TimedOut,
}

/// The writer lease as the loop sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseView {
    /// Missing, for this long.
    Gone(Duration),
    /// Held by the writer's own agent.
    Writer,
    /// Held for another node (a promotion in flight, or a node that took
    /// it).
    Other(String),
}

/// The writer's agent as its registration says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentView {
    /// No registration (none ever, or its session ended).
    Missing,
    /// Registered; what it says of mysqld.
    Registered(MysqldState),
}

/// The settings one pass uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterSettings {
    /// `failover.mode = manual` (cluster, dc or global).
    pub manual: bool,
    /// `failover.rpo_seconds` (dc, global).
    pub rpo: Duration,
    /// `failover.cluster_detect_seconds`.
    pub detect: Duration,
    /// `failover.cluster_detect_sources`.
    pub sources: u32,
    /// `failover.min_interval_minutes`.
    pub min_interval: Duration,
    /// `failover.clock_drift_margin_ms`.
    pub drift: Duration,
    /// `failover.writer_lease_seconds`.
    pub writer_lease: Duration,
    /// `failover.galera_quorum_wait_seconds`.
    pub quorum_wait: Duration,
}

impl ClusterSettings {
    /// Resolves the keys for `cluster` in `dc`.
    #[must_use]
    pub fn resolve(live: Option<&LiveSettings>, dc: &DcId, cluster: &ClusterId) -> Self {
        let Some(l) = live else {
            return Self::defaults();
        };
        let snap = l.snapshot();
        let cx = SettingCtx::process(dc).with_cluster(cluster);
        let pcx = SettingCtx::process(dc);
        Self {
            manual: snap.get(&fs::MODE, &cx) == "manual",
            rpo: Duration::from_secs(snap.get(&fs::RPO_SECONDS, &pcx)),
            detect: Duration::from_secs(snap.get(&fs::CLUSTER_DETECT_SECONDS, &cx)),
            sources: snap.get(&fs::CLUSTER_DETECT_SOURCES, &cx),
            min_interval: Duration::from_secs(snap.get(&fs::MIN_INTERVAL_MINUTES, &cx) * 60),
            drift: Duration::from_millis(snap.get(&fs::CLOCK_DRIFT_MARGIN_MS, &pcx)),
            writer_lease: Duration::from_secs(snap.get(&fs::WRITER_LEASE_SECONDS, &cx)),
            quorum_wait: Duration::from_secs(snap.get(&fs::GALERA_QUORUM_WAIT_SECONDS, &cx)),
        }
    }

    /// The book's defaults.
    #[must_use]
    pub fn defaults() -> Self {
        Self {
            manual: false,
            rpo: Duration::from_secs(fs::RPO_SECONDS.default),
            detect: Duration::from_secs(fs::CLUSTER_DETECT_SECONDS.default),
            sources: fs::CLUSTER_DETECT_SOURCES.default,
            min_interval: Duration::from_secs(fs::MIN_INTERVAL_MINUTES.default * 60),
            drift: Duration::from_millis(fs::CLOCK_DRIFT_MARGIN_MS.default),
            writer_lease: Duration::from_secs(fs::WRITER_LEASE_SECONDS.default),
            quorum_wait: Duration::from_secs(fs::GALERA_QUORUM_WAIT_SECONDS.default),
        }
    }
}

/// What a pass observed about the writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observation {
    /// The probe.
    pub probe: Probe,
    /// How long probes have failed (zero when it answered).
    pub failing_for: Duration,
    /// The writer lease.
    pub lease: LeaseView,
    /// The writer's agent.
    pub agent: AgentView,
    /// Since the last automatic failover, if any.
    pub since_last: Option<Duration>,
}

/// What the pass does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// The writer answers.
    Healthy,
    /// Not enough evidence yet (detect window, sources, the lease).
    Wait(String),
    /// Within the minimum interval and not a hard failure.
    Suppress(String),
    /// Stop and alert (39512).
    Blocked(String),
    /// Choose and promote.
    FailOver {
        /// The sources that agreed.
        sources: Vec<String>,
    },
}

/// Steps 2-5 of the pass (see the module docs).
#[must_use]
pub fn decide(o: &Observation, s: &ClusterSettings) -> Step {
    if matches!(o.probe, Probe::Up { .. }) {
        return Step::Healthy;
    }
    if o.failing_for < s.detect {
        return Step::Wait(format!(
            "the writer has failed probes for {} ms of failover.cluster_detect_seconds",
            o.failing_for.as_millis()
        ));
    }
    let lease_gone = matches!(o.lease, LeaseView::Gone(_));
    let agent_down = matches!(o.agent, AgentView::Registered(MysqldState::Down));
    let src = DownSources {
        probes_refused: o.probe == Probe::Refused,
        probes_timed_out: o.probe == Probe::TimedOut,
        agent: lease_gone || agent_down,
        gateways: false,
        hard: lease_gone && (o.agent == AgentView::Missing || agent_down),
    };
    let mut damper = FlapDamper::new(s.sources, s.min_interval);
    let now = Instant::now();
    if let Some(since) = o.since_last
        && let Some(last) = now.checked_sub(since)
    {
        damper.record(last);
    }
    match damper.may_fail_over(&src, now) {
        Damp::Wait(why) => return Step::Wait(why),
        Damp::Suppress(why) => return Step::Suppress(why),
        Damp::Proceed => {}
    }
    // The old writer is fenced first: its agent handed the lease back (or
    // its session ended), then the drift margin.
    match &o.lease {
        LeaseView::Writer => {
            return Step::Wait("the writer's agent still holds the writer lease".into());
        }
        LeaseView::Other(n) => {
            return Step::Blocked(format!(
                "the writer lease is held for {n}, not the writer the loop watches"
            ));
        }
        LeaseView::Gone(for_) if *for_ < s.drift => {
            return Step::Wait("the drift margin after the writer lease went".into());
        }
        LeaseView::Gone(_) => {}
    }
    if s.manual {
        return Step::Blocked("failover.mode is manual".into());
    }
    let mut sources = Vec::new();
    if src.probes_refused {
        sources.push("probes".to_owned());
    }
    if src.agent {
        sources.push("agent".to_owned());
    }
    if src.hard {
        sources.push("hard".to_owned());
    }
    Step::FailOver { sources }
}

// ---------------------------------------------------------------------------
// The loop
// ---------------------------------------------------------------------------

/// Per cluster, in memory (the record has the rest).
#[derive(Default)]
struct Watch {
    /// The leader token this state belongs to.
    token: u64,
    failing_since: Option<Instant>,
    lease_gone_since: Option<Instant>,
    semisync: bool,
    quorum: GaleraQuorum,
    /// The controller's claim of the writer lease while promoting.
    claim: Option<Session>,
    claimed_at: Option<Instant>,
    last_reason: Option<String>,
    /// An observed writer change and since when (r2opfix).
    observed: Option<(String, Instant)>,
    /// The last look at a diverged former writer's recovery jobs (r2close).
    recovery_checked: Option<Instant>,
}

/// The `cluster_failover` loop (see the module docs).
pub struct ClusterFailover {
    dc: DcId,
    node: String,
    clock: Arc<dyn Clock>,
    live: Option<LiveSettings>,
    watches: Mutex<HashMap<ClusterId, Arc<tokio::sync::Mutex<Watch>>>>,
}

impl std::fmt::Debug for ClusterFailover {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterFailover")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

/// What the loop reads about one cluster each pass.
struct Facts {
    lease: Option<(WriterLease, u64)>,
    agents: BTreeMap<String, AgentRecord>,
    record: ClusterFailoverRecord,
    record_rev: u64,
}

fn node_err(e: impl std::fmt::Display) -> SprigError {
    SprigError::internal(e.to_string())
}

impl ClusterFailover {
    /// The loop for controller `node` of `dc`.
    #[must_use]
    pub fn new(dc: DcId, node: String, clock: Arc<dyn Clock>, live: Option<LiveSettings>) -> Self {
        Self {
            dc,
            node,
            clock,
            live,
            watches: Mutex::new(HashMap::new()),
        }
    }

    fn watch(&self, c: &ClusterId) -> Arc<tokio::sync::Mutex<Watch>> {
        Arc::clone(
            self.watches
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(c.clone())
                .or_default(),
        )
    }

    async fn facts(&self, dc: &dyn MetaStore, c: &ClusterId) -> Result<Facts> {
        let lease = match dc
            .get(&writer_lease_key(dc.tier(), c)?, Consistency::Linearizable)
            .await?
        {
            Some(kv) => Record::<WriterLease>::decode(&kv.value)
                .ok()
                .map(|r| (r.body, kv.mod_revision)),
            None => None,
        };
        let page = dc
            .list(&agents_prefix(dc.tier())?, ListOpts::default())
            .await?;
        let agents = page
            .kvs
            .iter()
            .filter_map(|kv| Record::<AgentRecord>::decode(&kv.value).ok())
            .filter(|r| r.body.cluster == c.as_str())
            .map(|r| (r.body.node.clone(), r.body))
            .collect();
        let (record, record_rev) = match dc
            .get(&record_key(dc.tier(), c)?, Consistency::Linearizable)
            .await?
        {
            Some(kv) => (
                Record::<ClusterFailoverRecord>::decode(&kv.value)?.body,
                kv.mod_revision,
            ),
            None => (
                ClusterFailoverRecord {
                    cluster_id: c.to_string(),
                    state: "healthy".into(),
                    ..ClusterFailoverRecord::default()
                },
                0,
            ),
        };
        Ok(Facts {
            lease,
            agents,
            record,
            record_rev,
        })
    }

    /// Writes the record under the leader's token, compared on the
    /// revision read. Returns the new revision, or `None` when it changed.
    async fn save(
        &self,
        cx: &LoopCx,
        c: &ClusterId,
        rec: &ClusterFailoverRecord,
        rev: u64,
    ) -> Result<Option<u64>> {
        let key = record_key(cx.dc.tier(), c)?;
        let when = if rev == 0 {
            Cmp::Missing(key.clone())
        } else {
            Cmp::ModRev(key.clone(), rev)
        };
        let r = cx
            .leader()?
            .commit_if(
                vec![when],
                vec![Op::Put {
                    key,
                    value: Record::new(rec.clone()).encode()?,
                    lease: None,
                }],
            )
            .await?;
        Ok(r.succeeded.then_some(r.revision))
    }

    /// Sets state and reason, writing only a change.
    async fn note(
        &self,
        cx: &LoopCx,
        c: &ClusterId,
        f: &mut Facts,
        state: &str,
        reason: Option<String>,
    ) -> Result<()> {
        if f.record.state == state && f.record.reason == reason {
            return Ok(());
        }
        state.clone_into(&mut f.record.state);
        f.record.reason = reason;
        if let Some(rev) = self.save(cx, c, &f.record, f.record_rev).await? {
            f.record_rev = rev;
        }
        Ok(())
    }

    async fn pass(&self, cx: &LoopCx, c: &ClusterId) -> Result<Outcome> {
        let token = cx.leader()?.token;
        let reg = ClusterRegistry::new(Arc::clone(&cx.global), Arc::clone(&cx.dc))?;
        let Some(stored) = reg.get(c).await? else {
            self.watches
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(c);
            return Ok(Outcome::Done);
        };
        let rec = stored.body().clone();
        if rec.admin_state != AdminState::Active {
            return Ok(Outcome::RequeueAfter(Duration::from_secs(5)));
        }
        let observed_source = super::observed::observed_source(&rec);
        if observed_source.is_none() && rec.provisioner == ProvisionerKind::Kubernetes {
            return Ok(Outcome::RequeueAfter(Duration::from_secs(5)));
        }
        let w = self.watch(c);
        let mut w = w.lock().await;
        if w.token != token {
            // A new term: start from the record, not from memory.
            if let Some(s) = w.claim.take() {
                let _ = s.close().await;
            }
            *w = Watch {
                token,
                ..Watch::default()
            };
        }
        if let Some(source) = observed_source {
            return self.observed_writer(cx, &rec, source, &mut w).await;
        }
        let s = ClusterSettings::resolve(self.live.as_ref(), &self.dc, c);
        match rec.topology {
            TopologyKind::PrimaryReplica => self.primary_replica(cx, &rec, &s, &mut w).await,
            TopologyKind::Galera => self.galera(cx, &rec, &s, &mut w).await,
            _ => Ok(Outcome::RequeueAfter(Duration::from_secs(30))),
        }
    }

    // ---- writers someone else fails over (r2opfix) -------------------------

    /// Publishes the writer of a cluster someone else fails over
    /// ([`super::observed`]).
    async fn observed_writer(
        &self,
        cx: &LoopCx,
        rec: &ClusterRecord,
        source: &str,
        w: &mut Watch,
    ) -> Result<Outcome> {
        use super::observed::{Observed, decide, observe};
        let c = &rec.cluster_id;
        let mut f = self.facts(&*cx.dc, c).await?;
        let eps = ctl_endpoints(rec)?;
        let obs = futures::future::join_all(eps.iter().map(observe)).await;
        let settle =
            self.live
                .as_ref()
                .map_or(Duration::from_millis(fs::OBSERVE_SETTLE_MS.default), |l| {
                    Duration::from_millis(l.snapshot().get(
                        &fs::OBSERVE_SETTLE_MS,
                        &SettingCtx::process(&self.dc).with_cluster(c),
                    ))
                });
        match decide(f.record.writer.as_deref(), &obs) {
            Observed::Keep(_) => {
                w.observed = None;
                self.note(cx, c, &mut f, "healthy", None).await?;
                Ok(Outcome::RequeueAfter(HEALTHY_EVERY))
            }
            Observed::Candidate(x) => {
                let now = self.clock.now();
                let since = match &w.observed {
                    Some((n, t)) if *n == x => *t,
                    _ => {
                        w.observed = Some((x.clone(), now));
                        now
                    }
                };
                let held = now.saturating_duration_since(since);
                if held < settle {
                    let why = format!("{x} is the only writable node; settling ({} ms)", ms(held));
                    self.note(cx, c, &mut f, "writer_changing", Some(why))
                        .await?;
                    return Ok(Outcome::RequeueAfter(BUSY_EVERY));
                }
                let wall = self.clock.wall();
                let from = f.record.writer.clone().unwrap_or_default();
                let epoch = f.record.writer_epoch + 1;
                f.record.history.push(FailoverEvent {
                    kind: "observed".into(),
                    at: rfc3339(wall),
                    at_unix_ms: unix_ms(wall),
                    from: from.clone(),
                    to: x.clone(),
                    writer_epoch: epoch,
                    detect_ms: ms(held),
                    rto_ms: ms(held),
                    loss_estimate_ms: None,
                    sources: vec![source.to_owned()],
                    applied: String::new(),
                    domain: rec.cluster_number,
                    former: None,
                    errant: None,
                    extract_job: None,
                    rebuild_job: None,
                    recovery_error: None,
                });
                if f.record.history.len() > HISTORY {
                    let drop_n = f.record.history.len() - HISTORY;
                    f.record.history.drain(..drop_n);
                }
                f.record.writer = Some(x.clone());
                f.record.writer_epoch = epoch;
                f.record.source = Some(source.to_owned());
                f.record.state = "healthy".into();
                f.record.reason = None;
                if let Some(rev) = self.save(cx, c, &f.record, f.record_rev).await? {
                    f.record_rev = rev;
                    w.observed = None;
                    metrics::counter!("sprig_controller_observed_writer_changes_total")
                        .increment(1);
                    tracing::warn!(cluster = %c, %from, to = %x, epoch, %source, settled_ms = ms(held), "observed writer published");
                }
                Ok(Outcome::RequeueAfter(HEALTHY_EVERY))
            }
            Observed::NoWriter(why) => {
                w.observed = None;
                self.note(cx, c, &mut f, "writer_lost", Some(why)).await?;
                Ok(Outcome::RequeueAfter(BUSY_EVERY))
            }
            Observed::Diverged(nodes) => {
                w.observed = None;
                let why = format!(
                    "{} are all writable and replicating from nobody: nothing new is published",
                    nodes.join(", ")
                );
                if f.record.state != "diverged" {
                    tracing::error!(cluster = %c, %why, "two writable nodes");
                    metrics::counter!("sprig_controller_diverged_writers_total").increment(1);
                }
                self.note(cx, c, &mut f, "diverged", Some(why)).await?;
                Ok(Outcome::RequeueAfter(HEALTHY_EVERY))
            }
        }
    }

    // ---- primary + replicas ------------------------------------------------

    #[allow(clippy::too_many_lines)] // steps 1-9 in order
    async fn primary_replica(
        &self,
        cx: &LoopCx,
        rec: &ClusterRecord,
        s: &ClusterSettings,
        w: &mut Watch,
    ) -> Result<Outcome> {
        let c = &rec.cluster_id;
        let mut f = self.facts(&*cx.dc, c).await?;
        let eps: BTreeMap<String, NodeEndpoint> = ctl_endpoints(rec)?
            .into_iter()
            .map(|e| (e.node.to_string(), e))
            .collect();
        if f.record.promotion.is_some() {
            return self.finish_promotion(cx, rec, s, w, &mut f, &eps).await;
        }
        // 1. The writer.
        let writer = match (&f.lease, &f.record.writer) {
            (Some((l, _)), _) if !l.claimed_by_controller() => l.node_id.clone(),
            (_, Some(wr)) => wr.clone(),
            _ => return Ok(Outcome::RequeueAfter(HEALTHY_EVERY)),
        };
        if f.record.writer.as_deref() != Some(writer.as_str()) {
            f.record.writer = Some(writer.clone());
            if let Some((l, _)) = &f.lease {
                f.record.writer_epoch = f.record.writer_epoch.max(l.writer_epoch);
            }
            if let Some(rev) = self.save(cx, c, &f.record, f.record_rev).await? {
                f.record_rev = rev;
            }
        }
        let Some(ep) = eps.get(&writer) else {
            return Ok(Outcome::RequeueAfter(HEALTHY_EVERY));
        };
        // 2. Probe it.
        let probe = probe_writer(ep).await;
        let now = self.clock.now();
        if let Probe::Up { semisync } = probe {
            w.failing_since = None;
            w.lease_gone_since = None;
            w.semisync = semisync;
            w.last_reason = None;
            if f.record.state != "healthy" {
                tracing::info!(cluster = %c, %writer, "writer answers again");
            }
            self.note(cx, c, &mut f, "healthy", None).await?;
            self.former_writer(cx, rec, s, w, &mut f, &eps).await?;
            return Ok(Outcome::RequeueAfter(HEALTHY_EVERY));
        }
        let failing_since = *w.failing_since.get_or_insert(now);
        let lease = match &f.lease {
            None => {
                let since = *w.lease_gone_since.get_or_insert(now);
                LeaseView::Gone(now.saturating_duration_since(since))
            }
            Some((l, _)) if l.node_id == writer && !l.claimed_by_controller() => {
                w.lease_gone_since = None;
                LeaseView::Writer
            }
            Some((l, _)) => LeaseView::Other(l.node_id.clone()),
        };
        let agent = f
            .agents
            .get(&writer)
            .map_or(AgentView::Missing, |a| AgentView::Registered(a.mysqld));
        let wall = self.clock.wall();
        let since_last = f
            .record
            .last_failover_unix_ms
            .map(|t| Duration::from_millis(unix_ms(wall).saturating_sub(t)));
        let o = Observation {
            probe,
            failing_for: now.saturating_duration_since(failing_since),
            lease,
            agent,
            since_last,
        };
        let step = decide(&o, s);
        match step {
            Step::Healthy => Ok(Outcome::RequeueAfter(HEALTHY_EVERY)),
            Step::Wait(why) => {
                if w.last_reason.as_deref() != Some(why.as_str()) {
                    tracing::warn!(cluster = %c, %writer, ?o, why, "writer lost? waiting");
                    w.last_reason = Some(why.clone());
                }
                self.note(cx, c, &mut f, "writer_lost", Some(why)).await?;
                Ok(Outcome::RequeueAfter(BUSY_EVERY))
            }
            Step::Suppress(why) => {
                if f.record.state != "suppressed" {
                    tracing::warn!(cluster = %c, %writer, why, "FailoverSuppressed");
                    metrics::counter!("sprig_controller_failovers_suppressed_total", "level" => "cluster")
                        .increment(1);
                }
                self.note(cx, c, &mut f, "suppressed", Some(why)).await?;
                Ok(Outcome::RequeueAfter(Duration::from_secs(5)))
            }
            Step::Blocked(why) => self.blocked(cx, rec, &mut f, why).await,
            Step::FailOver { sources } => {
                self.fail_over(
                    cx,
                    rec,
                    s,
                    w,
                    &mut f,
                    &eps,
                    &writer,
                    sources,
                    now.saturating_duration_since(failing_since),
                )
                .await
            }
        }
    }

    async fn blocked(
        &self,
        cx: &LoopCx,
        rec: &ClusterRecord,
        f: &mut Facts,
        why: String,
    ) -> Result<Outcome> {
        if f.record.state != "blocked" || f.record.reason.as_deref() != Some(why.as_str()) {
            let e = SprigError::new(
                sprig_common::SprigCode::FailoverBlocked,
                format!("cluster {}: {why}", rec.cluster_id),
            );
            tracing::error!(cluster = %rec.cluster_id, error = %e, "cluster failover blocked; an operator decides");
            metrics::counter!("sprig_controller_cluster_failovers_total",
                "topology" => rec.topology.as_str(), "result" => "blocked")
            .increment(1);
        }
        self.note(cx, &rec.cluster_id, f, "blocked", Some(why))
            .await?;
        Ok(Outcome::RequeueAfter(Duration::from_secs(2)))
    }

    /// Steps 6-8.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn fail_over(
        &self,
        cx: &LoopCx,
        rec: &ClusterRecord,
        s: &ClusterSettings,
        w: &mut Watch,
        f: &mut Facts,
        eps: &BTreeMap<String, NodeEndpoint>,
        writer: &str,
        sources: Vec<String>,
        detect: Duration,
    ) -> Result<Outcome> {
        let c = &rec.cluster_id;
        // 6. Replicas with agents (an agent clears read_only on the chosen
        // one): drain their relay logs, choose.
        let candidates: Vec<&NodeEndpoint> = eps
            .iter()
            .filter(|(n, _)| n.as_str() != writer && f.agents.contains_key(*n))
            .map(|(_, e)| e)
            .collect();
        if candidates.is_empty() {
            return self
                .blocked(cx, rec, f, "no replica has an agent to unfence it".into())
                .await;
        }
        tracing::warn!(cluster = %c, %writer, ?sources, "writer lost: draining the replicas' relay logs");
        let drained: Vec<std::result::Result<ReplicaState, String>> =
            futures::future::join_all(candidates.iter().map(|e| async move {
                drain_relay(e)
                    .await
                    .map_err(|err| format!("{}: {err}", e.node))
            }))
            .await;
        let mut states = Vec::new();
        for d in drained {
            match d {
                Ok(st) => states.push(st),
                Err(e) => tracing::warn!(cluster = %c, error = %e, "replica didn't drain"),
            }
        }
        let domain = match states.first() {
            Some(st) => match eps.get(st.node.as_str()) {
                Some(e) => gtid_domain(e).await.unwrap_or(0),
                None => 0,
            },
            None => 0,
        };
        let mut cand = match choose(&states, domain) {
            Choice::Promote(cand) => cand,
            Choice::Blocked(why) => return self.blocked(cx, rec, f, why).await,
        };
        let lag = states
            .iter()
            .find(|st| st.node == cand.node)
            .and_then(|st| st.lag_seconds);
        cand.expected_loss = expected_loss(w.semisync, lag);
        if let Err(why) = check_rpo(cand.expected_loss, s.rpo, s.manual) {
            return self.blocked(cx, rec, f, why).await;
        }
        // 7. The writer epoch and the lease claim.
        let epoch = f.record.writer_epoch + 1;
        let to = cand.node.to_string();
        let session = Session::open(
            Arc::clone(&cx.dc),
            Arc::clone(&self.clock),
            SessionOpts::new(s.writer_lease.max(Duration::from_secs(3))),
        )
        .await?;
        let key = writer_lease_key(cx.dc.tier(), c)?;
        let claim = Record::new(WriterLease {
            node_id: to.clone(),
            agent: format!("{CONTROLLER_PREFIX}{}", self.node),
            writer_epoch: epoch,
        })
        .encode()?;
        let r = cx
            .leader()?
            .commit_if(
                vec![Cmp::Missing(key.clone())],
                vec![Op::Put {
                    key,
                    value: claim,
                    lease: Some(session.lease()),
                }],
            )
            .await?;
        if !r.succeeded {
            let _ = session.close().await;
            tracing::warn!(cluster = %c, "the writer lease was taken before the claim; looking again");
            return Ok(Outcome::RequeueAfter(BUSY_EVERY));
        }
        let others: Vec<String> = states
            .iter()
            .map(|st| st.node.to_string())
            .filter(|n| *n != to)
            .collect();
        let wall = self.clock.wall();
        f.record.promotion = Some(OpenPromotion {
            from: writer.to_owned(),
            to: to.clone(),
            writer_epoch: epoch,
            applied: cand.applied.clone(),
            domain,
            others,
            semisync: w.semisync,
            detect_ms: ms(detect),
            lost_at_unix_ms: unix_ms(wall).saturating_sub(ms(detect)),
            loss_estimate_ms: cand.expected_loss.map(ms),
            sources,
        });
        f.record.state = "promoting".into();
        f.record.reason = Some(cand.why.clone());
        let Some(rev) = self.save(cx, c, &f.record, f.record_rev).await? else {
            let _ = session.close().await;
            return Err(SprigError::internal(
                "the cluster-failover record changed under the promotion",
            ));
        };
        f.record_rev = rev;
        tracing::warn!(cluster = %c, from = %writer, %to, epoch, why = %cand.why, loss = ?cand.expected_loss, "cluster failover: promoting");
        w.claim = Some(session);
        w.claimed_at = Some(self.clock.now());
        self.finish_promotion(cx, rec, s, w, f, eps).await
    }

    /// Step 8 (idempotent) and 9: promote, repoint, wait for the agent to
    /// take the lease over, record.
    async fn finish_promotion(
        &self,
        cx: &LoopCx,
        rec: &ClusterRecord,
        s: &ClusterSettings,
        w: &mut Watch,
        f: &mut Facts,
        eps: &BTreeMap<String, NodeEndpoint>,
    ) -> Result<Outcome> {
        let c = &rec.cluster_id;
        let Some(p) = f.record.promotion.clone() else {
            return Ok(Outcome::RequeueAfter(BUSY_EVERY));
        };
        let key = writer_lease_key(cx.dc.tier(), c)?;
        match &f.lease {
            Some((l, _)) if l.node_id == p.to && !l.claimed_by_controller() => {
                return self.promoted(cx, rec, w, f, &p).await;
            }
            Some((l, _)) if l.node_id != p.to => {
                // Someone else's node took the lease (it was missing after a
                // leader change): stop and let an operator look.
                f.record.promotion = None;
                return self
                    .blocked(
                        cx,
                        rec,
                        f,
                        format!(
                            "{} took the writer lease while {} was being promoted",
                            l.node_id, p.to
                        ),
                    )
                    .await;
            }
            Some(_) => {}
            None => {
                // A new leader resumes: claim again for the chosen node.
                if let Some(old) = w.claim.take() {
                    let _ = old.close().await;
                }
                let session = Session::open(
                    Arc::clone(&cx.dc),
                    Arc::clone(&self.clock),
                    SessionOpts::new(s.writer_lease.max(Duration::from_secs(3))),
                )
                .await?;
                let claim = Record::new(WriterLease {
                    node_id: p.to.clone(),
                    agent: format!("{CONTROLLER_PREFIX}{}", self.node),
                    writer_epoch: p.writer_epoch,
                })
                .encode()?;
                let r = cx
                    .leader()?
                    .commit_if(
                        vec![Cmp::Missing(key)],
                        vec![Op::Put {
                            key: writer_lease_key(cx.dc.tier(), c)?,
                            value: claim,
                            lease: Some(session.lease()),
                        }],
                    )
                    .await?;
                if !r.succeeded {
                    let _ = session.close().await;
                    return Ok(Outcome::RequeueAfter(BUSY_EVERY));
                }
                w.claim = Some(session);
                w.claimed_at = Some(self.clock.now());
            }
        }
        // 8. Promote (read_only stays on) and repoint the others.
        let to = eps
            .get(&p.to)
            .ok_or_else(|| SprigError::internal(format!("{} isn't a node of {c}", p.to)))?;
        promote_fenced(to, p.semisync && !p.others.is_empty())
            .await
            .map_err(node_err)?;
        let source = repl_source(rec, to).await?;
        for o in &p.others {
            let Some(e) = eps.get(o) else { continue };
            if already_follows(e, &source).await {
                continue;
            }
            match repoint(e, &source).await {
                Ok(()) => {
                    tracing::info!(cluster = %c, replica = %o, to = %p.to, "replica repointed");
                }
                Err(err) => {
                    tracing::warn!(cluster = %c, replica = %o, error = %err, "repoint failed; retrying");
                }
            }
        }
        if w.claimed_at.is_some_and(|t| t.elapsed() >= TAKEOVER_ALERT) {
            tracing::error!(cluster = %c, to = %p.to, "the chosen node's agent hasn't taken the writer lease over");
            w.claimed_at = Some(self.clock.now());
        }
        Ok(Outcome::RequeueAfter(Duration::from_millis(200)))
    }

    /// Step 9: the agent took the lease over.
    async fn promoted(
        &self,
        cx: &LoopCx,
        rec: &ClusterRecord,
        w: &mut Watch,
        f: &mut Facts,
        p: &OpenPromotion,
    ) -> Result<Outcome> {
        let c = &rec.cluster_id;
        if let Some(s) = w.claim.take() {
            // The key is bound to the agent's lease now; revoking ours
            // deletes nothing.
            let _ = s.close().await;
        }
        let wall = self.clock.wall();
        let now_ms = unix_ms(wall);
        let ev = FailoverEvent {
            kind: "promote".into(),
            at: rfc3339(wall),
            at_unix_ms: now_ms,
            from: p.from.clone(),
            to: p.to.clone(),
            writer_epoch: p.writer_epoch,
            detect_ms: p.detect_ms,
            rto_ms: now_ms.saturating_sub(p.lost_at_unix_ms),
            loss_estimate_ms: p.loss_estimate_ms,
            sources: p.sources.clone(),
            applied: p.applied.clone(),
            domain: p.domain,
            former: Some(FormerWriter::Fenced),
            errant: None,
            extract_job: None,
            rebuild_job: None,
            recovery_error: None,
        };
        tracing::warn!(cluster = %c, from = %p.from, to = %p.to, epoch = p.writer_epoch, rto_ms = ev.rto_ms, "cluster failover done");
        metrics::counter!("sprig_controller_cluster_failovers_total",
            "topology" => rec.topology.as_str(), "result" => "promoted")
        .increment(1);
        #[allow(clippy::cast_precision_loss)] // metrics
        {
            metrics::histogram!("sprig_controller_failover_duration_seconds", "level" => "cluster")
                .record(ev.rto_ms as f64 / 1000.0);
            if let Some(l) = ev.loss_estimate_ms {
                metrics::histogram!("sprig_controller_failover_loss_seconds", "level" => "cluster")
                    .record(l as f64 / 1000.0);
            }
        }
        f.record.history.push(ev);
        if f.record.history.len() > HISTORY {
            let drop_n = f.record.history.len() - HISTORY;
            f.record.history.drain(..drop_n);
        }
        f.record.promotion = None;
        f.record.writer = Some(p.to.clone());
        f.record.writer_epoch = p.writer_epoch;
        f.record.source = Some("sprig failover".into());
        f.record.last_failover_unix_ms = Some(now_ms);
        f.record.state = "healthy".into();
        f.record.reason = None;
        if let Some(rev) = self.save(cx, c, &f.record, f.record_rev).await? {
            f.record_rev = rev;
        }
        w.failing_since = None;
        w.lease_gone_since = None;
        Ok(Outcome::RequeueAfter(HEALTHY_EVERY))
    }

    /// The former writer of the last failover, once it answers: rejoined
    /// when nothing diverged, reported and kept out otherwise; a diverged
    /// one is then extracted and rebuilt ([`Self::recover_former`]).
    async fn former_writer(
        &self,
        cx: &LoopCx,
        rec: &ClusterRecord,
        s: &ClusterSettings,
        w: &mut Watch,
        f: &mut Facts,
        eps: &BTreeMap<String, NodeEndpoint>,
    ) -> Result<()> {
        let c = &rec.cluster_id;
        let Some(ev) = f.record.history.last().cloned() else {
            return Ok(());
        };
        if ev.kind == "promote"
            && matches!(
                ev.former,
                Some(FormerWriter::Diverged | FormerWriter::Rebuilding)
            )
        {
            if w
                .recovery_checked
                .is_some_and(|t| t.elapsed() < RECOVERY_EVERY)
            {
                return Ok(());
            }
            w.recovery_checked = Some(self.clock.now());
            return self.recover_former(cx, rec, s, f, eps).await;
        }
        if ev.kind != "promote" || ev.former != Some(FormerWriter::Fenced) {
            return Ok(());
        }
        let (Some(old), Some(new)) = (eps.get(&ev.from), eps.get(&ev.to)) else {
            return Ok(());
        };
        let Ok(mut s) = ProbeSession::connect(old, PROBE_TIMEOUT).await else {
            return Ok(());
        };
        let replicating = s
            .rows("SHOW SLAVE STATUS")
            .await
            .map(|r| !r.is_empty())
            .unwrap_or(false);
        s.close().await;
        let (former, errant) = if replicating {
            (FormerWriter::Rejoined, None)
        } else {
            let pos = binlog_pos(old).await.map_err(node_err)?;
            match diverged(&pos, &ev.applied, ev.domain) {
                None => {
                    let source = repl_source(rec, new).await?;
                    rejoin(old, &source).await.map_err(node_err)?;
                    tracing::warn!(cluster = %c, node = %ev.from, to = %ev.to, "former writer rejoined as a replica (nothing diverged)");
                    (FormerWriter::Rejoined, None)
                }
                Some(range) => {
                    let txns = diverged_events(old, &range).await.map_err(node_err)?;
                    let report = errant_report(&range, &txns, &[]);
                    tracing::warn!(cluster = %c, node = %ev.from, count = report.count, gtids = ?report.gtids, "DivergedTransactions: the former writer stays out until it is rebuilt");
                    metrics::counter!("sprig_controller_diverged_transactions_total", "level" => "cluster")
                        .increment(report.count);
                    (FormerWriter::Diverged, Some(report))
                }
            }
        };
        if let Some(last) = f.record.history.last_mut() {
            last.former = Some(former);
            last.errant = errant;
        }
        if let Some(rev) = self.save(cx, c, &f.record, f.record_rev).await? {
            f.record_rev = rev;
        }
        Ok(())
    }

    /// A diverged former writer (chapter 25, "Recovery after an outage"
    /// steps 2-3): its `errant_extract` job, then, once the extract is
    /// verified, its `node_rebuild` job; `rebuilt` when that succeeds.
    /// Jobs start on their own only in `failover.mode = auto`; an
    /// operator's jobs (`sprigctl cluster extract|rebuild`) are followed
    /// either way. A failed job isn't retried on its own: its reason goes
    /// into the record for an operator.
    #[allow(clippy::too_many_lines)] // the two stages in order
    async fn recover_former(
        &self,
        cx: &LoopCx,
        rec: &ClusterRecord,
        s: &ClusterSettings,
        f: &mut Facts,
        eps: &BTreeMap<String, NodeEndpoint>,
    ) -> Result<()> {
        use crate::dispatch::{self, JobState, NewJob};
        use sprig_cluster::failover::{extract, rebuild};
        let c = &rec.cluster_id;
        let Some(mut ev) = f.record.history.last().cloned() else {
            return Ok(());
        };
        let node = ev.from.clone();
        let mine = |j: &dispatch::Job, kind: &str| {
            j.kind == kind
                && j.spec.get("cluster").and_then(serde_json::Value::as_str) == Some(c.as_str())
                && j.spec.get("node").and_then(serde_json::Value::as_str) == Some(node.as_str())
        };
        let jobs: Vec<dispatch::Job> = dispatch::list_jobs(&*cx.dc)
            .await?
            .into_iter()
            .map(|j| j.job().clone())
            .filter(|j| mine(j, extract::KIND) || mine(j, rebuild::KIND))
            .collect();
        let newest = |kind: &str| {
            jobs.iter()
                .filter(|j| j.kind == kind)
                .max_by(|a, b| a.created_at.cmp(&b.created_at))
                .cloned()
        };
        let before = ev.clone();
        let auto = !s.manual;
        let verified = ev
            .errant
            .as_ref()
            .and_then(|r| r.extract.as_ref())
            .is_some_and(|x| x.verified);
        // Stage 1: the extract.
        if !verified {
            match newest(extract::KIND) {
                Some(j) if j.state == JobState::Succeeded => {
                    match serde_json::from_value::<extract::ExtractSummary>(j.progress.clone()) {
                        Ok(x) => {
                            tracing::warn!(cluster = %c, %node, job = %j.job_id, rows = x.rows, location = %x.location, "errant extract verified");
                            if let Some(r) = ev.errant.as_mut() {
                                r.path = Some(x.location.clone());
                                r.extract = Some(x);
                            }
                            ev.extract_job = Some(j.job_id.to_string());
                            ev.recovery_error = None;
                        }
                        Err(e) => ev.recovery_error = Some(format!("extract job {}: {e}", j.job_id)),
                    }
                }
                Some(j) if j.state.is_final() => {
                    ev.extract_job = Some(j.job_id.to_string());
                    ev.recovery_error = Some(format!(
                        "errant_extract {} {}: {}",
                        j.job_id,
                        j.state.as_str(),
                        j.error.clone().unwrap_or_default()
                    ));
                }
                Some(j) => ev.extract_job = Some(j.job_id.to_string()),
                None if auto => {
                    let Some(old) = eps.get(&node) else {
                        return Ok(());
                    };
                    let Ok(pos) = binlog_pos(old).await else {
                        return Ok(());
                    };
                    let Some(range) = diverged(&pos, &ev.applied, ev.domain) else {
                        return Ok(());
                    };
                    if let Some(l) = cx.leadership.as_ref() {
                        let spec = extract::ErrantExtractSpec {
                            cluster: c.clone(),
                            node: node.clone(),
                            range,
                            failover_at_unix_ms: ev.at_unix_ms,
                        };
                        let id = dispatch::submit(
                            l,
                            &*cx.clock,
                            NewJob {
                                kind: extract::KIND.into(),
                                class: dispatch::JobClass::Recovery,
                                tenant_id: None,
                                clusters: vec![c.clone()],
                                priority: 10,
                                cause: "cluster_failover".into(),
                                zone: None,
                                spec: serde_json::to_value(&spec)
                                    .map_err(|e| SprigError::internal(e.to_string()))?,
                            },
                        )
                        .await?;
                        tracing::warn!(cluster = %c, %node, job = %id, "errant_extract submitted for the diverged former writer");
                        ev.extract_job = Some(id.to_string());
                    }
                }
                None => {}
            }
        }
        // Stage 2: the rebuild (after a verified extract, or an operator's
        // rebuild with --no-extract).
        let verified = ev
            .errant
            .as_ref()
            .and_then(|r| r.extract.as_ref())
            .is_some_and(|x| x.verified);
        match newest(rebuild::KIND) {
            Some(j) if j.state == JobState::Succeeded => {
                tracing::warn!(cluster = %c, %node, job = %j.job_id, "the diverged former writer was rebuilt and replicates");
                ev.former = Some(FormerWriter::Rebuilt);
                ev.rebuild_job = Some(j.job_id.to_string());
                ev.recovery_error = None;
            }
            Some(j) if j.state.is_final() => {
                if ev.rebuild_job.as_deref() != Some(j.job_id.to_string().as_str())
                    || ev.former != Some(FormerWriter::Diverged)
                {
                    tracing::error!(cluster = %c, %node, job = %j.job_id, error = ?j.error, "node_rebuild of the diverged former writer failed");
                }
                ev.former = Some(FormerWriter::Diverged);
                ev.rebuild_job = Some(j.job_id.to_string());
                ev.recovery_error = Some(format!(
                    "node_rebuild {} {}: {}",
                    j.job_id,
                    j.state.as_str(),
                    j.error.clone().unwrap_or_default()
                ));
            }
            Some(j) => {
                ev.former = Some(FormerWriter::Rebuilding);
                ev.rebuild_job = Some(j.job_id.to_string());
            }
            None if auto && verified => {
                if let Some(l) = cx.leadership.as_ref() {
                    let rate = self.live.as_ref().map_or(fs::REBUILD_MAX_MB_PER_SECOND.default, |l| {
                        l.snapshot().get(
                            &fs::REBUILD_MAX_MB_PER_SECOND,
                            &SettingCtx::process(&self.dc).with_cluster(c),
                        )
                    });
                    let mut spec = serde_json::to_value(rebuild::NodeRebuildSpec {
                        cluster: c.clone(),
                        node: node.clone(),
                        source: None,
                        require_extract: true,
                    })
                    .map_err(|e| SprigError::internal(e.to_string()))?;
                    spec["rate_mb_per_second"] = serde_json::json!(rate);
                    let id = dispatch::submit(
                        l,
                        &*cx.clock,
                        NewJob {
                            kind: rebuild::KIND.into(),
                            class: dispatch::JobClass::Recovery,
                            tenant_id: None,
                            clusters: vec![c.clone()],
                            priority: 10,
                            cause: "cluster_failover".into(),
                            zone: None,
                            spec,
                        },
                    )
                    .await?;
                    tracing::warn!(cluster = %c, %node, job = %id, rate_mb_per_second = rate, "node_rebuild submitted for the diverged former writer");
                    ev.former = Some(FormerWriter::Rebuilding);
                    ev.rebuild_job = Some(id.to_string());
                }
            }
            None => {}
        }
        if ev != before {
            if let Some(last) = f.record.history.last_mut() {
                *last = ev;
            }
            if let Some(rev) = self.save(cx, c, &f.record, f.record_rev).await? {
                f.record_rev = rev;
            }
        }
        Ok(())
    }

    // ---- Galera ------------------------------------------------------------

    async fn galera(
        &self,
        cx: &LoopCx,
        rec: &ClusterRecord,
        s: &ClusterSettings,
        w: &mut Watch,
    ) -> Result<Outcome> {
        let c = &rec.cluster_id;
        let mut f = self.facts(&*cx.dc, c).await?;
        let eps = ctl_endpoints(rec)?;
        let mut statuses =
            futures::future::join_all(eps.iter().map(|e| probe_node(e, PROBE_TIMEOUT))).await;
        for st in &mut statuses {
            // Confirmed down: unreachable and no live agent registration.
            st.confirmed_down = !st.reachable && !f.agents.contains_key(st.node.as_str());
        }
        let action = w
            .quorum
            .observe(&statuses, self.clock.now(), s.quorum_wait, s.manual);
        match action {
            GaleraAction::Primary(nodes) => {
                let out: Vec<String> = eps
                    .iter()
                    .map(|e| e.node.to_string())
                    .filter(|n| !nodes.iter().any(|p| p.as_str() == n))
                    .collect();
                let reason = (!out.is_empty())
                    .then(|| format!("outside the primary component: {}", out.join(", ")));
                if reason != f.record.reason {
                    tracing::info!(cluster = %c, primary = ?nodes, ?out, "Galera membership");
                }
                self.note(cx, c, &mut f, "healthy", reason).await?;
                Ok(Outcome::RequeueAfter(HEALTHY_EVERY))
            }
            GaleraAction::WaitQuorum { lost_for } => {
                self.note(
                    cx,
                    c,
                    &mut f,
                    "quorum_lost",
                    Some(format!(
                        "no primary component for {} s of failover.galera_quorum_wait_seconds",
                        lost_for.as_secs()
                    )),
                )
                .await?;
                Ok(Outcome::RequeueAfter(BUSY_EVERY))
            }
            GaleraAction::Blocked(why) => self.blocked(cx, rec, &mut f, why).await,
            GaleraAction::Bootstrap(node) => {
                let Some(ep) = eps.iter().find(|e| e.node == node) else {
                    return Ok(Outcome::RequeueAfter(BUSY_EVERY));
                };
                tracing::warn!(cluster = %c, %node, seqno = w.quorum.highest_primary(), "Galera quorum lost: bootstrapping the most advanced node");
                bootstrap(ep, PROBE_TIMEOUT).await.map_err(node_err)?;
                let wall = self.clock.wall();
                f.record.history.push(FailoverEvent {
                    kind: "galera_bootstrap".into(),
                    at: rfc3339(wall),
                    at_unix_ms: unix_ms(wall),
                    from: String::new(),
                    to: node.to_string(),
                    writer_epoch: f.record.writer_epoch,
                    detect_ms: ms(s.quorum_wait),
                    rto_ms: 0,
                    loss_estimate_ms: Some(0),
                    sources: vec!["quorum".into()],
                    applied: w.quorum.highest_primary().to_string(),
                    domain: 0,
                    former: None,
                    errant: None,
                    extract_job: None,
                    rebuild_job: None,
                    recovery_error: None,
                });
                if f.record.history.len() > HISTORY {
                    f.record.history.remove(0);
                }
                f.record.last_failover_unix_ms = Some(unix_ms(wall));
                metrics::counter!("sprig_controller_cluster_failovers_total",
                    "topology" => rec.topology.as_str(), "result" => "bootstrapped")
                .increment(1);
                self.note(cx, c, &mut f, "healthy", None).await?;
                Ok(Outcome::RequeueAfter(Duration::from_secs(2)))
            }
        }
    }
}

/// One probe of the writer: connect and read the semi-sync sample.
async fn probe_writer(ep: &NodeEndpoint) -> Probe {
    match ProbeSession::connect(ep, PROBE_TIMEOUT).await {
        Ok(mut s) => {
            let semisync = s
                .rows("SHOW GLOBAL STATUS LIKE 'Rpl_semi_sync_master_status'")
                .await
                .ok()
                .and_then(|r| r.into_iter().next())
                .and_then(|r| r.into_iter().nth(1))
                .and_then(|(_, v)| v)
                .is_some_and(|v| v.eq_ignore_ascii_case("ON"));
            s.close().await;
            Probe::Up { semisync }
        }
        Err(e) if e.timeout => Probe::TimedOut,
        // MariaDB answered with an error (too many connections, a
        // privilege): it runs.
        Err(e) if e.is_server_error() => Probe::Up { semisync: false },
        Err(_) => Probe::Refused,
    }
}

async fn gtid_domain(ep: &NodeEndpoint) -> Option<u32> {
    let mut s = ProbeSession::connect(ep, PROBE_TIMEOUT).await.ok()?;
    let v = s.scalar("SELECT @@gtid_domain_id").await.ok().flatten();
    s.close().await;
    v.and_then(|v| v.parse().ok())
}

/// Where replicas reach `to`, as `sprig_repl`: its `report_host` and
/// `report_port`, else its registered address.
///
/// # Errors
/// The credentials can't be resolved; `to` can't be reached.
pub async fn repl_source(rec: &ClusterRecord, to: &NodeEndpoint) -> Result<ReplSource> {
    let cred = sprig_provision::credentials::resolve(&rec.secrets.repl, "sprig_repl")?;
    let (host, port) = if let Some(hp) = report_address(to).await.map_err(node_err)? {
        hp
    } else {
        let addr = rec
            .nodes
            .iter()
            .find(|n| n.node == to.node)
            .map(|n| n.addr.clone())
            .unwrap_or_default();
        let (h, p) = addr.rsplit_once(':').unwrap_or((addr.as_str(), "3306"));
        (h.to_owned(), p.parse().unwrap_or(3306))
    };
    Ok(ReplSource {
        host,
        port,
        user: cred.user.clone(),
        password: cred.password().to_owned(),
    })
}

/// A replica already replicating from `source` (a resumed promotion).
async fn already_follows(ep: &NodeEndpoint, source: &ReplSource) -> bool {
    let Ok(mut s) = ProbeSession::connect(ep, PROBE_TIMEOUT).await else {
        return false;
    };
    let rows = s.rows("SHOW SLAVE STATUS").await.unwrap_or_default();
    s.close().await;
    rows.first().is_some_and(|r| {
        let get = |n: &str| {
            r.iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(n))
                .and_then(|(_, v)| v.clone())
                .unwrap_or_default()
        };
        get("Master_Host") == source.host && get("Master_Port") == source.port.to_string()
    })
}

fn cluster_of(ev: &MetaEvent) -> Option<ClusterId> {
    let path = ev.path();
    if let Some(id) = path
        .strip_prefix("clusters/")
        .or_else(|| path.strip_prefix("leases/writer/"))
    {
        return ClusterId::new(id).ok();
    }
    if path.starts_with("agents/") {
        let kv = ev.kv.as_ref()?;
        let a = Record::<AgentRecord>::decode(&kv.value).ok()?;
        return ClusterId::new(a.body.cluster).ok();
    }
    None
}

#[async_trait::async_trait]
impl Reconciler for ClusterFailover {
    type Key = ClusterId;

    fn name(&self) -> &'static str {
        "cluster_failover"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn watches(&self) -> Vec<WatchSpec> {
        vec![
            WatchSpec::dc("clusters/"),
            WatchSpec::dc("leases/writer/"),
            WatchSpec::dc("agents/"),
        ]
    }

    fn workers(&self) -> usize {
        8
    }

    fn keys_for(&self, ev: &MetaEvent) -> Vec<Self::Key> {
        cluster_of(ev).into_iter().collect()
    }

    async fn all_keys(&self, cx: &LoopCx) -> Result<Vec<Self::Key>> {
        let reg = ClusterRegistry::new(Arc::clone(&cx.global), Arc::clone(&cx.dc))?;
        Ok(reg
            .list()
            .await?
            .into_iter()
            .map(|s| s.body().clone())
            .filter(|r| {
                matches!(
                    r.topology,
                    TopologyKind::PrimaryReplica | TopologyKind::Galera
                )
            })
            .map(|r| r.cluster_id)
            .collect())
    }

    async fn reconcile(&self, cx: &LoopCx, key: &Self::Key) -> Result<Outcome> {
        self.pass(cx, key).await
    }
}

#[cfg(test)]
mod tests;
