//! The external provisioner: registers MariaDB that already exists, after
//! prechecks (chapter 27, "External registration" and "Registering an
//! external cluster"; build step 1).
//!
//! ```text
//! register(req):
//! 1. connect to each endpoint as sprig_ctl over TLS; refuse non-TLS
//! 2. SHOW GRANTS FOR CURRENT_USER; compare with the required list
//! 3. read the required variables; compare with the settings table
//! 4. create and drop user sprig_probe_<8 hex> and schema t0_sprig_probe_<8 hex>
//! 5. as sprig_repl, open a binlog dump at the current position and read one event
//! 6. chapter 09's probes build the capability profile; record declared values
//! 7. any failure -> 39511 ER_SPRIG_CLUSTER_PRECHECK_FAILED, listing every missing item;
//!    success -> clusters/<id> with provisioner = external, then active
//! 8. as sprig_ctl on the writer, make sure the gateways' monitoring account
//!    sprig_gw exists ([`crate::gw_account`]); if it can't be created, an
//!    advisory says what to run
//! ```
//!
//! The settings in step 3 are chapter 27's table; the binlog half is the
//! change feed's check ([`sprig_cluster::probe::feed`]), reused as is.
//! Differences from the book: step 4's schema is `t0_sprig_probe_<hex>`,
//! because `sprig_ctl`'s grants cover only `t%` (the book's
//! `sprig_probe_<hex>` schema would always fail); and missing privileges
//! other than those on `t%` and `__sprig_sys` are advisories, as chapter 09
//! has it ("a missing privilege disables only what needs it"), not
//! failures. `require_secure_transport = OFF` is an advisory: SprigDB's own
//! sessions are refused unless encrypted either way.
//!
//! Step 5 is chapter 09's binlog dump probe ([`sprig_cluster::probe::dump`]),
//! run once as `sprig_repl` on the node the active probes ran on, as part
//! of the deep probe, so the profile's feed level reflects it. By the same
//! rule, a dump that fails because of `sprig_repl`'s privileges or the
//! binlog itself (no `REPLICATION SLAVE`, a checksum or format the reader
//! doesn't know, a server that refuses the reader's plain-text connection)
//! is an advisory: registration succeeds with the feed and the mover
//! `unsupported`. A `sprig_repl` credential that doesn't resolve or log in,
//! and a dump that times out or loses its connection, block.
//!
//! On external clusters SprigDB never changes server settings, restarts,
//! adds or removes nodes, or touches schemas outside `t%` and
//! `__sprig_sys`.

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use sprig_cluster::health::HealthTracker;
use sprig_cluster::probe::dump::{self, DumpFailureKind};
use sprig_cluster::probe::feed::{self, GapSeverity};
use sprig_cluster::probe::{
    Grant, NodeFacts, NodeProbe, PrivilegeInfo, ProbeDepth, Prober, active_target,
};
use sprig_cluster::profile::{
    CapabilityProfile, CapabilityPublisher, DeriveInput, derive, registration_gaps,
};
use sprig_cluster::record::{AdminState, NodeSpec, ProvisionerKind, TopologyKind, rfc3339};
use sprig_cluster::registry::{ClusterRegistry, Registration, StoredCluster};
use sprig_cluster::settings::ClusterSettings;
use sprig_cluster::sql::{NodeEndpoint, ProbeError, ProbeSession, literal};
use sprig_common::ids::ClusterId;
use sprig_common::time::{Clock, SystemClock};
use sprig_common::{Result, SprigError};

use crate::credentials::CredentialConnector;
use crate::{NodeReport, PrecheckReport, ProvisionOp, Provisioner, RegisterCluster};

/// The shortest binlog retention chapter 27 accepts, seconds.
pub const MIN_BINLOG_EXPIRE_SECONDS: u64 = 86_400;

/// What one `sprig_ctl` session showed about its transport.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionCheck {
    /// `Ssl_cipher` of the session; empty or absent means plain text.
    pub cipher: Option<String>,
    /// `@@require_secure_transport`.
    pub require_secure_transport: Option<bool>,
    /// The account's grants, read on this session: they are reported even
    /// when a missing privilege stops the probes.
    pub privileges: Option<PrivilegeInfo>,
}

/// The external provisioner of one data center.
#[derive(Clone)]
pub struct ExternalProvisioner {
    registry: ClusterRegistry,
    settings: ClusterSettings,
    retention_hours: u64,
    clock: Arc<dyn Clock>,
    recorded_as: ProvisionerKind,
}

impl std::fmt::Debug for ExternalProvisioner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalProvisioner")
            .field("registry", &self.registry)
            .field("retention_hours", &self.retention_hours)
            .finish_non_exhaustive()
    }
}

impl ExternalProvisioner {
    /// A provisioner writing through `registry`, probing with `settings`
    /// (resolved at dc scope: the cluster doesn't exist yet).
    #[must_use]
    pub fn new(registry: ClusterRegistry, settings: ClusterSettings) -> Self {
        Self {
            registry,
            settings,
            retention_hours: feed::DEFAULT_RETENTION_HOURS,
            clock: Arc::new(SystemClock),
            recorded_as: ProvisionerKind::External,
        }
    }

    /// Records registered clusters with `provisioner = kind`: the
    /// Kubernetes operator registers the clusters it built through the same
    /// prechecks (chapter 27, "The Kubernetes operator", step 5), and the
    /// record says who manages them.
    #[must_use]
    pub fn recorded_as(mut self, kind: ProvisionerKind) -> Self {
        self.recorded_as = kind;
        self
    }

    /// Uses `feed.retention_hours` = `hours` for the binlog checks.
    #[must_use]
    pub fn with_retention_hours(mut self, hours: u64) -> Self {
        self.retention_hours = hours;
        self
    }

    /// Reads time through `clock`.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// The registry it writes through.
    #[must_use]
    pub fn registry(&self) -> &ClusterRegistry {
        &self.registry
    }

    /// Runs every precheck; never writes to etcd. On MariaDB it creates and
    /// drops one locked account and one schema, and `__sprig_sys`'s probe
    /// tables (chapter 09's active probes).
    pub async fn precheck_report(&self, req: &RegisterCluster) -> PrecheckReport {
        let mut report = PrecheckReport {
            blocking: validate(req, self.registry.dc()),
            ..PrecheckReport::default()
        };
        let ctl = CredentialConnector::ctl(&req.secrets);
        let mut eps = Vec::new();
        for n in &req.nodes {
            report.nodes.push(NodeReport {
                node: n.node.clone(),
                addr: n.addr.clone(),
                tls_cipher: None,
                version: None,
                role: None,
                error: None,
            });
            match sprig_cluster::driver::NodeConnector::endpoint(&ctl, n) {
                Ok(ep) => eps.push(ep),
                Err(e) => report.blocking.push(format!(
                    "node {}: sprig_ctl credential: {}",
                    n.node, e.message
                )),
            }
        }
        if eps.len() != req.nodes.len() || !report.blocking.is_empty() {
            return report;
        }
        let timeout = self.settings.probe_timeout;
        let mut sessions = Vec::new();
        for (ep, nr) in eps.iter().zip(report.nodes.iter_mut()) {
            match session_check(ep, timeout).await {
                Ok(s) => {
                    nr.tls_cipher.clone_from(&s.cipher);
                    sessions.push(Some(s));
                }
                Err(e) => {
                    nr.error = Some(e.to_string());
                    sessions.push(None);
                }
            }
        }
        let repl_eps = repl_endpoints(req);
        let probes = Prober::with_clock(timeout, Arc::clone(&self.clock))
            .probe_cluster_with_repl(req.topology, &eps, repl_eps.as_deref(), ProbeDepth::Deep)
            .await;
        for (p, nr) in probes.iter().zip(report.nodes.iter_mut()) {
            match &p.result {
                Ok(f) => {
                    nr.version = Some(f.server.version_text.clone());
                    nr.role = Some(role_of(f).to_owned());
                }
                Err(e) => nr.error = Some(e.to_string()),
            }
        }
        let profile = derive(DeriveInput {
            topology: req.topology,
            probes: &probes,
            declared: req.declared.as_ref(),
            previous: None,
        });
        let (blocking, advisories) = classify(&Classify {
            topology: req.topology,
            probes: &probes,
            profile: &profile,
            sessions: &sessions,
            fleet_lower_case_table_names: self.settings.lower_case_table_names,
            retention_hours: self.retention_hours,
        });
        report.blocking.extend(blocking);
        report.advisories.extend(advisories);
        let writer = active_target(req.topology, &probes)
            .or_else(|| sessions.iter().position(Option::is_some));
        if let Some(i) = writer {
            report
                .blocking
                .extend(self.account_checks(&eps[i], timeout).await);
            let (blocking, advisories) = repl_check(req, &req.nodes[i], &probes[i], timeout).await;
            report.blocking.extend(blocking);
            report.advisories.extend(advisories);
            report
                .advisories
                .extend(old_temporal_check(&eps[i], timeout).await);
        } else {
            report
                .blocking
                .push("no node answered: the account and sprig_repl checks need one".to_owned());
        }
        if probes.iter().any(|p| p.facts().is_some()) {
            report.health = Some(
                HealthTracker::new()
                    .observe(req.topology, &probes, &self.settings)
                    .health,
            );
            report.profile = Some(profile);
        }
        report.probes = probes;
        report
    }

    /// Step 4: `sprig_ctl` can create an account and a tenant-pattern
    /// schema and grant on it. Everything created is dropped again.
    async fn account_checks(&self, ep: &NodeEndpoint, timeout: Duration) -> Vec<String> {
        let tag = self.tag();
        let user = format!("{}@'%'", literal(&format!("sprig_probe_{tag}")));
        let schema = format!("`t0_sprig_probe_{tag}`");
        let mut s = match ProbeSession::connect(ep, timeout).await {
            Ok(s) => s,
            Err(e) => return vec![format!("node {}: sprig_ctl can't log in: {e}", ep.node)],
        };
        let steps = [
            (
                format!("CREATE USER {user} ACCOUNT LOCK"),
                "create accounts: GRANT CREATE USER ON *.* TO sprig_ctl",
            ),
            (
                format!("CREATE DATABASE {schema}"),
                "create tenant schemas: GRANT ALL ON `t%`.* TO sprig_ctl WITH GRANT OPTION",
            ),
            (
                format!("GRANT SELECT ON {schema}.* TO {user}"),
                "grant on tenant schemas: GRANT ALL ON `t%`.* TO sprig_ctl WITH GRANT OPTION",
            ),
        ];
        let mut out = Vec::new();
        for (sql, what) in &steps {
            if let Err(e) = s.exec(sql).await {
                out.push(format!("node {}: sprig_ctl can't {what} ({e})", ep.node));
                break;
            }
        }
        for sql in [
            format!("DROP DATABASE IF EXISTS {schema}"),
            format!("DROP USER IF EXISTS {user}"),
        ] {
            if let Err(e) = s.exec(&sql).await {
                tracing::warn!(node = %ep.node, error = %e, sql, "precheck cleanup failed");
            }
        }
        s.close().await;
        out
    }

    /// Makes sure `sprig_gw` exists on `node`, as `sprig_ctl`.
    async fn ensure_gw(&self, node: &NodeSpec, req: &RegisterCluster) -> Result<()> {
        let gw = crate::gw_account::gw_credential(&req.secrets.ctl)?;
        let ctl = CredentialConnector::ctl(&req.secrets);
        let ep = sprig_cluster::driver::NodeConnector::endpoint(&ctl, node)?;
        let r = crate::gw_account::ensure(node, &ep, &gw, self.settings.probe_timeout).await?;
        tracing::info!(node = %node.node, result = ?r, "sprig_gw");
        Ok(())
    }

    /// Eight hex digits that differ between concurrent prechecks.
    fn tag(&self) -> String {
        let nanos = self
            .clock
            .wall()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos());
        format!("{:08x}", nanos ^ std::process::id().rotate_left(16))
    }

    /// Prechecks, then writes the record and publishes the first profile:
    /// `probing`, then `active` with the advisories as the record's gaps.
    ///
    /// # Errors
    /// 39511 listing every blocking item; 39105 for a name in use; the
    /// registry's errors.
    pub async fn register_report(
        &self,
        req: RegisterCluster,
    ) -> Result<(StoredCluster, PrecheckReport)> {
        let mut report = self.precheck_report(&req).await;
        if !report.passed() {
            return Err(report.error(&req.name));
        }
        // The gateways' monitoring account (chapter 09): created now, or
        // an advisory naming what the operator runs.
        if let Some(i) = active_target(req.topology, &report.probes)
            && let Some(node) = req.nodes.get(i)
            && let Err(e) = self.ensure_gw(node, &req).await
        {
            report.advisories.push(e.message);
        }
        let profile = report
            .profile
            .clone()
            .ok_or_else(|| SprigError::internal("prechecks passed without a profile"))?;
        let stored = self
            .registry
            .register(Registration {
                name: req.name,
                topology: req.topology,
                provisioner: self.recorded_as,
                nodes: req.nodes,
                secrets: req.secrets,
                capacity: req.capacity,
                declared: req.declared,
            })
            .await?;
        let id = stored.body().cluster_id.clone();
        tracing::info!(cluster = %id, "registered; publishing the first profile");
        let mut publisher =
            CapabilityPublisher::new(None, self.settings.capability_upgrade_sustain);
        let first = publisher
            .observe(profile, self.clock.now())
            .ok_or_else(|| SprigError::internal("the first profile always publishes"))?;
        self.registry.publish_profile(&id, &first).await?;
        let mut stored = self
            .registry
            .set_admin_state(&id, AdminState::Active, report.advisories.clone())
            .await?;
        if let Some(h) = report.health {
            stored = self
                .registry
                .publish_health(&id, h, rfc3339(self.clock.wall()))
                .await?;
        }
        Ok((stored, report))
    }
}

#[async_trait::async_trait]
impl Provisioner for ExternalProvisioner {
    fn kind(&self) -> ProvisionerKind {
        ProvisionerKind::External
    }

    fn supports(&self, op: ProvisionOp) -> bool {
        op == ProvisionOp::Register
    }

    async fn precheck(&self, req: &RegisterCluster) -> Result<PrecheckReport> {
        Ok(self.precheck_report(req).await)
    }

    async fn register(&self, req: RegisterCluster) -> Result<ClusterId> {
        let (stored, _) = self.register_report(req).await?;
        Ok(stored.body().cluster_id.clone())
    }
}

/// Input checks that need no server.
fn validate(req: &RegisterCluster, dc: &sprig_common::ids::DcId) -> Vec<String> {
    let mut out = Vec::new();
    if &req.dc != dc {
        out.push(format!(
            "the cluster is for dc {}, but this controller registers clusters of {dc}",
            req.dc
        ));
    }
    if req.name.is_empty()
        || req.name.len() > 64
        || !req
            .name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        out.push(format!(
            "name {:?}: 1 to 64 letters, digits, '-' or '_'",
            req.name
        ));
    }
    if req.nodes.is_empty() {
        out.push("a cluster needs at least one node".to_owned());
    }
    if req.topology == TopologyKind::SingleNode && req.nodes.len() > 1 {
        out.push(format!(
            "a single_node cluster has one node, not {}",
            req.nodes.len()
        ));
    }
    let mut seen: Vec<&NodeSpec> = Vec::new();
    for n in &req.nodes {
        if seen.iter().any(|s| s.node == n.node || s.addr == n.addr) {
            out.push(format!("node {} ({}) is listed twice", n.node, n.addr));
        }
        seen.push(n);
    }
    out
}

fn role_of(f: &NodeFacts) -> &'static str {
    if f.galera.is_some() {
        "galera"
    } else if f.is_replica() {
        "replica"
    } else if f.writable() {
        "writer"
    } else {
        "read_only"
    }
}

/// The inputs of [`classify`].
#[derive(Debug, Clone, Copy)]
pub struct Classify<'a> {
    /// The topology the operator gave.
    pub topology: TopologyKind,
    /// One deep probe per node.
    pub probes: &'a [NodeProbe],
    /// The profile derived from them.
    pub profile: &'a CapabilityProfile,
    /// Each node's session check, `None` where it failed.
    pub sessions: &'a [Option<SessionCheck>],
    /// `cluster.lower_case_table_names`.
    pub fleet_lower_case_table_names: u32,
    /// `feed.retention_hours`.
    pub retention_hours: u64,
}

/// Sorts probe findings into blocking items and advisories (steps 1-3 and
/// 6). Pure, so the rules are tested without servers.
#[must_use]
#[allow(clippy::too_many_lines)] // one block per precheck, as chapter 27's table
pub fn classify(c: &Classify<'_>) -> (Vec<String>, Vec<String>) {
    let gates = registration_gaps(c.probes, c.profile, c.fleet_lower_case_table_names);
    // A probe refused for a privilege (1227, 1142, 1044) logged in fine.
    let mut blocking: Vec<String> = gates
        .blocking
        .into_iter()
        .map(|line| {
            c.probes
                .iter()
                .find(|p| {
                    line.starts_with(&format!("node {}: no sprig_ctl login", p.node))
                        && p.result
                            .as_ref()
                            .err()
                            .is_some_and(|e| matches!(e.errno, Some(1227 | 1142 | 1044)))
                })
                .and_then(|p| p.result.as_ref().err().map(|e| (p, e)))
                .map_or(line.clone(), |(p, e)| {
                    format!(
                        "node {}: sprig_ctl lacks a privilege the probes need: {e}",
                        p.node
                    )
                })
        })
        .collect();
    let mut advisories = Vec::new();
    let answered: Vec<(&NodeProbe, &NodeFacts)> = c
        .probes
        .iter()
        .filter_map(|p| p.facts().map(|f| (p, f)))
        .collect();
    let session_privileges = c.sessions.iter().flatten().find_map(|s| s.privileges);
    let privileges = if answered.is_empty() {
        session_privileges
    } else {
        Some(c.profile.privileges)
    };
    let Some(p) = privileges else {
        return (blocking, advisories);
    };

    // Step 2: grants. Without these two, SprigDB can't work at all.
    let p = &p;
    if !(p.tenant_schemas_all && p.tenant_schemas_grant_option) {
        blocking.push(
            "sprig_ctl can't manage tenant schemas: GRANT ALL ON `t%`.* TO sprig_ctl WITH GRANT OPTION"
                .to_owned(),
        );
    }
    if !p.sys_schema_all {
        blocking.push(
            "sprig_ctl can't manage __sprig_sys: GRANT ALL ON `\\_\\_sprig\\_sys`.* TO sprig_ctl WITH GRANT OPTION"
                .to_owned(),
        );
    }
    advisories.extend(
        p.missing_ctl_grants()
            .into_iter()
            .filter(|g| !g.starts_with("GRANT ALL ON"))
            .map(|g| format!("sprig_ctl lacks privileges; what needs them is disabled: {g}")),
    );

    // Step 1: TLS.
    for (probe, s) in c.probes.iter().zip(c.sessions) {
        let Some(s) = s else { continue };
        if s.cipher.as_deref().is_none_or(str::is_empty) {
            blocking.push(format!(
                "node {}: the sprig_ctl session isn't encrypted; SprigDB connects over TLS only \
                 (configure ssl_cert and ssl_key on the server)",
                probe.node
            ));
        }
        if s.require_secure_transport == Some(false) {
            advisories.push(format!(
                "node {}: require_secure_transport is OFF: SET GLOBAL require_secure_transport = ON",
                probe.node
            ));
        }
    }

    // Step 3: settings, the change feed's binlog checks first.
    for (probe, f) in &answered {
        let n = &probe.node;
        for g in feed::check(f, c.retention_hours).gaps {
            let line = format!(
                "node {n}: {} is {}, needs {}: {}",
                g.variable, g.found, g.needed, g.fix
            );
            if g.severity == GapSeverity::Blocking {
                blocking.push(line);
            } else {
                advisories.push(line);
            }
        }
        let expire = f.binlog.expire_logs_seconds;
        if expire != 0 && expire < MIN_BINLOG_EXPIRE_SECONDS {
            blocking.push(format!(
                "node {n}: binlog_expire_logs_seconds is {expire}, needs at least \
                 {MIN_BINLOG_EXPIRE_SECONDS}: SET GLOBAL binlog_expire_logs_seconds = {}",
                c.retention_hours.saturating_mul(3600)
            ));
        }
        if !f.binlog.in_files() {
            blocking.push(format!(
                "node {n}: the binlog is stored in InnoDB; AFTER_SYNC and the mover need the \
                 classic file binlog (binlog_storage_engine unset)"
            ));
        }
        match (&f.galera, c.topology) {
            (Some(g), TopologyKind::Galera) if !g.gtid_mode => blocking.push(format!(
                "node {n}: wsrep_gtid_mode is OFF, needs ON: set it in the server \
                 configuration and restart"
            )),
            (Some(_), TopologyKind::Galera) | (None, _) => {}
            (Some(_), t) => blocking.push(format!(
                "node {n} runs Galera, but the topology given is {t}"
            )),
        }
        if f.galera.is_none() && c.topology == TopologyKind::Galera {
            blocking.push(format!(
                "node {n} isn't a Galera node (wsrep_on is OFF), but the topology given is galera"
            ));
        }
    }

    // Compiled logic needs triggers (chapter 09, "PL/SQL routines").
    if c.profile.features.oracle_routines && !c.profile.features.trigger_create {
        advisories.push(
            "sprig_ctl can't create triggers (1419 with the binary log on), so compiled \
             logic is off: GRANT SUPER ON *.* TO sprig_ctl, or SET GLOBAL \
             log_bin_trust_function_creators = ON"
                .to_owned(),
        );
    }

    // Topology shape.
    if c.topology == TopologyKind::PrimaryReplica && answered.len() == c.probes.len() {
        let replicas = answered.iter().filter(|(_, f)| f.is_replica()).count();
        let primaries = answered
            .iter()
            .filter(|(_, f)| !f.is_replica() && f.writable())
            .count();
        if replicas == 0 {
            blocking.push(
                "no node replicates: a primary_replica cluster needs at least one replica"
                    .to_owned(),
            );
        }
        if primaries != 1 {
            blocking.push(format!(
                "a primary_replica cluster needs one writable primary; found {primaries}"
            ));
        }
    }
    (blocking, advisories)
}

/// Step 1: the session's cipher and `require_secure_transport`.
async fn session_check(ep: &NodeEndpoint, timeout: Duration) -> Result<SessionCheck, ProbeError> {
    let mut s = ProbeSession::connect(ep, timeout).await?;
    let rows = s.rows("SHOW SESSION STATUS LIKE 'Ssl_cipher'").await;
    let rst = s
        .scalar("SELECT @@GLOBAL.require_secure_transport")
        .await
        .ok()
        .flatten();
    let privileges = s.rows("SHOW GRANTS").await.ok().map(privileges_of);
    s.close().await;
    let cipher = rows?
        .into_iter()
        .next()
        .and_then(|r| r.into_iter().nth(1))
        .and_then(|(_, v)| v);
    Ok(SessionCheck {
        cipher,
        require_secure_transport: rst.map(|v| v == "1" || v.eq_ignore_ascii_case("ON")),
        privileges,
    })
}

/// Folds `SHOW GRANTS` rows into privilege flags.
fn privileges_of(rows: Vec<sprig_cluster::sql::TextRow>) -> PrivilegeInfo {
    let grants: Vec<Grant> = rows
        .into_iter()
        .filter_map(|r| r.into_iter().next().and_then(|(_, v)| v))
        .filter_map(|l| Grant::parse(&l))
        .collect();
    PrivilegeInfo::from_grants(&grants)
}

/// Every node's `sprig_repl` endpoint, for the dump probe; `None` if the
/// credential doesn't resolve (step 5 reports that).
fn repl_endpoints(req: &RegisterCluster) -> Option<Vec<NodeEndpoint>> {
    let repl = CredentialConnector::repl(&req.secrets);
    req.nodes
        .iter()
        .map(|n| sprig_cluster::driver::NodeConnector::endpoint(&repl, n).ok())
        .collect()
}

/// Step 5: `sprig_repl` logs in and reads the binlog: chapter 09's dump
/// probe, reusing the deep probe's run on this node (`probe`) when there
/// is one, else running it here from the node's `@@gtid_binlog_pos`. Its
/// grants are checked too, for a clearer message. Returns (blocking,
/// advisories).
async fn repl_check(
    req: &RegisterCluster,
    node: &NodeSpec,
    probe: &NodeProbe,
    timeout: Duration,
) -> (Vec<String>, Vec<String>) {
    let dump = probe.active.as_ref().and_then(|a| a.binlog_dump.clone());
    let from = probe
        .facts()
        .map(|f| f.binlog.gtid_binlog_pos.clone())
        .unwrap_or_default();
    let ep = match sprig_cluster::driver::NodeConnector::endpoint(
        &CredentialConnector::repl(&req.secrets),
        node,
    ) {
        Ok(ep) => ep,
        Err(e) => {
            return (
                vec![format!("sprig_repl credential: {}", e.message)],
                Vec::new(),
            );
        }
    };
    let mut s = match ProbeSession::connect(&ep, timeout).await {
        Ok(s) => s,
        Err(e) => {
            return (
                vec![format!("node {}: sprig_repl can't log in: {e}", node.node)],
                Vec::new(),
            );
        }
    };
    let mut advisories = Vec::new();
    match s.rows("SHOW GRANTS").await {
        Ok(rows) => {
            let p = privileges_of(rows);
            if !(p.replication_slave && p.binlog_monitor) {
                advisories.push(
                    "sprig_repl can't read binlogs, so the change feed and the mover are off: \
                     GRANT REPLICATION SLAVE, BINLOG MONITOR ON *.* TO sprig_repl"
                        .to_owned(),
                );
            }
        }
        Err(e) => advisories.push(format!(
            "node {}: sprig_repl SHOW GRANTS failed: {e}",
            node.node
        )),
    }
    s.close().await;
    let dump = match dump {
        Some(d) => d,
        None => dump::run(&ep, &from, timeout).await,
    };
    let mut blocking = Vec::new();
    if let Some(f) = &dump.failure {
        let line = format!(
            "node {}: {}",
            node.node,
            dump.advisory().unwrap_or_default()
        );
        match f.kind {
            DumpFailureKind::Login | DumpFailureKind::Unreachable | DumpFailureKind::Timeout => {
                blocking.push(line);
            }
            // The grants line above already says what to grant.
            DumpFailureKind::Privilege if !advisories.is_empty() => {}
            _ => advisories.push(line),
        }
    }
    (blocking, advisories)
}

#[cfg(test)]
mod tests;

/// Tenant tables with old-format (MariaDB 5.3) fractional temporal columns.
/// Their rows can't be decoded from the binlog (binlog spike; chapter 10),
/// so they can't move or feed until rebuilt with `ALTER TABLE ... FORCE`.
/// An advisory: only the shards holding them are affected. Run once at
/// registration, not in the periodic probe, because it scans
/// `information_schema.COLUMNS`.
async fn old_temporal_check(ep: &NodeEndpoint, timeout: Duration) -> Vec<String> {
    const LIMIT: usize = 20;
    let sql = "SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME FROM information_schema.COLUMNS \
               WHERE TABLE_SCHEMA LIKE 't%' AND COLUMN_TYPE LIKE '%mariadb-5.3%' \
               AND DATETIME_PRECISION > 0 ORDER BY 1, 2 LIMIT 21";
    let Ok(mut s) = ProbeSession::connect(ep, timeout).await else {
        return Vec::new(); // account_checks already reported the login failure
    };
    let rows = s.rows(sql).await;
    s.close().await;
    let tables: Vec<String> = match rows {
        Ok(rows) => rows
            .into_iter()
            .filter_map(|r| {
                let mut it = r.into_iter().map(|(_, v)| v);
                Some(format!("`{}`.`{}`", it.next()??, it.next()??))
            })
            .collect(),
        Err(e) => {
            return vec![format!(
                "node {}: couldn't check for old-format temporal columns ({e})",
                ep.node
            )];
        }
    };
    if tables.is_empty() {
        return Vec::new();
    }
    let more = if tables.len() > LIMIT {
        " and more"
    } else {
        ""
    };
    vec![format!(
        "tables with old-format (MariaDB 5.3) fractional TIME/DATETIME/TIMESTAMP columns \
         can't be read from the binlog, so their shards can't move or feed until rebuilt \
         with ALTER TABLE ... FORCE: {}{more}",
        tables[..tables.len().min(LIMIT)].join(", ")
    )]
}
