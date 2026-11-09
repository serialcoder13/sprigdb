//! `sprig-operator`: the operator role, the Kubernetes operator.
//!
//! Layer 3 (roles). Design: [chapter 27](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/27-provisioning.md) (Provisioning) of the SprigDB low-level design.
//!
//! `sprigdb operator` turns `SprigCluster` objects into mariadb-operator
//! `MariaDB` resources, one Kubernetes cluster per DC (chapter 27 step 10):
//!
//! - [`crd`]: `SprigCluster` and `SprigClusterPool` (`sprigdb.io/v1alpha1`)
//!   and their CRDs; [`yaml`] writes them for the Helm chart;
//! - [`mapping`]: the mapping table to `MariaDB`, `PhysicalBackup`,
//!   `PointInTimeRecovery` and the per-pod Services;
//! - [`kube`]: a small REST client ([`kube::KubeApi`], [`kube::HttpKube`]);
//! - [`reconcile`]: the reconcile steps and pools; [`accounts`] (system
//!   accounts) and [`register`] (the DC controller) are its seams;
//! - [`lease`]: leader election through a `Lease`;
//! - [`provisioner`]: the `Provisioner` trait over the custom resources.
//!
//! [`run`] reconciles when the process runs in a pod (the service-account
//! token and `KUBERNETES_SERVICE_HOST`); elsewhere it reports ready and
//! waits, as the skeleton did. It polls every [`RESYNC`] rather than
//! watching: a handful of objects per DC.

pub mod accounts;
pub mod crd;
pub mod failover;
pub mod kube;
pub mod lease;
pub mod mapping;
pub mod provisioner;
pub mod reconcile;
pub mod register;
pub mod yaml;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_common::time::SystemClock;
use sprig_common::{Result, Role, RoleContext};

use crate::kube::{HttpKube, KubeApi, KubeConfig};
use crate::reconcile::Reconciler;

/// How often the loop lists the custom resources and reconciles.
pub const RESYNC: Duration = Duration::from_secs(5);
/// How often the failover watch looks at each primary ([`watch_all`]).
pub const WATCH: Duration = Duration::from_secs(1);

/// The operator's settings, from its pod's environment (the Helm chart's
/// `operator.yaml`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorConfig {
    /// The namespace it works in.
    pub namespace: String,
    /// `values.nodeServices.type`: `LoadBalancer` (default), `NodePort` or
    /// `ClusterIP`.
    pub node_service_type: String,
    /// For `NodePort` node Services: the host every node port is reached
    /// at (`SPRIG_OPERATOR_NODE_PORT_HOST`), advertised as
    /// `<host>:<nodePort>`. Unset: the cluster IP, as for `ClusterIP`.
    pub node_port_host: Option<String>,
    /// cert-manager `ClusterIssuer` for MariaDB server certificates.
    pub server_cert_issuer: Option<String>,
    /// cert-manager `ClusterIssuer` for client certificates.
    pub client_cert_issuer: Option<String>,
    /// The SprigDB image for the fence-only agent sidecar; none without.
    pub agent_image: Option<String>,
    /// The `ConfigMap` with the agent's `sprigdb.toml`.
    pub agent_config_map: String,
    /// The Secret holding every cluster's credentials file.
    pub credentials_secret: String,
    /// Where controllers mount that Secret.
    pub creds_dir: String,
    /// The CA file controllers verify MariaDB with (`None`: development,
    /// `skip_verify`).
    pub mariadb_ca_file: Option<String>,
    /// The cluster DNS domain.
    pub cluster_domain: String,
    /// The DC controller's Admin API, `http://host:port`.
    pub controller_admin: Option<String>,
    /// How long a `MariaDB` may take to be Ready.
    pub ready_timeout: Duration,
    /// How long the primary must not answer, with its pod not Ready, before
    /// the failover watch promotes a replica (chapter 25's
    /// `failover.cluster_detect_seconds`).
    pub failover_detect: Duration,
}

impl OperatorConfig {
    /// The defaults in namespace `ns`.
    #[must_use]
    pub fn new(ns: &str) -> Self {
        Self {
            namespace: ns.to_owned(),
            node_service_type: "LoadBalancer".into(),
            node_port_host: None,
            server_cert_issuer: None,
            client_cert_issuer: None,
            agent_image: None,
            agent_config_map: "sprigdb-agent".into(),
            credentials_secret: "sprigdb-cluster-credentials".into(),
            creds_dir: "/etc/sprigdb/creds".into(),
            mariadb_ca_file: None,
            cluster_domain: "cluster.local".into(),
            controller_admin: None,
            ready_timeout: Duration::from_secs(30 * 60),
            failover_detect: Duration::from_secs(5),
        }
    }

    /// From `SPRIG_OPERATOR_*` variables over [`Self::new`].
    #[must_use]
    pub fn from_env(ns: &str, env: &dyn Fn(&str) -> Option<String>) -> Self {
        let mut c = Self::new(ns);
        let set = |k: &str, f: &mut String| {
            if let Some(v) = env(k).filter(|v| !v.is_empty()) {
                *f = v;
            }
        };
        set("SPRIG_OPERATOR_NODE_SERVICE_TYPE", &mut c.node_service_type);
        set("SPRIG_OPERATOR_AGENT_CONFIG_MAP", &mut c.agent_config_map);
        set(
            "SPRIG_OPERATOR_CREDENTIALS_SECRET",
            &mut c.credentials_secret,
        );
        set("SPRIG_OPERATOR_CREDS_DIR", &mut c.creds_dir);
        set("SPRIG_OPERATOR_CLUSTER_DOMAIN", &mut c.cluster_domain);
        let opt = |k: &str| env(k).filter(|v| !v.is_empty());
        c.server_cert_issuer = opt("SPRIG_OPERATOR_SERVER_CERT_ISSUER");
        c.client_cert_issuer = opt("SPRIG_OPERATOR_CLIENT_CERT_ISSUER");
        c.agent_image = opt("SPRIG_OPERATOR_AGENT_IMAGE");
        c.mariadb_ca_file = opt("SPRIG_OPERATOR_MARIADB_CA_FILE");
        c.node_port_host = opt("SPRIG_OPERATOR_NODE_PORT_HOST");
        c.controller_admin = opt("SPRIG_OPERATOR_CONTROLLER_ADMIN");
        if let Some(s) = opt("SPRIG_OPERATOR_READY_TIMEOUT_SECONDS").and_then(|s| s.parse().ok()) {
            c.ready_timeout = Duration::from_secs(s);
        }
        if let Some(s) = opt("SPRIG_OPERATOR_FAILOVER_DETECT_MS").and_then(|s| s.parse().ok()) {
            c.failover_detect = Duration::from_millis(s);
        }
        c
    }
}

/// Lists and reconciles every pool and cluster once; returns how many
/// clusters were reconciled. `due` keeps each cluster's next time.
///
/// # Errors
/// A failed list; individual reconcile errors are logged and retried.
#[allow(clippy::implicit_hasher)]
pub async fn pass(rec: &Reconciler, due: &mut HashMap<String, Instant>) -> Result<usize> {
    let ns = rec.config().namespace.clone();
    let kube = Arc::clone(rec.kube());
    let pools = kube
        .list(&crd::SPRIG_CLUSTER_POOL, &ns, None)
        .await
        .map_err(sprig_common::SprigError::from)?;
    for p in &pools {
        if let Err(e) = rec.reconcile_pool(p).await {
            tracing::warn!(pool = %p["metadata"]["name"], error = %e, "pool reconcile failed");
        }
    }
    let clusters = kube
        .list(&crd::SPRIG_CLUSTER, &ns, None)
        .await
        .map_err(sprig_common::SprigError::from)?;
    let now = Instant::now();
    let mut n = 0;
    for c in &clusters {
        let name = c["metadata"]["name"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let generation_key = format!("{name}@{}", c["metadata"]["generation"]);
        if due.get(&generation_key).is_some_and(|t| *t > now) {
            continue;
        }
        let next = match rec.reconcile(c).await {
            Ok(o) => {
                tracing::debug!(cluster = %name, phase = o.phase.as_str(), "reconciled");
                o.requeue
            }
            Err(e) => {
                tracing::warn!(cluster = %name, error = %e, "reconcile failed");
                Duration::from_secs(10)
            }
        };
        due.retain(|k, _| !k.starts_with(&format!("{name}@")));
        due.insert(generation_key, now + next);
        n += 1;
    }
    Ok(n)
}

/// One failover-watch pass over every `SprigCluster`
/// ([`Reconciler::watch`]); errors are logged.
///
/// # Errors
/// A failed list.
pub async fn watch_all(rec: &Reconciler) -> Result<()> {
    let ns = rec.config().namespace.clone();
    let clusters = rec
        .kube()
        .list(&crd::SPRIG_CLUSTER, &ns, None)
        .await
        .map_err(sprig_common::SprigError::from)?;
    for c in &clusters {
        match rec.watch(c).await {
            Ok(reconcile::WatchOutcome::Skipped | reconcile::WatchOutcome::Healthy { .. }) => {}
            Ok(o) => {
                tracing::info!(cluster = %c["metadata"]["name"], outcome = ?o, "failover watch");
            }
            Err(e) => {
                tracing::warn!(cluster = %c["metadata"]["name"], error = %e, "failover watch failed");
            }
        }
    }
    Ok(())
}

/// Runs the operator role until `ctx.shutdown` is cancelled.
///
/// The `sprigdb` binary has already bound the telemetry port, so
/// `/healthz` answers and `/readyz` reports `starting` until this marks the
/// process ready.
///
/// # Errors
/// A bad in-cluster configuration.
pub async fn run(ctx: RoleContext) -> Result<()> {
    let role = Role::Operator;
    let env = |k: &str| std::env::var(k).ok();
    let Some(kcfg) = KubeConfig::in_cluster(&env, Path::new(kube::SA_DIR)) else {
        tracing::warn!(%role, "not in a Kubernetes pod (no service-account token): nothing to reconcile");
        ctx.health.set_ready();
        ctx.shutdown.cancelled().await;
        ctx.health.set_draining();
        return Ok(());
    };
    let cfg = OperatorConfig::from_env(&kcfg.namespace, &env);
    let kube: Arc<dyn KubeApi> =
        Arc::new(HttpKube::new(kcfg).map_err(sprig_common::SprigError::from)?);
    let registrar: Arc<dyn register::ClusterRegistrar> = match &cfg.controller_admin {
        Some(ep) => Arc::new(register::AdminRegistrar::new(
            ep,
            env("SPRIG_OPERATOR_ADMIN_TOKEN"),
        )?),
        None => {
            return Err(sprig_common::SprigError::new(
                sprig_common::SprigCode::ConfigInvalid,
                "SPRIG_OPERATOR_CONTROLLER_ADMIN must name the DC controller's Admin API",
            ));
        }
    };
    let accounts: Arc<dyn accounts::SystemAccounts> = Arc::new(accounts::MysqlAccounts::new(
        cfg.mariadb_ca_file.as_ref().map(Into::into),
    ));
    let clock = Arc::new(SystemClock);
    let identity = env("POD_NAME").unwrap_or_else(|| ctx.config.node.id());
    let elector = lease::LeaseElector::new(
        Arc::clone(&kube),
        &cfg.namespace,
        "sprigdb-operator",
        &identity,
        Duration::from_secs(15),
        clock.clone(),
    );
    let repl: Arc<dyn failover::ReplicationOps> = Arc::new(failover::MysqlReplication::new(
        cfg.mariadb_ca_file.as_ref().map(Into::into),
    ));
    let rec = Reconciler::new(kube, accounts, registrar, cfg, clock).with_replication(repl);
    ctx.health.set_ready();
    tracing::info!(%role, %identity, "ready");
    let mut due = HashMap::new();
    let mut tick = tokio::time::interval(WATCH);
    // A failover can take 20 s; don't replay the missed ticks in a burst.
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_pass: Option<Instant> = None;
    let mut leading = false;
    loop {
        tokio::select! {
            () = ctx.shutdown.cancelled() => break,
            _ = tick.tick() => {}
        }
        ctx.health.tick();
        // The lease renews every RESYNC; the watch runs every WATCH while
        // this operator leads.
        if last_pass.is_none_or(|t| t.elapsed() >= RESYNC) {
            last_pass = Some(Instant::now());
            match elector.try_lead().await {
                Ok(true) => {
                    leading = true;
                    if let Err(e) = pass(&rec, &mut due).await {
                        tracing::warn!(error = %e, "operator pass failed");
                    }
                }
                Ok(false) => {
                    leading = false;
                    due.clear();
                }
                Err(e) => {
                    leading = false;
                    tracing::warn!(error = %e, "lease");
                }
            }
        }
        if leading && let Err(e) = watch_all(&rec).await {
            tracing::warn!(error = %e, "failover watch pass failed");
        }
    }
    ctx.health.set_draining();
    tracing::info!(%role, "draining");
    Ok(())
}
