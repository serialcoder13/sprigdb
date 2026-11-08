//! The capability profile: `derive()` from probe results, profile diffs and
//! publishing with hysteresis (chapter 09, "Capability profile", "Deriving
//! capabilities", "Publishing"; build step 3).
//!
//! Consumers read profile fields, never topology names or versions.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::probe::feed::{self, FeedLevel};
use crate::probe::{
    ActiveProbes, BinlogFormat, FeatureFlags, NodeFacts, NodeProbe, PrivilegeInfo, RoutineLimits,
    RowImage, RowMetadata, ServerVersion, TableCacheInfo, WaitPoint,
};
use crate::record::{Declared, TopologyKind, Trust};

/// Server identity, from the probed nodes.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ServerInfo {
    /// The lowest version across the nodes.
    pub version: Option<ServerVersion>,
    /// The writer's `@@version` text.
    pub version_text: String,
    /// `lower_case_table_names`.
    pub lower_case_table_names: u8,
    /// `default_storage_engine`.
    pub default_storage_engine: String,
    /// Largest clock skew seen against this controller, absolute, ms.
    pub max_clock_skew_ms: Option<u64>,
}

/// Galera facts.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct GaleraInfo {
    /// `wsrep_cluster_size` seen by the writer.
    pub size: u32,
    /// Nodes ready, `Primary` and `Synced`.
    pub synced: u32,
    /// `wsrep_gtid_mode` on every node.
    pub gtid_mode: bool,
    /// Every node has the same `wsrep_gtid_domain_id`: binlog readers can
    /// switch nodes at one position.
    pub shared_gtid_domain: bool,
    /// `wsrep_cluster_conf_id`.
    pub conf_id: u64,
}

/// Semi-sync on the writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SemiSyncInfo {
    /// `rpl_semi_sync_master_enabled`.
    pub enabled: bool,
    /// `Rpl_semi_sync_master_status`: off while enabled means fallen back
    /// to asynchronous.
    pub active: bool,
    /// `rpl_semi_sync_master_wait_point`.
    pub wait_point: Option<WaitPoint>,
    /// `Rpl_semi_sync_master_clients`.
    pub clients: u32,
}

/// Binlog facts of the binlog source.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct BinlogInfo {
    /// `log_bin`.
    pub log_bin: bool,
    /// `binlog_format`.
    pub format: Option<BinlogFormat>,
    /// `binlog_row_image`.
    pub row_image: Option<RowImage>,
    /// `binlog_row_metadata`.
    pub row_metadata: Option<RowMetadata>,
    /// `gtid_strict_mode`.
    pub gtid_strict_mode: bool,
    /// `log_slave_updates` on the source.
    pub log_slave_updates: bool,
    /// `binlog_expire_logs_seconds`; 0 means never.
    pub expire_logs_seconds: u64,
    /// The binlog is in files (not the 12.3+ `InnoDB` binlog).
    pub in_files: bool,
    /// `BINLOG_GTID_POS(oldest file, 4)`.
    pub horizon: Option<String>,
}

/// Expected data loss on a failover.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LossClass {
    /// No acknowledged commit is lost.
    Zero,
    /// Asynchronous replicas: recent commits may be lost.
    Async,
    /// No replica: everything since the last backup.
    NoReplica,
    /// As declared (`rpo_seconds`) for an external cluster.
    Declared,
    /// Unknown.
    #[default]
    Unknown,
}

/// How a read sees the session's own writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RywMethod {
    /// Galera `wsrep_sync_wait`.
    WsrepSyncWait,
    /// `MASTER_GTID_WAIT` on a replica.
    GtidWait,
    /// Read from the writer.
    #[default]
    WriterOnly,
}

/// A way to fence one shard (chapter 09, "Fencing primitives").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FenceMethod {
    /// Revoke the tenant's write grants and kill its sessions.
    RevokeKill,
    /// Rename the tables away.
    Rename,
}

/// Where automatic-index validation builds run (chapter 13).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationMethod {
    /// A Galera node desynced for a rolling schema upgrade.
    GaleraRsu,
    /// A replica taken out for the build.
    ReplicaBuild,
}

/// What automation may rely on.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct Derived {
    /// XA works and survives disconnects.
    pub xa: bool,
    /// A failover loses no acknowledged commit.
    pub zero_loss_failover: bool,
    /// Expected loss on failover.
    pub expected_loss: LossClass,
    /// Read-your-writes method.
    pub ryw: RywMethod,
    /// Shards may be moved off this cluster.
    pub mover_source: bool,
    /// Shards may be moved onto this cluster.
    pub mover_target: bool,
    /// The change feed can read here (its level is at least `degraded`).
    pub feed_source: bool,
    /// What the change feed can do here (chapter 38).
    #[serde(default)]
    pub feed_level: FeedLevel,
    /// How far back the feed can resume from the binlog, seconds; 0 when
    /// logs never expire.
    pub feed_retention_secs: u64,
    /// Fence methods available.
    pub fence_methods: Vec<FenceMethod>,
    /// Index validation method.
    pub validation: Option<ValidationMethod>,
    /// Indexes can be built online here (not Galera).
    pub online_index_build: bool,
    /// Validators may become `CHECK (JSON_SCHEMA_VALID(...))`.
    pub enforce_json_schema: bool,
    /// Logic may compile to routines here (chapter 41).
    pub compiled_logic: bool,
}

/// The capability profile.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct CapabilityProfile {
    /// Raised on every publication.
    pub version: u64,
    /// Server identity.
    pub server: ServerInfo,
    /// Galera facts, on Galera.
    pub galera: Option<GaleraInfo>,
    /// Semi-sync.
    pub semi_sync: SemiSyncInfo,
    /// Binlog.
    pub binlog: BinlogInfo,
    /// The control account's privileges.
    pub privileges: PrivilegeInfo,
    /// Feature flags.
    pub features: FeatureFlags,
    /// Routine limits.
    pub routines: RoutineLimits,
    /// Table cache variables and counters (not compared by [`diff`]).
    pub tables: TableCacheInfo,
    /// What automation relies on.
    pub derived: Derived,
}

/// The inputs of [`derive()`].
#[derive(Debug, Clone, Copy)]
pub struct DeriveInput<'a> {
    /// The record's topology.
    pub topology: TopologyKind,
    /// This pass's probes, one per node.
    pub probes: &'a [NodeProbe],
    /// `[declared]` of an external cluster.
    pub declared: Option<&'a Declared>,
    /// The published profile: facts that only deep probes collect are
    /// carried over from it when this pass didn't run them.
    pub previous: Option<&'a CapabilityProfile>,
}

/// Index of the node whose facts stand for the cluster: the first usable
/// Galera node, else the first writable non-replica, else the first node
/// that answered.
fn writer_index(topology: TopologyKind, probes: &[NodeProbe]) -> Option<usize> {
    crate::probe::active_target(topology, probes)
        .or_else(|| probes.iter().position(|p| p.facts().is_some()))
}

/// Derives a profile from one pass of probes (chapter 09, "Deriving
/// capabilities"). The returned `version` is 0; publishing sets it.
#[must_use]
#[allow(clippy::too_many_lines)] // one block, like the chapter's pseudo-code
pub fn derive(input: DeriveInput<'_>) -> CapabilityProfile {
    let DeriveInput {
        topology,
        probes,
        declared,
        previous,
    } = input;
    let prev = previous.cloned().unwrap_or_default();
    let answered: Vec<&NodeFacts> = probes.iter().filter_map(NodeProbe::facts).collect();
    let Some(w) = writer_index(topology, probes) else {
        // Nothing answered: nothing can be relied on, except declarations.
        let mut p = CapabilityProfile {
            server: prev.server,
            privileges: prev.privileges,
            features: prev.features,
            routines: prev.routines,
            ..CapabilityProfile::default()
        };
        p.derived.expected_loss = LossClass::Unknown;
        p.derived.fence_methods = Vec::new();
        return p;
    };
    let writer = probes[w].facts().unwrap_or_else(|| answered[0]);
    let active: Option<&ActiveProbes> = probes.iter().find_map(|p| p.active.as_ref());
    let galera_on = answered.iter().any(|f| f.galera.is_some()) || topology == TopologyKind::Galera;
    let replicas: Vec<&NodeFacts> = answered
        .iter()
        .copied()
        .filter(|f| f.is_replica())
        .collect();

    let server = ServerInfo {
        version: answered.iter().filter_map(|f| f.server.version).min(),
        version_text: writer.server.version_text.clone(),
        lower_case_table_names: writer.server.lower_case_table_names,
        default_storage_engine: writer.server.default_storage_engine.clone(),
        max_clock_skew_ms: answered
            .iter()
            .filter_map(|f| f.server.clock_skew_ms)
            .map(i64::unsigned_abs)
            .max()
            .or(prev.server.max_clock_skew_ms),
    };

    let galera = galera_on.then(|| {
        let g = writer.galera.clone();
        let domains: Vec<Option<u32>> = answered
            .iter()
            .filter_map(|f| f.galera.as_ref().map(|g| g.gtid_domain_id))
            .collect();
        GaleraInfo {
            size: g.as_ref().map_or(0, |g| g.cluster_size),
            synced: u32::try_from(
                answered
                    .iter()
                    .filter(|f| {
                        f.galera
                            .as_ref()
                            .is_some_and(super::probe::GaleraStatus::usable)
                    })
                    .count(),
            )
            .unwrap_or(u32::MAX),
            gtid_mode: answered
                .iter()
                .all(|f| f.galera.as_ref().is_some_and(|g| g.gtid_mode)),
            shared_gtid_domain: !domains.is_empty()
                && domains[0].is_some()
                && domains.iter().all(|d| *d == domains[0]),
            conf_id: g.as_ref().map_or(0, |g| g.conf_id),
        }
    });

    let semi_sync = SemiSyncInfo {
        enabled: writer.semi_sync.master_enabled,
        active: writer.semi_sync.master_active,
        wait_point: writer.semi_sync.wait_point,
        clients: writer.semi_sync.master_clients,
    };

    let b = &writer.binlog;
    let binlog = BinlogInfo {
        log_bin: b.log_bin,
        format: b.format,
        row_image: b.row_image,
        row_metadata: b.row_metadata,
        gtid_strict_mode: b.gtid_strict_mode,
        log_slave_updates: b.log_slave_updates,
        expire_logs_seconds: b.expire_logs_seconds,
        in_files: b.in_files(),
        horizon: b.horizon.clone().or(prev.binlog.horizon),
    };

    let mut privileges = writer.privileges.unwrap_or(prev.privileges);
    if let Some(a) = active {
        privileges.sql_log_bin_settable = Some(a.sql_log_bin_settable);
        privileges.osu_method_settable = a.osu_method_settable;
    } else {
        // Only the deep probe tests these; keep what it found.
        privileges.sql_log_bin_settable = prev.privileges.sql_log_bin_settable;
        privileges.osu_method_settable = prev.privileges.osu_method_settable;
    }

    let mut features = active.map_or(prev.features, |a| a.features);
    features.alter_two_phase = writer.server.binlog_alter_two_phase.is_some();
    features.performance_schema = writer.server.performance_schema;

    // xa := xa_probe.ok AND NOT legacy_xa_rollback_at_disconnect (checked
    //       only when present: 11.8 and 12.3 don't have the variable; XA spike)
    //       AND NOT (binlog in InnoDB AND replicas exist)
    // With 12.3's InnoDB-stored binlog nothing is logged at XA PREPARE, so a
    // prepared branch never reaches a replica and can't survive a failover
    // (binlog spike; chapters 09 and 18). A single node keeps XA.
    let xa_probe = active.map_or(prev.derived.xa, |a| a.xa);
    let prepared_survives_failover = probes.len() <= 1 || b.in_files();
    let xa = xa_probe
        && writer.server.legacy_xa_rollback_at_disconnect != Some(true)
        && prepared_survives_failover;

    let all_synced = !probes.is_empty()
        && probes.iter().all(|p| {
            p.facts()
                .and_then(|f| f.galera.as_ref())
                .is_some_and(super::probe::GaleraStatus::usable)
        });
    let semi_sync_zero = semi_sync.enabled
        && semi_sync.active
        && semi_sync.clients >= 1
        && semi_sync.wait_point == Some(WaitPoint::AfterSync)
        && binlog.in_files;
    let declared_zero =
        declared.is_some_and(|d| d.rpo_seconds == Some(0) && d.trust == Some(Trust::Asserted));
    let zero_loss_failover = match topology {
        TopologyKind::Galera => all_synced,
        TopologyKind::PrimaryReplica => semi_sync_zero,
        TopologyKind::SingleNode => false,
        TopologyKind::External => declared_zero || semi_sync_zero,
    };
    let expected_loss = if zero_loss_failover {
        LossClass::Zero
    } else {
        match topology {
            TopologyKind::Galera | TopologyKind::PrimaryReplica => LossClass::Async,
            TopologyKind::SingleNode => LossClass::NoReplica,
            TopologyKind::External => match declared.and_then(|d| d.rpo_seconds) {
                Some(_) => LossClass::Declared,
                None => LossClass::Unknown,
            },
        }
    };
    let ryw = if galera_on {
        RywMethod::WsrepSyncWait
    } else if !replicas.is_empty() {
        RywMethod::GtidWait
    } else {
        RywMethod::WriterOnly
    };

    // mover_source := a node with log_bin, ROW, FULL image, gtid_strict_mode,
    // log_slave_updates (replica) and a passing dump probe. The settings
    // half is the feed check (chapter 38), which also asks Galera nodes for
    // log_slave_updates.
    // The dump probe (`ActiveProbes::binlog_dump`) runs where a `sprig_repl`
    // endpoint was given; not run, or inconclusive (a timeout or a lost
    // connection), leaves the settings to decide.
    let dump_ok = !active
        .and_then(|a| a.binlog_dump.as_ref())
        .is_some_and(crate::probe::DumpProbe::blocks_feed);
    let feed_level = if dump_ok {
        answered
            .iter()
            .map(|f| feed::check(f, feed::DEFAULT_RETENTION_HOURS).level)
            .max()
            .unwrap_or_default()
    } else {
        FeedLevel::Unsupported
    };
    let mover_source = feed_level != FeedLevel::Unsupported;

    let mut fence_methods = vec![FenceMethod::Rename];
    if privileges.create_user
        && privileges.tenant_schemas_grant_option
        && privileges.connection_admin
    {
        fence_methods.insert(0, FenceMethod::RevokeKill);
    }
    let validation = if galera_on {
        galera
            .as_ref()
            .is_some_and(|g| g.size >= 3)
            .then_some(ValidationMethod::GaleraRsu)
    } else if !replicas.is_empty() && privileges.binlog_admin && privileges.read_only_admin {
        Some(ValidationMethod::ReplicaBuild)
    } else {
        None
    };

    let derived = Derived {
        xa,
        zero_loss_failover,
        expected_loss,
        ryw,
        mover_source,
        mover_target: privileges.tenant_schemas_all && privileges.tenant_schemas_grant_option,
        feed_source: mover_source,
        feed_level,
        // TODO(sprig-binlog): the book's value is now minus the commit time
        // of the horizon's first event, which needs a binlog read; until
        // then the expiry setting is the upper bound.
        feed_retention_secs: binlog.expire_logs_seconds,
        fence_methods,
        validation,
        online_index_build: !galera_on,
        enforce_json_schema: features.json_schema_valid,
        compiled_logic: features.oracle_routines && features.trigger_create && privileges.set_user,
    };

    CapabilityProfile {
        version: 0,
        server,
        galera,
        semi_sync,
        binlog,
        privileges,
        features,
        routines: writer.routines,
        tables: writer.tables,
        derived,
    }
}

/// Registration gates and advisories (chapter 09, "Registration").
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Gaps {
    /// Gates that reject the cluster: version below 11.8,
    /// `lower_case_table_names` mismatch, default engine not `InnoDB`, no
    /// login.
    pub blocking: Vec<String>,
    /// Missing privileges, each with the `GRANT` that fixes it; they
    /// disable only what needs them.
    pub advisories: Vec<String>,
}

/// Checks the registration gates.
#[must_use]
pub fn registration_gaps(
    probes: &[NodeProbe],
    profile: &CapabilityProfile,
    fleet_lower_case_table_names: u32,
) -> Gaps {
    let mut g = Gaps::default();
    for p in probes {
        if let Err(e) = &p.result {
            g.blocking
                .push(format!("node {}: no sprig_ctl login: {e}", p.node));
        }
    }
    if probes.iter().all(|p| p.result.is_err()) {
        return g;
    }
    match profile.server.version {
        Some(v) if v >= ServerVersion::MIN_SUPPORTED => {}
        Some(v) => g.blocking.push(format!(
            "MariaDB {v} is below {}",
            ServerVersion::MIN_SUPPORTED
        )),
        None => g.blocking.push("MariaDB version unknown".to_owned()),
    }
    for p in probes {
        if let Some(f) = p.facts()
            && u32::from(f.server.lower_case_table_names) != fleet_lower_case_table_names
        {
            g.blocking.push(format!(
                "node {}: lower_case_table_names = {}, the fleet uses {fleet_lower_case_table_names}",
                p.node, f.server.lower_case_table_names
            ));
        }
    }
    if !profile
        .server
        .default_storage_engine
        .eq_ignore_ascii_case("InnoDB")
    {
        g.blocking.push(format!(
            "default_storage_engine is {:?}, not InnoDB",
            profile.server.default_storage_engine
        ));
    }
    g.advisories = profile.privileges.missing_ctl_grants();
    g
}

/// Whether a change in a profile field is an improvement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    /// A capability appeared.
    Upgrade,
    /// A capability disappeared.
    Downgrade,
    /// A value changed.
    Changed,
}

/// One changed field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldChange {
    /// Dotted path, such as `derived.zero_loss_failover`.
    pub field: String,
    /// Which way.
    pub direction: Direction,
    /// Old value, as text.
    pub old: String,
    /// New value, as text.
    pub new: String,
}

type Flag = (
    &'static str,
    fn(&CapabilityProfile) -> bool,
    fn(&mut CapabilityProfile, bool),
);

macro_rules! flag {
    ($name:literal, $($f:ident).+) => {
        (
            $name,
            |p: &CapabilityProfile| p.$($f).+,
            |p: &mut CapabilityProfile, v: bool| p.$($f).+ = v,
        )
    };
}

fn has_fence(p: &CapabilityProfile, m: FenceMethod) -> bool {
    p.derived.fence_methods.contains(&m)
}

fn set_fence(p: &mut CapabilityProfile, m: FenceMethod, on: bool) {
    p.derived.fence_methods.retain(|x| *x != m);
    if on {
        p.derived.fence_methods.push(m);
        p.derived
            .fence_methods
            .sort_by_key(|m| *m != FenceMethod::RevokeKill);
    }
}

/// Boolean capabilities: an upgrade is held, a downgrade published at once.
const FLAGS: &[Flag] = &[
    flag!("derived.xa", derived.xa),
    flag!("derived.zero_loss_failover", derived.zero_loss_failover),
    flag!("derived.mover_source", derived.mover_source),
    flag!("derived.mover_target", derived.mover_target),
    flag!("derived.feed_source", derived.feed_source),
    flag!("derived.online_index_build", derived.online_index_build),
    flag!("derived.enforce_json_schema", derived.enforce_json_schema),
    flag!("derived.compiled_logic", derived.compiled_logic),
    (
        "derived.fence_methods.revoke_kill",
        |p| has_fence(p, FenceMethod::RevokeKill),
        |p, v| set_fence(p, FenceMethod::RevokeKill, v),
    ),
    (
        "derived.fence_methods.rename",
        |p| has_fence(p, FenceMethod::Rename),
        |p, v| set_fence(p, FenceMethod::Rename, v),
    ),
    flag!("semi_sync.enabled", semi_sync.enabled),
    flag!("semi_sync.active", semi_sync.active),
    flag!("binlog.log_bin", binlog.log_bin),
    flag!("binlog.gtid_strict_mode", binlog.gtid_strict_mode),
    flag!("binlog.log_slave_updates", binlog.log_slave_updates),
    flag!("binlog.in_files", binlog.in_files),
    flag!("features.ignored_indexes", features.ignored_indexes),
    flag!(
        "features.instant_ignored_toggle",
        features.instant_ignored_toggle
    ),
    flag!("features.vector", features.vector),
    flag!(
        "features.online_spatial_index",
        features.online_spatial_index
    ),
    flag!("features.alter_two_phase", features.alter_two_phase),
    flag!("features.performance_schema", features.performance_schema),
    flag!("features.json_schema_valid", features.json_schema_valid),
    flag!("features.json_table", features.json_table),
    flag!("features.vcol_index", features.vcol_index),
    flag!("features.vcol_index_online", features.vcol_index_online),
    flag!("features.oracle_routines", features.oracle_routines),
    flag!("features.trigger_create", features.trigger_create),
    flag!(
        "features.oracle_routines_any_mode",
        features.oracle_routines_any_mode
    ),
    flag!(
        "privileges.tenant_schemas_all",
        privileges.tenant_schemas_all
    ),
    flag!(
        "privileges.tenant_schemas_grant_option",
        privileges.tenant_schemas_grant_option
    ),
    flag!("privileges.sys_schema_all", privileges.sys_schema_all),
    flag!("privileges.create_user", privileges.create_user),
    flag!("privileges.process", privileges.process),
    flag!("privileges.connection_admin", privileges.connection_admin),
    flag!("privileges.slave_monitor", privileges.slave_monitor),
    flag!("privileges.binlog_monitor", privileges.binlog_monitor),
    flag!("privileges.binlog_admin", privileges.binlog_admin),
    flag!("privileges.read_only_admin", privileges.read_only_admin),
    flag!("privileges.set_user", privileges.set_user),
    flag!("privileges.replication_slave", privileges.replication_slave),
    flag!("privileges.read_mysql_db", privileges.read_mysql_db),
];

fn text<T: Serialize>(v: &T) -> String {
    match serde_json::to_value(v) {
        Ok(serde_json::Value::String(s)) => s,
        Ok(other) => other.to_string(),
        Err(_) => String::new(),
    }
}

/// Values compared as text; any change is published at once.
fn values(p: &CapabilityProfile) -> Vec<(&'static str, String)> {
    vec![
        (
            "server.version",
            text(&p.server.version.map(|v| v.to_string())),
        ),
        (
            "server.lower_case_table_names",
            text(&p.server.lower_case_table_names),
        ),
        (
            "server.default_storage_engine",
            p.server.default_storage_engine.clone(),
        ),
        ("galera.size", text(&p.galera.as_ref().map(|g| g.size))),
        (
            "galera.gtid_mode",
            text(&p.galera.as_ref().map(|g| g.gtid_mode)),
        ),
        (
            "galera.shared_gtid_domain",
            text(&p.galera.as_ref().map(|g| g.shared_gtid_domain)),
        ),
        ("semi_sync.wait_point", text(&p.semi_sync.wait_point)),
        ("binlog.format", text(&p.binlog.format)),
        ("binlog.row_image", text(&p.binlog.row_image)),
        ("binlog.row_metadata", text(&p.binlog.row_metadata)),
        (
            "binlog.expire_logs_seconds",
            text(&p.binlog.expire_logs_seconds),
        ),
        (
            "privileges.sql_log_bin_settable",
            text(&p.privileges.sql_log_bin_settable),
        ),
        (
            "privileges.osu_method_settable",
            text(&p.privileges.osu_method_settable),
        ),
        (
            "routines.stored_program_cache",
            text(&p.routines.stored_program_cache),
        ),
        (
            "routines.max_sp_recursion_depth",
            text(&p.routines.max_sp_recursion_depth),
        ),
        (
            "routines.max_statement_time_ms",
            text(&p.routines.max_statement_time_ms),
        ),
        ("derived.expected_loss", text(&p.derived.expected_loss)),
        ("derived.ryw", text(&p.derived.ryw)),
        ("derived.feed_level", text(&p.derived.feed_level)),
        (
            "derived.feed_retention_secs",
            text(&p.derived.feed_retention_secs),
        ),
        ("derived.validation", text(&p.derived.validation)),
    ]
}

/// The capability changes from `old` to `new`. Counters (`tables`), the
/// clock skew, the binlog horizon and Galera membership counts are not
/// capabilities and are ignored.
#[must_use]
pub fn diff(old: &CapabilityProfile, new: &CapabilityProfile) -> Vec<FieldChange> {
    let mut out = Vec::new();
    for (name, get, _) in FLAGS {
        let (o, n) = (get(old), get(new));
        if o != n {
            out.push(FieldChange {
                field: (*name).to_owned(),
                direction: if n {
                    Direction::Upgrade
                } else {
                    Direction::Downgrade
                },
                old: o.to_string(),
                new: n.to_string(),
            });
        }
    }
    for ((name, o), (_, n)) in values(old).into_iter().zip(values(new)) {
        if o != n {
            out.push(FieldChange {
                field: name.to_owned(),
                direction: Direction::Changed,
                old: o,
                new: n,
            });
        }
    }
    out
}

/// A profile to publish and what changed.
#[derive(Debug, Clone, PartialEq)]
pub struct Publication {
    /// The profile, with its new version.
    pub profile: CapabilityProfile,
    /// What changed against the previously published profile; empty for
    /// the first one.
    pub changes: Vec<FieldChange>,
}

/// Publishing with hysteresis: a downgrade is published at once; an upgrade
/// only after it held for `cluster.capability_upgrade_sustain_seconds`, so a
/// flapping capability is seen at its worse value.
#[derive(Debug, Clone)]
pub struct CapabilityPublisher {
    published: Option<CapabilityProfile>,
    pending: HashMap<&'static str, Instant>,
    sustain: Duration,
}

impl CapabilityPublisher {
    /// Starts from the profile in the record, if any.
    #[must_use]
    pub fn new(published: Option<CapabilityProfile>, sustain: Duration) -> Self {
        Self {
            published,
            pending: HashMap::new(),
            sustain,
        }
    }

    /// Applies a changed `cluster.capability_upgrade_sustain_seconds`.
    pub fn set_sustain(&mut self, sustain: Duration) {
        self.sustain = sustain;
    }

    /// The last published profile.
    #[must_use]
    pub fn published(&self) -> Option<&CapabilityProfile> {
        self.published.as_ref()
    }

    /// Upgrades still being held.
    #[must_use]
    pub fn pending(&self) -> Vec<&'static str> {
        let mut v: Vec<_> = self.pending.keys().copied().collect();
        v.sort_unstable();
        v
    }

    /// Feeds a freshly derived profile observed at `now`; returns what to
    /// publish, if anything changed.
    pub fn observe(&mut self, candidate: CapabilityProfile, now: Instant) -> Option<Publication> {
        let Some(old) = &self.published else {
            let mut first = candidate;
            first.version = first.version.max(1);
            self.published = Some(first.clone());
            return Some(Publication {
                profile: first,
                changes: Vec::new(),
            });
        };
        let mut next = candidate;
        for (name, get, set) in FLAGS {
            if !get(old) && get(&next) {
                let since = *self.pending.entry(name).or_insert(now);
                if now.saturating_duration_since(since) < self.sustain {
                    set(&mut next, false);
                } else {
                    self.pending.remove(name);
                }
            } else {
                self.pending.remove(name);
            }
        }
        // Values that follow a held flag are held with it.
        if next.derived.expected_loss == LossClass::Zero && !next.derived.zero_loss_failover {
            next.derived.expected_loss = match old.derived.expected_loss {
                LossClass::Zero => LossClass::Async,
                other => other,
            };
        }
        if !next.derived.feed_source {
            next.derived.feed_level = FeedLevel::Unsupported;
        }
        let changes = diff(old, &next);
        if changes.is_empty() {
            return None;
        }
        next.version = old.version + 1;
        self.published = Some(next.clone());
        Some(Publication {
            profile: next,
            changes,
        })
    }
}

/// Why the light probe asks for a full probe: a `wsrep_cluster_conf_id`
/// change, a new `@@version`, or a new `@@server_id` behind a node.
#[must_use]
pub fn full_probe_triggers(before: &[NodeProbe], after: &[NodeProbe]) -> Vec<String> {
    let mut out = Vec::new();
    for a in after {
        let Some(new) = a.facts() else { continue };
        let Some(old) = before
            .iter()
            .find(|b| b.node == a.node)
            .and_then(NodeProbe::facts)
        else {
            continue;
        };
        if old.server.version_text != new.server.version_text {
            out.push(format!(
                "node {}: version {} -> {}",
                a.node, old.server.version_text, new.server.version_text
            ));
        }
        if old.server.server_id != new.server.server_id {
            out.push(format!(
                "node {}: server_id {} -> {}",
                a.node, old.server.server_id, new.server.server_id
            ));
        }
        let conf = |f: &NodeFacts| f.galera.as_ref().map(|g| g.conf_id);
        if conf(old) != conf(new) {
            out.push(format!("node {}: wsrep_cluster_conf_id changed", a.node));
        }
    }
    out
}

#[cfg(test)]
mod tests;
