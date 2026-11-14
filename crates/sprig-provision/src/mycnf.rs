//! The baseline server settings of clusters SprigDB manages: one renderer for VMs (the
//! agent writes the file) and Kubernetes (`MariaDB.spec.myCnf`).
//!
//! ```text
//! server_id        = cluster_number * 100 + index      (VMs)
//! gtid_domain_id   = cluster_number
//! buffer pool      = 70% of memory less tuning.memory_budget_pct
//! redo             = max(1G, 25% of the buffer pool)
//! read_only        = ON: the node boots fenced; its agent unfences
//! semi-sync        = master side ON on the primary only, slave side on replicas
//! max_allowed_packet = the gateway's limit (64 MiB) + 1 MiB for its rewrites
//! Galera           = wsrep_* with mariabackup SST and wsrep_gtid_mode
//! ```
//!
//! Everything SprigDB tunes later is set live with `SET GLOBAL`; this is
//! only what a node starts with. On Kubernetes mariadb-operator owns
//! `server_id`, replication, semi-sync, `read_only` at boot, Galera's
//! `wsrep_*` and TLS, so [`Platform::Kubernetes`] leaves them out, except
//! `wsrep_gtid_mode`, which mariadb-operator leaves off.

use std::fmt::Write as _;

/// The binlog retention a new cluster starts with: `feed.retention_hours`
/// (168) × 3600.
pub const BINLOG_EXPIRE_SECONDS: u64 = 604_800;
/// `provision.open_files_limit`'s default.
pub const OPEN_FILES_LIMIT: u32 = 524_288;
/// The smallest redo log, bytes.
pub const MIN_REDO_BYTES: u64 = 1 << 30;
/// `tuning.memory_budget_pct`'s default.
pub const DEFAULT_TUNING_BUDGET_PCT: u8 = 10;
/// `gateway.max_allowed_packet_bytes`' default.
pub const GATEWAY_MAX_ALLOWED_PACKET: u64 = 64 * MIB;
/// What the gateway may add to a statement on its way to MariaDB
/// (rewritten names, the session's sync prefix); the gateway tells clients
/// a limit this much below the backend's (`PACKET_HEADROOM` there).
pub const PACKET_HEADROOM: u64 = MIB;
/// `max_allowed_packet` on clusters SprigDB provisions: the gateway's
/// default limit plus the headroom, so the gateway's own limit is the one
/// clients get.
pub const MAX_ALLOWED_PACKET: u64 = GATEWAY_MAX_ALLOWED_PACKET + PACKET_HEADROOM;

const MIB: u64 = 1 << 20;

/// What a node is in its cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    /// The primary of a primary + replicas cluster.
    Primary,
    /// A replica.
    Replica,
    /// A Galera node.
    Galera,
    /// The one node of a single-node cluster.
    Single,
}

/// Who runs the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    /// The agent on a VM.
    Vm,
    /// mariadb-operator (see the module docs for what it owns).
    Kubernetes,
}

/// TLS files on the host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsFiles {
    /// The deployment's CA bundle (every DC's intermediates).
    pub ca: String,
    /// The server certificate.
    pub cert: String,
    /// Its key.
    pub key: String,
}

/// Galera's per-node settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GaleraSettings {
    /// `wsrep_cluster_name`.
    pub cluster_name: String,
    /// Every node's `host` (port 4567 implied).
    pub members: Vec<String>,
    /// This node's address.
    pub node_address: String,
    /// `provision.galera_gcache_size`.
    pub gcache_size: String,
    /// `wsrep_provider`.
    pub provider: String,
}

/// One node's inputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeSettings {
    /// VM or Kubernetes.
    pub platform: Platform,
    /// The node's role.
    pub role: NodeRole,
    /// The cluster number (`gtid_domain_id`, and `server_id`'s hundreds).
    pub cluster_number: u32,
    /// The node's index in the cluster, from 1.
    pub index: u32,
    /// The host's memory, bytes.
    pub memory_bytes: u64,
    /// `tuning.memory_budget_pct`.
    pub tuning_budget_pct: u8,
    /// `binlog_expire_logs_seconds`.
    pub binlog_expire_seconds: u64,
    /// `open_files_limit` (start-up only).
    pub open_files_limit: u32,
    /// `report_host`: the address replicas reach a promoted node at.
    pub report_host: Option<String>,
    /// The data directory.
    pub datadir: String,
    /// TLS files (VMs).
    pub tls: Option<TlsFiles>,
    /// Galera, for [`NodeRole::Galera`].
    pub galera: Option<GaleraSettings>,
    /// `max_allowed_packet`, bytes ([`MAX_ALLOWED_PACKET`]; more when a
    /// deployment raised `gateway.max_allowed_packet_bytes`).
    pub max_allowed_packet: u64,
}

impl NodeSettings {
    /// A node with the defaults: 1-based `index` of cluster
    /// `cluster_number`, `memory_bytes` of memory.
    #[must_use]
    pub fn new(
        platform: Platform,
        role: NodeRole,
        cluster_number: u32,
        index: u32,
        memory_bytes: u64,
    ) -> Self {
        Self {
            platform,
            role,
            cluster_number,
            index,
            memory_bytes,
            tuning_budget_pct: DEFAULT_TUNING_BUDGET_PCT,
            binlog_expire_seconds: BINLOG_EXPIRE_SECONDS,
            open_files_limit: OPEN_FILES_LIMIT,
            report_host: None,
            datadir: "/var/lib/mysql".into(),
            tls: None,
            galera: None,
            max_allowed_packet: MAX_ALLOWED_PACKET,
        }
    }

    /// `server_id = cluster_number * 100 + index`.
    #[must_use]
    pub fn server_id(&self) -> u32 {
        self.cluster_number * 100 + self.index
    }

    /// `innodb_buffer_pool_size`: 70% of memory less the tuning budget,
    /// whole MiB, at least 128 MiB.
    #[must_use]
    pub fn buffer_pool_bytes(&self) -> u64 {
        let pct = 70u64.saturating_sub(u64::from(self.tuning_budget_pct));
        let bytes = self.memory_bytes / 100 * pct;
        (bytes / MIB * MIB).max(128 * MIB)
    }

    /// `innodb_log_file_size`: the larger of 1 GiB and 25% of the buffer
    /// pool, whole MiB.
    #[must_use]
    pub fn redo_bytes(&self) -> u64 {
        (self.buffer_pool_bytes() / 4 / MIB * MIB).max(MIN_REDO_BYTES)
    }
}

fn size(bytes: u64) -> String {
    if bytes.is_multiple_of(1 << 30) {
        format!("{}G", bytes >> 30)
    } else {
        format!("{}M", bytes / MIB)
    }
}

/// The node's `[mariadbd]` file.
#[must_use]
#[allow(clippy::too_many_lines)] // one line per setting
pub fn render(n: &NodeSettings) -> String {
    let vm = n.platform == Platform::Vm;
    let mut s = String::new();
    let mut line = |k: &str, v: &dyn std::fmt::Display, note: &str| {
        let _ = if note.is_empty() {
            writeln!(s, "{k:<31} = {v}")
        } else {
            writeln!(s, "{k:<31} = {v:<10} # {note}")
        };
    };
    // A header the agent and the tests recognise.
    let mut out = String::from(
        "# Written by SprigDB: the baseline a node starts with.\n\
         # Live values are set with SET GLOBAL by the controller and survive\n\
         # restarts without editing this file.\n[mariadbd]\n",
    );
    if vm {
        line("server_id", &n.server_id(), "cluster_number * 100 + index");
        line("datadir", &n.datadir, "");
    }
    // On Kubernetes the number is known only after registration (0): the
    // operator then sets gtid_domain_id live.
    if vm || n.cluster_number != 0 {
        line("gtid_domain_id", &n.cluster_number, "the cluster number");
    }
    line("log_bin", &"mariadb-bin", "");
    line("binlog_format", &"ROW", "");
    line("binlog_row_image", &"FULL", "");
    line(
        "binlog_row_metadata",
        &"FULL",
        "column names for the change feed",
    );
    line(
        "binlog_expire_logs_seconds",
        &n.binlog_expire_seconds,
        "feed.retention_hours x 3600",
    );
    line("sync_binlog", &1, "");
    line("innodb_flush_log_at_trx_commit", &1, "");
    line(
        "innodb_buffer_pool_size",
        &size(n.buffer_pool_bytes()),
        "70% of memory less the tuning budget",
    );
    line(
        "innodb_log_file_size",
        &size(n.redo_bytes()),
        "max(1G, 25% of the buffer pool)",
    );
    line("innodb_io_capacity", &2000, "SSD");
    line("innodb_io_capacity_max", &4000, "");
    line("gtid_strict_mode", &"ON", "");
    line("log_slave_updates", &"ON", "");
    line(
        "max_allowed_packet",
        &n.max_allowed_packet,
        "the gateway's limit + 1 MiB",
    );
    if vm {
        line("read_only", &"ON", "boot fenced; the agent unfences");
        match n.role {
            NodeRole::Primary => {
                line("rpl_semi_sync_master_enabled", &"ON", "primary only");
                line("rpl_semi_sync_slave_enabled", &"ON", "ready for a demotion");
                line("rpl_semi_sync_master_wait_point", &"AFTER_SYNC", "");
                line("rpl_semi_sync_master_timeout", &10000, "");
            }
            NodeRole::Replica => {
                line(
                    "rpl_semi_sync_master_enabled",
                    &"OFF",
                    "switched on at promotion",
                );
                line("rpl_semi_sync_slave_enabled", &"ON", "");
                line("rpl_semi_sync_master_wait_point", &"AFTER_SYNC", "");
                line("rpl_semi_sync_master_timeout", &10000, "");
            }
            NodeRole::Galera | NodeRole::Single => {}
        }
        if let Some(h) = &n.report_host {
            line("report_host", h, "replicas reach a promoted node here");
            line("report_port", &3306, "");
        }
        line("require_secure_transport", &"ON", "");
        if let Some(t) = &n.tls {
            line("ssl_ca", &t.ca, "");
            line("ssl_cert", &t.cert, "");
            line("ssl_key", &t.key, "");
        }
    }
    line("skip_name_resolve", &"ON", "");
    line("open_files_limit", &n.open_files_limit, "start-up only");
    line("table_open_cache_instances", &8, "start-up only");
    if vm
        && n.role == NodeRole::Galera
        && let Some(g) = &n.galera
    {
        line("wsrep_on", &"ON", "");
        line("wsrep_provider", &g.provider, "");
        line(
            "wsrep_cluster_address",
            &format!("gcomm://{}", g.members.join(",")),
            "",
        );
        line("wsrep_cluster_name", &g.cluster_name, "");
        line("wsrep_node_address", &g.node_address, "");
        line("wsrep_sst_method", &"mariabackup", "");
        line("wsrep_gtid_mode", &"ON", "");
        line("wsrep_gtid_domain_id", &n.cluster_number, "");
        line(
            "wsrep_provider_options",
            &format!("\"gcache.size={}\"", g.gcache_size),
            "",
        );
        line("binlog_format", &"ROW", "Galera needs it");
        line("innodb_autoinc_lock_mode", &2, "Galera needs it");
    }
    if !vm && n.role == NodeRole::Galera {
        // mariadb-operator turns it on only for its multi-cluster
        // topology, and registration requires it (found on kind).
        line("wsrep_gtid_mode", &"ON", "mariadb-operator leaves it OFF");
    }
    out.push_str(&s);
    out.push_str(
        "# table_open_cache, table_definition_cache, innodb_open_files and\n\
         # stored_program_cache start at MariaDB's defaults; the tuning loop sets them live.\n",
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get<'a>(cnf: &'a str, key: &str) -> Option<&'a str> {
        cnf.lines().find_map(|l| {
            let (k, v) = l.split_once('=')?;
            (k.trim() == key).then(|| v.split('#').next().unwrap_or("").trim())
        })
    }

    #[test]
    fn primary_on_a_vm() {
        let mut n = NodeSettings::new(Platform::Vm, NodeRole::Primary, 7, 1, 16 << 30);
        n.report_host = Some("10.8.1.11".into());
        let c = render(&n);
        assert_eq!(get(&c, "server_id"), Some("701"));
        assert_eq!(get(&c, "gtid_domain_id"), Some("7"));
        assert_eq!(get(&c, "read_only"), Some("ON"));
        assert_eq!(get(&c, "rpl_semi_sync_master_enabled"), Some("ON"));
        assert_eq!(
            get(&c, "rpl_semi_sync_master_wait_point"),
            Some("AFTER_SYNC")
        );
        // The gateway's 64 MiB plus 1 MiB of headroom.
        assert_eq!(get(&c, "max_allowed_packet"), Some("68157440"));
        // 16 GiB x 60% = 9830 MiB; redo 25% of that, over 1G.
        assert_eq!(get(&c, "innodb_buffer_pool_size"), Some("9830M"));
        assert_eq!(get(&c, "innodb_log_file_size"), Some("2457M"));
        assert_eq!(get(&c, "binlog_expire_logs_seconds"), Some("604800"));
        assert_eq!(get(&c, "report_host"), Some("10.8.1.11"));
        assert!(c.starts_with("# Written by SprigDB"));
    }

    #[test]
    fn replicas_keep_the_master_side_off_and_small_hosts_get_1g_redo() {
        let n = NodeSettings::new(Platform::Vm, NodeRole::Replica, 7, 2, 2 << 30);
        let c = render(&n);
        assert_eq!(get(&c, "server_id"), Some("702"));
        assert_eq!(get(&c, "rpl_semi_sync_master_enabled"), Some("OFF"));
        assert_eq!(get(&c, "rpl_semi_sync_slave_enabled"), Some("ON"));
        assert_eq!(get(&c, "innodb_log_file_size"), Some("1G"));
        assert_eq!(get(&c, "innodb_buffer_pool_size"), Some("1228M"));
    }

    #[test]
    fn galera_adds_wsrep() {
        let mut n = NodeSettings::new(Platform::Vm, NodeRole::Galera, 3, 2, 8 << 30);
        n.galera = Some(GaleraSettings {
            cluster_name: "fra1-c3".into(),
            members: vec!["10.8.1.1".into(), "10.8.1.2".into(), "10.8.1.3".into()],
            node_address: "10.8.1.2".into(),
            gcache_size: "2G".into(),
            provider: "/usr/lib/galera/libgalera_smm.so".into(),
        });
        let c = render(&n);
        assert_eq!(
            get(&c, "wsrep_cluster_address"),
            Some("gcomm://10.8.1.1,10.8.1.2,10.8.1.3")
        );
        assert_eq!(get(&c, "wsrep_sst_method"), Some("mariabackup"));
        assert_eq!(
            get(&c, "wsrep_provider_options"),
            Some("\"gcache.size=2G\"")
        );
        assert_eq!(get(&c, "rpl_semi_sync_master_enabled"), None);
    }

    #[test]
    fn kubernetes_leaves_what_the_operator_owns() {
        let n = NodeSettings::new(Platform::Kubernetes, NodeRole::Primary, 9, 1, 4 << 30);
        let c = render(&n);
        for k in [
            "server_id",
            "read_only",
            "rpl_semi_sync_master_enabled",
            "ssl_ca",
            "require_secure_transport",
        ] {
            assert_eq!(get(&c, k), None, "{k}");
        }
        assert_eq!(get(&c, "gtid_domain_id"), Some("9"));
        let unknown = NodeSettings::new(Platform::Kubernetes, NodeRole::Galera, 0, 1, 4 << 30);
        assert_eq!(get(&render(&unknown), "gtid_domain_id"), None);
        assert_eq!(get(&render(&unknown), "wsrep_gtid_mode"), Some("ON"));
        assert_eq!(get(&render(&unknown), "wsrep_on"), None);
        assert_eq!(get(&c, "log_bin"), Some("mariadb-bin"));
        assert_eq!(get(&c, "max_allowed_packet"), Some("68157440"));
    }
}
