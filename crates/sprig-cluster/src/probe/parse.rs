//! Parsers from a [`RawCapture`] (what MariaDB returned) to [`NodeFacts`].
//!
//! Pure functions: they run the same on live rows and on the 11.8 and 12.3
//! fixtures. Anything unexpected parses to "unknown" rather than failing,
//! because MariaDB output is untrusted input (chapter 03).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::grants::{Grant, PrivilegeInfo};
use crate::sql::TextRow;

/// What one probe read from one node, before parsing. Variable and status
/// names are lowercased.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawCapture {
    /// `SHOW GLOBAL VARIABLES WHERE Variable_name IN (...)`.
    pub variables: BTreeMap<String, String>,
    /// `SHOW GLOBAL STATUS WHERE Variable_name IN (...)`.
    pub status: BTreeMap<String, String>,
    /// `SHOW ALL SLAVES STATUS`, one map per replication channel.
    pub channels: Vec<BTreeMap<String, String>>,
    /// `SHOW BINARY LOGS` (full probe): file name and size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binary_logs: Option<Vec<(String, u64)>>,
    /// `BINLOG_GTID_POS(oldest file, 4)` (full probe).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub horizon: Option<String>,
    /// `SHOW GRANTS` (full probe), with password hashes removed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grants: Option<Vec<String>>,
    /// Server clock minus local clock, in milliseconds (full probe).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clock_skew_ms: Option<i64>,
    /// Heartbeat rows read from `__sprig_sys.__sprig_heartbeat`: writer
    /// node, sequence and age in microseconds by this node's clock. `None`
    /// when the table doesn't exist yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heartbeats: Option<Vec<(String, u64, i64)>>,
}

impl RawCapture {
    /// Folds `SHOW ... VARIABLES|STATUS` rows into a lowercased map.
    #[must_use]
    pub fn name_value_map(rows: &[TextRow]) -> BTreeMap<String, String> {
        rows.iter()
            .filter_map(|r| {
                let name = r.first()?.1.as_ref()?;
                let value = r.get(1).and_then(|(_, v)| v.clone()).unwrap_or_default();
                Some((name.to_ascii_lowercase(), value))
            })
            .collect()
    }

    /// Turns `SHOW ALL SLAVES STATUS` rows into maps by column name.
    #[must_use]
    pub fn channel_maps(rows: Vec<TextRow>) -> Vec<BTreeMap<String, String>> {
        rows.into_iter()
            .map(|r| {
                r.into_iter()
                    .map(|(k, v)| (k, v.unwrap_or_default()))
                    .collect()
            })
            .collect()
    }

    fn var(&self, name: &str) -> Option<&str> {
        self.variables.get(name).map(String::as_str)
    }

    fn stat(&self, name: &str) -> Option<&str> {
        self.status.get(name).map(String::as_str)
    }
}

/// A MariaDB version, from `@@version` such as `11.8.9-MariaDB-ubu2404-log`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ServerVersion {
    /// Major.
    pub major: u16,
    /// Minor.
    pub minor: u16,
    /// Patch.
    pub patch: u16,
}

impl ServerVersion {
    /// The oldest version SprigDB accepts.
    pub const MIN_SUPPORTED: ServerVersion = ServerVersion::new(11, 8, 0);

    /// A version.
    #[must_use]
    pub const fn new(major: u16, minor: u16, patch: u16) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// Parses the leading `major.minor.patch` of `@@version`.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        let head = s.split(['-', '+', ' ']).next()?;
        let mut it = head.split('.').map(|p| p.parse::<u16>().ok());
        let (major, minor) = (it.next()??, it.next()??);
        let patch = it.next().flatten().unwrap_or(0);
        Some(Self::new(major, minor, patch))
    }
}

impl std::fmt::Display for ServerVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// `@@read_only`: a boolean on 11.8, an enum from 12.0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadOnly {
    /// Writable.
    Off,
    /// Read-only.
    On,
    /// 12.0+: read-only without the global read lock.
    NoLock,
    /// 12.0+: read-only without the lock, also for `READ_ONLY ADMIN`.
    NoLockNoAdmin,
}

impl ReadOnly {
    /// Parses either form.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_uppercase().as_str() {
            "OFF" | "0" => Some(Self::Off),
            "ON" | "1" => Some(Self::On),
            "NO_LOCK" => Some(Self::NoLock),
            "NO_LOCK_NO_ADMIN" => Some(Self::NoLockNoAdmin),
            _ => None,
        }
    }

    /// Whether ordinary writes are refused.
    #[must_use]
    pub fn is_read_only(self) -> bool {
        self != Self::Off
    }
}

/// `rpl_semi_sync_master_wait_point`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WaitPoint {
    /// Wait for the acknowledgement before committing to the engine: no
    /// acknowledged commit is lost on failover.
    AfterSync,
    /// Wait after committing: others may see a commit a failover loses.
    AfterCommit,
}

/// `binlog_format`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BinlogFormat {
    /// Row events.
    Row,
    /// Statements.
    Statement,
    /// Statements, rows when unsafe.
    Mixed,
}

/// `binlog_row_image`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RowImage {
    /// Every column, before and after.
    Full,
    /// Only what identifies and changes the row.
    Minimal,
    /// Full except unchanged blobs.
    Noblob,
    /// `FULL_NODUP` (11.4+): full before image, changed columns after.
    FullNodup,
}

/// `binlog_row_metadata`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RowMetadata {
    /// No column metadata.
    NoLog,
    /// Types and signedness.
    Minimal,
    /// Column names too: no schema history needed.
    Full,
}

/// Galera's `wsrep_local_state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GaleraLocalState {
    /// 1: receiving state.
    Joining,
    /// 2: donor, or desynced for an RSU build.
    DonorDesynced,
    /// 3: has state, catching up.
    Joined,
    /// 4: in sync.
    Synced,
    /// Anything else.
    Other,
}

/// Galera status of one node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GaleraStatus {
    /// `wsrep_ready`.
    pub ready: bool,
    /// `wsrep_connected`.
    pub connected: bool,
    /// `wsrep_cluster_status`: `Primary`, `Non-Primary` or `Disconnected`.
    pub cluster_status: String,
    /// `wsrep_local_state`.
    pub local_state: GaleraLocalState,
    /// `wsrep_cluster_size`.
    pub cluster_size: u32,
    /// `wsrep_cluster_conf_id`: changes with membership.
    pub conf_id: u64,
    /// `wsrep_flow_control_paused`: fraction of time paused.
    pub flow_control_paused: f64,
    /// `wsrep_gtid_mode`.
    pub gtid_mode: bool,
    /// `wsrep_gtid_domain_id`.
    pub gtid_domain_id: Option<u32>,
    /// `wsrep_OSU_method`: `TOI` or `RSU`.
    pub osu_method: String,
    /// `wsrep_sync_wait`.
    pub sync_wait: u32,
}

impl GaleraStatus {
    /// Ready, `Primary` and `Synced`.
    #[must_use]
    pub fn usable(&self) -> bool {
        self.ready
            && self.cluster_status == "Primary"
            && self.local_state == GaleraLocalState::Synced
    }
}

/// Semi-synchronous replication on one node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)] // mirrors the server variables
pub struct SemiSyncFacts {
    /// `rpl_semi_sync_master_enabled`.
    pub master_enabled: bool,
    /// `Rpl_semi_sync_master_status`: `OFF` while enabled means the primary
    /// fell back to asynchronous.
    pub master_active: bool,
    /// `Rpl_semi_sync_master_clients`.
    pub master_clients: u32,
    /// `rpl_semi_sync_master_wait_point`.
    pub wait_point: Option<WaitPoint>,
    /// `rpl_semi_sync_master_timeout`.
    pub timeout_ms: u64,
    /// `rpl_semi_sync_slave_enabled`.
    pub slave_enabled: bool,
    /// `Rpl_semi_sync_slave_status`.
    pub slave_active: bool,
}

/// Binlog settings and positions of one node.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BinlogFacts {
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
    /// `log_slave_updates`.
    pub log_slave_updates: bool,
    /// `binlog_expire_logs_seconds`; 0 means never.
    pub expire_logs_seconds: u64,
    /// `binlog_checksum`.
    pub checksum: String,
    /// `binlog_storage_engine` (12.3+): `innodb` when the binlog lives in
    /// `InnoDB`. Empty or absent means files.
    pub storage_engine: Option<String>,
    /// `gtid_domain_id`.
    pub gtid_domain_id: u32,
    /// `@@gtid_binlog_pos`.
    pub gtid_binlog_pos: String,
    /// `@@gtid_current_pos`.
    pub gtid_current_pos: String,
    /// Oldest binlog file (full probe).
    pub oldest_file: Option<String>,
    /// Number of binlog files (full probe).
    pub files: Option<u32>,
    /// `BINLOG_GTID_POS(oldest file, 4)` (full probe).
    pub horizon: Option<String>,
}

impl BinlogFacts {
    /// Whether the binlog is stored in files (not in `InnoDB`, 12.3+).
    #[must_use]
    pub fn in_files(&self) -> bool {
        !self
            .storage_engine
            .as_deref()
            .is_some_and(|e| e.eq_ignore_ascii_case("innodb"))
    }
}

/// State of a replication thread: `Yes`, `No` or `Connecting`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadState {
    /// Running.
    Yes,
    /// Stopped.
    No,
    /// Trying to connect.
    Connecting,
}

/// One replication channel of `SHOW ALL SLAVES STATUS`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaChannel {
    /// `Connection_name`; empty for the default channel.
    pub name: String,
    /// `Master_Host`.
    pub master_host: String,
    /// `Master_Port`.
    pub master_port: u16,
    /// `Master_Server_Id`.
    pub master_server_id: u32,
    /// `Slave_IO_Running`.
    pub io: ThreadState,
    /// `Slave_SQL_Running`.
    pub sql: ThreadState,
    /// `Seconds_Behind_Master`; `None` when `NULL`.
    pub seconds_behind: Option<u64>,
    /// `Last_IO_Errno`.
    pub last_io_errno: u32,
    /// `Last_SQL_Errno`.
    pub last_sql_errno: u32,
    /// `Using_Gtid`: `No`, `Slave_Pos` or `Current_Pos`.
    pub using_gtid: String,
    /// `Gtid_IO_Pos`.
    pub gtid_io_pos: String,
}

impl ReplicaChannel {
    /// Both threads running and no recorded error.
    #[must_use]
    pub fn running(&self) -> bool {
        self.io == ThreadState::Yes
            && self.sql == ThreadState::Yes
            && self.last_io_errno == 0
            && self.last_sql_errno == 0
    }
}

/// Routine limits (feed `compiled_logic`, chapter 41).
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
pub struct RoutineLimits {
    /// `stored_program_cache`.
    pub stored_program_cache: u64,
    /// `max_sp_recursion_depth`.
    pub max_sp_recursion_depth: u64,
    /// `max_statement_time` in milliseconds; 0 means none.
    pub max_statement_time_ms: u64,
}

/// Table cache variables and counters (feed server tuning).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TableCacheInfo {
    /// `table_open_cache`.
    pub table_open_cache: u64,
    /// `table_definition_cache`.
    pub table_definition_cache: u64,
    /// `innodb_open_files`.
    pub innodb_open_files: u64,
    /// `open_files_limit`.
    pub open_files_limit: u64,
    /// `max_connections`.
    pub max_connections: u64,
    /// `Opened_tables`.
    pub opened_tables: u64,
    /// `Open_tables`.
    pub open_tables: u64,
    /// `Table_open_cache_hits`.
    pub table_open_cache_hits: u64,
    /// `Table_open_cache_misses`.
    pub table_open_cache_misses: u64,
    /// `Memory_used`, bytes.
    pub memory_used: u64,
    /// `Threads_running`.
    pub threads_running: u64,
}

/// Server identity and basic settings of one node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerFacts {
    /// Parsed `@@version`.
    pub version: Option<ServerVersion>,
    /// `@@version` as returned.
    pub version_text: String,
    /// `@@server_id`.
    pub server_id: u32,
    /// `lower_case_table_names`.
    pub lower_case_table_names: u8,
    /// `default_storage_engine`.
    pub default_storage_engine: String,
    /// `read_only`.
    pub read_only: Option<ReadOnly>,
    /// `performance_schema`.
    pub performance_schema: bool,
    /// `binlog_alter_two_phase`, if the server has it.
    pub binlog_alter_two_phase: Option<bool>,
    /// `legacy_xa_rollback_at_disconnect`, if the server has it.
    pub legacy_xa_rollback_at_disconnect: Option<bool>,
    /// Server clock minus local clock (full probe).
    pub clock_skew_ms: Option<i64>,
    /// `wsrep_on`.
    pub wsrep_on: bool,
}

/// A heartbeat row as one node sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatReading {
    /// The writer node that wrote it.
    pub writer_node: String,
    /// Its sequence number.
    pub seq: u64,
    /// Its age by this node's clock, in milliseconds (0 if negative).
    pub age_ms: u64,
}

/// Everything parsed from one node's [`RawCapture`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeFacts {
    /// Identity and basic settings.
    pub server: ServerFacts,
    /// Galera status, when `wsrep_on`.
    pub galera: Option<GaleraStatus>,
    /// Semi-sync.
    pub semi_sync: SemiSyncFacts,
    /// Binlog.
    pub binlog: BinlogFacts,
    /// Replication channels; empty on a primary or Galera node.
    pub channels: Vec<ReplicaChannel>,
    /// Routine limits.
    pub routines: RoutineLimits,
    /// Table cache.
    pub tables: TableCacheInfo,
    /// Grants of the probing account (full probe).
    pub privileges: Option<PrivilegeInfo>,
    /// Heartbeats visible on this node.
    pub heartbeats: Option<Vec<HeartbeatReading>>,
}

impl NodeFacts {
    /// Parses a capture.
    #[must_use]
    pub fn parse(raw: &RawCapture) -> Self {
        let wsrep_on = raw.var("wsrep_on").is_some_and(flag);
        Self {
            server: ServerFacts {
                version: raw.var("version").and_then(ServerVersion::parse),
                version_text: raw.var("version").unwrap_or_default().to_owned(),
                server_id: num(raw.var("server_id")),
                lower_case_table_names: num(raw.var("lower_case_table_names")),
                default_storage_engine: raw
                    .var("default_storage_engine")
                    .unwrap_or_default()
                    .to_owned(),
                read_only: raw.var("read_only").and_then(ReadOnly::parse),
                performance_schema: raw.var("performance_schema").is_some_and(flag),
                binlog_alter_two_phase: raw.var("binlog_alter_two_phase").map(flag),
                legacy_xa_rollback_at_disconnect: raw
                    .var("legacy_xa_rollback_at_disconnect")
                    .map(flag),
                clock_skew_ms: raw.clock_skew_ms,
                wsrep_on,
            },
            galera: wsrep_on.then(|| galera(raw)),
            semi_sync: SemiSyncFacts {
                master_enabled: raw.var("rpl_semi_sync_master_enabled").is_some_and(flag),
                master_active: raw.stat("rpl_semi_sync_master_status").is_some_and(flag),
                master_clients: num(raw.stat("rpl_semi_sync_master_clients")),
                wait_point: raw.var("rpl_semi_sync_master_wait_point").and_then(|w| {
                    match w.to_ascii_uppercase().as_str() {
                        "AFTER_SYNC" => Some(WaitPoint::AfterSync),
                        "AFTER_COMMIT" => Some(WaitPoint::AfterCommit),
                        _ => None,
                    }
                }),
                timeout_ms: num(raw.var("rpl_semi_sync_master_timeout")),
                slave_enabled: raw.var("rpl_semi_sync_slave_enabled").is_some_and(flag),
                slave_active: raw.stat("rpl_semi_sync_slave_status").is_some_and(flag),
            },
            binlog: binlog(raw),
            channels: raw.channels.iter().map(channel).collect(),
            routines: RoutineLimits {
                stored_program_cache: num(raw.var("stored_program_cache")),
                max_sp_recursion_depth: num(raw.var("max_sp_recursion_depth")),
                max_statement_time_ms: raw
                    .var("max_statement_time")
                    .and_then(|s| s.parse::<f64>().ok())
                    .map_or(0, seconds_to_ms),
            },
            tables: TableCacheInfo {
                table_open_cache: num(raw.var("table_open_cache")),
                table_definition_cache: num(raw.var("table_definition_cache")),
                innodb_open_files: num(raw.var("innodb_open_files")),
                open_files_limit: num(raw.var("open_files_limit")),
                max_connections: num(raw.var("max_connections")),
                opened_tables: num(raw.stat("opened_tables")),
                open_tables: num(raw.stat("open_tables")),
                table_open_cache_hits: num(raw.stat("table_open_cache_hits")),
                table_open_cache_misses: num(raw.stat("table_open_cache_misses")),
                memory_used: num(raw.stat("memory_used")),
                threads_running: num(raw.stat("threads_running")),
            },
            privileges: raw.grants.as_ref().map(|lines| {
                PrivilegeInfo::from_grants(
                    &lines
                        .iter()
                        .filter_map(|l| Grant::parse(l))
                        .collect::<Vec<_>>(),
                )
            }),
            heartbeats: raw.heartbeats.as_ref().map(|rows| {
                rows.iter()
                    .map(|(w, seq, age_us)| HeartbeatReading {
                        writer_node: w.clone(),
                        seq: *seq,
                        age_ms: u64::try_from(*age_us / 1000).unwrap_or(0),
                    })
                    .collect()
            }),
        }
    }

    /// Whether this node replicates from another (has a channel).
    #[must_use]
    pub fn is_replica(&self) -> bool {
        !self.channels.is_empty()
    }

    /// Whether ordinary writes are accepted (`read_only` off).
    #[must_use]
    pub fn writable(&self) -> bool {
        self.server.read_only.is_some_and(|r| !r.is_read_only())
    }
}

fn galera(raw: &RawCapture) -> GaleraStatus {
    GaleraStatus {
        ready: raw.stat("wsrep_ready").is_some_and(flag),
        connected: raw.stat("wsrep_connected").is_some_and(flag),
        cluster_status: raw
            .stat("wsrep_cluster_status")
            .unwrap_or_default()
            .to_owned(),
        local_state: match raw.stat("wsrep_local_state") {
            Some("1") => GaleraLocalState::Joining,
            Some("2") => GaleraLocalState::DonorDesynced,
            Some("3") => GaleraLocalState::Joined,
            Some("4") => GaleraLocalState::Synced,
            _ => GaleraLocalState::Other,
        },
        cluster_size: num(raw.stat("wsrep_cluster_size")),
        conf_id: num(raw.stat("wsrep_cluster_conf_id")),
        flow_control_paused: raw
            .stat("wsrep_flow_control_paused")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0),
        gtid_mode: raw.var("wsrep_gtid_mode").is_some_and(flag),
        gtid_domain_id: raw.var("wsrep_gtid_domain_id").and_then(|s| s.parse().ok()),
        osu_method: raw.var("wsrep_osu_method").unwrap_or_default().to_owned(),
        sync_wait: num(raw.var("wsrep_sync_wait")),
    }
}

fn binlog(raw: &RawCapture) -> BinlogFacts {
    let up = |n: &str| raw.var(n).map(str::to_ascii_uppercase);
    BinlogFacts {
        log_bin: raw.var("log_bin").is_some_and(flag),
        format: up("binlog_format").and_then(|f| match f.as_str() {
            "ROW" => Some(BinlogFormat::Row),
            "STATEMENT" => Some(BinlogFormat::Statement),
            "MIXED" => Some(BinlogFormat::Mixed),
            _ => None,
        }),
        row_image: up("binlog_row_image").and_then(|f| match f.as_str() {
            "FULL" => Some(RowImage::Full),
            "MINIMAL" => Some(RowImage::Minimal),
            "NOBLOB" => Some(RowImage::Noblob),
            "FULL_NODUP" => Some(RowImage::FullNodup),
            _ => None,
        }),
        row_metadata: up("binlog_row_metadata").and_then(|f| match f.as_str() {
            "NO_LOG" => Some(RowMetadata::NoLog),
            "MINIMAL" => Some(RowMetadata::Minimal),
            "FULL" => Some(RowMetadata::Full),
            _ => None,
        }),
        gtid_strict_mode: raw.var("gtid_strict_mode").is_some_and(flag),
        log_slave_updates: raw.var("log_slave_updates").is_some_and(flag),
        expire_logs_seconds: num(raw.var("binlog_expire_logs_seconds")),
        checksum: raw.var("binlog_checksum").unwrap_or_default().to_owned(),
        storage_engine: raw
            .var("binlog_storage_engine")
            .filter(|s| !s.is_empty())
            .map(str::to_owned),
        gtid_domain_id: num(raw.var("gtid_domain_id")),
        gtid_binlog_pos: raw.var("gtid_binlog_pos").unwrap_or_default().to_owned(),
        gtid_current_pos: raw.var("gtid_current_pos").unwrap_or_default().to_owned(),
        oldest_file: raw
            .binary_logs
            .as_ref()
            .and_then(|l| l.first())
            .map(|(f, _)| f.clone()),
        files: raw
            .binary_logs
            .as_ref()
            .map(|l| u32::try_from(l.len()).unwrap_or(u32::MAX)),
        horizon: raw.horizon.clone(),
    }
}

fn channel(m: &BTreeMap<String, String>) -> ReplicaChannel {
    let get = |k: &str| m.get(k).map(String::as_str);
    let thread = |k: &str| match get(k) {
        Some(v) if v.eq_ignore_ascii_case("yes") => ThreadState::Yes,
        Some(v) if v.eq_ignore_ascii_case("connecting") => ThreadState::Connecting,
        _ => ThreadState::No,
    };
    ReplicaChannel {
        name: get("Connection_name").unwrap_or_default().to_owned(),
        master_host: get("Master_Host").unwrap_or_default().to_owned(),
        master_port: num(get("Master_Port")),
        master_server_id: num(get("Master_Server_Id")),
        io: thread("Slave_IO_Running"),
        sql: thread("Slave_SQL_Running"),
        seconds_behind: get("Seconds_Behind_Master").and_then(|s| s.parse().ok()),
        last_io_errno: num(get("Last_IO_Errno")),
        last_sql_errno: num(get("Last_SQL_Errno")),
        using_gtid: get("Using_Gtid").unwrap_or_default().to_owned(),
        gtid_io_pos: get("Gtid_IO_Pos").unwrap_or_default().to_owned(),
    }
}

/// `ON`, `1`, `YES` and `TRUE` are true; anything else is false.
fn flag(s: &str) -> bool {
    matches!(s.to_ascii_uppercase().as_str(), "ON" | "1" | "YES" | "TRUE")
}

/// A number, or the type's default when absent or malformed.
fn num<T: std::str::FromStr + Default>(s: Option<&str>) -> T {
    s.and_then(|s| s.trim().parse().ok()).unwrap_or_default()
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn seconds_to_ms(s: f64) -> u64 {
    if s.is_finite() && s > 0.0 {
        (s * 1000.0).round() as u64
    } else {
        0
    }
}

/// Strips `IDENTIFIED BY PASSWORD '...'` and `IDENTIFIED VIA ... USING '...'`
/// from a `SHOW GRANTS` line, so captures and logs never hold a hash.
#[must_use]
pub fn redact_grant(line: &str) -> String {
    match line.find(" IDENTIFIED ") {
        None => line.to_owned(),
        Some(i) => {
            let tail = &line[i..];
            // Keep what follows the credential: REQUIRE and WITH clauses.
            let keep = [" REQUIRE ", " WITH "]
                .iter()
                .filter_map(|k| tail.find(k))
                .min()
                .map_or("", |j| &tail[j..]);
            format!("{}{keep}", &line[..i])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions() {
        assert_eq!(
            ServerVersion::parse("11.8.9-MariaDB-ubu2404-log"),
            Some(ServerVersion::new(11, 8, 9))
        );
        assert_eq!(
            ServerVersion::parse("12.3.3-MariaDB"),
            Some(ServerVersion::new(12, 3, 3))
        );
        assert_eq!(
            ServerVersion::parse("10.6"),
            Some(ServerVersion::new(10, 6, 0))
        );
        assert_eq!(ServerVersion::parse("MariaDB"), None);
        assert!(ServerVersion::new(11, 8, 0) >= ServerVersion::MIN_SUPPORTED);
        assert!(ServerVersion::new(11, 4, 9) < ServerVersion::MIN_SUPPORTED);
    }

    #[test]
    fn read_only_parses_both_forms() {
        assert_eq!(ReadOnly::parse("OFF"), Some(ReadOnly::Off));
        assert_eq!(ReadOnly::parse("1"), Some(ReadOnly::On));
        assert_eq!(ReadOnly::parse("NO_LOCK"), Some(ReadOnly::NoLock));
        assert_eq!(
            ReadOnly::parse("no_lock_no_admin"),
            Some(ReadOnly::NoLockNoAdmin)
        );
        assert!(ReadOnly::NoLock.is_read_only());
        assert_eq!(ReadOnly::parse("maybe"), None);
    }

    #[test]
    fn redaction() {
        let l = "GRANT ALL PRIVILEGES ON *.* TO `root`@`localhost` IDENTIFIED BY PASSWORD '*B76' WITH GRANT OPTION";
        assert_eq!(
            redact_grant(l),
            "GRANT ALL PRIVILEGES ON *.* TO `root`@`localhost` WITH GRANT OPTION"
        );
        let l = "GRANT USAGE ON *.* TO `a`@`%` IDENTIFIED VIA ed25519 USING 'x' REQUIRE SSL";
        assert_eq!(redact_grant(l), "GRANT USAGE ON *.* TO `a`@`%` REQUIRE SSL");
        let l = "GRANT REPLICATION SLAVE ON *.* TO `r`@`%` IDENTIFIED BY PASSWORD '*F6'";
        assert_eq!(redact_grant(l), "GRANT REPLICATION SLAVE ON *.* TO `r`@`%`");
    }

    #[test]
    fn missing_values_parse_as_unknown() {
        let f = NodeFacts::parse(&RawCapture::default());
        assert_eq!(f.server.version, None);
        assert_eq!(f.server.read_only, None);
        assert!(!f.writable());
        assert!(f.galera.is_none());
        assert!(f.binlog.in_files());
        assert!(f.privileges.is_none());
    }

    #[test]
    fn malformed_values_do_not_panic() {
        let mut raw = RawCapture::default();
        for k in [
            "server_id",
            "max_statement_time",
            "wsrep_on",
            "binlog_format",
        ] {
            raw.variables.insert(k.into(), "\u{0}garbage-1e999".into());
        }
        raw.status.insert("wsrep_local_state".into(), "x".into());
        raw.channels.push(BTreeMap::new());
        let f = NodeFacts::parse(&raw);
        assert_eq!(f.server.server_id, 0);
        assert_eq!(f.routines.max_statement_time_ms, 0);
        assert_eq!(f.channels[0].io, ThreadState::No);
        assert!(!f.channels[0].running());
    }
}
