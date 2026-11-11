//! The one error type, [`SprigError`], and how each error shows up in every
//! protocol (chapter 03, "Errors"; codes in Appendix E).
//!
//! Front ends never invent their own codes: they call [`SprigError::protocol`]
//! and send what it returns.

use std::fmt;

/// The result type used across SprigDB.
pub type Result<T, E = SprigError> = std::result::Result<T, E>;

/// Every SprigDB error.
///
/// `message` is always client-safe: physical names are already translated.
/// Internal detail goes into `source`, which is logged, never sent.
#[derive(Debug, thiserror::Error)]
#[error("{kind}: {message}")]
pub struct SprigError {
    /// What went wrong, as a code every protocol can render.
    pub kind: ErrorKind,
    /// Client-safe text.
    pub message: String,
    /// Safe for the client to retry the transaction.
    pub retryable: bool,
    /// Internal detail, logged only.
    #[source]
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

/// The kind of an error: from MariaDB, from SprigDB, or a standard error
/// SprigDB raises itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// An error MariaDB returned, passed through after name translation.
    Backend {
        /// MariaDB error number.
        errno: u16,
        /// SQLSTATE as five ASCII bytes.
        sqlstate: [u8; 5],
    },
    /// A SprigDB error, numbered 39000-39999.
    Sprig(SprigCode),
    /// A standard error SprigDB raises itself, such as 1213 (deadlock)
    /// when a cutover rolls back a transaction.
    Standard {
        /// MySQL error number.
        errno: u16,
        /// SQLSTATE as five ASCII bytes.
        sqlstate: [u8; 5],
    },
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorKind::Backend { errno, sqlstate } | ErrorKind::Standard { errno, sqlstate } => {
                write!(f, "{errno} ({})", String::from_utf8_lossy(sqlstate))
            }
            ErrorKind::Sprig(code) => write!(f, "{} {}", code.number(), code.name()),
        }
    }
}

/// How an error is rendered in each protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolError {
    /// MySQL and MariaDB error number.
    pub mysql_errno: u16,
    /// SQLSTATE.
    pub sqlstate: [u8; 5],
    /// MongoDB error code.
    pub mongo_code: i32,
    /// MongoDB `codeName`.
    pub mongo_code_name: &'static str,
    /// MongoDB error labels that hold whatever the session state, such as
    /// `RetryableWriteError`. Labels that depend on the session (for example
    /// `TransientTransactionError` inside a transaction) are added by the
    /// MongoDB front end (chapter 30).
    pub mongo_labels: &'static [&'static str],
    /// gRPC status code.
    pub grpc_status: tonic::Code,
    /// HTTP status for REST.
    pub http_status: u16,
}

const SQLSTATE_GENERAL: [u8; 5] = *b"HY000";
const SQLSTATE_RETRY: [u8; 5] = *b"40001";
const SQLSTATE_CONNECTION: [u8; 5] = *b"08S01";
/// Syntax error or access rule violation (chapter 07 uses it for reserved names).
const SQLSTATE_SYNTAX_ACCESS: [u8; 5] = *b"42000";

macro_rules! sprig_codes {
    ($(
        $(#[$doc:meta])*
        $variant:ident = $num:literal, $name:literal, $mongo:literal, $sqlstate:expr, $retryable:literal;
    )*) => {
        /// SprigDB's own error codes (Appendix E). Only codes that code in
        /// this workspace raises are listed; each chapter adds its own as it
        /// is implemented.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum SprigCode {
            $( $(#[$doc])* $variant, )*
        }

        impl SprigCode {
            /// Every code, for tests and generated documentation.
            pub const ALL: &'static [SprigCode] = &[$(SprigCode::$variant),*];

            /// The number, 39000-39999.
            #[must_use]
            pub const fn number(self) -> u16 {
                match self { $(SprigCode::$variant => $num,)* }
            }

            /// The MySQL name, `ER_SPRIG_<NAME>`.
            #[must_use]
            pub const fn name(self) -> &'static str {
                match self { $(SprigCode::$variant => $name,)* }
            }

            /// The MongoDB `codeName`, `Sprig<CamelCase>`.
            #[must_use]
            pub const fn mongo_code_name(self) -> &'static str {
                match self { $(SprigCode::$variant => $mongo,)* }
            }

            /// The SQLSTATE sent with this code.
            #[must_use]
            pub const fn sqlstate(self) -> [u8; 5] {
                match self { $(SprigCode::$variant => $sqlstate,)* }
            }

            /// Whether the error is safe to retry by default.
            #[must_use]
            pub const fn retryable(self) -> bool {
                match self { $(SprigCode::$variant => $retryable,)* }
            }
        }
    };
}

sprig_codes! {
    /// The gateway is draining (chapter 04).
    GatewayDraining = 39000, "ER_SPRIG_GATEWAY_DRAINING", "SprigGatewayDraining", SQLSTATE_CONNECTION, true;
    /// The tenant has `limits.tenant_connections` sessions on this gateway
    /// already (chapter 04).
    TenantConnectionLimit = 39001, "ER_SPRIG_TENANT_CONNECTION_LIMIT", "SprigTenantConnectionLimit", *b"08004", true;
    /// A statement waited longer than `limits.queue_timeout_ms` for an
    /// admission permit (chapter 04).
    AdmissionTimeout = 39002, "ER_SPRIG_ADMISSION_TIMEOUT", "SprigAdmissionTimeout", SQLSTATE_GENERAL, true;
    /// A memory charge failed: the gateway, tenant or user budget is spent
    /// (chapter 04).
    MemoryLimit = 39003, "ER_SPRIG_MEMORY_LIMIT", "SprigMemoryLimit", SQLSTATE_GENERAL, true;
    /// The session is closing at a safe point because the listener it
    /// arrived on moved, was disabled or tightened (chapter 04).
    ListenerRetired = 39004, "ER_SPRIG_LISTENER_RETIRED", "SprigListenerRetired", SQLSTATE_CONNECTION, true;
    /// The backend asked for a `LOAD DATA LOCAL INFILE` file the statement
    /// didn't name; the load was aborted (chapter 05).
    LocalInfileRejected = 39010, "ER_SPRIG_LOCAL_INFILE_REJECTED", "SprigLocalInfileRejected", SQLSTATE_GENERAL, false;
    /// Buffered `COM_STMT_SEND_LONG_DATA` for a statement went over
    /// `gateway.long_data_max_bytes` (chapter 05).
    LongDataLimit = 39011, "ER_SPRIG_LONG_DATA_LIMIT", "SprigLongDataLimit", SQLSTATE_GENERAL, false;
    /// The backend sent a packet the protocol doesn't allow at that point
    /// (chapter 05).
    BackendProtocol = 39012, "ER_SPRIG_BACKEND_PROTOCOL", "SprigBackendProtocol", SQLSTATE_CONNECTION, false;
    /// No backend node could be reached before the deadline (chapter 06).
    BackendUnavailable = 39030, "ER_SPRIG_BACKEND_UNAVAILABLE", "SprigBackendUnavailable", SQLSTATE_CONNECTION, true;
    /// No pooled backend connection became free before the deadline
    /// (chapter 06).
    BackendPoolTimeout = 39031, "ER_SPRIG_BACKEND_POOL_TIMEOUT", "SprigBackendPoolTimeout", SQLSTATE_GENERAL, true;
    /// The backend was lost during a commit or an autocommit write: the
    /// outcome is unknown (chapter 06).
    OutcomeUnknown = 39032, "ER_SPRIG_OUTCOME_UNKNOWN", "SprigOutcomeUnknown", SQLSTATE_CONNECTION, false;
    /// The backend holding state that can't be replayed was lost (chapter
    /// 06).
    SessionStateLost = 39033, "ER_SPRIG_SESSION_STATE_LOST", "SprigSessionStateLost", SQLSTATE_CONNECTION, false;
    /// The backend was lost after part of a result was sent (chapter 06).
    BackendLost = 39034, "ER_SPRIG_BACKEND_LOST", "SprigBackendLost", SQLSTATE_CONNECTION, false;
    /// A statement creates or names a reserved name: `__sprig_` objects,
    /// `_sprig_` columns, `t<digits>_` or system databases (chapter 07).
    ReservedName = 39050, "ER_SPRIG_RESERVED_NAME", "SprigReservedName", SQLSTATE_SYNTAX_ACCESS, false;
    /// A statement holds a name position the rewriter can't map safely
    /// (chapter 07).
    NameRewriteUnsupported = 39051, "ER_SPRIG_NAME_REWRITE_UNSUPPORTED", "SprigNameRewriteUnsupported", SQLSTATE_GENERAL, false;
    /// A SprigDB `information_schema` view would inline more rows than
    /// `names.local_view_max_rows` (chapter 07): narrow it by schema or
    /// table.
    MetadataTooLarge = 39052, "ER_SPRIG_METADATA_TOO_LARGE", "SprigMetadataTooLarge", SQLSTATE_GENERAL, false;
    /// The tenant's catalog writer or the global tier can't be reached, so
    /// DDL, setting and account changes fail; cached reads continue
    /// (chapter 08).
    CatalogUnavailable = 39070, "ER_SPRIG_CATALOG_UNAVAILABLE", "SprigCatalogUnavailable", SQLSTATE_CONNECTION, true;
    /// A catalog change met a concurrent one: a pending DDL job on the
    /// object, a busy version row or a held migration lock (chapter 08).
    CatalogConflict = 39071, "ER_SPRIG_CATALOG_CONFLICT", "SprigCatalogConflict", SQLSTATE_RETRY, true;
    /// The tenant is not active or its catalog is not bootstrapped yet
    /// (chapter 08).
    TenantNotReady = 39072, "ER_SPRIG_TENANT_NOT_READY", "SprigTenantNotReady", SQLSTATE_GENERAL, false;
    /// The tenant's catalog has a format newer than this binary reads
    /// (chapter 08).
    CatalogFormatTooNew = 39073, "ER_SPRIG_CATALOG_FORMAT_TOO_NEW", "SprigCatalogFormatTooNew", SQLSTATE_GENERAL, false;
    /// A setting value lies outside the bounds in force (chapter 08).
    SettingOutOfBounds = 39080, "ER_SPRIG_SETTING_OUT_OF_BOUNDS", "SprigSettingOutOfBounds", SQLSTATE_GENERAL, false;
    /// The setting key is not in the registry (chapter 08).
    SettingUnknown = 39081, "ER_SPRIG_SETTING_UNKNOWN", "SprigSettingUnknown", SQLSTATE_GENERAL, false;
    /// The key can't be set at this scope (chapter 08).
    SettingScopeNotAllowed = 39082, "ER_SPRIG_SETTING_SCOPE_NOT_ALLOWED", "SprigSettingScopeNotAllowed", SQLSTATE_GENERAL, false;
    /// A wider scope locked the key (chapter 08).
    SettingLocked = 39083, "ER_SPRIG_SETTING_LOCKED", "SprigSettingLocked", SQLSTATE_GENERAL, false;
    /// The value doesn't parse as the key's type (chapter 08).
    SettingBadValue = 39084, "ER_SPRIG_SETTING_BAD_VALUE", "SprigSettingBadValue", SQLSTATE_GENERAL, false;
    /// The cluster has no writer for the shard (chapter 09).
    ClusterUnavailable = 39100, "ER_SPRIG_CLUSTER_UNAVAILABLE", "SprigClusterUnavailable", SQLSTATE_CONNECTION, true;
    /// A managed cluster fenced itself (chapter 09).
    ClusterFenced = 39101, "ER_SPRIG_CLUSTER_FENCED", "SprigClusterFenced", SQLSTATE_GENERAL, false;
    /// A statement needs a capability a cluster holding the object lacks,
    /// with no fallback (chapter 09).
    ClusterCapabilityRequired = 39102, "ER_SPRIG_CLUSTER_CAPABILITY_REQUIRED", "SprigClusterCapabilityRequired", SQLSTATE_GENERAL, false;
    /// A shard fence couldn't be completed (killed sessions didn't stop in
    /// time, or a rename waited too long); it was rolled back (chapter 09).
    /// Not yet in Appendix E.
    ClusterFenceFailed = 39103, "ER_SPRIG_CLUSTER_FENCE_FAILED", "SprigClusterFenceFailed", SQLSTATE_GENERAL, true;
    /// No cluster has this id in the data center (chapter 09, Admin API).
    /// Not yet in Appendix E.
    ClusterNotFound = 39104, "ER_SPRIG_CLUSTER_NOT_FOUND", "SprigClusterNotFound", SQLSTATE_GENERAL, false;
    /// A cluster with this name already exists in the data center (chapter
    /// 09, Admin API). Not yet in Appendix E.
    ClusterExists = 39105, "ER_SPRIG_CLUSTER_EXISTS", "SprigClusterExists", SQLSTATE_GENERAL, false;
    /// The cluster's admin state doesn't allow the request, such as
    /// draining a cluster that isn't active (chapter 09). Not yet in
    /// Appendix E.
    ClusterStateInvalid = 39106, "ER_SPRIG_CLUSTER_STATE_INVALID", "SprigClusterStateInvalid", SQLSTATE_GENERAL, false;
    /// A mover cutover moved the shard while a transaction was open: the
    /// gateway rolled it back (sent inside a 1213; chapter 10).
    ShardMoved = 39110, "ER_SPRIG_SHARD_MOVED", "SprigShardMoved", SQLSTATE_RETRY, true;
    /// The move's target cluster lacks a capability the shard uses
    /// (chapter 10, "Preparing").
    MoveTargetIncompatible = 39111, "ER_SPRIG_MOVE_TARGET_INCOMPATIBLE", "SprigMoveTargetIncompatible", SQLSTATE_GENERAL, false;
    /// A binlog event failed its checksum or doesn't parse (chapter 10). Not
    /// yet in Appendix E.
    BinlogCorrupt = 39112, "ER_SPRIG_BINLOG_CORRUPT", "SprigBinlogCorrupt", SQLSTATE_GENERAL, false;
    /// The binlog stream to a MariaDB node broke or went silent; reconnect
    /// from the last committed position (chapter 10). Not yet in Appendix E.
    BinlogDisconnected = 39113, "ER_SPRIG_BINLOG_DISCONNECTED", "SprigBinlogDisconnected", SQLSTATE_CONNECTION, true;
    /// The server or binlog uses something the reader can't handle, such as
    /// an unknown checksum algorithm, an encrypted binlog file or an
    /// authentication plugin it lacks (chapter 10). Not yet in Appendix E.
    BinlogUnsupported = 39114, "ER_SPRIG_BINLOG_UNSUPPORTED", "SprigBinlogUnsupported", SQLSTATE_GENERAL, false;
    /// No mover job by that id in the data center (chapter 10).
    JobNotFound = 39130, "ER_SPRIG_JOB_NOT_FOUND", "SprigJobNotFound", SQLSTATE_GENERAL, false;
    /// Another job holds a lock on the shard (chapter 10, "Job record").
    ShardLocked = 39131, "ER_SPRIG_SHARD_LOCKED", "SprigShardLocked", SQLSTATE_GENERAL, true;
    /// The job's state doesn't allow the request, such as cancelling a job
    /// after its cutover committed (chapter 10).
    JobStateInvalid = 39132, "ER_SPRIG_JOB_STATE_INVALID", "SprigJobStateInvalid", SQLSTATE_GENERAL, false;
    /// The worker lost the job: its lease ended or a newer owner raised
    /// the fencing token (chapter 10, "Jobs and workers").
    JobLeaseLost = 39133, "ER_SPRIG_JOB_LEASE_LOST", "SprigJobLeaseLost", SQLSTATE_GENERAL, true;
    /// The source's binlog no longer holds the position the job needs
    /// (purged, or a failover lost transactions the target already has):
    /// the copy restarts (chapter 10, "Failure handling").
    MoveSourceDiverged = 39134, "ER_SPRIG_MOVE_SOURCE_DIVERGED", "SprigMoveSourceDiverged", SQLSTATE_GENERAL, true;
    /// A cutover attempt was aborted (over budget, fence failed, commit
    /// rejected); the sources stay authoritative and the job retries
    /// (chapter 10, "Cutover").
    MoveCutoverAborted = 39135, "ER_SPRIG_MOVE_CUTOVER_ABORTED", "SprigMoveCutoverAborted", SQLSTATE_GENERAL, true;
    /// Verification found the target different from the source after
    /// `mover.verify_max_repairs` repairs (chapter 10, "Verification").
    MoveVerifyFailed = 39136, "ER_SPRIG_MOVE_VERIFY_FAILED", "SprigMoveVerifyFailed", SQLSTATE_GENERAL, false;
    /// The source changed in a way the mover can't follow: statement-based
    /// DML in a moved schema, a table without a primary key, or DDL during
    /// a cutover (chapter 10, "Filtering and routing").
    MoveUnsupported = 39137, "ER_SPRIG_MOVE_UNSUPPORTED", "SprigMoveUnsupported", SQLSTATE_GENERAL, false;
    /// No cluster can take the placement (chapter 11; tenant bootstrap's
    /// interim placement in chapter 08).
    PlacementUnavailable = 39120, "ER_SPRIG_PLACEMENT_UNAVAILABLE", "SprigPlacementUnavailable", SQLSTATE_GENERAL, true;
    /// No automatic index (or recommendation) by that id or name in the
    /// tenant (chapter 13, `sprigctl index`).
    AutoIndexNotFound = 39201, "ER_SPRIG_AUTO_INDEX_NOT_FOUND", "SprigAutoIndexNotFound", SQLSTATE_GENERAL, false;
    /// The automatic index's state doesn't allow the action (approve a
    /// non-recommendation, drop a pinned one, ...), or it changed meanwhile.
    AutoIndexStateInvalid = 39202, "ER_SPRIG_AUTO_INDEX_STATE_INVALID", "SprigAutoIndexStateInvalid", SQLSTATE_GENERAL, false;
    /// Unknown sink or connection (chapter 51).
    SinkNotFound = 39210, "ER_SPRIG_SINK_NOT_FOUND", "SprigSinkNotFound", SQLSTATE_GENERAL, false;
    /// A sink or connection name in use (chapter 51).
    SinkExists = 39211, "ER_SPRIG_SINK_EXISTS", "SprigSinkExists", SQLSTATE_GENERAL, false;
    /// Creating or altering sinks or connections without `SPRIG_SINK_ADMIN`, naming a connection without `USAGE`, or a fan-in sink without `SPRIG_FAN_IN` (chapter 51).
    SinkPrivilege = 39212, "ER_SPRIG_SINK_PRIVILEGE", "SprigSinkPrivilege", SQLSTATE_SYNTAX_ACCESS, false;
    /// A destination not on `sink.destination_allowlist`, or an internal address not listed literally (chapter 51).
    SinkDestinationNotAllowed = 39213, "ER_SPRIG_SINK_DESTINATION_NOT_ALLOWED", "SprigSinkDestinationNotAllowed", SQLSTATE_GENERAL, false;
    /// The destination's tags don't satisfy the tenant's residency policy (chapter 51).
    SinkResidency = 39214, "ER_SPRIG_SINK_RESIDENCY", "SprigSinkResidency", SQLSTATE_GENERAL, false;
    /// Dropping a connection that sinks use (chapter 51).
    SinkConnectionInUse = 39215, "ER_SPRIG_SINK_CONNECTION_IN_USE", "SprigSinkConnectionInUse", SQLSTATE_GENERAL, false;
    /// A credential the connection type doesn't take, or one it needs is missing (chapter 51).
    SinkCredentialsInvalid = 39216, "ER_SPRIG_SINK_CREDENTIALS_INVALID", "SprigSinkCredentialsInvalid", SQLSTATE_GENERAL, false;
    /// `EXACTLY ONCE` for a source and target that can't give it (chapter 51).
    SinkExactlyOnceUnsupported = 39217, "ER_SPRIG_SINK_EXACTLY_ONCE_UNSUPPORTED", "SprigSinkExactlyOnceUnsupported", SQLSTATE_GENERAL, false;
    /// An unknown or invalid sink or connection option, template or column list (chapter 51).
    SinkOptionInvalid = 39218, "ER_SPRIG_SINK_OPTION_INVALID", "SprigSinkOptionInvalid", SQLSTATE_GENERAL, false;
    /// A transform that writes, calls out or isn't deterministic under `EXACTLY ONCE` (chapter 51).
    SinkTransformInvalid = 39219, "ER_SPRIG_SINK_TRANSFORM_INVALID", "SprigSinkTransformInvalid", SQLSTATE_GENERAL, false;
    /// A sink script raised an error or returned a malformed result (chapter 51).
    SinkScriptFailed = 39220, "ER_SPRIG_SINK_SCRIPT_FAILED", "SprigSinkScriptFailed", SQLSTATE_GENERAL, false;
    /// The destination refused or failed a delivery (chapter 51).
    SinkDeliveryFailed = 39221, "ER_SPRIG_SINK_DELIVERY_FAILED", "SprigSinkDeliveryFailed", SQLSTATE_GENERAL, true;
    /// The sink paused itself after `sink.pause_after_minutes` of failures (chapter 51).
    SinkPaused = 39222, "ER_SPRIG_SINK_PAUSED", "SprigSinkPaused", SQLSTATE_GENERAL, false;
    /// A sink's position fell behind its source's retention (chapter 51).
    SinkSourceLost = 39223, "ER_SPRIG_SINK_SOURCE_LOST", "SprigSinkSourceLost", SQLSTATE_GENERAL, false;
    /// The destination table or file schema doesn't fit the records (chapter 51).
    SinkSchemaMismatch = 39224, "ER_SPRIG_SINK_SCHEMA_MISMATCH", "SprigSinkSchemaMismatch", SQLSTATE_GENERAL, false;
    /// A record alone exceeds what the destination accepts (chapter 51).
    SinkRecordTooLarge = 39225, "ER_SPRIG_SINK_RECORD_TOO_LARGE", "SprigSinkRecordTooLarge", SQLSTATE_GENERAL, false;
    /// Over `sink.max_per_tenant` or a throughput quota (chapter 51).
    SinkQuota = 39226, "ER_SPRIG_SINK_QUOTA", "SprigSinkQuota", SQLSTATE_GENERAL, false;
    /// A sink task's epoch moved on; the old worker stops (chapter 51).
    SinkFenced = 39227, "ER_SPRIG_SINK_FENCED", "SprigSinkFenced", SQLSTATE_GENERAL, true;
    /// The worker's cloud identity without the operator's permission (chapter 51).
    SinkPlatformIdentityDenied = 39228, "ER_SPRIG_SINK_PLATFORM_IDENTITY_DENIED", "SprigSinkPlatformIdentityDenied", SQLSTATE_GENERAL, false;
    /// A sink's owner can no longer read its source (chapter 51).
    SinkOwnerPrivilegeLost = 39229, "ER_SPRIG_SINK_OWNER_PRIVILEGE_LOST", "SprigSinkOwnerPrivilegeLost", SQLSTATE_GENERAL, false;
    /// `sink.enabled` is off, or the target kind isn't in `sink.allowed_kinds` (chapter 51).
    SinkDisabled = 39230, "ER_SPRIG_SINK_DISABLED", "SprigSinkDisabled", SQLSTATE_GENERAL, false;
    /// A format or compression the target or source can't produce (chapter 51).
    SinkFormatUnsupported = 39231, "ER_SPRIG_SINK_FORMAT_UNSUPPORTED", "SprigSinkFormatUnsupported", SQLSTATE_GENERAL, false;
    /// An operation the sink's state doesn't allow (chapter 51).
    SinkState = 39232, "ER_SPRIG_SINK_STATE", "SprigSinkState", SQLSTATE_GENERAL, false;
    /// A platform connection the tenant isn't granted (chapter 51).
    SinkPlatformConnectionNotGranted = 39233, "ER_SPRIG_SINK_PLATFORM_CONNECTION_NOT_GRANTED", "SprigSinkPlatformConnectionNotGranted", SQLSTATE_GENERAL, false;
    /// A worker can't read a platform connection's secret file (chapter 51).
    SinkPlatformSecretUnavailable = 39234, "ER_SPRIG_SINK_PLATFORM_SECRET_UNAVAILABLE", "SprigSinkPlatformSecretUnavailable", SQLSTATE_GENERAL, true;
    /// A sink script's `db` handle was given anything but a read (chapter 51).
    SinkScriptReadOnly = 39235, "ER_SPRIG_SINK_SCRIPT_READ_ONLY", "SprigSinkScriptReadOnly", SQLSTATE_GENERAL, false;
    /// A distribution-key part has a type that can't be a key, or the
    /// values don't match the key's parts (chapter 15).
    UnsupportedKeyType = 39400, "ER_SPRIG_UNSUPPORTED_KEY_TYPE", "SprigUnsupportedKeyType", SQLSTATE_GENERAL, false;
    /// A string key uses a collation outside chapter 15's supported set.
    UnsupportedKeyCollation = 39401, "ER_SPRIG_UNSUPPORTED_KEY_COLLATION", "SprigUnsupportedKeyCollation", SQLSTATE_GENERAL, false;
    /// No cached shard map for the group and etcd unreachable (chapter 15).
    ShardMapUnavailable = 39403, "ER_SPRIG_SHARD_MAP_UNAVAILABLE", "SprigShardMapUnavailable", SQLSTATE_CONNECTION, true;
    /// A key value can't be normalized under its group's key: a string not
    /// valid in its character set or not convertible to the key's, a number
    /// that doesn't parse. The router scatters such a value (chapter 15).
    KeyValueUnroutable = 39405, "ER_SPRIG_KEY_VALUE_UNROUTABLE", "SprigKeyValueUnroutable", SQLSTATE_GENERAL, false;
    /// A sharding pin contradicts another pin or a key rule (chapter 16).
    PinConflict = 39410, "ER_SPRIG_PIN_CONFLICT", "SprigPinConflict", SQLSTATE_GENERAL, false;
    /// In R1, a pin that would change the key or group of a table whose
    /// group has already split (chapter 16).
    PinNeedsRekey = 39411, "ER_SPRIG_PIN_NEEDS_REKEY", "SprigPinNeedsRekey", SQLSTATE_GENERAL, false;
    /// No ID block can be fetched: the catalog cluster is unreachable and
    /// the cached blocks are used up (chapter 19).
    IdBlockUnavailable = 39460, "ER_SPRIG_ID_BLOCK_UNAVAILABLE", "SprigIdBlockUnavailable", SQLSTATE_GENERAL, true;
    /// A lookup shard is unreachable for a claim (chapter 19).
    LookupUnavailable = 39461, "ER_SPRIG_LOOKUP_UNAVAILABLE", "SprigLookupUnavailable", SQLSTATE_CONNECTION, true;
    /// An explicit `AUTO_INCREMENT` value lies in an ID block another
    /// gateway still holds (chapter 19).
    IdValueReserved = 39463, "ER_SPRIG_ID_VALUE_RESERVED", "SprigIdValueReserved", SQLSTATE_GENERAL, false;
    /// A cascading foreign-key action across groups the gateway can't
    /// apply: more than 15 levels, or an `ON UPDATE CASCADE` it can't map
    /// (chapter 19).
    FkActionNotDistributed = 39462, "ER_SPRIG_FK_ACTION_NOT_DISTRIBUTED", "SprigFkActionNotDistributed", SQLSTATE_GENERAL, false;
    /// A split, merge or move job already holds the shard (chapter 20).
    ShardBusy = 39470, "ER_SPRIG_SHARD_BUSY", "SprigShardBusy", SQLSTATE_GENERAL, true;
    /// The group isn't `active`, the range is a single hash, or the split
    /// or merge doesn't fit the map (chapter 20).
    SplitNotAllowed = 39471, "ER_SPRIG_SPLIT_NOT_ALLOWED", "SprigSplitNotAllowed", SQLSTATE_GENERAL, false;
    /// A whole-tenant move (to another cluster or data center) of a tenant
    /// whose tables are in table groups: R1 refuses it, since it would move
    /// the catalog map but not the groups' shard maps (chapters 10, 20).
    ShardedTenantMove = 39472, "ER_SPRIG_SHARDED_TENANT_MOVE", "SprigShardedTenantMove", SQLSTATE_GENERAL, false;
    /// The router has no exact plan for the statement across shards; the
    /// message names a reason code (chapter 17, "Statements R1 rejects").
    StatementNotDistributable = 39420, "ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE", "SprigStatementNotDistributable", SQLSTATE_GENERAL, false;
    /// A shard a transaction's statement reached moved (a split, merge or
    /// move fenced it) while the transaction was open: the transaction
    /// rolled back; retry it (chapter 17, "Failure handling").
    ShardMapChanged = 39424, "ER_SPRIG_SHARD_MAP_CHANGED", "SprigShardMapChanged", SQLSTATE_RETRY, true;
    /// A statement's rows held in the gateway passed
    /// `router.max_statement_memory_mb` (chapter 17).
    RouterMemoryLimit = 39422, "ER_SPRIG_ROUTER_MEMORY_LIMIT", "SprigRouterMemoryLimit", SQLSTATE_GENERAL, false;
    /// A runtime row limit of the router was passed: a gathered read
    /// copied more than `router.gather.max_rows` rows (chapter 17).
    PlanLimitExceeded = 39421, "ER_SPRIG_PLAN_LIMIT_EXCEEDED", "SprigPlanLimitExceeded", SQLSTATE_GENERAL, false;
    /// Too many failed logins from this address: it is refused for
    /// `auth.ip_block_seconds` before the greeting (chapter 14, "Rate
    /// limits").
    LoginRateLimited = 39300, "ER_SPRIG_LOGIN_RATE_LIMITED", "SprigLoginRateLimited", SQLSTATE_GENERAL, true;
    /// Logins can't be decided now: the tenant's credentials can't be
    /// loaded (catalog or directory unreachable), or hashing is saturated
    /// (chapter 14).
    AuthUnavailable = 39301, "ER_SPRIG_AUTH_UNAVAILABLE", "SprigAuthUnavailable", SQLSTATE_CONNECTION, true;
    /// The login came to a data center outside the tenant's allowed ones
    /// and `residency.deny_remote_access` is on (chapter 14, "Credential
    /// residency").
    RemoteAccessDenied = 39302, "ER_SPRIG_REMOTE_ACCESS_DENIED", "SprigRemoteAccessDenied", SQLSTATE_GENERAL, false;
    /// The tenant, or a tenant above it in the tree, is suspended; logins
    /// are refused (chapter 14).
    TenantSuspended = 39303, "ER_SPRIG_TENANT_SUSPENDED", "SprigTenantSuspended", SQLSTATE_GENERAL, false;
    /// The account must sign in with a second factor, or enrol one first
    /// (chapter 14, `auth.mfa_required`); operator mode without one under
    /// `studio.operator_require_mfa` is 39926.
    MfaRequired = 39304, "ER_SPRIG_MFA_REQUIRED", "SprigMfaRequired", SQLSTATE_GENERAL, false;
    /// Impersonation refused: not an administrator over the user's tenant,
    /// the user administers the caller's tenant, `auth.impersonation` is
    /// off, no reason, too long, or from an impersonation session (chapter
    /// 14).
    ImpersonationDenied = 39305, "ER_SPRIG_IMPERSONATION_DENIED", "SprigImpersonationDenied", SQLSTATE_GENERAL, false;
    /// The session's account was dropped or locked, or its tenant
    /// suspended, while it was connected; the session closes
    /// (`auth.close_sessions_on_revoke`, chapter 14).
    SessionRevoked = 39306, "ER_SPRIG_SESSION_REVOKED", "SprigSessionRevoked", SQLSTATE_CONNECTION, false;
    /// The tenant name is taken (chapter 29; the `tenant-names/` guard of
    /// chapter 23).
    TenantExists = 39360, "ER_SPRIG_TENANT_EXISTS", "SprigTenantExists", SQLSTATE_GENERAL, false;
    /// A subtree move would strand grants or shares from tenants that would
    /// stop being ancestors (chapter 23; chapter 42 checks shares).
    TenantMoveBlocked = 39361, "ER_SPRIG_TENANT_MOVE_BLOCKED", "SprigTenantMoveBlocked", SQLSTATE_GENERAL, false;
    /// A tenant with children can't be dropped (chapter 29; chapter 23's
    /// delete guard).
    TenantHasChildren = 39362, "ER_SPRIG_TENANT_HAS_CHILDREN", "SprigTenantHasChildren", SQLSTATE_GENERAL, false;
    /// The tenant's authorization engine filters rows of a table the
    /// statement touches, and the gateway can't apply such a filter yet
    /// (chapter 29, "Pluggable authorization").
    RlsUnsupported = 39366, "ER_SPRIG_RLS_UNSUPPORTED", "SprigRlsUnsupported", SQLSTATE_GENERAL, false;
    /// A stored program wasn't recorded with the session's backend account
    /// as its definer; the gateway dropped it again (chapter 29, "Definers
    /// of stored code").
    DefinerRefused = 39367, "ER_SPRIG_DEFINER_REFUSED", "SprigDefinerRefused", SQLSTATE_GENERAL, false;
    /// A change of `auth.authorizer` names an engine that some live
    /// gateway doesn't report knowing; upgrade those gateways first
    /// (chapter 29, "Choosing the engine").
    AuthorizerUnknownToGateways = 39368, "ER_SPRIG_AUTHORIZER_UNKNOWN_TO_GATEWAYS", "SprigAuthorizerUnknownToGateways", SQLSTATE_GENERAL, false;
    /// The control plane's store is unreachable (chapter 23).
    MetaUnavailable = 39500, "ER_SPRIG_META_UNAVAILABLE", "SprigMetaUnavailable", SQLSTATE_CONNECTION, true;
    /// This controller no longer leads (or never did): its session was lost
    /// or a guarded transaction failed its fencing-token compare. The caller
    /// steps down; it must not retry (chapter 23). Not yet in Appendix E.
    NotLeader = 39504, "ER_SPRIG_NOT_LEADER", "SprigNotLeader", SQLSTATE_GENERAL, false;
    /// The tenant moved to another data center while the session held
    /// state that can't follow it (temporary tables, `GET_LOCK` locks,
    /// `LOCK TABLES`, XA); the session must reconnect (chapter 24).
    SessionMoved = 39502, "ER_SPRIG_SESSION_MOVED", "SprigSessionMoved", SQLSTATE_CONNECTION, true;
    /// The data center isn't registered (chapter 24).
    UnknownDc = 39503, "ER_SPRIG_UNKNOWN_DC", "SprigUnknownDc", SQLSTATE_GENERAL, false;
    /// A tenant would be deeper than `tenant.max_depth` (chapter 23).
    TenantDepthExceeded = 39508, "ER_SPRIG_TENANT_DEPTH_EXCEEDED", "SprigTenantDepthExceeded", SQLSTATE_GENERAL, false;
    /// A tree operation on a subtree with an open move (chapter 23).
    TenantTreeBusy = 39509, "ER_SPRIG_TENANT_TREE_BUSY", "SprigTenantTreeBusy", SQLSTATE_GENERAL, true;
    /// A residency policy doesn't parse (chapter 26, "Interfaces").
    ResidencyInvalid = 39506, "ER_SPRIG_RESIDENCY_INVALID", "SprigResidencyInvalid", SQLSTATE_GENERAL, false;
    /// A residency policy allows no active data center within the
    /// ancestors' sets, or its compliance plan has no cluster with headroom
    /// in the allowed ones (chapter 26).
    ResidencyUnsatisfiable = 39507, "ER_SPRIG_RESIDENCY_UNSATISFIABLE", "SprigResidencyUnsatisfiable", SQLSTATE_GENERAL, false;
    /// A VM cluster can't be created: too few free hosts in the data
    /// center (chapter 27).
    ProvisionNoHosts = 39513, "ER_SPRIG_PROVISION_NO_HOSTS", "SprigProvisionNoHosts", SQLSTATE_GENERAL, true;
    /// No data center the new tenant's residency allows is active, stable
    /// and has a cluster that can take it (chapter 24, "Least-busy
    /// placement").
    NoDcCapacity = 39525, "ER_SPRIG_NO_DC_CAPACITY", "SprigNoDcCapacity", SQLSTATE_GENERAL, true;
    /// The provisioner doesn't support the operation (chapter 27).
    ProvisionerUnsupported = 39510, "ER_SPRIG_PROVISIONER_UNSUPPORTED", "SprigProvisionerUnsupported", SQLSTATE_GENERAL, false;
    /// The tenant's home DC holds no valid DC lease (its permit lapsed or
    /// was refused): writes are refused, reads continue; retry after the
    /// tenant is re-routed (chapter 25).
    DcFenced = 39501, "ER_SPRIG_DC_FENCED", "SprigDcFenced", SQLSTATE_GENERAL, true;
    /// A manual failover can't run: the old home is healthy, the target is
    /// not allowed or lacks a unit, or data loss wasn't accepted (chapter 25).
    FailoverRefused = 39505, "ER_SPRIG_FAILOVER_REFUSED", "SprigFailoverRefused", SQLSTATE_GENERAL, false;
    /// A cluster failover stopped before promoting: the loss estimate is
    /// over the RPO, two replicas diverge, the writer isn't confirmed down,
    /// or `failover.mode` is manual; an operator decides (chapter 25).
    FailoverBlocked = 39512, "ER_SPRIG_FAILOVER_BLOCKED", "SprigFailoverBlocked", SQLSTATE_GENERAL, false;
    /// A write reached a copy that stays fenced after an outage (a
    /// returning server or DC); retry after re-routing (chapter 25).
    RecoveryFenced = 39530, "ER_SPRIG_RECOVERY_FENCED", "SprigRecoveryFenced", SQLSTATE_GENERAL, true;
    /// A returning DC admits new connections in steps; retry after the
    /// jittered delay in the message (chapter 25).
    AdmissionBackoff = 39531, "ER_SPRIG_ADMISSION_BACKOFF", "SprigAdmissionBackoff", SQLSTATE_CONNECTION, true;
    /// An external cluster failed its registration prechecks; the message
    /// lists every missing item (chapter 27). Not yet in Appendix E.
    ClusterPrecheckFailed = 39511, "ER_SPRIG_CLUSTER_PRECHECK_FAILED", "SprigClusterPrecheckFailed", SQLSTATE_GENERAL, false;
    /// A write would add a second cluster without XA to a transaction
    /// under `txn.non_xa_policy = reject`; the statement didn't run
    /// (chapter 18).
    CrossClusterWriteRejected = 39440, "ER_SPRIG_CROSS_CLUSTER_WRITE_REJECTED", "SprigCrossClusterWriteRejected", SQLSTATE_GENERAL, false;
    /// A distributed commit's outcome is unknown; the connection is then
    /// closed (chapter 18).
    CommitOutcomeUnknown = 39441, "ER_SPRIG_COMMIT_OUTCOME_UNKNOWN", "SprigCommitOutcomeUnknown", SQLSTATE_CONNECTION, false;
    /// A best-effort commit committed only some clusters (chapter 18).
    PartialCommit = 39442, "ER_SPRIG_PARTIAL_COMMIT", "SprigPartialCommit", SQLSTATE_GENERAL, false;
    /// A distributed transaction was rolled back; retry it (chapter 18).
    DtxRolledBack = 39443, "ER_SPRIG_DTX_ROLLED_BACK", "SprigDtxRolledBack", SQLSTATE_RETRY, true;
    /// A schema change failed on a shard after others ran it, and the job
    /// could not finish there; the reconciler completes it (chapter 21).
    /// A standard MariaDB error from the shard is returned as itself.
    DdlFailed = 39480, "ER_SPRIG_DDL_FAILED", "SprigDdlFailed", SQLSTATE_GENERAL, false;
    /// A schema change that can't be distributed across shards in R1
    /// (chapter 21).
    DdlUnsupported = 39481, "ER_SPRIG_DDL_UNSUPPORTED", "SprigDdlUnsupported", SQLSTATE_GENERAL, false;
    /// The shards of a table disagree on its definition before a change;
    /// the change is refused until the reconciler or an operator repairs
    /// them (chapter 21).
    DdlShardDrift = 39483, "ER_SPRIG_DDL_SHARD_DRIFT", "SprigDdlShardDrift", SQLSTATE_GENERAL, false;
    /// A transaction would touch more clusters than `txn.max_branches`
    /// (chapter 18).
    TooManyBranches = 39444, "ER_SPRIG_TOO_MANY_BRANCHES", "SprigTooManyBranches", SQLSTATE_GENERAL, false;
    /// No prepared branch of the named distributed transaction is on the
    /// data center's clusters: nothing is in doubt (`sprigctl txn show` and
    /// `resolve`, chapter 18).
    TxnNotInDoubt = 39447, "ER_SPRIG_TXN_NOT_IN_DOUBT", "SprigTxnNotInDoubt", SQLSTATE_GENERAL, false;
    /// A held branch (Sprig 2PC, chapter 18) ran a statement that doesn't
    /// replay to the same result; the transaction is rolled back.
    HeldBranchStatement = 39446, "ER_SPRIG_HELD_BRANCH_STATEMENT", "SprigHeldBranchStatement", SQLSTATE_GENERAL, false;
    /// A tenant move between data centers can't start: the tenant is
    /// pinned, not active, already homed in the target, already moving,
    /// or the target has no cluster for it (chapter 24).
    TenantMoveRefused = 39550, "ER_SPRIG_TENANT_MOVE_REFUSED", "SprigTenantMoveRefused", SQLSTATE_GENERAL, false;
    /// A type with two parents (chapter 32).
    TypeMultipleInheritance = 39701, "ER_SPRIG_TYPE_MULTIPLE_INHERITANCE", "SprigTypeMultipleInheritance", SQLSTATE_GENERAL, false;
    /// A field clashes with an inherited one, or with a subtype's (chapter
    /// 32).
    TypeFieldConflict = 39702, "ER_SPRIG_TYPE_FIELD_CONFLICT", "SprigTypeFieldConflict", SQLSTATE_GENERAL, false;
    /// An insert of an abstract type (chapter 32).
    TypeAbstract = 39703, "ER_SPRIG_TYPE_ABSTRACT", "SprigTypeAbstract", SQLSTATE_GENERAL, false;
    /// A write to a type or path column (chapter 32).
    TypeColumnReadOnly = 39705, "ER_SPRIG_TYPE_COLUMN_READ_ONLY", "SprigTypeColumnReadOnly", SQLSTATE_GENERAL, false;
    /// A key update on a typed or hierarchical row (chapter 32).
    TypeKeyUpdate = 39706, "ER_SPRIG_TYPE_KEY_UPDATE", "SprigTypeKeyUpdate", SQLSTATE_GENERAL, false;
    /// Dropping a type with subtypes without `CASCADE` (chapter 32).
    TypeHasDependents = 39707, "ER_SPRIG_TYPE_HAS_DEPENDENTS", "SprigTypeHasDependents", SQLSTATE_GENERAL, false;
    /// A type path over 255 characters, or a hierarchy deeper than
    /// `types.max_depth` (chapter 32).
    TypePathTooLong = 39708, "ER_SPRIG_TYPE_PATH_TOO_LONG", "SprigTypePathTooLong", SQLSTATE_GENERAL, false;
    /// A statement form on typed or hierarchical tables this release
    /// doesn't support (chapter 32).
    TypeUnsupported = 39710, "ER_SPRIG_TYPE_UNSUPPORTED", "SprigTypeUnsupported", SQLSTATE_GENERAL, false;
    /// A record moved under its own descendant (chapter 32).
    HierarchyCycle = 39711, "ER_SPRIG_HIERARCHY_CYCLE", "SprigHierarchyCycle", SQLSTATE_GENERAL, false;
    /// A record's parent doesn't exist (chapter 32).
    HierarchyParentNotFound = 39712, "ER_SPRIG_HIERARCHY_PARENT_NOT_FOUND", "SprigHierarchyParentNotFound", SQLSTATE_GENERAL, false;
    /// A record hierarchy over its `MAX DEPTH` or path length (chapter 32).
    HierarchyTooDeep = 39713, "ER_SPRIG_HIERARCHY_TOO_DEEP", "SprigHierarchyTooDeep", SQLSTATE_GENERAL, false;
    /// Deleting a record with children under `ON DELETE RESTRICT` (chapter
    /// 32).
    HierarchyHasChildren = 39714, "ER_SPRIG_HIERARCHY_HAS_CHILDREN", "SprigHierarchyHasChildren", SQLSTATE_GENERAL, false;
    /// A subtree move or cascade over `hierarchy.max_move_rows` (chapter
    /// 32).
    SubtreeTooLarge = 39715, "ER_SPRIG_SUBTREE_TOO_LARGE", "SprigSubtreeTooLarge", SQLSTATE_GENERAL, false;
    /// `ADD HIERARCHY` found orphans or cycles (chapter 32).
    HierarchyBackfillFailed = 39716, "ER_SPRIG_HIERARCHY_BACKFILL_FAILED", "SprigHierarchyBackfillFailed", SQLSTATE_GENERAL, false;
    /// Unknown key-value namespace (chapter 33).
    KvNamespaceNotFound = 39720, "ER_SPRIG_KV_NAMESPACE_NOT_FOUND", "SprigKvNamespaceNotFound", SQLSTATE_GENERAL, false;
    /// A key's version isn't the one the write expected (chapter 33).
    KvVersionMismatch = 39721, "ER_SPRIG_KV_VERSION_MISMATCH", "SprigKvVersionMismatch", SQLSTATE_GENERAL, false;
    /// The key exists, and the write needed it absent (chapter 33).
    KvKeyExists = 39722, "ER_SPRIG_KV_KEY_EXISTS", "SprigKvKeyExists", SQLSTATE_GENERAL, false;
    /// An empty key, or one over 1024 bytes (chapter 33).
    KvInvalidKey = 39723, "ER_SPRIG_KV_INVALID_KEY", "SprigKvInvalidKey", SQLSTATE_GENERAL, false;
    /// A value over the namespace's limit or not in its codec (chapter 33).
    KvValueInvalid = 39724, "ER_SPRIG_KV_VALUE_INVALID", "SprigKvValueInvalid", SQLSTATE_GENERAL, false;
    /// The key holds another type (chapter 33; Redis `WRONGTYPE`).
    KvWrongType = 39730, "ER_SPRIG_KV_WRONG_TYPE", "SprigKvWrongType", SQLSTATE_GENERAL, false;
    /// A hash field or member over 2024 bytes (chapter 33).
    KvElementTooLong = 39731, "ER_SPRIG_KV_ELEMENT_TOO_LONG", "SprigKvElementTooLong", SQLSTATE_GENERAL, false;
    /// Unknown vector index (chapter 34).
    VectorIndexNotFound = 39735, "ER_SPRIG_VECTOR_INDEX_NOT_FOUND", "SprigVectorIndexNotFound", SQLSTATE_GENERAL, false;
    /// A vector of the wrong length (chapter 34).
    VectorDimensionMismatch = 39736, "ER_SPRIG_VECTOR_DIMENSION_MISMATCH", "SprigVectorDimensionMismatch", SQLSTATE_GENERAL, false;
    /// A metric the index doesn't use, or a zero vector under a cosine
    /// or dotProduct index (chapter 34).
    VectorMetricMismatch = 39737, "ER_SPRIG_VECTOR_METRIC_MISMATCH", "SprigVectorMetricMismatch", SQLSTATE_GENERAL, false;
    /// `limit` over `numCandidates`, or over `vector.max_k` (chapter 34).
    VectorLimitInvalid = 39738, "ER_SPRIG_VECTOR_LIMIT_INVALID", "SprigVectorLimitInvalid", SQLSTATE_GENERAL, false;
    /// A filter on a field the index can't evaluate (chapter 34).
    VectorFilterNotIndexed = 39739, "ER_SPRIG_VECTOR_FILTER_NOT_INDEXED", "SprigVectorFilterNotIndexed", SQLSTATE_GENERAL, false;
    /// Over `vector.max_indexes_per_table` (chapter 34).
    VectorTooManyIndexes = 39740, "ER_SPRIG_VECTOR_TOO_MANY_INDEXES", "SprigVectorTooManyIndexes", SQLSTATE_GENERAL, false;
    /// Unknown dimensional model (chapter 35).
    DimModelNotFound = 39750, "ER_SPRIG_DIM_MODEL_NOT_FOUND", "SprigDimModelNotFound", SQLSTATE_GENERAL, false;
    /// Unknown measure, dimension or level (chapter 35).
    DimUnknownMember = 39751, "ER_SPRIG_DIM_UNKNOWN_MEMBER", "SprigDimUnknownMember", SQLSTATE_GENERAL, false;
    /// A model naming a missing table, column or key (chapter 35).
    DimInvalidModel = 39752, "ER_SPRIG_DIM_INVALID_MODEL", "SprigDimInvalidModel", SQLSTATE_GENERAL, false;
    /// Over `analytics.max_groups` (chapter 35).
    DimTooManyGroups = 39753, "ER_SPRIG_DIM_TOO_MANY_GROUPS", "SprigDimTooManyGroups", SQLSTATE_GENERAL, false;
    /// A query the merge can't answer exactly (chapter 35).
    DimNotDecomposable = 39754, "ER_SPRIG_DIM_NOT_DECOMPOSABLE", "SprigDimNotDecomposable", SQLSTATE_GENERAL, false;
    /// A named time zone without MariaDB's zone tables (chapter 35).
    DimTzUnavailable = 39755, "ER_SPRIG_DIM_TZ_UNAVAILABLE", "SprigDimTzUnavailable", SQLSTATE_GENERAL, false;
    /// `TRUNCATE` of a versioned dimension (chapter 35).
    DimVersionedTruncate = 39756, "ER_SPRIG_DIM_VERSIONED_TRUNCATE", "SprigDimVersionedTruncate", SQLSTATE_GENERAL, false;
    /// `as_of` older than the kept history (chapter 35).
    DimHistoryPurged = 39757, "ER_SPRIG_DIM_HISTORY_PURGED", "SprigDimHistoryPurged", SQLSTATE_GENERAL, false;
    /// Bad geometry or coordinates (chapter 36).
    GeoInvalid = 39765, "ER_SPRIG_GEO_INVALID", "SprigGeoInvalid", SQLSTATE_GENERAL, false;
    /// A geographic SRID other than 4326 (chapter 36).
    GeoSridUnsupported = 39766, "ER_SPRIG_GEO_SRID_UNSUPPORTED", "SprigGeoSridUnsupported", SQLSTATE_GENERAL, false;
    /// A geodesic expression the gateway can't evaluate exactly (chapter 36).
    GeoUnsupportedExpression = 39767, "ER_SPRIG_GEO_UNSUPPORTED_EXPRESSION", "SprigGeoUnsupportedExpression", SQLSTATE_GENERAL, false;
    /// A geometry over `geo.max_vertices` (chapter 36).
    GeoTooComplex = 39768, "ER_SPRIG_GEO_TOO_COMPLEX", "SprigGeoTooComplex", SQLSTATE_GENERAL, false;
    /// Unknown space (chapter 42).
    SpaceNotFound = 39880, "ER_SPRIG_SPACE_NOT_FOUND", "SprigSpaceNotFound", SQLSTATE_GENERAL, false;
    /// A space or share name is taken (chapter 42).
    SpaceExists = 39881, "ER_SPRIG_SPACE_EXISTS", "SprigSpaceExists", SQLSTATE_GENERAL, false;
    /// Dropping a space with children or layer rows without `CASCADE`
    /// (chapter 42).
    SpaceHasChildren = 39882, "ER_SPRIG_SPACE_HAS_CHILDREN", "SprigSpaceHasChildren", SQLSTATE_GENERAL, false;
    /// A space deeper than `tenant.max_depth`, or a share chain longer than
    /// `inherit.max_depth` (chapter 42).
    SpaceDepthExceeded = 39883, "ER_SPRIG_SPACE_DEPTH_EXCEEDED", "SprigSpaceDepthExceeded", SQLSTATE_GENERAL, false;
    /// The session may not act in that space (chapter 42).
    SpaceAccessDenied = 39884, "ER_SPRIG_SPACE_ACCESS_DENIED", "SprigSpaceAccessDenied", SQLSTATE_SYNTAX_ACCESS, false;
    /// A share or reset on an ordinary object (chapter 42).
    NotInheritable = 39885, "ER_SPRIG_NOT_INHERITABLE", "SprigNotInheritable", SQLSTATE_GENERAL, false;
    /// Changing a record's identity key (chapter 42).
    InheritKeyUpdate = 39886, "ER_SPRIG_INHERIT_KEY_UPDATE", "SprigInheritKeyUpdate", SQLSTATE_GENERAL, false;
    /// Unknown share (chapter 42).
    ShareNotFound = 39887, "ER_SPRIG_SHARE_NOT_FOUND", "SprigShareNotFound", SQLSTATE_GENERAL, false;
    /// A share filter that isn't deterministic, has a subquery, or names
    /// unshared fields (chapter 42).
    ShareFilterInvalid = 39888, "ER_SPRIG_SHARE_FILTER_INVALID", "SprigShareFilterInvalid", SQLSTATE_GENERAL, false;
    /// No merge mode is possible for a share (chapter 42).
    ShareResidencyConflict = 39889, "ER_SPRIG_SHARE_RESIDENCY_CONFLICT", "SprigShareResidencyConflict", SQLSTATE_GENERAL, false;
    /// A re-share of inherited data without `WITH RESHARE` (chapter 42).
    ShareNotResharable = 39890, "ER_SPRIG_SHARE_NOT_RESHARABLE", "SprigShareNotResharable", SQLSTATE_GENERAL, false;
    /// A statement form on an inheritable object this release doesn't
    /// support (chapter 42).
    InheritStatementUnsupported = 39891, "ER_SPRIG_INHERIT_STATEMENT_UNSUPPORTED", "SprigInheritStatementUnsupported", SQLSTATE_GENERAL, false;
    /// A child tenant's object diverges from its source (chapter 42).
    InheritSchemaMismatch = 39892, "ER_SPRIG_INHERIT_SCHEMA_MISMATCH", "SprigInheritSchemaMismatch", SQLSTATE_GENERAL, false;
    /// A materialized copy lags beyond `inherit.copy_stale_seconds`
    /// (chapter 42; a note-level warning).
    InheritCopyStale = 39893, "ER_SPRIG_INHERIT_COPY_STALE", "SprigInheritCopyStale", SQLSTATE_GENERAL, true;
    /// A federated parent's cluster can't be reached (chapter 42).
    InheritFederatedUnavailable = 39894, "ER_SPRIG_INHERIT_FEDERATED_UNAVAILABLE", "SprigInheritFederatedUnavailable", SQLSTATE_GENERAL, true;
    /// A destructive schema change on an inheritable object that a share
    /// still reaches (chapter 42).
    InheritedObjectInUse = 39895, "ER_SPRIG_INHERITED_OBJECT_IN_USE", "SprigInheritedObjectInUse", SQLSTATE_GENERAL, false;
    /// A publish into an inherited object names no target where one is
    /// needed, a tenant that isn't a member, or one the publisher isn't a
    /// proper ancestor of (chapter 42, R9).
    InheritTargetTenant = 39896, "ER_SPRIG_INHERIT_TARGET_TENANT", "SprigInheritTargetTenant", SQLSTATE_GENERAL, false;
    /// A member changes what only the definition decides, drops an
    /// instance, or creates a local object with an inherited name (chapter
    /// 42, R9).
    InheritNotOverridable = 39897, "ER_SPRIG_INHERIT_NOT_OVERRIDABLE", "SprigInheritNotOverridable", SQLSTATE_GENERAL, false;
    /// A publish into a definition, or an instance, that is being dropped
    /// or has left its scope (chapter 42, R9).
    InheritDefinitionDropping = 39898, "ER_SPRIG_INHERIT_DEFINITION_DROPPING", "SprigInheritDefinitionDropping", SQLSTATE_GENERAL, false;
    /// An instance couldn't be created or reached in time (chapter 42, R9).
    InheritInstanceUnavailable = 39899, "ER_SPRIG_INHERIT_INSTANCE_UNAVAILABLE", "SprigInheritInstanceUnavailable", SQLSTATE_RETRY, true;
    /// Over `inherit.max_definitions_per_tenant` or
    /// `inherit.max_instances_per_tenant` (chapter 42, R9).
    InheritInstanceQuota = 39240, "ER_SPRIG_INHERIT_INSTANCE_QUOTA", "SprigInheritInstanceQuota", SQLSTATE_GENERAL, false;
    /// An inherited connection's `REQUIRED` option or credential isn't set
    /// for this member (chapter 42, R9).
    InheritConfigMissing = 39241, "ER_SPRIG_INHERIT_CONFIG_MISSING", "SprigInheritConfigMissing", SQLSTATE_GENERAL, false;
    /// The member's `sprig_inherited` principal lacks a privilege the
    /// definition's `GRANTS` asks for (chapter 42, R9).
    InheritRunAsPrivilege = 39242, "ER_SPRIG_INHERIT_RUN_AS_PRIVILEGE", "SprigInheritRunAsPrivilege", SQLSTATE_GENERAL, false;
    /// A fan-in read, or under `residency.cross_tenant_messaging_strict` a
    /// cross-tenant publish, would move a member's messages where its
    /// residency forbids (chapter 42, R9).
    InheritResidency = 39243, "ER_SPRIG_INHERIT_RESIDENCY", "SprigInheritResidency", SQLSTATE_GENERAL, false;
    /// A broadcast over `inherit.broadcast_max_bytes`,
    /// `inherit.broadcasts_per_hour` or `inherit.broadcast_max_members`
    /// (chapter 42, R9).
    InheritBroadcastLimit = 39244, "ER_SPRIG_INHERIT_BROADCAST_LIMIT", "SprigInheritBroadcastLimit", SQLSTATE_GENERAL, false;
    /// `DROP DATABASE` of a database whose cross-tenant outbox still holds
    /// committed rows after `inherit.outbox_drain_seconds` (chapter 42, R9).
    InheritOutboxPending = 39245, "ER_SPRIG_INHERIT_OUTBOX_PENDING", "SprigInheritOutboxPending", SQLSTATE_GENERAL, true;
    /// A request over a rate limit or admission control (chapter 37;
    /// analytics admission, chapter 35).
    RateLimited = 39780, "ER_SPRIG_RATE_LIMITED", "SprigRateLimited", SQLSTATE_GENERAL, true;
    /// A transaction handle names no open transaction on its gateway
    /// (chapter 37).
    TxnHandleUnknown = 39781, "ER_SPRIG_TXN_HANDLE_UNKNOWN", "SprigTxnHandleUnknown", SQLSTATE_GENERAL, false;
    /// The transaction behind a handle passed its idle or lifetime deadline
    /// and was rolled back (chapter 37).
    TxnHandleExpired = 39782, "ER_SPRIG_TXN_HANDLE_EXPIRED", "SprigTxnHandleExpired", SQLSTATE_GENERAL, false;
    /// A transaction handle belongs to another principal or tenant (chapter
    /// 37).
    TxnHandleForbidden = 39783, "ER_SPRIG_TXN_HANDLE_FORBIDDEN", "SprigTxnHandleForbidden", SQLSTATE_GENERAL, false;
    /// A buffered REST result went over `api.rest.max_buffered_bytes`; use
    /// NDJSON streaming (chapter 37).
    ResultTooLarge = 39785, "ER_SPRIG_RESULT_TOO_LARGE", "SprigResultTooLarge", SQLSTATE_GENERAL, false;
    /// The credential doesn't name one tenant (chapter 37).
    TenantAmbiguous = 39786, "ER_SPRIG_TENANT_AMBIGUOUS", "SprigTenantAmbiguous", SQLSTATE_GENERAL, false;
    /// A request body went over `api.max_request_bytes` (chapter 37).
    RequestTooLarge = 39787, "ER_SPRIG_REQUEST_TOO_LARGE", "SprigRequestTooLarge", SQLSTATE_GENERAL, false;
    /// A call couldn't be forwarded to the gateway owning its transaction
    /// handle (chapter 37).
    ForwardFailed = 39788, "ER_SPRIG_FORWARD_FAILED", "SprigForwardFailed", SQLSTATE_CONNECTION, true;
    /// A logic function called over HTTP or gRPC that isn't exposed to
    /// that protocol (chapter 37, "Logic functions as endpoints").
    FunctionNotExposed = 39789, "ER_SPRIG_FUNCTION_NOT_EXPOSED", "SprigFunctionNotExposed", SQLSTATE_GENERAL, false;
    /// gRPC over HTTP/1.1, or another transport the call can't use (chapter
    /// 37).
    TransportUnsupported = 39791, "ER_SPRIG_TRANSPORT_UNSUPPORTED", "SprigTransportUnsupported", SQLSTATE_GENERAL, false;
    /// An Admin API request is malformed or names something invalid
    /// (chapter 37). Not yet in Appendix E.
    AdminRequestInvalid = 39792, "ER_SPRIG_ADMIN_REQUEST_INVALID", "SprigAdminRequestInvalid", SQLSTATE_GENERAL, false;
    /// An Admin API request has no valid credential (chapter 37). Not yet
    /// in Appendix E.
    AdminUnauthenticated = 39793, "ER_SPRIG_ADMIN_UNAUTHENTICATED", "SprigAdminUnauthenticated", SQLSTATE_GENERAL, false;
    /// `Sql.Query`'s `language` is unknown, or a language other than SQL
    /// came with a transaction handle, `multi_statements` or a command it
    /// can't run (chapter 37, "Languages").
    QueryLanguageInvalid = 39794, "ER_SPRIG_QUERY_LANGUAGE_INVALID", "SprigQueryLanguageInvalid", SQLSTATE_GENERAL, false;
    /// A Cypher, GQL or MongoDB statement sent through `Sql.Query` failed
    /// with an error of that language that has no SprigDB or MySQL number;
    /// the message carries the language's own code (chapter 37).
    LanguageStatementFailed = 39795, "ER_SPRIG_LANGUAGE_STATEMENT_FAILED", "SprigLanguageStatementFailed", SQLSTATE_GENERAL, false;
    /// A change feed start is older than all history the feed can read
    /// (chapter 38).
    FeedHistoryLost = 39800, "ER_SPRIG_FEED_HISTORY_LOST", "SprigFeedHistoryLost", SQLSTATE_GENERAL, false;
    /// A resume token is malformed or belongs to another tenant (chapter 38).
    FeedBadResumeToken = 39801, "ER_SPRIG_FEED_BAD_RESUME_TOKEN", "SprigFeedBadResumeToken", SQLSTATE_GENERAL, false;
    /// A cluster's binlog settings fail the change feed's requirements
    /// (chapter 38, "Binlog requirements").
    FeedSourceUnsupported = 39802, "ER_SPRIG_FEED_SOURCE_UNSUPPORTED", "SprigFeedSourceUnsupported", SQLSTATE_GENERAL, false;
    /// A feed subscription by that name already exists (chapter 38).
    FeedSubscriptionExists = 39804, "ER_SPRIG_FEED_SUBSCRIPTION_EXISTS", "SprigFeedSubscriptionExists", SQLSTATE_GENERAL, false;
    /// No feed subscription by that name (chapter 38).
    FeedSubscriptionNotFound = 39805, "ER_SPRIG_FEED_SUBSCRIPTION_NOT_FOUND", "SprigFeedSubscriptionNotFound", SQLSTATE_GENERAL, false;
    /// An internal feed consumer is behind all history and must start from
    /// a snapshot (chapter 38).
    FeedSnapshotRequired = 39808, "ER_SPRIG_FEED_SNAPSHOT_REQUIRED", "SprigFeedSnapshotRequired", SQLSTATE_GENERAL, false;
    /// Statement-format DML in a shard schema: the feed can't decode it
    /// (chapter 38).
    FeedStatementEvent = 39810, "ER_SPRIG_FEED_STATEMENT_EVENT", "SprigFeedStatementEvent", SQLSTATE_GENERAL, false;
    /// The tenant's map moved to another cluster but the move's handoff
    /// record isn't readable yet; the feed waits (chapter 38).
    FeedHandoffPending = 39813, "ER_SPRIG_FEED_HANDOFF_PENDING", "SprigFeedHandoffPending", SQLSTATE_GENERAL, true;
    /// Unknown function, version or binding (chapter 41).
    LogicNotFound = 39860, "ER_SPRIG_LOGIC_NOT_FOUND", "SprigLogicNotFound", SQLSTATE_GENERAL, false;
    /// Parse or type error at deploy, with its location (chapter 41).
    LogicSyntax = 39861, "ER_SPRIG_LOGIC_SYNTAX", "SprigLogicSyntax", SQLSTATE_GENERAL, false;
    /// The code runs in no tier (chapter 41).
    LogicUnsupported = 39862, "ER_SPRIG_LOGIC_UNSUPPORTED", "SprigLogicUnsupported", SQLSTATE_GENERAL, false;
    /// A validation rejected the write (chapter 41); MongoDB 121.
    LogicValidationFailed = 39863, "ER_SPRIG_LOGIC_VALIDATION_FAILED", "SprigLogicValidationFailed", SQLSTATE_GENERAL, false;
    /// A budget ran out; the message names it (chapter 41).
    LogicBudgetExceeded = 39864, "ER_SPRIG_LOGIC_BUDGET_EXCEEDED", "SprigLogicBudgetExceeded", SQLSTATE_GENERAL, false;
    /// The code threw an error (chapter 41).
    LogicUserError = 39865, "ER_SPRIG_LOGIC_USER_ERROR", "SprigLogicUserError", SQLSTATE_GENERAL, false;
    /// Type error, overflow, division by zero, `sqlOne` with many rows
    /// (chapter 41).
    LogicRuntimeError = 39866, "ER_SPRIG_LOGIC_RUNTIME_ERROR", "SprigLogicRuntimeError", SQLSTATE_GENERAL, false;
    /// An outbound call to a host not on `logic.outbound_allowlist`, or
    /// to an internal address (chapter 41).
    LogicOutboundDenied = 39867, "ER_SPRIG_LOGIC_OUTBOUND_DENIED", "SprigLogicOutboundDenied", SQLSTATE_GENERAL, false;
    /// `SPRIG_TIER = 'mariadb'` but placement refuses (chapter 41).
    LogicTierUnavailable = 39868, "ER_SPRIG_LOGIC_TIER_UNAVAILABLE", "SprigLogicTierUnavailable", SQLSTATE_GENERAL, false;
    /// Concurrent deploys of one module (chapter 41).
    LogicVersionConflict = 39869, "ER_SPRIG_LOGIC_VERSION_CONFLICT", "SprigLogicVersionConflict", SQLSTATE_GENERAL, true;
    /// The logic engine's queue is full; retryable (chapter 41).
    LogicEngineBusy = 39871, "ER_SPRIG_LOGIC_ENGINE_BUSY", "SprigLogicEngineBusy", SQLSTATE_GENERAL, true;
    /// Nested `db.call` past `logic.max_call_depth` (chapter 41).
    LogicCallDepth = 39872, "ER_SPRIG_LOGIC_CALL_DEPTH", "SprigLogicCallDepth", SQLSTATE_GENERAL, false;
    /// Bad cron text or time zone of a schedule (chapter 41).
    LogicScheduleInvalid = 39873, "ER_SPRIG_LOGIC_SCHEDULE_INVALID", "SprigLogicScheduleInvalid", SQLSTATE_GENERAL, false;
    /// Arguments don't match the signature (chapter 41).
    LogicSignatureMismatch = 39874, "ER_SPRIG_LOGIC_SIGNATURE_MISMATCH", "SprigLogicSignatureMismatch", SQLSTATE_GENERAL, false;
    /// The function was placed in the engine tier, whose runtime this
    /// build doesn't have yet (chapter 41, as built).
    LogicEngineUnavailable = 39876, "ER_SPRIG_LOGIC_ENGINE_UNAVAILABLE", "SprigLogicEngineUnavailable", SQLSTATE_GENERAL, false;
    /// A trigger on a stream found its consumer group already past the
    /// record: another run of the trigger committed it (chapter 41, R9;
    /// the run is rolled back and skipped).
    LogicTriggerPositionMoved = 39877, "ER_SPRIG_LOGIC_TRIGGER_POSITION_MOVED", "SprigLogicTriggerPositionMoved", SQLSTATE_GENERAL, true;
    /// Studio is off, or not served on this listener (chapter 44).
    StudioDisabled = 39920, "ER_SPRIG_STUDIO_DISABLED", "SprigStudioDisabled", SQLSTATE_GENERAL, false;
    /// A plain-HTTP password login from a non-loopback client while
    /// `studio.allow_insecure_http` is off (chapter 44).
    StudioInsecureTransport = 39921, "ER_SPRIG_STUDIO_INSECURE_TRANSPORT", "SprigStudioInsecureTransport", SQLSTATE_GENERAL, false;
    /// A cookie request from another site: `Sec-Fetch-Site` or `Origin`
    /// (chapter 44).
    StudioOriginRejected = 39922, "ER_SPRIG_STUDIO_ORIGIN_REJECTED", "SprigStudioOriginRejected", SQLSTATE_GENERAL, false;
    /// A cookie request without `X-Sprig-CSRF` (chapter 44).
    StudioCsrfHeaderMissing = 39923, "ER_SPRIG_STUDIO_CSRF_HEADER_MISSING", "SprigStudioCsrfHeaderMissing", SQLSTATE_GENERAL, false;
    /// The token session was idle past `studio.session_idle_minutes`
    /// (chapter 44).
    StudioSessionIdle = 39924, "ER_SPRIG_STUDIO_SESSION_IDLE", "SprigStudioSessionIdle", SQLSTATE_GENERAL, false;
    /// The token session is older than `studio.session_max_hours`
    /// (chapter 44).
    StudioSessionExpired = 39925, "ER_SPRIG_STUDIO_SESSION_EXPIRED", "SprigStudioSessionExpired", SQLSTATE_GENERAL, false;
    /// Operator mode needs a session signed in with a second factor
    /// (`studio.operator_require_mfa`; chapter 44).
    StudioOperatorMfaRequired = 39926, "ER_SPRIG_STUDIO_OPERATOR_MFA_REQUIRED", "SprigStudioOperatorMfaRequired", SQLSTATE_GENERAL, false;
    /// Operator mode needs a verified client certificate
    /// (`studio.operator_mtls = required`; chapter 44).
    StudioOperatorCertRequired = 39927, "ER_SPRIG_STUDIO_OPERATOR_CERT_REQUIRED", "SprigStudioOperatorCertRequired", SQLSTATE_GENERAL, false;
    /// Operator mode is off (`studio.operator_mode`), or not open to this
    /// account (`studio.operator_logins`; chapter 44).
    StudioOperatorModeDisabled = 39928, "ER_SPRIG_STUDIO_OPERATOR_MODE_DISABLED", "SprigStudioOperatorModeDisabled", SQLSTATE_GENERAL, false;
    /// An export ticket is malformed, expired, or its session ended
    /// (chapter 44).
    StudioExportTicketInvalid = 39929, "ER_SPRIG_STUDIO_EXPORT_TICKET_INVALID", "SprigStudioExportTicketInvalid", SQLSTATE_GENERAL, false;
    /// The row-level security tester isn't allowed for this user: `studio.view_as`
    /// off, the caller not an administrator over the tenant, or the user an
    /// administrator over the caller's tenant (chapter 44, round 4).
    StudioViewAsDenied = 39930, "ER_SPRIG_STUDIO_VIEW_AS_DENIED", "SprigStudioViewAsDenied", SQLSTATE_GENERAL, false;
    /// The tester runs only one read-only statement (chapter 44, round 4).
    StudioViewAsNotReadOnly = 39931, "ER_SPRIG_STUDIO_VIEW_AS_NOT_READ_ONLY", "SprigStudioViewAsNotReadOnly", SQLSTATE_GENERAL, false;
    /// The binary was built without Studio's files (chapter 44).
    StudioNotBuilt = 39932, "ER_SPRIG_STUDIO_NOT_BUILT", "SprigStudioNotBuilt", SQLSTATE_GENERAL, false;
    /// No open cursor has this name, or it belongs to another user
    /// (chapter 44, decision ST30).
    StudioCursorUnknown = 39934, "ER_SPRIG_STUDIO_CURSOR_UNKNOWN", "SprigStudioCursorUnknown", SQLSTATE_GENERAL, false;
    /// The cursor was idle past `studio.cursor_idle_seconds` and closed;
    /// open it again (chapter 44, decision ST30).
    StudioCursorExpired = 39935, "ER_SPRIG_STUDIO_CURSOR_EXPIRED", "SprigStudioCursorExpired", SQLSTATE_GENERAL, false;
    /// Unknown stream or topic (chapter 45).
    StreamNotFound = 39940, "ER_SPRIG_STREAM_NOT_FOUND", "SprigStreamNotFound", SQLSTATE_GENERAL, false;
    /// `UPDATE` or `DELETE` on a stream; a produce to a change topic
    /// (chapter 45).
    StreamAppendOnly = 39941, "ER_SPRIG_STREAM_APPEND_ONLY", "SprigStreamAppendOnly", SQLSTATE_GENERAL, false;
    /// A stream name in use (chapter 45).
    StreamExists = 39942, "ER_SPRIG_STREAM_EXISTS", "SprigStreamExists", SQLSTATE_GENERAL, false;
    /// Partitions can only grow (chapter 45).
    StreamPartitionsDecrease = 39943, "ER_SPRIG_STREAM_PARTITIONS_DECREASE", "SprigStreamPartitionsDecrease", SQLSTATE_GENERAL, false;
    /// A record above `stream.max_message_bytes`, or a key above 4096
    /// bytes (chapter 45).
    StreamRecordTooLarge = 39944, "ER_SPRIG_STREAM_RECORD_TOO_LARGE", "SprigStreamRecordTooLarge", SQLSTATE_GENERAL, false;
    /// The event key doesn't determine the attached group's distribution
    /// key (chapter 45).
    StreamKeyNotAttached = 39945, "ER_SPRIG_STREAM_KEY_NOT_ATTACHED", "SprigStreamKeyNotAttached", SQLSTATE_GENERAL, false;
    /// No offset assigned within `stream.produce_wait_ms` (chapter 45).
    StreamSequencingTimeout = 39946, "ER_SPRIG_STREAM_SEQUENCING_TIMEOUT", "SprigStreamSequencingTimeout", SQLSTATE_GENERAL, true;
    /// An offset below the log start or above the high watermark
    /// (chapter 45).
    StreamOffsetOutOfRange = 39947, "ER_SPRIG_STREAM_OFFSET_OUT_OF_RANGE", "SprigStreamOffsetOutOfRange", SQLSTATE_GENERAL, false;
    /// A record without a key on a compacted stream (chapter 45).
    StreamCompactNeedsKey = 39955, "ER_SPRIG_STREAM_COMPACT_NEEDS_KEY", "SprigStreamCompactNeedsKey", SQLSTATE_GENERAL, false;
    /// `sprig.commit_offset` inside a transaction the gateway doesn't
    /// coordinate, with the tenant catalog on another cluster than the
    /// transaction's (chapter 45, R9).
    StreamOffsetTxnUnsupported = 39958, "ER_SPRIG_STREAM_OFFSET_TXN_UNSUPPORTED", "SprigStreamOffsetTxnUnsupported", SQLSTATE_GENERAL, false;
    /// Under `residency.cross_tenant_messaging_strict`, a publish from a
    /// pinned tenant into another tenant's stream outside its allowed data
    /// centers (chapter 45, R9).
    StreamResidency = 39949, "ER_SPRIG_STREAM_RESIDENCY", "SprigStreamResidency", SQLSTATE_GENERAL, false;
    /// A descendant's publish into an ancestor's stream with a key other
    /// than its own tenant name, under `stream.descendant_key = origin`
    /// (chapter 45, R9).
    StreamOriginKey = 39957, "ER_SPRIG_STREAM_ORIGIN_KEY", "SprigStreamOriginKey", SQLSTATE_GENERAL, false;
    /// A Cypher, GQL or graph DDL text that doesn't parse (chapter 43);
    /// Bolt `Neo.ClientError.Statement.SyntaxError`.
    GraphSyntax = 39900, "ER_SPRIG_GRAPH_SYNTAX", "SprigGraphSyntax", SQLSTATE_SYNTAX_ACCESS, false;
    /// A graph feature this build doesn't have; the message names it
    /// (chapter 43).
    GraphUnsupported = 39901, "ER_SPRIG_GRAPH_UNSUPPORTED", "SprigGraphUnsupported", SQLSTATE_GENERAL, false;
    /// No graph by that name (chapter 43); Bolt
    /// `Neo.ClientError.Database.DatabaseNotFound`.
    GraphNotFound = 39902, "ER_SPRIG_GRAPH_NOT_FOUND", "SprigGraphNotFound", SQLSTATE_GENERAL, false;
    /// A traversal went past a budget: hops, frontier, rows, time or memory;
    /// the message names it (chapter 43).
    GraphBudgetExceeded = 39903, "ER_SPRIG_GRAPH_BUDGET_EXCEEDED", "SprigGraphBudgetExceeded", SQLSTATE_GENERAL, false;
    /// A write to a mapped graph's read-only source, or to the labels of a
    /// mapped node (chapter 43).
    GraphReadOnlySource = 39904, "ER_SPRIG_GRAPH_READ_ONLY_SOURCE", "SprigGraphReadOnlySource", SQLSTATE_GENERAL, false;
    /// `id()` of an element whose id hashes its key can't be looked up:
    /// use `elementId()` (chapter 43).
    GraphIdNotRoutable = 39905, "ER_SPRIG_GRAPH_ID_NOT_ROUTABLE", "SprigGraphIdNotRoutable", SQLSTATE_GENERAL, false;
    /// `DELETE` of a node that still has relationships, checked at commit
    /// (chapter 43); Bolt `Neo.ClientError.Schema.ConstraintValidationFailed`.
    GraphNodeHasRelationships = 39906, "ER_SPRIG_GRAPH_NODE_HAS_RELATIONSHIPS", "SprigGraphNodeHasRelationships", SQLSTATE_GENERAL, false;
    /// `DETACH DELETE` past `graph.max_detach_edges` (chapter 43).
    GraphDetachTooLarge = 39907, "ER_SPRIG_GRAPH_DETACH_TOO_LARGE", "SprigGraphDetachTooLarge", SQLSTATE_GENERAL, false;
    /// A graph definition with a bad name, key or reference (chapter 43).
    GraphDefinitionInvalid = 39908, "ER_SPRIG_GRAPH_DEFINITION_INVALID", "SprigGraphDefinitionInvalid", SQLSTATE_GENERAL, false;
    /// A value that isn't a Neo4j property type, such as a map (chapter
    /// 43); Bolt `Neo.ClientError.Statement.TypeError`.
    GraphPropertyType = 39909, "ER_SPRIG_GRAPH_PROPERTY_TYPE", "SprigGraphPropertyType", SQLSTATE_GENERAL, false;
    /// No Bolt version in common with the client: the handshake answers
    /// `00 00 00 00` (chapter 43; logged only).
    BoltVersionUnsupported = 39912, "ER_SPRIG_BOLT_VERSION_UNSUPPORTED", "SprigBoltVersionUnsupported", SQLSTATE_CONNECTION, false;
    /// A Bolt message out of order or malformed (chapter 43); Bolt
    /// `Neo.ClientError.Request.Invalid`.
    BoltProtocol = 39914, "ER_SPRIG_BOLT_PROTOCOL", "SprigBoltProtocol", SQLSTATE_CONNECTION, false;
    /// A Bolt authentication scheme other than `basic` and `bearer`, or bad
    /// credentials (chapter 43); Bolt `Neo.ClientError.Security.Unauthorized`.
    BoltAuthScheme = 39915, "ER_SPRIG_BOLT_AUTH_SCHEME", "SprigBoltAuthScheme", SQLSTATE_SYNTAX_ACCESS, false;
    /// `imp_user` (impersonation) is refused (chapter 43); Bolt
    /// `Neo.ClientError.Security.Forbidden`.
    BoltImpersonation = 39916, "ER_SPRIG_BOLT_IMPERSONATION", "SprigBoltImpersonation", SQLSTATE_SYNTAX_ACCESS, false;
    /// A Bolt bookmark that doesn't parse, carries a bad signature or names
    /// another tenant (chapter 43); Bolt
    /// `Neo.ClientError.Transaction.InvalidBookmark`.
    BoltBookmarkInvalid = 39917, "ER_SPRIG_BOLT_BOOKMARK_INVALID", "SprigBoltBookmarkInvalid", SQLSTATE_GENERAL, false;
    /// A Bolt result kept unconsumed past `bolt.max_buffered_result_mb`
    /// (chapter 43).
    BoltResultBufferLimit = 39918, "ER_SPRIG_BOLT_RESULT_BUFFER_LIMIT", "SprigBoltResultBufferLimit", SQLSTATE_GENERAL, false;
    /// The audit spool is full (`audit.spool_max_mb`): admin API mutations
    /// and account statements fail closed (chapter 47).
    AuditUnavailable = 39970, "ER_SPRIG_AUDIT_UNAVAILABLE", "SprigAuditUnavailable", SQLSTATE_GENERAL, true;
    /// A tenant import's checksums or grants differ from its export's
    /// manifest (chapter 47).
    ImportVerifyFailed = 39971, "ER_SPRIG_IMPORT_VERIFY_FAILED", "SprigImportVerifyFailed", SQLSTATE_GENERAL, false;
    /// No recommendation with that id (chapter 52).
    RecommendationNotFound = 39972, "ER_SPRIG_RECOMMENDATION_NOT_FOUND", "SprigRecommendationNotFound", SQLSTATE_GENERAL, false;
    /// The action doesn't apply in the recommendation's state (chapter 52).
    RecommendationState = 39973, "ER_SPRIG_RECOMMENDATION_STATE", "SprigRecommendationState", SQLSTATE_GENERAL, false;
    /// The caller may not act at the recommendation's level (chapter 52).
    ApprovalDenied = 39974, "ER_SPRIG_APPROVAL_DENIED", "SprigApprovalDenied", SQLSTATE_SYNTAX_ACCESS, false;
    /// Re-validation failed before applying; the recommendation expired
    /// (chapter 52).
    RecommendationStale = 39975, "ER_SPRIG_RECOMMENDATION_STALE", "SprigRecommendationStale", SQLSTATE_GENERAL, false;
    /// An approval window that is empty, past or longer than 30 days
    /// (chapter 52).
    ApprovalWindowInvalid = 39976, "ER_SPRIG_APPROVAL_WINDOW_INVALID", "SprigApprovalWindowInvalid", SQLSTATE_GENERAL, false;
    /// A timer or kind list that doesn't parse (chapter 52).
    ApprovalPolicyInvalid = 39977, "ER_SPRIG_APPROVAL_POLICY_INVALID", "SprigApprovalPolicyInvalid", SQLSTATE_GENERAL, false;
    /// A test email or webhook failed (chapter 52).
    NotifyFailed = 39978, "ER_SPRIG_NOTIFY_FAILED", "SprigNotifyFailed", SQLSTATE_GENERAL, false;
    /// `sprigdb.toml` or its environment overrides are invalid. Raised only at
    /// process start; never reaches a client. Not yet in Appendix E.
    ConfigInvalid = 39998, "ER_SPRIG_CONFIG_INVALID", "SprigConfigInvalid", SQLSTATE_GENERAL, false;
    /// An internal invariant failed: a bug. Logged at error level with a
    /// trace id. Not yet in Appendix E.
    Internal = 39999, "ER_SPRIG_INTERNAL", "SprigInternal", SQLSTATE_GENERAL, false;
}

impl SprigError {
    /// A SprigDB error with the code's default retryability.
    pub fn new(code: SprigCode, message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Sprig(code),
            message: message.into(),
            retryable: code.retryable(),
            source: None,
        }
    }

    /// An internal-invariant error (`SprigCode::Internal`).
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(SprigCode::Internal, message)
    }

    /// An error MariaDB returned. 1213 and 1205 are retryable.
    pub fn backend(errno: u16, sqlstate: [u8; 5], message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Backend { errno, sqlstate },
            message: message.into(),
            retryable: is_retryable_errno(errno),
            source: None,
        }
    }

    /// A standard MySQL error raised by SprigDB itself.
    pub fn standard(errno: u16, sqlstate: [u8; 5], message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Standard { errno, sqlstate },
            message: message.into(),
            retryable: is_retryable_errno(errno),
            source: None,
        }
    }

    /// Attaches internal detail, which is logged and never sent to clients.
    #[must_use]
    pub fn with_source(mut self, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    /// The SprigDB code, if this is a SprigDB error.
    #[must_use]
    pub fn code(&self) -> Option<SprigCode> {
        match self.kind {
            ErrorKind::Sprig(code) => Some(code),
            _ => None,
        }
    }

    /// How this error shows up in each protocol (chapters 03, 30 and 37).
    #[must_use]
    pub fn protocol(&self) -> ProtocolError {
        match self.kind {
            ErrorKind::Sprig(code) => sprig_protocol(code),
            ErrorKind::Backend { errno, sqlstate } | ErrorKind::Standard { errno, sqlstate } => {
                mysql_protocol(errno, sqlstate)
            }
        }
    }
}

const fn is_retryable_errno(errno: u16) -> bool {
    matches!(errno, 1205 | 1213)
}

#[allow(clippy::too_many_lines)] // one arm per status group
fn sprig_protocol(code: SprigCode) -> ProtocolError {
    use tonic::Code;
    // Errors with a standard MongoDB equivalent use it (chapter 30).
    let (mongo_code, mongo_code_name, mongo_labels): (i32, &'static str, &'static [&str]) =
        match code {
            SprigCode::GatewayDraining => (91, "ShutdownInProgress", &["RetryableWriteError"]),
            SprigCode::DcFenced | SprigCode::RecoveryFenced => {
                (10107, "NotWritablePrimary", &["RetryableWriteError"])
            }
            SprigCode::LogicValidationFailed => (121, "DocumentValidationFailure", &[]),
            _ => (i32::from(code.number()), code.mongo_code_name(), &[]),
        };
    // gRPC and HTTP follow chapter 37's table.
    let (grpc_status, http_status) = match code {
        SprigCode::SettingOutOfBounds
        | SprigCode::TypeMultipleInheritance
        | SprigCode::TypeFieldConflict
        | SprigCode::TypeAbstract
        | SprigCode::TypeColumnReadOnly
        | SprigCode::TypeKeyUpdate
        | SprigCode::TypePathTooLong
        | SprigCode::TypeUnsupported
        | SprigCode::HierarchyParentNotFound
        | SprigCode::HierarchyTooDeep
        | SprigCode::SettingUnknown
        | SprigCode::SettingScopeNotAllowed
        | SprigCode::SettingBadValue
        | SprigCode::AdminRequestInvalid
        | SprigCode::QueryLanguageInvalid
        | SprigCode::LanguageStatementFailed
        | SprigCode::LogicSyntax
        | SprigCode::GraphSyntax
        | SprigCode::GraphDefinitionInvalid
        | SprigCode::GraphPropertyType
        | SprigCode::BoltProtocol
        | SprigCode::BoltBookmarkInvalid
        | SprigCode::LogicSignatureMismatch
        | SprigCode::LogicScheduleInvalid
        | SprigCode::LogicValidationFailed
        | SprigCode::FeedBadResumeToken
        | SprigCode::InheritKeyUpdate
        | SprigCode::ShareFilterInvalid
        | SprigCode::ResidencyInvalid
        | SprigCode::StreamPartitionsDecrease
        | SprigCode::StreamKeyNotAttached
        | SprigCode::StreamCompactNeedsKey
        | SprigCode::InheritTargetTenant
        | SprigCode::StreamOriginKey
        | SprigCode::SinkCredentialsInvalid
        | SprigCode::SinkOptionInvalid
        | SprigCode::SinkTransformInvalid
        | SprigCode::SinkFormatUnsupported
        | SprigCode::KvInvalidKey
        | SprigCode::KvValueInvalid
        | SprigCode::KvElementTooLong
        | SprigCode::VectorDimensionMismatch
        | SprigCode::VectorMetricMismatch
        | SprigCode::VectorLimitInvalid
        | SprigCode::VectorFilterNotIndexed
        | SprigCode::VectorTooManyIndexes
        | SprigCode::DimUnknownMember
        | SprigCode::DimInvalidModel
        | SprigCode::DimTzUnavailable
        | SprigCode::GeoInvalid
        | SprigCode::GeoSridUnsupported
        | SprigCode::GeoUnsupportedExpression
        | SprigCode::GeoTooComplex
        | SprigCode::ApprovalWindowInvalid
        | SprigCode::StudioViewAsNotReadOnly
        | SprigCode::ApprovalPolicyInvalid => (Code::InvalidArgument, 400),
        SprigCode::SettingLocked
        | SprigCode::BinlogUnsupported
        | SprigCode::FeedSourceUnsupported
        | SprigCode::FeedStatementEvent
        | SprigCode::ClusterCapabilityRequired
        | SprigCode::ProvisionerUnsupported
        | SprigCode::ClusterPrecheckFailed
        | SprigCode::LocalInfileRejected
        | SprigCode::LongDataLimit
        | SprigCode::ReservedName
        | SprigCode::NameRewriteUnsupported
        | SprigCode::MetadataTooLarge
        | SprigCode::ResultTooLarge
        | SprigCode::StatementNotDistributable
        | SprigCode::UnsupportedKeyType
        | SprigCode::UnsupportedKeyCollation
        | SprigCode::KeyValueUnroutable
        | SprigCode::SplitNotAllowed
        | SprigCode::ShardedTenantMove
        | SprigCode::PinConflict
        | SprigCode::PinNeedsRekey
        | SprigCode::IdValueReserved
        | SprigCode::DdlUnsupported
        | SprigCode::DdlShardDrift
        | SprigCode::FkActionNotDistributed
        | SprigCode::HeldBranchStatement => (Code::FailedPrecondition, 400),
        SprigCode::GatewayDraining
        | SprigCode::ListenerRetired
        | SprigCode::MetaUnavailable
        | SprigCode::NotLeader
        | SprigCode::BinlogDisconnected
        | SprigCode::ClusterUnavailable
        | SprigCode::ClusterFenced
        | SprigCode::BackendUnavailable
        | SprigCode::BackendPoolTimeout
        | SprigCode::BackendLost
        | SprigCode::CatalogUnavailable
        | SprigCode::AuthUnavailable
        | SprigCode::AuditUnavailable
        | SprigCode::ForwardFailed
        | SprigCode::JobLeaseLost
        | SprigCode::SessionMoved
        | SprigCode::DcFenced
        | SprigCode::RecoveryFenced
        | SprigCode::AdmissionBackoff
        | SprigCode::FeedHandoffPending
        | SprigCode::ShardMapUnavailable
        | SprigCode::IdBlockUnavailable
        | SprigCode::LookupUnavailable
        | SprigCode::MoveSourceDiverged
        | SprigCode::InheritCopyStale
        | SprigCode::InheritFederatedUnavailable
        | SprigCode::InheritInstanceUnavailable
        | SprigCode::LogicEngineBusy
        | SprigCode::StreamSequencingTimeout
        | SprigCode::SinkDeliveryFailed
        | SprigCode::SinkPlatformSecretUnavailable
        | SprigCode::StudioNotBuilt
        | SprigCode::NotifyFailed => (Code::Unavailable, 503),
        SprigCode::CatalogConflict
        | SprigCode::TenantTreeBusy
        | SprigCode::TxnHandleUnknown
        | SprigCode::TxnHandleExpired
        | SprigCode::StudioCursorExpired
        | SprigCode::ClusterFenceFailed
        | SprigCode::OutcomeUnknown
        | SprigCode::SessionStateLost
        | SprigCode::ShardMoved
        | SprigCode::ShardLocked
        | SprigCode::ShardBusy
        | SprigCode::LogicUserError
        | SprigCode::LogicRuntimeError
        | SprigCode::LogicCallDepth
        | SprigCode::LogicVersionConflict
        | SprigCode::MoveCutoverAborted
        | SprigCode::ShardMapChanged
        | SprigCode::DdlFailed
        | SprigCode::SinkScriptFailed
        | SprigCode::SinkFenced => (Code::Aborted, 409),
        SprigCode::CrossClusterWriteRejected | SprigCode::TooManyBranches => {
            (Code::FailedPrecondition, 400)
        }
        SprigCode::CommitOutcomeUnknown | SprigCode::DtxRolledBack | SprigCode::PartialCommit => {
            (Code::Aborted, 409)
        }
        SprigCode::CatalogFormatTooNew => (Code::FailedPrecondition, 500),
        SprigCode::BinlogCorrupt | SprigCode::MoveVerifyFailed | SprigCode::ImportVerifyFailed => {
            (Code::DataLoss, 500)
        }
        SprigCode::BackendProtocol => (Code::Internal, 502),
        SprigCode::ClusterNotFound
        | SprigCode::UnknownDc
        | SprigCode::AutoIndexNotFound
        | SprigCode::FeedSubscriptionNotFound
        | SprigCode::GraphNotFound
        | SprigCode::LogicNotFound
        | SprigCode::FunctionNotExposed
        | SprigCode::JobNotFound
        | SprigCode::TxnNotInDoubt
        | SprigCode::SpaceNotFound
        | SprigCode::ShareNotFound
        | SprigCode::StreamNotFound
        | SprigCode::SinkNotFound
        | SprigCode::KvNamespaceNotFound
        | SprigCode::VectorIndexNotFound
        | SprigCode::DimModelNotFound
        | SprigCode::StudioDisabled
        | SprigCode::StudioCursorUnknown
        | SprigCode::RecommendationNotFound => (Code::NotFound, 404),
        SprigCode::ClusterExists
        | SprigCode::TenantExists
        | SprigCode::FeedSubscriptionExists
        | SprigCode::SpaceExists
        | SprigCode::StreamExists
        | SprigCode::SinkExists
        | SprigCode::KvKeyExists => (Code::AlreadyExists, 409),
        SprigCode::ClusterStateInvalid
        | SprigCode::KvVersionMismatch
        | SprigCode::KvWrongType
        | SprigCode::DimNotDecomposable
        | SprigCode::DimVersionedTruncate
        | SprigCode::DimHistoryPurged
        | SprigCode::TypeHasDependents
        | SprigCode::HierarchyCycle
        | SprigCode::HierarchyHasChildren
        | SprigCode::SubtreeTooLarge
        | SprigCode::HierarchyBackfillFailed
        | SprigCode::AutoIndexStateInvalid
        | SprigCode::TenantNotReady
        | SprigCode::TenantHasChildren
        | SprigCode::TenantMoveBlocked
        | SprigCode::TenantDepthExceeded
        | SprigCode::MoveTargetIncompatible
        | SprigCode::MoveUnsupported
        | SprigCode::TenantMoveRefused
        | SprigCode::FailoverRefused
        | SprigCode::ResidencyUnsatisfiable
        | SprigCode::FailoverBlocked
        | SprigCode::FeedHistoryLost
        | SprigCode::FeedSnapshotRequired
        | SprigCode::LogicUnsupported
        | SprigCode::LogicTierUnavailable
        | SprigCode::LogicEngineUnavailable
        | SprigCode::GraphUnsupported
        | SprigCode::GraphNodeHasRelationships
        | SprigCode::GraphDetachTooLarge
        | SprigCode::BoltVersionUnsupported
        | SprigCode::JobStateInvalid
        | SprigCode::SpaceHasChildren
        | SprigCode::SpaceDepthExceeded
        | SprigCode::NotInheritable
        | SprigCode::ShareResidencyConflict
        | SprigCode::ShareNotResharable
        | SprigCode::InheritStatementUnsupported
        | SprigCode::InheritSchemaMismatch
        | SprigCode::InheritedObjectInUse
        | SprigCode::InheritNotOverridable
        | SprigCode::InheritDefinitionDropping
        | SprigCode::InheritConfigMissing
        | SprigCode::InheritRunAsPrivilege
        | SprigCode::InheritOutboxPending
        | SprigCode::AuthorizerUnknownToGateways
        | SprigCode::StreamAppendOnly
        | SprigCode::LogicTriggerPositionMoved
        | SprigCode::StreamOffsetTxnUnsupported
        | SprigCode::SinkConnectionInUse
        | SprigCode::SinkExactlyOnceUnsupported
        | SprigCode::SinkPaused
        | SprigCode::SinkSourceLost
        | SprigCode::SinkSchemaMismatch
        | SprigCode::SinkDisabled
        | SprigCode::GraphReadOnlySource
        | SprigCode::GraphIdNotRoutable
        | SprigCode::SinkState
        | SprigCode::RecommendationState
        | SprigCode::RecommendationStale => (Code::FailedPrecondition, 409),
        SprigCode::TenantSuspended
        | SprigCode::RemoteAccessDenied
        | SprigCode::DefinerRefused
        | SprigCode::LogicOutboundDenied
        | SprigCode::BoltImpersonation
        | SprigCode::RlsUnsupported
        | SprigCode::TxnHandleForbidden
        | SprigCode::SpaceAccessDenied
        | SprigCode::SinkPrivilege
        | SprigCode::SinkDestinationNotAllowed
        | SprigCode::SinkResidency
        | SprigCode::SinkPlatformIdentityDenied
        | SprigCode::SinkOwnerPrivilegeLost
        | SprigCode::SinkPlatformConnectionNotGranted
        | SprigCode::SinkScriptReadOnly
        | SprigCode::InheritResidency
        | SprigCode::StreamResidency
        | SprigCode::StudioInsecureTransport
        | SprigCode::StudioOriginRejected
        | SprigCode::StudioCsrfHeaderMissing
        | SprigCode::MfaRequired
        | SprigCode::ImpersonationDenied
        | SprigCode::StudioOperatorMfaRequired
        | SprigCode::StudioOperatorCertRequired
        | SprigCode::StudioOperatorModeDisabled
        | SprigCode::StudioViewAsDenied
        | SprigCode::ApprovalDenied => (Code::PermissionDenied, 403),
        SprigCode::LoginRateLimited
        | SprigCode::TenantConnectionLimit
        | SprigCode::AdmissionTimeout
        | SprigCode::MemoryLimit
        | SprigCode::LogicBudgetExceeded
        | SprigCode::GraphBudgetExceeded
        | SprigCode::BoltResultBufferLimit
        | SprigCode::RouterMemoryLimit
        | SprigCode::PlanLimitExceeded
        | SprigCode::SinkQuota
        | SprigCode::InheritInstanceQuota
        | SprigCode::InheritBroadcastLimit
        | SprigCode::DimTooManyGroups
        | SprigCode::RateLimited => (Code::ResourceExhausted, 429),
        SprigCode::PlacementUnavailable | SprigCode::ProvisionNoHosts | SprigCode::NoDcCapacity => {
            (Code::ResourceExhausted, 503)
        }
        SprigCode::RequestTooLarge
        | SprigCode::StreamRecordTooLarge
        | SprigCode::SinkRecordTooLarge => (Code::InvalidArgument, 413),
        SprigCode::StreamOffsetOutOfRange => (Code::OutOfRange, 416),
        SprigCode::TransportUnsupported => (Code::FailedPrecondition, 415),
        SprigCode::AdminUnauthenticated
        | SprigCode::BoltAuthScheme
        | SprigCode::SessionRevoked
        | SprigCode::TenantAmbiguous
        | SprigCode::StudioSessionIdle
        | SprigCode::StudioSessionExpired
        | SprigCode::StudioExportTicketInvalid => (Code::Unauthenticated, 401),
        SprigCode::ConfigInvalid | SprigCode::Internal => (Code::Internal, 500),
    };
    ProtocolError {
        mysql_errno: code.number(),
        sqlstate: code.sqlstate(),
        mongo_code,
        mongo_code_name,
        mongo_labels,
        grpc_status,
        http_status,
    }
}

fn mysql_protocol(errno: u16, sqlstate: [u8; 5]) -> ProtocolError {
    use tonic::Code;
    // MongoDB codes from chapter 30, gRPC and HTTP from chapter 37. Other
    // MariaDB errors map to MongoDB's generic `UnknownError` (code 8) until
    // chapter 30's front end refines them.
    let (mongo_code, mongo_code_name, mongo_labels): (i32, &'static str, &'static [&str]) =
        match errno {
            1062 => (11000, "DuplicateKey", &[]),
            1205 | 1213 => (112, "WriteConflict", &[]),
            _ => (8, "UnknownError", &[]),
        };
    let (grpc_status, http_status) = match errno {
        1205 | 1213 => (Code::Aborted, 409),
        1062 => (Code::AlreadyExists, 409),
        1049 | 1146 => (Code::NotFound, 404),
        1044 | 1142 | 1143 | 1227 => (Code::PermissionDenied, 403),
        1045 => (Code::Unauthenticated, 401),
        1064 => (Code::InvalidArgument, 400),
        _ if sqlstate == SQLSTATE_RETRY => (Code::Aborted, 409),
        _ if sqlstate.starts_with(b"08") => (Code::Unavailable, 503),
        _ => (Code::Unknown, 500),
    };
    ProtocolError {
        mysql_errno: errno,
        sqlstate,
        mongo_code,
        mongo_code_name,
        mongo_labels,
        grpc_status,
        http_status,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_are_in_range_unique_and_well_named() {
        let mut seen = std::collections::HashSet::new();
        for &code in SprigCode::ALL {
            assert!((39000..=39999).contains(&code.number()), "{code:?}");
            assert!(seen.insert(code.number()), "duplicate number {code:?}");
            assert!(code.name().starts_with("ER_SPRIG_"), "{code:?}");
            assert!(code.mongo_code_name().starts_with("Sprig"), "{code:?}");
        }
    }

    #[test]
    fn setting_error_renders_everywhere() {
        let e = SprigError::new(
            SprigCode::SettingOutOfBounds,
            "auth.failed_login_attempts: 12 > 10",
        );
        let p = e.protocol();
        assert_eq!(p.mysql_errno, 39080);
        assert_eq!(&p.sqlstate, b"HY000");
        assert_eq!(p.mongo_code, 39080);
        assert_eq!(p.mongo_code_name, "SprigSettingOutOfBounds");
        assert_eq!(p.grpc_status, tonic::Code::InvalidArgument);
        assert_eq!(p.http_status, 400);
        assert!(!e.retryable);
        assert_eq!(
            e.to_string(),
            "39080 ER_SPRIG_SETTING_OUT_OF_BOUNDS: auth.failed_login_attempts: 12 > 10"
        );
    }

    #[test]
    fn deadlock_reuses_standard_retry_signals() {
        let e = SprigError::standard(
            1213,
            *b"40001",
            "Deadlock found; try restarting transaction",
        );
        assert!(e.retryable);
        let p = e.protocol();
        assert_eq!(p.mysql_errno, 1213);
        assert_eq!(p.mongo_code, 112);
        assert_eq!(p.grpc_status, tonic::Code::Aborted);
        assert_eq!(p.http_status, 409);
    }

    #[test]
    fn draining_maps_to_mongo_shutdown_in_progress() {
        let p = SprigError::new(SprigCode::GatewayDraining, "draining").protocol();
        assert_eq!(p.mongo_code, 91);
        assert_eq!(p.mongo_labels, &["RetryableWriteError"]);
        assert_eq!(&p.sqlstate, b"08S01");
    }

    #[test]
    fn source_is_kept_but_not_displayed() {
        let io = std::io::Error::other("t42_d1_g3_s17 missing");
        let e = SprigError::internal("table missing").with_source(io);
        assert!(!e.to_string().contains("t42_"));
        assert!(std::error::Error::source(&e).is_some());
    }
}
