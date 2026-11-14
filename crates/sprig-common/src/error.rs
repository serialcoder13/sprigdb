//! The one error type, [`SprigError`], and how each error shows up in every
//! protocol.
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
    /// MongoDB front end.
    pub mongo_labels: &'static [&'static str],
    /// gRPC status code.
    pub grpc_status: tonic::Code,
    /// HTTP status for REST.
    pub http_status: u16,
}

const SQLSTATE_GENERAL: [u8; 5] = *b"HY000";
const SQLSTATE_RETRY: [u8; 5] = *b"40001";
const SQLSTATE_CONNECTION: [u8; 5] = *b"08S01";
/// Syntax error or access rule violation (also used for reserved names).
const SQLSTATE_SYNTAX_ACCESS: [u8; 5] = *b"42000";

macro_rules! sprig_codes {
    ($(
        $(#[$doc:meta])*
        $variant:ident = $num:literal, $name:literal, $mongo:literal, $sqlstate:expr, $retryable:literal;
    )*) => {
        /// SprigDB's own error codes. Only codes that code in this workspace
        /// raises are listed; each feature adds its own as it is implemented.
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
    /// The gateway is draining.
    GatewayDraining = 39000, "ER_SPRIG_GATEWAY_DRAINING", "SprigGatewayDraining", SQLSTATE_CONNECTION, true;
    /// The tenant has `limits.tenant_connections` sessions on this gateway
    /// already.
    TenantConnectionLimit = 39001, "ER_SPRIG_TENANT_CONNECTION_LIMIT", "SprigTenantConnectionLimit", *b"08004", true;
    /// A statement waited longer than `limits.queue_timeout_ms` for an
    /// admission permit.
    AdmissionTimeout = 39002, "ER_SPRIG_ADMISSION_TIMEOUT", "SprigAdmissionTimeout", SQLSTATE_GENERAL, true;
    /// A memory charge failed: the gateway, tenant or user budget is spent.
    MemoryLimit = 39003, "ER_SPRIG_MEMORY_LIMIT", "SprigMemoryLimit", SQLSTATE_GENERAL, true;
    /// The session is closing at a safe point because the listener it
    /// arrived on moved, was disabled or tightened.
    ListenerRetired = 39004, "ER_SPRIG_LISTENER_RETIRED", "SprigListenerRetired", SQLSTATE_CONNECTION, true;
    /// The backend asked for a `LOAD DATA LOCAL INFILE` file the statement
    /// didn't name; the load was aborted.
    LocalInfileRejected = 39010, "ER_SPRIG_LOCAL_INFILE_REJECTED", "SprigLocalInfileRejected", SQLSTATE_GENERAL, false;
    /// Buffered `COM_STMT_SEND_LONG_DATA` for a statement went over
    /// `gateway.long_data_max_bytes`.
    LongDataLimit = 39011, "ER_SPRIG_LONG_DATA_LIMIT", "SprigLongDataLimit", SQLSTATE_GENERAL, false;
    /// The backend sent a packet the protocol doesn't allow at that point.
    BackendProtocol = 39012, "ER_SPRIG_BACKEND_PROTOCOL", "SprigBackendProtocol", SQLSTATE_CONNECTION, false;
    /// No backend node could be reached before the deadline.
    BackendUnavailable = 39030, "ER_SPRIG_BACKEND_UNAVAILABLE", "SprigBackendUnavailable", SQLSTATE_CONNECTION, true;
    /// No pooled backend connection became free before the deadline.
    BackendPoolTimeout = 39031, "ER_SPRIG_BACKEND_POOL_TIMEOUT", "SprigBackendPoolTimeout", SQLSTATE_GENERAL, true;
    /// The backend was lost during a commit or an autocommit write: the
    /// outcome is unknown.
    OutcomeUnknown = 39032, "ER_SPRIG_OUTCOME_UNKNOWN", "SprigOutcomeUnknown", SQLSTATE_CONNECTION, false;
    /// The backend holding state that can't be replayed was lost.
    SessionStateLost = 39033, "ER_SPRIG_SESSION_STATE_LOST", "SprigSessionStateLost", SQLSTATE_CONNECTION, false;
    /// The backend was lost after part of a result was sent.
    BackendLost = 39034, "ER_SPRIG_BACKEND_LOST", "SprigBackendLost", SQLSTATE_CONNECTION, false;
    /// A statement creates or names a reserved name: `__sprig_` objects,
    /// `_sprig_` columns, `t<digits>_` or system databases.
    ReservedName = 39050, "ER_SPRIG_RESERVED_NAME", "SprigReservedName", SQLSTATE_SYNTAX_ACCESS, false;
    /// A statement holds a name position the rewriter can't map safely.
    NameRewriteUnsupported = 39051, "ER_SPRIG_NAME_REWRITE_UNSUPPORTED", "SprigNameRewriteUnsupported", SQLSTATE_GENERAL, false;
    /// A SprigDB `information_schema` view would inline more rows than
    /// `names.local_view_max_rows`: narrow it by schema or
    /// table.
    MetadataTooLarge = 39052, "ER_SPRIG_METADATA_TOO_LARGE", "SprigMetadataTooLarge", SQLSTATE_GENERAL, false;
    /// The tenant's catalog writer or the global tier can't be reached, so
    /// DDL, setting and account changes fail; cached reads continue.
    CatalogUnavailable = 39070, "ER_SPRIG_CATALOG_UNAVAILABLE", "SprigCatalogUnavailable", SQLSTATE_CONNECTION, true;
    /// A catalog change met a concurrent one: a pending DDL job on the
    /// object, a busy version row or a held migration lock.
    CatalogConflict = 39071, "ER_SPRIG_CATALOG_CONFLICT", "SprigCatalogConflict", SQLSTATE_RETRY, true;
    /// The tenant is not active or its catalog is not bootstrapped yet.
    TenantNotReady = 39072, "ER_SPRIG_TENANT_NOT_READY", "SprigTenantNotReady", SQLSTATE_GENERAL, false;
    /// The tenant's catalog has a format newer than this binary reads.
    CatalogFormatTooNew = 39073, "ER_SPRIG_CATALOG_FORMAT_TOO_NEW", "SprigCatalogFormatTooNew", SQLSTATE_GENERAL, false;
    /// A setting value lies outside the bounds in force.
    SettingOutOfBounds = 39080, "ER_SPRIG_SETTING_OUT_OF_BOUNDS", "SprigSettingOutOfBounds", SQLSTATE_GENERAL, false;
    /// The setting key is not in the registry.
    SettingUnknown = 39081, "ER_SPRIG_SETTING_UNKNOWN", "SprigSettingUnknown", SQLSTATE_GENERAL, false;
    /// The key can't be set at this scope.
    SettingScopeNotAllowed = 39082, "ER_SPRIG_SETTING_SCOPE_NOT_ALLOWED", "SprigSettingScopeNotAllowed", SQLSTATE_GENERAL, false;
    /// A wider scope locked the key.
    SettingLocked = 39083, "ER_SPRIG_SETTING_LOCKED", "SprigSettingLocked", SQLSTATE_GENERAL, false;
    /// The value doesn't parse as the key's type.
    SettingBadValue = 39084, "ER_SPRIG_SETTING_BAD_VALUE", "SprigSettingBadValue", SQLSTATE_GENERAL, false;
    /// The cluster has no writer for the shard.
    ClusterUnavailable = 39100, "ER_SPRIG_CLUSTER_UNAVAILABLE", "SprigClusterUnavailable", SQLSTATE_CONNECTION, true;
    /// A managed cluster fenced itself.
    ClusterFenced = 39101, "ER_SPRIG_CLUSTER_FENCED", "SprigClusterFenced", SQLSTATE_GENERAL, false;
    /// A statement needs a capability a cluster holding the object lacks,
    /// with no fallback.
    ClusterCapabilityRequired = 39102, "ER_SPRIG_CLUSTER_CAPABILITY_REQUIRED", "SprigClusterCapabilityRequired", SQLSTATE_GENERAL, false;
    /// A shard fence couldn't be completed (killed sessions didn't stop in
    /// time, or a rename waited too long); it was rolled back.
    ClusterFenceFailed = 39103, "ER_SPRIG_CLUSTER_FENCE_FAILED", "SprigClusterFenceFailed", SQLSTATE_GENERAL, true;
    /// No cluster has this id in the data center (Admin API).
    ClusterNotFound = 39104, "ER_SPRIG_CLUSTER_NOT_FOUND", "SprigClusterNotFound", SQLSTATE_GENERAL, false;
    /// A cluster with this name already exists in the data center (Admin
    /// API).
    ClusterExists = 39105, "ER_SPRIG_CLUSTER_EXISTS", "SprigClusterExists", SQLSTATE_GENERAL, false;
    /// The cluster's admin state doesn't allow the request, such as
    /// draining a cluster that isn't active.
    ClusterStateInvalid = 39106, "ER_SPRIG_CLUSTER_STATE_INVALID", "SprigClusterStateInvalid", SQLSTATE_GENERAL, false;
    /// A mover cutover moved the shard while a transaction was open: the
    /// gateway rolled it back (sent inside a 1213).
    ShardMoved = 39110, "ER_SPRIG_SHARD_MOVED", "SprigShardMoved", SQLSTATE_RETRY, true;
    /// The move's target cluster lacks a capability the shard uses.
    MoveTargetIncompatible = 39111, "ER_SPRIG_MOVE_TARGET_INCOMPATIBLE", "SprigMoveTargetIncompatible", SQLSTATE_GENERAL, false;
    /// A binlog event failed its checksum or doesn't parse.
    BinlogCorrupt = 39112, "ER_SPRIG_BINLOG_CORRUPT", "SprigBinlogCorrupt", SQLSTATE_GENERAL, false;
    /// The binlog stream to a MariaDB node broke or went silent; reconnect
    /// from the last committed position.
    BinlogDisconnected = 39113, "ER_SPRIG_BINLOG_DISCONNECTED", "SprigBinlogDisconnected", SQLSTATE_CONNECTION, true;
    /// The server or binlog uses something the reader can't handle, such as
    /// an unknown checksum algorithm, an encrypted binlog file or an
    /// authentication plugin it lacks.
    BinlogUnsupported = 39114, "ER_SPRIG_BINLOG_UNSUPPORTED", "SprigBinlogUnsupported", SQLSTATE_GENERAL, false;
    /// No mover job by that id in the data center.
    JobNotFound = 39130, "ER_SPRIG_JOB_NOT_FOUND", "SprigJobNotFound", SQLSTATE_GENERAL, false;
    /// Another job holds a lock on the shard.
    ShardLocked = 39131, "ER_SPRIG_SHARD_LOCKED", "SprigShardLocked", SQLSTATE_GENERAL, true;
    /// The job's state doesn't allow the request, such as cancelling a job
    /// after its cutover committed.
    JobStateInvalid = 39132, "ER_SPRIG_JOB_STATE_INVALID", "SprigJobStateInvalid", SQLSTATE_GENERAL, false;
    /// The worker lost the job: its lease ended or a newer owner raised
    /// the fencing token.
    JobLeaseLost = 39133, "ER_SPRIG_JOB_LEASE_LOST", "SprigJobLeaseLost", SQLSTATE_GENERAL, true;
    /// The source's binlog no longer holds the position the job needs
    /// (purged, or a failover lost transactions the target already has):
    /// the copy restarts.
    MoveSourceDiverged = 39134, "ER_SPRIG_MOVE_SOURCE_DIVERGED", "SprigMoveSourceDiverged", SQLSTATE_GENERAL, true;
    /// A cutover attempt was aborted (over budget, fence failed, commit
    /// rejected); the sources stay authoritative and the job retries.
    MoveCutoverAborted = 39135, "ER_SPRIG_MOVE_CUTOVER_ABORTED", "SprigMoveCutoverAborted", SQLSTATE_GENERAL, true;
    /// Verification found the target different from the source after
    /// `mover.verify_max_repairs` repairs.
    MoveVerifyFailed = 39136, "ER_SPRIG_MOVE_VERIFY_FAILED", "SprigMoveVerifyFailed", SQLSTATE_GENERAL, false;
    /// The source changed in a way the mover can't follow: statement-based
    /// DML in a moved schema, a table without a primary key, or DDL during
    /// a cutover.
    MoveUnsupported = 39137, "ER_SPRIG_MOVE_UNSUPPORTED", "SprigMoveUnsupported", SQLSTATE_GENERAL, false;
    /// No cluster can take the placement (including tenant bootstrap's
    /// placement).
    PlacementUnavailable = 39120, "ER_SPRIG_PLACEMENT_UNAVAILABLE", "SprigPlacementUnavailable", SQLSTATE_GENERAL, true;
    /// No automatic index (or recommendation) by that id or name in the
    /// tenant (`sprigctl index`).
    AutoIndexNotFound = 39201, "ER_SPRIG_AUTO_INDEX_NOT_FOUND", "SprigAutoIndexNotFound", SQLSTATE_GENERAL, false;
    /// The automatic index's state doesn't allow the action (approve a
    /// non-recommendation, drop a pinned one, ...), or it changed meanwhile.
    AutoIndexStateInvalid = 39202, "ER_SPRIG_AUTO_INDEX_STATE_INVALID", "SprigAutoIndexStateInvalid", SQLSTATE_GENERAL, false;
    /// Unknown sink or connection.
    SinkNotFound = 39210, "ER_SPRIG_SINK_NOT_FOUND", "SprigSinkNotFound", SQLSTATE_GENERAL, false;
    /// A sink or connection name in use.
    SinkExists = 39211, "ER_SPRIG_SINK_EXISTS", "SprigSinkExists", SQLSTATE_GENERAL, false;
    /// Creating or altering sinks or connections without `SPRIG_SINK_ADMIN`, naming a connection without `USAGE`, or a fan-in sink without `SPRIG_FAN_IN`.
    SinkPrivilege = 39212, "ER_SPRIG_SINK_PRIVILEGE", "SprigSinkPrivilege", SQLSTATE_SYNTAX_ACCESS, false;
    /// A destination not on `sink.destination_allowlist`, or an internal address not listed literally.
    SinkDestinationNotAllowed = 39213, "ER_SPRIG_SINK_DESTINATION_NOT_ALLOWED", "SprigSinkDestinationNotAllowed", SQLSTATE_GENERAL, false;
    /// The destination's tags don't satisfy the tenant's residency policy.
    SinkResidency = 39214, "ER_SPRIG_SINK_RESIDENCY", "SprigSinkResidency", SQLSTATE_GENERAL, false;
    /// Dropping a connection that sinks use.
    SinkConnectionInUse = 39215, "ER_SPRIG_SINK_CONNECTION_IN_USE", "SprigSinkConnectionInUse", SQLSTATE_GENERAL, false;
    /// A credential the connection type doesn't take, or one it needs is missing.
    SinkCredentialsInvalid = 39216, "ER_SPRIG_SINK_CREDENTIALS_INVALID", "SprigSinkCredentialsInvalid", SQLSTATE_GENERAL, false;
    /// `EXACTLY ONCE` for a source and target that can't give it.
    SinkExactlyOnceUnsupported = 39217, "ER_SPRIG_SINK_EXACTLY_ONCE_UNSUPPORTED", "SprigSinkExactlyOnceUnsupported", SQLSTATE_GENERAL, false;
    /// An unknown or invalid sink or connection option, template or column list.
    SinkOptionInvalid = 39218, "ER_SPRIG_SINK_OPTION_INVALID", "SprigSinkOptionInvalid", SQLSTATE_GENERAL, false;
    /// A transform that writes, calls out or isn't deterministic under `EXACTLY ONCE`.
    SinkTransformInvalid = 39219, "ER_SPRIG_SINK_TRANSFORM_INVALID", "SprigSinkTransformInvalid", SQLSTATE_GENERAL, false;
    /// A sink script raised an error or returned a malformed result.
    SinkScriptFailed = 39220, "ER_SPRIG_SINK_SCRIPT_FAILED", "SprigSinkScriptFailed", SQLSTATE_GENERAL, false;
    /// The destination refused or failed a delivery.
    SinkDeliveryFailed = 39221, "ER_SPRIG_SINK_DELIVERY_FAILED", "SprigSinkDeliveryFailed", SQLSTATE_GENERAL, true;
    /// The sink paused itself after `sink.pause_after_minutes` of failures.
    SinkPaused = 39222, "ER_SPRIG_SINK_PAUSED", "SprigSinkPaused", SQLSTATE_GENERAL, false;
    /// A sink's position fell behind its source's retention.
    SinkSourceLost = 39223, "ER_SPRIG_SINK_SOURCE_LOST", "SprigSinkSourceLost", SQLSTATE_GENERAL, false;
    /// The destination table or file schema doesn't fit the records.
    SinkSchemaMismatch = 39224, "ER_SPRIG_SINK_SCHEMA_MISMATCH", "SprigSinkSchemaMismatch", SQLSTATE_GENERAL, false;
    /// A record alone exceeds what the destination accepts.
    SinkRecordTooLarge = 39225, "ER_SPRIG_SINK_RECORD_TOO_LARGE", "SprigSinkRecordTooLarge", SQLSTATE_GENERAL, false;
    /// Over `sink.max_per_tenant` or a throughput quota.
    SinkQuota = 39226, "ER_SPRIG_SINK_QUOTA", "SprigSinkQuota", SQLSTATE_GENERAL, false;
    /// A sink task's epoch moved on; the old worker stops.
    SinkFenced = 39227, "ER_SPRIG_SINK_FENCED", "SprigSinkFenced", SQLSTATE_GENERAL, true;
    /// The worker's cloud identity without the operator's permission.
    SinkPlatformIdentityDenied = 39228, "ER_SPRIG_SINK_PLATFORM_IDENTITY_DENIED", "SprigSinkPlatformIdentityDenied", SQLSTATE_GENERAL, false;
    /// A sink's owner can no longer read its source.
    SinkOwnerPrivilegeLost = 39229, "ER_SPRIG_SINK_OWNER_PRIVILEGE_LOST", "SprigSinkOwnerPrivilegeLost", SQLSTATE_GENERAL, false;
    /// `sink.enabled` is off, or the target kind isn't in `sink.allowed_kinds`.
    SinkDisabled = 39230, "ER_SPRIG_SINK_DISABLED", "SprigSinkDisabled", SQLSTATE_GENERAL, false;
    /// A format or compression the target or source can't produce.
    SinkFormatUnsupported = 39231, "ER_SPRIG_SINK_FORMAT_UNSUPPORTED", "SprigSinkFormatUnsupported", SQLSTATE_GENERAL, false;
    /// An operation the sink's state doesn't allow.
    SinkState = 39232, "ER_SPRIG_SINK_STATE", "SprigSinkState", SQLSTATE_GENERAL, false;
    /// A platform connection the tenant isn't granted.
    SinkPlatformConnectionNotGranted = 39233, "ER_SPRIG_SINK_PLATFORM_CONNECTION_NOT_GRANTED", "SprigSinkPlatformConnectionNotGranted", SQLSTATE_GENERAL, false;
    /// A worker can't read a platform connection's secret file.
    SinkPlatformSecretUnavailable = 39234, "ER_SPRIG_SINK_PLATFORM_SECRET_UNAVAILABLE", "SprigSinkPlatformSecretUnavailable", SQLSTATE_GENERAL, true;
    /// A sink script's `db` handle was given anything but a read.
    SinkScriptReadOnly = 39235, "ER_SPRIG_SINK_SCRIPT_READ_ONLY", "SprigSinkScriptReadOnly", SQLSTATE_GENERAL, false;
    /// A distribution-key part has a type that can't be a key, or the
    /// values don't match the key's parts.
    UnsupportedKeyType = 39400, "ER_SPRIG_UNSUPPORTED_KEY_TYPE", "SprigUnsupportedKeyType", SQLSTATE_GENERAL, false;
    /// A string key uses a collation outside the supported set.
    UnsupportedKeyCollation = 39401, "ER_SPRIG_UNSUPPORTED_KEY_COLLATION", "SprigUnsupportedKeyCollation", SQLSTATE_GENERAL, false;
    /// No cached shard map for the group and etcd unreachable.
    ShardMapUnavailable = 39403, "ER_SPRIG_SHARD_MAP_UNAVAILABLE", "SprigShardMapUnavailable", SQLSTATE_CONNECTION, true;
    /// A key value can't be normalized under its group's key: a string not
    /// valid in its character set or not convertible to the key's, a number
    /// that doesn't parse. The router scatters such a value.
    KeyValueUnroutable = 39405, "ER_SPRIG_KEY_VALUE_UNROUTABLE", "SprigKeyValueUnroutable", SQLSTATE_GENERAL, false;
    /// A sharding pin contradicts another pin or a key rule.
    PinConflict = 39410, "ER_SPRIG_PIN_CONFLICT", "SprigPinConflict", SQLSTATE_GENERAL, false;
    /// A pin that would change the key or group of a table whose group has
    /// already split.
    PinNeedsRekey = 39411, "ER_SPRIG_PIN_NEEDS_REKEY", "SprigPinNeedsRekey", SQLSTATE_GENERAL, false;
    /// No ID block can be fetched: the catalog cluster is unreachable and
    /// the cached blocks are used up.
    IdBlockUnavailable = 39460, "ER_SPRIG_ID_BLOCK_UNAVAILABLE", "SprigIdBlockUnavailable", SQLSTATE_GENERAL, true;
    /// A lookup shard is unreachable for a claim.
    LookupUnavailable = 39461, "ER_SPRIG_LOOKUP_UNAVAILABLE", "SprigLookupUnavailable", SQLSTATE_CONNECTION, true;
    /// An explicit `AUTO_INCREMENT` value lies in an ID block another
    /// gateway still holds.
    IdValueReserved = 39463, "ER_SPRIG_ID_VALUE_RESERVED", "SprigIdValueReserved", SQLSTATE_GENERAL, false;
    /// A cascading foreign-key action across groups the gateway can't
    /// apply: more than 15 levels, or an `ON UPDATE CASCADE` it can't map.
    FkActionNotDistributed = 39462, "ER_SPRIG_FK_ACTION_NOT_DISTRIBUTED", "SprigFkActionNotDistributed", SQLSTATE_GENERAL, false;
    /// A split, merge or move job already holds the shard.
    ShardBusy = 39470, "ER_SPRIG_SHARD_BUSY", "SprigShardBusy", SQLSTATE_GENERAL, true;
    /// The group isn't `active`, the range is a single hash, or the split
    /// or merge doesn't fit the map.
    SplitNotAllowed = 39471, "ER_SPRIG_SPLIT_NOT_ALLOWED", "SprigSplitNotAllowed", SQLSTATE_GENERAL, false;
    /// A whole-tenant move (to another cluster or data center) of a tenant
    /// whose tables are in table groups is refused, since it would move the
    /// catalog map but not the groups' shard maps.
    ShardedTenantMove = 39472, "ER_SPRIG_SHARDED_TENANT_MOVE", "SprigShardedTenantMove", SQLSTATE_GENERAL, false;
    /// The router has no exact plan for the statement across shards; the
    /// message names a reason code.
    StatementNotDistributable = 39420, "ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE", "SprigStatementNotDistributable", SQLSTATE_GENERAL, false;
    /// A shard a transaction's statement reached moved (a split, merge or
    /// move fenced it) while the transaction was open: the transaction
    /// rolled back; retry it.
    ShardMapChanged = 39424, "ER_SPRIG_SHARD_MAP_CHANGED", "SprigShardMapChanged", SQLSTATE_RETRY, true;
    /// A statement's rows held in the gateway passed
    /// `router.max_statement_memory_mb`.
    RouterMemoryLimit = 39422, "ER_SPRIG_ROUTER_MEMORY_LIMIT", "SprigRouterMemoryLimit", SQLSTATE_GENERAL, false;
    /// A runtime row limit of the router was passed: a gathered read
    /// copied more than `router.gather.max_rows` rows.
    PlanLimitExceeded = 39421, "ER_SPRIG_PLAN_LIMIT_EXCEEDED", "SprigPlanLimitExceeded", SQLSTATE_GENERAL, false;
    /// Too many failed logins from this address: it is refused for
    /// `auth.ip_block_seconds` before the greeting.
    LoginRateLimited = 39300, "ER_SPRIG_LOGIN_RATE_LIMITED", "SprigLoginRateLimited", SQLSTATE_GENERAL, true;
    /// Logins can't be decided now: the tenant's credentials can't be
    /// loaded (catalog or directory unreachable), or hashing is saturated.
    AuthUnavailable = 39301, "ER_SPRIG_AUTH_UNAVAILABLE", "SprigAuthUnavailable", SQLSTATE_CONNECTION, true;
    /// The login came to a data center outside the tenant's allowed ones
    /// and `residency.deny_remote_access` is on.
    RemoteAccessDenied = 39302, "ER_SPRIG_REMOTE_ACCESS_DENIED", "SprigRemoteAccessDenied", SQLSTATE_GENERAL, false;
    /// The tenant, or a tenant above it in the tree, is suspended; logins
    /// are refused.
    TenantSuspended = 39303, "ER_SPRIG_TENANT_SUSPENDED", "SprigTenantSuspended", SQLSTATE_GENERAL, false;
    /// The account must sign in with a second factor, or enrol one first
    /// (`auth.mfa_required`); operator mode without one under
    /// `studio.operator_require_mfa` is 39926.
    MfaRequired = 39304, "ER_SPRIG_MFA_REQUIRED", "SprigMfaRequired", SQLSTATE_GENERAL, false;
    /// Impersonation refused: not an administrator over the user's tenant,
    /// the user administers the caller's tenant, `auth.impersonation` is
    /// off, no reason, too long, or from an impersonation session.
    ImpersonationDenied = 39305, "ER_SPRIG_IMPERSONATION_DENIED", "SprigImpersonationDenied", SQLSTATE_GENERAL, false;
    /// The session's account was dropped or locked, or its tenant
    /// suspended, while it was connected; the session closes
    /// (`auth.close_sessions_on_revoke`).
    SessionRevoked = 39306, "ER_SPRIG_SESSION_REVOKED", "SprigSessionRevoked", SQLSTATE_CONNECTION, false;
    /// Named operators are in use: the bootstrap token, a legacy DC-operator
    /// token or no credential at all is refused; log in with `sprigctl
    /// login`.
    OperatorLoginRequired = 39310, "ER_SPRIG_OPERATOR_LOGIN_REQUIRED", "SprigOperatorLoginRequired", SQLSTATE_GENERAL, false;
    /// An operator login needs a second-factor code.
    OperatorMfaRequired = 39311, "ER_SPRIG_OPERATOR_MFA_REQUIRED", "SprigOperatorMfaRequired", SQLSTATE_GENERAL, false;
    /// An operator login failed: unknown name, wrong password or code, a
    /// disabled or temporarily locked account (one message for all).
    OperatorLoginFailed = 39312, "ER_SPRIG_OPERATOR_LOGIN_FAILED", "SprigOperatorLoginFailed", SQLSTATE_SYNTAX_ACCESS, false;
    /// The operator's role, DC or token scopes don't cover the call.
    OperatorForbidden = 39313, "ER_SPRIG_OPERATOR_FORBIDDEN", "SprigOperatorForbidden", SQLSTATE_SYNTAX_ACCESS, false;
    /// An operator of that name exists.
    OperatorExists = 39314, "ER_SPRIG_OPERATOR_EXISTS", "SprigOperatorExists", SQLSTATE_GENERAL, false;
    /// No operator, or no operator token, by that name or id.
    OperatorNotFound = 39315, "ER_SPRIG_OPERATOR_NOT_FOUND", "SprigOperatorNotFound", SQLSTATE_GENERAL, false;
    /// The operator token is expired or revoked, or its owner is disabled:
    /// log in again.
    OperatorTokenExpired = 39316, "ER_SPRIG_OPERATOR_TOKEN_EXPIRED", "SprigOperatorTokenExpired", SQLSTATE_GENERAL, false;
    /// Warning: GRANT dropped a server-wide privilege (SUPER, FILE, ...), which means nothing in a tenant (`auth.unsafe_privilege_action = warn`).
    PrivilegeDropped = 39350, "ER_SPRIG_PRIVILEGE_DROPPED", "SprigPrivilegeDropped", SQLSTATE_GENERAL, false;
    /// Warning: an account or grant change holds in the gateway; MariaDB's own grants follow shortly.
    AuthPropagationPending = 39351, "ER_SPRIG_AUTH_PROPAGATION_PENDING", "SprigAuthPropagationPending", SQLSTATE_GENERAL, false;
    /// A user named where only a role is allowed.
    AccountAsRole = 39355, "ER_SPRIG_ACCOUNT_AS_ROLE", "SprigAccountAsRole", SQLSTATE_GENERAL, false;
    /// The tenant name is taken (the control plane's `tenant-names/`
    /// guard).
    TenantExists = 39360, "ER_SPRIG_TENANT_EXISTS", "SprigTenantExists", SQLSTATE_GENERAL, false;
    /// A subtree move would strand grants or shares from tenants that would
    /// stop being ancestors (the inheritance code checks shares).
    TenantMoveBlocked = 39361, "ER_SPRIG_TENANT_MOVE_BLOCKED", "SprigTenantMoveBlocked", SQLSTATE_GENERAL, false;
    /// A tenant with children can't be dropped (the control plane's delete
    /// guard).
    TenantHasChildren = 39362, "ER_SPRIG_TENANT_HAS_CHILDREN", "SprigTenantHasChildren", SQLSTATE_GENERAL, false;
    /// A new row version fails a row-level security policy's `WITH CHECK`
    /// (or an upsert's conflicting row fails `UPDATE`'s `USING`).
    RlsCheckFailed = 39363, "ER_SPRIG_RLS_CHECK_FAILED", "SprigRlsCheckFailed", SQLSTATE_SYNTAX_ACCESS, false;
    /// A policy expression is non-deterministic, names an internal object,
    /// reads a variable or calls an unknown function.
    RlsPolicyInvalid = 39364, "ER_SPRIG_RLS_POLICY_INVALID", "SprigRlsPolicyInvalid", SQLSTATE_GENERAL, false;
    /// Policy subqueries would form a cycle between tables.
    RlsPolicyCycle = 39365, "ER_SPRIG_RLS_POLICY_CYCLE", "SprigRlsPolicyCycle", SQLSTATE_GENERAL, false;
    /// The tenant's authorization engine filters rows of a table the
    /// statement touches, and the gateway can't apply such a filter yet.
    RlsUnsupported = 39366, "ER_SPRIG_RLS_UNSUPPORTED", "SprigRlsUnsupported", SQLSTATE_GENERAL, false;
    /// A stored program wasn't recorded with the session's backend account
    /// as its definer; the gateway dropped it again.
    DefinerRefused = 39367, "ER_SPRIG_DEFINER_REFUSED", "SprigDefinerRefused", SQLSTATE_GENERAL, false;
    /// A change of `auth.authorizer` names an engine that some live
    /// gateway doesn't report knowing; upgrade those gateways first.
    AuthorizerUnknownToGateways = 39368, "ER_SPRIG_AUTHORIZER_UNKNOWN_TO_GATEWAYS", "SprigAuthorizerUnknownToGateways", SQLSTATE_GENERAL, false;
    /// Note: a routine without SQL SECURITY runs as DEFINER and applies its definer's row-level security policies, not its caller's.
    RlsDefinerDefault = 39369, "ER_SPRIG_RLS_DEFINER_DEFAULT", "SprigRlsDefinerDefault", SQLSTATE_GENERAL, false;
    /// The control plane's store is unreachable.
    MetaUnavailable = 39500, "ER_SPRIG_META_UNAVAILABLE", "SprigMetaUnavailable", SQLSTATE_CONNECTION, true;
    /// This controller no longer leads (or never did): its session was lost
    /// or a guarded transaction failed its fencing-token compare. The caller
    /// steps down; it must not retry.
    NotLeader = 39504, "ER_SPRIG_NOT_LEADER", "SprigNotLeader", SQLSTATE_GENERAL, false;
    /// The tenant moved to another data center while the session held
    /// state that can't follow it (temporary tables, `GET_LOCK` locks,
    /// `LOCK TABLES`, XA); the session must reconnect.
    SessionMoved = 39502, "ER_SPRIG_SESSION_MOVED", "SprigSessionMoved", SQLSTATE_CONNECTION, true;
    /// The data center isn't registered.
    UnknownDc = 39503, "ER_SPRIG_UNKNOWN_DC", "SprigUnknownDc", SQLSTATE_GENERAL, false;
    /// A tenant would be deeper than `tenant.max_depth`.
    TenantDepthExceeded = 39508, "ER_SPRIG_TENANT_DEPTH_EXCEEDED", "SprigTenantDepthExceeded", SQLSTATE_GENERAL, false;
    /// A tree operation on a subtree with an open move.
    TenantTreeBusy = 39509, "ER_SPRIG_TENANT_TREE_BUSY", "SprigTenantTreeBusy", SQLSTATE_GENERAL, true;
    /// A residency policy doesn't parse.
    ResidencyInvalid = 39506, "ER_SPRIG_RESIDENCY_INVALID", "SprigResidencyInvalid", SQLSTATE_GENERAL, false;
    /// A residency policy allows no active data center within the
    /// ancestors' sets, or its compliance plan has no cluster with headroom
    /// in the allowed ones.
    ResidencyUnsatisfiable = 39507, "ER_SPRIG_RESIDENCY_UNSATISFIABLE", "SprigResidencyUnsatisfiable", SQLSTATE_GENERAL, false;
    /// A VM cluster can't be created: too few free hosts in the data
    /// center.
    ProvisionNoHosts = 39513, "ER_SPRIG_PROVISION_NO_HOSTS", "SprigProvisionNoHosts", SQLSTATE_GENERAL, true;
    /// No data center the new tenant's residency allows is active, stable
    /// and has a cluster that can take it.
    NoDcCapacity = 39525, "ER_SPRIG_NO_DC_CAPACITY", "SprigNoDcCapacity", SQLSTATE_GENERAL, true;
    /// The provisioner doesn't support the operation.
    ProvisionerUnsupported = 39510, "ER_SPRIG_PROVISIONER_UNSUPPORTED", "SprigProvisionerUnsupported", SQLSTATE_GENERAL, false;
    /// The tenant's home DC holds no valid DC lease (its permit lapsed or
    /// was refused): writes are refused, reads continue; retry after the
    /// tenant is re-routed.
    DcFenced = 39501, "ER_SPRIG_DC_FENCED", "SprigDcFenced", SQLSTATE_GENERAL, true;
    /// A manual failover can't run: the old home is healthy, the target is
    /// not allowed or lacks a unit, or data loss wasn't accepted.
    FailoverRefused = 39505, "ER_SPRIG_FAILOVER_REFUSED", "SprigFailoverRefused", SQLSTATE_GENERAL, false;
    /// A cluster failover stopped before promoting: the loss estimate is
    /// over the RPO, two replicas diverge, the writer isn't confirmed down,
    /// or `failover.mode` is manual; an operator decides.
    FailoverBlocked = 39512, "ER_SPRIG_FAILOVER_BLOCKED", "SprigFailoverBlocked", SQLSTATE_GENERAL, false;
    /// A write reached a copy that stays fenced after an outage (a
    /// returning server or DC); retry after re-routing.
    RecoveryFenced = 39530, "ER_SPRIG_RECOVERY_FENCED", "SprigRecoveryFenced", SQLSTATE_GENERAL, true;
    /// A returning DC admits new connections in steps; retry after the
    /// jittered delay in the message.
    AdmissionBackoff = 39531, "ER_SPRIG_ADMISSION_BACKOFF", "SprigAdmissionBackoff", SQLSTATE_CONNECTION, true;
    /// An external cluster failed its registration prechecks; the message
    /// lists every missing item.
    ClusterPrecheckFailed = 39511, "ER_SPRIG_CLUSTER_PRECHECK_FAILED", "SprigClusterPrecheckFailed", SQLSTATE_GENERAL, false;
    /// A write would add a second cluster without XA to a transaction
    /// under `txn.non_xa_policy = reject`; the statement didn't run.
    CrossClusterWriteRejected = 39440, "ER_SPRIG_CROSS_CLUSTER_WRITE_REJECTED", "SprigCrossClusterWriteRejected", SQLSTATE_GENERAL, false;
    /// A distributed commit's outcome is unknown; the connection is then
    /// closed.
    CommitOutcomeUnknown = 39441, "ER_SPRIG_COMMIT_OUTCOME_UNKNOWN", "SprigCommitOutcomeUnknown", SQLSTATE_CONNECTION, false;
    /// A best-effort commit committed only some clusters.
    PartialCommit = 39442, "ER_SPRIG_PARTIAL_COMMIT", "SprigPartialCommit", SQLSTATE_GENERAL, false;
    /// A distributed transaction was rolled back; retry it.
    DtxRolledBack = 39443, "ER_SPRIG_DTX_ROLLED_BACK", "SprigDtxRolledBack", SQLSTATE_RETRY, true;
    /// A schema change failed on a shard after others ran it, and the job
    /// could not finish there; the reconciler completes it.
    /// A standard MariaDB error from the shard is returned as itself.
    DdlFailed = 39480, "ER_SPRIG_DDL_FAILED", "SprigDdlFailed", SQLSTATE_GENERAL, false;
    /// A schema change that can't be distributed across shards yet.
    DdlUnsupported = 39481, "ER_SPRIG_DDL_UNSUPPORTED", "SprigDdlUnsupported", SQLSTATE_GENERAL, false;
    /// The shards of a table disagree on its definition before a change;
    /// the change is refused until the reconciler or an operator repairs
    /// them.
    DdlShardDrift = 39483, "ER_SPRIG_DDL_SHARD_DRIFT", "SprigDdlShardDrift", SQLSTATE_GENERAL, false;
    /// A transaction would touch more clusters than `txn.max_branches`.
    TooManyBranches = 39444, "ER_SPRIG_TOO_MANY_BRANCHES", "SprigTooManyBranches", SQLSTATE_GENERAL, false;
    /// No prepared branch of the named distributed transaction is on the
    /// data center's clusters: nothing is in doubt (`sprigctl txn show` and
    /// `resolve`).
    TxnNotInDoubt = 39447, "ER_SPRIG_TXN_NOT_IN_DOUBT", "SprigTxnNotInDoubt", SQLSTATE_GENERAL, false;
    /// A held branch (Sprig 2PC) ran a statement that doesn't replay to the
    /// same result; the transaction is rolled back.
    HeldBranchStatement = 39446, "ER_SPRIG_HELD_BRANCH_STATEMENT", "SprigHeldBranchStatement", SQLSTATE_GENERAL, false;
    /// Sprig 2PC's relay fence: the agent's relay refuses the tenant's
    /// connections to this node while it recovers a held branch whose
    /// holder was lost; retry.
    HeldBranchFenced = 39448, "ER_SPRIG_HELD_BRANCH_FENCED", "SprigHeldBranchFenced", SQLSTATE_CONNECTION, true;
    /// A branch of the session's last distributed commit is still being
    /// finished on this cluster (phase 2 outran `txn.phase2_wait_ms`, and
    /// settling it took longer than `txn.commit_resolve_timeout_ms`); the
    /// statement didn't run, retry it.
    Phase2Pending = 39449, "ER_SPRIG_PHASE2_PENDING", "SprigPhase2Pending", SQLSTATE_RETRY, true;
    /// A tenant move between data centers can't start: the tenant is
    /// pinned, not active, already homed in the target, already moving,
    /// or the target has no cluster for it.
    TenantMoveRefused = 39550, "ER_SPRIG_TENANT_MOVE_REFUSED", "SprigTenantMoveRefused", SQLSTATE_GENERAL, false;
    /// A type with two parents.
    TypeMultipleInheritance = 39701, "ER_SPRIG_TYPE_MULTIPLE_INHERITANCE", "SprigTypeMultipleInheritance", SQLSTATE_GENERAL, false;
    /// A field clashes with an inherited one, or with a subtype's.
    TypeFieldConflict = 39702, "ER_SPRIG_TYPE_FIELD_CONFLICT", "SprigTypeFieldConflict", SQLSTATE_GENERAL, false;
    /// An insert of an abstract type.
    TypeAbstract = 39703, "ER_SPRIG_TYPE_ABSTRACT", "SprigTypeAbstract", SQLSTATE_GENERAL, false;
    /// A write to a type or path column.
    TypeColumnReadOnly = 39705, "ER_SPRIG_TYPE_COLUMN_READ_ONLY", "SprigTypeColumnReadOnly", SQLSTATE_GENERAL, false;
    /// A key update on a typed or hierarchical row.
    TypeKeyUpdate = 39706, "ER_SPRIG_TYPE_KEY_UPDATE", "SprigTypeKeyUpdate", SQLSTATE_GENERAL, false;
    /// Dropping a type with subtypes without `CASCADE`.
    TypeHasDependents = 39707, "ER_SPRIG_TYPE_HAS_DEPENDENTS", "SprigTypeHasDependents", SQLSTATE_GENERAL, false;
    /// A type path over 255 characters, or a hierarchy deeper than
    /// `types.max_depth`.
    TypePathTooLong = 39708, "ER_SPRIG_TYPE_PATH_TOO_LONG", "SprigTypePathTooLong", SQLSTATE_GENERAL, false;
    /// A statement form on typed or hierarchical tables this release
    /// doesn't support.
    TypeUnsupported = 39710, "ER_SPRIG_TYPE_UNSUPPORTED", "SprigTypeUnsupported", SQLSTATE_GENERAL, false;
    /// A record moved under its own descendant.
    HierarchyCycle = 39711, "ER_SPRIG_HIERARCHY_CYCLE", "SprigHierarchyCycle", SQLSTATE_GENERAL, false;
    /// A record's parent doesn't exist.
    HierarchyParentNotFound = 39712, "ER_SPRIG_HIERARCHY_PARENT_NOT_FOUND", "SprigHierarchyParentNotFound", SQLSTATE_GENERAL, false;
    /// A record hierarchy over its `MAX DEPTH` or path length.
    HierarchyTooDeep = 39713, "ER_SPRIG_HIERARCHY_TOO_DEEP", "SprigHierarchyTooDeep", SQLSTATE_GENERAL, false;
    /// Deleting a record with children under `ON DELETE RESTRICT`.
    HierarchyHasChildren = 39714, "ER_SPRIG_HIERARCHY_HAS_CHILDREN", "SprigHierarchyHasChildren", SQLSTATE_GENERAL, false;
    /// A subtree move or cascade over `hierarchy.max_move_rows`.
    SubtreeTooLarge = 39715, "ER_SPRIG_SUBTREE_TOO_LARGE", "SprigSubtreeTooLarge", SQLSTATE_GENERAL, false;
    /// `ADD HIERARCHY` found orphans or cycles.
    HierarchyBackfillFailed = 39716, "ER_SPRIG_HIERARCHY_BACKFILL_FAILED", "SprigHierarchyBackfillFailed", SQLSTATE_GENERAL, false;
    /// A discriminator value that contradicts the table written to, or a
    /// subtype's value another subtype already declares.
    TypeDiscriminatorMismatch = 39717, "ER_SPRIG_TYPE_DISCRIMINATOR_MISMATCH", "SprigTypeDiscriminatorMismatch", SQLSTATE_GENERAL, false;
    /// Unknown key-value namespace.
    KvNamespaceNotFound = 39720, "ER_SPRIG_KV_NAMESPACE_NOT_FOUND", "SprigKvNamespaceNotFound", SQLSTATE_GENERAL, false;
    /// A key's version isn't the one the write expected.
    KvVersionMismatch = 39721, "ER_SPRIG_KV_VERSION_MISMATCH", "SprigKvVersionMismatch", SQLSTATE_GENERAL, false;
    /// The key exists, and the write needed it absent.
    KvKeyExists = 39722, "ER_SPRIG_KV_KEY_EXISTS", "SprigKvKeyExists", SQLSTATE_GENERAL, false;
    /// An empty key, or one over 1024 bytes.
    KvInvalidKey = 39723, "ER_SPRIG_KV_INVALID_KEY", "SprigKvInvalidKey", SQLSTATE_GENERAL, false;
    /// A value over the namespace's limit or not in its codec.
    KvValueInvalid = 39724, "ER_SPRIG_KV_VALUE_INVALID", "SprigKvValueInvalid", SQLSTATE_GENERAL, false;
    /// The key holds another type (Redis `WRONGTYPE`).
    KvWrongType = 39730, "ER_SPRIG_KV_WRONG_TYPE", "SprigKvWrongType", SQLSTATE_GENERAL, false;
    /// A hash field or member over 2024 bytes.
    KvElementTooLong = 39731, "ER_SPRIG_KV_ELEMENT_TOO_LONG", "SprigKvElementTooLong", SQLSTATE_GENERAL, false;
    /// Unknown vector index.
    VectorIndexNotFound = 39735, "ER_SPRIG_VECTOR_INDEX_NOT_FOUND", "SprigVectorIndexNotFound", SQLSTATE_GENERAL, false;
    /// A vector of the wrong length.
    VectorDimensionMismatch = 39736, "ER_SPRIG_VECTOR_DIMENSION_MISMATCH", "SprigVectorDimensionMismatch", SQLSTATE_GENERAL, false;
    /// A metric the index doesn't use, or a zero vector under a cosine
    /// or dotProduct index.
    VectorMetricMismatch = 39737, "ER_SPRIG_VECTOR_METRIC_MISMATCH", "SprigVectorMetricMismatch", SQLSTATE_GENERAL, false;
    /// `limit` over `numCandidates`, or over `vector.max_k`.
    VectorLimitInvalid = 39738, "ER_SPRIG_VECTOR_LIMIT_INVALID", "SprigVectorLimitInvalid", SQLSTATE_GENERAL, false;
    /// A filter on a field the index can't evaluate.
    VectorFilterNotIndexed = 39739, "ER_SPRIG_VECTOR_FILTER_NOT_INDEXED", "SprigVectorFilterNotIndexed", SQLSTATE_GENERAL, false;
    /// Over `vector.max_indexes_per_table`.
    VectorTooManyIndexes = 39740, "ER_SPRIG_VECTOR_TOO_MANY_INDEXES", "SprigVectorTooManyIndexes", SQLSTATE_GENERAL, false;
    /// Unknown dimensional model.
    DimModelNotFound = 39750, "ER_SPRIG_DIM_MODEL_NOT_FOUND", "SprigDimModelNotFound", SQLSTATE_GENERAL, false;
    /// Unknown measure, dimension or level.
    DimUnknownMember = 39751, "ER_SPRIG_DIM_UNKNOWN_MEMBER", "SprigDimUnknownMember", SQLSTATE_GENERAL, false;
    /// A model naming a missing table, column or key.
    DimInvalidModel = 39752, "ER_SPRIG_DIM_INVALID_MODEL", "SprigDimInvalidModel", SQLSTATE_GENERAL, false;
    /// Over `analytics.max_groups`.
    DimTooManyGroups = 39753, "ER_SPRIG_DIM_TOO_MANY_GROUPS", "SprigDimTooManyGroups", SQLSTATE_GENERAL, false;
    /// A query the merge can't answer exactly.
    DimNotDecomposable = 39754, "ER_SPRIG_DIM_NOT_DECOMPOSABLE", "SprigDimNotDecomposable", SQLSTATE_GENERAL, false;
    /// A named time zone without MariaDB's zone tables.
    DimTzUnavailable = 39755, "ER_SPRIG_DIM_TZ_UNAVAILABLE", "SprigDimTzUnavailable", SQLSTATE_GENERAL, false;
    /// `TRUNCATE` of a versioned dimension.
    DimVersionedTruncate = 39756, "ER_SPRIG_DIM_VERSIONED_TRUNCATE", "SprigDimVersionedTruncate", SQLSTATE_GENERAL, false;
    /// `as_of` older than the kept history.
    DimHistoryPurged = 39757, "ER_SPRIG_DIM_HISTORY_PURGED", "SprigDimHistoryPurged", SQLSTATE_GENERAL, false;
    /// Bad geometry or coordinates.
    GeoInvalid = 39765, "ER_SPRIG_GEO_INVALID", "SprigGeoInvalid", SQLSTATE_GENERAL, false;
    /// A geographic SRID other than 4326.
    GeoSridUnsupported = 39766, "ER_SPRIG_GEO_SRID_UNSUPPORTED", "SprigGeoSridUnsupported", SQLSTATE_GENERAL, false;
    /// A geodesic expression the gateway can't evaluate exactly.
    GeoUnsupportedExpression = 39767, "ER_SPRIG_GEO_UNSUPPORTED_EXPRESSION", "SprigGeoUnsupportedExpression", SQLSTATE_GENERAL, false;
    /// A geometry over `geo.max_vertices`.
    GeoTooComplex = 39768, "ER_SPRIG_GEO_TOO_COMPLEX", "SprigGeoTooComplex", SQLSTATE_GENERAL, false;
    /// Unknown space.
    SpaceNotFound = 39880, "ER_SPRIG_SPACE_NOT_FOUND", "SprigSpaceNotFound", SQLSTATE_GENERAL, false;
    /// A space or share name is taken.
    SpaceExists = 39881, "ER_SPRIG_SPACE_EXISTS", "SprigSpaceExists", SQLSTATE_GENERAL, false;
    /// Dropping a space with children or layer rows without `CASCADE`.
    SpaceHasChildren = 39882, "ER_SPRIG_SPACE_HAS_CHILDREN", "SprigSpaceHasChildren", SQLSTATE_GENERAL, false;
    /// A space deeper than `tenant.max_depth`, or a share chain longer than
    /// `inherit.max_depth`.
    SpaceDepthExceeded = 39883, "ER_SPRIG_SPACE_DEPTH_EXCEEDED", "SprigSpaceDepthExceeded", SQLSTATE_GENERAL, false;
    /// The session may not act in that space.
    SpaceAccessDenied = 39884, "ER_SPRIG_SPACE_ACCESS_DENIED", "SprigSpaceAccessDenied", SQLSTATE_SYNTAX_ACCESS, false;
    /// A share or reset on an ordinary object.
    NotInheritable = 39885, "ER_SPRIG_NOT_INHERITABLE", "SprigNotInheritable", SQLSTATE_GENERAL, false;
    /// Changing a record's identity key.
    InheritKeyUpdate = 39886, "ER_SPRIG_INHERIT_KEY_UPDATE", "SprigInheritKeyUpdate", SQLSTATE_GENERAL, false;
    /// Unknown share.
    ShareNotFound = 39887, "ER_SPRIG_SHARE_NOT_FOUND", "SprigShareNotFound", SQLSTATE_GENERAL, false;
    /// A share filter that isn't deterministic, has a subquery, or names
    /// unshared fields.
    ShareFilterInvalid = 39888, "ER_SPRIG_SHARE_FILTER_INVALID", "SprigShareFilterInvalid", SQLSTATE_GENERAL, false;
    /// No merge mode is possible for a share.
    ShareResidencyConflict = 39889, "ER_SPRIG_SHARE_RESIDENCY_CONFLICT", "SprigShareResidencyConflict", SQLSTATE_GENERAL, false;
    /// A re-share of inherited data without `WITH RESHARE`.
    ShareNotResharable = 39890, "ER_SPRIG_SHARE_NOT_RESHARABLE", "SprigShareNotResharable", SQLSTATE_GENERAL, false;
    /// A statement form on an inheritable object this release doesn't
    /// support.
    InheritStatementUnsupported = 39891, "ER_SPRIG_INHERIT_STATEMENT_UNSUPPORTED", "SprigInheritStatementUnsupported", SQLSTATE_GENERAL, false;
    /// A child tenant's object diverges from its source.
    InheritSchemaMismatch = 39892, "ER_SPRIG_INHERIT_SCHEMA_MISMATCH", "SprigInheritSchemaMismatch", SQLSTATE_GENERAL, false;
    /// A materialized copy lags beyond `inherit.copy_stale_seconds`
    /// (a note-level warning).
    InheritCopyStale = 39893, "ER_SPRIG_INHERIT_COPY_STALE", "SprigInheritCopyStale", SQLSTATE_GENERAL, true;
    /// A federated parent's cluster can't be reached.
    InheritFederatedUnavailable = 39894, "ER_SPRIG_INHERIT_FEDERATED_UNAVAILABLE", "SprigInheritFederatedUnavailable", SQLSTATE_GENERAL, true;
    /// A destructive schema change on an inheritable object that a share
    /// still reaches.
    InheritedObjectInUse = 39895, "ER_SPRIG_INHERITED_OBJECT_IN_USE", "SprigInheritedObjectInUse", SQLSTATE_GENERAL, false;
    /// A publish into an inherited object names no target where one is
    /// needed, a tenant that isn't a member, or one the publisher isn't a
    /// proper ancestor of.
    InheritTargetTenant = 39896, "ER_SPRIG_INHERIT_TARGET_TENANT", "SprigInheritTargetTenant", SQLSTATE_GENERAL, false;
    /// A member changes what only the definition decides, drops an
    /// instance, or creates a local object with an inherited name.
    InheritNotOverridable = 39897, "ER_SPRIG_INHERIT_NOT_OVERRIDABLE", "SprigInheritNotOverridable", SQLSTATE_GENERAL, false;
    /// A publish into a definition, or an instance, that is being dropped
    /// or has left its scope.
    InheritDefinitionDropping = 39898, "ER_SPRIG_INHERIT_DEFINITION_DROPPING", "SprigInheritDefinitionDropping", SQLSTATE_GENERAL, false;
    /// An instance couldn't be created or reached in time.
    InheritInstanceUnavailable = 39899, "ER_SPRIG_INHERIT_INSTANCE_UNAVAILABLE", "SprigInheritInstanceUnavailable", SQLSTATE_RETRY, true;
    /// Over `inherit.max_definitions_per_tenant` or
    /// `inherit.max_instances_per_tenant`.
    InheritInstanceQuota = 39240, "ER_SPRIG_INHERIT_INSTANCE_QUOTA", "SprigInheritInstanceQuota", SQLSTATE_GENERAL, false;
    /// An inherited connection's `REQUIRED` option or credential isn't set
    /// for this member.
    InheritConfigMissing = 39241, "ER_SPRIG_INHERIT_CONFIG_MISSING", "SprigInheritConfigMissing", SQLSTATE_GENERAL, false;
    /// The member's `sprig_inherited` principal lacks a privilege the
    /// definition's `GRANTS` asks for.
    InheritRunAsPrivilege = 39242, "ER_SPRIG_INHERIT_RUN_AS_PRIVILEGE", "SprigInheritRunAsPrivilege", SQLSTATE_GENERAL, false;
    /// A fan-in read, or under `residency.cross_tenant_messaging_strict` a
    /// cross-tenant publish, would move a member's messages where its
    /// residency forbids.
    InheritResidency = 39243, "ER_SPRIG_INHERIT_RESIDENCY", "SprigInheritResidency", SQLSTATE_GENERAL, false;
    /// A broadcast over `inherit.broadcast_max_bytes`,
    /// `inherit.broadcasts_per_hour` or `inherit.broadcast_max_members`.
    InheritBroadcastLimit = 39244, "ER_SPRIG_INHERIT_BROADCAST_LIMIT", "SprigInheritBroadcastLimit", SQLSTATE_GENERAL, false;
    /// `DROP DATABASE` of a database whose cross-tenant outbox still holds
    /// committed rows after `inherit.outbox_drain_seconds`.
    InheritOutboxPending = 39245, "ER_SPRIG_INHERIT_OUTBOX_PENDING", "SprigInheritOutboxPending", SQLSTATE_GENERAL, true;
    /// Unknown virtual host, exchange or queue (AMQP 404, `amqp:not-found`).
    MqNotFound = 39250, "ER_SPRIG_MQ_NOT_FOUND", "SprigMqNotFound", SQLSTATE_GENERAL, false;
    /// A redeclare with other arguments, or an unknown delivery tag (AMQP 406).
    MqPrecondition = 39251, "ER_SPRIG_MQ_PRECONDITION", "SprigMqPrecondition", SQLSTATE_GENERAL, false;
    /// An exclusive queue or consumer is held by another connection (AMQP 405).
    MqResourceLocked = 39252, "ER_SPRIG_MQ_RESOURCE_LOCKED", "SprigMqResourceLocked", SQLSTATE_GENERAL, false;
    /// Permission denied on a virtual host, exchange or queue (AMQP 403).
    MqAccessRefused = 39253, "ER_SPRIG_MQ_ACCESS_REFUSED", "SprigMqAccessRefused", SQLSTATE_GENERAL, false;
    /// An AMQP method, flag or argument SprigDB doesn't support (AMQP 540).
    MqNotImplemented = 39254, "ER_SPRIG_MQ_NOT_IMPLEMENTED", "SprigMqNotImplemented", SQLSTATE_GENERAL, false;
    /// A mandatory publish reached no queue (AMQP 312, returned).
    MqUnroutable = 39255, "ER_SPRIG_MQ_UNROUTABLE", "SprigMqUnroutable", SQLSTATE_GENERAL, false;
    /// A publish to a full queue with `reject-publish` overflow (nacked).
    MqQueueFull = 39256, "ER_SPRIG_MQ_QUEUE_FULL", "SprigMqQueueFull", SQLSTATE_GENERAL, false;
    /// A message above `amqp.max_message_bytes`.
    MqMessageTooLarge = 39257, "ER_SPRIG_MQ_MESSAGE_TOO_LARGE", "SprigMqMessageTooLarge", SQLSTATE_GENERAL, false;
    /// An ack in a transaction of a message already settled or whose lease expired; the transaction rolls back.
    MqAlreadySettled = 39258, "ER_SPRIG_MQ_ALREADY_SETTLED", "SprigMqAlreadySettled", SQLSTATE_GENERAL, false;
    /// A malformed or forged delivery token.
    MqDeliveryTokenInvalid = 39259, "ER_SPRIG_MQ_DELIVERY_TOKEN_INVALID", "SprigMqDeliveryTokenInvalid", SQLSTATE_GENERAL, false;
    /// The queue's owner changed during the call; retry.
    MqOwnerMoved = 39260, "ER_SPRIG_MQ_OWNER_MOVED", "SprigMqOwnerMoved", SQLSTATE_RETRY, true;
    /// A publish wasn't committed within `amqp.publish_timeout_ms` (nacked).
    MqConfirmTimeout = 39261, "ER_SPRIG_MQ_CONFIRM_TIMEOUT", "SprigMqConfirmTimeout", SQLSTATE_GENERAL, true;
    /// A delivery stayed unacknowledged past the consumer timeout; the consumer is cancelled.
    MqConsumerTimeout = 39262, "ER_SPRIG_MQ_CONSUMER_TIMEOUT", "SprigMqConsumerTimeout", SQLSTATE_GENERAL, false;
    /// An ack inside a transaction would add a cluster without XA while `txn.non_xa_policy` is `reject`.
    MqTxnNeedsXa = 39263, "ER_SPRIG_MQ_TXN_NEEDS_XA", "SprigMqTxnNeedsXa", SQLSTATE_GENERAL, false;
    /// An `x-delay` above `queue.delay_max_ms`.
    MqDelayOutOfRange = 39264, "ER_SPRIG_MQ_DELAY_OUT_OF_RANGE", "SprigMqDelayOutOfRange", SQLSTATE_GENERAL, false;
    /// A login or publish outside the data centers the tenant's residency allows.
    MqResidency = 39265, "ER_SPRIG_MQ_RESIDENCY", "SprigMqResidency", SQLSTATE_GENERAL, false;
    /// Over a tenant quota; the connection is blocked.
    MqQuota = 39266, "ER_SPRIG_MQ_QUOTA", "SprigMqQuota", SQLSTATE_GENERAL, true;
    /// Above the virtual host's `max-queues` or `max-connections`.
    MqVhostLimit = 39267, "ER_SPRIG_MQ_VHOST_LIMIT", "SprigMqVhostLimit", SQLSTATE_GENERAL, false;
    /// A malformed AMQP frame or command, or a method on a channel that isn't open.
    AmqpFrameError = 39268, "ER_SPRIG_AMQP_FRAME_ERROR", "SprigAmqpFrameError", SQLSTATE_GENERAL, false;
    /// AMQP authentication failed.
    AmqpSaslFailed = 39269, "ER_SPRIG_AMQP_SASL_FAILED", "SprigAmqpSaslFailed", SQLSTATE_GENERAL, false;
    /// An unknown AMQP protocol header; SprigDB answers with the one it speaks and closes.
    AmqpProtocolHeader = 39270, "ER_SPRIG_AMQP_PROTOCOL_HEADER", "SprigAmqpProtocolHeader", SQLSTATE_GENERAL, false;
    /// Above `channel_max` or `amqp.max_channels_per_tenant`.
    AmqpChannelLimit = 39271, "ER_SPRIG_AMQP_CHANNEL_LIMIT", "SprigAmqpChannelLimit", SQLSTATE_GENERAL, false;
    /// An AMQP 1.0 link with a bad address or terminus.
    Amqp10LinkRefused = 39272, "ER_SPRIG_AMQP10_LINK_REFUSED", "SprigAmqp10LinkRefused", SQLSTATE_GENERAL, false;
    /// An AMQP 1.0 feature SprigDB doesn't support (transactions, link recovery, the second settle mode).
    Amqp10Unsupported = 39273, "ER_SPRIG_AMQP10_UNSUPPORTED", "SprigAmqp10Unsupported", SQLSTATE_GENERAL, false;
    /// A bad `x-stream-offset` or offset specification.
    MqStreamOffset = 39274, "ER_SPRIG_MQ_STREAM_OFFSET", "SprigMqStreamOffset", SQLSTATE_GENERAL, false;
    /// Direct reply-to misused: manual ack, another channel or a second consumer.
    MqDirectReplyTo = 39275, "ER_SPRIG_MQ_DIRECT_REPLY_TO", "SprigMqDirectReplyTo", SQLSTATE_GENERAL, false;
    /// A `RabbitMQ` management endpoint SprigDB doesn't serve (HTTP 501).
    MqMgmtUnsupported = 39276, "ER_SPRIG_MQ_MGMT_UNSUPPORTED", "SprigMqMgmtUnsupported", SQLSTATE_GENERAL, false;
    /// A message dropped in a dead-letter cycle (logged).
    MqDeadLetterCycle = 39277, "ER_SPRIG_MQ_DEAD_LETTER_CYCLE", "SprigMqDeadLetterCycle", SQLSTATE_GENERAL, false;
    /// A request over a rate limit or admission control (including
    /// analytics admission).
    RateLimited = 39780, "ER_SPRIG_RATE_LIMITED", "SprigRateLimited", SQLSTATE_GENERAL, true;
    /// A transaction handle names no open transaction on its gateway.
    TxnHandleUnknown = 39781, "ER_SPRIG_TXN_HANDLE_UNKNOWN", "SprigTxnHandleUnknown", SQLSTATE_GENERAL, false;
    /// The transaction behind a handle passed its idle or lifetime deadline
    /// and was rolled back.
    TxnHandleExpired = 39782, "ER_SPRIG_TXN_HANDLE_EXPIRED", "SprigTxnHandleExpired", SQLSTATE_GENERAL, false;
    /// A transaction handle belongs to another principal or tenant.
    TxnHandleForbidden = 39783, "ER_SPRIG_TXN_HANDLE_FORBIDDEN", "SprigTxnHandleForbidden", SQLSTATE_GENERAL, false;
    /// A buffered REST result went over `api.rest.max_buffered_bytes`; use
    /// NDJSON streaming.
    ResultTooLarge = 39785, "ER_SPRIG_RESULT_TOO_LARGE", "SprigResultTooLarge", SQLSTATE_GENERAL, false;
    /// The credential doesn't name one tenant.
    TenantAmbiguous = 39786, "ER_SPRIG_TENANT_AMBIGUOUS", "SprigTenantAmbiguous", SQLSTATE_GENERAL, false;
    /// A request body went over `api.max_request_bytes`.
    RequestTooLarge = 39787, "ER_SPRIG_REQUEST_TOO_LARGE", "SprigRequestTooLarge", SQLSTATE_GENERAL, false;
    /// A call couldn't be forwarded to the gateway owning its transaction
    /// handle.
    ForwardFailed = 39788, "ER_SPRIG_FORWARD_FAILED", "SprigForwardFailed", SQLSTATE_CONNECTION, true;
    /// A logic function called over HTTP or gRPC that isn't exposed to
    /// that protocol.
    FunctionNotExposed = 39789, "ER_SPRIG_FUNCTION_NOT_EXPOSED", "SprigFunctionNotExposed", SQLSTATE_GENERAL, false;
    /// A change stream's client fell more than
    /// `api.stream.max_buffered_events` behind and the stream ended; it
    /// resumes from its last token.
    StreamSlowConsumer = 39790, "ER_SPRIG_STREAM_SLOW_CONSUMER", "SprigStreamSlowConsumer", SQLSTATE_GENERAL, true;
    /// gRPC over HTTP/1.1, or another transport the call can't use.
    TransportUnsupported = 39791, "ER_SPRIG_TRANSPORT_UNSUPPORTED", "SprigTransportUnsupported", SQLSTATE_GENERAL, false;
    /// An Admin API request is malformed or names something invalid.
    AdminRequestInvalid = 39792, "ER_SPRIG_ADMIN_REQUEST_INVALID", "SprigAdminRequestInvalid", SQLSTATE_GENERAL, false;
    /// An Admin API request has no valid credential.
    AdminUnauthenticated = 39793, "ER_SPRIG_ADMIN_UNAUTHENTICATED", "SprigAdminUnauthenticated", SQLSTATE_GENERAL, false;
    /// `Sql.Query`'s `language` is unknown, or a language other than SQL
    /// came with a transaction handle, `multi_statements` or a command it
    /// can't run.
    QueryLanguageInvalid = 39794, "ER_SPRIG_QUERY_LANGUAGE_INVALID", "SprigQueryLanguageInvalid", SQLSTATE_GENERAL, false;
    /// A Cypher, GQL or MongoDB statement sent through `Sql.Query` failed
    /// with an error of that language that has no SprigDB or MySQL number;
    /// the message carries the language's own code.
    LanguageStatementFailed = 39795, "ER_SPRIG_LANGUAGE_STATEMENT_FAILED", "SprigLanguageStatementFailed", SQLSTATE_GENERAL, false;
    /// A change feed start is older than all history the feed can read.
    FeedHistoryLost = 39800, "ER_SPRIG_FEED_HISTORY_LOST", "SprigFeedHistoryLost", SQLSTATE_GENERAL, false;
    /// A resume token is malformed or belongs to another tenant.
    FeedBadResumeToken = 39801, "ER_SPRIG_FEED_BAD_RESUME_TOKEN", "SprigFeedBadResumeToken", SQLSTATE_GENERAL, false;
    /// A cluster's binlog settings fail the change feed's requirements.
    FeedSourceUnsupported = 39802, "ER_SPRIG_FEED_SOURCE_UNSUPPORTED", "SprigFeedSourceUnsupported", SQLSTATE_GENERAL, false;
    /// A feed subscription by that name already exists.
    FeedSubscriptionExists = 39804, "ER_SPRIG_FEED_SUBSCRIPTION_EXISTS", "SprigFeedSubscriptionExists", SQLSTATE_GENERAL, false;
    /// No feed subscription by that name.
    FeedSubscriptionNotFound = 39805, "ER_SPRIG_FEED_SUBSCRIPTION_NOT_FOUND", "SprigFeedSubscriptionNotFound", SQLSTATE_GENERAL, false;
    /// An internal feed consumer is behind all history and must start from
    /// a snapshot.
    FeedSnapshotRequired = 39808, "ER_SPRIG_FEED_SNAPSHOT_REQUIRED", "SprigFeedSnapshotRequired", SQLSTATE_GENERAL, false;
    /// Statement-format DML in a shard schema: the feed can't decode it.
    FeedStatementEvent = 39810, "ER_SPRIG_FEED_STATEMENT_EVENT", "SprigFeedStatementEvent", SQLSTATE_GENERAL, false;
    /// The tenant's map moved to another cluster but the move's handoff
    /// record isn't readable yet; the feed waits.
    FeedHandoffPending = 39813, "ER_SPRIG_FEED_HANDOFF_PENDING", "SprigFeedHandoffPending", SQLSTATE_GENERAL, true;
    /// No search index by that name.
    SearchIndexNotFound = 39820, "ER_SPRIG_SEARCH_INDEX_NOT_FOUND", "SprigSearchIndexNotFound", SQLSTATE_GENERAL, false;
    /// No search index covers the columns of a `MATCH`.
    SearchNoIndexForMatch = 39821, "ER_SPRIG_SEARCH_NO_INDEX_FOR_MATCH", "SprigSearchNoIndexForMatch", SQLSTATE_GENERAL, false;
    /// A search query string or operator is malformed.
    SearchQuerySyntax = 39822, "ER_SPRIG_SEARCH_QUERY_SYNTAX", "SprigSearchQuerySyntax", SQLSTATE_SYNTAX_ACCESS, false;
    /// A search feature SprigDB doesn't offer.
    SearchUnsupported = 39823, "ER_SPRIG_SEARCH_UNSUPPORTED", "SprigSearchUnsupported", SQLSTATE_GENERAL, false;
    /// The search index is still being built.
    SearchIndexNotReady = 39824, "ER_SPRIG_SEARCH_INDEX_NOT_READY", "SprigSearchIndexNotReady", SQLSTATE_RETRY, true;
    /// No ready copy of a search partition.
    SearchUnavailable = 39825, "ER_SPRIG_SEARCH_UNAVAILABLE", "SprigSearchUnavailable", SQLSTATE_RETRY, true;
    /// A search's consistency target wasn't reached in time.
    SearchWaitTimeout = 39826, "ER_SPRIG_SEARCH_WAIT_TIMEOUT", "SprigSearchWaitTimeout", SQLSTATE_RETRY, true;
    /// A search read more hits than `search.max_window`.
    SearchWindowExceeded = 39827, "ER_SPRIG_SEARCH_WINDOW_EXCEEDED", "SprigSearchWindowExceeded", SQLSTATE_GENERAL, false;
    /// A search analyzer or language that isn't defined.
    SearchAnalyzerUnknown = 39828, "ER_SPRIG_SEARCH_ANALYZER_UNKNOWN", "SprigSearchAnalyzerUnknown", SQLSTATE_GENERAL, false;
    /// A search field's type can't take that kind.
    SearchFieldType = 39829, "ER_SPRIG_SEARCH_FIELD_TYPE", "SprigSearchFieldType", SQLSTATE_GENERAL, false;
    /// More search indexes on one object than `search.max_indexes_per_object`.
    SearchIndexLimit = 39830, "ER_SPRIG_SEARCH_INDEX_LIMIT", "SprigSearchIndexLimit", SQLSTATE_GENERAL, false;
    /// No place may hold a search index's copies.
    SearchNoPlacement = 39831, "ER_SPRIG_SEARCH_NO_PLACEMENT", "SprigSearchNoPlacement", SQLSTATE_GENERAL, false;
    /// Warning: a search inside a transaction that wrote the object reads
    /// committed data only.
    SearchStaleInTxn = 39832, "ER_SPRIG_SEARCH_STALE_IN_TXN", "SprigSearchStaleInTxn", SQLSTATE_GENERAL, false;
    /// A LATERAL the gateway must apply itself has more outer rows or bytes than it handles.
    TranslateLimit = 39601, "ER_SPRIG_TRANSLATE_LIMIT", "SprigTranslateLimit", SQLSTATE_GENERAL, false;
    /// A view, routine, trigger or event uses something only the gateway can do, which MariaDB can't do while it runs stored code.
    NeedsGateway = 39602, "ER_SPRIG_NEEDS_GATEWAY", "SprigNeedsGateway", SQLSTATE_GENERAL, false;
    /// A new MongoDB cursor past `mongo.max_cursors_per_tenant` or the cursor memory budget.
    CursorLimit = 39630, "ER_SPRIG_CURSOR_LIMIT", "SprigCursorLimit", SQLSTATE_GENERAL, false;
    /// A document's encoded `_id` is over 1024 bytes.
    DocIdTooLong = 39660, "ER_SPRIG_DOC_ID_TOO_LONG", "SprigDocIdTooLong", SQLSTATE_GENERAL, false;
    /// A MongoDB feature not supported in this version.
    DocFeatureUnsupported = 39662, "ER_SPRIG_DOC_FEATURE_UNSUPPORTED", "SprigDocFeatureUnsupported", SQLSTATE_GENERAL, false;
    /// Reserved: an array value in a collection's shard key field (not raised in this version).
    ShardKeyArray = 39663, "ER_SPRIG_SHARD_KEY_ARRAY", "SprigShardKeyArray", SQLSTATE_GENERAL, false;
    /// Not every gateway confirmed a new array path within `doc.path_publish_timeout_ms`; nothing of the write committed.
    PathPublishTimeout = 39664, "ER_SPRIG_PATH_PUBLISH_TIMEOUT", "SprigPathPublishTimeout", SQLSTATE_GENERAL, true;
    /// A document nested deeper than MariaDB's JSON limit (32 levels).
    DocTooDeep = 39665, "ER_SPRIG_DOC_TOO_DEEP", "SprigDocTooDeep", SQLSTATE_GENERAL, false;
    /// Reserved: a MongoDB message too small to hold its header (not raised in this version).
    PacketTooSmall = 39666, "ER_SPRIG_PACKET_TOO_SMALL", "SprigPacketTooSmall", SQLSTATE_GENERAL, false;
    /// Reserved: a document SprigDB can't store (not raised in this version).
    DocInvalid = 39667, "ER_SPRIG_DOC_INVALID", "SprigDocInvalid", SQLSTATE_GENERAL, false;
    /// Reserved: an update that changes `_id` (not raised in this version).
    DocIdImmutable = 39668, "ER_SPRIG_DOC_ID_IMMUTABLE", "SprigDocIdImmutable", SQLSTATE_GENERAL, false;
    /// A Redis 7.2 command not supported in this version.
    RedisUnsupported = 39840, "ER_SPRIG_REDIS_UNSUPPORTED", "SprigRedisUnsupported", SQLSTATE_GENERAL, false;
    /// A Lua scripting feature not supported (SCRIPT DEBUG, bytecode).
    RedisLuaUnsupported = 39841, "ER_SPRIG_REDIS_LUA_UNSUPPORTED", "SprigRedisLuaUnsupported", SQLSTATE_GENERAL, false;
    /// A Redis administrative command SprigDB doesn't offer; manage it through SQL, the API or sprigctl.
    RedisRestricted = 39842, "ER_SPRIG_REDIS_RESTRICTED", "SprigRedisRestricted", SQLSTATE_GENERAL, false;
    /// MULTI queued more commands than `redis.max_multi_commands`; EXEC aborts.
    RedisMultiTooLarge = 39844, "ER_SPRIG_REDIS_MULTI_TOO_LARGE", "SprigRedisMultiTooLarge", SQLSTATE_GENERAL, false;
    /// More blocked clients than `redis.max_blocked_per_tenant`.
    RedisTooManyBlocked = 39845, "ER_SPRIG_REDIS_TOO_MANY_BLOCKED", "SprigRedisTooManyBlocked", SQLSTATE_GENERAL, true;
    /// A write kept meeting conflicting writes until its retries ran out (TRYAGAIN).
    RedisShardBusy = 39846, "ER_SPRIG_REDIS_SHARD_BUSY", "SprigRedisShardBusy", SQLSTATE_GENERAL, true;
    /// No pub/sub hub could be reached.
    RedisPubsubUnavailable = 39848, "ER_SPRIG_REDIS_PUBSUB_UNAVAILABLE", "SprigRedisPubsubUnavailable", SQLSTATE_GENERAL, true;
    /// A script ran past its time or memory budget; it was rolled back.
    RedisLuaBudgetExceeded = 39850, "ER_SPRIG_REDIS_LUA_BUDGET_EXCEEDED", "SprigRedisLuaBudgetExceeded", SQLSTATE_GENERAL, false;
    /// A script used a key on another shard that isn't in KEYS.
    RedisLuaUndeclaredKey = 39851, "ER_SPRIG_REDIS_LUA_UNDECLARED_KEY", "SprigRedisLuaUndeclaredKey", SQLSTATE_GENERAL, false;
    /// Unknown function, version or binding.
    LogicNotFound = 39860, "ER_SPRIG_LOGIC_NOT_FOUND", "SprigLogicNotFound", SQLSTATE_GENERAL, false;
    /// Parse or type error at deploy, with its location.
    LogicSyntax = 39861, "ER_SPRIG_LOGIC_SYNTAX", "SprigLogicSyntax", SQLSTATE_GENERAL, false;
    /// The code runs in no tier.
    LogicUnsupported = 39862, "ER_SPRIG_LOGIC_UNSUPPORTED", "SprigLogicUnsupported", SQLSTATE_GENERAL, false;
    /// A validation rejected the write; MongoDB 121.
    LogicValidationFailed = 39863, "ER_SPRIG_LOGIC_VALIDATION_FAILED", "SprigLogicValidationFailed", SQLSTATE_GENERAL, false;
    /// A budget ran out; the message names it.
    LogicBudgetExceeded = 39864, "ER_SPRIG_LOGIC_BUDGET_EXCEEDED", "SprigLogicBudgetExceeded", SQLSTATE_GENERAL, false;
    /// The code threw an error.
    LogicUserError = 39865, "ER_SPRIG_LOGIC_USER_ERROR", "SprigLogicUserError", SQLSTATE_GENERAL, false;
    /// Type error, overflow, division by zero, `sqlOne` with many rows.
    LogicRuntimeError = 39866, "ER_SPRIG_LOGIC_RUNTIME_ERROR", "SprigLogicRuntimeError", SQLSTATE_GENERAL, false;
    /// An outbound call to a host not on `logic.outbound_allowlist`, or
    /// to an internal address.
    LogicOutboundDenied = 39867, "ER_SPRIG_LOGIC_OUTBOUND_DENIED", "SprigLogicOutboundDenied", SQLSTATE_GENERAL, false;
    /// `SPRIG_TIER = 'mariadb'` but placement refuses.
    LogicTierUnavailable = 39868, "ER_SPRIG_LOGIC_TIER_UNAVAILABLE", "SprigLogicTierUnavailable", SQLSTATE_GENERAL, false;
    /// Concurrent deploys of one module.
    LogicVersionConflict = 39869, "ER_SPRIG_LOGIC_VERSION_CONFLICT", "SprigLogicVersionConflict", SQLSTATE_GENERAL, true;
    /// A staged rollout can't stage this version (other parameters,
    /// language or rights), or its candidate was rolled back by itself.
    LogicRolloutBlocked = 39870, "ER_SPRIG_LOGIC_ROLLOUT_BLOCKED", "SprigLogicRolloutBlocked", SQLSTATE_GENERAL, false;
    /// The logic engine's queue is full; retryable.
    LogicEngineBusy = 39871, "ER_SPRIG_LOGIC_ENGINE_BUSY", "SprigLogicEngineBusy", SQLSTATE_GENERAL, true;
    /// Nested `db.call` past `logic.max_call_depth`.
    LogicCallDepth = 39872, "ER_SPRIG_LOGIC_CALL_DEPTH", "SprigLogicCallDepth", SQLSTATE_GENERAL, false;
    /// Bad cron text or time zone of a schedule.
    LogicScheduleInvalid = 39873, "ER_SPRIG_LOGIC_SCHEDULE_INVALID", "SprigLogicScheduleInvalid", SQLSTATE_GENERAL, false;
    /// Arguments don't match the signature.
    LogicSignatureMismatch = 39874, "ER_SPRIG_LOGIC_SIGNATURE_MISMATCH", "SprigLogicSignatureMismatch", SQLSTATE_GENERAL, false;
    /// A drop of a function or procedure that a schedule, a sink's
    /// `TRANSFORM`, a computed column or other code uses.
    LogicInUse = 39875, "ER_SPRIG_LOGIC_IN_USE", "SprigLogicInUse", SQLSTATE_GENERAL, false;
    /// The function was placed in the engine tier, whose runtime this
    /// build doesn't have yet.
    LogicEngineUnavailable = 39876, "ER_SPRIG_LOGIC_ENGINE_UNAVAILABLE", "SprigLogicEngineUnavailable", SQLSTATE_GENERAL, false;
    /// A trigger on a stream found its consumer group already past the
    /// record: another run of the trigger committed it (the run is rolled
    /// back and skipped).
    LogicTriggerPositionMoved = 39877, "ER_SPRIG_LOGIC_TRIGGER_POSITION_MOVED", "SprigLogicTriggerPositionMoved", SQLSTATE_GENERAL, true;
    /// Studio is off, or not served on this listener.
    StudioDisabled = 39920, "ER_SPRIG_STUDIO_DISABLED", "SprigStudioDisabled", SQLSTATE_GENERAL, false;
    /// A plain-HTTP password login from a non-loopback client while
    /// `studio.allow_insecure_http` is off.
    StudioInsecureTransport = 39921, "ER_SPRIG_STUDIO_INSECURE_TRANSPORT", "SprigStudioInsecureTransport", SQLSTATE_GENERAL, false;
    /// A cookie request from another site: `Sec-Fetch-Site` or `Origin`.
    StudioOriginRejected = 39922, "ER_SPRIG_STUDIO_ORIGIN_REJECTED", "SprigStudioOriginRejected", SQLSTATE_GENERAL, false;
    /// A cookie request without `X-Sprig-CSRF`.
    StudioCsrfHeaderMissing = 39923, "ER_SPRIG_STUDIO_CSRF_HEADER_MISSING", "SprigStudioCsrfHeaderMissing", SQLSTATE_GENERAL, false;
    /// The token session was idle past `studio.session_idle_minutes`.
    StudioSessionIdle = 39924, "ER_SPRIG_STUDIO_SESSION_IDLE", "SprigStudioSessionIdle", SQLSTATE_GENERAL, false;
    /// The token session is older than `studio.session_max_hours`.
    StudioSessionExpired = 39925, "ER_SPRIG_STUDIO_SESSION_EXPIRED", "SprigStudioSessionExpired", SQLSTATE_GENERAL, false;
    /// Operator mode needs a session signed in with a second factor
    /// (`studio.operator_require_mfa`).
    StudioOperatorMfaRequired = 39926, "ER_SPRIG_STUDIO_OPERATOR_MFA_REQUIRED", "SprigStudioOperatorMfaRequired", SQLSTATE_GENERAL, false;
    /// Operator mode needs a verified client certificate
    /// (`studio.operator_mtls = required`).
    StudioOperatorCertRequired = 39927, "ER_SPRIG_STUDIO_OPERATOR_CERT_REQUIRED", "SprigStudioOperatorCertRequired", SQLSTATE_GENERAL, false;
    /// Operator mode is off (`studio.operator_mode`), or not open to this
    /// account (`studio.operator_logins`).
    StudioOperatorModeDisabled = 39928, "ER_SPRIG_STUDIO_OPERATOR_MODE_DISABLED", "SprigStudioOperatorModeDisabled", SQLSTATE_GENERAL, false;
    /// An export ticket is malformed, expired, or its session ended.
    StudioExportTicketInvalid = 39929, "ER_SPRIG_STUDIO_EXPORT_TICKET_INVALID", "SprigStudioExportTicketInvalid", SQLSTATE_GENERAL, false;
    /// The row-level security tester isn't allowed for this user: `studio.view_as`
    /// off, the caller not an administrator over the tenant, or the user an
    /// administrator over the caller's tenant.
    StudioViewAsDenied = 39930, "ER_SPRIG_STUDIO_VIEW_AS_DENIED", "SprigStudioViewAsDenied", SQLSTATE_GENERAL, false;
    /// The tester runs only one read-only statement.
    StudioViewAsNotReadOnly = 39931, "ER_SPRIG_STUDIO_VIEW_AS_NOT_READ_ONLY", "SprigStudioViewAsNotReadOnly", SQLSTATE_GENERAL, false;
    /// The binary was built without Studio's files.
    StudioNotBuilt = 39932, "ER_SPRIG_STUDIO_NOT_BUILT", "SprigStudioNotBuilt", SQLSTATE_GENERAL, false;
    /// No open cursor has this name, or it belongs to another user.
    StudioCursorUnknown = 39934, "ER_SPRIG_STUDIO_CURSOR_UNKNOWN", "SprigStudioCursorUnknown", SQLSTATE_GENERAL, false;
    /// The cursor was idle past `studio.cursor_idle_seconds` and closed;
    /// open it again.
    StudioCursorExpired = 39935, "ER_SPRIG_STUDIO_CURSOR_EXPIRED", "SprigStudioCursorExpired", SQLSTATE_GENERAL, false;
    /// Unknown stream or topic.
    StreamNotFound = 39940, "ER_SPRIG_STREAM_NOT_FOUND", "SprigStreamNotFound", SQLSTATE_GENERAL, false;
    /// `UPDATE` or `DELETE` on a stream; a produce to a change topic.
    StreamAppendOnly = 39941, "ER_SPRIG_STREAM_APPEND_ONLY", "SprigStreamAppendOnly", SQLSTATE_GENERAL, false;
    /// A stream name in use.
    StreamExists = 39942, "ER_SPRIG_STREAM_EXISTS", "SprigStreamExists", SQLSTATE_GENERAL, false;
    /// Partitions can only grow.
    StreamPartitionsDecrease = 39943, "ER_SPRIG_STREAM_PARTITIONS_DECREASE", "SprigStreamPartitionsDecrease", SQLSTATE_GENERAL, false;
    /// A record above `stream.max_message_bytes`, or a key above 4096
    /// bytes.
    StreamRecordTooLarge = 39944, "ER_SPRIG_STREAM_RECORD_TOO_LARGE", "SprigStreamRecordTooLarge", SQLSTATE_GENERAL, false;
    /// The event key doesn't determine the attached group's distribution
    /// key.
    StreamKeyNotAttached = 39945, "ER_SPRIG_STREAM_KEY_NOT_ATTACHED", "SprigStreamKeyNotAttached", SQLSTATE_GENERAL, false;
    /// No offset assigned within `stream.produce_wait_ms`.
    StreamSequencingTimeout = 39946, "ER_SPRIG_STREAM_SEQUENCING_TIMEOUT", "SprigStreamSequencingTimeout", SQLSTATE_GENERAL, true;
    /// An offset below the log start or above the high watermark.
    StreamOffsetOutOfRange = 39947, "ER_SPRIG_STREAM_OFFSET_OUT_OF_RANGE", "SprigStreamOffsetOutOfRange", SQLSTATE_GENERAL, false;
    /// Reserved: a failover lost records a consumer had already seen (not raised in this version).
    StreamTruncated = 39948, "ER_SPRIG_STREAM_TRUNCATED", "SprigStreamTruncated", SQLSTATE_GENERAL, false;
    /// A Kafka produce with acks=0: SprigDB always acknowledges after commit.
    KafkaAcksZero = 39950, "ER_SPRIG_KAFKA_ACKS_ZERO", "SprigKafkaAcksZero", SQLSTATE_GENERAL, false;
    /// A Kafka producer with `transactional.id`: Kafka transactions aren't supported.
    KafkaTransactionsUnsupported = 39951, "ER_SPRIG_KAFKA_TRANSACTIONS_UNSUPPORTED", "SprigKafkaTransactionsUnsupported", SQLSTATE_GENERAL, false;
    /// A Kafka API or version SprigDB doesn't serve.
    KafkaUnsupportedApi = 39952, "ER_SPRIG_KAFKA_UNSUPPORTED_API", "SprigKafkaUnsupportedApi", SQLSTATE_GENERAL, false;
    /// Kafka SASL authentication failed.
    KafkaSaslFailed = 39953, "ER_SPRIG_KAFKA_SASL_FAILED", "SprigKafkaSaslFailed", SQLSTATE_GENERAL, false;
    /// Reserved: a consumer group moved to another gateway (not raised in this version; Kafka's `NOT_COORDINATOR` is sent).
    KafkaNotCoordinator = 39954, "ER_SPRIG_KAFKA_NOT_COORDINATOR", "SprigKafkaNotCoordinator", SQLSTATE_GENERAL, true;
    /// Reserved: over a tenant quota (not raised in this version).
    KafkaQuota = 39956, "ER_SPRIG_KAFKA_QUOTA", "SprigKafkaQuota", SQLSTATE_GENERAL, true;
    /// A record without a key on a compacted stream.
    StreamCompactNeedsKey = 39955, "ER_SPRIG_STREAM_COMPACT_NEEDS_KEY", "SprigStreamCompactNeedsKey", SQLSTATE_GENERAL, false;
    /// `sprig.commit_offset` inside a transaction the gateway doesn't
    /// coordinate, with the tenant catalog on another cluster than the
    /// transaction's.
    StreamOffsetTxnUnsupported = 39958, "ER_SPRIG_STREAM_OFFSET_TXN_UNSUPPORTED", "SprigStreamOffsetTxnUnsupported", SQLSTATE_GENERAL, false;
    /// Under `residency.cross_tenant_messaging_strict`, a publish from a
    /// pinned tenant into another tenant's stream outside its allowed data
    /// centers.
    StreamResidency = 39949, "ER_SPRIG_STREAM_RESIDENCY", "SprigStreamResidency", SQLSTATE_GENERAL, false;
    /// A descendant's publish into an ancestor's stream with a key other
    /// than its own tenant name, under `stream.descendant_key = origin`.
    StreamOriginKey = 39957, "ER_SPRIG_STREAM_ORIGIN_KEY", "SprigStreamOriginKey", SQLSTATE_GENERAL, false;
    /// A Cypher, GQL or graph DDL text that doesn't parse; Bolt
    /// `Neo.ClientError.Statement.SyntaxError`.
    GraphSyntax = 39900, "ER_SPRIG_GRAPH_SYNTAX", "SprigGraphSyntax", SQLSTATE_SYNTAX_ACCESS, false;
    /// A graph feature this build doesn't have; the message names it.
    GraphUnsupported = 39901, "ER_SPRIG_GRAPH_UNSUPPORTED", "SprigGraphUnsupported", SQLSTATE_GENERAL, false;
    /// No graph by that name; Bolt
    /// `Neo.ClientError.Database.DatabaseNotFound`.
    GraphNotFound = 39902, "ER_SPRIG_GRAPH_NOT_FOUND", "SprigGraphNotFound", SQLSTATE_GENERAL, false;
    /// A traversal went past a budget: hops, frontier, rows, time or memory;
    /// the message names it.
    GraphBudgetExceeded = 39903, "ER_SPRIG_GRAPH_BUDGET_EXCEEDED", "SprigGraphBudgetExceeded", SQLSTATE_GENERAL, false;
    /// A write to a mapped graph's read-only source, or to the labels of a
    /// mapped node.
    GraphReadOnlySource = 39904, "ER_SPRIG_GRAPH_READ_ONLY_SOURCE", "SprigGraphReadOnlySource", SQLSTATE_GENERAL, false;
    /// `id()` of an element whose id hashes its key can't be looked up:
    /// use `elementId()`.
    GraphIdNotRoutable = 39905, "ER_SPRIG_GRAPH_ID_NOT_ROUTABLE", "SprigGraphIdNotRoutable", SQLSTATE_GENERAL, false;
    /// `DELETE` of a node that still has relationships, checked at commit;
    /// Bolt `Neo.ClientError.Schema.ConstraintValidationFailed`.
    GraphNodeHasRelationships = 39906, "ER_SPRIG_GRAPH_NODE_HAS_RELATIONSHIPS", "SprigGraphNodeHasRelationships", SQLSTATE_GENERAL, false;
    /// `DETACH DELETE` past `graph.max_detach_edges`.
    GraphDetachTooLarge = 39907, "ER_SPRIG_GRAPH_DETACH_TOO_LARGE", "SprigGraphDetachTooLarge", SQLSTATE_GENERAL, false;
    /// A graph definition with a bad name, key or reference.
    GraphDefinitionInvalid = 39908, "ER_SPRIG_GRAPH_DEFINITION_INVALID", "SprigGraphDefinitionInvalid", SQLSTATE_GENERAL, false;
    /// A value that isn't a Neo4j property type, such as a map; Bolt
    /// `Neo.ClientError.Statement.TypeError`.
    GraphPropertyType = 39909, "ER_SPRIG_GRAPH_PROPERTY_TYPE", "SprigGraphPropertyType", SQLSTATE_GENERAL, false;
    /// No Bolt version in common with the client: the handshake answers
    /// `00 00 00 00` (logged only).
    BoltVersionUnsupported = 39912, "ER_SPRIG_BOLT_VERSION_UNSUPPORTED", "SprigBoltVersionUnsupported", SQLSTATE_CONNECTION, false;
    /// A Bolt message out of order or malformed; Bolt
    /// `Neo.ClientError.Request.Invalid`.
    BoltProtocol = 39914, "ER_SPRIG_BOLT_PROTOCOL", "SprigBoltProtocol", SQLSTATE_CONNECTION, false;
    /// A Bolt authentication scheme other than `basic` and `bearer`, or bad
    /// credentials; Bolt `Neo.ClientError.Security.Unauthorized`.
    BoltAuthScheme = 39915, "ER_SPRIG_BOLT_AUTH_SCHEME", "SprigBoltAuthScheme", SQLSTATE_SYNTAX_ACCESS, false;
    /// `imp_user` (impersonation) is refused; Bolt
    /// `Neo.ClientError.Security.Forbidden`.
    BoltImpersonation = 39916, "ER_SPRIG_BOLT_IMPERSONATION", "SprigBoltImpersonation", SQLSTATE_SYNTAX_ACCESS, false;
    /// A Bolt bookmark that doesn't parse, carries a bad signature or names
    /// another tenant; Bolt `Neo.ClientError.Transaction.InvalidBookmark`.
    BoltBookmarkInvalid = 39917, "ER_SPRIG_BOLT_BOOKMARK_INVALID", "SprigBoltBookmarkInvalid", SQLSTATE_GENERAL, false;
    /// A Bolt result kept unconsumed past `bolt.max_buffered_result_mb`.
    BoltResultBufferLimit = 39918, "ER_SPRIG_BOLT_RESULT_BUFFER_LIMIT", "SprigBoltResultBufferLimit", SQLSTATE_GENERAL, false;
    /// The audit spool is full (`audit.spool_max_mb`): admin API mutations
    /// and account statements fail closed.
    AuditUnavailable = 39970, "ER_SPRIG_AUDIT_UNAVAILABLE", "SprigAuditUnavailable", SQLSTATE_GENERAL, true;
    /// A tenant import's checksums or grants differ from its export's
    /// manifest.
    ImportVerifyFailed = 39971, "ER_SPRIG_IMPORT_VERIFY_FAILED", "SprigImportVerifyFailed", SQLSTATE_GENERAL, false;
    /// No recommendation with that id.
    RecommendationNotFound = 39972, "ER_SPRIG_RECOMMENDATION_NOT_FOUND", "SprigRecommendationNotFound", SQLSTATE_GENERAL, false;
    /// The action doesn't apply in the recommendation's state.
    RecommendationState = 39973, "ER_SPRIG_RECOMMENDATION_STATE", "SprigRecommendationState", SQLSTATE_GENERAL, false;
    /// The caller may not act at the recommendation's level.
    ApprovalDenied = 39974, "ER_SPRIG_APPROVAL_DENIED", "SprigApprovalDenied", SQLSTATE_SYNTAX_ACCESS, false;
    /// Re-validation failed before applying; the recommendation expired.
    RecommendationStale = 39975, "ER_SPRIG_RECOMMENDATION_STALE", "SprigRecommendationStale", SQLSTATE_GENERAL, false;
    /// An approval window that is empty, past or longer than 30 days.
    ApprovalWindowInvalid = 39976, "ER_SPRIG_APPROVAL_WINDOW_INVALID", "SprigApprovalWindowInvalid", SQLSTATE_GENERAL, false;
    /// A timer or kind list that doesn't parse.
    ApprovalPolicyInvalid = 39977, "ER_SPRIG_APPROVAL_POLICY_INVALID", "SprigApprovalPolicyInvalid", SQLSTATE_GENERAL, false;
    /// A test email or webhook failed.
    NotifyFailed = 39978, "ER_SPRIG_NOTIFY_FAILED", "SprigNotifyFailed", SQLSTATE_GENERAL, false;
    /// sprig: the profiles file is unreadable or invalid, or names no such profile.
    ShellProfileInvalid = 39980, "ER_SPRIG_SHELL_PROFILE_INVALID", "SprigShellProfileInvalid", SQLSTATE_GENERAL, false;
    /// sprig: the keychain is unusable, or `secrets.toml` is readable by others.
    ShellSecretUnavailable = 39981, "ER_SPRIG_SHELL_SECRET_UNAVAILABLE", "SprigShellSecretUnavailable", SQLSTATE_GENERAL, false;
    /// sprig: a destructive statement in the CLI without `--yes`.
    ShellConfirmationRequired = 39982, "ER_SPRIG_SHELL_CONFIRMATION_REQUIRED", "SprigShellConfirmationRequired", SQLSTATE_GENERAL, false;
    /// sprig: the language can't go over the chosen transport.
    ShellLanguageUnsupported = 39983, "ER_SPRIG_SHELL_LANGUAGE_UNSUPPORTED", "SprigShellLanguageUnsupported", SQLSTATE_GENERAL, false;
    /// sprig: `sprig admin` found no `sprigctl`.
    ShellSprigctlMissing = 39984, "ER_SPRIG_SHELL_SPRIGCTL_MISSING", "SprigShellSprigctlMissing", SQLSTATE_GENERAL, false;
    /// sprig: input the shell can't split or parse.
    ShellParse = 39985, "ER_SPRIG_SHELL_PARSE", "SprigShellParse", SQLSTATE_GENERAL, false;
    /// sprig: the user cancelled the statement.
    ShellCancelled = 39986, "ER_SPRIG_SHELL_CANCELLED", "SprigShellCancelled", SQLSTATE_GENERAL, false;
    /// sprig: the gateway or native endpoint can't be reached.
    ShellUnreachable = 39987, "ER_SPRIG_SHELL_UNREACHABLE", "SprigShellUnreachable", SQLSTATE_GENERAL, false;
    /// sprig: a password login over plain HTTP to another host.
    ShellInsecureTransport = 39988, "ER_SPRIG_SHELL_INSECURE_TRANSPORT", "SprigShellInsecureTransport", SQLSTATE_GENERAL, false;
    /// Docs search can't be answered: docs are turned off (`docs.enabled`),
    /// no cluster has this release's docs pack loaded yet, or (for
    /// `sprig_docs.embed`) the embedding model isn't available.
    DocsUnavailable = 39989, "ER_SPRIG_DOCS_UNAVAILABLE", "SprigDocsUnavailable", SQLSTATE_RETRY, true;
    /// `sprigdb.toml` or its environment overrides are invalid. Raised only at
    /// process start; never reaches a client.
    ConfigInvalid = 39998, "ER_SPRIG_CONFIG_INVALID", "SprigConfigInvalid", SQLSTATE_GENERAL, false;
    /// An internal invariant failed: a bug. Logged at error level with a
    /// trace id.
    Internal = 39999, "ER_SPRIG_INTERNAL", "SprigInternal", SQLSTATE_GENERAL, false;
}

impl SprigCode {
    /// Codes this version defines but never raises, each with a one-line
    /// note: the generated reference lists them as reserved. A code leaves
    /// this list when code starts raising it.
    pub const RESERVED: &'static [(SprigCode, &'static str)] = &[
        (
            SprigCode::MqTxnNeedsXa,
            "Reserved: an ack inside a transaction that would add a cluster without XA; not raised in this version.",
        ),
        (
            SprigCode::MqResidency,
            "Reserved: a queue login or publish outside the tenant's allowed data centers; not raised in this version.",
        ),
        (
            SprigCode::MqStreamOffset,
            "Reserved: a bad x-stream-offset; stream queues aren't served in this version.",
        ),
        (
            SprigCode::MqMgmtUnsupported,
            "Reserved: a RabbitMQ management endpoint SprigDB doesn't serve; not raised in this version.",
        ),
        (
            SprigCode::ShardKeyArray,
            "Reserved: an array value in a collection's shard key field; not raised in this version.",
        ),
        (
            SprigCode::PacketTooSmall,
            "Reserved: a MongoDB message too short for its header; not raised in this version.",
        ),
        (
            SprigCode::DocInvalid,
            "Reserved: a document SprigDB can't store; not raised in this version.",
        ),
        (
            SprigCode::DocIdImmutable,
            "Reserved: an update that changes _id; not raised in this version.",
        ),
        (
            SprigCode::VectorTooManyIndexes,
            "Reserved: more vector indexes on a table than vector.max_indexes_per_table; not raised in this version.",
        ),
        (
            SprigCode::DimTzUnavailable,
            "Reserved: a named time zone without MariaDB's time zone tables; not raised in this version.",
        ),
        (
            SprigCode::DimVersionedTruncate,
            "Reserved: TRUNCATE of a versioned dimension; not raised in this version.",
        ),
        (
            SprigCode::DimHistoryPurged,
            "Reserved: an as_of older than the history kept; not raised in this version.",
        ),
        (
            SprigCode::GeoSridUnsupported,
            "Reserved: a geographic SRID other than 4326; not raised in this version.",
        ),
        (
            SprigCode::TenantAmbiguous,
            "Reserved: a token whose issuer several tenants trust, with no SNI or x-sprig-tenant to pick one; token logins aren't offered in this version.",
        ),
        (
            SprigCode::SearchNoPlacement,
            "Reserved: no worker in an allowed data center can hold a search index's copies; not raised in this version.",
        ),
        (
            SprigCode::SearchStaleInTxn,
            "Reserved: a warning for a search inside a transaction that wrote the object; not raised in this version.",
        ),
        (
            SprigCode::StreamTruncated,
            "Reserved: a failover lost records a consumer had already read; not raised in this version.",
        ),
        (
            SprigCode::KafkaNotCoordinator,
            "Reserved: a consumer group moved to another gateway; this version sends Kafka's own NOT_COORDINATOR.",
        ),
        (
            SprigCode::KafkaQuota,
            "Reserved: a Kafka client over a tenant quota; not raised in this version.",
        ),
    ];

    /// The note of a code this version defines but never raises.
    #[must_use]
    pub fn reserved(self) -> Option<&'static str> {
        Self::RESERVED
            .iter()
            .find(|(c, _)| *c == self)
            .map(|(_, note)| *note)
    }

    /// The code of a number, if the registry has one.
    #[must_use]
    pub fn from_number(n: u16) -> Option<Self> {
        Self::ALL.iter().copied().find(|c| c.number() == n)
    }

    /// The name without its `ER_SPRIG_` prefix, as the Redis and Kafka
    /// front ends print it (`SPRIG <NAME> <number> ...`).
    #[must_use]
    pub fn short_name(self) -> &'static str {
        let n = self.name();
        n.strip_prefix("ER_SPRIG_").unwrap_or(n)
    }
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

    /// How this error shows up in each protocol (30 and 37).
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
    // Errors with a standard MongoDB equivalent use it.
    let (mongo_code, mongo_code_name, mongo_labels): (i32, &'static str, &'static [&str]) =
        match code {
            SprigCode::GatewayDraining => (91, "ShutdownInProgress", &["RetryableWriteError"]),
            SprigCode::DcFenced | SprigCode::RecoveryFenced => {
                (10107, "NotWritablePrimary", &["RetryableWriteError"])
            }
            SprigCode::LogicValidationFailed => (121, "DocumentValidationFailure", &[]),
            _ => (i32::from(code.number()), code.mongo_code_name(), &[]),
        };
    // gRPC and HTTP statuses per code.
    let (grpc_status, http_status) = match code {
        SprigCode::SettingOutOfBounds
        | SprigCode::TypeMultipleInheritance
        | SprigCode::TypeFieldConflict
        | SprigCode::TypeAbstract
        | SprigCode::TypeColumnReadOnly
        | SprigCode::TypeDiscriminatorMismatch
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
        | SprigCode::SearchNoIndexForMatch
        | SprigCode::SearchQuerySyntax
        | SprigCode::SearchWindowExceeded
        | SprigCode::SearchAnalyzerUnknown
        | SprigCode::SearchFieldType
        | SprigCode::DimUnknownMember
        | SprigCode::DimInvalidModel
        | SprigCode::DimTzUnavailable
        | SprigCode::GeoInvalid
        | SprigCode::GeoSridUnsupported
        | SprigCode::GeoUnsupportedExpression
        | SprigCode::GeoTooComplex
        | SprigCode::ApprovalWindowInvalid
        | SprigCode::StudioViewAsNotReadOnly
        | SprigCode::RlsPolicyInvalid
        | SprigCode::ApprovalPolicyInvalid
        | SprigCode::MqPrecondition
        | SprigCode::MqDeliveryTokenInvalid
        | SprigCode::MqDelayOutOfRange
        | SprigCode::MqStreamOffset
        | SprigCode::MqDirectReplyTo
        | SprigCode::MqUnroutable
        | SprigCode::AmqpFrameError
        | SprigCode::AmqpProtocolHeader
        | SprigCode::DocIdTooLong
        | SprigCode::DocTooDeep
        | SprigCode::DocInvalid
        | SprigCode::ShardKeyArray
        | SprigCode::PacketTooSmall
        | SprigCode::DocIdImmutable
        | SprigCode::RedisLuaUndeclaredKey
        | SprigCode::AccountAsRole
        | SprigCode::KafkaAcksZero
        | SprigCode::ShellParse
        | SprigCode::ShellProfileInvalid
        | SprigCode::ShellLanguageUnsupported => (Code::InvalidArgument, 400),
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
        | SprigCode::HeldBranchStatement
        | SprigCode::MqTxnNeedsXa
        | SprigCode::NeedsGateway
        | SprigCode::PrivilegeDropped
        | SprigCode::AuthPropagationPending
        | SprigCode::RlsDefinerDefault => (Code::FailedPrecondition, 400),
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
        | SprigCode::SearchUnavailable
        | SprigCode::DocsUnavailable
        | SprigCode::SearchWaitTimeout
        | SprigCode::SearchIndexNotReady
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
        | SprigCode::NotifyFailed
        | SprigCode::HeldBranchFenced
        | SprigCode::Phase2Pending
        | SprigCode::PathPublishTimeout
        | SprigCode::RedisPubsubUnavailable
        | SprigCode::KafkaNotCoordinator
        | SprigCode::ShellUnreachable => (Code::Unavailable, 503),
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
        | SprigCode::SinkFenced
        | SprigCode::MqOwnerMoved
        | SprigCode::RedisShardBusy => (Code::Aborted, 409),
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
        | SprigCode::SearchIndexNotFound
        | SprigCode::DimModelNotFound
        | SprigCode::StudioDisabled
        | SprigCode::StudioCursorUnknown
        | SprigCode::RecommendationNotFound
        | SprigCode::OperatorNotFound
        | SprigCode::MqNotFound
        | SprigCode::Amqp10LinkRefused
        | SprigCode::ShellSprigctlMissing => (Code::NotFound, 404),
        SprigCode::ClusterExists
        | SprigCode::TenantExists
        | SprigCode::FeedSubscriptionExists
        | SprigCode::SpaceExists
        | SprigCode::StreamExists
        | SprigCode::SinkExists
        | SprigCode::KvKeyExists
        | SprigCode::OperatorExists => (Code::AlreadyExists, 409),
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
        | SprigCode::RlsPolicyCycle
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
        | SprigCode::SearchUnsupported
        | SprigCode::SearchIndexLimit
        | SprigCode::SearchNoPlacement
        | SprigCode::SearchStaleInTxn
        | SprigCode::LogicRolloutBlocked
        | SprigCode::LogicInUse
        | SprigCode::RecommendationStale
        | SprigCode::MqResourceLocked
        | SprigCode::MqAlreadySettled
        | SprigCode::StreamTruncated => (Code::FailedPrecondition, 409),
        SprigCode::TenantSuspended
        | SprigCode::RemoteAccessDenied
        | SprigCode::DefinerRefused
        | SprigCode::LogicOutboundDenied
        | SprigCode::BoltImpersonation
        | SprigCode::RlsUnsupported
        | SprigCode::RlsCheckFailed
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
        | SprigCode::ApprovalDenied
        | SprigCode::OperatorForbidden
        | SprigCode::MqAccessRefused
        | SprigCode::MqResidency
        | SprigCode::ShellConfirmationRequired
        | SprigCode::ShellInsecureTransport => (Code::PermissionDenied, 403),
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
        | SprigCode::RateLimited
        | SprigCode::StreamSlowConsumer
        | SprigCode::MqQueueFull
        | SprigCode::MqQuota
        | SprigCode::MqVhostLimit
        | SprigCode::AmqpChannelLimit
        | SprigCode::CursorLimit
        | SprigCode::RedisMultiTooLarge
        | SprigCode::RedisTooManyBlocked
        | SprigCode::RedisLuaBudgetExceeded
        | SprigCode::KafkaQuota
        | SprigCode::TranslateLimit => (Code::ResourceExhausted, 429),
        SprigCode::PlacementUnavailable | SprigCode::ProvisionNoHosts | SprigCode::NoDcCapacity => {
            (Code::ResourceExhausted, 503)
        }
        SprigCode::RequestTooLarge
        | SprigCode::StreamRecordTooLarge
        | SprigCode::SinkRecordTooLarge
        | SprigCode::MqMessageTooLarge => (Code::InvalidArgument, 413),
        SprigCode::StreamOffsetOutOfRange => (Code::OutOfRange, 416),
        SprigCode::TransportUnsupported => (Code::FailedPrecondition, 415),
        SprigCode::AdminUnauthenticated
        | SprigCode::OperatorLoginRequired
        | SprigCode::OperatorMfaRequired
        | SprigCode::OperatorLoginFailed
        | SprigCode::OperatorTokenExpired
        | SprigCode::BoltAuthScheme
        | SprigCode::SessionRevoked
        | SprigCode::TenantAmbiguous
        | SprigCode::StudioSessionIdle
        | SprigCode::StudioSessionExpired
        | SprigCode::StudioExportTicketInvalid
        | SprigCode::AmqpSaslFailed
        | SprigCode::KafkaSaslFailed
        | SprigCode::ShellSecretUnavailable => (Code::Unauthenticated, 401),
        // Protocol front ends' and the shell's own codes.
        SprigCode::MqNotImplemented
        | SprigCode::Amqp10Unsupported
        | SprigCode::MqMgmtUnsupported
        | SprigCode::DocFeatureUnsupported
        | SprigCode::RedisUnsupported
        | SprigCode::RedisLuaUnsupported
        | SprigCode::RedisRestricted
        | SprigCode::KafkaTransactionsUnsupported
        | SprigCode::KafkaUnsupportedApi => (Code::Unimplemented, 501),
        SprigCode::MqConfirmTimeout | SprigCode::MqConsumerTimeout => (Code::DeadlineExceeded, 504),
        SprigCode::ShellCancelled => (Code::Cancelled, 499),
        SprigCode::MqDeadLetterCycle | SprigCode::ConfigInvalid | SprigCode::Internal => {
            (Code::Internal, 500)
        }
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
    // MongoDB, gRPC and HTTP equivalents of common MariaDB errors. Other
    // MariaDB errors map to MongoDB's generic `UnknownError` (code 8) until
    // the MongoDB front end refines them.
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
