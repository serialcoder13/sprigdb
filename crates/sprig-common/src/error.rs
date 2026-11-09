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
    /// gRPC over HTTP/1.1, or another transport the call can't use (chapter
    /// 37).
    TransportUnsupported = 39791, "ER_SPRIG_TRANSPORT_UNSUPPORTED", "SprigTransportUnsupported", SQLSTATE_GENERAL, false;
    /// An Admin API request is malformed or names something invalid
    /// (chapter 37). Not yet in Appendix E.
    AdminRequestInvalid = 39792, "ER_SPRIG_ADMIN_REQUEST_INVALID", "SprigAdminRequestInvalid", SQLSTATE_GENERAL, false;
    /// An Admin API request has no valid credential (chapter 37). Not yet
    /// in Appendix E.
    AdminUnauthenticated = 39793, "ER_SPRIG_ADMIN_UNAUTHENTICATED", "SprigAdminUnauthenticated", SQLSTATE_GENERAL, false;
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
    /// Bad cron text or time zone of a schedule (chapter 41).
    LogicScheduleInvalid = 39873, "ER_SPRIG_LOGIC_SCHEDULE_INVALID", "SprigLogicScheduleInvalid", SQLSTATE_GENERAL, false;
    /// Arguments don't match the signature (chapter 41).
    LogicSignatureMismatch = 39874, "ER_SPRIG_LOGIC_SIGNATURE_MISMATCH", "SprigLogicSignatureMismatch", SQLSTATE_GENERAL, false;
    /// The function was placed in the engine tier, whose runtime this
    /// build doesn't have yet (chapter 41, as built).
    LogicEngineUnavailable = 39876, "ER_SPRIG_LOGIC_ENGINE_UNAVAILABLE", "SprigLogicEngineUnavailable", SQLSTATE_GENERAL, false;
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
    /// The audit spool is full (`audit.spool_max_mb`): admin API mutations
    /// and account statements fail closed (chapter 47).
    AuditUnavailable = 39970, "ER_SPRIG_AUDIT_UNAVAILABLE", "SprigAuditUnavailable", SQLSTATE_GENERAL, true;
    /// A tenant import's checksums or grants differ from its export's
    /// manifest (chapter 47).
    ImportVerifyFailed = 39971, "ER_SPRIG_IMPORT_VERIFY_FAILED", "SprigImportVerifyFailed", SQLSTATE_GENERAL, false;
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
        | SprigCode::LogicSyntax
        | SprigCode::LogicSignatureMismatch
        | SprigCode::LogicScheduleInvalid
        | SprigCode::LogicValidationFailed
        | SprigCode::FeedBadResumeToken
        | SprigCode::InheritKeyUpdate
        | SprigCode::ShareFilterInvalid
        | SprigCode::ResidencyInvalid
        | SprigCode::StreamPartitionsDecrease
        | SprigCode::StreamKeyNotAttached
        | SprigCode::StreamCompactNeedsKey => (Code::InvalidArgument, 400),
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
        | SprigCode::DdlShardDrift => (Code::FailedPrecondition, 400),
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
        | SprigCode::StreamSequencingTimeout => (Code::Unavailable, 503),
        SprigCode::CatalogConflict
        | SprigCode::TenantTreeBusy
        | SprigCode::TxnHandleUnknown
        | SprigCode::TxnHandleExpired
        | SprigCode::ClusterFenceFailed
        | SprigCode::OutcomeUnknown
        | SprigCode::SessionStateLost
        | SprigCode::ShardMoved
        | SprigCode::ShardLocked
        | SprigCode::ShardBusy
        | SprigCode::LogicUserError
        | SprigCode::LogicRuntimeError
        | SprigCode::LogicVersionConflict
        | SprigCode::MoveCutoverAborted
        | SprigCode::ShardMapChanged
        | SprigCode::DdlFailed => (Code::Aborted, 409),
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
        | SprigCode::LogicNotFound
        | SprigCode::JobNotFound
        | SprigCode::TxnNotInDoubt
        | SprigCode::SpaceNotFound
        | SprigCode::ShareNotFound
        | SprigCode::StreamNotFound => (Code::NotFound, 404),
        SprigCode::ClusterExists
        | SprigCode::TenantExists
        | SprigCode::FeedSubscriptionExists
        | SprigCode::SpaceExists
        | SprigCode::StreamExists => (Code::AlreadyExists, 409),
        SprigCode::ClusterStateInvalid
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
        | SprigCode::JobStateInvalid
        | SprigCode::SpaceHasChildren
        | SprigCode::SpaceDepthExceeded
        | SprigCode::NotInheritable
        | SprigCode::ShareResidencyConflict
        | SprigCode::ShareNotResharable
        | SprigCode::InheritStatementUnsupported
        | SprigCode::InheritSchemaMismatch
        | SprigCode::InheritedObjectInUse
        | SprigCode::AuthorizerUnknownToGateways
        | SprigCode::StreamAppendOnly => (Code::FailedPrecondition, 409),
        SprigCode::TenantSuspended
        | SprigCode::RemoteAccessDenied
        | SprigCode::DefinerRefused
        | SprigCode::LogicOutboundDenied
        | SprigCode::RlsUnsupported
        | SprigCode::TxnHandleForbidden
        | SprigCode::SpaceAccessDenied => (Code::PermissionDenied, 403),
        SprigCode::LoginRateLimited
        | SprigCode::TenantConnectionLimit
        | SprigCode::AdmissionTimeout
        | SprigCode::MemoryLimit
        | SprigCode::LogicBudgetExceeded
        | SprigCode::RouterMemoryLimit => (Code::ResourceExhausted, 429),
        SprigCode::PlacementUnavailable | SprigCode::ProvisionNoHosts => {
            (Code::ResourceExhausted, 503)
        }
        SprigCode::RequestTooLarge | SprigCode::StreamRecordTooLarge => {
            (Code::InvalidArgument, 413)
        }
        SprigCode::StreamOffsetOutOfRange => (Code::OutOfRange, 416),
        SprigCode::TransportUnsupported => (Code::FailedPrecondition, 415),
        SprigCode::AdminUnauthenticated
        | SprigCode::SessionRevoked
        | SprigCode::TenantAmbiguous => (Code::Unauthenticated, 401),
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
        1044 | 1142 | 1143 => (Code::PermissionDenied, 403),
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
