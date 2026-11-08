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
    /// A tenant with children can't be dropped (chapter 29; chapter 23's
    /// delete guard).
    TenantHasChildren = 39362, "ER_SPRIG_TENANT_HAS_CHILDREN", "SprigTenantHasChildren", SQLSTATE_GENERAL, false;
    /// The control plane's store is unreachable (chapter 23).
    MetaUnavailable = 39500, "ER_SPRIG_META_UNAVAILABLE", "SprigMetaUnavailable", SQLSTATE_CONNECTION, true;
    /// This controller no longer leads (or never did): its session was lost
    /// or a guarded transaction failed its fencing-token compare. The caller
    /// steps down; it must not retry (chapter 23). Not yet in Appendix E.
    NotLeader = 39504, "ER_SPRIG_NOT_LEADER", "SprigNotLeader", SQLSTATE_GENERAL, false;
    /// The data center isn't registered (chapter 24).
    UnknownDc = 39503, "ER_SPRIG_UNKNOWN_DC", "SprigUnknownDc", SQLSTATE_GENERAL, false;
    /// A tenant would be deeper than `tenant.max_depth` (chapter 23).
    TenantDepthExceeded = 39508, "ER_SPRIG_TENANT_DEPTH_EXCEEDED", "SprigTenantDepthExceeded", SQLSTATE_GENERAL, false;
    /// A tree operation on a subtree with an open move (chapter 23).
    TenantTreeBusy = 39509, "ER_SPRIG_TENANT_TREE_BUSY", "SprigTenantTreeBusy", SQLSTATE_GENERAL, true;
    /// The provisioner doesn't support the operation (chapter 27).
    ProvisionerUnsupported = 39510, "ER_SPRIG_PROVISIONER_UNSUPPORTED", "SprigProvisionerUnsupported", SQLSTATE_GENERAL, false;
    /// An external cluster failed its registration prechecks; the message
    /// lists every missing item (chapter 27). Not yet in Appendix E.
    ClusterPrecheckFailed = 39511, "ER_SPRIG_CLUSTER_PRECHECK_FAILED", "SprigClusterPrecheckFailed", SQLSTATE_GENERAL, false;
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

fn sprig_protocol(code: SprigCode) -> ProtocolError {
    use tonic::Code;
    // Errors with a standard MongoDB equivalent use it (chapter 30).
    let (mongo_code, mongo_code_name, mongo_labels): (i32, &'static str, &'static [&str]) =
        match code {
            SprigCode::GatewayDraining => (91, "ShutdownInProgress", &["RetryableWriteError"]),
            _ => (i32::from(code.number()), code.mongo_code_name(), &[]),
        };
    // gRPC and HTTP follow chapter 37's table.
    let (grpc_status, http_status) = match code {
        SprigCode::SettingOutOfBounds
        | SprigCode::SettingUnknown
        | SprigCode::SettingScopeNotAllowed
        | SprigCode::SettingBadValue
        | SprigCode::AdminRequestInvalid => (Code::InvalidArgument, 400),
        SprigCode::SettingLocked
        | SprigCode::BinlogUnsupported
        | SprigCode::ClusterCapabilityRequired
        | SprigCode::ProvisionerUnsupported
        | SprigCode::ClusterPrecheckFailed
        | SprigCode::LocalInfileRejected
        | SprigCode::LongDataLimit
        | SprigCode::ReservedName
        | SprigCode::NameRewriteUnsupported
        | SprigCode::MetadataTooLarge
        | SprigCode::ResultTooLarge => (Code::FailedPrecondition, 400),
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
        | SprigCode::ForwardFailed
        | SprigCode::JobLeaseLost
        | SprigCode::MoveSourceDiverged => (Code::Unavailable, 503),
        SprigCode::CatalogConflict
        | SprigCode::TenantTreeBusy
        | SprigCode::TxnHandleUnknown
        | SprigCode::TxnHandleExpired
        | SprigCode::ClusterFenceFailed
        | SprigCode::OutcomeUnknown
        | SprigCode::SessionStateLost
        | SprigCode::ShardMoved
        | SprigCode::ShardLocked
        | SprigCode::MoveCutoverAborted => (Code::Aborted, 409),
        SprigCode::CatalogFormatTooNew => (Code::FailedPrecondition, 500),
        SprigCode::BinlogCorrupt | SprigCode::MoveVerifyFailed => (Code::DataLoss, 500),
        SprigCode::BackendProtocol => (Code::Internal, 502),
        SprigCode::ClusterNotFound
        | SprigCode::UnknownDc
        | SprigCode::AutoIndexNotFound
        | SprigCode::JobNotFound => (Code::NotFound, 404),
        SprigCode::ClusterExists | SprigCode::TenantExists => (Code::AlreadyExists, 409),
        SprigCode::ClusterStateInvalid
        | SprigCode::AutoIndexStateInvalid
        | SprigCode::TenantNotReady
        | SprigCode::TenantHasChildren
        | SprigCode::TenantDepthExceeded
        | SprigCode::MoveTargetIncompatible
        | SprigCode::MoveUnsupported
        | SprigCode::JobStateInvalid => (Code::FailedPrecondition, 409),
        SprigCode::TenantSuspended
        | SprigCode::RemoteAccessDenied
        | SprigCode::TxnHandleForbidden => (Code::PermissionDenied, 403),
        SprigCode::LoginRateLimited
        | SprigCode::TenantConnectionLimit
        | SprigCode::AdmissionTimeout
        | SprigCode::MemoryLimit => (Code::ResourceExhausted, 429),
        SprigCode::PlacementUnavailable => (Code::ResourceExhausted, 503),
        SprigCode::RequestTooLarge => (Code::InvalidArgument, 413),
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
