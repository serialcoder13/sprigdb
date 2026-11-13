//! Dispatch into the layers (chapter 04, "Dispatch into the layers"; build
//! step 9): the [`Request`] a front end produces, the [`Executor`] that runs
//! it, and the [`ResultSink`] results stream into.
//!
//! ```text
//! front end (sprig-gateway::mysql)            executor (pass-through in M0.2,
//!   COM_* ──► Request ──► SessionExec::execute     router in R1, chapter 17)
//!                              │
//!            ResultSink ◄──────┘  begin_rows / raw_row / end_rows / ok / error
//!   (encodes for the client's capabilities, streams with backpressure)
//! ```
//!
//! **One executor per session.** [`Executor::open`] runs once a login
//! succeeds and returns the session's [`SessionExec`], which holds whatever
//! the executor keeps per session: in M0.2's pass-through (decision D3) the
//! leased backend connection and the backend statement ids; in R1 the
//! router's transaction state. Chapter 04 places the lease on `Session`;
//! until chapter 06's `BackendLease` lands there, it lives here, and is
//! dropped with the connection.
//!
//! **Streaming.** Results never collect in memory: an executor calls the
//! sink as packets arrive, and each sink call awaits until the client
//! connection has room (the sink buffers at most `gateway.relay_buffer_kb`
//! before it waits on the socket), so a slow client's TCP backpressure
//! reaches the backend.
//!
//! **Relay mode.** [`ResultSink::raw_row`] passes a backend row packet's
//! payload through unchanged (zero copy). Column definitions, OK, ERR and
//! terminators go through the typed calls, so the sink re-encodes them for
//! the client's capabilities (`DEPRECATE_EOF`, `SESSION_TRACK`, MariaDB
//! extended metadata) after the executor has rewritten names (chapter 05,
//! "Resultsets: relay and rewrite"). Rewrite and synthesized results use
//! [`ResultSink::row`].
//!
//! **Cancellation.** `KILL QUERY`, `KILL CONNECTION` and a client hang-up
//! drop the `execute` future at its next await. The front end then calls
//! [`SessionExec::abandoned`] (except after `KILL CONNECTION`, which drops
//! the whole session), where a pass-through executor sends `KILL QUERY` for
//! its backend connection and discards that connection (chapter 04).
//!
//! **Errors.** An executor returns `Err` for a failure it hasn't reported;
//! the front end sends it as an ERR packet (ending an open resultset). A
//! backend ERR packet is relayed with [`ResultSink::error`] and `Ok`.
//!
//! **Workload hook.** [`with_workload`] puts a [`WorkloadHook`] between the
//! front end and the executor, so every request is seen before and after
//! it runs (chapter 04 build step 9; chapter 12 fills it in M0.4).
//!
//! The gateway runs the pass-through executor ([`crate::exec`], decision
//! D3). [`NullExecutor`] refuses every statement; [`canned::CannedExecutor`]
//! answers a few statements in-process, for tests without a backend.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use sprig_auth::BackendTarget;
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::command::{BulkExecute, ExecuteParams, QueryAttrs, StmtExecute};
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::state::ProtoState;
use sprig_proto_mysql::{Caps, ColumnDef, Persona, StatusFlags, Value};

use crate::session::{Principal, Session};

pub mod canned;

/// A prepared statement as the executor knows it: an id the executor
/// assigned in its [`PreparedMeta`]. The front end maps the client's
/// statement ids to these (chapter 05, "Prepared statements").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct StmtRef(pub u64);

/// What a front end asks the executor to do. Every MySQL `COM_*` that
/// reaches the data is one of these; commands the gateway answers itself
/// (ping, statistics, `KILL`, `SHOW PROCESSLIST`, the persona's version
/// variables) never become a request.
///
/// Chapter 04's protocol-neutral form (`Sql(BoundStatement)`,
/// `Plan(LogicalPlan)`, `Call(FunctionCall)`) arrives with the parser and
/// router (chapters 07 and 17); M0.2's pass-through executor sends text.
#[derive(Debug, Clone)]
pub enum Request {
    /// `COM_QUERY`: the SQL text, query attributes stripped (3306). With
    /// multi-statements on ([`ProtoState::multi_statements`]) the text may
    /// hold several statements; the executor streams each result, setting
    /// `MORE_RESULTS_EXISTS` on all but the last.
    Query {
        /// The SQL text.
        sql: Bytes,
        /// Query attributes (`traceparent`, ...); empty on 3307.
        attrs: QueryAttrs,
    },
    /// `COM_STMT_PREPARE`. Answered with [`ExecSummary::prepared`], not
    /// through the sink: the front end assigns the client's id and sends
    /// `PREPARE_OK`.
    Prepare {
        /// The SQL text.
        sql: Bytes,
    },
    /// `COM_STMT_EXECUTE`: a binary resultset or an OK through the sink.
    Execute {
        /// The statement.
        stmt: StmtRef,
        /// The packet as the client sent it (cursor flags, iteration
        /// count, raw parameters).
        raw: StmtExecute,
        /// The parameters decoded with the statement's types; on 3306,
        /// query attributes follow as named parameters.
        params: ExecuteParams,
    },
    /// `COM_STMT_FETCH`: more rows of an open cursor.
    Fetch {
        /// The statement.
        stmt: StmtRef,
        /// Rows wanted.
        rows: u32,
    },
    /// `COM_STMT_SEND_LONG_DATA`: no response, whatever happens; a failure
    /// is reported by the next execute.
    LongData {
        /// The statement.
        stmt: StmtRef,
        /// Parameter index.
        param: u16,
        /// The piece.
        data: Bytes,
    },
    /// `COM_STMT_RESET`: an OK through the sink.
    ResetStmt {
        /// The statement.
        stmt: StmtRef,
    },
    /// `COM_STMT_CLOSE`: no response.
    CloseStmt {
        /// The statement.
        stmt: StmtRef,
    },
    /// `COM_STMT_BULK_EXECUTE` (3307 only).
    BulkExecute {
        /// The statement.
        stmt: StmtRef,
        /// The packet as the client sent it.
        raw: BulkExecute,
    },
    /// `COM_FIELD_LIST`: column definitions and a terminator through
    /// [`ResultSink::column_list`].
    FieldList {
        /// The table.
        table: Bytes,
        /// A `LIKE` pattern for the columns, possibly empty.
        wildcard: Bytes,
    },
}

impl Request {
    /// The `COM_*` name, for logs and metrics.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Request::Query { .. } => "query",
            Request::Prepare { .. } => "stmt_prepare",
            Request::Execute { .. } => "stmt_execute",
            Request::Fetch { .. } => "stmt_fetch",
            Request::LongData { .. } => "stmt_send_long_data",
            Request::ResetStmt { .. } => "stmt_reset",
            Request::CloseStmt { .. } => "stmt_close",
            Request::BulkExecute { .. } => "stmt_bulk_execute",
            Request::FieldList { .. } => "field_list",
        }
    }

    /// The bytes the request carries (statement text, parameters, long
    /// data), which decides whether it runs off the worker threads
    /// ([`crate::offload`]).
    #[must_use]
    pub fn size(&self) -> usize {
        match self {
            Request::Query { sql, .. } | Request::Prepare { sql } => sql.len(),
            // Long data was buffered before, size unknown here: counted as
            // large.
            Request::Execute { raw, params, .. } => {
                if params
                    .params
                    .iter()
                    .any(|p| matches!(p.value, sprig_proto_mysql::command::ParamValue::LongData))
                {
                    usize::MAX
                } else {
                    raw.raw_params.len()
                }
            }
            Request::LongData { data, .. } => data.len(),
            Request::BulkExecute { raw, .. } => raw.raw.len(),
            Request::Fetch { .. }
            | Request::ResetStmt { .. }
            | Request::CloseStmt { .. }
            | Request::FieldList { .. } => 0,
        }
    }

    /// Whether the client expects no response at all.
    #[must_use]
    pub fn is_silent(&self) -> bool {
        matches!(self, Request::LongData { .. } | Request::CloseStmt { .. })
    }
}

/// A prepared statement's metadata, as `PREPARE_OK` reports it. Column
/// definitions are the logical ones the client sees.
#[derive(Debug, Clone, Default)]
pub struct PreparedMeta {
    /// The executor's id for the statement.
    pub stmt: StmtRef,
    /// One definition per parameter.
    pub params: Vec<ColumnDef>,
    /// The resultset's columns; empty for a statement without rows.
    pub columns: Vec<ColumnDef>,
    /// Warnings from the prepare.
    pub warnings: u16,
}

/// What a request did, beyond what went through the sink.
#[derive(Debug, Clone, Default)]
pub struct ExecSummary {
    /// The server status after the request (the backend's
    /// `SERVER_STATUS_IN_TRANS` and friends), when the executor knows it;
    /// the front end tracks the session's transaction state from it.
    pub status: Option<StatusFlags>,
    /// `Request::Prepare`'s answer.
    pub prepared: Option<PreparedMeta>,
    /// The session's default database after the request, when the executor
    /// tracks it (tenant sessions: the logical name; `Some(None)` when the
    /// session has none). The front end shows it in `SHOW PROCESSLIST` and
    /// passes it back in [`ExecCtx::db`].
    pub db: Option<Option<String>>,
}

/// Who logged in, as [`Executor::open`] needs it.
#[derive(Debug, Clone)]
pub struct LoginInfo {
    /// The session's principal (interim, decision D1).
    pub principal: Principal,
    /// The backend account and cluster the login's statements use
    /// (decision D3).
    pub backend: BackendTarget,
    /// The listener's persona.
    pub persona: Persona,
}

/// What an executor sees of the session while it runs a request.
pub struct ExecCtx<'a> {
    /// The gateway session: id, principal, TLS, kill flags.
    pub session: &'a mut Session,
    /// The client's negotiated protocol state (persona, capabilities,
    /// multi-statements, collation).
    pub proto: &'a ProtoState,
    /// The default database, as the client named it.
    pub db: Option<&'a str>,
}

impl fmt::Debug for ExecCtx<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecCtx")
            .field("session", &self.session.id)
            .field("db", &self.db)
            .finish_non_exhaustive()
    }
}

/// Why a statement was abandoned mid-way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Abandon {
    /// `KILL QUERY` (or `KILL` on another gateway task): the session goes
    /// on; the front end answers 1317.
    Killed,
    /// The client hung up; the session ends after this call.
    ClientGone,
}

/// Runs requests (chapter 04's `Executor`). Shared by every session.
pub trait Executor: Send + Sync + 'static {
    /// The executor's side of a session that just logged in.
    fn open(&self, login: &LoginInfo) -> Box<dyn SessionExec>;
}

/// One session's executor state. The front end calls it from the session's
/// own task, one request at a time.
#[async_trait]
pub trait SessionExec: Send {
    /// Runs `req`, streaming its response into `out`. See the module notes
    /// for errors and cancellation.
    async fn execute(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: Request,
        out: &mut dyn ResultSink,
    ) -> Result<ExecSummary>;

    /// `USE schema`: at login (`CONNECT_WITH_DB`) and for `COM_INIT_DB`.
    /// The front end sends the OK, with the schema in session tracking.
    async fn init_db(&mut self, cx: &mut ExecCtx<'_>, schema: &str) -> Result<()>;

    /// `COM_RESET_CONNECTION` and `COM_CHANGE_USER`: roll back, drop
    /// session variables, temporary tables, locks and prepared statements;
    /// a pass-through executor resets and releases its backend connection
    /// (chapter 05, "Command table").
    async fn reset(&mut self, cx: &mut ExecCtx<'_>) -> Result<()>;

    /// The current request's future was dropped (see [`Abandon`]): stop
    /// the backend's work and discard whatever state it left.
    async fn abandoned(&mut self, _cx: &mut ExecCtx<'_>, _why: Abandon) {}

    /// The session ends (quit, error, drain): roll back and release.
    async fn close(&mut self) {}

    /// Statement capture for this session ([`WorkloadHook::capture`]): an
    /// executor that sees statements fingerprints and records them through
    /// it (chapter 12). Executors that don't ignore it.
    fn attach_capture(&mut self, _capture: crate::workload::SessionCapture) {}

    /// The gateway's workload capture attached to this session, if any
    /// (chapter 12: front ends that fingerprint their own requests, such
    /// as MongoDB's `Fingerprinter::mongo`, record through it), and whether
    /// the tenant captures parameter samples (`workload.capture_samples`).
    fn workload(&self, _db: &str, _table: &str) -> Option<crate::workload::FrontCapture> {
        None
    }

    /// The `wait_timeout` the client set for its session, if it set one:
    /// the front end closes the session after that long without a command,
    /// as MySQL and MariaDB do. `None`: no limit from the client.
    fn idle_timeout(&self) -> Option<std::time::Duration> {
        None
    }

    /// Internal mode for the statements of a gateway layer that names its
    /// own reserved objects: chapter 31's document engine runs its SQL
    /// (`_sprig_p_` field columns, `__sprig_doc_meta`) through the user's
    /// session this way, as chapter 32's type layer does. Privileges still
    /// apply; only chapter 07's reserved-name check is lifted. Executors
    /// without tenant sessions ignore it.
    fn set_internal(&mut self, _on: bool) {}

    /// Whether row-level security policies on table `object` bind this
    /// session now (chapter 29): search counts and facets are then taken
    /// over the rows it may see only (decision S24). `None` when the
    /// executor can't tell (no tenant session, no catalog yet): callers
    /// treat that as bound.
    fn row_policy_binds(&self, _object: u64) -> Option<bool> {
        None
    }

    /// The tables whose policies bind this session, with those policies as
    /// MongoDB filters with its values bound (chapter 29, decision RL34):
    /// what a change stream shows. `None` when the executor can't tell:
    /// callers show nothing then.
    fn row_policy_docs(&self) -> Option<std::sync::Arc<crate::tenant::rls::DocViews>> {
        None
    }

    /// Whether the session holds `action` on the whole tenant (chapter
    /// 29), as `$currentOp` with `allUsers` asks for `PROCESS` (M5).
    /// `None` when the executor can't tell: callers refuse then.
    fn holds_tenant_privilege(&self, action: sprig_auth::authz::Action) -> Option<bool> {
        let _ = action;
        None
    }

    /// The verified claims of the token the session's caller presented,
    /// for `SPRIG_CLAIM()` (chapter 29); executors without row-level
    /// security ignore them.
    fn set_claims(&mut self, claims: Option<std::sync::Arc<serde_json::Value>>) {
        let _ = claims;
    }
}

/// The client side of a response, as an executor sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Wire {
    /// The listener's persona.
    pub persona: Persona,
    /// The negotiated capabilities, which decide whether a backend packet
    /// can pass unchanged.
    pub caps: Caps,
}

/// The rows of a resultset are text (`COM_QUERY`) or binary
/// (`COM_STMT_EXECUTE`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowFormat {
    /// Text protocol rows.
    Text,
    /// Binary protocol rows.
    Binary,
}

/// One row for the sink to encode.
#[derive(Debug, Clone, Copy)]
pub enum RowValues<'a> {
    /// Text: one value per column, `None` for NULL.
    Text(&'a [Option<&'a [u8]>]),
    /// Binary: one value per column, encoded by the resultset's column
    /// types.
    Binary(&'a [Value]),
}

/// The end of a resultset's rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EndOfRows {
    /// Status flags; `MORE_RESULTS_EXISTS` when another result follows.
    pub status: StatusFlags,
    /// Warnings.
    pub warnings: u16,
}

/// Where a response goes (chapter 04's `ResultSink`). Calls follow the
/// response grammar:
///
/// ```text
/// response  = { result MORE_RESULTS } result
/// result    = ok | error | begin_rows { row | raw_row } (end_rows | error)
///           | column_list | local_infile_request { local_infile_read } (ok | error)
/// ```
///
/// Every call may wait for the client to drain (backpressure) and fails if
/// the client is gone.
#[async_trait]
pub trait ResultSink: Send {
    /// The client's persona and capabilities.
    fn wire(&self) -> Wire;

    /// Starts a resultset: the column count, the column definitions (as
    /// the client should see them), and an EOF unless the client has
    /// `DEPRECATE_EOF`.
    async fn begin_rows(&mut self, columns: &[ColumnDef], format: RowFormat) -> Result<()>;

    /// Relay mode: starts a resultset with the backend's column definition
    /// packets unchanged. The executor calls it only when the client's
    /// capabilities encode column definitions exactly as the backend
    /// connection's do (MariaDB extended metadata on both or neither); by
    /// default the definitions are decoded and re-encoded.
    async fn begin_rows_raw(&mut self, defs: &[Bytes], format: RowFormat) -> Result<()> {
        let caps = self.wire().caps;
        let mut columns = Vec::with_capacity(defs.len());
        for d in defs {
            columns.push(ColumnDef::decode(d, caps)?);
        }
        self.begin_rows(&columns, format).await
    }

    /// A `COM_STMT_EXECUTE` that opened a server-side cursor: the column
    /// count and definitions, then one EOF (or OK-form terminator under
    /// `DEPRECATE_EOF`) carrying `end.status` with `CURSOR_EXISTS`. The
    /// rows follow with `COM_STMT_FETCH`. Ends the response.
    async fn cursor_opened(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
        self.begin_rows(columns, RowFormat::Binary).await?;
        self.end_rows(end).await
    }

    /// One row, encoded by the sink (synthesized and rewrite modes).
    async fn row(&mut self, values: RowValues<'_>) -> Result<()>;

    /// One row packet's payload exactly as the backend sent it (relay
    /// mode). Never re-encoded.
    async fn raw_row(&mut self, payload: Bytes) -> Result<()>;

    /// Ends the rows with an EOF or an OK terminator, whichever the client
    /// expects.
    async fn end_rows(&mut self, end: EndOfRows) -> Result<()>;

    /// An OK packet, re-encoded for the client (session tracking dropped
    /// without `SESSION_TRACK`).
    async fn ok(&mut self, ok: OkPacket) -> Result<()>;

    /// An ERR packet in the persona's numbering; ends the response (and an
    /// open resultset).
    async fn error(&mut self, err: &SprigError) -> Result<()>;

    /// `COM_FIELD_LIST`'s answer: column definitions, then the terminator.
    async fn column_list(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()>;

    /// `LOAD DATA LOCAL INFILE`: asks the client for `file_name` (0xFB).
    /// The executor must have checked the name against the statement.
    async fn local_infile_request(&mut self, file_name: &[u8]) -> Result<()>;

    /// The next packet of the client's file; empty at the end.
    async fn local_infile_read(&mut self) -> Result<Bytes>;

    /// Sends what is buffered.
    async fn flush(&mut self) -> Result<()>;

    /// Whether the response is complete (a final OK, ERR or terminator was
    /// sent).
    fn finished(&self) -> bool;
}

/// Statement capture's view of one request (chapter 04, "Dispatch into
/// the layers"; chapter 12). The front end's request passes through
/// [`WorkloadHook::begin`] before the executor runs it and
/// [`WorkloadHook::end`] after; [`with_workload`] puts a hook in front of
/// any executor. [`NoWorkload`] records nothing;
/// [`crate::workload::GatewayWorkload`] (M0.4) hands each session a
/// [`crate::workload::SessionCapture`] ([`WorkloadHook::capture`]), which
/// the executor calls per statement, where it knows the statement's
/// logical text, its backend connection and its rows.
pub trait WorkloadHook: Send + Sync + 'static {
    /// A request is about to run.
    fn begin(&self, session: &Session, req: &Request) -> WorkloadToken;

    /// The request finished. A request whose future was dropped (`KILL
    /// QUERY`, a client hang-up) never reaches `end`.
    fn end(&self, token: WorkloadToken, outcome: &WorkloadOutcome<'_>);

    /// A new session's statement capture, attached to its executor
    /// ([`SessionExec::attach_capture`]); `None` captures nothing.
    fn capture(&self) -> Option<crate::workload::SessionCapture> {
        None
    }
}

/// What [`WorkloadHook::begin`] hands to [`WorkloadHook::end`].
#[derive(Debug, Clone)]
pub struct WorkloadToken {
    /// When the request started.
    pub started: std::time::Instant,
    /// [`Request::name`].
    pub request: &'static str,
}

/// How a request ended, for [`WorkloadHook::end`].
#[derive(Debug, Clone, Copy)]
pub struct WorkloadOutcome<'a> {
    /// The executor's summary, when it succeeded.
    pub summary: Option<&'a ExecSummary>,
    /// The error it returned (a backend ERR relayed through the sink is a
    /// success here).
    pub error: Option<&'a SprigError>,
}

/// The workload hook that records nothing (until chapter 12, M0.4).
#[derive(Debug, Clone, Copy, Default)]
pub struct NoWorkload;

impl WorkloadHook for NoWorkload {
    fn begin(&self, _session: &Session, req: &Request) -> WorkloadToken {
        WorkloadToken {
            started: std::time::Instant::now(),
            request: req.name(),
        }
    }

    fn end(&self, _token: WorkloadToken, _outcome: &WorkloadOutcome<'_>) {}
}

/// `exec` with `hook` in front: front end → `Request` → workload hook →
/// executor (chapter 04 build step 9).
#[must_use]
pub fn with_workload(exec: SharedExecutor, hook: Arc<dyn WorkloadHook>) -> SharedExecutor {
    Arc::new(Hooked { exec, hook })
}

struct Hooked {
    exec: SharedExecutor,
    hook: Arc<dyn WorkloadHook>,
}

impl Executor for Hooked {
    fn open(&self, login: &LoginInfo) -> Box<dyn SessionExec> {
        let mut inner = self.exec.open(login);
        if let Some(c) = self.hook.capture() {
            inner.attach_capture(c);
        }
        Box::new(HookedSession {
            inner,
            hook: Arc::clone(&self.hook),
        })
    }
}

struct HookedSession {
    inner: Box<dyn SessionExec>,
    hook: Arc<dyn WorkloadHook>,
}

#[async_trait]
impl SessionExec for HookedSession {
    async fn execute(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: Request,
        out: &mut dyn ResultSink,
    ) -> Result<ExecSummary> {
        let token = self.hook.begin(cx.session, &req);
        let r = self.inner.execute(cx, req, out).await;
        let outcome = WorkloadOutcome {
            summary: r.as_ref().ok(),
            error: r.as_ref().err(),
        };
        self.hook.end(token, &outcome);
        r
    }

    async fn init_db(&mut self, cx: &mut ExecCtx<'_>, schema: &str) -> Result<()> {
        self.inner.init_db(cx, schema).await
    }

    async fn reset(&mut self, cx: &mut ExecCtx<'_>) -> Result<()> {
        self.inner.reset(cx).await
    }

    async fn abandoned(&mut self, cx: &mut ExecCtx<'_>, why: Abandon) {
        self.inner.abandoned(cx, why).await;
    }

    async fn close(&mut self) {
        self.inner.close().await;
    }

    fn attach_capture(&mut self, capture: crate::workload::SessionCapture) {
        self.inner.attach_capture(capture);
    }

    fn workload(&self, db: &str, table: &str) -> Option<crate::workload::FrontCapture> {
        self.inner.workload(db, table)
    }

    fn idle_timeout(&self) -> Option<std::time::Duration> {
        self.inner.idle_timeout()
    }

    fn set_internal(&mut self, on: bool) {
        self.inner.set_internal(on);
    }

    fn row_policy_binds(&self, object: u64) -> Option<bool> {
        self.inner.row_policy_binds(object)
    }

    fn row_policy_docs(&self) -> Option<std::sync::Arc<crate::tenant::rls::DocViews>> {
        self.inner.row_policy_docs()
    }

    fn holds_tenant_privilege(&self, action: sprig_auth::authz::Action) -> Option<bool> {
        self.inner.holds_tenant_privilege(action)
    }

    fn set_claims(&mut self, claims: Option<std::sync::Arc<serde_json::Value>>) {
        self.inner.set_claims(claims);
    }
}

/// Refuses every request (a gateway built without backends).
#[derive(Debug, Clone, Copy, Default)]
pub struct NullExecutor;

/// The error [`NullExecutor`] returns.
#[must_use]
pub fn no_executor() -> SprigError {
    SprigError::new(
        sprig_common::SprigCode::ClusterUnavailable,
        "this gateway has no executor yet: statements can't reach a cluster",
    )
}

impl Executor for NullExecutor {
    fn open(&self, _login: &LoginInfo) -> Box<dyn SessionExec> {
        Box::new(NullExecutor)
    }
}

#[async_trait]
impl SessionExec for NullExecutor {
    async fn execute(
        &mut self,
        _cx: &mut ExecCtx<'_>,
        _req: Request,
        _out: &mut dyn ResultSink,
    ) -> Result<ExecSummary> {
        Err(no_executor())
    }

    async fn init_db(&mut self, _cx: &mut ExecCtx<'_>, _schema: &str) -> Result<()> {
        Err(no_executor())
    }

    async fn reset(&mut self, _cx: &mut ExecCtx<'_>) -> Result<()> {
        Ok(())
    }
}

/// A shared executor.
pub type SharedExecutor = Arc<dyn Executor>;

/// The binary collation id, for numeric columns.
pub const BINARY_COLLATION: u16 = 63;

/// A synthesized `VARCHAR` column named `name` in collation `charset`.
#[must_use]
pub fn text_column(name: &str, charset: u16) -> ColumnDef {
    ColumnDef {
        name: Bytes::copy_from_slice(name.as_bytes()),
        charset,
        length: 1024,
        col_type: sprig_proto_mysql::resultset::column_type::VAR_STRING,
        ..ColumnDef::default()
    }
}

/// A synthesized `BIGINT` column named `name` (`UNSIGNED` when asked).
#[must_use]
pub fn int_column(name: &str, unsigned: bool) -> ColumnDef {
    use sprig_proto_mysql::resultset::ColumnFlags;
    let mut flags = ColumnFlags::NOT_NULL | ColumnFlags::NUM | ColumnFlags::BINARY;
    if unsigned {
        flags |= ColumnFlags::UNSIGNED;
    }
    ColumnDef {
        name: Bytes::copy_from_slice(name.as_bytes()),
        charset: BINARY_COLLATION,
        length: 21,
        col_type: sprig_proto_mysql::resultset::column_type::LONGLONG,
        flags: flags.bits(),
        ..ColumnDef::default()
    }
}
