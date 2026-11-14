//! The pass-through executor: every request of a session goes, as MySQL
//! text or binary protocol, to the login's cluster through the session's
//! [`BackendSession`], and the response streams back through the
//! [relay engine](relay). For tenants with sharded tables the
//! [router](router) works behind the same [`Executor`] trait.
//!
//! | Request | Backend |
//! |---|---|
//! | `Query` | Split into statements ([`split`]); each is classified, routed (reads may go to a replica) and relayed; all but the last get `MORE_RESULTS_EXISTS`; an ERR stops the rest. More than one statement without multi-statements fails with 1064 |
//! | `Prepare` | Prepared on the routed connection's statement cache; `PREPARE_OK`'s metadata from it |
//! | `Execute` | The connection's own statement for the text (re-prepared lazily on another connection or node), types re-bound when it is fresh, long data folded in ([`stmt`]); a cursor pins the connection |
//! | `Fetch` | `COM_STMT_FETCH` on the pinned connection; the last row unpins |
//! | `LongData` | Buffered (39011 past `gateway.long_data_max_bytes`) |
//! | `ResetStmt` | Buffers dropped; an open cursor is reset on its connection |
//! | `CloseStmt` | Mapping dropped (backend statements stay cached per connection) |
//! | `BulkExecute` | `COM_STMT_BULK_EXECUTE` with the id rewritten (3307) |
//! | `FieldList` | `COM_FIELD_LIST`, definitions re-encoded |
//!
//! `LOAD DATA LOCAL INFILE` needs the client's `LOCAL_FILES` and
//! `sql.local_infile = on`, else 3948 (MariaDB persona: 4166) before
//! anything reaches the backend;
//! the backend's request must name the statement's file, else 39010.
//!
//! The [`BackendSession`] opens at the first request, when the client's
//! capabilities and collation are known (the pool key depends on them).

mod capture;
mod compat_sink;
mod dc;
// A dropped database's sinks and logic modules go with it.
mod dropdb;
// The read-only `sprig_docs` database.
mod docs;
mod fence;
// A mover's fence never reaches clients as 1146 or 1049.
mod fence_retry;
mod follow;
pub mod relay;
pub(crate) mod router;
pub mod settings;
pub mod split;
pub mod stmt;
// Logic declarations.
mod logic;
// Validations, triggers, schedules, computed fields.
mod logic_bound;
// DROP of logic modules and their dependents.
mod logic_drop;
// `EXPLAIN FUNCTION | PROCEDURE` (placement of stored versions).
mod logic_explain;
// Staged rollouts of logic modules at call time.
mod logic_rollout;
// `Logic.TestRun`.
mod logic_test;
// Row-level security.
mod rls;
mod tenant;
// Hierarchical data inheritance.
pub(crate) mod inherit;
// Tree id sequences, the
// reference index, children receiving their parent's tables.
pub(crate) mod inherit_tree;
// Children receive the types of the
// type trees they inherit.
mod inherit_types;
// Inherited messaging objects (`FOR EACH TENANT`).
mod inherit_msg;
// FOR EACH TENANT connections and sinks.
mod inherit_sinks;
pub(crate) mod types;
// Event streams.
mod streams;
// `sprig.mq_*` calls.
mod queues;
// Sinks and connections.
pub mod sinks;
// CREATE GRAPH and DROP GRAPH.
mod graph_ddl;
// Triggers on streams and queues, and `sprig.commit_offset`.
pub(crate) mod msg_trigger;
mod offsets;
// Row images checked against a subscriber's
// privileges and row-level security (gRPC, SSE and WebSocket change
// streams, Redis keyspace notifications).
pub(crate) mod rls_images;
// Change topics' filter views, for views and
// stored code over them.
mod change_views;
// `queue.change_exchange`.
pub(crate) mod change_exchange;
// ONLY_FULL_GROUP_BY on 3306.
mod groupby;
// Nearest neighbours with a growing radius on 3306.
mod geo_knn;
// LATERAL's gateway apply on 3306.
mod lateral;
// Functional key parts typed from the catalog.
mod fxindex;
// MySQL's DDL answers that need a lookup.
mod mysql_ddl;
// `SET GLOBAL` server-wide assignments as tenant defaults.
mod set_global;
// A `DEFINER` naming another account.
mod definer;
// Per-session status counters.
pub(crate) mod status;
// Search DDL, MATCH ... AGAINST, sprig_search().
pub(crate) mod search;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use sprig_auth::BackendTarget;
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::client::PacketKind;
use sprig_proto_mysql::command::{
    CURSOR_TYPE_FOR_UPDATE, CURSOR_TYPE_READ_ONLY, CURSOR_TYPE_SCROLLABLE, QueryAttrs, com,
};
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::{Caps, ColumnDef, Persona, StatusFlags};

use self::relay::{Infile, NamesOut, Relay, Relayed, backend_error, protocol_error, relay};
pub use self::settings::{ExecSettings, ExecSettingsSource};
use self::stmt::{Stmt, Stmts, bulk_payload, execute_payload};
use crate::backend::{BackendSession, BackendStmt, Backends, SessionOpts, StmtInfo, StmtOutcome};
use crate::pipeline::{
    Abandon, EndOfRows, ExecCtx, ExecSummary, Executor, LoginInfo, PreparedMeta, Request,
    ResultSink, RowFormat, SessionExec, StmtRef,
};
use crate::tenant::TenantServices;
use crate::tenant::names::Accounts;
use crate::tenant::session::TenantSession;

/// How long `abandoned` waits for `KILL QUERY`, and for the killed
/// statement's response before its connection is discarded.
const KILL_TIMEOUT: Duration = Duration::from_secs(3);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// The pass-through executor: one per gateway.
#[derive(Debug, Clone)]
pub struct PassThrough {
    backends: Arc<Backends>,
    settings: ExecSettingsSource,
    tenants: Option<Arc<TenantServices>>,
}

impl PassThrough {
    /// An executor over `backends`, reading its settings from `settings`.
    #[must_use]
    pub fn new(backends: Arc<Backends>, settings: ExecSettingsSource) -> Self {
        Self {
            backends,
            settings,
            tenants: None,
        }
    }

    /// The same executor with tenant sessions: catalog logins get names
    /// virtualized, DDL through the catalog and settings in SQL (see
    /// `exec::tenant`). Accounts-file logins keep the plain pass-through.
    #[must_use]
    pub fn with_tenants(mut self, tenants: Arc<TenantServices>) -> Self {
        self.tenants = Some(tenants);
        self
    }
}

impl Executor for PassThrough {
    fn open(&self, login: &LoginInfo) -> Box<dyn SessionExec> {
        let tenant = match (&self.tenants, login.principal.tenant) {
            (Some(svc), Some(_)) => {
                match TenantSession::new(Arc::clone(svc), login.principal.clone()) {
                    Ok(t) => Some(Box::new(t)),
                    Err(e) => {
                        tracing::error!(error = %e, "tenant session failed to open");
                        None
                    }
                }
            }
            _ => None,
        };
        Box::new(PassSession {
            backends: Arc::clone(&self.backends),
            target: login.backend.clone(),
            persona: login.persona,
            settings: self.settings.clone(),
            bs: None,
            stmts: Stmts::default(),
            tenant,
            subst: Vec::new(),
            wl: None,
            wl_multi: false,
            router: router::RouterState::default(),
            packet_limit: None,
            placement_memo: follow::PlacementMemo::default(),
            status: status::SessionStatus::default(),
            globals_applied: false,
        })
    }
}

/// One session's side of the pass-through executor.
pub struct PassSession {
    backends: Arc<Backends>,
    target: BackendTarget,
    persona: Persona,
    settings: ExecSettingsSource,
    bs: Option<BackendSession>,
    stmts: Stmts,
    /// A catalog login's tenant state; `None` for accounts-file logins,
    /// whose names pass through unchanged.
    tenant: Option<Box<TenantSession>>,
    /// Columns the name rewrite's substitutions renamed (`DATABASE()`
    /// became `'shop'`): the next forwarded statement gives them back.
    subst: Vec<split::Renamed>,
    /// Workload capture, for tenant sessions.
    wl: Option<crate::workload::SessionCapture>,
    /// The current `COM_QUERY` holds several statements: none is probed.
    wl_multi: bool,
    /// The router: the session's connections to other clusters and the
    /// transaction coordinator, for tenants with sharded tables.
    router: router::RouterState,
    /// The session's packet limit, answered
    /// for `max_allowed_packet` in mixed selects and `SHOW VARIABLES`.
    packet_limit: Option<u64>,
    /// The session's own status counters, for `SHOW STATUS`.
    status: status::SessionStatus,
    /// Placement checks kept per map generation.
    placement_memo: follow::PlacementMemo,
    /// The tenant's `compat.global.*` defaults were given to the session:
    /// at its first request, and again after a reset.
    globals_applied: bool,
}

impl std::fmt::Debug for PassSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PassSession")
            .field("cluster", &self.target.cluster)
            .field("persona", &self.persona)
            .finish_non_exhaustive()
    }
}

/// What an execute carries from its checks to the backend.
#[allow(clippy::struct_excessive_bools)] // the statement's independent flags
struct ExecTail {
    mapper: Option<sprig_sql::names::ResultMapper>,
    logical: Option<Bytes>,
    values: Vec<sprig_proto_mysql::command::BoundParam>,
    cursor: bool,
    had_cursor: bool,
    sql: Bytes,
    info: StmtInfo,
    renames: Vec<split::Renamed>,
    nowait: bool,
    conn_status: Option<relay::ConnStatus>,
    flags: u8,
    client_bound: bool,
    backend_caps: Caps,
}

/// The MariaDB capabilities a backend connection takes from a 3307 client,
/// so its packets need no rewriting for that client.
const MARIADB_PASS_CAPS: Caps =
    Caps::MARIADB_CLIENT_EXTENDED_METADATA.union(Caps::MARIADB_CLIENT_STMT_BULK_OPERATIONS);

/// A 3306 session's backend connections ask for extended metadata too:
/// it marks JSON values, which the relay turns into MySQL's type 245
/// (the block itself never reaches a MySQL client).
fn mariadb_caps(persona: Persona, client: Caps) -> Caps {
    match persona {
        Persona::MariaDb => client & MARIADB_PASS_CAPS,
        Persona::MySql84 => Caps::MARIADB_CLIENT_EXTENDED_METADATA,
    }
}

/// 1064, as the servers answer several statements without
/// multi-statements, in the persona's wording ("your MySQL server
/// version", "your MariaDB server version").
fn multi_statements_off(rest: &[u8], persona: Persona) -> SprigError {
    let near: String = String::from_utf8_lossy(rest).chars().take(80).collect();
    let server = match persona {
        Persona::MySql84 => "MySQL",
        Persona::MariaDb => "MariaDB",
    };
    SprigError::standard(
        1064,
        *b"42000",
        format!(
            "You have an error in your SQL syntax; check the manual that corresponds to your {server} server version for the right syntax to use near '{}' at line 1",
            near.trim_start()
        ),
    )
}

/// `LOCAL INFILE` without the client's `LOCAL_FILES` or with
/// `sql.local_infile = off`, refused before the backend is asked, with
/// each persona's own error: MySQL's 3948, MariaDB's 4166.
fn local_infile_disabled(persona: Persona) -> SprigError {
    match persona {
        Persona::MySql84 => SprigError::standard(
            3948,
            *b"42000",
            "Loading local data is disabled; this must be enabled on both the client and server sides",
        ),
        Persona::MariaDb => SprigError::standard(
            4166,
            *b"HY000",
            "The used command is not allowed because the MariaDB server or client has disabled the local infile capability",
        ),
    }
}

/// 1421.
fn no_open_cursor(stmt: StmtRef) -> SprigError {
    SprigError::standard(
        1421,
        *b"HY000",
        format!("The statement ({}) has no open cursor.", stmt.0),
    )
}

/// Whether `flags` (of `COM_STMT_EXECUTE`) ask for a cursor.
fn wants_cursor(flags: u8) -> bool {
    flags & (CURSOR_TYPE_READ_ONLY | CURSOR_TYPE_FOR_UPDATE | CURSOR_TYPE_SCROLLABLE) != 0
}

impl PassSession {
    /// The backend session, opened with the client's capabilities. A
    /// tenant session opens it in the schema of its current logical
    /// database.
    fn bs(&mut self, cx: &ExecCtx<'_>) -> &mut BackendSession {
        let backends = &self.backends;
        let target = &self.target;
        let persona = self.persona;
        let schema = match self.tenant.as_deref() {
            Some(ts) => ts.schema_of(ts.current.as_deref().or(cx.db)),
            None => cx.db.map(str::to_owned),
        };
        self.bs.get_or_insert_with(|| {
            backends.session(
                target.clone(),
                SessionOpts {
                    persona,
                    found_rows: cx.proto.has(Caps::FOUND_ROWS),
                    mariadb_caps: mariadb_caps(persona, cx.proto.caps),
                    collation: cx.proto.collation,
                    schema,
                    ..SessionOpts::default()
                },
            )
        })
    }

    fn backend_caps(&self, cx: &ExecCtx<'_>) -> Caps {
        mariadb_caps(self.persona, cx.proto.caps)
    }

    /// The status flags the session has now, for answers the executor
    /// makes itself.
    fn status(&self) -> StatusFlags {
        let mut s = StatusFlags::empty();
        if let Some(bs) = &self.bs {
            let autocommit_off = bs
                .vars()
                .vars
                .get("autocommit")
                .is_some_and(|v| v.eq_ignore_ascii_case("off") || v == "0");
            if !autocommit_off {
                s |= StatusFlags::AUTOCOMMIT;
            }
            if bs.pins().in_transaction() {
                s |= StatusFlags::IN_TRANS;
            }
        } else {
            s |= StatusFlags::AUTOCOMMIT;
        }
        // A transaction the router's coordinator holds (its branches are
        // other connections).
        if self.router.in_txn() {
            s |= StatusFlags::IN_TRANS;
        }
        // As the servers do in every OK and EOF: clients pick their string
        // escaping by this bit (go-sql-driver's interpolateParams, the
        // `mysql` and `mariadb` clients' parsers), so an answer the gateway
        // makes itself (DDL, account statements) must carry it too.
        if self.lex_opts().0.no_backslash_escapes {
            s |= StatusFlags::NO_BACKSLASH_ESCAPES;
        }
        s
    }

    /// Ends the statement in flight. A failure after the client's response
    /// completed can't be reported any more (another packet would break
    /// the protocol), so it is only logged.
    async fn finish(&mut self, finished: bool) -> Result<Option<StatusFlags>> {
        Ok(self.finish_outcome(finished).await?.map(|o| o.status))
    }

    /// [`PassSession::finish`] with the whole outcome (rows, rows
    /// examined), for workload capture.
    async fn finish_outcome(&mut self, finished: bool) -> Result<Option<StmtOutcome>> {
        let Some(bs) = self.bs.as_mut() else {
            return Ok(None);
        };
        match bs.finish().await {
            Ok(o) => Ok(Some(o)),
            Err(e) if finished => {
                tracing::warn!(error = %e, "backend statement failed after its response was sent");
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    async fn query(
        &mut self,
        cx: &mut ExecCtx<'_>,
        sql: Bytes,
        out: &mut dyn ResultSink,
    ) -> Result<ExecSummary> {
        let mut pieces = split::split(&sql);
        if pieces.len() > 1 && !cx.proto.multi_statements {
            let second = pieces[1].as_ptr() as usize - sql.as_ptr() as usize;
            return Err(multi_statements_off(
                &sql[second.min(sql.len())..],
                self.persona,
            ));
        }
        if pieces.is_empty() {
            // Nothing but comments: the backend answers it (1065).
            pieces.push(sql);
        }
        let backend_caps = self.backend_caps(cx);
        let client_local = cx.proto.has(Caps::LOCAL_FILES);
        let n = pieces.len();
        let mut summary = ExecSummary::default();
        let conn_id = cx.session.id.0;
        let mysql = self.persona == Persona::MySql84;
        for (i, piece) in pieces.into_iter().enumerate() {
            let (piece, renames) = self.session_rewrites(piece, conn_id);
            let infile_name = split::local_infile_name(&piece);
            let mut allowed = false;
            if infile_name.is_some() {
                allowed = client_local && self.settings.current().local_infile;
                if !allowed {
                    return Err(local_infile_disabled(self.persona));
                }
            }
            let info = StmtInfo::classify(&piece);
            let conn_status = self.conn_status_for(&piece, cx.proto);
            let bs = self.bs(cx);
            bs.query(&info, &piece).await?;
            let r = Relay {
                more: i + 1 < n,
                format: RowFormat::Text,
                cursor: false,
                fetch: false,
                backend_caps: bs.backend_caps().unwrap_or(backend_caps),
                infile: Infile {
                    name: infile_name.as_deref(),
                    allowed,
                },
                conn_status: conn_status.as_ref(),
                renames: &renames,
                mysql_nowait: mysql && split::has_keyword(&piece, "NOWAIT"),
            };
            let relayed = relay(bs, out, &r, None).await?;
            let status = self.finish(out.finished()).await?;
            summary.status = status.or(relayed.status);
            if relayed.errored {
                break;
            }
        }
        Ok(summary)
    }

    async fn prepare(&mut self, cx: &mut ExecCtx<'_>, sql: Bytes) -> Result<ExecSummary> {
        let (sql, renames) = self.session_rewrites(sql, cx.session.id.0);
        let info = StmtInfo::classify(&sql);
        // Sharded tables are prepared in one shard's schema.
        let prepared = match self.prepare_meta(cx, &info, &sql).await {
            Some(r) => r?,
            None => self.bs(cx).prepare(&info, &sql).await?,
        };
        let mut meta_params = prepared.param_defs.clone();
        let mut meta_columns = prepared.columns.clone();
        relay::rename_all(&mut meta_columns, &renames);
        if self.persona == Persona::MySql84 {
            for c in meta_columns.iter_mut().chain(meta_params.iter_mut()) {
                sprig_sql::compat::meta::mysql_column(c);
            }
        }
        let mut st = Stmt::new(sql, info, &prepared);
        st.renames = renames;
        st.nowait = self.persona == Persona::MySql84 && split::has_keyword(&st.sql, "NOWAIT");
        st.session_status = split::is_session_status(&st.sql);
        let stmt = self.stmts.insert(st);
        metrics::gauge!("sprig_mysql_prepared_statements", "persona" => self.persona.as_str())
            .increment(1.0);
        Ok(ExecSummary {
            status: None,
            prepared: Some(PreparedMeta {
                stmt,
                params: meta_params,
                columns: meta_columns,
                warnings: 0,
            }),
            db: None,
        })
    }

    /// Prepares statement `stmt`'s client text again:
    /// its row-level security predicates were bound under another role
    /// set or policy version. `false` (the client gets 1615) when the text
    /// can't be prepared the same way: another default database now, or
    /// other parameters or columns.
    async fn reprepare(&mut self, cx: &mut ExecCtx<'_>, stmt: StmtRef) -> Result<bool> {
        let current = self.current_logical();
        let s = self.stmts.get_mut(stmt)?;
        let (Some(logical), Some(rc)) = (s.client.clone(), s.recheck.as_ref()) else {
            return Ok(false);
        };
        if rc.current() != current.as_deref() {
            return Ok(false);
        }
        let (params, columns) = (s.params, s.columns.len());
        let summary = self.tenant_prepare(cx, logical).await?;
        let Some(meta) = summary.prepared else {
            return Ok(false);
        };
        let Some(new) = self.stmts.remove(meta.stmt) else {
            return Ok(false);
        };
        metrics::gauge!("sprig_mysql_prepared_statements", "persona" => self.persona.as_str())
            .decrement(1.0);
        if new.deferred.is_some() || new.params != params || new.columns.len() != columns {
            return Ok(false);
        }
        let s = self.stmts.get_mut(stmt)?;
        s.sql = new.sql;
        s.info = new.info;
        s.mapper = new.mapper;
        s.recheck = new.recheck;
        s.logical = new.logical;
        s.renames = new.renames;
        s.nowait = new.nowait;
        s.session_status = new.session_status;
        metrics::counter!("sprig_rls_reprepares_total").increment(1);
        Ok(true)
    }

    #[allow(clippy::too_many_lines)] // the execute path, deferred and relayed
    async fn execute(
        &mut self,
        cx: &mut ExecCtx<'_>,
        stmt: StmtRef,
        raw: &sprig_proto_mysql::command::StmtExecute,
        params: &sprig_proto_mysql::command::ExecuteParams,
        out: &mut dyn ResultSink,
    ) -> Result<ExecSummary> {
        let persona = self.persona;
        let backend_caps = self.backend_caps(cx);
        // One statement per execute: probes allowed.
        self.wl_multi = false;
        let s = self.stmts.get_mut(stmt)?;
        if let Some(text) = s.deferred.clone() {
            // A search text: the bound values as literals.
            let search_text = s.deferred_params;
            let text = if s.graph_params {
                // GRAPH_TABLE with `?`.
                let values = s.params_for_backend(params, persona)?;
                let sql_l: Vec<String> = values.iter().map(search::param_literal).collect();
                let cy_l = values
                    .iter()
                    .map(router::graph::cypher_param)
                    .collect::<Result<Vec<String>>>()?;
                std::str::from_utf8(&text)
                    .ok()
                    .and_then(|t| sprig_router::graph::bind_params(t, &sql_l, &cy_l))
                    .map(Bytes::from)
                    .ok_or_else(|| {
                        SprigError::standard(
                            1210,
                            *b"HY000",
                            "Incorrect arguments to EXECUTE: a GRAPH_TABLE statement's parameters",
                        )
                    })?
            } else if s.deferred_params {
                let values: Vec<String> = s
                    .params_for_backend(params, persona)?
                    .iter()
                    .map(search::param_literal)
                    .collect();
                std::str::from_utf8(&text)
                    .ok()
                    .and_then(|t| sprig_search::sqlscan::interpolate(t, &values))
                    .map(Bytes::from)
                    .ok_or_else(|| SprigError::internal("a prepared search's parameters"))?
            } else {
                text
            };
            // A statement the gateway answers itself, planned again now.
            let step = if search_text {
                // Its rows come as text from MariaDB: binary for the client.
                let mut bin = search::ToBinary::new(out);
                self.tenant_text(
                    cx,
                    &text,
                    &QueryAttrs::default(),
                    false,
                    RowFormat::Text,
                    &mut bin,
                )
                .await?
            } else {
                self.tenant_text(
                    cx,
                    &text,
                    &QueryAttrs::default(),
                    false,
                    RowFormat::Binary,
                    out,
                )
                .await?
            };
            return Ok(ExecSummary {
                status: step.status,
                prepared: None,
                db: Some(self.current_logical()),
            });
        }
        let mapper = s.mapper.clone();
        let logical = s.logical.clone();
        let recheck = s.recheck.clone();
        let values = s.params_for_backend(params, persona)?;
        if params.new_params_bound {
            s.types = Some(values.iter().map(|p| p.param_type).collect());
        }
        let cursor = wants_cursor(raw.flags) && !s.columns.is_empty();
        let had_cursor = s.cursor.take().is_some();
        let (sql, info, renames, nowait) =
            (s.sql.clone(), s.info.clone(), s.renames.clone(), s.nowait);
        let conn_status = s.session_status.then(|| relay::ConnStatus::of(cx.proto));
        let flags = raw.flags;
        let client_bound = params.new_params_bound;
        tracing::trace!(
            stmt = stmt.0,
            types = ?values.iter().map(|p| p.param_type).collect::<Vec<_>>(),
            "execute parameters"
        );
        // Privileges are checked again at each execute, as on
        // MySQL, when they changed since the prepare.
        if let Some(rc) = &recheck
            && let Some(ts) = self.tenant.as_deref_mut()
        {
            ts.refresh().await?;
            match crate::tenant::authz::recheck(ts, persona, rc) {
                // A text whose row-level
                // security predicates changed is prepared again, under the
                // same id, and the execute goes on.
                Err(e) if e.protocol().mysql_errno == 1615 => {
                    if !Box::pin(self.reprepare(cx, stmt)).await? {
                        return Err(e);
                    }
                    return Box::pin(self.execute(cx, stmt, raw, params, out)).await;
                }
                r => r?,
            }
        }
        // A sharded tenant's statement is routed (see
        // `router::PassSession::routed_execute`).
        let mut sql = sql;
        let mut slot = None;
        if self.tenant.is_some() {
            match self.routed_execute(cx, &sql, &values, cursor, out).await? {
                router::ExecRoute::Answered => {
                    return Ok(ExecSummary {
                        status: None,
                        prepared: None,
                        db: None,
                    });
                }
                router::ExecRoute::Done(status) => {
                    return Ok(ExecSummary {
                        status,
                        prepared: None,
                        db: None,
                    });
                }
                router::ExecRoute::Normal(text, sl) => {
                    if let Some(t) = text {
                        sql = Bytes::from(t);
                    }
                    if !matches!(sl, router::ExecSlot::Main) {
                        let _ = self.bs(cx);
                        self.exec_slot_in(&sl).await?;
                        self.router.probe = if matches!(sl, router::ExecSlot::Branch(..)) {
                            router::Probe::Any
                        } else {
                            router::Probe::Off
                        };
                        slot = Some(sl);
                    }
                }
            }
        }
        let tail = ExecTail {
            mapper,
            logical,
            values,
            cursor,
            had_cursor,
            sql,
            info,
            renames,
            nowait,
            conn_status,
            flags,
            client_bound,
            backend_caps,
        };
        let r = self.exec_tail(cx, stmt, params, tail, out).await;
        let Some(sl) = slot else {
            return r;
        };
        self.router.probe = router::Probe::Off;
        self.exec_slot_out(&sl);
        let held = self.router.hit.take();
        match (r, held) {
            (Err(e), _) => Err(self.exec_failed(&sl, e).await),
            (Ok(_), Some(e)) => {
                let e = self.exec_failed(&sl, e).await;
                let e = self.map_error(e);
                out.error(&e).await?;
                Ok(ExecSummary {
                    status: None,
                    prepared: None,
                    db: None,
                })
            }
            (ok, None) => ok,
        }
    }

    /// The execute on `self.bs` (the session's connection, or the one a
    /// routed statement swapped in).
    #[allow(clippy::too_many_lines)]
    async fn exec_tail(
        &mut self,
        cx: &mut ExecCtx<'_>,
        stmt: StmtRef,
        params: &sprig_proto_mysql::command::ExecuteParams,
        tail: ExecTail,
        out: &mut dyn ResultSink,
    ) -> Result<ExecSummary> {
        let ExecTail {
            mut mapper,
            logical,
            values,
            cursor,
            had_cursor,
            sql,
            info,
            renames,
            nowait,
            conn_status,
            flags,
            client_bound,
            backend_caps,
        } = tail;
        let _ = self.bs(cx);
        // Workload capture: fingerprinted from the logical
        // text the client prepared; cursors are left out (their rows come
        // with later fetches).
        let pending = match &logical {
            Some(l) if !cursor => self.wl_start_stmt(stmt, l),
            _ => None,
        };
        self.wl_probe(pending.as_ref());
        let Some(bs) = self.bs.as_mut() else {
            return Err(SprigError::internal("no backend session"));
        };
        if had_cursor {
            // Executing again closes the old cursor on the server.
            bs.close_cursor();
        }
        if cursor {
            // Pin before routing, so FETCH finds the same connection.
            bs.open_cursor();
        }
        // A write on a branch that records its
        // redo is recorded with its values written in.
        if bs.recording() {
            let cs = bs
                .vars()
                .vars
                .get("character_set_client")
                .cloned()
                .unwrap_or_else(|| "utf8mb4".to_owned());
            let lits: Vec<sprig_router::ParamLit> =
                values.iter().map(|v| router::param_lit(v, &cs)).collect();
            if let Some(t) = sprig_router::render_params(&String::from_utf8_lossy(&sql), &lits) {
                bs.bind_text(t);
            }
        }
        let mut backend_id = 0u32;
        let mut build = |b: &BackendStmt| {
            backend_id = b.id;
            execute_payload(b, flags, &values, client_bound)
        };
        if let Err(e) = bs.execute(&info, &sql, &mut build).await {
            if cursor {
                bs.close_cursor();
            }
            if let Some(l) = &logical {
                let ran = capture::Ran {
                    errored: true,
                    ..capture::Ran::default()
                };
                self.wl_finish(cx, pending, l, &ran);
            }
            return Err(e);
        }
        // A routed statement on a coordinator branch: an ERR goes to the
        // coordinator before the client.
        if self.router.probe_first(bs).await? {
            return Ok(ExecSummary {
                status: None,
                prepared: None,
                db: None,
            });
        }
        let r = Relay {
            more: false,
            format: RowFormat::Binary,
            cursor,
            fetch: false,
            backend_caps: bs.backend_caps().unwrap_or(backend_caps),
            infile: Infile::default(),
            conn_status: conn_status.as_ref(),
            renames: &renames,
            mysql_nowait: nowait,
        };
        let map = self.tenant.as_deref().map(TenantSession::map);
        let accounts = Accounts(self.tenant.as_deref().and_then(TenantSession::auth));
        let aliases = self.tenant.as_deref().map(TenantSession::aliases);
        let mut names = match (&mut mapper, &map) {
            (Some(m), Some(map)) => Some(NamesOut {
                mapper: m,
                ctx: sprig_sql::names::OutputCtx::new(&**map).accounts(&accounts),
                aliases: aliases.as_ref(),
            }),
            _ => None,
        };
        let relayed = relay(bs, out, &r, names.as_mut()).await;
        let outcome = self.finish_outcome(out.finished()).await;
        if let Some(l) = &logical {
            let rendered: Vec<_> = if pending
                .as_ref()
                .is_some_and(crate::workload::Pending::wants_sample)
            {
                let nbe = self.lex_opts().0.no_backslash_escapes;
                params
                    .params
                    .iter()
                    .filter(|p| p.name.is_empty())
                    .map(|p| match &p.value {
                        sprig_proto_mysql::command::ParamValue::Value(v) => {
                            crate::workload::sample_value(v, nbe)
                        }
                        sprig_proto_mysql::command::ParamValue::LongData => {
                            crate::workload::sample_value(&sprig_proto_mysql::Value::Null, nbe)
                        }
                    })
                    .collect()
            } else {
                Vec::new()
            };
            let ran = capture::Ran {
                outcome: outcome.as_ref().ok().and_then(Option::as_ref),
                errored: relayed.as_ref().map_or(true, |r| r.errored),
                params: &rendered,
            };
            self.wl_finish(cx, pending, l, &ran);
        }
        let status = outcome.map(|o| o.map(|o| o.status));
        let opened = matches!(
            relayed,
            Ok(Relayed {
                cursor_opened: true,
                ..
            })
        );
        if cursor && !opened {
            if let Some(bs) = self.bs.as_mut() {
                bs.close_cursor();
            }
        } else if opened && let Ok(s) = self.stmts.get_mut(stmt) {
            s.cursor = Some(backend_id);
        }
        let relayed = relayed?;
        Ok(ExecSummary {
            status: status?.or(relayed.status),
            prepared: None,
            db: None,
        })
    }

    async fn fetch(
        &mut self,
        cx: &mut ExecCtx<'_>,
        stmt: StmtRef,
        rows: u32,
        out: &mut dyn ResultSink,
    ) -> Result<ExecSummary> {
        let backend_caps = self.backend_caps(cx);
        let s = self.stmts.get_mut(stmt)?;
        let Some(id) = s.cursor else {
            return Err(no_open_cursor(stmt));
        };
        let info = s.info.clone();
        let mut p = BytesMut::with_capacity(9);
        p.put_u8(com::STMT_FETCH);
        p.put_u32_le(id);
        p.put_u32_le(rows);
        let bs = self.bs(cx);
        bs.command(&info, p.freeze()).await?;
        let r = Relay {
            more: false,
            format: RowFormat::Binary,
            cursor: false,
            fetch: true,
            backend_caps: bs.backend_caps().unwrap_or(backend_caps),
            infile: Infile::default(),
            conn_status: None,
            renames: &[],
            mysql_nowait: false,
        };
        let relayed = relay(bs, out, &r, None).await;
        let status = self.finish(out.finished()).await;
        let closed = match &relayed {
            Ok(r) => {
                r.errored
                    || r.status
                        .is_some_and(|s| s.contains(StatusFlags::LAST_ROW_SENT))
            }
            Err(_) => true,
        };
        if closed {
            self.close_cursor(stmt);
        }
        let relayed = relayed?;
        Ok(ExecSummary {
            status: status?.or(relayed.status),
            prepared: None,
            db: None,
        })
    }

    /// Forgets `stmt`'s cursor and its pin.
    fn close_cursor(&mut self, stmt: StmtRef) {
        if let Ok(s) = self.stmts.get_mut(stmt)
            && s.cursor.take().is_some()
            && let Some(bs) = self.bs.as_mut()
        {
            bs.close_cursor();
        }
    }

    async fn reset_stmt(
        &mut self,
        cx: &mut ExecCtx<'_>,
        stmt: StmtRef,
        out: &mut dyn ResultSink,
    ) -> Result<ExecSummary> {
        let backend_caps = self.backend_caps(cx);
        let s = self.stmts.get_mut(stmt)?;
        s.clear_long_data();
        let Some(id) = s.cursor else {
            // Nothing on a backend: long data lives here.
            out.ok(OkPacket::new(self.status())).await?;
            return Ok(ExecSummary::default());
        };
        let info = s.info.clone();
        let mut p = BytesMut::with_capacity(5);
        p.put_u8(com::STMT_RESET);
        p.put_u32_le(id);
        let bs = self.bs(cx);
        bs.command(&info, p.freeze()).await?;
        let r = Relay {
            more: false,
            format: RowFormat::Binary,
            cursor: false,
            fetch: false,
            backend_caps: bs.backend_caps().unwrap_or(backend_caps),
            infile: Infile::default(),
            conn_status: None,
            renames: &[],
            mysql_nowait: false,
        };
        let relayed = relay(bs, out, &r, None).await;
        let status = self.finish(out.finished()).await;
        self.close_cursor(stmt);
        let relayed = relayed?;
        Ok(ExecSummary {
            status: status?.or(relayed.status),
            prepared: None,
            db: None,
        })
    }

    async fn bulk(
        &mut self,
        cx: &mut ExecCtx<'_>,
        stmt: StmtRef,
        raw: &sprig_proto_mysql::command::BulkExecute,
        out: &mut dyn ResultSink,
    ) -> Result<ExecSummary> {
        let backend_caps = self.backend_caps(cx);
        let s = self.stmts.get_mut(stmt)?;
        let (sql, info, params, types) = (s.sql.clone(), s.info.clone(), s.params, s.types.clone());
        let bs = self.bs(cx);
        let mut build = |b: &BackendStmt| bulk_payload(b, raw, params, types.as_deref());
        bs.execute(&info, &sql, &mut build).await?;
        let r = Relay {
            more: false,
            format: RowFormat::Binary,
            cursor: false,
            fetch: false,
            backend_caps: bs.backend_caps().unwrap_or(backend_caps),
            infile: Infile::default(),
            conn_status: None,
            renames: &[],
            mysql_nowait: false,
        };
        let relayed = relay(bs, out, &r, None).await?;
        let status = self.finish(out.finished()).await?;
        Ok(ExecSummary {
            status: status.or(relayed.status),
            prepared: None,
            db: None,
        })
    }

    async fn field_list(
        &mut self,
        cx: &mut ExecCtx<'_>,
        table: &[u8],
        wildcard: &[u8],
        out: &mut dyn ResultSink,
    ) -> Result<ExecSummary> {
        let backend_caps = self.backend_caps(cx);
        let mut p = BytesMut::with_capacity(2 + table.len() + wildcard.len());
        p.put_u8(com::FIELD_LIST);
        p.put_slice(table);
        p.put_u8(0);
        p.put_slice(wildcard);
        let map = self.tenant.as_deref().map(TenantSession::map);
        let _ = self.bs(cx);
        let Some(bs) = self.bs.as_mut() else {
            return Err(SprigError::internal("no backend session"));
        };
        bs.command(&StmtInfo::read(), p.freeze()).await?;
        // The definitions come encoded with the serving connection's
        // capabilities.
        let backend_caps = bs.backend_caps().unwrap_or(backend_caps);
        let mut cols: Vec<ColumnDef> = Vec::new();
        let mut done = false;
        while let Some(pk) = bs.next().await? {
            match pk.kind {
                PacketKind::ColumnDef => {
                    let mut c = ColumnDef::decode(&pk.payload, backend_caps)?;
                    if let Some(map) = &map {
                        // Tenant sessions see logical names.
                        sprig_sql::names::map_column_def(
                            &mut c,
                            &sprig_sql::names::OutputCtx::new(&**map),
                        );
                    }
                    cols.push(c);
                }
                PacketKind::End(t) => {
                    let mut status = t.status();
                    status.remove(StatusFlags::SESSION_STATE_CHANGED);
                    out.column_list(
                        &cols,
                        EndOfRows {
                            status,
                            warnings: t.warnings(),
                        },
                    )
                    .await?;
                    done = true;
                }
                PacketKind::Err(e) => {
                    let mut err = backend_error(&e);
                    if let Some(map) = &map {
                        err = relay::map_error(err, &sprig_sql::names::OutputCtx::new(&**map));
                    }
                    out.error(&err).await?;
                    done = true;
                }
                _ => return Err(protocol_error("in a COM_FIELD_LIST response")),
            }
        }
        let status = self.finish(out.finished()).await?;
        if !done {
            return Err(protocol_error(
                "COM_FIELD_LIST response without a terminator",
            ));
        }
        Ok(ExecSummary {
            status,
            prepared: None,
            db: None,
        })
    }
}

#[async_trait]
impl SessionExec for PassSession {
    fn idle_timeout(&self) -> Option<Duration> {
        // The client's own value (a gateway-owned variable):
        // backend connections keep the gateway's.
        let v = self.bs.as_ref()?.vars().gateway_vars.get("wait_timeout")?;
        let secs: u64 = v.trim().parse().ok()?;
        (secs > 0).then(|| Duration::from_secs(secs))
    }

    async fn execute(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: Request,
        out: &mut dyn ResultSink,
    ) -> Result<ExecSummary> {
        self.packet_limit = u64::try_from(cx.proto.max_packet).ok();
        // `SET @sprig_dc` is the gateway's, never MariaDB's.
        if let Some(r) = self.sprig_dc_set(cx, &req, out).await {
            return r;
        }
        // Queue acknowledgements held for this transaction learn how
        // it ended (the commit hook, after the request).
        let txn_word = queues::txn_word(&req);
        // The tenant's `SET GLOBAL` defaults, before the session's first
        // statement.
        if !self.globals_applied && self.tenant.is_some() {
            self.globals_applied = true;
            Box::pin(self.apply_tenant_globals(cx)).await;
        }
        // Holds, promotions, the DC's write gate, and a mover's fence met on
        // the way: `fence_retry`.
        let r = match Box::pin(self.run_followed(cx, req, out)).await {
            Ok(r) => r,
            Err(e) => return Err(self.map_error(e)),
        };
        self.mq_after_request(u64::from(cx.session.id.0), txn_word, r.is_ok());
        // Cross-tenant publishes of the transaction go
        // out once it has ended.
        crate::inherited::outbox::after_request(
            u64::from(cx.session.id.0),
            self.offsets_in_transaction(),
        );
        // Tenant sessions: no error the gateway returns carries a physical
        // name (the relay maps the ones it sends itself).
        r.map_err(|e| self.map_error(e))
    }

    async fn init_db(&mut self, cx: &mut ExecCtx<'_>, schema: &str) -> Result<()> {
        if self.tenant.is_some() {
            let r = self.tenant_init_db(cx, schema).await;
            return r.map_err(|e| self.map_error(e));
        }
        self.init_physical(cx, schema).await
    }

    async fn reset(&mut self, _cx: &mut ExecCtx<'_>) -> Result<()> {
        let n = self.stmts_len();
        self.stmts.clear();
        metrics::gauge!("sprig_mysql_prepared_statements", "persona" => self.persona.as_str())
            .decrement(n);
        if let Some(ts) = self.tenant.as_deref_mut() {
            ts.reset();
        }
        // A reset session starts from the tenant's defaults again.
        self.globals_applied = false;
        if let Some(wl) = self.wl.as_mut() {
            wl.reset();
        }
        self.router.reset().await;
        match self.bs.as_mut() {
            Some(bs) => bs.reset().await,
            None => Ok(()),
        }
    }

    async fn abandoned(&mut self, _cx: &mut ExecCtx<'_>, why: Abandon) {
        // The router's statements on other connections too.
        self.router.abandon_in_flight(DRAIN_TIMEOUT).await;
        let Some(bs) = self.bs.as_mut() else {
            return;
        };
        match why {
            // KILL QUERY on the backend, then drain the killed statement so
            // the connection and an open transaction survive (as on MySQL).
            Abandon::Killed => bs.abandon(DRAIN_TIMEOUT).await,
            // The session ends: stop the backend's work, drop the connection.
            Abandon::ClientGone => {
                if let Err(e) = tokio::time::timeout(KILL_TIMEOUT, bs.kill_query())
                    .await
                    .unwrap_or_else(|_| Err(SprigError::internal("KILL QUERY timed out")))
                {
                    tracing::debug!(error = %e, "KILL QUERY on the backend failed");
                }
                bs.close().await;
            }
        }
    }

    fn set_internal(&mut self, on: bool) {
        // The document engine's own statements (see the trait); the same
        // flag typed-table plans set.
        if let Some(ts) = self.tenant.as_deref_mut() {
            ts.types.internal = on;
        }
    }

    fn row_policy_binds(&self, object: u64) -> Option<bool> {
        // The session's compiled policies at its catalog.
        let ts = self.tenant.as_deref()?;
        let cat = ts.catalog()?;
        let p = crate::tenant::authz::privileges(ts, cat);
        Some(
            p.row_policies()
                .is_some_and(|r| r.tables.contains_key(&sprig_common::ids::ObjectId(object))),
        )
    }

    fn row_policy_docs(&self) -> Option<std::sync::Arc<crate::tenant::rls::DocViews>> {
        // The change stream's view of the tenant's documents.
        let ts = self.tenant.as_deref()?;
        let cat = ts.catalog()?;
        let p = crate::tenant::authz::privileges(ts, cat);
        Some(std::sync::Arc::new(crate::tenant::rls::doc_views(
            ts,
            cat,
            p.row_policies().as_deref(),
        )))
    }

    fn set_claims(&mut self, claims: Option<std::sync::Arc<serde_json::Value>>) {
        // SPRIG_CLAIM() reads the caller's verified token.
        if let Some(ts) = self.tenant.as_deref_mut() {
            ts.rls.claims = claims;
        }
    }

    fn holds_tenant_privilege(&self, action: sprig_auth::authz::Action) -> Option<bool> {
        // `$currentOp` with `allUsers` needs `PROCESS`.
        let ts = self.tenant.as_deref()?;
        let cat = ts.catalog()?;
        let p = crate::tenant::authz::privileges(ts, cat);
        Some(p.unrestricted() || p.allows(action, sprig_auth::authz::Target::Tenant))
    }

    fn attach_capture(&mut self, capture: crate::workload::SessionCapture) {
        // Tenant sessions only: accounts-file logins have no catalog.
        if self.tenant.is_some() {
            self.wl = Some(capture);
        }
    }

    fn workload(&self, db: &str, table: &str) -> Option<crate::workload::FrontCapture> {
        use sprig_workload::shape::TableResolver as _;
        let wl = self.wl.as_ref()?.workload().clone();
        let ts = self.tenant.as_deref()?;
        let cat = ts.catalog().cloned();
        let samples = self.wl_capture_allowed(ts, cat.as_deref());
        let map = ts.map();
        let resolver = crate::workload::CatalogResolver::new(
            cat.as_deref(),
            &*map,
            ts.lower_case_table_names(),
        );
        Some(crate::workload::FrontCapture {
            workload: wl,
            samples,
            table: resolver.resolve(Some(db), table),
        })
    }

    async fn close(&mut self) {
        let n = self.stmts_len();
        self.stmts.clear();
        metrics::gauge!("sprig_mysql_prepared_statements", "persona" => self.persona.as_str())
            .decrement(n);
        self.router.close().await;
        if let Some(bs) = self.bs.as_mut() {
            bs.close().await;
        }
    }
}

impl PassSession {
    /// The variables the persona answers: its version
    /// string, `version_comment`, and on 3306 `gtid_mode`.
    fn persona_vars(&self) -> sprig_sql::compat::vars::PersonaVars {
        let vars = self.bs.as_ref().map(BackendSession::vars);
        // Before the session's first backend statement there are no
        // session variables yet: 3306 answers MySQL's default `sql_mode`
        // (a client's first `SELECT @@sql_mode` read the backend value).
        let login_mode = (vars.is_none() && self.persona == Persona::MySql84)
            .then(|| sprig_sql::compat::sqlmode::MYSQL84_DEFAULT.render());
        sprig_sql::compat::vars::PersonaVars::new(
            self.persona,
            self.settings.persona_version(self.persona),
        )
        .with_session(
            vars.and_then(|v| v.client_sql_mode.as_deref())
                .or(login_mode.as_deref()),
            vars.and_then(|v| v.vars.get("max_statement_time").map(String::as_str)),
        )
        .with_max_allowed_packet(self.packet_limit)
        .with_max_connections(self.tenant_max_connections())
    }

    /// A tenant session's `@@max_connections`: the tenant's own limit
    /// (`limits.tenant_connections`), not the backend's.
    /// MySQL Connector/J's suite raises it with `SET GLOBAL` when it reads
    /// less than 200 free.
    fn tenant_max_connections(&self) -> Option<u64> {
        let ts = self.tenant.as_deref()?;
        let cat = ts.catalog()?;
        let svc = ts.services();
        let ctx = cat.setting_ctx(&svc.dc);
        Some(svc.settings.get(
            &crate::limits::TENANT_CONNECTIONS,
            &ctx,
            Some(&cat.settings),
        ))
    }

    /// The rows of `SHOW [SESSION] STATUS` (the client's TLS and
    /// compression) or `SHOW VARIABLES` (the persona's version strings)
    /// the relay rewrites; `None` for other statements.
    fn conn_status_for(
        &self,
        piece: &[u8],
        proto: &sprig_proto_mysql::state::ProtoState,
    ) -> Option<relay::ConnStatus> {
        if split::is_session_status(piece) {
            Some(relay::ConnStatus::of(proto).with_rows(self.status_rows()))
        } else if split::is_show_variables(piece) {
            let mut rows = self.persona_vars().show_rows();
            rows.extend(self.owned_var_rows());
            let st = relay::ConnStatus::from_rows(rows);
            // 3306: `collation_*` in MySQL's names.
            Some(if self.persona == Persona::MySql84 {
                st.with_mysql_collations()
            } else {
                st
            })
        } else {
            None
        }
    }

    /// The client's own values of gateway-owned variables (`SET
    /// wait_timeout = ...`), which `@@` reads already answer.
    fn owned_var_rows(&self) -> Vec<(&'static str, String)> {
        const OWNED: &[&str] = &[
            "wait_timeout",
            "interactive_timeout",
            "net_read_timeout",
            "net_write_timeout",
            "session_track_schema",
            "session_track_state_change",
            "session_track_system_variables",
            "session_track_transaction_info",
            "wsrep_sync_wait",
        ];
        let mut rows = Vec::new();
        if let Some(bs) = &self.bs {
            for (k, v) in &bs.vars().gateway_vars {
                if let Some(name) = OWNED.iter().find(|n| n.eq_ignore_ascii_case(k)) {
                    rows.push((*name, v.clone()));
                }
            }
        }
        rows
    }

    /// 3306: `performance_schema` and `sys` as derived tables over the
    /// session and the tenant's sessions (`compat::perfschema`). `None` when nothing changes.
    pub(super) fn ps_rewrite(&self, cx: &ExecCtx<'_>, sql: &[u8]) -> Option<Result<Vec<u8>>> {
        if self.persona != Persona::MySql84 {
            return None;
        }
        let current = self.current_logical();
        if !sprig_sql::compat::perfschema::wanted(sql, current.as_deref()) {
            return None;
        }
        let (lex, _) = self.lex_opts();
        // `TRUNCATE` of the digest summary starts the
        // tenant's digests again; the backend answers a no-op.
        if sprig_sql::compat::perfschema::is_truncate_digests(sql, current.as_deref()) {
            if let (Some(wl), Some(ts)) = (self.wl.as_ref(), self.tenant.as_deref()) {
                wl.workload().recorder().reset_digests(ts.tenant);
            }
            return Some(Ok(b"DO 0".to_vec()));
        }
        let vars = self.persona_vars();
        let owned = self.owned_var_rows();
        let status = relay::ConnStatus::of(cx.proto)
            .with_rows(self.status_rows())
            .into_rows();
        let digests = if sprig_sql::compat::perfschema::wants_digests(sql) {
            self.ps_digests()
        } else {
            Vec::new()
        };
        let s = sprig_sql::compat::perfschema::PsSession {
            vars: &vars,
            owned_vars: &owned,
            status: &status,
            uptime: status::uptime_secs(),
            connection_id: u64::from(cx.session.id.0),
            connect_attrs: &cx.session.connect_attrs,
            current_db: current.as_deref(),
            digests: &digests,
        };
        sprig_sql::compat::perfschema::rewrite(sql, &lex, &s)
            .map(|r| r.map_err(|e| SprigError::standard(e.code, e.sqlstate, e.message)))
    }

    /// The tenant's statement digests on this gateway (the workload
    /// recorder's) in the databases the user may see; statements
    /// run with no current database are listed for every user.
    fn ps_digests(&self) -> Vec<sprig_sql::compat::perfschema::PsDigest> {
        let (Some(wl), Some(ts)) = (self.wl.as_ref(), self.tenant.as_deref()) else {
            return Vec::new();
        };
        let Some(cat) = ts.catalog().cloned() else {
            return Vec::new();
        };
        let p = crate::tenant::authz::privileges(ts, &cat);
        let lctn = ts.lower_case_table_names();
        let us = |t: Option<std::time::SystemTime>| {
            t.and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
        };
        wl.workload()
            .recorder()
            .digests(ts.tenant)
            .into_iter()
            .filter_map(|r| {
                let schema = cat.databases.get(r.db_id).map(|d| d.name.clone());
                if let Some(name) = &schema
                    && !p
                        .sees_database(&sprig_catalog::ddl::database_key(name, lctn), Some(r.db_id))
                {
                    return None;
                }
                let d = &r.digest;
                let q = |x: f64| d.latency_hist.quantile_us(x).unwrap_or(d.max_us);
                Some(sprig_sql::compat::perfschema::PsDigest {
                    schema,
                    digest: format!("{:016x}", r.id.0),
                    text: r.text.to_string(),
                    count: d.exec_count,
                    errors: d.error_count,
                    sum_us: d.total_us,
                    min_us: d.min_us,
                    max_us: d.max_us,
                    rows_affected: d.rows_affected,
                    rows_sent: d.rows_returned,
                    rows_examined: d.rows_examined,
                    first_seen_us: us(d.first_seen),
                    last_seen_us: us(d.last_seen),
                    p95_us: q(0.95),
                    p99_us: q(0.99),
                    p999_us: q(0.999),
                })
            })
            .collect()
    }

    /// The gateway's rewrites of a statement's text for this session: the
    /// client's own values of gateway-owned variables, the persona's
    /// variables (`@@version`, `VERSION()`) and
    /// `CONNECTION_ID()`. The renames give the columns back their names.
    fn session_rewrites(&self, sql: Bytes, conn_id: u32) -> (Bytes, Vec<split::Renamed>) {
        let mut renames = Vec::new();
        let (lex, _) = self.lex_opts();
        let mut vars = self.persona_vars();
        // `@@global.<v>` of the tenant's own defaults.
        if sql.windows(7).any(|w| w.eq_ignore_ascii_case(b"@global")) {
            vars = vars.with_tenant_globals(self.tenant_globals());
        }
        // CURRENT_ROLE(): the gateway's active roles.
        if sql
            .windows(12)
            .any(|w| w.eq_ignore_ascii_case(b"current_role"))
        {
            vars = vars.with_current_role(
                self.tenant
                    .as_deref()
                    .map(|ts| crate::tenant::authz::current_role(ts, self.persona)),
            );
        }
        let sql = match sprig_sql::compat::vars::rewrite_reads(&sql, &lex, &vars) {
            Some((s, r)) => {
                renames.extend(r.into_iter().map(|r| split::Renamed {
                    from: Bytes::from(r.column),
                    to: Bytes::from(r.written),
                }));
                Bytes::from(s)
            }
            None => sql,
        };
        let sql = match self
            .bs
            .as_ref()
            .and_then(|b| split::rewrite_gateway_vars(&sql, &b.vars().gateway_vars))
        {
            Some((s, r)) => {
                renames.extend(r);
                s
            }
            None => sql,
        };
        match split::rewrite_connection_id(&sql, conn_id) {
            Some((s, r)) => {
                renames.extend(r);
                (s, renames)
            }
            None => (sql, renames),
        }
    }

    /// One request, dispatched (see `SessionExec::execute`).
    async fn run_request(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: Request,
        out: &mut dyn ResultSink,
    ) -> Result<ExecSummary> {
        match req {
            Request::Query { sql, attrs } => {
                if self.tenant.is_some() {
                    self.tenant_query(cx, sql, &attrs, out).await
                } else {
                    self.query(cx, sql, out).await
                }
            }
            Request::Prepare { sql } => {
                if self.tenant.is_some() {
                    self.tenant_prepare(cx, sql).await
                } else {
                    self.prepare(cx, sql).await
                }
            }
            Request::Execute { stmt, raw, params } => {
                // 3306: MySQL's `'table.key'` in 1062 for prepared
                // statements too (MySQL Connector/J's suite checks it).
                let dup_table = if self.persona == Persona::MySql84 {
                    let lex = self.lex_opts().0;
                    self.stmts
                        .get_mut(stmt)
                        .ok()
                        .and_then(|s| s.logical.clone())
                        .and_then(|l| sprig_sql::compat::errmap::dup_key_table(&l, &lex))
                } else {
                    None
                };
                // 3306: MySQL's integer result types.
                let logical = (self.persona == Persona::MySql84 && dup_table.is_none())
                    .then(|| {
                        self.stmts
                            .get_mut(stmt)
                            .ok()
                            .and_then(|s| s.logical.clone())
                    })
                    .flatten()
                    .filter(|l| compat_sink::IntMetaSink::wanted(l));
                match (dup_table.as_deref(), logical) {
                    (Some(table), _) => {
                        let mut wrapped = compat_sink::DupKeySink { inner: out, table };
                        self.execute(cx, stmt, &raw, &params, &mut wrapped)
                            .await
                            .map_err(|e| compat_sink::map(e, Some(table)))
                    }
                    (None, Some(l)) => {
                        let lex = self.lex_opts().0;
                        let mut wrapped = compat_sink::IntMetaSink::new(out, &l, lex);
                        self.execute(cx, stmt, &raw, &params, &mut wrapped).await
                    }
                    (None, None) => self.execute(cx, stmt, &raw, &params, out).await,
                }
            }
            Request::Fetch { stmt, rows } => self.fetch(cx, stmt, rows, out).await,
            Request::LongData { stmt, param, data } => {
                let limit = match self.tenant.as_deref() {
                    // Followed live (a resolved-setting watch).
                    Some(ts) => ts.long_data_max(),
                    None => self.settings.current().long_data_max,
                };
                // An unknown statement is reported by nothing: long data
                // has no response.
                if let Ok(s) = self.stmts.get_mut(stmt) {
                    s.add_long_data(param, &data, limit);
                }
                Ok(ExecSummary::default())
            }
            Request::ResetStmt { stmt } => self.reset_stmt(cx, stmt, out).await,
            Request::CloseStmt { stmt } => {
                self.close_cursor(stmt);
                if self.stmts.remove(stmt).is_some() {
                    metrics::gauge!("sprig_mysql_prepared_statements", "persona" => self.persona.as_str()).decrement(1.0);
                }
                Ok(ExecSummary::default())
            }
            Request::BulkExecute { stmt, raw } => self.bulk(cx, stmt, &raw, out).await,
            Request::FieldList { table, wildcard } => {
                self.field_list(cx, &table, &wildcard, out).await
            }
        }
    }

    /// A tenant session's error with physical names mapped to logical ones
    /// (another tenant's as `?`).
    fn map_error(&self, e: SprigError) -> SprigError {
        let Some(ts) = self.tenant.as_deref() else {
            return e;
        };
        let map = ts.map();
        let accounts = Accounts(ts.auth());
        let mut e = relay::map_error(
            e,
            &sprig_sql::names::OutputCtx::new(&*map).accounts(&accounts),
        );
        if let Some(m) = ts.aliases().apply(e.message.as_bytes()) {
            e.message = String::from_utf8_lossy(&m).into_owned();
        }
        // A validation's CHECK failure is 39863 with its message.
        logic_bound::map_validation(ts, e)
    }

    #[allow(clippy::cast_precision_loss)] // a gauge
    fn stmts_len(&self) -> f64 {
        self.stmts.len() as f64
    }

    /// `USE schema` with a physical (or system) schema name.
    async fn init_physical(&mut self, cx: &mut ExecCtx<'_>, schema: &str) -> Result<()> {
        let sql = format!("USE {}", crate::backend::state::quote_ident(schema));
        let info = StmtInfo::classify(sql.as_bytes());
        let bs = self.bs(cx);
        bs.query(&info, sql.as_bytes()).await?;
        let mut err = None;
        while let Some(p) = bs.next().await? {
            if let PacketKind::Err(e) = p.kind {
                err = Some(backend_error(&e));
            }
        }
        bs.finish().await?;
        err.map_or(Ok(()), Err)
    }
}
