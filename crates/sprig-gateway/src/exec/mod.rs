//! The pass-through executor (decision D3; chapter 05 build steps 7-9,
//! chapter 06 "Acquire, sync and release"): every request of a session
//! goes, as MySQL text or binary protocol, to the login's cluster through
//! the session's [`BackendSession`], and the response streams back through
//! the [relay engine](relay). The router (chapter 17) replaces it in R1
//! behind the same [`Executor`] trait.
//!
//! | Request | Backend |
//! |---|---|
//! | `Query` | Split into statements ([`split`]); each is classified, routed (reads may go to a replica, chapter 06) and relayed; all but the last get `MORE_RESULTS_EXISTS`; an ERR stops the rest. More than one statement without multi-statements fails with 1064 |
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
mod fence;
mod follow;
pub mod relay;
pub(crate) mod router;
pub mod settings;
pub mod split;
pub mod stmt;
// Chapter 41: logic declarations (R7).
mod logic;
// Chapter 41: validations, triggers, schedules, computed fields (R7 orders 3, 5).
mod logic_bound;
mod tenant;
// Chapter 32: typed tables and record hierarchies.
pub(crate) mod inherit;
pub(crate) mod types;

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

    /// The same executor with tenant sessions (M0.3): catalog logins get
    /// names virtualized, DDL through the catalog and settings in SQL (see
    /// `exec::tenant`). Accounts-file logins keep M0.2's pass-through.
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
    /// A catalog login's tenant state (M0.3); `None` for accounts-file
    /// logins, whose names pass through unchanged (decision D2).
    tenant: Option<Box<TenantSession>>,
    /// Columns the name rewrite's substitutions renamed (`DATABASE()`
    /// became `'shop'`): the next forwarded statement gives them back.
    subst: Vec<split::Renamed>,
    /// Workload capture (chapter 12), for tenant sessions.
    wl: Option<crate::workload::SessionCapture>,
    /// The current `COM_QUERY` holds several statements: none is probed.
    wl_multi: bool,
    /// Chapter 17's router: the session's connections to other clusters
    /// and chapter 18's coordinator, for tenants with sharded tables.
    router: router::RouterState,
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
/// (chapter 28; the block itself never reaches a MySQL client).
fn mariadb_caps(persona: Persona, client: Caps) -> Caps {
    match persona {
        Persona::MariaDb => client & MARIADB_PASS_CAPS,
        Persona::MySql84 => Caps::MARIADB_CLIENT_EXTENDED_METADATA,
    }
}

/// 1064, as MySQL answers several statements without multi-statements.
fn multi_statements_off(rest: &[u8]) -> SprigError {
    let near: String = String::from_utf8_lossy(rest).chars().take(80).collect();
    SprigError::standard(
        1064,
        *b"42000",
        format!(
            "You have an error in your SQL syntax; check the manual that corresponds to your server version for the right syntax to use near '{}' at line 1",
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
        // A transaction chapter 18's coordinator holds (its branches are
        // other connections).
        if self.router.in_txn() {
            s |= StatusFlags::IN_TRANS;
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
            return Err(multi_statements_off(&sql[second.min(sql.len())..]));
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
        let prepared = self.bs(cx).prepare(&info, &sql).await?;
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
        // One statement per execute: probes allowed (chapter 12).
        self.wl_multi = false;
        let s = self.stmts.get_mut(stmt)?;
        if let Some(text) = s.deferred.clone() {
            // A statement the gateway answers itself, planned again now.
            let step = self
                .tenant_text(
                    cx,
                    &text,
                    &QueryAttrs::default(),
                    false,
                    RowFormat::Binary,
                    out,
                )
                .await?;
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
        // Chapter 29: privileges are checked again at each execute, as on
        // MySQL, when they changed since the prepare.
        if let Some(rc) = &recheck
            && let Some(ts) = self.tenant.as_deref_mut()
        {
            ts.refresh().await?;
            crate::tenant::authz::recheck(ts, persona, rc)?;
        }
        // Chapter 17: a sharded tenant's statement is routed (see
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
        // Workload capture (chapter 12): fingerprinted from the logical
        // text the client prepared; cursors are left out (their rows come
        // with later fetches).
        let pending = match &logical {
            Some(l) if !cursor => self.wl_start(l),
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
                        // Tenant sessions see logical names (chapter 07).
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
    async fn execute(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: Request,
        out: &mut dyn ResultSink,
    ) -> Result<ExecSummary> {
        // Chapter 24: `SET @sprig_dc` is the gateway's, never MariaDB's.
        if let Some(r) = self.sprig_dc_set(cx, &req, out).await {
            return r;
        }
        // Tenant sessions wait out a mover's hold and follow a move.
        if let Err(e) = self.follow_tenant().await {
            return Err(self.map_error(e));
        }
        // Chapter 25 (R2): writes of tenants homed here need the DC's
        // permit, and so do their acknowledgements.
        let gate = match self.dc_write_gate(&req).await {
            Ok(g) => g,
            Err(e) => return Err(self.map_error(e)),
        };
        self.count_traffic(cx, &req);
        let r = match gate {
            Some(f) => {
                let mut gated = crate::failover::GatedSink::new(out, f);
                self.run_request(cx, req, &mut gated).await
            }
            None => self.run_request(cx, req, out).await,
        };
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

    fn attach_capture(&mut self, capture: crate::workload::SessionCapture) {
        // Tenant sessions only: accounts-file logins have no catalog.
        if self.tenant.is_some() {
            self.wl = Some(capture);
        }
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
    /// The variables the persona answers (chapter 28): its version
    /// string, `version_comment`, and on 3306 `gtid_mode`.
    fn persona_vars(&self) -> sprig_sql::compat::vars::PersonaVars {
        let vars = self.bs.as_ref().map(BackendSession::vars);
        sprig_sql::compat::vars::PersonaVars::new(
            self.persona,
            self.settings.persona_version(self.persona),
        )
        .with_session(
            vars.and_then(|v| v.client_sql_mode.as_deref()),
            vars.and_then(|v| v.vars.get("max_statement_time").map(String::as_str)),
        )
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
            Some(relay::ConnStatus::of(proto))
        } else if split::is_show_variables(piece) {
            Some(relay::ConnStatus::from_rows(
                self.persona_vars().show_rows(),
            ))
        } else {
            None
        }
    }

    /// The gateway's rewrites of a statement's text for this session: the
    /// client's own values of gateway-owned variables, the persona's
    /// variables (`@@version`, `VERSION()`, chapter 28) and
    /// `CONNECTION_ID()`. The renames give the columns back their names.
    fn session_rewrites(&self, sql: Bytes, conn_id: u32) -> (Bytes, Vec<split::Renamed>) {
        let mut renames = Vec::new();
        let (lex, _) = self.lex_opts();
        let mut vars = self.persona_vars();
        // CURRENT_ROLE(): the gateway's active roles (chapter 29, R4).
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
                self.execute(cx, stmt, &raw, &params, out).await
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
        // Chapter 41: a validation's CHECK failure is 39863 with its
        // message (R7 order 3).
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
