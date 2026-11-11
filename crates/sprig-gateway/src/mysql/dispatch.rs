//! Command dispatch (chapter 05, "Command table"; build step 6).
//!
//! [`CommandLoop`] reads one command at a time (the codec's sequence ids
//! restart with each), parses it, and either answers it in the gateway or
//! turns it into a [`Request`] for the session's executor:
//!
//! | Command | Handling |
//! |---|---|
//! | `COM_QUIT` | Close; the executor rolls back and releases |
//! | `COM_PING` | OK, no backend round trip |
//! | `COM_INIT_DB` | `SessionExec::init_db`; OK with the schema in session tracking |
//! | `COM_QUERY` | Gateway statements (version variables, `KILL`, `SHOW PROCESSLIST`) locally; the rest through the executor |
//! | `COM_FIELD_LIST`, `COM_STMT_*` | Through the executor; client statement ids mapped here |
//! | `COM_CREATE_DB`, `COM_DROP_DB` | As `CREATE DATABASE` / `DROP DATABASE` |
//! | `COM_STATISTICS`, `COM_PROCESS_INFO`, `COM_PROCESS_KILL` | From the `SessionTable` |
//! | `COM_CHANGE_USER` | Re-authenticate (a fresh nonce when the plugin switches); new executor session |
//! | `COM_RESET_CONNECTION` | Reset the executor session and statements; OK |
//! | `COM_SET_OPTION` | Multi-statements on or off; EOF (OK form under `DEPRECATE_EOF`) |
//! | refresh, shutdown, debug, binlog | 1227 |
//! | anything else | 1047 |
//!
//! Before each command the loop checks the session's flags: a session
//! asked to close at a safe point (drain, a retired listener) answers its
//! next command with the reason's error (39000 or 39004) and closes.
//!
//! While a statement runs, the loop also watches for `KILL QUERY` (the
//! statement's future is dropped, the executor told, and the client gets
//! 1317) and for the client hanging up (the executor is told and the
//! session ends). `KILL CONNECTION` drops the whole session in the session
//! table's task wrapper.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt as _};
use sprig_common::ids::ConnId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::auth::{AuthIo, AuthVerdict, authenticate};
use sprig_proto_mysql::command::{
    ChangeUser, MYSQL_OPTION_MULTI_STATEMENTS_OFF, MYSQL_OPTION_MULTI_STATEMENTS_ON, ParamInfo,
    StmtExecute, com,
};
use sprig_proto_mysql::error::StdErr;
use sprig_proto_mysql::packets::{EofPacket, OkPacket, SessionChange};
use sprig_proto_mysql::resultset::PrepareOk;
use sprig_proto_mysql::state::ProtoState;
use sprig_proto_mysql::{Caps, Command, ErrPacket, PacketCodec, Persona, StatusFlags};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use super::local::{self, Local};
use super::settings::FrontendSettings;
use super::sink::WireSink;
use super::watch::ClientWatch;
use super::{LoggedIn, Shared};
use crate::frontend::{GatewayContext, MaybeTls};
use crate::pipeline::{
    Abandon, ExecCtx, ExecSummary, LoginInfo, PreparedMeta, Request, ResultSink, SessionExec,
    StmtRef,
};
use crate::session::{Session, SessionState};

/// The client stream, paced so reading a large packet yields to other
/// sessions ([`crate::offload::Paced`]).
type ClientFramed = Framed<crate::offload::Paced<MaybeTls<TcpStream>>, PacketCodec>;

/// The initial write buffer of a client connection.
const WRITE_CAPACITY: usize = 16 * 1024;

/// What the loop does after a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Flow {
    Continue,
    Close,
}

/// How a statement sent to the executor ended.
enum Ran {
    /// It finished; the response went to the client (or, for a prepare,
    /// is in the summary).
    Done(ExecSummary),
    /// An ERR went to the client instead.
    Failed,
    /// The client is gone: end the session.
    Gone,
}

/// A client's prepared statement.
#[derive(Debug, Clone)]
struct ClientStmt {
    exec: StmtRef,
    params: u16,
    /// Types bound by the last execute that sent them.
    param_types: Option<Vec<u16>>,
    /// Parameters that got long data since the last execute.
    long_data: Vec<bool>,
}

/// The session's statements, by client id (assigned here, from 1).
#[derive(Debug, Default)]
struct Stmts {
    next: u32,
    map: HashMap<u32, ClientStmt>,
    last: Option<u32>,
}

impl Stmts {
    fn insert(&mut self, meta: &PreparedMeta) -> u32 {
        loop {
            self.next = self.next.wrapping_add(1).max(1);
            if self.next != u32::MAX && !self.map.contains_key(&self.next) {
                break;
            }
        }
        let params = u16::try_from(meta.params.len()).unwrap_or(u16::MAX);
        self.map.insert(
            self.next,
            ClientStmt {
                exec: meta.stmt,
                params,
                param_types: None,
                long_data: Vec::new(),
            },
        );
        self.last = Some(self.next);
        self.next
    }

    /// The id a command names; on 3307, 0xFFFFFFFF means the last one
    /// prepared.
    fn resolve(&self, persona: Persona, id: u32) -> u32 {
        if persona == Persona::MariaDb && id == u32::MAX {
            self.last.unwrap_or(id)
        } else {
            id
        }
    }

    fn clear(&mut self) {
        self.map.clear();
        self.last = None;
    }
}

/// One logged-in connection's command phase.
pub struct CommandLoop {
    shared: Arc<Shared>,
    ctx: Arc<GatewayContext>,
    session: Session,
    framed: ClientFramed,
    proto: ProtoState,
    exec: Box<dyn SessionExec>,
    db: Option<String>,
    status: StatusFlags,
    stmts: Stmts,
    watch: ClientWatch,
    settings: FrontendSettings,
}

impl std::fmt::Debug for CommandLoop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandLoop")
            .field("session", &self.session.id)
            .field("persona", &self.proto.persona)
            .finish_non_exhaustive()
    }
}

fn interrupted() -> SprigError {
    SprigError::standard(1317, *b"70100", "Query execution was interrupted")
}

fn no_response() -> SprigError {
    SprigError::new(
        SprigCode::BackendProtocol,
        "the executor finished without a response",
    )
}

impl CommandLoop {
    pub(crate) fn new(
        shared: Arc<Shared>,
        ctx: Arc<GatewayContext>,
        mut session: Session,
        l: LoggedIn,
        settings: FrontendSettings,
    ) -> Self {
        let watch = ClientWatch::new(&l.stream);
        let mut codec = PacketCodec::new(l.proto.max_packet);
        // Compression starts after the auth OK packet (chapter 05).
        codec.set_framing(l.proto.framing);
        let mut framed =
            Framed::with_capacity(crate::offload::Paced::new(l.stream), codec, WRITE_CAPACITY);
        framed.set_backpressure_boundary(settings.relay_buffer.max(WRITE_CAPACITY));
        // 3306's performance_schema.session_connect_attrs (R4 round 4).
        session.connect_attrs = l.attrs;
        Self {
            shared,
            ctx,
            session,
            framed,
            proto: l.proto,
            exec: l.exec,
            db: l.db,
            status: StatusFlags::AUTOCOMMIT,
            stmts: Stmts::default(),
            watch,
            settings,
        }
    }

    /// Runs commands until the client quits or the session must close.
    ///
    /// # Errors
    /// A protocol or transport error that ended the connection.
    pub async fn run(mut self) -> Result<()> {
        let r = self.serve().await;
        self.session.set_state(SessionState::Closing);
        self.exec.close().await;
        r
    }

    async fn serve(&mut self) -> Result<()> {
        loop {
            self.framed.codec_mut().reset_seq();
            // A drain closes an idle session (outside a transaction) at once
            // (chapter 04, "Drain" step 3), silently: an unsolicited ERR
            // packet is read out of turn by most clients ("Packets out of
            // sync"), so the client sees the closed connection on its next
            // use and reconnects. Other reasons (a stricter listener, a
            // revoked account) wait for the next command, which gets the
            // reason's error.
            let idle_close = !self.status.contains(StatusFlags::IN_TRANS);
            let handle = Arc::clone(self.session.handle());
            let mut watch_close = idle_close;
            // The client's `wait_timeout`, when it set one: an idle session
            // is closed as the servers close it. MySQL 8.0.24 and later
            // first send 4031 (ER_CLIENT_INTERACTION_TIMEOUT); MariaDB
            // closes silently.
            let idle = self.exec.idle_timeout();
            let idle_sleep = tokio::time::sleep(idle.unwrap_or(Duration::from_secs(86_400)));
            tokio::pin!(idle_sleep);
            let mut timed_out = false;
            let next = loop {
                tokio::select! {
                    biased;
                    n = self.framed.next() => break Some(n),
                    () = handle.flags().close_wanted(), if watch_close => {
                        if handle.flags().close_reason() == Some(crate::session::CloseReason::Drain)
                            || handle.flags().conn_killed()
                        {
                            break None;
                        }
                        watch_close = false;
                    }
                    () = &mut idle_sleep, if idle.is_some() => {
                        timed_out = true;
                        break None;
                    }
                }
            };
            if timed_out {
                tracing::debug!(session = %self.session.id, "idle session closed: the client's wait_timeout passed");
                if self.proto.persona == Persona::MySql84 {
                    let p = ErrPacket::new(
                        4031,
                        *b"HY000",
                        "The client was disconnected by the server because of inactivity. \
                         See wait_timeout and interactive_timeout for configuring this behavior."
                            .to_owned(),
                    );
                    let _ = <ClientFramed as SinkExt<Bytes>>::send(
                        &mut self.framed,
                        p.encode(self.proto.caps),
                    )
                    .await;
                }
                return Ok(());
            }
            let Some(next) = next else {
                tracing::debug!(session = %self.session.id, "idle session closed: the gateway is draining");
                return Ok(());
            };
            let payload = match next {
                None => return Ok(()),
                Some(Err(e)) => {
                    let (code, state) = e.errno();
                    let msg = SprigError::from(e);
                    let p = ErrPacket::new(code, state, msg.message.clone());
                    let _ = <ClientFramed as SinkExt<Bytes>>::send(
                        &mut self.framed,
                        p.encode(self.proto.caps),
                    )
                    .await;
                    return Err(msg);
                }
                Some(Ok(p)) => p.payload,
            };
            if payload.first() == Some(&com::QUIT) {
                return Ok(());
            }
            if self.session.should_close() {
                if let Some(reason) = self.session.flags().close_reason() {
                    let _ = self.send_error(&reason.error()).await;
                    let _ = super::sink::flush(&mut self.framed).await;
                }
                return Ok(());
            }
            self.shared.questions.fetch_add(1, Ordering::Relaxed);
            let cmd = match Command::parse(&payload, self.proto.caps, self.proto.persona) {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(session = %self.session.id, error = %e, head = ?&payload[..payload.len().min(32)], "malformed command");
                    self.send_err(ErrPacket::std(self.proto.persona, StdErr::MalformedPacket))
                        .await?;
                    super::sink::flush(&mut self.framed).await?;
                    continue;
                }
            };
            metrics::counter!("sprig_mysql_commands_total", "persona" => self.proto.persona.as_str(), "command" => command_name(&cmd)).increment(1);
            let flow = self.command(cmd).await?;
            super::sink::flush(&mut self.framed).await?;
            if flow == Flow::Close {
                return Ok(());
            }
        }
    }

    #[allow(clippy::too_many_lines)] // one arm per command
    async fn command(&mut self, cmd: Command) -> Result<Flow> {
        let persona = self.proto.persona;
        match cmd {
            Command::Quit => Ok(Flow::Close),
            Command::Ping => self.send_ok(OkPacket::new(self.status)).await,
            Command::Query(attrs, sql) => {
                self.session
                    .begin_command("Query", Some(info_of(&sql, 1024)));
                let r = if let Some(l) = local::classify(&sql) {
                    // Counted in the session's SHOW STATUS (gaps row 46).
                    self.session.local_questions += 1;
                    if matches!(l, Local::Variables(_)) {
                        self.session.local_selects += 1;
                    }
                    self.local(l).await
                } else {
                    self.run_answered(Request::Query { sql, attrs }).await
                };
                self.session.end_command();
                r
            }
            Command::InitDb(schema) => self.init_db(&String::from_utf8_lossy(&schema)).await,
            Command::FieldList(fl) => {
                self.run_answered(Request::FieldList {
                    table: fl.table,
                    wildcard: fl.wildcard,
                })
                .await
            }
            Command::CreateDb(name) => self.db_statement("CREATE", &name).await,
            Command::DropDb(name) => self.db_statement("DROP", &name).await,
            Command::Statistics => self.statistics().await,
            Command::ProcessInfo => self.process_list(false).await,
            Command::ProcessKill(id) => self.kill(ConnId(id), false).await,
            Command::ChangeUser(cu) => self.change_user(cu).await,
            Command::ResetConnection => {
                self.reset_session().await?;
                self.send_ok(OkPacket::new(self.status)).await
            }
            Command::SetOption(opt) => {
                match opt {
                    MYSQL_OPTION_MULTI_STATEMENTS_ON => self.proto.multi_statements = true,
                    MYSQL_OPTION_MULTI_STATEMENTS_OFF => self.proto.multi_statements = false,
                    _ => {
                        return self
                            .send_err(ErrPacket::std(persona, StdErr::UnknownCommand))
                            .await;
                    }
                }
                let eof = EofPacket {
                    warnings: 0,
                    status: self.status,
                };
                let t = sprig_proto_mysql::resultset::Terminator::Eof(eof);
                self.write(t.encode(self.proto.caps)).await?;
                Ok(Flow::Continue)
            }
            Command::StmtPrepare(sql) => self.prepare(sql).await,
            Command::StmtExecute(raw) => self.execute(raw).await,
            Command::StmtSendLongData(ld) => {
                let id = self.stmts.resolve(persona, ld.stmt_id);
                let Some(st) = self.stmts.map.get_mut(&id) else {
                    return Ok(Flow::Continue);
                };
                let i = usize::from(ld.param);
                if st.long_data.len() <= i {
                    st.long_data.resize(i + 1, false);
                }
                st.long_data[i] = true;
                let stmt = st.exec;
                self.run_answered(Request::LongData {
                    stmt,
                    param: ld.param,
                    data: ld.data,
                })
                .await
            }
            Command::StmtClose(id) => match self.stmts.map.remove(&id) {
                Some(st) => {
                    self.run_answered(Request::CloseStmt { stmt: st.exec })
                        .await
                }
                None => Ok(Flow::Continue),
            },
            Command::StmtReset(id) => {
                let id = self.stmts.resolve(persona, id);
                let Some(st) = self.stmts.map.get_mut(&id) else {
                    return self
                        .send_err(ErrPacket::unknown_stmt_handler(
                            persona,
                            id,
                            "mysqld_stmt_reset",
                        ))
                        .await;
                };
                st.long_data.clear();
                let stmt = st.exec;
                self.run_answered(Request::ResetStmt { stmt }).await
            }
            Command::StmtFetch(id, rows) => {
                let id = self.stmts.resolve(persona, id);
                let Some(st) = self.stmts.map.get(&id) else {
                    return self
                        .send_err(ErrPacket::unknown_stmt_handler(
                            persona,
                            id,
                            "mysqld_stmt_fetch",
                        ))
                        .await;
                };
                let stmt = st.exec;
                self.run_answered(Request::Fetch { stmt, rows }).await
            }
            Command::StmtBulkExecute(raw) => {
                let id = self.stmts.resolve(persona, raw.stmt_id);
                let Some(st) = self.stmts.map.get(&id) else {
                    return self
                        .send_err(ErrPacket::unknown_stmt_handler(
                            persona,
                            id,
                            "mysqld_stmt_bulk_execute",
                        ))
                        .await;
                };
                let stmt = st.exec;
                self.run_answered(Request::BulkExecute { stmt, raw }).await
            }
            Command::Privileged(code) => {
                let privilege = match code {
                    com::REFRESH => "RELOAD",
                    com::SHUTDOWN => "SHUTDOWN",
                    com::BINLOG_DUMP | com::BINLOG_DUMP_GTID | com::REGISTER_SLAVE => {
                        "REPLICATION SLAVE"
                    }
                    _ => "SUPER",
                };
                self.send_err(ErrPacket::specific_access_denied(persona, privilege))
                    .await
            }
            Command::Unknown(_) => {
                self.send_err(ErrPacket::std(persona, StdErr::UnknownCommand))
                    .await
            }
        }
    }

    // ----- writing -------------------------------------------------------

    async fn write(&mut self, payload: Bytes) -> Result<()> {
        self.framed.feed(payload).await.map_err(SprigError::from)
    }

    async fn send_ok(&mut self, ok: OkPacket) -> Result<Flow> {
        self.write(ok.encode(self.proto.caps)).await?;
        Ok(Flow::Continue)
    }

    async fn send_err(&mut self, e: ErrPacket) -> Result<Flow> {
        self.write(e.encode(self.proto.caps)).await?;
        Ok(Flow::Continue)
    }

    async fn send_error(&mut self, e: &SprigError) -> Result<Flow> {
        let mapped = sprig_sql::compat::errmap::for_persona(self.proto.persona, e);
        self.send_err(ErrPacket::from_error(
            self.proto.persona,
            mapped.as_ref().unwrap_or(e),
        ))
        .await
    }

    // ----- statements through the executor --------------------------------

    /// Runs a request whose response goes through the sink.
    async fn run_answered(&mut self, req: Request) -> Result<Flow> {
        match self.exec_request(req).await? {
            Ran::Gone => Ok(Flow::Close),
            Ran::Done(_) | Ran::Failed => Ok(Flow::Continue),
        }
    }

    /// Hands `req` to the executor, watching for `KILL QUERY` and the
    /// client hanging up (see the module docs).
    async fn exec_request(&mut self, req: Request) -> Result<Ran> {
        enum End {
            Done(Result<ExecSummary>),
            Killed,
            Gone,
        }
        let silent = req.is_silent();
        let is_prepare = matches!(req, Request::Prepare { .. });
        let size = req.size();
        let handle = Arc::clone(self.session.handle());
        let mut sink = WireSink::new(&mut self.framed, self.proto.persona, self.proto.caps);
        let mut cx = ExecCtx {
            session: &mut self.session,
            proto: &self.proto,
            db: self.db.as_deref(),
        };
        // A large request (gateway.large_statement_bytes) runs on a
        // blocking thread, with its KILL and hang-up watches (chapter 04,
        // "Large statements").
        let (exec, watch) = (&mut self.exec, &self.watch);
        let end = crate::offload::run_for(&handle, size, self.settings.large_statement, async {
            tokio::select! {
                biased;
                r = exec.execute(&mut cx, req, &mut sink) => End::Done(r),
                () = handle.flags().query_killed() => End::Killed,
                () = watch.gone() => End::Gone,
            }
        })
        .await;
        let ran = match end {
            End::Done(Ok(summary)) => {
                if let Some(db) = &summary.db {
                    // The executor tracks the session's database (tenant
                    // sessions: `USE` in SQL changes it).
                    self.db.clone_from(db);
                    self.session.set_db(db.clone());
                }
                if let Some(s) = summary.status.or(sink.last_status) {
                    self.status = s
                        & (StatusFlags::IN_TRANS
                            | StatusFlags::AUTOCOMMIT
                            | StatusFlags::IN_TRANS_READONLY);
                }
                if !silent && !is_prepare && !sink.finished() {
                    sink.error(&no_response()).await?;
                    Ran::Failed
                } else if is_prepare && summary.prepared.is_none() {
                    if !sink.finished() {
                        sink.error(&no_response()).await?;
                    }
                    Ran::Failed
                } else {
                    Ran::Done(summary)
                }
            }
            End::Done(Err(e)) => {
                if !silent {
                    sink.error(&e).await?;
                }
                Ran::Failed
            }
            End::Killed => {
                handle.flags().take_kill_query();
                self.exec.abandoned(&mut cx, Abandon::Killed).await;
                if !silent {
                    sink.error(&interrupted()).await?;
                }
                Ran::Failed
            }
            End::Gone => {
                self.exec.abandoned(&mut cx, Abandon::ClientGone).await;
                Ran::Gone
            }
        };
        let state = if self.status.contains(StatusFlags::IN_TRANS) {
            SessionState::InTransaction
        } else {
            SessionState::Ready
        };
        self.session.set_state(state);
        Ok(ran)
    }

    async fn prepare(&mut self, sql: Bytes) -> Result<Flow> {
        let persona = self.proto.persona;
        if self.stmts.map.len() >= self.settings.max_prepared {
            let limit = u32::try_from(self.settings.max_prepared).unwrap_or(u32::MAX);
            return self.send_err(ErrPacket::max_prepared(persona, limit)).await;
        }
        self.session
            .begin_command("Prepare", Some(info_of(&sql, 1024)));
        let ran = self.exec_request(Request::Prepare { sql }).await;
        self.session.end_command();
        let meta = match ran? {
            Ran::Done(ExecSummary {
                prepared: Some(meta),
                ..
            }) => meta,
            Ran::Gone => return Ok(Flow::Close),
            Ran::Done(_) | Ran::Failed => return Ok(Flow::Continue),
        };
        let id = self.stmts.insert(&meta);
        self.send_prepare_ok(id, &meta).await?;
        Ok(Flow::Continue)
    }

    async fn send_prepare_ok(&mut self, id: u32, meta: &PreparedMeta) -> Result<()> {
        let caps = self.proto.caps;
        let ok = PrepareOk {
            stmt_id: id,
            columns: u16::try_from(meta.columns.len()).unwrap_or(u16::MAX),
            params: u16::try_from(meta.params.len()).unwrap_or(u16::MAX),
            warnings: meta.warnings,
            metadata_follows: None,
        };
        self.write(ok.encode()).await?;
        for defs in [&meta.params, &meta.columns] {
            if defs.is_empty() {
                continue;
            }
            for c in defs {
                self.write(c.encode(caps)).await?;
            }
            if !caps.contains(Caps::DEPRECATE_EOF) {
                let eof = EofPacket {
                    warnings: 0,
                    status: self.status,
                };
                self.write(eof.encode(caps)).await?;
            }
        }
        Ok(())
    }

    async fn execute(&mut self, raw: StmtExecute) -> Result<Flow> {
        let persona = self.proto.persona;
        let id = self.stmts.resolve(persona, raw.stmt_id);
        let Some(st) = self.stmts.map.get_mut(&id) else {
            return self
                .send_err(ErrPacket::unknown_stmt_handler(
                    persona,
                    id,
                    "mysqld_stmt_execute",
                ))
                .await;
        };
        let info = ParamInfo {
            count: st.params,
            types: st.param_types.as_deref(),
            long_data: &st.long_data,
        };
        let params = match raw.decode_params(&info, self.proto.caps) {
            Ok(p) => p,
            Err(e) => {
                tracing::debug!(error = %e, "bad COM_STMT_EXECUTE parameters");
                return self
                    .send_err(ErrPacket::new(
                        1210,
                        *b"HY000",
                        "Incorrect arguments to mysqld_stmt_execute",
                    ))
                    .await;
            }
        };
        if params.new_params_bound {
            let mut types = params.types();
            types.truncate(usize::from(st.params));
            st.param_types = Some(types);
        }
        st.long_data.clear();
        let stmt = st.exec;
        self.session.begin_command("Execute", None);
        let r = self
            .run_answered(Request::Execute { stmt, raw, params })
            .await;
        self.session.end_command();
        r
    }

    /// `COM_CREATE_DB` and `COM_DROP_DB`, run as SQL (chapter 05).
    async fn db_statement(&mut self, verb: &str, name: &[u8]) -> Result<Flow> {
        let name = String::from_utf8_lossy(name).replace('`', "``");
        let sql = Bytes::from(format!("{verb} DATABASE `{name}`"));
        self.run_answered(Request::Query {
            sql,
            attrs: sprig_proto_mysql::command::QueryAttrs::default(),
        })
        .await
    }

    // ----- session commands -----------------------------------------------

    async fn init_db(&mut self, schema: &str) -> Result<Flow> {
        self.session
            .begin_command("Init DB", Some(schema.to_owned()));
        let r = {
            let mut cx = ExecCtx {
                session: &mut self.session,
                proto: &self.proto,
                db: self.db.as_deref(),
            };
            self.exec.init_db(&mut cx, schema).await
        };
        self.session.end_command();
        match r {
            Ok(()) => {
                self.db = Some(schema.to_owned());
                self.session.set_db(self.db.clone());
                let mut ok = OkPacket::new(self.status);
                ok.set_session_changes(&[SessionChange::Schema(Bytes::copy_from_slice(
                    schema.as_bytes(),
                ))]);
                self.send_ok(ok).await
            }
            Err(e) => self.send_error(&e).await,
        }
    }

    async fn reset_session(&mut self) -> Result<()> {
        let r = {
            let mut cx = ExecCtx {
                session: &mut self.session,
                proto: &self.proto,
                db: self.db.as_deref(),
            };
            self.exec.reset(&mut cx).await
        };
        self.stmts.clear();
        self.status = StatusFlags::AUTOCOMMIT;
        self.session.set_state(SessionState::Ready);
        r
    }

    /// `COM_CHANGE_USER`: the first response is checked against the
    /// connection's nonce when its plugin is the account's; otherwise an
    /// `AuthSwitchRequest` with a fresh nonce. That is what MySQL and
    /// MariaDB do, and libmariadb (mysqlclient, PHP's libmysqlclient
    /// builds) depends on it: it answers a switch to the plugin it already
    /// used by re-sending its first response, computed over the greeting's
    /// nonce. On success a new executor session for the new account (its
    /// cluster may differ). A failed change closes the connection.
    async fn change_user(&mut self, cu: ChangeUser) -> Result<Flow> {
        let persona = self.proto.persona;
        self.session.begin_command("Change user", None);
        self.session.set_state(SessionState::Authenticating);
        let mut login = self
            .shared
            .cfg
            .login
            .begin(super::login::request(
                persona,
                &cu.user,
                self.session.peer,
                self.session.tls.as_ref(),
                self.proto.caps,
            ))
            .await;
        let verdict = {
            let mut io = AuthIo {
                io: &mut self.framed,
                persona,
                tls: self.proto.is_tls(),
                nonce: self.proto.nonce,
            };
            let v = authenticate(
                &mut io,
                cu.plugin.as_deref(),
                cu.auth_response.clone(),
                false,
                &mut login,
            )
            .await?;
            self.proto.nonce = io.nonce;
            v
        };
        let principal = match verdict {
            AuthVerdict::Refuse(e) => {
                self.send_err(e).await?;
                return Ok(Flow::Close);
            }
            AuthVerdict::Accept => match login.principal() {
                Some(p) => p.clone(),
                None => return Err(SprigError::internal("login accepted without a principal")),
            },
        };
        // The old executor session rolls back and releases; the new login
        // gets its own.
        self.exec.close().await;
        self.stmts.clear();
        self.status = StatusFlags::AUTOCOMMIT;
        self.db = None;
        self.session.set_db(None);
        self.session.set_principal(principal.clone());
        // Another login may go to another cluster: its packet limit.
        if let Some(b) = self.ctx.backends.as_ref() {
            let limit = b
                .client_packet_limit(&principal.backend, self.settings.max_allowed_packet)
                .await;
            self.proto.max_packet = limit;
            self.framed.codec_mut().max_packet = limit;
        }
        self.exec = self.shared.cfg.executor.open(&LoginInfo {
            backend: principal.backend.clone(),
            principal,
            persona,
        });
        self.session.set_state(SessionState::Ready);
        self.session.end_command();
        if let Some(db) = cu
            .database
            .map(|d| String::from_utf8_lossy(&d).into_owned())
        {
            {
                let r = {
                    let mut cx = ExecCtx {
                        session: &mut self.session,
                        proto: &self.proto,
                        db: None,
                    };
                    self.exec.init_db(&mut cx, &db).await
                };
                if let Err(e) = r {
                    self.send_error(&e).await?;
                    return Ok(Flow::Close);
                }
                self.db = Some(db.clone());
                self.session.set_db(Some(db.clone()));
                let ok = super::auth_ok(self.proto.caps, Some(&db));
                self.write(ok).await?;
            }
        } else {
            let ok = super::auth_ok(self.proto.caps, None);
            self.write(ok).await?;
        }
        Ok(Flow::Continue)
    }

    // ----- answers from the gateway ---------------------------------------

    async fn local(&mut self, l: Local) -> Result<Flow> {
        let env = super::answer::LocalEnv {
            sessions: &self.ctx.sessions,
            persona: self.proto.persona,
            version: self.settings.version(self.proto.persona),
            // The session's limit: the gateway's, no more than its
            // cluster's (chapter 05, "Packet limit").
            max_allowed_packet: self.proto.max_packet,
            hostname: &self.shared.cfg.hostname,
            status: self.status,
            peers: self.ctx.peers.as_deref(),
        };
        let mut sink = WireSink::new(&mut self.framed, self.proto.persona, self.proto.caps);
        super::answer::answer(&l, &self.session, &env, &mut sink).await?;
        Ok(Flow::Continue)
    }

    /// `COM_PROCESS_KILL`.
    async fn kill(&mut self, id: ConnId, query_only: bool) -> Result<Flow> {
        self.local(Local::Kill { id, query_only }).await
    }

    /// `COM_PROCESS_INFO`.
    async fn process_list(&mut self, full: bool) -> Result<Flow> {
        self.local(Local::ProcessList { full }).await
    }

    /// `COM_STATISTICS`: a status string (not an OK packet).
    async fn statistics(&mut self) -> Result<Flow> {
        // The same uptime `SHOW STATUS` answers (gaps row 61).
        let uptime = crate::exec::status::uptime_secs();
        let questions = self.shared.questions.load(Ordering::Relaxed);
        #[allow(clippy::cast_precision_loss)] // a rate for display
        let qps = if uptime == 0 {
            0.0
        } else {
            questions as f64 / uptime as f64
        };
        let s = format!(
            "Uptime: {uptime}  Threads: {}  Questions: {questions}  Slow queries: 0  Opens: 0  Flush tables: 0  Open tables: 0  Queries per second avg: {qps:.3}",
            self.ctx.sessions.len()
        );
        self.write(Bytes::from(s)).await?;
        Ok(Flow::Continue)
    }
}

/// The statement text for `SHOW PROCESSLIST`'s `Info`, cut to `max` bytes.
fn info_of(sql: &[u8], max: usize) -> String {
    let s = String::from_utf8_lossy(&sql[..sql.len().min(max)]);
    s.into_owned()
}

fn command_name(cmd: &Command) -> &'static str {
    match cmd {
        Command::Quit => "quit",
        Command::InitDb(_) => "init_db",
        Command::Query(..) => "query",
        Command::FieldList(_) => "field_list",
        Command::CreateDb(_) => "create_db",
        Command::DropDb(_) => "drop_db",
        Command::Statistics => "statistics",
        Command::ProcessInfo => "process_info",
        Command::ProcessKill(_) => "process_kill",
        Command::Ping => "ping",
        Command::ChangeUser(_) => "change_user",
        Command::StmtPrepare(_) => "stmt_prepare",
        Command::StmtExecute(_) => "stmt_execute",
        Command::StmtSendLongData(_) => "stmt_send_long_data",
        Command::StmtClose(_) => "stmt_close",
        Command::StmtReset(_) => "stmt_reset",
        Command::SetOption(_) => "set_option",
        Command::StmtFetch(..) => "stmt_fetch",
        Command::ResetConnection => "reset_connection",
        Command::StmtBulkExecute(_) => "stmt_bulk_execute",
        Command::Privileged(_) => "privileged",
        Command::Unknown(_) => "unknown",
    }
}
