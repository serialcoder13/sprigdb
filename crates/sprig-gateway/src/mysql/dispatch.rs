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

use bytes::Bytes;
use futures::{SinkExt, StreamExt as _};
use sprig_auth::Account;
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
use sprig_proto_mysql::{Caps, ColumnDef, Command, ErrPacket, PacketCodec, Persona, StatusFlags};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use super::local::{self, Local, PersonaVar};
use super::login::AccountLogin;
use super::settings::FrontendSettings;
use super::sink::WireSink;
use super::watch::ClientWatch;
use super::{LoggedIn, Shared};
use crate::frontend::{GatewayContext, MaybeTls};
use crate::pipeline::{
    Abandon, EndOfRows, ExecCtx, ExecSummary, LoginInfo, PreparedMeta, Request, ResultSink,
    RowFormat, RowValues, SessionExec, StmtRef, int_column, text_column,
};
use crate::session::{Session, SessionState};

type ClientFramed = Framed<MaybeTls<TcpStream>, PacketCodec>;

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
    account: Arc<Account>,
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
        session: Session,
        l: LoggedIn,
        settings: FrontendSettings,
    ) -> Self {
        let watch = ClientWatch::new(&l.stream);
        let mut codec = PacketCodec::new(l.proto.max_packet);
        // Compression starts after the auth OK packet (chapter 05).
        codec.set_framing(l.proto.framing);
        let mut framed = Framed::with_capacity(l.stream, codec, WRITE_CAPACITY);
        framed.set_backpressure_boundary(settings.relay_buffer.max(WRITE_CAPACITY));
        let _ = l.attrs;
        Self {
            shared,
            ctx,
            session,
            framed,
            proto: l.proto,
            exec: l.exec,
            account: l.account,
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
            let payload = match self.framed.next().await {
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
        self.send_err(ErrPacket::from_error(self.proto.persona, e))
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
        let handle = Arc::clone(self.session.handle());
        let mut sink = WireSink::new(&mut self.framed, self.proto.persona, self.proto.caps);
        let mut cx = ExecCtx {
            session: &mut self.session,
            proto: &self.proto,
            db: self.db.as_deref(),
        };
        let end = tokio::select! {
            biased;
            r = self.exec.execute(&mut cx, req, &mut sink) => End::Done(r),
            () = handle.flags().query_killed() => End::Killed,
            () = self.watch.gone() => End::Gone,
        };
        let ran = match end {
            End::Done(Ok(summary)) => {
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
        let mut login = AccountLogin::new(
            Arc::clone(&self.shared.cfg.login),
            persona,
            &cu.user,
            self.session.peer,
        );
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
        let (account, principal) = match verdict {
            AuthVerdict::Refuse(e) => {
                self.send_err(e).await?;
                return Ok(Flow::Close);
            }
            AuthVerdict::Accept => match (login.account().cloned(), login.principal()) {
                (Some(a), Some(p)) => (a, p),
                _ => return Err(SprigError::internal("login accepted without an account")),
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
        self.exec = self.shared.cfg.executor.open(&LoginInfo {
            principal,
            backend: account.backend.clone(),
            persona,
        });
        self.account = account;
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
        match l {
            Local::Variables(items) => self.variables(&items).await,
            Local::Kill { id, query_only } => self.kill(id, query_only).await,
            Local::ProcessList { full } => self.process_list(full).await,
        }
    }

    async fn variables(&mut self, items: &[(String, PersonaVar)]) -> Result<Flow> {
        let persona = self.proto.persona;
        let charset = u16::from(persona.greeting_collation());
        let mut cols = Vec::with_capacity(items.len());
        let mut vals = Vec::with_capacity(items.len());
        for (name, var) in items {
            cols.push(if var.is_int() {
                int_column(name, true)
            } else {
                text_column(name, charset)
            });
            vals.push(match var {
                PersonaVar::Version => self.settings.version(persona).to_owned(),
                PersonaVar::VersionComment => {
                    format!("SprigDB {}", env!("CARGO_PKG_VERSION"))
                }
                PersonaVar::ProtocolVersion => "10".to_owned(),
                PersonaVar::MaxAllowedPacket => self.settings.max_allowed_packet.to_string(),
                PersonaVar::Hostname => self.shared.cfg.hostname.clone(),
            });
        }
        let row: Vec<Option<&[u8]>> = vals.iter().map(|v| Some(v.as_bytes())).collect();
        self.synthesized(&cols, &[row]).await
    }

    /// Sends a synthesized text resultset.
    async fn synthesized(
        &mut self,
        cols: &[ColumnDef],
        rows: &[Vec<Option<&[u8]>>],
    ) -> Result<Flow> {
        let status = self.status;
        let mut sink = WireSink::new(&mut self.framed, self.proto.persona, self.proto.caps);
        sink.begin_rows(cols, RowFormat::Text).await?;
        for r in rows {
            sink.row(RowValues::Text(r)).await?;
        }
        sink.end_rows(EndOfRows {
            status,
            warnings: 0,
        })
        .await?;
        Ok(Flow::Continue)
    }

    /// `KILL [CONNECTION | QUERY] id` and `COM_PROCESS_KILL`. Until chapter
    /// 29's privileges, a session may kill only sessions of its own user
    /// (MySQL's rule without `CONNECTION_ADMIN`).
    async fn kill(&mut self, id: ConnId, query_only: bool) -> Result<Flow> {
        let me = self.session.principal.as_ref().map(|p| p.user.clone());
        let Some(target) = self.ctx.sessions.get(id) else {
            return self
                .send_err(ErrPacket::new(
                    1094,
                    *b"HY000",
                    format!("Unknown thread id: {id}"),
                ))
                .await;
        };
        let owner = target.process_row(self.ctx.sessions.clock().now()).user;
        if owner.is_none() || owner != me {
            return self
                .send_err(ErrPacket::new(
                    1095,
                    *b"HY000",
                    format!("You are not owner of thread {id}"),
                ))
                .await;
        }
        if query_only {
            target.flags().kill_query();
        } else {
            target.flags().kill_conn();
        }
        self.send_ok(OkPacket::new(self.status)).await
    }

    /// `SHOW [FULL] PROCESSLIST` and `COM_PROCESS_INFO`: this gateway's
    /// sessions of the same user (chapter 29's `PROCESS` privilege later).
    async fn process_list(&mut self, full: bool) -> Result<Flow> {
        let charset = u16::from(self.proto.persona.greeting_collation());
        let me = self.session.principal.as_ref().map(|p| p.user.clone());
        let cols = vec![
            int_column("Id", true),
            text_column("User", charset),
            text_column("Host", charset),
            text_column("db", charset),
            text_column("Command", charset),
            int_column("Time", false),
            text_column("State", charset),
            text_column("Info", charset),
        ];
        let rows: Vec<[Option<String>; 8]> = self
            .ctx
            .sessions
            .list()
            .into_iter()
            .filter(|r| r.user.is_some() && r.user == me)
            .map(|r| {
                let info = r.info.map(|i| {
                    if full {
                        i
                    } else {
                        i.chars().take(100).collect()
                    }
                });
                [
                    Some(r.id.0.to_string()),
                    r.user,
                    Some(r.host.to_string()),
                    r.db,
                    Some(r.command.to_owned()),
                    Some(r.time.as_secs().to_string()),
                    Some(r.state.as_str().to_owned()),
                    info,
                ]
            })
            .collect();
        let refs: Vec<Vec<Option<&[u8]>>> = rows
            .iter()
            .map(|r| r.iter().map(|v| v.as_deref().map(str::as_bytes)).collect())
            .collect();
        self.synthesized(&cols, &refs).await
    }

    /// `COM_STATISTICS`: a status string (not an OK packet).
    async fn statistics(&mut self) -> Result<Flow> {
        let uptime = self.shared.started.elapsed().as_secs();
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
