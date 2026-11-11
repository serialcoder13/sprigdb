//! The document engine's SQL through the user's own executor session
//! (chapter 30, "Documents on the wire"; chapter 31, "Interfaces").
//!
//! A MongoDB login opens a gateway [`Session`] and the executor's session
//! for its principal, exactly as a MySQL login or an HTTP request does, so
//! name virtualization (chapter 07), chapter 29's privileges, the router
//! (chapter 17) and workload capture apply to every statement the engine
//! sends. The engine's statements run in *internal* mode
//! ([`SessionExec::set_internal`]) so its `_sprig_p_` field columns and
//! `__sprig_doc_meta` pass chapter 07's reserved-name check.

use std::net::SocketAddr;

use async_trait::async_trait;
use bytes::Bytes;
use sprig_common::{Result, SprigError};
use sprig_doc::engine::{Done, Param, Rows, SqlError, SqlRunner};
use sprig_proto_mysql::command::QueryAttrs;
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::state::ProtoState;
use sprig_proto_mysql::{Caps, ColumnDef, ErrPacket, Framing, Persona};

use crate::frontend::{Protocol, TlsInfo};
use crate::pipeline::{
    EndOfRows, ExecCtx, LoginInfo, Request, ResultSink, RowFormat, RowValues, SessionExec,
    SharedExecutor, Wire,
};
use crate::session::{Principal, Session, SessionState, SessionTable};

/// The persona of the engine's statements: MariaDB's, whose SQL the
/// engine writes. The MySQL 8.4 persona's compatibility layer (chapter 28:
/// canonical JSON text in results, MySQL's integer metadata and `1062`
/// texts, `ONLY_FULL_GROUP_BY` checks, text rewrites) is for MySQL
/// clients, and cost about a tenth of a point read's gateway CPU (R5
/// round 4).
const PERSONA: Persona = Persona::MariaDb;

/// The capabilities the executor sees: a client without TLS,
/// compression or `LOCAL INFILE`, with `DEPRECATE_EOF` and multi-results.
fn caps() -> Caps {
    let mut c = Caps::server_default(PERSONA);
    c.remove(Caps::SSL | Caps::COMPRESS | Caps::ZSTD_COMPRESSION_ALGORITHM | Caps::LOCAL_FILES);
    c
}

/// Collects one statement's result.
#[derive(Default)]
struct Collect {
    columns: usize,
    /// The columns, for binary rows (prepared statements, decision S26).
    defs: Vec<ColumnDef>,
    binary: bool,
    rows: Rows,
    ok: Option<Done>,
    error: Option<SprigError>,
    done: bool,
}

#[async_trait]
impl ResultSink for Collect {
    fn wire(&self) -> Wire {
        Wire {
            persona: PERSONA,
            caps: caps(),
        }
    }

    async fn begin_rows(&mut self, columns: &[ColumnDef], format: RowFormat) -> Result<()> {
        self.columns = columns.len();
        self.binary = format == RowFormat::Binary;
        if self.binary {
            self.defs = columns.to_vec();
        }
        Ok(())
    }

    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        let r = match values {
            RowValues::Text(v) => v.iter().map(|c| c.map(<[u8]>::to_vec)).collect(),
            RowValues::Binary(v) => v.iter().map(value_text).collect(),
        };
        self.rows.push(r);
        Ok(())
    }

    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        let r = if self.binary {
            sprig_proto_mysql::resultset::decode_binary_row(&payload, &self.defs)?
                .iter()
                .map(value_text)
                .collect()
        } else {
            sprig_proto_mysql::resultset::decode_text_row(&payload, self.columns)?
                .into_iter()
                .map(|c| c.map(<[u8]>::to_vec))
                .collect()
        };
        self.rows.push(r);
        Ok(())
    }

    async fn end_rows(&mut self, _end: EndOfRows) -> Result<()> {
        self.done = true;
        Ok(())
    }

    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        self.ok = Some(Done {
            affected: ok.affected_rows,
            info: String::from_utf8_lossy(&ok.info).into_owned(),
        });
        self.done = true;
        Ok(())
    }

    async fn error(&mut self, err: &SprigError) -> Result<()> {
        self.error = Some(SprigError {
            kind: err.kind,
            message: err.message.clone(),
            retryable: err.retryable,
            source: None,
        });
        self.done = true;
        Ok(())
    }

    async fn column_list(&mut self, _c: &[ColumnDef], _e: EndOfRows) -> Result<()> {
        Err(SprigError::internal(
            "COM_FIELD_LIST from the document engine",
        ))
    }

    async fn local_infile_request(&mut self, _f: &[u8]) -> Result<()> {
        Err(SprigError::standard(
            3948,
            *b"42000",
            "Loading local data is disabled",
        ))
    }

    async fn local_infile_read(&mut self) -> Result<Bytes> {
        Ok(Bytes::new())
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn finished(&self) -> bool {
        self.done
    }
}

/// A SQL error as the engine sees it: the engine persona's number and
/// text.
#[must_use]
pub fn sql_error(e: &SprigError) -> SqlError {
    let mapped = sprig_sql::compat::errmap::for_persona(PERSONA, e);
    let p = ErrPacket::from_error(PERSONA, mapped.as_ref().unwrap_or(e));
    SqlError {
        errno: Some(p.code),
        message: p.message,
        mongo: None,
    }
}

/// A binary protocol value as the text protocol would carry it.
fn value_text(v: &sprig_proto_mysql::Value) -> Option<Vec<u8>> {
    use std::fmt::Write as _;

    use sprig_proto_mysql::Value;
    Some(match v {
        Value::Null => return None,
        Value::Int(i) => i.to_string().into_bytes(),
        Value::UInt(u) => u.to_string().into_bytes(),
        Value::Float(f) => f.to_string().into_bytes(),
        Value::Double(f) => f.to_string().into_bytes(),
        Value::Bytes(b) => b.to_vec(),
        Value::DateTime(d) => {
            let mut t = format!("{:04}-{:02}-{:02}", d.year, d.month, d.day);
            if d.hour > 0 || d.minute > 0 || d.second > 0 || d.micro > 0 {
                let _ = write!(t, " {:02}:{:02}:{:02}", d.hour, d.minute, d.second);
            }
            if d.micro > 0 {
                let _ = write!(t, ".{:06}", d.micro);
            }
            t.into_bytes()
        }
        Value::Time(t) => format!(
            "{}{:02}:{:02}:{:02}",
            if t.negative { "-" } else { "" },
            u64::from(t.days) * 24 + u64::from(t.hour),
            t.minute,
            t.second
        )
        .into_bytes(),
    })
}

/// A parameter as `COM_STMT_EXECUTE` carries it: keys as binary strings
/// (`BLOB`, so MariaDB compares them byte for byte), text as `VAR_STRING`.
fn bound(p: &Param) -> sprig_proto_mysql::command::BoundParam {
    use sprig_proto_mysql::Value;
    use sprig_proto_mysql::command::{BoundParam, ParamValue};
    use sprig_proto_mysql::resultset::column_type as ct;
    let (t, v) = match p {
        Param::Null => (ct::NULL, Value::Null),
        Param::Int(i) => (ct::LONGLONG, Value::Int(*i)),
        Param::Bin(b) => (ct::BLOB, Value::Bytes(Bytes::copy_from_slice(b))),
        Param::Text(t) => (
            ct::VAR_STRING,
            Value::Bytes(Bytes::copy_from_slice(t.as_bytes())),
        ),
    };
    BoundParam {
        param_type: u16::from(t),
        name: Bytes::new(),
        value: ParamValue::Value(v),
    }
}

/// Statements a session keeps prepared (decision S26); past this many the
/// oldest are closed.
const MAX_PREPARED: usize = 64;

/// One executor session of a MongoDB login.
pub struct SqlSession {
    session: Session,
    exec: Box<dyn SessionExec>,
    proto: ProtoState,
    /// The principal's tenant (0 for none).
    tenant: u64,
    /// A transaction is open (`startTransaction`).
    pub in_txn: bool,
    /// The session runs at READ COMMITTED ([`Self::read_committed`]).
    read_committed: bool,
    /// The principal's tenant name and user id (chapter 39's definers).
    hint: Option<(String, u32)>,
    /// The engine's statements prepared in this session, by text, in
    /// order of preparing (decision S26).
    prepared: Vec<(String, crate::pipeline::StmtRef)>,
}

impl std::fmt::Debug for SqlSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlSession")
            .field("in_txn", &self.in_txn)
            .finish_non_exhaustive()
    }
}

impl SqlSession {
    /// Chapter 29 (RL34): the tables whose policies bind this session, as
    /// MongoDB filters; `None` when it can't tell.
    #[must_use]
    pub fn row_policy_docs(&self) -> Option<std::sync::Arc<crate::tenant::rls::DocViews>> {
        self.exec.row_policy_docs()
    }

    /// The gateway session's id (chapter 29: internal exemptions).
    #[must_use]
    pub fn session_id(&self) -> sprig_common::ids::ConnId {
        self.session.id
    }

    /// Opens a session for `principal`.
    ///
    /// # Errors
    /// The session table's (connection limits).
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        sessions: &std::sync::Arc<SessionTable>,
        executor: &SharedExecutor,
        principal: &Principal,
        listener: &'static str,
        generation: u64,
        peer: SocketAddr,
        tls: Option<TlsInfo>,
        max_packet: usize,
    ) -> Result<Self> {
        let mut session =
            sessions.open_parts(Protocol::Mongo, listener, generation, peer, tls.clone())?;
        session.set_principal(principal.clone());
        session.set_state(SessionState::Ready);
        let proto = ProtoState {
            persona: PERSONA,
            caps: caps(),
            collation: u16::from(PERSONA.greeting_collation()),
            multi_statements: false,
            framing: Framing::Plain,
            max_packet,
            tls: tls.map(|t| sprig_proto_mysql::state::TlsInfo {
                version: t.version,
                cipher: t.cipher,
                sni: t.sni,
            }),
            nonce: [0; 20],
        };
        let exec = executor.open(&LoginInfo {
            principal: principal.clone(),
            backend: principal.backend.clone(),
            persona: PERSONA,
        });
        Ok(Self {
            session,
            exec,
            proto,
            tenant: principal.tenant.map_or(0, |t| t.0),
            in_txn: false,
            read_committed: false,
            hint: principal
                .tenant_name
                .clone()
                .zip(principal.user_id.map(|u| u.0)),
            prepared: Vec::new(),
        })
    }

    /// Runs one request in internal mode, collecting its answer.
    async fn request(
        &mut self,
        req: Request,
        what: &'static str,
    ) -> std::result::Result<(Collect, crate::pipeline::ExecSummary), SqlError> {
        let mut sink = Collect::default();
        self.session.begin_command(what, None);
        self.exec.set_internal(true);
        let r = {
            let mut cx = ExecCtx {
                session: &mut self.session,
                proto: &self.proto,
                db: None,
            };
            self.exec.execute(&mut cx, req, &mut sink).await
        };
        self.exec.set_internal(false);
        self.session.end_command();
        let summary = r.map_err(|e| sql_error(&e))?;
        if let Some(e) = sink.error.take() {
            return Err(sql_error(&e));
        }
        Ok((sink, summary))
    }

    /// The statement `sql` prepared in this session (prepared now when it
    /// isn't yet).
    async fn statement(
        &mut self,
        sql: &str,
    ) -> std::result::Result<crate::pipeline::StmtRef, SqlError> {
        if let Some((_, s)) = self.prepared.iter().find(|(t, _)| t == sql) {
            return Ok(*s);
        }
        if self.prepared.len() >= MAX_PREPARED {
            let (_, old) = self.prepared.remove(0);
            let _ = self
                .request(Request::CloseStmt { stmt: old }, "Close stmt")
                .await;
        }
        let (_, summary) = self
            .request(
                Request::Prepare {
                    sql: Bytes::copy_from_slice(sql.as_bytes()),
                },
                "Prepare",
            )
            .await?;
        let stmt = summary
            .prepared
            .map(|m| m.stmt)
            .ok_or_else(|| sql_error(&SprigError::internal("a prepare without a statement")))?;
        self.prepared.push((sql.to_owned(), stmt));
        Ok(stmt)
    }

    /// Runs `sql` prepared with `params` bound; an execute the statement no
    /// longer fits (1243 unknown handler, 1615 re-prepare, a table changed
    /// under it) is prepared once more and run again.
    async fn run_bound(
        &mut self,
        sql: &str,
        params: &[Param],
    ) -> std::result::Result<Collect, SqlError> {
        let mut again = false;
        loop {
            let stmt = self.statement(sql).await?;
            let req = Request::Execute {
                stmt,
                raw: sprig_proto_mysql::command::StmtExecute {
                    stmt_id: 0,
                    flags: 0,
                    iterations: 1,
                    raw_params: Bytes::new(),
                    params: None,
                },
                params: sprig_proto_mysql::command::ExecuteParams {
                    new_params_bound: true,
                    params: params.iter().map(bound).collect(),
                },
            };
            match self.request(req, "Execute").await {
                Ok((sink, _)) => return Ok(sink),
                Err(e) if !again && matches!(e.errno, Some(1243 | 1615 | 1146 | 1054)) => {
                    again = true;
                    self.prepared.retain(|(t, _)| t != sql);
                    let _ = self
                        .request(Request::CloseStmt { stmt }, "Close stmt")
                        .await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn run(&mut self, sql: &str) -> std::result::Result<Collect, SqlError> {
        let mut sink = Collect::default();
        self.session
            .begin_command("Query", Some(sql.chars().take(1024).collect()));
        self.exec.set_internal(true);
        let r = {
            let mut cx = ExecCtx {
                session: &mut self.session,
                proto: &self.proto,
                db: None,
            };
            let req = Request::Query {
                sql: Bytes::copy_from_slice(sql.as_bytes()),
                attrs: QueryAttrs::default(),
            };
            self.exec.execute(&mut cx, req, &mut sink).await
        };
        self.exec.set_internal(false);
        self.session.end_command();
        if let Err(e) = r {
            return Err(sql_error(&e));
        }
        if let Some(e) = sink.error.take() {
            return Err(sql_error(&e));
        }
        Ok(sink)
    }

    /// The session's shared half: its kill flags, for the stall guard
    /// ([`crate::offload::run_for`]).
    #[must_use]
    pub fn handle(&self) -> std::sync::Arc<crate::session::SessionHandle> {
        std::sync::Arc::clone(self.session.handle())
    }

    /// Sets the session's isolation to READ COMMITTED, once (retryable
    /// writes); a failure leaves it as it was and is logged.
    pub async fn read_committed(&mut self) {
        if self.read_committed {
            return;
        }
        self.read_committed = true;
        if let Err(e) = self
            .run("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .await
        {
            tracing::debug!(error = %e.message, "READ COMMITTED for retryable writes not set");
        }
    }

    /// Closes the session (rolls back).
    pub async fn close(mut self) {
        self.session.set_state(SessionState::Closing);
        self.exec.close().await;
    }
}

#[async_trait]
impl SqlRunner for SqlSession {
    async fn query(&mut self, sql: &str) -> std::result::Result<Rows, SqlError> {
        Ok(self.run(sql).await?.rows)
    }

    async fn exec(&mut self, sql: &str) -> std::result::Result<Done, SqlError> {
        Ok(self.run(sql).await?.ok.unwrap_or_default())
    }

    fn tenant(&self) -> u64 {
        self.tenant
    }

    fn principal_hint(&self) -> Option<(String, u32)> {
        self.hint.clone()
    }

    fn row_policy_binds(&self, object: u64) -> Option<bool> {
        self.exec.row_policy_binds(object)
    }

    async fn query_bound(
        &mut self,
        sql: &str,
        params: &[Param],
    ) -> std::result::Result<Rows, SqlError> {
        Ok(self.run_bound(sql, params).await?.rows)
    }

    async fn exec_bound(
        &mut self,
        sql: &str,
        params: &[Param],
    ) -> std::result::Result<Done, SqlError> {
        Ok(self.run_bound(sql, params).await?.ok.unwrap_or_default())
    }
}
