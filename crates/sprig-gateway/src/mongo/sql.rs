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
use sprig_doc::engine::{Done, Rows, SqlError, SqlRunner};
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

/// The capabilities the executor sees: a MySQL 8.4 client without TLS,
/// compression or `LOCAL INFILE`, with `DEPRECATE_EOF` and multi-results.
fn caps() -> Caps {
    let mut c = Caps::server_default(Persona::MySql84);
    c.remove(Caps::SSL | Caps::COMPRESS | Caps::ZSTD_COMPRESSION_ALGORITHM | Caps::LOCAL_FILES);
    c
}

/// Collects one statement's result.
#[derive(Default)]
struct Collect {
    columns: usize,
    rows: Rows,
    ok: Option<Done>,
    error: Option<SprigError>,
    done: bool,
}

#[async_trait]
impl ResultSink for Collect {
    fn wire(&self) -> Wire {
        Wire {
            persona: Persona::MySql84,
            caps: caps(),
        }
    }

    async fn begin_rows(&mut self, columns: &[ColumnDef], _format: RowFormat) -> Result<()> {
        self.columns = columns.len();
        Ok(())
    }

    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        let r = match values {
            RowValues::Text(v) => v.iter().map(|c| c.map(<[u8]>::to_vec)).collect(),
            RowValues::Binary(_) => {
                return Err(SprigError::internal("a binary row for the document engine"));
            }
        };
        self.rows.push(r);
        Ok(())
    }

    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        let r = sprig_proto_mysql::resultset::decode_text_row(&payload, self.columns)?
            .into_iter()
            .map(|c| c.map(<[u8]>::to_vec))
            .collect();
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

/// A SQL error as the engine sees it: the MySQL persona's number and text
/// (MariaDB-only errors mapped as the wire maps them).
#[must_use]
pub fn sql_error(e: &SprigError) -> SqlError {
    let mapped = sprig_sql::compat::errmap::for_persona(Persona::MySql84, e);
    let p = ErrPacket::from_error(Persona::MySql84, mapped.as_ref().unwrap_or(e));
    SqlError {
        errno: Some(p.code),
        message: p.message,
        mongo: None,
    }
}

/// One executor session of a MongoDB login.
pub struct SqlSession {
    session: Session,
    exec: Box<dyn SessionExec>,
    proto: ProtoState,
    /// A transaction is open (`startTransaction`).
    pub in_txn: bool,
}

impl std::fmt::Debug for SqlSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlSession")
            .field("in_txn", &self.in_txn)
            .finish_non_exhaustive()
    }
}

impl SqlSession {
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
            persona: Persona::MySql84,
            caps: caps(),
            collation: u16::from(Persona::MySql84.greeting_collation()),
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
            persona: Persona::MySql84,
        });
        Ok(Self {
            session,
            exec,
            proto,
            in_txn: false,
        })
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
}
