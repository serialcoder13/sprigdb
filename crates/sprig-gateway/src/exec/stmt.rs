//! Prepared statements on pooled backend connections (chapter 05,
//! "Prepared statements"; build step 8).
//!
//! The front end maps the client's statement ids to [`StmtRef`]s; this
//! module keeps what each [`StmtRef`] needs to run on whichever backend
//! connection the session gets next: the SQL text (the backend connection's
//! statement cache maps it to that connection's statement id, preparing it
//! lazily), the parameter count, the parameter types last bound, long data
//! buffered until the execute, and the open cursor.
//!
//! **Execute rewrite.** The client's `COM_STMT_EXECUTE` is re-encoded for
//! the backend statement: its id, no query attributes (3306; MariaDB has
//! none), long data folded in as the parameter's value, and the parameter
//! types whenever the client sent them or the backend statement has never
//! seen them (it was prepared afresh on this connection).
//!
//! **Long data** is buffered here, never forwarded on its own: the
//! backend statement is only known once the execute has picked its
//! connection, and a buffered value travels inside the execute packet.
//! More than `gateway.long_data_max_bytes` for one statement fails the
//! next execute with 39011.

use std::collections::HashMap;

use bytes::{Bytes, BytesMut};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::command::{
    BULK_SEND_TYPES_TO_SERVER, BoundParam, BulkExecute, Command, ExecuteParams, PARAMETER_COUNT_AVAILABLE, ParamInfo,
    ParamValue, StmtExecute,
};
use sprig_proto_mysql::resultset::column_type;
use sprig_proto_mysql::{Caps, ColumnDef, Persona, ProtoResult, Value};

use crate::backend::{BackendStmt, StmtInfo};
use crate::pipeline::StmtRef;

/// One prepared statement of a session.
#[derive(Debug)]
pub(crate) struct Stmt {
    /// The SQL text, as prepared on every backend connection.
    pub sql: Bytes,
    /// Routing and session-state facts of the text.
    pub info: StmtInfo,
    /// Parameters, from the backend's `PREPARE_OK`.
    pub params: u16,
    /// Column definitions, from the backend's `PREPARE_OK`.
    pub columns: Vec<ColumnDef>,
    /// Types last bound by an execute (for bulk executes without types).
    pub types: Option<Vec<u16>>,
    /// Long data since the last execute, by parameter.
    pub long_data: HashMap<u16, BytesMut>,
    /// Bytes in `long_data`.
    pub long_data_bytes: usize,
    /// Long data went past the limit: the next execute fails with 39011.
    pub long_data_over: bool,
    /// The backend statement id of the open cursor (on the pinned
    /// connection), if a cursor is open.
    pub cursor: Option<u32>,
}

impl Stmt {
    pub(crate) fn new(sql: Bytes, info: StmtInfo, prepared: &BackendStmt) -> Self {
        Self {
            sql,
            info,
            params: prepared.params,
            columns: prepared.columns.clone(),
            types: None,
            long_data: HashMap::new(),
            long_data_bytes: 0,
            long_data_over: false,
            cursor: None,
        }
    }

    /// Buffers a piece of long data (`COM_STMT_SEND_LONG_DATA`).
    pub(crate) fn add_long_data(&mut self, param: u16, data: &[u8], limit: usize) {
        if self.long_data_over {
            return;
        }
        if self.long_data_bytes.saturating_add(data.len()) > limit {
            self.long_data_over = true;
            self.long_data.clear();
            self.long_data_bytes = 0;
            return;
        }
        self.long_data_bytes += data.len();
        self.long_data.entry(param).or_default().extend_from_slice(data);
    }

    /// Drops buffered long data (after an execute, `COM_STMT_RESET`).
    pub(crate) fn clear_long_data(&mut self) {
        self.long_data.clear();
        self.long_data_bytes = 0;
        self.long_data_over = false;
    }

    /// The parameters to send: the statement's own (query attributes
    /// dropped), with long data folded in. Fails with 39011 past the limit.
    pub(crate) fn params_for_backend(&mut self, decoded: &ExecuteParams, persona: Persona) -> Result<Vec<BoundParam>> {
        if self.long_data_over {
            self.clear_long_data();
            return Err(SprigError::new(
                SprigCode::LongDataLimit,
                "long data for a prepared statement exceeds gateway.long_data_max_bytes",
            ));
        }
        let n = usize::from(self.params);
        let mut out: Vec<BoundParam> = decoded.params.iter().take(n).cloned().collect();
        for (i, p) in out.iter_mut().enumerate() {
            let idx = u16::try_from(i).unwrap_or(u16::MAX);
            if matches!(p.value, ParamValue::LongData) {
                let data = self.long_data.remove(&idx).map(BytesMut::freeze).unwrap_or_default();
                p.value = ParamValue::Value(Value::Bytes(data));
                if !is_lenenc_type(p.type_code()) {
                    // Long data for a numeric parameter: send it as the
                    // bytes the server would have read.
                    p.param_type = u16::from(column_type::LONG_BLOB);
                }
            }
            p.name = Bytes::new();
            let _ = persona;
        }
        self.clear_long_data();
        Ok(out)
    }
}

/// Types whose binary value is a length-encoded string.
fn is_lenenc_type(t: u8) -> bool {
    matches!(
        t,
        column_type::VARCHAR
            | column_type::VAR_STRING
            | column_type::STRING
            | column_type::TINY_BLOB
            | column_type::MEDIUM_BLOB
            | column_type::LONG_BLOB
            | column_type::BLOB
            | column_type::DECIMAL
            | column_type::NEWDECIMAL
            | column_type::JSON
            | column_type::ENUM
            | column_type::SET
            | column_type::BIT
            | column_type::GEOMETRY
    )
}

/// Builds the `COM_STMT_EXECUTE` payload for backend statement `b`.
/// `client_bound` is the client's `new_params_bind_flag`.
pub(crate) fn execute_payload(
    b: &BackendStmt,
    flags: u8,
    params: &[BoundParam],
    client_bound: bool,
) -> ProtoResult<Bytes> {
    let ex = StmtExecute {
        stmt_id: b.id,
        flags: flags & !PARAMETER_COUNT_AVAILABLE,
        iterations: 1,
        raw_params: Bytes::new(),
        params: Some(ExecuteParams {
            new_params_bound: client_bound || !b.types_sent,
            params: params.to_vec(),
        }),
    };
    // MariaDB has no query attributes: encode without them.
    Command::StmtExecute(ex).encode(Caps::empty())
}

/// Builds the `COM_STMT_BULK_EXECUTE` payload for backend statement `b`.
/// A bulk execute without types on a backend statement that has never
/// seen them gets the types last bound (decoded and re-encoded).
pub(crate) fn bulk_payload(b: &BackendStmt, raw: &BulkExecute, params: u16, types: Option<&[u16]>) -> ProtoResult<Bytes> {
    let mut bulk = BulkExecute {
        stmt_id: b.id,
        flags: raw.flags,
        raw: raw.raw.clone(),
        rows: None,
    };
    if raw.flags & BULK_SEND_TYPES_TO_SERVER == 0 && !b.types_sent {
        let info = ParamInfo {
            count: params,
            types,
            long_data: &[],
        };
        bulk.rows = Some(raw.decode_rows(&info)?);
        bulk.flags |= BULK_SEND_TYPES_TO_SERVER;
    }
    Command::StmtBulkExecute(bulk).encode(Caps::empty())
}

/// The session's statements, by [`StmtRef`].
#[derive(Debug, Default)]
pub(crate) struct Stmts {
    next: u64,
    map: HashMap<StmtRef, Stmt>,
}

impl Stmts {
    pub(crate) fn insert(&mut self, s: Stmt) -> StmtRef {
        self.next += 1;
        let r = StmtRef(self.next);
        self.map.insert(r, s);
        r
    }

    pub(crate) fn get_mut(&mut self, r: StmtRef) -> Result<&mut Stmt> {
        self.map.get_mut(&r).ok_or_else(|| unknown_stmt(r))
    }

    pub(crate) fn remove(&mut self, r: StmtRef) -> Option<Stmt> {
        self.map.remove(&r)
    }

    pub(crate) fn clear(&mut self) {
        self.map.clear();
    }

    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }
}

/// 1243: a statement the executor doesn't know (the front end maps ids,
/// so this means the two disagree).
fn unknown_stmt(r: StmtRef) -> SprigError {
    SprigError::standard(1243, *b"HY000", format!("Unknown prepared statement handler ({}) given to the executor", r.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::StmtInfo;

    fn backend(types_sent: bool) -> BackendStmt {
        BackendStmt {
            id: 77,
            params: 2,
            columns: Vec::new(),
            param_defs: Vec::new(),
            types_sent,
        }
    }

    fn param(t: u8, v: ParamValue) -> BoundParam {
        BoundParam {
            param_type: u16::from(t),
            name: Bytes::new(),
            value: v,
        }
    }

    #[test]
    fn long_data_folds_into_the_execute_and_respects_the_limit() {
        let mut s = Stmt::new(Bytes::from_static(b"INSERT INTO t VALUES (?, ?)"), StmtInfo::write(), &backend(false));
        s.add_long_data(1, b"hello ", 100);
        s.add_long_data(1, b"world", 100);
        let decoded = ExecuteParams {
            new_params_bound: true,
            params: vec![
                param(column_type::LONGLONG, ParamValue::Value(Value::Int(5))),
                param(column_type::BLOB, ParamValue::LongData),
            ],
        };
        let out = s.params_for_backend(&decoded, Persona::MariaDb).unwrap();
        assert_eq!(out[1].value, ParamValue::Value(Value::Bytes(Bytes::from_static(b"hello world"))));
        assert_eq!(s.long_data_bytes, 0);

        s.add_long_data(0, &[0u8; 60], 100);
        s.add_long_data(0, &[0u8; 60], 100);
        let e = s.params_for_backend(&decoded, Persona::MariaDb).unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::LongDataLimit));
        // The next execute starts clean.
        assert!(s.params_for_backend(&decoded, Persona::MariaDb).is_ok());
    }

    #[test]
    fn execute_payload_rebinds_types_for_a_fresh_backend_statement() {
        let params = vec![
            param(column_type::LONGLONG, ParamValue::Value(Value::Int(1))),
            param(column_type::VAR_STRING, ParamValue::Value(Value::Null)),
        ];
        let fresh = execute_payload(&backend(false), 0, &params, false).unwrap();
        let seen = execute_payload(&backend(true), 0, &params, false).unwrap();
        // code, id, flags, iterations, bitmap (1 byte), bind flag.
        assert_eq!(&fresh[1..5], &77u32.to_le_bytes());
        assert_eq!(fresh[11], 1, "types re-sent to a fresh statement");
        assert_eq!(seen[11], 0);
        assert_eq!(fresh.len(), seen.len() + 4);
    }
}
