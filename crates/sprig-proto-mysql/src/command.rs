//! Commands: [`Command::parse`] decodes a command-phase payload without
//! panicking on any input, and [`Command::encode`] builds one for the
//! backend client.
//!
//! `COM_STMT_EXECUTE` and `COM_STMT_BULK_EXECUTE` parameters can only be
//! decoded with the statement's parameter count, the types bound by an
//! earlier execute and which parameters got long data. [`Command::parse`]
//! keeps them raw; [`Command::parse_with`] decodes them through a
//! [`StmtLookup`], as do [`StmtExecute::decode_params`] and
//! [`BulkExecute::decode_rows`] later.

use bytes::Bytes;

use crate::buf::{Reader, Writer};
use crate::packets::handshake::{decode_attrs, encode_attrs};
use crate::packets::{Caps, Persona};
use crate::proto_error::{ProtoError, ProtoResult};
use crate::resultset::{
    PARAM_UNSIGNED, Value, decode_binary_value, encode_binary_value, null_bitmap_get,
    null_bitmap_len, null_bitmap_set,
};

/// Command codes.
pub mod com {
    /// `COM_SLEEP` (internal).
    pub const SLEEP: u8 = 0x00;
    /// `COM_QUIT`.
    pub const QUIT: u8 = 0x01;
    /// `COM_INIT_DB`.
    pub const INIT_DB: u8 = 0x02;
    /// `COM_QUERY`.
    pub const QUERY: u8 = 0x03;
    /// `COM_FIELD_LIST`.
    pub const FIELD_LIST: u8 = 0x04;
    /// `COM_CREATE_DB`.
    pub const CREATE_DB: u8 = 0x05;
    /// `COM_DROP_DB`.
    pub const DROP_DB: u8 = 0x06;
    /// `COM_REFRESH`.
    pub const REFRESH: u8 = 0x07;
    /// `COM_SHUTDOWN`.
    pub const SHUTDOWN: u8 = 0x08;
    /// `COM_STATISTICS`.
    pub const STATISTICS: u8 = 0x09;
    /// `COM_PROCESS_INFO`.
    pub const PROCESS_INFO: u8 = 0x0A;
    /// `COM_CONNECT` (internal).
    pub const CONNECT: u8 = 0x0B;
    /// `COM_PROCESS_KILL`.
    pub const PROCESS_KILL: u8 = 0x0C;
    /// `COM_DEBUG`.
    pub const DEBUG: u8 = 0x0D;
    /// `COM_PING`.
    pub const PING: u8 = 0x0E;
    /// `COM_CHANGE_USER`.
    pub const CHANGE_USER: u8 = 0x11;
    /// `COM_BINLOG_DUMP`.
    pub const BINLOG_DUMP: u8 = 0x12;
    /// `COM_REGISTER_SLAVE`.
    pub const REGISTER_SLAVE: u8 = 0x15;
    /// `COM_STMT_PREPARE`.
    pub const STMT_PREPARE: u8 = 0x16;
    /// `COM_STMT_EXECUTE`.
    pub const STMT_EXECUTE: u8 = 0x17;
    /// `COM_STMT_SEND_LONG_DATA`.
    pub const STMT_SEND_LONG_DATA: u8 = 0x18;
    /// `COM_STMT_CLOSE`.
    pub const STMT_CLOSE: u8 = 0x19;
    /// `COM_STMT_RESET`.
    pub const STMT_RESET: u8 = 0x1A;
    /// `COM_SET_OPTION`.
    pub const SET_OPTION: u8 = 0x1B;
    /// `COM_STMT_FETCH`.
    pub const STMT_FETCH: u8 = 0x1C;
    /// `COM_BINLOG_DUMP_GTID`.
    pub const BINLOG_DUMP_GTID: u8 = 0x1E;
    /// `COM_RESET_CONNECTION`.
    pub const RESET_CONNECTION: u8 = 0x1F;
    /// MariaDB `COM_STMT_BULK_EXECUTE`.
    pub const STMT_BULK_EXECUTE: u8 = 0xFA;
}

/// `COM_SET_OPTION`: enable multi-statements.
pub const MYSQL_OPTION_MULTI_STATEMENTS_ON: u16 = 0;
/// `COM_SET_OPTION`: disable multi-statements.
pub const MYSQL_OPTION_MULTI_STATEMENTS_OFF: u16 = 1;

/// `COM_STMT_EXECUTE` flags: open a read-only cursor.
pub const CURSOR_TYPE_READ_ONLY: u8 = 0x01;
/// `COM_STMT_EXECUTE` flags: cursor for update.
pub const CURSOR_TYPE_FOR_UPDATE: u8 = 0x02;
/// `COM_STMT_EXECUTE` flags: scrollable cursor.
pub const CURSOR_TYPE_SCROLLABLE: u8 = 0x04;
/// `COM_STMT_EXECUTE` flags: a parameter count follows (query attributes).
pub const PARAMETER_COUNT_AVAILABLE: u8 = 0x08;

/// `COM_STMT_BULK_EXECUTE` flags: one result per row.
pub const BULK_SEND_UNIT_RESULTS: u16 = 64;
/// `COM_STMT_BULK_EXECUTE` flags: parameter types follow.
pub const BULK_SEND_TYPES_TO_SERVER: u16 = 128;

/// A command-phase request.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    /// `COM_QUIT`.
    Quit,
    /// `COM_INIT_DB` with the schema name.
    InitDb(Bytes),
    /// `COM_QUERY`: query attributes (empty without `QUERY_ATTRIBUTES`) and
    /// the SQL text.
    Query(QueryAttrs, Bytes),
    /// `COM_FIELD_LIST`.
    FieldList(FieldList),
    /// `COM_CREATE_DB` with the schema name.
    CreateDb(Bytes),
    /// `COM_DROP_DB` with the schema name.
    DropDb(Bytes),
    /// `COM_STATISTICS`.
    Statistics,
    /// `COM_PROCESS_INFO`.
    ProcessInfo,
    /// `COM_PROCESS_KILL` with the connection id.
    ProcessKill(u32),
    /// `COM_PING`.
    Ping,
    /// `COM_CHANGE_USER`.
    ChangeUser(ChangeUser),
    /// `COM_STMT_PREPARE` with the SQL text.
    StmtPrepare(Bytes),
    /// `COM_STMT_EXECUTE`.
    StmtExecute(StmtExecute),
    /// `COM_STMT_SEND_LONG_DATA`.
    StmtSendLongData(LongData),
    /// `COM_STMT_CLOSE` with the statement id.
    StmtClose(u32),
    /// `COM_STMT_RESET` with the statement id.
    StmtReset(u32),
    /// `COM_SET_OPTION` with the option.
    SetOption(u16),
    /// `COM_STMT_FETCH`: statement id and row count.
    StmtFetch(u32, u32),
    /// `COM_RESET_CONNECTION`.
    ResetConnection,
    /// MariaDB `COM_STMT_BULK_EXECUTE` (MariaDB persona only).
    StmtBulkExecute(BulkExecute),
    /// A command that needs a server-wide privilege (refresh, shutdown,
    /// debug, binlog dump, register replica): ERR 1227.
    Privileged(u8),
    /// Any other code: ERR 1047.
    Unknown(u8),
}

/// `COM_FIELD_LIST` arguments.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FieldList {
    /// Table name.
    pub table: Bytes,
    /// Column wildcard (`LIKE` pattern), possibly empty.
    pub wildcard: Bytes,
}

/// One query attribute or bound parameter.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundParam {
    /// The 2-byte type: type code plus [`PARAM_UNSIGNED`].
    pub param_type: u16,
    /// Attribute name; empty for ordinary parameters.
    pub name: Bytes,
    /// The value.
    pub value: ParamValue,
}

impl BoundParam {
    /// The type code (low byte of `param_type`).
    #[must_use]
    pub fn type_code(&self) -> u8 {
        self.param_type.to_le_bytes()[0]
    }

    /// Whether the unsigned flag is set.
    #[must_use]
    pub fn is_unsigned(&self) -> bool {
        self.param_type & PARAM_UNSIGNED != 0
    }
}

/// A parameter value in `COM_STMT_EXECUTE`.
#[derive(Debug, Clone, PartialEq)]
pub enum ParamValue {
    /// A value sent in the packet (`Value::Null` for NULL).
    Value(Value),
    /// The value was sent earlier with `COM_STMT_SEND_LONG_DATA`.
    LongData,
}

/// Query attributes of a `COM_QUERY` (`CLIENT_QUERY_ATTRIBUTES`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct QueryAttrs {
    /// Attributes in order. Values are never [`ParamValue::LongData`].
    pub attrs: Vec<BoundParam>,
}

impl QueryAttrs {
    /// The value of the attribute with this name.
    #[must_use]
    pub fn get(&self, name: &[u8]) -> Option<&Value> {
        self.attrs.iter().find_map(|a| match &a.value {
            ParamValue::Value(v) if a.name == name => Some(v),
            _ => None,
        })
    }

    /// Whether there are no attributes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.attrs.is_empty()
    }
}

/// `COM_CHANGE_USER` arguments.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ChangeUser {
    /// User name.
    pub user: Bytes,
    /// First auth response.
    pub auth_response: Bytes,
    /// Default database; an empty name reads as `None`.
    pub database: Option<Bytes>,
    /// Character set. When `None`, nothing after the database is encoded.
    pub charset: Option<u16>,
    /// Client auth plugin (`PLUGIN_AUTH`).
    pub plugin: Option<String>,
    /// Connection attributes (`CONNECT_ATTRS`).
    pub attrs: Vec<(String, String)>,
}

/// `COM_STMT_SEND_LONG_DATA` arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LongData {
    /// Statement id.
    pub stmt_id: u32,
    /// Parameter index.
    pub param: u16,
    /// A piece of the value.
    pub data: Bytes,
}

/// What decoding a statement's parameters needs to know about it.
#[derive(Debug, Clone, Copy, Default)]
pub struct ParamInfo<'a> {
    /// Parameters the statement has (from `PREPARE_OK`).
    pub count: u16,
    /// Types bound by an earlier execute, if any.
    pub types: Option<&'a [u16]>,
    /// Parameters that received long data since the last execute (index
    /// by parameter; shorter slices mean false).
    pub long_data: &'a [bool],
}

/// Looks up a prepared statement's parameter info by (client) id.
pub trait StmtLookup {
    /// Info for this statement, or `None` if the id is unknown.
    fn param_info(&self, stmt_id: u32) -> Option<ParamInfo<'_>>;
}

/// `COM_STMT_EXECUTE` arguments.
#[derive(Debug, Clone, PartialEq)]
pub struct StmtExecute {
    /// Statement id; on MariaDB 0xFFFFFFFF means the last one prepared.
    pub stmt_id: u32,
    /// Cursor type and [`PARAMETER_COUNT_AVAILABLE`].
    pub flags: u8,
    /// Iteration count, always 1.
    pub iterations: u32,
    /// Everything after the iteration count, undecoded.
    pub raw_params: Bytes,
    /// Decoded parameters, when the statement's info was known. When set,
    /// [`Command::encode`] writes these instead of `raw_params`.
    pub params: Option<ExecuteParams>,
}

/// Decoded `COM_STMT_EXECUTE` parameters.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ExecuteParams {
    /// Whether the packet carried types (`new_params_bind_flag`).
    pub new_params_bound: bool,
    /// Parameters, then query attributes (named) under
    /// `QUERY_ATTRIBUTES`. Types are filled in from the lookup when the
    /// packet didn't carry them.
    pub params: Vec<BoundParam>,
}

impl ExecuteParams {
    /// The 2-byte types, for remembering after a bind.
    #[must_use]
    pub fn types(&self) -> Vec<u16> {
        self.params.iter().map(|p| p.param_type).collect()
    }
}

/// MariaDB `COM_STMT_BULK_EXECUTE` arguments.
#[derive(Debug, Clone, PartialEq)]
pub struct BulkExecute {
    /// Statement id.
    pub stmt_id: u32,
    /// [`BULK_SEND_UNIT_RESULTS`], [`BULK_SEND_TYPES_TO_SERVER`].
    pub flags: u16,
    /// Everything after the flags, undecoded.
    pub raw: Bytes,
    /// Decoded rows, when the statement's info was known. When set,
    /// [`Command::encode`] writes these instead of `raw`.
    pub rows: Option<BulkRows>,
}

/// Decoded bulk rows.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct BulkRows {
    /// The parameter types in force (sent, or from the lookup).
    pub types: Vec<u16>,
    /// Rows of one value per parameter.
    pub rows: Vec<Vec<BulkValue>>,
}

/// One value in a bulk row, after its indicator byte.
#[derive(Debug, Clone, PartialEq)]
pub enum BulkValue {
    /// Indicator 0: a value follows.
    Value(Value),
    /// Indicator 1: NULL.
    Null,
    /// Indicator 2: the column default.
    Default,
    /// Indicator 3: leave the column unchanged.
    Ignore,
}

/// Reads a count and checks that at least `min_each` bytes per item
/// remain, so callers may allocate it.
fn bounded_count(r: &mut Reader<'_>, min_each: usize) -> ProtoResult<usize> {
    let n = r.lenenc_int()?;
    match usize::try_from(n) {
        Ok(n) if n.saturating_mul(min_each) <= r.remaining() => Ok(n),
        _ => Err(ProtoError::malformed(format!(
            "count {n} exceeds the {} bytes left",
            r.remaining()
        ))),
    }
}

fn read_value(r: &mut Reader<'_>, src: &Bytes, param_type: u16) -> ProtoResult<Value> {
    decode_binary_value(
        r,
        src,
        param_type.to_le_bytes()[0],
        param_type & PARAM_UNSIGNED != 0,
    )
}

impl QueryAttrs {
    /// Reads the attribute block at the start of a `COM_QUERY` payload
    /// (after the command byte).
    fn decode(r: &mut Reader<'_>, src: &Bytes) -> ProtoResult<Self> {
        let count = bounded_count(r, 3)?;
        let _sets = r.lenenc_int()?;
        if count == 0 {
            return Ok(Self::default());
        }
        let bitmap = r.take(null_bitmap_len(count, 0))?;
        if r.u8()? != 1 {
            return Err(ProtoError::malformed(
                "query attributes without new_params_bind_flag",
            ));
        }
        let mut heads = Vec::with_capacity(count);
        for _ in 0..count {
            let t = r.u16()?;
            let name = src.slice_ref(r.lenenc_bytes()?);
            heads.push((t, name));
        }
        let mut attrs = Vec::with_capacity(count);
        for (i, (param_type, name)) in heads.into_iter().enumerate() {
            let value = if null_bitmap_get(bitmap, i, 0) {
                Value::Null
            } else {
                read_value(r, src, param_type)?
            };
            attrs.push(BoundParam {
                param_type,
                name,
                value: ParamValue::Value(value),
            });
        }
        Ok(Self { attrs })
    }

    fn encode(&self, w: &mut Writer) -> ProtoResult<()> {
        let n = self.attrs.len();
        w.lenenc_int(n as u64).lenenc_int(1);
        if n == 0 {
            return Ok(());
        }
        encode_params(w, &self.attrs, true, true)
    }
}

/// Writes the NULL bitmap, bind flag, types (and names) and values.
fn encode_params(
    w: &mut Writer,
    params: &[BoundParam],
    bound: bool,
    names: bool,
) -> ProtoResult<()> {
    let mut bitmap = vec![0u8; null_bitmap_len(params.len(), 0)];
    for (i, p) in params.iter().enumerate() {
        if matches!(p.value, ParamValue::Value(Value::Null)) {
            null_bitmap_set(&mut bitmap, i, 0);
        }
    }
    w.bytes(&bitmap).u8(u8::from(bound));
    if bound {
        for p in params {
            w.u16(p.param_type);
            if names {
                w.lenenc_bytes(&p.name);
            }
        }
    }
    for p in params {
        if let ParamValue::Value(v) = &p.value {
            encode_binary_value(w, v, p.type_code())?;
        }
    }
    Ok(())
}

impl StmtExecute {
    /// Decodes the parameters with the statement's info. With
    /// `QUERY_ATTRIBUTES` and [`PARAMETER_COUNT_AVAILABLE`] the packet's own
    /// count (parameters plus attributes) is used.
    pub fn decode_params(&self, info: &ParamInfo<'_>, caps: Caps) -> ProtoResult<ExecuteParams> {
        let src = &self.raw_params;
        let mut r = Reader::new(src);
        let attrs = caps.contains(Caps::QUERY_ATTRIBUTES);
        let count = if attrs && self.flags & PARAMETER_COUNT_AVAILABLE != 0 {
            // NULL and long-data parameters take no value bytes, and a
            // re-execute carries no types, so a parameter can cost as little
            // as one NULL-bitmap bit: only the bitmap is checked here. The
            // allocations below grow with bytes actually read (or with the
            // lookup's remembered types), never with `count` alone.
            bounded_count(&mut r, 0)?
        } else {
            usize::from(info.count)
        };
        if count == 0 {
            return Ok(ExecuteParams::default());
        }
        let bitmap = r.take(null_bitmap_len(count, 0))?;
        let new_params_bound = r.u8()? != 0;
        let mut heads: Vec<(u16, Bytes)> = Vec::with_capacity(count.min(r.remaining()));
        if new_params_bound {
            for _ in 0..count {
                let t = r.u16()?;
                let name = if attrs {
                    src.slice_ref(r.lenenc_bytes()?)
                } else {
                    Bytes::new()
                };
                heads.push((t, name));
            }
        } else {
            let types = info
                .types
                .filter(|t| t.len() >= count)
                .ok_or_else(|| ProtoError::malformed("execute without bound parameter types"))?;
            heads.extend(types[..count].iter().map(|&t| (t, Bytes::new())));
        }
        let mut params = Vec::with_capacity(count);
        for (i, (param_type, name)) in heads.into_iter().enumerate() {
            let value = if info.long_data.get(i).copied().unwrap_or(false) {
                ParamValue::LongData
            } else if null_bitmap_get(bitmap, i, 0) {
                ParamValue::Value(Value::Null)
            } else {
                ParamValue::Value(read_value(&mut r, src, param_type)?)
            };
            params.push(BoundParam {
                param_type,
                name,
                value,
            });
        }
        Ok(ExecuteParams {
            new_params_bound,
            params,
        })
    }

    fn encode(&self, w: &mut Writer, caps: Caps) -> ProtoResult<()> {
        w.u32(self.stmt_id).u8(self.flags).u32(self.iterations);
        let Some(p) = &self.params else {
            w.bytes(&self.raw_params);
            return Ok(());
        };
        let attrs = caps.contains(Caps::QUERY_ATTRIBUTES);
        if attrs && self.flags & PARAMETER_COUNT_AVAILABLE != 0 {
            w.lenenc_int(p.params.len() as u64);
        }
        if p.params.is_empty() {
            return Ok(());
        }
        encode_params(w, &p.params, p.new_params_bound, attrs)
    }
}

impl BulkExecute {
    /// Decodes the rows with the statement's info.
    pub fn decode_rows(&self, info: &ParamInfo<'_>) -> ProtoResult<BulkRows> {
        let src = &self.raw;
        let mut r = Reader::new(src);
        let count = usize::from(info.count);
        let types: Vec<u16> = if self.flags & BULK_SEND_TYPES_TO_SERVER != 0 {
            let mut t = Vec::with_capacity(count.min(r.remaining() / 2));
            for _ in 0..count {
                t.push(r.u16()?);
            }
            t
        } else {
            info.types
                .filter(|t| t.len() >= count)
                .map(|t| t[..count].to_vec())
                .ok_or_else(|| ProtoError::malformed("bulk execute without parameter types"))?
        };
        if count == 0 {
            if !r.is_empty() {
                return Err(ProtoError::malformed(
                    "bulk rows for a statement without parameters",
                ));
            }
            return Ok(BulkRows {
                types,
                rows: Vec::new(),
            });
        }
        let mut rows = Vec::new();
        while !r.is_empty() {
            let mut row = Vec::with_capacity(count);
            for &t in &types {
                row.push(match r.u8()? {
                    0 => BulkValue::Value(read_value(&mut r, src, t)?),
                    1 => BulkValue::Null,
                    2 => BulkValue::Default,
                    3 => BulkValue::Ignore,
                    other => {
                        return Err(ProtoError::malformed(format!("bulk indicator {other}")));
                    }
                });
            }
            rows.push(row);
        }
        Ok(BulkRows { types, rows })
    }

    fn encode(&self, w: &mut Writer) -> ProtoResult<()> {
        w.u32(self.stmt_id).u16(self.flags);
        let Some(b) = &self.rows else {
            w.bytes(&self.raw);
            return Ok(());
        };
        if self.flags & BULK_SEND_TYPES_TO_SERVER != 0 {
            for &t in &b.types {
                w.u16(t);
            }
        }
        for row in &b.rows {
            if row.len() != b.types.len() {
                return Err(ProtoError::malformed("bulk row width differs from types"));
            }
            for (v, &t) in row.iter().zip(&b.types) {
                match v {
                    BulkValue::Value(v) => {
                        w.u8(0);
                        encode_binary_value(w, v, t.to_le_bytes()[0])?;
                    }
                    BulkValue::Null => {
                        w.u8(1);
                    }
                    BulkValue::Default => {
                        w.u8(2);
                    }
                    BulkValue::Ignore => {
                        w.u8(3);
                    }
                }
            }
        }
        Ok(())
    }
}

impl ChangeUser {
    fn decode(r: &mut Reader<'_>, src: &Bytes, caps: Caps) -> ProtoResult<Self> {
        let user = src.slice_ref(r.nul_bytes()?);
        let auth = if caps.contains(Caps::SECURE_CONNECTION) {
            let n = usize::from(r.u8()?);
            r.take(n)?
        } else {
            r.nul_bytes()?
        };
        let mut cu = Self {
            user,
            auth_response: src.slice_ref(auth),
            ..Self::default()
        };
        let db = r.nul_bytes_or_rest();
        cu.database = (!db.is_empty()).then(|| src.slice_ref(db));
        if r.remaining() >= 2 {
            cu.charset = Some(r.u16()?);
            if caps.contains(Caps::PLUGIN_AUTH) && !r.is_empty() {
                cu.plugin = Some(String::from_utf8_lossy(r.nul_bytes_or_rest()).into_owned());
            }
            if caps.contains(Caps::CONNECT_ATTRS) && !r.is_empty() {
                cu.attrs = decode_attrs(r)?;
            }
        }
        Ok(cu)
    }

    fn encode(&self, w: &mut Writer, caps: Caps) {
        w.nul_bytes(&self.user);
        if caps.contains(Caps::SECURE_CONNECTION) {
            let n = self.auth_response.len().min(255);
            w.u8(u8::try_from(n).unwrap_or(u8::MAX))
                .bytes(&self.auth_response[..n]);
        } else {
            w.nul_bytes(&self.auth_response);
        }
        w.nul_bytes(self.database.as_deref().unwrap_or_default());
        let Some(charset) = self.charset else {
            return;
        };
        w.u16(charset);
        if caps.contains(Caps::PLUGIN_AUTH) {
            w.nul_bytes(self.plugin.as_deref().unwrap_or_default().as_bytes());
        }
        if caps.contains(Caps::CONNECT_ATTRS) {
            encode_attrs(w, &self.attrs);
        }
    }
}

impl Command {
    /// Decodes a command payload. Never panics; malformed input is an
    /// error. Execute and bulk parameters stay raw (see
    /// [`Command::parse_with`]).
    pub fn parse(payload: &Bytes, caps: Caps, persona: Persona) -> ProtoResult<Command> {
        let mut r = Reader::new(payload);
        let code = r.u8()?;
        let src = payload;
        let rest = |r: &mut Reader<'_>| src.slice_ref(r.rest());
        Ok(match code {
            com::QUIT => Command::Quit,
            com::INIT_DB => Command::InitDb(rest(&mut r)),
            com::QUERY => {
                let attrs = if caps.contains(Caps::QUERY_ATTRIBUTES) {
                    QueryAttrs::decode(&mut r, src)?
                } else {
                    QueryAttrs::default()
                };
                Command::Query(attrs, rest(&mut r))
            }
            com::FIELD_LIST => {
                let table = src.slice_ref(r.nul_bytes_or_rest());
                Command::FieldList(FieldList {
                    table,
                    wildcard: rest(&mut r),
                })
            }
            com::CREATE_DB => Command::CreateDb(rest(&mut r)),
            com::DROP_DB => Command::DropDb(rest(&mut r)),
            com::STATISTICS => Command::Statistics,
            com::PROCESS_INFO => Command::ProcessInfo,
            com::PROCESS_KILL => Command::ProcessKill(r.u32()?),
            com::PING => Command::Ping,
            com::CHANGE_USER => Command::ChangeUser(ChangeUser::decode(&mut r, src, caps)?),
            com::STMT_PREPARE => Command::StmtPrepare(rest(&mut r)),
            com::STMT_EXECUTE => {
                let stmt_id = r.u32()?;
                let flags = r.u8()?;
                let iterations = r.u32()?;
                Command::StmtExecute(StmtExecute {
                    stmt_id,
                    flags,
                    iterations,
                    raw_params: rest(&mut r),
                    params: None,
                })
            }
            com::STMT_SEND_LONG_DATA => {
                let stmt_id = r.u32()?;
                let param = r.u16()?;
                Command::StmtSendLongData(LongData {
                    stmt_id,
                    param,
                    data: rest(&mut r),
                })
            }
            com::STMT_CLOSE => Command::StmtClose(r.u32()?),
            com::STMT_RESET => Command::StmtReset(r.u32()?),
            com::SET_OPTION => Command::SetOption(r.u16()?),
            com::STMT_FETCH => {
                let id = r.u32()?;
                Command::StmtFetch(id, r.u32()?)
            }
            com::RESET_CONNECTION => Command::ResetConnection,
            com::STMT_BULK_EXECUTE if persona == Persona::MariaDb => {
                let stmt_id = r.u32()?;
                let flags = r.u16()?;
                Command::StmtBulkExecute(BulkExecute {
                    stmt_id,
                    flags,
                    raw: rest(&mut r),
                    rows: None,
                })
            }
            com::REFRESH
            | com::SHUTDOWN
            | com::DEBUG
            | com::BINLOG_DUMP
            | com::REGISTER_SLAVE
            | com::BINLOG_DUMP_GTID => Command::Privileged(code),
            other => Command::Unknown(other),
        })
    }

    /// [`Command::parse`], then decodes execute and bulk parameters when
    /// `lookup` knows the statement. An unknown id leaves them raw; the
    /// caller answers 1243.
    pub fn parse_with(
        payload: &Bytes,
        caps: Caps,
        persona: Persona,
        lookup: &dyn StmtLookup,
    ) -> ProtoResult<Command> {
        let mut cmd = Self::parse(payload, caps, persona)?;
        match &mut cmd {
            Command::StmtExecute(e) => {
                if let Some(info) = lookup.param_info(e.stmt_id) {
                    e.params = Some(e.decode_params(&info, caps)?);
                }
            }
            Command::StmtBulkExecute(b) => {
                if let Some(info) = lookup.param_info(b.stmt_id) {
                    b.rows = Some(b.decode_rows(&info)?);
                }
            }
            _ => {}
        }
        Ok(cmd)
    }

    /// The command code.
    #[must_use]
    pub fn code(&self) -> u8 {
        match self {
            Command::Quit => com::QUIT,
            Command::InitDb(_) => com::INIT_DB,
            Command::Query(..) => com::QUERY,
            Command::FieldList(_) => com::FIELD_LIST,
            Command::CreateDb(_) => com::CREATE_DB,
            Command::DropDb(_) => com::DROP_DB,
            Command::Statistics => com::STATISTICS,
            Command::ProcessInfo => com::PROCESS_INFO,
            Command::ProcessKill(_) => com::PROCESS_KILL,
            Command::Ping => com::PING,
            Command::ChangeUser(_) => com::CHANGE_USER,
            Command::StmtPrepare(_) => com::STMT_PREPARE,
            Command::StmtExecute(_) => com::STMT_EXECUTE,
            Command::StmtSendLongData(_) => com::STMT_SEND_LONG_DATA,
            Command::StmtClose(_) => com::STMT_CLOSE,
            Command::StmtReset(_) => com::STMT_RESET,
            Command::SetOption(_) => com::SET_OPTION,
            Command::StmtFetch(..) => com::STMT_FETCH,
            Command::ResetConnection => com::RESET_CONNECTION,
            Command::StmtBulkExecute(_) => com::STMT_BULK_EXECUTE,
            Command::Privileged(c) | Command::Unknown(c) => *c,
        }
    }

    /// A short label for metrics (`sprig_mysql_commands_total`).
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
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

    /// Encodes the command payload for a server with these capabilities.
    /// Fails only when a parameter value doesn't fit its declared type.
    pub fn encode(&self, caps: Caps) -> ProtoResult<Bytes> {
        let mut w = Writer::new();
        w.u8(self.code());
        match self {
            Command::Quit
            | Command::Statistics
            | Command::ProcessInfo
            | Command::Ping
            | Command::ResetConnection
            | Command::Privileged(_)
            | Command::Unknown(_) => {}
            Command::InitDb(s)
            | Command::CreateDb(s)
            | Command::DropDb(s)
            | Command::StmtPrepare(s) => {
                w.bytes(s);
            }
            Command::Query(attrs, sql) => {
                if caps.contains(Caps::QUERY_ATTRIBUTES) {
                    attrs.encode(&mut w)?;
                }
                w.bytes(sql);
            }
            Command::FieldList(f) => {
                w.nul_bytes(&f.table).bytes(&f.wildcard);
            }
            Command::ProcessKill(id) | Command::StmtClose(id) | Command::StmtReset(id) => {
                w.u32(*id);
            }
            Command::ChangeUser(c) => c.encode(&mut w, caps),
            Command::StmtExecute(e) => e.encode(&mut w, caps)?,
            Command::StmtSendLongData(l) => {
                w.u32(l.stmt_id).u16(l.param).bytes(&l.data);
            }
            Command::SetOption(o) => {
                w.u16(*o);
            }
            Command::StmtFetch(id, rows) => {
                w.u32(*id).u32(*rows);
            }
            Command::StmtBulkExecute(b) => b.encode(&mut w)?,
        }
        Ok(w.into_bytes())
    }
}

#[cfg(test)]
mod tests;
