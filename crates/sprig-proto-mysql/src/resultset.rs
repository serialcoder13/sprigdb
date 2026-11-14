//! Resultsets: column definitions, column counts, text and binary rows, the
//! binary [`Value`] encoding shared with `COM_STMT_EXECUTE`, `PREPARE_OK`, and
//! classifiers for the packets of a response.
//!
//! A response starts with OK, ERR, a `LOCAL INFILE` request or a column
//! count ([`classify_response`]). A resultset then has the column
//! definitions, an EOF unless `DEPRECATE_EOF`, rows, and a terminator: EOF,
//! or OK with header 0xFE under `DEPRECATE_EOF` ([`classify_row`]).

mod value;

pub use value::{
    DateTime, Time, Value, decode_binary_row, decode_binary_row_types, decode_binary_value,
    encode_binary_row, encode_binary_row_types, encode_binary_value, null_bitmap_get,
    null_bitmap_len, null_bitmap_set,
};

use bytes::Bytes;

use crate::buf::{Reader, Writer};
use crate::error::ErrPacket;
use crate::packets::{
    Caps, EOF_HEADER, ERR_HEADER, EofPacket, LOCAL_INFILE_HEADER, OK_HEADER, OkPacket, StatusFlags,
};
use crate::proto_error::{ProtoError, ProtoResult};

/// Column type codes (`enum_field_types`).
pub mod column_type {
    /// `DECIMAL` (old).
    pub const DECIMAL: u8 = 0;
    /// `TINYINT`.
    pub const TINY: u8 = 1;
    /// `SMALLINT`.
    pub const SHORT: u8 = 2;
    /// `INT`.
    pub const LONG: u8 = 3;
    /// `FLOAT`.
    pub const FLOAT: u8 = 4;
    /// `DOUBLE`.
    pub const DOUBLE: u8 = 5;
    /// `NULL`.
    pub const NULL: u8 = 6;
    /// `TIMESTAMP`.
    pub const TIMESTAMP: u8 = 7;
    /// `BIGINT`.
    pub const LONGLONG: u8 = 8;
    /// `MEDIUMINT`.
    pub const INT24: u8 = 9;
    /// `DATE`.
    pub const DATE: u8 = 10;
    /// `TIME`.
    pub const TIME: u8 = 11;
    /// `DATETIME`.
    pub const DATETIME: u8 = 12;
    /// `YEAR`.
    pub const YEAR: u8 = 13;
    /// `NEWDATE` (internal).
    pub const NEWDATE: u8 = 14;
    /// `VARCHAR`.
    pub const VARCHAR: u8 = 15;
    /// `BIT`.
    pub const BIT: u8 = 16;
    /// `TIMESTAMP` with fractional seconds (internal).
    pub const TIMESTAMP2: u8 = 17;
    /// `DATETIME` with fractional seconds (internal).
    pub const DATETIME2: u8 = 18;
    /// `TIME` with fractional seconds (internal).
    pub const TIME2: u8 = 19;
    /// MySQL 9 `VECTOR`.
    pub const VECTOR: u8 = 242;
    /// `JSON` (MySQL; MariaDB sends `LONGTEXT` with extended type `json`).
    pub const JSON: u8 = 245;
    /// `DECIMAL`.
    pub const NEWDECIMAL: u8 = 246;
    /// `ENUM`.
    pub const ENUM: u8 = 247;
    /// `SET`.
    pub const SET: u8 = 248;
    /// `TINYBLOB`.
    pub const TINY_BLOB: u8 = 249;
    /// `MEDIUMBLOB`.
    pub const MEDIUM_BLOB: u8 = 250;
    /// `LONGBLOB`.
    pub const LONG_BLOB: u8 = 251;
    /// `BLOB`.
    pub const BLOB: u8 = 252;
    /// `VARCHAR` / `VARBINARY`.
    pub const VAR_STRING: u8 = 253;
    /// `CHAR` / `BINARY`.
    pub const STRING: u8 = 254;
    /// `GEOMETRY`.
    pub const GEOMETRY: u8 = 255;
}

/// The unsigned flag in a 2-byte parameter type (`COM_STMT_EXECUTE`).
pub const PARAM_UNSIGNED: u16 = 0x8000;

bitflags::bitflags! {
    /// Column definition flags.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct ColumnFlags: u16 {
        /// `NOT NULL`.
        const NOT_NULL = 1;
        /// Part of the primary key.
        const PRI_KEY = 1 << 1;
        /// Part of a unique key.
        const UNIQUE_KEY = 1 << 2;
        /// Part of a non-unique key.
        const MULTIPLE_KEY = 1 << 3;
        /// A BLOB or TEXT column.
        const BLOB = 1 << 4;
        /// `UNSIGNED`.
        const UNSIGNED = 1 << 5;
        /// `ZEROFILL`.
        const ZEROFILL = 1 << 6;
        /// Binary collation.
        const BINARY = 1 << 7;
        /// `ENUM`.
        const ENUM = 1 << 8;
        /// `AUTO_INCREMENT`.
        const AUTO_INCREMENT = 1 << 9;
        /// `TIMESTAMP`.
        const TIMESTAMP = 1 << 10;
        /// `SET`.
        const SET = 1 << 11;
        /// No default value.
        const NO_DEFAULT_VALUE = 1 << 12;
        /// `ON UPDATE CURRENT_TIMESTAMP`.
        const ON_UPDATE_NOW = 1 << 13;
        /// Numeric.
        const NUM = 1 << 15;
    }
}

/// Extended metadata entry types (MariaDB).
const EXT_TYPE_NAME: u8 = 0;
const EXT_FORMAT_NAME: u8 = 1;

/// A column definition (`Protocol::ColumnDefinition41`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ColumnDef {
    /// Schema; physical on the backend side, rewritten to logical.
    pub schema: Bytes,
    /// Table alias.
    pub table: Bytes,
    /// Table name.
    pub org_table: Bytes,
    /// Column alias.
    pub name: Bytes,
    /// Column name.
    pub org_name: Bytes,
    /// MariaDB extended metadata block, raw (`MARIADB_CLIENT_EXTENDED_METADATA`);
    /// `None` when absent or empty. See [`ColumnDef::ext_type_name`].
    pub ext_type: Option<Bytes>,
    /// Character set (collation id).
    pub charset: u16,
    /// Maximum display length.
    pub length: u32,
    /// Type code, see [`column_type`].
    pub col_type: u8,
    /// Flags, see [`ColumnFlags`].
    pub flags: u16,
    /// Decimals.
    pub decimals: u8,
}

impl ColumnDef {
    /// Decodes a column definition. The extended metadata block is read
    /// only when `caps` has `MARIADB_CLIENT_EXTENDED_METADATA`. Trailing
    /// bytes (`COM_FIELD_LIST` default values) are ignored.
    pub fn decode(payload: &Bytes, caps: Caps) -> ProtoResult<Self> {
        let mut r = Reader::new(payload);
        let mut s = || -> ProtoResult<Bytes> { Ok(payload.slice_ref(r.lenenc_bytes()?)) };
        let _catalog = s()?;
        let schema = s()?;
        let table = s()?;
        let org_table = s()?;
        let name = s()?;
        let org_name = s()?;
        let ext_type = if caps.contains(Caps::MARIADB_CLIENT_EXTENDED_METADATA) {
            Some(s()?).filter(|b| !b.is_empty())
        } else {
            None
        };
        let fixed = r.lenenc_int()?;
        if fixed < 12 {
            return Err(ProtoError::malformed(format!(
                "column definition fixed-field length {fixed}"
            )));
        }
        let charset = r.u16()?;
        let length = r.u32()?;
        let col_type = r.u8()?;
        let flags = r.u16()?;
        let decimals = r.u8()?;
        Ok(Self {
            schema,
            table,
            org_table,
            name,
            org_name,
            ext_type,
            charset,
            length,
            col_type,
            flags,
            decimals,
        })
    }

    /// Encodes for a peer with these capabilities: the extended metadata
    /// block is written (empty if `None`) exactly when `caps` has
    /// `MARIADB_CLIENT_EXTENDED_METADATA`.
    #[must_use]
    pub fn encode(&self, caps: Caps) -> Bytes {
        let mut w = Writer::with_capacity(
            32 + self.schema.len()
                + self.table.len()
                + self.org_table.len()
                + self.name.len()
                + self.org_name.len(),
        );
        w.lenenc_bytes(b"def")
            .lenenc_bytes(&self.schema)
            .lenenc_bytes(&self.table)
            .lenenc_bytes(&self.org_table)
            .lenenc_bytes(&self.name)
            .lenenc_bytes(&self.org_name);
        if caps.contains(Caps::MARIADB_CLIENT_EXTENDED_METADATA) {
            w.lenenc_bytes(self.ext_type.as_deref().unwrap_or_default());
        }
        w.u8(0x0C)
            .u16(self.charset)
            .u32(self.length)
            .u8(self.col_type)
            .u16(self.flags)
            .u8(self.decimals)
            .u16(0);
        w.into_bytes()
    }

    /// Whether the column is `UNSIGNED`.
    #[must_use]
    pub fn is_unsigned(&self) -> bool {
        self.flags & ColumnFlags::UNSIGNED.bits() != 0
    }

    fn ext_entry(&self, kind: u8) -> Option<&[u8]> {
        let block = self.ext_type.as_deref()?;
        let mut r = Reader::new(block);
        while !r.is_empty() {
            let k = r.u8().ok()?;
            let v = r.lenenc_bytes().ok()?;
            if k == kind {
                return Some(v);
            }
        }
        None
    }

    /// MariaDB extended type name, such as `json`, `uuid` or `inet6`.
    #[must_use]
    pub fn ext_type_name(&self) -> Option<&[u8]> {
        self.ext_entry(EXT_TYPE_NAME)
    }

    /// MariaDB extended format name, such as `json` for a `LONGTEXT`.
    #[must_use]
    pub fn ext_format_name(&self) -> Option<&[u8]> {
        self.ext_entry(EXT_FORMAT_NAME)
    }

    /// Builds an extended metadata block for [`ColumnDef::ext_type`];
    /// `None` when both are absent.
    #[must_use]
    pub fn ext_metadata(type_name: Option<&[u8]>, format_name: Option<&[u8]>) -> Option<Bytes> {
        let mut w = Writer::new();
        if let Some(t) = type_name {
            w.u8(EXT_TYPE_NAME).lenenc_bytes(t);
        }
        if let Some(f) = format_name {
            w.u8(EXT_FORMAT_NAME).lenenc_bytes(f);
        }
        (!w.is_empty()).then(|| w.into_bytes())
    }
}

/// Decodes the column-count packet that starts a resultset.
pub fn decode_column_count(payload: &[u8]) -> ProtoResult<u64> {
    let mut r = Reader::new(payload);
    let n = r.lenenc_int()?;
    if n == 0 {
        return Err(ProtoError::malformed("column count 0"));
    }
    Ok(n)
}

/// Encodes a column-count packet.
#[must_use]
pub fn encode_column_count(n: u64) -> Bytes {
    let mut w = Writer::with_capacity(9);
    w.lenenc_int(n);
    w.into_bytes()
}

/// Decodes a text row of `columns` values; `None` is SQL NULL.
pub fn decode_text_row(payload: &[u8], columns: usize) -> ProtoResult<Vec<Option<&[u8]>>> {
    let mut r = Reader::new(payload);
    let mut out = Vec::with_capacity(columns.min(payload.len()));
    for _ in 0..columns {
        out.push(r.lenenc_bytes_or_null()?);
    }
    if !r.is_empty() {
        return Err(ProtoError::malformed(format!(
            "text row has {} bytes after {columns} values",
            r.remaining()
        )));
    }
    Ok(out)
}

/// Encodes a text row.
pub fn encode_text_row<'a>(values: impl IntoIterator<Item = Option<&'a [u8]>>) -> Bytes {
    let mut w = Writer::new();
    for v in values {
        w.lenenc_bytes_or_null(v);
    }
    w.into_bytes()
}

/// `COM_STMT_PREPARE` success (`COM_STMT_PREPARE_OK`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PrepareOk {
    /// Statement id.
    pub stmt_id: u32,
    /// Columns in the resultset.
    pub columns: u16,
    /// Parameters.
    pub params: u16,
    /// Warning count.
    pub warnings: u16,
    /// `OPTIONAL_RESULTSET_METADATA` metadata-follows flag.
    pub metadata_follows: Option<u8>,
}

impl PrepareOk {
    /// Decodes the packet.
    pub fn decode(payload: &[u8]) -> ProtoResult<Self> {
        let mut r = Reader::new(payload);
        if r.u8()? != OK_HEADER {
            return Err(ProtoError::malformed("PREPARE_OK header"));
        }
        let stmt_id = r.u32()?;
        let columns = r.u16()?;
        let params = r.u16()?;
        let mut p = Self {
            stmt_id,
            columns,
            params,
            ..Self::default()
        };
        if r.is_empty() {
            return Ok(p);
        }
        r.skip(1)?;
        if r.remaining() >= 2 {
            p.warnings = r.u16()?;
        }
        p.metadata_follows = r.u8().ok();
        Ok(p)
    }

    /// Encodes the packet.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut w = Writer::with_capacity(13);
        w.u8(OK_HEADER)
            .u32(self.stmt_id)
            .u16(self.columns)
            .u16(self.params)
            .u8(0)
            .u16(self.warnings);
        if let Some(m) = self.metadata_follows {
            w.u8(m);
        }
        w.into_bytes()
    }
}

/// The packet that ends column definitions or rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Terminator {
    /// EOF, without `DEPRECATE_EOF`.
    Eof(EofPacket),
    /// OK with header 0xFE, under `DEPRECATE_EOF`.
    Ok(OkPacket),
}

impl Terminator {
    /// Status flags.
    #[must_use]
    pub fn status(&self) -> StatusFlags {
        match self {
            Terminator::Eof(e) => e.status,
            Terminator::Ok(o) => o.status,
        }
    }

    /// Warning count.
    #[must_use]
    pub fn warnings(&self) -> u16 {
        match self {
            Terminator::Eof(e) => e.warnings,
            Terminator::Ok(o) => o.warnings,
        }
    }

    /// Decodes a 0xFE packet in the form `caps` implies.
    pub fn decode(payload: &Bytes, caps: Caps) -> ProtoResult<Self> {
        if caps.contains(Caps::DEPRECATE_EOF) {
            OkPacket::decode(payload, caps).map(Terminator::Ok)
        } else {
            EofPacket::decode(payload, caps).map(Terminator::Eof)
        }
    }

    /// Encodes in the form a peer with `caps` expects: OK with header 0xFE
    /// under `DEPRECATE_EOF`, else EOF. Converting EOF to OK keeps status
    /// and warnings; converting OK to EOF drops the OK-only fields.
    #[must_use]
    pub fn encode(&self, caps: Caps) -> Bytes {
        if caps.contains(Caps::DEPRECATE_EOF) {
            let ok = match self {
                Terminator::Ok(o) => OkPacket {
                    header: EOF_HEADER,
                    ..o.clone()
                },
                Terminator::Eof(e) => OkPacket {
                    header: EOF_HEADER,
                    status: e.status,
                    warnings: e.warnings,
                    ..OkPacket::default()
                },
            };
            ok.encode(caps)
        } else {
            EofPacket {
                warnings: self.warnings(),
                status: self.status(),
            }
            .encode(caps)
        }
    }
}

/// A packet inside a row stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowPacket {
    /// A row (text or binary); relay or decode it.
    Row,
    /// The end of the rows.
    End(Terminator),
    /// An error ends the resultset.
    Err(ErrPacket),
}

/// Classifies a packet read where a row or terminator is expected. A 0xFE
/// packet shorter than 16 MiB - 1 is a terminator; a row starting with 0xFE
/// would hold a value of at least 16 MiB.
pub fn classify_row(payload: &Bytes, caps: Caps) -> ProtoResult<RowPacket> {
    match payload.first() {
        Some(&EOF_HEADER) if payload.len() < crate::codec::MAX_CHUNK => {
            Terminator::decode(payload, caps).map(RowPacket::End)
        }
        Some(&ERR_HEADER) => ErrPacket::decode(payload).map(RowPacket::Err),
        Some(_) => Ok(RowPacket::Row),
        None => Err(ProtoError::malformed("empty packet in a row stream")),
    }
}

/// The first packet of a response to a command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    /// OK.
    Ok(OkPacket),
    /// ERR.
    Err(ErrPacket),
    /// `LOCAL INFILE` request with the file name.
    LocalInfile(Bytes),
    /// A resultset with this many columns follows.
    ResultSet {
        /// Column count.
        columns: u64,
    },
}

/// Classifies the first packet of a `COM_QUERY` or `COM_STMT_EXECUTE`
/// response.
pub fn classify_response(payload: &Bytes, caps: Caps) -> ProtoResult<Response> {
    match payload.first() {
        Some(&OK_HEADER) => OkPacket::decode(payload, caps).map(Response::Ok),
        Some(&ERR_HEADER) => ErrPacket::decode(payload).map(Response::Err),
        Some(&LOCAL_INFILE_HEADER) => Ok(Response::LocalInfile(payload.slice(1..))),
        Some(_) => decode_column_count(payload).map(|columns| Response::ResultSet { columns }),
        None => Err(ProtoError::malformed("empty response packet")),
    }
}

#[cfg(test)]
mod tests;
