//! `TABLE_MAP` events: the table id, schema, table and column descriptions
//! that the following rows events refer to.
//!
//! Column metadata comes in two parts. The fixed part, always present, gives
//! each column's binlog type and type parameters (length, precision,
//! fractional digits). The optional part depends on `binlog_row_metadata`:
//! `MINIMAL` adds signedness, character sets and geometry types; `FULL` adds
//! column names, `ENUM` and `SET` values and the primary key. Without it,
//! integers decode as signed and strings carry no collation.

use sprig_common::SprigError;

use crate::buf::{Buf, corrupt};
use crate::consts::column_type as ct;

/// The resolved type of a column, from its binlog type code and metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    /// `TINYINT`, 1 byte.
    TinyInt,
    /// `SMALLINT`, 2 bytes.
    SmallInt,
    /// `MEDIUMINT`, 3 bytes.
    MediumInt,
    /// `INT`, 4 bytes.
    Int,
    /// `BIGINT`, 8 bytes.
    BigInt,
    /// `FLOAT`.
    Float,
    /// `DOUBLE`.
    Double,
    /// `DECIMAL(precision, scale)`.
    Decimal {
        /// Total digits.
        precision: u8,
        /// Digits after the point.
        scale: u8,
    },
    /// `BIT(bits)`.
    Bit {
        /// Width in bits, 1 to 64.
        bits: u16,
    },
    /// `YEAR`.
    Year,
    /// `DATE`.
    Date,
    /// `TIME(fsp)`. `legacy` is the pre-5.6 format (type 11).
    Time {
        /// Fractional-second digits, 0 to 6.
        fsp: u8,
        /// The old format (`mysql56_temporal_format = OFF`).
        legacy: bool,
    },
    /// `DATETIME(fsp)`.
    DateTime {
        /// Fractional-second digits.
        fsp: u8,
        /// The old format.
        legacy: bool,
    },
    /// `TIMESTAMP(fsp)`.
    Timestamp {
        /// Fractional-second digits.
        fsp: u8,
        /// The old format.
        legacy: bool,
    },
    /// `VARCHAR`/`VARBINARY` (and MariaDB's `UUID`, `INET4`, `INET6` and
    /// `VECTOR`, which log as one of the string types).
    VarChar {
        /// Maximum length in bytes.
        max_len: u16,
        /// MariaDB column compression.
        compressed: bool,
    },
    /// `CHAR`/`BINARY`.
    Char {
        /// Length in bytes.
        max_len: u16,
    },
    /// `ENUM`, stored as its 1-based index.
    Enum {
        /// Bytes of the index: 1 or 2.
        size: u8,
    },
    /// `SET`, stored as a bitmap of members.
    Set {
        /// Bytes of the bitmap: 1 to 8.
        size: u8,
    },
    /// `TINYBLOB` to `LONGBLOB` and `TINYTEXT` to `LONGTEXT`; MariaDB's
    /// `JSON` is `LONGTEXT`.
    Blob {
        /// Bytes of the length prefix: 1 to 4.
        pack_len: u8,
        /// MariaDB column compression.
        compressed: bool,
    },
    /// A spatial type, as `SRID (4 bytes LE) + WKB`.
    Geometry {
        /// Bytes of the length prefix.
        pack_len: u8,
    },
    /// MySQL's binary JSON; MariaDB never logs it.
    MysqlJson {
        /// Bytes of the length prefix.
        pack_len: u8,
    },
    /// The `NULL` type.
    Null,
    /// A type code this decoder doesn't know.
    Unknown(u8),
}

impl ColumnType {
    /// Whether the optional metadata's signedness bitmap covers this column
    /// (MariaDB's `is_numeric_type`).
    fn is_numeric(self) -> bool {
        matches!(
            self,
            ColumnType::TinyInt
                | ColumnType::SmallInt
                | ColumnType::MediumInt
                | ColumnType::Int
                | ColumnType::BigInt
                | ColumnType::Float
                | ColumnType::Double
                | ColumnType::Decimal { .. }
        )
    }

    /// Whether the character-set metadata covers this column.
    fn is_character(self) -> bool {
        matches!(
            self,
            ColumnType::VarChar { .. } | ColumnType::Char { .. } | ColumnType::Blob { .. }
        )
    }

    fn is_enum_or_set(self) -> bool {
        matches!(self, ColumnType::Enum { .. } | ColumnType::Set { .. })
    }
}

/// One column of a [`TableMap`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// The binlog type code, as logged.
    pub type_code: u8,
    /// The type metadata, as logged (0 to 2 bytes, little-endian).
    pub meta: u16,
    /// The resolved type.
    pub kind: ColumnType,
    /// Whether the column accepts `NULL`.
    pub nullable: bool,
    /// The name (`binlog_row_metadata = FULL`).
    pub name: Option<String>,
    /// Signedness for numeric columns (`MINIMAL` or `FULL`); `None` when
    /// unknown, in which case integers decode as signed.
    pub unsigned: Option<bool>,
    /// The collation id of a string column (63 is `binary`), or of the
    /// member names of an `ENUM`/`SET` column.
    pub collation: Option<u16>,
    /// `ENUM` or `SET` member names, in order (`FULL`).
    pub members: Option<Vec<Vec<u8>>>,
    /// The geometry subtype (`MINIMAL` or `FULL`): 0 geometry, 1 point,
    /// 2 linestring, 3 polygon, 4 multipoint, 5 multilinestring,
    /// 6 multipolygon, 7 geometrycollection.
    pub geometry_type: Option<u32>,
}

impl Column {
    /// Whether the column holds bytes rather than text (collation 63).
    #[must_use]
    pub fn is_binary(&self) -> bool {
        self.collation == Some(63)
    }
}

/// A decoded `TABLE_MAP` event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMap {
    /// The id rows events use; valid until the table's definition changes.
    pub table_id: u64,
    /// The event's flags.
    pub flags: u16,
    /// Schema (database) name.
    pub schema: String,
    /// Table name.
    pub table: String,
    /// Columns, in table order.
    pub columns: Vec<Column>,
    /// Primary-key columns, as (column index, prefix length or 0) (`FULL`).
    pub primary_key: Vec<(usize, u32)>,
}

impl TableMap {
    /// Parses a `TABLE_MAP` body (after the 19-byte header, without the
    /// checksum). `post_header_len` is 6 for 4-byte table ids and 8 for the
    /// usual 6-byte ones.
    ///
    /// # Errors
    /// `ER_SPRIG_BINLOG_CORRUPT` if the body is malformed.
    pub fn parse(body: &[u8], post_header_len: u8) -> Result<Self, SprigError> {
        let mut b = Buf::new(body);
        let table_id = if post_header_len == 6 {
            u64::from(b.u32()?)
        } else {
            b.u48()?
        };
        let flags = b.u16()?;
        let schema_len = usize::from(b.u8()?);
        let schema = text(b.take(schema_len)?);
        b.skip(1)?;
        let table_len = usize::from(b.u8()?);
        let table = text(b.take(table_len)?);
        b.skip(1)?;
        let ncols = b.lenenc_usize()?;
        if ncols > 4096 {
            return Err(corrupt(format!("table map lists {ncols} columns")));
        }
        let types = b.take(ncols)?.to_vec();
        let meta_block = b.lenenc_bytes()?;
        let mut mb = Buf::new(meta_block);
        let mut columns = Vec::with_capacity(ncols);
        for &t in &types {
            let (meta, kind) = read_column_meta(&mut mb, t)?;
            columns.push(Column {
                type_code: t,
                meta,
                kind,
                nullable: false,
                name: None,
                unsigned: None,
                collation: None,
                members: None,
                geometry_type: None,
            });
        }
        let nulls = b.take(ncols.div_ceil(8))?;
        for (i, c) in columns.iter_mut().enumerate() {
            c.nullable = bit(nulls, i);
        }
        let mut map = TableMap {
            table_id,
            flags,
            schema,
            table,
            columns,
            primary_key: Vec::new(),
        };
        while !b.is_empty() {
            let field_type = b.u8()?;
            let value = b.lenenc_bytes()?;
            map.apply_optional(field_type, value)?;
        }
        Ok(map)
    }

    /// The index of the column named `name` (`binlog_row_metadata = FULL`).
    #[must_use]
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.name.as_deref() == Some(name))
    }

    fn apply_optional(&mut self, field_type: u8, value: &[u8]) -> Result<(), SprigError> {
        let mut v = Buf::new(value);
        match field_type {
            // SIGNEDNESS: one bit per numeric column, most significant first.
            1 => {
                let numeric = self.indexes(|c| c.kind.is_numeric());
                for (n, i) in numeric.into_iter().enumerate() {
                    let byte = value.get(n / 8).copied().unwrap_or(0);
                    self.columns[i].unsigned = Some(byte & (0x80 >> (n % 8)) != 0);
                }
            }
            // DEFAULT_CHARSET / ENUM_AND_SET_DEFAULT_CHARSET.
            2 | 10 => {
                let targets = self.charset_targets(field_type == 10);
                let default = to_u16(v.lenenc()?);
                for &i in &targets {
                    self.columns[i].collation = Some(default);
                }
                while !v.is_empty() {
                    let n = v.lenenc_usize()?;
                    let coll = to_u16(v.lenenc()?);
                    if let Some(&i) = targets.get(n) {
                        self.columns[i].collation = Some(coll);
                    }
                }
            }
            // COLUMN_CHARSET / ENUM_AND_SET_COLUMN_CHARSET.
            3 | 11 => {
                for i in self.charset_targets(field_type == 11) {
                    if v.is_empty() {
                        break;
                    }
                    self.columns[i].collation = Some(to_u16(v.lenenc()?));
                }
            }
            // COLUMN_NAME.
            4 => {
                for c in &mut self.columns {
                    if v.is_empty() {
                        break;
                    }
                    c.name = Some(text(v.lenenc_bytes()?));
                }
            }
            // SET_STR_VALUE / ENUM_STR_VALUE.
            5 | 6 => {
                let want_set = field_type == 5;
                let targets = self.indexes(|c| {
                    if want_set {
                        matches!(c.kind, ColumnType::Set { .. })
                    } else {
                        matches!(c.kind, ColumnType::Enum { .. })
                    }
                });
                for i in targets {
                    if v.is_empty() {
                        break;
                    }
                    let n = v.lenenc_usize()?;
                    let mut members = Vec::with_capacity(n.min(1024));
                    for _ in 0..n {
                        members.push(v.lenenc_bytes()?.to_vec());
                    }
                    self.columns[i].members = Some(members);
                }
            }
            // GEOMETRY_TYPE.
            7 => {
                for i in self.indexes(|c| matches!(c.kind, ColumnType::Geometry { .. })) {
                    if v.is_empty() {
                        break;
                    }
                    self.columns[i].geometry_type = u32::try_from(v.lenenc()?).ok();
                }
            }
            // SIMPLE_PRIMARY_KEY.
            8 => {
                while !v.is_empty() {
                    self.primary_key.push((v.lenenc_usize()?, 0));
                }
            }
            // PRIMARY_KEY_WITH_PREFIX.
            9 => {
                while !v.is_empty() {
                    let i = v.lenenc_usize()?;
                    let prefix = u32::try_from(v.lenenc()?).unwrap_or(u32::MAX);
                    self.primary_key.push((i, prefix));
                }
            }
            // Column visibility (MySQL 8.0.23+) and anything newer: skip.
            _ => {}
        }
        Ok(())
    }

    fn indexes(&self, f: impl Fn(&Column) -> bool) -> Vec<usize> {
        (0..self.columns.len())
            .filter(|&i| f(&self.columns[i]))
            .collect()
    }

    fn charset_targets(&self, enum_set: bool) -> Vec<usize> {
        if enum_set {
            self.indexes(|c| c.kind.is_enum_or_set())
        } else {
            self.indexes(|c| c.kind.is_character())
        }
    }
}

/// Reads one column's fixed metadata and resolves its type.
#[allow(clippy::too_many_lines)] // one arm per binlog type code
fn read_column_meta(mb: &mut Buf<'_>, t: u8) -> Result<(u16, ColumnType), SprigError> {
    Ok(match t {
        ct::TINY => (0, ColumnType::TinyInt),
        ct::SHORT => (0, ColumnType::SmallInt),
        ct::INT24 => (0, ColumnType::MediumInt),
        ct::LONG => (0, ColumnType::Int),
        ct::LONGLONG => (0, ColumnType::BigInt),
        ct::FLOAT => (u16::from(mb.u8()?), ColumnType::Float),
        ct::DOUBLE => (u16::from(mb.u8()?), ColumnType::Double),
        ct::NEWDECIMAL => {
            let precision = mb.u8()?;
            let scale = mb.u8()?;
            (
                u16::from_le_bytes([precision, scale]),
                ColumnType::Decimal { precision, scale },
            )
        }
        ct::BIT => {
            let bits = mb.u8()?;
            let bytes = mb.u8()?;
            (
                u16::from_le_bytes([bits, bytes]),
                ColumnType::Bit {
                    bits: u16::from(bytes) * 8 + u16::from(bits),
                },
            )
        }
        ct::YEAR => (0, ColumnType::Year),
        ct::DATE | ct::NEWDATE => (0, ColumnType::Date),
        ct::TIME => (
            0,
            ColumnType::Time {
                fsp: 0,
                legacy: true,
            },
        ),
        ct::DATETIME => (
            0,
            ColumnType::DateTime {
                fsp: 0,
                legacy: true,
            },
        ),
        ct::TIMESTAMP => (
            0,
            ColumnType::Timestamp {
                fsp: 0,
                legacy: true,
            },
        ),
        ct::TIME2 | ct::DATETIME2 | ct::TIMESTAMP2 => {
            let fsp = mb.u8()?;
            let kind = match t {
                ct::TIME2 => ColumnType::Time { fsp, legacy: false },
                ct::DATETIME2 => ColumnType::DateTime { fsp, legacy: false },
                _ => ColumnType::Timestamp { fsp, legacy: false },
            };
            (u16::from(fsp), kind)
        }
        ct::VARCHAR | ct::VAR_STRING | ct::VARCHAR_COMPRESSED => {
            let max_len = mb.u16()?;
            (
                max_len,
                ColumnType::VarChar {
                    max_len,
                    compressed: t == ct::VARCHAR_COMPRESSED,
                },
            )
        }
        ct::STRING | ct::ENUM | ct::SET => {
            let b0 = mb.u8()?;
            let b1 = mb.u8()?;
            let meta = u16::from_le_bytes([b0, b1]);
            // Long CHAR columns fold length bits into the real-type byte.
            let (real, len) = if b0 & 0x30 == 0x30 {
                (b0, u16::from(b1))
            } else {
                (
                    b0 | 0x30,
                    u16::from(b1) | (u16::from((b0 & 0x30) ^ 0x30) << 4),
                )
            };
            let kind = match real {
                ct::ENUM => ColumnType::Enum { size: b1 },
                ct::SET => ColumnType::Set { size: b1 },
                _ => ColumnType::Char { max_len: len },
            };
            (meta, kind)
        }
        ct::BLOB | ct::TINY_BLOB | ct::MEDIUM_BLOB | ct::LONG_BLOB | ct::BLOB_COMPRESSED => {
            let pack_len = mb.u8()?;
            (
                u16::from(pack_len),
                ColumnType::Blob {
                    pack_len,
                    compressed: t == ct::BLOB_COMPRESSED,
                },
            )
        }
        ct::GEOMETRY => {
            let pack_len = mb.u8()?;
            (u16::from(pack_len), ColumnType::Geometry { pack_len })
        }
        ct::JSON => {
            let pack_len = mb.u8()?;
            (u16::from(pack_len), ColumnType::MysqlJson { pack_len })
        }
        ct::NULL => (0, ColumnType::Null),
        // Includes the pre-5.0 DECIMAL (type 0), which MariaDB never logs.
        _ => (0, ColumnType::Unknown(t)),
    })
}

fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn to_u16(v: u64) -> u16 {
    u16::try_from(v).unwrap_or(u16::MAX)
}

/// Bit `i` of a little-endian bitmap (bit 0 is the low bit of byte 0).
pub(crate) fn bit(bitmap: &[u8], i: usize) -> bool {
    bitmap.get(i / 8).is_some_and(|b| b & (1 << (i % 8)) != 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `frag.t (id INT PRIMARY KEY, b LONGBLOB)` as MariaDB 12.3 logged it
    /// (body only, from `mariadb-binlog --hexdump`).
    const FRAG_T: &[u8] = &[
        0x12, 0, 0, 0, 0, 0, 0x01, 0x00, 0x04, b'f', b'r', b'a', b'g', 0, 0x01, b't', 0, 0x02,
        0x03, 0xfc, 0x01, 0x04, 0x02, 0x01, 0x01, 0x00, 0x02, 0x01, 0x3f, 0x04, 0x05, 0x02, b'i',
        b'd', 0x01, b'b', 0x08, 0x01, 0x00,
    ];

    #[test]
    fn parses_a_captured_table_map() {
        let m = TableMap::parse(FRAG_T, 8).unwrap();
        assert_eq!(m.table_id, 18);
        assert_eq!((m.schema.as_str(), m.table.as_str()), ("frag", "t"));
        assert_eq!(m.columns.len(), 2);
        assert_eq!(m.columns[0].kind, ColumnType::Int);
        assert_eq!(m.columns[0].unsigned, Some(false));
        assert!(!m.columns[0].nullable);
        assert_eq!(
            m.columns[1].kind,
            ColumnType::Blob {
                pack_len: 4,
                compressed: false
            }
        );
        assert!(m.columns[1].nullable);
        assert!(m.columns[1].is_binary());
        assert_eq!(m.columns[1].name.as_deref(), Some("b"));
        assert_eq!(m.primary_key, vec![(0, 0)]);
        assert_eq!(m.column_index("b"), Some(1));
    }

    #[test]
    fn truncated_table_map_is_corrupt_not_a_panic() {
        // Every prefix that cuts the fixed part short (it ends at byte 23).
        for n in 0..23 {
            let r = TableMap::parse(&FRAG_T[..n], 8);
            assert!(r.is_err(), "prefix of {n} bytes parsed");
        }
    }

    #[test]
    fn long_char_lengths_fold_into_the_type_byte() {
        // CHAR(255) in utf8mb4 is 1020 bytes: 0xfe ^ ((1020 >> 4) & 0x30), 1020 & 0xff.
        let len: u16 = 1020;
        let b0 = 0xfe ^ u8::try_from((len >> 4) & 0x30).unwrap();
        let b1 = u8::try_from(len & 0xff).unwrap();
        let (_, kind) = read_column_meta(&mut Buf::new(&[b0, b1]), ct::STRING).unwrap();
        assert_eq!(kind, ColumnType::Char { max_len: 1020 });
        let (_, kind) = read_column_meta(&mut Buf::new(&[ct::ENUM, 2]), ct::STRING).unwrap();
        assert_eq!(kind, ColumnType::Enum { size: 2 });
    }
}
