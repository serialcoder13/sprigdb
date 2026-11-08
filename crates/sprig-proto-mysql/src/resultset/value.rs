//! The binary protocol's value encoding, used by binary resultset rows and
//! by `COM_STMT_EXECUTE` and `COM_STMT_BULK_EXECUTE` parameters.

use bytes::Bytes;

use super::column_type as ct;
use super::{ColumnDef, PARAM_UNSIGNED};
use crate::buf::{Reader, Writer};
use crate::proto_error::{ProtoError, ProtoResult};

/// A date or date-time (`DATE`, `DATETIME`, `TIMESTAMP`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DateTime {
    /// Year.
    pub year: u16,
    /// Month, 1-12 (0 in zero dates).
    pub month: u8,
    /// Day, 1-31 (0 in zero dates).
    pub day: u8,
    /// Hour.
    pub hour: u8,
    /// Minute.
    pub minute: u8,
    /// Second.
    pub second: u8,
    /// Microseconds.
    pub micro: u32,
}

/// A `TIME` value, which may be negative and exceed 24 hours.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Time {
    /// Negative interval.
    pub negative: bool,
    /// Whole days.
    pub days: u32,
    /// Hours, 0-23.
    pub hour: u8,
    /// Minutes.
    pub minute: u8,
    /// Seconds.
    pub second: u8,
    /// Microseconds.
    pub micro: u32,
}

/// A value in the binary protocol.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// SQL NULL; carried in the NULL bitmap, not in the value bytes.
    Null,
    /// A signed integer of any width.
    Int(i64),
    /// An unsigned integer of any width.
    UInt(u64),
    /// `FLOAT`.
    Float(f32),
    /// `DOUBLE`.
    Double(f64),
    /// Every length-encoded type: strings, `DECIMAL` text, BLOBs, `JSON`,
    /// `BIT`, `ENUM`, `SET`, `GEOMETRY`.
    Bytes(Bytes),
    /// `DATE`, `DATETIME`, `TIMESTAMP`.
    DateTime(DateTime),
    /// `TIME`.
    Time(Time),
}

impl Value {
    /// The 2-byte parameter type a client sends for this value in
    /// `COM_STMT_EXECUTE` (type code, plus [`PARAM_UNSIGNED`]).
    #[must_use]
    pub fn param_type(&self) -> u16 {
        match self {
            Value::Null => u16::from(ct::NULL),
            Value::Int(_) => u16::from(ct::LONGLONG),
            Value::UInt(_) => u16::from(ct::LONGLONG) | PARAM_UNSIGNED,
            Value::Float(_) => u16::from(ct::FLOAT),
            Value::Double(_) => u16::from(ct::DOUBLE),
            Value::Bytes(_) => u16::from(ct::VAR_STRING),
            Value::DateTime(_) => u16::from(ct::DATETIME),
            Value::Time(_) => u16::from(ct::TIME),
        }
    }

    /// Whether this is NULL.
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

/// Decodes one non-NULL value of type `col_type`. Owned bytes are cut from
/// `src`, which must contain the reader's slice.
#[allow(clippy::cast_possible_wrap)] // reinterpreting two's complement is the encoding
pub fn decode_binary_value(
    r: &mut Reader<'_>,
    src: &Bytes,
    col_type: u8,
    unsigned: bool,
) -> ProtoResult<Value> {
    let int = |v: u64, signed: i64| {
        if unsigned {
            Value::UInt(v)
        } else {
            Value::Int(signed)
        }
    };
    Ok(match col_type {
        ct::NULL => Value::Null,
        ct::TINY => {
            let v = r.u8()?;
            int(u64::from(v), i64::from(v as i8))
        }
        ct::SHORT | ct::YEAR => {
            let v = r.u16()?;
            int(u64::from(v), i64::from(v as i16))
        }
        ct::LONG | ct::INT24 => {
            let v = r.u32()?;
            int(u64::from(v), i64::from(v as i32))
        }
        ct::LONGLONG => {
            let v = r.u64()?;
            int(v, v as i64)
        }
        ct::FLOAT => Value::Float(f32::from_bits(r.u32()?)),
        ct::DOUBLE => Value::Double(f64::from_bits(r.u64()?)),
        ct::DATE | ct::DATETIME | ct::TIMESTAMP | ct::DATETIME2 | ct::TIMESTAMP2 | ct::NEWDATE => {
            let len = r.u8()?;
            let mut d = DateTime::default();
            if !matches!(len, 0 | 4 | 7 | 11) {
                return Err(ProtoError::malformed(format!("date length {len}")));
            }
            if len >= 4 {
                d.year = r.u16()?;
                d.month = r.u8()?;
                d.day = r.u8()?;
            }
            if len >= 7 {
                d.hour = r.u8()?;
                d.minute = r.u8()?;
                d.second = r.u8()?;
            }
            if len == 11 {
                d.micro = r.u32()?;
            }
            Value::DateTime(d)
        }
        ct::TIME | ct::TIME2 => {
            let len = r.u8()?;
            let mut t = Time::default();
            if !matches!(len, 0 | 8 | 12) {
                return Err(ProtoError::malformed(format!("time length {len}")));
            }
            if len >= 8 {
                t.negative = r.u8()? != 0;
                t.days = r.u32()?;
                t.hour = r.u8()?;
                t.minute = r.u8()?;
                t.second = r.u8()?;
            }
            if len == 12 {
                t.micro = r.u32()?;
            }
            Value::Time(t)
        }
        _ => Value::Bytes(src.slice_ref(r.lenenc_bytes()?)),
    })
}

fn mismatch(value: &Value, col_type: u8) -> ProtoError {
    ProtoError::malformed(format!("can't encode {value:?} as column type {col_type}"))
}

/// Encodes one value as type `col_type`. NULL writes nothing (it lives in
/// the NULL bitmap). Integers are written at the type's width, truncated;
/// numbers given for a length-encoded type are written as text.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::too_many_lines
)] // fixed-width encoding, one arm per type
pub fn encode_binary_value(w: &mut Writer, value: &Value, col_type: u8) -> ProtoResult<()> {
    let as_u64 = |v: &Value| match *v {
        Value::Int(i) => Some(i as u64),
        Value::UInt(u) => Some(u),
        _ => None,
    };
    if value.is_null() {
        return Ok(());
    }
    match col_type {
        ct::NULL => {}
        ct::TINY => {
            w.u8(as_u64(value).ok_or_else(|| mismatch(value, col_type))? as u8);
        }
        ct::SHORT | ct::YEAR => {
            w.u16(as_u64(value).ok_or_else(|| mismatch(value, col_type))? as u16);
        }
        ct::LONG | ct::INT24 => {
            w.u32(as_u64(value).ok_or_else(|| mismatch(value, col_type))? as u32);
        }
        ct::LONGLONG => {
            w.u64(as_u64(value).ok_or_else(|| mismatch(value, col_type))?);
        }
        ct::FLOAT => match *value {
            Value::Float(f) => {
                w.u32(f.to_bits());
            }
            Value::Double(d) => {
                w.u32((d as f32).to_bits());
            }
            _ => return Err(mismatch(value, col_type)),
        },
        ct::DOUBLE => match *value {
            Value::Double(d) => {
                w.u64(d.to_bits());
            }
            Value::Float(f) => {
                w.u64(f64::from(f).to_bits());
            }
            _ => return Err(mismatch(value, col_type)),
        },
        ct::DATE | ct::DATETIME | ct::TIMESTAMP | ct::DATETIME2 | ct::TIMESTAMP2 | ct::NEWDATE => {
            let Value::DateTime(d) = value else {
                return Err(mismatch(value, col_type));
            };
            let len = if d.micro != 0 {
                11
            } else if d.hour != 0 || d.minute != 0 || d.second != 0 {
                7
            } else if d.year != 0 || d.month != 0 || d.day != 0 {
                4
            } else {
                0
            };
            w.u8(len);
            if len >= 4 {
                w.u16(d.year).u8(d.month).u8(d.day);
            }
            if len >= 7 {
                w.u8(d.hour).u8(d.minute).u8(d.second);
            }
            if len == 11 {
                w.u32(d.micro);
            }
        }
        ct::TIME | ct::TIME2 => {
            let Value::Time(t) = value else {
                return Err(mismatch(value, col_type));
            };
            let len = if t.micro != 0 {
                12
            } else if *t != Time::default() {
                8
            } else {
                0
            };
            w.u8(len);
            if len >= 8 {
                w.u8(u8::from(t.negative))
                    .u32(t.days)
                    .u8(t.hour)
                    .u8(t.minute)
                    .u8(t.second);
            }
            if len == 12 {
                w.u32(t.micro);
            }
        }
        _ => match value {
            Value::Bytes(b) => {
                w.lenenc_bytes(b);
            }
            Value::Int(i) => {
                w.lenenc_bytes(i.to_string().as_bytes());
            }
            Value::UInt(u) => {
                w.lenenc_bytes(u.to_string().as_bytes());
            }
            Value::Float(f) => {
                w.lenenc_bytes(f.to_string().as_bytes());
            }
            Value::Double(d) => {
                w.lenenc_bytes(d.to_string().as_bytes());
            }
            _ => return Err(mismatch(value, col_type)),
        },
    }
    Ok(())
}

/// Bytes in a NULL bitmap for `n` values with this bit offset (2 for
/// binary rows, 0 for execute parameters).
#[must_use]
pub fn null_bitmap_len(n: usize, offset: usize) -> usize {
    (n + offset).div_ceil(8)
}

/// Whether value `i` is NULL in `bitmap`; out-of-range bits read as set.
#[must_use]
pub fn null_bitmap_get(bitmap: &[u8], i: usize, offset: usize) -> bool {
    let bit = i + offset;
    bitmap
        .get(bit / 8)
        .is_none_or(|b| b & (1 << (bit % 8)) != 0)
}

/// Marks value `i` NULL in `bitmap`; out-of-range bits are ignored.
pub fn null_bitmap_set(bitmap: &mut [u8], i: usize, offset: usize) {
    let bit = i + offset;
    if let Some(b) = bitmap.get_mut(bit / 8) {
        *b |= 1 << (bit % 8);
    }
}

/// Decodes a binary row given each column's (type, unsigned).
pub fn decode_binary_row_types(payload: &Bytes, types: &[(u8, bool)]) -> ProtoResult<Vec<Value>> {
    let mut r = Reader::new(payload);
    if r.u8()? != 0 {
        return Err(ProtoError::malformed("binary row header"));
    }
    let bitmap = r.take(null_bitmap_len(types.len(), 2))?;
    let mut out = Vec::with_capacity(types.len());
    for (i, &(t, unsigned)) in types.iter().enumerate() {
        if null_bitmap_get(bitmap, i, 2) {
            out.push(Value::Null);
        } else {
            out.push(decode_binary_value(&mut r, payload, t, unsigned)?);
        }
    }
    if !r.is_empty() {
        return Err(ProtoError::malformed(format!(
            "binary row has {} trailing bytes",
            r.remaining()
        )));
    }
    Ok(out)
}

/// Decodes a binary row for these columns.
pub fn decode_binary_row(payload: &Bytes, columns: &[ColumnDef]) -> ProtoResult<Vec<Value>> {
    let types: Vec<(u8, bool)> = columns
        .iter()
        .map(|c| (c.col_type, c.is_unsigned()))
        .collect();
    decode_binary_row_types(payload, &types)
}

/// Encodes a binary row given each column's type code.
pub fn encode_binary_row_types(values: &[Value], types: &[u8]) -> ProtoResult<Bytes> {
    if values.len() != types.len() {
        return Err(ProtoError::malformed(format!(
            "{} values for {} columns",
            values.len(),
            types.len()
        )));
    }
    let mut bitmap = vec![0u8; null_bitmap_len(values.len(), 2)];
    for (i, v) in values.iter().enumerate() {
        if v.is_null() {
            null_bitmap_set(&mut bitmap, i, 2);
        }
    }
    let mut w = Writer::new();
    w.u8(0).bytes(&bitmap);
    for (v, &t) in values.iter().zip(types) {
        encode_binary_value(&mut w, v, t)?;
    }
    Ok(w.into_bytes())
}

/// Encodes a binary row for these columns.
pub fn encode_binary_row(values: &[Value], columns: &[ColumnDef]) -> ProtoResult<Bytes> {
    let types: Vec<u8> = columns.iter().map(|c| c.col_type).collect();
    encode_binary_row_types(values, &types)
}
