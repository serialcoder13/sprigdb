//! The AMQP 1.0 type system (OASIS AMQP 1.0, part 1): primitive types with
//! their fixed, variable, compound and array encodings, and described
//! types.
//!
//! Decoding accepts every encoding the spec defines; encoding picks the
//! smallest (`uint0`, `smalluint`, `str8`, `list0`, `list8`, ...), as
//! brokers and clients do.

use bytes::{BufMut, Bytes, BytesMut};

/// A decoding error (`amqp:decode-error`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct DecodeError(pub String);

/// Decoding result.
pub type DResult<T> = Result<T, DecodeError>;

fn err<T>(s: impl Into<String>) -> DResult<T> {
    Err(DecodeError(s.into()))
}

/// Compound values nest at most this deep.
pub const MAX_DEPTH: usize = 64;

/// An AMQP 1.0 value.
#[derive(Debug, Clone, PartialEq)]
#[allow(missing_docs)]
pub enum Value {
    Null,
    Bool(bool),
    Ubyte(u8),
    Ushort(u16),
    Uint(u32),
    Ulong(u64),
    Byte(i8),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Decimal32([u8; 4]),
    Decimal64([u8; 8]),
    Decimal128([u8; 16]),
    Char(char),
    /// Milliseconds since the epoch.
    Timestamp(i64),
    Uuid([u8; 16]),
    Binary(Bytes),
    String(String),
    Symbol(String),
    List(Vec<Value>),
    /// Ordered pairs.
    Map(Vec<(Value, Value)>),
    /// Elements of one type.
    Array(Vec<Value>),
    /// Descriptor and value.
    Described(Box<Value>, Box<Value>),
}

impl Value {
    /// A symbol.
    #[must_use]
    pub fn sym(s: &str) -> Self {
        Value::Symbol(s.to_owned())
    }

    /// A string.
    #[must_use]
    pub fn str(s: &str) -> Self {
        Value::String(s.to_owned())
    }

    /// A described list (a composite type) with descriptor `code`;
    /// trailing nulls are left out, as the spec allows.
    #[must_use]
    pub fn composite(code: u64, mut fields: Vec<Value>) -> Self {
        while matches!(fields.last(), Some(Value::Null)) {
            fields.pop();
        }
        Value::Described(Box::new(Value::Ulong(code)), Box::new(Value::List(fields)))
    }

    /// The descriptor code of a described value: numeric, or the symbolic
    /// name of a type this crate knows.
    #[must_use]
    pub fn descriptor(&self) -> Option<u64> {
        match self {
            Value::Described(d, _) => match &**d {
                Value::Ulong(c) => Some(*c),
                Value::Symbol(s) => symbolic_descriptor(s),
                _ => None,
            },
            _ => None,
        }
    }

    /// The fields of a composite (described list).
    #[must_use]
    pub fn fields(&self) -> Option<&[Value]> {
        match self {
            Value::Described(_, v) => match &**v {
                Value::List(l) => Some(l),
                _ => None,
            },
            _ => None,
        }
    }

    /// Whether it is null.
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Text of a string or symbol.
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) | Value::Symbol(s) => Some(s),
            _ => None,
        }
    }

    /// An unsigned integer of any width.
    #[must_use]
    pub fn as_u64(&self) -> Option<u64> {
        match *self {
            Value::Ubyte(v) => Some(v.into()),
            Value::Ushort(v) => Some(v.into()),
            Value::Uint(v) => Some(v.into()),
            Value::Ulong(v) => Some(v),
            _ => None,
        }
    }

    /// A boolean.
    #[must_use]
    pub fn as_bool(&self) -> Option<bool> {
        match *self {
            Value::Bool(b) => Some(b),
            _ => None,
        }
    }

    /// Encodes the value.
    pub fn encode(&self, b: &mut BytesMut) {
        match self {
            Value::Null => b.put_u8(0x40),
            Value::Bool(true) => b.put_u8(0x41),
            Value::Bool(false) => b.put_u8(0x42),
            Value::Ubyte(v) => {
                b.put_u8(0x50);
                b.put_u8(*v);
            }
            Value::Ushort(v) => {
                b.put_u8(0x60);
                b.put_u16(*v);
            }
            Value::Uint(0) => b.put_u8(0x43),
            #[allow(clippy::cast_possible_truncation)]
            Value::Uint(v) if *v <= 255 => {
                b.put_u8(0x52);
                b.put_u8(*v as u8);
            }
            Value::Uint(v) => {
                b.put_u8(0x70);
                b.put_u32(*v);
            }
            Value::Ulong(0) => b.put_u8(0x44),
            #[allow(clippy::cast_possible_truncation)]
            Value::Ulong(v) if *v <= 255 => {
                b.put_u8(0x53);
                b.put_u8(*v as u8);
            }
            Value::Ulong(v) => {
                b.put_u8(0x80);
                b.put_u64(*v);
            }
            Value::Byte(v) => {
                b.put_u8(0x51);
                b.put_i8(*v);
            }
            Value::Short(v) => {
                b.put_u8(0x61);
                b.put_i16(*v);
            }
            Value::Int(v) => match i8::try_from(*v) {
                Ok(s) => {
                    b.put_u8(0x54);
                    b.put_i8(s);
                }
                Err(_) => {
                    b.put_u8(0x71);
                    b.put_i32(*v);
                }
            },
            Value::Long(v) => match i8::try_from(*v) {
                Ok(s) => {
                    b.put_u8(0x55);
                    b.put_i8(s);
                }
                Err(_) => {
                    b.put_u8(0x81);
                    b.put_i64(*v);
                }
            },
            Value::Float(v) => {
                b.put_u8(0x72);
                b.put_f32(*v);
            }
            Value::Double(v) => {
                b.put_u8(0x82);
                b.put_f64(*v);
            }
            Value::Decimal32(v) => {
                b.put_u8(0x74);
                b.put_slice(v);
            }
            Value::Decimal64(v) => {
                b.put_u8(0x84);
                b.put_slice(v);
            }
            Value::Decimal128(v) => {
                b.put_u8(0x94);
                b.put_slice(v);
            }
            Value::Char(c) => {
                b.put_u8(0x73);
                b.put_u32(*c as u32);
            }
            Value::Timestamp(v) => {
                b.put_u8(0x83);
                b.put_i64(*v);
            }
            Value::Uuid(v) => {
                b.put_u8(0x98);
                b.put_slice(v);
            }
            Value::Binary(v) => var(b, 0xa0, 0xb0, v),
            Value::String(v) => var(b, 0xa1, 0xb1, v.as_bytes()),
            Value::Symbol(v) => var(b, 0xa3, 0xb3, v.as_bytes()),
            Value::List(items) if items.is_empty() => b.put_u8(0x45),
            Value::List(items) => {
                let mut body = BytesMut::new();
                for v in items {
                    v.encode(&mut body);
                }
                compound(b, 0xc0, 0xd0, items.len(), &body);
            }
            Value::Map(pairs) => {
                let mut body = BytesMut::new();
                for (k, v) in pairs {
                    k.encode(&mut body);
                    v.encode(&mut body);
                }
                compound(b, 0xc1, 0xd1, pairs.len() * 2, &body);
            }
            Value::Array(items) => encode_array(b, items),
            Value::Described(d, v) => {
                b.put_u8(0x00);
                d.encode(b);
                v.encode(b);
            }
        }
    }

    /// The encoding as bytes.
    #[must_use]
    pub fn to_bytes(&self) -> Bytes {
        let mut b = BytesMut::new();
        self.encode(&mut b);
        b.freeze()
    }

    /// Decodes one value from the front of `buf`; returns it and the bytes
    /// it took.
    ///
    /// # Errors
    /// Malformed or truncated input.
    pub fn decode(buf: &[u8]) -> DResult<(Value, usize)> {
        let mut r = Reader { buf, pos: 0 };
        let v = r.value(0)?;
        Ok((v, r.pos))
    }

    /// Decodes a buffer that holds exactly one value.
    ///
    /// # Errors
    /// Malformed input or trailing bytes.
    pub fn decode_all(buf: &[u8]) -> DResult<Value> {
        let (v, n) = Value::decode(buf)?;
        if n != buf.len() {
            return err("trailing bytes after value");
        }
        Ok(v)
    }
}

#[allow(clippy::cast_possible_truncation)]
fn var(b: &mut BytesMut, small: u8, large: u8, data: &[u8]) {
    if data.len() <= 255 {
        b.put_u8(small);
        b.put_u8(data.len() as u8);
    } else {
        b.put_u8(large);
        b.put_u32(data.len() as u32);
    }
    b.put_slice(data);
}

#[allow(clippy::cast_possible_truncation)]
fn compound(b: &mut BytesMut, small: u8, large: u8, count: usize, body: &[u8]) {
    if body.len() < 255 && count <= 255 {
        b.put_u8(small);
        b.put_u8((body.len() + 1) as u8);
        b.put_u8(count as u8);
    } else {
        b.put_u8(large);
        b.put_u32((body.len() + 4) as u32);
        b.put_u32(count as u32);
    }
    b.put_slice(body);
}

/// The element constructor of an array: one format code for every element
/// (the wide forms, so every element fits), or a descriptor plus one.
fn array_constructor(first: &Value) -> Vec<u8> {
    let code = match first {
        Value::Null => 0x40,
        Value::Bool(_) => 0x56,
        Value::Ubyte(_) => 0x50,
        Value::Ushort(_) => 0x60,
        Value::Uint(_) => 0x70,
        Value::Ulong(_) => 0x80,
        Value::Byte(_) => 0x51,
        Value::Short(_) => 0x61,
        Value::Int(_) => 0x71,
        Value::Long(_) => 0x81,
        Value::Float(_) => 0x72,
        Value::Double(_) => 0x82,
        Value::Decimal32(_) => 0x74,
        Value::Decimal64(_) => 0x84,
        Value::Decimal128(_) => 0x94,
        Value::Char(_) => 0x73,
        Value::Timestamp(_) => 0x83,
        Value::Uuid(_) => 0x98,
        Value::Binary(_) => 0xb0,
        Value::String(_) => 0xb1,
        Value::Symbol(_) => 0xb3,
        Value::List(_) => 0xd0,
        Value::Map(_) => 0xd1,
        Value::Array(_) => 0xf0,
        Value::Described(d, inner) => {
            let mut out = vec![0x00];
            let mut db = BytesMut::new();
            d.encode(&mut db);
            out.extend_from_slice(&db);
            out.extend(array_constructor(inner));
            return out;
        }
    };
    vec![code]
}

/// Writes `v`'s data with the wide constructor `code` (no constructor
/// byte).
fn encode_element(b: &mut BytesMut, v: &Value) {
    match v {
        Value::Null => {}
        Value::Bool(x) => b.put_u8(u8::from(*x)),
        Value::Uint(x) => b.put_u32(*x),
        Value::Ulong(x) => b.put_u64(*x),
        Value::Int(x) => b.put_i32(*x),
        Value::Long(x) => b.put_i64(*x),
        Value::Binary(x) => {
            b.put_u32(u32::try_from(x.len()).unwrap_or(u32::MAX));
            b.put_slice(x);
        }
        Value::String(x) | Value::Symbol(x) => {
            b.put_u32(u32::try_from(x.len()).unwrap_or(u32::MAX));
            b.put_slice(x.as_bytes());
        }
        Value::List(items) => {
            let mut body = BytesMut::new();
            for i in items {
                i.encode(&mut body);
            }
            b.put_u32(u32::try_from(body.len() + 4).unwrap_or(u32::MAX));
            b.put_u32(u32::try_from(items.len()).unwrap_or(u32::MAX));
            b.put_slice(&body);
        }
        Value::Map(pairs) => {
            let mut body = BytesMut::new();
            for (k, x) in pairs {
                k.encode(&mut body);
                x.encode(&mut body);
            }
            b.put_u32(u32::try_from(body.len() + 4).unwrap_or(u32::MAX));
            b.put_u32(u32::try_from(pairs.len() * 2).unwrap_or(u32::MAX));
            b.put_slice(&body);
        }
        Value::Array(items) => {
            // The outer constructor says 0xf0: the wide form.
            let body = array_body(items);
            b.put_u32(u32::try_from(body.len() + 4).unwrap_or(u32::MAX));
            b.put_u32(u32::try_from(items.len()).unwrap_or(u32::MAX));
            b.put_slice(&body);
        }
        Value::Described(_, inner) => encode_element(b, inner),
        other => {
            // Fixed-width values: their normal encoding minus the code.
            let mut tmp = BytesMut::new();
            other.encode(&mut tmp);
            b.put_slice(&tmp[1..]);
        }
    }
}

/// An array's constructor and elements.
fn array_body(items: &[Value]) -> BytesMut {
    let ctor = items.first().map_or_else(|| vec![0x40], array_constructor);
    let mut body = BytesMut::new();
    body.put_slice(&ctor);
    for v in items {
        encode_element(&mut body, v);
    }
    body
}

#[allow(clippy::cast_possible_truncation)]
fn encode_array(b: &mut BytesMut, items: &[Value]) {
    let body = array_body(items);
    if body.len() < 255 && items.len() <= 255 {
        b.put_u8(0xe0);
        b.put_u8((body.len() + 1) as u8);
        b.put_u8(items.len() as u8);
    } else {
        b.put_u8(0xf0);
        b.put_u32((body.len() + 4) as u32);
        b.put_u32(items.len() as u32);
    }
    b.put_slice(&body);
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> DResult<&'a [u8]> {
        if self.buf.len() - self.pos < n {
            return err("truncated value");
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    fn u8(&mut self) -> DResult<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> DResult<u16> {
        let s = self.take(2)?;
        Ok(u16::from_be_bytes([s[0], s[1]]))
    }
    fn u32(&mut self) -> DResult<u32> {
        let s = self.take(4)?;
        Ok(u32::from_be_bytes([s[0], s[1], s[2], s[3]]))
    }
    fn u64(&mut self) -> DResult<u64> {
        let mut a = [0u8; 8];
        a.copy_from_slice(self.take(8)?);
        Ok(u64::from_be_bytes(a))
    }
    fn arr<const N: usize>(&mut self) -> DResult<[u8; N]> {
        let mut a = [0u8; N];
        a.copy_from_slice(self.take(N)?);
        Ok(a)
    }

    fn value(&mut self, depth: usize) -> DResult<Value> {
        if depth > MAX_DEPTH {
            return err("value nested too deeply");
        }
        let code = self.u8()?;
        if code == 0x00 {
            let d = self.value(depth + 1)?;
            let v = self.value(depth + 1)?;
            return Ok(Value::Described(Box::new(d), Box::new(v)));
        }
        self.with_code(code, depth)
    }

    /// The value whose constructor was `code`.
    #[allow(clippy::too_many_lines)]
    fn with_code(&mut self, code: u8, depth: usize) -> DResult<Value> {
        Ok(match code {
            0x40 => Value::Null,
            0x41 => Value::Bool(true),
            0x42 => Value::Bool(false),
            0x56 => match self.u8()? {
                0 => Value::Bool(false),
                1 => Value::Bool(true),
                _ => return err("invalid boolean"),
            },
            0x50 => Value::Ubyte(self.u8()?),
            0x60 => Value::Ushort(self.u16()?),
            0x70 => Value::Uint(self.u32()?),
            0x52 => Value::Uint(self.u8()?.into()),
            0x43 => Value::Uint(0),
            0x80 => Value::Ulong(self.u64()?),
            0x53 => Value::Ulong(self.u8()?.into()),
            0x44 => Value::Ulong(0),
            0x51 => Value::Byte(self.u8()?.cast_signed()),
            0x61 => Value::Short(self.u16()?.cast_signed()),
            0x71 => Value::Int(self.u32()?.cast_signed()),
            0x54 => Value::Int(self.u8()?.cast_signed().into()),
            0x81 => Value::Long(self.u64()?.cast_signed()),
            0x55 => Value::Long(self.u8()?.cast_signed().into()),
            0x72 => Value::Float(f32::from_bits(self.u32()?)),
            0x82 => Value::Double(f64::from_bits(self.u64()?)),
            0x74 => Value::Decimal32(self.arr()?),
            0x84 => Value::Decimal64(self.arr()?),
            0x94 => Value::Decimal128(self.arr()?),
            0x73 => Value::Char(
                char::from_u32(self.u32()?).ok_or_else(|| DecodeError("invalid char".into()))?,
            ),
            0x83 => Value::Timestamp(self.u64()?.cast_signed()),
            0x98 => Value::Uuid(self.arr()?),
            0xa0 | 0xb0 | 0xa1 | 0xb1 | 0xa3 | 0xb3 => {
                let n = if code & 0xf0 == 0xa0 {
                    self.u8()? as usize
                } else {
                    self.u32()? as usize
                };
                let data = self.take(n)?;
                match code & 0x0f {
                    0 => Value::Binary(Bytes::copy_from_slice(data)),
                    1 => Value::String(
                        String::from_utf8(data.to_vec())
                            .map_err(|_| DecodeError("string is not valid UTF-8".into()))?,
                    ),
                    _ => {
                        if !data.is_ascii() {
                            return err("symbol is not ASCII");
                        }
                        Value::Symbol(String::from_utf8_lossy(data).into_owned())
                    }
                }
            }
            0x45 => Value::List(Vec::new()),
            0xc0 | 0xd0 | 0xc1 | 0xd1 => {
                let (size, count) = if code & 0xf0 == 0xc0 {
                    (self.u8()? as usize, self.u8()? as usize)
                } else {
                    (self.u32()? as usize, self.u32()? as usize)
                };
                let width = if code & 0xf0 == 0xc0 { 1 } else { 4 };
                let body = self.take(size.checked_sub(width).ok_or_else(|| DecodeError("bad compound size".into()))?)?;
                // Every element takes at least one byte.
                if count > body.len() {
                    return err("compound count exceeds its size");
                }
                let mut inner = Reader { buf: body, pos: 0 };
                let mut items = Vec::with_capacity(count);
                for _ in 0..count {
                    items.push(inner.value(depth + 1)?);
                }
                if inner.pos != body.len() {
                    return err("compound size doesn't match its elements");
                }
                if code & 0x0f == 0 {
                    Value::List(items)
                } else {
                    if count % 2 != 0 {
                        return err("map with an odd number of elements");
                    }
                    let mut pairs = Vec::with_capacity(count / 2);
                    let mut it = items.into_iter();
                    while let (Some(k), Some(v)) = (it.next(), it.next()) {
                        pairs.push((k, v));
                    }
                    Value::Map(pairs)
                }
            }
            0xe0 | 0xf0 => {
                let (size, count) = if code == 0xe0 {
                    (self.u8()? as usize, self.u8()? as usize)
                } else {
                    (self.u32()? as usize, self.u32()? as usize)
                };
                let width = if code == 0xe0 { 1 } else { 4 };
                let body = self.take(size.checked_sub(width).ok_or_else(|| DecodeError("bad array size".into()))?)?;
                let mut inner = Reader { buf: body, pos: 0 };
                // The constructor: a code, or a descriptor and a code.
                let mut c = inner.u8()?;
                let mut descriptor = None;
                if c == 0x00 {
                    descriptor = Some(inner.value(depth + 1)?);
                    c = inner.u8()?;
                }
                if c == 0x00 {
                    return err("nested descriptor in array constructor");
                }
                // Zero-width elements (null, true, false, uint0, ...) are
                // the only ones that can outnumber the bytes.
                let zero_width = matches!(c, 0x40 | 0x41 | 0x42 | 0x43 | 0x44 | 0x45);
                if !zero_width && count > body.len() {
                    return err("array count exceeds its size");
                }
                if zero_width && count > 1 << 20 {
                    return err("array too long");
                }
                let mut items = Vec::with_capacity(count.min(4096));
                for _ in 0..count {
                    let v = inner.with_code(c, depth + 1)?;
                    items.push(match &descriptor {
                        Some(d) => Value::Described(Box::new(d.clone()), Box::new(v)),
                        None => v,
                    });
                }
                if inner.pos != body.len() {
                    return err("array size doesn't match its elements");
                }
                Value::Array(items)
            }
            other => return err(format!("unknown format code {other:#04x}")),
        })
    }
}

/// Descriptor codes of the types this crate knows, by symbolic name.
fn symbolic_descriptor(s: &str) -> Option<u64> {
    Some(match s {
        "amqp:open:list" => 0x10,
        "amqp:begin:list" => 0x11,
        "amqp:attach:list" => 0x12,
        "amqp:flow:list" => 0x13,
        "amqp:transfer:list" => 0x14,
        "amqp:disposition:list" => 0x15,
        "amqp:detach:list" => 0x16,
        "amqp:end:list" => 0x17,
        "amqp:close:list" => 0x18,
        "amqp:error:list" => 0x1d,
        "amqp:received:list" => 0x23,
        "amqp:accepted:list" => 0x24,
        "amqp:rejected:list" => 0x25,
        "amqp:released:list" => 0x26,
        "amqp:modified:list" => 0x27,
        "amqp:source:list" => 0x28,
        "amqp:target:list" => 0x29,
        "amqp:sasl-mechanisms:list" => 0x40,
        "amqp:sasl-init:list" => 0x41,
        "amqp:sasl-challenge:list" => 0x42,
        "amqp:sasl-response:list" => 0x43,
        "amqp:sasl-outcome:list" => 0x44,
        "amqp:header:list" => 0x70,
        "amqp:delivery-annotations:map" => 0x71,
        "amqp:message-annotations:map" => 0x72,
        "amqp:properties:list" => 0x73,
        "amqp:application-properties:map" => 0x74,
        "amqp:data:binary" => 0x75,
        "amqp:amqp-sequence:list" => 0x76,
        "amqp:amqp-value:*" => 0x77,
        "amqp:footer:map" => 0x78,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt(v: &Value) -> Bytes {
        let b = v.to_bytes();
        assert_eq!(&Value::decode_all(&b).unwrap(), v, "{b:02x?}");
        b
    }

    #[test]
    fn golden_primitives() {
        assert_eq!(&rt(&Value::Null)[..], &[0x40]);
        assert_eq!(&rt(&Value::Bool(true))[..], &[0x41]);
        assert_eq!(&rt(&Value::Uint(0))[..], &[0x43]);
        assert_eq!(&rt(&Value::Uint(7))[..], &[0x52, 7]);
        assert_eq!(&rt(&Value::Uint(65536))[..], &[0x70, 0, 1, 0, 0]);
        assert_eq!(&rt(&Value::Ulong(0x10))[..], &[0x53, 0x10]);
        assert_eq!(&rt(&Value::Int(-1))[..], &[0x54, 0xff]);
        assert_eq!(&rt(&Value::Long(1000))[..], &[0x81, 0, 0, 0, 0, 0, 0, 3, 0xe8]);
        assert_eq!(&rt(&Value::str("hi"))[..], &[0xa1, 2, b'h', b'i']);
        assert_eq!(&rt(&Value::sym("PLAIN"))[..], b"\xa3\x05PLAIN");
        assert_eq!(&rt(&Value::Binary(Bytes::from_static(&[1, 2])))[..], &[0xa0, 2, 1, 2]);
        assert_eq!(&rt(&Value::List(vec![]))[..], &[0x45]);
        assert_eq!(
            &rt(&Value::List(vec![Value::Uint(1), Value::Null]))[..],
            &[0xc0, 4, 2, 0x52, 1, 0x40]
        );
        assert_eq!(
            &rt(&Value::Map(vec![(Value::sym("k"), Value::Bool(false))]))[..],
            &[0xc1, 5, 2, 0xa3, 1, b'k', 0x42]
        );
        assert_eq!(
            &rt(&Value::Timestamp(1))[..],
            &[0x83, 0, 0, 0, 0, 0, 0, 0, 1]
        );
        rt(&Value::Uuid([9; 16]));
        rt(&Value::Char('é'));
        rt(&Value::Double(1.5));
        rt(&Value::Decimal64([1; 8]));
    }

    #[test]
    fn golden_symbol_array() {
        // sasl-server-mechanisms: an array of symbols, as brokers send it.
        let v = Value::Array(vec![Value::sym("PLAIN"), Value::sym("EXTERNAL")]);
        let b = rt(&v);
        let mut want = vec![0xe0, 23, 2, 0xb3, 0, 0, 0, 5];
        want.extend_from_slice(b"PLAIN");
        want.extend_from_slice(&[0, 0, 0, 8]);
        want.extend_from_slice(b"EXTERNAL");
        assert_eq!(&b[..], &want[..]);
        // The compact element code a client may use decodes too.
        let compact = [0xe0, 9, 2, 0xa3, 1, b'a', 2, b'b', b'c'];
        assert_eq!(
            Value::decode_all(&compact).unwrap(),
            Value::Array(vec![Value::sym("a"), Value::sym("bc")])
        );
    }

    #[test]
    fn golden_open_performative() {
        // open(container-id = "c") as described list 0x10.
        let v = Value::composite(0x10, vec![Value::str("c"), Value::Null]);
        assert_eq!(
            &rt(&v)[..],
            &[0x00, 0x53, 0x10, 0xc0, 4, 1, 0xa1, 1, b'c']
        );
        assert_eq!(v.descriptor(), Some(0x10));
        // A symbolic descriptor is understood.
        let s = Value::Described(
            Box::new(Value::sym("amqp:open:list")),
            Box::new(Value::List(vec![Value::str("c")])),
        );
        assert_eq!(s.descriptor(), Some(0x10));
    }

    #[test]
    fn large_and_nested() {
        let big = Value::List((0..300).map(Value::Uint).collect());
        let b = rt(&big);
        assert_eq!(b[0], 0xd0);
        rt(&Value::Array((0..300u64).map(Value::Ulong).collect()));
        rt(&Value::Array(vec![Value::List(vec![Value::Uint(1)]), Value::List(vec![])]));
        rt(&Value::Array(vec![Value::Array(vec![Value::Int(1)])]));
        rt(&Value::Array(vec![
            Value::Described(Box::new(Value::Ulong(0x24)), Box::new(Value::List(vec![]))),
        ]));
        rt(&Value::str(&"x".repeat(1000)));
    }

    #[test]
    fn malformed() {
        for bad in [
            &[0xa1, 5, b'a'][..],                // truncated string
            &[0xc0, 3, 9, 0x40, 0x40][..],       // count beyond size
            &[0xc1, 2, 1, 0x40][..],             // odd map
            &[0x56, 2][..],                      // bad boolean
            &[0xa1, 1, 0xff][..],                // bad UTF-8
            &[0x0f][..],                         // unknown code
            &[0xe0, 2, 1, 0x00][..],             // truncated descriptor
            &[0xc0, 0, 0][..],                   // size smaller than count width
            &[0xe0, 2, 0xff, 0x40][..],          // zero-width ok, 255 nulls
        ] {
            let r = Value::decode_all(bad);
            if bad == [0xe0, 2, 0xff, 0x40] {
                assert!(r.is_ok());
            } else {
                assert!(r.is_err(), "{bad:02x?}");
            }
        }
        let mut deep = vec![];
        for _ in 0..200 {
            deep.extend_from_slice(&[0x00, 0x53, 0x01]);
        }
        deep.push(0x40);
        assert!(Value::decode(&deep).is_err());
    }
}
