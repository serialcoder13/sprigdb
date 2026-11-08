//! Simplified chapter 31 `_id` encoding: class byte (MongoDB order) + order-preserving body.
//! The spike encodes numbers as f64 (the real design uses exact decimal digits).

use bson::raw::RawBsonRef;

pub fn class_of(v: RawBsonRef<'_>) -> u8 {
    match v {
        RawBsonRef::MinKey => 10,
        RawBsonRef::Undefined => 15,
        RawBsonRef::Null => 20,
        RawBsonRef::Int32(_) | RawBsonRef::Int64(_) | RawBsonRef::Double(_) | RawBsonRef::Decimal128(_) => 30,
        RawBsonRef::String(_) | RawBsonRef::Symbol(_) => 40,
        RawBsonRef::Document(_) => 50,
        RawBsonRef::Array(_) => 60,
        RawBsonRef::Binary(_) => 70,
        RawBsonRef::ObjectId(_) => 80,
        RawBsonRef::Boolean(_) => 90,
        RawBsonRef::DateTime(_) => 100,
        RawBsonRef::Timestamp(_) => 110,
        RawBsonRef::RegularExpression(_) => 120,
        RawBsonRef::MaxKey => 250,
        _ => 140,
    }
}

fn f64_key(f: f64, out: &mut Vec<u8>) {
    let bits = f.to_bits();
    let k = if bits >> 63 == 1 { !bits } else { bits ^ (1 << 63) };
    out.extend_from_slice(&k.to_be_bytes());
}

pub fn encode_key(v: RawBsonRef<'_>) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    out.push(class_of(v));
    match v {
        RawBsonRef::Int32(i) => f64_key(i as f64, &mut out),
        RawBsonRef::Int64(i) => f64_key(i as f64, &mut out),
        RawBsonRef::Double(f) => f64_key(f, &mut out),
        RawBsonRef::String(s) | RawBsonRef::Symbol(s) => {
            for &b in s.as_bytes() {
                out.push(b);
                if b == 0 {
                    out.push(0xff);
                }
            }
        }
        RawBsonRef::ObjectId(o) => out.extend_from_slice(&o.bytes()),
        RawBsonRef::Boolean(b) => out.push(b as u8),
        RawBsonRef::DateTime(d) => out.extend_from_slice(&((d.timestamp_millis() as u64) ^ (1 << 63)).to_be_bytes()),
        RawBsonRef::Binary(b) => {
            out.extend_from_slice(&(b.bytes.len() as u32).to_be_bytes());
            out.push(b.subtype.into());
            out.extend_from_slice(b.bytes);
        }
        RawBsonRef::Document(d) => out.extend_from_slice(d.as_bytes()),
        RawBsonRef::Array(a) => out.extend_from_slice(a.as_bytes()),
        _ => {}
    }
    out
}
