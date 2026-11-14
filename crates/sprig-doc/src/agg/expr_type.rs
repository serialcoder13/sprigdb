//! Type expressions: `$convert` and its shorthands (`$toInt`,
//! `$toLong`, `$toDouble`, `$toDecimal`, `$toBool`, `$toString`,
//! `$toDate`, `$toObjectId`, `$toUUID`), `$isNumber`, `$type`,
//! `$binarySize`, `$bsonSize`, `$tsIncrement`, `$tsSecond`; and from
//! MongoDB 8.1–8.3: `$convert`'s `base`, `byteOrder` and conversions to
//! and from arrays, objects and binData, `$toArray`, `$toObject`,
//! `$toString` of any value, `$subtype`, `$hash`, `$hexHash`,
//! `$serializeEJSON`, `$deserializeEJSON`, `$createObjectId` and
//! `$currentDate`.

use base64::Engine as _;
use bson::spec::BinarySubtype;
use bson::{Binary, Bson, DateTime, Document, oid::ObjectId};
use sprig_proto_mongo::errors::MongoError;

use super::expr_common::{
    dec_from_f64, dec_from_i64, dec_parse, dec_to_f64, err, fmt_double, is_nullish, is_num, one,
    show, tname, to_f64,
};
use super::expr_date::{date_ms, parse_date_string};
use super::{Vars, eval};

/// BSON type codes `$convert` accepts as targets.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Target {
    Double,
    String,
    Object,
    Array,
    BinData,
    ObjectId,
    Bool,
    Date,
    Int,
    Long,
    Decimal,
    /// A type `$convert` names but can't produce (`minKey`, `null` …).
    Other(&'static str),
}

fn target_by_name(s: &str) -> Option<Target> {
    Some(match s {
        "double" => Target::Double,
        "string" => Target::String,
        "object" => Target::Object,
        "array" => Target::Array,
        "binData" => Target::BinData,
        "objectId" => Target::ObjectId,
        "bool" => Target::Bool,
        "date" => Target::Date,
        "int" => Target::Int,
        "long" => Target::Long,
        "decimal" => Target::Decimal,
        "undefined" => Target::Other("undefined"),
        "null" => Target::Other("null"),
        "regex" => Target::Other("regex"),
        "dbPointer" => Target::Other("dbPointer"),
        "javascript" => Target::Other("javascript"),
        "symbol" => Target::Other("symbol"),
        "javascriptWithScope" => Target::Other("javascriptWithScope"),
        "timestamp" => Target::Other("timestamp"),
        "minKey" => Target::Other("minKey"),
        "maxKey" => Target::Other("maxKey"),
        _ => return None,
    })
}

fn target_by_code(c: i64) -> Option<Target> {
    Some(match c {
        1 => Target::Double,
        2 => Target::String,
        3 => Target::Object,
        4 => Target::Array,
        5 => Target::BinData,
        6 => Target::Other("undefined"),
        7 => Target::ObjectId,
        8 => Target::Bool,
        9 => Target::Date,
        10 => Target::Other("null"),
        11 => Target::Other("regex"),
        12 => Target::Other("dbPointer"),
        13 => Target::Other("javascript"),
        14 => Target::Other("symbol"),
        15 => Target::Other("javascriptWithScope"),
        16 => Target::Int,
        17 => Target::Other("timestamp"),
        18 => Target::Long,
        19 => Target::Decimal,
        -1 => Target::Other("minKey"),
        127 => Target::Other("maxKey"),
        _ => return None,
    })
}

fn target_name(t: Target) -> &'static str {
    match t {
        Target::Double => "double",
        Target::String => "string",
        Target::Object => "object",
        Target::Array => "array",
        Target::BinData => "binData",
        Target::ObjectId => "objectId",
        Target::Bool => "bool",
        Target::Date => "date",
        Target::Int => "int",
        Target::Long => "long",
        Target::Decimal => "decimal",
        Target::Other(n) => n,
    }
}

fn parse_target(v: &Bson) -> Result<Option<Target>, MongoError> {
    match v {
        Bson::String(s) => target_by_name(s)
            .map(Some)
            .ok_or_else(|| err(2, format!("Unknown type name: {s}"))),
        v if is_nullish(v) => Ok(None),
        v if is_num(v) => {
            let c = super::expr_common::integral_i64(v);
            c.and_then(target_by_code).map(Some).ok_or_else(|| {
                err(
                    9,
                    format!(
                        "In $convert, numeric value for 'to' does not correspond to a BSON type: {}",
                        super::expr_common::fmt_num6(v)
                    ),
                )
            })
        }
        v => Err(err(
            9,
            format!(
                "$convert's 'to' argument must be a string or number, but is {}",
                tname(v)
            ),
        )),
    }
}

/// The options of one conversion.
#[derive(Clone, Debug, Default)]
struct ConvOpts {
    subtype: Option<u8>,
    format: Option<String>,
    base: Option<u32>,
    big_endian: bool,
}

fn conv_fail(msg: impl Into<String>) -> MongoError {
    err(241, msg)
}

fn unsupported(v: &Bson, t: Target) -> MongoError {
    conv_fail(format!(
        "Unsupported conversion from {} to {} in $convert with no onError value",
        tname(v),
        target_name(t)
    ))
}

fn overflow(v: &Bson) -> MongoError {
    conv_fail(format!(
        "Conversion would overflow target type in $convert with no onError value: {}",
        super::expr_common::fmt_num6(v)
    ))
}

fn parse_fail(s: &str, why: &str) -> MongoError {
    conv_fail(format!(
        "Failed to parse number '{s}' in $convert with no onError value: {why}"
    ))
}

/// A string as an integer in `base`, strictly: an optional `-`, then
/// digits only.
fn parse_int_str(s: &str, base: u32) -> Result<i128, MongoError> {
    let (neg, digits) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    if digits.is_empty() {
        return Err(parse_fail(s, "No digits"));
    }
    if !digits.chars().all(|c| c.is_digit(base)) {
        return Err(parse_fail(s, "Did not consume whole string."));
    }
    let v = i128::from_str_radix(digits, base).map_err(|_| parse_fail(s, "Overflow"))?;
    Ok(if neg { -v } else { v })
}

fn parse_double_str(s: &str, base: Option<u32>) -> Result<f64, MongoError> {
    if let Some(b) = base.filter(|b| *b != 10) {
        #[allow(clippy::cast_precision_loss)]
        return parse_int_str(s, b).map(|i| i as f64);
    }
    let lower = s.to_ascii_lowercase();
    let ok_chars = !s.is_empty()
        && !s.starts_with(char::is_whitespace)
        && !s.ends_with(char::is_whitespace)
        && !lower.contains("0x");
    if !ok_chars
        || !s
            .chars()
            .any(|c| c.is_ascii_digit() || c.is_ascii_alphabetic())
    {
        return Err(parse_fail(s, "Did not consume any digits"));
    }
    let f: f64 = match lower.as_str() {
        "infinity" | "inf" | "+infinity" | "+inf" => f64::INFINITY,
        "-infinity" | "-inf" => f64::NEG_INFINITY,
        "nan" | "-nan" | "+nan" => f64::NAN,
        _ => {
            if !s
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_digit() || c == '-' || c == '+' || c == '.')
            {
                return Err(parse_fail(s, "Did not consume any digits"));
            }
            s.parse()
                .map_err(|_| parse_fail(s, "Did not consume whole string."))?
        }
    };
    if f.is_infinite() && !lower.contains("inf") {
        return Err(parse_fail(s, "Out of range"));
    }
    // MongoDB 8.0 refuses results that underflow (8.3 accepts subnormals).
    let nonzero_digits = s
        .split(['e', 'E'])
        .next()
        .is_some_and(|m| m.chars().any(|c| ('1'..='9').contains(&c)));
    if f.is_finite() && f.abs() < f64::MIN_POSITIVE && nonzero_digits {
        return Err(parse_fail(s, "Out of range"));
    }
    Ok(f)
}

fn int_from_f64(v: &Bson, f: f64, lo: f64, hi: f64) -> Result<i64, MongoError> {
    if f.is_nan() {
        return Err(conv_fail(
            "Attempt to convert NaN value to integer type in $convert with no onError value",
        ));
    }
    if f.is_infinite() {
        return Err(conv_fail(
            "Attempt to convert infinity value to integer type in $convert with no onError value",
        ));
    }
    let t = f.trunc();
    if f < lo || f >= hi {
        return Err(overflow(v));
    }
    #[allow(clippy::cast_possible_truncation)]
    Ok(t as i64)
}

/// Whole-number value of a decimal, truncated, if in range.
fn dec_trunc_i128(d: &bson::Decimal128) -> Option<i128> {
    let s = d.to_string();
    if s.contains("NaN") || s.contains("Inf") {
        return None;
    }
    // Plain or exponent notation: go through the exact digits.
    let (neg, body) = s
        .strip_prefix('-')
        .map_or((false, s.as_str()), |r| (true, r));
    let (mant, exp) = match body.find(['E', 'e']) {
        Some(i) => (&body[..i], body[i + 1..].parse::<i32>().ok()?),
        None => (body, 0),
    };
    let (ip, fp) = mant.split_once('.').unwrap_or((mant, ""));
    let digits = format!("{ip}{fp}");
    let scale = exp - i32::try_from(fp.len()).ok()?;
    let coef: i128 = digits.parse().ok()?;
    let v = if scale >= 0 {
        coef.checked_mul(10i128.checked_pow(u32::try_from(scale).ok()?)?)?
    } else {
        let p = u32::try_from(-scale).ok()?;
        if p > 38 { 0 } else { coef / 10i128.pow(p) }
    };
    Some(if neg { -v } else { v })
}

fn to_int64(v: &Bson, t: Target, o: &ConvOpts) -> Result<i64, MongoError> {
    // MongoDB checks the range before truncating: 2147483647.9 overflows.
    let (lo, hi) = if t == Target::Int {
        (f64::from(i32::MIN), 2_147_483_647.000_000_1)
    } else {
        (-9.223_372_036_854_776e18, 9.223_372_036_854_776e18)
    };
    let in_range = |i: i128| -> Result<i64, MongoError> {
        let ok = if t == Target::Int {
            i32::try_from(i).is_ok()
        } else {
            i64::try_from(i).is_ok()
        };
        if ok {
            Ok(i64::try_from(i).unwrap_or(0))
        } else {
            Err(overflow(v))
        }
    };
    match v {
        Bson::Boolean(b) => Ok(i64::from(*b)),
        Bson::Int32(i) => Ok(i64::from(*i)),
        Bson::Int64(i) => in_range(i128::from(*i)),
        Bson::Double(f) => int_from_f64(v, *f, lo, hi),
        Bson::Decimal128(d) => {
            let f = dec_to_f64(d);
            if f.is_nan() || f.is_infinite() {
                return int_from_f64(v, f, lo, hi);
            }
            dec_trunc_i128(d).map_or_else(|| Err(overflow(v)), in_range)
        }
        Bson::String(s) => {
            let base = o.base.unwrap_or(10);
            match parse_int_str(s, base) {
                Ok(i) => in_range(i).map_err(|_| parse_fail(s, "Overflow")),
                Err(e) => Err(e),
            }
        }
        Bson::DateTime(d) if t == Target::Long => Ok(d.timestamp_millis()),
        Bson::Binary(b) => bin_to_int(b, t, o.big_endian),
        _ => Err(unsupported(v, t)),
    }
}

fn bin_to_int(b: &Binary, t: Target, big: bool) -> Result<i64, MongoError> {
    let mut bytes = b.bytes.clone();
    let ok = match t {
        Target::Int => matches!(bytes.len(), 1 | 2 | 4),
        _ => matches!(bytes.len(), 1 | 2 | 4 | 8),
    };
    if !ok {
        return Err(conv_fail(format!(
            "Failed to convert BinData because of invalid length: {}",
            bytes.len()
        )));
    }
    if big {
        bytes.reverse();
    }
    // Little endian, two's complement, sign-extended.
    let n = bytes.len();
    let mut v: i64 = 0;
    for (i, x) in bytes.iter().enumerate() {
        v |= i64::from(*x) << (8 * i);
    }
    if n < 8 {
        let shift = 64 - 8 * n;
        v = (v << shift) >> shift;
    }
    Ok(v)
}

fn hex_upper(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02X}");
    }
    s
}

fn uuid_str(b: &[u8]) -> String {
    let h = hex_upper(b).to_ascii_lowercase();
    if h.len() != 32 {
        return h;
    }
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

fn bin_to_string(b: &Binary, format: Option<&str>) -> Result<String, MongoError> {
    let st = u8::from(b.subtype);
    let fmt = format.unwrap_or(if st == 4 { "uuid" } else { "base64" });
    Ok(match fmt {
        "base64" => base64::engine::general_purpose::STANDARD.encode(&b.bytes),
        "base64url" => base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&b.bytes),
        "hex" => hex_upper(&b.bytes),
        "utf8" => String::from_utf8(b.bytes.clone())
            .map_err(|_| conv_fail("Failed to convert binData to a UTF-8 string"))?,
        "uuid" => {
            if st != 4 || b.bytes.len() != 16 {
                return Err(conv_fail(
                    "Only UUID binData (subtype 4) can be converted with format 'uuid'",
                ));
            }
            uuid_str(&b.bytes)
        }
        other => {
            return Err(err(
                4_341_117,
                format!("Invalid format value for $convert: {other}"),
            ));
        }
    })
}

fn string_to_bin(s: &str, o: &ConvOpts) -> Result<Bson, MongoError> {
    let st = o.subtype.unwrap_or(0);
    let fmt = o.format.as_deref().unwrap_or("");
    let bytes = match fmt {
        "base64" => base64::engine::general_purpose::STANDARD
            .decode(s)
            .map_err(|_| conv_fail(format!("Failed to parse BinData '{s}' in $convert with no onError value: Input is not a valid base64 string.")))?,
        "base64url" => base64::engine::general_purpose::URL_SAFE
            .decode(s)
            .or_else(|_| base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(s))
            .map_err(|_| conv_fail(format!("Failed to parse BinData '{s}' in $convert with no onError value: Input is not a valid base64url string.")))?,
        "hex" => {
            if !s.len().is_multiple_of(2) || !s.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(conv_fail(format!("Failed to parse BinData '{s}' in $convert with no onError value: Input is not a valid hex string.")));
            }
            (0..s.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap_or(0))
                .collect()
        }
        "utf8" => s.as_bytes().to_vec(),
        "uuid" => {
            if st != 4 {
                return Err(conv_fail(
                    "Only the UUID subtype (4) is allowed with the 'uuid' format",
                ));
            }
            let h: String = s.chars().filter(|c| *c != '-').collect();
            let shape_ok = s.len() == 36
                && s.chars().enumerate().all(|(i, c)| {
                    if [8, 13, 18, 23].contains(&i) { c == '-' } else { c.is_ascii_hexdigit() }
                });
            if !shape_ok {
                return Err(conv_fail(format!("Failed to parse BinData '{s}' in $convert with no onError value: Input is not a valid UUID string.")));
            }
            (0..32)
                .step_by(2)
                .map(|i| u8::from_str_radix(&h[i..i + 2], 16).unwrap_or(0))
                .collect()
        }
        other => return Err(err(4_341_117, format!("Invalid format value for $convert: {other}"))),
    };
    Ok(Bson::Binary(Binary {
        subtype: BinarySubtype::from(st),
        bytes,
    }))
}

fn date_iso(ms: i64) -> String {
    super::expr_date::iso_utc(ms)
}

/// A value as `$toString` writes it (8.3: any type; objects and arrays
/// as compact relaxed Extended JSON).
fn any_to_string(v: &Bson, o: &ConvOpts) -> Result<String, MongoError> {
    Ok(match v {
        Bson::String(s) | Bson::Symbol(s) => s.clone(),
        Bson::Boolean(b) => b.to_string(),
        Bson::Int32(i) => i.to_string(),
        Bson::Int64(i) => i.to_string(),
        Bson::Double(f) => fmt_double(*f),
        Bson::Decimal128(d) => d.to_string(),
        Bson::DateTime(d) => date_iso(d.timestamp_millis()),
        Bson::ObjectId(oid) => oid.to_hex(),
        Bson::Binary(b) => bin_to_string(b, o.format.as_deref())?,
        Bson::RegularExpression(r) => format!("/{}/{}", r.pattern, r.options),
        Bson::Timestamp(t) => format!("Timestamp({}, {})", t.time, t.increment),
        Bson::MinKey => "MinKey".to_owned(),
        Bson::MaxKey => "MaxKey".to_owned(),
        Bson::Document(_) | Bson::Array(_) => {
            let mut s = String::new();
            write_json(&ejson(v, true), &mut s);
            s
        }
        Bson::JavaScriptCode(c) => c.clone(),
        other => return Err(unsupported(other, Target::String)),
    })
}

fn convert_value(v: &Bson, t: Target, o: &ConvOpts) -> Result<Bson, MongoError> {
    // `base` only changes string ↔ number conversions.
    let base = o.base.filter(|b| *b != 10);
    Ok(match t {
        Target::Double => match v {
            Bson::Boolean(b) => Bson::Double(f64::from(u8::from(*b))),
            Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) => Bson::Double(to_f64(v)),
            Bson::Decimal128(d) => {
                let f = dec_to_f64(d);
                if f.is_infinite() && !d.to_string().contains("Inf") {
                    return Err(overflow(v));
                }
                Bson::Double(f)
            }
            Bson::String(s) => Bson::Double(parse_double_str(s, base)?),
            Bson::DateTime(d) =>
            {
                #[allow(clippy::cast_precision_loss)]
                Bson::Double(d.timestamp_millis() as f64)
            }
            Bson::Binary(b) => {
                let mut bytes = b.bytes.clone();
                if o.big_endian {
                    bytes.reverse();
                }
                match bytes.len() {
                    4 => Bson::Double(f64::from(f32::from_le_bytes([
                        bytes[0], bytes[1], bytes[2], bytes[3],
                    ]))),
                    8 => Bson::Double(f64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]))),
                    n => {
                        return Err(conv_fail(format!(
                            "Failed to convert binData because of invalid length: {n}"
                        )));
                    }
                }
            }
            _ => return Err(unsupported(v, t)),
        },
        Target::Int =>
        {
            #[allow(clippy::cast_possible_truncation)]
            Bson::Int32(to_int64(v, t, o)? as i32)
        }
        Target::Long => Bson::Int64(to_int64(v, t, o)?),
        Target::Decimal => match v {
            Bson::Boolean(b) => Bson::Decimal128(dec_from_i64(i64::from(*b))),
            Bson::Int32(i) => Bson::Decimal128(dec_from_i64(i64::from(*i))),
            Bson::Int64(i) => Bson::Decimal128(dec_from_i64(*i)),
            Bson::Double(f) => Bson::Decimal128(dec_from_f64(*f)),
            Bson::Decimal128(_) => v.clone(),
            Bson::String(s) => {
                if let Some(b) = base {
                    Bson::Decimal128(dec_parse(&parse_int_str(s, b)?.to_string()))
                } else {
                    let ok = !s.is_empty()
                        && !s.starts_with(char::is_whitespace)
                        && !s.ends_with(char::is_whitespace)
                        && !s.to_ascii_lowercase().contains("0x");
                    let d = if ok {
                        s.parse::<bson::Decimal128>().ok()
                    } else {
                        None
                    };
                    Bson::Decimal128(
                        d.ok_or_else(|| parse_fail(s, "Failed to parse string to decimal"))?,
                    )
                }
            }
            Bson::DateTime(d) => Bson::Decimal128(dec_from_i64(d.timestamp_millis())),
            _ => return Err(unsupported(v, t)),
        },
        Target::Bool => Bson::Boolean(match v {
            Bson::Boolean(b) => *b,
            Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => {
                let f = to_f64(v);
                f != 0.0
            }
            _ => true,
        }),
        Target::String => match (v, base) {
            (x, Some(b)) if is_num(x) => {
                let i = super::expr_common::integral_i64(x).ok_or_else(|| {
                    conv_fail(format!(
                        "Base conversion is only supported for integral values, not {}",
                        show(x)
                    ))
                })?;
                let body = match b {
                    2 => format!("{:b}", i.unsigned_abs()),
                    8 => format!("{:o}", i.unsigned_abs()),
                    _ => format!("{:X}", i.unsigned_abs()),
                };
                Bson::String(if i < 0 { format!("-{body}") } else { body })
            }
            _ => Bson::String(any_to_string(v, o)?),
        },
        Target::ObjectId => match v {
            Bson::ObjectId(_) => v.clone(),
            Bson::String(s) => {
                if s.len() != 24 {
                    return Err(conv_fail(format!(
                        "Failed to parse objectId '{s}' in $convert with no onError value: Invalid string length for parsing to OID, expected 24 but found {}",
                        s.len()
                    )));
                }
                Bson::ObjectId(ObjectId::parse_str(s).map_err(|_| {
                    conv_fail(format!(
                        "Failed to parse objectId '{s}' in $convert with no onError value: Invalid character found in hex string"
                    ))
                })?)
            }
            _ => return Err(unsupported(v, t)),
        },
        Target::Date => match v {
            Bson::DateTime(_) => v.clone(),
            Bson::Int64(i) => Bson::DateTime(DateTime::from_millis(*i)),
            Bson::Double(f) => Bson::DateTime(DateTime::from_millis(int_from_f64(
                v,
                *f,
                -9.223_372_036_854_776e18,
                9.223_372_036_854_776e18,
            )?)),
            Bson::Decimal128(d) => {
                let f = dec_to_f64(d);
                if f.is_nan() || f.is_infinite() {
                    int_from_f64(v, f, 0.0, 0.0)?;
                }
                let i = dec_trunc_i128(d)
                    .and_then(|i| i64::try_from(i).ok())
                    .ok_or_else(|| overflow(v))?;
                Bson::DateTime(DateTime::from_millis(i))
            }
            Bson::String(s) => Bson::DateTime(DateTime::from_millis(parse_date_string(s)?)),
            Bson::ObjectId(_) | Bson::Timestamp(_) => {
                Bson::DateTime(DateTime::from_millis(date_ms(v).unwrap_or(0)))
            }
            _ => return Err(unsupported(v, t)),
        },
        Target::BinData => match v {
            Bson::Binary(b) => {
                let want = o.subtype.unwrap_or(0);
                if u8::from(b.subtype) != want {
                    return Err(conv_fail(format!(
                        "Conversions between different binData subtypes are not supported: {} to {want}",
                        u8::from(b.subtype)
                    )));
                }
                v.clone()
            }
            Bson::String(s) => string_to_bin(s, o)?,
            Bson::Int32(i) if o.format.is_none() => num_bin(&i.to_le_bytes(), o),
            Bson::Int64(i) if o.format.is_none() => num_bin(&i.to_le_bytes(), o),
            Bson::Double(f) if o.format.is_none() => num_bin(&f.to_le_bytes(), o),
            Bson::Document(d) => Bson::Binary(Binary {
                subtype: BinarySubtype::from(o.subtype.unwrap_or(0)),
                bytes: bson::to_vec(d).unwrap_or_default(),
            }),
            _ => return Err(unsupported(v, t)),
        },
        Target::Array => match v {
            Bson::String(s) => match parse_json_value(s)? {
                b @ Bson::Array(_) => b,
                _ => return Err(conv_fail("Input doesn't match expected type 'array'")),
            },
            _ => return Err(unsupported(v, t)),
        },
        Target::Object => match v {
            Bson::String(s) => match parse_json_value(s)? {
                b @ Bson::Document(_) => b,
                _ => return Err(conv_fail("Input doesn't match expected type 'object'")),
            },
            Bson::Document(_) => v.clone(),
            _ => return Err(unsupported(v, t)),
        },
        Target::Other(_) => return Err(unsupported(v, t)),
    })
}

fn num_bin(le: &[u8], o: &ConvOpts) -> Bson {
    let mut bytes = le.to_vec();
    if o.big_endian {
        bytes.reverse();
    }
    Bson::Binary(Binary {
        subtype: BinarySubtype::from(o.subtype.unwrap_or(0)),
        bytes,
    })
}

/// `$convert`, and the shorthands through it.
fn convert(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let Bson::Document(d) = arg else {
        return Err(err(
            9,
            format!(
                "$convert expects an object of named arguments but found: {}",
                tname(arg)
            ),
        ));
    };
    for k in d.keys() {
        if !matches!(
            k.as_str(),
            "input" | "to" | "onError" | "onNull" | "format" | "base" | "byteOrder"
        ) {
            return Err(err(9, format!("$convert found an unknown argument: {k}")));
        }
    }
    let Some(input) = d.get("input") else {
        return Err(err(9, "Missing 'input' parameter to $convert"));
    };
    let Some(to) = d.get("to") else {
        return Err(err(9, "Missing 'to' parameter to $convert"));
    };
    let mut o = ConvOpts::default();
    let target = match to {
        Bson::Document(t) => {
            let ty = eval(t.get("type").unwrap_or(&Bson::Null), vars)?;
            if let Some(st) = t.get("subtype") {
                let st = eval(st, vars)?;
                o.subtype =
                    super::expr_common::integral_i64(&st).and_then(|x| u8::try_from(x).ok());
                if o.subtype.is_none() && !is_nullish(&st) {
                    return Err(err(4_341_107, "Invalid 'subtype' argument for $convert"));
                }
            }
            parse_target(&ty)?
        }
        e => parse_target(&eval(e, vars)?)?,
    };
    if let Some(f) = d.get("format") {
        match eval(f, vars)? {
            Bson::String(s) => {
                if !matches!(s.as_str(), "base64" | "base64url" | "utf8" | "hex" | "uuid") {
                    return Err(err(
                        4_341_125,
                        format!("Invalid 'format' argument for $convert: {s}"),
                    ));
                }
                o.format = Some(s);
            }
            v if is_nullish(&v) => {}
            v => {
                return Err(err(
                    4_341_114,
                    format!(
                        "$convert's 'format' argument must be a string, but is {}",
                        tname(&v)
                    ),
                ));
            }
        }
    }
    if let Some(b) = d.get("byteOrder") {
        match eval(b, vars)? {
            Bson::String(s) if s == "big" => o.big_endian = true,
            Bson::String(s) if s == "little" => {}
            v if is_nullish(&v) => {}
            v => {
                return Err(err(
                    9,
                    format!("Invalid 'byteOrder' argument for $convert: {}", show(&v)),
                ));
            }
        }
    }
    if let Some(b) = d.get("base") {
        match eval(b, vars)? {
            v if is_nullish(&v) => {}
            v => match super::expr_common::integral_i64(&v) {
                Some(n @ (2 | 8 | 10 | 16)) => o.base = u32::try_from(n).ok(),
                _ => {
                    return Err(err(
                        9,
                        format!(
                            "Invalid 'base' argument for $convert: {}; must be 2, 8, 10 or 16",
                            show(&v)
                        ),
                    ));
                }
            },
        }
    }
    let Some(target) = target else {
        return Ok(Bson::Null);
    };
    let v = eval(input, vars)?;
    if is_nullish(&v) {
        return match d.get("onNull") {
            Some(e) => eval(e, vars),
            None => Ok(Bson::Null),
        };
    }
    // binData and strings need a format to cross (not caught by onError).
    if o.format.is_none()
        && ((matches!(v, Bson::Binary(_)) && target == Target::String)
            || (matches!(v, Bson::String(_)) && target == Target::BinData))
    {
        return Err(err(
            4_341_115,
            format!(
                "Format must be speficied when converting from '{}' to '{}'",
                tname(&v),
                target_name(target)
            ),
        ));
    }
    match convert_value(&v, target, &o) {
        Ok(r) => Ok(r),
        Err(e) if e.code == 241 => match d.get("onError") {
            Some(on) => eval(on, vars),
            None => Err(e),
        },
        Err(e) => Err(e),
    }
}

fn shorthand(op: &str, arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    if let Bson::Array(a) = arg
        && a.len() != 1
    {
        return Err(err(
            50723,
            format!("{op} requires a single argument, got {}", a.len()),
        ));
    }
    let e = match arg {
        Bson::Array(a) => a[0].clone(),
        x => x.clone(),
    };
    let v = eval(&e, vars)?;
    if is_nullish(&v) {
        return Ok(Bson::Null);
    }
    let mut o = ConvOpts::default();
    let t = match op {
        "$toInt" => Target::Int,
        "$toLong" => Target::Long,
        "$toDouble" => Target::Double,
        "$toDecimal" => Target::Decimal,
        "$toBool" => Target::Bool,
        "$toString" => Target::String,
        "$toDate" => Target::Date,
        "$toObjectId" => Target::ObjectId,
        "$toArray" => Target::Array,
        "$toObject" => Target::Object,
        _ => {
            // `$toUUID`.
            o.subtype = Some(4);
            o.format = Some("uuid".to_owned());
            Target::BinData
        }
    };
    if op == "$toUUID" && !matches!(v, Bson::String(_)) {
        return Err(unsupported(&v, t));
    }
    convert_value(&v, t, &o)
}

fn is_number(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    Ok(Bson::Boolean(is_num(&one("$isNumber", arg, vars)?)))
}

fn type_of(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let v = one("$type", arg, vars)?;
    Ok(Bson::String(tname(&v).to_owned()))
}

fn binary_size(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let v = one("$binarySize", arg, vars)?;
    let n = match &v {
        Bson::String(s) => s.len(),
        Bson::Binary(b) => b.bytes.len(),
        v if is_nullish(v) => return Ok(Bson::Null),
        v => {
            return Err(err(
                51276,
                format!(
                    "$binarySize requires a string or BinData argument, found: {}",
                    tname(v)
                ),
            ));
        }
    };
    Ok(Bson::Int32(i32::try_from(n).unwrap_or(i32::MAX)))
}

fn bson_size(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let v = one("$bsonSize", arg, vars)?;
    match &v {
        Bson::Document(d) => Ok(Bson::Int32(
            i32::try_from(bson::to_vec(d).map_or(0, |b| b.len())).unwrap_or(i32::MAX),
        )),
        v if is_nullish(v) => Ok(Bson::Null),
        v => Err(err(
            31393,
            format!("$bsonSize requires a document input, found: {}", tname(v)),
        )),
    }
}

fn ts_part(op: &str, arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let v = one(op, arg, vars)?;
    match &v {
        Bson::Timestamp(t) => Ok(Bson::Int64(i64::from(if op == "$tsSecond" {
            t.time
        } else {
            t.increment
        }))),
        v if is_nullish(v) => Ok(Bson::Null),
        v => Err(err(
            if op == "$tsSecond" {
                5_687_301
            } else {
                5_687_302
            },
            format!(" Argument to {op} must be a timestamp, but is {}", tname(v)),
        )),
    }
}

fn subtype(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let v = one("$subtype", arg, vars)?;
    match &v {
        Bson::Binary(b) => Ok(Bson::Int32(i32::from(u8::from(b.subtype)))),
        v if is_nullish(v) => Ok(Bson::Null),
        v => Err(err(
            14,
            format!(
                "$subtype requires a value with a subtype, found: {}",
                tname(v)
            ),
        )),
    }
}

fn hash(op: &str, arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    use sha2::Digest as _;
    let Bson::Document(d) = arg else {
        return Err(err(
            9,
            format!(
                "{op} requires an object as an argument, found: {}",
                tname(arg)
            ),
        ));
    };
    for k in d.keys() {
        if k != "input" && k != "algorithm" {
            return Err(err(9, format!("{op} found an unknown argument: {k}")));
        }
    }
    let (Some(input), Some(alg)) = (d.get("input"), d.get("algorithm")) else {
        return Err(err(
            9,
            format!("{op} requires 'input' and 'algorithm' to be specified"),
        ));
    };
    let alg = match eval(alg, vars)? {
        Bson::String(s) if matches!(s.as_str(), "md5" | "sha256" | "xxh64") => s,
        v => {
            return Err(err(
                9,
                format!(
                    "{op} requires 'algorithm' to be one of \"md5\", \"sha256\" or \"xxh64\", found: {}",
                    show(&v)
                ),
            ));
        }
    };
    let v = eval(input, vars)?;
    let bytes: Vec<u8> = match &v {
        Bson::String(s) => s.as_bytes().to_vec(),
        Bson::Binary(b) => b.bytes.clone(),
        v if is_nullish(v) => return Ok(Bson::Null),
        v => {
            return Err(err(
                14,
                format!(
                    "{op} requires 'input' to be a string or binData, found: {}",
                    tname(v)
                ),
            ));
        }
    };
    let digest: Vec<u8> = match alg.as_str() {
        "md5" => md5::Md5::digest(&bytes).to_vec(),
        "sha256" => sha2::Sha256::digest(&bytes).to_vec(),
        _ => twox_hash::XxHash64::oneshot(0, &bytes)
            .to_be_bytes()
            .to_vec(),
    };
    Ok(if op == "$hash" {
        Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: digest,
        })
    } else {
        Bson::String(hex_upper(&digest))
    })
}

fn empty_obj_only(op: &str, arg: &Bson) -> Result<(), MongoError> {
    match arg {
        Bson::Document(d) if d.is_empty() => Ok(()),
        _ => Err(err(
            9,
            format!("{op} requires an empty object as its argument, found: {arg}"),
        )),
    }
}

// --- Extended JSON -----------------------------------------------------------------

fn wrap(k: &str, v: Bson) -> Bson {
    let mut d = Document::new();
    d.insert(k, v);
    Bson::Document(d)
}

/// A value as Extended JSON, itself as BSON (strings, numbers and
/// documents with type wrappers).
fn ejson(v: &Bson, relaxed: bool) -> Bson {
    match v {
        Bson::Int32(i) if !relaxed => wrap("$numberInt", Bson::String(i.to_string())),
        Bson::Int64(i) if !relaxed => wrap("$numberLong", Bson::String(i.to_string())),
        Bson::Double(f) if !relaxed || !f.is_finite() => wrap(
            "$numberDouble",
            Bson::String(if f.is_nan() {
                "NaN".to_owned()
            } else if f.is_infinite() {
                if *f > 0.0 { "Infinity" } else { "-Infinity" }.to_owned()
            } else if f.fract() == 0.0 && f.abs() < 1e16 {
                format!("{f:.1}")
            } else {
                format!("{f:?}")
            }),
        ),
        Bson::Decimal128(d) => wrap("$numberDecimal", Bson::String(d.to_string())),
        Bson::ObjectId(o) => wrap("$oid", Bson::String(o.to_hex())),
        Bson::DateTime(d) => {
            let ms = d.timestamp_millis();
            if relaxed && (0..=253_402_300_799_999).contains(&ms) {
                wrap("$date", Bson::String(date_iso(ms)))
            } else {
                wrap("$date", wrap("$numberLong", Bson::String(ms.to_string())))
            }
        }
        Bson::Binary(b) => {
            let mut inner = Document::new();
            inner.insert(
                "base64",
                base64::engine::general_purpose::STANDARD.encode(&b.bytes),
            );
            inner.insert("subType", format!("{:02x}", u8::from(b.subtype)));
            wrap("$binary", Bson::Document(inner))
        }
        Bson::RegularExpression(r) => {
            let mut inner = Document::new();
            inner.insert("pattern", r.pattern.clone());
            inner.insert("options", r.options.clone());
            wrap("$regularExpression", Bson::Document(inner))
        }
        Bson::Timestamp(t) => {
            let mut inner = Document::new();
            inner.insert("t", Bson::Int64(i64::from(t.time)));
            inner.insert("i", Bson::Int64(i64::from(t.increment)));
            wrap("$timestamp", Bson::Document(inner))
        }
        Bson::MinKey => wrap("$minKey", Bson::Int32(1)),
        Bson::MaxKey => wrap("$maxKey", Bson::Int32(1)),
        Bson::Undefined => wrap("$undefined", Bson::Boolean(true)),
        Bson::Symbol(s) => wrap("$symbol", Bson::String(s.clone())),
        Bson::JavaScriptCode(c) => wrap("$code", Bson::String(c.clone())),
        Bson::Document(d) => Bson::Document(
            d.iter()
                .map(|(k, x)| (k.clone(), ejson(x, relaxed)))
                .collect(),
        ),
        Bson::Array(a) => Bson::Array(a.iter().map(|x| ejson(x, relaxed)).collect()),
        other => other.clone(),
    }
}

fn json_escape(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                use std::fmt::Write as _;
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Compact JSON of an Extended JSON value (from [`ejson`]).
fn write_json(v: &Bson, out: &mut String) {
    match v {
        Bson::Document(d) => {
            out.push('{');
            for (i, (k, x)) in d.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                json_escape(k, out);
                out.push(':');
                write_json(x, out);
            }
            out.push('}');
        }
        Bson::Array(a) => {
            out.push('[');
            for (i, x) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json(x, out);
            }
            out.push(']');
        }
        Bson::String(s) => json_escape(s, out),
        Bson::Int32(i) => out.push_str(&i.to_string()),
        Bson::Int64(i) => out.push_str(&i.to_string()),
        Bson::Double(f) => out.push_str(&fmt_double(*f)),
        Bson::Boolean(b) => out.push_str(if *b { "true" } else { "false" }),
        Bson::Null => out.push_str("null"),
        other => json_escape(&other.to_string(), out),
    }
}

/// Reverses [`ejson`]: type wrappers back to BSON values.
fn from_ejson(v: &Bson) -> Result<Bson, MongoError> {
    let bad = |m: &str| err(9, format!("Invalid Extended JSON: {m}"));
    match v {
        Bson::Document(d)
            if d.len() <= 2 && d.keys().next().is_some_and(|k| k.starts_with('$')) =>
        {
            let (k, x) = d.iter().next().ok_or_else(|| bad("empty"))?;
            let s = || x.as_str().ok_or_else(|| bad(k));
            Ok(match k.as_str() {
                "$numberInt" => Bson::Int32(s()?.parse().map_err(|_| bad(k))?),
                "$numberLong" => Bson::Int64(s()?.parse().map_err(|_| bad(k))?),
                "$numberDouble" => Bson::Double(match s()? {
                    "Infinity" => f64::INFINITY,
                    "-Infinity" => f64::NEG_INFINITY,
                    "NaN" => f64::NAN,
                    t => t.parse().map_err(|_| bad(k))?,
                }),
                "$numberDecimal" => Bson::Decimal128(s()?.parse().map_err(|_| bad(k))?),
                "$oid" => Bson::ObjectId(ObjectId::parse_str(s()?).map_err(|_| bad(k))?),
                "$date" => match x {
                    Bson::String(t) => Bson::DateTime(DateTime::from_millis(parse_date_string(t)?)),
                    Bson::Document(_) => match from_ejson(x)? {
                        Bson::Int64(i) => Bson::DateTime(DateTime::from_millis(i)),
                        _ => return Err(bad(k)),
                    },
                    Bson::Int32(i) => Bson::DateTime(DateTime::from_millis(i64::from(*i))),
                    Bson::Int64(i) => Bson::DateTime(DateTime::from_millis(*i)),
                    _ => return Err(bad(k)),
                },
                "$binary" => {
                    let inner = x.as_document().ok_or_else(|| bad(k))?;
                    let b64 = inner.get_str("base64").map_err(|_| bad(k))?;
                    let st = inner.get_str("subType").map_err(|_| bad(k))?;
                    Bson::Binary(Binary {
                        subtype: BinarySubtype::from(
                            u8::from_str_radix(st, 16).map_err(|_| bad(k))?,
                        ),
                        bytes: base64::engine::general_purpose::STANDARD
                            .decode(b64)
                            .map_err(|_| bad(k))?,
                    })
                }
                "$regularExpression" => {
                    let inner = x.as_document().ok_or_else(|| bad(k))?;
                    Bson::RegularExpression(bson::Regex {
                        pattern: inner.get_str("pattern").map_err(|_| bad(k))?.to_owned(),
                        options: inner.get_str("options").map_err(|_| bad(k))?.to_owned(),
                    })
                }
                "$timestamp" => {
                    let inner = x.as_document().ok_or_else(|| bad(k))?;
                    let n = |f: &str| {
                        inner
                            .get(f)
                            .and_then(super::expr_common::integral_i64)
                            .and_then(|v| u32::try_from(v).ok())
                            .ok_or_else(|| bad(k))
                    };
                    Bson::Timestamp(bson::Timestamp {
                        time: n("t")?,
                        increment: n("i")?,
                    })
                }
                "$minKey" => Bson::MinKey,
                "$maxKey" => Bson::MaxKey,
                "$undefined" => Bson::Undefined,
                "$symbol" => Bson::Symbol(s()?.to_owned()),
                "$code" => Bson::JavaScriptCode(s()?.to_owned()),
                _ => {
                    // Not a wrapper: an ordinary document.
                    return Ok(Bson::Document(
                        d.iter()
                            .map(|(k, x)| from_ejson(x).map(|y| (k.clone(), y)))
                            .collect::<Result<_, _>>()?,
                    ));
                }
            })
        }
        Bson::Document(d) => Ok(Bson::Document(
            d.iter()
                .map(|(k, x)| from_ejson(x).map(|y| (k.clone(), y)))
                .collect::<Result<_, _>>()?,
        )),
        Bson::Array(a) => Ok(Bson::Array(
            a.iter().map(from_ejson).collect::<Result<_, _>>()?,
        )),
        other => Ok(other.clone()),
    }
}

fn ejson_op(op: &str, arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let Bson::Document(d) = arg else {
        return Err(err(
            9,
            format!(
                "{op} requires an object as an argument, found: {}",
                tname(arg)
            ),
        ));
    };
    let allowed: &[&str] = if op == "$serializeEJSON" {
        &["input", "relaxed", "onError"]
    } else {
        &["input", "onError"]
    };
    for k in d.keys() {
        if !allowed.contains(&k.as_str()) {
            return Err(err(9, format!("{op} found an unknown argument: {k}")));
        }
    }
    let Some(input) = d.get("input") else {
        return Err(err(9, format!("{op} requires 'input' to be specified")));
    };
    let relaxed = match d.get("relaxed") {
        None => false,
        Some(e) => match eval(e, vars)? {
            Bson::Boolean(b) => b,
            v if is_nullish(&v) => false,
            v => {
                return Err(err(
                    9,
                    format!("{op} requires 'relaxed' to be a bool, found: {}", tname(&v)),
                ));
            }
        },
    };
    let v = eval(input, vars)?;
    if is_nullish(&v) {
        return Ok(Bson::Null);
    }
    let r = if op == "$serializeEJSON" {
        Ok(ejson(&v, relaxed))
    } else {
        from_ejson(&v)
    };
    match (r, d.get("onError")) {
        (Err(_), Some(on)) => eval(on, vars),
        (r, _) => r,
    }
}

// --- JSON text to BSON (string → array/object) ---------------------------------------

struct JsonParser<'a> {
    b: &'a [u8],
    i: usize,
}

fn json_err(why: &str) -> MongoError {
    conv_fail(format!("Input doesn't represent valid JSON: {why}"))
}

impl JsonParser<'_> {
    fn ws(&mut self) {
        while self.i < self.b.len() && matches!(self.b[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }

    fn value(&mut self) -> Result<Bson, MongoError> {
        self.ws();
        match self.b.get(self.i) {
            Some(b'{') => {
                self.i += 1;
                let mut d = Document::new();
                self.ws();
                if self.b.get(self.i) == Some(&b'}') {
                    self.i += 1;
                    return Ok(Bson::Document(d));
                }
                loop {
                    self.ws();
                    let k = self.string()?;
                    if k.contains('\0') {
                        return Err(json_err("Illegal embedded null byte"));
                    }
                    self.ws();
                    if self.b.get(self.i) != Some(&b':') {
                        return Err(json_err("Missing ':'"));
                    }
                    self.i += 1;
                    let v = self.value()?;
                    d.insert(k, v);
                    self.ws();
                    match self.b.get(self.i) {
                        Some(b',') => self.i += 1,
                        Some(b'}') => {
                            self.i += 1;
                            return Ok(Bson::Document(d));
                        }
                        _ => return Err(json_err("Missing ',' or '}'")),
                    }
                }
            }
            Some(b'[') => {
                self.i += 1;
                let mut a = Vec::new();
                self.ws();
                if self.b.get(self.i) == Some(&b']') {
                    self.i += 1;
                    return Ok(Bson::Array(a));
                }
                loop {
                    a.push(self.value()?);
                    self.ws();
                    match self.b.get(self.i) {
                        Some(b',') => self.i += 1,
                        Some(b']') => {
                            self.i += 1;
                            return Ok(Bson::Array(a));
                        }
                        _ => return Err(json_err("Missing ',' or ']'")),
                    }
                }
            }
            Some(b'"') => Ok(Bson::String(self.string()?)),
            Some(b't') if self.b[self.i..].starts_with(b"true") => {
                self.i += 4;
                Ok(Bson::Boolean(true))
            }
            Some(b'f') if self.b[self.i..].starts_with(b"false") => {
                self.i += 5;
                Ok(Bson::Boolean(false))
            }
            Some(b'n') if self.b[self.i..].starts_with(b"null") => {
                self.i += 4;
                Ok(Bson::Null)
            }
            Some(c) if c.is_ascii_digit() || *c == b'-' => {
                let start = self.i;
                self.i += 1;
                while self.i < self.b.len()
                    && matches!(
                        self.b[self.i],
                        b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-'
                    )
                {
                    self.i += 1;
                }
                let t = std::str::from_utf8(&self.b[start..self.i]).unwrap_or("");
                if let Ok(i) = t.parse::<i64>() {
                    return Ok(i32::try_from(i).map_or(Bson::Int64(i), Bson::Int32));
                }
                t.parse::<f64>()
                    .map(Bson::Double)
                    .map_err(|_| json_err("Invalid number"))
            }
            _ => Err(json_err("Invalid value")),
        }
    }

    fn string(&mut self) -> Result<String, MongoError> {
        if self.b.get(self.i) != Some(&b'"') {
            return Err(json_err("Missing a name for object member"));
        }
        let start = self.i;
        self.i += 1;
        let mut esc = false;
        while self.i < self.b.len() {
            let c = self.b[self.i];
            self.i += 1;
            if esc {
                esc = false;
            } else if c == b'\\' {
                esc = true;
            } else if c == b'"' {
                let raw = std::str::from_utf8(&self.b[start..self.i])
                    .map_err(|_| json_err("Invalid encoding in string"))?;
                return serde_json::from_str::<String>(raw)
                    .map_err(|_| json_err("Invalid escape in string"));
            }
        }
        Err(json_err("Missing a closing quotation mark in string"))
    }
}

fn parse_json_value(s: &str) -> Result<Bson, MongoError> {
    let mut p = JsonParser {
        b: s.as_bytes(),
        i: 0,
    };
    p.ws();
    if !matches!(p.b.get(p.i), Some(b'{' | b'[')) {
        return Err(json_err("Unexpected standalone value"));
    }
    let v = p.value()?;
    p.ws();
    if p.i != p.b.len() {
        return Err(json_err(
            "The document root must not be followed by other values",
        ));
    }
    Ok(v)
}

/// This module's operators.
pub(super) fn eval_op(op: &str, arg: &Bson, vars: &Vars) -> Option<Result<Bson, MongoError>> {
    Some(match op {
        "$convert" => convert(arg, vars),
        "$toInt" | "$toLong" | "$toDouble" | "$toDecimal" | "$toBool" | "$toString" | "$toDate"
        | "$toObjectId" | "$toUUID" | "$toArray" | "$toObject" => shorthand(op, arg, vars),
        "$isNumber" => is_number(arg, vars),
        "$type" => type_of(arg, vars),
        "$binarySize" => binary_size(arg, vars),
        "$bsonSize" => bson_size(arg, vars),
        "$tsIncrement" | "$tsSecond" => ts_part(op, arg, vars),
        "$subtype" => subtype(arg, vars),
        "$hash" | "$hexHash" => hash(op, arg, vars),
        "$serializeEJSON" | "$deserializeEJSON" => ejson_op(op, arg, vars),
        "$createObjectId" => empty_obj_only(op, arg).map(|()| Bson::ObjectId(ObjectId::new())),
        "$currentDate" => empty_obj_only(op, arg).map(|()| Bson::DateTime(DateTime::now())),
        _ => return None,
    })
}
