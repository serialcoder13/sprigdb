//! Chapter 31 "JSON rendering and the type map": BSON -> (JSON text, `_types`) and back.

use anyhow::{anyhow, bail, Result};
use base64::Engine;
use bson::raw::{RawArrayBuf, RawBsonRef, RawDocument, RawDocumentBuf};
use bson::spec::BinarySubtype;
use bson::{oid::ObjectId, Binary, DateTime, Decimal128, RawBson, Timestamp};
use serde_json::Value;
use std::fmt::Write;

pub struct Stored {
    pub id_key: Vec<u8>,
    pub doc: String,
    pub types: Option<String>,
}

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

pub fn json_str(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn push_path(path: &mut String, key: &str) {
    if !path.is_empty() {
        path.push('.');
    }
    for c in key.chars() {
        if c == '.' || c == '\\' {
            path.push('\\');
        }
        path.push(c);
    }
}

// ---- dates: "YYYY-MM-DDTHH:MM:SS.mmmZ" (years 1000-9999), else integer ms ----

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let mp = if m > 2 { m - 3 } else { m + 9 } as u64;
    let doy = (153 * mp + 2) / 5 + d as u64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe as i64 - 719468
}

/// (iso text, "YYYY-MM-DD HH:MM:SS.mmm" for SQL) or None outside 1000-9999.
pub fn iso_date(ms: i64) -> Option<(String, String)> {
    let days = ms.div_euclid(86_400_000);
    let rem = ms.rem_euclid(86_400_000);
    let (y, mo, d) = civil_from_days(days);
    if !(1000..=9999).contains(&y) {
        return None;
    }
    let (h, mi, s, milli) = (rem / 3_600_000, rem / 60_000 % 60, rem / 1000 % 60, rem % 1000);
    Some((
        format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{milli:03}Z"),
        format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{milli:03}"),
    ))
}

fn parse_iso(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() != 24 || b[4] != b'-' || b[10] != b'T' || b[23] != b'Z' {
        return None;
    }
    let n = |r: std::ops::Range<usize>| s.get(r)?.parse::<i64>().ok();
    let days = days_from_civil(n(0..4)?, n(5..7)? as u32, n(8..10)? as u32);
    Some(days * 86_400_000 + n(11..13)? * 3_600_000 + n(14..16)? * 60_000 + n(17..19)? * 1000 + n(20..23)?)
}

fn uuid_text(b: &[u8]) -> String {
    let h = hex::encode(b);
    format!("{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32])
}

// ---- rendering ----

pub struct Renderer {
    pub out: String,
    path: String,
    types: Vec<(String, String)>,
}

impl Renderer {
    pub fn new() -> Self {
        Renderer { out: String::with_capacity(2048), path: String::new(), types: Vec::new() }
    }

    fn entry(&mut self, t: impl Into<String>) {
        self.types.push((self.path.clone(), t.into()));
    }

    pub fn value(&mut self, v: RawBsonRef<'_>) -> Result<()> {
        match v {
            RawBsonRef::Double(f) => {
                if f.is_finite() {
                    let _ = write!(self.out, "{f:?}"); // always has '.' or 'e'
                } else {
                    let s = if f.is_nan() { "NaN" } else if f > 0.0 { "Infinity" } else { "-Infinity" };
                    json_str(&mut self.out, s);
                    self.entry("double");
                }
            }
            RawBsonRef::String(s) => json_str(&mut self.out, s),
            RawBsonRef::Document(d) => self.object(d, false)?,
            RawBsonRef::Array(a) => {
                self.out.push('[');
                let base = self.path.len();
                for (i, e) in a.into_iter().enumerate() {
                    if i > 0 {
                        self.out.push(',');
                    }
                    push_path(&mut self.path, &i.to_string());
                    self.value(e?)?;
                    self.path.truncate(base);
                }
                self.out.push(']');
            }
            RawBsonRef::Boolean(b) => self.out.push_str(if b { "true" } else { "false" }),
            RawBsonRef::Null => self.out.push_str("null"),
            RawBsonRef::Int32(i) => {
                let _ = write!(self.out, "{i}");
            }
            RawBsonRef::Int64(i) => {
                let _ = write!(self.out, "{i}");
                if i32::try_from(i).is_ok() {
                    self.entry("long");
                }
            }
            RawBsonRef::Decimal128(d) => {
                let s = d.to_string();
                let numeric = s.bytes().all(|c| c.is_ascii_digit() || b".-+eE".contains(&c))
                    && s.bytes().any(|c| c.is_ascii_digit());
                if numeric {
                    self.out.push_str(&s);
                } else {
                    json_str(&mut self.out, &s);
                }
                self.entry("decimal");
            }
            RawBsonRef::ObjectId(o) => {
                json_str(&mut self.out, &o.to_hex());
                self.entry("objectId");
            }
            RawBsonRef::DateTime(dt) => {
                match iso_date(dt.timestamp_millis()) {
                    Some((iso, _)) => json_str(&mut self.out, &iso),
                    None => {
                        let _ = write!(self.out, "{}", dt.timestamp_millis());
                    }
                }
                self.entry("date");
            }
            RawBsonRef::Binary(b) => {
                let st: u8 = b.subtype.into();
                if st == 4 && b.bytes.len() == 16 {
                    json_str(&mut self.out, &uuid_text(b.bytes));
                    self.entry("uuid");
                } else {
                    self.out.push('"');
                    B64.encode_string(b.bytes, &mut self.out);
                    self.out.push('"');
                    self.entry(format!("binData:{st}"));
                }
            }
            RawBsonRef::Timestamp(t) => {
                json_str(&mut self.out, &format!("{:010}.{:010}", t.time, t.increment));
                self.entry("timestamp");
            }
            RawBsonRef::RegularExpression(r) => {
                json_str(&mut self.out, &format!("/{}/{}", r.pattern, r.options));
                self.entry("regex");
            }
            RawBsonRef::Symbol(s) => {
                json_str(&mut self.out, s);
                self.entry("symbol");
            }
            RawBsonRef::JavaScriptCode(s) => {
                json_str(&mut self.out, s);
                self.entry("javascript");
            }
            RawBsonRef::Undefined => {
                self.out.push_str("null");
                self.entry("undefined");
            }
            RawBsonRef::MinKey => {
                json_str(&mut self.out, "MinKey");
                self.entry("minKey");
            }
            RawBsonRef::MaxKey => {
                json_str(&mut self.out, "MaxKey");
                self.entry("maxKey");
            }
            other => bail!("spike: unsupported BSON type {:?}", other.element_type()),
        }
        Ok(())
    }

    fn object(&mut self, d: &RawDocument, id_first: bool) -> Result<()> {
        self.out.push('{');
        let mut first = true;
        let base = self.path.len();
        if id_first {
            if let Some(id) = d.get("_id")? {
                json_str(&mut self.out, "_id");
                self.out.push(':');
                push_path(&mut self.path, "_id");
                self.value(id)?;
                self.path.truncate(base);
                first = false;
            }
        }
        for e in d {
            let (k, v) = e?;
            if id_first && k == "_id" {
                continue;
            }
            if !first {
                self.out.push(',');
            }
            first = false;
            json_str(&mut self.out, k);
            self.out.push(':');
            push_path(&mut self.path, k);
            self.value(v)?;
            self.path.truncate(base);
        }
        self.out.push('}');
        Ok(())
    }

    pub fn types_json(&self) -> Option<String> {
        if self.types.is_empty() {
            return None;
        }
        let mut s = String::with_capacity(self.types.len() * 24);
        s.push('{');
        for (i, (p, t)) in self.types.iter().enumerate() {
            if i > 0 {
                s.push(',');
            }
            json_str(&mut s, p);
            s.push(':');
            json_str(&mut s, t);
        }
        s.push('}');
        Some(s)
    }

    /// Render a value alone (for $set): returns (json text, entries relative to `base_path`).
    pub fn fragment(v: RawBsonRef<'_>, base_path: &str) -> Result<(String, Vec<(String, String)>)> {
        let mut r = Renderer { out: String::new(), path: base_path.to_string(), types: Vec::new() };
        r.value(v)?;
        Ok((r.out, r.types))
    }
}

/// to_stored: `_id` first (generated when missing), the type map, the encoded key.
pub fn to_stored(d: &RawDocument) -> Result<Stored> {
    let mut r = Renderer::new();
    let id_key;
    match d.get("_id")? {
        Some(id) => {
            id_key = crate::keyenc::encode_key(id);
            r.object(d, true)?;
        }
        None => {
            let oid = ObjectId::new();
            id_key = crate::keyenc::encode_key(RawBsonRef::ObjectId(oid));
            let mut nd = RawDocumentBuf::new();
            nd.append("_id", oid);
            for e in d {
                let (k, v) = e?;
                nd.append_ref(k, v);
            }
            r.object(&nd, true)?;
        }
    }
    let types = r.types_json();
    Ok(Stored { id_key, doc: r.out, types })
}

// ---- back to BSON ----

struct Types<'a> {
    map: Option<&'a serde_json::Map<String, Value>>,
}

impl Types<'_> {
    fn get(&self, path: &str) -> Option<&str> {
        self.map.and_then(|m| m.get(path)).and_then(|v| v.as_str())
    }
}

fn conv(v: &Value, path: &mut String, t: &Types) -> Result<RawBson> {
    let entry = if t.map.is_some() { t.get(path) } else { None };
    Ok(match v {
        Value::Null => {
            if entry == Some("undefined") {
                RawBson::Undefined
            } else {
                RawBson::Null
            }
        }
        Value::Bool(b) => RawBson::Boolean(*b),
        Value::Number(n) => match entry {
            Some("long") => RawBson::Int64(n.as_i64().ok_or_else(|| anyhow!("long"))?),
            Some("decimal") => RawBson::Decimal128(n.to_string().parse::<Decimal128>()?),
            Some("date") => RawBson::DateTime(DateTime::from_millis(n.as_i64().unwrap_or(0))),
            _ => {
                if let Some(i) = n.as_i64() {
                    match i32::try_from(i) {
                        Ok(i) => RawBson::Int32(i),
                        Err(_) => RawBson::Int64(i),
                    }
                } else {
                    RawBson::Double(n.as_f64().unwrap_or(f64::NAN))
                }
            }
        },
        Value::String(s) => match entry {
            None => RawBson::String(s.clone()),
            Some(e) => match e {
                "objectId" => RawBson::ObjectId(ObjectId::parse_str(s)?),
                "date" => RawBson::DateTime(DateTime::from_millis(parse_iso(s).ok_or_else(|| anyhow!("date {s}"))?)),
                "uuid" => RawBson::Binary(Binary { subtype: BinarySubtype::Uuid, bytes: hex::decode(s.replace('-', ""))? }),
                "double" => RawBson::Double(match s.as_str() {
                    "NaN" => f64::NAN,
                    "Infinity" => f64::INFINITY,
                    _ => f64::NEG_INFINITY,
                }),
                "decimal" => RawBson::Decimal128(s.parse::<Decimal128>()?),
                "timestamp" => {
                    let (a, b) = s.split_once('.').unwrap_or((s, "0"));
                    RawBson::Timestamp(Timestamp { time: a.parse()?, increment: b.parse()? })
                }
                "symbol" => RawBson::Symbol(s.clone()),
                "javascript" => RawBson::JavaScriptCode(s.clone()),
                "minKey" => RawBson::MinKey,
                "maxKey" => RawBson::MaxKey,
                "regex" => {
                    let i = s.rfind('/').unwrap_or(0);
                    RawBson::RegularExpression(bson::Regex { pattern: s[1..i].to_string(), options: s[i + 1..].to_string() })
                }
                e if e.starts_with("binData:") => {
                    let st: u8 = e[8..].parse()?;
                    RawBson::Binary(Binary { subtype: BinarySubtype::from(st), bytes: B64.decode(s)? })
                }
                _ => RawBson::String(s.clone()),
            },
        },
        Value::Array(a) => {
            let mut arr = RawArrayBuf::new();
            let base = path.len();
            for (i, e) in a.iter().enumerate() {
                if t.map.is_some() {
                    push_path(path, &i.to_string());
                }
                arr.push(conv(e, path, t)?);
                path.truncate(base);
            }
            RawBson::Array(arr)
        }
        Value::Object(m) => RawBson::Document(conv_obj(m, path, t)?),
    })
}

fn conv_obj(m: &serde_json::Map<String, Value>, path: &mut String, t: &Types) -> Result<RawDocumentBuf> {
    let mut d = RawDocumentBuf::new();
    let base = path.len();
    for (k, v) in m {
        if t.map.is_some() {
            push_path(path, k);
        }
        let b = conv(v, path, t)?;
        d.append(k, b);
        path.truncate(base);
    }
    Ok(d)
}

pub fn to_bson(doc: &[u8], types: Option<&[u8]>) -> Result<RawDocumentBuf> {
    let v: serde_json::Map<String, Value> = serde_json::from_slice(doc)?;
    let tm: Option<serde_json::Map<String, Value>> = match types {
        Some(t) => Some(serde_json::from_slice(t)?),
        None => None,
    };
    let t = Types { map: tm.as_ref() };
    let mut path = String::new();
    conv_obj(&v, &mut path, &t)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::rawdoc;

    #[test]
    fn round_trip() {
        let d = rawdoc! {
            "a": 1i32, "b": 2i64, "c": 5.0f64, "d": "x\"y",
            "e": ObjectId::parse_str("6721c5a4e1b2c3d4e5f60718").unwrap(),
            "f": DateTime::from_millis(1_793_000_000_123),
            "g": Binary { subtype: BinarySubtype::Generic, bytes: vec![1, 2, 3, 250] },
            "h": { "x": [1i32, 2i64, "s", { "y": 1.5f64 }] },
            "i": "59.90".parse::<Decimal128>().unwrap(),
            "_id": "user1",
        };
        let s = to_stored(&d).unwrap();
        assert!(s.doc.starts_with("{\"_id\":\"user1\""), "{}", s.doc);
        let back = to_bson(s.doc.as_bytes(), s.types.as_deref().map(str::as_bytes)).unwrap();
        // _id moved first; compare as Documents ignoring order
        let a: bson::Document = d.to_document().unwrap();
        let b: bson::Document = back.to_document().unwrap();
        for (k, v) in a.iter() {
            assert_eq!(Some(v), b.get(k), "{k}: {}", s.doc);
        }
        assert_eq!(a.len(), b.len());
        println!("{}\n{:?}", s.doc, s.types);
    }
}

#[cfg(test)]
mod bench {
    use super::*;
    use bson::rawdoc;
    use std::time::Instant;

    fn time(label: &str, d: &RawDocumentBuf) {
        let n = 200_000;
        let t = Instant::now();
        let mut s = None;
        for _ in 0..n {
            s = Some(to_stored(d).unwrap());
        }
        let render = t.elapsed().as_nanos() as f64 / n as f64;
        let s = s.unwrap();
        let t = Instant::now();
        for _ in 0..n {
            std::hint::black_box(to_bson(s.doc.as_bytes(), s.types.as_deref().map(str::as_bytes)).unwrap());
        }
        let back = t.elapsed().as_nanos() as f64 / n as f64;
        let fast_doc = crate::fastconv::to_bson_fast(s.doc.as_bytes(), s.types.as_deref().map(str::as_bytes)).unwrap();
        assert_eq!(fast_doc.to_document().unwrap(), to_bson(s.doc.as_bytes(), s.types.as_deref().map(str::as_bytes)).unwrap().to_document().unwrap());
        let t = Instant::now();
        for _ in 0..n {
            std::hint::black_box(crate::fastconv::to_bson_fast(s.doc.as_bytes(), s.types.as_deref().map(str::as_bytes)).unwrap());
        }
        let fast = t.elapsed().as_nanos() as f64 / n as f64;
        println!(
            "{label}: BSON {} B, JSON {} B, _types {} B; to_stored {:.2} us, to_bson {:.2} us, streaming to_bson {:.2} us",
            d.as_bytes().len(),
            s.doc.len(),
            s.types.as_ref().map_or(0, |t| t.len()),
            render / 1000.0,
            back / 1000.0,
            fast / 1000.0
        );
    }

    #[test]
    #[ignore]
    fn conversion_cost() {
        let mut y = RawDocumentBuf::new();
        y.append("_id", "user6284781860667377211");
        for i in 0..10 {
            let bytes: Vec<u8> = (0..100).map(|j| 32 + ((i * 7 + j * 13) % 94) as u8).collect();
            y.append(format!("field{i}"), Binary { subtype: BinarySubtype::Generic, bytes });
        }
        time("YCSB record (10 x 100-byte binData)", &y);
        let o = rawdoc! {
            "_id": ObjectId::parse_str("6721c5a4e1b2c3d4e5f60718").unwrap(), "status": "s3",
            "customer": {"id": 4242i32}, "qty": 7i64, "total": "59.90".parse::<Decimal128>().unwrap(),
            "created": DateTime::from_millis(1_767_225_600_000),
        };
        time("orders document", &o);
        let mut p = RawDocumentBuf::new();
        p.append("_id", "user6284781860667377211");
        for i in 0..10 {
            p.append(format!("field{i}"), "x".repeat(100));
        }
        time("YCSB-shaped, strings instead of binData (no _types)", &p);
    }
}
