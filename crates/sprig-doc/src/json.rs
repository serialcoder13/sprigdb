//! JSON rendering and the type map (chapter 31, "JSON rendering and the
//! type map"; build step 2): [`to_stored`] renders a BSON document as JSON
//! text in field order, `_id` first, collecting `_types`; [`to_bson`]
//! walks the JSON text and its type map back to BSON, so MongoDB clients
//! get exactly the types they wrote.
//!
//! | BSON | JSON in `doc` | `_types` |
//! |---|---|---|
//! | string, object, array, bool, null, int32 | itself | none |
//! | int64 | integer | `long` when within int32's range |
//! | double | number with a fraction or exponent; NaN, ±∞ as strings | `double` for non-finite |
//! | Decimal128 | number with its exact digits; non-finite or beyond double range as a string | `decimal` |
//! | ObjectId | 24 lowercase hex | `objectId` |
//! | date | ISO text for years 1000–9999, else integer ms | `date` |
//! | binary | canonical UUID (subtype 4), else base64 | `uuid`, `binData:<subtype>` |
//! | Timestamp | `<10-digit seconds>.<10-digit increment>` | `timestamp` |
//! | regex, DBPointer | `/<pattern>/<flags>`, `<namespace>:<24 hex>` | `regex`, `dbPointer` |
//! | symbol, JavaScript, with scope | the string; `{"$code":..,"$scope":..}` | `symbol`, `javascript`, `javascriptWithScope` |
//! | undefined, MinKey, MaxKey | `null`, `"MinKey"`, `"MaxKey"` | `undefined`, `minKey`, `maxKey` |
//!
//! Numbers are parsed with their exact digits, never through a binary
//! double, and the parser streams the JSON text without building a tree.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::str::FromStr;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use bson::spec::BinarySubtype;
use bson::{Binary, Bson, DateTime, Decimal128, Document, Regex, Timestamp, oid::ObjectId};
use sprig_proto_mongo::errors::{MongoError, code};

use crate::keyenc;

/// MariaDB's JSON nesting limit as the chapter assumes it (MongoDB's is 100).
pub const MAX_DEPTH: usize = 32;

/// A document as it is stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    /// The encoded `_id` (the primary key).
    pub id_key: Vec<u8>,
    /// The JSON text.
    pub doc: String,
    /// The type map, `None` for plain JSON.
    pub types: Option<String>,
}

/// Rendering options.
#[derive(Debug, Clone, Copy)]
pub struct RenderOpts {
    /// `doc.type_map`: write `_types`.
    pub type_map: bool,
}

impl Default for RenderOpts {
    fn default() -> Self {
        Self { type_map: true }
    }
}

fn invalid(msg: impl Into<String>) -> MongoError {
    MongoError::new(code::BAD_VALUE, msg)
}

/// Escapes a field name for a `_types` path: `.` and `\` get a `\`.
#[must_use]
pub fn escape_name(name: &str) -> String {
    if !name.contains(['.', '\\']) {
        return name.to_owned();
    }
    let mut s = String::with_capacity(name.len() + 2);
    for c in name.chars() {
        if c == '.' || c == '\\' {
            s.push('\\');
        }
        s.push(c);
    }
    s
}

/// Writes `s` as a JSON string literal.
pub fn write_json_string(out: &mut String, s: &str) {
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

/// Days from 1970-01-01 to the civil date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// `2026-11-07T10:15:00.000Z` for years 1000–9999, else `None`.
#[must_use]
pub fn iso_from_ms(ms: i64) -> Option<String> {
    let days = ms.div_euclid(86_400_000);
    let rem = ms.rem_euclid(86_400_000);
    let (y, m, d) = civil_from_days(days);
    if !(1000..=9999).contains(&y) {
        return None;
    }
    let (h, rem) = (rem / 3_600_000, rem % 3_600_000);
    let (mi, rem) = (rem / 60_000, rem % 60_000);
    let (s, milli) = (rem / 1000, rem % 1000);
    Some(format!(
        "{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{milli:03}Z"
    ))
}

/// Milliseconds of an ISO date written by [`iso_from_ms`]; also accepts
/// `YYYY-MM-DD`, `YYYY-MM-DDTHH:MM:SSZ` and a fraction of any length.
#[must_use]
pub fn ms_from_iso(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    let num = |r: std::ops::Range<usize>| -> Option<i64> {
        let t = s.get(r)?;
        if t.bytes().all(|c| c.is_ascii_digit()) && !t.is_empty() {
            t.parse().ok()
        } else {
            None
        }
    };
    if b.len() < 10 || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let (y, mo, d) = (num(0..4)?, num(5..7)?, num(8..10)?);
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) {
        return None;
    }
    let mut ms = days_from_civil(y, mo, d) * 86_400_000;
    if b.len() == 10 {
        return Some(ms);
    }
    if b.len() < 19 || (b[10] != b'T' && b[10] != b' ') || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let (h, mi, sec) = (num(11..13)?, num(14..16)?, num(17..19)?);
    ms += h * 3_600_000 + mi * 60_000 + sec * 1000;
    let mut i = 19;
    if b.get(i) == Some(&b'.') {
        i += 1;
        let start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        let frac = &s[start..i];
        let mut f = 0i64;
        for (k, c) in frac.bytes().take(3).enumerate() {
            f += i64::from(c - b'0') * [100, 10, 1][k];
        }
        ms += f;
    }
    match &s[i..] {
        "Z" | "" | "+00:00" => Some(ms),
        _ => None,
    }
}

/// The canonical text of a UUID.
fn uuid_text(b: &[u8]) -> String {
    let h = hex_lower(b);
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

fn hex_lower(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s
}

fn hex_bytes(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

/// A double as JSON: always with a fraction or exponent, shortest
/// round-trip digits. `None` for non-finite values.
#[must_use]
pub fn double_text(f: f64) -> Option<String> {
    if !f.is_finite() {
        return None;
    }
    let s = format!("{f:?}");
    Some(if s.contains(['.', 'e', 'E']) {
        s
    } else {
        format!("{s}.0")
    })
}

fn nonfinite_text(f: f64) -> &'static str {
    if f.is_nan() {
        "NaN"
    } else if f > 0.0 {
        "Infinity"
    } else {
        "-Infinity"
    }
}

/// Whether a Decimal128's text is a plain JSON number within double range.
fn decimal_json_number(t: &str) -> bool {
    let Some((_, digits, exp)) = keyenc::decimal_parts(t) else {
        return false;
    };
    // Normalized: value = 0.digits × 10^exp; double range is about 1e308.
    digits.is_empty() || (-300..=300).contains(&exp)
}

/// Converts a DBPointer to `(namespace, id)` through extended JSON (the
/// bson crate keeps its fields private).
fn db_pointer_parts(v: &Bson) -> Option<(String, String)> {
    let j = v.clone().into_canonical_extjson();
    let p = j.get("$dbPointer")?;
    let ns = p.get("$ref")?.as_str()?.to_owned();
    let id = p.get("$id")?.get("$oid")?.as_str()?.to_owned();
    Some((ns, id))
}

fn db_pointer_from(ns: &str, id: &str) -> Option<Bson> {
    let j = serde_json::json!({"$dbPointer": {"$ref": ns, "$id": {"$oid": id}}});
    Bson::try_from(j).ok()
}

struct Renderer<'a> {
    out: String,
    types: Vec<(String, &'static str)>,
    dyn_types: Vec<(String, String)>,
    opts: &'a RenderOpts,
}

impl Renderer<'_> {
    fn ty(&mut self, path: &str, t: &'static str) {
        self.types.push((path.to_owned(), t));
    }

    fn value(&mut self, v: &Bson, path: &str, depth: usize) -> Result<(), MongoError> {
        if depth > MAX_DEPTH {
            return Err(MongoError::new(
                code::SPRIG_DOC_TOO_DEEP,
                format!("document nesting is deeper than {MAX_DEPTH} levels"),
            ));
        }
        match v {
            Bson::Null => self.out.push_str("null"),
            Bson::Boolean(b) => self.out.push_str(if *b { "true" } else { "false" }),
            Bson::Int32(i) => {
                let _ = write!(self.out, "{i}");
            }
            Bson::Int64(i) => {
                let _ = write!(self.out, "{i}");
                if i32::try_from(*i).is_ok() {
                    self.ty(path, "long");
                }
            }
            Bson::Double(f) => {
                if let Some(t) = double_text(*f) {
                    self.out.push_str(&t);
                } else {
                    write_json_string(&mut self.out, nonfinite_text(*f));
                    self.ty(path, "double");
                }
            }
            Bson::Decimal128(d) => {
                let t = d.to_string();
                if decimal_json_number(&t) {
                    self.out.push_str(&t);
                } else {
                    write_json_string(&mut self.out, &t);
                }
                self.ty(path, "decimal");
            }
            Bson::String(s) => write_json_string(&mut self.out, s),
            Bson::ObjectId(o) => {
                write_json_string(&mut self.out, &o.to_hex());
                self.ty(path, "objectId");
            }
            Bson::DateTime(d) => {
                let ms = d.timestamp_millis();
                match iso_from_ms(ms) {
                    Some(t) => write_json_string(&mut self.out, &t),
                    None => {
                        let _ = write!(self.out, "{ms}");
                    }
                }
                self.ty(path, "date");
            }
            Bson::Binary(b) => {
                let st = u8::from(b.subtype);
                if b.subtype == BinarySubtype::Uuid && b.bytes.len() == 16 {
                    write_json_string(&mut self.out, &uuid_text(&b.bytes));
                    self.ty(path, "uuid");
                } else {
                    write_json_string(&mut self.out, &B64.encode(&b.bytes));
                    self.dyn_types
                        .push((path.to_owned(), format!("binData:{st}")));
                }
            }
            Bson::Timestamp(t) => {
                write_json_string(
                    &mut self.out,
                    &format!("{:010}.{:010}", t.time, t.increment),
                );
                self.ty(path, "timestamp");
            }
            Bson::RegularExpression(r) => {
                write_json_string(&mut self.out, &format!("/{}/{}", r.pattern, r.options));
                self.ty(path, "regex");
            }
            Bson::DbPointer(_) => {
                let (ns, id) =
                    db_pointer_parts(v).ok_or_else(|| invalid("unreadable DBPointer"))?;
                write_json_string(&mut self.out, &format!("{ns}:{id}"));
                self.ty(path, "dbPointer");
            }
            Bson::Symbol(s) => {
                write_json_string(&mut self.out, s);
                self.ty(path, "symbol");
            }
            Bson::JavaScriptCode(s) => {
                write_json_string(&mut self.out, s);
                self.ty(path, "javascript");
            }
            Bson::JavaScriptCodeWithScope(c) => {
                self.out.push_str("{\"$code\":");
                write_json_string(&mut self.out, &c.code);
                self.out.push_str(",\"$scope\":");
                // The scope's own types aren't kept: it is rendered as plain
                // JSON (scopes are deprecated in MongoDB).
                let mut r = Renderer {
                    out: String::new(),
                    types: Vec::new(),
                    dyn_types: Vec::new(),
                    opts: self.opts,
                };
                r.document(&c.scope, "", depth + 1)?;
                self.out.push_str(&r.out);
                self.out.push('}');
                self.ty(path, "javascriptWithScope");
            }
            Bson::Undefined => {
                self.out.push_str("null");
                self.ty(path, "undefined");
            }
            Bson::MinKey => {
                self.out.push_str("\"MinKey\"");
                self.ty(path, "minKey");
            }
            Bson::MaxKey => {
                self.out.push_str("\"MaxKey\"");
                self.ty(path, "maxKey");
            }
            Bson::Document(d) => self.document(d, path, depth + 1)?,
            Bson::Array(a) => {
                self.out.push('[');
                for (i, e) in a.iter().enumerate() {
                    if i > 0 {
                        self.out.push(',');
                    }
                    let p = if path.is_empty() {
                        i.to_string()
                    } else {
                        format!("{path}.{i}")
                    };
                    self.value(e, &p, depth + 1)?;
                }
                self.out.push(']');
            }
        }
        Ok(())
    }

    fn document(&mut self, d: &Document, path: &str, depth: usize) -> Result<(), MongoError> {
        self.out.push('{');
        // `_id` first at the top level.
        let mut first = true;
        let id = if path.is_empty() && depth == 0 {
            d.get("_id")
        } else {
            None
        };
        let mut emit = |r: &mut Self, k: &str, v: &Bson| -> Result<(), MongoError> {
            if !first {
                r.out.push(',');
            }
            first = false;
            write_json_string(&mut r.out, k);
            r.out.push(':');
            let name = escape_name(k);
            let p = if path.is_empty() {
                name
            } else {
                format!("{path}.{name}")
            };
            r.value(v, &p, depth)
        };
        if let Some(v) = id {
            emit(self, "_id", v)?;
        }
        for (k, v) in d {
            if id.is_some() && k == "_id" {
                continue;
            }
            emit(self, k, v)?;
        }
        self.out.push('}');
        Ok(())
    }
}

/// Renders `d` as `(doc, _types)` without its key; `_id` first.
///
/// # Errors
/// 39665 past [`MAX_DEPTH`]; 2 for an unreadable DBPointer.
pub fn render(d: &Document, opts: &RenderOpts) -> Result<(String, Option<String>), MongoError> {
    let mut r = Renderer {
        out: String::with_capacity(256),
        types: Vec::new(),
        dyn_types: Vec::new(),
        opts,
    };
    r.document(d, "", 0)?;
    let types = if !opts.type_map || (r.types.is_empty() && r.dyn_types.is_empty()) {
        None
    } else {
        let mut t = String::from("{");
        let mut first = true;
        let mut all: Vec<(String, String)> = r
            .types
            .into_iter()
            .map(|(p, t)| (p, t.to_owned()))
            .chain(r.dyn_types)
            .collect();
        // Keep the document's order: entries were pushed in walk order,
        // except binData ones; a stable sort isn't needed for correctness.
        all.dedup();
        for (p, ty) in all {
            if !first {
                t.push(',');
            }
            first = false;
            write_json_string(&mut t, &p);
            t.push(':');
            write_json_string(&mut t, &ty);
        }
        t.push('}');
        Some(t)
    };
    Ok((r.out, types))
}

/// Renders a document for storage: key, JSON and type map.
///
/// # Errors
/// 53 `InvalidIdField` for an array, regex or undefined `_id`; 39660 for
/// an encoded `_id` over 1024 bytes; the errors of [`render`].
pub fn to_stored(d: &Document, opts: &RenderOpts) -> Result<Stored, MongoError> {
    let id = d.get("_id").ok_or_else(|| invalid("document has no _id"))?;
    let id_key = id_key(id)?;
    let (doc, types) = render(d, opts)?;
    Ok(Stored { id_key, doc, types })
}

/// The encoded `_id`.
///
/// # Errors
/// 53 for arrays, regexes and undefined; 39660 past 1024 bytes.
pub fn id_key(id: &Bson) -> Result<Vec<u8>, MongoError> {
    match id {
        Bson::Array(_) => {
            return Err(MongoError::new(
                code::INVALID_ID_FIELD,
                "The '_id' value cannot be of type array",
            ));
        }
        Bson::RegularExpression(_) => {
            return Err(MongoError::new(
                code::INVALID_ID_FIELD,
                "The '_id' value cannot be of type regex",
            ));
        }
        Bson::Undefined => {
            return Err(MongoError::new(
                code::INVALID_ID_FIELD,
                "The '_id' value cannot be undefined",
            ));
        }
        _ => {}
    }
    let mut k = Vec::with_capacity(16);
    keyenc::encode_key(id, &mut k);
    if k.len() > 1024 {
        return Err(MongoError::new(
            code::SPRIG_DOC_ID_TOO_LONG,
            "the encoded _id is longer than 1024 bytes",
        ));
    }
    Ok(k)
}

// --- JSON to BSON ----------------------------------------------------------

/// Parses a type map.
fn parse_types(t: &str) -> Result<HashMap<String, String>, MongoError> {
    let v: serde_json::Value =
        serde_json::from_str(t).map_err(|e| invalid(format!("_types is not JSON: {e}")))?;
    let serde_json::Value::Object(m) = v else {
        return Ok(HashMap::new());
    };
    Ok(m.into_iter()
        .filter_map(|(k, v)| v.as_str().map(|s| (k, s.to_owned())))
        .collect())
}

struct Parser<'a> {
    s: &'a [u8],
    at: usize,
    types: &'a HashMap<String, String>,
}

/// A scalar token.
enum Tok<'a> {
    Str(String),
    Num(&'a str),
    Bool(bool),
    Null,
}

impl Parser<'_> {
    fn err(&self, what: &str) -> MongoError {
        MongoError::new(
            code::BAD_VALUE,
            format!("stored JSON is invalid at byte {}: {what}", self.at),
        )
    }

    fn ws(&mut self) {
        while self.at < self.s.len() && matches!(self.s[self.at], b' ' | b'\n' | b'\r' | b'\t') {
            self.at += 1;
        }
    }

    fn peek(&mut self) -> Option<u8> {
        self.ws();
        self.s.get(self.at).copied()
    }

    fn expect(&mut self, c: u8) -> Result<(), MongoError> {
        if self.peek() == Some(c) {
            self.at += 1;
            Ok(())
        } else {
            Err(self.err(&format!("expected '{}'", c as char)))
        }
    }

    fn string(&mut self) -> Result<String, MongoError> {
        self.expect(b'"')?;
        let mut out: Vec<u8> = Vec::new();
        loop {
            let Some(&c) = self.s.get(self.at) else {
                return Err(self.err("unterminated string"));
            };
            self.at += 1;
            match c {
                b'"' => break,
                b'\\' => {
                    let Some(&e) = self.s.get(self.at) else {
                        return Err(self.err("bad escape"));
                    };
                    self.at += 1;
                    match e {
                        b'"' => out.push(b'"'),
                        b'\\' => out.push(b'\\'),
                        b'/' => out.push(b'/'),
                        b'b' => out.push(8),
                        b'f' => out.push(12),
                        b'n' => out.push(b'\n'),
                        b'r' => out.push(b'\r'),
                        b't' => out.push(b'\t'),
                        b'u' => {
                            let mut cp = self.hex4()?;
                            if (0xD800..0xDC00).contains(&cp)
                                && self.s.get(self.at) == Some(&b'\\')
                                && self.s.get(self.at + 1) == Some(&b'u')
                            {
                                self.at += 2;
                                let lo = self.hex4()?;
                                cp = 0x10000 + ((cp - 0xD800) << 10) + (lo.wrapping_sub(0xDC00));
                            }
                            let ch = char::from_u32(cp).unwrap_or('\u{FFFD}');
                            let mut b = [0u8; 4];
                            out.extend_from_slice(ch.encode_utf8(&mut b).as_bytes());
                        }
                        _ => return Err(self.err("bad escape")),
                    }
                }
                c => out.push(c),
            }
        }
        String::from_utf8(out).map_err(|_| self.err("string is not UTF-8"))
    }

    fn hex4(&mut self) -> Result<u32, MongoError> {
        let t = self
            .s
            .get(self.at..self.at + 4)
            .and_then(|b| std::str::from_utf8(b).ok())
            .and_then(|t| u32::from_str_radix(t, 16).ok())
            .ok_or_else(|| self.err("bad \\u escape"))?;
        self.at += 4;
        Ok(t)
    }

    fn number(&mut self) -> Result<&str, MongoError> {
        let start = self.at;
        while self.at < self.s.len()
            && matches!(
                self.s[self.at],
                b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9'
            )
        {
            self.at += 1;
        }
        std::str::from_utf8(&self.s[start..self.at]).map_err(|_| self.err("number"))
    }

    fn lit(&mut self, w: &str) -> Result<(), MongoError> {
        if self.s[self.at..].starts_with(w.as_bytes()) {
            self.at += w.len();
            Ok(())
        } else {
            Err(self.err("unknown literal"))
        }
    }

    fn value(&mut self, path: &mut String, depth: usize) -> Result<Bson, MongoError> {
        if depth > 200 {
            return Err(self.err("too deep"));
        }
        let entry = self.types.get(path.as_str()).map(String::as_str);
        match self.peek() {
            Some(b'{') => {
                if entry == Some("javascriptWithScope") {
                    let start = self.at;
                    let empty = HashMap::new();
                    let mut sub = Parser {
                        s: self.s,
                        at: start,
                        types: &empty,
                    };
                    let mut p = String::new();
                    let d = sub.document(&mut p, depth + 1)?;
                    self.at = sub.at;
                    if let (Ok(code), Ok(scope)) = (d.get_str("$code"), d.get_document("$scope")) {
                        return Ok(Bson::JavaScriptCodeWithScope(
                            bson::JavaScriptCodeWithScope {
                                code: code.to_owned(),
                                scope: scope.clone(),
                            },
                        ));
                    }
                    return Ok(Bson::Document(d));
                }
                Ok(Bson::Document(self.document(path, depth + 1)?))
            }
            Some(b'[') => {
                self.at += 1;
                let mut a = Vec::new();
                if self.peek() == Some(b']') {
                    self.at += 1;
                    return Ok(Bson::Array(a));
                }
                loop {
                    let len = path.len();
                    if !path.is_empty() {
                        path.push('.');
                    }
                    let _ = write!(path, "{}", a.len());
                    let v = self.value(path, depth + 1)?;
                    path.truncate(len);
                    a.push(v);
                    match self.peek() {
                        Some(b',') => self.at += 1,
                        Some(b']') => {
                            self.at += 1;
                            break;
                        }
                        _ => return Err(self.err("expected ',' or ']'")),
                    }
                }
                Ok(Bson::Array(a))
            }
            Some(b'"') => {
                let s = self.string()?;
                Ok(convert(Tok::Str(s), entry))
            }
            Some(b't') => {
                self.lit("true")?;
                Ok(convert(Tok::Bool(true), entry))
            }
            Some(b'f') => {
                self.lit("false")?;
                Ok(convert(Tok::Bool(false), entry))
            }
            Some(b'n') => {
                self.lit("null")?;
                Ok(convert(Tok::Null, entry))
            }
            Some(b'-' | b'0'..=b'9') => {
                let n = self.number()?;
                Ok(convert(Tok::Num(n), entry))
            }
            _ => Err(self.err("unexpected character")),
        }
    }

    fn document(&mut self, path: &mut String, depth: usize) -> Result<Document, MongoError> {
        self.expect(b'{')?;
        let mut d = Document::new();
        if self.peek() == Some(b'}') {
            self.at += 1;
            return Ok(d);
        }
        loop {
            let k = self.string()?;
            self.expect(b':')?;
            let len = path.len();
            if !path.is_empty() {
                path.push('.');
            }
            path.push_str(&escape_name(&k));
            let v = self.value(path, depth)?;
            path.truncate(len);
            d.insert(k, v);
            match self.peek() {
                Some(b',') => self.at += 1,
                Some(b'}') => {
                    self.at += 1;
                    break;
                }
                _ => return Err(self.err("expected ',' or '}'")),
            }
        }
        Ok(d)
    }
}

/// A number by the inference rule: int32, int64, else double.
fn infer_number(n: &str) -> Bson {
    if !n.contains(['.', 'e', 'E']) {
        if let Ok(i) = n.parse::<i32>() {
            return Bson::Int32(i);
        }
        if let Ok(i) = n.parse::<i64>() {
            return Bson::Int64(i);
        }
    }
    n.parse::<f64>().map_or(Bson::Null, Bson::Double)
}

fn infer(t: Tok<'_>) -> Bson {
    match t {
        Tok::Str(s) => Bson::String(s),
        Tok::Num(n) => infer_number(n),
        Tok::Bool(b) => Bson::Boolean(b),
        Tok::Null => Bson::Null,
    }
}

/// Converts a scalar as its type-map entry says; a value the entry
/// doesn't fit (SQL changed it) is inferred instead.
fn convert(t: Tok<'_>, entry: Option<&str>) -> Bson {
    let Some(entry) = entry else {
        return infer(t);
    };
    let typed = match (&t, entry) {
        (Tok::Num(n), "long") => n.parse::<i64>().ok().map(Bson::Int64),
        (Tok::Num(n), "decimal") => Decimal128::from_str(n).ok().map(Bson::Decimal128),
        (Tok::Str(n), "decimal") => Decimal128::from_str(n).ok().map(Bson::Decimal128),
        (Tok::Num(n), "double") => n.parse::<f64>().ok().map(Bson::Double),
        (Tok::Str(s), "double") => match s.as_str() {
            "NaN" => Some(Bson::Double(f64::NAN)),
            "Infinity" => Some(Bson::Double(f64::INFINITY)),
            "-Infinity" => Some(Bson::Double(f64::NEG_INFINITY)),
            _ => None,
        },
        (Tok::Str(s), "objectId") => ObjectId::parse_str(s).ok().map(Bson::ObjectId),
        (Tok::Str(s), "date") => ms_from_iso(s).map(|ms| Bson::DateTime(DateTime::from_millis(ms))),
        (Tok::Num(n), "date") => n
            .parse::<i64>()
            .ok()
            .map(|ms| Bson::DateTime(DateTime::from_millis(ms))),
        (Tok::Str(s), "uuid") => hex_bytes(&s.replace('-', ""))
            .filter(|b| b.len() == 16)
            .map(|bytes| {
                Bson::Binary(Binary {
                    subtype: BinarySubtype::Uuid,
                    bytes,
                })
            }),
        (Tok::Str(s), e) if e.starts_with("binData:") => {
            e["binData:".len()..].parse::<u8>().ok().and_then(|st| {
                B64.decode(s).ok().map(|bytes| {
                    Bson::Binary(Binary {
                        subtype: BinarySubtype::from(st),
                        bytes,
                    })
                })
            })
        }
        (Tok::Str(s), "timestamp") => s.split_once('.').and_then(|(a, b)| {
            Some(Bson::Timestamp(Timestamp {
                time: a.parse().ok()?,
                increment: b.parse().ok()?,
            }))
        }),
        (Tok::Str(s), "regex") => s
            .strip_prefix('/')
            .and_then(|rest| rest.rsplit_once('/'))
            .map(|(p, o)| {
                Bson::RegularExpression(Regex {
                    pattern: p.to_owned(),
                    options: o.to_owned(),
                })
            }),
        (Tok::Str(s), "dbPointer") => s
            .rsplit_once(':')
            .and_then(|(ns, id)| db_pointer_from(ns, id)),
        (Tok::Str(s), "symbol") => Some(Bson::Symbol(s.clone())),
        (Tok::Str(s), "javascript") => Some(Bson::JavaScriptCode(s.clone())),
        (Tok::Null, "undefined") => Some(Bson::Undefined),
        (Tok::Str(s), "minKey") if s == "MinKey" => Some(Bson::MinKey),
        (Tok::Str(s), "maxKey") if s == "MaxKey" => Some(Bson::MaxKey),
        _ => None,
    };
    typed.unwrap_or_else(|| infer(t))
}

/// Rebuilds the BSON document from stored JSON and its type map.
///
/// # Errors
/// 2 `BadValue` when the text isn't a JSON object.
pub fn to_bson(doc: &str, types: Option<&str>) -> Result<Document, MongoError> {
    let map = match types {
        Some(t) if !t.is_empty() => parse_types(t)?,
        _ => HashMap::new(),
    };
    let mut p = Parser {
        s: doc.as_bytes(),
        at: 0,
        types: &map,
    };
    let mut path = String::new();
    let d = p.document(&mut path, 0)?;
    p.ws();
    if p.at != p.s.len() {
        return Err(p.err("trailing characters"));
    }
    Ok(d)
}

/// Checks a raw incoming document: no duplicate field names at any
/// level, valid UTF-8, size within 16 MiB.
///
/// # Errors
/// 2 for duplicates or invalid UTF-8; 10334 past 16 MiB.
pub fn check_raw(d: &bson::RawDocument) -> Result<(), MongoError> {
    if d.as_bytes().len() > 16 * 1024 * 1024 {
        return Err(MongoError::new(
            code::BSON_OBJECT_TOO_LARGE,
            "object to insert too large",
        ));
    }
    check_raw_doc(d, 0)
}

fn check_raw_doc(d: &bson::RawDocument, depth: usize) -> Result<(), MongoError> {
    let mut seen = std::collections::HashSet::new();
    for e in d {
        let (k, v) = e.map_err(|e| invalid(format!("invalid BSON: {e}")))?;
        if !seen.insert(k) {
            return Err(invalid(format!("duplicate field name '{k}'")));
        }
        check_raw_val(v, depth)?;
    }
    Ok(())
}

fn check_raw_val(v: bson::RawBsonRef<'_>, depth: usize) -> Result<(), MongoError> {
    if depth > 200 {
        return Err(invalid("document too deep"));
    }
    match v {
        bson::RawBsonRef::Document(d) => check_raw_doc(d, depth + 1),
        bson::RawBsonRef::Array(a) => {
            for e in a {
                check_raw_val(
                    e.map_err(|e| invalid(format!("invalid BSON: {e}")))?,
                    depth + 1,
                )?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests;
