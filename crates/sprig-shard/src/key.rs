//! Key specifications and key values (chapter 15, "Table groups",
//! "Normalization").

use std::borrow::Cow;
use std::fmt;

use serde_json::{Value, json};
use sprig_common::ids::ObjectId;
use sprig_common::{Result, SprigCode, SprigError};

use crate::collation::{self, Charset, CollationId};

/// The `xxh3_64` hash of a normalized key: where a row routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RoutingHash(pub u64);

impl fmt::Display for RoutingHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:#018x}", self.0)
    }
}

/// A table group's kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GroupKind {
    /// No key, always one shard: every database starts with one.
    Default,
    /// One key spec for all members, 1 to many shards.
    Sharded,
    /// Lookup tables (chapter 19), one spec per member; one per tenant.
    Lookup,
    /// Small, rarely written tables with a copy per cluster.
    Reference,
}

impl GroupKind {
    /// The catalog's spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Sharded => "sharded",
            Self::Lookup => "lookup",
            Self::Reference => "reference",
        }
    }

    /// Parses the catalog's spelling.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "default" => Self::Default,
            "sharded" => Self::Sharded,
            "lookup" => Self::Lookup,
            "reference" => Self::Reference,
            _ => return None,
        })
    }
}

/// The class of one key part.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyClass {
    /// Integers and decimals: exact values.
    Numeric,
    /// Strings under a collation.
    String {
        /// The collation the group's members compare under.
        collation: CollationId,
    },
    /// Bytes.
    Binary,
    /// UUIDs (16 bytes).
    Uuid,
    /// `DATE` and `DATETIME`: calendar fields.
    DateTime,
    /// `TIMESTAMP`: instants.
    Timestamp,
}

impl KeyClass {
    fn to_json(self) -> Value {
        match self {
            Self::Numeric => json!({"class": "numeric"}),
            Self::String { collation } => {
                let name = collation::collation(collation).map_or("?", |c| c.name);
                json!({"class": "string", "collation": name})
            }
            Self::Binary => json!({"class": "binary"}),
            Self::Uuid => json!({"class": "uuid"}),
            Self::DateTime => json!({"class": "datetime"}),
            Self::Timestamp => json!({"class": "timestamp"}),
        }
    }

    fn from_json(v: &Value) -> Result<Self> {
        let class = v.get("class").and_then(Value::as_str).unwrap_or_default();
        Ok(match class {
            "numeric" => Self::Numeric,
            "string" => {
                let name = v
                    .get("collation")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                Self::String {
                    collation: collation::require_name(name)?.id,
                }
            }
            "binary" => Self::Binary,
            "uuid" => Self::Uuid,
            "datetime" => Self::DateTime,
            "timestamp" => Self::Timestamp,
            other => {
                return Err(SprigError::new(
                    SprigCode::UnsupportedKeyType,
                    format!("unknown key class {other:?}"),
                ));
            }
        })
    }
}

/// A group's distribution-key specification: one class per part, and the
/// digest shard maps record (`key_spec_digest`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KeySpec {
    /// The parts, in key order.
    pub parts: Vec<KeyClass>,
    /// `xxh3_64` of the canonical JSON form.
    pub digest: u64,
}

impl KeySpec {
    /// A spec over `parts`.
    ///
    /// # Errors
    /// 39400 for an empty key; 39401 for an unsupported collation.
    pub fn new(parts: Vec<KeyClass>) -> Result<Self> {
        if parts.is_empty() {
            return Err(SprigError::new(
                SprigCode::UnsupportedKeyType,
                "a distribution key needs at least one part",
            ));
        }
        for p in &parts {
            if let KeyClass::String { collation } = p {
                collation::require(*collation)?;
            }
        }
        let mut s = Self { parts, digest: 0 };
        s.digest = twox_hash::XxHash3_64::oneshot(s.to_json().to_string().as_bytes());
        Ok(s)
    }

    /// The `table_groups.key_spec` JSON: `{"parts":[{"class":"numeric"}]}`.
    #[must_use]
    pub fn to_json(&self) -> Value {
        json!({"parts": self.parts.iter().map(|p| p.to_json()).collect::<Vec<_>>()})
    }

    /// Reads [`to_json`](Self::to_json)'s form.
    ///
    /// # Errors
    /// 39400 or 39401 for parts that can't be keys.
    pub fn from_json(v: &Value) -> Result<Self> {
        let parts = v
            .get("parts")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                SprigError::new(SprigCode::UnsupportedKeyType, "a key spec without parts")
            })?
            .iter()
            .map(KeyClass::from_json)
            .collect::<Result<Vec<_>>>()?;
        Self::new(parts)
    }
}

/// How a group member supplies its key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberRouting {
    /// Columns or document paths holding the key, in key order.
    Columns(Vec<String>),
    /// Inherits its parent's routing hash through a foreign key.
    Parent {
        /// The parent object.
        parent: ObjectId,
        /// The constraint that reaches it.
        fk: String,
    },
    /// A lookup table: routed by the hash of its own value (chapter 19).
    Lookup,
    /// No key (default and reference groups).
    None,
}

/// A decimal number in text: `-12.340`, `5`, `1.5E+3`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecimalRef<'a>(pub &'a str);

/// Calendar fields of a `DATE`, `DATETIME` or BSON date (UTC).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct DateTimeParts {
    /// 0 to 9999.
    pub year: u16,
    /// 0 to 12 (0 in zero dates).
    pub month: u8,
    /// 0 to 31.
    pub day: u8,
    /// 0 to 23.
    pub hour: u8,
    /// 0 to 59.
    pub minute: u8,
    /// 0 to 59.
    pub second: u8,
    /// 0 to 999,999.
    pub micros: u32,
}

impl DateTimeParts {
    /// A `DATE`.
    #[must_use]
    pub fn date(year: u16, month: u8, day: u8) -> Self {
        Self {
            year,
            month,
            day,
            ..Self::default()
        }
    }

    /// Parses `YYYY-MM-DD[ HH:MM:SS[.ffffff]]` (or `T` between).
    #[must_use]
    #[allow(clippy::many_single_char_names)]
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        let (d, t) = match s.find([' ', 'T']) {
            Some(i) => (&s[..i], Some(&s[i + 1..])),
            None => (s, None),
        };
        let mut dp = d.split('-');
        let year = dp.next()?.parse().ok()?;
        let month = dp.next()?.parse().ok()?;
        let day = dp.next()?.parse().ok()?;
        if dp.next().is_some() || month > 12 || day > 31 {
            return None;
        }
        let mut v = Self::date(year, month, day);
        if let Some(t) = t {
            let (hms, frac) = match t.split_once('.') {
                Some((a, b)) => (a, Some(b)),
                None => (t, None),
            };
            let mut tp = hms.split(':');
            v.hour = tp.next()?.parse().ok()?;
            v.minute = tp.next()?.parse().ok()?;
            v.second = tp.next().map_or(Some(0), |x| x.parse().ok())?;
            if tp.next().is_some() || v.hour > 23 || v.minute > 59 || v.second > 59 {
                return None;
            }
            if let Some(f) = frac {
                if f.is_empty() || f.len() > 6 || !f.bytes().all(|b| b.is_ascii_digit()) {
                    return None;
                }
                let n: u32 = f.parse().ok()?;
                v.micros = n * 10u32.pow(6 - u32::try_from(f.len()).ok()?);
            }
        }
        Some(v)
    }
}

/// A key value, as the gateway sees it from SQL, documents, key-value or
/// Redis.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum KeyValue<'a> {
    /// SQL `NULL`, BSON null.
    Null,
    /// Any integer (SQL integer types, BSON int32 and int64).
    Int(i128),
    /// SQL `DECIMAL`, BSON `Decimal128`, as text.
    Decimal(DecimalRef<'a>),
    /// BSON double, key-value numbers.
    Double(f64),
    /// Booleans: 0 and 1.
    Bool(bool),
    /// A string in a character set.
    Str {
        /// The encoded bytes.
        bytes: &'a [u8],
        /// Their character set.
        charset: Charset,
    },
    /// Binary data, Redis keys.
    Bytes(&'a [u8]),
    /// A UUID in RFC 9562 (textual) byte order.
    Uuid([u8; 16]),
    /// A BSON `ObjectId`.
    ObjectId([u8; 12]),
    /// Calendar fields.
    DateTime(DateTimeParts),
    /// An instant.
    Timestamp {
        /// Seconds since the Unix epoch.
        secs: i64,
        /// Microseconds.
        micros: u32,
    },
}

impl<'a> KeyValue<'a> {
    /// A UTF-8 string.
    #[must_use]
    pub fn str(s: &'a str) -> Self {
        Self::Str {
            bytes: s.as_bytes(),
            charset: Charset::Utf8mb4,
        }
    }

    /// An owned copy.
    #[must_use]
    pub fn to_owned_value(&self) -> KeyValueOwned {
        match *self {
            Self::Null => KeyValueOwned::Null,
            Self::Int(i) => KeyValueOwned::Int(i),
            Self::Decimal(d) => KeyValueOwned::Decimal(d.0.to_owned()),
            Self::Double(d) => KeyValueOwned::Double(d),
            Self::Bool(b) => KeyValueOwned::Bool(b),
            Self::Str { bytes, charset } => KeyValueOwned::Str {
                bytes: bytes.to_vec(),
                charset,
            },
            Self::Bytes(b) => KeyValueOwned::Bytes(b.to_vec()),
            Self::Uuid(u) => KeyValueOwned::Uuid(u),
            Self::ObjectId(o) => KeyValueOwned::ObjectId(o),
            Self::DateTime(d) => KeyValueOwned::DateTime(d),
            Self::Timestamp { secs, micros } => KeyValueOwned::Timestamp { secs, micros },
        }
    }
}

/// An owned [`KeyValue`], for buffers that outlive a statement (lookup
/// releases, `IN` lists).
#[derive(Debug, Clone, PartialEq)]
pub enum KeyValueOwned {
    /// See [`KeyValue::Null`].
    Null,
    /// See [`KeyValue::Int`].
    Int(i128),
    /// See [`KeyValue::Decimal`].
    Decimal(String),
    /// See [`KeyValue::Double`].
    Double(f64),
    /// See [`KeyValue::Bool`].
    Bool(bool),
    /// See [`KeyValue::Str`].
    Str {
        /// The encoded bytes.
        bytes: Vec<u8>,
        /// Their character set.
        charset: Charset,
    },
    /// See [`KeyValue::Bytes`].
    Bytes(Vec<u8>),
    /// See [`KeyValue::Uuid`].
    Uuid([u8; 16]),
    /// See [`KeyValue::ObjectId`].
    ObjectId([u8; 12]),
    /// See [`KeyValue::DateTime`].
    DateTime(DateTimeParts),
    /// See [`KeyValue::Timestamp`].
    Timestamp {
        /// Seconds since the Unix epoch.
        secs: i64,
        /// Microseconds.
        micros: u32,
    },
}

impl KeyValueOwned {
    /// A borrowed view.
    #[must_use]
    pub fn as_ref(&self) -> KeyValue<'_> {
        match self {
            Self::Null => KeyValue::Null,
            Self::Int(i) => KeyValue::Int(*i),
            Self::Decimal(d) => KeyValue::Decimal(DecimalRef(d)),
            Self::Double(d) => KeyValue::Double(*d),
            Self::Bool(b) => KeyValue::Bool(*b),
            Self::Str { bytes, charset } => KeyValue::Str {
                bytes,
                charset: *charset,
            },
            Self::Bytes(b) => KeyValue::Bytes(b),
            Self::Uuid(u) => KeyValue::Uuid(*u),
            Self::ObjectId(o) => KeyValue::ObjectId(*o),
            Self::DateTime(d) => KeyValue::DateTime(*d),
            Self::Timestamp { secs, micros } => KeyValue::Timestamp {
                secs: *secs,
                micros: *micros,
            },
        }
    }
}

/// A `BINARY(n)` value as MariaDB stores it: right-padded with `0x00` to
/// `n` bytes.
#[must_use]
pub fn pad_binary(v: &[u8], n: usize) -> Cow<'_, [u8]> {
    if v.len() >= n {
        Cow::Borrowed(v)
    } else {
        let mut p = v.to_vec();
        p.resize(n, 0);
        Cow::Owned(p)
    }
}

/// Parses a UUID in its textual form (with or without hyphens, braces).
#[must_use]
pub fn parse_uuid(s: &str) -> Option<[u8; 16]> {
    let hex: Vec<u8> = s
        .trim()
        .trim_start_matches('{')
        .trim_end_matches('}')
        .bytes()
        .filter(|&b| b != b'-')
        .collect();
    if hex.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, slot) in out.iter_mut().enumerate() {
        let h = std::str::from_utf8(&hex[2 * i..2 * i + 2]).ok()?;
        *slot = u8::from_str_radix(h, 16).ok()?;
    }
    Some(out)
}
