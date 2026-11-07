//! Identifier newtypes (chapter 03, "Identifiers"; sizes per the writing
//! guide, 12.12).
//!
//! Two kinds of id can't be mixed up, and each has one textual form, used in
//! names, logs and etcd keys: `Display` writes it and `FromStr` reads it back.
//! Serde uses the same representation as the textual form's value (numbers
//! as numbers, strings as strings).

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Error returned when an id's textual form doesn't parse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid {kind}: {reason}")]
pub struct IdParseError {
    kind: &'static str,
    reason: String,
}

impl IdParseError {
    fn new(kind: &'static str, reason: impl Into<String>) -> Self {
        Self {
            kind,
            reason: reason.into(),
        }
    }
}

macro_rules! numeric_id {
    ($(#[$doc:meta])* $name:ident($repr:ty)) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub $repr);

        impl $name {
            /// The raw value.
            #[must_use]
            pub const fn get(self) -> $repr {
                self.0
            }
        }

        impl From<$repr> for $name {
            fn from(v: $repr) -> Self {
                Self(v)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }

        impl FromStr for $name {
            type Err = IdParseError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                // Only plain decimal digits: no sign, no whitespace.
                if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(IdParseError::new(stringify!($name), format!("{s:?} is not a decimal number")));
                }
                s.parse::<$repr>()
                    .map(Self)
                    .map_err(|e| IdParseError::new(stringify!($name), e.to_string()))
            }
        }
    };
}

numeric_id! {
    /// A tenant. Global; assigned by the global tier.
    TenantId(u64)
}
numeric_id! {
    /// A database (schema) within a tenant.
    DbId(u32)
}
numeric_id! {
    /// A table group within a tenant.
    GroupId(u32)
}
numeric_id! {
    /// A shard within a table group; never reused.
    ShardId(u32)
}
numeric_id! {
    /// A catalog object within a tenant: collection, namespace, search index,
    /// logic routine and so on.
    ObjectId(u64)
}
numeric_id! {
    /// A user within a tenant.
    UserId(u32)
}
numeric_id! {
    /// A role within a tenant.
    RoleId(u32)
}
numeric_id! {
    /// An epoch, per shard map and per tenant directory entry.
    Epoch(u64)
}
numeric_id! {
    /// A client connection within a DC. Only 32 bits because MySQL's wire
    /// format fixes the connection id at 32 bits (chapter 04).
    ConnId(u32)
}

/// Checks a data-center id against `[a-z][a-z0-9-]{1,31}` (chapters 03 and 23):
/// 2 to 32 characters, starting with a letter.
fn check_dc_id(s: &str) -> Result<(), IdParseError> {
    check_slug("DcId", s, 32)?;
    if s.len() < 2 || !s.starts_with(|c: char| c.is_ascii_lowercase()) {
        return Err(IdParseError::new(
            "DcId",
            format!("{s:?} must be 2 to 32 characters and start with a letter"),
        ));
    }
    Ok(())
}

/// Checks `s` against `[a-z0-9-]{1,max}`.
fn check_slug(kind: &'static str, s: &str, max: usize) -> Result<(), IdParseError> {
    if s.is_empty() || s.len() > max {
        return Err(IdParseError::new(
            kind,
            format!("{s:?} must be 1 to {max} characters"),
        ));
    }
    if let Some(c) = s
        .chars()
        .find(|c| !matches!(c, 'a'..='z' | '0'..='9' | '-'))
    {
        return Err(IdParseError::new(
            kind,
            format!("{s:?} contains {c:?}; allowed: a-z, 0-9, -"),
        ));
    }
    Ok(())
}

macro_rules! string_id {
    ($(#[$doc:meta])* $name:ident, $check:expr) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            /// Validates and wraps `s`.
            ///
            /// # Errors
            /// If `s` isn't a valid textual form.
            pub fn new(s: impl Into<String>) -> Result<Self, IdParseError> {
                let s = s.into();
                let check: fn(&str) -> Result<(), IdParseError> = $check;
                check(&s)?;
                Ok(Self(s))
            }

            /// The textual form.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdParseError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::new(s)
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdParseError;
            fn try_from(s: String) -> Result<Self, Self::Error> {
                Self::new(s)
            }
        }

        impl From<$name> for String {
            fn from(id: $name) -> String {
                id.0
            }
        }
    };
}

string_id! {
    /// A data center, matching `[a-z][a-z0-9-]{1,31}`, such as `mum1`. Global.
    DcId, |s| check_dc_id(s)
}
string_id! {
    /// A cluster, matching `[a-z0-9-]{1,48}`, such as `mum1-c7`. Global,
    /// prefixed by its data center.
    ClusterId, |s| check_slug("ClusterId", s, 48)
}
string_id! {
    /// A node within a cluster, such as `mum1-c7-n2`. Uses the cluster id
    /// alphabet, up to 64 characters.
    NodeId, |s| check_slug("NodeId", s, 64)
}

/// A job, per data center: a UUIDv7, stored as `BINARY(16)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobId(pub uuid::Uuid);

impl JobId {
    /// A new, time-ordered job id.
    #[must_use]
    pub fn new_v7() -> Self {
        Self(uuid::Uuid::now_v7())
    }

    /// The 16 bytes stored in `BINARY(16)`.
    #[must_use]
    pub fn to_bytes(self) -> [u8; 16] {
        self.0.into_bytes()
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0.as_simple(), f)
    }
}

impl FromStr for JobId {
    type Err = IdParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        uuid::Uuid::try_parse(s)
            .map(Self)
            .map_err(|e| IdParseError::new("JobId", e.to_string()))
    }
}

/// A distributed transaction: 16 bytes, the issuing gateway's id plus a
/// monotonic counter (chapter 18). Global; used as the XA `gtrid`. Textual
/// form: 32 lowercase hex digits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TxnId(pub u128);

impl TxnId {
    /// The 16 bytes stored in `BINARY(16)`, big-endian.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0.to_be_bytes()
    }

    /// From the 16 bytes stored in `BINARY(16)`.
    #[must_use]
    pub const fn from_bytes(b: [u8; 16]) -> Self {
        Self(u128::from_be_bytes(b))
    }
}

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

impl FromStr for TxnId {
    type Err = IdParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_hex(s, 32)
            .and_then(|()| u128::from_str_radix(s, 16).ok())
            .map(Self)
            .ok_or_else(|| IdParseError::new("TxnId", format!("{s:?} is not 32 hex digits")))
    }
}

/// A statement fingerprint: `xxh3_64` of the normalized text (chapter 12).
/// Within a tenant. Textual form: 16 lowercase hex digits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Fingerprint(pub u64);

impl fmt::Display for Fingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl FromStr for Fingerprint {
    type Err = IdParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_hex(s, 16)
            .and_then(|()| u64::from_str_radix(s, 16).ok())
            .map(Self)
            .ok_or_else(|| IdParseError::new("Fingerprint", format!("{s:?} is not 16 hex digits")))
    }
}

/// `Some(())` when `s` is exactly `len` lowercase hex digits.
fn parse_hex(s: &str, len: usize) -> Option<()> {
    (s.len() == len && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))).then_some(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_ids_round_trip_and_reject_junk() {
        let t: TenantId = "42".parse().unwrap();
        assert_eq!(t, TenantId(42));
        assert_eq!(t.to_string(), "42");
        assert_eq!(
            TenantId(u64::MAX).to_string().parse::<TenantId>().unwrap(),
            TenantId(u64::MAX)
        );
        for bad in ["", "-1", "+1", " 1", "0x10", "4294967296"] {
            assert!(bad.parse::<ShardId>().is_err(), "{bad:?}");
        }
        assert_eq!(serde_json::to_string(&DbId(1)).unwrap(), "1");
    }

    #[test]
    fn sizes_follow_the_guide() {
        // Guide 12.12: tenant and object ids are u64; db, group, shard, user
        // and role ids at least u32; transaction ids u128; job ids UUIDs.
        assert_eq!(size_of::<TenantId>(), 8);
        assert_eq!(size_of::<ObjectId>(), 8);
        assert_eq!(size_of::<Epoch>(), 8);
        assert_eq!(size_of::<DbId>(), 4);
        assert_eq!(size_of::<GroupId>(), 4);
        assert_eq!(size_of::<ShardId>(), 4);
        assert_eq!(size_of::<UserId>(), 4);
        assert_eq!(size_of::<RoleId>(), 4);
        assert_eq!(size_of::<TxnId>(), 16);
        assert_eq!(size_of::<JobId>(), 16);
    }

    #[test]
    fn string_ids_validate() {
        assert_eq!(DcId::new("mum1").unwrap().as_str(), "mum1");
        assert!(DcId::new("").is_err());
        assert!(DcId::new("Mum1").is_err());
        assert!(DcId::new("mum_1").is_err());
        assert!(DcId::new("a".repeat(33)).is_err());
        assert!(DcId::new("a".repeat(32)).is_ok());
        assert!(DcId::new("1dc").is_err());
        assert!(DcId::new("a").is_err());
        assert!(ClusterId::new("mum1-c7").is_ok());
        assert!(ClusterId::new("a".repeat(49)).is_err());
        assert!(NodeId::new("mum1-c7-n2").is_ok());
        let dc: DcId = serde_json::from_str("\"fra1\"").unwrap();
        assert_eq!(dc.to_string(), "fra1");
        assert!(serde_json::from_str::<DcId>("\"FRA\"").is_err());
    }

    #[test]
    fn hex_ids_round_trip() {
        let x = TxnId(0x0123_4567_89ab_cdef_0011_2233_4455_6677);
        assert_eq!(x.to_string(), "0123456789abcdef0011223344556677");
        assert_eq!(x.to_string().parse::<TxnId>().unwrap(), x);
        assert_eq!(TxnId::from_bytes(x.to_bytes()), x);
        assert!("0123".parse::<TxnId>().is_err());
        assert!("0123456789ABCDEF0011223344556677".parse::<TxnId>().is_err());

        let f = Fingerprint(0x9c1e_0b7a);
        assert_eq!(f.to_string(), "000000009c1e0b7a");
        assert_eq!(f.to_string().parse::<Fingerprint>().unwrap(), f);
    }

    #[test]
    fn job_ids_are_v7_and_round_trip() {
        let j = JobId::new_v7();
        assert_eq!(j.0.get_version_num(), 7);
        assert_eq!(j.to_string().parse::<JobId>().unwrap(), j);
        assert_eq!(j.to_string().len(), 32);
    }
}
