//! Versioned records: how values are encoded in etcd.
//!
//! Every value is a UTF-8 JSON object under 64 KiB with a schema version
//! `"v"`. Unknown fields are kept and written back, and a build never
//! rewrites a record whose `v` it doesn't know.

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use sprig_common::{Result, SprigError};

/// Largest encoded value.
pub const MAX_VALUE_BYTES: usize = 64 * 1024;

/// A record type stored in etcd, with the schema version this build writes.
///
/// Implementations must not use `#[serde(deny_unknown_fields)]`: newer
/// builds add fields.
pub trait Versioned: Serialize + DeserializeOwned {
    /// The newest schema version this build understands.
    const V: u32;
}

/// A decoded record: its body, its schema version and the fields this build
/// doesn't know.
#[derive(Debug, Clone, PartialEq)]
pub struct Record<T> {
    /// The fields this build knows.
    pub body: T,
    v: u32,
    unknown: Map<String, Value>,
}

impl<T: Versioned> Record<T> {
    /// A new record at this build's schema version.
    pub fn new(body: T) -> Self {
        Self {
            body,
            v: T::V,
            unknown: Map::new(),
        }
    }

    /// The schema version the record was written with.
    #[must_use]
    pub fn schema_version(&self) -> u32 {
        self.v
    }

    /// Whether this build may write the record back.
    #[must_use]
    pub fn writable(&self) -> bool {
        self.v <= T::V
    }

    /// Decodes a stored value.
    ///
    /// # Errors
    /// `Internal` if the value isn't a JSON object with a numeric `v`, or
    /// the known fields don't parse.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let bad = |why: String| SprigError::internal(format!("bad etcd record: {why}"));
        let map: Map<String, Value> =
            serde_json::from_slice(bytes).map_err(|e| bad(e.to_string()))?;
        let v = map
            .get("v")
            .and_then(Value::as_u64)
            .and_then(|v| u32::try_from(v).ok())
            .ok_or_else(|| bad("missing schema version \"v\"".to_owned()))?;
        let body: T =
            serde_json::from_value(Value::Object(map.clone())).map_err(|e| bad(e.to_string()))?;
        let known = match serde_json::to_value(&body) {
            Ok(Value::Object(m)) => m,
            _ => Map::new(),
        };
        let unknown = map
            .into_iter()
            .filter(|(k, _)| k != "v" && !known.contains_key(k))
            .collect();
        Ok(Self { body, v, unknown })
    }

    /// Encodes the record, keeping unknown fields.
    ///
    /// # Errors
    /// `Internal` if the record has a newer schema version than this build
    /// knows, the body isn't a JSON object, or the value exceeds 64 KiB.
    pub fn encode(&self) -> Result<Vec<u8>> {
        if !self.writable() {
            return Err(SprigError::internal(format!(
                "record has schema version {}, this build knows {}: not rewriting it",
                self.v,
                T::V
            )));
        }
        let Ok(Value::Object(mut map)) = serde_json::to_value(&self.body) else {
            return Err(SprigError::internal("etcd records must be JSON objects"));
        };
        for (k, v) in &self.unknown {
            map.entry(k.clone()).or_insert_with(|| v.clone());
        }
        map.insert("v".to_owned(), Value::from(T::V));
        let bytes = serde_json::to_vec(&map)
            .map_err(|e| SprigError::internal(format!("encoding etcd record: {e}")))?;
        if bytes.len() > MAX_VALUE_BYTES {
            return Err(SprigError::internal(format!(
                "etcd record of {} bytes exceeds {MAX_VALUE_BYTES}",
                bytes.len()
            )));
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct DcRecord {
        state: String,
    }
    impl Versioned for DcRecord {
        const V: u32 = 1;
    }

    #[test]
    fn round_trip_keeps_unknown_fields() {
        let stored = br#"{"v":1,"state":"active","added_by_newer_build":[1,2]}"#;
        let mut r = Record::<DcRecord>::decode(stored).unwrap();
        assert_eq!(r.body.state, "active");
        r.body.state = "draining".to_owned();
        let again: Value = serde_json::from_slice(&r.encode().unwrap()).unwrap();
        assert_eq!(again["state"], "draining");
        assert_eq!(again["added_by_newer_build"], serde_json::json!([1, 2]));
        assert_eq!(again["v"], 1);
    }

    #[test]
    fn newer_versions_are_read_but_never_rewritten() {
        let r = Record::<DcRecord>::decode(br#"{"v":2,"state":"active"}"#).unwrap();
        assert_eq!(r.schema_version(), 2);
        assert!(!r.writable());
        assert!(r.encode().is_err());
    }

    #[test]
    fn rejects_bad_values() {
        assert!(Record::<DcRecord>::decode(br#"{"state":"active"}"#).is_err());
        assert!(Record::<DcRecord>::decode(b"[1]").is_err());
        let big = Record::new(DcRecord {
            state: "x".repeat(MAX_VALUE_BYTES),
        });
        assert!(big.encode().is_err());
    }
}
