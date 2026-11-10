//! Bolt bookmarks (chapter 43, "Bolt front end", bookmarks; decision G9's
//! follow-up): `SB1:` and base64url of {tenant, commit time, per-cluster
//! GTID positions}, then `.` and base64url of an HMAC-SHA256 tag.
//!
//! The key is the tenant's bookmark secret in its catalog (`graph_keys`),
//! so every gateway signs and checks the same way, as AMQP reply-to
//! tokens do. A bookmark that doesn't parse, has a bad tag, or names
//! another tenant fails with 39917 (`InvalidBookmark`).

use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use ring::hmac;
use serde_json::json;
use sprig_common::{Result, SprigCode, SprigError};

/// What a bookmark says.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Bookmark {
    /// The tenant.
    pub tenant: u64,
    /// Commit time, microseconds since the epoch.
    pub hlc: u64,
    /// GTID positions per cluster (`dc-a-c2` → `0-1-123`).
    pub gtids: BTreeMap<String, String>,
}

fn invalid(msg: &str) -> SprigError {
    SprigError::new(
        SprigCode::BoltBookmarkInvalid,
        format!("invalid bookmark: {msg}"),
    )
}

/// A key from a secret.
#[must_use]
pub fn key(secret: &[u8]) -> hmac::Key {
    hmac::Key::new(hmac::HMAC_SHA256, secret)
}

/// The bookmark text.
#[must_use]
pub fn encode(b: &Bookmark, key: &hmac::Key) -> String {
    let body = json!({ "t": b.tenant, "h": b.hlc, "g": b.gtids }).to_string();
    let tag = hmac::sign(key, body.as_bytes());
    format!("SB1:{}.{}", B64.encode(body), B64.encode(tag.as_ref()))
}

/// Checks and reads a bookmark of `tenant`.
///
/// # Errors
/// 39917 for a bookmark that doesn't parse, carries a bad tag or names
/// another tenant.
pub fn decode(text: &str, tenant: u64, key: &hmac::Key) -> Result<Bookmark> {
    let rest = text
        .strip_prefix("SB1:")
        .ok_or_else(|| invalid("not a SprigDB bookmark"))?;
    let (body, tag) = rest.split_once('.').ok_or_else(|| invalid("unsigned"))?;
    let body = B64.decode(body).map_err(|_| invalid("bad encoding"))?;
    let tag = B64.decode(tag).map_err(|_| invalid("bad encoding"))?;
    hmac::verify(key, &body, &tag).map_err(|_| invalid("bad signature"))?;
    let j: serde_json::Value = serde_json::from_slice(&body).map_err(|_| invalid("bad body"))?;
    let t = j["t"].as_u64().ok_or_else(|| invalid("no tenant"))?;
    if t != tenant {
        return Err(invalid("another tenant's bookmark"));
    }
    let gtids = j["g"]
        .as_object()
        .map(|o| {
            o.iter()
                .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_owned())))
                .collect()
        })
        .unwrap_or_default();
    Ok(Bookmark {
        tenant: t,
        hlc: j["h"].as_u64().unwrap_or(0),
        gtids,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bookmarks_round_trip_and_refuse_forgeries() {
        let k = key(b"secret");
        let b = Bookmark {
            tenant: 7,
            hlc: 1,
            gtids: BTreeMap::from([("dc-a-c2".into(), "0-1-5".into())]),
        };
        let s = encode(&b, &k);
        assert!(s.starts_with("SB1:"));
        assert_eq!(decode(&s, 7, &k).expect("ok"), b);
        let code = |r: Result<Bookmark>| r.expect_err("refused").code();
        assert_eq!(
            code(decode(&s, 8, &k)),
            Some(SprigCode::BoltBookmarkInvalid)
        );
        assert_eq!(
            code(decode(&s, 7, &key(b"other"))),
            Some(SprigCode::BoltBookmarkInvalid)
        );
        let (body, tag) = s.split_once('.').expect("dot");
        let forged = format!("{}x.{tag}", &body[..body.len() - 1]);
        assert!(decode(&forged, 7, &k).is_err());
        assert!(decode("SB1:abc", 7, &k).is_err());
        assert!(decode("FB:abc", 7, &k).is_err());
    }
}
