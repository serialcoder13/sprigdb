//! A namespace's tables (chapter 33, "Tables per namespace").
//!
//! - the **key table** `<name>`: one row per key (type, string value,
//!   element count, list bounds, generation, version, expiry);
//! - the **element table** `__sprig_ke_<name>`: hash fields, list items
//!   and set members, under `(k, g)`;
//! - the **sorted-set table** `__sprig_kz_<name>`: members with
//!   `(c, score)`, `c` being -1, 0 or 1 for `-inf`, finite and `+inf`.
//!
//! Every row carries the distribution key `_sprig_dk` and its routing hash
//! `_sprig_rk` (chapter 15), filled by the front end, so a sharded group
//! routes on `_sprig_dk` and `SCAN` walks keys in hash order.

use sprig_shard::key::{KeyClass, KeySpec, KeyValue};

use crate::sql::ident;

/// A key's type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyType {
    /// One value.
    String,
    /// Field-value pairs.
    Hash,
    /// Items by position.
    List,
    /// Members.
    Set,
    /// Members with scores.
    ZSet,
}

impl KeyType {
    /// The `type` column's value, also Redis's `TYPE` reply.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Hash => "hash",
            Self::List => "list",
            Self::Set => "set",
            Self::ZSet => "zset",
        }
    }

    /// From the `type` column.
    #[must_use]
    pub fn parse(s: &[u8]) -> Option<Self> {
        Some(match s {
            b"string" => Self::String,
            b"hash" => Self::Hash,
            b"list" => Self::List,
            b"set" => Self::Set,
            b"zset" => Self::ZSet,
            _ => return None,
        })
    }
}

/// How a key's distribution key is taken (`kv_namespaces.dist_rule`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistRule {
    /// The first `n` segments of a `/` key (`0` = the whole key).
    Segments(u8),
    /// Redis Cluster's hash tag.
    HashTag,
}

/// Redis Cluster's hash tag: the bytes between the first `{` and the next
/// `}`, when not empty; else the whole key.
#[must_use]
pub fn hash_tag(key: &[u8]) -> &[u8] {
    if let Some(open) = key.iter().position(|b| *b == b'{')
        && let Some(close) = key[open + 1..].iter().position(|b| *b == b'}')
        && close > 0
    {
        return &key[open + 1..open + 1 + close];
    }
    key
}

/// The first `n` segments of a key starting with `/` (without the leading
/// `/`), or the whole key.
#[must_use]
pub fn segments(key: &[u8], n: u8) -> &[u8] {
    if n == 0 || key.first() != Some(&b'/') {
        return key;
    }
    let body = &key[1..];
    let mut seen = 0u8;
    for (i, b) in body.iter().enumerate() {
        if *b == b'/' {
            seen += 1;
            if seen == n {
                return &body[..i];
            }
        }
    }
    body
}

/// Redis Cluster's slot for a key (`CLUSTER KEYSLOT`): CRC16/XMODEM of the
/// hash tag, mod 16384. For information only; routing uses `_sprig_rk`.
#[must_use]
pub fn key_slot(key: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for b in hash_tag(key) {
        crc ^= u16::from(*b) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc & 0x3FFF
}

/// One namespace: where its tables are and how its keys route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Namespace {
    /// The database holding the tables (logical through the gateway).
    pub database: String,
    /// The key table's name; the element tables derive from it.
    pub table: String,
    /// The distribution rule.
    pub rule: DistRule,
    /// Largest value or list item (`max_value_bytes`).
    pub max_value_bytes: usize,
}

/// Longest key (`max_key_bytes`).
pub const MAX_KEY_BYTES: usize = 1024;
/// Longest hash field, set or sorted-set member (39731).
pub const MAX_ELEMENT_BYTES: usize = 2024;

impl Namespace {
    /// A Redis database's namespace: `db<n>` with hash tags, 16 MiB values.
    #[must_use]
    pub fn redis(database: &str, index: u32) -> Self {
        Self {
            database: database.to_owned(),
            table: format!("db{index}"),
            rule: DistRule::HashTag,
            max_value_bytes: 16 << 20,
        }
    }

    /// The key table, qualified.
    #[must_use]
    pub fn keys(&self) -> String {
        format!("{}.{}", ident(&self.database), ident(&self.table))
    }

    /// The element table, qualified.
    #[must_use]
    pub fn elems(&self) -> String {
        format!(
            "{}.{}",
            ident(&self.database),
            ident(&format!("__sprig_ke_{}", self.table))
        )
    }

    /// The sorted-set table, qualified.
    #[must_use]
    pub fn zset(&self) -> String {
        format!(
            "{}.{}",
            ident(&self.database),
            ident(&format!("__sprig_kz_{}", self.table))
        )
    }

    /// The distribution key of `key`.
    #[must_use]
    pub fn dist_key<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        match self.rule {
            DistRule::HashTag => hash_tag(key),
            DistRule::Segments(n) => segments(key, n),
        }
    }

    /// `_sprig_rk` of `key`: `xxh3_64` of its normalized distribution key
    /// (chapter 15, binary class).
    #[must_use]
    pub fn routing_hash(&self, key: &[u8]) -> u64 {
        routing_hash_of(self.dist_key(key))
    }

    /// The statements that create the three tables if they don't exist.
    #[must_use]
    pub fn ddl(&self) -> Vec<String> {
        vec![
            format!("CREATE DATABASE IF NOT EXISTS {}", ident(&self.database)),
            format!(
                "CREATE TABLE IF NOT EXISTS {} (\
                 k VARBINARY(1024) NOT NULL, \
                 type ENUM('string','hash','list','set','zset') NOT NULL DEFAULT 'string', \
                 v LONGBLOB NULL, \
                 n BIGINT UNSIGNED NOT NULL DEFAULT 0, \
                 lh BIGINT NOT NULL DEFAULT 0, \
                 lt BIGINT NOT NULL DEFAULT 0, \
                 g BIGINT UNSIGNED NOT NULL, \
                 _v BIGINT UNSIGNED NOT NULL, \
                 expires_at DATETIME(3) NULL, \
                 _sprig_dk VARBINARY(1024) NOT NULL DEFAULT '' INVISIBLE, \
                 _sprig_rk BIGINT UNSIGNED NULL INVISIBLE, \
                 PRIMARY KEY (k), \
                 KEY _sprig_exp (expires_at), \
                 KEY _sprig_rk (_sprig_rk)\
                 ) ENGINE=InnoDB",
                self.keys()
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {} (\
                 k VARBINARY(1024) NOT NULL, \
                 g BIGINT UNSIGNED NOT NULL, \
                 o BIGINT NOT NULL, \
                 e VARBINARY(2024) NOT NULL DEFAULT '', \
                 v LONGBLOB NULL, \
                 _sprig_dk VARBINARY(1024) NOT NULL DEFAULT '' INVISIBLE, \
                 _sprig_rk BIGINT UNSIGNED NULL INVISIBLE, \
                 PRIMARY KEY (k, g, o, e), \
                 KEY _sprig_rk (_sprig_rk)\
                 ) ENGINE=InnoDB",
                self.elems()
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {} (\
                 k VARBINARY(1024) NOT NULL, \
                 g BIGINT UNSIGNED NOT NULL, \
                 o BIGINT NOT NULL, \
                 m VARBINARY(2024) NOT NULL, \
                 c TINYINT NOT NULL, \
                 score DOUBLE NOT NULL, \
                 _sprig_dk VARBINARY(1024) NOT NULL DEFAULT '' INVISIBLE, \
                 _sprig_rk BIGINT UNSIGNED NULL INVISIBLE, \
                 PRIMARY KEY (k, g, o, m), \
                 KEY _sprig_score (k, g, c, score, m), \
                 KEY _sprig_rk (_sprig_rk)\
                 ) ENGINE=InnoDB",
                self.zset()
            ),
        ]
    }

    /// The statements that drop the three tables.
    #[must_use]
    pub fn drop_ddl(&self) -> Vec<String> {
        vec![
            format!("DROP TABLE IF EXISTS {}", self.zset()),
            format!("DROP TABLE IF EXISTS {}", self.elems()),
            format!("DROP TABLE IF EXISTS {}", self.keys()),
        ]
    }
}

/// `xxh3_64` of `dk` normalized as a binary key.
#[must_use]
pub fn routing_hash_of(dk: &[u8]) -> u64 {
    thread_local! {
        static SPEC: Option<KeySpec> = KeySpec::new(vec![KeyClass::Binary]).ok();
    }
    SPEC.with(|s| {
        s.as_ref()
            .and_then(|spec| sprig_shard::hash::hash_key(spec, &[KeyValue::Bytes(dk)]).ok())
            .map_or_else(|| sprig_shard::hash::routing_hash(dk).0, |(h, _)| h.0)
    })
}

/// The live predicate on the key table's alias `t` (or none).
#[must_use]
pub fn live(alias: &str) -> String {
    let p = if alias.is_empty() {
        String::new()
    } else {
        format!("{alias}.")
    };
    format!("({p}expires_at IS NULL OR {p}expires_at > UTC_TIMESTAMP(3))")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_tags_follow_redis_cluster() {
        assert_eq!(hash_tag(b"{user7}:cart"), b"user7");
        assert_eq!(hash_tag(b"foo{}{bar}"), b"foo{}{bar}");
        assert_eq!(hash_tag(b"foo{{bar}}"), b"{bar");
        assert_eq!(hash_tag(b"foo{bar}{zap}"), b"bar");
        assert_eq!(hash_tag(b"plain"), b"plain");
        assert_eq!(hash_tag(b"{"), b"{");
    }

    #[test]
    fn slots_match_redis() {
        // Values from Redis's CLUSTER KEYSLOT.
        assert_eq!(key_slot(b"foo"), 12182);
        assert_eq!(key_slot(b"{user1000}.following"), key_slot(b"user1000"));
        assert_eq!(key_slot(b"123456789"), 0x31C3 & 0x3FFF);
    }

    #[test]
    fn segments_rule() {
        assert_eq!(segments(b"/users/42/profile", 1), b"users");
        assert_eq!(segments(b"/users/42/profile", 2), b"users/42");
        assert_eq!(segments(b"users/42", 1), b"users/42");
        assert_eq!(segments(b"/users", 1), b"users");
    }

    #[test]
    fn co_located_keys_share_a_hash() {
        let ns = Namespace::redis("redis", 0);
        assert_eq!(ns.routing_hash(b"{u7}:a"), ns.routing_hash(b"{u7}:b"));
        assert_ne!(ns.routing_hash(b"u7:a"), ns.routing_hash(b"u7:b"));
    }
}
