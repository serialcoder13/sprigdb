//! Keyspace notifications (chapter 40, "Pub/sub and keyspace
//! notifications"; build step 15; RLS round 5, decision RL46).
//!
//! ```text
//! a write command that changed something, once its transaction committed
//! (a single command, or each command of EXEC):
//!   its events (event name, class, key), as Redis names them
//!   the tenant's flags (CONFIG SET notify-keyspace-events here, else
//!   redis.notify_keyspace_events): K → PUBLISH __keyspace@<db>__:<key> <event>
//!                                  E → PUBLISH __keyevent@<db>__:<event> <key>
//! delivering one to a subscriber (Session::deliver):
//!   the subscriber's own session checks the key (KeyFilter): SELECT on the
//!   database's key table and its row-level security over the row {k: key};
//!   a key it may not see is not delivered
//! ```
//!
//! **As built.** Events come from the commands on the gateway that ran them,
//! not from the feed: writes through SQL, the key-value API and scripts,
//! and keys expiring, send none; `new`, `evicted` and `keymiss` never
//! occur. Whether a command changed anything is read from its reply (an
//! integer above 0, `OK`, a value), so a multi-key `DEL` that deleted some
//! of its keys names all of them.

use bytes::Bytes;
use sprig_proto_redis::Frame;

/// Who may see a key's notification: the gateway's check through the
/// subscriber's session.
pub trait KeyFilter: Send + Sync + std::fmt::Debug {
    /// The statement that checks `keys` of key table `table` of logical
    /// database `database`.
    fn statement(&self, database: &str, table: &str, keys: &[Bytes]) -> String;
    /// Its rows as one flag per key.
    fn visible(&self, rows: &[sprig_kv::sql::Row], keys: usize) -> Vec<bool>;
}

/// Flag bits.
const GENERIC: u16 = 1;
const STRING: u16 = 2;
const LIST: u16 = 4;
const SET: u16 = 8;
const HASH: u16 = 16;
const ZSET: u16 = 32;
const EXPIRED: u16 = 64;
const EVICTED: u16 = 128;
const STREAM: u16 = 256;
const MODULE: u16 = 512;
const NEW: u16 = 1024;
const KEYSPACE: u16 = 2048;
const KEYEVENT: u16 = 4096;
const KEYMISS: u16 = 8192;
/// `A`: Redis's `NOTIFY_ALL`.
const ALL: u16 = GENERIC | STRING | LIST | SET | HASH | ZSET | EXPIRED | EVICTED | STREAM | MODULE;

/// Parses a flag string as Redis does; `None` for an unknown character.
#[must_use]
pub fn parse_flags(s: &[u8]) -> Option<u16> {
    let mut f = 0;
    for c in s {
        f |= match c {
            b'A' => ALL,
            b'g' => GENERIC,
            b'$' => STRING,
            b'l' => LIST,
            b's' => SET,
            b'h' => HASH,
            b'z' => ZSET,
            b'x' => EXPIRED,
            b'e' => EVICTED,
            b't' => STREAM,
            b'd' => MODULE,
            b'n' => NEW,
            b'K' => KEYSPACE,
            b'E' => KEYEVENT,
            b'm' => KEYMISS,
            _ => return None,
        };
    }
    Some(f)
}

/// The canonical text of flags (Redis's `keyspaceEventsFlagsToString`).
#[must_use]
pub fn flags_text(f: u16) -> String {
    let mut s = String::new();
    if f & ALL == ALL {
        s.push('A');
    } else {
        for (bit, c) in [
            (GENERIC, 'g'),
            (STRING, '$'),
            (LIST, 'l'),
            (SET, 's'),
            (HASH, 'h'),
            (ZSET, 'z'),
            (EXPIRED, 'x'),
            (EVICTED, 'e'),
            (STREAM, 't'),
            (MODULE, 'd'),
            (NEW, 'n'),
        ] {
            if f & bit != 0 {
                s.push(c);
            }
        }
    }
    if f & KEYSPACE != 0 {
        s.push('K');
    }
    if f & KEYEVENT != 0 {
        s.push('E');
    }
    if f & KEYMISS != 0 {
        s.push('m');
    }
    s
}

/// Whether `reply` says the command changed something.
fn changed(reply: &Frame) -> bool {
    match reply {
        Frame::Integer(n) => *n > 0,
        Frame::Simple(_)
        | Frame::Bulk(_)
        | Frame::Double(_)
        | Frame::DoubleText(_)
        | Frame::BigNumber(_)
        | Frame::Boolean(true) => true,
        Frame::Array(v) | Frame::Set(v) => !v.is_empty(),
        _ => false,
    }
}

fn lower(b: &Bytes) -> String {
    String::from_utf8_lossy(b).to_ascii_lowercase()
}

/// The events of command `a` (its arguments, `a[0]` the name) that
/// answered `reply`: (event, class bit, key).
#[must_use]
pub fn events(a: &[Bytes], reply: &Frame) -> Vec<(&'static str, u16, Bytes)> {
    let Some(name) = a.first().map(lower) else {
        return Vec::new();
    };
    if matches!(reply, Frame::Error(_)) || !changed(reply) {
        return Vec::new();
    }
    let k = |i: usize| a.get(i).cloned();
    let one = |ev: &'static str, class: u16| k(1).map(|x| vec![(ev, class, x)]).unwrap_or_default();
    match name.as_str() {
        "set" | "setnx" | "setex" | "psetex" | "getset" => one("set", STRING),
        "mset" | "msetnx" => a[1..]
            .chunks(2)
            .map(|c| ("set", STRING, c[0].clone()))
            .collect(),
        "append" => one("append", STRING),
        "setrange" => one("setrange", STRING),
        "incr" | "decr" | "incrby" | "decrby" => one("incrby", STRING),
        "incrbyfloat" => one("incrbyfloat", STRING),
        "getdel" => one("del", GENERIC),
        "del" | "unlink" => a[1..].iter().map(|x| ("del", GENERIC, x.clone())).collect(),
        "expire" | "pexpire" | "expireat" | "pexpireat" => one("expire", GENERIC),
        "persist" => one("persist", GENERIC),
        "rename" | "renamenx" => match (k(1), k(2)) {
            (Some(f), Some(t)) => vec![("rename_from", GENERIC, f), ("rename_to", GENERIC, t)],
            _ => Vec::new(),
        },
        "hset" | "hsetnx" | "hmset" => one("hset", HASH),
        "hdel" => one("hdel", HASH),
        "hincrby" => one("hincrby", HASH),
        "hincrbyfloat" => one("hincrbyfloat", HASH),
        "lpush" | "lpushx" => one("lpush", LIST),
        "rpush" | "rpushx" => one("rpush", LIST),
        "lpop" => one("lpop", LIST),
        "rpop" => one("rpop", LIST),
        "lset" => one("lset", LIST),
        "lrem" => one("lrem", LIST),
        "ltrim" => one("ltrim", LIST),
        "linsert" => one("linsert", LIST),
        "sadd" => one("sadd", SET),
        "srem" => one("srem", SET),
        "spop" => one("spop", SET),
        "zadd" => one("zadd", ZSET),
        "zincrby" => one("zincr", ZSET),
        "zrem" => one("zrem", ZSET),
        "zpopmin" => one("zpopmin", ZSET),
        "zpopmax" => one("zpopmax", ZSET),
        _ => Vec::new(),
    }
}

/// The publishes of `events` under `flags` in database `db`:
/// (channel, message).
#[must_use]
pub fn publishes(
    flags: u16,
    db: u32,
    events: &[(&'static str, u16, Bytes)],
) -> Vec<(Bytes, Bytes)> {
    let mut out = Vec::new();
    for (ev, class, key) in events {
        if flags & class == 0 {
            continue;
        }
        if flags & KEYSPACE != 0 {
            let mut c = format!("__keyspace@{db}__:").into_bytes();
            c.extend_from_slice(key);
            out.push((Bytes::from(c), Bytes::from_static(ev.as_bytes())));
        }
        if flags & KEYEVENT != 0 {
            out.push((Bytes::from(format!("__keyevent@{db}__:{ev}")), key.clone()));
        }
    }
    out
}

/// The database and key a keyspace channel's message names: `None` for
/// any other channel.
#[must_use]
pub fn key_of(channel: &[u8], data: &Bytes) -> Option<(u32, Bytes)> {
    let (rest, keyspace) = if let Some(r) = channel.strip_prefix(b"__keyspace@") {
        (r, true)
    } else if let Some(r) = channel.strip_prefix(b"__keyevent@") {
        (r, false)
    } else {
        return None;
    };
    let end = rest.windows(3).position(|w| w == b"__:")?;
    let db: u32 = std::str::from_utf8(&rest[..end]).ok()?.parse().ok()?;
    let key = if keyspace {
        Bytes::copy_from_slice(&rest[end + 3..])
    } else {
        data.clone()
    };
    Some((db, key))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_owned())
    }

    #[test]
    fn flags() {
        assert_eq!(parse_flags(b"KEA").map(flags_text).as_deref(), Some("AKE"));
        assert_eq!(parse_flags(b"Kg$").map(flags_text).as_deref(), Some("g$K"));
        assert_eq!(parse_flags(b"").map(flags_text).as_deref(), Some(""));
        assert_eq!(parse_flags(b"Kq"), None);
    }

    #[test]
    fn events_and_channels() {
        let e = events(&[b("SET"), b("k1"), b("v")], &Frame::ok());
        assert_eq!(e, vec![("set", STRING, b("k1"))]);
        assert!(events(&[b("DEL"), b("k1")], &Frame::Integer(0)).is_empty());
        let p = publishes(parse_flags(b"KEA").unwrap_or(0), 0, &e);
        assert_eq!(
            p,
            vec![
                (b("__keyspace@0__:k1"), b("set")),
                (b("__keyevent@0__:set"), b("k1"))
            ]
        );
        assert!(publishes(parse_flags(b"Kh").unwrap_or(0), 0, &e).is_empty());
        assert_eq!(
            key_of(b"__keyspace@3__:a:b", &b("set")),
            Some((3, b("a:b")))
        );
        assert_eq!(key_of(b"__keyevent@0__:del", &b("k")), Some((0, b("k"))));
        assert_eq!(key_of(b"news", &b("k")), None);
    }
}
