//! Reply texts Redis 7.2 sends, so clients and tests see the same errors.

use std::fmt::Write as _;

use sprig_common::SprigCode;
use sprig_proto_redis::Frame;

/// `WRONGTYPE` (the key-value engine's 39730).
pub const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

/// `-WRONGTYPE ...`.
#[must_use]
pub fn wrongtype() -> Frame {
    Frame::error(WRONGTYPE)
}

/// `-ERR syntax error`.
#[must_use]
pub fn syntax() -> Frame {
    Frame::err("syntax error")
}

/// `-ERR value is not an integer or out of range`.
#[must_use]
pub fn not_int() -> Frame {
    Frame::err("value is not an integer or out of range")
}

/// `-ERR value is not a valid float`.
#[must_use]
pub fn not_float() -> Frame {
    Frame::err("value is not a valid float")
}

/// `-ERR no such key`.
#[must_use]
pub fn no_such_key() -> Frame {
    Frame::err("no such key")
}

/// `-ERR wrong number of arguments for '<cmd>' command`.
#[must_use]
pub fn arity(cmd: &str) -> Frame {
    Frame::err(&format!(
        "wrong number of arguments for '{}' command",
        cmd.to_ascii_lowercase()
    ))
}

/// `-ERR unknown command '<cmd>', with args beginning with: ...`.
#[must_use]
pub fn unknown(cmd: &[u8], args: &[bytes::Bytes]) -> Frame {
    let mut t = format!(
        "unknown command '{}', with args beginning with: ",
        trunc(cmd)
    );
    for a in args.iter().take(20) {
        let _ = write!(t, "'{}' ", trunc(a));
    }
    Frame::err(&t)
}

/// `-ERR unknown subcommand '<sub>'. Try <CMD> HELP.`.
#[must_use]
pub fn unknown_sub(cmd: &str, sub: &[u8]) -> Frame {
    Frame::err(&format!(
        "unknown subcommand '{}'. Try {} HELP.",
        trunc(sub),
        cmd.to_ascii_uppercase()
    ))
}

fn trunc(b: &[u8]) -> String {
    let s = String::from_utf8_lossy(&b[..b.len().min(128)]).into_owned();
    s.replace(['\r', '\n'], " ")
}

/// `-SPRIG <NAME> <number> <message>`, the name and number from the
/// registry (`SprigCode`).
#[must_use]
pub fn sprig(code: SprigCode, message: &str) -> Frame {
    Frame::error(format!(
        "SPRIG {} {} {message}",
        code.short_name(),
        code.number()
    ))
}

/// 39840: a Redis 7.2 command not in this release.
#[must_use]
pub fn unsupported(cmd: &str, when: &str) -> Frame {
    sprig(
        SprigCode::RedisUnsupported,
        &format!("{} is {when}", cmd.to_ascii_uppercase()),
    )
}

/// 39842: an administrative command SprigDB doesn't offer.
#[must_use]
pub fn restricted(cmd: &str) -> Frame {
    sprig(
        SprigCode::RedisRestricted,
        &format!(
            "{} is not offered; manage SprigDB through SQL, the API or sprigctl",
            cmd.to_ascii_uppercase()
        ),
    )
}

/// 39731: a hash field, set or sorted-set member over 2024 bytes.
#[must_use]
pub fn element_too_long() -> Frame {
    sprig(
        SprigCode::KvElementTooLong,
        "hash fields and set members are at most 2024 bytes",
    )
}

/// 39723: a key over 1024 bytes.
#[must_use]
pub fn key_too_long() -> Frame {
    sprig(SprigCode::KvInvalidKey, "keys are at most 1024 bytes")
}

/// 39724: a value over the namespace's `max_value_bytes`.
#[must_use]
pub fn value_too_big(max: usize) -> Frame {
    sprig(
        SprigCode::KvValueInvalid,
        &format!("values are at most {max} bytes in this database"),
    )
}

/// 39846: retries spent on conflicts.
#[must_use]
pub fn tryagain() -> Frame {
    let c = SprigCode::RedisShardBusy;
    Frame::error(format!(
        "TRYAGAIN SPRIG {} {} conflicting writes; retry",
        c.short_name(),
        c.number()
    ))
}

/// A backend failure, as `-ERR`.
#[must_use]
pub fn backend(e: &sprig_kv::SqlError) -> Frame {
    match e.errno {
        Some(1142 | 1143 | 1044 | 1045) => Frame::error(format!(
            "NOPERM this user has no permissions to run this command on this data: {}",
            e.message
        )),
        _ => Frame::err(&format!("SprigDB backend: {e}")),
    }
}
