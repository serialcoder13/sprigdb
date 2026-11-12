//! The SprigDB statement comment (chapter 38, "Statement comments").
//!
//! The gateway's backend layer prefixes every write statement with one
//! comment, which MariaDB copies into the statement's `ANNOTATE_ROWS`
//! event:
//!
//! ```text
//! /*sprig h=<16 hex: HLC> o=<i|u|r|d|x> [l=<32 hex: lsid>] [t=<hex: txnNumber>]
//!          [g=<32 hex: global transaction id>] [c=<Redis command>]*/
//! ```
//!
//! The change feed keeps only this prefix and drops the rest of the
//! annotate text at once, so SQL literals never leave the reader.

use std::fmt::Write as _;

/// The `o` field: what the statement did, as the front end saw it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OpHint {
    /// `i`: an insert.
    Insert,
    /// `u`: an update.
    Update,
    /// `r`: a whole-document replace.
    Replace,
    /// `d`: a delete.
    Delete,
    /// `x`: a TTL expiry.
    Expire,
    /// `k`: a Redis command's write, whose keyspace notifications the
    /// command sent itself (chapter 40; RLS round 5, RL51): the gateways'
    /// notifications from the feed pass over its rows.
    Notified,
}

impl OpHint {
    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "i" => Self::Insert,
            "u" => Self::Update,
            "r" => Self::Replace,
            "d" => Self::Delete,
            "x" => Self::Expire,
            "k" => Self::Notified,
            _ => return None,
        })
    }

    fn letter(self) -> char {
        match self {
            Self::Insert => 'i',
            Self::Update => 'u',
            Self::Replace => 'r',
            Self::Delete => 'd',
            Self::Expire => 'x',
            Self::Notified => 'k',
        }
    }
}

/// A parsed statement comment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SprigComment {
    /// `h`: the writer's HLC stamp.
    pub hlc: u64,
    /// `o`: the operation hint.
    pub op: OpHint,
    /// `l`: the MongoDB logical session id.
    pub lsid: Option<u128>,
    /// `t`: the MongoDB transaction number.
    pub txn_number: Option<u64>,
    /// `g`: the global transaction id of a distributed transaction.
    pub gtx: Option<u128>,
    /// `c`: the Redis command.
    pub command: Option<String>,
}

impl SprigComment {
    /// A comment with only the required fields.
    #[must_use]
    pub fn new(hlc: u64, op: OpHint) -> Self {
        Self {
            hlc,
            op,
            lsid: None,
            txn_number: None,
            gtx: None,
            command: None,
        }
    }

    /// The comment's text, as the gateway writes it.
    #[must_use]
    pub fn render(&self) -> String {
        let mut s = format!("/*sprig h={:016x} o={}", self.hlc, self.op.letter());
        if let Some(l) = self.lsid {
            let _ = write!(s, " l={l:032x}");
        }
        if let Some(t) = self.txn_number {
            let _ = write!(s, " t={t:x}");
        }
        if let Some(g) = self.gtx {
            let _ = write!(s, " g={g:032x}");
        }
        if let Some(c) = &self.command {
            let _ = write!(s, " c={c}");
        }
        s.push_str("*/");
        s
    }
}

/// A statement comment for a Redis command's write (`o=k`, RL51), with an
/// HLC in the feed's layout (seconds in the high 32 bits, a counter in the
/// low 32) that never goes backwards in this process.
#[must_use]
pub fn notified_comment() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static LAST: AtomicU64 = AtomicU64::new(0);
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let wall = secs << 32;
    let mut cur = LAST.load(Ordering::SeqCst);
    let h = loop {
        let next = wall.max(cur + 1);
        match LAST.compare_exchange(cur, next, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => break next,
            Err(c) => cur = c,
        }
    };
    SprigComment::new(h, OpHint::Notified).render()
}

/// Parses the comment at the start of an annotate text (leading spaces
/// allowed). `None` when the statement has no SprigDB comment (maintenance
/// statements) or the comment is malformed: such statements contribute no
/// stamp.
#[must_use]
pub fn parse_comment(text: &str) -> Option<SprigComment> {
    let t = text.trim_start();
    // RL51: an inner statement of a compound statement (`BEGIN NOT ATOMIC`)
    // is annotated without any comment before it, so writers mark those
    // right after the verb: `INSERT /*sprig ...*/ INTO ...`.
    let rest = t.strip_prefix("/*sprig ").or_else(|| {
        let verb = ["INSERT", "UPDATE", "DELETE", "REPLACE"]
            .into_iter()
            .find(|v| t.get(..v.len()).is_some_and(|h| h.eq_ignore_ascii_case(v)))?;
        let after = &t[verb.len()..];
        after
            .starts_with(|c: char| c.is_ascii_whitespace())
            .then(|| after.trim_start().strip_prefix("/*sprig "))
            .flatten()
    })?;
    let body = &rest[..rest.find("*/")?];
    let mut hlc = None;
    let mut op = None;
    let mut c = SprigComment::new(0, OpHint::Insert);
    for item in body.split_ascii_whitespace() {
        let (k, v) = item.split_once('=')?;
        match k {
            "h" if v.len() == 16 => hlc = Some(u64::from_str_radix(v, 16).ok()?),
            "o" => op = Some(OpHint::parse(v)?),
            "l" if v.len() == 32 => c.lsid = Some(u128::from_str_radix(v, 16).ok()?),
            "t" if !v.is_empty() && v.len() <= 16 => {
                c.txn_number = Some(u64::from_str_radix(v, 16).ok()?);
            }
            "g" if v.len() == 32 => c.gtx = Some(u128::from_str_radix(v, 16).ok()?),
            "c" if !v.is_empty() && v.len() <= 32 => c.command = Some(v.to_owned()),
            _ => return None,
        }
    }
    c.hlc = hlc?;
    c.op = op?;
    Some(c)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_what_it_renders() {
        let mut c = SprigComment::new(0x0000_6800_0000_0005, OpHint::Update);
        assert_eq!(c.render(), "/*sprig h=0000680000000005 o=u*/");
        assert_eq!(parse_comment(&c.render()), Some(c.clone()));
        c.lsid = Some(7);
        c.txn_number = Some(0x2a);
        c.gtx = Some(u128::MAX);
        c.command = Some("HSET".into());
        let text = format!("  {} UPDATE t SET a = 'x */ y'", c.render());
        assert_eq!(parse_comment(&text), Some(c));
    }

    #[test]
    fn reads_a_comment_after_the_verb() {
        let c = parse_comment("UPDATE /*sprig h=0000000100000002 o=k*/ t SET v = 1")
            .expect("a comment after the verb");
        assert_eq!((c.hlc, c.op), (0x1_0000_0002, OpHint::Notified));
        assert!(parse_comment("SELECT /*sprig h=0000000100000002 o=k*/ 1").is_none());
        assert!(parse_comment("UPDATEX /*sprig h=0000000100000002 o=k*/ t").is_none());
    }

    #[test]
    fn refuses_what_isnt_a_whole_comment() {
        for t in [
            "UPDATE t SET a = 1",
            "/* sprig h=0000000000000001 o=i*/",
            "/*sprig h=1 o=i*/",
            "/*sprig o=i*/",
            "/*sprig h=0000000000000001*/",
            "/*sprig h=0000000000000001 o=q*/",
            "/*sprig h=0000000000000001 o=i z=1*/",
            "/*sprig h=0000000000000001 o=i",
        ] {
            assert_eq!(parse_comment(t), None, "{t}");
        }
    }
}
