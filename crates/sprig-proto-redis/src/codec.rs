//! Requests and replies on the wire.
//!
//! Requests are RESP arrays of bulk strings ("multibulk") or inline
//! commands (a line of space-separated words, as `telnet` and
//! `redis-cli` pipes send). The decoder follows Redis 7.2's
//! `processMultibulkBuffer` and `processInlineBuffer`, with the same
//! protocol-error texts, and enforces `redis.max_bulk_bytes` and
//! `redis.max_command_bytes` before buffering.

use bytes::{Buf, Bytes, BytesMut};

use crate::num::d2string;

/// Longest inline request or multibulk header line Redis accepts
/// (`PROTO_INLINE_MAX_SIZE`).
pub const INLINE_MAX: usize = 64 * 1024;

/// Which RESP the connection speaks (`HELLO 2` / `HELLO 3`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Proto {
    /// RESP2, every connection's start.
    #[default]
    Resp2,
    /// RESP3.
    Resp3,
}

/// The RESP2 shape of a null.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullShape {
    /// `$-1`.
    Bulk,
    /// `*-1`.
    Array,
}

/// A reply. RESP3 shapes are downgraded for RESP2 by [`encode`].
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    /// `+OK`.
    Simple(Bytes),
    /// `-ERR ...`: the text after `-`, prefix included.
    Error(Bytes),
    /// `:1`.
    Integer(i64),
    /// `$3 foo`.
    Bulk(Bytes),
    /// `*n ...`.
    Array(Vec<Frame>),
    /// A null.
    Null(NullShape),
    /// RESP3 `#t`; RESP2 `:1` / `:0`.
    Boolean(bool),
    /// RESP3 `,1.5`; RESP2 a bulk string.
    Double(f64),
    /// A double already as text (Redis's "human" long doubles): RESP3
    /// `,<text>`, RESP2 a bulk string.
    DoubleText(Bytes),
    /// RESP3 `(123...`; RESP2 a bulk string.
    BigNumber(Bytes),
    /// RESP3 `=` with a three-letter format; RESP2 a bulk string.
    Verbatim([u8; 3], Bytes),
    /// RESP3 `%`; RESP2 a flat array.
    Map(Vec<(Frame, Frame)>),
    /// RESP3 `~`; RESP2 an array.
    Set(Vec<Frame>),
    /// RESP3 `>`; RESP2 an array.
    Push(Vec<Frame>),
}

impl Frame {
    /// `+OK`.
    #[must_use]
    pub fn ok() -> Self {
        Self::Simple(Bytes::from_static(b"OK"))
    }

    /// A simple string.
    #[must_use]
    pub fn simple(s: &str) -> Self {
        Self::Simple(Bytes::copy_from_slice(s.as_bytes()))
    }

    /// An error reply; `text` starts with its prefix (`ERR`, `WRONGTYPE`).
    #[must_use]
    pub fn error(text: impl Into<String>) -> Self {
        let mut t: String = text.into();
        // An error is one line.
        t.retain(|c| c != '\r' && c != '\n');
        Self::Error(Bytes::from(t))
    }

    /// `-ERR <text>`.
    #[must_use]
    pub fn err(text: &str) -> Self {
        Self::error(format!("ERR {text}"))
    }

    /// A bulk string.
    #[must_use]
    pub fn bulk(b: impl Into<Bytes>) -> Self {
        Self::Bulk(b.into())
    }

    /// A bulk string copied from a slice.
    #[must_use]
    pub fn bulk_from(b: &[u8]) -> Self {
        Self::Bulk(Bytes::copy_from_slice(b))
    }

    /// `$-1` (`_` in RESP3).
    #[must_use]
    pub const fn null() -> Self {
        Self::Null(NullShape::Bulk)
    }

    /// `*-1` (`_` in RESP3).
    #[must_use]
    pub const fn null_array() -> Self {
        Self::Null(NullShape::Array)
    }

    /// An optional bulk string.
    #[must_use]
    pub fn opt_bulk(b: Option<Vec<u8>>) -> Self {
        b.map_or(Self::null(), |v| Self::Bulk(Bytes::from(v)))
    }

    /// Whether this is an error reply.
    #[must_use]
    pub fn is_error(&self) -> bool {
        matches!(self, Self::Error(_))
    }
}

/// One request: the command name and its arguments, as sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    /// `args[0]` is the command name.
    pub args: Vec<Bytes>,
    /// Bytes this request took on the wire.
    pub wire_bytes: usize,
}

impl Command {
    /// A command from its parts (tests, scripts).
    #[must_use]
    pub fn from_parts<I, B>(parts: I) -> Self
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        let args: Vec<Bytes> = parts
            .into_iter()
            .map(|p| Bytes::copy_from_slice(p.as_ref()))
            .collect();
        let wire_bytes = args.iter().map(Bytes::len).sum();
        Self { args, wire_bytes }
    }

    /// The command name, lower case.
    #[must_use]
    pub fn name_lower(&self) -> String {
        self.args
            .first()
            .map(|a| String::from_utf8_lossy(a).to_ascii_lowercase())
            .unwrap_or_default()
    }
}

/// Size limits (`redis.max_bulk_bytes`, `redis.max_command_bytes`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Largest bulk string.
    pub max_bulk: usize,
    /// Largest whole command.
    pub max_command: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_bulk: 16 << 20,
            max_command: 64 << 20,
        }
    }
}

/// A protocol error: reply `-ERR Protocol error: <text>` and close.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Protocol error: {0}")]
pub struct DecodeError(pub String);

/// The request decoder of one connection.
#[derive(Debug, Default)]
pub struct Decoder {
    /// The limits in force.
    pub limits: Limits,
}

fn find_crlf(b: &[u8]) -> Option<usize> {
    b.windows(2).position(|w| w == b"\r\n")
}

fn perr(t: &str) -> DecodeError {
    DecodeError(t.to_owned())
}

impl Decoder {
    /// A decoder with `limits`.
    #[must_use]
    pub fn new(limits: Limits) -> Self {
        Self { limits }
    }

    /// The next whole request in `buf`, consuming it; `Ok(None)` when more
    /// bytes are needed. Empty requests (a blank inline line, `*0`) are
    /// skipped.
    ///
    /// # Errors
    /// [`DecodeError`], after which the connection closes.
    pub fn decode(&self, buf: &mut BytesMut) -> Result<Option<Command>, DecodeError> {
        loop {
            if buf.is_empty() {
                return Ok(None);
            }
            let r = if buf[0] == b'*' {
                self.multibulk(buf)?
            } else {
                Self::inline(buf)?
            };
            match r {
                Step::Need => return Ok(None),
                Step::Skip => {}
                Step::Done(c) => return Ok(Some(c)),
            }
        }
    }

    fn multibulk(&self, buf: &mut BytesMut) -> Result<Step, DecodeError> {
        let Some(nl) = find_crlf(buf) else {
            if buf.len() > INLINE_MAX {
                return Err(perr("too big mbulk count string"));
            }
            return Ok(Step::Need);
        };
        let n = std::str::from_utf8(&buf[1..nl])
            .ok()
            .and_then(|s| crate::num::parse_i64(s.as_bytes()));
        let Some(n) = n.filter(|n| *n <= i64::from(i32::MAX)) else {
            return Err(perr("invalid multibulk length"));
        };
        if n <= 0 {
            buf.advance(nl + 2);
            return Ok(Step::Skip);
        }
        if n > 1024 * 1024 {
            return Err(perr("invalid multibulk length"));
        }
        // Parse without consuming until the whole command is here.
        let mut pos = nl + 2;
        let mut spans = Vec::with_capacity(usize::try_from(n).unwrap_or(0));
        let mut total = 0usize;
        for _ in 0..n {
            if pos >= buf.len() {
                return Ok(Step::Need);
            }
            let Some(rel) = find_crlf(&buf[pos..]) else {
                if buf.len() - pos > INLINE_MAX {
                    return Err(perr("too big bulk count string"));
                }
                return Ok(Step::Need);
            };
            if buf[pos] != b'$' {
                return Err(DecodeError(format!(
                    "expected '$', got '{}'",
                    char::from(buf[pos])
                )));
            }
            let len = crate::num::parse_i64(&buf[pos + 1..pos + rel]);
            let Some(len) = len
                .filter(|l| *l >= 0)
                .and_then(|l| usize::try_from(l).ok())
                .filter(|l| *l <= self.limits.max_bulk)
            else {
                return Err(perr("invalid bulk length"));
            };
            total += len;
            if total > self.limits.max_command {
                return Err(perr("command too large"));
            }
            let start = pos + rel + 2;
            if buf.len() < start + len + 2 {
                return Ok(Step::Need);
            }
            spans.push((start, len));
            pos = start + len + 2;
        }
        let whole = buf.split_to(pos).freeze();
        let args = spans
            .into_iter()
            .map(|(s, l)| whole.slice(s..s + l))
            .collect();
        Ok(Step::Done(Command {
            args,
            wire_bytes: pos,
        }))
    }

    fn inline(buf: &mut BytesMut) -> Result<Step, DecodeError> {
        let Some(nl) = buf.iter().position(|b| *b == b'\n') else {
            if buf.len() > INLINE_MAX {
                return Err(perr("too big inline request"));
            }
            return Ok(Step::Need);
        };
        let line_end = if nl > 0 && buf[nl - 1] == b'\r' {
            nl - 1
        } else {
            nl
        };
        let args =
            split_args(&buf[..line_end]).ok_or_else(|| perr("unbalanced quotes in request"))?;
        buf.advance(nl + 1);
        if args.is_empty() {
            return Ok(Step::Skip);
        }
        Ok(Step::Done(Command {
            args: args.into_iter().map(Bytes::from).collect(),
            wire_bytes: nl + 1,
        }))
    }
}

enum Step {
    Need,
    Skip,
    Done(Command),
}

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Redis's `sdssplitargs`: words split on spaces, `"..."` with `\n \r \t
/// \b \a \\ \" \xHH` escapes, `'...'` with `\'`; a closing quote must be
/// followed by a space or the end. `None` for unbalanced quotes.
#[must_use]
pub fn split_args(line: &[u8]) -> Option<Vec<Vec<u8>>> {
    // Redis works on C strings: a NUL ends the line.
    let line = &line[..line.iter().position(|b| *b == 0).unwrap_or(line.len())];
    let mut out = Vec::new();
    let mut i = 0;
    let n = line.len();
    loop {
        while i < n && line[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= n {
            return Some(out);
        }
        let mut cur = Vec::new();
        let mut inq = false;
        let mut in_single = false;
        loop {
            if inq {
                if i >= n {
                    return None;
                }
                if line[i] == b'\\'
                    && i + 3 < n
                    && line[i + 1] == b'x'
                    && hex_val(line[i + 2]).is_some()
                    && hex_val(line[i + 3]).is_some()
                {
                    cur.push(hex_val(line[i + 2])? * 16 + hex_val(line[i + 3])?);
                    i += 3;
                } else if line[i] == b'\\' && i + 1 < n {
                    i += 1;
                    cur.push(match line[i] {
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        b'b' => 8,
                        b'a' => 7,
                        c => c,
                    });
                } else if line[i] == b'"' {
                    if i + 1 < n && !line[i + 1].is_ascii_whitespace() {
                        return None;
                    }
                    i += 1;
                    break;
                } else {
                    cur.push(line[i]);
                }
            } else if in_single {
                if i >= n {
                    return None;
                }
                if line[i] == b'\\' && i + 1 < n && line[i + 1] == b'\'' {
                    i += 1;
                    cur.push(b'\'');
                } else if line[i] == b'\'' {
                    if i + 1 < n && !line[i + 1].is_ascii_whitespace() {
                        return None;
                    }
                    i += 1;
                    break;
                } else {
                    cur.push(line[i]);
                }
            } else {
                if i >= n {
                    break;
                }
                match line[i] {
                    b' ' | b'\n' | b'\r' | b'\t' => {
                        break;
                    }
                    b'"' => inq = true,
                    b'\'' => in_single = true,
                    c => cur.push(c),
                }
            }
            i += 1;
        }
        out.push(cur);
    }
}

fn line(out: &mut BytesMut, prefix: u8, body: &[u8]) {
    out.extend_from_slice(&[prefix]);
    out.extend_from_slice(body);
    out.extend_from_slice(b"\r\n");
}

fn bulk(out: &mut BytesMut, b: &[u8]) {
    line(out, b'$', b.len().to_string().as_bytes());
    out.extend_from_slice(b);
    out.extend_from_slice(b"\r\n");
}

/// Appends `f` to `out` in `proto`, downgrading RESP3 shapes for RESP2.
pub fn encode(f: &Frame, proto: Proto, out: &mut BytesMut) {
    let r3 = proto == Proto::Resp3;
    match f {
        Frame::Simple(s) => line(out, b'+', s),
        Frame::Error(s) => line(out, b'-', s),
        Frame::Integer(i) => line(out, b':', i.to_string().as_bytes()),
        Frame::Bulk(b) => bulk(out, b),
        Frame::Array(items) => {
            line(out, b'*', items.len().to_string().as_bytes());
            for i in items {
                encode(i, proto, out);
            }
        }
        Frame::Null(shape) => {
            if r3 {
                out.extend_from_slice(b"_\r\n");
            } else {
                match shape {
                    NullShape::Bulk => out.extend_from_slice(b"$-1\r\n"),
                    NullShape::Array => out.extend_from_slice(b"*-1\r\n"),
                }
            }
        }
        Frame::Boolean(b) => {
            if r3 {
                out.extend_from_slice(if *b { b"#t\r\n" } else { b"#f\r\n" });
            } else {
                out.extend_from_slice(if *b { b":1\r\n" } else { b":0\r\n" });
            }
        }
        Frame::Double(d) => {
            let t = d2string(*d);
            if r3 {
                line(out, b',', t.as_bytes());
            } else {
                bulk(out, t.as_bytes());
            }
        }
        Frame::DoubleText(t) => {
            if r3 {
                line(out, b',', t);
            } else {
                bulk(out, t);
            }
        }
        Frame::BigNumber(n) => {
            if r3 {
                line(out, b'(', n);
            } else {
                bulk(out, n);
            }
        }
        Frame::Verbatim(fmt, body) => {
            if r3 {
                line(out, b'=', (body.len() + 4).to_string().as_bytes());
                out.extend_from_slice(fmt);
                out.extend_from_slice(b":");
                out.extend_from_slice(body);
                out.extend_from_slice(b"\r\n");
            } else {
                bulk(out, body);
            }
        }
        Frame::Map(pairs) => {
            if r3 {
                line(out, b'%', pairs.len().to_string().as_bytes());
            } else {
                line(out, b'*', (pairs.len() * 2).to_string().as_bytes());
            }
            for (k, v) in pairs {
                encode(k, proto, out);
                encode(v, proto, out);
            }
        }
        Frame::Set(items) | Frame::Push(items) => {
            let p = match (f, r3) {
                (Frame::Set(_), true) => b'~',
                (Frame::Push(_), true) => b'>',
                _ => b'*',
            };
            line(out, p, items.len().to_string().as_bytes());
            for i in items {
                encode(i, proto, out);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn dec(bytes: &[u8]) -> Result<Vec<Vec<Vec<u8>>>, DecodeError> {
        let d = Decoder::default();
        let mut b = BytesMut::from(bytes);
        let mut out = Vec::new();
        while let Some(c) = d.decode(&mut b)? {
            out.push(c.args.iter().map(|a| a.to_vec()).collect());
        }
        Ok(out)
    }

    #[test]
    fn multibulk_and_inline() {
        let r = dec(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\nPING\r\nset a \"b c\\x41\"\r\n").unwrap();
        assert_eq!(
            r,
            vec![
                vec![b"GET".to_vec(), b"k".to_vec()],
                vec![b"PING".to_vec()],
                vec![b"set".to_vec(), b"a".to_vec(), b"b cA".to_vec()],
            ]
        );
        assert_eq!(dec(b"*0\r\n\r\n").unwrap(), Vec::<Vec<Vec<u8>>>::new());
    }

    #[test]
    fn partial_input_waits() {
        let d = Decoder::default();
        let full = b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n";
        for cut in 0..full.len() {
            let mut b = BytesMut::from(&full[..cut]);
            assert_eq!(d.decode(&mut b).unwrap(), None, "cut {cut}");
        }
    }

    #[test]
    fn protocol_errors_have_redis_texts() {
        assert_eq!(
            dec(b"*x\r\n").unwrap_err().to_string(),
            "Protocol error: invalid multibulk length"
        );
        assert_eq!(
            dec(b"*1\r\n+x\r\n").unwrap_err().to_string(),
            "Protocol error: expected '$', got '+'"
        );
        assert_eq!(
            dec(b"*1\r\n$-5\r\n").unwrap_err().to_string(),
            "Protocol error: invalid bulk length"
        );
        assert_eq!(
            dec(b"set \"a\r\n").unwrap_err().to_string(),
            "Protocol error: unbalanced quotes in request"
        );
        let d = Decoder::new(Limits {
            max_bulk: 4,
            max_command: 100,
        });
        let mut b = BytesMut::from(&b"*1\r\n$5\r\nhello\r\n"[..]);
        assert!(d.decode(&mut b).is_err());
    }

    #[test]
    fn resp2_downgrades() {
        let mut out = BytesMut::new();
        let f = Frame::Map(vec![(Frame::bulk_from(b"a"), Frame::Double(1.5))]);
        encode(&f, Proto::Resp2, &mut out);
        assert_eq!(&out[..], b"*2\r\n$1\r\na\r\n$3\r\n1.5\r\n");
        out.clear();
        encode(&f, Proto::Resp3, &mut out);
        assert_eq!(&out[..], b"%1\r\n$1\r\na\r\n,1.5\r\n");
        out.clear();
        encode(&Frame::null_array(), Proto::Resp2, &mut out);
        encode(&Frame::null(), Proto::Resp3, &mut out);
        encode(&Frame::Boolean(true), Proto::Resp2, &mut out);
        assert_eq!(&out[..], b"*-1\r\n_\r\n:1\r\n");
    }

    proptest! {
        // Any request split at any point decodes to the same commands.
        #[test]
        fn decode_is_split_invariant(
            cmds in prop::collection::vec(prop::collection::vec(prop::collection::vec(any::<u8>(), 0..20), 1..5), 1..5),
            cut in 0usize..400,
        ) {
            let mut wire = Vec::new();
            for c in &cmds {
                wire.extend_from_slice(format!("*{}\r\n", c.len()).as_bytes());
                for a in c {
                    wire.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                    wire.extend_from_slice(a);
                    wire.extend_from_slice(b"\r\n");
                }
            }
            let d = Decoder::default();
            let cut = cut.min(wire.len());
            let mut b = BytesMut::from(&wire[..cut]);
            let mut got = Vec::new();
            while let Some(c) = d.decode(&mut b).unwrap() { got.push(c.args); }
            b.extend_from_slice(&wire[cut..]);
            while let Some(c) = d.decode(&mut b).unwrap() { got.push(c.args); }
            let got: Vec<Vec<Vec<u8>>> = got.into_iter().map(|c| c.into_iter().map(|a| a.to_vec()).collect()).collect();
            prop_assert_eq!(got, cmds);
        }

        // Garbage never panics.
        #[test]
        fn garbage_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..300)) {
            let d = Decoder::default();
            let mut b = BytesMut::from(&bytes[..]);
            for _ in 0..50 {
                match d.decode(&mut b) { Ok(Some(_)) => {}, _ => break }
            }
        }
    }
}
