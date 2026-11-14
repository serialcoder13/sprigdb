//! The native Redis client (`--protocol redis`): RESP3 after
//! `HELLO 3 AUTH user password` (RESP2 replies are read too). Replies come
//! back as JSON documents: strings, numbers, arrays, maps as objects; an
//! error reply is the statement's error.

use bytes::{Buf, BytesMut};
use serde_json::{Map, Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::error::{Result, ShellCode, ShellError};
use crate::lang::redis_argv;
use crate::model::{EndInfo, ResultEvent, Sink};
use crate::native::{NativeTarget, split_addr};

/// A RESP reply.
#[derive(Debug, Clone, PartialEq)]
pub enum Reply {
    /// `+`, `=`.
    Simple(String),
    /// `-`, `!`.
    Error(String),
    /// `:`.
    Int(i64),
    /// `$`; `None` for the RESP2 null.
    Bulk(Option<Vec<u8>>),
    /// `*`, `~`, `>`; `None` for the RESP2 null array.
    Array(Option<Vec<Reply>>),
    /// `%`, `|`.
    Map(Vec<(Reply, Reply)>),
    /// `,`.
    Double(f64),
    /// `#`.
    Bool(bool),
    /// `_`.
    Null,
    /// `(`.
    Big(String),
}

fn line(buf: &[u8], at: usize) -> Option<(&[u8], usize)> {
    let rest = buf.get(at..)?;
    let i = rest.windows(2).position(|w| w == b"\r\n")?;
    Some((&rest[..i], at + i + 2))
}

fn bad(m: &str) -> ShellError {
    ShellError::shell(ShellCode::Unreachable, format!("bad RESP reply: {m}"))
}

/// Parses one reply at `at`; `Ok(None)` when more bytes are needed.
///
/// # Errors
/// A malformed reply.
#[allow(clippy::many_single_char_names)]
pub fn parse(buf: &[u8], at: usize) -> Result<Option<(Reply, usize)>> {
    let Some(&t) = buf.get(at) else {
        return Ok(None);
    };
    let Some((l, next)) = line(buf, at + 1) else {
        return Ok(None);
    };
    let text = || String::from_utf8_lossy(l).into_owned();
    let num = || -> Result<i64> {
        std::str::from_utf8(l)
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| bad("number"))
    };
    Ok(Some(match t {
        b'+' => (Reply::Simple(text()), next),
        b'-' => (Reply::Error(text()), next),
        b':' => (Reply::Int(num()?), next),
        b',' => (Reply::Double(text().parse().unwrap_or(f64::NAN)), next),
        b'#' => (Reply::Bool(l == b"t"), next),
        b'_' => (Reply::Null, next),
        b'(' => (Reply::Big(text()), next),
        b'$' | b'=' | b'!' => {
            let n = num()?;
            if n < 0 {
                return Ok(Some((Reply::Bulk(None), next)));
            }
            let n = usize::try_from(n).map_err(|_| bad("length"))?;
            if buf.len() < next + n + 2 {
                return Ok(None);
            }
            let body = buf[next..next + n].to_vec();
            let end = next + n + 2;
            match t {
                b'!' => (
                    Reply::Error(String::from_utf8_lossy(&body).into_owned()),
                    end,
                ),
                // Verbatim strings start with "txt:".
                b'=' => (
                    Reply::Simple(
                        String::from_utf8_lossy(body.get(4..).unwrap_or_default()).into_owned(),
                    ),
                    end,
                ),
                _ => (Reply::Bulk(Some(body)), end),
            }
        }
        b'*' | b'~' | b'>' | b'%' | b'|' => {
            let n = num()?;
            if n < 0 {
                return Ok(Some((Reply::Array(None), next)));
            }
            let count = usize::try_from(n).map_err(|_| bad("length"))?
                * if matches!(t, b'%' | b'|') { 2 } else { 1 };
            let mut items = Vec::with_capacity(count.min(1024));
            let mut pos = next;
            for _ in 0..count {
                match parse(buf, pos)? {
                    Some((r, p)) => {
                        items.push(r);
                        pos = p;
                    }
                    None => return Ok(None),
                }
            }
            if t == b'|' {
                // An attribute: skip it and read the reply it decorates.
                return parse(buf, pos);
            }
            if t == b'%' {
                let mut pairs = Vec::new();
                let mut it = items.into_iter();
                while let (Some(k), Some(v)) = (it.next(), it.next()) {
                    pairs.push((k, v));
                }
                (Reply::Map(pairs), pos)
            } else {
                (Reply::Array(Some(items)), pos)
            }
        }
        _ => return Err(bad(&format!("type byte {t:#x}"))),
    }))
}

/// A reply as JSON.
#[must_use]
pub fn to_json(r: &Reply) -> Value {
    match r {
        Reply::Simple(s) | Reply::Big(s) => Value::String(s.clone()),
        Reply::Error(e) => json!({ "error": e }),
        Reply::Int(i) => json!(i),
        Reply::Bulk(None) | Reply::Array(None) | Reply::Null => Value::Null,
        Reply::Bulk(Some(b)) => Value::String(String::from_utf8_lossy(b).into_owned()),
        Reply::Array(Some(a)) => Value::Array(a.iter().map(to_json).collect()),
        Reply::Map(m) => {
            let mut o = Map::new();
            for (k, v) in m {
                let key = match to_json(k) {
                    Value::String(s) => s,
                    other => other.to_string(),
                };
                o.insert(key, to_json(v));
            }
            Value::Object(o)
        }
        Reply::Double(d) => json!(d),
        Reply::Bool(b) => Value::Bool(*b),
    }
}

/// Encodes a command as a RESP array of bulk strings.
#[must_use]
pub fn encode(argv: &[String]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", argv.len()).into_bytes();
    for a in argv {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// A Redis connection.
pub struct RedisBackend {
    target: NativeTarget,
    stream: Option<TcpStream>,
    buf: BytesMut,
}

impl RedisBackend {
    async fn command(&mut self, argv: &[String]) -> Result<Reply> {
        let s = self
            .stream
            .as_mut()
            .ok_or_else(|| ShellError::shell(ShellCode::Unreachable, "not connected"))?;
        let io = |e: std::io::Error| ShellError::shell(ShellCode::Unreachable, e.to_string());
        s.write_all(&encode(argv)).await.map_err(io)?;
        loop {
            // Push messages (pub/sub) arrive as `>`; a command's reply is
            // the first that isn't one.
            while let Some((r, used)) = parse(&self.buf, 0)? {
                let push = self.buf.first() == Some(&b'>');
                self.buf.advance(used);
                if !push {
                    return Ok(r);
                }
            }
            if s.read_buf(&mut self.buf).await.map_err(io)? == 0 {
                return Err(ShellError::shell(
                    ShellCode::Unreachable,
                    "connection closed",
                ));
            }
        }
    }

    async fn open(&mut self) -> Result<()> {
        let (host, port) = split_addr(&self.target.addr, 6379)?;
        let tcp = TcpStream::connect((host.as_str(), port))
            .await
            .map_err(|e| {
                ShellError::shell(ShellCode::Unreachable, format!("{}: {e}", self.target.addr))
            })?;
        let _ = tcp.set_nodelay(true);
        self.stream = Some(tcp);
        self.buf.clear();
        let mut hello = vec!["HELLO".to_owned(), "3".to_owned()];
        if !self.target.password.is_empty() {
            hello.extend([
                "AUTH".to_owned(),
                self.target.user.clone(),
                self.target.password.clone(),
            ]);
        }
        hello.extend(["SETNAME".to_owned(), "sprig".to_owned()]);
        if let Reply::Error(e) = self.command(&hello).await? {
            return Err(ShellError::server(1045, "", e));
        }
        Ok(())
    }

    /// Connects, authenticates and switches to RESP3.
    ///
    /// # Errors
    /// 39987 or the server's refusal.
    pub async fn connect(target: NativeTarget) -> Result<Self> {
        let mut b = Self {
            target,
            stream: None,
            buf: BytesMut::new(),
        };
        b.open().await?;
        Ok(b)
    }

    /// The address.
    #[must_use]
    pub fn addr(&self) -> &str {
        &self.target.addr
    }

    pub(crate) async fn reconnect(&mut self) -> Result<()> {
        self.stream = None;
        self.open().await
    }

    pub(crate) async fn run(&mut self, stmt: &str, sink: &mut dyn Sink) -> Result<()> {
        if self.stream.is_none() {
            self.open().await?;
        }
        let argv = redis_argv(stmt)?;
        if argv.is_empty() {
            return Ok(());
        }
        let io = |e: std::io::Error| ShellError::shell(ShellCode::Cancelled, e.to_string());
        match self.command(&argv).await? {
            Reply::Error(e) => {
                let code = e
                    .split_whitespace()
                    .find_map(|w| w.parse::<u32>().ok().filter(|n| (39000..40000).contains(n)))
                    .unwrap_or(0);
                Err(ShellError::server(code, "", e))
            }
            r => {
                sink.event(ResultEvent::Doc(to_json(&r))).map_err(io)?;
                sink.event(ResultEvent::End(EndInfo::default())).map_err(io)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_resp2_and_resp3() {
        let wire = b"+OK\r\n:42\r\n$5\r\nhello\r\n$-1\r\n*2\r\n$1\r\na\r\n:1\r\n%1\r\n+k\r\n,1.5\r\n#t\r\n_\r\n-ERR nope\r\n";
        let mut at = 0;
        let mut got = Vec::new();
        while let Some((r, n)) = parse(wire, at).expect("parse") {
            got.push(to_json(&r));
            at = n;
        }
        assert_eq!(
            got,
            vec![
                json!("OK"),
                json!(42),
                json!("hello"),
                Value::Null,
                json!(["a", 1]),
                json!({"k": 1.5}),
                json!(true),
                Value::Null,
                json!({"error": "ERR nope"})
            ]
        );
        assert_eq!(parse(b"$5\r\nhel", 0).expect("partial"), None);
        assert_eq!(
            encode(&["GET".into(), "k".into()]),
            b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"
        );
    }
}
