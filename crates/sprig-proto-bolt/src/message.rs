//! Bolt framing, handshake and messages.
//!
//! - **Handshake.** The client sends the magic `60 60 B0 17` and four
//!   4-byte proposals `00 range minor major`; each covers `major.minor`
//!   down to `major.(minor - range)`. The server answers the highest version
//!   it supports inside any proposal, or `00 00 00 00` and closes.
//! - **Framing.** A message is chunks of a 2-byte big-endian length (at
//!   most 65,535) and that many bytes, ended by an empty chunk `00 00`. An
//!   empty chunk between messages is a keep-alive.
//! - **Messages** are PackStream structures; the tag is the message type.

use bytes::{Buf, BufMut, BytesMut};

use crate::packstream::{self, PackError, PackValue};

/// The handshake's magic preamble.
pub const MAGIC: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];
/// Largest chunk payload.
pub const MAX_CHUNK: usize = 65_535;

/// A Bolt protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BoltVersion {
    /// Major version.
    pub major: u8,
    /// Minor version.
    pub minor: u8,
}

impl BoltVersion {
    /// `major.minor`.
    #[must_use]
    pub const fn new(major: u8, minor: u8) -> Self {
        Self { major, minor }
    }

    /// The handshake answer for this version.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 4] {
        [0, 0, self.minor, self.major]
    }

    /// Parses `5.4`.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        let (a, b) = s.trim().split_once('.')?;
        Some(Self::new(a.parse().ok()?, b.parse().ok()?))
    }
}

impl std::fmt::Display for BoltVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

/// The versions this build speaks, highest first: 5.0 to 5.8 (GQL status
/// objects in summaries from 5.5, GQL errors from 5.7, the database
/// echoed on `RUN` from 5.8).
pub const SUPPORTED: [BoltVersion; 9] = [
    BoltVersion::new(5, 8),
    BoltVersion::new(5, 7),
    BoltVersion::new(5, 6),
    BoltVersion::new(5, 5),
    BoltVersion::new(5, 4),
    BoltVersion::new(5, 3),
    BoltVersion::new(5, 2),
    BoltVersion::new(5, 1),
    BoltVersion::new(5, 0),
];

/// Picks the highest version in `ours` (highest first) that any proposal
/// covers, or `None` (answer `00 00 00 00`). Proposals are tried in the
/// client's order of preference, as the specification asks: the first
/// proposal with a version in common wins.
#[must_use]
pub fn negotiate(proposals: &[[u8; 4]; 4], ours: &[BoltVersion]) -> Option<BoltVersion> {
    for p in proposals {
        let (range, minor, major) = (p[1], p[2], p[3]);
        if major == 0 && minor == 0 {
            continue;
        }
        let low = minor.saturating_sub(range);
        if let Some(v) = ours
            .iter()
            .copied()
            .filter(|v| v.major == major && v.minor <= minor && v.minor >= low)
            .max()
        {
            return Some(v);
        }
    }
    None
}

/// Appends `payload` as one chunked message.
pub fn write_chunked(payload: &[u8], out: &mut BytesMut) {
    for c in payload.chunks(MAX_CHUNK) {
        out.put_u16(u16::try_from(c.len()).unwrap_or(u16::MAX));
        out.put_slice(c);
    }
    out.put_u16(0);
}

/// Reassembles chunked messages.
#[derive(Debug, Default)]
pub struct Dechunker {
    current: Vec<u8>,
    /// Largest message accepted, in bytes.
    pub max_message: usize,
}

impl Dechunker {
    /// A dechunker accepting messages up to `max_message` bytes.
    #[must_use]
    pub fn new(max_message: usize) -> Self {
        Self {
            current: Vec::new(),
            max_message,
        }
    }

    /// Takes the next complete message out of `buf`, consuming what it
    /// read; keep-alive chunks are skipped.
    ///
    /// # Errors
    /// A message over the size limit.
    pub fn next(&mut self, buf: &mut BytesMut) -> Result<Option<Vec<u8>>, PackError> {
        loop {
            if buf.len() < 2 {
                return Ok(None);
            }
            let n = usize::from(u16::from_be_bytes([buf[0], buf[1]]));
            if n == 0 {
                buf.advance(2);
                if self.current.is_empty() {
                    continue; // keep-alive (NOOP)
                }
                return Ok(Some(std::mem::take(&mut self.current)));
            }
            if buf.len() < 2 + n {
                return Ok(None);
            }
            if self.current.len() + n > self.max_message {
                return Err(PackError(format!(
                    "message larger than {} bytes",
                    self.max_message
                )));
            }
            self.current.extend_from_slice(&buf[2..2 + n]);
            buf.advance(2 + n);
        }
    }
}

/// A client request.
#[derive(Debug, Clone, PartialEq)]
pub enum Request {
    /// `HELLO` (`01`): user agent, routing context and, before 5.1,
    /// credentials.
    Hello(PackValue),
    /// `LOGON` (`6A`), 5.1+: `scheme`, `principal`, `credentials`.
    Logon(PackValue),
    /// `LOGOFF` (`6B`).
    Logoff,
    /// `GOODBYE` (`02`).
    Goodbye,
    /// `RESET` (`0F`).
    Reset,
    /// `RUN` (`10`): query text, parameters, extra.
    Run {
        /// The text.
        query: String,
        /// Parameters, a map.
        params: PackValue,
        /// `bookmarks`, `tx_timeout`, `tx_metadata`, `mode`, `db`,
        /// `imp_user`, notification filters.
        extra: PackValue,
    },
    /// `BEGIN` (`11`).
    Begin(PackValue),
    /// `COMMIT` (`12`).
    Commit,
    /// `ROLLBACK` (`13`).
    Rollback,
    /// `DISCARD` (`2F`): `n`, `qid`.
    Discard(PackValue),
    /// `PULL` (`3F`): `n`, `qid`.
    Pull(PackValue),
    /// `ROUTE` (`66`): routing context, bookmarks, extra (`db`,
    /// `imp_user`).
    Route {
        /// The routing context.
        routing: PackValue,
        /// Bookmarks.
        bookmarks: PackValue,
        /// Extra.
        extra: PackValue,
    },
    /// `TELEMETRY` (`54`), 5.4+: dropped.
    Telemetry(PackValue),
}

impl Request {
    /// The message name, for logs and metrics.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Request::Hello(_) => "HELLO",
            Request::Logon(_) => "LOGON",
            Request::Logoff => "LOGOFF",
            Request::Goodbye => "GOODBYE",
            Request::Reset => "RESET",
            Request::Run { .. } => "RUN",
            Request::Begin(_) => "BEGIN",
            Request::Commit => "COMMIT",
            Request::Rollback => "ROLLBACK",
            Request::Discard(_) => "DISCARD",
            Request::Pull(_) => "PULL",
            Request::Route { .. } => "ROUTE",
            Request::Telemetry(_) => "TELEMETRY",
        }
    }

    /// Decodes one message.
    ///
    /// # Errors
    /// Malformed PackStream, an unknown tag or wrong fields.
    pub fn decode(msg: &[u8]) -> Result<Self, PackError> {
        let PackValue::Struct { tag, fields } = packstream::decode(msg)? else {
            return Err(PackError("a message must be a structure".into()));
        };
        let mut f = fields.into_iter();
        let mut next = |what: &str| {
            f.next()
                .ok_or_else(|| PackError(format!("message {tag:#04X} lacks its {what}")))
        };
        let map_or_empty = |v: PackValue| match v {
            PackValue::Null => Ok(PackValue::Map(vec![])),
            m @ PackValue::Map(_) => Ok(m),
            other => Err(PackError(format!("expected a map, got {other:?}"))),
        };
        Ok(match tag {
            0x01 => Request::Hello(map_or_empty(next("extra")?)?),
            0x6A => Request::Logon(map_or_empty(next("auth")?)?),
            0x6B => Request::Logoff,
            0x02 => Request::Goodbye,
            0x0F => Request::Reset,
            0x10 => {
                let query = match next("query")? {
                    PackValue::String(s) => s,
                    other => return Err(PackError(format!("RUN query is {other:?}"))),
                };
                let params = map_or_empty(next("parameters")?)?;
                let extra = map_or_empty(next("extra").unwrap_or(PackValue::Null))?;
                Request::Run {
                    query,
                    params,
                    extra,
                }
            }
            0x11 => Request::Begin(map_or_empty(next("extra").unwrap_or(PackValue::Null))?),
            0x12 => Request::Commit,
            0x13 => Request::Rollback,
            0x2F => Request::Discard(map_or_empty(next("extra").unwrap_or(PackValue::Null))?),
            0x3F => Request::Pull(map_or_empty(next("extra").unwrap_or(PackValue::Null))?),
            0x66 => {
                let routing = map_or_empty(next("routing")?)?;
                let bookmarks = next("bookmarks")?;
                let extra = match next("extra").unwrap_or(PackValue::Null) {
                    // 4.3 sent the database name alone.
                    PackValue::String(db) => PackValue::map([("db", PackValue::String(db))]),
                    v => map_or_empty(v)?,
                };
                Request::Route {
                    routing,
                    bookmarks,
                    extra,
                }
            }
            0x54 => Request::Telemetry(next("api").unwrap_or(PackValue::Null)),
            t => return Err(PackError(format!("unknown message type {t:#04X}"))),
        })
    }

    /// Encodes the message (clients and tests).
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let (tag, fields) = match self {
            Request::Hello(m) => (0x01, vec![m.clone()]),
            Request::Logon(m) => (0x6A, vec![m.clone()]),
            Request::Logoff => (0x6B, vec![]),
            Request::Goodbye => (0x02, vec![]),
            Request::Reset => (0x0F, vec![]),
            Request::Run {
                query,
                params,
                extra,
            } => (
                0x10,
                vec![PackValue::str(query.clone()), params.clone(), extra.clone()],
            ),
            Request::Begin(m) => (0x11, vec![m.clone()]),
            Request::Commit => (0x12, vec![]),
            Request::Rollback => (0x13, vec![]),
            Request::Discard(m) => (0x2F, vec![m.clone()]),
            Request::Pull(m) => (0x3F, vec![m.clone()]),
            Request::Route {
                routing,
                bookmarks,
                extra,
            } => (
                0x66,
                vec![routing.clone(), bookmarks.clone(), extra.clone()],
            ),
            Request::Telemetry(a) => (0x54, vec![a.clone()]),
        };
        let mut b = BytesMut::new();
        packstream::encode(&PackValue::Struct { tag, fields }, &mut b);
        b.to_vec()
    }
}

/// A server response.
#[derive(Debug, Clone, PartialEq)]
pub enum Response {
    /// `SUCCESS` (`70`) with its metadata map.
    Success(PackValue),
    /// `RECORD` (`71`): one row.
    Record(Vec<PackValue>),
    /// `IGNORED` (`7E`).
    Ignored,
    /// `FAILURE` (`7F`): a Neo4j status code and a message.
    Failure {
        /// `Neo.ClientError.Statement.SyntaxError` and the like.
        code: String,
        /// The message.
        message: String,
    },
    /// `FAILURE` (`7F`) from Bolt 5.7 on: GQL's error fields (`gql_status`,
    /// `description`, `diagnostic_record`) beside the Neo4j code, now
    /// named `neo4j_code`.
    GqlFailure {
        /// The Neo4j status code.
        code: String,
        /// The message.
        message: String,
        /// The GQLSTATUS (`42001`, `50N42`, ...).
        gql_status: String,
        /// Its description.
        description: String,
        /// `_classification` and the like; `None`: left out.
        diagnostic: Option<PackValue>,
    },
}

impl Response {
    /// Encodes the response as one PackStream structure.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let v = match self {
            Response::Success(m) => PackValue::Struct {
                tag: 0x70,
                fields: vec![m.clone()],
            },
            Response::Record(r) => PackValue::Struct {
                tag: 0x71,
                fields: vec![PackValue::List(r.clone())],
            },
            Response::Ignored => PackValue::Struct {
                tag: 0x7E,
                fields: vec![],
            },
            Response::Failure { code, message } => PackValue::Struct {
                tag: 0x7F,
                fields: vec![PackValue::map([
                    ("code", PackValue::str(code.clone())),
                    ("message", PackValue::str(message.clone())),
                ])],
            },
            Response::GqlFailure {
                code,
                message,
                gql_status,
                description,
                diagnostic,
            } => {
                let mut m = vec![
                    ("neo4j_code", PackValue::str(code.clone())),
                    ("message", PackValue::str(message.clone())),
                    ("gql_status", PackValue::str(gql_status.clone())),
                    ("description", PackValue::str(description.clone())),
                ];
                if let Some(d) = diagnostic {
                    m.push(("diagnostic_record", d.clone()));
                }
                PackValue::Struct {
                    tag: 0x7F,
                    fields: vec![PackValue::map(m)],
                }
            }
        };
        let mut b = BytesMut::new();
        packstream::encode(&v, &mut b);
        b.to_vec()
    }

    /// Decodes a response (clients and tests).
    ///
    /// # Errors
    /// Malformed input or an unknown tag.
    pub fn decode(msg: &[u8]) -> Result<Self, PackError> {
        let PackValue::Struct { tag, fields } = packstream::decode(msg)? else {
            return Err(PackError("a message must be a structure".into()));
        };
        let first = fields.into_iter().next().unwrap_or(PackValue::Null);
        Ok(match tag {
            0x70 => Response::Success(first),
            0x71 => match first {
                PackValue::List(l) => Response::Record(l),
                other => return Err(PackError(format!("RECORD of {other:?}"))),
            },
            0x7E => Response::Ignored,
            0x7F => Response::Failure {
                code: first
                    .get("code")
                    .or_else(|| first.get("neo4j_code"))
                    .and_then(PackValue::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                message: first
                    .get("message")
                    .and_then(PackValue::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            },
            t => return Err(PackError(format!("unknown response type {t:#04X}"))),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        s.split_whitespace()
            .map(|h| u8::from_str_radix(h, 16).expect("hex"))
            .collect()
    }

    #[test]
    fn negotiation_picks_the_highest_common_version() {
        let ours = &SUPPORTED;
        // Java driver 5.x: 5.4..5.0 as one range, then 4.4..4.2.
        let p = [[0, 4, 4, 5], [0, 2, 4, 4], [0, 0, 0, 0], [0, 0, 0, 0]];
        assert_eq!(negotiate(&p, ours), Some(BoltVersion::new(5, 4)));
        // A newer client: 5.8 down to 5.0 (5.8 itself).
        let p = [[0, 8, 8, 5], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]];
        assert_eq!(negotiate(&p, ours), Some(BoltVersion::new(5, 8)));
        // Only 5.8..5.6: 5.8.
        let p = [[0, 2, 8, 5], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]];
        assert_eq!(negotiate(&p, ours), Some(BoltVersion::new(5, 8)));
        // 5.7 down to 5.5 only: 5.7.
        let p = [[0, 2, 7, 5], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]];
        assert_eq!(negotiate(&p, ours), Some(BoltVersion::new(5, 7)));
        // 6.0 only: nothing in common.
        let p = [[0, 0, 0, 6], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]];
        assert_eq!(negotiate(&p, ours), None);
        // 4.x only: refused (00 00 00 00).
        let p = [[0, 0, 4, 4], [0, 0, 3, 4], [0, 0, 0, 0], [0, 0, 0, 0]];
        assert_eq!(negotiate(&p, ours), None);
        // The manifest proposal (00 00 01 FF) of 5.8+ drivers is skipped.
        let p = [[0, 0, 1, 0xFF], [0, 3, 3, 5], [0, 0, 0, 0], [0, 0, 0, 0]];
        assert_eq!(negotiate(&p, ours), Some(BoltVersion::new(5, 3)));
        // The client's order wins over the highest number.
        let p = [[0, 0, 1, 5], [0, 0, 4, 5], [0, 0, 0, 0], [0, 0, 0, 0]];
        assert_eq!(negotiate(&p, ours), Some(BoltVersion::new(5, 1)));
        assert_eq!(BoltVersion::new(5, 4).to_bytes(), [0, 0, 4, 5]);
    }

    #[test]
    fn gql_failures_name_the_neo4j_code() {
        let f = Response::GqlFailure {
            code: "Neo.ClientError.Statement.SyntaxError".into(),
            message: "bad".into(),
            gql_status: "42001".into(),
            description: "error: syntax error or access rule violation - invalid syntax".into(),
            diagnostic: Some(PackValue::map([(
                "_classification",
                PackValue::str("CLIENT_ERROR"),
            )])),
        };
        let v = packstream::decode(&f.encode()).expect("decode");
        let PackValue::Struct { tag: 0x7F, fields } = v else {
            panic!("{v:?}")
        };
        let m = &fields[0];
        assert_eq!(
            m.get("neo4j_code").and_then(PackValue::as_str),
            Some("Neo.ClientError.Statement.SyntaxError")
        );
        assert_eq!(m.get("code"), None);
        assert_eq!(
            m.get("gql_status").and_then(PackValue::as_str),
            Some("42001")
        );
        assert!(m.get("diagnostic_record").is_some());
        // Decoded as a plain failure, the code is kept.
        assert_eq!(
            Response::decode(&f.encode()).expect("decode"),
            Response::Failure {
                code: "Neo.ClientError.Statement.SyntaxError".into(),
                message: "bad".into()
            }
        );
    }

    #[test]
    fn chunking_golden_bytes() {
        // SUCCESS {} is B1 70 A0: one chunk and the end marker.
        let mut out = BytesMut::new();
        write_chunked(
            &Response::Success(PackValue::Map(vec![])).encode(),
            &mut out,
        );
        assert_eq!(out.to_vec(), hex("00 03 B1 70 A0 00 00"));
        // RESET, GOODBYE, COMMIT, ROLLBACK, LOGOFF: no fields.
        for (r, h) in [
            (Request::Reset, "B0 0F"),
            (Request::Goodbye, "B0 02"),
            (Request::Commit, "B0 12"),
            (Request::Rollback, "B0 13"),
            (Request::Logoff, "B0 6B"),
        ] {
            assert_eq!(r.encode(), hex(h));
        }
        assert_eq!(Response::Ignored.encode(), hex("B0 7E"));
        // PULL {n: -1}.
        let pull = Request::Pull(PackValue::map([("n", PackValue::Int(-1))]));
        assert_eq!(pull.encode(), hex("B1 3F A1 81 6E FF"));
        assert_eq!(
            Request::decode(&hex("B1 3F A1 81 6E FF")).expect("pull"),
            pull
        );
        // RUN "RETURN 1 AS x" {} {}.
        let run = hex("B3 10 8D 52 45 54 55 52 4E 20 31 20 41 53 20 78 A0 A0");
        assert_eq!(
            Request::decode(&run).expect("run"),
            Request::Run {
                query: "RETURN 1 AS x".into(),
                params: PackValue::Map(vec![]),
                extra: PackValue::Map(vec![]),
            }
        );
        // RECORD [1].
        assert_eq!(
            Response::Record(vec![PackValue::Int(1)]).encode(),
            hex("B1 71 91 01")
        );
        // FAILURE {code, message}.
        let f = Response::Failure {
            code: "C".into(),
            message: "m".into(),
        };
        assert_eq!(
            f.encode(),
            hex("B1 7F A2 84 63 6F 64 65 81 43 87 6D 65 73 73 61 67 65 81 6D")
        );
        assert_eq!(Response::decode(&f.encode()).expect("failure"), f);
    }

    #[test]
    fn dechunker_reassembles_and_skips_keepalives() {
        let payload: Vec<u8> = (0..70_000u32)
            .map(|i| u8::try_from(i % 251).unwrap_or(0))
            .collect();
        let mut wire = BytesMut::new();
        wire.put_u16(0); // keep-alive
        write_chunked(&payload, &mut wire);
        write_chunked(&[0xB0, 0x0F], &mut wire);
        // Two chunks for the large message.
        assert_eq!(&wire[2..4], &[0xFF, 0xFF]);
        let mut d = Dechunker::new(1 << 20);
        // Feed byte by byte at first, then the rest.
        let mut buf = BytesMut::new();
        let mut got = Vec::new();
        for b in &wire {
            buf.put_u8(*b);
            while let Some(m) = d.next(&mut buf).expect("dechunk") {
                got.push(m);
            }
        }
        assert_eq!(got.len(), 2);
        assert_eq!(got[0], payload);
        assert_eq!(got[1], vec![0xB0, 0x0F]);
        // Over the limit.
        let mut d = Dechunker::new(10);
        let mut w = BytesMut::new();
        write_chunked(&[0; 11], &mut w);
        assert!(d.next(&mut w).is_err());
    }

    #[test]
    fn unknown_or_malformed_messages_are_refused() {
        for bad in ["B0 FF", "01", "B1 10 01", "B1 01 01"] {
            assert!(Request::decode(&hex(bad)).is_err(), "{bad}");
        }
    }
}
