//! The golden fixture format and the response walker, shared by the
//! capture tool (`tests/golden_capture.rs`) and the replay tests
//! (`tests/golden.rs`, `tests/golden_fuzz_replay.rs`).
//!
//! A fixture is a text file, one exchange per file:
//!
//! ```text
//! # free comment
//! @target mariadb-11.8            where it was captured
//! @server 11.8.3-MariaDB-ubu2404  the greeting's version string
//! @server_caps 0x...              what the greeting offered (64-bit)
//! @caps 0x...                     what the client negotiated: decode with these
//! @kind query                     how to walk the server's packets (Kind)
//! @framing plain                  plain, zlib or zstd
//! @command COM_QUERY SELECT 1     what was sent, for humans
//! @binary_types 3,253u            column types for a COM_STMT_FETCH (u = unsigned)
//! C 00 0353454c4543542031         a client packet: sequence id, payload hex
//! S 01 01                         a server packet
//! W 0b000000...                   raw wire bytes read (compressed fixtures)
//! ```

#![allow(dead_code, unreachable_pub, clippy::assigning_clones)]

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use sprig_proto_mysql::packets::{Caps, StatusFlags};

/// How a fixture's server packets are walked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Greeting, auth exchange, final OK or ERR.
    Connect,
    /// `COM_QUERY` and friends: OK, ERR, `LOCAL INFILE` or resultsets
    /// (text rows), following `MORE_RESULTS_EXISTS`.
    Query,
    /// `COM_STMT_EXECUTE` and `COM_STMT_BULK_EXECUTE`: as `Query`, binary
    /// rows.
    Execute,
    /// `COM_STMT_FETCH`: binary rows, then a terminator.
    Fetch,
    /// `COM_STMT_PREPARE`.
    Prepare,
    /// `COM_FIELD_LIST`: column definitions, then a terminator.
    FieldList,
    /// One OK, ERR or EOF.
    OkOrErr,
    /// One raw packet (`COM_STATISTICS`), or ERR.
    Raw,
    /// No response (`COM_STMT_CLOSE`).
    None,
}

impl Kind {
    pub fn parse(s: &str) -> Option<Kind> {
        Some(match s {
            "connect" => Kind::Connect,
            "query" => Kind::Query,
            "execute" => Kind::Execute,
            "fetch" => Kind::Fetch,
            "prepare" => Kind::Prepare,
            "field_list" => Kind::FieldList,
            "ok_or_err" => Kind::OkOrErr,
            "raw" => Kind::Raw,
            "none" => Kind::None,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Connect => "connect",
            Kind::Query => "query",
            Kind::Execute => "execute",
            Kind::Fetch => "fetch",
            Kind::Prepare => "prepare",
            Kind::FieldList => "field_list",
            Kind::OkOrErr => "ok_or_err",
            Kind::Raw => "raw",
            Kind::None => "none",
        }
    }

    /// The walk for a command code.
    pub fn for_command(code: u8) -> Kind {
        match code {
            0x03 | 0x0A => Kind::Query,
            0x17 | 0xFA => Kind::Execute,
            0x1C => Kind::Fetch,
            0x16 => Kind::Prepare,
            0x04 => Kind::FieldList,
            0x09 => Kind::Raw,
            0x01 | 0x18 | 0x19 => Kind::None,
            _ => Kind::OkOrErr,
        }
    }
}

/// One recorded line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Line {
    /// Client packet.
    C(u8, Bytes),
    /// Server packet.
    S(u8, Bytes),
    /// Raw wire bytes from the server.
    W(Bytes),
}

/// A parsed fixture.
#[derive(Debug, Clone, Default)]
pub struct Fixture {
    pub path: PathBuf,
    pub name: String,
    pub target: String,
    pub server: String,
    pub server_caps: Caps,
    pub caps: Caps,
    pub kind: Option<Kind>,
    pub framing: String,
    pub command: String,
    pub binary_types: Vec<(u8, bool)>,
    pub lines: Vec<Line>,
}

impl Fixture {
    /// The server packets, in order.
    pub fn server_packets(&self) -> Vec<(u8, Bytes)> {
        self.lines
            .iter()
            .filter_map(|l| match l {
                Line::S(s, p) => Some((*s, p.clone())),
                _ => None,
            })
            .collect()
    }

    /// The client packets, in order.
    pub fn client_packets(&self) -> Vec<(u8, Bytes)> {
        self.lines
            .iter()
            .filter_map(|l| match l {
                Line::C(s, p) => Some((*s, p.clone())),
                _ => None,
            })
            .collect()
    }

    /// The raw wire bytes, concatenated.
    pub fn wire(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for l in &self.lines {
            if let Line::W(b) = l {
                out.extend_from_slice(b);
            }
        }
        out
    }

    pub fn parse(path: &Path, text: &str) -> Result<Fixture, String> {
        let mut f = Fixture {
            path: path.to_owned(),
            name: path
                .file_stem()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default(),
            framing: "plain".into(),
            ..Fixture::default()
        };
        for (i, raw) in text.lines().enumerate() {
            let line = raw.trim_end();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let err = |m: &str| format!("{}:{}: {m}: {line}", path.display(), i + 1);
            if let Some(rest) = line.strip_prefix('@') {
                let (k, v) = rest.split_once(' ').unwrap_or((rest, ""));
                match k {
                    "target" => f.target = v.to_owned(),
                    "server" => f.server = v.to_owned(),
                    "server_caps" => f.server_caps = caps_of(v).ok_or_else(|| err("caps"))?,
                    "caps" => f.caps = caps_of(v).ok_or_else(|| err("caps"))?,
                    "kind" => f.kind = Some(Kind::parse(v).ok_or_else(|| err("kind"))?),
                    "framing" => f.framing = v.to_owned(),
                    "command" => f.command = v.to_owned(),
                    "binary_types" => {
                        f.binary_types = v
                            .split(',')
                            .filter(|s| !s.is_empty())
                            .map(|t| {
                                let u = t.ends_with('u');
                                t.trim_end_matches('u').parse::<u8>().map(|t| (t, u))
                            })
                            .collect::<Result<_, _>>()
                            .map_err(|_| err("binary_types"))?;
                    }
                    _ => return Err(err("unknown header")),
                }
                continue;
            }
            let mut parts = line.splitn(3, ' ');
            let tag = parts.next().unwrap_or_default();
            match tag {
                "C" | "S" => {
                    let seq = parts
                        .next()
                        .and_then(|s| u8::from_str_radix(s, 16).ok())
                        .ok_or_else(|| err("sequence id"))?;
                    let payload =
                        unhex(parts.next().unwrap_or_default()).ok_or_else(|| err("hex"))?;
                    f.lines.push(if tag == "C" {
                        Line::C(seq, payload.into())
                    } else {
                        Line::S(seq, payload.into())
                    });
                }
                "W" => {
                    let rest = line[1..].trim();
                    f.lines
                        .push(Line::W(unhex(rest).ok_or_else(|| err("hex"))?.into()));
                }
                _ => return Err(err("unknown line")),
            }
        }
        Ok(f)
    }

    pub fn render(&self) -> String {
        let mut s = String::new();
        let _ = writeln!(
            s,
            "# sprig-proto-mysql golden capture (tests/golden_capture.rs)"
        );
        let _ = writeln!(s, "@target {}", self.target);
        let _ = writeln!(s, "@server {}", self.server);
        let _ = writeln!(s, "@server_caps {:#018x}", self.server_caps.bits());
        let _ = writeln!(s, "@caps {:#018x}", self.caps.bits());
        if let Some(k) = self.kind {
            let _ = writeln!(s, "@kind {}", k.as_str());
        }
        let _ = writeln!(s, "@framing {}", self.framing);
        let _ = writeln!(s, "@command {}", self.command.replace('\n', " "));
        if !self.binary_types.is_empty() {
            let t: Vec<String> = self
                .binary_types
                .iter()
                .map(|(t, u)| format!("{t}{}", if *u { "u" } else { "" }))
                .collect();
            let _ = writeln!(s, "@binary_types {}", t.join(","));
        }
        for l in &self.lines {
            let _ = match l {
                Line::C(q, p) => writeln!(s, "C {q:02x} {}", hex(p)),
                Line::S(q, p) => writeln!(s, "S {q:02x} {}", hex(p)),
                Line::W(b) => writeln!(s, "W {}", hex(b)),
            };
        }
        s
    }
}

fn caps_of(v: &str) -> Option<Caps> {
    u64::from_str_radix(v.trim_start_matches("0x"), 16)
        .ok()
        .map(Caps::from_bits_retain)
}

pub fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s
}

pub fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

/// Every fixture under `tests/golden/`, sorted by path.
pub fn load_all() -> Vec<Fixture> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden");
    let mut paths = Vec::new();
    let Ok(targets) = std::fs::read_dir(&root) else {
        return Vec::new();
    };
    for t in targets.flatten() {
        if !t.path().is_dir() {
            continue;
        }
        for f in std::fs::read_dir(t.path()).into_iter().flatten().flatten() {
            if f.path().extension().is_some_and(|e| e == "hex") {
                paths.push(f.path());
            }
        }
    }
    paths.sort();
    paths
        .iter()
        .map(|p| {
            let text = std::fs::read_to_string(p).expect("read fixture");
            Fixture::parse(p, &text).expect("parse fixture")
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The response walker
// ---------------------------------------------------------------------------

/// What a server packet is, by its place in the response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    Greeting,
    AuthSwitch,
    AuthMoreData,
    Ok,
    Err,
    LocalInfile,
    ColumnCount(u64),
    ColumnDef,
    ColumnsEof,
    TextRow,
    BinaryRow,
    /// The 0xFE packet that ends rows (EOF or OK form by caps).
    End,
    PrepareOk,
    ParamDef,
    ParamsEof,
    /// A column definition answering `COM_FIELD_LIST`.
    FieldDef,
    /// An EOF answering a command whose answer is OK (`COM_SET_OPTION`).
    Eof,
    Raw,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expect {
    Greeting,
    Auth,
    First,
    Columns(u64),
    ColumnsEof,
    Rows,
    PrepareFirst,
    PrepareParams { left: u16, cols: u16 },
    PrepareParamsEof { cols: u16 },
    PrepareCols(u16),
    PrepareColsEof,
    FieldList,
    OkOrErr,
    Raw,
    Done,
}

/// Walks one response packet by packet, as a client would.
#[derive(Debug, Clone)]
pub struct Walker {
    caps: Caps,
    binary: bool,
    expect: Expect,
}

impl Walker {
    pub fn new(kind: Kind, caps: Caps) -> Self {
        let expect = match kind {
            Kind::Connect => Expect::Greeting,
            Kind::Query | Kind::Execute => Expect::First,
            Kind::Fetch => Expect::Rows,
            Kind::Prepare => Expect::PrepareFirst,
            Kind::FieldList => Expect::FieldList,
            Kind::OkOrErr => Expect::OkOrErr,
            Kind::Raw => Expect::Raw,
            Kind::None => Expect::Done,
        };
        Walker {
            caps,
            binary: matches!(kind, Kind::Execute | Kind::Fetch),
            expect,
        }
    }

    pub fn done(&self) -> bool {
        self.expect == Expect::Done
    }

    /// The capabilities in force; the connection phase learns them.
    pub fn set_caps(&mut self, caps: Caps) {
        self.caps = caps;
    }

    fn status_of_end(&self, p: &[u8]) -> StatusFlags {
        // EOF: fe warnings(2) status(2). OK form: fe lenenc lenenc status.
        if self.caps.contains(Caps::DEPRECATE_EOF) {
            let mut i = 1;
            for _ in 0..2 {
                i += match p.get(i) {
                    Some(0xFC) => 3,
                    Some(0xFD) => 4,
                    Some(0xFE) => 9,
                    _ => 1,
                };
            }
            status_at(p, i)
        } else {
            status_at(p, 3)
        }
    }

    fn after_end(status: StatusFlags) -> Expect {
        if status.contains(StatusFlags::MORE_RESULTS_EXISTS) {
            Expect::First
        } else {
            Expect::Done
        }
    }

    fn ok_status(p: &[u8]) -> StatusFlags {
        let mut i = 1;
        for _ in 0..2 {
            i += match p.get(i) {
                Some(0xFC) => 3,
                Some(0xFD) => 4,
                Some(0xFE) => 9,
                _ => 1,
            };
        }
        status_at(p, i)
    }

    /// Classifies the next server packet and advances.
    pub fn feed(&mut self, p: &[u8]) -> Class {
        let first = p.first().copied().unwrap_or(0);
        let deprecate_eof = self.caps.contains(Caps::DEPRECATE_EOF);
        let is_end = first == 0xFE && p.len() < 0xFF_FFFF;
        match self.expect {
            Expect::Greeting => {
                if first == 0xFF {
                    self.expect = Expect::Done;
                    return Class::Err;
                }
                self.expect = Expect::Auth;
                Class::Greeting
            }
            Expect::Auth => match first {
                0x00 => {
                    self.expect = Expect::Done;
                    Class::Ok
                }
                0xFF => {
                    self.expect = Expect::Done;
                    Class::Err
                }
                0xFE => Class::AuthSwitch,
                _ => Class::AuthMoreData,
            },
            Expect::First => match first {
                0x00 => {
                    self.expect = Self::after_end(Self::ok_status(p));
                    Class::Ok
                }
                0xFF => {
                    self.expect = Expect::Done;
                    Class::Err
                }
                0xFB => {
                    // The client sends the file; the final OK or ERR follows.
                    Class::LocalInfile
                }
                _ => {
                    let n = lenenc(p).unwrap_or(0);
                    self.expect = Expect::Columns(n);
                    Class::ColumnCount(n)
                }
            },
            Expect::Columns(_) | Expect::PrepareCols(_) if first == 0xFF => {
                self.expect = Expect::Done;
                Class::Err
            }
            Expect::Columns(left) => {
                self.expect = match left {
                    0 | 1 if deprecate_eof => Expect::Rows,
                    0 | 1 => Expect::ColumnsEof,
                    n => Expect::Columns(n - 1),
                };
                Class::ColumnDef
            }
            Expect::ColumnsEof => {
                let st = status_at(p, 3);
                self.expect = if st.contains(StatusFlags::CURSOR_EXISTS) {
                    Self::after_end(st)
                } else {
                    Expect::Rows
                };
                Class::ColumnsEof
            }
            Expect::Rows => {
                if is_end {
                    self.expect = Self::after_end(self.status_of_end(p));
                    Class::End
                } else if first == 0xFF {
                    self.expect = Expect::Done;
                    Class::Err
                } else if self.binary {
                    Class::BinaryRow
                } else {
                    Class::TextRow
                }
            }
            Expect::PrepareFirst => {
                if first == 0xFF {
                    self.expect = Expect::Done;
                    return Class::Err;
                }
                let u16_at = |i: usize| u16::from_le_bytes([p[i], p[i + 1]]);
                let (cols, params) = if p.len() >= 9 {
                    (u16_at(5), u16_at(7))
                } else {
                    (0, 0)
                };
                self.expect = match (params, cols) {
                    (0, 0) => Expect::Done,
                    (0, c) => Expect::PrepareCols(c),
                    (p, c) => Expect::PrepareParams { left: p, cols: c },
                };
                Class::PrepareOk
            }
            Expect::PrepareParams { left, cols } => {
                self.expect = match (left, cols) {
                    (0 | 1, 0) if deprecate_eof => Expect::Done,
                    (0 | 1, c) if deprecate_eof => Expect::PrepareCols(c),
                    (0 | 1, c) => Expect::PrepareParamsEof { cols: c },
                    (n, c) => Expect::PrepareParams {
                        left: n - 1,
                        cols: c,
                    },
                };
                Class::ParamDef
            }
            Expect::PrepareParamsEof { cols } => {
                self.expect = if cols == 0 {
                    Expect::Done
                } else {
                    Expect::PrepareCols(cols)
                };
                Class::ParamsEof
            }
            Expect::PrepareCols(left) => {
                self.expect = match left {
                    0 | 1 if deprecate_eof => Expect::Done,
                    0 | 1 => Expect::PrepareColsEof,
                    n => Expect::PrepareCols(n - 1),
                };
                Class::ColumnDef
            }
            Expect::PrepareColsEof => {
                self.expect = Expect::Done;
                Class::ColumnsEof
            }
            Expect::FieldList => {
                if is_end {
                    self.expect = Expect::Done;
                    Class::End
                } else if first == 0xFF {
                    self.expect = Expect::Done;
                    Class::Err
                } else {
                    Class::FieldDef
                }
            }
            Expect::OkOrErr => {
                self.expect = Expect::Done;
                match first {
                    0x00 => Class::Ok,
                    0xFF => Class::Err,
                    0xFE => Class::Eof,
                    _ => Class::Raw,
                }
            }
            Expect::Raw => {
                self.expect = Expect::Done;
                if first == 0xFF {
                    Class::Err
                } else {
                    Class::Raw
                }
            }
            Expect::Done => Class::Raw,
        }
    }
}

fn status_at(p: &[u8], i: usize) -> StatusFlags {
    match (p.get(i), p.get(i + 1)) {
        (Some(&a), Some(&b)) => StatusFlags::from_bits_retain(u16::from_le_bytes([a, b])),
        _ => StatusFlags::empty(),
    }
}

fn lenenc(p: &[u8]) -> Option<u64> {
    let mut r = sprig_proto_mysql::Reader::new(p);
    r.lenenc_int().ok()
}
