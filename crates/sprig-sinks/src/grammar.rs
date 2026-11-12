//! The sink and connection statements (chapter 51, "Connections",
//! "Sinks", build step 2), recognized before the gateway's name plan:
//!
//! ```text
//! CREATE CONNECTION [IF NOT EXISTS] [db.]c TYPE webhook|kafka|s3|mysql|postgresql
//!   [WITH (k = v, ...)] [CREDENTIALS (k = 'v', ...)]
//! ALTER CONNECTION c SET (k = v, ...) | RESET (k, ...) | SET CREDENTIALS (k = 'v', ...)
//! DROP CONNECTION [IF EXISTS] c
//! SHOW CONNECTIONS [FROM db] | SHOW CREATE CONNECTION c | SHOW PLATFORM CONNECTIONS
//! GRANT USAGE ON [PLATFORM] CONNECTION c TO user | REVOKE USAGE ON ... FROM user
//! CREATE SINK [IF NOT EXISTS] [db.]s
//!   FROM STREAM x | QUEUE q | CHANGES OF t [, t2 ...]
//!   [WHERE cond] [COLUMNS (...) | EXCEPT COLUMNS (...)] [TRANSFORM f]
//!   TO WEBHOOK [PLATFORM] CONNECTION c [PATH '/p']
//!    | TO KAFKA [PLATFORM] CONNECTION c TOPIC 't' [KEY 'expr']
//!    | TO S3 [PLATFORM] CONNECTION c PREFIX 'template'
//!    | TO DATABASE [PLATFORM] CONNECTION c TABLE 'name'
//!    | TO SCRIPT LANGUAGE l [USING CONNECTIONS (c, ...)] AS $$ ... $$
//!    | TO SCRIPT FUNCTION f [USING CONNECTIONS (c, ...)]
//!   [GUARANTEE AT LEAST ONCE | EXACTLY ONCE]
//!   [DEAD LETTER TO STREAM d | QUEUE q | EXCHANGE e]
//!   [START AT LATEST | EARLIEST | SNAPSHOT | TIMESTAMP 'iso']
//!   [WITH (k = v, ...)]
//! ALTER SINK s PAUSE | RESUME | SET (k = v, ...) | RESET (k, ...)
//!   | RESET TO LATEST | EARLIEST | SNAPSHOT | TIMESTAMP '...' | OWNER TO u
//!   | SET SCRIPT AS $$ ... $$
//! DROP SINK [IF EXISTS] s [WITH DEAD LETTERS]
//! SHOW SINKS [FROM db] | SHOW CREATE SINK s | SHOW SINK STATUS s
//! TEST [PLATFORM] CONNECTION c [WRITE [TO 't']] | TEST SINK s [WITH SAMPLE n]
//! TEST CREATE CONNECTION ...            (ST68: an unsaved definition's checks)
//! TEST CREATE SINK ... [WITH SAMPLE n]  (ST69: an unsaved definition's dry run)
//! ```
//!
//! The tokenizer is this module's own: it knows `$$` bodies, which
//! chapter 07's lexer doesn't, and keeps every token's bytes, so
//! [`redact`] can replace credential values in the text.

use std::collections::BTreeMap;
use std::ops::Range;

use sprig_common::{Result, SprigCode, SprigError};

use crate::model::{ConnKind, Guarantee, Projection, ScriptLanguage, StartAt, err};

/// A token's kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tk {
    /// A bare word.
    Word,
    /// `` `quoted` `` identifier.
    QIdent,
    /// `'text'` or `"text"`.
    Str,
    /// A number.
    Num,
    /// `$$ ... $$` or `$tag$ ... $tag$`.
    Dollar,
    /// One punctuation character.
    Punct,
    /// Anything else (operators).
    Other,
}

/// One token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tok {
    /// Its kind.
    pub kind: Tk,
    /// Its value: the word, the unquoted text, the body.
    pub text: String,
    /// Its bytes in the statement.
    pub range: Range<usize>,
}

fn syntax(msg: impl Into<String>) -> SprigError {
    SprigError::standard(
        1064,
        *b"42000",
        format!("You have an error in your SQL syntax; {}", msg.into()),
    )
}

/// Splits `sql` into tokens (comments and spaces dropped).
///
/// # Errors
/// 1064 for an unterminated string or body.
#[allow(clippy::too_many_lines)]
pub fn tokens(sql: &str) -> Result<Vec<Tok>> {
    let b = sql.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if c == b'-' && b.get(i + 1) == Some(&b'-') || c == b'#' {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if c == b'/' && b.get(i + 1) == Some(&b'*') {
            let end = sql[i + 2..]
                .find("*/")
                .ok_or_else(|| syntax("an unterminated comment"))?;
            i += end + 4;
            continue;
        }
        let start = i;
        if c == b'\'' || c == b'"' {
            let q = c;
            i += 1;
            let mut v = Vec::new();
            loop {
                let Some(&x) = b.get(i) else {
                    return Err(syntax("an unterminated string"));
                };
                if x == b'\\' {
                    let Some(&n) = b.get(i + 1) else {
                        return Err(syntax("an unterminated string"));
                    };
                    v.push(match n {
                        b'n' => b'\n',
                        b't' => b'\t',
                        b'r' => b'\r',
                        b'0' => 0,
                        o => o,
                    });
                    i += 2;
                    continue;
                }
                if x == q {
                    if b.get(i + 1) == Some(&q) {
                        v.push(q);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                v.push(x);
                i += 1;
            }
            out.push(Tok {
                kind: Tk::Str,
                text: String::from_utf8_lossy(&v).into_owned(),
                range: start..i,
            });
            continue;
        }
        if c == b'`' {
            i += 1;
            let mut v = Vec::new();
            loop {
                let Some(&x) = b.get(i) else {
                    return Err(syntax("an unterminated identifier"));
                };
                if x == b'`' {
                    if b.get(i + 1) == Some(&b'`') {
                        v.push(b'`');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                v.push(x);
                i += 1;
            }
            out.push(Tok {
                kind: Tk::QIdent,
                text: String::from_utf8_lossy(&v).into_owned(),
                range: start..i,
            });
            continue;
        }
        if c == b'$' {
            // $$ or $tag$
            let rest = &sql[i + 1..];
            if let Some(close) = rest.find('$') {
                let tag = &rest[..close];
                if tag
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
                {
                    let delim = format!("${tag}$");
                    let body_start = i + delim.len();
                    let end = sql[body_start..]
                        .find(&delim)
                        .ok_or_else(|| syntax(format!("an unterminated {delim} body")))?;
                    out.push(Tok {
                        kind: Tk::Dollar,
                        text: sql[body_start..body_start + end].to_owned(),
                        range: start..body_start + end + delim.len(),
                    });
                    i = body_start + end + delim.len();
                    continue;
                }
            }
        }
        if c.is_ascii_alphabetic() || c == b'_' || c >= 0x80 {
            while i < b.len()
                && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$' || b[i] >= 0x80)
            {
                i += 1;
            }
            out.push(Tok {
                kind: Tk::Word,
                text: sql[start..i].to_owned(),
                range: start..i,
            });
            continue;
        }
        if c.is_ascii_digit() || (c == b'-' && b.get(i + 1).is_some_and(u8::is_ascii_digit)) {
            i += 1;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'.') {
                i += 1;
            }
            out.push(Tok {
                kind: Tk::Num,
                text: sql[start..i].to_owned(),
                range: start..i,
            });
            continue;
        }
        if b"(),=.;*@".contains(&c) {
            i += 1;
            out.push(Tok {
                kind: Tk::Punct,
                text: (c as char).to_string(),
                range: start..i,
            });
            continue;
        }
        // Operators and anything else: one char at a time (UTF-8 safe).
        let ch = sql[i..].chars().next().map_or(1, char::len_utf8);
        i += ch;
        out.push(Tok {
            kind: Tk::Other,
            text: sql[start..i].to_owned(),
            range: start..i,
        });
    }
    Ok(out)
}

/// A possibly qualified name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QName {
    /// The database, if qualified.
    pub db: Option<String>,
    /// The name.
    pub name: String,
}

/// An option value as written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptVal {
    /// A string literal.
    Text(String),
    /// A number or bare word (`on`, `off`, `zstd`).
    Bare(String),
    /// `REQUIRED` (inherited connections; not built).
    Required,
}

impl OptVal {
    /// The value as text.
    #[must_use]
    pub fn as_text(&self) -> &str {
        match self {
            OptVal::Text(s) | OptVal::Bare(s) => s,
            OptVal::Required => "REQUIRED",
        }
    }
}

/// `CREATE CONNECTION`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateConnection {
    /// The name.
    pub name: QName,
    /// `IF NOT EXISTS`.
    pub if_not_exists: bool,
    /// `TYPE`.
    pub kind: ConnKind,
    /// `WITH (...)`, in order.
    pub options: Vec<(String, OptVal)>,
    /// `CREDENTIALS (...)`, in order.
    pub credentials: Vec<(String, String)>,
    /// `CREDENTIALS (name = REQUIRED, ...)`: credentials each member of a
    /// `FOR EACH TENANT` connection sets (R9, r9more).
    pub required_credentials: Vec<String>,
}

/// What `ALTER CONNECTION` does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterConnection {
    /// `SET (...)`.
    Set(Vec<(String, OptVal)>),
    /// `RESET (...)`.
    Reset(Vec<String>),
    /// `SET CREDENTIALS (...)`.
    Credentials(Vec<(String, String)>),
}

/// Where a sink's target connection comes from, as written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnName {
    /// The name.
    pub name: String,
    /// `PLATFORM CONNECTION`.
    pub platform: bool,
}

/// `FROM ...` as written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceAst {
    /// `FROM STREAM x`.
    Stream(QName),
    /// `FROM QUEUE q`.
    Queue(QName),
    /// `FROM CHANGES OF t, ...`.
    Changes(Vec<QName>),
    /// `FROM STREAM s FAN IN [UNDER t]`.
    FanIn {
        /// The inherited stream's name.
        stream: QName,
        /// `UNDER t`.
        under: Option<String>,
    },
    /// `FROM QUEUE q FAN IN [UNDER t]` (R9, r9queue).
    FanInQueue {
        /// The inherited queue's name.
        queue: QName,
        /// `UNDER t`.
        under: Option<String>,
    },
}

/// `TO ...` as written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetAst {
    /// `TO WEBHOOK`.
    Webhook {
        /// Its connection.
        conn: ConnName,
        /// `PATH`.
        path: Option<String>,
    },
    /// `TO KAFKA`.
    Kafka {
        /// Its connection.
        conn: ConnName,
        /// `TOPIC`.
        topic: String,
        /// `KEY`.
        key: Option<String>,
    },
    /// `TO S3`.
    S3 {
        /// Its connection.
        conn: ConnName,
        /// `PREFIX`.
        prefix: String,
    },
    /// `TO DATABASE`.
    Database {
        /// Its connection.
        conn: ConnName,
        /// `TABLE`.
        table: String,
    },
    /// `TO SCRIPT`.
    Script {
        /// The language (`LANGUAGE`), or `None` for `FUNCTION`.
        language: Option<ScriptLanguage>,
        /// `AS $$ ... $$`.
        source: Option<String>,
        /// `FUNCTION f`.
        function: Option<String>,
        /// `USING CONNECTIONS (...)`.
        using: Vec<ConnName>,
    },
}

/// `DEAD LETTER TO ...` as written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeadAst {
    /// A stream.
    Stream(String),
    /// A queue.
    Queue(String),
    /// An exchange.
    Exchange(String),
}

/// `CREATE SINK`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSink {
    /// The name.
    pub name: QName,
    /// `IF NOT EXISTS`.
    pub if_not_exists: bool,
    /// `FROM ...`.
    pub source: SourceAst,
    /// `WHERE ...`, as written.
    pub filter: Option<String>,
    /// `COLUMNS` / `EXCEPT COLUMNS`.
    pub projection: Option<Projection>,
    /// `TRANSFORM f`.
    pub transform: Option<String>,
    /// `TO ...`.
    pub target: TargetAst,
    /// `GUARANTEE`.
    pub guarantee: Guarantee,
    /// `DEAD LETTER TO`.
    pub dead_letter: Option<DeadAst>,
    /// `START AT`.
    pub start_at: StartAt,
    /// `WITH (...)`.
    pub options: Vec<(String, OptVal)>,
}

/// What `ALTER SINK` does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterSink {
    /// `PAUSE`.
    Pause,
    /// `RESUME`.
    Resume,
    /// `SET (...)`.
    Set(Vec<(String, OptVal)>),
    /// `RESET (...)`.
    Reset(Vec<String>),
    /// `RESET TO ...`.
    ResetTo(StartAt),
    /// `OWNER TO u`.
    Owner(String),
    /// `SET SCRIPT AS $$ ... $$`.
    Script(String),
}

/// A sink-layer statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stmt {
    /// `CREATE CONNECTION`.
    CreateConnection(CreateConnection),
    /// `ALTER CONNECTION`.
    AlterConnection(QName, AlterConnection),
    /// `DROP CONNECTION`.
    DropConnection {
        /// The name.
        name: QName,
        /// `IF EXISTS`.
        if_exists: bool,
    },
    /// `SHOW CONNECTIONS`.
    ShowConnections(Option<String>),
    /// `SHOW CREATE CONNECTION`.
    ShowCreateConnection(QName),
    /// `SHOW PLATFORM CONNECTIONS`.
    ShowPlatformConnections,
    /// `GRANT | REVOKE USAGE ON [PLATFORM] CONNECTION c TO | FROM u`.
    Usage {
        /// Grant or revoke.
        grant: bool,
        /// The connection.
        conn: ConnName,
        /// The connection's database, if qualified.
        db: Option<String>,
        /// The user (`name`, host dropped).
        user: String,
    },
    /// `CREATE SINK`.
    CreateSink(Box<CreateSink>),
    /// `ALTER SINK`.
    AlterSink(QName, AlterSink),
    /// `DROP SINK`.
    DropSink {
        /// The name.
        name: QName,
        /// `IF EXISTS`.
        if_exists: bool,
        /// `WITH DEAD LETTERS`.
        with_dead_letters: bool,
    },
    /// `SHOW SINKS`.
    ShowSinks(Option<String>),
    /// `SHOW CREATE SINK`.
    ShowCreateSink(QName),
    /// `SHOW SINK STATUS`.
    ShowSinkStatus(QName),
    /// `TEST [PLATFORM] CONNECTION c` (R9, r9tests): open and check the
    /// destination, write nothing.
    TestConnection(ConnName, Option<String>),
    /// `TEST [PLATFORM] CONNECTION c WRITE [TO 'target']` (R9, r9last):
    /// the checks, then one marked test record written (a webhook's
    /// request, a Kafka record into topic `target`, an S3 object deleted
    /// again; a database table's `INSERT` right, checked without a row).
    TestConnectionWrite {
        /// The connection.
        conn: ConnName,
        /// Its database.
        db: Option<String>,
        /// `TO 'target'`: a topic, an object key prefix, a table.
        to: Option<String>,
    },
    /// `TEST SINK s [WITH SAMPLE n]` (R9, r9tests): the sink's last `n`
    /// source records (10 by default, at most 1,000) as they would be
    /// delivered, delivering nothing.
    TestSink {
        /// The sink.
        name: QName,
        /// Records sampled.
        sample: u32,
    },
    /// `TEST CREATE CONNECTION ...` (ST68): `TEST CONNECTION`'s checks of
    /// a definition that isn't saved (the whole `CREATE CONNECTION`
    /// after `TEST`); nothing is created.
    TestNewConnection(CreateConnection),
    /// `TEST CREATE SINK ... [WITH SAMPLE n]` (ST69): `TEST SINK`'s dry run
    /// of a definition that isn't saved; nothing is created or delivered.
    TestNewSink {
        /// The definition.
        def: Box<CreateSink>,
        /// Records sampled.
        sample: u32,
    },
}

/// A cheap check before tokenizing: could `sql` be a sink statement?
#[must_use]
pub fn maybe_sink(sql: &[u8]) -> bool {
    let up: Vec<u8> = sql.iter().take(4096).map(u8::to_ascii_uppercase).collect();
    let has = |w: &[u8]| up.windows(w.len()).any(|x| x == w);
    has(b"SINK") || has(b"CONNECTION")
}

struct P<'a> {
    t: &'a [Tok],
    i: usize,
    sql: &'a str,
}

impl<'a> P<'a> {
    fn peek(&self) -> Option<&'a Tok> {
        self.t.get(self.i)
    }

    fn peek_kw(&self, w: &str) -> bool {
        self.peek()
            .is_some_and(|t| t.kind == Tk::Word && t.text.eq_ignore_ascii_case(w))
    }

    fn peek_kw_at(&self, n: usize, w: &str) -> bool {
        self.t
            .get(self.i + n)
            .is_some_and(|t| t.kind == Tk::Word && t.text.eq_ignore_ascii_case(w))
    }

    fn kw(&mut self, w: &str) -> bool {
        if self.peek_kw(w) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn kws(&mut self, ws: &[&str]) -> bool {
        if ws.iter().enumerate().all(|(n, w)| self.peek_kw_at(n, w)) {
            self.i += ws.len();
            true
        } else {
            false
        }
    }

    fn need_kw(&mut self, w: &str) -> Result<()> {
        if self.kw(w) {
            Ok(())
        } else {
            Err(self.unexpected(&format!("expected {w}")))
        }
    }

    fn punct(&mut self, p: &str) -> bool {
        if self
            .peek()
            .is_some_and(|t| t.kind == Tk::Punct && t.text == p)
        {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn need_punct(&mut self, p: &str) -> Result<()> {
        if self.punct(p) {
            Ok(())
        } else {
            Err(self.unexpected(&format!("expected '{p}'")))
        }
    }

    fn unexpected(&self, what: &str) -> SprigError {
        let near = self.peek().map_or("", |t| {
            &self.sql[t.range.start..self.sql.len().min(t.range.start + 40)]
        });
        syntax(format!("{what} near '{near}'"))
    }

    fn ident(&mut self) -> Result<String> {
        match self.peek() {
            Some(t) if t.kind == Tk::Word || t.kind == Tk::QIdent => {
                self.i += 1;
                Ok(t.text.clone())
            }
            _ => Err(self.unexpected("expected a name")),
        }
    }

    fn qname(&mut self) -> Result<QName> {
        let a = self.ident()?;
        if self.punct(".") {
            let b = self.ident()?;
            return Ok(QName {
                db: Some(a),
                name: b,
            });
        }
        Ok(QName { db: None, name: a })
    }

    fn string(&mut self) -> Result<String> {
        match self.peek() {
            Some(t) if t.kind == Tk::Str => {
                self.i += 1;
                Ok(t.text.clone())
            }
            _ => Err(self.unexpected("expected a quoted string")),
        }
    }

    /// `(k = v, ...)`; keys may be dotted (`s3.file_max_bytes`).
    fn options(&mut self) -> Result<Vec<(String, OptVal)>> {
        self.need_punct("(")?;
        let mut out = Vec::new();
        if self.punct(")") {
            return Ok(out);
        }
        loop {
            let mut k = self.ident()?;
            while self.punct(".") {
                k.push('.');
                k.push_str(&self.ident()?);
            }
            self.need_punct("=")?;
            let v = match self.peek() {
                Some(t) if t.kind == Tk::Str => OptVal::Text(t.text.clone()),
                Some(t) if t.kind == Tk::Word && t.text.eq_ignore_ascii_case("REQUIRED") => {
                    OptVal::Required
                }
                Some(t) if matches!(t.kind, Tk::Word | Tk::Num) => OptVal::Bare(t.text.clone()),
                _ => return Err(self.unexpected("expected a value")),
            };
            self.i += 1;
            out.push((k.to_ascii_lowercase(), v));
            if self.punct(")") {
                return Ok(out);
            }
            self.need_punct(",")?;
        }
    }

    fn name_list(&mut self) -> Result<Vec<String>> {
        self.need_punct("(")?;
        let mut out = Vec::new();
        loop {
            let mut k = self.ident()?;
            while self.punct(".") {
                k.push('.');
                k.push_str(&self.ident()?);
            }
            out.push(k);
            if self.punct(")") {
                return Ok(out);
            }
            self.need_punct(",")?;
        }
    }

    fn credentials(&mut self) -> Result<Vec<(String, String)>> {
        let opts = self.options()?;
        opts.into_iter()
            .map(|(k, v)| match v {
                OptVal::Text(s) => Ok((k, s)),
                _ => Err(err(
                    SprigCode::SinkCredentialsInvalid,
                    format!("credential {k} must be a quoted string"),
                )),
            })
            .collect()
    }

    fn conn_name(&mut self) -> Result<ConnName> {
        let platform = self.kw("PLATFORM");
        self.need_kw("CONNECTION")?;
        Ok(ConnName {
            name: self.ident()?,
            platform,
        })
    }

    fn end(&mut self) -> Result<()> {
        while self.punct(";") {}
        if self.i < self.t.len() {
            return Err(self.unexpected("unexpected text"));
        }
        Ok(())
    }

    fn start_at(&mut self) -> Result<StartAt> {
        if self.kw("LATEST") {
            Ok(StartAt::Latest)
        } else if self.kw("EARLIEST") {
            Ok(StartAt::Earliest)
        } else if self.kw("SNAPSHOT") {
            Ok(StartAt::Snapshot)
        } else if self.kw("TIMESTAMP") {
            let s = self.string()?;
            let ms = parse_time(&s)
                .ok_or_else(|| crate::model::bad_option(format!("bad time '{s}'")))?;
            Ok(StartAt::Timestamp { ms })
        } else {
            Err(self.unexpected("expected LATEST, EARLIEST, SNAPSHOT or TIMESTAMP"))
        }
    }
}

/// `YYYY-MM-DD[THH:MM[:SS[.fff]]][Z]` (UTC) as ms since the epoch.
#[must_use]
pub fn parse_time(s: &str) -> Option<i64> {
    let s = s.trim().trim_end_matches('Z');
    let (d, t) = s.split_once(['T', ' ']).unwrap_or((s, "00:00:00"));
    let mut dp = d.split('-');
    let y: i64 = dp.next()?.parse().ok()?;
    let mo: i64 = dp.next()?.parse().ok()?;
    let da: i64 = dp.next()?.parse().ok()?;
    let mut tp = t.split(':');
    let h: i64 = tp.next()?.parse().ok()?;
    let mi: i64 = tp.next().unwrap_or("0").parse().ok()?;
    let sec: f64 = tp.next().unwrap_or("0").parse().ok()?;
    if !(1..=12).contains(&mo) || !(1..=31).contains(&da) || h > 23 || mi > 59 {
        return None;
    }
    // Days from civil (Howard Hinnant's algorithm).
    let y2 = if mo <= 2 { y - 1 } else { y };
    let era = y2.div_euclid(400);
    let yoe = y2 - era * 400;
    let mp = (mo + 9) % 12;
    let doy = (153 * mp + 2) / 5 + da - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    #[allow(clippy::cast_possible_truncation)]
    let ms = (sec * 1000.0).round() as i64;
    Some(((days * 24 + h) * 60 + mi) * 60_000 + ms)
}

/// Who an `ALTER INHERITED CONNECTION | SINK` is for.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum InheritTarget {
    /// The session's own tenant.
    #[default]
    Own,
    /// `FOR TENANT t`.
    Tenant(String),
    /// `FOR SUBTREE [t]`.
    Subtree(Option<String>),
}

/// What an `ALTER INHERITED CONNECTION | SINK` does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InheritedOp {
    /// `SET (...) [CREDENTIALS (...)]`, or `SET CREDENTIALS (...)`.
    Set {
        /// Options (connections) or `sink.*` settings (sinks).
        options: Vec<(String, OptVal)>,
        /// Credentials (connections only).
        credentials: Vec<(String, String)>,
    },
    /// `RESET [(k, ...)]`: every override, or those keys.
    Reset(Vec<String>),
    /// `DISABLE` (sinks).
    Disable,
    /// `ENABLE` (sinks).
    Enable,
}

/// `ALTER INHERITED CONNECTION | SINK [db.]name [FOR TENANT t | FOR
/// SUBTREE [t]] ...` (chapter 51, "Inherited sinks and connections").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InheritedAlter {
    /// A sink (else a connection).
    pub sink: bool,
    /// The name.
    pub name: QName,
    /// Who for.
    pub target: InheritTarget,
    /// What.
    pub op: InheritedOp,
}

/// Parses `ALTER INHERITED CONNECTION | SINK ...`; `Ok(None)` for any
/// other statement. These are parsed here, with the other sink
/// statements, so their credentials are sealed and never logged.
///
/// # Errors
/// 1064 for a malformed statement; 39216 for an unquoted credential.
pub fn parse_inherited(sql: &str) -> Result<Option<InheritedAlter>> {
    if !maybe_sink(sql.as_bytes()) {
        return Ok(None);
    }
    let t = tokens(sql)?;
    let mut p = P { t: &t, i: 0, sql };
    if !p.kws(&["ALTER", "INHERITED"]) {
        return Ok(None);
    }
    let sink = if p.kw("SINK") {
        true
    } else if p.kw("CONNECTION") {
        false
    } else {
        return Ok(None);
    };
    let name = p.qname()?;
    let mut target = InheritTarget::Own;
    if p.kws(&["FOR", "TENANT"]) {
        target = InheritTarget::Tenant(p.ident()?);
    } else if p.kws(&["FOR", "SUBTREE"]) {
        let named = !["SET", "RESET", "DISABLE", "ENABLE"]
            .iter()
            .any(|w| p.peek_kw(w))
            && p.peek().is_some();
        target = InheritTarget::Subtree(if named { Some(p.ident()?) } else { None });
    }
    let op = if p.kws(&["SET", "CREDENTIALS"]) {
        InheritedOp::Set {
            options: Vec::new(),
            credentials: p.credentials()?,
        }
    } else if p.kw("SET") {
        let options = p.options()?;
        let credentials = if p.kw("CREDENTIALS") {
            p.credentials()?
        } else {
            Vec::new()
        };
        InheritedOp::Set {
            options,
            credentials,
        }
    } else if p.kw("RESET") {
        if p.peek()
            .is_some_and(|t| t.kind == Tk::Punct && t.text == "(")
        {
            InheritedOp::Reset(p.name_list()?)
        } else {
            InheritedOp::Reset(Vec::new())
        }
    } else if p.kw("DISABLE") {
        InheritedOp::Disable
    } else if p.kw("ENABLE") {
        InheritedOp::Enable
    } else {
        return Err(p.unexpected("expected SET, RESET, DISABLE or ENABLE"));
    };
    p.end()?;
    Ok(Some(InheritedAlter {
        sink,
        name,
        target,
        op,
    }))
}

/// Parses one statement; `Ok(None)` when it isn't a sink statement.
///
/// # Errors
/// 1064 for a sink statement that doesn't parse; 39218, 39216 for bad
/// options or credentials.
pub fn parse(sql: &str) -> Result<Option<Stmt>> {
    if !maybe_sink(sql.as_bytes()) {
        return Ok(None);
    }
    let t = tokens(sql)?;
    let mut p = P { t: &t, i: 0, sql };
    let st = if p.kw("CREATE") {
        if p.kw("CONNECTION") {
            create_connection(&mut p)?
        } else if p.kw("SINK") {
            create_sink(&mut p)?
        } else {
            return Ok(None);
        }
    } else if p.kw("ALTER") {
        if p.kw("CONNECTION") {
            let name = p.qname()?;
            let a = if p.kws(&["SET", "CREDENTIALS"]) {
                AlterConnection::Credentials(p.credentials()?)
            } else if p.kw("SET") {
                AlterConnection::Set(p.options()?)
            } else if p.kw("RESET") {
                AlterConnection::Reset(p.name_list()?)
            } else {
                return Err(p.unexpected("expected SET, RESET or SET CREDENTIALS"));
            };
            Stmt::AlterConnection(name, a)
        } else if p.kw("SINK") {
            let name = p.qname()?;
            let a = if p.kw("PAUSE") {
                AlterSink::Pause
            } else if p.kw("RESUME") {
                AlterSink::Resume
            } else if p.kws(&["SET", "SCRIPT"]) {
                p.need_kw("AS")?;
                match p.peek() {
                    Some(t) if t.kind == Tk::Dollar || t.kind == Tk::Str => {
                        p.i += 1;
                        AlterSink::Script(t.text.clone())
                    }
                    _ => return Err(p.unexpected("expected $$ ... $$")),
                }
            } else if p.kw("SET") {
                AlterSink::Set(p.options()?)
            } else if p.kws(&["RESET", "TO"]) {
                AlterSink::ResetTo(p.start_at()?)
            } else if p.kw("RESET") {
                AlterSink::Reset(p.name_list()?)
            } else if p.kws(&["OWNER", "TO"]) {
                let u = p.ident()?;
                if p.punct("@") {
                    let _ = p.ident().or_else(|_| p.string())?;
                }
                AlterSink::Owner(u)
            } else {
                return Err(p.unexpected("expected PAUSE, RESUME, SET, RESET or OWNER TO"));
            };
            Stmt::AlterSink(name, a)
        } else {
            return Ok(None);
        }
    } else if p.kw("DROP") {
        if p.kw("CONNECTION") {
            let if_exists = p.kws(&["IF", "EXISTS"]);
            Stmt::DropConnection {
                name: p.qname()?,
                if_exists,
            }
        } else if p.kw("SINK") {
            let if_exists = p.kws(&["IF", "EXISTS"]);
            let name = p.qname()?;
            let with_dead_letters = p.kws(&["WITH", "DEAD", "LETTERS"]);
            Stmt::DropSink {
                name,
                if_exists,
                with_dead_letters,
            }
        } else {
            return Ok(None);
        }
    } else if p.kw("SHOW") {
        if p.kw("CONNECTIONS") {
            Stmt::ShowConnections(show_from(&mut p)?)
        } else if p.kws(&["PLATFORM", "CONNECTIONS"]) {
            Stmt::ShowPlatformConnections
        } else if p.kws(&["CREATE", "CONNECTION"]) {
            Stmt::ShowCreateConnection(p.qname()?)
        } else if p.kw("SINKS") {
            Stmt::ShowSinks(show_from(&mut p)?)
        } else if p.kws(&["CREATE", "SINK"]) {
            Stmt::ShowCreateSink(p.qname()?)
        } else if p.kws(&["SINK", "STATUS"]) {
            Stmt::ShowSinkStatus(p.qname()?)
        } else {
            return Ok(None);
        }
    } else if p.peek_kw("TEST")
        && p.peek_kw_at(1, "CREATE")
        && (p.peek_kw_at(2, "CONNECTION") || p.peek_kw_at(2, "SINK"))
    {
        // ST68, ST69: the checks of a definition that isn't saved.
        p.need_kw("TEST")?;
        p.need_kw("CREATE")?;
        if p.kw("CONNECTION") {
            let Stmt::CreateConnection(c) = create_connection(&mut p)? else {
                return Err(p.unexpected("CREATE CONNECTION"));
            };
            Stmt::TestNewConnection(c)
        } else {
            p.need_kw("SINK")?;
            let Stmt::CreateSink(def) = create_sink(&mut p)? else {
                return Err(p.unexpected("CREATE SINK"));
            };
            Stmt::TestNewSink {
                def,
                sample: sample_clause(&mut p)?,
            }
        }
    } else if p.peek_kw("TEST")
        && (p.peek_kw_at(1, "CONNECTION")
            || p.peek_kw_at(1, "SINK")
            || (p.peek_kw_at(1, "PLATFORM") && p.peek_kw_at(2, "CONNECTION")))
    {
        p.need_kw("TEST")?;
        if p.kw("SINK") {
            let name = p.qname()?;
            let sample = sample_clause(&mut p)?;
            Stmt::TestSink { name, sample }
        } else {
            let platform = p.kw("PLATFORM");
            p.need_kw("CONNECTION")?;
            let q = p.qname()?;
            let conn = ConnName {
                name: q.name,
                platform,
            };
            if p.kw("WRITE") {
                let to = if p.kw("TO") { Some(p.string()?) } else { None };
                Stmt::TestConnectionWrite { conn, db: q.db, to }
            } else {
                Stmt::TestConnection(conn, q.db)
            }
        }
    } else if p.peek_kw("GRANT") || p.peek_kw("REVOKE") {
        let grant = p.kw("GRANT");
        if !grant {
            p.need_kw("REVOKE")?;
        }
        if !p.kws(&["USAGE", "ON"]) {
            return Ok(None);
        }
        if !(p.peek_kw("CONNECTION") || p.peek_kw("PLATFORM")) {
            return Ok(None);
        }
        let platform = p.kw("PLATFORM");
        p.need_kw("CONNECTION")?;
        let q = p.qname()?;
        if grant {
            p.need_kw("TO")?;
        } else {
            p.need_kw("FROM")?;
        }
        let user = p.ident().or_else(|_| p.string())?;
        if p.punct("@") {
            let _ = p.ident().or_else(|_| p.string())?;
        }
        Stmt::Usage {
            grant,
            conn: ConnName {
                name: q.name,
                platform,
            },
            db: q.db,
            user,
        }
    } else {
        return Ok(None);
    };
    p.end()?;
    Ok(Some(st))
}

/// `[WITH SAMPLE n]` of `TEST SINK`: 1 to 1,000, 10 when absent.
fn sample_clause(p: &mut P<'_>) -> Result<u32> {
    if !p.kws(&["WITH", "SAMPLE"]) {
        return Ok(10);
    }
    match p.peek() {
        Some(t) if t.kind == Tk::Num => {
            p.i += 1;
            t.text
                .parse::<u32>()
                .ok()
                .filter(|n| (1..=1000).contains(n))
                .ok_or_else(|| p.unexpected("a sample of 1 to 1000 records"))
        }
        _ => Err(p.unexpected("a sample of 1 to 1000 records")),
    }
}

fn show_from(p: &mut P<'_>) -> Result<Option<String>> {
    if p.kw("FROM") || p.kw("IN") {
        Ok(Some(p.ident()?))
    } else {
        Ok(None)
    }
}

fn create_connection(p: &mut P<'_>) -> Result<Stmt> {
    let if_not_exists = p.kws(&["IF", "NOT", "EXISTS"]);
    let name = p.qname()?;
    if p.kws(&["FOR", "EACH", "TENANT"]) {
        return Err(err(
            SprigCode::SinkOptionInvalid,
            "FOR EACH TENANT connections are not built yet",
        ));
    }
    p.need_kw("TYPE")?;
    let k = p.ident()?;
    let kind = match k.to_ascii_lowercase().as_str() {
        "mariadb" => ConnKind::Mysql,
        "postgres" => ConnKind::Postgresql,
        other => ConnKind::parse(other).ok_or_else(|| {
            crate::model::bad_option(format!(
                "unknown connection type {k}: webhook, kafka, s3, mysql or postgresql"
            ))
        })?,
    };
    let options = if p.kw("WITH") {
        p.options()?
    } else {
        Vec::new()
    };
    let mut credentials = Vec::new();
    let mut required_credentials = Vec::new();
    if p.kw("CREDENTIALS") {
        for (k, v) in p.options()? {
            match v {
                OptVal::Text(s) => credentials.push((k, s)),
                OptVal::Required => required_credentials.push(k),
                OptVal::Bare(_) => {
                    return Err(err(
                        SprigCode::SinkCredentialsInvalid,
                        format!("credential {k} must be a quoted string"),
                    ));
                }
            }
        }
    }
    Ok(Stmt::CreateConnection(CreateConnection {
        name,
        if_not_exists,
        kind,
        options,
        credentials,
        required_credentials,
    }))
}

/// Words that end a `WHERE` condition at depth 0.
const AFTER_WHERE: &[&str] = &["COLUMNS", "EXCEPT", "TRANSFORM", "TO"];

#[allow(clippy::too_many_lines)]
fn create_sink(p: &mut P<'_>) -> Result<Stmt> {
    let if_not_exists = p.kws(&["IF", "NOT", "EXISTS"]);
    let name = p.qname()?;
    if p.kws(&["FOR", "EACH", "TENANT"]) {
        return Err(err(
            SprigCode::SinkOptionInvalid,
            "FOR EACH TENANT sinks are not built yet (chapter 51, build step 20)",
        ));
    }
    p.need_kw("FROM")?;
    let source = if p.kw("STREAM") {
        SourceAst::Stream(p.qname()?)
    } else if p.kw("QUEUE") {
        SourceAst::Queue(p.qname()?)
    } else if p.kws(&["CHANGES", "OF"]) {
        let mut v = vec![p.qname()?];
        while p.punct(",") {
            v.push(p.qname()?);
        }
        SourceAst::Changes(v)
    } else {
        return Err(p.unexpected("expected STREAM, QUEUE or CHANGES OF"));
    };
    let source = if p.kws(&["FAN", "IN"]) {
        match source {
            SourceAst::Stream(q) => {
                let under = if p.kw("UNDER") {
                    Some(p.qname()?.name)
                } else {
                    None
                };
                SourceAst::FanIn { stream: q, under }
            }
            SourceAst::Queue(q) => {
                let under = if p.kw("UNDER") {
                    Some(p.qname()?.name)
                } else {
                    None
                };
                SourceAst::FanInQueue { queue: q, under }
            }
            _ => {
                return Err(err(
                    SprigCode::SinkOptionInvalid,
                    "FAN IN reads a stream or a queue, not CHANGES OF",
                ));
            }
        }
    } else {
        source
    };
    let mut filter = None;
    if p.kw("WHERE") {
        let start = p.peek().map_or(p.sql.len(), |t| t.range.start);
        let mut depth = 0i32;
        let mut end = start;
        while let Some(t) = p.peek() {
            if t.kind == Tk::Punct && t.text == "(" {
                depth += 1;
            } else if t.kind == Tk::Punct && t.text == ")" {
                depth -= 1;
            } else if depth == 0
                && t.kind == Tk::Word
                && AFTER_WHERE.iter().any(|w| t.text.eq_ignore_ascii_case(w))
            {
                break;
            }
            end = t.range.end;
            p.i += 1;
        }
        let f = p.sql[start..end].trim().to_owned();
        if f.is_empty() {
            return Err(p.unexpected("expected a condition after WHERE"));
        }
        filter = Some(f);
    }
    let mut projection = None;
    if p.kws(&["EXCEPT", "COLUMNS"]) {
        projection = Some(Projection::Except {
            columns: p.name_list()?,
        });
    } else if p.kw("COLUMNS") {
        projection = Some(Projection::Only {
            columns: p.name_list()?,
        });
    }
    let transform = if p.kw("TRANSFORM") {
        Some(p.ident()?)
    } else {
        None
    };
    p.need_kw("TO")?;
    let target = if p.kw("WEBHOOK") {
        let conn = p.conn_name()?;
        let path = if p.kw("PATH") {
            Some(p.string()?)
        } else {
            None
        };
        TargetAst::Webhook { conn, path }
    } else if p.kw("KAFKA") {
        let conn = p.conn_name()?;
        p.need_kw("TOPIC")?;
        let topic = p.string()?;
        let key = if p.kw("KEY") { Some(p.string()?) } else { None };
        TargetAst::Kafka { conn, topic, key }
    } else if p.kw("S3") {
        let conn = p.conn_name()?;
        p.need_kw("PREFIX")?;
        TargetAst::S3 {
            conn,
            prefix: p.string()?,
        }
    } else if p.kw("DATABASE") {
        let conn = p.conn_name()?;
        p.need_kw("TABLE")?;
        TargetAst::Database {
            conn,
            table: p.string()?,
        }
    } else if p.kw("SCRIPT") {
        let (language, function) = if p.kw("LANGUAGE") {
            let l = p.ident()?;
            let lang = match l.to_ascii_lowercase().as_str() {
                "js" => Some(ScriptLanguage::Javascript),
                other => ScriptLanguage::parse(other),
            }
            .ok_or_else(|| {
                crate::model::bad_option(format!(
                    "unknown script language {l}: javascript, kirun or lua"
                ))
            })?;
            (Some(lang), None)
        } else if p.kw("FUNCTION") {
            (None, Some(p.ident()?))
        } else {
            return Err(p.unexpected("expected LANGUAGE or FUNCTION"));
        };
        let mut using = Vec::new();
        if p.kws(&["USING", "CONNECTIONS"]) {
            p.need_punct("(")?;
            loop {
                let platform = p.kw("PLATFORM");
                using.push(ConnName {
                    name: p.ident()?,
                    platform,
                });
                if p.punct(")") {
                    break;
                }
                p.need_punct(",")?;
            }
        }
        let mut source = None;
        if language.is_some() {
            p.need_kw("AS")?;
            match p.peek() {
                Some(t) if t.kind == Tk::Dollar || t.kind == Tk::Str => {
                    p.i += 1;
                    source = Some(t.text.clone());
                }
                _ => return Err(p.unexpected("expected $$ ... $$")),
            }
        }
        TargetAst::Script {
            language,
            source,
            function,
            using,
        }
    } else {
        return Err(p.unexpected("expected WEBHOOK, KAFKA, S3, DATABASE or SCRIPT"));
    };
    let mut guarantee = Guarantee::AtLeastOnce;
    let mut dead_letter = None;
    let mut start_at = StartAt::Latest;
    let mut options = Vec::new();
    loop {
        if p.kw("GUARANTEE") {
            guarantee = if p.kws(&["AT", "LEAST", "ONCE"]) {
                Guarantee::AtLeastOnce
            } else if p.kws(&["EXACTLY", "ONCE"]) {
                Guarantee::ExactlyOnce
            } else {
                return Err(p.unexpected("expected AT LEAST ONCE or EXACTLY ONCE"));
            };
        } else if p.kws(&["DEAD", "LETTER", "TO"]) {
            dead_letter = Some(if p.kw("STREAM") {
                DeadAst::Stream(p.ident()?)
            } else if p.kw("QUEUE") {
                DeadAst::Queue(p.ident()?)
            } else if p.kw("EXCHANGE") {
                DeadAst::Exchange(p.ident()?)
            } else {
                return Err(p.unexpected("expected STREAM, QUEUE or EXCHANGE"));
            });
        } else if p.kws(&["START", "AT"]) {
            start_at = p.start_at()?;
        } else if p.peek_kw("WITH") && !p.peek_kw_at(1, "SAMPLE") {
            // (`WITH SAMPLE n` ends `TEST CREATE SINK`, ST69.)
            p.need_kw("WITH")?;
            options = p.options()?;
        } else {
            break;
        }
    }
    Ok(Stmt::CreateSink(Box::new(CreateSink {
        name,
        if_not_exists,
        source,
        filter,
        projection,
        transform,
        target,
        guarantee,
        dead_letter,
        start_at,
        options,
    })))
}

/// The statement with every credential value replaced by `'***'`, and the
/// values taken out (for sealing), when it has a `CREDENTIALS (...)`
/// clause. `None` when it has none. Used before the text reaches capture,
/// history, logs or traces.
#[must_use]
pub fn redact(sql: &str) -> Option<(String, Vec<(String, zeroize::Zeroizing<String>)>)> {
    let up = sql.to_ascii_uppercase();
    if !up.contains("CREDENTIALS") {
        return None;
    }
    let t = tokens(sql).ok()?;
    let at = t
        .iter()
        .position(|x| x.kind == Tk::Word && x.text.eq_ignore_ascii_case("CREDENTIALS"))?;
    let mut out = String::with_capacity(sql.len());
    let mut values = Vec::new();
    let mut last = 0;
    let mut i = at + 1;
    if !t
        .get(i)
        .is_some_and(|x| x.kind == Tk::Punct && x.text == "(")
    {
        return None;
    }
    i += 1;
    while let Some(x) = t.get(i) {
        if x.kind == Tk::Punct && x.text == ")" {
            break;
        }
        if x.kind == Tk::Punct && x.text == "=" {
            if let (Some(k), Some(v)) = (t.get(i - 1), t.get(i + 1)) {
                out.push_str(&sql[last..v.range.start]);
                out.push_str("'***'");
                last = v.range.end;
                values.push((
                    k.text.to_ascii_lowercase(),
                    zeroize::Zeroizing::new(v.text.clone()),
                ));
            }
        }
        i += 1;
    }
    out.push_str(&sql[last..]);
    Some((out, values))
}

/// `WITH` options as a map of text (keys without `sink.`).
#[must_use]
pub fn options_map(opts: &[(String, OptVal)]) -> BTreeMap<String, String> {
    opts.iter()
        .map(|(k, v)| {
            (
                k.strip_prefix("sink.").unwrap_or(k).to_owned(),
                v.as_text().to_owned(),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connections_parse() {
        let s = parse(
            "CREATE CONNECTION billing_api TYPE WEBHOOK
               WITH (url = 'https://billing.example.com', auth = 'bearer', http2 = off)
               CREDENTIALS (token = 's3cr3t', signing_secret = 'whsec_x''y')",
        )
        .unwrap()
        .unwrap();
        let Stmt::CreateConnection(c) = s else {
            panic!()
        };
        assert_eq!(c.kind, ConnKind::Webhook);
        assert_eq!(c.options[2], ("http2".into(), OptVal::Bare("off".into())));
        assert_eq!(c.credentials[1].1, "whsec_x'y");
        assert!(matches!(
            parse("ALTER CONNECTION kafka_eu SET CREDENTIALS (password = 'p')").unwrap(),
            Some(Stmt::AlterConnection(_, AlterConnection::Credentials(_)))
        ));
        assert!(matches!(
            parse("DROP CONNECTION IF EXISTS shop.reporting;").unwrap(),
            Some(Stmt::DropConnection {
                if_exists: true,
                ..
            })
        ));
        assert!(matches!(
            parse("GRANT USAGE ON CONNECTION billing_api TO alice@'%'").unwrap(),
            Some(Stmt::Usage { grant: true, .. })
        ));
        assert_eq!(parse("GRANT SELECT ON t TO alice").unwrap(), None);
        assert_eq!(parse("SELECT 1").unwrap(), None);
        assert!(parse("CREATE CONNECTION x TYPE ftp").is_err());
    }

    #[test]
    fn sinks_parse() {
        let s = parse(
            "CREATE SINK IF NOT EXISTS orders_to_billing
               FROM STREAM order_events
               WHERE value->>'$.type' = 'order.paid' AND (key IS NOT NULL)
               TO WEBHOOK CONNECTION billing_api PATH '/hooks/orders'
               GUARANTEE AT LEAST ONCE
               START AT TIMESTAMP '2026-11-09T10:00Z'
               WITH (format = 'cloudevents', batch_max_records = 100, sink.lanes = 4)",
        )
        .unwrap()
        .unwrap();
        let Stmt::CreateSink(c) = s else { panic!() };
        assert_eq!(
            c.filter.as_deref(),
            Some("value->>'$.type' = 'order.paid' AND (key IS NOT NULL)")
        );
        assert_eq!(
            c.start_at,
            StartAt::Timestamp {
                ms: 1_794_218_400_000
            }
        );
        assert_eq!(options_map(&c.options)["lanes"], "4");
        let s = parse(
            "CREATE SINK orders_pg FROM CHANGES OF orders, items EXCEPT COLUMNS (card_token)
               TO DATABASE CONNECTION reporting TABLE 'public.orders'
               GUARANTEE EXACTLY ONCE START AT SNAPSHOT WITH (schema_policy = 'add_columns')",
        )
        .unwrap()
        .unwrap();
        let Stmt::CreateSink(c) = s else { panic!() };
        assert!(matches!(c.source, SourceAst::Changes(ref v) if v.len() == 2));
        assert_eq!(c.guarantee, Guarantee::ExactlyOnce);
        let s = parse(
            "CREATE SINK crm FROM QUEUE jobs TO SCRIPT LANGUAGE javascript USING CONNECTIONS (crm_api)
             AS $$ export async function deliver(b, ctx) { return {ok: true}; } $$
             WITH (batch_max_records = 200)",
        )
        .unwrap()
        .unwrap();
        let Stmt::CreateSink(c) = s else { panic!() };
        assert!(
            matches!(c.target, TargetAst::Script { ref source, .. } if source.as_deref().is_some_and(|s| s.contains("deliver")))
        );
        assert!(matches!(
            parse("CREATE SINK k FROM STREAM s TO KAFKA PLATFORM CONNECTION kafka_main TOPIC 'acme.o'").unwrap(),
            Some(Stmt::CreateSink(c)) if matches!(c.target, TargetAst::Kafka { ref conn, .. } if conn.platform)
        ));
        assert!(matches!(
            parse("ALTER SINK x RESET TO EARLIEST").unwrap(),
            Some(Stmt::AlterSink(_, AlterSink::ResetTo(StartAt::Earliest)))
        ));
        assert!(matches!(
            parse("ALTER SINK x PAUSE").unwrap(),
            Some(Stmt::AlterSink(_, AlterSink::Pause))
        ));
        assert!(matches!(
            parse("DROP SINK x WITH DEAD LETTERS").unwrap(),
            Some(Stmt::DropSink {
                with_dead_letters: true,
                ..
            })
        ));
        assert!(matches!(
            parse("SHOW SINK STATUS shop.x").unwrap(),
            Some(Stmt::ShowSinkStatus(_))
        ));
        assert!(parse("CREATE SINK x FROM STREAM s TO NOWHERE").is_err());
        assert!(
            parse("CREATE SINK x FOR EACH TENANT FROM STREAM s TO WEBHOOK CONNECTION c").is_err()
        );
    }

    #[test]
    fn inherited_connections_and_overrides() {
        // A definition's credentials may be REQUIRED (the clause is cut
        // out by the gateway before this grammar sees the statement).
        let Some(Stmt::CreateConnection(c)) = parse(
            "CREATE CONNECTION hooks TYPE WEBHOOK WITH (url = REQUIRED, auth = 'bearer') \
             CREDENTIALS (token = REQUIRED, signing_secret = REQUIRED)",
        )
        .unwrap() else {
            panic!("not a connection");
        };
        assert_eq!(c.required_credentials, vec!["token", "signing_secret"]);
        assert!(c.credentials.is_empty());
        assert_eq!(c.options[0], ("url".into(), OptVal::Required));
        let a = parse_inherited(
            "ALTER INHERITED CONNECTION sec.hooks FOR TENANT acme SET (url = 'https://a.example') \
             CREDENTIALS (token = 't1')",
        )
        .unwrap()
        .unwrap();
        assert!(!a.sink);
        assert_eq!(a.name.db.as_deref(), Some("sec"));
        assert_eq!(a.target, InheritTarget::Tenant("acme".into()));
        assert_eq!(
            a.op,
            InheritedOp::Set {
                options: vec![("url".into(), OptVal::Text("https://a.example".into()))],
                credentials: vec![("token".into(), "t1".into())],
            }
        );
        let a = parse_inherited("ALTER INHERITED CONNECTION hooks SET CREDENTIALS (token = 'x')")
            .unwrap()
            .unwrap();
        assert_eq!(a.target, InheritTarget::Own);
        assert!(matches!(a.op, InheritedOp::Set { ref options, .. } if options.is_empty()));
        let a = parse_inherited("ALTER INHERITED SINK k FOR SUBTREE DISABLE")
            .unwrap()
            .unwrap();
        assert!(a.sink);
        assert_eq!(
            (a.target, a.op),
            (InheritTarget::Subtree(None), InheritedOp::Disable)
        );
        let a = parse_inherited("ALTER INHERITED SINK k FOR SUBTREE b RESET (batch_max_records)")
            .unwrap()
            .unwrap();
        assert_eq!(
            (a.target, a.op),
            (
                InheritTarget::Subtree(Some("b".into())),
                InheritedOp::Reset(vec!["batch_max_records".into()])
            )
        );
        assert!(
            parse_inherited("ALTER INHERITED STREAM s SPRIG SET (x = 1)")
                .unwrap()
                .is_none()
        );
        assert!(
            parse_inherited("ALTER INHERITED CONNECTION c SET CREDENTIALS (token = x)").is_err()
        );
        // The redaction covers ALTER INHERITED too.
        let (r, v) =
            redact("ALTER INHERITED CONNECTION c SET CREDENTIALS (token = 'zz9')").unwrap();
        assert!(!r.contains("zz9"), "{r}");
        assert_eq!(v.len(), 1);
    }

    #[test]
    fn fan_in_sources() {
        let Some(Stmt::CreateSink(c)) = parse(
            "CREATE SINK all_events FROM STREAM security.user_events FAN IN UNDER client_x \
             TO WEBHOOK CONNECTION h",
        )
        .unwrap() else {
            panic!("not a sink");
        };
        assert!(matches!(
            c.source,
            SourceAst::FanIn { ref stream, ref under }
                if stream.name == "user_events" && under.as_deref() == Some("client_x")
        ));
        let Some(Stmt::CreateSink(c)) =
            parse("CREATE SINK a FROM STREAM s FAN IN TO WEBHOOK CONNECTION h").unwrap()
        else {
            panic!("not a sink");
        };
        assert!(matches!(c.source, SourceAst::FanIn { under: None, .. }));
        // R9 (r9queue): QUEUE q FAN IN.
        let Some(Stmt::CreateSink(c)) =
            parse("CREATE SINK a FROM QUEUE jobs.work FAN IN UNDER x TO WEBHOOK CONNECTION h")
                .unwrap()
        else {
            panic!("not a sink");
        };
        assert!(matches!(
            c.source,
            SourceAst::FanInQueue { ref queue, ref under }
                if queue.name == "work" && under.as_deref() == Some("x")
        ));
        assert!(parse("CREATE SINK a FROM CHANGES OF t FAN IN TO WEBHOOK CONNECTION h").is_err());
    }

    #[test]
    fn credentials_are_redacted() {
        let sql = "CREATE CONNECTION k TYPE KAFKA WITH (bootstrap = 'b:9093') CREDENTIALS (username = 'sprig', password = 'p''w\\'d')";
        let (r, v) = redact(sql).unwrap();
        assert_eq!(
            r,
            "CREATE CONNECTION k TYPE KAFKA WITH (bootstrap = 'b:9093') CREDENTIALS (username = '***', password = '***')"
        );
        assert_eq!(v[1].0, "password");
        assert_eq!(v[1].1.as_str(), "p'w'd");
        assert!(redact("SELECT 1").is_none());
    }

    #[test]
    fn times_parse() {
        assert_eq!(parse_time("1970-01-01"), Some(0));
        assert_eq!(
            parse_time("2026-11-09T10:00:00.5Z"),
            Some(1_794_218_400_500)
        );
        assert_eq!(parse_time("nope"), None);
    }

    #[test]
    fn test_statements_parse() {
        // R9 (r9tests): TEST CONNECTION and TEST SINK.
        assert_eq!(
            parse("TEST CONNECTION shop.k").unwrap(),
            Some(Stmt::TestConnection(
                ConnName {
                    name: "k".into(),
                    platform: false
                },
                Some("shop".into())
            ))
        );
        assert_eq!(
            parse("test platform connection pk;").unwrap(),
            Some(Stmt::TestConnection(
                ConnName {
                    name: "pk".into(),
                    platform: true
                },
                None
            ))
        );
        assert_eq!(
            parse("TEST SINK s").unwrap(),
            Some(Stmt::TestSink {
                name: QName {
                    db: None,
                    name: "s".into()
                },
                sample: 10
            })
        );
        assert!(matches!(
            parse("TEST SINK shop.s WITH SAMPLE 25").unwrap(),
            Some(Stmt::TestSink { sample: 25, .. })
        ));
        assert!(parse("TEST SINK s WITH SAMPLE 0").is_err());
        assert!(parse("TEST SINK s WITH SAMPLE 1001").is_err());
        assert!(parse("TEST SINK s WITH SAMPLE x").is_err());
        assert!(parse("TEST CONNECTION").is_err());
        // R9 (r9last): WRITE [TO 'target'].
        assert_eq!(
            parse("TEST CONNECTION shop.k WRITE TO 'audit'").unwrap(),
            Some(Stmt::TestConnectionWrite {
                conn: ConnName {
                    name: "k".into(),
                    platform: false
                },
                db: Some("shop".into()),
                to: Some("audit".into())
            })
        );
        assert!(matches!(
            parse("test platform connection pk write").unwrap(),
            Some(Stmt::TestConnectionWrite { to: None, .. })
        ));
        assert!(parse("TEST CONNECTION k WRITE TO").is_err());
        // Not ours: a table or a word that merely starts like it.
        assert_eq!(parse("TEST_SINK_TABLE").unwrap(), None);
    }

    #[test]
    fn tests_of_unsaved_definitions() {
        // ST68: TEST CREATE CONNECTION, credentials and all; redacted as
        // CREATE CONNECTION is.
        let sql = "TEST CREATE CONNECTION crm TYPE webhook WITH (url = 'https://h.example.com/in', auth = 'bearer') CREDENTIALS (token = 'very-secret')";
        let Some(Stmt::TestNewConnection(c)) = parse(sql).unwrap() else {
            panic!("not a connection test")
        };
        assert_eq!((c.name.name.as_str(), c.kind), ("crm", ConnKind::Webhook));
        assert_eq!(c.credentials, vec![("token".into(), "very-secret".into())]);
        let (shown, values) = redact(sql).unwrap();
        assert!(!shown.contains("very-secret") && shown.contains("token = '***'"));
        assert_eq!(values.len(), 1);
        // ST69: TEST CREATE SINK, its own WITH (...) options and then WITH
        // SAMPLE n.
        let Some(Stmt::TestNewSink { def, sample }) = parse(
            "TEST CREATE SINK out FROM STREAM shop.orders WHERE amount > 10 TO WEBHOOK CONNECTION crm WITH (batch_max_records = 5) WITH SAMPLE 25",
        )
        .unwrap() else {
            panic!("not a sink test")
        };
        assert_eq!((def.name.name.as_str(), sample), ("out", 25));
        assert_eq!(def.options.len(), 1);
        assert!(def.filter.is_some());
        assert!(matches!(
            parse("test create sink s from queue q to webhook connection c").unwrap(),
            Some(Stmt::TestNewSink { sample: 10, .. })
        ));
        assert!(
            parse("TEST CREATE SINK s FROM QUEUE q TO WEBHOOK CONNECTION c WITH SAMPLE 0").is_err()
        );
        assert!(
            parse("TEST CREATE SINK s FROM QUEUE q TO WEBHOOK CONNECTION c WITH SAMPLE 5 PAUSE")
                .is_err()
        );
        // CREATE SINK's own WITH (...) still parses.
        assert!(matches!(
            parse(
                "CREATE SINK s FROM QUEUE q TO WEBHOOK CONNECTION c WITH (batch_max_records = 5)"
            )
            .unwrap(),
            Some(Stmt::CreateSink(_))
        ));
        // Not a test of anything else.
        assert_eq!(parse("TEST CREATE TABLE t").unwrap(), None);
    }
}
