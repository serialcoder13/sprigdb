//! SQL fingerprints: the normalizer and the digest (chapter 12, "SQL
//! fingerprints"; build step 1).
//!
//! [`normalize`] reads one statement through `sprig-sql`'s persona-aware
//! lexer and writes its normalized text:
//!
//! - comments are dropped; executable comments the persona runs
//!   (`/*!50001 ... */`, `/*M!100100 ... */` on MariaDB) are replaced by
//!   their normalized content, and the ones it skips are dropped like any
//!   comment; optimizer hints `/*+ ... */` are kept, normalized inside;
//! - literals (numbers, strings, hex and bit values, `DATE '...'` and the
//!   other typed literals, `_charset'...'`, `TRUE`, `FALSE`) and
//!   placeholders become `?`; a unary `-` or `+` before a number is folded
//!   into it; adjacent strings (`'a' 'b'`) are one literal;
//! - keywords and function names are upper case, columns lower case and
//!   unquoted, aliases and qualifiers as written (unquoted), table
//!   references are qualified with the logical database (`shop.orders`);
//! - `IN (?, ?, ...)` becomes `IN (...)`, `VALUES (...), (...)` becomes
//!   `VALUES (...)`, and a list of tuples of literals `((?, ?), (?, ?))`
//!   becomes `(...)`;
//! - tokens are joined with single spaces (none around `.`, inside
//!   parentheses, before `,` or between a function and its `(`), and the
//!   text is cut at `workload.max_text_bytes`.
//!
//! [`digest`] is `xxh3_64(kind byte ‖ db_id as u32 LE ‖ caller_fn_id as
//! u64 LE ‖ text)` over the whole normalized text (before truncation).
//! For SQL, `db_id` is the database of the first table the statement
//! names, else the session's: the same statement from sessions with
//! different default databases is one fingerprint.
//!
//! Each `?` and each collapsed `(...)` of the text is a **slot**; the
//! statement's literals are listed with their slot ([`Literal::slot`]),
//! and shape predicates name slots in their `args`.

mod cache;
pub mod compound;
mod keywords;
pub mod mongo;
mod normalize;

use std::ops::Range;
use std::sync::Arc;

use sprig_common::ids::DbId;
use sprig_proto_mysql::packets::Persona;
use sprig_sql::lex::LexOptions;

use crate::shape::Shape;

pub use cache::{DEFAULT_CACHE_ENTRIES, SqlFingerprinter};
pub use normalize::{Normalized, TableName, normalize};

/// A fingerprint id: `xxh3_64` of the kind, database, caller and
/// normalized text. Textual form: 16 lowercase hex digits.
pub type FingerprintId = sprig_common::ids::Fingerprint;

/// What was fingerprinted. The discriminant is the 1-based index of the
/// `workload_fingerprints.kind` ENUM, and the kind byte of the digest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum FpKind {
    /// A SQL statement (MySQL and MariaDB ports, SQL over HTTP).
    Sql = 1,
    /// A MongoDB query shape (R5).
    Mongo = 2,
    /// A Redis command pattern (R6).
    Redis = 3,
    /// A key-value API pattern (R6).
    Kv = 4,
    /// A vector API pattern (R6).
    Vector = 5,
    /// A search request (R5).
    Search = 6,
    /// An Analytics API pattern (R6).
    Analytics = 7,
    /// An HTTP/REST or gRPC call (R6).
    Api = 8,
    /// A logic-layer invocation (R7).
    Logic = 9,
    /// A transaction footprint.
    Txn = 10,
}

impl FpKind {
    /// The `workload_fingerprints.kind` value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sql => "sql",
            Self::Mongo => "mongo",
            Self::Redis => "redis",
            Self::Kv => "kv",
            Self::Vector => "vector",
            Self::Search => "search",
            Self::Analytics => "analytics",
            Self::Api => "api",
            Self::Logic => "logic",
            Self::Txn => "txn",
        }
    }
}

/// The front end a fingerprinted request came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrontEnd {
    /// The MySQL 8.4 persona (3306).
    Mysql,
    /// The MariaDB persona (3307).
    Mariadb,
    /// MongoDB (27017).
    Mongo,
    /// Redis (6379).
    Redis,
    /// HTTP, including SQL over HTTP.
    Http,
    /// gRPC.
    Grpc,
    /// A logic-layer `db.*` call.
    Logic,
    /// SprigDB's own statements.
    Internal,
}

impl FrontEnd {
    /// The `workload_fingerprints.front_end` value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mysql => "mysql",
            Self::Mariadb => "mariadb",
            Self::Mongo => "mongo",
            Self::Redis => "redis",
            Self::Http => "http",
            Self::Grpc => "grpc",
            Self::Logic => "logic",
            Self::Internal => "internal",
        }
    }

    /// The front end of a SQL persona.
    #[must_use]
    pub const fn of_persona(p: Persona) -> Self {
        match p {
            Persona::MySql84 => Self::Mysql,
            Persona::MariaDb => Self::Mariadb,
        }
    }
}

/// The logic function a `db.*` call ran in (chapter 41). Part of the id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Caller {
    /// The function's id.
    pub function_id: u64,
    /// Its deployed version.
    pub version: u64,
}

impl Caller {
    /// The reserved function id of statements run from Studio (chapter
    /// 44; chapter 12: "recorded with `caller = studio` in their own
    /// statistics bucket"). No logic function has it: function ids are
    /// allocated from 1 upwards.
    pub const STUDIO_FUNCTION_ID: u64 = u64::MAX;

    /// `caller = studio`.
    #[must_use]
    pub const fn studio() -> Self {
        Self {
            function_id: Self::STUDIO_FUNCTION_ID,
            version: 0,
        }
    }

    /// Whether this is [`Caller::studio`].
    #[must_use]
    pub const fn is_studio(&self) -> bool {
        self.function_id == Self::STUDIO_FUNCTION_ID
    }
}

/// Per-fingerprint flags, the `workload_fingerprints.flags` SET.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[allow(clippy::struct_excessive_bools)]
pub struct FpFlags {
    /// The normalized text was cut at `workload.max_text_bytes`.
    pub truncated: bool,
    /// Another text had the same id; this one was hashed with seed 1.
    pub collision: bool,
    /// The reserved "other" fingerprint of its kind (the tenant's cap).
    pub other: bool,
    /// The reserved "unparsed" fingerprint of its kind.
    pub unparsed: bool,
    /// Registered at deploy time, with no runtime counts (chapter 41).
    pub r#static: bool,
}

impl FpFlags {
    /// The SET value: `truncated,collision`, or empty.
    #[must_use]
    pub fn to_set(self) -> String {
        let names = [
            (self.truncated, "truncated"),
            (self.collision, "collision"),
            (self.other, "other"),
            (self.unparsed, "unparsed"),
            (self.r#static, "static"),
        ];
        names
            .iter()
            .filter(|(on, _)| *on)
            .map(|(_, n)| *n)
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// The type of a literal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LiteralKind {
    /// A decimal or approximate number, with its folded sign.
    Number,
    /// A string (`'x'`, `"x"` without `ANSI_QUOTES`, `N'x'`, `_utf8mb4'x'`).
    String,
    /// `X'0A'` or `0x0A`.
    Hex,
    /// `B'01'` or `0b01`.
    Bit,
    /// `TRUE` or `FALSE`.
    Bool,
    /// `DATE '...'`, `TIME '...'`, `TIMESTAMP '...'`.
    Temporal,
    /// A `?` placeholder: the value comes with `COM_STMT_EXECUTE`. Its
    /// position among the statement's placeholders is
    /// [`Literal::param_index`].
    Param,
}

/// One literal of one execution: where it is in the statement text and
/// which slot of the normalized text it fills.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Literal {
    /// Its type.
    pub kind: LiteralKind,
    /// Its bytes in the statement text, including a folded sign, a charset
    /// introducer or a `DATE` keyword.
    pub span: Range<u32>,
    /// The slot it fills: the index of its `?` or collapsed `(...)` in the
    /// normalized text. Several literals share the slot of a collapsed
    /// list.
    pub slot: u32,
    /// For [`LiteralKind::Param`]: its 0-based index among the
    /// statement's placeholders. Zero otherwise.
    pub param_index: u32,
}

impl Literal {
    /// The literal's text in `stmt`, the statement it came from.
    #[must_use]
    pub fn text<'a>(&self, stmt: &'a [u8]) -> &'a [u8] {
        stmt.get(self.span.start as usize..self.span.end as usize)
            .unwrap_or_default()
    }
}

/// Everything fingerprinting a SQL statement depends on besides its text.
#[derive(Debug, Clone, Copy)]
pub struct SqlCtx<'a> {
    /// Persona, version and `sql_mode` flags for the lexer.
    pub lex: LexOptions,
    /// The session's current logical database, which qualifies
    /// unqualified table references.
    pub current_db: Option<&'a str>,
    /// The current database's id, `DbId(0)` when there is none. Part of the
    /// id of a statement that names no table; otherwise the first table's
    /// database is.
    pub db_id: DbId,
    /// Where the statement came from.
    pub front_end: FrontEnd,
    /// The logic function, for its `db.sql` calls. Part of the id.
    pub caller: Option<Caller>,
    /// `names.lower_case_table_names = 1`: database, table and alias names
    /// are lower-cased.
    pub lower_case_table_names: bool,
    /// `workload.max_text_bytes` (default 4096).
    pub max_text_bytes: usize,
}

/// `workload.max_text_bytes`'s default.
pub const DEFAULT_MAX_TEXT_BYTES: usize = 4096;

impl SqlCtx<'_> {
    /// A context for a persona's defaults: no database, no caller.
    #[must_use]
    pub const fn new(persona: Persona) -> Self {
        Self {
            lex: LexOptions::for_persona(persona),
            current_db: None,
            db_id: DbId(0),
            front_end: FrontEnd::of_persona(persona),
            caller: None,
            lower_case_table_names: false,
            max_text_bytes: DEFAULT_MAX_TEXT_BYTES,
        }
    }
}

impl<'a> SqlCtx<'a> {
    /// With a current database and its id.
    #[must_use]
    pub const fn current_db(mut self, db: &'a str, id: DbId) -> Self {
        self.current_db = Some(db);
        self.db_id = id;
        self
    }
}

/// A fingerprinted request (chapter 12, "Interfaces"): its id, normalized
/// text and shape, shared by every execution, plus this execution's
/// literals.
#[derive(Debug, Clone)]
pub struct Fingerprint {
    /// The id.
    pub id: FingerprintId,
    /// What kind of request.
    pub kind: FpKind,
    /// The normalized text, at most `workload.max_text_bytes`.
    pub text: Arc<str>,
    /// Predicates, joins, order and columns.
    pub shape: Arc<Shape>,
    /// This execution's literals, in text order. Empty for statements that
    /// carry passwords ([`Fingerprint::sensitive`]).
    pub literals: Vec<Literal>,
    /// Where it came from.
    pub front_end: FrontEnd,
    /// The logic function, for its `db.*` calls.
    pub caller: Option<Caller>,
    /// The logical database part of the id: the database of the first
    /// table the statement names, else the session's (`DbId(0)` for none
    /// or an unknown one).
    pub db_id: DbId,
    /// Flags for `workload_fingerprints.flags`.
    pub flags: FpFlags,
    /// The longest collapsed `IN` list of this execution, in elements.
    pub in_list_max: u32,
    /// The statement carries a password (`IDENTIFIED BY`, `PASSWORD`): its
    /// literals were dropped, and it is never sampled.
    pub sensitive: bool,
    /// The slots that are collapsed lists (`IN (...)`, `VALUES (...)`),
    /// ascending: in a sample their literals form one list.
    pub list_slots: Vec<u32>,
}

/// The id of a normalized text (chapter 12): `xxh3_64(kind byte ‖ db_id as
/// u32 LE ‖ caller_fn_id as u64 LE (0 if none) ‖ text)`.
#[must_use]
pub fn digest(kind: FpKind, db_id: DbId, caller_fn_id: Option<u64>, text: &str) -> FingerprintId {
    digest_seeded(kind, db_id, caller_fn_id, text, 0)
}

/// [`digest`] with a seed: seed 1 is the second text of a collision.
#[must_use]
pub fn digest_seeded(
    kind: FpKind,
    db_id: DbId,
    caller_fn_id: Option<u64>,
    text: &str,
    seed: u64,
) -> FingerprintId {
    use std::hash::Hasher;
    let mut h = twox_hash::XxHash3_64::with_seed(seed);
    let mut head = [0u8; 13];
    head[0] = kind as u8;
    head[1..5].copy_from_slice(&db_id.0.to_le_bytes());
    head[5..13].copy_from_slice(&caller_fn_id.unwrap_or(0).to_le_bytes());
    h.write(&head);
    h.write(text.as_bytes());
    sprig_common::ids::Fingerprint(h.finish())
}

/// The reserved fingerprints of a kind. They are small numbers, shown as
/// `00000000000000KK` ("other") and `00000000000001KK` ("unparsed"), where
/// `KK` is the kind byte; a real digest equals one with probability
/// 2^-56.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Reserved {
    /// New fingerprints past `workload.max_fingerprints_per_tenant`.
    Other,
    /// Statements the gateway cannot lex or parse.
    Unparsed,
}

impl Reserved {
    /// The reserved id of `kind`.
    #[must_use]
    pub const fn id(self, kind: FpKind) -> FingerprintId {
        let k = kind as u64;
        sprig_common::ids::Fingerprint(match self {
            Self::Other => k,
            Self::Unparsed => 0x100 | k,
        })
    }

    /// The normalized text stored for it.
    #[must_use]
    pub const fn text(self) -> &'static str {
        match self {
            Self::Other => "<other>",
            Self::Unparsed => "<unparsed>",
        }
    }

    /// Which reserved id `id` is, if any.
    #[must_use]
    pub fn of(id: FingerprintId) -> Option<(Self, FpKind)> {
        let v = id.0;
        if v > 0x1ff {
            return None;
        }
        let kind = kind_of_byte(u8::try_from(v & 0xff).ok()?)?;
        Some((
            if v & 0x100 == 0 {
                Self::Other
            } else {
                Self::Unparsed
            },
            kind,
        ))
    }
}

/// The kind with a given kind byte.
#[must_use]
pub const fn kind_of_byte(b: u8) -> Option<FpKind> {
    Some(match b {
        1 => FpKind::Sql,
        2 => FpKind::Mongo,
        3 => FpKind::Redis,
        4 => FpKind::Kv,
        5 => FpKind::Vector,
        6 => FpKind::Search,
        7 => FpKind::Analytics,
        8 => FpKind::Api,
        9 => FpKind::Logic,
        10 => FpKind::Txn,
        _ => return None,
    })
}

#[cfg(test)]
mod tests;
