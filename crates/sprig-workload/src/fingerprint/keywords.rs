//! Keyword classification for the normalizer (chapter 12: "keyword →
//! UPPER CASE").
//!
//! One sorted table holds every word the normalizer treats specially, with
//! its flags, so each word costs one binary search:
//!
//! - **reserved**: MySQL 8.4 and MariaDB 11.8 reserved words, never
//!   identifiers unless quoted;
//! - **keyword**: written in upper case: the reserved words plus a short
//!   list of non-reserved keywords of statement syntax (`BEGIN`, `COMMIT`,
//!   `VALUE`, interval units, ...), which MySQL also accepts as names and
//!   which are therefore not keywords in table positions;
//! - the roles the walker needs (table modifiers, clause ends, reserved
//!   function names, JSON path functions) and an id for the words with
//!   their own rules.
//!
//! Quoted identifiers are never keywords.

use crate::shape::StmtType;

/// Written in upper case.
pub(crate) const KEYWORD: u16 = 1;
/// Reserved: never an unquoted name.
pub(crate) const RESERVED: u16 = 1 << 1;
/// May sit between a table keyword and the table name (`IGNORE`, `IF`,
/// `TEMPORARY`, ...).
pub(crate) const TABLE_MODIFIER: u16 = 1 << 2;
/// A non-reserved keyword that ends a table reference instead of naming
/// its alias.
pub(crate) const NOT_ALIAS: u16 = 1 << 3;
/// Ends a list of table references at its depth (`WHERE`, `SET`, ...).
pub(crate) const CLAUSE_END: u16 = 1 << 4;
/// A reserved word that is also a function name (`IF(`, `LEFT(`).
pub(crate) const RESERVED_FUNC: u16 = 1 << 5;
/// A JSON function whose second and later arguments are paths.
pub(crate) const JSON_PATH_FUNC: u16 = 1 << 6;

/// The words with their own rules in the walker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kw {
    /// No rule of its own.
    None,
    /// `FROM`.
    From,
    /// `JOIN`.
    Join,
    /// `STRAIGHT_JOIN`.
    StraightJoin,
    /// `UPDATE`.
    Update,
    /// `INTO`.
    Into,
    /// `TABLE`.
    Table,
    /// `TABLES`.
    Tables,
    /// `TO`.
    To,
    /// `RENAME`.
    Rename,
    /// `USING`.
    Using,
    /// `ON`.
    On,
    /// `AS`.
    As,
    /// `IN`.
    In,
    /// `LIKE`.
    Like,
    /// `NULL`.
    Null,
    /// `END`.
    End,
    /// `VALUES`.
    Values,
    /// `VALUE`.
    Value,
    /// `ROW`.
    Row,
    /// `WITH`.
    With,
    /// `RECURSIVE`.
    Recursive,
    /// `TRUE`.
    True,
    /// `FALSE`.
    False,
    /// `DATE`.
    Date,
    /// `TIME`.
    Time,
    /// `TIMESTAMP`.
    Timestamp,
    /// `IDENTIFIED`.
    Identified,
    /// `INSERT`.
    Insert,
    /// `REPLACE`.
    Replace,
    /// `DESCRIBE`.
    Describe,
    /// `DESC`.
    Desc,
    /// `EXPLAIN`.
    Explain,
    /// `FOR`.
    For,
    /// `KEY`.
    Key,
    /// `SELECT`.
    Select,
    /// `SET`.
    Set,
    /// `CASE`.
    Case,
    /// `INTERVAL`.
    Interval,
}

/// What a word is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WordInfo {
    /// Flag bits.
    pub(crate) flags: u16,
    /// Its own rule, if any.
    pub(crate) kw: Kw,
    /// The statement type when it starts a statement.
    pub(crate) stmt: Option<StmtType>,
}

impl WordInfo {
    /// An identifier: no flags, no rule.
    pub(crate) const NONE: Self = Self {
        flags: 0,
        kw: Kw::None,
        stmt: None,
    };

    /// Whether a flag is set.
    pub(crate) const fn has(self, flag: u16) -> bool {
        self.flags & flag != 0
    }
}

const fn w(flags: u16, kw: Kw, stmt: Option<StmtType>) -> WordInfo {
    WordInfo { flags, kw, stmt }
}

/// Sorted by word, upper case.
static WORDS: &[(&str, WordInfo)] = &[
    ("ACCESSIBLE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("ACTION", w(KEYWORD, Kw::None, None)),
    ("ADD", w(KEYWORD | RESERVED, Kw::None, None)),
    ("AFTER", w(KEYWORD, Kw::None, None)),
    ("AGAINST", w(KEYWORD, Kw::None, None)),
    ("ALGORITHM", w(KEYWORD, Kw::None, None)),
    ("ALL", w(KEYWORD | RESERVED, Kw::None, None)),
    (
        "ALTER",
        w(KEYWORD | RESERVED, Kw::None, Some(StmtType::Ddl)),
    ),
    ("ANALYZE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("AND", w(KEYWORD | RESERVED, Kw::None, None)),
    ("ANY", w(KEYWORD, Kw::None, None)),
    ("AS", w(KEYWORD | RESERVED, Kw::As, None)),
    ("ASC", w(KEYWORD | RESERVED, Kw::None, None)),
    ("ASENSITIVE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("AT", w(KEYWORD, Kw::None, None)),
    ("AUTOCOMMIT", w(KEYWORD, Kw::None, None)),
    ("BEFORE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("BEGIN", w(KEYWORD, Kw::None, Some(StmtType::Transaction))),
    ("BETWEEN", w(KEYWORD | RESERVED, Kw::None, None)),
    ("BIGINT", w(KEYWORD | RESERVED, Kw::None, None)),
    ("BINARY", w(KEYWORD | RESERVED, Kw::None, None)),
    ("BLOB", w(KEYWORD | RESERVED, Kw::None, None)),
    ("BOOLEAN", w(KEYWORD, Kw::None, None)),
    ("BOTH", w(KEYWORD | RESERVED, Kw::None, None)),
    ("BTREE", w(KEYWORD, Kw::None, None)),
    ("BY", w(KEYWORD | RESERVED, Kw::None, None)),
    (
        "CALL",
        w(KEYWORD | RESERVED, Kw::None, Some(StmtType::Call)),
    ),
    ("CASCADE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("CASCADED", w(KEYWORD, Kw::None, None)),
    ("CASE", w(KEYWORD | RESERVED, Kw::Case, None)),
    ("CHAIN", w(KEYWORD, Kw::None, None)),
    ("CHANGE", w(KEYWORD | RESERVED, Kw::None, None)),
    (
        "CHAR",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    ("CHARACTER", w(KEYWORD | RESERVED, Kw::None, None)),
    ("CHARSET", w(KEYWORD, Kw::None, None)),
    ("CHECK", w(KEYWORD | RESERVED, Kw::None, None)),
    ("CHECKSUM", w(KEYWORD, Kw::None, None)),
    ("CLOSE", w(KEYWORD | NOT_ALIAS, Kw::None, None)),
    ("COLLATE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("COLLATION", w(KEYWORD, Kw::None, None)),
    ("COLUMN", w(KEYWORD | RESERVED, Kw::None, None)),
    ("COLUMNS", w(KEYWORD, Kw::None, None)),
    ("COMMIT", w(KEYWORD, Kw::None, Some(StmtType::Transaction))),
    ("COMMITTED", w(KEYWORD, Kw::None, None)),
    ("CONDITION", w(KEYWORD | RESERVED, Kw::None, None)),
    ("CONSISTENT", w(KEYWORD, Kw::None, None)),
    ("CONSTRAINT", w(KEYWORD | RESERVED, Kw::None, None)),
    ("CONTINUE", w(KEYWORD | RESERVED, Kw::None, None)),
    (
        "CONVERT",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    (
        "CREATE",
        w(KEYWORD | RESERVED, Kw::None, Some(StmtType::Ddl)),
    ),
    ("CROSS", w(KEYWORD | RESERVED, Kw::None, None)),
    ("CUBE", w(KEYWORD | RESERVED, Kw::None, None)),
    (
        "CUME_DIST",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    (
        "CURRENT_DATE",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    ("CURRENT_ROLE", w(KEYWORD | RESERVED, Kw::None, None)),
    (
        "CURRENT_TIME",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    (
        "CURRENT_TIMESTAMP",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    (
        "CURRENT_USER",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    ("CURSOR", w(KEYWORD | RESERVED, Kw::None, None)),
    (
        "DATABASE",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    ("DATABASES", w(KEYWORD | RESERVED, Kw::None, None)),
    ("DATE", w(0, Kw::Date, None)),
    ("DAY", w(KEYWORD, Kw::None, None)),
    ("DAY_HOUR", w(KEYWORD | RESERVED, Kw::None, None)),
    ("DAY_MICROSECOND", w(KEYWORD | RESERVED, Kw::None, None)),
    ("DAY_MINUTE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("DAY_SECOND", w(KEYWORD | RESERVED, Kw::None, None)),
    ("DEALLOCATE", w(KEYWORD, Kw::None, None)),
    ("DEC", w(KEYWORD | RESERVED, Kw::None, None)),
    ("DECIMAL", w(KEYWORD | RESERVED, Kw::None, None)),
    ("DECLARE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("DEFAULT", w(KEYWORD | RESERVED, Kw::None, None)),
    ("DEFINER", w(KEYWORD, Kw::None, None)),
    (
        "DELAYED",
        w(KEYWORD | RESERVED | TABLE_MODIFIER, Kw::None, None),
    ),
    (
        "DELETE",
        w(KEYWORD | RESERVED, Kw::None, Some(StmtType::Delete)),
    ),
    ("DELETE_DOMAIN_ID", w(KEYWORD | RESERVED, Kw::None, None)),
    (
        "DENSE_RANK",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    (
        "DESC",
        w(KEYWORD | RESERVED, Kw::Desc, Some(StmtType::Show)),
    ),
    (
        "DESCRIBE",
        w(KEYWORD | RESERVED, Kw::Describe, Some(StmtType::Show)),
    ),
    ("DETERMINISTIC", w(KEYWORD | RESERVED, Kw::None, None)),
    ("DISABLE", w(KEYWORD, Kw::None, None)),
    ("DISTINCT", w(KEYWORD | RESERVED, Kw::None, None)),
    ("DISTINCTROW", w(KEYWORD | RESERVED, Kw::None, None)),
    ("DIV", w(KEYWORD | RESERVED, Kw::None, None)),
    ("DO", w(KEYWORD | NOT_ALIAS, Kw::None, None)),
    ("DOUBLE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("DO_DOMAIN_IDS", w(KEYWORD | RESERVED, Kw::None, None)),
    ("DROP", w(KEYWORD | RESERVED, Kw::None, Some(StmtType::Ddl))),
    ("DUAL", w(KEYWORD | RESERVED, Kw::None, None)),
    ("DUPLICATE", w(KEYWORD | CLAUSE_END, Kw::None, None)),
    ("EACH", w(KEYWORD | RESERVED, Kw::None, None)),
    ("ELSE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("ELSEIF", w(KEYWORD | RESERVED, Kw::None, None)),
    ("EMPTY", w(KEYWORD | RESERVED, Kw::None, None)),
    ("ENABLE", w(KEYWORD, Kw::None, None)),
    ("ENCLOSED", w(KEYWORD | RESERVED, Kw::None, None)),
    ("END", w(KEYWORD, Kw::End, None)),
    ("ENGINE", w(KEYWORD, Kw::None, None)),
    ("ENGINES", w(KEYWORD, Kw::None, None)),
    ("ESCAPE", w(KEYWORD, Kw::None, None)),
    ("ESCAPED", w(KEYWORD | RESERVED, Kw::None, None)),
    ("EVENTS", w(KEYWORD, Kw::None, None)),
    ("EXCEPT", w(KEYWORD | RESERVED | CLAUSE_END, Kw::None, None)),
    ("EXECUTE", w(KEYWORD, Kw::None, None)),
    (
        "EXISTS",
        w(KEYWORD | RESERVED | TABLE_MODIFIER, Kw::None, None),
    ),
    ("EXIT", w(KEYWORD | RESERVED, Kw::None, None)),
    (
        "EXPLAIN",
        w(KEYWORD | RESERVED, Kw::Explain, Some(StmtType::Show)),
    ),
    ("EXTENDED", w(KEYWORD, Kw::None, None)),
    ("FALSE", w(KEYWORD | RESERVED, Kw::False, None)),
    ("FETCH", w(KEYWORD | RESERVED, Kw::None, None)),
    ("FIELDS", w(KEYWORD, Kw::None, None)),
    ("FIRST", w(KEYWORD, Kw::None, None)),
    (
        "FIRST_VALUE",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    ("FLOAT", w(KEYWORD | RESERVED, Kw::None, None)),
    ("FLOAT4", w(KEYWORD | RESERVED, Kw::None, None)),
    ("FLOAT8", w(KEYWORD | RESERVED, Kw::None, None)),
    ("FLUSH", w(KEYWORD, Kw::None, None)),
    ("FOLLOWING", w(KEYWORD, Kw::None, None)),
    ("FOR", w(KEYWORD | RESERVED | CLAUSE_END, Kw::For, None)),
    ("FORCE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("FOREIGN", w(KEYWORD | RESERVED, Kw::None, None)),
    ("FROM", w(KEYWORD | RESERVED, Kw::From, None)),
    ("FULL", w(KEYWORD, Kw::None, None)),
    ("FULLTEXT", w(KEYWORD | RESERVED, Kw::None, None)),
    ("FUNCTION", w(KEYWORD | RESERVED, Kw::None, None)),
    ("GENERAL", w(KEYWORD | RESERVED, Kw::None, None)),
    ("GENERATED", w(KEYWORD | RESERVED, Kw::None, None)),
    ("GET", w(KEYWORD | RESERVED, Kw::None, None)),
    ("GLOBAL", w(KEYWORD, Kw::None, None)),
    ("GRANT", w(KEYWORD | RESERVED, Kw::None, None)),
    ("GRANTS", w(KEYWORD, Kw::None, None)),
    ("GROUP", w(KEYWORD | RESERVED | CLAUSE_END, Kw::None, None)),
    (
        "GROUPING",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    ("GROUPS", w(KEYWORD | RESERVED, Kw::None, None)),
    ("HANDLER", w(KEYWORD, Kw::None, None)),
    ("HAVING", w(KEYWORD | RESERVED | CLAUSE_END, Kw::None, None)),
    ("HELP", w(KEYWORD, Kw::None, None)),
    (
        "HIGH_PRIORITY",
        w(KEYWORD | RESERVED | TABLE_MODIFIER, Kw::None, None),
    ),
    ("HOUR", w(KEYWORD, Kw::None, None)),
    ("HOUR_MICROSECOND", w(KEYWORD | RESERVED, Kw::None, None)),
    ("HOUR_MINUTE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("HOUR_SECOND", w(KEYWORD | RESERVED, Kw::None, None)),
    ("IDENTIFIED", w(KEYWORD, Kw::Identified, None)),
    (
        "IF",
        w(
            KEYWORD | RESERVED | TABLE_MODIFIER | RESERVED_FUNC,
            Kw::None,
            None,
        ),
    ),
    (
        "IGNORE",
        w(KEYWORD | RESERVED | TABLE_MODIFIER, Kw::None, None),
    ),
    ("IGNORE_DOMAIN_IDS", w(KEYWORD | RESERVED, Kw::None, None)),
    ("IGNORE_SERVER_IDS", w(KEYWORD | RESERVED, Kw::None, None)),
    ("IN", w(KEYWORD | RESERVED, Kw::In, None)),
    ("INDEX", w(KEYWORD | RESERVED, Kw::None, None)),
    ("INDEXES", w(KEYWORD, Kw::None, None)),
    ("INFILE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("INNER", w(KEYWORD | RESERVED, Kw::None, None)),
    ("INOUT", w(KEYWORD | RESERVED, Kw::None, None)),
    ("INSENSITIVE", w(KEYWORD | RESERVED, Kw::None, None)),
    (
        "INSERT",
        w(
            KEYWORD | RESERVED | RESERVED_FUNC,
            Kw::Insert,
            Some(StmtType::Insert),
        ),
    ),
    ("INSTANT", w(KEYWORD, Kw::None, None)),
    ("INT", w(KEYWORD | RESERVED, Kw::None, None)),
    ("INT1", w(KEYWORD | RESERVED, Kw::None, None)),
    ("INT2", w(KEYWORD | RESERVED, Kw::None, None)),
    ("INT3", w(KEYWORD | RESERVED, Kw::None, None)),
    ("INT4", w(KEYWORD | RESERVED, Kw::None, None)),
    ("INT8", w(KEYWORD | RESERVED, Kw::None, None)),
    ("INTEGER", w(KEYWORD | RESERVED, Kw::None, None)),
    (
        "INTERSECT",
        w(KEYWORD | RESERVED | CLAUSE_END, Kw::None, None),
    ),
    (
        "INTERVAL",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::Interval, None),
    ),
    (
        "INTO",
        w(KEYWORD | RESERVED | TABLE_MODIFIER, Kw::Into, None),
    ),
    ("INVOKER", w(KEYWORD, Kw::None, None)),
    ("IO_AFTER_GTIDS", w(KEYWORD | RESERVED, Kw::None, None)),
    ("IO_BEFORE_GTIDS", w(KEYWORD | RESERVED, Kw::None, None)),
    ("IS", w(KEYWORD | RESERVED, Kw::None, None)),
    ("ISOLATION", w(KEYWORD, Kw::None, None)),
    ("ITERATE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("JOIN", w(KEYWORD | RESERVED, Kw::Join, None)),
    ("JSON_CONTAINS_PATH", w(JSON_PATH_FUNC, Kw::None, None)),
    ("JSON_EXISTS", w(JSON_PATH_FUNC, Kw::None, None)),
    ("JSON_EXTRACT", w(JSON_PATH_FUNC, Kw::None, None)),
    ("JSON_KEYS", w(JSON_PATH_FUNC, Kw::None, None)),
    ("JSON_LENGTH", w(JSON_PATH_FUNC, Kw::None, None)),
    ("JSON_QUERY", w(JSON_PATH_FUNC, Kw::None, None)),
    (
        "JSON_TABLE",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    ("JSON_VALUE", w(JSON_PATH_FUNC, Kw::None, None)),
    ("KEY", w(KEYWORD | RESERVED, Kw::Key, None)),
    ("KEYS", w(KEYWORD | RESERVED, Kw::None, None)),
    ("KILL", w(KEYWORD | RESERVED, Kw::None, None)),
    ("LAG", w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None)),
    ("LAST", w(KEYWORD, Kw::None, None)),
    (
        "LAST_VALUE",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    (
        "LATERAL",
        w(KEYWORD | RESERVED | TABLE_MODIFIER, Kw::None, None),
    ),
    (
        "LEAD",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    ("LEADING", w(KEYWORD | RESERVED, Kw::None, None)),
    ("LEAVE", w(KEYWORD | RESERVED, Kw::None, None)),
    (
        "LEFT",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    ("LESS", w(KEYWORD, Kw::None, None)),
    ("LEVEL", w(KEYWORD, Kw::None, None)),
    ("LIKE", w(KEYWORD | RESERVED, Kw::Like, None)),
    ("LIMIT", w(KEYWORD | RESERVED | CLAUSE_END, Kw::None, None)),
    ("LINEAR", w(KEYWORD | RESERVED, Kw::None, None)),
    ("LINES", w(KEYWORD | RESERVED, Kw::None, None)),
    ("LOAD", w(KEYWORD | RESERVED, Kw::None, None)),
    ("LOCAL", w(KEYWORD | NOT_ALIAS, Kw::None, None)),
    (
        "LOCALTIME",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    (
        "LOCALTIMESTAMP",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    ("LOCK", w(KEYWORD | RESERVED, Kw::None, None)),
    ("LOCKED", w(KEYWORD, Kw::None, None)),
    ("LONG", w(KEYWORD | RESERVED, Kw::None, None)),
    ("LONGBLOB", w(KEYWORD | RESERVED, Kw::None, None)),
    ("LONGTEXT", w(KEYWORD | RESERVED, Kw::None, None)),
    ("LOOP", w(KEYWORD | RESERVED, Kw::None, None)),
    (
        "LOW_PRIORITY",
        w(KEYWORD | RESERVED | TABLE_MODIFIER, Kw::None, None),
    ),
    ("MASTER_BIND", w(KEYWORD | RESERVED, Kw::None, None)),
    (
        "MASTER_HEARTBEAT_PERIOD",
        w(KEYWORD | RESERVED, Kw::None, None),
    ),
    (
        "MASTER_SSL_VERIFY_SERVER_CERT",
        w(KEYWORD | RESERVED, Kw::None, None),
    ),
    (
        "MATCH",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    ("MAXVALUE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("MEDIUMBLOB", w(KEYWORD | RESERVED, Kw::None, None)),
    ("MEDIUMINT", w(KEYWORD | RESERVED, Kw::None, None)),
    ("MEDIUMTEXT", w(KEYWORD | RESERVED, Kw::None, None)),
    ("MEMBER", w(KEYWORD, Kw::None, None)),
    ("MICROSECOND", w(KEYWORD, Kw::None, None)),
    ("MIDDLEINT", w(KEYWORD | RESERVED, Kw::None, None)),
    ("MINUTE", w(KEYWORD, Kw::None, None)),
    ("MINUTE_MICROSECOND", w(KEYWORD | RESERVED, Kw::None, None)),
    ("MINUTE_SECOND", w(KEYWORD | RESERVED, Kw::None, None)),
    ("MOD", w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None)),
    ("MODE", w(KEYWORD, Kw::None, None)),
    ("MODIFIES", w(KEYWORD | RESERVED, Kw::None, None)),
    ("MONTH", w(KEYWORD, Kw::None, None)),
    ("NAMES", w(KEYWORD, Kw::None, None)),
    ("NATURAL", w(KEYWORD | RESERVED, Kw::None, None)),
    ("NEXT", w(KEYWORD, Kw::None, None)),
    ("NO", w(KEYWORD, Kw::None, None)),
    (
        "NOT",
        w(KEYWORD | RESERVED | TABLE_MODIFIER, Kw::None, None),
    ),
    ("NOWAIT", w(KEYWORD | NOT_ALIAS, Kw::None, None)),
    ("NO_WRITE_TO_BINLOG", w(KEYWORD | RESERVED, Kw::None, None)),
    (
        "NTH_VALUE",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    (
        "NTILE",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    ("NULL", w(KEYWORD | RESERVED, Kw::Null, None)),
    ("NULLS", w(KEYWORD, Kw::None, None)),
    ("NUMERIC", w(KEYWORD | RESERVED, Kw::None, None)),
    ("OF", w(KEYWORD | RESERVED, Kw::None, None)),
    ("OFFSET", w(KEYWORD | RESERVED, Kw::None, None)),
    ("OJ", w(KEYWORD, Kw::None, None)),
    ("ON", w(KEYWORD | RESERVED, Kw::On, None)),
    ("ONLINE", w(KEYWORD, Kw::None, None)),
    ("ONLY", w(KEYWORD | TABLE_MODIFIER, Kw::None, None)),
    ("OPEN", w(KEYWORD | NOT_ALIAS, Kw::None, None)),
    ("OPTIMIZE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("OPTIMIZER_COSTS", w(KEYWORD | RESERVED, Kw::None, None)),
    ("OPTION", w(KEYWORD | RESERVED, Kw::None, None)),
    ("OPTIONALLY", w(KEYWORD | RESERVED, Kw::None, None)),
    ("OR", w(KEYWORD | RESERVED, Kw::None, None)),
    ("ORDER", w(KEYWORD | RESERVED | CLAUSE_END, Kw::None, None)),
    ("OUT", w(KEYWORD | RESERVED, Kw::None, None)),
    ("OUTER", w(KEYWORD | RESERVED, Kw::None, None)),
    ("OUTFILE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("OVER", w(KEYWORD | RESERVED, Kw::None, None)),
    ("PAGE_CHECKSUM", w(KEYWORD | RESERVED, Kw::None, None)),
    ("PARSE_VCOL_EXPR", w(KEYWORD | RESERVED, Kw::None, None)),
    ("PARTITION", w(KEYWORD | RESERVED, Kw::None, None)),
    (
        "PERCENT_RANK",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    (
        "POSITION",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    ("PRECEDING", w(KEYWORD, Kw::None, None)),
    ("PRECISION", w(KEYWORD | RESERVED, Kw::None, None)),
    ("PREPARE", w(KEYWORD, Kw::None, None)),
    ("PRIMARY", w(KEYWORD | RESERVED, Kw::None, None)),
    ("PRIVILEGES", w(KEYWORD, Kw::None, None)),
    (
        "PROCEDURE",
        w(KEYWORD | RESERVED | CLAUSE_END, Kw::None, None),
    ),
    ("PROCESSLIST", w(KEYWORD, Kw::None, None)),
    ("PURGE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("QUARTER", w(KEYWORD, Kw::None, None)),
    ("QUICK", w(KEYWORD | TABLE_MODIFIER, Kw::None, None)),
    ("RANGE", w(KEYWORD | RESERVED, Kw::None, None)),
    (
        "RANK",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    ("READ", w(KEYWORD | RESERVED, Kw::None, None)),
    ("READS", w(KEYWORD | RESERVED, Kw::None, None)),
    ("READ_ONLY", w(KEYWORD, Kw::None, None)),
    ("READ_WRITE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("REAL", w(KEYWORD | RESERVED, Kw::None, None)),
    ("RECOVER", w(KEYWORD, Kw::None, None)),
    ("RECURSIVE", w(KEYWORD | RESERVED, Kw::Recursive, None)),
    ("REFERENCES", w(KEYWORD | RESERVED, Kw::None, None)),
    ("REF_SYSTEM_ID", w(KEYWORD | RESERVED, Kw::None, None)),
    ("REGEXP", w(KEYWORD | RESERVED, Kw::None, None)),
    (
        "RELEASE",
        w(KEYWORD | RESERVED, Kw::None, Some(StmtType::Transaction)),
    ),
    (
        "RENAME",
        w(KEYWORD | RESERVED, Kw::Rename, Some(StmtType::Ddl)),
    ),
    ("REPAIR", w(KEYWORD, Kw::None, None)),
    (
        "REPEAT",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    ("REPEATABLE", w(KEYWORD, Kw::None, None)),
    (
        "REPLACE",
        w(
            KEYWORD | RESERVED | RESERVED_FUNC,
            Kw::Replace,
            Some(StmtType::Replace),
        ),
    ),
    ("REQUIRE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("RESET", w(KEYWORD, Kw::None, None)),
    ("RESIGNAL", w(KEYWORD | RESERVED, Kw::None, None)),
    ("RESTRICT", w(KEYWORD | RESERVED, Kw::None, None)),
    ("RETURN", w(KEYWORD | RESERVED, Kw::None, None)),
    (
        "RETURNING",
        w(KEYWORD | RESERVED | CLAUSE_END, Kw::None, None),
    ),
    ("REVOKE", w(KEYWORD | RESERVED, Kw::None, None)),
    (
        "RIGHT",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    ("RLIKE", w(KEYWORD | RESERVED, Kw::None, None)),
    (
        "ROLLBACK",
        w(KEYWORD, Kw::None, Some(StmtType::Transaction)),
    ),
    ("ROLLUP", w(KEYWORD, Kw::None, None)),
    ("ROW", w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::Row, None)),
    ("ROWS", w(KEYWORD | RESERVED, Kw::None, None)),
    (
        "ROW_NUMBER",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    (
        "SAVEPOINT",
        w(KEYWORD, Kw::None, Some(StmtType::Transaction)),
    ),
    (
        "SCHEMA",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    ("SCHEMAS", w(KEYWORD | RESERVED, Kw::None, None)),
    ("SECOND", w(KEYWORD, Kw::None, None)),
    ("SECOND_MICROSECOND", w(KEYWORD | RESERVED, Kw::None, None)),
    ("SECURITY", w(KEYWORD, Kw::None, None)),
    (
        "SELECT",
        w(
            KEYWORD | RESERVED | CLAUSE_END,
            Kw::Select,
            Some(StmtType::Select),
        ),
    ),
    ("SENSITIVE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("SEPARATOR", w(KEYWORD | RESERVED, Kw::None, None)),
    ("SERIALIZABLE", w(KEYWORD, Kw::None, None)),
    ("SESSION", w(KEYWORD, Kw::None, None)),
    (
        "SET",
        w(
            KEYWORD | RESERVED | CLAUSE_END,
            Kw::Set,
            Some(StmtType::Set),
        ),
    ),
    ("SHARE", w(KEYWORD, Kw::None, None)),
    (
        "SHOW",
        w(KEYWORD | RESERVED, Kw::None, Some(StmtType::Show)),
    ),
    ("SIGNAL", w(KEYWORD | RESERVED, Kw::None, None)),
    ("SIGNED", w(KEYWORD, Kw::None, None)),
    ("SKIP", w(KEYWORD | NOT_ALIAS, Kw::None, None)),
    ("SLOW", w(KEYWORD | RESERVED, Kw::None, None)),
    ("SMALLINT", w(KEYWORD | RESERVED, Kw::None, None)),
    ("SNAPSHOT", w(KEYWORD, Kw::None, None)),
    ("SOUNDS", w(KEYWORD, Kw::None, None)),
    ("SPATIAL", w(KEYWORD | RESERVED, Kw::None, None)),
    ("SPECIFIC", w(KEYWORD | RESERVED, Kw::None, None)),
    ("SQL", w(KEYWORD | RESERVED, Kw::None, None)),
    ("SQLEXCEPTION", w(KEYWORD | RESERVED, Kw::None, None)),
    ("SQLSTATE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("SQLWARNING", w(KEYWORD | RESERVED, Kw::None, None)),
    ("SQL_BIG_RESULT", w(KEYWORD | RESERVED, Kw::None, None)),
    ("SQL_BUFFER_RESULT", w(KEYWORD, Kw::None, None)),
    ("SQL_CACHE", w(KEYWORD, Kw::None, None)),
    ("SQL_CALC_FOUND_ROWS", w(KEYWORD | RESERVED, Kw::None, None)),
    ("SQL_NO_CACHE", w(KEYWORD, Kw::None, None)),
    ("SQL_SMALL_RESULT", w(KEYWORD | RESERVED, Kw::None, None)),
    ("SSL", w(KEYWORD | RESERVED, Kw::None, None)),
    ("START", w(KEYWORD, Kw::None, Some(StmtType::Transaction))),
    ("STARTING", w(KEYWORD | RESERVED, Kw::None, None)),
    ("STATS_AUTO_RECALC", w(KEYWORD | RESERVED, Kw::None, None)),
    ("STATS_PERSISTENT", w(KEYWORD | RESERVED, Kw::None, None)),
    ("STATS_SAMPLE_PAGES", w(KEYWORD | RESERVED, Kw::None, None)),
    ("STORED", w(KEYWORD | RESERVED, Kw::None, None)),
    (
        "STRAIGHT_JOIN",
        w(KEYWORD | RESERVED, Kw::StraightJoin, None),
    ),
    ("SYSTEM", w(KEYWORD | RESERVED, Kw::None, None)),
    ("SYSTEM_TIME", w(KEYWORD, Kw::None, None)),
    (
        "TABLE",
        w(
            KEYWORD | RESERVED | TABLE_MODIFIER,
            Kw::Table,
            Some(StmtType::Select),
        ),
    ),
    ("TABLES", w(KEYWORD, Kw::Tables, None)),
    ("TABLESAMPLE", w(KEYWORD | NOT_ALIAS, Kw::None, None)),
    ("TEMPORARY", w(KEYWORD | TABLE_MODIFIER, Kw::None, None)),
    ("TERMINATED", w(KEYWORD | RESERVED, Kw::None, None)),
    ("THEN", w(KEYWORD | RESERVED, Kw::None, None)),
    ("TIME", w(0, Kw::Time, None)),
    ("TIMESTAMP", w(0, Kw::Timestamp, None)),
    ("TINYBLOB", w(KEYWORD | RESERVED, Kw::None, None)),
    ("TINYINT", w(KEYWORD | RESERVED, Kw::None, None)),
    ("TINYTEXT", w(KEYWORD | RESERVED, Kw::None, None)),
    ("TO", w(KEYWORD | RESERVED, Kw::To, None)),
    ("TRAILING", w(KEYWORD | RESERVED, Kw::None, None)),
    ("TRANSACTION", w(KEYWORD, Kw::None, None)),
    ("TRIGGER", w(KEYWORD | RESERVED, Kw::None, None)),
    ("TRIGGERS", w(KEYWORD, Kw::None, None)),
    ("TRUE", w(KEYWORD | RESERVED, Kw::True, None)),
    ("TRUNCATE", w(KEYWORD, Kw::None, Some(StmtType::Ddl))),
    ("UNBOUNDED", w(KEYWORD, Kw::None, None)),
    ("UNCOMMITTED", w(KEYWORD, Kw::None, None)),
    ("UNDO", w(KEYWORD | RESERVED, Kw::None, None)),
    ("UNION", w(KEYWORD | RESERVED | CLAUSE_END, Kw::None, None)),
    ("UNIQUE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("UNKNOWN", w(KEYWORD, Kw::None, None)),
    ("UNLOCK", w(KEYWORD | RESERVED, Kw::None, None)),
    ("UNSIGNED", w(KEYWORD | RESERVED, Kw::None, None)),
    (
        "UPDATE",
        w(KEYWORD | RESERVED, Kw::Update, Some(StmtType::Update)),
    ),
    ("USAGE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("USE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("USING", w(KEYWORD | RESERVED, Kw::Using, None)),
    (
        "UTC_DATE",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    (
        "UTC_TIME",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    (
        "UTC_TIMESTAMP",
        w(KEYWORD | RESERVED | RESERVED_FUNC, Kw::None, None),
    ),
    (
        "VALUE",
        w(KEYWORD | NOT_ALIAS | CLAUSE_END, Kw::Value, None),
    ),
    (
        "VALUES",
        w(
            KEYWORD | RESERVED | CLAUSE_END,
            Kw::Values,
            Some(StmtType::Select),
        ),
    ),
    ("VARBINARY", w(KEYWORD | RESERVED, Kw::None, None)),
    ("VARCHAR", w(KEYWORD | RESERVED, Kw::None, None)),
    ("VARCHARACTER", w(KEYWORD | RESERVED, Kw::None, None)),
    ("VARIABLES", w(KEYWORD, Kw::None, None)),
    ("VARYING", w(KEYWORD | RESERVED, Kw::None, None)),
    ("VIEW", w(KEYWORD, Kw::None, None)),
    ("VIRTUAL", w(KEYWORD | RESERVED, Kw::None, None)),
    ("WAIT", w(KEYWORD | NOT_ALIAS, Kw::None, None)),
    ("WARNINGS", w(KEYWORD, Kw::None, None)),
    ("WEEK", w(KEYWORD, Kw::None, None)),
    ("WHEN", w(KEYWORD | RESERVED, Kw::None, None)),
    ("WHERE", w(KEYWORD | RESERVED | CLAUSE_END, Kw::None, None)),
    ("WHILE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("WINDOW", w(KEYWORD | RESERVED | CLAUSE_END, Kw::None, None)),
    (
        "WITH",
        w(KEYWORD | RESERVED, Kw::With, Some(StmtType::Select)),
    ),
    ("WORK", w(KEYWORD, Kw::None, None)),
    ("WRITE", w(KEYWORD | RESERVED, Kw::None, None)),
    ("XA", w(KEYWORD, Kw::None, Some(StmtType::Transaction))),
    ("XOR", w(KEYWORD | RESERVED, Kw::None, None)),
    ("YEAR", w(KEYWORD, Kw::None, None)),
    ("YEAR_MONTH", w(KEYWORD | RESERVED, Kw::None, None)),
    ("ZEROFILL", w(KEYWORD | RESERVED, Kw::None, None)),
];

/// Longest word in the table, in bytes.
const MAX_LEN: usize = 32;

/// Slots of the lookup table (a power of two, at least twice the words).
const SLOTS: usize = 2048;

/// FNV-1a over upper-case bytes.
fn hash(up: &[u8]) -> usize {
    let mut h: u32 = 0x811c_9dc5;
    for &b in up {
        h ^= u32::from(b);
        h = h.wrapping_mul(0x0100_0193);
    }
    h as usize & (SLOTS - 1)
}

/// Open-addressing table: index + 1 into [`WORDS`], 0 for empty.
fn table() -> &'static [u16; SLOTS] {
    static TABLE: std::sync::OnceLock<Box<[u16; SLOTS]>> = std::sync::OnceLock::new();
    TABLE.get_or_init(|| {
        let mut t = Box::new([0u16; SLOTS]);
        for (i, (k, _)) in WORDS.iter().enumerate() {
            let mut h = hash(k.as_bytes());
            while t[h] != 0 {
                h = (h + 1) & (SLOTS - 1);
            }
            t[h] = u16::try_from(i + 1).unwrap_or(0);
        }
        t
    })
}

/// Classifies a word (any case). Quoted identifiers must not be passed.
pub(crate) fn classify(word: &[u8]) -> WordInfo {
    if word.len() > MAX_LEN || word.len() < 2 || !word[0].is_ascii_alphabetic() {
        return WordInfo::NONE;
    }
    let mut buf = [0u8; MAX_LEN];
    for (d, s) in buf.iter_mut().zip(word) {
        *d = s.to_ascii_uppercase();
    }
    let up = &buf[..word.len()];
    let t = table();
    let mut h = hash(up);
    loop {
        let e = t[h];
        if e == 0 {
            return WordInfo::NONE;
        }
        let (k, info) = WORDS[usize::from(e) - 1];
        if k.as_bytes() == up {
            return info;
        }
        h = (h + 1) & (SLOTS - 1);
    }
}

/// Whether `word` equals the upper-case keyword `kw`, ignoring case.
pub(crate) fn eq_kw(word: &[u8], kw: &str) -> bool {
    word.eq_ignore_ascii_case(kw.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sorted_and_short() {
        assert!(WORDS.windows(2).all(|p| p[0].0 < p[1].0));
        for (k, info) in WORDS {
            assert_eq!(classify(k.as_bytes()), *info, "{k}");
            assert_eq!(classify(k.to_lowercase().as_bytes()), *info, "{k}");
        }
        assert!(WORDS.iter().all(|(k, _)| k.len() <= MAX_LEN));
        assert!(classify(b"select").has(KEYWORD) && classify(b"Commit").has(KEYWORD));
        assert!(!classify(b"status").has(KEYWORD));
        assert!(classify(b"offset").has(RESERVED) && !classify(b"value").has(RESERVED));
        assert_eq!(classify(b"from").kw, Kw::From);
        assert_eq!(classify(b"Update").stmt, Some(StmtType::Update));
    }
}
