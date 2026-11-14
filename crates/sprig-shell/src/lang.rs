//! Languages: splitting input into statements, classifying them, and
//! finding destructive ones.
//!
//! The SQL lexer here also drives highlighting, completion and redaction,
//! so comments, strings and quoted names are never mistaken for keywords.

use crate::error::{Result, ShellCode, ShellError};

/// An input language.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Lang {
    /// SQL (MySQL 8.4 and MariaDB dialects, plus SprigDB's statements).
    #[default]
    Sql,
    /// MongoDB shell call chains.
    Mongo,
    /// Redis commands, one per line.
    Redis,
    /// Cypher and GQL.
    Cypher,
}

impl Lang {
    /// Parses `sql`, `mongo`, `redis` or `cypher`.
    ///
    /// # Errors
    /// 39983 for another name.
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "sql" | "mysql" => Ok(Lang::Sql),
            "mongo" | "mongodb" | "mongosh" => Ok(Lang::Mongo),
            "redis" => Ok(Lang::Redis),
            "cypher" | "gql" => Ok(Lang::Cypher),
            _ => Err(ShellError::shell(
                ShellCode::LanguageUnsupported,
                format!("language {s:?}: use sql, mongo, redis or cypher"),
            )),
        }
    }

    /// The language's name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Lang::Sql => "sql",
            Lang::Mongo => "mongo",
            Lang::Redis => "redis",
            Lang::Cypher => "cypher",
        }
    }
}

/// What a statement does (Studio's `Sql.Check` classes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// Reads only.
    Read,
    /// Changes rows, documents, keys or messages.
    Write,
    /// Changes definitions.
    Ddl,
    /// Users, roles, grants, passwords.
    Account,
    /// Begin, commit, rollback.
    Transaction,
    /// Anything else (`USE`, `SET`, `KILL`).
    Other,
}

/// A token's kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tok {
    /// A word: keyword or bare name.
    Word,
    /// A back-quoted name.
    Quoted,
    /// A string literal.
    Str,
    /// A number.
    Number,
    /// A comment.
    Comment,
    /// Spaces and newlines.
    Space,
    /// Punctuation and operators.
    Op,
}

/// A token: its kind and byte range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Token {
    /// The kind.
    pub kind: Tok,
    /// Start byte.
    pub start: usize,
    /// End byte (exclusive).
    pub end: usize,
}

impl Token {
    /// The token's text in `src`.
    #[must_use]
    pub fn text<'a>(&self, src: &'a str) -> &'a str {
        &src[self.start..self.end]
    }
}

/// Lexes SQL (or Cypher, with `cypher` comments) into tokens. An open
/// string or comment runs to the end and is reported by [`open_at_end`].
#[must_use]
pub fn lex(src: &str, cypher: bool) -> Vec<Token> {
    let b = src.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let start = i;
        let c = b[i];
        let kind = if c.is_ascii_whitespace() {
            while i < b.len() && b[i].is_ascii_whitespace() {
                i += 1;
            }
            Tok::Space
        } else if (c == b'-'
            && b.get(i + 1) == Some(&b'-')
            && (cypher || b.get(i + 2).is_none_or(u8::is_ascii_whitespace)))
            || (c == b'#' && !cypher)
            || (cypher && c == b'/' && b.get(i + 1) == Some(&b'/'))
        {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            Tok::Comment
        } else if c == b'/' && b.get(i + 1) == Some(&b'*') {
            i += 2;
            while i < b.len() && !(b[i] == b'*' && b.get(i + 1) == Some(&b'/')) {
                i += 1;
            }
            i = (i + 2).min(b.len());
            Tok::Comment
        } else if c == b'\'' || c == b'"' || c == b'`' {
            i += 1;
            while i < b.len() {
                if b[i] == b'\\' && c != b'`' {
                    i += 2;
                    continue;
                }
                if b[i] == c {
                    if b.get(i + 1) == Some(&c) {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            i = i.min(b.len());
            if c == b'`' { Tok::Quoted } else { Tok::Str }
        } else if c.is_ascii_digit() {
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'.') {
                i += 1;
            }
            Tok::Number
        } else if c.is_ascii_alphabetic() || c == b'_' || c == b'$' || c == b'@' || c >= 0x80 {
            while i < b.len()
                && (b[i].is_ascii_alphanumeric()
                    || matches!(b[i], b'_' | b'$' | b'@')
                    || b[i] >= 0x80)
            {
                i += 1;
            }
            Tok::Word
        } else {
            i += 1;
            while i < b.len() && !src.is_char_boundary(i) {
                i += 1;
            }
            Tok::Op
        };
        out.push(Token {
            kind,
            start,
            end: i,
        });
    }
    out
}

/// Whether the text ends inside a string, quoted name or block comment.
#[must_use]
pub fn open_at_end(src: &str, cypher: bool) -> bool {
    let Some(t) = lex(src, cypher).into_iter().rfind(|t| t.kind != Tok::Space) else {
        return false;
    };
    let s = t.text(src);
    match t.kind {
        Tok::Str | Tok::Quoted => {
            let q = s.as_bytes()[0];
            if s.len() < 2 || s.as_bytes()[s.len() - 1] != q {
                return true;
            }
            // An escaped final quote: count the backslashes before it.
            let body = &s.as_bytes()[1..s.len() - 1];
            let slashes = body.iter().rev().take_while(|&&c| c == b'\\').count();
            q != b'`' && slashes % 2 == 1
        }
        Tok::Comment => s.starts_with("/*") && (s.len() < 4 || !s.ends_with("*/")),
        _ => false,
    }
}

/// A statement split from input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statement {
    /// The text, trimmed, without its delimiter.
    pub text: String,
    /// The 1-based line it starts on.
    pub line: usize,
}

fn line_of(src: &str, byte: usize) -> usize {
    src[..byte].bytes().filter(|&c| c == b'\n').count() + 1
}

fn only_comments(s: &str, cypher: bool) -> bool {
    lex(s, cypher)
        .iter()
        .all(|t| matches!(t.kind, Tok::Space | Tok::Comment))
}

/// Splits SQL on `;` outside strings and comments, honouring `mysql`'s
/// `DELIMITER` lines.
///
/// # Errors
/// 39985 for an open string, quoted name or comment at the end.
pub fn split_sql(src: &str) -> Result<Vec<Statement>> {
    split_with(src, false)
}

/// Splits Cypher on `;` outside strings and comments.
///
/// # Errors
/// 39985 as [`split_sql`].
pub fn split_cypher(src: &str) -> Result<Vec<Statement>> {
    split_with(src, true)
}

fn split_with(src: &str, cypher: bool) -> Result<Vec<Statement>> {
    if open_at_end(src, cypher) {
        return Err(ShellError::shell(
            ShellCode::Parse,
            "the input ends inside a string, quoted name or comment",
        ));
    }
    let mut out = Vec::new();
    let mut delim = ";".to_owned();
    let mut start = 0;
    let toks = lex(src, cypher);
    let mut k = 0;
    while k < toks.len() {
        let t = toks[k];
        // DELIMITER at the start of a line (SQL only).
        if !cypher
            && t.kind == Tok::Word
            && t.text(src).eq_ignore_ascii_case("delimiter")
            && src[..t.start]
                .rsplit('\n')
                .next()
                .is_some_and(|l| l.trim().is_empty())
            && src[start..t.start].trim().is_empty()
        {
            let line_end = src[t.end..].find('\n').map_or(src.len(), |i| t.end + i);
            let d = src[t.end..line_end].trim();
            if !d.is_empty() {
                d.clone_into(&mut delim);
            }
            start = line_end;
            while k < toks.len() && toks[k].start < line_end {
                k += 1;
            }
            continue;
        }
        if matches!(t.kind, Tok::Op | Tok::Word | Tok::Number)
            && src[t.start..].starts_with(delim.as_str())
        {
            push(src, start, t.start, cypher, &mut out);
            start = t.start + delim.len();
            while k < toks.len() && toks[k].start < start {
                k += 1;
            }
            continue;
        }
        k += 1;
    }
    push(src, start, src.len(), cypher, &mut out);
    Ok(out)
}

fn push(src: &str, from: usize, to: usize, cypher: bool, out: &mut Vec<Statement>) {
    let s = &src[from..to];
    if s.trim().is_empty() || only_comments(s, cypher) {
        return;
    }
    let lead = s.len() - s.trim_start().len();
    out.push(Statement {
        text: s.trim().to_owned(),
        line: line_of(src, from + lead),
    });
}

/// The words of a statement at nesting depth 0, upper-cased, with comments,
/// strings and quoted names left out.
#[must_use]
pub fn top_words(src: &str, cypher: bool) -> Vec<String> {
    let mut depth = 0_i32;
    let mut out = Vec::new();
    for t in lex(src, cypher) {
        match t.kind {
            Tok::Op => match t.text(src) {
                "(" | "[" | "{" => depth += 1,
                ")" | "]" | "}" => depth -= 1,
                _ => {}
            },
            Tok::Word if depth == 0 => out.push(t.text(src).to_ascii_uppercase()),
            _ => {}
        }
    }
    out
}

/// Classifies a SQL statement.
#[must_use]
pub fn classify_sql(stmt: &str) -> Class {
    let w = top_words(stmt, false);
    let first = w.first().map_or("", String::as_str);
    let second = w.get(1).map_or("", String::as_str);
    match first {
        "SELECT" | "SHOW" | "DESCRIBE" | "DESC" | "EXPLAIN" | "WITH" | "TABLE" | "VALUES"
        | "HELP" | "CHECKSUM" => Class::Read,
        "INSERT" | "UPDATE" | "DELETE" | "REPLACE" | "CALL" | "LOAD" | "MERGE" | "DO"
        | "HANDLER" | "PURGE" => Class::Write,
        "GRANT" | "REVOKE" => Class::Account,
        "CREATE" | "ALTER" | "DROP" | "RENAME" if matches!(second, "USER" | "ROLE") => {
            Class::Account
        }
        "SET" if second == "PASSWORD" || second == "ROLE" || second == "DEFAULT" => Class::Account,
        "CREATE" | "ALTER" | "DROP" | "TRUNCATE" | "RENAME" | "BIND" | "UNBIND" | "OPTIMIZE"
        | "ANALYZE" | "REPAIR" => Class::Ddl,
        "BEGIN" | "START" | "COMMIT" | "ROLLBACK" | "SAVEPOINT" | "RELEASE" | "XA" => {
            Class::Transaction
        }
        _ => Class::Other,
    }
}

/// Why a statement is destructive, or `None`.
#[must_use]
pub fn destructive(stmt: &str, lang: Lang) -> Option<String> {
    match lang {
        Lang::Sql => destructive_sql(stmt),
        Lang::Cypher => destructive_cypher(stmt),
        Lang::Redis => destructive_redis(stmt),
        Lang::Mongo => destructive_mongo(stmt),
    }
}

fn destructive_sql(stmt: &str) -> Option<String> {
    let w = top_words(stmt, false);
    let first = w.first().map_or("", String::as_str);
    let has = |x: &str| w.iter().any(|v| v == x);
    match first {
        "DROP" => Some(format!("DROP {}", w.get(1).map_or("", String::as_str))),
        "TRUNCATE" => Some("TRUNCATE".to_owned()),
        "PURGE" => Some(format!("PURGE {}", w.get(1).map_or("", String::as_str))),
        "DELETE" if !has("WHERE") => Some("DELETE without WHERE".to_owned()),
        "UPDATE" if !has("WHERE") => Some("UPDATE without WHERE".to_owned()),
        "ALTER" => {
            let i = w.iter().position(|v| v == "DROP")?;
            (w.get(i + 1).map(String::as_str) != Some("DEFAULT"))
                .then(|| format!("ALTER ... DROP {}", w.get(i + 1).map_or("", String::as_str)))
        }
        "CALL" if stmt.to_ascii_lowercase().contains("mq_purge") => Some("queue purge".to_owned()),
        _ => None,
    }
}

fn destructive_cypher(stmt: &str) -> Option<String> {
    let w = top_words(stmt, true);
    if w.first().is_some_and(|f| f == "DROP") {
        return Some(format!("DROP {}", w.get(1).map_or("", String::as_str)));
    }
    let deletes = w.iter().any(|v| v == "DELETE");
    let filtered = w.iter().any(|v| v == "WHERE") || stmt.contains('{');
    (deletes && !filtered).then(|| "DELETE without WHERE".to_owned())
}

fn destructive_redis(stmt: &str) -> Option<String> {
    let argv = redis_argv(stmt).ok()?;
    let cmd = argv.first()?.to_ascii_uppercase();
    matches!(cmd.as_str(), "FLUSHDB" | "FLUSHALL" | "SWAPDB").then_some(cmd)
}

fn destructive_mongo(stmt: &str) -> Option<String> {
    let s: String = stmt.chars().filter(|c| !c.is_whitespace()).collect();
    for m in [".drop()", ".dropDatabase()"] {
        if s.contains(m) {
            return Some(
                m.trim_matches(|c| c == '.' || c == '(' || c == ')')
                    .to_owned(),
            );
        }
    }
    for m in [
        ".deleteMany({})",
        ".remove({})",
        ".deleteMany()",
        ".remove()",
    ] {
        if s.contains(m) {
            return Some(format!(
                "{} with an empty filter",
                &m[1..m.find('(').unwrap_or(1)]
            ));
        }
    }
    None
}

/// Splits one Redis command line into arguments: words, `"..."` with
/// backslash escapes, `'...'` taken literally (as `redis-cli` does).
///
/// # Errors
/// 39985 for an open quote.
pub fn redis_argv(line: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_word = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' => {
                in_word = true;
                loop {
                    match chars.next() {
                        Some('"') => break,
                        Some('\\') => match chars.next() {
                            Some('n') => cur.push('\n'),
                            Some('r') => cur.push('\r'),
                            Some('t') => cur.push('\t'),
                            Some('x') => {
                                let h: String = chars.by_ref().take(2).collect();
                                let v = u8::from_str_radix(&h, 16).map_err(|_| {
                                    ShellError::shell(ShellCode::Parse, format!("bad \\x{h}"))
                                })?;
                                cur.push(char::from(v));
                            }
                            Some(o) => cur.push(o),
                            None => break,
                        },
                        Some(o) => cur.push(o),
                        None => {
                            return Err(ShellError::shell(ShellCode::Parse, "open \" quote"));
                        }
                    }
                }
            }
            '\'' => {
                in_word = true;
                loop {
                    match chars.next() {
                        Some('\'') => break,
                        Some(o) => cur.push(o),
                        None => {
                            return Err(ShellError::shell(ShellCode::Parse, "open ' quote"));
                        }
                    }
                }
            }
            c if c.is_whitespace() => {
                if in_word {
                    out.push(std::mem::take(&mut cur));
                    in_word = false;
                }
            }
            c => {
                in_word = true;
                cur.push(c);
            }
        }
    }
    if in_word {
        out.push(cur);
    }
    Ok(out)
}

/// Splits Redis input: one command per non-empty line; `#` lines are
/// comments.
///
/// # Errors
/// 39985 for an open quote.
pub fn split_redis(src: &str) -> Result<Vec<Statement>> {
    let mut out = Vec::new();
    for (i, l) in src.lines().enumerate() {
        let t = l.trim();
        if t.is_empty() || t.starts_with('#') {
            continue;
        }
        redis_argv(t)?;
        out.push(Statement {
            text: t.to_owned(),
            line: i + 1,
        });
    }
    Ok(out)
}

/// Splits MongoDB input into call chains: `;` or a new line at depth 0
/// ends one, unless the next line continues it with `.`.
///
/// # Errors
/// 39985 for an open string or bracket.
pub fn split_mongo(src: &str) -> Result<Vec<Statement>> {
    let b = src.as_bytes();
    let mut out = Vec::new();
    let (mut depth, mut start, mut i) = (0_i32, 0, 0);
    while i < b.len() {
        match b[i] {
            q @ (b'"' | b'\'') => {
                i += 1;
                while i < b.len() && b[i] != q {
                    if b[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
                if i >= b.len() {
                    return Err(ShellError::shell(ShellCode::Parse, "open string"));
                }
            }
            b'/' if b.get(i + 1) == Some(&b'/') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            b';' if depth == 0 => {
                push_mongo(src, start, i, &mut out);
                start = i + 1;
            }
            b'\n' if depth == 0 => {
                let next = src[i + 1..].trim_start();
                if !next.starts_with('.') {
                    push_mongo(src, start, i, &mut out);
                    start = i + 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    if depth != 0 {
        return Err(ShellError::shell(ShellCode::Parse, "unbalanced brackets"));
    }
    push_mongo(src, start, src.len(), &mut out);
    Ok(out)
}

fn push_mongo(src: &str, from: usize, to: usize, out: &mut Vec<Statement>) {
    let s = &src[from..to];
    let t: String = s
        .lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    if t.trim().is_empty() {
        return;
    }
    let lead = s.len() - s.trim_start().len();
    out.push(Statement {
        text: t.trim().to_owned(),
        line: line_of(src, from + lead),
    });
}

/// Splits input in `lang`.
///
/// # Errors
/// 39985 as the language's splitter.
pub fn split(src: &str, lang: Lang) -> Result<Vec<Statement>> {
    match lang {
        Lang::Sql => split_sql(src),
        Lang::Cypher => split_cypher(src),
        Lang::Redis => split_redis(src),
        Lang::Mongo => split_mongo(src),
    }
}

/// SQL keywords, for highlighting and completion.
pub const SQL_KEYWORDS: &[&str] = &[
    "ADD",
    "ALL",
    "ALTER",
    "ANALYZE",
    "AND",
    "AS",
    "ASC",
    "AUTO_INCREMENT",
    "BEGIN",
    "BETWEEN",
    "BIGINT",
    "BINARY",
    "BIND",
    "BLOB",
    "BY",
    "CALL",
    "CASE",
    "CHANGES",
    "CHAR",
    "CHECK",
    "COLUMN",
    "COMMIT",
    "CONNECTION",
    "CONSTRAINT",
    "CREATE",
    "CROSS",
    "DATABASE",
    "DATABASES",
    "DATE",
    "DATETIME",
    "DECIMAL",
    "DEFAULT",
    "DELETE",
    "DESC",
    "DESCRIBE",
    "DISTINCT",
    "DOUBLE",
    "DROP",
    "ELSE",
    "END",
    "ENUM",
    "EXCHANGE",
    "EXISTS",
    "EXPLAIN",
    "FALSE",
    "FOR",
    "FOREIGN",
    "FROM",
    "FULL",
    "FUNCTION",
    "GRANT",
    "GRAPH",
    "GROUP",
    "HAVING",
    "IDENTIFIED",
    "IF",
    "IN",
    "INDEX",
    "INNER",
    "INSERT",
    "INT",
    "INTEGER",
    "INTERVAL",
    "INTO",
    "IS",
    "JOIN",
    "JSON",
    "KEY",
    "KILL",
    "LEFT",
    "LIKE",
    "LIMIT",
    "NOT",
    "NULL",
    "OFFSET",
    "ON",
    "OR",
    "ORDER",
    "OUTER",
    "PRIMARY",
    "PROCEDURE",
    "PROCESSLIST",
    "QUEUE",
    "QUEUES",
    "REFERENCES",
    "RENAME",
    "REPLACE",
    "REVOKE",
    "RIGHT",
    "ROLE",
    "ROLLBACK",
    "SELECT",
    "SET",
    "SHOW",
    "SINK",
    "SINKS",
    "SPRIG",
    "START",
    "STREAM",
    "STREAMS",
    "TABLE",
    "TABLES",
    "TENANT",
    "TEXT",
    "THEN",
    "TIMESTAMP",
    "TO",
    "TRANSACTION",
    "TRIGGER",
    "TRUE",
    "TRUNCATE",
    "UNION",
    "UNIQUE",
    "UPDATE",
    "USE",
    "USER",
    "USING",
    "VALUES",
    "VARCHAR",
    "VIEW",
    "WHEN",
    "WHERE",
    "WITH",
];

/// Cypher keywords.
pub const CYPHER_KEYWORDS: &[&str] = &[
    "AND",
    "AS",
    "ASC",
    "BY",
    "CALL",
    "CASE",
    "CONSTRAINT",
    "CONTAINS",
    "CREATE",
    "DELETE",
    "DESC",
    "DETACH",
    "DISTINCT",
    "DROP",
    "ELSE",
    "END",
    "ENDS",
    "EXISTS",
    "FALSE",
    "FOREACH",
    "IN",
    "INDEX",
    "IS",
    "LIMIT",
    "MATCH",
    "MERGE",
    "NOT",
    "NULL",
    "ON",
    "OPTIONAL",
    "OR",
    "ORDER",
    "REMOVE",
    "RETURN",
    "SET",
    "SKIP",
    "STARTS",
    "THEN",
    "TRUE",
    "UNION",
    "UNWIND",
    "WHEN",
    "WHERE",
    "WITH",
    "XOR",
    "YIELD",
];

/// Redis commands with their arity (negative: at least), for completion.
pub const REDIS_COMMANDS: &[(&str, i32)] = &[
    ("APPEND", 3),
    ("DBSIZE", 1),
    ("DECR", 2),
    ("DEL", -2),
    ("EXISTS", -2),
    ("EXPIRE", -3),
    ("FLUSHDB", -1),
    ("FLUSHALL", -1),
    ("GET", 2),
    ("GETDEL", 2),
    ("HDEL", -3),
    ("HGET", 3),
    ("HGETALL", 2),
    ("HINCRBY", 4),
    ("HKEYS", 2),
    ("HLEN", 2),
    ("HMGET", -3),
    ("HSET", -4),
    ("INCR", 2),
    ("INCRBY", 3),
    ("INFO", -1),
    ("KEYS", 2),
    ("LINDEX", 3),
    ("LLEN", 2),
    ("LPOP", -2),
    ("LPUSH", -3),
    ("LRANGE", 4),
    ("MGET", -2),
    ("MSET", -3),
    ("PERSIST", 2),
    ("PEXPIRE", -3),
    ("PING", -1),
    ("PTTL", 2),
    ("PUBLISH", 3),
    ("RPOP", -2),
    ("RPUSH", -3),
    ("SADD", -3),
    ("SCAN", -2),
    ("SCARD", 2),
    ("SELECT", 2),
    ("SET", -3),
    ("SISMEMBER", 3),
    ("SMEMBERS", 2),
    ("SREM", -3),
    ("STRLEN", 2),
    ("SWAPDB", 3),
    ("TTL", 2),
    ("TYPE", 2),
    ("UNLINK", -2),
    ("XADD", -5),
    ("XLEN", 2),
    ("XRANGE", -4),
    ("ZADD", -4),
    ("ZCARD", 2),
    ("ZRANGE", -4),
    ("ZREM", -3),
    ("ZSCORE", 3),
];

/// Whether `w` is a keyword of `lang`.
#[must_use]
pub fn is_keyword(w: &str, lang: Lang) -> bool {
    let up = w.to_ascii_uppercase();
    match lang {
        Lang::Sql => SQL_KEYWORDS.binary_search(&up.as_str()).is_ok(),
        Lang::Cypher => CYPHER_KEYWORDS.binary_search(&up.as_str()).is_ok(),
        Lang::Redis => REDIS_COMMANDS.iter().any(|(c, _)| *c == up),
        Lang::Mongo => matches!(
            w,
            "db" | "find"
                | "findOne"
                | "aggregate"
                | "insertOne"
                | "insertMany"
                | "updateOne"
                | "updateMany"
                | "deleteOne"
                | "deleteMany"
                | "countDocuments"
                | "sort"
                | "limit"
                | "skip"
                | "projection"
                | "runCommand"
                | "drop"
                | "createIndex"
                | "getIndexes"
                | "distinct"
                | "replaceOne"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn texts(v: &[Statement]) -> Vec<&str> {
        v.iter().map(|s| s.text.as_str()).collect()
    }

    #[test]
    fn keyword_tables_are_sorted() {
        assert!(SQL_KEYWORDS.windows(2).all(|w| w[0] < w[1]));
        assert!(CYPHER_KEYWORDS.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn splits_sql() {
        let s = split_sql(
            "SELECT 'a;b', `c;d` FROM t; -- x;y\n# z;\nINSERT INTO t VALUES (\"q\\\";\");\n/* ; */ SELECT 2",
        )
        .expect("split");
        assert_eq!(
            texts(&s),
            [
                "SELECT 'a;b', `c;d` FROM t",
                "-- x;y\n# z;\nINSERT INTO t VALUES (\"q\\\";\")",
                "/* ; */ SELECT 2"
            ]
        );
        assert_eq!(s[1].line, 1);
        assert_eq!(s[2].line, 4);
        assert!(split_sql("SELECT 'open").is_err());
        assert!(split_sql("SELECT 1 /* open").is_err());
        assert!(split_sql("-- only a comment\n").expect("empty").is_empty());
    }

    #[test]
    fn splits_with_delimiter() {
        let s = split_sql(
            "DELIMITER //\nCREATE PROCEDURE p() BEGIN SELECT 1; SELECT 2; END//\nDELIMITER ;\nCALL p();",
        )
        .expect("split");
        assert_eq!(
            texts(&s),
            [
                "CREATE PROCEDURE p() BEGIN SELECT 1; SELECT 2; END",
                "CALL p()"
            ]
        );
    }

    #[test]
    fn classifies() {
        assert_eq!(classify_sql("select 1"), Class::Read);
        assert_eq!(
            classify_sql("/* x */ WITH a AS (SELECT 1) SELECT * FROM a"),
            Class::Read
        );
        assert_eq!(classify_sql("insert into t values (1)"), Class::Write);
        assert_eq!(classify_sql("CREATE USER bob"), Class::Account);
        assert_eq!(classify_sql("create table t (a int)"), Class::Ddl);
        assert_eq!(classify_sql("start transaction"), Class::Transaction);
        assert_eq!(classify_sql("USE shop"), Class::Other);
    }

    #[test]
    fn finds_destructive_statements() {
        let d = |s| destructive(s, Lang::Sql);
        assert!(d("DROP TABLE t").is_some());
        assert!(d("truncate t").is_some());
        assert!(d("DELETE FROM t").is_some());
        assert!(d("DELETE FROM t WHERE id = 1").is_none());
        assert!(d("DELETE FROM t WHERE id IN (SELECT id FROM u)").is_none());
        assert!(d("DELETE FROM t USING t JOIN (SELECT 1 FROM u WHERE x) z").is_some());
        assert!(d("UPDATE t SET a = 1").is_some());
        assert!(d("ALTER TABLE t DROP COLUMN a").is_some());
        assert!(d("ALTER TABLE t ALTER COLUMN a DROP DEFAULT").is_none());
        assert!(d("SELECT 'DROP TABLE x'").is_none());
        assert!(d("-- DROP TABLE x\nSELECT 1").is_none());
        assert!(d("DROP STREAM orders_events").is_some());
        assert!(destructive("MATCH (n) DETACH DELETE n", Lang::Cypher).is_some());
        assert!(destructive("MATCH (n:P) WHERE n.id = 1 DELETE n", Lang::Cypher).is_none());
        assert!(destructive("flushall", Lang::Redis).is_some());
        assert!(destructive("DEL a b", Lang::Redis).is_none());
        assert!(destructive("db.users.drop()", Lang::Mongo).is_some());
        assert!(destructive("db.users.deleteMany( { } )", Lang::Mongo).is_some());
        assert!(destructive("db.users.deleteMany({a: 1})", Lang::Mongo).is_none());
    }

    #[test]
    fn redis_arguments() {
        assert_eq!(
            redis_argv(r#"SET "a b" 'c\n' "x\"y\x41""#).expect("argv"),
            ["SET", "a b", "c\\n", "x\"yA"]
        );
        assert!(redis_argv("GET \"open").is_err());
        let s = split_redis("# c\nPING\n\n  GET k  \n").expect("split");
        assert_eq!(texts(&s), ["PING", "GET k"]);
        assert_eq!(s[1].line, 4);
    }

    #[test]
    fn splits_mongo_and_cypher() {
        let s = split_mongo(
            "db.a.find({x: ';'})\n  .limit(2)\ndb.b.insertOne({\n y: 1\n}); db.c.drop()",
        )
        .expect("split");
        assert_eq!(
            texts(&s),
            [
                "db.a.find({x: ';'})\n  .limit(2)",
                "db.b.insertOne({\n y: 1\n})",
                "db.c.drop()"
            ]
        );
        assert!(split_mongo("db.a.find({").is_err());
        let c = split_cypher("MATCH (n) RETURN n; // x;\nRETURN ';'").expect("split");
        assert_eq!(texts(&c), ["MATCH (n) RETURN n", "// x;\nRETURN ';'"]);
    }
}
