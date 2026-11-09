//! The stream statements (chapter 45, "SQL"), recognized on chapter 07's
//! lexer before the name plan:
//!
//! ```text
//! CREATE STREAM [IF NOT EXISTS] name
//!   [KEY (col, ...)] [ATTACHED TO GROUP OF table]
//!   [PARTITIONS n] [RETENTION 'text'] [CLEANUP delete | compact]
//! ALTER STREAM name [PARTITIONS n] [RETENTION 'text'] [CLEANUP delete | compact]
//! DROP STREAM [IF EXISTS] name
//! SHOW STREAMS [FROM | IN db]
//! INSERT [IGNORE] [INTO] name [(key, value, headers, partition_no, ts)]
//!   VALUES ... | SELECT ...                      -- publishes
//! UPDATE | DELETE | REPLACE | TRUNCATE on a stream -- 39941
//! ```
//!
//! Names with dots or dashes (`orders.changes`) are quoted, as any such
//! identifier is: `` `orders.changes` ``; `db.name` qualifies.

use std::ops::Range;

use sprig_common::{Result, SprigError};
use sprig_sql::lex::{LexOptions, Token, TokenKind, ident_value, lex, string_value};

use crate::model::Cleanup;
use crate::settings::parse_retention;

/// A possibly qualified name and where it is in the text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QName {
    /// The database, if qualified.
    pub db: Option<String>,
    /// The name.
    pub name: String,
    /// The bytes of the whole name in the text.
    pub range: Range<usize>,
}

/// `CREATE STREAM`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CreateStream {
    /// The stream.
    pub name: Option<QName>,
    /// `IF NOT EXISTS`.
    pub if_not_exists: bool,
    /// `KEY (...)`.
    pub key: Vec<String>,
    /// `ATTACHED TO GROUP OF t`.
    pub attached: Option<QName>,
    /// `PARTITIONS n`.
    pub partitions: Option<u32>,
    /// `RETENTION '...'`, in hours.
    pub retention_hours: Option<u64>,
    /// `CLEANUP ...`.
    pub cleanup: Option<Cleanup>,
}

/// `ALTER STREAM`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterStream {
    /// The stream.
    pub name: QName,
    /// `PARTITIONS n`.
    pub partitions: Option<u32>,
    /// `RETENTION '...'`, in hours.
    pub retention_hours: Option<u64>,
    /// `CLEANUP ...`.
    pub cleanup: Option<Cleanup>,
    /// `[SPRIG] SET (key = value, ...)` and `[SPRIG] RESET (key, ...)`:
    /// stream settings at the stream's own scope, in order; `None` resets.
    pub settings: Vec<(String, Option<String>)>,
}

/// An `INSERT` whose target may be a stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertInto {
    /// The target.
    pub target: QName,
    /// The column list as written: names and their bytes.
    pub columns: Option<Vec<(String, Range<usize>)>>,
    /// Where a column list would go when there is none (just after the
    /// target).
    pub after_target: usize,
    /// `ON DUPLICATE KEY UPDATE`.
    pub on_duplicate: bool,
    /// `INSERT ... SET col = ...`.
    pub set_form: bool,
    /// Values in the first `VALUES` row, when there is one.
    pub first_row_arity: Option<usize>,
    /// The longest string literal in the statement, in bytes.
    pub longest_literal: usize,
}

/// What a statement is, for the stream layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stmt {
    /// `CREATE STREAM`.
    Create(CreateStream),
    /// `ALTER STREAM`.
    Alter(AlterStream),
    /// `DROP STREAM`.
    Drop {
        /// `IF EXISTS`.
        if_exists: bool,
        /// The stream.
        name: QName,
    },
    /// `SHOW STREAMS`.
    Show {
        /// `FROM db`.
        db: Option<String>,
    },
    /// `INSERT` (publishes when the target is a stream).
    Insert(InsertInto),
    /// `REPLACE`, `UPDATE`, `DELETE` or `TRUNCATE` of `target` (39941 when
    /// it is a stream).
    Write {
        /// The verb.
        verb: &'static str,
        /// The first table written.
        target: QName,
    },
    /// `CREATE TABLE name` (39942 when a stream has the name).
    CreateTable(QName),
    /// `DROP DATABASE | SCHEMA [IF EXISTS] name`: its streams go first.
    DropDatabase(String),
    /// Anything else: a read may name streams.
    Other,
}

fn syntax(near: &str) -> SprigError {
    SprigError::standard(
        1064,
        *b"42000",
        format!(
            "You have an error in your SQL syntax; check the manual that corresponds to your server version for the right syntax to use near '{near}'"
        ),
    )
}

/// A cursor over the non-trivia tokens of a statement.
struct Cur<'a> {
    src: &'a [u8],
    toks: Vec<Token>,
    i: usize,
}

impl<'a> Cur<'a> {
    fn new(src: &'a [u8], lex_opts: LexOptions) -> Option<Self> {
        let toks = lex(src, &lex_opts)
            .ok()?
            .into_iter()
            .filter(|t| !t.kind.is_trivia())
            .collect();
        Some(Self { src, toks, i: 0 })
    }

    fn peek(&self) -> Option<&Token> {
        self.toks.get(self.i)
    }

    fn peek_at(&self, n: usize) -> Option<&Token> {
        self.toks.get(self.i + n)
    }

    fn text(&self, t: &Token) -> &'a [u8] {
        &self.src[t.range()]
    }

    fn is_word(&self, t: Option<&Token>, w: &str) -> bool {
        t.is_some_and(|t| {
            t.kind == TokenKind::Word && self.text(t).eq_ignore_ascii_case(w.as_bytes())
        })
    }

    fn at(&self, w: &str) -> bool {
        self.is_word(self.peek(), w)
    }

    fn eat(&mut self, w: &str) -> bool {
        if self.at(w) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn punct(&self, p: u8) -> bool {
        self.peek()
            .is_some_and(|t| t.kind == TokenKind::Punct && self.text(t) == [p])
    }

    fn eat_punct(&mut self, p: u8) -> bool {
        if self.punct(p) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn eat_op(&mut self, op: &[u8]) -> bool {
        if self
            .peek()
            .is_some_and(|t| t.kind == TokenKind::Op && self.text(t) == op)
        {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn near(&self) -> String {
        match self.peek() {
            Some(t) => String::from_utf8_lossy(&self.src[t.start as usize..])
                .chars()
                .take(40)
                .collect(),
            None => String::new(),
        }
    }

    fn expect(&mut self, w: &str) -> Result<()> {
        if self.eat(w) {
            Ok(())
        } else {
            Err(syntax(&self.near()))
        }
    }

    fn ident(&mut self) -> Option<(String, Range<usize>)> {
        let t = *self.peek()?;
        if !t.kind.is_ident_like() {
            return None;
        }
        self.i += 1;
        let v = String::from_utf8_lossy(&ident_value(self.text(&t))).into_owned();
        Some((v, t.range()))
    }

    fn qname(&mut self) -> Option<QName> {
        let (first, r) = self.ident()?;
        if self.punct(b'.') && self.peek_at(1).is_some_and(|t| t.kind.is_ident_like()) {
            self.i += 1;
            let (second, r2) = self.ident()?;
            return Some(QName {
                db: Some(first),
                name: second,
                range: r.start..r2.end,
            });
        }
        Some(QName {
            db: None,
            name: first,
            range: r,
        })
    }

    fn number(&mut self) -> Option<u64> {
        let t = *self.peek()?;
        if t.kind != TokenKind::Number {
            return None;
        }
        self.i += 1;
        std::str::from_utf8(self.text(&t)).ok()?.parse().ok()
    }

    fn string_or_word(&mut self) -> Option<String> {
        let t = *self.peek()?;
        self.i += 1;
        match t.kind {
            TokenKind::String => {
                Some(String::from_utf8_lossy(&string_value(self.text(&t), false)?).into_owned())
            }
            TokenKind::Word | TokenKind::QuotedIdent | TokenKind::Number => {
                Some(String::from_utf8_lossy(&ident_value(self.text(&t))).into_owned())
            }
            _ => None,
        }
    }

    fn done(&mut self) -> bool {
        self.eat_punct(b';');
        self.peek().is_none()
    }
}

/// Quick test: could `sql` be a stream statement or name a stream? True
/// when it has the word `STREAM`/`STREAMS`, or `has_streams` and it isn't
/// trivially unrelated.
#[must_use]
pub fn maybe_stream(sql: &[u8], has_streams: bool) -> bool {
    if has_streams {
        return true;
    }
    let up = sql.len().min(64);
    let head = &sql[..up];
    head.windows(6).any(|w| w.eq_ignore_ascii_case(b"stream"))
}

/// Classifies one statement.
///
/// # Errors
/// 1064 for a malformed stream statement.
#[allow(clippy::too_many_lines)] // one branch per statement kind
pub fn classify(sql: &[u8], lex_opts: &LexOptions) -> Result<Stmt> {
    let Some(mut c) = Cur::new(sql, *lex_opts) else {
        return Ok(Stmt::Other);
    };
    if c.at("CREATE") && c.is_word(c.peek_at(1), "STREAM") {
        c.i += 2;
        return create(&mut c).map(Stmt::Create);
    }
    if c.at("ALTER") && c.is_word(c.peek_at(1), "STREAM") {
        c.i += 2;
        return alter(&mut c).map(Stmt::Alter);
    }
    if c.at("DROP") && c.is_word(c.peek_at(1), "STREAM") {
        c.i += 2;
        let if_exists = if c.eat("IF") {
            c.expect("EXISTS")?;
            true
        } else {
            false
        };
        let name = c.qname().ok_or_else(|| syntax(&c.near()))?;
        if !c.done() {
            return Err(syntax(&c.near()));
        }
        return Ok(Stmt::Drop { if_exists, name });
    }
    if c.at("DROP") && (c.is_word(c.peek_at(1), "DATABASE") || c.is_word(c.peek_at(1), "SCHEMA")) {
        c.i += 2;
        if c.eat("IF") {
            c.eat("EXISTS");
        }
        return Ok(c
            .ident()
            .map_or(Stmt::Other, |(n, _)| Stmt::DropDatabase(n)));
    }
    if c.at("SHOW") && c.is_word(c.peek_at(1), "STREAMS") {
        c.i += 2;
        let db = if c.eat("FROM") || c.eat("IN") {
            Some(c.ident().ok_or_else(|| syntax(&c.near()))?.0)
        } else {
            None
        };
        if !c.done() {
            return Err(syntax(&c.near()));
        }
        return Ok(Stmt::Show { db });
    }
    if c.at("INSERT") {
        c.i += 1;
        return Ok(insert(&mut c).map_or(Stmt::Other, Stmt::Insert));
    }
    if c.eat("REPLACE") {
        while c.eat("LOW_PRIORITY") || c.eat("DELAYED") {}
        c.eat("INTO");
        return Ok(c.qname().map_or(Stmt::Other, |target| Stmt::Write {
            verb: "REPLACE",
            target,
        }));
    }
    if c.eat("UPDATE") {
        while c.eat("LOW_PRIORITY") || c.eat("IGNORE") {}
        return Ok(c.qname().map_or(Stmt::Other, |target| Stmt::Write {
            verb: "UPDATE",
            target,
        }));
    }
    if c.eat("DELETE") {
        while c.eat("LOW_PRIORITY") || c.eat("QUICK") || c.eat("IGNORE") {}
        if !c.eat("FROM") {
            // Multi-table DELETE t1 FROM ...: the first target.
            return Ok(c.qname().map_or(Stmt::Other, |target| Stmt::Write {
                verb: "DELETE",
                target,
            }));
        }
        return Ok(c.qname().map_or(Stmt::Other, |target| Stmt::Write {
            verb: "DELETE",
            target,
        }));
    }
    if c.eat("TRUNCATE") {
        c.eat("TABLE");
        return Ok(c.qname().map_or(Stmt::Other, |target| Stmt::Write {
            verb: "TRUNCATE",
            target,
        }));
    }
    if c.at("CREATE") {
        let mut j = 1;
        while c.is_word(c.peek_at(j), "OR")
            || c.is_word(c.peek_at(j), "REPLACE")
            || c.is_word(c.peek_at(j), "TEMPORARY")
        {
            j += 1;
        }
        if c.is_word(c.peek_at(j), "TABLE") {
            c.i += j + 1;
            if c.eat("IF") {
                c.eat("NOT");
                c.eat("EXISTS");
            }
            return Ok(c.qname().map_or(Stmt::Other, Stmt::CreateTable));
        }
    }
    Ok(Stmt::Other)
}

fn create(c: &mut Cur<'_>) -> Result<CreateStream> {
    let mut s = CreateStream::default();
    if c.eat("IF") {
        c.expect("NOT")?;
        c.expect("EXISTS")?;
        s.if_not_exists = true;
    }
    s.name = Some(c.qname().ok_or_else(|| syntax(&c.near()))?);
    loop {
        if c.done() {
            return Ok(s);
        }
        if c.eat("KEY") {
            if !c.eat_punct(b'(') {
                return Err(syntax(&c.near()));
            }
            loop {
                let (k, _) = c.ident().ok_or_else(|| syntax(&c.near()))?;
                s.key.push(k);
                if c.eat_punct(b')') {
                    break;
                }
                if !c.eat_punct(b',') {
                    return Err(syntax(&c.near()));
                }
            }
        } else if c.eat("ATTACHED") {
            c.expect("TO")?;
            c.expect("GROUP")?;
            c.expect("OF")?;
            s.attached = Some(c.qname().ok_or_else(|| syntax(&c.near()))?);
        } else if !options(c, &mut s.partitions, &mut s.retention_hours, &mut s.cleanup)? {
            return Err(syntax(&c.near()));
        }
    }
}

/// `PARTITIONS n`, `RETENTION '...'`, `CLEANUP x`: whether one was read.
fn options(
    c: &mut Cur<'_>,
    partitions: &mut Option<u32>,
    retention: &mut Option<u64>,
    cleanup: &mut Option<Cleanup>,
) -> Result<bool> {
    if c.eat("PARTITIONS") {
        c.eat_op(b"=");
        let n = c
            .number()
            .and_then(|n| u32::try_from(n).ok())
            .filter(|n| *n > 0)
            .ok_or_else(|| syntax(&c.near()))?;
        *partitions = Some(n);
        return Ok(true);
    }
    if c.eat("RETENTION") {
        c.eat_op(b"=");
        let t = c.string_or_word().ok_or_else(|| syntax(&c.near()))?;
        *retention = Some(parse_retention(&t).ok_or_else(|| syntax(&t))?);
        return Ok(true);
    }
    if c.eat("CLEANUP") {
        c.eat_op(b"=");
        let t = c.string_or_word().ok_or_else(|| syntax(&c.near()))?;
        *cleanup = Some(Cleanup::parse(&t).ok_or_else(|| syntax(&t))?);
        return Ok(true);
    }
    Ok(false)
}

/// A setting name: words joined by dots (`stream.retention_bytes`).
fn setting_name(c: &mut Cur<'_>) -> Option<String> {
    let (mut n, _) = c.ident()?;
    while c.punct(b'.') && c.peek_at(1).is_some_and(|t| t.kind.is_ident_like()) {
        c.i += 1;
        let (more, _) = c.ident()?;
        n.push('.');
        n.push_str(&more);
    }
    Some(n.to_ascii_lowercase())
}

/// `SET (k = v, ...)` or `RESET (k, ...)` after `ALTER STREAM s [SPRIG]`.
fn settings(c: &mut Cur<'_>, reset: bool, out: &mut Vec<(String, Option<String>)>) -> Result<()> {
    if !c.eat_punct(b'(') {
        return Err(syntax(&c.near()));
    }
    loop {
        let name = setting_name(c).ok_or_else(|| syntax(&c.near()))?;
        if reset {
            out.push((name, None));
        } else {
            if !c.eat_op(b"=") {
                return Err(syntax(&c.near()));
            }
            let v = c.string_or_word().ok_or_else(|| syntax(&c.near()))?;
            out.push((name, Some(v)));
        }
        if c.eat_punct(b',') {
            continue;
        }
        if c.eat_punct(b')') {
            return Ok(());
        }
        return Err(syntax(&c.near()));
    }
}

fn alter(c: &mut Cur<'_>) -> Result<AlterStream> {
    let name = c.qname().ok_or_else(|| syntax(&c.near()))?;
    let mut a = AlterStream {
        name,
        partitions: None,
        retention_hours: None,
        cleanup: None,
        settings: Vec::new(),
    };
    let mut any = false;
    loop {
        if c.done() {
            if !any {
                return Err(syntax(""));
            }
            return Ok(a);
        }
        c.eat_punct(b',');
        let sprig = c.eat("SPRIG");
        if c.eat("SET") {
            settings(c, false, &mut a.settings)?;
        } else if c.eat("RESET") {
            settings(c, true, &mut a.settings)?;
        } else if sprig || !options(c, &mut a.partitions, &mut a.retention_hours, &mut a.cleanup)? {
            return Err(syntax(&c.near()));
        }
        any = true;
    }
}

fn insert(c: &mut Cur<'_>) -> Option<InsertInto> {
    while c.eat("LOW_PRIORITY") || c.eat("DELAYED") || c.eat("HIGH_PRIORITY") || c.eat("IGNORE") {}
    c.eat("INTO");
    let target = c.qname()?;
    let after_target = target.range.end;
    let mut columns = None;
    if c.punct(b'(') && !c.is_word(c.peek_at(1), "SELECT") && !c.is_word(c.peek_at(1), "WITH") {
        c.i += 1;
        let mut cols = Vec::new();
        loop {
            let (n, r) = c.ident()?;
            cols.push((n, r));
            if c.eat_punct(b')') {
                break;
            }
            if !c.eat_punct(b',') {
                return None;
            }
        }
        columns = Some(cols);
    }
    let set_form = c.at("SET");
    let mut first_row_arity = None;
    if (c.eat("VALUES") || c.eat("VALUE")) && c.punct(b'(') {
        {
            let mut depth = 0usize;
            let mut n = 1usize;
            let mut empty = true;
            while let Some(t) = c.peek().copied() {
                c.i += 1;
                if t.kind == TokenKind::Punct {
                    match c.text(&t) {
                        b"(" => {
                            depth += 1;
                            continue;
                        }
                        b")" => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                            continue;
                        }
                        b"," if depth == 1 => n += 1,
                        _ => {}
                    }
                }
                if depth >= 1 {
                    empty = false;
                }
            }
            first_row_arity = Some(if empty { 0 } else { n });
        }
    }
    let mut on_duplicate = false;
    let mut longest_literal = 0;
    let rest_start = c.i;
    while let Some(t) = c.peek().copied() {
        if t.kind == TokenKind::String {
            longest_literal = longest_literal.max(t.range().len().saturating_sub(2));
        }
        if c.is_word(Some(&t), "DUPLICATE") {
            on_duplicate = true;
        }
        c.i += 1;
    }
    // Literals inside the first row count too.
    for t in &c.toks[..rest_start] {
        if t.kind == TokenKind::String {
            longest_literal = longest_literal.max(t.range().len().saturating_sub(2));
        }
    }
    Some(InsertInto {
        target,
        columns,
        after_target,
        on_duplicate,
        set_form,
        first_row_arity,
        longest_literal,
    })
}

/// Identifiers in `sql` (words and quoted names, values only), for the
/// quick "does it name a stream" test.
#[must_use]
pub fn identifiers(sql: &[u8], lex_opts: &LexOptions) -> Vec<String> {
    let Ok(toks) = lex(sql, lex_opts) else {
        return Vec::new();
    };
    toks.iter()
        .filter(|t| t.kind.is_ident_like())
        .map(|t| String::from_utf8_lossy(&ident_value(t.bytes(sql))).into_owned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cl(s: &str) -> Stmt {
        classify(s.as_bytes(), &LexOptions::mariadb()).unwrap()
    }

    #[test]
    fn create_alter_drop_show() {
        let Stmt::Create(c) = cl(
            "CREATE STREAM order_events KEY (order_id) ATTACHED TO GROUP OF orders \
             PARTITIONS 12 RETENTION '7 days' CLEANUP delete;",
        ) else {
            panic!()
        };
        assert_eq!(c.name.unwrap().name, "order_events");
        assert_eq!(c.key, ["order_id"]);
        assert_eq!(c.attached.unwrap().name, "orders");
        assert_eq!(c.partitions, Some(12));
        assert_eq!(c.retention_hours, Some(168));
        assert_eq!(c.cleanup, Some(Cleanup::Delete));
        let Stmt::Create(c) = cl("create stream if not exists shop.`a.b` cleanup = compact") else {
            panic!()
        };
        let n = c.name.unwrap();
        assert_eq!((n.db.as_deref(), n.name.as_str()), (Some("shop"), "a.b"));
        assert!(c.if_not_exists);
        assert!(classify(b"CREATE STREAM s PARTITIONS 0", &LexOptions::mariadb()).is_err());
        assert!(classify(b"CREATE STREAM s FOO", &LexOptions::mariadb()).is_err());
        let Stmt::Alter(a) = cl("ALTER STREAM s PARTITIONS 24, RETENTION '3 days'") else {
            panic!()
        };
        assert_eq!((a.partitions, a.retention_hours), (Some(24), Some(72)));
        assert!(classify(b"ALTER STREAM s", &LexOptions::mariadb()).is_err());
        let Stmt::Alter(a) = cl(
            "ALTER STREAM shop.s SET (stream.retention_bytes = 1048576, stream.max_message_bytes = '2000') \
             RESET (stream.tombstone_hours) SPRIG SET (stream.retention_hours = 5)",
        ) else {
            panic!()
        };
        assert_eq!(
            a.settings,
            vec![
                ("stream.retention_bytes".into(), Some("1048576".into())),
                ("stream.max_message_bytes".into(), Some("2000".into())),
                ("stream.tombstone_hours".into(), None),
                ("stream.retention_hours".into(), Some("5".into())),
            ]
        );
        assert!(classify(b"ALTER STREAM s SET stream.x = 1", &LexOptions::mariadb()).is_err());
        assert!(classify(b"ALTER STREAM s SET (stream.x 1)", &LexOptions::mariadb()).is_err());
        assert_eq!(
            cl("DROP STREAM IF EXISTS `orders.changes`"),
            Stmt::Drop {
                if_exists: true,
                name: QName {
                    db: None,
                    name: "orders.changes".into(),
                    range: 22..38
                }
            }
        );
        assert_eq!(
            cl("SHOW STREAMS FROM shop"),
            Stmt::Show {
                db: Some("shop".into())
            }
        );
    }

    #[test]
    fn inserts_and_writes() {
        let s = "INSERT INTO order_events (key, value, headers) VALUES (7001, JSON_OBJECT('type','order.paid'), NULL)";
        let Stmt::Insert(i) = cl(s) else { panic!() };
        assert_eq!(i.target.name, "order_events");
        let cols: Vec<&str> = i
            .columns
            .as_ref()
            .unwrap()
            .iter()
            .map(|c| c.0.as_str())
            .collect();
        assert_eq!(cols, ["key", "value", "headers"]);
        assert_eq!(i.first_row_arity, Some(3));
        assert_eq!(i.longest_literal, 10);
        assert!(!i.on_duplicate);
        let Stmt::Insert(i) = cl("insert s values ('k', 'v') on duplicate key update v = 1") else {
            panic!()
        };
        assert!(i.columns.is_none());
        assert_eq!(i.first_row_arity, Some(2));
        assert!(i.on_duplicate);
        assert_eq!(&s.as_bytes()[i.after_target..i.after_target], b"");
        let Stmt::Insert(i) = cl("INSERT INTO s (key, value) SELECT a, b FROM t") else {
            panic!()
        };
        assert_eq!(i.first_row_arity, None);
        assert!(matches!(
            cl("UPDATE s SET v = 1"),
            Stmt::Write { verb: "UPDATE", .. }
        ));
        assert!(matches!(
            cl("DELETE FROM shop.s WHERE 1"),
            Stmt::Write { verb: "DELETE", .. }
        ));
        assert!(matches!(
            cl("TRUNCATE TABLE s"),
            Stmt::Write {
                verb: "TRUNCATE",
                ..
            }
        ));
        assert!(matches!(
            cl("REPLACE INTO s VALUES (1)"),
            Stmt::Write {
                verb: "REPLACE",
                ..
            }
        ));
        assert!(matches!(
            cl("CREATE TABLE IF NOT EXISTS s (a INT)"),
            Stmt::CreateTable(_)
        ));
        assert_eq!(cl("SELECT 1"), Stmt::Other);
        assert_eq!(
            cl("DROP DATABASE IF EXISTS shop"),
            Stmt::DropDatabase("shop".into())
        );
    }

    #[test]
    fn quick_test() {
        assert!(maybe_stream(b"CREATE STREAM x", false));
        assert!(!maybe_stream(b"SELECT 1", false));
        assert!(maybe_stream(b"SELECT 1", true));
    }
}
