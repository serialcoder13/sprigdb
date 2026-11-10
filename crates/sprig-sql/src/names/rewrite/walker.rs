//! The lexer-level walker: statement splitting, every name position, and
//! the substitutions.
//!
//! The walker is a small recursive-descent recognizer over the persona's
//! tokens. It does not build a tree; it knows just enough of the MySQL and
//! MariaDB grammar to tell the three places a dotted name can stand:
//!
//! - **Object positions** (after `FROM`, `JOIN`, `INTO`, `UPDATE`,
//!   `TABLE`, `REFERENCES`, `CALL`, the `CREATE`, `ALTER` and `DROP`
//!   targets, `LOCK TABLES`, `RENAME TABLE`, `GRANT ... ON`, `SHOW ...
//!   FROM`, `LOAD DATA ... INTO TABLE`, `NEXT VALUE FOR`, ...): in `a.b`,
//!   `a` is a database. `a.*` is a table alias (multi-table `DELETE`).
//! - **Expressions**: `a.b.c` and `a.b.*` name a database `a`; `a.b(...)`
//!   calls a stored function in database `a`; `a.b` is a column of table
//!   (or alias) `a` and is left alone.
//! - **Database positions** (`USE`, `CREATE`/`ALTER`/`DROP DATABASE`,
//!   `SHOW ... FROM db`, `GRANT ... ON db.*`): a single name.
//!
//! Every dotted name the walker reads is marked; after the walk, a dotted
//! name left unmarked (in a statement the walker does not understand)
//! fails the statement with 39051, so no qualified name is forwarded
//! unrewritten.

use sprig_common::names::{is_internal_column, is_internal_object, is_reserved_database, quote};

use super::subst::{Builtin, sql_string};
use super::{
    Edit, EditKind, Engine, RewriteCtx, StatementInfo, StmtKind, SystemRef, SystemSchema, UseTarget,
};
use crate::lex::{Token, TokenKind, ident_value, string_value};
use crate::names::ddl::{AlterOp, DdlKind, DdlName, DdlTarget};
use crate::names::map::Purpose;
use crate::names::{NameError, ObjectKind};

type R<T = ()> = Result<T, NameError>;

/// What the walk produced.
pub(super) struct Out {
    pub(super) edits: Vec<Edit>,
    pub(super) statements: Vec<StatementInfo>,
    /// The current logical database each statement started with, for the
    /// AST path's unqualified DDL names.
    pub(super) currents: Vec<Option<String>>,
}

/// Walks every statement in `src`.
pub(super) fn walk(src: &[u8], tokens: &[Token], ctx: &RewriteCtx<'_>, depth: u8) -> R<Out> {
    let t: Vec<Token> = tokens
        .iter()
        .copied()
        .filter(|t| !t.kind.is_trivia())
        .collect();
    let mut w = Walker {
        src,
        classified: vec![false; t.len()],
        t,
        i: 0,
        ctx,
        depth,
        edits: Vec::new(),
        statements: Vec::new(),
        body: 0,
        account: false,
        creating_cols: false,
        purpose: Purpose::Read,
        cur: Acc::default(),
        current: ctx.current_db.map(str::to_owned),
        nest: 0,
        currents: Vec::new(),
    };
    w.run()?;
    w.check_all_classified()?;
    let mut statements = w.statements;
    whole_exec_comments(tokens, &mut statements);
    Ok(Out {
        edits: w.edits,
        statements,
        currents: w.currents,
    })
}

/// Per-statement facts being gathered.
#[derive(Default)]
struct Acc {
    kind: Option<StmtKind>,
    use_target: Option<UseTarget>,
    database: Option<String>,
    system_refs: Vec<SystemRef>,
    ddl: Option<DdlTarget>,
    nested_ddl: bool,
}

/// What `CREATE` modifiers before the object kind said.
#[derive(Default, Clone, Copy)]
struct Modifiers {
    or_replace: bool,
    temporary: bool,
}

/// A dotted name: up to four identifier tokens, maybe ending in `.*`.
#[derive(Debug, Clone, Copy)]
struct Chain {
    parts: [usize; 4],
    n: usize,
    star: bool,
    long: bool,
    end: usize,
}

/// Keywords that end an expression at depth 0, and whether a comma does.
#[derive(Clone, Copy)]
struct Stop {
    words: &'static [&'static [u8]],
    comma: bool,
}

const QUERY_WORDS: &[&[u8]] = &[
    b"FROM",
    b"WHERE",
    b"GROUP",
    b"HAVING",
    b"ORDER",
    b"LIMIT",
    b"WINDOW",
    b"QUALIFY",
    b"UNION",
    b"EXCEPT",
    b"INTERSECT",
    b"INTO",
    b"FOR",
    b"LOCK",
    b"PROCEDURE",
    b"SELECT",
    b"TABLE",
    b"ON",
    b"RETURNING",
];
const JOIN_WORDS: &[&[u8]] = &[
    b"FROM",
    b"WHERE",
    b"GROUP",
    b"HAVING",
    b"ORDER",
    b"LIMIT",
    b"WINDOW",
    b"QUALIFY",
    b"UNION",
    b"EXCEPT",
    b"INTERSECT",
    b"INTO",
    b"FOR",
    b"LOCK",
    b"PROCEDURE",
    b"SELECT",
    b"TABLE",
    b"ON",
    b"RETURNING",
    b"JOIN",
    b"INNER",
    b"CROSS",
    b"LEFT",
    b"RIGHT",
    b"NATURAL",
    b"STRAIGHT_JOIN",
    b"FULL",
    b"OUTER",
    b"USING",
    b"SET",
    b"WITH",
    b"TO",
];
const JOIN_STARTS: &[&[u8]] = &[
    b"JOIN",
    b"INNER",
    b"CROSS",
    b"LEFT",
    b"RIGHT",
    b"NATURAL",
    b"STRAIGHT_JOIN",
    b"FULL",
    b"OUTER",
];

impl Stop {
    const NONE: Stop = Stop {
        words: &[],
        comma: false,
    };
    const QUERY: Stop = Stop {
        words: QUERY_WORDS,
        comma: false,
    };
    const JOIN: Stop = Stop {
        words: JOIN_WORDS,
        comma: true,
    };
    const fn words(words: &'static [&'static [u8]]) -> Stop {
        Stop {
            words,
            comma: false,
        }
    }
}

/// Words that may begin the body of a stored program.
const BODY_STARTS: &[&[u8]] = &[
    b"BEGIN",
    b"RETURN",
    b"SELECT",
    b"INSERT",
    b"UPDATE",
    b"DELETE",
    b"REPLACE",
    b"SET",
    b"IF",
    b"CASE",
    b"LOOP",
    b"WHILE",
    b"REPEAT",
    b"CALL",
    b"DO",
    b"WITH",
    b"TRUNCATE",
    b"CREATE",
    b"DROP",
    b"ALTER",
    b"DECLARE",
    b"LEAVE",
    b"ITERATE",
    b"OPEN",
    b"FETCH",
    b"CLOSE",
    b"SIGNAL",
    b"RESIGNAL",
    b"LOCK",
    b"UNLOCK",
    b"START",
    b"COMMIT",
    b"ROLLBACK",
    b"SAVEPOINT",
    b"RELEASE",
    b"PREPARE",
    b"EXECUTE",
    b"DEALLOCATE",
    b"VALUES",
    b"TABLE",
    b"LOAD",
    b"RENAME",
    b"ANALYZE",
    b"OPTIMIZE",
    b"REPAIR",
    b"CHECK",
    b"FLUSH",
    b"GRANT",
    b"REVOKE",
    b"KILL",
    b"SHOW",
    b"HANDLER",
    b"FOR",
    b"GET",
    b"USE",
];

/// Words that start a database option (`CREATE`/`ALTER DATABASE`).
const DB_OPTIONS: &[&[u8]] = &[
    b"DEFAULT",
    b"CHARACTER",
    b"CHAR",
    b"CHARSET",
    b"COLLATE",
    b"COMMENT",
    b"ENCRYPTION",
    b"READ",
    b"UPGRADE",
];

/// Ends of a statement list inside a compound statement.
const LIST_ENDS: &[&[u8]] = &[b"END", b"ELSE", b"ELSEIF", b"WHEN", b"UNTIL"];

struct Walker<'a, 'c> {
    src: &'a [u8],
    /// Significant tokens only.
    t: Vec<Token>,
    classified: Vec<bool>,
    i: usize,
    ctx: &'c RewriteCtx<'c>,
    depth: u8,
    edits: Vec<Edit>,
    statements: Vec<StatementInfo>,
    /// Inside the stored body of a routine, trigger, event or view: no
    /// substitutions.
    body: u32,
    /// In an account statement: no substitutions.
    account: bool,
    /// In a `CREATE`/`ALTER TABLE` body: `_sprig_` names are 39050.
    creating_cols: bool,
    purpose: Purpose,
    cur: Acc,
    /// The current logical database: the session's, until a `USE` earlier
    /// in the same text changes it.
    current: Option<String>,
    /// Recursion depth of the grammar functions, bounded so client input
    /// can't exhaust the stack (chapter 03, "Security conventions").
    nest: u32,
    /// The current database each statement started with.
    currents: Vec<Option<String>>,
}

/// Deepest nesting of subqueries, joins and compound statements accepted.
const MAX_NEST: u32 = 200;

impl Walker<'_, '_> {
    fn enter(&mut self) -> R {
        if self.nest >= MAX_NEST {
            return Err(NameError::Unsupported("statement nested too deeply".into()));
        }
        self.nest += 1;
        Ok(())
    }

    // ----- token helpers -----

    fn n(&self) -> usize {
        self.t.len()
    }

    fn bytes(&self, j: usize) -> &[u8] {
        self.t[j].bytes(self.src)
    }

    fn kind(&self, j: usize) -> Option<TokenKind> {
        self.t.get(j).map(|t| t.kind)
    }

    fn kw(&self, j: usize, w: &[u8]) -> bool {
        self.kind(j) == Some(TokenKind::Word) && self.bytes(j).eq_ignore_ascii_case(w)
    }

    fn kw_any(&self, j: usize, ws: &[&[u8]]) -> bool {
        self.kind(j) == Some(TokenKind::Word) && {
            let b = self.bytes(j);
            ws.iter().any(|w| b.eq_ignore_ascii_case(w))
        }
    }

    fn at(&self, w: &[u8]) -> bool {
        self.kw(self.i, w)
    }

    fn eat(&mut self, w: &[u8]) -> bool {
        let yes = self.at(w);
        if yes {
            self.i += 1;
        }
        yes
    }

    fn punct(&self, j: usize, c: u8) -> bool {
        self.kind(j) == Some(TokenKind::Punct) && self.src[self.t[j].start as usize] == c
    }

    fn op(&self, j: usize, s: &[u8]) -> bool {
        self.kind(j) == Some(TokenKind::Op) && self.bytes(j) == s
    }

    fn eat_punct(&mut self, c: u8) -> bool {
        let yes = self.punct(self.i, c);
        if yes {
            self.i += 1;
        }
        yes
    }

    fn ident(&self, j: usize) -> bool {
        self.kind(j).is_some_and(TokenKind::is_ident_like)
    }

    /// At `;` or the end of the input.
    fn at_end(&self) -> bool {
        self.i >= self.n() || self.punct(self.i, b';')
    }

    /// No trivia between tokens `a` and `a + 1`.
    fn adjacent(&self, a: usize) -> bool {
        a + 1 < self.n() && self.t[a].end == self.t[a + 1].start
    }

    fn eat_if_exists(&mut self) -> bool {
        let yes = self.at(b"IF") && self.kw(self.i + 1, b"EXISTS");
        if yes {
            self.i += 2;
        }
        yes
    }

    fn eat_if_not_exists(&mut self) -> bool {
        let yes = self.at(b"IF") && self.kw(self.i + 1, b"NOT") && self.kw(self.i + 2, b"EXISTS");
        if yes {
            self.i += 3;
        }
        yes
    }

    /// MariaDB's `WAIT n` or `NOWAIT` after a table name.
    fn skip_wait(&mut self) {
        if self.at(b"WAIT") && self.kind(self.i + 1) == Some(TokenKind::Number) {
            self.i += 2;
        } else {
            self.eat(b"NOWAIT");
        }
    }

    // ----- DDL targets -----

    /// The logical name a chain in an object position spells: `name` (in
    /// the current database) or `db.name`.
    fn ddl_name(&self, c: &Chain) -> Option<DdlName> {
        match (c.n, c.star, c.long) {
            (1, false, false) => Some(DdlName::unqualified(
                &self.value(c.parts[0]),
                self.current.as_deref(),
            )),
            (2, false, false) => Some(DdlName::qualified(
                &self.value(c.parts[0]),
                &self.value(c.parts[1]),
            )),
            _ => None,
        }
    }

    /// Records the statement's DDL target: at the top level it is the
    /// statement's; inside a compound statement the gateway can't route it
    /// through the catalog; inside a stored body it is a definition.
    fn set_ddl(&mut self, target: Option<DdlTarget>) {
        let Some(target) = target else {
            return;
        };
        if self.body > 0 {
            return;
        }
        if self.nest == 1 && self.depth == 0 {
            self.cur.ddl = Some(target);
        } else {
            self.cur.nested_ddl = true;
        }
    }

    /// `[DEFAULT] CHARACTER SET [=] x`, `[DEFAULT] COLLATE [=] x` and the
    /// other database options, to the end of the statement.
    fn db_options(&mut self) -> (Option<String>, Option<String>, Vec<String>) {
        let (mut charset, mut collation, mut other) = (None, None, Vec::new());
        while !self.at_end() {
            self.eat(b"DEFAULT");
            if self.at_end() {
                break;
            }
            let word = self.bytes(self.i).to_ascii_uppercase();
            let is_word = self.kind(self.i) == Some(TokenKind::Word);
            match word.as_slice() {
                b"CHARACTER" | b"CHAR" if is_word && self.kw(self.i + 1, b"SET") => {
                    self.i += 2;
                    charset = self.option_value();
                }
                b"CHARSET" if is_word => {
                    self.i += 1;
                    charset = self.option_value();
                }
                b"COLLATE" if is_word => {
                    self.i += 1;
                    collation = self.option_value();
                }
                b"ENCRYPTION" if is_word => {
                    self.i += 1;
                    let v = self.option_value();
                    if !v.is_some_and(|v| v.eq_ignore_ascii_case("n")) {
                        other.push("ENCRYPTION".to_owned());
                    }
                }
                _ if is_word => {
                    // COMMENT, READ ONLY, UPGRADE DATA DIRECTORY NAME, ...
                    other.push(String::from_utf8_lossy(&word).into_owned());
                    self.i += 1;
                    while !self.at_end() && !self.kw_any(self.i, DB_OPTIONS) {
                        self.i += 1;
                    }
                }
                _ => self.i += 1,
            }
        }
        (charset, collation, other)
    }

    /// `[=] value` of a database option: a word, quoted identifier or
    /// string, lowercased.
    fn option_value(&mut self) -> Option<String> {
        if self.op(self.i, b"=") {
            self.i += 1;
        }
        let j = self.i;
        let v = match self.kind(j)? {
            TokenKind::Word | TokenKind::QuotedIdent => self.value(j),
            TokenKind::String => {
                let raw = string_value(self.bytes(j), self.ctx.lex.no_backslash_escapes)?;
                String::from_utf8_lossy(&raw).into_owned()
            }
            _ => return None,
        };
        self.i += 1;
        Some(v.to_ascii_lowercase())
    }

    /// `(` followed by a query.
    fn subquery_at(&self, j: usize) -> bool {
        self.kw_any(j, &[b"SELECT", b"WITH", b"TABLE"])
    }

    fn value(&self, j: usize) -> String {
        String::from_utf8_lossy(&ident_value(self.bytes(j))).into_owned()
    }

    fn no_subst(&self) -> bool {
        self.body > 0 || self.account
    }

    // ----- names -----

    fn chain_at(&self, j: usize) -> Option<Chain> {
        if !self.ident(j) {
            return None;
        }
        let mut c = Chain {
            parts: [j, 0, 0, 0],
            n: 1,
            star: false,
            long: false,
            end: j + 1,
        };
        let mut k = j + 1;
        while self.punct(k, b'.') {
            if self.ident(k + 1) {
                if c.n == 4 {
                    c.long = true;
                } else {
                    c.parts[c.n] = k + 1;
                    c.n += 1;
                }
                k += 2;
            } else if self.op(k + 1, b"*") {
                c.star = true;
                k += 2;
                break;
            } else {
                break;
            }
        }
        c.end = k;
        Some(c)
    }

    fn mark(&mut self, c: &Chain) {
        for k in c.parts[0]..c.end {
            self.classified[k] = true;
        }
    }

    /// Maps the database token `tok`; `object` is the token after it, for
    /// system-schema references. Returns the logical name, or `None` for a
    /// system schema.
    fn schema_part(&mut self, tok: usize, object: Option<usize>) -> R<Option<String>> {
        let raw = ident_value(self.bytes(tok));
        let Ok(name) = std::str::from_utf8(&raw) else {
            return Err(NameError::UnknownDatabase(
                String::from_utf8_lossy(&raw).into_owned(),
            ));
        };
        let name = name.to_owned();
        self.map_schema(tok, &name, object)
    }

    /// Maps database `name`, spelled by token `tok`.
    fn map_schema(&mut self, tok: usize, name: &str, object: Option<usize>) -> R<Option<String>> {
        let range = self.t[tok].range();
        if let Some(schema) = SystemSchema::parse(name) {
            let object = object.map(|o| self.value(o));
            self.cur.system_refs.push(SystemRef {
                schema,
                object,
                range,
            });
            return Ok(None);
        }
        let key = if self.ctx.lower_case_table_names {
            name.to_lowercase()
        } else {
            name.to_owned()
        };
        let set = self.ctx.map.physical(&key, self.purpose)?;
        // Metadata lists one schema per table group; a name in a statement
        // is the database's own (first) schema, and `grouped_table` moves
        // a grouped table's to its group's.
        let one = if self.purpose == Purpose::Metadata {
            set.0.first().copied()
        } else {
            set.single()
        };
        let schema = one.ok_or_else(|| {
            NameError::Unsupported(format!(
                "database '{key}' stands for several schemas in one statement"
            ))
        })?;
        self.edits.push(Edit {
            range,
            kind: EditKind::Schema {
                logical: key.clone(),
                schema,
            },
            replacement: quote(&schema.to_string()),
        });
        Ok(Some(key))
    }

    /// Chapter 07 (R1): a metadata statement about one table (`SHOW CREATE
    /// TABLE`, `SHOW COLUMNS`, `SHOW INDEX`, `DESCRIBE`) reads the table
    /// where it is. The advisor moves a grouped table out of its
    /// database's first shard into its group's schemas, so its name gets
    /// the group's representative schema: the edit of its database part
    /// (`db`, that part's range) changes, or a bare name is qualified.
    fn grouped_table(&mut self, table_tok: usize, db: Option<&std::ops::Range<usize>>) {
        if self.no_subst() {
            return;
        }
        let at = match db {
            Some(r) => match self.edits.iter().position(|e| e.range == *r) {
                Some(i) => Some(i),
                // A system schema or a name left alone.
                None => return,
            },
            None => None,
        };
        let logical = match at {
            Some(i) => match &self.edits[i].kind {
                EditKind::Schema { logical, .. } => logical.clone(),
                _ => return,
            },
            None => match &self.current {
                Some(c) if self.ctx.lower_case_table_names => c.to_lowercase(),
                Some(c) => c.clone(),
                None => return,
            },
        };
        let Some(dbid) = self.ctx.map.database(&logical).map(|d| d.id) else {
            return;
        };
        let table = self.value(table_tok);
        let Some(schema) = self.ctx.map.table_schema(dbid, &table) else {
            return;
        };
        let replacement = quote(&schema.to_string());
        if let Some(i) = at {
            let e = &mut self.edits[i];
            e.kind = EditKind::Schema { logical, schema };
            e.replacement = replacement;
        } else {
            let start = self.t[table_tok].range().start;
            let pos = self.edits.partition_point(|e| e.range.end <= start);
            self.edits.insert(
                pos,
                Edit {
                    range: start..start,
                    kind: EditKind::Schema { logical, schema },
                    replacement: format!("{replacement}."),
                },
            );
        }
    }

    /// [`Self::grouped_table`] for the name `c` (one or two parts).
    fn grouped_chain(&mut self, c: Option<Chain>) {
        match c {
            Some(c) if c.n == 1 && !c.star => self.grouped_table(c.parts[0], None),
            Some(c) if c.n == 2 => {
                let r = self.t[c.parts[0]].range();
                self.grouped_table(c.parts[1], Some(&r));
            }
            _ => {}
        }
    }

    /// Fails for a reserved object name.
    fn check_object(&self, tok: usize, kind: ObjectKind, db: Option<&str>, creating: bool) -> R {
        if self.ctx.internal {
            return Ok(());
        }
        let raw = ident_value(self.bytes(tok));
        let name = String::from_utf8_lossy(&raw);
        if !is_internal_object(&name) {
            return Ok(());
        }
        if creating {
            return Err(NameError::Reserved(name.into_owned()));
        }
        Err(NameError::Missing {
            kind,
            db: db.or(self.current.as_deref()).map(str::to_owned),
            name: name.into_owned(),
        })
    }

    /// Fails for a reserved column name.
    fn check_column(&self, tok: usize) -> R {
        if self.ctx.internal || !self.ident(tok) {
            return Ok(());
        }
        let raw = ident_value(self.bytes(tok));
        let name = String::from_utf8_lossy(&raw);
        if !is_internal_column(&name) {
            return Ok(());
        }
        if self.creating_cols {
            Err(NameError::Reserved(name.into_owned()))
        } else {
            Err(NameError::UnknownColumn(name.into_owned()))
        }
    }

    /// A name in an object position.
    fn object(&mut self, kind: ObjectKind, creating: bool) -> R<Option<Chain>> {
        let Some(c) = self.chain_at(self.i) else {
            return Ok(None);
        };
        self.mark(&c);
        self.i = c.end;
        if c.long {
            return Err(NameError::Unsupported(
                "a name with more than four parts".into(),
            ));
        }
        match (c.n, c.star) {
            (1, false) => self.check_object(c.parts[0], kind, None, creating)?,
            (1, true) => {}
            (2, _) => {
                let db = self.schema_part(c.parts[0], Some(c.parts[1]))?;
                self.check_object(c.parts[1], kind, db.as_deref(), creating)?;
            }
            _ => {
                return Err(NameError::Unsupported(format!(
                    "a {}-part name in a {} position",
                    c.n,
                    kind.as_str().to_ascii_lowercase()
                )));
            }
        }
        Ok(Some(c))
    }

    fn object_list(&mut self, kind: ObjectKind, creating: bool) -> R {
        loop {
            self.object(kind, creating)?;
            if !self.eat_punct(b',') {
                return Ok(());
            }
        }
    }

    /// A single database name in a database position.
    fn database(&mut self) -> R<Option<String>> {
        if !self.ident(self.i) {
            return Ok(None);
        }
        let tok = self.i;
        if let Some(c) = self.chain_at(tok) {
            self.mark(&c);
        }
        self.i += 1;
        let name = self.schema_part(tok, None)?;
        Ok(name)
    }

    // ----- expressions -----

    fn is_stop(&self, j: usize, stop: Stop) -> bool {
        if !self.kw_any(j, stop.words) {
            return false;
        }
        // LEFT( and RIGHT( are functions, not joins.
        !(self.kw_any(j, &[b"LEFT", b"RIGHT"]) && self.punct(j + 1, b'('))
    }

    fn expr(&mut self, stop: Stop) -> R {
        let mut depth = 0u32;
        let mut case_depth = 0u32;
        while self.i < self.n() {
            match self.t[self.i].kind {
                TokenKind::Punct => match self.src[self.t[self.i].start as usize] {
                    b';' => return Ok(()),
                    b'(' => {
                        if self.subquery_at(self.i + 1) {
                            self.i += 1;
                            self.query()?;
                            self.eat_punct(b')');
                        } else {
                            depth += 1;
                            self.i += 1;
                        }
                    }
                    b')' => {
                        if depth == 0 {
                            return Ok(());
                        }
                        depth -= 1;
                        self.i += 1;
                    }
                    b',' if depth == 0 && stop.comma => return Ok(()),
                    _ => self.i += 1,
                },
                TokenKind::Word => {
                    if self.at(b"CASE") {
                        case_depth += 1;
                        self.i += 1;
                    } else if case_depth > 0 && self.at(b"END") {
                        case_depth -= 1;
                        self.i += 1;
                    } else if depth == 0 && case_depth == 0 && self.is_stop(self.i, stop) {
                        return Ok(());
                    } else {
                        self.expr_word()?;
                    }
                }
                TokenKind::QuotedIdent => self.expr_word()?,
                _ => self.i += 1,
            }
        }
        Ok(())
    }

    /// `(`, then an expression list up to the matching `)`, consumed.
    fn parens(&mut self) -> R {
        if self.eat_punct(b'(') {
            self.expr(Stop::NONE)?;
            self.eat_punct(b')');
        }
        Ok(())
    }

    /// Whether a call's `(` is at `k` for a function name ending at `k-1`.
    fn call_at(&self, k: usize, strict: bool) -> bool {
        self.punct(k, b'(') && (!strict || self.ctx.ignore_space || self.adjacent(k - 1))
    }

    fn expr_word(&mut self) -> R {
        let j = self.i;
        if let Some(c) = self.chain_at(j)
            && (c.n > 1 || c.star)
        {
            self.mark(&c);
            self.i = c.end;
            if c.long {
                return Err(NameError::Unsupported(
                    "a name with more than four parts".into(),
                ));
            }
            if c.n >= 3 || (c.n == 2 && c.star) {
                // db.t.c, db.t.*
                let db = self.schema_part(c.parts[0], Some(c.parts[1]))?;
                self.check_object(c.parts[1], ObjectKind::Table, db.as_deref(), false)?;
                if c.n == 3 && !c.star {
                    self.check_column(c.parts[2])?;
                }
            } else if c.star {
                // alias.*
            } else if self.call_at(c.end, false) {
                let db = self.schema_part(c.parts[0], Some(c.parts[1]))?;
                self.check_object(c.parts[1], ObjectKind::Function, db.as_deref(), false)?;
            } else {
                self.check_column(c.parts[1])?;
            }
            return Ok(());
        }
        if self.t[j].kind == TokenKind::Word {
            let word = self.bytes(j);
            if self.call_at(j + 1, true)
                && let Some(b) = Builtin::parse(word)
            {
                return self.builtin(b);
            }
            if self.call_at(j + 1, false) {
                self.check_object(j, ObjectKind::Function, None, false)?;
                self.i += 1;
                return Ok(());
            }
            if word.eq_ignore_ascii_case(b"CURRENT_USER") {
                self.refuse_in_table(j)?;
                self.substitute_account(j, j, EditKind::CurrentUser);
                self.i += 1;
                return Ok(());
            }
            if self.kw_any(j, &[b"NEXT", b"PREVIOUS"])
                && self.kw(j + 1, b"VALUE")
                && self.kw(j + 2, b"FOR")
            {
                self.i = j + 3;
                self.object(ObjectKind::Sequence, false)?;
                return Ok(());
            }
        }
        self.check_column(j)?;
        self.i += 1;
        Ok(())
    }

    fn substitute(&mut self, first: usize, last: usize, kind: EditKind, replacement: String) {
        self.edits.push(Edit {
            range: self.t[first].start as usize..self.t[last].end as usize,
            kind,
            replacement,
        });
    }

    fn substitute_account(&mut self, first: usize, last: usize, kind: EditKind) {
        if self.no_subst() {
            return;
        }
        let Some(a) = self.ctx.account else { return };
        let host = if kind == EditKind::CurrentUser {
            a.account_host
        } else {
            a.client_host
        };
        let lit = sql_string(
            &format!("{}@{host}", a.user),
            self.ctx.lex.no_backslash_escapes,
        );
        self.substitute(first, last, kind, lit);
    }

    /// Chapter 07 step 12: an account function or `DATABASE()` in a
    /// table's definition (a column `DEFAULT`, a generated column, a
    /// `CHECK`) would run inside MariaDB on every row, as the backend
    /// account in the physical schema, and store or show backend names.
    /// MariaDB allows no subquery there, so no lookup can replace it: 39051.
    fn refuse_in_table(&self, j: usize) -> R {
        if self.creating_cols && !self.ctx.internal {
            return Err(NameError::Unsupported(format!(
                "{}() in a table definition (a column default, generated column or check); \
                 set the value in the INSERT or with a trigger",
                String::from_utf8_lossy(self.bytes(j)).to_ascii_uppercase()
            )));
        }
        Ok(())
    }

    /// At a built-in's name, with `(` next.
    fn builtin(&mut self, b: Builtin) -> R {
        let j = self.i;
        let empty_args = self.punct(j + 2, b')');
        if empty_args && matches!(b, Builtin::Database | Builtin::CurrentUser | Builtin::User) {
            self.refuse_in_table(j)?;
        }
        match b {
            Builtin::Database if empty_args => {
                if !self.no_subst() {
                    let lit = match self.current.as_deref() {
                        Some(db) => sql_string(db, self.ctx.lex.no_backslash_escapes),
                        None => "NULL".to_owned(),
                    };
                    self.substitute(j, j + 2, EditKind::Database, lit);
                }
                self.i = j + 3;
            }
            Builtin::CurrentUser if empty_args => {
                self.substitute_account(j, j + 2, EditKind::CurrentUser);
                self.i = j + 3;
            }
            Builtin::User if empty_args => {
                self.substitute_account(j, j + 2, EditKind::User);
                self.i = j + 3;
            }
            Builtin::QueryAttribute
                if self.ctx.lex.persona == sprig_proto_mysql::packets::Persona::MySql84
                    && !self.no_subst() =>
            {
                let lit = (self.kind(j + 2) == Some(TokenKind::String) && self.punct(j + 3, b')'))
                    .then(|| string_value(self.bytes(j + 2), self.ctx.lex.no_backslash_escapes))
                    .flatten()
                    .ok_or_else(|| {
                        NameError::Unsupported(
                            "mysql_query_attribute_string() needs a string literal".into(),
                        )
                    })?;
                let name = String::from_utf8_lossy(&lit);
                let value = self
                    .ctx
                    .query_attributes
                    .iter()
                    .find(|(n, _)| *n == name)
                    .and_then(|(_, v)| *v);
                let rep = match value {
                    Some(v) => sql_string(v, self.ctx.lex.no_backslash_escapes),
                    None => "NULL".to_owned(),
                };
                self.substitute(j, j + 3, EditKind::QueryAttribute, rep);
                self.i = j + 4;
            }
            Builtin::SequenceFn => {
                self.i = j + 2;
                self.object(ObjectKind::Sequence, false)?;
                self.expr(Stop::NONE)?;
                self.eat_punct(b')');
            }
            _ => self.i += 1,
        }
        Ok(())
    }

    // ----- queries -----

    fn with_clause(&mut self) -> R {
        self.i += 1; // WITH
        self.eat(b"RECURSIVE");
        loop {
            if self.ident(self.i) {
                self.i += 1;
            }
            if self.punct(self.i, b'(') {
                self.parens()?;
            }
            self.eat(b"AS");
            if self.eat_punct(b'(') {
                self.query()?;
                self.eat_punct(b')');
            }
            if !self.eat_punct(b',') {
                return Ok(());
            }
        }
    }

    fn query(&mut self) -> R {
        self.enter()?;
        let r = self.query_inner();
        self.nest -= 1;
        r
    }

    fn query_inner(&mut self) -> R {
        if self.at(b"WITH") {
            self.with_clause()?;
        }
        while !self.at_end() && !self.punct(self.i, b')') {
            let before = self.i;
            if self.at(b"FROM") {
                self.i += 1;
                self.table_refs()?;
            } else if self.at(b"TABLE") {
                self.i += 1;
                self.object(ObjectKind::Table, false)?;
            } else if self.at(b"WITH") {
                if self.kw_any(self.i + 1, &[b"CHECK", b"CASCADED", b"LOCAL", b"ROLLUP"]) {
                    self.i += 1;
                    self.expr(Stop::QUERY)?;
                } else {
                    self.with_clause()?;
                }
            } else if self.at(b"FOR") && self.kw_any(self.i + 1, &[b"UPDATE", b"SHARE"]) {
                self.i += 2;
                if self.eat(b"OF") {
                    self.object_list(ObjectKind::Table, false)?;
                }
            } else if self.punct(self.i, b'(') && self.subquery_at(self.i + 1) {
                self.i += 1;
                self.query()?;
                self.eat_punct(b')');
            } else if self.kw_any(self.i, QUERY_WORDS) {
                self.i += 1;
                self.expr(Stop::QUERY)?;
            } else {
                self.expr(Stop::QUERY)?;
            }
            if self.i == before {
                self.i += 1;
            }
        }
        Ok(())
    }

    fn table_refs(&mut self) -> R {
        self.enter()?;
        let r = self.table_refs_inner();
        self.nest -= 1;
        r
    }

    fn table_refs_inner(&mut self) -> R {
        loop {
            self.table_factor()?;
            loop {
                if self.kw_any(self.i, JOIN_STARTS) && !self.punct(self.i + 1, b'(') {
                    while self.kw_any(self.i, JOIN_STARTS) {
                        self.i += 1;
                    }
                    self.table_factor()?;
                } else if self.at(b"ON") || (self.at(b"USING") && self.punct(self.i + 1, b'(')) {
                    self.i += 1;
                    self.expr(Stop::JOIN)?;
                } else {
                    break;
                }
            }
            if !self.eat_punct(b',') {
                return Ok(());
            }
        }
    }

    fn table_factor(&mut self) -> R {
        if self.punct(self.i, b'(') {
            self.i += 1;
            if self.subquery_at(self.i) {
                self.query()?;
            } else {
                self.table_refs()?;
            }
            self.eat_punct(b')');
        } else if self.at(b"LATERAL") {
            self.i += 1;
            if self.eat_punct(b'(') {
                self.query()?;
                self.eat_punct(b')');
            }
        } else if self.kw_any(self.i, &[b"JSON_TABLE", b"SEQUENCE"]) && self.punct(self.i + 1, b'(')
        {
            self.i += 1;
            self.parens()?;
        } else {
            self.eat(b"ONLY");
            self.object(ObjectKind::Table, false)?;
        }
        self.table_tail()
    }

    fn table_tail(&mut self) -> R {
        loop {
            if self.at_end() || self.punct(self.i, b')') || self.punct(self.i, b',') {
                return Ok(());
            }
            if self.at(b"PARTITION") {
                self.i += 1;
                self.parens()?;
            } else if self.at(b"FOR") && self.kw_any(self.i + 1, &[b"SYSTEM_TIME", b"PORTION"]) {
                self.i += 2;
                if self.at(b"OF") {
                    self.i += 2; // FOR PORTION OF period
                }
                if self.eat(b"FROM") {
                    self.expr(Stop::JOIN)?;
                    if self.eat(b"TO") {
                        self.expr(Stop::JOIN)?;
                    }
                } else {
                    self.expr(Stop::JOIN)?;
                }
            } else if self.kw_any(self.i, &[b"USE", b"IGNORE", b"FORCE"])
                && self.kw_any(self.i + 1, &[b"INDEX", b"KEY"])
            {
                self.i += 2;
                if self.eat(b"FOR") {
                    self.i += 1;
                    self.eat(b"BY");
                }
                self.parens()?;
            } else if self.at(b"AS") {
                self.i += 1;
                if self.ident(self.i) {
                    self.i += 1;
                }
                if self.punct(self.i, b'(') {
                    self.parens()?;
                }
            } else if self.ident(self.i) && !self.is_stop(self.i, Stop::JOIN) {
                // An alias without AS, maybe with a column list.
                self.i += 1;
                if self.punct(self.i, b'(') {
                    self.parens()?;
                }
            } else {
                return Ok(());
            }
        }
    }

    // ----- statements -----

    fn run(&mut self) -> R {
        while self.i < self.n() {
            if self.eat_punct(b';') {
                continue;
            }
            let first = self.i;
            self.cur = Acc::default();
            self.body = 0;
            self.account = false;
            self.creating_cols = false;
            let current = self.current.clone();
            let kind = self.statement()?;
            // Whatever the grammar above did not consume, as expressions.
            while !self.at_end() {
                let before = self.i;
                self.expr(Stop::NONE)?;
                if self.i == before {
                    self.i += 1;
                }
            }
            let last = self.i.min(self.n()) - 1;
            let acc = std::mem::take(&mut self.cur);
            self.statements.push(StatementInfo {
                range: self.t[first].start as usize..self.t[last].end as usize,
                kind: acc.kind.unwrap_or(kind),
                use_target: acc.use_target,
                database: acc.database,
                system_refs: acc.system_refs,
                ddl: acc.ddl,
                nested_ddl: acc.nested_ddl,
                engine: Engine::Lexer,
                engines_agree: true,
            });
            self.currents.push(current);
        }
        Ok(())
    }

    /// Skips to the end of the statement, reading nothing.
    fn opaque(&mut self) {
        while !self.at_end() {
            self.i += 1;
        }
    }

    fn statement(&mut self) -> R<StmtKind> {
        self.enter()?;
        let r = self.statement_inner();
        self.nest -= 1;
        r
    }

    #[allow(clippy::too_many_lines)]
    fn statement_inner(&mut self) -> R<StmtKind> {
        let w = self.i;
        if self.punct(w, b'(') {
            self.purpose = Purpose::Read;
            self.query()?;
            return Ok(StmtKind::Select);
        }
        if self.kind(w) != Some(TokenKind::Word) {
            self.opaque();
            return Ok(StmtKind::Other);
        }
        let word = self.bytes(w).to_ascii_uppercase();
        match word.as_slice() {
            b"SELECT" | b"TABLE" | b"VALUES" => {
                self.purpose = Purpose::Read;
                self.query()?;
                Ok(StmtKind::Select)
            }
            b"WITH" => {
                self.purpose = Purpose::Read;
                self.with_clause()?;
                if self.kw_any(self.i, &[b"UPDATE", b"DELETE", b"INSERT", b"REPLACE"]) {
                    self.statement()
                } else {
                    self.query()?;
                    Ok(StmtKind::Select)
                }
            }
            b"INSERT" | b"REPLACE" => {
                self.purpose = Purpose::Write;
                self.i += 1;
                while self.kw_any(
                    self.i,
                    &[b"LOW_PRIORITY", b"DELAYED", b"HIGH_PRIORITY", b"IGNORE"],
                ) {
                    self.i += 1;
                }
                self.eat(b"INTO");
                self.object(ObjectKind::Table, false)?;
                self.query()?;
                Ok(if word == b"INSERT" {
                    StmtKind::Insert
                } else {
                    StmtKind::Replace
                })
            }
            b"UPDATE" => {
                self.purpose = Purpose::Write;
                self.i += 1;
                while self.kw_any(self.i, &[b"LOW_PRIORITY", b"IGNORE"]) {
                    self.i += 1;
                }
                self.table_refs()?;
                self.query()?;
                Ok(StmtKind::Update)
            }
            b"DELETE" => {
                self.purpose = Purpose::Write;
                self.i += 1;
                while self.kw_any(self.i, &[b"LOW_PRIORITY", b"QUICK", b"IGNORE"]) {
                    self.i += 1;
                }
                if self.eat(b"FROM") {
                    self.table_refs()?;
                    if self.eat(b"USING") {
                        self.table_refs()?;
                    }
                } else {
                    self.table_refs()?;
                    if self.eat(b"FROM") {
                        self.table_refs()?;
                    }
                }
                self.query()?;
                Ok(StmtKind::Delete)
            }
            b"USE" => {
                self.purpose = Purpose::Session;
                self.i += 1;
                if self.ident(self.i) {
                    let tok = self.i;
                    let name = self.database()?;
                    self.cur.use_target = Some(match name {
                        Some(logical) => {
                            let schema = self
                                .edits
                                .iter()
                                .rev()
                                .find_map(|e| match &e.kind {
                                    EditKind::Schema { schema, .. } => Some(*schema),
                                    _ => None,
                                })
                                .expect("database() pushed an edit");
                            self.current = Some(logical.clone());
                            UseTarget::Database { logical, schema }
                        }
                        None => UseTarget::System(
                            SystemSchema::parse(&self.value(tok)).expect("system schema"),
                        ),
                    });
                }
                Ok(StmtKind::Use)
            }
            b"CREATE" => self.create(),
            b"ALTER" => self.alter(),
            b"DROP" => self.drop(),
            b"RENAME" => {
                self.purpose = Purpose::Ddl;
                self.i += 1;
                if self.eat(b"TABLE") || self.eat(b"TABLES") {
                    let if_exists = self.eat_if_exists();
                    let mut pairs = Some(Vec::new());
                    loop {
                        let from = self.object(ObjectKind::Table, false)?;
                        self.skip_wait();
                        let to = if self.eat(b"TO") {
                            self.object(ObjectKind::Table, true)?
                        } else {
                            None
                        };
                        let pair = from
                            .zip(to)
                            .and_then(|(a, b)| self.ddl_name(&a).zip(self.ddl_name(&b)));
                        match (&mut pairs, pair) {
                            (Some(ps), Some(p)) => ps.push(p),
                            _ => pairs = None,
                        }
                        if !self.eat_punct(b',') {
                            break;
                        }
                    }
                    let target = pairs
                        .filter(|p| !p.is_empty() && self.at_end())
                        .map(|pairs| DdlTarget::RenameTables { pairs, if_exists });
                    self.set_ddl(target);
                    Ok(StmtKind::Ddl)
                } else {
                    self.account = true;
                    self.opaque();
                    Ok(StmtKind::Account)
                }
            }
            b"TRUNCATE" => {
                self.purpose = Purpose::Ddl;
                self.i += 1;
                self.eat(b"TABLE");
                let c = self.object(ObjectKind::Table, false)?;
                self.skip_wait();
                let target = c
                    .filter(|_| self.at_end())
                    .and_then(|c| self.ddl_name(&c))
                    .map(|name| DdlTarget::Truncate { name });
                self.set_ddl(target);
                Ok(StmtKind::Ddl)
            }
            b"GRANT" | b"REVOKE" => self.grant(),
            b"LOCK" => {
                self.purpose = Purpose::Write;
                self.i += 1;
                if self.eat(b"TABLES") || self.eat(b"TABLE") {
                    loop {
                        self.object(ObjectKind::Table, false)?;
                        while !self.at_end() && !self.punct(self.i, b',') {
                            self.i += 1; // [AS] alias, READ [LOCAL], [LOW_PRIORITY] WRITE ...
                        }
                        if !self.eat_punct(b',') {
                            break;
                        }
                    }
                }
                self.opaque();
                Ok(StmtKind::Lock)
            }
            b"UNLOCK" => {
                self.opaque();
                Ok(StmtKind::Lock)
            }
            b"SHOW" => self.show(),
            b"DESCRIBE" | b"DESC" | b"EXPLAIN" => self.describe(),
            b"CALL" => {
                self.purpose = Purpose::Write;
                self.i += 1;
                self.object(ObjectKind::Procedure, false)?;
                self.parens()?;
                Ok(StmtKind::Call)
            }
            b"LOAD" => {
                self.purpose = Purpose::Write;
                self.i += 1;
                if self.at(b"INDEX") {
                    // LOAD INDEX INTO CACHE t1 [INDEX (..)], t2 ...
                    while !self.at_end() && !self.at(b"CACHE") {
                        self.i += 1;
                    }
                    self.eat(b"CACHE");
                    self.index_cache_list()?;
                    return Ok(StmtKind::Other);
                }
                while !(self.at_end() || self.at(b"INTO") && self.kw(self.i + 1, b"TABLE")) {
                    self.i += 1;
                }
                if !self.at_end() {
                    self.i += 2;
                    self.object(ObjectKind::Table, false)?;
                }
                Ok(StmtKind::LoadData)
            }
            b"HANDLER" => {
                self.purpose = Purpose::Read;
                self.i += 1;
                self.object(ObjectKind::Table, false)?;
                self.opaque();
                Ok(StmtKind::Other)
            }
            b"FLUSH" => {
                self.i += 1;
                while self.kw_any(self.i, &[b"NO_WRITE_TO_BINLOG", b"LOCAL"]) {
                    self.i += 1;
                }
                if (self.eat(b"TABLES") || self.eat(b"TABLE"))
                    && self.ident(self.i)
                    && !self.kw_any(self.i, &[b"WITH", b"FOR"])
                {
                    self.object_list(ObjectKind::Table, false)?;
                }
                self.opaque();
                Ok(StmtKind::Other)
            }
            b"ANALYZE" | b"CHECK" | b"CHECKSUM" | b"OPTIMIZE" | b"REPAIR" => {
                self.purpose = Purpose::Ddl;
                self.i += 1;
                while self.kw_any(self.i, &[b"NO_WRITE_TO_BINLOG", b"LOCAL"]) {
                    self.i += 1;
                }
                if self.eat(b"TABLE") || self.eat(b"TABLES") || self.eat(b"VIEW") {
                    self.object_list(ObjectKind::Table, false)?;
                    self.opaque();
                    Ok(StmtKind::Other)
                } else if self.at(b"FORMAT")
                    || self.subquery_at(self.i)
                    || self.kw_any(self.i, &[b"INSERT", b"UPDATE", b"DELETE", b"REPLACE"])
                {
                    // MariaDB's ANALYZE [FORMAT=JSON] statement.
                    if self.eat(b"FORMAT") {
                        self.i += 2;
                    }
                    self.statement()
                } else {
                    self.opaque();
                    Ok(StmtKind::Other)
                }
            }
            b"CACHE" => {
                self.i += 1;
                self.eat(b"INDEX");
                self.index_cache_list()?;
                self.opaque();
                Ok(StmtKind::Other)
            }
            b"SET" => self.set(),
            b"DO" => {
                self.purpose = Purpose::Read;
                self.i += 1;
                self.expr(Stop::NONE)?;
                Ok(StmtKind::Other)
            }
            b"PREPARE" => {
                self.i += 1;
                if self.ident(self.i) {
                    self.i += 1;
                }
                if self.eat(b"FROM") {
                    self.embedded()?;
                }
                self.opaque();
                Ok(StmtKind::Prepare)
            }
            b"EXECUTE" => {
                self.i += 1;
                if self.eat(b"IMMEDIATE") {
                    self.embedded()?;
                }
                self.opaque();
                Ok(StmtKind::Prepare)
            }
            b"DEALLOCATE" => {
                self.opaque();
                Ok(StmtKind::Prepare)
            }
            b"BEGIN" if self.kw(w + 1, b"NOT") && self.kw(w + 2, b"ATOMIC") => {
                self.purpose = Purpose::Write;
                self.body_statement()?;
                Ok(StmtKind::Compound)
            }
            _ => {
                self.opaque();
                Ok(StmtKind::Other)
            }
        }
    }

    /// `t1 [INDEX|KEY (..)] [PARTITION (..)], t2 ...` of `CACHE INDEX` and
    /// `LOAD INDEX INTO CACHE`, up to `IN` or the end.
    fn index_cache_list(&mut self) -> R {
        loop {
            self.object(ObjectKind::Table, false)?;
            while !self.at_end() && !self.punct(self.i, b',') && !self.at(b"IN") {
                if self.punct(self.i, b'(') {
                    self.parens()?;
                } else {
                    self.i += 1;
                }
            }
            if !self.eat_punct(b',') {
                return Ok(());
            }
        }
    }

    /// The SQL text of `PREPARE ... FROM` or `EXECUTE IMMEDIATE`: a string
    /// literal is rewritten as a statement of its own; anything else (a
    /// user variable, an expression) runs where the gateway can't see it.
    fn embedded(&mut self) -> R {
        let j = self.i;
        if !matches!(
            self.kind(j),
            Some(TokenKind::String | TokenKind::NationalString)
        ) {
            return Ok(());
        }
        self.i += 1;
        if self.depth >= 2 {
            return Err(NameError::Unsupported("nested embedded SQL".into()));
        }
        let tok = self.bytes(j);
        let (prefix, lit) = if tok[0] == b'\'' || tok[0] == b'"' {
            (0, tok)
        } else {
            (1, &tok[1..])
        };
        let nbe = self.ctx.lex.no_backslash_escapes;
        let Some(inner) = string_value(lit, nbe) else {
            return Ok(());
        };
        let out = super::rewrite_depth(&inner, self.ctx, self.depth + 1)?;
        for s in out.statements {
            self.cur.nested_ddl |= s.nested_ddl || s.ddl.is_some();
            self.cur
                .system_refs
                .extend(s.system_refs.into_iter().map(|mut r| {
                    r.range = self.t[j].range();
                    r
                }));
        }
        if out.edits.is_empty() {
            return Ok(());
        }
        let text = String::from_utf8(out.sql)
            .map_err(|_| NameError::Unsupported("embedded SQL that is not valid UTF-8".into()))?;
        let start = self.t[j].start as usize + prefix;
        self.edits.push(Edit {
            range: start..self.t[j].end as usize,
            kind: EditKind::EmbeddedSql,
            replacement: sql_string(&text, nbe),
        });
        Ok(())
    }

    /// Skips `DEFINER = user`, `ALGORITHM = x`, `SQL SECURITY x` and the
    /// like before a `CREATE` or `ALTER` target, noting `OR REPLACE` and
    /// `TEMPORARY`.
    fn skip_create_modifiers(&mut self) -> Modifiers {
        let mut m = Modifiers::default();
        loop {
            if self.at(b"DEFINER") {
                self.i += 1;
                if self.op(self.i, b"=") {
                    self.i += 1;
                }
                if self.at(b"CURRENT_USER") {
                    self.i += 1;
                    if self.punct(self.i, b'(') && self.punct(self.i + 1, b')') {
                        self.i += 2;
                    }
                } else {
                    self.i += 1;
                    if self.kind(self.i) == Some(TokenKind::UserVar) {
                        let bare = self.bytes(self.i) == b"@";
                        self.i += if bare { 2 } else { 1 };
                    }
                }
            } else if self.at(b"ALGORITHM") {
                self.i += 1;
                if self.op(self.i, b"=") {
                    self.i += 1;
                }
                self.i += 1;
            } else if self.at(b"SQL") && self.kw(self.i + 1, b"SECURITY") {
                self.i += 3;
            } else if self.at(b"OR") && self.kw(self.i + 1, b"REPLACE") {
                m.or_replace = true;
                self.i += 2;
            } else if self.at(b"TEMPORARY") {
                m.temporary = true;
                self.i += 1;
            } else if self.kw_any(
                self.i,
                &[
                    b"OR",
                    b"REPLACE",
                    b"UNIQUE",
                    b"FULLTEXT",
                    b"SPATIAL",
                    b"AGGREGATE",
                    b"ONLINE",
                    b"OFFLINE",
                    b"IGNORE",
                ],
            ) {
                self.i += 1;
            } else {
                return m;
            }
        }
    }

    /// A `CREATE` target: the name, then the facts the caller adds.
    fn create_target(
        &self,
        kind: DdlKind,
        c: Option<Chain>,
        if_not_exists: bool,
        m: Modifiers,
    ) -> Option<DdlTarget> {
        let name = self.ddl_name(&c?)?;
        Some(DdlTarget::CreateObject {
            kind,
            name,
            if_not_exists,
            or_replace: m.or_replace,
            temporary: m.temporary,
            parent: None,
            like: None,
            as_select: false,
        })
    }

    #[allow(clippy::too_many_lines)]
    fn create(&mut self) -> R<StmtKind> {
        self.purpose = Purpose::Ddl;
        self.i += 1;
        let m = self.skip_create_modifiers();
        let Some(word) = (self.kind(self.i) == Some(TokenKind::Word))
            .then(|| self.bytes(self.i).to_ascii_uppercase())
        else {
            self.opaque();
            return Ok(StmtKind::Other);
        };
        self.i += 1;
        match word.as_slice() {
            b"DATABASE" | b"SCHEMA" => {
                let if_not_exists = self.eat_if_not_exists();
                let mut db = None;
                if self.ident(self.i) {
                    let name = self.value(self.i);
                    if !self.ctx.internal && is_reserved_database(&name) {
                        return Err(NameError::Reserved(name));
                    }
                    if let Some(c) = self.chain_at(self.i) {
                        self.mark(&c);
                    }
                    self.i += 1;
                    self.cur.database = Some(name.clone());
                    db = Some(name);
                }
                let (charset, collation, other_options) = self.db_options();
                self.set_ddl(db.map(|name| DdlTarget::CreateDatabase {
                    name,
                    if_not_exists,
                    or_replace: m.or_replace,
                    charset,
                    collation,
                    other_options,
                }));
                self.opaque();
                Ok(StmtKind::CreateDatabase)
            }
            b"TABLE" => {
                let ine = self.eat_if_not_exists();
                let c = self.object(ObjectKind::Table, true)?;
                let (like, select) = self.create_table_rest()?;
                let mut target = self.create_target(DdlKind::Table, c, ine, m);
                if let Some(DdlTarget::CreateObject {
                    like: l, as_select, ..
                }) = &mut target
                {
                    *l = like.and_then(|c| self.ddl_name(&c));
                    *as_select = select;
                    if like.is_some() && l.is_none() {
                        target = None;
                    }
                }
                self.set_ddl(target);
                Ok(StmtKind::Ddl)
            }
            b"VIEW" => {
                let ine = self.eat_if_not_exists();
                let c = self.object(ObjectKind::View, true)?;
                self.view_rest()?;
                let target = self.create_target(DdlKind::View, c, ine, m);
                self.set_ddl(target);
                Ok(StmtKind::Ddl)
            }
            b"PROCEDURE" | b"FUNCTION" => {
                let ine = self.eat_if_not_exists();
                let func = word == b"FUNCTION";
                // A loadable function: CREATE FUNCTION f RETURNS STRING SONAME 'x'.
                if func && self.kw(self.i + 1, b"RETURNS") {
                    self.opaque();
                    return Ok(StmtKind::Other);
                }
                let (kind, dk) = if func {
                    (ObjectKind::Function, DdlKind::Function)
                } else {
                    (ObjectKind::Procedure, DdlKind::Procedure)
                };
                let c = self.object(kind, true)?;
                self.routine_rest()?;
                let target = self.create_target(dk, c, ine, m);
                self.set_ddl(target);
                Ok(StmtKind::Ddl)
            }
            b"TRIGGER" => {
                let ine = self.eat_if_not_exists();
                let c = self.object(ObjectKind::Trigger, true)?;
                while !self.at_end() && !self.at(b"ON") {
                    self.i += 1; // BEFORE|AFTER INSERT|UPDATE|DELETE
                }
                let mut parent = None;
                if self.eat(b"ON") {
                    parent = self.object(ObjectKind::Table, false)?;
                }
                if self.at(b"FOR") && self.kw(self.i + 1, b"EACH") {
                    self.i += 3;
                }
                if self.eat(b"FOLLOWS") || self.eat(b"PRECEDES") {
                    self.object(ObjectKind::Trigger, false)?;
                }
                self.stored_body()?;
                let parent = parent.and_then(|p| self.ddl_name(&p));
                let mut target = self.create_target(DdlKind::Trigger, c, ine, m);
                if let Some(DdlTarget::CreateObject { parent: p, .. }) = &mut target {
                    *p = parent;
                    if p.is_none() {
                        target = None;
                    }
                }
                self.set_ddl(target);
                Ok(StmtKind::Ddl)
            }
            b"EVENT" => {
                let ine = self.eat_if_not_exists();
                let c = self.object(ObjectKind::Event, true)?;
                self.event_rest()?;
                let target = self.create_target(DdlKind::Event, c, ine, m);
                self.set_ddl(target);
                Ok(StmtKind::Ddl)
            }
            b"INDEX" => {
                let if_not_exists = self.eat_if_not_exists();
                let mut name = None;
                if self.ident(self.i) && !self.at(b"ON") {
                    name = Some(self.value(self.i));
                    self.i += 1;
                }
                while !self.at_end() && !self.at(b"ON") {
                    self.i += 1; // USING BTREE
                }
                let mut table = None;
                if self.eat(b"ON") {
                    table = self.object(ObjectKind::Table, false)?;
                }
                self.expr(Stop::NONE)?;
                let target =
                    table
                        .and_then(|t| self.ddl_name(&t))
                        .map(|table| DdlTarget::AlterObject {
                            kind: DdlKind::Table,
                            name: table,
                            if_exists: false,
                            op: AlterOp::CreateIndex {
                                name,
                                if_not_exists,
                                or_replace: m.or_replace,
                            },
                        });
                self.set_ddl(target);
                Ok(StmtKind::Ddl)
            }
            b"SEQUENCE" => {
                let ine = self.eat_if_not_exists();
                let c = self.object(ObjectKind::Sequence, true)?;
                self.expr(Stop::NONE)?;
                let target = self.create_target(DdlKind::Sequence, c, ine, m);
                self.set_ddl(target);
                Ok(StmtKind::Ddl)
            }
            b"USER" | b"ROLE" => {
                self.account = true;
                self.opaque();
                Ok(StmtKind::Account)
            }
            _ => {
                self.opaque();
                Ok(StmtKind::Other)
            }
        }
    }

    /// After `CREATE TABLE name`: the `LIKE` source, and whether a query
    /// follows.
    fn create_table_rest(&mut self) -> R<(Option<Chain>, bool)> {
        if self.eat(b"LIKE") {
            let c = self.object(ObjectKind::Table, false)?;
            return Ok((c, false));
        }
        if self.punct(self.i, b'(') && self.kw(self.i + 1, b"LIKE") {
            self.i += 2;
            let c = self.object(ObjectKind::Table, false)?;
            self.eat_punct(b')');
            return Ok((c, false));
        }
        let mut select = self.punct(self.i, b'(') && self.subquery_at(self.i + 1);
        if self.punct(self.i, b'(') && !select {
            self.i += 1;
            self.ddl(Stop::NONE)?;
            self.eat_punct(b')');
        }
        // Table options, then maybe [IGNORE|REPLACE] [AS] query.
        self.ddl(Stop::words(&[
            b"SELECT", b"WITH", b"AS", b"IGNORE", b"REPLACE", b"TABLE",
        ]))?;
        while self.kw_any(self.i, &[b"AS", b"IGNORE", b"REPLACE"]) {
            self.i += 1;
        }
        if !self.at_end() {
            select = true;
            self.query()?;
        }
        Ok((None, select))
    }

    /// A DDL body or option list: expressions, plus `REFERENCES t` and
    /// `UNION = (t1, t2)`, with `_sprig_` names refused.
    fn ddl(&mut self, stop: Stop) -> R {
        let saved = (self.creating_cols, self.body);
        self.creating_cols = true;
        self.body += 1;
        let mut depth = 0u32;
        let res = (|| {
            while self.i < self.n() {
                if self.punct(self.i, b';') {
                    break;
                }
                if self.punct(self.i, b'(') {
                    if self.subquery_at(self.i + 1) {
                        self.i += 1;
                        self.query()?;
                        self.eat_punct(b')');
                    } else {
                        depth += 1;
                        self.i += 1;
                    }
                } else if self.punct(self.i, b')') {
                    if depth == 0 {
                        break;
                    }
                    depth -= 1;
                    self.i += 1;
                } else if depth == 0 && self.is_stop(self.i, stop) {
                    break;
                } else if self.at(b"REFERENCES") {
                    self.i += 1;
                    self.object(ObjectKind::Table, false)?;
                } else if self.at(b"UNION")
                    && (self.op(self.i + 1, b"=") || self.punct(self.i + 1, b'('))
                {
                    self.i += 1;
                    if self.op(self.i, b"=") {
                        self.i += 1;
                    }
                    if self.eat_punct(b'(') {
                        self.object_list(ObjectKind::Table, false)?;
                        self.eat_punct(b')');
                    }
                } else if self.ident(self.i) {
                    self.expr_word()?;
                } else {
                    self.i += 1;
                }
            }
            Ok(())
        })();
        (self.creating_cols, self.body) = saved;
        res
    }

    /// After `CREATE VIEW name` or `ALTER VIEW name`.
    fn view_rest(&mut self) -> R {
        if self.punct(self.i, b'(') {
            self.parens()?;
        }
        self.eat(b"AS");
        self.body += 1;
        let r = self.query();
        self.body -= 1;
        r
    }

    /// After `CREATE PROCEDURE|FUNCTION name`: parameters, `RETURNS`,
    /// characteristics, then the body.
    fn routine_rest(&mut self) -> R {
        if self.punct(self.i, b'(') {
            self.parens()?;
        }
        while !self.at_end() && !self.body_starts_here() {
            if self.punct(self.i, b'(') {
                self.parens()?;
            } else {
                self.i += 1;
            }
        }
        self.stored_body()
    }

    fn body_starts_here(&self) -> bool {
        let j = self.i;
        if self.ident(j) && self.op(j + 1, b":") {
            return true; // a label
        }
        if !self.kw_any(j, BODY_STARTS) {
            return false;
        }
        // RETURNS SET('a'), CHARACTER SET x: types, not statements.
        !(self.kw(j, b"SET")
            && (self.punct(j + 1, b'(') || j > 0 && self.kw_any(j - 1, &[b"CHARACTER", b"CHAR"])))
    }

    /// A stored program's body: no substitutions inside.
    fn stored_body(&mut self) -> R {
        self.body += 1;
        let r = self.body_statement();
        self.body -= 1;
        r
    }

    /// After `CREATE|ALTER EVENT name`: the schedule and options, maybe a
    /// rename, then `DO body`.
    fn event_rest(&mut self) -> R<Option<Chain>> {
        let mut renamed = None;
        loop {
            self.expr(Stop::words(&[b"DO", b"RENAME"]))?;
            if self.at(b"RENAME") && self.kw(self.i + 1, b"TO") {
                self.i += 2;
                renamed = self.object(ObjectKind::Event, true)?;
                continue;
            }
            break;
        }
        if self.eat(b"DO") {
            self.stored_body()?;
        }
        Ok(renamed)
    }

    /// One statement of a compound body, or a whole single-statement body.
    fn body_statement(&mut self) -> R {
        self.enter()?;
        let r = self.body_statement_inner();
        self.nest -= 1;
        r
    }

    fn body_statement_inner(&mut self) -> R {
        if self.ident(self.i) && self.op(self.i + 1, b":") {
            self.i += 2;
        }
        if self.at(b"BEGIN") {
            self.i += 1;
            if self.at(b"NOT") && self.kw(self.i + 1, b"ATOMIC") {
                self.i += 2;
            }
            self.stmt_list()?;
            self.end_of(&[]);
        } else if self.at(b"IF") {
            self.i += 1;
            loop {
                self.expr(Stop::words(&[b"THEN"]))?;
                self.eat(b"THEN");
                self.stmt_list()?;
                if self.eat(b"ELSEIF") {
                    continue;
                }
                if self.eat(b"ELSE") {
                    self.stmt_list()?;
                }
                break;
            }
            self.end_of(&[b"IF"]);
        } else if self.at(b"CASE") {
            self.i += 1;
            if !self.at(b"WHEN") {
                self.expr(Stop::words(&[b"WHEN"]))?;
            }
            while self.eat(b"WHEN") {
                self.expr(Stop::words(&[b"THEN"]))?;
                self.eat(b"THEN");
                self.stmt_list()?;
            }
            if self.eat(b"ELSE") {
                self.stmt_list()?;
            }
            self.end_of(&[b"CASE"]);
        } else if self.at(b"LOOP") {
            self.i += 1;
            self.stmt_list()?;
            self.end_of(&[b"LOOP"]);
        } else if self.at(b"WHILE") || self.at(b"FOR") {
            let kw: &'static [u8] = if self.at(b"WHILE") { b"WHILE" } else { b"FOR" };
            self.i += 1;
            self.expr(Stop::words(&[b"DO"]))?;
            self.eat(b"DO");
            self.stmt_list()?;
            self.end_of(&[kw]);
        } else if self.at(b"REPEAT") {
            self.i += 1;
            self.stmt_list()?;
            if self.eat(b"UNTIL") {
                self.expr(Stop::words(&[b"END"]))?;
            }
            self.end_of(&[b"REPEAT"]);
        } else if self.at(b"DECLARE") {
            self.declare()?;
        } else if self.at(b"RETURN") {
            self.i += 1;
            self.expr(Stop::NONE)?;
        } else {
            let purpose = self.purpose;
            let account = self.account;
            self.statement()?;
            self.purpose = purpose;
            self.account = account;
        }
        Ok(())
    }

    /// Statements up to one of `LIST_ENDS`, each ending in `;`.
    fn stmt_list(&mut self) -> R {
        loop {
            if self.i >= self.n() || self.kw_any(self.i, LIST_ENDS) {
                return Ok(());
            }
            if self.eat_punct(b';') {
                continue;
            }
            let before = self.i;
            self.body_statement()?;
            // Anything left of this statement, as expressions.
            while !self.at_end() && !self.kw_any(self.i, LIST_ENDS) {
                let at = self.i;
                self.expr(Stop::words(LIST_ENDS))?;
                if self.i == at {
                    self.i += 1;
                }
            }
            if self.i == before {
                self.i += 1;
            }
        }
    }

    /// `END [kw] [label]`.
    fn end_of(&mut self, kw: &[&[u8]]) {
        if self.eat(b"END") {
            for k in kw {
                self.eat(k);
            }
            if self.ident(self.i) && !self.kw_any(self.i, LIST_ENDS) {
                self.i += 1; // label
            }
        }
    }

    fn declare(&mut self) -> R {
        self.i += 1;
        if self.kw_any(self.i, &[b"CONTINUE", b"EXIT", b"UNDO"]) && self.kw(self.i + 1, b"HANDLER")
        {
            self.i += 2;
            self.eat(b"FOR");
            loop {
                if self.eat(b"SQLSTATE") {
                    self.eat(b"VALUE");
                    self.i += 1;
                } else if self.at(b"NOT") {
                    self.i += 2;
                } else {
                    self.i += 1;
                }
                if !self.eat_punct(b',') {
                    break;
                }
            }
            return self.body_statement();
        }
        // DECLARE name CURSOR [(params)] FOR query
        if self.kw(self.i + 1, b"CURSOR") {
            self.i += 2;
            if self.punct(self.i, b'(') {
                self.parens()?;
            }
            self.eat(b"FOR");
            return self.query();
        }
        if self.kw(self.i + 1, b"CONDITION") {
            self.opaque();
            return Ok(());
        }
        // DECLARE a, b type [DEFAULT expr]
        self.expr(Stop::NONE)
    }

    /// An `ALTER` target.
    fn alter_target(
        &self,
        kind: DdlKind,
        c: Option<Chain>,
        if_exists: bool,
        rename_to: Option<Chain>,
    ) -> Option<DdlTarget> {
        let name = self.ddl_name(&c?)?;
        let rename_to = match rename_to {
            Some(r) => Some(self.ddl_name(&r)?),
            None => None,
        };
        Some(DdlTarget::AlterObject {
            kind,
            name,
            if_exists,
            op: AlterOp::Change { rename_to },
        })
    }

    #[allow(clippy::too_many_lines)]
    fn alter(&mut self) -> R<StmtKind> {
        self.purpose = Purpose::Ddl;
        self.i += 1;
        self.skip_create_modifiers();
        let Some(word) = (self.kind(self.i) == Some(TokenKind::Word))
            .then(|| self.bytes(self.i).to_ascii_uppercase())
        else {
            self.opaque();
            return Ok(StmtKind::Other);
        };
        self.i += 1;
        match word.as_slice() {
            b"TABLE" => {
                let if_exists = self.eat_if_exists();
                let c = self.object(ObjectKind::Table, false)?;
                // ALTER TABLE t SPRIG SET (...) writes settings (chapter 08).
                let settings = self.at(b"SPRIG");
                let mut renamed = None;
                let mut other = false;
                loop {
                    let before = self.i;
                    self.ddl(Stop::words(&[b"RENAME", b"EXCHANGE"]))?;
                    other |= self.i != before;
                    if self.at(b"RENAME") {
                        self.i += 1;
                        if self.kw_any(self.i, &[b"COLUMN", b"INDEX", b"KEY", b"CONSTRAINT"]) {
                            other = true;
                            self.i += 1;
                            continue;
                        }
                        if self.kw_any(self.i, &[b"TO", b"AS"]) {
                            self.i += 1;
                        }
                        renamed = self.object(ObjectKind::Table, true)?;
                        if renamed.is_none() {
                            other = true;
                        }
                    } else if self.at(b"EXCHANGE") {
                        other = true;
                        while !self.at_end() && !self.at(b"TABLE") {
                            self.i += 1;
                        }
                        if self.eat(b"TABLE") {
                            self.object(ObjectKind::Table, false)?;
                        }
                    } else {
                        break;
                    }
                }
                let target = if settings {
                    None
                } else if let (Some(from), Some(to), false) = (c, renamed, other) {
                    self.ddl_name(&from)
                        .zip(self.ddl_name(&to))
                        .map(|p| DdlTarget::RenameTables {
                            pairs: vec![p],
                            if_exists,
                        })
                } else {
                    self.alter_target(DdlKind::Table, c, if_exists, renamed)
                };
                self.set_ddl(target);
                Ok(StmtKind::Ddl)
            }
            b"DATABASE" | b"SCHEMA" => {
                let mut explicit = None;
                if self.ident(self.i) && !self.kw_any(self.i, DB_OPTIONS) && !self.at(b"SPRIG") {
                    let tok = self.i;
                    let name = self.database()?;
                    self.cur.database = Some(name.unwrap_or_else(|| self.value(tok)));
                    explicit = Some(self.value(tok));
                }
                if !self.at(b"SPRIG") {
                    let (charset, collation, other_options) = self.db_options();
                    let target = DdlTarget::AlterDatabase {
                        explicit: explicit.is_some(),
                        name: explicit.or_else(|| self.current.clone()),
                        charset,
                        collation,
                        other_options,
                    };
                    self.set_ddl(Some(target));
                }
                self.opaque();
                Ok(StmtKind::AlterDatabase)
            }
            b"VIEW" => {
                let c = self.object(ObjectKind::View, false)?;
                self.view_rest()?;
                let target = self.alter_target(DdlKind::View, c, false, None);
                self.set_ddl(target);
                Ok(StmtKind::Ddl)
            }
            b"PROCEDURE" | b"FUNCTION" => {
                let (kind, dk) = if word == b"FUNCTION" {
                    (ObjectKind::Function, DdlKind::Function)
                } else {
                    (ObjectKind::Procedure, DdlKind::Procedure)
                };
                let c = self.object(kind, false)?;
                self.opaque();
                let target = self.alter_target(dk, c, false, None);
                self.set_ddl(target);
                Ok(StmtKind::Ddl)
            }
            b"EVENT" => {
                let c = self.object(ObjectKind::Event, false)?;
                let renamed = self.event_rest()?;
                let target = self.alter_target(DdlKind::Event, c, false, renamed);
                self.set_ddl(target);
                Ok(StmtKind::Ddl)
            }
            b"SEQUENCE" => {
                let if_exists = self.eat_if_exists();
                let c = self.object(ObjectKind::Sequence, false)?;
                self.expr(Stop::NONE)?;
                let target = self.alter_target(DdlKind::Sequence, c, if_exists, None);
                self.set_ddl(target);
                Ok(StmtKind::Ddl)
            }
            b"USER" | b"ROLE" => {
                self.account = true;
                self.opaque();
                Ok(StmtKind::Account)
            }
            _ => {
                self.opaque();
                Ok(StmtKind::Other)
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn drop(&mut self) -> R<StmtKind> {
        self.purpose = Purpose::Ddl;
        self.i += 1;
        let temporary = self.eat(b"TEMPORARY");
        let Some(word) = (self.kind(self.i) == Some(TokenKind::Word))
            .then(|| self.bytes(self.i).to_ascii_uppercase())
        else {
            self.opaque();
            return Ok(StmtKind::Other);
        };
        self.i += 1;
        let kind = match word.as_slice() {
            b"TABLE" | b"TABLES" => ObjectKind::Table,
            b"VIEW" => ObjectKind::View,
            b"SEQUENCE" => ObjectKind::Sequence,
            b"PROCEDURE" => ObjectKind::Procedure,
            b"FUNCTION" => ObjectKind::Function,
            b"TRIGGER" => ObjectKind::Trigger,
            b"EVENT" => ObjectKind::Event,
            b"DATABASE" | b"SCHEMA" => {
                let if_exists = self.at(b"IF") && self.kw(self.i + 1, b"EXISTS");
                self.eat_if_exists();
                if self.ident(self.i) {
                    let tok = self.i;
                    let name = self.value(tok);
                    if !self.ctx.internal && SystemSchema::parse(&name).is_some() {
                        return Err(NameError::Reserved(name));
                    }
                    // DROP DATABASE IF EXISTS of an unknown database is a
                    // note, not an error: leave the name for MariaDB.
                    let known = self.ctx.map.database(&name).is_some();
                    if if_exists && !known {
                        if let Some(c) = self.chain_at(tok) {
                            self.mark(&c);
                        }
                        self.i += 1;
                        self.cur.database = Some(name.clone());
                    } else {
                        let logical = self.database()?;
                        self.cur.database = Some(logical.unwrap_or_else(|| name.clone()));
                    }
                    self.set_ddl(Some(DdlTarget::DropDatabase { name, if_exists }));
                }
                self.opaque();
                return Ok(StmtKind::DropDatabase);
            }
            b"INDEX" => {
                if self.kw_any(self.i, &[b"ONLINE", b"OFFLINE"]) && !self.kw(self.i + 1, b"ON") {
                    self.i += 1;
                }
                let if_exists = self.eat_if_exists();
                let mut name = None;
                if self.ident(self.i) && !self.at(b"ON") {
                    name = Some(self.value(self.i));
                }
                while !self.at_end() && !self.at(b"ON") {
                    self.i += 1;
                }
                let mut table = None;
                if self.eat(b"ON") {
                    table = self.object(ObjectKind::Table, false)?;
                }
                let target =
                    name.zip(table.and_then(|t| self.ddl_name(&t)))
                        .map(|(name, table)| DdlTarget::AlterObject {
                            kind: DdlKind::Table,
                            name: table,
                            if_exists: false,
                            op: AlterOp::DropIndex { name, if_exists },
                        });
                self.set_ddl(target);
                self.opaque();
                return Ok(StmtKind::Ddl);
            }
            b"USER" | b"ROLE" => {
                self.account = true;
                self.opaque();
                return Ok(StmtKind::Account);
            }
            b"PREPARE" => {
                self.opaque();
                return Ok(StmtKind::Prepare);
            }
            _ => {
                self.opaque();
                return Ok(StmtKind::Other);
            }
        };
        let if_exists = self.eat_if_exists();
        let mut names = Some(Vec::new());
        loop {
            let c = self.object(kind, false)?;
            match (&mut names, c.and_then(|c| self.ddl_name(&c))) {
                (Some(ns), Some(n)) => ns.push(n),
                _ => names = None,
            }
            if !self.eat_punct(b',') {
                break;
            }
        }
        let target = names
            .filter(|n| !n.is_empty())
            .map(|names| DdlTarget::DropObjects {
                kind: ddl_kind(kind),
                names,
                if_exists,
                temporary,
            });
        self.set_ddl(target);
        self.opaque();
        Ok(StmtKind::Ddl)
    }

    fn grant(&mut self) -> R<StmtKind> {
        self.purpose = Purpose::Ddl;
        self.account = true;
        self.i += 1;
        self.expr(Stop::words(&[b"ON", b"TO", b"FROM"]))?;
        if self.eat(b"ON") {
            let kind = if self.eat(b"FUNCTION") {
                ObjectKind::Function
            } else if self.eat(b"PROCEDURE") {
                ObjectKind::Procedure
            } else {
                self.eat(b"TABLE");
                ObjectKind::Table
            };
            if self.op(self.i, b"*") {
                // *.* (global) or * (current database): chapter 29.
                self.i += 1;
                if self.punct(self.i, b'.') && self.op(self.i + 1, b"*") {
                    self.i += 2;
                }
            } else if let Some(c) = self.chain_at(self.i) {
                if c.n == 1 && c.star {
                    // db.*: `_` and `%` are wildcards unless escaped. An
                    // unescaped `%` is a pattern no logical name maps;
                    // `\_` and `\%` stand for the characters themselves.
                    self.mark(&c);
                    self.i = c.end;
                    let db = self.value(c.parts[0]);
                    let unescaped = db.replace("\\%", "").replace("\\_", "");
                    if unescaped.contains('%') {
                        return Err(NameError::Unsupported(format!(
                            "a database pattern in GRANT: '{db}'"
                        )));
                    }
                    let name = db.replace("\\_", "_").replace("\\%", "%");
                    self.map_schema(c.parts[0], &name, None)?;
                } else {
                    self.object(kind, false)?;
                }
            }
        }
        self.opaque();
        Ok(StmtKind::Account)
    }

    fn show(&mut self) -> R<StmtKind> {
        self.purpose = Purpose::Metadata;
        self.i += 1;
        while self.kw_any(self.i, &[b"FULL", b"EXTENDED"]) {
            self.i += 1;
        }
        if self.eat(b"CREATE") {
            if self.eat(b"DATABASE") || self.eat(b"SCHEMA") {
                self.eat_if_not_exists();
                self.database()?;
            } else if let Some(kind) = self.show_object_kind() {
                let table = self.at(b"TABLE");
                self.i += 1;
                let c = self.object(kind, false)?;
                if table {
                    self.grouped_chain(c);
                }
            }
            self.opaque();
            return Ok(StmtKind::Show);
        }
        if self.kw_any(
            self.i,
            &[b"COLUMNS", b"FIELDS", b"INDEX", b"INDEXES", b"KEYS"],
        ) {
            self.i += 1;
            let mut c = None;
            if self.eat(b"FROM") || self.eat(b"IN") {
                c = self.object(ObjectKind::Table, false)?;
            }
            let before = self.edits.len();
            self.show_db_clauses()?;
            // `SHOW COLUMNS FROM t FROM db`: the database's edit.
            let from_db = self.edits[before..]
                .iter()
                .find(|e| matches!(e.kind, EditKind::Schema { .. }))
                .map(|e| e.range.clone());
            match (c, from_db) {
                (Some(c), Some(r)) if c.n == 1 && !c.star => {
                    self.grouped_table(c.parts[0], Some(&r));
                }
                (c, _) => self.grouped_chain(c),
            }
            return Ok(StmtKind::Show);
        }
        if self.kw_any(self.i, &[b"PROCEDURE", b"FUNCTION", b"PACKAGE"])
            && self.kw(self.i + 1, b"CODE")
        {
            self.i += 2;
            self.object(ObjectKind::Procedure, false)?;
            self.opaque();
            return Ok(StmtKind::Show);
        }
        let lists_db = self.kw_any(self.i, &[b"TABLES", b"TRIGGERS", b"EVENTS"])
            || (self.at(b"TABLE") && self.kw(self.i + 1, b"STATUS"))
            || (self.at(b"OPEN") && self.kw(self.i + 1, b"TABLES"));
        if lists_db {
            self.show_db_clauses()?;
        } else {
            self.opaque();
        }
        Ok(StmtKind::Show)
    }

    fn show_object_kind(&self) -> Option<ObjectKind> {
        let k = [
            (b"TABLE" as &[u8], ObjectKind::Table),
            (b"VIEW", ObjectKind::View),
            (b"PROCEDURE", ObjectKind::Procedure),
            (b"FUNCTION", ObjectKind::Function),
            (b"TRIGGER", ObjectKind::Trigger),
            (b"EVENT", ObjectKind::Event),
            (b"SEQUENCE", ObjectKind::Sequence),
            (b"PACKAGE", ObjectKind::Procedure),
        ];
        k.iter().find(|(w, _)| self.at(w)).map(|&(_, kind)| kind)
    }

    /// `[FROM|IN db] [LIKE 'p' | WHERE expr]` of the `SHOW` statements that
    /// list a database's objects.
    fn show_db_clauses(&mut self) -> R {
        while !self.at_end() {
            if self.eat(b"FROM") || self.eat(b"IN") {
                self.database()?;
            } else if self.eat(b"WHERE") {
                self.expr(Stop::NONE)?;
            } else {
                self.i += 1;
            }
        }
        Ok(())
    }

    fn describe(&mut self) -> R<StmtKind> {
        const OPTIONS: &[&[u8]] = &[b"EXTENDED", b"PARTITIONS", b"ANALYZE", b"FORMAT"];
        self.purpose = Purpose::Read;
        self.i += 1;
        let stmt_next = self.subquery_at(self.i)
            || self.punct(self.i, b'(')
            || self.kw_any(
                self.i,
                &[b"INSERT", b"UPDATE", b"DELETE", b"REPLACE", b"VALUES"],
            )
            || self.kw_any(self.i, OPTIONS)
            || self.at(b"FOR");
        if !stmt_next {
            let c = self.object(ObjectKind::Table, false)?;
            self.grouped_chain(c);
            self.opaque();
            return Ok(StmtKind::Describe);
        }
        while self.kw_any(self.i, OPTIONS) {
            self.i += 1;
            if self.op(self.i, b"=") {
                self.i += 2;
            }
        }
        if self.at(b"FOR") {
            self.opaque(); // EXPLAIN FOR CONNECTION n
            return Ok(StmtKind::Describe);
        }
        self.statement()
    }

    fn set(&mut self) -> R<StmtKind> {
        self.purpose = Purpose::Read;
        self.i += 1;
        if self.kw_any(self.i, &[b"PASSWORD", b"ROLE"])
            || (self.at(b"DEFAULT") && self.kw(self.i + 1, b"ROLE"))
        {
            self.account = true;
            self.opaque();
            return Ok(StmtKind::Account);
        }
        if self.at(b"STATEMENT") {
            // MariaDB: SET STATEMENT var = value [, ...] FOR statement
            self.i += 1;
            self.expr(Stop::words(&[b"FOR"]))?;
            if self.eat(b"FOR") {
                return self.statement();
            }
            return Ok(StmtKind::Set);
        }
        self.expr(Stop::NONE)?;
        Ok(StmtKind::Set)
    }

    /// Fails if any dotted name was never read by the grammar above.
    fn check_all_classified(&self) -> R {
        for j in 0..self.n() {
            if self.classified[j] || !self.ident(j) || !self.punct(j + 1, b'.') {
                continue;
            }
            if self.ident(j + 2) || self.op(j + 2, b"*") {
                return Err(NameError::Unsupported(format!(
                    "qualified name {}.{} in a position the rewriter does not handle",
                    self.value(j),
                    String::from_utf8_lossy(self.bytes(j + 2))
                )));
            }
        }
        Ok(())
    }
}

/// The DDL kind of an object kind the `DROP` grammar names.
const fn ddl_kind(kind: ObjectKind) -> DdlKind {
    match kind {
        ObjectKind::View => DdlKind::View,
        ObjectKind::Sequence => DdlKind::Sequence,
        ObjectKind::Procedure => DdlKind::Procedure,
        ObjectKind::Function => DdlKind::Function,
        ObjectKind::Trigger => DdlKind::Trigger,
        ObjectKind::Event => DdlKind::Event,
        _ => DdlKind::Table,
    }
}

/// Widens each statement's range to whole executable comments: dump tools
/// write `START TRANSACTION /*!40100 WITH CONSISTENT SNAPSHOT */` and
/// `/*!50003 CREATE*/ /*!50017 DEFINER=...*/ /*!50003 TRIGGER ... END */`,
/// whose markers are trivia, so a range from the first to the last
/// significant token would cut a comment open at either end. A statement
/// starting inside a comment starts at its `/*!`; one ending inside a
/// comment ends after its `*/`, unless another statement shares that
/// comment (`/*!40101 SET a = 1; SET b = 2 */` keeps its inner ranges).
fn whole_exec_comments(tokens: &[Token], statements: &mut [StatementInfo]) {
    // Depth before each token.
    let mut depth = Vec::with_capacity(tokens.len());
    let mut open = 0usize;
    for t in tokens {
        depth.push(open);
        match t.kind {
            TokenKind::ExecOpen => open += 1,
            TokenKind::ExecClose => open = open.saturating_sub(1),
            _ => {}
        }
    }
    let at = |pos: usize| tokens.iter().position(|t| t.start as usize >= pos);
    for i in 0..statements.len() {
        let prev_end = if i == 0 {
            0
        } else {
            statements[i - 1].range.end
        };
        let next_start = statements.get(i + 1).map_or(usize::MAX, |s| s.range.start);
        let r = statements[i].range.clone();
        let Some(first) = at(r.start) else { continue };
        let mut start = r.start;
        // Back over the openers this statement sits in.
        let mut back = first;
        let mut want = depth[first];
        while want > 0 && back > 0 {
            back -= 1;
            let t = tokens[back];
            if (t.start as usize) < prev_end {
                break;
            }
            if t.kind == TokenKind::ExecOpen && depth[back] < want {
                start = t.start as usize;
                want = depth[back];
            } else if !t.kind.is_trivia() {
                break;
            }
        }
        // Forward over the closers that bring the depth back.
        let last = at(r.end).unwrap_or(tokens.len());
        let mut end = r.end;
        let mut level = if last < tokens.len() {
            depth[last]
        } else {
            open
        };
        let mut fwd = last;
        while level > want && fwd < tokens.len() {
            let t = tokens[fwd];
            if t.start as usize >= next_start {
                break;
            }
            if t.kind == TokenKind::ExecClose {
                end = t.end as usize;
                level -= 1;
            } else if !t.kind.is_trivia() {
                break;
            }
            fwd += 1;
        }
        // Unbalanced either way: keep the original range.
        if level == want {
            statements[i].range = start..end;
        }
    }
}
