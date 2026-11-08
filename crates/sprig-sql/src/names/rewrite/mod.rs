//! The statement rewrite (chapter 07, "Statement rewrite"; build steps 2
//! and 3).
//!
//! [`rewrite`] edits the client's text instead of regenerating SQL: it
//! replaces only the tokens that name a logical database, and the calls it
//! substitutes, so comments, hints, spelling and layout survive byte for
//! byte. Each statement goes through:
//!
//! 1. [`crate::lex::lex`] with the persona's rules ([`RewriteCtx::lex`]);
//! 2. the **lexer-level walker**, which splits the text into statements
//!    (understanding `BEGIN ... END` bodies), finds every name position of
//!    the MySQL and MariaDB grammar, and makes the substitutions;
//! 3. in [`Engine::Auto`] (not the default, see [`Engine`]), for the
//!    statement kinds sqlparser-rs models that hold a dotted name
//!    (queries, `INSERT`, `REPLACE`, `UPDATE`, `DELETE`, and `CREATE`,
//!    `ALTER`, `DROP` and `TRUNCATE` of tables and views), the **AST
//!    path**: the statement is parsed with sqlparser-rs and the schema
//!    positions come from the AST. Its answer is used only when every
//!    dotted name in the text is accounted for by the AST; otherwise the
//!    walker's stands. [`StatementInfo::engine`] says which one ran.
//!
//! Fail-closed rules (chapter 07, "Failure handling"): an unknown database
//! is 1049; a qualified name in a position the rewriter does not
//! understand is 39051, never forwarded unrewritten; a reference to a
//! reserved `__sprig_` object fails as missing, a `_sprig_` column as 1054,
//! and creating either is 39050.
//!
//! String literals are never rewritten, with one exception: the SQL text
//! of `PREPARE ... FROM '...'` and MariaDB's `EXECUTE IMMEDIATE '...'`,
//! which is rewritten as a statement of its own. Schema names in
//! `information_schema` filters are handled by step 5's inline views; this
//! module only reports the system-schema references it sees
//! ([`StatementInfo::system_refs`]).

mod ast;
mod subst;
mod walker;

use std::ops::Range;

use sprig_proto_mysql::packets::Persona;

use super::NameError;
use super::map::{NameMap, SchemaName};
use crate::lex::{self, LexOptions};

pub use subst::sql_string;

/// Which name finder handles a statement.
///
/// The walker is the default: it is about ten times faster than parsing
/// with sqlparser-rs (1.6 µs against 20 µs for a typical 240-byte
/// qualified `SELECT`, release build), and the differential tests hold the
/// two to the same answers. [`Engine::Auto`] runs the AST path as chapter
/// 07 describes it, for shadow checking or once the router (R1) parses
/// every statement anyway.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Engine {
    /// The AST path where sqlparser-rs parses the statement and accounts for
    /// every dotted name; the lexer-level walker otherwise.
    Auto,
    /// The lexer-level walker only.
    #[default]
    Lexer,
    /// The AST path, which must succeed: for tests and differential checks.
    /// Statements it can't handle fail with 39051.
    Ast,
}

/// The session's account, for the account functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountInfo<'a> {
    /// The login user name, as the client knows it (`alice`).
    pub user: &'a str,
    /// The matched account's host pattern (`%`): `CURRENT_USER()`.
    pub account_host: &'a str,
    /// The client's real address (`203.0.113.7`): `USER()`,
    /// `SESSION_USER()`, `SYSTEM_USER()`.
    pub client_host: &'a str,
}

/// Everything a rewrite depends on besides the text.
#[derive(Clone, Copy)]
pub struct RewriteCtx<'a> {
    /// The tenant's names.
    pub map: &'a dyn NameMap,
    /// Persona, version and `sql_mode` flags for the lexer.
    pub lex: LexOptions,
    /// The session's current logical database, if any.
    pub current_db: Option<&'a str>,
    /// The session's account; account functions are left alone without it.
    pub account: Option<AccountInfo<'a>>,
    /// Query attributes sent with the statement (3306 only), by name.
    pub query_attributes: &'a [(&'a str, Option<&'a str>)],
    /// A SprigDB-internal session: reserved names are allowed.
    pub internal: bool,
    /// `IGNORE_SPACE` is in `sql_mode`: a function name may be followed by
    /// spaces before `(`.
    pub ignore_space: bool,
    /// `names.lower_case_table_names = 1`: database names are lowercased
    /// before lookup.
    pub lower_case_table_names: bool,
    /// The lowest MariaDB version among the backends, as a number. When set,
    /// executable comments the persona skips but MariaDB would run are
    /// turned into plain comments.
    pub backend_version: Option<u32>,
    /// Which name finder to use.
    pub engine: Engine,
}

impl std::fmt::Debug for RewriteCtx<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RewriteCtx")
            .field("tenant", &self.map.tenant())
            .field("lex", &self.lex)
            .field("current_db", &self.current_db)
            .field("account", &self.account)
            .field("internal", &self.internal)
            .field("engine", &self.engine)
            .finish_non_exhaustive()
    }
}

impl<'a> RewriteCtx<'a> {
    /// A context for `persona` with its default lexer options, no current
    /// database and no account.
    #[must_use]
    pub fn new(map: &'a dyn NameMap, persona: Persona) -> Self {
        Self {
            map,
            lex: LexOptions::for_persona(persona),
            current_db: None,
            account: None,
            query_attributes: &[],
            internal: false,
            ignore_space: false,
            lower_case_table_names: false,
            backend_version: None,
            engine: Engine::Lexer,
        }
    }

    /// With a current database.
    #[must_use]
    pub fn current_db(mut self, db: Option<&'a str>) -> Self {
        self.current_db = db;
        self
    }

    /// With an account.
    #[must_use]
    pub fn account(mut self, account: AccountInfo<'a>) -> Self {
        self.account = Some(account);
        self
    }

    /// With other lexer options.
    #[must_use]
    pub fn lex(mut self, lex: LexOptions) -> Self {
        self.lex = lex;
        self
    }

    /// With an engine.
    #[must_use]
    pub fn engine(mut self, engine: Engine) -> Self {
        self.engine = engine;
        self
    }

    /// With query attributes.
    #[must_use]
    pub fn query_attributes(mut self, attrs: &'a [(&'a str, Option<&'a str>)]) -> Self {
        self.query_attributes = attrs;
        self
    }

    /// With a backend version for executable-comment neutralization.
    #[must_use]
    pub fn backend_version(mut self, version: u32) -> Self {
        self.backend_version = Some(version);
        self
    }
}

/// What an edit replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EditKind {
    /// A logical database name became a physical schema.
    Schema {
        /// The name as the client meant it.
        logical: String,
        /// The schema it stands for.
        schema: SchemaName,
    },
    /// `DATABASE()` or `SCHEMA()`.
    Database,
    /// `CURRENT_USER` or `CURRENT_USER()`.
    CurrentUser,
    /// `USER()`, `SESSION_USER()` or `SYSTEM_USER()`.
    User,
    /// `mysql_query_attribute_string('name')`.
    QueryAttribute,
    /// The SQL text of `PREPARE ... FROM '...'` or `EXECUTE IMMEDIATE`,
    /// rewritten and quoted again.
    EmbeddedSql,
    /// An executable comment's `!`, blanked so the backend skips what the
    /// persona skipped.
    NeutralizedComment,
}

/// One span edit: the source range and what replaces it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Edit {
    /// The bytes replaced, in the original text.
    pub range: Range<usize>,
    /// Why.
    pub kind: EditKind,
    /// The new text.
    pub replacement: String,
}

/// A system schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SystemSchema {
    /// `information_schema`.
    InformationSchema,
    /// `performance_schema`.
    PerformanceSchema,
    /// `sys`.
    Sys,
    /// `mysql`.
    Mysql,
}

impl SystemSchema {
    /// Recognizes a system schema name, ignoring case.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        [
            ("information_schema", Self::InformationSchema),
            ("performance_schema", Self::PerformanceSchema),
            ("sys", Self::Sys),
            ("mysql", Self::Mysql),
        ]
        .into_iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(name))
        .map(|(_, s)| s)
    }

    /// The canonical name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InformationSchema => "information_schema",
            Self::PerformanceSchema => "performance_schema",
            Self::Sys => "sys",
            Self::Mysql => "mysql",
        }
    }
}

/// A reference to a system schema, left for steps 5 and 6.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemRef {
    /// Which schema.
    pub schema: SystemSchema,
    /// The table or routine named in it, when qualified (`TABLES` in
    /// `information_schema.TABLES`).
    pub object: Option<String>,
    /// The schema token's range.
    pub range: Range<usize>,
}

/// What `USE` (or a statement like it) selects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UseTarget {
    /// A tenant database: the session's logical database becomes
    /// `logical`, and state sync sends `USE` of `schema`.
    Database {
        /// The logical name.
        logical: String,
        /// Its schema.
        schema: SchemaName,
    },
    /// A system schema.
    System(SystemSchema),
}

/// The kind of a statement, as far as names care.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StmtKind {
    /// `SELECT`, `WITH`, `TABLE`, `VALUES`, or a parenthesized query.
    Select,
    /// `INSERT`.
    Insert,
    /// `REPLACE`.
    Replace,
    /// `UPDATE`.
    Update,
    /// `DELETE`.
    Delete,
    /// `USE`.
    Use,
    /// `CREATE DATABASE` or `CREATE SCHEMA`: the gateway creates databases
    /// through the catalog, so the name is not mapped.
    CreateDatabase,
    /// `DROP DATABASE` or `DROP SCHEMA`.
    DropDatabase,
    /// `ALTER DATABASE` or `ALTER SCHEMA`.
    AlterDatabase,
    /// Other DDL: tables, views, indexes, routines, triggers, events,
    /// sequences, `RENAME TABLE`, `TRUNCATE`.
    Ddl,
    /// The `SHOW` family.
    Show,
    /// `DESCRIBE`, `DESC` or `EXPLAIN` of a table.
    Describe,
    /// `CALL`.
    Call,
    /// `LOAD DATA` or `LOAD XML`.
    LoadData,
    /// `LOCK TABLES`, `UNLOCK TABLES`.
    Lock,
    /// `GRANT`, `REVOKE` and the other account statements.
    Account,
    /// `SET`.
    Set,
    /// `PREPARE`, `EXECUTE`, `DEALLOCATE PREPARE`.
    Prepare,
    /// MariaDB's `BEGIN NOT ATOMIC ... END`.
    Compound,
    /// `HANDLER`, `FLUSH`, the table maintenance statements, `DO`,
    /// transaction control and anything else.
    Other,
}

/// What the rewrite learned about one statement of the text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementInfo {
    /// The statement's range in the original text, without the `;`.
    pub range: Range<usize>,
    /// Its kind.
    pub kind: StmtKind,
    /// For `USE`: the new current database.
    pub use_target: Option<UseTarget>,
    /// For `CREATE`, `ALTER` and `DROP DATABASE`: the logical name.
    pub database: Option<String>,
    /// System-schema references (steps 5 and 6 handle them).
    pub system_refs: Vec<SystemRef>,
    /// Which name finder decided the schema positions.
    pub engine: Engine,
    /// For the AST path: whether the walker found the same schema
    /// positions. Always true for the walker.
    pub engines_agree: bool,
}

/// A rewritten text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rewritten {
    /// The text to send to MariaDB.
    pub sql: Vec<u8>,
    /// Every edit, in source order, non-overlapping.
    pub edits: Vec<Edit>,
    /// One entry per statement, in order.
    pub statements: Vec<StatementInfo>,
}

impl Rewritten {
    /// The text as a string, replacing invalid UTF-8.
    #[must_use]
    pub fn sql_lossy(&self) -> String {
        String::from_utf8_lossy(&self.sql).into_owned()
    }

    /// Whether nothing changed.
    #[must_use]
    pub fn is_unchanged(&self) -> bool {
        self.edits.is_empty()
    }
}

/// Rewrites every name position in `sql` for the session in `ctx`.
///
/// # Errors
///
/// A [`NameError`]: 1049 for an unknown database, 1146 and friends for a
/// reserved object, 1054 for a reserved column, 39050 for creating a
/// reserved name, 39051 for a name position the rewriter does not handle.
pub fn rewrite(sql: &[u8], ctx: &RewriteCtx<'_>) -> Result<Rewritten, NameError> {
    rewrite_depth(sql, ctx, 0)
}

/// [`rewrite`] with the nesting depth of embedded SQL.
fn rewrite_depth(sql: &[u8], ctx: &RewriteCtx<'_>, depth: u8) -> Result<Rewritten, NameError> {
    let tokens = lex::lex(sql, &ctx.lex).map_err(|e| NameError::Unsupported(e.to_string()))?;
    let mut out = walker::walk(sql, &tokens, ctx, depth)?;
    if ctx.engine != Engine::Lexer {
        ast::apply(sql, &tokens, ctx, &mut out)?;
    }
    if let Some(v) = ctx.backend_version {
        for at in lex::versioned::neutralize_edits(sql, &tokens, v) {
            out.edits.push(Edit {
                range: at..at + 1,
                kind: EditKind::NeutralizedComment,
                replacement: " ".to_owned(),
            });
        }
    }
    out.edits.sort_by_key(|e| e.range.start);
    let sql_out = apply_edits(sql, &out.edits);
    Ok(Rewritten {
        sql: sql_out,
        edits: out.edits,
        statements: out.statements,
    })
}

/// Applies non-overlapping edits, sorted by start, to `src`.
fn apply_edits(src: &[u8], edits: &[Edit]) -> Vec<u8> {
    let extra: usize = edits.iter().map(|e| e.replacement.len()).sum();
    let mut out = Vec::with_capacity(src.len() + extra);
    let mut at = 0;
    for e in edits {
        debug_assert!(e.range.start >= at, "edits overlap");
        out.extend_from_slice(&src[at..e.range.start]);
        out.extend_from_slice(e.replacement.as_bytes());
        at = e.range.end;
    }
    out.extend_from_slice(&src[at..]);
    out
}

/// The reverse of [`rewrite`] for the schema names: every identifier
/// token that is one of the tenant's physical schemas becomes its logical
/// name, backquoted. Used for text that comes back from MariaDB with
/// physical names in it (view bodies, `SHOW CREATE` output) and to check
/// round trips. Physical names of other tenants and the tenant catalog are
/// left alone (output mapping, step 7, replaces those with `?`).
///
/// String literals are untouched, as in [`rewrite`].
#[must_use]
pub fn unrewrite(sql: &[u8], map: &dyn NameMap, lex_opts: &LexOptions) -> Vec<u8> {
    let Ok(tokens) = lex::lex(sql, lex_opts) else {
        return sql.to_vec();
    };
    let mut edits = Vec::new();
    for t in &tokens {
        if !t.kind.is_ident_like() {
            continue;
        }
        let v = lex::ident_value(t.bytes(sql));
        let Ok(name) = std::str::from_utf8(&v) else {
            continue;
        };
        if let Some(db) = map.logical(name) {
            let schema: SchemaName = name.parse().expect("logical() parsed it");
            edits.push(Edit {
                range: t.range(),
                kind: EditKind::Schema {
                    logical: db.name.clone(),
                    schema,
                },
                replacement: super::map::quote(&db.name),
            });
        }
    }
    apply_edits(sql, &edits)
}

#[cfg(test)]
mod tests;
