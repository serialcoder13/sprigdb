//! What the gateway does with a client's text (chapter 07, "Flows"): the
//! statement rewrite, then per statement the `SHOW` handling, the
//! `information_schema` inline views and `CREATE TABLE ... LIKE`, each
//! ending in a [`StmtPlan`] with its [`Action`].
//!
//! ```text
//! text ─► rewrite ─► per statement ─┬─ SHOW ───────────► Forward(sql, show mapper) | Local | ERR
//!                                   ├─ information_schema ─► Forward(inline views, scrub)
//!                                   ├─ CREATE TABLE LIKE ─► CreateLike(SHOW CREATE TABLE, head)
//!                                   └─ anything else ────► Forward(sql, relay)
//! ```
//!
//! The gateway sends a forwarded statement and passes its result through
//! the statement's [`ResultMapper`]; a local answer is encoded as is.

use std::ops::Range;

use sprig_proto_mysql::packets::Persona;

use super::NameError;
use super::hide::Visibility;
use super::infoschema::{
    ColumnRegistry, InfoSchemaCtx, classify_table, rewrite_information_schema,
};
use super::map::{LogicalDb, Purpose, quote};
use super::result::{LocalAnswer, ResultMapper};
use super::rewrite::{
    Edit, RewriteCtx, Rewritten, StatementInfo, StmtKind, SystemSchema, UseTarget, rewrite,
    sql_string,
};
use super::show::{
    SessionSource, ShowInfo, ShowKind, classify, database_names, databases_column, show_databases,
    show_processlist,
};
use crate::lex::{self, Token, TokenKind, ident_value};

/// Everything a plan depends on.
#[derive(Clone, Copy)]
pub struct PlanCtx<'a> {
    /// The statement rewrite's context: names, persona, session.
    pub rewrite: RewriteCtx<'a>,
    /// What the session may see of internal objects.
    pub vis: Visibility,
    /// `information_schema` column lists for the backend version.
    pub registry: &'a ColumnRegistry,
    /// The tenant's sessions, for `SHOW PROCESSLIST` and
    /// `information_schema.PROCESSLIST`.
    pub sessions: Option<&'a dyn SessionSource>,
    /// Which databases the user may see (chapter 29); all without it.
    pub visible: Option<&'a (dyn Fn(&LogicalDb) -> bool + Sync)>,
    /// The session's cluster has chapter 07's name lookup (step 12): see
    /// [`InfoSchemaCtx::stored_names`]. Also maps caught error text in the
    /// client's own statements ([`super::stored::top_level`]).
    pub stored_names: bool,
    /// The session's `sql_mode` has `ORACLE`, where `SQLERRM` is the
    /// caught error's text.
    pub oracle: bool,
}

impl std::fmt::Debug for PlanCtx<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlanCtx")
            .field("rewrite", &self.rewrite)
            .field("vis", &self.vis)
            .field("sessions", &self.sessions.is_some())
            .finish_non_exhaustive()
    }
}

impl<'a> PlanCtx<'a> {
    /// A context with the persona's default visibility.
    #[must_use]
    pub fn new(rewrite: RewriteCtx<'a>, registry: &'a ColumnRegistry) -> Self {
        Self {
            vis: Visibility::for_persona(rewrite.lex.persona),
            rewrite,
            registry,
            sessions: None,
            visible: None,
            stored_names: false,
            oracle: false,
        }
    }

    /// With `sql_mode=ORACLE` on or off.
    #[must_use]
    pub fn oracle(mut self, on: bool) -> Self {
        self.oracle = on;
        self
    }

    /// With chapter 07's name lookup on the session's cluster.
    #[must_use]
    pub fn stored_names(mut self, on: bool) -> Self {
        self.stored_names = on;
        self
    }

    /// With a visibility.
    #[must_use]
    pub fn vis(mut self, vis: Visibility) -> Self {
        self.vis = vis;
        self
    }

    /// With the tenant's sessions.
    #[must_use]
    pub fn sessions(mut self, sessions: &'a dyn SessionSource) -> Self {
        self.sessions = Some(sessions);
        self
    }

    /// With the databases the user may see (chapter 29): `SHOW DATABASES`
    /// in every form and `information_schema` list only those.
    #[must_use]
    pub fn visible(mut self, f: &'a (dyn Fn(&LogicalDb) -> bool + Sync)) -> Self {
        self.visible = Some(f);
        self
    }
}

/// What to do with one statement.
#[derive(Debug, Clone)]
pub enum Action {
    /// Send `sql` to MariaDB and map its result with `mapper`.
    Forward {
        /// The statement to send.
        sql: Vec<u8>,
        /// The result mapper.
        mapper: ResultMapper,
    },
    /// Answer in the gateway.
    Local(LocalAnswer),
    /// `CREATE TABLE new LIKE old`: run `show_sql` (`SHOW CREATE TABLE` of
    /// the source), then send [`super::hide::create_like_sql`]`(head,
    /// <Create Table value>)` instead of the client's statement.
    CreateLike {
        /// `SHOW CREATE TABLE <old>`, physical.
        show_sql: Vec<u8>,
        /// The client's statement up to `LIKE`, rewritten.
        head: String,
    },
}

/// One statement's plan.
#[derive(Debug, Clone)]
pub struct StmtPlan {
    /// What the rewrite learned about it.
    pub info: StatementInfo,
    /// For a `SHOW`: which one. A `SHOW CREATE PROCEDURE` or `FUNCTION`
    /// may be answered from the catalog's original text instead
    /// ([`super::show::routine_answer`]).
    pub show: Option<ShowInfo>,
    /// What to do.
    pub action: Action,
}

/// The plan for a client's text.
#[derive(Debug, Clone)]
pub struct Plan {
    /// One entry per statement, in order.
    pub statements: Vec<StmtPlan>,
    /// The statement rewrite it started from.
    pub rewritten: Rewritten,
}

impl Plan {
    /// The text to send when every statement is forwarded: the statements
    /// joined by `;`. `None` when any is local or a `CREATE TABLE LIKE`.
    #[must_use]
    pub fn forward_sql(&self) -> Option<Vec<u8>> {
        let mut out = Vec::new();
        for (i, s) in self.statements.iter().enumerate() {
            let Action::Forward { sql, .. } = &s.action else {
                return None;
            };
            if i > 0 {
                out.push(b';');
            }
            out.extend_from_slice(sql);
        }
        Some(out)
    }

    /// The single statement's plan, for a text with one statement.
    #[must_use]
    pub fn single(&self) -> Option<&StmtPlan> {
        match self.statements.as_slice() {
            [one] => Some(one),
            _ => None,
        }
    }
}

/// Where byte `off` of the original text lands after `edits`.
fn map_offset(edits: &[Edit], off: usize) -> usize {
    let mut delta: isize = 0;
    for e in edits {
        if e.range.end > off || (e.range.is_empty() && e.range.start >= off) {
            break;
        }
        delta += e.replacement.len().cast_signed() - e.range.len().cast_signed();
    }
    off.saturating_add_signed(delta)
}

/// Plans the client's text: the statement rewrite, then each statement's
/// action (see the module documentation).
///
/// # Errors
///
/// Any [`NameError`] of [`rewrite`], 1227 for refused `SHOW`s, 1142 for
/// server-wide `information_schema` tables, 39051 for what can't be
/// mapped safely.
pub fn plan(sql: &[u8], ctx: &PlanCtx<'_>) -> Result<Plan, NameError> {
    let rewritten = rewrite(sql, &ctx.rewrite)?;
    let mut current: Option<String> = ctx.rewrite.current_db.map(str::to_owned);
    let mut statements = Vec::with_capacity(rewritten.statements.len());
    for info in &rewritten.statements {
        let r = map_offset(&rewritten.edits, info.range.start)
            ..map_offset(&rewritten.edits, info.range.end);
        let lead = rewritten.sql.get(..r.start).unwrap_or_default();
        let text = rewritten.sql.get(r).unwrap_or_default().to_vec();
        let (show, mut action) = plan_statement(text, info, current.as_deref(), ctx)?;
        if let Action::Forward { mapper, sql } = &mut action {
            mapper.set_persona(ctx.rewrite.lex.persona);
            // Chapter 38's statement comment (`/*sprig h=.. o=..*/`) is
            // kept in front of the gateway's own internal statements, so
            // the change feed reads it from the binlog; a client's
            // leading comments are dropped as before.
            if ctx.rewrite.internal
                && statements.is_empty()
                && let Some(c) = sprig_comment(lead)
            {
                let mut v = c.to_vec();
                v.push(b' ');
                v.extend_from_slice(sql);
                *sql = v;
            }
            // Chapter 07 step 12: caught error text, where the cluster has
            // the mapping function.
            if ctx.stored_names
                && let Some(t) = super::stored::top_level(sql, &ctx.rewrite.lex, ctx.oracle)
            {
                *sql = t;
            }
        }
        match &info.use_target {
            Some(UseTarget::Database { logical, .. }) => current = Some(logical.clone()),
            Some(UseTarget::System(s)) => current = Some(s.as_str().to_owned()),
            None => {}
        }
        statements.push(StmtPlan {
            info: info.clone(),
            show,
            action,
        });
    }
    Ok(Plan {
        statements,
        rewritten,
    })
}

/// The `/*sprig ...*/` comment `lead` starts with, if any.
fn sprig_comment(lead: &[u8]) -> Option<&[u8]> {
    let start = lead.iter().position(|b| !b.is_ascii_whitespace())?;
    let rest = lead.get(start..)?;
    if !rest.starts_with(b"/*sprig ") {
        return None;
    }
    let end = rest.windows(2).position(|w| w == b"*/")?;
    rest.get(..end + 2)
}

fn forward(sql: Vec<u8>, mapper: ResultMapper) -> Action {
    Action::Forward { sql, mapper }
}

/// The first word of a statement, upper case.
fn first_word(tokens: &[Token], sql: &[u8]) -> String {
    tokens
        .iter()
        .find(|t| !t.kind.is_trivia())
        .filter(|t| t.kind == TokenKind::Word)
        .map(|t| String::from_utf8_lossy(t.bytes(sql)).to_ascii_uppercase())
        .unwrap_or_default()
}

#[allow(clippy::too_many_lines)] // one arm per statement kind
fn plan_statement(
    text: Vec<u8>,
    info: &StatementInfo,
    current: Option<&str>,
    ctx: &PlanCtx<'_>,
) -> Result<(Option<ShowInfo>, Action), NameError> {
    let lex_opts = ctx.rewrite.lex;
    let tokens = lex::lex(&text, &lex_opts).map_err(|e| NameError::Unsupported(e.to_string()))?;
    let in_is =
        current.is_some_and(|c| SystemSchema::parse(c) == Some(SystemSchema::InformationSchema));
    match info.kind {
        StmtKind::Show => return plan_show(text, &tokens, current, ctx).map(|(s, a)| (Some(s), a)),
        StmtKind::Describe => {
            // DESCRIBE t is SHOW COLUMNS; EXPLAIN FOR CONNECTION and the
            // like are scrubbed.
            let word = first_word(&tokens, &text);
            // 3306: over the MySQL-shaped COLUMNS provider (chapter 28).
            if lex_opts.persona == Persona::MySql84
                && !in_is
                && let Some(sql) = crate::compat::schema::describe_rewrite(
                    &text,
                    &tokens,
                    lex_opts.no_backslash_escapes,
                    current,
                    ctx.rewrite.map,
                    &|v| sql_string(v, lex_opts.no_backslash_escapes),
                )
            {
                return Ok((
                    None,
                    forward(sql, ResultMapper::show(ShowKind::Columns, ctx.vis)),
                ));
            }
            let mapper = if matches!(word.as_str(), "DESCRIBE" | "DESC" | "EXPLAIN") && !in_is {
                ResultMapper::show(ShowKind::Columns, ctx.vis)
            } else {
                ResultMapper::scrub()
            };
            return Ok((None, forward(text, mapper)));
        }
        StmtKind::Prepare | StmtKind::Compound | StmtKind::Ddl => {
            for r in &info.system_refs {
                if r.schema != SystemSchema::InformationSchema {
                    continue;
                }
                // The AST path doesn't name the object: fail closed where
                // the text can't be scanned (a prepared statement's string,
                // a compound statement's body).
                let pass = r
                    .object
                    .as_deref()
                    .map_or(info.kind == StmtKind::Ddl, |obj| {
                        classify_table(obj) == super::infoschema::TableClass::PassThrough
                    });
                if !pass {
                    let obj = r.object.as_deref().unwrap_or("?");
                    return Err(NameError::Unsupported(format!(
                        "information_schema.{obj} in a prepared statement, view or stored program"
                    )));
                }
            }
            // Chapter 13's reserved index names (step 12).
            if info.kind == StmtKind::Ddl
                && !ctx.rewrite.internal
                && let Some(ch) = super::autoindex::index_changes(&text, &lex_opts)
            {
                super::autoindex::check_reserved(&ch, ctx.vis)?;
            }
            if info.kind == StmtKind::Ddl
                && let Some(action) = create_like(&text, &tokens)
            {
                return Ok((None, action));
            }
        }
        _ => {}
    }
    let is_ctx = InfoSchemaCtx {
        map: ctx.rewrite.map,
        lex: lex_opts,
        vis: ctx.vis,
        registry: ctx.registry,
        sessions: ctx.sessions,
        visible: ctx.visible,
        in_information_schema: in_is,
        user: ctx.rewrite.account.map(|a| (a.user, a.client_host)),
        stored_names: ctx.stored_names,
    };
    if !info.system_refs.is_empty() || in_is {
        if let Some(r) = rewrite_information_schema(&text, info.kind, &is_ctx)? {
            let mapper = if r.mapped {
                ResultMapper::scrub()
            } else {
                ResultMapper::relay()
            };
            return Ok((None, forward(r.sql, mapper)));
        }
        if info
            .system_refs
            .iter()
            .any(|r| r.schema == SystemSchema::InformationSchema)
        {
            // Pass-through tables: account columns are still mapped.
            return Ok((None, forward(text, ResultMapper::scrub())));
        }
    }
    // Statements whose results hold names in values (chapter 07, "Result
    // metadata and error messages").
    let word = first_word(&tokens, &text);
    let mapper = if matches!(
        word.as_str(),
        "CHECK" | "ANALYZE" | "OPTIMIZE" | "REPAIR" | "CHECKSUM" | "EXPLAIN" | "DESCRIBE" | "DESC"
    ) {
        ResultMapper::scrub()
    } else {
        ResultMapper::relay()
    };
    Ok((None, forward(text, mapper)))
}

/// `CREATE [TEMPORARY] TABLE [IF NOT EXISTS] new [(] LIKE old [)]`.
fn create_like(text: &[u8], tokens: &[Token]) -> Option<Action> {
    let t: Vec<&Token> = tokens.iter().filter(|t| !t.kind.is_trivia()).collect();
    let word = |j: usize, w: &str| {
        t.get(j).is_some_and(|tk| {
            tk.kind == TokenKind::Word && tk.bytes(text).eq_ignore_ascii_case(w.as_bytes())
        })
    };
    let punct = |j: usize, c: u8| {
        t.get(j)
            .is_some_and(|tk| tk.kind == TokenKind::Punct && text[tk.start as usize] == c)
    };
    let mut j = 1;
    if !word(0, "CREATE") {
        return None;
    }
    if word(j, "OR") && word(j + 1, "REPLACE") {
        j += 2;
    }
    if word(j, "TEMPORARY") {
        j += 1;
    }
    if !word(j, "TABLE") {
        return None;
    }
    j += 1;
    if word(j, "IF") && word(j + 1, "NOT") && word(j + 2, "EXISTS") {
        j += 3;
    }
    // The new table's name: one or three tokens.
    j += if punct(j + 1, b'.') { 3 } else { 1 };
    let head_end = t.get(j)?.start as usize;
    let paren = punct(j, b'(');
    let k = if paren { j + 1 } else { j };
    if !word(k, "LIKE") {
        return None;
    }
    let src_first = k + 1;
    let src_last = if punct(src_first + 1, b'.') {
        src_first + 2
    } else {
        src_first
    };
    let rest = src_last + 1 + usize::from(paren && punct(src_last + 1, b')'));
    if rest < t.len() && !punct(rest, b';') {
        return None;
    }
    let src = &text[t.get(src_first)?.start as usize..t.get(src_last)?.end as usize];
    let mut show_sql = b"SHOW CREATE TABLE ".to_vec();
    show_sql.extend_from_slice(src);
    Some(Action::CreateLike {
        show_sql,
        head: String::from_utf8_lossy(&text[..head_end])
            .trim_end()
            .to_owned(),
    })
}

fn plan_show(
    mut text: Vec<u8>,
    tokens: &[Token],
    current: Option<&str>,
    ctx: &PlanCtx<'_>,
) -> Result<(ShowInfo, Action), NameError> {
    let nbe = ctx.rewrite.lex.no_backslash_escapes;
    let Some(info) = classify(&text, tokens, nbe) else {
        return Err(NameError::Unsupported("a SHOW statement".into()));
    };
    let persona = ctx.rewrite.lex.persona;
    let action = match info.kind {
        ShowKind::Databases => {
            let names = database_names(ctx.rewrite.map, persona, ctx.visible);
            match &info.filter {
                None => Action::Local(show_databases(
                    &names,
                    info.like.as_deref(),
                    ctx.rewrite.lower_case_table_names,
                )),
                Some(f) => databases_where(&names, &text[f.clone()], nbe),
            }
        }
        ShowKind::Processlist => {
            let sessions = ctx.sessions.ok_or_else(|| {
                NameError::Unsupported("SHOW PROCESSLIST without the gateway's sessions".into())
            })?;
            Action::Local(show_processlist(&sessions.processes(), info.full, persona))
        }
        ShowKind::Denied => {
            return Err(NameError::Refused {
                errno: 1227,
                sqlstate: *b"42000",
                message: "Access denied; you need (at least one of) the SUPER, REPLICATION \
                          CLIENT privilege(s) for this operation"
                    .to_owned(),
            });
        }
        // 3306: MySQL's engines (gaps row 48; chapter 28, R4).
        ShowKind::Other
            if persona == Persona::MySql84 && crate::compat::schema::is_show_engines(&text) =>
        {
            forward(
                crate::compat::schema::show_engines_sql(&|v| sql_string(v, nbe)),
                ResultMapper::scrub(),
            )
        }
        // 3306: MySQL's collations (gaps row 54; chapter 28, R4).
        ShowKind::Other
            if persona == Persona::MySql84
                && crate::compat::schema::show_collation_sql(&text, &|v| sql_string(v, nbe))
                    .is_some() =>
        {
            let sql = crate::compat::schema::show_collation_sql(&text, &|v| sql_string(v, nbe))
                .unwrap_or_default();
            forward(sql, ResultMapper::scrub())
        }
        ShowKind::Grants | ShowKind::Other => forward(text, ResultMapper::scrub()),
        kind => {
            // R1: a database whose tables the advisor grouped lists them
            // from every group's schema (chapter 07, `grouped`).
            if let Some(a) = super::grouped::listing(&info, &text, tokens, current, ctx)? {
                return Ok((info, a));
            }
            if kind == ShowKind::Tables
                && let Some(f) = info.filter.clone()
            {
                text = tables_in_where(text, f, info.schema.as_deref(), current, ctx);
            }
            if matches!(
                kind,
                ShowKind::ProcedureStatus
                    | ShowKind::FunctionStatus
                    | ShowKind::PackageStatus
                    | ShowKind::Events
                    | ShowKind::OpenTables
            ) && let Some(f) = info.filter.clone()
            {
                text = schemas_in_where(text, f, ctx);
            }
            // 3306: SHOW [FULL] COLUMNS over the MySQL-shaped COLUMNS
            // provider (chapter 28).
            if kind == ShowKind::Columns
                && persona == Persona::MySql84
                && let Some(sql) = crate::compat::schema::show_columns_rewrite(
                    &info,
                    &text,
                    current,
                    ctx.rewrite.map,
                    &|v| sql_string(v, nbe),
                )
            {
                text = sql;
            }
            // 3306: SHOW INDEX in MySQL 8.4's shape (`Visible`,
            // `Expression`; chapter 28, R4).
            if kind == ShowKind::Index
                && persona == Persona::MySql84
                && let Some(sql) = crate::compat::schema::show_index_rewrite(
                    &info,
                    &text,
                    current,
                    ctx.rewrite.map,
                    &|v| sql_string(v, nbe),
                )
            {
                text = sql;
            }
            forward(text, ResultMapper::show(kind, ctx.vis))
        }
    };
    Ok((info, action))
}

/// `SHOW DATABASES WHERE expr`: MariaDB evaluates the client's expression
/// over the tenant's names inlined as a derived table.
fn databases_where(names: &[String], filter: &[u8], nbe: bool) -> Action {
    let mut sql = b"SELECT `Database` FROM (".to_vec();
    for (i, n) in names.iter().enumerate() {
        if i > 0 {
            sql.extend_from_slice(b" UNION ALL ");
        }
        sql.extend_from_slice(b"SELECT ");
        sql.extend_from_slice(sql_string(n, nbe).as_bytes());
        sql.extend_from_slice(b" AS `Database`");
    }
    sql.extend_from_slice(b") AS `SCHEMATA` ");
    sql.extend_from_slice(filter);
    sql.extend_from_slice(b" ORDER BY `Database`");
    let mapper = ResultMapper::scrub().with_columns(vec![databases_column(None)]);
    Action::Forward { sql, mapper }
}

/// `SHOW PROCEDURE STATUS WHERE Db = 'shop'` (as dump tools list
/// routines) and the other `SHOW`s whose rows name a database: MariaDB
/// evaluates the filter over physical names, so a string literal in it that
/// is one of the tenant's databases becomes that database's schema. The
/// rows' `Db` column is mapped back on the way out. Patterns (`LIKE 'sh%'`)
/// are left alone.
fn schemas_in_where(text: Vec<u8>, filter: Range<usize>, ctx: &PlanCtx<'_>) -> Vec<u8> {
    let map = ctx.rewrite.map;
    let nbe = ctx.rewrite.lex.no_backslash_escapes;
    let Ok(tokens) = lex::lex(&text, &ctx.rewrite.lex) else {
        return text;
    };
    let mut out = Vec::with_capacity(text.len() + 16);
    let mut at = 0;
    for t in tokens
        .iter()
        .filter(|t| t.kind == TokenKind::String && t.start as usize >= filter.start)
    {
        let Some(v) = lex::string_value(t.bytes(&text), nbe) else {
            continue;
        };
        let name = String::from_utf8_lossy(&v);
        let key = if ctx.rewrite.lower_case_table_names {
            name.to_lowercase()
        } else {
            name.into_owned()
        };
        let Some(schema) = map
            .physical(&key, Purpose::Metadata)
            .ok()
            .and_then(|s| s.0.first().copied())
        else {
            continue;
        };
        out.extend_from_slice(&text[at..t.start as usize]);
        out.extend_from_slice(sql_string(&schema.to_string(), nbe).as_bytes());
        at = t.end as usize;
    }
    out.extend_from_slice(&text[at..]);
    out
}

/// `SHOW TABLES ... WHERE`: the client names the column
/// `Tables_in_<logical>`; MariaDB calls it `Tables_in_<physical>`.
fn tables_in_where(
    text: Vec<u8>,
    filter: Range<usize>,
    schema: Option<&str>,
    current: Option<&str>,
    ctx: &PlanCtx<'_>,
) -> Vec<u8> {
    let map = ctx.rewrite.map;
    let (logical, physical) = if let Some(p) = schema {
        let Some(db) = map.logical(p) else {
            return text;
        };
        (db.name.clone(), p.to_owned())
    } else {
        let Some(cur) = current else { return text };
        let Some(phys) = map
            .physical(cur, Purpose::Metadata)
            .ok()
            .and_then(|s| s.0.first().copied())
        else {
            return text;
        };
        (cur.to_owned(), phys.to_string())
    };
    let want = format!("Tables_in_{logical}");
    let Ok(tokens) = lex::lex(&text, &ctx.rewrite.lex) else {
        return text;
    };
    let mut out = Vec::with_capacity(text.len() + 16);
    let mut at = 0;
    for t in tokens
        .iter()
        .filter(|t| t.kind.is_ident_like() && t.start as usize >= filter.start)
    {
        let v = ident_value(t.bytes(&text));
        if String::from_utf8_lossy(&v).eq_ignore_ascii_case(&want) {
            out.extend_from_slice(&text[at..t.start as usize]);
            out.extend_from_slice(quote(&format!("Tables_in_{physical}")).as_bytes());
            at = t.end as usize;
        }
    }
    out.extend_from_slice(&text[at..]);
    out
}

#[cfg(test)]
mod tests;
