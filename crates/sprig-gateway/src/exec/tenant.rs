//! Tenant sessions in the executor (milestone M0.3): every client text
//! goes through chapter 07's plan before it reaches MariaDB, DDL goes
//! through the catalog (chapter 08 step 10), settings statements through
//! the catalog's writer (step 8).
//!
//! Per statement of a `COM_QUERY` text (split as in M0.2):
//!
//! ```text
//! sandbox (password expired)? only SET and password changes, else 1820
//! SPRIG SET / RESET / BOUNDS / SHOW SPRIG SETTINGS?  → the catalog writer, answered here
//! snapshot = cache.get(Cached { min_version: last_change })
//! plan = names::plan(text)                      1049: one writer reload, plan again
//! per statement:
//!   SHOW WARNINGS after a gateway answer        → the gateway's warnings
//!   account statement                           → own password change, else 1227
//!   DDL with a target                           → DdlEngine (temporary objects: forwarded)
//!   DDL without one (not nested)                → 39051, never forwarded
//!   Local                                       → the answer, in the session's charset
//!   Forward                                     → rewritten SQL; ResultMapper; messages mapped
//! ```
//!
//! Prepared statements are planned at prepare time: a forwarded statement
//! is prepared with its rewritten text and its result metadata mapped; one
//! the gateway answers itself (DDL, settings, local answers) is kept as
//! text and planned again at execute time, and may not take parameters
//! (39051).
//!
//! `USE` and `COM_INIT_DB` map the logical name to its schema (1049 for a
//! name the tenant doesn't have, a physical name included); the session's
//! current logical database follows the backend session's tracked schema,
//! and goes back to the front end in [`ExecSummary::db`] and to clients in
//! `OK` session tracking (mapped by the relay).

use std::sync::Arc;

use bytes::Bytes;
use sprig_catalog::ddl::{DdlRequest, DdlStatement, MariaExecutor};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::command::QueryAttrs;
use sprig_proto_mysql::packets::{OkPacket, SessionChange};
use sprig_proto_mysql::{ColumnDef, Persona, StatusFlags, Value};
use sprig_sql::lex::{LexOptions, TokenKind};
use sprig_sql::names::{
    AccountInfo, Action, DdlKind, DdlName, DdlTarget, EditKind, Engine, LocalAnswer, NameError,
    NameMap, OutputCtx, PlanCtx, Purpose, ResultMapper, RewriteCtx, ShowKind, StmtKind, StmtPlan,
    SystemSchema, Visibility, create_like_sql, plan,
};
use sprig_sql::settings_stmt;

use super::relay::{Infile, NamesOut, Relay, map_error, relay};
use super::stmt::Stmt;
use super::{PassSession, local_infile_disabled, multi_statements_off, split};
use crate::backend::StmtInfo;
use crate::exec::settings::LOCAL_INFILE;
use crate::pipeline::{
    EndOfRows, ExecCtx, ExecSummary, PreparedMeta, ResultSink, RowFormat, RowValues,
};
use crate::tenant::BACKEND_VERSION;
use crate::tenant::ddl::GatewayDdl;
use crate::tenant::names::{Accounts, TenantSessions};
use crate::tenant::password;
use crate::tenant::session::LocalWarning;
use crate::tenant::settings_sql::{self, SettingsAnswer};

/// How one statement ended.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Step {
    /// An ERR went to the client: the rest of the text is skipped.
    pub errored: bool,
    /// The final packet's status.
    pub status: Option<StatusFlags>,
}

/// 1820: the password expired and must be changed first (sandbox mode).
fn must_reset() -> SprigError {
    SprigError::standard(
        1820,
        *b"HY000",
        "You must reset your password using ALTER USER statement before executing this statement.",
    )
}

/// Account statements other than one's own password change, until
/// chapter 29: never forwarded (MariaDB's accounts are backend accounts).
fn account_refused() -> SprigError {
    SprigError::standard(
        1227,
        *b"42000",
        "Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation",
    )
}

/// 39051 for DDL the catalog can't route: never forwarded.
fn ddl_unsupported() -> SprigError {
    SprigError::new(
        SprigCode::NameRewriteUnsupported,
        "name rewrite unsupported: a DDL statement of a shape the catalog doesn't handle",
    )
}

/// The `SHOW WARNINGS` columns.
fn warning_columns(charset: u16) -> Vec<ColumnDef> {
    vec![
        crate::pipeline::text_column("Level", charset),
        crate::pipeline::int_column("Code", true),
        crate::pipeline::text_column("Message", charset),
    ]
}

fn warnings_answer(w: &[LocalWarning], charset: u16, errors_only: bool) -> LocalAnswer {
    LocalAnswer {
        columns: warning_columns(charset),
        rows: w
            .iter()
            .filter(|w| !errors_only || w.level == "Error")
            .map(|w| {
                vec![
                    Some(Bytes::from_static(w.level.as_bytes())),
                    Some(Bytes::from(w.code.to_string())),
                    Some(Bytes::from(w.message.clone())),
                ]
            })
            .collect(),
    }
}

/// A text row as binary values for `columns` (a local answer to a
/// prepared statement).
fn binary_row(columns: &[ColumnDef], row: &[Option<Bytes>]) -> Vec<Value> {
    use sprig_proto_mysql::resultset::column_type as ct;
    row.iter()
        .zip(columns)
        .map(|(v, c)| match v {
            None => Value::Null,
            Some(b) => match c.col_type {
                ct::LONGLONG | ct::LONG | ct::INT24 | ct::SHORT | ct::TINY => {
                    let s = String::from_utf8_lossy(b);
                    s.parse::<i64>()
                        .map(Value::Int)
                        .or_else(|_| s.parse::<u64>().map(Value::UInt))
                        .unwrap_or_else(|_| Value::Bytes(b.clone()))
                }
                _ => Value::Bytes(b.clone()),
            },
        })
        .collect()
}

/// Sends a local answer.
async fn send_local(
    out: &mut dyn ResultSink,
    ans: &LocalAnswer,
    format: RowFormat,
    status: StatusFlags,
) -> Result<()> {
    out.begin_rows(&ans.columns, format).await?;
    for r in &ans.rows {
        match format {
            RowFormat::Text => {
                let vals: Vec<Option<&[u8]>> = r.iter().map(Option::as_deref).collect();
                out.row(RowValues::Text(&vals)).await?;
            }
            RowFormat::Binary => {
                let vals = binary_row(&ans.columns, r);
                out.row(RowValues::Binary(&vals)).await?;
            }
        }
    }
    out.end_rows(EndOfRows {
        status,
        warnings: 0,
    })
    .await
}

/// The query attributes as the rewrite takes them.
fn attr_pairs(attrs: &QueryAttrs) -> Vec<(String, Option<String>)> {
    use sprig_proto_mysql::command::ParamValue;
    attrs
        .attrs
        .iter()
        .map(|a| {
            let name = String::from_utf8_lossy(&a.name).into_owned();
            let v = match &a.value {
                ParamValue::Value(Value::Null) | ParamValue::LongData => None,
                ParamValue::Value(Value::Bytes(b)) => Some(String::from_utf8_lossy(b).into_owned()),
                ParamValue::Value(Value::Int(i)) => Some(i.to_string()),
                ParamValue::Value(Value::UInt(u)) => Some(u.to_string()),
                ParamValue::Value(v) => Some(format!("{v:?}")),
            };
            (name, v)
        })
        .collect()
}

/// The names a DDL statement's temporary-table check looks at: a plain
/// `ALTER TABLE`, `DROP TABLE`, `TRUNCATE` or `RENAME TABLE` naming one of
/// the session's temporary tables runs on the backend (MariaDB resolves a
/// temporary table first).
fn temp_candidates(t: &DdlTarget) -> Vec<&DdlName> {
    match t {
        DdlTarget::AlterObject {
            kind: DdlKind::Table,
            name,
            ..
        }
        | DdlTarget::Truncate { name } => vec![name],
        DdlTarget::DropObjects {
            kind: DdlKind::Table,
            names,
            ..
        } => names.iter().collect(),
        DdlTarget::RenameTables { pairs, .. } => pairs.iter().map(|(a, _)| a).collect(),
        _ => Vec::new(),
    }
}

/// A gateway-made error with every physical name in it as `?`: such a
/// name can only be one the client typed (an unknown database `t1_d2_g2_s2`
/// is 1049 like any unknown name), and the reply must not echo it.
pub(super) fn scrub_typed(mut e: SprigError) -> SprigError {
    let none = sprig_sql::names::MemoryNameMap::new(sprig_common::ids::TenantId(0));
    if let std::borrow::Cow::Owned(m) =
        sprig_sql::names::map_message(&e.message, &OutputCtx::new(&none))
    {
        e.message = m;
    }
    e
}

/// `DEFINER = <account>` in a view, routine, trigger or event as
/// `DEFINER = CURRENT_USER` (chapter 07, "Account names", the input side,
/// interim): the account a tenant names is its own user (as output shows
/// it, and as a dump restores it), which is no MariaDB account; until
/// chapter 29's per-user backend accounts, every definer is the account the
/// statement runs as.
fn definer_as_current_user(sql: &[u8], lex: LexOptions) -> Vec<u8> {
    let Ok(tokens) = sprig_sql::lex::lex(sql, &lex) else {
        return sql.to_vec();
    };
    let t: Vec<_> = tokens.iter().filter(|t| !t.kind.is_trivia()).collect();
    let mut out = Vec::with_capacity(sql.len());
    let mut copied = 0;
    let mut i = 0;
    while i < t.len() {
        let is_definer = t[i].kind == TokenKind::Word
            && t[i].bytes(sql).eq_ignore_ascii_case(b"DEFINER")
            && t.get(i + 1).is_some_and(|e| e.bytes(sql) == b"=");
        if !is_definer {
            i += 1;
            continue;
        }
        let Some(user) = t.get(i + 2) else { break };
        let mut last = i + 2;
        let current =
            user.kind == TokenKind::Word && user.bytes(sql).eq_ignore_ascii_case(b"CURRENT_USER");
        if current {
            if t.get(last + 1).is_some_and(|p| p.bytes(sql) == b"(")
                && t.get(last + 2).is_some_and(|p| p.bytes(sql) == b")")
            {
                last += 2;
            }
        } else if matches!(
            user.kind,
            TokenKind::Word | TokenKind::QuotedIdent | TokenKind::String
        ) {
            match t.get(last + 1) {
                // `'u'@'h'`, `` `u`@`h` ``: the host lexes as a user variable.
                Some(h) if h.kind == TokenKind::UserVar => last += 1,
                Some(at) if at.bytes(sql) == b"@" && t.get(last + 2).is_some() => last += 2,
                _ => {}
            }
        } else {
            i += 1;
            continue;
        }
        out.extend_from_slice(&sql[copied..t[i].start as usize]);
        out.extend_from_slice(b"DEFINER=CURRENT_USER");
        copied = t[last].end as usize;
        i = last + 1;
    }
    out.extend_from_slice(&sql[copied..]);
    out
}

/// MySQL 8.4 session variables MariaDB lacks, which tools set and that
/// change nothing SprigDB serves.
const MYSQL_ONLY_VARS: &[&str] = &["information_schema_stats_expiry"];

/// Whether `text` is `SET [SESSION] v = literal [, ...]` of
/// [`MYSQL_ONLY_VARS`] only.
fn mysql_only_set(text: &[u8], lex: LexOptions) -> bool {
    let Ok(tokens) = sprig_sql::lex::lex(text, &lex) else {
        return false;
    };
    let t: Vec<_> = tokens.iter().filter(|t| !t.kind.is_trivia()).collect();
    let word = |i: usize, w: &str| {
        t.get(i).is_some_and(|tk| {
            tk.kind == TokenKind::Word && tk.bytes(text).eq_ignore_ascii_case(w.as_bytes())
        })
    };
    if !word(0, "SET") {
        return false;
    }
    let mut i = 1;
    loop {
        if word(i, "SESSION") || word(i, "LOCAL") {
            i += 1;
        }
        let Some(name) = t.get(i) else { return false };
        let n = String::from_utf8_lossy(name.bytes(text)).to_ascii_lowercase();
        let n = n
            .trim_start_matches("@@session.")
            .trim_start_matches("@@local.")
            .trim_start_matches("@@");
        if !MYSQL_ONLY_VARS.contains(&n) {
            return false;
        }
        if t.get(i + 1).is_none_or(|e| e.bytes(text) != b"=") {
            return false;
        }
        if t.get(i + 2).is_none_or(|v| {
            !matches!(
                v.kind,
                TokenKind::Number | TokenKind::String | TokenKind::Word
            )
        }) {
            return false;
        }
        i += 3;
        match t.get(i) {
            None => return true,
            Some(c) if c.bytes(text) == b"," => i += 1,
            Some(c) if c.bytes(text) == b";" && i + 1 == t.len() => return true,
            Some(_) => return false,
        }
    }
}

/// The column name MariaDB gives a literal: a string's value, or the
/// literal itself (`NULL`).
fn literal_name(lit: &str) -> String {
    match lit.strip_prefix('\'').and_then(|l| l.strip_suffix('\'')) {
        Some(inner) => inner.replace("''", "'").replace("\\\\", "\\"),
        None => lit.to_owned(),
    }
}

/// DDL kinds: a statement of one without a target is refused (39051).
fn is_ddl_kind(k: StmtKind) -> bool {
    matches!(
        k,
        StmtKind::Ddl | StmtKind::CreateDatabase | StmtKind::DropDatabase | StmtKind::AlterDatabase
    )
}

/// What to do with one planned statement.
enum Route {
    /// Send the SQL, mapping its result.
    Forward(Vec<u8>, ResultMapper),
    /// Already answered.
    Done(Step),
}

impl PassSession {
    /// The lexer options of the session: the persona, and `sql_mode`'s
    /// `ANSI_QUOTES` and `NO_BACKSLASH_ESCAPES` as the session last set
    /// them.
    fn lex_opts(&self) -> (LexOptions, bool) {
        let mut lex = LexOptions::for_persona(self.persona);
        let mut ignore_space = false;
        if let Some(mode) = self.bs.as_ref().and_then(|b| b.vars().vars.get("sql_mode")) {
            let m = mode.to_ascii_uppercase();
            lex = lex
                .ansi_quotes(m.contains("ANSI_QUOTES") || m.contains("ANSI,") || m == "ANSI")
                .no_backslash_escapes(m.contains("NO_BACKSLASH_ESCAPES"));
            ignore_space = m.contains("IGNORE_SPACE");
        }
        (lex, ignore_space)
    }

    /// The session's current logical database: the backend session's
    /// tracked schema, mapped (a system schema keeps its name).
    pub(super) fn current_logical(&self) -> Option<String> {
        let ts = self.tenant.as_deref()?;
        let Some(bs) = &self.bs else {
            return ts.current.clone();
        };
        let schema = bs.vars().schema.as_deref()?;
        if let Some(db) = ts.map().logical(schema) {
            return Some(db.name.clone());
        }
        SystemSchema::parse(schema).map(|s| s.as_str().to_owned())
    }

    /// The backend session, opened (for a tenant session) in the schema of
    /// its current logical database (see `PassSession::bs`).
    fn tenant_bs(&mut self, cx: &ExecCtx<'_>) {
        let _ = self.bs(cx);
    }

    /// `sql.local_infile` for the session's tenant and current database.
    fn tenant_local_infile(&self) -> bool {
        let Some(ts) = self.tenant.as_deref() else {
            return self.settings.current().local_infile;
        };
        let Some(cat) = ts.catalog() else {
            return self.settings.current().local_infile;
        };
        let svc = ts.services();
        let mut ctx = cat.setting_ctx(&svc.dc);
        if let Some(db) = self
            .current_logical()
            .and_then(|d| cat.database(&d).map(|r| r.db_id))
        {
            ctx = ctx.with_db(db);
        }
        svc.settings.get(&LOCAL_INFILE, &ctx, Some(&cat.settings))
    }

    /// `COM_QUERY` for a tenant session.
    pub(super) async fn tenant_query(
        &mut self,
        cx: &mut ExecCtx<'_>,
        sql: Bytes,
        attrs: &QueryAttrs,
        out: &mut dyn ResultSink,
    ) -> Result<ExecSummary> {
        let mut pieces = split::split(&sql);
        if pieces.len() > 1 && !cx.proto.multi_statements {
            let second = pieces[1].as_ptr() as usize - sql.as_ptr() as usize;
            return Err(multi_statements_off(&sql[second.min(sql.len())..]));
        }
        if pieces.is_empty() {
            pieces.push(sql);
        }
        let n = pieces.len();
        let mut summary = ExecSummary::default();
        for (i, piece) in pieces.into_iter().enumerate() {
            let step = self
                .tenant_text(cx, &piece, attrs, i + 1 < n, RowFormat::Text, out)
                .await;
            let step = match step {
                Ok(s) => s,
                Err(e) => {
                    summary.db = Some(self.current_logical());
                    return Err(e);
                }
            };
            summary.status = step.status.or(summary.status);
            if step.errored {
                break;
            }
        }
        summary.db = Some(self.current_logical());
        Ok(summary)
    }

    /// One statement (or one compound piece) of a client text.
    #[allow(clippy::too_many_lines)] // the per-statement flow, step by step
    pub(super) async fn tenant_text(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        attrs: &QueryAttrs,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        let (lex, ignore_space) = self.lex_opts();
        let status = |s: StatusFlags| {
            if more {
                s | StatusFlags::MORE_RESULTS_EXISTS
            } else {
                s
            }
        };
        let charset = cx.proto.collation;
        let previous_warnings = self.tenant.as_deref_mut().and_then(|ts| ts.warnings.take());
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Err(SprigError::internal("a tenant statement without a tenant"));
        };
        let sandbox = ts.principal.must_change_password;
        let (user, host) = (ts.principal.user.clone(), ts.principal.host.clone());

        // Own password change (allowed in sandbox mode).
        if let Some(pc) = password::parse(text, &lex, &user, &host) {
            if pc.other_account {
                return self.fail(out, &account_refused()).await;
            }
            let (Some(tenant), Some(uid)) = (ts.principal.tenant, ts.principal.user_id) else {
                return self.fail(out, &account_refused()).await;
            };
            let svc = Arc::clone(ts.services());
            let r = sprig_auth::admin::set_password(
                &svc.catalog,
                tenant,
                uid,
                &pc.password,
                sprig_auth::admin::SetPassword {
                    retain_current: pc.retain_current,
                    ..sprig_auth::admin::SetPassword::default()
                },
                &*svc.clock,
            )
            .await;
            return match r {
                Ok(_) => {
                    if let Some(ts) = self.tenant.as_deref_mut() {
                        ts.principal.must_change_password = false;
                    }
                    let s = status(self.status());
                    out.ok(OkPacket::new(s)).await?;
                    Ok(Step {
                        errored: false,
                        status: Some(s),
                    })
                }
                Err(e) => self.fail(out, &e).await,
            };
        }

        // MySQL-only session variables MariaDB doesn't have (`mysqldump`
        // 8.4 sets them in executable comments): accepted and ignored on
        // the MySQL persona, until chapter 28's emulated variables.
        if self.persona == Persona::MySql84 && mysql_only_set(text, lex) {
            let s = status(self.status());
            out.ok(OkPacket::new(s)).await?;
            return Ok(Step {
                errored: false,
                status: Some(s),
            });
        }

        // Settings statements.
        if let Some(parsed) = settings_stmt::parse(text, &lex) {
            if sandbox {
                return self.fail(out, &must_reset()).await;
            }
            let stmt = match parsed {
                Ok(s) => s,
                Err(e) => return self.fail(out, &e.into()).await,
            };
            let current = self.current_logical();
            let Some(ts) = self.tenant.as_deref_mut() else {
                return Err(SprigError::internal("no tenant"));
            };
            let cat = match ts.refresh().await {
                Ok(c) => c,
                Err(e) => return self.fail(out, &e).await,
            };
            let r = settings_sql::execute(ts, &cat, current.as_deref(), &stmt, charset).await;
            let s = status(self.status());
            return match r {
                Ok(SettingsAnswer::Ok { warnings }) => {
                    let mut ok = OkPacket::new(s);
                    ok.warnings = u16::try_from(warnings.len()).unwrap_or(u16::MAX);
                    if let Some(ts) = self.tenant.as_deref_mut()
                        && !warnings.is_empty()
                    {
                        ts.warnings = Some(warnings);
                    }
                    out.ok(ok).await?;
                    Ok(Step {
                        errored: false,
                        status: Some(s),
                    })
                }
                Ok(SettingsAnswer::Rows(ans)) => {
                    send_local(out, &ans, format, s).await?;
                    Ok(Step {
                        errored: false,
                        status: Some(s),
                    })
                }
                Err(e) => self.fail(out, &e).await,
            };
        }

        // The snapshot and the plan.
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Err(SprigError::internal("no tenant"));
        };
        let cat = match ts.refresh().await {
            Ok(c) => c,
            Err(e) => return self.fail(out, &e).await,
        };
        // SprigDB's own information_schema view (chapter 07, step 11).
        let inlined = settings_sql::inline_view(text, &lex, || {
            settings_sql::view_sql(ts, &cat, lex.no_backslash_escapes)
        });
        let owned;
        let text: &Bytes = match inlined {
            None => text,
            Some(Ok(t)) => {
                owned = Bytes::from(t);
                &owned
            }
            Some(Err(e)) => return self.fail(out, &e).await,
        };
        let attr_owned = attr_pairs(attrs);
        let mut retried = false;
        let planned = loop {
            let current = self.current_logical();
            let Some(ts) = self.tenant.as_deref() else {
                return Err(SprigError::internal("no tenant"));
            };
            let map = ts.map();
            let r = {
                let attr_refs: Vec<(&str, Option<&str>)> = attr_owned
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_deref()))
                    .collect();
                let client_host = ts.principal.client_ip.to_string();
                let sessions = TenantSessions {
                    table: &ts.services().sessions,
                    tenant: ts.tenant,
                    user: &ts.principal.user,
                };
                let mut rw = RewriteCtx::new(&*map, self.persona)
                    .lex(lex)
                    .current_db(current.as_deref())
                    .account(AccountInfo {
                        user: &ts.principal.user,
                        account_host: &ts.principal.host,
                        client_host: &client_host,
                    })
                    .query_attributes(&attr_refs)
                    .engine(Engine::Lexer)
                    .backend_version(BACKEND_VERSION);
                rw.ignore_space = ignore_space;
                rw.lower_case_table_names = ts.lower_case_table_names();
                let pctx = PlanCtx::new(rw, &ts.services().columns)
                    .vis(Visibility::for_persona(self.persona))
                    .sessions(&sessions);
                plan(text, &pctx)
            };
            match r {
                Ok(p) => break Ok((p, map)),
                Err(NameError::UnknownDatabase(n)) if !retried => {
                    retried = true;
                    let Some(ts) = self.tenant.as_deref_mut() else {
                        return Err(SprigError::internal("no tenant"));
                    };
                    match ts.reload_after_miss().await {
                        Ok(true) => {}
                        Ok(false) => break Err(NameError::UnknownDatabase(n)),
                        Err(e) => return self.fail(out, &e).await,
                    }
                }
                Err(e) => break Err(e),
            }
        };
        let (p, map) = match planned {
            Ok(x) => x,
            Err(e) => return self.fail(out, &e.into()).await,
        };
        self.subst = p
            .rewritten
            .edits
            .iter()
            .filter(|e| {
                matches!(
                    e.kind,
                    EditKind::Database
                        | EditKind::CurrentUser
                        | EditKind::User
                        | EditKind::QueryAttribute
                )
            })
            .filter_map(|e| {
                let to = text.get(e.range.clone())?;
                Some(super::split::Renamed {
                    from: Bytes::from(literal_name(&e.replacement)),
                    to: Bytes::copy_from_slice(to),
                })
            })
            .collect();

        // Several statements in one piece (a compound tail): forwarded as
        // one text, as M0.2 did, when the gateway answers none of them.
        if p.statements.len() > 1 {
            if sandbox {
                return self.fail(out, &must_reset()).await;
            }
            let routable = p.statements.iter().all(|s| {
                matches!(s.action, Action::Forward { .. })
                    && s.info.ddl.is_none()
                    && s.info.kind != StmtKind::Account
                    && (s.info.nested_ddl || !is_ddl_kind(s.info.kind))
            });
            if !routable {
                return self
                    .fail(
                        out,
                        &SprigError::new(
                            SprigCode::NameRewriteUnsupported,
                            "name rewrite unsupported: a compound text mixing statements the \
                             gateway answers itself; send them one by one",
                        ),
                    )
                    .await;
            }
            let Some(sql) = p.forward_sql() else {
                return Err(SprigError::internal("forward_sql of a forwardable plan"));
            };
            let mapper = if p.statements.iter().any(|s| match &s.action {
                Action::Forward { mapper, .. } => mapper.needs_rows(),
                _ => false,
            }) {
                ResultMapper::scrub()
            } else {
                ResultMapper::relay()
            };
            return self
                .forward(cx, Bytes::from(sql), more, format, mapper, &*map, out)
                .await;
        }
        let Some(sp) = p.statements.into_iter().next() else {
            // Nothing but comments: the backend answers it (1065).
            return self
                .forward(
                    cx,
                    text.clone(),
                    more,
                    format,
                    ResultMapper::relay(),
                    &*map,
                    out,
                )
                .await;
        };

        // Sandbox mode: SET only.
        if sandbox && sp.info.kind != StmtKind::Set {
            return self.fail(out, &must_reset()).await;
        }
        // SHOW WARNINGS right after a statement the gateway answered.
        if let (Some(w), Some(show)) = (&previous_warnings, &sp.show)
            && matches!(show.kind, ShowKind::Warnings | ShowKind::Errors)
        {
            let ans = warnings_answer(w, charset, show.kind == ShowKind::Errors);
            if let Some(ts) = self.tenant.as_deref_mut() {
                ts.warnings.clone_from(&previous_warnings);
            }
            let s = status(self.status());
            send_local(out, &ans, format, s).await?;
            return Ok(Step {
                errored: false,
                status: Some(s),
            });
        }
        let original = text
            .get(sp.info.range.clone())
            .map_or_else(|| text.to_vec(), <[u8]>::to_vec);
        let route = self
            .route(cx, sp, &original, more, format, charset, out)
            .await?;
        match route {
            Route::Done(step) => Ok(step),
            Route::Forward(sql, mapper) => {
                self.forward(cx, Bytes::from(sql), more, format, mapper, &*map, out)
                    .await
            }
        }
    }

    /// What a single planned statement does.
    #[allow(clippy::too_many_arguments)]
    async fn route(
        &mut self,
        cx: &mut ExecCtx<'_>,
        sp: StmtPlan,
        original: &[u8],
        more: bool,
        format: RowFormat,
        charset: u16,
        out: &mut dyn ResultSink,
    ) -> Result<Route> {
        let s = {
            let st = self.status();
            if more {
                st | StatusFlags::MORE_RESULTS_EXISTS
            } else {
                st
            }
        };
        if sp.info.kind == StmtKind::Account {
            return Ok(Route::Done(self.fail(out, &account_refused()).await?));
        }
        if let Some(target) = &sp.info.ddl {
            return self.ddl(cx, &sp, target, original, s, out).await;
        }
        if is_ddl_kind(sp.info.kind) && !sp.info.nested_ddl {
            return Ok(Route::Done(self.fail(out, &ddl_unsupported()).await?));
        }
        match sp.action {
            // A routine's results hold what its body computed, in R0 with
            // backend names (`DATABASE()` in the body is the schema):
            // every value is scrubbed.
            Action::Forward { sql, .. } if sp.info.kind == StmtKind::Call => {
                Ok(Route::Forward(sql, ResultMapper::scrub()))
            }
            Action::Forward { sql, mapper } => Ok(Route::Forward(sql, mapper)),
            Action::Local(mut ans) => {
                ans.set_text_charset(charset);
                send_local(out, &ans, format, s).await?;
                Ok(Route::Done(Step {
                    errored: false,
                    status: Some(s),
                }))
            }
            Action::CreateLike { .. } => Ok(Route::Done(self.fail(out, &ddl_unsupported()).await?)),
        }
    }

    /// A DDL statement with a target (see the module docs).
    #[allow(clippy::too_many_lines)]
    async fn ddl(
        &mut self,
        cx: &mut ExecCtx<'_>,
        sp: &StmtPlan,
        target: &DdlTarget,
        original: &[u8],
        status: StatusFlags,
        out: &mut dyn ResultSink,
    ) -> Result<Route> {
        let Some(ts) = self.tenant.as_deref() else {
            return Err(SprigError::internal("no tenant"));
        };
        let forward = |sp: &StmtPlan| match &sp.action {
            Action::Forward { sql, mapper } => Some(Route::Forward(sql.clone(), mapper.clone())),
            _ => None,
        };
        // A plain ALTER, DROP, TRUNCATE or RENAME of one of the session's
        // temporary tables runs where the table lives.
        if temp_candidates(target)
            .iter()
            .any(|n| ts.is_temp(n.db.as_deref(), &n.name))
            && let Some(r) = forward(sp)
        {
            self.track_temp(target);
            return Ok(r);
        }
        let stmt = match target.to_catalog() {
            Ok(Some(s)) => s,
            Ok(None) => {
                // Temporary objects: the backend's, the session pins.
                self.track_temp(target);
                return match forward(sp) {
                    Some(r) => Ok(r),
                    None => Ok(Route::Done(self.fail(out, &ddl_unsupported()).await?)),
                };
            }
            Err(e) => return Ok(Route::Done(self.fail(out, &e.into()).await?)),
        };
        // The physical text.
        let physical = match &sp.action {
            Action::Forward { sql, .. } => {
                let (lex, _) = self.lex_opts();
                String::from_utf8_lossy(&definer_as_current_user(sql, lex)).into_owned()
            }
            Action::CreateLike { show_sql, head } => {
                match self.create_like_text(cx, show_sql, head).await {
                    Ok(t) => t,
                    Err(e) => return Ok(Route::Done(self.fail(out, &e).await?)),
                }
            }
            Action::Local(_) => return Ok(Route::Done(self.fail(out, &ddl_unsupported()).await?)),
        };
        let logical = String::from_utf8_lossy(original).into_owned();
        let current = self.current_logical();
        let Some(ts) = self.tenant.as_deref() else {
            return Err(SprigError::internal("no tenant"));
        };
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        let user = ts.principal.user_id;
        let lctn = ts.lower_case_table_names();
        let map = ts.map();
        let ctl = match svc.locator.ctl_pool(&self.target.cluster).await {
            Ok(p) => MariaExecutor::new(p),
            Err(e) => return Ok(Route::Done(self.fail(out, &e).await?)),
        };
        let req = DdlRequest {
            tenant,
            statement: stmt.clone(),
            sql: &physical,
            logical_sql: &logical,
            current_db: current.as_deref(),
            user,
            lower_case_table_names: lctn,
        };
        self.tenant_bs(cx);
        let Some(bs) = self.bs.as_mut() else {
            return Err(SprigError::internal("no backend session"));
        };
        let result = {
            let exec = GatewayDdl::new(bs, ctl);
            svc.ddl.execute(&req, &exec).await
        };
        match result {
            Ok(outcome) => {
                if let Some(ts) = self.tenant.as_deref_mut() {
                    ts.changed(outcome.version);
                    if !outcome.notes.is_empty() {
                        ts.warnings = Some(
                            outcome
                                .notes
                                .iter()
                                .map(|n| LocalWarning {
                                    level: "Note",
                                    code: n.code,
                                    message: n.message.clone(),
                                })
                                .collect(),
                        );
                    }
                }
                // Dropping the current database leaves the session without
                // one, as on MySQL.
                if let DdlStatement::DropDatabase { name, .. } = &stmt
                    && current.as_deref() == Some(name.as_str())
                    && outcome.version.is_some()
                {
                    if let Some(bs) = self.bs.as_mut() {
                        bs.vars_mut().schema = None;
                    }
                    if let Some(ts) = self.tenant.as_deref_mut() {
                        ts.current = None;
                    }
                }
                let mut ok = OkPacket::new(status);
                ok.affected_rows = outcome.affected_rows;
                ok.warnings = u16::try_from(outcome.notes.len()).unwrap_or(u16::MAX);
                if matches!(stmt, DdlStatement::DropDatabase { .. })
                    && self.current_logical().is_none()
                    && current.is_some()
                {
                    ok.set_session_changes(&[SessionChange::Schema(Bytes::new())]);
                }
                out.ok(ok).await?;
                Ok(Route::Done(Step {
                    errored: false,
                    status: Some(status),
                }))
            }
            Err(e) => {
                let accounts = Accounts(self.tenant.as_deref().and_then(|t| t.auth()));
                let octx = OutputCtx::new(&*map).accounts(&accounts);
                let e = map_error(e, &octx);
                Ok(Route::Done(self.fail(out, &e).await?))
            }
        }
    }

    /// Records the session's temporary tables as a forwarded statement
    /// changes them (`CREATE TEMPORARY TABLE`, `DROP [TEMPORARY] TABLE`,
    /// `RENAME` / `ALTER ... RENAME` of one).
    fn track_temp(&mut self, target: &DdlTarget) {
        let Some(ts) = self.tenant.as_deref_mut() else {
            return;
        };
        let key = |n: &DdlName| (n.db.clone(), n.name.to_ascii_lowercase());
        match target {
            DdlTarget::CreateObject {
                temporary: true,
                kind: DdlKind::Table | DdlKind::Sequence,
                name,
                ..
            } => {
                ts.temp.insert(key(name));
            }
            DdlTarget::DropObjects { names, .. } => {
                for n in names {
                    ts.temp.remove(&key(n));
                }
            }
            DdlTarget::RenameTables { pairs, .. } => {
                for (a, b) in pairs {
                    if ts.temp.remove(&key(a)) {
                        ts.temp.insert(key(b));
                    }
                }
            }
            _ => {}
        }
    }

    /// `CREATE TABLE new LIKE old`: `SHOW CREATE TABLE old` on the session,
    /// then the statement chapter 07 builds from it.
    async fn create_like_text(
        &mut self,
        cx: &mut ExecCtx<'_>,
        show_sql: &[u8],
        head: &str,
    ) -> Result<String> {
        self.tenant_bs(cx);
        let Some(bs) = self.bs.as_mut() else {
            return Err(SprigError::internal("no backend session"));
        };
        bs.query(&StmtInfo::read(), show_sql).await?;
        let mut text: Option<String> = None;
        let mut err = None;
        let mut idx = 1usize;
        let mut seen = 0usize;
        let mut cols = 0usize;
        while let Some(p) = bs.next().await? {
            use sprig_proto_mysql::client::PacketKind;
            match p.kind {
                PacketKind::ColumnCount(n) => cols = usize::try_from(n).unwrap_or(2),
                PacketKind::ColumnDef => {
                    let c = ColumnDef::decode(&p.payload, sprig_proto_mysql::Caps::empty())?;
                    if c.name.eq_ignore_ascii_case(b"Create Table") {
                        idx = seen;
                    }
                    seen += 1;
                }
                PacketKind::Row => {
                    let vals =
                        sprig_proto_mysql::resultset::decode_text_row(&p.payload, cols.max(2))?;
                    if let Some(Some(v)) = vals.get(idx) {
                        text = Some(String::from_utf8_lossy(v).into_owned());
                    }
                }
                PacketKind::Err(e) => {
                    err = Some(SprigError::backend(e.code, e.sqlstate, e.message.clone()));
                }
                _ => {}
            }
        }
        let _ = bs.finish().await;
        if let Some(e) = err {
            let map = self
                .tenant
                .as_deref()
                .map(crate::tenant::session::TenantSession::map);
            return Err(match &map {
                Some(m) => map_error(e, &OutputCtx::new(&**m)),
                None => e,
            });
        }
        let text =
            text.ok_or_else(|| SprigError::internal("SHOW CREATE TABLE returned no text"))?;
        create_like_sql(head, &text).ok_or_else(ddl_unsupported)
    }

    /// Sends `err` as the statement's ERR packet.
    async fn fail(&mut self, out: &mut dyn ResultSink, err: &SprigError) -> Result<Step> {
        let shown = SprigError {
            kind: err.kind,
            message: err.message.clone(),
            retryable: err.retryable,
            source: None,
        };
        out.error(&scrub_typed(shown)).await?;
        Ok(Step {
            errored: true,
            status: None,
        })
    }

    /// Sends `sql` (rewritten) to the backend and relays the response
    /// through `mapper`.
    #[allow(clippy::too_many_arguments)]
    async fn forward(
        &mut self,
        cx: &mut ExecCtx<'_>,
        sql: Bytes,
        more: bool,
        format: RowFormat,
        mut mapper: ResultMapper,
        map: &dyn NameMap,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        let (piece, mut renames) = self.session_rewrites(sql, cx.session.id.0);
        renames.append(&mut self.subst);
        let infile_name = split::local_infile_name(&piece);
        let mut allowed = false;
        if infile_name.is_some() {
            allowed =
                cx.proto.has(sprig_proto_mysql::Caps::LOCAL_FILES) && self.tenant_local_infile();
            if !allowed {
                return Err(local_infile_disabled(self.persona));
            }
        }
        let info = StmtInfo::classify(&piece);
        let conn_status =
            split::is_session_status(&piece).then(|| super::relay::ConnStatus::of(cx.proto));
        let backend_caps = self.backend_caps(cx);
        let mysql = self.persona == Persona::MySql84;
        self.tenant_bs(cx);
        let Some(bs) = self.bs.as_mut() else {
            return Err(SprigError::internal("no backend session"));
        };
        bs.query(&info, &piece).await?;
        let r = Relay {
            more,
            format,
            cursor: false,
            fetch: false,
            backend_caps: bs.backend_caps().unwrap_or(backend_caps),
            infile: Infile {
                name: infile_name.as_deref(),
                allowed,
            },
            conn_status: conn_status.as_ref(),
            renames: &renames,
            mysql_nowait: mysql && split::has_keyword(&piece, "NOWAIT"),
        };
        let accounts = Accounts(self.tenant.as_deref().and_then(|t| t.auth()));
        let aliases = self
            .tenant
            .as_deref()
            .map(crate::tenant::session::TenantSession::aliases);
        let mut names = NamesOut {
            mapper: &mut mapper,
            ctx: OutputCtx {
                map,
                accounts: Some(&accounts),
                lex: LexOptions::mariadb(),
            },
            aliases: aliases.as_ref(),
        };
        let relayed = relay(bs, out, &r, Some(&mut names)).await?;
        let status = self.finish(out.finished()).await?;
        Ok(Step {
            errored: relayed.errored,
            status: status.or(relayed.status),
        })
    }

    /// `USE` at login and `COM_INIT_DB` for a tenant session: the logical
    /// name's schema (1049 for a name the tenant doesn't have, after one
    /// writer reload); a system schema by its own name.
    pub(super) async fn tenant_init_db(&mut self, cx: &mut ExecCtx<'_>, name: &str) -> Result<()> {
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Err(SprigError::internal("no tenant"));
        };
        if ts.principal.must_change_password {
            return Err(must_reset());
        }
        ts.refresh().await?;
        let schema = if let Some(s) = SystemSchema::parse(name) {
            s.as_str().to_owned()
        } else {
            let lookup = |ts: &crate::tenant::session::TenantSession| {
                let key = if ts.lower_case_table_names() {
                    name.to_lowercase()
                } else {
                    name.to_owned()
                };
                ts.map()
                    .physical(&key, Purpose::Session)
                    .ok()
                    .and_then(|s| s.single())
            };
            if let Some(s) = lookup(ts) {
                s.to_string()
            } else {
                let again = ts.reload_after_miss().await?;
                match lookup(ts).filter(|_| again) {
                    Some(s) => s.to_string(),
                    None => {
                        return Err(scrub_typed(SprigError::from(NameError::UnknownDatabase(
                            name.to_owned(),
                        ))));
                    }
                }
            }
        };
        self.init_physical(cx, &schema).await?;
        if let Some(ts) = self.tenant.as_deref_mut() {
            ts.current = Some(name.to_owned());
        }
        Ok(())
    }

    /// `COM_STMT_PREPARE` for a tenant session (see the module docs).
    #[allow(clippy::too_many_lines)] // one branch per kind of answer
    pub(super) async fn tenant_prepare(
        &mut self,
        cx: &mut ExecCtx<'_>,
        sql: Bytes,
    ) -> Result<ExecSummary> {
        let (lex, ignore_space) = self.lex_opts();
        let charset = cx.proto.collation;
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Err(SprigError::internal("no tenant"));
        };
        let params = sprig_sql::lex::lex(&sql, &lex)
            .map(|t| t.iter().filter(|t| t.kind == TokenKind::Param).count())
            .unwrap_or(0);
        let deferred = |cols: Vec<ColumnDef>| -> Result<Option<Vec<ColumnDef>>> {
            if params > 0 {
                return Err(SprigError::new(
                    SprigCode::NameRewriteUnsupported,
                    "name rewrite unsupported: a prepared statement the gateway answers itself \
                     (DDL, settings, a local answer) can't take parameters",
                ));
            }
            Ok(Some(cols))
        };
        let sandbox = ts.principal.must_change_password;
        let mut keep: Option<Vec<ColumnDef>> = None;
        if password::parse(&sql, &lex, &ts.principal.user, &ts.principal.host).is_some() {
            keep = deferred(Vec::new())?;
        } else if let Some(parsed) = settings_stmt::parse(&sql, &lex) {
            if sandbox {
                return Err(must_reset());
            }
            let cols = match parsed? {
                settings_stmt::SettingsStmt::Show { .. } => settings_sql::show_columns(charset),
                settings_stmt::SettingsStmt::Alter { .. } => Vec::new(),
            };
            keep = deferred(cols)?;
        }
        if let Some(columns) = keep {
            return Ok(self.prepared_deferred(sql, columns));
        }
        ts.refresh().await?;
        let current = self.current_logical();
        let Some(ts) = self.tenant.as_deref() else {
            return Err(SprigError::internal("no tenant"));
        };
        let map = ts.map();
        let client_host = ts.principal.client_ip.to_string();
        let mut rw = RewriteCtx::new(&*map, self.persona)
            .lex(lex)
            .current_db(current.as_deref())
            .account(AccountInfo {
                user: &ts.principal.user,
                account_host: &ts.principal.host,
                client_host: &client_host,
            })
            .backend_version(BACKEND_VERSION);
        rw.ignore_space = ignore_space;
        rw.lower_case_table_names = ts.lower_case_table_names();
        let sessions = TenantSessions {
            table: &ts.services().sessions,
            tenant: ts.tenant,
            user: &ts.principal.user,
        };
        let pctx = PlanCtx::new(rw, &ts.services().columns)
            .vis(Visibility::for_persona(self.persona))
            .sessions(&sessions);
        let p = plan(&sql, &pctx)?;
        let [sp] = p.statements.as_slice() else {
            return Err(SprigError::standard(
                1064,
                *b"42000",
                "You have an error in your SQL syntax: a prepared statement holds one statement",
            ));
        };
        if sandbox && sp.info.kind != StmtKind::Set {
            return Err(must_reset());
        }
        if sp.info.kind == StmtKind::Account {
            return Err(account_refused());
        }
        let ddl_here = sp.info.ddl.as_ref().is_some_and(DdlTarget::is_catalog)
            && !temp_candidates(sp.info.ddl.as_ref().unwrap_or(&DdlTarget::Truncate {
                name: DdlName::unqualified("", None),
            }))
            .iter()
            .any(|n| ts.is_temp(n.db.as_deref(), &n.name));
        if ddl_here {
            let cols = deferred(Vec::new())?.unwrap_or_default();
            return Ok(self.prepared_deferred(sql, cols));
        }
        if sp.info.ddl.is_none() && is_ddl_kind(sp.info.kind) && !sp.info.nested_ddl {
            return Err(ddl_unsupported());
        }
        let (rewritten, mut mapper) = match &sp.action {
            Action::Forward { sql, mapper } => (Bytes::from(sql.clone()), mapper.clone()),
            Action::Local(ans) => {
                let mut ans = ans.clone();
                ans.set_text_charset(charset);
                let cols = deferred(ans.columns)?.unwrap_or_default();
                return Ok(self.prepared_deferred(sql, cols));
            }
            Action::CreateLike { .. } => {
                let cols = deferred(Vec::new())?.unwrap_or_default();
                return Ok(self.prepared_deferred(sql, cols));
            }
        };
        self.tenant_bs(cx);
        let mut summary = self.prepare(cx, rewritten).await?;
        let accounts = Accounts(self.tenant.as_deref().and_then(|t| t.auth()));
        let octx = OutputCtx::new(&*map).accounts(&accounts);
        if let Some(meta) = summary.prepared.as_mut() {
            mapper.column_defs(&mut meta.columns, &octx);
            let mut params = meta.params.clone();
            for c in &mut params {
                sprig_sql::names::map_column_def(c, &octx);
            }
            meta.params = params;
            if let Ok(s) = self.stmts.get_mut(meta.stmt) {
                s.mapper = Some(mapper);
                s.columns.clone_from(&meta.columns);
            }
        }
        Ok(summary)
    }

    /// A prepared statement kept as text, answered at execute time.
    fn prepared_deferred(&mut self, sql: Bytes, columns: Vec<ColumnDef>) -> ExecSummary {
        let st = Stmt::deferred(sql, columns.clone());
        let stmt = self.stmts.insert(st);
        metrics::gauge!("sprig_mysql_prepared_statements", "persona" => self.persona.as_str())
            .increment(1.0);
        ExecSummary {
            status: None,
            prepared: Some(PreparedMeta {
                stmt,
                params: Vec::new(),
                columns,
                warnings: 0,
            }),
            db: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mysql_only_variables_are_recognized() {
        let lex = LexOptions::mysql84();
        assert!(mysql_only_set(
            b"/*!80000 SET SESSION information_schema_stats_expiry=0 */",
            lex
        ));
        assert!(mysql_only_set(
            b"SET @@information_schema_stats_expiry = 86400",
            lex
        ));
        assert!(!mysql_only_set(
            b"SET SESSION information_schema_stats_expiry=0, autocommit=1",
            lex
        ));
        assert!(!mysql_only_set(b"SET autocommit = 0", lex));
    }

    #[test]
    fn definers_become_the_running_account() {
        let d = |s: &str| {
            String::from_utf8(definer_as_current_user(s.as_bytes(), LexOptions::mariadb())).unwrap()
        };
        assert_eq!(
            d("CREATE DEFINER=`app`@`%` PROCEDURE p() SELECT 1"),
            "CREATE DEFINER=CURRENT_USER PROCEDURE p() SELECT 1"
        );
        assert_eq!(
            d(
                "/*!50001 CREATE ALGORITHM=UNDEFINED */ /*!50013 DEFINER='app'@'%' SQL SECURITY DEFINER */ /*!50001 VIEW v AS SELECT 1 */"
            ),
            "/*!50001 CREATE ALGORITHM=UNDEFINED */ /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */ /*!50001 VIEW v AS SELECT 1 */"
        );
        assert_eq!(
            d("CREATE DEFINER = CURRENT_USER() VIEW v AS SELECT 1"),
            "CREATE DEFINER=CURRENT_USER VIEW v AS SELECT 1"
        );
        assert_eq!(
            d("CREATE VIEW v AS SELECT 'DEFINER=x'"),
            "CREATE VIEW v AS SELECT 'DEFINER=x'"
        );
    }

    #[test]
    fn literals_name_columns_as_mariadb_does() {
        assert_eq!(literal_name("'app@%'"), "app@%");
        assert_eq!(literal_name("'it''s'"), "it's");
        assert_eq!(literal_name("NULL"), "NULL");
    }
}
