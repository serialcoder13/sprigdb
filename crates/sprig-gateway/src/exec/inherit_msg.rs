//! Inherited messaging objects in a tenant session (chapter 42,
//! "Inherited messaging objects (`FOR EACH TENANT`)"; chapters 45 and 41;
//! R9). Runs before the stream layer ([`PassSession::tenant_streams`]) and
//! chapter 41's declarations:
//!
//! ```text
//! CREATE STREAM s FOR EACH TENANT [scope] ...      a definition: inherited_defs
//!                                                 in this tenant, no tables
//! CREATE [OR REPLACE] TRIGGER g ON STREAM s FOR EACH TENANT [scope]
//!   [GRANTS (...)] LANGUAGE ... AS ...            a trigger definition; existing
//!                                                 instances get it in the background
//! ALTER STREAM s PARTITIONS n | SET (...)          changes the definition
//! DROP STREAM s [IMMEDIATE] | DROP TRIGGER g        drops it (chapter 42, "Dropping")
//! INSERT INTO s ...                  in a member: into its own instance (created
//!                                    on first publish); headers stamped
//! INSERT INTO s FOR TENANT t ...     in an ancestor: into t's instance
//! INSERT INTO s (tenant, key, value) ...           one target per row
//! ALTER INHERITED kind name ...      overrides (nearest level wins)
//! SHOW INHERITED DEFINITIONS | INSTANCES | OVERRIDES [FOR kind name]
//! ```
//!
//! A publish evaluates its rows in this session (`VALUES` rows become
//! `SELECT ... UNION ALL ...`; an `INSERT ... SELECT` runs its `SELECT`),
//! under the user's rights, then writes the instance's outbox: through
//! this session for its own tenant (so it joins the transaction), or as
//! the provisioning account on the member's cluster for another tenant.
//! Every record carries `x-sprig-tenant` (the instance's tenant) and
//! `x-sprig-origin-tenant` (the publisher's), set here over anything the
//! client sent.

// Lexing options are passed by reference, as the lexer takes them.
#![allow(clippy::trivially_copy_pass_by_ref)]

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use mysql_async::prelude::Queryable;
use serde_json::{Map, Value, json};
use sprig_auth::authz::{Action, Target};
use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::ObjectKind;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_inherit::msg::grammar::{self, AlterOp, For, ShowInherited, Stmt};
use sprig_inherit::msg::{
    AppliesTo, Cause, Chain, DefKey, DefKind, DefState, Definition, MsgState, Override, Scope,
    StreamSpec, TriggerSpec, What,
};
use sprig_proto_mysql::StatusFlags;
use sprig_proto_mysql::command::QueryAttrs;
use sprig_proto_mysql::packets::OkPacket;
use sprig_sql::lex::{LexOptions, TokenKind, lex};
use sprig_streams::grammar::InsertInto;
use sprig_streams::publish::Publish;

use super::PassSession;
use super::tenant::Step;
use crate::inherited::{self, MsgWrite, streams, target_err, triggers};
use crate::pipeline::{ExecCtx, ResultSink, RowFormat};
use crate::tenant::TenantServices;
use crate::tenant::session::{LocalWarning, TenantSession};

/// What a statement answers.
enum Reply {
    /// OK, with notes.
    Ok(u64, Vec<String>),
    /// A result set.
    Rows(Vec<&'static str>, Vec<Vec<Option<String>>>),
    /// Run this statement instead, on the ordinary path.
    Forward(String),
}

fn not_built(what: &str) -> SprigError {
    SprigError::standard(
        1235,
        *b"42000",
        format!("This version of SprigDB doesn't yet support {what}"),
    )
}

pub(super) fn denied(ts: &TenantSession, cmd: &str, name: &str) -> SprigError {
    SprigError::standard(
        1142,
        *b"42000",
        format!(
            "{cmd} command denied to user '{}'@'{}' for table '{name}'",
            ts.principal.user, ts.principal.client_ip
        ),
    )
}

/// Whether the session may do `a` on `db.name`: by name, or on the
/// database.
pub(super) fn allows(
    ts: &TenantSession,
    cat: &TenantCatalog,
    a: Action,
    db: &str,
    name: &str,
) -> bool {
    let grants = crate::tenant::authz::privileges(ts, cat);
    if grants.unrestricted() {
        return true;
    }
    let lctn = ts.lower_case_table_names();
    let db_key = database_key(db, lctn);
    let name_key = object_key(ObjectKind::Table, name, lctn);
    grants.allows(
        a,
        Target::Named {
            db_key: &db_key,
            name_key: &name_key,
        },
    ) || grants.allows(a, Target::Database { db_key: &db_key })
}

pub(super) fn tenant_admin(ts: &TenantSession, cat: &TenantCatalog) -> bool {
    let grants = crate::tenant::authz::privileges(ts, cat);
    grants.unrestricted() || grants.allows(Action::TenantAdmin, Target::Tenant)
}

fn first_word(sql: &str, lex_opts: &LexOptions) -> Option<String> {
    let toks = lex(sql.as_bytes(), lex_opts).ok()?;
    let t = toks.iter().find(|t| !t.kind.is_trivia())?;
    (t.kind == TokenKind::Word).then(|| sql[t.range()].to_ascii_lowercase())
}

/// `VALUES (a, b), (c, d)` → `SELECT a, b UNION ALL SELECT c, d`; a
/// `SELECT` (or `WITH`, or a parenthesized query) as it is.
///
/// # Errors
/// 1064 for anything else.
pub(crate) fn rows_query(tail: &str, lex_opts: &LexOptions) -> Result<String> {
    let syntax = || {
        SprigError::standard(
            1064,
            *b"42000",
            format!(
                "You have an error in your SQL syntax near '{}'",
                tail.chars().take(40).collect::<String>()
            ),
        )
    };
    let toks: Vec<_> = lex(tail.as_bytes(), lex_opts)
        .map_err(|_| syntax())?
        .into_iter()
        .filter(|t| !t.kind.is_trivia())
        .collect();
    let Some(first) = toks.first() else {
        return Err(syntax());
    };
    let w = tail[first.range()].to_ascii_lowercase();
    if w == "select" || w == "with" || &tail[first.range()] == "(" {
        return Ok(tail.trim().trim_end_matches(';').to_owned());
    }
    if w != "values" && w != "value" {
        return Err(syntax());
    }
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for t in &toks[1..] {
        let s = &tail[t.range()];
        if t.kind == TokenKind::Punct && s == "(" {
            if depth == 0 {
                start = t.end as usize;
            }
            depth += 1;
        } else if t.kind == TokenKind::Punct && s == ")" {
            depth = depth.checked_sub(1).ok_or_else(syntax)?;
            if depth == 0 {
                out.push(format!("SELECT {}", &tail[start..t.start as usize]));
            }
        } else if depth == 0 && !(t.kind == TokenKind::Punct && (s == "," || s == ";")) {
            return Err(syntax());
        }
    }
    if out.is_empty() || depth != 0 {
        return Err(syntax());
    }
    Ok(out.join(" UNION ALL "))
}

/// Where each column of a publish goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Col {
    Key,
    Value,
    Headers,
    Partition,
    Tenant,
}

/// The column list and the rows' text of an insert.
fn insert_shape(sql: &str, i: &InsertInto) -> Result<(Vec<Col>, String)> {
    let cols = match &i.columns {
        Some(cs) => cs
            .iter()
            .map(|(n, _)| match n.to_ascii_lowercase().as_str() {
                "key" | "k" => Ok(Col::Key),
                "value" | "v" => Ok(Col::Value),
                "headers" => Ok(Col::Headers),
                "partition_no" => Ok(Col::Partition),
                "tenant" => Ok(Col::Tenant),
                "ts" => Err(not_built(
                    "the ts column on a publish into an inherited stream",
                )),
                _ => Err(SprigError::standard(
                    1054,
                    *b"42S22",
                    format!("Unknown column '{n}' in 'field list'"),
                )),
            })
            .collect::<Result<Vec<_>>>()?,
        None => match i.first_row_arity {
            Some(2) => vec![Col::Key, Col::Value],
            _ => vec![Col::Key, Col::Value, Col::Headers],
        },
    };
    let after = match &i.columns {
        Some(cs) => {
            let last = cs.last().map_or(i.after_target, |(_, r)| r.end);
            sql[last..].find(')').map_or(last, |p| last + p + 1)
        }
        None => i.after_target,
    };
    Ok((cols, sql[after..].to_owned()))
}

/// One evaluated row: its target tenant name (the `tenant` column) and its
/// record.
struct Row {
    tenant: Option<String>,
    rec: Publish,
}

fn stamp(headers: Option<&[u8]>, tenant: &str, origin: &str) -> Result<String> {
    let mut m: Map<String, Value> = match headers {
        None => Map::new(),
        Some(b) => match serde_json::from_slice::<Value>(b) {
            Ok(Value::Object(m)) => m,
            Ok(Value::Null) => Map::new(),
            _ => {
                return Err(SprigError::standard(
                    3140,
                    *b"22032",
                    "Invalid JSON text: a record's headers are a JSON object",
                ));
            }
        },
    };
    m.insert("x-sprig-tenant".into(), json!(tenant));
    m.insert("x-sprig-origin-tenant".into(), json!(origin));
    Ok(Value::Object(m).to_string())
}

/// Where a publish's records go.
enum Dest {
    /// This session's own instance: through the session.
    Own,
    /// Another tenant's: as the provisioning account.
    Other,
}

impl PassSession {
    /// Chapter 42's step of [`PassSession::tenant_text`]: `None` when the
    /// statement isn't about inherited messaging objects.
    #[allow(clippy::too_many_lines)]
    pub(super) async fn tenant_inherit_msg(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        attrs: &QueryAttrs,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Ok(None);
        };
        if ts.types.internal || ts.principal.must_change_password {
            return Ok(None);
        }
        let Ok(sql) = std::str::from_utf8(text) else {
            return Ok(None);
        };
        let (lex_opts, _) = self.lex_opts();
        let Some(head) = first_word(sql, &lex_opts) else {
            return Ok(None);
        };
        if !matches!(
            head.as_str(),
            "insert" | "create" | "drop" | "alter" | "show" | "select" | "bind" | "unbind"
        ) {
            return Ok(None);
        }
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Ok(None);
        };
        // The session's own last change included (a CREATE DATABASE just
        // before).
        let cat = ts.refresh().await?;
        let kw = grammar::maybe(text);
        let own = inherited::state_of(&cat);
        if !kw && own.is_empty() && cat.ancestry.is_empty() {
            return Ok(None);
        }
        let svc = Arc::clone(ts.services());
        let chain = match inherited::chain(&svc, &cat).await {
            Ok(c) => c,
            Err(e) if !kw => {
                tracing::debug!(error = %e, "inherited definitions unreadable; statement goes on");
                return Ok(None);
            }
            Err(e) => return Ok(Some(self.fail(out, &e).await?)),
        };
        if !kw && !inherited::has_defs(&chain) && own.instances.is_empty() {
            return Ok(None);
        }
        let r =
            Box::pin(self.inherit_msg(cx, sql, &head, &lex_opts, &svc, &cat, &chain, &own)).await;
        let mut status = self.status();
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        let step = Step {
            errored: false,
            status: Some(status),
        };
        match r {
            Ok(None) => Ok(None),
            Ok(Some(Reply::Forward(sql))) => {
                let text = Bytes::from(sql.into_bytes());
                Box::pin(self.tenant_text(cx, &text, attrs, more, format, out))
                    .await
                    .map(Some)
            }
            Err(e) => Ok(Some(self.fail(out, &e).await?)),
            Ok(Some(Reply::Ok(n, notes))) => {
                if !notes.is_empty()
                    && let Some(ts) = self.tenant.as_deref_mut()
                {
                    ts.warnings = Some(
                        notes
                            .into_iter()
                            .map(|m| LocalWarning {
                                level: "Note",
                                code: 1105,
                                message: m,
                            })
                            .collect(),
                    );
                }
                let mut ok = OkPacket::new(status);
                ok.affected_rows = n;
                ok.warnings = self
                    .tenant
                    .as_deref()
                    .and_then(|t| t.warnings.as_ref())
                    .map_or(0, |w| u16::try_from(w.len()).unwrap_or(u16::MAX));
                out.ok(ok).await?;
                Ok(Some(step))
            }
            Ok(Some(Reply::Rows(columns, rows))) => {
                let cols = columns
                    .iter()
                    .map(|c| crate::pipeline::text_column(c, cx.proto.collation))
                    .collect();
                let rows = sprig_types::Rows {
                    columns: cols,
                    rows: rows
                        .into_iter()
                        .map(|r| r.into_iter().map(|v| v.map(String::into_bytes)).collect())
                        .collect(),
                };
                super::types::send_rows(out, &rows, format, status).await?;
                Ok(Some(step))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn inherit_msg(
        &mut self,
        cx: &mut ExecCtx<'_>,
        sql: &str,
        head: &str,
        lex_opts: &LexOptions,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        chain: &Chain,
        own: &MsgState,
    ) -> Result<Option<Reply>> {
        if let Some(s) = grammar::statement(sql, lex_opts)? {
            return match s {
                Stmt::Show(s) => self.inherit_show(svc, cat, chain, own, &s).await.map(Some),
                Stmt::Alter(a) => Box::pin(self.inherit_alter(svc, cat, chain, own, &a))
                    .await
                    .map(Some),
            };
        }
        match head {
            "create" => {
                let Some(f) = grammar::for_each(sql, lex_opts)? else {
                    return self.inherit_shadow_check(sql, lex_opts, cat, chain);
                };
                match grammar::ddl_kind(sql, lex_opts) {
                    Some((DefKind::Stream, false)) => self
                        .define_stream(svc, cat, own, sql, lex_opts, &f)
                        .await
                        .map(Some),
                    Some((DefKind::Trigger, false)) => self
                        .define_trigger(svc, cat, chain, own, sql, lex_opts, &f)
                        .await
                        .map(Some),
                    Some((DefKind::Exchange | DefKind::Queue, false)) => self
                        .define_mq(svc, cat, own, sql, lex_opts, &f)
                        .await
                        .map(Some),
                    Some((k, _)) => Err(not_built(&format!(
                        "FOR EACH TENANT on a {} (chapter 42; streams, exchanges, queues, \
                         bindings and triggers are built)",
                        k.as_str()
                    ))),
                    None => Err(SprigError::standard(
                        1064,
                        *b"42000",
                        "FOR EACH TENANT follows the name of a stream, exchange, queue, binding, trigger, connection or sink",
                    )),
                }
            }
            "drop" | "alter" => self.inherit_ddl(svc, cat, own, sql, lex_opts).await,
            "bind" => match grammar::for_each(sql, lex_opts)? {
                Some(f) => self
                    .define_mq(svc, cat, own, sql, lex_opts, &f)
                    .await
                    .map(Some),
                None => Ok(None),
            },
            "unbind" => self.unbind_def(svc, cat, own, sql, lex_opts).await,
            "insert" => {
                Box::pin(self.inherit_insert(cx, svc, cat, chain, own, sql, lex_opts)).await
            }
            "select" if inherited::fanin::find(sql, lex_opts).is_some() => self
                .fan_in_select(svc, cat, chain, own, sql, lex_opts)
                .await
                .map(Some),
            "select" => Ok(empty_reads(
                sql,
                lex_opts,
                cat,
                chain,
                own,
                self.current_logical().as_deref(),
            )
            .map(Reply::Forward)),
            _ => Ok(None),
        }
    }
}

impl PassSession {
    /// `CREATE TABLE | STREAM | TRIGGER name` in a member whose chain has a
    /// definition of the name (39897); else nothing.
    fn inherit_shadow_check(
        &self,
        sql: &str,
        lex_opts: &LexOptions,
        cat: &TenantCatalog,
        chain: &Chain,
    ) -> Result<Option<Reply>> {
        let Some((kind, false)) = grammar::ddl_kind(sql, lex_opts) else {
            return Ok(None);
        };
        if kind != DefKind::Stream {
            return Ok(None);
        }
        let Ok(sprig_streams::grammar::Stmt::Create(c)) =
            sprig_streams::grammar::classify(sql.as_bytes(), lex_opts)
        else {
            return Ok(None);
        };
        let Some(n) = c.name else { return Ok(None) };
        let Some(db) = n.db.clone().or_else(|| self.current_logical()) else {
            return Ok(None);
        };
        if let Some(d) = chain.resolve(&db, DefKind::Stream, &n.name)
            && d.declarer != cat.tenant
        {
            return Err(inherited::not_overridable(format!(
                "stream {db}.{} is inherited from tenant {}; a member can't declare its own of \
                 that name",
                n.name, d.declarer
            )));
        }
        Ok(None)
    }

    /// `CREATE STREAM s FOR EACH TENANT ...`.
    #[allow(clippy::too_many_lines)]
    async fn define_stream(
        &mut self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        own: &MsgState,
        sql: &str,
        lex_opts: &LexOptions,
        f: &grammar::ForEach,
    ) -> Result<Reply> {
        let rest = f.strip(sql);
        let sprig_streams::grammar::Stmt::Create(c) =
            sprig_streams::grammar::classify(rest.as_bytes(), lex_opts)?
        else {
            return Err(SprigError::standard(
                1064,
                *b"42000",
                "expected CREATE STREAM",
            ));
        };
        if c.attached.is_some() {
            return Err(not_built(
                "ATTACHED TO GROUP OF on a FOR EACH TENANT stream",
            ));
        }
        if !f.grants.is_empty() {
            return Err(SprigError::standard(
                1064,
                *b"42000",
                "GRANTS belongs to triggers and sinks, not streams",
            ));
        }
        let name = c
            .name
            .clone()
            .ok_or_else(|| SprigError::internal("no name"))?;
        let db = name
            .db
            .clone()
            .or_else(|| self.current_logical())
            .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))?;
        let ts = self
            .tenant
            .as_deref()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let lctn = ts.lower_case_table_names();
        if cat.database(&database_key(&db, lctn)).is_none() {
            return Err(SprigError::standard(
                1049,
                *b"42000",
                format!("Unknown database '{db}'"),
            ));
        }
        if !sprig_streams::model::valid_name(&name.name) {
            return Err(SprigError::standard(
                1103,
                *b"42000",
                format!("Incorrect stream name '{}'", name.name),
            ));
        }
        if !allows(ts, cat, Action::Create, &db, &name.name) {
            return Err(denied(ts, "CREATE", &name.name));
        }
        let user = ts.principal.user_id.map_or(0, |u| u.0);
        if own.def(&db, DefKind::Stream, &name.name).is_some() {
            if c.if_not_exists {
                return Ok(Reply::Ok(0, Vec::new()));
            }
            return Err(SprigError::new(
                SprigCode::StreamExists,
                format!("ER_SPRIG_STREAM_EXISTS: '{db}.{}' exists", name.name),
            ));
        }
        let streams = cat
            .ext
            .get::<sprig_streams::StreamsState>(sprig_streams::MODULE)
            .cloned()
            .unwrap_or_default();
        if let Some(d) = cat.database(&database_key(&db, lctn))
            && streams.get(d.db_id, &name.name, lctn).is_some()
        {
            return Err(SprigError::new(
                SprigCode::StreamExists,
                format!("ER_SPRIG_STREAM_EXISTS: '{db}.{}' exists", name.name),
            ));
        }
        let scope = scope_of(svc, cat.tenant, f).await?;
        check_quota(svc, cat, own)?;
        let partitions = c.partitions.unwrap_or_else(|| {
            let ctx = cat.setting_ctx(&svc.dc);
            u32::try_from(svc.settings.get(
                &sprig_streams::settings::PARTITIONS_DEFAULT,
                &ctx,
                Some(&cat.settings),
            ))
            .unwrap_or(12)
        });
        let spec = StreamSpec {
            key: c.key.clone(),
            partitions: partitions.max(1),
            retention_hours: c.retention_hours,
            cleanup: c
                .cleanup
                .map_or_else(|| "delete".to_owned(), |c| c.as_str().to_owned()),
            settings: BTreeMap::new(),
        };
        let object_id = next_object_id(svc, cat.tenant).await?;
        let def = Definition {
            declarer: cat.tenant,
            object_id,
            kind: DefKind::Stream,
            db_name: db.clone(),
            name: name.name.clone(),
            scope,
            definition: serde_json::to_value(&spec).unwrap_or_default(),
            grants: None,
            def_version: 1,
            state: DefState::Active,
            drop_after_ms: None,
            created_by: user,
            created_at_ms: sprig_inherit::msg::store::now_ms(),
            changed_version: 0,
        };
        let v = inherited::commit(svc, cat.tenant, &[MsgWrite::Def(&def)]).await?;
        self.after_own_change(v).await?;
        tracing::info!(tenant = %cat.tenant, def = %def.key(), name = %def.name, "inherited stream defined");
        Ok(Reply::Ok(
            0,
            vec![format!(
                "stream {db}.{} is defined for every tenant in scope; each member's instance is \
                 created on its first publish",
                name.name
            )],
        ))
    }

    /// A catalog change of this session's own tenant: read it at once.
    pub(super) async fn after_own_change(&mut self, v: u64) -> Result<()> {
        if let Some(ts) = self.tenant.as_deref_mut() {
            ts.changed(Some(v));
            ts.refresh().await?;
        }
        Ok(())
    }

    /// `CREATE [OR REPLACE] TRIGGER g ON STREAM s FOR EACH TENANT ...`.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn define_trigger(
        &mut self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        chain: &Chain,
        own: &MsgState,
        sql: &str,
        _lex_opts: &LexOptions,
        f: &grammar::ForEach,
    ) -> Result<Reply> {
        let text = f.strip(sql).trim().trim_end_matches(';').to_owned();
        let decl = sprig_logic::declare::parse(&text)?;
        let Some(src) = decl.source.clone() else {
            return Err(not_built(
                "FOR EACH TENANT on an after-commit trigger (inherited triggers read streams)",
            ));
        };
        let src_kind = if src.kind == sprig_logic::declare::SourceKind::Queue {
            DefKind::Queue
        } else {
            DefKind::Stream
        };
        let current = self.current_logical();
        let ts = self
            .tenant
            .as_deref()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let db = decl
            .db
            .clone()
            .or_else(|| current.clone())
            .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))?;
        let src_db = src
            .db
            .clone()
            .or_else(|| current.clone())
            .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))?;
        // The source: a stream or queue definition of this tenant, or one
        // reaching it.
        let source = own
            .def(&src_db, src_kind, &src.name)
            .cloned()
            .or_else(|| chain.resolve(&src_db, src_kind, &src.name))
            .ok_or_else(|| {
                SprigError::new(
                    SprigCode::StreamNotFound,
                    format!(
                        "ER_SPRIG_STREAM_NOT_FOUND: a FOR EACH TENANT trigger reads a FOR EACH \
                         TENANT {}; {src_db}.{} isn't one here",
                        src_kind.as_str(),
                        src.name
                    ),
                )
            })?;
        // Chapter 41's checks: code in the trigger's database, reading the
        // stream; GRANTS are down-tree grants in every member.
        let lctn = ts.lower_case_table_names();
        let grants = crate::tenant::authz::privileges(ts, cat);
        let db_key = database_key(&db, lctn);
        if !grants.unrestricted()
            && !grants.allows(Action::CreateRoutine, Target::Database { db_key: &db_key })
        {
            return Err(SprigError::standard(
                1044,
                *b"42000",
                format!(
                    "Access denied for user '{}' to database '{db}'",
                    ts.principal.user
                ),
            ));
        }
        if !allows(ts, cat, Action::Select, &src_db, &src.name) {
            return Err(denied(ts, "SELECT", &src.name));
        }
        if !f.grants.is_empty() && !tenant_admin(ts, cat) {
            return Err(SprigError::standard(
                1227,
                *b"42000",
                "Access denied; you need (at least one of) the SPRIG_TENANT_ADMIN privilege(s) \
                 for this operation (GRANTS gives every member's sprig_inherited these rights)",
            ));
        }
        for g in &f.grants {
            triggers::actions_of(g)?;
        }
        sprig_logic::messages::check(&decl, src.kind).await?;
        let user = ts.principal.user_id.map_or(0, |u| u.0);
        let existing = own.def(&db, DefKind::Trigger, &decl.name).cloned();
        if existing.is_some() && !decl.or_replace {
            return Err(SprigError::standard(
                1304,
                *b"42000",
                format!("TRIGGER {} already exists", decl.name),
            ));
        }
        let scope = scope_of(svc, cat.tenant, f).await?;
        if existing.is_none() {
            check_quota(svc, cat, own)?;
        }
        let spec = TriggerSpec {
            text,
            source_kind: src_kind.as_str().into(),
            source_db: src_db.clone(),
            source_name: src.name.clone(),
            db: db.clone(),
        };
        let now = sprig_inherit::msg::store::now_ms();
        let def = match &existing {
            Some(e) => Definition {
                scope,
                definition: serde_json::to_value(&spec).unwrap_or_default(),
                grants: (!f.grants.is_empty())
                    .then(|| serde_json::to_value(&f.grants).unwrap_or_default()),
                def_version: e.def_version + 1,
                ..(**e).clone()
            },
            None => Definition {
                declarer: cat.tenant,
                object_id: next_object_id(svc, cat.tenant).await?,
                kind: DefKind::Trigger,
                db_name: db.clone(),
                name: decl.name.clone(),
                scope,
                definition: serde_json::to_value(&spec).unwrap_or_default(),
                grants: (!f.grants.is_empty())
                    .then(|| serde_json::to_value(&f.grants).unwrap_or_default()),
                def_version: 1,
                state: DefState::Active,
                drop_after_ms: None,
                created_by: user,
                created_at_ms: now,
                changed_version: 0,
            },
        };
        let v = inherited::commit(svc, cat.tenant, &[MsgWrite::Def(&def)]).await?;
        self.after_own_change(v).await?;
        // Existing instances of the source get the trigger now, in the
        // background; members without one get it with their instance.
        let n = if src_kind == DefKind::Queue {
            mq_rollout(svc, source.declarer, &src_db, None).await?
        } else {
            rollout(svc, source.key()).await?
        };
        tracing::info!(tenant = %cat.tenant, def = %def.key(), name = %def.name, version = def.def_version, instances = n, "inherited trigger defined");
        Ok(Reply::Ok(
            0,
            vec![format!(
                "trigger {db}.{} (version {}) on {} {src_db}.{} runs in every member as its \
                 sprig_inherited; rolling out to {n} existing instance(s)",
                decl.name,
                def.def_version,
                src_kind.as_str(),
                src.name
            )],
        ))
    }

    /// `ALTER STREAM`, `DROP STREAM`, `DROP TRIGGER` naming a definition
    /// (or, in a member, an instance: 39897).
    #[allow(clippy::too_many_lines)]
    async fn inherit_ddl(
        &mut self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        own: &MsgState,
        sql: &str,
        lex_opts: &LexOptions,
    ) -> Result<Option<Reply>> {
        let Some((kind, drop)) = grammar::ddl_kind(sql, lex_opts) else {
            return Ok(None);
        };
        let toks: Vec<_> = lex(sql.as_bytes(), lex_opts)
            .map_err(|_| SprigError::internal("lex"))?
            .into_iter()
            .filter(|t| !t.kind.is_trivia())
            .collect();
        let word = |i: usize| toks.get(i).map(|t| sql[t.range()].to_owned());
        let ident = |i: usize| {
            toks.get(i).map(|t| {
                String::from_utf8_lossy(&sprig_sql::lex::ident_value(t.bytes(sql.as_bytes())))
                    .into_owned()
            })
        };
        // ALTER|DROP KIND [IF EXISTS] [db.]name [IMMEDIATE | CASCADE]...
        let mut i = 2;
        let mut if_exists = false;
        if word(i).is_some_and(|w| w.eq_ignore_ascii_case("if")) {
            if_exists = true;
            i += 2;
        }
        let Some(first) = ident(i) else {
            return Ok(None);
        };
        let (db, name, next) = if word(i + 1).as_deref() == Some(".") {
            (Some(first), ident(i + 2).unwrap_or_default(), i + 3)
        } else {
            (None, first, i + 1)
        };
        let Some(db) = db.or_else(|| self.current_logical()) else {
            return Ok(None);
        };
        let rest: Vec<String> = (next..toks.len())
            .filter_map(|j| word(j).map(|w| w.to_ascii_lowercase()))
            .collect();
        // A member's instance: only ALTER INHERITED changes it. Not the
        // declarer's own instance of an `INCLUDING SELF` definition
        // (bugfix-39): there the statement names the definition, which
        // the declarer could otherwise never drop (nor, by the 39895
        // guard, its database).
        if own.def(&db, kind, &name).is_none()
            && let Some(i) = own.instances.values().find(|i| {
            i.kind == kind
                && i.state != sprig_inherit::msg::InstanceState::Detached
                && phys_name(i.physical.as_ref()) == Some((&db, &name))
        }) {
            let _ = i;
            return Err(inherited::not_overridable(format!(
                "{} {db}.{name} is inherited; use ALTER INHERITED {} {db}.{name} ... (settings, \
                 DISABLE, ENABLE, RESET, DETACH)",
                kind.as_str(),
                kind.as_str().to_ascii_uppercase()
            )));
        }
        let Some(def) = own.def(&db, kind, &name).cloned() else {
            // No SQL statement drops an ordinary exchange or queue yet: a
            // missing definition with IF EXISTS is a note, as MySQL answers
            // a missing table (1051 as a note).
            if drop && if_exists && matches!(kind, DefKind::Exchange | DefKind::Queue) {
                return Ok(Some(Reply::Ok(
                    0,
                    vec![format!("{} {db}.{name} doesn't exist", kind.as_str())],
                )));
            }
            return Ok(None);
        };
        let ts = self
            .tenant
            .as_deref()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        if drop {
            if !allows(ts, cat, Action::Drop, &db, &name) {
                return Err(denied(ts, "DROP", &name));
            }
            return self
                .drop_def(
                    svc,
                    cat,
                    own,
                    &def,
                    rest.iter().any(|w| w == "immediate"),
                    rest.iter().any(|w| w == "cascade"),
                )
                .await
                .map(Some);
        }
        if kind != DefKind::Stream {
            return Ok(None);
        }
        if !allows(ts, cat, Action::Alter, &db, &name) {
            return Err(denied(ts, "ALTER", &name));
        }
        let sprig_streams::grammar::Stmt::Alter(a) =
            sprig_streams::grammar::classify(sql.as_bytes(), lex_opts)?
        else {
            return Ok(None);
        };
        let mut spec: StreamSpec =
            serde_json::from_value(def.definition.clone()).unwrap_or_default();
        if let Some(n) = a.partitions {
            if n < spec.partitions {
                return Err(SprigError::new(
                    SprigCode::StreamPartitionsDecrease,
                    format!(
                        "ER_SPRIG_STREAM_PARTITIONS_DECREASE: stream '{name}' has {} partitions; partitions can only grow",
                        spec.partitions
                    ),
                ));
            }
            if n > spec.partitions {
                return Err(not_built(
                    "more partitions on a FOR EACH TENANT stream yet (declare a new definition)",
                ));
            }
        }
        if a.cleanup.is_some_and(|c| c.as_str() != spec.cleanup) {
            return Err(SprigError::new(
                SprigCode::InheritedObjectInUse,
                "ER_SPRIG_INHERITED_OBJECT_IN_USE: a FOR EACH TENANT stream's cleanup policy \
                 can't change; declare a new definition",
            ));
        }
        if let Some(h) = a.retention_hours {
            spec.retention_hours = Some(h);
        }
        for (k, v) in &a.settings {
            let c = sprig_streams::settings::stream_setting(k, v.as_deref()).map_err(|m| {
                SprigError::standard(1231, *b"42000", format!("ALTER STREAM '{name}': {m}"))
            })?;
            match c.value {
                Some(v) => spec.settings.insert(c.key.to_owned(), json!(v)),
                None => spec.settings.remove(c.key),
            };
        }
        let new = Definition {
            definition: serde_json::to_value(&spec).unwrap_or_default(),
            def_version: def.def_version + 1,
            ..(*def).clone()
        };
        let v = inherited::commit(svc, cat.tenant, &[MsgWrite::Def(&new)]).await?;
        self.after_own_change(v).await?;
        let n = rollout(svc, def.key()).await?;
        Ok(Some(Reply::Ok(
            0,
            vec![format!(
                "stream definition {db}.{name} is at version {}; rolling out to {n} instance(s)",
                new.def_version
            )],
        )))
    }

    /// `DROP STREAM | TRIGGER` of a definition (chapter 42, "Dropping a
    /// definition"). A stream: refused while trigger definitions read it
    /// (unless `CASCADE`); with `IMMEDIATE` every instance goes now, else
    /// the definition is `dropping` until `inherit.drop_grace_hours`.
    /// A trigger: its instances' jobs stop and it goes now (it holds no
    /// data).
    #[allow(clippy::too_many_lines)]
    async fn drop_def(
        &mut self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        own: &MsgState,
        def: &Arc<Definition>,
        immediate: bool,
        cascade: bool,
    ) -> Result<Reply> {
        let mut writes_defs: Vec<Definition> = Vec::new();
        let mut deletes: Vec<u64> = Vec::new();
        let mut removed = 0usize;
        if def.kind == DefKind::Trigger {
            removed += drop_trigger_instances(svc, def.key()).await?;
            deletes.push(def.object_id);
        } else {
            {
                let readers: Vec<Arc<Definition>> = own
                    .defs
                    .values()
                    .filter(|g| g.kind == DefKind::Trigger)
                    .filter(|g| {
                        serde_json::from_value::<TriggerSpec>(g.definition.clone()).is_ok_and(|s| {
                            s.source_db.eq_ignore_ascii_case(&def.db_name)
                                && s.source_name == def.name
                        })
                    })
                    .cloned()
                    .collect();
                if !readers.is_empty() && !cascade {
                    return Err(SprigError::new(
                        SprigCode::InheritedObjectInUse,
                        format!(
                            "ER_SPRIG_INHERITED_OBJECT_IN_USE: trigger definition(s) {} read \
                             stream {}.{}; drop them first, or DROP ... CASCADE",
                            readers
                                .iter()
                                .map(|g| g.name.as_str())
                                .collect::<Vec<_>>()
                                .join(", "),
                            def.db_name,
                            def.name
                        ),
                    ));
                }
                for g in &readers {
                    removed += drop_trigger_instances(svc, g.key()).await?;
                    deletes.push(g.object_id);
                }
                // Binding definitions joining an exchange or queue go with it
                // (CASCADE).
                let binds: Vec<Arc<Definition>> = own
                    .defs
                    .values()
                    .filter(|b| b.kind == DefKind::Binding && def.kind != DefKind::Stream)
                    .filter(|b| {
                        serde_json::from_value::<sprig_inherit::msg::BindingSpec>(
                            b.definition.clone(),
                        )
                        .is_ok_and(|s| {
                            b.db_name.eq_ignore_ascii_case(&def.db_name)
                                && (s.source == def.name
                                    || (s.dest == def.name && s.dest_kind == def.kind.as_str()))
                        })
                    })
                    .cloned()
                    .collect();
                if !binds.is_empty() && !cascade {
                    return Err(SprigError::new(
                        SprigCode::InheritedObjectInUse,
                        format!(
                            "ER_SPRIG_INHERITED_OBJECT_IN_USE: binding definition(s) {} join {} \
                             {}.{}; UNBIND them first, or DROP ... CASCADE",
                            binds
                                .iter()
                                .map(|b| b.name.as_str())
                                .collect::<Vec<_>>()
                                .join(", "),
                            def.kind.as_str(),
                            def.db_name,
                            def.name
                        ),
                    ));
                }
                for b in &binds {
                    deletes.push(b.object_id);
                    if let Ok(spec) = serde_json::from_value::<sprig_inherit::msg::BindingSpec>(
                        b.definition.clone(),
                    ) {
                        mq_rollout(svc, cat.tenant, &def.db_name, Some(&spec)).await?;
                    }
                }
                if immediate {
                    let ts = self
                        .tenant
                        .as_deref()
                        .ok_or_else(|| SprigError::internal("no tenant"))?;
                    if !tenant_admin(ts, cat) {
                        return Err(SprigError::standard(
                            1227,
                            *b"42000",
                            "Access denied; you need (at least one of) the SPRIG_TENANT_ADMIN \
                             privilege(s) for this operation (DROP ... IMMEDIATE)",
                        ));
                    }
                    // Every instance, those stuck in 'creating' included.
                    removed += inherited::lifecycle::remove_all(svc, def).await?;
                    metrics::counter!("sprig_inherit_drops_total", "how" => "immediate")
                        .increment(1);
                    deletes.push(def.object_id);
                } else {
                    let grace = svc.settings.get(
                        &sprig_inherit::msg::settings::DROP_GRACE_HOURS,
                        &cat.setting_ctx(&svc.dc),
                        Some(&cat.settings),
                    );
                    writes_defs.push(Definition {
                        state: DefState::Dropping,
                        drop_after_ms: Some(
                            sprig_inherit::msg::store::now_ms() + grace.saturating_mul(3_600_000),
                        ),
                        def_version: def.def_version + 1,
                        ..(**def).clone()
                    });
                }
            }
        }
        let mut writes: Vec<MsgWrite<'_>> = writes_defs.iter().map(MsgWrite::Def).collect();
        writes.extend(deletes.iter().map(|o| MsgWrite::DeleteDef(*o)));
        let v = inherited::commit(svc, cat.tenant, &writes).await?;
        self.after_own_change(v).await?;
        // The grace's end: a gateway's pass removes the instances then.
        for d in &writes_defs {
            if let Some(at) = d.drop_after_ms {
                inherited::lifecycle::list_dropping(svc, d.key(), at).await?;
            }
        }
        tracing::info!(tenant = %cat.tenant, def = %def.key(), immediate, removed, "inherited definition dropped");
        Ok(Reply::Ok(
            0,
            vec![if writes_defs.is_empty() {
                format!(
                    "{} {}.{} dropped; {removed} instance(s) removed",
                    def.kind.as_str(),
                    def.db_name,
                    def.name
                )
            } else {
                format!(
                    "{} {}.{} is dropping: publishes stop, consumers drain, and members may \
                     DETACH their instance until the grace ends",
                    def.kind.as_str(),
                    def.db_name,
                    def.name
                )
            }],
        ))
    }
}

/// `{"db": .., "name": ..}` of an instance row.
fn phys_name(p: Option<&Value>) -> Option<(&str, &str)> {
    let p = p?;
    Some((p.get("db")?.as_str()?, p.get("name")?.as_str()?))
}

/// The scope of a `FOR EACH TENANT` clause in `declarer`.
pub(super) async fn scope_of(
    svc: &TenantServices,
    declarer: TenantId,
    f: &grammar::ForEach,
) -> Result<Scope> {
    let under = match &f.under {
        None => None,
        Some(n) => {
            let id = inherited::tenant_by_name(svc, n)
                .await?
                .ok_or_else(|| target_err(format!("UNDER {n}: no descendant of that name")))?;
            let e = inherited::dir_entry(svc, id)
                .await?
                .ok_or_else(|| target_err(format!("UNDER {n}: no descendant of that name")))?;
            let path = e.path_ids();
            if id == declarer || !path.contains(&declarer.0) {
                return Err(target_err(format!("UNDER {n}: no descendant of that name")));
            }
            Some(id.0)
        }
    };
    Ok(Scope {
        depth: f.depth,
        under,
        including_self: f.including_self,
    })
}

pub(super) fn check_quota(svc: &TenantServices, cat: &TenantCatalog, own: &MsgState) -> Result<()> {
    let max = svc.settings.get(
        &sprig_inherit::msg::settings::MAX_DEFINITIONS_PER_TENANT,
        &cat.setting_ctx(&svc.dc),
        Some(&cat.settings),
    );
    let have = own.defs.len() as u64;
    if have >= max {
        return Err(SprigError::new(
            SprigCode::InheritInstanceQuota,
            format!(
                "ER_SPRIG_INHERIT_INSTANCE_QUOTA: this tenant declares {have} FOR EACH TENANT \
                 definitions (inherit.max_definitions_per_tenant = {max})"
            ),
        ));
    }
    Ok(())
}

pub(super) async fn next_object_id(svc: &TenantServices, t: TenantId) -> Result<u64> {
    use sprig_catalog::CatalogLocator;
    let ep = svc.locator.writer(t).await?;
    let mut conn = ep.pool.get_conn().await.map_err(|e| {
        SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
    })?;
    Ok(ep.store.next_object_id(&mut conn).await?.0)
}

/// Every member holding an instance of stream definition `def` re-applies
/// its derived state (settings, triggers, sinks) now, in the background;
/// for a queue definition, its sinks (r9fanin). The number of instances.
pub(super) async fn rollout(svc: &Arc<TenantServices>, def: DefKey) -> Result<usize> {
    let entries = inherited::instances_of(svc, def).await?;
    let n = entries.len();
    let svc = Arc::clone(svc);
    tokio::spawn(async move {
        for e in entries {
            // A stream's derived state, or (R9, r9fanin) a queue's sinks.
            let r = Box::pin(crate::inherited::sinks::reapply_source(&svc, e.tenant, def))
                .await
                .and_then(|reached| {
                    if reached {
                        Ok(())
                    } else {
                        Err(SprigError::internal("the definition is gone"))
                    }
                });
            if let Err(err) = r {
                tracing::warn!(tenant = %e.tenant, def = %def, error = %err, "inherited instance not updated; its next use retries");
            }
        }
    });
    Ok(n)
}

/// Stops and forgets every member's instance of trigger definition `g`:
/// its job's index entry and its instance row (the member's module keeps
/// its versions for the record). The number of instances.
async fn drop_trigger_instances(svc: &Arc<TenantServices>, g: DefKey) -> Result<usize> {
    use sprig_catalog::CatalogLocator;
    // Trigger instances live with the members' stream instances.
    let prefix = svc.locator.dc_store().tier().key("inherit-instances/")?;
    let page = svc
        .locator
        .dc_store()
        .list(
            &prefix,
            sprig_meta::ListOpts {
                limit: 1_000_000,
                ..sprig_meta::ListOpts::default()
            },
        )
        .await?;
    let tenants: std::collections::BTreeSet<TenantId> = page
        .kvs
        .iter()
        .filter_map(|kv| {
            sprig_meta::Record::<inherited::InstanceIndex>::decode(&kv.value)
                .ok()
                .map(|r| r.body.tenant)
        })
        .collect();
    let mut n = 0;
    for t in tenants {
        // A member deleted since has nothing left to stop.
        if inherited::dir_entry(svc, t).await?.is_none() {
            continue;
        }
        let ep = svc.locator.writer(t).await?;
        let mut conn = ep.pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
        })?;
        let store = sprig_inherit::msg::MsgStore::new(ep.store.schema());
        if let Some(i) = store.instance(&mut conn, g).await? {
            if let Some(b) = i
                .physical
                .as_ref()
                .and_then(|p| p.get("binding"))
                .and_then(Value::as_u64)
            {
                sprig_logic::messages::delete_index(svc.locator.dc_store().as_ref(), t, b).await?;
                // The job ends at its next check; its module stays, marked.
                let _ = conn
                    .exec_drop(
                        format!(
                            "UPDATE {}.logic_modules SET state = 'dropped' WHERE module_id = ?",
                            sprig_common::names::quote(ep.store.schema())
                        ),
                        (i.physical
                            .as_ref()
                            .and_then(|p| p.get("module"))
                            .and_then(Value::as_u64)
                            .unwrap_or(0),),
                    )
                    .await;
            }
            store.delete_instance(&mut conn, g).await?;
            n += 1;
        }
    }
    Ok(n)
}

impl PassSession {
    /// `INSERT` into an inherited stream: a member's own publish, `FOR
    /// TENANT t`, or the `tenant` column. `None` when the target isn't
    /// one.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn inherit_insert(
        &mut self,
        cx: &mut ExecCtx<'_>,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        chain: &Chain,
        own: &MsgState,
        sql: &str,
        lex_opts: &LexOptions,
    ) -> Result<Option<Reply>> {
        let target = grammar::insert_target(sql, lex_opts)?;
        let text = target
            .as_ref()
            .map_or_else(|| sql.to_owned(), |t| t.strip(sql));
        let sprig_streams::grammar::Stmt::Insert(i) =
            sprig_streams::grammar::classify(text.as_bytes(), lex_opts)?
        else {
            return Ok(None);
        };
        let Some(db) = i.target.db.clone().or_else(|| self.current_logical()) else {
            return Ok(None);
        };
        let name = i.target.name.clone();
        if let Some(grammar::Target::In(up)) = target.as_ref().map(|t| &t.target) {
            return self
                .publish_up(cx, svc, cat, &text, &i, &db, &name, up, lex_opts)
                .await
                .map(Some);
        }
        // Ours: a definition of this tenant, one reaching it, or its
        // instance of one; an ordinary stream or table of the name isn't.
        let def = own
            .def(&db, DefKind::Stream, &name)
            .cloned()
            .or_else(|| chain.resolve(&db, DefKind::Stream, &name));
        let Some(def) = def else {
            if target.is_some() {
                return Err(SprigError::new(
                    SprigCode::StreamNotFound,
                    format!(
                        "ER_SPRIG_STREAM_NOT_FOUND: '{db}.{name}' isn't a FOR EACH TENANT stream \
                         this tenant declares or inherits"
                    ),
                ));
            }
            return Ok(None);
        };
        if own
            .instances
            .get(&def.key())
            .is_some_and(|i| i.state == sprig_inherit::msg::InstanceState::Detached)
        {
            // Detached: the member's own stream now.
            return Ok(None);
        }
        if def.declarer != cat.tenant {
            let ts = self
                .tenant
                .as_deref()
                .ok_or_else(|| SprigError::internal("no tenant"))?;
            let lctn = ts.lower_case_table_names();
            // The member's own stream of that name (made before the
            // definition) shadows it.
            let instance = own.instances.get(&def.key()).map(|i| i.instance_id);
            if let Some(d) = cat.database(&database_key(&db, lctn))
                && let Some(s) = cat
                    .ext
                    .get::<sprig_streams::StreamsState>(sprig_streams::MODULE)
                    .and_then(|st| st.get(d.db_id, &name, lctn).cloned())
                && Some(s.id.0) != instance
            {
                return Ok(None);
            }
            if let Some(d) = cat.database(&database_key(&db, lctn))
                && cat
                    .object(
                        d.db_id,
                        sprig_catalog::store::Namespace::Relation,
                        &object_key(ObjectKind::Table, &name, lctn),
                    )
                    .is_some()
            {
                // The member's own table of that name shadows it.
                return Ok(None);
            }
        }
        match target.map(|t| t.target) {
            Some(grammar::Target::All {
                with_instance,
                under,
            }) => self
                .inherit_broadcast(
                    cx,
                    svc,
                    cat,
                    &text,
                    &i,
                    &def,
                    with_instance,
                    under.as_deref(),
                    lex_opts,
                )
                .await
                .map(Some),
            Some(grammar::Target::Tenant(t)) if t == "*" => self
                .inherit_broadcast(cx, svc, cat, &text, &i, &def, false, None, lex_opts)
                .await
                .map(Some),
            Some(grammar::Target::In(_)) => Ok(None),
            Some(grammar::Target::Tenant(t)) => Box::pin(self.inherit_publish(
                cx,
                svc,
                cat,
                chain,
                &text,
                &i,
                &db,
                &def,
                Some(t),
                lex_opts,
            ))
            .await
            .map(Some),
            None => Box::pin(
                self.inherit_publish(cx, svc, cat, chain, &text, &i, &db, &def, None, lex_opts),
            )
            .await
            .map(Some),
        }
    }

    /// Evaluates the insert's rows in this session and writes each into
    /// its target's instance (see the module docs).
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn inherit_publish(
        &mut self,
        cx: &mut ExecCtx<'_>,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        chain: &Chain,
        text: &str,
        i: &InsertInto,
        db: &str,
        def: &Arc<Definition>,
        fixed: Option<String>,
        lex_opts: &LexOptions,
    ) -> Result<Reply> {
        if i.on_duplicate {
            return Err(SprigError::new(
                SprigCode::StreamAppendOnly,
                "ER_SPRIG_STREAM_APPEND_ONLY: INSERT ... ON DUPLICATE KEY UPDATE on a stream",
            ));
        }
        if i.set_form {
            return Err(not_built(
                "'INSERT ... SET' on a stream: use VALUES or SELECT",
            ));
        }
        let (cols, tail) = insert_shape(text, i)?;
        let me = cat.tenant;
        let my_name = {
            let ts = self
                .tenant
                .as_deref()
                .ok_or_else(|| SprigError::internal("no tenant"))?;
            if !allows(ts, cat, Action::Insert, db, &def.name) {
                return Err(denied(ts, "INSERT", &def.name));
            }
            ts.principal.tenant_name.clone().unwrap_or_default()
        };
        if fixed.is_some() && cols.contains(&Col::Tenant) {
            return Err(SprigError::standard(
                1064,
                *b"42000",
                "FOR TENANT and a tenant column can't be used together",
            ));
        }
        let q = rows_query(&tail, lex_opts)?;
        let got = self.run_checked(cx, &q, false).await?;
        let mut rows: Vec<Row> = Vec::with_capacity(got.rows.len());
        for r in got.rows {
            if r.len() != cols.len() {
                return Err(SprigError::standard(
                    1136,
                    *b"21S01",
                    "Column count doesn't match value count at row 1",
                ));
            }
            let mut row = Row {
                tenant: fixed.clone(),
                rec: Publish::default(),
            };
            let mut headers = None;
            for (c, v) in cols.iter().zip(r) {
                match c {
                    Col::Key => row.rec.key = v,
                    Col::Value => row.rec.value = v,
                    Col::Headers => headers = v,
                    Col::Partition => {
                        row.rec.partition = v
                            .and_then(|b| String::from_utf8(b).ok())
                            .and_then(|s| s.trim().parse().ok());
                    }
                    Col::Tenant => {
                        row.tenant = Some(
                            v.and_then(|b| String::from_utf8(b).ok())
                                .ok_or_else(|| target_err("a row names no tenant"))?,
                        );
                    }
                }
            }
            row.rec.headers = headers.map(|h| String::from_utf8_lossy(&h).into_owned());
            rows.push(row);
        }
        // Group by target, in order of first appearance.
        let mut groups: Vec<(String, Vec<Publish>)> = Vec::new();
        for r in rows {
            let t = r.tenant.unwrap_or_else(|| my_name.clone());
            match groups.iter_mut().find(|(n, _)| *n == t) {
                Some((_, v)) => v.push(r.rec),
                None => groups.push((t, vec![r.rec])),
            }
        }
        let in_txn = self.offsets_in_transaction();
        let mut total = 0u64;
        let mut notes = Vec::new();
        for (tname, mut recs) in groups {
            let (si, dest) = if tname == my_name {
                // Into this tenant's own instance.
                if !chain.reaches_level(def, 0) {
                    return Err(target_err(format!(
                        "this tenant declares {}.{} without INCLUDING SELF: name a member with \
                         FOR TENANT t or a tenant column",
                        def.db_name, def.name
                    )));
                }
                if def.declarer != me {
                    let decl_cat = inherited::catalog(svc, def.declarer).await?;
                    let mut dctx =
                        decl_cat.object_ctx(&svc.dc, sprig_common::ids::ObjectId(def.object_id));
                    if let Some(d) = decl_cat
                        .databases
                        .iter()
                        .find(|d| d.name.eq_ignore_ascii_case(&def.db_name))
                    {
                        dctx = dctx.with_db(d.db_id);
                    }
                    let on = svc.settings.get(
                        &sprig_inherit::msg::settings::MEMBER_PUBLISH,
                        &dctx,
                        Some(&decl_cat.settings),
                    );
                    if on == "off" {
                        return Err(target_err(format!(
                            "{}.{} takes publishes from its ancestors only \
                             (inherit.member_publish = off)",
                            def.db_name, def.name
                        )));
                    }
                }
                let si = Box::pin(streams::ensure(svc, chain, def, Cause::Publish)).await?;
                if si.created || si.cat.version > cat.version {
                    self.after_own_change(si.cat.version).await?;
                }
                (si, Dest::Own)
            } else {
                // Down the tree only.
                let tid = inherited::tenant_by_name(svc, &tname)
                    .await?
                    .ok_or_else(|| no_member(&tname, def))?;
                let tcat = inherited::catalog(svc, tid).await?;
                let tchain = inherited::chain(svc, &tcat).await?;
                let tdef = tchain
                    .resolve(&def.db_name, DefKind::Stream, &def.name)
                    .filter(|d| d.key() == def.key())
                    .ok_or_else(|| no_member(&tname, def))?;
                if !tchain.is_proper_ancestor(me) {
                    return Err(no_member(&tname, def));
                }
                if in_txn {
                    // Through this tenant's cross-tenant outbox: the record
                    // exists only if the transaction commits (chapter 42).
                    let strict = svc.settings.get(
                        &sprig_inherit::msg::settings::CROSS_TENANT_MESSAGING_STRICT,
                        &cat.setting_ctx(&svc.dc),
                        Some(&cat.settings),
                    ) == "on";
                    if strict {
                        inherited::check_residency(svc, tid, me, SprigCode::InheritResidency)
                            .await?;
                    }
                    for r in &mut recs {
                        r.headers = Some(stamp(
                            r.headers.as_deref().map(str::as_bytes),
                            &tname,
                            &my_name,
                        )?);
                    }
                    let n = recs.len() as u64;
                    let dest = inherited::outbox::Dest::Instance {
                        tenant: tid.0,
                        def_tenant: def.declarer.0,
                        def_object: def.object_id,
                    };
                    self.xo_publish(cx, svc, cat, &def.db_name, &dest, recs)
                        .await?;
                    total += n;
                    continue;
                }
                (
                    Box::pin(streams::ensure(svc, &tchain, &tdef, Cause::Publish)).await?,
                    Dest::Other,
                )
            };
            let max = svc.settings.get(
                &sprig_streams::settings::MAX_MESSAGE_BYTES,
                &si.cat
                    .object_ctx(&svc.dc, sprig_common::ids::ObjectId(si.stream.id.0))
                    .with_db(si.stream.db),
                Some(&si.cat.settings),
            );
            for r in &mut recs {
                let stamped = stamp(r.headers.as_deref().map(str::as_bytes), &tname, &my_name)?;
                sprig_streams::publish::check_record(
                    &si.stream,
                    r.key.as_deref(),
                    r.value.as_ref().map_or(0, Vec::len),
                    stamped.len(),
                    max,
                )?;
                r.headers = Some(stamped);
            }
            let n = recs.len() as u64;
            match dest {
                Dest::Own => {
                    let ins =
                        sprig_streams::publish::outbox_insert(&si.stream, Some(&si.db_name), &recs)
                            .ok_or_else(|| SprigError::internal("an empty publish"))?;
                    self.run_checked(cx, &ins, true).await?;
                }
                Dest::Other => {
                    let schema = si.outbox_schema()?;
                    let ins =
                        sprig_streams::publish::outbox_insert(&si.stream, Some(&schema), &recs)
                            .ok_or_else(|| SprigError::internal("an empty publish"))?;
                    let cluster = svc.locator.cluster(si.tenant).await?;
                    let pool = svc.locator.ctl_pool(&cluster).await?;
                    let mut c = pool
                        .get_conn()
                        .await
                        .map_err(sprig_streams::maria::map_err)?;
                    c.query_drop(ins)
                        .await
                        .map_err(sprig_streams::maria::map_err)?;
                    metrics::counter!("sprig_inherit_publishes_total", "kind" => "stream", "path" => "direct")
                        .increment(n);
                }
            }
            if matches!(dest, Dest::Own) {
                metrics::counter!("sprig_inherit_publishes_total", "kind" => "stream", "path" => "own")
                    .increment(n);
            }
            if si.created {
                notes.push(format!(
                    "tenant {tname}'s instance of {}.{} was created",
                    def.db_name, def.name
                ));
            }
            total += n;
        }
        Ok(Reply::Ok(total, notes))
    }

    /// `ALTER INHERITED ...` (chapter 42, "Overrides: nearest level wins").
    #[allow(clippy::too_many_lines)]
    async fn inherit_alter(
        &mut self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        chain: &Chain,
        own: &MsgState,
        a: &grammar::AlterInherited,
    ) -> Result<Reply> {
        let db =
            a.db.clone()
                .or_else(|| self.current_logical())
                .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))?;
        let me = cat.tenant;
        let def = own
            .def(&db, a.kind, &a.name)
            .cloned()
            .or_else(|| chain.resolve(&db, a.kind, &a.name))
            .ok_or_else(|| {
                target_err(format!(
                    "no inherited {} {db}.{} reaches this tenant",
                    a.kind.as_str(),
                    a.name
                ))
            })?;
        let (target, applies) = match &a.target {
            For::Own => (me, AppliesTo::Own),
            For::Subtree(None) => (me, AppliesTo::Subtree),
            For::Tenant(n) | For::Subtree(Some(n)) => {
                let id = inherited::tenant_by_name(svc, n)
                    .await?
                    .ok_or_else(|| target_err(format!("no descendant named {n}")))?;
                let e = inherited::dir_entry(svc, id)
                    .await?
                    .ok_or_else(|| target_err(format!("no descendant named {n}")))?;
                let below = e.path_ids().contains(&me.0);
                let ok = match a.target {
                    For::Tenant(_) => below && id != me,
                    _ => below,
                };
                if !ok {
                    return Err(target_err(format!("no descendant named {n}")));
                }
                (
                    id,
                    if matches!(a.target, For::Tenant(_)) {
                        AppliesTo::Own
                    } else {
                        AppliesTo::Subtree
                    },
                )
            }
        };
        if def.declarer == me && target == me {
            return Err(inherited::not_overridable(format!(
                "this tenant declares {} {db}.{}: change the definition itself (ALTER STREAM, \
                 CREATE OR REPLACE TRIGGER), or override it FOR TENANT t | FOR SUBTREE t",
                a.kind.as_str(),
                a.name
            )));
        }
        {
            let ts = self
                .tenant
                .as_deref()
                .ok_or_else(|| SprigError::internal("no tenant"))?;
            let ok = if matches!(a.target, For::Own) {
                allows(ts, cat, Action::Alter, &db, &a.name)
            } else {
                tenant_admin(ts, cat)
            };
            if !ok {
                return Err(denied(ts, "ALTER", &a.name));
            }
        }
        let user = self
            .tenant
            .as_deref()
            .and_then(|t| t.principal.user_id)
            .map_or(0, |u| u.0);
        let existing = |what: What| {
            own.overrides
                .iter()
                .find(|o| {
                    o.def == def.key()
                        && o.target == target
                        && o.applies_to == applies
                        && o.what == what
                })
                .cloned()
        };
        let mk = |what: What, value: Value| Override {
            made_in: me,
            def: def.key(),
            target,
            applies_to: applies,
            what,
            value,
            set_by: user,
            set_at_ms: sprig_inherit::msg::store::now_ms(),
        };
        let mut puts: Vec<Override> = Vec::new();
        let mut deletes: Vec<Option<What>> = Vec::new();
        let what_done: String;
        match &a.op {
            AlterOp::Set(kvs) if a.kind == DefKind::Queue => {
                let mut m = existing(What::Settings)
                    .and_then(|o| o.value.as_object().cloned())
                    .unwrap_or_default();
                for (k, v) in kvs {
                    let k = k.replace('_', "-");
                    if !sprig_inherit::msg::model::QUEUE_OVERRIDABLE.contains(&k.as_str()) {
                        return Err(inherited::not_overridable(format!(
                            "{k} belongs to the definition; a member may set {}",
                            sprig_inherit::msg::model::QUEUE_OVERRIDABLE.join(", ")
                        )));
                    }
                    let val = if v.eq_ignore_ascii_case("null") {
                        Value::Null
                    } else {
                        v.parse::<i64>().map_or_else(|_| json!(v), |n| json!(n))
                    };
                    m.insert(k, val);
                }
                puts.push(mk(What::Settings, Value::Object(m)));
                what_done = "queue arguments".into();
            }
            AlterOp::Set(kvs) => {
                if a.kind != DefKind::Stream {
                    return Err(not_built(&format!(
                        "SET on an inherited {} yet (streams and queues take SET; DISABLE, \
                         ENABLE, REPLACE and RESET are built)",
                        a.kind.as_str()
                    )));
                }
                let mut m = existing(What::Settings)
                    .and_then(|o| o.value.as_object().cloned())
                    .unwrap_or_default();
                for (k, v) in kvs {
                    if !streams::OVERRIDABLE.contains(&k.as_str()) {
                        return Err(inherited::not_overridable(format!(
                            "{k} belongs to the definition; a member may set {}",
                            streams::OVERRIDABLE.join(", ")
                        )));
                    }
                    let c = sprig_streams::settings::stream_setting(k, Some(v)).map_err(|m| {
                        SprigError::standard(1231, *b"42000", format!("{}: {m}", a.name))
                    })?;
                    m.insert(k.clone(), json!(c.value));
                }
                puts.push(mk(What::Settings, Value::Object(m)));
                what_done = "settings".into();
            }
            AlterOp::Reset(keys) if keys.is_empty() => {
                deletes.push(None);
                what_done = "every override".into();
            }
            AlterOp::Reset(keys) => {
                let mut m = existing(What::Settings)
                    .and_then(|o| o.value.as_object().cloned())
                    .unwrap_or_default();
                for k in keys {
                    m.remove(k);
                }
                if m.is_empty() {
                    deletes.push(Some(What::Settings));
                } else {
                    puts.push(mk(What::Settings, Value::Object(m)));
                }
                what_done = format!("settings {}", keys.join(", "));
            }
            AlterOp::Disable | AlterOp::Enable => {
                if !matches!(a.kind, DefKind::Trigger | DefKind::Binding) {
                    return Err(inherited::not_overridable(format!(
                        "an inherited {} can't be disabled (triggers, sinks and bindings can)",
                        a.kind.as_str()
                    )));
                }
                let on = matches!(a.op, AlterOp::Enable);
                puts.push(mk(
                    What::State,
                    json!(if on { "enabled" } else { "disabled" }),
                ));
                what_done = if on {
                    "enabled".into()
                } else {
                    "disabled".into()
                };
            }
            AlterOp::Replace(body) => {
                if a.kind != DefKind::Trigger {
                    return Err(inherited::not_overridable(
                        "only a trigger's body can be replaced",
                    ));
                }
                let spec: TriggerSpec =
                    serde_json::from_value(def.definition.clone()).unwrap_or_default();
                let queue = spec.source_kind == "queue";
                let full = format!(
                    "CREATE TRIGGER {} ON {} {}.{} {body}",
                    sprig_common::names::quote(&def.name),
                    if queue { "QUEUE" } else { "STREAM" },
                    sprig_common::names::quote(&spec.source_db),
                    sprig_common::names::quote(&spec.source_name),
                );
                let decl = sprig_logic::declare::parse(&full)?;
                sprig_logic::messages::check(
                    &decl,
                    if queue {
                        sprig_logic::declare::SourceKind::Queue
                    } else {
                        sprig_logic::declare::SourceKind::Stream
                    },
                )
                .await?;
                puts.push(mk(
                    What::Body,
                    json!({"text": full, "at": sprig_inherit::msg::store::now_ms()}),
                ));
                what_done = "body replaced".into();
            }
            AlterOp::Detach => {
                if !matches!(a.target, For::Own) {
                    return Err(inherited::not_overridable(
                        "DETACH is the member's own choice: run it in the member",
                    ));
                }
                let stopped = inherited::lifecycle::detach(svc, me, &def).await?;
                let v = cat.version + 1;
                self.after_own_change(v).await.ok();
                return Ok(Reply::Ok(
                    0,
                    vec![format!(
                        "{} {db}.{} is now this tenant's own, with its data; {stopped} \
                         inherited trigger(s) on it stopped",
                        a.kind.as_str(),
                        a.name
                    )],
                ));
            }
        }
        let mut writes: Vec<MsgWrite<'_>> = puts.iter().map(MsgWrite::Override).collect();
        for w in &deletes {
            writes.push(MsgWrite::DeleteOverrides {
                def: def.key(),
                target,
                applies_to: applies,
                what: *w,
            });
        }
        let v = inherited::commit(svc, me, &writes).await?;
        self.after_own_change(v).await?;
        // Members with an instance apply it now.
        let stream_key = match def.kind {
            DefKind::Stream => Some(def.key()),
            DefKind::Trigger => {
                let spec: TriggerSpec =
                    serde_json::from_value(def.definition.clone()).unwrap_or_default();
                chain
                    .resolve(&spec.source_db, DefKind::Stream, &spec.source_name)
                    .or_else(|| {
                        own.def(&spec.source_db, DefKind::Stream, &spec.source_name)
                            .cloned()
                    })
                    .map(|d| d.key())
            }
            _ => None,
        };
        let mut reached = 0;
        // Exchanges, queues, bindings and triggers on queues: the members'
        // vhosts apply it now.
        let mq_db = match def.kind {
            DefKind::Exchange | DefKind::Queue | DefKind::Binding => Some(def.db_name.clone()),
            DefKind::Trigger => {
                let spec: TriggerSpec =
                    serde_json::from_value(def.definition.clone()).unwrap_or_default();
                (spec.source_kind == "queue").then_some(spec.source_db)
            }
            _ => None,
        };
        if let Some(mdb) = mq_db {
            let mut members = std::collections::BTreeSet::new();
            for d in inherited::mq::declared_in(chain, &mdb)
                .into_iter()
                .chain(inherited::mq::defs_in(chain, &mdb))
            {
                for e in inherited::instances_of(svc, d.key()).await? {
                    members.insert(e.tenant);
                }
            }
            for t in members {
                let hit = t == target
                    || (applies == AppliesTo::Subtree
                        && inherited::dir_entry(svc, t)
                            .await?
                            .is_some_and(|d| d.path_ids().contains(&target.0)));
                if !hit {
                    continue;
                }
                inherited::mq::forget_applied(t, Some(&mdb));
                let tcat = inherited::catalog(svc, t).await?;
                let tchain = inherited::chain(svc, &tcat).await?;
                if Box::pin(inherited::mq::ensure_vhost(
                    svc,
                    &tchain,
                    &mdb,
                    Cause::Declare,
                ))
                .await?
                {
                    reached += 1;
                }
            }
        }
        if let Some(sk) = stream_key {
            for e in inherited::instances_of(svc, sk).await? {
                let hit = e.tenant == target
                    || (applies == AppliesTo::Subtree
                        && inherited::dir_entry(svc, e.tenant)
                            .await?
                            .is_some_and(|d| d.path_ids().contains(&target.0)));
                if !hit {
                    continue;
                }
                streams::forget_applied(e.tenant, sk);
                let tcat = inherited::catalog(svc, e.tenant).await?;
                let tchain = inherited::chain(svc, &tcat).await?;
                if let Some(d) = tchain.def(sk) {
                    Box::pin(streams::ensure(svc, &tchain, &d, Cause::Declare)).await?;
                    reached += 1;
                }
            }
        }
        Ok(Reply::Ok(
            0,
            vec![format!(
                "{} {db}.{}: {what_done} for {}; applied to {reached} instance(s)",
                a.kind.as_str(),
                a.name,
                match applies {
                    AppliesTo::Own => format!("tenant {target}"),
                    AppliesTo::Subtree => format!("the subtree of tenant {target}"),
                }
            )],
        ))
    }

    /// `SHOW INHERITED ...`.
    #[allow(clippy::too_many_lines)]
    async fn inherit_show(
        &mut self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        chain: &Chain,
        own: &MsgState,
        s: &ShowInherited,
    ) -> Result<Reply> {
        let me = cat.tenant;
        match s {
            ShowInherited::Definitions => {
                let mut rows = Vec::new();
                for d in own.defs.values() {
                    let n = inherited::instances_of(svc, d.key())
                        .await
                        .map_or(0, |v| v.len());
                    rows.push(vec![
                        Some(d.kind.as_str().to_owned()),
                        Some(d.db_name.clone()),
                        Some(d.name.clone()),
                        Some(serde_json::to_string(&d.scope).unwrap_or_default()),
                        Some(d.def_version.to_string()),
                        Some(d.state.as_str().to_owned()),
                        Some(
                            if matches!(
                                d.kind,
                                DefKind::Stream | DefKind::Exchange | DefKind::Queue
                            ) {
                                n.to_string()
                            } else {
                                "-".into()
                            },
                        ),
                    ]);
                }
                Ok(Reply::Rows(
                    vec![
                        "Kind",
                        "Database",
                        "Name",
                        "Scope",
                        "Version",
                        "State",
                        "Instances",
                    ],
                    rows,
                ))
            }
            ShowInherited::Instances => {
                // Instance rows aren't versioned: read them, not the
                // snapshot.
                let live: std::collections::BTreeMap<DefKey, sprig_inherit::msg::Instance> = {
                    use sprig_catalog::CatalogLocator;
                    let ep = svc.locator.writer(me).await?;
                    let mut conn = ep.pool.get_conn().await.map_err(|e| {
                        SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable")
                            .with_source(e)
                    })?;
                    let st = sprig_inherit::msg::MsgStore::new(ep.store.schema());
                    if st.migrated(&mut conn).await? {
                        st.instances(&mut conn)
                            .await?
                            .into_iter()
                            .map(|i| (i.def, i))
                            .collect()
                    } else {
                        std::collections::BTreeMap::new()
                    }
                };
                let _ = own;
                let mut rows = Vec::new();
                for kind in [
                    DefKind::Stream,
                    DefKind::Exchange,
                    DefKind::Queue,
                    DefKind::Binding,
                    DefKind::Trigger,
                    DefKind::Connection,
                    DefKind::Sink,
                ] {
                    for d in chain.reaching(kind) {
                        let i = live.get(&d.key());
                        let state =
                            if matches!(kind, DefKind::Trigger | DefKind::Binding | DefKind::Sink)
                                && chain.disabled(d.key())
                            {
                                "disabled".to_owned()
                            } else {
                                i.map_or_else(
                                    || {
                                        if matches!(kind, DefKind::Binding | DefKind::Connection) {
                                            "inherited".to_owned()
                                        } else {
                                            "none".to_owned()
                                        }
                                    },
                                    |i| {
                                        if i.physical
                                            .as_ref()
                                            .is_some_and(|p| p.get("blocked").is_some())
                                        {
                                            "blocked".to_owned()
                                        } else if let Some(p) = i.physical.as_ref().filter(|p| {
                                            p.get("waiting_config").is_some()
                                                || p.get("error").is_some()
                                        }) {
                                            // R9 (r9more): an inherited sink waiting for its
                                            // member's configuration (39241), or not deployable.
                                            if p.get("waiting_config").is_some() {
                                                "waiting_config".to_owned()
                                            } else {
                                                "error".to_owned()
                                            }
                                        } else {
                                            i.state.as_str().to_owned()
                                        }
                                    },
                                )
                            };
                        let from = if d.declarer == me {
                            "self".to_owned()
                        } else {
                            inherited::tenant_name(svc, d.declarer)
                                .await
                                .unwrap_or_else(|_| d.declarer.to_string())
                        };
                        rows.push(vec![
                            Some(kind.as_str().to_owned()),
                            Some(d.db_name.clone()),
                            Some(d.name.clone()),
                            Some(from),
                            Some(d.def_version.to_string()),
                            Some(state),
                            i.map(|i| i.instance_id.to_string()),
                            i.and_then(|i| i.physical.as_ref())
                                .and_then(|p| p.get("binding"))
                                .map(Value::to_string),
                        ]);
                    }
                }
                Ok(Reply::Rows(
                    vec![
                        "Kind",
                        "Database",
                        "Name",
                        "Inherited_from",
                        "Version",
                        "Instance",
                        "Instance_id",
                        "Binding",
                    ],
                    rows,
                ))
            }
            ShowInherited::Broadcasts(kind, db, name) => {
                let db = db
                    .clone()
                    .or_else(|| self.current_logical())
                    .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))?;
                let def = own
                    .def(&db, *kind, name)
                    .cloned()
                    .or_else(|| {
                        inherited::mq::declared_in(chain, &db)
                            .into_iter()
                            .chain(chain.reaching(DefKind::Stream))
                            .find(|d| d.kind == *kind && d.name == *name)
                    })
                    .ok_or_else(|| {
                        target_err(format!("no definition {} {db}.{name}", kind.as_str()))
                    })?;
                let rows = inherited::broadcast::show(svc, &def).await?;
                Ok(Reply::Rows(
                    vec![
                        "Broadcast",
                        "Publisher",
                        "Committed_ms",
                        "Form",
                        "Under",
                        "State",
                        "Delivered",
                        "Skipped",
                        "Error",
                        "Pending",
                    ],
                    rows,
                ))
            }
            ShowInherited::Overrides(filter) => {
                let anc = chain.ancestry();
                let mut rows = Vec::new();
                for l in &chain.levels {
                    let Some(st) = &l.state else { continue };
                    for o in &st.overrides {
                        let applies_here = o.target == me
                            || (o.applies_to == AppliesTo::Subtree && anc.contains(&o.target));
                        let mine = o.made_in == me;
                        if !applies_here && !mine {
                            continue;
                        }
                        let Some(d) = chain
                            .def(o.def)
                            .or_else(|| own.def_by_id(o.def.object).cloned())
                        else {
                            continue;
                        };
                        if let Some((k, db, n)) = filter
                            && (d.kind != *k
                                || &d.name != n
                                || db
                                    .as_deref()
                                    .is_some_and(|x| !x.eq_ignore_ascii_case(&d.db_name)))
                        {
                            continue;
                        }
                        rows.push(vec![
                            Some(d.kind.as_str().to_owned()),
                            Some(format!("{}.{}", d.db_name, d.name)),
                            Some(o.what.as_str().to_owned()),
                            Some(if o.what == What::Body {
                                "(replaced body)".to_owned()
                            } else {
                                o.value.to_string()
                            }),
                            Some(o.made_in.to_string()),
                            Some(o.target.to_string()),
                            Some(o.applies_to.as_str().to_owned()),
                        ]);
                    }
                }
                Ok(Reply::Rows(
                    vec![
                        "Kind",
                        "Name",
                        "What",
                        "Value",
                        "Set_in",
                        "For",
                        "Applies_to",
                    ],
                    rows,
                ))
            }
        }
    }
}

impl PassSession {
    /// `INSERT INTO db.s IN TENANT a ...`: a descendant's session publishes
    /// into an ancestor's ordinary stream (chapter 45, "One platform
    /// stream for every client"): only when the stream's owner set
    /// `stream.descendant_publish = on`; with `stream.descendant_key =
    /// origin` every record's key is the publishing tenant's name (39957
    /// for another); `x-sprig-origin-tenant` is stamped; under
    /// `residency.cross_tenant_messaging_strict` a pinned publisher can't
    /// store into a data center its policy forbids (39949).
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn publish_up(
        &mut self,
        cx: &mut ExecCtx<'_>,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        text: &str,
        i: &InsertInto,
        db: &str,
        name: &str,
        up: &str,
        lex_opts: &LexOptions,
    ) -> Result<Reply> {
        let me = cat.tenant;
        let (my_name, user, host) = {
            let ts = self
                .tenant
                .as_deref()
                .ok_or_else(|| SprigError::internal("no tenant"))?;
            if !allows(ts, cat, Action::Insert, db, name) {
                return Err(denied(ts, "INSERT", name));
            }
            (
                ts.principal.tenant_name.clone().unwrap_or_default(),
                ts.principal.user.clone(),
                ts.principal.client_ip.to_string(),
            )
        };
        let refused = || {
            SprigError::standard(
                1142,
                *b"42000",
                format!(
                    "INSERT command denied to user '{user}'@'{host}' for table '{name}' in tenant \
                     {up}: its owner doesn't take publishes from descendants"
                ),
            )
        };
        let owner = inherited::tenant_by_name(svc, up)
            .await?
            .filter(|o| cat.ancestry.iter().any(|a| a.id == *o))
            .ok_or_else(refused)?;
        let ocat = inherited::catalog(svc, owner).await?;
        let olctn = svc.settings.get(
            &sprig_catalog::ddl::LOWER_CASE_TABLE_NAMES,
            &ocat.setting_ctx(&svc.dc),
            Some(&ocat.settings),
        ) == 1;
        let odb = ocat
            .database(&database_key(db, olctn))
            .cloned()
            .ok_or_else(refused)?;
        let stream = ocat
            .ext
            .get::<sprig_streams::StreamsState>(sprig_streams::MODULE)
            .and_then(|st| st.get(odb.db_id, name, olctn).cloned())
            .filter(|s| s.state == sprig_streams::StreamState::Active)
            .ok_or_else(refused)?;
        sprig_streams::publish::check_target(&stream)?;
        // Streams have no `objects` row: their database is named here.
        let octx = ocat
            .object_ctx(&svc.dc, sprig_common::ids::ObjectId(stream.id.0))
            .with_db(stream.db);
        let consent = svc.settings.get(
            &sprig_inherit::msg::settings::STREAM_DESCENDANT_PUBLISH,
            &octx,
            Some(&ocat.settings),
        );
        if consent != "on" {
            return Err(refused());
        }
        let origin_key = svc.settings.get(
            &sprig_inherit::msg::settings::STREAM_DESCENDANT_KEY,
            &octx,
            Some(&ocat.settings),
        ) == "origin";
        // Residency, only under the strict platform setting.
        let strict = svc.settings.get(
            &sprig_inherit::msg::settings::CROSS_TENANT_MESSAGING_STRICT,
            &cat.setting_ctx(&svc.dc),
            Some(&cat.settings),
        ) == "on";
        if strict {
            inherited::check_residency(svc, me, owner, SprigCode::StreamResidency).await?;
        }
        let in_txn = self.offsets_in_transaction();
        let (cols, tail) = insert_shape(text, i)?;
        if cols.contains(&Col::Tenant) {
            return Err(SprigError::standard(
                1064,
                *b"42000",
                "IN TENANT and a tenant column can't be used together",
            ));
        }
        let q = rows_query(&tail, lex_opts)?;
        let got = self.run_checked(cx, &q, false).await?;
        let max = svc.settings.get(
            &sprig_streams::settings::MAX_MESSAGE_BYTES,
            &octx,
            Some(&ocat.settings),
        );
        let mut recs = Vec::with_capacity(got.rows.len());
        for r in got.rows {
            if r.len() != cols.len() {
                return Err(SprigError::standard(
                    1136,
                    *b"21S01",
                    "Column count doesn't match value count at row 1",
                ));
            }
            let mut rec = Publish::default();
            let mut headers = None;
            for (c, v) in cols.iter().zip(r) {
                match c {
                    Col::Key => rec.key = v,
                    Col::Value => rec.value = v,
                    Col::Headers => headers = v,
                    Col::Partition => {
                        rec.partition = v
                            .and_then(|b| String::from_utf8(b).ok())
                            .and_then(|s| s.trim().parse().ok());
                    }
                    Col::Tenant => {}
                }
            }
            if origin_key {
                match &rec.key {
                    None => rec.key = Some(my_name.as_bytes().to_vec()),
                    Some(k) if k.as_slice() == my_name.as_bytes() => {}
                    Some(k) => {
                        return Err(SprigError::new(
                            SprigCode::StreamOriginKey,
                            format!(
                                "ER_SPRIG_STREAM_ORIGIN_KEY: a record from tenant {my_name} is keyed \
                                 by its own name (stream.descendant_key = origin), not '{}'",
                                String::from_utf8_lossy(k)
                            ),
                        ));
                    }
                }
            }
            let stamped = stamp(headers.as_deref(), up, &my_name)?;
            sprig_streams::publish::check_record(
                &stream,
                rec.key.as_deref(),
                rec.value.as_ref().map_or(0, Vec::len),
                stamped.len(),
                max,
            )?;
            rec.headers = Some(stamped);
            recs.push(rec);
        }
        let n = recs.len() as u64;
        if n == 0 {
            return Ok(Reply::Ok(0, Vec::new()));
        }
        if in_txn {
            let dest = inherited::outbox::Dest::Stream {
                tenant: owner.0,
                stream: stream.id.0,
            };
            self.xo_publish(cx, svc, cat, db, &dest, recs).await?;
            return Ok(Reply::Ok(n, Vec::new()));
        }
        let slot = stream
            .outbox
            .first()
            .ok_or_else(|| SprigError::internal("a stream without an outbox"))?;
        let schema = slot.schema(owner, stream.db).to_string();
        let ins = sprig_streams::publish::outbox_insert(&stream, Some(&schema), &recs)
            .ok_or_else(|| SprigError::internal("an empty publish"))?;
        let cluster = svc.locator.cluster(owner).await?;
        let pool = svc.locator.ctl_pool(&cluster).await?;
        let mut c = pool
            .get_conn()
            .await
            .map_err(sprig_streams::maria::map_err)?;
        c.query_drop(ins)
            .await
            .map_err(sprig_streams::maria::map_err)?;
        metrics::counter!("sprig_inherit_publishes_total", "kind" => "stream", "path" => "direct")
            .increment(n);
        Ok(Reply::Ok(n, Vec::new()))
    }
}

impl PassSession {
    /// Writes `recs` for `dest` into this tenant's cross-tenant outbox,
    /// through this session (in its transaction): in database `db` when the
    /// tenant has it, else the session's current one. The commit hook
    /// forwards them ([`inherited::outbox`]).
    async fn xo_publish(
        &mut self,
        cx: &mut ExecCtx<'_>,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        db: &str,
        dest: &inherited::outbox::Dest,
        recs: Vec<Publish>,
    ) -> Result<()> {
        let me = cat.tenant;
        let lctn = self
            .tenant
            .as_deref()
            .is_some_and(TenantSession::lower_case_table_names);
        let current = self.current_logical();
        let row = cat
            .database(&database_key(db, lctn))
            .or_else(|| {
                current
                    .as_deref()
                    .and_then(|c| cat.database(&database_key(c, lctn)))
            })
            .cloned()
            .ok_or_else(|| {
                SprigError::standard(
                    1046,
                    *b"3D000",
                    format!(
                        "No database selected: a publish into another tenant inside a \
                         transaction goes through an outbox in one of this tenant's \
                         databases ('{db}' or the current one)"
                    ),
                )
            })?;
        let schema = sprig_catalog::ddl::shard_schemas(me, &row)
            .first()
            .map(ToString::to_string)
            .ok_or_else(|| SprigError::internal("a database without a shard"))?;
        let cluster = svc.locator.cluster(me).await?;
        inherited::outbox::ensure_table(svc, me, &schema, &cluster).await?;
        let rows: Vec<([u8; 16], inherited::outbox::Dest, Publish)> = recs
            .into_iter()
            .map(|r| (inherited::outbox::new_id(), dest.clone(), r))
            .collect();
        let ids = rows.iter().map(|(i, _, _)| *i).collect();
        let sql = inherited::outbox::insert_sql(&row.name, &rows);
        self.run_checked(cx, &sql, true).await?;
        inherited::outbox::note(u64::from(cx.session.id.0), svc, me, &schema, ids);
        Ok(())
    }
}

impl PassSession {
    /// `CREATE EXCHANGE | QUEUE ... FOR EACH TENANT`, `BIND QUEUE |
    /// EXCHANGE ... FOR EACH TENANT` (chapter 50, "Inherited exchanges,
    /// queues and bindings"): definitions in this tenant; members make
    /// their instances on first use.
    #[allow(clippy::too_many_lines)]
    async fn define_mq(
        &mut self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        own: &MsgState,
        sql: &str,
        lex_opts: &LexOptions,
        f: &grammar::ForEach,
    ) -> Result<Reply> {
        let rest = f.strip(sql);
        let decl = grammar::mq_decl(&rest, lex_opts)?.ok_or_else(|| {
            SprigError::standard(
                1064,
                *b"42000",
                "expected CREATE EXCHANGE, CREATE QUEUE or BIND ... FOR EACH TENANT",
            )
        })?;
        if !f.grants.is_empty() {
            return Err(SprigError::standard(
                1064,
                *b"42000",
                "GRANTS belongs to triggers and sinks",
            ));
        }
        let ts = self
            .tenant
            .as_deref()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let lctn = ts.lower_case_table_names();
        let current = self.current_logical();
        let db_of = |d: &Option<String>| {
            d.clone()
                .or_else(|| current.clone())
                .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))
        };
        let (kind, db, name, definition, if_not_exists) = match &decl {
            grammar::MqDecl::Exchange {
                if_not_exists,
                db,
                name,
                kind,
                internal,
                arguments,
            } => {
                if sprig_queues::topology::ExKind::parse(kind).is_none() {
                    return Err(SprigError::standard(
                        1064,
                        *b"42000",
                        format!("unknown exchange type '{kind}'"),
                    ));
                }
                let spec = sprig_inherit::msg::ExchangeSpec {
                    kind: kind.clone(),
                    durable: true,
                    internal: *internal,
                    arguments: arguments.iter().cloned().collect(),
                };
                (
                    DefKind::Exchange,
                    db_of(db)?,
                    name.clone(),
                    serde_json::to_value(&spec).unwrap_or_default(),
                    *if_not_exists,
                )
            }
            grammar::MqDecl::Queue {
                if_not_exists,
                db,
                name,
                arguments,
            } => {
                if name.is_empty() || name.starts_with("amq.") {
                    return Err(SprigError::standard(
                        1103,
                        *b"42000",
                        format!("Incorrect queue name '{name}'"),
                    ));
                }
                if let Some((_, t)) = arguments.iter().find(|(k, _)| k == "x-queue-type")
                    && !matches!(t.as_str(), Some("classic" | "quorum"))
                {
                    return Err(not_built(&format!(
                        "x-queue-type {t} on a FOR EACH TENANT queue (classic and quorum are \
                         served)"
                    )));
                }
                let spec = sprig_inherit::msg::QueueSpec {
                    arguments: arguments.iter().cloned().collect(),
                };
                (
                    DefKind::Queue,
                    db_of(db)?,
                    name.clone(),
                    serde_json::to_value(&spec).unwrap_or_default(),
                    *if_not_exists,
                )
            }
            grammar::MqDecl::Bind {
                bind: true,
                db,
                dest_kind,
                dest,
                source,
                key,
                arguments,
            } => {
                let db = db_of(db)?;
                // Both ends are definitions of this tenant (chapter 50: a
                // binding between two inherited objects is inherited).
                let dk = if dest_kind == "exchange" {
                    DefKind::Exchange
                } else {
                    DefKind::Queue
                };
                if own.def(&db, DefKind::Exchange, source).is_none()
                    || own.def(&db, dk, dest).is_none()
                {
                    return Err(SprigError::standard(
                        1146,
                        *b"42S02",
                        format!(
                            "a FOR EACH TENANT binding joins two FOR EACH TENANT definitions of \
                             this tenant in {db}; exchange '{source}' or {dest_kind} '{dest}' \
                             isn't one"
                        ),
                    ));
                }
                let spec = sprig_inherit::msg::BindingSpec {
                    source: source.clone(),
                    dest: dest.clone(),
                    dest_kind: dest_kind.clone(),
                    key: key.clone(),
                    arguments: arguments.iter().cloned().collect(),
                };
                let name = spec.def_name();
                (
                    DefKind::Binding,
                    db,
                    name,
                    serde_json::to_value(&spec).unwrap_or_default(),
                    true,
                )
            }
            grammar::MqDecl::Bind { bind: false, .. } => {
                return Err(SprigError::standard(
                    1064,
                    *b"42000",
                    "UNBIND takes no FOR EACH TENANT: it drops the binding definition",
                ));
            }
        };
        if cat.database(&database_key(&db, lctn)).is_none() {
            return Err(SprigError::standard(
                1049,
                *b"42000",
                format!("Unknown database '{db}'"),
            ));
        }
        let check_name = if kind == DefKind::Binding {
            definition
                .get("dest")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        } else {
            name.clone()
        };
        if !allows(ts, cat, Action::Create, &db, &check_name) {
            return Err(denied(ts, "CREATE", &check_name));
        }
        let user = ts.principal.user_id.map_or(0, |u| u.0);
        if own.def(&db, kind, &name).is_some() {
            if if_not_exists {
                return Ok(Reply::Ok(0, Vec::new()));
            }
            return Err(SprigError::standard(
                1050,
                *b"42S01",
                format!("{} '{db}.{name}' is already defined", kind.as_str()),
            ));
        }
        let scope = scope_of(svc, cat.tenant, f).await?;
        check_quota(svc, cat, own)?;
        let def = Definition {
            declarer: cat.tenant,
            object_id: next_object_id(svc, cat.tenant).await?,
            kind,
            db_name: db.clone(),
            name: name.clone(),
            scope,
            definition,
            grants: None,
            def_version: 1,
            state: DefState::Active,
            drop_after_ms: None,
            created_by: user,
            created_at_ms: sprig_inherit::msg::store::now_ms(),
            changed_version: 0,
        };
        let v = inherited::commit(svc, cat.tenant, &[MsgWrite::Def(&def)]).await?;
        self.after_own_change(v).await?;
        // Members that already made this vhost's instances get it now, in
        // the background.
        let n = mq_rollout(svc, cat.tenant, &db, None).await?;
        tracing::info!(tenant = %cat.tenant, def = %def.key(), kind = kind.as_str(), name = %name, "inherited {} defined", kind.as_str());
        Ok(Reply::Ok(
            0,
            vec![format!(
                "{} {db}.{name} is defined for every tenant in scope; each member's instance is \
                 made on its first use of the vhost ({n} member(s) updated now)",
                kind.as_str()
            )],
        ))
    }

    /// `UNBIND QUEUE | EXCHANGE d FROM EXCHANGE s [KEY 'k']` naming a binding
    /// definition of this tenant: it goes, and so does each member's
    /// binding (a binding holds no data).
    async fn unbind_def(
        &mut self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        own: &MsgState,
        sql: &str,
        lex_opts: &LexOptions,
    ) -> Result<Option<Reply>> {
        let Some(grammar::MqDecl::Bind {
            bind: false,
            db,
            dest_kind,
            dest,
            source,
            key,
            arguments,
        }) = grammar::mq_decl(sql, lex_opts)?
        else {
            return Ok(None);
        };
        let Some(db) = db.or_else(|| self.current_logical()) else {
            return Ok(None);
        };
        let spec = sprig_inherit::msg::BindingSpec {
            source,
            dest,
            dest_kind,
            key,
            arguments: arguments.into_iter().collect(),
        };
        let Some(def) = own.def(&db, DefKind::Binding, &spec.def_name()).cloned() else {
            return Ok(None);
        };
        {
            let ts = self
                .tenant
                .as_deref()
                .ok_or_else(|| SprigError::internal("no tenant"))?;
            if !allows(ts, cat, Action::Drop, &db, &spec.dest) {
                return Err(denied(ts, "DROP", &spec.dest));
            }
        }
        let v = inherited::commit(svc, cat.tenant, &[MsgWrite::DeleteDef(def.object_id)]).await?;
        self.after_own_change(v).await?;
        let n = mq_rollout(svc, cat.tenant, &db, Some(&spec)).await?;
        Ok(Some(Reply::Ok(
            0,
            vec![format!(
                "binding definition {db}.{} dropped; removed in {n} member(s)",
                def.name
            )],
        )))
    }

    /// A `sprig.mq_publish` in this session naming an inherited exchange
    /// (or, through the default exchange, an inherited queue) of vhost
    /// database `db` (chapter 50, "Publishing by target tenant"):
    /// `Some(routed)` when it was handled here (a targeted publish, or a
    /// refusal), `None` to go on with the ordinary path, with `m` stamped
    /// when it goes into this tenant's own instance.
    #[allow(clippy::too_many_lines)]
    pub(super) async fn inherit_mq_publish(
        &mut self,
        cx: &mut ExecCtx<'_>,
        m: &mut sprig_proto_amqp::message::Message,
        mandatory: bool,
        vhost: &str,
    ) -> Result<Option<u32>> {
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Ok(None);
        };
        let cat = ts.refresh().await?;
        let own = inherited::state_of(&cat);
        if own.is_empty() && cat.ancestry.is_empty() {
            return Ok(None);
        }
        let svc = Arc::clone(ts.services());
        let my_name = ts.principal.tenant_name.clone().unwrap_or_default();
        let db = sprig_proto_amqp::mem::default_database(
            vhost,
            &svc.settings.get(
                &crate::amqp::DEFAULT_VHOST_DATABASE,
                &cat.setting_ctx(&svc.dc),
                Some(&cat.settings),
            ),
        );
        let chain = inherited::chain(&svc, &cat).await?;
        let Some((kind, name)) = inherited::mq::named_def(&chain, &db, &m.exchange, &m.routing_key)
        else {
            return Ok(None);
        };
        let me = cat.tenant;
        let mine = inherited::mq::defs_in(&chain, &db)
            .iter()
            .any(|d| d.kind == kind && d.name == name);
        let target = inherited::mq::target_of(m);
        match target {
            Some((t, key)) if t == "*" => {
                let ts = self
                    .tenant
                    .as_deref()
                    .ok_or_else(|| SprigError::internal("no tenant"))?;
                if !allows(ts, &cat, Action::Insert, &db, &name) {
                    return Err(denied(ts, "INSERT", &name));
                }
                let def = inherited::mq::declared_in(&chain, &db)
                    .into_iter()
                    .find(|d| d.kind == kind && d.name == name)
                    .ok_or_else(|| target_err(format!("no definition {db}.{name}")))?;
                let rec = Publish {
                    key: Some(key.into_bytes()),
                    value: Some(m.body.to_vec()),
                    headers: Some(inherited::mq::xo_headers(m)),
                    partition: None,
                    ts_ms: None,
                };
                let form = inherited::broadcast::Form::default();
                if self.offsets_in_transaction() {
                    let bytes = rec.value.as_ref().map_or(0, Vec::len);
                    inherited::broadcast::check_limits(&svc, &def, me, bytes, form).await?;
                    let dest = inherited::outbox::Dest::Broadcast {
                        def_tenant: def.declarer.0,
                        def_object: def.object_id,
                        publisher: me.0,
                        form,
                    };
                    let current = self.current_logical().unwrap_or_else(|| db.clone());
                    self.xo_publish(cx, &svc, &cat, &current, &dest, vec![rec])
                        .await?;
                } else {
                    inherited::broadcast::publish_now(&svc, &def, me, form, &rec).await?;
                }
                Ok(Some(1))
            }
            Some((t, key)) if t != my_name && t != me.0.to_string() => {
                let ts = self
                    .tenant
                    .as_deref()
                    .ok_or_else(|| SprigError::internal("no tenant"))?;
                if !allows(ts, &cat, Action::Insert, &db, &m.exchange) {
                    return Err(denied(ts, "INSERT", &m.exchange));
                }
                let (tid, tname, tchain) =
                    inherited::mq::resolve_target(&svc, me, &t, &db, kind, &name).await?;
                let stamped = inherited::mq::stamp(m, &key, &tname, &my_name)
                    .map_err(|e| SprigError::standard(3140, *b"22032", e.text))?;
                if !self.offsets_in_transaction() {
                    return Box::pin(inherited::mq::publish_into(
                        &svc, &tchain, &db, stamped, mandatory,
                    ))
                    .await
                    .map(Some);
                }
                // Inside a transaction: through this tenant's cross-tenant
                // outbox; forwarded into the member's queue outbox once
                // the transaction commits.
                let strict = svc.settings.get(
                    &sprig_inherit::msg::settings::CROSS_TENANT_MESSAGING_STRICT,
                    &cat.setting_ctx(&svc.dc),
                    Some(&cat.settings),
                ) == "on";
                if strict {
                    inherited::check_residency(&svc, tid, me, SprigCode::InheritResidency).await?;
                }
                let rec = Publish {
                    key: Some(stamped.routing_key.clone().into_bytes()),
                    value: Some(stamped.body.to_vec()),
                    headers: Some(inherited::mq::xo_headers(&stamped)),
                    partition: None,
                    ts_ms: None,
                };
                let dest = inherited::outbox::Dest::Exchange {
                    tenant: tid.0,
                    db: db.clone(),
                    exchange: stamped.exchange.clone(),
                };
                let current = self.current_logical().unwrap_or_else(|| db.clone());
                self.xo_publish(cx, &svc, &cat, &current, &dest, vec![rec])
                    .await?;
                Ok(Some(1))
            }
            Some((_, key)) if mine => {
                Box::pin(inherited::mq::ensure_vhost(
                    &svc,
                    &chain,
                    &db,
                    Cause::Publish,
                ))
                .await?;
                *m = inherited::mq::stamp(m, &key, &my_name, &my_name)
                    .map_err(|e| SprigError::standard(3140, *b"22032", e.text))?;
                Ok(None)
            }
            None if mine => {
                Box::pin(inherited::mq::ensure_vhost(
                    &svc,
                    &chain,
                    &db,
                    Cause::Publish,
                ))
                .await?;
                let key = m.routing_key.clone();
                *m = inherited::mq::stamp(m, &key, &my_name, &my_name)
                    .map_err(|e| SprigError::standard(3140, *b"22032", e.text))?;
                Ok(None)
            }
            _ => Err(target_err(format!(
                "{} {db}.{name} is a FOR EACH TENANT definition: name the member with the \
                 x-sprig-tenant header or a routing key '@tenant:key'",
                kind.as_str()
            ))),
        }
    }
}

impl PassSession {
    /// `INSERT INTO s FOR ALL TENANTS [WITH INSTANCE] [UNDER t] ...` (or
    /// `FOR TENANT '*'`): each row is one broadcast (chapter 42,
    /// "Broadcast"): its log row now, or, inside a transaction, a row of
    /// this tenant's cross-tenant outbox that becomes it at commit.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn inherit_broadcast(
        &mut self,
        cx: &mut ExecCtx<'_>,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        text: &str,
        i: &InsertInto,
        def: &Arc<Definition>,
        with_instance: bool,
        under: Option<&str>,
        lex_opts: &LexOptions,
    ) -> Result<Reply> {
        let me = cat.tenant;
        {
            let ts = self
                .tenant
                .as_deref()
                .ok_or_else(|| SprigError::internal("no tenant"))?;
            if !allows(ts, cat, Action::Insert, &def.db_name, &def.name) {
                return Err(denied(ts, "INSERT", &def.name));
            }
        }
        if def.state != DefState::Active {
            return Err(inherited::dropping(format!(
                "stream {}.{} is being dropped",
                def.db_name, def.name
            )));
        }
        let under = match under {
            None => None,
            Some(n) => {
                let id = inherited::tenant_by_name(svc, n)
                    .await?
                    .ok_or_else(|| target_err(format!("UNDER {n}: no descendant of that name")))?;
                let below = inherited::dir_entry(svc, id)
                    .await?
                    .is_some_and(|e| e.path_ids().contains(&me.0));
                if !below {
                    return Err(target_err(format!("UNDER {n}: no descendant of that name")));
                }
                Some(id.0)
            }
        };
        let form = inherited::broadcast::Form {
            with_instance,
            under,
        };
        let (cols, tail) = insert_shape(text, i)?;
        if cols.contains(&Col::Tenant) {
            return Err(SprigError::standard(
                1064,
                *b"42000",
                "FOR ALL TENANTS and a tenant column can't be used together",
            ));
        }
        let q = rows_query(&tail, lex_opts)?;
        let got = self.run_checked(cx, &q, false).await?;
        let mut recs = Vec::with_capacity(got.rows.len());
        for r in got.rows {
            let mut rec = Publish::default();
            for (c, v) in cols.iter().zip(r) {
                match c {
                    Col::Key => rec.key = v,
                    Col::Value => rec.value = v,
                    Col::Headers => {
                        rec.headers = v.map(|h| String::from_utf8_lossy(&h).into_owned());
                    }
                    Col::Partition | Col::Tenant => {}
                }
            }
            recs.push(rec);
        }
        let in_txn = self.offsets_in_transaction();
        let n = recs.len() as u64;
        let mut ids = Vec::new();
        for rec in recs {
            if in_txn {
                let bytes = rec.key.as_ref().map_or(0, Vec::len)
                    + rec.value.as_ref().map_or(0, Vec::len)
                    + rec.headers.as_ref().map_or(0, String::len);
                inherited::broadcast::check_limits(svc, def, me, bytes, form).await?;
                let dest = inherited::outbox::Dest::Broadcast {
                    def_tenant: def.declarer.0,
                    def_object: def.object_id,
                    publisher: me.0,
                    form,
                };
                let db = def.db_name.clone();
                self.xo_publish(cx, svc, cat, &db, &dest, vec![rec]).await?;
            } else {
                let bid = inherited::broadcast::publish_now(svc, def, me, form, &rec).await?;
                ids.push(inherited::broadcast::id_text(&bid));
            }
        }
        Ok(Reply::Ok(
            n,
            if in_txn {
                vec![
                    "broadcast(s) logged when the transaction commits, then delivered to every \
                     member in the background"
                        .into(),
                ]
            } else {
                ids.iter()
                    .map(|b| {
                        format!(
                            "broadcast {b} logged; members get it in the background (SHOW \
                             INHERITED BROADCASTS FOR STREAM {}.{})",
                            def.db_name, def.name
                        )
                    })
                    .collect()
            },
        ))
    }
}

impl PassSession {
    /// `SELECT ... FROM s FAN IN ...` (chapter 42, "Fan-in"): the members'
    /// instances as one derived table (see [`inherited::fanin`]).
    async fn fan_in_select(
        &mut self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        chain: &Chain,
        own: &MsgState,
        sql: &str,
        lex_opts: &LexOptions,
    ) -> Result<Reply> {
        let (r, only) = inherited::fanin::find(sql, lex_opts)
            .ok_or_else(|| SprigError::internal("no FAN IN"))?;
        let db =
            r.db.clone()
                .or_else(|| self.current_logical())
                .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))?;
        let ts = self
            .tenant
            .as_deref()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        if !allows(ts, cat, Action::Select, &db, &r.name) {
            return Err(denied(ts, "SELECT", &r.name));
        }
        let grants = crate::tenant::authz::privileges(ts, cat);
        if !grants.unrestricted() && !grants.allows(Action::FanIn, Target::Tenant) {
            return Err(SprigError::standard(
                1227,
                *b"42000",
                "Access denied; you need (at least one of) the SPRIG_FAN_IN privilege(s) for this \
                 operation (granted by name; GRANT ALL doesn't include it)",
            ));
        }
        let def = inherited::fanin::definition(chain, own, &db, &r.name).ok_or_else(|| {
            target_err(format!(
                "{db}.{} isn't a FOR EACH TENANT stream this tenant declares or is a member of",
                r.name
            ))
        })?;
        let max = svc.settings.get(
            &sprig_inherit::msg::settings::FAN_IN_SQL_MAX_TENANTS,
            &cat.setting_ctx(&svc.dc),
            Some(&cat.settings),
        );
        let (table, n) =
            inherited::fanin::derived(svc, cat.tenant, &def, &only, max, &r.name).await?;
        tracing::debug!(tenant = %cat.tenant, def = %def.key(), members = n, "fan-in read");
        Ok(Reply::Forward(format!(
            "{}{table}{}",
            &sql[..r.range.start],
            &sql[r.range.end..]
        )))
    }
}

/// Re-applies database `db`'s inherited exchanges, queues and bindings in
/// every member of `declarer`'s definitions that has instances there, in
/// the background (a new definition, an override); with `unbind`, also
/// removes that binding in each first. The number of members.
async fn mq_rollout(
    svc: &Arc<TenantServices>,
    declarer: TenantId,
    db: &str,
    unbind: Option<&sprig_inherit::msg::BindingSpec>,
) -> Result<usize> {
    let mut members: std::collections::BTreeSet<TenantId> = std::collections::BTreeSet::new();
    let dcat = inherited::catalog(svc, declarer).await?;
    let dchain = inherited::chain(svc, &dcat).await?;
    for d in inherited::mq::declared_in(&dchain, db) {
        for e in inherited::instances_of(svc, d.key()).await? {
            members.insert(e.tenant);
        }
    }
    let n = members.len();
    let svc = Arc::clone(svc);
    let db = db.to_owned();
    let unbind = unbind.cloned();
    tokio::spawn(async move {
        for t in members {
            inherited::mq::forget_applied(t, Some(&db));
            let r = async {
                if let Some(b) = &unbind {
                    // Only the binding goes; the vhost's other instances are
                    // applied at its next use (a drop is often followed by
                    // more drops).
                    return inherited::mq::remove_binding(&svc, t, &db, b)
                        .await
                        .map(|()| true);
                }
                let cat = inherited::catalog(&svc, t).await?;
                let chain = inherited::chain(&svc, &cat).await?;
                Box::pin(inherited::mq::ensure_vhost(
                    &svc,
                    &chain,
                    &db,
                    Cause::Declare,
                ))
                .await
            }
            .await;
            if let Err(e) = r {
                tracing::warn!(tenant = %t, %db, error = %e, "inherited exchanges and queues not updated; the next use retries");
            }
        }
    });
    Ok(n)
}

/// The id that marks an inherited stream without an instance in a read.
const NO_INSTANCE: u64 = u64::MAX - 42;

/// A read naming an inherited stream the member has no instance of yet:
/// the statement with that stream as an empty table (chapter 42: reads
/// never create an instance; an instance not created yet has no records).
/// `None` when the read names none.
fn empty_reads(
    sql: &str,
    lex_opts: &LexOptions,
    cat: &TenantCatalog,
    chain: &Chain,
    own: &MsgState,
    current: Option<&str>,
) -> Option<String> {
    let reaching = chain.reaching(DefKind::Stream);
    if !reaching.iter().any(|d| sql.contains(d.name.as_str())) {
        return None;
    }
    let streams = cat
        .ext
        .get::<sprig_streams::StreamsState>(sprig_streams::MODULE)
        .cloned()
        .unwrap_or_default();
    let lookup = |db: Option<&str>, name: &str| -> Option<sprig_streams::StreamId> {
        let db = db.or(current)?.to_owned();
        let d = reaching
            .iter()
            .find(|d| d.db_name.eq_ignore_ascii_case(&db) && d.name == name)?;
        // An instance that is usable: its stream is in the snapshot.
        if let Some(i) = own.instances.get(&d.key())
            && streams.all().any(|s| s.id.0 == i.instance_id)
        {
            return None;
        }
        // The member's own stream of the name shadows it.
        if let Some(row) = cat.database(&database_key(&db, false))
            && streams.get(row.db_id, name, true).is_some()
        {
            return None;
        }
        Some(sprig_streams::StreamId(NO_INSTANCE))
    };
    let r = sprig_streams::read::rewrite_select(sql, lex_opts, &lookup).ok()??;
    let log = sprig_common::names::quote(&sprig_streams::model::log_table(
        sprig_streams::StreamId(NO_INSTANCE),
    ));
    let empty = "(SELECT CAST(NULL AS UNSIGNED) AS partition_no, CAST(NULL AS UNSIGNED) AS \
                 offset_no, CAST(NULL AS DATETIME(3)) AS ts, CAST(NULL AS BINARY) AS k, \
                 CAST(NULL AS BINARY) AS v, CAST(NULL AS CHAR) AS headers FROM DUAL WHERE 0) \
                 AS __sprig_no_instance";
    let mut out = r.sql;
    for (db, _, _) in &r.streams {
        if let Some(d) = db {
            out = out.replace(&format!("{}.{log}", sprig_common::names::quote(d)), empty);
        }
    }
    Some(out.replace(&log, empty))
}

/// 39896 for a target that isn't a member (the same text whether or not
/// a tenant of that name exists).
fn no_member(name: &str, def: &Definition) -> SprigError {
    target_err(format!(
        "'{name}' isn't a member of {}.{} below this tenant",
        def.db_name, def.name
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_become_a_union() {
        let o = LexOptions::mariadb();
        assert_eq!(
            rows_query(" VALUES (1, JSON_OBJECT('a', (2))), (3, 'x,y')", &o).unwrap(),
            "SELECT 1, JSON_OBJECT('a', (2)) UNION ALL SELECT 3, 'x,y'"
        );
        assert_eq!(
            rows_query("SELECT a, b FROM t;", &o).unwrap(),
            "SELECT a, b FROM t"
        );
        assert!(rows_query("VALUES (1", &o).is_err());
        assert!(rows_query("SET a = 1", &o).is_err());
        assert!(rows_query("VALUES (1) junk", &o).is_err());
    }

    #[test]
    fn headers_are_stamped_over_the_clients() {
        let h = stamp(
            Some(br#"{"x-sprig-origin-tenant": "forged", "trace": "t1"}"#),
            "client_x",
            "system",
        )
        .unwrap();
        let v: Value = serde_json::from_str(&h).unwrap();
        assert_eq!(v["x-sprig-tenant"], "client_x");
        assert_eq!(v["x-sprig-origin-tenant"], "system");
        assert_eq!(v["trace"], "t1");
        assert!(stamp(Some(b"[1]"), "a", "b").is_err());
        assert!(stamp(None, "a", "b").is_ok());
    }

    #[test]
    fn insert_shapes() {
        let o = LexOptions::mariadb();
        let sql = "INSERT INTO s (tenant, key, value) SELECT t, k, v FROM x";
        let sprig_streams::grammar::Stmt::Insert(i) =
            sprig_streams::grammar::classify(sql.as_bytes(), &o).unwrap()
        else {
            panic!("not an insert");
        };
        let (cols, tail) = insert_shape(sql, &i).unwrap();
        assert_eq!(cols, vec![Col::Tenant, Col::Key, Col::Value]);
        assert_eq!(tail.trim(), "SELECT t, k, v FROM x");
        let sql = "INSERT INTO s (key, ts) VALUES (1, NOW())";
        let sprig_streams::grammar::Stmt::Insert(i) =
            sprig_streams::grammar::classify(sql.as_bytes(), &o).unwrap()
        else {
            panic!("not an insert");
        };
        assert!(insert_shape(sql, &i).is_err());
    }
}
