//! `FOR EACH TENANT` connections and sinks in a tenant session (chapter
//! 51, "Inherited sinks and connections"; chapter 42; R9, r9more). These
//! statements are taken in chapter 51's step, before chapter 12's capture
//! point, so credentials are sealed and never logged:
//!
//! ```text
//! CREATE CONNECTION c FOR EACH TENANT [scope] TYPE ... WITH (...) [CREDENTIALS (k = REQUIRED)]
//! CREATE SINK s FOR EACH TENANT [scope] [GRANTS (...)] FROM STREAM d ... TO ...
//! ALTER CONNECTION c SET | RESET (...)    on a definition: a new version
//! ALTER SINK s SET | RESET (...)          on a definition: a new version
//! DROP SINK s | DROP CONNECTION c         on a definition: every member's instance
//!                                         goes (a connection only when no sink
//!                                         definition names it, 39215)
//! ALTER INHERITED CONNECTION c [FOR TENANT t | FOR SUBTREE [t]]
//!   SET (...) [CREDENTIALS (...)] | SET CREDENTIALS (...) | RESET [(...)]
//! ALTER INHERITED SINK s [FOR ...] SET (...) | RESET [(...)] | DISABLE | ENABLE
//! ```
//!
//! Members get their instances from `inherited::sinks` when their stream
//! instance is used, and at once after each of these statements.

use std::sync::Arc;

use serde_json::{Map, Value, json};
use sprig_auth::authz::{Action, Target};
use sprig_catalog::TenantCatalog;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_inherit::msg::grammar as ig;
use sprig_inherit::msg::{
    AppliesTo, Chain, DefKey, DefKind, DefState, Definition, MsgState, Override, What,
};
use sprig_sinks::grammar::{self as sg, InheritTarget, InheritedOp, OptVal, SourceAst, TargetAst};
use sprig_sinks::sql::Handled;

use super::PassSession;
use super::inherit_msg::{check_quota, next_object_id, rollout, scope_of};
use crate::inherited::sinks::{ConnSpec, OptSpec, SinkSpec, seal_credentials, source_of};
use crate::inherited::{self, MsgWrite, target_err};
use crate::tenant::TenantServices;

fn err(code: SprigCode, m: impl Into<String>) -> SprigError {
    SprigError::new(code, m.into())
}

fn ok() -> Handled {
    Handled::Ok { affected: 0 }
}

/// The connections a sink target names: (name, platform).
fn conns_of(t: &TargetAst) -> Vec<(String, bool)> {
    match t {
        TargetAst::Webhook { conn, .. }
        | TargetAst::Kafka { conn, .. }
        | TargetAst::S3 { conn, .. }
        | TargetAst::Database { conn, .. } => vec![(conn.name.clone(), conn.platform)],
        TargetAst::Script { using, .. } => {
            using.iter().map(|c| (c.name.clone(), c.platform)).collect()
        }
    }
}

fn opt_text(v: &OptVal) -> Result<String> {
    match v {
        OptVal::Text(s) | OptVal::Bare(s) => Ok(s.clone()),
        OptVal::Required => Err(err(
            SprigCode::SinkOptionInvalid,
            "REQUIRED belongs in the FOR EACH TENANT definition; a member sets a value",
        )),
    }
}

impl PassSession {
    fn grants_allow(&self, cat: &TenantCatalog, a: Action) -> bool {
        self.tenant.as_deref().is_some_and(|ts| {
            let g = crate::tenant::authz::privileges(ts, cat);
            g.unrestricted() || g.allows(a, Target::Tenant)
        })
    }

    fn need_sink_admin(&self, cat: &TenantCatalog) -> Result<()> {
        if self.grants_allow(cat, Action::SinkAdmin) {
            Ok(())
        } else {
            Err(err(
                SprigCode::SinkPrivilege,
                "Access denied; you need the SPRIG_SINK_ADMIN privilege for this operation",
            ))
        }
    }

    fn user_id_or_zero(&self) -> u32 {
        self.tenant
            .as_deref()
            .and_then(|t| t.principal.user_id)
            .map_or(0, |u| u.0)
    }

    fn db_or_current(&self, db: Option<&str>) -> Result<String> {
        db.map(str::to_owned)
            .or_else(|| self.current_logical())
            .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))
    }

    /// The `FOR EACH TENANT` connection and sink statements; `None` for
    /// any other statement (it goes on to chapter 51's own handling).
    #[allow(clippy::too_many_lines, clippy::many_single_char_names)] // one arm per statement
    pub(super) async fn inherit_sinks(
        &mut self,
        sql: &str,
        cat: &Arc<TenantCatalog>,
    ) -> Result<Option<Handled>> {
        let Some(ts) = self.tenant.as_deref() else {
            return Ok(None);
        };
        let svc = Arc::clone(ts.services());
        if let Some(a) = sg::parse_inherited(sql)? {
            let chain = inherited::chain(&svc, cat).await?;
            let own = inherited::state_of(cat);
            return self
                .alter_inherited_sink(&svc, cat, &chain, &own, a)
                .await
                .map(Some);
        }
        let (lex_opts, _) = self.lex_opts();
        if let Some(f) = ig::for_each(sql, &lex_opts)? {
            let text = f.strip(sql).trim().trim_end_matches(';').to_owned();
            return match sg::parse(&text)? {
                Some(sg::Stmt::CreateConnection(c)) => {
                    let own = inherited::state_of(cat);
                    self.define_connection(&svc, cat, &own, c, &f)
                        .await
                        .map(Some)
                }
                Some(sg::Stmt::CreateSink(c)) => {
                    let chain = inherited::chain(&svc, cat).await?;
                    let own = inherited::state_of(cat);
                    self.define_sink(&svc, cat, &chain, &own, *c, &f, text)
                        .await
                        .map(Some)
                }
                _ => Ok(None),
            };
        }
        // ALTER | DROP of a definition of this tenant.
        let own = inherited::state_of(cat);
        if own.is_empty() {
            return Ok(None);
        }
        let Ok(Some(st)) = sg::parse(sql) else {
            return Ok(None);
        };
        match st {
            sg::Stmt::DropSink {
                name, if_exists, ..
            } => {
                let db = self.db_or_current(name.db.as_deref())?;
                let Some(d) = own.def(&db, DefKind::Sink, &name.name).cloned() else {
                    return Ok(None);
                };
                let _ = if_exists;
                self.need_sink_admin(cat)?;
                self.drop_sink_def(&svc, cat, &d).await.map(Some)
            }
            sg::Stmt::DropConnection { name, .. } => {
                let db = self.db_or_current(name.db.as_deref())?;
                let Some(d) = own.def(&db, DefKind::Connection, &name.name).cloned() else {
                    return Ok(None);
                };
                self.need_sink_admin(cat)?;
                let users: Vec<String> = own
                    .defs
                    .values()
                    .filter(|s| s.kind == DefKind::Sink && s.state == DefState::Active)
                    .filter(|s| {
                        serde_json::from_value::<SinkSpec>(s.definition.clone()).is_ok_and(|x| {
                            x.db == db && x.connections.iter().any(|(c, p)| !p && *c == d.name)
                        })
                    })
                    .map(|s| s.name.clone())
                    .collect();
                if !users.is_empty() {
                    return Err(err(
                        SprigCode::SinkConnectionInUse,
                        format!(
                            "FOR EACH TENANT connection {db}.{} is used by sink definition(s) {}",
                            d.name,
                            users.join(", ")
                        ),
                    ));
                }
                let v = inherited::commit(&svc, cat.tenant, &[MsgWrite::DeleteDef(d.object_id)])
                    .await?;
                self.after_own_change(v).await?;
                Ok(Some(ok()))
            }
            sg::Stmt::AlterSink(name, a) => {
                let db = self.db_or_current(name.db.as_deref())?;
                let Some(d) = own.def(&db, DefKind::Sink, &name.name).cloned() else {
                    return Ok(None);
                };
                self.need_sink_admin(cat)?;
                let mut spec: SinkSpec =
                    serde_json::from_value(d.definition.clone()).unwrap_or_default();
                match a {
                    sg::AlterSink::Set(o) => {
                        for (k, v) in &o {
                            spec.options.insert(
                                k.strip_prefix("sink.").unwrap_or(k).to_owned(),
                                opt_text(v)?,
                            );
                        }
                    }
                    sg::AlterSink::Reset(keys) => {
                        for k in keys {
                            spec.options.remove(k.strip_prefix("sink.").unwrap_or(&k));
                        }
                    }
                    _ => {
                        return Err(inherited::not_overridable(
                            "a FOR EACH TENANT sink takes SET and RESET; pause it per member \
                             with ALTER INHERITED SINK ... [FOR SUBTREE] DISABLE",
                        ));
                    }
                }
                let new = Definition {
                    definition: serde_json::to_value(&spec).unwrap_or_default(),
                    def_version: d.def_version + 1,
                    ..(*d).clone()
                };
                self.commit_def_and_rollout(&svc, cat, &new).await.map(Some)
            }
            sg::Stmt::AlterConnection(name, a) => {
                let db = self.db_or_current(name.db.as_deref())?;
                let Some(d) = own.def(&db, DefKind::Connection, &name.name).cloned() else {
                    return Ok(None);
                };
                self.need_sink_admin(cat)?;
                let mut spec: ConnSpec =
                    serde_json::from_value(d.definition.clone()).unwrap_or_default();
                match a {
                    sg::AlterConnection::Set(o) => {
                        for (k, v) in &o {
                            let v = OptSpec::from(v);
                            match spec.options.iter_mut().find(|(x, _)| x == k) {
                                Some(e) => e.1 = v,
                                None => spec.options.push((k.clone(), v)),
                            }
                        }
                    }
                    sg::AlterConnection::Reset(keys) => {
                        spec.options.retain(|(k, _)| !keys.iter().any(|x| x == k));
                    }
                    sg::AlterConnection::Credentials(_) => {
                        return Err(err(
                            SprigCode::SinkCredentialsInvalid,
                            "a FOR EACH TENANT connection's credentials are each member's: \
                             ALTER INHERITED CONNECTION ... FOR TENANT t SET CREDENTIALS (...)",
                        ));
                    }
                }
                let new = Definition {
                    definition: serde_json::to_value(&spec).unwrap_or_default(),
                    def_version: d.def_version + 1,
                    ..(*d).clone()
                };
                self.commit_def_and_rollout(&svc, cat, &new).await.map(Some)
            }
            _ => Ok(None),
        }
    }

    /// Commits a changed definition and re-applies every instance of the
    /// streams its sinks read.
    async fn commit_def_and_rollout(
        &mut self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        def: &Definition,
    ) -> Result<Handled> {
        let v = inherited::commit(svc, cat.tenant, &[MsgWrite::Def(def)]).await?;
        self.after_own_change(v).await?;
        let fresh = inherited::catalog(svc, cat.tenant).await?;
        let chain = inherited::chain(svc, &fresh).await?;
        let mut n = 0;
        for k in streams_of(&chain, def) {
            n += rollout(svc, k).await?;
        }
        tracing::info!(tenant = %cat.tenant, def = %def.key(), version = def.def_version, instances = n, "inherited sink definition changed");
        Ok(ok())
    }

    async fn define_connection(
        &mut self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        own: &MsgState,
        c: sg::CreateConnection,
        f: &ig::ForEach,
    ) -> Result<Handled> {
        self.need_sink_admin(cat)?;
        if !f.grants.is_empty() {
            return Err(err(
                SprigCode::SinkOptionInvalid,
                "GRANTS belongs to a FOR EACH TENANT sink, not to its connection",
            ));
        }
        if !c.credentials.is_empty() {
            return Err(err(
                SprigCode::SinkCredentialsInvalid,
                "a FOR EACH TENANT connection's credentials are REQUIRED: each member sets its \
                 own with ALTER INHERITED CONNECTION ... SET CREDENTIALS (...); for one secret \
                 shared by every member, use a PLATFORM CONNECTION",
            ));
        }
        let names: Vec<&str> = c.required_credentials.iter().map(String::as_str).collect();
        sprig_sinks::secrets::check_names(c.kind, &names)?;
        let db = self.db_or_current(c.name.db.as_deref())?;
        if own.def(&db, DefKind::Connection, &c.name.name).is_some() {
            if c.if_not_exists {
                return Ok(ok());
            }
            return Err(err(
                SprigCode::SinkExists,
                format!("connection {db}.{} exists", c.name.name),
            ));
        }
        check_quota(svc, cat, own)?;
        let spec = ConnSpec {
            kind: c.kind.to_string(),
            options: c
                .options
                .iter()
                .map(|(k, v)| (k.clone(), OptSpec::from(v)))
                .collect(),
            required_credentials: c.required_credentials.clone(),
        };
        let def = Definition {
            declarer: cat.tenant,
            object_id: next_object_id(svc, cat.tenant).await?,
            kind: DefKind::Connection,
            db_name: db.clone(),
            name: c.name.name.clone(),
            scope: scope_of(svc, cat.tenant, f).await?,
            definition: serde_json::to_value(&spec).unwrap_or_default(),
            grants: None,
            def_version: 1,
            state: DefState::Active,
            drop_after_ms: None,
            created_by: self.user_id_or_zero(),
            created_at_ms: sprig_inherit::msg::store::now_ms(),
            changed_version: 0,
        };
        let v = inherited::commit(svc, cat.tenant, &[MsgWrite::Def(&def)]).await?;
        self.after_own_change(v).await?;
        tracing::info!(tenant = %cat.tenant, def = %def.key(), name = %def.name, "inherited connection defined");
        Ok(ok())
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn define_sink(
        &mut self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        chain: &Chain,
        own: &MsgState,
        cs: sg::CreateSink,
        f: &ig::ForEach,
        text: String,
    ) -> Result<Handled> {
        self.need_sink_admin(cat)?;
        let ts = self
            .tenant
            .as_deref()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        if !f.grants.is_empty() && !super::inherit_msg::tenant_admin(ts, cat) {
            return Err(SprigError::standard(
                1227,
                *b"42000",
                "Access denied; you need (at least one of) the SPRIG_TENANT_ADMIN privilege(s) \
                 for this operation (GRANTS gives every member's sprig_inherited these rights)",
            ));
        }
        let db = self.db_or_current(cs.name.db.as_deref())?;
        // A FOR EACH TENANT stream, or (R9, r9fanin) queue: each member's
        // sink reads its own instance.
        let (q, src_kind) = match &cs.source {
            SourceAst::Stream(q) => (q, DefKind::Stream),
            SourceAst::Queue(q) => (q, DefKind::Queue),
            _ => {
                return Err(err(
                    SprigCode::SinkOptionInvalid,
                    "a FOR EACH TENANT sink reads a FOR EACH TENANT stream (FROM STREAM s) or \
                     queue (FROM QUEUE q); FAN IN is a separate form",
                ));
            }
        };
        let src_db = q.db.clone().unwrap_or_else(|| db.clone());
        if src_db != db {
            return Err(err(
                SprigCode::SinkOptionInvalid,
                format!(
                    "a FOR EACH TENANT sink lives in its source's database ({src_db}), with its \
                     connections"
                ),
            ));
        }
        let stream = own
            .def(&db, src_kind, &q.name)
            .cloned()
            .or_else(|| chain.resolve(&db, src_kind, &q.name))
            .ok_or_else(|| {
                if src_kind == DefKind::Queue {
                    err(
                        SprigCode::SinkOptionInvalid,
                        format!(
                            "a FOR EACH TENANT sink FROM QUEUE reads a FOR EACH TENANT queue; \
                             {db}.{} isn't one here",
                            q.name
                        ),
                    )
                } else {
                    err(
                        SprigCode::StreamNotFound,
                        format!(
                            "ER_SPRIG_STREAM_NOT_FOUND: a FOR EACH TENANT sink reads a FOR EACH \
                             TENANT stream; {db}.{} isn't one here",
                            q.name
                        ),
                    )
                }
            })?;
        if !super::inherit_msg::allows(ts, cat, Action::Select, &db, &q.name) {
            return Err(super::inherit_msg::denied(ts, "SELECT", &q.name));
        }
        if src_kind == DefKind::Queue
            && !super::inherit_msg::allows(ts, cat, Action::Delete, &db, &q.name)
        {
            return Err(super::inherit_msg::denied(ts, "DELETE", &q.name));
        }
        for g in &f.grants {
            inherited::triggers::actions_of(g)?;
        }
        let conns = conns_of(&cs.target);
        for (c, platform) in &conns {
            if *platform {
                continue;
            }
            if own.def(&db, DefKind::Connection, c).is_none()
                && chain.resolve(&db, DefKind::Connection, c).is_none()
            {
                return Err(err(
                    SprigCode::SinkNotFound,
                    format!(
                        "a FOR EACH TENANT sink names a FOR EACH TENANT connection (or a PLATFORM \
                         CONNECTION); {db}.{c} isn't one here"
                    ),
                ));
            }
        }
        if own.def(&db, DefKind::Sink, &cs.name.name).is_some() {
            if cs.if_not_exists {
                return Ok(ok());
            }
            return Err(err(
                SprigCode::SinkExists,
                format!("sink {db}.{} exists", cs.name.name),
            ));
        }
        check_quota(svc, cat, own)?;
        let spec = SinkSpec {
            text,
            db: db.clone(),
            source: q.name.clone(),
            source_kind: if src_kind == DefKind::Queue {
                "queue".into()
            } else {
                String::new()
            },
            connections: conns,
            options: std::collections::BTreeMap::new(),
        };
        let def = Definition {
            declarer: cat.tenant,
            object_id: next_object_id(svc, cat.tenant).await?,
            kind: DefKind::Sink,
            db_name: db.clone(),
            name: cs.name.name.clone(),
            scope: scope_of(svc, cat.tenant, f).await?,
            definition: serde_json::to_value(&spec).unwrap_or_default(),
            grants: (!f.grants.is_empty())
                .then(|| serde_json::to_value(&f.grants).unwrap_or_default()),
            def_version: 1,
            state: DefState::Active,
            drop_after_ms: None,
            created_by: self.user_id_or_zero(),
            created_at_ms: sprig_inherit::msg::store::now_ms(),
            changed_version: 0,
        };
        let v = inherited::commit(svc, cat.tenant, &[MsgWrite::Def(&def)]).await?;
        self.after_own_change(v).await?;
        let n = rollout(svc, stream.key()).await?;
        tracing::info!(tenant = %cat.tenant, def = %def.key(), name = %def.name, instances = n, "inherited sink defined");
        Ok(ok())
    }

    async fn drop_sink_def(
        &mut self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        d: &Arc<Definition>,
    ) -> Result<Handled> {
        let chain = inherited::chain(svc, cat).await?;
        let spec: SinkSpec = serde_json::from_value(d.definition.clone()).unwrap_or_default();
        let conn_names: Vec<String> = spec
            .connections
            .iter()
            .filter(|(_, p)| !p)
            .map(|(c, _)| c.clone())
            .collect();
        let members = crate::inherited::sinks::members_of(svc, &chain, d).await?;
        let mut n = 0;
        for t in members {
            if inherited::dir_entry(svc, t).await?.is_none() {
                continue;
            }
            match crate::inherited::sinks::drop_instance(svc, t, d, &conn_names).await {
                Ok(true) => n += 1,
                Ok(false) => {}
                Err(e) => {
                    tracing::warn!(tenant = %t, def = %d.key(), error = %e, "inherited sink instance not dropped");
                }
            }
        }
        let v = inherited::commit(svc, cat.tenant, &[MsgWrite::DeleteDef(d.object_id)]).await?;
        self.after_own_change(v).await?;
        tracing::info!(tenant = %cat.tenant, def = %d.key(), instances = n, "inherited sink dropped");
        Ok(Handled::Ok {
            affected: u64::try_from(n).unwrap_or(0),
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn alter_inherited_sink(
        &mut self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        chain: &Chain,
        own: &MsgState,
        a: sg::InheritedAlter,
    ) -> Result<Handled> {
        let kind = if a.sink {
            DefKind::Sink
        } else {
            DefKind::Connection
        };
        let db = self.db_or_current(a.name.db.as_deref())?;
        let me = cat.tenant;
        let def = own
            .def(&db, kind, &a.name.name)
            .cloned()
            .or_else(|| chain.resolve(&db, kind, &a.name.name))
            .ok_or_else(|| {
                target_err(format!(
                    "no inherited {} {db}.{} reaches this tenant",
                    kind.as_str(),
                    a.name.name
                ))
            })?;
        let (target, applies) = match &a.target {
            InheritTarget::Own => (me, AppliesTo::Own),
            InheritTarget::Subtree(None) => (me, AppliesTo::Subtree),
            InheritTarget::Tenant(n) | InheritTarget::Subtree(Some(n)) => {
                let id = inherited::tenant_by_name(svc, n)
                    .await?
                    .ok_or_else(|| target_err(format!("no descendant named {n}")))?;
                let e = inherited::dir_entry(svc, id)
                    .await?
                    .ok_or_else(|| target_err(format!("no descendant named {n}")))?;
                let below = e.path_ids().contains(&me.0);
                let single = matches!(a.target, InheritTarget::Tenant(_));
                if !(below && (!single || id != me)) {
                    return Err(target_err(format!("no descendant named {n}")));
                }
                (
                    id,
                    if single {
                        AppliesTo::Own
                    } else {
                        AppliesTo::Subtree
                    },
                )
            }
        };
        if def.declarer == me && target == me {
            return Err(inherited::not_overridable(format!(
                "this tenant declares {} {db}.{}: change the definition itself, or override it \
                 FOR TENANT t | FOR SUBTREE t",
                kind.as_str(),
                a.name.name
            )));
        }
        let allowed = if matches!(a.target, InheritTarget::Own) {
            self.grants_allow(cat, Action::SinkAdmin) || self.grants_allow(cat, Action::TenantAdmin)
        } else {
            self.grants_allow(cat, Action::TenantAdmin)
        };
        if !allowed {
            return Err(err(
                SprigCode::SinkPrivilege,
                "Access denied; you need SPRIG_SINK_ADMIN (for this tenant) or \
                 SPRIG_TENANT_ADMIN (FOR TENANT, FOR SUBTREE) to change an inherited connection \
                 or sink",
            ));
        }
        let user = self.user_id_or_zero();
        let existing = |what: What| -> Map<String, Value> {
            own.overrides
                .iter()
                .find(|o| {
                    o.def == def.key()
                        && o.target == target
                        && o.applies_to == applies
                        && o.what == what
                })
                .and_then(|o| o.value.as_object().cloned())
                .unwrap_or_default()
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
        match (&a.op, a.sink) {
            (
                InheritedOp::Set {
                    options,
                    credentials,
                },
                false,
            ) => {
                if !options.is_empty() {
                    let mut m = existing(What::Options);
                    for (k, v) in options {
                        m.insert(k.clone(), json!(opt_text(v)?));
                    }
                    puts.push(mk(What::Options, Value::Object(m)));
                }
                if !credentials.is_empty() {
                    if applies == AppliesTo::Subtree {
                        return Err(err(
                            SprigCode::SinkCredentialsInvalid,
                            "credentials are set for one tenant at a time (in the member, or FOR \
                             TENANT t): each member's are sealed under its own key",
                        ));
                    }
                    let spec: ConnSpec =
                        serde_json::from_value(def.definition.clone()).unwrap_or_default();
                    let kind = sprig_sinks::model::ConnKind::parse(&spec.kind)
                        .ok_or_else(|| SprigError::internal("connection kind"))?;
                    let names: Vec<&str> = credentials.iter().map(|(k, _)| k.as_str()).collect();
                    sprig_sinks::secrets::check_names(kind, &names)?;
                    let keys = super::sinks::keyring().ok_or_else(|| {
                        err(
                            SprigCode::SinkCredentialsInvalid,
                            "this gateway has no sink credential key ([gateway] workload_key_file)",
                        )
                    })?;
                    let sealed =
                        seal_credentials(keys.as_ref(), target, def.object_id, credentials)?;
                    let mut m = existing(What::Credentials);
                    if let Some(s) = sealed.as_object() {
                        for (k, v) in s {
                            m.insert(k.clone(), v.clone());
                        }
                    }
                    puts.push(mk(What::Credentials, Value::Object(m)));
                    metrics::counter!("sprig_sink_credential_rotations_total").increment(1);
                }
            }
            (
                InheritedOp::Set {
                    options,
                    credentials,
                },
                true,
            ) => {
                if !credentials.is_empty() {
                    return Err(err(
                        SprigCode::SinkCredentialsInvalid,
                        "credentials belong to the sink's connection: ALTER INHERITED CONNECTION",
                    ));
                }
                let mut m = existing(What::Settings);
                for (k, v) in options {
                    let k = k.strip_prefix("sink.").unwrap_or(k).to_owned();
                    if sprig_sinks::settings::u64_key(&k).is_none()
                        && !matches!(
                            k.as_str(),
                            "format" | "batch" | "signature" | "partial_results" | "http2"
                        )
                    {
                        return Err(inherited::not_overridable(format!(
                            "{k} belongs to the sink definition; a member may set the sink.* \
                             settings and the target's format options"
                        )));
                    }
                    m.insert(k, json!(opt_text(v)?));
                }
                puts.push(mk(What::Settings, Value::Object(m)));
            }
            (InheritedOp::Reset(keys), _) if keys.is_empty() => deletes.push(None),
            (InheritedOp::Reset(keys), sink) => {
                let whats: &[What] = if sink {
                    &[What::Settings]
                } else {
                    &[What::Options, What::Credentials]
                };
                for w in whats {
                    let mut m = existing(*w);
                    if m.is_empty() {
                        continue;
                    }
                    for k in keys {
                        m.remove(k.strip_prefix("sink.").unwrap_or(k));
                    }
                    if m.is_empty() {
                        deletes.push(Some(*w));
                    } else {
                        puts.push(mk(*w, Value::Object(m)));
                    }
                }
            }
            (InheritedOp::Disable | InheritedOp::Enable, true) => {
                let on = matches!(a.op, InheritedOp::Enable);
                puts.push(mk(
                    What::State,
                    json!(if on { "enabled" } else { "disabled" }),
                ));
            }
            (InheritedOp::Disable | InheritedOp::Enable, false) => {
                return Err(inherited::not_overridable(
                    "a connection can't be disabled; disable the sinks that use it",
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
        // Members with an instance of the streams involved apply it now.
        let fresh = inherited::catalog(svc, me).await?;
        let fchain = inherited::chain(svc, &fresh).await?;
        let mut reached = 0;
        for sk in streams_of(&fchain, &def) {
            for e in inherited::instances_of(svc, sk).await? {
                let hit = e.tenant == target
                    || (applies == AppliesTo::Subtree
                        && inherited::dir_entry(svc, e.tenant)
                            .await?
                            .is_some_and(|d| d.path_ids().contains(&target.0)));
                if !hit {
                    continue;
                }
                if crate::inherited::sinks::reapply_source(svc, e.tenant, sk).await? {
                    reached += 1;
                }
            }
        }
        tracing::info!(tenant = %me, def = %def.key(), target = %target, reached, "inherited sink override");
        Ok(Handled::Ok {
            affected: u64::try_from(reached).unwrap_or(0),
        })
    }
}

/// The stream (or queue) definitions whose instances `def` (a sink
/// definition, or a connection definition through the sinks that name it)
/// affects.
fn streams_of(chain: &Chain, def: &Definition) -> Vec<DefKey> {
    let sinks: Vec<Arc<Definition>> = match def.kind {
        DefKind::Sink => vec![Arc::new(def.clone())],
        DefKind::Connection => {
            let mut v = chain.reaching(DefKind::Sink);
            if let Some(st) = chain.levels.first().and_then(|l| l.state.as_ref()) {
                v.extend(
                    st.defs
                        .values()
                        .filter(|d| d.kind == DefKind::Sink)
                        .cloned(),
                );
            }
            v.into_iter()
                .filter(|s| {
                    serde_json::from_value::<SinkSpec>(s.definition.clone()).is_ok_and(|x| {
                        x.db == def.db_name
                            && x.connections.iter().any(|(c, p)| !p && *c == def.name)
                    })
                })
                .collect()
        }
        _ => Vec::new(),
    };
    let mut out: Vec<DefKey> = sinks.iter().filter_map(|s| source_of(chain, s)).collect();
    out.sort();
    out.dedup();
    out
}
