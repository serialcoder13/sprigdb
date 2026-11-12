//! Hierarchical data inheritance in a tenant session (chapter 42, R6
//! order 2: read-through, single-shard tenants).
//!
//! Before chapter 32's type layer, each statement of a tenant text goes to
//! [`sprig_inherit::handle`] with the tenant's `inherit` state, its
//! ancestors' snapshots (cross-tenant edges) and the session's space. A
//! statement that isn't the inheritance layer's goes on unchanged.
//! Otherwise its plan's statements run through this same session, as the
//! type layer's do ([`super::types::GwRunner`]): logical names, chapter
//! 29's checks, the session's backend connection and transaction, in
//! *internal* mode so the `_sprig_` columns pass chapter 07's reserved-name
//! check. A rewritten statement streams to the client the same way.
//!
//! The share views of cross-tenant edges are created as the provisioning
//! account on the session's cluster ([`InhRunner::ctl`]).

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use mysql_async::prelude::Queryable;
use sprig_auth::authz::SessionGrants;
use sprig_auth::authz::compile::Target as AuthTarget;
use sprig_auth::authz::privilege::Action;
use sprig_catalog::ddl::database_key;
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogLocator, Freshness, TenantCatalog};
use sprig_common::ids::{DbId, ObjectId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_inherit::{Handled, InheritChange, InheritState, InheritStore, Limits, SpaceId};
use sprig_proto_mysql::StatusFlags;
use sprig_proto_mysql::command::QueryAttrs;
use sprig_proto_mysql::packets::OkPacket;
use sprig_types::{Done, Rows, TypesChange, TypesState};

use super::PassSession;
use super::tenant::Step;
use super::types::GwRunner;
use crate::pipeline::{ExecCtx, ResultSink, RowFormat, Wire};
use crate::tenant::session::TenantSession;

/// The per-session state of the inheritance layer.
#[derive(Debug, Clone, Copy, Default)]
pub struct InheritSession {
    /// `USE SPACE`; `None`: the user's home space.
    pub space: Option<SpaceId>,
}

struct GwPrivileges(Arc<dyn SessionGrants>);

impl sprig_inherit::Privileges for GwPrivileges {
    fn space_admin(&self) -> bool {
        self.0.unrestricted()
            || self.0.allows(Action::CreateUser, AuthTarget::Tenant)
            || self.0.allows(Action::TenantAdmin, AuthTarget::Tenant)
    }

    fn alter(&self, db_name: &str, db: DbId, object: ObjectId) -> bool {
        let key = database_key(db_name, false);
        self.0.unrestricted()
            || self.0.allows(
                Action::Alter,
                AuthTarget::Object {
                    db_key: &key,
                    db,
                    object,
                },
            )
            || self.0.allows(
                Action::Alter,
                AuthTarget::Object {
                    db_key: &db_name.to_lowercase(),
                    db,
                    object,
                },
            )
    }
}

fn limits(ts: &TenantSession, cat: &TenantCatalog) -> Limits {
    use sprig_inherit::settings as s;
    let svc = ts.services();
    let ctx = cat.setting_ctx(&svc.dc);
    let num = |k: &'static sprig_settings::Setting<u64>| {
        usize::try_from(svc.settings.get(k, &ctx, Some(&cat.settings))).unwrap_or(usize::MAX)
    };
    let text = |k: &'static sprig_settings::Setting<std::borrow::Cow<'static, str>>| {
        svc.settings.get(k, &ctx, Some(&cat.settings)).into_owned()
    };
    let merge = text(&s::MERGE);
    Limits {
        parent_delete_follow: text(&s::PARENT_DELETE) == "follow",
        override_record: text(&s::OVERRIDE) == "record",
        max_depth: num(&s::MAX_DEPTH),
        read_through_max_depth: num(&s::READ_THROUGH_MAX_DEPTH),
        // `tenant.max_depth` is the controller's key; its default here.
        tenant_max_depth: 64,
        merge_unbuilt: merge == "federated",
        merge: sprig_inherit::model::MergeMode::parse(&merge),
    }
}

/// The copies of `cat`'s objects and their states (chapter 42,
/// "Materialized copies"), read as the provisioning account at most once
/// a second per tenant and catalog version (a schema change that keeps a
/// copy serving, decision T20, is seen by the next statement).
async fn copy_states(
    ts: &TenantSession,
    cat: &TenantCatalog,
    ancestors: &[Arc<TenantCatalog>],
) -> sprig_inherit::copy::CopyStates {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};
    use std::time::{Duration, Instant};
    type Cached = (Instant, u64, sprig_inherit::copy::CopyStates);
    static CACHE: OnceLock<Mutex<HashMap<u64, Cached>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let schemas = sprig_inherit::copy::copy_schemas(
        cat,
        ancestors.last().map(AsRef::as_ref),
        ts.lower_case_table_names(),
    );
    if schemas.is_empty() {
        return sprig_inherit::copy::CopyStates::default();
    }
    if let Ok(c) = cache.lock()
        && let Some((at, v, st)) = c.get(&cat.tenant.0)
        && at.elapsed() < Duration::from_secs(1)
        && *v == cat.version
    {
        return st.clone();
    }
    let mut out = sprig_inherit::copy::CopyStates::default();
    let svc = Arc::clone(ts.services());
    let cluster = ts.principal.backend.cluster.clone();
    if let Ok(pool) = svc.locator.ctl_pool(&cluster).await
        && let Ok(mut conn) = pool.get_conn().await
    {
        for s in &schemas {
            if let Ok(rows) = sprig_inherit::copy::load_states(&mut conn, cat.tenant, s).await {
                out.0.extend(rows);
            }
        }
    }
    if let Ok(mut c) = cache.lock() {
        c.insert(cat.tenant.0, (Instant::now(), cat.version, out.clone()));
    }
    out
}

/// Whether a statement to prepare names an inheritable object: refused
/// (39891) in this release.
pub(super) fn prepare_refusal(
    ts: &TenantSession,
    sql: &[u8],
    lex: sprig_sql::lex::LexOptions,
) -> Option<SprigError> {
    let cat = ts.catalog()?;
    let state = cat.ext.get::<InheritState>(sprig_inherit::MODULE)?;
    let names: Vec<String> = state.objects().map(|o| o.table.to_lowercase()).collect();
    if names.is_empty() {
        return None;
    }
    let hit = sprig_inherit::grammar::identifiers(sql, &lex)
        .iter()
        .any(|i| names.contains(&i.to_lowercase()));
    hit.then(|| {
        SprigError::new(
            SprigCode::InheritStatementUnsupported,
            "prepared statements on inheritable objects are not supported yet: send the \
             statement as text",
        )
    })
}

impl PassSession {
    /// Chapter 42's step of [`PassSession::tenant_text`]: `None` when the
    /// statement isn't the inheritance layer's.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    ///
    /// A statement naming a typed table goes to chapter 32 first (its
    /// plan's statements come back here, [`TypesSession::typed_plan`]):
    /// `deferred` is set, and the caller calls again with `after_types`
    /// when chapter 32 doesn't take it.
    ///
    /// [`TypesSession::typed_plan`]: super::types::TypesSession::typed_plan
    pub(super) async fn tenant_inherit(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        attrs: &QueryAttrs,
        more: bool,
        format: RowFormat,
        cat: &Arc<TenantCatalog>,
        out: &mut dyn ResultSink,
        after_types: bool,
        deferred: &mut bool,
    ) -> Result<Option<Step>> {
        let Some(ts) = self.tenant.as_deref() else {
            return Ok(None);
        };
        let from_types = ts.types.typed_plan;
        if (ts.types.internal && !from_types) || ts.principal.must_change_password {
            return Ok(None);
        }
        let state = cat.ext.get::<InheritState>(sprig_inherit::MODULE);
        if !sprig_inherit::maybe_inherit(text, state) {
            return Ok(None);
        }
        let Ok(sql) = std::str::from_utf8(text) else {
            return Ok(None);
        };
        let empty = InheritState::unmigrated();
        let state = state.unwrap_or(&empty);
        let svc = Arc::clone(ts.services());
        // Ancestors' snapshots, for edges from the parent tenant (and the
        // tree's root: id sequences and references).
        let mut ancestors = Vec::new();
        if state.objects().next().is_some() || !cat.ancestry.is_empty() {
            for a in cat.ancestry.iter() {
                match svc
                    .cache
                    .get(a.id, Freshness::Cached { min_version: 0 })
                    .await
                {
                    Ok(c) => ancestors.push(c),
                    Err(e) => return Ok(Some(self.fail(out, &e).await?)),
                }
            }
        }
        let typed: Vec<String> = cat
            .ext
            .get::<TypesState>(sprig_types::store::MODULE)
            .filter(|_| !from_types && !after_types)
            .map(|t| {
                let mut v = Vec::new();
                for (_, d) in t.dbs() {
                    v.extend(
                        d.iter()
                            .filter(|x| !x.is_root() || d.has_subtypes(x.id))
                            .map(|x| x.name.to_lowercase()),
                    );
                    v.extend(d.hierarchies().map(|h| h.table.to_lowercase()));
                }
                v
            })
            .unwrap_or_default();
        let user = ts.principal.user_id.map(|u| u.0);
        let space = ts
            .inherit
            .space
            .unwrap_or_else(|| user.map_or(sprig_inherit::ROOT_SPACE, |u| state.home(u)));
        let tenant_name = ts
            .principal
            .tenant_name
            .clone()
            .unwrap_or_else(|| ts.tenant.to_string());
        let privileges = GwPrivileges(crate::tenant::authz::privileges(ts, cat));
        let lctn = ts.lower_case_table_names();
        let lim = limits(ts, cat);
        let copies = copy_states(ts, cat, &ancestors).await;
        let current = self.current_logical();
        let (lex, _) = self.lex_opts();
        let ctx = sprig_inherit::Ctx {
            cat,
            state,
            tenant_name: &tenant_name,
            ancestors: &ancestors,
            lex,
            lctn,
            current_db: current,
            space,
            user,
            limits: lim,
            privileges: &privileges,
            typed: &typed,
            copies: &copies,
            from_types,
        };
        let wire = Wire {
            persona: self.persona,
            caps: cx.proto.caps,
        };
        let migrated = state.migrated && state.ext_ready;
        // A type plan's statement keeps its mode once the plan here ran.
        let saved = self.tenant.as_deref().map_or((false, false), |ts| {
            (ts.types.internal, ts.types.typed_plan)
        });
        let handled = {
            let mut runner = InhRunner {
                gw: GwRunner {
                    sess: self,
                    cx,
                    wire,
                    migrated: true,
                    typed: false,
                },
                migrated,
            };
            let h = Box::pin(sprig_inherit::handle(sql, &ctx, &mut runner)).await;
            if let Some(ts) = runner.gw.sess.tenant.as_deref_mut() {
                (ts.types.internal, ts.types.typed_plan) = saved;
            }
            h
        };
        let status = {
            let s = self.status();
            if more {
                s | StatusFlags::MORE_RESULTS_EXISTS
            } else {
                s
            }
        };
        let step = Step {
            errored: false,
            status: Some(status),
        };
        match handled {
            Ok(Handled::NotInherit) => Ok(None),
            Ok(Handled::Typed) => {
                *deferred = true;
                Ok(None)
            }
            Err(e) => Ok(Some(self.fail(out, &e).await?)),
            Ok(Handled::Space(s)) => {
                if let Some(ts) = self.tenant.as_deref_mut() {
                    ts.inherit.space = Some(s);
                }
                out.ok(OkPacket::new(status)).await?;
                Ok(Some(step))
            }
            Ok(Handled::Ok(d)) => {
                let mut ok = OkPacket::new(status);
                ok.affected_rows = d.affected_rows;
                ok.last_insert_id = d.last_insert_id;
                ok.warnings = d.warnings;
                ok.info = Bytes::from(d.info.into_bytes());
                out.ok(ok).await?;
                Ok(Some(step))
            }
            Ok(Handled::Rows(rows)) => {
                super::types::send_rows(out, &rows, format, status).await?;
                Ok(Some(step))
            }
            Ok(Handled::Forward(sql)) => {
                if let Some(ts) = self.tenant.as_deref_mut() {
                    ts.types.internal = true;
                    ts.types.typed_plan = false;
                }
                let text = Bytes::from(sql.into_bytes());
                let r = Box::pin(self.tenant_text(cx, &text, attrs, more, format, out)).await;
                if let Some(ts) = self.tenant.as_deref_mut() {
                    ts.types.internal = false;
                }
                r.map(Some)
            }
        }
    }
}

impl PassSession {
    /// Kiran's decisions 1 and 2 (11 November 2026): before a statement of
    /// a tenant whose parent shares it tables, the tenant (and each tenant
    /// above it, top first) gets every table and column its parent shares
    /// ([`super::inherit_tree::adopt`]). `cat` again, or the fresh
    /// snapshot when anything changed.
    pub(super) async fn inherit_adopt(
        &mut self,
        cat: Arc<TenantCatalog>,
    ) -> Result<Arc<TenantCatalog>> {
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Ok(cat);
        };
        if ts.types.internal || ts.principal.must_change_password || cat.ancestry.is_empty() {
            return Ok(cat);
        }
        let svc = Arc::clone(ts.services());
        let lctn = ts.lower_case_table_names();
        match super::inherit_tree::adopt(&svc, &cat, lctn).await? {
            Some(fresh) => {
                ts.changed(Some(fresh.version));
                ts.refresh().await
            }
            None => Ok(cat),
        }
    }
}

/// The inheritance layer's runner: chapter 32's, plus the catalog commit,
/// the provisioning account and the directory.
struct InhRunner<'s, 'c, 'x> {
    gw: GwRunner<'s, 'c, 'x>,
    migrated: bool,
}

#[async_trait]
impl sprig_types::Runner for InhRunner<'_, '_, '_> {
    async fn query(&mut self, sql: &str) -> Result<Rows> {
        self.gw.query(sql).await
    }

    async fn exec(&mut self, sql: &str) -> Result<Done> {
        self.gw.exec(sql).await
    }

    fn in_transaction(&self) -> bool {
        self.gw.in_transaction()
    }

    fn found_rows(&self) -> bool {
        self.gw.found_rows()
    }

    async fn snapshot_isolation(&mut self) -> Result<bool> {
        self.gw.snapshot_isolation().await
    }

    async fn object_id(
        &mut self,
        db: Option<&str>,
        table: &str,
    ) -> Result<Option<(DbId, ObjectId)>> {
        self.gw.object_id(db, table).await
    }

    async fn commit_types(&mut self, change: &TypesChange) -> Result<()> {
        self.gw.commit_types(change).await
    }
}

#[async_trait]
impl sprig_inherit::Runner for InhRunner<'_, '_, '_> {
    async fn commit(&mut self, change: &InheritChange) -> Result<()> {
        let Some(ts) = self.gw.sess.tenant.as_deref_mut() else {
            return Err(SprigError::internal("no tenant"));
        };
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        if !self.migrated {
            let ep = svc.locator.writer(tenant).await?;
            let mut conn = ep.pool.get_conn().await.map_err(|e| {
                SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
            })?;
            sprig_inherit::store::migrate(&mut conn, ep.store.schema()).await?;
            self.migrated = true;
        }
        let mut c = svc.catalog.change(tenant, VersionKind::Catalog).await?;
        let version = c.version();
        let applied = {
            let (store, tx) = c.tx().parts();
            InheritStore::new(store.schema())
                .apply(tx, change, version)
                .await
        };
        if let Err(e) = applied {
            let _ = c.rollback().await;
            return Err(e);
        }
        let v = c.commit().await?;
        ts.changed(Some(v));
        ts.refresh().await?;
        Ok(())
    }

    async fn ctl(&mut self, sql: &str) -> Result<()> {
        let Some(ts) = self.gw.sess.tenant.as_deref() else {
            return Err(SprigError::internal("no tenant"));
        };
        let svc = Arc::clone(ts.services());
        let cluster = ts.principal.backend.cluster.clone();
        let pool = svc.locator.ctl_pool(&cluster).await?;
        let mut conn = pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::BackendUnavailable, "cluster unreachable").with_source(e)
        })?;
        conn.query_drop(sql).await.map_err(|e| {
            let msg = match &e {
                mysql_async::Error::Server(s) => format!("{} ({})", s.message, s.code),
                other => other.to_string(),
            };
            SprigError::new(SprigCode::BackendUnavailable, msg).with_source(e)
        })
    }

    async fn child_tenant(&mut self, name: &str) -> Result<Option<TenantId>> {
        let Some(ts) = self.gw.sess.tenant.as_deref() else {
            return Err(SprigError::internal("no tenant"));
        };
        let svc = Arc::clone(ts.services());
        let me = ts.tenant;
        let Some(t) = crate::tenant::settings_sql::descendant(&svc, me, name).await? else {
            return Ok(None);
        };
        // A direct child: its catalog's parent is this tenant.
        let c = svc
            .cache
            .get(t, Freshness::Cached { min_version: 0 })
            .await?;
        Ok((c.ancestry.last().map(|a| a.id) == Some(me)).then_some(t))
    }

    fn user_id(&self, name: &str) -> Option<u32> {
        let ts = self.gw.sess.tenant.as_deref()?;
        ts.auth()?.accounts(name).first().map(|u| u.row.user_id.0)
    }

    async fn ctl_query(&mut self, t: TenantId, sql: &str) -> Result<sprig_inherit::RawRows> {
        let Some(ts) = self.gw.sess.tenant.as_deref() else {
            return Err(SprigError::internal("no tenant"));
        };
        let svc = Arc::clone(ts.services());
        super::inherit_tree::ctl_query(&svc, t, sql).await
    }

    async fn refs(&mut self, root: TenantId, sql: &str) -> Result<sprig_inherit::RawRows> {
        let Some(ts) = self.gw.sess.tenant.as_deref() else {
            return Err(SprigError::internal("no tenant"));
        };
        let svc = Arc::clone(ts.services());
        super::inherit_tree::refs(&svc, root, sql).await
    }

    async fn tenant_catalog(&mut self, t: TenantId) -> Result<Arc<TenantCatalog>> {
        let Some(ts) = self.gw.sess.tenant.as_deref() else {
            return Err(SprigError::internal("no tenant"));
        };
        let svc = Arc::clone(ts.services());
        svc.cache.get(t, Freshness::Latest).await
    }

    async fn tree_ids(
        &mut self,
        seq: &sprig_inherit::ids::TreeSeq,
        n: u64,
        explicit: Option<i128>,
    ) -> Result<Option<(i128, i128)>> {
        let var = |k: &str| {
            self.gw
                .sess
                .bs
                .as_ref()
                .and_then(|b| b.vars().vars.get(k).and_then(|v| v.parse::<u64>().ok()))
                .unwrap_or(1)
                .clamp(1, 65_535)
        };
        let s = sprig_shard::ids::AutoIncSession {
            increment: var("auto_increment_increment"),
            offset: var("auto_increment_offset"),
        };
        let Some(ts) = self.gw.sess.tenant.as_deref() else {
            return Err(SprigError::internal("no tenant"));
        };
        let svc = Arc::clone(ts.services());
        let lctn = ts.lower_case_table_names();
        super::inherit_tree::tree_ids(&svc, seq, n, explicit, &s, lctn).await
    }

    async fn tree_tables(
        &mut self,
        root: TenantId,
        db: &str,
        table: &str,
    ) -> Result<Vec<sprig_inherit::ids::TreeTable>> {
        let Some(ts) = self.gw.sess.tenant.as_deref() else {
            return Err(SprigError::internal("no tenant"));
        };
        let svc = Arc::clone(ts.services());
        let lctn = ts.lower_case_table_names();
        super::inherit_tree::tree_tables(&svc, root, db, table, lctn).await
    }

    async fn subscribe(&mut self) -> Result<()> {
        let Some(ts) = self.gw.sess.tenant.as_deref() else {
            return Err(SprigError::internal("no tenant"));
        };
        let svc = Arc::clone(ts.services());
        let me = ts.tenant;
        let cat = svc
            .cache
            .get(me, Freshness::Cached { min_version: 0 })
            .await?;
        let root = cat.ancestry.first().map_or(me, |a| a.id);
        let mut tenants: Vec<TenantId> = cat.ancestry.iter().map(|a| a.id).collect();
        tenants.push(me);
        // R6: a tree's tenants share the session's cluster.
        let cluster = ts.principal.backend.cluster.clone();
        let pool = svc.locator.ctl_pool(&cluster).await?;
        let mut conn = pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::BackendUnavailable, "cluster unreachable").with_source(e)
        })?;
        let unreachable = |e| {
            SprigError::new(SprigCode::BackendUnavailable, "cluster unreachable").with_source(e)
        };
        let pos: Option<String> = conn
            .query_first("SELECT @@gtid_binlog_pos")
            .await
            .map_err(unreachable)?;
        for t in tenants {
            sprig_inherit::subs::register(
                svc.locator.dc_store(),
                t,
                root,
                cluster.clone(),
                pos.as_deref().unwrap_or(""),
            )
            .await?;
        }
        Ok(())
    }
}
