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
    /// The statement running is one this layer generated (a rewritten read
    /// or a plan's statement): only then are references under
    /// [`sprig_inherit::view::LAYER_ALIAS`] the layers of merged views it
    /// filtered (chapter 29, "Data inheritance").
    pub generated: bool,
}

/// The record-hierarchy locks the session's transaction holds (decision
/// T27): each a provisioning connection inside a transaction holding a row
/// of `__sprig_hlock` `FOR UPDATE`, so the system takes them whatever the
/// user's grants. Released when the session's transaction ends
/// ([`PassSession::release_hier_locks`]) or the session goes.
#[derive(Default)]
pub struct HierLocks {
    held: Vec<((u64, String, u64), mysql_async::Conn)>,
}

impl std::fmt::Debug for HierLocks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HierLocks")
            .field("held", &self.held.len())
            .finish()
    }
}

impl HierLocks {
    /// Whether none is held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.held.is_empty()
    }

    /// Ends every held lock's transaction and closes its connection.
    pub async fn release(&mut self) {
        let n = self.held.len();
        for (_, mut c) in self.held.drain(..) {
            let _ = c.query_drop("ROLLBACK").await;
            let _ = c.disconnect().await;
        }
        if n > 0 {
            metrics::counter!("sprig_inherit_hierarchy_locks_released_total").increment(n as u64);
        }
    }
}

impl Drop for HierLocks {
    fn drop(&mut self) {
        // A connection dropped inside its transaction is reset when the
        // pool takes it back; end it first where a runtime is at hand.
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            for (_, mut c) in self.held.drain(..) {
                rt.spawn(async move {
                    let _ = c.query_drop("ROLLBACK").await;
                    let _ = c.disconnect().await;
                });
            }
        }
    }
}

impl PassSession {
    /// Releases the record-hierarchy locks once the session's transaction
    /// has ended (decision T27).
    pub(super) async fn release_hier_locks(&mut self) {
        if self.status().contains(StatusFlags::IN_TRANS) {
            return;
        }
        if let Some(ts) = self.tenant.as_deref_mut()
            && !ts.hier_locks.is_empty()
        {
            ts.hier_locks.release().await;
        }
    }
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
    let max_move_rows = svc.settings.get(
        &sprig_types::settings::HIERARCHY_MAX_MOVE_ROWS,
        &ctx,
        Some(&cat.settings),
    );
    Limits {
        parent_delete_follow: text(&s::PARENT_DELETE) == "follow",
        override_record: text(&s::OVERRIDE) == "record",
        max_depth: num(&s::MAX_DEPTH),
        read_through_max_depth: num(&s::READ_THROUGH_MAX_DEPTH),
        // `tenant.max_depth` is the controller's key; its default here.
        tenant_max_depth: 64,
        merge_unbuilt: merge == "federated",
        merge: sprig_inherit::model::MergeMode::parse(&merge),
        max_move_rows,
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
    // A copy appeared: accounts row-level security narrowed to single
    // relations need SELECT on its table (rlsinherit).
    let known = match cache.lock() {
        Ok(mut c) => {
            let known = c
                .get(&cat.tenant.0)
                .is_some_and(|(_, _, st)| out.0.keys().all(|k| st.0.contains_key(k)));
            c.insert(cat.tenant.0, (Instant::now(), cat.version, out.clone()));
            known
        }
        Err(_) => true,
    };
    if !known && !out.0.is_empty() {
        svc.authz.relations_changed(cat.tenant);
        if let Err(e) = Box::pin(svc.authz.ensure(&svc, cat, &cluster)).await {
            tracing::warn!(tenant = %cat.tenant, error = %e, "backend grants after a new copy not reconciled yet");
        }
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
        // A change stream's visibility probe (RL64; `inherit_probe`).
        let probe = super::inherit_probe::parse(text);
        if let Some(None) = probe {
            let e = SprigError::standard(
                1064,
                *b"42000",
                "You have an error in your SQL syntax: a visibility probe isn't a client statement",
            );
            return Ok(Some(self.fail(out, &e).await?));
        }
        let probe = probe.flatten();
        if probe.is_none() && !sprig_inherit::maybe_inherit(text, state) {
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
            let h = match &probe {
                Some(req) => {
                    let col =
                        |n: &str| crate::pipeline::text_column(n, runner.gw.cx.proto.collation);
                    Box::pin(super::inherit_probe::run(
                        &ctx,
                        &mut runner,
                        req,
                        (col("i"), col("v")),
                    ))
                    .await
                    .map(Handled::Rows)
                }
                None => Box::pin(sprig_inherit::handle(sql, &ctx, &mut runner)).await,
            };
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
                    ts.inherit.generated = true;
                }
                let text = Bytes::from(sql.into_bytes());
                let r = Box::pin(self.tenant_text(cx, &text, attrs, more, format, out)).await;
                if let Some(ts) = self.tenant.as_deref_mut() {
                    ts.types.internal = false;
                    ts.inherit.generated = false;
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

impl InhRunner<'_, '_, '_> {
    fn generated(&mut self, on: bool) {
        if let Some(ts) = self.gw.sess.tenant.as_deref_mut() {
            ts.inherit.generated = on;
        }
    }
}

#[async_trait]
impl sprig_types::Runner for InhRunner<'_, '_, '_> {
    async fn query(&mut self, sql: &str) -> Result<Rows> {
        self.generated(true);
        let r = self.gw.query(sql).await;
        self.generated(false);
        r
    }

    async fn exec(&mut self, sql: &str) -> Result<Done> {
        self.generated(true);
        let r = self.gw.exec(sql).await;
        self.generated(false);
        r
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
    async fn row_filter(
        &mut self,
        db: &str,
        table: &str,
        qual: &str,
        cmd: sprig_inherit::RowCmd,
    ) -> Result<Option<String>> {
        use sprig_sql::rls::Cmd;
        let cmds: &[Cmd] = match cmd {
            sprig_inherit::RowCmd::Select => &[Cmd::Select],
            sprig_inherit::RowCmd::Update => &[Cmd::Select, Cmd::Update],
            sprig_inherit::RowCmd::Delete => &[Cmd::Select, Cmd::Delete],
        };
        let session = self.gw.cx.session.id;
        Box::pin(self.gw.sess.rls_predicate(session, db, table, qual, cmds)).await
    }

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
        })?;
        // Back to the pool first: the reconcile below waits for the
        // tenant's gate and then for a slot of this same pool (4 slots), so
        // sessions holding a slot here while its holder waits for one
        // stopped every catalog read on the cluster (rlsinherit2).
        drop(conn);
        // A share view was made: accounts row-level security narrowed to
        // single relations need SELECT on it (rlsinherit).
        if let Some(ts) = self.gw.sess.tenant.as_deref()
            && let Some(cat) = ts.catalog().cloned()
        {
            svc.authz.relations_changed(ts.tenant);
            if let Err(e) = Box::pin(svc.authz.ensure(&svc, &cat, &cluster)).await {
                tracing::warn!(tenant = %ts.tenant, error = %e, "backend grants after a new share view not reconciled yet");
            }
        }
        Ok(())
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

    async fn hier_lock(&mut self, t: TenantId, schema: &str, object: u64) -> Result<()> {
        let key = (t.0, schema.to_owned(), object);
        let wait = self
            .gw
            .sess
            .bs
            .as_ref()
            .and_then(|b| {
                b.vars()
                    .vars
                    .get("innodb_lock_wait_timeout")
                    .and_then(|v| v.parse::<u64>().ok())
            })
            .unwrap_or(50)
            .clamp(1, 3600);
        let Some(ts) = self.gw.sess.tenant.as_deref() else {
            return Err(SprigError::internal("no tenant"));
        };
        if ts.hier_locks.held.iter().any(|(k, _)| *k == key) {
            return Ok(());
        }
        let svc = Arc::clone(ts.services());
        let cluster = svc.locator.cluster(t).await?;
        // Its own connection: held until the transaction ends, it can't
        // take a slot of the shared pool another holder waits for.
        let mut conn = svc.locator.ctl_conn(&cluster).await?;
        let (row, lock) = sprig_inherit::hier::lock_sql(schema, object);
        let fail = |e: mysql_async::Error| match &e {
            mysql_async::Error::Server(s) if s.code == 1205 => SprigError::standard(
                1205,
                *b"HY000",
                "Lock wait timeout exceeded; another structural change of this record \
                 hierarchy is running: try restarting transaction",
            ),
            mysql_async::Error::Server(s) => SprigError::new(
                SprigCode::BackendUnavailable,
                format!("the record hierarchy's lock: {} ({})", s.message, s.code),
            ),
            other => SprigError::new(
                SprigCode::BackendUnavailable,
                format!("the record hierarchy's lock: {other}"),
            ),
        };
        conn.query_drop(format!("SET SESSION innodb_lock_wait_timeout = {wait}"))
            .await
            .map_err(fail)?;
        // The row exists after the first lock of the object: try first,
        // and make the table and the row only when missing.
        let mut made = false;
        loop {
            conn.query_drop("BEGIN").await.map_err(fail)?;
            let got: std::result::Result<Vec<u64>, _> = conn.query(lock.as_str()).await;
            // No row, or no table, yet: made once, then taken again.
            let missing = match &got {
                Ok(rows) => rows.is_empty(),
                Err(mysql_async::Error::Server(e)) => e.code == 1146,
                Err(_) => false,
            };
            match got {
                Ok(rows) if !rows.is_empty() => break,
                _ if missing && !made => {
                    let _ = conn.query_drop("ROLLBACK").await;
                    conn.query_drop(sprig_inherit::hier::lock_ddl(schema))
                        .await
                        .map_err(fail)?;
                    conn.query_drop(row.as_str()).await.map_err(fail)?;
                    made = true;
                }
                Ok(_) => {
                    let _ = conn.query_drop("ROLLBACK").await;
                    return Err(SprigError::internal("the hierarchy's lock row is missing"));
                }
                Err(e) => {
                    let _ = conn.query_drop("ROLLBACK").await;
                    return Err(fail(e));
                }
            }
        }
        metrics::counter!("sprig_inherit_hierarchy_locks_total").increment(1);
        if let Some(ts) = self.gw.sess.tenant.as_deref_mut() {
            ts.hier_locks.held.push((key, conn));
        }
        Ok(())
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
