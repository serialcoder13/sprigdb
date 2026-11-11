//! Sinks and connections in a tenant session (chapter 51, R9: build steps
//! 2–4, 15 and 18).
//!
//! Before chapter 07's name plan, each statement of a tenant text goes to
//! [`sprig_sinks::sql::handle`]: `CREATE | ALTER | DROP CONNECTION`,
//! `CREATE | ALTER | DROP SINK`, `SHOW SINKS | CONNECTIONS | PLATFORM
//! CONNECTIONS`, `SHOW CREATE ...`, `SHOW SINK STATUS` and `GRANT | REVOKE
//! USAGE ON [PLATFORM] CONNECTION`. Catalog rows change in one versioned
//! catalog change; a sink's DC index entry starts its tasks in workers.
//!
//! Credentials are sealed here with the tenant's sink key (the master key
//! file of `[gateway] workload_key_file`, purpose `sink`) and never stored
//! or logged in plain text: these statements are answered before chapter
//! 12's capture point, and logs show the redacted text.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use bytes::Bytes;
use sprig_auth::authz::{Action, Target};
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::{Namespace, ObjectKind, ObjectState};
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogLocator, TenantCatalog};
use sprig_common::ids::{DbId, TenantId, UserId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::StatusFlags;
use sprig_proto_mysql::packets::OkPacket;
use sprig_sinks::meta::{PlatformConnection, SinkIndex};
use sprig_sinks::model::{SinkId, TaskRow};
use sprig_sinks::policy::Residency;
use sprig_sinks::secrets::SinkKeyring;
use sprig_sinks::sql::{Change, Handled, Host};
use sprig_sinks::store::SinksState;

use super::PassSession;
use super::tenant::Step;
use crate::pipeline::{ExecCtx, ResultSink, RowFormat};

static KEYRING: OnceLock<Option<Arc<sprig_workload::keyring::MasterKeyring>>> = OnceLock::new();

/// Loads the sink credential keyring from the master key file (once per
/// process). Without it, credentials can't be set (39216).
pub fn install_keyring(path: Option<&std::path::Path>) {
    let k = path.and_then(
        |p| match sprig_workload::keyring::MasterKeyring::from_file(p) {
            Ok(k) => Some(Arc::new(k)),
            Err(e) => {
                tracing::warn!(error = %e, "sink credential keys not loaded");
                None
            }
        },
    );
    let _ = KEYRING.set(k);
}

/// The sink credential keyring, if this gateway loaded one.
pub(crate) fn keyring() -> Option<Arc<dyn SinkKeyring>> {
    KEYRING
        .get()
        .and_then(Clone::clone)
        .map(|k| k as Arc<dyn SinkKeyring>)
}

/// The residency of `cat`'s tenant for sink destinations (its and its
/// ancestors' policies; unpinned without any).
///
/// # Errors
/// The global tier's; 39506 for a policy that doesn't parse.
pub(crate) async fn residency_of(
    svc: &crate::tenant::TenantServices,
    cat: &TenantCatalog,
) -> Result<sprig_sinks::residency::Shared> {
    let r = sprig_sinks::residency::residency_of(svc.global.as_ref(), cat.tenant).await?;
    Ok(sprig_sinks::residency::Shared(Arc::from(r)))
}

/// A resolved `sink.*` setting of tenant `cat` at database `db`, as text
/// (also for inherited sinks deployed in a member, R9).
pub(crate) fn setting_of(
    svc: &crate::tenant::TenantServices,
    cat: &TenantCatalog,
    key: &str,
    db: u32,
) -> Option<String> {
    use sprig_sinks::settings as s;
    let ctx = cat.setting_ctx(&svc.dc).with_db(DbId(db));
    let text = |k: &'static sprig_settings::Setting<std::borrow::Cow<'static, str>>| {
        svc.settings.get(k, &ctx, Some(&cat.settings)).into_owned()
    };
    Some(match key {
        "destination_allowlist" => text(&s::DESTINATION_ALLOWLIST),
        "allowed_kinds" => text(&s::ALLOWED_KINDS),
        "platform_identity" => text(&s::PLATFORM_IDENTITY),
        "enabled" => {
            if svc.settings.get(&s::ENABLED, &ctx, Some(&cat.settings)) {
                "on".into()
            } else {
                "off".into()
            }
        }
        other => {
            let k = s::u64_key(other)?;
            svc.settings.get(k, &ctx, Some(&cat.settings)).to_string()
        }
    })
}

struct GwHost<'s> {
    sess: &'s mut PassSession,
    cat: Arc<TenantCatalog>,
    migrated: bool,
    /// The tenant's residency, from its and its ancestors' directory
    /// entries (R9, r9more: checked at `CREATE` and `ALTER CONNECTION`).
    residency: sprig_sinks::residency::Shared,
}

impl GwHost<'_> {
    fn ts(&self) -> Option<&crate::tenant::session::TenantSession> {
        self.sess.tenant.as_deref()
    }

    fn lctn(&self) -> bool {
        self.ts()
            .is_some_and(crate::tenant::session::TenantSession::lower_case_table_names)
    }

    fn allows(&self, a: Action) -> bool {
        let Some(ts) = self.ts() else {
            return false;
        };
        let g = crate::tenant::authz::privileges(ts, &self.cat);
        g.allows(a, Target::Tenant)
    }

    async fn catalog_conn(&self) -> Result<(mysql_async::Conn, String)> {
        let ts = self.ts().ok_or_else(|| SprigError::internal("no tenant"))?;
        let ep = ts.services().locator.writer(self.cat.tenant).await?;
        let conn = ep.pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
        })?;
        Ok((conn, ep.store.schema().to_owned()))
    }
}

#[async_trait]
impl Host for GwHost<'_> {
    fn tenant(&self) -> TenantId {
        self.cat.tenant
    }

    fn user_id(&self) -> Option<u32> {
        self.ts().and_then(|t| t.principal.user_id).map(|u| u.0)
    }

    fn current_db(&self) -> Option<String> {
        self.sess.current_logical()
    }

    fn db_id(&self, name: &str) -> Option<u32> {
        self.cat
            .database(&database_key(name, self.lctn()))
            .map(|d| d.db_id.0)
    }

    fn db_name(&self, id: u32) -> Option<String> {
        self.cat.databases.get(DbId(id)).map(|d| d.name.clone())
    }

    fn state(&self) -> Arc<SinksState> {
        Arc::new(
            self.cat
                .ext
                .get::<SinksState>(sprig_sinks::store::MODULE)
                .cloned()
                .unwrap_or_default(),
        )
    }

    fn stream(&self, db: u32, name: &str) -> Option<(u64, u32)> {
        let st = self
            .cat
            .ext
            .get::<sprig_streams::StreamsState>(sprig_streams::MODULE)?;
        st.get(DbId(db), name, self.lctn())
            .map(|s| (s.id.0, s.partitions))
    }

    fn table(&self, db: u32, name: &str) -> Option<u64> {
        self.cat
            .object(
                DbId(db),
                Namespace::Relation,
                &object_key(ObjectKind::Table, name, self.lctn()),
            )
            .filter(|o| o.state == ObjectState::Active)
            .map(|o| o.object_id.0)
    }

    fn sink_admin(&self) -> bool {
        self.allows(Action::SinkAdmin)
    }

    fn tenant_admin(&self) -> bool {
        self.allows(Action::TenantAdmin)
    }

    fn can_select(&self, db_name: &str, name: &str) -> bool {
        let Some(ts) = self.ts() else {
            return false;
        };
        let g = crate::tenant::authz::privileges(ts, &self.cat);
        if g.unrestricted() {
            return true;
        }
        let lctn = self.lctn();
        let db_key = database_key(db_name, lctn);
        let name_key = object_key(ObjectKind::Table, name, lctn);
        // Chapter 29 (RL36): `FROM STREAM t.changes` exports table t's rows:
        // SELECT on t and its policies decide, as for `CHANGES OF t`.
        if let Some(row) = self.cat.database(&db_key)
            && let Some(st) = self
                .cat
                .ext
                .get::<sprig_streams::StreamsState>(sprig_streams::MODULE)
            && let Some(def) = st.get(row.db_id, name, lctn)
            && def.kind == sprig_streams::model::StreamKind::Changes
        {
            return crate::tenant::rls_streams::change_topic_readable(
                g.as_ref(),
                &self.cat,
                def,
                lctn,
                &ts.principal.user,
            )
            .is_ok();
        }
        // Chapter 29: a sink would export a table whose policies bind the
        // session unfiltered: refused.
        if let Some(row) = self.cat.database(&db_key)
            && let Some(o) = self.cat.object(
                row.db_id,
                sprig_catalog::store::Namespace::Relation,
                &name_key,
            )
            && g.row_filter(
                Target::Object {
                    db_key: &db_key,
                    db: row.db_id,
                    object: o.object_id,
                },
                Action::Select,
            )
            .is_some()
        {
            return false;
        }
        g.allows(
            Action::Select,
            Target::Named {
                db_key: &db_key,
                name_key: &name_key,
            },
        ) || g.allows(Action::Select, Target::Database { db_key: &db_key })
    }

    fn can_delete(&self, db_name: &str, name: &str) -> bool {
        let Some(ts) = self.ts() else {
            return false;
        };
        let g = crate::tenant::authz::privileges(ts, &self.cat);
        if g.unrestricted() {
            return true;
        }
        let lctn = self.lctn();
        let db_key = database_key(db_name, lctn);
        let name_key = object_key(ObjectKind::Table, name, lctn);
        g.allows(
            Action::Delete,
            Target::Named {
                db_key: &db_key,
                name_key: &name_key,
            },
        ) || g.allows(Action::Delete, Target::Database { db_key: &db_key })
    }

    // R9 (r9queue): a durable queue of the database's vhost, from the
    // snapshot's `mq` module.
    fn queue(&self, db: u32, name: &str) -> Option<u64> {
        let st = self
            .cat
            .ext
            .get::<sprig_queues::catalog::MqState>(sprig_queues::catalog::MODULE)?;
        st.dbs
            .get(&db)?
            .queues
            .get(name)
            .filter(|q| q.durable && q.temp_on.is_none())
            .map(|q| q.id)
    }

    fn setting(&self, key: &str, db: u32) -> Option<String> {
        use sprig_sinks::settings as s;
        let ts = self.ts()?;
        let svc = ts.services();
        let ctx = self.cat.setting_ctx(&svc.dc).with_db(DbId(db));
        let text = |k: &'static sprig_settings::Setting<std::borrow::Cow<'static, str>>| {
            svc.settings
                .get(k, &ctx, Some(&self.cat.settings))
                .into_owned()
        };
        Some(match key {
            "destination_allowlist" => text(&s::DESTINATION_ALLOWLIST),
            "allowed_kinds" => text(&s::ALLOWED_KINDS),
            "platform_identity" => text(&s::PLATFORM_IDENTITY),
            "enabled" => {
                if svc
                    .settings
                    .get(&s::ENABLED, &ctx, Some(&self.cat.settings))
                {
                    "on".into()
                } else {
                    "off".into()
                }
            }
            other => {
                let k = s::u64_key(other)?;
                svc.settings
                    .get(k, &ctx, Some(&self.cat.settings))
                    .to_string()
            }
        })
    }

    fn residency(&self) -> Box<dyn Residency> {
        Box::new(self.residency.clone())
    }

    fn keyring(&self) -> Option<Arc<dyn SinkKeyring>> {
        keyring()
    }

    fn user_by_name(&self, name: &str) -> Option<u32> {
        let auth = sprig_auth::TenantAuth::of(&self.cat)?;
        auth.accounts(name).first().map(|u| u.row.user_id.0)
    }

    fn user_name(&self, id: u32) -> Option<String> {
        let auth = sprig_auth::TenantAuth::of(&self.cat)?;
        auth.user(UserId(id)).map(|u| u.row.name.clone())
    }

    async fn next_id(&mut self) -> Result<u64> {
        let ts = self.ts().ok_or_else(|| SprigError::internal("no tenant"))?;
        let ep = ts.services().locator.writer(self.cat.tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
        })?;
        Ok(ep.store.next_object_id(&mut conn).await?.0)
    }

    async fn commit(&mut self, change: Change) -> Result<()> {
        let tenant = self.cat.tenant;
        let (svc, user) = {
            let ts = self.ts().ok_or_else(|| SprigError::internal("no tenant"))?;
            (
                Arc::clone(ts.services()),
                ts.principal.user_id.map_or(0, |u| u.0),
            )
        };
        if !self.migrated {
            let ep = svc.locator.writer(tenant).await?;
            let mut conn = ep.pool.get_conn().await.map_err(|e| {
                SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
            })?;
            sprig_sinks::store::migrate(&mut conn, ep.store.schema()).await?;
            self.migrated = true;
        }
        let mut c = svc.catalog.change(tenant, VersionKind::Catalog).await?;
        let keys = keyring();
        let applied = {
            let (store, tx) = c.tx().parts();
            sprig_sinks::sql::apply(tx, store.schema(), &change, keys.as_deref(), tenant, user)
                .await
        };
        if let Err(e) = applied {
            let _ = c.rollback().await;
            return Err(e);
        }
        let v = c.commit().await?;
        tracing::info!(tenant = %tenant, user, change = change_name(&change), "sink catalog change");
        if let Some(ts) = self.sess.tenant.as_deref_mut() {
            ts.changed(Some(v));
            self.cat = ts.refresh().await?;
        }
        Ok(())
    }

    async fn tasks(&mut self, sink: SinkId) -> Result<Vec<TaskRow>> {
        let (mut c, schema) = self.catalog_conn().await?;
        let store = sprig_sinks::store::SinksStore::new(schema);
        if !store.migrated(&mut c).await? {
            return Ok(Vec::new());
        }
        store.tasks(&mut c, sink).await
    }

    async fn live_states(
        &mut self,
    ) -> Result<std::collections::HashMap<u64, (sprig_sinks::model::SinkState, Option<String>)>>
    {
        let (mut c, schema) = self.catalog_conn().await?;
        let store = sprig_sinks::store::SinksStore::new(schema);
        if !store.migrated(&mut c).await? {
            return Ok(std::collections::HashMap::new());
        }
        store.states(&mut c).await
    }

    async fn index(&mut self, sink: SinkId, ix: Option<SinkIndex>) -> Result<()> {
        let dc = self
            .ts()
            .ok_or_else(|| SprigError::internal("no tenant"))?
            .services()
            .locator
            .dc_store();
        match ix {
            Some(ix) => sprig_sinks::meta::index_put(&dc, &ix).await,
            None => sprig_sinks::meta::index_delete(&dc, self.cat.tenant.0, sink.0).await,
        }
    }

    async fn platform(&mut self, name: &str) -> Result<Option<(PlatformConnection, bool)>> {
        let global = Arc::clone(
            &self
                .ts()
                .ok_or_else(|| SprigError::internal("no tenant"))?
                .services()
                .global,
        );
        let Some(p) = sprig_sinks::meta::platform_get(&global, name).await? else {
            return Ok(None);
        };
        let ancestors: Vec<u64> = self.cat.ancestry.iter().map(|a| a.id.0).collect();
        let granted =
            sprig_sinks::meta::platform_granted(&global, name, self.cat.tenant.0, &ancestors)
                .await?;
        Ok(Some((p, granted)))
    }

    async fn function_source(&mut self, db: u32, name: &str) -> Result<Option<(String, String)>> {
        let (mut c, schema) = self.catalog_conn().await?;
        Ok(sprig_sinks::transform::source_of(&mut c, &schema, db, name)
            .await?
            .map(|(runtime, _, source, _)| (runtime, source)))
    }

    async fn function_version(
        &mut self,
        db: u32,
        name: &str,
        version: Option<u64>,
    ) -> Result<Option<(String, u64, String, serde_json::Value)>> {
        let (mut c, schema) = self.catalog_conn().await?;
        sprig_sinks::transform::source_at(&mut c, &schema, db, name, version).await
    }

    async fn platform_granted(&mut self) -> Result<Vec<PlatformConnection>> {
        let global = Arc::clone(
            &self
                .ts()
                .ok_or_else(|| SprigError::internal("no tenant"))?
                .services()
                .global,
        );
        let ancestors: Vec<u64> = self.cat.ancestry.iter().map(|a| a.id.0).collect();
        let me = self.cat.tenant.0;
        Ok(sprig_sinks::meta::platform_list(&global)
            .await?
            .into_iter()
            .filter(|(_, g)| {
                g.iter()
                    .any(|g| g.tenant == me || (g.subtree && ancestors.contains(&g.tenant)))
            })
            .map(|(c, _)| c)
            .collect())
    }

    fn change_stream(&self, db: u32, table: u64) -> Option<(u64, u32)> {
        let st = self
            .cat
            .ext
            .get::<sprig_streams::StreamsState>(sprig_streams::MODULE)?;
        st.all()
            .find(|s| {
                s.db.0 == db
                    && s.kind == sprig_streams::model::StreamKind::Changes
                    && s.source_object.is_some_and(|o| o.0 == table)
            })
            .map(|s| (s.id.0, s.partitions))
    }

    fn fan_in(&self) -> bool {
        // Checked against the latest grants in `fan_in_stream`: the
        // session's snapshot may predate a GRANT or REVOKE it just ran.
        true
    }

    async fn fan_in_stream(
        &mut self,
        db: &str,
        name: &str,
        under: Option<&str>,
    ) -> Result<Option<sprig_sinks::sql::FanInDef>> {
        let svc = Arc::clone(
            self.ts()
                .ok_or_else(|| SprigError::internal("no tenant"))?
                .services(),
        );
        let me = self.cat.tenant;
        let fresh = crate::inherited::catalog(&svc, me).await?;
        let user = self.ts().and_then(|t| t.principal.user_id);
        let g = crate::tenant::authz::grants_of(&svc, &fresh, user);
        if !g.unrestricted() && !g.allows(Action::FanIn, Target::Tenant) {
            return Err(SprigError::new(
                SprigCode::SinkPrivilege,
                "Access denied; you need (at least one of) the SPRIG_FAN_IN privilege(s) for a \
                 fan-in sink",
            ));
        }
        let chain = crate::inherited::chain(&svc, &self.cat).await?;
        let own = crate::inherited::state_of(&self.cat);
        let Some(d) = crate::inherited::fanin::definition(&chain, &own, db, name) else {
            return Ok(None);
        };
        let partitions = d
            .definition
            .get("partitions")
            .and_then(serde_json::Value::as_u64)
            .and_then(|p| u32::try_from(p).ok())
            .unwrap_or(1);
        let under = match under {
            None => None,
            Some(u) => {
                let id = crate::inherited::tenant_by_name(&svc, u)
                    .await?
                    .ok_or_else(|| crate::inherited::target_err(format!("no tenant '{u}'")))?;
                let e = crate::inherited::dir_entry(&svc, id)
                    .await?
                    .unwrap_or_default();
                if id != me && !e.path_ids().contains(&me.0) {
                    return Err(crate::inherited::target_err(format!(
                        "tenant '{u}' isn't below this one"
                    )));
                }
                Some(id.0)
            }
        };
        let k = d.key();
        Ok(Some(sprig_sinks::sql::FanInDef {
            def_tenant: k.tenant.0,
            def_object: k.object,
            partitions,
            under,
        }))
    }

    // R9 (r9queue): `FROM QUEUE q FAN IN`.
    async fn fan_in_queue(
        &mut self,
        db: &str,
        name: &str,
        under: Option<&str>,
    ) -> Result<Option<sprig_sinks::sql::FanInDef>> {
        let svc = Arc::clone(
            self.ts()
                .ok_or_else(|| SprigError::internal("no tenant"))?
                .services(),
        );
        let me = self.cat.tenant;
        let fresh = crate::inherited::catalog(&svc, me).await?;
        let user = self.ts().and_then(|t| t.principal.user_id);
        let g = crate::tenant::authz::grants_of(&svc, &fresh, user);
        if !g.unrestricted() && !g.allows(Action::FanIn, Target::Tenant) {
            return Err(SprigError::new(
                SprigCode::SinkPrivilege,
                "Access denied; you need (at least one of) the SPRIG_FAN_IN privilege(s) for a \
                 fan-in sink",
            ));
        }
        let chain = crate::inherited::chain(&svc, &self.cat).await?;
        let Some(d) = crate::inherited::fanin_mq::queue_definition(&chain, db, name) else {
            return Ok(None);
        };
        let under = match under {
            None => None,
            Some(u) => {
                let id = crate::inherited::tenant_by_name(&svc, u)
                    .await?
                    .ok_or_else(|| crate::inherited::target_err(format!("no tenant '{u}'")))?;
                let e = crate::inherited::dir_entry(&svc, id)
                    .await?
                    .unwrap_or_default();
                if id != me && !e.path_ids().contains(&me.0) {
                    return Err(crate::inherited::target_err(format!(
                        "tenant '{u}' isn't below this one"
                    )));
                }
                Some(id.0)
            }
        };
        let k = d.key();
        Ok(Some(sprig_sinks::sql::FanInDef {
            def_tenant: k.tenant.0,
            def_object: k.object,
            partitions: 0,
            under,
        }))
    }
}

fn change_name(c: &Change) -> &'static str {
    match c {
        Change::InsertConnection(..) => "create connection",
        Change::UpdateConnection(_) => "alter connection",
        Change::Credentials(..) => "set credentials",
        Change::DropConnection(_) => "drop connection",
        Change::InsertSink(_) => "create sink",
        Change::UpdateSink(_) => "alter sink",
        Change::ResetSink(_) => "reset sink",
        Change::State(..) => "sink state",
        Change::DropSink(_) => "drop sink",
        Change::Usage(..) => "connection usage",
    }
}

impl PassSession {
    /// Chapter 51's step of [`PassSession::tenant_text`]: `None` when the
    /// statement isn't a sink statement.
    pub(super) async fn tenant_sinks(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let Some(ts) = self.tenant.as_deref() else {
            return Ok(None);
        };
        if ts.types.internal || ts.principal.must_change_password {
            return Ok(None);
        }
        if !sprig_sinks::grammar::maybe_sink(text) {
            return Ok(None);
        }
        let Ok(sql) = std::str::from_utf8(text) else {
            return Ok(None);
        };
        // Cheap: only statements that parse as sink statements go on
        // (R9: and ALTER INHERITED CONNECTION | SINK).
        if let Ok(None) = sprig_sinks::grammar::parse(sql)
            && let Ok(None) = sprig_sinks::grammar::parse_inherited(sql)
        {
            return Ok(None);
        }
        let cat = match ts.catalog() {
            Some(c) => Arc::clone(c),
            None => match self.tenant.as_deref_mut() {
                Some(ts) => ts.refresh().await?,
                None => return Ok(None),
            },
        };
        let migrated = cat
            .ext
            .get::<SinksState>(sprig_sinks::store::MODULE)
            .is_some_and(|s| s.migrated);
        let Some(svc) = self.tenant.as_deref().map(|t| Arc::clone(t.services())) else {
            return Ok(None);
        };
        let logged = sprig_sinks::grammar::redact(sql).map_or_else(|| sql.to_owned(), |(r, _)| r);
        tracing::debug!(statement = %logged.chars().take(300).collect::<String>(), "sink statement");
        // R9 (r9more): FOR EACH TENANT connections and sinks, and the
        // overrides of inherited ones.
        let handled = match self.inherit_sinks(sql, &cat).await {
            Ok(Some(h)) => Ok(h),
            Err(e) => Err(e),
            Ok(None) => match residency_of(&svc, &cat).await {
                Ok(residency) => {
                    let mut host = GwHost {
                        sess: self,
                        cat,
                        migrated,
                        residency,
                    };
                    sprig_sinks::sql::handle(sql, &mut host).await
                }
                Err(e) => Err(e),
            },
        };
        let s = self.status();
        let status = if more {
            s | StatusFlags::MORE_RESULTS_EXISTS
        } else {
            s
        };
        let step = Step {
            errored: false,
            status: Some(status),
        };
        match handled {
            Ok(Handled::NotSink) => Ok(None),
            Err(e) => Ok(Some(self.fail(out, &e).await?)),
            Ok(Handled::Ok { affected }) => {
                let mut ok = OkPacket::new(status);
                ok.affected_rows = affected;
                out.ok(ok).await?;
                Ok(Some(step))
            }
            Ok(Handled::Rows { columns, rows }) => {
                let cols: Vec<sprig_proto_mysql::ColumnDef> = columns
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
}
