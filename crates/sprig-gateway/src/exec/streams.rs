//! Event streams in a tenant session (chapter 45, R9: build steps 2 and 3,
//! and the SQL reads).
//!
//! Before chapter 07's name plan, each statement of a tenant text goes to
//! [`sprig_streams::sql::handle`] with the tenant's streams from the
//! session's catalog snapshot. Stream DDL runs here: physical tables on the
//! shards as `sprig_ctl` (like chapter 41's deploys), the catalog row in
//! one versioned change, and the stream's feed subscription. A publish or a
//! read becomes a statement on the outbox or the log, which runs through
//! this same session ([`PassSession::tenant_text`]) in internal mode, as
//! chapter 32's plans do: logical names, chapter 29's checks, the session's
//! connection and transaction, so a publish commits or rolls back with the
//! client's other writes.
//!
//! After `ALTER TABLE t SPRIG SET (stream.change_topic = ...)` succeeds,
//! [`PassSession::streams_after_settings`] creates or drops `t.changes`.

use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use mysql_async::prelude::Queryable;
use sprig_auth::authz::{Action, Target};
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::{Namespace, ObjectKind, ObjectState, SettingRow, SettingScope};
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogLocator, TenantCatalog};
use sprig_common::ids::{ClusterId, DbId, ObjectId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::command::QueryAttrs;
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::{ColumnDef, StatusFlags};
use sprig_streams::sql::{
    Ctx, Handled, Host, PhysSlot, Privilege, StmtSettings, StreamsChange, TableInfo,
};
use sprig_streams::{ShardSlot, StreamDef, StreamsState};

use super::PassSession;
use super::tenant::Step;
use crate::pipeline::{EndOfRows, ExecCtx, ResultSink, RowFormat, RowValues, Wire};

const MODULE: &str = sprig_streams::MODULE;

fn slot_of(schema: &str, cluster: ClusterId) -> Option<PhysSlot> {
    match schema.parse::<SchemaName>().ok()? {
        SchemaName::Shard { group, shard, .. } => Some(PhysSlot {
            slot: ShardSlot { group, shard },
            schema: schema.to_owned(),
            cluster,
        }),
        _ => None,
    }
}

/// The stream layer's view of a tenant session.
struct GwHost<'s> {
    sess: &'s mut PassSession,
    cat: Arc<TenantCatalog>,
    migrated: bool,
}

impl GwHost<'_> {
    fn ts(&self) -> Option<&crate::tenant::session::TenantSession> {
        self.sess.tenant.as_deref()
    }

    fn lctn(&self) -> bool {
        self.ts()
            .is_some_and(crate::tenant::session::TenantSession::lower_case_table_names)
    }

    fn db_name(&self, db: DbId) -> Option<String> {
        self.cat.databases.get(db).map(|d| d.name.clone())
    }

    fn action(p: Privilege) -> Action {
        match p {
            Privilege::Select => Action::Select,
            Privilege::Insert => Action::Insert,
            Privilege::Create => Action::Create,
            Privilege::Alter => Action::Alter,
            Privilege::Drop => Action::Drop,
        }
    }

    async fn ctl(&self, cluster: &ClusterId) -> Result<mysql_async::Conn> {
        let ts = self.ts().ok_or_else(|| SprigError::internal("no tenant"))?;
        let pool = ts.services().locator.ctl_pool(cluster).await?;
        pool.get_conn().await.map_err(sprig_streams::maria::map_err)
    }
}

#[async_trait]
impl Host for GwHost<'_> {
    fn tenant(&self) -> TenantId {
        self.cat.tenant
    }

    fn db_id(&self, name: &str) -> Option<DbId> {
        self.cat
            .database(&database_key(name, self.lctn()))
            .map(|d| d.db_id)
    }

    fn table(&self, db: DbId, name: &str) -> Option<TableInfo> {
        let o = self
            .cat
            .object(
                db,
                Namespace::Relation,
                &object_key(ObjectKind::Table, name, self.lctn()),
            )
            .filter(|o| o.state == ObjectState::Active && o.kind == ObjectKind::Table)?;
        let ts = self.ts()?;
        let template = self.template(db)?;
        let from_layout = super::router::ddl_layout(ts)
            .and_then(|l| l(&template.schema, Some(&o.name)))
            .filter(|v| !v.is_empty());
        let slots = match from_layout {
            Some(v) => v.into_iter().filter_map(|(s, c)| slot_of(&s, c)).collect(),
            None => vec![template],
        };
        Some(TableInfo {
            object: o.object_id,
            name: o.name.clone(),
            slots,
        })
    }

    fn template(&self, db: DbId) -> Option<PhysSlot> {
        let name = self.db_name(db)?;
        let schema = self.ts()?.schema_of(Some(&name))?;
        slot_of(&schema, self.sess.target.cluster.clone())
    }

    fn allowed(&self, p: Privilege, db_name: &str, object: Option<ObjectId>, name: &str) -> bool {
        let Some(ts) = self.ts() else {
            return false;
        };
        let grants = crate::tenant::authz::privileges(ts, &self.cat);
        if grants.unrestricted() {
            return true;
        }
        let lctn = self.lctn();
        let db_key = database_key(db_name, lctn);
        let a = Self::action(p);
        if let (Some(o), Some(db)) = (object, self.db_id(db_name)) {
            return grants.allows(
                a,
                Target::Object {
                    db_key: &db_key,
                    db,
                    object: o,
                },
            );
        }
        let name_key = object_key(ObjectKind::Table, name, lctn);
        grants.allows(
            a,
            Target::Named {
                db_key: &db_key,
                name_key: &name_key,
            },
        ) || grants.allows(a, Target::Database { db_key: &db_key })
    }

    fn row_filtered(&self, db_name: &str, object: ObjectId) -> bool {
        let Some(ts) = self.ts() else {
            return false;
        };
        let Some(db) = self.db_id(db_name) else {
            return false;
        };
        let db_key = database_key(db_name, self.lctn());
        crate::tenant::authz::privileges(ts, &self.cat)
            .row_filter(
                Target::Object {
                    db_key: &db_key,
                    db,
                    object,
                },
                Action::Select,
            )
            .is_some()
    }

    async fn change_view(&mut self, def: &StreamDef) -> Result<()> {
        // Chapter 29 (RL47).
        Box::pin(self.sess.deploy_change_view(def)).await
    }

    async fn change_filter(
        &mut self,
        db_name: &str,
        def: &StreamDef,
    ) -> Result<Option<sprig_streams::read::RecordFilter>> {
        // Chapter 29 (RL44): the records whose images this session's
        // policies show.
        let (Some(object), Some(db)) = (def.source_object, self.db_id(db_name)) else {
            return Err(crate::tenant::rls_streams::bound(
                def.source_name.as_deref().unwrap_or_default(),
                &def.name,
            ));
        };
        let obj = self
            .cat
            .objects
            .get(object)
            .cloned()
            .ok_or_else(|| SprigError::internal("a change topic's table is gone"))?;
        let slot = self
            .table(db, &obj.name)
            .and_then(|t| t.slots.into_iter().next())
            .ok_or_else(|| SprigError::internal("a change topic's table has no shard"))?;
        let (lex, _) = self.sess.lex_opts();
        let current = self.sess.current_logical();
        let ts = self.ts().ok_or_else(|| SprigError::internal("no tenant"))?;
        let columns = crate::tenant::rls_streams::filter_columns(
            ts.services(),
            self.cat.tenant.0,
            &obj,
            &slot.cluster,
            &slot.schema,
        )
        .await?;
        crate::tenant::rls_streams::record_filter(
            ts,
            &self.cat,
            db_name,
            object,
            &columns,
            lex,
            current.as_deref(),
        )
        .await
    }

    fn settings(&self, db: DbId) -> StmtSettings {
        use sprig_streams::settings as s;
        let Some(ts) = self.ts() else {
            return StmtSettings {
                partitions_default: 12,
                max_message_bytes: s::MAX_MESSAGE_BYTES.default,
            };
        };
        let svc = ts.services();
        let ctx = self.cat.setting_ctx(&svc.dc).with_db(db);
        let get = |k: &'static sprig_settings::Setting<u64>| {
            svc.settings.get(k, &ctx, Some(&self.cat.settings))
        };
        StmtSettings {
            partitions_default: u32::try_from(get(&s::PARTITIONS_DEFAULT)).unwrap_or(12),
            max_message_bytes: get(&s::MAX_MESSAGE_BYTES),
        }
    }

    fn change_topic_on(&self, _db: DbId, object: ObjectId) -> bool {
        let Some(ts) = self.ts() else {
            return false;
        };
        let svc = ts.services();
        let ctx = self.cat.object_ctx(&svc.dc, object);
        svc.settings.get(
            &sprig_streams::settings::CHANGE_TOPIC,
            &ctx,
            Some(&self.cat.settings),
        )
    }

    fn user(&self) -> (String, String) {
        self.ts().map_or_else(Default::default, |t| {
            (t.principal.user.clone(), t.principal.client_ip.to_string())
        })
    }

    async fn ddl(&mut self, slot: &PhysSlot, statements: &[String]) -> Result<()> {
        let mut c = self.ctl(&slot.cluster).await?;
        for s in statements {
            c.query_drop(s.as_str())
                .await
                .map_err(sprig_streams::maria::map_err)?;
        }
        Ok(())
    }

    async fn position(&mut self, slot: &PhysSlot) -> Result<String> {
        let mut c = self.ctl(&slot.cluster).await?;
        let p: Option<String> = c
            .query_first("SELECT @@gtid_binlog_pos")
            .await
            .map_err(sprig_streams::maria::map_err)?;
        Ok(p.unwrap_or_default())
    }

    async fn next_id(&mut self) -> Result<u64> {
        let ts = self.ts().ok_or_else(|| SprigError::internal("no tenant"))?;
        let ep = ts.services().locator.writer(self.cat.tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
        })?;
        Ok(ep.store.next_object_id(&mut conn).await?.0)
    }

    async fn commit(&mut self, change: &StreamsChange) -> Result<()> {
        let tenant = self.cat.tenant;
        let (svc, set_by) = {
            let ts = self.ts().ok_or_else(|| SprigError::internal("no tenant"))?;
            (
                Arc::clone(ts.services()),
                ts.principal
                    .user_id
                    .map_or_else(|| "system".to_owned(), |u| format!("user:{}", u.0)),
            )
        };
        if !self.migrated {
            let ep = svc.locator.writer(tenant).await?;
            let mut conn = ep.pool.get_conn().await.map_err(|e| {
                SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
            })?;
            sprig_streams::store::migrate(&mut conn, ep.store.schema()).await?;
            self.migrated = true;
        }
        let mut c = svc.catalog.change(tenant, VersionKind::Catalog).await?;
        let version = c.version();
        let applied = {
            let (store, tx) = c.tx().parts();
            let streams = sprig_streams::StreamsStore::new(store.schema());
            let retention = |def: &StreamDef, h: u64| SettingRow {
                scope: SettingScope::Object(ObjectId(def.id.0)),
                key: sprig_streams::settings::RETENTION_HOURS.key.to_owned(),
                value: Some(serde_json::json!(h)),
                bounds: None,
                set_by: set_by.clone(),
                set_at: SystemTime::now(),
                version,
            };
            async {
                match change {
                    StreamsChange::Insert(def, h) => {
                        streams.insert(tx, def).await?;
                        if let Some(h) = h {
                            store.put_setting(tx, &retention(def, *h)).await?;
                        }
                    }
                    StreamsChange::Update(def, changes) => {
                        streams.update(tx, def).await?;
                        // A change topic's settings are its table's.
                        let scope = SettingScope::Object(ObjectId(
                            def.source_object.map_or(def.id.0, |o| o.0),
                        ));
                        for c in changes {
                            match c.value {
                                Some(v) => {
                                    store
                                        .put_setting(
                                            tx,
                                            &SettingRow {
                                                scope,
                                                key: c.key.to_owned(),
                                                value: Some(serde_json::json!(v)),
                                                bounds: None,
                                                set_by: set_by.clone(),
                                                set_at: SystemTime::now(),
                                                version,
                                            },
                                        )
                                        .await?;
                                }
                                None => {
                                    store
                                        .reset_setting(
                                            tx,
                                            scope,
                                            c.key,
                                            true,
                                            false,
                                            &set_by,
                                            SystemTime::now(),
                                            version,
                                        )
                                        .await?;
                                }
                            }
                        }
                    }
                    StreamsChange::Delete(def) => streams.delete(tx, def.id).await?,
                }
                Ok::<_, SprigError>(())
            }
            .await
        };
        if let Err(e) = applied {
            let _ = c.rollback().await;
            return Err(e);
        }
        let v = c.commit().await?;
        if let Some(ts) = self.sess.tenant.as_deref_mut() {
            ts.changed(Some(v));
            let fresh = ts.refresh().await?;
            self.cat = fresh;
        }
        Ok(())
    }

    async fn subscribe(&mut self, def: &StreamDef) -> Result<()> {
        let dc = self
            .ts()
            .ok_or_else(|| SprigError::internal("no tenant"))?
            .services()
            .locator
            .dc_store();
        sprig_streams::runtime::subscribe(dc, self.cat.tenant, def).await
    }

    async fn unsubscribe(&mut self, def: &StreamDef) -> Result<()> {
        let dc = self
            .ts()
            .ok_or_else(|| SprigError::internal("no tenant"))?
            .services()
            .locator
            .dc_store();
        sprig_streams::runtime::unsubscribe(dc, self.cat.tenant, def.id).await
    }
}

/// The tenant's streams at `cat`.
fn state_of(cat: &TenantCatalog) -> (StreamsState, bool) {
    match cat.ext.get::<StreamsState>(MODULE) {
        Some(s) => (s.clone(), s.migrated),
        None => (StreamsState::default(), false),
    }
}

impl PassSession {
    fn streams_status(&self, more: bool) -> StatusFlags {
        let s = self.status();
        if more {
            s | StatusFlags::MORE_RESULTS_EXISTS
        } else {
            s
        }
    }

    /// Chapter 45's step of [`PassSession::tenant_text`]: `None` when the
    /// statement isn't the stream layer's.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn tenant_streams(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        attrs: &QueryAttrs,
        more: bool,
        format: RowFormat,
        cat: &Arc<TenantCatalog>,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let Some(ts) = self.tenant.as_deref() else {
            return Ok(None);
        };
        if ts.types.internal || ts.principal.must_change_password {
            return Ok(None);
        }
        let (state, migrated) = state_of(cat);
        if !sprig_streams::grammar::maybe_stream(text, !state.is_empty()) {
            return Ok(None);
        }
        let Ok(sql) = std::str::from_utf8(text) else {
            return Ok(None);
        };
        let lctn = ts.lower_case_table_names();
        let current = self.current_logical();
        let (lex, _) = self.lex_opts();
        let mysql = self.persona == sprig_proto_mysql::Persona::MySql84;
        let handled = {
            let ctx = Ctx {
                state: &state,
                lex,
                lctn,
                current: current.as_deref(),
                mysql,
            };
            let mut host = GwHost {
                sess: self,
                cat: Arc::clone(cat),
                migrated,
            };
            sprig_streams::sql::handle(sql, &ctx, &mut host).await
        };
        let status = self.streams_status(more);
        let step = Step {
            errored: false,
            status: Some(status),
        };
        match handled {
            Ok(Handled::NotStream) => Ok(None),
            Err(e) => Ok(Some(self.fail(out, &e).await?)),
            Ok(Handled::Ok { affected }) => {
                let mut ok = OkPacket::new(status);
                ok.affected_rows = affected;
                out.ok(ok).await?;
                Ok(Some(step))
            }
            Ok(Handled::Rows { columns, rows }) => {
                let cols: Vec<ColumnDef> = columns
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
            Ok(Handled::Forward { sql, compact, .. }) => {
                if let Some(ts) = self.tenant.as_deref_mut() {
                    ts.types.internal = true;
                }
                let text = Bytes::from(sql.into_bytes());
                let r = if compact {
                    let mut sink = CompactSink { inner: out };
                    Box::pin(self.tenant_text(cx, &text, attrs, more, format, &mut sink)).await
                } else {
                    Box::pin(self.tenant_text(cx, &text, attrs, more, format, out)).await
                };
                if let Some(ts) = self.tenant.as_deref_mut() {
                    ts.types.internal = false;
                }
                r.map(Some)
            }
        }
    }

    /// After a successful `ALTER TABLE t SPRIG SET | RESET (...)` naming
    /// `stream.change_topic`: creates or drops `t.changes` to match. An
    /// error is the statement's (the setting stays).
    pub(super) async fn streams_after_settings(&mut self, text: &[u8]) -> Result<()> {
        let Ok(sql) = std::str::from_utf8(text) else {
            return Ok(());
        };
        let (lex, _) = self.lex_opts();
        let Some(target) = sprig_streams::sql::change_topic_target(sql, &lex) else {
            return Ok(());
        };
        let current = self.current_logical();
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Ok(());
        };
        let lctn = ts.lower_case_table_names();
        let cat = ts.refresh().await?;
        let (state, migrated) = state_of(&cat);
        let ctx = Ctx {
            state: &state,
            lex,
            lctn,
            current: current.as_deref(),
            mysql: false,
        };
        let mut host = GwHost {
            sess: self,
            cat,
            migrated,
        };
        sprig_streams::sql::reconcile_change_topic(&target, &ctx, &mut host)
            .await
            .map(|_| ())
    }
}

/// A publish to a compacted stream: MariaDB's `CHECK` failure on a keyless
/// record (4025, `sprig_sc_needs_key`) becomes 39955.
struct CompactSink<'a> {
    inner: &'a mut dyn ResultSink,
}

#[async_trait]
impl ResultSink for CompactSink<'_> {
    fn wire(&self) -> Wire {
        self.inner.wire()
    }
    async fn begin_rows(&mut self, columns: &[ColumnDef], format: RowFormat) -> Result<()> {
        self.inner.begin_rows(columns, format).await
    }
    async fn begin_rows_raw(&mut self, defs: &[Bytes], format: RowFormat) -> Result<()> {
        self.inner.begin_rows_raw(defs, format).await
    }
    async fn cursor_opened(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
        self.inner.cursor_opened(columns, end).await
    }
    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        self.inner.row(values).await
    }
    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        self.inner.raw_row(payload).await
    }
    async fn end_rows(&mut self, end: EndOfRows) -> Result<()> {
        self.inner.end_rows(end).await
    }
    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        self.inner.ok(ok).await
    }
    async fn error(&mut self, err: &SprigError) -> Result<()> {
        if err.message.contains("sprig_sc_needs_key") {
            let e = SprigError::new(
                SprigCode::StreamCompactNeedsKey,
                "ER_SPRIG_STREAM_COMPACT_NEEDS_KEY: a record without a key on a compacted stream",
            );
            return self.inner.error(&e).await;
        }
        self.inner.error(err).await
    }
    async fn column_list(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
        self.inner.column_list(columns, end).await
    }
    async fn local_infile_request(&mut self, file_name: &[u8]) -> Result<()> {
        self.inner.local_infile_request(file_name).await
    }
    async fn local_infile_read(&mut self) -> Result<Bytes> {
        self.inner.local_infile_read().await
    }
    async fn flush(&mut self) -> Result<()> {
        self.inner.flush().await
    }
    fn finished(&self) -> bool {
        self.inner.finished()
    }
}
