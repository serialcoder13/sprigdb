//! Typed tables and record hierarchies in a tenant session (chapter 32,
//! R6 order 1, single-shard tenants).
//!
//! Before chapter 07's name plan, each statement of a tenant text goes to
//! [`sprig_types::handle`] with the tenant's `types` state from the
//! session's catalog snapshot. A statement that isn't the type layer's
//! goes on unchanged. Otherwise the plan's statements run through this
//! same session ([`PassSession::tenant_text`], as chapter 41's logic does):
//! logical names, chapter 29's checks, the session's backend connection
//! and transaction, and chapter 17's router once the tenant has shards.
//! They run in *internal* mode (the type layer's own `_sprig_` columns and
//! index names pass chapter 07's reserved-name check, and the type layer
//! doesn't see them again); a client's text naming a reserved name never
//! reaches the type layer.
//!
//! A rewritten `SELECT` streams to the client through [`MetaSink`], which
//! renames the generated table aliases in the column metadata back to the
//! names the client wrote. Prepared statements naming typed or
//! hierarchical tables are refused (39710) in this release.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::ObjectKind;
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogLocator, Namespace, TenantCatalog};
use sprig_common::ids::{DbId, ObjectId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::command::QueryAttrs;
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::{Caps, ColumnDef, StatusFlags};
use sprig_sql::lex::LexOptions;
use sprig_types::store::MODULE;
use sprig_types::{
    Ctx, Done, Handled, Limits, MetaRename, Rows, Runner, StateLookup, TypesChange, TypesState,
    TypesStore,
};

use super::PassSession;
use super::tenant::Step;
use crate::pipeline::{EndOfRows, ExecCtx, ResultSink, RowFormat, RowValues, Wire};
use crate::tenant::session::TenantSession;

/// The per-session state of the type layer.
#[derive(Debug, Clone, Copy, Default)]
pub struct TypesSession {
    /// A plan's own statement is running: no type planning, reserved
    /// names allowed.
    pub internal: bool,
    /// The running internal statement is chapter 32's own: chapter 42's
    /// layer still applies to it (typed tables in inheritance groups).
    pub typed_plan: bool,
    /// `SET @sprig_inheritance = 'only'`.
    pub inheritance_only: bool,
}

fn limits(ts: &TenantSession, cat: &TenantCatalog) -> Limits {
    use sprig_types::settings as s;
    let svc = ts.services();
    let ctx = cat.setting_ctx(&svc.dc);
    let get =
        |k: &'static sprig_settings::Setting<u64>| svc.settings.get(k, &ctx, Some(&cat.settings));
    Limits {
        type_max_depth: u8::try_from(get(&s::TYPES_MAX_DEPTH)).unwrap_or(8),
        write_batch_rows: usize::try_from(get(&s::TYPES_WRITE_BATCH_ROWS)).unwrap_or(1000),
        hierarchy_max_depth: u16::try_from(get(&s::HIERARCHY_MAX_DEPTH)).unwrap_or(64),
        max_move_rows: get(&s::HIERARCHY_MAX_MOVE_ROWS),
    }
}

/// Whether a statement to prepare names a typed or hierarchical table:
/// refused (39710) in this release.
pub(super) fn prepare_refusal(
    ts: &TenantSession,
    sql: &[u8],
    lex: LexOptions,
) -> Option<SprigError> {
    let cat = ts.catalog()?;
    let state = cat.ext.get::<TypesState>(MODULE)?;
    if state.is_empty() {
        return None;
    }
    let mut names = Vec::new();
    for (_, d) in state.dbs() {
        names.extend(
            d.iter()
                .filter(|t| !t.is_root() || d.has_subtypes(t.id))
                .map(|t| t.name.to_lowercase()),
        );
        names.extend(d.hierarchies().map(|h| h.table.to_lowercase()));
    }
    let hit = sprig_types::grammar::identifiers(sql, &lex)
        .iter()
        .any(|i| names.contains(&i.to_lowercase()));
    hit.then(|| {
        SprigError::new(
            SprigCode::TypeUnsupported,
            "prepared statements on typed or hierarchical tables are not supported yet: send \
             the statement as text",
        )
    })
}

impl PassSession {
    /// Chapter 32's step of [`PassSession::tenant_text`]: `None` when the
    /// statement isn't the type layer's.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) async fn tenant_types(
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
        let state = cat.ext.get::<TypesState>(MODULE);
        if !sprig_types::maybe_typed(text, state) {
            return Ok(None);
        }
        let Ok(sql) = std::str::from_utf8(text) else {
            return Ok(None);
        };
        let lctn = ts.lower_case_table_names();
        let inheritance_only = ts.types.inheritance_only;
        let lim = limits(ts, cat);
        let current = self.current_logical();
        let (lex, _) = self.lex_opts();
        let db_cat = Arc::clone(cat);
        let db_id = move |db: Option<&str>| -> Option<DbId> {
            let name = db.map(str::to_owned).or_else(|| current.clone())?;
            db_cat.database(&database_key(&name, lctn)).map(|r| r.db_id)
        };
        let empty = TypesState::unmigrated();
        let name_cat = Arc::clone(cat);
        let db_name = move |id: DbId| -> Option<String> {
            name_cat.databases.get(id).map(|d| d.name.clone())
        };
        let lookup = StateLookup {
            state: state.unwrap_or(&empty),
            db_id: &db_id,
            lctn,
            db_name: &db_name,
        };
        let ctx = Ctx {
            lookup: &lookup,
            lex,
            inheritance_only,
            limits: lim,
        };
        let wire = Wire {
            persona: self.persona,
            caps: cx.proto.caps,
        };
        let migrated = state.is_some_and(|s| s.migrated);
        let handled = {
            let mut runner = GwRunner {
                sess: self,
                cx,
                wire,
                migrated,
                typed: true,
            };
            let h = sprig_types::handle(sql, state, &ctx, &mut runner).await;
            if let Some(ts) = runner.sess.tenant.as_deref_mut() {
                ts.types.internal = false;
                ts.types.typed_plan = false;
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
        // A write the type layer leaves alone (to a single-table root, say)
        // can still fail a subtype's NOT NULL check: it goes on through the
        // error mapping. Its text names no reserved name, so the plan mode
        // grants it nothing.
        let handled = match handled {
            Ok(Handled::NotTyped)
                if sprig_types::has_nn_checks(state)
                    && !sprig_types::grammar::names_reserved(text, &lex)
                    && is_write(text) =>
            {
                Ok(Handled::Forward {
                    sql: sql.to_owned(),
                    meta: Vec::new(),
                })
            }
            other => other,
        };
        match handled {
            Ok(Handled::NotTyped) => Ok(None),
            Err(e) => {
                let e = sprig_types::map_error(state, &e).unwrap_or(e);
                Ok(Some(self.fail(out, &e).await?))
            }
            Ok(Handled::Inheritance(v)) => {
                if let Some(ts) = self.tenant.as_deref_mut() {
                    ts.types.inheritance_only = v == Some(true);
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
                send_rows(out, &rows, format, status).await?;
                Ok(Some(step))
            }
            Ok(Handled::Forward { sql, meta }) => {
                if let Some(ts) = self.tenant.as_deref_mut() {
                    ts.types.internal = true;
                    ts.types.typed_plan = true;
                }
                let mut sink = MetaSink {
                    inner: out,
                    meta,
                    types: state,
                };
                let text = Bytes::from(sql.into_bytes());
                let r = Box::pin(self.tenant_text(cx, &text, attrs, more, format, &mut sink)).await;
                if let Some(ts) = self.tenant.as_deref_mut() {
                    ts.types.internal = false;
                    ts.types.typed_plan = false;
                }
                r.map(Some)
            }
        }
    }
}

/// `INSERT`, `REPLACE` or `UPDATE` (after leading spaces and comments are
/// ignored by the first word only).
fn is_write(text: &[u8]) -> bool {
    let t: Vec<u8> = text
        .iter()
        .skip_while(|b| b.is_ascii_whitespace() || **b == b'(')
        .take(7)
        .map(u8::to_ascii_uppercase)
        .collect();
    t.starts_with(b"INSERT") || t.starts_with(b"REPLACE") || t.starts_with(b"UPDATE")
}

pub(super) async fn send_rows(
    out: &mut dyn ResultSink,
    rows: &Rows,
    format: RowFormat,
    status: StatusFlags,
) -> Result<()> {
    if format == RowFormat::Binary {
        return Err(SprigError::new(
            SprigCode::TypeUnsupported,
            "this statement on a typed table can't be prepared",
        ));
    }
    out.begin_rows(&rows.columns, RowFormat::Text).await?;
    for r in &rows.rows {
        let vals: Vec<Option<&[u8]>> = r.iter().map(Option::as_deref).collect();
        out.row(RowValues::Text(&vals)).await?;
    }
    out.end_rows(EndOfRows {
        status,
        warnings: 0,
    })
    .await
}

/// The type layer's runner: each statement through the session itself.
pub(super) struct GwRunner<'s, 'c, 'x> {
    pub(super) sess: &'s mut PassSession,
    pub(super) cx: &'c mut ExecCtx<'x>,
    pub(super) wire: Wire,
    pub(super) migrated: bool,
    /// Statements of chapter 32's plans ([`TypesSession::typed_plan`]),
    /// not chapter 42's.
    pub(super) typed: bool,
}

impl GwRunner<'_, '_, '_> {
    async fn run(&mut self, sql: &str) -> Result<Collect> {
        let mut sink = Collect {
            wire: self.wire,
            columns: Vec::new(),
            rows: Vec::new(),
            ok: None,
            error: None,
            done: false,
        };
        if let Some(ts) = self.sess.tenant.as_deref_mut() {
            ts.types.internal = true;
            ts.types.typed_plan = self.typed;
        }
        let text = Bytes::copy_from_slice(sql.as_bytes());
        let attrs = QueryAttrs::default();
        let r = Box::pin(self.sess.tenant_text(
            self.cx,
            &text,
            &attrs,
            false,
            RowFormat::Text,
            &mut sink,
        ))
        .await;
        if let Some(ts) = self.sess.tenant.as_deref_mut() {
            ts.types.internal = false;
            ts.types.typed_plan = false;
        }
        if let Err(e) = &r {
            tracing::debug!(sql, code = e.protocol().mysql_errno, message = %e.message, typed = self.typed, "plan statement failed");
        }
        r?;
        if let Some(e) = sink.error.take() {
            tracing::debug!(sql, code = e.protocol().mysql_errno, message = %e.message, typed = self.typed, "plan statement failed");
            return Err(e);
        }
        Ok(sink)
    }
}

#[async_trait]
impl Runner for GwRunner<'_, '_, '_> {
    async fn query(&mut self, sql: &str) -> Result<Rows> {
        let c = self.run(sql).await?;
        Ok(Rows {
            columns: c.columns,
            rows: c.rows,
        })
    }

    async fn exec(&mut self, sql: &str) -> Result<Done> {
        let c = self.run(sql).await?;
        Ok(c.ok.unwrap_or_default())
    }

    fn in_transaction(&self) -> bool {
        let s = self.sess.status();
        s.contains(StatusFlags::IN_TRANS) || !s.contains(StatusFlags::AUTOCOMMIT)
    }

    fn found_rows(&self) -> bool {
        self.cx.proto.caps.contains(Caps::FOUND_ROWS)
    }

    async fn snapshot_isolation(&mut self) -> Result<bool> {
        // The session's own value when it (or its persona, D9) set one;
        // else the server's (bugfix-54).
        let known = self.sess.bs.as_ref().and_then(|b| {
            b.vars()
                .vars
                .get("innodb_snapshot_isolation")
                .map(|v| v.eq_ignore_ascii_case("ON") || v == "1")
        });
        if let Some(on) = known {
            return Ok(on);
        }
        let rows = self
            .query(sprig_types::exec::SNAPSHOT_ISOLATION_READ)
            .await?;
        Ok(rows
            .rows
            .first()
            .and_then(|r| r.first())
            .and_then(Option::as_deref)
            .is_some_and(|v| v == b"1" || v.eq_ignore_ascii_case(b"ON")))
    }

    async fn object_id(
        &mut self,
        db: Option<&str>,
        table: &str,
    ) -> Result<Option<(DbId, ObjectId)>> {
        let current = self.sess.current_logical();
        let Some(ts) = self.sess.tenant.as_deref_mut() else {
            return Err(SprigError::internal("no tenant"));
        };
        let lctn = ts.lower_case_table_names();
        let cat = ts.refresh().await?;
        let Some(name) = db.map(str::to_owned).or(current) else {
            return Err(SprigError::standard(
                1046,
                *b"3D000",
                "No database selected",
            ));
        };
        let Some(d) = cat.database(&database_key(&name, lctn)) else {
            return Ok(None);
        };
        let key = object_key(ObjectKind::Table, table, lctn);
        Ok(cat
            .object(d.db_id, Namespace::Relation, &key)
            .map(|o| (d.db_id, o.object_id)))
    }

    async fn tree_inheritable(&mut self, db: Option<&str>, root: &str) -> Result<bool> {
        let current = self.sess.current_logical();
        let Some(ts) = self.sess.tenant.as_deref_mut() else {
            return Err(SprigError::internal("no tenant"));
        };
        let lctn = ts.lower_case_table_names();
        let cat = ts.refresh().await?;
        let Some(name) = db.map(str::to_owned).or(current) else {
            return Ok(false);
        };
        let Some(d) = cat.database(&database_key(&name, lctn)) else {
            return Ok(false);
        };
        Ok(cat
            .ext
            .get::<sprig_inherit::InheritState>(sprig_inherit::MODULE)
            .is_some_and(|s| s.object_named_ci(d.db_id, root).is_some()))
    }

    fn set_physical(&mut self, on: bool) {
        // Not a type plan's statement: chapter 42 doesn't plan it.
        self.typed = !on;
    }

    async fn tree_changed(&mut self, db: Option<&str>, root: &str, key: &[String]) -> Result<()> {
        if !self.tree_inheritable(db, root).await? {
            return Ok(());
        }
        let table = match db {
            Some(d) => format!(
                "{}.{}",
                sprig_common::names::quote(d),
                sprig_common::names::quote(root)
            ),
            None => sprig_common::names::quote(root),
        };
        let keys: Vec<String> = key.iter().map(|k| sprig_common::names::quote(k)).collect();
        // Through chapter 42 (a type plan's statement): the tree's new
        // tables become inheritable and new columns get field slots.
        self.exec(&format!(
            "ALTER TABLE {table} SPRIG INHERITABLE KEY ({})",
            keys.join(", ")
        ))
        .await
        .map(|_| ())
    }

    async fn commit_types(&mut self, change: &TypesChange) -> Result<()> {
        let Some(ts) = self.sess.tenant.as_deref_mut() else {
            return Err(SprigError::internal("no tenant"));
        };
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        if !self.migrated {
            let ep = svc.locator.writer(tenant).await?;
            let mut conn = ep.pool.get_conn().await.map_err(|e| {
                SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
            })?;
            sprig_types::store::migrate(&mut conn, ep.store.schema()).await?;
            self.migrated = true;
        }
        let mut c = svc.catalog.change(tenant, VersionKind::Catalog).await?;
        let applied = {
            let (store, tx) = c.tx().parts();
            TypesStore::new(store.schema()).apply(tx, change).await
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
}

/// Keeps one statement's result.
struct Collect {
    wire: Wire,
    columns: Vec<ColumnDef>,
    rows: Vec<Vec<Option<Vec<u8>>>>,
    ok: Option<Done>,
    error: Option<SprigError>,
    done: bool,
}

#[async_trait]
impl ResultSink for Collect {
    fn wire(&self) -> Wire {
        self.wire
    }

    async fn begin_rows(&mut self, columns: &[ColumnDef], _format: RowFormat) -> Result<()> {
        self.columns = columns.to_vec();
        Ok(())
    }

    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        let r = match values {
            RowValues::Text(v) => v.iter().map(|c| c.map(<[u8]>::to_vec)).collect(),
            RowValues::Binary(_) => {
                return Err(SprigError::internal("a binary row in a type plan"));
            }
        };
        self.rows.push(r);
        Ok(())
    }

    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        let r = sprig_proto_mysql::resultset::decode_text_row(&payload, self.columns.len())?
            .into_iter()
            .map(|c| c.map(<[u8]>::to_vec))
            .collect();
        self.rows.push(r);
        Ok(())
    }

    async fn end_rows(&mut self, _end: EndOfRows) -> Result<()> {
        self.done = true;
        Ok(())
    }

    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        self.ok = Some(Done {
            affected_rows: ok.affected_rows,
            last_insert_id: ok.last_insert_id,
            info: String::from_utf8_lossy(&ok.info).into_owned(),
            warnings: ok.warnings,
        });
        self.done = true;
        Ok(())
    }

    async fn error(&mut self, err: &SprigError) -> Result<()> {
        self.error = Some(SprigError {
            kind: err.kind,
            message: err.message.clone(),
            retryable: err.retryable,
            source: None,
        });
        self.done = true;
        Ok(())
    }

    async fn column_list(&mut self, _columns: &[ColumnDef], _end: EndOfRows) -> Result<()> {
        Ok(())
    }

    async fn local_infile_request(&mut self, _file_name: &[u8]) -> Result<()> {
        Err(SprigError::new(
            SprigCode::TypeUnsupported,
            "LOAD DATA LOCAL INFILE can't run inside a type plan",
        ))
    }

    async fn local_infile_read(&mut self) -> Result<Bytes> {
        Ok(Bytes::new())
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn finished(&self) -> bool {
        self.done
    }
}

/// The client's sink, with the rewrite's table aliases renamed in column
/// metadata (chapter 32, "Polymorphic SELECT", step 7).
struct MetaSink<'a> {
    inner: &'a mut dyn ResultSink,
    meta: Vec<MetaRename>,
    /// For [`sprig_types::map_error`].
    types: Option<&'a TypesState>,
}

impl MetaSink<'_> {
    fn rename(&self, c: &mut ColumnDef) -> bool {
        let Some(m) = self
            .meta
            .iter()
            .find(|m| c.table.as_ref() == m.alias.as_bytes())
        else {
            return false;
        };
        c.table = Bytes::copy_from_slice(m.table.as_bytes());
        c.org_table = Bytes::copy_from_slice(m.org_table.as_bytes());
        true
    }
}

#[async_trait]
impl ResultSink for MetaSink<'_> {
    fn wire(&self) -> Wire {
        self.inner.wire()
    }

    async fn begin_rows(&mut self, columns: &[ColumnDef], format: RowFormat) -> Result<()> {
        let mut cols = columns.to_vec();
        for c in &mut cols {
            self.rename(c);
        }
        self.inner.begin_rows(&cols, format).await
    }

    async fn begin_rows_raw(&mut self, defs: &[Bytes], format: RowFormat) -> Result<()> {
        if self.meta.is_empty() {
            return self.inner.begin_rows_raw(defs, format).await;
        }
        let caps = self.wire().caps;
        let mut cols = Vec::with_capacity(defs.len());
        let mut changed = false;
        for d in defs {
            let mut c = ColumnDef::decode(d, caps)?;
            changed |= self.rename(&mut c);
            cols.push(c);
        }
        if changed {
            self.inner.begin_rows(&cols, format).await
        } else {
            self.inner.begin_rows_raw(defs, format).await
        }
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
        match sprig_types::map_error(self.types, err) {
            Some(e) => self.inner.error(&e).await,
            None => self.inner.error(err).await,
        }
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
