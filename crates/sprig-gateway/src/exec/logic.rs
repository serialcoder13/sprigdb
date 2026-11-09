//! Logic declarations in a tenant session (chapter 41, "Declaring code";
//! R7 order 1 and 2): `CREATE FUNCTION | PROCEDURE ... LANGUAGE kirun |
//! javascript AS ...` is compiled, placed, recorded in the tenant's logic
//! catalog and deployed into the database's shard schema by
//! `sprig_logic::service`, on this session's own backend connection, so
//! the routines' definer is the session's backend account (chapter 29,
//! decision A9) and MariaDB checks its `CREATE ROUTINE` right.
//!
//! The client gets OK with one note per placement line (`SHOW WARNINGS`):
//! the module, version, tier, and each reason the code runs in the engine
//! tier.
//!
//! **Engine-tier calls** (R7 order 4, `sprig_logic::invoke`): a `CALL`
//! goes to MariaDB as always. When the routine is an engine-tier wrapper,
//! its first resultset is the marker row; [`Tap`] holds it back, with the
//! wrapper's 39876, and the gateway runs the version on the native KIRun
//! runtime. Each `Sprig.Db.*` statement runs through this same tenant
//! session ([`PassSession::tenant_text`]): name virtualization, chapter
//! 29's checks, the caller's backend connection and transaction. The
//! client gets the row a compiled wrapper returns, then OK.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use sprig_catalog::CatalogLocator;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic::deploy::Session;
use sprig_logic::engine::bridge::{self as eb, ColKind, Column, DbBridge, DbOp, Reply};
use sprig_logic::engine::kirun::Host;
use sprig_logic::engine::outbound::HttpOutbound;
use sprig_logic::invoke::{self, EngineVersion, Marker, OutTy};
use sprig_logic::service::{self, Target};
use sprig_proto_mysql::client::PacketKind;
use sprig_proto_mysql::command::QueryAttrs;
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::resultset::{ColumnFlags, column_type as ct};
use sprig_proto_mysql::{ColumnDef, StatusFlags, Value};
use sprig_sql::names::{NameMap, ResultMapper};
use tokio::sync::{mpsc, oneshot};

use super::PassSession;
use super::tenant::{Step, scrub_typed};
use crate::backend::{BackendSession, StmtInfo};
use crate::pipeline::{EndOfRows, ExecCtx, ResultSink, RowFormat, RowValues, Wire};
use crate::tenant::session::LocalWarning;

/// `logic.function_budget_ms`'s default (chapter 41).
const FUNCTION_BUDGET_MS: u64 = 5000;
/// `logic.max_loop_iterations`'s default.
const MAX_LOOP_ITERATIONS: u64 = 100_000;
/// `logic.compile_max_cost`'s default.
const COMPILE_MAX_COST: u64 = 100_000;

/// The session's backend connection as a [`Session`].
struct Bs<'a>(&'a mut BackendSession);

impl Bs<'_> {
    async fn run(&mut self, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
        let bs = &mut *self.0;
        bs.query(&StmtInfo::write(), sql.as_bytes()).await?;
        let mut rows = Vec::new();
        let mut cols = 0usize;
        let mut err = None;
        while let Some(p) = bs.next().await? {
            match p.kind {
                PacketKind::ColumnCount(n) => cols = usize::try_from(n).unwrap_or(0),
                PacketKind::Row => {
                    let vals = sprig_proto_mysql::resultset::decode_text_row(&p.payload, cols)?;
                    rows.push(
                        vals.into_iter()
                            .map(|v| v.map(|b| String::from_utf8_lossy(b).into_owned()))
                            .collect(),
                    );
                }
                PacketKind::Err(e) => {
                    err = Some(SprigError::backend(e.code, e.sqlstate, e.message.clone()));
                }
                _ => {}
            }
        }
        let _ = bs.finish().await;
        match err {
            Some(e) => Err(e),
            None => Ok(rows),
        }
    }
}

#[async_trait]
impl Session for Bs<'_> {
    async fn exec(&mut self, sql: &str) -> Result<()> {
        self.run(sql).await.map(|_| ())
    }

    async fn rows(&mut self, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
        self.run(sql).await
    }
}

impl PassSession {
    /// A logic declaration (see the module docs).
    pub(super) async fn tenant_logic(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        status: StatusFlags,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        match self.logic_deploy(cx, text).await {
            Ok(notes) => {
                let n = notes.len();
                if let Some(ts) = self.tenant.as_deref_mut() {
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
                ok.warnings = u16::try_from(n).unwrap_or(u16::MAX);
                out.ok(ok).await?;
                Ok(Step {
                    errored: false,
                    status: Some(status),
                })
            }
            Err(e) => {
                let shown = SprigError {
                    kind: e.kind,
                    message: e.message.clone(),
                    retryable: e.retryable,
                    source: None,
                };
                out.error(&scrub_typed(shown)).await?;
                Ok(Step {
                    errored: true,
                    status: None,
                })
            }
        }
    }

    async fn logic_deploy(&mut self, cx: &mut ExecCtx<'_>, text: &Bytes) -> Result<Vec<String>> {
        let decl = sprig_logic::declare::parse(&String::from_utf8_lossy(text))?;
        if matches!(
            decl.kind,
            sprig_logic::declare::Kind::Validation
                | sprig_logic::declare::Kind::Trigger
                | sprig_logic::declare::Kind::Schedule
        ) {
            return self.logic_deploy_bound(cx, decl).await;
        }
        // The session's logical database (`USE` included).
        let current = self.current_logical();
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Err(SprigError::internal("no tenant"));
        };
        let db = decl
            .db
            .clone()
            .or(current)
            .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))?;
        let catalog = match ts.catalog() {
            Some(c) => Arc::clone(c),
            None => ts.refresh().await?,
        };
        let row = catalog
            .database(&db)
            .or_else(|| catalog.database(&db.to_ascii_lowercase()))
            .cloned()
            .ok_or_else(|| {
                SprigError::standard(1049, *b"42000", format!("Unknown database '{db}'"))
            })?;
        let schema = ts.schema_of(Some(&db)).ok_or_else(|| {
            SprigError::new(
                SprigCode::LogicUnsupported,
                format!(
                    "database {db} has more than one shard; logic deploys need chapter 21's engine"
                ),
            )
        })?;
        let user = ts
            .principal
            .user_id
            .ok_or_else(|| SprigError::internal("a logic deploy without a user"))?;
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        let ep = svc.locator.writer(tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
        })?;
        let _ = self.bs(cx);
        let Some(bs) = self.bs.as_mut() else {
            return Err(SprigError::internal("no backend session"));
        };
        let mut session = Bs(bs);
        let outcome = service::deploy(
            &decl,
            Target {
                catalog: &mut conn,
                catalog_schema: ep.store.schema(),
                catalog_version: catalog.version,
                db_id: row.db_id.0,
                shard_schema: &schema,
                single_shard: true,
                user_id: user.0,
                budget_ms: FUNCTION_BUDGET_MS,
                max_loop_iterations: MAX_LOOP_ITERATIONS,
                compile_max_cost: COMPILE_MAX_COST,
            },
            &mut session,
        )
        .await?;
        Ok(outcome.notes())
    }
}

// ---- engine-tier calls (R7 order 4) ----------------------------------------

/// A request from the KIRun run to this session: one statement.
type DbRequest = (DbOp, oneshot::Sender<Result<Reply>>);

/// The run's `db`: statements go to the session's loop, which runs them
/// through [`PassSession::tenant_text`].
struct ChannelDb(mpsc::Sender<DbRequest>);

#[async_trait]
impl DbBridge for ChannelDb {
    async fn exec(&self, op: DbOp) -> Result<Reply> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send((op, tx))
            .await
            .map_err(|_| SprigError::internal("the logic call's session went away"))?;
        rx.await
            .map_err(|_| SprigError::internal("the logic call's session went away"))?
    }
}

fn lossy(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn value_text(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::Int(i) => Some(i.to_string()),
        Value::UInt(u) => Some(u.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Double(d) => Some(d.to_string()),
        Value::Bytes(b) => Some(lossy(b)),
        other => Some(format!("{other:?}")),
    }
}

fn row_text(values: RowValues<'_>) -> Vec<Option<String>> {
    match values {
        RowValues::Text(v) => v.iter().map(|c| c.map(lossy)).collect(),
        RowValues::Binary(v) => v.iter().map(value_text).collect(),
    }
}

fn is_marker(columns: &[ColumnDef]) -> bool {
    columns
        .first()
        .is_some_and(|c| c.name.as_ref() == invoke::MARKER.as_bytes())
}

/// What [`Tap`] has seen of a `CALL`'s response.
enum TapState {
    /// Nothing yet.
    Idle,
    /// An ordinary response: everything passes.
    Passing,
    /// The marker resultset is arriving.
    Capturing {
        names: Vec<String>,
        types: Vec<(u8, bool)>,
        format: RowFormat,
        row: Option<Vec<Option<String>>>,
    },
    /// The marker is complete: the rest (39876, or an OK) is dropped.
    Done(Option<Marker>),
}

/// A sink in front of the client's for a `CALL`: passes an ordinary
/// response through, and holds back an engine-tier wrapper's marker and
/// error.
struct Tap<'a> {
    inner: &'a mut dyn ResultSink,
    state: TapState,
}

impl Tap<'_> {
    fn captured(&self) -> bool {
        matches!(self.state, TapState::Capturing { .. } | TapState::Done(_))
    }

    fn finish_capture(&mut self) {
        if let TapState::Capturing { names, row, .. } = &self.state {
            let m = row.as_ref().and_then(|r| Marker::parse(names, r));
            self.state = TapState::Done(m);
        }
    }

    fn into_marker(mut self) -> Option<Marker> {
        self.finish_capture();
        match self.state {
            TapState::Done(m) => m,
            _ => None,
        }
    }
}

#[async_trait]
impl ResultSink for Tap<'_> {
    fn wire(&self) -> Wire {
        self.inner.wire()
    }

    async fn begin_rows(&mut self, columns: &[ColumnDef], format: RowFormat) -> Result<()> {
        if matches!(self.state, TapState::Idle) {
            if is_marker(columns) {
                self.state = TapState::Capturing {
                    names: columns.iter().map(|c| lossy(&c.name)).collect(),
                    types: columns
                        .iter()
                        .map(|c| (c.col_type, c.flags & ColumnFlags::UNSIGNED.bits() != 0))
                        .collect(),
                    format,
                    row: None,
                };
                return Ok(());
            }
            self.state = TapState::Passing;
        }
        if self.captured() {
            return Ok(());
        }
        self.inner.begin_rows(columns, format).await
    }

    async fn begin_rows_raw(&mut self, defs: &[Bytes], format: RowFormat) -> Result<()> {
        if matches!(self.state, TapState::Idle) {
            let caps = self.wire().caps;
            let mut cols = Vec::with_capacity(defs.len());
            for d in defs {
                cols.push(ColumnDef::decode(d, caps)?);
            }
            if is_marker(&cols) {
                return self.begin_rows(&cols, format).await;
            }
            self.state = TapState::Passing;
        }
        if self.captured() {
            return Ok(());
        }
        self.inner.begin_rows_raw(defs, format).await
    }

    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        match &mut self.state {
            TapState::Capturing { row, .. } => {
                *row = Some(row_text(values));
                Ok(())
            }
            TapState::Done(_) => Ok(()),
            _ => self.inner.row(values).await,
        }
    }

    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        match &mut self.state {
            TapState::Capturing {
                names,
                types,
                format,
                row,
            } => {
                *row = Some(match format {
                    RowFormat::Text => {
                        sprig_proto_mysql::resultset::decode_text_row(&payload, names.len())?
                            .into_iter()
                            .map(|c| c.map(lossy))
                            .collect()
                    }
                    RowFormat::Binary => {
                        sprig_proto_mysql::resultset::decode_binary_row_types(&payload, types)?
                            .iter()
                            .map(value_text)
                            .collect()
                    }
                });
                Ok(())
            }
            TapState::Done(_) => Ok(()),
            _ => self.inner.raw_row(payload).await,
        }
    }

    async fn end_rows(&mut self, end: EndOfRows) -> Result<()> {
        if self.captured() {
            self.finish_capture();
            return Ok(());
        }
        self.inner.end_rows(end).await
    }

    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        if self.captured() {
            return Ok(());
        }
        self.inner.ok(ok).await
    }

    async fn error(&mut self, err: &SprigError) -> Result<()> {
        if self.captured() {
            self.finish_capture();
            return Ok(());
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

/// A sink that keeps one statement's result for the KIRun run.
struct Collect {
    wire: Wire,
    columns: Vec<Column>,
    rows: Vec<Vec<Option<String>>>,
    affected: Option<u64>,
    error: Option<SprigError>,
    done: bool,
}

fn column_of(c: &ColumnDef) -> Column {
    let json = c.col_type == ct::JSON || c.ext_type_name() == Some(b"json".as_slice());
    Column {
        name: lossy(&c.name),
        kind: if json {
            ColKind::Json
        } else {
            eb::kind_of_type(c.col_type, c.flags & ColumnFlags::UNSIGNED.bits() != 0)
        },
    }
}

#[async_trait]
impl ResultSink for Collect {
    fn wire(&self) -> Wire {
        self.wire
    }

    async fn begin_rows(&mut self, columns: &[ColumnDef], _format: RowFormat) -> Result<()> {
        self.columns = columns.iter().map(column_of).collect();
        Ok(())
    }

    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        self.rows.push(row_text(values));
        Ok(())
    }

    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        let r = sprig_proto_mysql::resultset::decode_text_row(&payload, self.columns.len())?
            .into_iter()
            .map(|c| c.map(lossy))
            .collect();
        self.rows.push(r);
        Ok(())
    }

    async fn end_rows(&mut self, _end: EndOfRows) -> Result<()> {
        self.done = true;
        Ok(())
    }

    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        self.affected = Some(ok.affected_rows);
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
            SprigCode::LogicRuntimeError,
            "LOAD DATA LOCAL INFILE can't run from logic code",
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

/// The wrapper's column for an output column.
fn out_column(c: &invoke::OutCol, charset: u16) -> ColumnDef {
    let name = Bytes::copy_from_slice(c.name.as_bytes());
    match c.ty {
        OutTy::Event => ColumnDef {
            name,
            charset,
            length: 256,
            col_type: ct::VAR_STRING,
            ..ColumnDef::default()
        },
        OutTy::Int => ColumnDef {
            name,
            charset: 63,
            length: 21,
            col_type: ct::LONGLONG,
            flags: (ColumnFlags::NUM | ColumnFlags::BINARY).bits(),
            ..ColumnDef::default()
        },
        OutTy::Float => ColumnDef {
            name,
            charset: 63,
            length: 22,
            col_type: ct::DOUBLE,
            decimals: 31,
            flags: (ColumnFlags::NUM | ColumnFlags::BINARY).bits(),
            ..ColumnDef::default()
        },
        OutTy::Str | OutTy::Json => ColumnDef {
            name,
            charset,
            length: u32::MAX,
            col_type: ct::BLOB,
            ..ColumnDef::default()
        },
    }
}

fn binary_value(text: Option<&String>, ty: OutTy) -> Value {
    let Some(t) = text else {
        return Value::Null;
    };
    match ty {
        OutTy::Int => t
            .parse()
            .map_or_else(|_| Value::Bytes(Bytes::from(t.clone())), Value::Int),
        OutTy::Float => t
            .parse()
            .map_or_else(|_| Value::Bytes(Bytes::from(t.clone())), Value::Double),
        _ => Value::Bytes(Bytes::from(t.clone())),
    }
}

impl PassSession {
    /// A `CALL` in a tenant session (see the module docs): relayed as
    /// always, unless the routine is an engine-tier wrapper, which the
    /// gateway answers by running the version.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn tenant_call(
        &mut self,
        cx: &mut ExecCtx<'_>,
        sql: Vec<u8>,
        original: &[u8],
        more: bool,
        format: RowFormat,
        mapper: ResultMapper,
        map: &dyn NameMap,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        let marker = {
            let mut tap = Tap {
                inner: &mut *out,
                state: TapState::Idle,
            };
            let step = self
                .routed(cx, sql, original, more, format, mapper, map, &mut tap)
                .await?;
            if !tap.captured() {
                return Ok(step);
            }
            tap.into_marker()
        };
        let Some(marker) = marker else {
            return self
                .fail(
                    out,
                    &SprigError::internal("an engine-tier wrapper returned an unreadable marker"),
                )
                .await;
        };
        let charset = cx.proto.collation;
        let started = std::time::Instant::now();
        match self.engine_call(cx, &marker).await {
            Ok((v, event, result)) => {
                tracing::debug!(
                    module = v.module_id,
                    version = v.version,
                    elapsed_ms = started.elapsed().as_millis(),
                    "engine-tier call"
                );
                let defs: Vec<ColumnDef> =
                    v.columns.iter().map(|c| out_column(c, charset)).collect();
                let row = v.row(&event, &result);
                let st = self.status();
                out.begin_rows(&defs, format).await?;
                match format {
                    RowFormat::Text => {
                        let vals: Vec<Option<&[u8]>> = row
                            .iter()
                            .map(|c| c.as_deref().map(str::as_bytes))
                            .collect();
                        out.row(RowValues::Text(&vals)).await?;
                    }
                    RowFormat::Binary => {
                        let vals: Vec<Value> = row
                            .iter()
                            .zip(&v.columns)
                            .map(|(t, c)| binary_value(t.as_ref(), c.ty))
                            .collect();
                        out.row(RowValues::Binary(&vals)).await?;
                    }
                }
                out.end_rows(EndOfRows {
                    status: st | StatusFlags::MORE_RESULTS_EXISTS,
                    warnings: 0,
                })
                .await?;
                let fin = if more {
                    st | StatusFlags::MORE_RESULTS_EXISTS
                } else {
                    st
                };
                out.ok(OkPacket::new(fin)).await?;
                Ok(Step {
                    errored: false,
                    status: Some(fin),
                })
            }
            Err(e) => self.fail(out, &e).await,
        }
    }

    /// Runs the version a marker names: loads it (cached), resolves the
    /// budgets for the session's tenant and database, and runs it with
    /// this session as its `db`.
    async fn engine_call(
        &mut self,
        cx: &mut ExecCtx<'_>,
        marker: &Marker,
    ) -> Result<(Arc<EngineVersion>, String, serde_json::Value)> {
        let current = self.current_logical();
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Err(SprigError::internal("no tenant"));
        };
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        let cat = match ts.catalog() {
            Some(c) => Arc::clone(c),
            None => ts.refresh().await?,
        };
        let tenant_key = tenant.0;
        let v = if let Some(v) = invoke::cached(tenant_key, marker.module_id, marker.version) {
            v
        } else {
            let ep = svc.locator.writer(tenant).await?;
            let mut conn = ep.pool.get_conn().await.map_err(|e| {
                SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
            })?;
            invoke::load(
                &mut conn,
                ep.store.schema(),
                tenant_key,
                marker.module_id,
                marker.version,
            )
            .await?
        };
        let prepared = v.runnable()?;
        let args = prepared.coerce(marker.args.clone())?;
        // The code's own database: its budgets, and the default database
        // of its statements (a compiled routine runs in its own schema).
        let db_row = cat.databases.get(sprig_common::ids::DbId(v.db_id)).cloned();
        let mut ctx = cat.setting_ctx(&svc.dc);
        if let Some(r) = &db_row {
            ctx = ctx.with_db(r.db_id);
        } else if let Some(db) = current.and_then(|d| cat.database(&d).map(|r| r.db_id)) {
            ctx = ctx.with_db(db);
        }
        let (limits, policy) =
            sprig_logic::settings::limits(&svc.settings, &ctx, Some(&cat.settings));
        let wire = Wire {
            persona: self.persona,
            caps: cx.proto.caps,
        };
        let (tx, mut rx) = mpsc::channel::<DbRequest>(1);
        let host = Host {
            db: Arc::new(ChannelDb(tx)),
            outbound: Arc::new(HttpOutbound::new(policy)),
            default_db: db_row.map(|r| r.name.clone()),
        };
        let run = sprig_logic::engine::run(prepared, args, host, limits);
        tokio::pin!(run);
        let outcome = loop {
            tokio::select! {
                biased;
                r = &mut run => break r,
                Some((op, reply)) = rx.recv() => {
                    let r = self.engine_db(cx, op, wire).await;
                    let _ = reply.send(r);
                }
            }
        }?;
        Ok((Arc::clone(&v), outcome.event, outcome.result))
    }

    /// One `Sprig.Db.*` statement, run as if the client had sent it.
    async fn engine_db(&mut self, cx: &mut ExecCtx<'_>, op: DbOp, wire: Wire) -> Result<Reply> {
        let mut sink = Collect {
            wire,
            columns: Vec::new(),
            rows: Vec::new(),
            affected: None,
            error: None,
            done: false,
        };
        let text = Bytes::from(op.sql.into_bytes());
        let attrs = QueryAttrs::default();
        let fut = Box::pin(self.tenant_text(cx, &text, &attrs, false, RowFormat::Text, &mut sink));
        fut.await?;
        if let Some(e) = sink.error {
            return Err(e);
        }
        Ok(match (op.shape, sink.affected) {
            (eb::Shape::Exec, Some(n)) => Reply::Affected(n),
            _ => Reply::Rows {
                columns: sink.columns,
                rows: sink.rows,
            },
        })
    }
}
