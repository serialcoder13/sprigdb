//! `CALL sprig.commit_offset(group, stream, partition, offset [, metadata])`
//! in a tenant session (for exactly-once consume and write), and the same
//! commit for `db.commitOffset` and the logic triggers on streams.
//!
//! ```text
//! the arguments: SELECT <args> through this session (any expression)
//! the stream: [db.]name, the database's stream; SELECT on it
//! outside a transaction: written at once as sprig_ctl (sprig_streams::store)
//! inside one: CALL t<tenant>_sprig.__sprig_commit_offset(...) on the
//!   session's own connection to the catalog's cluster, in its transaction
//!   (the coordinator's branch there), after SET ROLE t<tenant>_sprig_offsets
//! ```
//!
//! The procedure and the role are made by `sprig_ctl` the first time an
//! account commits an offset ([`sprig_streams::offsets::grant_sql`]), then
//! remembered per cluster and account. The catalog must be on the cluster
//! of the session's own connection, as the router keeps it; a transaction
//! whose connection is elsewhere gets 39958 rather than an XA branch on the
//! catalog (the coordinator anchors its decisions in shard schemas).

use std::collections::HashSet;
use std::sync::{Mutex, OnceLock, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use mysql_async::prelude::Queryable;
use sprig_auth::authz::{Action, Target};
use sprig_catalog::CatalogLocator;
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::ObjectKind;
use sprig_common::ids::ClusterId;
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::command::QueryAttrs;
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::{ColumnDef, StatusFlags};
use sprig_streams::offsets::{self, Commit};
use sprig_streams::{StreamDef, StreamsState};

use super::PassSession;
use super::tenant::Step;
use crate::pipeline::{EndOfRows, ExecCtx, ResultSink, RowFormat, RowValues, Wire};

/// One statement's result, kept (internal statements of this module and
/// of [`super::msg_trigger`]).
pub(super) struct Collect {
    wire: Wire,
    pub(super) columns: Vec<String>,
    /// The columns' MySQL types (`LATERAL`'s gateway apply inlines numbers
    /// as numbers).
    pub(super) types: Vec<u8>,
    pub(super) rows: Vec<Vec<Option<Vec<u8>>>>,
    pub(super) affected: Option<u64>,
    pub(super) error: Option<SprigError>,
    done: bool,
}

impl Collect {
    pub(super) fn new(wire: Wire) -> Self {
        Self {
            wire,
            columns: Vec::new(),
            types: Vec::new(),
            rows: Vec::new(),
            affected: None,
            error: None,
            done: false,
        }
    }
}

#[async_trait]
impl ResultSink for Collect {
    fn wire(&self) -> Wire {
        self.wire
    }
    async fn begin_rows(&mut self, columns: &[ColumnDef], _format: RowFormat) -> Result<()> {
        self.columns = columns
            .iter()
            .map(|c| String::from_utf8_lossy(&c.name).into_owned())
            .collect();
        self.types = columns.iter().map(|c| c.col_type).collect();
        Ok(())
    }
    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        match values {
            RowValues::Text(v) => self
                .rows
                .push(v.iter().map(|c| c.map(<[u8]>::to_vec)).collect()),
            RowValues::Binary(_) => {
                return Err(SprigError::internal(
                    "a binary row in an internal statement",
                ));
            }
        }
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
            SprigCode::TypeUnsupported,
            "LOAD DATA LOCAL INFILE can't run inside an internal statement",
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

/// Accounts (by cluster) known to hold the offsets role.
fn granted() -> &'static Mutex<HashSet<(ClusterId, String)>> {
    static G: OnceLock<Mutex<HashSet<(ClusterId, String)>>> = OnceLock::new();
    G.get_or_init(|| Mutex::new(HashSet::new()))
}

fn arg_error() -> SprigError {
    SprigError::standard(
        1318,
        *b"42000",
        "Incorrect arguments to sprig.commit_offset",
    )
}

/// `[db.]name` of a stream, against the catalog's streams: a dotted name
/// is first a database and a stream (Kafka's `<db>.<stream>`), then a
/// stream of the current database.
pub(super) fn find_stream(
    streams: &StreamsState,
    db_of: &dyn Fn(&str) -> Option<sprig_common::ids::DbId>,
    current: Option<&str>,
    name: &str,
    lctn: bool,
) -> Option<(String, std::sync::Arc<StreamDef>)> {
    if let Some((db, s)) = name.split_once('.')
        && let Some(id) = db_of(db)
        && let Some(def) = streams.get(id, s, lctn)
    {
        return Some((db.to_owned(), std::sync::Arc::clone(def)));
    }
    let db = current?;
    let id = db_of(db)?;
    streams
        .get(id, name, lctn)
        .map(|d| (db.to_owned(), std::sync::Arc::clone(d)))
}

impl PassSession {
    /// Runs `sql` through this session (logical names, its transaction);
    /// `internal` allows `__sprig_` tables. The statement's own error is in
    /// the result, not returned.
    pub(super) async fn run_internal(
        &mut self,
        cx: &mut ExecCtx<'_>,
        sql: &str,
        internal: bool,
    ) -> Result<Collect> {
        let mut sink = Collect::new(Wire {
            persona: cx.proto.persona,
            caps: cx.proto.caps,
        });
        if internal && let Some(ts) = self.tenant.as_deref_mut() {
            ts.types.internal = true;
        }
        let text = Bytes::copy_from_slice(sql.as_bytes());
        let r = Box::pin(self.tenant_text(
            cx,
            &text,
            &QueryAttrs::default(),
            false,
            RowFormat::Text,
            &mut sink,
        ))
        .await;
        if internal && let Some(ts) = self.tenant.as_deref_mut() {
            ts.types.internal = false;
        }
        r?;
        Ok(sink)
    }

    /// As [`PassSession::run_internal`], with the statement's error
    /// returned.
    pub(super) async fn run_checked(
        &mut self,
        cx: &mut ExecCtx<'_>,
        sql: &str,
        internal: bool,
    ) -> Result<Collect> {
        let mut c = self.run_internal(cx, sql, internal).await?;
        match c.error.take() {
            Some(e) => Err(e),
            None => Ok(c),
        }
    }

    /// Whether the session is inside a transaction.
    pub(super) fn offsets_in_transaction(&self) -> bool {
        let s = self.status();
        s.contains(StatusFlags::IN_TRANS) || !s.contains(StatusFlags::AUTOCOMMIT)
    }

    /// The offset-commit step of [`PassSession::tenant_text`]: `None` when the
    /// statement isn't `CALL sprig.commit_offset(...)`.
    pub(super) async fn tenant_offsets(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        more: bool,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let Some(ts) = self.tenant.as_deref() else {
            return Ok(None);
        };
        if ts.types.internal
            || !text
                .windows(13)
                .any(|w| w.eq_ignore_ascii_case(b"commit_offset"))
        {
            return Ok(None);
        }
        let Ok(sql) = std::str::from_utf8(text) else {
            return Ok(None);
        };
        let Some(args) = offsets::parse_call(sql) else {
            return Ok(None);
        };
        let r = self.commit_offset_call(cx, &args).await;
        let mut status = self.status();
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        if let Err(e) = r {
            return Ok(Some(self.fail(out, &e).await?));
        }
        let mut ok = OkPacket::new(status);
        ok.affected_rows = 1;
        out.ok(ok).await?;
        Ok(Some(Step {
            errored: false,
            status: Some(status),
        }))
    }

    #[allow(clippy::too_many_lines)]
    async fn commit_offset_call(&mut self, cx: &mut ExecCtx<'_>, args: &[String]) -> Result<()> {
        if !(4..=5).contains(&args.len()) {
            return Err(arg_error());
        }
        let vals = {
            let c = self
                .run_checked(cx, &format!("SELECT {}", args.join(", ")), false)
                .await?;
            c.rows.into_iter().next().ok_or_else(arg_error)?
        };
        let text = |i: usize| -> Option<String> {
            vals.get(i)
                .cloned()
                .flatten()
                .map(|b| String::from_utf8_lossy(&b).into_owned())
        };
        let num = |i: usize| -> Result<u64> {
            text(i)
                .and_then(|s| s.trim().parse::<u64>().ok())
                .ok_or_else(arg_error)
        };
        let group = text(0).filter(|g| !g.is_empty()).ok_or_else(arg_error)?;
        if group.chars().count() > 255 {
            return Err(arg_error());
        }
        let stream = text(1).ok_or_else(arg_error)?;
        let partition = u32::try_from(num(2)?).map_err(|_| arg_error())?;
        let offset = num(3)?;
        let metadata = text(4);
        if metadata.as_ref().is_some_and(|m| m.chars().count() > 1024) {
            return Err(SprigError::standard(
                1406,
                *b"22001",
                "Data too long for column 'metadata': at most 1024 characters",
            ));
        }
        let current = self.current_logical();
        let ts = self
            .tenant
            .as_deref_mut()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let cat = match ts.catalog() {
            Some(c) => std::sync::Arc::clone(c),
            None => ts.refresh().await?,
        };
        let lctn = ts.lower_case_table_names();
        let streams = cat
            .ext
            .get::<StreamsState>(sprig_streams::MODULE)
            .cloned()
            .unwrap_or_default();
        let db_of = |n: &str| cat.database(&database_key(n, lctn)).map(|d| d.db_id);
        let (db_name, def) = find_stream(&streams, &db_of, current.as_deref(), &stream, lctn)
            .ok_or_else(|| {
                SprigError::new(SprigCode::StreamNotFound, format!("no stream {stream}"))
            })?;
        if def.partitions <= partition {
            return Err(SprigError::new(
                SprigCode::StreamNotFound,
                format!("stream {stream} has no partition {partition}"),
            ));
        }
        // Committing a position is reading the stream.
        {
            let grants = crate::tenant::authz::privileges(ts, &cat);
            let db_key = database_key(&db_name, lctn);
            let name_key = object_key(ObjectKind::Table, &def.name, lctn);
            if !grants.unrestricted()
                && !grants.allows(
                    Action::Select,
                    Target::Named {
                        db_key: &db_key,
                        name_key: &name_key,
                    },
                )
                && !grants.allows(Action::Select, Target::Database { db_key: &db_key })
            {
                return Err(SprigError::standard(
                    1142,
                    *b"42000",
                    format!(
                        "SELECT command denied to user '{}'@'{}' for table `{db_name}`.`{}`",
                        ts.principal.user, ts.principal.host, def.name
                    ),
                ));
            }
        }
        self.commit_offset_internal(
            cx,
            &Commit {
                group,
                instance: 0,
                stream: def.id.0,
                partition,
                offset,
                epoch: None,
                metadata,
                expect: None,
            },
        )
        .await
    }

    /// Commits `c` (see the module docs): inside the session's transaction
    /// when there is one, else at once.
    ///
    /// # Errors
    /// 39877 when `c.expect` doesn't hold; 39958 (see the module docs);
    /// the catalog's or the statement's error.
    pub(super) async fn commit_offset_internal(
        &mut self,
        cx: &mut ExecCtx<'_>,
        c: &Commit,
    ) -> Result<()> {
        let in_txn = self.offsets_in_transaction();
        let (svc, tenant, user) = {
            let ts = self
                .tenant
                .as_deref()
                .ok_or_else(|| SprigError::internal("no tenant"))?;
            (
                std::sync::Arc::clone(ts.services()),
                ts.tenant,
                ts.principal.backend.user.clone(),
            )
        };
        let schema = SchemaName::catalog(tenant).to_string();
        let catalog_cluster = svc.locator.cluster(tenant).await?;
        let call = offsets::call_sql(&schema, c);
        if !in_txn {
            let ep = svc.locator.writer(tenant).await?;
            let mut conn = ep
                .pool
                .get_conn()
                .await
                .map_err(sprig_streams::maria::map_err)?;
            if let Err(e) = conn.query_drop(&call).await {
                let missing = matches!(&e, mysql_async::Error::Server(s) if s.code == 1305);
                if !missing {
                    return Err(moved_or(sprig_streams::maria::map_err(e)));
                }
                conn.query_drop(offsets::procedure_for(&schema, c))
                    .await
                    .map_err(sprig_streams::maria::map_err)?;
                conn.query_drop(&call)
                    .await
                    .map_err(|e| moved_or(sprig_streams::maria::map_err(e)))?;
            }
            return Ok(());
        }
        if catalog_cluster != self.own_cluster() {
            return Err(SprigError::new(
                SprigCode::StreamOffsetTxnUnsupported,
                format!(
                    "sprig.commit_offset inside a transaction needs the tenant's catalog on the \
                     transaction's cluster ({}), but it is on {catalog_cluster}",
                    self.own_cluster()
                ),
            ));
        }
        let key = (catalog_cluster.clone(), user.clone());
        let known = granted()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&key);
        if !known {
            self.grant_offsets(&catalog_cluster, &schema, tenant, &user)
                .await?;
        }
        let role = format!("SET ROLE `{}`", offsets::role(tenant));
        let wire = Wire {
            persona: cx.proto.persona,
            caps: cx.proto.caps,
        };
        for attempt in 0..2 {
            let mut sink = Collect::new(wire);
            Box::pin(self.forward_raw_own(cx, &role, &mut sink)).await?;
            if let Some(e) = sink.error {
                // 1959: the role isn't granted (anymore): grant it again.
                if attempt == 0 && sql_errno(&e) == 1959 {
                    self.grant_offsets(&catalog_cluster, &schema, tenant, &user)
                        .await?;
                    continue;
                }
                return Err(e);
            }
            let mut sink = Collect::new(wire);
            Box::pin(self.forward_raw_own(cx, &call, &mut sink)).await?;
            match sink.error {
                None => return Ok(()),
                // 1370: no EXECUTE; 1305: no procedure.
                Some(e) if attempt == 0 && matches!(sql_errno(&e), 1370 | 1305) => {
                    self.grant_offsets(&catalog_cluster, &schema, tenant, &user)
                        .await?;
                }
                Some(e) => return Err(moved_or(e)),
            }
        }
        Err(SprigError::internal(
            "sprig.commit_offset: the grant didn't take",
        ))
    }

    async fn grant_offsets(
        &self,
        cluster: &ClusterId,
        schema: &str,
        tenant: sprig_common::ids::TenantId,
        user: &str,
    ) -> Result<()> {
        let ts = self
            .tenant
            .as_deref()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let pool = ts.services().locator.ctl_pool(cluster).await?;
        let mut conn = pool
            .get_conn()
            .await
            .map_err(sprig_streams::maria::map_err)?;
        for s in offsets::grant_sql(schema, tenant, user) {
            if let Err(e) = conn.query_drop(&s).await {
                // 1396 (role exists, raced) and 1304 are "already done".
                if !matches!(&e, mysql_async::Error::Server(x) if matches!(x.code, 1396 | 1304)) {
                    return Err(sprig_streams::maria::map_err(e));
                }
            }
        }
        granted()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert((cluster.clone(), user.to_owned()));
        Ok(())
    }
}

fn sql_errno(e: &SprigError) -> u16 {
    match e.kind {
        sprig_common::ErrorKind::Backend { errno, .. }
        | sprig_common::ErrorKind::Standard { errno, .. } => errno,
        sprig_common::ErrorKind::Sprig(c) => c.number(),
    }
}

/// The procedure's compare-and-set failure as 39877.
fn moved_or(e: SprigError) -> SprigError {
    if offsets::is_moved(&e) {
        SprigError::new(
            SprigCode::LogicTriggerPositionMoved,
            "the consumer group is no longer at this offset (another run committed it)",
        )
    } else {
        e
    }
}
