//! Triggers on streams and queues in a tenant session (chapter 41,
//! "Triggers on streams and queues"; R9, r9triggers).
//!
//! **Declaring.** `CREATE [OR REPLACE] TRIGGER g ON STREAM [db.]s` or `ON
//! QUEUE [db.]q`, `LANGUAGE javascript | lua | kirun AS ...`: the source
//! must exist and the deploying user needs `SELECT` on the stream (or
//! `SELECT` and `DELETE` on the queue's vhost) and `CREATE ROUTINE` on the
//! trigger's database. [`sprig_logic::messages::deploy`] records it; then
//! a new stream trigger's consumer group starts at each partition's high
//! watermark (records published from now on), and the index entry that
//! makes the controller start its job is written.
//!
//! **Running.** A worker sends each run to a gateway over the peer service
//! ([`crate::triggers`]), which opens a session as the trigger's definer
//! and runs one marked statement in it, `/* sprig message trigger <nonce>
//! */ <RunRequest as JSON>`. The nonce is this process's own, so no client
//! can send the statement. [`PassSession::tenant_msg_trigger`] answers it
//! with one row, the [`RunReply`] as JSON:
//!
//! ```text
//! stream record r of partition p at offset o:
//!   START TRANSACTION
//!   commit_offset(__sprig_logic_<b>, s, p, o + 1, expect o)  -- 39877: Moved
//!   the version with ctx.record = r (its db calls in this transaction)
//!   COMMIT                                       -- any failure: ROLLBACK
//! queue q:
//!   mq_get(q, 1, lease) as the definer           -- none: Empty
//!   START TRANSACTION; hold the delivery; DELETE its row (mq_ack's path)
//!   the version with ctx.message
//!   COMMIT; the owner settles it
//!   failure: ROLLBACK; the delivery returns as a failed one (its delivery
//!   count rises), or, at logic.trigger_max_attempts failures, a
//!   logic_dead_letters row and a reject (the queue's dead-letter exchange
//!   gets it when there is one)
//! ```

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use mysql_async::prelude::Queryable;
use serde_json::{Value, json};
use sprig_catalog::CatalogLocator;
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::ObjectKind;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic::declare::{Declaration, SourceKind};
use sprig_logic::engine::bridge::DbOp;
use sprig_logic::engine::kirun::Host;
use sprig_logic::engine::outbound::HttpOutbound;
use sprig_logic::messages::{self, MsgIndex, RunOutcome, RunReply, RunRequest};
use sprig_proto_mysql::StatusFlags;
use sprig_streams::StreamsState;
use sprig_streams::offsets::{self, Commit};
use tokio::sync::{mpsc, oneshot};

use super::PassSession;
use super::logic::{ChannelDb, DbRequest};
use super::offsets::find_stream;
use super::tenant::Step;
use crate::pipeline::{ExecCtx, ResultSink, RowFormat, Wire};

/// The marked statement's start.
pub(crate) const MARK: &str = "/* sprig message trigger ";

/// This process's nonce: only statements carrying it are runs.
#[must_use]
pub(crate) fn nonce() -> &'static str {
    static N: OnceLock<String> = OnceLock::new();
    N.get_or_init(|| {
        format!(
            "{}{}",
            uuid::Uuid::now_v7().simple(),
            uuid::Uuid::now_v7().simple()
        )
    })
}

/// The marked statement of `req`.
///
/// # Errors
/// Internal when `req` doesn't serialize.
pub(crate) fn statement(req: &RunRequest) -> Result<String> {
    let j = serde_json::to_string(req)
        .map_err(|e| SprigError::internal(format!("a trigger run: {e}")))?;
    Ok(format!("{MARK}{} */ {j}", nonce()))
}

fn denied(ts: &crate::tenant::session::TenantSession, what: &str, db: &str, t: &str) -> SprigError {
    SprigError::standard(
        1142,
        *b"42000",
        format!(
            "{what} command denied to user '{}'@'{}' for table `{db}`.`{t}`",
            ts.principal.user, ts.principal.host
        ),
    )
}

impl PassSession {
    /// `CREATE TRIGGER ... ON STREAM | ON QUEUE` (see the module docs).
    #[allow(clippy::too_many_lines)]
    pub(super) async fn logic_deploy_message(
        &mut self,
        cx: &mut ExecCtx<'_>,
        decl: Declaration,
    ) -> Result<Vec<String>> {
        let _ = cx;
        let src = decl
            .source
            .clone()
            .ok_or_else(|| SprigError::internal("no source"))?;
        let current = self.current_logical();
        let ts = self
            .tenant
            .as_deref_mut()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let db = decl
            .db
            .clone()
            .or(current)
            .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))?;
        let cat = match ts.catalog() {
            Some(c) => Arc::clone(c),
            None => ts.refresh().await?,
        };
        let lctn = ts.lower_case_table_names();
        let row = cat
            .database(&database_key(&db, lctn))
            .cloned()
            .ok_or_else(|| {
                SprigError::standard(1049, *b"42000", format!("Unknown database '{db}'"))
            })?;
        let user = ts
            .principal
            .user_id
            .ok_or_else(|| SprigError::internal("a logic deploy without a user"))?;
        let tenant_name = ts.principal.tenant_name.clone().unwrap_or_default();
        let grants = crate::tenant::authz::privileges(ts, &cat);
        let allows = |a: sprig_auth::authz::Action, db_name: &str, name: &str| {
            let db_key = database_key(db_name, lctn);
            let name_key = object_key(ObjectKind::Table, name, lctn);
            grants.unrestricted()
                || grants.allows(
                    a,
                    sprig_auth::authz::Target::Named {
                        db_key: &db_key,
                        name_key: &name_key,
                    },
                )
                || grants.allows(a, sprig_auth::authz::Target::Database { db_key: &db_key })
        };
        // Code: CREATE ROUTINE on the trigger's database (chapter 29).
        {
            let db_key = database_key(&db, lctn);
            if !grants.unrestricted()
                && !grants.allows(
                    sprig_auth::authz::Action::CreateRoutine,
                    sprig_auth::authz::Target::Database { db_key: &db_key },
                )
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
        }
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        let ep = svc.locator.writer(tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
        })?;
        let source_db = src.db.clone().unwrap_or_else(|| db.clone());
        // R9 (r9next): `ON STREAM s FAN IN` reads every member instance of
        // the inherited stream `s` below this tenant.
        let mut fanin_def = None;
        let (source_db_row, object_id, stream_def) = match src.kind {
            SourceKind::Stream if src.fan_in => {
                if !grants.unrestricted()
                    && !grants.allows(
                        sprig_auth::authz::Action::FanIn,
                        sprig_auth::authz::Target::Tenant,
                    )
                {
                    return Err(SprigError::standard(
                        1227,
                        *b"42000",
                        "Access denied; you need (at least one of) the SPRIG_FAN_IN privilege(s) \
                         for this operation",
                    ));
                }
                let chain = crate::inherited::chain(&svc, &cat).await?;
                let own = crate::inherited::state_of(&cat);
                let def = crate::inherited::fanin::definition(&chain, &own, &source_db, &src.name)
                    .filter(|d| d.state == sprig_inherit::msg::DefState::Active)
                    .ok_or_else(|| {
                        crate::inherited::target_err(format!(
                            "no inherited stream {source_db}.{} to fan in",
                            src.name
                        ))
                    })?;
                if !allows(sprig_auth::authz::Action::Select, &def.db_name, &def.name) {
                    return Err(denied(ts, "SELECT", &def.db_name, &def.name));
                }
                let id = messages::fanin_source_id(def.object_id)
                    .ok_or_else(|| SprigError::internal("a definition id too large for fan-in"))?;
                let r = cat
                    .database(&database_key(&def.db_name, lctn))
                    .cloned()
                    .unwrap_or_else(|| row.clone());
                fanin_def = Some(def);
                (r, id, None)
            }
            SourceKind::Stream => {
                let streams = cat
                    .ext
                    .get::<StreamsState>(sprig_streams::MODULE)
                    .cloned()
                    .unwrap_or_default();
                let db_of = |n: &str| cat.database(&database_key(n, lctn)).map(|d| d.db_id);
                let name = match &src.db {
                    Some(d) => format!("{d}.{}", src.name),
                    None => src.name.clone(),
                };
                let (sdb, def) =
                    find_stream(&streams, &db_of, Some(&db), &name, lctn).ok_or_else(|| {
                        SprigError::new(
                            SprigCode::StreamNotFound,
                            format!("no stream {source_db}.{}", src.name),
                        )
                    })?;
                if !allows(sprig_auth::authz::Action::Select, &sdb, &def.name) {
                    return Err(denied(ts, "SELECT", &sdb, &def.name));
                }
                // Chapter 29 (RL36, RL44): a change topic carries its table's
                // rows, so its table's SELECT decides, not the name's; when
                // the table's policies bind the definer, each run gets only
                // the records they show.
                crate::tenant::rls_streams::change_topic_access(
                    grants.as_ref(),
                    &cat,
                    &def,
                    lctn,
                    &ts.principal.user,
                )?;
                let r = cat
                    .database(&database_key(&sdb, lctn))
                    .cloned()
                    .ok_or_else(|| SprigError::internal("the stream's database"))?;
                (r, def.id.0, Some(def))
            }
            SourceKind::Queue => {
                let r = cat
                    .database(&database_key(&source_db, lctn))
                    .cloned()
                    .ok_or_else(|| {
                        SprigError::standard(
                            1049,
                            *b"42000",
                            format!("Unknown database '{source_db}'"),
                        )
                    })?;
                if !allows(sprig_auth::authz::Action::Select, &source_db, &src.name)
                    || !allows(sprig_auth::authz::Action::Delete, &source_db, &src.name)
                {
                    return Err(denied(ts, "SELECT and DELETE", &source_db, &src.name));
                }
                let id: Option<u64> = conn
                    .exec_first(
                        format!(
                            "SELECT object_id FROM {}.mq_queues WHERE db_id = ? AND name = ? \
                             AND state = 'active'",
                            sprig_common::names::quote(ep.store.schema())
                        ),
                        (r.db_id.0, src.name.as_bytes()),
                    )
                    .await
                    .map_err(|e| match &e {
                        mysql_async::Error::Server(s) if s.code == 1146 => SprigError::new(
                            SprigCode::LogicNotFound,
                            format!("no queue '{}' in vhost {source_db}", src.name),
                        ),
                        _ => SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable")
                            .with_source(e),
                    })?;
                let id = id.ok_or_else(|| {
                    SprigError::new(
                        SprigCode::LogicNotFound,
                        format!(
                            "no durable queue '{}' in vhost {source_db} (triggers read durable queues)",
                            src.name
                        ),
                    )
                })?;
                (r, id, None)
            }
        };
        let deployed = sprig_logic::messages::deploy(
            &decl,
            sprig_logic::messages::MsgTarget {
                catalog: &mut conn,
                catalog_schema: ep.store.schema(),
                catalog_version: cat.version,
                db_id: row.db_id.0,
                user_id: user.0,
                tenant_name: &tenant_name,
                source_db: (source_db_row.db_id.0, &source_db_row.name),
                object_id,
                fanin_declarer: fanin_def.as_ref().map_or(0, |d| d.declarer.0),
            },
        )
        .await?;
        let mut notes = deployed.notes();
        // A new stream trigger reads what is published from now on.
        if let Some(def) = &stream_def {
            let group = offsets::logic_group(deployed.binding_id);
            let have = offsets::positions(&mut conn, ep.store.schema(), &group, def.id.0).await?;
            if have.len() < def.partitions as usize {
                let mut start = Vec::new();
                if let Some(log) = def.log.first() {
                    let schema = log.slot.schema(tenant, def.db).to_string();
                    let cluster = svc.locator.cluster(tenant).await?;
                    let pool = svc.locator.ctl_pool(&cluster).await?;
                    let mut c = pool
                        .get_conn()
                        .await
                        .map_err(sprig_streams::maria::map_err)?;
                    let reader = sprig_streams::read::LogReader { schema };
                    for p in 0..def.partitions {
                        let hw = reader
                            .bounds(&mut c, def.id, sprig_streams::PartitionNo(p))
                            .await
                            .map_or(0, |b| b.high_watermark.0);
                        start.push((p, hw));
                    }
                } else {
                    start = (0..def.partitions).map(|p| (p, 0)).collect();
                }
                offsets::init_positions(&mut conn, ep.store.schema(), &group, def.id.0, &start)
                    .await?;
                notes.push(format!(
                    "consumer group {group} starts at the high watermark of each of {} partitions",
                    def.partitions
                ));
            }
        }
        if let Some(def) = &fanin_def {
            let n = crate::inherited::fanin_triggers::attach_existing(
                &svc,
                tenant,
                deployed.binding_id,
                def,
            )
            .await?;
            notes.push(format!(
                "fan-in over {n} member instance{} of {}.{} (each from its high watermark; \
                 instances made later from their start)",
                if n == 1 { "" } else { "s" },
                def.db_name,
                def.name
            ));
        }
        messages::put_index(
            svc.locator.dc_store().as_ref(),
            &MsgIndex {
                tenant,
                binding: deployed.binding_id,
                kind: src.kind,
            },
        )
        .await?;
        tracing::info!(%tenant, binding = deployed.binding_id, kind = src.kind.as_str(), name = %decl.name, "trigger on a stream or queue deployed");
        Ok(notes)
    }

    /// The marked statement of a run (see the module docs): `None` for any
    /// other statement.
    pub(super) async fn tenant_msg_trigger(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        more: bool,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        if !text.starts_with(MARK.as_bytes()) || self.tenant.is_none() {
            return Ok(None);
        }
        let s = String::from_utf8_lossy(&text[MARK.len()..]).into_owned();
        let parsed = s
            .split_once(" */ ")
            .filter(|(n, _)| *n == nonce())
            .and_then(|(_, j)| serde_json::from_str::<RunRequest>(j).ok());
        let Some(req) = parsed else {
            let e = SprigError::standard(
                1064,
                *b"42000",
                "You have an error in your SQL syntax: a trigger run isn't a client statement",
            );
            return Ok(Some(self.fail(out, &e).await?));
        };
        let guard = self.change_topic_guard(&req).await;
        let reply = if let Err(r) = guard {
            // Chapter 29 (RL36): the definer may no longer read the change
            // topic's table (a grant change since deploy): the trigger
            // waits, its records neither run nor dead-letter.
            r
        } else if let Some(rp) = &req.replay {
            // RC11: a dead letter's replay.
            self.run_replay(cx, &req, rp).await
        } else if req.record.is_some() {
            let filter = guard.ok().flatten();
            self.run_stream_trigger(cx, &req, filter.as_deref()).await
        } else {
            self.run_queue_trigger(cx, &req).await
        };
        // Chapter 42 (R9): an inherited trigger lacking a privilege waits
        // for the grant (39242) instead of failing its records.
        let reply = match self.tenant.as_deref() {
            Some(ts) => {
                crate::inherited::triggers::on_run(
                    ts.services(),
                    ts.tenant,
                    &ts.principal.user,
                    req.source.binding_id,
                    reply,
                )
                .await
            }
            None => reply,
        };
        if !matches!(
            reply.outcome,
            RunOutcome::Ok | RunOutcome::Empty | RunOutcome::Moved
        ) {
            tracing::debug!(binding = req.source.binding_id, code = reply.code, message = %reply.message, "trigger run failed");
        }
        let body = serde_json::to_string(&reply).unwrap_or_default();
        let mut status = self.status();
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        let rows = sprig_types::Rows {
            columns: vec![crate::pipeline::text_column("reply", cx.proto.collation)],
            rows: vec![vec![Some(body.into_bytes())]],
        };
        super::types::send_rows(out, &rows, RowFormat::Text, status).await?;
        Ok(Some(Step {
            errored: false,
            status: Some(status),
        }))
    }

    /// Chapter 29 (RL36, RL44): a trigger on a change topic runs while its
    /// definer (this session) may `SELECT` the topic's table; otherwise a
    /// transient failure (`Err`), so the runner retries with backoff and
    /// the record is neither run nor dead-lettered. `Ok(Some(topic))`
    /// (`` `db`.`t.changes` ``) when the table's policies bind the definer:
    /// the run reads its record through the topic's filter.
    async fn change_topic_guard(
        &mut self,
        req: &RunRequest,
    ) -> std::result::Result<Option<String>, RunReply> {
        if req.source.kind != SourceKind::Stream || req.instance_tenant != 0 {
            return Ok(None);
        }
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Ok(None);
        };
        let cat = match ts.refresh().await {
            Ok(c) => c,
            Err(e) => {
                let mut r = RunReply::failed(&e);
                r.transient = true;
                return Err(r);
            }
        };
        let Some(def) = cat
            .ext
            .get::<StreamsState>(sprig_streams::MODULE)
            .and_then(|st| st.all().find(|s| s.id.0 == req.source.object_id).cloned())
        else {
            return Ok(None);
        };
        let grants = crate::tenant::authz::privileges(ts, &cat);
        let lctn = ts.lower_case_table_names();
        match crate::tenant::rls_streams::change_topic_access(
            grants.as_ref(),
            &cat,
            &def,
            lctn,
            &ts.principal.user,
        ) {
            Ok(crate::tenant::rls_streams::TopicAccess::All) => Ok(None),
            Ok(crate::tenant::rls_streams::TopicAccess::Filtered) => {
                let db = cat
                    .databases
                    .get(def.db)
                    .map(|d| d.name.clone())
                    .unwrap_or_default();
                Ok(Some(format!(
                    "{}.{}",
                    sprig_common::names::quote(&db),
                    sprig_common::names::quote(&def.name)
                )))
            }
            Err(e) => {
                tracing::warn!(binding = req.source.binding_id, stream = %def.name, error = %e, "trigger on a change topic waits: its definer may not read the table");
                let mut r = RunReply::failed(&e);
                r.transient = true;
                Err(r)
            }
        }
    }

    /// Chapter 29 (RL44): record `r` of change topic `topic` as this
    /// session (the definer) may see it: its value through the topic's
    /// filter, `None` when the definer's policies hide it.
    async fn filtered_value(
        &mut self,
        cx: &mut ExecCtx<'_>,
        topic: &str,
        r: &messages::RecordIn,
    ) -> Result<Option<Option<Vec<u8>>>> {
        let sql = format!(
            "SELECT `value` FROM {topic} WHERE partition_no = {} AND offset_no = {}",
            r.partition, r.offset
        );
        let mut c = self.run_checked(cx, &sql, false).await?;
        Ok(c.rows.pop().map(|mut row| row.pop().flatten()))
    }

    /// One statement through this session; its error returned.
    async fn trigger_stmt(&mut self, cx: &mut ExecCtx<'_>, sql: &str) -> Result<()> {
        self.run_checked(cx, sql, false).await.map(|_| ())
    }

    async fn rollback_quietly(&mut self, cx: &mut ExecCtx<'_>) {
        if let Err(e) = self.trigger_stmt(cx, "ROLLBACK").await {
            tracing::debug!(error = %e, "trigger run: ROLLBACK failed");
        }
    }

    /// Loads the version and runs it in this session with `event` as
    /// `ctx.record` or `ctx.message`: the version's `db` calls go through
    /// this session, in its transaction.
    #[allow(clippy::too_many_lines)]
    async fn run_version(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: &RunRequest,
        field: &str,
        event: Value,
    ) -> Result<Vec<String>> {
        let (svc, tenant, cat, user) = {
            let ts = self
                .tenant
                .as_deref_mut()
                .ok_or_else(|| SprigError::internal("no tenant"))?;
            let cat = match ts.catalog() {
                Some(c) => Arc::clone(c),
                None => ts.refresh().await?,
            };
            (
                Arc::clone(ts.services()),
                ts.tenant,
                cat,
                ts.principal.user.clone(),
            )
        };
        let v = if let Some(v) =
            sprig_logic::invoke::cached(tenant.0, req.source.module_id, req.version)
        {
            v
        } else {
            let ep = svc.locator.writer(tenant).await?;
            let mut conn = ep.pool.get_conn().await.map_err(|e| {
                SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
            })?;
            sprig_logic::invoke::load(
                &mut conn,
                ep.store.schema(),
                tenant.0,
                req.source.module_id,
                req.version,
            )
            .await?
        };
        let program = v.program.as_ref().ok_or_else(|| {
            SprigError::new(
                SprigCode::LogicEngineUnavailable,
                format!("{}: the {} runtime isn't built", v.name, v.runtime),
            )
        })?;
        let db_row = cat.databases.get(sprig_common::ids::DbId(v.db_id)).cloned();
        let mut sctx = cat.setting_ctx(&svc.dc);
        if let Some(r) = &db_row {
            sctx = sctx.with_db(r.db_id);
        }
        let (mut limits, policy) =
            sprig_logic::settings::limits(&svc.settings, &sctx, Some(&cat.settings));
        limits.wall = Duration::from_millis(req.budget_ms.max(1));
        limits.wall_setting = "logic.trigger_budget_ms";
        let mut ctx = json!({
            "user": user,
            "tenant": tenant.0,
            "database": db_row.as_ref().map(|r| r.name.clone()),
            "function": v.name.clone(),
            "version": v.version,
            "trigger": {"binding": req.source.binding_id, "kind": req.source.kind.as_str()},
        });
        ctx[field] = event.clone();
        let params: Vec<String> = match program {
            sprig_logic::engine::Program::Kirun(p) => p.params().to_vec(),
            _ => Vec::new(),
        };
        let args = sprig_logic::messages::args_for(&v.runtime, &params, &event, &ctx);
        let wire = Wire {
            persona: self.persona,
            caps: cx.proto.caps,
        };
        let (tx, mut rx) = mpsc::channel::<DbRequest>(1);
        let host = Host {
            db: Arc::new(ChannelDb(tx)),
            outbound: Arc::new(HttpOutbound::new(policy)),
            default_db: db_row.map(|r| r.name.clone()),
            info: ctx,
        };
        let run = program.run(args, host, limits);
        tokio::pin!(run);
        // Chapter 42 (R9): a statement the definer may not run fails the
        // run with that error, whatever the code did with it, so an
        // inherited trigger can wait for the grant (39242).
        let mut denied: Option<SprigError> = None;
        let outcome = loop {
            tokio::select! {
                biased;
                r = &mut run => break r,
                Some((op, reply)) = rx.recv() => {
                    let op: DbOp = op;
                    let r: oneshot::Sender<Result<sprig_logic::engine::bridge::Reply>> = reply;
                    let x = self.engine_db(cx, op, wire).await;
                    if let Err(e) = &x
                        && crate::inherited::triggers::is_privilege_error(e)
                    {
                        denied.get_or_insert_with(|| SprigError {
                            kind: e.kind,
                            message: e.message.clone(),
                            retryable: e.retryable,
                            source: None,
                        });
                    }
                    let _ = r.send(x);
                }
            }
        };
        if let Some(e) = denied {
            return Err(e);
        }
        let outcome = outcome?;
        // An `error` or `reject` event fails the run, as for after-commit
        // triggers (39865).
        if matches!(outcome.event.as_str(), "error" | "reject") {
            let msg = outcome
                .result
                .get("message")
                .and_then(Value::as_str)
                .map_or_else(|| outcome.result.to_string(), str::to_owned);
            return Err(SprigError::new(
                SprigCode::LogicUserError,
                format!("{}: {msg}", outcome.event),
            ));
        }
        Ok(Vec::new())
    }

    /// A dead letter's replay (RC11): its event with the active version, in
    /// a transaction that moves its marker in the replay group from 0 to 1
    /// (39877 when a repeat finds it moved: `Moved`), outside the trigger's
    /// own position.
    async fn run_replay(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: &RunRequest,
        rp: &messages::ReplayRun,
    ) -> RunReply {
        if let Err(e) = self.trigger_stmt(cx, "START TRANSACTION").await {
            return RunReply::failed(&e);
        }
        let c = Commit {
            group: messages::replay_group(req.source.binding_id),
            instance: 0,
            stream: rp.dl_id,
            partition: 0,
            offset: 1,
            epoch: None,
            metadata: None,
            expect: Some(0),
        };
        if let Err(e) = self.commit_offset_internal(cx, &c).await {
            self.rollback_quietly(cx).await;
            if e.code() == Some(SprigCode::LogicTriggerPositionMoved) {
                return RunReply::of(RunOutcome::Moved);
            }
            return RunReply::failed(&e);
        }
        let arg = match req.source.kind {
            SourceKind::Stream => "record",
            SourceKind::Queue => "message",
        };
        match self.run_version(cx, req, arg, rp.event.clone()).await {
            Ok(logs) => match self.trigger_stmt(cx, "COMMIT").await {
                Ok(()) => RunReply {
                    logs,
                    ..RunReply::of(RunOutcome::Ok)
                },
                Err(e) => {
                    self.rollback_quietly(cx).await;
                    RunReply::failed(&e)
                }
            },
            Err(e) => {
                self.rollback_quietly(cx).await;
                RunReply::failed(&e)
            }
        }
    }

    /// One record of a stream (see the module docs).
    async fn run_stream_trigger(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: &RunRequest,
        filter: Option<&str>,
    ) -> RunReply {
        let Some(r) = &req.record else {
            return RunReply::failed(&SprigError::internal("a stream run without a record"));
        };
        let stream_name = format!("{}.{}", req.source.db_name, req.source.name);
        // Chapter 29 (RL44): a change record the definer's policies hide is
        // passed over (its offset committed, the code not run); one whose
        // row became visible or hidden reads as an insert or a delete.
        let mut shown = None;
        if let Some(topic) = filter {
            match self.filtered_value(cx, topic, r).await {
                Ok(Some(v)) => {
                    let mut x = r.clone();
                    x.value = v;
                    shown = Some(x);
                }
                Ok(None) => {
                    metrics::counter!("sprig_rls_feed_events_filtered_total", "protocol" => "trigger")
                        .increment(1);
                }
                Err(e) => {
                    let mut f = RunReply::failed(&e);
                    f.transient = true;
                    return f;
                }
            }
        }
        let hidden = filter.is_some() && shown.is_none();
        let event = messages::record_value(&stream_name, shown.as_ref().unwrap_or(r));
        if let Err(e) = self.trigger_stmt(cx, "START TRANSACTION").await {
            return RunReply::failed(&e);
        }
        let c = Commit {
            group: offsets::logic_group(req.source.binding_id),
            // A fan-in trigger's position is on the member's instance (R9).
            instance: req.instance_tenant,
            stream: if req.instance_tenant == 0 {
                req.source.object_id
            } else {
                req.instance_stream
            },
            partition: r.partition,
            offset: r.offset + 1,
            epoch: None,
            metadata: None,
            expect: Some(r.offset),
        };
        if let Err(e) = self.commit_offset_internal(cx, &c).await {
            self.rollback_quietly(cx).await;
            if e.code() == Some(SprigCode::LogicTriggerPositionMoved) {
                return RunReply::of(RunOutcome::Moved);
            }
            return RunReply::failed(&e);
        }
        if hidden {
            return match self.trigger_stmt(cx, "COMMIT").await {
                Ok(()) => RunReply::of(RunOutcome::Ok),
                Err(e) => {
                    self.rollback_quietly(cx).await;
                    RunReply::failed(&e)
                }
            };
        }
        match self.run_version(cx, req, "record", event).await {
            Ok(logs) => match self.trigger_stmt(cx, "COMMIT").await {
                Ok(()) => RunReply {
                    logs,
                    ..RunReply::of(RunOutcome::Ok)
                },
                Err(e) => {
                    self.rollback_quietly(cx).await;
                    RunReply::failed(&e)
                }
            },
            Err(e) => {
                self.rollback_quietly(cx).await;
                RunReply::failed(&e)
            }
        }
    }

    /// The next message of a queue (see the module docs).
    #[allow(clippy::too_many_lines)]
    async fn run_queue_trigger(&mut self, cx: &mut ExecCtx<'_>, req: &RunRequest) -> RunReply {
        let (engine, principal, tenant) = {
            let Some(ts) = self.tenant.as_deref() else {
                return RunReply::failed(&SprigError::internal("no tenant"));
            };
            let Some(engine) = ts.services().mq.get().cloned() else {
                let mut e = SprigError::new(
                    SprigCode::LogicEngineUnavailable,
                    "the queue engine isn't running on this gateway",
                );
                e.retryable = true;
                return RunReply::failed(&e);
            };
            let p = &ts.principal;
            let principal = Arc::new(sprig_proto_amqp::auth::Principal {
                tenant_id: ts.tenant.0,
                tenant: p.tenant_name.clone().unwrap_or_default(),
                user: p.user.clone(),
                user_id: p.user_id.map(|u| u64::from(u.0)),
                handle: Some(Arc::new(p.clone())),
            });
            (engine, principal, ts.tenant)
        };
        let mq_err = |e: &sprig_proto_amqp::error::MqError| {
            let mut s = SprigError::standard(
                u16::try_from(e.code.number()).unwrap_or(39250),
                *b"HY000",
                format!("{}: {}", e.code.name(), e.text),
            );
            s.retryable = e.code == sprig_proto_amqp::error::MqCode::OwnerMoved;
            s
        };
        let session = u64::from(cx.session.id.0);
        // Opening the vhost or taking a message isn't the code's failure:
        // transient (the job ends by itself when the source is gone).
        let infra = |e: &sprig_proto_amqp::error::MqError| {
            let mut r = RunReply::failed(&mq_err(e));
            r.transient = true;
            r
        };
        let v = match engine
            .sql_vhost(principal, &req.source.db_name, session)
            .await
        {
            Ok(v) => v,
            Err(e) => return infra(&e),
        };
        // The trigger reads the queue of its own database: one with the
        // name in a database made since is another queue.
        if v.vhost().db_id != req.source.db_id {
            return RunReply::of(RunOutcome::Empty);
        }
        let ds = match v.get(&req.source.name, 1, req.lease_ms.max(1000)).await {
            Ok(d) => d,
            Err(e) => return infra(&e),
        };
        let Some(d) = ds.into_iter().next() else {
            return RunReply::of(RunOutcome::Empty);
        };
        let token = d
            .annotations
            .get("x-sprig-delivery")
            .and_then(|t| t.as_str().map(std::borrow::Cow::into_owned))
            .unwrap_or_default();
        let t = match engine.verify_token(&token) {
            Ok(t) => t,
            Err(e) => return RunReply::failed(&mq_err(&e)),
        };
        let attempts = d.delivery_count;
        let message_id = t.msg_id.iter().fold(String::new(), |mut acc, b| {
            use std::fmt::Write as _;
            let _ = write!(acc, "{b:02x}");
            acc
        });
        let props: Value =
            serde_json::from_str(&sprig_queues::sql::properties_json(&d)).unwrap_or(json!({}));
        let body = sprig_queues::sql::body_bytes(&d);
        let event = json!({
            "queue": req.source.name,
            "vhost": req.source.db_name,
            "exchange": d.message.exchange,
            "routingKey": d.message.routing_key,
            "body": String::from_utf8_lossy(&body),
            "properties": props,
            "redelivered": d.redelivered,
            "deliveryCount": d.delivery_count,
        });
        let db = v.db().to_owned();
        let with_id = |mut r: RunReply| {
            r.message_id = Some(message_id.clone());
            r.attempts = attempts;
            r
        };
        let failure = if let Err(e) = self.trigger_stmt(cx, "START TRANSACTION").await {
            e
        } else {
            {
                // mq_ack's path: hold, then the row's delete in this
                // transaction.
                if let Err(e) = v.hold(&t).await {
                    self.rollback_quietly(cx).await;
                    return with_id(RunReply::of(RunOutcome::Moved)).with_note(&mq_err(&e));
                }
                let deleted = match self
                    .run_checked(cx, &sprig_queues::sql::ack_sql(&db, &t), true)
                    .await
                {
                    Ok(c) => c.affected.unwrap_or(0),
                    Err(e) => {
                        self.rollback_quietly(cx).await;
                        v.release(&t, false);
                        return with_id(RunReply::failed(&e));
                    }
                };
                if deleted == 0 {
                    self.rollback_quietly(cx).await;
                    v.release(&t, false);
                    return with_id(RunReply::of(RunOutcome::Moved));
                }
                match self.run_version(cx, req, "message", event.clone()).await {
                    Ok(logs) => match self.trigger_stmt(cx, "COMMIT").await {
                        Ok(()) => {
                            v.release(&t, true);
                            return with_id(RunReply {
                                logs,
                                ..RunReply::of(RunOutcome::Ok)
                            });
                        }
                        Err(e) => {
                            self.rollback_quietly(cx).await;
                            v.release(&t, false);
                            e
                        }
                    },
                    Err(e) => {
                        self.rollback_quietly(cx).await;
                        v.release(&t, false);
                        e
                    }
                }
            }
        };
        let mut reply = with_id(RunReply::failed(&failure));
        // Chapter 42 (R9): an inherited trigger whose sprig_inherited lost a
        // privilege waits for the grant (39242, marked by `on_run`): its
        // message goes back without counting, never to the dead letters.
        if self
            .tenant
            .as_deref()
            .is_some_and(|ts| ts.principal.user == crate::inherited::RUN_AS_USER)
            && crate::inherited::triggers::is_privilege_error(&failure)
        {
            reply.transient = true;
        }
        if reply.transient {
            // Not the code's fault: back as it was, without counting.
            v.nack(&t, true);
            return reply;
        }
        if attempts.saturating_add(1) < req.max_attempts.max(1) {
            v.fail(&t);
            return reply;
        }
        // The last attempt: a dead letter in the trigger's table first (so
        // nothing is lost when the queue has no dead-letter exchange), then
        // a reject, which the queue dead-letters through its exchange when
        // it has one.
        let written = async {
            let ep = {
                let ts = self
                    .tenant
                    .as_deref()
                    .ok_or_else(|| SprigError::internal("no tenant"))?;
                ts.services().locator.writer(tenant).await?
            };
            let mut conn = ep.pool.get_conn().await.map_err(|e| {
                SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
            })?;
            let store = sprig_logic::LogicStore::new(ep.store.schema());
            store
                .put_dead_letter(
                    &mut conn,
                    req.source.binding_id,
                    req.version,
                    &format!("m{message_id}"),
                    token.as_bytes(),
                    &event,
                    attempts.saturating_add(1),
                    reply.code,
                    &reply.message,
                )
                .await?;
            let _ = store
                .log(
                    &mut conn,
                    req.source.module_id,
                    Some(req.version),
                    "error",
                    "trigger",
                    &format!(
                        "queue {} message {message_id}: dead letter after {} attempts: {} {}",
                        req.source.name,
                        attempts.saturating_add(1),
                        reply.code,
                        reply.message
                    ),
                )
                .await;
            Ok::<(), SprigError>(())
        }
        .await;
        match written {
            Ok(()) => {
                v.nack(&t, false);
                reply.outcome = RunOutcome::Dead;
            }
            Err(e) => {
                // Try again later; the delivery count still says it's due.
                tracing::warn!(error = %e, binding = req.source.binding_id, "trigger dead letter not written");
                v.nack(&t, true);
            }
        }
        reply
    }
}

/// A note on a reply (the reason a run didn't happen).
trait WithNote {
    fn with_note(self, e: &SprigError) -> Self;
}

impl WithNote for RunReply {
    fn with_note(mut self, e: &SprigError) -> Self {
        self.message.clone_from(&e.message);
        self
    }
}
