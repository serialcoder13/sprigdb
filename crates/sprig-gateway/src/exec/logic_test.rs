//! `Logic.TestRun` in a tenant session (chapter 41, "Test runs"; R7
//! follow-ups): `sprig_logic::testrun` has the request, the checks and
//! the report; this runs it on this session.
//!
//! ```text
//! START TRANSACTION (SAVEPOINT __sprig_test_run inside the client's own)
//!   snapshot tables: CREATE TEMPORARY TABLE t LIKE t; INSERT the rows
//!   the version or draft on the engine tier; each db statement:
//!     classify → refused (39862, the code sees the error) or run through
//!     tenant_text (names, rights, row policies, this transaction), timed
//!     and recorded; db.call only of logic functions
//!   read the snapshot tables back
//! ROLLBACK (TO SAVEPOINT); DROP TEMPORARY TABLE ...
//! ```
//!
//! Calls nested inside the run (`db.call` of an engine-tier function) run
//! with [`TEST_RUN`] set: their statements meet the same refusals, they
//! send no outbound calls, and nothing is counted for rollouts.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use serde_json::{Value, json};
use sprig_catalog::CatalogLocator;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic::engine::bridge::{self as eb, DbBridge, DbOp, Reply};
use sprig_logic::engine::kirun::Host;
use sprig_logic::testrun::{self, MockOutbound, Recorder, Report, Statement, StmtClass, TestRun};
use sprig_proto_mysql::resultset::column_type as ct;
use sprig_proto_mysql::{ColumnDef, StatusFlags};
use tokio::sync::{mpsc, oneshot};

use super::PassSession;
use super::logic::{binary_value, out_column};
use super::tenant::Step;
use crate::pipeline::{EndOfRows, ExecCtx, ResultSink, RowFormat, RowValues, Wire};

tokio::task_local! {
    /// Set while a test run runs on this session's task: nested calls
    /// send no outbound calls and count nothing for rollouts.
    pub(super) static TEST_RUN: ();
}

/// Whether a test run is running on this task.
pub(super) fn in_test_run() -> bool {
    TEST_RUN.try_with(|()| ()).is_ok()
}

/// The savepoint inside a client's own transaction.
const SAVEPOINT: &str = "__sprig_test_run";

type Request = (DbOp, oneshot::Sender<Result<Reply>>);

/// The run's `db`: statements go to the session's loop; log lines to the
/// recorder.
struct TestDb {
    tx: mpsc::Sender<Request>,
    rec: Arc<Recorder>,
}

#[async_trait]
impl DbBridge for TestDb {
    async fn exec(&self, op: DbOp) -> Result<Reply> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send((op, tx))
            .await
            .map_err(|_| SprigError::internal("the test run's session went away"))?;
        rx.await
            .map_err(|_| SprigError::internal("the test run's session went away"))?
    }

    fn log(&self, line: &str) {
        self.rec.log(line);
    }
}

fn ms(since: Instant) -> f64 {
    (since.elapsed().as_secs_f64() * 1000.0 * 1000.0).round() / 1000.0
}

/// The names of a program's parameters, in call order.
fn param_names(p: &sprig_logic::engine::Program) -> Vec<String> {
    match p {
        sprig_logic::engine::Program::Kirun(k) => k.params().to_vec(),
        sprig_logic::engine::Program::Js(j) => {
            j.sig().params.iter().map(|(n, _)| n.clone()).collect()
        }
        sprig_logic::engine::Program::Lua(l) => {
            l.sig().params.iter().map(|(n, _)| n.clone()).collect()
        }
    }
}

fn denied(user: &str, db: &str) -> SprigError {
    SprigError::standard(
        1044,
        *b"42000",
        format!("Access denied for user '{user}' to database '{db}'"),
    )
}

/// What runs: the program and what the report says about it.
struct Subject {
    program: sprig_logic::engine::Program,
    name: String,
    kind: String,
    runtime: String,
    version: Option<u64>,
    deployed_tier: Option<String>,
}

impl PassSession {
    /// A test run (see the module docs): one row, `event`, `result` and
    /// `report`.
    pub(super) async fn tenant_test_run(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: Result<TestRun>,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        // As for `Functions.Call`: the code's statements never run in a
        // front end's internal mode.
        let internal = self
            .tenant
            .as_deref_mut()
            .is_some_and(|ts| std::mem::replace(&mut ts.types.internal, false));
        let r = match req {
            Ok(req) => Box::pin(TEST_RUN.scope((), self.test_run(cx, req))).await,
            Err(e) => Err(e),
        };
        if let Some(ts) = self.tenant.as_deref_mut() {
            ts.types.internal = internal;
        }
        let report = match r {
            Ok(r) => r,
            Err(e) => return self.fail(out, &e).await,
        };
        let charset = cx.proto.collation;
        let cols = [
            sprig_logic::invoke::OutCol {
                name: "event".into(),
                ty: sprig_logic::invoke::OutTy::Event,
            },
            sprig_logic::invoke::OutCol {
                name: "result".into(),
                ty: sprig_logic::invoke::OutTy::Json,
            },
            sprig_logic::invoke::OutCol {
                name: "report".into(),
                ty: sprig_logic::invoke::OutTy::Json,
            },
        ];
        let defs: Vec<ColumnDef> = cols
            .iter()
            .map(|c| {
                let d = out_column(c, charset);
                if c.ty == sprig_logic::invoke::OutTy::Json {
                    ColumnDef {
                        col_type: ct::JSON,
                        ..d
                    }
                } else {
                    d
                }
            })
            .collect();
        let row = [
            report["event"].as_str().map(str::to_owned),
            (!report["result"].is_null()).then(|| report["result"].to_string()),
            Some(report.to_string()),
        ];
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
                let vals: Vec<sprig_proto_mysql::Value> = row
                    .iter()
                    .map(|t| binary_value(t.as_ref(), sprig_logic::invoke::OutTy::Str))
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
        out.ok(sprig_proto_mysql::packets::OkPacket::new(fin))
            .await?;
        Ok(Step {
            errored: false,
            status: Some(fin),
        })
    }

    #[allow(clippy::too_many_lines)] // checks, setup, the run, the cleanup
    async fn test_run(&mut self, cx: &mut ExecCtx<'_>, req: TestRun) -> Result<Value> {
        let started = Instant::now();
        req.check()?;
        let kind = req.kind();
        // A draft names its own database when qualified.
        let draft = match &req.source {
            Some(src) => Some(testrun::draft(src, &kind)?),
            None => None,
        };
        let db = draft
            .as_ref()
            .and_then(|(d, _)| d.db.clone())
            .or_else(|| req.database.clone())
            .or_else(|| self.current_logical())
            .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))?;
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Err(SprigError::internal("no tenant"));
        };
        let cat = match ts.catalog() {
            Some(c) => Arc::clone(c),
            None => ts.refresh().await?,
        };
        let row = cat
            .database(&db)
            .or_else(|| cat.database(&db.to_ascii_lowercase()))
            .cloned()
            .ok_or_else(|| {
                SprigError::standard(1049, *b"42000", format!("Unknown database '{db}'"))
            })?;
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        let user = ts.principal.user.clone();
        // Chapter 29: a draft is code the caller could deploy (`CREATE
        // ROUTINE` or `ALTER ROUTINE`); a deployed version, code the
        // caller may run (`EXECUTE`) or administers.
        let (may_create, may_execute) = {
            use sprig_auth::authz::{Action, Target};
            let grants = crate::tenant::authz::privileges(ts, &cat);
            let db_key = sprig_catalog::ddl::database_key(&row.name, ts.lower_case_table_names());
            let on_db = |a| grants.allows(a, Target::Database { db_key: &db_key });
            let admin = grants.unrestricted()
                || on_db(Action::CreateRoutine)
                || on_db(Action::AlterRoutine);
            (admin, admin || on_db(Action::Execute))
        };
        let ep = svc.locator.writer(tenant).await?;
        let store = sprig_logic::LogicStore::new(ep.store.schema());
        let subject = if let Some((d, program)) = draft {
            if !may_create {
                return Err(denied(&user, &row.name));
            }
            Subject {
                program,
                name: d.name.clone(),
                kind: d.kind.as_str().to_owned(),
                runtime: d.runtime.as_str().to_owned(),
                version: None,
                deployed_tier: None,
            }
        } else {
            let name = req.name.clone().unwrap_or_default();
            let mut conn = ep.pool.get_conn().await.map_err(|e| {
                SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
            })?;
            let l =
                testrun::load(&mut conn, &store, row.db_id.0, &kind, &name, req.version).await?;
            let callable = matches!(kind.as_str(), "function" | "procedure");
            let object_ok = callable && {
                use sprig_auth::authz::{Action, Target};
                let Some(ts) = self.tenant.as_deref() else {
                    return Err(SprigError::internal("no tenant"));
                };
                let grants = crate::tenant::authz::privileges(ts, &cat);
                let db_key =
                    sprig_catalog::ddl::database_key(&row.name, ts.lower_case_table_names());
                grants.allows(
                    Action::Execute,
                    Target::Object {
                        db_key: &db_key,
                        db: row.db_id,
                        object: sprig_common::ids::ObjectId(l.module_id),
                    },
                )
            };
            let allowed = if callable {
                may_execute || object_ok
            } else {
                may_create
            };
            if !allowed {
                return Err(denied(&user, &row.name));
            }
            Subject {
                program: l.program()?,
                name: l.name.clone(),
                kind: l.kind.clone(),
                runtime: l.runtime.clone(),
                version: Some(l.version),
                deployed_tier: Some(l.tier.clone()),
            }
        };
        let info = json!({
            "user": user,
            "tenant": tenant.0,
            "database": row.name.clone(),
            "function": subject.name.clone(),
            "version": subject.version,
            "testRun": true,
        });
        let (args, info) = if matches!(subject.kind.as_str(), "function" | "procedure") {
            let names = param_names(&subject.program);
            let pos = testrun::positional(&req.args, &names, &subject.name)?;
            (subject.program.coerce(&subject.name, pos)?, info)
        } else {
            let kp = match &subject.program {
                sprig_logic::engine::Program::Kirun(p) => p.params().to_vec(),
                _ => Vec::new(),
            };
            testrun::trigger_args(&req, &subject.runtime, &kp, &info)
        };
        let sctx = cat.setting_ctx(&svc.dc).with_db(row.db_id);
        let (limits, policy) =
            sprig_logic::settings::limits(&svc.settings, &sctx, Some(&cat.settings));
        let wire = Wire {
            persona: self.persona,
            caps: cx.proto.caps,
        };
        let mut report = Report {
            database: row.name.clone(),
            kind: subject.kind.clone(),
            name: subject.name.clone(),
            version: subject.version,
            draft: subject.version.is_none(),
            runtime: subject.runtime.clone(),
            deployed_tier: subject.deployed_tier.clone(),
            ..Report::default()
        };
        if subject.deployed_tier.as_deref() == Some("mariadb") {
            report.notes.push(
                "the version is compiled into MariaDB; the test ran its source on the engine tier, \
                 which the differential suite holds to the same answers"
                    .to_owned(),
            );
        }
        if matches!(subject.kind.as_str(), "trigger" | "validation") {
            report
                .notes
                .push("ran with your rights, not the trigger's definer's".to_owned());
        }
        // The transaction everything happens in, rolled back at the end.
        let in_txn = self.status().contains(StatusFlags::IN_TRANS);
        let (begin, end) = if in_txn {
            (
                format!("SAVEPOINT {SAVEPOINT}"),
                format!("ROLLBACK TO SAVEPOINT {SAVEPOINT}"),
            )
        } else {
            ("START TRANSACTION".to_owned(), "ROLLBACK".to_owned())
        };
        self.test_stmt(cx, &begin, wire).await?;
        let mut temps: Vec<String> = Vec::new();
        let setup = Instant::now();
        let mut setup_err = None;
        'snap: for (t, rows) in &req.rows {
            // The table's definition, as the caller may see it.
            let show = self
                .engine_db(
                    cx,
                    DbOp {
                        shape: eb::Shape::Many,
                        sql: testrun::snapshot_show(&row.name, t),
                    },
                    wire,
                )
                .await;
            let made = match show {
                Ok(Reply::Rows { rows: r, .. }) => r
                    .first()
                    .and_then(|x| x.get(1).cloned().flatten())
                    .ok_or_else(|| {
                        SprigError::new(
                            SprigCode::LogicSignatureMismatch,
                            format!("rows: {t} has no definition"),
                        )
                    })
                    .and_then(|text| testrun::temporary_from_show(&row.name, t, &text))
                    .and_then(|create| testrun::snapshot_sql(&row.name, t, &create, rows)),
                Ok(Reply::Affected(_)) => {
                    Err(SprigError::internal("SHOW CREATE TABLE answered OK"))
                }
                Err(e) => Err(e),
            };
            let stmts = match made {
                Ok(s) => s,
                Err(e) => {
                    setup_err = Some(SprigError {
                        kind: e.kind,
                        message: format!("rows of {t}: {}", e.message),
                        retryable: e.retryable,
                        source: None,
                    });
                    break;
                }
            };
            for (i, sql) in stmts.iter().enumerate() {
                if let Err(e) = self.test_stmt(cx, sql, wire).await {
                    setup_err = Some(SprigError {
                        kind: e.kind,
                        message: format!("rows of {t}: {}", e.message),
                        retryable: e.retryable,
                        source: None,
                    });
                    break 'snap;
                }
                if i == 0 {
                    temps.push(t.clone());
                }
            }
        }
        let setup_ms = ms(setup);
        let rec = Arc::new(Recorder::default());
        let mut run_ms = 0.0;
        if let Some(e) = setup_err {
            report.error = Some(testrun::error_json(&e));
        } else {
            let (tx, mut rx) = mpsc::channel::<Request>(1);
            let host = Host {
                db: Arc::new(TestDb {
                    tx,
                    rec: Arc::clone(&rec),
                }),
                outbound: Arc::new(MockOutbound {
                    policy,
                    mocks: req.fetch.clone(),
                    rec: Arc::clone(&rec),
                }),
                default_db: Some(row.name.clone()),
                info,
            };
            let run_start = Instant::now();
            let run = subject.program.run(args, host, limits.clone());
            tokio::pin!(run);
            let outcome = loop {
                tokio::select! {
                    biased;
                    r = &mut run => break r,
                    Some((op, reply)) = rx.recv() => {
                        let r = self.test_db(cx, &store, &ep, row.db_id.0, &row.name, op, wire, &rec).await;
                        let _ = reply.send(r);
                    }
                }
            };
            run_ms = ms(run_start);
            match outcome {
                Ok(o) => {
                    report.event = Some(o.event.clone());
                    report.result = Some(o.result.clone());
                    report.budget = json!({
                        "limits": {
                            "wallMs": u64::try_from(limits.wall.as_millis()).unwrap_or(u64::MAX),
                            "steps": limits.max_steps,
                            "loopIterations": limits.max_loop_iterations,
                            "memoryBytes": limits.memory_bytes,
                            "dataOps": limits.max_data_ops,
                            "rowsRead": limits.max_rows_read,
                        },
                        "used": {
                            "wallMs": run_ms,
                            "steps": o.steps,
                            "dataOps": o.data_ops,
                            "rowsRead": o.rows_read,
                            "peakBytes": o.peak_bytes,
                        },
                    });
                }
                Err(e) => {
                    report.error = Some(testrun::error_json(&e));
                }
            }
            // The snapshot tables as the run left them.
            for t in &temps {
                let sql = testrun::snapshot_read(&row.name, t);
                match self
                    .engine_db(
                        cx,
                        DbOp {
                            shape: eb::Shape::Many,
                            sql,
                        },
                        wire,
                    )
                    .await
                {
                    Ok(Reply::Rows { columns, rows }) => {
                        let mut list: Vec<Value> = rows
                            .iter()
                            .take(testrun::MAX_TABLE_ROWS)
                            .map(|r| eb::row_object(&columns, r).unwrap_or(Value::Null))
                            .collect();
                        if rows.len() > testrun::MAX_TABLE_ROWS {
                            list.push(json!({"truncated": true}));
                        }
                        report.tables.insert(t.clone(), Value::Array(list));
                    }
                    Ok(Reply::Affected(_)) => {}
                    Err(e) => {
                        report
                            .notes
                            .push(format!("{t} couldn't be read back: {}", e.message));
                    }
                }
            }
        }
        // Nothing stays: the transaction (or savepoint), then the
        // temporary tables (DDL on temporary tables doesn't commit).
        let undo = self.test_stmt(cx, &end, wire).await;
        if in_txn {
            let _ = self
                .test_stmt(cx, &format!("RELEASE SAVEPOINT {SAVEPOINT}"), wire)
                .await;
        }
        for t in &temps {
            if let Err(e) = self
                .test_stmt(cx, &testrun::snapshot_drop(&row.name, t), wire)
                .await
            {
                tracing::warn!(error = %e, table = %t, "test run: a snapshot table wasn't dropped");
            }
        }
        undo?;
        report.fill(&rec);
        if report.budget.is_null() {
            report.budget = json!({
                "limits": {
                    "wallMs": u64::try_from(limits.wall.as_millis()).unwrap_or(u64::MAX),
                    "dataOps": limits.max_data_ops,
                    "rowsRead": limits.max_rows_read,
                },
                "used": {"wallMs": run_ms, "dataOps": report.statements.len()},
            });
        }
        report.timing = json!({
            "totalMs": ms(started),
            "setupMs": setup_ms,
            "runMs": run_ms,
            "dbMs": (report.db_ms() * 1000.0).round() / 1000.0,
        });
        tracing::debug!(name = %report.name, statements = report.statements.len(), "logic test run");
        Ok(serde_json::to_value(&report).unwrap_or(Value::Null))
    }

    /// One statement of the test run's own (transaction control, the
    /// snapshot).
    async fn test_stmt(&mut self, cx: &mut ExecCtx<'_>, sql: &str, wire: Wire) -> Result<()> {
        self.engine_db(
            cx,
            DbOp {
                shape: eb::Shape::Exec,
                sql: sql.to_owned(),
            },
            wire,
        )
        .await
        .map(|_| ())
    }

    /// One statement of the code, checked, run and recorded.
    #[allow(clippy::too_many_arguments)]
    async fn test_db(
        &mut self,
        cx: &mut ExecCtx<'_>,
        store: &sprig_logic::LogicStore,
        ep: &sprig_catalog::CatalogEndpoint,
        db_id: u32,
        db_name: &str,
        op: DbOp,
        wire: Wire,
        rec: &Recorder,
    ) -> Result<Reply> {
        let class = testrun::classify(&op.sql);
        let mut s = Statement {
            sql: op.sql.chars().take(4096).collect(),
            ..Statement::default()
        };
        let refuse = |why: String, mut s: Statement| {
            let e = SprigError::new(SprigCode::LogicUnsupported, why);
            "refused".clone_into(&mut s.kind);
            s.error = Some(testrun::error_json(&e));
            rec.statement(s);
            Err(e)
        };
        match &class {
            StmtClass::Refused(why) => return refuse(why.clone(), s),
            StmtClass::Call(name) => {
                "call".clone_into(&mut s.kind);
                // Only logic functions, which never end the transaction.
                let (db, n) = match name.split_once('.') {
                    Some((d, n)) => (Some(d.to_owned()), n.to_owned()),
                    None => (None, name.clone()),
                };
                let id = match &db {
                    Some(d) if !d.eq_ignore_ascii_case(db_name) => self
                        .tenant
                        .as_deref()
                        .and_then(|ts| ts.catalog())
                        .and_then(|c| {
                            c.database(d)
                                .or_else(|| c.database(&d.to_ascii_lowercase()))
                                .map(|r| r.db_id.0)
                        }),
                    _ => Some(db_id),
                };
                let found = match id {
                    Some(id) => {
                        let mut conn = ep.pool.get_conn().await.map_err(|e| {
                            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable")
                                .with_source(e)
                        })?;
                        store.callable(&mut conn, id, &n).await?.is_some()
                    }
                    None => false,
                };
                if !found {
                    return refuse(
                        format!(
                            "a test run calls logic functions only; {name} isn't one (a stored \
                             procedure could end the transaction)"
                        ),
                        s,
                    );
                }
            }
            StmtClass::Read => "read".clone_into(&mut s.kind),
            StmtClass::Write => "write".clone_into(&mut s.kind),
        }
        let t = Instant::now();
        let r = self.engine_db(cx, op, wire).await;
        s.ms = ms(t);
        match &r {
            Ok(Reply::Rows { rows, .. }) => {
                s.rows = Some(u64::try_from(rows.len()).unwrap_or(u64::MAX));
            }
            Ok(Reply::Affected(n)) => s.affected = Some(*n),
            Err(e) => s.error = Some(testrun::error_json(e)),
        }
        rec.statement(s);
        r
    }
}
