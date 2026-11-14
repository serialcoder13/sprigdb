//! The executor's side of workload capture (see [`crate::workload`]): fingerprint a tenant statement before it runs,
//! ask for a `Handler_read_*` probe, and record it after.

use sprig_catalog::TenantCatalog;
use sprig_common::ids::DbId;
use sprig_proto_mysql::StatusFlags;
use sprig_workload::fingerprint::{FrontEnd, SqlCtx};
use sprig_workload::samples::SampleContext;
use sprig_workload::window::SampleValue;

use super::PassSession;
use crate::backend::StmtOutcome;
use crate::pipeline::ExecCtx;
use crate::tenant::session::TenantSession;
use crate::workload::{CatalogResolver, Done, Pending, SampleInput, writes_of};

/// How a captured statement ended, as the executor saw it.
#[derive(Debug, Default)]
pub(super) struct Ran<'a> {
    /// The backend's outcome, when the statement finished there.
    pub outcome: Option<&'a StmtOutcome>,
    /// An error went to the client (or the executor failed).
    pub errored: bool,
    /// Bound values of a prepared statement, rendered as literals.
    pub params: &'a [SampleValue],
}

fn db_id_of(ts: &TenantSession, cat: Option<&TenantCatalog>, db: Option<&str>) -> DbId {
    let (Some(cat), Some(db)) = (cat, db) else {
        return DbId(0);
    };
    let key = if ts.lower_case_table_names() {
        db.to_lowercase()
    } else {
        db.to_owned()
    };
    cat.database(&key).map_or(DbId(0), |r| r.db_id)
}

/// What a prepared statement's fingerprint was made under:
/// the tenant catalog's version (table ids, and `lower_case_table_names`,
/// a tenant setting the catalog carries), the current database, the
/// lexer's options and the capture setting that changes the text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FpKey {
    version: u64,
    db: Option<String>,
    lex: sprig_sql::lex::LexOptions,
    max_text: usize,
}

impl PassSession {
    /// [`PassSession::wl_start`] for prepared statement `stmt`: its
    /// fingerprint is made once and kept with the statement while what it
    /// was made under stays the same (a logic function's statements, which
    /// carry their caller, are fingerprinted each time).
    pub(super) fn wl_start_stmt(
        &mut self,
        stmt: crate::pipeline::StmtRef,
        logical: &[u8],
    ) -> Option<Pending> {
        if crate::workload::task_caller().is_some() {
            return self.wl_start(logical);
        }
        let key = {
            let wl = self.wl.as_ref()?;
            let ts = self.tenant.as_deref()?;
            FpKey {
                version: ts.catalog().map_or(0, |c| c.version),
                db: self.current_logical(),
                lex: self.lex_opts().0,
                max_text: wl.workload().max_text_bytes(),
            }
        };
        let cached = self
            .stmts
            .get_mut(stmt)
            .ok()
            .and_then(|s| s.fp.as_ref())
            .filter(|(k, _)| *k == key)
            .map(|(_, fp)| fp.clone());
        if let Some(fp) = cached {
            let wl = self.wl.as_ref()?;
            let tenant = self.tenant.as_deref()?.tenant;
            return Some(wl.start(tenant, fp, self.wl_multi));
        }
        let p = self.wl_start(logical)?;
        if let Ok(s) = self.stmts.get_mut(stmt) {
            s.fp = Some((key, p.fingerprint().clone()));
        }
        Some(p)
    }

    /// Fingerprints `logical` (a tenant statement's text, logical names)
    /// and asks the recorder for a plan. `None` without capture or tenant.
    pub(super) fn wl_start(&self, logical: &[u8]) -> Option<Pending> {
        let wl = self.wl.as_ref()?;
        let ts = self.tenant.as_deref()?;
        let (lex, _) = self.lex_opts();
        let current = self.current_logical();
        let cat = ts.catalog().map(AsRef::as_ref);
        let map = ts.map();
        let lower = ts.lower_case_table_names();
        let ctx = SqlCtx {
            lex,
            current_db: current.as_deref(),
            db_id: db_id_of(ts, cat, current.as_deref()),
            front_end: FrontEnd::of_persona(self.persona),
            // `caller = studio` for a Studio session's statements.
            caller: crate::workload::task_caller(),
            lower_case_table_names: lower,
            max_text_bytes: wl.workload().max_text_bytes(),
        };
        let resolver = CatalogResolver::new(cat, &*map, lower);
        let fp = wl.fingerprint(ts.tenant, logical, &ctx, &resolver);
        Some(wl.start(ts.tenant, fp, self.wl_multi))
    }

    /// Asks the backend session to probe the next statement when `p` says
    /// so.
    pub(super) fn wl_probe(&mut self, p: Option<&Pending>) {
        if p.is_some_and(Pending::probe)
            && let Some(bs) = self.bs.as_mut()
        {
            bs.probe_next_statement();
        }
    }

    /// Records a statement started with [`PassSession::wl_start`].
    pub(super) fn wl_finish(
        &mut self,
        cx: &ExecCtx<'_>,
        p: Option<Pending>,
        logical: &[u8],
        ran: &Ran<'_>,
    ) {
        let Some(p) = p else {
            return;
        };
        let Some(ts) = self.tenant.as_deref() else {
            return;
        };
        let cat = ts.catalog().cloned();
        let map = ts.map();
        let lower = ts.lower_case_table_names();
        let resolver = CatalogResolver::new(cat.as_deref(), &*map, lower);
        let (groups, tables) = writes_of(p.fingerprint(), &resolver);
        let sample = if p.wants_sample() && self.wl_capture_allowed(ts, cat.as_deref()) {
            let current = self.current_logical();
            let vars = self.bs.as_ref().map(|b| &b.vars().vars);
            let var = |k: &str| vars.and_then(|v| v.get(k)).cloned();
            Some(SampleInput {
                text: logical,
                params: ran.params,
                context: SampleContext {
                    db_id: db_id_of(ts, cat.as_deref(), current.as_deref()),
                    database: current,
                    sql_mode: var("sql_mode"),
                    time_zone: var("time_zone"),
                    charset: var("character_set_client"),
                    collation: var("collation_connection"),
                },
            })
        } else {
            None
        };
        let tenant = ts.tenant;
        let o = ran.outcome;
        let done = Done {
            error: ran.errored || o.is_some_and(|o| o.error.is_some()),
            rows_returned: o.map_or(0, |o| o.rows),
            rows_affected: o.map_or(0, |o| o.affected_rows),
            examined: o.and_then(|o| o.examined),
            in_transaction: o.is_some_and(|o| o.status.contains(StatusFlags::IN_TRANS)),
            groups: &groups,
            tables: &tables,
            sample,
        };
        let failed = done.error;
        if let Some(wl) = self.wl.as_mut() {
            wl.finish(tenant, cx.session.id, &p, done);
        }
        if !failed {
            self.wl_compound_writes(cx, logical);
        }
    }

    /// The writes inside a compound statement that ran: the guarded
    /// batches, one per Redis or key-value write command, have no shape, so
    /// each `INSERT`,
    /// `REPLACE`, `UPDATE` and `DELETE` in the body is fingerprinted and
    /// counted on its own (weighted by its `IF` branches), and the tables'
    /// write load includes them.
    fn wl_compound_writes(&self, cx: &ExecCtx<'_>, logical: &[u8]) {
        if !logical
            .get(..5)
            .is_some_and(|b| b.eq_ignore_ascii_case(b"BEGIN"))
        {
            return;
        }
        let (Some(wl), Some(ts)) = (self.wl.as_ref(), self.tenant.as_deref()) else {
            return;
        };
        let (lex, _) = self.lex_opts();
        let inner = sprig_workload::fingerprint::compound::writes(logical, &lex);
        if inner.is_empty() {
            return;
        }
        let current = self.current_logical();
        let cat = ts.catalog().map(AsRef::as_ref);
        let map = ts.map();
        let lower = ts.lower_case_table_names();
        let ctx = SqlCtx {
            lex,
            current_db: current.as_deref(),
            db_id: db_id_of(ts, cat, current.as_deref()),
            front_end: FrontEnd::of_persona(self.persona),
            caller: crate::workload::task_caller(),
            lower_case_table_names: lower,
            max_text_bytes: wl.workload().max_text_bytes(),
        };
        let resolver = CatalogResolver::new(cat, &*map, lower);
        for w in inner {
            let Some(text) = logical.get(w.range) else {
                continue;
            };
            let fp = wl.fingerprint(ts.tenant, text, &ctx, &resolver);
            wl.record_inner(ts.tenant, cx.session.id, &fp, w.weight);
        }
    }

    /// `workload.capture_samples` for the tenant (and its current
    /// database).
    pub(super) fn wl_capture_allowed(
        &self,
        ts: &TenantSession,
        cat: Option<&TenantCatalog>,
    ) -> bool {
        let Some(cat) = cat else {
            return false;
        };
        let svc = ts.services();
        let mut ctx = cat.setting_ctx(&svc.dc);
        let current = self.current_logical();
        let db = db_id_of(ts, Some(cat), current.as_deref());
        if db != DbId(0) {
            ctx = ctx.with_db(db);
        }
        svc.settings.get(
            &sprig_workload::settings::CAPTURE_SAMPLES,
            &ctx,
            Some(&cat.settings),
        )
    }
}
