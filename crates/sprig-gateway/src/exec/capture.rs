//! The executor's side of workload capture (chapter 12; see
//! [`crate::workload`]): fingerprint a tenant statement before it runs,
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

impl PassSession {
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
            caller: None,
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
        if let Some(wl) = self.wl.as_mut() {
            wl.finish(tenant, cx.session.id, &p, done);
        }
    }

    /// `workload.capture_samples` for the tenant (and its current
    /// database).
    fn wl_capture_allowed(&self, ts: &TenantSession, cat: Option<&TenantCatalog>) -> bool {
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
