//! DC routing in the executor: `SET @sprig_dc` and the traffic counts (see
//! [`crate::dc`]).

use sprig_common::{Result, SprigCode};
use sprig_proto_mysql::packets::OkPacket;

use super::PassSession;
use crate::backend::classify::StmtClass;
use crate::dc::{self, DcHint};
use crate::pipeline::{ExecCtx, ExecSummary, Request, ResultSink};
use crate::tenant::session::LocalWarning;

impl PassSession {
    /// Answers `SET @sprig_dc = '<id>'` of a tenant session itself: the
    /// session's client DC changes (or clears, for an empty value), and an
    /// unknown id answers OK with warning 39503 for `SHOW WARNINGS`.
    /// `None` for any other request.
    pub(super) async fn sprig_dc_set(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: &Request,
        out: &mut dyn ResultSink,
    ) -> Option<Result<ExecSummary>> {
        let Request::Query { sql, .. } = req else {
            return None;
        };
        self.tenant.as_ref()?;
        let raw = dc::parse_set(sql)?;
        let hint = dc::hint("mysql", Some(&raw));
        let mut ok = OkPacket::new(self.status());
        let warning = match &hint {
            DcHint::Unknown(s) => Some(LocalWarning {
                level: "Warning",
                code: SprigCode::UnknownDc.number(),
                message: format!("Unknown data center '{s}': the DC id is ignored"),
            }),
            _ => None,
        };
        if let Some(ts) = self.tenant.as_deref_mut() {
            ts.warnings = warning.clone().map(|w| vec![w]);
        }
        if warning.is_some() {
            ok.warnings = 1;
        }
        cx.session.client_dc = hint.known();
        if let Err(e) = out.ok(ok).await {
            return Some(Err(e));
        }
        Some(Ok(ExecSummary {
            status: Some(self.status()),
            prepared: None,
            db: Some(self.current_logical()),
        }))
    }

    /// Counts a tenant statement as a read or a write from the session's
    /// client DC.
    pub(super) fn count_traffic(&mut self, cx: &ExecCtx<'_>, req: &Request) {
        let Some(ts) = self.tenant.as_deref() else {
            return;
        };
        let write = match req {
            Request::Query { sql, .. } => dc::is_write(sql),
            Request::Execute { stmt, .. } | Request::BulkExecute { stmt, .. } => self
                .stmts
                .get_mut(*stmt)
                .is_ok_and(|s| s.info.class == StmtClass::Write),
            _ => return,
        };
        ts.services()
            .locator
            .routing()
            .record(ts.tenant, cx.session.client_dc.as_ref(), write);
    }
}
