//! Point reads for protocol engines (S149 step 1, S154): a prepared
//! single-row read (MongoDB's `_id` finds) executed on the session's
//! backend connection without the relay, the result sink or the general
//! execute's per-statement planning.
//!
//! What the general path checks, this one checks the same way:
//!
//! - **placement**: the session's placement memo must show that
//!   `follow_tenant` found nothing to wait for at the tenant's current
//!   (home, map generation) and cluster. A hold, a cutover, a promotion or
//!   a move changes the generation, and the read falls back;
//! - **privileges**: the catalog is refreshed and the statement's
//!   privileges checked again (`authz::recheck`) exactly as an execute
//!   does. A `REVOKE` changes the compiled privileges, the recheck fails,
//!   and the read falls back (the general path then answers 1142);
//! - **row-level security**: the statement's text is the one prepared
//!   with the session's policy predicates, the recheck fails when the role
//!   set or policy version changed (1615, prepared again by the general
//!   path), and the backend connection reads through the session's policy
//!   views as every statement does ([`BackendSession::execute`]);
//! - **routing**: a tenant with grouped tables goes through the router:
//!   always the general path. Whether it has any is kept per (catalog
//!   version, home, map generation);
//! - **state**: the backend session syncs its variables (`max_statement_time`
//!   of `maxTimeMS`, the schema) and routes the read (writer, or a replica
//!   that has the session's writes) as for any statement.
//!
//! Anything but a row or an empty result (an ERR, a fence, a lost
//! connection, a transaction, a cursor, long data) falls back, and the
//! general path runs the statement again: a read is safe to repeat, and its
//! errors, fences and retries stay in one place.

use bytes::Bytes;
use sprig_common::Result;
use sprig_common::ids::{ClusterId, DcId};
use sprig_proto_mysql::ColumnDef;
use sprig_proto_mysql::client::PacketKind;
use sprig_proto_mysql::command::ExecuteParams;

use super::PassSession;
use super::stmt::execute_payload;
use crate::backend::BackendStmt;
use crate::backend::classify::StmtClass;
use crate::pipeline::{ExecCtx, PointRead, StmtRef};

/// What decided that the tenant's statements aren't routed, kept per
/// (catalog version, home, map generation).
#[derive(Debug, Default)]
pub(super) struct PointMemo {
    unrouted: Option<(u64, DcId, u64, ClusterId)>,
}

impl PassSession {
    /// [`SessionExec::point_read`](crate::pipeline::SessionExec::point_read);
    /// see the module docs.
    ///
    /// # Errors
    /// None of its own: whatever goes wrong falls back.
    #[allow(clippy::too_many_lines)] // the checks, then one statement
    pub(super) async fn point_read(
        &mut self,
        cx: &mut ExecCtx<'_>,
        stmt: StmtRef,
        params: &ExecuteParams,
    ) -> Result<PointRead> {
        let fallback = Ok(PointRead::Fallback);
        // A session in a transaction (the router's or the backend's), with
        // a recording branch or with state pinned to its connection, and
        // one whose tenant defaults aren't applied yet: the general path.
        if !self.globals_applied || self.router.in_txn() {
            return fallback;
        }
        if self
            .bs
            .as_ref()
            .is_none_or(|b| b.is_pinned() || b.recording() || b.in_flight())
        {
            return fallback;
        }
        let Some(ts) = self.tenant.as_deref_mut() else {
            return fallback;
        };
        let (home, generation) = ts.services().locator.maps_generation(ts.tenant);
        if !self
            .placement_memo
            .followed_at(&home, generation, &self.target.cluster)
        {
            return fallback;
        }
        let Ok(cat) = ts.refresh().await else {
            return fallback;
        };
        let persona = self.persona;
        let Ok(s) = self.stmts.get_mut(stmt) else {
            return fallback;
        };
        if s.deferred.is_some()
            || s.cursor.is_some()
            || s.info.class != StmtClass::Read
            || !s.long_data.is_empty()
            || s.long_data_over
            || !s.renames.is_empty()
            || s.session_status
            || s.mapper.as_ref().is_some_and(|m| m.needs_rows())
        {
            return fallback;
        }
        if let Some(rc) = &s.recheck
            && let Some(ts) = self.tenant.as_deref()
            && crate::tenant::authz::recheck(ts, persona, rc).is_err()
        {
            return fallback;
        }
        let Ok(values) = s.params_for_backend(params, persona) else {
            return fallback;
        };
        if params.new_params_bound {
            s.types = Some(values.iter().map(|p| p.param_type).collect());
        }
        let (sql, info, logical) = (s.sql.clone(), s.info.clone(), s.logical.clone());
        let columns: Vec<(u8, bool)> = s.columns.iter().map(binary_type).collect();
        // Routing: kept per catalog version and map generation.
        let key = (cat.version, home, generation, self.target.cluster.clone());
        if self.point_memo.unrouted.as_ref() != Some(&key) {
            if self.routes().await {
                return fallback;
            }
            self.point_memo.unrouted = Some(key);
        }
        if let Some(ts) = self.tenant.as_deref() {
            ts.services()
                .locator
                .routing()
                .record(ts.tenant, cx.session.client_dc.as_ref(), false);
        }
        self.count_execute(stmt, cx.session);
        self.wl_multi = false;
        let pending = logical
            .as_ref()
            .and_then(|l| self.wl_start_stmt(stmt, l));
        self.wl_probe(pending.as_ref());
        let Some(bs) = self.bs.as_mut() else {
            return fallback;
        };
        let client_bound = params.new_params_bound;
        let mut build = |b: &BackendStmt| execute_payload(b, 0, &values, client_bound);
        if bs.execute(&info, &sql, &mut build).await.is_err() {
            return fallback;
        }
        let caps = bs.backend_caps().unwrap_or(self.backend_caps(cx));
        let Some(bs) = self.bs.as_mut() else {
            return fallback;
        };
        let mut defs: Vec<(u8, bool)> = Vec::new();
        let mut row: Option<Bytes> = None;
        let mut bad = false;
        loop {
            match bs.next().await {
                Ok(Some(p)) => match p.kind {
                    PacketKind::ColumnCount(_) => defs.clear(),
                    PacketKind::ColumnDef => match ColumnDef::decode(&p.payload, caps) {
                        Ok(c) => defs.push(binary_type(&c)),
                        Err(_) => bad = true,
                    },
                    PacketKind::Row => {
                        if row.is_none() {
                            row = Some(p.payload);
                        }
                    }
                    PacketKind::ColumnsEof | PacketKind::End(_) | PacketKind::Ok(_) => {}
                    _ => bad = true,
                },
                Ok(None) => break,
                Err(_) => {
                    bad = true;
                    break;
                }
            }
        }
        let outcome = bs.finish().await;
        let errored = bad || outcome.as_ref().map_or(true, |o| o.error.is_some());
        if let Some(l) = &logical {
            let rendered: Vec<_> = if pending
                .as_ref()
                .is_some_and(crate::workload::Pending::wants_sample)
            {
                let nbe = self.lex_opts().0.no_backslash_escapes;
                values
                    .iter()
                    .map(|p| match &p.value {
                        sprig_proto_mysql::command::ParamValue::Value(v) => {
                            crate::workload::sample_value(v, nbe)
                        }
                        sprig_proto_mysql::command::ParamValue::LongData => {
                            crate::workload::sample_value(&sprig_proto_mysql::Value::Null, nbe)
                        }
                    })
                    .collect()
            } else {
                Vec::new()
            };
            let ran = super::capture::Ran {
                outcome: outcome.as_ref().ok(),
                errored,
                params: &rendered,
            };
            self.wl_finish(cx, pending, l, &ran);
        }
        if errored {
            return fallback;
        }
        let types = if defs.is_empty() { &columns } else { &defs };
        let Some(payload) = row else {
            return Ok(PointRead::Row(None));
        };
        match sprig_proto_mysql::resultset::decode_binary_row_types(&payload, types) {
            Ok(v) => Ok(PointRead::Row(Some(v))),
            Err(_) => fallback,
        }
    }
}

/// A column's type and unsignedness, as a binary row encodes its values.
fn binary_type(c: &ColumnDef) -> (u8, bool) {
    (c.col_type, c.is_unsigned())
}
