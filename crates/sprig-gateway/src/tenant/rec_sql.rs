//! Chapter 52's SQL: `SHOW SPRIG RECOMMENDATIONS` reads the tenant's own
//! catalog and shows the items the session may see; `SPRIG APPROVE`,
//! `IGNORE` and `SNOOZE RECOMMENDATION` go to the DC leader's
//! `Recommendations` service with the session as a caller claim (its
//! privileges on the item's database and table, checked here), so every
//! transition and its notifications happen in one place.

use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use sprig_approvals::catalog::SqlRecs;
use sprig_approvals::events::rfc3339;
use sprig_approvals::model::{Level, Recommendation};
use sprig_approvals::store::Filter;
use sprig_approvals::{Caller, may_act, may_see};
use sprig_auth::authz::{Action, SessionGrants, Target};
use sprig_catalog::ddl::database_key;
use sprig_catalog::{CatalogLocator, TenantCatalog};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::ColumnDef;
use sprig_sql::names::LocalAnswer;
use sprig_sql::rec_stmt::{RecScope, RecStmt};

use super::session::TenantSession;
use crate::http::admin::AdminApi;
use crate::pipeline::text_column;

static ADMIN: OnceLock<Arc<AdminApi>> = OnceLock::new();

/// Gives the SQL path the Admin API forwarder (set when HTTP starts).
pub fn install(admin: Arc<AdminApi>) {
    let _ = ADMIN.set(admin);
}

/// The Admin API forwarder, once HTTP started (chapter 52's gate for the
/// gateway's own decisions uses it too).
#[must_use]
pub fn admin() -> Option<Arc<AdminApi>> {
    ADMIN.get().cloned()
}

/// A statement's answer.
#[derive(Debug)]
pub enum RecAnswer {
    /// An action: one row affected.
    Ok,
    /// `SHOW`.
    Rows(LocalAnswer),
}

/// `SHOW SPRIG RECOMMENDATIONS`'s columns.
#[must_use]
pub fn columns(charset: u16) -> Vec<ColumnDef> {
    [
        "ID",
        "KIND",
        "LEVEL",
        "STATE",
        "TARGET",
        "SUMMARY",
        "ACTION",
        "AGE_HOURS",
        "NOTIFIES",
        "AUTO_APPLY_AT",
        "FULL_ID",
    ]
    .iter()
    .map(|n| text_column(n, charset))
    .collect()
}

fn backend(e: mysql_async::Error) -> SprigError {
    SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e)
}

/// The session as a caller of `rec`: its tenant, administrator right and
/// `ALTER`/`INDEX` on the item's database and table.
fn caller_for(
    ts: &TenantSession,
    cat: &TenantCatalog,
    p: &dyn SessionGrants,
    rec: &Recommendation,
) -> Caller {
    let lower = ts.lower_case_table_names();
    let tenant_admin = p.allows(Action::TenantAdmin, Target::Tenant);
    let action_ok = |t: Target<'_>| {
        p.allows(Action::Alter, t) || (rec.kind.is_index() && p.allows(Action::Index, t))
    };
    let (mut db_privilege, mut table_privilege) = (false, false);
    if let Some(db) = rec.subject.db.and_then(|d| cat.databases.get(d)) {
        let key = database_key(&db.name, lower);
        db_privilege = action_ok(Target::Database { db_key: &key });
        if let Some(o) = rec.subject.object {
            table_privilege = db_privilege
                || action_ok(Target::Object {
                    db_key: &key,
                    db: db.db_id,
                    object: o,
                });
        }
    }
    Caller {
        name: format!(
            "sql:{}.{}",
            ts.principal.tenant_name.clone().unwrap_or_default(),
            ts.principal.user
        ),
        tenant: Some(ts.tenant),
        tenant_admin: tenant_admin || ts.principal.user_id.is_none(),
        db_privilege,
        table_privilege,
        ..Caller::default()
    }
}

#[allow(clippy::unnecessary_wraps)] // a row cell: `None` is NULL
fn text(s: &str) -> Option<Bytes> {
    Some(Bytes::from(s.to_owned()))
}

/// Runs `stmt` for `ts`.
///
/// # Errors
/// 1046, 1049, 1146 for unknown names; 39972-39977 from the service;
/// 39070 when the catalog or the controller is unreachable.
#[allow(clippy::too_many_lines)] // one arm per statement
pub async fn execute(
    ts: &TenantSession,
    cat: &Arc<TenantCatalog>,
    current: Option<&str>,
    stmt: &RecStmt,
    charset: u16,
) -> Result<RecAnswer> {
    let p = super::authz::privileges(ts, cat);
    let ep = ts.services().locator.writer(ts.tenant).await?;
    let recs = SqlRecs::new(ts.tenant, ep.store.schema());
    let mut conn = ep.pool.get_conn().await.map_err(backend)?;
    let id_of = |n: u64| sprig_approvals::RecId::Tenant(ts.tenant, n);
    match stmt {
        RecStmt::Show { all, scope } => {
            let f = Filter {
                all: *all,
                ..Filter::default()
            };
            let items = match recs.list(&mut conn, &f).await {
                Ok(v) => v,
                // No item was ever raised: the module isn't migrated yet.
                Err(e)
                    if matches!(e.kind, sprig_common::ErrorKind::Backend { errno: 1146, .. }) =>
                {
                    Vec::new()
                }
                Err(e) => return Err(e),
            };
            let (db_name, table) = match scope {
                RecScope::Tenant => (None, None),
                RecScope::Database(d) => (Some(d.clone()), None),
                RecScope::Table { db, name } => (
                    db.clone().or_else(|| current.map(str::to_owned)),
                    Some(name.clone()),
                ),
            };
            let now = sprig_approvals::model::wall_micros(ts.services().clock.wall());
            let mut rows = Vec::new();
            for r in items {
                if db_name.as_ref().is_some_and(|d| {
                    r.subject
                        .db_name
                        .as_ref()
                        .is_none_or(|x| !x.eq_ignore_ascii_case(d))
                }) {
                    continue;
                }
                if table.as_ref().is_some_and(|t| {
                    r.subject
                        .object_name
                        .as_ref()
                        .is_none_or(|x| !x.eq_ignore_ascii_case(t))
                }) {
                    continue;
                }
                let c = caller_for(ts, cat, &*p, &r);
                // Ancestry doesn't matter inside the tenant itself.
                if !may_see(&r, &c, &[]) {
                    continue;
                }
                rows.push(vec![
                    text(&r.id.number().to_string()),
                    text(r.kind.as_str()),
                    text(r.level.as_str()),
                    text(r.state.as_str()),
                    text(&r.subject.display()),
                    text(&r.summary),
                    text(&r.action.text),
                    text(&format!("{:.1}", r.age_hours(now))),
                    text(&r.notify_level.to_string()),
                    r.auto_apply_at_us.map(rfc3339).as_deref().and_then(text),
                    text(&r.id.to_string()),
                ]);
            }
            Ok(RecAnswer::Rows(LocalAnswer {
                columns: columns(charset),
                rows,
            }))
        }
        RecStmt::Approve { id, .. } | RecStmt::Ignore { id, .. } | RecStmt::Snooze { id, .. } => {
            let Ok(Some(rec)) = recs.get(&mut conn, *id).await else {
                return Err(sprig_approvals::store::not_found(&id_of(*id)));
            };
            let caller = caller_for(ts, cat, &*p, &rec);
            if !may_act(&rec, &caller, &[]) && rec.level <= Level::Tenant {
                return Err(SprigError::new(
                    SprigCode::ApprovalDenied,
                    format!(
                        "{} may not act on recommendation {} (level {}): it needs ALTER{} on {}",
                        caller.name,
                        rec.id,
                        rec.level,
                        if rec.kind.is_index() { " or INDEX" } else { "" },
                        rec.subject.display()
                    ),
                ));
            }
            let (action, mut body) = match stmt {
                RecStmt::Approve { reason, .. } => (
                    "approve",
                    serde_json::json!({ "reason": reason.clone().unwrap_or_default() }),
                ),
                RecStmt::Ignore {
                    days,
                    forever,
                    reason,
                    ..
                } => (
                    "ignore",
                    serde_json::json!({
                        "reason": reason.clone().unwrap_or_default(),
                        "for_days": days,
                        "forever": forever,
                    }),
                ),
                RecStmt::Snooze { hours, .. } => ("snooze", serde_json::json!({ "hours": hours })),
                RecStmt::Show { .. } => unreachable!("handled above"),
            };
            if let serde_json::Value::Object(m) = &mut body {
                m.insert(
                    "caller".into(),
                    serde_json::to_value(&caller).unwrap_or_default(),
                );
            }
            let admin = ADMIN.get().ok_or_else(|| {
                SprigError::new(
                    SprigCode::CatalogUnavailable,
                    "this gateway can't reach a controller",
                )
            })?;
            let reply = admin
                .forward_internal(
                    &hyper::Method::POST,
                    &format!("/admin/v1/recommendations/{}:{action}", rec.id),
                    Bytes::from(body.to_string()),
                )
                .await?;
            if reply.status >= 400 {
                let v: serde_json::Value = serde_json::from_slice(&reply.body).unwrap_or_default();
                let code = v
                    .get("code")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                let msg = v
                    .get("message")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("the recommendations service refused")
                    .to_owned();
                let code = SprigCode::ALL
                    .iter()
                    .copied()
                    .find(|c| u64::from(c.number()) == code)
                    .unwrap_or(SprigCode::Internal);
                return Err(SprigError::new(code, msg));
            }
            Ok(RecAnswer::Ok)
        }
    }
}

#[allow(dead_code)]
fn _locator_is_used(l: &dyn CatalogLocator) -> &dyn CatalogLocator {
    l
}
