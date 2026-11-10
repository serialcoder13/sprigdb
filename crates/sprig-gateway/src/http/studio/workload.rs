//! `Workload.ListFingerprints` and `Workload.Series` (chapter 37, "APIs
//! for Studio"; chapter 44, "Automatic indexing": the Fingerprints and
//! Before-and-after views), for the caller's own tenant.
//!
//! | Route | Does |
//! |---|---|
//! | `GET /v1/workload/fingerprints?hours=&limit=&source=` | Top fingerprints by total time over the last `hours` (24): runs, errors, p50, p95, max, rows examined per row returned, unsargable predicates, and `caller` (`studio` for Studio's own runs) |
//! | `GET /v1/workload/fingerprints/{id}/series?days=` | One fingerprint per bucket over the last `days` (7): runs, p50, p95, total |
//!
//! Both read chapter 12's tables in the tenant's catalog directly (the
//! gateway already reaches the catalog writer), so they need no controller
//! call. A fingerprint is shown only when the caller has a privilege on its
//! database (any grant naming it, or `*.*`) or is a tenant administrator:
//! normalized text carries no literals, but names other databases' tables.

#![allow(clippy::cast_precision_loss)] // milliseconds for display

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hyper::Response;
use serde_json::{Value, json};
use sprig_common::SprigError;
use sprig_workload::store::WorkloadTables;
use sprig_workload::window::FpWindowStats;
use tonic::body::Body;

use super::super::Core;
use super::super::sql::Caller;
use super::{grants, json_response, rows_of, run_sql};

/// Fingerprints listed at most.
pub const MAX_LIMIT: usize = 500;

fn ms_text(t: SystemTime) -> String {
    t.duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .to_string()
}

fn secs(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

fn query_u64(query: &str, key: &str, default: u64) -> u64 {
    query
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .find(|(k, _)| *k == key)
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(default)
}

fn query_str<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .find(|(k, _)| *k == key)
        .map(|(_, v)| v)
}

/// The databases the caller may see workload of: `None` for all.
pub(super) async fn visible(
    core: &Arc<Core>,
    caller: &Caller,
) -> Result<Option<Vec<String>>, SprigError> {
    let r = run_sql(core, caller, "-", "SHOW GRANTS".to_owned()).await?;
    let lines: Vec<String> = rows_of(&r)
        .into_iter()
        .filter_map(|row| row.into_iter().next().flatten())
        .collect();
    let privs = grants::Privileges::of(&lines);
    // `GRANT USAGE ON *.*` (every account has it) grants nothing.
    let global = |l: &String| l.contains(" ON *.* ") && !l.starts_with("GRANT USAGE ON ");
    if privs.tenant_admin || lines.iter().any(global) {
        return Ok(None);
    }
    let mut dbs = Vec::new();
    for l in lines.iter().filter(|l| !l.starts_with("GRANT USAGE ON ")) {
        if let Some((_, rest)) = l.split_once(" ON ")
            && let Some((db, _)) = rest.split_once('.')
        {
            dbs.push(db.trim_matches('`').replace("``", "`").to_lowercase());
        }
    }
    Ok(Some(dbs))
}

fn caller_label(w: &FpWindowStats) -> Value {
    match w.caller {
        Some(c) if c.is_studio() => json!("studio"),
        Some(c) => json!(format!("function:{}", c.function_id)),
        None => Value::Null,
    }
}

fn unsargable(w: &FpWindowStats) -> Vec<Value> {
    serde_json::to_value(&w.shape.predicates)
        .ok()
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_default()
        .into_iter()
        .filter(|p| {
            p.get("op")
                .and_then(Value::as_str)
                .is_some_and(|o| o.contains("unsargable"))
        })
        .collect()
}

fn round(x: f64) -> f64 {
    (x * 1000.0).round() / 1000.0
}

/// The tenant's catalog connection and tables.
async fn tables(
    core: &Arc<Core>,
    caller: &Caller,
) -> Result<
    (
        mysql_async::Conn,
        WorkloadTables,
        Arc<sprig_catalog::TenantCatalog>,
    ),
    SprigError,
> {
    let (Some(svc), Some(tid)) = (&core.studio.tenants, caller.principal.tenant) else {
        return Err(super::invalid("workload views need a tenant login"));
    };
    let ep = sprig_catalog::CatalogLocator::writer(&*svc.locator, tid).await?;
    let conn = ep.pool.get_conn().await.map_err(|e| {
        SprigError::new(
            sprig_common::SprigCode::CatalogUnavailable,
            "the tenant's catalog is unreachable",
        )
        .with_source(e)
    })?;
    let cat = svc
        .cache
        .cached(tid)
        .ok_or_else(|| super::invalid("the tenant's catalog isn't loaded yet; retry"))?;
    Ok((conn, WorkloadTables::new(ep.store.schema()), cat))
}

/// `Workload.ListFingerprints`.
pub(super) async fn list(
    core: &Arc<Core>,
    caller: &Caller,
    query: &str,
) -> Result<Response<Body>, SprigError> {
    let hours = query_u64(query, "hours", 24).clamp(1, 24 * 90);
    let limit = usize::try_from(query_u64(query, "limit", 50))
        .unwrap_or(50)
        .clamp(1, MAX_LIMIT);
    let source = query_str(query, "source").unwrap_or("all");
    let seen = visible(core, caller).await?;
    let (mut conn, t, cat) = tables(core, caller).await?;
    let to = SystemTime::now() + Duration::from_secs(3600);
    // From the start of the hour: older buckets are rolled up to hours
    // (chapter 12), so a window cut mid-hour would miss them.
    let from_s = secs(SystemTime::now()).saturating_sub(hours * 3600) / 3600 * 3600;
    let from = UNIX_EPOCH + Duration::from_secs(from_s);
    let window = t.window(&mut conn, cat.tenant, from, to).await?;
    let ids: Vec<_> = window.iter().map(|w| w.id).collect();
    let rows: std::collections::HashMap<u64, _> = t
        .fingerprints(&mut conn, &ids)
        .await?
        .into_iter()
        .map(|r| (r.id.0, r))
        .collect();
    let mut list: Vec<(f64, Value)> = window
        .iter()
        .filter(|w| match source {
            "studio" => w.caller.is_some_and(|c| c.is_studio()),
            "client" => !w.caller.is_some_and(|c| c.is_studio()),
            _ => true,
        })
        .filter_map(|w| {
            let row = rows.get(&w.id.0)?;
            let db = cat.databases.get(row.db_id).map(|d| d.name.clone());
            if let Some(allowed) = &seen {
                let key = db.as_deref().unwrap_or("").to_lowercase();
                if !allowed.contains(&key) {
                    return None;
                }
            }
            let total = w.total_ms();
            Some((
                total,
                json!({
                    "id": format!("{:016x}", w.id.0),
                    "kind": format!("{:?}", w.kind).to_lowercase(),
                    "stmt_type": row.stmt_type,
                    "db": db,
                    "text": &*w.text,
                    "caller": caller_label(w),
                    "runs": w.runs(),
                    "errors": w.stats.error_count,
                    "total_ms": round(total),
                    "p50_ms": round(w.quantile_ms(0.5)),
                    "p95_ms": round(w.p95_ms()),
                    "max_ms": round(w.stats.max_us as f64 / 1000.0),
                    "rows_returned": w.stats.rows_returned,
                    "examined_per_returned": w.examined_per_returned().map(round),
                    "unsargable": unsargable(w),
                    "first_seen_ms": ms_text(row.first_seen),
                    "last_seen_ms": ms_text(row.last_seen),
                }),
            ))
        })
        .collect();
    list.sort_by(|a, b| b.0.total_cmp(&a.0));
    let out: Vec<Value> = list.into_iter().take(limit).map(|(_, v)| v).collect();
    Ok(json_response(
        200,
        &json!({ "hours": hours, "fingerprints": out }),
    ))
}

/// `Workload.Series`.
pub(super) async fn series(
    core: &Arc<Core>,
    caller: &Caller,
    id: &str,
    query: &str,
) -> Result<Response<Body>, SprigError> {
    let fp = u64::from_str_radix(id, 16)
        .map_err(|_| super::invalid("a fingerprint id is 16 hex digits"))?;
    let days = query_u64(query, "days", 7).clamp(1, 90);
    let seen = visible(core, caller).await?;
    let (mut conn, t, cat) = tables(core, caller).await?;
    let fid = sprig_common::ids::Fingerprint(fp);
    let row = t
        .fingerprints(&mut conn, &[fid])
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| {
            SprigError::standard(1146, *b"42S02", "no such fingerprint in this tenant")
        })?;
    if let Some(allowed) = &seen {
        let db = cat
            .databases
            .get(row.db_id)
            .map(|d| d.name.to_lowercase())
            .unwrap_or_default();
        if !allowed.contains(&db) {
            return Err(SprigError::standard(
                1146,
                *b"42S02",
                "no such fingerprint in this tenant",
            ));
        }
    }
    let to = secs(SystemTime::now()) + 3600;
    let from = to.saturating_sub(days * 86_400 + 3600) / 3600 * 3600;
    let buckets: Vec<Value> = t
        .series(&mut conn, fid, from, to)
        .await?
        .into_iter()
        .map(|(start, s)| {
            let q = |x: f64| {
                s.latency_hist
                    .quantile_us(x)
                    .map_or(0.0, |us| round(us as f64 / 1000.0))
            };
            json!({
                "start": start,
                "runs": s.exec_count,
                "errors": s.error_count,
                "p50_ms": q(0.5),
                "p95_ms": q(0.95),
                "total_ms": round(s.total_us as f64 / 1000.0),
            })
        })
        .collect();
    Ok(json_response(
        200,
        &json!({ "id": id, "text": &*row.text, "days": days, "buckets": buckets }),
    ))
}
