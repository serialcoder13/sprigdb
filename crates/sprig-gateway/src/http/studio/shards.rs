//! The shards view's routes (the `Shards` service for the caller's own
//! tenant): table groups with their keys and members, each group's shard
//! map, and the sharding advisor's runs.
//!
//! | Route | Does |
//! |---|---|
//! | `GET /v1/shards` | Table groups (`table_groups`) with their members and key columns (`group_members`), read from the tenant's catalog, and each group's map (ranges, clusters, states, epochs, holding jobs) from the controller's `GET /admin/v1/tenants/{t}/shards` |
//! | `GET /v1/shards/advisor` | The advisor's last 50 runs (`advisor_runs`): cause, mode, status, verdict, scores, confidence, times |
//! | `GET /v1/shards/advisor/{run}` | One run with its design, evidence and plan, from the controller's `GET /admin/v1/tenants/{t}/advisor?run=` |
//!
//! Members show only in databases the caller holds a grant on (or all, for
//! `*.*` and `SPRIG_TENANT_ADMIN`), as the workload views do; a group with
//! no member shown is left out. Advisor runs span every database, so they
//! need `SPRIG_TENANT_ADMIN` or a grant on `*.*` (1227 otherwise). The view
//! is read-only: splitting, merging, moving and applying a run are operator
//! actions (`sprigctl shard`, `sprigctl advisor`, operator mode).
//!
//! Shard maps come over the gateway's internal credential; without it the
//! groups still show, with `maps_error` saying why.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use hyper::{Method, Response};
use mysql_async::prelude::Queryable as _;
use serde_json::{Value, json};
use sprig_common::ids::{DbId, ObjectId};
use sprig_common::{SprigCode, SprigError};
use tonic::body::Body;

use super::super::Core;
use super::super::sql::Caller;
use super::{invalid, json_response, passthrough, workload};

/// Advisor runs listed.
const RUNS: usize = 50;

fn denied() -> SprigError {
    SprigError::standard(
        1227,
        *b"42000",
        "Access denied; you need (at least one of) the SPRIG_TENANT_ADMIN privilege(s) for this operation",
    )
}

async fn catalog(
    core: &Arc<Core>,
    caller: &Caller,
) -> Result<(mysql_async::Conn, String, Arc<sprig_catalog::TenantCatalog>), SprigError> {
    let (Some(svc), Some(tid)) = (&core.studio.tenants, caller.principal.tenant) else {
        return Err(invalid("the shards view needs a tenant login"));
    };
    let ep = sprig_catalog::CatalogLocator::writer(&*svc.locator, tid).await?;
    let conn = ep.pool.get_conn().await.map_err(|e| {
        SprigError::new(
            SprigCode::CatalogUnavailable,
            "the tenant's catalog is unreachable",
        )
        .with_source(e)
    })?;
    let cat = svc
        .cache
        .cached(tid)
        .ok_or_else(|| invalid("the tenant's catalog isn't loaded yet; retry"))?;
    Ok((conn, ep.store.schema().to_owned(), cat))
}

fn json_col(s: Option<String>) -> Value {
    s.and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or(Value::Null)
}

fn missing_table(e: &mysql_async::Error) -> bool {
    matches!(e, mysql_async::Error::Server(s) if s.code == 1146)
}

fn db_err(what: &str, e: &mysql_async::Error) -> SprigError {
    SprigError::new(SprigCode::CatalogUnavailable, format!("{what}: {e}"))
}

/// One member as the view shows it.
#[derive(Debug, Clone, PartialEq)]
pub struct Member {
    /// The group.
    pub group: u32,
    /// The logical database.
    pub db: String,
    /// The table.
    pub table: String,
    /// The member's JSON.
    pub json: Value,
}

/// Groups and members as JSON, members filtered to `seen` databases (all
/// when `None`); groups without a shown member are left out, unless every
/// database is visible.
#[must_use]
pub fn assemble(
    groups: Vec<(u32, Value)>,
    members: Vec<Member>,
    seen: Option<&[String]>,
    maps: &BTreeMap<u32, Value>,
) -> Vec<Value> {
    let mut by_group: BTreeMap<u32, Vec<Value>> = BTreeMap::new();
    for m in members {
        if seen.is_some_and(|s| !s.iter().any(|d| d.eq_ignore_ascii_case(&m.db))) {
            continue;
        }
        by_group.entry(m.group).or_default().push(m.json);
    }
    groups
        .into_iter()
        .filter_map(|(id, mut g)| {
            let ms = by_group.remove(&id).unwrap_or_default();
            if ms.is_empty() && seen.is_some() {
                return None;
            }
            g["members"] = Value::Array(ms);
            if let Some(m) = maps.get(&id) {
                g["map"] = m.clone();
            }
            Some(g)
        })
        .collect()
}

/// The controller's maps by group: `{group: {version, state, shards}}`.
fn maps_by_group(v: &Value) -> BTreeMap<u32, Value> {
    v.as_array()
        .into_iter()
        .flatten()
        .filter_map(|g| {
            let id = u32::try_from(g.get("group")?.as_u64()?).ok()?;
            Some((
                id,
                json!({
                    "version": g.get("map_version").cloned().unwrap_or(Value::Null),
                    "state": g.get("state").cloned().unwrap_or(Value::Null),
                    "shards": g.get("shards").cloned().unwrap_or(json!([])),
                }),
            ))
        })
        .collect()
}

/// The maps by group from the controller (the router's maps live in
/// etcd), or why they couldn't be read.
async fn maps(core: &Arc<Core>, tenant: &str) -> (BTreeMap<u32, Value>, Value) {
    match core
        .admin
        .forward_internal(
            &Method::GET,
            &format!("/admin/v1/tenants/{tenant}/shards"),
            Bytes::new(),
        )
        .await
    {
        Ok(r) if r.status == 200 => {
            let v: Value = serde_json::from_slice(&r.body).unwrap_or(Value::Null);
            (maps_by_group(&v), Value::Null)
        }
        Ok(r) => (
            BTreeMap::new(),
            json!(format!(
                "the controller answered {}: {}",
                r.status,
                String::from_utf8_lossy(&r.body)
                    .chars()
                    .take(300)
                    .collect::<String>()
            )),
        ),
        Err(e) => (BTreeMap::new(), json!(e.to_string())),
    }
}

/// A `table_groups` row as read.
type GroupRow = (
    u32,
    String,
    String,
    Option<String>,
    String,
    Option<u32>,
    String,
    Option<u64>,
    Option<String>,
);

/// A `group_members` row as read.
type MemberRow = (
    u64,
    u32,
    u32,
    String,
    Option<String>,
    Option<u64>,
    Option<String>,
    String,
    u8,
);

/// `GET /v1/shards`.
async fn groups(core: &Arc<Core>, caller: &Caller, tenant: &str) -> Result<Value, SprigError> {
    let seen = workload::visible(core, caller).await?;
    let (mut conn, schema, cat) = catalog(core, caller).await?;
    let gsql = format!(
        "SELECT group_id, kind, name, CAST(key_spec AS CHAR), state, min_shards, origin, \
         advisor_run_id, DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%s') \
         FROM `{schema}`.table_groups ORDER BY group_id"
    );
    let rows: Vec<GroupRow> = match conn.query(gsql).await {
        Ok(r) => r,
        Err(e) if missing_table(&e) => Vec::new(),
        Err(e) => return Err(db_err("table groups", &e)),
    };
    let groups: Vec<(u32, Value)> = rows
        .into_iter()
        .map(|(id, kind, name, key, state, min, origin, run, updated)| {
            (
                id,
                json!({
                    "group": id, "kind": kind, "name": name, "key_spec": json_col(key),
                    "state": state, "min_shards": min, "origin": origin,
                    "advisor_run": run.map(|r| r.to_string()), "updated_at": updated,
                }),
            )
        })
        .collect();
    let msql = format!(
        "SELECT object_id, group_id, db_id, routing, CAST(key_columns AS CHAR), \
         parent_object_id, parent_fk, rk_state, pinned FROM `{schema}`.group_members \
         ORDER BY group_id, db_id, object_id"
    );
    let rows: Vec<MemberRow> = match conn.query(msql).await {
        Ok(r) => r,
        Err(e) if missing_table(&e) => Vec::new(),
        Err(e) => return Err(db_err("group members", &e)),
    };
    drop(conn);
    let name_of = |o: u64| {
        cat.objects
            .get(ObjectId(o))
            .map(|r| (r.db_id, r.name.clone()))
    };
    let db_name = |d: DbId| cat.databases.get(d).map(|r| r.name.clone());
    let members: Vec<Member> = rows
        .into_iter()
        .filter_map(|(oid, gid, dbid, routing, keys, parent, fk, rk, pinned)| {
            let db = db_name(DbId(dbid))?;
            let (_, table) = name_of(oid)?;
            let parent = parent.and_then(name_of).map(|(_, n)| n);
            Some(Member {
                group: gid,
                db: db.clone(),
                table: table.clone(),
                json: json!({
                    "db": db, "table": table, "routing": routing,
                    "key_columns": json_col(keys), "parent": parent, "parent_fk": fk,
                    "rekey_state": rk, "pinned": pinned != 0,
                }),
            })
        })
        .collect();
    let (maps, maps_error) = maps(core, tenant).await;
    let out = assemble(groups, members, seen.as_deref(), &maps);
    Ok(json!({ "groups": out, "maps_error": maps_error, "all_databases": seen.is_none() }))
}

/// `GET /v1/shards/advisor`: the last runs.
async fn runs(core: &Arc<Core>, caller: &Caller) -> Result<Value, SprigError> {
    if workload::visible(core, caller).await?.is_some() {
        return Err(denied());
    }
    let (mut conn, schema, _) = catalog(core, caller).await?;
    let sql = format!(
        "SELECT run_id, cause, mode, status, verdict, units_sampled, current_score, best_score, \
         runner_up_score, confidence, decision_id, approved_by, error, \
         DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s'), DATE_FORMAT(finished_at, '%Y-%m-%d %H:%i:%s') \
         FROM `{schema}`.advisor_runs ORDER BY run_id DESC LIMIT {RUNS}"
    );
    let rows: Vec<mysql_async::Row> = match conn.query(sql).await {
        Ok(r) => r,
        Err(e) if missing_table(&e) => Vec::new(),
        Err(e) => return Err(db_err("advisor runs", &e)),
    };
    let s = |r: &mysql_async::Row, i: usize| -> Value {
        r.get_opt::<Option<String>, _>(i)
            .and_then(Result::ok)
            .flatten()
            .map_or(Value::Null, Value::String)
    };
    let f = |r: &mysql_async::Row, i: usize| -> Value {
        r.get_opt::<Option<f64>, _>(i)
            .and_then(Result::ok)
            .flatten()
            .map_or(Value::Null, |x| json!((x * 1000.0).round() / 1000.0))
    };
    let out: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "run": s(r, 0), "cause": s(r, 1), "mode": s(r, 2), "status": s(r, 3),
                "verdict": s(r, 4), "units_sampled": s(r, 5), "current_score": f(r, 6),
                "best_score": f(r, 7), "runner_up_score": f(r, 8), "confidence": f(r, 9),
                "decision": s(r, 10), "approved_by": s(r, 11), "error": s(r, 12),
                "created_at": s(r, 13), "finished_at": s(r, 14),
            })
        })
        .collect();
    Ok(json!({ "runs": out }))
}

/// Answers `/v1/shards...` for `caller`.
///
/// # Errors
/// 1227 for advisor runs without tenant-wide rights; catalog errors.
pub(super) async fn route(
    core: &Arc<Core>,
    caller: &Caller,
    path: &str,
) -> Result<Response<Body>, SprigError> {
    let Some(tenant) = caller.principal.tenant_name.clone() else {
        return Err(invalid("the shards view needs a tenant login"));
    };
    let rest = path
        .trim_start_matches("/v1/shards")
        .trim_start_matches('/');
    match rest.split('/').collect::<Vec<_>>().as_slice() {
        [""] => Ok(json_response(200, &groups(core, caller, &tenant).await?)),
        ["advisor"] => Ok(json_response(200, &runs(core, caller).await?)),
        ["advisor", run] if !run.is_empty() && run.bytes().all(|b| b.is_ascii_digit()) => {
            if workload::visible(core, caller).await?.is_some() {
                return Err(denied());
            }
            let r = core
                .admin
                .forward_internal(
                    &Method::GET,
                    &format!("/admin/v1/tenants/{tenant}/advisor?run={run}"),
                    Bytes::new(),
                )
                .await?;
            Ok(passthrough(r.status, r.body))
        }
        _ => Err(invalid(format!("no route GET {path}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(group: u32, db: &str, table: &str) -> Member {
        Member {
            group,
            db: db.into(),
            table: table.into(),
            json: json!({"db": db, "table": table}),
        }
    }

    #[test]
    fn members_follow_the_callers_databases() {
        let groups = vec![
            (1, json!({"group": 1, "name": "customers"})),
            (2, json!({"group": 2, "name": "logs"})),
            (3, json!({"group": 3, "name": "empty"})),
        ];
        let members = vec![
            member(1, "shop", "customers"),
            member(1, "shop", "orders"),
            member(2, "ops", "logs"),
        ];
        let mut maps = BTreeMap::new();
        maps.insert(
            1,
            json!({"version": 4, "shards": [{"shard": 1}, {"shard": 2}]}),
        );
        // An administrator sees every group, the empty one included.
        let all = assemble(groups.clone(), members.clone(), None, &maps);
        assert_eq!(all.len(), 3);
        assert_eq!(all[0]["members"].as_array().unwrap().len(), 2);
        assert_eq!(all[0]["map"]["version"], 4);
        assert!(all[1].get("map").is_none());
        // A user with grants on shop only sees the shop group.
        let seen = vec!["SHOP".to_owned()];
        let mine = assemble(groups, members, Some(&seen), &maps);
        assert_eq!(mine.len(), 1);
        assert_eq!(mine[0]["name"], "customers");
    }

    #[test]
    fn maps_are_keyed_by_group() {
        let v = json!([
            {"group": 3, "name": "c", "state": "active", "map_version": 9,
             "shards": [{"shard": 7, "lo": "0", "hi": null, "cluster": "dc-a-c1",
                         "state": "serving", "epoch": 2, "job": null}]},
            {"name": "no id"}
        ]);
        let m = maps_by_group(&v);
        assert_eq!(m.len(), 1);
        assert_eq!(m[&3]["version"], 9);
        assert_eq!(m[&3]["shards"][0]["cluster"], "dc-a-c1");
    }
}
