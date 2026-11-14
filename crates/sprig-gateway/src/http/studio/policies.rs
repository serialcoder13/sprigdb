//! `Policies.TestAs` (row-level security's "view as this user"): one
//! read-only statement run as another user of the caller's tenant or a
//! descendant, for Studio's tester.
//!
//! `POST /v1/policies:testAs` `{tenant?, user, host?, database, sql}`:
//!
//! 1. `studio.view_as` on for the user's tenant, the caller holds
//!    `SPRIG_TENANT_ADMIN` and the tenant is the caller's or below it, and
//!    the user doesn't administer the caller's tenant (as for
//!    impersonation) — else 39930. Not from an impersonation session.
//! 2. The text is one read-only statement (`SELECT`, `WITH`, `TABLE`,
//!    `VALUES`, `SHOW`; no `INTO`, `FOR UPDATE`, `FOR SHARE`, `LOCK IN SHARE
//!    MODE`) — else 39931.
//! 3. The statement runs through the executor as the user's own principal
//!    (the same path as an impersonation session's statements): the user's
//!    privileges, policies and session functions apply. Because only tenant
//!    administrators may test, and an administrator's own view of a tenant
//!    they administer is the whole tenant, the result is never wider than
//!    the caller's view (step 4 of the chapter's algorithm).
//! 4. Capped at `studio.max_result_rows` and `studio.statement_timeout_ms`
//!    at the user's tenant and database.
//! 5. Audited as `studio.view_as` (caller, user, tenant, rows, a hash of
//!    the text).
//!
//! 6. Optional `claims` and `attributes` (JSON objects) stand in for the
//!    user's token claims and over its attributes while the statement runs
//!    (`SPRIG_CLAIM`, `SPRIG_USER_ATTR`), and the response's `policies`
//!    lists each policy the rewrite applied, with its predicate as bound
//!    for this run: Studio shows it as a column of the policy table.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use hyper::body::Incoming;
use hyper::{Request, Response};
use serde_json::{Value, json};
use sprig_auth::LoginSource;
use sprig_auth::mfa::AccountRef;
use sprig_common::ids::TenantId;
use sprig_common::{SprigCode, SprigError};
use tokio::sync::mpsc;
use tonic::body::Body;

use super::super::sql::{CHANNEL_DEPTH, Caller, Caps, SqlCall, collect};
use super::super::{ConnInfo, Core};
use super::{
    hdr, invalid, json_response, parse_json, read_body, rows_of, run_sql, tenant_tree, text,
};

fn denied(why: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::StudioViewAsDenied, why)
}

/// The words of `sql` outside strings, quoted names and comments,
/// upper-cased.
fn words(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let cs: Vec<char> = sql.chars().collect();
    let mut i = 0;
    let flush = |cur: &mut String, out: &mut Vec<String>| {
        if !cur.is_empty() {
            out.push(cur.to_ascii_uppercase());
            cur.clear();
        }
    };
    while i < cs.len() {
        let c = cs[i];
        let d = cs.get(i + 1).copied();
        if c == '-' && d == Some('-') || c == '#' {
            flush(&mut cur, &mut out);
            while i < cs.len() && cs[i] != '\n' {
                i += 1;
            }
            continue;
        }
        if c == '/' && d == Some('*') {
            flush(&mut cur, &mut out);
            i += 2;
            while i + 1 < cs.len() && !(cs[i] == '*' && cs[i + 1] == '/') {
                i += 1;
            }
            i += 2;
            continue;
        }
        if c == '\'' || c == '"' || c == '`' {
            flush(&mut cur, &mut out);
            i += 1;
            while i < cs.len() && cs[i] != c {
                if cs[i] == '\\' && c != '`' {
                    i += 1;
                }
                i += 1;
            }
            i += 1;
            out.push("?".into());
            continue;
        }
        if c.is_alphanumeric() || c == '_' || c == '$' {
            cur.push(c);
        } else {
            flush(&mut cur, &mut out);
            if c == ';' {
                out.push(";".into());
            }
        }
        i += 1;
    }
    flush(&mut cur, &mut out);
    out
}

/// Whether `sql` is one read-only statement (step 2).
///
/// # Errors
/// 39931 naming why not.
pub fn read_only(sql: &str) -> Result<(), SprigError> {
    let not = |why: &str| SprigError::new(SprigCode::StudioViewAsNotReadOnly, why.to_owned());
    let mut w = words(sql);
    while w.last().is_some_and(|x| x == ";") {
        w.pop();
    }
    if w.is_empty() {
        return Err(not("there is no statement to test"));
    }
    if w.iter().any(|x| x == ";") {
        return Err(not("the tester runs one statement at a time"));
    }
    if !matches!(
        w[0].as_str(),
        "SELECT" | "WITH" | "TABLE" | "VALUES" | "SHOW"
    ) {
        return Err(not(
            "the tester runs only reads: SELECT, WITH, TABLE, VALUES or SHOW",
        ));
    }
    for (i, x) in w.iter().enumerate() {
        let next = w.get(i + 1).map_or("", String::as_str);
        let writes = match x.as_str() {
            "INTO" => true,
            "FOR" => matches!(next, "UPDATE" | "SHARE"),
            "LOCK" => next == "IN",
            "INSERT" | "UPDATE" | "DELETE" | "REPLACE" if w[0] == "WITH" => true,
            _ => false,
        };
        if writes {
            return Err(not(
                "the statement writes or locks rows (INTO, FOR UPDATE, FOR SHARE, LOCK IN SHARE MODE)",
            ));
        }
    }
    Ok(())
}

/// `POST /v1/policies:testAs`.
///
/// # Errors
/// As the module docs say.
#[allow(clippy::too_many_lines)] // the checks in the module docs' order, then the run
pub async fn test_as(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let auth = hdr(&req, "authorization");
    super::mfa::not_impersonating(auth.as_deref(), "test as another user")?;
    let caller = core.caller(info, auth, None, hdr(&req, "host")).await?;
    let b = parse_json(&read_body(req).await?)?;
    let me = caller.principal.clone();
    let home = me.tenant_name.clone().unwrap_or_default();
    let tenant = Some(text(&b, "tenant"))
        .filter(|t| !t.is_empty())
        .unwrap_or_else(|| home.clone());
    let user = text(&b, "user");
    let host = text(&b, "host");
    let database = Some(text(&b, "database"))
        .filter(|d| !d.is_empty())
        .unwrap_or_else(|| "-".to_owned());
    let sql = text(&b, "sql");
    if user.is_empty() {
        return Err(invalid("user is required"));
    }
    read_only(&sql)?;
    let grants = grant_lines(core, &caller).await?;
    if !super::grants::Privileges::of(&grants).tenant_admin {
        return Err(denied(
            "the tester needs SPRIG_TENANT_ADMIN over the user's tenant",
        ));
    }
    let tree = tenant_tree(core, &me, true).await;
    let node = tree
        .as_array()
        .into_iter()
        .flatten()
        .find(|n| n["name"].as_str() == Some(tenant.as_str()))
        .cloned()
        .ok_or_else(|| denied(format!("tenant {tenant} is not yours or below yours")))?;
    let tid = node["id"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .map(TenantId);
    let on = core
        .studio
        .tenant_get(&sprig_studio::settings::VIEW_AS, tid, None);
    if on.as_ref() != "on" {
        return Err(denied(format!("studio.view_as is off for tenant {tenant}")));
    }
    let login = core
        .studio
        .login
        .as_ref()
        .ok_or_else(|| invalid("the tester needs catalog logins"))?;
    let LoginSource::Catalog(cl) = login.source() else {
        return Err(invalid("the tester needs catalog logins"));
    };
    let target = sprig_auth::mfa::account_principal(
        cl,
        &tenant,
        AccountRef::Name(&user, (!host.is_empty()).then_some(host.as_str())),
        info.peer.ip(),
        info.tls.is_some(),
        SystemTime::now(),
    )
    .await?;
    if target.tenant == me.tenant && target.user_id == me.user_id {
        return Err(denied(
            "test as another user; you see your own view already",
        ));
    }
    let as_user = Caller {
        principal: target.clone(),
        ..caller.clone()
    };
    if target.tenant == me.tenant {
        let theirs = grant_lines(core, &as_user).await?;
        if super::grants::Privileges::of(&theirs).tenant_admin {
            return Err(denied(format!(
                "{user} administers your tenant: administrators can't be tested as"
            )));
        }
    }
    let db = (database != "-").then_some(database.as_str());
    let max_rows = core
        .studio
        .tenant_get(&sprig_studio::settings::MAX_RESULT_ROWS, tid, db);
    let timeout_ms = core
        .studio
        .tenant_get(&sprig_studio::settings::STATEMENT_TIMEOUT_MS, tid, db);
    let caps = Caps {
        max_rows,
        max_bytes: 0,
        timeout: Some(Duration::from_millis(timeout_ms)),
    };
    let (tx, rx) = mpsc::channel(CHANNEL_DEPTH);
    let engine = Arc::clone(&core.engine);
    let c2 = as_user.clone();
    let call = SqlCall {
        database: database.clone(),
        sql: sql.clone(),
        multi: false,
    };
    let obj = |k: &str| b.get(k).filter(|v| v.is_object()).cloned();
    let sim = Arc::new(crate::tenant::rls::Simulated {
        claims: obj("claims").map(Arc::new),
        attrs: obj("attributes"),
        ..Default::default()
    });
    let simulated = sim.claims.is_some() || sim.attrs.is_some();
    let scoped = Arc::clone(&sim);
    tokio::spawn(crate::tenant::rls::SIMULATED.scope(scoped, async move {
        engine.run_capped(&c2, call, None, caps, tx).await;
    }));
    let r = collect(
        rx,
        core.engine.settings().max_buffered,
        core.engine.charge(&caller),
    )
    .await?;
    let first = r.results.iter().find(|x| !x.columns.is_empty());
    let columns: Vec<Value> = first
        .map(|x| x.columns.iter().map(|c| json!(c.name)).collect())
        .unwrap_or_default();
    let rows = rows_of(&r);
    let truncated = first.is_some_and(|x| x.truncated);
    // The statement's fingerprint for the audit: a hash, never the text.
    let digest = format!("{:016x}", twox_hash::XxHash3_64::oneshot(sql.as_bytes()));
    tracing::info!(target: "sprig_audit", event = "studio.view_as",
        caller = %format!("{home}.{}", me.user), user = %format!("{tenant}.{}", target.user),
        tenant = %tenant, database = %database, statement = %digest, rows = rows.len(),
        simulated, "a statement was run as another user");
    let mut seen = sim
        .seen
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    seen.sort();
    seen.dedup();
    let policies: Vec<Value> = seen
        .into_iter()
        .map(|(table, policy, mode, predicate)| {
            json!({"table": table, "policy": policy, "mode": mode, "predicate": predicate})
        })
        .collect();
    Ok(json_response(
        200,
        &json!({
            "as": format!("{tenant}.{}", target.user),
            "columns": columns,
            "rows": rows,
            "truncated": truncated,
            "max_rows": max_rows,
            "policies": policies,
        }),
    ))
}

async fn grant_lines(core: &Arc<Core>, caller: &Caller) -> Result<Vec<String>, SprigError> {
    let r = run_sql(core, caller, "-", "SHOW GRANTS".to_owned()).await?;
    Ok(rows_of(&r)
        .into_iter()
        .filter_map(|row| row.into_iter().next().flatten())
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_one_read_only_statement_is_tested() {
        for ok in [
            "SELECT * FROM orders",
            "select id from t where note = 'for update; into x'",
            "WITH x AS (SELECT 1) SELECT * FROM x;",
            "SHOW TABLES",
            "TABLE orders",
            "SELECT `into` FROM t -- FOR UPDATE",
        ] {
            assert!(read_only(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            "UPDATE t SET a = 1",
            "DELETE FROM t",
            "SELECT 1; SELECT 2",
            "SELECT * FROM t FOR UPDATE",
            "SELECT * FROM t FOR SHARE",
            "SELECT * FROM t LOCK IN SHARE MODE",
            "SELECT a INTO @x FROM t",
            "SELECT a FROM t INTO OUTFILE '/tmp/x'",
            "WITH d AS (SELECT 1) DELETE FROM t",
            "CALL p()",
            "SET @a = 1",
        ] {
            let e = read_only(bad).unwrap_err();
            assert_eq!(e.code(), Some(SprigCode::StudioViewAsNotReadOnly), "{bad}");
        }
    }
}
