//! Studio's approvals inbox (chapter 52, "Studio"; chapter 44):
//! `/v1/recommendations...` to the DC leader's `Recommendations` service,
//! with the caller as a claim the controller checks against every item:
//! the tenant, `SPRIG_TENANT_ADMIN`, and the `ALTER` and `INDEX` grants
//! read from `SHOW GRANTS`.

use std::fmt::Write as _;
use std::sync::Arc;

use bytes::Bytes;
use hyper::body::Incoming;
use hyper::{Method, Request, Response};
use serde_json::{Value, json};
use sprig_approvals::{Caller, GrantClaim};
use sprig_common::SprigError;
use tonic::body::Body;

use super::{hdr, invalid, parse_json, passthrough, read_body, rows_of, run_sql};
use crate::http::sql::Caller as SqlCaller;
use crate::http::{ConnInfo, Core};

/// The caller's `ALTER` and `INDEX` grants and administrator right, from
/// `SHOW GRANTS` lines.
#[must_use]
#[allow(clippy::similar_names)] // privs, priv lists
pub fn claims(lines: &[String]) -> (bool, Vec<GrantClaim>) {
    let mut admin = false;
    let mut out = Vec::new();
    for line in lines {
        let u = line.trim();
        let Some(rest) = u
            .strip_prefix("GRANT ")
            .or_else(|| u.strip_prefix("grant "))
        else {
            continue;
        };
        let upper = rest.to_ascii_uppercase();
        let Some(on_at) = upper.find(" ON ") else {
            continue;
        };
        let privs: Vec<&str> = upper[..on_at].split(',').map(str::trim).collect();
        let all = privs.iter().any(|p| *p == "ALL" || *p == "ALL PRIVILEGES");
        let after = &rest[on_at + 4..];
        let to_at = after
            .to_ascii_uppercase()
            .find(" TO ")
            .unwrap_or(after.len());
        let target = after[..to_at].trim();
        let unq = |s: &str| s.trim().trim_matches('`').replace("``", "`");
        let (db, table) = match target
            .split_once("`.`")
            .map(|(a, b)| (format!("{a}`"), format!("`{b}")))
        {
            Some((a, b)) => (unq(&a), unq(&b)),
            None => match target.split_once('.') {
                Some((a, b)) => (unq(a), unq(b)),
                None => (unq(target), "*".into()),
            },
        };
        if db == "*" && (all || privs.contains(&"SPRIG_TENANT_ADMIN")) {
            admin = true;
        }
        let alter = all || privs.contains(&"ALTER");
        let index = all || privs.contains(&"INDEX");
        if alter || index {
            out.push(GrantClaim {
                db,
                table,
                alter,
                index,
            });
        }
    }
    (admin, out)
}

fn enc(s: &str) -> String {
    let mut o = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            o.push(char::from(b));
        } else {
            let _ = write!(o, "%{b:02X}");
        }
    }
    o
}

/// `/v1/recommendations...`.
///
/// # Errors
/// 39792 for an unknown route; the controller's errors.
pub async fn handle(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let auth = hdr(&req, "authorization");
    let host = hdr(&req, "host");
    let method = req.method().clone();
    let path = req.uri().path().to_owned();
    let query = req.uri().query().unwrap_or("").to_owned();
    let body = read_body(req).await?;
    let caller = core.caller(info, auth, None, host).await?;
    let rest = path.trim_start_matches("/v1/recommendations");
    let r = proxy(core, &caller, &method, rest, &query, &body).await?;
    Ok(passthrough(r.status, r.body))
}

/// One call of a tenant user to the controller's `Recommendations`
/// service, with the user as a caller claim: `rest` is the part after
/// `/v1/recommendations` (`""`, `/{id}`, `/{id}:{action}`, `:bulk`,
/// `:policy`, `:subscriptions`). The REST face above and gRPC's
/// `sprig.admin.v1.Recommendations` (for tenant logins) both call it.
///
/// # Errors
/// 39792 for an unknown route or a login without a tenant; the
/// controller's errors.
#[allow(clippy::too_many_lines)] // one arm per route
pub async fn proxy(
    core: &Arc<Core>,
    caller: &SqlCaller,
    method: &Method,
    rest: &str,
    query: &str,
    body: &[u8],
) -> Result<crate::http::admin::Reply, SprigError> {
    let query = query.to_owned();
    let Some(tenant) = caller.principal.tenant_name.clone() else {
        return Err(invalid("the approvals inbox needs a tenant login"));
    };
    let grants = run_sql(core, caller, "-", "SHOW GRANTS".to_owned()).await?;
    let lines: Vec<String> = rows_of(&grants)
        .into_iter()
        .filter_map(|r| r.into_iter().next().flatten())
        .collect();
    let (admin, grants) = claims(&lines);
    let claim = Caller {
        name: format!("studio:{tenant}.{}", caller.principal.user),
        tenant: caller.principal.tenant,
        tenant_admin: admin,
        grants,
        ..Caller::default()
    };
    let claim_json = serde_json::to_string(&claim).unwrap_or_default();
    let keep = |allowed: &[&str]| -> Vec<String> {
        query
            .split('&')
            .filter(|kv| kv.split('=').next().is_some_and(|k| allowed.contains(&k)))
            .map(str::to_owned)
            .collect()
    };
    let safe = |s: &str| {
        !s.is_empty()
            && s.chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'))
    };
    let base = "/admin/v1/recommendations";
    let (m, target, fwd): (Method, String, Bytes) = match (method.as_str(), rest) {
        ("GET", "" | "/") => {
            let mut q = keep(&[
                "state",
                "kind",
                "level",
                "all",
                "db",
                "table",
                "min_age_hours",
                "limit",
            ]);
            // An ancestor's administrator may name a descendant; the
            // controller shows only what the claim may see.
            let t = keep(&["tenant"]).first().map_or_else(
                || tenant.clone(),
                |kv| kv.trim_start_matches("tenant=").to_owned(),
            );
            q.push(format!("tenant={}", enc(&t)));
            q.push(format!("caller={}", enc(&claim_json)));
            (Method::GET, format!("{base}?{}", q.join("&")), Bytes::new())
        }
        ("GET", ":policy") => {
            let mut q = keep(&["db", "object"]);
            q.push(format!("tenant={}", enc(&tenant)));
            q.push(format!("caller={}", enc(&claim_json)));
            (
                Method::GET,
                format!("{base}:policy?{}", q.join("&")),
                Bytes::new(),
            )
        }
        ("GET", ":subscriptions") => (
            Method::GET,
            format!(
                "{base}:subscriptions?tenant={}&caller={}",
                enc(&tenant),
                enc(&claim_json)
            ),
            Bytes::new(),
        ),
        ("POST", ":subscriptions") => {
            let b = parse_json(body)?;
            let fwd = json!({
                "tenant": tenant,
                "user": caller.principal.user,
                "email": b.get("email").cloned().unwrap_or(Value::Null),
                "delivery": b.get("delivery").cloned().unwrap_or(json!("immediate")),
                "kinds": b.get("kinds").cloned().unwrap_or(json!("*")),
                "caller": claim,
            });
            (
                Method::POST,
                format!("{base}:subscriptions"),
                Bytes::from(fwd.to_string()),
            )
        }
        ("POST", ":bulk") => {
            let mut b = parse_json(body)?;
            if let Value::Object(o) = &mut b {
                o.insert(
                    "caller".into(),
                    serde_json::to_value(&claim).unwrap_or_default(),
                );
                o.remove("actor");
            }
            (
                Method::POST,
                format!("{base}:bulk"),
                Bytes::from(b.to_string()),
            )
        }
        ("GET", id) if id.starts_with('/') && safe(&id[1..]) => (
            Method::GET,
            format!("{base}{id}?caller={}", enc(&claim_json)),
            Bytes::new(),
        ),
        ("POST", id) if id.starts_with('/') && safe(&id[1..]) && id.contains(':') => {
            let mut b = if body.iter().all(u8::is_ascii_whitespace) {
                json!({})
            } else {
                parse_json(body)?
            };
            if let Value::Object(o) = &mut b {
                o.insert(
                    "caller".into(),
                    serde_json::to_value(&claim).unwrap_or_default(),
                );
                o.remove("actor");
            }
            (
                Method::POST,
                format!("{base}{id}"),
                Bytes::from(b.to_string()),
            )
        }
        _ => {
            return Err(invalid(format!(
                "no route {method} /v1/recommendations{rest}"
            )));
        }
    };
    core.admin.forward_internal(&m, &target, fwd).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grants_become_claims() {
        let lines = vec![
            "GRANT USAGE ON *.* TO `app`@`%`".to_owned(),
            "GRANT SELECT, INDEX ON `shop`.* TO `app`@`%`".to_owned(),
            "GRANT ALTER ON `crm`.`deals` TO `app`@`%`".to_owned(),
        ];
        let (admin, c) = claims(&lines);
        assert!(!admin);
        assert_eq!(c.len(), 2);
        assert_eq!(
            (
                c[0].db.as_str(),
                c[0].table.as_str(),
                c[0].alter,
                c[0].index
            ),
            ("shop", "*", false, true)
        );
        assert_eq!(
            (
                c[1].db.as_str(),
                c[1].table.as_str(),
                c[1].alter,
                c[1].index
            ),
            ("crm", "deals", true, false)
        );
        let (admin, c) = claims(&["GRANT ALL PRIVILEGES ON *.* TO `root`@`%`".to_owned()]);
        assert!(admin);
        assert_eq!(c[0].db, "*");
        let (admin, _) = claims(&["GRANT SPRIG_TENANT_ADMIN ON *.* TO `a`@`%`".to_owned()]);
        assert!(admin);
    }
}
