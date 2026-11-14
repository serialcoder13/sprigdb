//! The REST face: transcoding REST requests to the RPCs.
//!
//! Every RPC's `google.api.http` rule is one row of [`ROUTES`]; a test reads
//! the `.proto` files and fails if the table and the annotations disagree,
//! so the protos stay the one definition. [`find`] matches a request's
//! method and path against the table and returns the RPC with its path
//! variables; [`openapi`] builds the `OpenAPI` v3 document served at
//! `/v1/openapi.json` and `/admin/v1/openapi.json`.
//!
//! Path templates follow `google.api.http`: `{name}` matches one segment,
//! `{name=prefix/*}` matches `prefix/<segment>`, and a custom method is a
//! `:verb` suffix on the last segment (`/v1/{transaction=transactions/*}:commit`).
//!
//! **Dispatch.** There is no generated handler per rule: the data RPCs are
//! few and the admin RPCs forward to one place, so the gateway dispatches
//! through this table. REST and gRPC still call the same implementation,
//! and the routes still come from the protos.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use serde_json::{Value, json};

/// One REST route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Route {
    /// The proto service, `sprig.v1.Sql`.
    pub service: &'static str,
    /// The RPC, `Execute`.
    pub rpc: &'static str,
    /// `GET`, `POST`, `PUT` or `DELETE`.
    pub method: &'static str,
    /// The path template.
    pub template: &'static str,
    /// The request message's name, for the `OpenAPI` document.
    pub request: &'static str,
    /// The RPC streams its replies (NDJSON over REST).
    pub streaming: bool,
}

impl Route {
    /// `Service.Rpc` without the package, `Sql.Execute`.
    #[must_use]
    pub fn short_name(&self) -> String {
        let svc = self.service.rsplit('.').next().unwrap_or(self.service);
        format!("{svc}.{}", self.rpc)
    }

    /// Whether the route belongs to the Admin API.
    #[must_use]
    pub fn is_admin(&self) -> bool {
        self.service.starts_with("sprig.admin.")
    }

    /// The request takes a JSON body.
    #[must_use]
    pub fn has_body(&self) -> bool {
        matches!(self.method, "POST" | "PUT")
    }

    /// The path for `vars`, the inverse of matching.
    #[must_use]
    pub fn path(&self, vars: &BTreeMap<String, String>) -> String {
        let mut out = String::new();
        let mut rest = self.template;
        while let Some(open) = rest.find('{') {
            out.push_str(&rest[..open]);
            let close = rest[open..].find('}').map_or(rest.len(), |c| open + c);
            let inner = &rest[open + 1..close];
            let name = inner.split('=').next().unwrap_or(inner);
            out.push_str(vars.get(name).map_or("", String::as_str));
            rest = rest.get(close + 1..).unwrap_or("");
        }
        out.push_str(rest);
        out
    }
}

macro_rules! r {
    ($svc:literal, $rpc:literal, $m:literal, $t:literal, $req:literal) => {
        Route {
            service: $svc,
            rpc: $rpc,
            method: $m,
            template: $t,
            request: $req,
            streaming: false,
        }
    };
    ($svc:literal, $rpc:literal, $m:literal, $t:literal, $req:literal, stream) => {
        Route {
            service: $svc,
            rpc: $rpc,
            method: $m,
            template: $t,
            request: $req,
            streaming: true,
        }
    };
}

/// Every REST route, in the order of the protos.
pub static ROUTES: &[Route] = &[
    r!(
        "sprig.v1.Sql",
        "Query",
        "POST",
        "/v1/databases/{database}/sql:query",
        "QueryRequest",
        stream
    ),
    r!(
        "sprig.v1.Sql",
        "Execute",
        "POST",
        "/v1/databases/{database}/sql:execute",
        "QueryRequest"
    ),
    r!(
        "sprig.v1.Transactions",
        "Begin",
        "POST",
        "/v1/databases/{database}/transactions",
        "BeginRequest"
    ),
    r!(
        "sprig.v1.Transactions",
        "Commit",
        "POST",
        "/v1/{transaction=transactions/*}:commit",
        "EndTransactionRequest"
    ),
    r!(
        "sprig.v1.Transactions",
        "Rollback",
        "POST",
        "/v1/{transaction=transactions/*}:rollback",
        "EndTransactionRequest"
    ),
    r!(
        "sprig.v1.Functions",
        "Call",
        "POST",
        "/v1/databases/{database}/functions/{name}:call",
        "CallRequest"
    ),
    r!(
        "sprig.v1.Logic",
        "TestRun",
        "POST",
        "/v1/databases/{database}/logic:testRun",
        "TestRunRequest"
    ),
    r!(
        "sprig.v1.KeyValue",
        "Get",
        "POST",
        "/v1/databases/{database}/namespaces/{namespace}:get",
        "KvGetRequest"
    ),
    r!(
        "sprig.v1.KeyValue",
        "Put",
        "POST",
        "/v1/databases/{database}/namespaces/{namespace}:put",
        "KvPutRequest"
    ),
    r!(
        "sprig.v1.KeyValue",
        "Delete",
        "POST",
        "/v1/databases/{database}/namespaces/{namespace}:delete",
        "KvDeleteRequest"
    ),
    r!(
        "sprig.v1.KeyValue",
        "Scan",
        "POST",
        "/v1/databases/{database}/namespaces/{namespace}:scan",
        "KvScanRequest"
    ),
    r!(
        "sprig.v1.KeyValue",
        "Exec",
        "POST",
        "/v1/databases/{database}/namespaces/{namespace}:exec",
        "KvExecRequest"
    ),
    r!(
        "sprig.v1.KeyValue",
        "GeoSearch",
        "POST",
        "/v1/databases/{database}/namespaces/{namespace}:geoSearch",
        "KvGeoSearchRequest"
    ),
    r!(
        "sprig.v1.Vectors",
        "CreateIndex",
        "POST",
        "/v1/databases/{database}/vectorIndexes",
        "CreateVectorIndexRequest"
    ),
    r!(
        "sprig.v1.Vectors",
        "GetIndex",
        "GET",
        "/v1/databases/{database}/vectorIndexes/{index}",
        "GetVectorIndexRequest"
    ),
    r!(
        "sprig.v1.Vectors",
        "DropIndex",
        "DELETE",
        "/v1/databases/{database}/vectorIndexes/{index}",
        "GetVectorIndexRequest"
    ),
    r!(
        "sprig.v1.Vectors",
        "Upsert",
        "POST",
        "/v1/databases/{database}/vectorIndexes/{index}:upsert",
        "UpsertVectorsRequest"
    ),
    r!(
        "sprig.v1.Vectors",
        "Delete",
        "POST",
        "/v1/databases/{database}/vectorIndexes/{index}:delete",
        "DeleteVectorsRequest"
    ),
    r!(
        "sprig.v1.Vectors",
        "Search",
        "POST",
        "/v1/databases/{database}/vectorIndexes/{index}:search",
        "SearchVectorsRequest"
    ),
    r!(
        "sprig.v1.Analytics",
        "CreateModel",
        "POST",
        "/v1/databases/{database}/models",
        "ModelRequest"
    ),
    r!(
        "sprig.v1.Analytics",
        "GetModel",
        "GET",
        "/v1/databases/{database}/models/{model}",
        "GetModelRequest"
    ),
    r!(
        "sprig.v1.Analytics",
        "ListModels",
        "GET",
        "/v1/databases/{database}/models",
        "ListModelsRequest"
    ),
    r!(
        "sprig.v1.Analytics",
        "UpdateModel",
        "PUT",
        "/v1/databases/{database}/models/{model}",
        "ModelRequest"
    ),
    r!(
        "sprig.v1.Analytics",
        "DropModel",
        "DELETE",
        "/v1/databases/{database}/models/{model}",
        "GetModelRequest"
    ),
    r!(
        "sprig.v1.Analytics",
        "Query",
        "POST",
        "/v1/databases/{database}/models/{model}:query",
        "AnalyticsQuery"
    ),
    r!(
        "sprig.v1.Analytics",
        "Explain",
        "POST",
        "/v1/databases/{database}/models/{model}:explain",
        "AnalyticsQuery"
    ),
    r!(
        "sprig.v1.Analytics",
        "Drill",
        "POST",
        "/v1/databases/{database}/models/{model}:drill",
        "DrillRequest"
    ),
    r!(
        "sprig.v1.Search",
        "Query",
        "POST",
        "/v1/databases/{database}/search/{index}:query",
        "SearchQueryRequest"
    ),
    r!(
        "sprig.v1.Changes",
        "Watch",
        "GET",
        "/v1/databases/{database}/changes",
        "WatchRequest",
        stream
    ),
    r!(
        "sprig.admin.v1.Clusters",
        "RegisterCluster",
        "POST",
        "/admin/v1/clusters",
        "BodyRequest"
    ),
    r!(
        "sprig.admin.v1.Clusters",
        "ListClusters",
        "GET",
        "/admin/v1/clusters",
        "ListRequest"
    ),
    r!(
        "sprig.admin.v1.Clusters",
        "GetCluster",
        "GET",
        "/admin/v1/clusters/{name}",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Clusters",
        "ProbeCluster",
        "POST",
        "/admin/v1/clusters/{name}:probe",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Clusters",
        "DrainCluster",
        "POST",
        "/admin/v1/clusters/{name}:drain",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.DataCenters",
        "RegisterDc",
        "POST",
        "/admin/v1/dcs",
        "BodyRequest"
    ),
    r!(
        "sprig.admin.v1.DataCenters",
        "ListDcs",
        "GET",
        "/admin/v1/dcs",
        "ListRequest"
    ),
    r!(
        "sprig.admin.v1.DataCenters",
        "GetDc",
        "GET",
        "/admin/v1/dcs/{name}",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Tenants",
        "CreateTenant",
        "POST",
        "/admin/v1/tenants",
        "BodyRequest"
    ),
    r!(
        "sprig.admin.v1.Tenants",
        "ListTenants",
        "GET",
        "/admin/v1/tenants",
        "ListRequest"
    ),
    r!(
        "sprig.admin.v1.Tenants",
        "GetTenant",
        "GET",
        "/admin/v1/tenants/{name}",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Tenants",
        "DeleteTenant",
        "DELETE",
        "/admin/v1/tenants/{name}",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Tenants",
        "SuspendTenant",
        "POST",
        "/admin/v1/tenants/{name}:suspend",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Tenants",
        "ResumeTenant",
        "POST",
        "/admin/v1/tenants/{name}:resume",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Users",
        "CreateUser",
        "POST",
        "/admin/v1/tenants/{name}/users",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Settings",
        "ListSettingKeys",
        "GET",
        "/admin/v1/settings",
        "ListRequest"
    ),
    r!(
        "sprig.admin.v1.Settings",
        "GetSetting",
        "GET",
        "/admin/v1/settings/{name}",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Settings",
        "ExplainSetting",
        "GET",
        "/admin/v1/settings/{name}:explain",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Settings",
        "SetSetting",
        "PUT",
        "/admin/v1/settings/{name}",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Settings",
        "UnsetSetting",
        "DELETE",
        "/admin/v1/settings/{name}",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Settings",
        "ResetSetting",
        "POST",
        "/admin/v1/settings/{name}:reset",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Settings",
        "SetBounds",
        "POST",
        "/admin/v1/settings/{name}:bounds",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Settings",
        "LockSetting",
        "POST",
        "/admin/v1/settings/{name}:lock",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Settings",
        "UnlockSetting",
        "POST",
        "/admin/v1/settings/{name}:unlock",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Settings",
        "TenantSettings",
        "GET",
        "/admin/v1/tenants/{name}/settings",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Indexes",
        "ListIndexes",
        "GET",
        "/admin/v1/tenants/{tenant}/indexes",
        "TenantRequest"
    ),
    r!(
        "sprig.admin.v1.Indexes",
        "ListRecommendations",
        "GET",
        "/admin/v1/tenants/{tenant}/recommendations",
        "TenantRequest"
    ),
    r!(
        "sprig.admin.v1.Indexes",
        "GetIndex",
        "GET",
        "/admin/v1/tenants/{tenant}/indexes/{name}",
        "TenantObjectRequest"
    ),
    r!(
        "sprig.admin.v1.Indexes",
        "Apply",
        "POST",
        "/admin/v1/tenants/{tenant}/indexes/{name}:approve",
        "TenantObjectRequest"
    ),
    r!(
        "sprig.admin.v1.Indexes",
        "Dismiss",
        "POST",
        "/admin/v1/tenants/{tenant}/indexes/{name}:reject",
        "TenantObjectRequest"
    ),
    r!(
        "sprig.admin.v1.Indexes",
        "Drop",
        "POST",
        "/admin/v1/tenants/{tenant}/indexes/{name}:drop",
        "TenantObjectRequest"
    ),
    r!(
        "sprig.admin.v1.Indexes",
        "Freeze",
        "POST",
        "/admin/v1/tenants/{tenant}/indexes/{name}:pin",
        "TenantObjectRequest"
    ),
    r!(
        "sprig.admin.v1.Indexes",
        "Unfreeze",
        "POST",
        "/admin/v1/tenants/{tenant}/indexes/{name}:unpin",
        "TenantObjectRequest"
    ),
    r!(
        "sprig.admin.v1.Explain",
        "ExplainQuery",
        "GET",
        "/admin/v1/tenants/{tenant}/queries/{name}:explain",
        "TenantObjectRequest"
    ),
    r!(
        "sprig.admin.v1.Jobs",
        "ListJobs",
        "GET",
        "/admin/v1/jobs",
        "ListRequest"
    ),
    r!(
        "sprig.admin.v1.Jobs",
        "GetJob",
        "GET",
        "/admin/v1/jobs/{name}",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Jobs",
        "CancelJob",
        "POST",
        "/admin/v1/jobs/{name}:cancel",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Gateways",
        "ListGateways",
        "GET",
        "/admin/v1/gateways",
        "ListRequest"
    ),
    r!(
        "sprig.admin.v1.Gateways",
        "GetGateway",
        "GET",
        "/admin/v1/gateways/{name}",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Gateways",
        "DrainGateway",
        "POST",
        "/admin/v1/gateways/{name}:drain",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Recommendations",
        "ListRecommendations",
        "GET",
        "/admin/v1/recommendations",
        "ListRequest"
    ),
    r!(
        "sprig.admin.v1.Recommendations",
        "GetRecommendation",
        "GET",
        "/admin/v1/recommendations/{name}",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Recommendations",
        "ActOnRecommendation",
        "POST",
        "/admin/v1/recommendations/{name}:act",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Recommendations",
        "BulkAct",
        "POST",
        "/admin/v1/recommendations:bulk",
        "BodyRequest"
    ),
    r!(
        "sprig.admin.v1.Recommendations",
        "GetApprovalPolicy",
        "GET",
        "/admin/v1/recommendations:policy",
        "ListRequest"
    ),
    r!(
        "sprig.admin.v1.Recommendations",
        "SetApprovalPolicy",
        "POST",
        "/admin/v1/recommendations:policy",
        "BodyRequest"
    ),
    r!(
        "sprig.admin.v1.Recommendations",
        "ListSubscriptions",
        "GET",
        "/admin/v1/recommendations:subscriptions",
        "ListRequest"
    ),
    r!(
        "sprig.admin.v1.Recommendations",
        "SetSubscription",
        "POST",
        "/admin/v1/recommendations:subscriptions",
        "BodyRequest"
    ),
    r!(
        "sprig.admin.v1.Operators",
        "Login",
        "POST",
        "/admin/v1/operators:login",
        "BodyRequest"
    ),
    r!(
        "sprig.admin.v1.Operators",
        "Enrol",
        "POST",
        "/admin/v1/operators:enrol",
        "BodyRequest"
    ),
    r!(
        "sprig.admin.v1.Operators",
        "Logout",
        "POST",
        "/admin/v1/operators:logout",
        "BodyRequest"
    ),
    r!(
        "sprig.admin.v1.Operators",
        "WhoAmI",
        "GET",
        "/admin/v1/operators:whoami",
        "ListRequest"
    ),
    r!(
        "sprig.admin.v1.Operators",
        "ListOperators",
        "GET",
        "/admin/v1/operators",
        "ListRequest"
    ),
    r!(
        "sprig.admin.v1.Operators",
        "CreateOperator",
        "POST",
        "/admin/v1/operators",
        "BodyRequest"
    ),
    r!(
        "sprig.admin.v1.Operators",
        "GetOperator",
        "GET",
        "/admin/v1/operators/{name}",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Operators",
        "DisableOperator",
        "POST",
        "/admin/v1/operators/{name}:disable",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Operators",
        "EnableOperator",
        "POST",
        "/admin/v1/operators/{name}:enable",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Operators",
        "ResetOperatorFactor",
        "POST",
        "/admin/v1/operators/{name}:resetFactor",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Operators",
        "SetOperatorPassword",
        "POST",
        "/admin/v1/operators/{name}:setPassword",
        "NameRequest"
    ),
    r!(
        "sprig.admin.v1.Operators",
        "CreateToken",
        "POST",
        "/admin/v1/operator-tokens",
        "BodyRequest"
    ),
    r!(
        "sprig.admin.v1.Operators",
        "ListTokens",
        "GET",
        "/admin/v1/operator-tokens",
        "ListRequest"
    ),
    r!(
        "sprig.admin.v1.Operators",
        "RevokeToken",
        "POST",
        "/admin/v1/operator-tokens/{name}:revoke",
        "NameRequest"
    ),
];

/// The route for `Service.Rpc` (package-less, as [`Route::short_name`]).
#[must_use]
pub fn by_name(short: &str) -> Option<&'static Route> {
    ROUTES.iter().find(|r| r.short_name() == short)
}

/// A matched route and its path variables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Matched {
    /// The route.
    pub route: &'static Route,
    /// Path variables by name, percent-decoded.
    pub vars: BTreeMap<String, String>,
}

/// Matches `method` and `path` (no query string) against [`ROUTES`].
#[must_use]
pub fn find(method: &str, path: &str) -> Option<Matched> {
    ROUTES.iter().find_map(|r| {
        if !r.method.eq_ignore_ascii_case(method) {
            return None;
        }
        match_template(r.template, path).map(|vars| Matched { route: r, vars })
    })
}

/// Whether some route has this path under another method (405, not 404).
#[must_use]
pub fn path_known(path: &str) -> bool {
    ROUTES
        .iter()
        .any(|r| match_template(r.template, path).is_some())
}

/// Matches one template; `None` if it doesn't fit.
#[must_use]
pub fn match_template(template: &str, path: &str) -> Option<BTreeMap<String, String>> {
    let (t_body, t_verb) = split_verb(template);
    let (p_body, p_verb) = split_verb(path);
    if t_verb != p_verb {
        return None;
    }
    let mut vars = BTreeMap::new();
    let mut p = p_body.split('/');
    let mut t = t_body.split('/').peekable();
    while let Some(seg) = t.next() {
        if let Some(inner) = seg.strip_prefix('{') {
            // `{name}` or `{name=prefix/*}`; a pattern with '/' spans the
            // template's following segments up to the closing '}'.
            let mut spec = inner.to_owned();
            while !spec.ends_with('}') {
                let next = t.next()?;
                spec.push('/');
                spec.push_str(next);
            }
            spec.pop();
            let (name, pattern) = spec.split_once('=').unwrap_or((spec.as_str(), "*"));
            let mut value = Vec::new();
            for pseg in pattern.split('/') {
                let actual = p.next()?;
                if pseg == "*" {
                    if actual.is_empty() {
                        return None;
                    }
                } else if pseg != actual {
                    return None;
                }
                value.push(actual);
            }
            vars.insert(name.to_owned(), percent_decode(&value.join("/")));
        } else if p.next()? != seg {
            return None;
        }
    }
    if p.next().is_some() {
        return None;
    }
    Some(vars)
}

/// Splits a `:verb` off the last segment.
fn split_verb(s: &str) -> (&str, Option<&str>) {
    let last = s.rfind('/').map_or(0, |i| i + 1);
    match s[last..].rfind(':') {
        // A ':' inside `{...}` is not a verb.
        Some(i) if !s[last..last + i].contains('{') || s[last..last + i].contains('}') => {
            (&s[..last + i], Some(&s[last + i + 1..]))
        }
        _ => (s, None),
    }
}

/// Decodes `%XX` escapes; invalid escapes are kept as they are.
#[must_use]
pub fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            let hex = s
                .get(i + 1..i + 3)
                .and_then(|h| u8::from_str_radix(h, 16).ok());
            if let Some(v) = hex {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Encodes a path segment: everything but unreserved characters.
#[must_use]
pub fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

/// The `OpenAPI` v3 document for the routes `admin` selects (the Admin API,
/// or the data API).
#[must_use]
pub fn openapi(admin: bool) -> Value {
    let mut paths = serde_json::Map::new();
    for r in ROUTES.iter().filter(|r| r.is_admin() == admin) {
        let params: Vec<Value> = template_vars(r.template)
            .into_iter()
            .map(|v| {
                json!({"name": v, "in": "path", "required": true, "schema": {"type": "string"}})
            })
            .chain(
                (r.method == "GET").then(|| {
                    json!({"name": "query", "in": "query", "required": false,
                           "description": "Filters, as the route documents them",
                           "schema": {"type": "string"}})
                }),
            )
            .collect();
        let mut op = json!({
            "operationId": r.short_name(),
            "tags": [r.service],
            "parameters": params,
            "responses": {
                "200": {"description": "OK", "content": {
                    response_type(r): {"schema": {"$ref": format!("#/components/schemas/{}", reply(r))}}
                }},
                "default": {"description": "An error", "content": {"application/json": {
                    "schema": {"$ref": "#/components/schemas/Error"}}}}
            }
        });
        if r.has_body() {
            op["requestBody"] = json!({"required": false, "content": {"application/json": {
                "schema": {"$ref": format!("#/components/schemas/{}", r.request)}}}});
        }
        let entry = paths
            .entry(openapi_path(r.template))
            .or_insert_with(|| json!({}));
        entry[r.method.to_ascii_lowercase()] = op;
    }
    let title = if admin {
        "SprigDB Admin API (sprig.admin.v1)"
    } else {
        "SprigDB data API (sprig.v1)"
    };
    // Every message a route names has a schema: one written above, or a
    // plain object for messages not described yet, so generated clients
    // resolve every reference.
    let mut all = schemas();
    for r in ROUTES.iter().filter(|r| r.is_admin() == admin) {
        for name in [r.request, reply(r)] {
            if let Some(m) = all.as_object_mut()
                && !m.contains_key(name)
            {
                m.insert(
                    name.to_owned(),
                    json!({"type": "object", "description": format!("The {name} message as JSON (proto3 mapping)")}),
                );
            }
        }
    }
    json!({
        "openapi": "3.0.3",
        "info": {"title": title, "version": "v1"},
        "paths": paths,
        "components": {"schemas": all},
    })
}

fn response_type(r: &Route) -> &'static str {
    if r.streaming {
        "application/x-ndjson"
    } else {
        "application/json"
    }
}

fn reply(r: &Route) -> &'static str {
    match (r.service, r.rpc) {
        ("sprig.v1.Analytics", "Query") => "AnalyticsResult",
        ("sprig.v1.Search", "Query") => "SearchQueryResponse",
        ("sprig.v1.Changes", "Watch") => "ChangeEvent",
        (_, "Query") => "QueryResponse",
        (_, "Execute") => "ExecuteResponse",
        (_, "Begin") => "Transaction",
        ("sprig.v1.Transactions", _) => "EndTransactionResponse",
        ("sprig.v1.Functions", "Call") => "CallResponse",
        ("sprig.v1.Logic", "TestRun") => "TestRunResponse",
        ("sprig.v1.KeyValue", "Get") => "KvGetResponse",
        ("sprig.v1.KeyValue", "Put") => "KvPutResponse",
        ("sprig.v1.KeyValue", "Delete") => "KvDeleteResponse",
        ("sprig.v1.KeyValue", "Scan") => "KvScanResponse",
        ("sprig.v1.KeyValue", "Exec") => "KvExecResponse",
        ("sprig.v1.KeyValue", "GeoSearch") => "KvGeoSearchResponse",
        ("sprig.v1.Vectors", "CreateIndex" | "GetIndex") => "VectorIndex",
        ("sprig.v1.Vectors", "DropIndex") => "DropVectorIndexResponse",
        ("sprig.v1.Vectors", "Upsert") => "UpsertVectorsResponse",
        ("sprig.v1.Vectors", "Delete") => "DeleteVectorsResponse",
        ("sprig.v1.Vectors", "Search") => "SearchVectorsResponse",
        ("sprig.v1.Analytics", "CreateModel" | "GetModel" | "UpdateModel") => "ModelInfo",
        ("sprig.v1.Analytics", "ListModels") => "ListModelsResponse",
        ("sprig.v1.Analytics", "DropModel") => "DropModelResponse",
        ("sprig.v1.Analytics", "Explain") => "AnalyticsExplain",
        ("sprig.v1.Analytics", "Drill") => "AnalyticsQuery",
        _ => "JsonReply",
    }
}

/// `{transaction=transactions/*}` becomes `{transaction}`, as `OpenAPI` wants.
fn openapi_path(template: &str) -> String {
    let mut out = String::new();
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let close = rest[open..].find('}').map_or(rest.len(), |c| open + c);
        let inner = &rest[open + 1..close];
        out.push('{');
        out.push_str(inner.split('=').next().unwrap_or(inner));
        out.push('}');
        rest = rest.get(close + 1..).unwrap_or("");
    }
    out.push_str(rest);
    out
}

fn template_vars(template: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        let close = rest[open..].find('}').map_or(rest.len(), |c| open + c);
        let inner = &rest[open + 1..close];
        out.push(inner.split('=').next().unwrap_or(inner).to_owned());
        rest = rest.get(close + 1..).unwrap_or("");
    }
    out
}

fn schemas() -> Value {
    let s = |props: Value| json!({"type": "object", "properties": props});
    let string = json!({"type": "string"});
    let u64s = json!({"type": "string", "format": "uint64"});
    let row = json!({"type": "array", "items": {"type": "string", "nullable": true}});
    json!({
        "QueryRequest": s(json!({"database": string, "sql": string, "transaction": string,
                                 "multi_statements": {"type": "boolean"},
                                 "max_rows": u64s, "max_bytes": u64s, "timeout_ms": u64s,
                                 "language": {"type": "string",
                                     "enum": ["sql", "cypher", "gql", "mongo"]}})),
        "Column": s(json!({"name": string, "org_name": string, "table": string,
            "org_table": string, "database": string, "type": string,
            "mysql_type": {"type": "integer"}, "flags": {"type": "integer"},
            "charset": {"type": "integer"}, "decimals": {"type": "integer"},
            "length": u64s, "binary": {"type": "boolean"}})),
        "Result": s(json!({"columns": {"type": "array", "items": {"$ref": "#/components/schemas/Column"}},
            "rows": {"type": "array", "items": row}, "affected_rows": u64s,
            "last_insert_id": u64s, "warnings": {"type": "integer"}, "info": string,
            "truncated": {"type": "boolean"}})),
        "ExecuteResponse": s(json!({"results": {"type": "array", "items": {"$ref": "#/components/schemas/Result"}}})),
        "QueryResponse": s(json!({"header": {"type": "object"}, "rows": {"type": "object"}, "end": {"type": "object"}})),
        "BeginRequest": s(json!({"database": string, "read_only": {"type": "boolean"}})),
        "Transaction": s(json!({"name": string, "idle_deadline_ms": {"type": "string", "format": "int64"},
            "deadline_ms": {"type": "string", "format": "int64"}})),
        "EndTransactionRequest": s(json!({"transaction": string})),
        "EndTransactionResponse": s(json!({})),
        "CallRequest": s(json!({"database": string, "name": string,
            "args": {"description": "A list (by position), an object (by parameter name) or one value"},
            "transaction": string})),
        "CallResponse": s(json!({"event": string, "result": {"type": "object"}})),
        "ListRequest": s(json!({})),
        "BodyRequest": {"type": "object", "description": "The call's JSON body (see the route's documentation)"},
        "NameRequest": {"type": "object", "description": "The call's JSON body (see the route's documentation)"},
        "TenantRequest": {"type": "object", "description": "The call's JSON body"},
        "TenantObjectRequest": {"type": "object", "description": "The call's JSON body"},
        "JsonReply": {"type": "object", "description": "The call's JSON reply"},
        "Error": s(json!({"code": {"type": "integer"}, "name": string, "message": string,
            "sqlstate": string, "trace_id": string, "retryable": {"type": "boolean"}})),
    })
}

#[cfg(test)]
mod tests;
