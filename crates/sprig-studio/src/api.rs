//! Studio's REST routes in the data API's `OpenAPI` document (chapter 37,
//! "APIs for Studio", R0 part).
//!
//! The gateway answers these ahead of the proto route table (they are REST
//! only in R0); [`merge_into`] adds them to `/v1/openapi.json`, which the
//! Studio client's types are generated from (`pnpm gen:api`).

use serde_json::{Value, json};

fn op(id: &str, tag: &str, summary: &str, req: Option<&str>, reply: &str) -> Value {
    let mut o = json!({
        "operationId": id,
        "tags": [tag],
        "summary": summary,
        "responses": {
            "200": {"description": "OK", "content": {"application/json": {
                "schema": {"$ref": format!("#/components/schemas/{reply}")}}}},
            "default": {"description": "An error", "content": {"application/json": {
                "schema": {"$ref": "#/components/schemas/Error"}}}}
        }
    });
    if let Some(r) = req {
        o["requestBody"] = json!({"required": true, "content": {"application/json": {
            "schema": {"$ref": format!("#/components/schemas/{r}")}}}});
    }
    o
}

fn with_params(mut o: Value, params: &[Value]) -> Value {
    o["parameters"] = Value::Array(params.to_vec());
    o
}

/// The paths, `{path: {method: operation}}`.
#[must_use]
pub fn paths() -> Value {
    let mut v = core_paths();
    if let (Value::Object(m), Value::Object(r)) = (&mut v, recommendation_paths()) {
        m.extend(r);
    }
    if let (Value::Object(m), Value::Object(r)) = (&mut v, round4_paths()) {
        m.extend(r);
    }
    v
}

/// Studio round 4 (chapter 44 steps 18, 26, 28–31): the logic editors'
/// `Logic` routes, the stream, queue and sink views' routes, and
/// `Policies.TestAs`.
fn round4_paths() -> Value {
    let p = |name: &str| json!({"name": name, "in": "path", "required": true, "schema": {"type": "string"}});
    let q = |name: &str| json!({"name": name, "in": "query", "required": false, "schema": {"type": "string"}});
    let one = [p("database"), p("kind"), p("name")];
    json!({
        "/v1/databases/{database}/logic": {"get": with_params(op("Logic.ListModules", "sprig.v1.Logic",
            "The database's logic modules", None, "JsonReply"), &[p("database")])},
        "/v1/databases/{database}/logic:compile": {"post": with_params(op("Logic.Compile", "sprig.v1.Logic",
            "A dry run of a CREATE FUNCTION or PROCEDURE: tier, reasons, signature, PL/SQL; nothing recorded", None, "JsonReply"),
            &[p("database")])},
        "/v1/databases/{database}/logic/{kind}/{name}": {"get": with_params(op("Logic.GetModule", "sprig.v1.Logic",
            "A module with its versions, binding, schedule and (for routine administrators) source and PL/SQL", None, "JsonReply"), &one)},
        "/v1/databases/{database}/logic/{kind}/{name}/versions/{version}": {"get": with_params(op("Logic.GetVersion", "sprig.v1.Logic",
            "One version's source and PL/SQL", None, "JsonReply"), &[p("database"), p("kind"), p("name"), p("version")])},
        "/v1/databases/{database}/logic/{kind}/{name}/rollout": {"get": with_params(op("Logic.GetRollout", "sprig.v1.Logic",
            "A staged rollout (active and candidate versions, share, key) and each version's calls, errors, p95 and histogram", None, "JsonReply"), &one)},
        "/v1/databases/{database}/logic/{kind}/{name}/logs": {"get": with_params(op("Logic.TailLogs", "sprig.v1.Logic",
            "The module's log lines after an id (after, limit)", None, "JsonReply"), &[p("database"), p("kind"), p("name"), q("after"), q("limit")])},
        "/v1/databases/{database}/logic/{kind}/{name}/deadLetters": {"get": with_params(op("Logic.ListDeadLetters", "sprig.v1.Logic",
            "The module's dead letters (state)", None, "JsonReply"), &[p("database"), p("kind"), p("name"), q("state")])},
        "/v1/databases/{database}/logic/{kind}/{name}/deadLetters:replay": {"post": with_params(op("Logic.ReplayDeadLetters", "sprig.v1.Logic",
            "Replays dead letters ({ids} or {all: true})", None, "JsonReply"), &one)},
        "/v1/databases/{database}/logic/{kind}/{name}/deadLetters:discard": {"post": with_params(op("Logic.DiscardDeadLetters", "sprig.v1.Logic",
            "Discards dead letters ({ids} or {all: true})", None, "JsonReply"), &one)},
        "/v1/databases/{database}/streams/{stream}/groups": {"get": with_params(op("Streams.ListGroups", "sprig.v1.Streams",
            "Partitions with log start and high watermark, and consumer groups with offsets and lag", None, "JsonReply"),
            &[p("database"), p("stream")])},
        "/v1/databases/{database}/mq": {"get": with_params(op("Mq.GetTopology", "sprig.v1.Mq",
            "The database's virtual host with its exchanges, queues and bindings", None, "JsonReply"), &[p("database")])},
        "/v1/databases/{database}/sinks/{sink}/deadLetters": {"get": with_params(op("Sinks.ListDeadLetters", "sprig.v1.Sinks",
            "The sink's dead letters", None, "JsonReply"), &[p("database"), p("sink")])},
        "/v1/databases/{database}/sinks/{sink}/deadLetters:replay": {"post": with_params(op("Sinks.ReplayDeadLetters", "sprig.v1.Sinks",
            "Marks dead letters for replay ({ids} or {all: true})", None, "JsonReply"), &[p("database"), p("sink")])},
        "/v1/databases/{database}/sinks/{sink}/deadLetters:discard": {"post": with_params(op("Sinks.DiscardDeadLetters", "sprig.v1.Sinks",
            "Discards dead letters ({ids} or {all: true})", None, "JsonReply"), &[p("database"), p("sink")])},
        "/v1/databases/{database}/sinks/{sink}:test": {"post": with_params(op("Sinks.Test", "sprig.v1.Sinks",
            "A saved sink's dry run ({sample}, 10 by default): TEST SINK as the caller; {columns, rows}", None, "JsonReply"),
            &[p("database"), p("sink")])},
        "/v1/databases/{database}/connections/{connection}:test": {"post": with_params(op("Connections.Test", "sprig.v1.Connections",
            "A saved connection's checks ({platform, write, to}): TEST [PLATFORM] CONNECTION [WRITE [TO 'to']] as the caller; {columns, rows}",
            None, "JsonReply"), &[p("database"), p("connection")])},
        "/v1/policies:testAs": {"post": op("Policies.TestAs", "sprig.v1.Policies",
            "One read-only statement run as another user of the tenant ({tenant, user, host, database, sql})", None, "JsonReply")},
        "/v1/library": {"get": with_params(op("Library.Get", "sprig.v1.Library",
            "The caller's saved queries and last runs (history: how many, 100; at most studio.history_max)", None, "JsonReply"),
            &[q("history")])},
        "/v1/library/saved": {"post": op("Library.Save", "sprig.v1.Library",
            "Saves {name, language, database, text} (redacted), replacing a query of that name", None, "JsonReply")},
        "/v1/library/saved:delete": {"post": op("Library.Delete", "sprig.v1.Library",
            "Deletes the caller's saved query {name}", None, "JsonReply")},
        "/v1/library/history": {"post": op("Library.Record", "sprig.v1.Library",
            "Records a run {language, database, text, ok, ms, error, client} (redacted)", None, "JsonReply")},
        "/v1/library/history:clear": {"post": op("Library.ClearHistory", "sprig.v1.Library",
            "Clears the caller's history", None, "JsonReply")},
    })
}

/// Chapter 52's approvals inbox.
fn recommendation_paths() -> Value {
    let p = |name: &str| json!({"name": name, "in": "path", "required": true, "schema": {"type": "string"}});
    json!({
        "/v1/recommendations": {"get": op("Recommendations.ListRecommendations", "sprig.admin.v1.Recommendations",
            "Chapter 52's approvals inbox: recommendations the caller may see (filters kind, state, level, all, db, table)",
            None, "JsonReply")},
        "/v1/recommendations/{id}": {
            "get": with_params(op("Recommendations.GetRecommendation", "sprig.admin.v1.Recommendations",
                "One recommendation with its evidence, history and comments", None, "JsonReply"), &[p("id")]),
            "post": with_params(op("Recommendations.ActOnRecommendation", "sprig.admin.v1.Recommendations",
                "{id}:approve, :ignore, :snooze, :reopen or :comment, as the caller", None, "JsonReply"), &[p("id")])},
        "/v1/recommendations:bulk": {"post": op("Recommendations.BulkAct", "sprig.admin.v1.Recommendations",
            "One action on up to 200 recommendations", None, "JsonReply")},
        "/v1/recommendations:policy": {"get": op("Recommendations.GetApprovalPolicy", "sprig.admin.v1.Recommendations",
            "Every kind's mode, level and timers at the tenant, a database (db) or a table (object)", None, "JsonReply")},
        "/v1/recommendations:subscriptions": {
            "get": op("Recommendations.ListSubscriptions", "sprig.admin.v1.Recommendations",
                "The tenant's approval email subscriptions", None, "JsonReply"),
            "post": op("Recommendations.SetSubscription", "sprig.admin.v1.Recommendations",
                "The caller's own address and delivery (immediate, digest or off)", None, "JsonReply")}
    })
}

#[allow(clippy::too_many_lines)] // one entry per route
fn core_paths() -> Value {
    let q = |name: &str| json!({"name": name, "in": "query", "required": false, "schema": {"type": "string"}});
    let p = |name: &str| json!({"name": name, "in": "path", "required": true, "schema": {"type": "string"}});
    let mut list = op(
        "Indexing.ListIndexes",
        "sprig.v1.Indexing",
        "The caller's tenant's automatic indexes",
        None,
        "JsonReply",
    );
    list["parameters"] = json!([q("db"), q("table"), q("state"), q("all")]);
    let mut recs = op(
        "Indexing.ListRecommendations",
        "sprig.v1.Indexing",
        "Recommendations awaiting approval",
        None,
        "JsonReply",
    );
    recs["parameters"] = json!([q("db"), q("table")]);
    let mut get = op(
        "Indexing.GetIndex",
        "sprig.v1.Indexing",
        "One index with its evidence and history",
        None,
        "JsonReply",
    );
    get["parameters"] = json!([p("index")]);
    let mut act = op(
        "Indexing.Act",
        "sprig.v1.Indexing",
        "index:approve|apply|reject|dismiss|pin|freeze|unpin|unfreeze|drop, for users who could create the index",
        Some("IndexActionRequest"),
        "JsonReply",
    );
    act["parameters"] = json!([p("index")]);
    let mut explain = op(
        "Indexing.ExplainQuery",
        "sprig.v1.Indexing",
        "fingerprint:explain",
        None,
        "JsonReply",
    );
    explain["parameters"] = json!([p("fingerprint")]);
    let mut decision = op(
        "Indexing.GetDecision",
        "sprig.v1.Indexing",
        "A decision-log entry",
        None,
        "JsonReply",
    );
    decision["parameters"] = json!([p("id")]);
    json!({
        "/v1/auth/login": {"post": op("Auth.Login", "sprig.v1.Auth",
            "Password login; the refresh token comes as an HttpOnly cookie (refresh_transport cookie, needs X-Sprig-CSRF) or in the body",
            Some("LoginRequest"), "TokenReply")},
        "/v1/auth/refresh": {"post": op("Auth.Refresh", "sprig.v1.Auth",
            "Rotates the refresh token (cookie with X-Sprig-CSRF, or refresh_token in the body)",
            Some("RefreshRequest"), "TokenReply")},
        "/v1/auth/logout": {"post": op("Auth.Logout", "sprig.v1.Auth", "Ends the session", None, "LogoutReply")},
        "/v1/auth/mfa": {"post": op("Auth.CompleteMfa", "sprig.v1.Auth",
            "Completes a login's challenge with a TOTP code or a recovery code (cookie rules as login)",
            Some("MfaRequest"), "TokenReply")},
        "/v1/auth/factors": {"get": op("Factors.List", "sprig.v1.Factors",
            "The caller's second factors (no secrets), recovery codes left, whether one is required", None, "JsonReply")},
        "/v1/auth/factors:enrol": {"post": op("Factors.Enrol", "sprig.v1.Factors",
            "A new TOTP seed, shown once; with a session or an enrol ticket", Some("FactorRequest"), "JsonReply")},
        "/v1/auth/factors:confirm": {"post": op("Factors.Confirm", "sprig.v1.Factors",
            "Activates the pending factor with a code; recovery codes shown once; an enrol ticket also signs in",
            Some("FactorRequest"), "JsonReply")},
        "/v1/auth/factors:reset": {"post": op("Factors.Reset", "sprig.v1.Factors",
            "Removes every factor and ends every session: one's own with a code, or a user's as an administrator",
            Some("FactorRequest"), "JsonReply")},
        "/v1/auth/impersonate": {"post": op("Auth.Impersonate", "sprig.v1.Auth",
            "A session as a user of one's tenant or a descendant, with a reason; no refresh token", Some("ImpersonateRequest"), "TokenReply")},
        "/v1/auth/me": {"get": op("Auth.WhoAmI", "sprig.v1.Auth",
            "The principal, session, tenant tree, grants and studio.* settings", None, "WhoAmI")},
        "/v1/exports:ticket": {"post": op("Exports.CreateTicket", "sprig.v1.Exports",
            "A short-lived ticket for a form POST /v1/exports (form field ticket)", Some("ExportTicketRequest"), "ExportTicket")},
        "/v1/users:setPassword": {"post": op("Users.SetPassword", "sprig.v1.Users",
            "ALTER USER ... IDENTIFIED BY, run as the caller, without the password in statement text",
            Some("SetPasswordRequest"), "JsonReply")},
        "/v1/users": {"get": op("Users.ListUsers", "sprig.v1.Users",
            "The tenant's accounts (all for CREATE USER or SPRIG_TENANT_ADMIN, else the caller)", None, "JsonReply")},
        "/v1/api-keys": {
            "get": op("ApiKeys.ListKeys", "sprig.v1.ApiKeys", "API keys, never their secrets", None, "JsonReply"),
            "post": op("ApiKeys.CreateKey", "sprig.v1.ApiKeys", "Creates a key; the reply is the only time it is shown",
                Some("CreateApiKeyRequest"), "CreatedApiKey")},
        "/v1/api-keys/{key_id}:revoke": {"post": op("ApiKeys.RevokeKey", "sprig.v1.ApiKeys", "Revokes a key", None, "JsonReply")},
        "/v1/indexing/indexes": {"get": list},
        "/v1/indexing/recommendations": {"get": recs},
        "/v1/indexing/indexes/{index}": {"get": get, "post": act},
        "/v1/indexing/queries/{fingerprint}": {"get": explain},
        "/v1/indexing/decisions/{id}": {"get": decision},
        "/v1/catalog": {"get": op("Catalog.ListObjects", "sprig.v1.Catalog",
            "The logical databases the caller can see; ETag of the catalog and auth versions (If-None-Match gives 304)",
            None, "CatalogDatabases")},
        "/v1/databases/{database}/catalog": {"get": with_params(op("Catalog.Snapshot", "sprig.v1.Catalog",
            "Tables, views, routines, graphs (labels and relationship types the caller may read) and columns of one database, read with the caller's rights; ETag as ListObjects",
            None, "CatalogSnapshot"), &[p("database")])},
        "/v1/databases/{database}/catalog/{kind}/{name}": {"get": with_params(op("Catalog.GetObject", "sprig.v1.Catalog",
            "One object (kind table, view, procedure or function): columns, indexes, definition",
            None, "JsonReply"), &[p("database"), p("kind"), p("name")])},
        "/v1/databases/{database}/sql:check": {"post": with_params(op("Sql.Check", "sprig.v1.Sql",
            "Each statement's class and its problems with positions (UTF-16 offsets), parsed and bound without running",
            Some("CheckRequest"), "CheckReply"), &[p("database")])},
        "/v1/workload/fingerprints": {"get": with_params(op("Workload.ListFingerprints", "sprig.v1.Workload",
            "The caller's tenant's top fingerprints by total time (hours, limit, source = all | client | studio)",
            None, "JsonReply"), &[q("hours"), q("limit"), q("source")])},
        "/v1/workload/fingerprints/{id}/series": {"get": with_params(op("Workload.Series", "sprig.v1.Workload",
            "One fingerprint per bucket: runs, p50, p95, total (days)", None, "JsonReply"), &[p("id"), q("days")])},
        "/v1/databases/{database}/cursors": {"post": with_params(op("Cursors.Open", "sprig.v1.Cursors",
            "Runs a read and keeps its stream for paging (first result only): skip rows, then the first page; the stream waits between fetches and closes after studio.cursor_idle_seconds",
            Some("CursorOpenRequest"), "CursorOpened"), &[p("database")])},
        "/v1/cursors/{cursor}:fetch": {"post": with_params(op("Cursors.Fetch", "sprig.v1.Cursors",
            "The next page; done closes the cursor (39934 unknown, 39935 expired)",
            Some("CursorFetchRequest"), "CursorPage"), &[p("cursor")])},
        "/v1/cursors/{cursor}:close": {"post": with_params(op("Cursors.Close", "sprig.v1.Cursors",
            "Closes the cursor and kills its statement", None, "CursorClosed"), &[p("cursor")])},
    })
}

/// The component schemas the paths use.
#[must_use]
pub fn schemas() -> Value {
    let s = |props: Value, req: &[&str]| json!({"type": "object", "properties": props, "required": req});
    let string = json!({"type": "string"});
    let int = json!({"type": "integer"});
    let boolean = json!({"type": "boolean"});
    let obj = json!({"type": "object", "additionalProperties": true});
    let nullable = json!({"type": "string", "nullable": true});
    json!({
        "LoginRequest": s(json!({"login": string, "password": string,
            "refresh_transport": {"type": "string", "enum": ["cookie", "body"]},
            "client": {"type": "string", "enum": ["studio", "api"]}}), &["login", "password"]),
        "RefreshRequest": s(json!({"refresh_token": string}), &[]),
        "TokenReply": s(json!({"access_token": string, "token_type": string, "expires_in": int,
            "session_id": string, "refresh_token": string}), &["access_token", "token_type", "expires_in", "session_id"]),
        "LogoutReply": s(json!({"ended": boolean}), &["ended"]),
        "TenantNode": s(json!({"name": nullable, "id": nullable, "path": nullable,
            "relation": {"type": "string", "enum": ["home", "admin", "foreign"]}}), &["name", "relation"]),
        "WhoAmI": s(json!({
            "principal": s(json!({"tenant": nullable, "tenant_id": nullable, "user": string,
                "host": string, "login": string}), &["user", "host", "login"]),
            "amr": {"type": "array", "items": string},
            "session": s(json!({"id": string, "client": string, "created_at_ms": string,
                "idle_minutes": int, "max_hours": int, "access_ttl_seconds": int}),
                &["id", "idle_minutes", "max_hours", "access_ttl_seconds"]),
            "tenants": {"type": "array", "items": {"$ref": "#/components/schemas/TenantNode"}},
            "grants": {"type": "array", "items": string},
            "privileges": s(json!({"tenant_admin": boolean, "settings_admin": boolean,
                "create_user": boolean, "grant_option": boolean, "index_all": boolean,
                "index_dbs": {"type": "array", "items": string}}),
                &["tenant_admin", "settings_admin", "create_user", "grant_option", "index_all", "index_dbs"]),
            "platform_roles": {"type": "array", "items": string},
            "studio": obj,
            "gateway": s(json!({"id": string, "dc": string, "studio_build": string}), &[]),
        }), &["principal", "amr", "session", "tenants", "grants", "privileges", "platform_roles", "studio"]),
        "ExportTicketRequest": s(json!({"database": string, "sql": string,
            "format": {"type": "string", "enum": ["csv", "ndjson"]}, "name": string}), &["sql", "format"]),
        "ExportTicket": s(json!({"ticket": string, "expires_in": int}), &["ticket", "expires_in"]),
        "SetPasswordRequest": s(json!({"user": string, "host": string, "password": string}), &["user", "password"]),
        "IndexActionRequest": s(json!({"reason": string}), &[]),
        "CreateApiKeyRequest": s(json!({"name": string, "user": string, "host": string,
            "expires_days": int, "allowed_cidrs": {"type": "array", "items": string}}), &["name"]),
        "CreatedApiKey": s(json!({"key_id": string, "key": string, "name": string}), &["key_id", "key", "name"]),
        "CatalogDatabases": s(json!({"databases": {"type": "array", "items": string}}), &["databases"]),
        "CatalogSnapshot": s(json!({"db": string, "objects": {"type": "array", "items": obj},
            "columns": obj}), &["db", "objects", "columns"]),
        "CheckRequest": s(json!({"sql": string}), &["sql"]),
        "CheckReply": s(json!({"statements": {"type": "array", "items": s(json!({
            "start": int, "end": int, "class": string, "checked": string,
            "diagnostics": {"type": "array", "items": s(json!({"severity": string, "source": string,
                "code": int, "name": string, "message": string, "start": int, "end": int}),
                &["severity", "message", "start", "end"])}}), &["start", "end", "class", "diagnostics"])}}),
            &["statements"]),
        "CursorOpenRequest": s(json!({"sql": string, "skip": int, "page_rows": int}), &["sql"]),
        "CursorFetchRequest": s(json!({"rows": int}), &[]),
        "CursorPage": s(json!({"rows": {"type": "array", "items": {"type": "array",
            "items": {"type": "string", "nullable": true}}}, "position": string, "done": boolean}),
            &["rows", "position", "done"]),
        "CursorOpened": s(json!({"name": string, "columns": {"type": "array", "items": obj},
            "rows": {"type": "array", "items": {"type": "array", "items": {"type": "string", "nullable": true}}},
            "position": string, "done": boolean, "idle_seconds": int, "end": obj}),
            &["name", "columns", "rows", "position", "done"]),
        "CursorClosed": s(json!({"closed": boolean}), &["closed"]),
        "MfaRequest": s(json!({"challenge": string, "code": string,
            "refresh_transport": {"type": "string", "enum": ["cookie", "body"]}}), &["challenge", "code"]),
        "FactorRequest": s(json!({"enrol": string, "code": string, "user": string, "host": string,
            "refresh_transport": {"type": "string", "enum": ["cookie", "body"]}}), &[]),
        "ImpersonateRequest": s(json!({"tenant": string, "user": string, "host": string,
            "reason": string, "minutes": int}), &["user", "reason"]),
    })
}

/// Adds Studio's paths and schemas to the data API's document.
pub fn merge_into(doc: &mut Value) {
    if let (Some(paths), Value::Object(add)) = (doc["paths"].as_object_mut(), paths()) {
        paths.extend(add);
    }
    if let (Some(s), Value::Object(add)) = (doc["components"]["schemas"].as_object_mut(), schemas())
    {
        s.extend(add);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The document Studio's client is generated from
    /// (`studio/src/api/openapi-v1.json`) matches this build. With
    /// `SPRIG_STUDIO_REGEN=1` it is rewritten.
    #[test]
    fn studio_openapi_document_is_current() {
        let mut doc = sprig_api::rest::openapi(false);
        merge_into(&mut doc);
        let text = serde_json::to_string_pretty(&doc).unwrap() + "\n";
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../studio/src/api/openapi-v1.json");
        if std::env::var_os("SPRIG_STUDIO_REGEN").is_some() {
            std::fs::write(&path, &text).unwrap();
            return;
        }
        let have = std::fs::read_to_string(&path).unwrap_or_default();
        // Compared as JSON values: in a workspace build serde_json's
        // `preserve_order` (another crate's feature) changes the key order
        // of `text`, not its content.
        let have: serde_json::Value = serde_json::from_str(&have).unwrap_or_default();
        assert!(
            have == doc,
            "studio/src/api/openapi-v1.json is stale: run SPRIG_STUDIO_REGEN=1 cargo test -p sprig-studio, then pnpm gen:api"
        );
    }

    #[test]
    fn merge_adds_paths() {
        let mut doc = sprig_api::rest::openapi(false);
        merge_into(&mut doc);
        assert!(doc["paths"]["/v1/auth/login"]["post"].is_object());
        assert!(doc["paths"]["/v1/databases/{database}/sql:query"].is_object());
        assert!(doc["components"]["schemas"]["WhoAmI"].is_object());
    }
}
