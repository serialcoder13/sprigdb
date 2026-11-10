//! The gateway's public REST API, as `sprig` uses it (chapter 37; chapter
//! 53, "Signing in" and "Running statements").

use std::fmt::Write as _;

use hyper::Method;
use serde_json::{Value, json};

use crate::error::{Result, ShellCode, ShellError};
use crate::http::{HttpClient, Response};
use crate::model::{Column, EndInfo, ResultEvent, Sink};

/// How calls authenticate.
#[derive(Clone, Default)]
pub enum Auth {
    /// No `Authorization` header.
    #[default]
    None,
    /// `Bearer <token>`: a session token or an API key.
    Bearer(String),
    /// HTTP Basic: user and password (chapter 14's Basic path).
    Basic(String, String),
}

impl std::fmt::Debug for Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Auth::None => f.write_str("None"),
            Auth::Bearer(_) => f.write_str("Bearer(***)"),
            Auth::Basic(u, _) => write!(f, "Basic({u}, ***)"),
        }
    }
}

/// Tokens from a login or a refresh.
#[derive(Clone, PartialEq, Eq)]
pub struct Tokens {
    /// The access token (`sprigat_...`).
    pub access_token: String,
    /// The refresh token.
    pub refresh_token: String,
    /// Seconds the access token lives.
    pub expires_in: u64,
}

impl std::fmt::Debug for Tokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Tokens(***, expires_in {})", self.expires_in)
    }
}

/// Caps sent with each statement (chapter 37, decision ST22).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Caps {
    /// Rows per result; 0: none.
    pub max_rows: u64,
    /// Cell bytes per result; 0: none.
    pub max_bytes: u64,
    /// Statement timeout in milliseconds; 0: none.
    pub timeout_ms: u64,
}

/// A client of the data API.
#[derive(Debug, Clone)]
pub struct DataClient {
    http: HttpClient,
    /// How calls authenticate.
    pub auth: Auth,
    /// `x-sprig-tenant` (chapter 29's `USE TENANT` over HTTP).
    pub tenant: Option<String>,
}

/// An API error from a JSON body and an HTTP status.
#[must_use]
pub fn error_from(status: u16, body: &[u8]) -> ShellError {
    let v: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let e = v.get("error").unwrap_or(&v);
    let code = e.get("code").and_then(Value::as_u64).unwrap_or(0);
    let message = e.get("message").and_then(Value::as_str).map_or_else(
        || String::from_utf8_lossy(body).trim().to_owned(),
        str::to_owned,
    );
    let mut err = ShellError::server(
        u32::try_from(code).unwrap_or(0),
        e.get("name").and_then(Value::as_str).unwrap_or_default(),
        if message.is_empty() {
            format!("HTTP {status}")
        } else {
            message
        },
    );
    err.http_status = status;
    err
}

fn enc(s: &str) -> String {
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

/// Percent-encodes one path segment or query value.
#[must_use]
pub fn encode(s: &str) -> String {
    enc(s)
}

fn s_of(v: &Value, k: &str) -> String {
    v.get(k)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn u64_of(v: &Value, k: &str) -> u64 {
    match v.get(k) {
        Some(Value::String(s)) => s.parse().unwrap_or(0),
        Some(Value::Number(n)) => n.as_u64().unwrap_or(0),
        _ => 0,
    }
}

/// A column from chapter 37's JSON.
#[must_use]
pub fn column_from(v: &Value) -> Column {
    Column {
        name: s_of(v, "name"),
        type_name: s_of(v, "type"),
        binary: v.get("binary").and_then(Value::as_bool).unwrap_or(false),
    }
}

/// A row from chapter 37's JSON: strings (base64 for binary columns) and
/// nulls.
#[must_use]
pub fn row_from(v: &Value) -> Vec<Option<String>> {
    v.as_array()
        .map(|a| {
            a.iter()
                .map(|c| match c {
                    Value::Null => None,
                    Value::String(s) => Some(s.clone()),
                    other => Some(other.to_string()),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// One NDJSON line of `Sql.Query` as an event; an `{"error"}` line as an
/// error.
///
/// # Errors
/// The error the line carries, or 39985 for a line that isn't JSON.
pub fn query_line(line: &[u8]) -> Result<Option<ResultEvent>> {
    let v: Value = serde_json::from_slice(line)
        .map_err(|e| ShellError::shell(ShellCode::Parse, format!("unexpected reply line: {e}")))?;
    if v.get("error").is_some() {
        return Err(error_from(200, line));
    }
    if let Some(h) = v.get("header") {
        let cols = h
            .get("columns")
            .and_then(Value::as_array)
            .map(|a| a.iter().map(column_from).collect())
            .unwrap_or_default();
        return Ok(Some(ResultEvent::Header(cols)));
    }
    if let Some(r) = v.get("rows") {
        let rows = r
            .get("rows")
            .and_then(Value::as_array)
            .map(|a| a.iter().map(row_from).collect())
            .unwrap_or_default();
        return Ok(Some(ResultEvent::Rows(rows)));
    }
    if let Some(e) = v.get("end") {
        return Ok(Some(ResultEvent::End(EndInfo {
            affected: u64_of(e, "affected_rows"),
            last_insert_id: u64_of(e, "last_insert_id"),
            warnings: u32::try_from(u64_of(e, "warnings")).unwrap_or(u32::MAX),
            info: s_of(e, "info"),
            truncated: e.get("truncated").and_then(Value::as_bool).unwrap_or(false),
        })));
    }
    Ok(None)
}

impl DataClient {
    /// A client over `http`.
    #[must_use]
    pub fn new(http: HttpClient, auth: Auth) -> Self {
        Self {
            http,
            auth,
            tenant: None,
        }
    }

    /// The HTTP client.
    #[must_use]
    pub fn http(&self) -> &HttpClient {
        &self.http
    }

    fn headers(&self, accept: &str) -> Vec<(&'static str, String)> {
        let mut h = vec![("accept", accept.to_owned())];
        match &self.auth {
            Auth::None => {}
            Auth::Bearer(t) => h.push(("authorization", format!("Bearer {t}"))),
            Auth::Basic(u, p) => {
                use base64::Engine as _;
                let b = base64::engine::general_purpose::STANDARD.encode(format!("{u}:{p}"));
                h.push(("authorization", format!("Basic {b}")));
            }
        }
        if let Some(t) = &self.tenant {
            h.push(("x-sprig-tenant", t.clone()));
        }
        h
    }

    async fn send(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
        accept: &str,
    ) -> Result<Response> {
        let bytes = body.map(|b| serde_json::to_vec(b).unwrap_or_default());
        self.http
            .request(method, path, &self.headers(accept), bytes)
            .await
    }

    /// A call answered with JSON.
    ///
    /// # Errors
    /// The API's error, or 39987.
    pub async fn call(&self, method: Method, path: &str, body: Option<&Value>) -> Result<Value> {
        let r = self.send(method, path, body, "application/json").await?;
        let status = r.status;
        let bytes = r.bytes().await?;
        if !(200..300).contains(&status) {
            return Err(error_from(status, &bytes));
        }
        if bytes.is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_slice(&bytes).map_err(|e| {
            ShellError::shell(ShellCode::Parse, format!("{path}: unexpected reply: {e}"))
        })
    }

    /// `GET path`.
    ///
    /// # Errors
    /// As [`DataClient::call`].
    pub async fn get(&self, path: &str) -> Result<Value> {
        self.call(Method::GET, path, None).await
    }

    /// `POST /v1/auth/login` with the refresh token in the reply's body.
    ///
    /// # Errors
    /// 1045 for a wrong password, 39921 for plain HTTP from another host.
    pub async fn login(&self, login: &str, password: &str) -> Result<Tokens> {
        let anon = Self::new(self.http.clone(), Auth::None);
        let v = anon
            .call(
                Method::POST,
                "/v1/auth/login",
                Some(&json!({
                    "login": login, "password": password,
                    "refresh_transport": "body", "client": "api",
                })),
            )
            .await?;
        Ok(tokens(&v))
    }

    /// `POST /v1/auth/refresh` with the refresh token in the body.
    ///
    /// # Errors
    /// 1045 (reused or revoked), 39924 (idle), 39925 (too old).
    pub async fn refresh(&self, refresh_token: &str) -> Result<Tokens> {
        let anon = Self::new(self.http.clone(), Auth::None);
        let v = anon
            .call(
                Method::POST,
                "/v1/auth/refresh",
                Some(&json!({ "refresh_token": refresh_token })),
            )
            .await?;
        Ok(tokens(&v))
    }

    /// `POST /v1/auth/logout` with the current access token.
    ///
    /// # Errors
    /// The API's error.
    pub async fn logout(&self) -> Result<()> {
        self.call(Method::POST, "/v1/auth/logout", Some(&json!({})))
            .await
            .map(|_| ())
    }

    /// `GET /v1/auth/me`.
    ///
    /// # Errors
    /// The API's error.
    pub async fn whoami(&self) -> Result<Value> {
        self.get("/v1/auth/me").await
    }

    /// Runs `sql` with `Sql.Query` as NDJSON, writing each event to `sink`
    /// as it arrives. Dropping the future drops the connection, and the
    /// gateway kills the statement.
    ///
    /// # Errors
    /// The statement's error, or 39987.
    pub async fn sql_query(
        &self,
        database: Option<&str>,
        sql: &str,
        transaction: Option<&str>,
        caps: Caps,
        sink: &mut dyn Sink,
    ) -> Result<()> {
        let db = database.filter(|d| !d.is_empty()).unwrap_or("-");
        let mut body = json!({ "sql": sql, "multi_statements": true });
        if let Some(t) = transaction {
            body["transaction"] = Value::String(t.to_owned());
        }
        if caps.max_rows > 0 {
            body["max_rows"] = json!(caps.max_rows.to_string());
        }
        if caps.max_bytes > 0 {
            body["max_bytes"] = json!(caps.max_bytes.to_string());
        }
        if caps.timeout_ms > 0 {
            body["timeout_ms"] = json!(caps.timeout_ms.to_string());
        }
        let path = format!("/v1/databases/{}/sql:query", enc(db));
        let mut r = self
            .send(Method::POST, &path, Some(&body), "application/x-ndjson")
            .await?;
        if !(200..300).contains(&r.status) {
            let status = r.status;
            return Err(error_from(status, &r.bytes().await?));
        }
        while let Some(line) = r.next_line().await? {
            if let Some(ev) = query_line(&line)? {
                sink.event(ev).map_err(|e| {
                    ShellError::shell(ShellCode::Cancelled, format!("output closed: {e}"))
                })?;
            }
        }
        Ok(())
    }

    /// `Transactions.Begin`: the handle (`transactions/...`).
    ///
    /// # Errors
    /// The API's error.
    pub async fn begin(&self, database: Option<&str>, read_only: bool) -> Result<String> {
        let db = database.filter(|d| !d.is_empty()).unwrap_or("-");
        let v = self
            .call(
                Method::POST,
                &format!("/v1/databases/{}/transactions", enc(db)),
                Some(&json!({ "read_only": read_only })),
            )
            .await?;
        Ok(s_of(&v, "name"))
    }

    /// `Transactions.Commit` or `Rollback`.
    ///
    /// # Errors
    /// The API's error (39782 for an expired handle).
    pub async fn end(&self, handle: &str, commit: bool) -> Result<()> {
        let verb = if commit { "commit" } else { "rollback" };
        self.call(
            Method::POST,
            &format!("/v1/{handle}:{verb}"),
            Some(&json!({ "transaction": handle })),
        )
        .await
        .map(|_| ())
    }

    /// `KeyValue.Exec`: one Redis command on a namespace.
    ///
    /// # Errors
    /// The API's error.
    pub async fn kv_exec(&self, database: &str, namespace: &str, argv: &[String]) -> Result<Value> {
        let v = self
            .call(
                Method::POST,
                &format!(
                    "/v1/databases/{}/namespaces/{}:exec",
                    enc(database),
                    enc(namespace)
                ),
                Some(&json!({ "namespace": namespace, "command": argv })),
            )
            .await?;
        Ok(v.get("reply").cloned().unwrap_or(v))
    }

    /// `Catalog.ListObjects`.
    ///
    /// # Errors
    /// The API's error.
    pub async fn catalog(&self) -> Result<Value> {
        self.get("/v1/catalog").await
    }

    /// `Catalog.Snapshot` of one database.
    ///
    /// # Errors
    /// The API's error.
    pub async fn database_catalog(&self, database: &str) -> Result<Value> {
        self.get(&format!("/v1/databases/{}/catalog", enc(database)))
            .await
    }

    /// `Workload.ListFingerprints`.
    ///
    /// # Errors
    /// The API's error.
    pub async fn fingerprints(&self, hours: u32, limit: u32) -> Result<Value> {
        self.get(&format!(
            "/v1/workload/fingerprints?hours={hours}&limit={limit}"
        ))
        .await
    }
}

fn tokens(v: &Value) -> Tokens {
    Tokens {
        access_token: s_of(v, "access_token"),
        refresh_token: s_of(v, "refresh_token"),
        expires_in: u64_of(v, "expires_in"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Collect;

    #[test]
    fn ndjson_lines() {
        let mut c = Collect::default();
        for l in [
            r#"{"header":{"index":0,"columns":[{"name":"id","type":"BIGINT","mysql_type":8},{"name":"b","type":"BLOB","mysql_type":252,"binary":true}]}}"#,
            r#"{"rows":{"rows":[["1","AAE="],["2",null]]}}"#,
            r#"{"end":{"index":0,"affected_rows":"0","warnings":1,"truncated":true}}"#,
        ] {
            if let Some(ev) = query_line(l.as_bytes()).expect("line") {
                c.event(ev).expect("sink");
            }
        }
        assert_eq!(c.0.len(), 3);
        match &c.0[0] {
            ResultEvent::Header(cols) => {
                assert!(cols[0].numeric() && cols[1].binary);
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(
            c.0[1],
            ResultEvent::Rows(vec![
                vec![Some("1".into()), Some("AAE=".into())],
                vec![Some("2".into()), None]
            ])
        );
        match &c.0[2] {
            ResultEvent::End(e) => assert!(e.truncated && e.warnings == 1),
            other => panic!("{other:?}"),
        }
        let e = query_line(
            br#"{"error":{"code":1146,"name":"","message":"Table 'shop.x' doesn't exist"}}"#,
        )
        .expect_err("error line");
        assert_eq!(e.code, 1146);
    }

    #[test]
    fn errors_and_encoding() {
        let e = error_from(403, br#"{"code":1142,"name":"","message":"denied"}"#);
        assert_eq!((e.code, e.http_status), (1142, 403));
        let e = error_from(502, b"bad gateway");
        assert_eq!(e.message, "bad gateway");
        assert_eq!(encode("my shop/x"), "my%20shop%2Fx");
        assert_eq!(
            format!("{:?}", Auth::Basic("u".into(), "pw".into())),
            "Basic(u, ***)"
        );
    }
}
