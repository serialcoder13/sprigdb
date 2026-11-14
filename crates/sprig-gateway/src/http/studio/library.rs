//! Saved queries and query history in the tenant's catalog, so Studio and
//! `sprig` share one library that follows the user between browsers and
//! machines.
//!
//! | Route | Does |
//! |---|---|
//! | `GET /v1/library?history=N` | The caller's saved queries (newest first) and last `N` runs (100; at most `studio.history_max`) |
//! | `POST /v1/library/saved` | Saves `{name, language, database, text}`, replacing a query of that name |
//! | `POST /v1/library/saved:delete` | Deletes `{name}` |
//! | `POST /v1/library/history` | Records a run `{language, database, text, ok, ms, error, client}` |
//! | `POST /v1/library/history:clear` | Clears the caller's history |
//!
//! - **Whose.** Each row belongs to one user of the tenant (`user_id`);
//!   every route reads and writes only the caller's own rows. A tenant
//!   login is needed (not the accounts file).
//! - **Where.** Two tables in `t<tenant>_sprig`, module `library`, created
//!   the first time a gateway needs them for a tenant. They aren't
//!   versioned catalog state: nothing is cached, every call reads them.
//! - **Secrets.** Text is redacted here as well as in the clients: a
//!   string after `IDENTIFIED BY`, `PASSWORD`, `SECRET`, `TOKEN` and the
//!   other secret words becomes `'***'`, words that look like API keys or
//!   session tokens become `***`, Redis `AUTH` arguments are masked, and
//!   text with an unclosed string is kept only as a placeholder.
//! - **Limits.** Names of 1 to 128 characters, text up to 64 KiB, errors
//!   cut to 500 characters, and `studio.history_max` runs per user (500;
//!   0 stops recording and clears nothing).
//! - **Left behind.** A dropped user's rows stay until the tenant is
//!   dropped; they are never shown to a new user (ids aren't reused).

use std::sync::Arc;

use hyper::Request;
use hyper::Response;
use hyper::body::Incoming;
use mysql_async::prelude::Queryable as _;
use serde_json::{Value, json};
use sprig_catalog::migrate::{MigrateOpts, Migration, Migrator, Module, Phase};
use sprig_common::names::quote;
use sprig_common::{SprigCode, SprigError};
use tonic::body::Body;

use super::super::sql::Caller;
use super::super::{ConnInfo, Core};
use super::{hdr, invalid, json_response, parse_json, read_body};

/// The catalog module.
pub const MODULE: &str = "library";

const DDL_SAVED: &str = "CREATE TABLE IF NOT EXISTS library_saved (
  user_id   INT UNSIGNED NOT NULL,
  name      VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
  language  VARCHAR(16) NOT NULL,
  db_name   VARCHAR(64) NULL,
  body      MEDIUMTEXT NOT NULL,
  saved_at  DATETIME(3) NOT NULL,
  PRIMARY KEY (user_id, name)
) ENGINE=InnoDB";

const DDL_HISTORY: &str = "CREATE TABLE IF NOT EXISTS library_history (
  run_id    BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id   INT UNSIGNED NOT NULL,
  language  VARCHAR(16) NOT NULL,
  db_name   VARCHAR(64) NULL,
  body      MEDIUMTEXT NOT NULL,
  ok        TINYINT(1) NOT NULL,
  ms        INT UNSIGNED NOT NULL,
  error     VARCHAR(512) NULL,
  client    VARCHAR(16) NOT NULL,
  ran_at    DATETIME(3) NOT NULL,
  PRIMARY KEY (run_id),
  KEY by_user (user_id, run_id)
) ENGINE=InnoDB";

/// The module's migrations.
pub static LIBRARY_MIGRATIONS: &[Migration] = &[Migration {
    format: 1,
    phase: Phase::Expand,
    name: "library_saved, library_history",
    statements: &[DDL_SAVED, DDL_HISTORY],
}];

/// Longest text kept, in bytes.
pub const MAX_TEXT: usize = 64 * 1024;
/// Longest name, in characters.
pub const MAX_NAME: usize = 128;
const MAX_ERROR: usize = 500;
const LANGUAGES: &[&str] = &["sql", "mongo", "redis", "cypher", "gql"];
const CLIENTS: &[&str] = &["studio", "shell", "other"];

// ---- redaction ----

/// Words after which the next string literal is a secret (the shell's
/// list).
const SECRET_WORDS: &[&str] = &[
    "BY",
    "PASSWORD",
    "SECRET",
    "TOKEN",
    "CREDENTIALS",
    "API_KEY",
    "ACCESS_KEY",
    "SECRET_KEY",
    "SASL_PASSWORD",
    "PWD",
    "AUTH",
];

const TOKEN_PREFIXES: &[&str] = &["sprig_", "sprigat_", "sprigrt_"];

fn looks_like_token(w: &str) -> bool {
    let w = w.trim_matches(|c| c == '\'' || c == '"');
    TOKEN_PREFIXES
        .iter()
        .any(|p| w.starts_with(p) && w.len() > p.len() + 8)
}

/// `text` with its secrets masked; `language` is one of [`LANGUAGES`].
#[must_use]
pub fn redact(text: &str, language: &str) -> String {
    if language == "redis" {
        return redact_redis(text);
    }
    let b = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut armed = false;
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'\'' || c == b'"' || c == b'`' {
            let Some(end) = string_end(b, i) else {
                return "-- redacted: a statement with an unclosed string".to_owned();
            };
            let lit = &text[i..end];
            if c != b'`' && (armed || looks_like_token(lit)) {
                out.push_str(if c == b'"' { "\"***\"" } else { "'***'" });
            } else {
                out.push_str(lit);
            }
            if c != b'`' {
                armed = false;
            }
            i = end;
        } else if (c == b'-' && b.get(i + 1) == Some(&b'-'))
            || c == b'#'
            || (c == b'/' && b.get(i + 1) == Some(&b'/'))
        {
            let end = b[i..]
                .iter()
                .position(|&x| x == b'\n')
                .map_or(b.len(), |p| i + p);
            out.push_str(&text[i..end]);
            i = end;
        } else if c == b'/' && b.get(i + 1) == Some(&b'*') {
            let end = text[i + 2..].find("*/").map_or(b.len(), |p| i + 2 + p + 2);
            out.push_str(&text[i..end]);
            i = end;
        } else if c.is_ascii_alphanumeric() || c == b'_' || c == b'$' {
            let end = b[i..]
                .iter()
                .position(|&x| !(x.is_ascii_alphanumeric() || x == b'_' || x == b'$'))
                .map_or(b.len(), |p| i + p);
            let w = &text[i..end];
            if looks_like_token(w) {
                out.push_str("***");
                armed = false;
            } else {
                let up = w.to_ascii_uppercase();
                armed = SECRET_WORDS.contains(&up.as_str())
                    || (armed && matches!(up.as_str(), "USING" | "WITH"));
                out.push_str(w);
            }
            i = end;
        } else {
            // Spaces, `=`, `:` and `(` keep a secret word armed; anything
            // else ends it.
            if !(c.is_ascii_whitespace() || matches!(c, b'=' | b':' | b'(')) {
                armed = false;
            }
            let n = utf8_len(c);
            out.push_str(&text[i..(i + n).min(b.len())]);
            i += n;
        }
    }
    out
}

fn utf8_len(c: u8) -> usize {
    match c {
        0xF0..=0xFF => 4,
        0xE0..=0xEF => 3,
        0xC0..=0xDF => 2,
        _ => 1,
    }
}

/// The end (exclusive) of the quoted string starting at `start`, with
/// doubled quotes and backslash escapes; `None` when it isn't closed.
fn string_end(b: &[u8], start: usize) -> Option<usize> {
    let q = b[start];
    let mut i = start + 1;
    while i < b.len() {
        if b[i] == b'\\' && q != b'`' {
            i += 2;
        } else if b[i] == q {
            if b.get(i + 1) == Some(&q) {
                i += 2;
            } else {
                return Some(i + 1);
            }
        } else {
            i += 1;
        }
    }
    None
}

fn redact_redis(text: &str) -> String {
    text.lines()
        .map(|line| {
            let words: Vec<&str> = line.split_whitespace().collect();
            let auth = words
                .iter()
                .position(|w| w.eq_ignore_ascii_case("AUTH"))
                .or_else(|| {
                    words
                        .iter()
                        .position(|w| w.eq_ignore_ascii_case("requirepass"))
                        .filter(|_| {
                            words
                                .first()
                                .is_some_and(|w| w.eq_ignore_ascii_case("CONFIG"))
                        })
                });
            match auth {
                Some(at) => words
                    .iter()
                    .enumerate()
                    .map(|(i, w)| if i > at { "***" } else { w })
                    .collect::<Vec<_>>()
                    .join(" "),
                None => words
                    .iter()
                    .map(|w| if looks_like_token(w) { "***" } else { w })
                    .collect::<Vec<_>>()
                    .join(" "),
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

// ---- requests ----

/// A save request, checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaveReq {
    /// The name.
    pub name: String,
    /// The language.
    pub language: String,
    /// The database, if any.
    pub database: Option<String>,
    /// The redacted text.
    pub text: String,
}

fn language_of(v: &Value) -> Result<String, SprigError> {
    let l = v
        .get("language")
        .and_then(Value::as_str)
        .unwrap_or("sql")
        .to_ascii_lowercase();
    if LANGUAGES.contains(&l.as_str()) {
        Ok(l)
    } else {
        Err(invalid(format!(
            "language must be one of {}",
            LANGUAGES.join(", ")
        )))
    }
}

fn database_of(v: &Value) -> Result<Option<String>, SprigError> {
    match v.get("database").and_then(Value::as_str).map(str::trim) {
        None | Some("") => Ok(None),
        Some(d) if d.chars().count() > 64 => Err(invalid("database is longer than 64 characters")),
        Some(d) => Ok(Some(d.to_owned())),
    }
}

fn text_of(v: &Value, language: &str) -> Result<String, SprigError> {
    let t = v.get("text").and_then(Value::as_str).unwrap_or("").trim();
    if t.is_empty() {
        return Err(invalid("text is empty"));
    }
    if t.len() > MAX_TEXT {
        return Err(SprigError::new(
            SprigCode::RequestTooLarge,
            format!("text is longer than {MAX_TEXT} bytes"),
        ));
    }
    Ok(redact(t, language))
}

/// Checks a save request.
///
/// # Errors
/// A missing or too long name, an unknown language, empty or too long text.
pub fn save_req(v: &Value) -> Result<SaveReq, SprigError> {
    let name = v.get("name").and_then(Value::as_str).unwrap_or("").trim();
    if name.is_empty() || name.chars().count() > MAX_NAME {
        return Err(invalid(format!(
            "a saved query's name must be 1 to {MAX_NAME} characters"
        )));
    }
    let language = language_of(v)?;
    Ok(SaveReq {
        name: name.to_owned(),
        database: database_of(v)?,
        text: text_of(v, &language)?,
        language,
    })
}

/// A run to record, checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunReq {
    /// The language.
    pub language: String,
    /// The database, if any.
    pub database: Option<String>,
    /// The redacted text.
    pub text: String,
    /// Whether it succeeded.
    pub ok: bool,
    /// How long it took.
    pub ms: u32,
    /// The error, cut to 500 characters and redacted.
    pub error: Option<String>,
    /// `studio`, `shell` or `other`.
    pub client: String,
}

/// Checks a run to record.
///
/// # Errors
/// An unknown language, empty or too long text.
pub fn run_req(v: &Value) -> Result<RunReq, SprigError> {
    let language = language_of(v)?;
    let client = v
        .get("client")
        .and_then(Value::as_str)
        .unwrap_or("other")
        .to_ascii_lowercase();
    let error = v
        .get("error")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|e| !e.is_empty())
        .map(|e| redact(&e.chars().take(MAX_ERROR).collect::<String>(), "sql"));
    Ok(RunReq {
        database: database_of(v)?,
        text: text_of(v, &language)?,
        ok: v
            .get("ok")
            .and_then(Value::as_bool)
            .unwrap_or(error.is_none()),
        ms: v.get("ms").and_then(Value::as_f64).map_or(0, whole_ms),
        error,
        client: if CLIENTS.contains(&client.as_str()) {
            client
        } else {
            "other".to_owned()
        },
        language,
    })
}

/// `m` milliseconds as a whole number, clamped to `u32`.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)] // clamped first
fn whole_ms(m: f64) -> u32 {
    if m.is_nan() {
        0
    } else {
        m.clamp(0.0, f64::from(u32::MAX)) as u32
    }
}

fn query_u64(query: &str, key: &str) -> Option<u64> {
    query.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == key).then(|| v.parse().ok()).flatten()
    })
}

// ---- storage ----

struct Ctx {
    conn: mysql_async::Conn,
    schema: String,
    user: u32,
    history_max: u64,
}

impl Ctx {
    fn t(&self, table: &str) -> String {
        format!("{}.{}", quote(&self.schema), quote(table))
    }
}

fn unavailable(e: mysql_async::Error) -> SprigError {
    SprigError::new(
        SprigCode::CatalogUnavailable,
        "the tenant's catalog is unreachable",
    )
    .with_source(e)
}

async fn context(core: &Arc<Core>, caller: &Caller) -> Result<Ctx, SprigError> {
    let (Some(svc), Some(tid), Some(user)) = (
        &core.studio.tenants,
        caller.principal.tenant,
        caller.principal.user_id,
    ) else {
        return Err(invalid("saved queries need a tenant login"));
    };
    let ep = sprig_catalog::CatalogLocator::writer(&*svc.locator, tid).await?;
    let mut conn = ep.pool.get_conn().await.map_err(unavailable)?;
    let schema = ep.store.schema().to_owned();
    let have: Option<u64> = conn
        .exec_first(
            "SELECT COUNT(*) FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'library_history'",
            (schema.as_str(),),
        )
        .await
        .map_err(unavailable)?;
    if have.unwrap_or(0) == 0 {
        Migrator::new(&[Module {
            name: MODULE,
            migrations: LIBRARY_MIGRATIONS,
        }])?
        .run(&mut conn, &schema, &MigrateOpts::default())
        .await?;
    }
    let history_max = core
        .studio
        .tenant_get(&sprig_studio::settings::HISTORY_MAX, Some(tid), None);
    Ok(Ctx {
        conn,
        schema,
        user: user.0,
        history_max,
    })
}

type SavedRow = (String, String, Option<String>, String, i64);
type RunRow = (
    u64,
    String,
    Option<String>,
    String,
    bool,
    u32,
    Option<String>,
    String,
    i64,
);

async fn list(c: &mut Ctx, history: u64) -> Result<Value, SprigError> {
    let saved: Vec<SavedRow> = c
        .conn
        .exec(
            format!(
                "SELECT name, language, db_name, body, \
                 CAST(TIMESTAMPDIFF(MICROSECOND, '1970-01-01', saved_at) DIV 1000 AS SIGNED) \
                 FROM {} WHERE user_id = ? ORDER BY saved_at DESC, name",
                c.t("library_saved")
            ),
            (c.user,),
        )
        .await
        .map_err(unavailable)?;
    let runs: Vec<RunRow> = if history == 0 {
        Vec::new()
    } else {
        c.conn
            .exec(
                format!(
                    "SELECT run_id, language, db_name, body, ok, ms, error, client, \
                     CAST(TIMESTAMPDIFF(MICROSECOND, '1970-01-01', ran_at) DIV 1000 AS SIGNED) \
                     FROM {} WHERE user_id = ? ORDER BY run_id DESC LIMIT ?",
                    c.t("library_history")
                ),
                (c.user, history),
            )
            .await
            .map_err(unavailable)?
    };
    Ok(json!({
        "saved": saved.into_iter().map(|(name, language, db, text, at)| json!({
            "name": name, "language": language, "database": db, "text": text, "saved_at_ms": at,
        })).collect::<Vec<_>>(),
        "history": runs.into_iter().map(|(id, language, db, text, ok, ms, error, client, at)| json!({
            "id": id.to_string(), "language": language, "database": db, "text": text,
            "ok": ok, "ms": ms, "error": error, "client": client, "at_ms": at,
        })).collect::<Vec<_>>(),
        "history_max": c.history_max,
    }))
}

async fn save(c: &mut Ctx, r: &SaveReq) -> Result<(), SprigError> {
    c.conn
        .exec_drop(
            format!(
                "INSERT INTO {} (user_id, name, language, db_name, body, saved_at) \
                 VALUES (?, ?, ?, ?, ?, UTC_TIMESTAMP(3)) \
                 ON DUPLICATE KEY UPDATE language = VALUES(language), db_name = VALUES(db_name), \
                 body = VALUES(body), saved_at = VALUES(saved_at)",
                c.t("library_saved")
            ),
            (c.user, &r.name, &r.language, &r.database, &r.text),
        )
        .await
        .map_err(unavailable)
}

async fn delete(c: &mut Ctx, name: &str) -> Result<bool, SprigError> {
    let t = c.t("library_saved");
    c.conn
        .exec_drop(
            format!("DELETE FROM {t} WHERE user_id = ? AND name = ?"),
            (c.user, name),
        )
        .await
        .map_err(unavailable)?;
    Ok(c.conn.affected_rows() > 0)
}

async fn record(c: &mut Ctx, r: &RunReq) -> Result<bool, SprigError> {
    if c.history_max == 0 {
        return Ok(false);
    }
    let t = c.t("library_history");
    c.conn
        .exec_drop(
            format!(
                "INSERT INTO {t} (user_id, language, db_name, body, ok, ms, error, client, ran_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3))"
            ),
            (
                c.user,
                &r.language,
                &r.database,
                &r.text,
                r.ok,
                r.ms,
                &r.error,
                &r.client,
            ),
        )
        .await
        .map_err(unavailable)?;
    // Keep the newest `history_max`.
    let cut: Option<u64> = c
        .conn
        .exec_first(
            format!(
                "SELECT run_id FROM {t} WHERE user_id = ? ORDER BY run_id DESC LIMIT 1 OFFSET ?"
            ),
            (c.user, c.history_max),
        )
        .await
        .map_err(unavailable)?;
    if let Some(cut) = cut {
        c.conn
            .exec_drop(
                format!("DELETE FROM {t} WHERE user_id = ? AND run_id <= ?"),
                (c.user, cut),
            )
            .await
            .map_err(unavailable)?;
    }
    Ok(true)
}

async fn clear(c: &mut Ctx) -> Result<(), SprigError> {
    let t = c.t("library_history");
    c.conn
        .exec_drop(format!("DELETE FROM {t} WHERE user_id = ?"), (c.user,))
        .await
        .map_err(unavailable)
}

// ---- routes ----

/// Whether `path` is one of the library's.
#[must_use]
pub fn is_library_path(path: &str) -> bool {
    path == "/v1/library" || path.starts_with("/v1/library/")
}

/// Answers `/v1/library...`.
pub(super) async fn route(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let method = req.method().as_str().to_owned();
    let path = req.uri().path().to_owned();
    let query = req.uri().query().unwrap_or("").to_owned();
    let caller = core
        .caller(info, hdr(&req, "authorization"), None, hdr(&req, "host"))
        .await?;
    let body = if method == "POST" {
        parse_json(&read_body(req).await?)?
    } else {
        json!({})
    };
    match (method.as_str(), path.as_str()) {
        ("GET", "/v1/library") => {
            let mut c = context(core, &caller).await?;
            let n = query_u64(&query, "history")
                .unwrap_or(100)
                .min(c.history_max);
            Ok(json_response(200, &list(&mut c, n).await?))
        }
        ("POST", "/v1/library/saved") => {
            let r = save_req(&body)?;
            let mut c = context(core, &caller).await?;
            save(&mut c, &r).await?;
            Ok(json_response(
                200,
                &json!({"name": r.name, "language": r.language, "database": r.database, "text": r.text}),
            ))
        }
        ("POST", "/v1/library/saved:delete") => {
            let name = body
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim();
            if name.is_empty() {
                return Err(invalid("name is empty"));
            }
            let mut c = context(core, &caller).await?;
            let deleted = delete(&mut c, name).await?;
            Ok(json_response(200, &json!({"deleted": deleted})))
        }
        ("POST", "/v1/library/history") => {
            let r = run_req(&body)?;
            let mut c = context(core, &caller).await?;
            let recorded = record(&mut c, &r).await?;
            Ok(json_response(200, &json!({"recorded": recorded})))
        }
        ("POST", "/v1/library/history:clear") => {
            let mut c = context(core, &caller).await?;
            clear(&mut c).await?;
            Ok(json_response(200, &json!({})))
        }
        _ => Err(invalid(format!("no route {method} {path}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_are_masked() {
        assert_eq!(
            redact("CREATE USER bob IDENTIFIED BY 'hunter2'", "sql"),
            "CREATE USER bob IDENTIFIED BY '***'"
        );
        assert_eq!(
            redact(
                "ALTER USER bob IDENTIFIED VIA ed25519 USING PASSWORD('x''y')",
                "sql"
            ),
            "ALTER USER bob IDENTIFIED VIA ed25519 USING PASSWORD('***')"
        );
        assert_eq!(
            redact("SELECT 'by' FROM t WHERE a = 'open'", "sql"),
            "SELECT 'by' FROM t WHERE a = 'open'"
        );
        assert_eq!(
            redact(
                "CREATE CONNECTION c WITH (password = 'p', host = 'h')",
                "sql"
            ),
            "CREATE CONNECTION c WITH (password = '***', host = 'h')"
        );
        assert_eq!(
            redact(
                "db.users.insertOne({name: \"a\", password: \"pw\"})",
                "mongo"
            ),
            "db.users.insertOne({name: \"a\", password: \"***\"})"
        );
        assert_eq!(
            redact("SELECT 'sprigat_0123456789abcdef'", "sql"),
            "SELECT '***'"
        );
        assert_eq!(
            redact("SELECT * FROM t -- by 'x'\nWHERE b = 'é'", "sql"),
            "SELECT * FROM t -- by 'x'\nWHERE b = 'é'"
        );
        assert!(redact("CREATE USER a IDENTIFIED BY 'oops", "sql").starts_with("-- redacted"));
        assert_eq!(redact("AUTH alice s3cret", "redis"), "AUTH *** ***");
        assert_eq!(
            redact("CONFIG SET requirepass x", "redis"),
            "CONFIG SET requirepass ***"
        );
        assert_eq!(redact("GET k", "redis"), "GET k");
    }

    #[test]
    fn requests_are_checked() {
        let r = save_req(&json!({"name": " open orders ", "text": "SELECT 1", "database": "shop"}))
            .expect("ok");
        assert_eq!(r.name, "open orders");
        assert_eq!(r.language, "sql");
        assert_eq!(r.database.as_deref(), Some("shop"));
        assert!(save_req(&json!({"name": "", "text": "SELECT 1"})).is_err());
        assert!(save_req(&json!({"name": "x".repeat(129), "text": "SELECT 1"})).is_err());
        assert!(save_req(&json!({"name": "a", "text": "  "})).is_err());
        assert!(save_req(&json!({"name": "a", "text": "x", "language": "cobol"})).is_err());
        assert!(save_req(&json!({"name": "a", "text": "x".repeat(MAX_TEXT + 1)})).is_err());

        let r =
            run_req(&json!({"text": "SELECT 1", "ms": 12.6, "client": "shell", "language": "SQL"}))
                .expect("ok");
        assert!(r.ok);
        assert_eq!(
            (r.ms, r.client.as_str(), r.language.as_str()),
            (12, "shell", "sql")
        );
        let r = run_req(&json!({"text": "SELEC 1", "error": "syntax", "client": "x"})).expect("ok");
        assert!(!r.ok);
        assert_eq!(r.client, "other");
        assert_eq!(r.error.as_deref(), Some("syntax"));
    }

    #[test]
    fn paths_and_queries() {
        assert!(is_library_path("/v1/library"));
        assert!(is_library_path("/v1/library/saved:delete"));
        assert!(!is_library_path("/v1/libraryx"));
        assert_eq!(query_u64("history=20&x=1", "history"), Some(20));
        assert_eq!(query_u64("x=1", "history"), None);
    }
}
