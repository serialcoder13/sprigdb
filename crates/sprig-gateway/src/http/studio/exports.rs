//! `Exports` (chapter 44, "Exports"; chapter 37, "APIs for Studio").
//!
//! ```text
//! POST /v1/exports:ticket {database, sql, format, name}   (bearer: a session token)
//!   → {ticket, expires_in}: a JWS (EdDSA, typ sprig-export) signed with this
//!     gateway's token key; payload = {sid, ten, sub, db, sql, fmt, exp (Unix
//!     ms), name}
//! POST /v1/exports  (an HTML form: ticket=<ticket>)
//!   1. same origin (39922); the signature (any gateway's published key), the
//!      expiry (studio.export_ticket_seconds) and the session (live, account
//!      checked now) → else 39929
//!   2. run the statement as the session's user, streaming
//!   3. CSV (RFC 4180, a header row, NULL as an empty field) or NDJSON (one
//!      object per row) as an attachment
//! ```
//!
//! The browser's download manager handles any size, and no ticket appears
//! in a URL. Since Studio round 2 tickets are signed like access tokens, so
//! the form post may reach any gateway (decision ST11's alternative).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bytes::Bytes;
use futures::StreamExt as _;
use http_body_util::StreamBody;
use hyper::body::{Frame, Incoming};
use hyper::header::{self, HeaderValue};
use hyper::{Request, Response};
use serde_json::{Map, Value, json};
use sprig_api::proto::v1::query_response::Part;
use sprig_common::{SprigCode, SprigError};
use sprig_auth::token::{Tokens, Who};
use sprig_studio::StudioConfig;
use tokio::sync::mpsc;
use tonic::body::Body;

use super::super::sink::Item;
use super::super::sql::{CHANNEL_DEPTH, Caller, SqlCall};
use super::super::{ConnInfo, Core};
use super::{hdr, json_response, parse_json, read_body, same_origin, session_caller, text};

/// Export formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// RFC 4180 CSV with a header row.
    Csv,
    /// One JSON object per row.
    Ndjson,
}

impl Format {
    /// `csv` or `ndjson`.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "csv" => Some(Self::Csv),
            "ndjson" | "jsonl" => Some(Self::Ndjson),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Ndjson => "ndjson",
        }
    }

    fn content_type(self) -> &'static str {
        match self {
            Self::Csv => "text/csv; charset=utf-8",
            Self::Ndjson => "application/x-ndjson",
        }
    }
}

fn invalid_ticket(why: &str) -> SprigError {
    SprigError::new(
        SprigCode::StudioExportTicketInvalid,
        format!("the export ticket is not valid: {why}"),
    )
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Signs `payload` with this gateway's token key.
///
/// # Errors
/// `Internal`.
pub fn sign(tokens: &Tokens, payload: &Value) -> Result<String, SprigError> {
    tokens.sign_ticket(payload)
}

/// The payload of a ticket signed by any gateway, checked for expiry at
/// `now_ms`.
///
/// # Errors
/// 39929.
pub async fn open(tokens: &Tokens, ticket: &str, now_ms: u64) -> Result<Value, SprigError> {
    let v: Value = tokens
        .verify_ticket(ticket, SystemTime::now())
        .await
        .map_err(|_| invalid_ticket("bad signature"))?;
    if v["exp"].as_u64().unwrap_or(0) <= now_ms {
        return Err(invalid_ticket("expired"));
    }
    Ok(v)
}

/// `Exports.CreateTicket`.
pub(super) async fn ticket(
    core: &Arc<Core>,
    cfg: &StudioConfig,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let (_caller, s) = session_caller(core, info, &req).await?;
    let body = parse_json(&read_body(req).await?)?;
    let fmt = Format::parse(&text(&body, "format")).ok_or_else(|| {
        SprigError::new(SprigCode::AdminRequestInvalid, "format is csv or ndjson")
    })?;
    let sql = text(&body, "sql");
    if sql.trim().is_empty() {
        return Err(SprigError::new(
            SprigCode::AdminRequestInvalid,
            "sql is required",
        ));
    }
    let ttl = cfg.export_ticket_seconds;
    let name: String = text(&body, "name")
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
        .take(80)
        .collect();
    let payload = json!({
        "sid": s.claims.sid,
        "ten": s.claims.ten,
        "sub": s.claims.sub,
        "db": text(&body, "database"),
        "sql": sql,
        "fmt": fmt.name(),
        "exp": now_ms() + ttl * 1000,
        "name": if name.is_empty() { "export".to_owned() } else { name },
    });
    let t = sign(&core.studio.tokens, &payload)?;
    Ok(json_response(
        200,
        &json!({ "ticket": t, "expires_in": ttl }),
    ))
}

/// Decodes `application/x-www-form-urlencoded` `key`.
fn form_value(body: &[u8], key: &str) -> Option<String> {
    let text = std::str::from_utf8(body).ok()?;
    text.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == key).then(|| {
            let v = v.replace('+', " ");
            let b = v.as_bytes();
            let mut out = Vec::with_capacity(b.len());
            let mut i = 0;
            while i < b.len() {
                if b[i] == b'%'
                    && let Some(h) = v.get(i + 1..i + 3)
                    && let Ok(n) = u8::from_str_radix(h, 16)
                {
                    out.push(n);
                    i += 3;
                    continue;
                }
                out.push(b[i]);
                i += 1;
            }
            String::from_utf8_lossy(&out).into_owned()
        })
    })
}

/// One CSV field.
#[must_use]
pub fn csv_field(v: Option<&str>) -> String {
    match v {
        None => String::new(),
        Some(s) if s.contains([',', '"', '\n', '\r']) || s.starts_with(' ') || s.ends_with(' ') => {
            format!("\"{}\"", s.replace('"', "\"\""))
        }
        Some(s) => s.to_owned(),
    }
}

fn cell(v: &prost_types::Value) -> Option<String> {
    use prost_types::value::Kind;
    match &v.kind {
        Some(Kind::StringValue(s)) => Some(s.clone()),
        Some(Kind::NumberValue(n)) => Some(n.to_string()),
        Some(Kind::BoolValue(b)) => Some(b.to_string()),
        _ => None,
    }
}

/// The rows of one message in `fmt`; `columns` holds the current
/// result's column names (set by its header).
fn encode(fmt: Format, part: &Part, columns: &mut Vec<String>, done: &mut bool) -> Vec<u8> {
    let mut out = Vec::new();
    match part {
        Part::Header(h) => {
            if *done {
                return out;
            }
            *columns = h.columns.iter().map(|c| c.name.clone()).collect();
            if fmt == Format::Csv {
                let line: Vec<String> = columns.iter().map(|c| csv_field(Some(c))).collect();
                out.extend_from_slice(line.join(",").as_bytes());
                out.extend_from_slice(b"\r\n");
            }
        }
        Part::Rows(b) => {
            if *done {
                return out;
            }
            for row in &b.rows {
                let cells: Vec<Option<String>> = row.values.iter().map(cell).collect();
                match fmt {
                    Format::Csv => {
                        let line: Vec<String> =
                            cells.iter().map(|c| csv_field(c.as_deref())).collect();
                        out.extend_from_slice(line.join(",").as_bytes());
                        out.extend_from_slice(b"\r\n");
                    }
                    Format::Ndjson => {
                        let mut m = Map::new();
                        for (i, c) in cells.into_iter().enumerate() {
                            let k = columns.get(i).cloned().unwrap_or_else(|| format!("_{i}"));
                            m.insert(k, c.map_or(Value::Null, Value::String));
                        }
                        out.extend_from_slice(Value::Object(m).to_string().as_bytes());
                        out.push(b'\n');
                    }
                }
            }
        }
        Part::End(_) => {
            // Only the first resultset is exported.
            if !columns.is_empty() {
                *done = true;
            }
        }
    }
    out
}

/// The form post: checks the ticket and streams the file.
pub(super) async fn download(
    core: &Arc<Core>,
    cfg: &StudioConfig,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    same_origin(
        cfg,
        info.tls.is_some(),
        hdr(&req, "host").as_deref(),
        hdr(&req, "origin").as_deref(),
        hdr(&req, "sec-fetch-site").as_deref(),
    )?;
    let body = read_body(req).await?;
    let ticket = form_value(&body, "ticket").ok_or_else(|| invalid_ticket("missing"))?;
    let st = &core.studio;
    let v = open(&st.tokens, &ticket, now_ms()).await?;
    let sid = v["sid"].as_str().unwrap_or("").to_owned();
    let (tid, uid) = v["sub"]
        .as_str()
        .and_then(|s| s.split_once(':'))
        .and_then(|(t, u)| Some((t.parse::<u64>().ok()?, u.parse::<u32>().ok()?)))
        .ok_or_else(|| invalid_ticket("malformed"))?;
    let sid_bytes: [u8; 16] = URL_SAFE_NO_PAD
        .decode(&sid)
        .ok()
        .and_then(|b| b.try_into().ok())
        .ok_or_else(|| invalid_ticket("malformed"))?;
    let (principal, _) = st
        .tokens
        .session_principal(
            &Who {
                tenant: v["ten"].as_str().unwrap_or("-"),
                tenant_id: tid,
                user_id: uid,
                sid: sid_bytes,
                client_ip: info.peer.ip(),
                tls: info.tls.is_some(),
            },
            SystemTime::now(),
        )
        .await
        .map_err(|_| invalid_ticket("the session ended or its account changed"))?;
    let fmt = Format::parse(v["fmt"].as_str().unwrap_or("")).unwrap_or(Format::Csv);
    let caller = Caller {
        principal,
        peer: info.peer,
        tls: info.tls.clone(),
        listener: info.listener,
        generation: info.generation,
        authorization: None,
        client_dc: None,
    };
    let call = SqlCall {
        database: v["db"].as_str().unwrap_or("-").to_owned(),
        sql: v["sql"].as_str().unwrap_or("").to_owned(),
        multi: false,
    };
    tracing::info!(target: "sprig_audit", event = "studio.export", sid = %sid,
        format = fmt.name(), "Studio export");
    metrics::counter!("sprig_studio_exports_total", "format" => fmt.name()).increment(1);
    let (tx, rx) = mpsc::channel(CHANNEL_DEPTH);
    let engine = Arc::clone(&core.engine);
    tokio::spawn(async move { engine.run(&caller, call, None, tx).await });
    let state = (Some(rx), Vec::<String>::new(), false);
    let stream = futures::stream::unfold(state, move |(rx, mut cols, mut done)| async move {
        let mut rx = rx?;
        loop {
            match rx.recv().await? {
                Item::Part(p) => {
                    let Some(part) = p.part else { continue };
                    let bytes = encode(fmt, &part, &mut cols, &mut done);
                    if bytes.is_empty() {
                        continue;
                    }
                    metrics::counter!("sprig_studio_export_bytes_total")
                        .increment(bytes.len() as u64);
                    return Some((
                        Ok::<_, std::io::Error>(Frame::data(Bytes::from(bytes))),
                        (Some(rx), cols, done),
                    ));
                }
                Item::Error(e) => {
                    // Breaking the body marks the download failed.
                    return Some((
                        Err(std::io::Error::other(e.message.clone())),
                        (None, cols, done),
                    ));
                }
            }
        }
    })
    .boxed();
    let name = v["name"].as_str().unwrap_or("export");
    let mut r = Response::new(Body::new(StreamBody::new(stream)));
    let h = r.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(fmt.content_type()),
    );
    if let Ok(cd) =
        HeaderValue::from_str(&format!("attachment; filename=\"{name}.{}\"", fmt.name()))
    {
        h.insert(header::CONTENT_DISPOSITION, cd);
    }
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    Ok(r)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_api::proto::v1::{Column, ResultHeader, RowBatch};

    fn tokens(gw: &str) -> Tokens {
        let keys = sprig_auth::token::KeyRing::new(
            sprig_common::ids::DcId::new("dc-a").unwrap(),
            gw,
            None,
            std::time::Duration::from_secs(86_400),
        )
        .unwrap();
        Tokens::new(
            Arc::new(keys),
            Arc::new(sprig_auth::token::MemorySessions::default()),
        )
    }

    #[tokio::test]
    async fn tickets_sign_verify_and_expire() {
        let k = tokens("gw-a");
        let t = sign(&k, &json!({"sid": "s1", "exp": 2000, "sql": "SELECT 1"})).unwrap();
        assert_eq!(open(&k, &t, 1000).await.unwrap()["sid"], "s1");
        let e = open(&k, &t, 2000).await.unwrap_err();
        assert_eq!(e.protocol().mysql_errno, 39929);
        // Another (unpublished) key, a changed body, garbage.
        assert!(open(&tokens("gw-b"), &t, 1000).await.is_err());
        let mut parts: Vec<&str> = t.split('.').collect();
        let body = URL_SAFE_NO_PAD.encode(r#"{"sid":"s2","exp":2000}"#);
        parts[1] = &body;
        assert!(open(&k, &parts.join("."), 1000).await.is_err());
        assert!(open(&k, "nope", 1000).await.is_err());
    }

    #[test]
    fn csv_quoting() {
        assert_eq!(csv_field(None), "");
        assert_eq!(csv_field(Some("a")), "a");
        assert_eq!(csv_field(Some("a,b")), "\"a,b\"");
        assert_eq!(csv_field(Some("say \"hi\"")), "\"say \"\"hi\"\"\"");
        assert_eq!(csv_field(Some("l1\nl2")), "\"l1\nl2\"");
    }

    #[test]
    fn form_values_decode() {
        assert_eq!(
            form_value(b"x=1&ticket=ab%2Ecd%3D+e", "ticket").as_deref(),
            Some("ab.cd= e")
        );
        assert_eq!(form_value(b"x=1", "ticket"), None);
    }

    #[test]
    fn encodes_the_first_result_only() {
        let header = Part::Header(ResultHeader {
            index: 0,
            columns: vec![
                Column {
                    name: "id".into(),
                    ..Default::default()
                },
                Column {
                    name: "note".into(),
                    ..Default::default()
                },
            ],
        });
        let s = |x: &str| prost_types::Value {
            kind: Some(prost_types::value::Kind::StringValue(x.into())),
        };
        let null = prost_types::Value {
            kind: Some(prost_types::value::Kind::NullValue(0)),
        };
        let rows = Part::Rows(RowBatch {
            rows: vec![prost_types::ListValue {
                values: vec![s("1"), null],
            }],
        });
        let end = Part::End(sprig_api::proto::v1::ResultEnd::default());
        let mut cols = Vec::new();
        let mut done = false;
        let mut out = Vec::new();
        for p in [&header, &rows, &end, &header, &rows] {
            out.extend(encode(Format::Csv, p, &mut cols, &mut done));
        }
        assert_eq!(String::from_utf8(out).unwrap(), "id,note\r\n1,\r\n");
        let (mut cols, mut done) = (Vec::new(), false);
        let mut out = Vec::new();
        for p in [&header, &rows] {
            out.extend(encode(Format::Ndjson, p, &mut cols, &mut done));
        }
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "{\"id\":\"1\",\"note\":null}\n"
        );
    }
}
