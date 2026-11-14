//! One chat turn as an NDJSON stream: one JSON object per line, each with
//! a `type`.
//!
//! | `type` | Fields | When |
//! |---|---|---|
//! | `start` | `conversation` (id or null), `role`, `model`, `share_data` | first |
//! | `text` | `text` | more answer text |
//! | `thinking` | `text` | more reasoning text, when the provider shows it |
//! | `tool_start` | `id`, `name`, `input` | a tool call starts |
//! | `tool_end` | `id`, `name`, `ok`, `reduced` | it ended; `reduced` when rows or values were withheld |
//! | `proposal` | `proposal: {id, kind, tool_use_id, body}`, `filed` | something for the user to review; never run |
//! | `usage` | `usage` | tokens of one model call |
//! | `sources` | `sources: [{kind, title, reference}]` | what the answer drew on |
//! | `error` | `error: {code, name, message, ...}` | the turn failed (39992, 39993, 39996, ...), or the model refused (39994) |
//! | `end` | `stop` | the turn ended: `end_turn`, `max_tokens`, `refusal` or `other` (`detail`: `max_tool_rounds`, `stopped`) |
//!
//! Errors found before the turn starts (39990, 39991, 39992, 39995, a
//! role the caller may not use) are answered as plain HTTP errors instead;
//! once the stream has started they arrive as an `error` line. A client
//! that goes away stops the turn.

use std::fmt::Write as _;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt as _;
use http_body_util::StreamBody;
use hyper::Response;
use hyper::body::Frame;
use hyper::header::{self, HeaderValue};
use serde_json::{Value, json};
use sprig_ai::glossary::{GlossaryStore, NewTerm};
use sprig_ai::provider::Stop;
use sprig_ai::{Agent, ConvId, ProposalKind, Role, TurnConfig, TurnEvent, TurnRequest, TurnSink};
use sprig_common::ids::UserId;
use sprig_common::{CancellationToken, Result, SprigError};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tonic::body::Body;

/// Everything one turn needs, resolved before the stream starts.
pub struct Turn {
    /// The provider, tools and store.
    pub agent: Agent,
    /// Its configuration (from the settings).
    pub cfg: TurnConfig,
    /// The request (its `cancel` is set here).
    pub req: TurnRequest,
    /// Where `propose_glossary` proposals are filed (tenant roles).
    pub glossary: Option<Arc<dyn GlossaryStore>>,
    /// The caller's user name, as a glossary proposal's author.
    pub author: String,
}

impl std::fmt::Debug for Turn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Turn")
            .field("role", &self.cfg.role)
            .field("req", &self.req)
            .finish_non_exhaustive()
    }
}

/// The `error` line of `e`.
#[must_use]
pub fn error_json(e: &SprigError) -> Value {
    let (_, body) = sprig_api::status::to_rest(e);
    json!({"type": "error", "error": body})
}

/// The lines one turn event becomes.
#[must_use]
pub fn event_json(e: &TurnEvent) -> Vec<Value> {
    match e {
        TurnEvent::Text(t) => vec![json!({"type": "text", "text": t})],
        TurnEvent::Thinking(t) => vec![json!({"type": "thinking", "text": t})],
        TurnEvent::ToolStart { id, name, input } => {
            vec![json!({"type": "tool_start", "id": id, "name": name, "input": input})]
        }
        TurnEvent::ToolEnd {
            id,
            name,
            ok,
            reduced,
        } => {
            vec![json!({"type": "tool_end", "id": id, "name": name, "ok": ok, "reduced": reduced})]
        }
        TurnEvent::Proposal(p) => vec![json!({
            "type": "proposal",
            "proposal": p,
            "filed": if p.kind == ProposalKind::Glossary { json!("glossary") } else { Value::Null },
        })],
        TurnEvent::Usage(u) => vec![json!({"type": "usage", "usage": u})],
        TurnEvent::Sources(s) => vec![json!({"type": "sources", "sources": s})],
        TurnEvent::End { stop } => {
            let mut out = Vec::new();
            if let Stop::Refusal(t) = stop {
                out.push(error_json(&sprig_ai::error::refused(t)));
            }
            let mut end = json!({"type": "end", "stop": stop.label()});
            if let Stop::Other(d) = stop {
                end["detail"] = json!(d);
            }
            out.push(end);
            out
        }
    }
}

fn line(v: &Value) -> Bytes {
    let mut b = v.to_string().into_bytes();
    b.push(b'\n');
    Bytes::from(b)
}

struct LineSink {
    tx: mpsc::UnboundedSender<Bytes>,
    glossary: Option<Arc<dyn GlossaryStore>>,
    user: UserId,
    author: String,
    filing: Vec<JoinHandle<()>>,
}

impl TurnSink for LineSink {
    fn event(&mut self, e: TurnEvent) -> Result<()> {
        if let TurnEvent::Proposal(p) = &e
            && p.kind == ProposalKind::Glossary
            && let Some(g) = &self.glossary
        {
            let g = Arc::clone(g);
            let (user, author, id, body) =
                (self.user, self.author.clone(), p.id.clone(), p.body.clone());
            self.filing.push(tokio::spawn(async move {
                let r = match NewTerm::from_json(&body) {
                    Ok(t) => g.propose(&t, user, &author, Some(&id)).await,
                    Err(e) => Err(e),
                };
                if let Err(e) = r {
                    tracing::warn!(error = %e, proposal = %id, "a glossary proposal wasn't filed");
                }
            }));
        }
        for v in event_json(&e) {
            self.tx
                .send(line(&v))
                .map_err(|_| SprigError::internal("the client went away"))?;
        }
        Ok(())
    }
}

/// Starts `t` on its own task: its lines, and the token that stops it.
/// The `start` line names `conv`.
#[must_use]
pub fn start(
    mut t: Turn,
    conv: Option<ConvId>,
) -> (mpsc::UnboundedReceiver<Bytes>, CancellationToken) {
    let (tx, rx) = mpsc::unbounded_channel();
    let cancel = CancellationToken::new();
    t.req.cancel = Some(cancel.clone());
    let _ = tx.send(line(&json!({
        "type": "start",
        "conversation": conv.map(|c| c.0.to_string()),
        "role": t.cfg.role.as_str(),
        "model": t.cfg.model,
        "share_data": t.cfg.share_data,
    })));
    tokio::spawn(async move {
        let mut sink = LineSink {
            tx: tx.clone(),
            glossary: t.glossary.clone(),
            user: t.req.user,
            author: t.author.clone(),
            filing: Vec::new(),
        };
        let r = t.agent.run(&t.cfg, t.req, &mut sink).await;
        for h in std::mem::take(&mut sink.filing) {
            let _ = h.await;
        }
        if let Err(e) = r {
            let _ = tx.send(line(&error_json(&e)));
        }
    });
    (rx, cancel)
}

/// Cancels the turn when the response body is dropped (the client went
/// away).
struct StopOnDrop(CancellationToken);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// The streaming response over `rx`.
#[must_use]
pub fn response(rx: mpsc::UnboundedReceiver<Bytes>, cancel: CancellationToken) -> Response<Body> {
    let guard = StopOnDrop(cancel);
    let stream = futures::stream::unfold((rx, guard), |(mut rx, g)| async move {
        let b = rx.recv().await?;
        Some((Ok::<_, std::convert::Infallible>(Frame::data(b)), (rx, g)))
    })
    .boxed();
    let mut r = Response::new(Body::new(StreamBody::new(stream)));
    let h = r.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-ndjson"),
    );
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

/// The role a request asks for, checked against the caller: `dba` and
/// `data` need a tenant login, `admin` a platform operator; `triage` is
/// never chatted with.
///
/// # Errors
/// 39790 for an unknown role or a login without a tenant; 1227 for the
/// administration assistant without operator rights.
pub fn role_for(asked: &str, tenant_login: bool, operator: bool) -> Result<Role> {
    match Role::parse(asked.trim()) {
        Some(Role::Admin) if operator => Ok(Role::Admin),
        Some(Role::Admin) => Err(SprigError::standard(
            1227,
            *b"42000",
            "Access denied; the administration assistant is for platform operators",
        )),
        Some(r @ (Role::Dba | Role::Data)) if tenant_login => Ok(r),
        Some(Role::Dba | Role::Data) => Err(super::invalid(
            "the DBA and data assistants need a tenant login",
        )),
        _ => Err(super::invalid("role is data, dba or admin")),
    }
}

/// The context chips' text, appended to the user's message. Kinds:
/// `query` (a statement), `table` (`db.table`), `note`, and `result` (rows
/// the user has on screen), which needs `ai.share_data`.
///
/// # Errors
/// 39995 for a `result` with sharing off; 39790 for an unknown kind or a
/// chip that is too long.
pub fn context_text(ctx: Option<&Value>, share_data: bool) -> Result<String> {
    let Some(items) = ctx.filter(|v| !v.is_null()) else {
        return Ok(String::new());
    };
    let items = items
        .as_array()
        .ok_or_else(|| super::invalid("context is an array of {kind, ref}"))?;
    if items.len() > 10 {
        return Err(super::invalid("context takes at most 10 items"));
    }
    let mut out = String::new();
    for it in items {
        let kind = it.get("kind").and_then(Value::as_str).unwrap_or("");
        let r = it.get("ref").and_then(Value::as_str).unwrap_or("");
        if r.len() > super::MAX_MESSAGE {
            return Err(super::invalid(format!(
                "a context item is longer than {} bytes",
                super::MAX_MESSAGE
            )));
        }
        let label = match kind {
            "query" => "The query in the user's editor",
            "table" => "The table the user is looking at",
            "note" => "A note from the user",
            "result" => {
                if !share_data {
                    return Err(sprig_ai::error::egress_denied("a result in the context"));
                }
                "Rows the user has on screen"
            }
            _ => {
                return Err(super::invalid(
                    "context kinds are query, table, note and result",
                ));
            }
        };
        let _ = write!(out, "\n\n[{label}]\n{r}");
    }
    Ok(out)
}
