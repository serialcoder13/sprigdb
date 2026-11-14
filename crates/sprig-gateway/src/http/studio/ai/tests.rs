//! The chat stream with the scripted provider, an in-memory store and a
//! fake tool host: no request leaves the process.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use sprig_ai::audit::{AuditStore, ConvId, MemoryAudit, MsgKind};
use sprig_ai::budget::Budget;
use sprig_ai::glossary::{GlossaryStore, MemoryGlossary, Status};
use sprig_ai::provider::scripted::{Reply, ScriptedProvider};
use sprig_ai::provider::{Effort, Provider, Usage};
use sprig_ai::tools::{Column, ResultSet, ToolBody, ToolOutput, ToolSpec};
use sprig_ai::{Agent, Role, ToolHost, ToolRegistry, TurnConfig, TurnRequest};
use sprig_common::ids::{TenantId, UserId};
use sprig_common::{Result, SprigError};

use super::chat::{self, Turn};

#[derive(Default)]
struct Host {
    calls: Mutex<Vec<String>>,
}

#[async_trait]
impl ToolHost for Host {
    async fn call(&self, spec: &ToolSpec, input: &Value) -> Result<ToolOutput> {
        self.calls.lock().unwrap().push(spec.name.to_owned());
        Ok(match spec.name {
            "describe" => {
                ToolOutput::json(json!({"table": input["table"], "columns": [{"name": "email"}]}))
            }
            "run_query" => ToolOutput {
                body: ToolBody::Rows(ResultSet {
                    columns: vec![Column {
                        name: "email".into(),
                        ty: "VARCHAR".into(),
                    }],
                    rows: vec![vec![json!("carol@example.com")]],
                    row_count: 1,
                    truncated: false,
                }),
                sources: Vec::new(),
            },
            _ => return Err(SprigError::internal("not in this test")),
        })
    }
}

fn usage() -> Usage {
    Usage {
        input_tokens: 100,
        output_tokens: 20,
        cache_read_tokens: 0,
        cache_write_tokens: 0,
    }
}

fn cfg(role: Role, share_data: bool, user_budget: u64) -> TurnConfig {
    TurnConfig {
        role,
        enabled: true,
        model: "test-model".into(),
        effort: Some(Effort::Medium),
        max_output_tokens: 1000,
        max_tool_rounds: 12,
        timeout: Duration::from_secs(30),
        budget: Budget {
            tenant_tokens_day: 1_000_000,
            user_tokens_day: user_budget,
        },
        share_data,
        keep_payloads: true,
    }
}

struct Run {
    lines: Vec<Value>,
    provider: Arc<ScriptedProvider>,
    host: Arc<Host>,
    audit: Arc<MemoryAudit>,
    glossary: Arc<MemoryGlossary>,
}

impl Run {
    fn types(&self) -> Vec<String> {
        let mut t: Vec<String> = Vec::new();
        for l in &self.lines {
            let s = l["type"].as_str().unwrap().to_owned();
            if t.last() != Some(&s) {
                t.push(s);
            }
        }
        t
    }
}

async fn run(replies: Vec<Reply>, cfg: TurnConfig, audit: Arc<MemoryAudit>, message: &str) -> Run {
    let provider = Arc::new(ScriptedProvider::new(replies, usage()));
    let host = Arc::new(Host::default());
    let glossary = Arc::new(MemoryGlossary::default());
    let role = cfg.role;
    let conv = audit.create(UserId(9), role, message).await.unwrap();
    let turn = Turn {
        agent: Agent::new(
            Arc::clone(&provider) as Arc<dyn Provider>,
            ToolRegistry::builtin(),
            Arc::clone(&audit) as Arc<dyn AuditStore>,
        ),
        cfg,
        req: TurnRequest {
            tenant: Some(TenantId(5)),
            user: UserId(9),
            conv: Some(conv),
            system: sprig_ai::prompt::base(role),
            history: Vec::new(),
            message: message.into(),
            host: Arc::clone(&host) as Arc<dyn ToolHost>,
            scrubber: None,
            cancel: None,
        },
        glossary: Some(Arc::clone(&glossary) as Arc<dyn GlossaryStore>),
        author: "ann".into(),
    };
    let (mut rx, _cancel) = chat::start(turn, Some(conv));
    let mut lines = Vec::new();
    while let Some(b) = rx.recv().await {
        assert_eq!(b.last(), Some(&b'\n'), "one object per line");
        lines.push(serde_json::from_slice(&b[..b.len() - 1]).unwrap());
    }
    Run {
        lines,
        provider,
        host,
        audit,
        glossary,
    }
}

fn call(id: &str, name: &str, input: Value) -> (String, String, Value) {
    (id.into(), name.into(), input)
}

#[tokio::test]
async fn the_stream_has_one_line_per_event() {
    let r = run(
        vec![
            Reply::Tools(
                "Let me look.".into(),
                vec![call(
                    "t1",
                    "describe",
                    json!({"db": "shop", "table": "customers"}),
                )],
            ),
            Reply::Text("It has an email column.".into()),
        ],
        cfg(Role::Data, false, 100_000),
        Arc::new(MemoryAudit::default()),
        "What is in customers?",
    )
    .await;
    assert_eq!(
        r.types(),
        [
            "start",
            "text",
            "usage",
            "tool_start",
            "tool_end",
            "text",
            "usage",
            "end"
        ],
        "{:#?}",
        r.lines
    );
    assert_eq!(r.lines[0]["conversation"], "1");
    assert_eq!(r.lines[0]["role"], "data");
    assert_eq!(r.lines[0]["share_data"], false);
    let end = r.lines.last().unwrap();
    assert_eq!(end["stop"], "end_turn");
    let text: String = r
        .lines
        .iter()
        .filter(|l| l["type"] == "text")
        .map(|l| l["text"].as_str().unwrap())
        .collect();
    assert_eq!(text, "Let me look.It has an email column.");
    assert_eq!(*r.host.calls.lock().unwrap(), ["describe"]);
}

#[tokio::test]
async fn proposals_are_shown_never_run_and_glossary_ones_are_filed() {
    let r = run(
        vec![
            Reply::Tools(
                String::new(),
                vec![
                    call(
                        "t1",
                        "propose_sql",
                        json!({"db": "shop", "sql": "DELETE FROM orders WHERE test = 1", "why": "remove test rows"}),
                    ),
                    call(
                        "t2",
                        "propose_glossary",
                        json!({"term": "active customer", "definition": "ordered in the last 90 days"}),
                    ),
                ],
            ),
            Reply::Text("Review the statement.".into()),
        ],
        cfg(Role::Dba, false, 100_000),
        Arc::new(MemoryAudit::default()),
        "Clean up the test orders",
    )
    .await;
    assert!(
        r.host.calls.lock().unwrap().is_empty(),
        "nothing reached the host"
    );
    let props: Vec<&Value> = r.lines.iter().filter(|l| l["type"] == "proposal").collect();
    assert_eq!(props.len(), 2);
    assert_eq!(props[0]["proposal"]["kind"], "sql");
    assert_eq!(
        props[0]["proposal"]["body"]["sql"],
        "DELETE FROM orders WHERE test = 1"
    );
    assert_eq!(props[1]["filed"], "glossary");
    // The model was told it hasn't run.
    let reqs = r.provider.requests();
    assert!(
        serde_json::to_string(&reqs[1].messages)
            .unwrap()
            .contains("has not run")
    );
    // The glossary proposal waits for an administrator.
    let g = r.glossary.list(None).await.unwrap();
    assert_eq!(g.len(), 1);
    assert_eq!(g[0].status, Status::Proposed);
    assert_eq!(g[0].author, "ann");
    assert_eq!(
        g[0].proposal_id.as_deref(),
        props[1]["proposal"]["id"].as_str()
    );
    let stored = r.audit.messages(ConvId(1)).await.unwrap();
    assert_eq!(
        stored
            .iter()
            .filter(|m| m.kind == MsgKind::Proposal)
            .count(),
        2
    );
}

#[tokio::test]
async fn rows_stay_home_when_sharing_is_off() {
    let replies = || {
        vec![
            Reply::Tools(
                String::new(),
                vec![call(
                    "t1",
                    "run_query",
                    json!({"db": "shop", "sql": "SELECT email FROM customers", "limit": 10}),
                )],
            ),
            Reply::Text("One customer.".into()),
        ]
    };
    let off = run(
        replies(),
        cfg(Role::Data, false, 100_000),
        Arc::new(MemoryAudit::default()),
        "Who are my customers?",
    )
    .await;
    let end = off.lines.iter().find(|l| l["type"] == "tool_end").unwrap();
    assert_eq!(end["reduced"], true);
    let sent = serde_json::to_string(&off.provider.requests()[1].messages).unwrap();
    assert!(!sent.contains("carol@example.com"), "{sent}");
    assert!(sent.contains("rows_withheld"));
    // What is stored is what was sent: the shape only.
    let stored = serde_json::to_string(&off.audit.messages(ConvId(1)).await.unwrap()).unwrap();
    assert!(!stored.contains("carol@example.com"));

    let on = run(
        replies(),
        cfg(Role::Data, true, 100_000),
        Arc::new(MemoryAudit::default()),
        "Who are my customers?",
    )
    .await;
    let sent = serde_json::to_string(&on.provider.requests()[1].messages).unwrap();
    assert!(sent.contains("carol@example.com"));
    assert_eq!(on.lines[0]["share_data"], true);

    // A result in the context can't leave with sharing off.
    let ctx = json!([{"kind": "result", "ref": "carol@example.com"}]);
    let e = chat::context_text(Some(&ctx), false).unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39995);
    assert!(
        chat::context_text(Some(&ctx), true)
            .unwrap()
            .contains("carol")
    );
    let q = json!([{"kind": "query", "ref": "SELECT 1"}]);
    assert!(
        chat::context_text(Some(&q), false)
            .unwrap()
            .contains("SELECT 1")
    );
    assert!(chat::context_text(Some(&json!([{"kind": "x", "ref": ""}])), true).is_err());
}

#[tokio::test]
async fn budgets_stop_turns_before_the_call() {
    // Spent before the turn: nothing is sent.
    let audit = Arc::new(MemoryAudit::default());
    audit.set_usage(UserId(9), Role::Data, 900, 200);
    let r = run(
        vec![Reply::Text("never".into())],
        cfg(Role::Data, false, 1000),
        audit,
        "hi",
    )
    .await;
    assert_eq!(r.types(), ["start", "error"]);
    assert_eq!(r.lines[1]["error"]["code"], 39992);
    assert!(r.provider.requests().is_empty());

    // Spent during the turn: the second call isn't made.
    let r = run(
        vec![
            Reply::Tools(
                String::new(),
                vec![call("t1", "describe", json!({"db": "shop", "table": "t"}))],
            ),
            Reply::Text("never".into()),
        ],
        cfg(Role::Data, false, 110),
        Arc::new(MemoryAudit::default()),
        "hi",
    )
    .await;
    assert_eq!(r.provider.requests().len(), 1);
    assert_eq!(r.lines.last().unwrap()["error"]["code"], 39992);
}

#[tokio::test]
async fn refusals_and_tool_denials_surface_as_error_lines() {
    let r = run(
        vec![Reply::Refuse("I can't help with that.".into())],
        cfg(Role::Data, false, 100_000),
        Arc::new(MemoryAudit::default()),
        "hi",
    )
    .await;
    let err = r.lines.iter().find(|l| l["type"] == "error").unwrap();
    assert_eq!(err["error"]["code"], 39994);
    assert_eq!(r.lines.last().unwrap()["stop"], "refusal");

    // The data specialist calling a DBA tool three times: 39996.
    let bad = || call("x", "workload", json!({"hours": 1}));
    let r = run(
        vec![
            Reply::Tools(String::new(), vec![bad()]),
            Reply::Tools(String::new(), vec![bad()]),
            Reply::Tools(String::new(), vec![bad()]),
        ],
        cfg(Role::Data, false, 100_000),
        Arc::new(MemoryAudit::default()),
        "hi",
    )
    .await;
    assert_eq!(r.lines.last().unwrap()["error"]["code"], 39996);
    assert!(r.host.calls.lock().unwrap().is_empty());
}

#[test]
fn roles_are_checked_against_the_caller() {
    assert_eq!(chat::role_for("data", true, false).unwrap(), Role::Data);
    assert_eq!(chat::role_for(" dba ", true, false).unwrap(), Role::Dba);
    let e = chat::role_for("admin", true, false).unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 1227);
    assert_eq!(chat::role_for("admin", false, true).unwrap(), Role::Admin);
    assert!(
        chat::role_for("dba", false, false).is_err(),
        "no tenant login"
    );
    assert!(chat::role_for("triage", true, true).is_err());
    assert!(chat::role_for("root", true, true).is_err());
}

#[tokio::test]
async fn a_stored_conversation_continues_where_it_left_off() {
    let audit = Arc::new(MemoryAudit::default());
    let provider = Arc::new(ScriptedProvider::new(
        vec![
            Reply::Tools(
                "Looking.".into(),
                vec![call("t1", "describe", json!({"db": "shop", "table": "t"}))],
            ),
            Reply::Text("Done.".into()),
        ],
        usage(),
    ));
    let conv = audit.create(UserId(9), Role::Data, "q").await.unwrap();
    let agent = Agent::new(
        Arc::clone(&provider) as Arc<dyn Provider>,
        ToolRegistry::builtin(),
        Arc::clone(&audit) as Arc<dyn AuditStore>,
    );
    let mut ev = Vec::new();
    let r = agent
        .run(
            &cfg(Role::Data, false, 100_000),
            TurnRequest {
                tenant: Some(TenantId(5)),
                user: UserId(9),
                conv: Some(conv),
                system: sprig_ai::prompt::base(Role::Data),
                history: Vec::new(),
                message: "What is t?".into(),
                host: Arc::new(Host::default()),
                scrubber: None,
                cancel: None,
            },
            &mut ev,
        )
        .await
        .unwrap();
    let rebuilt = sprig_ai::audit::history(&audit.messages(conv).await.unwrap());
    assert_eq!(
        rebuilt, r.messages,
        "the provider sees the same history again"
    );
}

#[test]
fn every_event_maps_to_a_typed_line() {
    use sprig_ai::TurnEvent;
    use sprig_ai::provider::Stop;
    let lines = chat::event_json(&TurnEvent::End {
        stop: Stop::Other("max_tool_rounds".into()),
    });
    assert_eq!(lines[0]["detail"], "max_tool_rounds");
    let lines = chat::event_json(&TurnEvent::ToolEnd {
        id: "a".into(),
        name: "describe".into(),
        ok: false,
        reduced: false,
    });
    assert_eq!(lines[0]["type"], "tool_end");
    assert_eq!(super::pct("a b/ü"), "a%20b%2F%C3%BC");
}
