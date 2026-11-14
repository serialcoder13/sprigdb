//! The turn loop with the scripted provider and a fake tool host.

use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::json;
use sprig_common::SprigCode;

use super::*;
use crate::audit::MemoryAudit;
use crate::egress::NameKind;
use crate::provider::scripted::{Reply, ScriptedProvider};
use crate::tools::{Column, ResultSet, ToolBody, ToolOutput, ToolSpec};

#[derive(Default)]
struct Host {
    calls: Mutex<Vec<String>>,
}

#[async_trait]
impl ToolHost for Host {
    async fn call(&self, spec: &ToolSpec, input: &Value) -> Result<ToolOutput> {
        self.calls.lock().unwrap().push(spec.name.to_owned());
        Ok(match spec.name {
            "describe" => ToolOutput {
                body: ToolBody::Json(
                    json!({"table": input["table"], "columns": [{"name": "id", "type": "bigint"}]}),
                ),
                sources: vec![Source {
                    kind: "table".into(),
                    title: "orders".into(),
                    reference: "shop.orders".into(),
                }],
            },
            "run_query" => ToolOutput {
                body: ToolBody::Rows(ResultSet {
                    columns: vec![Column {
                        name: "email".into(),
                        ty: "varchar".into(),
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
        cache_read_tokens: 500,
        cache_write_tokens: 0,
    }
}

fn cfg(role: Role) -> TurnConfig {
    TurnConfig {
        role,
        enabled: true,
        model: "claude-opus-5-5".into(),
        effort: Some(Effort::Medium),
        max_output_tokens: 4000,
        max_tool_rounds: 12,
        timeout: Duration::from_secs(30),
        budget: Budget {
            tenant_tokens_day: 1_000_000,
            user_tokens_day: 100_000,
        },
        share_data: false,
        keep_payloads: true,
    }
}

fn req(role: Role, host: Arc<dyn ToolHost>, message: &str) -> TurnRequest {
    TurnRequest {
        tenant: Some(TenantId(5)),
        user: UserId(9),
        conv: Some(ConvId(1)),
        system: crate::prompt::base(role),
        history: Vec::new(),
        message: message.into(),
        host,
        scrubber: None,
        cancel: None,
    }
}

fn agent(p: &Arc<ScriptedProvider>, audit: &Arc<MemoryAudit>) -> Agent {
    Agent::new(
        Arc::clone(p) as Arc<dyn Provider>,
        ToolRegistry::builtin(),
        Arc::clone(audit) as Arc<dyn AuditStore>,
    )
}

fn call(id: &str, name: &str, input: Value) -> (String, String, Value) {
    (id.into(), name.into(), input)
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // one turn, checked end to end
async fn a_turn_reads_as_the_caller_proposes_and_answers() {
    let p = Arc::new(ScriptedProvider::new(
        [
            Reply::Tools(
                "Let me look.".into(),
                vec![
                    call("t1", "describe", json!({"db": "shop", "table": "orders"})),
                    call(
                        "t2",
                        "run_query",
                        json!({"db": "shop", "sql": "SELECT email FROM customers"}),
                    ),
                    call(
                        "t3",
                        "propose_sql",
                        json!({"db": "shop", "sql": "CREATE INDEX i ON orders (id)", "why": "faster"}),
                    ),
                ],
            ),
            Reply::Text("Add the index I proposed.".into()),
        ],
        usage(),
    ));
    let audit = Arc::new(MemoryAudit::default());
    let host = Arc::new(Host::default());
    let mut ev = Vec::new();
    let r = agent(&p, &audit)
        .run(
            &cfg(Role::Data),
            req(Role::Data, host.clone(), "Why are orders slow?"),
            &mut ev,
        )
        .await
        .unwrap();

    assert_eq!(r.stop, Stop::EndTurn);
    assert_eq!(r.rounds, 1);
    assert_eq!(r.text(), "Add the index I proposed.");
    assert_eq!(
        r.messages.len(),
        4,
        "user, assistant, tool results, assistant"
    );
    assert_eq!(r.sent.len(), 2);
    assert_eq!(r.usage.billable(), 240);
    // The proposal never reached the host; reads ran as the caller.
    assert_eq!(*host.calls.lock().unwrap(), vec!["describe", "run_query"]);
    assert_eq!(r.proposals.len(), 1);
    assert_eq!(r.proposals[0].kind, crate::tools::ProposalKind::Sql);
    // Events: text, three starts, the proposal, three ends, sources, end.
    let starts = ev
        .iter()
        .filter(|e| matches!(e, TurnEvent::ToolStart { .. }))
        .count();
    assert_eq!(starts, 3);
    assert!(ev.iter().any(|e| matches!(e, TurnEvent::Proposal(_))));
    assert!(ev.contains(&TurnEvent::ToolEnd {
        id: "t2".into(),
        name: "run_query".into(),
        ok: true,
        reduced: true
    }));
    assert!(matches!(
        ev.last(),
        Some(TurnEvent::End {
            stop: Stop::EndTurn
        })
    ));
    assert!(matches!(&ev[ev.len() - 2], TurnEvent::Sources(s) if s.len() == 1));

    // The second request carried the gated results: no row left.
    let reqs = p.requests();
    assert_eq!(reqs.len(), 2);
    let results = &reqs[1].messages[2];
    let body = serde_json::to_string(results).unwrap();
    assert!(!body.contains("carol@example.com"));
    assert!(body.contains("rows_withheld"));
    assert!(body.contains("has not run"));
    for b in &results.content {
        let Block::ToolResult { content, .. } = b else {
            panic!("{b:?}")
        };
        assert!(content.starts_with("<tool_result untrusted=\"true\">"));
    }
    // The data specialist is offered its tools, not the DBA's.
    let names: Vec<&str> = reqs[0].tools.iter().map(|t| t.name.as_str()).collect();
    assert!(names.contains(&"run_query") && !names.contains(&"workload"));
    // Append-only: the first request's messages are a prefix of the second's.
    assert_eq!(reqs[0].messages[..], reqs[1].messages[..1]);
    assert_eq!(reqs[0].system, reqs[1].system);

    // Audit: payload notes with hashes, usage, the proposal, every result.
    let stored = audit.messages(ConvId(1)).await.unwrap();
    let kinds: Vec<MsgKind> = stored.iter().map(|m| m.kind).collect();
    assert_eq!(
        kinds.iter().filter(|k| **k == MsgKind::SystemNote).count(),
        2
    );
    assert_eq!(
        kinds.iter().filter(|k| **k == MsgKind::ToolResult).count(),
        3
    );
    assert!(kinds.contains(&MsgKind::Proposal));
    let note = stored
        .iter()
        .find(|m| m.kind == MsgKind::SystemNote)
        .unwrap();
    assert!(note.sent_hash.is_some());
    assert!(note.body["payload"].is_string());
    let used = audit.usage_today(UserId(9)).await.unwrap();
    assert_eq!(used.user, 240);
}

#[tokio::test]
async fn turns_check_enabled_model_and_budget_first() {
    let p = Arc::new(ScriptedProvider::new([], usage()));
    let audit = Arc::new(MemoryAudit::default());
    let a = agent(&p, &audit);
    let host: Arc<dyn ToolHost> = Arc::new(Host::default());
    let mut c = cfg(Role::Dba);
    c.enabled = false;
    let e = a
        .run(&c, req(Role::Dba, host.clone(), "hi"), &mut Vec::new())
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::AiDisabled));
    let mut c = cfg(Role::Dba);
    c.model.clear();
    let e = a
        .run(&c, req(Role::Dba, host.clone(), "hi"), &mut Vec::new())
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::AiNotConfigured));
    audit.set_usage(UserId(9), Role::Dba, 90_000, 10_000);
    let e = a
        .run(&cfg(Role::Dba), req(Role::Dba, host, "hi"), &mut Vec::new())
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::AiBudgetExceeded));
    assert!(p.requests().is_empty(), "nothing was sent");
}

#[tokio::test]
async fn a_budget_spent_mid_turn_stops_before_the_next_call() {
    let p = Arc::new(ScriptedProvider::new(
        [
            Reply::Tools(String::new(), vec![call("t1", "catalog", json!({}))]),
            Reply::Text("never".into()),
        ],
        Usage {
            input_tokens: 800,
            output_tokens: 0,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
        },
    ));
    let audit = Arc::new(MemoryAudit::default());
    let mut c = cfg(Role::Dba);
    c.budget.user_tokens_day = 1000;
    audit.set_usage(UserId(9), Role::Dba, 300, 0);
    let e = agent(&p, &audit)
        .run(
            &c,
            req(Role::Dba, Arc::new(Host::default()), "hi"),
            &mut Vec::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::AiBudgetExceeded));
    assert_eq!(p.requests().len(), 1);
    // The one call's output was capped at what was left.
    assert_eq!(p.requests()[0].max_tokens, 700);
}

#[tokio::test]
async fn repeated_refused_calls_end_the_turn() {
    let bad = || {
        Reply::Tools(
            String::new(),
            vec![call(
                "x",
                "run_query",
                json!({"db": "s", "sql": "SELECT 1"}),
            )],
        )
    };
    let p = Arc::new(ScriptedProvider::new([bad(), bad(), bad(), bad()], usage()));
    let audit = Arc::new(MemoryAudit::default());
    let host = Arc::new(Host::default());
    let e = agent(&p, &audit)
        .run(
            &cfg(Role::Dba),
            req(Role::Dba, host.clone(), "hi"),
            &mut Vec::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::AiToolDenied));
    assert_eq!(p.requests().len(), 3);
    assert!(
        host.calls.lock().unwrap().is_empty(),
        "the DBA can't run queries"
    );
    // Each refusal went back to the model as an error result first.
    let second = &p.requests()[1].messages;
    let Block::ToolResult {
        is_error, content, ..
    } = &second[2].content[0]
    else {
        panic!()
    };
    assert!(*is_error && content.contains("may not call"));
}

#[tokio::test]
async fn the_round_limit_leaves_a_valid_history() {
    let again = || Reply::Tools(String::new(), vec![call("c", "catalog", json!({}))]);
    let p = Arc::new(ScriptedProvider::new(
        [again(), again(), Reply::Text("ok".into())],
        usage(),
    ));
    let audit = Arc::new(MemoryAudit::default());
    let mut c = cfg(Role::Data);
    c.max_tool_rounds = 1;
    let host: Arc<dyn ToolHost> = Arc::new(Host::default());
    let a = agent(&p, &audit);
    let r = a
        .run(&c, req(Role::Data, host.clone(), "hi"), &mut Vec::new())
        .await
        .unwrap();
    assert_eq!(r.stop, Stop::Other("max_tool_rounds".into()));
    let last = r.messages.last().unwrap();
    assert_eq!(last.role, MsgRole::User);
    assert!(matches!(
        &last.content[0],
        Block::ToolResult { is_error: true, .. }
    ));
    // The next turn appends the user's text to that message.
    let mut next = req(Role::Data, host, "go on");
    next.history = r.messages;
    let r2 = a.run(&c, next, &mut Vec::new()).await.unwrap();
    assert_eq!(r2.stop, Stop::EndTurn);
    let sent = &p.requests()[2].messages;
    assert_eq!(sent.len(), 5, "user, two rounds of call and result");
    assert_eq!(sent[4].content.len(), 2, "tool result and the new text");
}

#[tokio::test]
async fn refusals_and_provider_failures() {
    let p = Arc::new(ScriptedProvider::new(
        [
            Reply::Refuse("no".into()),
            Reply::Fail("HTTP 503".into(), true),
            Reply::MaxTokens("cut".into()),
        ],
        usage(),
    ));
    let audit = Arc::new(MemoryAudit::default());
    let a = agent(&p, &audit);
    let host: Arc<dyn ToolHost> = Arc::new(Host::default());
    let r = a
        .run(
            &cfg(Role::Data),
            req(Role::Data, host.clone(), "x"),
            &mut Vec::new(),
        )
        .await
        .unwrap();
    assert_eq!(r.stop, Stop::Refusal("no".into()));
    assert_eq!(r.refusal().unwrap().code(), Some(SprigCode::AiRefused));
    let e = a
        .run(
            &cfg(Role::Data),
            req(Role::Data, host.clone(), "x"),
            &mut Vec::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::AiProviderError));
    assert!(e.retryable);
    let r = a
        .run(
            &cfg(Role::Data),
            req(Role::Data, host, "x"),
            &mut Vec::new(),
        )
        .await
        .unwrap();
    assert_eq!(r.stop, Stop::MaxTokens);
    assert!(r.refusal().is_none());
}

#[tokio::test]
async fn the_admin_agent_sends_placeholders_only() {
    let p = Arc::new(ScriptedProvider::new(
        [Reply::Text("Check the disk.".into())],
        usage(),
    ));
    let audit = Arc::new(MemoryAudit::default());
    let mut s = Scrubber::new(&[3; 32]);
    s.add(NameKind::Table, "payroll");
    let mut r = req(
        Role::Admin,
        Arc::new(crate::tools::NoHost),
        "39993 on payroll from 10.1.2.3:3306 password 'x1'",
    );
    r.tenant = None;
    r.conv = None;
    r.scrubber = Some(Arc::new(s));
    let mut c = cfg(Role::Admin);
    c.share_data = true;
    agent(&p, &audit).run(&c, r, &mut Vec::new()).await.unwrap();
    let sent = serde_json::to_string(&p.requests()[0].messages).unwrap();
    assert!(!sent.contains("payroll") && !sent.contains("10.1.2.3") && !sent.contains("x1"));
    assert!(sent.contains("39993"));
    let names: Vec<String> = p.requests()[0]
        .tools
        .iter()
        .map(|t| t.name.clone())
        .collect();
    assert!(names.contains(&"search_docs".to_owned()) && !names.contains(&"run_query".to_owned()));
}

#[tokio::test]
async fn a_stopped_turn_sends_nothing() {
    let p = Arc::new(ScriptedProvider::new([Reply::Text("x".into())], usage()));
    let audit = Arc::new(MemoryAudit::default());
    let mut r = req(Role::Data, Arc::new(Host::default()), "hi");
    let cancel = CancellationToken::new();
    cancel.cancel();
    r.cancel = Some(cancel);
    let out = agent(&p, &audit)
        .run(&cfg(Role::Data), r, &mut Vec::new())
        .await
        .unwrap();
    assert_eq!(out.stop, Stop::Other("stopped".into()));
}
