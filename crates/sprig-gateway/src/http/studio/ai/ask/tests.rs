//! `ASK` with the scripted provider, an in-memory store and a recording
//! tool host: no request leaves the process, and nothing can run except
//! through the host.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use sprig_ai::audit::{AuditStore, MemoryAudit, MsgKind};
use sprig_ai::budget::Budget;
use sprig_ai::provider::scripted::{Reply, ScriptedProvider};
use sprig_ai::provider::{Block, Effort, Provider, Usage};
use sprig_ai::settings::TurnSettings;
use sprig_ai::tools::{Source, ToolOutput, ToolSpec};
use sprig_ai::{Role, ToolHost};
use sprig_common::ids::{TenantId, UserId};
use sprig_common::{Result, SprigError};

use super::{AskEnv, AskRequest, AskStores, ask_with};

/// Records every tool call; it is the only way a turn could touch data.
#[derive(Default)]
struct Host {
    calls: Mutex<Vec<(String, Value)>>,
}

#[async_trait]
impl ToolHost for Host {
    async fn call(&self, spec: &ToolSpec, input: &Value) -> Result<ToolOutput> {
        self.calls
            .lock()
            .unwrap()
            .push((spec.name.to_owned(), input.clone()));
        match spec.name {
            "describe" => Ok(ToolOutput {
                body: sprig_ai::tools::ToolBody::Json(
                    json!({"table": input["table"], "columns": [{"name": "id"}, {"name": "total"}]}),
                ),
                sources: vec![Source {
                    kind: "table".into(),
                    title: "shop.orders".into(),
                    reference: "shop.orders".into(),
                }],
            }),
            _ => Err(SprigError::internal("not in this test")),
        }
    }
}

struct Env {
    enabled: bool,
    model: String,
    user_budget: u64,
    tenant_login: bool,
    provider: Arc<ScriptedProvider>,
    audit: Arc<MemoryAudit>,
    host: Arc<Host>,
    provider_asked: AtomicUsize,
}

impl Env {
    fn new(replies: Vec<Reply>) -> Self {
        Self {
            enabled: true,
            model: "scripted/test-model".into(),
            user_budget: 100_000,
            tenant_login: true,
            provider: Arc::new(ScriptedProvider::new(
                replies,
                Usage {
                    input_tokens: 100,
                    output_tokens: 20,
                    cache_read_tokens: 0,
                    cache_write_tokens: 0,
                },
            )),
            audit: Arc::new(MemoryAudit::default()),
            host: Arc::new(Host::default()),
            provider_asked: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> Vec<String> {
        self.host
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|(n, _)| n.clone())
            .collect()
    }
}

#[async_trait]
impl AskEnv for Env {
    fn settings(&self, _role: Role, _db: Option<&str>) -> TurnSettings {
        TurnSettings {
            enabled: self.enabled,
            share_data: false,
            model: self.model.clone(),
            effort: Effort::Medium,
            budget: Budget {
                tenant_tokens_day: 1_000_000,
                user_tokens_day: self.user_budget,
            },
            max_tool_rounds: 12,
            timeout: Duration::from_secs(30),
            max_output_tokens: 1000,
            audit_payload_days: 30,
        }
    }
    fn tenant_login(&self) -> bool {
        self.tenant_login
    }
    fn tenant(&self) -> Option<TenantId> {
        Some(TenantId(5))
    }
    fn user(&self) -> UserId {
        UserId(9)
    }
    async fn provider(&self, role: Role, model: &str) -> Result<(Arc<dyn Provider>, String)> {
        self.provider_asked.fetch_add(1, Ordering::SeqCst);
        if model.is_empty() {
            return Err(sprig_ai::error::not_configured(
                role,
                format!("ai.model.{role} is empty"),
            ));
        }
        Ok((
            Arc::clone(&self.provider) as Arc<dyn Provider>,
            "test-model".into(),
        ))
    }
    async fn stores(&self) -> Result<AskStores> {
        Ok(AskStores {
            audit: Arc::clone(&self.audit) as Arc<dyn AuditStore>,
            glossary: None,
            feedback: None,
        })
    }
    fn host(&self, _share_data: bool) -> Arc<dyn ToolHost> {
        Arc::clone(&self.host) as Arc<dyn ToolHost>
    }
}

fn call(id: &str, name: &str, input: Value) -> (String, String, Value) {
    (id.into(), name.into(), input)
}

fn propose(id: &str, sql: &str, why: &str) -> Reply {
    Reply::Tools(
        String::new(),
        vec![call(
            id,
            "propose_sql",
            json!({"db": "shop", "sql": sql, "why": why}),
        )],
    )
}

fn req(q: &str) -> AskRequest {
    AskRequest {
        question: q.into(),
        role: None,
        database: None,
    }
}

fn code(e: &SprigError) -> u16 {
    e.code().map(sprig_common::SprigCode::number).unwrap_or(0)
}

#[tokio::test]
async fn a_proposed_write_comes_back_as_a_row_and_never_runs() {
    let env = Env::new(vec![
        Reply::Tools(
            String::new(),
            vec![call(
                "t1",
                "describe",
                json!({"db": "shop", "table": "orders"}),
            )],
        ),
        Reply::Tools(
            String::new(),
            vec![call(
                "p1",
                "propose_sql",
                json!({
                    "db": "shop",
                    "sql": "DELETE FROM orders WHERE total = 0;",
                    "why": "Removes the empty orders.",
                    "estimate": "about 40 rows",
                }),
            )],
        ),
        Reply::Text("I proposed a DELETE; it hasn't run.".into()),
    ]);
    let a = ask_with(&env, &req("remove empty orders"), Some("shop"))
        .await
        .unwrap();
    assert_eq!(a.rows.len(), 1);
    let r = &a.rows[0];
    assert_eq!(
        r.query.as_deref(),
        Some("DELETE FROM orders WHERE total = 0")
    );
    assert_eq!(r.database.as_deref(), Some("shop"));
    assert_eq!(r.explanation, "Removes the empty orders.");
    let notes = r.notes.as_deref().unwrap();
    assert!(notes.contains("review it before you run it"), "{notes}");
    assert!(notes.contains("Estimate: about 40 rows"), "{notes}");
    assert_eq!(r.sources.as_deref(), Some("table: shop.orders"));
    // Nothing ran: the host only described the table.
    assert_eq!(env.calls(), ["describe"]);
    // The turn saw the ASK rules and the session's database.
    let sent = env.provider.requests();
    assert_eq!(sent.len(), 3);
    assert!(
        sent[0]
            .system
            .iter()
            .any(|b| matches!(b, Block::Text { text } if text == sprig_ai::prompt::ASK))
    );
    assert!(
        sent[0].messages[0]
            .text()
            .ends_with("[The user's current database]\nshop")
    );
    // The conversation is stored, with the proposal.
    let conv = a.conversation.unwrap();
    let msgs = env.audit.messages(conv).await.unwrap();
    assert!(msgs.iter().any(|m| m.kind == MsgKind::Proposal));
}

#[tokio::test]
async fn reads_say_so_and_two_statements_become_two_rows() {
    let env = Env::new(vec![
        propose(
            "p1",
            "SELECT id FROM orders WHERE total > 100; SELECT COUNT(*) FROM orders",
            "Big orders, then how many orders there are.",
        ),
        Reply::Text("Two queries proposed.".into()),
    ]);
    let a = ask_with(&env, &req("big orders"), None).await.unwrap();
    let q: Vec<_> = a.rows.iter().map(|r| r.query.clone().unwrap()).collect();
    assert_eq!(
        q,
        [
            "SELECT id FROM orders WHERE total > 100",
            "SELECT COUNT(*) FROM orders"
        ]
    );
    let n0 = a.rows[0].notes.as_deref().unwrap();
    assert!(n0.starts_with("Reads only."), "{n0}");
    assert!(n0.contains("Statement 1 of 2"), "{n0}");
    assert!(
        a.rows[1]
            .notes
            .as_deref()
            .unwrap()
            .contains("Statement 2 of 2")
    );
    assert!(env.calls().is_empty());
    // No database context without one.
    assert!(
        !env.provider.requests()[0].messages[0]
            .text()
            .contains("current database")
    );
}

#[tokio::test]
async fn fenced_sql_counts_when_nothing_was_proposed() {
    let env = Env::new(vec![Reply::Text(
        "Here it is:\n\n```sql\nSELECT 1;\nSELECT 2\n```\n\nBoth are cheap.\n\n```text\nnot sql\n```"
            .into(),
    )]);
    let a = ask_with(&env, &req("two numbers"), None).await.unwrap();
    let q: Vec<_> = a.rows.iter().map(|r| r.query.clone().unwrap()).collect();
    assert_eq!(q, ["SELECT 1", "SELECT 2"]);
    assert_eq!(a.rows[0].explanation, "Here it is:\n\nBoth are cheap.");
    assert_eq!(a.rows[0].database, None);
}

#[tokio::test]
async fn an_answer_in_words_is_one_row_without_a_query() {
    let env = Env::new(vec![Reply::Text(
        "That needs a table of exchange rates, which this tenant doesn't have.".into(),
    )]);
    let a = ask_with(&env, &req("revenue in euros"), None)
        .await
        .unwrap();
    assert_eq!(a.rows.len(), 1);
    assert_eq!(a.rows[0].query, None);
    assert!(a.rows[0].explanation.starts_with("That needs a table"));
    assert!(a.rows[0].notes.as_deref().unwrap().starts_with("No query"));
}

#[tokio::test]
async fn a_cut_off_answer_is_flagged() {
    let env = Env::new(vec![Reply::MaxTokens(
        "```sql\nSELECT id, total FROM orders WHERE".into(),
    )]);
    let a = ask_with(&env, &req("orders"), None).await.unwrap();
    assert_eq!(
        a.rows[0].query.as_deref(),
        Some("SELECT id, total FROM orders WHERE")
    );
    assert!(a.rows[0].notes.as_deref().unwrap().contains("cut off"));
}

#[tokio::test]
async fn the_checks_come_in_the_chat_routes_order() {
    // Off: 39991 before the provider is even looked up.
    let mut env = Env::new(vec![Reply::Text("never".into())]);
    env.enabled = false;
    env.model = String::new();
    let e = ask_with(&env, &req("hi"), None).await.unwrap_err();
    assert_eq!(code(&e), 39991);
    assert_eq!(env.provider_asked.load(Ordering::SeqCst), 0);

    // No model: 39990.
    let mut env = Env::new(vec![Reply::Text("never".into())]);
    env.model = String::new();
    let e = ask_with(&env, &req("hi"), None).await.unwrap_err();
    assert_eq!(code(&e), 39990);

    // Budget spent: 39992, nothing sent.
    let mut env = Env::new(vec![Reply::Text("never".into())]);
    env.user_budget = 1000;
    env.audit.set_usage(UserId(9), Role::Data, 900, 200);
    let e = ask_with(&env, &req("hi"), None).await.unwrap_err();
    assert_eq!(code(&e), 39992);
    assert!(env.provider.requests().is_empty());

    // A login without a tenant, or a role ASK doesn't offer: 39792.
    let mut env = Env::new(vec![Reply::Text("never".into())]);
    env.tenant_login = false;
    assert_eq!(
        code(&ask_with(&env, &req("hi"), None).await.unwrap_err()),
        39792
    );
    let env = Env::new(vec![Reply::Text("never".into())]);
    let admin = AskRequest {
        role: Some("admin".into()),
        ..req("hi")
    };
    assert_eq!(
        code(&ask_with(&env, &admin, None).await.unwrap_err()),
        39792
    );
    assert_eq!(
        code(&ask_with(&env, &req("   "), None).await.unwrap_err()),
        39792
    );
    assert!(env.provider.requests().is_empty());
}

#[tokio::test]
async fn refusals_and_provider_failures_are_errors() {
    let env = Env::new(vec![Reply::Refuse("I can't help with that.".into())]);
    let e = ask_with(&env, &req("hi"), None).await.unwrap_err();
    assert_eq!(code(&e), 39994);
    assert!(e.message.contains("I can't help with that."));

    let env = Env::new(vec![Reply::Fail("overloaded".into(), true)]);
    let e = ask_with(&env, &req("hi"), None).await.unwrap_err();
    assert_eq!(code(&e), 39993);
    assert!(e.retryable);
}

#[tokio::test]
async fn the_dba_role_is_used_when_asked() {
    let env = Env::new(vec![
        propose(
            "p1",
            "CREATE INDEX by_total ON orders (total)",
            "Speeds up the filter.",
        ),
        Reply::Text("Proposed an index.".into()),
    ]);
    let dba = AskRequest {
        role: Some("dba".into()),
        database: Some("shop".into()),
        ..req("why is my query slow")
    };
    let a = ask_with(&env, &dba, Some("other")).await.unwrap();
    assert_eq!(
        a.rows[0].query.as_deref(),
        Some("CREATE INDEX by_total ON orders (total)")
    );
    let sent = env.provider.requests();
    assert!(
        sent[0]
            .system
            .iter()
            .any(|b| matches!(b, Block::Text { text } if text.contains("DBA assistant")))
    );
    // IN DATABASE wins over the session's database.
    assert!(sent[0].messages[0].text().ends_with("database]\nshop"));
    let conv = env
        .audit
        .get(UserId(9), a.conversation.unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(conv.role, Role::Dba);
}
