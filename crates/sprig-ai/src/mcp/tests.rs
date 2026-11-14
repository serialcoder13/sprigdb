use std::sync::Mutex;

use sprig_common::ids::UserId;

use super::*;
use crate::audit::MemoryAudit;
use crate::tools::{Column, ResultSet, ToolBody, ToolOutput};

#[derive(Default)]
struct Env {
    share: bool,
    calls: Arc<Mutex<Vec<String>>>,
    filed: Mutex<Vec<String>>,
}

struct Host {
    calls: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl ToolHost for Host {
    async fn call(&self, spec: &ToolSpec, _input: &Value) -> Result<ToolOutput> {
        self.calls.lock().unwrap().push(spec.name.to_owned());
        match spec.name {
            "run_query" => Ok(ToolOutput {
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
            }),
            "describe" => Err(SprigError::standard(
                1142,
                *b"42000",
                "SELECT command denied to user 'ann'@'%' for table 'customers'",
            )),
            _ => Ok(ToolOutput::json(json!({"ok": true}))),
        }
    }
}

#[async_trait]
impl McpEnv for Env {
    fn share_data(&self, _db: Option<&str>) -> bool {
        self.share
    }
    fn host(&self, _share_data: bool) -> Arc<dyn ToolHost> {
        Arc::new(Host {
            calls: Arc::clone(&self.calls),
        })
    }
    async fn file_glossary(&self, p: &Proposal) -> Result<()> {
        self.filed.lock().unwrap().push(p.id.clone());
        Ok(())
    }
}

fn names(v: &[Arc<ToolSpec>]) -> Vec<&'static str> {
    v.iter().map(|s| s.name).collect()
}

#[test]
fn versions() {
    assert_eq!(supported_versions()[0], MODERN_VERSION);
    assert_eq!(negotiate("2025-06-18"), "2025-06-18");
    assert_eq!(negotiate("2024-11-05"), "2025-11-25");
    assert_eq!(negotiate(MODERN_VERSION), "2025-11-25");
    assert!(is_legacy(ASSUMED_VERSION) && !is_legacy(MODERN_VERSION));
}

#[test]
fn roles_are_the_tenant_ones() {
    assert_eq!(role(None), Ok(Role::Data));
    assert_eq!(role(Some("dba")), Ok(Role::Dba));
    assert!(role(Some("admin")).is_err());
    assert!(role(Some("triage")).is_err());
    assert!(role(Some("root")).is_err());
}

#[test]
fn the_allow_list_narrows_the_role() {
    let reg = ToolRegistry::builtin();
    let data = exposed(&reg, Role::Data, &AllowList::parse("read"));
    let n = names(&data);
    assert!(n.contains(&"run_query") && n.contains(&"describe") && n.contains(&"search_docs"));
    assert!(n.iter().all(|t| !t.starts_with("propose_")), "{n:?}");
    let dba = names(&exposed(&reg, Role::Dba, &AllowList::parse("read")));
    assert!(dba.contains(&"workload") && !dba.contains(&"run_query"));
    assert!(!dba.contains(&"metrics_trend"), "admin-only tools never");
    let both = names(&exposed(
        &reg,
        Role::Data,
        &AllowList::parse(" read , propose "),
    ));
    assert!(both.contains(&"propose_sql") && both.contains(&"propose_glossary"));
    let some = names(&exposed(
        &reg,
        Role::Data,
        &AllowList::parse("describe,propose_sql,workload,nonsense"),
    ));
    assert_eq!(some, ["describe", "propose_sql"]);
    assert!(exposed(&reg, Role::Admin, &AllowList::parse("read,propose")).is_empty());
    assert!(exposed(&reg, Role::Data, &AllowList::parse("")).is_empty());
}

#[test]
fn tool_definitions() {
    let reg = ToolRegistry::builtin();
    let d = tool_def(reg.get("run_query").unwrap());
    assert_eq!(d["name"], "run_query");
    assert_eq!(d["title"], "Run query");
    assert_eq!(d["inputSchema"]["type"], "object");
    assert_eq!(d["annotations"]["readOnlyHint"], true);
    let p = tool_def(reg.get("propose_sql").unwrap());
    assert!(p["description"].as_str().unwrap().contains("runs nothing"));
    assert_eq!(p["annotations"]["readOnlyHint"], true);
    let g = tool_def(reg.get("propose_glossary").unwrap());
    assert_eq!(g["annotations"]["readOnlyHint"], false);
    assert!(instructions(Role::Dba).contains("untrusted"));
}

fn data_tools(allow: &str) -> Vec<Arc<ToolSpec>> {
    exposed(
        &ToolRegistry::builtin(),
        Role::Data,
        &AllowList::parse(allow),
    )
}

async fn audit() -> Audit {
    let store = Arc::new(MemoryAudit::default());
    let conv = store
        .create(UserId(7), Role::Data, "MCP: test")
        .await
        .unwrap();
    Audit { store, conv }
}

#[tokio::test]
async fn reads_run_as_the_caller_and_pass_the_gate() {
    let tools = data_tools("read");
    let a = audit().await;
    let args = json!({"db": "shop", "sql": "SELECT email FROM customers"});
    // Sharing off: the shape only.
    let env = Env::default();
    let c = call(&env, Role::Data, &tools, "run_query", Some(&args), Some(&a))
        .await
        .unwrap();
    assert_eq!(c.outcome, "ok");
    assert!(c.reduced);
    let text = c.result["content"][0]["text"].as_str().unwrap();
    assert!(
        text.starts_with("<tool_result untrusted=\"true\">"),
        "{text}"
    );
    assert!(!text.contains("carol@example.com"), "{text}");
    assert!(text.contains("rows_withheld") && text.contains("\"row_count\":1"));
    assert_eq!(c.result["isError"], false);
    assert_eq!(*env.calls.lock().unwrap(), ["run_query"]);
    // Sharing on: the rows.
    let env = Env {
        share: true,
        ..Env::default()
    };
    let c = call(&env, Role::Data, &tools, "run_query", Some(&args), None)
        .await
        .unwrap();
    assert!(!c.reduced);
    assert!(
        c.result["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("carol@example.com")
    );
    // Audited: the call and its gated result.
    let msgs = a.store.messages(a.conv).await.unwrap();
    let kinds: Vec<MsgKind> = msgs.iter().map(|m| m.kind).collect();
    assert_eq!(kinds, [MsgKind::ToolCall, MsgKind::ToolResult]);
    assert_eq!(msgs[1].body["result"]["row_count"], 1);
    assert!(msgs[1].body["result"]["rows"].is_null());
}

#[tokio::test]
async fn refusals_and_failures() {
    let env = Env::default();
    let tools = data_tools("read");
    // Not offered: a protocol error, 39996, and the host never sees it.
    let e = call(
        &env,
        Role::Data,
        &tools,
        "propose_sql",
        Some(&json!({})),
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39996);
    let e = call(&env, Role::Data, &tools, "workload", None, None)
        .await
        .unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39996);
    // Invalid input and writes: an error result the model can act on.
    let c = call(
        &env,
        Role::Data,
        &tools,
        "run_query",
        Some(&json!({"db": "shop", "sql": "DELETE FROM customers"})),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        (c.outcome, c.result["isError"].as_bool()),
        ("denied", Some(true))
    );
    let c = call(
        &env,
        Role::Data,
        &tools,
        "describe",
        Some(&json!({"db": "shop"})),
        None,
    )
    .await
    .unwrap();
    assert_eq!(c.outcome, "denied");
    assert!(env.calls.lock().unwrap().is_empty());
    // The caller's own refusal from the database.
    let c = call(
        &env,
        Role::Data,
        &tools,
        "describe",
        Some(&json!({"db": "shop", "table": "customers"})),
        None,
    )
    .await
    .unwrap();
    assert_eq!(c.outcome, "error");
    let text = c.result["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("error 1142") && text.contains("untrusted"),
        "{text}"
    );
}

#[tokio::test]
async fn proposals_come_back_and_never_run() {
    let env = Env::default();
    let tools = data_tools("read,propose");
    let a = audit().await;
    let c = call(
        &env,
        Role::Data,
        &tools,
        "propose_sql",
        Some(&json!({"db": "shop", "sql": "DELETE FROM customers WHERE id = 1", "why": "a duplicate"})),
        Some(&a),
    )
    .await
    .unwrap();
    assert_eq!(c.outcome, "proposed");
    let p = c.proposal.unwrap();
    assert_eq!(p.kind, ProposalKind::Sql);
    let text = c.result["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("NOT run") && text.contains("DELETE FROM customers"),
        "{text}"
    );
    assert!(
        env.calls.lock().unwrap().is_empty(),
        "nothing reached the host"
    );
    let kinds: Vec<MsgKind> = a
        .store
        .messages(a.conv)
        .await
        .unwrap()
        .iter()
        .map(|m| m.kind)
        .collect();
    assert_eq!(
        kinds,
        [MsgKind::ToolCall, MsgKind::Proposal, MsgKind::ToolResult]
    );
    // A glossary proposal is filed for an administrator, as in chat.
    let c = call(
        &env,
        Role::Data,
        &tools,
        "propose_glossary",
        Some(&json!({"term": "churn", "definition": "customers who left"})),
        None,
    )
    .await
    .unwrap();
    assert_eq!(*env.filed.lock().unwrap(), [c.proposal.unwrap().id]);
}
