//! The MCP endpoint over a real HTTP connection: an in-process server on
//! 127.0.0.1 running [`serve`] with a test backend (two users, a fake
//! tool host that refuses SELECT to one of them, in-memory stores), and a
//! small MCP client written here. No request leaves the process.

use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering};

use http_body_util::BodyExt;
use hyper_util::rt::TokioIo;
use sprig_ai::audit::{MemoryAudit, MsgKind};
use sprig_ai::tools::{Column, ResultSet, ToolBody, ToolOutput, ToolSpec};

use super::*;

// ---- the backend ----

#[derive(Default)]
struct Shared {
    settings: Mutex<Option<McpSettings>>,
    share: AtomicBool,
    /// (login, tool) for every call that reached a host.
    calls: Mutex<Vec<(String, String)>>,
    audit: Arc<MemoryAudit>,
    filed: Mutex<Vec<String>>,
}

impl Shared {
    fn set(&self, f: impl FnOnce(&mut McpSettings)) {
        let mut g = lock(&self.settings);
        let s = g.get_or_insert_with(on);
        f(s);
    }
}

fn on() -> McpSettings {
    McpSettings {
        ai_enabled: true,
        enabled: true,
        tools: "read".into(),
        calls_per_minute: 120,
    }
}

struct TestBackend {
    shared: Arc<Shared>,
}

struct Env {
    login: String,
    shared: Arc<Shared>,
}

struct Host {
    login: String,
    shared: Arc<Shared>,
}

#[async_trait]
impl ToolHost for Host {
    async fn call(&self, spec: &ToolSpec, input: &Value) -> sprig_common::Result<ToolOutput> {
        lock(&self.shared.calls).push((self.login.clone(), spec.name.to_owned()));
        // bob has no SELECT on shop: what the engine answers him.
        if self.login == "bob" && input["db"] == "shop" {
            return Err(SprigError::standard(
                1142,
                *b"42000",
                "SELECT command denied to user 'bob'@'%' for table 'customers'",
            ));
        }
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
            _ => Ok(ToolOutput::json(json!({"tables": ["customers"]}))),
        }
    }
}

#[async_trait]
impl McpEnv for Env {
    fn share_data(&self, _db: Option<&str>) -> bool {
        self.shared.share.load(Ordering::SeqCst)
    }
    fn host(&self, _share_data: bool) -> Arc<dyn ToolHost> {
        Arc::new(Host {
            login: self.login.clone(),
            shared: Arc::clone(&self.shared),
        })
    }
    async fn file_glossary(&self, p: &Proposal) -> sprig_common::Result<()> {
        lock(&self.shared.filed).push(p.id.clone());
        Ok(())
    }
}

#[async_trait]
impl McpCaller for Env {
    async fn audit_store(&self) -> Result<Arc<dyn AuditStore>, SprigError> {
        Ok(Arc::clone(&self.shared.audit) as Arc<dyn AuditStore>)
    }
}

#[async_trait]
impl Backend for TestBackend {
    async fn sign_in(&self, h: &HeaderMap) -> Result<Signed, SprigError> {
        let (login, user) = match get(h, "authorization") {
            Some("Bearer tok-ann") => ("ann", 7),
            Some("Bearer tok-bob") => ("bob", 8),
            Some("Bearer tok-operator") => {
                return Err(invalid("the MCP server needs a tenant login"));
            }
            _ => {
                return Err(SprigError::standard(
                    1045,
                    *b"28000",
                    "Access denied: bad token",
                ));
            }
        };
        let settings = lock(&self.shared.settings).clone().unwrap_or_else(on);
        Ok(Signed {
            who: Who {
                tenant: TenantId(5),
                user: UserId(user),
                login: login.into(),
            },
            settings,
            env: Arc::new(Env {
                login: login.into(),
                shared: Arc::clone(&self.shared),
            }),
        })
    }

    fn origin_ok(&self, origin: &str, host: Option<&str>) -> bool {
        origin_allowed(origin, host, false, &["https://db.example.com".to_owned()])
    }
}

/// An in-process server on 127.0.0.1; its address and shared state.
async fn server() -> (String, Arc<Shared>) {
    let shared = Arc::new(Shared::default());
    let backend = Arc::new(TestBackend {
        shared: Arc::clone(&shared),
    });
    let st = Arc::new(McpState::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            let (backend, st) = (Arc::clone(&backend), Arc::clone(&st));
            tokio::spawn(async move {
                let svc = hyper::service::service_fn(move |req: Request<Incoming>| {
                    let (backend, st) = (Arc::clone(&backend), Arc::clone(&st));
                    async move {
                        let method = req.method().clone();
                        let query = req.uri().query().unwrap_or("").to_owned();
                        let headers = req.headers().clone();
                        let body = req.into_body().collect().await.unwrap().to_bytes();
                        let r = McpRequest {
                            method: &method,
                            query: &query,
                            headers: &headers,
                            body,
                        };
                        Ok::<_, Infallible>(serve(&*backend, &st, r).await)
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(tcp), svc)
                    .await;
            });
        }
    });
    (addr, shared)
}

// ---- the client ----

struct Reply {
    status: u16,
    headers: HeaderMap,
    body: Value,
    raw: String,
}

struct Client {
    addr: String,
    token: Option<String>,
    role: Option<&'static str>,
    session: Option<String>,
    /// The negotiated legacy version, sent as MCP-Protocol-Version.
    version: Option<String>,
    next: u64,
}

impl Client {
    fn new(addr: &str, token: &str) -> Self {
        Self {
            addr: addr.to_owned(),
            token: Some(format!("Bearer {token}")),
            role: None,
            session: None,
            version: None,
            next: 1,
        }
    }

    async fn send(&self, method: &str, extra: &[(&str, &str)], body: Option<&Value>) -> Reply {
        let tcp = tokio::net::TcpStream::connect(&self.addr).await.unwrap();
        let uri = match self.role {
            Some(r) => format!("{PATH}?role={r}"),
            None => PATH.to_owned(),
        };
        let mut req = Request::builder()
            .method(method)
            .uri(uri)
            .header("host", &self.addr)
            .header("content-type", "application/json");
        let mut has_accept = false;
        for (k, v) in extra {
            has_accept |= k.eq_ignore_ascii_case("accept");
            req = req.header(*k, *v);
        }
        if !has_accept {
            req = req.header("accept", "application/json, text/event-stream");
        }
        if let Some(t) = &self.token {
            req = req.header("authorization", t);
        }
        if let Some(s) = &self.session {
            req = req.header(SESSION_HEADER, s);
        }
        if let Some(v) = &self.version {
            req = req.header(VERSION_HEADER, v);
        }
        let b = body.map_or_else(Bytes::new, |b| Bytes::from(b.to_string()));
        let req = req.body(Full::new(b)).unwrap();
        let (mut tx, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
            .await
            .unwrap();
        tokio::spawn(conn);
        let resp = tx.send_request(req).await.unwrap();
        let status = resp.status().as_u16();
        let headers = resp.headers().clone();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let raw = String::from_utf8_lossy(&bytes).into_owned();
        Reply {
            status,
            headers,
            body: serde_json::from_slice(&bytes).unwrap_or(Value::Null),
            raw,
        }
    }

    /// A legacy-era request.
    async fn rpc(&mut self, method: &str, params: Value) -> Reply {
        let id = self.next;
        self.next += 1;
        let body = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        let r = self.send("POST", &[], Some(&body)).await;
        if r.status == 200 {
            assert_eq!(r.body["id"], id, "{}", r.raw);
        }
        r
    }

    async fn notify(&self, method: &str) -> Reply {
        let body = json!({"jsonrpc": "2.0", "method": method});
        self.send("POST", &[], Some(&body)).await
    }

    /// `initialize`, then `notifications/initialized`: the legacy handshake.
    async fn connect(&mut self) -> Reply {
        let r = self
            .rpc(
                "initialize",
                json!({
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": {"name": "test-client", "version": "1.0"},
                }),
            )
            .await;
        if r.status == 200 {
            self.session = Some(get(&r.headers, SESSION_HEADER).unwrap().to_owned());
            self.version = Some(r.body["result"]["protocolVersion"].as_str().unwrap().into());
            assert_eq!(self.notify("notifications/initialized").await.status, 202);
        }
        r
    }

    async fn call(&mut self, tool: &str, args: Value) -> Reply {
        self.rpc("tools/call", json!({"name": tool, "arguments": args}))
            .await
    }

    /// A stateless-era request with its headers.
    async fn modern(&mut self, method: &str, mut params: Value, headers: &[(&str, &str)]) -> Reply {
        let id = self.next;
        self.next += 1;
        if params.get("_meta").is_none() {
            params["_meta"] = json!({
                "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                "io.modelcontextprotocol/clientInfo": {"name": "modern-client", "version": "2"},
                "io.modelcontextprotocol/clientCapabilities": {},
            });
        }
        let name = params["name"].as_str().unwrap_or("").to_owned();
        let mut h: Vec<(&str, &str)> = vec![
            ("mcp-protocol-version", "2026-07-28"),
            ("mcp-method", method),
        ];
        if method == "tools/call" {
            h.push(("mcp-name", &name));
        }
        for (k, v) in headers {
            h.retain(|(x, _)| !x.eq_ignore_ascii_case(k));
            if !v.is_empty() {
                h.push((*k, *v));
            }
        }
        let body = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        self.send("POST", &h, Some(&body)).await
    }
}

fn text(r: &Reply) -> &str {
    r.body["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("no text: {}", r.raw))
}

fn calls(s: &Shared) -> Vec<(String, String)> {
    lock(&s.calls).clone()
}

// ---- the tests ----

#[tokio::test]
async fn a_session_lists_and_calls_tools_as_the_user() {
    let (addr, shared) = server().await;
    let mut c = Client::new(&addr, "tok-ann");
    let r = c.connect().await;
    assert_eq!(r.status, 200, "{}", r.raw);
    let res = &r.body["result"];
    assert_eq!(res["protocolVersion"], "2025-11-25");
    assert_eq!(res["serverInfo"]["name"], "sprigdb");
    assert_eq!(res["capabilities"]["tools"]["listChanged"], false);
    assert!(res["instructions"].as_str().unwrap().contains("untrusted"));
    assert_eq!(c.session.as_ref().unwrap().len(), 64);

    // The data role's read tools; no proposals by default.
    let r = c.rpc("tools/list", json!({})).await;
    assert_eq!(r.status, 200, "{}", r.raw);
    let names: Vec<&str> = r.body["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(
        names.contains(&"run_query") && names.contains(&"describe"),
        "{names:?}"
    );
    assert!(
        !names.iter().any(|n| n.starts_with("propose_")),
        "{names:?}"
    );
    assert!(!names.contains(&"workload"), "a DBA tool");

    // A read, as ann; data sharing is off, so rows are reduced to shape.
    let r = c
        .call(
            "run_query",
            json!({"db": "shop", "sql": "SELECT email FROM customers"}),
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.raw);
    assert_eq!(r.body["result"]["isError"], false);
    let t = text(&r);
    assert!(t.starts_with("<tool_result untrusted=\"true\">"), "{t}");
    assert!(
        t.contains("rows_withheld") && !t.contains("carol@example.com"),
        "{t}"
    );
    assert_eq!(calls(&shared), [("ann".to_owned(), "run_query".to_owned())]);

    // With sharing on, the rows.
    shared.share.store(true, Ordering::SeqCst);
    let r = c
        .call(
            "run_query",
            json!({"db": "shop", "sql": "SELECT email FROM customers"}),
        )
        .await;
    assert!(text(&r).contains("carol@example.com"), "{}", r.raw);

    // Audited: one conversation for the client, with calls and results.
    let convs = shared.audit.list(UserId(7), 10).await.unwrap();
    assert_eq!(convs.len(), 1);
    assert_eq!(convs[0].title, "MCP: test-client");
    let kinds: Vec<MsgKind> = shared
        .audit
        .messages(convs[0].id)
        .await
        .unwrap()
        .iter()
        .map(|m| m.kind)
        .collect();
    assert_eq!(
        kinds,
        [
            MsgKind::ToolCall,
            MsgKind::ToolResult,
            MsgKind::ToolCall,
            MsgKind::ToolResult
        ]
    );

    // ping; an unknown method.
    let r = c.rpc("ping", json!({})).await;
    assert_eq!(r.body["result"], json!({}));
    let r = c.rpc("resources/list", json!({})).await;
    assert_eq!(r.body["error"]["code"], -32601, "{}", r.raw);

    // The session is ann's alone.
    let mut bob = Client::new(&addr, "tok-bob");
    bob.session.clone_from(&c.session);
    bob.version.clone_from(&c.version);
    let r = bob.rpc("tools/list", json!({})).await;
    assert_eq!(r.status, 404, "{}", r.raw);

    // Ending it.
    let r = c.send("DELETE", &[], None).await;
    assert_eq!(r.status, 204, "{}", r.raw);
    let r = c.rpc("tools/list", json!({})).await;
    assert_eq!(r.status, 404, "{}", r.raw);
    c.session = None;
    let r = c.rpc("tools/list", json!({})).await;
    assert_eq!(r.status, 400, "{}", r.raw);
    c.session = Some("0".repeat(64));
    let r = c.rpc("tools/list", json!({})).await;
    assert_eq!(r.status, 404, "{}", r.raw);
}

#[tokio::test]
async fn proposals_come_back_and_never_run() {
    let (addr, shared) = server().await;
    let mut c = Client::new(&addr, "tok-ann");
    c.connect().await;
    // Not offered by default: a protocol error naming 39996.
    let r = c
        .call(
            "propose_sql",
            json!({"db": "shop", "sql": "DELETE FROM customers", "why": "cleanup"}),
        )
        .await;
    assert_eq!(r.body["error"]["code"], -32602, "{}", r.raw);
    assert_eq!(r.body["error"]["data"]["code"], 39996, "{}", r.raw);

    shared.set(|s| s.tools = "read,propose".into());
    let r = c.rpc("tools/list", json!({})).await;
    assert!(r.raw.contains("\"propose_sql\""), "{}", r.raw);
    let r = c
        .call(
            "propose_sql",
            json!({"db": "shop", "sql": "DELETE FROM customers WHERE id = 3", "why": "a duplicate"}),
        )
        .await;
    assert_eq!(r.body["result"]["isError"], false, "{}", r.raw);
    let t = text(&r);
    assert!(
        t.contains("NOT run") && t.contains("DELETE FROM customers WHERE id = 3"),
        "{t}"
    );
    assert!(calls(&shared).is_empty(), "nothing ran");
    let convs = shared.audit.list(UserId(7), 10).await.unwrap();
    let msgs = shared.audit.messages(convs[0].id).await.unwrap();
    assert!(msgs.iter().any(|m| m.kind == MsgKind::Proposal));

    // A write through the read tool is refused as input.
    let r = c
        .call(
            "run_query",
            json!({"db": "shop", "sql": "DELETE FROM customers"}),
        )
        .await;
    assert_eq!(r.body["result"]["isError"], true, "{}", r.raw);
    assert!(calls(&shared).is_empty());
}

#[tokio::test]
async fn the_callers_own_rights_apply() {
    let (addr, shared) = server().await;
    let mut c = Client::new(&addr, "tok-bob");
    assert_eq!(c.connect().await.status, 200);
    let r = c
        .call(
            "run_query",
            json!({"db": "shop", "sql": "SELECT email FROM customers"}),
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.raw);
    assert_eq!(r.body["result"]["isError"], true);
    let t = text(&r);
    assert!(
        t.contains("error 1142") && t.contains("SELECT command denied"),
        "{t}"
    );
    assert_eq!(calls(&shared), [("bob".to_owned(), "run_query".to_owned())]);
}

#[tokio::test]
async fn sign_in_and_the_tenant_switches() {
    let (addr, shared) = server().await;
    // No one anonymous.
    let mut anon = Client::new(&addr, "x");
    anon.token = None;
    let r = anon.connect().await;
    assert_eq!(r.status, 401, "{}", r.raw);
    assert_eq!(r.body["error"]["code"], 1045);
    assert!(r.headers.contains_key(header::WWW_AUTHENTICATE));
    let r = Client::new(&addr, "tok-wrong").connect().await;
    assert_eq!(r.status, 401, "{}", r.raw);
    // A login without a tenant.
    let r = Client::new(&addr, "tok-operator").connect().await;
    assert_eq!(r.status, 400, "{}", r.raw);
    // The administration role is never offered.
    let mut c = Client::new(&addr, "tok-ann");
    c.role = Some("admin");
    let r = c.connect().await;
    assert_eq!(r.status, 400, "{}", r.raw);
    assert!(r.raw.contains("data or dba"), "{}", r.raw);
    // The DBA role's tools.
    c.role = Some("dba");
    assert_eq!(c.connect().await.status, 200);
    let r = c.rpc("tools/list", json!({})).await;
    assert!(r.raw.contains("\"workload\"") && !r.raw.contains("\"run_query\""));

    // ai.mcp.enabled off: 39991, before any session.
    shared.set(|s| s.enabled = false);
    let r = Client::new(&addr, "tok-ann").connect().await;
    assert_eq!(r.status, 403, "{}", r.raw);
    assert_eq!(r.body["error"]["code"], 39991);
    assert!(r.raw.contains("ai.mcp.enabled"), "{}", r.raw);
    // ... and for a session already open.
    let r = c.rpc("tools/list", json!({})).await;
    assert_eq!(r.status, 403, "{}", r.raw);
    // ai.enabled off.
    shared.set(|s| {
        s.enabled = true;
        s.ai_enabled = false;
    });
    let r = Client::new(&addr, "tok-ann").connect().await;
    assert_eq!(r.status, 403);
    assert!(r.raw.contains("(ai.enabled)"), "{}", r.raw);
}

#[tokio::test]
async fn transport_rules() {
    let (addr, shared) = server().await;
    let c = Client::new(&addr, "tok-ann");
    // Another site's page.
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": "ping"});
    let r = c
        .send("POST", &[("origin", "https://evil.example")], Some(&body))
        .await;
    assert_eq!(r.status, 403, "{}", r.raw);
    let own = format!("http://{addr}");
    let r = c.send("POST", &[("origin", own.as_str())], Some(&body)).await;
    assert_ne!(r.status, 403, "{}", r.raw);
    let r = c
        .send("POST", &[("origin", "https://db.example.com")], Some(&body))
        .await;
    assert_ne!(r.status, 403, "{}", r.raw);
    // No stream to open; no batches; no garbage.
    let r = c.send("GET", &[], None).await;
    assert_eq!(r.status, 405);
    assert_eq!(get(&r.headers, "allow"), Some("POST, DELETE"));
    let r = c.send("POST", &[], Some(&json!([body]))).await;
    assert_eq!((r.status, &r.body["error"]["code"]), (400, &json!(-32600)));
    let r = c.send("POST", &[], Some(&json!("hello"))).await;
    assert_eq!(r.status, 400);
    let mut c = Client::new(&addr, "tok-ann");
    c.version = Some("2024-01-01".into());
    let r = c.connect().await;
    assert_eq!(r.status, 200, "initialize negotiates: {}", r.raw);
    assert_eq!(r.body["result"]["protocolVersion"], "2025-11-25");
    c.version = Some("2024-01-01".into());
    let r = c.rpc("ping", json!({})).await;
    assert_eq!(r.status, 400, "{}", r.raw);
    // An SSE-only client gets one event.
    c.version = Some("2025-11-25".into());
    let body = json!({"jsonrpc": "2.0", "id": 9, "method": "ping"});
    let r = c
        .send("POST", &[("accept", "text/event-stream")], Some(&body))
        .await;
    assert_eq!(get(&r.headers, "content-type"), Some("text/event-stream"));
    assert!(r.raw.starts_with("event: message\ndata: {"), "{}", r.raw);
    // The per-minute limit.
    shared.set(|s| s.calls_per_minute = 2);
    for _ in 0..2 {
        let r = c.call("catalog", json!({})).await;
        assert_eq!(r.body["result"]["isError"], false, "{}", r.raw);
    }
    let r = c.call("catalog", json!({})).await;
    assert_eq!(r.body["result"]["isError"], true);
    assert!(text(&r).contains("ai.mcp.calls_per_minute"), "{}", r.raw);
    assert_eq!(calls(&shared).len(), 2);
}

#[tokio::test]
async fn the_stateless_revision() {
    let (addr, shared) = server().await;
    let mut c = Client::new(&addr, "tok-ann");
    let r = c.modern("server/discover", json!({}), &[]).await;
    assert_eq!(r.status, 200, "{}", r.raw);
    let res = &r.body["result"];
    assert_eq!(res["resultType"], "complete");
    assert_eq!(res["supportedVersions"], json!(["2026-07-28"]));
    assert_eq!(
        res["_meta"]["io.modelcontextprotocol/serverInfo"]["name"],
        "sprigdb"
    );
    assert!(get(&r.headers, SESSION_HEADER).is_none(), "no sessions");

    let r = c.modern("tools/list", json!({}), &[]).await;
    assert_eq!(r.body["result"]["resultType"], "complete");
    assert!(r.raw.contains("\"run_query\""));
    let args = json!({"name": "describe", "arguments": {"db": "shop", "table": "customers"}});
    let r = c.modern("tools/call", args.clone(), &[]).await;
    assert_eq!(r.status, 200, "{}", r.raw);
    assert_eq!(r.body["result"]["isError"], false);
    assert_eq!(calls(&shared), [("ann".to_owned(), "describe".to_owned())]);
    let convs = shared.audit.list(UserId(7), 10).await.unwrap();
    assert_eq!(convs[0].title, "MCP: modern-client");
    // Mcp-Name in its base64 form.
    let r = c
        .modern(
            "tools/call",
            args.clone(),
            &[("mcp-name", "=?base64?ZGVzY3JpYmU=?=")],
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.raw);

    // Headers that don't match the body.
    let r = c
        .modern("tools/call", args.clone(), &[("mcp-name", "run_query")])
        .await;
    assert_eq!(
        (r.status, &r.body["error"]["code"]),
        (400, &json!(-32020)),
        "{}",
        r.raw
    );
    let r = c
        .modern("tools/call", args.clone(), &[("mcp-name", "")])
        .await;
    assert_eq!(r.body["error"]["code"], -32020, "{}", r.raw);
    let r = c
        .modern("tools/list", json!({}), &[("mcp-method", "tools/call")])
        .await;
    assert_eq!(r.body["error"]["code"], -32020, "{}", r.raw);
    let r = c
        .modern("tools/list", json!({}), &[("mcp-protocol-version", "")])
        .await;
    assert_eq!(r.body["error"]["code"], -32020, "{}", r.raw);
    // A version this server doesn't speak.
    let meta = json!({"_meta": {
        "io.modelcontextprotocol/protocolVersion": "2027-01-01",
        "io.modelcontextprotocol/clientCapabilities": {},
    }});
    let r = c
        .modern(
            "tools/list",
            meta,
            &[("mcp-protocol-version", "2027-01-01")],
        )
        .await;
    assert_eq!(
        (r.status, &r.body["error"]["code"]),
        (400, &json!(-32022)),
        "{}",
        r.raw
    );
    assert_eq!(r.body["error"]["data"]["supported"], json!(["2026-07-28"]));
    // Required metadata missing.
    let meta = json!({"_meta": {"io.modelcontextprotocol/protocolVersion": "2026-07-28"}});
    let r = c.modern("tools/list", meta, &[]).await;
    assert_eq!(
        (r.status, &r.body["error"]["code"]),
        (400, &json!(-32602)),
        "{}",
        r.raw
    );
    // An unknown method; a tool not offered.
    let r = c.modern("prompts/list", json!({}), &[]).await;
    assert_eq!(
        (r.status, &r.body["error"]["code"]),
        (404, &json!(-32601)),
        "{}",
        r.raw
    );
    let r = c
        .modern(
            "tools/call",
            json!({"name": "workload", "arguments": {"hours": 1}}),
            &[],
        )
        .await;
    assert_eq!(r.body["error"]["data"]["code"], 39996, "{}", r.raw);
    // Turned off.
    shared.set(|s| s.enabled = false);
    let r = c.modern("tools/list", json!({}), &[]).await;
    assert_eq!(
        (r.status, &r.body["error"]["code"]),
        (403, &json!(39991)),
        "{}",
        r.raw
    );
}
