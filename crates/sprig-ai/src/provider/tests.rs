//! The adapters against recorded streams: decoding the fixtures, encoding
//! requests, and whole calls against an in-process server (retries, errors,
//! headers). Nothing here reaches a real provider.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use sprig_common::SprigCode;
use zeroize::Zeroizing;

use super::anthropic::{self, AnthropicConfig, AnthropicProvider};
use super::openai::{self, KeyHeader, OpenAiConfig, OpenAiProvider};
use super::*;

const A_TEXT: &str = include_str!("../../fixtures/anthropic/text.sse");
const A_TOOLS: &str = include_str!("../../fixtures/anthropic/tools.sse");
const A_REFUSAL: &str = include_str!("../../fixtures/anthropic/refusal.sse");
const A_MAX: &str = include_str!("../../fixtures/anthropic/max_tokens.sse");
const A_OVERLOADED: &str = include_str!("../../fixtures/anthropic/overloaded.sse");
const O_TEXT: &str = include_str!("../../fixtures/openai/text.sse");
const O_TOOLS: &str = include_str!("../../fixtures/openai/tools.sse");
const O_REFUSAL: &str = include_str!("../../fixtures/openai/refusal.sse");
const O_MAX: &str = include_str!("../../fixtures/openai/max_tokens.sse");
const O_OLLAMA: &str = include_str!("../../fixtures/openai/ollama_tools.sse");
const O_ERROR: &str = include_str!("../../fixtures/openai/error.sse");

fn texts(d: &[Delta]) -> String {
    d.iter()
        .filter_map(|x| match x {
            Delta::Text(t) => Some(t.as_str()),
            _ => None,
        })
        .collect()
}

fn calls(d: &[Delta]) -> Vec<(String, String, Value)> {
    d.iter()
        .filter_map(|x| match x {
            Delta::ToolCall { id, name, input } => Some((id.clone(), name.clone(), input.clone())),
            _ => None,
        })
        .collect()
}

// ---- Anthropic: recorded streams ----

#[test]
fn anthropic_text_with_thinking() {
    let mut d = Vec::new();
    let o = anthropic::decode_recorded(A_TEXT, &mut d).unwrap();
    assert_eq!(o.stop, Stop::EndTurn);
    assert_eq!(texts(&d), "A gateway speaks every client protocol.");
    assert!(d.contains(&Delta::Thinking(
        "The user asks what a gateway does.".into()
    )));
    assert_eq!(
        o.usage,
        Usage {
            input_tokens: 1210,
            output_tokens: 57,
            cache_read_tokens: 0,
            cache_write_tokens: 2048,
        }
    );
    assert_eq!(o.model, "claude-opus-5-5");
    // The thinking block is kept, with its signature, before the text.
    assert_eq!(
        o.message.content[0],
        Block::Thinking {
            text: "The user asks what a gateway does.".into(),
            signature: "EqQBCkYIBRgCIkCsig".into()
        }
    );
    assert_eq!(o.message.text(), "A gateway speaks every client protocol.");
}

#[test]
fn anthropic_parallel_tool_calls() {
    let mut d = Vec::new();
    let o = anthropic::decode_recorded(A_TOOLS, &mut d).unwrap();
    assert_eq!(o.stop, Stop::ToolUse);
    assert_eq!(
        calls(&d),
        vec![
            (
                "toolu_01A".into(),
                "describe".into(),
                json!({"db": "shop", "table": "orders"})
            ),
            (
                "toolu_01B".into(),
                "describe".into(),
                json!({"db": "shop", "table": "customers"})
            ),
        ]
    );
    assert_eq!(o.message.tool_calls().len(), 2);
    assert_eq!(o.usage.cache_read_tokens, 3050);
    assert_eq!(o.usage.output_tokens, 92);
}

#[test]
fn anthropic_refusal_carries_its_explanation() {
    let mut d = Vec::new();
    let o = anthropic::decode_recorded(A_REFUSAL, &mut d).unwrap();
    assert_eq!(
        o.stop,
        Stop::Refusal(
            "This request asks for help attacking a system I can't verify you own.".into()
        )
    );
    assert!(o.message.content.is_empty());
}

#[test]
fn anthropic_max_tokens_drops_the_unfinished_tool_call() {
    let mut d = Vec::new();
    let o = anthropic::decode_recorded(A_MAX, &mut d).unwrap();
    assert_eq!(o.stop, Stop::MaxTokens);
    assert!(calls(&d).is_empty());
    assert!(o.message.tool_calls().is_empty());
    assert_eq!(o.message.text(), "The plan reads every row of");
}

#[test]
fn anthropic_stream_errors_and_cut_streams() {
    let e = anthropic::decode_recorded(A_OVERLOADED, &mut Vec::new()).unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::AiProviderError));
    assert!(e.retryable);
    assert!(e.message.contains("overloaded_error"));
    let cut = &A_TEXT[..A_TEXT.find("event: message_delta").unwrap()];
    let e = anthropic::decode_recorded(cut, &mut Vec::new()).unwrap_err();
    assert!(e.message.contains("before message_stop"));
}

// ---- OpenAI-compatible: recorded streams ----

#[test]
fn openai_text_and_cached_usage() {
    let mut d = Vec::new();
    let o = openai::decode_recorded(O_TEXT, &mut d).unwrap();
    assert_eq!(o.stop, Stop::EndTurn);
    assert_eq!(texts(&d), "A gateway speaks every client protocol.");
    assert_eq!(
        o.usage,
        Usage {
            input_tokens: 476,
            output_tokens: 40,
            cache_read_tokens: 1024,
            cache_write_tokens: 0,
        }
    );
}

#[test]
fn openai_parallel_tool_calls() {
    let mut d = Vec::new();
    let o = openai::decode_recorded(O_TOOLS, &mut d).unwrap();
    assert_eq!(o.stop, Stop::ToolUse);
    assert_eq!(
        calls(&d),
        vec![
            (
                "call_A".into(),
                "describe".into(),
                json!({"db": "shop", "table": "orders"})
            ),
            (
                "call_B".into(),
                "describe".into(),
                json!({"db": "shop", "table": "customers"})
            ),
        ]
    );
}

#[test]
fn openai_refusal_and_max_tokens() {
    let o = openai::decode_recorded(O_REFUSAL, &mut Vec::new()).unwrap();
    assert_eq!(
        o.stop,
        Stop::Refusal("I can't help with that request.".into())
    );
    let mut d = Vec::new();
    let o = openai::decode_recorded(O_MAX, &mut d).unwrap();
    assert_eq!(o.stop, Stop::MaxTokens);
    assert!(d.contains(&Delta::Thinking("Thinking about the plan.".into())));
    assert_eq!(o.usage.output_tokens, 64);
    // Reasoning from these servers isn't kept: it can't be sent back.
    assert_eq!(o.message.content.len(), 1);
}

#[test]
fn ollama_tool_calls_that_end_with_stop_still_ask_for_tools() {
    let o = openai::decode_recorded(O_OLLAMA, &mut Vec::new()).unwrap();
    assert_eq!(o.stop, Stop::ToolUse);
    assert_eq!(o.message.tool_calls()[0].2, &json!({"db": "shop"}));
}

#[test]
fn openai_stream_error() {
    let e = openai::decode_recorded(O_ERROR, &mut Vec::new()).unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::AiProviderError));
    assert!(e.retryable);
}

// ---- encoding ----

fn request() -> ChatRequest {
    ChatRequest {
        model: anthropic::DEFAULT_MODEL.into(),
        system: vec![Block::text("You are the DBA assistant.")],
        messages: vec![
            Message::user("Why is this slow?"),
            Message {
                role: MsgRole::Assistant,
                content: vec![
                    Block::Thinking {
                        text: "check".into(),
                        signature: "sig".into(),
                    },
                    Block::Text {
                        text: "Let me look.".into(),
                    },
                    Block::ToolUse {
                        id: "t1".into(),
                        name: "describe".into(),
                        input: json!({"db": "shop", "table": "orders"}),
                    },
                ],
            },
            Message {
                role: MsgRole::User,
                content: vec![Block::ToolResult {
                    tool_use_id: "t1".into(),
                    content: "<tool_result untrusted=\"true\">\n{}\n</tool_result>".into(),
                    is_error: true,
                }],
            },
        ],
        tools: vec![ToolDef {
            name: "describe".into(),
            description: "Describe a table".into(),
            input_schema: json!({"type": "object", "additionalProperties": false,
                "properties": {"db": {"type": "string"}, "table": {"type": "string"}},
                "required": ["db", "table"]}),
            strict: true,
        }],
        max_tokens: 16_000,
        effort: Some(Effort::High),
        cache_prefix: true,
    }
}

#[test]
fn anthropic_request_shape() {
    let v = anthropic::encode_request(&AnthropicConfig::default(), &request());
    assert_eq!(v["model"], "claude-opus-5-5");
    assert_eq!(v["stream"], true);
    assert_eq!(v["max_tokens"], 16_000);
    assert_eq!(v["system"][0]["cache_control"]["type"], "ephemeral");
    assert_eq!(v["cache_control"]["type"], "ephemeral");
    assert_eq!(v["tools"][0]["strict"], true);
    assert_eq!(v["tools"][0]["input_schema"]["required"][1], "table");
    assert!(v.get("tool_choice").is_none());
    assert_eq!(
        v["thinking"],
        json!({"type": "adaptive", "display": "summarized"})
    );
    assert_eq!(v["output_config"]["effort"], "high");
    let m = &v["messages"];
    assert_eq!(
        m[1]["content"][0],
        json!({"type": "thinking", "thinking": "check", "signature": "sig"})
    );
    assert_eq!(m[1]["content"][2]["type"], "tool_use");
    assert_eq!(m[2]["content"][0]["type"], "tool_result");
    assert_eq!(m[2]["content"][0]["is_error"], true);
    // No cache markers without caching; no thinking for models without it.
    let mut r = request();
    r.cache_prefix = false;
    let cfg = AnthropicConfig {
        adaptive_thinking: false,
        ..AnthropicConfig::default()
    };
    let v = anthropic::encode_request(&cfg, &r);
    assert!(v.get("cache_control").is_none());
    assert!(v["system"][0].get("cache_control").is_none());
    assert!(v.get("thinking").is_none());
}

#[test]
fn openai_request_shape() {
    let cfg = OpenAiConfig {
        strict_tools: true,
        send_effort: true,
        ..OpenAiConfig::default()
    };
    let v = openai::encode_request(&cfg, &request());
    let m = v["messages"].as_array().unwrap();
    assert_eq!(
        m[0],
        json!({"role": "system", "content": "You are the DBA assistant."})
    );
    assert_eq!(m[1]["role"], "user");
    assert_eq!(m[2]["role"], "assistant");
    assert_eq!(m[2]["content"], "Let me look.");
    assert_eq!(m[2]["tool_calls"][0]["function"]["name"], "describe");
    let args: Value = serde_json::from_str(
        m[2]["tool_calls"][0]["function"]["arguments"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(args, json!({"db": "shop", "table": "orders"}));
    assert_eq!(m[3]["role"], "tool");
    assert_eq!(m[3]["tool_call_id"], "t1");
    assert!(m[3]["content"].as_str().unwrap().starts_with("error: "));
    assert_eq!(v["tools"][0]["type"], "function");
    assert_eq!(v["tools"][0]["function"]["strict"], true);
    assert_eq!(v["reasoning_effort"], "high");
    assert_eq!(v["max_tokens"], 16_000);
    assert_eq!(v["stream_options"]["include_usage"], true);
    // A schema with an optional property isn't sent as strict.
    let mut r = request();
    r.tools[0].input_schema["required"] = json!(["db"]);
    let v = openai::encode_request(&cfg, &r);
    assert!(v["tools"][0]["function"].get("strict").is_none());
}

// ---- whole calls against an in-process server ----

type Seen = Arc<Mutex<Vec<(hyper::HeaderMap, String, String)>>>;

async fn server(replies: Vec<(u16, &'static str)>) -> (SocketAddr, Seen) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let replies = Arc::new(Mutex::new(replies.into_iter().collect::<VecDeque<_>>()));
    let seen: Seen = Arc::default();
    let s2 = Arc::clone(&seen);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let replies = Arc::clone(&replies);
            let seen = Arc::clone(&s2);
            tokio::spawn(async move {
                let svc = service_fn(move |req: Request<hyper::body::Incoming>| {
                    let replies = Arc::clone(&replies);
                    let seen = Arc::clone(&seen);
                    async move {
                        let (parts, body) = req.into_parts();
                        let body = body
                            .collect()
                            .await
                            .map(http_body_util::Collected::to_bytes)
                            .unwrap_or_default();
                        seen.lock().unwrap().push((
                            parts.headers,
                            parts.uri.to_string(),
                            String::from_utf8_lossy(&body).into_owned(),
                        ));
                        let (status, text) = replies.lock().unwrap().pop_front().unwrap_or((
                            500,
                            "{\"error\":{\"type\":\"api_error\",\"message\":\"no reply\"}}",
                        ));
                        let ct = if status == 200 {
                            "text/event-stream"
                        } else {
                            "application/json"
                        };
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(status)
                                .header("content-type", ct)
                                .header("retry-after", "0")
                                .body(Full::new(Bytes::from_static(text.as_bytes())))
                                .unwrap(),
                        )
                    }
                });
                let _ = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), svc)
                    .await;
            });
        }
    });
    (addr, seen)
}

fn fast() -> HttpLimits {
    HttpLimits {
        connect_timeout: Duration::from_secs(2),
        idle_timeout: Duration::from_secs(2),
        retries: 2,
        backoff: Duration::from_millis(1),
    }
}

fn anthropic_at(addr: SocketAddr) -> AnthropicProvider {
    AnthropicProvider::new(
        AnthropicConfig {
            name: "anthropic".into(),
            base_url: format!("http://{addr}"),
            limits: fast(),
            ..AnthropicConfig::default()
        },
        Zeroizing::new("sk-ant-test-key".into()),
    )
    .unwrap()
}

#[tokio::test]
async fn anthropic_call_retries_overload_then_streams() {
    let overloaded =
        "{\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}";
    let (addr, seen) = server(vec![(529, overloaded), (200, A_OVERLOADED), (200, A_TOOLS)]).await;
    let p = anthropic_at(addr);
    let mut d = Vec::new();
    let o = p.chat(request(), &mut d).await.unwrap();
    assert_eq!(o.stop, Stop::ToolUse);
    assert_eq!(calls(&d).len(), 2);
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 3, "a 529 and an overloaded stream are retried");
    let (h, uri, body) = &seen[2];
    assert_eq!(uri, "/v1/messages");
    assert_eq!(h["x-api-key"], "sk-ant-test-key");
    assert_eq!(h["anthropic-version"], anthropic::API_VERSION);
    assert_eq!(h["content-type"], "application/json");
    // What was sent is exactly the body, and holds no key.
    assert_eq!(*body, *o.sent.body);
    assert!(!body.contains("sk-ant-test-key"));
    assert_eq!(o.sent, Sent::of(body.clone()));
}

#[tokio::test]
async fn bad_keys_are_not_retried() {
    let (addr, seen) = server(vec![(
        401,
        "{\"type\":\"error\",\"error\":{\"type\":\"authentication_error\",\"message\":\"invalid x-api-key\"}}",
    )])
    .await;
    let e = anthropic_at(addr)
        .chat(request(), &mut Vec::new())
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::AiProviderError));
    assert!(!e.retryable);
    assert!(e.message.contains("authentication_error"), "{}", e.message);
    assert_eq!(seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn retries_end_with_39993() {
    let busy = "{\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"busy\"}}";
    let (addr, seen) = server(vec![(500, busy), (503, busy), (502, busy), (200, A_TEXT)]).await;
    let e = anthropic_at(addr)
        .chat(request(), &mut Vec::new())
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::AiProviderError));
    assert!(e.retryable);
    assert_eq!(
        seen.lock().unwrap().len(),
        3,
        "the first try and two retries"
    );
}

#[tokio::test]
async fn nothing_is_retried_once_text_streamed() {
    // Text, then the stream breaks off: the user saw part of an answer, so
    // it isn't silently tried again.
    let cut: &'static str = A_TEXT.split("event: message_delta").next().unwrap();
    let (addr, seen) = server(vec![(200, cut), (200, A_TEXT)]).await;
    let mut d = Vec::new();
    let e = anthropic_at(addr)
        .chat(request(), &mut d)
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::AiProviderError));
    assert!(!texts(&d).is_empty());
    assert_eq!(seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn openai_compatible_call_with_azure_style_key_and_query() {
    let (addr, seen) = server(vec![(200, O_TOOLS)]).await;
    let p = OpenAiProvider::new(
        OpenAiConfig {
            name: "azure".into(),
            base_url: format!("http://{addr}/openai/deployments/gpt"),
            query: Some("api-version=2024-10-21".into()),
            key_header: KeyHeader::ApiKey,
            limits: fast(),
            ..OpenAiConfig::default()
        },
        Zeroizing::new("azure-key".into()),
    )
    .unwrap();
    let o = p.chat(request(), &mut Vec::new()).await.unwrap();
    assert_eq!(o.stop, Stop::ToolUse);
    let seen = seen.lock().unwrap();
    let (h, uri, _) = &seen[0];
    assert_eq!(
        uri,
        "/openai/deployments/gpt/chat/completions?api-version=2024-10-21"
    );
    assert_eq!(h["api-key"], "azure-key");
    assert!(h.get("authorization").is_none());
}

#[tokio::test]
async fn openai_bearer_and_no_key() {
    let (addr, seen) = server(vec![(200, O_TEXT), (200, O_TEXT)]).await;
    let mk = |kh, key: &str| {
        OpenAiProvider::new(
            OpenAiConfig {
                base_url: format!("http://{addr}/v1"),
                key_header: kh,
                limits: fast(),
                ..OpenAiConfig::default()
            },
            Zeroizing::new(key.into()),
        )
        .unwrap()
    };
    mk(KeyHeader::Bearer, "sk-test")
        .chat(request(), &mut Vec::new())
        .await
        .unwrap();
    mk(KeyHeader::None, "")
        .chat(request(), &mut Vec::new())
        .await
        .unwrap();
    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].0["authorization"], "Bearer sk-test");
    assert!(seen[1].0.get("authorization").is_none());
    assert_eq!(seen[0].1, "/v1/chat/completions");
}

#[test]
fn base_urls_are_checked() {
    let k = || Zeroizing::new(String::new());
    for bad in [
        "ftp://x",
        "api.anthropic.com",
        "https://x/v1?key=1",
        "http://",
    ] {
        let cfg = OpenAiConfig {
            base_url: bad.into(),
            ..OpenAiConfig::default()
        };
        let e = OpenAiProvider::new(cfg, k()).unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::AiNotConfigured), "{bad}");
    }
    assert!(AnthropicProvider::new(AnthropicConfig::default(), k()).is_ok());
}

#[test]
fn the_debug_form_never_shows_a_key() {
    let p = AnthropicProvider::new(
        AnthropicConfig::default(),
        Zeroizing::new("sk-ant-hidden".into()),
    )
    .unwrap();
    assert!(!format!("{p:?}").contains("hidden"));
}
