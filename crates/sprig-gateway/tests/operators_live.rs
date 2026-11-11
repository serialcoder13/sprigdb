//! `sprig.admin.v1.Operators` on a gateway's 8080/8443 (chapter 14,
//! "Operators"; decision OP12): sign-in, enrolment, tokens and sign-out
//! over gRPC and REST, each relayed to the controller, whose guard decides.
//!
//! ```sh
//! # a deployment whose controller has admin_token_file (and, after the
//! # first operator login, admin_token_break_glass = true), and a gateway
//! # with listener.http.* bound and enabled
//! export SPRIG_TEST_OPERATORS_GATEWAY=127.0.0.1:8080
//! export SPRIG_TEST_OPERATORS_TOKEN_FILE=/path/to/admin.token
//! cargo test -p sprig-gateway --test operators_live -- --nocapture
//! ```
//!
//! Each run creates one platform operator, `gw-op-<unix seconds>`, and
//! disables it at the end (operators are never deleted, decision OP8).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    missing_docs
)]

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use sprig_api::proto::admin::operators_client::OperatorsClient;
use sprig_api::proto::admin::{BodyRequest, ListRequest, NameRequest};
use sprig_testkit::skip;
use tonic::transport::{Channel, Endpoint};

const GATEWAY: &str = "SPRIG_TEST_OPERATORS_GATEWAY";
const TOKEN_FILE: &str = "SPRIG_TEST_OPERATORS_TOKEN_FILE";

/// RFC 4648 base32 without padding.
fn base32_decode(s: &str) -> Vec<u8> {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let (mut bits, mut value, mut out) = (0u32, 0u32, Vec::new());
    for c in s.trim_end_matches('=').bytes() {
        let i = u32::try_from(
            ALPHABET
                .iter()
                .position(|a| *a == c.to_ascii_uppercase())
                .expect("base32"),
        )
        .unwrap();
        value = (value << 5) | i;
        bits += 5;
        if bits >= 8 {
            out.push(((value >> (bits - 8)) & 0xff) as u8);
            bits -= 8;
        }
    }
    out
}

fn totp(secret: &str, offset: i64) -> String {
    let step = sprig_auth::mfa::step_of(std::time::SystemTime::now());
    let step = u64::try_from(i64::try_from(step).unwrap() + offset).unwrap();
    format!(
        "{:06}",
        sprig_auth::mfa::code_at(&base32_decode(secret), step)
    )
}

/// One REST call on the gateway: status and JSON.
async fn rest(
    addr: &str,
    method: &str,
    path: &str,
    auth: Option<&str>,
    body: &Value,
) -> (u16, Value) {
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut tx, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
        .await
        .unwrap();
    tokio::spawn(conn);
    let mut b = hyper::Request::builder()
        .method(method)
        .uri(path)
        .header("host", addr)
        .header("content-type", "application/json");
    if let Some(a) = auth {
        b = b.header("authorization", a);
    }
    let payload = if body.is_null() {
        Bytes::new()
    } else {
        Bytes::from(body.to_string())
    };
    let resp = tx
        .send_request(b.body(Full::new(payload)).unwrap())
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn with_auth<T>(msg: T, auth: &str) -> tonic::Request<T> {
    let mut r = tonic::Request::new(msg);
    r.metadata_mut()
        .insert("authorization", auth.parse().unwrap());
    r
}

fn body(v: &Value) -> BodyRequest {
    BodyRequest {
        body_json: v.to_string(),
        query: String::new(),
    }
}

fn named(name: &str, v: &Value) -> NameRequest {
    NameRequest {
        name: name.to_owned(),
        query: String::new(),
        body_json: v.to_string(),
    }
}

fn parse(json: &str) -> Value {
    serde_json::from_str(json).unwrap()
}

#[tokio::test]
async fn operators_through_the_gateway() {
    let Some(addr) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, "operators_through_the_gateway");
        return;
    };
    let Some(token_file) = skip::var(TOKEN_FILE) else {
        skip::report_missing(TOKEN_FILE, "operators_through_the_gateway");
        return;
    };
    let bootstrap = format!(
        "Bearer {}",
        std::fs::read_to_string(token_file).unwrap().trim()
    );
    let ch: Channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut ops = OperatorsClient::new(ch);
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let name = format!("gw-op-{secs}");
    let password = format!("{name}-long-password");

    // CreateOperator over gRPC with the bootstrap credential.
    let r = ops
        .create_operator(with_auth(
            body(
                &json!({"name": name, "role": "platform_operator", "password": password,
                "reason": "OP12 live test"}),
            ),
            &bootstrap,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(parse(&r.json)["name"], name.as_str(), "{}", r.json);
    println!("created {name}");

    // Without a credential the guard refuses everything but Login and Enrol.
    let e = ops
        .list_operators(ListRequest::default())
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unauthenticated, "{e:?}");

    // Login over REST: no factor yet, so an enrolment ticket and a URI.
    let (s, v) = rest(
        &addr,
        "POST",
        "/admin/v1/operators:login",
        None,
        &json!({"name": name, "password": password, "client": "live-test"}),
    )
    .await;
    assert_eq!(s, 200, "{v}");
    let ticket = v["enrol_ticket"].as_str().expect("enrol ticket").to_owned();
    let uri = v["otpauth_uri"].as_str().unwrap().to_owned();
    let secret = uri
        .split("secret=")
        .nth(1)
        .unwrap()
        .split('&')
        .next()
        .unwrap()
        .to_owned();
    assert!(v.get("token").is_none(), "no session before enrolment: {v}");

    // A wrong password: refused with the login error, whichever part was wrong.
    let e = ops
        .login(body(
            &json!({"name": name, "password": "not-the-password-x"}),
        ))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unauthenticated, "{e:?}");

    // Enrol over gRPC: a session and ten recovery codes.
    let r = ops
        .enrol(body(
            &json!({"name": name, "ticket": ticket, "code": totp(&secret, 0)}),
        ))
        .await
        .unwrap()
        .into_inner();
    let v = parse(&r.json);
    let session = format!("Bearer {}", v["token"].as_str().expect("session"));
    assert!(session.starts_with("Bearer sprigop_"), "{session}");
    assert_eq!(v["recovery_codes"].as_array().unwrap().len(), 10);
    let recovery = v["recovery_codes"][0].as_str().unwrap().to_owned();

    // WhoAmI over gRPC, GetOperator over REST.
    let r = ops
        .who_am_i(with_auth(ListRequest::default(), &session))
        .await
        .unwrap()
        .into_inner();
    let v = parse(&r.json);
    assert_eq!(v["operator"]["name"], name.as_str(), "{v}");
    assert_eq!(v["switched"], true, "{v}");
    let (s, v) = rest(
        &addr,
        "GET",
        &format!("/admin/v1/operators/{name}"),
        Some(&session),
        &Value::Null,
    )
    .await;
    assert_eq!(s, 200, "{v}");
    assert_eq!(v["recovery_codes_left"], 10, "{v}");

    // A scoped token from the session; it reads clusters and nothing else.
    let r = ops
        .create_token(with_auth(
            body(&json!({"label": "op12", "scopes": ["clusters:read"], "expires_in_days": 1})),
            &session,
        ))
        .await
        .unwrap()
        .into_inner();
    let v = parse(&r.json);
    let tok = format!("Bearer {}", v["token"].as_str().unwrap());
    let key_id = v["record"]["key_id"].as_str().unwrap().to_owned();
    let (s, _) = rest(&addr, "GET", "/admin/v1/clusters", Some(&tok), &Value::Null).await;
    assert_eq!(s, 200);
    let (s, v) = rest(&addr, "GET", "/admin/v1/tenants", Some(&tok), &Value::Null).await;
    assert_eq!(s, 403, "{v}");
    assert_eq!(v["code"], 39313, "{v}");
    // A token can't issue tokens (OP5).
    let e = ops
        .create_token(with_auth(body(&json!({"scopes": ["*:read"]})), &tok))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::PermissionDenied, "{e:?}");
    let r = ops
        .list_tokens(with_auth(ListRequest::default(), &session))
        .await
        .unwrap()
        .into_inner();
    assert!(r.json.contains(&key_id), "{}", r.json);
    ops.revoke_token(with_auth(
        named(&key_id, &json!({"reason": "OP12 live test"})),
        &session,
    ))
    .await
    .unwrap();
    let (s, v) = rest(&addr, "GET", "/admin/v1/clusters", Some(&tok), &Value::Null).await;
    assert_eq!(s, 401, "{v}");
    assert_eq!(v["code"], 39316, "{v}");

    // Logout over gRPC ends the session.
    ops.logout(with_auth(body(&json!({})), &session))
        .await
        .unwrap();
    let e = ops
        .who_am_i(with_auth(ListRequest::default(), &session))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unauthenticated, "{e:?}");

    // A recovery code signs in once.
    let login = json!({"name": name, "password": password, "recovery_code": recovery});
    let r = ops.login(body(&login)).await.unwrap().into_inner();
    let v = parse(&r.json);
    assert!(
        v["token"]
            .as_str()
            .is_some_and(|t| t.starts_with("sprigop_")),
        "{v}"
    );
    let e = ops.login(body(&login)).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unauthenticated, "{e:?}");

    // Disable it (bootstrap, break glass): its sessions end.
    let session2 = format!("Bearer {}", v["token"].as_str().unwrap());
    ops.disable_operator(with_auth(
        named(&name, &json!({"reason": "OP12 live test done"})),
        &bootstrap,
    ))
    .await
    .unwrap();
    let e = ops
        .who_am_i(with_auth(ListRequest::default(), &session2))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unauthenticated, "{e:?}");
    println!("disabled {name}");
}
