#![allow(clippy::unwrap_used, clippy::many_single_char_names)]

use std::time::{Duration, SystemTime};

use sha2::{Digest, Sha256};
use sprig_common::ids::UserId;
use sprig_common::time::Clock as _;
use sprig_settings::Value;

use super::*;
use crate::apikey::ApiKeyRow;
use crate::cache::TenantAuth;
use crate::mysql::FixedPolicy;
use crate::mysql::tests::{ACME, World, snapshot, world};

fn b64(s: &str) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for c in s.as_bytes().chunks(3) {
        let n = (u32::from(c[0]) << 16)
            | (u32::from(*c.get(1).unwrap_or(&0)) << 8)
            | u32::from(*c.get(2).unwrap_or(&0));
        for i in 0..4 {
            if i <= c.len() {
                out.push(A[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

fn basic(user: &str, pw: &str) -> String {
    format!("Basic {}", b64(&format!("{user}:{pw}")))
}

fn req(auth: &str) -> HttpRequest<'_> {
    HttpRequest {
        authorization: Some(auth),
        host: Some("127.0.0.1:8443"),
        sni: None,
        tenant_header: None,
        client_ip: "192.0.2.10".parse().unwrap(),
        tls: true,
    }
}

const SECRET: &str = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";

/// `acme` gets two keys of `svc` (user 10, `api_key` only): `aaaaaaaaaaaa`
/// live, `bbbbbbbbbbbb` revoked; and `cccccccccccc` of alice (no `api_key`
/// method).
fn with_keys(w: &World) {
    let now = w.clock.wall();
    let key = |id: &str, user: u32, revoked: Option<SystemTime>| ApiKeyRow {
        key_id: id.into(),
        user_id: UserId(user),
        name: "ci".into(),
        secret_sha256: Sha256::digest(SECRET.as_bytes()).into(),
        scopes: serde_json::json!([]),
        allowed_cidrs: None,
        expires_at: None,
        revoked_at: revoked,
        created_by: None,
        created_at: now,
        changed_version: 8,
    };
    let mut map = w.catalogs.map.lock().unwrap();
    let auth = TenantAuth::of(&map[&ACME]).unwrap().clone().with_api_keys(
        vec![
            key("aaaaaaaaaaaa", 10, None),
            key("bbbbbbbbbbbb", 10, Some(now)),
            key("cccccccccccc", 1, None),
        ],
        false,
    );
    map.insert(ACME, snapshot(ACME, auth, 8));
}

fn api(id: &str) -> String {
    format!("Bearer sprig_acme_{id}_{SECRET}")
}

#[test]
fn base64_decodes() {
    assert_eq!(
        base64_decode(&b64("acme.alice:pw")).unwrap(),
        b"acme.alice:pw"
    );
    assert_eq!(base64_decode("YQ==").unwrap(), b"a");
    assert_eq!(base64_decode("YWI").unwrap(), b"ab");
    assert!(base64_decode("Y").is_none());
    assert!(base64_decode("!!!!").is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn basic_logins_then_the_fast_path() {
    let w = world(FixedPolicy::default());
    let h = HttpAuth::new(Arc::clone(&w.svc));
    let a = basic("acme.alice", "pw");
    let p = h.authenticate(&req(&a)).await.unwrap();
    assert_eq!((p.tenant, p.user.as_str()), (Some(ACME), "alice"));
    let p = h.authenticate(&req(&a)).await.unwrap();
    assert_eq!(p.user_id, Some(UserId(1)));
    let ev = w.audit.events();
    let mech: Vec<_> = ev.iter().map(|e| (e.protocol, e.mechanism)).collect();
    assert_eq!(
        mech,
        vec![("http", Some("full")), ("http", Some("fast"))],
        "the second request takes the fast cache"
    );
    // The tenant from the Host header under a tenant suffix, or the header.
    let a = basic("alice", "pw");
    let mut r = req(&a);
    r.host = Some("acme.db.test:8443");
    assert_eq!(h.authenticate(&r).await.unwrap().user, "alice");
    let mut r = req(&a);
    r.tenant_header = Some("acme");
    r.host = None;
    assert_eq!(h.authenticate(&r).await.unwrap().tenant, Some(ACME));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refusals_map_to_statuses() {
    let w = world(FixedPolicy::default());
    let h = HttpAuth::new(Arc::clone(&w.svc));
    let check = async |auth: Option<&str>, status: u16, code: u16| {
        let r = HttpRequest {
            authorization: auth,
            ..req("")
        };
        let e = h.authenticate(&r).await.unwrap_err();
        assert_eq!((e.status, e.error.code), (status, code), "{auth:?}: {e:?}");
        assert_eq!(e.www_authenticate.is_some(), status == 401);
        e
    };
    let e = check(None, 401, 1045).await;
    assert_eq!(e.www_authenticate, Some(WWW_AUTHENTICATE));
    check(Some(&basic("acme.alice", "wrong")), 401, 1045).await;
    check(Some(&basic("acme.nobody", "pw")), 401, 1045).await;
    check(Some(&basic("acme.locked", "pw")), 401, 3118).await;
    check(Some(&basic("acme.blocked", "pw")), 401, 3955).await;
    // No sandbox mode over HTTP: an expired password is refused.
    check(Some(&basic("acme.expired", "pw")), 401, 1862).await;
    check(Some(&basic("susp.carol", "pw")), 403, 39303).await;
    check(Some("Bearer eyJhbGciOi.x.y"), 401, 1045).await;
    check(Some("Digest abc"), 401, 1045).await;
    w.catalogs
        .fail
        .store(true, std::sync::atomic::Ordering::Relaxed);
    check(Some(&basic("acme.alice", "pw")), 503, 39301).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_keys() {
    let w = world(FixedPolicy::default());
    with_keys(&w);
    let h = HttpAuth::new(Arc::clone(&w.svc));
    let k = api("aaaaaaaaaaaa");
    let p = h.authenticate(&req(&k)).await.unwrap();
    assert_eq!((p.user.as_str(), p.user_id), ("svc", Some(UserId(10))));
    let ok = w.audit.events().pop().unwrap();
    assert_eq!(
        (ok.event, ok.mechanism, ok.detail.as_deref()),
        (
            "auth.login_succeeded",
            Some("api_key"),
            Some("key aaaaaaaaaaaa")
        )
    );
    let refused = async |auth: String, reason: &str| {
        let e = h.authenticate(&req(&auth)).await.unwrap_err();
        assert_eq!((e.status, e.error.code), (401, 1045), "{reason}");
        assert_eq!(
            w.audit.events().pop().unwrap().reason,
            Some(reason).map(|r| {
                // 'static from the event set
                match r {
                    "key_revoked" => "key_revoked",
                    _ => "bad_credentials",
                }
            })
        );
    };
    refused(api("bbbbbbbbbbbb"), "key_revoked").await;
    refused(api("cccccccccccc"), "bad_credentials").await; // no api_key method
    refused(api("dddddddddddd"), "bad_credentials").await; // unknown id
    let wrong = format!("Bearer sprig_acme_aaaaaaaaaaaa_{}", "f".repeat(64));
    refused(wrong, "bad_credentials").await;
    let other_tenant = format!("Bearer sprig_nobody_aaaaaaaaaaaa_{SECRET}");
    refused(other_tenant, "bad_credentials").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_address_gets_429() {
    let mut p = FixedPolicy::default();
    p.0.insert(crate::settings::IP_FAILURES_PER_MINUTE.key, Value::Int(2));
    p.0.insert(crate::settings::IP_BLOCK_SECONDS.key, Value::Int(30));
    let w = world(p);
    let h = HttpAuth::new(Arc::clone(&w.svc));
    let bad = basic("acme.alice", "wrong");
    for _ in 0..3 {
        let _ = h.authenticate(&req(&bad)).await;
    }
    let good = basic("acme.alice", "pw");
    let e = h.authenticate(&req(&good)).await.unwrap_err();
    assert_eq!((e.status, e.error.code), (429, 39300));
    assert_eq!(e.retry_after, Some(30));
    w.clock.advance(Duration::from_secs(30));
    h.authenticate(&req(&good)).await.unwrap();
}
