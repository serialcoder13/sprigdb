#![allow(clippy::unwrap_used, clippy::many_single_char_names)]

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use sprig_common::ids::{DcId, TenantId, UserId};
use sprig_meta::{MemStore, MetaStore, Tier};

use super::*;

fn principal() -> Principal {
    Principal {
        tenant: Some(TenantId(7)),
        tenant_name: Some("acme".into()),
        user_id: Some(UserId(3)),
        user: "app".into(),
        host: "%".into(),
        client_ip: "127.0.0.1".parse().unwrap(),
        must_change_password: false,
        limits: crate::store::AccountLimits::default(),
        auth_version: 0,
        backend: crate::BackendTarget {
            cluster: sprig_common::ids::ClusterId::new("dev-c1").unwrap(),
            user: "t7_u3".into(),
            credentials: "file:/dev/null".parse().unwrap(),
        },
    }
}

fn code(e: &SprigError) -> u16 {
    e.protocol().mysql_errno
}

fn ring(gw: &str, store: Option<Arc<dyn MetaStore>>) -> Arc<KeyRing> {
    Arc::new(
        KeyRing::new(
            DcId::new("dc-a").unwrap(),
            gw,
            store,
            Duration::from_secs(30 * 86_400),
        )
        .unwrap(),
    )
}

fn ip() -> IpAddr {
    "127.0.0.1".parse().unwrap()
}

#[tokio::test]
async fn login_verify_refresh_and_reuse() {
    let store: Arc<dyn SessionStore> = Arc::new(MemorySessions::default());
    let t = Tokens::new(ring("gw-1", None), Arc::clone(&store));
    let now = SystemTime::now();
    let l = Limits::default();
    let i = t.login(principal(), "studio", l, now).await.unwrap();
    assert!(i.access_token.starts_with(ACCESS_PREFIX));
    assert!(i.refresh_token.starts_with("acme."));
    assert_eq!(i.expires_in, 900);
    let v = t.verify(&i.access_token, ip(), false, now).await.unwrap();
    assert_eq!(v.principal.user, "app");
    assert_eq!(v.claims.sub, "7:3");
    assert_eq!(v.claims.cli, "studio");
    // Expired access token: refused; the session lives on.
    let later = now + Duration::from_secs(900 + 61);
    assert_eq!(
        code(&t.verify(&i.access_token, ip(), false, later).await.unwrap_err()),
        1045
    );
    let r = t.refresh(&i.refresh_token, l, later).await.unwrap();
    assert!(t.verify(&r.access_token, ip(), false, later).await.is_ok());
    // The first refresh token again: theft. The session ends, for the new
    // tokens too.
    let e = t.refresh(&i.refresh_token, l, later).await.unwrap_err();
    assert_eq!(code(&e), 1045);
    assert!(t.verify(&r.access_token, ip(), false, later).await.is_err());
    assert!(t.refresh(&r.refresh_token, l, later).await.is_err());
}

#[tokio::test]
async fn idle_and_age_limits() {
    let store = Arc::new(MemorySessions::default());
    let t = Tokens::new(ring("gw-1", None), store.clone());
    let t0 = SystemTime::now();
    let l = Limits {
        idle: Duration::from_secs(60),
        max_age: Duration::from_secs(300),
        access_ttl: Duration::from_secs(900),
    };
    let i = t.login(principal(), "studio", l, t0).await.unwrap();
    let e = t
        .refresh(&i.refresh_token, l, t0 + Duration::from_secs(61))
        .await
        .unwrap_err();
    assert_eq!(code(&e), 39924);
    assert!(store.is_empty());

    let i = t.login(principal(), "studio", l, t0).await.unwrap();
    let mut tok = i.refresh_token;
    let mut at = t0;
    for _ in 0..5 {
        at += Duration::from_secs(50);
        tok = t.refresh(&tok, l, at).await.unwrap().refresh_token;
    }
    let e = t
        .refresh(&tok, l, t0 + Duration::from_secs(300))
        .await
        .unwrap_err();
    assert_eq!(code(&e), 39925);
}

#[tokio::test]
async fn any_gateway_verifies_through_the_published_key() {
    let global: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Global));
    let sessions: Arc<dyn SessionStore> = Arc::new(MemorySessions::default());
    let a = Tokens::new(ring("gw-a", Some(Arc::clone(&global))), Arc::clone(&sessions));
    let b = Tokens::new(ring("gw-b", Some(Arc::clone(&global))), Arc::clone(&sessions));
    a.keys().publish().await.unwrap();
    b.keys().publish().await.unwrap();
    let now = SystemTime::now();
    let i = a
        .login(principal(), "studio", Limits::default(), now)
        .await
        .unwrap();
    // B never saw the login: it verifies with A's published public key.
    let v = b.verify(&i.access_token, ip(), true, now).await.unwrap();
    assert_eq!(v.principal.tenant_name.as_deref(), Some("acme"));
    // B rotates, A sees the reuse of the first token.
    let r = b
        .refresh(&i.refresh_token, Limits::default(), now)
        .await
        .unwrap();
    assert!(a.verify(&r.access_token, ip(), true, now).await.is_ok());
    assert_eq!(
        code(
            &a.refresh(&i.refresh_token, Limits::default(), now)
                .await
                .unwrap_err()
        ),
        1045
    );
    assert!(b.verify(&r.access_token, ip(), true, now).await.is_err());
}

#[tokio::test]
async fn forged_and_foreign_tokens_are_refused() {
    let sessions: Arc<dyn SessionStore> = Arc::new(MemorySessions::default());
    let a = Tokens::new(ring("gw-a", None), Arc::clone(&sessions));
    let other = Tokens::new(ring("gw-x", None), Arc::clone(&sessions));
    let now = SystemTime::now();
    let i = a
        .login(principal(), "studio", Limits::default(), now)
        .await
        .unwrap();
    // A key nobody published: unknown.
    assert!(other.verify(&i.access_token, ip(), true, now).await.is_err());
    // A changed payload: the signature fails.
    let jws = i.access_token.strip_prefix(ACCESS_PREFIX).unwrap();
    let mut parts: Vec<String> = jws.split('.').map(str::to_owned).collect();
    let mut claims: serde_json::Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(&parts[1]).unwrap()).unwrap();
    claims["sub"] = "7:1".into();
    parts[1] = URL_SAFE_NO_PAD.encode(claims.to_string());
    let forged = format!("{ACCESS_PREFIX}{}", parts.join("."));
    assert_eq!(
        code(&a.verify(&forged, ip(), true, now).await.unwrap_err()),
        1045
    );
    // alg "none", and an HMAC header: refused before any key is read.
    for alg in ["none", "HS256"] {
        let h = URL_SAFE_NO_PAD.encode(
            serde_json::json!({"alg": alg, "kid": "dc-a/gw-a-0", "typ": "JWT"}).to_string(),
        );
        let t = format!("{ACCESS_PREFIX}{h}.{}.", parts[1]);
        assert!(a.verify(&t, ip(), true, now).await.is_err(), "{alg}");
    }
    // A malformed refresh token, and one naming a session that isn't there.
    assert!(a.refresh("nope", Limits::default(), now).await.is_err());
    let fake = format!(
        "acme.{}.{}",
        URL_SAFE_NO_PAD.encode([1u8; 16]),
        URL_SAFE_NO_PAD.encode([2u8; 32])
    );
    assert_eq!(
        code(&a.refresh(&fake, Limits::default(), now).await.unwrap_err()),
        1045
    );
}

#[tokio::test]
async fn logout_needs_the_real_refresh_token() {
    let sessions = Arc::new(MemorySessions::default());
    let t = Tokens::new(ring("gw-a", None), sessions.clone());
    let now = SystemTime::now();
    let i = t
        .login(principal(), "studio", Limits::default(), now)
        .await
        .unwrap();
    let (_, sid) = parse_refresh(&i.refresh_token).unwrap();
    let forged = format!(
        "acme.{}.{}",
        URL_SAFE_NO_PAD.encode(sid),
        URL_SAFE_NO_PAD.encode([9u8; 32])
    );
    assert_eq!(t.logout_refresh(&forged, now).await.unwrap(), None);
    assert!(t.verify(&i.access_token, ip(), true, now).await.is_ok());
    assert_eq!(
        t.logout_refresh(&i.refresh_token, now).await.unwrap(),
        Some(i.sid.clone())
    );
    assert!(t.verify(&i.access_token, ip(), true, now).await.is_err());
    assert!(sessions.is_empty());
}

#[tokio::test]
async fn tickets_verify_on_any_gateway() {
    let global: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Global));
    let sessions: Arc<dyn SessionStore> = Arc::new(MemorySessions::default());
    let a = Tokens::new(ring("gw-a", Some(Arc::clone(&global))), Arc::clone(&sessions));
    let b = Tokens::new(ring("gw-b", Some(Arc::clone(&global))), Arc::clone(&sessions));
    a.keys().publish().await.unwrap();
    let t = a
        .sign_ticket(&serde_json::json!({"sid": "x", "exp": 1}))
        .unwrap();
    let v: serde_json::Value = b.verify_ticket(&t, SystemTime::now()).await.unwrap();
    assert_eq!(v["sid"], "x");
    // An access token isn't a ticket, nor the other way round.
    let i = a
        .login(principal(), "studio", Limits::default(), SystemTime::now())
        .await
        .unwrap();
    let jws = i.access_token.strip_prefix(ACCESS_PREFIX).unwrap();
    assert!(
        b.verify_ticket::<serde_json::Value>(jws, SystemTime::now())
            .await
            .is_err()
    );
    assert!(
        b.verify(&format!("{ACCESS_PREFIX}{t}"), ip(), true, SystemTime::now())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn keys_rotate_and_retire() {
    let global: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Global));
    let rotation = Duration::from_secs(86_400);
    let r = KeyRing::new(
        DcId::new("dc-a").unwrap(),
        "gw-a",
        Some(Arc::clone(&global)),
        rotation,
    )
    .unwrap();
    r.publish().await.unwrap();
    let first = r.signer();
    let now = SystemTime::now();
    assert!(!r.rotate_if_due(rotation, now).await.unwrap());
    assert!(r.rotate_if_due(rotation, now + rotation).await.unwrap());
    assert_ne!(r.signer().kid, first.kid);
    // Another gateway still verifies the old key until verify_until.
    let other = KeyRing::new(
        DcId::new("dc-b").unwrap(),
        "gw-b",
        Some(Arc::clone(&global)),
        rotation,
    )
    .unwrap();
    assert!(other.public_key(&first.kid, now).await.unwrap().is_some());
    let after = first.verify_until + Duration::from_secs(1);
    assert_eq!(other.sweep(now).await.unwrap(), 0);
    assert_eq!(other.sweep(after).await.unwrap(), 1);
    let fresh = KeyRing::new(DcId::new("dc-b").unwrap(), "gw-c", Some(global), rotation).unwrap();
    assert!(fresh.public_key(&first.kid, now).await.unwrap().is_none());
}

#[test]
fn kids_and_refresh_tokens_parse() {
    assert!(jws::valid_kid("dc-a/gw-1-0a0b0c0d0e0f"));
    assert!(!jws::valid_kid("dc-a"));
    assert!(!jws::valid_kid("dc-a/x/y"));
    assert!(!jws::valid_kid("dc-a/../x"));
    assert!(!jws::valid_kid("/x"));
    assert!(parse_refresh("acme.AAAAAAAAAAAAAAAAAAAAAA.short").is_none());
    let ok = format!(
        "acme.{}.{}",
        URL_SAFE_NO_PAD.encode([0u8; 16]),
        URL_SAFE_NO_PAD.encode([0u8; 32])
    );
    assert_eq!(parse_refresh(&ok).unwrap(), ("acme", [0u8; 16]));
}
