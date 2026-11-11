//! Operators over `MemStore`: bootstrap and the switch, login with TOTP
//! (enrolment, codes used once, recovery codes, lockout), tokens (issued
//! only from sessions, scopes, revocation), DC operators limited to their
//! DCs, disabling, and the guard over HTTP (actor rewriting, a forged
//! caller header dropped, audit names).

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::items_after_statements
)]

use std::time::Duration;

use axum::Router;
use axum::routing::{get, post};
use serde_json::{Value, json};
use sprig_common::time::ManualClock;
use sprig_meta::mem::MemStore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::guard::{AUTHN_HEADER, Guard};
use super::*;

struct Fixed;

impl FactorKeys for Fixed {
    fn current(&self, _: TenantId) -> Option<(u32, [u8; 32])> {
        Some((1, [7; 32]))
    }
    fn key(&self, _: TenantId, v: u32) -> Option<[u8; 32]> {
        (v == 1).then_some([7; 32])
    }
}

struct Env {
    ops: Arc<Operators>,
    clock: Arc<ManualClock>,
    global: Arc<MemStore>,
}

fn env_in(dc: &str, global: Arc<MemStore>, clock: Arc<ManualClock>) -> Env {
    let ops = Arc::new(Operators::new(
        Arc::clone(&global) as Arc<dyn MetaStore>,
        Arc::clone(&clock) as Arc<dyn Clock>,
        Some(Arc::new(Fixed)),
        None,
        DcId::new(dc).unwrap(),
    ));
    Env { ops, clock, global }
}

fn env() -> Env {
    env_in(
        "dc-a",
        Arc::new(MemStore::new(Tier::Global)),
        Arc::new(ManualClock::new()),
    )
}

fn guard(e: &Env) -> Guard {
    Guard::new(
        Arc::clone(&e.ops),
        Some("boot-secret".into()),
        Some("dc-secret".into()),
        vec![("gateway".into(), "gw-secret".into())],
        false,
    )
}

fn headers(token: &str) -> HeaderMapT {
    let mut h = axum::http::HeaderMap::new();
    h.insert(
        axum::http::header::AUTHORIZATION,
        format!("Bearer {token}").parse().unwrap(),
    );
    h
}

type HeaderMapT = axum::http::HeaderMap;

fn unbase32(s: &str) -> Vec<u8> {
    let (mut buf, mut bits, mut out) = (0u32, 0u32, Vec::new());
    for c in s.bytes() {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'2'..=b'7' => c - b'2' + 26,
            _ => continue,
        };
        buf = (buf << 5) | u32::from(v);
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push(u8::try_from((buf >> bits) & 0xff).unwrap());
        }
    }
    out
}

fn seed_of(uri: &str) -> Vec<u8> {
    let secret = uri
        .split("secret=")
        .nth(1)
        .unwrap()
        .split('&')
        .next()
        .unwrap();
    unbase32(secret)
}

fn code(e: &Env, seed: &[u8]) -> String {
    format!("{:06}", mfa::code_at(seed, mfa::step_of(e.clock.wall())))
}

fn create_req(name: &str, role: Role, dcs: &[&str]) -> CreateOperatorRequest {
    CreateOperatorRequest {
        name: name.into(),
        role: Some(role),
        dcs: dcs.iter().map(|d| DcId::new(*d).unwrap()).collect(),
        password: format!("{name}-password-long"),
        reason: "test".into(),
        ..CreateOperatorRequest::default()
    }
}

/// Creates `name` with the bootstrap principal (or `by`), logs in, enrols:
/// the session token and the seed.
async fn onboard(
    e: &Env,
    by: &Principal,
    name: &str,
    role: Role,
    dcs: &[&str],
) -> (String, Vec<u8>) {
    e.ops.create(by, create_req(name, role, dcs)).await.unwrap();
    let r = e
        .ops
        .login(LoginRequest {
            name: name.into(),
            password: format!("{name}-password-long"),
            ..LoginRequest::default()
        })
        .await
        .unwrap();
    assert!(r.token.is_none(), "a factor is enrolled first");
    let seed = seed_of(r.otpauth_uri.as_deref().unwrap());
    let r = e
        .ops
        .enrol(EnrolRequest {
            name: name.into(),
            ticket: r.enrol_ticket.unwrap(),
            code: code(e, &seed),
            client: Some("test".into()),
        })
        .await
        .unwrap();
    assert_eq!(r.recovery_codes.len(), 10);
    (r.token.unwrap(), seed)
}

fn code_of(e: &SprigError) -> SprigCode {
    match e.kind {
        sprig_common::ErrorKind::Sprig(c) => c,
        _ => panic!("not a SprigDB error: {e}"),
    }
}

#[tokio::test]
async fn bootstrap_until_the_first_login_then_refused() {
    let e = env();
    let g = guard(&e);
    // Before the switch: the bootstrap token, the legacy DC token, the
    // gateway's file identity.
    assert_eq!(
        g.authenticate(&headers("boot-secret")).await.unwrap().kind,
        PrincipalKind::Bootstrap
    );
    let dc = g.authenticate(&headers("dc-secret")).await.unwrap();
    assert_eq!(dc.kind, PrincipalKind::LegacyDcOperator);
    assert_eq!(dc.dcs, vec![DcId::new("dc-a").unwrap()]);
    assert!(
        g.authenticate(&headers("gw-secret"))
            .await
            .unwrap()
            .is_gateway()
    );
    let wrong = g.authenticate(&headers("nope")).await.unwrap_err();
    assert_eq!(code_of(&wrong), SprigCode::AdminUnauthenticated);
    let none = g.authenticate(&HeaderMapT::new()).await.unwrap_err();
    assert_eq!(code_of(&none), SprigCode::AdminUnauthenticated);

    // Creating an operator doesn't switch; their first login does.
    let boot = Principal::bootstrap();
    e.ops
        .create(&boot, create_req("alice", Role::PlatformOperator, &[]))
        .await
        .unwrap();
    assert!(!e.ops.switched().await.unwrap());
    let r = e
        .ops
        .login(LoginRequest {
            name: "alice".into(),
            password: "alice-password-long".into(),
            ..LoginRequest::default()
        })
        .await
        .unwrap();
    assert!(!e.ops.switched().await.unwrap(), "not before the enrolment");
    let seed = seed_of(r.otpauth_uri.as_deref().unwrap());
    let bad = e
        .ops
        .enrol(EnrolRequest {
            name: "alice".into(),
            ticket: "0".repeat(48),
            code: code(&e, &seed),
            client: None,
        })
        .await
        .unwrap_err();
    assert_eq!(code_of(&bad), SprigCode::OperatorLoginFailed);
    let done = e
        .ops
        .enrol(EnrolRequest {
            name: "alice".into(),
            ticket: r.enrol_ticket.unwrap(),
            code: code(&e, &seed),
            client: None,
        })
        .await
        .unwrap();
    assert!(e.ops.switched().await.unwrap());
    let token = done.token.unwrap();
    let p = g.authenticate(&headers(&token)).await.unwrap();
    assert_eq!(p.kind, PrincipalKind::Operator);
    assert_eq!(p.name, "alice");
    assert!(p.platform && p.is_person_session());

    // After the switch: bootstrap and legacy tokens, or none, are 39310;
    // the gateway's identity keeps working.
    for h in [
        headers("boot-secret"),
        headers("dc-secret"),
        HeaderMapT::new(),
    ] {
        let err = g.authenticate(&h).await.unwrap_err();
        assert_eq!(code_of(&err), SprigCode::OperatorLoginRequired);
    }
    assert!(
        g.authenticate(&headers("gw-secret"))
            .await
            .unwrap()
            .is_gateway()
    );
    // Break glass.
    let bg = Guard::new(
        Arc::clone(&e.ops),
        Some("boot-secret".into()),
        None,
        vec![],
        true,
    );
    let p = bg.authenticate(&headers("boot-secret")).await.unwrap();
    assert_eq!(p.kind, PrincipalKind::BreakGlass);
    assert_eq!(p.actor().as_deref(), Some("breakglass"));
    // The bootstrap token may no longer create operators either.
    assert_eq!(
        code_of(&g.authenticate(&headers("boot-secret")).await.unwrap_err()),
        SprigCode::OperatorLoginRequired
    );
}

#[tokio::test]
async fn codes_work_once_recovery_codes_and_lockout() {
    let e = env();
    let (_, seed) = onboard(
        &e,
        &Principal::bootstrap(),
        "bob",
        Role::PlatformOperator,
        &[],
    )
    .await;
    let login = |code: Option<String>, rec: Option<String>, pw: &str| LoginRequest {
        name: "bob".into(),
        password: pw.into(),
        code,
        recovery_code: rec,
        client: None,
    };
    // Without a code: 39311.
    let err = e
        .ops
        .login(login(None, None, "bob-password-long"))
        .await
        .unwrap_err();
    assert_eq!(code_of(&err), SprigCode::OperatorMfaRequired);
    // The enrolment's code (same step) doesn't work again.
    let err = e
        .ops
        .login(login(Some(code(&e, &seed)), None, "bob-password-long"))
        .await
        .unwrap_err();
    assert_eq!(code_of(&err), SprigCode::OperatorLoginFailed);
    e.clock.advance(Duration::from_secs(30));
    let ok = e
        .ops
        .login(login(Some(code(&e, &seed)), None, "bob-password-long"))
        .await
        .unwrap();
    assert!(ok.token.is_some());
    // A recovery code works once.
    e.ops
        .create(
            &Principal::bootstrap(),
            create_req("carol", Role::PlatformOperator, &[]),
        )
        .await
        .unwrap();
    let r = e
        .ops
        .login(LoginRequest {
            name: "carol".into(),
            password: "carol-password-long".into(),
            ..LoginRequest::default()
        })
        .await
        .unwrap();
    let cseed = seed_of(r.otpauth_uri.as_deref().unwrap());
    let done = e
        .ops
        .enrol(EnrolRequest {
            name: "carol".into(),
            ticket: r.enrol_ticket.unwrap(),
            code: code(&e, &cseed),
            client: None,
        })
        .await
        .unwrap();
    let rc = done.recovery_codes[3].clone();
    let lr = |rc: &str| LoginRequest {
        name: "carol".into(),
        password: "carol-password-long".into(),
        recovery_code: Some(rc.to_owned()),
        ..LoginRequest::default()
    };
    assert!(e.ops.login(lr(&rc)).await.unwrap().token.is_some());
    assert!(e.ops.login(lr(&rc)).await.is_err());
    // Five wrong passwords lock the account, even for the right one.
    for _ in 0..5 {
        let err = e.ops.login(login(None, None, "wrong")).await.unwrap_err();
        assert_eq!(code_of(&err), SprigCode::OperatorLoginFailed);
    }
    e.clock.advance(Duration::from_secs(30));
    assert!(
        e.ops
            .login(login(Some(code(&e, &seed)), None, "bob-password-long"))
            .await
            .is_err()
    );
    e.clock.advance(Duration::from_secs(16 * 60));
    assert!(
        e.ops
            .login(login(Some(code(&e, &seed)), None, "bob-password-long"))
            .await
            .unwrap()
            .token
            .is_some()
    );
    // Unknown operators fail the same way.
    let err = e
        .ops
        .login(LoginRequest {
            name: "nobody".into(),
            password: "whatever-long-pw".into(),
            ..LoginRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(code_of(&err), SprigCode::OperatorLoginFailed);
}

#[tokio::test]
async fn tokens_are_issued_from_sessions_scoped_and_revocable() {
    let e = env();
    let g = guard(&e);
    let (session, _) = onboard(
        &e,
        &Principal::bootstrap(),
        "alice",
        Role::PlatformOperator,
        &[],
    )
    .await;
    let me = g.authenticate(&headers(&session)).await.unwrap();
    let t = e
        .ops
        .create_token(
            &me,
            CreateTokenRequest {
                label: "dashboards".into(),
                scopes: vec!["clusters:read".into(), "settings:write".into()],
                expires_in_days: Some(7),
                ..CreateTokenRequest::default()
            },
        )
        .await
        .unwrap();
    assert!(t.token.starts_with("sprigop_"));
    let p = g.authenticate(&headers(&t.token)).await.unwrap();
    assert_eq!(p.token_kind, Some(TokenKind::Token));
    assert_eq!(p.name, "alice");
    let ok = |m: Method, path: &str| g.authorize(&p, &m, path, None, None);
    use axum::http::Method;
    assert!(ok(Method::GET, "/admin/v1/clusters").is_ok());
    assert!(ok(Method::GET, "/admin/v1/clusters/dc-a-c1").is_ok());
    let err = ok(Method::POST, "/admin/v1/clusters/dc-a-c1:drain").unwrap_err();
    assert_eq!(code_of(&err), SprigCode::OperatorForbidden);
    assert!(ok(Method::GET, "/admin/v1/tenants").is_err());
    assert!(ok(Method::PUT, "/admin/v1/settings/x.y").is_ok());
    // A token can't issue tokens or create operators.
    let err = e
        .ops
        .create_token(
            &p,
            CreateTokenRequest {
                scopes: vec!["*:read".into()],
                ..CreateTokenRequest::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(code_of(&err), SprigCode::OperatorForbidden);
    let err = e
        .ops
        .create(&p, create_req("mallory", Role::PlatformOperator, &[]))
        .await
        .unwrap_err();
    assert_eq!(code_of(&err), SprigCode::OperatorForbidden);
    // Bad scopes, no scopes, too long.
    for req in [
        CreateTokenRequest {
            scopes: vec!["clusters".into()],
            ..CreateTokenRequest::default()
        },
        CreateTokenRequest::default(),
        CreateTokenRequest {
            scopes: vec!["*:read".into()],
            expires_in_days: Some(91),
            ..CreateTokenRequest::default()
        },
    ] {
        let err = e.ops.create_token(&me, req).await.unwrap_err();
        assert_eq!(code_of(&err), SprigCode::AdminRequestInvalid, "{err}");
    }
    // Listed and revoked; a revoked token is 39316.
    let mine = e.ops.list_tokens(&me, None).await.unwrap();
    assert_eq!(mine.len(), 2, "the session and the token");
    e.ops.revoke_token(&me, &t.record.key_id).await.unwrap();
    let err = g.authenticate(&headers(&t.token)).await.unwrap_err();
    assert_eq!(code_of(&err), SprigCode::OperatorTokenExpired);
    // An expired one too.
    let t2 = e
        .ops
        .create_token(
            &me,
            CreateTokenRequest {
                scopes: vec!["*:read".into()],
                expires_in_days: Some(1),
                ..CreateTokenRequest::default()
            },
        )
        .await
        .unwrap();
    e.clock.advance(Duration::from_secs(86_401));
    let err = g.authenticate(&headers(&t2.token)).await.unwrap_err();
    assert_eq!(code_of(&err), SprigCode::OperatorTokenExpired);
    // A wrong secret for a real id is plain 39793.
    let forged = format!("{}{}", &t2.token[..t2.token.len() - 1], "0");
    let forged = if forged == t2.token {
        format!("{}{}", &t2.token[..t2.token.len() - 1], "1")
    } else {
        forged
    };
    let err = g.authenticate(&headers(&forged)).await.unwrap_err();
    assert_eq!(code_of(&err), SprigCode::AdminUnauthenticated);
    // Logout ends the session.
    let fresh = e.ops.whoami(&me).await.unwrap();
    assert!(fresh.switched);
    e.ops.logout(&me).await.unwrap();
    let err = g.authenticate(&headers(&session)).await.unwrap_err();
    assert_eq!(code_of(&err), SprigCode::OperatorTokenExpired);
}

#[tokio::test]
async fn dc_operators_stay_in_their_dcs() {
    use axum::http::Method;
    let a = env();
    let (_, _) = onboard(
        &a,
        &Principal::bootstrap(),
        "root",
        Role::PlatformOperator,
        &[],
    )
    .await;
    let root = Principal {
        kind: PrincipalKind::Operator,
        name: "root".into(),
        platform: true,
        token_kind: Some(TokenKind::Session),
        ..Principal::bootstrap()
    };
    let (session, _) = onboard(&a, &root, "dana", Role::DcOperator, &["dc-b"]).await;
    // The same directory seen from dc-a's and dc-b's controllers.
    let b = env_in("dc-b", Arc::clone(&a.global), Arc::clone(&a.clock));
    let ga = guard(&a);
    let gb = guard(&b);
    let pa = ga.authenticate(&headers(&session)).await.unwrap();
    assert!(!pa.platform);
    assert_eq!(pa.dcs, vec![DcId::new("dc-b").unwrap()]);
    // On dc-a: nothing but self-service.
    let err = ga
        .authorize(&pa, &Method::GET, "/admin/v1/clusters", None, None)
        .unwrap_err();
    assert_eq!(code_of(&err), SprigCode::OperatorForbidden);
    assert!(err.message.contains("dc-a"), "{err}");
    assert!(
        ga.authorize(&pa, &Method::GET, "/admin/v1/operators:whoami", None, None)
            .is_ok()
    );
    // On dc-b: reads, cluster work, its own drain, dc and cluster settings.
    let pb = gb.authenticate(&headers(&session)).await.unwrap();
    let ok = |m: Method, path: &str, body: Option<Value>| {
        gb.authorize(&pb, &m, path, None, body.as_ref())
    };
    assert!(ok(Method::GET, "/admin/v1/tenants", None).is_ok());
    assert!(ok(Method::POST, "/admin/v1/clusters/dc-b-c1:drain", None).is_ok());
    assert!(ok(Method::POST, "/admin/v1/dcs/dc-b:drain", None).is_ok());
    assert!(ok(Method::POST, "/admin/v1/dcs/dc-a:drain", None).is_err());
    assert!(ok(Method::POST, "/admin/v1/tenants", None).is_err());
    assert!(ok(Method::POST, "/admin/v1/tenants/acme:move", None).is_err());
    assert!(
        ok(
            Method::PUT,
            "/admin/v1/settings/k",
            Some(json!({"scope": "dc"}))
        )
        .is_ok()
    );
    assert!(
        ok(
            Method::PUT,
            "/admin/v1/settings/k",
            Some(json!({"scope": "cluster:dc-b-c1"}))
        )
        .is_ok()
    );
    assert!(
        ok(
            Method::PUT,
            "/admin/v1/settings/k",
            Some(json!({"scope": "global"}))
        )
        .is_err()
    );
    assert!(
        ok(
            Method::PUT,
            "/admin/v1/settings/k",
            Some(json!({"scope": "dc:dc-a"}))
        )
        .is_err()
    );
    assert!(
        gb.authorize(
            &pb,
            &Method::DELETE,
            "/admin/v1/settings/k",
            Some("scope=dc%3Adc-b"),
            None
        )
        .is_ok()
    );
    // A DC operator can't manage operators or issue service identities.
    let err = b
        .ops
        .create(&pb, create_req("eve", Role::PlatformOperator, &[]))
        .await
        .unwrap_err();
    assert_eq!(code_of(&err), SprigCode::OperatorForbidden);
    let err = b
        .ops
        .create_token(
            &pb,
            CreateTokenRequest {
                service: Some("ci".into()),
                scopes: vec!["*:read".into()],
                ..CreateTokenRequest::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(code_of(&err), SprigCode::OperatorForbidden);
    // Its own token can't name a DC it doesn't operate.
    let err = b
        .ops
        .create_token(
            &pb,
            CreateTokenRequest {
                scopes: vec!["*:read".into()],
                dcs: vec![DcId::new("dc-a").unwrap()],
                ..CreateTokenRequest::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(code_of(&err), SprigCode::OperatorForbidden);
    // A platform operator's token narrowed to dc-b acts like a DC operator.
    let t = a
        .ops
        .create_token(
            &root,
            CreateTokenRequest {
                scopes: vec!["*:write".into()],
                dcs: vec![DcId::new("dc-b").unwrap()],
                ..CreateTokenRequest::default()
            },
        )
        .await
        .unwrap();
    let pt = ga.authenticate(&headers(&t.token)).await.unwrap();
    assert!(!pt.platform);
    assert!(
        ga.authorize(&pt, &Method::GET, "/admin/v1/clusters", None, None)
            .is_err()
    );
    let pt = gb.authenticate(&headers(&t.token)).await.unwrap();
    assert!(
        gb.authorize(
            &pt,
            &Method::POST,
            "/admin/v1/clusters/dc-b-c1:drain",
            None,
            None
        )
        .is_ok()
    );
    assert!(
        gb.authorize(&pt, &Method::POST, "/admin/v1/tenants", None, None)
            .is_err()
    );
}

#[tokio::test]
async fn disabling_revokes_and_keeps_one_platform_operator() {
    let e = env();
    let g = guard(&e);
    let (s1, _) = onboard(
        &e,
        &Principal::bootstrap(),
        "alice",
        Role::PlatformOperator,
        &[],
    )
    .await;
    let alice = g.authenticate(&headers(&s1)).await.unwrap();
    let err = e
        .ops
        .set_state(&alice, "alice", true, "leaving")
        .await
        .unwrap_err();
    assert_eq!(code_of(&err), SprigCode::AdminRequestInvalid);
    let (s2, _) = onboard(&e, &alice, "bob", Role::PlatformOperator, &[]).await;
    let bob = g.authenticate(&headers(&s2)).await.unwrap();
    // A service identity bob issues outlives bob's account.
    let svc = e
        .ops
        .create_token(
            &bob,
            CreateTokenRequest {
                service: Some("gw-fra1".into()),
                service_kind: Some(ServiceKind::Gateway),
                never_expires: true,
                ..CreateTokenRequest::default()
            },
        )
        .await
        .unwrap();
    assert!(svc.token.starts_with("sprigsvc_"));
    let sp = g.authenticate(&headers(&svc.token)).await.unwrap();
    assert!(sp.is_gateway());
    assert_eq!(sp.audit_name(), "svc:gw-fra1");
    assert_eq!(sp.owner.as_deref(), Some("bob"));
    let v = e.ops.set_state(&alice, "bob", true, "left").await.unwrap();
    assert_eq!(v.state, OperatorState::Disabled);
    let err = g.authenticate(&headers(&s2)).await.unwrap_err();
    assert_eq!(code_of(&err), SprigCode::OperatorTokenExpired);
    assert!(g.authenticate(&headers(&svc.token)).await.is_ok());
    let err = e
        .ops
        .login(LoginRequest {
            name: "bob".into(),
            password: "bob-password-long".into(),
            ..LoginRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(code_of(&err), SprigCode::OperatorLoginFailed);
    // Factor reset: the next login enrols again.
    e.ops.set_state(&alice, "bob", false, "back").await.unwrap();
    e.ops
        .reset_factor(&alice, "bob", "new phone")
        .await
        .unwrap();
    let r = e
        .ops
        .login(LoginRequest {
            name: "bob".into(),
            password: "bob-password-long".into(),
            ..LoginRequest::default()
        })
        .await
        .unwrap();
    assert!(r.enrol_ticket.is_some());
    // Own password change needs the current one.
    let err = e
        .ops
        .set_password(
            &alice,
            "alice",
            SetPasswordRequest {
                current: Some("wrong".into()),
                password: "another-long-password".into(),
                ..SetPasswordRequest::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(code_of(&err), SprigCode::OperatorLoginFailed);
    e.ops
        .set_password(
            &alice,
            "alice",
            SetPasswordRequest {
                current: Some("alice-password-long".into()),
                password: "another-long-password".into(),
                ..SetPasswordRequest::default()
            },
        )
        .await
        .unwrap();
    // The session used survives its own password change.
    assert!(g.authenticate(&headers(&s1)).await.is_ok());
}

async fn echo(headers: HeaderMapT, body: axum::body::Bytes) -> axum::response::Response {
    crate::admin::http::respond(Ok(json!({
        "authn": headers.get(AUTHN_HEADER).and_then(|v| v.to_str().ok()),
        "body": serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null),
    })))
}

#[tokio::test]
async fn the_layer_names_the_actor_and_drops_forged_callers() {
    let e = env();
    let (session, _) = onboard(
        &e,
        &Principal::bootstrap(),
        "alice",
        Role::PlatformOperator,
        &[],
    )
    .await;
    let g = Arc::new(guard(&e));
    let app = Router::new()
        .route("/admin/v1/settings/{k}", axum::routing::put(echo))
        .route("/admin/v1/clusters", get(echo))
        .route("/admin/v1/traffic/reports", post(echo))
        .merge(http::router(Arc::clone(&e.ops)))
        .layer(axum::middleware::from_fn_with_state(g, guard::layer));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(l, app).await });
    let call = |m: &str, path: &str, token: Option<&str>, body: Value| {
        let auth = token.map_or(String::new(), |t| format!("authorization: Bearer {t}\r\n"));
        let body = if body.is_null() {
            String::new()
        } else {
            body.to_string()
        };
        let req = format!(
            "{m} {path} HTTP/1.1\r\nhost: x\r\ncontent-type: application/json\r\n\
             {AUTHN_HEADER}: {{\"kind\":\"bootstrap\",\"name\":\"forged\",\"platform\":true}}\r\n\
             {auth}content-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        async move {
            let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
            s.write_all(req.as_bytes()).await.unwrap();
            let mut out = String::new();
            s.read_to_string(&mut out).await.unwrap();
            let status: u16 = out[9..12].parse().unwrap();
            let body = out.split_once("\r\n\r\n").unwrap().1;
            (
                status,
                serde_json::from_str::<Value>(body).unwrap_or(Value::Null),
            )
        }
    };
    // The forged header is dropped: no credential after the switch.
    let (s, v) = call("GET", "/admin/v1/clusters", None, Value::Null).await;
    assert_eq!(s, 401);
    assert_eq!(v["code"], 39310);
    // A person's change records their name, whatever the body says.
    let (s, v) = call(
        "PUT",
        "/admin/v1/settings/x.y",
        Some(&session),
        json!({"scope": "global", "value_text": "1", "actor": "someone-else"}),
    )
    .await;
    assert_eq!(s, 200, "{v}");
    assert_eq!(v["body"]["actor"], "alice");
    let authn: Principal = serde_json::from_str(v["authn"].as_str().unwrap()).unwrap();
    assert_eq!(authn.name, "alice");
    // Login over HTTP needs no credential; whoami does.
    let (s, v) = call(
        "POST",
        "/admin/v1/operators:login",
        None,
        json!({"name": "alice", "password": "alice-password-long"}),
    )
    .await;
    assert_eq!(s, 401);
    assert_eq!(v["code"], 39311);
    let (s, v) = call(
        "GET",
        "/admin/v1/operators:whoami",
        Some(&session),
        Value::Null,
    )
    .await;
    assert_eq!(s, 200, "{v}");
    assert_eq!(v["principal"]["name"], "alice");
    assert_eq!(v["switched"], true);
    // Traffic reports stay open, as before.
    let (s, _) = call("POST", "/admin/v1/traffic/reports", None, json!({})).await;
    assert_eq!(s, 200);
    // The operators list over HTTP.
    let (s, v) = call("GET", "/admin/v1/operators", Some(&session), Value::Null).await;
    assert_eq!(s, 200, "{v}");
    assert_eq!(v[0]["name"], "alice");
    assert_eq!(v[0]["totp"], true);
    assert!(v[0].get("verifier").is_none());
    server.abort();
}

#[test]
fn a_forwarded_address_counts_only_with_a_service_identity() {
    use super::guard::{CLIENT_ADDR_HEADER, ClientAddr, RELAY_AUTH_HEADER};
    let e = env();
    let g = guard(&e);
    let peer: std::net::SocketAddr = "10.0.0.5:41000".parse().unwrap();
    let mut h = axum::http::HeaderMap::new();
    h.insert(CLIENT_ADDR_HEADER, "203.0.113.7".parse().unwrap());
    // No relay credential: the header is ignored, the peer counts.
    assert_eq!(
        g.client_addr(&h, Some(peer)),
        ClientAddr {
            addr: Some("10.0.0.5".into()),
            via: None,
            peer: Some("10.0.0.5".into()),
        }
    );
    // A wrong one, or another credential (the bootstrap token), the same.
    for wrong in ["nope", "boot-secret", "dc-secret", ""] {
        let mut h2 = h.clone();
        h2.insert(RELAY_AUTH_HEADER, wrong.parse().unwrap());
        assert_eq!(g.client_addr(&h2, Some(peer)).addr.as_deref(), Some("10.0.0.5"), "{wrong}");
    }
    // The gateway's service secret: the forwarded address, via the gateway.
    h.insert(RELAY_AUTH_HEADER, "gw-secret".parse().unwrap());
    assert_eq!(
        g.client_addr(&h, Some(peer)),
        ClientAddr {
            addr: Some("203.0.113.7".into()),
            via: Some("gateway".into()),
            peer: Some("10.0.0.5".into()),
        }
    );
    // An IPv6 address is fine; something that isn't an address is not.
    h.insert(CLIENT_ADDR_HEADER, "2001:db8::1".parse().unwrap());
    assert_eq!(g.client_addr(&h, Some(peer)).addr.as_deref(), Some("2001:db8::1"));
    h.insert(CLIENT_ADDR_HEADER, "evil, 1.2.3.4".parse().unwrap());
    assert_eq!(g.client_addr(&h, Some(peer)).addr.as_deref(), Some("10.0.0.5"));
    assert_eq!(g.client_addr(&h, Some(peer)).via, None);
    // No peer known and nothing vouched: no address.
    assert_eq!(g.client_addr(&axum::http::HeaderMap::new(), None), ClientAddr::default());
}

async fn echo_client(headers: HeaderMapT) -> axum::response::Response {
    use super::guard::{CLIENT_ADDR_HEADER, CLIENT_HEADER, RELAY_AUTH_HEADER};
    let h = |n: &str| headers.get(n).and_then(|v| v.to_str().ok()).map(str::to_owned);
    crate::admin::http::respond(Ok(json!({
        "client": h(CLIENT_HEADER),
        "forwarded": h(CLIENT_ADDR_HEADER),
        "relay": h(RELAY_AUTH_HEADER),
    })))
}

#[tokio::test]
async fn the_layer_hands_routes_the_client_address() {
    use super::guard::{CLIENT_ADDR_HEADER, CLIENT_HEADER, RELAY_AUTH_HEADER};
    let e = env();
    let g = Arc::new(guard(&e));
    let app = Router::new()
        .route("/admin/v1/traffic/reports", post(echo_client))
        .route("/admin/v1/clusters", get(echo_client))
        .layer(axum::middleware::from_fn_with_state(g, guard::layer));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let stop = sprig_common::CancellationToken::new();
    let server = tokio::spawn(crate::admin::http::serve(l, app, stop.clone()));
    let call = |m: &str, path: &str, extra: String| {
        let req = format!(
            "{m} {path} HTTP/1.1\r\nhost: x\r\ncontent-type: application/json\r\n{extra}\
             content-length: 2\r\nconnection: close\r\n\r\n{{}}"
        );
        async move {
            let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
            s.write_all(req.as_bytes()).await.unwrap();
            let mut out = String::new();
            s.read_to_string(&mut out).await.unwrap();
            let status: u16 = out[9..12].parse().unwrap();
            let body = out.split_once("\r\n\r\n").unwrap().1;
            (status, serde_json::from_str::<Value>(body).unwrap_or(Value::Null))
        }
    };
    // A client's own copies of the headers are dropped; the peer counts.
    let forged = format!(
        "{CLIENT_ADDR_HEADER}: 203.0.113.7\r\n{CLIENT_HEADER}: 198.51.100.1\r\n\
         {RELAY_AUTH_HEADER}: guess\r\n"
    );
    let (s, v) = call("POST", "/admin/v1/traffic/reports", forged).await;
    assert_eq!(s, 200, "{v}");
    assert_eq!(v["client"], "127.0.0.1");
    assert_eq!(v["forwarded"], Value::Null);
    assert_eq!(v["relay"], Value::Null);
    // A gateway vouching with its service secret: the person's address,
    // and the secret goes no further.
    let relayed = format!("{CLIENT_ADDR_HEADER}: 203.0.113.7\r\n{RELAY_AUTH_HEADER}: gw-secret\r\n");
    let (s, v) = call("POST", "/admin/v1/traffic/reports", relayed.clone()).await;
    assert_eq!(s, 200, "{v}");
    assert_eq!(v["client"], "203.0.113.7");
    assert_eq!(v["relay"], Value::Null);
    // Authenticated routes too (the gateway's own bearer).
    let (s, v) = call(
        "GET",
        "/admin/v1/clusters",
        format!("{relayed}authorization: Bearer boot-secret\r\n"),
    )
    .await;
    assert_eq!(s, 200, "{v}");
    assert_eq!(v["client"], "203.0.113.7");
    stop.cancel();
    let _ = server.await;
}
