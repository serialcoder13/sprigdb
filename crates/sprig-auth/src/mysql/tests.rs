//! The login flow end to end: the wire protocol's real `authenticate()`
//! over a duplex stream with a scripted client, against [`LoginService`] with a
//! catalog source (static tenants, prebuilt tenant snapshots).

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use bytes::Bytes;
use sprig_catalog::cache::Loaded;
use sprig_catalog::{ExtState, Snapshot, Versions};
use sprig_common::time::ManualClock;
use sprig_proto_mysql::auth::{AuthIo, authenticate};
use sprig_proto_mysql::handshake::{PacketIo, RawIo};
use sprig_settings::{AnySetting, Registry};

use super::*;
use crate::account::StaticTenants;
use crate::cache::tests::{at, row, sha2};
use crate::store::{Slot, UserRow, VerifierKind, VerifierRow};
use crate::verifier::{caching_sha2_scramble, native_scramble};

const NONCE: [u8; NONCE_LEN] = [b'n'; NONCE_LEN];
pub(crate) const ACME: TenantId = TenantId(42);
const SUSP: TenantId = TenantId(43);

fn registry() -> &'static Registry {
    static R: OnceLock<Registry> = OnceLock::new();
    R.get_or_init(|| {
        Registry::new([
            &crate::settings::REQUIRE_TLS as &dyn AnySetting,
            &crate::settings::DEFAULT_PASSWORD_LIFETIME_DAYS,
            &crate::settings::MYSQL_NATIVE_PASSWORD,
            &crate::settings::CACHING_SHA2_ROUNDS,
        ])
        .unwrap()
    })
}

pub(crate) fn snapshot(
    tenant: TenantId,
    auth: TenantAuth,
    auth_version: u64,
) -> Arc<TenantCatalog> {
    let loaded = Loaded {
        snapshot: Snapshot {
            versions: Versions {
                catalog: 1,
                auth: auth_version,
                purged: 0,
            },
            since: None,
            databases: Vec::new(),
            objects: Vec::new(),
            settings: Vec::new(),
        },
        ext: vec![("auth", Arc::new(auth) as Arc<dyn ExtState>)],
    };
    Arc::new(TenantCatalog::build(registry(), tenant, loaded))
}

#[derive(Default)]
pub(crate) struct Catalogs {
    pub(crate) map: Mutex<HashMap<TenantId, Arc<TenantCatalog>>>,
    pub(crate) fail: AtomicBool,
}

#[async_trait]
impl AuthCatalogs for Catalogs {
    async fn catalog(&self, tenant: TenantId) -> Result<Arc<TenantCatalog>> {
        if self.fail.load(Ordering::Relaxed) {
            return Err(SprigError::new(SprigCode::CatalogUnavailable, "down"));
        }
        self.map
            .lock()
            .unwrap()
            .get(&tenant)
            .cloned()
            .ok_or_else(|| SprigError::new(SprigCode::TenantNotReady, "no catalog"))
    }
}

/// A verifier at the default cost, so timings compare with the decoy.
fn sha2_full(vid: u64, user: u32, slot: Slot, pw: &[u8]) -> VerifierRow {
    let mut r = VerifierRow::mysql(
        UserId(user),
        VerifierKind::CachingSha2,
        slot,
        crate::verifier::CachingSha2Verifier::create(pw, 5000)
            .unwrap()
            .to_bytes(),
        1,
        at(),
    );
    r.verifier_id = vid;
    r
}

fn native(vid: u64, user: u32, pw: &[u8]) -> VerifierRow {
    let mut r = VerifierRow::mysql(
        UserId(user),
        VerifierKind::Native,
        Slot::Primary,
        crate::verifier::NativeVerifier::create(pw)
            .to_string()
            .into_bytes(),
        1,
        at(),
    );
    r.verifier_id = vid;
    r
}

fn backend() -> BackendTarget {
    BackendTarget {
        cluster: ClusterId::new("dc-a-c1").unwrap(),
        user: "sprig_t42".into(),
        credentials: "env:SPRIG_T42".parse().unwrap(),
    }
}

pub(crate) struct World {
    pub(crate) svc: Arc<LoginService>,
    pub(crate) catalogs: Arc<Catalogs>,
    pub(crate) clock: Arc<ManualClock>,
    pub(crate) state: Arc<crate::policy::MemLoginState>,
    pub(crate) audit: Arc<crate::audit::MemoryAudit>,
    /// The directory, for residency.
    pub(crate) tenants: Arc<StaticTenants>,
}

/// Tenant `acme` (42) with:
/// 1 alice@% "pw" (+ secondary "old-pw", history "ancient")
/// 2 locked@% "pw", account locked
/// 3 tls@% "pw", REQUIRE SSL
/// 4 x509@% "pw", REQUIRE X509
/// 5 expired@% "pw", PASSWORD EXPIRE
/// 6 aged@% "pw", lifetime 1 day, changed 2 days ago
/// 7 blocked@% "pw", temporarily locked for an hour
/// 8 alice@10.% "office"
/// 9 nat@% native only "npw"
/// 10 svc@% "pw", `api_key` only
/// 11 dual@% "pw", `caching_sha2_password` and native verifiers
/// 12 subj@% "pw", REQUIRE SUBJECT '/CN=alice' AND ISSUER '/CN=ca'
/// and tenant `susp` (43, suspended) with 1 carol@% "pw".
pub(crate) fn world(policy: FixedPolicy) -> World {
    world_at(policy, None)
}

/// [`world`] for a gateway handing logins off through `handoff`.
pub(crate) fn world_at(
    mut policy: FixedPolicy,
    handoff: Option<Arc<crate::handoff::HandoffConfig>>,
) -> World {
    // Rate limits off unless a test asks: they'd slow the timing tests.
    for key in [
        crate::settings::DELAY_AFTER_FAILURES.key,
        crate::settings::IP_FAILURES_PER_MINUTE.key,
    ] {
        policy.0.entry(key).or_insert(Value::Int(0));
    }
    let clock = Arc::new(ManualClock::new());
    let now = clock.wall();
    let mut users: Vec<UserRow> = Vec::new();
    let mut vs = Vec::new();
    let mut add = |id: u32, name: &str, host: &str, pw: &[u8], f: &dyn Fn(&mut UserRow)| {
        let mut r = row(id, name, host, 1);
        r.password_changed_at = Some(now);
        f(&mut r);
        users.push(r);
        if !pw.is_empty() {
            vs.push(sha2_full(u64::from(id) * 10, id, Slot::Primary, pw));
        }
    };
    add(1, "alice", "%", b"pw", &|_| {});
    add(2, "locked", "%", b"pw", &|r| r.account_locked = true);
    add(3, "tls", "%", b"pw", &|r| r.require_tls = RequireTls::Ssl);
    add(4, "x509", "%", b"pw", &|r| r.require_tls = RequireTls::X509);
    add(5, "expired", "%", b"pw", &|r| r.password_expired = true);
    add(6, "aged", "%", b"pw", &|r| {
        r.password_lifetime = Some(1);
        r.password_changed_at = Some(now - Duration::from_secs(2 * 86_400));
    });
    add(7, "blocked", "%", b"pw", &|_| {});
    add(8, "alice", "10.%", b"office", &|_| {});
    add(9, "nat", "%", b"", &|r| {
        r.plugin = "mysql_native_password".into();
    });
    add(10, "svc", "%", b"pw", &|r| {
        r.auth_methods = AuthMethods::API_KEY;
    });
    add(11, "dual", "%", b"pw", &|_| {});
    add(12, "subj", "%", b"pw", &|r| {
        r.require_tls = RequireTls::X509;
        r.require_subject = Some("/CN=alice".into());
        r.require_issuer = Some("/CN=ca".into());
    });
    vs.push(sha2(11, 1, Slot::Secondary, b"old-pw"));
    vs.push(sha2(12, 1, Slot::History, b"ancient"));
    vs.push(native(91, 9, b"npw"));
    vs.push(native(111, 11, b"pw"));
    // dual@% also has both SCRAM verifiers of "pw".
    for (vid, kind) in [(112, ScramKind::Sha256), (113, ScramKind::Sha512)] {
        let mut r = ScramVerifier::create(kind, b"pw", None, 5000)
            .unwrap()
            .unwrap()
            .to_row(UserId(11), Slot::Primary, 1, at());
        r.verifier_id = vid;
        vs.push(r);
    }
    let lock = TempLock {
        user_id: UserId(7),
        locked_until: now + Duration::from_secs(3600),
        consecutive_failures: 3,
    };
    let acme = TenantAuth::build(users, &vs, vec![lock]);
    let susp = TenantAuth::build(
        vec![row(1, "carol", "%", 1)],
        &[sha2_full(1, 1, Slot::Primary, b"pw")],
        Vec::new(),
    );
    let catalogs = Arc::new(Catalogs::default());
    catalogs
        .map
        .lock()
        .unwrap()
        .insert(ACME, snapshot(ACME, acme, 7));
    catalogs
        .map
        .lock()
        .unwrap()
        .insert(SUSP, snapshot(SUSP, susp, 1));
    let tenants = Arc::new(StaticTenants::new([
        TenantInfo {
            id: ACME,
            name: "acme".into(),
            can_log_in: true,
            suspended: false,
        },
        TenantInfo {
            id: SUSP,
            name: "susp".into(),
            can_log_in: true,
            suspended: true,
        },
        TenantInfo {
            id: TenantId(44),
            name: "newco".into(),
            can_log_in: false,
            suspended: false,
        },
    ]));
    let logins = CatalogLogins::new(
        Arc::clone(&tenants) as Arc<dyn crate::account::TenantDirectory>,
        catalogs.clone(),
        Arc::new(StaticBackends::new(Some(backend()))),
        Arc::new(policy),
    );
    logins.set_sni_suffixes(vec!["db.test".into()]);
    let svc = LoginService::new(
        LoginSource::Catalog(Arc::new(logins)),
        Arc::new(FastCache::new(
            clock.clone(),
            crate::fast_cache::DEFAULT_TTL,
        )),
    )
    .with_clock(clock.clone());
    let state = crate::policy::MemLoginState::new();
    let audit = crate::audit::MemoryAudit::new();
    let mut svc = svc
        .with_login_state(state.clone())
        .with_audit(audit.clone());
    if let Some(h) = handoff {
        svc = svc.with_handoff(h);
    }
    World {
        svc: Arc::new(svc),
        catalogs,
        clock,
        state,
        audit,
        tenants,
    }
}

pub(crate) fn req(login: &str) -> LoginRequest {
    LoginRequest {
        persona: Persona::MySql84,
        login_name: login.as_bytes().to_vec(),
        sni: None,
        client_ip: "192.0.2.10".parse().unwrap(),
        tls: true,
        client_cert: false,
        client_dn: None,
        can_handle_expired_password: false,
    }
}

pub(crate) struct Run {
    pub(crate) verdict: AuthVerdict,
    pub(crate) login: MysqlLogin,
    /// Packets the client received.
    pub(crate) seen: Vec<Vec<u8>>,
    pub(crate) took: Duration,
}

/// One whole exchange: the client starts with `plugin`'s scramble of
/// `password` and answers whatever the server asks.
pub(crate) async fn run_with(
    svc: &Arc<LoginService>,
    r: LoginRequest,
    plugin: &str,
    password: &[u8],
) -> Run {
    let first = if plugin == Method::Native.plugin_name() {
        native_scramble(password, &NONCE)
    } else {
        caching_sha2_scramble(password, &NONCE).to_vec()
    };
    run_first(svc, r, plugin, first, password).await
}

/// [`run_with`] with the client's first response given: libmysqlclient's
/// placeholder (empty, under a plugin other than the greeting's) or its
/// single NUL for the empty password.
pub(crate) async fn run_first(
    svc: &Arc<LoginService>,
    r: LoginRequest,
    plugin: &str,
    first: Vec<u8>,
    password: &[u8],
) -> Run {
    let t0 = Instant::now();
    let tls = r.tls;
    let persona = r.persona;
    let (mut c, s) = tokio::io::duplex(1 << 16);
    let plugin = plugin.to_owned();
    let svc = Arc::clone(svc);
    let server = async move {
        let mut s = s;
        let mut login = svc.start(r).await;
        let mut sio = RawIo::new(&mut s, 2);
        let mut io = AuthIo {
            io: &mut sio,
            persona,
            tls,
            nonce: NONCE,
        };
        let v = authenticate(
            &mut io,
            Some(&plugin),
            Bytes::from(first),
            false,
            &mut login,
        )
        .await
        .unwrap();
        drop(s);
        (v, login)
    };
    let pw = password.to_vec();
    let client = async move {
        let mut io = RawIo::new(&mut c, 2);
        let mut seen = Vec::new();
        while let Ok(p) = io.read_packet().await {
            seen.push(p.to_vec());
            match p.as_ref() {
                [0x01, 0x04] if tls => {
                    let mut m = pw.clone();
                    m.push(0);
                    io.write_packet(Bytes::from(m)).await.unwrap();
                }
                [0x01, 0x04] => {
                    // No TLS: ask for the public key, as clients do.
                    io.write_packet(Bytes::from_static(&[0x02])).await.unwrap();
                }
                [0xFE, rest @ ..] => {
                    let nul = rest.iter().position(|&b| b == 0).unwrap();
                    let name = std::str::from_utf8(&rest[..nul]).unwrap();
                    let data = &rest[nul + 1..];
                    let nonce = data.strip_suffix(&[0]).unwrap_or(data);
                    let reply = match name {
                        "mysql_native_password" => native_scramble(&pw, nonce),
                        "mysql_clear_password" => {
                            let mut m = pw.clone();
                            m.push(0);
                            m
                        }
                        _ => caching_sha2_scramble(&pw, nonce).to_vec(),
                    };
                    io.write_packet(Bytes::from(reply)).await.unwrap();
                }
                _ => {}
            }
        }
        seen
    };
    let ((verdict, login), seen) = tokio::join!(server, client);
    Run {
        verdict,
        login,
        seen,
        took: t0.elapsed(),
    }
}

pub(crate) async fn run(svc: &Arc<LoginService>, r: LoginRequest, password: &[u8]) -> Run {
    run_with(svc, r, "caching_sha2_password", password).await
}

pub(crate) fn err(v: &AuthVerdict) -> &ErrPacket {
    match v {
        AuthVerdict::Refuse(e) => e,
        AuthVerdict::Accept => panic!("accepted"),
    }
}

pub(crate) const FULL: &[u8] = &[0x01, 0x04];
pub(crate) const FAST: &[u8] = &[0x01, 0x03];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_catalog_login_then_the_fast_path() {
    let w = world(FixedPolicy::default());
    let r = run(&w.svc, req("acme.alice"), b"pw").await;
    assert_eq!(r.verdict, AuthVerdict::Accept);
    assert_eq!(r.seen, [FULL]);
    let p = r.login.principal().unwrap();
    assert_eq!(p.tenant, Some(ACME));
    assert_eq!(p.tenant_name.as_deref(), Some("acme"));
    assert_eq!(
        (p.user_id, p.user.as_str(), p.host.as_str()),
        (Some(UserId(1)), "alice", "%")
    );
    assert_eq!(p.backend, backend());
    assert_eq!(p.auth_version, 7);
    assert!(!p.must_change_password);
    // The full login filled the fast cache: the next one takes the fast
    // path, even without TLS.
    let r = run(
        &w.svc,
        LoginRequest {
            tls: false,
            ..req("acme.alice")
        },
        b"pw",
    )
    .await;
    assert_eq!(r.verdict, AuthVerdict::Accept);
    assert_eq!(r.seen, [FAST]);
    // A wrong scramble misses the fast path and fails the full one.
    let r = run(&w.svc, req("acme.alice"), b"nope").await;
    assert_eq!(r.seen, [FULL]);
    assert_eq!(err(&r.verdict).code, 1045);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dual_passwords_and_history() {
    let w = world(FixedPolicy::default());
    assert_eq!(
        run(&w.svc, req("acme.alice"), b"old-pw").await.verdict,
        AuthVerdict::Accept
    );
    assert_eq!(
        err(&run(&w.svc, req("acme.alice"), b"ancient").await.verdict).code,
        1045
    );
    // The secondary has its own fast-cache entry.
    let r = run(&w.svc, req("acme.alice"), b"old-pw").await;
    assert_eq!(
        (r.verdict, r.seen),
        (AuthVerdict::Accept, vec![FAST.to_vec()])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tenant_endpoints_and_host_matching() {
    let w = world(FixedPolicy::default());
    // SNI names the tenant; the login name is the user, whole.
    let r = run(
        &w.svc,
        LoginRequest {
            sni: Some("acme.db.test".into()),
            ..req("alice")
        },
        b"pw",
    )
    .await;
    assert_eq!(r.verdict, AuthVerdict::Accept);
    assert_eq!(r.login.principal().unwrap().tenant, Some(ACME));
    // From 10.x, 'alice'@'10.%' is the account, with its own password.
    let office = || LoginRequest {
        client_ip: "10.1.2.3".parse().unwrap(),
        ..req("acme.alice")
    };
    assert_eq!(err(&run(&w.svc, office(), b"pw").await.verdict).code, 1045);
    let r = run(&w.svc, office(), b"office").await;
    assert_eq!(r.login.principal().unwrap().host, "10.%");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_tenants_users_and_wrong_passwords_look_alike() {
    let w = world(FixedPolicy::default());
    let cases = [
        ("nobody.alice", "wrong tenant"),
        ("newco.alice", "tenant not yet active"),
        ("acme.bob", "wrong user"),
        ("acme.alice", "wrong password"),
        ("acme.locked", "locked account, wrong password"),
        ("noname", "no tenant at all"),
    ];
    // Warm up (thread pool, lazies).
    let _ = run(&w.svc, req("acme.alice"), b"warm").await;
    let mut took = Vec::new();
    for (login, what) in cases {
        let mut times = Vec::new();
        for _ in 0..3 {
            let r = run(&w.svc, req(login), b"wrong-password").await;
            // Same packets: the full-authentication shape.
            assert_eq!(r.seen, [FULL], "{what}");
            let e = err(&r.verdict);
            assert_eq!(e.code, 1045, "{what}");
            assert_eq!(&e.sqlstate, b"28000", "{what}");
            assert_eq!(
                e.message,
                format!("Access denied for user '{login}'@'192.0.2.10' (using password: YES)"),
                "{what}"
            );
            times.push(r.took);
        }
        times.sort();
        took.push((times[1], what));
    }
    // Same cost class: every case does one SHA-256 crypt at the default
    // cost (the decoy for unknown accounts). Generous bounds: a timing
    // test must not flake, but a skipped hash is 100x faster.
    let max = took.iter().map(|t| t.0).max().unwrap();
    let min = took.iter().map(|t| t.0).min().unwrap();
    eprintln!("login refusal times: {took:?}");
    assert!(min * 4 >= max, "refusals differ in cost: {took:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn statuses_in_mysql_order() {
    let w = world(FixedPolicy::default());
    // c. ACCOUNT LOCK with the right password: 3118 (4151 on MariaDB).
    let e = err(&run(&w.svc, req("acme.locked"), b"pw").await.verdict).clone();
    assert_eq!(
        (e.code, e.message.as_str()),
        (
            3118,
            "Access denied for user 'locked'@'%'. Account is locked."
        )
    );
    let maria = LoginRequest {
        persona: Persona::MariaDb,
        ..req("acme.locked")
    };
    assert_eq!(err(&run(&w.svc, maria, b"pw").await.verdict).code, 4151);
    // a. A temporary lock: 3955 even with the right password, and with a
    // wrong one; MariaDB's 4150. Once it passes, the login goes through.
    let e = err(&run(&w.svc, req("acme.blocked"), b"pw").await.verdict).clone();
    assert_eq!(e.code, 3955);
    assert!(
        e.message.contains("due to 3 consecutive failed logins"),
        "{}",
        e.message
    );
    assert_eq!(
        err(&run(&w.svc, req("acme.blocked"), b"bad").await.verdict).code,
        3955
    );
    let maria = LoginRequest {
        persona: Persona::MariaDb,
        ..req("acme.blocked")
    };
    assert_eq!(err(&run(&w.svc, maria, b"pw").await.verdict).code, 4150);
    w.clock.advance(Duration::from_secs(3601));
    assert_eq!(
        run(&w.svc, req("acme.blocked"), b"pw").await.verdict,
        AuthVerdict::Accept
    );
    // A service account without the password method: 1045.
    assert_eq!(
        err(&run(&w.svc, req("acme.svc"), b"pw").await.verdict).code,
        1045
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_requirements() {
    let w = world(FixedPolicy::default());
    // Fill the fast cache over TLS, then try without TLS: REQUIRE SSL
    // refuses with 1045 after a verified credential.
    assert_eq!(
        run(&w.svc, req("acme.tls"), b"pw").await.verdict,
        AuthVerdict::Accept
    );
    let plain = LoginRequest {
        tls: false,
        ..req("acme.tls")
    };
    let r = run(&w.svc, plain, b"pw").await;
    assert_eq!(r.seen, [FAST]);
    assert_eq!(err(&r.verdict).code, 1045);
    // REQUIRE X509 needs a verified client certificate.
    assert_eq!(
        err(&run(&w.svc, req("acme.x509"), b"pw").await.verdict).code,
        1045
    );
    let cert = LoginRequest {
        client_cert: true,
        client_dn: None,
        ..req("acme.x509")
    };
    assert_eq!(run(&w.svc, cert, b"pw").await.verdict, AuthVerdict::Accept);
    // REQUIRE SUBJECT and ISSUER compare the verified names.
    let dn = |s: &str, i: &str| LoginRequest {
        client_cert: true,
        client_dn: Some(crate::mysql::ClientDn {
            subject: s.into(),
            issuer: i.into(),
        }),
        ..req("acme.subj")
    };
    assert_eq!(
        run(&w.svc, dn("/CN=alice", "/CN=ca"), b"pw").await.verdict,
        AuthVerdict::Accept
    );
    for bad in [dn("/CN=bob", "/CN=ca"), dn("/CN=alice", "/CN=other")] {
        assert_eq!(err(&run(&w.svc, bad, b"pw").await.verdict).code, 1045);
    }
    let no_names = LoginRequest {
        client_cert: true,
        client_dn: None,
        ..req("acme.subj")
    };
    assert_eq!(err(&run(&w.svc, no_names, b"pw").await.verdict).code, 1045);
    // Without TLS the full path needs RSA: 1045 with a hint.
    let r = run(
        &w.svc,
        LoginRequest {
            tls: false,
            ..req("acme.alice")
        },
        b"pw",
    )
    .await;
    assert!(err(&r.verdict).message.contains("connect with TLS"));

    // auth.require_tls for the tenant refuses plaintext logins of anyone.
    let w = world(FixedPolicy(HashMap::from([(
        "auth.require_tls",
        Value::Bool(true),
    )])));
    assert_eq!(
        run(&w.svc, req("acme.alice"), b"pw").await.verdict,
        AuthVerdict::Accept
    );
    let r = run(
        &w.svc,
        LoginRequest {
            tls: false,
            ..req("acme.alice")
        },
        b"pw",
    )
    .await;
    assert_eq!(r.seen, [FAST]);
    assert_eq!(err(&r.verdict).code, 1045);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_passwords_and_sandbox_mode() {
    let w = world(FixedPolicy::default());
    for user in ["acme.expired", "acme.aged"] {
        let e = err(&run(&w.svc, req(user), b"pw").await.verdict).clone();
        assert_eq!((e.code, &e.sqlstate), (1862, b"HY000"), "{user}");
        let able = LoginRequest {
            can_handle_expired_password: true,
            ..req(user)
        };
        let r = run(&w.svc, able, b"pw").await;
        assert_eq!(r.verdict, AuthVerdict::Accept, "{user}");
        assert!(r.login.principal().unwrap().must_change_password, "{user}");
    }
    // A tenant default lifetime applies to accounts without their own.
    let w = world(FixedPolicy(HashMap::from([(
        "auth.default_password_lifetime_days",
        Value::Int(30),
    )])));
    assert_eq!(
        run(&w.svc, req("acme.alice"), b"pw").await.verdict,
        AuthVerdict::Accept
    );
    w.clock.advance(Duration::from_secs(31 * 86_400));
    assert_eq!(
        err(&run(&w.svc, req("acme.alice"), b"pw").await.verdict).code,
        1862
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn suspension_shows_only_to_holders_of_the_password() {
    let w = world(FixedPolicy::default());
    let e = err(&run(&w.svc, req("susp.carol"), b"pw").await.verdict).clone();
    assert_eq!(e.code, 39303);
    assert_eq!(
        err(&run(&w.svc, req("susp.carol"), b"bad").await.verdict).code,
        1045
    );
    assert_eq!(
        err(&run(&w.svc, req("susp.nobody"), b"pw").await.verdict).code,
        1045
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unreadable_catalogs_fail_with_39301_after_the_exchange() {
    let w = world(FixedPolicy::default());
    w.catalogs.fail.store(true, Ordering::Relaxed);
    let r = run(&w.svc, req("acme.alice"), b"pw").await;
    assert_eq!(r.seen, [FULL]);
    let e = err(&r.verdict);
    assert_eq!((e.code, &e.sqlstate), (39301, b"08S01"));
    // Unknown tenants never reach the catalog: still the decoy.
    assert_eq!(
        err(&run(&w.svc, req("nobody.alice"), b"pw").await.verdict).code,
        1045
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_password_only_when_the_tenant_enables_it() {
    // Off (the default): the native-only account can't log in.
    let w = world(FixedPolicy::default());
    let r = run_with(&w.svc, req("acme.nat"), "mysql_native_password", b"npw").await;
    assert_eq!(err(&r.verdict).code, 1045);
    let on = FixedPolicy(HashMap::from([(
        "auth.mysql_native_password",
        Value::Bool(true),
    )]));
    let w = world(on);
    let r = run_with(&w.svc, req("acme.nat"), "mysql_native_password", b"npw").await;
    assert_eq!(r.verdict, AuthVerdict::Accept);
    assert!(
        r.seen.is_empty(),
        "no switch: the client already used the plugin"
    );
    // A client starting with caching_sha2 is switched to native.
    let r = run(&w.svc, req("acme.nat"), b"npw").await;
    assert_eq!(r.verdict, AuthVerdict::Accept);
    assert_eq!(r.seen.len(), 1);
    assert_eq!(r.seen[0][0], 0xFE);
    assert_eq!(
        err(&run(&w.svc, req("acme.nat"), b"bad").await.verdict).code,
        1045
    );
}

/// The packets a client sees, without the nonces: (first byte, length).
fn shape(seen: &[Vec<u8>]) -> Vec<(u8, usize)> {
    seen.iter().map(|p| (p[0], p.len())).collect()
}

/// Unknown users of a tenant look like its accounts on every plugin path
/// (loginerr): with `auth.mysql_native_password` on, a client asking for
/// native gets native from an unknown user, as from an account whose
/// password was set with both verifiers; with it off, both get
/// `caching_sha2_password`. Also for libmysqlclient's empty placeholder
/// (`--default-auth=mysql_native_password`), and the empty password.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_users_follow_the_tenants_native_policy() {
    for on in [false, true] {
        let w = world(FixedPolicy(HashMap::from([(
            "auth.mysql_native_password",
            Value::Bool(on),
        )])));
        let native = "mysql_native_password";
        let firsts: [(&str, Vec<u8>, &[u8]); 4] = [
            ("scramble", native_scramble(b"wrong", &NONCE), b"wrong"),
            ("placeholder", Vec::new(), b"wrong"),
            ("placeholder, empty password", Vec::new(), b""),
            ("NUL under caching_sha2", vec![0], b""),
        ];
        for (what, first, pw) in firsts {
            let plugin = if first == [0] {
                "caching_sha2_password"
            } else {
                native
            };
            let real = run_first(&w.svc, req("acme.dual"), plugin, first.clone(), pw).await;
            let ghost = run_first(&w.svc, req("acme.ghost"), plugin, first, pw).await;
            let what = format!("native on={on}, {what}");
            assert_eq!(shape(&real.seen), shape(&ghost.seen), "{what}");
            let (r, g) = (err(&real.verdict), err(&ghost.verdict));
            assert_eq!((r.code, g.code), (1045, 1045), "{what}");
            assert_eq!(
                r.message.replace("acme.dual", "acme.ghost"),
                g.message,
                "{what}"
            );
        }
        // The placeholder gets a nonce, and the right password then logs in.
        let ok = run_first(&w.svc, req("acme.dual"), native, Vec::new(), b"pw").await;
        assert_eq!(ok.verdict, AuthVerdict::Accept, "native on={on}");
        assert_eq!(ok.seen[0][0], 0xFE, "native on={on}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_file_source_still_works() {
    let account = Account {
        user: "app".into(),
        caching_sha2: Some(crate::verifier::CachingSha2Verifier::create(b"pw", 5000).unwrap()),
        native: None,
        backend: backend(),
    };
    let svc = Arc::new(LoginService::new(
        LoginSource::File(Arc::new(crate::accounts::StaticAccounts::new([account]))),
        Arc::new(FastCache::new(
            Arc::new(ManualClock::new()),
            crate::fast_cache::DEFAULT_TTL,
        )),
    ));
    let r = run(&svc, req("app"), b"pw").await;
    assert_eq!(r.verdict, AuthVerdict::Accept);
    let p = r.login.principal().unwrap();
    assert_eq!((p.tenant, p.user.as_str()), (None, "app"));
    assert!(r.login.file_account().is_some());
    let r = run(&svc, req("app"), b"pw").await;
    assert_eq!(r.seen, [FAST]);
    assert_eq!(
        err(&run(&svc, req("nobody"), b"pw").await.verdict).code,
        1045
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_saturated_hash_pool_gives_39301() {
    let w = world(FixedPolicy::default());
    let slots = Arc::new(Semaphore::new(1));
    let held = Arc::clone(&slots).acquire_owned().await.unwrap();
    let mut svc = LoginService::new(w.svc.source().clone(), Arc::clone(w.svc.fast_cache()))
        .with_hash_slots(Arc::clone(&slots));
    svc.slot_wait = Duration::from_millis(50);
    let svc = Arc::new(svc);
    let e = err(&run(&svc, req("acme.alice"), b"pw").await.verdict).clone();
    assert_eq!(e.code, 39301);
    drop(held);
    assert_eq!(
        run(&svc, req("acme.alice"), b"pw").await.verdict,
        AuthVerdict::Accept
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_logins_lock_the_account() {
    let mut p = FixedPolicy::default();
    p.0.insert(crate::settings::FAILED_LOGIN_ATTEMPTS.key, Value::Int(2));
    p.0.insert(crate::settings::PASSWORD_LOCK_MINUTES.key, Value::Int(1));
    let w = world(p);
    // Two wrong passwords: the second locks (in the shared state).
    for _ in 0..2 {
        assert_eq!(
            err(&run(&w.svc, req("acme.alice"), b"wrong").await.verdict).code,
            1045
        );
    }
    let row = w.state.row(ACME, UserId(1)).unwrap();
    assert_eq!(row.consecutive_failures, 2);
    let until = row.locked_until.unwrap();
    assert_eq!(until, w.clock.wall() + Duration::from_secs(60));
    // Unknown users and accounts of other names aren't counted.
    let _ = run(&w.svc, req("acme.nobody"), b"wrong").await;
    assert!(w.state.row(ACME, UserId(99)).is_none());
    let names = w.audit.names();
    assert_eq!(
        names,
        vec![
            "auth.login_failed",
            "auth.login_failed",
            "auth.account_locked",
            "auth.login_failed"
        ]
    );
    let locked = &w.audit.events()[2];
    assert_eq!(locked.reason, Some("failed_logins"));
    assert_eq!(locked.user.as_deref(), Some("alice"));
    // The lock reaches the login through the cache (here: a new snapshot).
    let lock = TempLock {
        user_id: UserId(1),
        locked_until: until,
        consecutive_failures: 2,
    };
    let acme = TenantAuth::of(&w.catalogs.map.lock().unwrap()[&ACME])
        .unwrap()
        .apply(Vec::new(), &[], vec![lock]);
    w.catalogs
        .map
        .lock()
        .unwrap()
        .insert(ACME, snapshot(ACME, acme, 8));
    let e = err(&run(&w.svc, req("acme.alice"), b"pw").await.verdict).clone();
    assert_eq!(e.code, 3955);
    let maria = LoginRequest {
        persona: Persona::MariaDb,
        ..req("acme.alice")
    };
    assert_eq!(err(&run(&w.svc, maria, b"pw").await.verdict).code, 4150);
    // After a minute it unlocks by itself, and the login clears the row.
    w.clock.advance(Duration::from_secs(61));
    let r = run(&w.svc, req("acme.alice"), b"pw").await;
    assert_eq!(r.verdict, AuthVerdict::Accept);
    for _ in 0..50 {
        if w.audit.names().last() == Some(&"auth.account_unlocked") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(w.state.row(ACME, UserId(1)).unwrap().locked_until.is_none());
    let ev = w.audit.events();
    let unlocked = ev
        .iter()
        .find(|e| e.event == "auth.account_unlocked")
        .unwrap();
    assert_eq!(unlocked.reason, Some("lock_expired"));
    assert!(ev.iter().any(|e| e.event == "auth.login_succeeded"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tracking_off_counts_nothing() {
    let w = world(FixedPolicy::default());
    for _ in 0..3 {
        let _ = run(&w.svc, req("acme.alice"), b"wrong").await;
    }
    assert!(w.state.row(ACME, UserId(1)).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn too_many_failures_from_an_address_refuse_it() {
    let mut p = FixedPolicy::default();
    p.0.insert(crate::settings::IP_FAILURES_PER_MINUTE.key, Value::Int(3));
    p.0.insert(crate::settings::IP_BLOCK_SECONDS.key, Value::Int(60));
    let w = world(p);
    let ip: IpAddr = "192.0.2.10".parse().unwrap();
    for _ in 0..3 {
        let _ = run(&w.svc, req("acme.nobody"), b"wrong").await;
        w.svc.admit(ip).unwrap();
    }
    let _ = run(&w.svc, req("acme.alice"), b"wrong").await;
    let e = w.svc.admit(ip).unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::LoginRateLimited));
    assert!(w.svc.admit("192.0.2.11".parse().unwrap()).is_ok());
    assert!(w.audit.names().contains(&"auth.rate_limited"));
    w.clock.advance(Duration::from_secs(60));
    w.svc.admit(ip).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_failures_are_delayed() {
    let mut p = FixedPolicy::default();
    p.0.insert(crate::settings::DELAY_AFTER_FAILURES.key, Value::Int(1));
    p.0.insert(crate::settings::FAILURE_DELAY_MS.key, Value::Int(300));
    p.0.insert(crate::settings::FAILURE_DELAY_MAX_MS.key, Value::Int(300));
    let w = world(p);
    let first = run(&w.svc, req("acme.alice"), b"wrong").await.took;
    let second = run(&w.svc, req("acme.alice"), b"wrong").await.took;
    assert!(
        second >= first + Duration::from_millis(250),
        "{first:?} then {second:?}"
    );
    // Another address isn't slowed; a success resets.
    let other = LoginRequest {
        client_ip: "192.0.2.99".parse().unwrap(),
        ..req("acme.alice")
    };
    let t = run(&w.svc, other, b"wrong").await.took;
    assert!(t < first + Duration::from_millis(250), "{t:?}");
    assert_eq!(
        run(&w.svc, req("acme.alice"), b"pw").await.verdict,
        AuthVerdict::Accept
    );
    let t = run(&w.svc, req("acme.alice"), b"wrong").await.took;
    assert!(t < first + Duration::from_millis(250), "{t:?}");
}

/// Waits for the background backfills to reach `n`.
async fn backfills(w: &World, n: usize) -> Vec<(TenantId, UserId, u64)> {
    for _ in 0..200 {
        let b = w.state.backfills();
        if b.len() >= n {
            return b;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    w.state.backfills()
}

/// The upgrade on login: a successful login that saw the clear
/// password of a generation without SCRAM verifiers stores them, once per
/// verifier; the fast path (no clear password), a failed login, an account
/// that has them, and `auth.backfill_verifiers = off` don't.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::many_single_char_names)]
async fn full_logins_backfill_scram_verifiers() {
    let w = world(FixedPolicy::default());
    // Full path, primary (verifier 10).
    assert_eq!(
        run(&w.svc, req("acme.alice"), b"pw").await.verdict,
        AuthVerdict::Accept
    );
    assert_eq!(backfills(&w, 1).await, [(ACME, UserId(1), 10)]);
    let added: Vec<_> = w
        .audit
        .events()
        .into_iter()
        .filter(|e| e.event == crate::audit::events::VERIFIERS_ADDED)
        .collect();
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].protocol, "mysql");
    assert_eq!(added[0].reason, Some("upgrade_on_login"));
    assert_eq!(
        added[0].detail.as_deref(),
        Some("scram_sha_256,scram_sha_512")
    );
    // The fast path sees no password.
    let r = run(&w.svc, req("acme.alice"), b"pw").await;
    assert_eq!(
        (r.verdict, r.seen),
        (AuthVerdict::Accept, vec![FAST.to_vec()])
    );
    // The secondary password: its own generation (verifier 11).
    assert_eq!(
        run(&w.svc, req("acme.alice"), b"old-pw").await.verdict,
        AuthVerdict::Accept
    );
    assert_eq!(backfills(&w, 2).await[1], (ACME, UserId(1), 11));
    // Wrong passwords, refused accounts and accounts with SCRAM: nothing.
    let _ = run(&w.svc, req("acme.alice"), b"nope").await;
    let _ = run(&w.svc, req("acme.locked"), b"pw").await;
    assert_eq!(
        run(&w.svc, req("acme.dual"), b"pw").await.verdict,
        AuthVerdict::Accept
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(w.state.backfills().len(), 2);
    // HTTP Basic's clear-text fast path sees the password: it backfills,
    // labelled http.
    let w = world(FixedPolicy::default());
    let mut l = w.svc.start_http(req("acme.tls")).await;
    assert!(l.check_password(Zeroizing::new(b"pw".to_vec())).await);
    assert_eq!(
        l.decide(Outcome::Verified, AuthPath::Full).await,
        AuthVerdict::Accept
    );
    let mut l = w.svc.start_kafka(req("acme.x509")).await;
    assert!(!l.check_clear_fast(b"pw"));
    assert!(l.check_password(Zeroizing::new(b"pw".to_vec())).await);
    let _ = l.decide(Outcome::Verified, AuthPath::Full).await;
    let _ = backfills(&w, 1).await;
    let mut l = w.svc.start_http(req("acme.alice")).await;
    assert!(l.check_password(Zeroizing::new(b"pw".to_vec())).await);
    let _ = l.decide(Outcome::Verified, AuthPath::Full).await;
    let _ = backfills(&w, 2).await;
    // (The gateway backfills a user once at a time.)
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut l = w.svc.start_kafka(req("acme.alice")).await;
    assert!(l.check_clear_fast(b"pw"));
    assert_eq!(
        l.decide(Outcome::Verified, AuthPath::Fast).await,
        AuthVerdict::Accept
    );
    let b = backfills(&w, 3).await;
    assert_eq!(b.len(), 3, "{b:?}");
    let protocols: Vec<_> = w
        .audit
        .events()
        .into_iter()
        .filter(|e| e.event == crate::audit::events::LOGIN_SUCCEEDED)
        .map(|e| e.protocol)
        .collect();
    assert_eq!(protocols, ["http", "http", "kafka"]);
    // x509 was refused (no client certificate): no backfill, a kafka
    // failure event.
    assert!(
        w.audit
            .events()
            .iter()
            .any(|e| e.event == crate::audit::events::LOGIN_FAILED
                && e.protocol == "kafka"
                && e.reason == Some("tls_required"))
    );
    // Off.
    let mut p = FixedPolicy::default();
    p.0.insert(crate::settings::BACKFILL_VERIFIERS.key, Value::Bool(false));
    let w = world(p);
    assert_eq!(
        run(&w.svc, req("acme.alice"), b"pw").await.verdict,
        AuthVerdict::Accept
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(w.state.backfills().is_empty());
}

/// The SCRAM verifier a SCRAM exchange uses comes from the cache.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_credentials_from_the_cache() {
    let w = world(FixedPolicy::default());
    let l = w.svc.start_kafka(req("acme.dual")).await;
    for kind in ScramKind::ALL {
        let (v, secondary) = l.scram_credential(kind).unwrap();
        assert_eq!(v.kind(), kind);
        assert!(v.verify_password(b"pw"));
        assert!(secondary.is_none());
    }
    // No verifiers, the decoy, an unknown tenant: none.
    for name in ["acme.alice", "acme.nobody", "nowhere.alice"] {
        let l = w.svc.start_kafka(req(name)).await;
        assert!(l.scram_credential(ScramKind::Sha256).is_none(), "{name}");
    }
}

/// One certificate check for every protocol's login.
#[test]
fn certificate_clauses_are_checked_alike() {
    let mut r = UserRow::new(
        sprig_common::ids::UserId(9),
        "carol",
        "%",
        crate::store::UserSource::Sql,
        std::time::UNIX_EPOCH,
        1,
    );
    let dn = ClientDn {
        subject: "/O=Acme/CN=carol".into(),
        issuer: "/CN=Acme CA".into(),
    };
    assert!(certificate_ok(&r, false, None), "no clause");
    r.require_tls = RequireTls::X509;
    assert!(
        !certificate_ok(&r, false, None),
        "X509 without a certificate"
    );
    assert!(certificate_ok(&r, true, None), "X509 with one");
    r.require_subject = Some("/O=Acme/CN=carol".into());
    assert!(!certificate_ok(&r, true, None), "SUBJECT without names");
    assert!(certificate_ok(&r, true, Some(&dn)));
    assert!(
        !certificate_ok(&r, false, Some(&dn)),
        "names without a verified certificate"
    );
    r.require_issuer = Some("/CN=Other CA".into());
    assert!(!certificate_ok(&r, true, Some(&dn)), "ISSUER differs");
    r.require_issuer = Some("/CN=Acme CA".into());
    assert!(certificate_ok(&r, true, Some(&dn)));
}
