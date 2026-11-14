//! Account policies end to end, against two running gateways: lockout
//! with automatic unlock, password expiry and policy, failure delays and
//! the address limit through both personas; revocation timed across two
//! gateway processes.
//!
//! Skipped unless `SPRIG_IDENTITY_LIVE=1`. Needs, besides the Compose
//! stack:
//!
//! - a tenant made by the controller: `SPRIG_IDENTITY_TENANT` (name) and
//!   `SPRIG_IDENTITY_TENANT_ID`, its catalog on `SPRIG_TEST_MARIADB_SINGLE`
//!   (`mysql://root:sprig-dev-root@127.0.0.1:13301`), its directory in
//!   `SPRIG_TEST_ETCD` (`http://127.0.0.1:2379`);
//! - two gateways of this build in catalog mode: A on
//!   `SPRIG_IDENTITY_GW_A` (MySQL persona, default 15306) and
//!   `SPRIG_IDENTITY_GW_A_MARIA` (MariaDB persona, 15307), B on
//!   `SPRIG_IDENTITY_GW_B` (15316).
//!
//! The address-limit case runs last: it leaves 127.0.0.1 refused on A for
//! `auth.ip_block_seconds` (300 s by default).
//!
//! Account changes are made the way the MySQL account statements make
//! them: through `sprig_auth::admin` on the tenant's catalog, as auth
//! changes. Every measured time is printed with `IDENTITY:` in front.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::print_stderr,
    clippy::many_single_char_names
)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Pool, SslOpts};
use sprig_auth::admin::{self, NewUser, SetPassword};
use sprig_auth::policy::AccountClauses;
use sprig_catalog::settings::{FixedEtcd, SetRequest, Target};
use sprig_catalog::{
    AllowAll, CatalogEndpoint, DirectoryAllocator, LogDecisions, MariaBackend, MariaCatalog,
    StaticLocator, Store, TenantSettingsWriter, Versioner,
};
use sprig_common::ids::{DcId, TenantId, UserId};
use sprig_common::names::SchemaName;
use sprig_common::time::SystemClock;
use sprig_meta::{EtcdConfig, EtcdStore, MetaStore, Tier};
use sprig_settings::{MemSettings, Registry, SettingChange, Value};

fn env(k: &str, default: &str) -> String {
    std::env::var(k)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_owned())
}

struct World {
    tenant: String,
    tid: TenantId,
    cat: MariaCatalog,
    global: Arc<dyn MetaStore>,
    a: u16,
    a_maria: u16,
    b: u16,
}

/// A login: the connection, or the server's (code, message).
async fn login(port: u16, tenant: &str, user: &str, pw: &str) -> Result<Conn, (u16, String)> {
    let opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some(format!("{tenant}.{user}")))
        .pass(Some(pw))
        .prefer_socket(false)
        .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true));
    match Conn::new(opts).await {
        Ok(c) => Ok(c),
        Err(mysql_async::Error::Server(e)) => Err((e.code, e.message)),
        Err(e) => Err((0, e.to_string())),
    }
}

fn code<T>(r: &Result<T, (u16, String)>) -> u16 {
    match r {
        Ok(_) => 0,
        Err((c, _)) => *c,
    }
}

async fn query(c: &mut Conn, sql: &str) -> Result<(), (u16, String)> {
    match c.query_drop(sql).await {
        Ok(()) => Ok(()),
        Err(mysql_async::Error::Server(e)) => Err((e.code, e.message)),
        Err(e) => Err((0, e.to_string())),
    }
}

impl World {
    async fn user(&self, name: &str, pw: &str) -> UserId {
        let clock = SystemClock;
        if let Some(id) = admin::find_user(&self.cat, self.tid, name, "%")
            .await
            .unwrap()
        {
            admin::drop_user(&self.cat, self.tid, id).await.unwrap();
        }
        admin::create_user(
            &self.cat,
            self.tid,
            &NewUser::new(name, pw.as_bytes()),
            &clock,
        )
        .await
        .unwrap()
        .user_id
    }

    async fn clauses(&self, user: UserId, c: AccountClauses) {
        admin::set_clauses(&self.cat, self.tid, user, c)
            .await
            .unwrap();
    }

    async fn set_tenant_setting(&self, key: &str, v: Value) {
        let registry = Registry::global().unwrap();
        let writer = TenantSettingsWriter::new(
            self.cat.clone(),
            registry,
            Arc::new(FixedEtcd {
                rows: Arc::new(MemSettings::new(registry)),
                dc: DcId::new("dc-a").unwrap(),
            }),
            Arc::new(AllowAll),
            Arc::new(LogDecisions),
            Arc::new(SystemClock),
        );
        let change = match v {
            Value::Str(s) if s == "<reset>" => SettingChange::Reset,
            v => SettingChange::Set(v),
        };
        writer
            .set(&SetRequest {
                tenant: self.tid,
                target: Target::Tenant,
                key: key.to_owned(),
                change,
                principal: sprig_catalog::Principal::Operator("identity-live".into()),
                reason: "identity live test".into(),
            })
            .await
            .unwrap();
    }

    async fn login(&self, port: u16, user: &str, pw: &str) -> Result<Conn, (u16, String)> {
        login(port, &self.tenant, user, pw).await
    }

    /// Polls until `f` holds, every few milliseconds; returns how long it
    /// took, or panics after `limit`.
    async fn until<F, Fut>(what: &str, limit: Duration, mut f: F) -> Duration
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let t0 = Instant::now();
        loop {
            if f().await {
                return t0.elapsed();
            }
            assert!(t0.elapsed() < limit, "{what}: not within {limit:?}");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
}

/// Runs `SELECT 1` on `c` every 2 ms until it fails; returns how long that
/// took. An idle session gets 39306 unsolicited (sequence 0, like MySQL's
/// 4031) and the connection closes, so the client's next statement reads
/// the error out of turn ("Packets out of sync") or finds the socket
/// closed; a busy one gets 39306 as the answer to its next statement.
async fn closed(c: &mut Conn) -> Duration {
    let t0 = Instant::now();
    loop {
        match query(c, "SELECT 1").await {
            Ok(()) => {}
            Err((code, m)) => {
                eprintln!("IDENTITY: session closed: {code} {m}");
                assert!(code == 39306 || code == 0, "{code} {m}");
                return t0.elapsed();
            }
        }
        assert!(
            t0.elapsed() < Duration::from_secs(10),
            "the session wasn't closed"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

async fn world() -> Option<World> {
    if std::env::var("SPRIG_IDENTITY_LIVE").ok().as_deref() != Some("1") {
        eprintln!("skipped: set SPRIG_IDENTITY_LIVE=1 (see the file's docs)");
        return None;
    }
    let tenant = env("SPRIG_IDENTITY_TENANT", "identity-t1");
    let tid = TenantId(env("SPRIG_IDENTITY_TENANT_ID", "0").parse().unwrap());
    let url = env(
        "SPRIG_TEST_MARIADB_SINGLE",
        "mysql://root:sprig-dev-root@127.0.0.1:13301",
    );
    let pool = Pool::new(mysql_async::Opts::from_url(&url).unwrap());
    let schema = SchemaName::catalog(tid).to_string();
    let locator = StaticLocator::new()
        .with(
            tid,
            CatalogEndpoint {
                pool,
                store: Store::new(&schema),
            },
        )
        .shared();
    let global: Arc<dyn MetaStore> = Arc::new(
        EtcdStore::connect(
            Tier::Global,
            &EtcdConfig::new(vec![env("SPRIG_TEST_ETCD", "http://127.0.0.1:2379")]),
        )
        .await
        .unwrap(),
    );
    let cat = Versioner::new(
        MariaBackend::new(locator),
        Arc::new(DirectoryAllocator::new(Arc::clone(&global))),
    );
    Some(World {
        tenant,
        tid,
        cat,
        global,
        a: env("SPRIG_IDENTITY_GW_A", "15306").parse().unwrap(),
        a_maria: env("SPRIG_IDENTITY_GW_A_MARIA", "15307").parse().unwrap(),
        b: env("SPRIG_IDENTITY_GW_B", "15316").parse().unwrap(),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identity_live() {
    let Some(w) = world().await else { return };
    let _ = &w.global;
    let pw = "Identity-pw-1";

    // ---- 1. Lockout: 3 failures lock for a minute, on every gateway ----
    let lock = w.user("lock", pw).await;
    w.clauses(
        lock,
        AccountClauses {
            failed_login_attempts: Some(Some(3)),
            password_lock_minutes: Some(Some(1)),
            ..AccountClauses::default()
        },
    )
    .await;
    assert_eq!(
        code(&w.login(w.a, "lock", pw).await),
        0,
        "the right password"
    );
    for i in 1..=3 {
        let r = w.login(w.a, "lock", "wrong").await;
        eprintln!(
            "IDENTITY: lock: failure {i} on A (MySQL): {:?}",
            r.as_ref().err()
        );
        assert_eq!(code(&r), 1045);
    }
    let r = w.login(w.a, "lock", pw).await;
    eprintln!(
        "IDENTITY: lock: right password on A (MySQL): {:?}",
        r.as_ref().err()
    );
    assert_eq!(code(&r), 3955);
    let r = w.login(w.a_maria, "lock", pw).await;
    eprintln!(
        "IDENTITY: lock: right password on A (MariaDB): {:?}",
        r.as_ref().err()
    );
    assert_eq!(code(&r), 4150);
    let r = w.login(w.b, "lock", pw).await;
    eprintln!(
        "IDENTITY: lock: right password on B: {:?}",
        r.as_ref().err()
    );
    assert_eq!(code(&r), 3955, "the lock is shared by every gateway");
    // ACCOUNT UNLOCK ends it at once.
    admin::set_account_locked(&w.cat, w.tid, lock, false)
        .await
        .unwrap();
    let wr = &w;
    let t = World::until(
        "unlock reaches B",
        Duration::from_secs(5),
        move || async move { wr.login(wr.b, "lock", pw).await.is_ok() },
    )
    .await;
    eprintln!("IDENTITY: lock: ACCOUNT UNLOCK reached B after {t:?}");
    // Lock again, then wait it out: the lock ends by itself.
    for _ in 0..3 {
        let _ = w.login(w.b, "lock", "wrong").await;
    }
    assert_eq!(code(&w.login(w.a, "lock", pw).await), 3955);
    let t0 = Instant::now();
    let t = World::until(
        "automatic unlock",
        Duration::from_secs(75),
        move || async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            wr.login(wr.a, "lock", pw).await.is_ok()
        },
    )
    .await;
    eprintln!(
        "IDENTITY: lock: unlocked by itself {:?} after the third failure (1-minute lock)",
        t0.elapsed().max(t)
    );

    // ---- 2. Failure delays: after 3 failures in a row, 1 s, doubling ----
    // Delays live in gateway memory for 15 idle minutes, keyed by name:
    // a name of this run's own.
    let dname = format!("delay{}", std::process::id());
    w.user(&dname, pw).await;
    let mut took = Vec::new();
    for _ in 0..5 {
        let t0 = Instant::now();
        let r = w.login(w.b, &dname, "wrong").await;
        assert_eq!(code(&r), 1045, "{:?}", r.err());
        took.push(t0.elapsed().as_millis());
    }
    eprintln!("IDENTITY: delay: five wrong passwords on B took {took:?} ms");
    assert!(took[3] >= 900 && took[4] >= 1900, "{took:?}");
    let t0 = Instant::now();
    let other = w.login(w.a, &dname, "wrong").await;
    eprintln!(
        "IDENTITY: delay: the same account from gateway A (another key) took {} ms",
        t0.elapsed().as_millis()
    );
    assert_eq!(code(&other), 1045);

    // ---- 3. ACCOUNT LOCK, expiry and sandbox mode, both personas ----
    let manual = w.user("manual", pw).await;
    admin::set_account_locked(&w.cat, w.tid, manual, true)
        .await
        .unwrap();
    let my = w.login(w.a, "manual", pw).await;
    let maria = w.login(w.a_maria, "manual", pw).await;
    eprintln!(
        "IDENTITY: ACCOUNT LOCK: MySQL {:?}, MariaDB {:?}",
        my.as_ref().err(),
        maria.as_ref().err()
    );
    assert_eq!((code(&my), code(&maria)), (3118, 4151));
    let exp = w.user("exp", pw).await;
    admin::set_password_expired(&w.cat, w.tid, exp, true)
        .await
        .unwrap();
    match w.login(w.a, "exp", pw).await {
        Err((c, m)) => {
            eprintln!("IDENTITY: expired: login refused {c}: {m}");
            assert_eq!(c, 1862);
        }
        Ok(mut c) => {
            let r = query(&mut c, "SELECT 1").await;
            eprintln!(
                "IDENTITY: expired: sandbox mode, SELECT gives {:?}",
                r.as_ref().err()
            );
            assert_eq!(code(&r), 1820);
            query(&mut c, "ALTER USER USER() IDENTIFIED BY 'Identity-pw-2'")
                .await
                .unwrap();
            query(&mut c, "SELECT 1").await.unwrap();
            eprintln!("IDENTITY: expired: after ALTER USER the session runs statements");
        }
    }

    // ---- 4. Password policy: history (account clause), strength (setting) ----
    let hist = w.user("hist", pw).await;
    w.clauses(
        hist,
        AccountClauses {
            password_history: Some(Some(2)),
            ..AccountClauses::default()
        },
    )
    .await;
    let mut c = w.login(w.a, "hist", pw).await.unwrap();
    let r = query(&mut c, &format!("ALTER USER USER() IDENTIFIED BY '{pw}'")).await;
    eprintln!("IDENTITY: history (MySQL): {:?}", r.as_ref().err());
    assert_eq!(code(&r), 3638);
    let mut c = w.login(w.a_maria, "hist", pw).await.unwrap();
    let r = query(&mut c, &format!("SET PASSWORD = '{pw}'")).await;
    eprintln!("IDENTITY: history (MariaDB): {:?}", r.as_ref().err());
    assert_eq!(code(&r), 1819);
    w.set_tenant_setting("auth.password_policy", Value::Str("medium".into()))
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut c = w.login(w.a, "hist", pw).await.unwrap();
    let weak = query(&mut c, "SET PASSWORD = 'weakweak'").await;
    eprintln!("IDENTITY: strength medium: weak {:?}", weak.as_ref().err());
    assert_eq!(code(&weak), 1819);
    query(&mut c, "SET PASSWORD = 'Strong-pw-3!'")
        .await
        .unwrap();
    eprintln!("IDENTITY: strength medium: 'Strong-pw-3!' accepted");
    w.set_tenant_setting("auth.password_policy", Value::Str("<reset>".into()))
        .await;

    // ---- 5. Revocation across two gateways ----
    for round in 0..3 {
        let name = format!("rev{round}");
        let id = w.user(&name, pw).await;
        let mut on_b = w.login(w.b, &name, pw).await.unwrap();
        let mut on_a = w.login(w.a, &name, pw).await.unwrap();
        query(&mut on_b, "SELECT 1").await.unwrap();
        query(&mut on_a, "SELECT 1").await.unwrap();
        // Drop: new logins refused, and open sessions closed (39306).
        let t0 = Instant::now();
        admin::drop_user(&w.cat, w.tid, id).await.unwrap();
        let committed = t0.elapsed();
        let n = name.as_str();
        // All three polled at once, timed from the commit's return.
        let (login_b, session_b, session_a) = tokio::join!(
            World::until(
                "drop reaches B logins",
                Duration::from_secs(10),
                move || async move { code(&wr.login(wr.b, n, pw).await) == 1045 }
            ),
            closed(&mut on_b),
            closed(&mut on_a),
        );
        eprintln!(
            "IDENTITY: revocation round {round}: drop committed in {committed:?}; then new logins refused on B after {login_b:?}, B session closed (39306) after {session_b:?}, A session after {session_a:?}"
        );
    }
    // A password change: the old password stops working on B.
    let pwu = w.user("pwchange", pw).await;
    assert!(w.login(w.b, "pwchange", pw).await.is_ok());
    let t0 = Instant::now();
    admin::set_password(
        &w.cat,
        w.tid,
        pwu,
        b"Identity-new-9",
        SetPassword::default(),
        &SystemClock,
    )
    .await
    .unwrap();
    let committed = t0.elapsed();
    let old_refused = World::until(
        "old password refused on B",
        Duration::from_secs(10),
        move || async move { code(&wr.login(wr.b, "pwchange", pw).await) == 1045 },
    )
    .await;
    assert!(w.login(w.b, "pwchange", "Identity-new-9").await.is_ok());
    eprintln!(
        "IDENTITY: password change committed in {committed:?}; old password refused on B after {old_refused:?}"
    );
    // ACCOUNT LOCK closes sessions too.
    let lk = w.user("lockme", pw).await;
    let mut s = w.login(w.b, "lockme", pw).await.unwrap();
    let t0 = Instant::now();
    admin::set_account_locked(&w.cat, w.tid, lk, true)
        .await
        .unwrap();
    let changed = t0.elapsed();
    let took = closed(&mut s).await;
    eprintln!(
        "IDENTITY: ACCOUNT LOCK returned in {changed:?}; the B session got 39306 {took:?} later"
    );

    // ---- 6. The address limit (last: it blocks 127.0.0.1 on A) ----
    let mut n = 0;
    for i in 0..40 {
        n += 1;
        let r = w.login(w.a, &format!("nobody{i}"), "x").await;
        if code(&r) == 39300 {
            eprintln!(
                "IDENTITY: address limit: attempt {n} refused before the greeting: {:?}",
                r.err()
            );
            break;
        }
        assert_eq!(code(&r), 1045);
    }
    let maria = w.login(w.a_maria, "lock", pw).await;
    eprintln!(
        "IDENTITY: address limit: the MariaDB persona of A too: {:?}",
        maria.as_ref().err()
    );
    assert_eq!(code(&maria), 39300);
    assert!(
        w.login(w.b, "lock", pw).await.is_ok(),
        "B is another gateway"
    );

    for name in [
        "lock", &dname, "manual", "exp", "hist", "pwchange", "lockme",
    ] {
        if let Some(id) = admin::find_user(&w.cat, w.tid, name, "%").await.unwrap() {
            admin::drop_user(&w.cat, w.tid, id).await.unwrap();
        }
    }
}

/// Account changes through `AccountOps`, with their audit events (an
/// operator acting from tenant 0, above the tenant). Needs only the
/// catalog and etcd.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identity_live_account_ops() {
    use sprig_auth::admin::AccountOps;
    use sprig_auth::audit::{Actor, MemoryAudit};
    let Some(w) = world().await else { return };
    let audit = MemoryAudit::new();
    let ops = AccountOps {
        cat: &w.cat,
        tenant: w.tid,
        tenant_name: Some(w.tenant.clone()),
        actor: Actor::operator(TenantId(0), "op:identity-live"),
        audit: &*audit,
        clock: &SystemClock,
        protocol: "admin",
    };
    if let Some(id) = admin::find_user(&w.cat, w.tid, "audited", "%")
        .await
        .unwrap()
    {
        admin::drop_user(&w.cat, w.tid, id).await.unwrap();
    }
    let id = ops
        .create_user(&NewUser::new("audited", b"Audited-pw-1"))
        .await
        .unwrap()
        .user_id;
    ops.set_locked(id, "audited", "%", true).await.unwrap();
    ops.set_locked(id, "audited", "%", false).await.unwrap();
    ops.set_expired(id, "audited", "%", true).await.unwrap();
    ops.reset_password(id, "audited", "%", b"Audited-pw-2", SetPassword::default())
        .await
        .unwrap();
    ops.drop_user(id, "audited", "%").await.unwrap();
    let e = ops.drop_user(id, "audited", "%").await.unwrap_err();
    eprintln!("IDENTITY: second drop refused: {e}");
    for e in audit.events() {
        eprintln!("IDENTITY: audit {}", e.to_json());
    }
    let created = &audit.events()[0];
    assert_eq!(created.streams(), vec![w.tid, TenantId(0)]);
    assert_eq!(
        audit.names(),
        vec![
            "auth.user_created",
            "auth.account_locked",
            "auth.account_unlocked",
            "auth.account_changed",
            "auth.password_changed",
            "auth.user_dropped",
            "auth.user_dropped"
        ]
    );
}
