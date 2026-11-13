#![allow(clippy::many_single_char_names)]

use std::time::Duration;

use super::*;

fn limiter(f: impl FnOnce(&mut Limits)) -> Arc<Limiter> {
    let mut l = Limits {
        gateway_memory: Some(10_000),
        ..Limits::default()
    };
    f(&mut l);
    Limiter::new(LimitSource::Fixed(l))
}

const T: Option<TenantId> = Some(TenantId(7));

#[tokio::test]
async fn statements_queue_and_time_out_past_the_tenant_limit() {
    let l = limiter(|l| {
        l.tenant_active = 2;
        l.queue_timeout = Duration::from_millis(100);
    });
    let a = l.admit(T, "u1", false).await.unwrap();
    let _b = l.admit(T, "u2", false).await.unwrap();
    let e = l.admit(T, "u3", false).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::AdmissionTimeout));
    // A waiter gets the slot when one frees.
    let l2 = Arc::clone(&l);
    let waiter = tokio::spawn(async move { l2.admit(T, "u3", false).await.map(|_| ()) });
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(a);
    waiter.await.unwrap().unwrap();
    // Another tenant is not affected.
    let _c = l.admit(Some(TenantId(8)), "u1", false).await.unwrap();
}

#[tokio::test]
async fn users_have_their_own_limit_and_transactions_go_first() {
    let l = limiter(|l| {
        l.tenant_active = 2;
        l.user_active = 1;
        l.queue_timeout = Duration::from_secs(2);
    });
    let a = l.admit(T, "u1", false).await.unwrap();
    let b = l.admit(T, "u2", false).await.unwrap();
    // u1 at its own limit: u1's next waits even when the tenant has room.
    let order = Arc::new(Mutex::new(Vec::new()));
    let spawn = |user: &'static str, high: bool| {
        let (l, order) = (Arc::clone(&l), Arc::clone(&order));
        tokio::spawn(async move {
            let p = l.admit(T, user, high).await.unwrap();
            order.lock().unwrap().push(user);
            tokio::time::sleep(Duration::from_millis(10)).await;
            drop(p);
        })
    };
    let low = spawn("u3", false);
    tokio::time::sleep(Duration::from_millis(10)).await;
    let high = spawn("u4", true);
    tokio::time::sleep(Duration::from_millis(10)).await;
    drop(b);
    high.await.unwrap();
    drop(a);
    low.await.unwrap();
    assert_eq!(
        *order.lock().unwrap(),
        vec!["u4", "u3"],
        "the transaction's statement first"
    );
}

#[test]
fn connections_are_capped_per_gateway_and_tenant() {
    let l = limiter(|l| {
        l.max_connections = 2;
        l.tenant_connections = 1;
    });
    let a = l.try_connection().unwrap();
    let _b = l.try_connection().unwrap();
    assert!(l.try_connection().is_none());
    drop(a);
    assert!(l.try_connection().is_some());
    let s = l.attach(T).unwrap();
    assert_eq!(
        l.attach(T).unwrap_err().code(),
        Some(SprigCode::TenantConnectionLimit)
    );
    drop(s);
    let _s = l.attach(T).unwrap();
}

#[test]
fn memory_charges_nest() {
    let l = limiter(|l| {
        l.gateway_memory = Some(1000);
        l.tenant_memory = 800;
    });
    let mut a = l.charge(T, "u1");
    a.grow(200).unwrap();
    // The user's budget is a quarter of the tenant's.
    assert_eq!(a.grow(1).unwrap_err().code(), Some(SprigCode::MemoryLimit));
    let mut b = l.charge(T, "u2");
    b.grow(200).unwrap();
    let mut c = l.charge(Some(TenantId(9)), "u1");
    c.grow(200).unwrap();
    let mut d = l.charge(Some(TenantId(10)), "u1");
    d.grow(200).unwrap();
    // The gateway's budget (1000) is now 800 used.
    assert_eq!(
        l.charge(Some(TenantId(11)), "u")
            .grow(201)
            .unwrap_err()
            .code(),
        Some(SprigCode::MemoryLimit)
    );
    drop(a);
    assert_eq!(l.memory(), 600);
    drop((b, c, d));
    assert_eq!(l.memory(), 0);
}

#[test]
fn commits_and_rollbacks_skip_admission() {
    for (sql, free) in [
        ("COMMIT", true),
        ("  rollback", true),
        ("SELECT 1", false),
        ("commitx", true),
    ] {
        let r = Request::Query {
            sql: bytes::Bytes::from(sql),
            attrs: sprig_proto_mysql::command::QueryAttrs::default(),
        };
        assert_eq!(free_pass(&r), free, "{sql}");
    }
}

/// Chapter 04: the `limits.*` keys are tenant-scoped. A tenant's own value
/// (from its cached catalog settings) applies to that tenant only; a tenant
/// whose catalog isn't cached, and the gateway keys, keep the dc and global
/// values. A new settings snapshot of the tenant applies at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tenant_scoped_limits_apply_per_tenant() {
    use sprig_meta::MemStore;
    use sprig_meta::conformance::unique_tier;
    use sprig_settings::{Entry, Registry, SettingsStores, TenantScope, Value};

    let registry: &'static Registry = Registry::global().unwrap();
    let dc_store = MemStore::new(unique_tier());
    let global = dc_store.with_tier(unique_tier());
    let stores = SettingsStores::new(Arc::new(global), Arc::new(dc_store)).unwrap();
    let dc = stores.dc_id().clone();
    let live = LiveSettings::start(registry, stores).unwrap();
    tokio::time::timeout(Duration::from_secs(10), live.ready())
        .await
        .unwrap();
    let with = |n: i64, version: u64| {
        let mut b = TenantSettings::builder(registry, TenantId(7), version);
        b.put(
            TenantScope::Tenant,
            TENANT_CONNECTIONS.key,
            Entry {
                value: Some(Value::Int(n)),
                bounds: None,
            },
        );
        b.put(
            TenantScope::Tenant,
            QUEUE_TIMEOUT_MS.key,
            Entry {
                value: Some(Value::Int(n * 10)),
                bounds: None,
            },
        );
        Arc::new(b.build())
    };
    let current = Arc::new(Mutex::new(with(1, 1)));
    let lookup: TenantSettingsLookup = {
        let current = Arc::clone(&current);
        Arc::new(move |t| (t == TenantId(7)).then(|| Arc::clone(&current.lock().unwrap())))
    };
    let l = Limiter::new(LimitSource::Live {
        live,
        dc,
        tenants: Some(lookup),
    });
    let mine = l.limits_for(T);
    assert_eq!(mine.tenant_connections, 1);
    assert_eq!(mine.queue_timeout, Duration::from_millis(10));
    assert_eq!(mine.max_connections, us(MAX_CONNECTIONS.default));
    let other = l.limits_for(Some(TenantId(8)));
    assert_eq!(other.tenant_connections, us(TENANT_CONNECTIONS.default));
    assert_eq!(l.limits_for(None), l.limits());
    // Admission uses the tenant's value.
    let s = l.attach(T).unwrap();
    assert_eq!(
        l.attach(T).unwrap_err().code(),
        Some(SprigCode::TenantConnectionLimit)
    );
    let _o = l.attach(Some(TenantId(8))).unwrap();
    let _o2 = l.attach(Some(TenantId(8))).unwrap();
    // A changed tenant snapshot applies at once (the memo follows it).
    *current.lock().unwrap() = with(2, 2);
    assert_eq!(l.limits_for(T).tenant_connections, 2);
    let _s2 = l.attach(T).unwrap();
    drop(s);
}
