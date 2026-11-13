#![allow(clippy::unwrap_used)]

use serde_json::json;
use sprig_meta::{MemStore, Op, Tier, Txn};

use super::*;

fn sfx() -> Vec<String> {
    vec!["db.example.com".to_owned()]
}

#[test]
fn tenant_names() {
    assert!(valid_tenant_name("acme"));
    assert!(valid_tenant_name("a1-b"));
    assert!(!valid_tenant_name("a"));
    assert!(!valid_tenant_name("Acme"));
    assert!(!valid_tenant_name("1acme"));
    assert!(!valid_tenant_name("ac_me"));
    assert!(!valid_tenant_name(&"a".repeat(64)));
    assert!(valid_tenant_name(&"a".repeat(63)));
    // A DNS label never ends with a hyphen (the rule creation uses too).
    assert!(!valid_tenant_name("acme-"));
}

#[test]
fn logins_accept_every_name_creation_accepts() {
    // The one shared rule: what `sprigctl tenant create` takes, a login
    // can name, on a shared endpoint, by SNI and in an API key.
    for name in ["ab", "a1", "pune-2", &"x".repeat(63)] {
        assert!(sprig_common::names::is_tenant_name(name));
        let p = parse_login(&format!("{name}.alice"), None, &sfx(), 32).unwrap();
        assert_eq!(p.tenant, name);
        let sni = format!("{name}.db.example.com");
        let p = parse_login("alice", Some(&sni), &sfx(), 32).unwrap();
        assert_eq!(p.tenant, name);
        let key = format!("sprig_{name}_{}_{}", "a".repeat(12), "0".repeat(64));
        assert_eq!(crate::apikey::parse_key(&key).unwrap().tenant, name);
    }
    for name in ["a", "acme-", "Acme"] {
        assert!(parse_login(&format!("{name}.alice"), None, &sfx(), 32).is_none());
        // Host names are case-insensitive: SNI `Acme.` is tenant `acme`.
        let sni = format!("{name}.db.example.com");
        assert_eq!(
            parse_login("alice", Some(&sni), &sfx(), 32).is_none(),
            name != "Acme"
        );
        let key = format!("sprig_{name}_{}_{}", "a".repeat(12), "0".repeat(64));
        assert!(crate::apikey::parse_key(&key).is_none());
    }
}

#[test]
fn dotted_names_on_shared_endpoints() {
    let p = parse_login("acme.alice", None, &sfx(), 32).unwrap();
    assert_eq!(
        (p.tenant.as_str(), p.user.as_str(), p.form),
        ("acme", "alice", LoginForm::Qualified)
    );
    // Split at the first dot: the user may contain dots.
    let p = parse_login("acme.alice.b", None, &[], 32).unwrap();
    assert_eq!(p.user, "alice.b");
    for bad in ["alice", "acme.", ".alice", "ACME.alice", "a.alice", ""] {
        assert!(parse_login(bad, None, &sfx(), 32).is_none(), "{bad}");
    }
    // The persona limit applies to the user part.
    assert!(parse_login(&format!("acme.{}", "u".repeat(32)), None, &[], 32).is_some());
    assert!(parse_login(&format!("acme.{}", "u".repeat(33)), None, &[], 32).is_none());
    assert!(parse_login(&format!("acme.{}", "u".repeat(80)), None, &[], 80).is_some());
}

#[test]
fn sni_names_the_tenant_on_tenant_endpoints() {
    let p = parse_login("alice", Some("acme.db.example.com"), &sfx(), 32).unwrap();
    assert_eq!(
        (p.tenant.as_str(), p.user.as_str(), p.form),
        ("acme", "alice", LoginForm::Plain)
    );
    // SNI wins over a dotted name; the whole name is the user.
    let p = parse_login("other.alice", Some("ACME.DB.example.com."), &sfx(), 32).unwrap();
    assert_eq!(
        (p.tenant.as_str(), p.user.as_str()),
        ("acme", "other.alice")
    );
    // More than one label before the suffix, or a bad label: no tenant.
    assert!(parse_login("alice", Some("x.acme.db.example.com"), &sfx(), 32).is_none());
    assert!(parse_login("alice", Some("a_b.db.example.com"), &sfx(), 32).is_none());
    // The bare suffix, or an SNI matching no suffix: the shared endpoint.
    let p = parse_login("acme.alice", Some("db.example.com"), &sfx(), 32).unwrap();
    assert_eq!(p.form, LoginForm::Qualified);
    let p = parse_login("acme.alice", Some("other.host"), &sfx(), 32).unwrap();
    assert_eq!(p.tenant, "acme");
    // A suffix must match whole labels.
    assert!(parse_login("alice", Some("acmexdb.example.com"), &sfx(), 32).is_none());
}

async fn put(store: &MemStore, key: &str, value: serde_json::Value) {
    store
        .txn(Txn {
            then: vec![Op::Put {
                key: store.tier().key(key).unwrap(),
                value: serde_json::to_vec(&value).unwrap(),
                lease: None,
            }],
            ..Txn::default()
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn meta_directory_reads_names_states_and_ancestors() {
    let store = Arc::new(MemStore::new(Tier::Global));
    put(
        &store,
        "tenant-names/platform",
        json!({"v": 1, "tenant_id": 1}),
    )
    .await;
    put(
        &store,
        "tenant-names/acme",
        json!({"v": 1, "tenant_id": 42}),
    )
    .await;
    put(&store, "tenant-names/pune", json!(77)).await;
    put(&store, "tenant-names/new", json!({"v": 1, "tenant_id": 90})).await;
    put(
        &store,
        "tenants/1",
        json!({"v": 1, "name": "platform", "state": "active", "path": "/1/"}),
    )
    .await;
    put(
        &store,
        "tenants/42",
        json!({"v": 1, "name": "acme", "state": "active", "path": "/1/42/"}),
    )
    .await;
    put(
        &store,
        "tenants/77",
        json!({"v": 1, "name": "pune", "state": "migrating", "path": "/1/42/77/"}),
    )
    .await;
    put(
        &store,
        "tenants/90",
        json!({"v": 1, "name": "new", "state": "creating", "path": "/1/90/"}),
    )
    .await;
    let dir = MetaTenants::new(store.clone(), Duration::ZERO);

    let acme = dir.lookup("acme").await.unwrap().unwrap();
    assert_eq!(
        acme,
        TenantInfo {
            id: TenantId(42),
            name: "acme".into(),
            can_log_in: true,
            suspended: false
        }
    );
    let pune = dir.lookup("pune").await.unwrap().unwrap();
    assert!(pune.can_log_in && !pune.suspended);
    assert!(!dir.lookup("new").await.unwrap().unwrap().can_log_in);
    assert!(dir.lookup("nobody").await.unwrap().is_none());

    // Suspending an ancestor suspends the subtree.
    put(
        &store,
        "tenants/42",
        json!({"v": 1, "name": "acme", "state": "active", "path": "/1/42/", "suspended": true}),
    )
    .await;
    assert!(dir.lookup("pune").await.unwrap().unwrap().suspended);
    assert!(dir.lookup("acme").await.unwrap().unwrap().suspended);
    assert!(!dir.lookup("platform").await.unwrap().unwrap().suspended);

    // A cached answer lasts its TTL.
    let cached = MetaTenants::new(store.clone(), Duration::from_secs(60));
    assert!(cached.lookup("acme").await.unwrap().unwrap().suspended);
    put(
        &store,
        "tenants/42",
        json!({"v": 1, "name": "acme", "state": "active", "path": "/1/42/"}),
    )
    .await;
    assert!(cached.lookup("acme").await.unwrap().unwrap().suspended);
    assert!(!dir.lookup("acme").await.unwrap().unwrap().suspended);

    // An unreachable tier is an error, not "no tenant".
    store.set_unavailable(true);
    assert!(dir.lookup("acme").await.is_err());
}
