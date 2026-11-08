//! The tenant tree in the directory (chapter 23): names, ids, paths and
//! child counts, the parent checks, the delete guard and removal, on
//! `MemStore`.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

use std::time::{Duration, UNIX_EPOCH};

use sprig_meta::{MemStore, Tier};

use super::*;

fn dir() -> Directory {
    Directory::new(Arc::new(MemStore::new(Tier::Global)))
}

fn dc() -> DcId {
    DcId::new("mum1").unwrap()
}

fn new(name: &str, parent: Option<TenantId>) -> NewTenant {
    NewTenant {
        name: name.to_owned(),
        parent,
        home_dc: dc(),
        placement_hint: None,
    }
}

fn now() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_762_560_000)
}

async fn activate(d: &Directory, id: TenantId) {
    d.update(id, |e| {
        e.state = TenantState::Active;
        Ok(true)
    })
    .await
    .unwrap();
}

fn code(e: &SprigError) -> u16 {
    e.protocol().mysql_errno
}

#[test]
fn names_are_dns_labels() {
    for ok in ["acme", "a", "pune-2", &"x".repeat(63)] {
        assert!(validate_name(ok).is_ok(), "{ok}");
    }
    for bad in [
        "",
        "Acme",
        "2acme",
        "acme-",
        "ac_me",
        "ac.me",
        &"x".repeat(64),
        "test me",
    ] {
        let e = validate_name(bad).unwrap_err();
        assert_eq!(code(&e), 39792, "{bad}");
    }
}

#[tokio::test]
async fn create_builds_the_tree() {
    let d = dir();
    let platform = d.create(&new("platform", None), 64, now()).await.unwrap();
    let p = platform.body().clone();
    assert_eq!(p.tenant_id, TenantId(1));
    assert_eq!((p.path.as_str(), p.depth, p.parent()), ("/1/", 0, None));
    assert_eq!(p.state, TenantState::Creating);
    assert_eq!(p.created_at, "2025-11-08T00:00:00Z");
    // A parent must be active first.
    let e = d
        .create(&new("acme", Some(p.tenant_id)), 64, now())
        .await
        .unwrap_err();
    assert_eq!(code(&e), 39072);
    activate(&d, p.tenant_id).await;
    let acme = d
        .create(&new("acme", Some(p.tenant_id)), 64, now())
        .await
        .unwrap();
    activate(&d, acme.body().tenant_id).await;
    let pune = d
        .create(&new("pune", Some(acme.body().tenant_id)), 64, now())
        .await
        .unwrap();
    let c = pune.body();
    assert_eq!(c.tenant_id, TenantId(3));
    assert_eq!((c.path.as_str(), c.depth), ("/1/2/3/", 2));
    assert!(c.path_contains(TenantId(2)) && !c.path_contains(TenantId(4)));
    assert_eq!(
        d.get(TenantId(1))
            .await
            .unwrap()
            .unwrap()
            .body()
            .child_count,
        1
    );
    assert_eq!(
        d.get(TenantId(2))
            .await
            .unwrap()
            .unwrap()
            .body()
            .child_count,
        1
    );
    // The ancestry walks the parent links, root first.
    let a = d.ancestry(c).await.unwrap();
    let names: Vec<_> = a.iter().map(|(r, _)| r.name.as_str()).collect();
    assert_eq!(names, ["platform", "acme"]);
    assert!(d.ancestry(&p).await.unwrap().is_empty());
    // Names and the list.
    assert_eq!(
        d.require("pune").await.unwrap().body().tenant_id,
        TenantId(3)
    );
    assert!(d.by_name("nobody").await.unwrap().is_none());
    assert!(d.by_name("Not A Name").await.unwrap().is_none());
    assert_eq!(code(&d.require("nobody").await.unwrap_err()), 39072);
    let all: Vec<_> = d
        .list()
        .await
        .unwrap()
        .iter()
        .map(|e| e.body().tenant_id.0)
        .collect();
    assert_eq!(all, [1, 2, 3]);
    assert_eq!(d.tenant_of(&d.key(TenantId(3)).unwrap()), Some(TenantId(3)));
}

#[tokio::test]
async fn parents_are_checked_and_names_are_unique() {
    let d = dir();
    let root = d.create(&new("root", None), 1, now()).await.unwrap();
    let rid = root.body().tenant_id;
    activate(&d, rid).await;
    assert_eq!(
        code(&d.create(&new("root", None), 64, now()).await.unwrap_err()),
        39360
    );
    assert_eq!(
        code(
            &d.create(&new("x", Some(TenantId(99))), 64, now())
                .await
                .unwrap_err()
        ),
        39072
    );
    let child = d.create(&new("child", Some(rid)), 1, now()).await.unwrap();
    activate(&d, child.body().tenant_id).await;
    // max_depth 1: no grandchildren.
    let e = d
        .create(&new("grand", Some(child.body().tenant_id)), 1, now())
        .await
        .unwrap_err();
    assert_eq!(code(&e), 39508);
    // A suspended or moving parent takes no children.
    d.update(rid, |e| {
        e.suspended = true;
        Ok(true)
    })
    .await
    .unwrap();
    assert_eq!(
        code(&d.create(&new("y", Some(rid)), 64, now()).await.unwrap_err()),
        39303
    );
    d.update(rid, |e| {
        e.suspended = false;
        e.tree_lock = Some("m1".into());
        Ok(true)
    })
    .await
    .unwrap();
    assert_eq!(
        code(&d.create(&new("y", Some(rid)), 64, now()).await.unwrap_err()),
        39509
    );
    // Ids are never reused: a failed create took none.
    let z = d.create(&new("z", None), 64, now()).await.unwrap();
    assert_eq!(z.body().tenant_id, TenantId(3));
}

#[tokio::test]
async fn delete_needs_a_leaf_and_frees_the_name_on_removal() {
    let d = dir();
    let root = d.create(&new("root", None), 64, now()).await.unwrap();
    let rid = root.body().tenant_id;
    // Creating: not deletable yet.
    assert_eq!(code(&d.mark_deleting(rid).await.unwrap_err()), 39072);
    activate(&d, rid).await;
    let leaf = d.create(&new("leaf", Some(rid)), 64, now()).await.unwrap();
    let lid = leaf.body().tenant_id;
    activate(&d, lid).await;
    assert_eq!(code(&d.mark_deleting(rid).await.unwrap_err()), 39362);
    // Removing needs `deleting`.
    assert_eq!(code(&d.remove(&leaf).await.unwrap_err()), 39072);
    let gone = d.mark_deleting(lid).await.unwrap();
    assert_eq!(gone.body().state, TenantState::Deleting);
    assert_eq!(d.get(rid).await.unwrap().unwrap().body().child_count, 0);
    // Marking again is a no-op; the name still resolves until removal.
    assert_eq!(
        d.mark_deleting(lid).await.unwrap().body().state,
        TenantState::Deleting
    );
    assert!(d.by_name("leaf").await.unwrap().is_some());
    let cur = d.get(lid).await.unwrap().unwrap();
    d.remove(&cur).await.unwrap();
    assert!(d.get(lid).await.unwrap().is_none());
    assert!(d.by_name("leaf").await.unwrap().is_none());
    // The name is free; the id isn't reused.
    let again = d.create(&new("leaf", Some(rid)), 64, now()).await.unwrap();
    assert_eq!(again.body().tenant_id, TenantId(3));
    // A failed tenant is deletable too.
    d.update(again.body().tenant_id, |e| {
        e.state = TenantState::Failed;
        Ok(true)
    })
    .await
    .unwrap();
    d.mark_deleting(again.body().tenant_id).await.unwrap();
}

#[tokio::test]
async fn entries_keep_fields_this_build_does_not_know() {
    let store = Arc::new(MemStore::new(Tier::Global));
    let d = Directory::new(store.clone());
    let t = d.create(&new("acme", None), 64, now()).await.unwrap();
    let key = d.key(t.body().tenant_id).unwrap();
    let mut v: serde_json::Value = serde_json::from_slice(
        &store
            .get(&key, Consistency::Linearizable)
            .await
            .unwrap()
            .unwrap()
            .value,
    )
    .unwrap();
    v["residency"] = serde_json::json!({"pinned": "mum1"});
    store
        .txn(Txn {
            then: vec![Op::Put {
                key: key.clone(),
                value: serde_json::to_vec(&v).unwrap(),
                lease: None,
            }],
            ..Txn::default()
        })
        .await
        .unwrap();
    activate(&d, t.body().tenant_id).await;
    let after: serde_json::Value = serde_json::from_slice(
        &store
            .get(&key, Consistency::Linearizable)
            .await
            .unwrap()
            .unwrap()
            .value,
    )
    .unwrap();
    assert_eq!(after["residency"]["pinned"], "mum1");
    assert_eq!(after["state"], "active");
}
