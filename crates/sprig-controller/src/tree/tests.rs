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
    for ok in ["acme", "ab", "pune-2", &"x".repeat(63)] {
        assert!(validate_name(ok).is_ok(), "{ok}");
    }
    for bad in [
        "",
        // One letter: logins need two (decision bugfix-interim), so the
        // same rule refuses it here.
        "a",
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
            &d.create(&new("xx", Some(TenantId(99))), 64, now())
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
        code(
            &d.create(&new("yy", Some(rid)), 64, now())
                .await
                .unwrap_err()
        ),
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
        code(
            &d.create(&new("yy", Some(rid)), 64, now())
                .await
                .unwrap_err()
        ),
        39509
    );
    // Ids are never reused: a failed create took none.
    let z = d.create(&new("zz", None), 64, now()).await.unwrap();
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

/// `tenant.id_min..=tenant.id_max`: a counter below the range starts at
/// its lowest id, and a used-up or empty range is a clear 39080.
#[tokio::test]
async fn ids_come_from_the_deployments_range() {
    let d = dir();
    let range = IdRange { min: 500, max: 501 };
    let a = d
        .create_in(&new("aa", None), 64, range, now())
        .await
        .unwrap();
    assert_eq!(a.body().tenant_id, TenantId(500));
    let b = d
        .create_in(&new("bb", None), 64, range, now())
        .await
        .unwrap();
    assert_eq!(b.body().tenant_id, TenantId(501));
    let e = d
        .create_in(&new("cc", None), 64, range, now())
        .await
        .unwrap_err();
    assert_eq!(code(&e), 39080, "{e}");
    assert!(
        e.message.contains("500..=501") && e.message.contains("502"),
        "{e}"
    );
    assert!(d.by_name("cc").await.unwrap().is_none(), "nothing written");
    // A wider range continues from the counter.
    let c = d
        .create_in(&new("cc", None), 64, IdRange { min: 500, max: 600 }, now())
        .await
        .unwrap();
    assert_eq!(c.body().tenant_id, TenantId(502));
    let e = d
        .create_in(&new("ee", None), 64, IdRange { min: 9, max: 8 }, now())
        .await
        .unwrap_err();
    assert_eq!(code(&e), 39080, "{e}");
    assert!(IdRange::ALL.contains(1) && !IdRange::ALL.contains(0));
}

#[tokio::test]
async fn suspend_and_resume_set_the_flag_with_its_reason() {
    let d = dir();
    let root = d.create(&new("root", None), 64, now()).await.unwrap();
    let rid = root.body().tenant_id;
    activate(&d, rid).await;
    let child = d.create(&new("child", Some(rid)), 64, now()).await.unwrap();
    let cid = child.body().tenant_id;
    activate(&d, cid).await;
    let s = d.set_suspended(rid, Some("unpaid"), now()).await.unwrap();
    let b = s.body();
    assert!(b.suspended);
    assert_eq!(b.suspended_reason.as_deref(), Some("unpaid"));
    assert_eq!(b.suspended_at.as_deref(), Some("2025-11-08T00:00:00Z"));
    // Again with the same reason: unchanged (same revision).
    let again = d.set_suspended(rid, Some("unpaid"), now()).await.unwrap();
    assert_eq!(again.mod_revision, s.mod_revision);
    let below = d.descendants(rid).await.unwrap();
    assert_eq!(
        below.iter().map(|e| e.body().tenant_id).collect::<Vec<_>>(),
        vec![cid]
    );
    let r = d.set_suspended(rid, None, now()).await.unwrap();
    assert!(!r.body().suspended);
    assert!(r.body().suspended_reason.is_none() && r.body().suspended_at.is_none());
    // A tenant being deleted can't be suspended.
    d.mark_deleting(cid).await.unwrap();
    assert_eq!(
        code(&d.set_suspended(cid, Some("x"), now()).await.unwrap_err()),
        39072
    );
}
