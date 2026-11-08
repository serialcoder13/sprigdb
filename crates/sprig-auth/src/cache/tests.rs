#![allow(clippy::unwrap_used)]

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::*;
use crate::store::{UserSource, UserState, VerifierRow};

pub(crate) fn at() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_762_560_000)
}

pub(crate) fn row(id: u32, name: &str, host: &str, v: u64) -> UserRow {
    UserRow::new(UserId(id), name, host, UserSource::Sql, at(), v)
}

/// A verifier row for `pw` (1000 rounds: these tests check structure, not
/// cost).
pub(crate) fn sha2(vid: u64, user: u32, slot: Slot, pw: &[u8]) -> VerifierRow {
    let mut r = VerifierRow::mysql(
        UserId(user),
        VerifierKind::CachingSha2,
        slot,
        CachingSha2Verifier::create(pw, 1000).unwrap().to_bytes(),
        1,
        at(),
    );
    r.verifier_id = vid;
    r
}

#[test]
fn accounts_match_most_specific_host_first() {
    let users = vec![
        row(1, "alice", "%", 1),
        row(2, "alice", "10.%", 1),
        row(3, "alice", "10.0.0.7", 1),
        row(4, "bob", "%", 1),
    ];
    let a = TenantAuth::build(users, &[], Vec::new());
    let ids = |ip: &str| {
        a.match_account("alice", ip.parse().unwrap())
            .map(|u| u.row.user_id.0)
    };
    assert_eq!(ids("10.0.0.7"), Some(3));
    assert_eq!(ids("10.0.0.8"), Some(2));
    assert_eq!(ids("192.168.0.1"), Some(1));
    assert!(
        a.match_account("carol", "10.0.0.1".parse().unwrap())
            .is_none()
    );
    assert_eq!(a.accounts("alice").len(), 3);
    assert_eq!(a.len(), 4);
}

#[test]
fn verifiers_by_slot_without_history_or_garbage() {
    let mut bad = sha2(13, 1, Slot::Primary, b"x");
    bad.auth_string = Some(b"not a verifier".to_vec());
    let vs = vec![
        sha2(12, 1, Slot::Secondary, b"old"),
        sha2(11, 1, Slot::Primary, b"new"),
        sha2(10, 1, Slot::History, b"older"),
        bad,
    ];
    let (u, bad) = AuthUser::new(row(1, "alice", "%", 1), &vs);
    assert_eq!(bad, 1);
    assert_eq!(
        u.caching_sha2.iter().map(|v| v.id).collect::<Vec<_>>(),
        [11, 12]
    );
    assert!(u.caching_sha2[0].verifier.verify(b"new"));
    assert!(u.native.is_empty());
}

#[test]
fn incremental_apply_equals_a_full_build() {
    // Version 1: alice and bob.
    let mut users = vec![row(1, "alice", "%", 1), row(2, "bob", "%", 1)];
    let mut verifiers = vec![
        sha2(1, 1, Slot::Primary, b"a1"),
        sha2(2, 2, Slot::Primary, b"b1"),
    ];
    let v1 = TenantAuth::build(users.clone(), &verifiers, Vec::new());

    // Version 2: alice changes her password (dual), bob is dropped, a
    // second alice account appears, carol's temporary lock is loaded.
    verifiers.retain(|v| v.user_id != UserId(1) && v.user_id != UserId(2));
    let mut a2 = row(1, "alice", "%", 2);
    a2.account_locked = true;
    let mut b2 = row(2, "bob", "%", 2);
    b2.state = UserState::Dropped;
    let a3 = row(3, "alice", "10.%", 2);
    let changed_verifiers = vec![
        sha2(3, 1, Slot::Secondary, b"a1"),
        sha2(4, 1, Slot::Primary, b"a2"),
        sha2(5, 3, Slot::Primary, b"x"),
    ];
    let lock = TempLock {
        user_id: UserId(3),
        locked_until: at(),
        consecutive_failures: 3,
    };
    let v2 = v1.apply(
        vec![a2.clone(), b2.clone(), a3.clone()],
        &changed_verifiers,
        vec![lock],
    );

    users = vec![a2, b2, a3];
    verifiers.extend(changed_verifiers);
    let full = TenantAuth::build(users, &verifiers, vec![lock]);
    assert_eq!(v2, full);
    assert!(v2.user(UserId(2)).is_none());
    assert!(v2.accounts("bob").is_empty());
    assert_eq!(
        v2.accounts("alice")
            .iter()
            .map(|u| u.row.user_id.0)
            .collect::<Vec<_>>(),
        [3, 1]
    );
    assert_eq!(v2.user(UserId(1)).unwrap().caching_sha2.len(), 2);
    assert_eq!(v2.temp_lock(UserId(3)), Some(lock));
    // The previous state is unchanged (snapshots are immutable).
    assert!(v1.user(UserId(2)).is_some());
    assert!(v1.temp_lock(UserId(3)).is_none());
    assert!(v2.bytes() > 0);

    // Renaming moves the account between names.
    let mut renamed = v2.user(UserId(3)).unwrap().row.clone();
    renamed.name = "dave".into();
    let v3 = v2.apply(
        vec![renamed],
        &[sha2(5, 3, Slot::Primary, b"x")],
        Vec::new(),
    );
    assert_eq!(v3.accounts("alice").len(), 1);
    assert_eq!(v3.accounts("dave").len(), 1);
    assert!(v3.temp_lock(UserId(3)).is_none());
}
