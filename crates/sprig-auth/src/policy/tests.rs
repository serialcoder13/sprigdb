#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use sprig_settings::{Bounds, Value};

use super::*;
use crate::cache::TenantAuth;
use crate::cache::tests::{at, row, sha2};
use crate::mysql::tests::snapshot;

const T: TenantId = TenantId(42);

/// Fixed values and bounds per key.
#[derive(Default)]
struct Policy {
    values: HashMap<&'static str, Value>,
    bounds: HashMap<&'static str, Bounds>,
}

impl PolicySettings for Policy {
    fn resolve(&self, key: &'static str, _c: &TenantCatalog) -> Option<Value> {
        self.values.get(key).cloned()
    }
    fn bounds(&self, key: &'static str, _c: &TenantCatalog) -> Option<Bounds> {
        self.bounds.get(key).cloned()
    }
}

fn catalog() -> Arc<TenantCatalog> {
    snapshot(T, TenantAuth::default(), 1)
}

#[test]
fn strength_levels_follow_validate_password() {
    let ok = |s: Strength, pw: &str| s.accepts(pw.as_bytes(), "alice");
    assert!(ok(Strength::Off, "a"));
    assert!(!ok(Strength::Low, "short"));
    assert!(ok(Strength::Low, "longenough"));
    assert!(!ok(Strength::Low, "alice"), "the user name");
    assert!(!ok(Strength::Low, "ecila"), "the user name reversed");
    assert!(!ok(Strength::Medium, "longenough"));
    assert!(!ok(Strength::Medium, "Longenough1"), "no special character");
    assert!(ok(Strength::Medium, "Longenough1!"));
    assert!(
        ok(Strength::Medium, "Alice-pw-2026"),
        "medium allows the name inside"
    );
    assert!(!ok(Strength::Strong, "Alice-pw-2026"), "strong doesn't");
    assert!(ok(Strength::Strong, "Correct-Horse-9"));
    assert_eq!(Strength::parse("MEDIUM"), Strength::Medium);
    assert_eq!(Strength::parse("nonsense"), Strength::Off);
}

#[test]
fn lock_lengths() {
    let now = at();
    assert_eq!(LockLength::from_minutes(0), LockLength::Off);
    assert_eq!(LockLength::from_minutes(-1), LockLength::Unbounded);
    assert_eq!(LockLength::Off.until(now), None);
    assert_eq!(
        LockLength::Minutes(2).until(now),
        Some(now + Duration::from_secs(120))
    );
    assert_eq!(
        LockLength::Unbounded.until(now),
        Some(from_micros(UNBOUNDED_LOCK_MICROS))
    );
}

#[test]
fn lockout_comes_from_the_account_else_the_settings() {
    let c = catalog();
    let mut p = Policy::default();
    let r = row(1, "alice", "%", 1);
    assert!(!LockoutPolicy::of(&r, &p, &c).tracking(), "off by default");
    p.values.insert(FAILED_LOGIN_ATTEMPTS.key, Value::Int(3));
    assert!(
        !LockoutPolicy::of(&r, &p, &c).tracking(),
        "needs a lock time too"
    );
    p.values.insert(PASSWORD_LOCK_MINUTES.key, Value::Int(15));
    assert_eq!(
        LockoutPolicy::of(&r, &p, &c),
        LockoutPolicy {
            attempts: 3,
            lock: LockLength::Minutes(15)
        }
    );
    let mut r2 = r.clone();
    r2.failed_login_attempts = Some(5);
    r2.password_lock_minutes = Some(-1);
    assert_eq!(
        LockoutPolicy::of(&r2, &p, &c),
        LockoutPolicy {
            attempts: 5,
            lock: LockLength::Unbounded
        }
    );
}

#[tokio::test]
async fn memory_counters_lock_and_unlock() {
    let s = MemLoginState::new();
    let u = UserId(1);
    let now = at();
    let pol = LockoutPolicy {
        attempts: 3,
        lock: LockLength::Minutes(1),
    };
    for n in 1..3 {
        let o = s.record_failure(T, u, pol, now).await.unwrap();
        assert_eq!((o.failures, o.locked_until), (n, None));
    }
    // A success resets.
    s.record_success(T, u, now).await.unwrap();
    for _ in 0..2 {
        s.record_failure(T, u, pol, now).await.unwrap();
    }
    let o = s.record_failure(T, u, pol, now).await.unwrap();
    let until = now + Duration::from_secs(60);
    assert_eq!(o.locked_until, Some(until));
    // While locked, failures don't count.
    let o = s.record_failure(T, u, pol, now).await.unwrap();
    assert_eq!((o.failures, o.locked_until), (3, None));
    // Not yet expired: no unlock.
    assert!(!s.unlock_expired(T, u, until, now).await.unwrap());
    let later = until + Duration::from_secs(1);
    // A failure after the lock passed starts counting again.
    let o = s.record_failure(T, u, pol, later).await.unwrap();
    assert_eq!((o.failures, o.locked_until), (1, None));
}

#[tokio::test]
async fn expired_locks_unlock_once() {
    let s = MemLoginState::new();
    let u = UserId(1);
    let now = at();
    let pol = LockoutPolicy {
        attempts: 1,
        lock: LockLength::Minutes(1),
    };
    let until = s
        .record_failure(T, u, pol, now)
        .await
        .unwrap()
        .locked_until
        .unwrap();
    let later = until + Duration::from_secs(1);
    assert!(s.unlock_expired(T, u, until, later).await.unwrap());
    assert!(
        !s.unlock_expired(T, u, until, later).await.unwrap(),
        "a second gateway finds it cleared"
    );
    assert_eq!(s.row(T, u).unwrap().consecutive_failures, 0);
}

#[test]
fn history_and_reuse_interval() {
    let now = at() + Duration::from_secs(10 * 86_400);
    let mut v = Vec::new();
    // Generations 1 (oldest) .. 4 (current), one a day before `now`.
    for (g, pw) in [(1, "one"), (2, "two"), (3, "three"), (4, "four")] {
        let mut r = sha2(
            g,
            1,
            if g == 4 { Slot::Primary } else { Slot::History },
            pw.as_bytes(),
        );
        r.generation = g;
        r.created_at = now - Duration::from_secs((5 - g) * 86_400);
        v.push(r);
    }
    let rules = |history, reuse_days| PasswordRules {
        history,
        reuse_days,
        ..PasswordRules::default()
    };
    assert!(!rules(0, 0).reused(b"four", &v, now), "no rules, no check");
    assert!(
        rules(1, 0).reused(b"four", &v, now),
        "history 1: the current"
    );
    assert!(!rules(1, 0).reused(b"three", &v, now));
    assert!(rules(2, 0).reused(b"three", &v, now));
    assert!(!rules(3, 0).reused(b"one", &v, now));
    assert!(rules(4, 0).reused(b"one", &v, now));
    // Reuse interval: set within the last 3 days (generations 3 and 4).
    assert!(rules(0, 3).reused(b"three", &v, now));
    assert!(!rules(0, 3).reused(b"two", &v, now));
    assert!(!rules(4, 0).reused(b"five", &v, now));
    assert_eq!(rules(5, 0).keep_history(), 5);
    assert_eq!(rules(2, 30).keep_history(), 30);
}

#[test]
fn rules_come_from_the_account_else_the_settings() {
    let c = catalog();
    let mut p = Policy::default();
    p.values
        .insert(PASSWORD_POLICY.key, Value::Str("medium".into()));
    p.values.insert(PASSWORD_HISTORY.key, Value::Int(3));
    p.values
        .insert(PASSWORD_REQUIRE_CURRENT.key, Value::Bool(true));
    let mut r = row(1, "alice", "%", 1);
    let got = PasswordRules::of(&r, &p, &c);
    assert_eq!(
        got,
        PasswordRules {
            strength: Strength::Medium,
            history: 3,
            reuse_days: 0,
            require_current: true
        }
    );
    r.password_history = Some(0);
    r.require_current = RequireCurrent::Optional;
    let got = PasswordRules::of(&r, &p, &c);
    assert_eq!((got.history, got.require_current), (0, false));
}

#[test]
fn persona_errors() {
    use sprig_common::ErrorKind;
    let code = |e: SprigError| match e.kind {
        ErrorKind::Standard { errno, .. } => errno,
        _ => 0,
    };
    let my = Persona::MySql84;
    let maria = Persona::MariaDb;
    assert_eq!(code(PasswordProblem::Weak.error(my, "a", "%")), 1819);
    assert_eq!(code(PasswordProblem::Reused.error(my, "a", "%")), 3638);
    assert_eq!(code(PasswordProblem::Reused.error(maria, "a", "%")), 1819);
    assert_eq!(
        code(PasswordProblem::CurrentMissing.error(my, "a", "%")),
        3892
    );
    assert_eq!(
        code(PasswordProblem::CurrentWrong.error(my, "a", "%")),
        3891
    );
    assert_eq!(
        PasswordProblem::Reused.error(my, "alice", "%").message,
        "Cannot use these credentials for 'alice@%' because they contradict the password history policy"
    );
}

#[tokio::test]
async fn new_password_checks_in_order() {
    let now = at();
    let r = row(1, "alice", "%", 1);
    let v = vec![sha2(1, 1, Slot::Primary, b"Current-Pw-1")];
    let rules = PasswordRules {
        strength: Strength::Medium,
        history: 1,
        reuse_days: 0,
        require_current: true,
    };
    let change =
        |pw: &'static [u8], current: Option<&'static [u8]>, actor: Option<Actor>| PasswordChange {
            password: pw,
            current,
            retain_current: false,
            discard_old: false,
            actor,
            persona: Persona::MySql84,
        };
    let check = async |c: PasswordChange<'_>| check_new_password(&rules, &r, &v, &c, now).await;
    assert_eq!(
        check(change(b"weak", None, None)).await,
        Some(PasswordProblem::Weak)
    );
    assert_eq!(
        check(change(b"New-Pw-2026", None, None)).await,
        Some(PasswordProblem::CurrentMissing)
    );
    assert_eq!(
        check(change(b"New-Pw-2026", Some(b"nope"), None)).await,
        Some(PasswordProblem::CurrentWrong)
    );
    assert_eq!(
        check(change(b"Current-Pw-1", Some(b"Current-Pw-1"), None)).await,
        Some(PasswordProblem::Reused)
    );
    assert_eq!(
        check(change(b"New-Pw-2026", Some(b"Current-Pw-1"), None)).await,
        None
    );
    // An administrator's reset needs no current password.
    let admin = Some(Actor::operator(TenantId(1), "root"));
    assert_eq!(check(change(b"New-Pw-2026", None, admin)).await, None);
    // MariaDB has no REPLACE clause: REQUIRE CURRENT doesn't apply on 3307.
    let mut c = change(b"New-Pw-2026", None, None);
    c.persona = Persona::MariaDb;
    assert_eq!(check(c).await, None);
}

#[test]
fn account_clauses_stay_within_the_tree_bounds() {
    let c = catalog();
    let mut p = Policy::default();
    p.bounds.insert(
        FAILED_LOGIN_ATTEMPTS.key,
        Bounds {
            max: Some(10.0),
            ..Bounds::default()
        },
    );
    let ok = AccountClauses {
        failed_login_attempts: Some(Some(4)),
        ..AccountClauses::default()
    };
    ok.validate(&p, &c).unwrap();
    let bad = AccountClauses {
        failed_login_attempts: Some(Some(12)),
        ..AccountClauses::default()
    };
    let e = bad.validate(&p, &c).unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::SettingOutOfBounds));
    // Clearing a clause is always allowed.
    AccountClauses {
        failed_login_attempts: Some(None),
        ..AccountClauses::default()
    }
    .validate(&p, &c)
    .unwrap();
    // A locked key: only the resolved value.
    p.bounds.insert(PASSWORD_HISTORY.key, Bounds::locked());
    p.values.insert(PASSWORD_HISTORY.key, Value::Int(5));
    let five = AccountClauses {
        password_history: Some(Some(5)),
        ..AccountClauses::default()
    };
    five.validate(&p, &c).unwrap();
    let six = AccountClauses {
        password_history: Some(Some(6)),
        ..AccountClauses::default()
    };
    assert!(six.validate(&p, &c).is_err());
    let mut r = row(1, "alice", "%", 1);
    AccountClauses {
        failed_login_attempts: Some(Some(4)),
        password_lock_minutes: Some(Some(-1)),
        password_lifetime: Some(Some(30)),
        require_current: Some(RequireCurrent::Yes),
        ..AccountClauses::default()
    }
    .apply(&mut r);
    assert_eq!(
        (
            r.failed_login_attempts,
            r.password_lock_minutes,
            r.password_lifetime,
            r.require_current
        ),
        (Some(4), Some(-1), Some(30), RequireCurrent::Yes)
    );
    let _ = SystemTime::now();
}
