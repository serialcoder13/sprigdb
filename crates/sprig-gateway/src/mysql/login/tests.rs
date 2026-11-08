//! The gateway's login flow against hashes a MySQL 8.4.11 server made
//! (`HEX(authentication_string)` for `secret`, from sprig-auth's vectors)
//! and golden client scrambles (computed independently with Python's
//! hashlib for the nonce `0123456789abcdefghij`).

use std::path::Path;
use std::time::Duration;

use sprig_auth::StaticAccounts;
use sprig_auth::accounts::parse_accounts;
use sprig_common::time::ManualClock;

use super::*;

const NONCE: &[u8; NONCE_LEN] = b"0123456789abcdefghij";
/// `caching_sha2_password` for `secret` at 5000 rounds, from MySQL 8.4.11.
const MYSQL_SHA2_SECRET: &str = "0x2441243030352461054B524B4E691D0B2578570D1B79530D08384A714E5655304852656B74683135346F2E52555868477930305A4B5A4A494D455555626546566B7254436C2E";
/// `mysql_native_password` for `secret`.
const MYSQL_NATIVE_SECRET: &str = "*14E65567ABDB5135D0CFD9A70B3032C179A49EE7";
/// `SHA1(pw) XOR SHA1(nonce || SHA1(SHA1(pw)))`.
const GOLDEN_NATIVE: &str = "f5cac3f3b3df2133feb57db682c71e6ed433a988";
/// `SHA256(pw) XOR SHA256(SHA256(SHA256(pw)) || nonce)`.
const GOLDEN_SHA2: &str = "7e4adda2f2d5ef09b4147518becb0cee2939376203650a8c2bb2815959364bd0";

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn deps() -> Arc<LoginDeps> {
    let text = format!(
        r#"
[[account]]
user = "app"
caching_sha2_password = "{MYSQL_SHA2_SECRET}"
mysql_native_password = "{MYSQL_NATIVE_SECRET}"
cluster = "dc-a-c1"
backend_user = "root"
backend_credentials = "env:SPRIG_TEST_BACKEND"

[[account]]
user = "nat"
mysql_native_password = "{MYSQL_NATIVE_SECRET}"
cluster = "dc-a-c1"
backend_user = "root"
backend_credentials = "env:SPRIG_TEST_BACKEND"
"#
    );
    let accounts = parse_accounts(&text, Path::new(".")).unwrap();
    let cache = FastCache::new(Arc::new(ManualClock::new()), Duration::from_secs(3600));
    Arc::new(LoginDeps::new(
        Arc::new(StaticAccounts::new(accounts)),
        Arc::new(cache),
    ))
}

fn login(deps: &Arc<LoginDeps>, user: &str) -> AccountLogin {
    AccountLogin::new(
        Arc::clone(deps),
        Persona::MySql84,
        user.as_bytes(),
        "10.0.0.7:5000".parse().unwrap(),
    )
}

#[tokio::test]
async fn golden_native_scramble() {
    let d = deps();
    let mut l = login(&d, "app");
    assert_eq!(l.method(Some("mysql_native_password")), Method::Native);
    assert!(l.check_native(NONCE, &unhex(GOLDEN_NATIVE)));
    assert!(!l.check_native(NONCE, &[0; 20]));
    assert!(!l.check_native(b"abcdefghij0123456789", &unhex(GOLDEN_NATIVE)));
    assert_eq!(
        l.decide(Outcome::Verified, AuthPath::Native).await,
        AuthVerdict::Accept
    );
}

#[tokio::test]
async fn golden_sha2_scramble_takes_the_fast_path_after_a_full_login() {
    let d = deps();
    let mut l = login(&d, "app");
    assert_eq!(l.method(Some("caching_sha2_password")), Method::CachingSha2);
    // Cold cache: the fast path misses, the full path verifies and fills.
    assert!(!l.check_fast(NONCE, &unhex(GOLDEN_SHA2)));
    assert!(!l.check_password(Zeroizing::new(b"wrong".to_vec())).await);
    assert!(d.fast_cache.is_empty());
    assert!(l.check_password(Zeroizing::new(b"secret".to_vec())).await);
    assert_eq!(d.fast_cache.len(), 1);
    // The next login's golden scramble matches the cached stage 2.
    let mut l = login(&d, "app");
    assert!(l.check_fast(NONCE, &unhex(GOLDEN_SHA2)));
    assert!(!l.check_fast(NONCE, &[0; 32]));
    assert_eq!(l.principal().map(|p| p.user), Some("app".to_owned()));
}

#[tokio::test]
async fn native_only_accounts_switch_to_native() {
    let d = deps();
    let l = login(&d, "nat");
    assert_eq!(l.method(Some("caching_sha2_password")), Method::Native);
    assert_eq!(l.method(Some("mysql_clear_password")), Method::ClearText);
}

#[tokio::test]
async fn unknown_users_look_like_sha2_accounts_and_fail_with_1045() {
    let d = deps();
    let mut l = login(&d, "ghost");
    assert_eq!(l.method(Some("mysql_native_password")), Method::CachingSha2);
    assert!(!l.check_fast(NONCE, &unhex(GOLDEN_SHA2)));
    assert!(!l.check_native(NONCE, &unhex(GOLDEN_NATIVE)));
    // The decoy hashes and fails even with the "right" password.
    assert!(!l.check_password(Zeroizing::new(b"secret".to_vec())).await);
    let AuthVerdict::Refuse(e) = l.decide(Outcome::Verified, AuthPath::Full).await else {
        panic!("an unknown user was accepted");
    };
    assert_eq!(e.code, 1045);
    assert_eq!(
        e.message,
        "Access denied for user 'ghost'@'10.0.0.7' (using password: YES)"
    );
    // A real user with a wrong password gets the same message shape.
    let mut real = login(&d, "app");
    let _ = real.check_password(Zeroizing::new(b"wrong".to_vec())).await;
    let AuthVerdict::Refuse(e2) = real.decide(Outcome::WrongCredentials, AuthPath::Full).await
    else {
        panic!("a wrong password was accepted");
    };
    assert_eq!(e2.message.replace("app", "ghost"), e.message);
}

#[tokio::test]
async fn overlong_names_are_unknown() {
    let d = deps();
    let long = "a".repeat(33);
    let l = AccountLogin::new(
        d,
        Persona::MySql84,
        long.as_bytes(),
        "10.0.0.7:1".parse().unwrap(),
    );
    assert!(l.account().is_none());
}

#[tokio::test]
async fn rsa_refusal_says_to_use_tls() {
    let d = deps();
    let mut l = login(&d, "app");
    let AuthVerdict::Refuse(e) = l.decide(Outcome::NeedsTls, AuthPath::Rsa).await else {
        panic!("accepted");
    };
    assert_eq!(e.code, 1045);
    assert!(e.message.contains("connect with TLS"));
}
