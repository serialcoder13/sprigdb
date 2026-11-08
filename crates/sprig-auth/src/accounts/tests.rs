use std::sync::atomic::{AtomicU64, Ordering};

use super::*;
use crate::verifier::caching_sha2_scramble;

/// A fresh directory under the system temp directory, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "sprig-auth-accounts-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl std::ops::Deref for TempDir {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The verifier of `pw` at 5000 rounds, with a printable salt.
fn sha2(pw: &str) -> String {
    CachingSha2Verifier::create(pw.as_bytes(), 5000)
        .unwrap()
        .to_string()
}

fn file_with(user: &str, verifier: &str) -> String {
    format!(
        r#"
[[account]]
user = "{user}"
caching_sha2_password = "{verifier}"
cluster = "dc-a-c1"
backend_user = "root"
backend_credentials = "file:creds.toml#backend"
"#
    )
}

#[test]
fn parses_accounts_and_resolves_relative_credentials() {
    let base = TempDir::new();
    std::fs::write(
        base.join("creds.toml"),
        "[backend]\nuser = \"root\"\npassword = \"sprig-dev-root\"\ntls = \"skip_verify\"\n",
    )
    .unwrap();
    let text = format!(
        "{}\n[[account]]\nuser = \"legacy\"\nmysql_native_password = \"*14E65567ABDB5135D0CFD9A70B3032C179A49EE7\"\ncluster = \"dc-a-c2\"\nbackend_user = \"root\"\nbackend_credentials = \"env:SPRIG_TEST_UNSET_VAR\"\n\n[unrelated]\nkey = 1\n",
        file_with("app", &sha2("sprig-dev-app"))
    );
    let accounts = parse_accounts(&text, &base).unwrap();
    assert_eq!(accounts.len(), 2);
    let app = &accounts[0];
    assert!(app.verify_password(b"sprig-dev-app"));
    assert!(!app.verify_password(b"nope"));
    assert_eq!(app.backend.cluster.as_str(), "dc-a-c1");
    assert_eq!(
        app.backend.credentials.source,
        SecretSource::File(base.join("creds.toml"))
    );
    let cred = app.backend.credential().unwrap();
    assert_eq!(cred.user, "root");
    let legacy = &accounts[1];
    assert!(legacy.caching_sha2.is_none());
    assert!(legacy.verify_password(b"secret"));
    assert!(legacy.backend.credential().is_err(), "variable unset");
}

#[test]
fn mysql_hashes_import_as_hex() {
    // MySQL 8.4's hash of "secret" (salt with control characters).
    let hex = "2441243030352461054B524B4E691D0B2578570D1B79530D08384A714E5655304852656B74683135346F2E52555868477930305A4B5A4A494D455555626546566B7254436C2E";
    let accounts = parse_accounts(&file_with("m", &format!("0x{hex}")), Path::new("/")).unwrap();
    assert!(accounts[0].verify_password(b"secret"));
}

#[test]
fn invalid_files_are_refused_without_leaking() {
    let v = sha2("pw");
    let base = Path::new("/etc");
    let cases = [
        // No verifier.
        "[[account]]\nuser = \"a\"\ncluster = \"dc-a-c1\"\nbackend_user = \"r\"\nbackend_credentials = \"env:X\"\n".to_owned(),
        // Duplicate user.
        format!("{}{}", file_with("a", &v), file_with("a", &v)),
        // Bad verifier.
        file_with("a", "$A$005$tooshort"),
        // Bad cluster id.
        file_with("a", &v).replace("dc-a-c1", "DC A"),
        // Bad credentials reference.
        file_with("a", &v).replace("file:creds.toml#backend", "vault:x"),
        // Unknown field.
        file_with("a", &v).replace("backend_user", "backend_usr"),
        // Empty user.
        file_with("", &v),
        // Not TOML.
        "[[account]\n".to_owned(),
    ];
    for text in &cases {
        let e = parse_accounts(text, base).unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::ConfigInvalid), "{text}");
        assert!(!e.message.contains(&v[7..]), "{}", e.message);
    }
    assert!(parse_accounts("", base).unwrap().is_empty());
}

#[test]
fn backend_user_must_match_the_credential() {
    let base = TempDir::new();
    std::fs::write(
        base.join("creds.toml"),
        "[backend]\nuser = \"other\"\npassword = \"x\"\n",
    )
    .unwrap();
    let accounts = parse_accounts(&file_with("a", &sha2("pw")), &base).unwrap();
    let e = accounts[0].backend.credential().unwrap_err();
    assert!(e.message.contains("other"), "{}", e.message);
    assert!(!e.message.contains("password = "));
}

#[test]
fn file_accounts_follow_the_file() {
    let dir = TempDir::new();
    let path = dir.join("accounts.toml");
    let store = FileAccounts::open(&path).unwrap();
    assert!(store.is_empty(), "missing file: no accounts");
    assert!(store.lookup("app").is_none());

    std::fs::write(&path, file_with("app", &sha2("one"))).unwrap();
    assert!(matches!(
        store.poll(),
        PollOutcome::Reloaded { accounts: 1 }
    ));
    let first = store.lookup("app").unwrap();
    assert!(first.verify_password(b"one"));
    assert!(matches!(store.poll(), PollOutcome::Unchanged));

    // A broken edit keeps the previous accounts, and is reported once.
    std::fs::write(&path, "[[account]]\nuser = 1\n").unwrap();
    assert!(matches!(store.poll(), PollOutcome::Rejected(_)));
    assert!(matches!(store.poll(), PollOutcome::Unchanged));
    assert!(store.lookup("app").unwrap().verify_password(b"one"));

    // A fixed file swaps in; holders of the old Arc keep theirs.
    std::fs::write(&path, file_with("app", &sha2("two"))).unwrap();
    assert!(matches!(
        store.poll(),
        PollOutcome::Reloaded { accounts: 1 }
    ));
    assert!(store.lookup("app").unwrap().verify_password(b"two"));
    assert!(first.verify_password(b"one"));

    std::fs::remove_file(&path).unwrap();
    assert!(matches!(store.poll(), PollOutcome::Missing));
    assert!(store.lookup("app").is_none());
    assert!(matches!(store.poll(), PollOutcome::Unchanged));
}

#[test]
fn a_broken_file_at_start_is_an_error() {
    let dir = TempDir::new();
    let path = dir.join("accounts.toml");
    std::fs::write(&path, "not toml [").unwrap();
    let e = FileAccounts::open(&path).unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ConfigInvalid));
}

#[tokio::test]
async fn watcher_picks_up_changes() {
    let dir = TempDir::new();
    let path = dir.join("accounts.toml");
    let store = FileAccounts::open(&path).unwrap();
    let stop = CancellationToken::new();
    let task = store.spawn_watcher(Duration::from_millis(20), stop.clone());
    std::fs::write(&path, file_with("app", &sha2("pw"))).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while store.lookup("app").is_none() {
        assert!(tokio::time::Instant::now() < deadline, "not reloaded");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    stop.cancel();
    task.await.unwrap();
}

#[test]
fn static_accounts_and_fast_path() {
    let accounts = parse_accounts(&file_with("app", &sha2("pw")), Path::new("/")).unwrap();
    let store = StaticAccounts::new(accounts);
    let a = store.lookup("app").unwrap();
    assert!(store.lookup("nobody").is_none());
    let cache = crate::FastCache::new(
        Arc::new(sprig_common::time::SystemClock),
        crate::fast_cache::DEFAULT_TTL,
    );
    let nonce = [1u8; 20];
    assert!(a.verify_password(b"pw"));
    cache.fill(&a, b"pw");
    assert!(cache.verify_scramble(&a, &nonce, &caching_sha2_scramble(b"pw", &nonce)));
}

/// The Compose example file parses, its verifiers accept the documented dev
/// password, and its backend credential resolves from the same file.
#[test]
fn compose_dev_accounts_file() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../deploy/compose/gateway-accounts.dev.toml");
    let store = FileAccounts::open(&path).unwrap();
    assert_eq!(store.len(), 3);
    for (user, cluster) in [
        ("app", "dc-a-c1"),
        ("app_repl", "dc-a-c2"),
        ("app_galera", "dc-a-c3"),
    ] {
        let a = store.lookup(user).unwrap();
        assert!(a.verify_password(b"sprig-dev-app"), "{user}");
        assert!(
            a.native.as_ref().unwrap().verify(b"sprig-dev-app"),
            "{user}"
        );
        assert_eq!(a.backend.cluster.as_str(), cluster);
        let cred = a.backend.credential().unwrap();
        assert_eq!(cred.user, "root");
    }
}
