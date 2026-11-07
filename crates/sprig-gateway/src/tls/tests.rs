//! Certificate bundle tests: loading, validation, expiry and rotation.

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use sprig_common::time::SystemClock;

use super::*;
use crate::testutil::{TempDir, TestCa};

const FAR: (i32, u8, u8) = (2045, 1, 1);
/// 2045-01-01T00:00:00Z.
const FAR_UNIX: u64 = 2_366_841_600;

fn clock() -> Arc<dyn Clock> {
    Arc::new(SystemClock)
}

#[test]
fn not_after_is_read_from_the_leaf() {
    let ca = TestCa::new();
    let leaf = ca.leaf("gateway", FAR);
    let t = not_after(&leaf.der).unwrap();
    assert_eq!(t.duration_since(UNIX_EPOCH).unwrap().as_secs(), FAR_UNIX);
    // rcgen's default notAfter, 4096-01-01, is a GeneralizedTime.
    assert!(not_after(&ca.der).unwrap() > SystemTime::now() + Duration::from_secs(1 << 32));
}

#[test]
fn a_valid_bundle_loads_and_reports_its_expiry() {
    let (ca, dir) = (TestCa::new(), TempDir::new());
    let leaf = ca.leaf("gateway", FAR);
    let (c, k) = dir.write_leaf(&leaf);
    let b = CertBundle::load("default", &c, &k, clock()).unwrap();
    assert_eq!(b.name(), "default");
    assert_eq!(b.certified_key().cert[0], leaf.der);
    assert_eq!(
        b.not_after().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        FAR_UNIX
    );
    let left = b.seconds_to_expiry();
    assert!(left > 0.0 && left < 30.0 * 365.0 * 86_400.0, "{left}");
    assert!(matches!(b.poll(), PollOutcome::Unchanged));
}

#[test]
fn bad_bundles_are_refused() {
    let (ca, dir) = (TestCa::new(), TempDir::new());
    let (c, k) = dir.write_leaf(&ca.leaf("gateway", FAR));
    let load = || CertBundle::load("default", &c, &k, clock());
    let refused = |why: &str| {
        let e = load().unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::ConfigInvalid));
        assert!(e.message.contains(why), "{} lacks {why:?}", e.message);
    };

    let other = ca.leaf("other", FAR);
    std::fs::write(&k, &other.key_pem).unwrap();
    refused("don't match");

    std::fs::write(&k, "not a key").unwrap();
    refused("bad key PEM");

    let expired = ca.leaf("old", (2001, 1, 1));
    dir.write_leaf(&expired);
    refused("expired");

    std::fs::write(&c, "").unwrap();
    refused("no certificate");

    let missing = CertBundle::load("default", dir.path().join("nope.pem"), &k, clock());
    assert!(missing.unwrap_err().message.contains("can't read"));
}

#[test]
fn rotation_is_debounced_validated_and_atomic() {
    let (ca, dir) = (TestCa::new(), TempDir::new());
    let first = ca.leaf("first", FAR);
    let (c, k) = dir.write_leaf(&first);
    let b = CertBundle::load("default", &c, &k, clock()).unwrap();
    let held = b.certified_key();

    // Half-written: only the certificate changed. The pair doesn't match, so
    // it is rejected once it settles; the old pair keeps serving.
    let second = ca.leaf("second", (2044, 6, 1));
    std::fs::write(&c, &second.cert_pem).unwrap();
    assert!(matches!(b.poll(), PollOutcome::Pending));
    assert!(matches!(b.poll(), PollOutcome::Rejected(_)));
    assert!(
        matches!(b.poll(), PollOutcome::Unchanged),
        "a rejected pair is not retried"
    );
    assert_eq!(b.certified_key().cert[0], first.der);

    // The key arrives: the pair is loaded on the second poll that sees it.
    std::fs::write(&k, &second.key_pem).unwrap();
    assert!(matches!(b.poll(), PollOutcome::Pending));
    assert!(matches!(b.poll(), PollOutcome::Rotated));
    assert_eq!(b.certified_key().cert[0], second.der);
    assert!(b.not_after() < not_after(&first.der).unwrap());
    assert!(matches!(b.poll(), PollOutcome::Unchanged));

    // Holders of the old pair keep it.
    assert_eq!(held.cert[0], first.der);

    // A file that vanishes mid-rotation is waited for.
    std::fs::remove_file(&k).unwrap();
    assert!(matches!(b.poll(), PollOutcome::Pending));
    assert_eq!(b.certified_key().cert[0], second.der);
}

#[test]
fn the_store_shares_bundles_by_name_and_paths() {
    let (ca, dir) = (TestCa::new(), TempDir::new());
    let (c, k) = dir.write_leaf(&ca.leaf("gateway", FAR));
    let store = CertStore::new(clock());
    let a = store.bundle("default", &c, &k).unwrap();
    let b = store.bundle("default", &c, &k).unwrap();
    assert!(Arc::ptr_eq(&a, &b));

    let other = TempDir::new();
    let (c2, k2) = other.write_leaf(&ca.leaf("moved", FAR));
    let moved = store.bundle("default", &c2, &k2).unwrap();
    assert!(!Arc::ptr_eq(&a, &moved));
    assert_eq!(moved.cert_path(), c2);
    assert_eq!(moved.key_path(), k2);
    assert_eq!(store.poll().len(), 1);
}

#[tokio::test]
async fn the_watcher_rotates_on_its_own() {
    let (ca, dir) = (TestCa::new(), TempDir::new());
    let (c, k) = dir.write_leaf(&ca.leaf("first", FAR));
    let store = Arc::new(CertStore::new(clock()));
    let b = store.bundle("default", &c, &k).unwrap();
    let stop = CancellationToken::new();
    let watcher = store.spawn_watcher(Duration::from_millis(20), stop.clone());

    let second = ca.leaf("second", FAR);
    dir.write_leaf(&second);
    let rotated = async {
        while b.certified_key().cert[0] != second.der {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), rotated)
        .await
        .unwrap();
    stop.cancel();
    watcher.await.unwrap();
}

#[test]
fn server_config_honours_the_minimum_version() {
    let (ca, dir) = (TestCa::new(), TempDir::new());
    let (c, k) = dir.write_leaf(&ca.leaf("gateway", FAR));
    let b = CertBundle::load("default", &c, &k, clock()).unwrap();
    assert!(server_config(b.clone(), TlsVersion::V1_2).is_ok());
    assert!(server_config(b, TlsVersion::V1_3).is_ok());
}
