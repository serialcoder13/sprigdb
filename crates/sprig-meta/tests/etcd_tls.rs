//! `EtcdStore` over mutual TLS, against a real etcd.
//!
//! Skipped unless `SPRIG_TEST_ETCD_TLS` (an `https://` endpoint) and
//! `SPRIG_TEST_ETCD_TLS_DIR` (the test PKI) are set. To run it:
//!
//! ```text
//! export SPRIG_TEST_ETCD_TLS_DIR=$(mktemp -d)
//! cargo test -p sprig-meta --test etcd_tls -- --ignored generate_test_pki
//! docker run -d --name sprig-test-etcd-tls -p 127.0.0.1:23791:2379 \
//!   -v $SPRIG_TEST_ETCD_TLS_DIR:/pki:ro quay.io/coreos/etcd:v3.6.5 etcd \
//!   --data-dir /tmp/etcd --listen-client-urls https://0.0.0.0:2379 \
//!   --advertise-client-urls https://127.0.0.1:23791 --client-cert-auth \
//!   --trusted-ca-file /pki/ca.pem --cert-file /pki/server.pem \
//!   --key-file /pki/server-key.pem
//! SPRIG_TEST_ETCD_TLS=https://127.0.0.1:23791 cargo test -p sprig-meta --test etcd_tls
//! ```

#![allow(clippy::unwrap_used, missing_docs)]

use std::path::{Path, PathBuf};

use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair,
};
use sprig_common::SprigCode;
use sprig_common::config::TlsConfig;
use sprig_meta::{Consistency, EtcdConfig, EtcdStore, EtcdTls, MetaStore, Op, Tier, Txn};

fn pki_dir() -> Option<PathBuf> {
    std::env::var_os("SPRIG_TEST_ETCD_TLS_DIR").map(PathBuf::from)
}

fn ca(name: &str) -> CertifiedIssuer<'static, KeyPair> {
    let mut p = CertificateParams::new(Vec::<String>::new()).unwrap();
    p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    p.distinguished_name.push(DnType::CommonName, name);
    CertifiedIssuer::self_signed(p, KeyPair::generate().unwrap()).unwrap()
}

fn leaf(
    issuer: &CertifiedIssuer<'static, KeyPair>,
    cn: &str,
    sans: &[&str],
    usages: &[ExtendedKeyUsagePurpose],
    dir: &Path,
    file: &str,
) {
    let mut p =
        CertificateParams::new(sans.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>()).unwrap();
    p.distinguished_name.push(DnType::CommonName, cn);
    p.extended_key_usages = usages.to_vec();
    let key = KeyPair::generate().unwrap();
    let cert = p.signed_by(&key, issuer).unwrap();
    std::fs::write(dir.join(format!("{file}.pem")), cert.pem()).unwrap();
    std::fs::write(dir.join(format!("{file}-key.pem")), key.serialize_pem()).unwrap();
}

/// Writes the CA, etcd's server certificate, a client certificate, and a
/// client certificate from a foreign CA into `SPRIG_TEST_ETCD_TLS_DIR`.
#[test]
#[ignore = "writes the test PKI; run before starting the TLS etcd"]
fn generate_test_pki() {
    let dir = pki_dir().expect("set SPRIG_TEST_ETCD_TLS_DIR");
    std::fs::create_dir_all(&dir).unwrap();
    let ca1 = ca("sprig test ca");
    std::fs::write(dir.join("ca.pem"), ca1.pem()).unwrap();
    leaf(
        &ca1,
        "etcd",
        &["localhost", "127.0.0.1"],
        // etcd's own gateway dials itself with this certificate.
        &[
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ],
        &dir,
        "server",
    );
    leaf(
        &ca1,
        "sprig-controller",
        &[],
        &[ExtendedKeyUsagePurpose::ClientAuth],
        &dir,
        "client",
    );
    let rogue = ca("rogue ca");
    leaf(
        &rogue,
        "sprig-controller",
        &[],
        &[ExtendedKeyUsagePurpose::ClientAuth],
        &dir,
        "rogue",
    );
}

#[tokio::test]
async fn etcd_mutual_tls() {
    let (Ok(endpoint), Some(dir)) = (std::env::var("SPRIG_TEST_ETCD_TLS"), pki_dir()) else {
        eprintln!(
            "skipping etcd_mutual_tls: set SPRIG_TEST_ETCD_TLS and SPRIG_TEST_ETCD_TLS_DIR (see the file's header)"
        );
        return;
    };
    let files = |cert: &str| TlsConfig {
        ca: dir.join("ca.pem"),
        cert: dir.join(format!("{cert}.pem")),
        key: dir.join(format!("{cert}-key.pem")),
    };
    let tier = Tier::Dc(sprig_common::ids::DcId::new("tls1").unwrap());

    // The right client certificate: reads and writes work.
    let cfg = EtcdConfig::new(vec![endpoint.clone()])
        .with_tls(EtcdTls::from_files(&files("client")).unwrap());
    let s = EtcdStore::connect(tier.clone(), &cfg).await.unwrap();
    let k = tier.key("settings/tls-check").unwrap();
    s.txn(Txn {
        then: vec![Op::Put {
            key: k.clone(),
            value: b"{\"v\":1}".to_vec(),
            lease: None,
        }],
        ..Txn::default()
    })
    .await
    .unwrap();
    let got = s.get(&k, Consistency::Linearizable).await.unwrap().unwrap();
    assert_eq!(got.value, b"{\"v\":1}");

    // A certificate from another CA is refused.
    let rogue = EtcdConfig::new(vec![endpoint.clone()])
        .with_tls(EtcdTls::from_files(&files("rogue")).unwrap());
    let err = EtcdStore::connect(tier.clone(), &rogue).await.unwrap_err();
    assert_eq!(err.code(), Some(SprigCode::MetaUnavailable), "{err}");

    // Plain text to a TLS port is refused.
    let plain = EtcdConfig::new(vec![endpoint.replace("https://", "http://")]);
    assert!(EtcdStore::connect(tier, &plain).await.is_err());
}
