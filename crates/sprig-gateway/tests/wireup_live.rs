//! Tenant endpoints by TLS SNI and client certificates on the MySQL
//! listener, against a running gateway.
//!
//! - `tenant_endpoints_by_sni`: with `listener.tenant_sni_suffixes` set,
//!   `app` connecting to `acme.<suffix>` logs in to acme and to
//!   `globex.<suffix>` to globex, with no tenant in the user name; a
//!   dotted name naming another tenant (`globex.app` at `acme.<suffix>`),
//!   an unknown tenant, a plain name without SNI and a name under a
//!   suffix taken off live are refused (1045).
//! - `client_certificates`: a test CA made now, `listener.mysql.client_cert`
//!   `optional` then `required`, and users with `REQUIRE X509`, `REQUIRE
//!   SUBJECT ... AND ISSUER ...` and a subject that doesn't match.
//!
//! Variables: `SPRIG_TEST_WIREUP_PORT` (the gateway's MySQL port; skipped
//! without it), `SPRIG_TEST_WIREUP_SET` (a command `<cmd> <key> <json>`
//! that writes a dc-scope setting), `SPRIG_TEST_SNI_SUFFIX` (default
//! `sni.localhost`, whose names resolve to the loopback address on macOS
//! and with systemd-resolved), `SPRIG_TEST_WIREUP_PASSWORD` (the `app`
//! users' password, default `sprig-dev-app`). The deployment's tenants are
//! `acme` and `globex`, each with the user `app` (gateway-dev.sh).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stderr,
    clippy::too_many_lines,
    clippy::doc_markdown
)]

use std::time::Duration;

use mysql_async::prelude::Queryable;
use mysql_async::{ClientIdentity, Conn, OptsBuilder, SslOpts};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
};

fn var(v: &str) -> Option<String> {
    std::env::var(v).ok().filter(|s| !s.is_empty())
}

struct Env {
    port: u16,
    set: String,
    suffix: String,
    password: String,
}

fn env() -> Option<Env> {
    let Some(port) = var("SPRIG_TEST_WIREUP_PORT").and_then(|p| p.parse().ok()) else {
        eprintln!("SPRIG_TEST_WIREUP_PORT not set: skipped");
        return None;
    };
    Some(Env {
        port,
        set: var("SPRIG_TEST_WIREUP_SET").expect("SPRIG_TEST_WIREUP_SET"),
        suffix: var("SPRIG_TEST_SNI_SUFFIX").unwrap_or_else(|| "sni.localhost".into()),
        password: var("SPRIG_TEST_WIREUP_PASSWORD").unwrap_or_else(|| "sprig-dev-app".into()),
    })
}

impl Env {
    fn set(&self, key: &str, json: &str) {
        let out = std::process::Command::new(&self.set)
            .args([key, json])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// A TLS connection to `host` as `user`, with a client certificate
    /// (PEM chain, PEM key) when given.
    async fn connect(
        &self,
        host: &str,
        user: &str,
        pass: &str,
        cert: Option<(&str, &str)>,
    ) -> Result<Conn, mysql_async::Error> {
        let mut ssl = SslOpts::default().with_danger_accept_invalid_certs(true);
        if let Some((c, k)) = cert {
            ssl = ssl.with_client_identity(Some(ClientIdentity::new(
                c.as_bytes().to_vec().into(),
                k.as_bytes().to_vec().into(),
            )));
        }
        let o = OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(self.port)
            .user(Some(user))
            .pass(Some(pass))
            .ssl_opts(Some(ssl))
            .prefer_socket(false);
        Conn::new(o).await
    }
}

fn errno(r: &Result<Conn, mysql_async::Error>) -> Option<u16> {
    match r {
        Ok(_) => None,
        Err(mysql_async::Error::Server(s)) => Some(s.code),
        Err(_) => Some(0),
    }
}

async fn who(c: &mut Conn) -> String {
    c.query_first::<String, _>("SELECT CURRENT_USER()")
        .await
        .unwrap()
        .unwrap_or_default()
}

/// Waits up to 10 s for `f` to log in (`want` true) or be refused.
async fn settle<F, Fut>(want: bool, f: F) -> bool
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<Conn, mysql_async::Error>>,
{
    for _ in 0..40 {
        let r = f().await;
        if r.is_ok() == want {
            if let Ok(c) = r {
                let _ = c.disconnect().await;
            }
            return true;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tenant_endpoints_by_sni() {
    let Some(e) = env() else { return };
    e.set("listener.tenant_sni_suffixes", &format!("\"{}\"", e.suffix));
    let acme = format!("acme.{}", e.suffix);
    let globex = format!("globex.{}", e.suffix);
    assert!(
        settle(true, || e.connect(&acme, "app", &e.password, None)).await,
        "app at {acme} never logged in"
    );
    let mut c = e.connect(&acme, "app", &e.password, None).await.unwrap();
    let at_acme = who(&mut c).await;
    c.disconnect().await.unwrap();
    let mut c = e.connect(&globex, "app", &e.password, None).await.unwrap();
    let at_globex = who(&mut c).await;
    c.disconnect().await.unwrap();
    eprintln!("CURRENT_USER(): at {acme}: {at_acme}; at {globex}: {at_globex}");
    // Each tenant's own `app`: the same name, two accounts.
    let mut c = e
        .connect("127.0.0.1", "acme.app", &e.password, None)
        .await
        .unwrap();
    let a_dotted = who(&mut c).await;
    assert_eq!(
        at_acme, a_dotted,
        "SNI and the dotted name reach the same account"
    );
    // And the tenant's own databases: one made in acme is seen only there.
    c.query_drop("CREATE DATABASE IF NOT EXISTS wu_sni_acme")
        .await
        .unwrap();
    c.disconnect().await.unwrap();
    let dbs = |mut c: Conn| async move {
        let names: Vec<String> = c.query("SHOW DATABASES").await.unwrap();
        c.disconnect().await.unwrap();
        names
    };
    let in_acme = dbs(e.connect(&acme, "app", &e.password, None).await.unwrap()).await;
    let in_globex = dbs(e.connect(&globex, "app", &e.password, None).await.unwrap()).await;
    assert!(in_acme.iter().any(|d| d == "wu_sni_acme"), "{in_acme:?}");
    assert!(
        !in_globex.iter().any(|d| d == "wu_sni_acme"),
        "{in_globex:?}"
    );
    let mut c = e.connect(&acme, "app", &e.password, None).await.unwrap();
    c.query_drop("DROP DATABASE wu_sni_acme").await.unwrap();
    c.disconnect().await.unwrap();

    // SNI wins: a dotted name naming another tenant is that tenant's
    // user name, which doesn't exist.
    let conflict = e.connect(&acme, "globex.app", &e.password, None).await;
    assert_eq!(errno(&conflict), Some(1045), "globex.app at {acme}");
    let unknown = e
        .connect(&format!("nosuch.{}", e.suffix), "app", &e.password, None)
        .await;
    assert_eq!(errno(&unknown), Some(1045));
    let two_labels = e
        .connect(&format!("x.acme.{}", e.suffix), "app", &e.password, None)
        .await;
    assert_eq!(errno(&two_labels), Some(1045));
    let no_sni = e.connect("127.0.0.1", "app", &e.password, None).await;
    assert_eq!(errno(&no_sni), Some(1045));
    let wrong_pw = e.connect(&acme, "app", "not-the-password", None).await;
    assert_eq!(errno(&wrong_pw), Some(1045));

    // Live: the suffix taken off, then put back.
    e.set("listener.tenant_sni_suffixes", "\"\"");
    assert!(
        settle(false, || e.connect(&acme, "app", &e.password, None)).await,
        "app at {acme} still logs in without the suffix"
    );
    e.set(
        "listener.tenant_sni_suffixes",
        &format!("\"other.test, {}\"", e.suffix),
    );
    assert!(settle(true, || e.connect(&acme, "app", &e.password, None)).await);
    e.set("listener.tenant_sni_suffixes", "\"\"");
}

struct Pki {
    issuer: Issuer<'static, KeyPair>,
    pem: String,
}

fn ca(cn: &str) -> Pki {
    let mut p = CertificateParams::new(Vec::<String>::new()).unwrap();
    p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    p.distinguished_name = rcgen::DistinguishedName::new();
    p.distinguished_name.push(DnType::CommonName, cn);
    let key = KeyPair::generate().unwrap();
    let cert = p.self_signed(&key).unwrap();
    Pki {
        pem: cert.pem(),
        issuer: Issuer::new(p, key),
    }
}

/// A client certificate `/O=Wireup/CN=<cn>` from `ca`: (chain, key).
fn client(ca: &Pki, cn: &str) -> (String, String) {
    let mut p = CertificateParams::new(Vec::<String>::new()).unwrap();
    p.distinguished_name = rcgen::DistinguishedName::new();
    p.distinguished_name
        .push(DnType::OrganizationName, "Wireup");
    p.distinguished_name.push(DnType::CommonName, cn);
    p.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let key = KeyPair::generate().unwrap();
    let cert = p.signed_by(&key, &ca.issuer).unwrap();
    (cert.pem(), key.serialize_pem())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_certificates() {
    let Some(e) = env() else { return };
    let dir = std::env::temp_dir().join(format!("wireup-ca-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let good = ca("Wireup test CA");
    let rogue = ca("Rogue CA");
    let ca_path = dir.join("ca.pem");
    std::fs::write(&ca_path, &good.pem).unwrap();
    let (alice, alice_key) = client(&good, "alice");
    let (mallory, mallory_key) = client(&rogue, "alice");

    // Users, through acme's app.
    let mut admin = e
        .connect("127.0.0.1", "acme.app", &e.password, None)
        .await
        .unwrap();
    for u in ["wu_x509", "wu_subj", "wu_subj_bad", "wu_plain"] {
        admin
            .query_drop(format!("DROP USER IF EXISTS '{u}'@'%'"))
            .await
            .unwrap();
    }
    admin
        .query_drop("CREATE USER 'wu_x509'@'%' IDENTIFIED BY 'wu-pass-1' REQUIRE X509")
        .await
        .unwrap();
    admin
        .query_drop(
            "CREATE USER 'wu_subj'@'%' IDENTIFIED BY 'wu-pass-1' \
             REQUIRE SUBJECT '/O=Wireup/CN=alice' AND ISSUER '/CN=Wireup test CA'",
        )
        .await
        .unwrap();
    admin
        .query_drop("CREATE USER 'wu_subj_bad'@'%' IDENTIFIED BY 'wu-pass-1' REQUIRE SUBJECT '/O=Wireup/CN=bob'")
        .await
        .unwrap();
    admin
        .query_drop("CREATE USER 'wu_plain'@'%' IDENTIFIED BY 'wu-pass-1'")
        .await
        .unwrap();
    let cipher = admin
        .query_drop("CREATE USER 'wu_cipher'@'%' IDENTIFIED BY 'wu-pass-1' REQUIRE CIPHER 'x'")
        .await;
    eprintln!("REQUIRE CIPHER: {cipher:?}");
    assert!(cipher.is_err());

    // Off (the default): no certificate is asked for, so X509 users can't
    // log in.
    e.set("listener.mysql.client_cert", "\"off\"");
    e.set(
        "tls.bundles.default.client_ca_path",
        &format!("\"{}\"", ca_path.display()),
    );
    let h = "127.0.0.1";
    let with = Some((alice.as_str(), alice_key.as_str()));
    assert!(settle(false, || e.connect(h, "acme.wu_x509", "wu-pass-1", with)).await);

    // Optional: verified when presented.
    e.set("listener.mysql.client_cert", "\"optional\"");
    assert!(
        settle(true, || e.connect(h, "acme.wu_x509", "wu-pass-1", with)).await,
        "REQUIRE X509 with a good certificate"
    );
    let r = e.connect(h, "acme.wu_x509", "wu-pass-1", None).await;
    assert_eq!(errno(&r), Some(1045), "REQUIRE X509 without a certificate");
    let r = e.connect(h, "acme.wu_subj", "wu-pass-1", with).await;
    assert!(r.is_ok(), "REQUIRE SUBJECT/ISSUER matching: {r:?}");
    let r = e.connect(h, "acme.wu_subj_bad", "wu-pass-1", with).await;
    assert_eq!(errno(&r), Some(1045), "REQUIRE SUBJECT not matching");
    let r = e
        .connect(
            h,
            "acme.wu_x509",
            "wu-pass-1",
            Some((&mallory, &mallory_key)),
        )
        .await;
    eprintln!("a certificate from another CA: {r:?}");
    assert!(r.is_err(), "a certificate the CA didn't sign");
    let r = e.connect(h, "acme.wu_plain", "wu-pass-1", None).await;
    assert!(r.is_ok(), "a plain user without a certificate: {r:?}");

    // Required: a TLS handshake without one fails.
    e.set("listener.mysql.client_cert", "\"required\"");
    assert!(settle(false, || e.connect(h, "acme.wu_plain", "wu-pass-1", None)).await);
    let r = e.connect(h, "acme.wu_plain", "wu-pass-1", with).await;
    assert!(r.is_ok(), "with a certificate under required: {r:?}");

    e.set("listener.mysql.client_cert", "\"off\"");
    assert!(settle(true, || e.connect(h, "acme.wu_plain", "wu-pass-1", None)).await);
    let mut admin = e
        .connect("127.0.0.1", "acme.app", &e.password, None)
        .await
        .unwrap();
    for u in ["wu_x509", "wu_subj", "wu_subj_bad", "wu_plain", "wu_cipher"] {
        admin
            .query_drop(format!("DROP USER IF EXISTS '{u}'@'%'"))
            .await
            .unwrap();
    }
    let _ = std::fs::remove_dir_all(&dir);
}
