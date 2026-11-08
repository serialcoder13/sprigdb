//! The backend client against real servers (chapter 05 step 5).
//!
//! ```sh
//! export SPRIG_TEST_MARIADB_SINGLE=mysql://root:sprig-dev-root@127.0.0.1:13301
//! export SPRIG_TEST_DOCKER=1        # throwaway MySQL 8.4 for caching_sha2_password
//! cargo test -p sprig-proto-mysql --test client_live -- --nocapture
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::path::PathBuf;
use std::time::Duration;

use sprig_proto_mysql::client::{
    ClientCert, ClientConfig, ClientError, MysqlClient, PacketKind, TlsMode, TlsOptions,
};
use sprig_testkit::faults::compose::{OwnContainer, docker};
use sprig_testkit::skip;
use sprig_testkit::util::TempDir;

/// `(user, password, host:port)` of the first URL in `var`.
fn url(var: &str) -> Option<(String, String, String)> {
    let v = skip::var(var)?;
    let first = skip::urls(&v).into_iter().next()?;
    let rest = first.strip_prefix("mysql://")?;
    let (cred, addr) = rest.rsplit_once('@')?;
    let (user, pass) = cred.split_once(':').unwrap_or((cred, ""));
    Some((
        user.to_owned(),
        pass.to_owned(),
        addr.trim_end_matches('/').to_owned(),
    ))
}

fn skip_verify() -> TlsOptions {
    TlsOptions {
        skip_verify: true,
        ..TlsOptions::default()
    }
}

fn cfg(user: &str, pass: &str, tls: TlsMode, opts: &TlsOptions) -> ClientConfig {
    let mut c = ClientConfig::new(user, pass);
    c.tls = tls;
    c.tls_config = Some(opts.build().unwrap());
    c.connect_timeout = Duration::from_secs(10);
    c
}

#[tokio::test]
async fn native_password_over_tls_against_mariadb() {
    let Some((user, pass, addr)) = url(skip::MARIADB_SINGLE) else {
        skip::report_missing(
            skip::MARIADB_SINGLE,
            "native_password_over_tls_against_mariadb",
        );
        return;
    };
    let mut c = MysqlClient::connect(&addr, &cfg(&user, &pass, TlsMode::Required, &skip_verify()))
        .await
        .unwrap();
    assert!(c.server().is_mariadb());
    assert!(c.tls_info().is_some(), "TLS negotiated");
    assert!(c.caps().contains(sprig_proto_mysql::Caps::SESSION_TRACK));
    assert!(c.caps().contains(sprig_proto_mysql::Caps::DEPRECATE_EOF));
    assert!(!c.caps().contains(sprig_proto_mysql::Caps::LOCAL_FILES));

    c.query_drop(
        "SET SESSION session_track_system_variables = '*', session_track_schema = ON, \
         session_track_state_change = ON, session_track_transaction_info = 'STATE'",
    )
    .await
    .unwrap();
    c.query_drop("CREATE DATABASE IF NOT EXISTS sprig_client_live")
        .await
        .unwrap();
    c.query_drop("USE sprig_client_live").await.unwrap();
    assert_eq!(c.tracker().schema(), Some("sprig_client_live"));
    c.query_drop("CREATE TABLE IF NOT EXISTS t (id INT AUTO_INCREMENT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    c.query_drop("BEGIN").await.unwrap();
    assert!(c.in_transaction());
    assert_eq!(c.tracker().trx_state(), Some("T_______"));
    c.query_drop("INSERT INTO t (v) VALUES (1)").await.unwrap();
    c.query_drop("COMMIT").await.unwrap();
    assert!(!c.in_transaction());
    assert!(
        c.tracker().last_gtid().is_some(),
        "last_gtid tracked after COMMIT"
    );

    // Multi-statement with mixed results, streamed raw.
    c.send_query(b"SET @x = 1; SELECT @x + 1 AS y, 'two'; DO 1")
        .await
        .unwrap();
    let mut kinds = Vec::new();
    while let Some(p) = c.next_packet().await.unwrap() {
        kinds.push(match p.kind {
            PacketKind::Ok(_) => "ok",
            PacketKind::ColumnCount(_) => "count",
            PacketKind::ColumnDef => "def",
            PacketKind::Row => "row",
            PacketKind::End(_) => "end",
            other => panic!("{other:?}"),
        });
    }
    assert_eq!(kinds, ["ok", "count", "def", "def", "row", "end", "ok"]);
    assert!(c.is_idle());

    let row = c.query_first("SELECT @x + 1, NULL").await.unwrap().unwrap();
    assert_eq!(row[0].as_deref(), Some(&b"2"[..]));
    assert_eq!(row[1], None);

    let p = c.prepare(b"SELECT v FROM t WHERE id > ?").await.unwrap();
    assert_eq!(p.ok.params, 1);
    assert_eq!(p.columns.len(), 1);
    c.close_statement(p.ok.stmt_id).await.unwrap();

    let err = c.query("SELECT * FROM no_such_table").await.unwrap_err();
    assert_eq!(err.server_error().unwrap().code, 1146);
    c.ping().await.unwrap();
    c.reset_connection().await.unwrap();
    let row = c.query_first("SELECT @x").await.unwrap().unwrap();
    assert_eq!(row[0], None, "reset dropped user variables");
    c.quit().await;
}

#[tokio::test]
async fn compressed_plaintext_against_mariadb() {
    let Some((user, pass, addr)) = url(skip::MARIADB_SINGLE) else {
        skip::report_missing(skip::MARIADB_SINGLE, "compressed_plaintext_against_mariadb");
        return;
    };
    let mut cfg = cfg(&user, &pass, TlsMode::Disabled, &skip_verify());
    cfg.compress = true;
    let mut c = MysqlClient::connect(&addr, &cfg).await.unwrap();
    assert!(c.tls_info().is_none());
    let rs = c
        .query("SELECT REPEAT('x', 100000) AS big, 1")
        .await
        .unwrap();
    assert_eq!(
        rs[0].text_rows().unwrap()[0][0].as_ref().unwrap().len(),
        100_000
    );
    c.quit().await;
}

const MYSQL_ROOT: &str = "sprig-sim-root";

async fn mysql84() -> Option<(OwnContainer, String)> {
    if !skip::flag(skip::DOCKER) {
        skip::report_missing(skip::DOCKER, "caching_sha2_password against MySQL 8.4");
        return None;
    }
    let env = format!("MYSQL_ROOT_PASSWORD={MYSQL_ROOT}");
    let c = OwnContainer::run(
        "mysql84",
        &["-e", &env, "-p", "127.0.0.1::3306", "mysql:8.4"],
    )
    .unwrap();
    let addr = c.port(3306).unwrap();
    let opts = cfg("root", MYSQL_ROOT, TlsMode::Required, &skip_verify());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
    loop {
        match MysqlClient::connect(&addr, &opts).await {
            Ok(mut cl) => {
                cl.quit().await;
                break;
            }
            Err(e) if tokio::time::Instant::now() < deadline => {
                let _ = e;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => panic!("MySQL 8.4 never accepted connections: {e}"),
        }
    }
    Some((c, addr))
}

#[tokio::test]
async fn caching_sha2_full_and_fast_paths_and_client_cert() {
    let Some((container, addr)) = mysql84().await else {
        return;
    };
    let mut root = MysqlClient::connect(
        &addr,
        &cfg("root", MYSQL_ROOT, TlsMode::Required, &skip_verify()),
    )
    .await
    .unwrap();
    assert_eq!(root.server().auth_plugin, "caching_sha2_password");
    root.query_drop(
        "CREATE USER 'app'@'%' IDENTIFIED WITH caching_sha2_password BY 'app-pw'; \
         CREATE USER 'certy'@'%' IDENTIFIED WITH caching_sha2_password BY 'cert-pw' REQUIRE X509; \
         FLUSH PRIVILEGES",
    )
    .await
    .unwrap();

    // Empty cache, plaintext: the full path needs TLS (no RSA, D4).
    let plain = cfg("app", "app-pw", TlsMode::Disabled, &skip_verify());
    let err = MysqlClient::connect(&addr, &plain).await.unwrap_err();
    assert!(
        matches!(err, ClientError::Auth(ref m) if m.contains("TLS")),
        "{err}"
    );

    // Full path over TLS fills the server's cache...
    let tls = cfg("app", "app-pw", TlsMode::Required, &skip_verify());
    let mut a = MysqlClient::connect(&addr, &tls).await.unwrap();
    assert!(a.tls_info().is_some());
    a.quit().await;
    // ...so the fast path works even without TLS.
    let mut b = MysqlClient::connect(&addr, &plain).await.unwrap();
    let row = b
        .query_first("SELECT CURRENT_USER()")
        .await
        .unwrap()
        .unwrap();
    assert!(row[0].as_deref().unwrap().starts_with(b"app@"));
    b.quit().await;
    // A wrong password fails with 1045 either way.
    let bad = cfg("app", "nope", TlsMode::Required, &skip_verify());
    let err = MysqlClient::connect(&addr, &bad).await.unwrap_err();
    assert_eq!(err.server_error().unwrap().code, 1045);

    // Client certificate: MySQL generated a CA and a client pair.
    let dir = TempDir::new("sprig-client-cert").unwrap();
    for f in ["ca.pem", "client-cert.pem", "client-key.pem"] {
        let to = dir.join(f);
        docker(&[
            "cp",
            &format!("{}:/var/lib/mysql/{f}", container.name()),
            to.to_str().unwrap(),
        ])
        .unwrap();
    }
    let no_cert = cfg("certy", "cert-pw", TlsMode::Required, &skip_verify());
    let err = MysqlClient::connect(&addr, &no_cert).await.unwrap_err();
    assert_eq!(
        err.server_error().unwrap().code,
        1045,
        "REQUIRE X509 without a certificate"
    );
    let with_cert = TlsOptions {
        skip_verify: true,
        client_cert: Some(ClientCert {
            cert_file: dir.join("client-cert.pem"),
            key_file: dir.join("client-key.pem"),
        }),
        ca_file: None::<PathBuf>,
    };
    let mut c = MysqlClient::connect(
        &addr,
        &cfg("certy", "cert-pw", TlsMode::Required, &with_cert),
    )
    .await
    .unwrap();
    let row = c
        .query_first("SELECT CURRENT_USER()")
        .await
        .unwrap()
        .unwrap();
    assert!(row[0].as_deref().unwrap().starts_with(b"certy@"));
    c.quit().await;
    root.quit().await;
    drop(container);
}
