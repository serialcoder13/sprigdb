//! The binlog reader over TLS (r2opfix), against a server that refuses
//! plain text (`require_secure_transport = ON`, an account `REQUIRE SSL`).
//!
//! `SPRIG_TEST_BINLOG_TLS` is `mysql://<repl user>:<pw>@host:port` of such
//! a server, `SPRIG_TEST_BINLOG_TLS_ROOT` a root URL there (over TLS, for
//! the writes), `SPRIG_TEST_BINLOG_TLS_CA` the CA that signed its
//! certificate (issued for `localhost` and `127.0.0.1`), and
//! `SPRIG_TEST_BINLOG_TLS_OTHER_CA` a CA that didn't. Skips when unset.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

mod common;

use std::time::Duration;

use mysql_async::prelude::Queryable;
use sprig_binlog::{BinlogSource, BinlogStream, BinlogTls, Event, GtidPos, StreamOptions};
use sprig_common::SprigCode;

struct Env {
    url: String,
    root: String,
    ca: String,
    other_ca: String,
}

fn env(test: &str) -> Option<Env> {
    let get = |k: &str| common::urls(k, test).and_then(|v| v.into_iter().next());
    Some(Env {
        url: get("SPRIG_TEST_BINLOG_TLS")?,
        root: get("SPRIG_TEST_BINLOG_TLS_ROOT")?,
        ca: get("SPRIG_TEST_BINLOG_TLS_CA")?,
        other_ca: get("SPRIG_TEST_BINLOG_TLS_OTHER_CA")?,
    })
}

async fn root_conn(e: &Env) -> mysql_async::Conn {
    let opts = mysql_async::OptsBuilder::from_opts(mysql_async::Opts::from_url(&e.root).unwrap())
        .ssl_opts(
            mysql_async::SslOpts::default()
                .with_root_certs(vec![std::path::PathBuf::from(&e.ca).into()]),
        );
    mysql_async::Conn::new(opts).await.expect("root over TLS")
}

fn opts() -> StreamOptions {
    let mut o = StreamOptions::new(common::server_id());
    o.non_blocking = true;
    o.connect_timeout = Duration::from_secs(5);
    o
}

async fn read_all(
    src: &BinlogSource,
    from: &GtidPos,
) -> sprig_common::Result<(Vec<Event>, Option<String>)> {
    let mut s = BinlogStream::connect(src, from, &opts()).await?;
    let tls = s.tls_version().map(str::to_owned);
    let mut out = Vec::new();
    loop {
        match s.next().await? {
            Event::DumpEnd => return Ok((out, tls)),
            ev => out.push(ev),
        }
    }
}

#[tokio::test]
async fn reads_rows_over_verified_tls() {
    let Some(e) = env("reads_rows_over_verified_tls") else {
        return;
    };
    let mut root = root_conn(&e).await;
    let from = common::gtid_pos(&mut root).await;
    let schema = common::schema_name();
    root.query_drop(format!("CREATE DATABASE {schema}"))
        .await
        .unwrap();
    root.query_drop(format!(
        "CREATE TABLE {schema}.t (id INT PRIMARY KEY, v VARCHAR(20))"
    ))
    .await
    .unwrap();
    root.query_drop(format!(
        "INSERT INTO {schema}.t VALUES (1, 'over tls'), (2, 'b')"
    ))
    .await
    .unwrap();

    let src = BinlogSource::from_url(&format!("{}?tls=verify&ca_file={}", e.url, e.ca)).unwrap();
    let (events, tls) = read_all(&src, &from).await.expect("verified TLS dump");
    let rows: usize = events
        .iter()
        .map(|ev| match ev {
            Event::Rows { table, rows, .. } if table.schema == schema => rows.len(),
            _ => 0,
        })
        .sum();
    assert_eq!(rows, 2, "both inserted rows arrive over TLS");
    let tls = tls.expect("negotiated TLS");
    assert!(tls.starts_with("TLSv1_"), "{tls}");
    eprintln!(
        "verified dump over {tls}: {} events, {rows} rows",
        events.len()
    );

    // The name the certificate carries, given explicitly, also verifies.
    let named = BinlogSource::from_url(&e.url).unwrap().with_tls(BinlogTls {
        ca_file: Some(e.ca.clone().into()),
        server_name: Some("localhost".into()),
        ..BinlogTls::default()
    });
    assert!(read_all(&named, &from).await.is_ok());
    // skip_verify encrypts without checking.
    let skip = BinlogSource::from_url(&format!("{}?tls=skip_verify", e.url)).unwrap();
    assert!(read_all(&skip, &from).await.unwrap().1.is_some());
    root.query_drop(format!("DROP DATABASE {schema}"))
        .await
        .unwrap();
}

#[tokio::test]
async fn refuses_wrong_ca_wrong_name_and_plain_text() {
    let Some(e) = env("refuses_wrong_ca_wrong_name_and_plain_text") else {
        return;
    };
    let from = GtidPos::default();
    // A CA that didn't sign the server's certificate.
    let other =
        BinlogSource::from_url(&format!("{}?tls=verify&ca_file={}", e.url, e.other_ca)).unwrap();
    let err = read_all(&other, &from).await.unwrap_err();
    assert_eq!(err.code(), Some(SprigCode::BinlogUnsupported), "{err}");
    assert!(err.message.starts_with("TLS: handshake"), "{err}");
    eprintln!("other CA: {err}");
    // The right CA, a name the certificate doesn't carry.
    let wrong_name = BinlogSource::from_url(&e.url).unwrap().with_tls(BinlogTls {
        ca_file: Some(e.ca.clone().into()),
        server_name: Some("db.example".into()),
        ..BinlogTls::default()
    });
    let err = read_all(&wrong_name, &from).await.unwrap_err();
    assert!(
        err.message.contains("NotValidForName") || err.message.contains("name"),
        "{err}"
    );
    eprintln!("wrong name: {err}");
    // Mozilla's roots don't know a private CA.
    let public = BinlogSource::from_url(&format!("{}?tls=verify", e.url)).unwrap();
    assert!(read_all(&public, &from).await.is_err());
    // Plain text: the account is REQUIRE SSL and the server
    // require_secure_transport, so the login is refused.
    let plain = BinlogSource::from_url(&e.url).unwrap();
    let err = read_all(&plain, &from).await.unwrap_err();
    assert!(
        matches!(
            err.kind,
            sprig_common::ErrorKind::Backend {
                errno: 3159 | 1045,
                ..
            }
        ),
        "{err}"
    );
    eprintln!("plain text: {err}");
}
