#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use futures::{SinkExt as _, StreamExt as _};
use tokio::io::DuplexStream;
use tokio_util::codec::Framed;

use super::*;
use crate::buf::Writer;
use crate::codec::PacketCodec;
use crate::packets::{SessionChange, session_track};
use crate::resultset::encode_column_count;

const NONCE: [u8; 20] = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
];

fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    b.iter().fold(String::new(), |mut s, x| {
        let _ = write!(s, "{x:02x}");
        s
    })
}

#[test]
fn scrambles_match_reference_vectors() {
    assert_eq!(
        hex(&auth::native_scramble(b"secret", &NONCE)),
        "b32bb3a583e1340c0a1108d58b1be49781ad8c2f"
    );
    assert_eq!(
        hex(&auth::caching_sha2_scramble(b"secret", &NONCE)),
        "746ebe205d56a0707acb3e796e834e0dd7b1d61743b26bd5202c7a623230c7c9"
    );
    assert!(auth::native_scramble(b"", &NONCE).is_empty());
    assert!(auth::caching_sha2_scramble(b"", &NONCE).is_empty());
    assert_eq!(
        auth::first_response(auth::CLEAR, b"pw", &NONCE, false),
        None
    );
    assert_eq!(
        auth::first_response(auth::CLEAR, b"pw", &NONCE, true),
        Some(b"pw\0".to_vec())
    );
    assert_eq!(
        auth::first_response("client_ed25519", b"pw", &NONCE, true),
        None
    );
    assert_eq!(auth::nonce_of(b"abc\0"), b"abc");
}

#[test]
fn state_change_entry_is_read_as_servers_send_it() {
    // MariaDB: `02 01 31` (entry length 1, bare '1'), after a variable.
    let mut w = Writer::new();
    w.u8(session_track::SYSTEM_VARIABLES);
    let mut d = Writer::new();
    d.lenenc_bytes(b"autocommit").lenenc_bytes(b"OFF");
    w.lenenc_bytes(&d.into_bytes());
    w.u8(session_track::STATE_CHANGE).lenenc_bytes(b"1");
    w.u8(session_track::SCHEMA);
    let mut d = Writer::new();
    d.lenenc_bytes(b"app");
    w.lenenc_bytes(&d.into_bytes());
    let changes = tracker::decode_session_state(&w.into_bytes()).unwrap();
    assert_eq!(
        changes,
        vec![
            SessionChange::SystemVariable {
                name: Bytes::from_static(b"autocommit"),
                value: Bytes::from_static(b"OFF"),
            },
            SessionChange::StateChange(true),
            SessionChange::Schema(Bytes::from_static(b"app")),
        ]
    );
    let mut t = SessionTracker::new();
    let c = t.apply(&changes);
    assert!(c.state_changed);
    assert_eq!(c.var("autocommit"), Some("OFF"));
    assert_eq!(t.schema(), Some("app"));
}

#[test]
fn tracker_takes_last_gtid_from_the_variable() {
    let mut t = SessionTracker::new();
    let c = t.apply(&[SessionChange::SystemVariable {
        name: Bytes::from_static(b"last_gtid"),
        value: Bytes::from_static(b"2-201-7936"),
    }]);
    assert_eq!(c.gtid.as_deref(), Some("2-201-7936"));
    assert_eq!(t.last_gtid(), Some("2-201-7936"));
    let mut merged = TrackedChanges::default();
    merged.merge(c);
    merged.merge(TrackedChanges {
        vars: vec![("last_gtid".into(), "2-201-7937".into())],
        state_changed: true,
        ..TrackedChanges::default()
    });
    assert_eq!(merged.var("last_gtid"), Some("2-201-7937"));
    assert_eq!(merged.vars.len(), 1);
    assert!(merged.state_changed);
}

type Server = Framed<DuplexStream, PacketCodec>;

fn server_caps() -> Caps {
    // A MariaDB-like server: no CLIENT_MYSQL, no SSL.
    Caps::server_default(crate::Persona::MariaDb) - Caps::COMPRESS
}

fn greeting(plugin: &str) -> Bytes {
    HandshakeV10 {
        server_version: "11.8.3-MariaDB".into(),
        connection_id: 77,
        auth_data: Bytes::copy_from_slice(&NONCE),
        caps: server_caps(),
        collation: 45,
        status: StatusFlags::AUTOCOMMIT,
        auth_plugin: plugin.into(),
    }
    .encode()
}

fn ok_with(status: StatusFlags, changes: &[SessionChange]) -> Bytes {
    let mut ok = OkPacket::new(status);
    if !changes.is_empty() {
        ok.set_session_changes(changes);
    }
    ok.encode(server_caps())
}

async fn recv(s: &mut Server) -> Bytes {
    s.next().await.unwrap().unwrap().payload
}

async fn reply(s: &mut Server, payload: Bytes) {
    s.send(payload).await.unwrap();
}

fn cfg() -> ClientConfig {
    let mut c = ClientConfig::new("app", "secret");
    c.tls = TlsMode::Disabled;
    c
}

async fn connect_with(
    plugin: &'static str,
    script: impl FnOnce(Server) -> tokio::task::JoinHandle<()>,
) -> (ClientResult<MysqlClient>, tokio::task::JoinHandle<()>) {
    let (a, b) = tokio::io::duplex(1 << 16);
    let mut server = Framed::new(b, PacketCodec::default());
    server.send(greeting(plugin)).await.unwrap();
    let task = script(server);
    let client =
        MysqlClient::handshake(ClientStream::Other(Box::new(a)), "localhost", &cfg()).await;
    (client, task)
}

#[tokio::test]
async fn native_login_and_streaming_multi_results() {
    let (client, task) = connect_with(auth::NATIVE, |mut s| {
        tokio::spawn(async move {
            let resp = HandshakeResponse::decode(&recv(&mut s).await).unwrap();
            assert_eq!(resp.user, b"app");
            assert_eq!(resp.plugin.as_deref(), Some(auth::NATIVE));
            assert_eq!(
                &resp.auth_response[..],
                &auth::native_scramble(b"secret", &NONCE)[..]
            );
            assert!(
                resp.caps
                    .contains(Caps::SESSION_TRACK | Caps::DEPRECATE_EOF)
            );
            assert!(resp.caps.contains(Caps::MULTI_STATEMENTS));
            assert!(!resp.caps.contains(Caps::LOCAL_FILES));
            assert!(!resp.caps.contains(Caps::CLIENT_MYSQL));
            reply(&mut s, ok_with(StatusFlags::AUTOCOMMIT, &[])).await;

            // A multi-statement: OK (more results), then a resultset.
            s.codec_mut().reset_seq();
            let q = recv(&mut s).await;
            assert_eq!(&q[..], b"\x03USE app; SELECT 1");
            reply(
                &mut s,
                ok_with(
                    StatusFlags::AUTOCOMMIT
                        | StatusFlags::MORE_RESULTS_EXISTS
                        | StatusFlags::SESSION_STATE_CHANGED,
                    &[SessionChange::Schema(Bytes::from_static(b"app"))],
                ),
            )
            .await;
            reply(&mut s, encode_column_count(1)).await;
            let col = ColumnDef {
                name: Bytes::from_static(b"1"),
                col_type: crate::resultset::column_type::LONGLONG,
                ..ColumnDef::default()
            };
            reply(&mut s, col.encode(Caps::empty())).await;
            reply(&mut s, crate::resultset::encode_text_row([Some(&b"1"[..])])).await;
            let mut end = OkPacket::new(StatusFlags::AUTOCOMMIT);
            end.header = EOF_HEADER;
            reply(&mut s, end.encode(server_caps())).await;

            // A failing statement.
            s.codec_mut().reset_seq();
            let _ = recv(&mut s).await;
            reply(
                &mut s,
                ErrPacket::new(1146, *b"42S02", "Table 'app.x' doesn't exist")
                    .encode(server_caps()),
            )
            .await;
        })
    })
    .await;
    let mut c = client.unwrap();
    assert_eq!(c.connection_id(), 77);
    assert!(c.server().is_mariadb());
    assert!(c.is_idle());

    c.send_query(b"USE app; SELECT 1").await.unwrap();
    let mut kinds = Vec::new();
    while let Some(p) = c.next_packet().await.unwrap() {
        if let PacketKind::Ok(_) = p.kind {
            assert_eq!(p.tracked.schema.as_deref(), Some("app"));
            assert!(p.more_results());
        }
        kinds.push(std::mem::discriminant(&p.kind));
    }
    assert_eq!(kinds.len(), 5); // OK, count, def, row, end
    assert_eq!(c.tracker().schema(), Some("app"));
    assert!(c.is_idle());

    let err = c.query("SELECT * FROM x").await.unwrap_err();
    assert_eq!(err.server_error().unwrap().code, 1146);
    assert!(!err.is_connection_lost());
    assert!(c.is_idle());
    task.await.unwrap();
}

#[tokio::test]
async fn caching_sha2_fast_path() {
    let (client, task) = connect_with(auth::CACHING_SHA2, |mut s| {
        tokio::spawn(async move {
            let resp = HandshakeResponse::decode(&recv(&mut s).await).unwrap();
            assert_eq!(
                &resp.auth_response[..],
                &auth::caching_sha2_scramble(b"secret", &NONCE)[..]
            );
            reply(
                &mut s,
                crate::packets::AuthMoreData {
                    data: Bytes::from_static(&[auth::FAST_AUTH_SUCCESS]),
                }
                .encode(),
            )
            .await;
            reply(&mut s, ok_with(StatusFlags::AUTOCOMMIT, &[])).await;
        })
    })
    .await;
    assert!(client.unwrap().is_idle());
    task.await.unwrap();
}

#[tokio::test]
async fn caching_sha2_full_path_needs_tls() {
    let (client, task) = connect_with(auth::CACHING_SHA2, |mut s| {
        tokio::spawn(async move {
            let _ = recv(&mut s).await;
            reply(
                &mut s,
                crate::packets::AuthMoreData {
                    data: Bytes::from_static(&[auth::PERFORM_FULL_AUTH]),
                }
                .encode(),
            )
            .await;
        })
    })
    .await;
    let err = client.unwrap_err();
    assert!(
        matches!(err, ClientError::Auth(ref m) if m.contains("TLS")),
        "{err}"
    );
    task.await.unwrap();
}

#[tokio::test]
async fn auth_switch_to_native_and_login_refusal() {
    let (client, task) = connect_with(auth::CACHING_SHA2, |mut s| {
        tokio::spawn(async move {
            let _ = recv(&mut s).await;
            let fresh = [9u8; 20];
            let mut data = fresh.to_vec();
            data.push(0);
            reply(
                &mut s,
                crate::packets::AuthSwitchRequest {
                    plugin: auth::NATIVE.into(),
                    data: Bytes::from(data),
                }
                .encode(),
            )
            .await;
            let r = recv(&mut s).await;
            assert_eq!(&r[..], &auth::native_scramble(b"secret", &fresh)[..]);
            reply(
                &mut s,
                ErrPacket::new(1045, *b"28000", "Access denied").encode(server_caps()),
            )
            .await;
        })
    })
    .await;
    assert_eq!(client.unwrap_err().server_error().unwrap().code, 1045);
    task.await.unwrap();
}

#[tokio::test]
async fn prepare_reads_params_and_columns_without_eof() {
    let (client, task) = connect_with(auth::NATIVE, |mut s| {
        tokio::spawn(async move {
            let _ = recv(&mut s).await;
            reply(&mut s, ok_with(StatusFlags::AUTOCOMMIT, &[])).await;
            s.codec_mut().reset_seq();
            let q = recv(&mut s).await;
            assert_eq!(q[0], crate::command::com::STMT_PREPARE);
            reply(
                &mut s,
                PrepareOk {
                    stmt_id: 5,
                    columns: 1,
                    params: 2,
                    ..PrepareOk::default()
                }
                .encode(),
            )
            .await;
            for name in [&b"?"[..], b"?", b"c"] {
                let col = ColumnDef {
                    name: Bytes::copy_from_slice(name),
                    ..ColumnDef::default()
                };
                reply(&mut s, col.encode(Caps::empty())).await;
            }
        })
    })
    .await;
    let mut c = client.unwrap();
    let p = c.prepare(b"SELECT ? + ? AS c").await.unwrap();
    assert_eq!(p.ok.stmt_id, 5);
    assert_eq!(p.params.len(), 2);
    assert_eq!(p.columns.len(), 1);
    assert_eq!(&p.columns[0].name[..], b"c");
    assert!(c.is_idle());
    task.await.unwrap();
}

#[tokio::test]
async fn closed_connection_breaks_the_client() {
    let (client, task) = connect_with(auth::NATIVE, |mut s| {
        tokio::spawn(async move {
            let _ = recv(&mut s).await;
            reply(&mut s, ok_with(StatusFlags::AUTOCOMMIT, &[])).await;
            s.codec_mut().reset_seq();
            let _ = recv(&mut s).await;
            drop(s);
        })
    })
    .await;
    let mut c = client.unwrap();
    c.send_query(b"SELECT SLEEP(10)").await.unwrap();
    let err = c.next_packet().await.unwrap_err();
    assert!(err.is_connection_lost());
    assert!(c.is_broken());
    assert!(!c.is_idle());
    assert!(matches!(
        c.send_query(b"SELECT 1").await,
        Err(ClientError::Closed)
    ));
    task.await.unwrap();
}
