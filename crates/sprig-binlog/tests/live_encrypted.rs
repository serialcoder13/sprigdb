//! Encrypted binlogs (`encrypt_binlog = ON`) on a throwaway MariaDB 12.3
//! container this test starts itself: a dump streams the rows, because the
//! server decrypts the events and the stream skips the file's
//! `START_ENCRYPTION` event; the same file read directly is refused, since
//! only the server holds the key.
//!
//! Runs with `SPRIG_TEST_DOCKER=1` (skips otherwise, fails under
//! `SPRIG_TEST_STRICT=1`). The container, `sprigdb-binlog-enc-<pid>-<n>`,
//! runs with `--rm` on a random local port, loads `file_key_management`
//! with a key generated inside it from `/dev/urandom`, and is removed when
//! the test ends, pass or fail. `SPRIG_TEST_MARIADB_IMAGE` overrides the
//! image (`mariadb:12.3`).

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::process::Command;
use std::time::Duration;

use common::{gtid_pos, read_until, schema_name, server_id, source};
use mysql_async::prelude::Queryable;
use sprig_binlog::{BinlogFile, BinlogStream, Event, RowsKind, StreamOptions, Value};
use sprig_common::SprigCode;

const ROOT_PASSWORD: &str = "sprig-enc-root";
const BINLOG_BASE: &str = "enc-bin";

/// One row change: its kind, then the before and after images.
type Change = (RowsKind, Option<Vec<Value>>, Option<Vec<Value>>);

/// A throwaway container, removed on drop.
struct Container {
    name: String,
}

impl Drop for Container {
    fn drop(&mut self) {
        if std::env::var("SPRIG_TEST_KEEP").is_ok_and(|v| v.trim() == "1") {
            eprintln!("kept container {}", self.name);
            return;
        }
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .output();
    }
}

fn docker(args: &[&str]) -> Vec<u8> {
    let out = Command::new("docker")
        .args(args)
        .output()
        .expect("running docker");
    assert!(
        out.status.success(),
        "docker {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}

/// Whether docker tests may run; reports a skip otherwise.
fn docker_allowed(test: &str) -> bool {
    let on =
        |v: &str| std::env::var(v).is_ok_and(|s| matches!(s.trim(), "1" | "true" | "yes" | "on"));
    if on("SPRIG_TEST_DOCKER") {
        return true;
    }
    assert!(
        !on("SPRIG_TEST_STRICT"),
        "{test}: SPRIG_TEST_DOCKER is not set and SPRIG_TEST_STRICT is on"
    );
    eprintln!("skipped {test}: set SPRIG_TEST_DOCKER=1 to run it");
    false
}

/// Starts the encrypted-binlog server and returns it with its root URL once
/// it accepts connections.
async fn start() -> (Container, String) {
    let image =
        std::env::var("SPRIG_TEST_MARIADB_IMAGE").unwrap_or_else(|_| "mariadb:12.3".to_owned());
    let name = format!("sprigdb-binlog-enc-{}-{}", std::process::id(), server_id());
    // The key file is generated inside the container, then the image's
    // entrypoint starts the server with it.
    let script = format!(
        "printf '1;%s\\n' \"$(od -An -tx1 -N32 /dev/urandom | tr -d ' \\n')\" > /tmp/keys \
         && chmod 0644 /tmp/keys \
         && exec docker-entrypoint.sh mariadbd \
         --plugin-load-add=file_key_management \
         --file-key-management-filename=/tmp/keys \
         --encrypt-binlog=ON --log-bin={BINLOG_BASE} --binlog-format=ROW \
         --server-id=1 --gtid-strict-mode=ON"
    );
    docker(&[
        "run",
        "--rm",
        "-d",
        "--name",
        &name,
        "-e",
        &format!("MARIADB_ROOT_PASSWORD={ROOT_PASSWORD}"),
        "-p",
        "127.0.0.1::3306",
        "--entrypoint",
        "bash",
        &image,
        "-c",
        &script,
    ]);
    let c = Container { name };
    let port = String::from_utf8(docker(&["port", &c.name, "3306/tcp"])).unwrap();
    let port = port
        .lines()
        .find_map(|l| l.trim().strip_prefix("127.0.0.1:"))
        .expect("a published port")
        .to_owned();
    let url = format!("mysql://root:{ROOT_PASSWORD}@127.0.0.1:{port}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        // The entrypoint's initialization runs a server without networking
        // first; the real one answers once it is done.
        if let Ok(mut conn) = mysql_async::Conn::from_url(url.as_str()).await {
            let on: Option<String> = conn
                .query_first("SELECT @@global.encrypt_binlog")
                .await
                .unwrap_or(None);
            if on.is_some() {
                assert!(
                    on.as_deref() == Some("1") || on.as_deref() == Some("ON"),
                    "encrypt_binlog = {on:?}"
                );
                return (c, url);
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{} didn't come up: {}",
            c.name,
            String::from_utf8_lossy(&docker(&["logs", "--tail", "40", &c.name]))
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Inserts, updates and deletes through the encrypted binlog arrive over a
/// dump, decrypted, in order; the binlog file itself is refused.
#[tokio::test]
async fn rows_stream_through_an_encrypted_binlog() {
    if !docker_allowed(module_path!()) {
        return;
    }
    let (container, url) = start().await;
    let mut conn = mysql_async::Conn::from_url(url.as_str()).await.unwrap();
    let from = gtid_pos(&mut conn).await;
    let schema = schema_name();
    for q in [
        format!("CREATE DATABASE {schema}"),
        format!("CREATE TABLE {schema}.t (id INT PRIMARY KEY, v VARCHAR(64), n BIGINT)"),
        format!("INSERT INTO {schema}.t VALUES (1, 'one', 10), (2, 'two', 20), (3, 'three', 30)"),
        format!("UPDATE {schema}.t SET v = 'TWO', n = n + 1 WHERE id = 2"),
        format!("DELETE FROM {schema}.t WHERE id = 3"),
    ] {
        conn.query_drop(q).await.unwrap();
    }
    let to = gtid_pos(&mut conn).await;

    // A dump from before the writes: the stream starts at the start of the
    // file, so it meets START_ENCRYPTION, skips it and decodes the rest.
    let mut opts = StreamOptions::new(server_id());
    opts.heartbeat = Duration::from_secs(5);
    let mut stream = BinlogStream::connect(&source(&url), &from, &opts)
        .await
        .unwrap();
    let events = read_until(&mut stream, &to).await;
    let rows: Vec<Change> = events
        .into_iter()
        .filter_map(|e| match e {
            Event::Rows {
                table, kind, rows, ..
            } if table.schema == schema => Some((kind, rows)),
            _ => None,
        })
        .flat_map(|(kind, rows)| {
            rows.into_iter().map(move |r| {
                let img = |i: Option<sprig_binlog::RowImage>| {
                    i.map(|i| i.0.into_iter().map(Option::unwrap).collect::<Vec<_>>())
                };
                (kind, img(r.before), img(r.after))
            })
        })
        .collect();
    let row = |id: i64, v: &str, n: i64| {
        vec![
            Value::Int(id),
            Value::String(v.as_bytes().to_vec()),
            Value::Int(n),
        ]
    };
    assert_eq!(
        rows,
        vec![
            (RowsKind::Insert, None, Some(row(1, "one", 10))),
            (RowsKind::Insert, None, Some(row(2, "two", 20))),
            (RowsKind::Insert, None, Some(row(3, "three", 30))),
            (
                RowsKind::Update,
                Some(row(2, "two", 20)),
                Some(row(2, "TWO", 21))
            ),
            (RowsKind::Delete, Some(row(3, "three", 30)), None),
        ]
    );
    drop(stream);

    // The file on disk is encrypted: read directly, it is refused at its
    // START_ENCRYPTION event.
    let mut logs: Vec<(String, u64)> = conn.query("SHOW BINARY LOGS").await.unwrap();
    logs.retain(|(n, _)| n.starts_with(BINLOG_BASE));
    let (last, _) = logs.last().expect("a binlog file").clone();
    let bytes = docker(&[
        "exec",
        &container.name,
        "cat",
        &format!("/var/lib/mysql/{last}"),
    ]);
    let mut f = BinlogFile::new(bytes.as_slice()).await.unwrap();
    let err = loop {
        match f.next().await {
            Ok(Some(_)) => {}
            Ok(None) => panic!("{last} read to the end without START_ENCRYPTION"),
            Err(e) => break e,
        }
    };
    assert_eq!(err.code(), Some(SprigCode::BinlogUnsupported), "{err}");
    assert!(err.message.contains("encrypted"), "{err}");
    conn.disconnect().await.unwrap();
    drop(container);
}
