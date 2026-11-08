//! The golden capture tool (chapter 05, "Testing": golden captures).
//!
//! Connects to real servers over plain TCP, speaks the protocol with this
//! crate's codec and packet encoders, and records every packet each side
//! sent as a fixture under `tests/golden/<label>/` (format:
//! `tests/golden/fixture.rs`). `tests/golden.rs` replays them.
//!
//! Skipped unless `SPRIG_GOLDEN_CAPTURE` names the servers:
//!
//! ```sh
//! SPRIG_GOLDEN_CAPTURE='mariadb-11.8=root:sprig-dev-root@127.0.0.1:13301,
//!   mariadb-12.3=root:golden-root@127.0.0.1:13392!,
//!   mysql-8.4=root:golden-root@127.0.0.1:13391!' \
//! cargo test -p sprig-proto-mysql --test golden_capture -- --nocapture
//! ```
//!
//! A trailing `!` marks a throwaway server the tool may change globally
//! (`SET GLOBAL local_infile`, `require_secure_transport`); without it only
//! session state is touched, plus a `sprig_golden` schema that is dropped at
//! the end. Fixtures are rewritten on each run; connection ids, nonces,
//! timestamps and GTIDs differ between runs, so diffs are expected there.
//!
//! Everything captured is plaintext: `caching_sha2_password`'s full path
//! needs TLS or RSA, so on MySQL the tool primes the fast-auth cache with
//! one TLS login (through `client::MysqlClient`) and then records the
//! plaintext fast path, and records the start of the full path up to the
//! server's RSA public key.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::many_single_char_names,
    missing_docs,
    unreachable_pub
)]

#[path = "golden/fixture.rs"]
mod fixture;

use std::path::PathBuf;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder as _, Encoder as _};

use fixture::{Class, Fixture, Kind, Line, Walker};
use sprig_proto_mysql::client::{ClientConfig, MysqlClient, TlsMode, TlsOptions, auth as cauth};
use sprig_proto_mysql::command::{
    BoundParam, Command, ExecuteParams, ParamValue, QueryAttrs, StmtExecute,
};
use sprig_proto_mysql::packets::{Caps, HandshakeResponse, HandshakeV10, StatusFlags};
use sprig_proto_mysql::resultset::{ColumnDef, PrepareOk, column_type as ct};
use sprig_proto_mysql::{Framing, PacketCodec, Value};

const TIMEOUT: Duration = Duration::from_secs(15);

/// One server to capture from.
#[derive(Debug, Clone)]
struct Target {
    label: String,
    user: String,
    pass: String,
    addr: String,
    throwaway: bool,
}

fn targets() -> Vec<Target> {
    let Ok(v) = std::env::var("SPRIG_GOLDEN_CAPTURE") else {
        return Vec::new();
    };
    v.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            let (label, rest) = s.split_once('=').expect("label=user:pass@host:port");
            let throwaway = rest.ends_with('!');
            let rest = rest.trim_end_matches('!');
            let (cred, addr) = rest.rsplit_once('@').expect("user:pass@host:port");
            let (user, pass) = cred.split_once(':').unwrap_or((cred, ""));
            Target {
                label: label.to_owned(),
                user: user.to_owned(),
                pass: pass.to_owned(),
                addr: addr.to_owned(),
                throwaway,
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// A recording connection
// ---------------------------------------------------------------------------

/// How to log in.
#[derive(Debug, Clone, Default)]
struct Login {
    user: String,
    pass: String,
    /// The plugin named in the handshake response; the greeting's when
    /// `None`. The first auth response is computed for it.
    plugin: Option<String>,
    /// Capabilities added to, and removed from, the default request.
    add: Caps,
    remove: Caps,
    database: Option<String>,
    /// zstd level when asking for `ZSTD_COMPRESSION_ALGORITHM`.
    zstd_level: Option<u8>,
    /// On `AuthMoreData 0x04`, ask for the RSA public key (0x02) and stop
    /// after it, instead of failing.
    rsa_probe: bool,
    /// On `AuthMoreData 0x04`, send 256 bytes that aren't an RSA
    /// ciphertext, to get the server's ERR in plaintext.
    full_garbage: bool,
}

struct Conn {
    tcp: TcpStream,
    codec: PacketCodec,
    rbuf: BytesMut,
    lines: Vec<Line>,
    caps: Caps,
    server_caps: Caps,
    server: String,
    greeting: Option<HandshakeV10>,
    framing: Framing,
    /// Data sent when the server asks for a `LOCAL INFILE`.
    infile: Vec<u8>,
    /// Set when the login ended with ERR or was abandoned.
    failed: bool,
}

impl Conn {
    async fn read(&mut self) -> Bytes {
        loop {
            match self.codec.decode(&mut self.rbuf) {
                Ok(Some(p)) => {
                    self.lines.push(Line::S(p.seq, p.payload.clone()));
                    return p.payload;
                }
                Ok(None) => {}
                Err(e) => panic!("{}: decode: {e}; so far {:?}", self.server, self.lines),
            }
            let mut tmp = BytesMut::with_capacity(64 * 1024);
            let n = tokio::time::timeout(TIMEOUT, self.tcp.read_buf(&mut tmp))
                .await
                .unwrap_or_else(|_| {
                    panic!("{}: read timed out; so far {:?}", self.server, self.lines)
                })
                .expect("read");
            assert!(
                n > 0,
                "{}: connection closed; so far {:?}",
                self.server,
                self.lines
            );
            if self.framing.is_compressed() {
                self.lines.push(Line::W(Bytes::copy_from_slice(&tmp)));
            }
            self.rbuf.extend_from_slice(&tmp);
        }
    }

    async fn write(&mut self, payload: Bytes) {
        let seq = self.codec.next_seq();
        let mut out = BytesMut::new();
        self.codec
            .encode(payload.clone(), &mut out)
            .expect("encode");
        self.lines.push(Line::C(seq, payload));
        self.tcp.write_all(&out).await.expect("write");
    }

    /// Sends a command and records the whole response.
    async fn command(&mut self, payload: Bytes) -> Vec<Line> {
        self.lines.clear();
        self.codec.reset_seq();
        let kind = Kind::for_command(payload[0]);
        self.write(payload).await;
        self.read_response(kind).await;
        std::mem::take(&mut self.lines)
    }

    async fn read_response(&mut self, kind: Kind) {
        let mut w = Walker::new(kind, self.caps);
        while !w.done() {
            let p = self.read().await;
            if w.feed(&p) == Class::LocalInfile {
                let data = std::mem::take(&mut self.infile);
                for chunk in data.chunks(16 * 1024) {
                    self.write(Bytes::copy_from_slice(chunk)).await;
                }
                self.write(Bytes::new()).await;
            }
        }
    }

    async fn query(&mut self, sql: &str) -> Vec<Line> {
        let mut p = vec![0x03];
        p.extend_from_slice(sql.as_bytes());
        self.command(p.into()).await
    }

    /// Runs `sql`, panicking on ERR (setup statements).
    async fn exec(&mut self, sql: &str) {
        let lines = self.query(sql).await;
        for l in &lines {
            if let Line::S(_, p) = l {
                assert!(
                    p.first() != Some(&0xFF),
                    "{sql}: {:?}",
                    String::from_utf8_lossy(p)
                );
            }
        }
    }

    async fn prepare(&mut self, sql: &str) -> (PrepareOk, Vec<ColumnDef>, Vec<Line>) {
        let mut p = vec![0x16];
        p.extend_from_slice(sql.as_bytes());
        let lines = self.command(p.into()).await;
        let mut w = Walker::new(Kind::Prepare, self.caps);
        let mut ok = PrepareOk::default();
        let mut cols = Vec::new();
        for l in &lines {
            if let Line::S(_, p) = l {
                match w.feed(p) {
                    Class::PrepareOk => ok = PrepareOk::decode(p).unwrap(),
                    Class::ColumnDef => cols.push(ColumnDef::decode(p, self.caps).unwrap()),
                    Class::Err => panic!("prepare {sql}: {:?}", String::from_utf8_lossy(p)),
                    _ => {}
                }
            }
        }
        (ok, cols, lines)
    }
}

const CLIENT_WANTS: Caps = Caps::LONG_PASSWORD
    .union(Caps::LONG_FLAG)
    .union(Caps::PROTOCOL_41)
    .union(Caps::TRANSACTIONS)
    .union(Caps::SECURE_CONNECTION)
    .union(Caps::PLUGIN_AUTH)
    .union(Caps::PLUGIN_AUTH_LENENC_CLIENT_DATA)
    .union(Caps::MULTI_STATEMENTS)
    .union(Caps::MULTI_RESULTS)
    .union(Caps::PS_MULTI_RESULTS)
    .union(Caps::SESSION_TRACK)
    .union(Caps::DEPRECATE_EOF)
    .union(Caps::CONNECT_ATTRS)
    .union(Caps::LOCAL_FILES)
    .union(Caps::MARIADB_CLIENT_STMT_BULK_OPERATIONS)
    .union(Caps::MARIADB_CLIENT_EXTENDED_METADATA)
    .union(Caps::MARIADB_CLIENT_BULK_UNIT_RESULTS);

/// Connects and logs in, recording the connection phase. Returns the
/// connection (usable unless `failed`) and the recorded lines.
async fn connect(addr: &str, login: &Login) -> (Conn, Vec<Line>) {
    let tcp = TcpStream::connect(addr).await.expect("connect");
    tcp.set_nodelay(true).ok();
    let mut c = Conn {
        tcp,
        codec: PacketCodec::new(1 << 30),
        rbuf: BytesMut::new(),
        lines: Vec::new(),
        caps: Caps::empty(),
        server_caps: Caps::empty(),
        server: addr.to_owned(),
        greeting: None,
        framing: Framing::Plain,
        infile: Vec::new(),
        failed: false,
    };
    let g = c.read().await;
    if g.first() == Some(&0xFF) {
        c.failed = true;
        let lines = std::mem::take(&mut c.lines);
        return (c, lines);
    }
    let hs = HandshakeV10::decode(&g).expect("greeting");
    c.server.clone_from(&hs.server_version);
    c.server_caps = hs.caps;
    let mut want = (CLIENT_WANTS | login.add) - login.remove;
    want.set(Caps::CONNECT_WITH_DB, login.database.is_some());
    if login.zstd_level.is_some() {
        want |= Caps::ZSTD_COMPRESSION_ALGORITHM;
    }
    let caps = want & hs.caps;
    c.caps = caps;
    let mut plugin = login
        .plugin
        .clone()
        .unwrap_or_else(|| hs.auth_plugin.clone());
    let nonce = cauth::nonce_of(&hs.auth_data).to_vec();
    let first = cauth::first_response(&plugin, login.pass.as_bytes(), &nonce, false)
        .unwrap_or_else(|| b"\x01\x02\x03".to_vec());
    let resp = HandshakeResponse {
        caps,
        max_packet: 1 << 24,
        collation: 45,
        user: login.user.as_bytes().to_vec(),
        auth_response: Bytes::from(first),
        database: login.database.as_ref().map(|d| d.as_bytes().to_vec()),
        plugin: Some(plugin.clone()),
        attrs: vec![
            ("_client_name".into(), "sprig-golden".into()),
            ("_os".into(), "capture".into()),
        ],
        zstd_level: login.zstd_level,
    };
    c.greeting = Some(hs);
    c.write(resp.encode()).await;
    loop {
        let p = c.read().await;
        match p.first() {
            Some(0x00) => break,
            Some(0xFF) => {
                c.failed = true;
                break;
            }
            Some(0xFE) => {
                let sw = sprig_proto_mysql::packets::AuthSwitchRequest::decode(&p).unwrap();
                plugin = sw.plugin;
                let n = cauth::nonce_of(&sw.data).to_vec();
                let r = cauth::first_response(&plugin, login.pass.as_bytes(), &n, false)
                    .expect("switch to a plugin this client knows");
                c.write(Bytes::from(r)).await;
            }
            Some(0x01) => match p.get(1) {
                Some(&cauth::FAST_AUTH_SUCCESS) => {}
                Some(&cauth::PERFORM_FULL_AUTH) if login.full_garbage => {
                    c.write(Bytes::from(vec![0x55; 256])).await;
                }
                Some(&cauth::PERFORM_FULL_AUTH) if login.rsa_probe => {
                    c.write(Bytes::from_static(&[0x02])).await;
                    let _pem = c.read().await;
                    c.failed = true;
                    break;
                }
                other => panic!("{addr}: AuthMoreData {other:?} without TLS"),
            },
            other => panic!("{addr}: auth packet {other:?}"),
        }
    }
    if !c.failed {
        if caps.contains(Caps::COMPRESS) {
            c.framing = Framing::Zlib { level: 6 };
        } else if caps.contains(Caps::ZSTD_COMPRESSION_ALGORITHM) {
            c.framing = Framing::Zstd {
                level: i32::from(login.zstd_level.unwrap_or(3)),
            };
        }
        c.codec.set_framing(c.framing);
    }
    let lines = std::mem::take(&mut c.lines);
    (c, lines)
}

// ---------------------------------------------------------------------------
// Writing fixtures
// ---------------------------------------------------------------------------

struct Capture {
    target: Target,
    dir: PathBuf,
    n: usize,
}

impl Capture {
    fn new(target: &Target) -> Self {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/golden")
            .join(&target.label);
        if dir.exists() {
            for f in std::fs::read_dir(&dir).unwrap().flatten() {
                if f.path().extension().is_some_and(|e| e == "hex") {
                    std::fs::remove_file(f.path()).unwrap();
                }
            }
        }
        std::fs::create_dir_all(&dir).unwrap();
        Capture {
            target: target.clone(),
            dir,
            n: 0,
        }
    }

    fn login(&self) -> Login {
        Login {
            user: self.target.user.clone(),
            pass: self.target.pass.clone(),
            ..Login::default()
        }
    }

    fn save(&mut self, name: &str, c: &Conn, kind: Kind, command: &str, lines: Vec<Line>) {
        self.save_with(name, c, kind, command, lines, Vec::new());
    }

    fn save_with(
        &mut self,
        name: &str,
        c: &Conn,
        kind: Kind,
        command: &str,
        lines: Vec<Line>,
        binary_types: Vec<(u8, bool)>,
    ) {
        self.n += 1;
        let f = Fixture {
            target: self.target.label.clone(),
            server: c.server.clone(),
            server_caps: c.server_caps,
            caps: c.caps,
            kind: Some(kind),
            framing: match c.framing {
                Framing::Plain => "plain".into(),
                Framing::Zlib { .. } => "zlib".into(),
                Framing::Zstd { .. } => "zstd".into(),
            },
            command: command.to_owned(),
            binary_types,
            lines,
            ..Fixture::default()
        };
        let path = self.dir.join(format!("{:03}_{name}.hex", self.n));
        std::fs::write(&path, f.render()).unwrap();
        eprintln!("  {}", path.display());
    }

    async fn q(&mut self, c: &mut Conn, name: &str, sql: &str) {
        let lines = c.query(sql).await;
        self.save(name, c, Kind::Query, &format!("COM_QUERY {sql}"), lines);
    }

    async fn cmd(&mut self, c: &mut Conn, name: &str, desc: &str, payload: Vec<u8>) {
        let kind = Kind::for_command(payload[0]);
        let lines = c.command(payload.into()).await;
        self.save(name, c, kind, desc, lines);
    }

    async fn connect_fixture(&mut self, name: &str, login: &Login) -> Conn {
        let (c, lines) = connect(&self.target.addr, login).await;
        let desc = format!(
            "connect user={} plugin={} add={:#x} remove={:#x} db={:?}",
            login.user,
            login.plugin.as_deref().unwrap_or("(greeting's)"),
            login.add.bits(),
            login.remove.bits(),
            login.database
        );
        self.save(name, &c, Kind::Connect, &desc, lines);
        c
    }
}

fn execute(stmt_id: u32, flags: u8, params: Vec<(u16, Value)>) -> Vec<u8> {
    let cmd = Command::StmtExecute(StmtExecute {
        stmt_id,
        flags,
        iterations: 1,
        raw_params: Bytes::new(),
        params: Some(ExecuteParams {
            new_params_bound: !params.is_empty(),
            params: params
                .into_iter()
                .map(|(t, v)| BoundParam {
                    param_type: t,
                    name: Bytes::new(),
                    value: ParamValue::Value(v),
                })
                .collect(),
        }),
    });
    cmd.encode(Caps::PROTOCOL_41).unwrap().to_vec()
}

fn bytes_param(s: &str) -> (u16, Value) {
    (
        u16::from(ct::VAR_STRING),
        Value::Bytes(Bytes::copy_from_slice(s.as_bytes())),
    )
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

const ALL_TYPES_MARIADB: &str = "CREATE TABLE all_types (
  id INT NOT NULL PRIMARY KEY,
  c_tinyint TINYINT, c_utinyint TINYINT UNSIGNED, c_smallint SMALLINT,
  c_mediumint MEDIUMINT, c_int INT, c_uint INT UNSIGNED ZEROFILL, c_bigint BIGINT,
  c_ubigint BIGINT UNSIGNED, c_decimal DECIMAL(12,4), c_float FLOAT, c_double DOUBLE,
  c_bit BIT(10), c_bool BOOL, c_date DATE, c_time TIME(3), c_datetime DATETIME(6),
  c_timestamp TIMESTAMP(2) NULL, c_year YEAR, c_char CHAR(4), c_varchar VARCHAR(20),
  c_binary BINARY(3), c_varbinary VARBINARY(8), c_tinytext TINYTEXT, c_text TEXT,
  c_mediumblob MEDIUMBLOB, c_longtext LONGTEXT, c_enum ENUM('x','y'), c_set SET('a','b','c'),
  c_json JSON, c_geometry GEOMETRY, c_point POINT, c_uuid UUID, c_inet4 INET4, c_inet6 INET6,
  c_vector VECTOR(2), c_neg_time TIME, c_zero_dt DATETIME
) CHARSET utf8mb4";

const ALL_TYPES_MARIADB_ROW: &str = "INSERT INTO all_types VALUES (1,
  -128, 255, -32768, -8388608, -2147483648, 42, -9223372036854775808,
  18446744073709551615, -12345678.1234, 3.5, -2.25e-300,
  b'1010101010', TRUE, '2024-02-29', '838:59:58.999', '2024-12-31 23:59:59.123456',
  '2038-01-19 03:14:07.99', 2155, 'ab', 'héllo wörld', X'00FF10', X'DEADBEEF', 'tiny', 'text',
  X'000102', REPEAT('L', 300), 'y', 'a,c',
  '{\"k\": [1, 2.5, null]}', ST_GeomFromText('LINESTRING(0 0,1 1)'), POINT(1, 2),
  '123e4567-e89b-12d3-a456-426614174000', '192.168.0.1', '2001:db8::1',
  VEC_FromText('[1.5,-2]'), '-12:34:56', '2024-01-01 00:00:00'),
  (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)";

const ALL_TYPES_MYSQL: &str = "CREATE TABLE all_types (
  id INT NOT NULL PRIMARY KEY,
  c_tinyint TINYINT, c_utinyint TINYINT UNSIGNED, c_smallint SMALLINT,
  c_mediumint MEDIUMINT, c_int INT, c_uint INT UNSIGNED ZEROFILL, c_bigint BIGINT,
  c_ubigint BIGINT UNSIGNED, c_decimal DECIMAL(12,4), c_float FLOAT, c_double DOUBLE,
  c_bit BIT(10), c_bool BOOL, c_date DATE, c_time TIME(3), c_datetime DATETIME(6),
  c_timestamp TIMESTAMP(2) NULL, c_year YEAR, c_char CHAR(4), c_varchar VARCHAR(20),
  c_binary BINARY(3), c_varbinary VARBINARY(8), c_tinytext TINYTEXT, c_text TEXT,
  c_mediumblob MEDIUMBLOB, c_longtext LONGTEXT, c_enum ENUM('x','y'), c_set SET('a','b','c'),
  c_json JSON, c_geometry GEOMETRY, c_point POINT, c_neg_time TIME, c_zero_dt DATETIME
) CHARSET utf8mb4";

const ALL_TYPES_MYSQL_ROW: &str = "INSERT INTO all_types VALUES (1,
  -128, 255, -32768, -8388608, -2147483648, 42, -9223372036854775808,
  18446744073709551615, -12345678.1234, 3.5, -2.25e-300,
  b'1010101010', TRUE, '2024-02-29', '838:59:58.999', '2024-12-31 23:59:59.123456',
  '2038-01-19 03:14:07.99', 2155, 'ab', 'héllo wörld', X'00FF10', X'DEADBEEF', 'tiny', 'text',
  X'000102', REPEAT('L', 300), 'y', 'a,c',
  '{\"k\": [1, 2.5, null]}', ST_GeomFromText('LINESTRING(0 0,1 1)'), POINT(1, 2),
  '-12:34:56', '2024-01-01 00:00:00'),
  (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL)";

async fn capture(t: &Target) {
    eprintln!("capturing {} at {}", t.label, t.addr);
    let mut cap = Capture::new(t);
    let base = cap.login();
    let mysql = {
        // Before priming, MySQL takes the full path: stop at the RSA key.
        let probe = Login {
            rsa_probe: true,
            ..base.clone()
        };
        let (c, _) = connect(&t.addr, &probe).await;
        let m = !c.server.contains("MariaDB");
        drop(c);
        m
    };

    // --- connection phase -------------------------------------------------
    if mysql {
        // Prime the caching_sha2_password fast-auth cache with one TLS login.
        let mut cfg = ClientConfig::new(t.user.clone(), t.pass.clone());
        cfg.tls = TlsMode::Required;
        cfg.tls_config = Some(
            TlsOptions {
                skip_verify: true,
                ..TlsOptions::default()
            }
            .build()
            .unwrap(),
        );
        cfg.connect_timeout = TIMEOUT;
        let mut primed = MysqlClient::connect(&t.addr, &cfg)
            .await
            .expect("TLS login");
        primed
            .query_drop(
                "DROP USER IF EXISTS golden_full; \
                 CREATE USER golden_full IDENTIFIED WITH caching_sha2_password BY 'full-pw'",
            )
            .await
            .unwrap();
        primed.quit().await;
        // Account changes flush the fast-auth cache: log in once more.
        let mut primed = MysqlClient::connect(&t.addr, &cfg)
            .await
            .expect("TLS login");
        primed.ping().await.unwrap();
        primed.quit().await;
    }
    let c = cap.connect_fixture("connect_default", &base).await;
    assert!(!c.failed, "login failed");
    drop(c);
    let wrong = Login {
        pass: "wrong-password".into(),
        full_garbage: mysql,
        ..base.clone()
    };
    cap.connect_fixture("connect_wrong_password", &wrong).await;
    let other_plugin = Login {
        plugin: Some(if mysql {
            cauth::NATIVE.into()
        } else {
            cauth::CACHING_SHA2.into()
        }),
        ..base.clone()
    };
    let c = cap
        .connect_fixture("connect_auth_switch", &other_plugin)
        .await;
    assert!(!c.failed, "switched login failed");
    drop(c);
    let bad_db = Login {
        database: Some("sprig_golden_missing".into()),
        ..base.clone()
    };
    cap.connect_fixture("connect_unknown_database", &bad_db)
        .await;
    if mysql {
        let full = Login {
            user: "golden_full".into(),
            pass: "full-pw".into(),
            rsa_probe: true,
            ..Login::default()
        };
        cap.connect_fixture("connect_sha2_full_path_rsa_key", &full)
            .await;
    }

    // --- session tracking -------------------------------------------------
    let mut c = cap.connect_fixture("connect_main", &base).await;
    let ver = c.server.clone();
    cap.q(
        &mut c,
        "q1_collations",
        if mysql {
            "SELECT @@character_set_server, @@collation_server, @@collation_connection, \
         @@default_collation_for_utf8mb4, @@character_set_client"
        } else {
            "SELECT @@character_set_server, @@collation_server, @@collation_connection, \
         @@character_set_collations, @@character_set_client"
        },
    )
    .await;
    cap.q(
        &mut c,
        "q1_default_utf8mb4_collation",
        "SELECT COLLATION_NAME, ID FROM information_schema.COLLATIONS \
         WHERE CHARACTER_SET_NAME = 'utf8mb4' AND IS_DEFAULT = 'Yes'",
    )
    .await;
    if !mysql {
        cap.q(
            &mut c,
            "q1_show_character_set",
            "SHOW CHARACTER SET LIKE 'utf8mb4'",
        )
        .await;
        cap.q(
            &mut c,
            "q1_collation_ids",
            "SELECT FULL_COLLATION_NAME, ID, IS_DEFAULT \
             FROM information_schema.COLLATION_CHARACTER_SET_APPLICABILITY \
             WHERE FULL_COLLATION_NAME IN ('utf8mb4_general_ci', 'utf8mb4_uca1400_ai_ci') ORDER BY ID",
        )
        .await;
    }
    cap.q(
        &mut c,
        "ok_track_schema",
        "SET SESSION session_track_schema = ON",
    )
    .await;
    cap.q(
        &mut c,
        "ok_track_state_change",
        "SET SESSION session_track_state_change = ON",
    )
    .await;
    cap.q(
        &mut c,
        "ok_track_system_variables",
        "SET SESSION session_track_system_variables = '*'",
    )
    .await;
    cap.q(
        &mut c,
        "ok_track_transaction_info",
        "SET SESSION session_track_transaction_info = CHARACTERISTICS",
    )
    .await;
    if mysql {
        cap.q(
            &mut c,
            "ok_track_gtids",
            "SET SESSION session_track_gtids = OWN_GTID",
        )
        .await;
    }
    c.exec("DROP DATABASE IF EXISTS sprig_golden").await;
    cap.q(&mut c, "ok_create_database", "CREATE DATABASE sprig_golden")
        .await;
    cap.q(&mut c, "ok_use", "USE sprig_golden").await;
    cap.cmd(
        &mut c,
        "ok_com_init_db",
        "COM_INIT_DB sprig_golden",
        b"\x02sprig_golden".to_vec(),
    )
    .await;
    cap.cmd(
        &mut c,
        "err_com_init_db_unknown",
        "COM_INIT_DB sprig_golden_missing",
        b"\x02sprig_golden_missing".to_vec(),
    )
    .await;
    cap.q(
        &mut c,
        "ok_set_variable",
        "SET SESSION time_zone = '+05:30'",
    )
    .await;
    cap.q(&mut c, "ok_set_user_variable", "SET @golden = 1")
        .await;
    cap.q(
        &mut c,
        "ok_sql_mode_ansi_quotes",
        "SET SESSION sql_mode = CONCAT(@@sql_mode, ',ANSI_QUOTES,NO_BACKSLASH_ESCAPES')",
    )
    .await;
    cap.q(
        &mut c,
        "ok_sql_mode_restore",
        "SET SESSION sql_mode = DEFAULT",
    )
    .await;
    cap.q(
        &mut c,
        "ok_temporary_table",
        "CREATE TEMPORARY TABLE tt (a INT)",
    )
    .await;
    cap.q(
        &mut c,
        "ok_create_table",
        "CREATE TABLE kv (id INT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(20) UNIQUE)",
    )
    .await;
    cap.q(&mut c, "ok_begin", "BEGIN").await;
    cap.q(
        &mut c,
        "ok_insert_last_id",
        "INSERT INTO kv (v) VALUES ('a'), ('b')",
    )
    .await;
    cap.q(
        &mut c,
        "ok_update_info",
        "UPDATE kv SET v = 'c' WHERE v = 'b'",
    )
    .await;
    cap.q(&mut c, "ok_commit", "COMMIT").await;
    cap.q(&mut c, "ok_autocommit_off", "SET autocommit = 0")
        .await;
    cap.q(
        &mut c,
        "ok_insert_in_implicit_trx",
        "INSERT INTO kv (v) VALUES ('d')",
    )
    .await;
    cap.q(&mut c, "ok_rollback", "ROLLBACK").await;
    cap.q(&mut c, "ok_autocommit_on", "SET autocommit = 1")
        .await;
    cap.q(
        &mut c,
        "ok_set_transaction_characteristics",
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
    )
    .await;
    cap.q(&mut c, "ok_start_read_only", "START TRANSACTION READ ONLY")
        .await;
    cap.q(&mut c, "ok_rollback_read_only", "ROLLBACK").await;
    cap.q(&mut c, "ok_warning", "DO 1/0").await;

    // --- resultsets -------------------------------------------------------
    c.exec(if mysql {
        ALL_TYPES_MYSQL
    } else {
        ALL_TYPES_MARIADB
    })
    .await;
    c.exec(if mysql {
        ALL_TYPES_MYSQL_ROW
    } else {
        ALL_TYPES_MARIADB_ROW
    })
    .await;
    cap.q(
        &mut c,
        "rs_text_all_types",
        "SELECT * FROM all_types ORDER BY id",
    )
    .await;
    cap.q(
        &mut c,
        "rs_text_literals",
        "SELECT 1 AS i, -1.5 AS d, 'x' AS s, NULL AS n, X'00FF' AS b, \
         CAST('2024-01-02' AS DATE) AS dt, 18446744073709551615 AS big, \
         REPEAT('z', 300) AS long_text, '' AS empty",
    )
    .await;
    cap.q(
        &mut c,
        "rs_text_empty",
        "SELECT id FROM all_types WHERE id < 0",
    )
    .await;
    cap.q(&mut c, "rs_text_warning", "SELECT 1/0 AS x").await;
    let (ok, cols, lines) = c
        .prepare("SELECT * FROM all_types WHERE id >= ? ORDER BY id")
        .await;
    cap.save(
        "prepare_all_types",
        &c,
        Kind::Prepare,
        "COM_STMT_PREPARE SELECT * FROM all_types WHERE id >= ?",
        lines,
    );
    let lines = c
        .command(
            execute(
                ok.stmt_id,
                0,
                vec![(u16::from(ct::LONGLONG), Value::Int(0))],
            )
            .into(),
        )
        .await;
    cap.save(
        "execute_all_types",
        &c,
        Kind::Execute,
        "COM_STMT_EXECUTE all_types id >= 0",
        lines,
    );
    // A cursor: column definitions, then rows on COM_STMT_FETCH.
    let lines = c
        .command(
            execute(
                ok.stmt_id,
                0x01,
                vec![(u16::from(ct::LONGLONG), Value::Int(0))],
            )
            .into(),
        )
        .await;
    cap.save(
        "execute_cursor",
        &c,
        Kind::Execute,
        "COM_STMT_EXECUTE all_types CURSOR_TYPE_READ_ONLY",
        lines,
    );
    let types: Vec<(u8, bool)> = cols.iter().map(|c| (c.col_type, c.is_unsigned())).collect();
    let mut fetch = vec![0x1C];
    fetch.extend_from_slice(&ok.stmt_id.to_le_bytes());
    fetch.extend_from_slice(&1u32.to_le_bytes());
    let lines = c.command(fetch.clone().into()).await;
    cap.save_with(
        "fetch_cursor_one_row",
        &c,
        Kind::Fetch,
        "COM_STMT_FETCH 1 row",
        lines,
        types.clone(),
    );
    let mut fetch10 = fetch.clone();
    fetch10[5..9].copy_from_slice(&10u32.to_le_bytes());
    let lines = c.command(fetch10.into()).await;
    cap.save_with(
        "fetch_cursor_rest",
        &c,
        Kind::Fetch,
        "COM_STMT_FETCH 10 rows",
        lines,
        types,
    );
    let mut reset = vec![0x1A];
    reset.extend_from_slice(&ok.stmt_id.to_le_bytes());
    cap.cmd(&mut c, "ok_stmt_reset", "COM_STMT_RESET", reset)
        .await;
    let mut close = vec![0x19];
    close.extend_from_slice(&ok.stmt_id.to_le_bytes());
    cap.cmd(&mut c, "stmt_close", "COM_STMT_CLOSE (no response)", close)
        .await;
    cap.cmd(
        &mut c,
        "err_execute_unknown_stmt",
        "COM_STMT_EXECUTE unknown id",
        execute(ok.stmt_id + 1000, 0, vec![]),
    )
    .await;
    let (ok, _, lines) = c
        .prepare("SELECT ? AS i, ? AS d, ? AS s, ? AS dt, ? AS t, ? AS n, ? AS u")
        .await;
    cap.save(
        "prepare_params",
        &c,
        Kind::Prepare,
        "COM_STMT_PREPARE SELECT ?,?,?,?,?,?,?",
        lines,
    );
    let params = vec![
        (u16::from(ct::LONGLONG), Value::Int(-7)),
        (u16::from(ct::DOUBLE), Value::Double(2.5)),
        bytes_param("str"),
        (
            u16::from(ct::DATETIME),
            Value::DateTime(sprig_proto_mysql::resultset::DateTime {
                year: 2024,
                month: 5,
                day: 6,
                hour: 7,
                minute: 8,
                second: 9,
                micro: 10,
            }),
        ),
        (
            u16::from(ct::TIME),
            Value::Time(sprig_proto_mysql::resultset::Time {
                negative: true,
                days: 1,
                hour: 2,
                minute: 3,
                second: 4,
                micro: 0,
            }),
        ),
        (u16::from(ct::NULL), Value::Null),
        (u16::from(ct::LONGLONG) | 0x8000, Value::UInt(u64::MAX)),
    ];
    let lines = c.command(execute(ok.stmt_id, 0, params).into()).await;
    cap.save(
        "execute_params",
        &c,
        Kind::Execute,
        "COM_STMT_EXECUTE typed params",
        lines,
    );
    let lines = c.command(execute(ok.stmt_id, 0, vec![]).into()).await;
    cap.save(
        "execute_params_rebound_missing",
        &c,
        Kind::Execute,
        "COM_STMT_EXECUTE without types or values",
        lines,
    );
    let mut p = vec![0x16];
    p.extend_from_slice(b"SELECT * FROM missing_table");
    cap.cmd(&mut c, "err_prepare", "COM_STMT_PREPARE missing table", p)
        .await;
    let (ok, _, lines) = c.prepare("INSERT INTO kv (v) VALUES (?)").await;
    cap.save(
        "prepare_insert",
        &c,
        Kind::Prepare,
        "COM_STMT_PREPARE INSERT ?",
        lines,
    );
    let lines = c
        .command(execute(ok.stmt_id, 0, vec![bytes_param("e")]).into())
        .await;
    cap.save(
        "execute_insert_ok",
        &c,
        Kind::Execute,
        "COM_STMT_EXECUTE INSERT",
        lines,
    );

    // --- multiple results, CALL -------------------------------------------
    cap.q(
        &mut c,
        "multi_results",
        "SELECT 1 AS a; SELECT 'x' AS b, 2 AS c; DO 0",
    )
    .await;
    cap.q(
        &mut c,
        "multi_results_error",
        "SELECT 1 AS a; SELECT * FROM missing_table; SELECT 2",
    )
    .await;
    c.exec("CREATE PROCEDURE p2() BEGIN SELECT 1 AS a; SELECT 2 AS b, 'z' AS c; END")
        .await;
    c.exec("CREATE PROCEDURE pout(IN a INT, OUT b INT, INOUT c VARCHAR(10)) BEGIN SET b = a * 2; SET c = CONCAT(c, '!'); SELECT a AS echoed; END").await;
    cap.q(&mut c, "call_two_resultsets", "CALL p2()").await;
    let (ok, _, lines) = c.prepare("CALL p2()").await;
    cap.save(
        "prepare_call",
        &c,
        Kind::Prepare,
        "COM_STMT_PREPARE CALL p2()",
        lines,
    );
    let lines = c.command(execute(ok.stmt_id, 0, vec![]).into()).await;
    cap.save(
        "execute_call_two_resultsets",
        &c,
        Kind::Execute,
        "COM_STMT_EXECUTE CALL p2()",
        lines,
    );
    let (ok, _, lines) = c.prepare("CALL pout(?, ?, ?)").await;
    cap.save(
        "prepare_call_out_params",
        &c,
        Kind::Prepare,
        "COM_STMT_PREPARE CALL pout(?,?,?)",
        lines,
    );
    let lines = c
        .command(
            execute(
                ok.stmt_id,
                0,
                vec![
                    (u16::from(ct::LONGLONG), Value::Int(21)),
                    (u16::from(ct::NULL), Value::Null),
                    bytes_param("hi"),
                ],
            )
            .into(),
        )
        .await;
    cap.save(
        "execute_call_out_params",
        &c,
        Kind::Execute,
        "COM_STMT_EXECUTE CALL pout(21, ?, 'hi')",
        lines,
    );

    // --- errors -------------------------------------------------------------
    cap.q(&mut c, "err_no_such_table", "SELECT * FROM missing_table")
        .await;
    cap.q(&mut c, "err_syntax", "SELEC 1").await;
    cap.q(
        &mut c,
        "err_duplicate_key",
        "INSERT INTO kv (v) VALUES ('a')",
    )
    .await;
    cap.q(
        &mut c,
        "err_long_message",
        &format!("SELECT * FROM `{}`", "x".repeat(60)),
    )
    .await;

    // --- LOCAL INFILE -----------------------------------------------------
    c.exec("CREATE TABLE li (a INT)").await;
    c.infile = b"1\n2\n3\n".to_vec();
    cap.q(
        &mut c,
        "local_infile",
        "LOAD DATA LOCAL INFILE 'golden.csv' INTO TABLE li",
    )
    .await;
    c.infile.clear();
    cap.q(
        &mut c,
        "local_infile_declined",
        "LOAD DATA LOCAL INFILE 'declined.csv' INTO TABLE li",
    )
    .await;

    // --- other commands ---------------------------------------------------
    cap.cmd(
        &mut c,
        "field_list",
        "COM_FIELD_LIST all_types",
        b"\x04all_types\x00".to_vec(),
    )
    .await;
    cap.cmd(
        &mut c,
        "field_list_wildcard",
        "COM_FIELD_LIST kv v%",
        b"\x04kv\x00v%".to_vec(),
    )
    .await;
    cap.cmd(
        &mut c,
        "err_field_list_missing",
        "COM_FIELD_LIST missing",
        b"\x04missing_table\x00".to_vec(),
    )
    .await;
    cap.cmd(&mut c, "statistics", "COM_STATISTICS", vec![0x09])
        .await;
    cap.cmd(&mut c, "ok_ping", "COM_PING", vec![0x0E]).await;
    cap.cmd(
        &mut c,
        "set_option_off",
        "COM_SET_OPTION MULTI_STATEMENTS_OFF",
        vec![0x1B, 1, 0],
    )
    .await;
    cap.q(&mut c, "err_multi_statements_off", "SELECT 1; SELECT 2")
        .await;
    cap.cmd(
        &mut c,
        "set_option_on",
        "COM_SET_OPTION MULTI_STATEMENTS_ON",
        vec![0x1B, 0, 0],
    )
    .await;
    cap.cmd(
        &mut c,
        "err_process_kill_unknown",
        "COM_PROCESS_KILL 99999999",
        vec![0x0C, 0xFF, 0xE0, 0xF5, 0x05],
    )
    .await;
    cap.cmd(&mut c, "com_process_info", "COM_PROCESS_INFO", vec![0x0A])
        .await;
    cap.cmd(
        &mut c,
        "q4_com_create_db",
        "COM_CREATE_DB sprig_golden_comdb",
        b"\x05sprig_golden_comdb".to_vec(),
    )
    .await;
    cap.cmd(
        &mut c,
        "q4_com_drop_db",
        "COM_DROP_DB sprig_golden_comdb",
        b"\x06sprig_golden_comdb".to_vec(),
    )
    .await;
    cap.cmd(&mut c, "err_unknown_command", "command 0x20", vec![0x20])
        .await;
    cap.cmd(
        &mut c,
        "com_refresh_no_flags",
        "COM_REFRESH 0",
        vec![0x07, 0x00],
    )
    .await;
    cap.cmd(
        &mut c,
        "ok_reset_connection",
        "COM_RESET_CONNECTION",
        vec![0x1F],
    )
    .await;
    c.exec("USE sprig_golden").await;

    // --- persona specifics --------------------------------------------------
    if mysql {
        // Query attributes.
        if c.caps.contains(Caps::QUERY_ATTRIBUTES) {
            let cmd = Command::Query(
                QueryAttrs {
                    attrs: vec![BoundParam {
                        param_type: u16::from(ct::VAR_STRING),
                        name: Bytes::from_static(b"traceparent"),
                        value: ParamValue::Value(Value::Bytes(Bytes::from_static(b"00-abc-01"))),
                    }],
                },
                Bytes::from_static(b"SELECT mysql_query_attribute_string('traceparent') AS tp"),
            );
            let p = cmd.encode(c.caps).unwrap().to_vec();
            cap.cmd(
                &mut c,
                "query_attributes",
                "COM_QUERY with attribute traceparent",
                p,
            )
            .await;
        }
    } else {
        mariadb_q3(&mut cap, &mut c).await;
    }

    // --- EOF packets (no DEPRECATE_EOF), no extended metadata --------------
    let eof_login = Login {
        remove: Caps::DEPRECATE_EOF | Caps::MARIADB_CLIENT_EXTENDED_METADATA,
        database: Some("sprig_golden".into()),
        ..base.clone()
    };
    let mut e = cap.connect_fixture("connect_eof", &eof_login).await;
    cap.q(
        &mut e,
        "eof_rs_text_all_types",
        "SELECT * FROM all_types ORDER BY id",
    )
    .await;
    cap.q(
        &mut e,
        "eof_rs_text_empty",
        "SELECT id FROM all_types WHERE id < 0",
    )
    .await;
    cap.q(
        &mut e,
        "eof_multi_results",
        "SELECT 1 AS a; SELECT 'x' AS b",
    )
    .await;
    cap.q(&mut e, "eof_call_two_resultsets", "CALL p2()").await;
    let (ok, cols, lines) = e
        .prepare("SELECT * FROM all_types WHERE id >= ? ORDER BY id")
        .await;
    cap.save(
        "eof_prepare_all_types",
        &e,
        Kind::Prepare,
        "COM_STMT_PREPARE all_types",
        lines,
    );
    let lines = e
        .command(
            execute(
                ok.stmt_id,
                0,
                vec![(u16::from(ct::LONGLONG), Value::Int(0))],
            )
            .into(),
        )
        .await;
    cap.save(
        "eof_execute_all_types",
        &e,
        Kind::Execute,
        "COM_STMT_EXECUTE all_types",
        lines,
    );
    let lines = e
        .command(
            execute(
                ok.stmt_id,
                0x01,
                vec![(u16::from(ct::LONGLONG), Value::Int(0))],
            )
            .into(),
        )
        .await;
    cap.save(
        "eof_execute_cursor",
        &e,
        Kind::Execute,
        "COM_STMT_EXECUTE CURSOR_TYPE_READ_ONLY",
        lines,
    );
    let types: Vec<(u8, bool)> = cols.iter().map(|c| (c.col_type, c.is_unsigned())).collect();
    let mut fetch = vec![0x1C];
    fetch.extend_from_slice(&ok.stmt_id.to_le_bytes());
    fetch.extend_from_slice(&10u32.to_le_bytes());
    let lines = e.command(fetch.into()).await;
    cap.save_with(
        "eof_fetch_cursor",
        &e,
        Kind::Fetch,
        "COM_STMT_FETCH 10 rows",
        lines,
        types,
    );
    cap.cmd(
        &mut e,
        "eof_field_list",
        "COM_FIELD_LIST kv",
        b"\x04kv\x00".to_vec(),
    )
    .await;
    cap.cmd(
        &mut e,
        "eof_set_option",
        "COM_SET_OPTION MULTI_STATEMENTS_ON",
        vec![0x1B, 0, 0],
    )
    .await;
    drop(e);

    // --- no LOCAL_FILES capability ----------------------------------------
    let nolocal = Login {
        remove: Caps::LOCAL_FILES,
        database: Some("sprig_golden".into()),
        ..base.clone()
    };
    let mut n = cap
        .connect_fixture("connect_no_local_files", &nolocal)
        .await;
    cap.q(
        &mut n,
        "err_local_infile_without_client_capability",
        "LOAD DATA LOCAL INFILE 'golden.csv' INTO TABLE li",
    )
    .await;
    drop(n);

    // --- compression ------------------------------------------------------
    let zlib = Login {
        add: Caps::COMPRESS,
        database: Some("sprig_golden".into()),
        ..base.clone()
    };
    let mut z = cap.connect_fixture("connect_zlib", &zlib).await;
    cap.q(&mut z, "zlib_small_uncompressed", "SELECT 1").await;
    cap.q(
        &mut z,
        "zlib_rs_text_all_types",
        "SELECT * FROM all_types ORDER BY id",
    )
    .await;
    cap.q(
        &mut z,
        "zlib_large_row",
        "SELECT REPEAT('abc', 30000) AS big",
    )
    .await;
    drop(z);
    if mysql {
        let zstd = Login {
            zstd_level: Some(7),
            database: Some("sprig_golden".into()),
            ..base.clone()
        };
        let mut z = cap.connect_fixture("connect_zstd", &zstd).await;
        cap.q(
            &mut z,
            "zstd_rs_text_all_types",
            "SELECT * FROM all_types ORDER BY id",
        )
        .await;
        cap.q(
            &mut z,
            "zstd_large_row",
            "SELECT REPEAT('abc', 30000) AS big",
        )
        .await;
        drop(z);
    }

    // --- throwaway-only: global settings (open questions 5 and 8) ----------
    if t.throwaway {
        if mysql {
            cap.q(
                &mut c,
                "q5_local_infile_disabled_global",
                "SELECT @@GLOBAL.local_infile",
            )
            .await;
            c.infile = b"1\n".to_vec();
            cap.q(
                &mut c,
                "q5_err_local_infile_disabled",
                "LOAD DATA LOCAL INFILE 'golden.csv' INTO TABLE li",
            )
            .await;
            c.infile.clear();
        } else {
            c.exec("SET GLOBAL local_infile = 0").await;
            let mut d = cap
                .connect_fixture("connect_local_infile_off", &with_db(&base))
                .await;
            cap.q(
                &mut d,
                "q5_err_local_infile_disabled",
                "LOAD DATA LOCAL INFILE 'golden.csv' INTO TABLE li",
            )
            .await;
            drop(d);
            c.exec("SET GLOBAL local_infile = 1").await;
        }
        c.exec("SET GLOBAL require_secure_transport = ON").await;
        let r = cap
            .connect_fixture("q8_err_require_secure_transport", &base)
            .await;
        assert!(
            r.failed,
            "plaintext login accepted under require_secure_transport"
        );
        drop(r);
        c.exec("SET GLOBAL require_secure_transport = OFF").await;
    }

    c.exec("DROP DATABASE IF EXISTS sprig_golden").await;
    if mysql {
        c.exec("DROP USER IF EXISTS golden_full").await;
    }
    eprintln!("{}: {} fixtures from {ver}", t.label, cap.n);
}

fn with_db(base: &Login) -> Login {
    Login {
        database: Some("sprig_golden".into()),
        ..base.clone()
    }
}

/// MariaDB: `COM_STMT_EXECUTE` with statement id 0xFFFFFFFF (open question
/// 3), alone and pipelined after its `COM_STMT_PREPARE`, and
/// `COM_STMT_BULK_EXECUTE` with and without `BULK_SEND_UNIT_RESULTS`.
async fn mariadb_q3(cap: &mut Capture, c: &mut Conn) {
    c.exec("CREATE TABLE bulk_t (id INT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(10) DEFAULT 'dflt')")
        .await;
    let (_, _, lines) = c.prepare("SELECT ? + 1 AS next").await;
    cap.save(
        "q3_prepare_for_last",
        c,
        Kind::Prepare,
        "COM_STMT_PREPARE SELECT ?+1",
        lines,
    );
    let lines = c
        .command(execute(u32::MAX, 0, vec![(u16::from(ct::LONGLONG), Value::Int(41))]).into())
        .await;
    cap.save(
        "q3_execute_last_prepared",
        c,
        Kind::Execute,
        "COM_STMT_EXECUTE id 0xFFFFFFFF",
        lines,
    );

    // Pipelined: PREPARE and EXECUTE(-1) in one write, as MariaDB
    // Connector/C's mariadb_stmt_execute_direct does.
    c.lines.clear();
    c.codec.reset_seq();
    let mut prep = vec![0x16];
    prep.extend_from_slice(b"SELECT ? * 2 AS doubled");
    let exec = execute(u32::MAX, 0, vec![(u16::from(ct::LONGLONG), Value::Int(21))]);
    let mut out = BytesMut::new();
    for p in [prep, exec] {
        c.codec.reset_seq();
        let seq = c.codec.next_seq();
        c.codec.encode(Bytes::from(p.clone()), &mut out).unwrap();
        c.lines.push(Line::C(seq, Bytes::from(p)));
    }
    c.tcp.write_all(&out).await.unwrap();
    c.codec.reset_seq();
    c.codec.set_next_seq(1);
    c.read_response(Kind::Prepare).await;
    c.codec.set_next_seq(1);
    c.read_response(Kind::Execute).await;
    let lines = std::mem::take(&mut c.lines);
    // The walk of this fixture is two responses; the replay handles it by
    // name.
    cap.save(
        "q3_direct_execute_pipelined",
        c,
        Kind::Prepare,
        "COM_STMT_PREPARE + COM_STMT_EXECUTE(-1) pipelined",
        lines,
    );

    // Bulk insert: types sent (128), rows with a value, NULL, DEFAULT.
    let (ok, _, lines) = c.prepare("INSERT INTO bulk_t (id, v) VALUES (?, ?)").await;
    cap.save(
        "q3_prepare_bulk",
        c,
        Kind::Prepare,
        "COM_STMT_PREPARE INSERT bulk_t (?, ?)",
        lines,
    );
    let bulk = |flags: u16, rows: &[&[u8]]| {
        let mut p = vec![0xFA];
        p.extend_from_slice(&ok.stmt_id.to_le_bytes());
        p.extend_from_slice(&flags.to_le_bytes());
        if flags & 128 != 0 {
            p.extend_from_slice(&[ct::LONG, 0, ct::VAR_STRING, 0]);
        }
        for r in rows {
            p.extend_from_slice(r);
        }
        p
    };
    // Row = id indicator [+ value], v indicator [+ value].
    let rows: [&[u8]; 3] = [
        b"\x02\x00\x01a",            // id DEFAULT, v 'a'
        b"\x00\x0a\x00\x00\x00\x01", // id 10, v NULL
        b"\x02\x02",                 // id DEFAULT, v DEFAULT
    ];
    cap.cmd(
        c,
        "q3_bulk_execute",
        "COM_STMT_BULK_EXECUTE flags=SEND_TYPES rows: (DEFAULT,'a'), (10,NULL), (DEFAULT,DEFAULT)",
        bulk(128, &rows),
    )
    .await;
    let rows2: [&[u8]; 2] = [b"\x02\x00\x01b", b"\x02\x00\x01c"];
    cap.cmd(
        c,
        "q3_bulk_execute_unit_results",
        "COM_STMT_BULK_EXECUTE flags=SEND_TYPES|SEND_UNIT_RESULTS",
        bulk(128 | 64, &rows2),
    )
    .await;
    let rows3: [&[u8]; 1] = [b"\x02\x00\x01d"];
    cap.cmd(
        c,
        "q3_bulk_execute_types_remembered",
        "COM_STMT_BULK_EXECUTE flags=0 (types from the last bulk)",
        bulk(0, &rows3),
    )
    .await;
    let dup: [&[u8]; 2] = [
        b"\x00\x0b\x00\x00\x00\x00\x01e",
        b"\x00\x0b\x00\x00\x00\x00\x01f",
    ];
    cap.cmd(
        c,
        "q3_bulk_execute_error_midway",
        "COM_STMT_BULK_EXECUTE duplicate key in row 2",
        bulk(128, &dup),
    )
    .await;
    cap.q(c, "q3_bulk_rows", "SELECT * FROM bulk_t ORDER BY id")
        .await;
    let _ = StatusFlags::empty();
}

#[tokio::test]
async fn capture_golden_fixtures() {
    let ts = targets();
    if ts.is_empty() {
        eprintln!("golden_capture: SPRIG_GOLDEN_CAPTURE is not set; skipping");
        return;
    }
    for t in &ts {
        capture(t).await;
    }
}
