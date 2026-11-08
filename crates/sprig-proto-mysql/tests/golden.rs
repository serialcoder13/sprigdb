//! Golden captures from MariaDB 11.8, MariaDB 12.3 and MySQL 8.4, replayed
//! through this crate's decoders and encoders (chapter 05, "Testing").
//!
//! The fixtures under `tests/golden/<target>/` were recorded by
//! `tests/golden_capture.rs`. Every server packet is classified by its
//! place in the response, decoded with the matching decoder, and
//! re-encoded: OK, EOF, ERR, column definitions, column counts, text and
//! binary rows, `PREPARE_OK`, the greeting, `AuthSwitchRequest` and
//! `AuthMoreData` must come back byte-identical. The client's own packets
//! (handshake responses, commands) must parse and re-encode identically
//! too. The one documented normalisation: a `COM_FIELD_LIST` column
//! definition's trailing default-value field is dropped (see
//! `field_list_defaults_are_the_only_normalisation`).
//!
//! The server streams are also replayed through `client::MysqlClient`, and
//! the compressed fixtures' raw wire bytes through the zlib and zstd codecs.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::needless_pass_by_value,
    clippy::ref_option,
    missing_docs
)]

#[path = "golden/fixture.rs"]
mod fixture;

use std::collections::BTreeMap;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_util::codec::Decoder as _;

use fixture::{Class, Fixture, Kind, Walker};
use sprig_proto_mysql::client::{
    ClientConfig, ClientStream, MysqlClient, PacketKind, TlsMode, decode_session_state,
};
use sprig_proto_mysql::command::Command;
use sprig_proto_mysql::error::ErrPacket;
use sprig_proto_mysql::packets::{
    AuthMoreData, AuthSwitchRequest, Caps, EofPacket, HandshakeResponse, HandshakeV10, OkPacket,
    Persona, SessionChange, StatusFlags,
};
use sprig_proto_mysql::resultset::{
    ColumnDef, PrepareOk, Terminator, column_type as ct, decode_binary_row,
    decode_binary_row_types, decode_column_count, decode_text_row, encode_binary_row,
    encode_binary_row_types, encode_column_count, encode_text_row,
};
use sprig_proto_mysql::{Framing, PacketCodec, Value};

fn persona_of(f: &Fixture) -> Persona {
    if f.server.contains("MariaDB") {
        Persona::MariaDb
    } else {
        Persona::MySql84
    }
}

/// Per-class counts: decoded, re-encoded byte-identically.
#[derive(Debug, Default)]
struct Stats {
    classes: BTreeMap<String, (usize, usize)>,
    normalised: Vec<String>,
    failures: Vec<String>,
}

impl Stats {
    fn check(&mut self, f: &Fixture, what: &str, got: &[u8], want: &[u8]) {
        let e = self.classes.entry(what.to_owned()).or_default();
        e.0 += 1;
        if got == want {
            e.1 += 1;
        } else {
            self.failures.push(format!(
                "{}/{}: {what} re-encodes differently\n  want {}\n  got  {}",
                f.target,
                f.name,
                fixture::hex(want),
                fixture::hex(got)
            ));
        }
    }

    fn fail(&mut self, f: &Fixture, msg: String) {
        self.failures
            .push(format!("{}/{}: {msg}", f.target, f.name));
    }
}

/// What one fixture's replay found, for the field assertions.
#[derive(Debug, Default)]
struct Replay {
    greeting: Option<HandshakeV10>,
    oks: Vec<OkPacket>,
    errs: Vec<ErrPacket>,
    columns: Vec<Vec<ColumnDef>>,
    text_rows: Vec<Vec<Option<Vec<u8>>>>,
    binary_rows: Vec<Vec<Value>>,
    ends: Vec<Terminator>,
    prepare_oks: Vec<PrepareOk>,
    auth_more: Vec<Bytes>,
    auth_switch: Vec<AuthSwitchRequest>,
    local_infile: Vec<Bytes>,
    classes: Vec<Class>,
}

fn replay(f: &Fixture, stats: &mut Stats) -> Replay {
    let caps = f.caps;
    let mut out = Replay::default();
    let kind = f.kind.expect("kind");
    let pipelined = f.name.contains("pipelined");
    let mut w = Walker::new(kind, caps);
    let mut cols: Vec<ColumnDef> = Vec::new();
    let mut fetch_types = f.binary_types.clone();
    let mut in_rs = false;
    for (_, p) in f.server_packets() {
        if w.done() && pipelined {
            w = Walker::new(Kind::Execute, caps);
        }
        let class = w.feed(&p);
        out.classes.push(class);
        match class {
            Class::Greeting => match HandshakeV10::decode(&p) {
                Ok(hs) => {
                    stats.check(f, "greeting", &hs.encode(), &p);
                    out.greeting = Some(hs);
                }
                Err(e) => stats.fail(f, format!("greeting: {e}")),
            },
            Class::AuthSwitch => match AuthSwitchRequest::decode(&p) {
                Ok(sw) => {
                    stats.check(f, "auth_switch", &sw.encode(), &p);
                    out.auth_switch.push(sw);
                }
                Err(e) => stats.fail(f, format!("auth switch: {e}")),
            },
            Class::AuthMoreData => match AuthMoreData::decode(&p) {
                Ok(m) => {
                    stats.check(f, "auth_more_data", &m.encode(), &p);
                    out.auth_more.push(m.data);
                }
                Err(e) => stats.fail(f, format!("auth more data: {e}")),
            },
            Class::Ok => match OkPacket::decode(&p, caps) {
                Ok(ok) => {
                    stats.check(f, "ok", &ok.encode(caps), &p);
                    check_session_state(f, stats, &ok);
                    out.oks.push(ok);
                }
                Err(e) => stats.fail(f, format!("OK: {e}")),
            },
            Class::Err => match ErrPacket::decode(&p) {
                Ok(e) => {
                    // Before the handshake response the client's caps are
                    // unknown and the SQLSTATE is absent.
                    let c = if p.get(3) == Some(&b'#') {
                        Caps::PROTOCOL_41
                    } else {
                        Caps::empty()
                    };
                    stats.check(f, "err", &e.encode(c), &p);
                    out.errs.push(e);
                }
                Err(e) => stats.fail(f, format!("ERR: {e}")),
            },
            Class::LocalInfile => match sprig_proto_mysql::resultset::classify_response(&p, caps) {
                Ok(sprig_proto_mysql::resultset::Response::LocalInfile(name)) => {
                    out.local_infile.push(name);
                }
                other => stats.fail(f, format!("LOCAL INFILE classified as {other:?}")),
            },
            Class::ColumnCount(n) => {
                match decode_column_count(&p) {
                    Ok(m) if m == n => stats.check(f, "column_count", &encode_column_count(m), &p),
                    other => stats.fail(f, format!("column count: {other:?}")),
                }
                cols.clear();
                in_rs = true;
            }
            Class::ColumnDef | Class::ParamDef | Class::FieldDef => {
                match ColumnDef::decode(&p, caps) {
                    Ok(c) => {
                        let e = c.encode(caps);
                        if class == Class::FieldDef
                            && e.len() < p.len()
                            && p.starts_with(&e[..e.len()])
                        {
                            stats.normalised.push(format!(
                                "{}/{}: COM_FIELD_LIST default value {:02x?} dropped",
                                f.target,
                                f.name,
                                &p[e.len()..]
                            ));
                            let entry = stats.classes.entry("field_def".into()).or_default();
                            entry.0 += 1;
                        } else {
                            let what = match class {
                                Class::ParamDef => "param_def",
                                Class::FieldDef => "field_def",
                                _ => "column_def",
                            };
                            stats.check(f, what, &e, &p);
                        }
                        if class == Class::ColumnDef {
                            if !in_rs {
                                // Prepare response columns.
                                fetch_types.push((c.col_type, c.is_unsigned()));
                            }
                            cols.push(c);
                        }
                    }
                    Err(e) => stats.fail(f, format!("column definition: {e}")),
                }
            }
            Class::ColumnsEof | Class::ParamsEof | Class::Eof => {
                if caps.contains(Caps::DEPRECATE_EOF) && class == Class::Eof {
                    match Terminator::decode(&p.clone(), caps) {
                        Ok(t) => {
                            stats.check(f, "eof_or_ok_terminator", &t.encode(caps), &p);
                            out.ends.push(t);
                        }
                        Err(e) => stats.fail(f, format!("terminator: {e}")),
                    }
                } else {
                    match EofPacket::decode(&p, caps) {
                        Ok(e) => stats.check(f, "eof", &e.encode(caps), &p),
                        Err(e) => stats.fail(f, format!("EOF: {e}")),
                    }
                }
                if class == Class::ColumnsEof {
                    out.columns.push(cols.clone());
                }
            }
            Class::TextRow => match decode_text_row(&p, cols.len()) {
                Ok(vals) => {
                    stats.check(f, "text_row", &encode_text_row(vals.iter().copied()), &p);
                    out.text_rows
                        .push(vals.into_iter().map(|v| v.map(<[u8]>::to_vec)).collect());
                }
                Err(e) => stats.fail(f, format!("text row ({} cols): {e}", cols.len())),
            },
            Class::BinaryRow => {
                let r = if kind == Kind::Fetch {
                    let codes: Vec<u8> = fetch_types.iter().map(|t| t.0).collect();
                    decode_binary_row_types(&p, &fetch_types)
                        .map(|v| (encode_binary_row_types(&v, &codes), v))
                } else {
                    decode_binary_row(&p, &cols).map(|v| (encode_binary_row(&v, &cols), v))
                };
                match r {
                    Ok((Ok(e), v)) => {
                        stats.check(f, "binary_row", &e, &p);
                        out.binary_rows.push(v);
                    }
                    Ok((Err(e), _)) => stats.fail(f, format!("binary row re-encode: {e}")),
                    Err(e) => stats.fail(f, format!("binary row: {e}")),
                }
            }
            Class::End => {
                if !cols.is_empty() && out.columns.last() != Some(&cols) {
                    out.columns.push(cols.clone());
                }
                in_rs = false;
                match Terminator::decode(&p, caps) {
                    Ok(t) => {
                        stats.check(f, "terminator", &t.encode(caps), &p);
                        if let Terminator::Ok(ok) = &t {
                            check_session_state(f, stats, ok);
                        }
                        out.ends.push(t);
                    }
                    Err(e) => stats.fail(f, format!("terminator: {e}")),
                }
            }
            Class::PrepareOk => match PrepareOk::decode(&p) {
                Ok(ok) => {
                    stats.check(f, "prepare_ok", &ok.encode(), &p);
                    out.prepare_oks.push(ok);
                }
                Err(e) => stats.fail(f, format!("PREPARE_OK: {e}")),
            },
            Class::Raw => {}
        }
    }
    if !w.done() && kind != Kind::Connect {
        stats.fail(f, format!("response incomplete: {:?}", out.classes));
    }
    if in_rs && !cols.is_empty() && out.columns.last() != Some(&cols) {
        out.columns.push(cols);
    }
    out
}

fn check_session_state(f: &Fixture, stats: &mut Stats, ok: &OkPacket) {
    if ok.session_state.is_empty() {
        return;
    }
    let codec = ok.session_changes();
    let client = decode_session_state(&ok.session_state);
    match (&codec, &client) {
        (Ok(a), Ok(b)) => {
            if a != b {
                stats.fail(
                    f,
                    format!("codec and client read session state differently: {a:?} vs {b:?}"),
                );
            }
            stats.check(
                f,
                "session_state",
                &SessionChange::encode_all(b),
                &ok.session_state,
            );
        }
        _ => stats.fail(f, format!("session state: {codec:?} / {client:?}")),
    }
}

fn replay_client_packets(f: &Fixture, stats: &mut Stats) {
    let persona = persona_of(f);
    for (i, (_, p)) in f.client_packets().into_iter().enumerate() {
        if f.kind == Some(Kind::Connect) {
            if i == 0 {
                match HandshakeResponse::decode(&p) {
                    Ok(r) => stats.check(f, "handshake_response", &r.encode(), &p),
                    Err(e) => stats.fail(f, format!("handshake response: {e}")),
                }
            }
            continue;
        }
        if i > 0 && !f.name.contains("pipelined") {
            continue; // LOCAL INFILE data
        }
        match Command::parse(&p, f.caps, persona) {
            // Privileged commands (COM_REFRESH, COM_SHUTDOWN, ...) are
            // refused with 1227 and never forwarded, so their arguments
            // aren't kept: a documented normalisation.
            Ok(Command::Privileged(code)) => {
                stats.normalised.push(format!(
                    "{}/{}: privileged command {code:#04x}: arguments {:02x?} not kept",
                    f.target,
                    f.name,
                    &p[1..]
                ));
            }
            Ok(cmd) => match cmd.encode(f.caps) {
                Ok(e) => stats.check(f, "command", &e, &p),
                Err(e) => stats.fail(f, format!("command re-encode: {e}")),
            },
            Err(e) => stats.fail(f, format!("command: {e}")),
        }
    }
}

fn all() -> Vec<Fixture> {
    let v = fixture::load_all();
    assert!(!v.is_empty(), "no golden fixtures under tests/golden");
    v
}

#[test]
fn every_target_has_fixtures() {
    let fs = all();
    for t in ["mariadb-11.8", "mariadb-12.3", "mysql-8.4"] {
        let n = fs.iter().filter(|f| f.target == t).count();
        assert!(n >= 90, "{t}: only {n} fixtures");
    }
}

#[test]
fn every_packet_decodes_and_reencodes_byte_identically() {
    let mut stats = Stats::default();
    for f in &all() {
        replay(f, &mut stats);
        replay_client_packets(f, &mut stats);
    }
    for (k, (n, exact)) in &stats.classes {
        eprintln!("{k:>22}: {n:>5} decoded, {exact:>5} byte-identical");
    }
    for n in &stats.normalised {
        eprintln!("normalised: {n}");
    }
    assert!(
        stats.failures.is_empty(),
        "{} failures:\n{}",
        stats.failures.len(),
        stats.failures.join("\n")
    );
}

#[test]
fn field_list_defaults_are_the_only_normalisation() {
    let mut stats = Stats::default();
    for f in &all() {
        replay(f, &mut stats);
        replay_client_packets(f, &mut stats);
    }
    let mut field_list = 0;
    for n in &stats.normalised {
        if n.contains("COM_FIELD_LIST") {
            field_list += 1;
        } else {
            assert!(
                n.contains("privileged command 0x07"),
                "unexpected normalisation: {n}"
            );
        }
    }
    assert!(field_list > 0);
}

#[test]
fn compressed_wire_bytes_decode_to_the_recorded_packets() {
    let mut n = 0;
    // Connection fixtures record the framing negotiated for afterwards; the
    // connection phase itself is plain.
    for f in all()
        .iter()
        .filter(|f| f.framing != "plain" && f.kind != Some(Kind::Connect))
    {
        let framing = match f.framing.as_str() {
            "zlib" => Framing::Zlib { level: 6 },
            "zstd" => Framing::Zstd { level: 3 },
            other => panic!("framing {other}"),
        };
        let mut codec = PacketCodec::new(1 << 30);
        codec.set_framing(framing);
        codec.set_next_seq(1);
        // Client packets aren't in W; the server's compressed sequence
        // continues after the client's frame.
        let mut wire = BytesMut::from(&f.wire()[..]);
        let mut got = Vec::new();
        while let Some(p) = codec
            .decode(&mut wire)
            .unwrap_or_else(|e| panic!("{}: {e}", f.name))
        {
            got.push(p.payload);
        }
        let want: Vec<Bytes> = f.server_packets().into_iter().map(|p| p.1).collect();
        assert_eq!(got, want, "{}/{}", f.target, f.name);
        assert!(wire.is_empty());
        n += 1;
    }
    assert!(n >= 5, "only {n} compressed fixtures");
}

/// The first fixture of `target` whose name ends with `name`.
fn find<'a>(fs: &'a [Fixture], target: &str, name: &str) -> &'a Fixture {
    fs.iter()
        .find(|f| f.target == target && f.name.split_once('_').is_some_and(|(_, n)| n == name))
        .unwrap_or_else(|| panic!("no fixture {target}/{name}"))
}

fn rp(fs: &[Fixture], target: &str, name: &str) -> Replay {
    let mut s = Stats::default();
    let r = replay(find(fs, target, name), &mut s);
    assert!(s.failures.is_empty(), "{:?}", s.failures);
    r
}

fn text(v: &Option<Vec<u8>>) -> &str {
    std::str::from_utf8(v.as_deref().unwrap_or(b"NULL")).unwrap()
}

const MARIA: [&str; 2] = ["mariadb-11.8", "mariadb-12.3"];
const ALL: [&str; 3] = ["mariadb-11.8", "mariadb-12.3", "mysql-8.4"];

#[test]
fn greetings() {
    let fs = all();
    for t in ALL {
        let r = rp(&fs, t, "connect_default");
        let hs = r.greeting.expect("greeting");
        assert_eq!(hs.auth_data.len(), 20, "{t}: 20-byte nonce");
        assert!(
            hs.auth_data.iter().all(|&b| b != 0),
            "{t}: no NUL in the nonce"
        );
        assert!(
            hs.caps
                .contains(Caps::REQUIRED | Caps::SESSION_TRACK | Caps::DEPRECATE_EOF)
        );
        assert_eq!(hs.status, StatusFlags::AUTOCOMMIT);
        if t.starts_with("mariadb") {
            // Open question 1: MariaDB 11.8 and 12.3 send collation 45
            // (utf8mb4_general_ci) in the greeting, clear CLIENT_MYSQL and
            // put their extended capabilities in the reserved bytes.
            assert_eq!(hs.collation, 45, "{t}");
            assert!(!hs.caps.contains(Caps::CLIENT_MYSQL));
            assert!(hs.caps.contains(
                Caps::MARIADB_CLIENT_STMT_BULK_OPERATIONS
                    | Caps::MARIADB_CLIENT_EXTENDED_METADATA
                    | Caps::MARIADB_CLIENT_BULK_UNIT_RESULTS
            ));
            assert_eq!(hs.auth_plugin, "mysql_native_password");
            assert!(hs.server_version.contains("-MariaDB"));
        } else {
            assert_eq!(hs.collation, 255, "{t}: utf8mb4_0900_ai_ci");
            assert!(hs.caps.contains(
                Caps::CLIENT_MYSQL | Caps::ZSTD_COMPRESSION_ALGORITHM | Caps::QUERY_ATTRIBUTES
            ));
            assert_eq!(hs.caps.extended(), 0);
            assert_eq!(hs.auth_plugin, "caching_sha2_password");
        }
    }
}

#[test]
fn open_question_1_default_utf8mb4_collation() {
    let fs = all();
    for t in MARIA {
        let r = rp(&fs, t, "q1_collations");
        let row = &r.text_rows[0];
        assert_eq!(text(&row[0]), "utf8mb4");
        assert_eq!(
            text(&row[1]),
            "utf8mb4_uca1400_ai_ci",
            "{t}: @@collation_server"
        );
        // The handshake response asked for 45, but character_set_collations
        // maps utf8mb4 to its UCA 14.0.0 collation.
        assert_eq!(
            text(&row[2]),
            "utf8mb4_uca1400_ai_ci",
            "{t}: @@collation_connection"
        );
        assert!(text(&row[3]).contains("utf8mb4=utf8mb4_uca1400_ai_ci"));
        // information_schema.COLLATIONS marks no utf8mb4 collation default
        // (IS_DEFAULT is empty for both); SHOW CHARACTER SET reports the
        // character_set_collations mapping.
        let r = rp(&fs, t, "q1_default_utf8mb4_collation");
        assert!(r.text_rows.is_empty(), "{t}");
        let r = rp(&fs, t, "q1_show_character_set");
        assert_eq!(text(&r.text_rows[0][2]), "utf8mb4_uca1400_ai_ci", "{t}");
        let r = rp(&fs, t, "q1_collation_ids");
        // The default is 2304, which doesn't fit the greeting's one byte:
        // the greeting carries 45 instead.
        let ids: Vec<(&str, &str, &str)> = r
            .text_rows
            .iter()
            .map(|row| (text(&row[0]), text(&row[1]), text(&row[2])))
            .collect();
        assert_eq!(
            ids,
            [
                ("utf8mb4_general_ci", "45", ""),
                ("utf8mb4_uca1400_ai_ci", "2304", "Yes")
            ],
            "{t}"
        );
    }
    let r = rp(&fs, "mysql-8.4", "q1_collations");
    assert_eq!(text(&r.text_rows[0][1]), "utf8mb4_0900_ai_ci");
    assert_eq!(text(&r.text_rows[0][3]), "utf8mb4_0900_ai_ci");
}

#[test]
fn auth_exchanges() {
    let fs = all();
    for t in MARIA {
        // Answering with caching_sha2_password: MariaDB switches to the
        // account's mysql_native_password with a fresh 20-byte nonce + NUL.
        let r = rp(&fs, t, "connect_auth_switch");
        assert_eq!(r.auth_switch[0].plugin, "mysql_native_password");
        assert_eq!(r.auth_switch[0].data.len(), 21);
        assert_eq!(r.auth_switch[0].data.last(), Some(&0));
        assert_eq!(r.oks.len(), 1);
        let r = rp(&fs, t, "connect_wrong_password");
        assert_eq!(r.errs[0].code, 1045);
        assert_eq!(&r.errs[0].sqlstate, b"28000");
    }
    let t = "mysql-8.4";
    let r = rp(&fs, t, "connect_default");
    assert_eq!(&r.auth_more[0][..], &[0x03], "fast path");
    let r = rp(&fs, t, "connect_auth_switch");
    assert_eq!(r.auth_switch[0].plugin, "caching_sha2_password");
    assert_eq!(r.auth_switch[0].data.len(), 21);
    assert_eq!(&r.auth_more[0][..], &[0x03]);
    let r = rp(&fs, t, "connect_sha2_full_path_rsa_key");
    assert_eq!(&r.auth_more[0][..], &[0x04], "perform full authentication");
    assert!(r.auth_more[1].starts_with(b"-----BEGIN PUBLIC KEY-----"));
    let r = rp(&fs, t, "connect_wrong_password");
    assert_eq!(&r.auth_more[0][..], &[0x04]);
    assert_eq!(r.errs[0].code, 1045);
    for t in ALL {
        let r = rp(&fs, t, "connect_unknown_database");
        assert_eq!(r.errs[0].code, 1049, "{t}");
        assert_eq!(&r.errs[0].sqlstate, b"42000");
    }
}

fn changes(r: &Replay) -> Vec<SessionChange> {
    r.oks
        .iter()
        .chain(r.ends.iter().filter_map(|t| match t {
            Terminator::Ok(o) => Some(o),
            Terminator::Eof(_) => None,
        }))
        .flat_map(|o| o.session_changes().unwrap())
        .collect()
}

#[test]
fn session_tracking() {
    let fs = all();
    for t in ALL {
        let r = rp(&fs, t, "ok_use");
        let c = changes(&r);
        assert!(
            c.contains(&SessionChange::Schema(Bytes::from_static(b"sprig_golden"))),
            "{t}: {c:?}"
        );
        assert!(c.contains(&SessionChange::StateChange(true)), "{t}: {c:?}");
        let r = rp(&fs, t, "ok_set_variable");
        assert!(changes(&r).iter().any(|c| matches!(c,
            SessionChange::SystemVariable { name, value } if &name[..] == b"time_zone" && &value[..] == b"+05:30")), "{t}");
        let r = rp(&fs, t, "ok_begin");
        let c = changes(&r);
        assert!(r.oks[0].status.contains(StatusFlags::IN_TRANS));
        assert!(c.iter().any(|c| matches!(c, SessionChange::TransactionCharacteristics(s) if &s[..] == b"START TRANSACTION;")), "{t}: {c:?}");
        let r = rp(&fs, t, "ok_start_read_only");
        assert!(
            r.oks[0]
                .status
                .contains(StatusFlags::IN_TRANS | StatusFlags::IN_TRANS_READONLY)
        );
        let c = changes(&r);
        assert!(c.iter().any(|c| matches!(c, SessionChange::TransactionCharacteristics(s)
            if std::str::from_utf8(s).unwrap().contains("SERIALIZABLE") && std::str::from_utf8(s).unwrap().contains("READ ONLY"))), "{t}: {c:?}");
        let r = rp(&fs, t, "ok_insert_last_id");
        assert_eq!((r.oks[0].affected_rows, r.oks[0].last_insert_id), (2, 1));
        assert_eq!(
            &r.oks[0].info[..],
            b"Records: 2  Duplicates: 0  Warnings: 0"
        );
        let r = rp(&fs, t, "ok_update_info");
        assert_eq!(
            &r.oks[0].info[..],
            b"Rows matched: 1  Changed: 1  Warnings: 0"
        );
        let r = rp(&fs, t, "ok_warning");
        assert_eq!(r.oks[0].warnings, 1);
        let r = rp(&fs, t, "ok_temporary_table");
        assert!(
            changes(&r).contains(&SessionChange::StateChange(true)),
            "{t}"
        );
        let r = rp(&fs, t, "ok_sql_mode_ansi_quotes");
        assert!(
            r.oks[0].status.contains(StatusFlags::NO_BACKSLASH_ESCAPES),
            "{t}"
        );
        if t.starts_with("mariadb") {
            // MariaDB reports ANSI_QUOTES in status bit 15.
            assert!(r.oks[0].status.contains(StatusFlags::ANSI_QUOTES), "{t}");
        }
        let r = rp(&fs, t, "ok_commit");
        let c = changes(&r);
        if t == "mysql-8.4" {
            // session_track_gtids = OWN_GTID: entry type 3, encoding 0.
            assert!(c.iter().any(|c| matches!(c, SessionChange::Gtids { encoding: 0, gtids } if gtids.contains(&b':'))), "{c:?}");
        } else {
            // MariaDB: last_gtid as a system variable (binlog on on 11.8).
            let has_last_gtid = c.iter().any(|c| matches!(c, SessionChange::SystemVariable { name, .. } if &name[..] == b"last_gtid"));
            assert!(has_last_gtid || t == "mariadb-12.3", "{t}: {c:?}");
        }
    }
}

#[test]
fn resultsets_of_every_column_type() {
    let fs = all();
    for t in ALL {
        let r = rp(&fs, t, "rs_text_all_types");
        let cols = &r.columns[0];
        assert_eq!(r.text_rows.len(), 2);
        let by_name = |n: &str| {
            cols.iter()
                .position(|c| &c.name[..] == n.as_bytes())
                .unwrap()
        };
        let row = &r.text_rows[0];
        assert_eq!(text(&row[by_name("c_ubigint")]), "18446744073709551615");
        assert_eq!(text(&row[by_name("c_uint")]), "0000000042", "ZEROFILL");
        assert_eq!(
            text(&row[by_name("c_datetime")]),
            "2024-12-31 23:59:59.123456"
        );
        assert_eq!(text(&row[by_name("c_time")]), "838:59:58.999");
        assert_eq!(text(&row[by_name("c_varchar")]), "héllo wörld");
        assert!(
            r.text_rows[1][1..].iter().all(Option::is_none),
            "row 2 is NULLs"
        );
        let c = &cols[by_name("c_uint")];
        assert!(c.is_unsigned());
        assert_eq!(c.col_type, ct::LONG);
        assert_eq!(
            cols[by_name("c_json")].col_type,
            if t == "mysql-8.4" { ct::JSON } else { ct::BLOB }
        );
        assert_eq!(cols[by_name("c_geometry")].col_type, ct::GEOMETRY);
        assert_eq!(cols[by_name("c_bit")].col_type, ct::BIT);
        assert_eq!(cols[by_name("c_decimal")].col_type, ct::NEWDECIMAL);
        assert_eq!(cols[by_name("c_enum")].col_type, ct::STRING);
        assert_eq!(&cols[0].schema[..], b"sprig_golden");
        assert_eq!(&cols[0].org_table[..], b"all_types");

        // Binary protocol: the same values, typed.
        let r = rp(&fs, t, "execute_all_types");
        let cols = &r.columns[0];
        let by_name = |n: &str| {
            cols.iter()
                .position(|c| &c.name[..] == n.as_bytes())
                .unwrap()
        };
        let row = &r.binary_rows[0];
        assert_eq!(row[by_name("c_tinyint")], Value::Int(-128));
        assert_eq!(row[by_name("c_utinyint")], Value::UInt(255));
        assert_eq!(row[by_name("c_bigint")], Value::Int(i64::MIN));
        assert_eq!(row[by_name("c_ubigint")], Value::UInt(u64::MAX));
        assert_eq!(row[by_name("c_float")], Value::Float(3.5));
        assert_eq!(row[by_name("c_double")], Value::Double(-2.25e-300));
        assert_eq!(row[by_name("c_year")], Value::UInt(2155));
        let Value::DateTime(d) = &row[by_name("c_datetime")] else {
            panic!()
        };
        assert_eq!(
            (d.year, d.month, d.day, d.hour, d.micro),
            (2024, 12, 31, 23, 123_456)
        );
        let Value::DateTime(d) = &row[by_name("c_date")] else {
            panic!()
        };
        assert_eq!((d.year, d.month, d.day), (2024, 2, 29));
        let Value::Time(tm) = &row[by_name("c_time")] else {
            panic!()
        };
        assert_eq!(
            (
                tm.negative,
                tm.days,
                tm.hour,
                tm.minute,
                tm.second,
                tm.micro
            ),
            (false, 34, 22, 59, 58, 999_000)
        );
        let Value::Time(tm) = &row[by_name("c_neg_time")] else {
            panic!()
        };
        assert!(tm.negative);
        assert_eq!((tm.hour, tm.minute, tm.second), (12, 34, 56));
        assert_eq!(
            row[by_name("c_decimal")],
            Value::Bytes(Bytes::from_static(b"-12345678.1234"))
        );
        assert!(r.binary_rows[1][1..].iter().all(Value::is_null));
    }
    // MariaDB extended metadata.
    for t in MARIA {
        let r = rp(&fs, t, "rs_text_all_types");
        let cols = &r.columns[0];
        let ext = |n: &str| {
            let c = cols.iter().find(|c| &c.name[..] == n.as_bytes()).unwrap();
            (
                c.ext_type_name()
                    .map(|b| String::from_utf8_lossy(b).into_owned()),
                c.ext_format_name()
                    .map(|b| String::from_utf8_lossy(b).into_owned()),
            )
        };
        assert_eq!(ext("c_json"), (None, Some("json".into())), "{t}");
        assert_eq!(ext("c_uuid"), (Some("uuid".into()), None), "{t}");
        assert_eq!(ext("c_inet4"), (Some("inet4".into()), None), "{t}");
        assert_eq!(ext("c_inet6"), (Some("inet6".into()), None), "{t}");
        assert_eq!(ext("c_point").0.as_deref(), Some("point"), "{t}");
        assert_eq!(ext("c_int"), (None, None), "{t}");
        // VECTOR(2) travels as a binary VAR_STRING without a type name.
        let vector = cols.iter().find(|c| &c.name[..] == b"c_vector").unwrap();
        assert_eq!(
            (vector.col_type, vector.charset, vector.ext_type.as_ref()),
            (ct::VAR_STRING, 63, None),
            "{t}"
        );
        // Without the capability the same columns carry no block.
        let r = rp(&fs, t, "eof_rs_text_all_types");
        assert!(r.columns[0].iter().all(|c| c.ext_type.is_none()));
    }
}

#[test]
fn multiple_results_and_call() {
    let fs = all();
    for t in ALL {
        let r = rp(&fs, t, "multi_results");
        assert_eq!(r.columns.len(), 2, "{t}");
        assert!(
            r.ends[0]
                .status()
                .contains(StatusFlags::MORE_RESULTS_EXISTS)
        );
        assert!(
            r.ends[1]
                .status()
                .contains(StatusFlags::MORE_RESULTS_EXISTS)
        );
        assert!(
            !r.oks
                .last()
                .unwrap()
                .status
                .contains(StatusFlags::MORE_RESULTS_EXISTS)
        );
        let r = rp(&fs, t, "multi_results_error");
        assert_eq!(r.errs[0].code, 1146);
        let r = rp(&fs, t, "call_two_resultsets");
        assert_eq!(r.columns.len(), 2);
        // The CALL's own OK ends the response.
        assert!(
            !r.oks
                .last()
                .unwrap()
                .status
                .contains(StatusFlags::MORE_RESULTS_EXISTS)
        );
        let r = rp(&fs, t, "execute_call_two_resultsets");
        assert_eq!(r.binary_rows.len(), 2, "{t}");
        let r = rp(&fs, t, "execute_call_out_params");
        assert!(
            r.ends
                .iter()
                .any(|e| e.status().contains(StatusFlags::PS_OUT_PARAMS)),
            "{t}"
        );
        assert!(
            r.binary_rows
                .iter()
                .any(|row| row.contains(&Value::Int(42))),
            "{t}: {:?}",
            r.binary_rows
        );
        let r = rp(&fs, t, "eof_multi_results");
        assert!(r.ends.iter().all(|e| matches!(e, Terminator::Eof(_))));
    }
}

#[test]
fn errors_and_local_infile() {
    let fs = all();
    for t in ALL {
        for (name, code, state) in [
            ("err_no_such_table", 1146, *b"42S02"),
            ("err_syntax", 1064, *b"42000"),
            ("err_duplicate_key", 1062, *b"23000"),
            ("err_execute_unknown_stmt", 1243, *b"HY000"),
            ("err_com_init_db_unknown", 1049, *b"42000"),
            ("err_multi_statements_off", 1064, *b"42000"),
        ] {
            let r = rp(&fs, t, name);
            assert_eq!(
                (r.errs[0].code, r.errs[0].sqlstate),
                (code, state),
                "{t}/{name}"
            );
        }
    }
    // COM_PROCESS_KILL: MariaDB answers it; MySQL 8.4 no longer knows it.
    for t in MARIA {
        let r = rp(&fs, t, "err_process_kill_unknown");
        assert_eq!(
            (r.errs[0].code, r.errs[0].sqlstate),
            (1094, *b"HY000"),
            "{t}"
        );
    }
    let r = rp(&fs, "mysql-8.4", "err_process_kill_unknown");
    assert_eq!((r.errs[0].code, r.errs[0].sqlstate), (1047, *b"08S01"));
    for t in MARIA {
        let r = rp(&fs, t, "local_infile");
        assert_eq!(&r.local_infile[0][..], b"golden.csv");
        assert_eq!(r.oks[0].affected_rows, 3);
        let r = rp(&fs, t, "local_infile_declined");
        assert_eq!(&r.local_infile[0][..], b"declined.csv");
    }
    // Open question 5, client side: without CLIENT_LOCAL_FILES.
    for t in ALL {
        let r = rp(&fs, t, "err_local_infile_without_client_capability");
        let want = if t == "mysql-8.4" {
            (3948, *b"42000")
        } else {
            (4166, *b"HY000")
        };
        assert_eq!(
            (r.errs[0].code, r.errs[0].sqlstate),
            want,
            "{t}: {:?}",
            r.errs[0]
        );
    }
    // Open question 5, server side: local_infile = 0.
    let r = rp(&fs, "mariadb-12.3", "q5_err_local_infile_disabled");
    assert_eq!(r.errs[0].code, 4166, "{:?}", r.errs[0]);
    let r = rp(&fs, "mysql-8.4", "q5_err_local_infile_disabled");
    assert_eq!(r.errs[0].code, 3948, "{:?}", r.errs[0]);
    // Open question 8: require_secure_transport refuses a plaintext login.
    // MariaDB uses MySQL's number and wording, with SQLSTATE 08004 where
    // MySQL 8.4 sends HY000.
    let r = rp(&fs, "mariadb-12.3", "q8_err_require_secure_transport");
    assert_eq!(
        (r.errs[0].code, r.errs[0].sqlstate),
        (3159, *b"08004"),
        "{:?}",
        r.errs[0]
    );
    let r = rp(&fs, "mysql-8.4", "q8_err_require_secure_transport");
    assert_eq!(
        (r.errs[0].code, r.errs[0].sqlstate),
        (3159, *b"HY000"),
        "{:?}",
        r.errs[0]
    );
}

#[test]
fn open_question_3_mariadb_direct_execute_and_bulk() {
    let fs = all();
    for t in MARIA {
        let r = rp(&fs, t, "q3_execute_last_prepared");
        assert_eq!(
            r.binary_rows[0][0],
            Value::Int(42),
            "{t}: id 0xFFFFFFFF ran the last prepared statement"
        );
        let r = rp(&fs, t, "q3_direct_execute_pipelined");
        assert_eq!(r.prepare_oks.len(), 1);
        assert_eq!(r.binary_rows.last().unwrap()[0], Value::Int(42), "{t}");
        let r = rp(&fs, t, "q3_bulk_execute");
        assert_eq!(r.oks[0].affected_rows, 3, "{t}");
        let r = rp(&fs, t, "q3_bulk_execute_unit_results");
        // With BULK_SEND_UNIT_RESULTS: one row per parameter set.
        assert_eq!(r.columns.len(), 1, "{t}: {:?}", r.classes);
        assert_eq!(r.binary_rows.len() + r.text_rows.len(), 2, "{t}");
        let r = rp(&fs, t, "q3_bulk_execute_error_midway");
        assert_eq!(r.errs[0].code, 1062);
    }
}

#[test]
fn open_question_4_com_create_db_and_drop_db() {
    let fs = all();
    let r = rp(&fs, "mysql-8.4", "q4_com_create_db");
    assert_eq!(r.errs[0].code, 1047, "{:?}", r.errs);
    let r = rp(&fs, "mysql-8.4", "q4_com_drop_db");
    assert_eq!(r.errs[0].code, 1047, "{:?}", r.errs);
}

#[test]
fn cursors_and_other_commands() {
    let fs = all();
    for t in ALL {
        let r = rp(&fs, t, "fetch_cursor_one_row");
        assert_eq!(r.binary_rows.len(), 1);
        let r = rp(&fs, t, "fetch_cursor_rest");
        assert!(
            r.ends[0].status().contains(StatusFlags::LAST_ROW_SENT),
            "{t}"
        );
        let r = rp(&fs, t, "eof_execute_cursor");
        assert!(r.classes.contains(&Class::ColumnsEof));
        let r = rp(&fs, t, "prepare_params");
        assert_eq!(r.prepare_oks[0].params, 7);
        let r = rp(&fs, t, "execute_params");
        let row = &r.binary_rows[0];
        assert_eq!(row[0], Value::Int(-7));
        assert_eq!(row[1], Value::Double(2.5));
        assert_eq!(row[2], Value::Bytes(Bytes::from_static(b"str")));
        assert!(row[5].is_null());
        let r = rp(&fs, t, "ok_ping");
        assert_eq!(r.oks.len(), 1);
    }
}

// ---------------------------------------------------------------------------
// The client state machine over the recorded streams
// ---------------------------------------------------------------------------

struct Script {
    data: Vec<u8>,
    pos: usize,
}

impl AsyncRead for Script {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let n = (self.data.len() - self.pos).min(buf.remaining()).min(997);
        let start = self.pos;
        buf.put_slice(&self.data[start..start + n]);
        self.pos += n;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for Script {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn frame(out: &mut Vec<u8>, seq: u8, p: &[u8]) {
    let n = u32::try_from(p.len()).unwrap().to_le_bytes();
    out.extend_from_slice(&[n[0], n[1], n[2], seq]);
    out.extend_from_slice(p);
}

/// Each main-connection command fixture, after its target's login, through
/// `MysqlClient::next_packet`: the client must see as many packets as the
/// walker, end the response at the same place, and agree on the kinds.
#[test]
fn client_reads_every_recorded_response() {
    let fs = all();
    let mut n = 0;
    for t in ALL {
        let login = find(&fs, t, "connect_main");
        let login_packets = login.server_packets();
        // The client asks for what the capture tool asked for on the main
        // connection.
        for f in fs.iter().filter(|f| {
            f.target == t
                && f.caps == login.caps
                && f.framing == "plain"
                && !matches!(f.kind, Some(Kind::Connect | Kind::None))
                && !f.name.contains("pipelined")
        }) {
            let mut stream = Vec::new();
            for (seq, p) in &login_packets {
                frame(&mut stream, *seq, p);
            }
            let server = f.server_packets();
            for (seq, p) in &server {
                frame(&mut stream, *seq, p);
            }
            let mut cfg = ClientConfig::new("root", "pw");
            cfg.tls = TlsMode::Disabled;
            cfg.local_files = true;
            cfg.extra_caps = Caps::MARIADB_CLIENT_STMT_BULK_OPERATIONS
                | Caps::MARIADB_CLIENT_EXTENDED_METADATA
                | Caps::MARIADB_CLIENT_BULK_UNIT_RESULTS;
            let client = f.client_packets();
            let cmd = client[0].1.clone();
            let infile: Vec<Bytes> = client[1..]
                .iter()
                .map(|c| c.1.clone())
                .filter(|p| !p.is_empty())
                .collect();
            let got: Vec<PacketKind> = futures::executor::block_on(async {
                let io = Script {
                    data: stream,
                    pos: 0,
                };
                let mut c = MysqlClient::handshake(ClientStream::Other(Box::new(io)), "h", &cfg)
                    .await
                    .unwrap_or_else(|e| panic!("{t}: login replay: {e}"));
                assert_eq!(
                    c.caps(),
                    login.caps,
                    "{t}: client caps differ from the capture's"
                );
                c.send_command_payload(cmd).await.unwrap();
                let mut kinds = Vec::new();
                loop {
                    match c.next_packet().await {
                        Ok(Some(p)) => {
                            if p.kind == PacketKind::LocalInfile {
                                for d in &infile {
                                    c.send_infile_data(d).await.unwrap();
                                }
                                c.finish_infile().await.unwrap();
                            }
                            kinds.push(p.kind);
                        }
                        Ok(None) => break,
                        Err(e) => panic!("{}/{}: client: {e} after {kinds:?}", f.target, f.name),
                    }
                }
                assert!(c.is_idle(), "{}/{}", f.target, f.name);
                kinds
            });
            assert_eq!(got.len(), server.len(), "{}/{}: {got:?}", f.target, f.name);
            let mut w = Walker::new(f.kind.unwrap(), f.caps);
            for (k, (_, p)) in got.iter().zip(&server) {
                let class = w.feed(p);
                let agree = matches!(
                    (class, k),
                    (Class::Ok, PacketKind::Ok(_))
                        | (Class::Err, PacketKind::Err(_))
                        | (Class::LocalInfile, PacketKind::LocalInfile)
                        | (Class::ColumnCount(_), PacketKind::ColumnCount(_))
                        | (Class::ColumnDef | Class::FieldDef, PacketKind::ColumnDef)
                        | (Class::ColumnsEof, PacketKind::ColumnsEof)
                        | (Class::TextRow | Class::BinaryRow, PacketKind::Row)
                        | (Class::End | Class::Eof, PacketKind::End(_))
                        | (Class::PrepareOk, PacketKind::PrepareOk(_))
                        | (Class::ParamDef, PacketKind::ParamDef)
                        | (Class::ParamsEof, PacketKind::ParamsEof)
                        | (Class::Raw, PacketKind::Raw)
                );
                assert!(
                    agree,
                    "{}/{}: walker {class:?}, client {k:?}",
                    f.target, f.name
                );
            }
            n += 1;
        }
    }
    assert!(n >= 150, "only {n} responses replayed through the client");
}
