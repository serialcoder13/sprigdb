//! The cargo-fuzz targets' corpora and crash regressions, replayed on
//! stable in the ordinary test run (chapter 46, step 4).
//!
//! The target bodies live in `fuzz/src/lib.rs` (the cargo-fuzz project,
//! outside the workspace); this file includes it by path, so every
//! committed corpus input and every crash regression runs here without
//! nightly or libFuzzer.
//!
//! `write_fuzz_seeds` regenerates `fuzz/corpus/<target>/golden-*` from the
//! golden fixtures when `SPRIG_WRITE_FUZZ_SEEDS=1`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    missing_docs,
    unreachable_pub,
    dead_code
)]

#[path = "../fuzz/src/lib.rs"]
mod targets;

#[path = "golden/fixture.rs"]
mod fixture;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use fixture::{Class, Fixture, Kind, Walker};
use sprig_proto_mysql::packets::Caps;
use sprig_proto_mysql::resultset::ColumnDef;

fn fuzz_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("fuzz")
}

#[test]
fn fuzz_corpora_and_regressions_replay_cleanly() {
    let n = targets::replay_dirs(&fuzz_dir(), &["corpus", "regressions"]);
    assert!(n >= 100, "only {n} corpus inputs found under fuzz/");
}

#[test]
fn every_regression_has_a_target() {
    let names: BTreeSet<&str> = targets::TARGETS.iter().map(|t| t.0).collect();
    for dir in ["corpus", "regressions"] {
        let Ok(entries) = std::fs::read_dir(fuzz_dir().join(dir)) else {
            continue;
        };
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            assert!(
                names.contains(name.as_str()),
                "fuzz/{dir}/{name}: no such target"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Seeds from the golden captures
// ---------------------------------------------------------------------------

fn frame(out: &mut Vec<u8>, seq: u8, p: &[u8]) {
    let n = u32::try_from(p.len()).unwrap().to_le_bytes();
    out.extend_from_slice(&[n[0], n[1], n[2], seq]);
    out.extend_from_slice(p);
}

/// The `caps_set` index closest to what a fixture negotiated.
fn caps_index(f: &Fixture) -> u8 {
    let maria = f.server.contains("MariaDB");
    match (maria, f.caps.contains(Caps::DEPRECATE_EOF)) {
        (true, true) => 3,
        (true, false) => 4,
        (false, _) => 2,
    }
}

/// The `BINARY_TYPES` index of a column type, if listed.
fn type_index(t: u8) -> Option<u8> {
    targets::BINARY_TYPES
        .iter()
        .position(|&x| x == t)
        .and_then(|i| u8::try_from(i).ok())
}

/// The `client` target's command selector for a fixture's command.
fn client_selector(code: u8) -> Option<u8> {
    Some(match code {
        0x03 => 0,
        0x16 => 1,
        0x04 => 2,
        0x09 => 3,
        0x0E => 4,
        0x17 => 5,
        0x1B => 6,
        0x1C => 7,
        0x0A => 8,
        0xFA => 9,
        _ => return None,
    })
}

const MAX_SEED: usize = 64 * 1024;

#[test]
fn write_fuzz_seeds() {
    if std::env::var("SPRIG_WRITE_FUZZ_SEEDS").as_deref() != Ok("1") {
        eprintln!("write_fuzz_seeds: SPRIG_WRITE_FUZZ_SEEDS is not 1; skipping");
        return;
    }
    let fixtures = fixture::load_all();
    let mut seeds: Vec<(&str, Vec<u8>)> = Vec::new();
    let logins: Vec<&Fixture> = fixtures
        .iter()
        .filter(|f| f.name.ends_with("connect_main"))
        .collect();
    for f in &fixtures {
        let ci = caps_index(f);
        let ext = u8::from(f.caps.contains(Caps::MARIADB_CLIENT_EXTENDED_METADATA));
        let server = f.server_packets();
        let kind = f.kind.unwrap();

        // codec: the server's packets as plain wire bytes, or the raw
        // compressed bytes.
        if let Some(&(seq, _)) = server.first() {
            let mut d = vec![0xFC, seq];
            for (s, p) in &server {
                frame(&mut d, *s, p);
            }
            seeds.push(("codec", d));
        }
        if f.framing != "plain" && kind != Kind::Connect {
            let cfg = if f.framing == "zlib" { 0xFD } else { 0xFE };
            let mut d = vec![cfg, 1];
            d.extend_from_slice(&f.wire());
            seeds.push(("codec", d));
        }

        // Classified server packets.
        let mut w = Walker::new(kind, f.caps);
        let mut cols: Vec<ColumnDef> = Vec::new();
        for (_, p) in &server {
            if w.done() {
                w = Walker::new(Kind::Execute, f.caps);
            }
            let class = w.feed(p);
            match class {
                Class::Greeting => seeds.push(("handshake_v10", p.to_vec())),
                Class::Err | Class::AuthSwitch | Class::AuthMoreData => {
                    seeds.push(("err_packet", p.to_vec()));
                }
                Class::Ok | Class::End | Class::Eof | Class::ColumnsEof | Class::ParamsEof => {
                    let mut d = vec![ci];
                    d.extend_from_slice(p);
                    seeds.push(("ok_packet", d));
                }
                Class::ColumnDef
                | Class::ParamDef
                | Class::FieldDef
                | Class::PrepareOk
                | Class::ColumnCount(_)
                | Class::LocalInfile => {
                    let mut d = vec![(ci << 1) | ext];
                    d.extend_from_slice(p);
                    seeds.push(("column_def", d));
                    if matches!(class, Class::ColumnCount(_)) {
                        cols.clear();
                    }
                    if class == Class::ColumnDef
                        && let Ok(c) = ColumnDef::decode(p, f.caps)
                    {
                        cols.push(c);
                    }
                }
                Class::TextRow => {
                    if let Ok(n) = u8::try_from(cols.len())
                        && n < 64
                    {
                        let mut d = vec![n];
                        d.extend_from_slice(p);
                        seeds.push(("rows", d));
                    }
                }
                Class::BinaryRow => {
                    let spec: Option<Vec<u8>> = cols
                        .iter()
                        .map(|c| {
                            type_index(c.col_type)
                                .map(|i| i | if c.is_unsigned() { 0x80 } else { 0 })
                        })
                        .collect();
                    if let (Some(spec), Ok(n)) = (spec, u8::try_from(cols.len()))
                        && n < 64
                    {
                        let mut d = vec![n | 0x40];
                        d.extend_from_slice(&spec);
                        d.extend_from_slice(p);
                        seeds.push(("rows", d));
                    }
                }
                Class::Raw => {}
            }
        }

        // The client's packets.
        let client = f.client_packets();
        if kind == Kind::Connect {
            if let Some((_, resp)) = client.first() {
                seeds.push(("handshake_response", resp.to_vec()));
                // server_auth: handshake response, then what followed.
                let persona_bit = if f.server.contains("MariaDB") {
                    0x40
                } else {
                    0
                };
                for cfg in [persona_bit, 0x1D | persona_bit, 0x25 | persona_bit] {
                    let mut d = vec![cfg, 0];
                    let packets: Vec<&[u8]> = client.iter().map(|c| &c.1[..]).collect();
                    d.extend_from_slice(&targets::join_packets(packets));
                    seeds.push(("server_auth", d));
                }
            }
        } else if let Some((_, cmd)) = client.first() {
            let persona = if f.server.contains("MariaDB") {
                0x08
            } else {
                0
            };
            let mut d = vec![
                ci | persona | 0x10,
                0x88,
                0x81,
                0x95,
                0x8F,
                0x12,
                0x01,
                0x06,
                0x0A,
            ];
            d.extend_from_slice(cmd);
            seeds.push(("command", d));
            let mut d = vec![ci | persona, 0, 0, 0, 0, 0, 0, 0, 0];
            d.extend_from_slice(cmd);
            seeds.push(("command", d));
        }

        // client: a login, then this response, for the command the target
        // sends with the same selector.
        if f.framing == "plain"
            && !matches!(kind, Kind::Connect | Kind::None)
            && let Some(login) = logins
                .iter()
                .find(|l| l.target == f.target && l.caps == f.caps)
            && let Some(sel) = client.first().and_then(|c| client_selector(c.1[0]))
        {
            let cfg = 0x02 | if ext == 1 { 0x04 } else { 0 };
            let mut d = vec![cfg, sel];
            for (s, p) in login.server_packets() {
                frame(&mut d, s, &p);
            }
            for (s, p) in &server {
                frame(&mut d, *s, p);
            }
            seeds.push(("client", d));
        }
    }

    // Write, deduplicated, named by content hash.
    let root = fuzz_dir().join("corpus");
    let mut written = 0;
    let mut seen = BTreeSet::new();
    for (target, data) in seeds {
        if data.len() > MAX_SEED || !seen.insert((target, data.clone())) {
            continue;
        }
        let dir = root.join(target);
        std::fs::create_dir_all(&dir).unwrap();
        let name = format!("golden-{:016x}", fnv1a(&data));
        std::fs::write(dir.join(name), &data).unwrap();
        written += 1;
    }
    eprintln!("wrote {written} seeds under {}", root.display());
}

fn fnv1a(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in data {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}
