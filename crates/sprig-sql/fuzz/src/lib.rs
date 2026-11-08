//! Fuzz harnesses for `sprig-sql` (chapter 46 step 4): the persona-aware
//! lexer, chapter 07's name rewriter and plan, and chapter 28's R0
//! rewrites.
//!
//! Each `fuzz_*` function is one cargo-fuzz target's body. They live here,
//! not in `fuzz_targets/`, so the same code also runs on stable as a corpus
//! replay: `cargo test` in this crate, and the workspace test
//! `crates/sprig-sql/tests/fuzz_replay.rs`, which includes this file by
//! path.
//!
//! The oracle is chapter 46's (no panic, no hang, bounded memory) plus the
//! invariants each layer promises:
//!
//! - **lexer**: tokens cover every byte, in order, with no gaps; every
//!   neutralization offset is an executable comment's `!`; an expanded
//!   text lexes again.
//! - **rewriter**: a forwarded statement lexes, unrewrites, and its result
//!   mapper runs over a resultset whose values are the statement's own
//!   bytes.
//! - **compat**: every R0 rewrite returns, and rewritten text lexes.
//!
//! The first input byte picks the settings (persona, `sql_mode` flags),
//! so every target accepts any byte string.

#![allow(clippy::missing_panics_doc, missing_docs)]

use bytes::Bytes;
use sprig_common::ids::{DbId, GroupId, ShardId, TenantId};
use sprig_proto_mysql::ColumnDef;
use sprig_proto_mysql::packets::Persona;
use sprig_sql::compat::{collation, errmap, meta, schema, vars};
use sprig_sql::lex::{self, LexOptions, TokenKind, ident_value, string_value, versioned};
use sprig_sql::names::map::SchemaName;
use sprig_sql::names::{
    Action, ColumnRegistry, Engine, LogicalDb, MemoryNameMap, OutputCtx, PlanCtx, RewriteCtx,
    TextRow, plan, unrewrite,
};

/// The persona and `sql_mode` flags a config byte picks.
#[must_use]
pub fn lex_opts(cfg: u8) -> LexOptions {
    let persona = if cfg & 1 == 0 {
        Persona::MySql84
    } else {
        Persona::MariaDb
    };
    LexOptions::for_persona(persona)
        .ansi_quotes(cfg & 2 != 0)
        .no_backslash_escapes(cfg & 4 != 0)
}

/// Tenant 42 with awkward names, a sharded database with a reference
/// schema, and tenant 43's names to watch for.
fn tenant() -> MemoryNameMap {
    let mut m = MemoryNameMap::new(TenantId(42))
        .with_db(1, "billing", 1, 1)
        .with_db(2, "crm", 2, 2)
        .with_db(3, "a`b", 3, 3)
        .with_db(4, "o'k", 4, 4);
    let t = TenantId(42);
    m.insert(LogicalDb {
        id: DbId(5),
        name: "sharded".into(),
        shards: vec![
            SchemaName::shard(t, DbId(5), GroupId(5), ShardId(6)),
            SchemaName::shard(t, DbId(5), GroupId(5), ShardId(7)),
        ],
        reference: Some(SchemaName::reference(t, DbId(5))),
    });
    m
}

fn split(data: &[u8]) -> Option<(u8, &[u8])> {
    data.split_first().map(|(c, rest)| (*c, rest))
}

/// `lexer`: [`lex::lex`] and the executable-comment functions.
pub fn fuzz_lexer(data: &[u8]) {
    let Some((cfg, sql)) = split(data) else {
        return;
    };
    let opts = lex_opts(cfg);
    let Ok(tokens) = lex::lex(sql, &opts) else {
        return;
    };
    let mut at = 0u32;
    for t in &tokens {
        assert_eq!(t.start, at, "gap or overlap before {t:?}");
        assert!(t.end > t.start, "empty token {t:?}");
        at = t.end;
        let b = t.bytes(sql);
        match t.kind {
            TokenKind::String => {
                let _ = string_value(b, opts.no_backslash_escapes);
            }
            TokenKind::Word | TokenKind::QuotedIdent => {
                let _ = ident_value(b);
            }
            _ => {}
        }
    }
    assert_eq!(at as usize, sql.len(), "tokens end before the input");
    for off in versioned::neutralize_edits(sql, &tokens, 999_999) {
        assert_eq!(sql.get(off), Some(&b'!'), "neutralization off a '!'");
    }
    if let Some(out) = versioned::expand_for_mariadb(sql, &tokens, 999_999) {
        assert!(lex::lex(&out, &opts).is_ok());
    }
}

/// `rewriter`: chapter 07's plan for tenant 42 in `billing`, then each
/// forwarded statement's result mapper.
pub fn fuzz_rewriter(data: &[u8]) {
    let Some((cfg, sql)) = split(data) else {
        return;
    };
    let opts = lex_opts(cfg);
    let map = tenant();
    let registry = ColumnRegistry::mariadb_11_8();
    let mut rw = RewriteCtx::new(&map, opts.persona)
        .lex(opts)
        .current_db(Some("billing"))
        .engine(if cfg & 8 == 0 {
            Engine::Lexer
        } else {
            Engine::Auto
        })
        .backend_version(999_999);
    rw.lower_case_table_names = cfg & 16 != 0;
    let pctx = PlanCtx::new(rw, &registry);
    let Ok(p) = plan(sql, &pctx) else {
        return;
    };
    let octx = OutputCtx::new(&map);
    for s in &p.statements {
        let Action::Forward { sql: fwd, mapper } = &s.action else {
            continue;
        };
        assert!(lex::lex(fwd, &LexOptions::mariadb()).is_ok());
        let _ = unrewrite(fwd, &map, &opts);
        // The mapper over a resultset made of the statement's bytes.
        let mut m = mapper.clone();
        let cols: Vec<ColumnDef> = ["Field", "Create Table", "Db", "Name", "Message"]
            .iter()
            .map(|n| ColumnDef {
                name: Bytes::from_static(n.as_bytes()),
                schema: Bytes::copy_from_slice(&fwd[..fwd.len().min(20)]),
                ..ColumnDef::default()
            })
            .collect();
        let mut defs = cols.clone();
        let row: TextRow = (0..cols.len())
            .map(|i| (i % 2 == 0).then(|| Bytes::copy_from_slice(sql)))
            .collect();
        let _ = m.map_all(&mut defs, [row], &octx);
    }
}

/// `compat`: chapter 28's R0 rewrites on arbitrary text.
pub fn fuzz_compat(data: &[u8]) {
    let Some((cfg, sql)) = split(data) else {
        return;
    };
    let opts = lex_opts(cfg);
    let text = String::from_utf8_lossy(sql);
    let pv = vars::PersonaVars::new(opts.persona, "8.4.8-SprigDB");
    if let Some((out, replaced)) = vars::rewrite_reads(sql, &opts, &pv) {
        assert!(lex::lex(&out, &opts).is_ok());
        assert!(!replaced.is_empty());
    }
    let _ = vars::version_number(&text);
    if let Some(out) = sprig_sql::compat::mysql_text_rewrites(sql, &opts, 999_999) {
        assert!(lex::lex(&out, &opts).is_ok());
    }
    if let Ok(tokens) = lex::lex(sql, &opts) {
        let _ = schema::describe_target(sql, &tokens, opts.no_backslash_escapes);
    }
    let _ = schema::mysql_create_table(&text);
    let _ = collation::mysql_name(&text);
    let _ = collation::mariadb_name(&text);
    let code = u16::from(cfg) * 17;
    let _ = errmap::to_mysql(&errmap::WireError::new(code, *b"HY000", text.clone()));
    let _ = errmap::to_mysql(&errmap::WireError::new(4025, *b"23000", text.clone()));
    let _ = errmap::to_mysql(&errmap::WireError::new(1451, *b"23000", text.clone()));
    let caps = sprig_proto_mysql::Caps::MARIADB_CLIENT_EXTENDED_METADATA;
    if let Ok(mut c) = ColumnDef::decode(&Bytes::copy_from_slice(sql), caps) {
        meta::mysql_column(&mut c);
        assert!(c.ext_type.is_none());
    }
    let lit = |s: &str| sprig_sql::names::rewrite::sql_string(s, opts.no_backslash_escapes);
    let _ = schema::show_columns_sql(&text, &text, cfg & 8 != 0, Some(&text), None, &lit);
}

/// A fuzz target: its name and body.
pub type Target = (&'static str, fn(&[u8]));

/// Every target, by name.
pub const TARGETS: &[Target] = &[
    ("lexer", fuzz_lexer),
    ("rewriter", fuzz_rewriter),
    ("compat", fuzz_compat),
];

/// Runs every file under `<root>/<dir>/<target>/` through its target, for
/// each `dir` in `dirs`; returns how many inputs ran.
pub fn replay_dirs(root: &std::path::Path, dirs: &[&str]) -> usize {
    let mut n = 0;
    for dir in dirs {
        for (name, f) in TARGETS {
            let d = root.join(dir).join(name);
            let Ok(entries) = std::fs::read_dir(&d) else {
                continue;
            };
            let mut paths: Vec<_> = entries.filter_map(|e| e.ok().map(|e| e.path())).collect();
            paths.sort();
            for p in paths {
                if p.is_file() {
                    let bytes = std::fs::read(&p).expect("read corpus file");
                    let r = std::panic::catch_unwind(|| f(&bytes));
                    assert!(r.is_ok(), "{name}: {} panics", p.display());
                    n += 1;
                }
            }
        }
    }
    n
}
