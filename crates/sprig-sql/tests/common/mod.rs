//! Shared fixtures for the rewrite tests: tenant 42 ("acme") with a few
//! awkwardly named databases.

#![allow(dead_code, unreachable_pub)]

pub mod fixture;
pub mod leaks;

use sprig_common::ids::TenantId;
use sprig_proto_mysql::packets::Persona;
use sprig_sql::lex::LexOptions;
use sprig_sql::names::{AccountInfo, Engine, MemoryNameMap, RewriteCtx, Rewritten, rewrite};

/// `billing` → `t42_d1_g1_s1`.
pub const B: &str = "`t42_d1_g1_s1`";
/// `crm` → `t42_d2_g2_s2`.
pub const C: &str = "`t42_d2_g2_s2`";

pub fn acme() -> MemoryNameMap {
    MemoryNameMap::new(TenantId(42))
        .with_db(1, "billing", 1, 1)
        .with_db(2, "crm", 2, 2)
        .with_db(3, "my.db", 3, 3)
        .with_db(4, "a`b", 4, 4)
        .with_db(5, "Ünïcødé", 5, 5)
        .with_db(6, "select", 6, 6)
        .with_db(7, "my_db", 7, 7)
        .with_db(8, "o'k\\db", 8, 8)
        .with_db(9, "Mixed", 9, 9)
}

pub const ALICE: AccountInfo<'static> = AccountInfo {
    user: "alice",
    account_host: "%",
    client_host: "203.0.113.7",
};

pub const ATTRS: &[(&str, Option<&str>)] = &[("tag", Some("v'1")), ("nul", None)];

/// A session of alice in `billing`.
pub fn ctx(map: &MemoryNameMap, lex: LexOptions, engine: Engine) -> RewriteCtx<'_> {
    let mut c = RewriteCtx::new(map, lex.persona)
        .lex(lex)
        .current_db(Some("billing"))
        .account(ALICE)
        .query_attributes(ATTRS)
        .engine(engine);
    c.backend_version = None;
    c
}

pub fn render(r: Result<Rewritten, sprig_sql::names::NameError>) -> String {
    match r {
        Ok(r) => r.sql_lossy(),
        Err(e) => format!("ERR {} {e}", e.errno()),
    }
}

/// Rewrites with both engines, checks they agree, and returns the
/// rendered result.
pub fn both(map: &MemoryNameMap, lex: LexOptions, sql: &str) -> String {
    let lexer = render(rewrite(sql.as_bytes(), &ctx(map, lex, Engine::Lexer)));
    let auto_r = rewrite(sql.as_bytes(), &ctx(map, lex, Engine::Auto));
    if let Ok(r) = &auto_r {
        for s in &r.statements {
            assert!(
                s.engines_agree,
                "the AST and the walker disagree on {sql:?} ({:?})",
                lex.persona
            );
        }
    }
    let auto = render(auto_r);
    assert_eq!(lexer, auto, "engines differ on {sql:?}");
    lexer
}

pub fn mysql() -> LexOptions {
    LexOptions::mysql84()
}

pub fn mariadb() -> LexOptions {
    LexOptions::mariadb()
}

pub const PERSONAS: [Persona; 2] = [Persona::MySql84, Persona::MariaDb];
