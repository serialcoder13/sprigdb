//! Seeded fuzzing: random bytes, token soup and mutated statements never
//! panic, and whatever rewrites keeps every byte outside its edits.
//! `SPRIG_FUZZ_ITERS` raises the iteration count.

#![allow(clippy::unwrap_used)]

mod common;

use sprig_proto_mysql::packets::Persona;
use sprig_sql::lex::LexOptions;
use sprig_sql::names::{Engine, RewriteCtx, rewrite};

/// xorshift64*: small, seeded, reproducible.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % (n as u64)).unwrap()
    }
}

const VOCAB: &[&str] = &[
    "SELECT ",
    "FROM ",
    "crm",
    "billing",
    ".",
    "*",
    ",",
    "(",
    ")",
    ";",
    " ",
    "`",
    "'",
    "\"",
    "\\",
    "--",
    "-- ",
    "\n",
    "/*",
    "*/",
    "/*!",
    "/*!50001",
    "/*M!100100",
    "/*+",
    "#",
    "JOIN ",
    "ON ",
    "USING ",
    "WHERE ",
    "USE ",
    "CREATE ",
    "ALTER ",
    "DROP ",
    "TABLE ",
    "VIEW ",
    "PROCEDURE ",
    "FUNCTION ",
    "TRIGGER ",
    "EVENT ",
    "BEGIN ",
    "END ",
    "IF ",
    "THEN ",
    "ELSE ",
    "CASE ",
    "WHEN ",
    "LOOP ",
    "WHILE ",
    "DO ",
    "REPEAT ",
    "UNTIL ",
    "DECLARE ",
    "HANDLER ",
    "FOR ",
    "CURSOR ",
    "DATABASE()",
    "SCHEMA()",
    "USER()",
    "CURRENT_USER",
    "mysql_query_attribute_string('tag')",
    "INSERT INTO ",
    "VALUES ",
    "UPDATE ",
    "SET ",
    "DELETE ",
    "GRANT ",
    "REVOKE ",
    "TO ",
    "LOCK ",
    "TABLES ",
    "SHOW ",
    "COLUMNS ",
    "IN ",
    "LIKE ",
    "LOAD DATA INFILE 'x' INTO TABLE ",
    "CALL ",
    "PREPARE s FROM ",
    "EXECUTE IMMEDIATE ",
    "WITH ",
    "AS ",
    "UNION ",
    "NEXT VALUE FOR ",
    "NEXTVAL(",
    "@x",
    "@@x",
    "?",
    "0x1",
    "1e5",
    ".5",
    "_utf8mb4",
    "N'",
    "X'",
    "Ünï",
    "__sprig_x",
    "_sprig_y",
    "information_schema",
    "t42_d1_g1_s1",
    "NOT ATOMIC ",
    "RENAME ",
    "TRUNCATE ",
    "DROP ",
    "ALTER ",
    "TABLE ",
    "TEMPORARY ",
    "OR REPLACE ",
    "IF NOT EXISTS ",
    "IF EXISTS ",
    "INDEX ",
    "TRIGGER ",
    "VIEW ",
    "SEQUENCE ",
    "EVENT ",
    "DATABASE ",
    "DEFINER=`root`@`%` ",
    "CHARACTER SET ",
    "COLLATE ",
    "LIKE ",
    "RENAME TO ",
    "NOWAIT ",
];

fn iterations() -> usize {
    std::env::var("SPRIG_FUZZ_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3000)
}

fn check(sql: &[u8], map: &sprig_sql::names::MemoryNameMap, rng: &mut Rng) {
    let persona = if rng.below(2) == 0 {
        Persona::MySql84
    } else {
        Persona::MariaDb
    };
    let lex = LexOptions::for_persona(persona)
        .ansi_quotes(rng.below(4) == 0)
        .no_backslash_escapes(rng.below(4) == 0);
    let engine = [Engine::Auto, Engine::Lexer, Engine::Ast][rng.below(3)];
    let mut ctx = RewriteCtx::new(map, persona)
        .lex(lex)
        .current_db(Some("billing"))
        .account(common::ALICE)
        .query_attributes(common::ATTRS)
        .engine(engine);
    if rng.below(2) == 0 {
        ctx = ctx.backend_version(110_803);
    }
    if let Ok(r) = rewrite(sql, &ctx) {
        // Classification never panics, nor does the catalog conversion.
        for s in &r.statements {
            if let Some(t) = &s.ddl {
                let _ = t.to_catalog();
            }
        }
        let mut at = 0;
        let mut out = Vec::new();
        for e in &r.edits {
            assert!(
                e.range.start >= at && e.range.end <= sql.len(),
                "edits overlap"
            );
            out.extend_from_slice(&sql[at..e.range.start]);
            out.extend_from_slice(e.replacement.as_bytes());
            at = e.range.end;
        }
        out.extend_from_slice(&sql[at..]);
        assert_eq!(out, r.sql, "output is not input plus edits");
    }
}

#[test]
fn random_bytes_never_panic() {
    let map = common::acme();
    let mut rng = Rng(0x5eed_0001);
    for _ in 0..iterations() {
        let len = rng.below(200);
        let sql: Vec<u8> = (0..len)
            .map(|_| {
                // Mostly printable ASCII, some control and high bytes.
                match rng.below(10) {
                    0 => u8::try_from(rng.below(256)).unwrap(),
                    _ => u8::try_from(32 + rng.below(95)).unwrap(),
                }
            })
            .collect();
        check(&sql, &map, &mut rng);
    }
}

#[test]
fn token_soup_never_panics() {
    let map = common::acme();
    let mut rng = Rng(0x5eed_0002);
    for _ in 0..iterations() {
        let n = rng.below(60);
        let sql: String = (0..n).map(|_| VOCAB[rng.below(VOCAB.len())]).collect();
        check(sql.as_bytes(), &map, &mut rng);
    }
}

#[test]
fn mutated_statements_never_panic() {
    const SEEDS: &[&str] = &[
        "SELECT o.id, DATABASE(), CURRENT_USER() FROM orders AS o JOIN crm.customers AS c ON c.id = o.customer_id",
        "CREATE PROCEDURE crm.p() BEGIN DECLARE c CURSOR FOR SELECT a FROM billing.t; IF x THEN UPDATE crm.u SET a = 1; END IF; END",
        "INSERT INTO crm.t (a) SELECT a FROM billing.s ON DUPLICATE KEY UPDATE a = VALUES(a)",
        "/*!50001 CREATE ALGORITHM=UNDEFINED */ /*!50001 VIEW `v` AS select `crm`.`t`.`a` from `crm`.`t` */",
        "GRANT SELECT ON crm.* TO bob; LOCK TABLES crm.a READ; SHOW TABLES FROM crm",
        "PREPARE s FROM 'SELECT * FROM crm.t WHERE a = ''x'''",
        "CREATE OR REPLACE TEMPORARY TABLE IF NOT EXISTS crm.t2 (LIKE billing.t)",
        "DROP TEMPORARY TABLE IF EXISTS a, crm.b, `my.db`.c; RENAME TABLE IF EXISTS a NOWAIT TO crm.a, crm.b TO b",
        "ALTER TABLE IF EXISTS crm.t ADD c INT, RENAME TO billing.u; TRUNCATE TABLE crm.t WAIT 5",
        "/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 TRIGGER `trg` BEFORE INSERT ON `crm`.`t` FOR EACH ROW SET NEW.a = 1 */",
        "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `crm` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /*!80016 DEFAULT ENCRYPTION='N' */",
        "CREATE OR REPLACE UNIQUE INDEX IF NOT EXISTS i ON crm.t (a); DROP INDEX IF EXISTS i ON crm.t",
        "ALTER EVENT crm.e ON SCHEDULE EVERY 1 DAY RENAME TO billing.e DO DELETE FROM crm.t",
    ];
    let map = common::acme();
    let mut rng = Rng(0x5eed_0003);
    for _ in 0..iterations() {
        let mut sql = SEEDS[rng.below(SEEDS.len())].as_bytes().to_vec();
        for _ in 0..=rng.below(4) {
            let at = rng.below(sql.len() + 1);
            match rng.below(4) {
                0 if at < sql.len() => {
                    sql.remove(at);
                }
                1 => sql.insert(at, b"`'\"()./*-#;\\"[rng.below(12)]),
                2 => sql.truncate(at),
                _ => {
                    let w = VOCAB[rng.below(VOCAB.len())].as_bytes();
                    sql.splice(at..at, w.iter().copied());
                }
            }
        }
        check(&sql, &map, &mut rng);
    }
}
