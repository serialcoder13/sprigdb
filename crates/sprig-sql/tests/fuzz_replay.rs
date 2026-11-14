//! The cargo-fuzz targets' corpora and crash regressions (lexer, name
//! rewriter, the translator's core rewrites), replayed on stable in the
//! ordinary test run.
//!
//! The target bodies live in `fuzz/src/lib.rs` (the cargo-fuzz project,
//! outside the workspace); this file includes it by path, so every
//! committed corpus input and every crash regression runs here without
//! nightly or libFuzzer.
//!
//! `write_fuzz_seeds` regenerates `fuzz/corpus/<target>/seed-*` from
//! [`SEEDS`] when `SPRIG_WRITE_FUZZ_SEEDS=1`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    unreachable_pub,
    dead_code
)]

#[path = "../fuzz/src/lib.rs"]
mod targets;

use std::path::{Path, PathBuf};

fn fuzz_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("fuzz")
}

/// Statements the seeds start from: the shapes the name layer and the
/// translator handle.
const SEEDS: &[&str] = &[
    "SELECT * FROM billing.orders WHERE id = 1",
    "SELECT o.id, c.name FROM billing.orders o JOIN crm.customers c ON c.id = o.customer_id",
    "USE crm; SELECT DATABASE(), CURRENT_USER(), @@version, VERSION()",
    "INSERT INTO `billing`.`t` (a, b) VALUES (1, 'x'), (2, \"y\") ON DUPLICATE KEY UPDATE b = VALUES(b)",
    "UPDATE billing.t SET a = a + 1 WHERE b LIKE 'z%' ORDER BY a LIMIT 3",
    "DELETE t1 FROM billing.t1 JOIN crm.t2 USING (id)",
    "CREATE TABLE billing.j (id int PRIMARY KEY, j json, k int /*!80023 INVISIBLE */, KEY ix (k) /*!80000 INVISIBLE */)",
    "CREATE DATABASE shop /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /*!80016 DEFAULT ENCRYPTION='N' */",
    "/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */",
    "/*M!100100 SET sql_mode = 'ORACLE' */ SELECT 1",
    "SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'billing' AND TABLE_NAME = 'j'",
    "SELECT WORD FROM INFORMATION_SCHEMA.KEYWORDS WHERE RESERVED=1 ORDER BY WORD",
    "SELECT information_schema.KEYWORDS.WORD FROM information_schema.KEYWORDS",
    "SELECT COLUMN_NAME, JSON_EXTRACT(HISTOGRAM, '$.\"number-of-buckets-specified\"') FROM information_schema.COLUMN_STATISTICS WHERE SCHEMA_NAME = 'billing' AND TABLE_NAME = 't'",
    "SELECT * FROM information_schema.TABLES t JOIN information_schema.STATISTICS s USING (TABLE_SCHEMA, TABLE_NAME)",
    "SELECT * FROM information_schema.SCHEMATA WHERE SCHEMA_NAME IN ('billing', 'crm')",
    "SELECT * FROM information_schema.CHECK_CONSTRAINTS",
    "SELECT * FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = 'sharded'",
    "SELECT * FROM information_schema.COLLATIONS",
    "SHOW FULL COLUMNS FROM orders FROM billing LIKE 'i%'",
    "SHOW COLUMNS FROM billing.j WHERE `Type` = 'json'",
    "DESCRIBE crm.customers 'n%'",
    "DESC j",
    "EXPLAIN FORMAT=JSON SELECT * FROM billing.t",
    "SHOW CREATE TABLE billing.j",
    "SHOW TABLES FROM crm WHERE Tables_in_crm LIKE 'a%'",
    "SHOW DATABASES WHERE `Database` LIKE '%i%'",
    "SHOW VARIABLES LIKE 'version%'",
    "SHOW INDEX FROM orders",
    "SHOW PROCEDURE STATUS WHERE Db = 'billing'",
    "SELECT @@GLOBAL.version version",
    "SELECT @@session.gtid_mode, @@sql_mode, @@version_comment LIMIT 1",
    "SET @x = @@version; SELECT @x",
    "CREATE VIEW billing.v AS SELECT * FROM crm.c",
    "CREATE PROCEDURE billing.p() BEGIN SELECT * FROM crm.c; END",
    "PREPARE s FROM 'SELECT * FROM billing.t WHERE id = ?'",
    "SELECT * FROM `a``b`.t, `o'k`.u",
    "SELECT * FROM sharded.t",
    "SELECT * FROM __sprig_x",
    "SELECT \"x\" FROM t -- comment\n# other\n/* block */",
    "SELECT 'it''s', 'back\\\\slash', N'n', X'0A', B'01', 0x0A, 0b01, 1.5e-3",
    "CREATE TABLE `j` (\n  `id` int(11) NOT NULL AUTO_INCREMENT,\n  `j` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`j`)),\n  KEY `ix` (`id`) IGNORED\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci",
    "CONSTRAINT `j.k` failed for `shop`.`j`",
    "utf8mb4_uca1400_german2_nopad_as_cs",
    "8.4.8-SprigDB",
];

/// Config bytes the seeds are written with: both personas, with and
/// without `ANSI_QUOTES` and `NO_BACKSLASH_ESCAPES`.
const CONFIGS: &[u8] = &[0, 1, 2, 5];

#[test]
fn fuzz_corpora_and_regressions_replay_cleanly() {
    let n = targets::replay_dirs(&fuzz_dir(), &["corpus", "regressions"]);
    assert!(n >= 100, "only {n} corpus inputs found under fuzz/");
}

#[test]
fn every_corpus_directory_has_a_target() {
    for dir in ["corpus", "regressions"] {
        let Ok(entries) = std::fs::read_dir(fuzz_dir().join(dir)) else {
            continue;
        };
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            assert!(
                targets::TARGETS.iter().any(|t| t.0 == name),
                "fuzz/{dir}/{name}: no such target"
            );
        }
    }
}

#[test]
fn seeds_run_on_every_target() {
    for (name, f) in targets::TARGETS {
        for cfg in CONFIGS {
            for s in SEEDS {
                let mut input = vec![*cfg];
                input.extend_from_slice(s.as_bytes());
                let r = std::panic::catch_unwind(|| f(&input));
                assert!(r.is_ok(), "{name}: seed {cfg} {s:?} panics");
            }
        }
    }
}

#[test]
fn write_fuzz_seeds() {
    if std::env::var("SPRIG_WRITE_FUZZ_SEEDS").is_err() {
        return;
    }
    for (name, _) in targets::TARGETS {
        let dir = fuzz_dir().join("corpus").join(name);
        std::fs::create_dir_all(&dir).unwrap();
        for (i, s) in SEEDS.iter().enumerate() {
            for cfg in CONFIGS {
                let mut input = vec![*cfg];
                input.extend_from_slice(s.as_bytes());
                std::fs::write(dir.join(format!("seed-{i:03}-{cfg}")), input).unwrap();
            }
        }
    }
}
