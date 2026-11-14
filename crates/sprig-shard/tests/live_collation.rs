//! Collation conformance against a live MariaDB.
//!
//! For every supported collation, a corpus built from the weight-string
//! spike's pieces and traps (plus case, accent, space and ignorable
//! variants of each string, so equal pairs are common):
//!
//! 1. `Collation::weight_string(s, LEVEL k)` must equal MariaDB's
//!    `WEIGHT_STRING(s LEVEL k)` byte for byte, for every physical level;
//! 2. hashing must agree with MariaDB's own comparator: `STRCMP(a, b) = 0`
//!    exactly when the canonical bytes are equal (so equal values land on
//!    one shard and different values don't collide by construction).
//!
//! Needs `SPRIG_TEST_MARIADB_SINGLE`. `SPRIG_TEST_COLLATION_STRINGS` sets
//! the random corpus size (default 1500).

#![allow(
    clippy::unwrap_used,
    clippy::many_single_char_names,
    clippy::format_collect,
    clippy::too_many_lines
)]

use mysql_async::prelude::Queryable;
use sprig_shard::collation::{self, Charset};

const PIECES: &[&str] = &[
    "a",
    "A",
    "á",
    "Á",
    "a\u{301}",
    "à",
    "ä",
    "å",
    "Å",
    "\u{212b}",
    "æ",
    "Æ",
    "ae",
    "AE",
    "ß",
    "ss",
    "SS",
    "ẞ",
    "ſ",
    "s",
    "ﬁ",
    "fi",
    "ﬀ",
    "ff",
    "œ",
    "oe",
    "Œ",
    "ı",
    "i",
    "I",
    "İ",
    "ǅ",
    "Ǆ",
    "dž",
    "ĳ",
    "ij",
    "ø",
    "o",
    "ö",
    "ð",
    "d",
    "þ",
    "th",
    "ł",
    "l",
    "ch",
    "c",
    "h",
    "ll",
    "y",
    "ü",
    "u",
    "e",
    "é",
    "E",
    " ",
    "  ",
    "\t",
    "\u{a0}",
    "\u{200b}",
    "\u{ad}",
    "\u{0}",
    "\u{301}",
    "😀",
    "😁",
    "👍",
    "👍🏽",
    "🏽",
    "👨\u{200d}👩",
    "𝒜",
    "\u{10000}",
    "\u{10ffff}",
    "\u{fffd}",
    "中",
    "日",
    "한",
    "ㄱ",
    "ｱ",
    "ア",
    "①",
    "1",
    "¹",
    "½",
    "0",
    "9",
    "-",
    "_",
    ".",
    "Ω",
    "\u{2126}",
    "z",
    "Z",
    "\u{3000}",
    "\u{418}\u{306}",
    "\u{419}",
    "L\u{b7}",
    "l\u{b7}",
    "\u{627}\u{653}",
    "\u{cc6}\u{cc2}\u{cd5}",
    "€",
    "Š",
    "\u{81}",
];

const FIXED: &[&str] = &[
    "",
    " ",
    "  ",
    "a",
    "a ",
    "a  ",
    " a",
    "a\t",
    "a\u{a0}",
    "a\u{200b}",
    "A ",
    "a\u{0}",
    "strasse",
    "straße",
    "STRASSE",
    "Straße ",
    "STRAẞE",
    "ae",
    "æ",
    "Ae",
    "AE",
    "Æ",
    "oe",
    "œ",
    "fi",
    "ﬁ",
    "ff",
    "ﬀ",
    "é",
    "e\u{301}",
    "É",
    "E\u{301}",
    "😀",
    "😁",
    "😀 ",
    "👍",
    "👍🏽",
    "𝒜",
    "ab",
    "a b",
    "a\tb",
    "ab ",
    "a\u{0}b",
    "z",
    "Z ",
    "ı",
    "İ",
    "i",
    "I",
    "Å",
    "\u{212b}",
    "Ω",
    "\u{2126}",
    "o ",
    "ö\u{0}",
    "oe \u{ad}",
    "a\u{3000}",
    "a \u{a0}",
    "a \u{200b}",
    "a \u{0}",
];

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % n as u64).unwrap()
    }
}

fn corpus(n: usize) -> Vec<String> {
    let mut rng = Rng(20_261_108);
    let mut base: Vec<String> = FIXED.iter().map(|s| (*s).to_owned()).collect();
    for _ in 0..n {
        let k = 1 + rng.below(5);
        base.push((0..k).map(|_| PIECES[rng.below(PIECES.len())]).collect());
    }
    // Variants make equal pairs common.
    let mut out = Vec::new();
    for s in base {
        out.push(s.to_uppercase());
        out.push(s.to_lowercase());
        out.push(format!("{s} "));
        out.push(format!("{s}\u{a0}"));
        out.push(format!("{s}\u{200b}"));
        out.push(s.replace('é', "e\u{301}").replace('ß', "ss"));
        out.push(s);
    }
    let mut seen = std::collections::HashSet::new();
    out.retain(|s| s.chars().count() <= 60 && seen.insert(s.clone()));
    out
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02X}")).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn live_collation_conformance() {
    let Ok(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE") else {
        eprintln!("skipped: SPRIG_TEST_MARIADB_SINGLE not set");
        return;
    };
    let n: usize = std::env::var("SPRIG_TEST_COLLATION_STRINGS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1500);
    let pool = mysql_async::Pool::new(url.as_str());
    let mut c = pool.get_conn().await.unwrap();
    let db = "r1shard_coll";
    c.query_drop(format!("DROP DATABASE IF EXISTS {db}"))
        .await
        .unwrap();
    c.query_drop(format!("CREATE DATABASE {db}")).await.unwrap();
    c.query_drop(format!("USE {db}")).await.unwrap();
    c.query_drop(
        "CREATE TABLE corpus (id INT PRIMARY KEY, s VARBINARY(400) NOT NULL) ENGINE=InnoDB",
    )
    .await
    .unwrap();
    let strings = corpus(n);
    for chunk in strings.chunks(500).enumerate() {
        let (ci, rows) = chunk;
        let mut params = Vec::new();
        for (j, s) in rows.iter().enumerate() {
            params.push(mysql_async::Value::Int(
                i64::try_from(ci * 500 + j).unwrap(),
            ));
            params.push(mysql_async::Value::Bytes(s.as_bytes().to_vec()));
        }
        let qs = vec!["(?, ?)"; rows.len()].join(", ");
        c.exec_drop(format!("INSERT INTO corpus VALUES {qs}"), params)
            .await
            .unwrap();
    }
    // Pairs: every string with its variants' neighbours in id order, plus
    // random pairs.
    let mut rng = Rng(7);
    let mut pairs: Vec<(usize, usize)> = Vec::new();
    for i in 0..strings.len() {
        for d in 1..=7 {
            if i + d < strings.len() {
                pairs.push((i, i + d));
            }
        }
    }
    for _ in 0..20_000 {
        pairs.push((rng.below(strings.len()), rng.below(strings.len())));
    }
    c.query_drop("CREATE TABLE pairs (a INT NOT NULL, b INT NOT NULL) ENGINE=InnoDB")
        .await
        .unwrap();
    for chunk in pairs.chunks(2000) {
        let vals: Vec<String> = chunk.iter().map(|(a, b)| format!("({a},{b})")).collect();
        c.query_drop(format!("INSERT INTO pairs VALUES {}", vals.join(",")))
            .await
            .unwrap();
    }

    let mut report = Vec::new();
    let mut failures = Vec::new();
    for coll in collation::all() {
        let cs = match coll.charset {
            Charset::Utf8mb4 => "utf8mb4",
            Charset::Utf8mb3 => "utf8mb3",
            Charset::Latin1 => "latin1",
            Charset::Ascii => "ascii",
            Charset::Binary => "binary",
        };
        // Strings the collation's character set can hold.
        let mut keys: Vec<Option<Vec<u8>>> = Vec::with_capacity(strings.len());
        for s in &strings {
            let mut k = Vec::new();
            keys.push(
                coll.hash_key(s.as_bytes(), Charset::Utf8mb4, &mut k)
                    .ok()
                    .map(|()| k),
            );
        }
        let expr = |col: &str| {
            if coll.charset == Charset::Binary {
                col.to_owned()
            } else {
                format!(
                    "CONVERT(CAST({col} AS CHAR CHARACTER SET utf8mb4) USING {cs}) COLLATE {}",
                    coll.name
                )
            }
        };
        // 1. Weight strings per level.
        let mut checked = 0usize;
        for &level in coll.level_numbers() {
            let lv = if coll.levels() > 1 || level > 1 {
                format!(" LEVEL {level}")
            } else {
                String::new()
            };
            let rows: Vec<(i64, Option<String>)> = c
                .query(format!(
                    "SELECT id, HEX(WEIGHT_STRING({}{lv})) FROM corpus ORDER BY id",
                    expr("s")
                ))
                .await
                .unwrap();
            for (id, w) in rows {
                let i = usize::try_from(id).unwrap();
                if keys[i].is_none() {
                    continue;
                }
                let mine = coll
                    .weight_string(strings[i].as_bytes(), Charset::Utf8mb4, Some(level))
                    .unwrap();
                checked += 1;
                if Some(hex(&mine)) != w && failures.len() < 20 {
                    failures.push(format!(
                        "{} level {level} {:?}: rust {} mariadb {:?}",
                        coll.name,
                        strings[i],
                        hex(&mine),
                        w
                    ));
                }
            }
        }
        // 2. Equality agrees with STRCMP.
        let rows: Vec<(i64, i64, Option<i64>)> = c
            .query(format!(
                "SELECT p.a, p.b, STRCMP({}, {}) FROM pairs p JOIN corpus x ON x.id = p.a \
                 JOIN corpus y ON y.id = p.b",
                expr("x.s"),
                expr("y.s")
            ))
            .await
            .unwrap();
        let (mut npairs, mut equal, mut bad) = (0usize, 0usize, 0usize);
        for (a, b, cmp) in rows {
            let (a, b) = (usize::try_from(a).unwrap(), usize::try_from(b).unwrap());
            let (Some(ka), Some(kb), Some(cmp)) = (&keys[a], &keys[b], cmp) else {
                continue;
            };
            npairs += 1;
            if cmp == 0 {
                equal += 1;
            }
            if (cmp == 0) != (ka == kb) {
                bad += 1;
                if failures.len() < 40 {
                    failures.push(format!(
                        "{}: {:?} vs {:?}: STRCMP {cmp}, keys equal {}",
                        coll.name,
                        strings[a],
                        strings[b],
                        ka == kb
                    ));
                }
            }
        }
        report.push(format!(
            "{:32} weights checked {checked:6}  pairs {npairs:6}  equal {equal:5}  mismatches {bad}",
            coll.name
        ));
    }
    c.query_drop(format!("DROP DATABASE {db}")).await.unwrap();
    drop(c);
    pool.disconnect().await.unwrap();
    println!("corpus {} strings, {} pairs", strings.len(), pairs.len());
    for r in &report {
        println!("{r}");
    }
    assert!(failures.is_empty(), "mismatches:\n{}", failures.join("\n"));
}
