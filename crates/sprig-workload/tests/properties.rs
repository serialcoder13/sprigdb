//! Property tests and a seeded fuzz test of the normalizer, digest and
//! shape extractor (chapter 12, "Testing"):
//!
//! - normalization is idempotent;
//! - the id is stable across whitespace, comments and literal values
//!   (including `IN` list lengths and `VALUES` row counts);
//! - statements of different shapes never share a text or an id;
//! - nothing panics, on generated SQL or on arbitrary bytes.

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use sprig_common::ids::{DbId, TenantId};
use sprig_proto_mysql::packets::Persona;
use sprig_sql::lex::LexOptions;
use sprig_workload::fingerprint::{FpKind, SqlCtx, SqlFingerprinter, digest, normalize};
use sprig_workload::shape::{ExtractCtx, NoResolver, extract};

/// Statement templates. `{n}` is a number, `{s}` a string without a
/// leading wildcard, `{l}` an IN list, `{v}` VALUES rows of two, `{_}`
/// whitespace (where the template has a space).
const TEMPLATES: &[&str] = &[
    "SELECT c FROM sbtest1 WHERE id={n}",
    "SELECT c FROM sbtest1 WHERE id BETWEEN {n} AND {n}",
    "UPDATE sbtest1 SET k=k+{n} WHERE id={n}",
    "DELETE FROM sbtest1 WHERE id={n}",
    "INSERT INTO sbtest1 (id, c) VALUES {v}",
    "SELECT * FROM orders WHERE status IN {l} AND customer_id = {n}",
    "SELECT o.id, o.total FROM orders o JOIN customers c ON c.id = o.customer_id WHERE c.name = {s} ORDER BY o.created_at DESC LIMIT {n}",
    "SELECT * FROM orders WHERE email LIKE {s}",
    "SELECT COUNT(*) FROM orders WHERE created_at > {s} GROUP BY status",
    "SELECT * FROM orders WHERE id IN (SELECT order_id FROM order_items WHERE qty > {n})",
    "SELECT /*+ MAX_EXECUTION_TIME({n}) */ * FROM orders WHERE total < {n}",
    "INSERT INTO orders (id, total) VALUES {v} ON DUPLICATE KEY UPDATE total = VALUES(total) + {n}",
    "SELECT * FROM orders WHERE (customer_id, status) IN (({n}, {s}), ({n}, {s}))",
    "SELECT JSON_VALUE(doc, '$.a') FROM products WHERE JSON_VALUE(doc, '$.b') = {s}",
    "WITH big AS (SELECT customer_id FROM orders WHERE total > {n}) SELECT * FROM customers c JOIN big ON big.customer_id = c.id",
    "SELECT IF(total > {n}, {s}, {s}), LEFT(note, {n}) FROM orders WHERE id = {n}",
    "SELECT * FROM orders WHERE created_at > NOW() - INTERVAL {n} DAY",
    "SELECT * FROM `orders` WHERE `orders`.`id` = {n} LIMIT {n}",
    "SELECT * FROM orders WHERE id = {n} FOR UPDATE",
    "SET @a = {n}",
];

/// A literal: numbers (with signs, decimals, exponents, hex), strings
/// (quoted both ways, introducers, typed), booleans, placeholders.
fn number() -> impl Strategy<Value = String> {
    prop_oneof![
        any::<u32>().prop_map(|n| n.to_string()),
        any::<i32>().prop_map(|n| n.to_string()),
        (0u32..1000, 0u32..1000).prop_map(|(a, b)| format!("{a}.{b}")),
        (1u32..9, 0i32..20).prop_map(|(a, e)| format!("{a}e{e}")),
        any::<u16>().prop_map(|n| format!("0x{n:X}")),
        Just("?".to_owned()),
    ]
}

fn string() -> impl Strategy<Value = String> {
    prop_oneof![
        "[a-z0-9 ]{0,12}".prop_map(|s| format!("'a{s}'")),
        "[a-z0-9]{0,8}".prop_map(|s| format!("\"b{s}\"")),
        "[a-z]{0,6}".prop_map(|s| format!("_utf8mb4'c{s}'")),
        Just("DATE '2026-11-08'".to_owned()),
        Just("'it''s'".to_owned()),
        Just("?".to_owned()),
    ]
}

fn list() -> impl Strategy<Value = String> {
    proptest::collection::vec(prop_oneof![number(), string()], 1..20)
        .prop_map(|v| format!("({})", v.join(", ")))
}

fn rows() -> impl Strategy<Value = String> {
    proptest::collection::vec((number(), string()), 1..10).prop_map(|v| {
        v.iter()
            .map(|(a, b)| format!("({a}, {b})"))
            .collect::<Vec<_>>()
            .join(", ")
    })
}

fn plain_space() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(" ".to_owned()),
        Just("  ".to_owned()),
        Just("\n".to_owned()),
        Just("\t ".to_owned())
    ]
}

fn space() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(" ".to_owned()),
        Just("  ".to_owned()),
        Just("\n".to_owned()),
        Just("\t ".to_owned()),
        Just(" /* c */ ".to_owned()),
        Just(" /* multi\nline */ ".to_owned()),
        Just(" -- line comment\n".to_owned()),
        Just(" # hash\n".to_owned()),
    ]
}

/// Fills a template: each placeholder from its own strategy, each space
/// replaced by a whitespace or comment variant.
#[allow(clippy::many_single_char_names)]
fn fill(t: &'static str) -> impl Strategy<Value = String> {
    let parts: Vec<&'static str> = t.split(' ').collect();
    let n = parts.len();
    let slots = t.matches('{').count();
    // Comments can't nest inside a hint: plain whitespace there.
    let spaces = if t.contains("/*+") {
        proptest::collection::vec(plain_space().boxed(), n).boxed()
    } else {
        proptest::collection::vec(space().boxed(), n).boxed()
    };
    (
        spaces,
        proptest::collection::vec((number(), string(), list(), rows()), slots),
    )
        .prop_map(move |(spaces, vals)| {
            let mut out = String::new();
            for (k, p) in parts.iter().enumerate() {
                if k > 0 {
                    out.push_str(&spaces[k]);
                }
                out.push_str(p);
            }
            let mut res = String::new();
            let mut rest = out.as_str();
            let mut k = 0;
            while let Some(i) = rest.find('{') {
                res.push_str(&rest[..i]);
                let close = rest[i..].find('}').map_or(rest.len(), |c| i + c);
                let (num, s, l, r) = &vals[k.min(vals.len() - 1)];
                res.push_str(match &rest[i + 1..close] {
                    "n" => num,
                    "s" => s,
                    "l" => l,
                    "v" => r,
                    _ => "",
                });
                k += 1;
                rest = &rest[(close + 1).min(rest.len())..];
            }
            res.push_str(rest);
            res
        })
}

fn opts() -> LexOptions {
    LexOptions::mysql84()
}

fn text(sql: &str) -> String {
    normalize(sql.as_bytes(), &opts(), Some("shop"), false).text
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, ..ProptestConfig::default() })]

    #[test]
    fn idempotent(sql in (0..TEMPLATES.len()).prop_flat_map(|k| fill(TEMPLATES[k]))) {
        let once = text(&sql);
        prop_assert_eq!(text(&once), once.clone(), "{}", sql);
    }

    #[test]
    fn stable_across_whitespace_comments_and_literals(
        k in 0..TEMPLATES.len(),
        seed in any::<[u8; 32]>(),
    ) {
        let mut runner = proptest::test_runner::TestRunner::new_with_rng(
            ProptestConfig::default(),
            proptest::test_runner::TestRng::from_seed(proptest::test_runner::RngAlgorithm::ChaCha, &seed),
        );
        let a = fill(TEMPLATES[k]).new_tree(&mut runner).expect("tree").current();
        let b = fill(TEMPLATES[k]).new_tree(&mut runner).expect("tree").current();
        let (ta, tb) = (text(&a), text(&b));
        prop_assert_eq!(&ta, &tb, "\n{}\n{}", a, b);
        prop_assert_eq!(
            digest(FpKind::Sql, DbId(1), None, &ta),
            digest(FpKind::Sql, DbId(1), None, &tb)
        );
    }

    #[test]
    fn different_shapes_never_share_a_text(
        i in 0..TEMPLATES.len(),
        j in 0..TEMPLATES.len(),
        seed in any::<[u8; 32]>(),
    ) {
        prop_assume!(i != j);
        let mut runner = proptest::test_runner::TestRunner::new_with_rng(
            ProptestConfig::default(),
            proptest::test_runner::TestRng::from_seed(proptest::test_runner::RngAlgorithm::ChaCha, &seed),
        );
        let a = fill(TEMPLATES[i]).new_tree(&mut runner).expect("tree").current();
        let b = fill(TEMPLATES[j]).new_tree(&mut runner).expect("tree").current();
        let (ta, tb) = (text(&a), text(&b));
        prop_assert_ne!(&ta, &tb);
        prop_assert_ne!(digest(FpKind::Sql, DbId(1), None, &ta), digest(FpKind::Sql, DbId(1), None, &tb));
    }

    #[test]
    fn never_panics_on_bytes(bytes in proptest::collection::vec(any::<u8>(), 0..300)) {
        run_all(&bytes);
    }

    #[test]
    fn never_panics_on_sql_soup(words in proptest::collection::vec(soup(), 0..60)) {
        run_all(words.join(" ").as_bytes());
        run_all(words.concat().as_bytes());
    }
}

/// The literal-sensitive differences the id must keep: these pairs are
/// different shapes.
#[test]
fn shape_changes_change_the_id() {
    let pairs = [
        ("SELECT * FROM t WHERE a = 1", "SELECT * FROM t WHERE b = 1"),
        ("SELECT * FROM t WHERE a = 1", "SELECT * FROM t WHERE a > 1"),
        (
            "SELECT * FROM t WHERE a = 1",
            "SELECT * FROM t WHERE a IS NULL",
        ),
        (
            "SELECT * FROM t WHERE a LIKE 'x%'",
            "SELECT * FROM t WHERE a LIKE '%x'",
        ),
        (
            "SELECT * FROM t WHERE a IN (1, 2)",
            "SELECT * FROM t WHERE a NOT IN (1, 2)",
        ),
        ("SELECT a FROM t", "SELECT b FROM t"),
        (
            "SELECT * FROM t ORDER BY a",
            "SELECT * FROM t ORDER BY a DESC",
        ),
        ("SELECT * FROM t", "SELECT * FROM u"),
        ("SELECT * FROM t LIMIT 1", "SELECT * FROM t"),
        (
            "SELECT JSON_VALUE(d, '$.a') FROM t",
            "SELECT JSON_VALUE(d, '$.b') FROM t",
        ),
    ];
    for (a, b) in pairs {
        assert_ne!(text(a), text(b), "{a} / {b}");
    }
    // The database and the caller are part of the id.
    let t = text("SELECT 1");
    assert_ne!(
        digest(FpKind::Sql, DbId(1), None, &t),
        digest(FpKind::Sql, DbId(2), None, &t)
    );
    assert_ne!(
        digest(FpKind::Sql, DbId(1), None, &t),
        digest(FpKind::Sql, DbId(1), Some(9), &t)
    );
}

fn soup() -> impl Strategy<Value = String> {
    prop_oneof![
        prop::sample::select(vec![
            "SELECT",
            "FROM",
            "WHERE",
            "AND",
            "OR",
            "IN",
            "(",
            ")",
            ",",
            ".",
            "=",
            "<",
            ">=",
            "-",
            "+",
            "*",
            "LIKE",
            "IS",
            "NULL",
            "NOT",
            "JOIN",
            "ON",
            "AS",
            "ORDER",
            "BY",
            "GROUP",
            "LIMIT",
            "VALUES",
            "INSERT",
            "INTO",
            "UPDATE",
            "SET",
            "DELETE",
            "WITH",
            "UNION",
            "EXISTS",
            "BETWEEN",
            "CASE",
            "WHEN",
            "THEN",
            "END",
            "/*+",
            "*/",
            "/*!",
            "/*M!",
            "--",
            "#",
            "\n",
            "'",
            "\"",
            "`",
            "?",
            "@x",
            "@@y",
            "->>",
            "COUNT(",
            "JSON_VALUE(",
            "ROW",
            "DATE",
            "_utf8mb4",
            "TRUE",
            "1e",
            "0x",
            ";",
            "LATERAL",
            "RECURSIVE",
            "USING",
            "TABLE",
            "TRUNCATE",
        ])
        .prop_map(str::to_owned),
        "[a-z_]{1,6}",
        "[0-9]{1,4}",
        "'[a-z%_]{0,4}'",
        "`[a-z ]{0,4}`",
    ]
}

/// Runs everything on one input; panics are failures.
fn run_all(bytes: &[u8]) {
    let f = SqlFingerprinter::new(64);
    for lex in [
        LexOptions::mysql84(),
        LexOptions::mariadb(),
        LexOptions::mysql84()
            .ansi_quotes(true)
            .no_backslash_escapes(true),
    ] {
        for (db, ltn) in [(Some("shop"), false), (None, true)] {
            let n = normalize(bytes, &lex, db, ltn);
            let ctx = ExtractCtx {
                lex,
                current_db: db,
                lower_case_table_names: ltn,
            };
            let _ = extract(bytes, &n, &ctx, &NoResolver).to_json();
            let mut sctx = SqlCtx::new(Persona::MySql84);
            sctx.lex = lex;
            sctx.current_db = db;
            sctx.lower_case_table_names = ltn;
            sctx.max_text_bytes = 64;
            let fp = f.sql(TenantId(1), bytes, &sctx, &NoResolver);
            assert!(fp.text.len() <= 64 || fp.flags.unparsed);
            for l in &fp.literals {
                assert!(l.span.start <= l.span.end && l.span.end as usize <= bytes.len());
            }
        }
    }
}

/// A deterministic PRNG (splitmix64), so the fuzz run is the same on
/// every machine.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % (n.max(1) as u64)).unwrap_or(0)
    }
}

/// Seeded fuzz: the golden corpora's statements, mutated (bytes flipped,
/// inserted, deleted, spliced with other statements, truncated), through
/// the normalizer, the extractor and the fingerprinter. Set
/// `SPRIG_FUZZ_ITERS` for a longer run; `SPRIG_FUZZ_SEED` for another seed.
#[test]
fn seeded_fuzz() {
    let mut corpus: Vec<Vec<u8>> = Vec::new();
    for file in ["normalize.txt", "shapes.txt"] {
        let path = format!("{}/tests/fixtures/{file}", env!("CARGO_MANIFEST_DIR"));
        let text = std::fs::read_to_string(path).expect("fixture");
        corpus.extend(
            text.lines()
                .filter_map(|l| l.strip_prefix("in: "))
                .map(|s| s.replace('⏎', "\n").into_bytes()),
        );
    }
    let iters: usize = std::env::var("SPRIG_FUZZ_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3000);
    let seed: u64 = std::env::var("SPRIG_FUZZ_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0x5eed_2026);
    let mut rng = Rng(seed);
    let alphabet = b" '\"`()/*+-#!?.,;=<>_%\\\n0123456789abcxyzSELECTFROMWHERE";
    for _ in 0..iters {
        let mut s = corpus[rng.below(corpus.len())].clone();
        for _ in 0..=rng.below(6) {
            let len = s.len();
            match rng.below(6) {
                0 if len > 0 => s[rng.below(len)] = alphabet[rng.below(alphabet.len())],
                1 => s.insert(rng.below(len + 1), alphabet[rng.below(alphabet.len())]),
                2 if len > 0 => {
                    s.remove(rng.below(len));
                }
                3 => {
                    let other = &corpus[rng.below(corpus.len())];
                    let at = rng.below(other.len() + 1);
                    let pos = rng.below(len + 1);
                    let tail: Vec<u8> = other[at..].to_vec();
                    s.splice(pos..pos, tail);
                }
                4 if len > 0 => s.truncate(rng.below(len)),
                _ => {
                    #[allow(clippy::cast_possible_truncation)]
                    let b = rng.next() as u8;
                    s.insert(rng.below(len + 1), b);
                }
            }
        }
        run_all(&s);
    }
}
