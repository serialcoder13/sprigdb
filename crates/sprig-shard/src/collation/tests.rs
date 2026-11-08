#![allow(
    clippy::unwrap_used,
    clippy::many_single_char_names,
    clippy::format_collect,
    clippy::too_many_lines
)]

use super::*;

fn ws(name: &str, s: &str, level: Option<u8>) -> String {
    let c = collation_by_name(name).unwrap();
    c.weight_string(s.as_bytes(), Charset::Utf8mb4, level)
        .unwrap()
        .iter()
        .map(|b| format!("{b:02X}"))
        .collect()
}

fn key(name: &str, s: &str) -> Vec<u8> {
    let c = collation_by_name(name).unwrap();
    let mut out = Vec::new();
    c.hash_key(s.as_bytes(), Charset::Utf8mb4, &mut out)
        .unwrap();
    out
}

#[test]
fn tables_load_and_match_known_weights() {
    // Values seen in the weight-string spike and on MariaDB 11.8.9.
    assert_eq!(ws("utf8mb4_uca1400_ai_ci", "a", None), "2075");
    assert_eq!(ws("utf8mb4_uca1400_ai_ci", "a ", None), "20750209");
    assert_eq!(ws("utf8mb4_uca1400_as_cs", "a", None), "207500200002");
    assert_eq!(
        ws("utf8mb4_uca1400_ai_cs", "Aé", None),
        "207520DB000800020002"
    );
    assert_eq!(ws("utf8mb4_uca1400_ai_cs", "Aé", Some(3)), "000800020002");
    assert_eq!(ws("utf8mb4_uca1400_as_ci", "Aé", Some(2)), "002000200024");
    assert_eq!(ws("utf8mb4_unicode_ci", "a", None), "0E33");
    assert_eq!(ws("utf8mb4_bin", "a", None), "000061");
    assert_eq!(ws("utf8mb3_bin", "aé ", None), "006100E90020");
    assert_eq!(ws("utf8mb4_general_ci", "aé😀 ", None), "00410045FFFD0020");
    assert_eq!(ws("utf8mb4_unicode_ci", "aé😀 ", None), "0E330E8BFFFD0209");
    assert_eq!(
        ws("utf8mb4_unicode_520_ci", "aé😀 ", None),
        "120F126BFBC3F600020A"
    );
    // Contractions (UCA 14.0.0 DUCET): И + breve is Й.
    assert_eq!(
        ws("utf8mb4_uca1400_as_cs", "\u{418}\u{306}", None),
        ws("utf8mb4_uca1400_as_cs", "\u{419}", None)
    );
    // latin1 bytes.
    let l = collation_by_name("latin1_swedish_ci").unwrap();
    let w = l
        .weight_string(&[0x61, 0xE9, 0x20], Charset::Latin1, None)
        .unwrap();
    assert_eq!(w, vec![0x41, 0x45, 0x20]);
}

#[test]
fn space_weights() {
    assert_eq!(
        collation_by_name("utf8mb4_uca1400_as_cs")
            .unwrap()
            .space_weight(0),
        &[0x02, 0x09]
    );
    assert_eq!(
        collation_by_name("utf8mb4_uca1400_as_cs")
            .unwrap()
            .space_weight(1),
        &[0x00, 0x20]
    );
    assert_eq!(
        collation_by_name("utf8mb4_uca1400_as_cs")
            .unwrap()
            .space_weight(2),
        &[0x00, 0x02]
    );
    assert_eq!(
        collation_by_name("utf8mb4_uca1400_ai_cs")
            .unwrap()
            .space_weight(1),
        &[0x00, 0x02]
    );
    assert_eq!(
        collation_by_name("utf8mb4_unicode_520_ci")
            .unwrap()
            .space_weight(0),
        &[0x02, 0x0A]
    );
    assert_eq!(
        collation_by_name("utf8mb4_bin").unwrap().space_weight(0),
        &[0, 0, 0x20]
    );
    assert_eq!(
        collation_by_name("latin1_swedish_ci")
            .unwrap()
            .space_weight(0),
        &[0x20]
    );
    assert!(
        collation_by_name("binary")
            .unwrap()
            .space_weight(0)
            .is_empty()
    );
}

#[test]
fn pad_space_equality() {
    // Trailing spaces, NBSP, ideographic space and ignorables (the spike's traps).
    let c = "utf8mb4_uca1400_ai_ci";
    for s in ["a ", "a\u{a0}", "a\u{3000}", "a\u{200b}", "a \u{ad}", "A"] {
        assert_eq!(key(c, s), key(c, "a"), "{s:?}");
    }
    assert_ne!(key(c, "a\t"), key(c, "a"));
    // NO PAD keeps them.
    assert_ne!(
        key("utf8mb4_uca1400_nopad_ai_ci", "a "),
        key("utf8mb4_uca1400_nopad_ai_ci", "a")
    );
    // Multi-level: case differs under as_cs only.
    assert_ne!(
        key("utf8mb4_uca1400_as_cs", "A"),
        key("utf8mb4_uca1400_as_cs", "a")
    );
    assert_eq!(
        key("utf8mb4_uca1400_as_cs", "a  "),
        key("utf8mb4_uca1400_as_cs", "a")
    );
    assert_eq!(
        key("utf8mb4_uca1400_as_ci", "A"),
        key("utf8mb4_uca1400_as_ci", "a")
    );
    assert_ne!(
        key("utf8mb4_uca1400_as_ci", "é"),
        key("utf8mb4_uca1400_as_ci", "e")
    );
    assert_eq!(
        key("utf8mb4_uca1400_ai_ci", "é"),
        key("utf8mb4_uca1400_ai_ci", "e")
    );
    // ß: 'ss' under UCA, 's' under general_ci.
    assert_eq!(key(c, "ß"), key(c, "ss"));
    assert_eq!(
        key("utf8mb4_general_ci", "ß"),
        key("utf8mb4_general_ci", "s")
    );
    // Emoji under general_ci all weigh FFFD.
    assert_eq!(
        key("utf8mb4_general_ci", "😀"),
        key("utf8mb4_general_ci", "😁")
    );
    // Levels are separated: the canonical form from MariaDB's level
    // columns equals the Rust one.
    let a = collation_by_name("utf8mb4_uca1400_as_cs").unwrap();
    let l1 = a
        .weight_string("Ab ".as_bytes(), Charset::Utf8mb4, Some(1))
        .unwrap();
    let l2 = a
        .weight_string("Ab ".as_bytes(), Charset::Utf8mb4, Some(2))
        .unwrap();
    let l3 = a
        .weight_string("Ab ".as_bytes(), Charset::Utf8mb4, Some(3))
        .unwrap();
    let mut from_levels = Vec::new();
    a.canonical_from_levels(&[&l1, &l2, &l3], &mut from_levels);
    assert_eq!(from_levels, key("utf8mb4_uca1400_as_cs", "Ab"));
}

#[test]
fn charsets_and_aliases() {
    assert_eq!(
        collation_by_name("utf8mb4_0900_ai_ci").unwrap().name,
        "utf8mb4_uca1400_nopad_ai_ci"
    );
    assert_eq!(collation(CollationId(255)).unwrap().id, CollationId(2308));
    assert_eq!(
        collation_by_name("utf8_general_ci").unwrap().id,
        CollationId(33)
    );
    assert!(collation_by_name("utf8mb4_de_pb_0900_ai_ci").is_none());
    assert_eq!(
        require_name("latin2_general_ci").unwrap_err().code(),
        Some(SprigCode::UnsupportedKeyCollation)
    );
    // A latin1 'é' and a UTF-8 'é' are the same key under a UTF-8 collation.
    let c = collation_by_name("utf8mb4_unicode_ci").unwrap();
    let mut a = Vec::new();
    let mut b = Vec::new();
    c.hash_key(&[0xE9], Charset::Latin1, &mut a).unwrap();
    c.hash_key("é".as_bytes(), Charset::Utf8mb4, &mut b)
        .unwrap();
    assert_eq!(a, b);
    // cp1252: 0x80 is the euro sign.
    let l = collation_by_name("latin1_bin").unwrap();
    let mut e = Vec::new();
    l.hash_key("€".as_bytes(), Charset::Utf8mb4, &mut e)
        .unwrap();
    assert_eq!(e, vec![0x80]);
    // Unconvertible and invalid input is unroutable.
    let mut x = Vec::new();
    let err = l
        .hash_key("😀".as_bytes(), Charset::Utf8mb4, &mut x)
        .unwrap_err();
    assert_eq!(err.code(), Some(SprigCode::KeyValueUnroutable));
    assert!(c.hash_key(&[0xFF, 0xFE], Charset::Utf8mb4, &mut x).is_err());
    let m3 = collation_by_name("utf8mb3_general_ci").unwrap();
    assert!(
        m3.hash_key("😀".as_bytes(), Charset::Utf8mb4, &mut x)
            .is_err()
    );
    // Every supported collation has a space weight per level (but binary).
    for c in all() {
        for l in 0..c.levels() {
            assert_eq!(
                c.space_weight(l).is_empty(),
                c.charset == Charset::Binary,
                "{}",
                c.name
            );
        }
    }
}
