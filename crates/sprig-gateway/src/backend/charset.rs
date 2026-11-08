//! Character sets and collations at login (chapter 06, "Login collation").
//!
//! MariaDB 11.8 sets a direct client's session from the collation id in its
//! handshake: `character_set_client`, `character_set_connection` and
//! `character_set_results` take the id's character set, and
//! `collation_connection` takes the collation. When the id is its
//! character set's compiled default (`utf8mb4_general_ci`, 45), the server
//! maps it through `@@character_set_collations` first, so 45 becomes
//! `utf8mb4_uca1400_ai_ci`. The client character set then holds that
//! collation too: string parameters of prepared statements get it, so
//! `SELECT 'a' = ?` fails with 1267 when `collation_connection` disagrees.
//!
//! The table holds every collation id below 256 that a handshake can name
//! (one byte) and a client character set can use, captured from MariaDB
//! 11.8.9 with `character_set_collations` empty (so `IS_DEFAULT` marks the
//! compiled defaults). `ucs2`, `utf16`, `utf16le` and `utf32` can't be
//! client character sets and are left out.

/// `(id, collation, compiled default of its character set)`.
const COLLATIONS: &[(u8, &str, bool)] = &[
    (1, "big5_chinese_ci", true),
    (2, "latin2_czech_cs", false),
    (3, "dec8_swedish_ci", true),
    (4, "cp850_general_ci", true),
    (5, "latin1_german1_ci", false),
    (6, "hp8_english_ci", true),
    (7, "koi8r_general_ci", true),
    (8, "latin1_swedish_ci", true),
    (9, "latin2_general_ci", true),
    (10, "swe7_swedish_ci", true),
    (11, "ascii_general_ci", true),
    (12, "ujis_japanese_ci", true),
    (13, "sjis_japanese_ci", true),
    (14, "cp1251_bulgarian_ci", false),
    (15, "latin1_danish_ci", false),
    (16, "hebrew_general_ci", true),
    (18, "tis620_thai_ci", true),
    (19, "euckr_korean_ci", true),
    (20, "latin7_estonian_cs", false),
    (21, "latin2_hungarian_ci", false),
    (22, "koi8u_general_ci", true),
    (23, "cp1251_ukrainian_ci", false),
    (24, "gb2312_chinese_ci", true),
    (25, "greek_general_ci", true),
    (26, "cp1250_general_ci", true),
    (27, "latin2_croatian_ci", false),
    (28, "gbk_chinese_ci", true),
    (29, "cp1257_lithuanian_ci", false),
    (30, "latin5_turkish_ci", true),
    (31, "latin1_german2_ci", false),
    (32, "armscii8_general_ci", true),
    (33, "utf8mb3_general_ci", true),
    (34, "cp1250_czech_cs", false),
    (36, "cp866_general_ci", true),
    (37, "keybcs2_general_ci", true),
    (38, "macce_general_ci", true),
    (39, "macroman_general_ci", true),
    (40, "cp852_general_ci", true),
    (41, "latin7_general_ci", true),
    (42, "latin7_general_cs", false),
    (43, "macce_bin", false),
    (44, "cp1250_croatian_ci", false),
    (45, "utf8mb4_general_ci", true),
    (46, "utf8mb4_bin", false),
    (47, "latin1_bin", false),
    (48, "latin1_general_ci", false),
    (49, "latin1_general_cs", false),
    (50, "cp1251_bin", false),
    (51, "cp1251_general_ci", true),
    (52, "cp1251_general_cs", false),
    (53, "macroman_bin", false),
    (57, "cp1256_general_ci", true),
    (58, "cp1257_bin", false),
    (59, "cp1257_general_ci", true),
    (63, "binary", true),
    (64, "armscii8_bin", false),
    (65, "ascii_bin", false),
    (66, "cp1250_bin", false),
    (67, "cp1256_bin", false),
    (68, "cp866_bin", false),
    (69, "dec8_bin", false),
    (70, "greek_bin", false),
    (71, "hebrew_bin", false),
    (72, "hp8_bin", false),
    (73, "keybcs2_bin", false),
    (74, "koi8r_bin", false),
    (75, "koi8u_bin", false),
    (77, "latin2_bin", false),
    (78, "latin5_bin", false),
    (79, "latin7_bin", false),
    (80, "cp850_bin", false),
    (81, "cp852_bin", false),
    (82, "swe7_bin", false),
    (83, "utf8mb3_bin", false),
    (84, "big5_bin", false),
    (85, "euckr_bin", false),
    (86, "gb2312_bin", false),
    (87, "gbk_bin", false),
    (88, "sjis_bin", false),
    (89, "tis620_bin", false),
    (91, "ujis_bin", false),
    (92, "geostd8_general_ci", true),
    (93, "geostd8_bin", false),
    (94, "latin1_spanish_ci", false),
    (95, "cp932_japanese_ci", true),
    (96, "cp932_bin", false),
    (97, "eucjpms_japanese_ci", true),
    (98, "eucjpms_bin", false),
    (99, "cp1250_polish_ci", false),
    (192, "utf8mb3_unicode_ci", false),
    (193, "utf8mb3_icelandic_ci", false),
    (194, "utf8mb3_latvian_ci", false),
    (195, "utf8mb3_romanian_ci", false),
    (196, "utf8mb3_slovenian_ci", false),
    (197, "utf8mb3_polish_ci", false),
    (198, "utf8mb3_estonian_ci", false),
    (199, "utf8mb3_spanish_ci", false),
    (200, "utf8mb3_swedish_ci", false),
    (201, "utf8mb3_turkish_ci", false),
    (202, "utf8mb3_czech_ci", false),
    (203, "utf8mb3_danish_ci", false),
    (204, "utf8mb3_lithuanian_ci", false),
    (205, "utf8mb3_slovak_ci", false),
    (206, "utf8mb3_spanish2_ci", false),
    (207, "utf8mb3_roman_ci", false),
    (208, "utf8mb3_persian_ci", false),
    (209, "utf8mb3_esperanto_ci", false),
    (210, "utf8mb3_hungarian_ci", false),
    (211, "utf8mb3_sinhala_ci", false),
    (212, "utf8mb3_german2_ci", false),
    (213, "utf8mb3_croatian_mysql561_ci", false),
    (214, "utf8mb3_unicode_520_ci", false),
    (215, "utf8mb3_vietnamese_ci", false),
    (223, "utf8mb3_general_mysql500_ci", false),
    (224, "utf8mb4_unicode_ci", false),
    (225, "utf8mb4_icelandic_ci", false),
    (226, "utf8mb4_latvian_ci", false),
    (227, "utf8mb4_romanian_ci", false),
    (228, "utf8mb4_slovenian_ci", false),
    (229, "utf8mb4_polish_ci", false),
    (230, "utf8mb4_estonian_ci", false),
    (231, "utf8mb4_spanish_ci", false),
    (232, "utf8mb4_swedish_ci", false),
    (233, "utf8mb4_turkish_ci", false),
    (234, "utf8mb4_czech_ci", false),
    (235, "utf8mb4_danish_ci", false),
    (236, "utf8mb4_lithuanian_ci", false),
    (237, "utf8mb4_slovak_ci", false),
    (238, "utf8mb4_spanish2_ci", false),
    (239, "utf8mb4_roman_ci", false),
    (240, "utf8mb4_persian_ci", false),
    (241, "utf8mb4_esperanto_ci", false),
    (242, "utf8mb4_hungarian_ci", false),
    (243, "utf8mb4_sinhala_ci", false),
    (244, "utf8mb4_german2_ci", false),
    (245, "utf8mb4_croatian_mysql561_ci", false),
    (246, "utf8mb4_unicode_520_ci", false),
    (247, "utf8mb4_vietnamese_ci", false),
    (255, "utf8mb4_0900_ai_ci", false),
];

/// MariaDB 11.8's default `character_set_collations`.
pub const DEFAULT_CHARSET_COLLATIONS: &str = "utf8mb3=utf8mb3_uca1400_ai_ci,\
    ucs2=ucs2_uca1400_ai_ci,utf8mb4=utf8mb4_uca1400_ai_ci,utf16=utf16_uca1400_ai_ci,\
    utf32=utf32_uca1400_ai_ci";

/// The character set of a collation name: the part before the first `_`
/// (`binary` is its own).
#[must_use]
pub fn charset_of(collation: &str) -> &str {
    collation.split('_').next().unwrap_or(collation)
}

/// The collation a handshake id names, and whether it is its character
/// set's compiled default; `None` for an id no client character set has.
#[must_use]
pub fn by_id(id: u16) -> Option<(&'static str, bool)> {
    COLLATIONS
        .iter()
        .find(|(i, _, _)| u16::from(*i) == id)
        .map(|(_, n, p)| (*n, *p))
}

/// The compiled default collation of a character set (what
/// `SET character_set_client = cs` gives the client character set: that
/// statement ignores `character_set_collations`).
#[must_use]
pub fn compiled_default(charset: &str) -> Option<&'static str> {
    COLLATIONS
        .iter()
        .find(|(_, n, p)| *p && charset_of(n).eq_ignore_ascii_case(charset))
        .map(|(_, n, _)| *n)
}

/// The collation `character_set_collations` (`cs=collation,...`) maps
/// `charset` to.
#[must_use]
pub fn mapped<'a>(charset_collations: &'a str, charset: &str) -> Option<&'a str> {
    charset_collations.split(',').find_map(|pair| {
        let (cs, coll) = pair.split_once('=')?;
        cs.trim()
            .eq_ignore_ascii_case(charset)
            .then(|| coll.trim())
            .filter(|c| !c.is_empty())
    })
}

/// What MariaDB sets for a handshake collation id, given the server's
/// `character_set_collations`: `(character set, collation)`. `None` for an
/// unknown id: the server keeps its defaults, which is the pool's baseline.
#[must_use]
pub fn login(id: u16, charset_collations: &str) -> Option<(String, String)> {
    let (name, primary) = by_id(id)?;
    let cs = charset_of(name);
    let coll = if primary {
        mapped(charset_collations, cs).unwrap_or(name)
    } else {
        name
    };
    Some((cs.to_owned(), coll.to_owned()))
}

/// Whether `s` can be written bare in `SET NAMES` (character set and
/// collation names are ASCII letters, digits and `_`).
#[must_use]
pub fn plain_name(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn login_follows_character_set_collations() {
        let m = DEFAULT_CHARSET_COLLATIONS;
        let l = |id| login(id, m);
        // Compiled defaults are mapped; other collations are kept.
        assert_eq!(
            l(45),
            Some(("utf8mb4".into(), "utf8mb4_uca1400_ai_ci".into()))
        );
        assert_eq!(
            l(33),
            Some(("utf8mb3".into(), "utf8mb3_uca1400_ai_ci".into()))
        );
        assert_eq!(
            l(224),
            Some(("utf8mb4".into(), "utf8mb4_unicode_ci".into()))
        );
        assert_eq!(l(46), Some(("utf8mb4".into(), "utf8mb4_bin".into())));
        assert_eq!(
            l(255),
            Some(("utf8mb4".into(), "utf8mb4_0900_ai_ci".into()))
        );
        // No mapping for latin1.
        assert_eq!(l(8), Some(("latin1".into(), "latin1_swedish_ci".into())));
        assert_eq!(l(63), Some(("binary".into(), "binary".into())));
        // ucs2 can't be a client character set; 0 is no collation.
        assert_eq!(l(35), None);
        assert_eq!(l(0), None);
        // A server with another mapping.
        assert_eq!(
            login(45, "utf8mb4=utf8mb4_unicode_ci").map(|x| x.1),
            Some("utf8mb4_unicode_ci".into())
        );
        assert_eq!(
            login(45, "").map(|x| x.1),
            Some("utf8mb4_general_ci".into())
        );
    }

    #[test]
    fn compiled_defaults_and_names() {
        assert_eq!(compiled_default("utf8mb4"), Some("utf8mb4_general_ci"));
        assert_eq!(compiled_default("latin1"), Some("latin1_swedish_ci"));
        assert_eq!(compiled_default("binary"), Some("binary"));
        assert_eq!(compiled_default("nope"), None);
        assert_eq!(charset_of("utf8mb4_0900_ai_ci"), "utf8mb4");
        assert_eq!(charset_of("binary"), "binary");
        assert!(plain_name("utf8mb4_uca1400_ai_ci"));
        assert!(!plain_name("x' OR 1"));
        assert!(!plain_name(""));
        // Every compiled default is unique per character set.
        for (_, n, p) in COLLATIONS {
            if *p {
                assert_eq!(compiled_default(charset_of(n)), Some(*n));
            }
        }
    }
}
