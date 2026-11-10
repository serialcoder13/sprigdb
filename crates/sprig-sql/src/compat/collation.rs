//! The collation map (chapter 28, "Collation map"; build step 3).
//!
//! MySQL 8.4's `utf8mb4_*_0900_*` collations and MariaDB's UCA 14.0
//! collations, both ways, with MySQL's metadata ids. MariaDB 11.4.5 and
//! later accept the MySQL names as aliases (`COLLATIONS.COMMENT` reads
//! "Alias for `utf8mb4_uca1400_nopad_ai_ci`"), keep the alias name in
//! `information_schema` and `SHOW CREATE TABLE`, and send the alias id in
//! column metadata, so a client that names a MySQL collation gets it back.
//! What the map adds:
//!
//! - **Output on 3306** ([`mysql_name`]): MariaDB's own UCA 14.0 names,
//!   which a table gets from the server default when the client named no
//!   collation, read as the MySQL 0900 collation with the same tailoring
//!   and strength. MariaDB's PAD SPACE variants (`utf8mb4_uca1400_ai_ci`)
//!   read as the NO PAD MySQL name too: MySQL has no PAD SPACE UCA 9.0.0
//!   collation, and the only difference is trailing spaces in comparisons
//!   (decision in chapter 28).
//! - **Input** ([`mariadb_name`]): the MariaDB collation a MySQL name
//!   stands for, and `None` for the MySQL tailorings MariaDB lacks
//!   (`ja`, `zh`, `ru`, `nb`, `nn`, `sr_latn`, `bs`, `bg`, `gl`, `mn_cyrl`),
//!   which fail with 1273 as on a server without them.
//! - **Ids** ([`mysql_id`]): MySQL's id for each 0900 name, for metadata.
//!
//! Shard hashing ([chapter 15]) uses the same map, so a column's mapping
//! never changes.
//!
//! The table was captured on 8 November 2026 from MySQL 8.4.11
//! (`information_schema.COLLATIONS`) and MariaDB 11.8.9 (the aliases'
//! `COMMENT`).

use std::fmt::Write as _;

/// One MySQL 0900 collation: its id, its name, and the MariaDB collation
/// it is an alias of (`None`: MariaDB 11.8 lacks it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Collation {
    /// MySQL 8.4's id.
    pub id: u16,
    /// MySQL's name.
    pub mysql: &'static str,
    /// The MariaDB collation the name is an alias of.
    pub mariadb: Option<&'static str>,
}

const fn c(id: u16, mysql: &'static str, mariadb: &'static str) -> Collation {
    Collation {
        id,
        mysql,
        mariadb: Some(mariadb),
    }
}

const fn missing(id: u16, mysql: &'static str) -> Collation {
    Collation {
        id,
        mysql,
        mariadb: None,
    }
}

/// Every MySQL 8.4 `utf8mb4` 0900 collation, by id.
pub const MAP: &[Collation] = &[
    c(255, "utf8mb4_0900_ai_ci", "utf8mb4_uca1400_nopad_ai_ci"),
    c(
        256,
        "utf8mb4_de_pb_0900_ai_ci",
        "utf8mb4_uca1400_german2_nopad_ai_ci",
    ),
    c(
        257,
        "utf8mb4_is_0900_ai_ci",
        "utf8mb4_uca1400_icelandic_nopad_ai_ci",
    ),
    c(
        258,
        "utf8mb4_lv_0900_ai_ci",
        "utf8mb4_uca1400_latvian_nopad_ai_ci",
    ),
    c(
        259,
        "utf8mb4_ro_0900_ai_ci",
        "utf8mb4_uca1400_romanian_nopad_ai_ci",
    ),
    c(
        260,
        "utf8mb4_sl_0900_ai_ci",
        "utf8mb4_uca1400_slovenian_nopad_ai_ci",
    ),
    c(
        261,
        "utf8mb4_pl_0900_ai_ci",
        "utf8mb4_uca1400_polish_nopad_ai_ci",
    ),
    c(
        262,
        "utf8mb4_et_0900_ai_ci",
        "utf8mb4_uca1400_estonian_nopad_ai_ci",
    ),
    c(
        263,
        "utf8mb4_es_0900_ai_ci",
        "utf8mb4_uca1400_spanish_nopad_ai_ci",
    ),
    c(
        264,
        "utf8mb4_sv_0900_ai_ci",
        "utf8mb4_uca1400_swedish_nopad_ai_ci",
    ),
    c(
        265,
        "utf8mb4_tr_0900_ai_ci",
        "utf8mb4_uca1400_turkish_nopad_ai_ci",
    ),
    c(
        266,
        "utf8mb4_cs_0900_ai_ci",
        "utf8mb4_uca1400_czech_nopad_ai_ci",
    ),
    c(
        267,
        "utf8mb4_da_0900_ai_ci",
        "utf8mb4_uca1400_danish_nopad_ai_ci",
    ),
    c(
        268,
        "utf8mb4_lt_0900_ai_ci",
        "utf8mb4_uca1400_lithuanian_nopad_ai_ci",
    ),
    c(
        269,
        "utf8mb4_sk_0900_ai_ci",
        "utf8mb4_uca1400_slovak_nopad_ai_ci",
    ),
    c(
        270,
        "utf8mb4_es_trad_0900_ai_ci",
        "utf8mb4_uca1400_spanish2_nopad_ai_ci",
    ),
    c(
        271,
        "utf8mb4_la_0900_ai_ci",
        "utf8mb4_uca1400_roman_nopad_ai_ci",
    ),
    c(
        273,
        "utf8mb4_eo_0900_ai_ci",
        "utf8mb4_uca1400_esperanto_nopad_ai_ci",
    ),
    c(
        274,
        "utf8mb4_hu_0900_ai_ci",
        "utf8mb4_uca1400_hungarian_nopad_ai_ci",
    ),
    c(
        275,
        "utf8mb4_hr_0900_ai_ci",
        "utf8mb4_uca1400_croatian_nopad_ai_ci",
    ),
    c(
        277,
        "utf8mb4_vi_0900_ai_ci",
        "utf8mb4_uca1400_vietnamese_nopad_ai_ci",
    ),
    c(278, "utf8mb4_0900_as_cs", "utf8mb4_uca1400_nopad_as_cs"),
    c(
        279,
        "utf8mb4_de_pb_0900_as_cs",
        "utf8mb4_uca1400_german2_nopad_as_cs",
    ),
    c(
        280,
        "utf8mb4_is_0900_as_cs",
        "utf8mb4_uca1400_icelandic_nopad_as_cs",
    ),
    c(
        281,
        "utf8mb4_lv_0900_as_cs",
        "utf8mb4_uca1400_latvian_nopad_as_cs",
    ),
    c(
        282,
        "utf8mb4_ro_0900_as_cs",
        "utf8mb4_uca1400_romanian_nopad_as_cs",
    ),
    c(
        283,
        "utf8mb4_sl_0900_as_cs",
        "utf8mb4_uca1400_slovenian_nopad_as_cs",
    ),
    c(
        284,
        "utf8mb4_pl_0900_as_cs",
        "utf8mb4_uca1400_polish_nopad_as_cs",
    ),
    c(
        285,
        "utf8mb4_et_0900_as_cs",
        "utf8mb4_uca1400_estonian_nopad_as_cs",
    ),
    c(
        286,
        "utf8mb4_es_0900_as_cs",
        "utf8mb4_uca1400_spanish_nopad_as_cs",
    ),
    c(
        287,
        "utf8mb4_sv_0900_as_cs",
        "utf8mb4_uca1400_swedish_nopad_as_cs",
    ),
    c(
        288,
        "utf8mb4_tr_0900_as_cs",
        "utf8mb4_uca1400_turkish_nopad_as_cs",
    ),
    c(
        289,
        "utf8mb4_cs_0900_as_cs",
        "utf8mb4_uca1400_czech_nopad_as_cs",
    ),
    c(
        290,
        "utf8mb4_da_0900_as_cs",
        "utf8mb4_uca1400_danish_nopad_as_cs",
    ),
    c(
        291,
        "utf8mb4_lt_0900_as_cs",
        "utf8mb4_uca1400_lithuanian_nopad_as_cs",
    ),
    c(
        292,
        "utf8mb4_sk_0900_as_cs",
        "utf8mb4_uca1400_slovak_nopad_as_cs",
    ),
    c(
        293,
        "utf8mb4_es_trad_0900_as_cs",
        "utf8mb4_uca1400_spanish2_nopad_as_cs",
    ),
    c(
        294,
        "utf8mb4_la_0900_as_cs",
        "utf8mb4_uca1400_roman_nopad_as_cs",
    ),
    c(
        296,
        "utf8mb4_eo_0900_as_cs",
        "utf8mb4_uca1400_esperanto_nopad_as_cs",
    ),
    c(
        297,
        "utf8mb4_hu_0900_as_cs",
        "utf8mb4_uca1400_hungarian_nopad_as_cs",
    ),
    c(
        298,
        "utf8mb4_hr_0900_as_cs",
        "utf8mb4_uca1400_croatian_nopad_as_cs",
    ),
    c(
        300,
        "utf8mb4_vi_0900_as_cs",
        "utf8mb4_uca1400_vietnamese_nopad_as_cs",
    ),
    missing(303, "utf8mb4_ja_0900_as_cs"),
    missing(304, "utf8mb4_ja_0900_as_cs_ks"),
    c(305, "utf8mb4_0900_as_ci", "utf8mb4_uca1400_nopad_as_ci"),
    missing(306, "utf8mb4_ru_0900_ai_ci"),
    missing(307, "utf8mb4_ru_0900_as_cs"),
    missing(308, "utf8mb4_zh_0900_as_cs"),
    c(309, "utf8mb4_0900_bin", "utf8mb4_nopad_bin"),
    missing(310, "utf8mb4_nb_0900_ai_ci"),
    missing(311, "utf8mb4_nb_0900_as_cs"),
    missing(312, "utf8mb4_nn_0900_ai_ci"),
    missing(313, "utf8mb4_nn_0900_as_cs"),
    missing(314, "utf8mb4_sr_latn_0900_ai_ci"),
    missing(315, "utf8mb4_sr_latn_0900_as_cs"),
    missing(316, "utf8mb4_bs_0900_ai_ci"),
    missing(317, "utf8mb4_bs_0900_as_cs"),
    missing(318, "utf8mb4_bg_0900_ai_ci"),
    missing(319, "utf8mb4_bg_0900_as_cs"),
    missing(320, "utf8mb4_gl_0900_ai_ci"),
    missing(321, "utf8mb4_gl_0900_as_cs"),
    missing(322, "utf8mb4_mn_cyrl_0900_ai_ci"),
    missing(323, "utf8mb4_mn_cyrl_0900_as_cs"),
];

/// MySQL's default `utf8mb4` collation.
pub const MYSQL_DEFAULT: &str = "utf8mb4_0900_ai_ci";

/// Every collation MySQL 8.4 has, by name (286, captured from MySQL 8.4.11
/// on 10 November 2026). On 3306, `information_schema.COLLATIONS` and
/// `COLLATION_CHARACTER_SET_APPLICABILITY` list only these: MySQL
/// Connector/J's `detectCustomCollations` fails to connect on a collation
/// name it doesn't know whose id is below 2048, and MariaDB has 17 such
/// (`utf8mb4_myanmar_ci`, `*_croatian_mysql561_ci`, ...; gaps row 54).
pub const MYSQL84_NAMES: &[&str] = &[
    "armscii8_bin",
    "armscii8_general_ci",
    "ascii_bin",
    "ascii_general_ci",
    "big5_bin",
    "big5_chinese_ci",
    "binary",
    "cp1250_bin",
    "cp1250_croatian_ci",
    "cp1250_czech_cs",
    "cp1250_general_ci",
    "cp1250_polish_ci",
    "cp1251_bin",
    "cp1251_bulgarian_ci",
    "cp1251_general_ci",
    "cp1251_general_cs",
    "cp1251_ukrainian_ci",
    "cp1256_bin",
    "cp1256_general_ci",
    "cp1257_bin",
    "cp1257_general_ci",
    "cp1257_lithuanian_ci",
    "cp850_bin",
    "cp850_general_ci",
    "cp852_bin",
    "cp852_general_ci",
    "cp866_bin",
    "cp866_general_ci",
    "cp932_bin",
    "cp932_japanese_ci",
    "dec8_bin",
    "dec8_swedish_ci",
    "eucjpms_bin",
    "eucjpms_japanese_ci",
    "euckr_bin",
    "euckr_korean_ci",
    "gb18030_bin",
    "gb18030_chinese_ci",
    "gb18030_unicode_520_ci",
    "gb2312_bin",
    "gb2312_chinese_ci",
    "gbk_bin",
    "gbk_chinese_ci",
    "geostd8_bin",
    "geostd8_general_ci",
    "greek_bin",
    "greek_general_ci",
    "hebrew_bin",
    "hebrew_general_ci",
    "hp8_bin",
    "hp8_english_ci",
    "keybcs2_bin",
    "keybcs2_general_ci",
    "koi8r_bin",
    "koi8r_general_ci",
    "koi8u_bin",
    "koi8u_general_ci",
    "latin1_bin",
    "latin1_danish_ci",
    "latin1_general_ci",
    "latin1_general_cs",
    "latin1_german1_ci",
    "latin1_german2_ci",
    "latin1_spanish_ci",
    "latin1_swedish_ci",
    "latin2_bin",
    "latin2_croatian_ci",
    "latin2_czech_cs",
    "latin2_general_ci",
    "latin2_hungarian_ci",
    "latin5_bin",
    "latin5_turkish_ci",
    "latin7_bin",
    "latin7_estonian_cs",
    "latin7_general_ci",
    "latin7_general_cs",
    "macce_bin",
    "macce_general_ci",
    "macroman_bin",
    "macroman_general_ci",
    "sjis_bin",
    "sjis_japanese_ci",
    "swe7_bin",
    "swe7_swedish_ci",
    "tis620_bin",
    "tis620_thai_ci",
    "ucs2_bin",
    "ucs2_croatian_ci",
    "ucs2_czech_ci",
    "ucs2_danish_ci",
    "ucs2_esperanto_ci",
    "ucs2_estonian_ci",
    "ucs2_general_ci",
    "ucs2_general_mysql500_ci",
    "ucs2_german2_ci",
    "ucs2_hungarian_ci",
    "ucs2_icelandic_ci",
    "ucs2_latvian_ci",
    "ucs2_lithuanian_ci",
    "ucs2_persian_ci",
    "ucs2_polish_ci",
    "ucs2_roman_ci",
    "ucs2_romanian_ci",
    "ucs2_sinhala_ci",
    "ucs2_slovak_ci",
    "ucs2_slovenian_ci",
    "ucs2_spanish2_ci",
    "ucs2_spanish_ci",
    "ucs2_swedish_ci",
    "ucs2_turkish_ci",
    "ucs2_unicode_520_ci",
    "ucs2_unicode_ci",
    "ucs2_vietnamese_ci",
    "ujis_bin",
    "ujis_japanese_ci",
    "utf16_bin",
    "utf16_croatian_ci",
    "utf16_czech_ci",
    "utf16_danish_ci",
    "utf16_esperanto_ci",
    "utf16_estonian_ci",
    "utf16_general_ci",
    "utf16_german2_ci",
    "utf16_hungarian_ci",
    "utf16_icelandic_ci",
    "utf16_latvian_ci",
    "utf16_lithuanian_ci",
    "utf16_persian_ci",
    "utf16_polish_ci",
    "utf16_roman_ci",
    "utf16_romanian_ci",
    "utf16_sinhala_ci",
    "utf16_slovak_ci",
    "utf16_slovenian_ci",
    "utf16_spanish2_ci",
    "utf16_spanish_ci",
    "utf16_swedish_ci",
    "utf16_turkish_ci",
    "utf16_unicode_520_ci",
    "utf16_unicode_ci",
    "utf16_vietnamese_ci",
    "utf16le_bin",
    "utf16le_general_ci",
    "utf32_bin",
    "utf32_croatian_ci",
    "utf32_czech_ci",
    "utf32_danish_ci",
    "utf32_esperanto_ci",
    "utf32_estonian_ci",
    "utf32_general_ci",
    "utf32_german2_ci",
    "utf32_hungarian_ci",
    "utf32_icelandic_ci",
    "utf32_latvian_ci",
    "utf32_lithuanian_ci",
    "utf32_persian_ci",
    "utf32_polish_ci",
    "utf32_roman_ci",
    "utf32_romanian_ci",
    "utf32_sinhala_ci",
    "utf32_slovak_ci",
    "utf32_slovenian_ci",
    "utf32_spanish2_ci",
    "utf32_spanish_ci",
    "utf32_swedish_ci",
    "utf32_turkish_ci",
    "utf32_unicode_520_ci",
    "utf32_unicode_ci",
    "utf32_vietnamese_ci",
    "utf8mb3_bin",
    "utf8mb3_croatian_ci",
    "utf8mb3_czech_ci",
    "utf8mb3_danish_ci",
    "utf8mb3_esperanto_ci",
    "utf8mb3_estonian_ci",
    "utf8mb3_general_ci",
    "utf8mb3_general_mysql500_ci",
    "utf8mb3_german2_ci",
    "utf8mb3_hungarian_ci",
    "utf8mb3_icelandic_ci",
    "utf8mb3_latvian_ci",
    "utf8mb3_lithuanian_ci",
    "utf8mb3_persian_ci",
    "utf8mb3_polish_ci",
    "utf8mb3_roman_ci",
    "utf8mb3_romanian_ci",
    "utf8mb3_sinhala_ci",
    "utf8mb3_slovak_ci",
    "utf8mb3_slovenian_ci",
    "utf8mb3_spanish2_ci",
    "utf8mb3_spanish_ci",
    "utf8mb3_swedish_ci",
    "utf8mb3_tolower_ci",
    "utf8mb3_turkish_ci",
    "utf8mb3_unicode_520_ci",
    "utf8mb3_unicode_ci",
    "utf8mb3_vietnamese_ci",
    "utf8mb4_0900_ai_ci",
    "utf8mb4_0900_as_ci",
    "utf8mb4_0900_as_cs",
    "utf8mb4_0900_bin",
    "utf8mb4_bg_0900_ai_ci",
    "utf8mb4_bg_0900_as_cs",
    "utf8mb4_bin",
    "utf8mb4_bs_0900_ai_ci",
    "utf8mb4_bs_0900_as_cs",
    "utf8mb4_croatian_ci",
    "utf8mb4_cs_0900_ai_ci",
    "utf8mb4_cs_0900_as_cs",
    "utf8mb4_czech_ci",
    "utf8mb4_da_0900_ai_ci",
    "utf8mb4_da_0900_as_cs",
    "utf8mb4_danish_ci",
    "utf8mb4_de_pb_0900_ai_ci",
    "utf8mb4_de_pb_0900_as_cs",
    "utf8mb4_eo_0900_ai_ci",
    "utf8mb4_eo_0900_as_cs",
    "utf8mb4_es_0900_ai_ci",
    "utf8mb4_es_0900_as_cs",
    "utf8mb4_es_trad_0900_ai_ci",
    "utf8mb4_es_trad_0900_as_cs",
    "utf8mb4_esperanto_ci",
    "utf8mb4_estonian_ci",
    "utf8mb4_et_0900_ai_ci",
    "utf8mb4_et_0900_as_cs",
    "utf8mb4_general_ci",
    "utf8mb4_german2_ci",
    "utf8mb4_gl_0900_ai_ci",
    "utf8mb4_gl_0900_as_cs",
    "utf8mb4_hr_0900_ai_ci",
    "utf8mb4_hr_0900_as_cs",
    "utf8mb4_hu_0900_ai_ci",
    "utf8mb4_hu_0900_as_cs",
    "utf8mb4_hungarian_ci",
    "utf8mb4_icelandic_ci",
    "utf8mb4_is_0900_ai_ci",
    "utf8mb4_is_0900_as_cs",
    "utf8mb4_ja_0900_as_cs",
    "utf8mb4_ja_0900_as_cs_ks",
    "utf8mb4_la_0900_ai_ci",
    "utf8mb4_la_0900_as_cs",
    "utf8mb4_latvian_ci",
    "utf8mb4_lithuanian_ci",
    "utf8mb4_lt_0900_ai_ci",
    "utf8mb4_lt_0900_as_cs",
    "utf8mb4_lv_0900_ai_ci",
    "utf8mb4_lv_0900_as_cs",
    "utf8mb4_mn_cyrl_0900_ai_ci",
    "utf8mb4_mn_cyrl_0900_as_cs",
    "utf8mb4_nb_0900_ai_ci",
    "utf8mb4_nb_0900_as_cs",
    "utf8mb4_nn_0900_ai_ci",
    "utf8mb4_nn_0900_as_cs",
    "utf8mb4_persian_ci",
    "utf8mb4_pl_0900_ai_ci",
    "utf8mb4_pl_0900_as_cs",
    "utf8mb4_polish_ci",
    "utf8mb4_ro_0900_ai_ci",
    "utf8mb4_ro_0900_as_cs",
    "utf8mb4_roman_ci",
    "utf8mb4_romanian_ci",
    "utf8mb4_ru_0900_ai_ci",
    "utf8mb4_ru_0900_as_cs",
    "utf8mb4_sinhala_ci",
    "utf8mb4_sk_0900_ai_ci",
    "utf8mb4_sk_0900_as_cs",
    "utf8mb4_sl_0900_ai_ci",
    "utf8mb4_sl_0900_as_cs",
    "utf8mb4_slovak_ci",
    "utf8mb4_slovenian_ci",
    "utf8mb4_spanish2_ci",
    "utf8mb4_spanish_ci",
    "utf8mb4_sr_latn_0900_ai_ci",
    "utf8mb4_sr_latn_0900_as_cs",
    "utf8mb4_sv_0900_ai_ci",
    "utf8mb4_sv_0900_as_cs",
    "utf8mb4_swedish_ci",
    "utf8mb4_tr_0900_ai_ci",
    "utf8mb4_tr_0900_as_cs",
    "utf8mb4_turkish_ci",
    "utf8mb4_unicode_520_ci",
    "utf8mb4_unicode_ci",
    "utf8mb4_vi_0900_ai_ci",
    "utf8mb4_vi_0900_as_cs",
    "utf8mb4_vietnamese_ci",
    "utf8mb4_zh_0900_as_cs",
];

/// The MariaDB name without its `_nopad`, so PAD SPACE and NO PAD UCA 14.0
/// collations compare equal.
fn without_nopad(name: &str) -> std::borrow::Cow<'_, str> {
    if name.contains("_nopad_") {
        std::borrow::Cow::Owned(name.replacen("_nopad_", "_", 1))
    } else {
        std::borrow::Cow::Borrowed(name)
    }
}

/// The name a MySQL 8.4 client sees for a collation MariaDB reports: the
/// MySQL 0900 collation it stands for, or `name` itself (already a MySQL
/// name, or one with no MySQL counterpart). Case-insensitive.
#[must_use]
pub fn mysql_name(name: &str) -> &str {
    let lower = name.to_ascii_lowercase();
    if !lower.starts_with("utf8mb4_") || lower.contains("0900") {
        return name;
    }
    if lower == "utf8mb4_nopad_bin" {
        return "utf8mb4_0900_bin";
    }
    if !lower.starts_with("utf8mb4_uca1400_") {
        return name;
    }
    let key = without_nopad(&lower);
    MAP.iter()
        .find(|c| c.mariadb.is_some_and(|m| without_nopad(m) == key))
        .map_or(name, |c| c.mysql)
}

/// The MariaDB collation a MySQL 0900 name stands for; `None` when MariaDB
/// lacks it or the name isn't a MySQL 0900 collation.
#[must_use]
pub fn mariadb_name(mysql: &str) -> Option<&'static str> {
    MAP.iter()
        .find(|c| c.mysql.eq_ignore_ascii_case(mysql))
        .and_then(|c| c.mariadb)
}

/// MySQL's metadata id for a MySQL collation name.
#[must_use]
pub fn mysql_id(mysql: &str) -> Option<u16> {
    MAP.iter()
        .find(|c| c.mysql.eq_ignore_ascii_case(mysql))
        .map(|c| c.id)
}

/// The MySQL name for a MySQL collation id among the 0900 collations.
#[must_use]
pub fn by_id(id: u16) -> Option<&'static Collation> {
    MAP.iter().find(|c| c.id == id)
}

/// The MariaDB UCA 14.0 names (PAD SPACE and NO PAD) that read as a MySQL
/// 0900 name on 3306, as `(mariadb, mysql)` pairs: the `CASE` arms of
/// [`sql_case`].
#[must_use]
pub fn output_pairs() -> Vec<(String, &'static str)> {
    let mut v = Vec::new();
    for c in MAP {
        if let Some(m) = c.mariadb {
            v.push((m.to_owned(), c.mysql));
            let pad = without_nopad(m);
            if pad != m {
                v.push((pad.into_owned(), c.mysql));
            }
        }
    }
    v
}

/// An SQL expression that maps the collation names in `expr` to their MySQL
/// names, for `information_schema` views on 3306. Only the names MariaDB's
/// defaults can produce are listed (the root UCA 14.0 collations and
/// `utf8mb4_nopad_bin`); a tailoring the client named comes back under
/// the MySQL alias already. `lit` quotes a string literal for the session.
#[must_use]
pub fn sql_case(expr: &str, lit: &dyn Fn(&str) -> String) -> String {
    let mut s = format!("CASE {expr}");
    for (m, my) in output_pairs()
        .iter()
        .filter(|(_, my)| my.starts_with("utf8mb4_0900_"))
    {
        let _ = write!(s, " WHEN {} THEN {}", lit(m), lit(my));
    }
    let _ = write!(s, " ELSE {expr} END");
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mariadb_names_read_as_mysql_ones() {
        assert_eq!(mysql_name("utf8mb4_uca1400_ai_ci"), "utf8mb4_0900_ai_ci");
        assert_eq!(
            mysql_name("utf8mb4_uca1400_nopad_ai_ci"),
            "utf8mb4_0900_ai_ci"
        );
        assert_eq!(mysql_name("UTF8MB4_UCA1400_AS_CS"), "utf8mb4_0900_as_cs");
        assert_eq!(
            mysql_name("utf8mb4_uca1400_nopad_as_ci"),
            "utf8mb4_0900_as_ci"
        );
        assert_eq!(mysql_name("utf8mb4_nopad_bin"), "utf8mb4_0900_bin");
        assert_eq!(
            mysql_name("utf8mb4_uca1400_german2_ai_ci"),
            "utf8mb4_de_pb_0900_ai_ci"
        );
        assert_eq!(
            mysql_name("utf8mb4_uca1400_spanish2_nopad_as_cs"),
            "utf8mb4_es_trad_0900_as_cs"
        );
        // Shared names, aliases and names without a counterpart pass.
        for same in [
            "utf8mb4_bin",
            "utf8mb4_general_ci",
            "utf8mb4_0900_ai_ci",
            "utf8mb4_unicode_ci",
            "utf8mb4_uca1400_ai_cs",
            "latin1_swedish_ci",
        ] {
            assert_eq!(mysql_name(same), same);
        }
    }

    #[test]
    fn mysql_names_map_to_mariadb_and_ids() {
        assert_eq!(
            mariadb_name("utf8mb4_0900_ai_ci"),
            Some("utf8mb4_uca1400_nopad_ai_ci")
        );
        assert_eq!(mariadb_name("utf8mb4_ja_0900_as_cs"), None);
        assert_eq!(mariadb_name("utf8mb4_bin"), None);
        assert_eq!(mysql_id("utf8mb4_0900_ai_ci"), Some(255));
        assert_eq!(mysql_id("utf8mb4_0900_bin"), Some(309));
        assert_eq!(by_id(305).map(|c| c.mysql), Some("utf8mb4_0900_as_ci"));
        // Ids are unique and ascending.
        assert!(MAP.windows(2).all(|w| w[0].id < w[1].id));
    }

    #[test]
    fn the_sql_case_lists_the_root_collations() {
        let s = sql_case("`COLLATION_NAME`", &|v| format!("'{v}'"));
        assert!(s.starts_with("CASE `COLLATION_NAME` WHEN"));
        assert!(s.contains("WHEN 'utf8mb4_uca1400_ai_ci' THEN 'utf8mb4_0900_ai_ci'"));
        assert!(s.contains("WHEN 'utf8mb4_nopad_bin' THEN 'utf8mb4_0900_bin'"));
        assert!(!s.contains("german2"));
        assert!(s.ends_with("ELSE `COLLATION_NAME` END"));
    }
}
