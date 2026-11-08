//! The collation table the ordered merge needs (chapter 17, "Ordered
//! merge"): per collation id, whether it is `PAD SPACE` and the weight of
//! one space at levels 1 to 3, as `WEIGHT_STRING(' ' COLLATE c LEVEL k)`
//! returns it.
//!
//! Captured on 8 November 2026 from MariaDB 11.8.9 for every collation of
//! `utf8mb4`, `utf8mb3`, `latin1`, `ascii` and `binary` (496 ids). The
//! `_0900_` names are aliases of `NO PAD` collations. A
//! level a collation lacks repeats its last level, as `WEIGHT_STRING`
//! does, so comparing all three levels is exact for one-level collations
//! too. `utf8mb4_uca1400_ai_cs` has no level 2 (empty weights). Chapter
//! 15's collation table in `sprig-shard` replaces this one when it lands.

use std::cmp::Ordering;

/// One collation: id, `PAD SPACE`, character set, space weight per level,
/// name.
type Entry = (u16, bool, &'static str, [&'static [u8]; 3], &'static str);

#[rustfmt::skip]
static TABLE: &[Entry] = &[
    (5, true, "latin1", [b"\x20", b"\x20", b"\x20"], "latin1_german1_ci"),
    (8, true, "latin1", [b"\x20", b"\x20", b"\x20"], "latin1_swedish_ci"),
    (11, true, "ascii", [b"\x20", b"\x20", b"\x20"], "ascii_general_ci"),
    (15, true, "latin1", [b"\x20", b"\x20", b"\x20"], "latin1_danish_ci"),
    (31, true, "latin1", [b"\x20", b"\x20", b"\x20"], "latin1_german2_ci"),
    (33, true, "utf8mb3", [b"\x00\x20", b"\x00\x20", b"\x00\x20"], "utf8mb3_general_ci"),
    (45, true, "utf8mb4", [b"\x00\x20", b"\x00\x20", b"\x00\x20"], "utf8mb4_general_ci"),
    (46, true, "utf8mb4", [b"\x00\x00\x20", b"\x00\x00\x20", b"\x00\x00\x20"], "utf8mb4_bin"),
    (47, true, "latin1", [b"\x20", b"\x20", b"\x20"], "latin1_bin"),
    (48, true, "latin1", [b"\x20", b"\x20", b"\x20"], "latin1_general_ci"),
    (49, true, "latin1", [b"\x20", b"\x20", b"\x20"], "latin1_general_cs"),
    (63, false, "binary", [b"\x20", b"\x20", b"\x20"], "binary"),
    (65, true, "ascii", [b"\x20", b"\x20", b"\x20"], "ascii_bin"),
    (83, true, "utf8mb3", [b"\x00\x20", b"\x00\x20", b"\x00\x20"], "utf8mb3_bin"),
    (94, true, "latin1", [b"\x20", b"\x20", b"\x20"], "latin1_spanish_ci"),
    (192, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_unicode_ci"),
    (193, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_icelandic_ci"),
    (194, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_latvian_ci"),
    (195, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_romanian_ci"),
    (196, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_slovenian_ci"),
    (197, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_polish_ci"),
    (198, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_estonian_ci"),
    (199, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_spanish_ci"),
    (200, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_swedish_ci"),
    (201, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_turkish_ci"),
    (202, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_czech_ci"),
    (203, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_danish_ci"),
    (204, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_lithuanian_ci"),
    (205, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_slovak_ci"),
    (206, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_spanish2_ci"),
    (207, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_roman_ci"),
    (208, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_persian_ci"),
    (209, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_esperanto_ci"),
    (210, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_hungarian_ci"),
    (211, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_sinhala_ci"),
    (212, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_german2_ci"),
    (213, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_croatian_mysql561_ci"),
    (214, true, "utf8mb3", [b"\x02\x0a", b"\x02\x0a", b"\x02\x0a"], "utf8mb3_unicode_520_ci"),
    (215, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_vietnamese_ci"),
    (223, true, "utf8mb3", [b"\x00\x20", b"\x00\x20", b"\x00\x20"], "utf8mb3_general_mysql500_ci"),
    (224, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_unicode_ci"),
    (225, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_icelandic_ci"),
    (226, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_latvian_ci"),
    (227, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_romanian_ci"),
    (228, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_slovenian_ci"),
    (229, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_polish_ci"),
    (230, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_estonian_ci"),
    (231, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_spanish_ci"),
    (232, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_swedish_ci"),
    (233, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_turkish_ci"),
    (234, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_czech_ci"),
    (235, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_danish_ci"),
    (236, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_lithuanian_ci"),
    (237, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_slovak_ci"),
    (238, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_spanish2_ci"),
    (239, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_roman_ci"),
    (240, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_persian_ci"),
    (241, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_esperanto_ci"),
    (242, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_hungarian_ci"),
    (243, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_sinhala_ci"),
    (244, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_german2_ci"),
    (245, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_croatian_mysql561_ci"),
    (246, true, "utf8mb4", [b"\x02\x0a", b"\x02\x0a", b"\x02\x0a"], "utf8mb4_unicode_520_ci"),
    (247, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_vietnamese_ci"),
    (255, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_0900_ai_ci"),
    (256, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_de_pb_0900_ai_ci"),
    (257, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_is_0900_ai_ci"),
    (258, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_lv_0900_ai_ci"),
    (259, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_ro_0900_ai_ci"),
    (260, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_sl_0900_ai_ci"),
    (261, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_pl_0900_ai_ci"),
    (262, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_et_0900_ai_ci"),
    (263, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_es_0900_ai_ci"),
    (264, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_sv_0900_ai_ci"),
    (265, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_tr_0900_ai_ci"),
    (266, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_cs_0900_ai_ci"),
    (267, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_da_0900_ai_ci"),
    (268, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_lt_0900_ai_ci"),
    (269, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_sk_0900_ai_ci"),
    (270, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_es_trad_0900_ai_ci"),
    (271, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_la_0900_ai_ci"),
    (273, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_eo_0900_ai_ci"),
    (274, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_hu_0900_ai_ci"),
    (275, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_hr_0900_ai_ci"),
    (277, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_vi_0900_ai_ci"),
    (278, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_0900_as_cs"),
    (279, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_de_pb_0900_as_cs"),
    (280, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_is_0900_as_cs"),
    (281, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_lv_0900_as_cs"),
    (282, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_ro_0900_as_cs"),
    (283, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_sl_0900_as_cs"),
    (284, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_pl_0900_as_cs"),
    (285, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_et_0900_as_cs"),
    (286, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_es_0900_as_cs"),
    (287, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_sv_0900_as_cs"),
    (288, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_tr_0900_as_cs"),
    (289, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_cs_0900_as_cs"),
    (290, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_da_0900_as_cs"),
    (291, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_lt_0900_as_cs"),
    (292, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_sk_0900_as_cs"),
    (293, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_es_trad_0900_as_cs"),
    (294, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_la_0900_as_cs"),
    (296, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_eo_0900_as_cs"),
    (297, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_hu_0900_as_cs"),
    (298, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_hr_0900_as_cs"),
    (300, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_vi_0900_as_cs"),
    (305, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_0900_as_ci"),
    (309, false, "utf8mb4", [b"\x00\x00\x20", b"\x00\x00\x20", b"\x00\x00\x20"], "utf8mb4_0900_bin"),
    (576, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_croatian_ci"),
    (577, true, "utf8mb3", [b"\x02\x0a", b"\x02\x0a", b"\x02\x0a"], "utf8mb3_myanmar_ci"),
    (578, true, "utf8mb3", [b"\x02\x0a", b"\x00\x20", b"\x00\x20"], "utf8mb3_thai_520_w2"),
    (579, true, "utf8mb3", [b"\x00\x20", b"\x00\x20", b"\x00\x20"], "utf8mb3_general1400_as_ci"),
    (608, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_croatian_ci"),
    (609, true, "utf8mb4", [b"\x02\x0a", b"\x02\x0a", b"\x02\x0a"], "utf8mb4_myanmar_ci"),
    (610, true, "utf8mb4", [b"\x02\x0a", b"\x00\x20", b"\x00\x20"], "utf8mb4_thai_520_w2"),
    (611, true, "utf8mb4", [b"\x00\x00\x20", b"\x00\x00\x20", b"\x00\x00\x20"], "utf8mb4_general1400_as_ci"),
    (1032, false, "latin1", [b"\x20", b"\x20", b"\x20"], "latin1_swedish_nopad_ci"),
    (1035, false, "ascii", [b"\x20", b"\x20", b"\x20"], "ascii_general_nopad_ci"),
    (1057, false, "utf8mb3", [b"\x00\x20", b"\x00\x20", b"\x00\x20"], "utf8mb3_general_nopad_ci"),
    (1069, false, "utf8mb4", [b"\x00\x20", b"\x00\x20", b"\x00\x20"], "utf8mb4_general_nopad_ci"),
    (1070, false, "utf8mb4", [b"\x00\x00\x20", b"\x00\x00\x20", b"\x00\x00\x20"], "utf8mb4_nopad_bin"),
    (1071, false, "latin1", [b"\x20", b"\x20", b"\x20"], "latin1_nopad_bin"),
    (1089, false, "ascii", [b"\x20", b"\x20", b"\x20"], "ascii_nopad_bin"),
    (1107, false, "utf8mb3", [b"\x00\x20", b"\x00\x20", b"\x00\x20"], "utf8mb3_nopad_bin"),
    (1216, false, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_unicode_nopad_ci"),
    (1238, false, "utf8mb3", [b"\x02\x0a", b"\x02\x0a", b"\x02\x0a"], "utf8mb3_unicode_520_nopad_ci"),
    (1248, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_unicode_nopad_ci"),
    (1270, false, "utf8mb4", [b"\x02\x0a", b"\x02\x0a", b"\x02\x0a"], "utf8mb4_unicode_520_nopad_ci"),
    (2048, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_ai_ci"),
    (2049, true, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_ai_cs"),
    (2050, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_as_ci"),
    (2051, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_as_cs"),
    (2052, false, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_nopad_ai_ci"),
    (2053, false, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_nopad_ai_cs"),
    (2054, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_nopad_as_ci"),
    (2055, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_nopad_as_cs"),
    (2056, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_icelandic_ai_ci"),
    (2057, true, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_icelandic_ai_cs"),
    (2058, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_icelandic_as_ci"),
    (2059, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_icelandic_as_cs"),
    (2060, false, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_icelandic_nopad_ai_ci"),
    (2061, false, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_icelandic_nopad_ai_cs"),
    (2062, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_icelandic_nopad_as_ci"),
    (2063, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_icelandic_nopad_as_cs"),
    (2064, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_latvian_ai_ci"),
    (2065, true, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_latvian_ai_cs"),
    (2066, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_latvian_as_ci"),
    (2067, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_latvian_as_cs"),
    (2068, false, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_latvian_nopad_ai_ci"),
    (2069, false, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_latvian_nopad_ai_cs"),
    (2070, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_latvian_nopad_as_ci"),
    (2071, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_latvian_nopad_as_cs"),
    (2072, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_romanian_ai_ci"),
    (2073, true, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_romanian_ai_cs"),
    (2074, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_romanian_as_ci"),
    (2075, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_romanian_as_cs"),
    (2076, false, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_romanian_nopad_ai_ci"),
    (2077, false, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_romanian_nopad_ai_cs"),
    (2078, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_romanian_nopad_as_ci"),
    (2079, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_romanian_nopad_as_cs"),
    (2080, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_slovenian_ai_ci"),
    (2081, true, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_slovenian_ai_cs"),
    (2082, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_slovenian_as_ci"),
    (2083, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_slovenian_as_cs"),
    (2084, false, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_slovenian_nopad_ai_ci"),
    (2085, false, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_slovenian_nopad_ai_cs"),
    (2086, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_slovenian_nopad_as_ci"),
    (2087, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_slovenian_nopad_as_cs"),
    (2088, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_polish_ai_ci"),
    (2089, true, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_polish_ai_cs"),
    (2090, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_polish_as_ci"),
    (2091, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_polish_as_cs"),
    (2092, false, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_polish_nopad_ai_ci"),
    (2093, false, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_polish_nopad_ai_cs"),
    (2094, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_polish_nopad_as_ci"),
    (2095, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_polish_nopad_as_cs"),
    (2096, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_estonian_ai_ci"),
    (2097, true, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_estonian_ai_cs"),
    (2098, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_estonian_as_ci"),
    (2099, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_estonian_as_cs"),
    (2100, false, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_estonian_nopad_ai_ci"),
    (2101, false, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_estonian_nopad_ai_cs"),
    (2102, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_estonian_nopad_as_ci"),
    (2103, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_estonian_nopad_as_cs"),
    (2104, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_spanish_ai_ci"),
    (2105, true, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_spanish_ai_cs"),
    (2106, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_spanish_as_ci"),
    (2107, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_spanish_as_cs"),
    (2108, false, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_spanish_nopad_ai_ci"),
    (2109, false, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_spanish_nopad_ai_cs"),
    (2110, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_spanish_nopad_as_ci"),
    (2111, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_spanish_nopad_as_cs"),
    (2112, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_swedish_ai_ci"),
    (2113, true, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_swedish_ai_cs"),
    (2114, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_swedish_as_ci"),
    (2115, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_swedish_as_cs"),
    (2116, false, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_swedish_nopad_ai_ci"),
    (2117, false, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_swedish_nopad_ai_cs"),
    (2118, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_swedish_nopad_as_ci"),
    (2119, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_swedish_nopad_as_cs"),
    (2120, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_turkish_ai_ci"),
    (2121, true, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_turkish_ai_cs"),
    (2122, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_turkish_as_ci"),
    (2123, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_turkish_as_cs"),
    (2124, false, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_turkish_nopad_ai_ci"),
    (2125, false, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_turkish_nopad_ai_cs"),
    (2126, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_turkish_nopad_as_ci"),
    (2127, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_turkish_nopad_as_cs"),
    (2128, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_czech_ai_ci"),
    (2129, true, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_czech_ai_cs"),
    (2130, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_czech_as_ci"),
    (2131, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_czech_as_cs"),
    (2132, false, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_czech_nopad_ai_ci"),
    (2133, false, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_czech_nopad_ai_cs"),
    (2134, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_czech_nopad_as_ci"),
    (2135, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_czech_nopad_as_cs"),
    (2136, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_danish_ai_ci"),
    (2137, true, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_danish_ai_cs"),
    (2138, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_danish_as_ci"),
    (2139, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_danish_as_cs"),
    (2140, false, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_danish_nopad_ai_ci"),
    (2141, false, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_danish_nopad_ai_cs"),
    (2142, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_danish_nopad_as_ci"),
    (2143, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_danish_nopad_as_cs"),
    (2144, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_lithuanian_ai_ci"),
    (2145, true, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_lithuanian_ai_cs"),
    (2146, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_lithuanian_as_ci"),
    (2147, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_lithuanian_as_cs"),
    (2148, false, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_lithuanian_nopad_ai_ci"),
    (2149, false, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_lithuanian_nopad_ai_cs"),
    (2150, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_lithuanian_nopad_as_ci"),
    (2151, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_lithuanian_nopad_as_cs"),
    (2152, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_slovak_ai_ci"),
    (2153, true, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_slovak_ai_cs"),
    (2154, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_slovak_as_ci"),
    (2155, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_slovak_as_cs"),
    (2156, false, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_slovak_nopad_ai_ci"),
    (2157, false, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_slovak_nopad_ai_cs"),
    (2158, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_slovak_nopad_as_ci"),
    (2159, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_slovak_nopad_as_cs"),
    (2160, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_spanish2_ai_ci"),
    (2161, true, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_spanish2_ai_cs"),
    (2162, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_spanish2_as_ci"),
    (2163, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_spanish2_as_cs"),
    (2164, false, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_spanish2_nopad_ai_ci"),
    (2165, false, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_spanish2_nopad_ai_cs"),
    (2166, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_spanish2_nopad_as_ci"),
    (2167, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_spanish2_nopad_as_cs"),
    (2168, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_roman_ai_ci"),
    (2169, true, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_roman_ai_cs"),
    (2170, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_roman_as_ci"),
    (2171, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_roman_as_cs"),
    (2172, false, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_roman_nopad_ai_ci"),
    (2173, false, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_roman_nopad_ai_cs"),
    (2174, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_roman_nopad_as_ci"),
    (2175, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_roman_nopad_as_cs"),
    (2176, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_persian_ai_ci"),
    (2177, true, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_persian_ai_cs"),
    (2178, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_persian_as_ci"),
    (2179, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_persian_as_cs"),
    (2180, false, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_persian_nopad_ai_ci"),
    (2181, false, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_persian_nopad_ai_cs"),
    (2182, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_persian_nopad_as_ci"),
    (2183, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_persian_nopad_as_cs"),
    (2184, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_esperanto_ai_ci"),
    (2185, true, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_esperanto_ai_cs"),
    (2186, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_esperanto_as_ci"),
    (2187, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_esperanto_as_cs"),
    (2188, false, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_esperanto_nopad_ai_ci"),
    (2189, false, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_esperanto_nopad_ai_cs"),
    (2190, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_esperanto_nopad_as_ci"),
    (2191, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_esperanto_nopad_as_cs"),
    (2192, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_hungarian_ai_ci"),
    (2193, true, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_hungarian_ai_cs"),
    (2194, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_hungarian_as_ci"),
    (2195, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_hungarian_as_cs"),
    (2196, false, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_hungarian_nopad_ai_ci"),
    (2197, false, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_hungarian_nopad_ai_cs"),
    (2198, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_hungarian_nopad_as_ci"),
    (2199, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_hungarian_nopad_as_cs"),
    (2200, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_sinhala_ai_ci"),
    (2201, true, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_sinhala_ai_cs"),
    (2202, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_sinhala_as_ci"),
    (2203, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_sinhala_as_cs"),
    (2204, false, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_sinhala_nopad_ai_ci"),
    (2205, false, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_sinhala_nopad_ai_cs"),
    (2206, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_sinhala_nopad_as_ci"),
    (2207, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_sinhala_nopad_as_cs"),
    (2208, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_german2_ai_ci"),
    (2209, true, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_german2_ai_cs"),
    (2210, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_german2_as_ci"),
    (2211, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_german2_as_cs"),
    (2212, false, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_german2_nopad_ai_ci"),
    (2213, false, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_german2_nopad_ai_cs"),
    (2214, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_german2_nopad_as_ci"),
    (2215, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_german2_nopad_as_cs"),
    (2232, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_vietnamese_ai_ci"),
    (2233, true, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_vietnamese_ai_cs"),
    (2234, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_vietnamese_as_ci"),
    (2235, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_vietnamese_as_cs"),
    (2236, false, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_vietnamese_nopad_ai_ci"),
    (2237, false, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_vietnamese_nopad_ai_cs"),
    (2238, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_vietnamese_nopad_as_ci"),
    (2239, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_vietnamese_nopad_as_cs"),
    (2240, true, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_croatian_ai_ci"),
    (2241, true, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_croatian_ai_cs"),
    (2242, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_croatian_as_ci"),
    (2243, true, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_croatian_as_cs"),
    (2244, false, "utf8mb3", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb3_uca1400_croatian_nopad_ai_ci"),
    (2245, false, "utf8mb3", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb3_uca1400_croatian_nopad_ai_cs"),
    (2246, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb3_uca1400_croatian_nopad_as_ci"),
    (2247, false, "utf8mb3", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb3_uca1400_croatian_nopad_as_cs"),
    (2304, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_ai_ci"),
    (2305, true, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_ai_cs"),
    (2306, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_as_ci"),
    (2307, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_as_cs"),
    (2308, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_nopad_ai_ci"),
    (2309, false, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_nopad_ai_cs"),
    (2310, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_nopad_as_ci"),
    (2311, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_nopad_as_cs"),
    (2312, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_icelandic_ai_ci"),
    (2313, true, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_icelandic_ai_cs"),
    (2314, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_icelandic_as_ci"),
    (2315, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_icelandic_as_cs"),
    (2316, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_icelandic_nopad_ai_ci"),
    (2317, false, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_icelandic_nopad_ai_cs"),
    (2318, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_icelandic_nopad_as_ci"),
    (2319, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_icelandic_nopad_as_cs"),
    (2320, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_latvian_ai_ci"),
    (2321, true, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_latvian_ai_cs"),
    (2322, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_latvian_as_ci"),
    (2323, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_latvian_as_cs"),
    (2324, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_latvian_nopad_ai_ci"),
    (2325, false, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_latvian_nopad_ai_cs"),
    (2326, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_latvian_nopad_as_ci"),
    (2327, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_latvian_nopad_as_cs"),
    (2328, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_romanian_ai_ci"),
    (2329, true, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_romanian_ai_cs"),
    (2330, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_romanian_as_ci"),
    (2331, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_romanian_as_cs"),
    (2332, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_romanian_nopad_ai_ci"),
    (2333, false, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_romanian_nopad_ai_cs"),
    (2334, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_romanian_nopad_as_ci"),
    (2335, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_romanian_nopad_as_cs"),
    (2336, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_slovenian_ai_ci"),
    (2337, true, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_slovenian_ai_cs"),
    (2338, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_slovenian_as_ci"),
    (2339, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_slovenian_as_cs"),
    (2340, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_slovenian_nopad_ai_ci"),
    (2341, false, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_slovenian_nopad_ai_cs"),
    (2342, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_slovenian_nopad_as_ci"),
    (2343, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_slovenian_nopad_as_cs"),
    (2344, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_polish_ai_ci"),
    (2345, true, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_polish_ai_cs"),
    (2346, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_polish_as_ci"),
    (2347, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_polish_as_cs"),
    (2348, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_polish_nopad_ai_ci"),
    (2349, false, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_polish_nopad_ai_cs"),
    (2350, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_polish_nopad_as_ci"),
    (2351, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_polish_nopad_as_cs"),
    (2352, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_estonian_ai_ci"),
    (2353, true, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_estonian_ai_cs"),
    (2354, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_estonian_as_ci"),
    (2355, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_estonian_as_cs"),
    (2356, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_estonian_nopad_ai_ci"),
    (2357, false, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_estonian_nopad_ai_cs"),
    (2358, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_estonian_nopad_as_ci"),
    (2359, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_estonian_nopad_as_cs"),
    (2360, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_spanish_ai_ci"),
    (2361, true, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_spanish_ai_cs"),
    (2362, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_spanish_as_ci"),
    (2363, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_spanish_as_cs"),
    (2364, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_spanish_nopad_ai_ci"),
    (2365, false, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_spanish_nopad_ai_cs"),
    (2366, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_spanish_nopad_as_ci"),
    (2367, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_spanish_nopad_as_cs"),
    (2368, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_swedish_ai_ci"),
    (2369, true, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_swedish_ai_cs"),
    (2370, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_swedish_as_ci"),
    (2371, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_swedish_as_cs"),
    (2372, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_swedish_nopad_ai_ci"),
    (2373, false, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_swedish_nopad_ai_cs"),
    (2374, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_swedish_nopad_as_ci"),
    (2375, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_swedish_nopad_as_cs"),
    (2376, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_turkish_ai_ci"),
    (2377, true, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_turkish_ai_cs"),
    (2378, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_turkish_as_ci"),
    (2379, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_turkish_as_cs"),
    (2380, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_turkish_nopad_ai_ci"),
    (2381, false, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_turkish_nopad_ai_cs"),
    (2382, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_turkish_nopad_as_ci"),
    (2383, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_turkish_nopad_as_cs"),
    (2384, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_czech_ai_ci"),
    (2385, true, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_czech_ai_cs"),
    (2386, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_czech_as_ci"),
    (2387, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_czech_as_cs"),
    (2388, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_czech_nopad_ai_ci"),
    (2389, false, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_czech_nopad_ai_cs"),
    (2390, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_czech_nopad_as_ci"),
    (2391, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_czech_nopad_as_cs"),
    (2392, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_danish_ai_ci"),
    (2393, true, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_danish_ai_cs"),
    (2394, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_danish_as_ci"),
    (2395, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_danish_as_cs"),
    (2396, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_danish_nopad_ai_ci"),
    (2397, false, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_danish_nopad_ai_cs"),
    (2398, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_danish_nopad_as_ci"),
    (2399, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_danish_nopad_as_cs"),
    (2400, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_lithuanian_ai_ci"),
    (2401, true, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_lithuanian_ai_cs"),
    (2402, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_lithuanian_as_ci"),
    (2403, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_lithuanian_as_cs"),
    (2404, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_lithuanian_nopad_ai_ci"),
    (2405, false, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_lithuanian_nopad_ai_cs"),
    (2406, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_lithuanian_nopad_as_ci"),
    (2407, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_lithuanian_nopad_as_cs"),
    (2408, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_slovak_ai_ci"),
    (2409, true, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_slovak_ai_cs"),
    (2410, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_slovak_as_ci"),
    (2411, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_slovak_as_cs"),
    (2412, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_slovak_nopad_ai_ci"),
    (2413, false, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_slovak_nopad_ai_cs"),
    (2414, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_slovak_nopad_as_ci"),
    (2415, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_slovak_nopad_as_cs"),
    (2416, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_spanish2_ai_ci"),
    (2417, true, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_spanish2_ai_cs"),
    (2418, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_spanish2_as_ci"),
    (2419, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_spanish2_as_cs"),
    (2420, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_spanish2_nopad_ai_ci"),
    (2421, false, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_spanish2_nopad_ai_cs"),
    (2422, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_spanish2_nopad_as_ci"),
    (2423, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_spanish2_nopad_as_cs"),
    (2424, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_roman_ai_ci"),
    (2425, true, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_roman_ai_cs"),
    (2426, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_roman_as_ci"),
    (2427, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_roman_as_cs"),
    (2428, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_roman_nopad_ai_ci"),
    (2429, false, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_roman_nopad_ai_cs"),
    (2430, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_roman_nopad_as_ci"),
    (2431, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_roman_nopad_as_cs"),
    (2432, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_persian_ai_ci"),
    (2433, true, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_persian_ai_cs"),
    (2434, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_persian_as_ci"),
    (2435, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_persian_as_cs"),
    (2436, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_persian_nopad_ai_ci"),
    (2437, false, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_persian_nopad_ai_cs"),
    (2438, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_persian_nopad_as_ci"),
    (2439, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_persian_nopad_as_cs"),
    (2440, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_esperanto_ai_ci"),
    (2441, true, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_esperanto_ai_cs"),
    (2442, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_esperanto_as_ci"),
    (2443, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_esperanto_as_cs"),
    (2444, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_esperanto_nopad_ai_ci"),
    (2445, false, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_esperanto_nopad_ai_cs"),
    (2446, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_esperanto_nopad_as_ci"),
    (2447, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_esperanto_nopad_as_cs"),
    (2448, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_hungarian_ai_ci"),
    (2449, true, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_hungarian_ai_cs"),
    (2450, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_hungarian_as_ci"),
    (2451, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_hungarian_as_cs"),
    (2452, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_hungarian_nopad_ai_ci"),
    (2453, false, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_hungarian_nopad_ai_cs"),
    (2454, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_hungarian_nopad_as_ci"),
    (2455, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_hungarian_nopad_as_cs"),
    (2456, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_sinhala_ai_ci"),
    (2457, true, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_sinhala_ai_cs"),
    (2458, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_sinhala_as_ci"),
    (2459, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_sinhala_as_cs"),
    (2460, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_sinhala_nopad_ai_ci"),
    (2461, false, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_sinhala_nopad_ai_cs"),
    (2462, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_sinhala_nopad_as_ci"),
    (2463, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_sinhala_nopad_as_cs"),
    (2464, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_german2_ai_ci"),
    (2465, true, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_german2_ai_cs"),
    (2466, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_german2_as_ci"),
    (2467, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_german2_as_cs"),
    (2468, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_german2_nopad_ai_ci"),
    (2469, false, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_german2_nopad_ai_cs"),
    (2470, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_german2_nopad_as_ci"),
    (2471, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_german2_nopad_as_cs"),
    (2488, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_vietnamese_ai_ci"),
    (2489, true, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_vietnamese_ai_cs"),
    (2490, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_vietnamese_as_ci"),
    (2491, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_vietnamese_as_cs"),
    (2492, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_vietnamese_nopad_ai_ci"),
    (2493, false, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_vietnamese_nopad_ai_cs"),
    (2494, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_vietnamese_nopad_as_ci"),
    (2495, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_vietnamese_nopad_as_cs"),
    (2496, true, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_croatian_ai_ci"),
    (2497, true, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_croatian_ai_cs"),
    (2498, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_croatian_as_ci"),
    (2499, true, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_croatian_as_cs"),
    (2500, false, "utf8mb4", [b"\x02\x09", b"\x02\x09", b"\x02\x09"], "utf8mb4_uca1400_croatian_nopad_ai_ci"),
    (2501, false, "utf8mb4", [b"\x02\x09", b"", b"\x00\x02"], "utf8mb4_uca1400_croatian_nopad_ai_cs"),
    (2502, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x20"], "utf8mb4_uca1400_croatian_nopad_as_ci"),
    (2503, false, "utf8mb4", [b"\x02\x09", b"\x00\x20", b"\x00\x02"], "utf8mb4_uca1400_croatian_nopad_as_cs"),
];

/// What the merge needs of a collation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CollationInfo {
    /// Trailing spaces don't count.
    pub pad_space: bool,
    /// The character set (`utf8mb4`, `latin1`, ...).
    pub charset: &'static str,
    /// The space weight at levels 1, 2 and 3.
    pub space: [&'static [u8]; 3],
}

/// The collation with this id, if the table has it.
#[must_use]
pub fn info(id: u16) -> Option<CollationInfo> {
    TABLE
        .binary_search_by_key(&id, |e| e.0)
        .ok()
        .map(|i| CollationInfo {
            pad_space: TABLE[i].1,
            charset: TABLE[i].2,
            space: TABLE[i].3,
        })
}

/// The id of the collation named `name` (as `COLLATION(e)` returns it).
#[must_use]
pub fn id_of(name: &[u8]) -> Option<u16> {
    TABLE
        .iter()
        .find(|e| e.4.as_bytes().eq_ignore_ascii_case(name))
        .map(|e| e.0)
}

/// Compares two weight strings of one level, with the `PAD SPACE` rule:
/// when one is a prefix of the other, the rest of the longer one is
/// compared, unit by unit, with the space weight.
#[must_use]
pub fn cmp_level(a: &[u8], b: &[u8], pad_space: bool, space: &[u8]) -> Ordering {
    let n = a.len().min(b.len());
    match a[..n].cmp(&b[..n]) {
        Ordering::Equal => {}
        o => return o,
    }
    if a.len() == b.len() {
        return Ordering::Equal;
    }
    if !pad_space || space.is_empty() {
        return a.len().cmp(&b.len());
    }
    let (rest, flip) = if a.len() > b.len() {
        (&a[n..], false)
    } else {
        (&b[n..], true)
    };
    let o = cmp_with_spaces(rest, space);
    if flip { o.reverse() } else { o }
}

/// `rest` against as many space weights as it is long.
fn cmp_with_spaces(rest: &[u8], space: &[u8]) -> Ordering {
    let mut i = 0;
    while i < rest.len() {
        let end = (i + space.len()).min(rest.len());
        let chunk = &rest[i..end];
        match chunk.cmp(&space[..chunk.len()]) {
            Ordering::Equal if chunk.len() < space.len() => return Ordering::Less,
            Ordering::Equal => {}
            o => return o,
        }
        i = end;
    }
    Ordering::Equal
}

/// A level's weights with trailing space weights removed (`PAD SPACE`),
/// for equality and hashing (chapter 15's weight-level trim).
#[must_use]
pub fn trim_level<'a>(w: &'a [u8], pad_space: bool, space: &[u8]) -> &'a [u8] {
    if !pad_space || space.is_empty() {
        return w;
    }
    let mut end = w.len();
    while end >= space.len() && &w[end - space.len()..end] == space {
        end -= space.len();
    }
    &w[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_is_sorted_and_has_the_common_collations() {
        assert!(TABLE.windows(2).all(|w| w[0].0 < w[1].0));
        let general = info(45).unwrap_or_else(|| panic!("utf8mb4_general_ci"));
        assert!(general.pad_space);
        assert_eq!(general.space[0], b"\x00\x20");
        let uca = info(2307).unwrap_or_else(|| panic!("utf8mb4_uca1400_as_cs"));
        assert_eq!(uca.space, [&b"\x02\x09"[..], b"\x00\x20", b"\x00\x02"]);
        assert!(!info(255).unwrap_or_else(|| panic!("0900")).pad_space);
        assert_eq!(id_of(b"utf8mb4_general_ci"), Some(45));
        assert_eq!(id_of(b"UTF8MB4_UCA1400_AI_CI"), Some(2304));
    }

    #[test]
    fn pad_space_rule() {
        let sp = b"\x00\x20";
        // 'a' vs 'a ' equal; 'a' vs 'a\t' : tab (0009) < space.
        assert_eq!(
            cmp_level(b"\x00\x41", b"\x00\x41\x00\x20", true, sp),
            Ordering::Equal
        );
        assert_eq!(
            cmp_level(b"\x00\x41", b"\x00\x41\x00\x09", true, sp),
            Ordering::Greater
        );
        assert_eq!(
            cmp_level(b"\x00\x41", b"\x00\x41\x00\x09", false, sp),
            Ordering::Less
        );
        assert_eq!(
            trim_level(b"\x00\x41\x00\x20\x00\x20", true, sp),
            b"\x00\x41"
        );
    }
}
