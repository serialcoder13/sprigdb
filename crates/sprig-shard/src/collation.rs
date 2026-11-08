//! Collation weights in Rust (chapter 15, "Strings: weight strings in
//! Rust, not from MariaDB").
//!
//! A string key is hashed by its weight string under the group's
//! collation, so values MariaDB calls equal land on one shard. The weights
//! are computed here, not with `SELECT WEIGHT_STRING()`, from tables
//! generated out of MariaDB itself (`tools/gen_collations.py`): every code
//! point's weights at every level, plus every contraction of the UCA 14.0.0
//! DUCET, which MariaDB's `uca1400` collations apply (its UCA 4.0.0 and
//! 5.2.0 collations don't). [`Collation::weight_string`] reproduces
//! `WEIGHT_STRING(s LEVEL k)` byte for byte; the conformance test checks it
//! on generated strings against a live MariaDB.
//!
//! Hashing ([`Collation::hash_key`]) uses the weight-level rule of the
//! weight-string spike: under `PAD SPACE`, trailing weights equal to the
//! level's space weight are removed from each level; under `NO PAD`, from
//! every level but the first (MariaDB's comparator ignores them there too).
//! Levels are joined by a
//! zero weight, which no level contains, so the encoding stays injective.
//!
//! The supported set is chapter 15's: `binary`; the `_bin` collations of
//! `utf8mb4`, `utf8mb3`, `latin1` and `ascii`; `utf8mb4_general_ci`,
//! `utf8mb3_general_ci`, `latin1_swedish_ci`, `utf8mb4_unicode_ci`,
//! `utf8mb4_unicode_520_ci`; the `utf8mb4_uca1400_*` and
//! `utf8mb3_uca1400_*` families; each with its `NO PAD` twin; and the
//! `utf8mb4_0900_*` names, which MariaDB 11.8 and 12.3 alias to
//! `utf8mb4_uca1400_nopad_*`.

use std::collections::HashMap;
use std::io::Read;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};
use sprig_common::{Result, SprigCode, SprigError};

/// MariaDB's collation id (`information_schema.COLLATIONS.ID`, or the
/// full id of `COLLATION_CHARACTER_SET_APPLICABILITY` for `uca1400`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CollationId(pub u16);

/// A character set a key string can arrive in or be compared under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Charset {
    /// Bytes, no character set.
    Binary,
    /// 7-bit US ASCII.
    Ascii,
    /// MariaDB's `latin1`, which is Windows-1252.
    Latin1,
    /// UTF-8 limited to the BMP.
    Utf8mb3,
    /// UTF-8.
    Utf8mb4,
}

impl Charset {
    /// The charset of a MariaDB character-set name (`utf8` is `utf8mb3`).
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name.to_ascii_lowercase().as_str() {
            "binary" => Self::Binary,
            "ascii" => Self::Ascii,
            "latin1" => Self::Latin1,
            "utf8" | "utf8mb3" => Self::Utf8mb3,
            "utf8mb4" => Self::Utf8mb4,
            _ => return None,
        })
    }
}

/// The pad attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Pad {
    /// Trailing spaces don't count.
    Space,
    /// Every character counts.
    NoPad,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// Weights from a generated table, at these physical levels.
    Table(TableId, &'static [u8]),
    /// The code point, big-endian, in this many bytes.
    CodePoint(u8),
    /// The byte itself.
    Byte,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TableId {
    Uca1400 = 0,
    Unicode = 1,
    Unicode520 = 2,
    General = 3,
    Latin1Swedish = 4,
}

const TABLE_NAMES: [&str; 5] = [
    "uca1400",
    "unicode_ci",
    "unicode_520_ci",
    "general_ci",
    "latin1_swedish_ci",
];

/// One supported collation.
#[derive(Debug)]
pub struct Collation {
    /// MariaDB's id.
    pub id: CollationId,
    /// MariaDB's name.
    pub name: &'static str,
    /// The character set it compares.
    pub charset: Charset,
    /// Its pad attribute.
    pub pad: Pad,
    kind: Kind,
}

const AI_CI: &[u8] = &[1];
const AI_CS: &[u8] = &[1, 3];
const AS_CI: &[u8] = &[1, 2];
const AS_CS: &[u8] = &[1, 2, 3];
const L1: &[u8] = &[1];

macro_rules! coll {
    ($id:literal, $name:literal, $cs:ident, $pad:ident, $kind:expr) => {
        Collation {
            id: CollationId($id),
            name: $name,
            charset: Charset::$cs,
            pad: Pad::$pad,
            kind: $kind,
        }
    };
}

use Kind::{Byte, CodePoint, Table};
use TableId::{General, Latin1Swedish, Uca1400, Unicode, Unicode520};

/// The supported set (chapter 15).
static COLLATIONS: &[Collation] = &[
    coll!(
        8,
        "latin1_swedish_ci",
        Latin1,
        Space,
        Table(Latin1Swedish, L1)
    ),
    coll!(
        1032,
        "latin1_swedish_nopad_ci",
        Latin1,
        NoPad,
        Table(Latin1Swedish, L1)
    ),
    coll!(47, "latin1_bin", Latin1, Space, Byte),
    coll!(1071, "latin1_nopad_bin", Latin1, NoPad, Byte),
    coll!(65, "ascii_bin", Ascii, Space, Byte),
    coll!(1089, "ascii_nopad_bin", Ascii, NoPad, Byte),
    coll!(63, "binary", Binary, NoPad, Byte),
    coll!(33, "utf8mb3_general_ci", Utf8mb3, Space, Table(General, L1)),
    coll!(
        1057,
        "utf8mb3_general_nopad_ci",
        Utf8mb3,
        NoPad,
        Table(General, L1)
    ),
    coll!(83, "utf8mb3_bin", Utf8mb3, Space, CodePoint(2)),
    coll!(1107, "utf8mb3_nopad_bin", Utf8mb3, NoPad, CodePoint(2)),
    coll!(45, "utf8mb4_general_ci", Utf8mb4, Space, Table(General, L1)),
    coll!(
        1069,
        "utf8mb4_general_nopad_ci",
        Utf8mb4,
        NoPad,
        Table(General, L1)
    ),
    coll!(46, "utf8mb4_bin", Utf8mb4, Space, CodePoint(3)),
    coll!(1070, "utf8mb4_nopad_bin", Utf8mb4, NoPad, CodePoint(3)),
    coll!(
        224,
        "utf8mb4_unicode_ci",
        Utf8mb4,
        Space,
        Table(Unicode, L1)
    ),
    coll!(
        1248,
        "utf8mb4_unicode_nopad_ci",
        Utf8mb4,
        NoPad,
        Table(Unicode, L1)
    ),
    coll!(
        246,
        "utf8mb4_unicode_520_ci",
        Utf8mb4,
        Space,
        Table(Unicode520, L1)
    ),
    coll!(
        1270,
        "utf8mb4_unicode_520_nopad_ci",
        Utf8mb4,
        NoPad,
        Table(Unicode520, L1)
    ),
    coll!(
        2304,
        "utf8mb4_uca1400_ai_ci",
        Utf8mb4,
        Space,
        Table(Uca1400, AI_CI)
    ),
    coll!(
        2305,
        "utf8mb4_uca1400_ai_cs",
        Utf8mb4,
        Space,
        Table(Uca1400, AI_CS)
    ),
    coll!(
        2306,
        "utf8mb4_uca1400_as_ci",
        Utf8mb4,
        Space,
        Table(Uca1400, AS_CI)
    ),
    coll!(
        2307,
        "utf8mb4_uca1400_as_cs",
        Utf8mb4,
        Space,
        Table(Uca1400, AS_CS)
    ),
    coll!(
        2308,
        "utf8mb4_uca1400_nopad_ai_ci",
        Utf8mb4,
        NoPad,
        Table(Uca1400, AI_CI)
    ),
    coll!(
        2309,
        "utf8mb4_uca1400_nopad_ai_cs",
        Utf8mb4,
        NoPad,
        Table(Uca1400, AI_CS)
    ),
    coll!(
        2310,
        "utf8mb4_uca1400_nopad_as_ci",
        Utf8mb4,
        NoPad,
        Table(Uca1400, AS_CI)
    ),
    coll!(
        2311,
        "utf8mb4_uca1400_nopad_as_cs",
        Utf8mb4,
        NoPad,
        Table(Uca1400, AS_CS)
    ),
    coll!(
        2048,
        "utf8mb3_uca1400_ai_ci",
        Utf8mb3,
        Space,
        Table(Uca1400, AI_CI)
    ),
    coll!(
        2049,
        "utf8mb3_uca1400_ai_cs",
        Utf8mb3,
        Space,
        Table(Uca1400, AI_CS)
    ),
    coll!(
        2050,
        "utf8mb3_uca1400_as_ci",
        Utf8mb3,
        Space,
        Table(Uca1400, AS_CI)
    ),
    coll!(
        2051,
        "utf8mb3_uca1400_as_cs",
        Utf8mb3,
        Space,
        Table(Uca1400, AS_CS)
    ),
    coll!(
        2052,
        "utf8mb3_uca1400_nopad_ai_ci",
        Utf8mb3,
        NoPad,
        Table(Uca1400, AI_CI)
    ),
    coll!(
        2053,
        "utf8mb3_uca1400_nopad_ai_cs",
        Utf8mb3,
        NoPad,
        Table(Uca1400, AI_CS)
    ),
    coll!(
        2054,
        "utf8mb3_uca1400_nopad_as_ci",
        Utf8mb3,
        NoPad,
        Table(Uca1400, AS_CI)
    ),
    coll!(
        2055,
        "utf8mb3_uca1400_nopad_as_cs",
        Utf8mb3,
        NoPad,
        Table(Uca1400, AS_CS)
    ),
];

/// MySQL 8 names MariaDB 11.8 and 12.3 accept as aliases (ids 255, 278,
/// 305), mapped to their `uca1400` targets.
static ALIASES: &[(u16, &str, u16)] = &[
    (255, "utf8mb4_0900_ai_ci", 2308),
    (278, "utf8mb4_0900_as_cs", 2311),
    (305, "utf8mb4_0900_as_ci", 2310),
];

/// The collation with MariaDB id `id`, if supported. Alias ids resolve
/// to their target.
#[must_use]
pub fn collation(id: CollationId) -> Option<&'static Collation> {
    let id = ALIASES.iter().find(|a| a.0 == id.0).map_or(id.0, |a| a.2);
    COLLATIONS.iter().find(|c| c.id.0 == id)
}

/// The collation named `name` (case-insensitive), if supported. Alias
/// names resolve to their target. `utf8_*` means `utf8mb3_*`.
#[must_use]
pub fn collation_by_name(name: &str) -> Option<&'static Collation> {
    let lower = name.to_ascii_lowercase();
    let lower = match lower.strip_prefix("utf8_") {
        Some(rest) => format!("utf8mb3_{rest}"),
        None => lower,
    };
    if let Some(a) = ALIASES.iter().find(|a| a.1 == lower) {
        return collation(CollationId(a.2));
    }
    COLLATIONS.iter().find(|c| c.name == lower)
}

/// The supported collation `id`, or 39401.
///
/// # Errors
/// 39401 `ER_SPRIG_UNSUPPORTED_KEY_COLLATION`.
pub fn require(id: CollationId) -> Result<&'static Collation> {
    collation(id).ok_or_else(|| unsupported(&format!("collation id {}", id.0)))
}

/// The supported collation `name`, or 39401.
///
/// # Errors
/// 39401 `ER_SPRIG_UNSUPPORTED_KEY_COLLATION`.
pub fn require_name(name: &str) -> Result<&'static Collation> {
    collation_by_name(name).ok_or_else(|| unsupported(name))
}

/// Every supported collation (aliases excluded).
#[must_use]
pub fn all() -> &'static [Collation] {
    COLLATIONS
}

fn unsupported(what: &str) -> SprigError {
    SprigError::new(
        SprigCode::UnsupportedKeyCollation,
        format!("{what} is not a supported collation for a distribution key"),
    )
}

fn unroutable(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::KeyValueUnroutable, msg)
}

impl Collation {
    /// The number of levels its weight string has: 1 to 3.
    #[must_use]
    pub fn levels(&self) -> usize {
        self.level_numbers().len()
    }

    /// The physical UCA levels it uses, as `WEIGHT_STRING(s LEVEL k)`
    /// names them: `[1]`, `[1, 2]`, `[1, 3]` (`_ai_cs`) or `[1, 2, 3]`.
    #[must_use]
    pub fn level_numbers(&self) -> &'static [u8] {
        match self.kind {
            Table(_, l) => l,
            CodePoint(_) | Byte => L1,
        }
    }

    /// Bytes per weight: 1, 2 or 3.
    #[must_use]
    pub fn weight_width(&self) -> usize {
        match self.kind {
            Table(Latin1Swedish, _) | Byte => 1,
            Table(..) => 2,
            CodePoint(w) => usize::from(w),
        }
    }

    /// The space's weight at level index `level` (0-based in
    /// [`level_numbers`](Self::level_numbers)); what `PAD SPACE` pads with.
    /// Empty for `binary`, which doesn't pad.
    #[must_use]
    pub fn space_weight(&self, level: usize) -> &'static [u8] {
        static SPACES: OnceLock<HashMap<(u16, usize), &'static [u8]>> = OnceLock::new();
        if self.charset == Charset::Binary {
            return &[];
        }
        let map = SPACES.get_or_init(|| {
            let mut m = HashMap::new();
            for c in COLLATIONS {
                if c.charset == Charset::Binary {
                    continue;
                }
                for l in 0..c.levels() {
                    let mut w = Vec::new();
                    c.unit_weights(0x20, l, &mut w);
                    m.insert((c.id.0, l), &*Box::leak(w.into_boxed_slice()));
                }
            }
            m
        });
        map.get(&(self.id.0, level)).copied().unwrap_or(&[])
    }

    /// Whether this is a `_bin` collation or `binary`: equality is
    /// equality of the stored characters (after `PAD SPACE`, if any).
    #[must_use]
    pub fn is_binary(&self) -> bool {
        matches!(self.kind, CodePoint(_) | Byte)
    }

    /// MariaDB's `WEIGHT_STRING(s)` (all levels, concatenated) or
    /// `WEIGHT_STRING(s LEVEL k)` for one physical level `k` of
    /// [`level_numbers`](Self::level_numbers), for `s` given in `from`.
    ///
    /// # Errors
    /// 39405 if `s` isn't valid in `from` or can't be converted to the
    /// collation's character set; 39401 for a level the collation lacks.
    pub fn weight_string(&self, s: &[u8], from: Charset, level: Option<u8>) -> Result<Vec<u8>> {
        let units = self.units(s, from)?;
        let mut out = Vec::with_capacity(units.len() * self.weight_width() * 2);
        match level {
            Some(k) => {
                let i = self
                    .level_numbers()
                    .iter()
                    .position(|&n| n == k)
                    .ok_or_else(|| unsupported(&format!("{} LEVEL {k}", self.name)))?;
                self.level_weights(&units, i, &mut out);
            }
            None => {
                for i in 0..self.levels() {
                    self.level_weights(&units, i, &mut out);
                }
            }
        }
        Ok(out)
    }

    /// Appends the canonical bytes of `s` (given in `from`) to `out`: each
    /// level's weights, trailing space weights removed (under `PAD SPACE`
    /// from every level, under `NO PAD` from levels after the first),
    /// levels separated by one zero weight. Values equal under the
    /// collation give equal bytes, and different values different bytes.
    ///
    /// # Errors
    /// 39405 if `s` isn't valid in `from` or can't be converted to the
    /// collation's character set (the router then scatters).
    pub fn hash_key(&self, s: &[u8], from: Charset, out: &mut Vec<u8>) -> Result<()> {
        let units = self.units(s, from)?;
        let mut level = Vec::with_capacity(units.len() * self.weight_width());
        let mut parts: Vec<Vec<u8>> = Vec::with_capacity(self.levels());
        for i in 0..self.levels() {
            level.clear();
            self.level_weights(&units, i, &mut level);
            parts.push(level.clone());
        }
        let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
        self.canonical_from_levels(&refs, out);
        Ok(())
    }

    /// The canonical bytes [`hash_key`](Self::hash_key) produces, built
    /// from MariaDB's own per-level weight strings
    /// (`WEIGHT_STRING(e LEVEL k)` for each of
    /// [`level_numbers`](Self::level_numbers)). The router uses it for
    /// hidden weight columns.
    pub fn canonical_from_levels(&self, levels: &[&[u8]], out: &mut Vec<u8>) {
        let w = self.weight_width();
        for (i, l) in levels.iter().enumerate() {
            if i > 0 {
                out.extend(std::iter::repeat_n(0u8, w));
            }
            let mut end = l.len() - l.len() % w;
            // Levels after the first trim under NO PAD too: MariaDB's
            // comparator ignores trailing space-level weights there (under
            // `_nopad_ai_cs`, 'a' = 'a' + U+0302 although their level-3
            // weight strings differ by a trailing 0002; found by the
            // conformance test).
            if self.pad == Pad::Space || i > 0 {
                let space = self.space_weight(i);
                while end >= w && !space.is_empty() && &l[end - w..end] == space {
                    end -= w;
                }
            }
            out.extend_from_slice(&l[..end]);
        }
    }

    /// Converts `s` from `from` into this collation's units: code points
    /// for UTF-8 collations, bytes for the single-byte ones.
    fn units(&self, s: &[u8], from: Charset) -> Result<Vec<u32>> {
        // A binary string compared with a text column is read as text in
        // the column's character set, as MariaDB converts it.
        let from = if from == Charset::Binary {
            self.charset
        } else {
            from
        };
        let cps = decode(s, from)?;
        match self.charset {
            // Bytes as given, whatever their character set.
            Charset::Binary => Ok(s.iter().map(|&b| u32::from(b)).collect()),
            Charset::Ascii => cps
                .into_iter()
                .map(|c| {
                    if c < 0x80 {
                        Ok(c)
                    } else {
                        Err(unroutable(format!("U+{c:04X} is not in ascii")))
                    }
                })
                .collect(),
            Charset::Latin1 => cps
                .into_iter()
                .map(|c| {
                    latin1_byte(c)
                        .map(u32::from)
                        .ok_or_else(|| unroutable(format!("U+{c:04X} is not in latin1")))
                })
                .collect(),
            Charset::Utf8mb3 => cps
                .into_iter()
                .map(|c| {
                    if c <= 0xFFFF {
                        Ok(c)
                    } else {
                        Err(unroutable(format!("U+{c:04X} is not in utf8mb3")))
                    }
                })
                .collect(),
            Charset::Utf8mb4 => Ok(cps),
        }
    }

    /// Appends the weights of `units` at level index `level`.
    fn level_weights(&self, units: &[u32], level: usize, out: &mut Vec<u8>) {
        match self.kind {
            Table(t, levels) => {
                let tables = tables();
                let table = &tables.tables[t as usize];
                let phys = usize::from(levels[level]) - 1;
                let width = self.weight_width();
                let mut i = 0;
                while i < units.len() {
                    if let Some((n, ws)) = table.contraction(&units[i..], phys) {
                        push_weights(ws.iter().copied(), width, out);
                        i += n;
                        continue;
                    }
                    table.levels[phys].push(units[i], width, out);
                    i += 1;
                }
            }
            CodePoint(w) => {
                for &u in units {
                    out.extend_from_slice(&u.to_be_bytes()[4 - usize::from(w)..]);
                }
            }
            #[allow(clippy::cast_possible_truncation)]
            Byte => out.extend(units.iter().map(|&u| u as u8)),
        }
    }

    /// The weights of one unit at level index `level` (no contractions).
    fn unit_weights(&self, unit: u32, level: usize, out: &mut Vec<u8>) {
        self.level_weights(&[unit], level, out);
    }
}

fn push_weights(ws: impl Iterator<Item = u16>, width: usize, out: &mut Vec<u8>) {
    for w in ws {
        if width == 1 {
            #[allow(clippy::cast_possible_truncation)]
            out.push(w as u8);
        } else {
            out.extend_from_slice(&w.to_be_bytes());
        }
    }
}

/// Decodes `s` in `cs` into code points (Latin-1 bytes through
/// Windows-1252, as MariaDB's `latin1`).
fn decode(s: &[u8], cs: Charset) -> Result<Vec<u32>> {
    match cs {
        Charset::Binary => Ok(s.iter().map(|&b| u32::from(b)).collect()),
        Charset::Ascii => s
            .iter()
            .map(|&b| {
                if b < 0x80 {
                    Ok(u32::from(b))
                } else {
                    Err(unroutable("a byte above 0x7F in an ascii string"))
                }
            })
            .collect(),
        Charset::Latin1 => Ok(s.iter().map(|&b| tables().latin1[usize::from(b)]).collect()),
        Charset::Utf8mb3 | Charset::Utf8mb4 => {
            let t = std::str::from_utf8(s).map_err(|_| unroutable("invalid UTF-8 in a key"))?;
            let cps: Vec<u32> = t.chars().map(u32::from).collect();
            if cs == Charset::Utf8mb3 && cps.iter().any(|&c| c > 0xFFFF) {
                return Err(unroutable("a 4-byte character in a utf8mb3 string"));
            }
            Ok(cps)
        }
    }
}

/// `s` (in `cs`) as UTF-8: how off-class strings are hashed.
///
/// # Errors
/// 39405 if `s` isn't valid in `cs`.
pub(crate) fn to_utf8(s: &[u8], cs: Charset) -> Result<std::borrow::Cow<'_, [u8]>> {
    match cs {
        Charset::Utf8mb4 | Charset::Utf8mb3 | Charset::Ascii | Charset::Binary => {
            decode(s, cs)?;
            Ok(std::borrow::Cow::Borrowed(s))
        }
        Charset::Latin1 => Ok(std::borrow::Cow::Owned(
            decode(s, cs)?
                .into_iter()
                .map(|c| char::from_u32(c).unwrap_or(char::REPLACEMENT_CHARACTER))
                .collect::<String>()
                .into_bytes(),
        )),
    }
}

/// The `latin1` byte of a code point, if it has one.
fn latin1_byte(cp: u32) -> Option<u8> {
    let t = tables();
    t.latin1_rev.get(&cp).copied()
}

// ---- generated tables ----------------------------------------------------

static DATA: &[u8] = include_bytes!("../data/collations.bin.z");

#[derive(Debug)]
struct Run {
    start: u32,
    len: u32,
    delta: u8,
    n: u8,
    off: u32,
}

#[derive(Debug, Default)]
struct Level {
    runs: Vec<Run>,
    pool: Vec<u16>,
}

impl Level {
    fn push(&self, cp: u32, width: usize, out: &mut Vec<u8>) {
        let i = self.runs.partition_point(|r| r.start <= cp);
        let Some(r) = i.checked_sub(1).map(|i| &self.runs[i]) else {
            return;
        };
        if cp - r.start >= r.len {
            return;
        }
        let off = r.off as usize;
        let ws = &self.pool[off..off + usize::from(r.n)];
        let Some((last, rest)) = ws.split_last() else {
            return;
        };
        push_weights(rest.iter().copied(), width, out);
        #[allow(clippy::cast_possible_truncation)]
        let last = last.wrapping_add((u32::from(r.delta) * (cp - r.start)) as u16);
        push_weights(std::iter::once(last), width, out);
    }
}

#[derive(Debug)]
struct Contraction {
    cps: Vec<u32>,
    weights: Vec<Vec<u16>>,
}

#[derive(Debug, Default)]
struct WeightTable {
    levels: Vec<Level>,
    /// Contractions by first code point, longest first.
    contractions: HashMap<u32, Vec<Contraction>>,
}

impl WeightTable {
    fn contraction(&self, units: &[u32], level: usize) -> Option<(usize, &[u16])> {
        let first = units.first()?;
        let cands = self.contractions.get(first)?;
        cands
            .iter()
            .find(|c| units.starts_with(&c.cps))
            .map(|c| (c.cps.len(), c.weights[level].as_slice()))
    }
}

#[derive(Debug)]
#[allow(clippy::struct_field_names)]
struct Tables {
    tables: Vec<WeightTable>,
    latin1: [u32; 256],
    latin1_rev: HashMap<u32, u8>,
}

fn tables() -> &'static Tables {
    static T: OnceLock<Tables> = OnceLock::new();
    T.get_or_init(|| {
        // The data is compiled in; a failure here is a build defect, caught
        // by the unit tests.
        #[allow(clippy::expect_used)]
        parse_tables(DATA).expect("sprig-shard's collation tables are corrupt")
    })
}

struct Reader<'a> {
    b: &'a [u8],
}

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Option<&[u8]> {
        if self.b.len() < n {
            return None;
        }
        let (h, t) = self.b.split_at(n);
        self.b = t;
        Some(h)
    }
    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|b| b[0])
    }
    fn u16(&mut self) -> Option<u16> {
        self.take(2).map(|b| u16::from_le_bytes([b[0], b[1]]))
    }
    fn u32(&mut self) -> Option<u32> {
        self.take(4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
}

#[allow(clippy::many_single_char_names)]
fn parse_tables(z: &[u8]) -> Option<Tables> {
    let mut raw = Vec::new();
    flate2::read::ZlibDecoder::new(z)
        .read_to_end(&mut raw)
        .ok()?;
    let mut r = Reader { b: &raw };
    if r.take(4)? != b"SPRW" || r.u8()? != 1 {
        return None;
    }
    let count = r.u16()?;
    let mut by_name: HashMap<String, WeightTable> = HashMap::new();
    for _ in 0..count {
        let nl = usize::from(r.u16()?);
        let name = String::from_utf8(r.take(nl)?.to_vec()).ok()?;
        let _width = r.u8()?;
        let nlevels = r.u8()?;
        let mut t = WeightTable::default();
        for _ in 0..nlevels {
            let mut lv = Level::default();
            let runs = r.u32()?;
            for _ in 0..runs {
                let start = r.u32()?;
                let len = r.u32()?;
                let delta = r.u8()?;
                let n = r.u8()?;
                let off = u32::try_from(lv.pool.len()).ok()?;
                for _ in 0..n {
                    lv.pool.push(r.u16()?);
                }
                lv.runs.push(Run {
                    start,
                    len,
                    delta,
                    n,
                    off,
                });
            }
            t.levels.push(lv);
        }
        let nc = r.u32()?;
        for _ in 0..nc {
            let k = usize::from(r.u8()?);
            let mut cps = Vec::with_capacity(k);
            for _ in 0..k {
                cps.push(r.u32()?);
            }
            let mut weights = Vec::new();
            for _ in 0..nlevels {
                let n = r.u8()?;
                let mut ws = Vec::with_capacity(usize::from(n));
                for _ in 0..n {
                    ws.push(r.u16()?);
                }
                weights.push(ws);
            }
            t.contractions
                .entry(cps[0])
                .or_default()
                .push(Contraction { cps, weights });
        }
        for v in t.contractions.values_mut() {
            v.sort_by_key(|c| std::cmp::Reverse(c.cps.len()));
        }
        by_name.insert(name, t);
    }
    if r.u16()? != 256 {
        return None;
    }
    let mut latin1 = [0u32; 256];
    let mut latin1_rev = HashMap::new();
    for (b, slot) in latin1.iter_mut().enumerate() {
        *slot = r.u32()?;
        latin1_rev.insert(*slot, u8::try_from(b).ok()?);
    }
    let mut tables = Vec::new();
    for n in TABLE_NAMES {
        tables.push(by_name.remove(n)?);
    }
    Some(Tables {
        tables,
        latin1,
        latin1_rev,
    })
}

#[cfg(test)]
mod tests;
