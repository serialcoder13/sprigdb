//! Joins across data models, the SQL part (chapter 22, "The type mapping",
//! "Comparison rules per query language", "When a join is co-located").
//!
//! - [`SqlType`] and [`SqlFamily`]: a SQL column type as chapter 22's type
//!   map sees it, read from `information_schema.COLUMNS.DATA_TYPE` (or a
//!   `COLUMN_TYPE` such as `tinyint(1) unsigned`) and the column's
//!   collation;
//! - [`SqlType::key_class`]: the map's last column, chapter 15's key class
//!   (`None`: not a key);
//! - [`cmp_type`]: the comparison MySQL's rules pick for `a = b`;
//! - [`CompareRules`], [`MemberFlags`] and [`colocated`]: whether an
//!   equality between two members' key parts holds only for rows on one
//!   shard, under SQL's or MongoDB's rules;
//! - [`edge_colocated`]: the check the advisor (chapter 16) applies to a
//!   recorded join edge between two members of one group.
//!
//! The book puts these in `sprig-router::colocate`; they live here, beside
//! chapter 15's `colocation_safe` and the collation tables they read, so
//! the advisor and the router share one copy without the advisor
//! depending on the router.
//!
//! R1 builds the SQL side. The BSON, stored-JSON and protobuf columns of
//! the type map (`sprig-doc::typemap`), field columns and the
//! `double_inexact_seen` catalog flag come with documents in R5:
//! [`MemberFlags::double_inexact_seen`] exists in memory and is `false`
//! for every table.

use crate::collation::{self, CollationId};
use crate::groups::{CmpType, finer_or_equal};
use crate::key::KeyClass;

/// A SQL type family, as chapter 22's type map groups types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlFamily {
    /// `TINYINT(1)`, `BOOLEAN`.
    Bool,
    /// `TINYINT` to `BIGINT` (signed or not), `YEAR`.
    Integer,
    /// `DECIMAL(p,s)`, `NUMERIC`.
    Decimal,
    /// `FLOAT`, `DOUBLE`, `REAL`.
    Double,
    /// `CHAR`, `VARCHAR`, the `TEXT` types.
    Text,
    /// `ENUM` (compares as its string with strings, as its index with
    /// numbers).
    Enum,
    /// `BINARY`, `VARBINARY`, the `BLOB` types.
    Binary,
    /// `UUID`.
    Uuid,
    /// `DATE`.
    Date,
    /// `DATETIME(n)`.
    DateTime,
    /// `TIMESTAMP(n)`.
    Timestamp,
    /// `TIME`.
    Time,
    /// `BIT(n)`.
    Bit,
    /// `SET`.
    Set,
    /// `JSON` (MariaDB stores it as `LONGTEXT`; `information_schema`
    /// reports `longtext`, which reads as [`Self::Text`]).
    Json,
    /// `VECTOR(n)`.
    Vector,
    /// Geometry types.
    Geometry,
}

/// A SQL column type: its family and, for character types, its collation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlType {
    /// The family.
    pub family: SqlFamily,
    /// The column's collation (`Text` and `Enum`); `None` when unknown or
    /// not a supported collation.
    pub collation: Option<CollationId>,
}

impl SqlType {
    /// A type of `family` without a collation.
    #[must_use]
    pub const fn of(family: SqlFamily) -> Self {
        Self {
            family,
            collation: None,
        }
    }

    /// A character type under `collation`.
    #[must_use]
    pub const fn text(collation: CollationId) -> Self {
        Self {
            family: SqlFamily::Text,
            collation: Some(collation),
        }
    }

    /// Reads a type from `DATA_TYPE` (`int`, `varchar`) or a `COLUMN_TYPE`
    /// (`tinyint(1) unsigned`, `decimal(10,2)`) and the column's collation
    /// name. `None` for a type the map doesn't know (`inet6`, a user type).
    #[must_use]
    pub fn parse(ty: &str, collation: Option<&str>) -> Option<Self> {
        use SqlFamily as F;
        let lower = ty.trim().to_ascii_lowercase();
        let base: String = lower
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        let args = lower
            .get(base.len()..)
            .map(str::trim_start)
            .and_then(|r| r.strip_prefix('('))
            .and_then(|r| r.split_once(')'))
            .map(|(a, _)| a.trim().to_owned());
        let family = match base.as_str() {
            "bool" | "boolean" => F::Bool,
            "tinyint" | "int1" if args.as_deref() == Some("1") => F::Bool,
            "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "int1"
            | "int2" | "int3" | "int4" | "int8" | "middleint" | "year" | "serial" => F::Integer,
            "decimal" | "numeric" | "dec" | "fixed" => F::Decimal,
            "float" | "double" | "real" | "float4" | "float8" => F::Double,
            "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "long"
            | "nchar" | "nvarchar" | "character" => F::Text,
            "enum" => F::Enum,
            "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => F::Binary,
            "uuid" => F::Uuid,
            "date" => F::Date,
            "datetime" => F::DateTime,
            "timestamp" => F::Timestamp,
            "time" => F::Time,
            "bit" => F::Bit,
            "set" => F::Set,
            "json" => F::Json,
            "vector" => F::Vector,
            "geometry" | "point" | "linestring" | "polygon" | "multipoint" | "multilinestring"
            | "multipolygon" | "geometrycollection" => F::Geometry,
            _ => return None,
        };
        let collation = match family {
            F::Text | F::Enum => collation
                .and_then(collation::collation_by_name)
                .map(|c| c.id),
            _ => None,
        };
        Some(Self { family, collation })
    }

    /// Chapter 22's key class for the type (the type map's last column);
    /// `None` for types that are not a key (`FLOAT`, `TIME`, `BIT`, `SET`,
    /// `JSON`, `VECTOR`, geometry) and for character types whose
    /// collation is unknown.
    ///
    /// This is the full map. What the R1 advisor and router accept as a
    /// distribution key is narrower (`sprig_shard::pins::column_class`:
    /// no `TEXT`, `ENUM` or `BLOB` keys yet).
    #[must_use]
    pub fn key_class(&self) -> Option<KeyClass> {
        use SqlFamily as F;
        match self.family {
            F::Bool | F::Integer | F::Decimal => Some(KeyClass::Numeric),
            F::Text | F::Enum => self.collation.map(|c| KeyClass::String { collation: c }),
            F::Binary => Some(KeyClass::Binary),
            F::Uuid => Some(KeyClass::Uuid),
            F::Date | F::DateTime => Some(KeyClass::DateTime),
            F::Timestamp => Some(KeyClass::Timestamp),
            F::Double | F::Time | F::Bit | F::Set | F::Json | F::Vector | F::Geometry => None,
        }
    }

    fn numeric(self) -> bool {
        matches!(
            self.family,
            SqlFamily::Bool | SqlFamily::Integer | SqlFamily::Decimal | SqlFamily::Double
        )
    }

    fn stringy(self) -> bool {
        matches!(self.family, SqlFamily::Text | SqlFamily::Enum)
    }
}

/// The collation MySQL's coercibility rules give two column operands of
/// the same coercibility: the same collation, or the `_bin` one when both
/// are of one character set. `None` where MariaDB reports an illegal mix
/// (or the superset conversion between character sets, which R1 doesn't
/// follow).
fn string_collation(a: CollationId, b: CollationId) -> Option<CollationId> {
    if a == b {
        return Some(a);
    }
    let (ca, cb) = (collation::collation(a)?, collation::collation(b)?);
    if ca.charset != cb.charset {
        return None;
    }
    match (ca.is_binary(), cb.is_binary()) {
        (true, false) => Some(a),
        (false, true) => Some(b),
        _ => None,
    }
}

/// The comparison MySQL's rules pick for `a = b` between two columns, as a
/// [`CmpType`]; `None` when the comparison is none of them, so it never
/// co-locates:
///
/// - `TIME`, `BIT`, `SET`, `JSON`, `VECTOR` and geometry on either side;
/// - `ENUM` against a number (MySQL compares the index);
/// - two strings whose collations MariaDB won't mix;
/// - `TIMESTAMP` against `DATE` or `DATETIME` (compared in the session
///   time zone);
/// - temporal or `UUID` against an unrelated type.
///
/// Numbers compare by value: integers as integers, integers and decimals as
/// decimals, anything with a `FLOAT`/`DOUBLE` or a string against a number
/// in double precision.
#[must_use]
pub fn cmp_type(a: &SqlType, b: &SqlType) -> Option<CmpType> {
    use SqlFamily as F;
    let never = |t: &SqlType| {
        matches!(
            t.family,
            F::Time | F::Bit | F::Set | F::Json | F::Vector | F::Geometry
        )
    };
    if never(a) || never(b) {
        return None;
    }
    let int = |t: &SqlType| matches!(t.family, F::Bool | F::Integer);
    let temporal = |t: &SqlType| matches!(t.family, F::Date | F::DateTime | F::Timestamp);
    match (a.family, b.family) {
        _ if int(a) && int(b) => Some(CmpType::Integer),
        (F::Double, _) | (_, F::Double) if a.numeric() && b.numeric() => Some(CmpType::Double),
        _ if a.numeric() && b.numeric() => Some(CmpType::Decimal),
        (F::Enum, _) if b.numeric() => None,
        (_, F::Enum) if a.numeric() => None,
        _ if (a.stringy() && b.numeric()) || (a.numeric() && b.stringy()) => Some(CmpType::Double),
        _ if a.stringy() && b.stringy() => {
            string_collation(a.collation?, b.collation?).map(CmpType::String)
        }
        (F::Binary, F::Binary) => Some(CmpType::Binary),
        (F::Binary, _) | (_, F::Binary) if a.stringy() || b.stringy() => Some(CmpType::Binary),
        (F::Uuid, F::Uuid) => Some(CmpType::Uuid),
        (F::Uuid, _) | (_, F::Uuid) if a.stringy() || b.stringy() => Some(CmpType::Uuid),
        (F::Timestamp, F::Timestamp) => Some(CmpType::Timestamp),
        (F::Timestamp, _) | (_, F::Timestamp) => None,
        _ if temporal(a) && temporal(b) => Some(CmpType::DateTime),
        _ => None,
    }
}

/// Which query language's equality a join uses (chapter 22, "Comparison
/// rules per query language").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareRules {
    /// SQL: MySQL's rules, evaluated by MariaDB, with the comparison type
    /// [`cmp_type`] picks.
    MySql(CmpType),
    /// `$lookup`: MongoDB's rules (numbers by value across types, strings
    /// under the aggregation's collation, a string never equals a number).
    /// R5 records such joins; the advisor already counts them.
    Mongo {
        /// The aggregation's collation, if any.
        collation: Option<CollationId>,
    },
}

/// A member's one-way flags, as joins read them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MemberFlags {
    /// Chapter 15's `group_members.off_class_seen`.
    pub off_class_seen: bool,
    /// Chapter 22's proposed `double_inexact_seen`: a key value that is
    /// not an integer within ±2^53 was stored. Always `false` for tables
    /// (only documents compared in double precision can set it; R5 adds the
    /// catalog column and its writer).
    pub double_inexact_seen: bool,
}

/// Whether an equality under `rules` between two members' key parts of
/// class `key` (each side being that part for its member, directly or
/// through a parent chain: chapter 15's first condition, which the caller
/// checks) holds only for rows on the same shard (chapter 22's table).
#[must_use]
pub fn colocated(rules: &CompareRules, key: KeyClass, a: MemberFlags, b: MemberFlags) -> bool {
    let cmp = match rules {
        // Chapter 15's normalization hashes every value of a key the same
        // way whatever its BSON type; MongoDB's equality implies equal
        // normalized bytes.
        CompareRules::Mongo { .. } => return true,
        CompareRules::MySql(c) => *c,
    };
    let clean = !a.off_class_seen && !b.off_class_seen;
    match (key, cmp) {
        (KeyClass::Numeric, CmpType::Integer | CmpType::Decimal) => true,
        (KeyClass::Numeric, CmpType::Double) => {
            clean && !a.double_inexact_seen && !b.double_inexact_seen
        }
        (KeyClass::String { collation: g }, CmpType::String(c)) => clean && finer_or_equal(c, g),
        (KeyClass::Binary, CmpType::Binary)
        | (KeyClass::DateTime, CmpType::DateTime)
        | (KeyClass::Timestamp, CmpType::Timestamp)
        | (KeyClass::Uuid, CmpType::Uuid) => clean,
        _ => false,
    }
}

/// Whether a column of type `t` can hold key part `key`: its own key class
/// is of the same kind (a string column of any collation for a string key;
/// [`colocated`] then checks the comparison's collation).
#[must_use]
pub fn fits_key(t: &SqlType, key: KeyClass) -> bool {
    matches!(
        (t.key_class(), key),
        (Some(KeyClass::Numeric), KeyClass::Numeric)
            | (Some(KeyClass::String { .. }), KeyClass::String { .. })
            | (Some(KeyClass::Binary), KeyClass::Binary)
            | (Some(KeyClass::Uuid), KeyClass::Uuid)
            | (Some(KeyClass::DateTime), KeyClass::DateTime)
            | (Some(KeyClass::Timestamp), KeyClass::Timestamp)
    )
}

/// A recorded join edge `a.x = b.y` between two members whose columns `x`
/// and `y` are the group's key part `key`: co-located when, under SQL's
/// rules, both columns fit the key and [`colocated`] holds for MySQL's
/// comparison of their types; under MongoDB's rules, always.
#[must_use]
pub fn edge_colocated(
    sql_rules: bool,
    a: &SqlType,
    b: &SqlType,
    key: KeyClass,
    fa: MemberFlags,
    fb: MemberFlags,
) -> bool {
    if !sql_rules {
        return colocated(&CompareRules::Mongo { collation: None }, key, fa, fb);
    }
    if !fits_key(a, key) || !fits_key(b, key) {
        return false;
    }
    cmp_type(a, b).is_some_and(|c| colocated(&CompareRules::MySql(c), key, fa, fb))
}

#[cfg(test)]
mod tests;
