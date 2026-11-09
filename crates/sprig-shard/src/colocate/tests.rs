use super::*;
use crate::collation::collation_by_name;

fn coll(n: &str) -> CollationId {
    collation_by_name(n).unwrap_or_else(|| panic!("{n}")).id
}

fn t(ty: &str) -> SqlType {
    SqlType::parse(ty, None).unwrap_or_else(|| panic!("{ty}"))
}

fn s(ty: &str, c: &str) -> SqlType {
    SqlType::parse(ty, Some(c)).unwrap_or_else(|| panic!("{ty}"))
}

const CLEAN: MemberFlags = MemberFlags {
    off_class_seen: false,
    double_inexact_seen: false,
};

#[test]
#[allow(clippy::too_many_lines)] // one row per type of the map
fn type_map_covers_every_sql_family() {
    let ci = coll("utf8mb4_general_ci");
    let cases: &[(&str, Option<&str>, SqlFamily, Option<KeyClass>)] = &[
        ("tinyint(1)", None, SqlFamily::Bool, Some(KeyClass::Numeric)),
        ("BOOLEAN", None, SqlFamily::Bool, Some(KeyClass::Numeric)),
        ("tinyint", None, SqlFamily::Integer, Some(KeyClass::Numeric)),
        (
            "smallint(6)",
            None,
            SqlFamily::Integer,
            Some(KeyClass::Numeric),
        ),
        (
            "int(11) unsigned",
            None,
            SqlFamily::Integer,
            Some(KeyClass::Numeric),
        ),
        ("bigint", None, SqlFamily::Integer, Some(KeyClass::Numeric)),
        ("year", None, SqlFamily::Integer, Some(KeyClass::Numeric)),
        (
            "decimal(12,2)",
            None,
            SqlFamily::Decimal,
            Some(KeyClass::Numeric),
        ),
        ("float", None, SqlFamily::Double, None),
        ("double", None, SqlFamily::Double, None),
        (
            "varchar(40)",
            Some("utf8mb4_general_ci"),
            SqlFamily::Text,
            Some(KeyClass::String { collation: ci }),
        ),
        (
            "char",
            Some("utf8mb4_general_ci"),
            SqlFamily::Text,
            Some(KeyClass::String { collation: ci }),
        ),
        (
            "text",
            Some("utf8mb4_general_ci"),
            SqlFamily::Text,
            Some(KeyClass::String { collation: ci }),
        ),
        (
            "enum('a','b')",
            Some("utf8mb4_general_ci"),
            SqlFamily::Enum,
            Some(KeyClass::String { collation: ci }),
        ),
        // A character type under a collation the tables don't have.
        ("varchar(10)", Some("no_such_ci"), SqlFamily::Text, None),
        (
            "binary(16)",
            None,
            SqlFamily::Binary,
            Some(KeyClass::Binary),
        ),
        (
            "binary(12)",
            None,
            SqlFamily::Binary,
            Some(KeyClass::Binary),
        ),
        (
            "varbinary(30)",
            None,
            SqlFamily::Binary,
            Some(KeyClass::Binary),
        ),
        ("blob", None, SqlFamily::Binary, Some(KeyClass::Binary)),
        ("uuid", None, SqlFamily::Uuid, Some(KeyClass::Uuid)),
        ("date", None, SqlFamily::Date, Some(KeyClass::DateTime)),
        (
            "datetime(6)",
            None,
            SqlFamily::DateTime,
            Some(KeyClass::DateTime),
        ),
        (
            "timestamp(3)",
            None,
            SqlFamily::Timestamp,
            Some(KeyClass::Timestamp),
        ),
        ("time", None, SqlFamily::Time, None),
        ("bit(8)", None, SqlFamily::Bit, None),
        ("set('x','y')", None, SqlFamily::Set, None),
        ("json", None, SqlFamily::Json, None),
        ("vector(3)", None, SqlFamily::Vector, None),
        ("point", None, SqlFamily::Geometry, None),
        ("geometry", None, SqlFamily::Geometry, None),
    ];
    for (ty, c, fam, class) in cases {
        let got = SqlType::parse(ty, *c).unwrap_or_else(|| panic!("{ty}"));
        assert_eq!(got.family, *fam, "{ty}");
        assert_eq!(got.key_class(), *class, "{ty}");
    }
    assert_eq!(SqlType::parse("inet6", None), None);
    // Only character types keep their collation.
    assert_eq!(
        SqlType::parse("int", Some("utf8mb4_general_ci")).map(|x| x.collation),
        Some(None)
    );
}

#[test]
fn mysql_comparison_types() {
    let ci = s("varchar(10)", "utf8mb4_general_ci");
    let bin = s("varchar(10)", "utf8mb4_bin");
    let uca = s("varchar(10)", "utf8mb4_uca1400_ai_ci");
    let latin = s("varchar(10)", "latin1_swedish_ci");
    assert_eq!(cmp_type(&t("int"), &t("bigint")), Some(CmpType::Integer));
    assert_eq!(
        cmp_type(&t("tinyint(1)"), &t("year")),
        Some(CmpType::Integer)
    );
    assert_eq!(
        cmp_type(&t("int"), &t("decimal(10,2)")),
        Some(CmpType::Decimal)
    );
    assert_eq!(
        cmp_type(&t("decimal"), &t("decimal")),
        Some(CmpType::Decimal)
    );
    assert_eq!(cmp_type(&t("int"), &t("double")), Some(CmpType::Double));
    // A string against a number: double precision.
    assert_eq!(cmp_type(&ci, &t("int")), Some(CmpType::Double));
    // ENUM against a number compares the index: never by value.
    assert_eq!(
        cmp_type(&s("enum('a')", "utf8mb4_general_ci"), &t("int")),
        None
    );
    assert_eq!(
        cmp_type(&s("enum('a')", "utf8mb4_general_ci"), &ci),
        Some(CmpType::String(coll("utf8mb4_general_ci")))
    );
    // Strings: same collation, or the _bin side of one character set.
    assert_eq!(
        cmp_type(&ci, &ci),
        Some(CmpType::String(coll("utf8mb4_general_ci")))
    );
    assert_eq!(
        cmp_type(&ci, &bin),
        Some(CmpType::String(coll("utf8mb4_bin")))
    );
    assert_eq!(cmp_type(&ci, &uca), None, "illegal mix");
    assert_eq!(cmp_type(&ci, &latin), None, "two character sets");
    assert_eq!(cmp_type(&ci, &t("varbinary(10)")), Some(CmpType::Binary));
    assert_eq!(
        cmp_type(&t("binary(16)"), &t("blob")),
        Some(CmpType::Binary)
    );
    assert_eq!(cmp_type(&t("uuid"), &t("uuid")), Some(CmpType::Uuid));
    assert_eq!(cmp_type(&t("uuid"), &ci), Some(CmpType::Uuid));
    assert_eq!(cmp_type(&t("uuid"), &t("int")), None);
    assert_eq!(
        cmp_type(&t("date"), &t("datetime")),
        Some(CmpType::DateTime)
    );
    assert_eq!(
        cmp_type(&t("timestamp"), &t("timestamp(6)")),
        Some(CmpType::Timestamp)
    );
    assert_eq!(
        cmp_type(&t("timestamp"), &t("datetime")),
        None,
        "session time zone"
    );
    for never in ["time", "bit(1)", "set('a')", "json", "vector(2)", "point"] {
        assert_eq!(cmp_type(&t(never), &t(never)), None, "{never}");
        assert_eq!(cmp_type(&t(never), &t("int")), None, "{never}");
    }
}

#[test]
fn colocation_per_chapter_22_table() {
    let ci = coll("utf8mb4_general_ci");
    let bin = coll("utf8mb4_bin");
    let off = MemberFlags {
        off_class_seen: true,
        double_inexact_seen: false,
    };
    let inexact = MemberFlags {
        off_class_seen: false,
        double_inexact_seen: true,
    };
    let sql = |c| CompareRules::MySql(c);
    // Integer and decimal: nothing else required.
    assert!(colocated(
        &sql(CmpType::Integer),
        KeyClass::Numeric,
        CLEAN,
        off
    ));
    assert!(colocated(
        &sql(CmpType::Decimal),
        KeyClass::Numeric,
        off,
        off
    ));
    // Double: both flags clear on both members.
    assert!(colocated(
        &sql(CmpType::Double),
        KeyClass::Numeric,
        CLEAN,
        CLEAN
    ));
    assert!(!colocated(
        &sql(CmpType::Double),
        KeyClass::Numeric,
        CLEAN,
        off
    ));
    assert!(!colocated(
        &sql(CmpType::Double),
        KeyClass::Numeric,
        inexact,
        CLEAN
    ));
    // Strings: the comparison's collation is the group's or finer.
    let skey = KeyClass::String { collation: ci };
    assert!(colocated(&sql(CmpType::String(ci)), skey, CLEAN, CLEAN));
    assert!(colocated(&sql(CmpType::String(bin)), skey, CLEAN, CLEAN));
    assert!(!colocated(
        &sql(CmpType::String(ci)),
        KeyClass::String { collation: bin },
        CLEAN,
        CLEAN
    ));
    assert!(!colocated(&sql(CmpType::String(ci)), skey, off, CLEAN));
    // Binary, temporal, UUID: the same class, off_class_seen clear.
    assert!(colocated(
        &sql(CmpType::Binary),
        KeyClass::Binary,
        CLEAN,
        CLEAN
    ));
    assert!(!colocated(
        &sql(CmpType::Binary),
        KeyClass::Binary,
        CLEAN,
        off
    ));
    assert!(colocated(
        &sql(CmpType::DateTime),
        KeyClass::DateTime,
        CLEAN,
        CLEAN
    ));
    assert!(!colocated(
        &sql(CmpType::DateTime),
        KeyClass::Timestamp,
        CLEAN,
        CLEAN
    ));
    assert!(colocated(&sql(CmpType::Uuid), KeyClass::Uuid, CLEAN, CLEAN));
    assert!(!colocated(
        &sql(CmpType::Integer),
        KeyClass::Binary,
        CLEAN,
        CLEAN
    ));
    // MongoDB's $lookup: any type, whatever the flags.
    let mongo = CompareRules::Mongo { collation: None };
    assert!(colocated(&mongo, KeyClass::Numeric, off, inexact));
    assert!(colocated(&mongo, skey, off, off));
}

#[test]
fn join_edges_between_members() {
    let ci = coll("utf8mb4_general_ci");
    let num = KeyClass::Numeric;
    let yes = |a: &SqlType, b: &SqlType, k| edge_colocated(true, a, b, k, CLEAN, CLEAN);
    assert!(yes(&t("int"), &t("bigint"), num));
    assert!(yes(&t("int"), &t("decimal(20,0)"), num));
    // A string column holding numeric keys compares in double precision
    // with an INT; for tables it can't hold a numeric key at all.
    assert!(!yes(
        &s("varchar(10)", "utf8mb4_general_ci"),
        &t("int"),
        num
    ));
    // FLOAT is not a key.
    assert!(!yes(&t("float"), &t("int"), num));
    let skey = KeyClass::String { collation: ci };
    assert!(yes(
        &s("varchar(10)", "utf8mb4_general_ci"),
        &s("char(10)", "utf8mb4_general_ci"),
        skey
    ));
    assert!(yes(
        &s("varchar(10)", "utf8mb4_bin"),
        &s("varchar(10)", "utf8mb4_general_ci"),
        skey
    ));
    assert!(!yes(
        &s("varchar(10)", "utf8mb4_uca1400_ai_ci"),
        &s("varchar(10)", "utf8mb4_general_ci"),
        skey
    ));
    assert!(!yes(&t("timestamp"), &t("datetime"), KeyClass::DateTime));
    assert!(yes(&t("date"), &t("datetime"), KeyClass::DateTime));
    // MongoDB's rules: a $lookup edge counts whatever the SQL types.
    assert!(edge_colocated(
        false,
        &s("varchar(10)", "utf8mb4_general_ci"),
        &t("int"),
        num,
        CLEAN,
        CLEAN
    ));
    assert!(fits_key(&t("tinyint(1)"), num));
    assert!(!fits_key(&t("json"), num));
}
