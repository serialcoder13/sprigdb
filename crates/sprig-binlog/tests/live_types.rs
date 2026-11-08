//! Every MariaDB column type, written through SQL and decoded back from the
//! binlog: each decoded value, rendered as MariaDB renders it, must equal
//! what the server returns for the same row. Runs on
//! `SPRIG_TEST_MARIADB_SINGLE` and on each `SPRIG_TEST_BINLOG_SCRATCH` server.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::similar_names
)]

mod common;

use common::{drop_schema, gtid_pos, hex, opts, read_range, schema_name, sql, urls};
use mysql_async::prelude::Queryable;
use sprig_binlog::value::{inet4_text, inet6_text, uuid_text, vector_values};
use sprig_binlog::{Event, RowImage, RowsKind, Value};

/// How to compare a column: the SQL expression whose text MariaDB returns,
/// and how the decoded value renders.
#[derive(Clone, Copy, Debug)]
enum Render {
    /// `CAST(c AS CHAR)`: integers, decimals, floats, years, temporals.
    Text,
    /// `c + 0` as unsigned text: BIT, ENUM and SET as numbers (a 64-member
    /// SET would otherwise print as signed).
    Number,
    /// `HEX(c)`: strings, binaries, JSON, geometry.
    Hex,
    /// `CAST(c AS CHAR)` against the UUID/INET helpers.
    Uuid,
    Inet4,
    Inet6,
    /// `VEC_ToText(c)`.
    Vector,
}

struct Col {
    name: &'static str,
    ddl: String,
    vals: [String; 2],
    render: Render,
}

fn col(name: &'static str, ddl: &str, a: &str, b: &str, render: Render) -> Col {
    Col {
        name,
        ddl: ddl.to_owned(),
        vals: [a.to_owned(), b.to_owned()],
        render,
    }
}

fn columns() -> Vec<Col> {
    use Render::{Hex, Inet4, Inet6, Number, Text, Uuid, Vector};
    let enum300: Vec<String> = (0..300).map(|i| format!("'m{i}'")).collect();
    let set64: Vec<String> = (0..64).map(|i| format!("'s{i}'")).collect();
    vec![
        col("ti", "TINYINT", "-128", "127", Text),
        col("tiu", "TINYINT UNSIGNED", "0", "255", Text),
        col("si", "SMALLINT", "-32768", "32767", Text),
        col("siu", "SMALLINT UNSIGNED", "65535", "1", Text),
        col("mi", "MEDIUMINT", "-8388608", "8388607", Text),
        col("miu", "MEDIUMINT UNSIGNED", "16777215", "0", Text),
        col("i", "INT", "-2147483648", "2147483647", Text),
        col("iu", "INT UNSIGNED", "4294967295", "7", Text),
        col(
            "bi",
            "BIGINT",
            "-9223372036854775808",
            "9223372036854775807",
            Text,
        ),
        col("biu", "BIGINT UNSIGNED", "18446744073709551615", "0", Text),
        col("f", "FLOAT", "1.5", "-0.1", Text),
        col("d", "DOUBLE", "3.141592653589793", "-1e-300", Text),
        col(
            "dec65",
            "DECIMAL(65,30)",
            "-12345678901234567890123456789012345.123456789012345678901234567890",
            "0.000000000000000000000000000001",
            Text,
        ),
        col("dec10", "DECIMAL(10,2)", "-0.05", "99999999.99", Text),
        col("dec5", "DECIMAL(5,0)", "-99999", "0", Text),
        col(
            "dec9",
            "DECIMAL(18,9)",
            "123456789.987654321",
            "-1.000000001",
            Text,
        ),
        col("dec1", "DECIMAL(1,1)", "0.5", "-0.9", Text),
        col("b1", "BIT(1)", "b'1'", "b'0'", Number),
        col("b13", "BIT(13)", "b'1010101010101'", "b'1'", Number),
        col(
            "b64",
            "BIT(64)",
            "x'ffffffffffffffff'",
            "x'8000000000000001'",
            Number,
        ),
        col("y", "YEAR", "1901", "2155", Text),
        col("dt", "DATE", "'1000-01-01'", "'9999-12-31'", Text),
        col("dtz", "DATE", "'0000-00-00'", "'2024-02-29'", Text),
        col("tm", "TIME", "'-838:59:59'", "'838:59:59'", Text),
        col("tm1", "TIME(1)", "'-01:02:03.4'", "'00:00:00.1'", Text),
        col("tm3", "TIME(3)", "'-00:00:00.001'", "'12:34:56.789'", Text),
        col(
            "tm5",
            "TIME(5)",
            "'-838:59:58.99999'",
            "'00:00:01.00001'",
            Text,
        ),
        col(
            "tm6",
            "TIME(6)",
            "'-00:00:00.000001'",
            "'12:34:56.789012'",
            Text,
        ),
        col(
            "dtm",
            "DATETIME",
            "'1000-01-01 00:00:00'",
            "'9999-12-31 23:59:59'",
            Text,
        ),
        col(
            "dtm2",
            "DATETIME(2)",
            "'2024-02-29 01:02:03.45'",
            "'1970-01-01 00:00:00.01'",
            Text,
        ),
        col(
            "dtm4",
            "DATETIME(4)",
            "'2024-02-29 01:02:03.4567'",
            "'0000-00-00 00:00:00.0000'",
            Text,
        ),
        col(
            "dtm6",
            "DATETIME(6)",
            "'2024-02-29 01:02:03.123456'",
            "'9999-12-31 23:59:59.999999'",
            Text,
        ),
        col(
            "ts",
            "TIMESTAMP NULL",
            "'1970-01-01 00:00:01'",
            "'2038-01-19 03:14:07'",
            Text,
        ),
        col(
            "ts3",
            "TIMESTAMP(3) NULL",
            "'2024-02-29 01:02:03.456'",
            "'0000-00-00 00:00:00'",
            Text,
        ),
        col(
            "ts6",
            "TIMESTAMP(6) NULL",
            "'2001-09-09 01:46:40.000001'",
            "'2038-01-19 03:14:07.999999'",
            Text,
        ),
        col("c", "CHAR(10) CHARACTER SET utf8mb4", "'héllo'", "''", Hex),
        col(
            "c255",
            "CHAR(255) CHARACTER SET utf8mb4",
            "REPEAT('é', 255)",
            "'x'",
            Hex,
        ),
        col(
            "cl1",
            "CHAR(5) CHARACTER SET latin1",
            "'café'",
            "'a b'",
            Hex,
        ),
        col(
            "vc",
            "VARCHAR(100) CHARACTER SET latin1",
            "'abc'",
            "''",
            Hex,
        ),
        col(
            "vcl",
            "VARCHAR(1000) CHARACTER SET utf8mb4",
            "REPEAT('ü', 1000)",
            "'z'",
            Hex,
        ),
        col("bn", "BINARY(4)", "x'00ff'", "x'01020304'", Hex),
        col(
            "vb",
            "VARBINARY(300)",
            "x'00010203'",
            "REPEAT(x'ff', 300)",
            Hex,
        ),
        col("tt", "TINYTEXT", "'tiny'", "REPEAT('t', 255)", Hex),
        col("tx", "TEXT", "REPEAT('text ', 1000)", "''", Hex),
        col("mt", "MEDIUMTEXT", "REPEAT('m', 70000)", "'m'", Hex),
        col("lt", "LONGTEXT", "'long'", "REPEAT('é', 40000)", Hex),
        col("tb", "TINYBLOB", "x'00'", "REPEAT(x'01', 255)", Hex),
        col("bl", "BLOB", "x'deadbeef'", "''", Hex),
        col("mb", "MEDIUMBLOB", "REPEAT(x'aa', 70000)", "x'bb'", Hex),
        col("lb", "LONGBLOB", "x'cc'", "REPEAT(x'00', 1000)", Hex),
        col(
            "j",
            "JSON",
            r#"'{"a": [1, 2.5, "x"], "b": null}'"#,
            "'[]'",
            Hex,
        ),
        col("e", "ENUM('a','b','c')", "'c'", "'a'", Number),
        col(
            "e300",
            &format!("ENUM({})", enum300.join(",")),
            "'m299'",
            "'m0'",
            Number,
        ),
        col("s", "SET('a','b','c','d')", "'a,d'", "''", Number),
        col(
            "s64",
            &format!("SET({})", set64.join(",")),
            "'s0,s63'",
            "'s31,s32'",
            Number,
        ),
        col(
            "g",
            "GEOMETRY",
            "ST_GeomFromText('POINT(1 2)', 4326)",
            "ST_GeomFromText('LINESTRING(0 0,1 1,2 2)')",
            Hex,
        ),
        col("pt", "POINT", "POINT(1.5, -2.5)", "POINT(0, 0)", Hex),
        col(
            "pg",
            "POLYGON",
            "ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 0))')",
            "ST_GeomFromText('POLYGON((0 0,2 0,2 2,0 0))')",
            Hex,
        ),
        col(
            "mpt",
            "MULTIPOINT",
            "ST_GeomFromText('MULTIPOINT(1 1,2 2)')",
            "ST_GeomFromText('MULTIPOINT(3 3)')",
            Hex,
        ),
        col(
            "gc",
            "GEOMETRYCOLLECTION",
            "ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 1))')",
            "ST_GeomFromText('GEOMETRYCOLLECTION(POINT(2 2))')",
            Hex,
        ),
        col(
            "u",
            "UUID",
            "'12345678-9abc-1def-8123-456789abcdef'",
            "'00000000-0000-4000-8000-000000000001'",
            Uuid,
        ),
        col("i4", "INET4", "'10.1.2.3'", "'255.255.255.255'", Inet4),
        col("i6", "INET6", "'2001:db8::1'", "'::ffff:1.2.3.4'", Inet6),
        col(
            "vec",
            "VECTOR(3)",
            "VEC_FromText('[1.5,2,-3.25]')",
            "VEC_FromText('[0,0,0.5]')",
            Vector,
        ),
        col(
            "cvc",
            "VARCHAR(500) COMPRESSED",
            "REPEAT('ab', 250)",
            "'short'",
            Hex,
        ),
        col("ctx", "TEXT COMPRESSED", "REPEAT('cd', 2000)", "'x'", Hex),
        col(
            "cbl",
            "BLOB COMPRESSED",
            "REPEAT(x'1234', 500)",
            "x'01'",
            Hex,
        ),
    ]
}

fn sql_expr(c: &Col) -> String {
    let n = c.name;
    match c.render {
        Render::Text | Render::Uuid | Render::Inet4 | Render::Inet6 => {
            format!("CAST({n} AS CHAR)")
        }
        Render::Number => format!("CAST(CAST({n} + 0 AS UNSIGNED) AS CHAR)"),
        Render::Hex => format!("HEX({n})"),
        Render::Vector => format!("VEC_ToText({n})"),
    }
}

/// Renders a decoded value as MariaDB renders `sql_expr`.
fn render(v: &Value, r: Render) -> Option<String> {
    Some(match (v, r) {
        (Value::Null, _) => return None,
        (Value::Int(i), _) => i.to_string(),
        (Value::UInt(u), _) => u.to_string(),
        (Value::Decimal(s), _) => s.clone(),
        (Value::Float(f), _) => format!("{f:?}"),
        (Value::Double(d), _) => format!("{d:?}"),
        (Value::Year(y), _) => format!("{y:04}"),
        (Value::Date(d), _) => d.to_string(),
        (Value::Time(t), _) => t.to_string(),
        (Value::DateTime(d), _) => d.to_string(),
        (Value::Timestamp(t), _) => t.to_string(),
        (Value::Bit(b), _) => b.to_string(),
        (Value::Enum(e), _) => e.to_string(),
        (Value::Set(s), _) => s.to_string(),
        (v, Render::Uuid) => uuid_text(v.as_bytes()?)?,
        (v, Render::Inet4) => inet4_text(v.as_bytes()?)?,
        (v, Render::Inet6) => inet6_text(v.as_bytes()?)?,
        (v, Render::Vector) => {
            let f = vector_values(v.as_bytes()?)?;
            format!(
                "[{}]",
                f.iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
        (v, _) => hex(v.as_bytes()?),
    })
}

fn check_image(img: &RowImage, cols: &[Col], want: &[Option<String>], what: &str) {
    // Column 0 is the id.
    for (k, c) in cols.iter().enumerate() {
        let v = img
            .get(k + 1)
            .unwrap_or_else(|| panic!("{what}: {} absent", c.name));
        let got = render(v, c.render);
        assert_eq!(
            got,
            want[k + 1],
            "{what}: column {} ({}) decoded as {v:?}",
            c.name,
            c.ddl
        );
    }
}

async fn round_trip(url: &str) {
    let schema = schema_name();
    let cols = columns();
    let mut c = sql(url).await;
    c.query_drop(format!("CREATE DATABASE {schema}"))
        .await
        .unwrap();
    let ddl = cols
        .iter()
        .map(|c| format!("{} {}", c.name, c.ddl))
        .collect::<Vec<_>>()
        .join(", ");
    c.query_drop(format!(
        "CREATE TABLE {schema}.t (id INT PRIMARY KEY, {ddl})"
    ))
    .await
    .unwrap();
    let from = gtid_pos(&mut c).await;
    let names = cols.iter().map(|c| c.name).collect::<Vec<_>>().join(", ");
    for (id, k) in [(1, 0), (2, 1)] {
        let vals = cols
            .iter()
            .map(|c| c.vals[k].as_str())
            .collect::<Vec<_>>()
            .join(", ");
        c.query_drop(format!(
            "INSERT INTO {schema}.t (id, {names}) VALUES ({id}, {vals})"
        ))
        .await
        .unwrap();
    }
    c.query_drop(format!("INSERT INTO {schema}.t (id) VALUES (3)"))
        .await
        .unwrap();
    let exprs = cols.iter().map(sql_expr).collect::<Vec<_>>().join(", ");
    let mut want: Vec<Vec<Option<String>>> = Vec::new();
    for id in 1..=3 {
        let row: mysql_async::Row = c
            .query_first(format!(
                "SELECT CAST(id AS CHAR), {exprs} FROM {schema}.t WHERE id = {id}"
            ))
            .await
            .unwrap()
            .unwrap();
        want.push(
            (0..row.len())
                .map(|i| row.get::<Option<String>, _>(i).unwrap())
                .collect(),
        );
    }
    // MariaDB can't read the table it updates in a subquery; copy row 2
    // through a derived table instead.
    let assigns = cols
        .iter()
        .map(|c| format!("a.{n} = b.{n}", n = c.name))
        .collect::<Vec<_>>()
        .join(", ");
    c.query_drop(format!(
        "UPDATE {schema}.t a JOIN (SELECT * FROM {schema}.t WHERE id = 2) b SET {assigns} WHERE a.id = 1"
    ))
    .await
    .unwrap();
    c.query_drop(format!("DELETE FROM {schema}.t WHERE id = 2"))
        .await
        .unwrap();
    let to = gtid_pos(&mut c).await;

    let events = read_range(url, &from, &to, &common::opts(&schema)).await;
    let mut inserts = Vec::new();
    let mut updates = Vec::new();
    let mut deletes = Vec::new();
    for ev in &events {
        if let Event::Rows {
            kind, rows, table, ..
        } = ev
        {
            assert_eq!(table.schema, schema);
            assert_eq!(table.columns.len(), cols.len() + 1);
            for (k, c) in cols.iter().enumerate() {
                assert_eq!(table.columns[k + 1].name.as_deref(), Some(c.name));
            }
            match kind {
                RowsKind::Insert => inserts.extend(rows.iter().cloned()),
                RowsKind::Update => updates.extend(rows.iter().cloned()),
                RowsKind::Delete => deletes.extend(rows.iter().cloned()),
            }
        }
    }
    assert_eq!((inserts.len(), updates.len(), deletes.len()), (3, 1, 1));
    for (i, r) in inserts.iter().enumerate() {
        check_image(
            r.after.as_ref().unwrap(),
            &cols,
            &want[i],
            &format!("insert {}", i + 1),
        );
    }
    check_image(
        updates[0].before.as_ref().unwrap(),
        &cols,
        &want[0],
        "update before",
    );
    check_image(
        updates[0].after.as_ref().unwrap(),
        &cols,
        &want[1],
        "update after",
    );
    check_image(
        deletes[0].before.as_ref().unwrap(),
        &cols,
        &want[1],
        "delete before",
    );
    eprintln!(
        "{url}: {} columns, {} events decoded and matched",
        cols.len(),
        events.len()
    );
    drop_schema(url, &schema).await;
}

#[tokio::test]
async fn every_column_type_round_trips_on_the_single_node() {
    let Some(u) = urls(common::SINGLE, module_path!()) else {
        return;
    };
    round_trip(&u[0]).await;
}

#[tokio::test]
async fn every_column_type_round_trips_on_scratch_servers() {
    let Some(us) = urls(common::SCRATCH, module_path!()) else {
        return;
    };
    for u in us {
        round_trip(&u).await;
    }
}

/// `binlog_row_image = MINIMAL` and `NOBLOB` (session settings): absent
/// columns decode as `None`, present ones correctly.
#[tokio::test]
async fn minimal_and_noblob_images() {
    let Some(u) = urls(common::SINGLE, module_path!()) else {
        return;
    };
    let url = &u[0];
    let schema = schema_name();
    let mut c = sql(url).await;
    c.query_drop(format!("CREATE DATABASE {schema}"))
        .await
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {schema}.t (id INT PRIMARY KEY, a INT, b BLOB, s VARCHAR(10))"
    ))
    .await
    .unwrap();
    c.query_drop(format!("INSERT INTO {schema}.t VALUES (1, 10, x'01', 'x')"))
        .await
        .unwrap();
    let from = gtid_pos(&mut c).await;
    c.query_drop("SET SESSION binlog_row_image = MINIMAL")
        .await
        .unwrap();
    c.query_drop(format!("UPDATE {schema}.t SET a = 11 WHERE id = 1"))
        .await
        .unwrap();
    c.query_drop("SET SESSION binlog_row_image = NOBLOB")
        .await
        .unwrap();
    c.query_drop(format!("UPDATE {schema}.t SET s = 'y' WHERE id = 1"))
        .await
        .unwrap();
    let to = gtid_pos(&mut c).await;
    let events = read_range(url, &from, &to, &opts(&schema)).await;
    let ups: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            Event::Rows {
                rows,
                kind: RowsKind::Update,
                ..
            } => Some(rows[0].clone()),
            _ => None,
        })
        .collect();
    assert_eq!(ups.len(), 2);
    let (b, a) = (
        ups[0].before.as_ref().unwrap(),
        ups[0].after.as_ref().unwrap(),
    );
    assert_eq!(b.0, vec![Some(Value::Int(1)), None, None, None]);
    assert_eq!(a.0, vec![None, Some(Value::Int(11)), None, None]);
    let (b, a) = (
        ups[1].before.as_ref().unwrap(),
        ups[1].after.as_ref().unwrap(),
    );
    assert_eq!(b.0[2], None, "NOBLOB omits the unchanged blob");
    assert_eq!(b.get(3), Some(&Value::String(b"x".to_vec())));
    assert_eq!(a.get(3), Some(&Value::String(b"y".to_vec())));
    drop_schema(url, &schema).await;
}
