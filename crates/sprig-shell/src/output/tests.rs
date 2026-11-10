//! Golden outputs per format (chapter 53, "Testing"). Set
//! `SPRIG_UPDATE_GOLDEN=1` to rewrite them after a deliberate change.

use super::*;

fn col(name: &str, ty: &str, binary: bool) -> Column {
    Column {
        name: name.into(),
        type_name: ty.into(),
        binary,
    }
}

#[allow(clippy::unnecessary_wraps)]
fn s(x: &str) -> Cell {
    Some(x.to_owned())
}

/// Two results, a write, and a document: NULLs, binary cells, wide text,
/// quotes, tabs and newlines, a duplicate column name, a truncated result.
fn stream() -> Vec<ResultEvent> {
    vec![
        ResultEvent::Header(vec![
            col("id", "BIGINT", false),
            col("name", "VARCHAR", false),
            col("note", "TEXT", false),
            col("raw", "BLOB", true),
            col("name", "VARCHAR", false),
        ]),
        ResultEvent::Rows(vec![
            vec![s("1"), s("Asha"), None, s("AAH/"), s("x")],
            vec![
                s("22"),
                s("Zoë, \"the\" 2nd"),
                s("line1\nline2\ttab"),
                None,
                s(""),
            ],
        ]),
        ResultEvent::Rows(vec![vec![s("333"), s(""), s("back\\slash"), s(""), None]]),
        ResultEvent::End(EndInfo {
            warnings: 1,
            ..EndInfo::default()
        }),
        ResultEvent::End(EndInfo {
            affected: 3,
            last_insert_id: 41,
            info: "Records: 3  Duplicates: 0  Warnings: 0".into(),
            ..EndInfo::default()
        }),
        ResultEvent::Header(vec![col("n", "INT", false)]),
        ResultEvent::Rows(vec![vec![s("7")]]),
        ResultEvent::End(EndInfo {
            truncated: true,
            ..EndInfo::default()
        }),
        ResultEvent::Doc(json!({"_id": {"$oid": "65a1"}, "tags": ["a", "b"]})),
        ResultEvent::End(EndInfo::default()),
    ]
}

fn render(f: Format) -> String {
    let mut w = Writer::new(Vec::new(), f);
    for ev in stream() {
        w.event(ev).expect("write");
    }
    w.finish().expect("finish");
    String::from_utf8(w.into_inner()).expect("utf8")
}

fn golden(name: &str, got: &str) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/output/golden")
        .join(name);
    if std::env::var_os("SPRIG_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, got).expect("write golden");
        return;
    }
    let want = std::fs::read_to_string(&path).unwrap_or_default();
    assert_eq!(
        got,
        want,
        "{} differs; SPRIG_UPDATE_GOLDEN=1 rewrites it",
        path.display()
    );
}

#[test]
fn table() {
    golden("table.txt", &render(Format::Table));
}

#[test]
fn json() {
    let out = render(Format::Json);
    // Each result is one JSON value.
    for v in serde_json::Deserializer::from_str(&out).into_iter::<Value>() {
        v.expect("valid JSON");
    }
    golden("json.txt", &out);
}

#[test]
fn ndjson() {
    let out = render(Format::Ndjson);
    for l in out.lines() {
        serde_json::from_str::<Value>(l).expect("one object per line");
    }
    golden("ndjson.txt", &out);
}

#[test]
fn csv() {
    golden("csv.txt", &render(Format::Csv));
}

#[test]
fn tsv() {
    golden("tsv.txt", &render(Format::Tsv));
}

#[test]
fn formats_parse() {
    assert_eq!(Format::parse("jsonl"), Ok(Format::Ndjson));
    assert!(Format::parse("xml").is_err());
}
