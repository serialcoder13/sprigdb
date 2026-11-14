//! The leak suite's helpers: no physical name (`t<digits>_...`) may reach a
//! client in any byte it sees: column definitions, values, messages.

use sprig_proto_mysql::ColumnDef;
use sprig_sql::names::{OutputCtx, TextRow, map_message, physical_name_leaks};

/// Fails if `bytes` hold a physical name.
pub fn assert_clean(label: &str, bytes: &[u8]) {
    let leaks = physical_name_leaks(bytes);
    assert!(
        leaks.is_empty(),
        "{label}: physical names {leaks:?} in {:?}",
        String::from_utf8_lossy(bytes)
    );
}

/// Fails if any field of a column definition or any value of a row holds
/// a physical name.
pub fn assert_result_clean(label: &str, defs: &[ColumnDef], rows: &[TextRow]) {
    for d in defs {
        for f in [&d.schema, &d.table, &d.org_table, &d.name, &d.org_name] {
            assert_clean(label, f);
        }
    }
    for r in rows {
        for v in r.iter().flatten() {
            assert_clean(label, v);
        }
    }
}

/// The captured error messages (`tests/fixtures/names/errors.json`):
/// `(statement, errno, message)`.
pub fn captured_errors() -> Vec<(String, u64, String)> {
    let text =
        std::fs::read_to_string(super::fixture::dir().join("errors.json")).unwrap_or_default();
    let v: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
    v.as_array()
        .map(|a| {
            a.iter()
                .map(|e| {
                    (
                        e["sql"].as_str().unwrap_or_default().to_owned(),
                        e["errno"].as_u64().unwrap_or_default(),
                        e["message"].as_str().unwrap_or_default().to_owned(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Maps every captured message and checks it is clean; returns the
/// mapped messages.
pub fn check_messages(ctx: &OutputCtx<'_>) -> Vec<String> {
    captured_errors()
        .into_iter()
        .map(|(sql, _, msg)| {
            let m = map_message(&msg, ctx).into_owned();
            assert_clean(&sql, m.as_bytes());
            m
        })
        .collect()
}
