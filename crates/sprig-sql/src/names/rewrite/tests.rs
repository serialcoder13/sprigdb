use sprig_common::ids::TenantId;
use sprig_proto_mysql::packets::Persona;

use super::*;
use crate::names::MemoryNameMap;

#[test]
fn edits_apply_in_order() {
    let e = |r: Range<usize>, s: &str| Edit {
        range: r,
        kind: EditKind::Database,
        replacement: s.to_owned(),
    };
    assert_eq!(
        apply_edits(b"abcdef", &[e(0..1, "X"), e(3..5, "")]),
        b"Xbcf"
    );
    assert_eq!(apply_edits(b"abc", &[]), b"abc");
    assert_eq!(apply_edits(b"abc", &[e(3..3, "!")]), b"abc!");
}

#[test]
fn deep_nesting_fails_closed_without_overflowing() {
    let m = MemoryNameMap::new(TenantId(1)).with_db(1, "d", 1, 1);
    let ctx = RewriteCtx::new(&m, Persona::MySql84);
    let deep = format!("SELECT {}1{}", "(SELECT ".repeat(5000), ")".repeat(5000));
    assert_eq!(rewrite(deep.as_bytes(), &ctx).unwrap_err().errno(), 39051);
    let deep = format!("SELECT * FROM {}d.t{}", "(".repeat(5000), ")".repeat(5000));
    assert_eq!(rewrite(deep.as_bytes(), &ctx).unwrap_err().errno(), 39051);
    let deep = format!("BEGIN NOT ATOMIC {} END", "BEGIN ".repeat(5000));
    assert_eq!(rewrite(deep.as_bytes(), &ctx).unwrap_err().errno(), 39051);
    // Deep but bounded is fine.
    let ok = format!(
        "SELECT {}1{} FROM d.t",
        "(SELECT ".repeat(40),
        ")".repeat(40)
    );
    assert!(rewrite(ok.as_bytes(), &ctx).is_ok());
}

#[test]
fn deep_statements_never_overflow_the_ast_path() {
    let m = MemoryNameMap::new(TenantId(1)).with_db(1, "d", 1, 1);
    let ctx = RewriteCtx::new(&m, Persona::MySql84);
    let cases = [
        format!(
            "SELECT {}1{} FROM d.t",
            "(SELECT ".repeat(12),
            ")".repeat(12)
        ),
        format!("SELECT {}1{} FROM d.t", "(".repeat(150), ")".repeat(150)),
        format!("SELECT {}x FROM d.t", "NOT ".repeat(150)),
        format!("SELECT {}x FROM d.t", "- ".repeat(150)),
        format!(
            "SELECT {}1{} FROM d.t",
            "CASE WHEN a THEN ".repeat(150),
            " END".repeat(150)
        ),
        format!(
            "SELECT * FROM d.t WHERE {}",
            vec!["a = 1"; 2000].join(" AND ")
        ),
        format!(
            "SELECT * FROM d.t WHERE {}",
            vec!["a = 1"; 98].join(" AND ")
        ),
        format!("SELECT * FROM {}", vec!["d.t"; 500].join(" JOIN ")),
    ];
    for c in cases {
        let _ = rewrite(c.as_bytes(), &ctx);
    }
}
