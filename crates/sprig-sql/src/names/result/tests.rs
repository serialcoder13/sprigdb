use sprig_common::ids::TenantId;
use sprig_proto_mysql::packets::Persona;

use super::*;
use crate::names::MemoryNameMap;

fn acme() -> MemoryNameMap {
    MemoryNameMap::new(TenantId(42)).with_db(1, "billing", 1, 1)
}

fn def(name: &str) -> ColumnDef {
    ColumnDef {
        schema: Bytes::new(),
        table: Bytes::new(),
        org_table: Bytes::new(),
        name: Bytes::from(name.to_owned()),
        org_name: Bytes::new(),
        ext_type: None,
        charset: 224,
        length: 256,
        col_type: 253,
        flags: 0,
        decimals: 0,
    }
}

fn row(vals: &[&str]) -> TextRow {
    vals.iter()
        .map(|v| (*v != "NULL").then(|| Bytes::from((*v).to_owned())))
        .collect()
}

fn strings(rows: &[TextRow]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| {
            r.iter()
                .map(|v| {
                    v.as_deref()
                        .map_or("NULL".into(), |b| String::from_utf8_lossy(b).into_owned())
                })
                .collect()
        })
        .collect()
}

#[test]
fn relay_maps_only_definitions() {
    let m = acme();
    let ctx = OutputCtx::new(&m);
    let mut r = ResultMapper::relay();
    assert!(!r.needs_rows());
    let mut defs = vec![ColumnDef {
        schema: Bytes::from_static(b"t42_d1_g1_s1"),
        ..def("x")
    }];
    let rows = r.map_all(&mut defs, vec![row(&["t42_d1_g1_s1"])], &ctx);
    assert_eq!(&defs[0].schema[..], b"billing");
    assert_eq!(
        strings(&rows),
        [["t42_d1_g1_s1"]],
        "relay never touches rows"
    );
}

#[test]
fn scrub_maps_every_value() {
    let m = acme();
    let ctx = OutputCtx::new(&m);
    let mut r = ResultMapper::scrub();
    assert!(r.needs_rows());
    let mut defs = vec![def("a"), def("b")];
    let rows = r.map_all(
        &mut defs,
        vec![
            row(&["t42_d1_g1_s1.orders", "NULL"]),
            row(&["t7_sprig", "x"]),
        ],
        &ctx,
    );
    assert_eq!(strings(&rows), [["billing.orders", "NULL"], ["?", "x"]]);
    assert_eq!(r.foreign_names(), 1);
}

#[test]
fn index_groups_are_held_until_complete() {
    let m = acme();
    let ctx = OutputCtx::new(&m);
    let mut r = ResultMapper::show(ShowKind::Index, Visibility::for_persona(Persona::MariaDb));
    let mut defs = vec![def("Table"), def("Key_name"), def("Column_name")];
    r.column_defs(&mut defs, &ctx);
    let mut out = Vec::new();
    r.row(row(&["t", "k1", "a"]), &ctx, &mut out);
    assert!(out.is_empty(), "held");
    r.row(row(&["t", "k1", "b"]), &ctx, &mut out);
    r.row(row(&["t", "k2", "a"]), &ctx, &mut out);
    assert_eq!(out.len(), 2, "k1 flushed when k2 starts");
    r.row(row(&["t", "k2", "_sprig_x"]), &ctx, &mut out);
    r.row(row(&["u", "k2", "a"]), &ctx, &mut out);
    assert_eq!(out.len(), 2, "k2 of t dropped");
    r.finish(&ctx, &mut out);
    assert_eq!(strings(&out[2..]), [["u", "k2", "a"]]);
    assert_eq!(r.hidden_rows(), 2);
}

#[test]
fn row_packets_round_trip() {
    let m = acme();
    let ctx = OutputCtx::new(&m);
    let mut r = ResultMapper::show(ShowKind::Tables, Visibility::for_persona(Persona::MariaDb));
    let mut defs = vec![def("Tables_in_t42_d1_g1_s1")];
    r.column_defs(&mut defs, &ctx);
    assert_eq!(&defs[0].name[..], b"Tables_in_billing");
    let mut out = Vec::new();
    for v in ["orders", "__sprig_x"] {
        let p = encode_text_row([Some(v.as_bytes())]);
        r.row_packet(&p, 1, &ctx, &mut out).unwrap();
    }
    r.finish_packets(&ctx, &mut out);
    assert_eq!(out.len(), 1);
    assert_eq!(decode_text_row(&out[0], 1).unwrap(), [Some(&b"orders"[..])]);
    assert!(
        r.row_packet(&Bytes::from_static(b"\xfc"), 1, &ctx, &mut out)
            .is_err()
    );
}

#[test]
fn fixed_columns_and_text_charset() {
    let m = acme();
    let ctx = OutputCtx::new(&m);
    let mut r = ResultMapper::scrub().with_columns(vec![def("Database")]);
    let mut defs = vec![def("x")];
    r.column_defs(&mut defs, &ctx);
    assert_eq!(&defs[0].name[..], b"Database");
    let mut a = LocalAnswer {
        columns: vec![
            def("a"),
            ColumnDef {
                charset: 63,
                ..def("n")
            },
        ],
        rows: vec![],
    };
    a.set_text_charset(45);
    assert_eq!((a.columns[0].charset, a.columns[1].charset), (45, 63));
}
