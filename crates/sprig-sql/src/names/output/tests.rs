use bytes::Bytes;
use sprig_common::ids::TenantId;

use super::*;
use crate::names::MemoryNameMap;

fn acme() -> MemoryNameMap {
    MemoryNameMap::new(TenantId(42))
        .with_db(1, "billing", 1, 1)
        .with_db(2, "a`b", 2, 2)
}

struct Users;

impl AccountResolver for Users {
    fn account_name(&self, a: &AccountName) -> Option<String> {
        match a.to_string().as_str() {
            "t42_u7" => Some("alice".into()),
            "t42_r3" => Some("auditors".into()),
            _ => None,
        }
    }
}

#[test]
fn messages_get_logical_names_quoted_the_same_way() {
    let m = acme();
    let ctx = OutputCtx::new(&m);
    assert_eq!(
        map_message("Table 't42_d1_g1_s1.orders' doesn't exist", &ctx),
        "Table 'billing.orders' doesn't exist"
    );
    assert_eq!(
        map_message("fails (`t42_d1_g1_s1`.`orders`, CONSTRAINT x)", &ctx),
        "fails (`billing`.`orders`, CONSTRAINT x)"
    );
    // A backquote in the logical name is doubled inside backquotes only.
    assert_eq!(map_message("`t42_d2_g2_s2`.t", &ctx), "`a``b`.t");
    assert_eq!(map_message("'t42_d2_g2_s2.t'", &ctx), "'a`b.t'");
    // Untouched text is borrowed.
    assert!(matches!(
        map_message("no names here", &ctx),
        Cow::Borrowed(_)
    ));
    // Not a physical name: a run that only starts like one.
    assert_eq!(
        map_message("t42_d1_g1_s1x t42_x mt42_d1_g1_s1", &ctx),
        "t42_d1_g1_s1x t42_x mt42_d1_g1_s1"
    );
}

#[test]
fn foreign_names_become_question_marks_and_are_counted() {
    let m = acme();
    let ctx = OutputCtx::new(&m);
    let (t, n) = map_text(
        "t43_d1_g1_s1.x and t42_sprig.y and t42_d9_g1_s1.z and t42_d1_ref",
        &ctx,
    );
    assert_eq!(
        t, "?.x and ?.y and ?.z and billing",
        "a reference schema is its database's"
    );
    assert_eq!(n, 3);
}

#[test]
fn accounts_map_through_the_resolver() {
    let m = acme();
    let ctx = OutputCtx::new(&m).accounts(&Users);
    assert_eq!(
        map_message("Access denied for user 't42_u7'@'10.0.0.1'", &ctx),
        "Access denied for user 'alice'@'10.0.0.1'"
    );
    assert_eq!(
        map_message("DEFINER=`t42_r3`@`%`", &ctx),
        "DEFINER=`auditors`@`%`"
    );
    let (t, n) = map_text("t42_u8@% t43_u7@%", &ctx);
    assert_eq!((t.as_ref(), n), ("?@% ?@%", 2));
    let (t, n) = map_text("t42_u7@%", &OutputCtx::new(&m));
    assert_eq!((t.as_ref(), n), ("?@%", 1), "no resolver");
}

#[test]
fn internal_tables_in_messages_name_their_owner() {
    let m = acme();
    let ctx = OutputCtx::new(&m);
    assert_eq!(
        map_message(
            "Duplicate entry 'a' for key 'PRIMARY' in __sprig_lk_orders__email",
            &ctx
        ),
        "Duplicate entry 'a' for key 'PRIMARY' in orders"
    );
    assert_eq!(map_message("__sprig_wx_events", &ctx), "events");
    assert_eq!(map_message("__sprig_ix_docs__2", &ctx), "docs");
    assert_eq!(map_message("__sprig_dtx", &ctx), "__sprig_dtx");
    // Values keep internal names (they are shown under @sprig_show_internal).
    assert_eq!(
        map_bytes(b"__sprig_lk_orders__email", &ctx).text.as_ref(),
        b"__sprig_lk_orders__email"
    );
}

#[test]
fn tables_in_headers() {
    let m = acme();
    let ctx = OutputCtx::new(&m);
    assert_eq!(
        map_message("Tables_in_t42_d1_g1_s1 (o%)", &ctx),
        "Tables_in_billing (o%)"
    );
    assert_eq!(map_message("Tables_in_t43_d1_g1_s1", &ctx), "Tables_in_?");
}

#[test]
fn column_definitions() {
    let m = acme();
    let ctx = OutputCtx::new(&m);
    let mut cd = ColumnDef {
        schema: Bytes::from_static(b"t42_d1_g1_s1"),
        table: Bytes::from_static(b"o"),
        org_table: Bytes::from_static(b"orders"),
        name: Bytes::from_static(b"Tables_in_t42_d1_g1_s1"),
        org_name: Bytes::from_static(b"TABLE_NAME"),
        ext_type: None,
        charset: 45,
        length: 10,
        col_type: 253,
        flags: 0,
        decimals: 0,
    };
    assert_eq!(map_column_def(&mut cd, &ctx), 0);
    assert_eq!(&cd.schema[..], b"billing");
    assert_eq!(&cd.name[..], b"Tables_in_billing");
    assert_eq!(&cd.org_table[..], b"orders");
    let mut cd2 = ColumnDef {
        schema: Bytes::from_static(b"t43_d1_g1_s1"),
        ..cd.clone()
    };
    assert_eq!(map_column_def(&mut cd2, &ctx), 1);
    assert!(cd2.schema.is_empty());
    let mut cd3 = ColumnDef {
        schema: Bytes::from_static(b"information_schema"),
        ..cd
    };
    map_column_def(&mut cd3, &ctx);
    assert_eq!(&cd3.schema[..], b"information_schema");
}

#[test]
fn leak_detector() {
    assert_eq!(physical_name_leaks(b"x t42_d1_g1_s1.y"), ["t42_d1_g1_s1"]);
    assert_eq!(
        physical_name_leaks(b"Tables_in_t1_d1_g1_s1"),
        ["t1_d1_g1_s1"]
    );
    assert_eq!(physical_name_leaks(b"`t42_u7`@`%`"), ["t42_u7"]);
    assert!(physical_name_leaks(b"billing.orders t42 at1_x").is_empty());
}
