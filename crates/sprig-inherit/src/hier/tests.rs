use super::*;
use crate::grammar::{Stmt, classify, is_extension};
use crate::model::fixtures;
use sprig_sql::lex::LexOptions;

fn tree() -> InheritableObject {
    let mut o = fixtures::products();
    let cols: Vec<String> = o
        .columns
        .iter()
        .cloned()
        .chain(PATH_COLUMNS.iter().map(|c| (*c).to_owned()))
        .collect();
    o = o.with_slots_for(&cols).expect("new slots");
    o
}

#[test]
fn path_columns_are_fields_with_their_own_bits() {
    let o = tree();
    assert!(has_paths(&o));
    let pb = path_bits(&o);
    assert_eq!(pb.count_ones(), 3);
    assert_eq!(pb & o.bit("name"), 0);
    assert_eq!(pb & o.bit("price"), 0);
    let plain = without_paths(&o);
    assert!(!has_paths(&plain));
    assert_eq!(plain.columns.len(), o.columns.len() - 3);
    // Slots stay: the overlay of a trimmed view reads the same bits.
    assert_eq!(plain.bit("price"), o.bit("price"));
}

#[test]
fn reconcile_statement_parses() {
    let l = LexOptions::mariadb();
    let c = |sql: &str| classify(sql.as_bytes(), &l).unwrap();
    match c("ALTER TABLE `shop`.`cats` SPRIG RECONCILE HIERARCHY CHANGED (1, 'a,b', -3, X'0A')") {
        Stmt::Reconcile { table, change } => {
            assert_eq!(table.db.as_deref(), Some("shop"));
            assert_eq!(table.name, "cats");
            assert_eq!(
                change,
                HierChange::Changed(vec![
                    "1".into(),
                    "'a,b'".into(),
                    "-3".into(),
                    "X'0A'".into()
                ])
            );
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        c("ALTER TABLE cats SPRIG RECONCILE HIERARCHY LOCK"),
        Stmt::Reconcile {
            change: HierChange::Lock,
            ..
        }
    ));
    assert!(matches!(
        c("ALTER TABLE cats SPRIG RECONCILE HIERARCHY ADDED"),
        Stmt::Reconcile {
            change: HierChange::Added,
            ..
        }
    ));
    assert!(matches!(
        c("ALTER TABLE cats SPRIG RECONCILE HIERARCHY DELETED (7)"),
        Stmt::Reconcile {
            change: HierChange::Deleted(k),
            ..
        } if k == vec!["7".to_owned()]
    ));
    assert!(classify(b"ALTER TABLE cats SPRIG RECONCILE HIERARCHY MOVED (1)", &l).is_err());
    assert!(classify(b"ALTER TABLE cats SPRIG RECONCILE HIERARCHY CHANGED 1", &l).is_err());
    assert!(is_extension(
        b"ALTER TABLE cats SPRIG RECONCILE HIERARCHY LOCK",
        &l
    ));
    // What chapter 32 sends round-trips.
    let ch = HierChange::Deleted(vec!["'x'".into(), "2".into()]);
    let sql = format!("ALTER TABLE t SPRIG RECONCILE HIERARCHY {}", ch.clause());
    assert!(matches!(c(&sql), Stmt::Reconcile { change, .. } if change == ch));
}

#[test]
fn only_path_columns_pass_for_type_plans() {
    let l = LexOptions::mariadb();
    assert!(names_only_paths(
        "UPDATE t SET _sprig_hpath = CONCAT(X'2F', SUBSTRING(_sprig_hpath, 3)), \
         _sprig_hdepth = _sprig_hdepth + 1, _sprig_rk = 4 WHERE _sprig_hpath >= X'2F'",
        &l
    ));
    assert!(!names_only_paths("SELECT _sprig_space FROM t", &l));
    assert!(!names_only_paths("SELECT * FROM __sprig_share_4", &l));
}

#[test]
fn shares_of_trees_stay_inside_the_tenant_and_whole() {
    let o = tree();
    assert!(check_share(&o, false, false, false).is_ok());
    assert!(check_share(&o, true, false, false).is_err());
    assert!(check_share(&o, false, true, false).is_err());
    assert!(check_share(&o, false, false, true).is_err());
    let plain = fixtures::products();
    assert!(check_share(&plain, true, true, true).is_ok());
}

#[test]
fn nodes_read_keys_parents_and_paths() {
    use sprig_proto_mysql::ColumnDef;
    use sprig_proto_mysql::resultset::column_type as ct;
    let col = |t: u8| ColumnDef {
        col_type: t,
        ..ColumnDef::default()
    };
    let rows = Rows {
        columns: vec![
            col(ct::LONGLONG),
            col(ct::LONGLONG),
            col(ct::VAR_STRING),
            col(ct::SHORT),
            col(ct::LONGLONG),
        ],
        rows: vec![
            vec![
                Some(b"7".to_vec()),
                Some(b"1".to_vec()),
                Some(b"/1/7/".to_vec()),
                Some(b"1".to_vec()),
                Some(b"1".to_vec()),
            ],
            vec![Some(b"1".to_vec()), None, None, None, None],
        ],
    };
    let n = nodes(&rows, false);
    assert_eq!(n[0].key, "7");
    assert_eq!(n[0].parent.as_deref(), Some("1"));
    assert_eq!(n[0].path.as_deref(), Some(&b"/1/7/"[..]));
    assert_eq!(n[0].depth, Some(1));
    assert_eq!(n[0].rk.as_deref(), Some("1"));
    assert!(n[1].parent.is_none() && n[1].path.is_none() && n[1].rk.is_none());
}
