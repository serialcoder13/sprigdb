use sprig_catalog::ddl::{DdlStatement, ObjectName};
use sprig_catalog::store::ObjectKind;

use super::*;

fn q(db: &str, name: &str) -> DdlName {
    DdlName::qualified(db, name)
}

fn u(name: &str) -> DdlName {
    DdlName::unqualified(name, Some("billing"))
}

#[test]
fn names_keep_the_resolved_database() {
    assert_eq!(
        u("t").to_catalog(),
        ObjectName {
            db: Some("billing".into()),
            name: "t".into()
        }
    );
    assert_eq!(
        q("crm", "t").to_catalog(),
        ObjectName::qualified("crm", "t")
    );
    assert_eq!(
        DdlName::unqualified("t", None).to_catalog(),
        ObjectName::bare("t")
    );
    assert_eq!(u("t").to_string(), "t");
    assert_eq!(q("crm", "t").to_string(), "crm.t");
}

#[test]
fn every_kind_maps_to_the_catalog() {
    let kinds = [
        (DdlKind::Table, ObjectKind::Table),
        (DdlKind::View, ObjectKind::View),
        (DdlKind::Sequence, ObjectKind::Sequence),
        (DdlKind::Procedure, ObjectKind::Procedure),
        (DdlKind::Function, ObjectKind::Function),
        (DdlKind::Trigger, ObjectKind::Trigger),
        (DdlKind::Event, ObjectKind::Event),
    ];
    for (k, c) in kinds {
        assert_eq!(k.to_catalog(), c);
        assert_eq!(k.to_string(), k.as_str());
    }
}

#[test]
fn conversion() {
    let create = DdlTarget::CreateObject {
        kind: DdlKind::Trigger,
        name: q("crm", "trg"),
        if_not_exists: true,
        or_replace: false,
        temporary: false,
        parent: Some(u("orders")),
        like: None,
        as_select: false,
    };
    assert_eq!(
        create.to_catalog().unwrap(),
        Some(DdlStatement::CreateObject {
            kind: ObjectKind::Trigger,
            name: ObjectName::qualified("crm", "trg"),
            if_not_exists: true,
            or_replace: false,
            parent: Some(ObjectName::qualified("billing", "orders")),
        })
    );
    let rename = DdlTarget::RenameTables {
        pairs: vec![(u("a"), q("crm", "b"))],
        if_exists: false,
    };
    assert_eq!(
        rename.to_catalog().unwrap(),
        Some(DdlStatement::RenameTables {
            pairs: vec![(
                ObjectName::qualified("billing", "a"),
                ObjectName::qualified("crm", "b")
            )],
            if_exists: false,
        })
    );
    let index = DdlTarget::AlterObject {
        kind: DdlKind::Table,
        name: u("t"),
        if_exists: false,
        op: AlterOp::DropIndex {
            name: "i".into(),
            if_exists: true,
        },
    };
    assert_eq!(
        index.to_catalog().unwrap(),
        Some(DdlStatement::AlterObject {
            kind: ObjectKind::Table,
            name: ObjectName::qualified("billing", "t"),
            if_exists: false,
            rename_to: None,
        })
    );
    let drop = DdlTarget::DropObjects {
        kind: DdlKind::View,
        names: vec![u("a"), q("crm", "b")],
        if_exists: true,
        temporary: false,
    };
    assert!(matches!(
        drop.to_catalog().unwrap(),
        Some(DdlStatement::DropObjects { names, if_exists: true, .. }) if names.len() == 2
    ));
    let db = DdlTarget::CreateDatabase {
        name: "sales".into(),
        if_not_exists: false,
        or_replace: false,
        charset: Some("utf8mb4".into()),
        collation: None,
        other_options: Vec::new(),
    };
    assert_eq!(
        db.to_catalog().unwrap(),
        Some(DdlStatement::CreateDatabase {
            name: "sales".into(),
            if_not_exists: false,
            charset: Some("utf8mb4".into()),
            collation: None,
        })
    );
    assert_eq!(
        DdlTarget::Truncate { name: u("t") }.to_catalog().unwrap(),
        Some(DdlStatement::Truncate {
            name: ObjectName::qualified("billing", "t")
        })
    );
}

#[test]
fn temporary_objects_skip_the_engine() {
    let t = DdlTarget::CreateObject {
        kind: DdlKind::Table,
        name: u("tmp"),
        if_not_exists: false,
        or_replace: false,
        temporary: true,
        parent: None,
        like: None,
        as_select: true,
    };
    assert!(t.is_temporary() && !t.is_catalog());
    assert_eq!(t.to_catalog().unwrap(), None);
    let d = DdlTarget::DropObjects {
        kind: DdlKind::Table,
        names: vec![u("tmp")],
        if_exists: true,
        temporary: true,
    };
    assert_eq!(d.to_catalog().unwrap(), None);
}

#[test]
fn what_the_engine_cannot_do_fails_closed() {
    let replace_db = DdlTarget::CreateDatabase {
        name: "d".into(),
        if_not_exists: false,
        or_replace: true,
        charset: None,
        collation: None,
        other_options: Vec::new(),
    };
    assert_eq!(replace_db.to_catalog().unwrap_err().errno(), 39051);
    let comment = DdlTarget::AlterDatabase {
        name: Some("d".into()),
        explicit: true,
        charset: None,
        collation: None,
        other_options: vec!["COMMENT".into()],
    };
    assert_eq!(comment.to_catalog().unwrap_err().errno(), 39051);
    let no_db = DdlTarget::AlterDatabase {
        name: None,
        explicit: false,
        charset: Some("latin1".into()),
        collation: None,
        other_options: Vec::new(),
    };
    let e = no_db.to_catalog().unwrap_err();
    assert_eq!((e.errno(), e.sqlstate()), (1046, *b"3D000"));
    let alter_rename = DdlTarget::AlterObject {
        kind: DdlKind::Table,
        name: u("a"),
        if_exists: false,
        op: AlterOp::Change {
            rename_to: Some(u("b")),
        },
    };
    assert_eq!(
        alter_rename.to_catalog().unwrap(),
        Some(DdlStatement::AlterObject {
            kind: ObjectKind::Table,
            name: ObjectName::qualified("billing", "a"),
            if_exists: false,
            rename_to: Some(ObjectName::qualified("billing", "b")),
        })
    );
    let rename_if = DdlTarget::RenameTables {
        pairs: vec![(u("a"), u("b"))],
        if_exists: true,
    };
    assert!(matches!(
        rename_if.to_catalog().unwrap(),
        Some(DdlStatement::RenameTables {
            if_exists: true,
            ..
        })
    ));
}
