use super::*;

fn my() -> LexOptions {
    LexOptions::mysql84()
}

fn s(v: Option<Vec<u8>>) -> Option<String> {
    v.map(|b| String::from_utf8(b).unwrap())
}

const LONG: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[test]
fn long_names_get_mysqls_1059() {
    assert_eq!(LONG.len(), 65);
    for sql in [
        format!("CREATE TABLE {LONG} (id int)"),
        format!("create table if not exists d.`{LONG}` (id int)"),
        format!("CREATE OR REPLACE ALGORITHM=MERGE VIEW {LONG} AS SELECT 1"),
        format!("CREATE DATABASE {LONG}"),
        format!("CREATE DEFINER=CURRENT_USER PROCEDURE {LONG}() SELECT 1"),
        format!("RENAME TABLE a TO b, c TO {LONG}"),
        format!("ALTER TABLE t RENAME TO {LONG}"),
        format!("ALTER TABLE t ADD COLUMN x int, RENAME AS d.{LONG}"),
    ] {
        let e = long_name(sql.as_bytes(), &my()).unwrap_or_else(|| panic!("{sql}"));
        assert_eq!(e.code, 1059, "{sql}");
        assert_eq!(e.message, format!("Identifier name '{LONG}' is too long"));
    }
    // 64 characters fit; other renames and other statements pass.
    let ok = &LONG[1..];
    for sql in [
        format!("CREATE TABLE {ok} (id int)"),
        format!("ALTER TABLE t RENAME COLUMN a TO {LONG}"),
        format!("CREATE INDEX {LONG} ON t (a)"),
        format!("SELECT 1 AS {LONG}"),
        "CREATE TABLE t (id int)".to_owned(),
    ] {
        assert_eq!(long_name(sql.as_bytes(), &my()), None, "{sql}");
    }
    // 3307: MariaDB's 1103 for tables, its 1102 already for databases.
    let m = LexOptions::mariadb();
    let e = long_name(format!("CREATE TABLE {LONG} (id int)").as_bytes(), &m).unwrap();
    assert_eq!(
        (e.code, e.message.as_str()),
        (1103, format!("Incorrect table name '{LONG}'").as_str())
    );
    assert_eq!(
        long_name(format!("CREATE DATABASE {LONG}").as_bytes(), &m),
        None
    );
}

#[test]
fn descending_parts_on_myisam() {
    let e = engine_descending(
        b"CREATE TABLE my (a int, b int, KEY k (a DESC, b)) ENGINE=MyISAM",
        &my(),
    );
    assert_eq!(e.map(|e| e.code), Some(1178));
    assert!(
        engine_descending(
            b"CREATE TABLE `m` (`a` int, UNIQUE `u` (`b`, `a`(5) DESC)) ENGINE MEMORY",
            &my()
        )
        .is_some()
    );
    for ok in [
        "CREATE TABLE my (a int, KEY k (a DESC)) ENGINE=InnoDB",
        "CREATE TABLE my (a int, KEY k (a)) ENGINE=MyISAM",
        "CREATE TABLE my (a int, KEY k (a DESC))",
        "CREATE TABLE my (a int, `desc` int, CHECK (a > 0)) ENGINE=MyISAM",
    ] {
        assert_eq!(engine_descending(ok.as_bytes(), &my()), None, "{ok}");
    }
    assert_eq!(
        engine_descending(
            b"CREATE TABLE my (a int, KEY k (a DESC)) ENGINE=MyISAM",
            &LexOptions::mariadb()
        ),
        None
    );
    assert_eq!(
        descending_index_table(b"CREATE UNIQUE INDEX k2 ON d.my2 (a DESC)", &my()).as_deref(),
        Some("d.my2")
    );
    assert_eq!(
        descending_index_table(
            b"ALTER TABLE `models` ADD UNIQUE INDEX `a_b_uniq` USING BTREE (`fieldB`, `fieldA`(5) DESC)",
            &my()
        )
        .as_deref(),
        Some("`models`")
    );
    assert_eq!(
        descending_index_table(b"ALTER TABLE t ADD INDEX i (a)", &my()),
        None
    );
    assert_eq!(
        descending_index_table(b"SELECT a FROM t ORDER BY a DESC", &my()),
        None
    );
}

#[test]
fn now_defaults_are_kept_apart() {
    let r = |q: &str| s(now_defaults(q.as_bytes(), &my()));
    assert_eq!(
        r("CREATE TABLE t (a datetime DEFAULT (now()), b datetime(6) DEFAULT (NOW(6)), c timestamp DEFAULT (CURRENT_TIMESTAMP))")
            .as_deref(),
        Some("CREATE TABLE t (a datetime DEFAULT (localtimestamp()), b datetime(6) DEFAULT (localtimestamp(6)), c timestamp DEFAULT (localtimestamp()))")
    );
    assert_eq!(
        r("ALTER TABLE `testings` ALTER COLUMN `updated_at` SET DEFAULT (now())").as_deref(),
        Some("ALTER TABLE `testings` ALTER COLUMN `updated_at` SET DEFAULT (localtimestamp())")
    );
    assert_eq!(
        r("CREATE TABLE t (a datetime(3) DEFAULT LOCALTIME(3) ON UPDATE localtimestamp(3), b datetime DEFAULT localtimestamp)")
            .as_deref(),
        Some("CREATE TABLE t (a datetime(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3), b datetime DEFAULT CURRENT_TIMESTAMP)")
    );
    // The special default, other expressions and other statements pass.
    for q in [
        "CREATE TABLE t (a datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE NOW())",
        "CREATE TABLE t (a datetime DEFAULT now())",
        "CREATE TABLE t (a datetime DEFAULT (now() + interval 1 day))",
        "INSERT INTO t VALUES (now())",
    ] {
        assert_eq!(r(q), None, "{q}");
    }
    assert_eq!(
        now_defaults(
            b"CREATE TABLE t (a datetime DEFAULT (now()))",
            &LexOptions::mariadb()
        ),
        None
    );
}

fn part(index: &str, seq: u32, column: &str, unique: bool) -> KeyPart {
    KeyPart {
        index: index.to_owned(),
        seq,
        column: Some(column.to_owned()),
        sub_part: None,
        desc: false,
        unique,
        index_type: "BTREE".to_owned(),
        comment: String::new(),
        visible: true,
    }
}

#[test]
fn dropping_a_column_of_a_unique_index() {
    let sql = b"ALTER TABLE `test_models` DROP COLUMN `hat_size`";
    let (table, at, cols) = dropped_columns(sql, &my()).unwrap();
    assert_eq!(table, "`test_models`");
    assert_eq!(cols, vec!["hat_size".to_owned()]);
    let mut b = part("u", 2, "b", true);
    b.sub_part = Some(10);
    b.desc = true;
    b.comment = "it's".to_owned();
    let parts = vec![
        part("PRIMARY", 1, "id", true),
        part("index_hat", 1, "hat_style", true),
        part("index_hat", 2, "hat_size", true),
        part("k", 1, "hat_size", false),
        part("k", 2, "c", false),
        part("u", 1, "hat_size", true),
        b,
        part("solo", 1, "hat_size", true),
    ];
    let out = s(unique_rebuild(sql, at, &cols, &parts, false)).unwrap();
    assert_eq!(
        out,
        "ALTER TABLE `test_models` DROP INDEX `index_hat`, ADD UNIQUE INDEX `index_hat` (`hat_style`), \
         DROP INDEX `u`, ADD UNIQUE INDEX `u` (`b`(10) DESC) COMMENT 'it''s', DROP COLUMN `hat_size`"
    );
    // No unique index with another column: nothing to do.
    assert_eq!(unique_rebuild(sql, at, &cols, &parts[3..5], false), None);
    // Other drops aren't columns.
    assert_eq!(
        dropped_columns(b"ALTER TABLE t DROP INDEX i, DROP FOREIGN KEY f", &my()),
        None
    );
    let (_, _, cols) =
        dropped_columns(b"alter table d.t drop a, drop column if exists `b`", &my()).unwrap();
    assert_eq!(cols, vec!["a".to_owned(), "b".to_owned()]);
}

#[test]
fn foreign_keys_and_their_errors() {
    let f = fk_refs(
        b"CREATE TABLE fc (id int, p int, CONSTRAINT fk1 FOREIGN KEY (p) REFERENCES missing_t(id), \
          FOREIGN KEY (id) REFERENCES o.p (id))",
        &my(),
    )
    .unwrap();
    assert_eq!(f.table, "fc");
    assert_eq!(
        f.refs,
        vec![
            (None, "missing_t".to_owned()),
            (Some("o".to_owned()), "p".to_owned())
        ]
    );
    assert_eq!(f.names, vec!["fk1".to_owned()]);
    let f = fk_refs(
        b"ALTER TABLE `fc` ADD CONSTRAINT `fk2` FOREIGN KEY (`id`) REFERENCES `nope` (`id`)",
        &my(),
    )
    .unwrap();
    assert_eq!((f.table.as_str(), f.names.len()), ("fc", 1));
    assert_eq!(fk_refs(b"CREATE TABLE t (id int)", &my()), None);
    assert!(is_fk_formed_error(
        1005,
        "Can't create table `d`.`fc` (errno: 150 \"Foreign key constraint is incorrectly formed\")"
    ));
    assert!(is_fk_duplicate_error(
        1005,
        "Can't create table `d`.`fc2` (errno: 121 \"Duplicate key on write or update\")"
    ));
    assert_eq!(
        fk_missing_table(None, "missing_t").message,
        "Failed to open the referenced table 'missing_t'"
    );
    assert_eq!(fk_duplicate_name("fk1").code, 1826);
}
