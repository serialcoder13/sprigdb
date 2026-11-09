//! Golden tests for the rewriter: SQL in, physical SQL out (chapter 32,
//! "Testing").

use std::sync::Arc;

use super::*;
use crate::model::fixtures::assets;

pub(crate) struct One(pub(crate) Arc<DbTypes>);

impl Lookup for One {
    fn db(&self, db: Option<&str>) -> Option<Arc<DbTypes>> {
        match db {
            None | Some("shop") => Some(Arc::clone(&self.0)),
            Some(_) => None,
        }
    }
    fn lctn(&self) -> bool {
        false
    }
}

fn rewrite_with(sql: &str, only_all: bool, reads: &[(usize, Option<NodeRead>)]) -> String {
    let l = One(Arc::new(assets()));
    let marked = crate::grammar::mark_only(
        sql.as_bytes(),
        &sprig_sql::lex::LexOptions::mariadb(),
        &|_, n| l.0.get(n, false).is_some(),
    );
    let mut stmt = parse_one(marked.as_deref().unwrap_or(sql)).unwrap();
    let Statement::Query(q) = &mut stmt else {
        panic!()
    };
    let mut w = Walker::new(&l);
    w.only_all = only_all;
    w.query(Pass::Collect, q).unwrap();
    w.plan_refs();
    for (i, r) in reads {
        w.reads.insert(*i, r.clone());
    }
    w.query(Pass::Rewrite, q).unwrap();
    stmt.to_string()
}

fn rw(sql: &str) -> String {
    rewrite_with(sql, false, &[])
}

#[test]
fn roots_pass_through() {
    assert_eq!(rw("SELECT * FROM asset"), "SELECT * FROM asset");
    assert_eq!(
        rw("SELECT name FROM asset WHERE site_id = 3"),
        "SELECT name FROM asset WHERE site_id = 3"
    );
}

#[test]
fn subtype_columns_join_their_tables() {
    assert_eq!(
        rw("SELECT name, vin FROM vehicle WHERE vin LIKE '1FT%'"),
        "SELECT `vehicle__r`.name, `vehicle__x1`.vin FROM (`asset` AS `vehicle__r` JOIN `vehicle` AS \
         `vehicle__x1` ON `vehicle__x1`.`id` = `vehicle__r`.`id`) WHERE `vehicle__x1`.vin LIKE '1FT%'"
    );
}

#[test]
fn root_columns_only_use_the_path_range() {
    assert_eq!(
        rw("SELECT COUNT(*) FROM vehicle WHERE site_id = 3"),
        "SELECT COUNT(*) FROM `asset` AS `vehicle` WHERE (`vehicle`.site_id = 3) AND \
         (`vehicle`.`_type_path` >= '/asset/vehicle/' AND `vehicle`.`_type_path` < '/asset/vehicle0')"
    );
}

#[test]
fn own_columns_only_read_the_extension_table() {
    assert_eq!(
        rw("SELECT v.vin FROM vehicle v"),
        "SELECT `v`.vin FROM `vehicle` AS `v`"
    );
}

#[test]
fn wildcards_expand_inherited_first() {
    assert_eq!(
        rw("SELECT * FROM truck"),
        "SELECT `truck__r`.`id`, `truck__r`.`name`, `truck__r`.`site_id`, `truck__x1`.`vin`, \
         `truck__x2`.`payload_kg` FROM (`asset` AS `truck__r` JOIN `vehicle` AS `truck__x1` ON \
         `truck__x1`.`id` = `truck__r`.`id` JOIN `truck` AS `truck__x2` ON `truck__x2`.`id` = \
         `truck__r`.`id`)"
    );
    // Another table keeps its own wildcard.
    let s = rw("SELECT * FROM site s JOIN vehicle v ON v.site_id = s.id");
    assert!(s.starts_with("SELECT `s`.*, `v__r`.`id`"), "{s}");
    assert!(s.contains("ON `v__r`.site_id = s.id"), "{s}");
}

#[test]
fn only_filters_on_the_type_name() {
    assert_eq!(
        rw("SELECT name FROM ONLY asset"),
        "SELECT `asset`.name FROM `asset` AS `asset` WHERE `asset`.`_type` = 'asset'"
    );
    assert_eq!(
        rewrite_with("SELECT /*!40001 SQL_NO_CACHE */ * FROM vehicle", true, &[]),
        "SELECT SQL_NO_CACHE `vehicle__r`.`id`, `vehicle__r`.`name`, `vehicle__r`.`site_id`, \
         `vehicle__x1`.`vin` FROM (`asset` AS `vehicle__r` JOIN `vehicle` AS `vehicle__x1` ON `vehicle__x1`.`id` = \
         `vehicle__r`.`id`) WHERE `vehicle__r`.`_type` = 'vehicle'"
    );
    // ONLY on a type without subtypes reads its own table.
    assert_eq!(
        rw("SELECT payload_kg FROM ONLY truck"),
        "SELECT `truck`.payload_kg FROM `truck` AS `truck`"
    );
}

#[test]
fn outer_joins_take_the_filter_in_on() {
    let s = rw("SELECT s.id, v.name FROM site s LEFT JOIN vehicle v ON v.site_id = s.id");
    assert!(
        s.ends_with(
            "LEFT JOIN `asset` AS `v` ON (`v`.site_id = s.id) AND (`v`.`_type_path` >= \
                     '/asset/vehicle/' AND `v`.`_type_path` < '/asset/vehicle0')"
        ),
        "{s}"
    );
}

#[test]
fn subqueries_and_correlation() {
    let s = rw("SELECT id FROM site WHERE id IN (SELECT site_id FROM truck WHERE payload_kg > 10)");
    assert!(
        s.contains("SELECT `truck__r`.site_id FROM (`asset` AS `truck__r` JOIN"),
        "{s}"
    );
    assert!(s.contains("WHERE `truck__x2`.payload_kg > 10"), "{s}");
    let s = rw(
        "SELECT v.vin, (SELECT COUNT(*) FROM building b WHERE b.site_id = v.site_id) FROM vehicle v",
    );
    // building is a subtype too: both sides are rewritten.
    assert!(
        s.contains("(`b`.site_id = `v__r`.site_id) AND (`b`.`_type_path` >= '/asset/building/'"),
        "{s}"
    );
    // A select-list alias in ORDER BY stays.
    let s = rw("SELECT vin AS name FROM vehicle ORDER BY name");
    assert!(s.ends_with("ORDER BY name"), "{s}");
    // A CTE named like a type shadows it.
    assert_eq!(
        rw("WITH vehicle AS (SELECT 1 AS vin) SELECT vin FROM vehicle"),
        "WITH vehicle AS (SELECT 1 AS vin) SELECT vin FROM vehicle"
    );
}

#[test]
fn qualified_database_names() {
    let s = rw("SELECT shop.vehicle.name FROM shop.vehicle");
    assert_eq!(
        s,
        "SELECT `vehicle`.name FROM `shop`.`asset` AS `vehicle` WHERE `vehicle`.`_type_path` >= \
         '/asset/vehicle/' AND `vehicle`.`_type_path` < '/asset/vehicle0'"
            .replace("WHERE `vehicle`.`_type_path` >= '/asset/vehicle/' AND `vehicle`.`_type_path` < '/asset/vehicle0'",
                     "WHERE (`vehicle`.`_type_path` >= '/asset/vehicle/' AND `vehicle`.`_type_path` < '/asset/vehicle0')")
    );
}

#[test]
fn hierarchy_predicates() {
    let read = NodeRead {
        path: b"/1/7/".to_vec(),
        rk: b"1".to_vec(),
    };
    let s = rewrite_with(
        "SELECT id FROM org_unit WHERE SPRIG_SUBTREE(org_unit, 7)",
        false,
        &[(0, Some(read.clone()))],
    );
    assert_eq!(
        s,
        "SELECT id FROM org_unit WHERE (`org_unit`.`_sprig_hpath` >= X'2F312F372F' AND \
         `org_unit`.`_sprig_hpath` < X'2F312F3730' AND `org_unit`.`_sprig_rk` = 1)"
    );
    let s = rewrite_with(
        "SELECT o.id, SPRIG_DEPTH(o) FROM org_unit o WHERE SPRIG_ANCESTORS(o, 7)",
        false,
        &[(1, Some(read))],
    );
    assert_eq!(
        s,
        "SELECT o.id, (`o`.`_sprig_hdepth`) FROM org_unit o WHERE (`o`.`id` IN (1) AND \
         `o`.`_sprig_rk` = 1)"
    );
    // A missing node matches nothing.
    let s = rewrite_with(
        "SELECT id FROM org_unit WHERE SPRIG_DESCENDANTS(org_unit, 99)",
        false,
        &[(0, None)],
    );
    assert!(s.ends_with("WHERE (false)"), "{s}");
    // A non-literal key reads the node in a subquery.
    let s =
        rw("SELECT a.id FROM org_unit a JOIN org_unit b ON SPRIG_SUBTREE(a, b.id) WHERE b.id = 1");
    assert!(s.contains("(SELECT `sprig_h`.`_sprig_hpath` FROM `org_unit` AS `sprig_h` WHERE `sprig_h`.`id` = b.id)"), "{s}");
}

#[test]
fn segments_round_trip() {
    use crate::model::KeyClass;
    assert_eq!(segment(b"a/b,c%", KeyClass::Str), "a%2Fb%2Cc%25");
    assert_eq!(
        seg_literal("a%2Fb%2Cc%25", KeyClass::Str, false),
        "'a/b,c%'"
    );
    assert_eq!(segment(&[0xAB, 0x01], KeyClass::Bin), "AB01");
    assert_eq!(seg_literal("AB01", KeyClass::Bin, false), "X'AB01'");
    assert_eq!(segments(b"/1/7/42/"), ["1", "7", "42"]);
    assert_eq!(key_literal(b"42", KeyClass::Int, false), "42");
    assert_eq!(key_literal(b"it's", KeyClass::Str, false), "'it''s'");
}

#[test]
fn strings_keep_their_escapes() {
    assert_eq!(
        rw(r"SELECT vin FROM vehicle WHERE name = 'it''s \\ \' x'"),
        r"SELECT `vehicle__x1`.vin FROM (`asset` AS `vehicle__r` JOIN `vehicle` AS `vehicle__x1` ON `vehicle__x1`.`id` = `vehicle__r`.`id`) WHERE `vehicle__r`.name = 'it''s \\ \' x'"
    );
}
