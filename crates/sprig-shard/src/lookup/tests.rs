#![allow(
    clippy::unwrap_used,
    clippy::many_single_char_names,
    clippy::format_collect,
    clippy::too_many_lines
)]

use super::*;
use crate::collation::CollationId;

pub(crate) fn email_lookup() -> LookupIndex {
    LookupIndex {
        id: LookupId(7),
        owner: ObjectId(11),
        owner_table: "customers".into(),
        index_name: "uk_email".into(),
        columns: vec![LookupColumn {
            name: "email".into(),
            sql_type: "VARCHAR(255) COLLATE utf8mb4_uca1400_ai_ci".into(),
            class: KeyClass::String {
                collation: CollationId(2304),
            },
        }],
        owner_pk: vec![LookupColumn {
            name: "id".into(),
            sql_type: "BIGINT".into(),
            class: KeyClass::Numeric,
        }],
        unique: true,
        origin: LookupOrigin::UniqueKey,
        group: GroupId(9),
        state: LookupState::Public,
        auto_index_id: None,
    }
}

#[test]
fn ddl_and_names() {
    let l = email_lookup();
    assert_eq!(l.physical(), "__sprig_lk_customers__uk_email");
    assert_eq!(
        l.create_table_sql("t1_d1_g9_s1"),
        "CREATE TABLE IF NOT EXISTS `t1_d1_g9_s1`.`__sprig_lk_customers__uk_email` (
  `email` VARCHAR(255) COLLATE utf8mb4_uca1400_ai_ci NOT NULL,
  `owner_id` BIGINT NOT NULL,
  owner_rk BIGINT UNSIGNED NOT NULL,
  prev_rk BIGINT UNSIGNED NULL,
  _sprig_rk BIGINT UNSIGNED NOT NULL DEFAULT 0 INVISIBLE,
  PRIMARY KEY (`email`),
  KEY _sprig_rk (_sprig_rk)
) ENGINE=InnoDB"
    );
    let long = "x".repeat(60);
    assert_eq!(lookup_table_name(&long, "k", LookupId(42)), "__sprig_lk_42");
    let a = propose_auto_lookup(
        LookupId(8),
        ObjectId(11),
        "orders",
        l.columns.clone(),
        l.owner_pk.clone(),
        GroupId(9),
        Some(5),
    );
    assert!(a.index_name.starts_with("sprig_auto_") && a.index_name.len() == 19);
    assert_eq!(
        a.index_name,
        auto_lookup_name(ObjectId(11), &["email".into()])
    );
    assert!(!a.unique && a.origin == LookupOrigin::Auto && a.state == LookupState::Building);
    assert!(
        a.create_table_sql("s")
            .contains("PRIMARY KEY (`email`, `owner_id`)")
    );
    // Equal emails under the collation hash alike.
    assert_eq!(
        value_hash(&l, &[text("Ann@X.com ")]).unwrap(),
        value_hash(&l, &[text("ann@x.com")]).unwrap()
    );
}
