use super::*;

fn sub(sql: &str) -> String {
    substitute(sql, &LexOptions::mariadb()).unwrap_or_else(|| sql.to_owned())
}

#[test]
fn bodies_get_lookups_and_restore_byte_for_byte() {
    for sql in [
        "CREATE DEFINER=`t1_u1`@`%` PROCEDURE p() SELECT CURRENT_USER(), USER() AS u, DATABASE()",
        "CREATE FUNCTION f() RETURNS VARCHAR(80) DETERMINISTIC RETURN CONCAT(current_user, '|', schema())",
        "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW SET NEW.who = SESSION_USER(), NEW.db = DATABASE()",
        "CREATE EVENT e ON SCHEDULE EVERY 1 DAY DO INSERT INTO t VALUES (SYSTEM_USER(), DATABASE())",
        "CREATE VIEW v AS SELECT id FROM t WHERE owner = CURRENT_USER",
    ] {
        let s = sub(sql);
        assert_ne!(s, sql, "{sql}");
        assert!(has_lookup(s.as_bytes()));
        let back = restore_str(&s);
        assert!(!has_lookup(back.as_bytes()), "{back}");
        // Nothing nests when the text is substituted again.
        assert_eq!(sub(&s), s, "{sql}");
    }
    assert_eq!(
        restore_str(&sub(
            "CREATE VIEW v AS SELECT id FROM t WHERE owner = CURRENT_USER()"
        )),
        "CREATE VIEW v AS SELECT id FROM t WHERE owner = CURRENT_USER()"
    );
    assert_eq!(
        restore_str(&sub(
            "CREATE PROCEDURE p() SELECT a FROM t WHERE d = database()"
        )),
        "CREATE PROCEDURE p() SELECT a FROM t WHERE d = database()"
    );
}

#[test]
fn whole_select_items_keep_the_call_as_their_name() {
    let s = sub("CREATE VIEW v AS SELECT CURRENT_USER(), user() AS u, DATABASE() FROM t");
    assert!(s.contains(") AS `CURRENT_USER()`,"), "{s}");
    assert!(s.contains(") AS u,"), "{s}");
    assert!(s.contains(") AS `DATABASE()` FROM"), "{s}");
    assert_eq!(
        restore_str(&s),
        "CREATE VIEW v AS SELECT CURRENT_USER() AS `CURRENT_USER()`, user() AS u, DATABASE() AS `DATABASE()` FROM t"
    );
    // Not a whole item: no alias.
    let s = sub("CREATE PROCEDURE p() SELECT CONCAT(CURRENT_USER(), 'x'), 1 + LENGTH(USER())");
    assert!(!s.contains(" AS `"), "{s}");
    let s = sub("CREATE PROCEDURE p() BEGIN DECLARE x TEXT; SELECT CURRENT_USER INTO x; END");
    assert!(s.contains(" AS `CURRENT_USER` INTO x"), "{s}");
}

#[test]
fn definers_account_statements_strings_and_qualified_calls_stay() {
    for sql in [
        "CREATE DEFINER = CURRENT_USER PROCEDURE p() SELECT 1",
        "CREATE DEFINER=CURRENT_USER() VIEW v AS SELECT 1",
        "CREATE PROCEDURE p() SET PASSWORD FOR CURRENT_USER() = PASSWORD('x')",
        "CREATE PROCEDURE p() GRANT SELECT ON t TO CURRENT_USER",
        "CREATE PROCEDURE p() SELECT 'CURRENT_USER()', \"DATABASE()\"",
        "CREATE PROCEDURE p() SELECT db.user(), x.database()",
        "CREATE PROCEDURE p() SELECT user FROM t",
        "CREATE PROCEDURE p() BEGIN PREPARE s FROM 'SELECT USER()'; EXECUTE s; END",
    ] {
        assert_eq!(substitute(sql, &LexOptions::mariadb()), None, "{sql}");
    }
}

#[test]
fn executable_comments_are_substituted_inside() {
    let dump = "/*!50001 CREATE ALGORITHM=UNDEFINED */ /*!50013 DEFINER=`t1_u1`@`%` SQL SECURITY DEFINER */ /*!50001 VIEW `v` AS select current_user() AS `cu` */";
    let s = sub(dump);
    assert!(s.contains("DEFINER=`t1_u1`@`%`"));
    assert!(
        s.contains("WHERE `name` = SUBSTRING_INDEX(current_user(), '@', 1)) AS `cu` */"),
        "{s}"
    );
    assert_eq!(restore_str(&s), dump);
}

#[test]
fn canonical_view_text_restores() {
    // MariaDB 11.8's SHOW CREATE VIEW of a substituted view (captured).
    let canon = "CREATE ALGORITHM=UNDEFINED DEFINER=`r4fixA_u1`@`%` SQL SECURITY DEFINER VIEW `r4fix_pa`.`v2` AS select (select `__sprig_names_v`.`account` from `__sprig_sys`.`__sprig_names_v` where `__sprig_names_v`.`name` = substring_index(current_user(),'@',1)) AS `CURRENT_USER()`,(select concat(`__sprig_names_v`.`logical`,'@',substring_index(user(),'@',-1)) from `__sprig_sys`.`__sprig_names_v` where `__sprig_names_v`.`name` = substring_index(user(),'@',1)) AS `u`,(select `__sprig_names_v`.`logical` from `__sprig_sys`.`__sprig_names_v` where `__sprig_names_v`.`name` = database()) AS `d`";
    assert_eq!(
        restore_str(canon),
        "CREATE ALGORITHM=UNDEFINED DEFINER=`r4fixA_u1`@`%` SQL SECURITY DEFINER VIEW `r4fix_pa`.`v2` AS select current_user() AS `CURRENT_USER()`,user() AS `u`,database() AS `d`"
    );
    // A user's own subquery around a lookup keeps its shape.
    let nested = "select (select max(`t`.`a`) from `t` where `t`.`o` = (select `__sprig_names_v`.`account` from `__sprig_sys`.`__sprig_names_v` where `__sprig_names_v`.`name` = substring_index(current_user(),'@',1))) AS `m`";
    assert_eq!(
        restore_str(nested),
        "select (select max(`t`.`a`) from `t` where `t`.`o` = current_user()) AS `m`"
    );
}

#[test]
fn ansi_quotes_bodies() {
    let lex = LexOptions::mariadb().ansi_quotes(true);
    let s = substitute(
        "CREATE PROCEDURE p() SELECT \"a\" FROM t WHERE \"who\" = CURRENT_USER()",
        &lex,
    )
    .unwrap();
    assert!(s.contains("WHERE \"who\" = (SELECT `account`"), "{s}");
}

// --- caught error text (GET DIAGNOSTICS, SQLERRM) ----------------------------

#[test]
fn diagnostics_in_bodies_are_mapped_and_restore_byte_for_byte() {
    for sql in [
        "CREATE FUNCTION f() RETURNS TEXT BEGIN DECLARE m TEXT; DECLARE CONTINUE HANDLER FOR SQLEXCEPTION GET DIAGNOSTICS CONDITION 1 m = MESSAGE_TEXT; SELECT 1 INTO @x FROM nosuch; RETURN m; END",
        "CREATE PROCEDURE p() BEGIN DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN GET CURRENT DIAGNOSTICS CONDITION 1 @m = MESSAGE_TEXT, @e = MYSQL_ERRNO, `s` = SCHEMA_NAME; INSERT INTO a VALUES (@m); END; INSERT INTO c VALUES (1); END",
        "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW BEGIN DECLARE m TEXT; DECLARE CONTINUE HANDLER FOR SQLEXCEPTION get stacked diagnostics condition 1 m = table_name; SET NEW.v = m; END",
        "CREATE EVENT e ON SCHEDULE EVERY 1 DAY DO BEGIN IF 1 THEN GET DIAGNOSTICS CONDITION 1 @m = MESSAGE_TEXT; END IF; END",
        "CREATE PROCEDURE p() GET DIAGNOSTICS CONDITION 1 @m = MESSAGE_TEXT",
        "CREATE PROCEDURE p() BEGIN GET DIAGNOSTICS CONDITION 1 @m = MESSAGE_TEXT; SET @x = 1; END",
        "CREATE PROCEDURE p() BEGIN DECLARE m TEXT; DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN GET DIAGNOSTICS CONDITION 1 m = MESSAGE_TEXT; END; begin get diagnostics condition 1 m = message_text; end; END",
        "/*!50003 CREATE*/ /*!50003 TRIGGER tr BEFORE INSERT ON t FOR EACH ROW BEGIN DECLARE CONTINUE HANDLER FOR SQLEXCEPTION GET DIAGNOSTICS CONDITION 1 @m = MESSAGE_TEXT; SET @x = 1; END */",
    ] {
        let s = sub(sql);
        assert_ne!(s, sql, "{sql}");
        assert!(s.contains("`__sprig_sys`.`__sprig_names_map`("), "{s}");
        assert_eq!(restore_str(&s), sql, "{s}");
        // Nothing nests when the text is substituted again.
        assert_eq!(sub(&s), s, "{sql}");
    }
    // The block holds the statement and one mapping per text item.
    let s = sub(
        "CREATE PROCEDURE p() BEGIN DECLARE CONTINUE HANDLER FOR SQLEXCEPTION GET DIAGNOSTICS CONDITION 1 @m = MESSAGE_TEXT, @e = MYSQL_ERRNO, `s` = SCHEMA_NAME, @q = RETURNED_SQLSTATE; END",
    );
    assert!(
        s.contains(
            "FOR SQLEXCEPTION BEGIN GET DIAGNOSTICS CONDITION 1 @m = MESSAGE_TEXT, @e = MYSQL_ERRNO, `s` = SCHEMA_NAME, @q = RETURNED_SQLSTATE; \
             SET @m = `__sprig_sys`.`__sprig_names_map`(@m); SET `s` = `__sprig_sys`.`__sprig_names_map`(`s`); END; END"
        ),
        "{s}"
    );
}

#[test]
fn diagnostics_without_text_items_and_look_alikes_stay() {
    for sql in [
        "CREATE PROCEDURE p() BEGIN GET DIAGNOSTICS @n = NUMBER, @r = ROW_COUNT; END",
        "CREATE PROCEDURE p() BEGIN DECLARE CONTINUE HANDLER FOR SQLEXCEPTION GET DIAGNOSTICS CONDITION 1 @e = MYSQL_ERRNO; END",
        "CREATE PROCEDURE p() SELECT 'GET DIAGNOSTICS CONDITION 1 @m = MESSAGE_TEXT'",
        "CREATE PROCEDURE p() BEGIN PREPARE s FROM 'GET DIAGNOSTICS CONDITION 1 @m = MESSAGE_TEXT'; END",
        "CREATE PROCEDURE p() SELECT sqlerrm FROM t",
    ] {
        assert_eq!(substitute(sql, &LexOptions::mariadb()), None, "{sql}");
    }
}

#[test]
fn sqlerrm_is_mapped_under_oracle_only() {
    let lex = LexOptions::mariadb().ansi_quotes(true);
    let sql = "CREATE PROCEDURE p AS BEGIN SELECT 1 INTO @x FROM nosuch; EXCEPTION WHEN OTHERS THEN SET @e = SQLERRM; INSERT INTO a VALUES (sqlerrm || '!', x.sqlerrm); END";
    let s = substitute_with(sql, &lex, true).unwrap();
    assert!(
        s.contains("SET @e = `__sprig_sys`.`__sprig_names_map`(SQLERRM);"),
        "{s}"
    );
    assert!(
        s.contains("(`__sprig_sys`.`__sprig_names_map`(sqlerrm) || '!', x.sqlerrm)"),
        "{s}"
    );
    assert_eq!(restore_str(&s), sql);
    assert_eq!(substitute_with(&s, &lex, true), None);
    assert_eq!(substitute_with(sql, &lex, false), None);
}

#[test]
fn top_level_statements() {
    let lex = LexOptions::mariadb();
    let top =
        |sql: &str| top_level(sql.as_bytes(), &lex, false).map(|v| String::from_utf8(v).unwrap());
    assert_eq!(
        top("GET DIAGNOSTICS CONDITION 1 @m = MESSAGE_TEXT, @e = MYSQL_ERRNO").as_deref(),
        Some(
            "BEGIN NOT ATOMIC GET DIAGNOSTICS CONDITION 1 @m = MESSAGE_TEXT, @e = MYSQL_ERRNO; \
             SET @m = `__sprig_sys`.`__sprig_names_map`(@m); END"
        )
    );
    assert_eq!(top("GET DIAGNOSTICS @n = NUMBER"), None);
    assert_eq!(top("SELECT 'GET DIAGNOSTICS'"), None);
    assert_eq!(top("SELECT 1"), None);
    let c = top("BEGIN NOT ATOMIC DECLARE CONTINUE HANDLER FOR SQLEXCEPTION GET DIAGNOSTICS CONDITION 1 @m = MESSAGE_TEXT; SELECT 1 FROM x; END").unwrap();
    assert!(c.contains("SQLEXCEPTION BEGIN GET DIAGNOSTICS"), "{c}");
    let i = top("IF 1 THEN GET DIAGNOSTICS CONDITION 1 @m = MESSAGE_TEXT; END IF").unwrap();
    assert!(i.contains("THEN BEGIN GET DIAGNOSTICS CONDITION 1 @m = MESSAGE_TEXT; SET @m = `__sprig_sys`.`__sprig_names_map`(@m); END; END IF"), "{i}");
    assert_eq!(
        top("PREPARE s FROM 'GET DIAGNOSTICS CONDITION 1 @m = MESSAGE_TEXT'").as_deref(),
        Some(
            "PREPARE s FROM 'BEGIN NOT ATOMIC GET DIAGNOSTICS CONDITION 1 @m = MESSAGE_TEXT; \
             SET @m = `__sprig_sys`.`__sprig_names_map`(@m); END'"
        )
    );
    assert!(
        top("EXECUTE IMMEDIATE 'GET DIAGNOSTICS CONDITION 1 @m = MESSAGE_TEXT'")
            .unwrap()
            .starts_with("EXECUTE IMMEDIATE 'BEGIN NOT ATOMIC GET")
    );
    assert_eq!(top("PREPARE s FROM @q"), None);
    // SQLERRM only under ORACLE.
    let o = "BEGIN NOT ATOMIC SELECT 1; EXCEPTION WHEN OTHERS THEN SET @e = SQLERRM; END";
    assert_eq!(top(o), None);
    assert!(
        String::from_utf8(top_level(o.as_bytes(), &lex, true).unwrap())
            .unwrap()
            .contains("`__sprig_sys`.`__sprig_names_map`(SQLERRM)")
    );
}

/// The mapping function computes, inside MariaDB, what the gateway's
/// `map_message` computes. The expected text is what the function's
/// prototype returned on MariaDB 11.8.9 for the same input and names
/// (database 1 `shop`, database 2 ``we`ird``, user 7 `alice`, tenant 42).
#[test]
fn the_mapping_function_agrees_with_map_message() {
    use sprig_common::ids::{DbId, GroupId, ShardId, TenantId};
    use sprig_common::names::{AccountName, SchemaName};

    use super::super::map::{LogicalDb, MemoryNameMap};
    use super::super::output::{AccountResolver, OutputCtx, map_message};

    struct Users;
    impl AccountResolver for Users {
        fn account_name(&self, a: &AccountName) -> Option<String> {
            matches!(a, AccountName::User { user, .. } if user.0 == 7).then(|| "alice".to_owned())
        }
    }
    let t = TenantId(42);
    let mut map = MemoryNameMap::new(t);
    map.insert(LogicalDb {
        id: DbId(1),
        name: "shop".into(),
        shards: vec![
            SchemaName::shard(t, DbId(1), GroupId(1), ShardId(1)),
            SchemaName::shard(t, DbId(1), GroupId(1), ShardId(2)),
        ],
        reference: None,
    });
    map.insert(LogicalDb {
        id: DbId(2),
        name: "we`ird".into(),
        shards: vec![SchemaName::shard(t, DbId(2), GroupId(1), ShardId(1))],
        reference: None,
    });
    let ctx = OutputCtx::new(&map).accounts(&Users);
    let input = "`t42_d2_g1_s9`.`x` t42_d1_ref t42_d3_g1_s1 t42_sprig t43_d1_g1_s1 t42_u7 t42_u8 t42_r1 t42_rc0000000a T42_d1_g1_s1 xt42_d1_g1_s1 t42_d1_g1_s1x t042_d1_g1_s1 Tables_in_t42_d1_g1_s1 Tables_in_tx __sprig_lk_orders__email __sprig_wx_coll __sprig_ix___x é t42_d1_g1_s1é ét42_d1_g1_s1 t42_d1_g1_s1, t1";
    let from_mariadb = "`we``ird`.`x` shop ? ? ? alice ? ? ? T42_d1_g1_s1 xt42_d1_g1_s1 t42_d1_g1_s1x t042_d1_g1_s1 Tables_in_shop Tables_in_tx orders coll __sprig_ix___x é t42_d1_g1_s1é ét42_d1_g1_s1 shop, t1";
    assert_eq!(map_message(input, &ctx), from_mariadb);
    assert_eq!(
        map_message("Table 't42_d1_g1_s1.nosuch' doesn't exist", &ctx),
        "Table 'shop.nosuch' doesn't exist"
    );
    // The function's text has no backslash (no sql_mode changes it).
    assert!(!map_function_sql().contains('\\'));
}
