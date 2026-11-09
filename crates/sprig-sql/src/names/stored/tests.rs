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
