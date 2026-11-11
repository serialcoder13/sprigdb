use super::*;

fn rw(sql: &str) -> Option<Result<String, WireError>> {
    rewrite(sql.as_bytes(), &LexOptions::mysql84())
        .map(|r| r.map(|v| String::from_utf8(v).unwrap()))
}

const TOP2: &str = "SELECT c.name, l.amt FROM c, LATERAL (SELECT o.amt FROM o WHERE o.cid = c.id ORDER BY o.amt DESC LIMIT 2) AS l";

#[test]
fn objects() {
    let o = LexOptions::mysql84();
    let k = |s: &str| object(s.as_bytes(), &o);
    assert_eq!(
        k(
            "CREATE OR REPLACE ALGORITHM=MERGE DEFINER=`a`@`%` SQL SECURITY INVOKER VIEW v AS SELECT 1"
        ),
        Some(Object::View)
    );
    assert_eq!(
        k("CREATE DEFINER=CURRENT_USER PROCEDURE p() SELECT 1"),
        Some(Object::Procedure)
    );
    assert_eq!(
        k("create function f() returns int return 1"),
        Some(Object::Function)
    );
    assert_eq!(
        k("CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW SET NEW.a = 1"),
        Some(Object::Trigger)
    );
    assert_eq!(
        k("CREATE EVENT e ON SCHEDULE EVERY 1 DAY DO DELETE FROM t"),
        Some(Object::Event)
    );
    assert_eq!(k("ALTER VIEW v AS SELECT 2"), Some(Object::View));
    assert_eq!(k("CREATE TABLE view (a INT)"), None);
    assert_eq!(k("CREATE TABLE t (function INT)"), None);
    assert_eq!(k("SELECT 1"), None);
}

#[test]
fn lateral_in_a_view_and_a_procedure() {
    let v = rw(&format!("CREATE VIEW v AS {TOP2}")).unwrap().unwrap();
    assert!(
        v.starts_with(
            "CREATE VIEW v AS SELECT c.name, l.amt FROM c, (SELECT o.amt, o.cid AS `_lat_k0`"
        ),
        "{v}"
    );
    assert!(!v.contains("LATERAL"), "{v}");
    let v = rw(&format!(
        "CREATE VIEW v (n, a) AS {TOP2} WITH CASCADED CHECK OPTION"
    ))
    .unwrap()
    .unwrap();
    assert!(v.ends_with(" WITH CASCADED CHECK OPTION"), "{v}");
    assert!(!v.contains("LATERAL"), "{v}");
    let p = rw(&format!(
        "CREATE PROCEDURE p(IN x INT)\nBEGIN\n  DECLARE n INT;\n  IF x > 0 THEN\n    {TOP2};\n  END IF;\n  SET n = (SELECT COUNT(*) FROM o);\nEND"
    ))
    .unwrap()
    .unwrap();
    assert!(
        p.contains("IF x > 0 THEN\n    SELECT c.name, l.amt FROM c, (SELECT"),
        "{p}"
    );
    assert!(
        p.contains(";\n  END IF;\n  SET n = (SELECT COUNT(*) FROM o);\nEND"),
        "{p}"
    );
    assert!(!p.contains("LATERAL"), "{p}");
    // A cursor's query and a function's subquery.
    let f = rw(&format!(
        "CREATE FUNCTION f() RETURNS INT READS SQL DATA BEGIN DECLARE c CURSOR FOR {TOP2}; RETURN (SELECT SUM(l.t) FROM c, LATERAL (SELECT SUM(o.amt) AS t FROM o WHERE o.cid = c.id) AS l); END"
    ))
    .unwrap()
    .unwrap();
    assert!(!f.contains("LATERAL"), "{f}");
}

#[test]
fn rollup_in_a_view() {
    let v =
        rw("CREATE VIEW r AS SELECT a, SUM(b) AS s FROM t GROUP BY a WITH ROLLUP ORDER BY a DESC")
            .unwrap()
            .unwrap();
    assert!(v.starts_with("CREATE VIEW r AS SELECT "), "{v}");
    assert!(v.contains("FROM (SELECT"), "{v}");
}

#[test]
fn what_needs_the_gateway_fails_with_39602() {
    // A LATERAL shape the rewrite can't express.
    let e = rw(
        "CREATE VIEW v AS SELECT * FROM c, LATERAL (SELECT o.amt FROM o WHERE o.cid > c.id) AS l",
    )
    .unwrap()
    .unwrap_err();
    assert_eq!(e.code, 39602);
    assert_eq!(&e.sqlstate, b"42000");
    assert!(
        e.message.contains("a view can't use this LATERAL"),
        "{}",
        e.message
    );
    let e = rw("CREATE PROCEDURE p() SELECT * FROM performance_schema.threads")
        .unwrap()
        .unwrap_err();
    assert!(
        e.message
            .contains("a procedure can't use performance_schema.threads"),
        "{}",
        e.message
    );
    let e =
        rw("CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW CALL sprig.mq_publish('q', NEW.a)")
            .unwrap()
            .unwrap_err();
    assert!(e.message.contains("CALL sprig.mq_publish"), "{}", e.message);
    // sys's functions are expressions: fine.
    assert!(
        rw("CREATE FUNCTION f(x BIGINT) RETURNS TEXT DETERMINISTIC RETURN sys.format_bytes(x)")
            .is_none()
    );
}

#[test]
fn others_are_left_alone() {
    assert!(rw("CREATE PROCEDURE p() BEGIN SELECT 1; END").is_none());
    assert!(rw(TOP2).is_none());
    assert!(rw("CREATE TABLE t (a INT)").is_none());
    assert!(
        rw("CREATE FUNCTION f(o JSON) RETURNS JSON LANGUAGE javascript AS $$ return o $$")
            .is_none()
    );
    // 3307 isn't touched.
    let o = LexOptions::mariadb();
    assert!(rewrite(format!("CREATE VIEW v AS {TOP2}").as_bytes(), &o).is_none());
}
