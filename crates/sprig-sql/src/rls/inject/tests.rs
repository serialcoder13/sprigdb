use sprig_common::{SprigCode, SprigError};

use super::{Cmd, Resolver, inject};
use crate::lex::LexOptions;

/// `orders` (key 1, any database but `other`) and `secrets` (key 2) are
/// protected; `orders` reads `owner = 'u1'`, updates `locked = 0`,
/// deletes `del = 1`.
struct Mock;

impl Resolver for Mock {
    fn mentions(&self, word: &str) -> bool {
        word == "orders" || word == "secrets" || word == "v_orders" || word == "f_orders"
    }

    fn refused_relation(&self, _db: Option<&str>, name: &str) -> bool {
        name == "v_orders"
    }

    fn refused_function(&self, name: &str) -> bool {
        name.eq_ignore_ascii_case("f_orders")
    }

    fn protected(&self, db: Option<&str>, table: &str) -> Option<usize> {
        if db == Some("other") {
            return None;
        }
        match table {
            "orders" => Some(1),
            "secrets" => Some(2),
            _ => None,
        }
    }

    fn predicate(&self, key: usize, cmds: &[Cmd], qual: &str) -> Result<String, SprigError> {
        let one = |c: &Cmd| match (key, c) {
            (1, Cmd::Select) => format!("{qual}.owner = 'u1'"),
            (1, Cmd::Update) => format!("{qual}.locked = 0"),
            (1, Cmd::Delete) => format!("{qual}.del = 1"),
            (_, _) => "FALSE".to_owned(),
        };
        Ok(cmds.iter().map(one).collect::<Vec<_>>().join(" AND "))
    }
}

fn rw(s: &str) -> String {
    inject(s, &LexOptions::mariadb(), &Mock)
        .unwrap_or_else(|e| panic!("{s}: {e}"))
        .unwrap_or_else(|| panic!("{s}: nothing to do"))
        .sql
}

fn refused(s: &str) -> SprigError {
    inject(s, &LexOptions::mariadb(), &Mock).expect_err(s)
}

#[test]
fn untouched_without_protected_names() {
    assert!(
        inject("SELECT * FROM customers", &LexOptions::mariadb(), &Mock)
            .unwrap()
            .is_none()
    );
    // A column called orders is not a table.
    assert!(
        inject(
            "SELECT orders FROM customers",
            &LexOptions::mariadb(),
            &Mock
        )
        .unwrap()
        .is_none()
    );
    // A CTE called orders.
    assert!(
        inject(
            "WITH orders AS (SELECT 1 AS a) SELECT * FROM orders",
            &LexOptions::mariadb(),
            &Mock
        )
        .unwrap()
        .is_none()
    );
    // Another database's orders.
    assert!(
        inject("SELECT * FROM other.orders", &LexOptions::mariadb(), &Mock)
            .unwrap()
            .is_none()
    );
}

#[test]
fn selects() {
    assert_eq!(
        rw("SELECT * FROM orders"),
        "SELECT * FROM orders WHERE (`orders`.owner = 'u1')"
    );
    assert_eq!(
        rw("SELECT id FROM shop.orders o WHERE o.total > 5 OR o.id = 1 ORDER BY id LIMIT 3"),
        "SELECT id FROM shop.orders o WHERE (`o`.owner = 'u1') AND (o.total > 5 OR o.id = 1) \
         ORDER BY id LIMIT 3"
    );
    assert_eq!(
        rw("select count(*) from orders group by x having count(*) > 1"),
        "select count(*) from orders WHERE (`orders`.owner = 'u1') group by x having count(*) > 1"
    );
    // Comments, hints and literals stay.
    assert_eq!(
        rw(
            "SELECT /*+ NO_INDEX(o) */ 'orders' /* orders */ FROM `orders` AS o -- x\nWHERE a = 'b'"
        ),
        "SELECT /*+ NO_INDEX(o) */ 'orders' /* orders */ FROM `orders` AS o -- x\nWHERE \
         (`o`.owner = 'u1') AND (a = 'b')"
    );
    // Both tables of a join, in the WHERE.
    assert_eq!(
        rw("SELECT * FROM orders o JOIN secrets s ON s.id = o.sid WHERE o.x = 1"),
        "SELECT * FROM orders o JOIN secrets s ON s.id = o.sid WHERE ((`o`.owner = 'u1') AND \
         (FALSE)) AND (o.x = 1)"
    );
    // Subqueries, derived tables, unions, CTEs each get their own.
    assert_eq!(
        rw(
            "SELECT * FROM c WHERE c.id IN (SELECT cust FROM orders) UNION SELECT * FROM (SELECT * FROM orders) d"
        ),
        "SELECT * FROM c WHERE c.id IN (SELECT cust FROM orders WHERE (`orders`.owner = 'u1')) \
         UNION SELECT * FROM (SELECT * FROM orders WHERE (`orders`.owner = 'u1')) d"
    );
    assert_eq!(
        rw("WITH o AS (SELECT * FROM orders) SELECT * FROM o"),
        "WITH o AS (SELECT * FROM orders WHERE (`orders`.owner = 'u1')) SELECT * FROM o"
    );
    // A non-recursive CTE's own name inside its body is the table.
    assert_eq!(
        rw("WITH orders AS (SELECT * FROM orders) SELECT * FROM orders"),
        "WITH orders AS (SELECT * FROM orders WHERE (`orders`.owner = 'u1')) SELECT * FROM orders"
    );
    assert_eq!(
        rw("SELECT (SELECT max(id) FROM orders) AS m, x FROM t FOR UPDATE"),
        "SELECT (SELECT max(id) FROM orders WHERE (`orders`.owner = 'u1')) AS m, x FROM t FOR UPDATE"
    );
    assert_eq!(
        rw("SELECT * FROM orders LOCK IN SHARE MODE"),
        "SELECT * FROM orders WHERE (`orders`.owner = 'u1') LOCK IN SHARE MODE"
    );
}

#[test]
fn outer_joins_filter_the_nullable_side_in_on() {
    assert_eq!(
        rw("SELECT * FROM c LEFT JOIN orders o ON o.cust = c.id WHERE c.x = 1"),
        "SELECT * FROM c LEFT JOIN orders o ON (`o`.owner = 'u1') AND (o.cust = c.id) WHERE c.x = 1"
    );
    assert_eq!(
        rw("SELECT * FROM orders o RIGHT JOIN c ON (o.cust = c.id)"),
        "SELECT * FROM orders o RIGHT JOIN c ON (`o`.owner = 'u1') AND ((o.cust = c.id))"
    );
    // The preserved side goes to the WHERE.
    assert_eq!(
        rw("SELECT * FROM orders o LEFT JOIN c ON o.cust = c.id"),
        "SELECT * FROM orders o LEFT JOIN c ON o.cust = c.id WHERE (`o`.owner = 'u1')"
    );
    // A nested join on the nullable side: its tables in the outer ON.
    assert_eq!(
        rw("SELECT * FROM c LEFT JOIN (orders o JOIN secrets s ON s.id = o.sid) ON o.cust = c.id"),
        "SELECT * FROM c LEFT JOIN (orders o JOIN secrets s ON s.id = o.sid) ON ((`o`.owner = \
         'u1') AND (FALSE)) AND (o.cust = c.id)"
    );
    // USING on the nullable side: a filtered derived table.
    assert_eq!(
        rw("SELECT * FROM c LEFT JOIN orders o USING (id)"),
        "SELECT * FROM c LEFT JOIN (SELECT * FROM `orders` WHERE (`orders`.owner = 'u1')) AS `o` \
         USING (id)"
    );
    // A chain: the ON of the join that nullifies it.
    assert_eq!(
        rw("SELECT * FROM a LEFT JOIN orders o ON o.a = a.id LEFT JOIN b ON b.o = o.id"),
        "SELECT * FROM a LEFT JOIN orders o ON (`o`.owner = 'u1') AND (o.a = a.id) LEFT JOIN b \
         ON b.o = o.id"
    );
}

#[test]
fn writes() {
    assert_eq!(
        rw("UPDATE orders SET total = 1 WHERE id = 5"),
        "UPDATE orders SET total = 1 WHERE (`orders`.owner = 'u1' AND `orders`.locked = 0) AND \
         (id = 5)"
    );
    assert_eq!(
        rw("UPDATE orders SET total = total + 1 ORDER BY id LIMIT 2"),
        "UPDATE orders SET total = total + 1 WHERE (`orders`.owner = 'u1' AND `orders`.locked = \
         0) ORDER BY id LIMIT 2"
    );
    assert_eq!(
        rw("DELETE FROM orders WHERE id = 5 OR 1=1"),
        "DELETE FROM orders WHERE (`orders`.owner = 'u1' AND `orders`.del = 1) AND (id = 5 OR 1=1)"
    );
    assert_eq!(
        rw("DELETE FROM orders"),
        "DELETE FROM orders WHERE (`orders`.owner = 'u1' AND `orders`.del = 1)"
    );
    // Multi-table: the target gets its write policies, the other table
    // only SELECT's.
    assert_eq!(
        rw("UPDATE c JOIN orders o ON o.c = c.id SET c.n = o.n"),
        "UPDATE c JOIN orders o ON o.c = c.id SET c.n = o.n WHERE (`o`.owner = 'u1')"
    );
    assert_eq!(
        rw("DELETE o FROM orders o JOIN c ON o.c = c.id WHERE c.x = 1"),
        "DELETE o FROM orders o JOIN c ON o.c = c.id WHERE (`o`.owner = 'u1' AND `o`.del = 1) AND \
         (c.x = 1)"
    );
    // INSERT ... SELECT: the source filtered; the target is MariaDB's.
    let i = inject(
        "INSERT INTO orders (a) SELECT a FROM orders WHERE b = 1",
        &LexOptions::mariadb(),
        &Mock,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        i.sql,
        "INSERT INTO orders (a) SELECT a FROM orders WHERE (`orders`.owner = 'u1') AND (b = 1)"
    );
    assert_eq!(i.written, vec![1]);
    let i = inject(
        "INSERT INTO orders VALUES (1, 2)",
        &LexOptions::mariadb(),
        &Mock,
    )
    .unwrap()
    .unwrap();
    assert_eq!(i.sql, "INSERT INTO orders VALUES (1, 2)");
    assert_eq!(i.written, vec![1]);
    assert!(i.filtered.is_empty());
    assert_eq!(
        rw(
            "INSERT INTO t (a) VALUES ((SELECT count(*) FROM orders)) ON DUPLICATE KEY UPDATE a = (SELECT 1 FROM secrets)"
        ),
        "INSERT INTO t (a) VALUES ((SELECT count(*) FROM orders WHERE (`orders`.owner = 'u1'))) \
         ON DUPLICATE KEY UPDATE a = (SELECT 1 FROM secrets WHERE (FALSE))"
    );
}

#[test]
fn refusals_fail_closed() {
    for s in [
        "EXPLAIN SELECT * FROM orders",
        "TABLE orders",
        "SELECT * FROM orders; SELECT 1",
        "CREATE TABLE x AS SELECT * FROM orders",
        "SELECT * FROM orders o FULL OUTER JOIN c ON 1",
        "SELECT * FROM v_orders",
        "SELECT F_ORDERS(1) FROM t",
    ] {
        let e = refused(s);
        assert_eq!(e.code(), Some(SprigCode::RlsUnsupported), "{s}: {e}");
    }
}

#[test]
fn the_context_variable_is_reserved() {
    let l = LexOptions::mariadb();
    for s in [
        "SET @sprig_rls = '{}'",
        "SELECT @`SPRIG_RLS`",
        "SELECT 1 INTO @'sprig_rls_x'",
        "SET @Sprig_Rls := 1",
    ] {
        assert!(super::names_context_variable(s.as_bytes(), &l), "{s}");
    }
    assert!(!super::names_context_variable(
        b"SELECT @sprig, 'sprig_rls'",
        &l
    ));
}

/// Protects `t1` and `t3` with `k % 3 <> 0` for the differential run.
struct DiffMock;

impl Resolver for DiffMock {
    fn mentions(&self, word: &str) -> bool {
        word == "t1" || word == "t3"
    }

    fn protected(&self, _db: Option<&str>, table: &str) -> Option<usize> {
        match table {
            "t1" => Some(1),
            "t3" => Some(3),
            _ => None,
        }
    }

    fn predicate(&self, _key: usize, _cmds: &[Cmd], qual: &str) -> Result<String, SprigError> {
        Ok(format!("{qual}.k IS NOT NULL AND {qual}.k % 3 <> 0"))
    }
}

/// A small deterministic generator (no extra crates).
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % n
    }
}

/// One random statement in two forms: as the client writes it, and with
/// each protected table replaced by a filtered derived table
/// (the reference semantics).
fn random_pair(r: &mut Lcg) -> (String, String) {
    let n = 2 + r.next(3);
    let tables: Vec<u64> = (0..n).map(|_| 1 + r.next(4)).collect();
    let factor = |i: usize, derived: bool| {
        let t = tables[i];
        if derived && (t == 1 || t == 3) {
            format!("(SELECT * FROM t{t} WHERE t{t}.k IS NOT NULL AND t{t}.k % 3 <> 0) AS a{i}")
        } else {
            format!("t{t} AS a{i}")
        }
    };
    let joins = ["JOIN", "LEFT JOIN", "RIGHT JOIN", "LEFT JOIN", ","];
    let mut picks = Vec::new();
    for i in 1..tables.len() {
        let j = joins[usize::try_from(r.next(5)).unwrap()];
        let other = r.next(u64::try_from(i).unwrap());
        let col = ["id", "k", "v"][usize::try_from(r.next(3)).unwrap()];
        picks.push((j, other, col));
    }
    let nest = tables.len() >= 3 && r.next(3) == 0;
    let where_kind = r.next(4);
    let sub_t = 1 + r.next(4);
    let agg = r.next(3) == 0;
    let build = |derived: bool| {
        let mut s = factor(0, derived);
        for (i, (j, other, col)) in picks.iter().enumerate() {
            let i = i + 1;
            if *j == "," {
                s = format!("{s}, {}", factor(i, derived));
            } else if nest && i == 1 {
                // a0 J (a1 JOIN a2 ON ...) ON ...: built when i == 2.
            } else if nest && i == 2 {
                s = format!(
                    "{s} {j} ({} JOIN {} ON a2.{col} = a1.{col}) ON a1.{col} = a0.{col}",
                    factor(1, derived),
                    factor(2, derived)
                );
            } else {
                s = format!(
                    "{s} {j} {} ON a{i}.{col} = a{other}.{col}",
                    factor(i, derived)
                );
            }
        }
        let sub = if derived && (sub_t == 1 || sub_t == 3) {
            format!(
                "(SELECT * FROM t{sub_t} WHERE t{sub_t}.k IS NOT NULL AND t{sub_t}.k % 3 <> 0) s"
            )
        } else {
            format!("t{sub_t} s")
        };
        let w = match where_kind {
            0 => String::new(),
            1 => " WHERE a0.v > 2 OR a0.v IS NULL".to_owned(),
            2 => format!(" WHERE a0.id IN (SELECT s.id FROM {sub})"),
            _ => format!(" WHERE NOT EXISTS (SELECT 1 FROM {sub} WHERE s.k = a0.k)"),
        };
        let cols = (0..tables.len())
            .map(|i| format!("a{i}.id, a{i}.k"))
            .collect::<Vec<_>>()
            .join(", ");
        if agg {
            format!("SELECT COUNT(*), SUM(a0.v) FROM {s}{w}")
        } else {
            format!("SELECT {cols} FROM {s}{w}")
        }
    };
    (build(false), build(true))
}

/// Writes `SPRIG_TEST_RLS_DIFF_N` (default 500) pairs, rewritten and
/// reference, to `SPRIG_TEST_RLS_DIFF_OUT` for a differential script to
/// run on MariaDB.
#[test]
#[ignore = "generates the differential corpus for a live run"]
fn differential_corpus() {
    let Ok(out) = std::env::var("SPRIG_TEST_RLS_DIFF_OUT") else {
        return;
    };
    let n: usize = std::env::var("SPRIG_TEST_RLS_DIFF_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500);
    let mut r = Lcg(0x5eed_5eed);
    let mut lines = Vec::new();
    let mut refused = 0;
    for _ in 0..n {
        let (client, reference) = random_pair(&mut r);
        match inject(&client, &LexOptions::mariadb(), &DiffMock) {
            Ok(Some(i)) => lines.push(format!("{}\t{}", i.sql, reference)),
            Ok(None) => lines.push(format!("{client}\t{reference}")),
            Err(e) => {
                refused += 1;
                eprintln!("refused: {client}: {e}");
            }
        }
    }
    eprintln!("{} pairs, {refused} refused", lines.len());
    std::fs::write(out, lines.join("\n")).unwrap();
}

/// `Mock`, whose `orders` references under `__sprig_lyr` are layers of
/// merged views the caller filtered (chapter 42).
struct Merged;

impl Resolver for Merged {
    fn mentions(&self, word: &str) -> bool {
        Mock.mentions(word)
    }

    fn protected(&self, db: Option<&str>, table: &str) -> Option<usize> {
        Mock.protected(db, table)
    }

    fn predicate(&self, key: usize, cmds: &[Cmd], qual: &str) -> Result<String, SprigError> {
        Mock.predicate(key, cmds, qual)
    }

    fn merged_layer(&self, key: usize, alias: &str) -> bool {
        key == 1 && alias == "__sprig_lyr"
    }
}

#[test]
fn merged_view_layers_are_left_to_the_merged_filter() {
    let lex = LexOptions::mariadb();
    // Only merged-view layers: nothing edited, the table reported merged.
    let sql = "SELECT * FROM (SELECT * FROM (WITH l AS (SELECT id FROM orders `__sprig_lyr` \
               WHERE x = 1) SELECT id FROM l) AS `__sprig_m` WHERE (`__sprig_m`.owner = 'u1')) \
               AS orders";
    let i = inject(sql, &lex, &Merged).unwrap().unwrap();
    assert_eq!(i.sql, sql);
    assert_eq!(i.merged, vec![1]);
    assert!(i.filtered.is_empty());
    // A reference under any other alias, or a write target, is filtered.
    let i = inject(
        "SELECT o.id FROM orders o, orders `__sprig_lyr` WHERE o.id = `__sprig_lyr`.id",
        &lex,
        &Merged,
    )
    .unwrap()
    .unwrap();
    assert!(i.sql.contains("`o`.owner = 'u1'"), "{}", i.sql);
    assert_eq!((i.filtered, i.merged), (vec![1], vec![1]));
    let i = inject(
        "UPDATE orders `__sprig_lyr` SET a = 1 WHERE id = 2",
        &lex,
        &Merged,
    )
    .unwrap()
    .unwrap();
    assert!(i.sql.contains("`__sprig_lyr`.locked = 0"), "{}", i.sql);
    assert!(i.merged.is_empty());
    // The plain resolver never skips a reference.
    let i = inject("SELECT id FROM orders `__sprig_lyr`", &lex, &Mock)
        .unwrap()
        .unwrap();
    assert!(i.sql.contains("owner = 'u1'") && i.merged.is_empty());
}
