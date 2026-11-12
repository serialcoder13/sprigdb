//! MariaDB's side of row-level security (chapter 29, "Inside MariaDB";
//! build step 24): `BEFORE INSERT`, `BEFORE UPDATE` and `BEFORE DELETE`
//! triggers on each shard of a protected table that `SIGNAL` 39363 when a
//! row version fails the policies of the session's context.
//!
//! The context is the user variable `@sprig_rls`, which the gateway sets
//! on every backend connection of a tenant with row-level security (a
//! JSON document; clients can never name the variable):
//!
//! ```text
//! {"b": 0|1,              BYPASS RLS at tenant level
//!  "x": [object ids],     tables with security on that don't bind the session
//!  "p": [policy ids],     the policies that apply to the session
//!  "s": {"user", "uid", "tid", "htid", "space", "claims", "attrs", "sub", "man"}}
//! ```
//!
//! ```text
//! ok(INSERT) = internal OR exempt OR CHECK(INSERT)(NEW)
//! ok(UPDATE) = internal OR exempt OR (USING(UPDATE)(OLD) AND CHECK(UPDATE)(NEW))
//! ok(DELETE) = internal OR exempt OR USING(DELETE)(OLD)
//! internal   = no context and a session that isn't a tenant user account
//!              (sprig_ctl: DDL copies, the mover, backfills)
//! exempt     = "b" = 1, or the table in "x"
//! USING(c)   = (OR over permissive p: p in "p" AND p.using) AND
//!              (AND over restrictive p: p not in "p" OR p.using); no permissive: FALSE
//! CHECK(c)   = the same over p.check, else p.using
//! IF NOT COALESCE(ok, FALSE) THEN SIGNAL SQLSTATE '42000' SET MYSQL_ERRNO = 39363
//! ```
//!
//! A NULL result fails, as in PostgreSQL. `INSERT ... ON DUPLICATE KEY
//! UPDATE` reaches the conflicting row through `BEFORE UPDATE`, and
//! `REPLACE` through `BEFORE DELETE` (MariaDB deletes and inserts when a
//! table has delete triggers), so both refuse a hidden conflicting row.

use sprig_common::SprigError;

use super::expr::{Binder, invalid, render};
use super::text::{quote, sql_string};
use crate::lex::LexOptions;

/// The user variable holding the session's context.
pub const CONTEXT_VAR: &str = "sprig_rls";

/// A policy as the triggers need it.
#[derive(Debug, Clone, Copy)]
pub struct TriggerPolicy<'a> {
    /// The id (`"p"` lists it when the policy applies to the session).
    pub id: u32,
    /// `AS RESTRICTIVE`.
    pub restrictive: bool,
    /// Command bits (`SELECT` 1, `INSERT` 2, `UPDATE` 4, `DELETE` 8).
    pub commands: u8,
    /// `USING`, as stored.
    pub using: Option<&'a str>,
    /// `WITH CHECK`, as stored.
    pub check: Option<&'a str>,
}

/// One table's triggers on one shard schema.
pub struct TriggerSpec<'a> {
    /// The shard schema.
    pub schema: &'a str,
    /// The table's name there.
    pub table: &'a str,
    /// Its object id (the trigger names, the `"x"` test).
    pub object: u64,
    /// The tenant (`t<tenant>_u` accounts are its users).
    pub tenant: u64,
    /// `` `t42_u7`@`%` ``.
    pub definer: &'a str,
    /// Every policy of the table.
    pub policies: &'a [TriggerPolicy<'a>],
    /// How the stored expressions lex.
    pub lex: LexOptions,
    /// Maps a rendered expression's logical names to physical ones.
    pub physical: &'a dyn Fn(&str) -> Result<String, SprigError>,
}

/// The trigger names of `object` (insert, update, delete).
#[must_use]
pub fn names(object: u64) -> [String; 3] {
    [
        format!("__sprig_rls_bi_{object}"),
        format!("__sprig_rls_bu_{object}"),
        format!("__sprig_rls_bd_{object}"),
    ]
}

/// `DROP TRIGGER IF EXISTS` for each of `object`'s triggers in `schema`.
#[must_use]
pub fn drops(schema: &str, object: u64) -> Vec<String> {
    names(object)
        .iter()
        .map(|n| format!("DROP TRIGGER IF EXISTS {}.{}", quote(schema), quote(n)))
        .collect()
}

/// Session functions as lookups in the context: `@sprig_rls` in
/// triggers, `__sprig_rls_ctx()` in policy views (MariaDB refuses user
/// variables there, 1351).
struct JsonBinder<'a> {
    ctx: &'a str,
}

fn lookup(ctx: &str, path: &str) -> String {
    format!("JSON_VALUE({ctx}, {})", sql_string(path, false))
}

fn json_key(k: &str) -> String {
    let mut s = String::from("\"");
    for ch in k.chars() {
        if ch == '"' || ch == '\\' {
            s.push('\\');
        }
        s.push(ch);
    }
    s.push('"');
    s
}

impl Binder for JsonBinder<'_> {
    fn value(&self, func: &str, arg: Option<&str>) -> Result<String, SprigError> {
        let c = self.ctx;
        Ok(match func {
            "CURRENT_USER" => lookup(c, "$.s.user"),
            "SPRIG_USER_ID" => lookup(c, "$.s.uid"),
            "SPRIG_TENANT_ID" => lookup(c, "$.s.tid"),
            "SPRIG_HOME_TENANT_ID" => lookup(c, "$.s.htid"),
            "SPRIG_SESSION_SPACE" => lookup(c, "$.s.space"),
            "SPRIG_CLAIM" => lookup(c, &format!("$.s.claims.{}", json_key(arg.unwrap_or("")))),
            "SPRIG_USER_ATTR" => lookup(c, &format!("$.s.attrs.{}", json_key(arg.unwrap_or("")))),
            other => return Err(invalid(format!("{other}() can't be bound in MariaDB"))),
        })
    }

    fn membership(&self, func: &str) -> Result<(String, String), SprigError> {
        let path = if func == "SPRIG_TENANT_IN_SUBTREE" {
            "$.s.sub"
        } else {
            "$.s.man"
        };
        Ok((
            format!(
                "JSON_CONTAINS(JSON_EXTRACT({}, {}), CONCAT(",
                self.ctx,
                sql_string(path, false)
            ),
            "))".to_owned(),
        ))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Clause {
    Using,
    Check,
}

impl TriggerSpec<'_> {
    fn render(&self, expr: &str, row: &str, ctx: &str) -> Result<String, SprigError> {
        let r = render(expr, &self.lex, self.table, row, &JsonBinder { ctx })?;
        (self.physical)(&r)
    }

    fn combination(&self, bit: u8, k: Clause, row: &str, ctx: &str) -> Result<String, SprigError> {
        let mut perm = Vec::new();
        let mut restr = Vec::new();
        for p in self.policies {
            if p.commands & bit == 0 {
                continue;
            }
            let e = match k {
                Clause::Using => p.using,
                Clause::Check => p.check.or(p.using),
            };
            let Some(e) = e else { continue };
            let member = format!("JSON_CONTAINS({ctx}, '{}', '$.p')", p.id);
            let body = self.render(e, row, ctx)?;
            if p.restrictive {
                restr.push(format!("(NOT {member} OR ({body}))"));
            } else {
                perm.push(format!("({member} AND ({body}))"));
            }
        }
        let perm = if perm.is_empty() {
            "FALSE".to_owned()
        } else {
            format!("({})", perm.join(" OR "))
        };
        if restr.is_empty() {
            return Ok(perm);
        }
        Ok(format!("({perm} AND {})", restr.join(" AND ")))
    }

    fn guard(&self, ctx: &str) -> String {
        let prefix = format!("t{}_u", self.tenant);
        format!(
            "({v} IS NULL AND LEFT(SUBSTRING_INDEX(USER(), '@', 1), {n}) <> '{prefix}') \
             OR JSON_VALUE({v}, '$.b') = '1' \
             OR JSON_CONTAINS({v}, '{o}', '$.x')",
            v = ctx,
            n = prefix.len(),
            o = self.object
        )
    }

    /// Unqualified, to run with the shard schema as the default database:
    /// moves and regrouping re-create a table's triggers from `SHOW CREATE
    /// TRIGGER` in another schema.
    fn trigger(&self, name: &str, event: &str, ok: &str) -> String {
        let msg = format!(
            "ER_SPRIG_RLS_CHECK_FAILED: new row violates row-level security policy for table '{}'",
            self.table
        );
        format!(
            "CREATE OR REPLACE DEFINER = {definer} TRIGGER {name} BEFORE {event} ON \
             {table} FOR EACH ROW BEGIN IF NOT COALESCE({guard} OR {ok}, FALSE) THEN \
             SIGNAL SQLSTATE '42000' SET MYSQL_ERRNO = 39363, MESSAGE_TEXT = {msg}; END IF; END",
            definer = self.definer,
            name = quote(name),
            table = quote(self.table),
            guard = self.guard(&format!("@{CONTEXT_VAR}")),
            msg = sql_string(&msg, false),
        )
    }

    /// The three `CREATE OR REPLACE TRIGGER` statements, to run after
    /// `USE` of [`TriggerSpec::schema`].
    ///
    /// # Errors
    /// 39364 if a stored expression can't be rendered.
    pub fn statements(&self) -> Result<Vec<String>, SprigError> {
        let [bi, bu, bd] = names(self.object);
        let v = format!("@{CONTEXT_VAR}");
        let ins = self.combination(2, Clause::Check, "NEW", &v)?;
        let upd = format!(
            "({} AND {})",
            self.combination(4, Clause::Using, "OLD", &v)?,
            self.combination(4, Clause::Check, "NEW", &v)?
        );
        let del = self.combination(8, Clause::Using, "OLD", &v)?;
        Ok(vec![
            self.trigger(&bi, "INSERT", &ins),
            self.trigger(&bu, "UPDATE", &upd),
            self.trigger(&bd, "DELETE", &del),
        ])
    }

    /// The policy views (and the context function they read) on
    /// [`TriggerSpec::schema`], to run after `USE` of it: updatable
    /// `MERGE` views of the rows the session's `SELECT` policies show
    /// (`super::views`). `columns` are the table's columns there in order,
    /// `true` for invisible ones (RL28): the first view names the visible
    /// ones (what `SELECT *` on the table shows); a table with invisible
    /// columns gets a second view with all of them, any other loses a stale
    /// one. Without `columns` the first view is `SELECT *`. `definer` must
    /// be able to read and write the table.
    ///
    /// # Errors
    /// 39364 if a stored expression can't be rendered.
    pub fn view_statements(
        &self,
        definer: &str,
        columns: &[(String, bool)],
    ) -> Result<ViewStatements, SprigError> {
        let ctx = format!("{}()", quote(super::views::CTX_FUNCTION));
        let qual = quote(self.table);
        let ok = self.combination(1, Clause::Using, &qual, &ctx)?;
        let views = self.views_over(definer, self.table, columns, &ok, &ctx);
        Ok(ViewStatements {
            function: format!(
                "CREATE OR REPLACE DEFINER = {definer} FUNCTION {f}() RETURNS LONGTEXT \
                 CHARSET utf8mb4 COLLATE utf8mb4_bin NOT DETERMINISTIC NO SQL \
                 SQL SECURITY DEFINER RETURN @{CONTEXT_VAR}",
                f = quote(super::views::CTX_FUNCTION),
            ),
            views,
        })
    }

    /// The two policy views of `from` (this table, or one of its
    /// key-value element tables) keeping the rows where `ok` (or the
    /// guard) holds: the visible columns' view, then the all-columns view
    /// or its `DROP` (RL28).
    fn views_over(
        &self,
        definer: &str,
        from: &str,
        columns: &[(String, bool)],
        ok: &str,
        ctx: &str,
    ) -> Vec<String> {
        let qual = quote(from);
        let list = |all: bool| -> String {
            let v: Vec<String> = columns
                .iter()
                .filter(|(_, inv)| all || !inv)
                .map(|(c, _)| format!("{qual}.{}", quote(c)))
                .collect();
            if v.is_empty() {
                "*".to_owned()
            } else {
                v.join(", ")
            }
        };
        let view = |name: String, all: bool| {
            format!(
                "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = {definer} SQL SECURITY DEFINER \
                 VIEW {v} AS SELECT {cols} FROM {qual} WHERE COALESCE({guard} OR {ok}, FALSE)",
                v = quote(&name),
                cols = list(all),
                guard = self.guard(ctx),
            )
        };
        let mut views = vec![view(super::views::view_name(from, self.object), false)];
        if columns.iter().any(|(_, inv)| *inv) {
            views.push(view(super::views::hidden_view_name(from, self.object), true));
        } else {
            views.push(format!(
                "DROP VIEW IF EXISTS {}",
                quote(&super::views::hidden_view_name(from, self.object))
            ));
        }
        views
    }

    /// The policy views of key-value element table `element` (chapter
    /// 33's `__sprig_ke_<ns>` or `__sprig_kz_<ns>` next to this key table;
    /// RLS round 6, decision RL55), to run after `USE` of
    /// [`TriggerSpec::schema`] once the context function is there: an
    /// element row shows when its key's row passes the key table's
    /// `SELECT` policies, read through a correlated `EXISTS` on the key
    /// table's primary key, so policies on any key-table column (a value,
    /// the type) apply to the structure's elements too. `columns` as in
    /// [`TriggerSpec::view_statements`], for the element table.
    ///
    /// # Errors
    /// 39364 if a stored expression can't be rendered.
    pub fn element_view_statements(
        &self,
        definer: &str,
        element: &str,
        columns: &[(String, bool)],
    ) -> Result<Vec<String>, SprigError> {
        let ctx = format!("{}()", quote(super::views::CTX_FUNCTION));
        let kq = quote(KEY_ALIAS);
        let ok = self.combination(1, Clause::Using, &kq, &ctx)?;
        let ok = format!(
            "EXISTS (SELECT 1 FROM {t} AS {kq} WHERE {kq}.`k` = {e}.`k` AND {ok})",
            t = quote(self.table),
            e = quote(element),
        );
        Ok(self.views_over(definer, element, columns, &ok, &ctx))
    }
}

/// The key table's alias inside a key-value element policy (the views'
/// `EXISTS`, and the gateway's rewrite of element tables).
pub const KEY_ALIAS: &str = "__sprig_kq";

/// The prefixes of a key-value namespace's element and sorted-set tables
/// (chapter 33): the namespace's key table name follows.
pub const KV_ELEMENT_PREFIXES: [&str; 2] = ["__sprig_ke_", "__sprig_kz_"];

/// The element tables of key-value key table `table`, by name.
#[must_use]
pub fn kv_element_tables(table: &str) -> [String; 2] {
    KV_ELEMENT_PREFIXES.map(|p| format!("{p}{table}"))
}

impl TriggerSpec<'_> {
    /// The filter view of a change topic of this table (chapter 29, RLS
    /// round 5, decision RL47), to run after `USE` of the topic's log
    /// schema: view `view` over log table `log` showing each reader (by the
    /// context `__sprig_rls_ctx()` of its connection) the records whose
    /// row images its `SELECT` policies show, an update that hides or shows
    /// the row as a delete or an insert (`sprig_streams::read`'s rules).
    /// `columns` is the table's `JSON_TABLE` column list; `filtered` false
    /// (security off) makes a plain view of the log.
    ///
    /// # Errors
    /// 39364 if a stored expression can't be rendered.
    pub fn change_view_statement(
        &self,
        view: &str,
        log: &str,
        columns: &str,
        definer: &str,
        filtered: bool,
    ) -> Result<String, SprigError> {
        const COLS: &str = "partition_no, offset_no, day, ts, k";
        const TAIL: &str = "headers, source_event, leader_epoch";
        let head = format!(
            "CREATE OR REPLACE DEFINER = {definer} SQL SECURITY DEFINER VIEW {} AS ",
            quote(view)
        );
        if !filtered {
            return Ok(format!(
                "{head}SELECT {COLS}, v, {TAIL} FROM {}",
                quote(log)
            ));
        }
        let ctx = format!("{}()", quote(super::views::CTX_FUNCTION));
        let qual = "`__sprig_ci`";
        let ok = self.combination(1, Clause::Using, qual, &ctx)?;
        let guard = self.guard(&ctx);
        let v = "CONVERT(`__sprig_cl`.v USING utf8mb4)";
        let vis = |path: &str| {
            format!(
                "COALESCE(({guard}) OR (JSON_TYPE(JSON_EXTRACT({v}, '{path}')) = 'OBJECT' AND \
                 EXISTS (SELECT 1 FROM JSON_TABLE({v}, '{path}' COLUMNS ({columns})) AS {qual} \
                 WHERE {ok})), FALSE)"
            )
        };
        let set = |op: &str, gone: &str| {
            format!(
                "CAST(JSON_COMPACT(JSON_SET(CONVERT(x.v USING utf8mb4), '$.op', '{op}', \
                 '$.{gone}', NULL)) AS BINARY)"
            )
        };
        Ok(format!(
            "{head}SELECT x.partition_no, x.offset_no, x.day, x.ts, x.k, CASE WHEN x.o = 'u' AND \
             NOT x.a THEN {to_d} WHEN x.o = 'u' AND NOT x.b THEN {to_c} ELSE x.v END AS v, \
             x.headers, x.source_event, x.leader_epoch FROM (SELECT `__sprig_cl`.partition_no, \
             `__sprig_cl`.offset_no, `__sprig_cl`.day, `__sprig_cl`.ts, `__sprig_cl`.k, \
             `__sprig_cl`.v, `__sprig_cl`.headers, `__sprig_cl`.source_event, \
             `__sprig_cl`.leader_epoch, JSON_VALUE({v}, '$.op') AS o, {b} AS b, {a} AS a FROM {log} \
             AS `__sprig_cl`) AS x WHERE x.o IS NULL OR x.o NOT IN ('c', 'u', 'd') OR x.b OR x.a",
            to_d = set("d", "after"),
            to_c = set("c", "before"),
            b = vis("$.before"),
            a = vis("$.after"),
            log = quote(log),
        ))
    }
}

/// [`TriggerSpec::view_statements`]: the context function, then (after
/// its definer may run it) the views.
#[derive(Debug, Clone)]
pub struct ViewStatements {
    /// `CREATE OR REPLACE FUNCTION __sprig_rls_ctx()`.
    pub function: String,
    /// The policy view, then the all-columns view (or its `DROP`).
    pub views: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn triggers_read_the_context() {
        let pols = [
            TriggerPolicy {
                id: 3,
                restrictive: false,
                commands: 15,
                using: Some("owner = SPRIG_CLAIM('sub')"),
                check: None,
            },
            TriggerPolicy {
                id: 4,
                restrictive: true,
                commands: 2,
                using: None,
                check: Some("amount < 100"),
            },
        ];
        let physical = |s: &str| Ok(s.to_owned());
        let spec = TriggerSpec {
            schema: "t42_d1_g1_s1",
            table: "orders",
            object: 77,
            tenant: 42,
            definer: "`t42_u7`@`%`",
            policies: &pols,
            lex: LexOptions::mariadb(),
            physical: &physical,
        };
        let s = spec.statements().unwrap();
        assert_eq!(s.len(), 3);
        assert!(s[0].starts_with(
            "CREATE OR REPLACE DEFINER = `t42_u7`@`%` TRIGGER `__sprig_rls_bi_77` \
             BEFORE INSERT ON `orders`"
        ));
        assert!(
            s[0].contains(
                "((JSON_CONTAINS(@sprig_rls, '3', '$.p') AND (NEW.owner = \
             JSON_VALUE(@sprig_rls, '$.s.claims.\"sub\"')))) AND (NOT JSON_CONTAINS(@sprig_rls, \
             '4', '$.p') OR (NEW.amount < 100)))"
            ),
            "{}",
            s[0]
        );
        assert!(s[0].contains("LEFT(SUBSTRING_INDEX(USER(), '@', 1), 5) <> 't42_u'"));
        assert!(s[1].contains("OLD.owner") && s[1].contains("NEW.owner"));
        assert!(s[2].contains("BEFORE DELETE") && !s[2].contains("NEW."));
        let vs = spec.view_statements("`t42_u7`@`%`", &[]).unwrap();
        let v = [vs.function.clone(), vs.views[0].clone()];
        assert!(
            v[0].contains("FUNCTION `__sprig_rls_ctx`() RETURNS LONGTEXT"),
            "{}",
            v[0]
        );
        assert!(
            v[1].contains(
                "VIEW `__sprig_rls_orders` AS SELECT * FROM `orders` WHERE COALESCE((`__sprig_rls_ctx`() IS NULL"
            ) && v[1].contains("`orders`.owner = JSON_VALUE(`__sprig_rls_ctx`(), '$.s.claims.\"sub\"')")
                && !v[1].contains("@sprig_rls"),
            "{}",
            v[1]
        );
        assert_eq!(vs.views[1], "DROP VIEW IF EXISTS `__sprig_rlsh_orders`");
        // RL28: named columns; invisible ones only in the second view.
        let cols = [
            ("id".to_owned(), false),
            ("_sprig_rk".to_owned(), true),
            ("owner".to_owned(), false),
        ];
        let vs = spec.view_statements("`t42_u7`@`%`", &cols).unwrap();
        assert!(
            vs.views[0].contains(
                "VIEW `__sprig_rls_orders` AS SELECT `orders`.`id`, `orders`.`owner` FROM `orders` WHERE"
            ),
            "{}",
            vs.views[0]
        );
        assert!(
            vs.views[1].contains(
                "VIEW `__sprig_rlsh_orders` AS SELECT `orders`.`id`, `orders`.`_sprig_rk`, `orders`.`owner` FROM `orders` WHERE"
            ),
            "{}",
            vs.views[1]
        );
        assert_eq!(
            drops("s", 9)[2],
            "DROP TRIGGER IF EXISTS `s`.`__sprig_rls_bd_9`"
        );
    }

    #[test]
    fn element_views_follow_the_key_tables_policies() {
        // RL55: a policy on the key table's value column reaches the
        // namespace's elements through the key row.
        let pols = [TriggerPolicy {
            id: 5,
            restrictive: false,
            commands: 1,
            using: Some("k LIKE 'alice:%' OR v = 'shared'"),
            check: None,
        }];
        let physical = |s: &str| Ok(s.to_owned());
        let spec = TriggerSpec {
            schema: "t42_d3_g1_s1",
            table: "db0",
            object: 81,
            tenant: 42,
            definer: "`t42_u7`@`%`",
            policies: &pols,
            lex: LexOptions::mariadb(),
            physical: &physical,
        };
        let [ke, kz] = kv_element_tables("db0");
        assert_eq!((ke.as_str(), kz.as_str()), ("__sprig_ke_db0", "__sprig_kz_db0"));
        let cols = [
            ("k".to_owned(), false),
            ("e".to_owned(), false),
            ("_sprig_dk".to_owned(), true),
        ];
        let v = spec
            .element_view_statements("`t42_u7`@`%`", &ke, &cols)
            .unwrap();
        assert_eq!(v.len(), 2);
        assert!(
            v[0].starts_with(
                "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `t42_u7`@`%` SQL SECURITY DEFINER \
                 VIEW `__sprig_rls___sprig_ke_db0` AS SELECT `__sprig_ke_db0`.`k`, \
                 `__sprig_ke_db0`.`e` FROM `__sprig_ke_db0` WHERE COALESCE((`__sprig_rls_ctx`() IS NULL"
            ),
            "{}",
            v[0]
        );
        assert!(
            v[0].contains(
                "OR EXISTS (SELECT 1 FROM `db0` AS `__sprig_kq` WHERE `__sprig_kq`.`k` = \
                 `__sprig_ke_db0`.`k` AND ((JSON_CONTAINS(`__sprig_rls_ctx`(), '5', '$.p') AND \
                 (`__sprig_kq`.k LIKE 'alice:%' OR `__sprig_kq`.v = 'shared')))), FALSE)"
            ),
            "{}",
            v[0]
        );
        // The guard names the key table's object: exempt sessions see all.
        assert!(v[0].contains("JSON_CONTAINS(`__sprig_rls_ctx`(), '81', '$.x')"));
        assert!(
            v[1].contains("VIEW `__sprig_rlsh___sprig_ke_db0` AS SELECT `__sprig_ke_db0`.`k`, `__sprig_ke_db0`.`e`, `__sprig_ke_db0`.`_sprig_dk` FROM"),
            "{}",
            v[1]
        );
        assert!(super::super::views::is_policy_view("__sprig_rls___sprig_kz_db0"));
        assert_eq!(
            super::super::views::unswap("FROM `__sprig_rls___sprig_kz_db0` AS `__sprig_kz_db0` x"),
            "FROM `__sprig_kz_db0` x"
        );
    }
}
