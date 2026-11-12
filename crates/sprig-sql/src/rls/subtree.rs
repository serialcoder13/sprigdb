//! `__sprig_subtree`: tenant subtrees too large to inline (chapter 29;
//! RLS round 6, decision RL58).
//!
//! `SPRIG_TENANT_IN_SUBTREE(x)` and `SPRIG_TENANT_MANAGED(x)` become
//! `x IN (id, ...)` up to `rls.inline_subtree_max` tenants (RL22). Above
//! it, they read a table on each cluster the tenant uses, beside chapter
//! 07's names table:
//!
//! ```text
//! __sprig_sys.__sprig_subtree (ancestor, descendant)
//!   one row per tenant d of tenant a's subtree (a itself included), for
//!   the tenants whose policies need it; written by the gateways (sprig_ctl)
//!   from the directory, when the subtree they read differs from what they
//!   last wrote
//! __sprig_sys.__sprig_subtree_v (descendant)
//!   the rows of the session's own tenant (from SESSION_USER(), the
//!   backend account t<a>_u<n>); SELECT granted to PUBLIC, so every
//!   account reads its own tenant's subtree and no other
//! ```
//!
//! The rewrite then binds `x IN (SELECT descendant FROM
//! __sprig_sys.__sprig_subtree_v)`; MariaDB's policy views and triggers
//! read the same view when the session's context says so (`"s"."subt"`,
//! `"s"."mant"`), else the context's own lists.

use super::text::quote;

/// The schema (chapter 07's).
pub const SYS_SCHEMA: &str = "__sprig_sys";
/// The table.
pub const TABLE: &str = "__sprig_subtree";
/// The view every account reads.
pub const VIEW: &str = "__sprig_subtree_v";

/// The statements that create the table and its view (idempotent).
#[must_use]
pub fn setup_sql() -> [String; 3] {
    let t = format!("{}.{}", quote(SYS_SCHEMA), quote(TABLE));
    let v = format!("{}.{}", quote(SYS_SCHEMA), quote(VIEW));
    [
        format!(
            "CREATE TABLE IF NOT EXISTS {t} (`ancestor` BIGINT UNSIGNED NOT NULL, \
             `descendant` BIGINT UNSIGNED NOT NULL, PRIMARY KEY (`ancestor`, `descendant`)) \
             ENGINE=InnoDB"
        ),
        // The session's tenant from its backend account, `t<a>_u<n>@host`
        // (no warning for other accounts: they read nothing).
        format!(
            "CREATE OR REPLACE SQL SECURITY DEFINER VIEW {v} AS SELECT `descendant` FROM {t} \
             WHERE `ancestor` = IF(SESSION_USER() RLIKE '^t[0-9]+_', CAST(SUBSTRING(SESSION_USER(), \
             2, LOCATE('_', SESSION_USER()) - 2) AS UNSIGNED), 0)"
        ),
        format!("GRANT SELECT ON {v} TO PUBLIC"),
    ]
}

/// The subquery a membership reads (`x IN (<this>)`).
#[must_use]
pub fn subquery() -> String {
    format!(
        "SELECT `descendant` FROM {}.{}",
        quote(SYS_SCHEMA),
        quote(VIEW)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_view_reads_the_sessions_tenant() {
        let s = setup_sql();
        assert!(s[0].starts_with("CREATE TABLE IF NOT EXISTS `__sprig_sys`.`__sprig_subtree`"));
        assert!(s[1].contains("SQL SECURITY DEFINER VIEW `__sprig_sys`.`__sprig_subtree_v`"));
        assert_eq!(
            subquery(),
            "SELECT `descendant` FROM `__sprig_sys`.`__sprig_subtree_v`"
        );
    }
}
