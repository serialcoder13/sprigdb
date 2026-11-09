//! What a deleted tenant leaves on one cluster, and removing it (chapter
//! 08, "Deleting a tenant").
//!
//! A tenant's objects on a cluster all carry its id in their names:
//!
//! | Object | Name |
//! |---|---|
//! | per-user backend accounts (chapter 29) | `t<id>_u<user>@'%'` |
//! | MariaDB roles (chapter 29, later) | `t<id>_r<role>`, `t<id>_rc<hash>` |
//! | schemas | `t<id>_sprig`, `t<id>_d<db>_g<g>_s<s>`, `t<id>_d<db>_ref` |
//! | `__sprig_sys.__sprig_names` rows (chapter 07) | an account's or a schema's name |
//!
//! [`read`] finds candidates with `LIKE 't<id>\_%'` (the `_` escaped, so
//! `t3\_%` can't match `t30_u1`), and [`plan`] keeps only names that parse
//! exactly as this tenant's ([`AccountName`], [`SchemaName`]): a name is
//! never matched by pattern alone. Grants, stored routines, events and
//! views go with the accounts and schemas that hold them (`DROP USER`
//! removes an account's rows in `mysql.db`, `tables_priv`, `columns_priv`
//! and `procs_priv`; `DROP DATABASE` its routines and events).
//!
//! Safety rules of the plan:
//! - a `t<id>_sprig` schema here whose owner mark isn't this tenant's
//!   belongs to another tenant with the same id (another deployment on the
//!   same servers): the cluster is left alone ([`Plan::foreign`]);
//! - a data schema is dropped only if its database id is one the catalog
//!   listed; others are reported ([`Plan::left`]), never dropped;
//! - every statement is idempotent (`IF EXISTS`, deletes by exact key),
//!   so a crash mid-way is finished by running the plan again.

use std::collections::BTreeSet;
use std::fmt::Write as _;

use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use sprig_common::ids::{DbId, JobId, TenantId};
use sprig_common::names::{AccountName, SYS_SCHEMA, SchemaName, internal_object, quote};
use sprig_common::{Result, SprigCode, SprigError};

use crate::sql::literal;

/// The `LIKE` pattern that finds a tenant's candidate names: `t<id>\_%`.
/// Only a pre-filter: [`plan`] parses each name exactly.
#[must_use]
pub fn like_prefix(tenant: TenantId) -> String {
    format!("t{tenant}\\_%")
}

/// Whether `user` is exactly one of `tenant`'s backend accounts or roles.
#[must_use]
pub fn owns_account(tenant: TenantId, user: &str) -> bool {
    user.parse::<AccountName>()
        .is_ok_and(|a| a.tenant() == tenant)
}

/// `name` as one of `tenant`'s schemas, if it is exactly one.
#[must_use]
pub fn owned_schema(tenant: TenantId, name: &str) -> Option<SchemaName> {
    name.parse::<SchemaName>()
        .ok()
        .filter(|s| s.tenant() == tenant)
}

/// Whether a `__sprig_names` row's `name` is one of `tenant`'s.
#[must_use]
pub fn owns_name_row(tenant: TenantId, name: &str) -> bool {
    owns_account(tenant, name) || owned_schema(tenant, name).is_some()
}

/// An account found on the cluster.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Account {
    /// `User`.
    pub user: String,
    /// `Host` (empty for a role).
    pub host: String,
    /// A MariaDB role.
    pub role: bool,
}

/// Candidates read from one cluster with [`like_prefix`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Found {
    /// Accounts and roles.
    pub accounts: Vec<Account>,
    /// `__sprig_sys.__sprig_names` rows' `name`.
    pub name_rows: Vec<String>,
    /// Schemas with their comment (a catalog's owner mark).
    pub schemas: Vec<(String, String)>,
}

/// What the caller knows about the tenant.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Known {
    /// The catalog's owner mark (`sprig tenant <id> <incarnation>`);
    /// `None` when unknown: then any mark not starting with
    /// `sprig tenant <id> ` counts as foreign.
    pub owner_mark: Option<String>,
    /// Drop schemas too (tenant deletion); without it only accounts and
    /// name rows go (a mover job's cleaning).
    pub drop_schemas: bool,
    /// Every database id the catalog listed, in any state.
    pub db_ids: BTreeSet<DbId>,
    /// Leave `t<id>_sprig` here: the caller drops the live catalog last.
    pub keep_catalog: bool,
    /// Finished mover jobs of the tenant whose `__sprig_sys` rows go.
    pub jobs: Vec<JobId>,
}

/// What to do on one cluster.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Plan {
    /// Set when `t<id>_sprig` here carries another tenant's mark: nothing
    /// else is planned.
    pub foreign: Option<String>,
    /// Schemas to drop.
    pub drop_schemas: Vec<String>,
    /// Accounts and roles to drop.
    pub drop_accounts: Vec<Account>,
    /// Name rows to delete.
    pub delete_names: Vec<String>,
    /// Jobs whose rows to delete.
    pub jobs: Vec<JobId>,
    /// Schemas of the tenant left in place, with why.
    pub left: Vec<(String, &'static str)>,
}

/// The mover's tables in `__sprig_sys` keyed by job (chapter 10).
const JOB_TABLES: [&str; 3] = ["mover_pos", "watermark", "mover_xa"];

fn sys_table(name: &str) -> String {
    format!("{}.{}", quote(SYS_SCHEMA), quote(&internal_object(name)))
}

impl Plan {
    /// Whether there is nothing to run.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.drop_schemas.is_empty()
            && self.drop_accounts.is_empty()
            && self.delete_names.is_empty()
            && self.jobs.is_empty()
    }

    /// The statements, in order: schemas (and with them routines, events,
    /// views), accounts (and with them their grants), name rows, job rows.
    #[must_use]
    pub fn statements(&self) -> Vec<String> {
        let mut out = Vec::new();
        for s in &self.drop_schemas {
            out.push(format!("DROP DATABASE IF EXISTS {}", quote(s)));
        }
        for a in &self.drop_accounts {
            if a.role {
                out.push(format!("DROP ROLE IF EXISTS {}", literal(&a.user)));
            } else {
                out.push(format!(
                    "DROP USER IF EXISTS {}@{}",
                    literal(&a.user),
                    literal(&a.host)
                ));
            }
        }
        for n in &self.delete_names {
            out.push(format!(
                "DELETE FROM {} WHERE `name` = {}",
                sys_table("names"),
                literal(n)
            ));
        }
        for j in &self.jobs {
            let mut hex = String::with_capacity(32);
            for b in j.to_bytes() {
                let _ = write!(hex, "{b:02x}");
            }
            for t in JOB_TABLES {
                out.push(format!(
                    "DELETE FROM {} WHERE job_id = X'{hex}'",
                    sys_table(t)
                ));
            }
        }
        out
    }
}

/// The plan for `tenant` on a cluster where `found` was read.
#[must_use]
pub fn plan(tenant: TenantId, found: &Found, known: &Known) -> Plan {
    let mut p = Plan::default();
    let catalog = SchemaName::catalog(tenant).to_string();
    let ours = |mark: &str| match &known.owner_mark {
        Some(m) => mark == m,
        None => mark.starts_with(&format!("sprig tenant {tenant} ")),
    };
    if let Some((_, mark)) = found.schemas.iter().find(|(s, _)| *s == catalog)
        && !mark.is_empty()
        && !ours(mark)
    {
        p.foreign = Some(mark.clone());
        return p;
    }
    if known.drop_schemas {
        for (name, mark) in &found.schemas {
            let Some(s) = owned_schema(tenant, name) else {
                continue;
            };
            match s.db() {
                None if known.keep_catalog => {}
                None if !mark.is_empty() && known.owner_mark.is_some() && ours(mark) => {
                    p.drop_schemas.push(name.clone());
                }
                None => p
                    .left
                    .push((name.clone(), "a catalog copy without this tenant's mark")),
                Some(d) if known.db_ids.contains(&d) => p.drop_schemas.push(name.clone()),
                Some(_) => p
                    .left
                    .push((name.clone(), "a database the catalog doesn't list")),
            }
        }
    }
    p.drop_accounts = found
        .accounts
        .iter()
        .filter(|a| owns_account(tenant, &a.user))
        .cloned()
        .collect();
    p.delete_names = found
        .name_rows
        .iter()
        .filter(|n| owns_name_row(tenant, n))
        .cloned()
        .collect();
    p.jobs.clone_from(&known.jobs);
    p.drop_schemas.sort();
    p.drop_accounts.sort();
    p.delete_names.sort();
    p.delete_names.dedup();
    p
}

fn failed(what: &str, e: &mysql_async::Error) -> SprigError {
    SprigError::new(
        SprigCode::ClusterUnavailable,
        format!("tenant cleanup: {what}: {e}"),
    )
}

/// "Unknown database" or "no such table": a cluster without `__sprig_sys`
/// or without the table has no rows to remove.
fn missing(e: &mysql_async::Error) -> bool {
    matches!(e, mysql_async::Error::Server(s) if matches!(s.code, 1049 | 1146))
}

/// Reads `tenant`'s candidates on the cluster behind `conn` (`sprig_ctl`
/// on the writer).
///
/// # Errors
/// 39100 for a failed query.
pub async fn read(conn: &mut Conn, tenant: TenantId) -> Result<Found> {
    let like = like_prefix(tenant);
    let accounts: Vec<(String, String, String)> = conn
        .exec(
            "SELECT User, Host, IFNULL(JSON_VALUE(Priv, '$.is_role'), 'false') \
             FROM mysql.global_priv WHERE User LIKE ?",
            (&like,),
        )
        .await
        .map_err(|e| failed("read mysql.global_priv", &e))?;
    let schemas: Vec<(String, String)> = conn
        .exec(
            "SELECT SCHEMA_NAME, IFNULL(SCHEMA_COMMENT, '') FROM information_schema.SCHEMATA \
             WHERE SCHEMA_NAME LIKE ?",
            (&like,),
        )
        .await
        .map_err(|e| failed("list schemas", &e))?;
    let name_rows: Vec<String> = match conn
        .exec(
            format!(
                "SELECT `name` FROM {} WHERE `name` LIKE ?",
                sys_table("names")
            ),
            (&like,),
        )
        .await
    {
        Ok(v) => v,
        Err(e) if missing(&e) => Vec::new(),
        Err(e) => return Err(failed("read __sprig_names", &e)),
    };
    Ok(Found {
        accounts: accounts
            .into_iter()
            .map(|(user, host, role)| Account {
                user,
                host,
                role: role.eq_ignore_ascii_case("true"),
            })
            .collect(),
        name_rows,
        schemas,
    })
}

/// Runs `plan` on `conn`; returns the statements run.
///
/// # Errors
/// 39100 for a failed statement (a missing table counts as done).
pub async fn apply(conn: &mut Conn, plan: &Plan) -> Result<usize> {
    let mut n = 0;
    for s in plan.statements() {
        match conn.query_drop(&s).await {
            Ok(()) => n += 1,
            Err(e) if missing(&e) => {}
            Err(e) => return Err(failed(&s, &e)),
        }
    }
    Ok(n)
}

/// [`read`], [`plan`] and [`apply`] in one call; returns the plan.
///
/// # Errors
/// As [`read`] and [`apply`].
pub async fn purge(conn: &mut Conn, tenant: TenantId, known: &Known) -> Result<Plan> {
    let found = read(conn, tenant).await?;
    let p = plan(tenant, &found, known);
    if p.foreign.is_none() {
        apply(conn, &p).await?;
    }
    Ok(p)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn acct(u: &str) -> Account {
        Account {
            user: u.to_owned(),
            host: "%".to_owned(),
            role: false,
        }
    }

    fn mark(t: u64) -> String {
        format!("sprig tenant {t} 2026-11-09T00:00:00Z")
    }

    /// Tenants 3, 30, 300 and 31 share the leading digit: only 3's names
    /// are ever 3's.
    fn crowded() -> Found {
        let names = [
            "t3_u1",
            "t3_u10",
            "t30_u1",
            "t300_u1",
            "t31_u3",
            "t3_r2",
            "t3_rc0000abcd",
            "t30_r2",
            "t3x_u1",
            "t03_u1",
            "t3_u01",
            "t3_u1x",
            "t3_",
            "t3_d1_g1_s1",
        ];
        let mut accounts: Vec<Account> = names.iter().map(|n| acct(n)).collect();
        accounts.push(Account {
            user: "t3_r7".into(),
            host: String::new(),
            role: true,
        });
        Found {
            accounts,
            name_rows: [
                "t3_u1",
                "t30_u1",
                "t3_d1_g1_s1",
                "t30_d1_g1_s1",
                "t3_d1_g1_s1_x",
                "t3_sprig",
            ]
            .iter()
            .map(|s| (*s).to_owned())
            .collect(),
            schemas: vec![
                ("t3_sprig".into(), mark(3)),
                ("t30_sprig".into(), mark(30)),
                ("t3_d1_g1_s1".into(), String::new()),
                ("t3_d1_ref".into(), String::new()),
                ("t3_d9_g1_s1".into(), String::new()),
                ("t30_d1_g1_s1".into(), String::new()),
                ("t3_d1_g1_s1_old".into(), String::new()),
            ],
        }
    }

    #[test]
    fn exact_names_only_three_is_never_thirty() {
        let t = TenantId(3);
        assert_eq!(like_prefix(t), "t3\\_%");
        for yes in ["t3_u1", "t3_u10", "t3_r2", "t3_rc0000abcd"] {
            assert!(owns_account(t, yes), "{yes}");
        }
        for no in [
            "t30_u1",
            "t300_u1",
            "t31_u3",
            "t30_r2",
            "t3x_u1",
            "t03_u1",
            "t3_u01",
            "t3_u1x",
            "t3_",
            "t3_d1_g1_s1",
            "t3",
            "u3_u1",
        ] {
            assert!(!owns_account(t, no), "{no}");
        }
        assert!(owned_schema(t, "t3_d1_g1_s1").is_some());
        assert!(owned_schema(t, "t3_sprig").is_some());
        assert!(owned_schema(t, "t30_d1_g1_s1").is_none());
        assert!(owned_schema(t, "t3_d1_g1_s1_old").is_none());
        assert!(owned_schema(TenantId(30), "t3_d1_g1_s1").is_none());
    }

    #[test]
    fn the_plan_drops_only_the_tenants_own() {
        let t = TenantId(3);
        let known = Known {
            owner_mark: Some(mark(3)),
            drop_schemas: true,
            db_ids: [DbId(1)].into_iter().collect(),
            keep_catalog: false,
            jobs: Vec::new(),
        };
        let p = plan(t, &crowded(), &known);
        assert_eq!(p.foreign, None);
        assert_eq!(p.drop_schemas, ["t3_d1_g1_s1", "t3_d1_ref", "t3_sprig"]);
        let users: Vec<&str> = p.drop_accounts.iter().map(|a| a.user.as_str()).collect();
        assert_eq!(
            users,
            ["t3_r2", "t3_r7", "t3_rc0000abcd", "t3_u1", "t3_u10"]
        );
        assert_eq!(p.delete_names, ["t3_d1_g1_s1", "t3_sprig", "t3_u1"]);
        assert_eq!(
            p.left,
            [(
                "t3_d9_g1_s1".to_owned(),
                "a database the catalog doesn't list"
            )]
        );
        let sql = p.statements().join(";\n");
        assert!(sql.contains("DROP USER IF EXISTS 't3_u1'@'%'"), "{sql}");
        assert!(sql.contains("DROP ROLE IF EXISTS 't3_r7'"), "{sql}");
        assert!(
            sql.contains("DROP DATABASE IF EXISTS `t3_d1_g1_s1`"),
            "{sql}"
        );
        assert!(
            sql.contains("DELETE FROM `__sprig_sys`.`__sprig_names` WHERE `name` = 't3_u1'"),
            "{sql}"
        );
        for other in ["t30", "t300", "t31", "t3x", "t03", "t3_u01", "t3_d9"] {
            assert!(!sql.contains(&format!("'{other}")), "{other} in {sql}");
            assert!(!sql.contains(&format!("`{other}")), "{other} in {sql}");
        }
        // Thirty's plan in the same crowd touches none of three's.
        let p30 = plan(
            TenantId(30),
            &crowded(),
            &Known {
                owner_mark: Some(mark(30)),
                drop_schemas: true,
                db_ids: [DbId(1)].into_iter().collect(),
                ..Known::default()
            },
        );
        assert_eq!(p30.drop_schemas, ["t30_d1_g1_s1", "t30_sprig"]);
        let u30: Vec<&str> = p30.drop_accounts.iter().map(|a| a.user.as_str()).collect();
        assert_eq!(u30, ["t30_r2", "t30_u1"]);
        assert_eq!(p30.delete_names, ["t30_d1_g1_s1", "t30_u1"]);
    }

    #[test]
    fn another_tenants_catalog_mark_leaves_the_cluster_alone() {
        let t = TenantId(3);
        let mut found = crowded();
        found.schemas[0].1 = "sprig tenant 3 an-earlier-incarnation".into();
        let p = plan(
            t,
            &found,
            &Known {
                owner_mark: Some(mark(3)),
                drop_schemas: true,
                ..Known::default()
            },
        );
        assert!(p.foreign.is_some());
        assert!(p.is_empty() && p.statements().is_empty());
        // Without a known mark (a mover job's cleaning) a mark of tenant
        // 3 passes, and only accounts and names are planned.
        let p = plan(t, &found, &Known::default());
        assert_eq!(p.foreign, None);
        assert!(p.drop_schemas.is_empty());
        assert_eq!(p.drop_accounts.len(), 5);
        found.schemas[0].1 = mark(30);
        assert!(plan(t, &found, &Known::default()).foreign.is_some());
    }

    #[test]
    fn the_live_catalog_and_unmarked_copies_stay() {
        let t = TenantId(3);
        let mut found = crowded();
        let known = Known {
            owner_mark: Some(mark(3)),
            drop_schemas: true,
            keep_catalog: true,
            ..Known::default()
        };
        let p = plan(t, &found, &known);
        assert!(!p.drop_schemas.contains(&"t3_sprig".to_owned()));
        found.schemas[0].1 = String::new();
        let p = plan(
            t,
            &found,
            &Known {
                keep_catalog: false,
                ..known
            },
        );
        assert!(!p.drop_schemas.contains(&"t3_sprig".to_owned()));
        assert!(p.left.iter().any(|(s, _)| s == "t3_sprig"));
    }

    #[test]
    fn job_rows_go_by_exact_id() {
        let id: JobId = "0123456789abcdef0011223344556677".parse().unwrap();
        let p = plan(
            TenantId(3),
            &Found::default(),
            &Known {
                jobs: vec![id],
                ..Known::default()
            },
        );
        let s = p.statements();
        assert_eq!(s.len(), 3);
        assert!(
            s[0].ends_with("job_id = X'0123456789abcdef0011223344556677'"),
            "{}",
            s[0]
        );
        assert!(s.iter().any(|x| x.contains("`__sprig_mover_xa`")));
    }
}
