//! Read-only standby copies (chapter 25, "Standbys"; decision RF10,
//! r2last).
//!
//! A `Continuous` job (a job standby) copies the tenant's backend accounts
//! and grants with its schemas, so a promoted copy works at once. Until the
//! promotion nothing may write to the copy except the job itself: a direct
//! write would make it drift from the home. So right after `prepare` has
//! created the accounts and grants, the target revokes every privilege that
//! changes data or objects from the tenant's accounts on the copy's
//! schemas, and records exactly what it revoked in
//! `__sprig_sys.__sprig_standby_grants` on the copy's cluster. Reads keep
//! working. The job itself writes as `sprig_ctl` and isn't affected.
//!
//! The promotion (`sprig-controller::failover::nodes`) grants the recorded
//! privileges back ([`restore`]) before the copy takes writes; a dropped
//! standby forgets its rows ([`forget`]).
//!
//! The record lives with the copy, not in etcd: the promotion runs in the
//! copy's DC, which is the one that answers when the home DC is lost.
//!
//! **Triggers and events (RL12, r2harden).** A copy carries the tenant's
//! views and routines from its start (the stream applies their later
//! changes) and its events disabled, but no triggers: they would fire on
//! the rows the job applies. A move creates triggers and enables events at
//! its cutover; a standby never cuts over. So while it streams, its job
//! records the statements the cutover would run (the source's triggers
//! and events, as the source has them) in
//! `__sprig_sys.__sprig_standby_objects` on the copy, at its start, every
//! 30 s and when it is stopped ([`record_objects`]); the promotion runs
//! them after the job stopped, with the grants ([`restore`]).

use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use sprig_cluster::sql::literal;
use sprig_common::ids::TenantId;
use sprig_common::names::quote;
use sprig_common::{Result, SprigError};

/// The record of revoked privileges, one row per grant.
pub const GRANTS_TABLE: &str = "`__sprig_sys`.`__sprig_standby_grants`";

/// The statements a promotion runs on the copy (RL12), in order.
pub const OBJECTS_TABLE: &str = "`__sprig_sys`.`__sprig_standby_objects`";

/// Creates [`OBJECTS_TABLE`].
pub const OBJECTS_DDL: &str = "CREATE TABLE IF NOT EXISTS `__sprig_sys`.`__sprig_standby_objects` (\
    tenant_id BIGINT UNSIGNED NOT NULL, \
    seq INT UNSIGNED NOT NULL, \
    stmt LONGTEXT NOT NULL, \
    PRIMARY KEY (tenant_id, seq)\
    ) ENGINE=InnoDB";

/// Creates [`GRANTS_TABLE`].
pub const GRANTS_DDL: &str = "CREATE TABLE IF NOT EXISTS `__sprig_sys`.`__sprig_standby_grants` (\
    tenant_id BIGINT UNSIGNED NOT NULL, \
    grantee_user VARCHAR(128) NOT NULL, \
    grantee_host VARCHAR(255) NOT NULL, \
    db VARCHAR(255) NOT NULL, \
    tbl VARCHAR(64) NOT NULL DEFAULT '', \
    col VARCHAR(64) NOT NULL DEFAULT '', \
    privileges VARCHAR(512) NOT NULL, \
    PRIMARY KEY (tenant_id, grantee_user, grantee_host, db, tbl, col)\
    ) ENGINE=InnoDB";

/// Accounts never touched: SprigDB's own and MariaDB's.
pub const SYSTEM_USERS: [&str; 6] = [
    "system user",
    "event_scheduler",
    "sprig_ctl",
    "sprig_repl",
    "root",
    "mariadb.sys",
];

/// `mysql.db` columns of privileges that change data or objects, with
/// their SQL names.
pub const DB_WRITE_PRIVS: [(&str, &str); 15] = [
    ("Insert_priv", "INSERT"),
    ("Update_priv", "UPDATE"),
    ("Delete_priv", "DELETE"),
    ("Create_priv", "CREATE"),
    ("Drop_priv", "DROP"),
    ("Index_priv", "INDEX"),
    ("Alter_priv", "ALTER"),
    ("Create_tmp_table_priv", "CREATE TEMPORARY TABLES"),
    ("Lock_tables_priv", "LOCK TABLES"),
    ("Create_view_priv", "CREATE VIEW"),
    ("Create_routine_priv", "CREATE ROUTINE"),
    ("Alter_routine_priv", "ALTER ROUTINE"),
    ("Event_priv", "EVENT"),
    ("Trigger_priv", "TRIGGER"),
    ("Delete_history_priv", "DELETE HISTORY"),
];

/// A grant row as read: user, host, db, table, column, privileges.
type GrantRow = (String, String, String, String, String, String);

/// One grant the fence revoked, exactly as it was held.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldGrant {
    /// The account or role name.
    pub user: String,
    /// The host; empty for a role.
    pub host: String,
    /// The database as `mysql.db` stores it (a pattern, `\_` escaped).
    pub db: String,
    /// The table, for a table or column grant; empty otherwise.
    pub tbl: String,
    /// The column, for a column grant; empty otherwise.
    pub col: String,
    /// The privileges, in SQL spelling.
    pub privileges: Vec<String>,
}

impl HeldGrant {
    fn grantee(&self) -> String {
        if self.host.is_empty() {
            literal(&self.user)
        } else {
            format!("{}@{}", literal(&self.user), literal(&self.host))
        }
    }

    fn privs(&self) -> String {
        if self.col.is_empty() {
            self.privileges.join(", ")
        } else {
            self.privileges
                .iter()
                .map(|p| format!("{p} ({})", quote(&self.col)))
                .collect::<Vec<_>>()
                .join(", ")
        }
    }

    fn on(&self) -> String {
        if self.tbl.is_empty() {
            format!("{}.*", quote(&self.db))
        } else {
            format!("{}.{}", quote(&self.db), quote(&self.tbl))
        }
    }

    /// `REVOKE ... FROM grantee`.
    #[must_use]
    pub fn revoke_sql(&self) -> String {
        format!(
            "REVOKE {} ON {} FROM {}",
            self.privs(),
            self.on(),
            self.grantee()
        )
    }

    /// `GRANT ... TO grantee`: the undo of [`Self::revoke_sql`].
    #[must_use]
    pub fn grant_sql(&self) -> String {
        format!(
            "GRANT {} ON {} TO {}",
            self.privs(),
            self.on(),
            self.grantee()
        )
    }
}

/// Whether `db` (a `mysql.db` pattern, `\_` escaped) names `tenant`'s
/// schemas.
#[must_use]
pub fn tenant_db(db: &str, tenant: TenantId) -> bool {
    db.replace("\\_", "_")
        .strip_prefix(&format!("t{tenant}_"))
        .is_some()
}

/// The write privileges of a `tables_priv.Table_priv` or
/// `columns_priv.Column_priv` set (`Select,Insert,Delete versioning rows`),
/// in SQL spelling.
#[must_use]
pub fn write_privs_of_set(set: &str) -> Vec<String> {
    set.split(',')
        .map(str::trim)
        .filter_map(|p| match p.to_ascii_lowercase().as_str() {
            "insert" => Some("INSERT"),
            "update" => Some("UPDATE"),
            "delete" => Some("DELETE"),
            "create" => Some("CREATE"),
            "drop" => Some("DROP"),
            "index" => Some("INDEX"),
            "alter" => Some("ALTER"),
            "create view" => Some("CREATE VIEW"),
            "trigger" => Some("TRIGGER"),
            "delete versioning rows" => Some("DELETE HISTORY"),
            _ => None,
        })
        .map(str::to_owned)
        .collect()
}

fn err(what: &str, e: &mysql_async::Error) -> SprigError {
    SprigError::internal(format!("standby grants: {what}: {e}"))
}

/// The tenant's held write grants on the copy: database, table and column
/// level, for every account but [`SYSTEM_USERS`].
async fn held(c: &mut Conn, tenant: TenantId) -> Result<Vec<HeldGrant>> {
    let like = format!("t{tenant}%");
    let cols: Vec<&str> = DB_WRITE_PRIVS.iter().map(|(c, _)| *c).collect();
    let rows: Vec<mysql_async::Row> = c
        .exec(
            format!(
                "SELECT User, Host, Db, {} FROM mysql.db WHERE Db LIKE ?",
                cols.join(", ")
            ),
            (like.clone(),),
        )
        .await
        .map_err(|e| err("read mysql.db", &e))?;
    let mut out = Vec::new();
    for r in rows {
        let (Some(user), Some(host), Some(db)) = (
            r.get::<String, _>(0),
            r.get::<String, _>(1),
            r.get::<String, _>(2),
        ) else {
            continue;
        };
        if SYSTEM_USERS.contains(&user.as_str()) || !tenant_db(&db, tenant) {
            continue;
        }
        let privileges: Vec<String> = DB_WRITE_PRIVS
            .iter()
            .enumerate()
            .filter(|(i, _)| r.get::<String, _>(i + 3).as_deref() == Some("Y"))
            .map(|(_, (_, sql))| (*sql).to_owned())
            .collect();
        if !privileges.is_empty() {
            out.push(HeldGrant {
                user,
                host,
                db,
                tbl: String::new(),
                col: String::new(),
                privileges,
            });
        }
    }
    for (q, has_col) in [
        (
            "SELECT User, Host, Db, Table_name, '', Table_priv FROM mysql.tables_priv WHERE Db LIKE ?",
            false,
        ),
        (
            "SELECT User, Host, Db, Table_name, Column_name, Column_priv FROM mysql.columns_priv WHERE Db LIKE ?",
            true,
        ),
    ] {
        let rows: Vec<GrantRow> = c
            .exec(q, (like.clone(),))
            .await
            .map_err(|e| err("read table and column grants", &e))?;
        for (user, host, db, tbl, col, set) in rows {
            if SYSTEM_USERS.contains(&user.as_str()) || !tenant_db(&db, tenant) {
                continue;
            }
            let mut privileges = write_privs_of_set(&set);
            if has_col {
                privileges.retain(|p| p == "INSERT" || p == "UPDATE");
            }
            if !privileges.is_empty() {
                out.push(HeldGrant {
                    user,
                    host,
                    db,
                    tbl,
                    col: if has_col { col } else { String::new() },
                    privileges,
                });
            }
        }
    }
    Ok(out)
}

/// Makes the tenant's copy read-only for its accounts (see the module
/// docs): records and revokes every held write grant, then ends the
/// accounts' sessions. Idempotent: a grant revoked earlier and not granted
/// again keeps its first record. Returns (grants revoked, sessions ended).
///
/// # Errors
/// A statement failed.
pub async fn fence(c: &mut Conn, tenant: TenantId) -> Result<(usize, usize)> {
    c.query_drop("CREATE DATABASE IF NOT EXISTS `__sprig_sys`")
        .await
        .map_err(|e| err("create __sprig_sys", &e))?;
    c.query_drop(GRANTS_DDL)
        .await
        .map_err(|e| err("create the table", &e))?;
    let grants = held(c, tenant).await?;
    let mut users = std::collections::BTreeSet::new();
    for g in &grants {
        // The record first: a crash between the two leaves a grant that
        // is recorded and still held, which the next fence revokes.
        c.exec_drop(
            format!(
                "INSERT INTO {GRANTS_TABLE} (tenant_id, grantee_user, grantee_host, db, tbl, col, privileges) \
                 VALUES (?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE privileges = VALUES(privileges)"
            ),
            (
                tenant.0,
                g.user.as_str(),
                g.host.as_str(),
                g.db.as_str(),
                g.tbl.as_str(),
                g.col.as_str(),
                g.privileges.join(","),
            ),
        )
        .await
        .map_err(|e| err("record a grant", &e))?;
        c.query_drop(g.revoke_sql())
            .await
            .map_err(|e| err("revoke a grant", &e))?;
        users.insert(g.user.clone());
    }
    // Sessions opened before the revoke keep their privileges.
    let killed = kill_sessions(c, &users).await?;
    Ok((grants.len(), killed))
}

/// The recorded grants of `tenant`.
///
/// # Errors
/// A statement failed (a missing table is no grants).
pub async fn recorded(c: &mut Conn, tenant: TenantId) -> Result<Vec<HeldGrant>> {
    let rows: std::result::Result<Vec<GrantRow>, _> = c
        .exec(
            format!(
                "SELECT grantee_user, grantee_host, db, tbl, col, privileges FROM {GRANTS_TABLE} \
                 WHERE tenant_id = ? ORDER BY grantee_user, grantee_host, db, tbl, col"
            ),
            (tenant.0,),
        )
        .await;
    match rows {
        Ok(rows) => Ok(rows
            .into_iter()
            .map(|(user, host, db, tbl, col, p)| HeldGrant {
                user,
                host,
                db,
                tbl,
                col,
                privileges: p.split(',').map(str::to_owned).collect(),
            })
            .collect()),
        Err(e) if super::errno(&e) == Some(1146) => Ok(Vec::new()),
        Err(e) => Err(err("read the record", &e)),
    }
}

/// Grants back what [`fence`] revoked (a promotion), then forgets the
/// record. Returns the grants restored.
///
/// # Errors
/// A statement failed; nothing is forgotten then, so a retry restores
/// the rest.
pub async fn restore(c: &mut Conn, tenant: TenantId) -> Result<usize> {
    let grants = recorded(c, tenant).await?;
    for g in &grants {
        match c.query_drop(g.grant_sql()).await {
            Ok(()) => {}
            // The account or table is gone meanwhile: nothing to give back.
            Err(e) if matches!(super::errno(&e), Some(1133 | 1146 | 1147)) => {
                tracing::debug!(%tenant, grant = %g.grant_sql(), error = %e, "standby grant not restored");
            }
            Err(e) => return Err(err("restore a grant", &e)),
        }
    }
    // RL12: the tenant's triggers and events, now that nothing applies
    // rows to the copy any more.
    let (made, failed) = create_objects(c, tenant).await?;
    if made + failed > 0 {
        tracing::info!(%tenant, made, failed, "standby's triggers and events created for its promotion");
    }
    // A session opened while the grants were revoked keeps the revoked
    // database privileges until it changes database (MariaDB reads them
    // at `USE`): a gateway's pooled connection would go on getting 1142.
    // End them; their clients reconnect with the restored privileges.
    let users: std::collections::BTreeSet<String> = grants.iter().map(|g| g.user.clone()).collect();
    let killed = kill_sessions(c, &users).await?;
    if killed > 0 {
        tracing::info!(%tenant, killed, "sessions opened while the standby was read-only ended");
    }
    forget(c, tenant).await?;
    Ok(grants.len())
}

/// Ends every session of `users` but this one; returns how many.
async fn kill_sessions(c: &mut Conn, users: &std::collections::BTreeSet<String>) -> Result<usize> {
    if users.is_empty() {
        return Ok(0);
    }
    let me: u64 = c.id().into();
    let procs: Vec<(u64, Option<String>)> = c
        .query("SELECT ID, USER FROM information_schema.PROCESSLIST")
        .await
        .map_err(|e| err("list sessions", &e))?;
    let mut killed = 0;
    for (id, user) in procs {
        if id != me
            && user.is_some_and(|u| users.contains(&u))
            && c.query_drop(format!("KILL CONNECTION {id}")).await.is_ok()
        {
            killed += 1;
        }
    }
    Ok(killed)
}

/// Forgets the record of `tenant` (a dropped standby).
///
/// # Errors
/// A statement failed (a missing table is fine).
pub async fn forget(c: &mut Conn, tenant: TenantId) -> Result<()> {
    match c
        .exec_drop(
            format!("DELETE FROM {GRANTS_TABLE} WHERE tenant_id = ?"),
            (tenant.0,),
        )
        .await
    {
        Ok(()) => {}
        Err(e) if super::errno(&e) == Some(1146) => {}
        Err(e) => return Err(err("forget the record", &e)),
    }
    match c
        .exec_drop(
            format!("DELETE FROM {OBJECTS_TABLE} WHERE tenant_id = ?"),
            (tenant.0,),
        )
        .await
    {
        Ok(()) => Ok(()),
        Err(e) if super::errno(&e) == Some(1146) => Ok(()),
        Err(e) => Err(err("forget the recorded objects", &e)),
    }
}

/// The statements recorded for `tenant`'s promotion (RL12).
///
/// # Errors
/// A statement failed (a missing table is none).
pub async fn recorded_objects(c: &mut Conn, tenant: TenantId) -> Result<Vec<String>> {
    match c
        .exec(
            format!("SELECT stmt FROM {OBJECTS_TABLE} WHERE tenant_id = ? ORDER BY seq"),
            (tenant.0,),
        )
        .await
    {
        Ok(v) => Ok(v),
        Err(e) if super::errno(&e) == Some(1146) => Ok(Vec::new()),
        Err(e) => Err(err("read the recorded objects", &e)),
    }
}

/// Records `stmts` as `tenant`'s triggers and events for its promotion,
/// replacing an older record in one transaction; `false` when the record
/// was the same already.
///
/// # Errors
/// A statement failed.
pub async fn record_objects(c: &mut Conn, tenant: TenantId, stmts: &[String]) -> Result<bool> {
    if recorded_objects(c, tenant).await? == stmts {
        return Ok(false);
    }
    c.query_drop("CREATE DATABASE IF NOT EXISTS `__sprig_sys`")
        .await
        .map_err(|e| err("create __sprig_sys", &e))?;
    c.query_drop(OBJECTS_DDL)
        .await
        .map_err(|e| err("create the objects table", &e))?;
    let mut tx = c
        .start_transaction(mysql_async::TxOpts::default())
        .await
        .map_err(|e| err("begin", &e))?;
    tx.exec_drop(
        format!("DELETE FROM {OBJECTS_TABLE} WHERE tenant_id = ?"),
        (tenant.0,),
    )
    .await
    .map_err(|e| err("replace the recorded objects", &e))?;
    for (i, q) in stmts.iter().enumerate() {
        tx.exec_drop(
            format!("INSERT INTO {OBJECTS_TABLE} (tenant_id, seq, stmt) VALUES (?, ?, ?)"),
            (tenant.0, u32::try_from(i).unwrap_or(u32::MAX), q.as_str()),
        )
        .await
        .map_err(|e| err("record an object", &e))?;
    }
    tx.commit().await.map_err(|e| err("commit", &e))?;
    Ok(true)
}

/// Runs the statements recorded for `tenant` (its triggers, its events as
/// the source had them) on the promoted copy. A trigger that exists
/// already counts as made; a statement that fails otherwise is logged and
/// skipped, so one broken definition never blocks a failover. Returns
/// (statements run, statements failed); the session's `sql_mode` and
/// character set are reset after.
///
/// # Errors
/// The record can't be read.
pub async fn create_objects(c: &mut Conn, tenant: TenantId) -> Result<(usize, usize)> {
    let stmts = recorded_objects(c, tenant).await?;
    let (mut made, mut failed) = (0, 0);
    for q in &stmts {
        match c.query_drop(q.as_str()).await {
            Ok(()) => made += 1,
            Err(e) if super::errno(&e) == Some(1359) => made += 1,
            Err(e) => {
                failed += 1;
                tracing::warn!(%tenant, error = %e, statement = %q.chars().take(120).collect::<String>(), "a recorded trigger or event wasn't created on the promoted copy");
            }
        }
    }
    if !stmts.is_empty() {
        let _ = c.query_drop("SET SESSION sql_mode = DEFAULT").await;
        let _ = c.query_drop("SET NAMES utf8mb4").await;
    }
    Ok((made, failed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grants_round_trip_as_sql() {
        let g = HeldGrant {
            user: "t42_u1".into(),
            host: "%".into(),
            db: "t42\\_%".into(),
            tbl: String::new(),
            col: String::new(),
            privileges: vec!["INSERT".into(), "DELETE HISTORY".into()],
        };
        assert_eq!(
            g.revoke_sql(),
            "REVOKE INSERT, DELETE HISTORY ON `t42\\_%`.* FROM 't42_u1'@'%'"
        );
        assert_eq!(
            g.grant_sql(),
            "GRANT INSERT, DELETE HISTORY ON `t42\\_%`.* TO 't42_u1'@'%'"
        );
        let c = HeldGrant {
            tbl: "orders".into(),
            col: "note".into(),
            host: String::new(),
            privileges: vec!["UPDATE".into()],
            ..g
        };
        assert_eq!(
            c.revoke_sql(),
            "REVOKE UPDATE (`note`) ON `t42\\_%`.`orders` FROM 't42_u1'"
        );
    }

    #[test]
    fn only_write_privileges_and_the_tenants_schemas() {
        assert_eq!(
            write_privs_of_set("Select,Insert,Show view,Delete versioning rows,Create View"),
            vec!["INSERT", "DELETE HISTORY", "CREATE VIEW"]
        );
        assert!(write_privs_of_set("Select,References").is_empty());
        let t = TenantId(42);
        assert!(tenant_db("t42\\_d%", t));
        assert!(tenant_db("t42_d1", t));
        assert!(!tenant_db("t420\\_d%", t));
        assert!(!tenant_db("t4\\_d%", t));
    }
}
