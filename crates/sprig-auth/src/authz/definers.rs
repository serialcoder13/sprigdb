//! Definers of stored code (chapter 29, "Definers of stored code").
//!
//! MariaDB runs a `SQL SECURITY DEFINER` routine, every trigger, every
//! event and a `SQL SECURITY DEFINER` view with its definer's rights. R0
//! creates stored programs as the cluster's `sprig_ctl` account (binary
//! logging refuses them from accounts without `SUPER`, error 1419), so
//! before M0.5's fix their definer was `sprig_ctl`, which holds `ALL` on
//! every tenant's schemas: dynamic SQL in a procedure or event body (which
//! the gateway never sees) could read any tenant. Now every stored program
//! names the creating session's backend account as `DEFINER` (the gateway
//! adds it; `sprig_ctl` needs MariaDB's `SET USER` privilege for that),
//! and this module moves code created before the fix:
//!
//! ```text
//! migrate(tenant T, cluster C):                     # as sprig_ctl, once per
//!   found := routines, triggers, events and views   # gateway start
//!            in t<T>_d% schemas whose definer is not one of T's accounts
//!   for each: definer := the account of the database's creator if live,
//!             else the lowest-id live user holding SPRIG_TENANT_ADMIN,
//!             else t<T>_u0 (no such account: the code fails, closed)
//!     SHOW CREATE, then CREATE OR REPLACE with DEFINER = that account, in
//!     the object's sql_mode, character set and time zone
//! ```

use std::collections::BTreeMap;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row};
use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::shard_schemas;
use sprig_catalog::store::DbState;
use sprig_common::ids::{TenantId, UserId};
use sprig_common::names::AccountName;
use sprig_common::{Result, SprigError};

use super::privilege::Action;
use super::state::AuthzState;
use super::store::{Grantee, PrivTarget};
use crate::cache::TenantAuth;

/// What kind of stored code an object is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum StoredKind {
    /// `PROCEDURE`.
    Procedure,
    /// `FUNCTION`.
    Function,
    /// `TRIGGER`.
    Trigger,
    /// `EVENT`.
    Event,
    /// `VIEW`.
    View,
}

impl StoredKind {
    /// The SQL keyword.
    #[must_use]
    pub const fn keyword(self) -> &'static str {
        match self {
            Self::Procedure => "PROCEDURE",
            Self::Function => "FUNCTION",
            Self::Trigger => "TRIGGER",
            Self::Event => "EVENT",
            Self::View => "VIEW",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "PROCEDURE" => Self::Procedure,
            "FUNCTION" => Self::Function,
            "TRIGGER" => Self::Trigger,
            "EVENT" => Self::Event,
            "VIEW" => Self::View,
            _ => return None,
        })
    }
}

/// A stored program or view found on a cluster.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StoredObject {
    /// Its kind.
    pub kind: StoredKind,
    /// Its schema.
    pub schema: String,
    /// Its name.
    pub name: String,
    /// Its definer, as `information_schema` writes it (`user@host`).
    pub definer: String,
}

/// Whether `definer` (`user@host`) is one of `tenant`'s backend accounts.
#[must_use]
pub fn is_tenant_account(tenant: TenantId, definer: &str) -> bool {
    let user = definer.rsplit_once('@').map_or(definer, |(u, _)| u);
    user.parse::<AccountName>()
        .is_ok_and(|a| a.tenant() == tenant && matches!(a, AccountName::User { .. }))
}

/// The account stored code of each of the tenant's schemas moves to (see
/// the module docs): schema name to account user name (host `%`).
#[must_use]
pub fn wanted(
    cat: &TenantCatalog,
    state: &AuthzState,
    auth: &TenantAuth,
) -> BTreeMap<String, String> {
    let tenant = cat.tenant;
    let live = |u: UserId| auth.user(u).is_some();
    let admin = auth
        .users()
        .map(|u| u.row.user_id)
        .filter(|u| {
            !state.is_migrated()
                || state.privileges_of(Grantee::User(*u)).any(|p| {
                    matches!(p.target, PrivTarget::Tenant) && p.actions.has(Action::TenantAdmin)
                })
        })
        .min();
    let fallback = admin.unwrap_or(UserId(0));
    let mut out = BTreeMap::new();
    for d in cat.databases.iter().filter(|d| d.state == DbState::Active) {
        let user = d.created_by.filter(|u| live(*u)).unwrap_or(fallback);
        let acct = AccountName::User { tenant, user }.to_string();
        for s in shard_schemas(tenant, d) {
            out.insert(s.to_string(), acct.clone());
        }
    }
    out
}

/// The tenant's stored programs and views on the cluster behind `conn`
/// whose definer isn't one of its accounts.
///
/// # Errors
/// A backend error.
pub async fn foreign(conn: &mut Conn, tenant: TenantId) -> Result<Vec<StoredObject>> {
    let like = format!("t{tenant}\\_d%");
    let rows: Vec<(String, String, String, String)> = conn
        .exec(
            "SELECT ROUTINE_TYPE, ROUTINE_SCHEMA, ROUTINE_NAME, DEFINER \
               FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA LIKE ? \
             UNION ALL SELECT 'TRIGGER', TRIGGER_SCHEMA, TRIGGER_NAME, DEFINER \
               FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA LIKE ? \
             UNION ALL SELECT 'EVENT', EVENT_SCHEMA, EVENT_NAME, DEFINER \
               FROM information_schema.EVENTS WHERE EVENT_SCHEMA LIKE ? \
             UNION ALL SELECT 'VIEW', TABLE_SCHEMA, TABLE_NAME, DEFINER \
               FROM information_schema.VIEWS WHERE TABLE_SCHEMA LIKE ?",
            (&like, &like, &like, &like),
        )
        .await
        .map_err(crate::store::map_err)?;
    let mut out: Vec<StoredObject> = rows
        .into_iter()
        .filter(|(_, _, _, d)| !is_tenant_account(tenant, d))
        .filter_map(|(k, schema, name, definer)| {
            Some(StoredObject {
                kind: StoredKind::parse(&k)?,
                schema,
                name,
                definer,
            })
        })
        .collect();
    out.sort();
    Ok(out)
}

/// `create` (a `SHOW CREATE` text MariaDB wrote: `CREATE [ALGORITHM=...]
/// DEFINER=`u`@`h` ...`) as `CREATE OR REPLACE ... DEFINER=`account`@`%`
/// ...`; `None` when it has no such definer clause.
#[must_use]
pub fn redefine(create: &str, account: &str) -> Option<String> {
    let head = create.get(..7)?;
    if !head.eq_ignore_ascii_case("CREATE ") {
        return None;
    }
    let rest = &create[7..];
    let at = rest.find("DEFINER=`")?;
    // Only the clause MariaDB writes before a view's definer.
    let before = &rest[..at];
    let algorithm = before
        .strip_prefix("ALGORITHM=")
        .and_then(|a| a.strip_suffix(' '))
        .is_some_and(|a| !a.is_empty() && a.bytes().all(|c| c.is_ascii_alphabetic()));
    if !before.is_empty() && !algorithm {
        return None;
    }
    let after = &rest[at + "DEFINER=".len()..];
    let user_end = quoted_end(after)?;
    let host = after[user_end..].strip_prefix('@')?;
    let host_end = quoted_end(host)?;
    let tail = &host[host_end..];
    Some(format!(
        "CREATE OR REPLACE {}DEFINER=`{}`@`%`{tail}",
        &rest[..at],
        account.replace('`', "``")
    ))
}

/// One past the closing backtick of the identifier `s` starts with.
fn quoted_end(s: &str) -> Option<usize> {
    let b = s.as_bytes();
    if b.first() != Some(&b'`') {
        return None;
    }
    let mut i = 1;
    while i < b.len() {
        if b[i] == b'`' {
            if b.get(i + 1) == Some(&b'`') {
                i += 2;
                continue;
            }
            return Some(i + 1);
        }
        i += 1;
    }
    None
}

fn quote(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// What [`migrate`] did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DefinerReport {
    /// Objects given a tenant account as definer.
    pub moved: Vec<StoredObject>,
    /// Objects left as they were, with the reason.
    pub failed: Vec<(StoredObject, String)>,
}

fn text(row: &Row, name: &str) -> Option<String> {
    let i = row
        .columns_ref()
        .iter()
        .position(|c| c.name_str() == name)?;
    match row.as_ref(i)? {
        mysql_async::Value::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    }
}

/// Moves the tenant's stored code on the cluster behind `conn` (a
/// `sprig_ctl` connection to its writer) to tenant accounts: `wanted`
/// maps each schema to its account ([`wanted`]). The connection's session
/// state is reset afterwards.
///
/// # Errors
/// A backend error listing the objects; single objects that fail are
/// reported in [`DefinerReport::failed`].
pub async fn migrate(
    conn: &mut Conn,
    tenant: TenantId,
    wanted: &BTreeMap<String, String>,
) -> Result<DefinerReport> {
    let found = foreign(conn, tenant).await?;
    let mut rep = DefinerReport::default();
    if found.is_empty() {
        return Ok(rep);
    }
    let fallback = AccountName::User {
        tenant,
        user: UserId(0),
    }
    .to_string();
    for o in found {
        let account = wanted.get(&o.schema).unwrap_or(&fallback);
        match migrate_one(conn, &o, account).await {
            Ok(()) => rep.moved.push(o),
            Err(e) => rep.failed.push((o, e)),
        }
    }
    if let Err(e) = conn.reset().await {
        return Err(crate::store::map_err(e));
    }
    Ok(rep)
}

async fn migrate_one(
    conn: &mut Conn,
    o: &StoredObject,
    account: &str,
) -> std::result::Result<(), String> {
    let show = format!(
        "SHOW CREATE {} {}.{}",
        o.kind.keyword(),
        quote(&o.schema),
        quote(&o.name)
    );
    let row: Option<Row> = conn.query_first(show).await.map_err(|e| e.to_string())?;
    let row = row.ok_or_else(|| "gone".to_owned())?;
    let create_col = match o.kind {
        StoredKind::Procedure => "Create Procedure",
        StoredKind::Function => "Create Function",
        StoredKind::Trigger => "SQL Original Statement",
        StoredKind::Event => "Create Event",
        StoredKind::View => "Create View",
    };
    let create =
        text(&row, create_col).ok_or_else(|| "no definition (no privilege?)".to_owned())?;
    let sql = redefine(&create, account).ok_or_else(|| "no DEFINER clause".to_owned())?;
    let mut set = Vec::new();
    if let Some(m) = text(&row, "sql_mode") {
        set.push(format!("sql_mode = '{}'", m.replace('\'', "''")));
    }
    if let Some(z) = text(&row, "time_zone") {
        set.push(format!("time_zone = '{}'", z.replace('\'', "''")));
    }
    if let Some(c) = text(&row, "character_set_client") {
        set.push(format!(
            "character_set_client = '{}'",
            c.replace('\'', "''")
        ));
    }
    if let Some(c) = text(&row, "collation_connection") {
        set.push(format!(
            "collation_connection = '{}'",
            c.replace('\'', "''")
        ));
    }
    if !set.is_empty() {
        conn.query_drop(format!("SET SESSION {}", set.join(", ")))
            .await
            .map_err(|e| e.to_string())?;
    }
    conn.query_drop(format!("USE {}", quote(&o.schema)))
        .await
        .map_err(|e| e.to_string())?;
    conn.query_drop(sql).await.map_err(|e| e.to_string())
}

/// The definer error a stored program's creation is refused with when
/// MariaDB recorded another definer than the session's account.
#[must_use]
pub fn mismatch(kind: &str, name: &str, got: &str) -> SprigError {
    SprigError::new(
        sprig_common::SprigCode::DefinerRefused,
        format!(
            "{kind} {name} was not created with the session's account as its definer \
             (MariaDB recorded {got}); it was dropped"
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redefines_what_mariadb_writes() {
        assert_eq!(
            redefine(
                "CREATE DEFINER=`root`@`%` PROCEDURE `p`() select 1",
                "t1_u1"
            )
            .as_deref(),
            Some("CREATE OR REPLACE DEFINER=`t1_u1`@`%` PROCEDURE `p`() select 1")
        );
        assert_eq!(
            redefine(
                "CREATE ALGORITHM=UNDEFINED DEFINER=`sprig_ctl`@`localhost` SQL SECURITY DEFINER VIEW `v` AS select 1",
                "t1_u2"
            )
            .as_deref(),
            Some(
                "CREATE OR REPLACE ALGORITHM=UNDEFINED DEFINER=`t1_u2`@`%` SQL SECURITY DEFINER VIEW `v` AS select 1"
            )
        );
        assert_eq!(
            redefine(
                "CREATE DEFINER=`a``b`@`h` TRIGGER t AFTER INSERT ON x FOR EACH ROW SET @a=1",
                "t1_u1"
            )
            .as_deref(),
            Some(
                "CREATE OR REPLACE DEFINER=`t1_u1`@`%` TRIGGER t AFTER INSERT ON x FOR EACH ROW SET @a=1"
            )
        );
        assert_eq!(
            redefine("CREATE PROCEDURE p() select 'DEFINER=`x`@`y`'", "a"),
            None
        );
        assert_eq!(redefine("SELECT 1", "a"), None);
    }

    #[test]
    fn tenant_accounts() {
        assert!(is_tenant_account(TenantId(1), "t1_u7@%"));
        assert!(!is_tenant_account(TenantId(1), "t12_u7@%"));
        assert!(!is_tenant_account(TenantId(1), "root@%"));
        assert!(!is_tenant_account(TenantId(1), "sprig_ctl@localhost"));
        assert!(!is_tenant_account(TenantId(1), "t1_r3@%"));
    }
}
