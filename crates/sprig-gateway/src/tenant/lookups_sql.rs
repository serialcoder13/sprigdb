//! Global lookup indexes in SQL (chapter 19): the gateway's side of
//! `sprig_shard::lookup::admin`.
//!
//! - **`SPRIG SHOW LOOKUP INDEXES [FROM t]`** (also `SHOW SPRIG LOOKUP
//!   INDEXES`) lists the tenant's lookup indexes the session can see, read
//!   fresh from the catalog: table, key name, column, unique, origin, state,
//!   conflicts, id and last change. `information_schema.SPRIG_LOOKUP_INDEXES`
//!   is the same rows as a view.
//! - **`SPRIG REBUILD LOOKUP INDEX k ON t`** (needs `ALTER` on `t`) rebuilds
//!   `t`'s lookup `k`: routing stops, every row is checked and repaired,
//!   orphans go, routing resumes. When `k` is a declared single-column
//!   unique key without a lookup (a table the advisor sharded), it builds
//!   one, which makes the key unique across shards (1062 if it isn't).
//! - **`SPRIG DROP LOOKUP INDEX k ON t`** (needs `ALTER`) drops only the
//!   lookup; the key stays unique within each shard.
//!
//! `ALTER TABLE ... ADD UNIQUE`, `CREATE UNIQUE INDEX` and `DROP INDEX` on
//! sharded tables go through the DDL path (`exec::router::lookup_ddl`),
//! which uses [`GwHost`] and [`build_for_key`] from here.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::{DbState, Namespace, ObjectKind, ObjectState};
use sprig_catalog::{CatalogLocator, Target, TenantCatalog, VersionKind};
use sprig_common::ids::{ClusterId, DbId, GroupId, ObjectId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Tier, TierView};
use sprig_proto_mysql::ColumnDef;
use sprig_shard::ShardMapStore;
use sprig_shard::lookup::admin::{Admin, LookupHost, NewLookup, lookup_keys, owner_shape};
use sprig_shard::lookup::direct::{BuildReport, Pools};
use sprig_shard::lookup::{LookupCatalog, LookupOrigin, LookupRecord};
use sprig_sql::names::LocalAnswer;

use super::TenantServices;
use super::session::TenantSession;
use crate::pipeline::text_column;

/// How long a publish waits for every gateway to load the new catalog
/// version (each checks the directory's version at most once a second).
pub const PUBLISH_SETTLE: Duration = Duration::from_millis(2500);

/// Rows a back-fill chunk reads (`mover.copy_chunk_rows`' default).
pub const CHUNK_ROWS: usize = 1000;

fn catalog_err(e: mysql_async::Error) -> SprigError {
    SprigError::new(
        SprigCode::CatalogUnavailable,
        format!("lookup indexes: the catalog's cluster: {e}"),
    )
    .with_source(e)
}

/// The tenant's home DC's shard maps.
#[must_use]
pub fn maps(svc: &TenantServices, tenant: TenantId) -> ShardMapStore {
    let home = svc.locator.home_of(tenant);
    let dc = svc.locator.dc_store();
    if home == svc.dc {
        ShardMapStore::new(dc)
    } else {
        ShardMapStore::new(TierView::over(&dc, Tier::Dc(home)))
    }
}

/// [`LookupHost`] for a gateway: the tenant's catalog through its writer
/// endpoint, clusters through the `sprig_ctl` pools, and a publish that is
/// an empty versioned catalog change followed by [`PUBLISH_SETTLE`].
pub struct GwHost {
    /// The services.
    pub svc: Arc<TenantServices>,
    /// The tenant.
    pub tenant: TenantId,
}

impl std::fmt::Debug for GwHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GwHost")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl LookupHost for GwHost {
    async fn catalog(&self) -> Result<Conn> {
        let ep = self.svc.locator.writer(self.tenant).await?;
        let schema = ep.store.schema().to_owned();
        let mut c = ep.pool.get_conn().await.map_err(catalog_err)?;
        sprig_shard::groups::migrate(&mut c, &schema).await?;
        c.query_drop(format!("USE {}", sprig_common::names::quote(&schema)))
            .await
            .map_err(catalog_err)?;
        Ok(c)
    }

    async fn pools(&self, clusters: &[ClusterId]) -> Result<Pools> {
        let mut m = HashMap::new();
        for c in clusters {
            m.insert(c.clone(), self.svc.locator.ctl_pool(c).await?);
        }
        Ok(Arc::new(m))
    }

    async fn publish(&self) -> Result<()> {
        let v = self
            .svc
            .catalog
            .change(self.tenant, VersionKind::Catalog)
            .await?
            .commit()
            .await?;
        tracing::debug!(tenant = %self.tenant, version = v, "lookup index change published");
        tokio::time::sleep(PUBLISH_SETTLE).await;
        Ok(())
    }
}

/// A table's group membership as a lookup needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Membership {
    /// Its group.
    pub group: GroupId,
    /// Its database.
    pub db: DbId,
    /// `columns`, `parent`, ...
    pub routing: String,
    /// The key columns of a `columns` member, lowercase.
    pub key: Vec<String>,
    /// `group_members.rk_state`.
    pub rk_state: String,
}

/// `object`'s `group_members` row in a sharded group, if any.
///
/// # Errors
/// The catalog's.
pub async fn membership(c: &mut Conn, object: ObjectId) -> Result<Option<Membership>> {
    let row: Option<(u32, u32, String, Option<String>, String)> = c
        .exec_first(
            "SELECT m.group_id, m.db_id, CAST(m.routing AS CHAR), CAST(m.key_columns AS CHAR), \
             CAST(m.rk_state AS CHAR) FROM group_members m \
             JOIN table_groups g ON g.group_id = m.group_id \
             WHERE m.object_id = ? AND g.kind = 'sharded'",
            (object.0,),
        )
        .await
        .map_err(catalog_err)?;
    Ok(row.map(|(g, db, routing, key, rk)| Membership {
        group: GroupId(g),
        db: DbId(db),
        routing,
        key: key
            .and_then(|k| serde_json::from_str::<Vec<String>>(&k).ok())
            .unwrap_or_default()
            .into_iter()
            .map(|c| c.to_ascii_lowercase())
            .collect(),
        rk_state: rk,
    }))
}

fn refused(why: &str) -> SprigError {
    SprigError::new(
        SprigCode::StatementNotDistributable,
        format!("statement not distributable (lookup): {why}"),
    )
}

/// Builds the unique lookup of `table`'s single-column unique key `key`
/// on `column`: what `ADD UNIQUE` and `SPRIG REBUILD LOOKUP INDEX` run.
///
/// # Errors
/// 39420 for a member that follows its parent or hasn't filled
/// `_sprig_rk`; 1062 when the values repeat across shards; the lookup
/// jobs' errors.
pub async fn build_for_key(
    svc: &Arc<TenantServices>,
    tenant: TenantId,
    object: ObjectId,
    table: &str,
    key: &str,
    column: &str,
) -> Result<BuildReport> {
    let host = GwHost {
        svc: Arc::clone(svc),
        tenant,
    };
    let mut cat = host.catalog().await?;
    let m = membership(&mut cat, object)
        .await?
        .ok_or_else(|| refused(&format!("{table} isn't sharded")))?;
    if m.routing != "columns" {
        return Err(refused(&format!(
            "{table} follows its parent's rows; a lookup index can't name them"
        )));
    }
    if m.rk_state != "ready" {
        return Err(refused(&format!(
            "{table}'s routing hashes are still being filled ({}); try again when its group is ready",
            m.rk_state
        )));
    }
    drop(cat);
    let store = maps(svc, tenant);
    let (owner_map, _) = store.load(tenant, m.group).await?;
    let admin = Admin {
        tenant,
        maps: &store,
        host: &host,
        chunk: CHUNK_ROWS,
    };
    let (l, report) = admin
        .create(&NewLookup {
            db: m.db,
            owner: object,
            owner_table: table.to_owned(),
            owner_map: Arc::new(owner_map),
            columns: vec![column.to_ascii_lowercase()],
            key: Some(key.to_owned()),
            origin: LookupOrigin::UniqueKey,
        })
        .await?;
    tracing::info!(
        tenant = %tenant, table, lookup = %l.index_name, rows = report.rows,
        inserted = report.inserted, repaired = report.repaired, "lookup index built"
    );
    Ok(report)
}

/// Drops `table`'s lookup `name`; `Ok(false)` when it has none.
///
/// # Errors
/// The lookup jobs' errors.
pub async fn drop_for_key(
    svc: &Arc<TenantServices>,
    tenant: TenantId,
    object: ObjectId,
    name: &str,
) -> Result<bool> {
    let host = GwHost {
        svc: Arc::clone(svc),
        tenant,
    };
    let store = maps(svc, tenant);
    Admin {
        tenant,
        maps: &store,
        host: &host,
        chunk: CHUNK_ROWS,
    }
    .drop(object, name)
    .await
}

/// The tenant's lookup rows, fresh from the catalog (none before the
/// `sharding` module or the table exist).
///
/// # Errors
/// The catalog's.
pub async fn records(svc: &Arc<TenantServices>, tenant: TenantId) -> Result<Vec<LookupRecord>> {
    let ep = svc.locator.writer(tenant).await?;
    let schema = ep.store.schema().to_owned();
    let mut c = ep.pool.get_conn().await.map_err(catalog_err)?;
    let has: Option<u64> = c
        .exec_first(
            "SELECT COUNT(*) FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'lookup_indexes'",
            (schema.as_str(),),
        )
        .await
        .map_err(catalog_err)?;
    if has.unwrap_or(0) == 0 {
        return Ok(Vec::new());
    }
    c.query_drop(format!("USE {}", sprig_common::names::quote(&schema)))
        .await
        .map_err(catalog_err)?;
    LookupCatalog.rows(&mut c).await
}

/// A parsed lookup statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupStmt {
    /// `SPRIG SHOW LOOKUP INDEXES [FROM [db.]t]`.
    Show {
        /// The table's database, if named.
        db: Option<String>,
        /// The table, if named.
        table: Option<String>,
    },
    /// `SPRIG REBUILD LOOKUP INDEX k ON [db.]t`.
    Rebuild {
        /// The key.
        name: String,
        /// The table's database, if named.
        db: Option<String>,
        /// The table.
        table: String,
    },
    /// `SPRIG DROP LOOKUP INDEX k ON [db.]t`.
    Drop {
        /// The key.
        name: String,
        /// The table's database, if named.
        db: Option<String>,
        /// The table.
        table: String,
    },
}

/// Words and quoted names of `text`, with `.` as its own token; `None` for
/// anything else (strings, comments, other punctuation).
fn words(text: &str) -> Option<Vec<(String, bool)>> {
    let b = text.trim().trim_end_matches(';').trim();
    let mut out = Vec::new();
    let mut it = b.chars().peekable();
    while let Some(&ch) = it.peek() {
        if ch.is_whitespace() {
            it.next();
        } else if ch == '.' {
            it.next();
            out.push((".".to_owned(), false));
        } else if ch == '`' {
            it.next();
            let mut s = String::new();
            loop {
                match it.next()? {
                    '`' if it.peek() == Some(&'`') => {
                        it.next();
                        s.push('`');
                    }
                    '`' => break,
                    c => s.push(c),
                }
            }
            out.push((s, true));
        } else if ch.is_alphanumeric() || ch == '_' || ch == '$' {
            let mut s = String::new();
            while let Some(&c) = it.peek() {
                if c.is_alphanumeric() || c == '_' || c == '$' {
                    s.push(c);
                    it.next();
                } else {
                    break;
                }
            }
            out.push((s, false));
        } else {
            return None;
        }
    }
    Some(out)
}

/// [`parse`] for a statement's bytes, after a cheap look at its first
/// word (every statement passes here).
#[must_use]
pub fn parse_bytes(text: &[u8]) -> Option<LookupStmt> {
    let t = text.trim_ascii_start();
    let first = t.get(..5)?;
    if !(first.eq_ignore_ascii_case(b"SPRIG") || first[..4].eq_ignore_ascii_case(b"SHOW")) {
        return None;
    }
    if !t.windows(6).any(|w| w.eq_ignore_ascii_case(b"LOOKUP")) {
        return None;
    }
    parse(std::str::from_utf8(t).ok()?)
}

/// `text` as a lookup statement; `None` for any other statement.
#[must_use]
pub fn parse(text: &str) -> Option<LookupStmt> {
    let w = words(text)?;
    let kw = |i: usize, k: &str| {
        w.get(i)
            .is_some_and(|(s, q)| !q && s.eq_ignore_ascii_case(k))
    };
    // `[db.]t` from token i: (db, table, next index).
    let name_at = |i: usize| -> Option<(Option<String>, String, usize)> {
        let (a, _) = w.get(i)?;
        if a == "." {
            return None;
        }
        if w.get(i + 1).is_some_and(|(s, q)| !q && s == ".") {
            let (b, _) = w.get(i + 2)?;
            Some((Some(a.clone()), b.clone(), i + 3))
        } else {
            Some((None, a.clone(), i + 1))
        }
    };
    let show_at = if kw(0, "SPRIG") && kw(1, "SHOW") || kw(0, "SHOW") && kw(1, "SPRIG") {
        Some(2)
    } else {
        None
    };
    if let Some(i) = show_at {
        if !(kw(i, "LOOKUP") && (kw(i + 1, "INDEXES") || kw(i + 1, "INDEX"))) {
            return None;
        }
        let i = i + 2;
        if i == w.len() {
            return Some(LookupStmt::Show {
                db: None,
                table: None,
            });
        }
        if kw(i, "FROM") || kw(i, "IN") {
            let (db, table, j) = name_at(i + 1)?;
            return (j == w.len()).then_some(LookupStmt::Show {
                db,
                table: Some(table),
            });
        }
        return None;
    }
    if !kw(0, "SPRIG") || !kw(2, "LOOKUP") || !kw(3, "INDEX") {
        return None;
    }
    let (name, _) = w.get(4)?.clone();
    if !kw(5, "ON") {
        return None;
    }
    let (db, table, j) = name_at(6)?;
    if j != w.len() {
        return None;
    }
    if kw(1, "REBUILD") {
        Some(LookupStmt::Rebuild { name, db, table })
    } else if kw(1, "DROP") {
        Some(LookupStmt::Drop { name, db, table })
    } else {
        None
    }
}

/// The columns of `SPRIG SHOW LOOKUP INDEXES`, as
/// `information_schema.SPRIG_LOOKUP_INDEXES` names them.
pub const VIEW_COLUMNS: [&str; 10] = [
    "TABLE_SCHEMA",
    "TABLE_NAME",
    "INDEX_NAME",
    "COLUMN_NAME",
    "IS_UNIQUE",
    "ORIGIN",
    "STATE",
    "CONFLICTS",
    "LOOKUP_ID",
    "UPDATED_AT",
];

/// `SPRIG SHOW LOOKUP INDEXES`' columns.
#[must_use]
pub fn columns(charset: u16) -> Vec<ColumnDef> {
    VIEW_COLUMNS
        .iter()
        .map(|n| text_column(n, charset))
        .collect()
}

/// What a lookup statement answers.
#[derive(Debug)]
pub enum LookupAnswer {
    /// OK with this many rows (owner rows a build read).
    Ok {
        /// Rows affected.
        affected: u64,
    },
    /// `SHOW`.
    Rows(LocalAnswer),
}

fn no_database() -> SprigError {
    SprigError::standard(1046, *b"3D000", "No database selected")
}

/// `db.t` as the session names it: its database and table rows.
fn resolve(
    ts: &TenantSession,
    cat: &TenantCatalog,
    db: Option<&str>,
    table: &str,
) -> Result<(String, ObjectId, String)> {
    let lctn = ts.lower_case_table_names();
    let dbn = db.ok_or_else(no_database)?;
    let d = cat
        .database(&database_key(dbn, lctn))
        .filter(|d| d.state == DbState::Active)
        .ok_or_else(|| {
            SprigError::standard(1049, *b"42000", format!("Unknown database '{dbn}'"))
        })?;
    let o = cat
        .object(
            d.db_id,
            Namespace::Relation,
            &object_key(ObjectKind::Table, table, lctn),
        )
        .filter(|o| o.state == ObjectState::Active)
        .ok_or_else(|| {
            SprigError::standard(
                1146,
                *b"42S02",
                format!("Table '{dbn}.{table}' doesn't exist"),
            )
        })?;
    Ok((d.name.clone(), o.object_id, o.name.clone()))
}

fn need_alter(ts: &TenantSession, db: &str, object: ObjectId, table: &str) -> Result<()> {
    super::authz::settings_check(
        ts,
        Target::Object(object),
        false,
        false,
        false,
        Some(db),
        Some(table),
        sprig_proto_mysql::Persona::MariaDb,
    )
}

/// The rows the session may see, as text: one per lookup.
async fn visible_rows(
    ts: &TenantSession,
    cat: &TenantCatalog,
    only: Option<ObjectId>,
) -> Result<Vec<Vec<Option<Bytes>>>> {
    let svc = Arc::clone(ts.services());
    let text = |s: &str| Some(Bytes::copy_from_slice(s.as_bytes()));
    let mut out = Vec::new();
    for r in records(&svc, ts.tenant).await? {
        if only.is_some_and(|o| o != r.owner) {
            continue;
        }
        let Some(obj) = cat.objects.get(r.owner) else {
            continue;
        };
        let Some(d) = cat.databases.get(obj.db_id) else {
            continue;
        };
        if !super::authz::settings_visible(ts, cat, &d.name, d.db_id, Some(obj.object_id)) {
            continue;
        }
        out.push(vec![
            text(&d.name),
            text(&obj.name),
            text(&r.index_name),
            text(&r.columns.join(",")),
            text(if r.unique { "YES" } else { "NO" }),
            text(r.origin.as_str()),
            text(r.state.as_str()),
            text(&r.conflicts.to_string()),
            text(&r.id.0.to_string()),
            text(&r.updated_at),
        ]);
    }
    Ok(out)
}

/// Runs a lookup statement (module docs).
///
/// # Errors
/// 1046, 1049, 1146, 1142, 1091, 1062, 39420 and the lookup jobs' errors.
pub async fn execute(
    ts: &TenantSession,
    cat: &Arc<TenantCatalog>,
    current: Option<&str>,
    stmt: &LookupStmt,
    charset: u16,
) -> Result<LookupAnswer> {
    let svc = Arc::clone(ts.services());
    match stmt {
        LookupStmt::Show { db, table } => {
            let only = match table {
                Some(t) => Some(resolve(ts, cat, db.as_deref().or(current), t)?.1),
                None => None,
            };
            Ok(LookupAnswer::Rows(LocalAnswer {
                columns: columns(charset),
                rows: visible_rows(ts, cat, only).await?,
            }))
        }
        LookupStmt::Drop { name, db, table } => {
            let (dbn, object, tname) = resolve(ts, cat, db.as_deref().or(current), table)?;
            need_alter(ts, &dbn, object, &tname)?;
            if !drop_for_key(&svc, ts.tenant, object, name).await? {
                return Err(SprigError::standard(
                    1091,
                    *b"42000",
                    format!("Can't DROP LOOKUP INDEX `{name}`; check that it exists"),
                ));
            }
            Ok(LookupAnswer::Ok { affected: 0 })
        }
        LookupStmt::Rebuild { name, db, table } => {
            let (dbn, object, tname) = resolve(ts, cat, db.as_deref().or(current), table)?;
            need_alter(ts, &dbn, object, &tname)?;
            let host = GwHost {
                svc: Arc::clone(&svc),
                tenant: ts.tenant,
            };
            let store = maps(&svc, ts.tenant);
            let admin = Admin {
                tenant: ts.tenant,
                maps: &store,
                host: &host,
                chunk: CHUNK_ROWS,
            };
            let mut c = host.catalog().await?;
            if admin.find(&mut c, object, name).await?.is_some() {
                drop(c);
                let r = admin.rebuild(object, &tname, name).await?;
                return Ok(LookupAnswer::Ok { affected: r.rows });
            }
            // A declared unique key without a lookup: build one.
            let m = membership(&mut c, object)
                .await?
                .ok_or_else(|| refused(&format!("{tname} isn't sharded")))?;
            drop(c);
            let (owner_map, _) = store.load(ts.tenant, m.group).await?;
            let r0 = owner_map
                .ranges
                .first()
                .ok_or_else(|| SprigError::internal("a group without shards"))?;
            let schema = sprig_shard::shard_schema(ts.tenant, m.db, m.group, r0.shard);
            let pool = svc.locator.ctl_pool(&r0.cluster).await?;
            let mut sc = pool.get_conn().await.map_err(catalog_err)?;
            let shape = owner_shape(&mut sc, &schema, &tname).await?;
            let Some((key, column)) = lookup_keys(&shape, &m.key)
                .into_iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
            else {
                return Err(SprigError::standard(
                    1176,
                    *b"42000",
                    format!(
                        "Key '{name}' doesn't exist in table '{tname}' as a unique key of one \
                         column without the distribution key"
                    ),
                ));
            };
            let r = build_for_key(&svc, ts.tenant, object, &tname, &key, &column).await?;
            Ok(LookupAnswer::Ok { affected: r.rows })
        }
    }
}

/// `information_schema.SPRIG_LOOKUP_INDEXES` as a derived table of literal
/// rows.
///
/// # Errors
/// The catalog's.
pub async fn view_sql(ts: &TenantSession, cat: &Arc<TenantCatalog>, nbe: bool) -> Result<String> {
    let rows = visible_rows(ts, cat, None).await?;
    let lit = |v: &Option<Bytes>| match v {
        Some(b) => sprig_sql::names::rewrite::sql_string(&String::from_utf8_lossy(b), nbe),
        None => "NULL".to_owned(),
    };
    let mut sql = String::from("(");
    if rows.is_empty() {
        let cols: Vec<String> = VIEW_COLUMNS
            .iter()
            .map(|n| format!("CAST(NULL AS CHAR) AS `{n}`"))
            .collect();
        let _ = write!(sql, "SELECT {} FROM DUAL WHERE 0", cols.join(", "));
    }
    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            sql.push_str(" UNION ALL ");
        }
        sql.push_str("SELECT ");
        for (j, (v, n)) in row.iter().zip(VIEW_COLUMNS).enumerate() {
            if j > 0 {
                sql.push_str(", ");
            }
            if i == 0 {
                let _ = write!(sql, "CAST({} AS CHAR) AS `{n}`", lit(v));
            } else {
                sql.push_str(&lit(v));
            }
        }
    }
    sql.push(')');
    Ok(sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements_parse() {
        assert_eq!(
            parse("SPRIG SHOW LOOKUP INDEXES"),
            Some(LookupStmt::Show {
                db: None,
                table: None
            })
        );
        assert_eq!(
            parse("show sprig lookup indexes from `shop`.customers;"),
            Some(LookupStmt::Show {
                db: Some("shop".into()),
                table: Some("customers".into())
            })
        );
        assert_eq!(
            parse("SPRIG REBUILD LOOKUP INDEX uk_email ON customers"),
            Some(LookupStmt::Rebuild {
                name: "uk_email".into(),
                db: None,
                table: "customers".into()
            })
        );
        assert_eq!(
            parse("SPRIG DROP LOOKUP INDEX `uk``x` ON d.t"),
            Some(LookupStmt::Drop {
                name: "uk`x".into(),
                db: Some("d".into()),
                table: "t".into()
            })
        );
        for other in [
            "SPRIG SHOW PINS",
            "SHOW INDEX FROM t",
            "SPRIG REBUILD LOOKUP INDEX k ON t extra",
            "SPRIG DROP LOOKUP INDEX k",
            "SELECT 'SPRIG SHOW LOOKUP INDEXES'",
        ] {
            assert_eq!(parse(other), None, "{other}");
        }
    }
}
